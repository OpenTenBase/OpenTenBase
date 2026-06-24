/*-------------------------------------------------------------------------
 *
 * execFragment.c
 *	  This code provides support for fragment execution.
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execFragment.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/gtm.h"
#include "access/htup_details.h"
#include "access/printtup.h"
#include "access/tupdesc.h"
#include "access/tuptoaster.h"
#include "catalog/partition.h"
#include "catalog/pgxc_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/tablespace.h"
#include "executor/execDispatchFragment.h"
#include "executor/execFragment.h"
#include "executor/execParallel.h"
#include "executor/executor.h"
#include "executor/nodeSubplan.h"
#include "forward/fnbufmgr.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/subselect.h"
#include "pgxc/nodemgr.h"
#include "pgxc/groupmgr.h"
#include "pgxc/planner.h"
#include "pgxc/poolmgr.h"
#include "pgxc/squeue.h"
#include "port/pg_bitutils.h"
#include "storage/shmem.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "commands/resgroupcmds.h"

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif


#define get_node_idx(nvirtualdop, nodeId) \
	((nvirtualdop) * (nodeId).nodeid + (nodeId).virtualid)

/* GUC parameter */
bool enable_exec_fragment_print = false;
bool enable_exec_fragment_critical_print = false;
bool force_transfer_datarow = false;
int  fn_send_regiser_factor = 1;

struct AssignFidContext
{
	FragmentTable	*ftable;
	int				 parentIndex;
	EState			*estate;
	int				 eflags;
	PlannedStmt		*plannedstmt;
	Bitmapset		*plan_id;
	Fragment		*write_fragment;
} AssignFidContext;

static int local_fid = -1;		/* local fid passed from coordinator */
static int local_flevel = -1;
static int saved_local_fid = -1;
static int saved_local_flevel = -1;
static int param_srcnodeid = -1;
static int param_srcfid = InvalidFid;
static int g_broadcast_node_group_id = 0;
static char dummy_page[BLCKSZ] = {0};

static void RemoteFragmentInit(RemoteFragmentState *fragmentstate,
							   EState *estate, int eflags);

static void WaitForParallelRemoteWorkerDone(RemoteFragmentState *node);
/* ora_compatible */
static multi_distribution *GetLocatorExtraData(RemoteSubplan *topplan,
											   Oid *attrsType);
static bool contain_lockrows(Node *node);

/*
 * Walk a list of SubPlans (or initPlans, which also use SubPlan nodes).
 */
static bool
planstate_walk_subplans_extend(List *plans,
							   bool (*walker) (),
							   struct AssignFidContext *context)
{
	ListCell   *lc;

	foreach(lc, plans)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);

		/*
		 * We must handle CTE subplan separately
		 */
		if (bms_is_member(sps->subplan->plan_id,
						  context->plannedstmt->sharedCtePlanIds))
		{
			Assert(CTE_SUBLINK == sps->subplan->subLinkType);
			continue;
		}

		DEBUG_FRAG(elog(LOG, "init subplan fragment"));
		if (walker(sps->planstate, context))
		{
			return true;
		}
	}
	return false;
}

/*
 * Walk the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * Note: we don't actually need to examine the Plan list members, but
 * we need the list in order to determine the length of the PlanState array.
 */
static bool
planstate_walk_members_extend(PlanState **planstates, int nplans,
							  bool (*walker) (), struct AssignFidContext *context)
{
	int			j;

	for (j = 0; j < nplans; j++)
	{
		if (planstates[j])
		{
			if (walker(planstates[j], context))
				return true;
		}
	}

	return false;
}

/*
 * A slightly modified version of planstate_tree_walker 
 * which is aware of whether the walker dives into
 * subqueries and marks fragments within subqueries.
 */
static bool
planstate_tree_walker_extend(PlanState *planstate,
							 bool (*walker) (), void *context)
{
	Plan	   *plan = planstate->plan;
	ListCell   *lc;

	/* initPlan-s */
	if (planstate_walk_subplans_extend(planstate->initPlan, walker, context))
		return true;

	/* lefttree */
	if (outerPlanState(planstate))
	{
		if (walker(outerPlanState(planstate), context))
			return true;
	}

	/* righttree */
	if (innerPlanState(planstate))
	{
		if (walker(innerPlanState(planstate), context))
			return true;
	}

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_Append:
			if (planstate_walk_members_extend(((AppendState *) planstate)->appendplans,
											  ((AppendState *) planstate)->as_nplans,
											  walker, context))
				return true;
			break;
		case T_MergeAppend:
			if (planstate_walk_members_extend(((MergeAppendState *) planstate)->mergeplans,
											  ((MergeAppendState *) planstate)->ms_nplans,
											  walker, context))
				return true;
			break;
		case T_BitmapAnd:
			if (planstate_walk_members_extend(((BitmapAndState *) planstate)->bitmapplans,
											  ((BitmapAndState *) planstate)->nplans,
											  walker, context))
				return true;
			break;
		case T_BitmapOr:
			if (planstate_walk_members_extend(((BitmapOrState *) planstate)->bitmapplans,
											  ((BitmapOrState *) planstate)->nplans,
											  walker, context))
				return true;
			break;
		case T_SubqueryScan:
			if (walker(((SubqueryScanState *) planstate)->subplan, context))
				return true;
			break;
		case T_CustomScan:
			foreach(lc, ((CustomScanState *) planstate)->custom_ps)
			{
				if (walker((PlanState *) lfirst(lc), context))
					return true;
			}
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate_walk_subplans_extend(planstate->subPlan, walker, context))
		return true;

	return false;
}

static bool
contain_lockrows(Node *node)
{
	Plan *plan;

	if (node == NULL || !IS_PLAN_NODE(node))
		return false;

	plan = (Plan *) node;

	if (IsA(plan, RemoteSubplan))
		return false;

	if (IsA(plan, LockRows))
		return true;

	return plan_tree_walker(plan, contain_lockrows, NULL);
}

#ifdef __RESOURCE_QUEUE__
/* Collect the number of Dprocess created on each node */
static void
CollectPerDNDProcessNumber(PGXCNodeHandle **datanode_handles, int dn_conn_count,
						   int16 *perdn_dprocess_num)
{
	int i = 0;
	char ntype = PGXC_NODE_DATANODE;

	for (i = 0; i < dn_conn_count; i++)
	{
		PGXCNodeHandle * handle = NULL;
		int handle_PGXCNodeId = 0;

		handle = datanode_handles[i];
		handle_PGXCNodeId = PGXCNodeGetNodeId(handle->nodeoid, &ntype);

		if (handle_PGXCNodeId >= 0 && handle_PGXCNodeId < NumDataNodes)
		{
			perdn_dprocess_num[handle_PGXCNodeId] += 1;
		}
	}
}
#endif

static void
FragmentCreateDProcess(Fragment *fragment, struct AssignFidContext *context)
{
	RemoteSubplan *node = (RemoteSubplan *) fragment->plan;
	bool execOnAll = true;
	bool write_fragment = false;
	PGXCNodeAllHandles *pgxc_conn_res;
	int i;

	DEBUG_FRAG(elog(LOG, "FragmentCreateDProcess: fid %d index %d parent index %d",
					fragment->fid, fragment->index, fragment->parentIndex));

	/*
	 * fragment level > 1 indicates its parent fragment is executed on DN
	 * rather than CN
	 */
	if (fragment->level > 1 || context->plannedstmt->commandType == CMD_SELECT)
		execOnAll = node->execOnAll;

	if (outerPlan(fragment->plan))
	{
		Plan *outerplan = outerPlan(fragment->plan);
		write_fragment = IsA(outerplan, ModifyTable) ||
						 IsA(outerplan, MultiModifyTable);

		/* select for update or share, need get write handles */
		if (!write_fragment && context->plannedstmt->rowMarks != NIL)
			write_fragment = contain_lockrows((Node*)outerplan);
	}
	else
	{
		if (IsFragmentGtidScan(fragment) || IsFragmentSharedCte(fragment))
		{
			write_fragment = false;
		}
		else
		{
			elog(ERROR, "FragmentCreateDProcess: unexpected fragment");
		}
	}

	if (write_fragment)
	{
		if (context->write_fragment)
			elog(ERROR, "There are multiple writable fragments");
		else
			context->write_fragment = fragment;
	}

	if (execOnAll)
	{
		pgxc_conn_res = get_handles(node->nodeList, NULL, false, true,
									fragment->level, fragment->fid, write_fragment, true);
		fragment->connections = pgxc_conn_res->datanode_handles;
		fragment->connNum = pgxc_conn_res->dn_conn_count;
#ifdef __RESOURCE_QUEUE__
		CollectPerDNDProcessNumber(fragment->connections, fragment->connNum,
								   context->estate->es_perdn_dprocess_num);
#endif

		DEBUG_FRAG(elog(LOG, "FragmentCreateDProcess: get all fid handles num %d, fid %d complete",
						fragment->connNum, fragment->fid));

	}
	else
	{
		fragment->connections = (PGXCNodeHandle **)palloc(sizeof(PGXCNodeHandle *));
		fragment->connections[0] = get_any_handle(node->nodeList, fragment->level,
												  fragment->fid, write_fragment);
		fragment->connNum = 1;
#ifdef __RESOURCE_QUEUE__
		CollectPerDNDProcessNumber(fragment->connections, fragment->connNum,
								   context->estate->es_perdn_dprocess_num);
#endif

		DEBUG_FRAG(elog(LOG, "FragmentCreateDProcess: get any fid handle num %d, fid %d complete",
						fragment->connNum, fragment->fid));

	}

	/*
	 * For fragment, CN forks all DN processes, so we need to begin transaction
	 * and/or sub-transaction at all fragment levels in plpgsql case.
	 *
	 * Also, remember cursor name here.
	 */
	for (i = 0; i < fragment->connNum; i++)
	{
		snprintf(fragment->cursor, NAMEDATALEN, "%ld_%ld_%d",
				 context->estate->queryid.timestamp_nodeid,
				 context->estate->queryid.sequence, fragment->fid);

		if (enable_distri_print)
			elog(LOG, "fragment %p cursor %s %s %d", fragment, fragment->cursor,
				 fragment->connections[i]->nodename, fragment->connections[i]->backend_pid);
	}
}

static Fragment *
AllocNewFragment(struct AssignFidContext *context, RemoteFragmentState *pstate)
{
	FragmentTable	*fragmentTable = context->ftable;
	Fragment 		*fragment;
	Fragment		*parent;
	RemoteSubplan   *subplan = (RemoteSubplan *) pstate->combiner.ss.ps.plan;

	if (bms_is_member(subplan->scan.plan.plan_node_id, fragmentTable->allocated))
		return NULL;

	parent = fragmentTable->fragments[context->parentIndex];
	/* Assign unique fragmentId to this fragment */
	fragment = MemoryContextAllocZero(fragmentTable->ftcxt, sizeof(Fragment));

	/* assign fid */
	fragment->fid = subplan->fid;

	/* Save the plan */
	fragment->plan = &subplan->scan.plan;
	fragment->pstate = pstate;
	fragment->index = fragmentTable->currentAssignIndex;
	/* Build the parent-child relation */
	fragment->parentIndex = context->parentIndex;
	/* Initialize fragment level */
	fragment->level = parent->level + 1;

	fragmentTable->allocated =
		bms_add_member(fragmentTable->allocated, subplan->scan.plan.plan_node_id);

	return fragment;
}

/* Assign Fid to target node */
static bool
InitFragment(PlanState *planstate, struct AssignFidContext *context)
{
	Plan	   *plan;
	FragmentTable  *fragmentTable;

	if (planstate == NULL)
		return false;

	plan = planstate->plan;
	fragmentTable = context->ftable;

	if (IsA(plan, RemoteSubplan))
	{
		RemoteFragmentState *fstate = castNode(RemoteFragmentState, planstate);
		RemoteSubplan  *subplan = (RemoteSubplan *) plan;
		RemoteSubplan  *parentPlan;
		Fragment 	   *fragment;
		Fragment	   *parentFragment;
		int 			parentIndex;
		bool			res;

		fragment = AllocNewFragment(context, fstate);
		if (fragment == NULL)
			return false;

		subplan->flevel = fragment->level;

		DEBUG_FRAG(elog(LOG, "InitFragment: fid %d level %d cursor %s",
						fragment->fid, fragment->level, subplan->cursor));

		/* Save fragment into fragment table */
		fragmentTable->fragments[fragmentTable->currentAssignIndex++] = fragment;

		/* Non redistribution case */
		if (list_length(subplan->distributionRestrict) <= 1 ||
			subplan->replicated_list != NULL ||
			subplan->roundrobin_replicate == true ||
			subplan->roundrobin_distributed == true)
		{
			if (context->parentIndex == RootFragmentIndex)
			{
				if (subplan->targetNodes == NIL)
				{
					MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(subplan));
					subplan->targetNodes = list_make1_int(PGXCNodeId - 1);
					MemoryContextSwitchTo(oldcxt);
				}
				subplan->targetNodeType = CoordinatorNode;
				DEBUG_FRAG(elog(LOG, "InitFragment: remote read from CN fid %d level %d",
								fragment->fid, fragment->level));
			}
			else
			{
				parentFragment = fragmentTable->fragments[context->parentIndex];
				parentPlan = castNode(RemoteSubplan, parentFragment->plan);
				subplan->targetNodes = parentPlan->nodeList;
				subplan->targetNodeType = DataNode;

				DEBUG_FRAG(elog(LOG, "InitFragment: broadcast fid %d level %d",
								fragment->fid, fragment->level));
			}
		}
		else /* redistribution case */
		{
			subplan->targetNodes = subplan->distributionRestrict;
			subplan->targetNodeType = DataNode;

			DEBUG_FRAG(elog(LOG, "InitFragment: redistribute fid %d level %d",
							fragment->fid, fragment->level));
		}

		if (!(context->eflags & EXEC_FLAG_EXPLAIN_ONLY))
		{
			/*
			 * Create DProcesses for this fragment, we will dispatch
			 * the fragment plan to the DProcesses to execute.
			 */
			FragmentCreateDProcess(fragment, context);

			if (IsLevelOneFragment(fragment) && !IsFragmentSharedCte(fragment))
			{
				if (planstate->state->es_plannedstmt->commandType == CMD_SELECT)
				{
					fstate->combiner.combine_type = COMBINE_TYPE_NONE;
				}
				else
				{
					if (subplan->execOnAll)
						fstate->combiner.combine_type = COMBINE_TYPE_SUM;
					else
						fstate->combiner.combine_type = COMBINE_TYPE_SAME;
				}
			}
			else
				fstate->combiner.combine_type = COMBINE_TYPE_NONE;
		}

		/*
		 * save the current fragment index as context parent index
		 * before recursing into the children fragments.
		 */
		parentIndex = fragment->parentIndex;
		context->parentIndex = fragment->index;
		res = planstate_tree_walker_extend(planstate, InitFragment, context);

		/*
		 * restore the parent index of the current fragment into context
		 * before returning to the upper layer.
		 */
		context->parentIndex = parentIndex;
		return res;
	}

	return planstate_tree_walker_extend(planstate, InitFragment, context);
}

/* The entry point to assign FID to target nodes */
static void
FragmentPlanTree(PlanState *planstate, FragmentTable *fragmentTable, int eflags)
{
	struct AssignFidContext context;
	context.ftable = fragmentTable;
	context.parentIndex = RootFragmentIndex;
	context.estate = planstate->state;
	context.eflags = eflags;
	context.plannedstmt = planstate->state->es_plannedstmt;
	context.write_fragment = NULL;

	InitFragment(planstate, &context);
}

/*
 * ExecInitFragmentTree
 *
 * New async-execution framework is introduced in opentenbase-c to solve the
 * Hyperscale cluster communication problem. The plan tree is divided into
 * multiple execution fragments. Fragments communicate with their peer through
 * forward node.
 * Thus in this function, we split the plan tree by counting the RemoteSubquery
 * node, and then assign a unique fragment id, which will be an unique data
 * routing path for this specific data stream.
 */
void
ExecInitFragmentTree(PlanState *planstate, int eflags)
{
	FragmentTable  *fragmentTable;
	Fragment	   *rootFragment;
	MemoryContext	old = NULL;
	MemoryContext	ftcxt;
	int				fragmentNum;
	EState	   	   *estate = planstate->state;
	ListCell	   *lc;
	ResponseCombiner *combiner = NULL;
	RemoteFragmentController *control = NULL;
	int i;

	fragmentNum = estate->es_plannedstmt->fragmentNum;
	/* Only root fragment or it's not a fragment plan, then we are done. */
	if (fragmentNum <= 1)
		return;

	if (PGXCPlaneNameID == PGXCMainPlaneNameID && RecoveryInProgress())
		elog(ERROR, "can't do distributed query via fid because it is the main plane standby node.");

	DEBUG_FRAG(elog(LOG, "ExecInitFragmentTree: fragment num %d", fragmentNum));

	ftcxt = AllocSetContextCreate(TopTransactionContext,
								  "FragmentTable Context",
								  ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(ftcxt);

	/* Build up the fragment table */
	fragmentTable = palloc0(sizeof(FragmentTable));
	fragmentTable->ftcxt = ftcxt;
	fragmentTable->fragments = palloc0(fragmentNum * sizeof(Fragment *));
	GenerateFNQueryId(&estate->queryid);
	pgstat_refresh_fn_queryid(estate->queryid);

#ifdef __RESOURCE_QUEUE__
	if (IsResQueueEnabled() && !superuser() && estate != NULL &&
		estate->es_plannedstmt != NULL)
	{
		int64	acquiredMemBytes = 0;

		acquiredMemBytes = EstimateQueryMemoryBytes(estate->es_plannedstmt);
		AcquireResourceFromResQueue(acquiredMemBytes);

		CNAssignLocalResQueueInfo();
		ResQUsageAddLocalResourceInfo();
	}
#endif

	/*
	 * Acquire a resource group slot, which can be performed on local CN or remote one,
	 * depending on whehter current is leader CN or not.
	 * Note that resource group control is only performed on fragment plan for now. Others
	 * may not be such valuable than that.
	 */
	if (IS_LOCAL_ACCESS_NODE && !IsParallelWorker() && !(eflags & EXEC_FLAG_EXPLAIN_ONLY) && ShouldAssignResGroup())
	{
		PGXCNodeHandle *leaderCNHandle = find_leader_cn();
		if (IS_CENTRALIZED_DATANODE || resource_group_local||
			leaderCNHandle == NULL || PGXCNodeName == NULL ||
			strcmp(leaderCNHandle->nodename, PGXCNodeName) == 0)
		{
			/* Perform locally, unassign first to prevent reassign */
			UnassignResGroup();
			AssignResGroup();
		}
		else
		{
			/*
				* Still perform locally for bypass query, otherwise, request it on remote.
				* Also, unassign first to prevent reassign
				*/
			if (!AssignResGroupForBypassQueryOrNot())
			{
				/* Check whether leader cn is avaiable first */
				if (!is_leader_cn_avaialble(leaderCNHandle))
					elog(ERROR, "leader coordinator not available, please check and set "
								"GUC resource_group_leader_coordinator to another coordinator");

				pgstat_report_wait_start(PG_WAIT_RESOURCE_GROUP);
				if (estate->es_leader_handle != NULL)
					CNSendResGroupReq(estate->es_leader_handle);
				pgstat_report_resgroup(get_resgroup_oid(GetResGroupNameForRole(GetUserId()), false));
				estate->es_leader_handle = leaderCNHandle;
				CNSendResGroupReq(leaderCNHandle);
				pgstat_report_wait_end();
			}
		}
		/* update snapshot which may be too old to execute after waiting on group */
		UnregisterSnapshot(estate->es_snapshot);
		estate->es_snapshot = RegisterSnapshot(GetTransactionSnapshot());
		DBUG_SUSPEND_IF_PREFIX("suspend_after_resgroup_assigned");
	}

	rootFragment = palloc0(sizeof(Fragment));
	rootFragment->fid = 0;
	rootFragment->level = 0;
	rootFragment->parentIndex = RootParentFragmentIndex;
	rootFragment->index = RootFragmentIndex;
	rootFragment->plan = planstate->plan;

	/* Root fragment always appears in the first slot */
	fragmentTable->fragments[0] = rootFragment;
	fragmentTable->currentAssignIndex++;

	DEBUG_FRAG(elog(LOG, "ExecInitFragmentTree: root fragment fid %d",
					rootFragment->fid));

	FragmentPlanTree(planstate, fragmentTable, eflags);

	/*
	 * Even if we have used fragmentNum as a condition for early exit before,
	 * there is still a chance to exit earlier after traversing the planstate
	 * tree, because there may be fragments that do not need to be executed
	 * and were not completely eliminated in the planning stage.
	 */
	if (fragmentTable->currentAssignIndex <= 1)
		return;

	/* do real dispatch if not EXPLAIN */
	if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		ExecDispatchRemoteFragment(fragmentTable, planstate->state);

	/* init remote controller */
	control = InitRemoteController(estate);
	for (i = 1; i < fragmentTable->currentAssignIndex; i++)
	{
		Fragment *fragment = fragmentTable->fragments[i];

		if (IsLevelOneFragment(fragment) && !IsFragmentSharedCte(fragment))
			combiner = &fragment->pstate->combiner;

		if (IsA(fragment->plan, RemoteSubplan) &&
			((RemoteSubplan *)fragment->plan)->protocol == ProtocolPQ)
		{
			control->libpq = fragment;
			continue;
		}
	}

	if (combiner == NULL)
		elog(ERROR, "Could not find main combiner");

	if (control->libpq)
	{
		combiner = palloc0(sizeof(ResponseCombiner));
		InitResponseCombiner(combiner, control->nConnections, COMBINE_TYPE_NONE);
		combiner->extended_query = true;
		combiner->controller = control;
	}

	control->combiner = combiner;
	estate->es_remote_controller = control;

	for (i = 1; i < fragmentTable->currentAssignIndex; i++)
	{
		Fragment *fragment = fragmentTable->fragments[i];

		if (IsA(fragment->plan, RemoteSubplan) &&
			((RemoteSubplan *)fragment->plan)->protocol == ProtocolPQ)
		{
			continue;
		}

		RemoteControllerBindListen(control, fragment);
	}

	i = 0;
	combiner->connections = palloc0(sizeof(PGXCNodeHandle *) * control->nConnections);
	foreach(lc ,control->connections)
		combiner->connections[i++] = (PGXCNodeHandle *)lfirst(lc);
	combiner->node_count = combiner->conn_count = control->nConnections;

	/*
	 * The remote subplan operator that receives data on the CN is not actually
	 * initialized during ExecInitRemoteFragment. Additionally, due to the
	 * existence of the initplan redirection mechanism, we must initialize
	 * these receive operators only after all fids have been allocated and all
	 * plans have been sent.
	 */
	for (i = 1; i < fragmentTable->currentAssignIndex; i++)
	{
		Fragment *f = fragmentTable->fragments[i];
		if (f->pstate != NULL &&
			castNode(RemoteSubplan, f->plan)->targetNodeType == CoordinatorNode)
			RemoteFragmentInit(f->pstate, estate, eflags);
	}

	MemoryContextSwitchTo(old);
}

void
ExecEndFragmentTree(EState *estate)
{
	DEBUG_FRAG(elog(LOG, "exec end fragment tree"));

	WaitRemoteFragmentDone(estate);
	estate->es_fragment_execute = false;

#ifdef __RESOURCE_QUEUE__
	if (IsResQueueEnabled() && !superuser())
	{
		ResQUsageRemoveLocalResourceInfo();
		ReleaseResourceBackToResQueue();
	}
#endif

	if (IS_LOCAL_ACCESS_NODE && !IsParallelWorker() && ShouldUnassignResGroup())
	{
		if (IS_CENTRALIZED_DATANODE || resource_group_local ||
			estate->es_leader_handle == NULL)
		{
			UnassignResGroup();
		}
		else
		{
			if (estate->es_leader_handle != NULL)
			{
				CNSendResGroupReq(estate->es_leader_handle);
				estate->es_leader_handle = NULL;
			}
		}
	}
}

void
ExecCloseFragmentTree(EState *estate, bool free)
{
	FragmentTable *ftable = (FragmentTable *) estate->es_ftable;

	/*
	 * Update coordinator statistics
	 */
	DistributeRecordRelChangesInfo(estate);

	CloseRemoteFragment(ftable);

	if (free)
	{
		MemoryContextDelete(ftable->ftcxt);
		estate->es_ftable = NULL;
	}
}

/*
 * --------------------------------------------------------------------
 * The below functions provide ways to execute fragments in DProcess
 * --------------------------------------------------------------------
 */

static SendBuffer *
FragmentSendStateInit(uint16 fid, int ntargets, int nvirtualdop, uint8 flag)
{
	SendBuffer *sendBuffer;
	int	max_nodes = ((flag & SEND_BUFFER_PARAM) ||
					 (flag & SEND_BUFFER_COORD) == 0) ?
					   NumDataNodes: NumCoords;

	sendBuffer = (SendBuffer *)palloc0(sizeof(SendBuffer));
	sendBuffer->fid = fid;
	sendBuffer->targetNodeNum = ntargets;
	sendBuffer->flag = flag;

	sendBuffer->virtualdopNum = nvirtualdop;
	sendBuffer->buffers = (FnBuffer *) palloc0(max_nodes * nvirtualdop * sizeof(FnBuffer));

	return sendBuffer;
}

static void
FragmentSendStateDestroy(SendBuffer *sendBuffer)
{
	if (!sendBuffer)
		return;

	if (sendBuffer->control && sendBuffer->control->owner == MyProcPid)
	{
		int	curidx = sendBuffer->control->head;
		FnBufferDesc *buf;

		while (curidx != -1)
		{
			buf = GetFnBufferDescriptor(curidx, FNPAGE_SEND);
			ResourceOwnerForgetFnBuffer(CurrentResourceOwner,
										FnBufferDescriptorGetBuffer(buf));
			curidx = buf->next;
			FnBufferFree(buf);
		}

		ClearFnSndQueueEntry(sendBuffer->control);
		ResourceOwnerForgetFnSend(CurrentResourceOwner, sendBuffer->control);
		sendBuffer->control = NULL;
	}
}

static void
FragmentRecvStateRemember(RemoteFragmentState *fstate)
{
	/* make a fake receiver for resource owner */
	RemoteSubplan *subplan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	MemoryContext old, context;
	TupleQueueReceiver *receiver;
	int num_workers = 1;
	int i, node_count;
	FnRcvQueueEntry *entry;
	ResourceOwner owner = IsParallelWorker() ?
						  CurrentResourceOwner : TopTransactionResourceOwner;

	if (subplan->localSend &&
		!list_member_int(subplan->nodeList, PGXCNodeId - 1))
		return;

	if (subplan->distributionRestrict &&
		!list_member_int(subplan->distributionRestrict, PGXCNodeId - 1))
	{
		fstate->needRecvData = false;
		return;
	}

	node_count = subplan->localSend ? 1 : list_length(subplan->nodeList);
	if (subplan->num_workers > 0)
		num_workers += subplan->num_workers; /* including main process */

	ResourceOwnerEnlargeFnRecv(owner);

	/* setup per-node per-column buffer */
	fstate->recvBuffer = (RecvBuffer *)palloc0(sizeof(RecvBuffer));
	fstate->recvBuffer->transfer_datarow = subplan->transfer_datarow;

	context = AllocSetContextCreate(TopTransactionContext,
									"TupleQueue Context",
									ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(context);
	receiver = palloc0(sizeof(TupleQueueReceiver));
	receiver->context = context;

	if (subplan->num_virtualdop > 1)
	{
		for (i = 0; i < subplan->num_virtualdop; i++)
		{
			entry = GetFnRcvQueueEntry(fstate->combiner.ss.ps.state->queryid, subplan->fid, i,
									   node_count, num_workers,
									   fstate->combineType, NULL);
			receiver->wentry = lappend(receiver->wentry, entry);
		}
	}
	else
	{
		receiver->entry = GetFnRcvQueueEntry(fstate->combiner.ss.ps.state->queryid, subplan->fid, 0,
											 node_count, num_workers,
											 fstate->combineType, NULL);
	}

	receiver->owner = owner;
	fstate->recvBuffer->queue = receiver;
	MemoryContextSwitchTo(old);

	ResourceOwnerRememberFnRecv(owner, receiver);
}

static void
FragmentRecvStateInit(RemoteFragmentState *fragmentstate)
{
	PlanState *planstate = &fragmentstate->combiner.ss.ps;
	RemoteSubplan *subplan = (RemoteSubplan *) planstate->plan;
	int num_workers = 1;
	int node_count;
	int workerid = 0;

	if (subplan->localSend &&
		!list_member_int(subplan->nodeList, PGXCNodeId - 1))
		return;

	node_count = subplan->localSend ? 1 : list_length(subplan->nodeList);
	if (subplan->num_workers > 0)
		num_workers += subplan->num_workers; /* including main process */
	if (subplan->num_virtualdop > 1 && IsParallelWorker())
		workerid = ParallelWorkerNumber;

	DEBUG_FRAG(elog(LOG, "register recv fragment %d cursor %s queuenum %d "
						 "worker %d, node count %d",
					subplan->fid, subplan->cursor, num_workers,
					ParallelWorkerNumber, node_count));

	if (subplan->distributionRestrict &&
		!list_member_int(subplan->distributionRestrict, PGXCNodeId - 1))
	{
		fragmentstate->needRecvData = false;
		return;
	}

	DEBUG_FRAG(elog(LOG, "recv fragment parallel workers %d nodecount %d",
					subplan->num_workers, node_count));

	fragmentstate->total_pages = 0;
	fragmentstate->disk_pages = 0;

	/* setup per-node per-column buffer */
	fragmentstate->recvBuffer = (RecvBuffer *)palloc0(sizeof(RecvBuffer));
	fragmentstate->recvBuffer->transfer_datarow = subplan->transfer_datarow;

	fragmentstate->recvBuffer->queue =
		TupleQueueThreadInit(fn_recv_work_mem,
							 planstate->state->queryid,
							 subplan->fid,
							 node_count,
							 num_workers,
							 workerid,
							 subplan->sort != NULL,
							 fragmentstate->combineType);

	if (IS_PGXC_COORDINATOR)
	{
		fragmentstate->recvBuffer->controller = RemoteFragmentGetController(fragmentstate);
	}
	else
	{
		fragmentstate->recvBuffer->controller = NULL;
	}

	fragmentstate->needRecvData = true;
}

static void
FragmentRecvParamStateInit(RemoteFragmentState *fragmentstate)
{
	PlanState *planstate = &fragmentstate->combiner.ss.ps;
	RemoteSubplan *subplan = (RemoteSubplan *) planstate->plan;
	int node_count;

	node_count = subplan->localSend ? 1 : list_length(subplan->nodeList);

	DEBUG_FRAG(elog(LOG, "register recv %s fid %d cursor %s",
					subplan->under_subplan ? "param" : "msg",
					subplan->param_fid, subplan->cursor));

	/* setup per-node per-column buffer */
	fragmentstate->recvParamBuffer = (RecvBuffer *)palloc0(sizeof(RecvBuffer));

	fragmentstate->recvParamBuffer->queue =
		TupleQueueThreadInit(fn_recv_work_mem, planstate->state->queryid,
							 subplan->param_fid, node_count, 1, 0,
							 false, fragmentstate->combineType);
}

static void
FragmentRedistributionSetup(RemoteFragmentState *fragmentstate)
{
	RemoteSubplan *node = (RemoteSubplan *) fragmentstate->combiner.ss.ps.plan;
	EState *estate = fragmentstate->combiner.ss.ps.state;
#ifdef _MIGRATE_
	Oid groupid = InvalidOid;
	Oid reloid  = InvalidOid;
	ListCell *lc;
#endif
	multi_distribution *m_dist = NULL; /* for opentenbase_ora compatible */

	/* begin opentenbase_ora */
	if (node->distributionType == LOCATOR_TYPE_MIXED)
	{
		TupleDesc	typeInfo;
		Oid			*typOids;
		int			 i;

		typeInfo = fragmentstate->combiner.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
		typOids = palloc0(sizeof(Oid) * typeInfo->natts);
		for (i = 0; i < typeInfo->natts; i++)
			typOids[i] = TupleDescAttr(typeInfo, i)->atttypid;

		m_dist = GetLocatorExtraData(node, typOids);
	}
	/* end opentenbase_ora */

	/*
	 * The distribution type of "INSERT ALL" is LOCATOR_TYPE_MIXED. In this case,
	 * the information of the CASE WHEN expression is stored in the locator.
	 * Therefore, regardless of the number of Data Nodes (DNs), even if there is only one DN,
	 * it is necessary to create the locator.
	 */
	if (list_length(node->distributionNodes) > 1 || node->distributionType == LOCATOR_TYPE_MIXED)
	{
		TupleDesc	typeInfo;
#ifdef __OPENTENBASE_C__
		int ndiscols = 0;
		AttrNumber *discolnums = NULL;
		Oid *discoltypes = NULL;
#endif

		typeInfo = fragmentstate->combiner.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
		if (node->ndiskeys > 0)
		{
			int i = 0;
			Form_pg_attribute attr;
			ndiscols = node->ndiskeys;
			discolnums = node->diskeys;
			discoltypes = (Oid *)palloc0(sizeof(Oid) * ndiscols);
			for (i = 0; i < ndiscols; i++)
			{
				attr = TupleDescAttr(typeInfo, discolnums[i] - 1);
				discoltypes[i] = attr->atttypid;
			}
			fragmentstate->disValues = (Datum *)palloc0(sizeof(Datum) * ndiscols);
			fragmentstate->disIsNulls = (bool *)palloc0(sizeof(bool) * ndiscols);
		}
		/* Set up locator */
#ifdef _MIGRATE_
		foreach(lc, estate->es_range_table)
		{
			RangeTblEntry *tbl_entry = (RangeTblEntry *)lfirst(lc);
			if (tbl_entry->rtekind == RTE_RELATION)
			{
				reloid = tbl_entry->relid;
				elog(DEBUG5, "[ExecInitRemoteSubplan]reloid=%d", reloid);
				break;
			}
		}
		foreach(lc, node->distributionNodes)
		{
			int nodeidx = lfirst_int(lc);
			Oid nodeid = PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE);

			elog(DEBUG5, "[ExecInitRemoteSubplan]nodeid=%d", nodeid);
			groupid = GetGroupOidByNode(nodeid);
			break;
		}
		elog(DEBUG5, "[ExecInitRemoteSubplan]groupid=%d", groupid);

		fragmentstate->locator = createLocator(node->distributionType,
											   RELATION_ACCESS_INSERT,
											   LOCATOR_LIST_LIST,
											   0,
											   (void *) node->distributionNodes,
											   (void **) &fragmentstate->dest_nodes,
											   false,
											   groupid,
											   discoltypes, discolnums, ndiscols,
											   m_dist);
#else
		fragmentstate->locator = createLocator(node->distributionType,
											 RELATION_ACCESS_INSERT,
											 distributionType,
											 LOCATOR_LIST_LIST,
											 0,
											 (void *) node->distributionNodes,
											 (void **) &fragmentstate->dest_nodes,
											 false,
											 m_dist);
#endif
	}
	else
		fragmentstate->locator = NULL;

	if (fragmentstate->locator == NULL && node->targetNodes == NIL)
		elog(ERROR, "fragment should have locator for redistribution");
}

static bool
CheckBroadcastData(List *targetNodes)
{
	int i,j,match_count = -1;
	bool needLock = g_NodeGroupMgr->needLock;
	bool ret = false;
	ListCell *lc;

	if (needLock)
		LWLockAcquire(NodeGroupLock, LW_SHARED);

	for (i = 0; i < g_NodeGroupMgr->shmemNumNodeGroups; i++)
	{
		/* targetNodes amount is equal to node amount in NodeGroupDefs */
		if (targetNodes->length == g_NodeGroupMgr->NodeGroupDefs[i].numidx)
		{
			match_count = 0;

			/* compare targetNodes with nodes in NodeGroupDefs */
			foreach(lc, targetNodes)
			{
				int 	nodeid = lfirst_int(lc);
				for (j = 0; j < g_NodeGroupMgr->NodeGroupDefs[i].numidx; j++)
				{
					if (nodeid == g_NodeGroupMgr->NodeGroupDefs[i].node_idx[j])
						break;
				}

				if (j == g_NodeGroupMgr->NodeGroupDefs[i].numidx)
				{
					/* nodeid is not exist in this NodeGroupDefs, switch to the next NodeGroupDefs */
					elog(LOG, "[CheckBroadcastData] targetNodes nodeid %d is not found in g_NodeGroupMgr->NodeGroupDefs[%d].node_idx array! " \
						"j %d, g_NodeGroupMgr->NodeGroupDefs[%d].numidx %d, g_NodeGroupMgr->NodeGroupDefs[%d].node_idx[0] %d",
						 nodeid, i, j, i, g_NodeGroupMgr->NodeGroupDefs[i].numidx, i, g_NodeGroupMgr->NodeGroupDefs[i].node_idx[0]);
					break;
				}
				else
				{
					match_count++;
				}
			}

			if (match_count == targetNodes->length)
			{
				g_broadcast_node_group_id = i;
				ret = true;
				break;
			}
		}
	}

	if (!ret)
		elog(LOG, "[CheckBroadcastData] targetNodes->length %d, match_count %d, g_NodeGroupMgr->shmemNumNodeGroups %d, g_NodeGroupMgr->NodeGroupDefs[0].numidx %d",
			 targetNodes->length, match_count, g_NodeGroupMgr->shmemNumNodeGroups, g_NodeGroupMgr->NodeGroupDefs[0].numidx);

	if (needLock)
		LWLockRelease(NodeGroupLock);

	return ret;
}

static void
RemoteFragmentInit(RemoteFragmentState *fragmentstate,
				   EState *estate, int eflags)
{
	RemoteSubplan *subplan = (RemoteSubplan *)fragmentstate->combiner.ss.ps.plan;

	DEBUG_FRAG(elog(LOG, "RemoteFragmentInit qid_ts_node:%ld, qid_seq:%ld, fid:%d, cursor:%s",
					estate->queryid.timestamp_nodeid, estate->queryid.sequence,
					subplan->fid, subplan->cursor));

	if (subplan->flevel > 1 || estate->es_plannedstmt->commandType == CMD_SELECT)
	{
		fragmentstate->combineType = COMBINE_TYPE_NONE;
		fragmentstate->execOnAll = subplan->execOnAll;
	}
	else
	{
		if (subplan->execOnAll)
			fragmentstate->combineType = COMBINE_TYPE_SUM;
		else
			fragmentstate->combineType = COMBINE_TYPE_SAME;

		/*
		 * If we are updating replicated table we should run plan on all nodes.
		 * We are choosing single node only to read
		 */
		fragmentstate->execOnAll = true;
	}

	if (subplan->flevel > 1)
	{
		if (fragmentstate->execNodes == NIL)
		{
			elog(ERROR, "no execNodes for fragment fid:%d", subplan->fid);
		}
		else if (!fragmentstate->execOnAll)
		{
			/*
			 * XXX We should change planner and remove this flag.
			 * We want only one node is producing the replicated result set,
			 * and planner should choose that node - it is too hard to determine
			 * right node at execution time, because it should be guaranteed
			 * that all consumers make the same decision.
			 * For now always execute replicated plan on local node to save
			 * resources.
			 */

			/*
			 * Make sure local node is in execution list
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Getting replicated results from remote node is not supported")));
		}
	}

	/*
	 * For CN, the whole plan state tree is already initialized 
	 * during ExecInitNode. Don't try to reinitialize.
	 */
	if (IS_PGXC_DATANODE)
	{
		if (!(eflags & EXEC_FLAG_SUBPLAN) && fragmentstate->sendFragment)
		{
			if (subplan->num_workers > 0)
			{
				if (!IsParallelWorker() &&
					subplan->num_virtualdop == 0 && parallel_leader_participation)
					fragmentstate->need_to_scan_locally = true;
				else
					eflags |= EXEC_FLAG_IDLE_LEADER;
			}

			if (enable_eager_free && !IsFragmentInSubquery(subplan))
			{
				estate->es_top_eflags |= EXEC_FLAG_EAGER_FREE;
				eflags |= EXEC_FLAG_EAGER_FREE;
			}

			/*
			 * 1. Only leader will receive msg for cache send.
			 *
			 * 2. For fragments in subquery, we also should register the same
			 * 	  fid as recv fid for receiving paramters sent down by recv
			 * 	  fragment. We do not support parallel under subplan, so it's
			 * 	  okay to do it without considering it's an idle leader.
			 *
			 * Notice we do it here instead of ExecFinishInitRemoteFragment
			 * to ensure tqueue-thread would be ready when upper remote sends
			 * message/param.
			 */
			if ((subplan->cacheSend && !IsParallelWorker()) || IsFragmentInSubquery(subplan))
				FragmentRecvParamStateInit(fragmentstate);

			outerPlanState(fragmentstate) = ExecInitNode(outerPlan(subplan),
														 estate, eflags);
			fragmentstate->outPlanInit = true;
		}
	}

	fragmentstate->eflags = eflags;

	/* parallel worker will do this when choose_next_subplan */
	if (!(eflags & EXEC_FLAG_SUBPLAN) &&
		!(eflags & EXEC_FLAG_EXPLAIN_ONLY) &&
		!estate->is_cursor &&
		(!subplan->cacheSend || ((eflags & EXEC_FLAG_IDLE_LEADER) && !IsParallelWorker())))
	{
		ExecFinishInitRemoteFragment(fragmentstate);
	}
}

/*
 * FindMCVMaxLength
 *	find out the maximum length of datum whose type is varchar
 */
static int
FindMCVMaxLength(List *mcv_list)
{
	int max_mcv_length = 0;
	int data_length;

	ListCell *mcv_cell;
	Const *mcv_const;

	foreach(mcv_cell, mcv_list)
	{
		mcv_const = (Const *)lfirst(mcv_cell);
		data_length = VARSIZE_ANY_EXHDR(mcv_const->constvalue);

		if (data_length > max_mcv_length)
		{
			max_mcv_length = data_length;
		}
	}

	return max_mcv_length;
}

/* 
 * BuilValueListHash
 *	Build hash table against MCV list
 */
static struct HTAB*
BuilValueListHash(List *value_list)
{
	ListCell *mcv_cell = NULL;
	Const *mcv_const;
	struct HTAB *hash_table = NULL;
	HASHCTL	hash_parameters;
	int hash_key_length = 0;
	Oid mcv_type;
	Oid out_function_oid;
	bool is_var_len = false;
	bool is_found = false;
	char *data_buffer;

	if (value_list == NULL)
		return NULL;

	/*
	 * set length against hash entry
	 */
	mcv_cell = linitial(value_list);
	mcv_type = ((Const *)mcv_cell)->consttype;
	getTypeBinaryOutputInfo(mcv_type, &out_function_oid, &is_var_len);

	hash_key_length = (is_var_len == true ?
					   FindMCVMaxLength(value_list) : sizeof(Datum));

	/*
	 * Set parameters to prepare hash table construction
	 */
	MemSet(&hash_parameters, 0, sizeof(hash_parameters));
	hash_parameters.keysize = hash_key_length;
	hash_parameters.entrysize = hash_key_length;

	/*
	 * Construct hash table
	 */
	hash_table = hash_create("Create hash table against MCV list",
							 list_length(value_list),
							 &hash_parameters, HASH_ELEM);

	/*
	 * Insert each MCV entry into hash table
	 */
	foreach(mcv_cell, value_list)
	{
		mcv_const = (Const *)lfirst(mcv_cell);
		is_found = false;
		if (is_var_len == true)
		{
			data_buffer = VARDATA_ANY(mcv_const->constvalue);
			hash_search(hash_table, data_buffer, HASH_ENTER, &is_found);
		}
		else
		{
			hash_search(hash_table, &(mcv_const->constvalue),
						HASH_ENTER, &is_found);
		}
		Assert(!is_found);
	}

	/*
	 * return hash table created
	 */
	return hash_table;
}

/*
 * ExecInitNode for Remote Subplan
 */
RemoteFragmentState *
ExecInitRemoteFragment(RemoteSubplan *node, EState *estate, int eflags)
{
	RemoteFragmentState *fragmentstate = NULL;
	ResponseCombiner *combiner = NULL;

	DEBUG_FRAG(elog(LOG, "ExecInitRemoteFragment qid_ts_node:%ld, qid_seq:%ld, "
						 "fid:%d, cursor:%s, local:%d",
					estate->queryid.timestamp_nodeid, estate->queryid.sequence,
					node->fid, node->cursor, local_fid));

	if (node->localSend && IsFragmentInSubquery(node))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("a fragment under subplan can't be local-send")));
	if (node->num_workers > 0 && IsFragmentInSubquery(node))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("a fragment under subplan can't be paralleled")));

	fragmentstate = makeNode(RemoteFragmentState);
	combiner = &fragmentstate->combiner;
	fragmentstate->execNodes = list_copy(node->nodeList);
	fragmentstate->combiner.ss.ps.plan = (Plan *) node;
	fragmentstate->combiner.ss.ps.state = estate;
	fragmentstate->combiner.ss.ps.ExecProcNode = ExecRemoteFragment;
	fragmentstate->combiner.ss.ps.qual = NULL;
	fragmentstate->combineType = COMBINE_TYPE_NONE;
	fragmentstate->sendFragment = (node->fid == local_fid);

	combiner->extended_query = true;
	combiner->request_type = REQUEST_TYPE_QUERY;

	/*
	 * Create memory contexts for fragmenstate.
	 */
	fragmentstate->cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "FragmentstateMemoryContext",
											   ALLOCSET_DEFAULT_SIZES);
	fragmentstate->tmp_cxt = AllocSetContextCreate(estate->es_query_cxt,
												   "FragmentstateTempContext",
												   ALLOCSET_DEFAULT_SIZES);

	ExecAssignExprContext(estate, &fragmentstate->combiner.ss.ps);

	ExecInitResultTupleSlotTL(&fragmentstate->combiner.ss.ps);
	fragmentstate->combiner.ss.ps.ps_ResultTupleSlot->tts_nodebufferindex = -1;
	fragmentstate->combiner.ss.ps.ps_ResultTupleSlot->tts_nvalid =
		fragmentstate->combiner.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts;

	/*
		* Build up data skew list for retain and broadcase
		*/
	fragmentstate->retain_hash = BuilValueListHash(node->retain_list);
	fragmentstate->broadcast_hash = BuilValueListHash(node->replicated_list);

	if (IS_PGXC_COORDINATOR)
	{
		/*
		 * On CN, we walk the whole plan tree to
		 * build the fragment states and defer the state initialization.
		 */
		outerPlanState(fragmentstate) = ExecInitNode(outerPlan(node),
													 estate, eflags);
		fragmentstate->outPlanInit = true;
	}
	else
	{
		RemoteFragmentInit(fragmentstate, estate, eflags);

		/* Skip initPlan for Recv Fragment */
		if (!fragmentstate->sendFragment)
			((Plan *)node)->initPlan = NIL;
	}

	/*
	 * The es_not_send_tuple" flag of the remote subplan is set to true, it indicates that
	 * only the replicated table is for update. In this case, only one data node needs to
	 * send data. However, if the "es_not_send_tuple" flag is not set, it means that the
	 * data needs to be sent to the upper layer for computation. es_not_send_tuple is set
	 * to true, it indicates that locking rows is performed on a replicated table, and it
	 * is not the first DN.
	 */
	estate->es_not_send_tuple = node->only_one_send_tuple && estate->es_not_send_tuple;

	return fragmentstate;
}

void
ExecFinishInitRemoteFragment(RemoteFragmentState *fragmentstate)
{
	RemoteSubplan *subplan = (RemoteSubplan *) fragmentstate->combiner.ss.ps.plan;

	/*
	 * Special case:
	 * 1.We are coordinator and not yet generate queryid for this plan.
	 * 2.GenericPlan has queryid (last time's) and we use targetNodeType.
	 * 3.subplan or initplan that this fragment won't execute. (Notice we
	 *   check top-level-eflags not the eflags in fragment itself.
	 */
	if (fragmentstate->combiner.ss.ps.state->queryid.timestamp_nodeid == 0 ||
		(IS_PGXC_COORDINATOR && subplan->targetNodeType != CoordinatorNode) ||
		(fragmentstate->combiner.ss.ps.state->es_top_eflags & EXEC_FLAG_SUBPLAN))
	{
		return;
	}

	if (fragmentstate->finish_init)
		return;

	if (fragmentstate->sendFragment)
	{
		int workers = subplan->num_workers;
		int ntargets;
		int nvirtualdop = Max(subplan->num_virtualdop, 1);
		uint8 flag = 0;

		if (local_flevel != subplan->flevel)
			elog(ERROR, "flevel does not match, local flevel %d subplan flevel %d",
				 local_flevel, subplan->flevel);

		DEBUG_FRAG(elog(LOG, "register send fragment fid %d cursor %s workers %d num_workers %d",
						subplan->fid, subplan->cursor, workers, subplan->num_workers));

		FragmentRedistributionSetup(fragmentstate);

		if (subplan->transfer_datarow)
			flag |= SEND_BUFFER_DATAROW;

		if (list_length(subplan->dests) > 0)
		{
			ListCell *lc;
			int 	  i;

			Assert(subplan->num_virtualdop <= 1);
			fragmentstate->extraSendBuffers =
				palloc(sizeof(SendBuffer *) * list_length(subplan->dests));

			i = 0;
			foreach(lc, subplan->dests)
			{
				FragmentDest *dest = lfirst_node(FragmentDest, lc);

				if (dest->targetNodeType == CoordinatorNode)
					flag |= SEND_BUFFER_COORD;

				ntargets = list_length(dest->targetNodes);
				fragmentstate->extraSendBuffers[i++] =
					FragmentSendStateInit(dest->fid, ntargets, nvirtualdop,
										  flag);
			}
		}
		else
		{
			ntargets = list_length(subplan->targetNodes);

			if (subplan->targetNodeType == CoordinatorNode)
				flag |= SEND_BUFFER_COORD;

			fragmentstate->sendBuffer =
				FragmentSendStateInit(subplan->fid, ntargets, nvirtualdop,
									  flag);
			fragmentstate->sendBufferBroadcast =
				FragmentSendStateInit(subplan->fid, ntargets, nvirtualdop,
									  flag | SEND_BUFFER_BROADCAST);
		}

		if (list_length(subplan->distributionRestrict) > 1 || subplan->distributionType == LOCATOR_TYPE_MIXED)
		{
			if (fragmentstate->combiner.ss.ps.plan->parallel_num < 0)
			{
				int step = shard_cluster_num / abs(fragmentstate->combiner.ss.ps.plan->parallel_num);
				int i;
				fragmentstate->sc_virtualid =
					(uint8 *) palloc0(sizeof(uint8) * shard_cluster_num);
				for (i = 0; i < shard_cluster_num; i++)
					fragmentstate->sc_virtualid[i] = i / step;
			}
		}
		else
		{
			if (CoordinatorNode == subplan->targetNodeType)
			{
				fragmentstate->nodeid.nodeid = linitial_int(subplan->targetNodes);
			}
			else if (subplan->localSend)
			{
				fragmentstate->nodeid.nodeid = PGXCNodeId - 1;
			}
			else if (!IsFragmentInSubquery(subplan) && !subplan->cacheSend)
			{
				fragmentstate->broadcast_flag = CheckBroadcastData(subplan->targetNodes);
				if (fragmentstate->broadcast_flag)
					fragmentstate->nodeid.nodeid = linitial_int(subplan->targetNodes);
			}
		}
	}
	else
	{
		/* 
		 * For fragments in subquery, we should register the same fid as send
		 * fid for sending paramters down to the send fragment.
		 */
		if (IsFragmentInSubquery(subplan) || subplan->cacheSend)
		{
			DEBUG_FRAG(elog(LOG, "register send %s fid %d cursor %s",
							subplan->under_subplan ? "param" : "msg",
							subplan->param_fid, subplan->cursor));

			/* the param can't be sent to CN */
			fragmentstate->sendParamBuffer =
				FragmentSendStateInit(subplan->param_fid,
									  list_length(subplan->targetNodes), 1,
									  SEND_BUFFER_PARAM);
		}

		if ((fragmentstate->eflags & EXEC_FLAG_IDLE_LEADER) == 0 ||
			IsParallelWorker())
			FragmentRecvStateInit(fragmentstate);
		else
			FragmentRecvStateRemember(fragmentstate);
	}

	fragmentstate->finish_init = true;
}

/*
 * Allocate enough buffer for this fragment, construct an entry identifying
 * this fragment and put it into the shared message queue for FN sender to
 * flush.
 */
static FnSndQueueEntry *
RegisterSenderFragment(FNQueryId queryid, uint16 fid, int nodenum,
					   bool allow_duplicate)
{
	FnSndQueueEntry *entry;
	FragmentKey		 key = {};
	bool			 found;
	int				 i;
	FnBufferDesc	*pre = NULL;
	FnBufferDesc	*cur;

	ResourceOwnerEnlargeFnSend(CurrentResourceOwner);

	key.queryid = queryid;
	key.fid = fid;
	key.workerid = ParallelWorkerNumber;

	LWLockAcquire(FnSndBufferLock, LW_EXCLUSIVE);
	entry = (FnSndQueueEntry *) hash_search(FnSndBufferHash,
											(const void *) &key,
											HASH_ENTER, &found);
	if (!found)
	{
		int npage = Max(nodenum * fn_send_regiser_factor, 10);

		entry->owner = MyProcPid;
		LWLockRelease(FnSndBufferLock);

		/* register into resowner now, we could abort in FnBufferAlloc below */
		ResourceOwnerRememberFnSend(CurrentResourceOwner, entry);

		for (i = 0; i < npage; i++)
		{
			FnPage	page;

			ResourceOwnerEnlargeFnBuffers(CurrentResourceOwner);
			cur = FnBufferAlloc(FNPAGE_SEND, queryid);
			ResourceOwnerRememberFnBuffer(CurrentResourceOwner,
										  FnBufferDescriptorGetBuffer(cur));

			if (pre != NULL)
				pre->next = cur->buf_id;

			if (i == 0)
				entry->head = cur->buf_id;

			/* mark this page with our queryid and fid */
			page = FnBufferDescriptorGetPage(cur);
			FnPageInit(page, queryid, fid, PGXCNodeId - 1,
					   ParallelWorkerNumber + 1);

			pre = cur;

			DEBUG_FRAG(elog(DEBUG1, "RegisterSenderFragment: register page %d. "
									"qid_ts_node:%ld, qid_seq:%ld, fid:%d, "
									"node:%d, worker:%d",
							cur->buf_id, queryid.timestamp_nodeid, queryid.sequence,
							fid, PGXCNodeId - 1, ParallelWorkerNumber + 1));
		}
	}
	else if (!allow_duplicate)
	{
		/* should this happen? */
		elog(ERROR, "find an existing hash entry for qid_ts_node:%ld, qid_seq:%ld sending fragment %d",
			 queryid.timestamp_nodeid, queryid.sequence, fid);
	}
	else
	{
		LWLockRelease(FnSndBufferLock);
	}

	DEBUG_FRAG(elog(LOG, "RegisterSenderFragment: register entry of "
						 "qid_ts_node:%ld, qid_seq:%ld, fid:%d, node:%d, worker:%d",
					entry->key.queryid.timestamp_nodeid, entry->key.queryid.sequence,
					entry->key.fid, PGXCNodeId - 1, entry->key.workerid));
	return entry;
}

/* Get an empty buffer from the beginning of a local send buffer list */
static FnBufferDesc *
GetNextBufferFromStart(FnSndQueueEntry *entry)
{
	FnBufferDesc *result = NULL;
	int		cur = entry->head;
	uint32	state;
	bool	wait = false;

	while (result == NULL)
	{
		result = GetFnBufferDescriptor(cur, FNPAGE_SEND);
		state = pg_atomic_read_u32(&result->state);
		if ((state & BM_DIRTY) != 0)
		{
			cur = result->next;
			result = NULL;

			if (unlikely(cur == -1))
			{
				/*
				 * Failed to find a valid buffer, just loop once again.
				 * This will finally find a buffer since the quota should
				 * greater than the number of datanode.
				 */
				wait_event_fn_start(local_fid, WAIT_EVENT_SEND_DATA);
				wait = true;

				pg_usleep(1000L);
				cur = entry->head;

				CHECK_FOR_INTERRUPTS();
				if (end_query_requested)
					return NULL;
			}
		}
	}

	if (unlikely(wait))
		wait_event_fn_end();
	return result;
}

/*
 * Get a empty buffer AFTER last buffer that was same target node, same
 * queryid, same fid
 */
static FnBufferDesc *
GetNextBuffer(FnSndQueueEntry *entry, FnBuffer last)
{
	FnBufferDesc *cur = NULL;
	uint32		  state;
	int			  curidx = last - 1;

	/*
	 * If this is a new requirement, just get a buffer from start. Also, if
	 * the destination changed, it means other requests have got this page
	 * which indicates the old data was flushed.
	 */
	if (FnBufferIsInvalid(last))
		return GetNextBufferFromStart(entry);

	/* find a buffer after "last" in the queue */
	while (curidx != -1)
	{
		cur = GetFnBufferDescriptor(curidx, FNPAGE_SEND);
		/* check if the buffer is already flushed */
		state = pg_atomic_read_u32(&cur->state);
		if ((state & BM_DIRTY) == 0)
			return cur;
		curidx = cur->next;
	}

	/* we can't find a valid buffer, so get a new one from the start */
	return GetNextBufferFromStart(entry);
}

/*
 * Get a share buffer page that must have enough space
 * as given len for specific fid and queryid.
 */
static FnPage
FragmentGetPage(SendBuffer *sendBuffer, FNQueryId queryid,
				NodeId nodeid, Size len, bool need_lock)
{
	uint16	fid = sendBuffer->fid;
	int		idx = get_node_idx(sendBuffer->virtualdopNum, nodeid);
	FnBuffer		 buf = sendBuffer->buffers[idx];
	FnPage			 page;
	FnPageHeader	 head;
	FnBufferDesc	*desc;

	if (unlikely(len > BLCKSZ - SizeOfFnPageHeaderData))
		elog(ERROR, "fragment sending oversize tuple %ld", len);

	if (unlikely(sendBuffer->control == NULL))
		sendBuffer->control =
			RegisterSenderFragment(queryid, fid,
								   sendBuffer->targetNodeNum *
								   sendBuffer->virtualdopNum,
								   sendBuffer->flag & SEND_BUFFER_PARAM);
retry:
	if (FnBufferIsInvalid(buf))
	{
		/* need a new buffer */
		desc = GetNextBuffer(sendBuffer->control,
							 sendBuffer->buffers[idx]);

		if (unlikely(desc == NULL))
		{
			Assert(end_query_requested);
			((FnPageHeader) dummy_page)->lower = SizeOfFnPageHeaderData;
			sendBuffer->buffers[idx] = InvalidBuffer;
			return dummy_page;
		}

		buf = FnBufferDescriptorGetBuffer(desc);
		page = FnBufferDescriptorGetPage(desc);

		/* remember this new buf */
		sendBuffer->buffers[idx] = buf;

		if (sendBuffer->flag & SEND_BUFFER_BROADCAST)
		{
			desc->broadcast_node_num = sendBuffer->targetNodeNum;
			desc->node_group_id = (uint16) g_broadcast_node_group_id;
		}
		else if (sendBuffer->flag & SEND_BUFFER_COORD)
		{
			desc->broadcast_node_num = 0;
			desc->dst_node_id = (uint16) MaskCoordinatorNode(nodeid.nodeid);
		}
		else /* distribute to single DN */
		{
			desc->broadcast_node_num = 0;
			desc->dst_node_id = (uint16) nodeid.nodeid;
		}

		LWLockAcquire(FnBufferDescriptorGetContentLock(desc), LW_EXCLUSIVE);

		head = (FnPageHeader) page;
		head->virtualid = nodeid.virtualid;

		DEBUG_FRAG(elog(DEBUG1, "FragmentGetPage: new page %d to %d. "
								"qid_ts_node:%ld, qid_seq:%ld, fid:%d, "
								"node:%d, worker:%d",
						desc->buf_id, desc->dst_node_id,
						head->queryid.timestamp_nodeid,
						head->queryid.sequence, head->fid,
						head->nodeid, head->workerid));
	}
	else
	{
		/* we already have a big enough page */
		desc = GetFnBufferDescriptor(buf - 1, FNPAGE_SEND);
		page = FnBufferDescriptorGetPage(desc);
		if (need_lock)
			LWLockAcquire(FnBufferDescriptorGetContentLock(desc), LW_EXCLUSIVE);
	}

	head = (FnPageHeader) page;
	if (len > BLCKSZ - head->lower)
	{
		/* mark buffer dirty and full */
		MarkFnBufferDirty(desc, BM_FLUSH);
		DEBUG_FRAG(elog(DEBUG1, "FragmentGetPage: mark buffer %d full", buf - 1));
		LWLockRelease(FnBufferDescriptorGetContentLock(desc));

		/* pin it again to increase refcount for FN sender */
		PinFnBuffer(desc);

		/* push it to flush queue */
		fn_desc_enqueue(ForwardSenderQueue, desc->buf_id);

		/* wake up sender to flush full page */
		FnStrategyWakeSender(desc->buf_id);

		/* retry, allocate a new one */
		buf = InvalidFnBuffer;
		goto retry;
	}

	MarkFnBufferDirty(desc, BM_DIRTY);
	DEBUG_FRAG(elog(DEBUG2, "FragmentGetPage: mark buffer %d dirty", buf - 1));
	return page;
}

/*
 * Mark corresponding buffer as dirty (and full), and release content lock
 */
static void
FragmentSendDone(SendBuffer *sendBuffer, NodeId nodeid, bool complete, bool isparams)
{
	int		idx = get_node_idx(sendBuffer->virtualdopNum, nodeid);
	FnBuffer buf = sendBuffer->buffers[idx];
	FnBufferDesc *desc;
	fn_page_kind kind = FNPAGE_SEND;
	FnPageHeader head;

	if (unlikely(FnBufferIsInvalid(buf)))
		return;

	desc = GetFnBufferDescriptor(buf - 1, kind);

	head = (FnPageHeader) FnBufferDescriptorGetPage(desc);
	if (complete && (head->flag & FNPAGE_HUGE))
		FnPageSetFlag((FnPage) head, FNPAGE_HUGE_END);

	if (complete)
	{
		MarkFnBufferDirty(desc, BM_FLUSH);
		DEBUG_FRAG(elog(DEBUG1, "FragmentSendDone: mark buffer %d full", buf - 1));
	}

	if (isparams)
		FnPageSetFlag((FnPage) head, FNPAGE_PARAMS);
	
	LWLockRelease(FnBufferDescriptorGetContentLock(desc));

	if (complete)
	{
		sendBuffer->buffers[idx] = InvalidFnBuffer;

		/* pin it again to increase refcount for FN sender */
		PinFnBuffer(desc);

		/* push it to flush queue */
		fn_desc_enqueue(ForwardSenderQueue, desc->buf_id);

		/* wake up sender to flush full page */
		FnStrategyWakeSender(desc->buf_id);
	}
}

/*
 * Write customized data instead of tuple into our buffer, we
 * assume the receiver knows our protocol, but still set a length
 * since the receive thread need that for iteration.
 * 
 * Must acquire content lock before calling this, mostly using
 * FragmentGetPage with argument need_lock is true.
 */
static void
FragmentSendData(SendBuffer *sendBuffer, FNQueryId queryid,
				 NodeId nodeid, char *data, Size len, bool need_lock)
{
	FnPage  page;
	FnPageHeader header;
	uint16	offset;
	uint32	total = len + sizeof(uint32);

	page = FragmentGetPage(sendBuffer, queryid, nodeid,
						   MAXALIGN(total), need_lock);
	header = (FnPageHeader) page;
	offset = header->lower;
	memcpy(page + offset, &total, sizeof(total));
	offset += sizeof(total);
	memcpy(page + offset, data, len);
	header->lower += MAXALIGN(total);
}

/*
 * Send a MAX_UINT32 as NULL "tuple".
 * This is often used as a complete message.
 */
static void
FragmentSendNullTuple(SendBuffer *sendBuffer, FNQueryId queryid, NodeId nodeid)
{
	FnPage  page;
	FnPageHeader header;
	uint32  n32 = MAX_UINT32;

	page = FragmentGetPage(sendBuffer, queryid, nodeid, sizeof(uint32), true);
	header = (FnPageHeader) page;
	memcpy(page + header->lower, &n32, sizeof(uint32));
	header->lower += sizeof(uint32);
	FnPageSetFlag(page, FNPAGE_END);

	if (enable_exec_fragment_print)
	{	
		elog(LOG, "FragmentSendNullTuple: send EOF. qid_ts_node:%ld,"
				  "qid_seq:%ld, fid:%d, node:%d, virtualid:%d, "
				  "worker:%d, size:%d, flag:%d",
			 header->queryid.timestamp_nodeid, header->queryid.sequence,
			 header->fid, nodeid.nodeid, nodeid.virtualid,
			 header->workerid, header->lower, header->flag);
	}
}

/* Sending large amounts of data beyond valid pages */
static void
FragmentSendHuge(SendBuffer *sendBuffer, FNQueryId queryid, NodeId nodeid,
				 char *data, Size size, bool datarow, uint32 flag)
{
	FnPage	 		page;
	FnPageHeader	head;

	Size	 left = size;
	Size	 write;
	char	*pos = data;
	Size	 kicksize;

	flag |= FNPAGE_HUGE;

	/*
	 * Here we choose a kickstart size of maximum space for transmitting tuple
	 * to get a brand new page, because we don't want other tuples in the old
	 * page rendered as "FNPAGE_HUGE".
	 */
	kicksize = BLCKSZ - SizeOfFnPageHeaderData;
	page = FragmentGetPage(sendBuffer, queryid, nodeid, kicksize, true);

	if (datarow)
	{
		/* if it's not a mintuple, add "length" in the beginning */
		uint32	total = size + sizeof(uint32);

		head = (FnPageHeader) page;

		memcpy(page + head->lower, &total, sizeof(uint32));
		head->lower += sizeof(uint32);

		/* fill this page to avoid alignment issue */
		write = BLCKSZ - head->lower;
		Assert(left > write);
		FnPageSetFlag(page, flag);
		FnPageAddItem(page, pos, write, false);

		left -= write;
		pos += write;
	}

	while (left > 0)
	{
		page = FragmentGetPage(sendBuffer, queryid, nodeid, 1, false);
		head = (FnPageHeader) page;

		write = Min(BLCKSZ - head->lower, left);
		/* we don't set FNPAGE_HUGE_END for vectors */
		if (left <= write)
			flag |= FNPAGE_HUGE_END;
		FnPageSetFlag(page, flag);
		FnPageAddItem(page, pos, write, true);

		left -= write;
		pos += write;
	}
}

/* copy a minimal tuple into share buffer */
static void
FragmentSendMemTuple(SendBuffer *sendBuffer, FNQueryId queryid,
					 NodeId nodeid, MinimalTuple memtuple)
{
	if (memtuple->t_len > BLCKSZ - SizeOfFnPageHeaderData)
	{
		FragmentSendHuge(sendBuffer, queryid, nodeid,
						 (char *) memtuple, memtuple->t_len, false, 0);
	}
	else
	{
		FnPage  page;

		page = FragmentGetPage(sendBuffer, queryid, nodeid,
							   MAXALIGN(memtuple->t_len), true);
		FnPageAddItem(page, (Item) memtuple, memtuple->t_len, true);
	}
}

/* copy value/null array directly into share buffer as a minimal tuple */
static void
FragmentSendAttrs(SendBuffer *sendBuffer, FNQueryId queryid, NodeId nodeid,
				  TupleDesc desc, Datum *values, bool *isnull)
{
	Size    len;    /* total len including hoff */
	Size	alignedSize;
	int     hoff;   /* minimal tuple header offset */
	int     numberOfAttributes = desc->natts;
	int     i;
	FnPage  page;
	FnPageHeader header;
	void   *tuple;
	bool    hasnull = false;

	if (numberOfAttributes > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
					errmsg("number of columns (%d) exceeds limit (%d)",
						   numberOfAttributes, MaxTupleAttributeNumber)));

	/*
	 * Check for nulls
	 */
	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	/*
	 * Deform external values (still compressed)
	 */
	for (i = 0; i < numberOfAttributes; i++)
	{
		/*
		 * Look at non-null varlena attributes
		 */
		if (!isnull[i] && TupleDescAttr(desc, i)->attlen == -1)
		{
			struct varlena *new_value;

			new_value = (struct varlena *) DatumGetPointer(values[i]);
			if (VARATT_IS_EXTERNAL(new_value))
			{
				new_value = heap_tuple_fetch_attr(new_value);
				values[i] = PointerGetDatum(new_value);
			}
		}
	}

	hoff = heap_minimal_tuple_header_size(desc, hasnull);
	len = hoff + heap_compute_data_size(desc, values, isnull);
	alignedSize = MAXALIGN(len);

	if (alignedSize > BLCKSZ - SizeOfFnPageHeaderData)
	{
		MinimalTuple tmptuple = heap_form_minimal_tuple(desc, values, isnull);
		FragmentSendHuge(sendBuffer, queryid, nodeid,
						 (char *) tmptuple, tmptuple->t_len, false, 0);
		heap_free_minimal_tuple(tmptuple);
	}
	else
	{
		page = FragmentGetPage(sendBuffer, queryid,
							   nodeid, alignedSize, true);
		header = (FnPageHeader) page;

		/* put tuple */
		tuple = page + header->lower;
		heap_form_minimal_tuple_ptr(desc, values, isnull, hasnull,
									len, hoff, (MinimalTuple *) &tuple);
		header->lower += MAXALIGN(len);
	}

	/* allocated temp values will be free with MemoryContextReset */
}

static void
FragmentSendTuple(SendBuffer *sendBuffer, TupleTableSlot *slot,
				  FNQueryId queryid, NodeId nodeid)
{
	if (TupIsNull(slot))
	{
		FragmentSendNullTuple(sendBuffer, queryid, nodeid);
		FragmentSendDone(sendBuffer, nodeid, true, false);
	}
	else
	{
		TupleDesc	tdesc = slot->tts_tupleDescriptor;
		int			natts = tdesc->natts;

		if (force_transfer_datarow || (sendBuffer->flag & SEND_BUFFER_DATAROW))
		{
			RemoteDataRow datarow;

			if (slot->tts_datarow)
				datarow = slot->tts_datarow;
			else
			{
				PG_TRY();
				{
					set_portable_output(true);
					datarow = ExecCopySlotDatarow(slot, NULL);
					set_portable_output(false);
				}
				PG_CATCH();
				{
					set_portable_output(false);
					PG_RE_THROW();
				}
				PG_END_TRY();
			}

			if (MAXALIGN(datarow->msglen + sizeof(uint32)) >
				BLCKSZ - SizeOfFnPageHeaderData)
			{
				FragmentSendHuge(sendBuffer, queryid, nodeid,
								 datarow->msg, datarow->msglen, true, 0);
			}
			else
				FragmentSendData(sendBuffer, queryid, nodeid,
								 datarow->msg, datarow->msglen, true);
		}
		else if (slot->tts_mintuple &&
				 !HeapTupleHeaderHasExternal(slot->tts_mintuple))
		{
			FragmentSendMemTuple(sendBuffer, queryid,
								 nodeid, slot->tts_mintuple);
		}
		else if (slot->tts_nvalid == natts)
		{
			FragmentSendAttrs(sendBuffer, queryid, nodeid, tdesc,
							  slot->tts_values, slot->tts_isnull);
		}
		else if (slot->tts_tuple)
		{
			HeapTuple		tuple = slot->tts_tuple;
			MinimalTuple	mtuple;

			if (HeapTupleHasExternal(tuple))
				tuple = toast_flatten_tuple(tuple, slot->tts_tupleDescriptor);

			mtuple = minimal_tuple_from_heap_tuple(tuple);
			FragmentSendMemTuple(sendBuffer, queryid, nodeid, mtuple);
			heap_free_minimal_tuple(mtuple);
		}
		else
		{
			/* last option, we have to deform tuple into values/nulls array */
			slot_getallattrs(slot);
			FragmentSendAttrs(sendBuffer, queryid, nodeid, tdesc,
							  slot->tts_values, slot->tts_isnull);
		}

		FragmentSendDone(sendBuffer, nodeid, false, false);
	}
}

/*
 * FetchSkewDistributionKey
 *	Return distribution key value for join with data skew
 */
static char *
FetchSkewDistributionKey(TupleTableSlot *slot, AttrNumber skew_key_position)
{
	char *datum_buffer = NULL;
	bool is_key_null = false ;
	TupleDesc tuple_descriptor;
	Form_pg_attribute skew_distribution_key_attribute;
	Datum slot_value;
	Datum char_value;

	/*
	 * Get attribute of skew distribution key
	 */
	tuple_descriptor = slot->tts_tupleDescriptor;
	skew_distribution_key_attribute = TupleDescAttr(tuple_descriptor, skew_key_position - 1);

	/*
	 * Get slot value of skew distribution key
	 */
	slot_value = slot_getattr(slot, skew_key_position, &is_key_null);
	Assert(!is_key_null);

	/*
	 * Fetch value of skew distribution key from slot
	 */
	switch(skew_distribution_key_attribute->attlen)
	{
		/*
		 * Datum is char/varchar related type
		 */
		case -1:
		{
			struct varlena *datum_ptr = (struct varlena *) DatumGetPointer(slot_value);
			if (VARATT_IS_COMPRESSED(datum_ptr) || VARATT_IS_EXTERNAL(datum_ptr))
			{
				char_value = PointerGetDatum(pg_detoast_datum(datum_ptr));
			}
			else
			{
				char_value = slot_value;
			}
			datum_buffer = VARDATA_ANY(char_value);
			break;
		}
			/*
			 * Datum is CString type
			 */
		case -2:
		{
			datum_buffer = DatumGetCString(slot_value);
			break;
		}
			/*
			 * Datum is numerical type
			 */
		default:
		{
			if (skew_distribution_key_attribute->attbyval)
			{
				datum_buffer = (char *)(&slot_value);
			}
			else
			{
				datum_buffer = NULL;
			}
			break;
		}
	}

	return datum_buffer;
}

/*
 * check_remote_list
 *	check whether the value is in the remote list
 */
static bool
check_remote_list(HTAB* value_hash, TupleTableSlot *slot,
				  AttrNumber distribution_key_num)
{
	bool is_key_found = false;
	char *datum_buffer = NULL;

	if (value_hash == NULL)
		return false;

	/*
	 * Get value of skew distribution key
	 */
	datum_buffer = FetchSkewDistributionKey(slot, distribution_key_num);

	if (datum_buffer != NULL)
	{
		hash_search(value_hash, datum_buffer, HASH_FIND, &is_key_found);
	}

	return is_key_found;
}

/*
 * check_remote_type
 *	Check romote type for partial broadcast
 *
 * Return value:
 *   0 : no partial broadcast
 *   1 : send slot in local
 *   2 : broadcast the slot
 */
static bool
check_remote_type(TupleTableSlot *slot, AttrNumber distribution_key_num,
				  bool *is_retain, RemoteFragmentState *fstate)
{
	if (check_remote_list(fstate->retain_hash, slot, distribution_key_num))
	{
		*is_retain = true;
		return true;
	}

	if (check_remote_list(fstate->broadcast_hash, slot, distribution_key_num))
	{
		*is_retain = false;
		return true;
	}

	return false;
}

static void
GetDataRouting(RemoteFragmentState *fstate, TupleTableSlot *slot)
{
	RemoteSubplan *plan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	int		i, nid;
	uint32	hashvalue;

	/* Get partitioning value if defined */
	for (i = 0; i < plan->ndiskeys; i++)
	{
		fstate->disValues[i] = slot_getattr(slot, plan->diskeys[i],
											&fstate->disIsNulls[i]);
	}
	/* Determine target nodes */
	(void) GET_NODES(fstate->locator,
						 fstate->disValues, fstate->disIsNulls, plan->ndiskeys,
						 &hashvalue);

	nid = fstate->dest_nodes[0];
	if (IsFragmentInSubquery(plan) && param_srcnodeid != nid)
	{
		fstate->nodeid.nodeid = MAX_UINT16;
		return;
	}
	fstate->nodeid.nodeid = fstate->dest_nodes[0];

	if (plan->num_virtualdop > 1)
	{
		if (fstate->sc_virtualid)
			fstate->nodeid.virtualid =
				fstate->sc_virtualid[GetNodeShardClusterByHashValue(hashvalue)];
		else
			fstate->nodeid.virtualid =
				pg_rotate_right32(hashvalue, 16) % plan->num_virtualdop;
	}
}

/*
 * Form a new tuple by changing the 'startno' and 'endno' attribute to
 * indicate which tableId should be deleted in the tuple.
 *
 * TODO: Original tuple will be deformed multiple times, save values/nulls
 * somewhere.
 */
static TupleTableSlot *
GetDistributedTuple(Locator *self, TupleTableSlot *oldtup, int consumerId)
{
	int					i = 0;
	multi_distribution *edist = get_locator_extradata(self);
	TupleTableSlot	   *resultSlot;
	HeapTuple			tuple;
	Datum			   *values;
	bool			   *isnull;
	bool				valid_tup = false;
	bool				has_rels = false;

	tuple = ExecFetchSlotHeapTuple(oldtup, true, NULL);
	values = palloc(sizeof(Datum) * oldtup->tts_tupleDescriptor->natts);
	isnull = palloc(sizeof(bool) * oldtup->tts_tupleDescriptor->natts);

	heap_deform_tuple(tuple, oldtup->tts_tupleDescriptor, values, isnull);

	resultSlot = (TupleTableSlot *) edist->resultslot;
	Assert(resultSlot != NULL);
	if (resultSlot->tts_tupleDescriptor == NULL)
		ExecSetSlotDescriptor(resultSlot, oldtup->tts_tupleDescriptor);

	Assert(edist->startno > 0 && edist->endno > 0);
	Assert(edist->startno <= oldtup->tts_tupleDescriptor->natts &&
		   edist->endno <= oldtup->tts_tupleDescriptor->natts);

	/* Convert tuple to distributed form. */
	for (i = edist->startno - 1; i <= edist->endno - 1; i++)
	{
		int		j = 0;
		Datum	*intarr;
		int		cnt;

		if (isnull[i])
			continue;

		if (has_rels && edist->has_else && i == edist->endno)
		{
			/* If WHEN matched, ignore ELSE clause. */
			isnull[i] = true;
			continue;
		}

		deconstruct_array(DatumGetArrayTypeP(values[i]),
						  INT4OID,
						  sizeof(int),
						  true,
						  'i',
						  &intarr,
						  NULL,
						  &cnt);
		/*
		 * Check if the should distribute the tuple to the relations on the
		 * consumerId.
		 */
		for (j = 0; j < cnt; j++)
		{
			int		tabid = intarr[j];
			List	*sub_cons = (List *) edist->dist_nodes[tabid];

			if (!list_member_int(sub_cons, consumerId))
				intarr[j] = -1; /* Ignore the table for multi-insert table */
			else
				valid_tup = true;
		}

		/* Reform the array and update the older */
		has_rels = true;
		values[i] = PointerGetDatum(construct_array(intarr,
													cnt,
													INT4OID,
													4,
													true,
													'i'));
	}

	if (valid_tup)
	{
		tuple = heap_form_tuple(oldtup->tts_tupleDescriptor, values, isnull);
		ExecStoreHeapTuple(tuple, resultSlot, false);

		/* No need setup locator->results */
		return resultSlot;
	}
	else
		return NULL;
}

static void
FragmentSendTupleBroadcast(RemoteFragmentState *fstate, TupleTableSlot *slot)
{
	int i;
	RemoteSubplan *plan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	SendBuffer *sendBuffer = fstate->broadcast_flag ?
							 fstate->sendBufferBroadcast : fstate->sendBuffer;

	if (plan->num_virtualdop > 1)
	{
		for (i = 0; i < plan->num_virtualdop; i++)
		{
			fstate->nodeid.virtualid = i;
			FragmentSendTuple(sendBuffer, slot, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
	}
	else
	{
		FragmentSendTuple(sendBuffer, slot, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
	}
}

static void
FragmentRedistributeData(RemoteFragmentState *fstate, TupleTableSlot *slot, Bitmapset *recvMsgNodes)
{
	RemoteSubplan *plan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	int			i, nodeidx;
	bool		is_data_skew = false;
	bool		is_retain = false;
	ListCell   *lc;
	MemoryContext savecxt = NULL;

	if (fstate->combiner.ss.ps.state->es_not_send_tuple)
		return;

	MemoryContextReset(fstate->cxt);
	savecxt = MemoryContextSwitchTo(fstate->cxt);

	if (plan->diskeys != NULL &&
		plan->roundrobin_distributed == false &&
		plan->roundrobin_replicate == false)
		is_data_skew = check_remote_type(slot, plan->diskeys[0], &is_retain, fstate);

	/*
	 * begin opentenbase_ora
	 *
	 * For INSERT ALL/FIRST operation, do the following step:
	 * 1. Get the max node id of the tuple should distribute to, and set node id
	 *    for each locator.
	 * 2. Loop all nodes and send the tuple to them. The tuple including one or
	 *    more int array, which rely on how many WHEN and ELSE clause exist.
	 * 3. On receiver side, MultiModifyTable operator will deconstruct the int
	 *    array and decide which table should be insert by it.
	 */
	if (plan->distributionType == LOCATOR_TYPE_MIXED)
	{
		int		nnodes;
		int		i;
		uint32	hashvalue;
		TupleTableSlot *oldslot = slot;

		set_locator_inputslot(fstate->locator, slot);

		nnodes = GET_NODES(fstate->locator,
						   fstate->disValues,
						   fstate->disIsNulls,
						   plan->ndiskeys,
						   &hashvalue);

		for (i = 0; i < nnodes; i++)
		{
			slot = GetDistributedTuple(fstate->locator, oldslot, i);
			if (slot == NULL)
				continue;

			/*
			 * It will skip send tuple if the target node don't need the tuple,
			 * so we can use i as nodeid directly.
			 */
			fstate->nodeid.nodeid = i;
			FragmentSendTuple(fstate->sendBuffer,
							  slot,
							  fstate->combiner.ss.ps.state->queryid,
							  fstate->nodeid);
		}
	}
	/* end opentenbase_ora */
	else if (list_length(plan->dests) > 0)
	{
		ListCell *dest_lc, *target;

		i = 0;
		foreach(dest_lc, plan->dests)
		{
			FragmentDest *dest = lfirst_node(FragmentDest, dest_lc);

			if (IsFragmentInSubquery(plan))
			{
				if (dest->recvfid == param_srcfid)
				{
					/* only send results to the node which sends us params */
					fstate->nodeid.nodeid = param_srcnodeid;
					FragmentSendTuple(fstate->extraSendBuffers[i], slot,
									  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
				}
				/* or we skip this dest fragment */
			}
			else
			{
				foreach(target, dest->targetNodes)
				{
					fstate->nodeid.nodeid = lfirst_int(target);
					if (recvMsgNodes == NULL || bms_is_member(fstate->nodeid.nodeid, recvMsgNodes))
						FragmentSendTuple(fstate->extraSendBuffers[i], slot,
										  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
				}
			}
			i++;
		}
	}
	else if (plan->roundrobin_distributed == true)
	{
		int nodeid;
		/*
		 * Send slot in roundrobin, the other side should do replicate
		 */
		if (fstate->roundrobin_node >= list_length(plan->targetNodes))
			fstate->roundrobin_node = 0;

		nodeid = list_nth_int(plan->targetNodes, fstate->roundrobin_node);
		if (recvMsgNodes != NULL)
		{
			while (!bms_is_member(nodeid, recvMsgNodes))
			{
				fstate->roundrobin_node++;
				if (fstate->roundrobin_node >= list_length(plan->targetNodes))
					fstate->roundrobin_node = 0;
				nodeid = list_nth_int(plan->targetNodes, fstate->roundrobin_node);
			}
		}

		fstate->nodeid.nodeid = nodeid;
		FragmentSendTuple(fstate->sendBuffer, slot,
						  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);

		fstate->roundrobin_node++;
	}
	else if (plan->roundrobin_replicate == true ||
			 (is_data_skew == true && is_retain == false))
	{
		/*
		 * Broadcase slot : the other side may be roundroubin or local send
		 */
		foreach(lc, plan->targetNodes)
		{
			fstate->nodeid.nodeid = lfirst_int(lc);
			if (recvMsgNodes == NULL || bms_is_member(fstate->nodeid.nodeid, recvMsgNodes))
				FragmentSendTuple(fstate->sendBuffer, slot,
								  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
	}
	else if (is_data_skew == true && is_retain == true)
	{
		/*
		 * Send slot local, the other side should do replicate
		 */
		fstate->nodeid.nodeid = PGXCNodeId - 1;
		if (recvMsgNodes == NULL || bms_is_member(fstate->nodeid.nodeid, recvMsgNodes))
			FragmentSendTuple(fstate->sendBuffer, slot,
							  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
	}
	else if (list_length(plan->distributionRestrict) > 1)
	{
		int nvirtualdop = Max(plan->num_virtualdop, 1);

		GetDataRouting(fstate, slot);

		/* Send slot data to the target nodes */
		nodeidx = get_node_idx(nvirtualdop, fstate->nodeid);

		if (fstate->nodeid.nodeid != MAX_UINT16 &&
			list_member_int(plan->distributionRestrict, fstate->nodeid.nodeid) &&
			(recvMsgNodes == NULL || bms_is_member(nodeidx, recvMsgNodes)))
			FragmentSendTuple(fstate->sendBuffer, slot, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
	}
	else
	{
		/*
		 * mask target nodes which are coordinator nodes
		 */
		if (CoordinatorNode == plan->targetNodeType)
		{
			Assert((fstate->sendBuffer->flag & SEND_BUFFER_COORD));
			FragmentSendTuple(fstate->sendBuffer, slot, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
		else if (plan->localSend)
		{
			if (recvMsgNodes == NULL || bms_is_member(fstate->nodeid.nodeid, recvMsgNodes))
				FragmentSendTuple(fstate->sendBuffer, slot,
								  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
		else if (IsFragmentInSubquery(plan))
		{
			Assert(recvMsgNodes == NULL);
			/* only send results to the node which sends us params */
			fstate->nodeid.nodeid = param_srcnodeid;
			FragmentSendTuple(fstate->sendBuffer, slot, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
		else
		{
			/* broadcast case */
			if (fstate->broadcast_flag)
			{
				Assert(recvMsgNodes == NULL);
				FragmentSendTupleBroadcast(fstate, slot);
			}
			else if (recvMsgNodes == NULL)
			{
				foreach(lc, plan->targetNodes)
				{
					fstate->nodeid.nodeid = lfirst_int(lc);
					FragmentSendTupleBroadcast(fstate, slot);
				}
			}
			else
			{
				int nodeidx = -1;
				while ((nodeidx = bms_next_member(recvMsgNodes, nodeidx)) > -1)
				{
					NodeId nodeid;
					if (plan->num_virtualdop > 1)
					{
						nodeid.nodeid = nodeidx / plan->num_virtualdop;
						nodeid.virtualid = nodeidx % plan->num_virtualdop;
					}
					else
					{
						nodeid.nodeid = nodeidx;
						nodeid.virtualid = 0;
					}
					if (list_member_int(plan->targetNodes, nodeid.nodeid))
						FragmentSendTuple(fstate->sendBuffer, slot, fstate->combiner.ss.ps.state->queryid, nodeid);
				}
			}
		}
	}

	MemoryContextSwitchTo(savecxt);
}

static void
StoreParamData(char **msg, Oid ptype, Datum *value, bool *isnull,
			   MemoryContext tmpcxt)
{
	*isnull = *(bool *)(*msg);
	*msg += sizeof(bool);

	if (*isnull)
	{
		*value = (Datum) 0;
		DEBUG_FRAG(elog(DEBUG1, "StoreParamData null"));
	}
	else
	{
		Oid		typinput, typioparam;
		char   *pstring;
		uint32  len;
		MemoryContext oldcxt;

		pstring = *msg;
		len = strlen(pstring) + 1;
		*msg += len;

		DEBUG_FRAG(elog(DEBUG1, "StoreParamData %s len %d", pstring, len));

		/* Get info needed to input the value */
		getTypeInputInfo(ptype, &typinput, &typioparam);

		oldcxt = MemoryContextSwitchTo(tmpcxt);
		*value = OidInputFunctionCall(typinput, pstring, typioparam, -1);
		MemoryContextSwitchTo(oldcxt);
	}
}

static bool 
FragmentRecvParams(RemoteFragmentState *fstate)
{
	PlanState			*pstate = &fstate->combiner.ss.ps;
	RemoteSubplan		*subplan = (RemoteSubplan *)pstate->plan;
	RecvBuffer			*recvBuffer = fstate->recvParamBuffer;
	EState				*estate = pstate->state;
	int					 nparams = subplan->nparams;
	RemoteParam			*remoteparams = (RemoteParam *) subplan->remoteparams;
	int					 i;
	char				*msg, *pos;

	CHECK_FOR_INTERRUPTS();
	MemoryContextReset(fstate->tmp_cxt);

	msg = (char *) TupleQueueReceiveMessage(recvBuffer->queue, 0, NULL);
	if (!msg)
		return false;
	pos = msg;
	/* skip a useless length field */
	pos += sizeof(uint32);
	param_srcnodeid = pg_ntoh32(*(int *) pos);
	pos += sizeof(int);
	param_srcfid = pg_ntoh32(*(int *) pos);
	pos += sizeof(int);

	DEBUG_FRAG(elog(DEBUG1, "FragmentRecvParams from nodeid:%d, qid_ts_node:%ld, "
							"qid_seq:%ld, fid:%d, my nodeid:%d, fid:%d flevel:%d, nparams:%d",
					param_srcnodeid, pstate->state->queryid.timestamp_nodeid,
					pstate->state->queryid.sequence, param_srcfid,
					PGXCNodeId - 1, subplan->param_fid, subplan->flevel, nparams));

	for (i = 0; i < nparams; i++)
	{
		RemoteParam *rparam = &remoteparams[i];
		if (rparam->paramkind == PARAM_EXEC)
		{
			ParamExecData *param;
			param = &(estate->es_param_exec_vals[rparam->paramid]);
			StoreParamData(&pos, rparam->paramtype,
						   &param->value, &param->isnull, fstate->tmp_cxt);
			param->ptype = rparam->paramtype;
			param->done = true;
			param->execPlan = NULL;

			pstate->chgParam = bms_add_member(pstate->chgParam, rparam->paramid);
		}
	}

	return true;
}

/*
 * Although upper node got its data and stopped in advance,
 * we still need to drain out all left tuples from low-level fragments.
 */
static void
DrainOutTuple(PlanState *planstate)
{
	int count = 0;

	TupleTableSlot *slot;
	for (;;)
	{
		slot = ExecRemoteFragment(planstate);
		if(TupIsNull(slot))
		{
			break;
		}
		count++;
	}

	DEBUG_FRAG(elog(LOG, "DrainOutTuple %d tuples", count));
}

static void 
ExecReScanSendRemoteFragment(RemoteFragmentState *fstate)
{
	/*
	 * The tqueue thread of recvParamBuffer will keep working until end of
	 * query (they won't get any EOF from upper node) so don't touch
	 * recvParamBuffer here.
	 */

	/* rescan lefttree */
	if (outerPlanState(fstate))
		ExecReScan(outerPlanState(fstate));
}

static void
ExecReScanRecvRemoteFragment(RemoteFragmentState *fstate)
{
	PlanState *planstate = &fstate->combiner.ss.ps;
	RecvBuffer *recvBuffer = fstate->recvBuffer;
	RemoteSubplan *subplan = (RemoteSubplan *) planstate->plan;
	TupleTableSlot *resultSlot = planstate->ps_ResultTupleSlot;

	if (!fstate->finish_init)
		return;

	if (subplan->distributionRestrict &&
		!list_member_int(subplan->distributionRestrict, PGXCNodeId - 1))
		return;

	if (subplan->localSend &&
		!list_member_int(subplan->nodeList, PGXCNodeId - 1))
		return;

	if (IsFragmentInSubquery(subplan) && !fstate->sendParam)
		return;

	DrainOutTuple(planstate);

	fstate->needRecvData = true;

	fstate->num_finish = 0;

	if (recvBuffer->tuplesortstate)
	{
		tuplesort_end(recvBuffer->tuplesortstate);
		recvBuffer->tuplesortstate = NULL;
	}

	if (recvBuffer->queue)
	{
		DestroyTupleQueueReceiver(recvBuffer->queue, recvBuffer->queue->owner, true);
		recvBuffer->queue = NULL;
	}

	FragmentRecvStateInit(fstate);

	FragmentSendStateDestroy(fstate->sendParamBuffer);
	fstate->sendParam = false;

	ExecClearTuple(resultSlot);
}

void
ExecReScanRemoteFragment(RemoteFragmentState *node)
{
	int fid = ((RemoteSubplan *)node->combiner.ss.ps.plan)->fid;
	PlanState *pstate = &node->combiner.ss.ps;
	ResponseCombiner *combiner = &node->combiner;
	RemoteSubplan *plan = (RemoteSubplan *) pstate->plan;

	if (plan->protocol == ProtocolPQ)
	{
		/*
		 * Consume any possible pending input
		 */
		pgxc_connections_cleanup(combiner, false);

		/* misc cleanup */
		combiner->command_complete_count = 0;
		combiner->description_count = 0;
	}

	if (node->sendFragment)
	{
		DEBUG_FRAG(elog(LOG, "rescan send fragment qid_ts_node:%ld, qid_seq:%ld, fid:%d",
						pstate->state->queryid.timestamp_nodeid,
						pstate->state->queryid.sequence, fid));
		return ExecReScanSendRemoteFragment(node);
	}
	else
	{
		DEBUG_FRAG(elog(LOG, "rescan recv fragment qid_ts_node:%ld, qid_seq:%ld, fid:%d",
						pstate->state->queryid.timestamp_nodeid,
						pstate->state->queryid.sequence, fid));
		return ExecReScanRecvRemoteFragment(node);
	}
}

/*
 * Send complete message.
 */
static void
FragmentSendCompleteMsg(RemoteFragmentState *fstate)
{
	RemoteSubplan *plan = (RemoteSubplan *)((PlanState *)fstate)->plan;
	ListCell *lc;
	int i;

	if (list_length(plan->dests) > 0)
	{
		ListCell *dest_lc, *target;

		i = 0;
		foreach(dest_lc, plan->dests)
		{
			FragmentDest *dest = lfirst_node(FragmentDest, dest_lc);

			if (IsFragmentInSubquery(plan))
			{
				if (dest->recvfid == param_srcfid)
				{
					/* only send results to the node which sends us params */
					fstate->nodeid.nodeid = param_srcnodeid;
					FragmentSendTuple(fstate->extraSendBuffers[i], NULL,
									  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
				}
				/* or we skip this dest fragment */
			}
			else
			{
				foreach(target, dest->targetNodes)
				{
					fstate->nodeid.nodeid = lfirst_int(target);
					FragmentSendTuple(fstate->extraSendBuffers[i], NULL,
									  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
				}
			}
			i++;
		}
	}
	else if (list_length(plan->distributionRestrict) > 1 || plan->distributionType == LOCATOR_TYPE_MIXED)
	{
		/* Redistribution case */
		foreach(lc, plan->distributionNodes)
		{
			fstate->nodeid.nodeid = lfirst_int(lc);

			if (list_member_int(plan->distributionRestrict, fstate->nodeid.nodeid) &&
				(!IsFragmentInSubquery(plan) || param_srcnodeid == fstate->nodeid.nodeid))
			{
				if (plan->num_virtualdop > 1)
				{
					for (i = 0; i < plan->num_virtualdop; i++)
					{
						fstate->nodeid.virtualid = i;
						FragmentSendTuple(fstate->sendBuffer, NULL,
										  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
					}
				}
				else
				{
					FragmentSendTuple(fstate->sendBuffer, NULL,
									  fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
				}
			}
		}
	}
	else
	{
		/*
		 * mask target nodes which are coordinator nodes
		 */
		if (CoordinatorNode == plan->targetNodeType)
		{
			Assert((fstate->sendBuffer->flag & SEND_BUFFER_COORD));
			FragmentSendTuple(fstate->sendBuffer, NULL, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
		else if (plan->localSend)
		{
			FragmentSendTuple(fstate->sendBuffer, NULL, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
		else if (IsFragmentInSubquery(plan))
		{
			fstate->nodeid.nodeid = param_srcnodeid;
			FragmentSendTuple(fstate->sendBuffer, NULL, fstate->combiner.ss.ps.state->queryid, fstate->nodeid);
		}
		else
		{
			/* broadcast case */
			if (fstate->broadcast_flag)
			{
				FragmentSendTupleBroadcast(fstate, NULL);
			}
			else
			{
				foreach(lc, plan->targetNodes)
				{
					fstate->nodeid.nodeid = lfirst_int(lc);
					FragmentSendTupleBroadcast(fstate, NULL);
				}
			}
		}
	}

	DEBUG_FRAG(elog(LOG, "[FragmentSendCompleteMsg] fid %d send complete message",
					plan->fid));
}

/*
 * Send complete message to special node.
 */
static void
FragmentSendCompleteMsgToNode(RemoteFragmentState *fstate, NodeId nodeid)
{
	RemoteSubplan *plan = (RemoteSubplan *)((PlanState *)fstate)->plan;
	int i;

	if (list_length(plan->dests) > 0)
	{
		ListCell *dest_lc;

		i = 0;
		foreach(dest_lc, plan->dests)
		{
			FragmentSendTuple(fstate->extraSendBuffers[i], NULL,
							  fstate->combiner.ss.ps.state->queryid, nodeid);
			i++;
		}
	}
	else if (list_length(plan->distributionRestrict) > 1)
	{
		/* Redistribution case */
		if (list_member_int(plan->distributionRestrict, nodeid.nodeid) &&
			(!IsFragmentInSubquery(plan) || param_srcnodeid == nodeid.nodeid))
		{
			FragmentSendTuple(fstate->sendBuffer, NULL,
							  fstate->combiner.ss.ps.state->queryid, nodeid);
		}
	}
	else
	{
		Assert(!IsFragmentInSubquery(plan));

		/*
		 * mask target nodes which are coordinator nodes
		 */
		if (CoordinatorNode == plan->targetNodeType)
		{
			Assert((fstate->sendBuffer->flag & SEND_BUFFER_COORD));
			Assert(fstate->nodeid.nodeid == nodeid.nodeid);
			FragmentSendTuple(fstate->sendBuffer, NULL, fstate->combiner.ss.ps.state->queryid, nodeid);
		}
		else if (plan->localSend)
		{
			if (fstate->nodeid.nodeid == nodeid.nodeid)
				FragmentSendTuple(fstate->sendBuffer, NULL, fstate->combiner.ss.ps.state->queryid, nodeid);
		}
		else
		{
			/* broadcast case */
			if (list_member_int(plan->targetNodes, nodeid.nodeid))
				FragmentSendTuple(fstate->sendBuffer, NULL, fstate->combiner.ss.ps.state->queryid, nodeid);
		}
	}

	DEBUG_FRAG(elog(LOG, "[FragmentSendCompleteMsgToNode] fid %d send complete message to %d(%d)",
					plan->fid, nodeid.nodeid, nodeid.virtualid));
}

static void
FragmentCacheSendCatchUp(RemoteFragmentState *fstate, NodeId nodeid)
{
	RemoteSubplan   *subplan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	int				nvirtualdop = Max(subplan->num_virtualdop, 1);
	int				nodeidx = get_node_idx(nvirtualdop, nodeid);
	Tuplestorestate *datatable;
	Bitmapset	    *node;
	bool			need_distribute;
	int 			readptr;
	TupleTableSlot  *slot;

	if (fstate->num_table > 1)
	{
		datatable = fstate->cacheSendTable[nodeidx];
		node = NULL;
		need_distribute = false;
	}
	else
	{
		datatable = fstate->cacheSendTable[0];
		node = bms_make_singleton(nodeidx);
		need_distribute = true;
	}
	Assert(datatable != NULL);

	if (tuplestore_tuple_count(datatable) == 0)
		return;

	readptr = tuplestore_alloc_read_pointer(datatable, 0);
	tuplestore_select_read_pointer(datatable, readptr);

	slot = MakeSingleTupleTableSlot(fstate->combiner.ss.ps.ps_ResultTupleDesc);
	while (tuplestore_gettupleslot(datatable, true, false, slot))
	{
		CHECK_FOR_INTERRUPTS();
		if (end_query_requested)
			break;

		if (!need_distribute)
			FragmentSendTuple(fstate->sendBuffer, slot,
								fstate->combiner.ss.ps.state->queryid, nodeid);
		else
			FragmentRedistributeData(fstate, slot, node);
	}
	ExecDropSingleTupleTableSlot(slot);

	if (!need_distribute)
	{
		tuplestore_end(fstate->cacheSendTable[nodeidx]);
		fstate->cacheSendTable[nodeidx] = NULL;
	}
	else
	{
		tuplestore_select_read_pointer(datatable, 0);
		bms_free(node);
	}
}

/*
 * Send message to cancel cache send data.
 */
static void
FragmentSendMsg(RemoteFragmentState *fstate)
{
	PlanState			*pstate = &fstate->combiner.ss.ps;
	RemoteSubplan		*subplan = (RemoteSubplan *) pstate->plan;
	SendBuffer			*sendBuffer = fstate->sendParamBuffer;
	ListCell			*lc;
	NodeId				nodeid = {0};
	NodeId				srcnodeid = {0};

	if (fstate->sendParamBuffer == NULL)
	{
		sendBuffer = FragmentSendStateInit(subplan->param_fid,
										   list_length(subplan->targetNodes), 1,
										   SEND_BUFFER_PARAM);
	}

	srcnodeid.nodeid = PGXCNodeId - 1;
	if (subplan->num_virtualdop > 1 && IsParallelWorker())
		srcnodeid.virtualid = ParallelWorkerNumber;

	foreach(lc, subplan->nodeList)
	{
		nodeid.nodeid = lfirst_int(lc);

		/* should never send parameters to CN */
		Assert(!NodeIsCoordinator(nodeid.nodeid));

		/* Send node id */
		FragmentSendData(sendBuffer, fstate->combiner.ss.ps.state->queryid,
						 nodeid, (char *) &srcnodeid, sizeof(NodeId), true);

		FragmentSendDone(sendBuffer, nodeid, true, true);

		DEBUG_FRAG(elog(DEBUG1, "FragmentSendMsg to nodeid:%d, qid_ts_node:%ld, qid_seq:%ld, "
								"src nodeid:%d, src virtualid:%d, fid:%d flevel:%d",
						nodeid.nodeid, pstate->state->queryid.timestamp_nodeid,
						pstate->state->queryid.sequence, srcnodeid.nodeid,
						srcnodeid.virtualid, sendBuffer->fid, subplan->flevel));
	}

	if (fstate->sendParamBuffer == NULL)
	{
		FragmentSendStateDestroy(sendBuffer);
	}

	fstate->sendMsg = true;
}

/*
 * Receive message to cancel cache send data.
 */
static bool
FragmentRecvMsg(RemoteFragmentState *fstate, bool all, bool complete)
{
	PlanState			*pstate = &fstate->combiner.ss.ps;
	RemoteSubplan		*subplan = (RemoteSubplan *) pstate->plan;
	RecvBuffer			*recvBuffer = fstate->recvParamBuffer;
	char				*msg, *pos;
	NodeId				srcnodeid;
	int					nodeidx;
	int					nvirtualdop = Max(subplan->num_virtualdop, 1);
	int					num_nodes = list_length(subplan->targetNodes) * nvirtualdop;

	if (!all)
	{
		bool loop = false;

		do
		{
			bool block = false;
			loop = bms_is_empty(fstate->recvMsgNodes);

			CHECK_FOR_INTERRUPTS();
			if (end_query_requested)
				return true;

			msg = (char *) TupleQueueReceiveMessage(recvBuffer->queue, 0, &block);
			if (!block)
			{
				if (!msg)
					return true;
				pos = msg;
				/* skip a useless length field */
				pos += sizeof(uint32);
				/* src nodeid */
				srcnodeid = *(NodeId *) pos;

				DEBUG_FRAG(elog(DEBUG1, "FragmentRecvMsg from nodeid:%d, virtualid:%d, "
										"qid_ts_node:%ld, qid_seq:%ld, my nodeid:%d, "
										"fid:%d, flevel:%d",
								srcnodeid.nodeid, srcnodeid.virtualid,
								pstate->state->queryid.timestamp_nodeid,
								pstate->state->queryid.sequence,
								PGXCNodeId - 1, subplan->param_fid, subplan->flevel));

				nodeidx = get_node_idx(nvirtualdop, srcnodeid);
				if (!bms_is_member(nodeidx, fstate->recvMsgNodes))
				{
					fstate->recvMsgNodes = bms_add_member(fstate->recvMsgNodes, nodeidx);
					FragmentCacheSendCatchUp(fstate, srcnodeid);
					if (complete)
						FragmentSendCompleteMsgToNode(fstate, srcnodeid);
				}
			}
			else if (loop)
			{
				wait_event_fn_start(subplan->param_fid, WAIT_EVENT_RECEIVE_MSG);
				pg_usleep(1000L);
				wait_event_fn_end();
			}
		} while (loop);
	}
	else
	{
		while (true)
		{
			CHECK_FOR_INTERRUPTS();
			if (end_query_requested)
				return true;

			msg = (char *) TupleQueueReceiveMessage(recvBuffer->queue, 0, NULL);
			if (!msg)
				return true;
			pos = msg;
			/* skip a useless length field */
			pos += sizeof(uint32);
			/* src nodeid */
			srcnodeid = *(NodeId *) pos;

			DEBUG_FRAG(elog(DEBUG1, "FragmentRecvMsg from nodeid:%d, virtualid:%d, "
									"qid_ts_node:%ld, qid_seq:%ld, my nodeid:%d, "
									"fid:%d flevel:%d",
							srcnodeid.nodeid, srcnodeid.virtualid,
							pstate->state->queryid.timestamp_nodeid,
							pstate->state->queryid.sequence, PGXCNodeId - 1,
							subplan->param_fid, subplan->flevel));

			nodeidx = get_node_idx(nvirtualdop, srcnodeid);
			fstate->recvMsgNodes = bms_add_member(fstate->recvMsgNodes, nodeidx);
			if (bms_num_members(fstate->recvMsgNodes) == num_nodes)
				break;
		}
	}

	return bms_num_members(fstate->recvMsgNodes) == num_nodes;
}

static void
FragmentCacheSendOut(RemoteFragmentState *fstate)
{
	Tuplestorestate *datatable = fstate->cacheSendTable[0];
	TupleTableSlot *slot;

	Assert(datatable != NULL);
	slot = MakeSingleTupleTableSlot(fstate->combiner.ss.ps.ps_ResultTupleDesc);
	while (tuplestore_gettupleslot(datatable, true, false, slot))
	{
		CHECK_FOR_INTERRUPTS();
		if (end_query_requested)
			break;

		FragmentRedistributeData(fstate, slot, NULL);
	}
	ExecDropSingleTupleTableSlot(slot);

	tuplestore_end(fstate->cacheSendTable[0]);
	fstate->cacheSendTable[0] = NULL;
}

static void
FragmentCacheSendIn(RemoteFragmentState *fstate, TupleTableSlot *slot, int nodeidx)
{
	Tuplestorestate *datatable = fstate->cacheSendTable[nodeidx];

	Assert(datatable != NULL);
	tuplestore_puttupleslot(datatable, slot);
}

static void
FragmentCacheSendInOrSend(RemoteFragmentState *fstate, TupleTableSlot *slot)
{
	Bitmapset *recvMsgNodes = fstate->recvMsgNodes;

	if (fstate->num_table > 1)
	{
		RemoteSubplan *plan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
		int nvirtualdop = Max(plan->num_virtualdop, 1);
		int nodeidx;

		GetDataRouting(fstate, slot);

		nodeidx = get_node_idx(nvirtualdop, fstate->nodeid);

		if (bms_is_member(nodeidx, recvMsgNodes))
			FragmentSendTuple(fstate->sendBuffer,
							  slot,
							  fstate->combiner.ss.ps.state->queryid,
							  fstate->nodeid);
		else
			FragmentCacheSendIn(fstate, slot, nodeidx);
	}
	else
	{
		FragmentRedistributeData(fstate, slot, recvMsgNodes);
		FragmentCacheSendIn(fstate, slot, 0);
	}
}

static void
FragmentCacheSendInit(RemoteFragmentState *fstate)
{
	RemoteSubplan *plan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	int nvirtualdop = Max(plan->num_virtualdop, 1);
	SendBuffer *sendBuffer;
	MemoryContext oldcxt;
	int num_table = 1;
	int per_work_mem = work_mem;
	int i;

	/* distribute case */
	if (!IsParallelWorker() && !IsFragmentInSubquery(plan) &&
		plan->distributionType == LOCATOR_TYPE_SHARD &&
		list_length(plan->distributionRestrict) > 1)
	{
		num_table = NumDataNodes * nvirtualdop;
		/* at least 64 as work_mem setting */
		per_work_mem = Max(work_mem / num_table, 64);
	}

	/* use per_query context */
	oldcxt = MemoryContextSwitchTo(fstate->cxt->parent);

	fstate->num_table = num_table;
	fstate->cacheSendTable = (Tuplestorestate **) palloc0(sizeof(Datum) * num_table);

	if (list_length(plan->dests) > 0)
		sendBuffer = fstate->extraSendBuffers[0];
	else if (fstate->broadcast_flag)
		sendBuffer = fstate->sendBufferBroadcast;
	else
		sendBuffer = fstate->sendBuffer;

	if (force_transfer_datarow || (sendBuffer->flag & SEND_BUFFER_DATAROW))
	{
		for (i = 0; i < num_table; i++)
		{
			fstate->cacheSendTable[i] = tuplestore_begin_datarow(false, per_work_mem, NULL, NULL, NULL);
		}
	}
	else
	{
		for (i = 0; i < num_table; i++)
		{
			fstate->cacheSendTable[i] = tuplestore_begin_heap(false, false, per_work_mem);
		}
	}

	MemoryContextSwitchTo(oldcxt);
}

static TupleTableSlot *
ExecSendRemoteFragment(PlanState *pstate)
{
	RemoteFragmentState *fstate = castNode(RemoteFragmentState, pstate);
	RemoteSubplan *plan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;
	TupleTableSlot *slot;
	bool cacheSend = plan->cacheSend;

	DEBUG_FRAG(elog(LOG, "ExecSendRemoteFragment qid_ts_node:%ld, qid_seq:%ld, fid:%d",
					pstate->state->queryid.timestamp_nodeid,
					pstate->state->queryid.sequence, plan->fid));

	if (IsFragmentInSubquery(plan))
	{
recv_parameters:
		/*
		 * Try to receive params from upper node, failure means we got end
		 * query request, and don't try to send complete msg here, because
		 * param_srcnodeid is not decided yet, and upper node don't need
		 * that too.
		 */
		if (!FragmentRecvParams(fstate))
			return NULL;

		if (pstate->instrument)
			InstrStopNode(pstate->instrument, 0);

		ExecReScan(pstate);

		if (pstate->instrument)
		{
			InstrStartFragment(pstate->instrument, &(pstate->state->executor_starttime));
			InstrStartNode(pstate->instrument);
		}
	}

	/*
	 * Check whether upper layer fragments request
	 * us not to send data to them.
	 */
	if (end_query_requested)
	{
		FragmentSendCompleteMsg(fstate);
		if (fstate->need_to_scan_locally && fstate->parallel_status)
		{
			fstate->need_to_scan_locally = false;

			/* we should tell workers that leader is not participating */
			fstate->pei->pcxt->leader_participate = false;
			ExecParallelAdjustDSM(pstate, fstate->pei->pcxt);

			/* now send end request to workers and wait */
			WaitForParallelRemoteWorkerDone(fstate);
		}
		return NULL;
	}

	/* first time through */
	if (cacheSend && fstate->cacheSendTable == NULL)
		FragmentCacheSendInit(fstate);

	/* Advance subplan in a loop until all data has been sent */
	for (;;)
	{
		/* Reset the per-output-tuple exprcontext */
		ResetPerTupleExprContext(pstate->state);

		/* Install our DSA area while executing the plan. */
		if (fstate->need_to_scan_locally)
			pstate->state->es_query_dsa = fstate->pei ?
										  fstate->pei->area : NULL;

		slot = ExecProcNode(outerPlanState(fstate));

		/* Reset DSA area. */
		if (fstate->need_to_scan_locally)
			pstate->state->es_query_dsa = NULL;

		if (plan->protocol == ProtocolPQ)
		{
			RemoteDataRow datarow;

			if (!TupIsNull(slot) && !slot->tts_datarow)
			{
				PG_TRY();
				{
					set_portable_output(true);
					datarow = ExecCopySlotDatarow(slot, NULL);
					set_portable_output(false);
				}
				PG_CATCH();
				{
					set_portable_output(false);
					PG_RE_THROW();
				}
				PG_END_TRY();

				slot->tts_datarow = datarow;
				slot->tts_shouldFreeRow = true;
				slot->tts_tupleDescriptor->cn_penetrate_tuple = true;
			}

			return slot;
		}

		/* If slot is null, send complete message to each redistribution nodes */
		if (TupIsNull(slot) || end_query_requested)
		{
			if (cacheSend && !end_query_requested)
			{
				int nodeidx = -1;
				while ((nodeidx = bms_next_member(fstate->recvMsgNodes, nodeidx)) > -1)
				{
					NodeId nodeid;
					if (plan->num_virtualdop > 1)
					{
						nodeid.nodeid = nodeidx / plan->num_virtualdop;
						nodeid.virtualid = nodeidx % plan->num_virtualdop;
					}
					else
					{
						nodeid.nodeid = nodeidx;
						nodeid.virtualid = 0;
					}
					FragmentSendCompleteMsgToNode(fstate, nodeid);
				}

				while (cacheSend)
				{
					CHECK_FOR_INTERRUPTS();
					if (end_query_requested)
					{
						FragmentSendCompleteMsg(fstate);
						break;
					}

					if (IsParallelWorker())
					{
						cacheSend = fstate->parallel_status->cacheSend;
						if (!cacheSend)
						{
							FragmentCacheSendOut(fstate);
							FragmentSendCompleteMsg(fstate);
						}
					}
					else
					{
						cacheSend = !FragmentRecvMsg(fstate, false, true);
					}

					if (cacheSend)
					{
						wait_event_fn_start(plan->param_fid, WAIT_EVENT_RECEIVE_MSG);
						pg_usleep(1000L);
						wait_event_fn_end();
					}
				}
			}
			else
				FragmentSendCompleteMsg(fstate);

			if (fstate->need_to_scan_locally && fstate->parallel_status)
			{
				fstate->need_to_scan_locally = false;
				WaitForParallelRemoteWorkerDone(fstate);
			}
			break;
		}

		if (cacheSend)
		{
			if (IsParallelWorker())
			{
				cacheSend = fstate->parallel_status->cacheSend;
				if (cacheSend)
				{
					FragmentCacheSendIn(fstate, slot, 0);
				}
				else
				{
					FragmentCacheSendOut(fstate);
					FragmentRedistributeData(fstate, slot, NULL);
				}
			}
			else
			{
				cacheSend = !FragmentRecvMsg(fstate, false, false);
				if (cacheSend)
				{
					FragmentCacheSendInOrSend(fstate, slot);
				}
				else
				{
					FragmentRedistributeData(fstate, slot, NULL);
				}
			}
		}
		else
		{
			FragmentRedistributeData(fstate, slot, NULL);
		}

		if (pstate->instrument)
		{
			InstrStopFragment(pstate->instrument, &(pstate->state->executor_starttime));
			InstrStopNode(pstate->instrument, 1);
			InstrStartFragment(pstate->instrument, &(pstate->state->executor_starttime));
			InstrStartNode(pstate->instrument);
		}
	}

	if (end_query_requested)
		return NULL;

	if (IsFragmentInSubquery(plan))
		goto recv_parameters;

	return NULL;
}

static void
SendParamData(StringInfo str, Oid ptype, Datum value, bool isnull,
			  MemoryContext tmpcxt)
{
	/* append isnull */
	appendBinaryStringInfo(str, (const char *) &isnull, sizeof(bool));

	if (isnull)
	{
		DEBUG_FRAG(elog(DEBUG1, "SendParamData null"));
	}
	else
	{
		Oid		typOutput;
		bool	typIsVarlena;
		Datum	pval;
		char   *pstring;
		uint32  len;
		MemoryContext oldcxt;

		/* Get info needed to output the value */
		getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

		/* switch to temporary context */
		oldcxt = MemoryContextSwitchTo(tmpcxt);
		/*
		 * If we have a toasted datum, forcibly detoast it here to avoid
		 * memory leakage inside the type's output routine.
		 */
		if (typIsVarlena)
			pval = PointerGetDatum(PG_DETOAST_DATUM(value));
		else
			pval = value;

		/* Convert Datum to string */
		pstring = OidOutputFunctionCall(typOutput, pval);
		MemoryContextSwitchTo(oldcxt);

		len = strlen(pstring) + 1;
		DEBUG_FRAG(elog(DEBUG1, "SendParamData %s len %d", pstring, len));

		/* append datum, and we need that '\0' to separate each param */
		appendBinaryStringInfo(str, pstring, len);
	}
}

/* Sending parameter to SubQuery */
static void 
FragmentSendParams(RemoteFragmentState *fstate)
{
	PlanState			*pstate = &fstate->combiner.ss.ps;
	RemoteSubplan		*subplan = (RemoteSubplan *)pstate->plan;
	SendBuffer			*sendBuffer = fstate->sendParamBuffer;
	EState				*estate = pstate->state;
	int					 nparams = subplan->nparams;
	RemoteParam			*remoteparams = (RemoteParam *) subplan->remoteparams;
	ListCell			*lc;
	int					 i;
	StringInfoData		 str;
	NodeId				 nodeid = {0};

	MemoryContextReset(fstate->tmp_cxt);

	initStringInfo(&str);

	foreach(lc, subplan->nodeList)
	{
		uint32 n32;
		nodeid.nodeid = lfirst_int(lc);

		/* should never send parameters to CN */
		Assert(!NodeIsCoordinator(nodeid.nodeid));

		/* Send node id */
		n32 = pg_hton32(PGXCNodeId - 1);
		appendBinaryStringInfo(&str, (char *) &n32, sizeof(uint32));
		/* Send local fid */
		n32 = pg_hton32(local_fid);
		appendBinaryStringInfo(&str, (char *) &n32, sizeof(uint32));

		DEBUG_FRAG(elog(DEBUG1, "FragmentSendParams to nodeid:%d, qid_ts_node:%ld, qid_seq:%ld, "
								"src nodeid:%d, fid:%d flevel:%d, nparams:%d",
						nodeid.nodeid, pstate->state->queryid.timestamp_nodeid,
						pstate->state->queryid.sequence, PGXCNodeId - 1,
						sendBuffer->fid, subplan->flevel, nparams));

		for (i = 0; i < nparams; i++)
		{
			RemoteParam *rparam = &remoteparams[i];

			if (rparam->paramkind == PARAM_EXEC)
			{
				ParamExecData *param;
				param = &(estate->es_param_exec_vals[rparam->paramid]);

				if (param->execPlan)
				{
					/* Parameter not evaluated yet, so go do it */
					ExecSetParamPlan((SubPlanState *) param->execPlan,
									 pstate->ps_ExprContext);
					/* ExecSetParamPlan should have processed this param... */
					Assert(param->execPlan == NULL);
				}
				SendParamData(&str, rparam->paramtype, param->value,
							  param->done ? param->isnull : true, fstate->tmp_cxt);
			}
		}

		if (MAXALIGN(str.len + sizeof(uint32)) > BLCKSZ - SizeOfFnPageHeaderData)
		{
			FragmentSendHuge(sendBuffer, fstate->combiner.ss.ps.state->queryid,
							 nodeid, str.data, str.len, true, 0);
		}
		else
		{
			FragmentSendData(sendBuffer, fstate->combiner.ss.ps.state->queryid,
							 nodeid, str.data, str.len, true);
		}

		FragmentSendDone(sendBuffer, nodeid, true, true);
	}

	/* release string data */
	pfree(str.data);

	fstate->sendParam = true;
}

/* ----------------------------------------------------------------
 *		ExecRecvRemoteFragment
 *
 *		Receive remote fragment data through forward node.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRecvRemoteFragment(PlanState *pstate)
{
	RemoteFragmentState *fstate = castNode(RemoteFragmentState, pstate);
	RemoteSubplan *subplan = castNode(RemoteSubplan, pstate->plan);
	RecvBuffer			*recvBuffer = fstate->recvBuffer;
	TupleTableSlot		*slot = pstate->ps_ResultTupleSlot;
	ResponseCombiner    *combiner = &fstate->combiner;
	void				*msg;

	CHECK_FOR_INTERRUPTS();

	/* Return the ending tuple in slot if receiver stopped. */
	if (!fstate->needRecvData)
	{
		slot->tts_flags |= TTS_FLAG_EMPTY;
		return slot;
	}

	if (!recvBuffer->queue)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("forward recv is not ready")));

	if (subplan->protocol == ProtocolPQ)
	{
		TupleTableSlot *result = NULL;

		//TOOD freetuple?
		result = FetchTuple(combiner);

		if (combiner->errorMessage)
			pgxc_node_report_error(combiner);

		if (TupIsNull(result))
			return ExecClearTuple(slot);

		/* extract data from message */
		slot_deform_datarow(result);
		pfree(slot->tts_datarow);
		slot->tts_datarow = NULL;
		slot->tts_shouldFreeRow = false;

		/* all done, mark it not empty */
		return ExecStoreVirtualTuple(result);
	}
	else if (subplan->protocol == ProtocolFN)
	{
		RunRemoteControllerWithFN(recvBuffer->queue, 0,RemoteFragmentGetController(fstate));
	}
	else
	{
		elog(ERROR, "unknown protocol %d", subplan->protocol);
	}

	msg = TupleQueueReceiveMessage(recvBuffer->queue, 0, NULL);
	if (!msg)
		return ExecClearTuple(slot);

	if (force_transfer_datarow || recvBuffer->transfer_datarow)
	{
		char	*pos = (char *) msg;
		/* ignore size */
		pos += sizeof(uint32);
		/* reset slot */
		ExecClearTuple(slot);
		/* extract data from message */
		slot_deform_datarow_internal(slot, pos, false);
		slot->tts_shouldFreeRow = true;
		/* all done, mark it not empty */
		return ExecStoreVirtualTuple(slot);
	}
	else
	{
		(void) ExecStoreMinimalTuple((MinimalTuple) msg, slot, false);
		return slot;
	}
}

TupleTableSlot *
ExecRemoteFragment(PlanState *pstate)
{
	RemoteFragmentState *node = castNode(RemoteFragmentState, pstate);
	RemoteSubplan *subplan = (RemoteSubplan * ) pstate->plan;
	ParallelWorkerStatus *parallel_status = node->parallel_status;
	bool asgather = false;
	ParallelContext *pcxt = NULL;
	bool cacheSend = subplan->cacheSend;

	DEBUG_FRAG(elog(DEBUG1, "ExecRemoteFragment qid_ts_node:%ld, qid_seq:%ld, "
							"fid:%d, cursor:%s, local:%d",
					pstate->state->queryid.timestamp_nodeid,
					pstate->state->queryid.sequence,
					subplan->fid, subplan->cursor, local_fid));

	if (pstate->state->es_epqTuple != NULL)
		elog(ERROR, "ExecRemoteFragment not support remote epq now.");

	if (!node->finish_init)
	{
		if (!IsFragmentInSubquery(subplan) && !subplan->cacheSend)
			elog(ERROR, "fragment %d is not ready for execution, local_fid %d", subplan->fid, local_fid);
		ExecFinishInitRemoteFragment(node);
	}

	if (unlikely(!node->finish_init))
		elog(ERROR, "fragment is not ready for execution");

	/*
	 * Initialize the parallel context and workers on first execution. We do
	 * this on first execution rather than during node initialization, as it
	 * needs to allocate a large dynamic segment, so it is better to do it
	 * only if it is really needed.
	 */
	if (!node->initialized && !IsParallelWorker())
	{
		EState	   *estate = node->combiner.ss.ps.state;

		/*
		 * Sometimes we might have to run without parallelism; but if parallel
		 * mode is active then we can try to fire up some workers.
		 */
		if (node->sendFragment && subplan->num_workers > 0 && estate->es_use_parallel_mode)
		{
			ParallelWorkerStatus *num_parallel_workers = NULL;
			int send_complete_msg;

			/* Initialize, or re-initialize, shared state needed by workers. */
			if (!node->pei)
				node->pei = ExecInitParallelPlan(&node->combiner.ss.ps,
												 estate,
												 subplan->initParam,
												 subplan->num_workers,
												 -1);
			else
				ExecParallelReinitialize(&node->combiner.ss.ps,
										 node->pei,
										 subplan->initParam);

			/*
			 * Register backend workers. We might not get as many as we
			 * requested, or indeed any at all.
			 */
			pcxt = node->pei->pcxt;
			LaunchParallelWorkers(pcxt, contain_remote_subplan(pstate->plan->lefttree));

			if (subplan->num_virtualdop > 0 &&
				subplan->num_workers != pcxt->nworkers_launched)
				elog(ERROR,
					"not enough workers for virtual dop: launched %d, needed %d",
					pcxt->nworkers_launched, subplan->num_workers);

			/* Send complete message myself for unstarted workers */
			send_complete_msg = subplan->num_workers - pcxt->nworkers_launched;
			while (send_complete_msg-- > 0)
				FragmentSendCompleteMsg(node);

			DEBUG_FRAG(elog(LOG, "ExecRemoteFragment launch %d workers",
							pcxt->nworkers_launched));

			/* Set up tuple queue readers to read the results. */
			if (pcxt->nworkers_launched > 0)
			{
#ifdef __OPENTENBASE__
				/* set up launched parallel workers' total number in shm */
				num_parallel_workers = GetParallelWorkerStatusInfo(pcxt->toc);
				num_parallel_workers->numLaunchedWorkers = pcxt->nworkers_launched;
#endif
#ifdef __OPENTENBASE_C__
				asgather = true;
				node->parallel_status = num_parallel_workers;
#endif
			}
			else
			{
				pstate->state->es_query_dsa = node->pei->area;
			}
		}

		if (node->pei && node->pei->pcxt)
		{
			pcxt = node->pei->pcxt;
			/* Run plan locally if no workers or enabled and not single-copy. */
			node->need_to_scan_locally = (pcxt->nworkers_launched == 0) ||
										 (subplan->num_virtualdop == 0 &&
										  parallel_leader_participation);

			pcxt->leader_participate = node->need_to_scan_locally;
			ExecParallelAdjustDSM(pstate, pcxt);

			while (cacheSend && !node->need_to_scan_locally)
			{
				if (FragmentRecvMsg(node, true, false))
				{
					cacheSend = false;
					node->parallel_status->cacheSend = false;
				}
			}
		}
	}

	node->initialized = true;

	if (asgather && !node->need_to_scan_locally)
	{
		FragmentSendCompleteMsg(node);
		WaitForParallelRemoteWorkerDone(node);
		return NULL;
	}

	if (node->sendFragment)
	{
		TupleTableSlot *slot;
		slot = ExecSendRemoteFragment(pstate);
		return slot;
	}
	else
	{
		TupleTableSlot *resultslot = NULL;

		if (subplan->localSend &&
			!list_member_int(subplan->nodeList, PGXCNodeId - 1))
			return NULL;

		/* If we are in SubQuery, send parameters down first. */
		if (IsFragmentInSubquery(subplan) && !node->sendParam)
			FragmentSendParams(node);

		if (subplan->cacheSend && !node->sendMsg)
			FragmentSendMsg(node);

		/* Return the ending tuple in slot if receiver stopped. */
		if (!node->needRecvData)
		{
			ExecEagerFreeRemoteFragment(pstate);
			return NULL;
		}

		if (subplan->sort)
		{
			resultslot = node->combiner.ss.ps.ps_ResultTupleSlot;

			if (node->recvBuffer->tuplesortstate == NULL)
			{
				node->recvBuffer->tuplesortstate = tuplesort_begin_merge_remote(
					resultslot->tts_tupleDescriptor,
					subplan->sort->numCols,
					subplan->sort->sortColIdx,
					subplan->sort->sortOperators,
					subplan->sort->sortCollations,
					subplan->sort->nullsFirst,
					node,
					work_mem);
			}

			if (tuplesort_gettupleslot((Tuplesortstate *) node->recvBuffer->tuplesortstate,
									   true, true, resultslot, NULL))
			{
				return resultslot;
			}
			ExecEagerFreeRemoteFragment(pstate);
			return NULL;
		}

		if (parallel_status == NULL && IsParallelWorker())
			elog(ERROR, "Incorrect parallel status");

		resultslot = ExecRecvRemoteFragment(pstate);
		if (unlikely(TupIsNull(resultslot)))
			ExecEagerFreeRemoteFragment(pstate);

		return resultslot;
	}

	/* Never reach here, just keep compiler quiet */
	Assert(false);
	return NULL;
}

static void
ExecEndSendRemoteFragment(RemoteFragmentState *fstate)
{
	int i;
	RemoteSubplan *plan = (RemoteSubplan *)fstate->combiner.ss.ps.plan;

	Assert(fstate->sendParamBuffer == NULL);
	FragmentSendStateDestroy(fstate->sendBuffer);
	FragmentSendStateDestroy(fstate->sendBufferBroadcast);

	if (fstate->extraSendBuffers)
	{
		Assert(plan->dests != NULL);
		for (i = 0; i < list_length(plan->dests); i++)
			FragmentSendStateDestroy(fstate->extraSendBuffers[i]);
	}

	if (fstate->recvParamBuffer)
	{
		Assert(fstate->recvParamBuffer->tuplesortstate == NULL);

		if (fstate->recvParamBuffer->queue)
		{
			DestroyTupleQueueReceiver(fstate->recvParamBuffer->queue,
									  fstate->recvParamBuffer->queue->owner, false);
			fstate->recvParamBuffer->queue = NULL;
		}
	}

	if (fstate->cacheSendTable)
	{
		for (i = 0; i < fstate->num_table; i++)
		{
			if (fstate->cacheSendTable[i])
			{
				tuplestore_end(fstate->cacheSendTable[i]);
				fstate->cacheSendTable[i] = NULL;
			}
		}
		pfree(fstate->cacheSendTable);
		fstate->cacheSendTable = NULL;
	}

	if (fstate->shared_info && IsParallelWorker())
	{
		Size cxt_mem;
		FragmentInstrumentation *si;

		Assert(ParallelWorkerNumber <= fstate->shared_info->num_workers);
		si = &fstate->shared_info->sinstrument[ParallelWorkerNumber];
		cxt_mem = MemoryContextMemAllocated(fstate->combiner.ss.ps.state->es_query_cxt, true);
		si->executor_mem = (cxt_mem + 1023) / 1024;
		cxt_mem = MemoryContextMemAllocated(fstate->cxt, true);
		si->fragment_mem = (cxt_mem + 1023) / 1024;
	}

	if (fstate->parallel_status && IsParallelWorker())
	{
		fstate->parallel_status->workerSendStatus[ParallelWorkerNumber] = true;
	}
}

static void
ExecEndRecvRemoteFragment(RemoteFragmentState *fstate)
{
	if (fstate->sendParamBuffer)
	{
		FragmentSendStateDestroy(fstate->sendParamBuffer);
		fstate->sendParamBuffer = NULL;
	}

	if (!fstate->needRecvData)
		return;

	if (fstate->shared_info && IsParallelWorker())
	{
		Size cxt_mem;
		FragmentInstrumentation *si;

		Assert(ParallelWorkerNumber <= fstate->shared_info->num_workers);
		si = &fstate->shared_info->sinstrument[ParallelWorkerNumber];
		cxt_mem = MemoryContextMemAllocated(fstate->cxt, true);
		si->fragment_mem = (cxt_mem + 1023) / 1024;
	}

	if (fstate->recvBuffer == NULL)
		return;

	if (fstate->recvBuffer->tuplesortstate)
	{
		tuplesort_end(fstate->recvBuffer->tuplesortstate);
		fstate->recvBuffer->tuplesortstate = NULL;
	}

	if (fstate->recvBuffer->queue)
	{
		/*
		 * Parallel workers must drain out all data from the underlying
		 * fragments before they can shut down the thread and exit.
		 */
		DestroyTupleQueueReceiver(fstate->recvBuffer->queue,
								  fstate->recvBuffer->queue->owner,
								  IsParallelWorker());
		fstate->recvBuffer->queue = NULL;
	}
}

void
ExecEndRemoteFragment(RemoteFragmentState *fstate)
{
	PlanState 	*pstate = &fstate->combiner.ss.ps;
	RemoteSubplan *plan = (RemoteSubplan *) pstate->plan;
	TupleTableSlot *slot = pstate->ps_ResultTupleSlot;
	ResponseCombiner *combiner = (ResponseCombiner *) fstate;

	if (plan->protocol == ProtocolPQ)
	{
		/*
		 * Clean up remote connections
		 */
		pgxc_connections_cleanup(combiner, false);

		CloseCombiner(combiner);
	}

	if (fstate->outPlanInit && outerPlanState(fstate))
	{
		ExecEndNode(outerPlanState(fstate));
	}

	ExecClearTuple(slot);

	fstate->total_pages = 0;
	fstate->disk_pages = 0;

	if (!fstate->sendFragment)
	{
		ExecEndRecvRemoteFragment(fstate);
	}
	else
	{
		ExecEndSendRemoteFragment(fstate);
	}

	/* Release working memory */
	if (fstate->cxt)
	{
		MemoryContextDelete(fstate->cxt);
		fstate->cxt = NULL;
	}
}

void
ExecDisconnectRemoteFragment(RemoteFragmentState *fstate)
{
	RemoteSubplan *subplan = (RemoteSubplan *) fstate->combiner.ss.ps.plan;

	DEBUG_FRAG(elog(LOG, "disconnect fid %d parallel %d",
					subplan->fid, IsParallelWorker()));

	/*
	 * When a fragment has not sent parameters. When it
	 * needs to be "disconnected", it can be returned directly
	 * without draining the remaining data. The execution
	 * end phase ensures that the remaining data is cleared.
	 *
	 * But set flag to true to make sure there is no data left.
	 */
	if (!fstate->sendParam)
	{
		/*
		 * When I'm an idle leader, a parallel worker should do this for me.
		 * ExecRemoteFragmentAdjustDSM will clear this flag if workers were
		 * failed to launch.
		 */
		if ((fstate->eflags & EXEC_FLAG_IDLE_LEADER) && !IsParallelWorker())
			return;

		if (subplan->cacheSend && !fstate->sendMsg)
		{
			/*
			 * Here, we also need ensure that the tqthread lunched correctly
			 * to avoid resources leakage.
			 */
			FragmentRecvStateInit(fstate);
			FragmentSendMsg(fstate);
		}
		if (fstate->recvBuffer && fstate->recvBuffer->queue)
			fstate->recvBuffer->queue->disconnected = true;
		/* we shouldn't have a param buffer, but let's be sure. */
		if (fstate->recvParamBuffer && fstate->recvParamBuffer->queue)
			fstate->recvParamBuffer->queue->disconnected = true;
		return;
	}

	/*
	 * For local execution, nothing needs to do.
	 */
	if (!fstate->needRecvData)
	{
		DEBUG_FRAG(elog(LOG, "disconnect fid %d parallel %d local execution",
						subplan->fid, IsParallelWorker()));
		return;
	}

	if (fstate->sendFragment)
	{
		elog(ERROR, "We should not disconnect send remote fragment fid %d",
			 subplan->fid);
	}

	DEBUG_FRAG(elog(LOG, "disconnect remote fragment fstate fid %d "
						 "localnodeid %d local fid %d",
					subplan->fid, PGXCNodeId - 1, local_fid));

	/*
	 * Drain out all left data
	 */
	DrainOutTuple(&fstate->combiner.ss.ps);
}

void
SetLocalFidFlevel(int fid, int flevel)
{
	volatile PGPROC *proc = MyProc;

	DEBUG_FRAG(elog(LOG, "SetLocalFidFlevel %d %d", fid, flevel));
	saved_local_fid = local_fid;
	saved_local_flevel = local_flevel;

	local_fid = fid;
	local_flevel = flevel;
	proc->local_fid = fid;
}

void
ResetLocalFidFlevel(void)
{
	volatile PGPROC *proc = MyProc;

	local_fid = saved_local_fid;
	local_flevel = saved_local_flevel;
	proc->local_fid = saved_local_fid;
}

int
GetLocalFlevel(void)
{
	return local_flevel;
}

int
GetLocalFid(void)
{
	return local_fid;
}

Size
EstimateFstateSpace(void)
{
	return add_size(sizeof(int), sizeof(int));
}

void
SerializeFstate(Size maxsize, char *start_address)
{
	int *result = (int *) start_address;

	result[0] = local_fid;
	result[1] = local_flevel;
	elog(LOG, "serialize local %d flevel %d",
		 local_fid, local_flevel);
}

void
StartParallelWorkerFstate(char *address)
{
	int *fstate = (int *) address;

	SetLocalFidFlevel(fstate[0], fstate[1]);
	elog(LOG, "deserialize local %d flevel %d",
		 local_fid, local_flevel);
}

/* ----------------------------------------------------------------
 *		ExecRemoteFragmentEstimate
 *
 *		Estimate space required to propagate fragment statistics.
 * ----------------------------------------------------------------
 */
void
ExecRemoteFragmentEstimate(RemoteFragmentState *node, ParallelContext *pcxt)
{
	Size		size;

	/* don't need this if not instrumenting or no workers */
	if (!node->combiner.ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = mul_size(pcxt->nworkers, sizeof(FragmentInstrumentation));
	size = add_size(size, offsetof(SharedFragmentInfo, sinstrument));
	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecRemoteFragmentInitializeDSM(RemoteFragmentState *node,
								ParallelContext *pcxt)
{
	Size		size;
	ParallelWorkerStatus *worker_status = NULL;

	worker_status  = GetParallelWorkerStatusInfo(pcxt->toc);
	node->parallel_status = worker_status;

	/* don't need this if not instrumenting or no workers */
	if (!node->combiner.ss.ps.instrument || pcxt->nworkers == 0)
		return;

	size = offsetof(SharedFragmentInfo, sinstrument)
		   + pcxt->nworkers * sizeof(FragmentInstrumentation);
	node->shared_info = shm_toc_allocate(pcxt->toc, size);
	/* ensure any unfilled slots will contain zeroes */
	memset(node->shared_info, 0, size);
	node->shared_info->num_workers = pcxt->nworkers;
	shm_toc_insert(pcxt->toc, node->combiner.ss.ps.plan->plan_node_id,
				   node->shared_info);
}

void
ExecRemoteFragmentInitializeDSMWorker(RemoteFragmentState *node,
									  ParallelWorkerContext *pwcxt)
{
	ParallelWorkerStatus  *worker_status = NULL;

	worker_status  = GetParallelWorkerStatusInfo(pwcxt->toc);
	if (worker_status == NULL)
		elog(ERROR, "Fragment worker can not get worker status");

	node->parallel_status = worker_status;
	node->toc = pwcxt->toc;
	elog(LOG, "initializa DSM worker fid %d", ((RemoteSubplan *)node->combiner.ss.ps.plan)->fid);

	node->shared_info =
		shm_toc_lookup(pwcxt->toc, node->combiner.ss.ps.plan->plan_node_id, true);
}

void
ExecRemoteFragmentRetrieveInstrumentation(RemoteFragmentState *node)
{
	Size		size;
	SharedFragmentInfo *si;

	if (node->shared_info == NULL)
		return;

	size = offsetof(SharedFragmentInfo, sinstrument)
		   + node->shared_info->num_workers * sizeof(FragmentInstrumentation);
	si = palloc(size);
	memcpy(si, node->shared_info, size);
	node->shared_info = si;
}

void
ExecShutdownRemoteFragment(RemoteFragmentState *node)
{
	/* similar work as ExecShutdownGather */
	if (node->pei)
	{
		ExecParallelFinish(node->pei);
		ExecParallelCleanup(node->pei);
		node->pei = NULL;
	}
}

void
ExecEagerFreeRemoteFragment(PlanState *pstate)
{
	RemoteFragmentState *node = castNode(RemoteFragmentState, pstate);

	if (IsFragmentInSubquery((RemoteSubplan *) pstate->plan))
		return;

	if (!node->sendFragment)
	{
		ExecEndRecvRemoteFragment(node);
	}
	else
	{
		ExecEndSendRemoteFragment(node);
	}

	node->needRecvData = false;

	if (node->cxt && pstate->instrument == NULL)
	{
		MemoryContextDelete(node->cxt);
		node->cxt = NULL;
	}
}

static void
WaitForParallelRemoteWorkerDone(RemoteFragmentState *node)
{
	bool adjusted = false;
	int  rc;
	ParallelContext *pcxt = node->pei->pcxt;

	while (!CheckParallelDone(node->parallel_status, pcxt))
	{
		if (end_query_requested && !adjusted)
		{
			int nworkers = pcxt->nworkers_launched;
			bool leader = pcxt->leader_participate;

			/*
			 * Set nworkers_launched/leader_participate to 0 to force end
			 * parallel hashjoin by ExecParallelAdjustDSM.
			 */
			pcxt->leader_participate = false;
			pcxt->nworkers_launched = 0;
			ExecParallelAdjustDSM(&node->combiner.ss.ps, pcxt);
			adjusted = true;

			/*
			 * Set original nworkers_launched/leader_participate to avoid rare
			 * race condition that failed to send SIGUSR2 in CheckParallelDone.
			 */
			pcxt->leader_participate = leader;
			pcxt->nworkers_launched = nworkers;
		}

		CheckFailedParallelWorkers(pcxt);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					   1000L /* 1s */, WAIT_EVENT_BGWORKER_SHUTDOWN);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		ResetLatch(MyLatch);
	}
}

void
ExecRemoteFragmentAdjustDSM(RemoteFragmentState *state, ParallelContext *pcxt)
{
	RemoteSubplan *plan = (RemoteSubplan *) state->combiner.ss.ps.plan;
	if (pcxt->leader_participate &&
		!parallel_leader_participation &&
		local_flevel == plan->flevel - 1)
	{
		state->eflags &= ~EXEC_FLAG_IDLE_LEADER;
		FragmentRecvStateInit(state);
	}
}

/*
 * Get the sub-distribution strategy.
 */
static multi_distribution *
GetLocatorExtraData(RemoteSubplan *topplan, Oid *attrsType)
{
	List	*sublocators = NIL;
	multi_distribution	*edist;
	ListCell	*lc;
	Oid		groupid = InvalidOid;

	foreach(lc, topplan->distributionNodes)
	{
		int nodeidx = lfirst_int(lc);
		Oid nodeid = PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE);

		groupid = GetGroupOidByNode(nodeid);
		break;
	}

	edist = palloc0(sizeof(multi_distribution));

	foreach(lc, topplan->multi_dist)
	{
		RemoteSubplan	*rsplan;
		Locator	*sub_loc;
		Oid	   *disTypes = NULL;

		rsplan = lfirst_node(RemoteSubplan, lc);

		if (rsplan->ndiskeys > 0)
		{
			int	i;
			disTypes = palloc0(sizeof(Oid) * rsplan->ndiskeys);

			for (i = 0; i < rsplan->ndiskeys; i++)
				disTypes[i] = attrsType[rsplan->diskeys[i] - 1];
		}

		sub_loc = createLocator(rsplan->distributionType,
								RELATION_ACCESS_INSERT,
								LOCATOR_LIST_LIST,
								0,					/* nodeCount */
								rsplan->distributionNodes,	/* nodeList */
								NULL,				/* result */
								false,				/* primary */
								groupid,			/* groupid */
								disTypes,			/* discoltypes */
								rsplan->diskeys,	/* discolnums */
								rsplan->ndiskeys,	/* ndiscols */
								NULL);				/* locator_extra */

		sublocators = lappend(sublocators, sub_loc);
	}

	edist->locator = sublocators;
	edist->startno = topplan->startno;
	edist->endno = topplan->endno;
	edist->has_else = topplan->has_else;

	return edist;
}
