/*-------------------------------------------------------------------------
 *
 * nodeRemoteModifyTable.c
 *	  routines to handle remote ModifyTable nodes.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeRemoteModifyTable.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitRemoteModifyTable - initialize the RemoteModifyTable node
 *		ExecEndRemoteModifyTable	- shut down the RemoteModifyTable node
 *		ExecReScanRemoteModifyTable - rescan the RemoteModifyTable node
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/gtm.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeRemoteModifyTable.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "pgxc/execRemote.h"
#include "pgxc/planner.h"
#include "pgxc/nodemgr.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "executor/execMerge.h"


/* GUC to enable print message while executing dist-update */
bool enable_distribute_update_print;
bool enable_global_index_print;

static TupleTableSlot *ExecRemoteModifyTable(PlanState *pstate);
static void fill_gindex_value(TupleTableSlot *slot, Datum *values, bool *nulls,
							  IndexInfo *info, ResultRelInfo *resultRelInfo);
static void modify_single_gindex(IndexInfo *info, TupleDesc desc, Locator *locator, 
							TupleTableSlot *slot, ModifyTable *mt, 
							GlobalTimestamp gts, 
							ResultRelInfo *resultRelInfo);
static void
ExecUpdateGlobalIndex(RemoteModifyTableState *node, ModifyTable *mt,
					  TupleTableSlot *inserted, TupleTableSlot *deleted, List *gindex_info);
static void
ExecRemoteDeleteTable(TupleDesc desc, Locator *locator, 
							TupleTableSlot *slot, ModifyTable *mt, 
							GlobalTimestamp gts, 
							ResultRelInfo *resultRelInfo);
static void
ExecRemoteUpdateTable(TupleDesc desc, Locator *locator, TupleTableSlot *deletedslot,
                                  TupleTableSlot *insertslot, ModifyTable *mt, GlobalTimestamp gts,
                                  ResultRelInfo *resultRelInfo);
static void
ExecRemoteInsertTable(TupleDesc desc, Locator *locator, TupleTableSlot *insertslot,
                                  ModifyTable *mt, GlobalTimestamp gts,
                                  ResultRelInfo *resultRelInfo);
static TupleTableSlot *
checkSlotExistAndPut(RemoteModifyTableState *node, TupleTableSlot *slot);

/*
 * Same as function in nodeModifyTable.c
 * 
 * ExecProcessReturning --- evaluate a RETURNING list
 *
 * projectReturning: RETURNING projection info for current result rel
 * tupleSlot: slot holding tuple actually inserted/updated/deleted
 * planSlot: slot holding tuple returned by top subplan node
 *
 * Note: If tupleSlot is NULL, the FDW should have already provided econtext's
 * scan tuple.
 *
 * Returns a slot holding the result tuple
 */
static TupleTableSlot *
ExecProcessReturning(ResultRelInfo *resultRelInfo,
					 TupleTableSlot *tupleSlot,
					 TupleTableSlot *planSlot)
{
	ProjectionInfo *projectReturning = resultRelInfo->ri_projectReturning;
	ExprContext *econtext = projectReturning->pi_exprContext;
	
	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous cycle.
	 */
	ResetExprContext(econtext);
	
	/* Make tuple and any needed join variables available to ExecProject */
	if (tupleSlot)
		econtext->ecxt_scantuple = tupleSlot;
	else
	{
		HeapTuple	tuple;
		
		/*
		 * RETURNING expressions might reference the tableoid column, so
		 * initialize t_tableOid before evaluating them.
		 */
		Assert(!TupIsNull(econtext->ecxt_scantuple));
		ExecMaterializeSlot(econtext->ecxt_scantuple);
		tuple = econtext->ecxt_scantuple->tts_tuple;
		tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
	}
	econtext->ecxt_outertuple = planSlot;
	
	/* Compute the RETURNING expressions */
	return ExecProject(projectReturning);
}

static inline void
initRemoteQueryParams(RemoteQuery *rq, RemoteQueryState *rqs)
{
	Assert(IsA(rq, RemoteQuery) && IsA(rqs, RemoteQueryState));
	
	rqs->rqs_num_params = rq->rq_num_params;
	rqs->rqs_param_types = rq->rq_param_types;
	rqs->rqs_param_formats = NULL;
	rqs->ss_num_params = rq->ss_num_params;
	rqs->ss_param_types = rq->ss_param_types;
	rqs->su_num_params = rq->su_num_params;
	rqs->su_param_types = rq->su_param_types;
}

/* ----------------------------------------------------------------
 *		ExecInitRemoteModifyTable
 * ----------------------------------------------------------------
 */
RemoteModifyTableState *
ExecInitRemoteModifyTable(RemoteModifyTable *node, EState *estate, int eflags)
{
	RemoteModifyTableState *rmtstate;
	Plan		*outer = outerPlan(node);
	ModifyTable *mt = NULL;
	ResultRelInfo *resultRelInfo;
	CmdType		operation = node->operation;
	int			nplans, nrels;
	int		 i;
	TupleDesc   tupDesc; /* returning tup desc */
	/* create a new estate for remote plans */
	EState		 *remote_estate;
	MemoryContext   oldcontext;
	
	List	 *gindex_infos = NIL;
	ListCell *l;
	
	RelationLocInfo *rel_loc_info;
	rmtstate = makeNode(RemoteModifyTableState);
	rmtstate->operation = operation;
	rmtstate->ps.plan = (Plan *) node;
	rmtstate->ps.state = estate;
	rmtstate->ps.ExecProcNode = ExecRemoteModifyTable;
	
	if (IsA(outer, RemoteSubplan))
	{
		mt = castNode(ModifyTable, ((Plan *)node)->lefttree->lefttree);
	}
	else if (IsA(outer, RemoteQuery))
	{
		mt = ((RemoteQuery *) outer)->remote_mt;
	}
	else if (IS_CENTRALIZED_MODE)
	{
		mt = (ModifyTable *) outer;
	}
	else
		elog(ERROR, "error in RemoteModifyTable construction");
	
	/* set resultRelInfo as ModifyTable do */
	nrels = list_length(mt->resultRelations);
	resultRelInfo = rmtstate->resultRelInfo = estate->es_result_relations + mt->resultRelIndex;
	rmtstate->resultTupDesc = palloc(sizeof(TupleDesc) * nrels);
	
	/* set num_gindex for global index */
	rmtstate->num_gindex = palloc(sizeof(int) * nrels);
	
	/* for each subplan (same number as result rels), construct a junkfilter and a tuple desc */
	for (i = 0; i < nrels; i++)
	{
		Plan *plan = mt->plan.lefttree;
		List *cur_gindex_infos = RelationGetGlobalIndexInfoList(resultRelInfo->ri_RelationDesc);
		
		gindex_infos = list_concat(gindex_infos, cur_gindex_infos);
		
		rmtstate->num_gindex[i] = list_length(cur_gindex_infos);
		rmtstate->resultTupDesc[i] = ExecTypeFromTL(plan->targetlist, false);
		
		if (operation == CMD_INSERT || operation == CMD_UPDATE)
			ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc,
								plan->targetlist);
		
		if (operation == CMD_INSERT || operation == CMD_UPDATE ||
			operation == CMD_DELETE || operation == CMD_MERGE)
		{
			/* For UPDATE/DELETE, find the appropriate junk attr now */
			char		relkind;
			
			relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
			if (relkind == RELKIND_RELATION ||
				relkind == RELKIND_MATVIEW ||
				relkind == RELKIND_PARTITIONED_TABLE)
			{
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(plan->targetlist, "ctid");
				if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk ctid column");
				if (node->operation != CMD_MERGE)
				{
					resultRelInfo->ri_shardid =
						ExecFindJunkAttributeInTlist(plan->targetlist, "shardid");
					if (!AttributeNumberIsValid(resultRelInfo->ri_shardid))
						elog(ERROR, "could not find junk shardid column");
				}
			}
			else if (relkind == RELKIND_FOREIGN_TABLE)
			{
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(plan->targetlist, "wholerow");
			}
			else
			{
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(plan->targetlist, "wholerow");
				if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk wholerow column");
			}
		}
		
		resultRelInfo++;
		i++;
	}
	rel_loc_info = RelationGetLocInfo(rmtstate->resultRelInfo->ri_RelationDesc);
	rmtstate->gindex_locators = palloc(sizeof(Locator *) * list_length(gindex_infos));
	rmtstate->gindex_descs = palloc(sizeof(TupleDesc) * list_length(gindex_infos));
	rmtstate->locators =  createLocator(rel_loc_info->locatorType,
							RELATION_ACCESS_INSERT,
							LOCATOR_LIST_LIST,
							0,
							rel_loc_info->rl_nodeList,
							NULL,
							false,
							rel_loc_info->groupId, 
							rel_loc_info->disAttrTypes, 
							rel_loc_info->disAttrNums, 
							rel_loc_info->nDisAttrs, NULL);
	rmtstate->desc = CreateTupleDescCopy(RelationGetDescr(rmtstate->resultRelInfo->ri_RelationDesc));
	i = 0;
	foreach(l, gindex_infos)
	{
		IndexInfo  *info = lfirst_node(IndexInfo, l);
		Relation	gindex = relation_open(info->gindex_oid, NoLock);
		RelationLocInfo *gindex_rel_loc_info = RelationGetLocInfo(gindex);
		Locator	*locator;

		locator = createLocator(gindex_rel_loc_info->locatorType,
								RELATION_ACCESS_INSERT,
								LOCATOR_LIST_LIST,
								0,
								gindex_rel_loc_info->rl_nodeList,
								NULL,
								false,
								gindex_rel_loc_info->groupId, 
								gindex_rel_loc_info->disAttrTypes, 
								gindex_rel_loc_info->disAttrNums, 
								gindex_rel_loc_info->nDisAttrs, NULL);
		rmtstate->gindex_locators[i] = locator;
		rmtstate->gindex_descs[i] = CreateTupleDescCopy(RelationGetDescr(gindex));
		
		relation_close(gindex, NoLock);
		i++;
	}
	
	/* Initialize all remote dist-update plans we created, numbers of them should be equal */
	if (operation == CMD_UPDATE)
	{
		nplans = list_length(node->dist_update_inserts);
		Assert(nplans == list_length(node->dist_update_deletes));
		Assert(nplans == list_length(node->dist_update_locks));
		Assert(nplans == list_length(node->dist_update_updates));
	}
	else if (operation == CMD_DELETE)
	{
		nplans = list_length(node->dist_update_deletes);
		Assert(nplans == list_length(node->dist_update_locks));
	}
	else if (operation == CMD_MERGE)
	{
		nplans = list_length(node->dist_update_inserts);
	}
	else
	{
		nplans = 0;
	}

	if (nplans > 0)
	{
		remote_estate = CreateExecutorState();
		
		rmtstate->dist_update_inserts = (PlanState **)palloc0(sizeof(PlanState*) * nplans);
		rmtstate->dist_update_deletes = (PlanState **)palloc0(sizeof(PlanState*) * nplans);
		rmtstate->dist_update_updates = (PlanState **)palloc0(sizeof(PlanState*) * nplans);
		rmtstate->dist_update_locks = (PlanState **)palloc0(sizeof(PlanState*) * nplans);
		
		oldcontext = MemoryContextSwitchTo(remote_estate->es_query_cxt);
		remote_estate->es_snapshot = GetActiveSnapshot();
		/* copy same rtable for epqtuple */
		remote_estate->es_range_table = list_copy(estate->es_range_table);
		
		for (i = 0; i < nplans; i++)
		{
			RemoteQuery *rq = NULL;
			RemoteQueryState *rqs = NULL;
			
			if (node->dist_update_inserts)
			{
				rq = (RemoteQuery *) list_nth(node->dist_update_inserts, i);
				rmtstate->dist_update_inserts[i] =
					(PlanState *)ExecInitNode((Plan *)rq, remote_estate, eflags);
				rqs = (RemoteQueryState *) rmtstate->dist_update_inserts[i];
				
				/* set params' number and type */
				initRemoteQueryParams(rq, rqs);
				
				/* prepare statements */
				PrepareRemoteDMLStatement(false, rq->statement,
										  rq->select_cursor, rq->update_cursor);
			}
			
			if (node->dist_update_deletes)
			{
				rq = (RemoteQuery *) list_nth(node->dist_update_deletes, i);
				rmtstate->dist_update_deletes[i] =
					(PlanState *)ExecInitNode((Plan *)rq, remote_estate, eflags);
				rqs = (RemoteQueryState *) rmtstate->dist_update_deletes[i];
				/* set params' number and type */
				initRemoteQueryParams(rq, rqs);
				
				/* prepare statements */
				PrepareRemoteDMLStatement(false, rq->statement,
										  rq->select_cursor, rq->update_cursor);
			}
			
			if (node->dist_update_updates)
			{
				
				rq = (RemoteQuery *) list_nth(node->dist_update_updates, i);
				rmtstate->dist_update_updates[i] =
					(PlanState *)ExecInitNode((Plan *)rq, remote_estate, eflags);
				rqs = (RemoteQueryState *) rmtstate->dist_update_updates[i];
				/* set params' number and type */
				initRemoteQueryParams(rq, rqs);
				
				/* set ctid and shardid as result tuple desc of insert plan */
				// ExecSetSlotDescriptor(rqs->combiner.ss.ps.ps_ResultTupleSlot, desc);
				
				/* prepare statements */
				PrepareRemoteDMLStatement(false, rq->statement,
										  rq->select_cursor, rq->update_cursor);
			}
			
			if (node->dist_update_locks)
			{
				TupleDesc   desc;
				
				rq = (RemoteQuery *) list_nth(node->dist_update_locks, i);
				/* lock stmt require full tuple with xc_node_id, ctid and shardid for tuple desc compatible */
				rmtstate->dist_update_locks[i] =
					(PlanState *)ExecInitNode((Plan *)rq, remote_estate, eflags);
				rqs = (RemoteQueryState *) rmtstate->dist_update_locks[i];
				/* set params' number and type */
				initRemoteQueryParams(rq, rqs);
				
				desc = ExecTypeFromTL(rq->remote_query->targetList, false);
				ExecSetSlotDescriptor(rqs->combiner.ss.ps.ps_ResultTupleSlot,
									  desc);
				
				ExecAssignExprContext(estate, &rqs->combiner.ss.ps);
				
				if (rq->remote_target)
				{
					TupleTableSlot *pslot = ExecInitExtraTupleSlot(estate, desc);
					
					ExecSetSlotDescriptor(pslot,
										  ExecTypeFromTL(rq->remote_target, false));
					remote_estate->es_subplanstates = estate->es_subplanstates;
					remote_estate->es_param_list_info = estate->es_param_list_info;
					remote_estate->es_param_exec_vals = estate->es_param_exec_vals;
					
					rqs->combiner.ss.ps.ps_ProjInfo =
						ExecBuildProjectionInfo(rq->remote_target,
												rqs->combiner.ss.ps.ps_ExprContext,
												pslot,
												&rqs->combiner.ss.ps,
												desc);
				}
				
				if (rq->remote_quals)
				{
					Assert(rq->remote_target);
					/*
					 * initialize child expressions
					 */
					rqs->combiner.ss.ps.qual =
						ExecInitQual(rq->remote_quals, (PlanState *) rqs);
				}
				
				/* prepare statements */
				PrepareRemoteDMLStatement(false, rq->statement,
										  rq->select_cursor, rq->update_cursor);
			}
		}
		
		MemoryContextSwitchTo(oldcontext);
	}
	
	/*
	 * Initialize RETURNING projections if needed.
	 */
	if (node->returningLists)
	{
		TupleTableSlot *slot;
		ExprContext *econtext;
		
		/*
		 * Initialize result tuple slot and assign its rowtype using the first
		 * RETURNING list.  We assume the rest will look the same.
		 */
 		tupDesc = ExecTypeFromTL((List *) linitial(node->returningLists), false);
		
		/* Set up a slot for the output of the RETURNING projection(s) */
		ExecInitResultSlot(&rmtstate->ps);
		ExecAssignResultType(&rmtstate->ps, tupDesc);
		slot = rmtstate->ps.ps_ResultTupleSlot;
		rmtstate->ps.ps_ResultTupleDesc = tupDesc;
		/* Need an econtext too */
		if (rmtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &rmtstate->ps);
		econtext = rmtstate->ps.ps_ExprContext;
		
		/*
		 * Build a projection for each result rel.
		 */
		resultRelInfo = rmtstate->resultRelInfo;
		foreach(l, node->returningLists)
		{
			List	*rlist = (List *) lfirst(l);
			
			resultRelInfo->ri_projectReturning =
				ExecBuildProjectionInfo(rlist, econtext, slot, &rmtstate->ps,
										resultRelInfo->ri_RelationDesc->rd_att);

			resultRelInfo++;
		}
	}
	else
	{
		/*
		 * We still must construct a dummy result tuple type, because InitPlan
		 * expects one (maybe should change that?).
		 */
		tupDesc = ExecTypeFromTL(NIL, false);
		ExecInitResultSlot(&rmtstate->ps);
		ExecAssignResultType(&rmtstate->ps, tupDesc);
		
		rmtstate->ps.ps_ExprContext = NULL;
	}

	if (operation == CMD_UPDATE)
	{
		rmtstate->tuple_for_update = (TupleTableSlot **)palloc0(NumDataNodes * sizeof(TupleTableSlot));
	}

	if (operation == CMD_MERGE)
	{
		resultRelInfo = rmtstate->resultRelInfo;
		ExecInitRemoteMerge(rmtstate, estate, resultRelInfo, eflags);
	}
	/* Initialize child planstate */
	outerPlanState(rmtstate) = ExecInitNode(outer, estate, eflags);
	return rmtstate;
}

/*
 * transfer global index value to string
 */
static void
GlobalIndexValue2String(TupleDesc tdesc, Datum *values, bool *nulls, StringInfo buf)
{
	int				attindex;
	int				numatts = tdesc->natts;
	char *pstring;
	Oid		typOutput;
	bool	typIsVarlena;
	Datum val;
	Oid valtype;
	
	appendStringInfoChar(buf, '(');
	for (attindex = 0; attindex < numatts; attindex++)
	{
		if (nulls[attindex])
		{
			pstring = "null";
		}
		else
		{
			val = values[attindex];
			valtype = TupleDescAttr(tdesc, attindex)->atttypid;

			/* Get info needed to output the value */
			getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);

			/*
			 * If we have a toasted datum, forcibly detoast it here to avoid
			 * memory leakage inside the type's output routine.
			 */
			if (typIsVarlena)
				val = PointerGetDatum(PG_DETOAST_DATUM(val));

			pstring = OidOutputFunctionCall(typOutput, val);
		}

		appendBinaryStringInfo(buf, pstring, strlen(pstring));
		if (attindex != numatts - 1)
		{
			appendStringInfoChar(buf, ',');
		}
	}
	appendStringInfoChar(buf, ')');
}

/*
 * ExecUpdateGlobalIndex
 */
static void
ExecUpdateGlobalIndex(RemoteModifyTableState *node, ModifyTable *mt,
					  TupleTableSlot *inserted, TupleTableSlot *deleted, List *gindex_info)
{
	int		 planno = 0;
	int		 i = 0;
	ListCell	*lc = NULL;
	bool succeed = false;
	GlobalTimestamp gts = InvalidGlobalTimestamp;
	int resultidx = 0;
	ResultRelInfo *resultRelInfo = node->resultRelInfo + resultidx;

	Assert(mt->operation == CMD_UPDATE || mt->operation == CMD_DELETE);
	Assert(mt->global_index);

	if (gindex_info == NULL)
	{
		return;
	}

	/*
	 * get new gts from gtm to make datanodes can see the newest tuple,
	 * this is allowed because there is no concurrent operation of global index,
	 * and there is a one-to-one correspondence between global index and the tuple.
	 */
	gts = GetGlobalTimestampGTM();

	/* calculate which plan */
	planno = 0;
	for (i = 0; i < resultidx; i++)
	{
		planno += node->num_gindex[i];
	}

	foreach(lc, gindex_info)
	{
		IndexInfo	*info = (IndexInfo *) lfirst(lc);
		TupleDesc	desc = node->gindex_descs[planno];
		Locator		*locator = node->gindex_locators[planno];
		HeapTuple	ituple = NULL, dtuple = NULL;
		Datum		*deleted_values = palloc(sizeof(Datum) * (info->ii_NumIndexAttrs + CNI_META_NUM));
		bool		*deleted_nulls  = palloc0(sizeof(bool) * (info->ii_NumIndexAttrs + CNI_META_NUM));
		Datum		*insert_values  = palloc(sizeof(Datum) * (info->ii_NumIndexAttrs + CNI_META_NUM));
		bool		*insert_nulls   = palloc0(sizeof(bool) * (info->ii_NumIndexAttrs + CNI_META_NUM));
		bool		gindex_cross_node = true;
		fill_gindex_value(deleted, deleted_values, deleted_nulls, info, resultRelInfo);
		fill_gindex_value(inserted, insert_values, insert_nulls, info, resultRelInfo);
		
		dtuple = heap_form_tuple(desc, deleted_values, deleted_nulls);
		ituple = heap_form_tuple(desc, insert_values, insert_nulls);

		if (mt->update_gindex_key)
		{
			int i_node, d_node;
			GET_NODES(locator,
						deleted_values,
						deleted_nulls,
						info->ii_NumIndexAttrs,
						NULL);
			d_node = ((int *)(getLocatorResults(locator)))[0];
			GET_NODES(locator,
						insert_values,
						insert_nulls,
						info->ii_NumIndexAttrs,
						NULL);
			i_node = ((int *)(getLocatorResults(locator)))[0];
			if (i_node == d_node)
				gindex_cross_node = false;
		}
		/* if dis update or update global index key, transfer to insert + delete */
		if (mt->update_gindex_key && gindex_cross_node)
		{
			if (enable_global_index_print)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, deleted_values, deleted_nulls, &buf);
		
				elog(LOG, "[gindex-update] delete old global index key %s of relation %s",
					 buf.data,
					 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
				pfree(buf.data);
			}
			succeed = ExecSimpleRemoteDML(ituple, 
								dtuple, 
								deleted_values, 
								deleted_nulls,
								info->ii_NumIndexAttrs,
								get_rel_name(info->gindex_oid), 
								get_namespace_name(get_rel_namespace(info->gindex_oid)), 
								locator,
								CMD_DELETE, 
								gts,
								NULL);
			if (!succeed)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, deleted_values, deleted_nulls, &buf);
				elog(ERROR, "[gindex-update] delete old global index key %s failed for relation %s, "
							"maybe the index key does not exist",
					 buf.data, 
					 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			}
			
			if (mt->operation != CMD_DELETE)
			{
				fill_gindex_value(inserted, insert_values, insert_nulls, info, resultRelInfo);
				ituple = heap_form_tuple(desc, insert_values, insert_nulls);

				if (enable_global_index_print)
				{
					StringInfoData	buf;
					initStringInfo(&buf);
					GlobalIndexValue2String(desc, deleted_values, deleted_nulls, &buf);
			
					elog(LOG, "[gindex-update] insert new global index key %s of relation %s",
						 buf.data,
						 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
					pfree(buf.data);
				}
				succeed = ExecSimpleRemoteDML(ituple, 
											NULL, 
											insert_values, 
											insert_nulls,
											info->ii_NumIndexAttrs,
											get_rel_name(info->gindex_oid), 
											get_namespace_name(get_rel_namespace(info->gindex_oid)), 
											locator,
											CMD_INSERT, 
											gts,
											NULL);

				if (!succeed)
				{
					StringInfoData	buf;
					initStringInfo(&buf);
					GlobalIndexValue2String(desc, deleted_values, deleted_nulls, &buf);
					elog(ERROR, "[gindex-update] insert new global index key %s failed for relation %s, "
					   		"please check the index table",
						 buf.data, RelationGetRelationName(resultRelInfo->ri_RelationDesc));
				}
			}
		}
		else
		{
			if (enable_global_index_print)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, deleted_values, deleted_nulls, &buf);

				elog(LOG, 
					"[gindex-update] update global index key %s of relation %s, shard update to %d, "
					 "location update to (%u,%u)",
					 buf.data,
					 RelationGetRelationName(resultRelInfo->ri_RelationDesc),
					 (int)deleted_values[info->ii_NumIndexAttrs + 1],
					 ItemPointerGetBlockNumberNoCheck((ItemPointer)deleted_values[info->ii_NumIndexAttrs + 2]),
					 ItemPointerGetOffsetNumberNoCheck((ItemPointer)deleted_values[info->ii_NumIndexAttrs + 2]));
				pfree(buf.data);
			}
			succeed = ExecSimpleRemoteDML(ituple, 
										dtuple, 
										deleted_values, 
										deleted_nulls,
										info->ii_NumIndexAttrs,
										get_rel_name(info->gindex_oid), 
										get_namespace_name(get_rel_namespace(info->gindex_oid)), 
										locator,
										CMD_UPDATE, 
										gts,
										NULL);

			if (!succeed)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, deleted_values, deleted_nulls, &buf);
				elog(ERROR, "[gindex-update] update global index key %s's shard to %d location to (%u,%u) failed "
						"for relation %s, please check the index table",
					 buf.data,
					 (int)deleted_values[info->ii_NumIndexAttrs + 1],
					 ItemPointerGetBlockNumberNoCheck((ItemPointer)deleted_values[info->ii_NumIndexAttrs + 2]),
					 ItemPointerGetOffsetNumberNoCheck((ItemPointer)deleted_values[info->ii_NumIndexAttrs + 2]),
					 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			}
		}
		pfree_ext(deleted_values);
		pfree_ext(deleted_nulls);
		pfree_ext(insert_values);
		pfree_ext(insert_nulls);
		heap_freetuple_ext(ituple);
		heap_freetuple_ext(dtuple);
		planno++;
	}
}

/* ----------------------------------------------------------------
 *	   ExecRemoteModifyTable
 *
 *		Perform remote table modifications as required, and return
 *		RETURNING results if needed.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecRemoteModifyTable(PlanState *pstate)
{
	RemoteModifyTableState *node = castNode(RemoteModifyTableState, pstate);
	EState	   *estate = node->ps.state;
	CmdType		operation = node->operation;
	ResultRelInfo *resultRelInfo = node->resultRelInfo;
	PlanState  *subplanstate = outerPlanState(node);
	TupleTableSlot *slot = NULL;
	TupleTableSlot *planSlot = NULL;
	ModifyTable *mt = NULL;
	TupleTableSlot *slotForUpdateNew = NULL;
	TupleTableSlot *slotForUpdateOld = NULL;
	static int planno = 0;
	static int resultidx = 0;
	
	Assert(IS_PGXC_COORDINATOR);
	Assert(IsA(subplanstate, RemoteQueryState) || IsA(subplanstate, RemoteFragmentState));
	if (IsParallelWorker())
		elog(ERROR, "[gindex-modify] not support parallel");
	if (IsA(subplanstate, RemoteQueryState))
		mt = ((RemoteQuery *) subplanstate->plan)->remote_mt;
	else if (IsA(subplanstate, RemoteFragmentState))
		mt = (ModifyTable *) subplanstate->plan->lefttree;
	else
		elog(ERROR, "error in RemoteModifyTable construction");
	
	for (;;)
	{
		int nodeidx = 0;
		/*
		 * Reset the per-output-tuple exprcontext.  This is needed because
		 * triggers expect to use that context as workspace.  It's a bit ugly
		 * to do this below the top level of the plan, however.  We might need
		 * to rethink this later.
		 */
		ResetPerTupleExprContext(estate);
		
		/*
		 * before receiving tuple from DN, we should clean extra
		 * message in result slot of remote planstate
		 */
		ExecClearTuple(subplanstate->ps_ResultTupleSlot);
		
		planSlot = ExecProcNode(subplanstate);

		if (TupIsNull(planSlot))
		{
			if (slotForUpdateNew != NULL)
				ExecDropSingleTupleTableSlot(slotForUpdateNew);
			return NULL;
		}

		resultRelInfo = node->resultRelInfo + resultidx;

		if (mt->operation == CMD_MERGE)
		{
			planSlot = ExecRemoteMergeQualProj(node, resultRelInfo, planSlot);
			if (TupIsNull(planSlot))
				continue;
			ExecRemoteInsertTable(node->desc,
			                      node->locators,
			                      planSlot,
			                      mt,
			                      InvalidGlobalTimestamp,
			                      resultRelInfo);
			node->ps.state->es_processed += 1;
		}
		else if (mt->global_index)
		{
			/* DML is done, deal with global index now */
			int	 i;
			GlobalTimestamp gts = InvalidGlobalTimestamp;

			/* must have global index table */
			List		*gindex_info = RelationGetGlobalIndexInfoList(resultRelInfo->ri_RelationDesc);
			ListCell	*lc;
			if (operation == CMD_UPDATE)
			{
				slotForUpdateNew = checkSlotExistAndPut(node, planSlot);
				while (slotForUpdateNew == NULL)
				{
					planSlot = ExecProcNode(subplanstate);
					slotForUpdateNew = checkSlotExistAndPut(node, planSlot);
				}
				if (planSlot->tts_datarow)
					nodeidx = PGXCNodeGetNodeId(planSlot->tts_datarow->msgnode, NULL);
				else
					nodeidx = planSlot->tts_nodebufferindex;
				slotForUpdateOld = planSlot;
				planSlot = slotForUpdateNew;

				if (TupIsNull(slotForUpdateOld))
					elog(ERROR, "global index error when get old tuple for update data");

				slot_getallattrs(slotForUpdateNew);
				slot_getallattrs(slotForUpdateOld);
			}
			slot = planSlot;
			
			if (mt->operation == CMD_UPDATE)
			{
				gts = GetGlobalTimestampGTM();
				if (TTS_REMOTETID_SCAN(slot))
				{
					ExecRemoteUpdateTable(node->desc, 
									node->locators, 
									slotForUpdateOld,
									slot, 
									mt, 
									gts, 
									resultRelInfo);
					node->ps.state->es_processed += 1;
				}
				ExecUpdateGlobalIndex(node, mt, slot, slotForUpdateOld, gindex_info);
			}
			else if (gindex_info != NIL)
			{
				Assert(mt->operation == CMD_INSERT || mt->operation == CMD_DELETE);
				if (mt->operation == CMD_DELETE)
				{
					/*
					 * get new gts from gtm to make datanodes can see the newest tuple,
					 * this is allowed because there is no concurrent operation of global index,
					 * and there is a one-to-one correspondence between global index and the tuple.
					 */
					gts = GetGlobalTimestampGTM();
				}
				
				if (TTS_REMOTETID_SCAN(slot))
				{
					ExecRemoteDeleteTable(node->desc, 
									node->locators, 
									slot, 
									mt, 
									gts, 
									resultRelInfo);
					node->ps.state->es_processed += 1;
				}
				/* calculate which plan */
				planno = 0;
				for (i = 0; i < resultidx; i++)
				{
					planno += node->num_gindex[i];
				}
				
				foreach(lc, gindex_info)
				{
					IndexInfo	  *info = (IndexInfo *) lfirst(lc);
					TupleDesc	   desc = node->gindex_descs[planno];
					Locator		*locator = node->gindex_locators[planno];
					modify_single_gindex(info, desc, locator, slot, mt, gts, resultRelInfo);
					planno++;
				}
			}
			
			/*
			 * process RETURNING as needed.
			 */
			if (resultRelInfo->ri_projectReturning)
			{
				TupleTableSlot *rslot;
				
				slot_getallattrs(slot);
				ExecMaterializeSlot(slot);
				rslot = ExecProcessReturning(resultRelInfo, slot, planSlot);
				
				if (slotForUpdateNew != NULL)
					ExecDropSingleTupleTableSlot(slotForUpdateNew);
				
				return rslot;
			}
		}
		else
		{
			/*
			 * or it may be a returned tuple for RETURNING clause,
			 * and should be done with projection, just return it.
			 */
			if (enable_distribute_update_print)
				elog(LOG, "[dist-update] CN got a none dist-update tuple,"
						  "should return it to client directly");
			if (slotForUpdateNew != NULL)
				ExecDropSingleTupleTableSlot(slotForUpdateNew);
			return planSlot;
		}
		if (mt->global_index && operation == CMD_UPDATE)
		{
			(void)ExecClearTuple(node->tuple_for_update[nodeidx]);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecReScanRemoteModifyTable
 * ----------------------------------------------------------------
 */
void
ExecReScanRemoteModifyTable(RemoteModifyTableState *node)
{
	elog(ERROR, "ExecReScanRemoteModifyTable is not implemented");
}

/* ----------------------------------------------------------------
 *		ExecEndRemoteModifyTable
 * ----------------------------------------------------------------
 */
void
ExecEndRemoteModifyTable(RemoteModifyTableState *node)
{
	RemoteModifyTable *plan = (RemoteModifyTable *)node->ps.plan;
	ListCell	*lc;
	int		  i;
	RemoteQuery *rq;
	
	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
	
	Assert(IS_LOCAL_ACCESS_NODE);
	
	i = 0;
	foreach(lc, plan->dist_update_inserts)
	{
		ExecEndNode(node->dist_update_inserts[i++]);
		rq = (RemoteQuery *) lfirst(lc);
		DropRemoteDMLStatement(rq->statement, rq->update_cursor);
	}
	
	i = 0;
	foreach(lc, plan->dist_update_locks)
	{
		ExecEndNode(node->dist_update_locks[i++]);
		rq = (RemoteQuery *) lfirst(lc);
		DropRemoteDMLStatement(rq->statement, rq->update_cursor);
	}
	
	i = 0;
	foreach(lc, plan->dist_update_deletes)
	{
		ExecEndNode(node->dist_update_deletes[i++]);
		rq = (RemoteQuery *) lfirst(lc);
		DropRemoteDMLStatement(rq->statement, rq->update_cursor);
	}
	
	i = 0;
	foreach(lc, plan->dist_update_updates)
	{
		ExecEndNode(node->dist_update_updates[i++]);
		rq = (RemoteQuery *) lfirst(lc);
		DropRemoteDMLStatement(rq->statement, rq->update_cursor);
	}
	pfree_ext(node->tuple_for_update);
}

void 
modify_single_gindex(IndexInfo *info, TupleDesc desc, Locator *locator, 
							TupleTableSlot *slot, ModifyTable *mt, 
							GlobalTimestamp gts, 
							ResultRelInfo *resultRelInfo)
{
		HeapTuple	tuple;
		bool succeed = false;
		Datum		*values = palloc(sizeof(Datum) * (info->ii_NumIndexAttrs + CNI_META_NUM));
		bool		*nulls = palloc0(sizeof(bool) * (info->ii_NumIndexAttrs + CNI_META_NUM));
		fill_gindex_value(slot, values, nulls, info, resultRelInfo);
		tuple = heap_form_tuple(desc, values, nulls);

		if (enable_global_index_print)
		{
			StringInfoData	buf;
			initStringInfo(&buf);
			GlobalIndexValue2String(desc, values, nulls, &buf);

			elog(LOG, "[gindex-%s] %s global index key %s of relation %s",
				 (mt->operation == CMD_INSERT) ? "insert" : "delete",
				 (mt->operation == CMD_INSERT) ? "insert" : "delete",
				 buf.data,
				 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			pfree(buf.data);
		}

		if (mt->operation == CMD_INSERT)
		{
			succeed = ExecSimpleRemoteDML(tuple,
										  NULL,
										  values,
										  nulls,
										  info->ii_NumIndexAttrs,
										  get_rel_name(info->gindex_oid),
										  get_namespace_name(get_rel_namespace(info->gindex_oid)),
										  locator,
										  CMD_INSERT,
										  gts,
										  NULL);

			if (!succeed)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, values, nulls, &buf);
				elog(ERROR, "[gindex-insert] insert global index key %s failed for relation %s, "
							"please check the index table",
					 buf.data, RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			}
		}
		
		if (mt->operation == CMD_DELETE)
		{
			succeed = ExecSimpleRemoteDML(NULL,
										  tuple,
										  values,
										  nulls,
										  info->ii_NumIndexAttrs,
										  get_rel_name(info->gindex_oid),
										  get_namespace_name(get_rel_namespace(info->gindex_oid)),
										  locator,
										  CMD_DELETE,
										  gts,
										  NULL);

			if (!succeed)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, values, nulls, &buf);
				elog(ERROR, "[gindex-delete] delete global index key %s failed for relation %s,"
							" maybe the index key does not exist",
					 buf.data, RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			}
		}
		pfree_ext(values);
		pfree_ext(nulls);
		heap_freetuple_ext(tuple);
}

void 
fill_gindex_value(TupleTableSlot *slot, Datum *values, bool *nulls,
				  IndexInfo *info, ResultRelInfo *resultRelInfo)
{
	int i;
	slot_getallattrs(slot);
	/* fill origin data */
	for (i = 0; i < info->ii_NumIndexAttrs; i++)
		values[i] = slot_getattr(slot, info->ii_KeyAttrNumbers[i], &nulls[i]);
	
	/* fill index location data */
	/* shardid */
	values[info->ii_NumIndexAttrs] = 
		ExecGetJunkAttribute(slot, resultRelInfo->ri_shardid,
							 &nulls[info->ii_NumIndexAttrs]);
	
	/* tableoid */
	/* values[info->ii_NumIndexAttrs + 1] = 
		ExecGetJunkAttribute(slot, junkfilter->jf_xc_part_id,
							 &nulls[info->ii_NumIndexAttrs + 1]); */
	nulls[info->ii_NumIndexAttrs + 1] = true;

	/* ctid */
	values[info->ii_NumIndexAttrs + 2] = 
		ExecGetJunkAttribute(slot, resultRelInfo->ri_RowIdAttNo,
							 &nulls[info->ii_NumIndexAttrs + 2]);
	/* extra */
	nulls[info->ii_NumIndexAttrs + 3] = true;
}

static void
fill_distribute_values(TupleTableSlot *slot, Datum *values, bool *nulls, 
					   AttrNumber attno, RelationLocInfo *rel_loc_info,
					   ItemPointer dctid)
{
	int i;
	ItemPointer ip;
	bool null;
	slot_getallattrs(slot);
	
	for (i = 0; i < rel_loc_info->nDisAttrs; i++)
		values[i] = slot_getattr(slot, rel_loc_info->disAttrNums[i], &nulls[i]);
	ip = (ItemPointer)ExecGetJunkAttribute(slot, attno, &null);
	if (null)
		elog(ERROR, "fill_distribute_values not find ctid");
	memcpy(dctid, ip, sizeof(ItemPointerData));
}

static
void fill_table_values(TupleTableSlot *slot, Datum *values, bool *nulls, Relation rel)
{
	int		attrno = 0, numattrs;
	slot_getallattrs(slot);
	
	numattrs = RelationGetNumberOfAttributes(rel);
	
	for (attrno = 0; attrno < numattrs; attrno++)
	{
		Form_pg_attribute att_tup = &rel->rd_att->attrs[attrno];
			
		if (att_tup->attisdropped)
		{
			nulls[attrno] = true;
			continue;
		}
		values[attrno] = slot_getattr(slot, attrno + 1, &nulls[attrno]);
	}
}

static void
ExecRemoteDeleteTable(TupleDesc desc, Locator *locator, 
							TupleTableSlot *slot, ModifyTable *mt, 
							GlobalTimestamp gts, 
							ResultRelInfo *resultRelInfo)
{
		bool succeed = false;
		ItemPointerData dtarget_ctid;
		RelationLocInfo *rel_loc_info;
		Datum		*values = palloc(sizeof(Datum) * (desc->natts));
		bool		*nulls = palloc0(sizeof(bool) * (desc->natts));
		rel_loc_info = RelationGetLocInfo(resultRelInfo->ri_RelationDesc);
		fill_distribute_values(slot, values, nulls, resultRelInfo->ri_RowIdAttNo, rel_loc_info, &dtarget_ctid);

		if (enable_global_index_print)
		{
			StringInfoData	buf;
			initStringInfo(&buf);
			GlobalIndexValue2String(desc, values, nulls, &buf);

			elog(LOG, "[gindex-%s] %s global index key %s of relation %s",
				 (mt->operation == CMD_INSERT) ? "insert" : "delete",
				 (mt->operation == CMD_INSERT) ? "insert" : "delete",
				 buf.data,
				 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			pfree(buf.data);
		}

		if (mt->operation == CMD_DELETE)
		{
			succeed = ExecSimpleRemoteDML(NULL,
										  slot->tts_tuple,
										  values,
										  nulls,
										  rel_loc_info->nDisAttrs,
										  get_rel_name(resultRelInfo->ri_RelationDesc->rd_id),
										  get_namespace_name(get_rel_namespace(resultRelInfo->ri_RelationDesc->rd_id)),
										  locator,
										  CMD_DELETE,
										  gts,
										  &dtarget_ctid);

			if (!succeed)
			{
				StringInfoData	buf;
				initStringInfo(&buf);
				GlobalIndexValue2String(desc, values, nulls, &buf);
				elog(ERROR, "[gindex-delete-table] delete global index key %s failed for relation %s,"
							" maybe the index key does not exist",
					 buf.data, RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			}
		}
		pfree_ext(values);
		pfree_ext(nulls);
}

static void
ExecRemoteUpdateTable(TupleDesc desc, Locator *locator, 
							TupleTableSlot *deletedslot, 
							TupleTableSlot *insertslot,ModifyTable *mt, 
							GlobalTimestamp gts, 
							ResultRelInfo *resultRelInfo)
{
		bool succeed = false;
		ItemPointerData dtarget_ctid;
		RelationLocInfo *rel_loc_info;
		HeapTuple	ituple;
		Datum	newctid;
		bool isnull;
		Datum		*values = palloc(sizeof(Datum) * (desc->natts));
		bool		*nulls = palloc0(sizeof(bool) * (desc->natts));
		Datum		*newvalues = palloc(sizeof(Datum) * (desc->natts));
		bool		*newnulls = palloc0(sizeof(bool) * (desc->natts));
		rel_loc_info = RelationGetLocInfo(resultRelInfo->ri_RelationDesc);
		fill_distribute_values(deletedslot, values, nulls, resultRelInfo->ri_RowIdAttNo, rel_loc_info, &dtarget_ctid);
		fill_table_values(insertslot, newvalues, newnulls, resultRelInfo->ri_RelationDesc);
		ituple = heap_form_tuple(RelationGetDescr(resultRelInfo->ri_RelationDesc), newvalues, newnulls);

		if (enable_global_index_print)
		{
			StringInfoData	buf;
			initStringInfo(&buf);
			GlobalIndexValue2String(desc, values, nulls, &buf);

			elog(LOG, "[gindex-%s] %s global index key %s of relation %s",
				 (mt->operation == CMD_INSERT) ? "insert" : "delete",
				 (mt->operation == CMD_INSERT) ? "insert" : "delete",
				 buf.data,
				 RelationGetRelationName(resultRelInfo->ri_RelationDesc));
			pfree(buf.data);
		}

		succeed = ExecSimpleRemoteDML(ituple,
									  deletedslot->tts_tuple,
									  values,
									  nulls,
									  rel_loc_info->nDisAttrs,
									  get_rel_name(resultRelInfo->ri_RelationDesc->rd_id),
									  get_namespace_name(get_rel_namespace(resultRelInfo->ri_RelationDesc->rd_id)),
									  locator,
									  CMD_UPDATE,
									  gts,
									  &dtarget_ctid);

		if (!succeed)
		{
			StringInfoData	buf;
			initStringInfo(&buf);
			GlobalIndexValue2String(desc, values, nulls, &buf);
			elog(ERROR, "[gindex-delete-table] delete global index key %s failed for relation %s,"
						" maybe the index key does not exist",
				 buf.data, RelationGetRelationName(resultRelInfo->ri_RelationDesc));
		}
		/* hard replace new ctid */
		newctid = ExecGetJunkAttribute(insertslot, 1, &isnull);
		memcpy(DatumGetPointer(newctid), &(ituple->t_self), sizeof(ItemPointerData));
		pfree_ext(values);
		pfree_ext(nulls);
		pfree_ext(newvalues);
		pfree_ext(newnulls);
		heap_freetuple_ext(ituple);
}

static void
ExecRemoteInsertTable(TupleDesc desc, Locator *locator, 
							TupleTableSlot *insertslot,ModifyTable *mt, 
							GlobalTimestamp gts, 
							ResultRelInfo *resultRelInfo)
{
		bool succeed = false;
		RelationLocInfo *rel_loc_info;
		HeapTuple	ituple;
		Datum		*values = palloc(sizeof(Datum) * (desc->natts));
		bool		*nulls = palloc0(sizeof(bool) * (desc->natts));
		rel_loc_info = RelationGetLocInfo(resultRelInfo->ri_RelationDesc);
		fill_table_values(insertslot, values, nulls, resultRelInfo->ri_RelationDesc);
		ituple = heap_form_tuple(RelationGetDescr(resultRelInfo->ri_RelationDesc), values, nulls);

		succeed = ExecSimpleRemoteDML(
			ituple,
			NULL,
			values,
			nulls,
			rel_loc_info->nDisAttrs,
			get_rel_name(resultRelInfo->ri_RelationDesc->rd_id),
			get_namespace_name(get_rel_namespace(resultRelInfo->ri_RelationDesc->rd_id)),
			locator,
			CMD_INSERT,
			gts,
			NULL);

		if (!succeed)
		{
			elog(ERROR,
		         "Insert failed for relation %s",
		         RelationGetRelationName(resultRelInfo->ri_RelationDesc));
		}
		
		pfree_ext(values);
		pfree_ext(nulls);
}

TupleTableSlot *
checkSlotExistAndPut(RemoteModifyTableState *node, TupleTableSlot *slot)
{
	int nodeidx = 0;
	PlanState *ps = castNode(PlanState, node);
	if (slot->tts_datarow)
		nodeidx = PGXCNodeGetNodeId(slot->tts_datarow->msgnode, NULL);
	else
		nodeidx = slot->tts_nodebufferindex;

	if (node->tuple_for_update[nodeidx] == NULL || 
		TTS_EMPTY(node->tuple_for_update[nodeidx]))
	{
		if (node->tuple_for_update[nodeidx] == NULL )
			node->tuple_for_update[nodeidx] = 
				ExecAllocTableSlot(&ps->state->es_tupleTable, slot->tts_tupleDescriptor);
		if (TTS_REMOTETID_SCAN(slot))
			SET_TTS_REMOTETID_SCAN(node->tuple_for_update[nodeidx]);
		node->tuple_for_update[nodeidx] = 
			ExecCopySlot(node->tuple_for_update[nodeidx], slot);
		return NULL;
	}
	return node->tuple_for_update[nodeidx];
}
