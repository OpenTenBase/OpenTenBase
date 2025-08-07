/*-------------------------------------------------------------------------
 *
 * execDispatchFragment.c
 *	  This code provides support for fragment dispatch.
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execDispatchFragment.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include <sys/epoll.h>

#include "access/atxact.h"
#include "audit/audit.h"
#include "catalog/pgxc_class.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/planner.h"
#include "pgxc/poolmgr.h"
#include "pgxc/execRemote.h"
#include "nodes/bitmapset.h"
#include "executor/execDispatchFragment.h"
#include "executor/execFragment.h"
#include "pgxc/squeue.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "nodes/nodes.h"
#include "access/gtm.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "nodes/bitmapset.h"
#include "utils/hsearch.h"
#include "pgxc/nodemgr.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "storage/spin.h"
#include "optimizer/memctl.h"
#include "commands/explain_dist.h"
#include "utils/builtins.h"
#include "utils/resowner_private.h"

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#include "utils/resgroup.h"
#include "poll.h"
#endif

#define MAX_NODES 		1024

volatile sig_atomic_t end_query_requested = false;

#if (OPENTENBASE_MAX_COORDINATOR_NUMBER > OPENTENBASE_MAX_DATANODE_NUMBER)
#define MAX_NODEID_NUMBER OPENTENBASE_MAX_COORDINATOR_NUMBER
#else
#define MAX_NODEID_NUMBER OPENTENBASE_MAX_DATANODE_NUMBER
#endif

typedef struct
{
	int 	fid;
	int 	level;
	List   *subplans;
} redirect_subplan_recv_context;

typedef struct
{
	PlannerGlobal *glob;
	Bitmapset *processed_subplan;
	bool	recv_top;
	int 	recv_fid;
	List   *nodeList;
	bool	isCteWithCB;
	char   *ctename;
} redirect_subplan_send_context;

/*
 * Given the param no, find the corresponding init plan.
 */
SubPlan *
lookup_subplan_for_param(List *subplans, int paramno)
{
	ListCell   *l;

	foreach(l, subplans)
	{
		SubPlan    *subplan = (SubPlan *) lfirst(l);

		if (!subplan)
			continue;

		if (list_member_int(subplan->setParam, paramno) ||
			list_member_int(subplan->paramIds, paramno))
			return subplan;
	}

	return NULL;
}

/*
 * Generate remote params for RemoteSubplan which is under subplan
 */
static void
SetFragmentSubPlanParams(RemoteSubplan *remotesubplan, EState *estate)
{
	int i = -1;
	int paramno = 0;
	Bitmapset *defineParams = NULL;
	Plan *plan = (Plan *)remotesubplan;

	/* only need to handle extParam */
	remotesubplan->nparams = bms_num_members(plan->extParam);
	if (remotesubplan->nparams == 0)
		return;

	/* Allocate enough space */
	remotesubplan->remoteparams =
		(RemoteParam *)palloc0(sizeof(RemoteParam) * remotesubplan->nparams);

	while ((i = bms_next_member(plan->extParam, i)) >= 0)
	{
		ParamExecData *prmdata = &(estate->es_param_exec_vals[i]);
		remotesubplan->remoteparams[paramno].paramkind = PARAM_EXEC;
		remotesubplan->remoteparams[paramno].paramid = i;
		remotesubplan->remoteparams[paramno].paramtype = prmdata->ptype;
		remotesubplan->remoteparams[paramno].paramused = 1;
		/* Will scan plan tree to find out data type of the param */
		if (prmdata->ptype == InvalidOid)
			defineParams = bms_add_member(defineParams, i);
		paramno++;
	}

	if (!bms_is_empty(defineParams))
	{
		struct find_params_context context;
		bool all_found;

		context.rparams = remotesubplan->remoteparams;
		context.defineParams = defineParams;
		context.subplans = estate->es_plannedstmt->subplans;

		all_found = determine_param_types(plan, &context);
		/*
		 * Remove not defined params from the list of remote params.
		 * If they are not referenced no need to send them down
		 */
		if (!all_found)
		{
			for (i = 0; i < remotesubplan->nparams; i++)
			{
				if (remotesubplan->remoteparams[i].paramkind == PARAM_EXEC &&
					bms_is_member(remotesubplan->remoteparams[i].paramid,
								  context.defineParams))
				{
					/* Copy last parameter inplace */
					remotesubplan->nparams--;
					if (i < remotesubplan->nparams)
						remotesubplan->remoteparams[i] =
							remotesubplan->remoteparams[remotesubplan->nparams];
					/* keep current in the same position */
					i--;
				}
			}
		}
		bms_free(context.defineParams);
	}
}

/* serialize plan to string */
static void
BuildFragmentStmt(Fragment *fragment, EState *estate, RemoteStmt *rstmt)
{
	Plan *plan = fragment->plan;
	ParamListInfo ext_params;
	RemoteSubplan *remotesubplan = NULL;

	if (IsA(plan, RemoteSubplan))
		remotesubplan = castNode(RemoteSubplan, plan);

	if (remotesubplan && fragment->fid != remotesubplan->fid)
		remotesubplan->fid = fragment->fid;

	if (outerPlan(plan))
	{
		if (IsA(outerPlan(plan), ModifyTable) ||
			IsA(outerPlan(plan), MultiModifyTable))
		{
			rstmt->commandType = estate->es_plannedstmt->commandType;
			rstmt->hasReturning = estate->es_plannedstmt->hasReturning;
			rstmt->resultRelations = estate->es_plannedstmt->resultRelations;
			rstmt->nonleafResultRelations = estate->es_plannedstmt->nonleafResultRelations;
			rstmt->rootResultRelations = estate->es_plannedstmt->rootResultRelations;
		}
		else
		{
			rstmt->commandType = CMD_SELECT;
			rstmt->hasReturning = false;
			rstmt->resultRelations = NIL;
			rstmt->nonleafResultRelations = NIL;
			rstmt->rootResultRelations = NIL;
		}
	}
	else
	{
		if (IsA(plan, CteScan))
		{
			rstmt->commandType = CMD_SELECT;
			rstmt->hasReturning = false;
			rstmt->resultRelations = NIL;
		}
		else
		{
			elog(ERROR, "unexpected fragment plan");
		}
	}

	rstmt->rtable = estate->es_range_table;
	rstmt->subplans = estate->es_plannedstmt->subplans;
	rstmt->rewindPlanIDs = estate->es_plannedstmt->rewindPlanIDs;
	rstmt->sharedCtePlanIds = estate->es_plannedstmt->sharedCtePlanIds;
	rstmt->paramExecTypes = estate->es_plannedstmt->paramExecTypes;
	ext_params = estate->es_param_list_info;
	rstmt->nParamRemote = (ext_params ? ext_params->numParams : 0) +
						  (remotesubplan ? remotesubplan->nparams : 0);
	if (rstmt->nParamRemote > 0)
	{
		int i;
		int paramno = 0;

		/* Allocate enough space */
		rstmt->remoteparams =
			(RemoteParam *) palloc0(rstmt->nParamRemote * sizeof(RemoteParam));

		if (ext_params)
		{
			for (i = 0; i < ext_params->numParams; i++)
			{
				ParamExternData *param;
				ParamExternData  param_data;

				/*
				 * If parameter type is not yet defined but can be defined
				 * do that
				 */
				if (ext_params->paramFetch)
					param = (*ext_params->paramFetch) (ext_params, i + 1, false,
													   &param_data);
				else
					param = &ext_params->params[i];

				/*
				 * If the parameter type is still not defined, assume that
				 * it is unused. But we put a default INT4OID type for such
				 * unused parameters to keep the parameter pushdown code
				 * happy.
				 *
				 * These unused parameters are never accessed during
				 * execution and we will just a null value for these
				 * "dummy" parameters. But including them here ensures that
				 * we send down the parameters in the correct order and at
				 * the position that the datanode needs
				 */
				if (OidIsValid(param->ptype))
				{
					rstmt->remoteparams[paramno].paramused = 1;
					rstmt->remoteparams[paramno].paramtype = param->ptype;
				}
				else
				{
					rstmt->remoteparams[paramno].paramused = 0;
					rstmt->remoteparams[paramno].paramtype = INT4OID;
				}

				rstmt->remoteparams[paramno].paramkind = PARAM_EXTERN;
				rstmt->remoteparams[paramno].paramid = i + 1;
				paramno++;
			}
		}
		if (remotesubplan && remotesubplan->remoteparams)
		{
			for (i = 0; i < remotesubplan->nparams; i++)
			{
				rstmt->remoteparams[paramno] = remotesubplan->remoteparams[i];
				paramno++;
			}
		}
		/* store actual number of parameters */
		rstmt->nParamRemote = paramno;
	}
	else
	{
		rstmt->remoteparams = NULL;
	}

	rstmt->rowMarks = estate->es_plannedstmt->rowMarks;
	if (remotesubplan && remotesubplan->num_workers > 0)
	{
		rstmt->parallelModeNeeded = true;
	}
	else
	{
		rstmt->parallelModeNeeded = estate->es_plannedstmt->parallelModeNeeded;
	}

#ifdef __AUDIT__
	if (enable_audit)
	{
		rstmt->queryString = estate->es_sourceText;
		rstmt->parseTree = estate->es_plannedstmt->parseTree;
	}
#endif
}

static void
SendFragmentCommandInfo(Fragment *fragment, EState *estate)
{
	GlobalTransactionId gxid = InvalidGlobalTransactionId;
	Snapshot            snapshot;
	int                 i;

	gxid = GetCurrentTransactionIdIfAny();
	snapshot = estate->es_snapshot;

	for (i = 0; i < fragment->connNum; i++)
	{
		PGXCNodeHandle *connection = fragment->connections[i];

		/* begin */
		if (pgxc_node_begin(1, &connection, gxid, true , true))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_CONNECTION_EXCEPTION),
			         errmsg("Could not begin transaction on data node:%s", connection->nodename)));
		}
		/* send fid flevel */
		if (pgxc_node_send_fid_flevel(connection, fragment->fid, fragment->level))
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			                errmsg("Failed to send fid to data node:%s, connection state is %d",
			                       connection->nodename, GetConnectionState(connection))));
		}
		if (ORA_MODE && pgxc_node_send_exec_env_if(connection))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to setup security context to data node:%s, connection state is %d",
					 connection->nodename, GetConnectionState(connection))));
		/* send snapshot */
		if (pgxc_node_send_snapshot(connection, snapshot))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_CONNECTION_EXCEPTION),
			         errmsg("Failed to send snapshot to data node:%s", connection->nodename)));
		}
		/* send cid */
		if (pgxc_node_send_cmd_id(connection, estate->es_snapshot->curcid) < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command ID"),
							errnode(connection)));
		}

		/* submit */
		if (pgxc_node_send_flush(connection))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command ID"),
							errnode(connection)));
		}

		connection->needClose = true;
	}
}

static void
SendFragmentPlan(Fragment *fragment, RemoteStmt *rstmt, EState *estate)
{
	StringInfoData str;
	int            msgLen;
	char          *paramType;
	int            i;
	int            tmp_num_params;
	char          *statement;
	char          *query = "Remote Fragment";
	int            instrument_options;

	Assert(fragment->connNum > 0);

	snprintf(fragment->cursor, NAMEDATALEN, "%ld_%ld_%d",
			 estate->queryid.timestamp_nodeid, estate->queryid.sequence, fragment->fid);

	statement = fragment->cursor;

	initStringInfo(&str);

	appendStringInfoChar(&str, 'p');

	/* Placeholder for msglen*/
	appendStringInfoString(&str, "0000");

	/* statement name */
	appendStringInfoString(&str, statement);
	appendStringInfoChar(&str, '\0');
	/* source query */
	appendStringInfoString(&str, query);
	appendStringInfoChar(&str, '\0');
	/* query plan */
	PG_TRY();
	{
		set_portable_output(true);
		outNode(&str, rstmt);
		appendStringInfoChar(&str, '\0');
	}
	PG_CATCH();
	{
		set_portable_output(false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	set_portable_output(false);
	/* parameter types number */
	tmp_num_params = pg_hton32(rstmt->nParamRemote);
	appendBinaryStringInfo(&str, (char *) &tmp_num_params, 4);

	/*
	 * instead of parameter ids we should send parameter names (qualified by
	 * schema name if required). The OIDs of types can be different on
	 * datanodes.
	 */
	for (i = 0; i < rstmt->nParamRemote; i++)
	{
		paramType = format_type_be(rstmt->remoteparams[i].paramtype);
		appendStringInfoString(&str, paramType);
		appendStringInfoChar(&str, '\0');
		pfree(paramType);
	}

#ifdef __OPENTENBASE_C__
	instrument_options = pg_hton32(estate->es_instrument);
	appendBinaryStringInfo(&str, (char *) &instrument_options, 4);
#endif

	msgLen = str.len - 1;
	msgLen = pg_hton32(msgLen);
	memcpy(str.data + 1, &msgLen, 4);

	for (i = 0; i < fragment->connNum; i++)
	{
		PGXCNodeHandle *connection = fragment->connections[i];

		if(enable_distri_print)
			elog(LOG, "send plan %s to node %s %d", statement, connection->nodename, connection->backend_pid);

		snprintf(fragment->cursor, NAMEDATALEN, "%ld_%ld_%d",
				 estate->queryid.timestamp_nodeid, estate->queryid.sequence, fragment->fid);

		/* Invalid connection state, return error */
		if (!IsConnectionStateIdle(connection))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command info"),
							errnode(connection)));

		/* send plan */
		if (pgxc_node_send_msg(connection, str.data, str.len))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send plan"),
							errnode(connection)));
		}

		SetTransitionInExtendedQuery(connection, true);
		connection->needClose = true;
	}
}

static void
AppendParamData(StringInfo buf, Oid ptype, int pused, Datum value, bool isnull)
{
	uint32 n32;

	/* Assume unused parameters to have null values */
	if (!pused)
		ptype = INT4OID;

	if (isnull)
	{
		n32 = pg_hton32(-1);
		appendBinaryStringInfo(buf, (char *) &n32, 4);
	}
	else
	{
		Oid		typOutput;
		bool	typIsVarlena;
		Datum	pval;
		char   *pstring;
		int		len;

		/* Get info needed to output the value */
		getTypeOutputInfo(ptype, &typOutput, &typIsVarlena);

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

		/* copy data to the buffer */
		len = strlen(pstring);
		n32 = pg_hton32(len);
		appendBinaryStringInfo(buf, (char *) &n32, 4);
		appendBinaryStringInfo(buf, pstring, len);
	}
}

static int
EncodeFragmentParameters(Plan *plan, int nparams, RemoteParam *remoteparams,
						 char **result, int *nparam_formats_o,
						 int16 **param_formats_o, EState *estate)
{

	int				nparam_formats;
	int 			i;
	int16		   *param_formats;
	uint16 			n16;
	bool			have_binary = false;
	StringInfoData	buf;

	if (nparams > 0)
	{
		initStringInfo(&buf);
		param_formats = (int16 *) palloc0(sizeof(int16) * nparams);
		nparam_formats = nparams;

		/* Number of parameter values */
		n16 = pg_hton16(nparams);
		appendBinaryStringInfo(&buf, (char *) &n16, 2);

		/* Parameter values */
		for (i = 0; i < nparams; i++)
		{
			RemoteParam *rparam = &remoteparams[i];
			int ptype = rparam->paramtype;
			int pused = rparam->paramused;
			if (rparam->paramkind == PARAM_EXTERN)
			{
				ParamExternData *param;
				param = &(estate->es_param_list_info->params[rparam->paramid - 1]);
				AppendParamData(&buf, ptype, pused, param->value, param->isnull);
			}
			else
			{
				/* treat PARAM_EXEC as null */
				AppendParamData(&buf, ptype, pused, (Datum)0, true);
			}
		}

		if (have_binary)
		{
			if (nparam_formats_o)
				*nparam_formats_o = nparam_formats;
			if (param_formats_o)
				*param_formats_o = param_formats;
		}
		else
		{
			if (nparam_formats_o)
				*nparam_formats_o = 0;
			if (param_formats_o)
				*param_formats_o = NULL;

			pfree(param_formats);
		}

		/* Take data from the buffer */
		*result = palloc(buf.len);
		memcpy(*result, buf.data, buf.len);
		pfree(buf.data);
		return buf.len;
	}
	else
	{
		if (nparam_formats_o)
			*nparam_formats_o = 0;
		if (param_formats_o)
			*param_formats_o = NULL;
		return 0;
	}
}

static void inline
handleAllResponseMessage(int count, FragmentTable *ftable)
{
	int offset = 0, connNum = 0, i = 1;
	ResponseCombiner combiner;
	InitResponseCombiner(&combiner, count, COMBINE_TYPE_NONE);
	combiner.extended_query = true;
	combiner.conn_count = count;
	combiner.connections = (PGXCNodeHandle **) palloc(count * sizeof(PGXCNodeHandle *));

	combiner.waitfor2 = true;

	for (; i < ftable->currentAssignIndex; ++i)
	{
		connNum = ftable->fragments[i]->connNum;
		memcpy(combiner.connections + offset, ftable->fragments[i]->connections, connNum * sizeof(PGXCNodeHandle *));
		offset += connNum;
	}

	/*
	* Binding a DN must be a blocking operation; we need to ensure that all
	* DNs have started their tqThread.
	*/
	ProcessResponseMessage(&combiner, NULL, RESPONSE_BIND_COMPLETE, false);
	CloseCombiner(&combiner);
}

static void
PrepareRemoteFragment(Fragment *fragment, EState *estate,
					  int paramlen, char *paramdata,
					  int nparam_formats, int16 *param_formats)
{
	int i;

	for (i = 0; i < fragment->connNum; i++)
	{
		PGXCNodeHandle *conn = fragment->connections[i];

		/* bind */
		if (pgxc_node_send_bind(conn, fragment->cursor, fragment->cursor,
								paramlen, paramdata,
								param_formats, nparam_formats,
								NULL, 0))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send bind"),
							errnode(conn)));
		}

#ifdef __RESOURCE_QUEUE__
		if (IS_PGXC_COORDINATOR && IsResQueueEnabled() && !superuser())
		{
			char ntype = PGXC_NODE_DATANODE;
			int handle_PGXCNodeId = PGXCNodeGetNodeId(conn->nodeoid, &ntype);

			if (handle_PGXCNodeId >= 0 && handle_PGXCNodeId < NumDataNodes)
			{
				int64 network_bytes_limit = 0;
				int16 dn_dprocess_number = 0;

				dn_dprocess_number = estate->es_perdn_dprocess_num[handle_PGXCNodeId];
				network_bytes_limit = EstimateDProcessNetworkBytes(dn_dprocess_number);
				SendResQueueInfoToDProcess(conn, network_bytes_limit);
			}
		}
#endif

		if (IS_PGXC_COORDINATOR && IsResGroupActivated())
		{
			if (pgxc_node_send_resginfo(conn))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Failed to send resgroupinfo"),
								errnode(conn)));
			}
		}

		/* submit */
		if (estate->is_cursor)
		{
			/* need to wait the replay */
			if (SendFlushRequest(conn))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("Failed to send flush command"),
								errnode(conn)));
		}
		else
		{
			if (pgxc_node_send_flush(conn))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
								errmsg("Failed to send command"),
								errnode(conn)));
		}

		UpdateConnectionState(conn, DN_CONNECTION_STATE_BIND);
	}
}

static void
DispatchFragment(Fragment *fragment, EState *estate)
{
	RemoteStmt rstmt;
	int        paramlen = 0;
	int        nparam_formats = 0;
	char      *paramdata = NULL;
	int16     *param_formats = NULL;

	redirect_remote_subplan_recv(estate->es_plannedstmt->subplans, fragment);

	memset(&rstmt, 0, sizeof(RemoteStmt));
	rstmt.type = T_RemoteStmt;
	rstmt.planTree = fragment->plan;
	rstmt.jitFlags = estate->es_jit_flags;
	rstmt.fragmentType = fragment->ftype;
	rstmt.fragment_work_mem = estate->es_plannedstmt->planned_work_mem;
	rstmt.fragment_num = estate->es_plannedstmt->fragmentNum;
	rstmt.queryid = estate->queryid;

	/* send command info */
	SendFragmentCommandInfo(fragment, estate);
	/* encode plan */
	BuildFragmentStmt(fragment, estate, &rstmt);
	/* send plan*/
	SendFragmentPlan(fragment, &rstmt, estate);

	/* encode parameters */
	paramlen = EncodeFragmentParameters(fragment->plan, rstmt.nParamRemote,
										rstmt.remoteparams, &paramdata,
										&nparam_formats, &param_formats,
										estate);
	/* send bind */
	if (!estate->is_cursor)
		PrepareRemoteFragment(fragment, estate, paramlen, paramdata,
							  nparam_formats, param_formats);
	else
	{
		fragment->nparam_formats = nparam_formats;
		if (nparam_formats > 0)
		{
			fragment->param_formats = palloc(sizeof(int16) * nparam_formats);
			memcpy(fragment->param_formats, param_formats, sizeof(int16) * nparam_formats);
		}

		fragment->paramlen = paramlen;
		if (paramlen > 0)
		{
			fragment->paramdata = palloc(paramlen);
			memcpy(fragment->paramdata, paramdata, paramlen);
		}
	}
}

void
RemoteControllerBindListen(RemoteFragmentController *ctl, Fragment *fragment)
{
	int i;
	struct epoll_event event;
	RemoteFragmentController *control = ctl;
	int cnt;

	if (!fragment)
		return ;

	/* events can not be null */
	cnt = control->nConnections + fragment->connNum;
	control->events = (struct epoll_event *) repalloc(control->events, sizeof(struct epoll_event) * cnt);

	for (i = 0; i < fragment->connNum; i++)
	{
		PGXCNodeHandle *conn = fragment->connections[i];

		if (conn->sock == NO_SOCKET)
			continue;

		conn->ftype = fragment->ftype;

		event.data.ptr = conn;
		event.events = EPOLLIN | EPOLLERR | EPOLLHUP;

		if (epoll_ctl(control->efd, EPOLL_CTL_ADD, conn->sock, &event) == -1)
		{
			int err = errno;
			elog(ERROR,"could not bind sock to epoll %s", strerror(err));
		}

		DrainConnectionAndSetNewCombiner(conn, control->combiner);
		control->connections = lappend(control->connections, conn);
		control->nConnections++;
	}

}

void
RewindRemoteController(RemoteFragmentController *control)
{
	ListCell *lc;
	struct epoll_event event;
	MemoryContext old;

	if (!IS_PGXC_COORDINATOR || control->efd != -1)
		return ;

	old = MemoryContextSwitchTo(control->estate->es_ftable->ftcxt);

	control->efd = epoll_create1(0);
	if (control->efd == -1)
		elog(ERROR, "Fail to create epoll, err_msg:%s", strerror(errno));

	ResourceOwnerEnlargeFd(control->owner);
	ResourceOwnerRememberFd(control->owner, control->efd);

	control->connections = list_concat(control->connections,
									   control->free_connections);
	control->free_connections = NIL;

	if (control->libpq)
	{
		Fragment *fragment = control->libpq;
		RemoteFragmentState *fstate = fragment->pstate;
		ResponseCombiner *combiner = &fstate->combiner;
		int i;

		memcpy(combiner->connections, fragment->connections,
			   sizeof(PGXCNodeHandle *) * fragment->connNum);
		combiner->conn_count = combiner->node_count = fragment->connNum;
		combiner->request_type = REQUEST_TYPE_QUERY;

		for (i = 0; i < fragment->connNum; i++)
			fragment->connections[i]->combiner = combiner;
	}

	foreach(lc, control->connections)
	{
		PGXCNodeHandle *conn = lfirst(lc);

		event.data.ptr = conn;
		event.events = EPOLLIN | EPOLLERR | EPOLLHUP;

		if (epoll_ctl(control->efd, EPOLL_CTL_ADD, conn->sock, &event) == -1)
			elog(ERROR,"could not bind sock to epoll %s", strerror(errno));

		conn->combiner = control->combiner;
	}

	MemoryContextSwitchTo(old);
}

void*
InitRemoteController(EState *estate)
{
	RemoteFragmentController *control = NULL;
	int efd = -1;

	control = palloc0(sizeof(RemoteFragmentController));
	control->owner = CurrentResourceOwner;

	control->free_connections = NIL;
	control->connections = NIL;
	control->estate = estate;
	/* at lease one for listen external fd */
	control->nConnections = 1;
	control->events = palloc0(sizeof(struct epoll_event));

	control->efd = efd = epoll_create1(0);
	if (efd == -1)
		elog(ERROR, "Fail to create epoll, err_msg:%s", strerror(errno));

	ResourceOwnerEnlargeFd(control->owner);
	ResourceOwnerRememberFd(control->owner, efd);

	return control;
}

static void
ReleaseRemoteController(RemoteFragmentController *ctl)
{
	if (ctl->efd == -1)
		return ;

	ResourceOwnerForgetFd(ctl->owner, ctl->efd);
	ctl->efd = -1;
}

int
RunRemoteControllerWithFD(int fd, void* controller, int timeout)
{
	RemoteFragmentController *ctl = controller;
	struct epoll_event event;
	int ret;

	if (ctl->efd == -1)
		return RemoteControllerEventFinish;

	if (fd == -1)
		elog(ERROR, "Wrong fn id or efd");

	event.data.ptr = NULL;
	event.events = EPOLLIN | EPOLLERR | EPOLLHUP;
	if (epoll_ctl(ctl->efd, EPOLL_CTL_ADD, fd, &event) == -1)
		elog(ERROR, "Fail to add socket to epoll errno: %s", strerror(errno));

	ctl->external_fd = true;

	PG_TRY();
	{
		for (;;)
		{
			ret = RunRemoteController(controller,  false, true, timeout);
			if (ret == RemoteControllerEventFinish || ret == RemoteControllerEventFN)
				break;
		}
	}
	PG_CATCH();
	{
		ctl->external_fd = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (epoll_ctl(ctl->efd, EPOLL_CTL_DEL, fd, &event) == -1)
		elog(ERROR, "Fail to delete socket to epoll errno: %s", strerror(errno));
	ctl->external_fd = false;

	return ret;
}

int
RunRemoteControllerWithFN(TupleQueueReceiver *receiver, int tapenum, void* controller)
{
	local_mq_handle *mqh = receiver->queue[tapenum];
	int fn_fd;
	int ret;

	if (!IS_PGXC_COORDINATOR || local_mq_detached(mqh) ||
		local_mq_get_remain(mqh))
		return 0;

	fn_fd = local_mq_get_receive_pipe(mqh);

	/* tell TupleQueueThread we are waiting */
	local_mq_set_wait(mqh, true);
	ret = RunRemoteControllerWithFD(fn_fd, controller, -1);
	local_mq_set_wait(mqh, false);

	return ret;
}

static void
RemoteControllerPurneIdle(RemoteFragmentController *control, bool ignore_error)
{
	ListCell *lc;
	PGXCNodeHandle *conn;
	List *delete = NIL;

	foreach(lc, control->connections)
	{
		conn = (PGXCNodeHandle *) lfirst(lc);

		if (HAS_MESSAGE_BUFFERED(conn))
			RemoteControlMessageProcess(conn, control, ignore_error);

		if (IsConnectionStateIdle(conn))
			delete = lappend(delete, conn);
	}

	foreach(lc, delete)
	{
		conn = (PGXCNodeHandle *) lfirst(lc);

		control->connections = list_delete_ptr(control->connections, conn);
		control->free_connections = lappend(control->free_connections, conn);
	}
}

void
RemoteControlMessageProcess(PGXCNodeHandle *conn, RemoteFragmentController *control, bool ignore_error)
{
	for (;;)
	{
		int res;
		ResponseCombiner *combiner = control->combiner;
		res = handle_response(conn, combiner);

		if (res == RESPONSE_EOF)
			break;
		else if (res == RESPONSE_READY)
			elog(ERROR, "should not receive ready for completion, err_msg:%s",conn->error);
		else if (res == RESPONSE_ERROR)
		{
			if (combiner)
				pgxc_node_report_error(combiner);
			else
				elog(ERROR, "%s", conn->error);
		}
		else if (res == RESPONSE_COMPLETE)
		{
			if (IsConnectionStateError(conn))
				elog(ERROR, "Remote node FATAL error: %s", conn->error);
		}
		else if (res == RESPONSE_DATAROW || res == RESPONSE_TUPDESC ||
				 res == RESPONSE_COPY || res == RESPONSE_SUSPENDED)
		{
			if (!ignore_error)
				elog(ERROR, "Unexpected response %d from remote connection", res);
		}
	}
}

RemoteControllerRet
RunRemoteController(RemoteFragmentController *control,bool event_return, bool ignore_error, int timeout)
{
	int n;
	int i;
	PGXCNodeHandle *conn = NULL;
	struct epoll_event *events;
	int epoll_timeout = 10;
	struct timespec timeout_start, now;
	MemoryContext old;

	if (timeout != -1)
		epoll_timeout = timeout < 10 ? timeout: epoll_timeout;

	clock_gettime(CLOCK_REALTIME, &timeout_start);
	events = control->events;

	/* nothing to listen */
	if ((list_length(control->connections) == 0 && !control->external_fd) || control->efd == -1)
		return RemoteControllerEventFinish;

	old = MemoryContextSwitchTo(control->estate->es_ftable->ftcxt);

	RemoteControllerPurneIdle(control, ignore_error);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/* Wait for available event */
		n = epoll_wait(control->efd, events, control->nConnections, epoll_timeout);

		if (n == 0 && timeout != -1)
		{
			double diff;

			clock_gettime(CLOCK_REALTIME, &now);
			diff = (now.tv_sec - timeout_start.tv_sec) * 1000.0 + (now.tv_nsec - timeout_start.tv_nsec) / 1e6;
			if (diff >= timeout)
			{
				MemoryContextSwitchTo(old);
				return RemoteControllerEventTimeout;
			}
		}

		for (i = 0; i < n; i++)
		{
			conn = events[i].data.ptr;

			if (conn == NULL)
			{
				MemoryContextSwitchTo(old);
				return RemoteControllerEventFN;
			}

			if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP))
			{
				UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
				SetTxnState(conn, TXN_STATE_ERROR);

				ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("Node closed the connection unexpectedly"),
						errnode(conn)));
			}
			else if (events[i].events & EPOLLIN)
			{
				int read_status = pgxc_node_read_data(conn, true);

				if (read_status == EOF || read_status < 0)
				{
					UpdateConnectionState(conn, DN_CONNECTION_STATE_FATAL);
					ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("Node closed the connection unexpectedly"),
							errnode(conn)));

				}
			}
			else
				continue;

			RemoteControlMessageProcess(conn, control, ignore_error);

			if (IsConnectionStateIdle(conn))
			{
				control->connections = list_delete_ptr(control->connections, conn);
				control->free_connections = lappend(control->free_connections, conn);
			}
		}

		if (event_return && n)
		{
			MemoryContextSwitchTo(old);
			return RemoteControllerEventConnState;
		}
	}

	return RemoteControllerEventFinish;
}

/* execute fragment on remote nodes */
void
ExecDispatchRemoteFragment(FragmentTable *ftable, EState *estate)
{
	int i;
	int count = 0;

	if (ftable->currentAssignIndex <= 1)
		return;

	estate->es_ftable = ftable;
	estate->es_distributed = true;

	/* dispatch fragment */
	for (i = 1; i < ftable->currentAssignIndex; i++)
	{
		if (IsA(ftable->fragments[i]->plan, RemoteSubplan))
		{
			RemoteSubplan *remotesubplan = (RemoteSubplan *)ftable->fragments[i]->plan;

			if (remotesubplan->under_subplan)
			{
				/* set subplan's params before go through */
				SetFragmentSubPlanParams(remotesubplan, estate);
			}

			if (ftable->fragments[i]->level == 0)
				elog(ERROR, "fragment fid %d level should not be 0",
					ftable->fragments[i]->fid);
		}
	}

	for (i = 1; i < ftable->currentAssignIndex; i++)
	{
		DispatchFragment(ftable->fragments[i], estate);
		count += ftable->fragments[i]->connNum;
	}

	if (count > 0 && !estate->is_cursor)
		handleAllResponseMessage(count, ftable);

	if (!IsA(ftable->fragments[0]->plan, RemoteSubplan))
	{
		redirect_remote_subplan_recv(estate->es_plannedstmt->subplans,
									 ftable->fragments[0]);
	}
}

static void
ExecuteRemoteFragment(Fragment *fragment, FNQueryId queryid, EState *estate)
{
	int i = 0;

	for (i = 0; i < fragment->connNum; i++)
	{
		PGXCNodeHandle *conn = fragment->connections[i];

		/* resend fid flevel */
		if (pgxc_node_send_fid_flevel(conn, fragment->fid, fragment->level))
		{
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
					errmsg("Failed to send fid connection state is %d",GetConnectionState(conn)),
					errnode(conn)));
		}

		/*
		 * Update Command Id. Other command may be executed after we
		 * prepare and advanced Command Id. We should use one that
		 * was active at the moment when command started.
		 */
		if (pgxc_node_send_cmd_id(conn, estate->es_snapshot->curcid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command ID"),
							errnode(conn)));
		}

		if (ORA_MODE && pgxc_node_send_exec_env_if(conn))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Failed to setup security context"),
							errnode(conn)));
		/*
		 * Resend the snapshot as well since the connection may have
		 * been buffered and use by other commands, with different
		 * snapshot. Set the snapshot back to what it was
		 */
		if (pgxc_node_send_snapshot(conn, estate->es_snapshot))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send snapshot"),
							errnode(conn)));

		/* senquence*/
		if (pgxc_node_send_cmd_seq(conn))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send sequence"),
							errnode(conn)));

		if (enable_distri_print)
			elog(LOG, "ExecuteRemoteFragment pgxc_node_send_execute %s %d %s", conn->nodename, conn->backend_pid, fragment->cursor);

		/* execute */
		if (pgxc_node_send_execute(conn, fragment->cursor, 0, true))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send execute"),
							errnode(conn)));

		/* executing a blocking remote fragment */
		if (IsA(fragment->plan, RemoteSubplan))
		{
			RemoteSubplan *plan = (RemoteSubplan *)fragment->plan;

			if (plan->under_subplan || plan->cacheSend)
				conn->block_connection = true;
		}

		/* submit */
		if (pgxc_node_send_my_sync(conn))
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("Failed to send command"),
							errnode(conn)));

		if(enable_distri_print)
		{
			elog(LOG, "execute ExecuteRemoteFragment, node: %s pid %d",
				conn->nodename, conn->backend_pid);
			PrintHandleState(conn);
		}

	}
}

void
ExecRunRemoteFragment(EState *estate)
{
	int i = 0;
	MemoryContext old = NULL;
	FragmentTable *ftable = (FragmentTable *)estate->es_ftable;

	old = MemoryContextSwitchTo(estate->es_query_cxt);


	/* first time run or rewinding */
	if (!estate->es_fragment_execute)
	{
		for (i = 1; i < ftable->currentAssignIndex; i++)
		{
			int j;
			Fragment *fragment = ftable->fragments[i];
			for (j = 0; j < fragment->connNum; j++)
			{
				snprintf(fragment->cursor, NAMEDATALEN, "%ld_%ld_%d",
						 estate->queryid.timestamp_nodeid, estate->queryid.sequence, fragment->fid);
			}
		}

		if (estate->is_cursor)
		{
			int count = 0;
			for (i = 1; i < ftable->currentAssignIndex; i++)
			{
				Fragment *fragment = ftable->fragments[i];
				if (fragment->pstate != NULL &&
					castNode(RemoteSubplan, fragment->plan)->targetNodeType == CoordinatorNode)
				{
					ExecFinishInitRemoteFragment(fragment->pstate);
				}
				count += fragment->connNum;
				SendFragmentCommandInfo(fragment, estate);
				PrepareRemoteFragment(fragment, estate,
									  fragment->paramlen, fragment->paramdata,
									  fragment->nparam_formats, fragment->param_formats);
			}

			if (count > 0)
				handleAllResponseMessage(count, ftable);
		}
		for (i = 1; i < ftable->currentAssignIndex; i++)
			ExecuteRemoteFragment(ftable->fragments[i], estate->queryid, estate);
	}

	/* remember estates so that we can rollback */
	estate->es_fragment_execute = true;

	MemoryContextSwitchTo(old);
}

static void
FragmentSendEndQuery(RemoteFragmentController *control)
{
	int i, j;
	FragmentTable *ftable = control->estate->es_ftable;

	for (i = 1; i < ftable->currentAssignIndex; i++)
	{
		Fragment *fragment = ftable->fragments[i];

		if (!(IsA(fragment->plan, RemoteSubplan)))
			continue;

		for (j = 0; j < fragment->connNum; j++)
		{
			if (!IsConnectionStateIdle(fragment->connections[j]))
				break;
		}

		if (j != fragment->connNum)
		{
			int dn_count = 0;
			int *dn_list = (int*)palloc(sizeof(int) * NumDataNodes);
			int *pid_list = (int*)palloc(sizeof(int) * NumDataNodes);

			for (j = 0; j < fragment->connNum; j++)
			{
				PGXCNodeHandle *handle = fragment->connections[j];
				dn_list[dn_count] = PGXCNodeGetNodeId(handle->nodeoid, NULL);
				pid_list[dn_count] = handle->backend_pid;
				++dn_count;
			}

			PoolManagerCancelQuery(dn_count, dn_list, pid_list, 0, NULL, SIGNAL_SIGUSR2);
			pfree(pid_list);
			pfree(dn_list);
		}
	}
}

/* Send end query to all fragments */
static void
SendEndAllRemoteFragment(RemoteFragmentController *control)
{
	do
	{
		FragmentSendEndQuery(control);
		RunRemoteController(control,  false, true, 1);
	} while (list_length(control->connections) != 0);
}

/*
 * Send end query request to fragments that are under subplan,
 * and wait for all fragments to end.
 */
void
WaitRemoteFragmentDone(EState *estate)
{
	RemoteFragmentController *control = NULL;
	Fragment *dml_fragment = NULL;
	FragmentTable *ftable;
	Fragment *fragment;
	int i, j;

	if (!IS_PGXC_COORDINATOR)
		return ;

	if (enable_distri_print)
		elog(LOG, "Wait remote fragment done");

	if (estate == NULL || !estate->es_fragment_execute)
		return ;

	ftable = estate->es_ftable;
	if (!ftable)
		return ;

	for (i = 1; i < ftable->currentAssignIndex; i++)
	{
		fragment = ftable->fragments[i];
		if(enable_distri_print)
			elog(LOG, "Wait remote fragment done, conn number: %d", fragment->connNum);

		for (j = 0; j < fragment->connNum; j++)
		{
			PrintHandleState(fragment->connections[j]);
			fragment->connections[j]->used = false;
		}
	}

	control = estate->es_remote_controller;
	if (control == NULL)
		return ;

	if (control->connections == NIL)
		goto finish;

	/* send end query to for select */
	if (estate->es_plannedstmt->commandType == CMD_SELECT)
		SendEndAllRemoteFragment(control);

	/* send end query to for dml if needed */
	if (dml_fragment == NULL &&
		estate->es_plannedstmt->commandType != CMD_SELECT &&
		ftable->currentAssignIndex > 1)
	{
		for (i = 1; i < ftable->currentAssignIndex; i++)
		{
			fragment = ftable->fragments[i];
			/* find the dml fragment (only one) */
			if (IsA(fragment->plan, RemoteSubplan) &&
				(IsA(fragment->plan->lefttree, ModifyTable) ||
				 IsA(fragment->plan->lefttree, MultiModifyTable)))
			{
				dml_fragment = fragment;
				break;
			}
		}

		/* it could be a query that DML on a table in CN */
		if (!dml_fragment)
		{
			fragment = ftable->fragments[0];
			if (IsA(fragment->plan, ModifyTable) || IsA(fragment->plan, MultiModifyTable))
				dml_fragment = fragment;
		}

		/* still couldn't find? raise an error */
		if (!dml_fragment)
			elog(ERROR, "failed to find dml_fragment");
	}

	if (dml_fragment)
	{
		do
		{
			for (j = 0; j < dml_fragment->connNum; j++)
			{
				if (!IsConnectionStateIdle(dml_fragment->connections[j]))
					break;
			}

			if (j == dml_fragment->connNum)
			{
				/* dml is finished, we can send end query */
				SendEndAllRemoteFragment(control);
				break;
			}
			RunRemoteController(control, true, false, -1);
		} while (list_length(control->connections) != 0);
	}

finish:

	ReleaseRemoteController(control);
}

/* close portal and send sync msg */
void
CloseRemoteFragment(FragmentTable *ftable)
{
	int i;
	int fragmentIndex;

	for (fragmentIndex = 1; fragmentIndex < ftable->currentAssignIndex; fragmentIndex++)
	{
		int32 count    = 0;
		ResponseCombiner combiner;
		Fragment *fragment = ftable->fragments[fragmentIndex];

		InitResponseCombiner(&combiner, fragment->connNum, COMBINE_TYPE_NONE);
		combiner.extended_query = true;
		combiner.ignore_datarow = true;
		combiner.conn_count = fragment->connNum;
		combiner.connections = (PGXCNodeHandle **) palloc(
					combiner.conn_count * sizeof(PGXCNodeHandle *));
		memcpy(combiner.connections, fragment->connections,
					combiner.conn_count * sizeof(PGXCNodeHandle *));

		/* Close statements, even if they never were bound */
		for (i = 0; i < combiner.conn_count; i++)
		{
			PGXCNodeHandle *conn = combiner.connections[i];

			if (!IsConnectionStateClose(conn))
			{
				if(enable_distri_print)
					elog(LOG, "pgxc_node_send_close %s %s %d", fragment->cursor, conn->nodename, conn->backend_pid);

				if (pgxc_node_send_close(conn, false, fragment->cursor) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed to close statement on data node "
									"%s backend_pid %d",
									conn->nodename, conn->backend_pid)));
				}

				if (pgxc_node_send_close(conn, true, fragment->cursor) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed to close statement on data node "
									"%s backend_pid %d",
									conn->nodename, conn->backend_pid)));
				}

				/* Send SYNC and wait for ReadyForQuery */
				if (pgxc_node_send_sync(conn) != 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
							 errmsg("Failed to sync msg to node %s backend_pid:%d",
									conn->nodename, conn->backend_pid)));
				}
				/*
				 * Formally connection is not in QUERY state, we set the state to read
				 * CloseDone and ReadyForQuery responses. Upon receiving ReadyForQuery
				 * state will be changed back to IDLE and conn->coordinator will be
				 * cleared.
				 */
				UpdateConnectionState(conn, DN_CONNECTION_STATE_CLOSE);
				conn->needClose = false;
				conn->block_connection = false;
			}
		}

		count = combiner.conn_count;
		while (count > 0)
		{
			if (pgxc_node_receive(count, combiner.connections, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("Failed to close remote fragment")));

			i = 0;
			while (i < count)
			{
				int res = handle_response(combiner.connections[i], &combiner);

				if (IsConnectionStateFatal(combiner.connections[i]))
					ereport(ERROR,
							(errcode(ERRCODE_CONNECTION_EXCEPTION),
									errmsg("Unexpected FATAL on Connection "
										   "to node %s pid %d",
										   combiner.connections[i]->nodename,
										   combiner.connections[i]->backend_pid)));

				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_READY)
				{
					/* Done, connection is reade for query */
					if (--count > i)
						combiner.connections[i] =
								combiner.connections[count];
				}
				else if (res == RESPONSE_DATAROW)
				{
					/*
					 * If we are finishing slowly running remote subplan while it
					 * is still working (because of Limit, for example) it may
					 * produce one or more tuples between connection cleanup and
					 * handling Close command. One tuple does not cause any problem,
					 * but if it will not be read the next tuple will trigger
					 * assertion failure. So if we got a tuple, just read and
					 * discard it here.
					 */
					pfree(combiner.currentRow);
					combiner.currentRow = NULL;
				}
				else if (res == RESPONSE_ERROR)
				{
					pgxc_node_report_error(&combiner);
				}
				else if (res == RESPONSE_COMPLETE)
				{
					continue;
				}
				else
				{
					elog(ERROR, "Unexpected message ret:%d in CloseRemoteFragment", res);
				}
			}
		}

		ValidateAndCloseCombiner(&combiner);
	}
}

/* reset necessary status of all connections in a ftable */
void
RemoteFragmentResetConnection(EState *estate)
{
	FragmentTable *ftable = estate->es_ftable;

	if (ftable->currentAssignIndex <= 1)
		return;

	RewindRemoteController(estate->es_remote_controller);
}

void
RemoteFragmentSigusr2Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress)
	{
		end_query_requested = true;
	}

	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}

bool
CheckParallelDone(ParallelWorkerStatus *parallel_status, ParallelContext *pcxt)
{
	int exec_done, i;

	CHECK_FOR_INTERRUPTS();

	if (end_query_requested)
	{
		/* Kill each worker in turn. */
		if (pcxt->worker != NULL)
		{
			for (i = 0; i < pcxt->nworkers_launched; ++i)
			{
				if (pcxt->worker[i].error_mqh != NULL)
				{
					EndQueryBackgroundWorker(pcxt->worker[i].bgwhandle);
				}
			}
		}
	}

	exec_done = 0;

	for (i = 0; i < parallel_status->numExpectedWorkers; i++)
	{
		if (parallel_status->workerSendStatus[i])
			exec_done++;
	}

	elog(DEBUG1, "CheckParallelDone %d %d", exec_done, parallel_status->numLaunchedWorkers);

	return (exec_done == parallel_status->numLaunchedWorkers);
}

static bool
redirect_subplan_recv(Node *node, void *context)
{
	redirect_subplan_recv_context *ctx;

	if (node == NULL)
		return false;

	ctx = (redirect_subplan_recv_context *) context;

	if (IsA(node, RemoteSubplan))
	{
		RemoteSubplan *plan = (RemoteSubplan *) node;
		FragmentDest  *dest = NULL;
		ListCell	  *lc;

		foreach(lc, plan->dests)
		{
			dest = lfirst_node(FragmentDest, lc);
			if (dest->recvfid == ctx->fid)
				break;
			else
				dest = NULL;
		}

		if (dest == NULL)
			elog(ERROR, "failed to find parent framgent for initplan");

		plan->fid = dest->fid;
		plan->targetNodeType = dest->targetNodeType;
		return false;
	}
	else if (IsA(node, SubPlan))
	{
		/* the expression_tree_walker won't lookup real subplan, do it here */
		SubPlan	*subplan = castNode(SubPlan, node);
		Plan	*plan;

		if (!subplan->isInitPlan)
		{
			plan = ((Plan *) list_nth(ctx->subplans,
									  (subplan)->plan_id - 1));
			redirect_subplan_recv((Node *) plan, context);
		}
	}

	return plan_or_expression_tree_walker(node, redirect_subplan_recv, context);
}

void
redirect_remote_subplan_recv(List *subplans, Fragment *receiver)
{
	ListCell *lc;
	Plan	 *plan = receiver->plan;
	MemoryContext old;

	if (!IS_PGXC_COORDINATOR)
		return;

	old = MemoryContextSwitchTo(GetMemoryChunkContext(plan));

	foreach(lc, plan->exec_subplan)
	{
		redirect_subplan_recv_context context;
		SubPlan	*sp = lfirst_node(SubPlan, lc);
		Plan 	*subplan;

		subplan = list_nth(subplans, sp->plan_id - 1);
		context.fid = receiver->fid;
		context.level = receiver->level;
		context.subplans = subplans;
		(void) redirect_subplan_recv((Node *) subplan, &context);
	}

	MemoryContextSwitchTo(old);
}

static bool
redirect_subplan_send(Node *node, void *context)
{
	redirect_subplan_send_context *ctx;

	if (node == NULL)
		return false;

	ctx = (redirect_subplan_send_context *) context;

	if (IsA(node, RemoteSubplan))
	{
		RemoteSubplan	*plan = (RemoteSubplan *) node;
		FragmentDest	*dest = makeNode(FragmentDest);

		dest->fid = (plan->dests != NIL ?
					 ++ctx->glob->fragmentNum : plan->fid);
		if (ctx->recv_top)
		{
			dest->targetNodeType = CoordinatorNode;
			dest->targetNodes = list_make1_int(PGXCNodeId - 1);
			dest->recvfid = 0;
		}
		else
		{
			dest->targetNodeType = DataNode;
			dest->targetNodes = list_copy(ctx->nodeList);
			dest->recvfid = ctx->recv_fid;
		}

		plan->dests = list_append_unique(plan->dests, dest);

		if (ctx->isCteWithCB && list_length(plan->dests) > 1)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                errmsg("WITH query \"%s\" with [CONNECT BY] cannot be referenced by "
			                       "multiple fragments.",
			                       ctx->ctename)));
		return false;
	}
	else if (IsA(node, SubPlan))
	{
		/* the expression_tree_walker won't lookup real subplan, do it here */
		SubPlan	*subplan = castNode(SubPlan, node);
		Plan	*plan;

		if (!subplan->isInitPlan &&
			!bms_is_member(subplan->plan_id, ctx->processed_subplan))
		{
			ctx->processed_subplan =
				bms_add_member(ctx->processed_subplan, subplan->plan_id);
			plan = ((Plan *) list_nth(ctx->glob->subplans,
									  (subplan)->plan_id - 1));
			redirect_subplan_send((Node *) plan, context);
		}
	}

	return plan_or_expression_tree_walker(node, redirect_subplan_send, context);
}

void
redirect_remote_subplan_send(PlannerGlobal *glob, Plan *receiver)
{
	ListCell *lc;
	redirect_subplan_send_context context = {0};

	if (!IS_PGXC_COORDINATOR)
		return;

	context.glob = glob;

	if (IsA(receiver, RemoteSubplan))
	{
		RemoteSubplan *plan = castNode(RemoteSubplan, receiver);

		context.nodeList = list_copy(plan->nodeList);
		context.recv_fid = plan->fid;
	}
	else	/* cn */
	{
		context.recv_top = true;
	}

	foreach(lc, receiver->exec_subplan)
	{
		SubPlan	*sp = lfirst_node(SubPlan, lc);
		Plan 	*initplan;

		initplan = list_nth(glob->subplans, sp->plan_id - 1);

		if (!context.recv_top && context.nodeList == NIL)
			elog(ERROR, "Unable to find the nodelist of the fragment %d in DN", context.recv_fid);

		if (bms_is_member(sp->plan_id, context.processed_subplan))
			continue;

		context.processed_subplan = bms_add_member(context.processed_subplan, sp->plan_id);

		if (sp->subLinkType == CTE_SUBLINK && sp->hasConnectBy)
		{
			context.isCteWithCB = true;
			context.ctename = sp->plan_name;
		}

		(void) redirect_subplan_send((Node *) initplan, &context);

		glob->exec_subplan = list_delete_ptr(glob->exec_subplan, sp);
	}

	bms_free(context.processed_subplan);
}
