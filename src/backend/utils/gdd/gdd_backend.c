/*-------------------------------------------------------------------------
 *
 * gdd_backend.c
 *	  Global DeadLock Detector - Backend
 *
 *
 * Copyright (c) 2018-2020 VMware, Inc. or its affiliates.
 * Copyright (c) 2021-Present OpenTenBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/backend/utils/gdd/gdd_backend.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/procarray.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"
#include "utils/gdd.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "utils/gdd_common.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "catalog/namespace.h"
#include "catalog/pgxc_node.h"
#include "pgstat.h"
#include "utils/elog.h"
#include "utils/gdd_detector.h"
#include "utils/gdd_detectorpriv.h"

#define RET_STATUS_OK 0
#define RET_STATUS_ERROR 1

/*
 * edge info
 */
typedef struct EdgeSatelliteData
{
	char *lockmode;
	char *locktype;
} EdgeSatelliteData;

/*
 * gdd rum mode
 */
typedef enum RunMode
{
    DISABLE_GLOBAL_DEADLOCK_DETECTION = 0,
    ONE_COORDINATOR_NODE_RUN = 1,
    CURRENT_COORDINATOR_NODE_RUN = 2
} RunMode;

#define GET_LOCKMODE_FROM_EDGE(edge) (((EdgeSatelliteData *) ((edge)->data))->lockmode)
#define GET_LOCKTYPE_FROM_EDGE(edge) (((EdgeSatelliteData *) ((edge)->data))->locktype)

void GlobalDeadLockDetectorMain(Datum main_arg);

static void GlobalDeadLockDetectorLoop(void);
static int  DoDeadLockCheck(void);

static void DumpGddCtx(GddCtx *ctx, StringInfo str);
static void DumpGddGraph(GddCtx *ctx, GddGraph *graph, StringInfo str);
static void DumpGddEdge(GddCtx *ctx, GddEdge *edge, StringInfo str);

static MemoryContext	gddContext;
static MemoryContext    oldContext;

static volatile sig_atomic_t got_SIGHUP = false;

int global_deadlock_detector_period = 5;
int global_deadlock_detector_mode = 0;
int	log_min_duration_detection = -1;

/*
 * SIGHUP: set flag to reload config file
 */
static void
SigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;

	if(MyProc)
		SetLatch(&MyProc->procLatch);
}

/*
 * GlobalDeadLockDetectorMain
 */
void
GlobalDeadLockDetectorMain(Datum main_arg)
{
	pqsignal(SIGHUP, SigHupHandler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGQUIT, quickdie);	/* hard crash time */
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

    StartTransactionCommand();
    /* Initialize XL executor. This must be done inside a transaction block. */
    InitMultinodeExecutor(false);
    CommitTransactionCommand();

	GlobalDeadLockDetectorLoop();

	/* One iteration done, go away */
	proc_exit(0);
}

/*
 * check deadlock loop
 */
static void
GlobalDeadLockDetectorLoop(void)
{
	int	status;
    TimestampTz begin = 0;
    TimestampTz end   = 0;
    int			cost  = 0;

	/* Allocate MemoryContext */
	gddContext = AllocSetContextCreate(TopMemoryContext,
									   "GddContext",
									   ALLOCSET_DEFAULT_SIZES);

	while (true)
	{
		int			rc;
		int			timeout;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

        CHECK_FOR_INTERRUPTS();

        CheckInvalidateRemoteHandles();

        /*
         *  execute under 2 conditions:
         *  1. if global_deadlock_detector_mode is ONE_COORDINATOR_NODE_RUN, use is_gdd_runner_cn to
         *  decide whether the current node should be executed
         *  2. if global_deadlock_detector_mode is CURRENT_COORDINATOR_NODE_RUN, run on current node
         */
		if ((global_deadlock_detector_mode == ONE_COORDINATOR_NODE_RUN && is_gdd_runner_cn()) ||
                        global_deadlock_detector_mode == CURRENT_COORDINATOR_NODE_RUN)
        {
            if (log_min_duration_detection >= 0)
            {
                begin = GetCurrentTimestamp();
            }

            StartTransactionCommand();
            status = DoDeadLockCheck();
            if (status == STATUS_OK)
                CommitTransactionCommand();
            else
                AbortCurrentTransaction();

            if (log_min_duration_detection >= 0)
            {
                /* log the duration if the detector is run too long */
                end = GetCurrentTimestamp();
                cost = (end - begin) / 1000;
                if (log_min_duration_detection == 0 ||
                            cost >= log_min_duration_detection * 1000)
                {
                    elog(LOG, "global deadlock detector duration: %d ms", cost);
                }
            }
        }

		timeout = got_SIGHUP ? 0 : global_deadlock_detector_period;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   timeout * 1000L,
					   WAIT_EVENT_GLOBAL_DEADLOCK_DETECTOR_MAIN);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	return;
}

/*
 * transfer gxid to txnid
 */
static int
TransferGxid2Txnid(GddCtx *ctx, char* gxid)
{
    int i_txn;
    Assert(ctx != NULL);

    /*check whether the gid is already existed*/
    for (i_txn = 0; i_txn < ctx->global_transaction_count; i_txn++)
    {
        if (strcmp(gxid, ctx->global_transaction[i_txn].gid) == 0)
        {
            break;
        }
    }

    /*insert this new transaction when gid is not find in pgxc_transaction*/
    if (i_txn >= ctx->global_transaction_count)
    {
        RPALLOC(ctx->global_transaction);
        InitGddCtxTransaction(ctx, ctx->global_transaction_count);
        memcpy(ctx->global_transaction[ctx->global_transaction_count].gid, gxid, strlen(gxid) + 1);
        i_txn = ctx->global_transaction_count++;
    }

    return i_txn;
}

/*
 * handle each wait relation tuple
 */
static void
HandleWaitRelationTuple(void *context, TupleTableRecord *tuple)
{
    GddCtx *ctx = (GddCtx *)context;
    char *temp = NULL;
    int from_txnid;
    int to_txnid;
    char* query;
    char*  waiter_gxid;
    char*  holder_gxid;
    int  waiter_pid;
    int  holder_pid;
    bool   solidedge;
    GddEdge *edge;
    char* node_name;
    int node;
    TransactionInfo *txn = NULL;
    EdgeSatelliteData *edge_data = NULL;
    Assert(ctx != NULL);

    /* 1. nodename */
    node_name = RecordGetvalue(tuple, 0);
    node = get_pgxc_nodeoid(node_name);

    /* 2. waiter gxid */
    waiter_gxid = RecordGetvalue(tuple, 1);
    /* 3. hloder gxid */
    holder_gxid = RecordGetvalue(tuple, 2);
    from_txnid = TransferGxid2Txnid(ctx, waiter_gxid);
    to_txnid = TransferGxid2Txnid(ctx, holder_gxid);

    /* 4. holdtillendxact */
    temp = RecordGetvalue(tuple, 3);
    solidedge =  (strcmp(temp, "true") == 0) ? true : false;

    /* 5. waiter pid */
    waiter_pid = strtoul(RecordGetvalue(tuple, 4), NULL, 10);
    /* 6. holder pid */
    holder_pid = strtoul(RecordGetvalue(tuple, 5), NULL, 10);

    edge_data = (EdgeSatelliteData *) palloc(sizeof(EdgeSatelliteData));
    /* 7. waiter lockmode */
    edge_data->lockmode = (char *)pstrdup(RecordGetvalue(tuple, 6));
    /* 8. waiter locktype */
    edge_data->locktype = (char *)pstrdup(RecordGetvalue(tuple, 7));

    txn = ctx->global_transaction;
    if (txn[from_txnid].query == NULL)
    {
        /* 9. waiter query sql */
        query = RecordGetvalue(tuple, 8);
        if (query)
        {
            if (strlen(query) <= 1024)
            {
                txn[from_txnid].query = (char *)pstrdup(query);
            }
            else
            {
                txn[from_txnid].query = (char *)palloc0(1025);
                strncpy(txn[from_txnid].query, query, 1024);
            }
        }
    }

    /* store wait relation into gdd context */
    edge = GddCtxAddEdge(ctx, node, from_txnid, to_txnid, solidedge);
    edge->from->pid = waiter_pid;
    edge->to->pid = holder_pid;
    edge->data = (void *) edge_data;
}

/*
 * kill txn's backend use txn id
 */
static void
KillTxnById(void *context, int txn_id)
{
    int i = 0;
    int node_count = 0;
    Oid* node = NULL;
    uint32* pid = NULL;
    char query[500];
    TupleTableSlots result;
    StringInfoData  str;
    GddCtx *ctx = (GddCtx *)context;
    TransactionInfo *txn = ctx->global_transaction;

    Assert(ctx != NULL);

    /* use to dump results*/
    initStringInfo(&str);

    node = txn[txn_id].node;
    pid = txn[txn_id].pid;
    node_count = txn[txn_id].node_count;

    for (i = 0; i < node_count; i++)
    {
        snprintf(query, 500,"select pg_cancel_backend(%u);", pid[i]);
        execute_on_single_node(node[i], query, 0, &result);
        DropTupleTableSlots(&result);

        appendStringInfo(&str, "(node: %d pid: %d)", node[i], pid[i]);
        if (i != node_count - 1)
        {
            appendStringInfo(&str, ",");
        }
    }

    elog(LOG, "These processes of %s are cancelled to break global deadlock: %s", txn[txn_id].gid, str.data);
    pfree(str.data);
}

/*
 * check if pg_unlock_wait_status funciton exist
 */
static bool
GetWaitStatusFuncExists(void)
{
    List	   *names;
    FuncCandidateList clist;
    char *pro_name = "pg_unlock_wait_status";

    /*
     * Parse the name into components and see if it matches any pg_proc
     * entries in the current search path.
     */
    names = list_make1(makeString(pro_name));
    clist = FuncnameGetCandidates(names, -1, NIL, false, false, true);

    if (clist == NULL || clist->next != NULL)
    {
        return false;
    }

    return true;
}

/*
 * If a transaction is autoxact.
 */
static bool
transaction_is_atxact(GddCtx *ctx, int txid)
{
	TransactionInfo	*tx = &ctx->global_transaction[txid];
	int	npid = tx->pid_count;
	int	i;

	for (i = 0; i < npid; i++)
	{
		if ((int) tx->pid[i] < 0)
			return true;
	}

	return false;
}

/*
 * do once deadlock check and break deadlock
 */
static int
DoDeadLockCheck(void)
{

	GddCtx		 *ctx;
	volatile int ret_status = RET_STATUS_OK;
    StringInfoData wait_graph_str;

	oldContext = MemoryContextSwitchTo(gddContext);

	PG_TRY();
	{
	    /*
	     * the global deadlock detector depends on pg_unlock_wait_status()
	     * now, and if the get wait status function not exists, break here
	     */
	    if (!GetWaitStatusFuncExists())
        {
            break;
        }

        /*
         * after unlocking a round of deadlock, it is possible to form a new deadlock,
         * so here we use the do while loop to detect until there is no deadlock
         */
        do
        {
            ctx = GddCtxNew();

			ctx->is_atxact_transaction = transaction_is_atxact;

            BuildWaitGraph(ctx, HandleWaitRelationTuple);

            GddCtxReduce(ctx);

            if (GddCtxEmpty(ctx))
            {
                break;
            }

            /*
             * at least one deadlock cycle is detected, and as all the invalid
             * verts and edges were filtered out at the beginning, the left
             * deadlock cycles are all valid ones.
             */
            initStringInfo(&wait_graph_str);
            DumpGddCtx(ctx, &wait_graph_str);

            elog(LOG,
                 "global deadlock detected! Final graph is :%s",
                 wait_graph_str.data);

            InitGddTxnPidNodeInfo(ctx);

            BreakDeadLock(ctx, KillTxnById);

            /* reset memory and detect again */
            MemoryContextReset(gddContext);
        } while(true);
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();
		ret_status = RET_STATUS_ERROR;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldContext);
	MemoryContextReset(gddContext);

	return ret_status;
}

/*
 * Dump the graphs.
 */
void
DumpGddCtx(GddCtx *ctx, StringInfo str)
{
	GddMapIter	iter;

	Assert(ctx != NULL);
	Assert(str != NULL);

	appendStringInfo(str, "{");

	gdd_ctx_foreach_graph(iter, ctx)
	{
		GddGraph	*graph = gdd_map_iter_get_ptr(iter);

		DumpGddGraph(ctx, graph, str);

		if (gdd_map_iter_has_next(iter))
			appendStringInfo(str, ",");
	}

	appendStringInfo(str, "}");
}

/*
 * Dump the verts and edges.
 */
static void
DumpGddGraph(GddCtx *ctx, GddGraph *graph, StringInfo str)
{
	GddMapIter	vertiter;
	GddListIter	edgeiter;
	bool		first = true;

	Assert(graph != NULL);
	Assert(str != NULL);

	appendStringInfo(str, "\"node %d\": [", graph->id);

	gdd_graph_foreach_out_edge(vertiter, edgeiter, graph)
	{
		GddEdge		*edge = gdd_list_iter_get_ptr(edgeiter);

		if (first)
			first = false;
		else
			appendStringInfo(str, ",");

		DumpGddEdge(ctx, edge, str);
	}

	appendStringInfo(str, "]");
}

/*
 * Dump edge.
 */
static void
DumpGddEdge(GddCtx *ctx, GddEdge *edge, StringInfo str)
{
    Assert(ctx != NULL);
    Assert(edge != NULL);
	Assert(edge->from != NULL);
	Assert(edge->to != NULL);
	Assert(str != NULL);

	appendStringInfo(str,
					 "\"pid %d of gxid %s waits for a %s lock on %s mode, "
					 "blocked by pid %d of gxid %s, query: %s\"",
					 edge->from->pid,
                     ctx->global_transaction[edge->from->id].gid,
					 GET_LOCKTYPE_FROM_EDGE(edge),
					 GET_LOCKMODE_FROM_EDGE(edge),
					 edge->to->pid,
                     ctx->global_transaction[edge->to->id].gid,
                     ctx->global_transaction[edge->from->id].query);
}

/*
 * ApplyGlobalDeadLockDetectorRegister
 *		Register a ApplyGlobalDeadLockDetectorRegister worker.
 */
void
ApplyGlobalDeadLockDetectorRegister(void)
{
    BackgroundWorker bgw;

    memset(&bgw, 0, sizeof(bgw));
    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "GlobalDeadLockDetectorMain");
    snprintf(bgw.bgw_name, BGW_MAXLEN, "global deadlock detector");
    bgw.bgw_restart_time = 5;
    bgw.bgw_notify_pid = 0;
    bgw.bgw_main_arg = (Datum) 0;

    RegisterBackgroundWorker(&bgw);
}
