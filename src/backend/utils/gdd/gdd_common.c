/*-------------------------------------------------------------------------
 *
 * gdd_common.c
 *	  some global deadlock detector(gdd) and pg_unlock extension's common functions
 *
 * Copyright (c) 2021-Present OpenTenBase development team, Tencent
 *
 * IDENTIFICATION
 *	  src/backend/utils/gdd/gdd_common.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq-fe.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/snapmgr.h"
#include "utils/gdd_common.h"

/*
 * recordGetvalue -- get attribute from TupleTableRecord
 * input: 	result, index of field
 * return:	attribute result
 */
char*
RecordGetvalue(TupleTableRecord *result, int field_num)
{
    return result->slot[field_num];
}

/*
 * DropTupleTableRecord -- free record result
 * input: result
 * return:
 */
static void
DropTupleTableRecord(TupleTableRecord *result)
{
    int i;

    for (i = 0; i < result->attnum; i++)
    {
        if (result->slot[i])
        {
            pfree(result->slot[i]);
            result->slot[i] = NULL;
        }
    }

    return;
}

/*
 * TTSgetvalue -- get attribute from TupleTableSlots
 * input: 	result, index of tuple, index of field
 * return:	attribute result
 */
char *
TTSgetvalue(TupleTableSlots *result, int tup_num, int field_num)
{
    return result->slot[tup_num][field_num];
}

/*
 * free tuples mem
 */
void
DropTupleTableSlots(TupleTableSlots *Slots)
{
    int i;
    int j;
    for (i = 0; i < Slots->slot_count; i++)
    {
        if (Slots->slot[i])
        {
            for (j = 0; j < Slots->attnum; j++)
            {
                if (Slots->slot[i][j])
                {
                    pfree(Slots->slot[i][j]);
                }
            }
            pfree(Slots->slot[i]);
        }
    }
    RFREE(Slots->slot);
    Slots->attnum = 0;
    return;
}

/*
 * execute_on_single_node -- execute query on certain node and get results
 * input: 	node oid, execute query, number of attribute in results, results
 * return:	(Datum) 0
 */
Datum
execute_on_single_node(Oid node, const char *query, int attnum, TupleTableSlots *tuples)
{
    int			ii;
    Datum		datum = (Datum) 0;
    bool		isnull = false;
    int			i_tuple;
    int			i_attnum;
    /*check health of node*/
    bool ishealthy;

#ifdef XCP
    EState				*estate;
    MemoryContext		oldcontext;
    RemoteQuery 		*plan;
    RemoteQueryState	*pstate;
    TupleTableSlot		*result = NULL;
    Var 				*dummy;
    char				ntype;
#endif

    /*get heathy status of query node*/
    ishealthy = PoolPingNodeRecheck(node);
#ifdef XCP
    /*
     * Make up RemoteQuery plan node
     */
    plan = makeNode(RemoteQuery);
    plan->combine_type = COMBINE_TYPE_NONE;
    plan->exec_nodes = makeNode(ExecNodes);
    plan->exec_type = EXEC_ON_NONE;

    ntype = PGXC_NODE_NONE;
    plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList,
                                             PGXCNodeGetNodeId(node, &ntype));
    if (ntype == PGXC_NODE_NONE)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Unknown node Oid: %u", node)));
    else if (ntype == PGXC_NODE_COORDINATOR)
    {
        plan->exec_type = EXEC_ON_COORDS;
    }
    else
    {
        plan->exec_type = EXEC_ON_DATANODES;
    }

    plan->sql_statement = (char*)query;
    plan->force_autocommit = false;
    /*
     * We only need the target entry to determine result data type.
     * So create dummy even if real expression is a function.
     */
    for (ii = 1; ii <= attnum; ii++)
    {
        dummy = makeVar(1, ii, TEXTOID, 0, InvalidOid, 0);	//TEXTOID??
        plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
                                             makeTargetEntry((Expr *) dummy, ii, NULL, false));
    }
    /* prepare to execute */
    estate = CreateExecutorState();
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    estate->es_snapshot = GetActiveSnapshot();
    pstate = ExecInitRemoteQuery(plan, estate, 0);
    MemoryContextSwitchTo(oldcontext);

    /*execute query on node when node is healthy*/
    INIT(tuples->slot);
    tuples->attnum = 0;
    if (ishealthy)
    {
        result = ExecRemoteQuery((PlanState *) pstate);
        tuples->attnum = attnum;
        i_tuple = 0;
        i_attnum = 0;
        while (result != NULL && !TupIsNull(result))
        {
            slot_getallattrs(result);
            RPALLOC(tuples->slot);
            tuples->slot[i_tuple] = (char **) palloc(attnum * sizeof(char *));

            for (i_attnum = 0; i_attnum < attnum; i_attnum++)
            {
                if (result->tts_values[i_attnum] != (Datum)0)
                {
                    tuples->slot[i_tuple][i_attnum] = text_to_cstring(DatumGetTextP(result->tts_values[i_attnum]));
                }
                else
                {
                    tuples->slot[i_tuple][i_attnum] = NULL;
                }
            }
            tuples->slot_count++;

            result = ExecRemoteQuery((PlanState *) pstate);
            i_tuple++;
        }
    }
    ExecEndRemoteQuery(pstate);
#else
    /*
	 * Connect to SPI manager
	 */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "SPI connect failure - returned %d", ret);

	initStringInfo(&buf);

	/* Get pg_***_size function results from all Datanodes */
	nodename = get_pgxc_nodename(node);

	ret = SPI_execute_direct(query, nodename);
	spi_tupdesc = SPI_tuptable->tupdesc;

	if (ret != SPI_OK_SELECT)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to execute query '%s' on node '%s'",
				        query, nodename)));
	}

	/*
	 * The query must always return one row having one column:
	 */
	Assert(SPI_processed == 1 && spi_tupdesc->natts == 1);

	datum = SPI_getbinval(SPI_tuptable->vals[0], spi_tupdesc, 1, &isnull);

	/* For single node, don't assume the type of datum. It can be bool also. */
	SPI_finish();
#endif
    return (Datum) 0;
    if (isnull
        #ifdef _MLS_
        && (NULL != result))
#endif
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Expected datum but got null instead "
                               "while executing query '%s'",
                               query)));
    PG_RETURN_DATUM(datum);
}

/*
 * execute_on_all_nodes -- execute query on all nodes and handle results
 * input: 	execute query, number of attribute in results,
 *          context for handle result function, handle result function callback
 * return:	(Datum) 0
 */
static Datum
execute_on_all_nodes(const char *query, int attnum, void *context,
					 tuple_handle_function_cb tuple_handle_func, bool readonly)
{
    int			i;
    int			i_attnum;

    EState				*estate;
    MemoryContext		oldcontext;
    RemoteQuery 		*plan;
    RemoteQueryState	*pstate;
    TupleTableSlot		*result = NULL;
    Var 				*dummy;
    TupleTableRecord    tuple;

    Assert(tuple_handle_func != NULL);

    /*
     * Make up RemoteQuery plan node
     */
    plan = makeNode(RemoteQuery);
    plan->combine_type = COMBINE_TYPE_NONE;
    plan->exec_type = EXEC_ON_ALL_NODES;
    plan->sql_statement = (char*)query;
    plan->force_autocommit = false;

    plan->exec_nodes = makeNode(ExecNodes);
    plan->exec_nodes->include_local_cn = true;
	plan->read_only = readonly;

    /*
     * We only need the target entry to determine result data type.
     * So create dummy even if real expression is a function.
     */
    for (i = 1; i <= attnum; i++)
    {
        dummy = makeVar(1, i, TEXTOID, 0, InvalidOid, 0);
        plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
                                             makeTargetEntry((Expr *) dummy, i, NULL, false));
    }
    /* prepare to execute */
    estate = CreateExecutorState();
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    estate->es_snapshot = GetActiveSnapshot();
    pstate = ExecInitRemoteQuery(plan, estate, 0);
    MemoryContextSwitchTo(oldcontext);

    tuple.attnum = attnum;
    tuple.slot = (char **) palloc(attnum * sizeof(char *));

    result = ExecRemoteQuery((PlanState *) pstate);
    while (result != NULL && !TupIsNull(result))
    {
        slot_getallattrs(result);

        for (i_attnum = 0; i_attnum < attnum; i_attnum++)
        {
            if (result->tts_values[i_attnum] != (Datum)0)
            {
                tuple.slot[i_attnum] = text_to_cstring(DatumGetTextP(result->tts_values[i_attnum]));
            }
            else
            {
                tuple.slot[i_attnum] = NULL;
            }
        }

        /* do not store all results, handle result here */
        (*tuple_handle_func) (context, &tuple);
        DropTupleTableRecord(&tuple);

        result = ExecRemoteQuery((PlanState *) pstate);
    }

    if (tuple.slot)
    {
        pfree(tuple.slot);
        tuple.slot = NULL;
    }

    ExecEndRemoteQuery(pstate);

    return (Datum) 0;
}

/*
 * get all wait relations from all nodes
 * and build the wait graph, use the callback function to
 * handle each tuple
 */
void
BuildWaitGraph(GddCtx *ctx, tuple_handle_function_cb tuple_handle_func)
{
#define RESULT_ATTAUM 9
    const char *query_stmt = "select node_name::text, waiter_gxid::text, holder_gxid::text, holdtillendxact::text, waiter_lpid::text, holder_lpid::text, "
                             "waiter_lockmode::text, waiter_locktype::text, "
                             " a1.query::text from pg_unlock_wait_status() left join (select pid, query::text from pg_stat_activity) a1 on waiter_lpid = a1.pid;";

    /* exec sql on all nodes, and handle results */
    execute_on_all_nodes(query_stmt, RESULT_ATTAUM, (void*)ctx, tuple_handle_func, true);
}

/*
 *  break all deadlocks, use the callback function
 *  to kill the backend of txn
 */
void
BreakDeadLock(GddCtx *ctx, kill_txn_function_cb kill_txn_function)
{
    List		   *xids;
    ListCell	   *cell;

    Assert(kill_txn_function != NULL);
    xids = GddCtxBreakDeadLock(ctx);

    foreach(cell, xids)
    {
        int txnid = lfirst_int(cell);

        (*kill_txn_function) (ctx, txnid);
    }

    if (xids != NIL)
        list_free(xids);
}
