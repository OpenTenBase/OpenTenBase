/*-------------------------------------------------------------------------
 *
 * execdesc.h
 *      plan and query descriptor accessor macros used by the executor
 *      and related modules.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/executor/execdesc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECDESC_H
#define EXECDESC_H

#include "nodes/execnodes.h"
#ifdef XCP
#include "pgxc/squeue.h"
#endif
#include "tcop/dest.h"

/* ----------------
 *        query descriptor:
 *
 *    a QueryDesc encapsulates everything that the executor
 *    needs to execute the query.
 *
 *    For the convenience of SQL-language functions, we also support QueryDescs
 *    containing utility statements; these must not be passed to the executor
 *    however.
 * ---------------------
 */
typedef struct QueryDesc
{
    /* These fields are provided by CreateQueryDesc */
    CmdType        operation;        /* CMD_SELECT, CMD_UPDATE, etc. */
    PlannedStmt *plannedstmt;    /* planner's output (could be utility, too) */
    const char *sourceText;        /* source text of the query */
    Snapshot    snapshot;        /* snapshot to use for query */
    Snapshot    crosscheck_snapshot;    /* crosscheck for RI update/delete */
    DestReceiver *dest;            /* the destination for tuple output */
    ParamListInfo params;        /* param values being passed in */
    QueryEnvironment *queryEnv; /* query environment passed in */
    int            instrument_options; /* OR of InstrumentOption flags */

    /* These fields are set by ExecutorStart */
    TupleDesc    tupDesc;        /* descriptor for result tuples */
    EState       *estate;            /* executor's query-wide state */
    PlanState  *planstate;        /* tree of per-plan-node state */

#ifdef XCP
    SharedQueue squeue;         /* the shared memory queue to sent data to other
                                 * nodes */
#ifdef __OPENTENBASE__
     DataPumpSender sender; /* used for locally data transfering */
    ParamExecData *es_param_exec_vals;    /* values of internal params */
	RemoteEPQContext *epqContext; /* information about EvalPlanQual from remote */
#endif
                                 
    int         myindex;        /* -1 if locally executed subplan is producing
                                 * data and distribute via squeue. Otherwise
                                 * get local data from squeue */
#endif
    /* This field is set by ExecutorRun */
    bool        already_executed;    /* true if previously executed */

    /* This is always set NULL by the core system, but plugins can change it */
    struct Instrumentation *totaltime;    /* total time spent in ExecutorRun */
} QueryDesc;

/* in pquery.c */
extern QueryDesc *CreateQueryDesc(PlannedStmt *plannedstmt,
                const char *sourceText,
                Snapshot snapshot,
                Snapshot crosscheck_snapshot,
                DestReceiver *dest,
                ParamListInfo params,
                QueryEnvironment *queryEnv,
                int instrument_options);

extern void FreeQueryDesc(QueryDesc *qdesc);

#endif                            /* EXECDESC_H  */
