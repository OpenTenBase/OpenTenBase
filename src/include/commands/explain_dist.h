/*-------------------------------------------------------------------------
 *
 * explain_dist.h
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/explain_dist.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAINDIST_H
#define EXPLAINDIST_H

#include "commands/explain.h"
#include "executor/execFragment.h"
#include "executor/instrument.h"
#include "nodes/nodes.h"
#include "utils/tuplesort.h"

typedef struct
{
    bool isvalid;       /* TRUE if instrument value is valid */
    int num_workers;    /* number of parallel workers launched */
} RemoteState;

typedef struct
{
    RemoteState rs;
    TuplesortInstrumentation stat;      /* instrument if no parallel */
    TuplesortInstrumentation *w_stats;  /* instrument of parallel workers */
} RemoteSortState;

typedef struct
{
    RemoteState rs;
    /* values used in explain analyze from HashState */
    HashInstrumentation stat;
} RemoteHashState;

typedef struct
{
    RemoteState rs;
    /* values used in explain analyze from AggState */
    AggregateInstrumentation stat;
    AggregateInstrumentation *w_stats;
} RemoteAggState;

typedef struct ResultCacheInfo
{
    char    *rc_name;
    Size    rc_name_len;
    Size    rc_memory;
    long    rc_lru_count;
    long    rc_total_search;
    double  rc_hits;
} ResultCacheInfo;

typedef struct ResultCacheState
{
    bool            rcisvalid;
    int             num_rc_objs;
    ResultCacheInfo *rc_stats;
} ResultCacheState;

typedef struct
{
    RemoteState rs;
    /* either stat or w_stats is valid */
    FragmentInstrumentation stat;      /* instrument if no parallel */
    FragmentInstrumentation *w_stats;  /* instrument of parallel workers */
} RemoteFragState;

typedef struct
{
    Instrumentation instr;          /* normal case */
    ResultCacheState rcstate;      /* result cache info */
    RemoteState *state;             /* specific instrument info */
    WorkerInstrumentation *w_instrs;/* instrument of parallel workers */
} RemoteInstrumentation;

extern void InitRemoteInstr(void);
extern void ResetRemoteInstr(void);
extern bool HasRemoteInstr(void);

extern void SendLocalInstr(QueryDesc *queryDesc);
extern void HandleRemoteInstr(char *msg_body, size_t len, int nodeoid, int fid);

extern RemoteInstrumentation *ConstructRemoteInstr(Plan *plan, int fid);
extern void DestroyRemoteInstr(RemoteInstrumentation *remote_instrs);
extern void ExplainResultCacheInstr(RemoteInstrumentation *instrs,
                                    ExplainState *es);
extern void ExplainCommonRemoteInstr(RemoteInstrumentation *instrs,
                                     ExplainState *es, const char *prefix);
extern void ExplainCommonRemoteInstrJson(RemoteInstrumentation *instrs,
                                     ExplainState *es, const char *prefix);
extern void ExplainSpecialRemoteInstr(RemoteInstrumentation *instrs,
                                     ExplainState *es, NodeTag plantag);
extern void ExplainRemoteWorkerInstr(RemoteInstrumentation *instrs,
                                     ExplainState *es, NodeTag plantag);
extern void ExplainRemoteWorkerInstrJson(RemoteInstrumentation *instrs,
                                     ExplainState *es, NodeTag plantag,
									 bool isRecv);
extern void ExplainCommonRemoteInstrRecv(Plan *plan, ExplainState *es);
extern void ExplainCommonRemoteInstrRecvJson(Plan *plan, ExplainState *es);

#endif  /* EXPLAINDIST_H  */
