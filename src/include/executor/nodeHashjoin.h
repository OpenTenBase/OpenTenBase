/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.h
 *      prototypes for nodeHashjoin.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/executor/nodeHashjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASHJOIN_H
#define NODEHASHJOIN_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"
#ifdef __OPENTENBASE__
#include "access/parallel.h"
#endif

extern HashJoinState *ExecInitHashJoin(HashJoin *node, EState *estate, int eflags);
extern void ExecEndHashJoin(HashJoinState *node);
extern void ExecReScanHashJoin(HashJoinState *node);

extern void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
                      BufFile **fileptr);
#ifdef __OPENTENBASE__
extern void ExecParallelHashJoinEstimate(HashJoinState *node, ParallelContext *pcxt);

extern void ExecParallelHashJoinInitializeDSM(HashJoinState *node, ParallelContext *pcxt);

extern void ExecParallelHashJoinInitWorker(HashJoinState *node, ParallelWorkerContext *pwcxt);

extern void ParallelHashJoinEreport(void);
#endif

#endif                            /* NODEHASHJOIN_H */
