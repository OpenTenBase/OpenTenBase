/*-------------------------------------------------------------------------
 *
 * nodeGather.h
 *        prototypes for nodeGather.c
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeGather.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEGATHER_H
#define NODEGATHER_H

#include "nodes/execnodes.h"

extern GatherState *ExecInitGather(Gather *node, EState *estate, int eflags);
extern void ExecEndGather(GatherState *node);
extern void ExecShutdownGather(GatherState *node);
extern void ExecReScanGather(GatherState *node);
#ifdef __OPENTENBASE__
extern void ExecFinishGather(PlanState *pstate);
#endif

#endif                            /* NODEGATHER_H */
