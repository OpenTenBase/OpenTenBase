/*-------------------------------------------------------------------------
 *
 * nodePartIterator.h
 *
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodePartIterator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEPARTITERATOR_H
#define NODEPARTITERATOR_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern PartIteratorState *ExecInitPartIterator(PartIterator *node, EState *estate, int eflags);
extern void ExecEndPartIterator(PartIteratorState *node);
extern void ExecReScanPartIterator(PartIteratorState *node);
extern void ExecPartIteratorEstimate(PartIteratorState *node, ParallelContext *pcxt);
extern void ExecPartIteratorInitializeDSM(PartIteratorState *node, ParallelContext *pcxt);
extern void ExecPartIteratorReInitializeDSM(PartIteratorState *node, ParallelContext *pcxt);
extern void ExecPartIteratorInitializeWorker(PartIteratorState *node, ParallelWorkerContext *pwcxt);

#endif							/* NODEPARTITERATOR_H */
