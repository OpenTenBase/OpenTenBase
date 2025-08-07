/*-------------------------------------------------------------------------
 *
 * nodeModifyTable.h
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeModifyTable.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMODIFYTABLE_H
#define NODEMODIFYTABLE_H

#include "nodes/execnodes.h"
#include "executor/execPartition.h"

extern ModifyTableState *ExecInitModifyTable(ModifyTable *node, EState *estate, int eflags);
extern void ExecEndModifyTable(ModifyTableState *node);
extern void ExecReScanModifyTable(ModifyTableState *node);
#ifdef __OPENTENBASE__
extern TupleTableSlot *ExecRemoteUpdate(ModifyTableState *mtstate,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool canSetTag);
extern TupleTableSlot *ExecInsert(ModifyTableState *mtstate,
                                  TupleTableSlot *slot,
                                  TupleTableSlot *planSlot,
                                  EState *estate,
                                  bool canSetTag);
extern Oid ExecInsertCheckConflict(Relation rel, HeapTuple tuple);
extern TupleTableSlot *ExecUpdate(ModifyTableState *mtstate, ItemPointer tupleid,
                                  HeapTuple oldtuple, TupleTableSlot *slot,
                                  TupleTableSlot *planSlot, EPQState *epqstate, EState *estate,
                                  bool canSetTag, bool merge_ignore);
extern void ExecCheckPlanOutput(Relation resultRel, List* targetList);
extern TupleTableSlot *ExecPrepareTupleRouting(ModifyTableState *mtstate,
                                               EState *estate,
                                               PartitionTupleRouting *proute,
                                               ResultRelInfo *targetRelInfo,
                                               TupleTableSlot *slot,
                                               Oid oldpartitionid);
extern TupleTableSlot *ExecDelete(ModifyTableState *mtstate,
								  ItemPointer tupleid,
								  HeapTuple oldtuple,
								  TupleTableSlot *sourceslot,
								  TupleTableSlot *planSlot,
								  EPQState *epqstate,
								  EState *estate,
								  bool *tupleDeleted,
								  bool processReturning,
								  bool canSetTag,
								  bool changingPart,
								  TupleTableSlot **epqslot);
#else
extern TupleTableSlot *ExecDelete(ModifyTableState *mtstate,
							      ItemPointer tupleid,
							      HeapTuple oldtuple,
							      TupleTableSlot *planSlot,
							      EPQState *epqstate,
							      EState *estate,
							      bool *tupleDeleted,
							      bool processReturning,
								  bool canSetTag,
								  bool changingPart,
								  TupleTableSlot **epqslot);
#endif
extern void ExecSimpleDML(const char *relname, const char *space, CmdType cmdtype, HeapTuple itup, HeapTuple dtup, ItemPointer target_ctid);
#endif							/* NODEMODIFYTABLE_H */
