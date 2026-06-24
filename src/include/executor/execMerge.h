/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * execMerge.h
 *
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/execMerge.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef EXECMERGE_H
#define EXECMERGE_H

#include "nodes/execnodes.h"

/* flags for mt_merge_subcommands */
#define MERGE_INSERT 0x01
#define MERGE_UPDATE 0x02
#define MERGE_DELETE 0x04

#define MERGE_ACTION_TYPES(actions)                                                                \
	({                                                                                             \
		int merge_action_mode = 0;                                                                 \
		foreach(l, root->parse->mergeActionList)                                                   \
		{                                                                                          \
			MergeAction *action = lfirst_node(MergeAction, l);                                     \
			switch (action->commandType)                                                           \
			{                                                                                      \
				case CMD_INSERT:                                                                   \
					merge_action_mode |= MERGE_INSERT;                                             \
					break;                                                                         \
				case CMD_UPDATE:                                                                   \
					merge_action_mode |= MERGE_UPDATE;                                             \
					break;                                                                         \
				case CMD_DELETE:                                                                   \
					merge_action_mode |= MERGE_DELETE;                                             \
					break;                                                                         \
				case CMD_NOTHING:                                                                  \
					break;                                                                         \
				default:                                                                           \
					Assert(0);                                                                     \
					break;                                                                         \
			}                                                                                      \
		}                                                                                          \
		merge_action_mode;                                                                         \
	})

#define CONTAINS_ACTIONS(ac_mode, actions) ((ac_mode & actions) == actions)
#define CONTAINS_INSERT_ACTION(mtstate) (((mtstate->mt_merge_subcommands) & MERGE_INSERT) == MERGE_INSERT)
#define CONTAINS_UPDATE_ACTION(mtstate) (((mtstate->mt_merge_subcommands) & MERGE_UPDATE) == MERGE_UPDATE)
#define CONTAINS_DELETE_ACTION(mtstate) (((mtstate->mt_merge_subcommands) & MERGE_DELETE) == MERGE_DELETE)

extern TupleTableSlot * ExecMerge(ModifyTableState* mtstate, EState* estate, TupleTableSlot* slot, JunkFilter* junkfilter,
    ResultRelInfo* resultRelInfo);
extern bool CompareMergeUpdateSlots(TupleTableSlot *old_slot, TupleTableSlot *new_slot,
                                         ResultRelInfo *resultRelInfo, Bitmapset *update_cols);
extern void ExecInitMerge(ModifyTableState* mtstate, EState* estate, ResultRelInfo* resultRelInfo);

extern TupleTableSlot* ExtractScanTuple(PlanState *ps, TupleTableSlot *slot, TupleTableSlot *retslot, ItemPointer tupleid);

extern MergeQualProjState *ExecInitMergeQualProj(MergeQualProj *node, EState *estate, int eflags);
extern void                ExecEndMergeQualProj(MergeQualProjState *node);
extern TupleTableSlot     *MakeNewSrcTuple(PlanState *ps, TupleTableSlot *origslot,
                                           TupleTableSlot *slot, TupleTableSlot *retslot);
extern void                ExecInitRemoteMerge(RemoteModifyTableState *mtstate, EState *estate,
                                               ResultRelInfo *resultRelInfo, int eflags);
extern TupleTableSlot     *ExecRemoteMergeQualProj(RemoteModifyTableState *rmtstate,
                                                   ResultRelInfo          *resultRelInfo,
                                                   TupleTableSlot         *planSlot);
#endif /* NODEMERGE_H */
