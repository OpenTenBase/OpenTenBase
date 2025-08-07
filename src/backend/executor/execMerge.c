/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * execMerge.c
 *
 *
 * IDENTIFICATION
 *    src/backend/executor/execMerge.c
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/index.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/execMerge.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "access/htup_details.h"
#include "storage/bufmgr.h"
static void ExecMergeNotMatched(ModifyTableState *mtstate, EState *estate,
                                ResultRelInfo *resultRelInfo, TupleTableSlot *slot, JunkFilter *jf);
static TupleTableSlot * ExecMergeMatched(ModifyTableState *mtstate, EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, JunkFilter *junkfilter,
                             ItemPointer tupleid, HeapTuple oldtuple, Oid oldPartitionOid);
static TupleTableSlot *ExecMergeQualProj(PlanState *pstate);

bool CompareMergeUpdateSlots(TupleTableSlot *old_slot, TupleTableSlot *new_slot,
				ResultRelInfo *resultRelInfo, Bitmapset *update_cols)
{
		TupleDesc tupleDesc = NULL;
		bool result = true;
		int16 attnum = 1;
		TypeCacheEntry *typeCacheEntry = NULL;
		FunctionCallInfoData locfcinfo;

		tupleDesc = resultRelInfo->ri_RelationDesc->rd_att;
		Assert(old_slot->tts_tupleDescriptor != NULL);
		for (; attnum <= tupleDesc->natts; attnum++)
		{
				if (update_cols && !bms_is_member(attnum, update_cols))
					continue;
				if (tupleDesc->attrs[attnum - 1].attisdropped)
						continue;
				typeCacheEntry = lookup_type_cache(
								tupleDesc->attrs[attnum - 1].atttypid,
								TYPECACHE_EQ_OPR_FINFO);
				if (!OidIsValid(typeCacheEntry->eq_opr_finfo.fn_oid))
						return false;
				if (!old_slot->tts_isnull[attnum - 1] ||
								!new_slot->tts_isnull[attnum - 1])
				{
						if (old_slot->tts_isnull[attnum - 1] ||
										new_slot->tts_isnull[attnum - 1])
						{
								result = false;
								break;
						}
						/* Compare the pair of elements */
						InitFunctionCallInfoData(locfcinfo, &typeCacheEntry->eq_opr_finfo,
										2,
										tupleDesc->attrs[attnum - 1].attcollation,
										NULL, NULL);
						locfcinfo.arg[0] = old_slot->tts_values[attnum - 1];
						locfcinfo.arg[1] = new_slot->tts_values[attnum - 1];
						locfcinfo.argnull[0] = false;
						locfcinfo.argnull[1] = false;
						locfcinfo.isnull = false;
						result = DatumGetBool(FunctionCallInvoke(&locfcinfo));
						if (!result)
								break;
				}
		}
		return result;
}

/*
 * Perform MERGE.
 */
TupleTableSlot *
ExecMerge(ModifyTableState *mtstate, EState *estate, TupleTableSlot *slot, JunkFilter *junkfilter,
          ResultRelInfo *resultRelInfo)
{
    ExprContext *econtext = mtstate->ps.ps_ExprContext;
    ItemPointer tupleid;
    ItemPointerData tuple_ctid;
    bool matched = false;
    Datum datum;
    bool isNull = false;
    HeapTuple oldtuple = NULL;
    Oid oldPartitionOid = InvalidOid;
    Relation targetRelation = resultRelInfo->ri_RelationDesc;
    TupleTableSlot *latestSlot      = NULL;

	if (relation_has_cross_node_index(targetRelation))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
					errmsg("global index does not support merge yet")));
    Assert((targetRelation->rd_rel->relkind == RELKIND_RELATION ||
           targetRelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) &&
           junkfilter != NULL);

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous cycle.
     */
    ResetExprContext(econtext);

    /*
     * We run a JOIN between the target relation and the source relation to
     * find a set of candidate source rows that has matching row in the target
     * table and a set of candidate source rows that does not have matching
     * row in the target table. If the join returns us a tuple with target
     * relation's tid set, that implies that the join found a matching row for
     * the given source tuple. This case triggers the WHEN MATCHED clause of
     * the MERGE. Whereas a NULL in the target relation's ctid column
     * indicates a NOT MATCHED case.
     */
	datum = ExecGetJunkAttribute(slot, resultRelInfo->ri_RowIdAttNo, &isNull);

    if (!isNull)
    {
        if (targetRelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        {
            Datum tableOiddatum;
            bool tableOidisnull = false;

			tableOiddatum = ExecGetJunkAttribute(slot, mtstate->mt_resultOidAttno,
												 &tableOidisnull);
            if (tableOidisnull)
                ereport(ERROR,
                        (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
						errmsg("tableoid is null when merge partitioned table")));

            oldPartitionOid = DatumGetObjectId(tableOiddatum);
        }

        matched = true;
        tupleid = (ItemPointer) DatumGetPointer(datum);
        tuple_ctid = *tupleid; /* be sure we don't free ctid!! */
        tupleid = &tuple_ctid;
    }
    else
    {
        matched = false;
        tupleid = NULL; /* we don't need it for INSERT actions */
    }

    /*
     * If we are dealing with a WHEN MATCHED case, we execute the first action
     * for which the additional WHEN MATCHED AND quals pass. If an action
     * without quals is found, that action is executed.
     *
     * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at the
     * given WHEN NOT MATCHED actions in sequence until one passes.
     *
     * Things get interesting in case of concurrent update/delete of the
     * target tuple. Such concurrent update/delete is detected while we are
     * executing a WHEN MATCHED action.
     *
     * A concurrent update can:
     *
     * 1. modify the target tuple so that it no longer satisfies the
     * additional quals attached to the current WHEN MATCHED action OR
     *
     * In this case, we are still dealing with a WHEN MATCHED case, but
     * we should recheck the list of WHEN MATCHED actions and choose the first
     * one that satisfies the new target tuple.
     *
     * 2. modify the target tuple so that the join quals no longer pass and
     * hence the source tuple no longer has a match.
     *
     * In the second case, the source tuple no longer matches the target tuple,
     * so we now instead find a qualifying WHEN NOT MATCHED action to execute.
     *
     * A concurrent delete, changes a WHEN MATCHED case to WHEN NOT MATCHED.
     *
     * ExecMergeMatched takes care of following the update chain and
     * re-finding the qualifying WHEN MATCHED action, as long as the updated
     * target tuple still satisfies the join quals i.e. it still remains a
     * WHEN MATCHED case. If the tuple gets deleted or the join quals fail, it
     * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
     * always make progress by following the update chain and we never switch
     * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
     * livelock.
     */
    if (matched && (CONTAINS_UPDATE_ACTION(mtstate) || CONTAINS_DELETE_ACTION(mtstate)))
		latestSlot = ExecMergeMatched(mtstate,
		                              estate,
		                              resultRelInfo,
		                              slot,
		                              junkfilter,
		                              tupleid,
		                              oldtuple,
		                              oldPartitionOid);
	if (!TupIsNull(latestSlot))
    {
        matched = false;
    }

	/*
     * Either we were dealing with a NOT MATCHED tuple or ExecMergeNotMatched()
     * returned "false", indicating the previously MATCHED tuple is no longer a
     * matching tuple.
     */
    if (!matched && CONTAINS_INSERT_ACTION(mtstate))
		ExecMergeNotMatched(mtstate, estate, resultRelInfo, slot, junkfilter);
	return NULL;
}

/*
 * Extract scan tuple for target table from plan slot
 */
TupleTableSlot *
ExtractScanTuple(PlanState *ps, TupleTableSlot *slot, TupleTableSlot *retslot, ItemPointer tupleid)
{
    ExprContext    *econtext = ps->ps_ExprContext;
	TupleDesc       tupDesc = retslot->tts_tupleDescriptor;
	MemoryContext   oldContext;
    HeapTuple       tempTuple = NULL;
    Datum          *values    = NULL;
    bool           *isnull    = NULL;
    int             index     = 0;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	values = (Datum *) palloc0(sizeof(Datum) * tupDesc->natts);
	isnull = (bool *) palloc0(sizeof(bool) * tupDesc->natts);

	for (index = 0; index < tupDesc->natts; index++)
    {
        if (TupleDescAttr(tupDesc, index)->attisdropped == true)
        {
            isnull[index] = true;
            continue;
        }

        values[index] = slot->tts_values[index];
        isnull[index] = slot->tts_isnull[index];
    }

    tempTuple = heap_form_tuple(tupDesc, values, isnull);
	ItemPointerSet(&tempTuple->t_data->t_ctid,
	               ItemPointerGetBlockNumber(tupleid),
	               ItemPointerGetOffsetNumber(tupleid));
    (void) ExecStoreHeapTuple(tempTuple, retslot, true);

    MemoryContextSwitchTo(oldContext);

    return retslot;
}

/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the WHEN AND quals
 * pass, if any. If the WHEN AND quals for the first action do not pass, we
 * check the second, then the third and so on. If we reach to the end, no
 * action is taken and we return true, indicating that no further action is
 * required for this tuple.
 *
 * If we do find a qualifying action, then we attempt to execute the action.
 *
 * If the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated separately by the MERGE code, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the first
 * action to look for a qualifying action. Otherwise, we return false meaning
 * that a NOT MATCHED action must now be executed for the current source tuple.
 */
static TupleTableSlot *
ExecMergeMatched(ModifyTableState *mtstate, EState *estate, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, JunkFilter *junkfilter,
                 ItemPointer tupleid, HeapTuple oldtuple, Oid oldPartitionOid)
{
	ExprContext    *econtext = mtstate->ps.ps_ExprContext;
	EPQState       *epqstate = &mtstate->mt_epqstate;
	TupleTableSlot *saved_slot = slot;
	TupleTableSlot *delete_slot = NULL;
	TupleTableSlot *result_slot = NULL;
	TupleTableSlot *latestSlot = NULL;
	ListCell       *lc = NULL;
	bool            deleteAction = false;
	HeapTupleData   update_tuple;

	/*
	 * If there are no WHEN MATCHED actions, we are done.
	 */
	if (resultRelInfo->ri_matchedMergeAction == NIL)
		return NULL;

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the scantuple.
	 * Again, this target relation's slot is required only in the case of a
	 * MATCHED tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

lmerge_matched:;
	
	Buffer update_buffer;
	update_tuple.t_self = *tupleid;
	if (!heap_fetch(resultRelInfo->ri_RelationDesc,
					SnapshotAny,
					&update_tuple,
					&update_buffer,
					false,
					NULL))
		elog(ERROR, "failed to fetch the target tuple");
	/* we need pin the buffer of updated tuple */
	ExecStoreBufferHeapTuple(&update_tuple, resultRelInfo->ri_oldTupleSlot, update_buffer);
	ReleaseBuffer(update_buffer);


	foreach (lc, resultRelInfo->ri_matchedMergeAction)
	{
		MergeActionState *relaction = (MergeActionState *) lfirst(lc);
		CmdType           commandType = relaction->mas_action->commandType;

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since ExecQual() will
		 * return true if there are no conditions to evaluate).
		 */
		if (!ExecQual(relaction->mas_whenqual, econtext))
			continue;

		/*
		 * Check if the existing target tuple meets the USING checks of
		 * UPDATE/DELETE RLS policies. If those checks fail, we throw an
		 * error.
		 *
		 * The WITH CHECK quals are applied in ExecUpdate() and hence we need
		 * not do anything special to handle them.
		 *
		 * NOTE: We must do this after WHEN quals are evaluated, so that we
		 * check policies only when they matter.
		 */
		if (resultRelInfo->ri_WithCheckOptions)
		{
			ExecWithCheckOptions(commandType == CMD_UPDATE ? WCO_RLS_MERGE_UPDATE_CHECK
			                                               : WCO_RLS_MERGE_DELETE_CHECK,
			                     resultRelInfo,
			                     resultRelInfo->ri_oldTupleSlot,
			                     estate);
		}

		switch (commandType)
		{
			case CMD_UPDATE:
			{
				result_slot = ExecProject(relaction->mas_proj);
				mtstate->relaction = relaction;

				if (!TupIsNull(latestSlot))
					ExecClearTuple(latestSlot);

				if (ORA_MODE && relaction->deleteActionState != NULL)
				{

					delete_slot =
						MakeNewSrcTuple(&mtstate->ps, result_slot, slot, mtstate->mt_mergedel);
					econtext->ecxt_innertuple = delete_slot;
					econtext->ecxt_scantuple = delete_slot;
					if (ExecQual(relaction->deleteActionState->mas_whenqual, econtext))
					{
						ExecDelete(mtstate,
						           tupleid,
						           oldtuple,
						           result_slot,
						           saved_slot,
						           epqstate,
						           estate,
						           &deleteAction,
						           false,
								   mtstate->canSetTag,
								   false /* changingPart */, NULL);
						if (deleteAction)
							InstrCountFiltered2(&mtstate->ps, 1);
						deleteAction = true;
					}
					ExecClearTuple(delete_slot);
					econtext->ecxt_scantuple = NULL;
					econtext->ecxt_innertuple = NULL;
				}
				if (!deleteAction)
				{
						/* compare updating cols sanme as old tuple */
						if (ORA_MODE && CompareMergeUpdateSlots(slot, result_slot, resultRelInfo, relaction->update_cols))
						{
							Buffer update_buffer;
							SnapshotData snap_dirty;

							update_tuple.t_self = *tupleid;

							InitDirtySnapshot(snap_dirty);
							if (!heap_fetch(resultRelInfo->ri_RelationDesc,
							                &snap_dirty,
							                &update_tuple,
							                &update_buffer,
							                false,
							                NULL))
							{
								ereport(ERROR,
								        (errcode(ERRCODE_CARDINALITY_VIOLATION),
									          errmsg("MERGE command cannot affect row a "
									                 "second time"),
									          errhint("Ensure that not more than one source "
									                  "row matches any one target row.")));
							}
							ReleaseBuffer(update_buffer);

							latestSlot = ExecUpdate(mtstate,
							                        tupleid,
							                        oldtuple,
							                        result_slot,
							                        saved_slot,
							                        epqstate,
							                        estate,
							                        mtstate->canSetTag,
							                        true);
						}
						else
								latestSlot = ExecUpdate(mtstate,
												tupleid,
												oldtuple,
												result_slot,
												saved_slot,
												epqstate,
												estate,
												mtstate->canSetTag,
												false);
						if (TupIsNull(latestSlot))
								InstrCountFiltered2(&mtstate->ps, 1);
				}
			}
			break;
			case CMD_DELETE:
			{
				bool tupleDeleted = false;
				ExecDelete(mtstate,
				           tupleid,
				           oldtuple,
				           result_slot,
				           saved_slot,
				           epqstate,
				           estate,
				           &tupleDeleted,
				           false,
						   mtstate->canSetTag,
						   false /* changingPart */, NULL);
				if (tupleDeleted)
					InstrCountFiltered2(&mtstate->ps, 1);
			}
			break;
			case CMD_NOTHING:
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");
		}
		/* Failed, updated by other session */
		if (!TupIsNull(latestSlot))
		{
			bool  isNull = false;

			/* Ignore return value to avoid compiler warning */
			ExecGetJunkAttribute(latestSlot, resultRelInfo->ri_RowIdAttNo, &isNull);
			if (isNull)
			{
				ExecClearTuple(resultRelInfo->ri_oldTupleSlot);
				/* CN needs to perform a not matched operation. */
				return latestSlot;
			}
			/*
			 * TODO: Now, latestSlot generated from EvalPlanQual is wrong.
			 * Reorder of EPQ in PG16 can get the right slot.
			 * Comment the code below is to avoid program getting stuck in a loop;
			 *
			 * ItemPointerCopy((ItemPointer) DatumGetPointer(datum), tupleid);
			 */
			econtext->ecxt_innertuple = latestSlot;
			econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
			goto lmerge_matched;
		}
		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimization.
		 */
		break;
	}
	ExecClearTuple(resultRelInfo->ri_oldTupleSlot);
    /*
     * Successfully executed an action or no qualifying action was found.
     */
    return NULL;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
static void
ExecMergeNotMatched(ModifyTableState *mtstate, EState *estate, ResultRelInfo *resultRelInfo,
                    TupleTableSlot *slot, JunkFilter *jf)
{
	ListCell       *l;
	ExprContext    *econtext = mtstate->ps.ps_ExprContext;
	TupleTableSlot *result_slot = NULL;
	ResultRelInfo *old_resultRelInfo = NULL;
	/*
	 * Make source tuple available to ExecQual and ExecProject. We don't need
	 * the target tuple since the WHEN quals and the targetlist can't refer to
	 * the target columns.
	 */
	econtext->ecxt_scantuple = slot;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	old_resultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;
	foreach (l, resultRelInfo->ri_notMatchedMergeAction)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType           commandType = action->mas_action->commandType;

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since ExecQual() will
		 * return true if there are no conditions to evaluate).
		 */
		if (!action->mas_action->actionOnly && !ExecQual(action->mas_whenqual, econtext))
			continue;

		/* Perform stated action */
		switch (commandType)
		{
			case CMD_INSERT:
			{
				PartitionTupleRouting *proute = mtstate->mt_partition_tuple_routing;
				ResultRelInfo         *oldResultRelInfo = estate->es_result_relation_info;

				/*
				 * Project the tuple.  In case of a partitioned table, the
				 * projection was already built to use the root's descriptor,
				 * so we don't need to map the tuple here.
				 */
				if (!action->mas_action->actionOnly )
					result_slot = ExecProject(action->mas_proj);
				else
					result_slot = ExecFilterJunk(jf, slot);
				
				mtstate->relaction = action;

				/* Prepare for tuple routing if needed. */
				if (proute)
					result_slot = ExecPrepareTupleRouting(mtstate,
					                                      estate,
					                                      proute,
					                                      mtstate->rootResultRelInfo,
					                                      result_slot,
					                                      InvalidOid);
				ExecInsert(mtstate, result_slot, slot, estate, mtstate->canSetTag);

				/* Revert ExecPrepareTupleRouting's state change. */
				if (proute)
					estate->es_result_relation_info = oldResultRelInfo;
				InstrCountFiltered1(&mtstate->ps, 1);
			}
			break;
			case CMD_NOTHING:
				/* Do nothing */
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
		}

		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimization.
		 */
		break;
	}
	estate->es_result_relation_info = old_resultRelInfo;
}

/*
 * Creates the run-time state information for the Merge node
 */
void
ExecInitMerge(ModifyTableState *mtstate, EState *estate, ResultRelInfo *resultRelInfo)
{
	ListCell      *lc = NULL;
	ExprContext   *econtext = NULL;
	ModifyTable   *node = (ModifyTable *) mtstate->ps.plan;
	Plan          *subPlan = outerPlan(node);
	ResultRelInfo *rootRelInfo;
	int            i;

	if (node->mergeActionLists == NIL)
		return;

	if (node->rootResultRelIndex >= 0)
	{
		rootRelInfo = mtstate->rootResultRelInfo;
	}
	else
	{
		rootRelInfo = mtstate->resultRelInfo;
	}

	mtstate->mt_merge_subcommands = 0;

	if (mtstate->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mtstate->ps);

	econtext = mtstate->ps.ps_ExprContext;

	/* initialize scan slot and constraint slot */
	mtstate->mt_mergeret = NULL;

	/*
	 * Create a MergeActionState for each action on the mergeActionList and
	 * add it to either a list of matched actions or not-matched actions.
	 *
	 * Similar logic appears in ExecInitPartitionInfo(), so if changing
	 * anything here, do so there too.
	 */
	i = 0;
	foreach (lc, node->mergeActionLists)
	{
		List     *mergeActionList = lfirst(lc);
		TupleDesc relationDesc;
		ListCell *l;

		resultRelInfo = mtstate->resultRelInfo + i;
		i++;
		relationDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		resultRelInfo->ri_oldTupleSlot =
			ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
		                       RelationGetDescr(resultRelInfo->ri_RelationDesc));
		resultRelInfo->ri_newTupleSlot =
			ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
		                       RelationGetDescr(resultRelInfo->ri_RelationDesc));

		foreach (l, mergeActionList)
		{
			MergeAction      *action = (MergeAction *) lfirst(l);
			MergeActionState *action_state = makeNode(MergeActionState);
			TupleTableSlot   *tgtslot;
			TupleDesc         tgtdesc;
			List            **list;

			action_state->matched = action->matched;
			action_state->commandType = action->commandType;
			action_state->mas_action = action;
			action_state->mas_whenqual = ExecInitQual((List *) action->qual, &mtstate->ps);

			/*
			 * We create two lists - one for WHEN MATCHED actions and one for
			 * WHEN NOT MATCHED actions - and stick the MergeActionState into
			 * the appropriate list.
			 */
			if (action_state->mas_action->matched)
				list = &resultRelInfo->ri_matchedMergeAction;
			else
				list = &resultRelInfo->ri_notMatchedMergeAction;
			*list = lappend(*list, action_state);

			switch (action->commandType)
			{
				case CMD_INSERT:
					if (!action->actionOnly)
					{
						ExecCheckPlanOutput(rootRelInfo->ri_RelationDesc, action->targetList);

						tgtslot = resultRelInfo->ri_newTupleSlot;
						tgtdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

						action_state->mas_proj = ExecBuildProjectionInfo(
							action->targetList, econtext, tgtslot, &mtstate->ps, tgtdesc);
					}
					mtstate->mt_merge_subcommands |= MERGE_INSERT;
					break;
				case CMD_UPDATE:
					if (action->updateColnos)
					{
						ListCell *lc_col = NULL;

						action_state->mas_proj =
							ExecBuildUpdateProjection(action->targetList,
						                              true,
						                              action->updateColnos,
						                              relationDesc,
						                              econtext,
						                              resultRelInfo->ri_newTupleSlot,
						                              &mtstate->ps);
						foreach(lc_col, action->updateColnos)
						{
							AttrNumber	targetattnum = lfirst_int(lc_col);

							action_state->update_cols = bms_add_member(action_state->update_cols, targetattnum);
						}
					}
					else
					{
						tgtdesc = ExecTypeFromTL(action->targetList, false);
						if (mtstate->mt_mergeret == NULL)
						{
							mtstate->mt_mergeret =
								ExecInitExtraTupleSlot(mtstate->ps.state, tgtdesc);
						}
						action_state->mas_proj = ExecBuildProjectionInfo(action->targetList,
						                                                 econtext,
						                                                 mtstate->mt_mergeret,
						                                                 &mtstate->ps,
						                                                 tgtdesc);
					}

					if (ORA_MODE && action->deleteAction != NULL)
					{
						TupleDesc      retTupDesc;
						action_state->deleteActionState = makeNode(MergeActionState);
						retTupDesc = ExecTypeFromTL((List *) subPlan->targetlist, false);
						mtstate->mt_mergedel = ExecInitExtraTupleSlot(mtstate->ps.state,
						retTupDesc);
						if (NULL != action->deleteAction->qual &&
						    IsA(action->deleteAction->qual, List))
							action_state->deleteActionState->mas_whenqual =
								ExecInitQual((List *) action->deleteAction->qual, &mtstate->ps);
					}
					mtstate->mt_merge_subcommands |= MERGE_UPDATE;
					break;
				case CMD_DELETE:
					mtstate->mt_merge_subcommands |= MERGE_DELETE;
					break;
				case CMD_NOTHING:
					break;
				default:
					elog(ERROR, "unknown operation");
					break;
			}
		}
	}
}

/*
 * Creates the run-time state information for the MergeQualProj node
 */
MergeQualProjState *
ExecInitMergeQualProj(MergeQualProj *node, EState *estate, int eflags)
{
	MergeQualProjState *mqpState;
	ListCell           *l = NULL;
	List               *mergeNotMatchedActionStates = NIL;

	Assert(node->mergeActionList);

	mqpState = makeNode(MergeQualProjState);
	mqpState->ps.plan = (Plan *) node;
	mqpState->ps.state = estate;
	mqpState->ps.ExecProcNode = ExecMergeQualProj;

	if (mqpState->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mqpState->ps);

	ExecInitResultTupleSlotTL(&mqpState->ps);

	/*
	 * initialize child nodes
	 */
	outerPlanState(mqpState) = ExecInitNode(outerPlan(node), estate, eflags);

	mqpState->rowIdAttNo = ExecFindJunkAttributeInTlist(node->plan.targetlist, "ctid");

	/*
	 * Create a MergeActionState for each action on the mergeActionList
	 * and add it to either a list of matched actions or not-matched
	 * actions.
	 */
	foreach (l, node->mergeActionList)
	{
		MergeAction      *action = (MergeAction *) lfirst(l);
		MergeActionState *action_state = makeNode(MergeActionState);
		TupleDesc         tupDesc;

		action_state->matched = action->matched;
        action_state->commandType = action->commandType;
		action_state->mas_whenqual = ExecInitQual((List *) action->qual, &mqpState->ps);

		if (action->commandType == CMD_INSERT)
		{
			tupDesc = ExecTypeFromTL((List *) action->targetList, false);
			action_state->tupDesc = tupDesc;

			action_state->proj = ExecBuildProjectionInfo(action->targetList,
			                                                 mqpState->ps.ps_ExprContext,
			                                                 mqpState->ps.ps_ResultTupleSlot,
			                                                 &mqpState->ps,
			                                                 NULL);
			mergeNotMatchedActionStates = lappend(mergeNotMatchedActionStates, action_state);
		}

		switch (action->commandType)
		{
			case CMD_INSERT:
				mqpState->mt_merge_subcommands |= MERGE_INSERT;
				break;
			case CMD_UPDATE:
				mqpState->mt_merge_subcommands |= MERGE_UPDATE;
				break;
			case CMD_DELETE:
				mqpState->mt_merge_subcommands |= MERGE_DELETE;
				break;
			case CMD_NOTHING:
				break;
			default:
				Assert(0);
				break;
		}
	}
	mqpState->notMatchedMergeActionStates = mergeNotMatchedActionStates;
	return mqpState;
}

/*
 *	 ExecMergeQualProj -
 *
 * Treatment the Join result for Merge to determine whether the records meet the
 * ACTION conditions, and skip the record that is not satisfied. If it is
 * satisfied, for the record NOT MATCHED, projects the tuple based on projection
 * info and store the field value of the target table, for the MATCHED value,
 * directly return the tuple.
 */
static TupleTableSlot *
ExecMergeQualProj(PlanState *pstate)
{
	MergeQualProjState *node = castNode(MergeQualProjState, pstate);
	ExprContext        *econtext;
	TupleTableSlot     *oslot = NULL;
	TupleTableSlot     *rslot = NULL;
	bool                isNull = false;

	CHECK_FOR_INTERRUPTS();

	econtext = node->ps.ps_ExprContext;
	for (;;)
	{
		oslot = ExecProcNode(outerPlanState(node));
		if (TupIsNull(oslot))
			return NULL;

		/*
		 * Reset per-tuple memory context to free any expression evaluation
		 * storage allocated in the previous cycle.
		 */
		ResetExprContext(econtext);

		ExecGetJunkAttribute(oslot, node->rowIdAttNo, &isNull);

		/* not matched */
		if (isNull && CONTAINS_INSERT_ACTION(node))
		{
			ListCell *l;
			/*
			 * Make source tuple available to ExecQual and ExecProject. We don't need
			 * the target tuple since the WHEN quals and the targetlist can't refer to
			 * the target columns.
			 */
			econtext->ecxt_scantuple = oslot;
			econtext->ecxt_innertuple = oslot;
			econtext->ecxt_outertuple = NULL;

			foreach (l, node->notMatchedMergeActionStates)
			{
				MergeActionState *action = (MergeActionState *) lfirst(l);

				/*
				 * Test condition
				 */
				if (!ExecQual(action->mas_whenqual, econtext))
					continue;

				/*
				 * Project the tuple.
				 */
				rslot = ExecProject(action->proj);

				/*
				 * We've activated one of the WHEN clauses, so we don't search
				 * further. This is required behaviour, not an optimization.
				 */
				break;
			}
			if (rslot != NULL)
				return rslot;
		}
		else if (CONTAINS_UPDATE_ACTION(node) || CONTAINS_DELETE_ACTION(node))
		{
			return oslot;
		}
	}
	return NULL;
}

void
ExecEndMergeQualProj(MergeQualProjState *node)
{
	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
}

/*
 * MakeDelSrcTuple
 *
 * Create a tuple that contains the source table data and the projected data for
 * delete action.
 */
TupleTableSlot *
MakeNewSrcTuple(PlanState *ps, TupleTableSlot *origslot, TupleTableSlot *slot,
                TupleTableSlot *retslot)
{
    int           origIdx    = 0;
    int           index      = 0;
    ExprContext  *econtext   = ps->ps_ExprContext;
    MemoryContext oldContext;
    TupleDesc     tupDesc    = slot->tts_tupleDescriptor;
    Datum        *values     = NULL;
    bool         *isnull     = NULL;
    HeapTuple     tempTuple  = NULL;
    int           srcStart   = -1;

	oldContext                = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    values                    = (Datum *)palloc0(sizeof(Datum) * tupDesc->natts);
    isnull                    = (bool *)palloc0(sizeof(bool) * tupDesc->natts);

    slot_getsomeattrs(slot, tupDesc->natts);
    slot_getsomeattrs(origslot, origslot->tts_tupleDescriptor->natts);

    for (origIdx = 0; origIdx < origslot->tts_tupleDescriptor->natts; origIdx++)
    {
        if (TupIsNull(origslot))
        {
            isnull[index] = true;
        }
        else
        {
            values[index] = origslot->tts_values[origIdx];
            isnull[index] = origslot->tts_isnull[origIdx];
        }
        index++;
    }
    /* there are no source target */
    srcStart = (srcStart == -1) ? tupDesc->natts : srcStart;
    while (index < slot->tts_tupleDescriptor->natts)
    {
        values[index] = slot->tts_values[index];
        isnull[index] = slot->tts_isnull[index];
        index++;
    }

    tempTuple = heap_form_tuple(tupDesc, values, isnull);
    (void)ExecStoreHeapTuple(tempTuple, retslot, true);

    MemoryContextSwitchTo(oldContext);
    return retslot;
}

/*
 * ExecInitRemoteMerge
 *
 * Creates the run-time state information for Remote Merge
 */
void
ExecInitRemoteMerge(RemoteModifyTableState *mtstate, EState *estate,
                    ResultRelInfo *resultRelInfo, int eflags)
{
    ListCell          *l            = NULL;
    ExprContext       *econtext     = NULL;
    RemoteModifyTable *node         = (RemoteModifyTable *)mtstate->ps.plan;

    if (node->remoteMergeActionList == NIL)
        return;

    if (mtstate->ps.ps_ExprContext == NULL)
        ExecAssignExprContext(estate, &mtstate->ps);

    econtext                   = mtstate->ps.ps_ExprContext;

	mtstate->mt_merge_subcommands = 0;

	resultRelInfo->ri_oldTupleSlot =
		ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
	                       RelationGetDescr(resultRelInfo->ri_RelationDesc));
	resultRelInfo->ri_newTupleSlot =
		ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
	                       RelationGetDescr(resultRelInfo->ri_RelationDesc));
	/*
     * Create a MergeActionState for each action on the mergeActionList
     * and add it to either a list of matched actions or not-matched
     * actions.
     */
    foreach (l, node->remoteMergeActionList)
    {
        MergeAction      *action      = lfirst_node(MergeAction, l);

		if (action->commandType == CMD_INSERT)
		{
			MergeActionState *action_state = makeNode(MergeActionState);
			TupleTableSlot   *tgtslot;
			TupleDesc         tgtdesc;
			action_state->matched = action->matched;
			action_state->commandType = action->commandType;
			action_state->mas_action = action;

			action_state->mas_whenqual = ExecInitQual((List *) action->qual, &mtstate->ps);
			resultRelInfo->ri_notMatchedMergeAction =
				lappend(resultRelInfo->ri_notMatchedMergeAction, action_state);
			ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, action->targetList);
			tgtslot = resultRelInfo->ri_newTupleSlot;
			tgtdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
			action_state->mas_proj = ExecBuildProjectionInfo(action->targetList,
			                                                 econtext,
			                                                 tgtslot,
			                                                 &mtstate->ps,
			                                                 tgtdesc);
		}

		switch (action->commandType)
        {
            case CMD_INSERT:
                mtstate->mt_merge_subcommands |= MERGE_INSERT;
                break;
            case CMD_UPDATE:
                mtstate->mt_merge_subcommands |= MERGE_UPDATE;
                break;
            case CMD_DELETE:
                mtstate->mt_merge_subcommands |= MERGE_DELETE;
                break;
            case CMD_NOTHING:
                break;
            default:
                Assert(0);
                break;
        }
    }
}

TupleTableSlot *
ExecRemoteMergeQualProj(RemoteModifyTableState *rmtstate, ResultRelInfo *resultRelInfo,
                        TupleTableSlot *planSlot)
{
	ExprContext    *econtext = rmtstate->ps.ps_ExprContext;
	TupleTableSlot *rslot = NULL;
	bool            isNull = false;

	slot_getallattrs(planSlot);
	ExecGetJunkAttribute(planSlot, resultRelInfo->ri_RowIdAttNo, &isNull);

	/* not matched */
	if (isNull && CONTAINS_INSERT_ACTION(rmtstate))
	{
		ListCell *l;

		/*
		 * Make source tuple available to ExecQual and ExecProject. We don't need
		 * the target tuple since the WHEN quals and the targetlist can't refer to
		 * the target columns.
		 */
		econtext->ecxt_scantuple = planSlot;
		econtext->ecxt_innertuple = planSlot;
		econtext->ecxt_outertuple = NULL;
		foreach (l, resultRelInfo->ri_notMatchedMergeAction)
		{
			MergeActionState *action = (MergeActionState *) lfirst(l);

			/*
			 * Test condition
			 */
			if (!ExecQual(action->mas_whenqual, econtext))
				continue;

			/*
			 * Project the tuple.
			 */
			rslot = ExecProject(action->proj);

			/*
			 * We've activated one of the WHEN clauses, so we don't search
			 * further. This is required behaviour, not an optimization.
			 */
			break;
		}
		if (rslot != NULL)
			return rslot;
	}
	return NULL;
}
