/*-------------------------------------------------------------------------
 *
 * nodeModifyTable.c
 *	  routines to handle ModifyTable nodes.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeModifyTable.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitModifyTable - initialize the ModifyTable node
 *		ExecModifyTable		- retrieve the next tuple from the node
 *		ExecEndModifyTable	- shut down the ModifyTable node
 *		ExecReScanModifyTable - rescan the ModifyTable node
 *
 *	 NOTES
 *		The ModifyTable node receives input from its outerPlan, which is
 *		the data to insert for INSERT cases, or the changed columns' new
 *		values plus row-locating info for UPDATE cases, or just the
 *		row-locating info for DELETE cases.
 *
 *		If the query specifies RETURNING, then the ModifyTable returns a
 *		RETURNING tuple after completing each row insert, update, or delete.
 *		It must be called again to continue the operation.  Without RETURNING,
 *		we just loop within the node until all the work is done, then
 *		return NULL.  This avoids useless call/return overhead.
 */

#include "postgres.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/nodelock.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"

#ifdef __OPENTENBASE__
#include "access/gtm.h"
#include "access/relscan.h"
#include "commands/prepare.h"
#include "commands/vacuum.h"
#include "executor/execMerge.h"
#include "optimizer/pgxcship.h"
#include "optimizer/spm.h"
#include "pgxc/execRemote.h"
#include "pgxc/planner.h"
#include "utils/typcache.h"
#include "utils/ruleutils.h"
#endif
#ifdef _MLS_
#include "utils/mls.h"
#endif
#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif
#ifdef __OPENTENBASE_C__
#include "executor/execFragment.h"
#include "executor/execExpr.h"
#endif

typedef struct MTTargetRelLookup
{
	Oid			relationOid;	/* hash key, must be first */
	int			relationIndex;	/* rel's index in resultRelInfo[] array */
} MTTargetRelLookup;

static bool ExecOnConflictUpdate(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
                                 ItemPointer conflictTid, TupleTableSlot *planSlot,
                                 TupleTableSlot *excludedSlot, EState *estate, bool canSetTag,
                                 TupleTableSlot **returning);
static ResultRelInfo *getTargetResultRelInfo(ModifyTableState *node);
static void ExecSetupChildParentMapForSubplan(ModifyTableState *mtstate);
static TupleConversionMap *tupconv_map_for_subplan(ModifyTableState *node,
						int whichplan);

#ifdef __OPENTENBASE_C__
static void ExecSetupChildParentMapForTcs(ModifyTableState *mtstate);
static void updateGindexInsertSlotInfo(ModifyTableState *mtstate, TupleDesc dataSlotDesc, TupleDesc planSlotDesc, ResultRelInfo *resultRelInfo);
#endif


/*
 * Verify that the tuples to be produced by INSERT match the
 * target relation's rowtype
 * We do this to guard against stale plans.  If plan invalidation is
 * functioning properly then we should never get a failure here, but better
 * safe than sorry.  Note that this is called after we have obtained lock
 * on the target rel, so the rowtype can't change underneath us.
 *
 * The plan output is represented by its targetlist, because that makes
 * handling the dropped-column case easier.
 *
 * We used to use this for UPDATE as well, but now the equivalent checks
 * are done in ExecBuildUpdateProjection.
 */
void
ExecCheckPlanOutput(Relation resultRel, List *targetList)
{
	TupleDesc	resultDesc = RelationGetDescr(resultRel);
	int			attno = 0;
	ListCell   *lc;

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr;
		Assert(!tle->resjunk);	/* caller removed junk items already */

		if (attno >= resultDesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table row type and query-specified row type do not match"),
					 errdetail("Query has too many columns.")));
		attr = TupleDescAttr(resultDesc, attno);
		attno++;

		if (!attr->attisdropped)
		{
			/* Normal case: demand type match */
			if (exprType((Node *) tle->expr) != attr->atttypid &&
				!NestedTableDisguiseTypeEqual(exprType((Node *) tle->expr), attr->atttypid))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Table has type %s at ordinal position %d, but query expects %s.",
								   format_type_be(attr->atttypid),
								   attno,
								   format_type_be(exprType((Node *) tle->expr)))));
		}
		else
		{
			/*
			 * For a dropped column, we can't check atttypid (it's likely 0).
			 * In any case the planner has most likely inserted an INT4 null.
			 * What we insist on is just *some* NULL constant.
			 */
			if (!IsA(tle->expr, Const) ||
				!((Const *) tle->expr)->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Query provides a value for a dropped column at ordinal position %d.",
								   attno)));
		}
	}

	if (attno != resultDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("table row type and query-specified row type do not match"),
				 errdetail("Query has too few columns. attno is %d, and natts is %d", attno, resultDesc->natts)));
}

/*
 * ExecProcessReturning --- evaluate a RETURNING list
 *
 * projectReturning: RETURNING projection info for current result rel
 * tupleSlot: slot holding tuple actually inserted/updated/deleted
 * planSlot: slot holding tuple returned by top subplan node
 *
 * Note: If tupleSlot is NULL, the FDW should have already provided econtext's
 * scan tuple.
 *
 * Returns a slot holding the result tuple
 */
static TupleTableSlot *
ExecProcessReturning(ResultRelInfo *resultRelInfo,
					 TupleTableSlot *tupleSlot,
					 TupleTableSlot *planSlot)
{
	ProjectionInfo *projectReturning = resultRelInfo->ri_projectReturning;
	ExprContext *econtext = projectReturning->pi_exprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous cycle.
	 */
	ResetExprContext(econtext);

	/* Make tuple and any needed join variables available to ExecProject */
	if (tupleSlot)
		econtext->ecxt_scantuple = tupleSlot;
	else
	{
		HeapTuple	tuple;

		/*
		 * RETURNING expressions might reference the tableoid column, so
		 * initialize t_tableOid before evaluating them.
		 */
		Assert(!TupIsNull(econtext->ecxt_scantuple));
		tuple = ExecFetchSlotHeapTuple(econtext->ecxt_scantuple, true, NULL);
		tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
	}
	econtext->ecxt_outertuple = planSlot;

	/* Compute the RETURNING expressions */
	return ExecProject(projectReturning);
}

/*
 * ExecCheckHeapTupleVisible -- verify heap tuple is visible
 *
 * It would not be consistent with guarantees of the higher isolation levels to
 * proceed with avoiding insertion (taking speculative insertion's alternative
 * path) on the basis of another tuple that is not visible to MVCC snapshot.
 * Check for the need to raise a serialization failure, and do so as necessary.
 */
static void
ExecCheckHeapTupleVisible(EState *estate,
						  HeapTuple tuple,
						  Buffer buffer)
{
	if (!IsolationUsesXactSnapshot())
		return;

	/*
	 * We need buffer pin and lock to call HeapTupleSatisfiesVisibility.
	 * Caller should be holding pin, but not lock.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	if (!HeapTupleSatisfiesVisibility(tuple, estate->es_snapshot, buffer))
	{
		/*
		 * We should not raise a serialization failure if the conflict is
		 * against a tuple inserted by our own transaction, even if it's not
		 * visible to our snapshot.  (This would happen, for example, if
		 * conflicting keys are proposed for insertion in a single command.)
		 */
		if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
	}
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

/*
 * ExecCheckTIDVisible -- convenience variant of ExecCheckHeapTupleVisible()
 */
static void
ExecCheckTIDVisible(EState *estate,
					ResultRelInfo *relinfo,
					ItemPointer tid, bool isStash)
{
	Relation	rel;
	Buffer		buffer;
	HeapTupleData tuple;

	/* Redundantly check isolation level */
	if (!IsolationUsesXactSnapshot())
		return;

	if (isStash)
		rel = relinfo->ri_StashRelationDesc;
	else
		rel = relinfo->ri_RelationDesc;

	tuple.t_self = *tid;
	if (!heap_fetch(rel, SnapshotAny, &tuple, &buffer, false, NULL))
		elog(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");
	ExecCheckHeapTupleVisible(estate, &tuple, buffer);
	ReleaseBuffer(buffer);
}

/*
 * ExecGetInsertNewTuple
 *		This prepares a "new" tuple ready to be inserted into given result
 *		relation, by removing any junk columns of the plan's output tuple
 *		and (if necessary) coercing the tuple to the right tuple format.
 */
static TupleTableSlot *
ExecGetInsertNewTuple(ResultRelInfo *relinfo,
					  TupleTableSlot *planSlot)
{
	ProjectionInfo *newProj = relinfo->ri_projectNew;
	ExprContext *econtext;

	/*
	 * If there's no projection to be done, just make sure the slot is of the
	 * right type for the target rel.  If the planSlot is the right type we
	 * can use it as-is, else copy the data into ri_newTupleSlot.
	 */
	if (newProj == NULL)
		return planSlot;

	/*
	 * Else project; since the projection output slot is ri_newTupleSlot, this
	 * will also fix any slot-type problem.
	 *
	 * Note: currently, this is dead code, because INSERT cases don't receive
	 * any junk columns so there's never a projection to be done.
	 */
	econtext = newProj->pi_exprContext;
	econtext->ecxt_outertuple = planSlot;
	return ExecProject(newProj);
}

/*
 * ExecGetUpdateNewTuple
 *		This prepares a "new" tuple by combining an UPDATE subplan's output
 *		tuple (which contains values of changed columns) with unchanged
 *		columns taken from the old tuple.
 *
 * The subplan tuple might also contain junk columns, which are ignored.
 * Note that the projection also ensures we have a slot of the right type.
 */
TupleTableSlot *
ExecGetUpdateNewTuple(ResultRelInfo *relinfo,
					  TupleTableSlot *planSlot,
					  TupleTableSlot *oldSlot)
{
	ProjectionInfo *newProj = relinfo->ri_projectNew;
	ExprContext *econtext;

	Assert(planSlot != NULL && !TTS_EMPTY(planSlot));
	Assert(oldSlot != NULL && !TTS_EMPTY(oldSlot));

	econtext = newProj->pi_exprContext;
	econtext->ecxt_outertuple = planSlot;
	econtext->ecxt_scantuple = oldSlot;
	return ExecProject(newProj);
}

static List *
SPMGetUKIndexes(Relation rel)
{
	List *results = NIL;
	Oid   relid = RelationGetRelid(rel);

	if (relid == SPMPlanRelationId)
	{
		results = lappend_oid(results, SPMPlanSqlidPlanidIndexId);
	}
	else
	{
		Assert(relid == SPMPlanHistoryRelationId);
		results = lappend_oid(results, SPMPlanHistorySqlidPlanidIndexId);
	}

	return results;
}

Oid
ExecInsertCheckConflict(Relation rel, HeapTuple tuple)
{
	/* Perform a speculative insertion. */
	uint32          specToken;
	bool            specConflict = false;
	List           *arbiterIndexes;
	Oid             newId;
	List           *recheckIndexes = NIL;
	EState         *estate = NULL;
	ResultRelInfo  *resultRelInfo;
	TupleDesc       tupdesc = RelationGetDescr(rel);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc);
	Oid             ret = InvalidOid;

	ExecStoreHeapTuple(tuple, slot, false);

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo,
	                  rel,
	                  1, /* dummy rangetable index */
	                  NULL,
	                  0,
	                  false);

	ExecOpenIndices(resultRelInfo, false);

	estate = CreateExecutorState();
	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_range_table = NULL;

	arbiterIndexes = SPMGetUKIndexes(rel);

	/*
	 * Before we start insertion proper, acquire our "speculative
	 * insertion lock".  Others can use that to wait for us to decide
	 * if we're going to go ahead with the insertion, instead of
	 * waiting for the whole transaction to complete.
	 */
	specToken = SpeculativeInsertionLockAcquire(GetCurrentTransactionId());
	HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);

	/* insert the tuple, with the speculative token */
	newId = heap_insert(rel, tuple, estate->es_output_cid, HEAP_INSERT_SPECULATIVE, NULL);

	/* insert index entries for tuple */
	recheckIndexes =
		ExecInsertIndexTuples(slot, &(tuple->t_self), estate, true, &specConflict, arbiterIndexes);

	/* adjust the tuple's state accordingly */
	if (!specConflict)
	{
		heap_finish_speculative(rel, tuple);
		ret = newId;
	}
	else
		heap_abort_speculative(rel, tuple);

	/*
	 * Wake up anyone waiting for our decision.  They will re-check
	 * the tuple, see that it's no longer speculative, and wait on our
	 * XID as if this was a regularly inserted tuple all along.  Or if
	 * we killed the tuple, they will see it's dead, and proceed as if
	 * the tuple never existed.
	 */
	SpeculativeInsertionLockRelease(GetCurrentTransactionId());

	/*
	 * If there was a conflict, start from the beginning.  We'll do
	 * the pre-check again, which will now find the conflicting tuple
	 * (unless it aborts before we get there).
	 */
	if (specConflict)
		list_free(recheckIndexes);

	ExecCloseIndices(resultRelInfo);
	ExecDropSingleTupleTableSlot(slot);
	return ret;
}

/* ----------------------------------------------------------------
 *		ExecInsert
 *
 *		For INSERT, we have to insert the tuple into the target relation
 *		and insert appropriate tuples into the index relations.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access "junk" columns that are not going to be stored.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecInsert(ModifyTableState *mtstate,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EState *estate,
		   bool canSetTag)
{
	HeapTuple	tuple;
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	Oid			newId;
	List	   *recheckIndexes = NIL;
	TupleTableSlot *result = NULL;
	TransitionCaptureState *ar_insert_trig_tcs;
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	OnConflictAction onconflict = node->onConflictAction;
#ifdef __OPENTENBASE__
	bool		has_unshippable_trigger = false;
	int			remoterel_index = 0;
	bool		rel_is_external;
	bool 		hasshard = false;
	AttrNumber *discolnums = NULL;
	int 		ndiscols = 0;
#endif

	/*
	 * get information on the (current) result relation
	 */
	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;
#ifdef _SHARDING_
	rel_is_external = resultRelInfo->ri_FdwRoutine != NULL &&
					  resultRelInfo->ri_FdwRoutine->IsExternalTable != NULL;

#ifdef _MLS_
	mls_update_cls_with_current_user(slot);
#endif

#ifdef __OPENTENBASE_C__
	/*
	 * For column store or external table, we don't need materialize slot
	 * into tuple here.
	 */
	tuple = NULL;
	if (!rel_is_external)
#endif
	/*
	 * get the heap tuple out of the tuple table slot, making sure we have a
	 * writable copy
	 */
	{
		hasshard = RelationIsSharded(resultRelationDesc);
		if (hasshard)
		{
			ndiscols = RelationGetNumDisKeys(resultRelationDesc);
			discolnums = RelationGetDisKeys(resultRelationDesc);
		}

		ExecMaterializeSlot_shard(slot, hasshard, discolnums, ndiscols,
								  RelationGetRelid(resultRelationDesc));
		tuple = slot->tts_tuple;
	}
#else
	tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
#endif

	/*
	 * If the result relation has OIDs, force the tuple's OID to zero so that
	 * heap_insert will assign a fresh OID.  Usually the OID already will be
	 * zero at this point, but there are corner cases where the plan tree can
	 * return a tuple extracted literally from some table with the same
	 * rowtype.
	 *
	 * XXX if we ever wanted to allow users to assign their own OIDs to new
	 * rows, this'd be the place to do it.  For the moment, we make a point of
	 * doing this before calling triggers, so that a user-supplied trigger
	 * could hack the OID if desired.
	 */
#ifdef __OPENTENBASE_C__
	if (!rel_is_external && resultRelationDesc->rd_rel->relhasoids)
#else
	if (resultRelationDesc->rd_rel->relhasoids)
#endif
		HeapTupleSetOid(tuple, InvalidOid);


#ifdef __OPENTENBASE__
	if (IS_DISTRIBUTED_DATANODE && onconflict == ONCONFLICT_UPDATE && resultRelInfo->ri_TrigDesc)
	{
		int16 trigevent = pgxc_get_trigevent(mtstate->operation);

		has_unshippable_trigger = pgxc_find_unshippable_triggers(resultRelInfo->ri_TrigDesc, trigevent, 0, true);
	}
#endif

	/*
	 * BEFORE ROW INSERT Triggers.
	 *
	 * Note: We fire BEFORE ROW TRIGGERS for every attempted insertion in an
	 * INSERT ... ON CONFLICT statement.  We cannot check for constraint
	 * violations before firing these triggers, because they can change the
	 * values to insert.  Also, they can run arbitrary user-defined code with
	 * side-effects that we can't cancel by just not inserting the tuple.
	 */
	if ((mtstate->operation != CMD_MERGE || (IS_CENTRALIZED_DATANODE && ORA_MODE)) &&
		resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		slot = ExecBRInsertTriggers(estate, resultRelInfo, slot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/* trigger might have changed tuple */
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);

		if (hasshard)
		{
			slot_getallattrs(slot);
			heap_tuple_set_shardid(tuple, slot, discolnums, ndiscols,
									RelationGetRelid(resultRelationDesc));
		}
	}

	/* INSTEAD OF ROW INSERT Triggers */
	if ((mtstate->operation != CMD_MERGE || (IS_CENTRALIZED_DATANODE && ORA_MODE)) &&
		resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_insert_instead_row)
	{
		slot = ExecIRInsertTriggers(estate, resultRelInfo, slot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/* trigger might have changed tuple */
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
		if (hasshard)
		{
			slot_getallattrs(slot);
			heap_tuple_set_shardid(tuple, slot, discolnums, ndiscols,
									RelationGetRelid(resultRelationDesc));
		}
		newId = InvalidOid;
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * insert into foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/* FDW might have changed tuple */
		if (!rel_is_external)
		{
			tuple = ExecFetchSlotHeapTuple(slot, true, NULL);

			/*
			 * AFTER ROW Triggers or RETURNING expressions might reference the
			 * tableoid column, so initialize t_tableOid before evaluating them.
			 */
			tuple->t_tableOid = RelationGetRelid(resultRelationDesc);
		}

		newId = InvalidOid;
	}
	else
	{
		WCOKind		wco_kind;

		/*
		 * Constraints might reference the tableoid column, so initialize
		 * t_tableOid before evaluating them.
		 */
		tuple->t_tableOid = RelationGetRelid(resultRelationDesc);

		/*
		 * Check any RLS WITH CHECK policies.
		 *
		 * Normally we should check INSERT policies. But if the insert is the
		 * result of a partition key update that moved the tuple to a new
		 * partition, we should instead check UPDATE policies, because we are
		 * executing policies defined on the target table, and not those
		 * defined on the child partitions.
		 *
		 * If we're running MERGE, we refer to the action that we're executing
		 * to know if we're doing an INSERT or UPDATE to a partition table.
		 */
		if (mtstate->operation == CMD_UPDATE)
			wco_kind = WCO_RLS_UPDATE_CHECK;
		else if (mtstate->operation == CMD_MERGE)
			wco_kind = (mtstate->relaction->mas_action->commandType == CMD_UPDATE) ?
				WCO_RLS_UPDATE_CHECK : WCO_RLS_INSERT_CHECK;
		else
			wco_kind = WCO_RLS_INSERT_CHECK;

		/*
		 * ExecWithCheckOptions() will skip any WCOs which are not of the kind
		 * we are looking for at this point.
		 */
		if (resultRelInfo->ri_WithCheckOptions != NIL)
			ExecWithCheckOptions(wco_kind, resultRelInfo, slot, estate);

		/* Check the constraints of the tuple */
		if (resultRelationDesc->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate);

		/*
		* Also check the tuple against the partition constraint, if there is
		* one; except that if we got here via tuple-routing, we don't need to
		* if there's no BR trigger defined on the partition.
		*/
		if (resultRelInfo->ri_PartitionCheck &&
			(resultRelInfo->ri_PartitionRoot == NULL ||
			(resultRelInfo->ri_TrigDesc &&
			resultRelInfo->ri_TrigDesc->trig_insert_before_row)))
			ExecPartitionCheck(resultRelInfo, slot, estate, true);

#ifdef __OPENTENBASE__
		/*
		 * DML with unshippable triggers on resultrelation, we execute DML
		 * on coordiantor.
		 */
		if (IS_PGXC_COORDINATOR && node->remote_plans)
		{
			bool succeed = false;
			UPSERT_ACTION result = UPSERT_NONE;
			TupleTableSlot *returning = NULL;
			
			succeed = ExecRemoteDML(mtstate, NULL, NULL,
								      slot, planSlot, estate, NULL,
								      canSetTag, &returning, &result,
								      resultRelInfo, remoterel_index);

			if (succeed)
			{
				if (result == UPSERT_UPDATE)
					return returning;
			}
			else
			{
				return NULL;
			}

			newId = InvalidOid;
		}
		else
		{
#endif
		if (onconflict != ONCONFLICT_NONE && resultRelInfo->ri_NumIndices > 0)
		{
			/* Perform a speculative insertion. */
			uint32          specToken;
			ItemPointerData conflictTid;
			bool            specConflict;
			bool            isStash;
			List           *arbiterIndexes;

			arbiterIndexes = resultRelInfo->ri_onConflictArbiterIndexes;

			/*
			 * Do a non-conclusive check for conflicts first.
			 *
			 * We're not holding any locks yet, so this doesn't guarantee that
			 * the later insert won't conflict.  But it avoids leaving behind
			 * a lot of canceled speculative insertions, if you run a lot of
			 * INSERT ON CONFLICT statements that do conflict.
			 *
			 * We loop back here if we find a conflict below, either during
			 * the pre-check, or when we re-check after inserting the tuple
			 * speculatively.
			 */
	vlock:
			specConflict = false;
			if (!ExecCheckIndexConstraints(slot, estate, &conflictTid, arbiterIndexes, &isStash))
			{
				/* committed conflict tuple found */
				if (onconflict == ONCONFLICT_UPDATE)
				{
					/*
					 * In case of ON CONFLICT DO UPDATE, execute the UPDATE
					 * part.  Be prepared to retry if the UPDATE fails because
					 * of another concurrent UPDATE/DELETE to the conflict
					 * tuple.
					 */
					TupleTableSlot *returning = NULL;
#ifdef _MLS_
                    bool            ret;
                    int             oldtag;
#endif
#ifdef __OPENTENBASE__
					if (has_unshippable_trigger)
						return NULL;
#endif
#ifdef _MLS_
					oldtag = mls_command_tag_switch_to(CLS_CMD_WRITE);

					ret = ExecOnConflictUpdate(mtstate, resultRelInfo, &conflictTid, planSlot,
							                       slot, estate, canSetTag, &returning);
					mls_command_tag_switch_to(oldtag);
#endif
					if (ret)
					{
						InstrCountTuples2(&mtstate->ps, 1);
						return returning;
					}
					else
						goto vlock;
				}
				else
				{
					/*
					 * In case of ON CONFLICT DO NOTHING, do nothing. However,
					 * verify that the tuple is visible to the executor's MVCC
					 * snapshot at higher isolation levels.
					 */
					Assert(onconflict == ONCONFLICT_NOTHING);
					ExecCheckTIDVisible(estate, resultRelInfo, &conflictTid, isStash);

					InstrCountTuples2(&mtstate->ps, 1);
					return NULL;
				}
			}

			/*
			 * Before we start insertion proper, acquire our "speculative
			 * insertion lock".  Others can use that to wait for us to decide
			 * if we're going to go ahead with the insertion, instead of
			 * waiting for the whole transaction to complete.
			 */
			specToken = SpeculativeInsertionLockAcquire(GetCurrentTransactionId());
			HeapTupleHeaderSetSpeculativeToken(tuple->t_data, specToken);

			/* insert the tuple, with the speculative token */
			newId = heap_insert(resultRelationDesc, tuple,
								estate->es_output_cid,
								HEAP_INSERT_SPECULATIVE,
								NULL);

			/* insert index entries for tuple */
			recheckIndexes = ExecInsertIndexTuples(slot, &(tuple->t_self),
												   estate, true, &specConflict,
												   arbiterIndexes);

			/* adjust the tuple's state accordingly */
			if (!specConflict)
				heap_finish_speculative(resultRelationDesc, tuple);
			else
				heap_abort_speculative(resultRelationDesc, tuple);

			/*
			 * Wake up anyone waiting for our decision.  They will re-check
			 * the tuple, see that it's no longer speculative, and wait on our
			 * XID as if this was a regularly inserted tuple all along.  Or if
			 * we killed the tuple, they will see it's dead, and proceed as if
			 * the tuple never existed.
			 */
			SpeculativeInsertionLockRelease(GetCurrentTransactionId());

			/*
			 * If there was a conflict, start from the beginning.  We'll do
			 * the pre-check again, which will now find the conflicting tuple
			 * (unless it aborts before we get there).
			 */
			if (specConflict)
			{
				list_free(recheckIndexes);
#ifdef __OPENTENBASE__
				if (has_unshippable_trigger)
					return NULL;
#endif
				goto vlock;
			}
			/* Since there was no insertion conflict, we're done */
		}
		else
		{
			/*
			 * insert the tuple normally.
			 *
			 * Note: heap_insert returns the tid (location) of the new tuple
			 * in the t_self field.
			 */
			newId = heap_insert(resultRelationDesc, tuple,
								estate->es_output_cid,
								0, NULL);

			/* insert index entries for tuple */
			if (resultRelInfo->ri_NumIndices > 0)
				recheckIndexes = ExecInsertIndexTuples(slot, &(tuple->t_self),
													   estate, false, NULL,
													   NIL);
		}
#ifdef __OPENTENBASE__
		}
#endif
	}

	/* ora_compatible */
	if (canSetTag && !mtstate->mi_dis_settag)
	{
		(estate->es_processed)++;

		estate->es_lastoid = newId;
#ifdef __OPENTENBASE_C__
		if (!rel_is_external)
#endif
		setLastTid(&(tuple->t_self));
	}

	/*
	 * If this insert is the result of a partition key update that moved the
	 * tuple to a new partition, put this row into the transition NEW TABLE,
	 * if there is one. We need to do this separately for DELETE and INSERT
	 * because they happen on different tables.
	 */
	ar_insert_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture
		&& mtstate->mt_transition_capture->tcs_update_new_table)
	{
		ExecARUpdateTriggers(estate, resultRelInfo, NULL,
							 NULL,
							 tuple,
							 NULL,
							 mtstate->mt_transition_capture);

		/*
		 * We've already captured the NEW TABLE row, so make sure any AR
		 * INSERT trigger fired below doesn't capture it again.
		 */
		ar_insert_trig_tcs = NULL;
	}

	/* AFTER ROW INSERT Triggers */
	if (!rel_is_external &&
		(mtstate->operation != CMD_MERGE || (IS_CENTRALIZED_DATANODE && ORA_MODE)))
		ExecARInsertTriggers(estate, resultRelInfo, tuple, recheckIndexes,
						 ar_insert_trig_tcs);

	list_free(recheckIndexes);

    /*
     * Check any WITH CHECK OPTION constraints from parent views.  We are
     * required to do this after testing all constraints and uniqueness
     * violations per the SQL spec, so we do it after actually inserting the
     * record into the heap and all indexes.
     *
     * ExecWithCheckOptions will elog(ERROR) if a violation is found, so the
     * tuple will never be seen, if it violates the WITH CHECK OPTION.
     *
     * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
     * are looking for at this point.
     */
    if (resultRelInfo->ri_WithCheckOptions != NIL)
        ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);

    /* Process RETURNING if present */
    if (resultRelInfo->ri_projectReturning)
        result = ExecProcessReturning(resultRelInfo, slot, planSlot);

    return result;
}

/* ----------------------------------------------------------------
 *		ExecDelete
 *
 *		DELETE is like UPDATE, except that we delete the tuple and no
 *		index modifications are needed.
 *
 *		When deleting from a table, tupleid identifies the tuple to
 *		delete and oldtuple is NULL.  When deleting from a view,
 *		oldtuple is passed to the INSTEAD OF triggers and identifies
 *		what to delete, and tupleid is invalid.  When deleting from a
 *		foreign table, tupleid is invalid; the FDW has to figure out
 *		which row to delete using data from the planSlot.  oldtuple is
 *		passed to foreign table triggers; it is NULL when the foreign
 *		table has no relevant triggers. We use tupleDeleted to indicate
 *		whether the tuple is actually deleted, callers can use it to
 *		decide whether to continue the operation.  When this DELETE is a
 *		part of an UPDATE of partition-key, then the slot returned by
 *		EvalPlanQual() is passed back using output parameter epqslot.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
#ifdef __OPENTENBASE__
TupleTableSlot *
ExecDelete(ModifyTableState *mtstate,
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
		   TupleTableSlot **epqslot)
#else
TupleTableSlot *
ExecDelete(ModifyTableState *mtstate,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool *tupleDeleted,
		   bool processReturning,
		   bool canSetTag,
		   bool changingPart,
		   TupleTableSlot **epqslot)
#endif
{
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	HTSU_Result result;
	HeapUpdateFailureData hufd;
	TupleTableSlot *slot = NULL;
	TransitionCaptureState *ar_delete_trig_tcs;
#ifdef __OPENTENBASE__
	int        remoterel_index = 0;
	ModifyTable *mt = (ModifyTable *)mtstate->ps.plan;
	bool set_processed = true;
#endif

	if (tupleDeleted)
		*tupleDeleted = false;

	/*
	 * get information on the (current) result relation
	 */
	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;
#ifdef __OPENTENBASE__
	if (TTS_REMOTETID_SCAN(planSlot))
	{
		PlanState  *subplanstate = outerPlanState(mtstate);
		return ((ScanState*)subplanstate)->ps.ps_ExprContext->ecxt_outertuple;
	}
#endif

	/* BEFORE ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_before_row)
	{
		bool		dodelete;

		dodelete = ExecBRDeleteTriggers(estate, epqstate, resultRelInfo,
										tupleid, oldtuple, epqslot);

		if (!dodelete)			/* "do nothing" */
			return NULL;
	}

	/* INSTEAD OF ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_instead_row)
	{
		bool		dodelete;

		Assert(oldtuple != NULL);
		dodelete = ExecIRDeleteTriggers(estate, resultRelInfo, oldtuple);

		if (!dodelete)			/* "do nothing" */
			return NULL;
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		HeapTuple	tuple;

		/*
		 * delete from foreign table: let the FDW do it
		 *
		 * We offer the trigger tuple slot as a place to store RETURNING data,
		 * although the FDW can return some other slot if it wants.  Set up
		 * the slot's tupdesc so the FDW doesn't need to do that for itself.
		 */
		slot = estate->es_trig_tuple_slot;
		if (slot->tts_tupleDescriptor != RelationGetDescr(resultRelationDesc))
			ExecSetSlotDescriptor(slot, RelationGetDescr(resultRelationDesc));

		slot = resultRelInfo->ri_FdwRoutine->ExecForeignDelete(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/*
		 * RETURNING expressions might reference the tableoid column, so
		 * initialize t_tableOid before evaluating them.
		 */
		if (TTS_EMPTY(slot))
			ExecStoreAllNullTuple(slot);
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
		tuple->t_tableOid = RelationGetRelid(resultRelationDesc);
	}
	else
	{
#ifdef __OPENTENBASE__
		if (IS_PGXC_COORDINATOR && mt->remote_plans)
		{
			bool succeed = false;

			succeed = ExecRemoteDML(mtstate, tupleid, oldtuple,
						sourceslot, planSlot, estate, epqstate,
						canSetTag, NULL, NULL,
						resultRelInfo, remoterel_index);
			if (!succeed)
				return NULL;
		}
		else
		{
#endif
			{
		/*
		 * delete the tuple
		 *
		 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
		 * that the row to be deleted is visible to that snapshot, and throw a
		 * can't-serialize error if not. This is a special-case behavior
		 * needed for referential integrity updates in transaction-snapshot
		 * mode transactions.
		 */
ldelete:;
		result = heap_delete(resultRelationDesc, tupleid,
							 estate->es_output_cid,
							 estate->es_crosscheck_snapshot,
							 true /* wait for commit */ ,
							 &hufd,
							 changingPart);
		switch (result)
		{
			case HeapTupleSelfUpdated:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  The former case is possible in a join DELETE
				 * where multiple tuples join to the same target tuple. This
				 * is somewhat questionable, but Postgres has always allowed
				 * it: we just ignore additional deletion attempts.
				 *
				 * The latter case arises if the tuple is modified by a
				 * command in a BEFORE trigger, or perhaps by a command in a
				 * volatile function used in the query.  In such situations we
				 * should not ignore the deletion, but it is equally unsafe to
				 * proceed.  We don't want to discard the original DELETE
				 * while keeping the triggered actions based on its deletion;
				 * and it would be no better to allow the original DELETE
				 * while discarding updates that it triggered.  The row update
				 * carries some information that might be important according
				 * to business rules; so throwing an error is the only safe
				 * course.
				 *
				 * If a trigger actually intends this type of interaction, it
				 * can re-execute the DELETE and then return NULL to cancel
				 * the outer delete.
				 */
				if (hufd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
				if (mtstate->operation == CMD_MERGE)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CARDINALITY_VIOLATION),
							 errmsg("MERGE command cannot affect row a "
							        "second time"),
							 errhint("Ensure that not more than one source "
							         "row matches any one target row.")));
				}
				/* Else, already deleted by self; nothing to do */
				return NULL;

			case HeapTupleMayBeUpdated:
				break;

			case HeapTupleUpdated:
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				if (ItemPointerIndicatesMovedPartitions(&hufd.ctid))
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
									errmsg("tuple to be deleted was already moved to another partition due to concurrent update")));

				if (!ItemPointerEquals(tupleid, &hufd.ctid))
				{
					TupleTableSlot *my_epqslot;

					my_epqslot = EvalPlanQual(estate,
										   epqstate,
										   resultRelationDesc,
										   resultRelInfo->ri_RangeTableIndex,
										   LockTupleExclusive,
										   &hufd.ctid,
										   hufd.xmax);
					if (!TupIsNull(my_epqslot))
					{
						*tupleid = hufd.ctid;

						/*
						 * If requested, skip delete and pass back the updated
						 * row.
						 */
						if (epqslot)
						{
							*epqslot = my_epqslot;
							return NULL;
						}
						else
							goto ldelete;
					}
				}

				/* tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized heap_delete status: %u", result);
				return NULL;
		}

		/*
		 * Note: Normally one would think that we have to delete index tuples
		 * associated with the heap tuple now...
		 *
		 * ... but in POSTGRES, we have no need to do this because VACUUM will
		 * take care of it later.  We can't delete index tuples immediately
		 * anyway, since the tuple is still visible to other transactions.
		 */

			}
#ifdef __OPENTENBASE__
		}
#endif
	}

	if (canSetTag && set_processed)
		(estate->es_processed)++;

	/* Tell caller that the delete actually happened. */
	if (tupleDeleted)
		*tupleDeleted = true;

	/*
	 * If this delete is the result of a partition key update that moved the
	 * tuple to a new partition, put this row into the transition OLD TABLE,
	 * if there is one. We need to do this separately for DELETE and INSERT
	 * because they happen on different tables.
	 */
	ar_delete_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture
		&& mtstate->mt_transition_capture->tcs_update_old_table)
	{
		ExecARUpdateTriggers(estate, resultRelInfo,
							 tupleid,
							 oldtuple,
							 NULL,
							 NULL,
							 mtstate->mt_transition_capture);

		/*
		 * We've already captured the NEW TABLE row, so make sure any AR
		 * DELETE trigger fired below doesn't capture it again.
		 */
		ar_delete_trig_tcs = NULL;
	}

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggers(estate, resultRelInfo, tupleid, oldtuple,
						 ar_delete_trig_tcs);

	/* Process RETURNING if present and if requested */
	if (processReturning && resultRelInfo->ri_projectReturning)
	{
		/*
		 * We have to put the target tuple into a slot, which means first we
		 * gotta fetch it.  We can use the trigger tuple slot.
		 */
		TupleTableSlot *rslot;
		HeapTupleData deltuple;
		Buffer		delbuffer;

		if (resultRelInfo->ri_FdwRoutine)
		{
			/* FDW must have provided a slot containing the deleted row */
			Assert(!TupIsNull(slot));
			delbuffer = InvalidBuffer;
		}
		else
		{
			slot = estate->es_trig_tuple_slot;
			if (oldtuple != NULL)
			{
				deltuple = *oldtuple;
				delbuffer = InvalidBuffer;
			}
			else
			{
				deltuple.t_self = *tupleid;
				if (!heap_fetch(resultRelationDesc, SnapshotAny,
								&deltuple, &delbuffer, false, NULL))
					elog(ERROR, "failed to fetch deleted tuple for DELETE RETURNING");
			}

			if (slot->tts_tupleDescriptor != RelationGetDescr(resultRelationDesc))
				ExecSetSlotDescriptor(slot, RelationGetDescr(resultRelationDesc));
			ExecStoreHeapTuple(&deltuple, slot, false);
		}

		rslot = ExecProcessReturning(resultRelInfo, slot, planSlot);

		/*
		 * Before releasing the target tuple again, make sure rslot has a
		 * local copy of any pass-by-reference values.
		 */
		ExecMaterializeSlot(rslot);

		ExecClearTuple(slot);
		if (BufferIsValid(delbuffer))
			ReleaseBuffer(delbuffer);

		return rslot;
	}

	return NULL;
}

/*
 * ExecCrossPartitionUpdate --- Move an updated tuple to another partition.
 *
 * This works by first deleting the old tuple from the current partition,
 * followed by inserting the new tuple into the root parent table, that is,
 * mtstate->rootResultRelInfo.  It will be re-routed from there to the
 * correct partition.
 *
 * Returns true if the tuple has been successfully moved, or if it's found
 * that the tuple was concurrently deleted so there's nothing more to do
 * for the caller.
 *
 * False is returned if the tuple we're trying to move is found to have been
 * concurrently updated.  In that case, the caller must to check if the
 * updated tuple that's returned in *retry_slot still needs to be re-routed,
 * and call this function again or perform a regular update accordingly.
 */
static bool
ExecCrossPartitionUpdate(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo,
						 ItemPointer tupleid, HeapTuple oldtuple,
						 TupleTableSlot *slot, TupleTableSlot *planSlot,
						 EPQState *epqstate, bool canSetTag,
						 TupleTableSlot **retry_slot,
						 TupleTableSlot **inserted_tuple,
						 TupleTableSlot **epqslot)
{
	EState	   *estate = mtstate->ps.state;
	PartitionTupleRouting *proute = mtstate->mt_partition_tuple_routing;
	TupleConversionMap *tupconv_map;
	TupleConversionMap *saved_tcs_map = NULL;
	TupleTableSlot *my_epqslot = NULL;
	bool		tuple_deleted;
#ifdef __OPENTENBASE__
	bool		set_processed = true;
#endif

	*inserted_tuple = NULL;
	*retry_slot = NULL;

	/*
	 * When an UPDATE is run on a leaf partition, we will not have
	 * partition tuple routing set up. In that case, fail with
	 * partition constraint violation error.
	 */
	if (proute == NULL)
		ExecPartitionCheckEmitError(resultRelInfo, slot, estate);

	/*
	 * Row movement, part 1.  Delete the tuple, but skip RETURNING
	 * processing. We want to return rows from INSERT.
	 */
	ExecDelete(mtstate, tupleid, oldtuple, slot, planSlot, epqstate, estate,
			   &tuple_deleted, false, false, true, &my_epqslot);

	/*
	 * For some reason if DELETE didn't happen (e.g. trigger prevented
	 * it, or it was already deleted by self, or it was concurrently
	 * deleted by another transaction), then we should skip the insert
	 * as well; otherwise, an UPDATE could cause an increase in the
	 * total number of rows across all partitions, which is clearly
	 * wrong.
	 *
	 * For a normal UPDATE, the case where the tuple has been the
	 * subject of a concurrent UPDATE or DELETE would be handled by
	 * the EvalPlanQual machinery, but for an UPDATE that we've
	 * translated into a DELETE from this partition and an INSERT into
	 * some other partition, that's not available, because CTID chains
	 * can't span relation boundaries.  We mimic the semantics to a
	 * limited extent by skipping the INSERT if the DELETE fails to
	 * find a tuple. This ensures that two concurrent attempts to
	 * UPDATE the same tuple at the same time can't turn one tuple
	 * into two, and that an UPDATE of a just-deleted tuple can't
	 * resurrect it.
	 */
	if (!tuple_deleted)
	{
		/*
		 * my_epqslot will be typically NULL.  But when ExecDelete() finds that
		 * another transaction has concurrently updated the same row, it
		 * re-fetches the row, skips the delete, and my_epqslot is set to the
		 * re-fetched tuple slot.  In that case, we need to do all the checks
		 * again.
		 */
		if (TupIsNull(my_epqslot))
			return true;
		else
		{
			/* Fetch the most recent version of old tuple. */
			TupleTableSlot *oldSlot = resultRelInfo->ri_oldTupleSlot;
			HeapTupleData updtuple;
			Buffer		updbuffer;

			if (epqslot && mtstate->operation == CMD_MERGE)
			{
				*epqslot = my_epqslot;
				return true;
			}
	
			updtuple.t_self = *tupleid;
			if (!heap_fetch(resultRelInfo->ri_RelationDesc, SnapshotAny, &updtuple, &updbuffer, false, NULL))
				elog(ERROR, "failed to fetch tuple being updated");

			ExecStoreBufferHeapTuple(&updtuple, oldSlot, updbuffer);
			ReleaseBuffer(updbuffer);
			*retry_slot = ExecGetUpdateNewTuple(resultRelInfo, my_epqslot, oldSlot);
			return false;
		}
	}


	/*
	 * Updates set the transition capture map only when a new subplan
	 * is chosen.  But for inserts, it is set for each row. So after
	 * INSERT, we need to revert back to the map created for UPDATE;
	 * otherwise the next UPDATE will incorrectly use the one created
	 * for INSERT.  So first save the one created for UPDATE.
	 */
	if (mtstate->mt_transition_capture)
		saved_tcs_map = mtstate->mt_transition_capture->tcs_map;

	/*
	 * resultRelInfo is one of the per-subplan resultRelInfos.  So we
	 * should convert the tuple into root's tuple descriptor, since
	 * ExecInsert() starts the search from root.  The tuple conversion
	 * map list is in the order of mtstate->resultRelInfo[], so to
	 * retrieve the one for this resultRel, we need to know the
	 * position of the resultRel in mtstate->resultRelInfo[].
	 *
	 * when exec merge into, the resultRelInfo was modify by ExecPrepareTupleRouting
	 * before call the ExecUpdate function, hence map_index is invalid
	 * and ExecPrepareTupleRouting will call ConvertPartitionTupleSlot, so skip it
	 */
	if (mtstate->operation != CMD_MERGE)
	{
		int map_index = resultRelInfo - mtstate->resultRelInfo;
		Assert(map_index >= 0 && map_index < mtstate->mt_nrels);
		tupconv_map = tupconv_map_for_subplan(mtstate, map_index);
		if (tupconv_map != NULL)
			slot = execute_attr_map_slot(tupconv_map->attrMap,
					slot, proute->root_tuple_slot,
					resultRelInfo->ri_RelationDesc);
	}

	/* Prepare for tuple routing */
	Assert(mtstate->rootResultRelInfo != NULL);
	slot = ExecPrepareTupleRouting(mtstate, estate, proute,
								   mtstate->rootResultRelInfo, slot, InvalidOid);

	*inserted_tuple = ExecInsert(mtstate, slot, planSlot,
							estate, (canSetTag && set_processed));

	/* Revert ExecPrepareTupleRouting's node change. */
	estate->es_result_relation_info = resultRelInfo;
	if (mtstate->mt_transition_capture)
	{
		mtstate->mt_transition_capture->tcs_original_insert_tuple = NULL;
		mtstate->mt_transition_capture->tcs_map = saved_tcs_map;
	}

	/* We're done moving. */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecUpdate
 *
 *		note: we can't run UPDATE queries with transactions
 *		off because UPDATEs are actually INSERTs and our
 *		scan will mistakenly loop forever, updating the tuple
 *		it just inserted..  This should be fixed but until it
 *		is, we don't want to get stuck in an infinite loop
 *		which corrupts your database..
 *
 *		When updating a table, tupleid identifies the tuple to
 *		update and oldtuple is NULL.  When updating a view, oldtuple
 *		is passed to the INSTEAD OF triggers and identifies what to
 *		update, and tupleid is invalid.  When updating a foreign table,
 *		tupleid is invalid; the FDW has to figure out which row to
 *		update using data from the planSlot.  oldtuple is passed to
 *		foreign table triggers; it is NULL when the foreign table has
 *		no relevant triggers.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access values from other input tables (for RETURNING),
 *		row-ID junk columns, etc.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecUpdate(ModifyTableState *mtstate,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool canSetTag,
		   bool merge_ignore)
{
	HeapTuple	tuple;
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	HTSU_Result result;
	HeapUpdateFailureData hufd;
	List	   *recheckIndexes = NIL;
#ifdef __OPENTENBASE__
	int		remoterel_index = 0;
	ModifyTable *mt = (ModifyTable *)mtstate->ps.plan;
	bool set_processed = true;
#endif
#ifdef _SHARDING_
	bool hasshard = false;
	AttrNumber *discolnums = NULL;
	int ndiscols = 0;
#endif

	/*
	 * abort the operation if not running transactions
	 */
	if (IsBootstrapProcessingMode())
		elog(ERROR, "cannot UPDATE during bootstrap");

	/*
	 * get information on the (current) result relation
	 */
	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (TTS_REMOTETID_SCAN(planSlot))
	{
		PlanState  *subplanstate = outerPlanState(mtstate);
		if (relation_has_cross_node_index(resultRelationDesc))
		{
			mtstate->mt_gindex_result_slot = ((ScanState*)subplanstate)->ps.ps_ExprContext->ecxt_outertuple;
			mtstate->mt_gindex_origin_slot_used = false;
			SET_TTS_REMOTETID_SCAN(mtstate->mt_gindex_result_slot);
		}
		return planSlot;
	}

	/*
	 * get the heap tuple out of the tuple table slot, making sure we have a
	 * writable copy
	 */
#ifdef _SHARDING_
	{
		hasshard = RelationIsSharded(resultRelationDesc);
		if(hasshard)
		{
			ndiscols = RelationGetNumDisKeys(resultRelationDesc);
			discolnums = RelationGetDisKeys(resultRelationDesc);
		}

		ExecMaterializeSlot_shard(slot, hasshard, discolnums, ndiscols,
								  RelationGetRelid(resultRelationDesc));
		tuple = slot->tts_tuple;
	}
#else
	tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
#endif

	/* BEFORE ROW UPDATE Triggers */
	if ((mtstate->operation != CMD_MERGE || (IS_CENTRALIZED_DATANODE && ORA_MODE)) &&
		resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		slot = ExecBRUpdateTriggers(estate, epqstate, resultRelInfo,
									tupleid, oldtuple, slot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/* trigger might have changed tuple */
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
#ifdef _SHARDING_
		/* recompute shardid and set it to tuple header */
		if (hasshard)
		{
			slot_getallattrs(slot);
			heap_tuple_set_shardid(tuple, slot, discolnums, ndiscols,
									RelationGetRelid(resultRelationDesc));
		}

		if(RelationHasExtent(resultRelationDesc) &&
			!ShardIDIsValid(HeapTupleGetShardId(tuple)))
			elog(PANIC, "relation is extent, but shardid of tuple is invalid.");
#endif
	}

	/* INSTEAD OF ROW UPDATE Triggers */
	if ((mtstate->operation != CMD_MERGE || (IS_CENTRALIZED_DATANODE && ORA_MODE)) &&
		resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_instead_row)
	{
		slot = ExecIRUpdateTriggers(estate, resultRelInfo,
									oldtuple, slot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/* trigger might have changed tuple */
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
#ifdef _SHARDING_
		/* recompute shardid and set it to tuple header */
		if (hasshard)
		{
			slot_getallattrs(slot);
			heap_tuple_set_shardid(tuple, slot, discolnums, ndiscols,
									RelationGetRelid(resultRelationDesc));
		}

		if (RelationHasExtent(resultRelationDesc) &&
			!ShardIDIsValid(HeapTupleGetShardId(tuple)))
			elog(PANIC, "relation is extent, but shardid of tuple is invalid.");
#endif
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * update in foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

		if (slot == NULL)		/* "do nothing" */
			return NULL;

		/* FDW might have changed tuple */
		tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
#ifdef _SHARDING_
		if(RelationHasExtent(resultRelationDesc) &&
			!ShardIDIsValid(HeapTupleGetShardId(tuple)))
			elog(PANIC, "relation is extent, but shardid of tuple is invalid.");
#endif

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so initialize t_tableOid before evaluating them.
		 */
		tuple->t_tableOid = RelationGetRelid(resultRelationDesc);
	}
	else
	{
		LockTupleMode lockmode;
		bool		partition_constraint_failed;

		/*
		 * Constraints might reference the tableoid column, so initialize
		 * t_tableOid before evaluating them.
		 */
		tuple->t_tableOid = RelationGetRelid(resultRelationDesc);

		/*
		 * Check any RLS UPDATE WITH CHECK policies
		 *
		 * If we generate a new candidate tuple after EvalPlanQual testing, we
		 * must loop back here and recheck any RLS policies and constraints.
		 * (We don't need to redo triggers, however.  If there are any BEFORE
		 * triggers then trigger.c will have done heap_lock_tuple to lock the
		 * correct tuple, so there's no need to do them again.)
		 */
lreplace:;

		/*
		 * If partition constraint fails, this row might get moved to another
		 * partition, in which case we should check the RLS CHECK policy just
		 * before inserting into the new partition, rather than doing it here.
		 * This is because a trigger on that partition might again change the
		 * row.  So skip the WCO checks if the partition constraint fails.
		 */
		partition_constraint_failed =
				resultRelInfo->ri_PartitionCheck &&
			!ExecPartitionCheck(resultRelInfo, slot, estate, false);

		if (!partition_constraint_failed &&
			resultRelInfo->ri_WithCheckOptions != NIL)
		{
			/*
			 * ExecWithCheckOptions() will skip any WCOs which are not of the
			 * kind we are looking for at this point.
			 */

			ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
								 resultRelInfo, slot, estate);
		}

		/*
		 * If a partition check failed, try to move the row into the right
		 * partition.
		 */
		if (partition_constraint_failed)
		{
			TupleTableSlot	*inserted_tuple,
							*retry_slot,
							*epqslot = NULL;
			bool		retry;

			/*
			 * Disallow an INSERT ON CONFLICT DO UPDATE that causes the
			 * original row to migrate to a different partition.  Maybe this
			 * can be implemented some day, but it seems a fringe feature with
			 * little redeeming value.
			 */
			if (((ModifyTable *) mtstate->ps.plan)->onConflictAction == ONCONFLICT_UPDATE)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				                errmsg("invalid ON UPDATE specification"),
				                errdetail("The result tuple would appear in a different partition "
				                          "than the original tuple.")));
			
			/*
			 * ExecCrossPartitionUpdate will first DELETE the row from the
			 * partition it's currently in and then insert it back into the
			 * root table, which will re-route it to the correct partition.
			 * The first part may have to be repeated if it is detected that
			 * the tuple we're trying to move has been concurrently updated.
			 */
			retry = !ExecCrossPartitionUpdate(mtstate, resultRelInfo, tupleid,
											  oldtuple, slot, planSlot,
											  epqstate, canSetTag,
											  &retry_slot, &inserted_tuple, &epqslot);
			if (retry)
			{
				slot = retry_slot;
				goto lreplace;
			}

			if (!TupIsNull(epqslot) && mtstate->operation == CMD_MERGE)
				return epqslot;

			return inserted_tuple;
		}

		/*
		 * Check the constraints of the tuple.  Note that we pass the same
		 * slot for the orig_slot argument, because unlike ExecInsert(), no
		 * tuple-routing is performed here, hence the slot remains unchanged.
		 * We've already checked the partition constraint above; however, we
		 * must still ensure the tuple passes all other constraints, so we
		 * will call ExecConstraints() and have it validate all remaining
		 * checks.
		 */
		if (resultRelationDesc->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate);

#ifdef __OPENTENBASE__
		if (IS_PGXC_COORDINATOR && mt->remote_plans && !merge_ignore)
		{
			bool succeed = false;

			succeed = ExecRemoteDML(mtstate, tupleid, oldtuple,
						slot, planSlot, estate, epqstate,
						canSetTag, NULL, NULL,
						resultRelInfo, remoterel_index);

			if (!succeed)
				return NULL;
		}
		else
		{
			{
#endif
		/*
		 * replace the heap tuple
		 *
		 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
		 * that the row to be updated is visible to that snapshot, and throw a
		 * can't-serialize error if not. This is a special-case behavior
		 * needed for referential integrity updates in transaction-snapshot
		 * mode transactions.
		 */
		if (!merge_ignore)
		{
			result = heap_update(resultRelationDesc, tupleid, tuple,
			                     estate->es_output_cid,
			                     estate->es_crosscheck_snapshot,
			                     true /* wait for commit */ ,
			                     &hufd, &lockmode,
			                     mtstate->mt_gindex_origin_tuple);
		}
		else
		{
			result = HeapTupleMayBeUpdated;
			tuple->t_self = *tupleid;
		}
		switch (result)
		{
			case HeapTupleSelfUpdated:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  The former case is possible in a join UPDATE
				 * where multiple tuples join to the same target tuple. This
				 * is pretty questionable, but Postgres has always allowed it:
				 * we just execute the first update action and ignore
				 * additional update attempts.
				 *
				 * The latter case arises if the tuple is modified by a
				 * command in a BEFORE trigger, or perhaps by a command in a
				 * volatile function used in the query.  In such situations we
				 * should not ignore the update, but it is equally unsafe to
				 * proceed.  We don't want to discard the original UPDATE
				 * while keeping the triggered actions based on it; and we
				 * have no principled way to merge this update with the
				 * previous ones.  So throwing an error is the only safe
				 * course.
				 *
				 * If a trigger actually intends this type of interaction, it
				 * can re-execute the UPDATE (assuming it can figure out how)
				 * and then return NULL to cancel the outer update.
				 */
				if (hufd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be updated was already modified by an operation "
							        "triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger "
							         "to propagate changes to other rows.")));
				if (mtstate->operation == CMD_MERGE)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CARDINALITY_VIOLATION),
							 errmsg("MERGE command cannot affect row a "
							        "second time"),
							 errhint("Ensure that not more than one source "
							         "row matches any one target row.")));
				}

				/* Else, already updated by self; nothing to do */
				return NULL;

			case HeapTupleMayBeUpdated:
				break;

			case HeapTupleUpdated:
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				if (ItemPointerIndicatesMovedPartitions(&hufd.ctid))
						ereport(ERROR,
								(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
										errmsg("tuple to be updated was already moved to another partition due to concurrent update")));
				if (!ItemPointerEquals(tupleid, &hufd.ctid))
				{
					TupleTableSlot *epqslot;
					TupleTableSlot *oldSlot;

					epqslot = EvalPlanQual(estate,
										   epqstate,
										   resultRelationDesc,
										   resultRelInfo->ri_RangeTableIndex,
										   lockmode,
										   &hufd.ctid,
										   hufd.xmax);
					if (!TupIsNull(epqslot))
					{
						*tupleid = hufd.ctid;
						/*
						 * For merge into query, mergeMatchedAction's targetlist is not same as junk filter's
						 * targetlist. Here, epqslot is a plan slot, target table needs slot to be projected
						 * from plan slot.
						 */
						if (mtstate->operation == CMD_MERGE)
						{
							return epqslot;
						}
						else
						{
							/* Fetch the most recent version of old tuple. */
							Relation	relation = resultRelInfo->ri_RelationDesc;
							HeapTupleData updtuple;
							Buffer		updbuffer;

							updtuple.t_self = *tupleid;
							if (!heap_fetch(relation, SnapshotAny,
											&updtuple, &updbuffer, false, NULL))
								elog(ERROR, "failed to fetch tuple being updated");

							oldSlot = resultRelInfo->ri_oldTupleSlot;
							ExecStoreBufferHeapTuple(&updtuple, oldSlot, updbuffer);
							ReleaseBuffer(updbuffer);
							slot = ExecGetUpdateNewTuple(resultRelInfo,
														 epqslot, oldSlot);

							tuple = ExecFetchSlotHeapTuple(slot, true, NULL);
							if (hasshard)
							{
								slot_getallattrs(slot);
								heap_tuple_set_shardid(tuple, slot, discolnums, ndiscols,
									RelationGetRelid(resultRelationDesc));
							}
							goto lreplace;
						}
					}
				}
				/* tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized heap_update status: %u", result);
				return NULL;
		}

		/*
		 * Note: instead of having to update the old index tuples associated
		 * with the heap tuple, all we do is form and insert new index tuples.
		 * This is because UPDATEs are actually DELETEs and INSERTs, and index
		 * tuple deletion is done later by VACUUM (see notes in ExecDelete).
		 * All we do here is insert new index tuples.  -cim 9/27/89
		 */

		/*
		 * insert index entries for tuple
		 *
		 * Note: heap_update returns the tid (location) of the new tuple in
		 * the t_self field.
		 *
		 * If it's a HOT update, we mustn't insert new index entries.
		 */
		if (resultRelInfo->ri_NumIndices > 0 && !HeapTupleIsHeapOnly(tuple) && !merge_ignore)
			recheckIndexes = ExecInsertIndexTuples(slot, &(tuple->t_self),
												   estate, false, NULL, NIL);
#ifdef __OPENTENBASE__
			}
		}
#endif
	}

	if (canSetTag && set_processed)
		(estate->es_processed)++;

	/* AFTER ROW UPDATE Triggers */
    if (mtstate->operation != CMD_MERGE || (IS_CENTRALIZED_DATANODE && ORA_MODE))
	{
		TransitionCaptureState *transition_capture = 
											(mtstate->operation == CMD_INSERT ?
											mtstate->mt_oc_transition_capture :
											mtstate->mt_transition_capture);
		/* 
		 * If this update comes from an upsert, because the data required for
		 * the update is recorded in mt_oc_transition_capture, so use
		 * mt_transition_capture instead of mt_oc_transition_capture
		 */
		if (IS_PGXC_COORDINATOR && mtstate->mt_conflproj && mtstate->operation == CMD_UPDATE)
		{
			transition_capture = mtstate->mt_oc_transition_capture;
		}

		ExecARUpdateTriggers(estate, resultRelInfo, tupleid, oldtuple, tuple,
						 recheckIndexes, transition_capture);
	}

	list_free(recheckIndexes);

	if (relation_has_cross_node_index(resultRelationDesc))
	{
		updateGindexInsertSlotInfo(mtstate,
									slot->tts_tupleDescriptor,
									planSlot->tts_tupleDescriptor,
									resultRelInfo);
	}

	/*
	 * Check any WITH CHECK OPTION constraints from parent views.  We are
	 * required to do this after testing all constraints and uniqueness
	 * violations per the SQL spec, so we do it after actually updating the
	 * record in the heap and all indexes.
	 *
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);

	/* Process RETURNING if present */
	if (resultRelInfo->ri_projectReturning)
		return ExecProcessReturning(resultRelInfo, slot, planSlot);

	return NULL;
}

/*
 * ExecOnConflictUpdate --- execute UPDATE of INSERT ON CONFLICT DO UPDATE
 *
 * Try to lock tuple for update as part of speculative insertion.  If
 * a qual originating from ON CONFLICT DO UPDATE is satisfied, update
 * (but still lock row, even though it may not satisfy estate's
 * snapshot).
 *
 * Returns true if if we're done (with or without an update), or false if
 * the caller must retry the INSERT from scratch.
 */
static bool
ExecOnConflictUpdate(ModifyTableState *mtstate,
					 ResultRelInfo *resultRelInfo,
					 ItemPointer conflictTid,
					 TupleTableSlot *planSlot,
					 TupleTableSlot *excludedSlot,
					 EState *estate,
					 bool canSetTag,
					 TupleTableSlot **returning)
{
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	Relation	relation = resultRelInfo->ri_RelationDesc;
	ExprState  *onConflictSetWhere = resultRelInfo->ri_onConflict->oc_WhereClause;
	HeapTupleData tuple;
	HeapUpdateFailureData hufd;
	LockTupleMode lockmode;
	HTSU_Result test;
	Buffer		buffer;

	/* Determine lock mode to use */
	lockmode = ExecUpdateLockMode(estate, resultRelInfo);

	/*
	 * Lock tuple for update.  Don't follow updates when tuple cannot be
	 * locked without doing so.  A row locking conflict here means our
	 * previous conclusion that the tuple is conclusively committed is not
	 * true anymore.
	 */
	tuple.t_self = *conflictTid;
	test = heap_lock_tuple(relation, &tuple, estate->es_output_cid,
						   lockmode, LockWaitBlock, false, &buffer,
						   &hufd);
	switch (test)
	{
		case HeapTupleMayBeUpdated:
			/* success! */
			break;

		case HeapTupleInvisible:

			/*
			 * This can occur when a just inserted tuple is updated again in
			 * the same command. E.g. because multiple rows with the same
			 * conflicting key values are inserted.
			 *
			 * This is somewhat similar to the ExecUpdate()
			 * HeapTupleSelfUpdated case.  We do not want to proceed because
			 * it would lead to the same row being updated a second time in
			 * some unspecified order, and in contrast to plain UPDATEs
			 * there's no historical behavior to break.
			 *
			 * It is the user's responsibility to prevent this situation from
			 * occurring.  These problems are why SQL-2003 similarly specifies
			 * that for SQL MERGE, an exception must be raised in the event of
			 * an attempt to update the same row twice.
			 */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple.t_data)))
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("ON CONFLICT DO UPDATE command cannot affect row a second time"),
						 errhint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")));

			/* This shouldn't happen */
			elog(ERROR, "attempted to lock invisible tuple");

		case HeapTupleSelfUpdated:

			/*
			 * This state should never be reached. As a dirty snapshot is used
			 * to find conflicting tuples, speculative insertion wouldn't have
			 * seen this row to conflict with.
			 */
			elog(ERROR, "unexpected self-updated tuple");

		case HeapTupleUpdated:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));

			/*
			 * As long as we don't support an UPDATE of INSERT ON CONFLICT for
			 * a partitioned table we shouldn't reach to a case where tuple to
			 * be lock is moved to another partition due to concurrent update
			 * of the partition key.
			 */
			Assert(!ItemPointerIndicatesMovedPartitions(&hufd.ctid));
			/*
			 * Tell caller to try again from the very start.
			 *
			 * It does not make sense to use the usual EvalPlanQual() style
			 * loop here, as the new version of the row might not conflict
			 * anymore, or the conflicting tuple has actually been deleted.
			 */
			ReleaseBuffer(buffer);
			return false;

		default:
			elog(ERROR, "unrecognized heap_lock_tuple status: %u", test);
	}

	/*
	 * Success, the tuple is locked.
	 *
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Verify that the tuple is visible to our MVCC snapshot if the current
	 * isolation level mandates that.
	 *
	 * It's not sufficient to rely on the check within ExecUpdate() as e.g.
	 * CONFLICT ... WHERE clause may prevent us from reaching that.
	 *
	 * This means we only ever continue when a new command in the current
	 * transaction could see the row, even though in READ COMMITTED mode the
	 * tuple will not be visible according to the current statement's
	 * snapshot.  This is in line with the way UPDATE deals with newer tuple
	 * versions.
	 */
	ExecCheckHeapTupleVisible(estate, &tuple, buffer);

	/* Store target's existing tuple in the state's dedicated slot */
	ExecStoreBufferHeapTuple(&tuple, mtstate->mt_existing, buffer);

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject.  The EXCLUDED tuple is installed in ecxt_innertuple, while
	 * the target's existing tuple is installed in the scantuple.  EXCLUDED
	 * has been made to reference INNER_VAR in setrefs.c, but there is no
	 * other redirection.
	 */
	econtext->ecxt_scantuple = mtstate->mt_existing;
	econtext->ecxt_innertuple = excludedSlot;
	econtext->ecxt_outertuple = NULL;

	if (!ExecQual(onConflictSetWhere, econtext))
	{
		ReleaseBuffer(buffer);
		InstrCountFiltered1(&mtstate->ps, 1);
		return true;			/* done with the tuple */
	}

	if (resultRelInfo->ri_WithCheckOptions != NIL)
	{
		/*
		 * Check target's existing tuple against UPDATE-applicable USING
		 * security barrier quals (if any), enforced here as RLS checks/WCOs.
		 *
		 * The rewriter creates UPDATE RLS checks/WCOs for UPDATE security
		 * quals, and stores them as WCOs of "kind" WCO_RLS_CONFLICT_CHECK,
		 * but that's almost the extent of its special handling for ON
		 * CONFLICT DO UPDATE.
		 *
		 * The rewriter will also have associated UPDATE applicable straight
		 * RLS checks/WCOs for the benefit of the ExecUpdate() call that
		 * follows.  INSERTs and UPDATEs naturally have mutually exclusive WCO
		 * kinds, so there is no danger of spurious over-enforcement in the
		 * INSERT or UPDATE path.
		 */
		ExecWithCheckOptions(WCO_RLS_CONFLICT_CHECK, resultRelInfo,
							 mtstate->mt_existing,
							 mtstate->ps.state);
	}

	/* Project the new tuple version */
	ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo);

	/*
	 * Note that it is possible that the target tuple has been modified in
	 * this session, after the above heap_lock_tuple. We choose to not error
	 * out in that case, in line with ExecUpdate's treatment of similar cases.
	 * This can happen if an UPDATE is triggered from within ExecQual(),
	 * ExecWithCheckOptions() or ExecProject() above, e.g. by selecting from a
	 * wCTE in the ON CONFLICT's SET.
	 */

	/* Execute UPDATE with projection */
	*returning = ExecUpdate(mtstate, &tuple.t_self, NULL,
							mtstate->mt_conflproj, planSlot,
							&mtstate->mt_epqstate, mtstate->ps.state,
							canSetTag, false);

	ReleaseBuffer(buffer);
	return true;
}


/*
 * Process BEFORE EACH STATEMENT triggers
 */
static void
fireBSTriggers(ModifyTableState *node)
{
	ModifyTable *plan = (ModifyTable *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->resultRelInfo;

	/*
	 * If the node modifies a partitioned table, we must fire its triggers.
	 * Note that in that case, node->resultRelInfo points to the first leaf
	 * partition, not the root table.
	 */
	if (node->rootResultRelInfo != NULL)
		resultRelInfo = node->rootResultRelInfo;

	switch (node->operation)
	{
		case CMD_INSERT:
			ExecBSInsertTriggers(node->ps.state, resultRelInfo);
			if (plan->onConflictAction == ONCONFLICT_UPDATE)
				ExecBSUpdateTriggers(node->ps.state,
									 resultRelInfo);
			break;
		case CMD_UPDATE:
			ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
			break;
		case CMD_DELETE:
			ExecBSDeleteTriggers(node->ps.state, resultRelInfo);
			break;
        case CMD_MERGE:
            break;
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}

/*
 * Return the target rel ResultRelInfo.
 *
 * This relation is the same as :
 * - the relation for which we will fire AFTER STATEMENT triggers.
 * - the relation into whose tuple format all captured transition tuples must
 *   be converted.
 * - the root partitioned table.
 */
static ResultRelInfo *
getTargetResultRelInfo(ModifyTableState *node)
{
	/*
	 * Note that if the node modifies a partitioned table, node->resultRelInfo
	 * points to the first leaf partition, not the root table.
	 */
	if (node->rootResultRelInfo != NULL)
		return node->rootResultRelInfo;
	else
		return node->resultRelInfo;
}

/*
 * Process AFTER EACH STATEMENT triggers
 */
static void
fireASTriggers(ModifyTableState *node)
{
	ModifyTable *plan = (ModifyTable *) node->ps.plan;
	ResultRelInfo *resultRelInfo = getTargetResultRelInfo(node);

	switch (node->operation)
	{
		case CMD_INSERT:
			if (plan->onConflictAction == ONCONFLICT_UPDATE)
				ExecASUpdateTriggers(node->ps.state,
									 resultRelInfo,
									 node->mt_oc_transition_capture);
			ExecASInsertTriggers(node->ps.state, resultRelInfo,
								 node->mt_transition_capture);
			break;
		case CMD_UPDATE:
			ExecASUpdateTriggers(node->ps.state, resultRelInfo,
								 node->mt_transition_capture);
			break;
		case CMD_DELETE:
			ExecASDeleteTriggers(node->ps.state, resultRelInfo,
								 node->mt_transition_capture);
			break;
        case CMD_MERGE:
            break;
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}

/*
 * Set up the state needed for collecting transition tuples for AFTER
 * triggers.
 */
static void
ExecSetupTransitionCaptureState(ModifyTableState *mtstate, EState *estate)
{
	ResultRelInfo *targetRelInfo = getTargetResultRelInfo(mtstate);
	ModifyTable *plan = (ModifyTable *) mtstate->ps.plan;

	/* Check for transition tables on the directly targeted relation. */
	mtstate->mt_transition_capture =
		MakeTransitionCaptureState(targetRelInfo->ri_TrigDesc,
	                               RelationGetRelid(targetRelInfo->ri_RelationDesc),
	                               mtstate->operation);

	if (mtstate->operation == CMD_INSERT && plan->onConflictAction == ONCONFLICT_UPDATE)
		mtstate->mt_oc_transition_capture =
			MakeTransitionCaptureState(targetRelInfo->ri_TrigDesc,
		                               RelationGetRelid(targetRelInfo->ri_RelationDesc),
		                               CMD_UPDATE);

	/*
	 * If we found that we need to collect transition tuples then we may also
	 * need tuple conversion maps for any children that have TupleDescs that
	 * aren't compatible with the tuplestores. (We can share these maps
	 * between the regular and ON CONFLICT cases.)
	 */
	if (mtstate->mt_transition_capture != NULL ||
	    mtstate->mt_oc_transition_capture != NULL)
	{
		ExecSetupChildParentMapForTcs(mtstate);

		/*
         * Install the conversion map for the first plan for UPDATE and DELETE
         * operations.  It will be advanced each time we switch to the next
         * plan.  (INSERT operations set it every time, so we need not update
         * mtstate->mt_oc_transition_capture here.)
		 */
	       if (mtstate->mt_transition_capture && mtstate->operation != CMD_INSERT)
	           mtstate->mt_transition_capture->tcs_map =
	               tupconv_map_for_subplan(mtstate, 0);
	}
}

/*
 * ExecPrepareTupleRouting --- prepare for routing one tuple
 *
 * Determine the partition in which the tuple in slot is to be inserted,
 * and modify mtstate and estate to prepare for it.
 *
 * Caller must revert the estate changes after executing the insertion!
 * In mtstate, transition capture changes may also need to be reverted.
 *
 * Returns a slot holding the tuple of the partition rowtype.
 */
TupleTableSlot *
ExecPrepareTupleRouting(ModifyTableState *mtstate,
						EState *estate,
						PartitionTupleRouting *proute,
						ResultRelInfo *targetRelInfo,
						TupleTableSlot *slot,
						Oid oldPartitionOid)
{
	int			partidx = 0;
	ResultRelInfo *partrel;
	HeapTuple	tuple;
	TupleConversionMap *map;

	if (oldPartitionOid != InvalidOid)
	{
		int i = 0;
		partrel = ExecInitPartitionInfo(mtstate, targetRelInfo,
										proute, estate,
										partidx, oldPartitionOid);
		for (i = 0; i < proute->num_partitions; ++i)
		{
			if (oldPartitionOid == proute->partition_oids[i])
			{
				partidx = i;
				break;
			}
		}
	}
	else
	{
		/*
		 * Determine the target partition.  If ExecFindPartition does not find
		 * a partition after all, it doesn't return here; otherwise, the returned
		 * value is to be used as an index into the arrays for the ResultRelInfo
		 * and TupleConversionMap for the partition.
		 */
		partidx = ExecFindPartition(targetRelInfo,
									proute->partition_dispatch_info,
									slot,
									estate);
		Assert(partidx >= 0 && partidx < proute->num_partitions);

		/*
		 * Get the ResultRelInfo corresponding to the selected partition; if not
		 * yet there, initialize it.
		 */
		partrel = proute->partitions[partidx];
		if (partrel == NULL)
			partrel = ExecInitPartitionInfo(mtstate, targetRelInfo,
											proute, estate,
											partidx, oldPartitionOid);
	}

	/* We do not yet have a way to insert into a foreign partition */
	if (partrel->ri_FdwRoutine)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route inserted tuples to a foreign table")));

	/*
	 * Make it look like we are inserting into the partition.
	 */
	estate->es_result_relation_info = partrel;

	/* Get the heap tuple out of the given slot. */
	tuple = ExecFetchSlotHeapTuple(slot, true, NULL);

	/*
	 * If we're capturing transition tuples, we might need to convert from the
	 * partition rowtype to parent rowtype.
	 */
	if (mtstate->mt_transition_capture != NULL)
	{
		if (partrel->ri_TrigDesc &&
			partrel->ri_TrigDesc->trig_insert_before_row)
		{
			/*
			 * If there are any BEFORE triggers on the partition, we'll have
			 * to be ready to convert their result back to tuplestore format.
			 */
			mtstate->mt_transition_capture->tcs_original_insert_tuple = NULL;
			mtstate->mt_transition_capture->tcs_map =
				TupConvMapForLeaf(proute, targetRelInfo, partidx);
		}
		else
		{
			/*
			 * Otherwise, just remember the original unconverted tuple, to
			 * avoid a needless round trip conversion.
			 */
			mtstate->mt_transition_capture->tcs_original_insert_tuple = tuple;
			mtstate->mt_transition_capture->tcs_map = NULL;
		}
	}

	/*
	 * Convert the tuple, if necessary.
	 */
	map = proute->parent_child_tupconv_maps[partidx];
	if (map != NULL)
	{
		TupleTableSlot *new_slot;

		Assert(proute->partition_tuple_slots != NULL &&
			   proute->partition_tuple_slots[partidx] != NULL);
		new_slot = proute->partition_tuple_slots[partidx];
		slot = execute_attr_map_slot(map->attrMap, slot, new_slot, partrel->ri_RelationDesc);
	}

	return slot;
}

/*
 * Initialize the child-to-root tuple conversion map array for UPDATE subplans.
 *
 * This map array is required to convert the tuple from the subplan result rel
 * to the target table descriptor. This requirement arises for two independent
 * scenarios:
 * 1. For update-tuple-routing.
 * 2. For capturing tuples in transition tables.
 */
void
ExecSetupChildParentMapForSubplan(ModifyTableState *mtstate)
{
   ResultRelInfo *targetRelInfo = getTargetResultRelInfo(mtstate);
   ResultRelInfo *resultRelInfos = mtstate->resultRelInfo;
   TupleDesc   outdesc;
   int         numResultRelInfos = list_length(((ModifyTable *) mtstate->ps.plan)->resultRelations);
   int         i;

   /*
    * First check if there is already a per-subplan array allocated. Even if
    * there is already a per-leaf map array, we won't require a per-subplan
    * one, since we will use the subplan offset array to convert the subplan
    * index to per-leaf index.
    */
   if (mtstate->mt_per_subplan_tupconv_maps ||
       (mtstate->mt_partition_tuple_routing &&
        mtstate->mt_partition_tuple_routing->child_parent_tupconv_maps))
       return;

   /*
    * Build array of conversion maps from each child's TupleDesc to the one
    * used in the target relation.  The map pointers may be NULL when no
    * conversion is necessary, which is hopefully a common case.
    */

   /* Get tuple descriptor of the target rel. */
   outdesc = RelationGetDescr(targetRelInfo->ri_RelationDesc);

   mtstate->mt_per_subplan_tupconv_maps = (TupleConversionMap **)
       palloc(sizeof(TupleConversionMap *) * numResultRelInfos);

   for (i = 0; i < numResultRelInfos; ++i)
   {
       mtstate->mt_per_subplan_tupconv_maps[i] =
           convert_tuples_by_name(RelationGetDescr(resultRelInfos[i].ri_RelationDesc),
                                  outdesc,
                                  gettext_noop("could not convert row type"));
   }
}

/*
 * Initialize the child-to-root tuple conversion map array required for
 * capturing transition tuples.
 *
 * The map array can be indexed either by subplan index or by leaf-partition
 * index.  For transition tables, we need a subplan-indexed access to the map,
 * and where tuple-routing is present, we also require a leaf-indexed access.
 */
static void
ExecSetupChildParentMapForTcs(ModifyTableState *mtstate)
{
   PartitionTupleRouting *proute = mtstate->mt_partition_tuple_routing;

   /*
    * If partition tuple routing is set up, we will require partition-indexed
    * access. In that case, create the map array indexed by partition; we
    * will still be able to access the maps using a subplan index by
    * converting the subplan index to a partition index using
    * subplan_partition_offsets. If tuple routing is not set up, it means we
    * don't require partition-indexed access. In that case, create just a
    * subplan-indexed map.
    */
   if (proute)
   {
		/*
		 * If a partition-indexed map array is to be created, the subplan map
         * array has to be NULL.  If the subplan map array is already created,
         * we won't be able to access the map using a partition index.
		 */
       Assert(mtstate->mt_per_subplan_tupconv_maps == NULL);

       ExecSetupChildParentMapForLeaf(proute);
   }
   else
       ExecSetupChildParentMapForSubplan(mtstate);
}

/*
 * For a given subplan index, get the tuple conversion map.
 */
static TupleConversionMap *
tupconv_map_for_subplan(ModifyTableState *mtstate, int whichplan)
{
	/*
	 * If a partition-index tuple conversion map array is allocated, we need
	 * to first get the index into the partition array. Exactly *one* of the
	 * two arrays is allocated. This is because if there is a partition array
	 * required, we don't require subplan-indexed array since we can translate
	 * subplan index into partition index. And, we create a subplan-indexed
	 * array *only* if partition-indexed array is not required.
	 */
	if (mtstate->mt_per_subplan_tupconv_maps == NULL)
	{
		int         leaf_index;
		PartitionTupleRouting *proute = mtstate->mt_partition_tuple_routing;

		/*
		 * If subplan-indexed array is NULL, things should have been arranged
		 * to convert the subplan index to partition index.
		 */
		Assert(proute && proute->subplan_partition_offsets != NULL &&
			   whichplan < proute->num_subplan_partition_offsets);

		leaf_index = proute->subplan_partition_offsets[whichplan];

		return TupConvMapForLeaf(proute, getTargetResultRelInfo(mtstate),
								 leaf_index);
	}
	else
	{
		Assert(whichplan >= 0 && whichplan < mtstate->mt_nrels);
		return mtstate->mt_per_subplan_tupconv_maps[whichplan];
	}
}

/* ----------------------------------------------------------------
 *	   ExecModifyTable
 *
 *		Perform table modifications as required, and return RETURNING results
 *		if needed.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecModifyTable(PlanState *pstate)
{
	ModifyTableState *node = castNode(ModifyTableState, pstate);
	PartitionTupleRouting *proute = node->mt_partition_tuple_routing;
	EState	   *estate = node->ps.state;
	CmdType		operation = node->operation;
	ResultRelInfo *saved_resultRelInfo;
	ResultRelInfo *resultRelInfo;
	PlanState  *subplanstate;
	TupleTableSlot *slot;
	TupleTableSlot *planSlot;
	TupleTableSlot *oldSlot;
	ItemPointer tupleid;
	ItemPointerData tuple_ctid;
	HeapTupleData oldtupdata;
	HeapTuple	oldtuple;
#ifdef __OPENTENBASE__
	ModifyTable *mt = (ModifyTable *) node->ps.plan;
	int64		insert_tuple_count = 0;
#endif
#ifdef __AUDIT_FGA__
	ListCell      *item = NULL;
	ExprContext *econtext = NULL;
	TupleTableSlot *audit_fga_slot = NULL;
	char *cmd_type = NULL;
	TupleDesc audit_fga_slot_tupdesc;
	TupleTableSlot *old_ecxt_scantuple = NULL;
#endif
#ifdef _MLS_
	int oldtag = CLS_CMD_UNKNOWN;
#endif

	CHECK_FOR_INTERRUPTS();

	/*
	 * This should NOT get called during EvalPlanQual; we should have passed a
	 * subplan tree to EvalPlanQual, instead.  Use a runtime test not just
	 * Assert because this condition is easy to miss in testing.  (Note:
	 * although ModifyTable should not get executed within an EvalPlanQual
	 * operation, we do have to allow it to be initialized and shut down in
	 * case it is within a CTE subplan.  Hence this test must be here, not in
	 * ExecInitModifyTable.)
	 */
	if (estate->es_epqTuple != NULL)
		elog(ERROR, "ModifyTable should not be called during EvalPlanQual");

	/*
	 * If we've already completed processing, don't try to do more.  We need
	 * this test because ExecPostprocessPlan might call us an extra time, and
	 * our subplan's nodes aren't necessarily robust against being called
	 * extra times.
	 */
	if (node->mt_done)
		return NULL;

	/*
	 * On first call, fire BEFORE STATEMENT triggers before proceeding.
	 */
	if (node->fireBSTriggers)
	{
		fireBSTriggers(node);
		node->fireBSTriggers = false;
	}

	/* Preload local variables */
	resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
	subplanstate = outerPlanState(node);

	/*
	 * es_result_relation_info must point to the currently active result
	 * relation while we are within this ModifyTable node.  Even though
	 * ModifyTable nodes can't be nested statically, they can be nested
	 * dynamically (since our subplan could include a reference to a modifying
	 * CTE).  So we have to save and restore the caller's value.
	 */
	saved_resultRelInfo = estate->es_result_relation_info;

	estate->es_result_relation_info = resultRelInfo;

#ifdef _MLS_
    oldtag = mls_command_tag_switch_to(CLS_CMD_WRITE);
#endif

	/*
	 * Fetch rows from subplan, and execute the required table modification
	 * for each row.
	 */
	for (;;)
	{
		/*
		 * Reset the per-output-tuple exprcontext.  This is needed because
		 * triggers expect to use that context as workspace.  It's a bit ugly
		 * to do this below the top level of the plan, however.  We might need
		 * to rethink this later.
		 */
		ResetPerTupleExprContext(estate);
		if (!node->mt_gindex_origin_slot_used)
		{
			node->mt_gindex_origin_slot_used = true;
			return node->mt_gindex_result_slot;
		}
			
		planSlot = ExecProcNode(subplanstate);

		/* No more tuples to process? */
		if (TupIsNull(planSlot))
			break;

		/*
		 * When there are multiple result relations, each tuple contains a
		 * junk column that gives the OID of the rel from which it came.
		 * Extract it and select the correct result relation.
		 */
		if (AttributeNumberIsValid(node->mt_resultOidAttno))
		{
			Datum		datum;
			bool		isNull;
			Oid			resultoid;

			datum = ExecGetJunkAttribute(planSlot, node->mt_resultOidAttno,
										 &isNull);
			if (isNull)
			{
				/*
				 * For commands other than MERGE, any tuples having InvalidOid
				 * for tableoid are errors.  For MERGE, we may need to handle
				 * them as WHEN NOT MATCHED clauses if any, so do that.
				 *
				 * Note that we use the node's toplevel resultRelInfo, not any
				 * specific partition's.
				 */
				if (operation == CMD_MERGE)
				{
					EvalPlanQualSetSlot(&node->mt_epqstate, planSlot);

					slot = ExecMerge(node, estate, planSlot, node->resultRelInfo->ri_junkFilter, node->resultRelInfo);
					if (!TupIsNull(slot))
						return slot;
					continue;	/* no RETURNING support yet */
				}
				elog(ERROR, "tableoid is NULL");
			}
			resultoid = DatumGetObjectId(datum);

			/* If it's not the same as last time, we need to locate the rel */
			if (resultoid != node->mt_lastResultOid)
			{
				if (node->mt_resultOidHash)
				{
					/* Use the pre-built hash table to locate the rel */
					MTTargetRelLookup *mtlookup;

					mtlookup = (MTTargetRelLookup *)
						hash_search(node->mt_resultOidHash, &resultoid,
									HASH_FIND, NULL);
					if (!mtlookup)
						elog(ERROR, "incorrect result rel OID %u", resultoid);
					node->mt_lastResultOid = resultoid;
					node->mt_lastResultIndex = mtlookup->relationIndex;
					resultRelInfo = node->resultRelInfo + mtlookup->relationIndex;
				}
				else
				{
					/* With few target rels, just do a simple search */
					int			ndx;

					for (ndx = 0; ndx < node->mt_nrels; ndx++)
					{
						resultRelInfo = node->resultRelInfo + ndx;
						if (RelationGetRelid(resultRelInfo->ri_RelationDesc) == resultoid)
							break;
					}
					if (ndx >= node->mt_nrels)
						elog(ERROR, "incorrect result rel OID %u", resultoid);
					node->mt_lastResultOid = resultoid;
					node->mt_lastResultIndex = ndx;
				}
				estate->es_result_relation_info = resultRelInfo;
			}
		}

		/*
		 * If resultRelInfo->ri_usesFdwDirectModify is true, all we need to do
		 * here is compute the RETURNING expressions.
		 */
		if (resultRelInfo->ri_usesFdwDirectModify)
		{
			Assert(resultRelInfo->ri_projectReturning);

			/*
			 * A scan slot containing the data that was actually inserted,
			 * updated or deleted has already been made available to
			 * ExecProcessReturning by IterateDirectModify, so no need to
			 * provide it here.
			 */
			slot = ExecProcessReturning(resultRelInfo, NULL, planSlot);

			estate->es_result_relation_info = saved_resultRelInfo;
#ifdef _MLS_            
            mls_command_tag_switch_to(oldtag);
#endif
            return slot;
		}

		EvalPlanQualSetSlot(&node->mt_epqstate, planSlot);
		slot = planSlot;

		if (operation == CMD_MERGE)
		{
			slot = ExecMerge(node, estate, slot, resultRelInfo->ri_junkFilter, resultRelInfo);
			if (!TupIsNull(slot))
				return slot;
			continue;
		}

		tupleid = NULL;
		oldtuple = NULL;

		/*
		 * For UPDATE/DELETE, fetch the row identity info for the tuple to be
		 * updated/deleted.  For a heap relation, that's a TID; otherwise we
		 * may have a wholerow junk attr that carries the old tuple in toto.
		 * Keep this in step with the part of ExecInitModifyTable that sets up
		 * ri_RowIdAttNo.
		 */
		if (operation == CMD_UPDATE || operation == CMD_DELETE)
		{
			char		relkind;
			Datum		datum;
			bool		isNull;

			relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
			if (relkind == RELKIND_RELATION ||
				relkind == RELKIND_MATVIEW ||
				relkind == RELKIND_PARTITIONED_TABLE)
			{
				/* ri_RowIdAttNo refers to a ctid attribute */
				Assert(AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo));
				datum = ExecGetJunkAttribute(slot,
											 resultRelInfo->ri_RowIdAttNo,
											 &isNull);
				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "ctid is NULL");

				tupleid = (ItemPointer) DatumGetPointer(datum);
				tuple_ctid = *tupleid;	/* be sure we don't free ctid!! */
				tupleid = &tuple_ctid;
#ifdef __OPENTENBASE__
				/*
				 * for update/delete with unshippable triggers, we need to get
				 * the oldtuple for triggers.
				 */
				if (IS_PGXC_COORDINATOR && mt->remote_plans)
				{
					datum = ExecGetJunkAttribute(slot,
												 resultRelInfo->ri_xc_wholerow,
												 &isNull);
					/* shouldn't ever get a null result... */
					if (isNull)
						elog(ERROR, "wholerow is NULL");

					oldtupdata.t_data = DatumGetHeapTupleHeader(datum);
					oldtupdata.t_len =
						HeapTupleHeaderGetDatumLength(oldtupdata.t_data);
					ItemPointerSetInvalid(&(oldtupdata.t_self));
					/* Historically, view triggers see invalid t_tableOid. */
					oldtupdata.t_tableOid =
						(relkind == RELKIND_VIEW) ? InvalidOid :
						RelationGetRelid(resultRelInfo->ri_RelationDesc);

					oldtuple = &oldtupdata;
				}
#endif
			}

			/*
			 * Use the wholerow attribute, when available, to reconstruct the
			 * old relation tuple.  The old tuple serves one or both of two
			 * purposes: 1) it serves as the OLD tuple for row triggers, 2) it
			 * provides values for any unchanged columns for the NEW tuple of
			 * an UPDATE, because the subplan does not produce all the columns
			 * of the target table.
			 *
			 * Note that the wholerow attribute does not carry system columns,
			 * so foreign table triggers miss seeing those, except that we
			 * know enough here to set t_tableOid.  Quite separately from
			 * this, the FDW may fetch its own junk attrs to identify the row.
			 *
			 * Other relevant relkinds, currently limited to views, always
			 * have a wholerow attribute.
			 */
			else if (AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
			{
				datum = ExecGetJunkAttribute(slot,
											 resultRelInfo->ri_RowIdAttNo,
											 &isNull);
				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "wholerow is NULL");

				oldtupdata.t_data = DatumGetHeapTupleHeader(datum);
				oldtupdata.t_len =
					HeapTupleHeaderGetDatumLength(oldtupdata.t_data);
				ItemPointerSetInvalid(&(oldtupdata.t_self));
				/* Historically, view triggers see invalid t_tableOid. */
				oldtupdata.t_tableOid =
					(relkind == RELKIND_VIEW) ? InvalidOid :
					RelationGetRelid(resultRelInfo->ri_RelationDesc);

				oldtuple = &oldtupdata;
			}
			else
			{
				/* Only foreign tables are allowed to omit a row-ID attr */
				Assert(relkind == RELKIND_FOREIGN_TABLE);
			}
		}

#ifdef __OPENTENBASE__
		if (IsA(subplanstate, TidScanState) && operation != CMD_INSERT)
		{
			int remote_node = castNode(TidScanState, subplanstate)->tss_remote_node;
			
			if (remote_node > -1 && remote_node != PGXCNodeId - 1)
			{
				SET_TTS_REMOTETID_SCAN(planSlot);
			}
		}
#endif
#ifdef __AUDIT_FGA__
        if (IsNormalProcessingMode() && IsUnderPostmaster && enable_fga)
        {
            foreach (item, node->ps.audit_fga_qual)
            {
                HeapTuple	result = NULL;
                audit_fga_policy_state *audit_fga_qual = (audit_fga_policy_state *) lfirst(item);
        
                if (operation == CMD_UPDATE || operation == CMD_DELETE)
                {
                    Page		page;
    		        ItemId		lp;
                    Buffer		buffer;
                    HeapTupleData tuple;
                    Relation	relation = estate->es_result_relation_info->ri_RelationDesc;

                    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tupleid));

                    /*
                    		 * Although we already know this tuple is valid, we must lock the
                    		 * buffer to ensure that no one has a buffer cleanup lock; otherwise
                    		 * they might move the tuple while we try to copy it.  But we can
                    		 * release the lock before actually doing the heap_copytuple call,
                    		 * since holding pin is sufficient to prevent anyone from getting a
                    		 * cleanup lock they don't already hold.
                    		 */
            		LockBuffer(buffer, BUFFER_LOCK_SHARE);

            		page = BufferGetPage(buffer);
            		lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tupleid));

            		Assert(ItemIdIsNormal(lp));

            		tuple.t_data = (HeapTupleHeader) PageGetItem(page, lp);
            		tuple.t_len = ItemIdGetLength(lp);
            		tuple.t_self = *tupleid;
            		tuple.t_tableOid = RelationGetRelid(relation);

                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                    result = heap_copytuple(&tuple);
    	            ReleaseBuffer(buffer);                
                }              

                audit_fga_slot_tupdesc = CreateTupleDescCopy(RelationGetDescr(estate->es_result_relation_info->ri_RelationDesc));
                audit_fga_slot = MakeSingleTupleTableSlot(audit_fga_slot_tupdesc);
            
                switch (operation)
                {
                    case CMD_INSERT:
            			cmd_type = "INSERT";
                        ExecCopySlot(audit_fga_slot, slot);
            			break;
            		case CMD_UPDATE:
            			cmd_type = "UPDATE";
                        ExecStoreHeapTuple(result, audit_fga_slot, false);
            			break;
            		case CMD_DELETE:
            			cmd_type = "DELETE";
                        ExecStoreHeapTuple(result, audit_fga_slot, false);
            			break;
            		default:
            			cmd_type = "???";
                        ExecCopySlot(audit_fga_slot, slot);
            			break;
                }
            
                //ExecCopySlot(audit_fga_slot, slot);
                
                econtext = GetPerTupleExprContext(estate);
                old_ecxt_scantuple = econtext->ecxt_scantuple;
                econtext->ecxt_scantuple = audit_fga_slot;
                            
                if (audit_fga_qual != NULL)
    		    {
                    if(ExecQual(audit_fga_qual->qual, econtext))
                    {
                        audit_fga_log_policy_info_2(audit_fga_qual, cmd_type);
                        
                        node->ps.audit_fga_qual = list_delete(node->ps.audit_fga_qual, audit_fga_qual);
                    }
            		else
                    {
                        elog(DEBUG1, "AUDIT_FGA: NOT EQAL");
                    }
                }

                econtext->ecxt_scantuple = old_ecxt_scantuple;
                ExecDropSingleTupleTableSlot(audit_fga_slot);
                if (audit_fga_slot_tupdesc)
                {
    			    FreeTupleDesc(audit_fga_slot_tupdesc);
                }
            }           
        }
#endif	        

		switch (operation)
		{
			case CMD_INSERT:
#ifdef _MLS_
                /* resultstate is 'insert values/insert select /copy from' action, need to change _cls values embedded. */
                if (IsA(subplanstate, ResultState) || IsA(subplanstate, RemoteFragmentState))
                {
                    oldtag = mls_command_tag_switch_to(CLS_CMD_ROW);
                }
#endif
				slot = ExecGetInsertNewTuple(resultRelInfo, planSlot);
                /* Prepare for tuple routing if needed. */
				if (proute)
                    slot = ExecPrepareTupleRouting(node, estate, proute,
                                                   resultRelInfo, slot, InvalidOid);
				slot = ExecInsert(node, slot, planSlot, estate, node->canSetTag);
				/* Revert ExecPrepareTupleRouting's state change. */
				if (proute)
                    estate->es_result_relation_info = resultRelInfo;
#ifdef _MLS_
                if (IsA(subplanstate, ResultState) || IsA(subplanstate, RemoteFragmentState))
                {
                    mls_command_tag_switch_to(oldtag);
                }
#endif                
				if(enable_distri_debug)
				{
					insert_tuple_count++;
				}
				break;
			case CMD_UPDATE:
#ifdef __OPENTENBASE_C__
				if (resultRelInfo->ri_projectNew)
				{
					HeapTupleData updtuple;
#endif
				/*
				 * Make the new tuple by combining plan's output tuple with
				 * the old tuple being updated.
				 */
				oldSlot = resultRelInfo->ri_oldTupleSlot;
				if (oldtuple != NULL)
				{
					/* Use the wholerow junk attr as the old tuple. */
					ExecStoreHeapTuple(oldtuple, oldSlot, false);
				}
				else
				{
					/* Fetch the most recent version of old tuple. */
					Relation	relation = resultRelInfo->ri_RelationDesc;
					Buffer		updbuffer;

					updtuple.t_self = *tupleid;
					if (!heap_fetch(relation, SnapshotAny,
									&updtuple, &updbuffer, false, NULL))
						elog(ERROR, "failed to fetch tuple being updated");

					ExecStoreBufferHeapTuple(&updtuple, oldSlot, updbuffer);
					ReleaseBuffer(updbuffer);
				}
				slot = ExecGetUpdateNewTuple(resultRelInfo, planSlot,
											 oldSlot);
#ifdef __OPENTENBASE_C__
				}
#endif
				/* Now apply the update. */
				slot = ExecUpdate(node, tupleid, oldtuple, slot, planSlot,
								  &node->mt_epqstate, estate, node->canSetTag, false);
				break;
			case CMD_DELETE:
#ifdef __OPENTENBASE__
				if (IS_PGXC_COORDINATOR && mt->remote_plans)
				{
					oldSlot = resultRelInfo->ri_oldTupleSlot;
					/* Use the wholerow junk attr as the old tuple. */
					ExecStoreHeapTuple(oldtuple, oldSlot, false);
					slot = oldSlot;
				}
				slot = ExecDelete(node, tupleid, oldtuple, slot, planSlot,
						         &node->mt_epqstate, estate,
								 NULL, true, node->canSetTag,
								 false /* changingPart */, NULL);
#else
				slot = ExecDelete(node, tupleid, oldtuple, planSlot,
						         &node->mt_epqstate, estate,
								 NULL, true, node->canSetTag,
								 false /* changingPart */, NULL);
#endif
				break;
			default:
				elog(ERROR, "unknown operation");
				break;
		}

		/*
		 * If we got a RETURNING result, return it to caller.  We'll continue
		 * the work on next call.
		 */
		if (slot)
		{
			estate->es_result_relation_info = saved_resultRelInfo;
#ifdef _MLS_            
            mls_command_tag_switch_to(oldtag);
#endif
			return slot;
		}
	}

	if(enable_distri_debug)
	{
		GlobalTimestamp start_ts;

		if(estate->es_snapshot)
		{
			start_ts = estate->es_snapshot->start_ts;
		}
		else
		{
			start_ts = InvalidGlobalTimestamp;
		}
		LogScanGTM(GetTopTransactionIdIfAny(), 
					   PGXCNodeName, 
					   start_ts,
					   GetCurrentTimestamp(),
					   GetCurrentTimestamp(),
					   INSERT_TUPLES,
					   RelationGetRelationName(estate->es_result_relation_info->ri_RelationDesc),
					   insert_tuple_count);
	}
	/* Restore es_result_relation_info before exiting */
	estate->es_result_relation_info = saved_resultRelInfo;

	/* begin ora_compatible */
	if (node->fireASTriggers)
	{
		node->fireASTriggers = false;

		/*
		 * We're done, but fire AFTER STATEMENT triggers before exiting.
		 */
		fireASTriggers(node);
	}
	/* end ora_compatible */

	node->mt_done = true;
#ifdef _MLS_            
    mls_command_tag_switch_to(oldtag);
#endif

	/*
	 * Online statistics gathering for bulk loads
	 * 
	 * Ordinary    Table: supported
	 * Temp        Table: supported
	 * Partitioned Table: currently not supported (analyze only gathering child)
	 * Global temp Table: supported (analyze gathering to localhash not catalog)
	 * 
	 * Index            : currently not supported
	 */
	if (ORA_MODE &&
		IS_CENTRALIZED_MODE &&
		operation == CMD_INSERT &&
		RelationGetOnlineGatheringThreshold(resultRelInfo->ri_RelationDesc) != -1 &&
		estate->es_processed >= RelationGetOnlineGatheringThreshold(resultRelInfo->ri_RelationDesc) &&
		subplanstate &&
		((!IsA(subplanstate, ValuesScanState) &&
		  !IsA(subplanstate, ProjectSetState) &&
		  !IsA(subplanstate, ResultState)) ||
		(IsA(subplanstate, ResultState) && subplanstate->lefttree != NULL)))
	{
		elog(DEBUG1, "online gathing [%s]: %ld", 
			 RelationGetRelationName(resultRelInfo->ri_RelationDesc), 
			 estate->es_processed);
		online_update_relstats(resultRelInfo->ri_RelationDesc,
							   (double) estate->es_processed);
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitModifyTable
 * ----------------------------------------------------------------
 */
ModifyTableState *
ExecInitModifyTable(ModifyTable *node, EState *estate, int eflags)
{
	ModifyTableState *mtstate;
	Plan	   *subplan = outerPlan(node);
	CmdType		operation = node->operation;
	int			nrels = list_length(node->resultRelations);
	ResultRelInfo *saved_resultRelInfo;
	ResultRelInfo *resultRelInfo;
	List	   *arowmarks;
	ListCell   *l;
	int			i;
	Relation	rel;
	bool		update_tuple_routing_needed = node->partColsUpdated;
#ifdef __OPENTENBASE__
	bool		remote_dml = false;
	bool		part_estore_with_stash = false;
#endif
#ifdef __AUDIT_FGA__
    ListCell      *item;
#endif

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = (Plan *) node;
	mtstate->ps.state = estate;

	mtstate->ps.ExecProcNode = ExecModifyTable;

	mtstate->operation = operation;
	mtstate->canSetTag = node->canSetTag;
	mtstate->mt_done = false;

	mtstate->mt_nrels = nrels;

	mtstate->resultRelInfo = estate->es_result_relations + node->resultRelIndex;

	/* If modifying a partitioned table, initialize the root table info */
	if (node->rootResultRelIndex >= 0)
		mtstate->rootResultRelInfo = estate->es_root_result_relations +
			node->rootResultRelIndex;

	/* set up epqstate with dummy subplan data for the moment */
	EvalPlanQualInit(&mtstate->mt_epqstate, estate, NULL, NIL, node->epqParam);
	mtstate->fireBSTriggers = !IsParallelWorker();
	mtstate->fireASTriggers = !IsParallelWorker(); /* ora_compatible */

#ifdef __OPENTENBASE__

	mtstate->mt_gindex_origin_slot = NULL;
	mtstate->mt_gindex_result_slot = NULL;
	mtstate->mt_gindex_origin_slot_used = true;
	if (node->update_gindex_key && node->global_index)
		mtstate->mt_gindex_origin_tuple = (HeapTuple)palloc0(sizeof(HeapTupleData));
	else
		mtstate->mt_gindex_origin_tuple = NULL;
#endif
	saved_resultRelInfo = estate->es_result_relation_info;

	/*
	 * Open all the result relations and initialize the ResultRelInfo structs.
	 * (But root relation was initialized above, if it's part of the array.)
	 * We must do this before initializing the subplan, because direct-modify
	 * FDWs expect their ResultRelInfos to be available.
	 */
	resultRelInfo = mtstate->resultRelInfo;

	i = 0;
	foreach(l, node->resultRelations)
	{
		/* Initialize the usesFdwDirectModify flag */
		resultRelInfo->ri_usesFdwDirectModify = bms_is_member(i,
															  node->fdwDirectModifyPlans);

		/*
		 * Verify result relation is a valid target for the current operation
		 */
		CheckValidResultRel(resultRelInfo, operation);

		resultRelInfo++;
		i++;
	}

	/*
	 * Now we may initialize the subplan.
	 */
	estate->es_result_relation_info = mtstate->resultRelInfo;
	outerPlanState(mtstate) = ExecInitNode(subplan, estate, eflags);

	/*
	 * Do additional per-result-relation initialization.
	 */
	for (i = 0; i < nrels; i++)
	{
		resultRelInfo = &mtstate->resultRelInfo[i];

		/*
		 * If there are indices on the result relation, open them and save
		 * descriptors in the result relation info, so that we can add new
		 * index entries for the tuples we add/update.  We need not do this
		 * for a DELETE, however, since deletion doesn't affect indexes. Also,
		 * inside an EvalPlanQual operation, the indexes might be open
		 * already, since we share the resultrel state with the original
		 * query.
		 */
		if (resultRelInfo->ri_RelationDesc->rd_rel->relhasindex &&
			operation != CMD_DELETE &&
			resultRelInfo->ri_IndexRelationDescs == NULL)
			ExecOpenIndices(resultRelInfo,
							node->onConflictAction != ONCONFLICT_NONE);

		/*
		* If this is an UPDATE and a BEFORE UPDATE trigger is present, the
		* trigger itself might modify the partition-key values. So arrange
		* for tuple routing.
		*/
		if (resultRelInfo->ri_TrigDesc &&
		   resultRelInfo->ri_TrigDesc->trig_update_before_row &&
		   operation == CMD_UPDATE)
		   update_tuple_routing_needed = true;

		/* Also let FDWs init themselves for foreign-table result rels */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			resultRelInfo->ri_FdwRoutine != NULL &&
			resultRelInfo->ri_FdwRoutine->BeginForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, i);

			resultRelInfo->ri_FdwRoutine->BeginForeignModify(mtstate,
															 resultRelInfo,
															 fdw_private,
															 i,
															 eflags);
		}
	}

#ifdef __OPENTENBASE__
	/*
	 * We have to execDML on coordinator, init remoteDML planstate.
	 */
	if (node->remote_plans)
	{
		Plan *remoteplan = NULL;
		int nremote_plans = list_length(node->remote_plans);
		EState *estate_dml = CreateExecutorState();

		remote_dml = true;

		mtstate->mt_remoterels = (PlanState **) palloc0(sizeof(PlanState *) * nremote_plans);
		
		for (i = 0; i < nremote_plans; i++)
		{
			remoteplan = list_nth(node->remote_plans, i);

			mtstate->mt_remoterels[i] = ExecInitNode(remoteplan, estate_dml, eflags | EXEC_FLAG_WITH_DESC);

			/* set params' number and type */
			{
				RemoteQuery *rq = (RemoteQuery *)remoteplan;
				RemoteQueryState *rqs = (RemoteQueryState *)mtstate->mt_remoterels[i];

				rqs->rqs_num_params = rq->rq_num_params;
				rqs->rqs_param_types = rq->rq_param_types;
				rqs->rqs_param_formats = NULL;
				rqs->ss_num_params = rq->ss_num_params;
				rqs->ss_param_types = rq->ss_param_types;
				rqs->su_num_params = rq->su_num_params;
				rqs->su_param_types = rq->su_param_types;
			}
		}
	}
#endif

	estate->es_result_relation_info = saved_resultRelInfo;

	/* Get the root target relation */
	rel = (getTargetResultRelInfo(mtstate))->ri_RelationDesc;

	/*
	* If it's not a partitioned table after all, UPDATE tuple routing should
	* not be attempted.
	*/
	if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	   update_tuple_routing_needed = false;

	/*
	* Build state for tuple routing if it's an INSERT or if it's an UPDATE of
	* partition key.
	*/
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
        (operation == CMD_INSERT || update_tuple_routing_needed ||
        operation == CMD_MERGE))
		mtstate->mt_partition_tuple_routing =
                        ExecSetupPartitionTupleRouting(mtstate, rel);

	if (mtstate->mt_partition_tuple_routing)
	{
		mtstate->vecslot_part_index_info = (int *)palloc(8192 * sizeof(int));
	}
	/* Build state for collecting transition tuples */
	ExecSetupTransitionCaptureState(mtstate, estate);

	/*
	* Construct mapping from each of the per-subplan partition attnos to the
	* root attno.  This is required when during update row movement the tuple
	* descriptor of a source partition does not match the root partitioned
	* table descriptor.  In such a case we need to convert tuples to the root
	* tuple descriptor, because the search for destination partition starts
	* from the root.  Skip this setup if it's not a partition key update.
	*/
	if (update_tuple_routing_needed)
	   ExecSetupChildParentMapForSubplan(mtstate);

	/*
	 * Initialize any WITH CHECK OPTION constraints if needed.
	 */
	resultRelInfo = mtstate->resultRelInfo;
	i = 0;
	foreach(l, node->withCheckOptionLists)
	{
		List	   *wcoList = (List *) lfirst(l);
		List	   *wcoExprs = NIL;
		ListCell   *ll;

		foreach(ll, wcoList)
		{
			WithCheckOption *wco = (WithCheckOption *) lfirst(ll);
			ExprState  *wcoExpr = ExecInitQual((List *) wco->qual,
											   &mtstate->ps);

			wcoExprs = lappend(wcoExprs, wcoExpr);
		}

		resultRelInfo->ri_WithCheckOptions = wcoList;
		resultRelInfo->ri_WithCheckOptionExprs = wcoExprs;
		resultRelInfo++;
		i++;
	}

#ifdef __AUDIT_FGA__
    if (enable_fga)
    {
        foreach (item, node->plan.audit_fga_quals)
        {
            AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
            
            audit_fga_policy_state * audit_fga_policy_state_item
                    = palloc0(sizeof(audit_fga_policy_state));

            audit_fga_policy_state_item->policy_name = audit_fga_qual->policy_name;
            audit_fga_policy_state_item->query_string = audit_fga_qual->query_string;
            audit_fga_policy_state_item->qual = 
                ExecInitQual(audit_fga_qual->qual, &mtstate->ps);

            mtstate->ps.audit_fga_qual = 
                lappend(mtstate->ps.audit_fga_qual, audit_fga_policy_state_item);      
        }
    }
#endif    

	/*
	 * Initialize RETURNING projections if needed.
	 */
	if (node->returningLists)
	{
		TupleTableSlot *slot;
		ExprContext *econtext;

		/*
		 * Initialize result tuple slot and assign its rowtype using the first
		 * RETURNING list.  We assume the rest will look the same.
		 */
		mtstate->ps.plan->targetlist = (List *) linitial(node->returningLists);

		/* Set up a slot for the output of the RETURNING projection(s) */
		ExecInitResultTupleSlotTL(&mtstate->ps);
		slot = mtstate->ps.ps_ResultTupleSlot;

		/* Need an econtext too */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);
		econtext = mtstate->ps.ps_ExprContext;

		/*
		 * Build a projection for each result rel.
		 */
		resultRelInfo = mtstate->resultRelInfo;
		foreach(l, node->returningLists)
		{
			List	   *rlist = (List *) lfirst(l);

			resultRelInfo->ri_projectReturning =
				ExecBuildProjectionInfo(rlist, econtext, slot, &mtstate->ps,
										resultRelInfo->ri_RelationDesc->rd_att);

			resultRelInfo++;
		}
	}
	else
	{
		/*
		 * We still must construct a dummy result tuple type, because InitPlan
		 * expects one (maybe should change that?).
		 */
		mtstate->ps.plan->targetlist = NIL;
		ExecInitResultTypeTL(&mtstate->ps);

		mtstate->ps.ps_ExprContext = NULL;
	}

	/* Set the list of arbiter indexes if needed for ON CONFLICT */
	resultRelInfo = mtstate->resultRelInfo;
	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		resultRelInfo->ri_onConflictArbiterIndexes = node->arbiterIndexes;
	}

	/*
	 * If needed, Initialize target list, projection and qual for ON CONFLICT
	 * DO UPDATE.
	 */
	if (node->onConflictAction == ONCONFLICT_UPDATE)
	{
		ExprContext *econtext;
		TupleDesc	relationDesc;
		TupleDesc	tupDesc;

		/* insert may only have one relation, inheritance is not expanded */
		Assert(nrels == 1);

		/* already exists if created by RETURNING processing above */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);

		econtext = mtstate->ps.ps_ExprContext;
		relationDesc = resultRelInfo->ri_RelationDesc->rd_att;

		/* initialize slot for the existing tuple */
		mtstate->mt_existing =
			ExecInitExtraTupleSlot(mtstate->ps.state, relationDesc);

		/* carried forward solely for the benefit of explain */
		mtstate->mt_excludedtlist = node->exclRelTlist;

		/* create state for DO UPDATE SET operation */
		resultRelInfo->ri_onConflict = makeNode(OnConflictSetState);

		/* create target slot for UPDATE SET projection */
		tupDesc = ExecTypeFromTL((List *) node->onConflictSet,
								 relationDesc->tdhasoid);
		mtstate->mt_conflproj =
			ExecInitExtraTupleSlot(mtstate->ps.state, tupDesc);

		resultRelInfo->ri_onConflict->oc_ProjTupdesc = tupDesc;
		
		/* build UPDATE SET projection state */
		resultRelInfo->ri_onConflict->oc_ProjInfo =
			ExecBuildProjectionInfo(node->onConflictSet, econtext,
									mtstate->mt_conflproj, &mtstate->ps,
									relationDesc);

		/* build DO UPDATE WHERE clause expression */
		if (node->onConflictWhere)
		{
			ExprState  *qualexpr;

			qualexpr = ExecInitQual((List *) node->onConflictWhere,
									&mtstate->ps);

			resultRelInfo->ri_onConflict->oc_WhereClause = qualexpr;
		}
	}

	/*
	 * If we have any secondary relations in an UPDATE or DELETE, they need to
	 * be treated like non-locked relations in SELECT FOR UPDATE, ie, the
	 * EvalPlanQual mechanism needs to be told about them.  Locate the
	 * relevant ExecRowMarks.
	 */
	arowmarks = NIL;
	foreach(l, node->rowMarks)
	{
		PlanRowMark *rc = lfirst_node(PlanRowMark, l);
		ExecRowMark *erm;
		ExecAuxRowMark *aerm;

		/* ignore "parent" rowmarks; they are irrelevant at runtime */
		if (rc->isParent)
			continue;

		/* Find ExecRowMark and build ExecAuxRowMark */
		erm = ExecFindRowMark(estate, rc->rti, false);
		aerm = ExecBuildAuxRowMark(erm, subplan->targetlist);
		arowmarks = lappend(arowmarks, aerm);
	}

	if (mtstate->operation == CMD_MERGE)
		ExecInitMerge(mtstate, estate, resultRelInfo);

	EvalPlanQualSetPlan(&mtstate->mt_epqstate, subplan, arowmarks);

	/*
	 * Initialize projection(s) to create tuples suitable for result rel(s).
	 * INSERT queries may need a projection to filter out junk attrs in the
	 * tlist.  UPDATE always needs a projection, because (1) there's always
	 * some junk attrs, and (2) we may need to merge values of not-updated
	 * columns from the old tuple into the final tuple.  In UPDATE, the tuple
	 * arriving from the subplan contains only new values for the changed
	 * columns, plus row identity info in the junk attrs.
	 *
	 * If there are multiple result relations, each one needs its own
	 * projection.  Note multiple rels are only possible for UPDATE/DELETE, so
	 * we can't be fooled by some needing a projection and some not.
	 *
	 * This section of code is also a convenient place to verify that the
	 * output of an INSERT or UPDATE matches the target table(s).
	 */
	for (i = 0; i < nrels; i++)
	{
		resultRelInfo = &mtstate->resultRelInfo[i];

		/*
		 * Prepare to generate tuples suitable for the target relation.
		 */
		if (operation == CMD_INSERT)
		{
			List	   *insertTargetList = NIL;
			bool		need_projection = false;
			TupleDesc	relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

			foreach(l, subplan->targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(l);

				if (!tle->resjunk)
					insertTargetList = lappend(insertTargetList, tle);
				else
					need_projection = true;
			}

			/*
			 * The junk-free list must produce a tuple suitable for the result
			 * relation.
			 */
			ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc,
								insertTargetList);

			/* We'll need a slot matching the table's format. */
			resultRelInfo->ri_newTupleSlot =
				ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable, relDesc);

			/* Build ProjectionInfo if needed (it probably isn't). */
			if (need_projection)
			{
				/* need an expression context to do the projection */
				if (mtstate->ps.ps_ExprContext == NULL)
					ExecAssignExprContext(estate, &mtstate->ps);

				resultRelInfo->ri_projectNew =
					ExecBuildProjectionInfo(insertTargetList,
											mtstate->ps.ps_ExprContext,
											resultRelInfo->ri_newTupleSlot,
											&mtstate->ps,
											relDesc);
			}
		}
		else if (operation == CMD_UPDATE)
		{
			List	   *updateColnos;
			TupleDesc	relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

			updateColnos = (List *) list_nth(node->updateColnosLists, i);

			/*
			 * For UPDATE, we use the old tuple to fill up missing values in
			 * the tuple produced by the plan to get the new tuple.  We need
			 * two slots, both matching the table's desired format.
			 */
			resultRelInfo->ri_oldTupleSlot =
				ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
								  RelationGetDescr(resultRelInfo->ri_RelationDesc));
			resultRelInfo->ri_newTupleSlot =
				ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
								  RelationGetDescr(resultRelInfo->ri_RelationDesc));

			/* need an expression context to do the projection */
			if (mtstate->ps.ps_ExprContext == NULL)
				ExecAssignExprContext(estate, &mtstate->ps);

#ifdef __OPENTENBASE_C__
			if (updateColnos == NULL)
			{
				List	   *updateTargetList = NIL;

				foreach(l, subplan->targetlist)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(l);

					if (!tle->resjunk)
						updateTargetList = lappend(updateTargetList, tle);
					else
						break;
				}

				ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc,
									updateTargetList);

				resultRelInfo->ri_projectNew =
					ExecBuildUpdateProjection(subplan->targetlist,
												false,
												NULL,
												relDesc,
												mtstate->ps.ps_ExprContext,
												resultRelInfo->ri_newTupleSlot,
												&mtstate->ps);
			}
			else
#endif
				resultRelInfo->ri_projectNew =
					ExecBuildUpdateProjection(subplan->targetlist,
				                              false,
				                              updateColnos,
				                              relDesc,
				                              mtstate->ps.ps_ExprContext,
				                              resultRelInfo->ri_newTupleSlot,
				                              &mtstate->ps);
		}
#ifdef __OPENTENBASE_C__
		else if (operation == CMD_DELETE && remote_dml)
		{
			/* use the old tuple for distributed key evaluation */
			resultRelInfo->ri_oldTupleSlot =
				ExecAllocTableSlot(&mtstate->ps.state->es_tupleTable,
								  RelationGetDescr(resultRelInfo->ri_RelationDesc));
		}
#endif

		/*
		 * For UPDATE/DELETE, find the appropriate junk attr now, either a
		 * 'ctid' or 'wholerow' attribute depending on relkind.  For foreign
		 * tables, the FDW might have created additional junk attr(s), but
		 * those are no concern of ours.
		 */
		if (operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE)
		{
			char		relkind;

			relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
			resultRelInfo->ri_junkFilter =
				  ExecInitJunkFilter(subplan->targetlist,
				                     resultRelInfo->ri_RelationDesc->rd_att->tdhasoid,
                                     ExecInitExtraTupleSlot(estate, NULL));
			if (relkind == RELKIND_RELATION ||
				relkind == RELKIND_MATVIEW ||
				relkind == RELKIND_PARTITIONED_TABLE)
			{
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
				if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk ctid column");
#ifdef __OPENTENBASE__
				/* shardid can be missed in some cases */
				resultRelInfo->ri_shardid =
					ExecFindJunkAttributeInTlist(subplan->targetlist, "shardid");
				if (remote_dml)
				{
					resultRelInfo->ri_xc_node_id =
						ExecFindJunkAttributeInTlist(subplan->targetlist, "xc_node_id");
					if (!AttributeNumberIsValid(resultRelInfo->ri_xc_node_id))
						elog(ERROR, "could not find junk xc_node_id column");
					resultRelInfo->ri_xc_wholerow =
						ExecFindJunkAttributeInTlist(subplan->targetlist, "wholerow");
					if (!AttributeNumberIsValid(resultRelInfo->ri_xc_wholerow) && node->has_trigger)
						elog(ERROR, "could not find junk wholerow column");
				}
#endif
			}
			else if (relkind == RELKIND_FOREIGN_TABLE)
			{
				/*
				 * When there is a row-level trigger, there should be a
				 * wholerow attribute.  We also require it to be present in
				 * UPDATE, so we can get the values of unchanged columns.
				 */
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(subplan->targetlist,
												 "wholerow");
				if (mtstate->operation == CMD_UPDATE &&
					!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk wholerow column");
			}
			else
			{
				/* Other valid target relkinds must provide wholerow */
				resultRelInfo->ri_RowIdAttNo =
					ExecFindJunkAttributeInTlist(subplan->targetlist,
												 "wholerow");
				if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
					elog(ERROR, "could not find junk wholerow column");
			}
		}
	}

	/*
	 * If this is an inherited update/delete, there will be a junk attribute
	 * named "tableoid" present in the subplan's targetlist.  It will be used
	 * to identify the result relation for a given tuple to be
	 * updated/deleted.
	 */
	mtstate->mt_resultOidAttno =
		ExecFindJunkAttributeInTlist(subplan->targetlist, "tableoid");
	Assert(AttributeNumberIsValid(mtstate->mt_resultOidAttno) || nrels == 1);
	mtstate->mt_lastResultOid = InvalidOid; /* force lookup at first tuple */
	mtstate->mt_lastResultIndex = 0;	/* must be zero if no such attr */

	/*
	 * If there are a lot of result relations, use a hash table to speed the
	 * lookups.  If there are not a lot, a simple linear search is faster.
	 *
	 * It's not clear where the threshold is, but try 64 for starters.  In a
	 * debugging build, use a small threshold so that we get some test
	 * coverage of both code paths.
	 */
#ifdef USE_ASSERT_CHECKING
#define MT_NRELS_HASH 4
#else
#define MT_NRELS_HASH 64
#endif
	if (nrels >= MT_NRELS_HASH || part_estore_with_stash)
	{
		HASHCTL		hash_ctl;

		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(MTTargetRelLookup);
		hash_ctl.hcxt = CurrentMemoryContext;
		mtstate->mt_resultOidHash =
			hash_create("ModifyTable target hash",
						nrels, &hash_ctl,
						HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		for (i = 0; i < nrels; i++)
		{
			Oid			hashkey;
			MTTargetRelLookup *mtlookup;
			bool		found;

			resultRelInfo = &mtstate->resultRelInfo[i];
			hashkey = RelationGetRelid(resultRelInfo->ri_RelationDesc);
			mtlookup = (MTTargetRelLookup *)
				hash_search(mtstate->mt_resultOidHash, &hashkey,
							HASH_ENTER, &found);
			Assert(!found);
			mtlookup->relationIndex = i;
		}
	}
	else
		mtstate->mt_resultOidHash = NULL;

	/*
	 * Set up a tuple table slot for use for trigger output tuples. In a plan
	 * containing multiple ModifyTable nodes, all can share one such slot, so
	 * we keep it in the estate.
	 */
	if (estate->es_trig_tuple_slot == NULL)
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);

	/*
	 * Lastly, if this is not the primary (canSetTag) ModifyTable node, add it
	 * to estate->es_auxmodifytables so that it will be run to completion by
	 * ExecPostprocessPlan.  (It'd actually work fine to add the primary
	 * ModifyTable node too, but there's no need.)  Note the use of lcons not
	 * lappend: we need later-initialized ModifyTable nodes to be shut down
	 * before earlier ones.  This ensures that we don't throw away RETURNING
	 * rows that need to be seen by a later CTE subplan.
	 */
#ifdef __OPENTENBASE_C__
	if (IS_PGXC_DATANODE)
#endif
	if (!mtstate->canSetTag)
		estate->es_auxmodifytables = lcons(mtstate,
										   estate->es_auxmodifytables);

	return mtstate;
}

/* ----------------------------------------------------------------
 *		ExecEndModifyTable
 *
 *		Shuts down the plan.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndModifyTable(ModifyTableState *node)
{
	int i;

	/*
	 * Allow any FDWs to shut down
	 */
	for (i = 0; i < node->mt_nrels; i++)
	{
		ResultRelInfo *resultRelInfo = node->resultRelInfo + i;
		
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			resultRelInfo->ri_FdwRoutine != NULL &&
			resultRelInfo->ri_FdwRoutine->EndForeignModify != NULL)
			resultRelInfo->ri_FdwRoutine->EndForeignModify(node->ps.state,
														   resultRelInfo);
	}

#ifdef __OPENTENBASE__
	if (IS_PGXC_COORDINATOR)
	{
		ModifyTable *plan = (ModifyTable *)node->ps.plan;

		if (plan->remote_plans)
		{
			int nremote_plans = list_length(plan->remote_plans);

			for (i = 0; i < nremote_plans; i++)
			{
				RemoteQuery *rq = (RemoteQuery *)list_nth(plan->remote_plans, i);

				ExecEndNode(node->mt_remoterels[i]);

				DropRemoteDMLStatement(rq->statement, rq->update_cursor);
			}
		}
	}
#endif

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (node->mt_partition_tuple_routing)
		ExecCleanupTupleRouting(node->mt_partition_tuple_routing);

	if (node->vecslot_part_index_info)
		pfree(node->vecslot_part_index_info);
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * Terminate EPQ execution if active
	 */
	EvalPlanQualEnd(&node->mt_epqstate);

	/*
	 * shut down subplan
	 */
	ExecEndNode(outerPlanState(node));

	if (node->mt_gindex_origin_slot != NULL)
		ExecDropSingleTupleTableSlot(node->mt_gindex_origin_slot);
	if (node->mt_gindex_result_slot != NULL)
		ExecDropSingleTupleTableSlot(node->mt_gindex_result_slot);
	if (node->mt_gindex_origin_tuple)
		heap_freetuple_ext(node->mt_gindex_origin_tuple);
	node->mt_gindex_origin_slot = NULL;
}

void
ExecReScanModifyTable(ModifyTableState *node)
{
	/* begin ora_compatible */
	if (ORA_MODE)
	{
		ModifyTable	*mplan = (ModifyTable *) node->ps.plan;

		if (mplan->is_submod)
		{
			PlanState	*subnode = outerPlanState(node);

			node->mt_done = false;
			if (node->ps.chgParam != NULL)
				UpdateChangedParamSet(subnode, node->ps.chgParam);

			if (subnode->chgParam == NULL)
				ExecReScan(subnode);

			return;
		}
	}
	/* end ora_compatible */
	/*
	 * Currently, we don't need to support rescan on ModifyTable nodes. The
	 * semantics of that would be a bit debatable anyway.
	 */
	elog(ERROR, "ExecReScanModifyTable is not implemented");
}

#ifdef __OPENTENBASE__
TupleTableSlot *
ExecRemoteUpdate(ModifyTableState *mtstate,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   TupleTableSlot *slot,
		   TupleTableSlot *planSlot,
		   EPQState *epqstate,
		   EState *estate,
		   bool canSetTag)
{
	return ExecUpdate(mtstate, tupleid, oldtuple, slot, 
		              planSlot, epqstate, estate, canSetTag, false);
}
#endif

#ifdef __OPENTENBASE_C__
/*
 * ExecSimpleDML
 * 
 * Part of the process of ExecSimpleRemoteDML on the DN, receiving the message 
 * from the CN and executing the corresponding INSERT, DELETE, UPDATE.
 * Raise error when concurrent UPDATE, DELETE happened. We use transaction
 * snapshot here, MAKE SURE DN IS ALREADY IN A TRANSACTION!
 * 
 * relname: name of target relation
 * cmdtype: INSERT, DELETE and UPDATE are supported
 * itup:    the new tuple of INSERT or UPDATE
 * dtup:    the old tuple of DELETE or UPDATE
 */
void
ExecSimpleDML(const char *relname, const char *space, CmdType cmdtype, HeapTuple itup, HeapTuple dtup, ItemPointer target_ctid)
{
	ItemPointer dtid = NULL;
	List       *recheck = NIL;
	ResultRelInfo   *resultRelInfo = makeNode(ResultRelInfo);
	TupleTableSlot  *slot = NULL;
	EState     *estate;
	Relation    relation;
	TupleDesc   desc;
	Snapshot    snapshot;
	Oid         relid = InvalidOid;
	
	snapshot = GetTransactionSnapshot_without_shard();
	relid = get_relname_relid(relname, get_namespaceid(space));
	relation = relation_open(relid, RowExclusiveLock);
	desc = RelationGetDescr(relation);
	
	resultRelInfo->ri_RelationDesc = relation;
	resultRelInfo->ri_RangeTableIndex = 1; /* dummy */
	ExecOpenIndices(resultRelInfo, false);
	
	estate = CreateExecutorState();
	estate->es_snapshot = snapshot;
	estate->es_result_relations = resultRelInfo;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_num_result_relations = 1;
	
	if (itup != NULL)
	{
		slot = MakeSingleTupleTableSlot(desc);
		ExecStoreHeapTuple(itup, slot, true);
	}
	
	/* delete same tuple as dtup locally */
	if (dtup != NULL || ItemPointerIsValid(target_ctid))
	{
		HeapTuple   tup;
		int         i;
		List       *indexlist = RelationGetIndexList(relation);
		if (ItemPointerIsValid(target_ctid))
		{
			HeapTupleData tuple;
			Buffer		buffer = InvalidBuffer;
			tuple.t_self = *target_ctid;
			if (!heap_fetch(relation, SnapshotAny, &tuple, &buffer, false, NULL))
			{
				if (BufferIsValid(buffer))
					ReleaseBuffer(buffer);
				elog(ERROR, "error when get %d/%d for %s", 
					ItemPointerGetBlockNumber(target_ctid), 
					target_ctid->ip_posid, relname);
			}
			if (!ItemPointerEquals(&(tuple.t_self), &(tuple.t_data->t_ctid)))
			{
				if (BufferIsValid(buffer))
					ReleaseBuffer(buffer);
				elog(ERROR, "error when get %d/%d for %s, allready be updated", 
						ItemPointerGetBlockNumber(target_ctid), target_ctid->ip_posid, relname);
			}
			ReleaseBuffer(buffer);
			dtid = target_ctid;
		}
		/* try use index first */
		if (dtid == NULL && (list_length(indexlist) > 0))
		{
			Relation        index = NULL;
			ListCell       *lc;
			
			/* if we have a primary key, use that index */
			if (OidIsValid(relation->rd_pkindex))
			{
				index = index_open(relation->rd_pkindex, RowExclusiveLock);
			}
			else
			{
				/* or we have to look through all indexes of this table */
				foreach(lc, indexlist)
				{
					Relation candidate = index_open(lfirst_oid(lc), RowExclusiveLock);
					Form_pg_index pg_index = candidate->rd_index;
					
					/* skip invalid ones */
					if (!IndexIsValid(pg_index))
					{
						index_close(candidate, RowExclusiveLock);
						continue;
					}
					
					/* use unique index if exists */
					if (pg_index->indisunique)
					{
						index = candidate;
						break;
					}
					
					/* or use the first one we meet */
					if (index == NULL)
					{
						index = candidate;
						continue;
					}
					
					/* close the index we didn't choose */
					index_close(candidate, RowExclusiveLock);
				}
			}
			
			/* index could still be NULL, if all indexes are invalid */
			if (index != NULL)
			{
				IndexScanDesc   scan;
				Datum           value;
				bool            isnull = true;
				int             nkeys;
				ScanKeyData    *key;
				TypeCacheEntry *typecache;
				Bitmapset      *indexkeys = NULL;
				
				nkeys = RelationGetNumberOfAttributes(index);
				key = palloc(sizeof(ScanKeyData) * nkeys);
				
				for (i = 0; i < nkeys; i++)
				{
					AttrNumber attno = index->rd_index->indkey.values[i];
					
					/* record index key and we don't need to compare them again */
					indexkeys = bms_add_member(indexkeys, attno);
					
					value = heap_getattr(dtup, attno, desc, &isnull);
					typecache = lookup_type_cache(desc->attrs[attno-1].atttypid,
					                              TYPECACHE_EQ_OPR);
					
					/* Initialize the scankey. */
					ScanKeyInit(&key[i],
					            attno,
					            BTEqualStrategyNumber,
					            get_opcode(typecache->eq_opr),
					            value);
					
					if (isnull)
						key[i].sk_flags = SK_ISNULL | SK_SEARCHNULL;
				}
				
				scan = index_beginscan(relation, index, snapshot, nkeys, 0);
				index_rescan(scan, key, nkeys, NULL, 0);
				
				while (dtid == NULL &&
				       (tup = index_getnext(scan, ForwardScanDirection, NULL)) != NULL)
				{
					/* compare other attrs besides indexkey */
					for (i = 1; i <= desc->natts; i++)
					{
						/* we don't need to compare indexkey again */
						if (!bms_is_member(i, indexkeys) &&
						    !heap_tuple_attr_equals(desc, i, tup, dtup))
							break;
					}
					
					if (i > desc->natts)
					{
						dtid = palloc(sizeof(ItemPointerData));
						ItemPointerCopy(&tup->t_self, dtid);
					}
				}
				
				index_endscan(scan);
				index_close(index, RowExclusiveLock);
				bms_free(indexkeys);
				pfree(key);
			}
		}
		
		/* we don't have a valid index, use seqscan although it could be very slow */
		if (dtid == NULL)
		{
			HeapScanDesc heapscan;
			
			heapscan = heap_beginscan(relation, snapshot, 0, NULL);
			heap_rescan(heapscan, NULL);
			
			while (dtid == NULL &&
			       (tup = heap_getnext(heapscan, ForwardScanDirection)) != NULL)
			{
				for (i = 1; i <= desc->natts; i++)
				{
					if (!heap_tuple_attr_equals(desc, i, tup, dtup))
						break;
				}
				
				if (i > desc->natts)
				{
					dtid = palloc(sizeof(ItemPointerData));
					ItemPointerCopy(&tup->t_self, dtid);
				}
			}
			
			heap_endscan(heapscan);
		}
	}
	
	/* set shard id for shard table */
	if (RelationIsSharded(relation) &&
	    itup && !ShardIDIsValid(HeapTupleGetShardId(itup)))
	{
		int32   shardId;
		slot_getallattrs(slot);
		shardId = EvaluateShardIdWithValuesNulls(relation, desc, slot->tts_values, slot->tts_isnull);
		HeapTupleHeaderSetShardId(itup->t_data, shardId);
	}
	
	if (cmdtype == CMD_INSERT)
	{
		Assert(itup != NULL);
		simple_heap_insert(relation, itup);
		if (resultRelInfo->ri_NumIndices > 0)
		{
			recheck = ExecInsertIndexTuples(slot, &(itup->t_self),
			                                estate, false, NULL,
			                                NIL);
		}
	}
	else if (cmdtype == CMD_DELETE)
	{
		if (dtid == NULL)
			elog(ERROR, "[gindex-delete] failed to find tid we want to delete");
		simple_heap_delete(relation, dtid);
	}
	else if (cmdtype == CMD_UPDATE)
	{
		Assert(itup != NULL);
		if (dtid == NULL)
			elog(ERROR, "[gindex-update] failed to find old tid we want to update");
		simple_heap_update(relation, dtid, itup);
		if (resultRelInfo->ri_NumIndices > 0 && !HeapTupleIsHeapOnly(itup))
		{
			recheck = ExecInsertIndexTuples(slot, &(itup->t_self),
			                                estate, false, NULL,
			                                NIL);
		}
	}
	
	if (slot != NULL)
		ExecDropSingleTupleTableSlot(slot);
	
	ExecCloseIndices(resultRelInfo);
	list_free(recheck);
	
	relation_close(relation, RowExclusiveLock);
	
	CommandCounterIncrement();
}

void 
updateGindexInsertSlotInfo(ModifyTableState *mtstate, 
										TupleDesc dataSlotDesc,
										TupleDesc planSlotDesc,
										ResultRelInfo *resultRelInfo)
{
	if (mtstate->mt_gindex_origin_slot == NULL)
		mtstate->mt_gindex_origin_slot = MakeTupleTableSlot(dataSlotDesc);
		
	if (mtstate->mt_gindex_result_slot == NULL)
		mtstate->mt_gindex_result_slot = MakeTupleTableSlot(planSlotDesc);
	mtstate->mt_gindex_origin_slot_used = false;
	
	mtstate->mt_gindex_origin_slot = 
		ExecStoreHeapTuple(mtstate->mt_gindex_origin_tuple, mtstate->mt_gindex_origin_slot, false);
	ExecProcessReturning(resultRelInfo,
						mtstate->mt_gindex_origin_slot,
						mtstate->mt_gindex_result_slot);
	(void)ExecCopySlot(mtstate->mt_gindex_result_slot,
				resultRelInfo->ri_projectReturning->pi_state.resultslot);
}
#endif
