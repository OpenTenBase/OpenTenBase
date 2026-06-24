/*-------------------------------------------------------------------------
 *
 * nodeIndexonlyscan.c
 *	  Routines to support index-only scans
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeIndexonlyscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecIndexOnlyScan			scans an index
 *		IndexOnlyNext				retrieve next tuple
 *		ExecInitIndexOnlyScan		creates and initializes state info.
 *		ExecReScanIndexOnlyScan		rescans the indexed relation.
 *		ExecEndIndexOnlyScan		releases all storage.
 *		ExecIndexOnlyMarkPos		marks scan position.
 *		ExecIndexOnlyRestrPos		restores scan position.
 *		ExecIndexOnlyScanEstimate	estimates DSM space needed for
 *						parallel index-only scan
 *		ExecIndexOnlyScanInitializeDSM	initialize DSM for parallel
 *						index-only scan
 *		ExecIndexOnlyScanReInitializeDSM	reinitialize DSM for fresh scan
 *		ExecIndexOnlyScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/visibilitymap.h"
#include "catalog/pg_type.h"
#include "executor/execdebug.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeIndexscan.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#ifdef _MLS_
#include "utils/mls.h"
#endif
#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif
#ifdef __OPENTENBASE_C__
#include "executor/nodeIndexscan.h"
#endif

static TupleTableSlot *IndexOnlyNext(IndexOnlyScanState *node);
static void StoreIndexTuple(TupleTableSlot *slot, IndexTuple itup,
				TupleDesc itupdesc);
static void ExecInitNextPartitionForIndexOnlyScan(IndexOnlyScanState* node);


/* ----------------------------------------------------------------
 *		IndexOnlyNext
 *
 *		Retrieve a tuple from the IndexOnlyScan node's index.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexOnlyNext(IndexOnlyScanState *node)
{
	EState	   *estate;
	ExprContext *econtext;
	ScanDirection direction;
	IndexScanDesc scandesc;
	TupleTableSlot *slot;
	ItemPointer tid;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((IndexOnlyScan *) node->ss.ps.plan)->indexorderdir))
	{
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->ioss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the index only scan is not parallel, or if we're
		 * executing a index only scan that was intended to be parallel
		 * serially.
		 */
		scandesc = index_beginscan(node->ss.ss_currentRelation,
								   node->ioss_RelationDesc,
								   estate->es_snapshot,
								   node->ioss_NumScanKeys,
								   node->ioss_NumOrderByKeys);

		node->ioss_ScanDesc = scandesc;


		/* Set it up for index-only scan */
		node->ioss_ScanDesc->xs_want_itup = true;
		node->ioss_VMBuffer = InvalidBuffer;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and
		 * pass the scankeys to the index AM.
		 */
		if (node->ioss_NumRuntimeKeys == 0 || node->ioss_RuntimeKeysReady)
			index_rescan(scandesc,
						 node->ioss_ScanKeys,
						 node->ioss_NumScanKeys,
						 node->ioss_OrderByKeys,
						 node->ioss_NumOrderByKeys);
	}

	/*
	 * OK, now that we have what we need, fetch the next tuple.
	 *
	 * Also check if we are index scan over estore relations with
	 * stash and are actually scanning stash. If so, skip the index
	 * part, because in this case we should have already scanned
	 * index.
	 */

	while ((tid = index_getnext_tid(scandesc, direction)) != NULL)
	{
		HeapTuple	tuple = NULL;
		bool		check_visible = false;

		CHECK_FOR_INTERRUPTS();

		/*
			* We can skip the heap fetch if the TID references a heap page on
			* which all tuples are known visible to everybody.  In any case,
			* we'll use the index tuple not the heap tuple as the data source.
			*
			* Note on Memory Ordering Effects: visibilitymap_get_status does not
			* lock the visibility map buffer, and therefore the result we read
			* here could be slightly stale.  However, it can't be stale enough to
			* matter.
			*
			* We need to detect clearing a VM bit due to an insert right away,
			* because the tuple is present in the index page but not visible. The
			* reading of the TID by this scan (using a shared lock on the index
			* buffer) is serialized with the insert of the TID into the index
			* (using an exclusive lock on the index buffer). Because the VM bit
			* is cleared before updating the index, and locking/unlocking of the
			* index page acts as a full memory barrier, we are sure to see the
			* cleared bit if we see a recently-inserted TID.
			*
			* Deletes do not update the index page (only VACUUM will clear out
			* the TID), so the clearing of the VM bit by a delete is not
			* serialized with this test below, and we may see a value that is
			* significantly stale. However, we don't care about the delete right
			* away, because the tuple is still visible until the deleting
			* transaction commits or the statement ends (if it's our
			* transaction). In either case, the lock on the VM buffer will have
			* been released (acting as a write barrier) after clearing the bit.
			* And for us to have a snapshot that includes the deleting
			* transaction (making the tuple invisible), we must have acquired
			* ProcArrayLock after that time, acting as a read barrier.
			*
			* It's worth going through this complexity to avoid needing to lock
			* the VM buffer, which could cause significant contention.
			*/
		if (!VM_ALL_VISIBLE(scandesc->heapRelation,
							ItemPointerGetBlockNumber(tid),
							&node->ioss_VMBuffer) || NeedMvcc())
		{
			check_visible = true;
		}

		if (check_visible)
		{
			/*
				* Rats, we have to visit the heap to check visibility.
				*/
			InstrCountTuples2(node, 1);
			
			tuple = index_fetch_heap(scandesc);
			
			if (tuple == NULL)
				continue;		/* no visible tuple, try next index entry */

			/*
				* Only MVCC snapshots are supported here, so there should be no
				* need to keep following the HOT chain once a visible entry has
				* been found.  If we did want to allow that, we'd need to keep
				* more state to remember not to call index_getnext_tid next time.
				*/
			if (scandesc->xs_continue_hot)
				elog(ERROR, "non-MVCC snapshots are not supported in index-only scans");

			/*
				* Note: at this point we are holding a pin on the heap page, as
				* recorded in scandesc->xs_cbuf.  We could release that pin now,
				* but it's not clear whether it's a win to do so.  The next index
				* entry might require a visit to the same heap page.
				*/
		}

		/*
			* Fill the scan tuple slot with data from the index.  This might be
			* provided in either HeapTuple or IndexTuple format.  Conceivably an
			* index AM might fill both fields, in which case we prefer the heap
			* format, since it's probably a bit cheaper to fill a slot from.
			*/
		if (scandesc->xs_hitup)
		{
			/*
				* We don't take the trouble to verify that the provided tuple has
				* exactly the slot's format, but it seems worth doing a quick
				* check on the number of fields.
				*/
			Assert(slot->tts_tupleDescriptor->natts ==
					scandesc->xs_hitupdesc->natts);
			ExecStoreHeapTuple(scandesc->xs_hitup, slot, false);
		}
		else if (scandesc->xs_itup)
			StoreIndexTuple(slot, scandesc->xs_itup, scandesc->xs_itupdesc);
		else
			elog(ERROR, "no data returned for index-only scan");

		/*
			* If the index was lossy, we have to recheck the index quals.
			* (Currently, this can never happen, but we should support the case
			* for possible future use, eg with GiST indexes.)
			*/
		if (scandesc->xs_recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->indexqual, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				continue;
			}
		}

		/*
			* We don't currently support rechecking ORDER BY distances.  (In
			* principle, if the index can support retrieval of the originally
			* indexed value, it should be able to produce an exact distance
			* calculation too.  So it's not clear that adding code here for
			* recheck/re-sort would be worth the trouble.  But we should at least
			* throw an error if someone tries it.)
			*/
		if (scandesc->numberOfOrderBys > 0 && scandesc->xs_recheckorderby)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("lossy distance functions are not supported in index-only scans")));

		/*
			* Predicate locks for index-only scans must be acquired at the page
			* level when the heap is not accessed, since tuple-level predicate
			* locks need the tuple's xmin value.  If we had to visit the tuple
			* anyway, then we already have the tuple-level lock and can skip the
			* page lock.
			*/
		if (tuple == NULL)
			PredicateLockPage(scandesc->heapRelation,
								ItemPointerGetBlockNumber(tid),
								estate->es_snapshot);

		return slot;
	}


	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 * StoreIndexTuple
 *		Fill the slot with data from the index tuple.
 *
 * At some point this might be generally-useful functionality, but
 * right now we don't need it elsewhere.
 */
static void
StoreIndexTuple(TupleTableSlot *slot, IndexTuple itup, TupleDesc itupdesc)
{
	int			nindexatts = itupdesc->natts;
	Datum	   *values = slot->tts_values;
	bool	   *isnull = slot->tts_isnull;
	int			i;

	/*
	 * Note: we must use the tupdesc supplied by the AM in index_getattr, not
	 * the slot's tupdesc, in case the latter has different datatypes (this
	 * happens for btree name_ops in particular).  They'd better have the same
	 * number of columns though, as well as being datatype-compatible which is
	 * something we can't so easily check.
	 */
	Assert(slot->tts_tupleDescriptor->natts == nindexatts);

	ExecClearTuple(slot);
	for (i = 0; i < nindexatts; i++)
		values[i] = index_getattr(itup, i + 1, itupdesc, &isnull[i]);
	ExecStoreVirtualTuple(slot);
}

/*
 * IndexOnlyRecheck -- access method routine to recheck a tuple in EvalPlanQual
 *
 * This can't really happen, since an index can't supply CTID which would
 * be necessary data for any potential EvalPlanQual target relation.  If it
 * did happen, the EPQ code would pass us the wrong data, namely a heap
 * tuple not an index tuple.  So throw an error.
 */
static bool
IndexOnlyRecheck(IndexOnlyScanState *node, TupleTableSlot *slot)
{
	elog(ERROR, "EvalPlanQual recheck is not supported in index-only scans");
	return false;				/* keep compiler quiet */
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecIndexOnlyScan(PlanState *pstate)
{
	IndexOnlyScanState *node = castNode(IndexOnlyScanState, pstate);

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 */
	if (node->ioss_NumRuntimeKeys != 0 && !node->ioss_RuntimeKeysReady)
		ExecReScan((PlanState *) node);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) IndexOnlyNext,
					(ExecScanRecheckMtd) IndexOnlyRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanIndexOnlyScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void
ExecReScanIndexOnlyScan(IndexOnlyScanState *node)
{
	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.  But first,
	 * reset the context so we don't leak memory as each outer tuple is
	 * scanned.  Note this assumes that we will recalculate *all* runtime keys
	 * on each call.
	 */
	if (node->ioss_NumRuntimeKeys != 0)
	{
		ExprContext *econtext = node->ioss_RuntimeContext;

		ResetExprContext(econtext);
		ExecIndexEvalRuntimeKeys(econtext,
								 node->ioss_RuntimeKeys,
								 node->ioss_NumRuntimeKeys);
	}
	node->ioss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->ss.isPartTbl)
	{
		if (node->ioss_ScanDesc != NULL)
		{
			index_endscan(node->ioss_ScanDesc);
			node->ioss_ScanDesc = NULL;
		}

		ExecInitNextPartitionForIndexOnlyScan(node);
	}
	else if (node->ioss_ScanDesc)
		index_rescan(node->ioss_ScanDesc, node->ioss_ScanKeys, node->ioss_NumScanKeys,
		             node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);

	ExecScanReScan(&node->ss);
}


/* ----------------------------------------------------------------
 *		ExecEndIndexOnlyScan
 * ----------------------------------------------------------------
 */
void
ExecEndIndexOnlyScan(IndexOnlyScanState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc indexScanDesc;
	Relation	relation;

	if (node->ss.isPartTbl)
	{
		ListCell *cell = NULL;
		if (node->ss.parentRelation)
			node->ss.ss_currentRelation = node->ss.parentRelation;

		if (node->ioss_ParentRelationDesc)
			node->ioss_RelationDesc = node->ioss_ParentRelationDesc;

		indexScanDesc = node->ioss_ScanDesc;
		if (indexScanDesc != NULL)
		{
			index_endscan(indexScanDesc);
			node->ioss_ScanDesc = NULL;
		}

		foreach (cell, node->ss.partition_leaf_rels)
		{
			relation = (Relation) lfirst(cell);
			ExecCloseScanRelation(relation);
		}

		foreach (cell, node->partition_index_leaf_rels)
		{
			relation = (Relation) lfirst(cell);
			index_close(relation, NoLock);
		}
	}

	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->ioss_RelationDesc;
	indexScanDesc = node->ioss_ScanDesc;
	relation = node->ss.ss_currentRelation;

	/* Release VM buffer pin, if any. */
	if (node->ioss_VMBuffer != InvalidBuffer)
	{
		ReleaseBuffer(node->ioss_VMBuffer);
		node->ioss_VMBuffer = InvalidBuffer;
	}

	/*
	 * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
	 */
#ifdef NOT_USED
	ExecFreeExprContext(&node->ss.ps);
	if (node->ioss_RuntimeContext)
		FreeExprContext(node->ioss_RuntimeContext, true);
#endif

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (indexScanDesc)
		index_endscan(indexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyMarkPos
 *
 * Note: we assume that no caller attempts to set a mark before having read
 * at least one tuple.  Otherwise, ioss_ScanDesc might still be NULL.
 * ----------------------------------------------------------------
 */
void
ExecIndexOnlyMarkPos(IndexOnlyScanState *node)
{
	EState	   *estate = node->ss.ps.state;

	if (estate->es_epqTuple != NULL)
	{
		/*
		 * We are inside an EvalPlanQual recheck.  If a test tuple exists for
		 * this relation, then we shouldn't access the index at all.  We would
		 * instead need to save, and later restore, the state of the
		 * es_epqScanDone flag, so that re-fetching the test tuple is
		 * possible.  However, given the assumption that no caller sets a mark
		 * at the start of the scan, we can only get here with es_epqScanDone
		 * already set, and so no state need be saved.
		 */
		Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (estate->es_epqTupleSet[scanrelid - 1])
		{
			/* Verify the claim above */
			if (!estate->es_epqScanDone[scanrelid - 1])
				elog(ERROR, "unexpected ExecIndexOnlyMarkPos call in EPQ recheck");
			return;
		}
	}

	index_markpos(node->ioss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyRestrPos
 * ----------------------------------------------------------------
 */
void
ExecIndexOnlyRestrPos(IndexOnlyScanState *node)
{
	EState	   *estate = node->ss.ps.state;

	if (estate->es_epqTuple != NULL)
	{
		/* See comments in ExecIndexOnlyMarkPos */
		Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;

		Assert(scanrelid > 0);
		if (estate->es_epqTupleSet[scanrelid - 1])
		{
			/* Verify the claim above */
			if (!estate->es_epqScanDone[scanrelid - 1])
				elog(ERROR, "unexpected ExecIndexOnlyRestrPos call in EPQ recheck");
			return;
		}
	}

	index_restrpos(node->ioss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecInitIndexOnlyScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
IndexOnlyScanState *
ExecInitIndexOnlyScan(IndexOnlyScan *node, EState *estate, int eflags)
{
	IndexOnlyScanState *indexstate;
	Relation	currentRelation;
	bool		relistarget;
	TupleDesc	tupDesc;

	/*
	 * create state structure
	 */
	indexstate = makeNode(IndexOnlyScanState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;
	indexstate->ss.isPartTbl = node->scan.isPartTbl;
	indexstate->ss.curPartIdx = -1;
	indexstate->ss.partScanDirection = node->scan.partScanDirection;
	indexstate->ss.ps.ExecProcNode = ExecIndexOnlyScan;
	indexstate->ioss_VMBuffer = InvalidBuffer;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &indexstate->ss.ps);

	/*
	 * open the base relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
#ifdef _MLS_
	mls_check_datamask_need_passby((ScanState *)indexstate, currentRelation->rd_id);
#endif

	indexstate->ss.ss_currentRelation = currentRelation;
	indexstate->ss.ss_currentScanDesc = NULL;	/* no heap scan here */

	/*
	 * Build the scan tuple type using the indextlist generated by the
	 * planner.  We use this, rather than the index's physical tuple
	 * descriptor, because the latter contains storage column types not the
	 * types of the original datums.  (It's the AM's responsibility to return
	 * suitable data anyway.)
	 */
	tupDesc = ExecTypeFromTL(node->indextlist, false);
	ExecInitScanTupleSlot(estate, &indexstate->ss, tupDesc);

	/*
	 * Initialize result type and projection info.  The node's targetlist will
	 * contain Vars with varno = INDEX_VAR, referencing the scan tuple.
	 */
	ExecInitResultTypeTL(&indexstate->ss.ps);
	ExecAssignScanProjectionInfoWithVarno(&indexstate->ss, INDEX_VAR);

	/*
	 * initialize child expressions
	 *
	 * Note: we don't initialize all of the indexorderby expression, only the
	 * sub-parts corresponding to runtime keys (see below).
	 */
	indexstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) indexstate);
	indexstate->indexqual =
		ExecInitQual(node->indexqual, (PlanState *) indexstate);

#ifdef __OPENTENBASE_C__
	indexstate->ioss_StashTupleSlot =
		ExecInitExtraTupleSlot(estate, RelationGetDescr(currentRelation));
	indexstate->ioss_StashProjInfo =
		ExecBuildProjectionInfo(node->indextlist,
								indexstate->ss.ps.ps_ExprContext,
								indexstate->ss.ss_ScanTupleSlot,
								&indexstate->ss.ps,
								RelationGetDescr(currentRelation));
#endif
#ifdef __AUDIT_FGA__
	if (enable_fga)
	{
		ListCell      *item;
		foreach (item, node->scan.plan.audit_fga_quals)
		{
			AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);

			audit_fga_policy_state * audit_fga_policy_state_item
				= palloc0(sizeof(audit_fga_policy_state));

			audit_fga_policy_state_item->policy_name = audit_fga_qual->policy_name;
			audit_fga_policy_state_item->query_string = audit_fga_qual->query_string;
			audit_fga_policy_state_item->qual = 
				ExecInitQual(audit_fga_qual->qual, (PlanState *) indexstate);

			indexstate->ss.ps.audit_fga_qual = 
				lappend(indexstate->ss.ps.audit_fga_qual, audit_fga_policy_state_item);      
		}
	}
#endif

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
	 * references to nonexistent indexes.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return indexstate;

	/*
	 * Open the index relation.
	 *
	 * If the parent table is one of the target relations of the query, then
	 * InitPlan already opened and write-locked the index, so we can avoid
	 * taking another lock here.  Otherwise we need a normal reader's lock.
	 */
	relistarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
	indexstate->ioss_RelationDesc = index_open(node->indexid,
											   relistarget ? NoLock : AccessShareLock);
	EstateAddItable(estate, node->indexid);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->ioss_RuntimeKeysReady = false;
	indexstate->ioss_RuntimeKeys = NULL;
	indexstate->ioss_NumRuntimeKeys = 0;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->ioss_RelationDesc,
						   node->indexqual,
						   false,
						   &indexstate->ioss_ScanKeys,
						   &indexstate->ioss_NumScanKeys,
						   &indexstate->ioss_RuntimeKeys,
						   &indexstate->ioss_NumRuntimeKeys,
						   NULL,	/* no ArrayKeys */
						   NULL);

	/*
	 * any ORDER BY exprs have to be turned into scankeys in the same way
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->ioss_RelationDesc,
						   node->indexorderby,
						   true,
						   &indexstate->ioss_OrderByKeys,
						   &indexstate->ioss_NumOrderByKeys,
						   &indexstate->ioss_RuntimeKeys,
						   &indexstate->ioss_NumRuntimeKeys,
						   NULL,	/* no ArrayKeys */
						   NULL);

	/*
	 * If we have runtime keys, we need an ExprContext to evaluate them. The
	 * node's standard context won't do because we want to reset that context
	 * for every tuple.  So, build another context just like the other one...
	 * -tgl 7/11/00
	 */
	if (indexstate->ioss_NumRuntimeKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->ioss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->ioss_RuntimeContext = NULL;
	}

	if (node->scan.isPartTbl)
	{
		ListCell *cell = NULL;
		ListCell *idxCell = NULL;
		Form_pg_index pindex;
		Relation      indexRelation;
		List *idx_list = NIL;
		LOCKMODE      lockmode;

		indexstate->ss.parentRelation = indexstate->ss.ss_currentRelation;
		indexstate->ss.parentScanDesc = indexstate->ss.ss_currentScanDesc;
		indexstate->ioss_ParentRelationDesc = indexstate->ioss_RelationDesc;
		pindex = indexstate->ioss_RelationDesc->rd_index;

		/*
		 * Determine the lock type we need.  First, scan to see if target relation
		 * is a result relation.  If not, check if it's a FOR UPDATE/FOR SHARE
		 * relation.  In either of those cases, we got the lock already.
		 */
		lockmode = AccessShareLock;
		if (ExecRelationIsTargetRelation(estate, node->scan.scanrelid))
			lockmode = NoLock;
		else
		{
			/* Keep this check in sync with InitPlan! */
			ExecRowMark *erm = ExecFindRowMark(estate, node->scan.scanrelid, true);

			if (erm != NULL && erm->relation != NULL)
				lockmode = NoLock;
		}

		foreach (cell, node->scan.partition_leaf_rels)
		{
			Relation rel;
			Oid      reloid = lfirst_oid(cell);

			rel = heap_open(reloid, lockmode);

			indexstate->ss.partition_leaf_rels = lappend(indexstate->ss.partition_leaf_rels, rel);
			indexRelation = NULL;
			idx_list = RelationGetIndexList(rel);

			foreach (idxCell, idx_list)
			{
				Form_pg_index index;
				bool          idx_match = false;

				indexRelation =
					index_open(lfirst_oid(idxCell), relistarget ? NoLock : AccessShareLock);
				index = indexRelation->rd_index;
				if (IndexIsValid(index) && pindex->indnatts == index->indnatts)
				{
					int i = 0;
					for (i = 0; i < index->indnatts; i++)
					{
						if (index->indkey.values[i] != pindex->indkey.values[i] ||
						    indexstate->ioss_RelationDesc->rd_indcollation[i] !=
						        indexRelation->rd_indcollation[i] ||
						    indexstate->ioss_RelationDesc->rd_opfamily[i] !=
						        indexRelation->rd_opfamily[i] ||
						    indexstate->ioss_RelationDesc->rd_opcintype[i] !=
						        indexRelation->rd_opcintype[i])
							break;
					}
					if (i == index->indnatts)
						idx_match = true;
				}

				if (idx_match)
					break;

				index_close(indexRelation, relistarget ? NoLock : AccessShareLock);
				indexRelation = NULL;
			}
			Assert(indexRelation != NULL);
			if (indexRelation != NULL)
			{
				EstateAddItable(estate, indexRelation->rd_id);
				indexstate->partition_index_leaf_rels =
					lappend(indexstate->partition_index_leaf_rels, indexRelation);
			}
		}
	}

	/*
	 * all done.
	 */
	return indexstate;
}

/* ----------------------------------------------------------------
 *		Parallel Index-only Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecIndexOnlyScanEstimate(IndexOnlyScanState *node,
						  ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->ioss_PscanLen = index_parallelscan_estimate(node->ioss_RelationDesc, estate->es_snapshot);

	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		shm_toc_estimate_chunk(&pcxt->estimator,
		                       BUFFERALIGN(node->ioss_PscanLen) * list_length(node->ss.partition_leaf_rels));
		shm_toc_estimate_keys(&pcxt->estimator, list_length(node->ss.partition_leaf_rels));
	}
	else
	{
		shm_toc_estimate_chunk(&pcxt->estimator, node->ioss_PscanLen);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanInitializeDSM
 *
 *		Set up a parallel index-only scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecIndexOnlyScanInitializeDSM(IndexOnlyScanState *node,
							   ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelIndexScanDesc piscan;

	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		ListCell *c1 = NULL;
		ListCell *c2 = NULL;
		int       whichplan = 0;
		forboth(c1, node->ss.partition_leaf_rels, c2, node->partition_index_leaf_rels)
		{
			Relation part = lfirst(c1);
			Relation partIdx = lfirst(c2);

			piscan = shm_toc_allocate(pcxt->toc, node->ioss_PscanLen);
			index_parallelscan_initialize(part, partIdx, estate->es_snapshot, piscan);
			shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id + whichplan, piscan);
			whichplan++;
		}
		node->ss.is_parallel = true;
		node->ss.toc = pcxt->toc;
	}
	else
	{
		piscan = shm_toc_allocate(pcxt->toc, node->ioss_PscanLen);
		index_parallelscan_initialize(node->ss.ss_currentRelation, node->ioss_RelationDesc,
		                              estate->es_snapshot, piscan);
		shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, piscan);
		node->ioss_ScanDesc =
			index_beginscan_parallel(node->ss.ss_currentRelation, node->ioss_RelationDesc,
		                             node->ioss_NumScanKeys, node->ioss_NumOrderByKeys, piscan);
		node->ioss_ScanDesc->xs_want_itup = true;
		node->ioss_VMBuffer = InvalidBuffer;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and pass
		 * the scankeys to the index AM.
		 */
		if (node->ioss_NumRuntimeKeys == 0 || node->ioss_RuntimeKeysReady)
			index_rescan(node->ioss_ScanDesc, node->ioss_ScanKeys, node->ioss_NumScanKeys,
			             node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);
	}
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecIndexOnlyScanReInitializeDSM(IndexOnlyScanState *node,
								 ParallelContext *pcxt)
{
	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		int           whichplan = 0;
		Relation      currentpartitionidxrel;
		Relation      currentpartitionrel;
		IndexScanDesc isdesc;
		int           part_num = list_length(node->ss.partition_leaf_rels);

		if (node->ioss_ScanDesc != NULL)
		{
			index_endscan(node->ioss_ScanDesc);
			node->ioss_ScanDesc = NULL;
		}

		for (whichplan = 0; whichplan < part_num; whichplan++)
		{
			ParallelIndexScanDesc pscan;
			pscan = shm_toc_lookup(node->ss.toc,
			                       node->ss.ps.plan->plan_node_id + whichplan, false);
			currentpartitionrel = list_nth(node->ss.partition_leaf_rels, whichplan);
			currentpartitionidxrel = list_nth(node->partition_index_leaf_rels, whichplan);

			isdesc =
				index_beginscan_parallel(currentpartitionrel, currentpartitionidxrel,
			                             node->ioss_NumScanKeys, node->ioss_NumOrderByKeys, pscan);

			index_parallelrescan(isdesc);
			index_endscan(isdesc);
		}
	}
	else
		index_parallelrescan(node->ioss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexOnlyScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecIndexOnlyScanInitializeWorker(IndexOnlyScanState *node,
								  ParallelWorkerContext *pwcxt)
{
	ParallelIndexScanDesc piscan;

	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		node->ss.is_parallel = true;
		node->ss.toc = pwcxt->toc;
	}
	else
	{
		piscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
		node->ioss_ScanDesc =
			index_beginscan_parallel(node->ss.ss_currentRelation, node->ioss_RelationDesc,
		                             node->ioss_NumScanKeys, node->ioss_NumOrderByKeys, piscan);
		node->ioss_ScanDesc->xs_want_itup = true;

		/*
		 * If no run-time keys to calculate or they are ready, go ahead and pass
		 * the scankeys to the index AM.
		 */
		if (node->ioss_NumRuntimeKeys == 0 || node->ioss_RuntimeKeysReady)
			index_rescan(node->ioss_ScanDesc, node->ioss_ScanKeys, node->ioss_NumScanKeys,
			             node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);
	}
}

static void
ExecInitNextPartitionForIndexOnlyScan(IndexOnlyScanState *node)
{
	PlanState     *pstate = &node->ss.ps;
	EState        *estate = pstate->state;
	Relation       currentpartitionrel = NULL;
	Relation       currentpartitionidxrel = NULL;

	int            paramno = -1;
	ParamExecData *param = NULL;
	IndexOnlyScan       *plan = NULL;

	plan = (IndexOnlyScan *) node->ss.ps.plan;

	/* get partition sequnce */
	paramno = plan->scan.partIterParamno;
	param = &(estate->es_param_exec_vals[paramno]);
	node->ss.curPartIdx = (int) param->value;

	Assert(node->ss.curPartIdx <= plan->scan.leafNum);
	/* construct HeapScanDesc for new partition */
	currentpartitionrel = list_nth(node->ss.partition_leaf_rels, node->ss.curPartIdx);
	currentpartitionidxrel = list_nth(node->partition_index_leaf_rels, node->ss.curPartIdx);
	node->ss.ss_currentRelation = currentpartitionrel;
	node->ioss_RelationDesc = currentpartitionidxrel;

	if (node->ss.is_parallel == true)
	{
		ParallelIndexScanDesc pscan;
		pscan = shm_toc_lookup(node->ss.toc, node->ss.ps.plan->plan_node_id + node->ss.curPartIdx,
		                       false);

		node->ioss_ScanDesc =
			index_beginscan_parallel(node->ss.ss_currentRelation, node->ioss_RelationDesc,
		                             node->ioss_NumScanKeys, node->ioss_NumOrderByKeys, pscan);
	}
	else
	{
		node->ioss_ScanDesc =
			index_beginscan(node->ss.ss_currentRelation, node->ioss_RelationDesc,
		                    estate->es_snapshot, node->ioss_NumScanKeys, node->ioss_NumOrderByKeys);
	}

	/* Set it up for index-only scan */
	node->ioss_ScanDesc->xs_want_itup = true;
	if (node->ioss_VMBuffer != InvalidBuffer)
	{
		ReleaseBuffer(node->ioss_VMBuffer);
		node->ioss_VMBuffer = InvalidBuffer;
	}

	/*
	 * If no run-time keys to calculate or they are ready, go ahead and
	 * pass the scankeys to the index AM.
	 */
	if (node->ioss_NumRuntimeKeys == 0 || node->ioss_RuntimeKeysReady)
		index_rescan(node->ioss_ScanDesc, node->ioss_ScanKeys, node->ioss_NumScanKeys,
		             node->ioss_OrderByKeys, node->ioss_NumOrderByKeys);
}
