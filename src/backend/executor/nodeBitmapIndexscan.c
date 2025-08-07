/*-------------------------------------------------------------------------
 *
 * nodeBitmapIndexscan.c
 *	  Routines to support bitmapped index scans of relations
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBitmapIndexscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecBitmapIndexScan	scans a relation using index.
 *		ExecInitBitmapIndexScan		creates and initializes state info.
 *		ExecReScanBitmapIndexScan	prepares to rescan the plan.
 *		ExecEndBitmapIndexScan		releases all storage.
 */
#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeIndexscan.h"
#include "parser/parsetree.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif

static void ExecInitNextPartitionForBitmapIndexScan(BitmapIndexScanState* node);

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScan
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapIndexScan(PlanState *pstate)
{
	elog(ERROR, "BitmapIndexScan node does not support ExecProcNode call convention");
	return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecBitmapIndexScan(node)
 * ----------------------------------------------------------------
 */
Node *
MultiExecBitmapIndexScan(BitmapIndexScanState *node)
{
	TIDBitmap  *tbm;
	IndexScanDesc scandesc;
	double		nTuples = 0;
	bool		doscan;

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
	{
		InstrStartFragment(node->ss.ps.instrument, &(node->ss.ps.state->executor_starttime));
		InstrStartNode(node->ss.ps.instrument);
	}
		

	/*
	 * extract necessary information from index scan node
	 */
	scandesc = node->biss_ScanDesc;

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 * Array keys are also treated as runtime keys; note that if ExecReScan
	 * returns with biss_RuntimeKeysReady still false, then there is an empty
	 * array key so we should do nothing.
	 */
	if (!node->biss_RuntimeKeysReady &&
		(node->biss_NumRuntimeKeys != 0 || node->biss_NumArrayKeys != 0))
	{
		ExecReScan((PlanState *) node);
		doscan = node->biss_RuntimeKeysReady;
	}
	else
		doscan = true;

	/*
	 * Prepare the result bitmap.  Normally we just create a new one to pass
	 * back; however, our parent node is allowed to store a pre-made one into
	 * node->biss_result, in which case we just OR our tuple IDs into the
	 * existing bitmap.  (This saves needing explicit UNION steps.)
	 */
	if (node->biss_result)
	{
		tbm = node->biss_result;
		node->biss_result = NULL;	/* reset for next time */
	}
	else
	{
		int flags = TBM_FLAG_RSTORE;

		/* XXX should we use less than work_mem for this? */
		tbm = tbm_create(work_mem * 1024L,
				((BitmapIndexScan *) node->ss.ps.plan)->isshared ?
				node->ss.ps.state->es_query_dsa : NULL,
				flags);
	}

	/*
	 * Get TIDs from index and insert into bitmap
	 */
	while (doscan)
	{
		nTuples += (double) index_getbitmap(scandesc, tbm);

		CHECK_FOR_INTERRUPTS();

		doscan = ExecIndexAdvanceArrayKeys(node->biss_ArrayKeys,
										   node->biss_NumArrayKeys);
		if (doscan)				/* reset index scan */
			index_rescan(node->biss_ScanDesc,
						 node->biss_ScanKeys, node->biss_NumScanKeys,
						 NULL, 0);
	}

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
	{
		InstrStopFragment(node->ss.ps.instrument, &(node->ss.ps.state->executor_starttime));
		InstrStopNode(node->ss.ps.instrument, nTuples);
	}
		

	return (Node *) tbm;
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanBitmapIndexScan(BitmapIndexScanState *node)
{
	ExprContext *econtext = node->biss_RuntimeContext;

	/*
	 * Reset the runtime-key context so we don't leak memory as each outer
	 * tuple is scanned.  Note this assumes that we will recalculate *all*
	 * runtime keys on each call.
	 */
	if (econtext)
		ResetExprContext(econtext);

	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.
	 *
	 * Array keys are also treated as runtime keys; note that if we return
	 * with biss_RuntimeKeysReady still false, then there is an empty array
	 * key so no index scan is needed.
	 */
	if (node->biss_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext,
								 node->biss_RuntimeKeys,
								 node->biss_NumRuntimeKeys);
	if (node->biss_NumArrayKeys != 0)
		node->biss_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								   node->biss_ArrayKeys,
								   node->biss_NumArrayKeys);
	else
		node->biss_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->ss.isPartTbl)
	{
		if (node->biss_ScanDesc != NULL)
			index_endscan(node->biss_ScanDesc);

		ExecInitNextPartitionForBitmapIndexScan(node);
	}
	else if (node->biss_RuntimeKeysReady)
		index_rescan(node->biss_ScanDesc,
					 node->biss_ScanKeys, node->biss_NumScanKeys,
					 NULL, 0);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapIndexScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapIndexScan(BitmapIndexScanState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc indexScanDesc;

	if (node->ss.isPartTbl)
	{
		ListCell *cell = NULL;
		Relation  relation;
		if (node->ss.parentRelation)
			node->ss.ss_currentRelation = node->ss.parentRelation;

		if (node->biss_ParentRelationDesc)
			node->biss_RelationDesc = node->biss_ParentRelationDesc;

		indexScanDesc = node->biss_ScanDesc;
		if (indexScanDesc != NULL)
		{
			index_endscan(indexScanDesc);
			node->biss_ScanDesc = NULL;
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
	indexRelationDesc = node->biss_RelationDesc;
	indexScanDesc = node->biss_ScanDesc;

	/*
	 * Free the exprcontext ... now dead code, see ExecFreeExprContext
	 */
#ifdef NOT_USED
	if (node->biss_RuntimeContext)
		FreeExprContext(node->biss_RuntimeContext, true);
#endif

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (indexScanDesc)
		index_endscan(indexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);

	/* close base relation */
	if (node->ss.ss_currentRelation)
		ExecCloseScanRelation(node->ss.ss_currentRelation);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapIndexScan
 *
 *		Initializes the index scan's state information.
 * ----------------------------------------------------------------
 */
BitmapIndexScanState *
ExecInitBitmapIndexScan(BitmapIndexScan *node, EState *estate, int eflags)
{
	BitmapIndexScanState *indexstate;
	bool		relistarget;
	Oid			reloid = InvalidOid;

#ifdef __AUDIT_FGA__
    ListCell      *item;
#endif    

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	indexstate = makeNode(BitmapIndexScanState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;
	indexstate->ss.isPartTbl = node->scan.isPartTbl;
	indexstate->ss.curPartIdx = -1;
	indexstate->ss.partScanDirection = node->scan.partScanDirection;
	indexstate->ss.ps.ExecProcNode = ExecBitmapIndexScan;

	/* normally we don't make the result bitmap till runtime */
	indexstate->biss_result = NULL;

	/*
	 * We do not open or lock the base relation here.  We assume that an
	 * ancestor BitmapHeapScan node is holding AccessShareLock (or better) on
	 * the heap relation throughout the execution of the plan tree.
	 */

	indexstate->ss.ss_currentScanDesc = NULL;

	/*
	 * Open the base relation to get storage type, we assume that the
	 * BitmapHeapScan node have been holed the lock, so we open it without lock
	 */
	reloid = getrelid(node->scan.scanrelid, estate->es_range_table);
	indexstate->ss.ss_currentRelation = heap_open(reloid, NoLock);

	/*
	 * Miscellaneous initialization
	 *
	 * We do not need a standard exprcontext for this node, though we may
	 * decide below to create a runtime-key exprcontext
	 */

	/*
	 * initialize child expressions
	 *
	 * We don't need to initialize targetlist or qual since neither are used.
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).
	 */

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
	indexstate->biss_RelationDesc = index_open(node->indexid,
											   relistarget ? NoLock : AccessShareLock);

	EstateAddItable(estate, node->indexid);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->biss_RuntimeKeysReady = false;
	indexstate->biss_RuntimeKeys = NULL;
	indexstate->biss_NumRuntimeKeys = 0;

#ifdef __AUDIT_FGA__
    if (enable_fga)
    {
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
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->biss_RelationDesc,
						   node->indexqual,
						   false,
						   &indexstate->biss_ScanKeys,
						   &indexstate->biss_NumScanKeys,
						   &indexstate->biss_RuntimeKeys,
						   &indexstate->biss_NumRuntimeKeys,
						   &indexstate->biss_ArrayKeys,
						   &indexstate->biss_NumArrayKeys);

	/*
	 * If we have runtime keys or array keys, we need an ExprContext to
	 * evaluate them. We could just create a "standard" plan node exprcontext,
	 * but to keep the code looking similar to nodeIndexscan.c, it seems
	 * better to stick with the approach of using a separate ExprContext.
	 */
	if (indexstate->biss_NumRuntimeKeys != 0 ||
		indexstate->biss_NumArrayKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->biss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->biss_RuntimeContext = NULL;
	}

	if (!node->scan.isPartTbl)
	{
		/*
		 * Initialize scan descriptor.
		 */
		indexstate->biss_ScanDesc = index_beginscan_bitmap(
			indexstate->biss_RelationDesc, estate->es_snapshot, indexstate->biss_NumScanKeys);

		/*
		 * If no run-time keys to calculate, go ahead and pass the scankeys to the
		 * index AM.
		 */
		if (indexstate->biss_NumRuntimeKeys == 0 && indexstate->biss_NumArrayKeys == 0)
			index_rescan(indexstate->biss_ScanDesc, indexstate->biss_ScanKeys,
			             indexstate->biss_NumScanKeys, NULL, 0);
	}

	if (node->scan.isPartTbl)
	{
		ListCell *cell = NULL;
		ListCell *idxCell = NULL;
		Form_pg_index pindex;
		Relation      indexRelation;
		List *idx_list = NIL;

		indexstate->ss.parentRelation = indexstate->ss.ss_currentRelation;
		indexstate->ss.parentScanDesc = indexstate->ss.ss_currentScanDesc;
		indexstate->biss_ParentRelationDesc = indexstate->biss_RelationDesc;
		pindex = indexstate->biss_RelationDesc->rd_index;

		foreach (cell, node->scan.partition_leaf_rels)
		{
			Relation rel;
			Oid      reloid = lfirst_oid(cell);
			LOCKMODE lockmode;

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
						    indexstate->biss_RelationDesc->rd_indcollation[i] !=
						        indexRelation->rd_indcollation[i] ||
						    indexstate->biss_RelationDesc->rd_opfamily[i] !=
						        indexRelation->rd_opfamily[i] ||
						    indexstate->biss_RelationDesc->rd_opcintype[i] !=
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

static void
ExecInitNextPartitionForBitmapIndexScan(BitmapIndexScanState* node)
{
	PlanState     *pstate = &node->ss.ps;
	EState        *estate = pstate->state;
	Relation       currentpartitionrel = NULL;
	Relation       currentpartitionidxrel = NULL;

	int            paramno = -1;
	ParamExecData *param = NULL;
	BitmapIndexScan       *plan = NULL;

	plan = (BitmapIndexScan *) node->ss.ps.plan;

	/* get partition sequnce */
	paramno = plan->scan.partIterParamno;
	param = &(estate->es_param_exec_vals[paramno]);
	node->ss.curPartIdx = (int) param->value;

	Assert(node->ss.curPartIdx <= plan->scan.leafNum);
	/* construct HeapScanDesc for new partition */
	currentpartitionrel = list_nth(node->ss.partition_leaf_rels, node->ss.curPartIdx);
	currentpartitionidxrel = list_nth(node->partition_index_leaf_rels, node->ss.curPartIdx);
	node->ss.ss_currentRelation = currentpartitionrel;
	node->biss_RelationDesc = currentpartitionidxrel;

	node->biss_ScanDesc = index_beginscan_bitmap(node->biss_RelationDesc, estate->es_snapshot,
	                                             node->biss_NumScanKeys);
	/*
	 * If no run-time keys to calculate or they are ready, go ahead and
	 * pass the scankeys to the index AM.
	 */
	if (node->biss_NumRuntimeKeys == 0 || node->biss_RuntimeKeysReady)
		index_rescan(node->biss_ScanDesc, node->biss_ScanKeys, node->biss_NumScanKeys, NULL, 0);
}
