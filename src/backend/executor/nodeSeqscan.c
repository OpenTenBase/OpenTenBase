/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"

#ifdef _MLS_
#include "utils/mls.h"
#endif
#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif
#ifdef __OPENTENBASE_C__

#endif

static TupleTableSlot *SeqNext(SeqScanState *node);
static void ExecInitNextPartitionForSeqScan(SeqScanState* node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're executing a
		 * scan that was intended to be parallel serially.
		 */
		scandesc = heap_beginscan(node->ss.ss_currentRelation,
								  estate->es_snapshot,
								  0, NULL);
		if(enable_distri_print)
		{
			elog(LOG, "seq scan snapshot local %d start ts "INT64_FORMAT " rel %s", estate->es_snapshot->local,
							estate->es_snapshot->start_ts, RelationGetRelationName(node->ss.ss_currentRelation));
		}
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	tuple = heap_getnext(scandesc, direction);

	if(enable_distri_debug)
	{
		if(tuple)
		{
			scandesc->rs_scan_number++;
		}
	}


	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreHeapTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreBufferHeapTuple(tuple,	/* tuple to store */
								 slot,	/* slot to store in */
								 scandesc->rs_cbuf);	/* buffer associated
														 * with this tuple */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}


/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.isPartTbl = node->isPartTbl;
	scanstate->ss.curPartIdx = -1;
	scanstate->ss.partScanDirection = node->partScanDirection;
	scanstate->ss.ps.ExecProcNode = ExecSeqScan;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * Initialize scan relation.
	 *
	 * Get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	scanstate->ss.ss_currentRelation =
		ExecOpenScanRelation(estate,
							 node->scanrelid,
							 eflags);

#ifdef _MLS_
	mls_check_datamask_need_passby((ScanState *)scanstate, scanstate->ss.ss_currentRelation->rd_id);
#endif 

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) scanstate);

#ifdef __AUDIT_FGA__
	if (enable_fga)
	{
		ListCell      *item;
		foreach (item, node->plan.audit_fga_quals)
		{
			AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);

			audit_fga_policy_state * audit_fga_policy_state_item
				 = palloc0(sizeof(audit_fga_policy_state));

			audit_fga_policy_state_item->policy_name = audit_fga_qual->policy_name;
			audit_fga_policy_state_item->query_string = audit_fga_qual->query_string;
			audit_fga_policy_state_item->qual = 
				ExecInitQual(audit_fga_qual->qual, (PlanState *) scanstate);

			scanstate->ss.ps.audit_fga_qual = 
				lappend(scanstate->ss.ps.audit_fga_qual, audit_fga_policy_state_item);      
		}
	}
#endif

	if (node->isPartTbl)
	{
		ListCell *cell = NULL;
		LOCKMODE  lockmode;

		scanstate->ss.parentRelation = scanstate->ss.ss_currentRelation;
		scanstate->ss.parentScanDesc = scanstate->ss.ss_currentScanDesc;
		/*
		 * Determine the lock type we need.  First, scan to see if target relation
		 * is a result relation.  If not, check if it's a FOR UPDATE/FOR SHARE
		 * relation.  In either of those cases, we got the lock already.
		 */
		lockmode = AccessShareLock;
		if (ExecRelationIsTargetRelation(estate, node->scanrelid))
			lockmode = NoLock;
		else
		{
			/* Keep this check in sync with InitPlan! */
			ExecRowMark *erm = ExecFindRowMark(estate, node->scanrelid, true);

			if (erm != NULL && erm->relation != NULL)
				lockmode = NoLock;
		}

		foreach (cell, node->partition_leaf_rels)
		{
			Relation rel;
			Oid      reloid = lfirst_oid(cell);

			rel = heap_open(reloid, lockmode);

			scanstate->ss.partition_leaf_rels = lappend(scanstate->ss.partition_leaf_rels, rel);
		}
	}

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	if (node->ss.isPartTbl)
	{
		ListCell *cell = NULL;
		node->ss.ss_currentRelation = node->ss.parentRelation;

		scanDesc = node->ss.ss_currentScanDesc;
		if (scanDesc != NULL)
		{
			heap_endscan(scanDesc);
			node->ss.ss_currentScanDesc = NULL;
		}

		node->ss.ss_currentScanDesc = node->ss.parentScanDesc;

		foreach (cell, node->ss.partition_leaf_rels)
		{
			relation = (Relation) lfirst(cell);
			ExecCloseScanRelation(relation);
		}
	}
	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	HeapScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (node->ss.isPartTbl)
	{
		if (scan != NULL)
			heap_endscan(scan);

		ExecInitNextPartitionForSeqScan(node);
	}
	else if (scan != NULL)
		heap_rescan(scan,		/* scan desc */
					NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = heap_parallelscan_estimate(estate->es_snapshot);
	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		shm_toc_estimate_chunk(&pcxt->estimator,
		                       BUFFERALIGN(node->pscan_len) * list_length(node->ss.partition_leaf_rels));
		shm_toc_estimate_keys(&pcxt->estimator, list_length(node->ss.partition_leaf_rels));
	}
	else
	{
		shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelHeapScanDesc pscan;

	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		ListCell *cell = NULL;
		int whichplan = 0;
		foreach (cell, node->ss.partition_leaf_rels)
		{
			Relation part = lfirst(cell);
			pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
			heap_parallelscan_initialize(pscan, part, estate->es_snapshot);
			shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id + whichplan, pscan);
			whichplan++;
		}
		node->ss.is_parallel = true;
		node->ss.toc = pcxt->toc;
	}
	else
	{
		Assert(!node->ss.isPartTbl);
		pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
		heap_parallelscan_initialize(pscan, node->ss.ss_currentRelation, estate->es_snapshot);
		shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
		node->ss.ss_currentScanDesc = heap_beginscan_parallel(node->ss.ss_currentRelation, pscan);
	}
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	HeapScanDesc scan = node->ss.ss_currentScanDesc;
	ParallelHeapScanDesc pscan;

	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		int whichplan = 0;
		int part_num = list_length(node->ss.partition_leaf_rels);
		for (whichplan = 0; whichplan < part_num; whichplan++)
		{
			pscan = shm_toc_lookup(node->ss.toc, node->ss.ps.plan->plan_node_id + whichplan, false);

			heap_parallelscan_reinitialize(pscan);
		}
	}
	else
		heap_parallelscan_reinitialize(scan->rs_parallel);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelHeapScanDesc pscan;

	if (node->ss.isPartTbl && enable_partition_iterator)
	{
		node->ss.is_parallel = true;
		node->ss.toc = pwcxt->toc;
	}
	else
	{
		pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
		node->ss.ss_currentScanDesc = heap_beginscan_parallel(node->ss.ss_currentRelation, pscan);
	}
}

static void
ExecInitNextPartitionForSeqScan(SeqScanState *node)
{
	PlanState     *pstate = &node->ss.ps;
	EState        *estate = pstate->state;
	Relation       currentpartitionrel = NULL;

	int            paramno = -1;
	ParamExecData *param = NULL;
	SeqScan       *plan = NULL;

	plan = (SeqScan *) node->ss.ps.plan;

	/* get partition sequnce */
	paramno = plan->partIterParamno;
	param = &(estate->es_param_exec_vals[paramno]);
	node->ss.curPartIdx = (int) param->value;

	Assert(node->ss.curPartIdx < plan->leafNum);

	/* construct HeapScanDesc for new partition */
	currentpartitionrel = list_nth(node->ss.partition_leaf_rels, node->ss.curPartIdx);
	node->ss.ss_currentRelation = currentpartitionrel;

	if (node->ss.is_parallel == true)
	{
		ParallelHeapScanDesc pscan;
		pscan = shm_toc_lookup(node->ss.toc,
		                       node->ss.ps.plan->plan_node_id + node->ss.curPartIdx, false);
		node->ss.ss_currentScanDesc = heap_beginscan_parallel(node->ss.ss_currentRelation, pscan);
	}
	else
	{
		node->ss.ss_currentScanDesc =
			heap_beginscan(node->ss.ss_currentRelation, estate->es_snapshot, 0, NULL);
	}
}
