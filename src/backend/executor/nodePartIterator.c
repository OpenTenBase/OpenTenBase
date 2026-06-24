/*-------------------------------------------------------------------------
 *
 * nodePartIterator.c
 *	  routines to handle PartIterator nodes.
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodePartIterator.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitPartIterator	- initialize the append node
 *		ExecPartIterator		- retrieve the next tuple from the node
 *		ExecEndPartIterator		- shut down the append node
 *		ExecReScanPartIterator 	- rescan the append node
 *
 *	 NOTES
 *		TODO: Add comment
 */

#include "postgres.h"

#include "executor/nodePartIterator.h"
#include "executor/execPartition.h"
#include "executor/execdebug.h"
#include "executor/nodeAppend.h"
#include "miscadmin.h"

#ifdef __OPENTENBASE_C__
#include "pgxc/planner.h"
#include "optimizer/planmain.h"
#endif

/* Shared state for parallel-aware Append. */
struct ParallelPartIteratorState
{
	LWLock		pa_lock;		/* mutual exclusion to choose next subplan */
	int			pa_next_plan;	/* next plan to choose by any worker */

	/*
	 * pa_finished[i] should be true if no more workers should select subplan
	 * i.  for a non-partial plan, this should be set to true as soon as a
	 * worker selects the plan; for a partial plan, it remains false until
	 * some worker executes the plan to completion.
	 */
	bool		pa_finished[FLEXIBLE_ARRAY_MEMBER];
};

#define INVALID_SUBPLAN_INDEX -1
#define NO_MATCHING_PARTITION  -2

static TupleTableSlot *ExecPartIterator(PlanState *pstate);
static bool            choose_next_part_locally(PartIteratorState *node);
static bool choose_next_part_for_leader(PartIteratorState *node);
static bool choose_next_part_for_worker(PartIteratorState *node);

static void InitScanPartition(PartIteratorState *node, int partitionScan);

/* ----------------------------------------------------------------
 *		ExecInitPartIterator
 *
 * ----------------------------------------------------------------
 */
PartIteratorState *
ExecInitPartIterator(PartIterator *node, EState *estate, int eflags)
{
	PartIteratorState *partIterState = makeNode(PartIteratorState);
	int			nplans;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create new PartIteratorState for our append node
	 */
	partIterState->ps.plan = (Plan *) node;
	partIterState->ps.state = estate;
	partIterState->ps.ExecProcNode = ExecPartIterator;

	/* initiate sub node */
    partIterState->ps.qual = NULL;
    partIterState->ps.righttree = NULL;
    partIterState->ps.subPlan = NULL;
    partIterState->ps.ps_ProjInfo = NULL;
	partIterState->pis_whichpart = INVALID_SUBPLAN_INDEX;
	partIterState->pis_leafNum = node->leafNum;

	/* If run-time partition pruning is enabled, then set that up now */
	if (node->part_prune_info != NULL)
	{
		PartitionPruneState *prunestate;

		/* We may need an expression context to evaluate partition exprs */
		ExecAssignExprContext(estate, &partIterState->ps);

		/* Create the working data structure for pruning. */
		prunestate = ExecCreatePartitionPruneState(&partIterState->ps,
												   node->part_prune_info);
		partIterState->pis_prune_state = prunestate;

		/* Perform an initial partition prune, if required. */
		if (prunestate->do_initial_prune)
		{
			/* Determine which parts survive initial pruning */
			partIterState->pis_valid_parts = ExecFindInitialMatchingSubPlans(prunestate,
															node->leafNum);

			/*
			 * The case where no parts survive pruning must be handled
			 * specially.
			 */
			if (bms_is_empty(partIterState->pis_valid_parts))
			{
				partIterState->pis_whichpart = NO_MATCHING_PARTITION;

				partIterState->pis_valid_parts = NULL;
			}

			nplans = bms_num_members(partIterState->pis_valid_parts);
		}
		else
		{
			/* We'll need to initialize all subplans */
			nplans = node->leafNum;
			partIterState->pis_valid_parts = bms_add_range(NULL, 0, nplans - 1);
		}
	}
	else
	{
		nplans = node->leafNum;

		/*
		 * When run-time partition pruning is not enabled we can just mark all
		 * subplans as valid; they must also all be initialized.
		 */
		partIterState->pis_valid_parts = bms_add_range(NULL, 0, nplans - 1);
		partIterState->pis_prune_state = NULL;
	}

	partIterState->pis_liveNum = nplans;
	partIterState->ps.lefttree = ExecInitNode(node->plan.lefttree, estate, eflags);

	/*
	 * Initialize result tuple type and slot.
	 */
	ExecInitResultTupleSlotTL(&partIterState->ps);

	/* For parallel query, this will be overridden later. */
	partIterState->choose_next_part = choose_next_part_locally;

	return partIterState;
}

/* ----------------------------------------------------------------
 *	   ExecPartIterator
 *
 *		Handles iteration over multiple parts.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecPartIterator(PlanState *pstate)
{
	PartIteratorState *node = castNode(PartIteratorState, pstate);

	/* If no part has been chosen, we must choose one before proceeding. */
	if (node->pis_whichpart == NO_MATCHING_PARTITION ||
	    (node->pis_whichpart == INVALID_SUBPLAN_INDEX && !node->choose_next_part(node)))
	{
		return NULL;
	}

	for (;;)
	{
		PlanState  *subnode;
		TupleTableSlot *result;

#ifdef __OPENTENBASE_C__
		if (node->pis_leafNum == 0)
			return NULL;
#endif

		CHECK_FOR_INTERRUPTS();

		/*
		 * figure out which part we are currently processing
		 */
		Assert(node->pis_whichpart >= 0 && node->pis_whichpart < node->pis_leafNum);

		subnode = outerPlanState(node);
		/*
		 * get a tuple from the part
		 */
		result = ExecProcNode(subnode);

		if (!TupIsNull(result))
		{
			/*
			 * If the part gave us something then return it as-is. We do
			 * NOT make use of the result slot that was set up in
			 * ExecInitAppend; there's no need for it.
			 */
			return result;
		}

		/* choose new part; if none, we're done */
		if (!node->choose_next_part(node))
			return NULL;
	}
}

/* ----------------------------------------------------------------
 *		ExecEndPartIterator
 * ----------------------------------------------------------------
 */
void
ExecEndPartIterator(PartIteratorState *node)
{
	/* Shut down any outer plan. */
	if (outerPlanState(node))
		ExecEndNode(outerPlanState(node));

	/*
	 * release any resources associated with run-time pruning
	 */
	if (node->pis_prune_state)
		ExecDestroyPartitionPruneState(node->pis_prune_state);
}

void
ExecReScanPartIterator(PartIteratorState *node)
{
	node->pis_whichpart = INVALID_SUBPLAN_INDEX;
}

/* ----------------------------------------------------------------
 *						Parallel PartIterator Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecPartIteratorEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecPartIteratorEstimate(PartIteratorState *node,
				   ParallelContext *pcxt)
{
	node->pstate_len =
		add_size(offsetof(ParallelPartIteratorState, pa_finished),
				 sizeof(bool) * node->pis_leafNum);

	shm_toc_estimate_chunk(&pcxt->estimator, node->pstate_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}


/* ----------------------------------------------------------------
 *		ExecPartIteratorInitializeDSM
 *
 *		Set up shared state for Parallel PartIterator.
 * ----------------------------------------------------------------
 */
void
ExecPartIteratorInitializeDSM(PartIteratorState *node,
						ParallelContext *pcxt)
{
	ParallelPartIteratorState *pstate;

	pstate = shm_toc_allocate(pcxt->toc, node->pstate_len);
	memset(pstate, 0, node->pstate_len);
	LWLockInitialize(&pstate->pa_lock, LWTRANCHE_PARALLEL_PARTITER);
	shm_toc_insert(pcxt->toc, node->ps.plan->plan_node_id, pstate);

	node->pis_pstate = pstate;
	node->choose_next_part = choose_next_part_for_leader;
}

/* ----------------------------------------------------------------
 *		ExecPartIteratorReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecPartIteratorReInitializeDSM(PartIteratorState *node, ParallelContext *pcxt)
{
	ParallelPartIteratorState *pstate = node->pis_pstate;

	pstate->pa_next_plan = 0;
	memset(pstate->pa_finished, 0, sizeof(bool) * node->pis_leafNum);
}

/* ----------------------------------------------------------------
 *		ExecPartIteratorInitializeWorker
 *
 *		Copy relevant information from TOC into planstate, and initialize
 *		whatever is required to choose and execute the optimal part.
 * ----------------------------------------------------------------
 */
void
ExecPartIteratorInitializeWorker(PartIteratorState *node, ParallelWorkerContext *pwcxt)
{
	node->pis_pstate = shm_toc_lookup(pwcxt->toc, node->ps.plan->plan_node_id, false);
	node->choose_next_part = choose_next_part_for_worker;
}

/* ----------------------------------------------------------------
 *		choose_next_part_locally
 *
 *		Choose next part for a non-parallel-aware PartIterator,
 *		returning false if there are no more.
 * ----------------------------------------------------------------
 */
static bool
choose_next_part_locally(PartIteratorState *node)
{
	int			whichplan;

next_valid:
	whichplan = node->pis_whichpart;
	if (ScanDirectionIsForward(node->ps.state->es_direction))
	{
		/*
		 * We won't normally see INVALID_SUBPLAN_INDEX in this case, but we
		 * might if a plan intended to be run in parallel ends up being run
		 * serially.
		 */
		if (whichplan == INVALID_SUBPLAN_INDEX)
			node->pis_whichpart = 0;
		else
		{
			if (whichplan >= node->pis_leafNum - 1)
				return false;
			node->pis_whichpart++;
		}
	}
	else
	{
		if (whichplan <= 0)
			return false;
		node->pis_whichpart--;
	}

	if (!bms_is_member(node->pis_whichpart, node->pis_valid_parts))
		goto next_valid;
	
	InitScanPartition(node, node->pis_whichpart);
	return true;
}

/* ----------------------------------------------------------------
 *		choose_next_part_for_leader
 *
 *      Try to pick a plan which doesn't commit us to doing much
 *      work locally, so that as much work as possible is done in
 *      the workers. 
 * ----------------------------------------------------------------
 */
static bool
choose_next_part_for_leader(PartIteratorState *node)
{
	ParallelPartIteratorState *pstate = node->pis_pstate;


	/* Backward scan is not supported by parallel-aware plans */
	Assert(ScanDirectionIsForward(node->ps.state->es_direction));

	LWLockAcquire(&pstate->pa_lock, LW_EXCLUSIVE);

	if (node->pis_whichpart != INVALID_SUBPLAN_INDEX)
	{
		/* Mark just-completed subplan as finished. */
		node->pis_pstate->pa_finished[node->pis_whichpart] = true;
	}
	else
	{
		/* Start with last subplan. */
		node->pis_whichpart = node->pis_leafNum - 1;
	}

	/* Loop until we find a subplan to execute. */
	while (pstate->pa_finished[node->pis_whichpart] ||
	       !bms_is_member(node->pis_whichpart, node->pis_valid_parts))
	{
		if (node->pis_whichpart == 0)
		{
			pstate->pa_next_plan = INVALID_SUBPLAN_INDEX;
			node->pis_whichpart = INVALID_SUBPLAN_INDEX;
			LWLockRelease(&pstate->pa_lock);
			return false;
		}
		node->pis_whichpart--;
	}

	LWLockRelease(&pstate->pa_lock);

	InitScanPartition(node, node->pis_whichpart);
	return true;
}

/* ----------------------------------------------------------------
 *		choose_next_part_for_worker
 *
 *		Choose next part for a parallel-aware Append, returning
 *		false if there are no more.
 *
 * ----------------------------------------------------------------
 */
static bool
choose_next_part_for_worker(PartIteratorState *node)
{
	ParallelPartIteratorState *pstate = node->pis_pstate;

	/* Backward scan is not supported by parallel-aware parts */
	Assert(ScanDirectionIsForward(node->ps.state->es_direction));
	LWLockAcquire(&pstate->pa_lock, LW_EXCLUSIVE);

	/* Mark just-completed part as finished. */
	if (node->pis_whichpart != INVALID_SUBPLAN_INDEX)
		node->pis_pstate->pa_finished[node->pis_whichpart] = true;

	/* If all the part are already done, we have nothing to do */
	if (pstate->pa_next_plan == INVALID_SUBPLAN_INDEX)
	{
		LWLockRelease(&pstate->pa_lock);
		return false;
	}

	/* Save the part from which we are starting the search. */
	node->pis_whichpart = pstate->pa_next_plan;

	/* Loop until we find a subplan to execute. */
	while (pstate->pa_finished[pstate->pa_next_plan] ||
	       !bms_is_member(pstate->pa_next_plan, node->pis_valid_parts))
	{
		if (pstate->pa_next_plan < node->pis_leafNum - 1)
		{
			/* Advance to next part. */
			pstate->pa_next_plan++;
		}
		else
		{
#ifndef _PG_REGRESS_
			/* Loop back to first part. */
			pstate->pa_next_plan = 0;
#else

			pstate->pa_next_plan = INVALID_SUBPLAN_INDEX;
			LWLockRelease(&pstate->pa_lock);
			return false;
#endif
		}

		if (pstate->pa_next_plan == node->pis_whichpart)
		{
			/* We've tried everything! */
			pstate->pa_next_plan = INVALID_SUBPLAN_INDEX;
			LWLockRelease(&pstate->pa_lock);
			return false;
		}
	}

	/* Pick the part we found, and advance pa_next_plan one more time. */
	node->pis_whichpart = pstate->pa_next_plan++;

	if (pstate->pa_next_plan >= node->pis_leafNum)
	{
		/* Loop back to first part. */
#ifndef _PG_REGRESS_
		pstate->pa_next_plan = 0;
#else
		pstate->pa_next_plan = INVALID_SUBPLAN_INDEX;
#endif
	}

	LWLockRelease(&pstate->pa_lock);
	InitScanPartition(node, node->pis_whichpart);
	return true;
}

static void
InitScanPartition(PartIteratorState *node, int partitionScan)
{
	unsigned int   itr_idx = 0;
	PartIterator  *pi_node = (PartIterator *) node->ps.plan;
	ParamExecData *param = NULL;

	Assert(ForwardScanDirection == pi_node->direction ||
	       BackwardScanDirection == pi_node->direction);

	itr_idx = node->pis_whichpart;
	if (BackwardScanDirection == pi_node->direction)
		itr_idx = partitionScan - itr_idx - 1;

	param = &(node->ps.state->es_param_exec_vals[pi_node->partParamno]);
	param->isnull = false;
	param->value = (Datum) itr_idx;
	node->ps.lefttree->chgParam = bms_add_member(node->ps.lefttree->chgParam, pi_node->partParamno);

	/* subplan will be re-scanned by first ExecProcNode. We do not need to call rescan*/
}
