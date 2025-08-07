/*-------------------------------------------------------------------------
 *
 * nodeLimit.c
 *	  Routines to handle limiting of query results where appropriate
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeLimit.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecLimit		- extract a limited range of tuples
 *		ExecInitLimit	- initialize node and subnodes..
 *		ExecEndLimit	- shutdown node and subnodes
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeLimit.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "catalog/pg_type.h"
#ifdef __OPENTENBASE_C__
#include "executor/execFragment.h"
#endif
#ifdef __OPENTENBASE__
#include "pgxc/squeue.h"
#ifdef _PG_ORCL_
#include "utils/guc.h"
#include <math.h>
#endif
#endif

typedef enum LimitStoreSlotOption
{
	LIMIT_STORE_REBUILD,
	LIMIT_STORE_VEC_ONE_SLOT,
	LIMIT_STORE_DEFAULT
} LimitStoreSlotOption;

extern Datum dtrunc(PG_FUNCTION_ARGS);
extern Datum dceil(PG_FUNCTION_ARGS);
extern float8 GetlimitCount(Datum val, bool float8_datum, bool percent);
static void recompute_limits(LimitState *node);
static int64 compute_tuples_needed(LimitState *node);

/*
 * Test if the new tuple and the last tuple match. If so we return true.
 */
static bool
ExecLimitWithTiesEqual(LimitState *node, TupleTableSlot *left, TupleTableSlot *right)
{
	ExprContext *econtext = node->ps.ps_ExprContext;

	econtext->ecxt_innertuple = left;
	econtext->ecxt_outertuple = right;
	return ExecQualAndReset(node->eqfunction, econtext);
}

/*
 * Execute the query plan and store the results in the tuple storage area.
 */
static int64
ExecLimitStoreSlot(LimitState *node, PlanState *outerplan)
{
	TupleTableSlot  *slot = NULL;
	int64            rows = 0;

	for(;;)
	{
		slot = ExecProcNode(outerplan);
		if (TupIsNull(slot))
		{
			break;
		}
		else
		{
			tuplestore_puttupleslot(node->tupstore, slot);
			rows++;
		}
	}
	return rows;
}

/*
 * To make the syntax compatible, the 'opentenbase_ora fetch percent_num PERCENT rows' statement caches all slots
 * and corrects the count.
 */
static void *
ExecLimitGetSlot(LimitState *node, PlanState  *outerplan, LimitStoreSlotOption option)
{
	TupleTableSlot  *slot = node->tmp_slot;
	void            *result = NULL;
	int64            rows = 0;
	int64            old_count = (int64) node->count;
	const int64      default_mincount = 1;

	if (!(node->limitOption == LIMIT_OPTION_PERCENT_COUNT ||
	      node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES))
	{
		return (void *) ExecProcNode(outerplan);
	}

	/* Release the resources of the tuple storage area. */
	if (option == LIMIT_STORE_REBUILD && node->tupstore)
	{
		tuplestore_end(node->tupstore);
		node->tupstore = NULL;
		node->tupstore_done = false;
	}

	if (node->tupstore_done)
	{
		tuplestore_gettupleslot(node->tupstore, true, false, slot);
		result = (void *) slot;
	}
	else
	{
		node->tupstore = tuplestore_begin_heap(true, false, work_mem);

		/* Execute the query plan and store the results in the tuple storage area. */
		rows = ExecLimitStoreSlot(node, outerplan);

		if (rows == 0)
		{
			/*
			 * The subplan returns too few tuples for us to produce
			 * any output at all.
			 */
			node->lstate = LIMIT_EMPTY;

			/* Release the resources of the tuple storage area. */
			tuplestore_end(node->tupstore);
			node->tupstore = NULL;
			node->tupstore_done = false;
			return NULL;
		}

		node->tupstore_done = true;

		if (node->limitOption == LIMIT_OPTION_PERCENT_COUNT ||
		    node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES)
		{
			/*
			 * Return at least one row of data. If there are decimals, round up.
			 * eg. select * from t_limit order by q1 fetch first 90 percent  rows only;
			 * The table t_limit has a total of 9 rows of data, and 90% calculation results in returning 9 columns.
			 */
			node->count = ceil(rows * node->count / 100);
			if (node->count == 0 && old_count > 0)
			{
				node->count = (float8) default_mincount;
			}
		}

		/* Reset the pointer of the tuple storage area so that tuples can be read from the beginning. */
		tuplestore_rescan(node->tupstore);

		slot = node->tmp_slot;
		ExecClearTuple(slot);
		tuplestore_gettupleslot(node->tupstore, true, false, slot);
		result = (void *) slot;
	}
	return result;
}

/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecLimit(PlanState *pstate)
{
	LimitState     *node = castNode(LimitState, pstate);
	ScanDirection   direction;
	TupleTableSlot *slot;
	PlanState      *outerPlan;
	bool            rebuild_tuplestore = false;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	direction = node->ps.state->es_direction;
	outerPlan = outerPlanState(node);

	/*
	 * The main logic is a simple state machine.
	 */
	switch (node->lstate)
	{
		case LIMIT_INITIAL:

			/*
			 * First call for this node, so compute limit/offset. (We can't do
			 * this any earlier, because parameters from upper nodes will not
			 * be set during ExecInitLimit.)  This also sets position = 0 and
			 * changes the state to LIMIT_RESCAN.
			 */
			recompute_limits(node);

			/* FALL THRU */

		case LIMIT_RESCAN:
			/*
			 * If backwards scan, just return NULL without changing state.
			 */
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Check for empty window; if so, treat like empty subplan.
			 */
			if (node->count <= 0 && !node->noCount)
			{
				node->lstate = LIMIT_EMPTY;
				return NULL;
			}

			rebuild_tuplestore = true;

			/*
			 * Fetch rows from subplan until we reach position > offset.
			 */
			for (;;)
			{
				LimitStoreSlotOption option = rebuild_tuplestore ? LIMIT_STORE_REBUILD : LIMIT_STORE_DEFAULT;

				slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, option);
				if (TupIsNull(slot))
				{
					/*
					 * The subplan returns too few tuples for us to produce
					 * any output at all.
					 */
					node->lstate = LIMIT_EMPTY;
					return NULL;
				}

				/*
				 * Tuple at limit is needed for comparation in subsequent
				 * execution to detect ties.
				 */
				if ((node->limitOption == LIMIT_OPTION_WITH_TIES ||
				     node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES) &&
				    node->position - node->offset == node->count - 1)
				{
					ExecCopySlot(node->last_slot, slot);
				}
				node->subSlot = slot;
				if (++node->position > node->offset)
					break;
				else
					rebuild_tuplestore = false;
			}

			/*
			 * Okay, we have the first tuple of the window.
			 */
			node->lstate = LIMIT_INWINDOW;
			break;

		case LIMIT_EMPTY:

			/*
			 * The subplan is known to return no tuples (or not more than
			 * OFFSET tuples, in general).  So we return no tuples.
			 */
			return NULL;

		case LIMIT_INWINDOW:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * Forwards scan, so check for stepping off end of window.  At
				 * the end of the window, the behavior depends on whether WITH
				 * TIES was specified: in that case, we need to change the
				 * state machine to LIMIT_WINDOWTIES.  If not (nothing was
				 * specified, or ONLY was) return NULL without advancing the
				 * subplan or the position variable but change the state
				 * machine to record having done so
				 *
				 * Once at the end, ideally, we can shut down parallel
				 * resources but that would destroy the parallel context which
				 * would be required for rescans.  To do that, we need to find
				 * a way to pass down more information about whether rescans
				 * are possible.
				 */
				if (!node->noCount &&
					node->position - node->offset >= node->count)
				{
					if (node->limitOption == LIMIT_OPTION_COUNT ||
					    node->limitOption == LIMIT_OPTION_PERCENT_COUNT)
					{
						node->lstate = LIMIT_WINDOWEND;
						ExecDisconnectNode(pstate);
						return NULL;
					}
					else
					{
						node->lstate = LIMIT_WINDOWEND_TIES;
						/* fall-through */
					}
				}
				else
				{
					/*
					 * Get next tuple from subplan, if any.
					 */
					slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, LIMIT_STORE_DEFAULT);
					if (TupIsNull(slot))
					{
						node->lstate = LIMIT_SUBPLANEOF;
						return NULL;
					}

					/*
					 * Tuple at limit is needed for comparation in subsequent
					 * execution to detect ties.
					 */
					if ((node->limitOption == LIMIT_OPTION_WITH_TIES ||
					     node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES) &&
						node->position - node->offset == node->count - 1)
					{
						ExecCopySlot(node->last_slot, slot);
					}
					node->subSlot = slot;
					node->position++;
					break;
				}
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * As above, change only state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					ExecDisconnectNode(pstate);
					return NULL;
				}

				/*
				 * Get previous tuple from subplan; there should be one!
				 */
				slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, LIMIT_STORE_DEFAULT);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->position--;
				break;
			}

		case LIMIT_WINDOWEND_TIES:
			if (ScanDirectionIsForward(direction))
			{
				/*
				 * Advance the subplan until we find the first row with
				 * different ORDER BY pathkeys.
				 */
				slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, LIMIT_STORE_DEFAULT);
				if (TupIsNull(slot))
				{
					node->lstate = LIMIT_SUBPLANEOF;
					return NULL;
				}

				/*
				 * Test if the new tuple and the last tuple match. If so we
				 * return the tuple.
				 */
				if (ExecLimitWithTiesEqual(node, node->last_slot, slot))
				{
					node->subSlot = slot;
					node->position++;
				}
				else
				{
					node->lstate = LIMIT_WINDOWEND;
					ExecDisconnectNode(pstate);
					return NULL;
				}
			}
			else
			{
				/*
				 * Backwards scan, so check for stepping off start of window.
				 * Change only state-machine status if so.
				 */
				if (node->position <= node->offset + 1)
				{
					node->lstate = LIMIT_WINDOWSTART;
					ExecDisconnectNode(pstate);
					return NULL;
				}

				/*
				 * Get previous tuple from subplan; there should be one! And
				 * change state-machine status.
				 */
				slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, LIMIT_STORE_DEFAULT);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->position--;
				node->lstate = LIMIT_INWINDOW;
			}
			break;

		case LIMIT_SUBPLANEOF:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Backing up from subplan EOF, so re-fetch previous tuple; there
			 * should be one!  Note previous tuple must be in window.
			 */
			slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, LIMIT_STORE_DEFAULT);
			if (TupIsNull(slot))
				elog(ERROR, "LIMIT subplan failed to run backwards");
			node->subSlot = slot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't advance it before */
			break;

		case LIMIT_WINDOWEND:
			if (ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * We already past one position to detect ties so re-fetch
			 * previous tuple; there should be one!  Note previous tuple must
			 * be in window.
			 */
			if (node->limitOption == LIMIT_OPTION_WITH_TIES ||
			    node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES)
			{
				slot = (TupleTableSlot *) ExecLimitGetSlot(node, outerPlan, LIMIT_STORE_DEFAULT);
				if (TupIsNull(slot))
					elog(ERROR, "LIMIT subplan failed to run backwards");
				node->subSlot = slot;
				node->lstate = LIMIT_INWINDOW;
			}
			else
			{
				/*
				 * Backing up from window end: simply re-return the last tuple
				 * fetched from the subplan.
				 */
				slot = node->subSlot;
				node->lstate = LIMIT_INWINDOW;
				/* position does not change 'cause we didn't advance it before */
			}
			break;

		case LIMIT_WINDOWSTART:
			if (!ScanDirectionIsForward(direction))
				return NULL;

			/*
			 * Advancing after having backed off window start: simply
			 * re-return the last tuple fetched from the subplan.
			 */
			slot = node->subSlot;
			node->lstate = LIMIT_INWINDOW;
			/* position does not change 'cause we didn't change it before */
			break;

		default:
			elog(ERROR, "impossible LIMIT state: %d",
				 (int) node->lstate);
			slot = NULL;		/* keep compiler quiet */
			break;
	}

	/* Return the current tuple */
	Assert(!TupIsNull(slot));

	return slot;
}


float8
GetlimitCount(Datum val, bool float8_datum, bool percent)
{
	float8 res = 0;

	if (float8_datum)
	{
		if (!percent)
			res = DatumGetFloat8(DirectFunctionCall1(dtrunc, val));
		else
			res = DatumGetFloat8(val);

		if (res < 0)
		{
			res = 0;
		}
	}
	else
	{
		res = (float8) DatumGetInt64(val);
	}
	return res;
}

/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
static void
recompute_limits(LimitState *node)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	Datum        val;
	bool         isNull;
	bool         ora_nulloffset = false;

	if (node->limitOffset)
	{
		val = ExecEvalExprSwitchContext(node->limitOffset,
										econtext,
										&isNull);
		/* Interpret NULL offset as no offset */
		if (isNull)
		{
			node->offset = 0;

			/* opentenbase_ora null offset, return nothing */
			if (ORA_MODE)
			{
				ora_nulloffset = true;
			}
		}
		else
		{
			bool  float8_datum = false;
			Expr *expr = node->limitOffset->expr;

			if (expr && exprType((const Node *) expr) == FLOAT8OID)
			{
				float8_datum = true;
			}

			node->offset = GetlimitCount(val, float8_datum, false);
			if (node->offset < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
						 errmsg("OFFSET must not be negative")));
		}
	}
	else
	{
		/* No OFFSET supplied */
		node->offset = 0;
	}

	if (node->limitCount && !ora_nulloffset)
	{
		val = ExecEvalExprSwitchContext(node->limitCount,
										econtext,
										&isNull);
		if (isNull)
		{
			/* opentenbase_ora mode, Interpret NULL count as 0 count (LIMIT NOTHING)  */
			if (ORA_MODE)
			{
				node->count = 0;
				node->noCount = false;
			}
			/* Interpret NULL count as no count (LIMIT ALL) */
			else
			{
				node->count = 0;
				node->noCount = true;
			}
		}
		else
		{
			bool  float8_datum = false;
			Expr *expr = node->limitCount->expr;

			if (expr && exprType((const Node *) expr) == FLOAT8OID)
			{
				float8_datum = true;
			}

			node->count = GetlimitCount(val, float8_datum,
				(node->limitOption == LIMIT_OPTION_PERCENT_COUNT || node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES));
			if (node->count < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
						 errmsg("LIMIT must not be negative")));
			node->noCount = false;
		}
	}
	else if (ora_nulloffset)
	{
		node->count = 0;
		node->noCount = false;
	}
	else
	{
		/* No COUNT supplied */
		node->count = 0;
		node->noCount = true;
	}

	/* Reset position to start-of-scan */
	node->position = 0;
	node->subSlot = NULL;

	/* Set state-machine state */
	node->lstate = LIMIT_RESCAN;

	/*
	 * Notify child node about limit.  Note: think not to "optimize" by
	 * skipping ExecSetTupleBound if compute_tuples_needed returns < 0.  We
	 * must update the child node anyway, in case this is a rescan and the
	 * previous time we got a different result.
	 */
	ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node));
}

/*
 * Compute the maximum number of tuples needed to satisfy this Limit node.
 * Return a negative value if there is not a determinable limit.
 * 
 * Required for opentenbase_ora compatibility. The 'FETCH FIRST/NEXT percent_cnt PERCENT'
 * syntax does not require limiting the sort boundary.
 */
static int64
compute_tuples_needed(LimitState *node)
{
	if ((node->noCount) ||
	    (node->limitOption == LIMIT_OPTION_WITH_TIES) ||
		(node->limitOption == LIMIT_OPTION_PERCENT_COUNT) ||
		(node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES))
		return -1;

	/* Note: if this overflows, we'll return a negative value, which is OK */
	return (int64) (node->count + node->offset);
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
LimitState *
ExecInitLimit(Limit *node, EState *estate, int eflags)
{
	LimitState *limitstate;
	Plan	   *outerPlan;
	TupleDesc   desc;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * create state structure
	 */
	limitstate = makeNode(LimitState);
	limitstate->ps.plan = (Plan *) node;
	limitstate->ps.state = estate;
	limitstate->ps.ExecProcNode = ExecLimit;

	limitstate->lstate = LIMIT_INITIAL;

	/*
	 * Miscellaneous initialization
	 *
	 * Limit nodes never call ExecQual or ExecProject, but they need an
	 * exprcontext anyway to evaluate the limit/offset parameters in.
	 */
	ExecAssignExprContext(estate, &limitstate->ps);

	/*
	 * initialize outer plan
	 */
	outerPlan = outerPlan(node);
	outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * initialize child expressions
	 */
	limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
										   (PlanState *) limitstate);
	limitstate->limitCount = ExecInitExpr((Expr *) node->limitCount,
										  (PlanState *) limitstate);
	limitstate->limitOption = node->limitOption;

	/*
	 * Initialize result type.
	 */
	ExecInitResultTypeTL(&limitstate->ps);

	/*
	 * limit nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	limitstate->ps.ps_ProjInfo = NULL;

	/* Initialize the common structure to facilitate the fetch percent syntax to obtain the tuplestore. */
	desc = ExecGetResultType(outerPlanState(limitstate));
	limitstate->tmp_slot = ExecInitExtraTupleSlot(estate, desc);
	limitstate->vec_withties_done = false;

	/*
	 * Initialize the equality evaluation, to detect ties.
	 */
	if (node->limitOption == LIMIT_OPTION_WITH_TIES ||
	    node->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES)
	{
		limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc);
		limitstate->eqfunction = execTuplesMatchPrepare(desc,
														node->uniqNumCols,
														node->uniqColIdx,
														node->uniqOperators,
														&limitstate->ps);
	}

	return limitstate;
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndLimit(LimitState *node)
{
	if (node->tupstore)
	{
		tuplestore_end(node->tupstore);
		node->tupstore = NULL;
	}

	ExecFreeExprContext(&node->ps);
	ExecEndNode(outerPlanState(node));
}


void
ExecReScanLimit(LimitState *node)
{
	/*
	 * Recompute limit/offset in case parameters changed, and reset the state
	 * machine.  We must do this before rescanning our child node, in case
	 * it's a Sort that we are passing the parameters down to.
	 */
	recompute_limits(node);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}
