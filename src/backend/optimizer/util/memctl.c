/*-------------------------------------------------------------------------
 *
 * memctl.c
 *	  Routines to calculate and adjust memory amount used by query.
 *
 * Copyright (c) 2022-Present OpenTenBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/memctl.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>

#include "postgres.h"
#include "optimizer/memctl.h"

#include "access/htup_details.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/nodemgr.h"
#include "pgxc/planner.h"
#include "utils/lsyscache.h"
#include "utils/resgroup.h"

int query_mem = 0;
int max_query_mem = 0;

typedef struct plan_tree_base_prefix
{
	Node *node; /* PlannerInfo* or PlannedStmt* */
} plan_tree_base_prefix;

typedef struct PlanMemWalkerContext
{
	plan_tree_base_prefix base;	/* required prefix for plan_tree_walker/mutator */

	bool dn_executed;		/* only case about plannodes executed on dn */
	int hash_mem;			/* work_mem used for hash and hashagg */
	int parallel_workers;	/* parallel worker number of current node */

	int calc_fix_mem;		/* fix part of calculated query memory (in kB) */
	int mem_nodes;			/* normalized memory intensive node number */
	int work_mem_nodes;		/* node number used work_mem */
	int hash_mem_nodes;		/* node number used hash_mem */
	int work_mem_nodes2;	/* node number used less than work_mem */
	int hash_mem_nodes2;	/* node number used less than hash_mem */
	int calc_work_mem2;		/* less than work_mem node part of calculated memory */
	int calc_hash_mem2;		/* less than hash_mem node part of calculated memory */
} PlanMemWalkerContext;


static void
exec_init_plan_tree_base(plan_tree_base_prefix *base, PlannedStmt *stmt)
{
	base->node = (Node*)stmt;
}

/*
 * Calculate memory used by normal plan node.
 */
static void
calc_plannode_mem(Plan *plan, PlanMemWalkerContext *ctx)
{
	int tupsize;

	if (!ctx->dn_executed)
		return;

	tupsize = MAXALIGN(plan->plan_width) + MAXALIGN(SizeofHeapTupleHeader);
	/*
	 * Note: heap tuple overhead here can provide some fudge factor for
	 * vectorization execution overhead.
	 */

	plan->operator_mem = (tupsize + 1023)/1024;
	ctx->calc_fix_mem += plan->operator_mem * ctx->parallel_workers;
}

/*
 * Calculate memory used by plan node using hashtable without spilling.
 */
static void
calc_plannode_hashmem(Plan *plan, PlanMemWalkerContext *ctx)
{
	int tupsize, total_size;

	if (!ctx->dn_executed)
		return;

	/*
	 * Note: we use heap tuple overhead here even though the tuples will
	 * actually be stored as MinimalTuples; this provides some fudge factor for
	 * hashtable overhead.
	 */
	tupsize = MAXALIGN(plan->plan_width) + MAXALIGN(SizeofHeapTupleHeader);
	total_size = (plan->plan_rows * tupsize + 1023)/1024;

	/* can not be limited by work_mem */
	plan->operator_mem = total_size;
	ctx->calc_fix_mem += plan->operator_mem * ctx->parallel_workers;
}

/*
 * Calculate memory used by plan node using tuplestore.
 */
static void
calc_tuplestore_mem(Plan *plan, PlanMemWalkerContext *ctx)
{
	int tupsize, total_size;

	if (!ctx->dn_executed)
		return;

	/*
	 * Note: we use heap tuple overhead here even though the tuples will
	 * actually be stored as MinimalTuples; this provides some fudge factor for
	 * execution overhead.
	 */
	tupsize = MAXALIGN(plan->plan_width) + MAXALIGN(SizeofHeapTupleHeader);
	total_size = (plan->plan_rows * tupsize + 1023)/1024;

	if (total_size >= work_mem)
	{
		total_size = work_mem;
		ctx->work_mem_nodes += ctx->parallel_workers;
	}
	else
	{
		if (total_size < MIN_WORK_MEM)
			total_size = MIN_WORK_MEM;
		ctx->work_mem_nodes2 += ctx->parallel_workers;
		ctx->calc_work_mem2 += total_size * ctx->parallel_workers;
	}

	plan->operator_mem = total_size;
}

/*
 * Calculate memory used by plan node using hashtable.
 */
static void
calc_hashtable_mem(Plan *plan, PlanMemWalkerContext *ctx)
{
	int tupsize, total_size;

	if (!ctx->dn_executed)
		return;

	/*
	 * Note: we use heap tuple overhead here even though the tuples will
	 * actually be stored as MinimalTuples; this provides some fudge factor for
	 * hashtable overhead.
	 */
	tupsize = MAXALIGN(plan->plan_width) + MAXALIGN(SizeofHeapTupleHeader);
	total_size = (plan->plan_rows * tupsize + 1023)/1024;

	if (total_size >= ctx->hash_mem)
	{
		total_size = ctx->hash_mem;
		ctx->hash_mem_nodes += ctx->parallel_workers;
	}
	else
	{
		if (total_size < MIN_WORK_MEM)
			total_size = MIN_WORK_MEM;
		ctx->hash_mem_nodes2 += ctx->parallel_workers;
		ctx->calc_hash_mem2 += total_size * ctx->parallel_workers;
	}

	plan->operator_mem = total_size;
}

/*
 * Calculate memory of send/recv buffer used by remote fragment.
 */
static void
calc_fragment_mem(RemoteSubplan *rplan, PlanMemWalkerContext *ctx)
{
	Plan *plan = (Plan *)rplan;
	int sort_mem = 0;
	int parallel_ratio = 1;
	int buffer_mem;

	if (!ctx->dn_executed)
		return;

	/* according to FragmentRecvStateInit */
	buffer_mem = (8192 * MAXALIGN(plan->plan_width) + 1023)/1024;
	if (rplan->sort)
	{
		/* final mergesort only use limited memory */
		sort_mem = MIN_WORK_MEM;
	}
	if (rplan->num_workers > 0)
	{
		parallel_ratio = ceil((double)rplan->num_workers/ctx->parallel_workers);
	}

	/* just count recv buffer while send buffer is small enough to be ignored */
	plan->operator_mem = sort_mem + NumDataNodes * parallel_ratio * buffer_mem;
	ctx->calc_fix_mem += plan->operator_mem * ctx->parallel_workers;
}

/*
 * Plan tree walker to estimate the query memory.
 */
static bool
PlanTreeMemWalker(Node *node, PlanMemWalkerContext *ctx)
{
	bool result = false;
	int org_parallel_workers = ctx->parallel_workers;
	bool org_dn_executed = ctx->dn_executed;

	if (node == NULL)
		return false;

	/* plan node normal memory */
	if (IS_PLAN_NODE(node))
		calc_plannode_mem((Plan *)node, ctx);

	switch (nodeTag(node))
	{
		/* node related to parallel worker number */
		case T_Gather:
			ctx->parallel_workers = ((Gather *)node)->num_workers;
			break;
		case T_GatherMerge:
			ctx->parallel_workers = ((GatherMerge *)node)->num_workers;
			break;
		case T_RemoteSubplan:
			{
				RemoteSubplan *rplan = (RemoteSubplan *)node;
				calc_fragment_mem(rplan, ctx);
				if (rplan->num_workers > 0)
					ctx->parallel_workers = rplan->num_workers;
				else
					ctx->parallel_workers = 1;
				ctx->dn_executed = true;
			}
			break;
		/* memory intensive node */
		case T_Material:
		case T_Sort:
			calc_tuplestore_mem((Plan *)node, ctx);
			break;
		case T_Hash:
			calc_hashtable_mem((Plan *)node, ctx);
			break;
		case T_SetOp:
			{
				SetOp *plan = (SetOp *)node;
				if (plan->strategy == SETOP_HASHED)
					calc_hashtable_mem((Plan *)node, ctx);
			}
			break;
		case T_Agg:
			{
				Agg *agg = (Agg *)node;
				if (agg->aggstrategy == AGG_HASHED ||
					agg->aggstrategy == AGG_MIXED)
					calc_hashtable_mem((Plan *)node, ctx);
			}
			break;
		case T_WindowAgg:
			calc_tuplestore_mem((Plan *)node, ctx);
			break;
		case T_Aggref:
			{
				Aggref *aggref = (Aggref *)node;
				if (aggref->distinct_num != 0)
				{
					TargetEntry *te = (TargetEntry *) linitial(aggref->distinct_args);
					Node *arg = (Node *) te->expr;
					Plan dummy;
					dummy.plan_rows = labs(aggref->distinct_num);
					dummy.plan_width = get_typavgwidth(exprType(arg), exprTypmod(arg));
					if (aggref->distinct_num > 0)
						calc_plannode_hashmem(&dummy, ctx);
					else
						calc_tuplestore_mem(&dummy, ctx);
				}
			}
			break;
		case T_SubPlan:
			{
				SubPlan *subplan = (SubPlan *)node;
				PlannedStmt *pstmt = (PlannedStmt *)ctx->base.node;
				Plan *plan = exec_subplan_get_plan(pstmt, subplan);

				if (subplan->useHashTable)
					calc_plannode_hashmem(plan, ctx);

				/* special for shared ctescan */
				if (subplan->subLinkType == CTE_SUBLINK && pstmt->sharedCtePlans)
				{
					ListCell *lc;
					foreach(lc, pstmt->sharedCtePlans)
					{
						CteScan *cteplan = (CteScan *)lfirst(lc);
						if (cteplan->ctePlanId == subplan->plan_id)
						{
							ctx->dn_executed = true;
							calc_plannode_mem((Plan *)cteplan, ctx);
							break;
						}
					}
				}
				ctx->parallel_workers = 1;
				result = plan_tree_walker(plan, PlanTreeMemWalker, ctx);
			}
			break;
		/* TODO: following plan node also use work_mem */
		case T_CteScan:
		case T_BitmapIndexScan:
		case T_FunctionScan:
		case T_RecursiveUnion:
		case T_Result:
		default:
			break;
	}

	result |= plan_or_expression_tree_walker(node, PlanTreeMemWalker, ctx);
	ctx->parallel_workers = org_parallel_workers;
	ctx->dn_executed = org_dn_executed;
	return result;
}

/*
 * Calculate the original query memory of plan tree.
 */
static void
calc_plan_querymem(PlannedStmt *stmt, PlanMemWalkerContext *ctx)
{
	(void)PlanTreeMemWalker((Node *)stmt->planTree, ctx);

	ctx->mem_nodes = (ctx->work_mem_nodes + ctx->work_mem_nodes2) +
		ceil(hash_mem_multiplier * (ctx->hash_mem_nodes + ctx->hash_mem_nodes2));

	stmt->planned_query_mem = ctx->calc_fix_mem +
		ctx->work_mem_nodes * work_mem + ctx->hash_mem_nodes * ctx->hash_mem +
		ctx->calc_work_mem2 + ctx->calc_hash_mem2;
	/* up to MB */
	stmt->planned_query_mem = ceil(stmt->planned_query_mem/1024.0) * 1024;

	ereport(DEBUG1,
			(errmsg_internal("planned_query_mem %d mem_nodes %d calc_fix_mem %d "
				"work_mem_nodes %d hash_mem_nodes %d "
				"work_mem_nodes2 %d hash_mem_nodes2 %d "
				"calc_work_mem2 %d calc_hash_mem2 %d",
				stmt->planned_query_mem, ctx->mem_nodes, ctx->calc_fix_mem,
				ctx->work_mem_nodes, ctx->hash_mem_nodes,
				ctx->work_mem_nodes2, ctx->hash_mem_nodes2,
				ctx->calc_work_mem2, ctx->calc_hash_mem2)));
}

/*
 * Adjust query memory of plan tree according to query_mem and work_mem.
 */
static void
adjust_plan_querymem(PlannedStmt *stmt, PlanMemWalkerContext *ctx, int tmp_query_mem)
{
	int tmp_work_mem;

	/* no way to adjust query memory by work_mem */
	if (ctx->mem_nodes == 0)
	{
		stmt->planned_work_mem = work_mem;
		/* up to an integral number of 32M */
		stmt->planned_query_mem = SIMPLE_QUERY_THRESHOLD *
			ceil(stmt->planned_query_mem / SIMPLE_QUERY_THRESHOLD);
		return;
	}

	tmp_work_mem = (tmp_query_mem - ctx->calc_fix_mem) / ctx->mem_nodes;

	if (tmp_work_mem <= MIN_WORK_MEM)
	{
		stmt->planned_work_mem = MIN_WORK_MEM;
		stmt->planned_query_mem = ctx->calc_fix_mem + MIN_WORK_MEM * ctx->mem_nodes;
		stmt->planned_query_mem = SIMPLE_QUERY_THRESHOLD *
			ceil(stmt->planned_query_mem / SIMPLE_QUERY_THRESHOLD);
		ereport(DEBUG1,
				(errmsg_internal("[adjust_plan_querymem] final query_mem(1) %d",
					stmt->planned_query_mem)));
		return;
	}

	if (tmp_work_mem >= work_mem)
	{
		stmt->planned_work_mem = work_mem;
		stmt->planned_query_mem = ctx->calc_fix_mem + work_mem * ctx->mem_nodes;
		stmt->planned_query_mem = SIMPLE_QUERY_THRESHOLD *
			ceil(stmt->planned_query_mem / SIMPLE_QUERY_THRESHOLD);
		ereport(DEBUG1,
				(errmsg_internal("[adjust_plan_querymem] final query_mem(2) %d",
					stmt->planned_query_mem)));
		return;
	}

	stmt->planned_work_mem = MAXALIGN_DOWN((tmp_query_mem - ctx->calc_fix_mem) / ctx->mem_nodes);
	stmt->planned_query_mem = ctx->calc_fix_mem + stmt->planned_work_mem * ctx->mem_nodes;
	/* up to an integral number of 32M */
	stmt->planned_query_mem = SIMPLE_QUERY_THRESHOLD *
		ceil(stmt->planned_query_mem / SIMPLE_QUERY_THRESHOLD);
	ereport(DEBUG1,
			(errmsg_internal("[desc_plan_querymem] final query_mem(3) %d planned_work_mem %d",
				stmt->planned_query_mem, stmt->planned_work_mem)));
}

/*
 * Set query memory used by plan tree for resource control.
 */
void
set_plan_querymem(PlannedStmt *stmt, int curr_query_mem)
{
	PlanMemWalkerContext ctx;

	MemSet(&ctx, 0, sizeof(PlanMemWalkerContext));
	exec_init_plan_tree_base(&ctx.base, stmt);
	ctx.hash_mem = get_hash_mem();
	ctx.parallel_workers = 1;
	if (IS_CENTRALIZED_DATANODE)
		ctx.dn_executed = true;

	calc_plan_querymem(stmt, &ctx);

	adjust_plan_querymem(stmt, &ctx, curr_query_mem);
}

/*
 * Print out query_mem setting info
 */
void
ExplainQueryMem(ExplainState *es, QueryDesc *queryDesc)
{
	/* nothing to do */
	if (es->format != EXPLAIN_FORMAT_TEXT ||
		(es->pstmt->planned_query_mem == 0 && !es->analyze))
		return;

	appendStringInfoSpaces(es->str, es->indent * 2);
	appendStringInfo(es->str, "Query Memory:");

	if (es->pstmt->planned_query_mem > 0)
	{
		appendStringInfo(es->str,
			" planned query_mem %dMB work_mem ",
			es->pstmt->planned_query_mem/1024);

		if (es->pstmt->planned_work_mem < 1024)
			appendStringInfo(es->str, "%dkB", es->pstmt->planned_work_mem);
		else
			appendStringInfo(es->str, "%.0fMB", ceil(es->pstmt->planned_work_mem/1024.0));
	}

	if (es->analyze)
	{
		if (es->query_mem == 0)
		{
			es->query_mem = MemoryContextMemAllocated(queryDesc->estate->es_query_cxt, true);
			es->query_mem = ceil(es->query_mem/1024.0);
		}
		else
			es->query_mem = ceil((double)es->query_mem/NumDataNodes);

		if (es->query_mem > 0)
			appendStringInfo(es->str, " execution query_mem %.0fMB",
				ceil(es->query_mem/1024.0));
	}

	appendStringInfoChar(es->str, '\n');
}
