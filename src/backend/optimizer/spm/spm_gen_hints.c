/*-------------------------------------------------------------------------
 *
 * spm_gen_hints.c
 *		
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "optimizer/spm_gen_hints.h"
#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "utils/varlena.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "parser/parsetree.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/planner.h"
#include "utils/ruleutils.h"
#include "utils/guc_tables.h"
#include "nodes/pg_list.h"
#include "optimizer/spm.h"
#include "commands/explain.h"
#include "nodes/makefuncs.h"
#include "executor/spi.h"
#include "commands/prepare.h"
#include "access/hash.h"
typedef enum DistributionHintType
{
	NO_DISTRIBUTION_TYPE,
	LEFT_REPLICATE_TYPE,
	RIGHT_REPLICATE_TYPE,
	DISTRIBUTED_TYPE
} DistributionHintType;

typedef struct
{
	bool	invalid;
	bool	under_append;
	int		plannerinfo_id;
	int		join_nums;
} query_block_validation_context;

char *spm_set_hint = NULL;

static bool                 create_leading_hint(PlanState *planstate, SpmHintsState *shs);
static bool                 get_used_rels(PlanState *planstate, Bitmapset **rels_used);
static DistributionHintType check_distribution_type(Plan *plan);
static int                  get_rteid(int plannerinfo_seq_no, int rte_array_id, SpmHintsState *shs);
static char                *get_rte_seq_no(SpmHintsState *shs, Relids relids);
static RangeTblEntry       *check_real_relation_node(SpmHintsState *shs, RangeTblEntry *rte,
                                                     int plannerinfo_id);
static int                  get_relids_plannerinfo_id(SpmHintsState *shs, Bitmapset *rels_used);
static void                 show_table_name_by_seq_no(SpmHintsState *shs);
static void create_parallel_hint(PlanState *planstate, RangeTblEntry *rte, SpmHintsState *shs);
static void create_scan_hint(Plan *plan, Index rti, SpmHintsState *shs);
static void create_subplan_hints(List *plans, List *ancestors, SpmHintsState *shs);
static void create_memberplan_hints(List *plans, PlanState **planstates, List *ancestors,
                                    SpmHintsState *shs);
static bool create_leading_hint(PlanState *planstate, SpmHintsState *shs);
static void create_plan_hints(PlanState *planstate, List *ancestors, SpmHintsState *shs);
static void spm_explain_agg(PlanState *planstate, SpmHintsState *shs);
static void create_set_hints(SpmHintsState *shs);

SpmHintsState *
NewSpmHintsState(void)
{
	SpmHintsState *shs = (SpmHintsState *) palloc0(sizeof(SpmHintsState));

	shs->scan_str = makeStringInfo();
	shs->join_str = makeStringInfo();
	shs->rows_str = makeStringInfo();
	shs->distribution_str = makeStringInfo();
	shs->parallel_str = makeStringInfo();
	shs->aggpath_str = makeStringInfo();
	shs->guc_str = makeStringInfo();
	shs->table_seq_no_str = makeStringInfo();
	shs->mergejoin_key_str = makeStringInfo();
	shs->table_hint_list = makeStringInfo();
	shs->init_plan_list = NULL;

	return shs;
}

/*
 * Get only RTE in current plannerinfo.
 *
 * Algorithm :
 * 1. If RTE is in current plannerinfo, continue;
 * 2. If parent RTE is current plannerinfo, mark the RTE in bitmap;
 * 3. Remove from bitmap for RTE which in not in current plannerinfo.
 *
 * brother_used:
 * 	Sometimes, current relations is under subquery. Also check brother relations
 *  to make bother shrink to the same query block.
 */
static bool
ShrinkRelids(SpmHintsState *shs, Bitmapset **rels_used, Bitmapset **brother_used, int plannerinfo_id_input)
{
	int x = -1;
	int plannerinfo_id;
	RangeTblEntry *rte;

	if (bms_num_members(*rels_used) == 0)
		return false;

	if (plannerinfo_id_input > 0)
	{
		plannerinfo_id = plannerinfo_id_input;
	}
	else
	{
		plannerinfo_id = get_relids_plannerinfo_id(shs, *rels_used);

		if (brother_used != NULL)
			plannerinfo_id = Min(plannerinfo_id, get_relids_plannerinfo_id(shs, *brother_used));
	}

	if (plannerinfo_id <= 0)
		return false;

	while ((x = bms_prev_member(*rels_used, x)) >= 0)
	{
		rte = rt_fetch(x, shs->rtable);

		if (plannerinfo_id == rte->plannerinfo_seq_no &&
			plannerinfo_id != rte->parent_plannerinfo_seq_no)
			continue;

		if (plannerinfo_id <= rte->parent_plannerinfo_seq_no ||
			(plannerinfo_id == 1 && rte->parent_plannerinfo_seq_no == 0))
		{
			int relid = get_rteid(rte->parent_plannerinfo_seq_no,
								rte->parent_rte_array_id, shs);
			if (relid > 0)
				*rels_used = bms_add_member(*rels_used, relid);
		}
		else
		{
			return false;
		}

		*rels_used = bms_del_member(*rels_used, x);
	}

	return true;
}

/*
 * Get relation number by plannerinfo_seq_no and rte_array_id
 */
static int
get_rteid(int plannerinfo_seq_no, int rte_array_id, SpmHintsState *shs)
{
	int i = 1;
	ListCell *lc;
	RangeTblEntry *rte;

	foreach(lc, shs->rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);

		if (rte->plannerinfo_seq_no == plannerinfo_seq_no &&
			rte->rte_array_id == rte_array_id)
			return i;
		i++;
	}
	return 0;
}

static void
spm_gen_planinfo(QueryDesc *queryDesc, SpmHintsState *shs)
{
	ExplainState *es = NewExplainState();
	es->analyze = false;
	es->verbose = true;
	es->buffers = false;
	es->timing = false;
	es->costs = false;
	es->format = EXPLAIN_FORMAT_TEXT;
	es->jstate = NULL;
	es->jumble_const = true;

	ExplainBeginOutput(es);
	ExplainPrintPlan(es, queryDesc);
	ExplainEndOutput(es);
	shs->plan_id = hash_any((const unsigned char*)es->str->data, es->str->len);
	shs->plan_str = es->str;
}
	
SpmHintsState *
spm_gen_hints(QueryDesc *queryDesc)
{
	ListCell	*cell;
	Bitmapset  *rels_used = NULL;
	SpmHintsState *shs = NewSpmHintsState();
	shs->rtable = queryDesc->plannedstmt->rtable;
	get_used_rels(queryDesc->planstate, &rels_used);
	shs->rtable_names = select_rtable_names_for_explain(shs->rtable, rels_used);
	create_plan_hints(queryDesc->planstate, NULL, shs);
	if (queryDesc->plannedstmt->guc_str)
	{
		shs->guc_str = queryDesc->plannedstmt->guc_str;
	}
	else
	{
		create_set_hints(shs);
	}
	
	shs->leadcxt =  (SpmLeadingContext *) palloc0(sizeof(SpmLeadingContext));
	shs->leadcxt->lead_str = makeStringInfo();
	shs->leadcxt->planstate_list = lappend(shs->leadcxt->planstate_list, queryDesc->planstate);
	shs->leadcxt->working = false;
	list_concat_unique_ptr(shs->leadcxt->planstate_list, shs->init_plan_list);
	foreach(cell, shs->leadcxt->planstate_list)
	{
		shs->leadcxt->plannerinfo_id = 0;
		
		create_leading_hint((PlanState *)lfirst(cell), shs);

		if (shs->leadcxt->working == true)
			appendStringInfo(shs->leadcxt->lead_str, ")\n");

		shs->leadcxt->working = false;
	}
	show_table_name_by_seq_no(shs);
	spm_gen_planinfo(queryDesc, shs);
	return shs;
}

/*
 * Generate parallel hint.
 */
static void
create_parallel_hint(PlanState *planstate, RangeTblEntry *rte, SpmHintsState *shs)
{
	StringInfo	seq_str = makeStringInfo();

	if (rte->parent_plannerinfo_seq_no == rte->plannerinfo_seq_no)
	{
		if (rte->parent_plannerinfo_seq_no <= 0)
			return;

		appendStringInfo(seq_str, "Parallel(%d.%d ",
						 rte->parent_plannerinfo_seq_no, rte->parent_rte_array_id);
		if (strstr(shs->parallel_str->data, seq_str->data) != NULL)
			return;
	}
	else
	{
		if (rte->plannerinfo_seq_no <= 0)
			return;

		appendStringInfo(seq_str, "Parallel(%d.%d ", 
					rte->plannerinfo_seq_no, rte->rte_array_id);
	}

	if (strstr(shs->parallel_str->data, seq_str->data) != NULL)
		return;

	if (planstate->plan->parallel_aware &&
		planstate->plan->parallel_num > 0)
	{
		appendStringInfo(shs->parallel_str, "%s%d hard) ",
						 seq_str->data, planstate->plan->parallel_num);
	}
	else
	{
		appendStringInfo(shs->parallel_str, "%s%d hard) ", seq_str->data, 0);
	}

	appendStringInfo(shs->parallel_str, "\n");
	return;
}

/*
 * Show the target relation of a scan or modify node
 */
static void
create_scan_hint(Plan *plan, Index rti, SpmHintsState *shs)
{
	/* Calculate length for sequence no as #.# */
	RangeTblEntry *rte_tmp;
	StringInfo	seq_no = makeStringInfo();
	rte_tmp = rt_fetch(rti, shs->rtable);

	/* If parent table in the same plannerinfo, it means current table is
	 * extended from base table. Use parent plannerinfo.
	 */
	if (rte_tmp->parent_plannerinfo_seq_no == rte_tmp->plannerinfo_seq_no)
	{
		if (rte_tmp->parent_plannerinfo_seq_no <= 0)
			return;

		appendStringInfo(seq_no, "(%d.%d)",
						rte_tmp->parent_plannerinfo_seq_no, rte_tmp->parent_rte_array_id);
	}
	else
	{
		if (rte_tmp->plannerinfo_seq_no <= 0)
			return;

		appendStringInfo(seq_no, "(%d.%d)", rte_tmp->plannerinfo_seq_no, rte_tmp->rte_array_id);
	}

	if (strstr(shs->table_hint_list->data, seq_no->data) != NULL)
		return;

	appendStringInfo(shs->table_hint_list, "%s ", seq_no->data);

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_CustomScan:
			appendStringInfo(shs->scan_str, "SeqScan%s\n", seq_no->data);
			break;
		case T_BitmapHeapScan:
		case T_BitmapIndexScan:
			appendStringInfo(shs->scan_str, "BitmapScan%s\n", seq_no->data);
			break;
		case T_IndexScan:
		{
			Oid indexid = ((IndexScan *) plan)->indexid;
			const char *indexname = explain_get_index_name(indexid);

			if (rte_tmp->parent_plannerinfo_seq_no == rte_tmp->plannerinfo_seq_no)
				appendStringInfo(shs->scan_str, "IndexScan(%d.%d %s)\n",
								rte_tmp->parent_plannerinfo_seq_no,
								rte_tmp->parent_rte_array_id,
								indexname);
			else
				appendStringInfo(shs->scan_str, "IndexScan(%d.%d %s)\n",
								rte_tmp->plannerinfo_seq_no,
								rte_tmp->rte_array_id,
								indexname);

			break;
		}
		case T_IndexOnlyScan:
		{
			Oid indexid = ((IndexOnlyScan *) plan)->indexid;
			const char *indexname = explain_get_index_name(indexid);

			if (rte_tmp->parent_plannerinfo_seq_no == rte_tmp->plannerinfo_seq_no)
				appendStringInfo(shs->scan_str, "IndexOnlyScan(%d.%d %s)\n",
								rte_tmp->parent_plannerinfo_seq_no,
								rte_tmp->parent_rte_array_id,
								indexname);
			else
				appendStringInfo(shs->scan_str, "IndexOnlyScan(%d.%d %s)\n",
								rte_tmp->plannerinfo_seq_no,
								rte_tmp->rte_array_id,
								indexname);
			break;
		}
		default:
			break;
	}
}

/*
 * get_used_rels -
 *    Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
static bool
get_used_rels(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_CteScan:
		{
			*rels_used = bms_add_member(*rels_used,
									((Scan *) plan)->scanrelid);

			return false;
		}
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
			break;
		case T_SubPlan:
			return false;
		default:
			break;
	}

	return planstate_tree_walker_get_rels(planstate, get_used_rels, rels_used);
}

/* 
 * return rte sequence numbers as plannerinfo_seq_no.rte_array_id by RTE.
 */
static char *
get_rte_seq_no(SpmHintsState *shs, Relids relids)
{
	int	x;
	int	first = true;
	RangeTblEntry *rte;
	StringInfo	seq_no = makeStringInfo();

	x = -1;
	while ((x = bms_next_member(relids, x)) >= 0)
	{
		rte = rt_fetch(x, shs->rtable);
		if (!first)
			appendStringInfo(seq_no, " %d.%d", rte->plannerinfo_seq_no, rte->rte_array_id);
		else
			appendStringInfo(seq_no, "%d.%d", rte->plannerinfo_seq_no, rte->rte_array_id);
		first = false;
	}
	return seq_no->data;
}

static DistributionHintType
check_distribution_type(Plan *plan)
{
	bool set_left_replication = false;
	bool set_right_replication = false;
	Plan *righ_node = NULL;

	if (plan->lefttree == NULL ||
		  plan->righttree == NULL)
		return NO_DISTRIBUTION_TYPE;

	if (nodeTag(plan->lefttree) == T_RemoteSubplan)
	{
		RemoteSubplan  *subplan = (RemoteSubplan *) plan->lefttree;

		switch (subplan->distributionType)
		{
			case LOCATOR_TYPE_HASH:
			case LOCATOR_TYPE_SHARD:
			{
				return DISTRIBUTED_TYPE;
			}
			case LOCATOR_TYPE_REPLICATED:
			case LOCATOR_TYPE_NONE:
			{
				set_left_replication = true;
				break;
			}
			default:
				break;
		}
	}

	/*
	 * Check for right hand is Hash with replication
	 */
	righ_node = plan->righttree;
	if (IsA(righ_node, Hash))
		righ_node = righ_node->lefttree;

	if (righ_node != NULL &&
		nodeTag(righ_node) == T_RemoteSubplan)
	{
		RemoteSubplan  *subplan = (RemoteSubplan *) righ_node;


		switch (subplan->distributionType)
		{
			case LOCATOR_TYPE_HASH:
			case LOCATOR_TYPE_SHARD:
			{
				return DISTRIBUTED_TYPE;
			}
			case LOCATOR_TYPE_REPLICATED:
			case LOCATOR_TYPE_NONE:
			{
				set_right_replication = true;
				break;
			}
			default:
				break;
		}
	}

	if (set_left_replication == true && set_right_replication == true)
		return NO_DISTRIBUTION_TYPE;

	if (set_left_replication == true)
		return LEFT_REPLICATE_TYPE;

	if (set_right_replication == true)
		return RIGHT_REPLICATE_TYPE;

	return NO_DISTRIBUTION_TYPE;
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
static void
create_subplan_hints(List *plans, List *ancestors, SpmHintsState *shs)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		SubPlan    *sp = sps->subplan;

		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
		if (bms_is_member(sp->plan_id, shs->hinted_subplans))
			continue;
		shs->hinted_subplans = bms_add_member(shs->hinted_subplans,
											  sp->plan_id);

		create_plan_hints(sps->planstate, ancestors, shs);

		shs->init_plan_list = lappend(shs->init_plan_list, sps->planstate);

	}
}

static void
create_memberplan_hints(List *plans, PlanState **planstates,
				   List *ancestors, SpmHintsState *shs)
{
	int			nplans = list_length(plans);
	int			j;

	for (j = 0; j < nplans; j++)
	{
		switch(nodeTag(planstates[j]))
		{
			case T_ExprState:
			case T_AggrefExprState:
			case T_WindowFuncExprState:
			case T_SetExprState:
			case T_SubPlanState:
			case T_AlternativeSubPlanState:
			case T_DomainConstraintState:
				continue;
			default:
				break;
		}
		create_plan_hints(planstates[j], ancestors, shs);

		shs->init_plan_list = lappend(shs->init_plan_list,planstates[j]);
	}
}

static void
create_appendplan_hints(AppendState *appendstate,
				   List *ancestors, SpmHintsState *shs)
{
	Append	   *append = (Append *)appendstate->ps.plan;
	PlanState **planstates = appendstate->appendplans;
	int			i, j;
	Bitmapset  *validsubplans = appendstate->validsubplans;
	ListCell   *lc;

	i = j = 0;
	foreach(lc, append->appendplans)
	{
		if (bms_is_member(i, validsubplans))
		{
			switch(nodeTag(planstates[j]))
			{
				case T_ExprState:
				case T_AggrefExprState:
				case T_WindowFuncExprState:
				case T_SetExprState:
				case T_SubPlanState:
				case T_AlternativeSubPlanState:
				case T_DomainConstraintState:
					j++;
					continue;
				default:
					break;
			}

			create_plan_hints(planstates[j], ancestors, shs);
			shs->init_plan_list = lappend(shs->init_plan_list,planstates[j]);
			j++;
		}
		i++;
	}
}

/*
 * Get real rte for current node. The subquery rte is signed as a join node,
 * this function is used to find the rte for the join node.
 *
 * Algorithm : Recursive find the parent of RTEs until RTE is in the disired plannerinfo.
 */
static RangeTblEntry *
check_real_relation_node(SpmHintsState *shs, RangeTblEntry * rte, int plannerinfo_id)
{
	ListCell   *lc;

	if (rte->plannerinfo_seq_no <= plannerinfo_id && rte->plannerinfo_seq_no > 0)
		return rte;

	while (rte->parent_plannerinfo_seq_no >= plannerinfo_id )
	{
		foreach(lc, shs->rtable)
		{
			RangeTblEntry *rte_tmp = (RangeTblEntry *) lfirst(lc);

			if (rte_tmp->plannerinfo_seq_no == rte->parent_plannerinfo_seq_no &&
				  rte_tmp->rte_array_id == rte->parent_rte_array_id)
			{
				rte = rte_tmp;
				break;
			}
		}
		
		if (lc == NULL || rte->plannerinfo_seq_no <= plannerinfo_id)
			break;
	}

	return rte;
}

/*
 * Get current plannerinfo_id.
 *
 * Algorithm : Find ancestor of all the tables
 * 1. Get first table plannerinfo_id to check whether ancestor of all tables
 * 2. If not, check ancestor of the first table
 * 3. Repeat step 2 until find a ancestor.
 */
static int
get_relids_plannerinfo_id(SpmHintsState *shs, Bitmapset *rels_used)
{
	int x = 0;
	int min_candidate;
	int candidate_ancestor;
	RangeTblEntry *rte;
	RangeTblEntry *rte_check;
	bool found_ancestor;
	int bms_next;

	if (bms_num_members(rels_used) <= 0)
		return 0;

	bms_next = bms_next_member(rels_used, 0);

	if (bms_next <=0)
		return 0;

	rte = rt_fetch(bms_next, shs->rtable);
	candidate_ancestor = rte->plannerinfo_seq_no;
	min_candidate = candidate_ancestor;

	while (candidate_ancestor > 0)
	{
		found_ancestor = true;
		x = 0;

		while ((x = bms_next_member(rels_used, x)) >= 0)
		{
			rte_check = rt_fetch(x, shs->rtable);

			min_candidate = Min(min_candidate, rte_check->plannerinfo_seq_no);

			if (rte_check->plannerinfo_seq_no == candidate_ancestor)
				continue;
			
			while (rte_check->plannerinfo_seq_no >= candidate_ancestor)
			{
				int rteid = 0;

				if (rte_check->plannerinfo_seq_no == candidate_ancestor)
					break;

				rteid = get_rteid(rte_check->parent_plannerinfo_seq_no,
								rte_check->parent_rte_array_id, shs);
				rte_check = rt_fetch(rteid, shs->rtable);
			}

			if (rte_check->plannerinfo_seq_no < candidate_ancestor)
			{
				found_ancestor = false;
				break;
			}
		}

		if (found_ancestor == true)
			break;
		
		candidate_ancestor = rte->parent_rte_array_id;
		if (candidate_ancestor <= 1)
			break;
	
		rte = rt_fetch(candidate_ancestor, shs->rtable);
	}

	if (candidate_ancestor < 0)
		candidate_ancestor = 1;
	else if (candidate_ancestor == 0)
		candidate_ancestor = min_candidate;

	return candidate_ancestor;
}

/*
 * Get rte under current PlanState
 */
static RangeTblEntry *
get_child_rte(PlanState *planstate, SpmHintsState *shs, int plannerinfo_id_input)
{
	RangeTblEntry *rte;
	Plan          *plan;

	if (planstate == NULL)
		return NULL;

	plan = planstate->plan;
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
			rte = rt_fetch(((Scan *) plan)->scanrelid, shs->rtable);
			return rte;
		case T_CteScan:
		{
			rte = rt_fetch(((Scan *) plan)->scanrelid, shs->rtable);
			return rte;
		}
		default:
		{
			Bitmapset *relids = NULL;
			int        x = -1;

			/*
			 * Get table in current query block
			 */
			get_used_rels(planstate, &relids);
			ShrinkRelids(shs, &relids, NULL, plannerinfo_id_input);

			while ((x = bms_next_member(relids, x)) >= 0)
			{
				rte = rt_fetch(x, shs->rtable);
				if (rte != NULL)
					return rte;
			}
		}
	}
	return NULL;
}

bool
planstate_tree_walker_hints(PlanState *planstate,
									bool (*walker) (),
									void *context)
{
	/* lefttree */
	if (outerPlanState(planstate))
	{
		if (walker(outerPlanState(planstate), context))
			return true;
	}

	/* righttree */
	if (innerPlanState(planstate))
	{
		if (walker(innerPlanState(planstate), context))
			return true;
	}

	return false;
}

static void
create_appendleading_hints(PlanState **plans, int nplans, Bitmapset **rels_used)
{
	int			i;

	for (i = 0; i < nplans; i++)
	{
		get_used_rels(plans[i], rels_used);
	}
}

/*
 * check_partitionwise_join_walker
 *	Walker for checking whether the join is partitionwise join.
 */
static bool
check_partitionwise_join_walker(PlanState *planstate, query_block_validation_context *context)
{
	if (planstate == NULL)
		return false;

	switch (nodeTag(planstate->plan))
	{
		case T_Append:
			/*
			 * Check whether there are partition wise join.
			 * If there are join under append with the same plannerinfo_id
			 * as parent query block, it should be partition wise join.
			 */
			context->under_append = true;
			planstate_tree_walker(planstate, check_partitionwise_join_walker, (void *) context);
			context->under_append = false;
			return false;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			if(context->plannerinfo_id == -1)
			{
				context->plannerinfo_id = ((Join*)planstate->plan)->plannerinfo_id;
			}
			if (context->under_append &&
				((Join*)planstate->plan)->plannerinfo_id != context->plannerinfo_id)
			{
				context->invalid = true;
				return true;
			}
			++context->join_nums;
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, check_partitionwise_join_walker, (void *) context);
}

/*
 * check_partitionwise_join
 *	Check whether the join is partitionwise join.
 */
static bool
check_partitionwise_join(PlanState *planstate, int plannerinfo_id)
{
	query_block_validation_context context;

	context.invalid = false;
	context.plannerinfo_id = plannerinfo_id;
	context.under_append = false;
	context.join_nums = 0;
	check_partitionwise_join_walker(planstate, &context);
	return (context.join_nums >= 2) && !context.invalid && (context.plannerinfo_id > 0);
}

/* 
 * For creating Leading hint on the same plannerinfo.
 *
 * Algorithm : 
 * 1. If RTE node, return plannerinfo_seq_no.rte_array_id directly;
 * 2. If join node is in the same plannerinfo, contunue to walk tree;
 * 3. If join node is not in the same plannerinfo;
 *   3.1 Find RTE for current join node and return plannerinfo_seq_no.rte_array_id;
 *   3.2 Add current join node to plannerinfo list for further generation;
 */
static bool
create_leading_hint(PlanState *planstate, SpmHintsState *shs)
{
	Plan	   *plan = planstate->plan;
	RangeTblEntry *rte;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_WorkTableScan:
		case T_CteScan:
		case T_ForeignScan:
		case T_CustomScan:
			if (shs->leadcxt->working == false)
				break;
			if (nodeTag(plan) == T_ForeignScan && ((Scan *) plan)->scanrelid <=0)
				break;
			elog(DEBUG1, "seqscan: %u", ((Scan *) plan)->scanrelid);
			rte = rt_fetch(((Scan *) plan)->scanrelid, shs->rtable);
			rte = check_real_relation_node(shs, rte, shs->leadcxt->plannerinfo_id);

			if (rte->parent_plannerinfo_seq_no == shs->leadcxt->plannerinfo_id &&
				rte->parent_plannerinfo_seq_no == rte->plannerinfo_seq_no)
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", 
								rte->parent_plannerinfo_seq_no, rte->parent_rte_array_id);
			else
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", 
								rte->plannerinfo_seq_no, rte->rte_array_id);
			break;
		case T_IndexScan:
		case T_IndexOnlyScan:
			if (shs->leadcxt->working == false)
				break;
			elog(DEBUG1, "indscan: %u", ((Scan *) plan)->scanrelid);
			rte = rt_fetch(((Scan *) plan)->scanrelid, shs->rtable);
			rte = check_real_relation_node(shs, rte, shs->leadcxt->plannerinfo_id);

			if (rte->parent_plannerinfo_seq_no == shs->leadcxt->plannerinfo_id &&
				rte->parent_plannerinfo_seq_no == rte->plannerinfo_seq_no)
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", 
								rte->parent_plannerinfo_seq_no, rte->parent_rte_array_id);
			else
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", 
								rte->plannerinfo_seq_no, rte->rte_array_id);
			break;
		case T_HashJoin:
			if (bms_is_member(((Join*)plan)->plannerinfo_id, shs->invalid_query_block) &&
				shs->leadcxt->working == false)
				break;

			if (shs->leadcxt->working == false)
			{
				shs->leadcxt->plannerinfo_id = ((Join*)plan)->plannerinfo_id;

				if (shs->leadcxt->plannerinfo_id <= 0)
					break;

				if (list_member_int(shs->leadcxt->plannerinfo_id_list,
				                    shs->leadcxt->plannerinfo_id) == true)
					return true;

				shs->leadcxt->plannerinfo_id_list = lappend_int(shs->leadcxt->plannerinfo_id_list,
														shs->leadcxt->plannerinfo_id);

				appendStringInfo(shs->leadcxt->lead_str, "Leading(");
				shs->leadcxt->working = true;
			}

			if (((Join*)plan)->plannerinfo_id != shs->leadcxt->plannerinfo_id)
			{
				rte = get_child_rte(planstate, shs, ((Join*)plan)->plannerinfo_id);
				if (rte != NULL)
				{
					rte = check_real_relation_node(shs, rte, shs->leadcxt->plannerinfo_id);
					appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", rte->plannerinfo_seq_no,
					                 rte->rte_array_id);
					if (llast(shs->leadcxt->planstate_list) != planstate)
						shs->leadcxt->planstate_list =
							lappend(shs->leadcxt->planstate_list, planstate);
					break;
				}
			}
			elog(DEBUG1, "HJ(");
			appendStringInfo(shs->leadcxt->lead_str, "(");
			planstate_tree_walker_hints(planstate, create_leading_hint, shs);
			appendStringInfo(shs->leadcxt->lead_str, ")");
			elog(DEBUG1, ")");
			break;
		case T_NestLoop:
			if (bms_is_member(((Join*)plan)->plannerinfo_id, shs->invalid_query_block) &&
				shs->leadcxt->working == false)
				break;

			if (shs->leadcxt->working == false)
			{
				shs->leadcxt->plannerinfo_id = ((Join*)plan)->plannerinfo_id;

				if (shs->leadcxt->plannerinfo_id <= 0)
					break;

				if (list_member_int(shs->leadcxt->plannerinfo_id_list,
				                    shs->leadcxt->plannerinfo_id) == true)
					return true;

				shs->leadcxt->plannerinfo_id_list =
					lappend_int(shs->leadcxt->plannerinfo_id_list, shs->leadcxt->plannerinfo_id);

				appendStringInfo(shs->leadcxt->lead_str, "Leading(");
				shs->leadcxt->working = true;
			}

			if (((Join*)plan)->plannerinfo_id != shs->leadcxt->plannerinfo_id)
			{
				rte = get_child_rte(planstate, shs, ((Join*)plan)->plannerinfo_id);
				if (rte != NULL)
				{
					rte = check_real_relation_node(shs, rte, shs->leadcxt->plannerinfo_id);
					appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", rte->plannerinfo_seq_no,
					                 rte->rte_array_id);
					if(llast(shs->leadcxt->planstate_list) != planstate)
						shs->leadcxt->planstate_list = lappend(shs->leadcxt->planstate_list, planstate);
					break;
				}
			}
			elog(DEBUG1, "NL(");
			appendStringInfo(shs->leadcxt->lead_str, "(");
			planstate_tree_walker_hints(planstate, create_leading_hint, shs);
			appendStringInfo(shs->leadcxt->lead_str, ")");
			elog(DEBUG1, ")");
			break;
		case T_MergeJoin:
			if (bms_is_member(((Join*)plan)->plannerinfo_id, shs->invalid_query_block) &&
				shs->leadcxt->working == false)
				break;

			if (shs->leadcxt->working == false)
			{
				shs->leadcxt->plannerinfo_id = ((Join*)plan)->plannerinfo_id;

				if (shs->leadcxt->plannerinfo_id <= 0)
					break;

				if (list_member_int(shs->leadcxt->plannerinfo_id_list,
				                    shs->leadcxt->plannerinfo_id) == true)
					return true;

				shs->leadcxt->plannerinfo_id_list =
					lappend_int(shs->leadcxt->plannerinfo_id_list, shs->leadcxt->plannerinfo_id);

				appendStringInfo(shs->leadcxt->lead_str, "Leading(");
				shs->leadcxt->working = true;
			}

			if (((Join*)plan)->plannerinfo_id != shs->leadcxt->plannerinfo_id)
			{
				rte = get_child_rte(planstate, shs, ((Join*)plan)->plannerinfo_id);
				if (rte != NULL)
				{
					rte = check_real_relation_node(shs, rte, shs->leadcxt->plannerinfo_id);
					appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", rte->plannerinfo_seq_no,
					                 rte->rte_array_id);
					if (llast(shs->leadcxt->planstate_list) != planstate)
						shs->leadcxt->planstate_list =
							lappend(shs->leadcxt->planstate_list, planstate);
					break;
				}
			}
			elog(DEBUG1, "MJ(");
			appendStringInfo(shs->leadcxt->lead_str, "(");
			planstate_tree_walker_hints(planstate, create_leading_hint, shs);
			appendStringInfo(shs->leadcxt->lead_str, ")");
			elog(DEBUG1, ")");
			break;
		case T_Append:
		{
			Bitmapset  *relids = NULL;
			AppendState *append = (AppendState *)planstate;
			bool pwj = check_partitionwise_join(planstate, shs->leadcxt->plannerinfo_id);

			if (shs->leadcxt->working == false)
				break;

			create_appendleading_hints(append->appendplans, append->as_nplans, &relids);
			ShrinkRelids(shs, &relids, NULL, shs->leadcxt->plannerinfo_id);
			if (pwj)
			{
				appendStringInfo(shs->leadcxt->lead_str, "(");
				appendStringInfo(shs->leadcxt->lead_str, "%s ", get_rte_seq_no(shs, relids));
				appendStringInfo(shs->leadcxt->lead_str, ")");
			}
			else
			{
				appendStringInfo(shs->leadcxt->lead_str, "%s ", get_rte_seq_no(shs, relids));
			}
		}
			break;
		case T_MergeAppend:
		{
			Bitmapset  *relids = NULL;
			MergeAppendState *mergeappend = (MergeAppendState *)planstate;
			bool pwj = check_partitionwise_join(planstate, shs->leadcxt->plannerinfo_id);

			if (shs->leadcxt->working == false)
				break;

			create_appendleading_hints(mergeappend->mergeplans, mergeappend->ms_nplans, &relids);
			ShrinkRelids(shs, &relids, NULL, shs->leadcxt->plannerinfo_id);
			if (pwj)
			{
				appendStringInfo(shs->leadcxt->lead_str, "(");
				appendStringInfo(shs->leadcxt->lead_str, "%s ", get_rte_seq_no(shs, relids));
				appendStringInfo(shs->leadcxt->lead_str, ")");
			}
			else
			{
				appendStringInfo(shs->leadcxt->lead_str, "%s ", get_rte_seq_no(shs, relids));
			}
		}
			break;
		case T_SubqueryScan:
			if (shs->leadcxt->working == false)
				break;

			rte = rt_fetch(((Scan *)plan)->scanrelid, shs->rtable);

			if (rte->parent_plannerinfo_seq_no == shs->leadcxt->plannerinfo_id &&
				rte->parent_plannerinfo_seq_no == rte->plannerinfo_seq_no)
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", 
								rte->parent_plannerinfo_seq_no, rte->parent_rte_array_id);
			else
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", 
								rte->plannerinfo_seq_no, rte->rte_array_id);
			break;
		case T_ConnectBy:
		{
			if (shs->leadcxt->plannerinfo_id == 0)
				break;

			rte = get_child_rte(planstate, shs, shs->leadcxt->plannerinfo_id);

			if (rte != NULL)
			{
				appendStringInfo(shs->leadcxt->lead_str, "%d.%d ", rte->plannerinfo_seq_no,
										rte->rte_array_id);
			}

			break;
		}
		default:
			planstate_tree_walker_hints(planstate, create_leading_hint, shs);
	}
	return false;
}

static bool
check_invalid_hint_walker(PlanState *planstate, query_block_validation_context *context)
{
	if (planstate == NULL)
		return false;

	switch (nodeTag(planstate->plan))
	{
		case T_Result:
			if (planstate->plan->lefttree == NULL)
			{
				context->invalid = true;
				return true;
			}
			break;
		case T_Append:
			/*
			 * Check whether there are partition wise join.
			 * If there are join under append with the same plannerinfo_id
			 * as parent query block, it should be partition wise join.
			 */
			context->under_append = true;
			planstate_tree_walker(planstate, check_invalid_hint_walker, (void *) context);
			context->under_append = false;
			return false;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			if (context->under_append &&
				((Join*)planstate->plan)->plannerinfo_id == context->plannerinfo_id)
			{
				context->invalid = true;
				return true;
			}
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, check_invalid_hint_walker, (void *) context);
}

static bool
check_invalid_hint(PlanState *planstate, int plannerinfo_id)
{
	query_block_validation_context context;

	context.invalid = false;
	context.plannerinfo_id = plannerinfo_id;
	context.under_append = false;

	check_invalid_hint_walker(planstate, &context);
	return context.invalid;
}

/*
 * CreateExplainHints -
 *	  Appends a description of a plan tree to es->str *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent PlanState nodes, most-closely-nested first.
 * These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 */
static void
create_plan_hints(PlanState *planstate, List *ancestors,
						SpmHintsState *shs)
{
	Plan	   *plan = planstate->plan;
	bool		haschildren;
	DistributionHintType distribution_type;
	StringInfo	tmp_relnames = makeStringInfo();

	/*
	 * Create join and scan hints.
	 */
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
			{
				if (nodeTag(plan) == T_ForeignScan || nodeTag(plan) == T_CustomScan)
				{
					if (((Scan *) plan)->scanrelid > 0)
					{
						RangeTblEntry *rte;
						rte = rt_fetch(((Scan *) plan)->scanrelid, shs->rtable);
						create_parallel_hint(planstate, rte, shs);
						create_scan_hint(plan, ((Scan *) plan)->scanrelid, shs);
					}
				}
				else
				{
					if (((Scan *) plan)->scanrelid > 0)
					{
						RangeTblEntry *rte;
						rte = rt_fetch(((Scan *) plan)->scanrelid, shs->rtable);
						create_parallel_hint(planstate, rte, shs);
						create_scan_hint(plan, ((Scan *) plan)->scanrelid, shs);
					}
				}
			}
			break;
		case T_RemoteQuery:
			if (((RemoteQuery *) plan)->fqs_mode == FQS_MODE_FQS_PLANER)
				return;
			break;
		case T_RemoteSubplan:
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				Bitmapset  *relids = NULL;
				char	   *rte_seq_no = NULL;

				shs->appear_join = bms_add_member(shs->appear_join,
												  ((Join*)plan)->plannerinfo_id);

				if (bms_is_member(((Join*)plan)->plannerinfo_id, shs->invalid_query_block))
				{
					break;	
				}
				
				if (check_invalid_hint(planstate, ((Join*)plan)->plannerinfo_id))
				{
					shs->invalid_query_block = bms_add_member(shs->invalid_query_block,
															  ((Join*)plan)->plannerinfo_id);
					break;
				}

				get_used_rels(planstate, &relids);

				if (ShrinkRelids(shs, &relids, NULL, ((Join*) plan)->plannerinfo_id) == false)
				{
					shs->invalid_query_block = bms_add_member(shs->invalid_query_block,
															  ((Join*)plan)->plannerinfo_id);
					break;					
				}

				if (bms_num_members(relids) <= 1)
				{
					shs->invalid_query_block = bms_add_member(shs->invalid_query_block,
															  ((Join*)plan)->plannerinfo_id);
					break;
				}

				rte_seq_no = get_rte_seq_no(shs, relids);
				appendStringInfo(tmp_relnames, "(%s)", rte_seq_no);

				if (strstr(shs->join_str->data, tmp_relnames->data) != NULL)
					break;

				if (nodeTag(plan) == T_NestLoop)
					appendStringInfo(shs->join_str, "%s", "NestLoop");
				else if (nodeTag(plan) == T_MergeJoin)
					appendStringInfo(shs->join_str, "%s", "MergeJoin");
				else if (nodeTag(plan) == T_HashJoin)
					appendStringInfo(shs->join_str, "%s", "HashJoin");

				appendStringInfo(shs->join_str, "%s ", tmp_relnames->data);
				appendStringInfo(shs->join_str, "\n");

				if (nodeTag(plan) == T_MergeJoin)
				{
					if (((MergeJoin *)plan)->is_not_full)
						appendStringInfo(shs->mergejoin_key_str, "MergeJoinKey(%s NotFull)\n", rte_seq_no);
					else
						appendStringInfo(shs->mergejoin_key_str, "MergeJoinKey(%s Full)\n", rte_seq_no);
				}

				/*
				 * Generate distribution hint
				 */
				distribution_type = check_distribution_type(plan);

				if (distribution_type != NO_DISTRIBUTION_TYPE)
				{
					Bitmapset *left_relids = NULL;
					Bitmapset *right_relids = NULL;
					char *left_str = NULL;
					char *right_str = NULL;

					get_used_rels(planstate->lefttree, &left_relids);
					get_used_rels(planstate->righttree, &right_relids);
					ShrinkRelids(shs, &left_relids, &right_relids, ((Join*) plan)->plannerinfo_id);

					if (left_relids == NULL || bms_num_members(left_relids) <= 0)
						break;

					left_str = get_rte_seq_no(shs, left_relids);

					if (left_str == NULL || strlen(left_str) == 0)
						break;

					ShrinkRelids(shs, &right_relids, &left_relids, ((Join*) plan)->plannerinfo_id);

					if (right_relids == NULL || bms_num_members(right_relids) <= 0)
						break;

					right_str = get_rte_seq_no(shs, right_relids);

					if (right_str == NULL || strlen(right_str) == 0)
						break;

					if (distribution_type == DISTRIBUTED_TYPE)
						appendStringInfo(
							shs->distribution_str, "Distribution(Left( %s ) Right( %s ))", left_str, right_str);

					if (distribution_type == LEFT_REPLICATE_TYPE)
						appendStringInfo(
							shs->distribution_str, "Distribution(Left(Replicate %s ) Right( %s ))", left_str, right_str);

					if (distribution_type == RIGHT_REPLICATE_TYPE)
						appendStringInfo(
							shs->distribution_str, "Distribution(Left( %s ) Right(Replicate %s ))", left_str, right_str);

					appendStringInfo(shs->distribution_str, "\n");
				}
			}
			break;
		case T_Agg:
		case T_Unique:
		case T_Group:
			{
				spm_explain_agg(planstate, shs);
			}
			break;
		default:
			break;
	}

	/* Get ready to display the child plans */
	haschildren = planstate->initPlan ||
		outerPlanState(planstate) ||
		innerPlanState(planstate) ||
		IsA(plan, ModifyTable) ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(planstate, CustomScanState) &&
		 ((CustomScanState *) planstate)->custom_ps != NIL) ||
		planstate->subPlan;
	if (haschildren)
	{
		/* Pass current PlanState as head of ancestors list for children */
		ancestors = lcons(planstate, ancestors);
	}

	/* Handle initplan */
	if (planstate->initPlan)
		create_subplan_hints(planstate->initPlan, ancestors, shs);

	/* Handle subplan */
	if (planstate->subPlan)
		create_subplan_hints(planstate->subPlan, ancestors, shs);

	/* lefttree */
	if (outerPlanState(planstate))
		create_plan_hints(outerPlanState(planstate), ancestors, shs);

	/* righttree */
	if (innerPlanState(planstate))
		create_plan_hints(innerPlanState(planstate), ancestors, shs);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_Append:
			create_appendplan_hints((AppendState *) planstate,
							   ancestors, shs);
			break;
		case T_MergeAppend:
			create_memberplan_hints(((MergeAppend *) plan)->mergeplans,
							   ((MergeAppendState *) planstate)->mergeplans,
							   ancestors, shs);
			break;
		case T_BitmapAnd:
			create_memberplan_hints(((BitmapAnd *) plan)->bitmapplans,
							   ((BitmapAndState *) planstate)->bitmapplans,
							   ancestors, shs);
			break;
		case T_BitmapOr:
			create_memberplan_hints(((BitmapOr *) plan)->bitmapplans,
							   ((BitmapOrState *) planstate)->bitmapplans,
							   ancestors, shs);
			break;
		case T_SubqueryScan:
			create_plan_hints(((SubqueryScanState *) planstate)->subplan, ancestors, shs);
			shs->init_plan_list =
				lappend(shs->init_plan_list, ((SubqueryScanState *) planstate)->subplan);
			break;
		case T_CustomScan:
			break;
		default:
			break;
	}
}

/*
 * check_duplicate_rte
 *	Check whether there are duplicated tables.
 */
static bool
check_duplicate_rte(List *rtables, RangeTblEntry *rte)
{
	RangeTblEntry *rte_temp;
	ListCell   *lc;

	foreach(lc, rtables)
	{
		rte_temp = (RangeTblEntry *) lfirst(lc);
		if (rte->plannerinfo_seq_no == rte_temp->plannerinfo_seq_no &&
			rte->rte_array_id == rte_temp->rte_array_id)
		{
			if (rte == rte_temp)
				return false;
			else
				return true;
		}
	}

	return false;
}

/*
 * Get target relation name of a scan
 */
static char *
get_target_relname(Index rti, SpmHintsState *shs)
{
	RangeTblEntry *rte;
	char	   *refname;

	elog(DEBUG1, "    #get_target_relname#");

	rte = rt_fetch(rti, shs->rtable);
	refname = (char *) list_nth(shs->rtable_names, rti - 1);
	if (refname == NULL)
		refname = rte->eref->aliasname;

	return refname;
}

/*
 * show_table_name_by_seq_no
 *	Generate table name list.
 */
static void
show_table_name_by_seq_no(SpmHintsState *shs)
{
	RangeTblEntry *rte;
	ListCell   *lc;
	int i = 1;

	foreach(lc, shs->rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);

		if (!check_duplicate_rte(shs->rtable, rte))
		{
			appendStringInfo(shs->table_seq_no_str, "%d.%d\t%d.%d\t%s\n",
							rte->plannerinfo_seq_no, rte->rte_array_id, 
							rte->parent_plannerinfo_seq_no, rte->parent_rte_array_id, 
							get_target_relname(i, shs));
		}
		i++;
	}
	return;
}

ExplainStmt *
TransformToExplanHintStmt(Query *query)
{
	ExplainStmt *new_query = NULL;

	new_query= makeNode(ExplainStmt);
	new_query->query = (Node *) query;
	new_query->options = list_make1((void *) makeDefElem("spm",  (Node *) makeString("capture"), 1));

	return new_query;
}

void
SPMExecExplain(ExplainStmt *new_query)
{
	ParseState *pstate;

	if (!SPM_ENABLED)
		return;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = pre_spmplan->sql;

	ExplainQuery(pstate, new_query, pre_spmplan->sql, NULL,
						 NULL, None_Receiver);
}

/*
 * spm_explain_agg
 *	Generate Agg and Distinct hint.
 */
static void
spm_explain_agg(PlanState *planstate, SpmHintsState *shs)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_Agg:
			if (bms_is_member(((Agg*) plan)->plannerinfo_seq_no, shs->appear_join))
				break;

			switch (((Agg*) plan)->agg_type)
			{
				case FUN_GROUP_BY:
					if (bms_is_member(((Agg*) plan)->plannerinfo_seq_no, shs->appear_join))
						break;

					if (bms_is_member(((Agg*) plan)->plannerinfo_seq_no, shs->gen_groupby))
						break;

					if (((Agg*) plan)->aggstrategy == AGG_HASHED)
					{
						appendStringInfo(shs->aggpath_str, "AggPath(%d GroupBy Hash)\n",
														((Agg*) plan)->plannerinfo_seq_no);

						shs->gen_groupby = bms_add_member(shs->gen_groupby,
												  ((Agg*) plan)->plannerinfo_seq_no);
					}
					else if (((Agg*) plan)->aggstrategy == AGG_SORTED)
					{
						appendStringInfo(shs->aggpath_str, "AggPath(%d GroupBy Sort)\n",
														((Agg*) plan)->plannerinfo_seq_no);

						shs->gen_groupby = bms_add_member(shs->gen_groupby,
												  ((Agg*) plan)->plannerinfo_seq_no);
					}

					break;
				case FUN_DISTINCT:
					if (bms_is_member(((Agg*) plan)->plannerinfo_seq_no, shs->appear_join))
						break;

					if (bms_is_member(((Agg*) plan)->plannerinfo_seq_no, shs->gen_distinct))
						break;

					if (((Agg*) plan)->aggstrategy == AGG_HASHED)
					{
						appendStringInfo(shs->aggpath_str, "AggPath(%d Distinct Hash)\n",
										((Agg*) plan)->plannerinfo_seq_no);

						shs->gen_distinct = bms_add_member(shs->gen_distinct,
												  ((Agg*) plan)->plannerinfo_seq_no);
					}
					else if (((Agg*) plan)->aggstrategy == AGG_SORTED)
					{
						appendStringInfo(shs->aggpath_str, "AggPath(%d Distinct Sort)\n",
														((Agg*) plan)->plannerinfo_seq_no);

						shs->gen_distinct = bms_add_member(shs->gen_distinct,
												  ((Agg*) plan)->plannerinfo_seq_no);
					}

					break;
				default:
					break;
			}
			break;
		case T_Group:
			{
				if (bms_is_member(((Group*)plan)->plannerinfo_seq_no, shs->appear_join))
					break;

				if (bms_is_member(((Group*)plan)->plannerinfo_seq_no, shs->gen_groupby))
					break;

				if (((Group*)plan)->plan.lefttree != NULL &&
					!IsA(((Group*)plan)->plan.lefttree, RemoteSubplan))
				{
					appendStringInfo(shs->aggpath_str, "AggPath(%d GroupBy Sort)\n",
													((Group*)plan)->plannerinfo_seq_no);

					shs->gen_groupby = bms_add_member(shs->gen_groupby,
												  ((Group*)plan)->plannerinfo_seq_no);
				}

				break;
			}
		case T_Unique:
				if (bms_is_member(((Unique*)plan)->plannerinfo_seq_no, shs->appear_join))
					break;

				if (bms_is_member(((Unique*)plan)->plannerinfo_seq_no, shs->gen_distinct))
					break;

				if (((Unique*)plan)->plan.lefttree != NULL &&
					!IsA(((Unique*)plan)->plan.lefttree, RemoteSubplan))
				{
					appendStringInfo(shs->aggpath_str, "AggPath(%d Distinct Sort)\n",
													((Unique*)plan)->plannerinfo_seq_no);

					shs->gen_distinct = bms_add_member(shs->gen_distinct,
												  ((Unique*)plan)->plannerinfo_seq_no);
				}

			break;
		default:
			break;
	}
	return;
}

static void
create_set_hints(SpmHintsState *shs)
{
	struct config_generic *record;
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;

	if (spm_set_hint == NULL ||
		spm_set_hint[0] == '\0')
		return;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(spm_set_hint);

	/* Parse string into list of filename paths */
	if (!SplitDirectoriesString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		list_free_deep(elemlist);
		pfree(rawstring);
		return;
	}

	foreach(l, elemlist)
	{
		/* Note that filename was already canonicalized */
		char *gucname = (char *) lfirst(l);
		const char *val;

		record = get_guc_value(gucname);

		if (record)
		{
			val = getGucVal(record, true);
			appendStringInfo(shs->guc_str, "Set(%s %s)\n", gucname, val);
		}
	}

	list_free_deep(elemlist);
	pfree(rawstring);
}

Datum
sql_identity(PG_FUNCTION_ARGS)
{
	text       *sql_text = PG_GETARG_TEXT_P(0);
	bool        for_prepare = PG_GETARG_BOOL(1);
	bool        strip_utility = PG_GETARG_BOOL(2);
	char       *sql_src = NULL;
	Query      *query = NULL;
	Query      *new_jump_query = NULL;
	const char *jump_query_str;
	bool        snapshot_set = false;
	int         spi_conn_ret;
	uint32      queryId = 0;

	sql_src = text_to_cstring(sql_text);

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		snapshot_set = true;
	}

	if ((spi_conn_ret = SPI_connect()) < 0)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("SPI connect failure - returned %d", spi_conn_ret)));

	if (ORA_MODE)
		SPI_set_and_check_pl_conflict(ORA_PLSQL);

	query = SPI_get_query(sql_src, for_prepare);

	if (!query)
		goto FAIL_END;

	new_jump_query = query;
	jump_query_str = sql_src;

	while (strip_utility && new_jump_query && new_jump_query->commandType == CMD_UTILITY && new_jump_query->utilityStmt)
	{
		new_jump_query = (Query *) new_jump_query->utilityStmt;
		if (IsA(new_jump_query, ExplainStmt))
		{
			ExplainStmt *stmt = (ExplainStmt *) new_jump_query;

			Assert(IsA(stmt->query, Query));
			new_jump_query = (Query *) stmt->query;
		}

		if (IsA(new_jump_query, DeclareCursorStmt))
		{
			DeclareCursorStmt *stmt = (DeclareCursorStmt *) new_jump_query;
			Query             *query = (Query *) stmt->query;

			/* the target must be CMD_SELECT in this case */
			Assert(IsA(query, Query) && query->commandType == CMD_SELECT);
			new_jump_query = query;
		}

		if (IsA(new_jump_query, CreateTableAsStmt))
		{
			CreateTableAsStmt *stmt = (CreateTableAsStmt *) new_jump_query;

			Assert(IsA(stmt->query, Query));
			new_jump_query = (Query *) stmt->query;

			/* strip out the top-level query for further processing */
			if (new_jump_query->commandType == CMD_UTILITY && new_jump_query->utilityStmt != NULL)
				new_jump_query = (Query *) new_jump_query->utilityStmt;
		}

		if (IsA(new_jump_query, ExecuteStmt))
		{
			/*
			 * Use the prepared query for EXECUTE. The Query for jumble
			 * also replaced with the corresponding one.
			 */
			ExecuteStmt       *stmt = (ExecuteStmt *) new_jump_query;
			PreparedStatement *entry;

			/*
			 * Silently ignore nonexistent prepared statements.  This may
			 * happen for EXECUTE within a function definition.  Otherwise the
			 * execution will fail anyway.
			 */
			entry = FetchPreparedStatement(stmt->name, false);

			if (entry && entry->plansource->is_valid)
			{
				jump_query_str = (char *) entry->plansource->query_string;
				new_jump_query = (Query *) linitial(entry->plansource->query_list);
			}
			else
			{
				new_jump_query = NULL;
			}
		}
	}

	if (new_jump_query && (!IsA(new_jump_query, Query) || new_jump_query->utilityStmt != NULL))
		new_jump_query = NULL;

	if (new_jump_query != NULL && new_jump_query != query)
	{
		JumbleQuery(new_jump_query, jump_query_str);
		query = new_jump_query;
	}
	else if (query->queryId == 0)
		JumbleQuery(query, sql_src);

	queryId = query->queryId;

FAIL_END:
	SPI_finish();
	if (snapshot_set)
		PopActiveSnapshot();

	PG_RETURN_INT32(queryId);
}
