/*-------------------------------------------------------------------------
 *
 * planner.c
 *	  The query optimizer external interface.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planner.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_inherits_fn.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "jit/jit.h"
#include "lib/bipartite_match.h"
#include "lib/knapsack.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "optimizer/spm.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_param.h"
#include "rewrite/rewriteManip.h"
#include "storage/dsm_impl.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "commands/prepare.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#endif
#ifdef __OPENTENBASE__
#include "optimizer/distribution.h"
#endif
#ifdef __OPENTENBASE_C__
#include "executor/execDispatchFragment.h"
#include "executor/execFragment.h"
#include "executor/execMerge.h"
#include "optimizer/memctl.h"
#include "parser/parse_agg.h"
#include "postmaster/bgworker_internals.h"
#include "utils/resgroup.h"
#endif
#ifdef _PG_ORCL_
#include "catalog/heap.h"
#include "commands/sequence.h"
#include "utils/guc.h"
#endif

/* GUC parameters */
double		cursor_tuple_fraction = DEFAULT_CURSOR_TUPLE_FRACTION;
int			force_parallel_mode = FORCE_PARALLEL_OFF;
/* bool		parallel_leader_participation = true; */
int			optimize_distinct_subquery = 1;
int			union_limit_pushdown = 10000;

/* Hook for plugins to get control in planner() */
planner_hook_type planner_hook = NULL;

/* Hook for plugins to get control when grouping_planner() plans upper rels */
create_upper_paths_hook_type create_upper_paths_hook = NULL;

set_agg_rows_hook_type set_agg_rows_hook = NULL;
set_agg_path_hook_type set_agg_path_hook = NULL;
set_merge_join_key_hook_type set_merge_join_key_hook = NULL;

#ifdef __OPENTENBASE_C__
bool window_optimizer = false;
bool group_optimizer = false;
bool distinct_optimizer = false;
bool distinct_with_hash = false;

/* Late Materialization GUCs */
double late_materialize_threshold = 0;
#endif

/* Expression kind codes for preprocess_expression */
#define EXPRKIND_QUAL				0
#define EXPRKIND_TARGET				1
#define EXPRKIND_RTFUNC				2
#define EXPRKIND_RTFUNC_LATERAL		3
#define EXPRKIND_VALUES				4
#define EXPRKIND_VALUES_LATERAL		5
#define EXPRKIND_LIMIT				6
#define EXPRKIND_APPINFO			7
#define EXPRKIND_PHV				8
#define EXPRKIND_TABLESAMPLE		9
#define EXPRKIND_ARBITER_ELEM		10
#define EXPRKIND_TABLEFUNC			11
#define EXPRKIND_TABLEFUNC_LATERAL	12

/* Passthrough data for standard_qp_callback */
typedef struct
{
	List	   *activeWindows;	/* active windows, if any */
	List	   *groupClause;	/* overrides parse->groupClause */
} standard_qp_extra;

/*
 * Various flags indicating what kinds of grouping are possible.
 *
 * GROUPING_CAN_USE_SORT should be set if it's possible to perform
 * sort-based implementations of grouping.  When grouping sets are in use,
 * this will be true if sorting is potentially usable for any of the grouping
 * sets, even if it's not usable for all of them.
 *
 * GROUPING_CAN_USE_HASH should be set if it's possible to perform
 * hash-based implementations of grouping.
 *
 * GROUPING_CAN_PARTIAL_AGG should be set if the aggregation is of a type
 * for which we support partial aggregation (not, for example, grouping sets).
 * It says nothing about parallel-safety or the availability of suitable paths.
 */
#define GROUPING_CAN_USE_SORT       0x0001
#define GROUPING_CAN_USE_HASH       0x0002
#define GROUPING_CAN_PARTIAL_AGG	0x0004

/*
 * Data specific to grouping sets
 */

typedef struct
{
	List	   *rollups;
	List	   *hash_sets_idx;
	double		dNumHashGroups;
	bool		any_hashable;
	Bitmapset  *unsortable_refs;
	Bitmapset  *unhashable_refs;
	List	   *unsortable_sets;
	int		   *tleref_to_colnum_map;
} grouping_sets_data;

typedef struct
{
	List   *targetlist;
	bool	replace_var;
	bool	contain_sublink;
	bool	contain_unsupport_var;
	int		num_check_var;
	int		num_qualify_var;
	int		varlevelsup;
	List   *lack_var_list;
} related_targetlist_sublink_context;

char *fqs_mode_string[] = {"fqs_mode_fqs_planer", "fqs_mode_std_planer"};

typedef struct
{
	PlannerInfo *root;
	List    *exprs;
	List    *params;
	Param   *path_param;
	bool     reset_cache;
	bool     qual_again;
	int8	 cb_flags;
} replace_prior_param_context;

/* Local functions */
static Node *preprocess_expression(PlannerInfo *root, Node *expr, int kind);
static void preprocess_qual_conditions(PlannerInfo *root, Node *jtnode);
static void grouping_planner(PlannerInfo *root, double tuple_fraction);
static grouping_sets_data *preprocess_grouping_sets(PlannerInfo *root);
static List *remap_to_groupclause_idx(List *groupClause, List *gsets,
						 int *tleref_to_colnum_map);
static double preprocess_limit(PlannerInfo *root,
				 double tuple_fraction,
				 int64 *offset_est, int64 *count_est, bool *ora_nulllimits);
static bool limit_needed(Query *parse);
static void remove_useless_groupby_columns(PlannerInfo *root);
static void pushdown_union_orderby_limit(PlannerInfo *root);
static List *preprocess_groupclause(PlannerInfo *root, List *force);
static List *extract_rollup_sets(List *groupingSets);
static List *reorder_grouping_sets(List *groupingSets, List *sortclause);
static void standard_qp_callback(PlannerInfo *root, void *extra);
static double get_number_of_groups(PlannerInfo *root,
					 double path_rows,
					 grouping_sets_data *gd,
					 List *target_list);
static RelOptInfo *create_grouping_paths(PlannerInfo *root,
					  RelOptInfo *input_rel,
					  PathTarget *target,
					  bool target_parallel_safe,
					  const AggClauseCosts *agg_costs,
					  grouping_sets_data *gd);
static bool is_degenerate_grouping(PlannerInfo *root);
static void create_degenerate_grouping_paths(PlannerInfo *root,
								 RelOptInfo *input_rel,
								 PathTarget *target, RelOptInfo *grouped_rel);
static void create_ordinary_grouping_paths(PlannerInfo *root,
							   RelOptInfo *input_rel,
							   PathTarget *target, RelOptInfo *grouped_rel,
							   const AggClauseCosts *agg_costs,
							   grouping_sets_data *gd, int flags);
static void consider_groupingsets_paths(PlannerInfo *root,
							RelOptInfo *grouped_rel,
							Path *path,
							bool is_sorted,
							bool can_hash,
							PathTarget *target,
							grouping_sets_data *gd,
							const AggClauseCosts *agg_costs,
							double dNumGroups);
static RelOptInfo *create_window_paths(PlannerInfo *root,
					RelOptInfo *input_rel,
					PathTarget *input_target,
					PathTarget *output_target,
					bool output_target_parallel_safe,
					WindowFuncLists *wflists,
					List *activeWindows);
static void create_one_window_path(PlannerInfo *root,
					   RelOptInfo *window_rel,
					   Path *path,
					   PathTarget *input_target,
					   PathTarget *output_target,
					   WindowFuncLists *wflists,
					   List *activeWindows);
static RelOptInfo *create_distinct_paths(PlannerInfo *root,
					  RelOptInfo *input_rel);
static RelOptInfo *create_ordered_paths(PlannerInfo *root,
					 RelOptInfo *input_rel,
					 PathTarget *target,
					 bool target_parallel_safe,
					 double limit_tuples);
static PathTarget *make_group_input_target(PlannerInfo *root,
						PathTarget *final_target);
static PathTarget *make_partial_grouping_target(PlannerInfo *root,
							 PathTarget *grouping_target,
							 Node *havingQual);
static PathTarget *make_cn_input_target(PlannerInfo *root,
						PathTarget *origin_target);
static List *postprocess_setop_tlist(List *new_tlist, List *orig_tlist);
static List *select_active_windows(PlannerInfo *root, WindowFuncLists *wflists);
static PathTarget *make_window_input_target(PlannerInfo *root,
						 PathTarget *final_target,
						 List *activeWindows);
static List *make_pathkeys_for_window(PlannerInfo *root, WindowClause *wc,
						 List *tlist);
static PathTarget *make_sort_input_target(PlannerInfo *root,
					   PathTarget *final_target,
					   bool *have_postponed_srfs,
					   bool *have_postponed_cnfs);
static bool grouping_distribution_match(PlannerInfo *root, Query *parse,
										Path *path, List *clauses,
										List *targetList);
static bool groupingsets_distribution_match(PlannerInfo *root, Query *parse,
					  Path *path);
static Path *adjust_path_distribution(PlannerInfo *root, Query *parse,
					  Path *path);
static bool can_push_down_grouping(PlannerInfo *root, Query *parse, Path *path);
static bool can_push_down_window(PlannerInfo *root, Path *path);
static void adjust_paths_for_srfs(PlannerInfo *root, RelOptInfo *rel,
					  List *targets, List *targets_contain_srfs);
static void add_paths_to_grouping_rel(PlannerInfo *root, RelOptInfo *input_rel,
						  RelOptInfo *grouped_rel,
						  PathTarget *target,
						  RelOptInfo *partially_grouped_rel,
						  const AggClauseCosts *agg_costs,
						  const AggClauseCosts *agg_final_costs,
						  grouping_sets_data *gd, bool can_sort, bool can_hash,
						  double dNumGroups, List *havingQual);
static RelOptInfo *create_partial_grouping_paths(PlannerInfo *root,
							  RelOptInfo *grouped_rel,
							  RelOptInfo *input_rel,
							  grouping_sets_data *gd,
							  bool can_sort,
							  bool can_hash,
							  AggClauseCosts *agg_final_costs);
static void gather_grouping_paths(PlannerInfo *root, RelOptInfo *rel);
static bool can_partial_agg(PlannerInfo *root,
				const AggClauseCosts *agg_costs);
static void apply_scanjoin_target_to_paths(PlannerInfo *root,
							   RelOptInfo *rel,
							   List *scanjoin_targets,
							   List *scanjoin_targets_contain_srfs,
							   bool scanjoin_target_parallel_safe,
							   bool tlist_same_exprs,
							   bool have_postponed_cnfs);

#ifdef __OPENTENBASE__
static Path *adjust_modifytable_subpath(PlannerInfo *root, Query *parse,
										Path *path);
#endif
#ifdef __OPENTENBASE_C__
static Path *create_grouping_append_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
                                         PathTarget *target, List *having_qual, List *rollups,
                                         const AggClauseCosts *agg_costs, grouping_sets_data *gd,
                                         bool can_hash);
static List *get_partition_clauses(PathTarget *window_target, WindowClause *wc);
static void              create_cn_expr_paths(PlannerInfo *root, RelOptInfo *current_rel,
                                              PathTarget *origin_target, List *cn_process_targets,
                                              List *cn_process_targets_contain_srfs, PathTarget *final_target);
#endif

static Path * generate_final_rel_path(PlannerInfo *root, Query *parse, int64 offset_est, int64 count_est, double limit_tuples,
						RelOptInfo *final_rel, bool ora_nulllimits, bool *parallel_modify_partial_path_added,
						Path *path, bool isParallelModify);

extern float8 GetlimitCount(Datum val, bool float8_datum, bool percent);

/*****************************************************************************
 *
 *	   Query optimizer entry point
 *
 * To support loadable plugins that monitor or modify planner behavior,
 * we provide a hook variable that lets a plugin get control before and
 * after the standard planning process.  The plugin would normally call
 * standard_planner().
 *
 * Note to plugin authors: standard_planner() scribbles on its Query input,
 * so you'd better copy that data structure if you want to plan more than once.
 *
 *****************************************************************************/
PlannedStmt *
planner(Query *parse, int cursorOptions, ParamListInfo boundParams, 
		int cached_param_num, bool explain)
{
	PlannedStmt *result;

	planning_dml_sql = false;
	if (is_parallel_allowed_for_modify(parse))
		planning_dml_sql = true;
	
	if (planner_hook)
		result = (*planner_hook) (parse, cursorOptions, boundParams, 
									cached_param_num, explain);
	else
#ifdef PGXC
		/*
		 * A Coordinator receiving a query from another Coordinator
		 * is not allowed to go into PGXC planner.
		 */
		if (IS_PGXC_LOCAL_COORDINATOR)
			result = pgxc_planner(parse, cursorOptions, boundParams, 
									cached_param_num, explain);
		else
#endif
			result = standard_planner(parse, cursorOptions, boundParams);
	return result;
}

PlannedStmt *
standard_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;
	PlannerGlobal *glob;
	double		tuple_fraction;
	PlannerInfo *root;
	RelOptInfo *final_rel;
	Path	   *best_path;
	Plan	   *top_plan;
	ListCell   *lp,
			   *lr;
    RangeTblEntry *rte;
    bool is_foreign_rte;
    Query      *orig_query = copyObject(parse);
	bool        is_cursorstmt = false;
	bool is_hdfsforeign_rte = false;
	ListCell   *lc;

    /* has nextval and parallel safe */
    bool nextval_parallel_safe = false;

#ifdef XCP
	if (IS_PGXC_LOCAL_COORDINATOR && parse->utilityStmt &&
			IsA(parse->utilityStmt, RemoteQuery))
		return pgxc_direct_planner(parse, cursorOptions, boundParams);
#endif
	/*
	 * Set up global state for this planner invocation.  This data is needed
	 * across all levels of sub-Query that might exist in the given command,
	 * so we keep it in a separate struct that's linked to by each per-Query
	 * PlannerInfo.
	 */
	glob = makeNode(PlannerGlobal);

	glob->boundParams = boundParams;
	glob->subplans = NIL;
	glob->subroots = NIL;
	glob->subplan_nodes = NIL;
	glob->rewindPlanIDs = NULL;
	glob->finalrtable = NIL;
	glob->finalrowmarks = NIL;
	glob->resultRelations = NIL;
	glob->nonleafResultRelations = NIL;
	glob->rootResultRelations = NIL;
	glob->relationOids = NIL;
	glob->invalItems = NIL;
	glob->paramExecTypes = NIL;
	glob->lastPHId = 0;
	glob->lastRowMarkId = 0;
	glob->lastPlanNodeId = 0;
	glob->transientPlan = false;
	glob->dependsOnRole = false;
	glob->has_val_func = false;
#ifdef __OPENTENBASE_C__
	glob->sharedCtePlans = NULL;
	glob->sharedCtePlanIds = NULL;
	glob->subquery_seq_no = 1;
	glob->plannerinfo_seq_no = 1;

	reset_internal_index();
#endif

	/*
	 * Assess whether it's feasible to use parallel mode for this query. We
	 * can't do this in a standalone backend, or if the command will try to
	 * modify any data (except for Insert), or if this is a cursor operation,
	 * or if GUCs are set to values that don't permit parallelism, or if
	 * parallel-unsafe functions are present in the query tree.
	 *
	 * MATERIALIZED VIEW to use parallel plans, but as of now, only the leader
	 * backend writes into a completely new table.  In the future, we can
	 * extend it to allow workers to write into the table.  However, to allow
	 * parallel updates and deletes, we have to solve other problems,
	 * especially around combo CIDs.)
	 *
	 * For now, we don't try to use parallel mode if we're running inside a
	 * parallel worker.  We might eventually be able to relax this
	 * restriction, but for now it seems best not to have parallel workers
	 * trying to create their own parallel workers.
	 *
	 * We can't use parallelism in serializable mode because the predicate
	 * locking code is not parallel-aware.  It's not catastrophic if someone
	 * tries to run a parallel plan in serializable mode; it just won't get
	 * any workers and will run serially.  But it seems like a good heuristic
	 * to assume that the same serialization level will be in effect at plan
	 * time and execution time, so don't generate a parallel plan if we're in
	 * serializable mode.
	 */
	if ((cursorOptions & CURSOR_OPT_PARALLEL_OK) != 0 &&
		IsUnderPostmaster &&
		dynamic_shared_memory_type != DSM_IMPL_NONE &&
		(parse->commandType == CMD_SELECT ||
		 is_parallel_allowed_for_modify(parse)) &&
		!parse->hasModifyingCTE &&
		max_parallel_workers_per_gather > 0 &&
		!IsParallelWorker() &&
		!IsolationIsSerializable())
	{
		/* all the cheap tests pass, so scan the query tree */
		glob->maxParallelHazard = max_parallel_hazard(parse, &nextval_parallel_safe);
		glob->parallelModeOK = (glob->maxParallelHazard != PROPARALLEL_UNSAFE);
	}
	else
	{
		/* skip the query tree scan, just assume it's unsafe */
		glob->maxParallelHazard = PROPARALLEL_UNSAFE;
		glob->parallelModeOK = false;
	}

	/*
	 * glob->parallelModeNeeded is normally set to false here and changed to
	 * true during plan creation if a Gather or Gather Merge plan is actually
	 * created (cf. create_gather_plan, create_gather_merge_plan).
	 *
	 * However, if force_parallel_mode = on or force_parallel_mode = regress,
	 * then we impose parallel mode whenever it's safe to do so, even if the
	 * final plan doesn't use parallelism.  It's not safe to do so if the
	 * query contains anything parallel-unsafe; parallelModeOK will be false
	 * in that case.  Note that parallelModeOK can't change after this point.
	 * Otherwise, everything in the query is either parallel-safe or
	 * parallel-restricted, and in either case it should be OK to impose
	 * parallel-mode restrictions.  If that ends up breaking something, then
	 * either some function the user included in the query is incorrectly
	 * labelled as parallel-safe or parallel-restricted when in reality it's
	 * parallel-unsafe, or else the query planner itself has a bug.
	 */
	glob->parallelModeNeeded = glob->parallelModeOK &&
		(force_parallel_mode != FORCE_PARALLEL_OFF);

	/* Determine what fraction of the plan is likely to be scanned */
	if (cursorOptions & CURSOR_OPT_FAST_PLAN)
	{
		/*
		 * We have no real idea how many tuples the user will ultimately FETCH
		 * from a cursor, but it is often the case that he doesn't want 'em
		 * all, or would prefer a fast-start plan anyway so that he can
		 * process some of the tuples sooner.  Use a GUC parameter to decide
		 * what fraction to optimize for.
		 */
		tuple_fraction = cursor_tuple_fraction;

		/*
		 * We document cursor_tuple_fraction as simply being a fraction, which
		 * means the edge cases 0 and 1 have to be treated specially here.  We
		 * convert 1 to 0 ("all the tuples") and 0 to a very small fraction.
		 */
		if (tuple_fraction >= 1.0)
			tuple_fraction = 0.0;
		else if (tuple_fraction <= 0.0)
			tuple_fraction = 1e-10;
	}
	else
	{
		/* Default assumption is we need all the tuples */
		tuple_fraction = 0.0;
	}
	/* primary planning entry point (may recurse for subqueries) */
	root = subquery_planner(glob, parse, NULL,
							false, tuple_fraction);

	/* Select best Path and turn it into a Plan */
	final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
	best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);

    if (IsA(best_path, ModifyTablePath) && IS_CENTRALIZED_DATANODE && root -> parallel_dml)
    {
        best_path->total_cost -= best_path->dml_cost;
    }

	if (!root->distribution && !IS_CENTRALIZED_MODE)
		root->distribution = best_path->distribution;

#ifdef __OPENTENBASE__
	if (root->distribution && !parse->hasUnshippableDml)
	{
	    remote_subplan_depth++;
	}
#endif

	top_plan = create_plan(root, best_path);
#ifdef XCP
#ifdef __OPENTENBASE__
	/* dblink supports DML:insert/update/delete */
    is_foreign_rte = false;
    if (root->parse->resultRelation)
	{
        rte = rt_fetch(root->parse->resultRelation, root->parse->rtable);
		is_foreign_rte = (rte->relkind == RELKIND_FOREIGN_TABLE && 
							!rel_is_external_table(rte->relid) &&
							!rel_is_hdfsfdw_table(rte->relid));
    }
	if (root->distribution && !parse->hasUnshippableDml && !is_foreign_rte)
#else
	if (root->distribution)
#endif
	{
		/*
         * If distribution is needed, we need pass the sort information
         * to remote plan so that cn can sort results from distributed
         * dns. But when "order by" in the inner layer of the SQL 
         * statement, the sort information will be generated in the 
         * subplan so that we can't generate remote plan with top_plan
         * directly. In order to generate remote plan corretly, we 
         * pass subplan that owns sort information to remote subplan.
         */
		bool remote_set = false;
		if (IsA(best_path, SubqueryScanPath) && IsA(top_plan, SubqueryScan))
		{
			SubqueryScan    *sub_plan = (SubqueryScan *) top_plan;
			Assert(best_path->parent && best_path->parent->subroot);

			/*
			 * We use subroot here to solve the sitution that the plan
			 * doesn't have a sort operator but need to merge sort.
			 */
			if (best_path->parent->subroot->sort_pathkeys)
			{
				sub_plan->subplan =
					(Plan *) make_remotesubplan(root, sub_plan->subplan, NULL,
												root->distribution,
												best_path->parent->subroot->sort_pathkeys,
												false,
												best_path->parent->relids);
				remote_set = true;
			}
		}
		if (!remote_set)
		{
			/*
			 * FIXME, this keeps adding RemoteSubplan at a top of queries that
			 * don't really need it (e.g above a MergeAppend with subplans pushed
			 * to remote nodes). Not sure why it's happening, though ...
			 */
			top_plan = (Plan *) make_remotesubplan(root, top_plan, NULL,
												   root->distribution,
												   root->sort_pathkeys ? root->sort_pathkeys : best_path->pathkeys,
												   false,
												   best_path->parent->relids);
		}
		if ((parse->commandType == CMD_UPDATE || parse->commandType == CMD_DELETE ||
		     parse->commandType == CMD_INSERT || parse->commandType == CMD_MERGE) &&
		    (root->distribution->distributionType == LOCATOR_TYPE_SHARD ||
		     root->distribution->distributionType == LOCATOR_TYPE_HASH))
		{
			ModifyTable *mt = NULL;
			Plan        *old_plan = top_plan;
			
			/*
			 * The plan now should be like:
			 * Remote Subquery Scan
			 *      -> Update
			 *              -> Some scan or Join
			 */
			Assert(IsA(top_plan, RemoteSubplan));
			Assert(IsA(top_plan->lefttree, ModifyTable) || IsA(top_plan->lefttree, MultiModifyTable));

			/* MultiModifyTable no need do it. */
			if (IsA(top_plan->lefttree, ModifyTable))
			{
				mt = (ModifyTable *) top_plan->lefttree;
				top_plan = make_remote_modifytable(root, top_plan, mt);
			}
			if (old_plan != top_plan)
				SS_attach_initplans(root, top_plan);
		}
		remote_subplan_depth--;
	}
#endif

	/* if exist hdfs fdw foreign table, deny to FQS*/
	foreach(lc, root->parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		is_hdfsforeign_rte = (rte->relkind == RELKIND_FOREIGN_TABLE && rel_is_hdfsfdw_table(rte->relid));
		if (is_hdfsforeign_rte) 
			break;
	}

	is_cursorstmt = cursorOptions & (CURSOR_OPT_BINARY | CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL |
	                                 CURSOR_OPT_INSENSITIVE | CURSOR_OPT_HOLD);

	/*
	 * The currently supported SQL features for FQS (Fast Query Shipping) are as follows:
	 * Requirements: Non-user-defined functions, non-for-update statements, non-global indexes, non-cursor
	 * operations, and the statement must be a SELECT query.
	 *
	 * Additional support is provided for executing
	 * distributed queries spanning multiple nodes, but only if there is a single layer of remote subplan
	 * at the topmost level, allowing it to qualify for FQS.
	 */
	if (enable_fast_query_shipping &&
		parse->canShip &&
	    (list_length(glob->execNodeSet) == 1 ||
	     (list_length(glob->execNodeSet) > 1 && glob->remotesubplan_nums == 1 &&
	      IsA(top_plan, RemoteSubplan) && ((RemoteSubplan *) top_plan)->sort == NULL)) &&
		glob->lastRowMarkId == 0 &&
		!glob->has_gidx_plan &&
		!is_cursorstmt &&
		!is_hdfsforeign_rte &&
		parse->commandType == CMD_SELECT)
	{
		Query     *query = orig_query;
		List      *rte_list = NULL;
		ExecNodes *exec_nodes = makeNode(ExecNodes);

		exec_nodes->nodeList = glob->execNodeSet;
		top_plan = (Plan *) pgxc_FQS_create_remote_plan(query, exec_nodes, false, FQS_MODE_STD_PLANER);

		if (query->returningList)
			top_plan->targetlist = query->returningList;

		top_plan = set_plan_references(root, top_plan);

		/* build the PlannedStmt result */
		result = makeNode(PlannedStmt);
		/* Try and set what we can, rest must have been zeroed out by makeNode() */
		result->commandType = query->commandType;
		result->canSetTag = query->canSetTag;
		result->utilityStmt = query->utilityStmt;

		result->planTree = top_plan;
		result->rtable = query->rtable;
		result->queryId = query->queryId;
		result->relationOids = glob->relationOids;
		result->invalItems = glob->invalItems;
		result->rowMarks = glob->finalrowmarks;
		result->hasReturning = (query->returningList != NULL);
		flatten_unplanned_query(&rte_list, query);
		result->fqs_rte_list = rte_list;

		result->p_has_val_func = glob->has_val_func;
		SetSPMUnsupported();
		return result;
	}

	/*
	 * If creating a plan for a scrollable cursor, make sure it can run
	 * backwards on demand.  Add a Material node at the top at need.
	 */
#ifdef __OPENTENBASE_C__
	if ((cursorOptions & CURSOR_OPT_SCROLL) || parse->cursor_opt_scroll)
#else
	if (cursorOptions & CURSOR_OPT_SCROLL)
#endif
	{
		Plan	*old_plan = top_plan;

		if (!ExecSupportsBackwardScan(top_plan))
			top_plan = materialize_finished_plan(top_plan);
		
		if (IS_PGXC_LOCAL_COORDINATOR)
			top_plan->remote_flag = -1; /* force*/
		
		if (old_plan != top_plan)
			SS_attach_initplans(root, top_plan);
	}

	/*
	 * Optionally add a Gather node for testing purposes, provided this is
	 * actually a safe thing to do.
	 */
	if (force_parallel_mode != FORCE_PARALLEL_OFF && top_plan->parallel_safe &&
		!IS_PGXC_COORDINATOR)
	{
		Gather	   *gather = makeNode(Gather);

		/*
		 * If there are any initPlans attached to the formerly-top plan node,
		 * move them up to the Gather node; same as we do for Material node in
		 * materialize_finished_plan.
		 */
		gather->plan.initPlan = top_plan->initPlan;
		top_plan->initPlan = NIL;

		gather->plan.targetlist = top_plan->targetlist;
		gather->plan.qual = NIL;
		gather->plan.lefttree = top_plan;
		gather->plan.righttree = NULL;
		gather->num_workers = 1;
		gather->single_copy = true;
		gather->invisible = (force_parallel_mode == FORCE_PARALLEL_REGRESS);

		/*
		 * Since this Gather has no parallel-aware descendants to signal to,
		 * we don't need a rescan Param.
		 */
		gather->rescan_param = -1;

		/*
		 * Ideally we'd use cost_gather here, but setting up dummy path data
		 * to satisfy it doesn't seem much cleaner than knowing what it does.
		 */
		gather->plan.startup_cost = top_plan->startup_cost +
			parallel_setup_cost;
		gather->plan.total_cost = top_plan->total_cost +
			parallel_setup_cost + parallel_tuple_cost * top_plan->plan_rows;
		gather->plan.plan_rows = top_plan->plan_rows;
		gather->plan.plan_width = top_plan->plan_width;
		gather->plan.parallel_aware = false;
		gather->plan.parallel_safe = false;

		/* use parallel mode for parallel plans. */
		root->glob->parallelModeNeeded = true;

		top_plan = &gather->plan;
	}

	/*
	 * If any Params were generated, run through the plan tree and compute
	 * each plan node's extParam/allParam sets.  Ideally we'd merge this into
	 * set_plan_references' tree traversal, but for now it has to be separate
	 * because we need to visit subplans before not after main plan.
	 *
	 * Although there are no parameters, the right tree of the connect by
	 * clause must be under the subplan.
	 */
	if (glob->paramExecTypes != NIL || parse->connectByExpr != NULL)
	{
		Assert(list_length(glob->subplans) == list_length(glob->subroots));
		forboth(lp, glob->subplans, lr, glob->subroots)
		{
			Plan	   *subplan = (Plan *) lfirst(lp);
			PlannerInfo *subroot = (PlannerInfo *) lfirst(lr);

			SS_finalize_plan(subroot, subplan);
		}
		SS_finalize_plan(root, top_plan);

#ifdef __OPENTENBASE_C__
		/* adjust plan (for subplan (including withrecursive) and nestloop) */
		if (IS_PGXC_COORDINATOR)
		{
			foreach(lp, glob->subplans)
			{
				Plan	   *subplan = (Plan *) lfirst(lp);

				if (subplan->is_subplan || subplan->extParam)
					lfirst(lp) = SS_adjust_plan(subplan, true);
				else
					(void) SS_adjust_plan(subplan, false);
			}
			(void) SS_adjust_plan(top_plan, false);
		}
#endif
	}

	/* final cleanup of the plan */
	Assert(glob->finalrtable == NIL);
	Assert(glob->finalrowmarks == NIL);
	Assert(glob->resultRelations == NIL);
	Assert(glob->nonleafResultRelations == NIL);
	Assert(glob->rootResultRelations == NIL);
	top_plan = set_plan_references(root, top_plan);
	/* ... and the subplans (both regular subplans and initplans) */
	Assert(list_length(glob->subplans) == list_length(glob->subroots));

	glob->exec_subplan = list_concat_unique_ptr(glob->exec_subplan, root->init_plans);

	forboth(lp, glob->subplans, lr, glob->subroots)
	{
		Plan	   *subplan = (Plan *) lfirst(lp);
		PlannerInfo *subroot = (PlannerInfo *) lfirst(lr);

		lfirst(lp) = set_plan_references(subroot, subplan);

		glob->exec_subplan = list_concat_unique_ptr(glob->exec_subplan, subroot->init_plans);
		SS_set_subplan(subroot, lfirst(lp));
	}

	SS_set_subplan(root, top_plan);

	/*
	 * Redirect initplan of top_plan, if it's not a remote fragment,
	 * it must be root fragment
	 */
	if (!IsA(top_plan, RemoteSubplan) && IS_PGXC_COORDINATOR)
	{
		/* attach any unprocessed initplans to top_plan */
		top_plan->exec_subplan =
			list_concat_unique_ptr(top_plan->exec_subplan, glob->exec_subplan);
		redirect_remote_subplan_send(glob, top_plan);
	}

	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);

	result->commandType = parse->commandType;
	result->queryId = parse->queryId;
	result->hasReturning = (parse->returningList != NIL);
	result->hasModifyingCTE = parse->hasModifyingCTE;
	result->canSetTag = parse->canSetTag;
	result->transientPlan = glob->transientPlan;
	result->dependsOnRole = glob->dependsOnRole;
	result->parallelModeNeeded = glob->parallelModeNeeded;
    result->canNextvalParallel = nextval_parallel_safe;
	result->planTree = top_plan;
	result->rtable = glob->finalrtable;
	result->resultRelations = glob->resultRelations;
	result->nonleafResultRelations = glob->nonleafResultRelations;
	result->rootResultRelations = glob->rootResultRelations;
	result->subplans = glob->subplans;
	result->rewindPlanIDs = glob->rewindPlanIDs;
	result->rowMarks = glob->finalrowmarks;
	result->relationOids = glob->relationOids;
	result->invalItems = glob->invalItems;
	result->paramExecTypes = glob->paramExecTypes;
	result->p_has_val_func = glob->has_val_func;

	/* utilityStmt should be null, but we might as well copy it */
	result->utilityStmt = parse->utilityStmt;
	result->stmt_location = parse->stmt_location;
	result->stmt_len = parse->stmt_len;
#ifdef __OPENTENBASE_C__
	result->sharedCtePlans = glob->sharedCtePlans;
	result->sharedCtePlanIds = glob->sharedCtePlanIds;
	/* Add the shared CTE fragments and root fragment as total */
	result->fragmentNum = glob->fragmentNum + list_length(glob->sharedCtePlans) + 1;
	result->guc_str = NULL;

	if (query_mem > 0 || IsResGroupActivated())
	{
		/*
		 * When query_mem is set, we calculate the total memory used by
		 * the plan tree. If the estimated memory is larger than query_mem,
		 * we adjust it to query_mem by reducing work_mem.
		 * The value will be used in resource control.
		 */
		int curr_query_mem = query_mem;

		if (IsResGroupActivated())
		{
			/* reset query_mem for current query if resgroup activated */
			int groupMemLimit = ResourceGroupGetQueryMemoryLimit();

			if (groupMemLimit > 0)
			{
				if (curr_query_mem == 0 || curr_query_mem > groupMemLimit)
					curr_query_mem = Max(groupMemLimit, SIMPLE_QUERY_THRESHOLD);
			}
		}

		if (curr_query_mem > 0)
			set_plan_querymem(result, curr_query_mem);
	}
#endif

	result->jitFlags = PGJIT_NONE;
	if (jit_enabled && jit_above_cost >= 0 &&
		top_plan->total_cost > jit_above_cost &&
		experiment_feature && !ORA_MODE)
	{
		result->jitFlags |= PGJIT_PERFORM;

		/*
		 * Decide how much effort should be put into generating better code.
		 */
		if (jit_optimize_above_cost >= 0 &&
			top_plan->total_cost > jit_optimize_above_cost)
			result->jitFlags |= PGJIT_OPT3;
		if (jit_inline_above_cost >= 0 &&
			top_plan->total_cost > jit_inline_above_cost)
			result->jitFlags |= PGJIT_INLINE;

		/*
		 * Decide which operations should be JITed.
		 */
		if (jit_expressions)
			result->jitFlags |= PGJIT_EXPR;
		if (jit_tuple_deforming)
			result->jitFlags |= PGJIT_DEFORM;
	}

	return result;
}

/*
 * var_in_targetlist
 *	Check whether vars in sublink are in the target list.
 */
static bool
var_in_targetlist(Node *node, List	*targetlist, bool replace_var, int varlevelsup)
{
	ListCell		*lc;
	TargetEntry		*targetEntry;
	Var				*var;
	Var				*node_var;
	int				targetlist_index = 1;

	if (node == NULL || !IsA(node, Var))
		return false;

	node_var = (Var *) node;

	foreach(lc, targetlist)
	{
		targetEntry = (TargetEntry *) lfirst(lc);

		if (IsA((Node *)targetEntry->expr, Var))
		{
			var = (Var*) targetEntry->expr;

			if (node_var->varlevelsup != varlevelsup)
				continue;

			if (equal_var(node_var, var))
			{
				if (replace_var)
				{
					node_var->varno = 1;
					node_var->varattno = targetlist_index;
					node_var->varoattno = targetlist_index;
					return false;
				}
				else
					return true;
			}
		}

		targetlist_index++;
	}

	return false;
}

static bool
related_targetlist_sublink_walker(Node *node,
				related_targetlist_sublink_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == context->varlevelsup)
		{
			context->num_check_var++;

			if (var_in_targetlist(node, context->targetlist,
								  context->replace_var, context->varlevelsup))
			{
				context->num_qualify_var++;
			}
			else if (context->replace_var == false)
			{
				ListCell	*lc;
				Var			*var;
				foreach(lc, context->lack_var_list)
				{
					var = (Var *) lfirst(lc);
					if (equal_var(var, (Var *) node))
					{
						return false;
					}
				}
				context->lack_var_list = lappend(context->lack_var_list, node);
			}
		}

		if (((Var *) node)->varlevelsup > context->varlevelsup)
		{
			context->contain_unsupport_var = true;
			return true;
		}

		return false;
	}
	if (IsA(node, Query))
	{
		context->contain_sublink = true;
		return true;
	}

	return expression_tree_walker(node, related_targetlist_sublink_walker,
								  (void *) context);
}

/* 
 * related_targetlist_sublink
 *	Walk tree for qualified sublinks.
 */
static bool
related_targetlist_sublink(Node *node, List *targetlist, bool replace_var,
						   List **lack_var_list, int varlevelsup)
{
	related_targetlist_sublink_context context;

	context.targetlist = targetlist;
	context.replace_var = replace_var;
	context.contain_sublink = false;
	context.contain_unsupport_var = false;
	context.num_check_var = 0;
	context.num_qualify_var = 0;
	context.varlevelsup = varlevelsup;

	if (lack_var_list != NULL)
		context.lack_var_list = *lack_var_list;
	else
		context.lack_var_list = NULL;

	query_or_expression_tree_walker(node, related_targetlist_sublink_walker,
								  (void *) &context, 0);

	if (context.num_check_var > 0 &&
		!context.contain_unsupport_var &&
		!context.contain_sublink)
	{
		if (context.num_check_var != context.num_qualify_var &&
			!replace_var)
		{
			*lack_var_list = context.lack_var_list;
		}
		return true;
	}

	return false;
}

/* 
 * update_subquery_varlevelsup_walker
 *	Walk tree to update varlevelsup for subquery.
 */
static bool
update_subquery_varlevelsup_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup >= 1)
		{
			((Var *) node)->varlevelsup++;
		}
	}

	return expression_tree_walker(node, update_subquery_varlevelsup_walker, (void *) context);
}

/* 
 * update_subquery_varlevelsup_walker
 *	Walk tree to update varlevelsup for subquery.
 */
static bool
update_subquery_varlevelsup(Node *node)
{
	return query_or_expression_tree_walker(node, update_subquery_varlevelsup_walker, NULL, 0);
}

/*
 * check_valid_func_of_distinct_list
 *	Check whether the function node could be supported for distinct subquery pull up.
 */
static bool
check_valid_func_of_distinct_list(Node *node)
{
	switch(nodeTag(node))
	{
		case T_FuncExpr:
		{
			FuncExpr *funcExpr = (FuncExpr*)node;
			ListCell *lc = NULL;
			int		  var_num = 0;

			if (contain_volatile_functions((Node*) funcExpr))
			{
				return false;
			}

			foreach(lc, funcExpr->args)
			{
				Node *arg_node = (Node *)lfirst(lc);

				if (!IsA(arg_node, Var) &&
					contain_vars_of_level_or_above(arg_node, 0))
				{
					return false;
				}

				if (IsA(arg_node, Var))
				{
					if (var_num > 0)
						return false;

					var_num++;
				}
			}

			return true;
		}
		case T_RelabelType:
		{
			if (IsA(((RelabelType *)node)->arg, Var))
				return true;

			break;
		}
		case T_CoerceViaIO:
		{
			if (IsA(((CoerceViaIO *)node)->arg, Var))
				return true;

			break;
		}
		default:
			break;
	}

	return false;
}

/*
 * replace_var_under_func_of_distinct_list
 *	Replace var node for function nodes.
 */
static void
replace_var_under_func_of_distinct_list(Node *node, List *target_list)
{
	/*
	 * Set the node to var
	 */
	switch(nodeTag(node))
	{
		case T_FuncExpr:
		{
			ListCell *lc = NULL;
			FuncExpr *funcExpr = (FuncExpr*)node;

			foreach(lc, funcExpr->args)
			{
				Node* sub_node = (Node*) lfirst(lc);
				if (IsA(sub_node, Var))
				{
					var_in_targetlist(sub_node, target_list, true, 0);
				}
			}
			break;
		}
		case T_RelabelType:
		{
			Var *var = (Var*) ((RelabelType *)node)->arg;

			var_in_targetlist((Node*)var, target_list, true, 0);
			break;
		}
		case T_CoerceViaIO:
		{
			Var *var = (Var *) ((CoerceViaIO *)node)->arg;
			var_in_targetlist((Node*)var, target_list, true, 0);
		}
		default:
			break;
	}

	return;
}

/* 
 * build_targetlist_related_sublink
 *	Build up target list for new query block.
 */
static void
build_targetlist_related_sublink(Query *subquery, Query *parse)
{
	TargetEntry		*targetEntry;
	ListCell		*lc;
	Var				*parse_var;
	Var				*sublink_var;
	TargetEntry		*ent = NULL;
	int				 targetlist_index = 1;
	RangeTblEntry	*rte;

	foreach(lc, parse->targetList)
	{
		targetEntry = (TargetEntry *) lfirst(lc);

		if (targetEntry->resjunk)
			break;

		if (IsA((Node *)targetEntry->expr, Var))
		{
			parse_var = (Var *) targetEntry->expr;
			sublink_var = makeVar(1, targetlist_index,
								  parse_var->vartype,
								  parse_var->vartypmod,
								  parse_var->varcollid,
								  0);

			rte = rt_fetch(parse_var->varno, parse->rtable);
			Assert(targetEntry->resname != NULL);
			if (targetEntry->resname)
				ent = makeTargetEntry((Expr *)sublink_var,
									  list_length(subquery->targetList)+1,
									  targetEntry->resname,
									  targetEntry->resjunk);
			else if (parse_var->varattno > 0 &&
					parse_var->varattno <= list_length(rte->eref->colnames))
				ent = makeTargetEntry((Expr *)sublink_var,
				                      list_length(subquery->targetList)+1,
				                      strVal(list_nth(rte->eref->colnames,
				                                      parse_var->varattno-1)),
				                      targetEntry->resjunk);
			else
				ent = makeTargetEntry((Expr *)sublink_var,
				                      list_length(subquery->targetList)+1,
				                      "?column?",
				                      targetEntry->resjunk);
			subquery->targetList = lappend(subquery->targetList, ent);
			targetlist_index++;
		}
		else if (IsA((Node *)targetEntry->expr, SubLink))
		{
			ent = targetEntry;
			subquery->targetList = lappend(subquery->targetList, copyObject(targetEntry));
		}
		else
		{
			ent = targetEntry;
			subquery->targetList = lappend(subquery->targetList, copyObject(targetEntry));
		}

		ent->ressortgroupref = targetEntry->ressortgroupref;
		ent->resorigcol = targetEntry->resorigcol;
	}

	return;
}

/* 
 * remove_related_sublink
 *	Remove sublink from query block.
 */
static void
remove_related_sublink(Query *parse)
{
	ListCell	*lc_targetlist;
	ListCell	*lc_targetlist_sub;
	ListCell	*lc_distinct;
	TargetEntry	*targetEntry;
	TargetEntry	*targetEntry_sub;
	int			targetlist_index = 1;
	SortGroupClause *sortGroupClause;

	for (lc_targetlist = list_head(parse->targetList); lc_targetlist != NULL;)
	{
		targetEntry = (TargetEntry *) lfirst(lc_targetlist);
		lc_targetlist = lnext(lc_targetlist);

		if (IsA(targetEntry->expr, SubLink) ||
			check_valid_func_of_distinct_list((Node *)targetEntry->expr))
		{
			parse->targetList = list_delete_nth(parse->targetList, targetlist_index);

			for (lc_distinct = list_head(parse->distinctClause); lc_distinct != NULL;)
			{
				sortGroupClause = (SortGroupClause *) lfirst(lc_distinct);
				lc_distinct = lnext(lc_distinct);

				if (sortGroupClause->tleSortGroupRef == targetEntry->ressortgroupref)
				{
					parse->distinctClause = list_delete(parse->distinctClause, sortGroupClause);
				}
			}

			foreach(lc_targetlist_sub, parse->targetList)
			{
				targetEntry_sub = (TargetEntry *) lfirst(lc_targetlist_sub);

				if (targetEntry_sub->resno >= targetlist_index)
				{
					targetEntry_sub->resno--;
				}

				if (targetEntry_sub->ressortgroupref >= targetEntry->ressortgroupref)
				{
					targetEntry_sub->ressortgroupref--;
				}
			}

			foreach(lc_distinct, parse->distinctClause)
			{
				sortGroupClause = (SortGroupClause *) lfirst(lc_distinct);

				if (sortGroupClause->tleSortGroupRef >= targetEntry->ressortgroupref)
				{
					sortGroupClause->tleSortGroupRef--;
				}
			}
		}
		else
		{
			targetlist_index++;
		}
	}
}

/*
 * build_up_eref_distinct_targetlist_subquery
 *	Build up column name from parse tree.
 */
static Alias *
build_up_eref_distinct_targetlist_subquery(Query *parse)
{
	Alias			*eref;
	Var				*var;
	ListCell		*lc;
	RangeTblEntry	*rte;
	TargetEntry		*targetEntry;

	eref = makeAlias("Subquery", NIL);
	
	foreach(lc, parse->targetList)
	{
		targetEntry = (TargetEntry *) lfirst(lc);

		if (IsA(targetEntry->expr, Var))
		{
			var = (Var *) targetEntry->expr;

			rte = rt_fetch(var->varno, parse->rtable);
			if (var->varattno > 0 &&
				var->varattno <= list_length(rte->eref->colnames))
				eref->colnames = lappend(eref->colnames,
										 makeString(strVal(list_nth(rte->eref->colnames, var->varattno-1))));
			else
			{
				Assert(targetEntry->resname != NULL);
				eref->colnames = lappend(eref->colnames,
				                         makeString(targetEntry->resname));
			}
		}
	}

	return eref;
}

/*
 * check_targetlist_related_sublink
 *	Check whether the query could pull up sublink in distinct target list.
 */
static bool
check_targetlist_related_sublink(List* sublink_targetlist, List *parse_targetlist,
								 bool replace_var, List **lack_var_list)
{
	TargetEntry	*targetEntry;
	ListCell	*lc;
	bool		 valid = false;

	foreach(lc, sublink_targetlist)
	{
		targetEntry = (TargetEntry *) lfirst(lc);

		if (IsA((Node *)targetEntry->expr, Var))
		{
			if (((Var*) targetEntry->expr)->varlevelsup != 0)
			{
				return false;
			}

			continue;
		}

		if (IsA((Node *)targetEntry->expr, SubLink) &&
			related_targetlist_sublink(((SubLink*) targetEntry->expr)->subselect,
										parse_targetlist, replace_var, lack_var_list, 1))
		{
			valid = true;
		}
		else if (check_valid_func_of_distinct_list((Node *)targetEntry->expr))
		{
			if (replace_var == true)
			{
				replace_var_under_func_of_distinct_list((Node *)targetEntry->expr,
														parse_targetlist);
			}
			else
			{
				/*
				 * Add neccessary vars to lack var list.
				 */
				related_targetlist_sublink((Node *)targetEntry->expr,
											parse_targetlist, false,
											lack_var_list, 0);
			}
		}
		else if (replace_var == false)
		{
			return false;
		}
	}

	return valid;
}

/*
 * check_valid_limit_clause
 *	Check validation of limit clause, and claculate limit node for parse.
 */
static Node*
check_valid_limit_clause(Query *parse)
{
	Node	   *est = NULL;
	Const	   *nodeLimit = NULL;
	int64		count_est = 0;
	int64		offset_est = 0;

	if (parse->sortClause)
		return NULL;

	/*
	 * Try to obtain the clause values.  We use estimate_expression_value
	 * primarily because it can sometimes do something useful with Params.
	 */
	if (parse->limitCount)
	{
		est = estimate_expression_value(NULL, parse->limitCount);

		if (est && IsA(est, Const))
		{
			if (((Const *) est)->constisnull)
			{
				/* NULL indicates LIMIT ALL, ie, no limit */
				count_est = 0; /* treat as not present */
			}
			else
			{
				count_est = DatumGetInt64(((Const *) est)->constvalue);
				if (count_est <= 0)
					count_est = 1; /* force to at least 1 */
			}
		}
		else
			count_est = -1;	/* can't estimate */

		if (count_est <= 0)
			return NULL;
		
		nodeLimit = (Const *) copyObject(est);
	}

	if (parse->limitOffset)
	{
		est = estimate_expression_value(NULL, parse->limitOffset);
		if (est && IsA(est, Const))
		{
			if (((Const *) est)->constisnull)
			{
				/* Treat NULL as no offset; the executor will too */
				offset_est = 0;	/* treat as not present */
			}
			else
			{
				offset_est = DatumGetInt64(((Const *) est)->constvalue);
				if (offset_est < 0)
					offset_est = 0;	/* treat as not present */
			}
		}
		else
			return NULL;
	}
	else
	{
		offset_est = 0;
	}

	if (parse->limitCount == NULL &&
		est != NULL)
	{
		nodeLimit = (Const *) copyObject(est);
	}
	else if (nodeLimit && parse->limitOffset)
	{
		nodeLimit->constvalue = count_est + offset_est;
	}

	return (Node*)nodeLimit;
}

/*
 * check_valid_postpone_subquery_targetlist
 *	Check whether the query could pull up sublink in distinct target list.
 */
static bool
check_valid_postpone_subquery_targetlist(Query *parse, List **lack_var_list, Node **nodeLimit)
{
	if (list_length(parse->targetList) != list_length(parse->distinctClause))
	{
		return false;
	}

	if (parse->groupClause != NULL ||
		parse->groupingSets != NULL)
	{
		return false;
	}

	if (contain_volatile_functions((Node*)parse))
	{
		return false;
	}

	if (parse->limitCount != NULL ||
		parse->limitOffset != NULL)
	{
		*nodeLimit = check_valid_limit_clause(parse);

		if (*nodeLimit == NULL)
			return false;
	}

	if (check_targetlist_related_sublink(parse->targetList, 
										 parse->targetList,
										 false,
										 lack_var_list))
	{
		if (optimize_distinct_subquery == 1 && *lack_var_list != NULL)
			return false;

		return true;
	}

	return false;
}

static void
add_lack_vars_to_target_list(Query *parse, List *lack_var_list)
{
	ListCell *lc;
	SortGroupClause* grpcl = NULL;
	foreach(lc, lack_var_list)
	{
		TargetEntry *tle = NULL;
		Var *var = NULL;
		Oid sortop = InvalidOid;
		Oid eqop = InvalidOid;
		bool hashable = InvalidOid;
		var = (Var *) lfirst(lc);
		var = copyObject(var);
		var->varlevelsup = 0;
		tle = makeTargetEntry((Expr *)var,
				list_length(parse->targetList) + 1,
				NULL,
				false);
		parse->targetList = lappend(parse->targetList, tle);
		get_sort_group_operators(exprType((Node*)var), false, true, false,
								&sortop, &eqop, NULL, &hashable);
		tle->ressortgroupref = list_length(parse->distinctClause) + 1;
		grpcl = makeNode(SortGroupClause);
		grpcl->tleSortGroupRef = list_length(parse->distinctClause) + 1;
		grpcl->eqop = eqop;
		grpcl->sortop = sortop;
		grpcl->nulls_first = false;
		grpcl->hashable = hashable;
		parse->distinctClause = lappend(parse->distinctClause, grpcl);
	}
	return;
}

/*
 * postpone_subquery_targetlist
 *	Enhance for query with sublinks in the distinct target list.
 *	The rows after distinct may reduce to elimate the execution times of
 *	sublinks in the target list. A new query block would be build to
 *	force the sublinks execute after distinct result.
 */
static Query *
postpone_subquery_targetlist(Query *parse, PlannerInfo *parent_root)
{
	Query			*selectQuery = NULL;
	RangeTblRef		*rtr = NULL;
	RangeTblEntry	*rte;
	List			*lack_var_list = NULL;
	Node			*nodeLimit = NULL;

	/*
	 * Todo : Support subquery under union later
	 */
	if (parent_root != NULL &&
		parent_root->parse != NULL &&
		parent_root->parse->setOperations != NULL)
	{
		ListCell 		*lc;
		RangeTblEntry	*rte;

		foreach(lc, parent_root->parse->rtable)
		{
			rte = (RangeTblEntry *) lfirst(lc);
			
			if (rte->subquery == parse)
			{
				return parse;
			}
		}
	}

	/*
	 * Check whether there is valid sublink in the target list.
	 */
	if (!check_valid_postpone_subquery_targetlist(parse, &lack_var_list, &nodeLimit))
		return parse;

	/*
	 * Build up rte for current parse tree
	 */
	rte = makeNode(RangeTblEntry);
	rte->subquery = parse;
	rte->lateral = false;
	rte->inh = false;
	rte->rtekind = 1;
	rte->inFromCl = true;

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = 1;

	/* Build up new query block as parent of current query block. */
	selectQuery = makeNode(Query);
	selectQuery->commandType = CMD_SELECT;
	selectQuery->hasSubLinks = true;
	selectQuery->rtable = list_make1(rte);
	selectQuery->jointree = makeFromExpr(list_make1(rtr), NULL);

	/* Build up target list from current query block. */
	build_targetlist_related_sublink(selectQuery, parse);

	/* Build up distinct clause if needed */
	if (lack_var_list != NULL)
		selectQuery->distinctClause = copyObject(parse->distinctClause);

	/* Add lack vars into parse target list */
	add_lack_vars_to_target_list(parse, lack_var_list);

	/* Remove sublink from query block. */
	remove_related_sublink(parse);

	/* Replace var information in the sublink. */
	lack_var_list = NULL;
	check_targetlist_related_sublink(selectQuery->targetList, parse->targetList,
									 true, &lack_var_list);

	/* Build column name information. */
	rte->eref = build_up_eref_distinct_targetlist_subquery(parse);

	/* Reset hasSubLinks for query. */
	parse->hasSubLinks = checkExprHasSubLink((Node*)parse);

	/* Set sortClause */
	selectQuery->sortClause = parse->sortClause;
	parse->sortClause = NULL;

	/* Set Limit Clause */
	selectQuery->limitCount = nodeLimit;

	/* Set CET list */
	selectQuery->cteList = parse->cteList;

	update_subquery_varlevelsup((Node *)parse);

	return selectQuery;
}

/*--------------------
 * subquery_planner
 *	  Invokes the planner on a subquery.  We recurse to here for each
 *	  sub-SELECT found in the query tree.
 *
 * glob is the global state for the current planner run.
 * parse is the querytree produced by the parser & rewriter.
 * parent_root is the immediate parent Query's info (NULL at the top level).
 * hasRecursion is true if this is a recursive WITH query.
 * tuple_fraction is the fraction of tuples we expect will be retrieved.
 * tuple_fraction is interpreted as explained for grouping_planner, below.
 *
 * Basically, this routine does the stuff that should only be done once
 * per Query object.  It then calls grouping_planner.  At one time,
 * grouping_planner could be invoked recursively on the same Query object;
 * that's not currently true, but we keep the separation between the two
 * routines anyway, in case we need it again someday.
 *
 * subquery_planner will be called recursively to handle sub-Query nodes
 * found within the query's expressions and rangetable.
 *
 * Returns the PlannerInfo struct ("root") that contains all data generated
 * while planning the subquery.  In particular, the Path(s) attached to
 * the (UPPERREL_FINAL, NULL) upperrel represent our conclusions about the
 * cheapest way(s) to implement the query.  The top level will select the
 * best Path and pass it through createplan.c to produce a finished Plan.
 *--------------------
 */
PlannerInfo *
subquery_planner(PlannerGlobal *glob, Query *parse,
				 PlannerInfo *parent_root,
				 bool hasRecursion, double tuple_fraction)
{
	PlannerInfo *root;
	List	   *newWithCheckOptions;
	List	   *newHaving;
	bool		hasOuterJoins;
	RelOptInfo *final_rel;
	ListCell   *l;
	bool recursiveOk = true;
	bool         is_target_replicated = false;

#ifdef XCP
	/* XL currently does not support DML in subqueries. */
	if ((parse->commandType != CMD_SELECT) &&
		((parent_root ? parent_root->query_level + 1 : 1) > 1) &&
		!glob->is_multi_insert)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("INSERT/UPDATE/DELETE is not supported in subquery")));
#endif

	if (optimize_distinct_subquery > 0)
		parse = postpone_subquery_targetlist(parse, parent_root);

	/* Create a PlannerInfo data structure for this subquery */
	root = makeNode(PlannerInfo);
	root->parse = parse;
	root->glob = glob;
	root->query_level = parent_root ? parent_root->query_level + 1 : 1;
	root->parent_root = parent_root;
	root->plan_params = NIL;
	root->outer_params = NULL;
	root->planner_cxt = CurrentMemoryContext;
	root->init_plans = NIL;
	root->cte_plan_ids = NIL;
	root->multiexpr_params = NIL;
	root->eq_classes = NIL;
	root->all_result_relids =
		parse->resultRelation ? bms_make_singleton(parse->resultRelation) : NULL;
	root->leaf_result_relids = NULL;	/* we'll find out leaf-ness later */
	root->append_rel_list = NIL;
	root->row_identity_vars = NIL;
	root->rowMarks = NIL;
	memset(root->upper_rels, 0, sizeof(root->upper_rels));
	memset(root->upper_targets, 0, sizeof(root->upper_targets));
	root->processed_tlist = NIL;
	root->update_colnos = NIL;
	root->grouping_map = NULL;
	root->recursiveOk = true;
	root->minmax_aggs = NIL;
	root->qual_security_level = 0;
	root->hasRecursion = hasRecursion;
	if (hasRecursion)
		root->wt_param_id = SS_assign_special_param(root);
	else
		root->wt_param_id = -1;
	root->non_recursive_path = NULL;
	root->partColsUpdated = false;
#ifdef __OPENTENBASE__
	root->plannerinfo_seq_no = glob->plannerinfo_seq_no;
	glob->plannerinfo_seq_no++;
    root->hasUserDefinedFun = false;
#endif
#ifdef _MLS_
	root->hasClsPolicy = false;
#endif

#ifdef _PG_ORCL_
	if (parse->commandType == CMD_INSERT && parse->resultRelation)
	{
		Relation	target_rel;
		Oid		gSeqId = InvalidOid;

		target_rel = heap_open(getrelid(parse->resultRelation, parse->rtable),
									NoLock);
		if (target_rel->rd_locator_info)
			is_target_replicated = IsRelationReplicated(target_rel->rd_locator_info);
		heap_close(target_rel, NoLock);

		/* Initialize it once */
		root->target_rel_gSeqId = gSeqId;
	}
#endif

	if (ORA_MODE && list_length(parse->rtable) != 0 &&
		contain_plpgsql_local_functions((Node *) parse))
	{
		elog(ERROR, "local functions should not be used in SQL");
	}

	/* now check user defined pull-up function */
	if (parse->commandType == CMD_SELECT)
		root->hasUserDefinedFun = contain_user_defined_functions((Node *) parse);
	else
	{
		if (contain_user_defined_functions_warning((Node *) parse))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Using pull-up functions in DML statements is not supported"),
						errhint("Consider using ALTER FUNCTION PUSHDOWN to enable pushdown execution.")));
		}

		root->hasUserDefinedFun = false;
	}

	if (parse->commandType == CMD_INSERT && contain_opentenbaseLO_functions((Node *) parse->onConflict))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Upsert action with large object function is not supported")));
	}
	else if (contain_opentenbaseLO_functions((Node *) parse))
	{
		root->hasUserDefinedFun = true;
		if (!IS_CENTRALIZED_DATANODE && parse->commandType == CMD_INSERT && is_target_replicated)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Insert to replicated table with large object function is not supported")));
		}
		else if (!IS_CENTRALIZED_DATANODE &&
				 (parse->commandType == CMD_UPDATE ||
				  parse->commandType == CMD_DELETE ||
				  parse->commandType == CMD_MERGE))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Using large-object functions in DML statements is not supported")));
		}
	}


	/*
	 * Find functions with only a single sql search statement in the
	 * function body in targetList, and inline them.
	 */
	if (ORA_MODE && enable_inline_target_function && IS_CENTRALIZED_DATANODE)
		inline_target_functions(root);

	/*
	 * If there is a WITH list, process each WITH query and either convert it
	 * to RTE_SUBQUERY RTE(s) or build an initplan SubPlan structure for it.
	 */
	if (parse->cteList)
		SS_process_ctes(root);

	/*
	 * If it's a MERGE command, transform the joinlist as appropriate.
	 */
	transform_MERGE_to_join(parse);

	/*
	 * Look for ANY and EXISTS SubLinks in WHERE and JOIN/ON clauses, and try
	 * to transform them into joins.  Note that this step does not descend
	 * into subqueries; if we pull up any subqueries below, their SubLinks are
	 * processed just before pulling them up.
	 *
	 * OpenTenBase: according to opentenbase_ora semantic, condition in where clause should be
	 * "other qual" in CONNECT BY sql, pull up such sublink to join will turn
	 * these qual into "join qual" and push down below connect by execution
	 * so don't try to pull up any sublink as opentenbase_ora.
	 */
	if (parse->hasSubLinks && !parse->connectByExpr)
		pull_up_sublinks(root);

	/*
	 * Scan the rangetable for set-returning functions, and inline them if
	 * possible (producing subqueries that might get pulled up next).
	 * Recursion issues here are handled in the same way as for SubLinks.
	 */
	inline_set_returning_functions(root);

	/*
	 * Check to see if any subqueries in the jointree can be merged into this
	 * query.
	 */
	pull_up_subqueries(root);

	/*
	 * If this is a simple UNION ALL query, flatten it into an appendrel. We
	 * do this now because it requires applying pull_up_subqueries to the leaf
	 * queries of the UNION ALL, which weren't touched above because they
	 * weren't referenced by the jointree (they will be after we do this).
	 */
	if (parse->setOperations)
		flatten_simple_union_all(root);

	/*
	 * Survey the rangetable to see what kinds of entries are present.  We can
	 * skip some later processing if relevant SQL features are not used; for
	 * example if there are no JOIN RTEs we can avoid the expense of doing
	 * flatten_join_alias_vars().  This must be done after we have finished
	 * adding rangetable entries, of course.  (Note: actually, processing of
	 * inherited or partitioned rels can cause RTEs for their child tables to
	 * get added later; but those must all be RTE_RELATION entries, so they
	 * don't invalidate the conclusions drawn here.)
	 */
	root->hasJoinRTEs = false;
	root->hasLateralRTEs = false;
	hasOuterJoins = false;
	foreach(l, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		if (rte->rtekind == RTE_JOIN)
		{
			root->hasJoinRTEs = true;
			if (IS_OUTER_JOIN(rte->jointype))
				hasOuterJoins = true;
		}
		else if (rte->rtekind == RTE_RELATION && rte->inh)
		{
			/*
			 * Check to see if the relation actually has any children;
			 * if not, clear the inh flag so we can treat it as a
			 * plain base relation.
			 *
			 * Note: this could give a false-positive result, if the
			 * rel once had children but no longer does.  We used to
			 * be able to clear rte->inh later on when we discovered
			 * that, but no more; we have to handle such cases as
			 * full-fledged inheritance.
			 */
			rte->inh = has_subclass(rte->relid);
		}

		if (rte->lateral)
			root->hasLateralRTEs = true;
	}

	/*
	 * If we have now verified that the query target relation is
	 * non-inheriting, mark it as a leaf target.
	 */
	if (parse->resultRelation)
	{
		RangeTblEntry *rte = rt_fetch(parse->resultRelation, parse->rtable);

		if (!rte->inh)
			root->leaf_result_relids =
				bms_make_singleton(parse->resultRelation);
	}

	/*
	 * Preprocess RowMark information.  We need to do this after subquery
	 * pullup, so that all base relations are present.
	 */
	preprocess_rowmarks(root);

	/*
	 * Set hasHavingQual to remember if HAVING clause is present.  Needed
	 * because preprocess_expression will reduce a constant-true condition to
	 * an empty qual list ... but "HAVING TRUE" is not a semantic no-op.
	 */
	root->hasHavingQual = (parse->havingQual != NULL);

	/* Clear this flag; might get set in distribute_qual_to_rels */
	root->hasPseudoConstantQuals = false;

	parse->targetList = (List *)
		preprocess_expression(root, (Node *) parse->targetList,
							  EXPRKIND_TARGET);

	/* Constant-folding might have removed all set-returning functions */
	if (parse->hasTargetSRFs)
		parse->hasTargetSRFs = expression_returns_set((Node *) parse->targetList);

	newWithCheckOptions = NIL;
	foreach(l, parse->withCheckOptions)
	{
		WithCheckOption *wco = (WithCheckOption *) lfirst(l);

		wco->qual = preprocess_expression(root, wco->qual,
										  EXPRKIND_QUAL);
		if (wco->qual != NULL)
			newWithCheckOptions = lappend(newWithCheckOptions, wco);
	}
	parse->withCheckOptions = newWithCheckOptions;

	parse->returningList = (List *)
		preprocess_expression(root, (Node *) parse->returningList,
							  EXPRKIND_TARGET);

	preprocess_qual_conditions(root, (Node *) parse->jointree);

	/* preprocess CONNECT BY related clauses */
	if (parse->connectByExpr)
	{
		List        *join_quals = NIL;
		List        *other_quals = NIL;

		/* start qual and connect by qual are same as parse->jointree->quals */
		parse->connectByExpr->start =
			preprocess_expression(root, (Node *) parse->connectByExpr->start,
								  EXPRKIND_QUAL);
		parse->connectByExpr->expr =
			preprocess_expression(root, (Node *) parse->connectByExpr->expr,
								  EXPRKIND_QUAL);
		/*
		 * If there are more than one table in our query, split out
		 * join qualifiers for pushing down.
		 */
		join_quals = (List *) connectby_split_jquals(root, parse->jointree->quals, true);
		other_quals = (List *) connectby_split_jquals(root, parse->jointree->quals, false);

		/* append join quals into both start and connect qual */
		parse->connectByExpr->start = (Node *)
			list_concat((List *) parse->connectByExpr->start, join_quals);
		parse->connectByExpr->expr = (Node *)
			list_concat((List *) parse->connectByExpr->expr, join_quals);
		/* preserve other quals */
		root->other_qual = other_quals;

		/* process START WITH qual first in grouping_planner */
		parse->jointree->quals = parse->connectByExpr->start;
	}

	parse->havingQual = preprocess_expression(root, parse->havingQual,
											  EXPRKIND_QUAL);

	foreach(l, parse->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(l);

		/* partitionClause/orderClause are sort/group expressions */
		wc->startOffset = preprocess_expression(root, wc->startOffset,
												EXPRKIND_LIMIT);
		wc->endOffset = preprocess_expression(root, wc->endOffset,
											  EXPRKIND_LIMIT);
	}

	parse->limitOffset = preprocess_expression(root, parse->limitOffset,
											   EXPRKIND_LIMIT);
	parse->limitCount = preprocess_expression(root, parse->limitCount,
											  EXPRKIND_LIMIT);


	if (parse->mergeActionList)
	{
		int merge_action_mode = MERGE_ACTION_TYPES(parse->mergeActionList);
		foreach (l, parse->mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(l);

			if (action->commandType == CMD_INSERT && IS_PGXC_LOCAL_COORDINATOR &&
			    CONTAINS_ACTIONS(merge_action_mode, (MERGE_INSERT | MERGE_UPDATE)))
			{
				action->rmt_targetList = (List *) preprocess_expression(
					root, (Node *) copyObject(action->targetList), EXPRKIND_TARGET);
				action->rmt_qual =
					preprocess_expression(root, (Node *) copyObject(action->qual), EXPRKIND_QUAL);
			}
			action->targetList =
				(List *) preprocess_expression(root, (Node *) action->targetList, EXPRKIND_TARGET);

			action->qual = preprocess_expression(root, (Node *) action->qual, EXPRKIND_QUAL);

			if (action->commandType == CMD_UPDATE && action->deleteAction)
			{
				MergeAction *delAction = action->deleteAction;

				delAction->qual =
					preprocess_expression(root, (Node *) delAction->qual, EXPRKIND_QUAL);
			}
		}
	}
	parse->mergeSourceTargetList =
            (List*)preprocess_expression(root, (Node*)parse->mergeSourceTargetList, EXPRKIND_TARGET);

	if (parse->onConflict)
	{
		parse->onConflict->arbiterElems = (List *)
			preprocess_expression(root,
								  (Node *) parse->onConflict->arbiterElems,
								  EXPRKIND_ARBITER_ELEM);
		parse->onConflict->arbiterWhere =
			preprocess_expression(root,
								  parse->onConflict->arbiterWhere,
								  EXPRKIND_QUAL);
		parse->onConflict->onConflictSet = (List *)
			preprocess_expression(root,
								  (Node *) parse->onConflict->onConflictSet,
								  EXPRKIND_TARGET);
		parse->onConflict->onConflictWhere =
			preprocess_expression(root,
								  parse->onConflict->onConflictWhere,
								  EXPRKIND_QUAL);
#ifdef _MLS_
        {   
            int             rt_index;
            RangeTblEntry * rte;
            
            rt_index = 1;
        	while (rt_index <= list_length(root->parse->rtable))
        	{
                rte = rt_fetch(rt_index, root->parse->rtable);
                if (rte->cls_expr)
                {
                    if (NULL == parse->onConflict->onConflictWhere)
                    {
                        parse->onConflict->onConflictWhere = 
            			    preprocess_expression(root,
            								  rte->cls_expr,
            								  EXPRKIND_QUAL);
                    }
                    else
                    {
                        parse->onConflict->onConflictWhere = (Node*)lappend((List*)(parse->onConflict->onConflictWhere), 
                                                                            rte->cls_expr);
                    }
                }
                rt_index++;
        	}
        }
#endif

		/* exclRelTlist contains only Vars, so no preprocessing needed */
	}

	root->append_rel_list = (List *)
		preprocess_expression(root, (Node *) root->append_rel_list,
							  EXPRKIND_APPINFO);

	/* Also need to preprocess expressions within RTEs */
	foreach(l, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
		int			kind;
		ListCell   *lcsq;

		if (rte->rtekind == RTE_RELATION)
		{
			if (rte->tablesample)
				rte->tablesample = (TableSampleClause *)
					preprocess_expression(root,
										  (Node *) rte->tablesample,
										  EXPRKIND_TABLESAMPLE);
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			/*
			 * We don't want to do all preprocessing yet on the subquery's
			 * expressions, since that will happen when we plan it.  But if it
			 * contains any join aliases of our level, those have to get
			 * expanded now, because planning of the subquery won't do it.
			 * That's only possible if the subquery is LATERAL.
			 */
			if (rte->lateral && root->hasJoinRTEs)
				rte->subquery = (Query *)
					flatten_join_alias_vars(root, (Node *) rte->subquery);
		}
		else if (rte->rtekind == RTE_FUNCTION)
		{
			/* Preprocess the function expression(s) fully */
			kind = rte->lateral ? EXPRKIND_RTFUNC_LATERAL : EXPRKIND_RTFUNC;
			rte->functions = (List *)
				preprocess_expression(root, (Node *) rte->functions, kind);
		}
		else if (rte->rtekind == RTE_TABLEFUNC)
		{
			/* Preprocess the function expression(s) fully */
			kind = rte->lateral ? EXPRKIND_TABLEFUNC_LATERAL : EXPRKIND_TABLEFUNC;
			rte->tablefunc = (TableFunc *)
				preprocess_expression(root, (Node *) rte->tablefunc, kind);
		}
		else if (rte->rtekind == RTE_VALUES)
		{
			/* Preprocess the values lists fully */
			kind = rte->lateral ? EXPRKIND_VALUES_LATERAL : EXPRKIND_VALUES;
			rte->values_lists = (List *)
				preprocess_expression(root, (Node *) rte->values_lists, kind);
		}

		/*
		 * Process each element of the securityQuals list as if it were a
		 * separate qual expression (as indeed it is).  We need to do it this
		 * way to get proper canonicalization of AND/OR structure.  Note that
		 * this converts each element into an implicit-AND sublist.
		 */
		foreach(lcsq, rte->securityQuals)
		{
			lfirst(lcsq) = preprocess_expression(root,
												 (Node *) lfirst(lcsq),
												 EXPRKIND_QUAL);
		}
	}

	/*
	 * In some cases we may want to transfer a HAVING clause into WHERE. We
	 * cannot do so if the HAVING clause contains aggregates (obviously) or
	 * volatile functions (since a HAVING clause is supposed to be executed
	 * only once per group).  We also can't do this if there are any nonempty
	 * grouping sets; moving such a clause into WHERE would potentially change
	 * the results, if any referenced column isn't present in all the grouping
	 * sets.  (If there are only empty grouping sets, then the HAVING clause
	 * must be degenerate as discussed below.)
	 *
	 * Also, it may be that the clause is so expensive to execute that we're
	 * better off doing it only once per group, despite the loss of
	 * selectivity.  This is hard to estimate short of doing the entire
	 * planning process twice, so we use a heuristic: clauses containing
	 * subplans are left in HAVING.  Otherwise, we move or copy the HAVING
	 * clause into WHERE, in hopes of eliminating tuples before aggregation
	 * instead of after.
	 *
	 * If the query has explicit grouping then we can simply move such a
	 * clause into WHERE; any group that fails the clause will not be in the
	 * output because none of its tuples will reach the grouping or
	 * aggregation stage.  Otherwise we must have a degenerate (variable-free)
	 * HAVING clause, which we put in WHERE so that query_planner() can use it
	 * in a gating Result node, but also keep in HAVING to ensure that we
	 * don't emit a bogus aggregated row. (This could be done better, but it
	 * seems not worth optimizing.)
	 *
	 * Note that both havingQual and parse->jointree->quals are in
	 * implicitly-ANDed-list form at this point, even though they are declared
	 * as Node *.
	 */
	newHaving = NIL;
	foreach(l, (List *) parse->havingQual)
	{
		Node	   *havingclause = (Node *) lfirst(l);

		if ((parse->groupClause && parse->groupingSets) ||
			contain_agg_clause(havingclause) ||
			contain_volatile_functions(havingclause) ||
			contain_subplans(havingclause) ||
			contain_rownum(havingclause))
		{
			/* keep it in HAVING */
			newHaving = lappend(newHaving, havingclause);
		}
		else if (parse->groupClause && !parse->groupingSets)
		{
			/* move it to WHERE */
			parse->jointree->quals = (Node *)
				lappend((List *) parse->jointree->quals, havingclause);
		}
		else
		{
			/* put a copy in WHERE, keep it in HAVING */
			parse->jointree->quals = (Node *)
				lappend((List *) parse->jointree->quals,
						copyObject(havingclause));
			newHaving = lappend(newHaving, havingclause);
		}
	}
	parse->havingQual = (Node *) newHaving;

	/* Remove any redundant GROUP BY columns */
	remove_useless_groupby_columns(root);

	/* Push down union order by limit */
	pushdown_union_orderby_limit(root);

	/*
	 * If we have any outer joins, try to reduce them to plain inner joins.
	 * This step is most easily done after we've done expression
	 * preprocessing.
	 */
	if (hasOuterJoins)
		reduce_outer_joins(root);

	/*
	 * Do the main planning.
	 */
	grouping_planner(root, tuple_fraction);

	/*
	 * Capture the set of outer-level param IDs we have access to, for use in
	 * extParam/allParam calculations later.
	 */
	SS_identify_outer_params(root);

	/*
	 * If any initPlans were created in this query level, adjust the surviving
	 * Paths' costs and parallel-safety flags to account for them.  The
	 * initPlans won't actually get attached to the plan tree till
	 * create_plan() runs, but we must include their effects now.
	 */
	final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);
	SS_charge_for_initplans(root, final_rel);

	if (IS_PGXC_COORDINATOR)
	{
		foreach(l, final_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(l);
			if (path->distribution && path->pathkeys)
				path->pathkeys = get_reasonable_pathkeys(path->pathtarget,
														 path->pathkeys,
														 path->parent->relids);

		}
	}

	/*
	 * Make sure we've identified the cheapest Path for the final rel.  (By
	 * doing this here not in grouping_planner, we include initPlan costs in
	 * the decision, though it's unlikely that will change anything.)
	 */
	set_cheapest(final_rel);

	/* 
	 * XCPTODO	
	 * Temporarily block WITH RECURSIVE for most cases 
	 * until we can fix. Allow for pg_catalog tables and replicated tables.
	 */
	{
		int idx;
		recursiveOk = true;

		/* seems to start at 1... */
		for (idx = 1; idx < root->simple_rel_array_size && recursiveOk; idx++)
		{
			RangeTblEntry *rte;

			rte = planner_rt_fetch(idx, root);
			if (!rte)
			   continue;
		
			switch (rte->rtekind)
			{
				case RTE_JOIN:
				case RTE_VALUES:
				case RTE_CTE:
				case RTE_FUNCTION:
					continue;
				case RTE_RELATION:
					{
						char loc_type;

						loc_type = GetRelationLocType(rte->relid);

						/* skip pg_catalog */
						if (loc_type == LOCATOR_TYPE_NONE)
							continue;

						/* If replicated, allow */
						if (IsLocatorReplicated(loc_type))
							continue;
						else
							recursiveOk = false;
						break;
					} 
				case RTE_SUBQUERY:
					{
						RelOptInfo *relOptInfo = root->simple_rel_array[idx];
						if (relOptInfo && relOptInfo->subroot &&
								!relOptInfo->subroot->recursiveOk)
							recursiveOk = false;
						break;
					}
				default:	
					recursiveOk = false;
					break;
			}
		}
	}

	/*
	 * XXX This is a bit strange. root->recursiveOk is set to true explicitly,
	 * and now we check it. Harmless, but confusing.
	 */
	if (root->recursiveOk)
		root->recursiveOk = recursiveOk;

	return root;
}

/*
 * preprocess_expression
 *		Do subquery_planner's preprocessing work for an expression,
 *		which can be a targetlist, a WHERE clause (including JOIN/ON
 *		conditions), a HAVING clause, or a few other things.
 */
static Node *
preprocess_expression(PlannerInfo *root, Node *expr, int kind)
{
	/*
	 * Fall out quickly if expression is empty.  This occurs often enough to
	 * be worth checking.  Note that null->null is the correct conversion for
	 * implicit-AND result format, too.
	 */
	if (expr == NULL)
		return NULL;

	/*
	 * If the query has any join RTEs, replace join alias variables with
	 * base-relation variables.  We must do this before sublink processing,
	 * else sublinks expanded out from join aliases would not get processed.
	 * We can skip it in non-lateral RTE functions, VALUES lists, and
	 * TABLESAMPLE clauses, however, since they can't contain any Vars of the
	 * current query level.
	 */
	if (root->hasJoinRTEs &&
		!(kind == EXPRKIND_RTFUNC ||
		  kind == EXPRKIND_VALUES ||
		  kind == EXPRKIND_TABLESAMPLE ||
		  kind == EXPRKIND_TABLEFUNC))
		expr = flatten_join_alias_vars(root, expr);

	/*
	 * Simplify constant expressions.
	 *
	 * Note: an essential effect of this is to convert named-argument function
	 * calls to positional notation and insert the current actual values of
	 * any default arguments for functions.  To ensure that happens, we *must*
	 * process all expressions here.  Previous PG versions sometimes skipped
	 * const-simplification if it didn't seem worth the trouble, but we can't
	 * do that anymore.
	 *
	 * Note: this also flattens nested AND and OR expressions into N-argument
	 * form.  All processing of a qual expression after this point must be
	 * careful to maintain AND/OR flatness --- that is, do not generate a tree
	 * with AND directly under AND, nor OR directly under OR.
	 */
	expr = eval_const_expressions(root, expr);

	/*
	 * If it's a qual or havingQual, canonicalize it.
	 */
	if (kind == EXPRKIND_QUAL)
	{
		expr = (Node *) canonicalize_qual((Expr *) expr, false);

#ifdef OPTIMIZER_DEBUG
		printf("After canonicalize_qual()\n");
		pprint(expr);
#endif
	}

	/* Expand SubLinks to SubPlans */
	if (root->parse->hasSubLinks)
		expr = SS_process_sublinks(root, expr, (kind == EXPRKIND_QUAL));

	/*
	 * XXX do not insert anything here unless you have grokked the comments in
	 * SS_replace_correlation_vars ...
	 */

	/* Replace uplevel vars with Param nodes (this IS possible in VALUES) */
	if (root->query_level > 1)
		expr = SS_replace_correlation_vars(root, expr);

	/*
	 * If it's a qual or havingQual, convert it to implicit-AND format. (We
	 * don't want to do this before eval_const_expressions, since the latter
	 * would be unable to simplify a top-level AND correctly. Also,
	 * SS_process_sublinks expects explicit-AND format.)
	 */
	if (kind == EXPRKIND_QUAL)
		expr = (Node *) make_ands_implicit((Expr *) expr);

	return expr;
}

/*
 * preprocess_qual_conditions
 *		Recursively scan the query's jointree and do subquery_planner's
 *		preprocessing work on each qual condition found therein.
 */
static void
preprocess_qual_conditions(PlannerInfo *root, Node *jtnode)
{
	if (jtnode == NULL)
		return;
	if (IsA(jtnode, RangeTblRef))
	{
		/* nothing to do here */
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;

		foreach(l, f->fromlist)
			preprocess_qual_conditions(root, lfirst(l));

		f->quals = preprocess_expression(root, f->quals, EXPRKIND_QUAL);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		preprocess_qual_conditions(root, j->larg);
		preprocess_qual_conditions(root, j->rarg);

		j->quals = preprocess_expression(root, j->quals, EXPRKIND_QUAL);
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * set_rte_seq_info
 *	Set relation information for rte.
 */
void
set_rte_seq_info(PlannerInfo *root)
{
	int		rti;
	int		rti_sub;
	RangeTblEntry *rte;
	RangeTblEntry *rte_sub;

	if (root->parse == NULL ||
		root->parse->rtable == NULL)
		return;

	for (rti = 0; rti < list_length(root->parse->rtable); rti++)
	{
		rte = (RangeTblEntry *) list_nth(root->parse->rtable, rti);

		if(rte == NULL)
			continue;

		rte->plannerinfo_seq_no = root->plannerinfo_seq_no;
		rte->rte_array_id = rti + 1;

		if (root->parse->plannerinfo_seq_no > 0 &&
			root->parse->rte_array_id > 0)
		{
			rte->parent_plannerinfo_seq_no = root->parse->plannerinfo_seq_no;
			rte->parent_rte_array_id = root->parse->rte_array_id;	
		}

		/* If table is suqbuery, mark query node for parent PlannerInfo
		 * sequence number and RTE array id.
		 */
		if (rte->rtekind == RTE_SUBQUERY &&
			rte->subquery != NULL)
		{
			rte->subquery->plannerinfo_seq_no = root->plannerinfo_seq_no;
			rte->subquery->rte_array_id = rti + 1;

			for (rti_sub = 0; rti_sub < list_length(rte->subquery->rtable); rti_sub++)
			{
				rte_sub = (RangeTblEntry *) list_nth(rte->subquery->rtable, rti_sub);

				if(rte_sub == NULL)
					continue;

				if (rte->plannerinfo_seq_no > 0 &&
					rte->rte_array_id > 0 &&
					rte_sub->parent_plannerinfo_seq_no == 0 &&
					rte_sub->parent_rte_array_id == 0)
				{
					rte_sub->parent_plannerinfo_seq_no = rte->plannerinfo_seq_no;
					rte_sub->parent_rte_array_id = rte->rte_array_id;	
				}
			}
		}
	}

	return;
}

/*
 * preprocess_phv_expression
 *	  Do preprocessing on a PlaceHolderVar expression that's been pulled up.
 *
 * If a LATERAL subquery references an output of another subquery, and that
 * output must be wrapped in a PlaceHolderVar because of an intermediate outer
 * join, then we'll push the PlaceHolderVar expression down into the subquery
 * and later pull it back up during find_lateral_references, which runs after
 * subquery_planner has preprocessed all the expressions that were in the
 * current query level to start with.  So we need to preprocess it then.
 */
Expr *
preprocess_phv_expression(PlannerInfo *root, Expr *expr)
{
	return (Expr *) preprocess_expression(root, (Node *) expr, EXPRKIND_PHV);
}


/*--------------------
 * grouping_planner
 *	  Perform planning steps related to grouping, aggregation, etc.
 *
 * This function adds all required top-level processing to the scan/join
 * Path(s) produced by query_planner.
 *
 * tuple_fraction is the fraction of tuples we expect will be retrieved.
 * tuple_fraction is interpreted as follows:
 *	  0: expect all tuples to be retrieved (normal case)
 *	  0 < tuple_fraction < 1: expect the given fraction of tuples available
 *		from the plan to be retrieved
 *	  tuple_fraction >= 1: tuple_fraction is the absolute number of tuples
 *		expected to be retrieved (ie, a LIMIT specification)
 *
 * Returns nothing; the useful output is in the Paths we attach to the
 * (UPPERREL_FINAL, NULL) upperrel in *root.  In addition,
 * root->processed_tlist contains the final processed targetlist.
 *
 * Note that we have not done set_cheapest() on the final rel; it's convenient
 * to leave this to the caller.
 *--------------------
 */
static void
grouping_planner(PlannerInfo *root, double tuple_fraction)
{
	Query	   *parse = root->parse;
	int64		offset_est = 0;
	int64		count_est = 0;
	double		limit_tuples = -1.0;
	bool		have_postponed_srfs = false;
	bool		have_postponed_cnfs = false;
	PathTarget *final_target;
	List	   *final_targets;
	List	   *final_targets_contain_srfs;
	bool		final_target_parallel_safe;
	RelOptInfo *current_rel;
	RelOptInfo *final_rel;
	ListCell   *lc;
	bool        ora_nulllimits = false;
	bool		parallel_modify_partial_path_added = false;

	root -> parallel_dml = false;
	if (IS_CENTRALIZED_DATANODE && is_parallel_dml_safe(root->parse))
		root -> parallel_dml = true;
	
	/* Tweak caller-supplied tuple_fraction if have LIMIT/OFFSET */
	if (parse->limitCount || parse->limitOffset)
	{
		tuple_fraction = preprocess_limit(root, tuple_fraction,
										  &offset_est, &count_est, &ora_nulllimits);

		/*
		 * If we have a known LIMIT, and don't have an unknown OFFSET, we can
		 * estimate the effects of using a bounded sort.
		 */
		if (count_est > 0 && offset_est >= 0)
			limit_tuples = (double) count_est + (double) offset_est;
	}

	/* Make tuple_fraction accessible to lower-level routines */
	root->tuple_fraction = tuple_fraction;

	if (parse->setOperations)
	{
		/*
		 * If there's a top-level ORDER BY, assume we have to fetch all the
		 * tuples.  This might be too simplistic given all the hackery below
		 * to possibly avoid the sort; but the odds of accurate estimates here
		 * are pretty low anyway.  XXX try to get rid of this in favor of
		 * letting plan_set_operations generate both fast-start and
		 * cheapest-total paths.
		 */
		if (parse->sortClause)
			root->tuple_fraction = 0.0;

		/*
		 * Construct Paths for set operations.  The results will not need any
		 * work except perhaps a top-level sort and/or LIMIT.  Note that any
		 * special work for recursive unions is the responsibility of
		 * plan_set_operations.
		 */
		current_rel = plan_set_operations(root);

		/*
		 * We should not need to call preprocess_targetlist, since we must be
		 * in a SELECT query node.  Instead, use the processed_tlist returned
		 * by plan_set_operations (since this tells whether it returned any
		 * resjunk columns!), and transfer any sort key information from the
		 * original tlist.
		 */
		Assert(parse->commandType == CMD_SELECT);

		/* for safety, copy processed_tlist instead of modifying in-place */
		root->processed_tlist =
			postprocess_setop_tlist(copyObject(root->processed_tlist),
									parse->targetList);

		/* Also extract the PathTarget form of the setop result tlist */
		final_target = current_rel->cheapest_total_path->pathtarget;

		/* And check whether it's parallel safe */
		final_target_parallel_safe =
			is_parallel_safe(root, (Node *) final_target->exprs);

		/* The setop result tlist couldn't contain any SRFs */
		Assert(!parse->hasTargetSRFs);
		final_targets = final_targets_contain_srfs = NIL;

		/*
		 * Can't handle FOR [KEY] UPDATE/SHARE here (parser should have
		 * checked already, but let's make sure).
		 */
		if (parse->rowMarks)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			/*------
			  translator: %s is a SQL row locking clause such as FOR UPDATE */
					 errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT",
							LCS_asString(((RowMarkClause *)
										  linitial(parse->rowMarks))->strength))));

		/*
		 * Calculate pathkeys that represent result ordering requirements
		 */
		Assert(parse->distinctClause == NIL);
		root->sort_pathkeys = make_pathkeys_for_sortclauses(root,
															parse->sortClause,
															root->processed_tlist);
	
		set_rte_seq_info(root);
	}
	else
	{
		/* No set operations, do regular planning */
		PathTarget *sort_input_target;
		List	   *sort_input_targets;
		List	   *sort_input_targets_contain_srfs;
		bool		sort_input_target_parallel_safe;
		PathTarget *grouping_target;
		List	   *grouping_targets;
		List	   *grouping_targets_contain_srfs;
		bool		grouping_target_parallel_safe;
		PathTarget *scanjoin_target;
		List	   *scanjoin_targets;
		List	   *scanjoin_targets_contain_srfs;
		bool		scanjoin_target_parallel_safe;
		bool		scanjoin_target_same_exprs;
		PathTarget *cn_process_target = NULL;
		List	   *cn_process_targets = NIL;
		List	   *cn_process_targets_contain_srfs = NIL;
		bool		have_grouping = false;
		AggClauseCosts agg_costs;
		WindowFuncLists *wflists = NULL;
		List	   *activeWindows = NIL;
		grouping_sets_data *gset_data = NULL;
		standard_qp_extra qp_extra;
		/* A recursive query should always have setOperations */
		Assert(!root->hasRecursion);
		/* Preprocess grouping sets and GROUP BY clause, if any */
		if (parse->groupingSets)
		{
			gset_data = preprocess_grouping_sets(root);
		}
		else
		{
			/* Preprocess regular GROUP BY clause, if any */
			if (parse->groupClause)
				parse->groupClause = preprocess_groupclause(root, NIL);
		}

		/*
		 * Preprocess targetlist.  Note that much of the remaining planning
		 * work will be done with the PathTarget representation of tlists, but
		 * we must also maintain the full representation of the final tlist so
		 * that we can transfer its decoration (resnames etc) to the topmost
		 * tlist of the finished Plan.  This is kept in processed_tlist.
		 */
		preprocess_targetlist(root);

		/*
		 * Collect statistics about aggregates for estimating costs, and mark
		 * all the aggregates with resolved aggtranstypes.  We must do this
		 * before slicing and dicing the tlist into various pathtargets, else
		 * some copies of the Aggref nodes might escape being marked with the
		 * correct transtypes.
		 *
		 * Note: currently, we do not detect duplicate aggregates here.  This
		 * may result in somewhat-overestimated cost, which is fine for our
		 * purposes since all Paths will get charged the same.  But at some
		 * point we might wish to do that detection in the planner, rather
		 * than during executor startup.
		 */
		MemSet(&agg_costs, 0, sizeof(AggClauseCosts));
		if (parse->hasAggs)
		{
			get_agg_clause_costs(root, (Node *) root->processed_tlist,
								 AGGSPLIT_SIMPLE, &agg_costs);
			get_agg_clause_costs(root, parse->havingQual, AGGSPLIT_SIMPLE,
								 &agg_costs);
		}

		/*
		 * Locate any window functions in the tlist.  (We don't need to look
		 * anywhere else, since expressions used in ORDER BY will be in there
		 * too.)  Note that they could all have been eliminated by constant
		 * folding, in which case we don't need to do any more work.
		 */
		if (parse->hasWindowFuncs)
		{
			wflists = find_window_functions((Node *) root->processed_tlist,
											list_length(parse->windowClause));
			if (wflists->numWindowFuncs > 0)
				activeWindows = select_active_windows(root, wflists);
			else
				parse->hasWindowFuncs = false;
		}

		/*
		 * Preprocess MIN/MAX aggregates, if any.  Note: be careful about
		 * adding logic between here and the query_planner() call.  Anything
		 * that is needed in MIN/MAX-optimizable cases will have to be
		 * duplicated in planagg.c.
		 */
		if (parse->hasAggs)
			preprocess_minmax_aggregates(root);

		/*
		 * Figure out whether there's a hard limit on the number of rows that
		 * query_planner's result subplan needs to return.  Even if we know a
		 * hard limit overall, it doesn't apply if the query has any
		 * grouping/aggregation operations, or SRFs in the tlist.
		 */
		if (parse->groupClause ||
			parse->groupingSets ||
			parse->distinctClause ||
			parse->hasAggs ||
			parse->hasWindowFuncs ||
			parse->hasTargetSRFs ||
			root->hasHavingQual)
			root->limit_tuples = -1.0;
		else
			root->limit_tuples = limit_tuples;

		/* Set up data needed by standard_qp_callback */
		qp_extra.activeWindows = activeWindows;
		qp_extra.groupClause = (gset_data
								? (gset_data->rollups ? ((RollupData *) linitial(gset_data->rollups))->groupClause : NIL)
								: parse->groupClause);

		/*
		 * Generate the best unsorted and presorted paths for the scan/join
		 * portion of this Query, ie the processing represented by the
		 * FROM/WHERE clauses.  (Note there may not be any presorted paths.)
		 * We also generate (in standard_qp_callback) pathkey representations
		 * of the query's sort clause, distinct clause, etc.
		 */
		current_rel = query_planner(root, standard_qp_callback, &qp_extra);

		/*
		 * Convert the query's result tlist into PathTarget format.
		 *
		 * Note: this cannot be done before query_planner() has performed
		 * appendrel expansion, because that might add resjunk entries to
		 * root->processed_tlist.  Waiting till afterwards is also helpful
		 * because the target width estimates can use per-Var width numbers
		 * that were obtained within query_planner().
		 */
		final_target = create_pathtarget(root, root->processed_tlist);
		final_target_parallel_safe =
			is_parallel_safe(root, (Node *) final_target->exprs);

		/*
		 * If ORDER BY was given, consider whether we should use a post-sort
		 * projection, and compute the adjusted target for preceding steps if
		 * so.
		 */
		if (parse->sortClause)
		{
			sort_input_target = make_sort_input_target(root,
													   final_target,
													   &have_postponed_srfs,
													   &have_postponed_cnfs);
			sort_input_target_parallel_safe =
				is_parallel_safe(root, (Node *) sort_input_target->exprs);
		}
		else
		{
			sort_input_target = final_target;
			sort_input_target_parallel_safe = final_target_parallel_safe;
		}

		/*
		 * If we have window functions to deal with, the output from any
		 * grouping step needs to be what the window functions want;
		 * otherwise, it should be sort_input_target.
		 */
		if (activeWindows)
		{
			grouping_target = make_window_input_target(root,
													   final_target,
													   activeWindows);
			grouping_target_parallel_safe =
				is_parallel_safe(root, (Node *) grouping_target->exprs);
			have_postponed_cnfs = false;
		}
		else
		{
			grouping_target = sort_input_target;
			grouping_target_parallel_safe = sort_input_target_parallel_safe;
		}

		/*
		 * If we have grouping or aggregation to do, the topmost scan/join
		 * plan node must emit what the grouping step wants; otherwise, it
		 * should emit grouping_target.
		 */
		have_grouping = (parse->groupClause || parse->groupingSets ||
						 parse->hasAggs || root->hasHavingQual);
		if (have_grouping)
		{
			scanjoin_target = make_group_input_target(root, final_target);
			scanjoin_target_parallel_safe =
				is_parallel_safe(root, (Node *) scanjoin_target->exprs);
			have_postponed_cnfs = false;
		}
		else
		{
			scanjoin_target = grouping_target;
			scanjoin_target_parallel_safe = grouping_target_parallel_safe;
		}

		/*
		 * If the query contains rownum/connect-by, the scanjoin_target
		 * will be modified soon, copy it to make other targets clean.
		 */
		if (parse->connectByExpr || parse->hasRowNumExpr || root->hasUserDefinedFun)
		{
			grouping_target_parallel_safe = false;
			sort_input_target_parallel_safe = false;
			final_target_parallel_safe = false;

			if (scanjoin_target == grouping_target)
				scanjoin_target = copy_pathtarget(scanjoin_target);
		}


		/*
		 * In postgresql, vars in qual didn't count into targetlist as junk,
		 * since they are evaluated just after scan happened, but a qual with
		 * cn-expr will be evaluated after collecting tuple to CN, so we need
		 * to pull out vars from them.
		 *
		 * This is a bit ugly doing things here, but root->rownum_quals and
		 * root->udf_quals are determined after query_planner, and targetlist
		 * is determined way before that.
		 */
		if (current_rel->cn_qual)
		{
			List    *quals_var = pull_var_clause((Node *) current_rel->cn_qual,
												 PVC_RECURSE_AGGREGATES |
												 PVC_RECURSE_WINDOWFUNCS |
												 PVC_INCLUDE_PLACEHOLDERS);

			add_new_columns_to_pathtarget(scanjoin_target, quals_var);
			list_free(quals_var);
		}

		cn_process_target = scanjoin_target;
		/* exclude cn-expr from scanjoin_target */
		scanjoin_target = make_cn_input_target(root, scanjoin_target);

		/*
		 * If there are any SRFs in the targetlist, we must separate each of
		 * these PathTargets into SRF-computing and SRF-free targets.  Replace
		 * each of the named targets with a SRF-free version, and remember the
		 * list of additional projection steps we need to add afterwards.
		 */
		if (parse->hasTargetSRFs)
		{
			/* final_target doesn't recompute any SRFs in sort_input_target */
			split_pathtarget_at_srfs(root, final_target, sort_input_target,
									 &final_targets,
									 &final_targets_contain_srfs);
			final_target = (PathTarget *) linitial(final_targets);
			Assert(!linitial_int(final_targets_contain_srfs));
			/* likewise for sort_input_target vs. grouping_target */
			split_pathtarget_at_srfs(root, sort_input_target, grouping_target,
									 &sort_input_targets,
									 &sort_input_targets_contain_srfs);
			sort_input_target = (PathTarget *) linitial(sort_input_targets);
			Assert(!linitial_int(sort_input_targets_contain_srfs));
			/* likewise for grouping_target vs. scanjoin_target */
			split_pathtarget_at_srfs(root, grouping_target, scanjoin_target,
									 &grouping_targets,
									 &grouping_targets_contain_srfs);
			grouping_target = (PathTarget *) linitial(grouping_targets);
			Assert(!linitial_int(grouping_targets_contain_srfs));
			/* scanjoin_target will not have any SRFs precomputed for it */
			split_pathtarget_at_srfs(root, scanjoin_target, NULL,
									 &scanjoin_targets,
									 &scanjoin_targets_contain_srfs);
			scanjoin_target = (PathTarget *) linitial(scanjoin_targets);
			Assert(!linitial_int(scanjoin_targets_contain_srfs));
			split_pathtarget_at_srfs(root, cn_process_target, NULL,
									 &cn_process_targets,
									 &cn_process_targets_contain_srfs);
			cn_process_target = (PathTarget *) linitial(cn_process_targets);
			Assert(!linitial_int(cn_process_targets_contain_srfs));

		}
		else
		{
			/* initialize lists; for most of these, dummy values are OK */
			final_targets = final_targets_contain_srfs = NIL;
			sort_input_targets = sort_input_targets_contain_srfs = NIL;
			grouping_targets = grouping_targets_contain_srfs = NIL;
			scanjoin_targets = list_make1(scanjoin_target);
			scanjoin_targets_contain_srfs = NIL;
		}

		/*
		 * If the final scan/join target is not parallel-safe, we must
		 * generate Gather paths now, since no partial paths will be generated
		 * with the final scan/join targetlist.  Otherwise, the Gather or
		 * Gather Merge paths generated within apply_scanjoin_target_to_paths
		 * will be superior to any we might generate now in that the
		 * projection will be done in by each participant rather than only in
		 * the leader.
		 */
		if (!scanjoin_target_parallel_safe)
			generate_gather_paths(root, current_rel, false);

		/* Apply scan/join target. */
		scanjoin_target_same_exprs = list_length(scanjoin_targets) == 1
			&& equal(scanjoin_target->exprs, current_rel->reltarget->exprs);
		apply_scanjoin_target_to_paths(root, current_rel, scanjoin_targets,
									   scanjoin_targets_contain_srfs,
									   scanjoin_target_parallel_safe,
									   scanjoin_target_same_exprs,
									   have_postponed_cnfs);
		/*
		 * Save the various upper-rel PathTargets we just computed into
		 * root->upper_targets[].  The core code doesn't use this, but it
		 * provides a convenient place for extensions to get at the info.  For
		 * consistency, we save all the intermediate targets, even though some
		 * of the corresponding upperrels might not be needed for this query.
		 */
		root->upper_targets[UPPERREL_FINAL] = final_target;
		root->upper_targets[UPPERREL_WINDOW] = sort_input_target;
		root->upper_targets[UPPERREL_GROUP_AGG] = grouping_target;

		if (parse->connectByExpr || parse->hasRowNumExpr || root->hasUserDefinedFun)
			create_cn_expr_paths(
				root, current_rel, cn_process_target, cn_process_targets, cn_process_targets_contain_srfs,
				(!have_grouping && !activeWindows && final_target == sort_input_target)
					? final_target
					: NULL);

		/*
		 * If we have grouping and/or aggregation, consider ways to implement
		 * that.  We build a new upperrel representing the output of this
		 * phase.
		 */
		if (have_grouping)
		{
			current_rel = create_grouping_paths(root,
												current_rel,
												grouping_target,
												grouping_target_parallel_safe,
												&agg_costs,
												gset_data);
			/* Fix things up if grouping_target contains SRFs */
			if (parse->hasTargetSRFs)
				adjust_paths_for_srfs(root, current_rel,
									  grouping_targets,
									  grouping_targets_contain_srfs);
		}

		/*
		 * If we have window functions, consider ways to implement those.  We
		 * build a new upperrel representing the output of this phase.
		 */
		if (activeWindows)
		{
			current_rel = create_window_paths(root,
											  current_rel,
											  grouping_target,
											  sort_input_target,
											  sort_input_target_parallel_safe,
											  wflists,
											  activeWindows);
			/* Fix things up if sort_input_target contains SRFs */
			if (parse->hasTargetSRFs)
				adjust_paths_for_srfs(root, current_rel,
									  sort_input_targets,
									  sort_input_targets_contain_srfs);
		}

		/*
		 * If there is a DISTINCT clause, consider ways to implement that. We
		 * build a new upperrel representing the output of this phase.
		 */
		if (parse->distinctClause)
		{
			current_rel = create_distinct_paths(root,
												current_rel);
		}
	}							/* end of if (setOperations) */

	/*
	 * If ORDER BY was given, consider ways to implement that, and generate a
	 * new upperrel containing only paths that emit the correct ordering and
	 * project the correct final_target.  We can apply the original
	 * limit_tuples limit in sort costing here, but only if there are no
	 * postponed SRFs.
	 */
	if (parse->sortClause)
	{
		current_rel = create_ordered_paths(root,
										   current_rel,
										   final_target,
										   final_target_parallel_safe,
										   have_postponed_srfs ? -1.0 :
										   limit_tuples);
		/* Fix things up if final_target contains SRFs */
		if (parse->hasTargetSRFs)
			adjust_paths_for_srfs(root, current_rel,
								  final_targets,
								  final_targets_contain_srfs);
	}

	/*
	 * Now we are prepared to build the final-output upperrel.
	 */
	final_rel = fetch_upper_rel(root, UPPERREL_FINAL, NULL);

	/*
	 * If the input rel is marked consider_parallel and there's nothing that's
	 * not parallel-safe in the LIMIT clause, then the final_rel can be marked
	 * consider_parallel as well.  Note that if the query has rowMarks or is
	 * not a SELECT, consider_parallel will be false for every relation in the
	 * query.
	 */
	if (current_rel->consider_parallel &&
		is_parallel_safe(root, parse->limitOffset) &&
		is_parallel_safe(root, parse->limitCount))
		final_rel->consider_parallel = true;

	/*
	 * If the current_rel belongs to a single FDW, so does the final_rel.
	 */
	final_rel->serverid = current_rel->serverid;
	final_rel->userid = current_rel->userid;
	final_rel->useridiscurrent = current_rel->useridiscurrent;
	final_rel->fdwroutine = current_rel->fdwroutine;

	if (root->parse->commandType == CMD_MERGE)
	{
		Path *path = get_cheapest_fractional_path(current_rel, root->tuple_fraction);
		path = generate_final_rel_path(root, parse, offset_est, count_est, limit_tuples, final_rel, ora_nulllimits,
		                               &parallel_modify_partial_path_added, path, false);

		/* And shove it into final_rel */
		add_path(final_rel, path);
	}
	else
	{
		/*
	    * Generate paths for the final_rel.  Insert all surviving paths, with
	    * LockRows, Limit, and/or ModifyTable steps added if needed.
		*/
		foreach(lc, current_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			path = generate_final_rel_path(root, parse, offset_est, count_est, limit_tuples, final_rel, ora_nulllimits,
									&parallel_modify_partial_path_added, path, false);

			/* And shove it into final_rel */
			add_path(final_rel, path);
		}
	}
	/* Consider a supported parallel table-modification command */
	if (IS_CENTRALIZED_DATANODE && root->parallel_dml && parse->rowMarks == NIL)
	{
		foreach(lc, current_rel->partial_pathlist)
		{
			Path *path = (Path *) lfirst(lc);

			path = generate_final_rel_path(root, parse, offset_est, count_est, limit_tuples, final_rel, ora_nulllimits,
										   &parallel_modify_partial_path_added, path, true);

			add_partial_path(final_rel, path);
			parallel_modify_partial_path_added = true;
		}
	}
	
	if (parallel_modify_partial_path_added)
	{
		/*
		 * Generate gather paths according to the added partial paths for the
		 * parallel table-modification command.
		 * Note that true is passed for the "override_rows" parameter, so that
		 * the rows from the cheapest partial path (ModifyTablePath) are used,
		 * not the rel's (possibly estimated) rows.
		 */
		generate_gather_paths(root, final_rel, true);
	}
	
	/*
	 * Generate partial paths for final_rel, too, if outer query levels might
	 * be able to make use of them.
	 */
	if (final_rel->consider_parallel && root->query_level > 1 &&
		!limit_needed(parse))
	{
		Assert(!parse->rowMarks && parse->commandType == CMD_SELECT);
		foreach(lc, current_rel->partial_pathlist)
		{
			Path	   *partial_path = (Path *) lfirst(lc);

			add_partial_path(final_rel, partial_path);
		}
	}

	/*
	 * If there is an FDW that's responsible for all baserels of the query,
	 * let it consider adding ForeignPaths.
	 */
	if (final_rel->fdwroutine &&
		final_rel->fdwroutine->GetForeignUpperPaths)
		final_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_FINAL,
													current_rel, final_rel);

	/* Let extensions possibly add some more paths */
	if (create_upper_paths_hook)
		(*create_upper_paths_hook) (root, UPPERREL_FINAL,
									current_rel, final_rel);

	/* Note: currently, we leave it to callers to do set_cheapest() */
}

static Path *
generate_final_rel_path(PlannerInfo *root, Query *parse, int64 offset_est, int64 count_est, double limit_tuples,
						RelOptInfo *final_rel, bool ora_nulllimits, bool *parallel_modify_partial_path_added,
						Path *path_tmp, bool isParallelModify)
{
	Path *path = path_tmp;
	/*
	 * If there is a FOR [KEY] UPDATE/SHARE clause, add the LockRows node.
	 * (Note: we intentionally test parse->rowMarks not root->rowMarks
	 * here.  If there are only non-locking rowmarks, they should be
	 * handled by the ModifyTable node instead.  However, root->rowMarks
	 * is what goes into the LockRows node.)
	 */
	if (parse->rowMarks)
	{
		path = (Path *) create_lockrows_path(root, final_rel, path,
												root->rowMarks,
												SS_assign_special_param(root));
	}

	/*
	 * If there is a LIMIT/OFFSET clause, add the LIMIT node.
	 */
	if (limit_needed(parse))
	{
		/* If needed, add a LimitPath on top of a RemoteSubplan. */
		if (path->distribution && !ora_nulllimits &&
			(!IsLocatorReplicated(path->distribution->distributionType) || 
			path->distribution->replication_for_update))
		{
			/*
			 * Try to push down LIMIT clause to the remote node, in order
			 * to limit the number of rows that get shipped over network.
			 * This can be done even if there is an ORDER BY clause, as
			 * long as we fetch at least (limit + offset) rows from all the
			 * nodes and then do a local sort and apply the original limit.
			 *
			 * We can only push down the LIMIT clause when it's a constant.
			 * Similarly, if the OFFSET is specified, then it must be constant
			 * too.
			 * 
			 * To be compatible with opentenbase_ora, the syntax 'fetch first/next
			 * percent_cnt PERCENT'requires passing complete boundary information.
			 * Offset and count adjustments are made during function
			 * ExecLimitGetSlot execution.
			 *
			 * Simple expressions get folded into constants by the time we come
			 * here. So this works well in case of constant expressions such as
			 *
			 *     SELECT .. LIMIT (1024 * 1024);
			 */
			if (limit_tuples > 0 && (limit_tuples <= (double)INT64_MAX) &&
				parse->limitOption != LIMIT_OPTION_PERCENT_COUNT &&
				parse->limitOption != LIMIT_OPTION_PERCENT_WITH_TIES)
			{
				Node       *limit_count = NULL;
				Node       *limit_offset = NULL;
				int64       new_offset_est = 0;
				int64       new_count_est = 0;

				if (offset_est &&
					(parse->limitOption == LIMIT_OPTION_PERCENT_COUNT ||
					 parse->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES))
				{
					Oid   target_typeoid = FLOAT8OID;

					limit_offset = (Node *) makeConst(target_typeoid, -1,
													  InvalidOid,
													  sizeof(int64),
													  Float8GetDatum((float8) offset_est),
													  false, FLOAT8PASSBYVAL);

					limit_count = (Node *) makeConst(target_typeoid, -1,
													 InvalidOid,
													 sizeof(int64),
													 Float8GetDatum((float8) count_est),
													 false, FLOAT8PASSBYVAL);

					new_offset_est = offset_est;
					new_count_est = count_est;
				}
				else
				{
					Oid   target_typeoid = (ORA_MODE) ? FLOAT8OID : INT8OID;
					Datum total_est = (ORA_MODE) ? Float8GetDatum((float8) (offset_est + count_est)) : Int64GetDatum(offset_est + count_est);

					limit_count = (Node *) makeConst(target_typeoid, -1,
													 InvalidOid,
													 sizeof(int64),
													 total_est,
													 false, FLOAT8PASSBYVAL);

					new_offset_est = 0;

					/* LIMIT + OFFSET */
					new_count_est = offset_est + count_est;
				}

				path = (Path *) create_limit_path(root, final_rel, path,
												  limit_offset,
												  limit_count,
												  parse->limitOption,
												  new_offset_est, new_count_est);
			}

			/*
			 * Add replication distribution for subquery only.
			 * Actually, the final limit can be executed on any datanode,
			 * and can be treated as replicated.
			 */
			if (root->query_level > 1 && 
				!path->distribution->replication_for_update)
			{
				path = create_remotesubplan_replicated_path(root, path);

				/* The limit with sort does not require a secondary reshuffle */
				if (path->pathkeys == NULL)
					path->distribution->replication_for_limit = true;
			}
			else
				path = create_remotesubplan_path(root, path, NULL);

			/*
			 * Path keys may be cleared by plan LockRows. The limit node could only appear
			 * at the end of query block, so the pathkeys could be set as root sort keys
			 * as logic for setting RemotePlan.
			 */
			if (parse->rowMarks)
			{
				path->pathkeys = root->sort_pathkeys ? root->sort_pathkeys : path->pathkeys;
			}
		}

		path = (Path *) create_limit_path(root, final_rel, path,
										  parse->limitOffset,
										  parse->limitCount,
										  parse->limitOption,
										  offset_est, count_est);
	}
		/*
		 * Under opentenbase_ora_compatible mode, if there is RowNumExpr, add a RemoteSubplan.
		 * For example:
		 * query - explain select * from (select rownum r from aa) e where r > 5;
		 * before -
		 *                                 QUERY PLAN
		 * ----------------------------------------------------------------------------
		 *  Remote Subquery Scan on all (dn1,dn2)  (cost=0.00..32.73 rows=337 width=8)
		 *   ->  Subquery Scan on e  (cost=0.00..32.73 rows=337 width=8)
		 *          Filter: (e.r > 5)
		 *          ->  Seq Scan on aa  (cost=0.00..20.10 rows=1010 width=8)
		 *  (4 rows)
		 *
		 * after -
		 *                                   QUERY PLAN
		 *  --------------------------------------------------------------------------------------
		 *  Subquery Scan on e  (cost=100.00..137.78 rows=337 width=8)
		 *      Filter: (e.r > 5)
		 *      ->  Remote Subquery Scan on all (dn1,dn2)  (cost=100.00..125.15 rows=1010 width=8)
		 *          ->  Seq Scan on aa  (cost=0.00..20.10 rows=1010 width=8)
		 *  (4 rows)
		 */
	else if (ORA_MODE && parse->hasRowNumExpr && path->distribution)
	{
		if (IsA(path, SubqueryScanPath))
		{
			Path *subpath = ((SubqueryScanPath *)path)->subpath;
			subpath = create_remotesubplan_path(root, subpath, NULL);
			((SubqueryScanPath *)path)->subpath = subpath;
			path->distribution = subpath->distribution;
		}
		else
		{
			path = create_remotesubplan_path(root, path, NULL);
		}
	}

	/*
	 * If this is an INSERT/UPDATE/DELETE/MERGE, add the ModifyTable node.
	 */
	if (parse->commandType != CMD_SELECT)
	{
		List	   *partitioned_rels = NIL;
		List	   *resultRelations = NIL;
		List	   *updateColnosLists = NIL;
		List	   *withCheckOptionLists = NIL;
		List       *returningLists = NIL;
		List	   *mergeActionLists = NIL;
		List	   *rowMarks;

		if (bms_membership(root->all_result_relids) == BMS_MULTIPLE)
		{
			/* Inherited UPDATE/DELETE */
			RelOptInfo *top_result_rel = find_base_rel(root,
													   parse->resultRelation);
			int			resultRelation = -1;

			/* Add only leaf children to ModifyTable. */
			while ((resultRelation = bms_next_member(root->leaf_result_relids,
													 resultRelation)) >= 0)
			{
				RelOptInfo *this_result_rel = find_base_rel(root,
															resultRelation);

				/*
				 * Also exclude any leaf rels that have turned dummy since
				 * being added to the list, for example, by being excluded
				 * by constraint exclusion.
				 */
				if (IS_DUMMY_REL(this_result_rel))
					continue;

				/* Build per-target-rel lists needed by ModifyTable */
				resultRelations = lappend_int(resultRelations,
											  resultRelation);
				if (parse->commandType == CMD_UPDATE)
				{
					List	   *update_colnos = root->update_colnos;

					if (this_result_rel != top_result_rel)
						update_colnos =
							adjust_inherited_attnums_multilevel(root,
																update_colnos,
																this_result_rel->relid,
																top_result_rel->relid);
					updateColnosLists = lappend(updateColnosLists,
												update_colnos);
				}
				if (parse->withCheckOptions)
				{
					List	   *withCheckOptions = parse->withCheckOptions;

					if (this_result_rel != top_result_rel)
						withCheckOptions = (List *)
							adjust_appendrel_attrs_multilevel(root,
															  (Node *) withCheckOptions,
															  this_result_rel->relids,
															  top_result_rel->relids);
					withCheckOptionLists = lappend(withCheckOptionLists,
												   withCheckOptions);
				}
				if (parse->returningList)
				{
					List	   *returningList = parse->returningList;

					if (this_result_rel != top_result_rel)
						returningList = (List *)
							adjust_appendrel_attrs_multilevel(root,
															  (Node *) returningList,
															  this_result_rel->relids,
															  top_result_rel->relids);
					returningLists = lappend(returningLists,
											 returningList);
				}
				if (parse->mergeActionList)
				{
					ListCell   *l;
					List	   *mergeActionList = NIL;
					/* 1 for update estore, -1 for update row, 0 for not update */
					int16       update_info = 0;
					RangeTblEntry *target_rte;
					Relation target_relation;

					target_rte = rt_fetch(parse->resultRelation, parse->rtable);

					target_relation = heap_open(target_rte->relid, NoLock);
					update_info = check_rel_for_uppdate(target_relation);
					heap_close(target_relation, NoLock);
					/*
					 * Copy MergeActions and translate stuff that
					 * references attribute numbers.
					 */
					foreach(l, parse->mergeActionList)
					{
						MergeAction *action = lfirst(l),
							*leaf_action = copyObject(action);

						if (!(action->commandType == CMD_UPDATE && update_info > 0))
						{
							leaf_action->qual =
								adjust_appendrel_attrs_multilevel(root,
																  (Node *) action->qual,
																  this_result_rel->relids,
																  top_result_rel->relids);
							leaf_action->targetList =
								(List *) adjust_appendrel_attrs_multilevel(
									root,
									(Node *) action->targetList,
									this_result_rel->relids,
									top_result_rel->relids);
						}

						if (leaf_action->commandType == CMD_UPDATE)
						{
							leaf_action->updateColnos =
								adjust_inherited_attnums_multilevel(root,
																	action->updateColnos,
																	this_result_rel->relid,
																	top_result_rel->relid);
							if (ORA_MODE && action->deleteAction)
							{
								MergeAction *delAction = leaf_action->deleteAction;
								delAction->targetList =
									(List *) adjust_appendrel_attrs_multilevel(
										root,
										(Node *) delAction->targetList,
										this_result_rel->relids,
										top_result_rel->relids);

								delAction->qual =
									adjust_appendrel_attrs_multilevel(root,
																	  (Node *) delAction->qual,
																	  this_result_rel->relids,
																	  top_result_rel->relids);
							}
						}

						mergeActionList = lappend(mergeActionList,
												  leaf_action);
					}

					mergeActionLists = lappend(mergeActionLists,
											   mergeActionList);
				}
			}

			if (resultRelations == NIL)
			{
				/*
				 * We managed to exclude every child rel, so generate a
				 * dummy one-relation plan using info for the top target
				 * rel (even though that may not be a leaf target).
				 * Although it's clear that no data will be updated or
				 * deleted, we still need to have a ModifyTable node so
				 * that any statement triggers will be executed.  (This
				 * could be cleaner if we fixed nodeModifyTable.c to allow
				 * zero target relations, but that probably wouldn't be a
				 * net win.)
				 */
				resultRelations = list_make1_int(parse->resultRelation);
				if (parse->commandType == CMD_UPDATE)
					updateColnosLists = list_make1(root->update_colnos);
				if (parse->withCheckOptions)
					withCheckOptionLists = list_make1(parse->withCheckOptions);
				if (parse->returningList)
					returningLists = list_make1(parse->returningList);
				if (parse->mergeActionList)
					mergeActionLists = list_make1(parse->mergeActionList);
			}
		}
		else
		{
			/* Single-relation INSERT/UPDATE/DELETE/MERGE. */
			resultRelations = list_make1_int(parse->resultRelation);
			if (parse->commandType == CMD_UPDATE)
				updateColnosLists = list_make1(root->update_colnos);
			if (parse->withCheckOptions)
				withCheckOptionLists = list_make1(parse->withCheckOptions);
			if (parse->returningList)
				returningLists = list_make1(parse->returningList);
			if (parse->mergeActionList)
				mergeActionLists = list_make1(parse->mergeActionList);
		}

		/*
		 * If target is a partition root table, we need to mark the
		 * ModifyTable node appropriately for that.
		 * ora_compatible
		 */
		if ((PG_MODE || !parse->multi_inserts) && rt_fetch(parse->resultRelation, parse->rtable)->relkind ==
												  RELKIND_PARTITIONED_TABLE)
		{
			ListCell   *lc;
			Bitmapset  *parent_relids;
			int			i = -1;

			parent_relids = bms_make_singleton(parse->resultRelation);
			foreach(lc, root->append_rel_list)
			{
				AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);

				/* append_rel_list contains all append rels; ignore others */
				if (!bms_is_member(appinfo->parent_relid, parent_relids))
					continue;

				/* if child is itself partitioned, update parent_relids */
				if (rt_fetch(appinfo->child_relid, root->parse->rtable)->inh)
				{
					parent_relids = bms_add_member(parent_relids, appinfo->child_relid);
				}
			}

			while ((i = bms_next_member(parent_relids, i)) >= 0)
				partitioned_rels = lappend_int(partitioned_rels, i);

			bms_free(parent_relids);

			/*
			 * If we're going to create ModifyTable at all, the list should
			 * contain at least one member, that is, the root parent's index.
			 */
			Assert(list_length(partitioned_rels) >= 1);
			partitioned_rels = list_make1(partitioned_rels);
		}

		/*
		 * If there was a FOR [KEY] UPDATE/SHARE clause, the LockRows node
		 * will have dealt with fetching non-locked marked rows, else we
		 * need to have ModifyTable do that.
		 */
		if (parse->rowMarks)
			rowMarks = NIL;
		else
			rowMarks = root->rowMarks;

		/* Multiple insert case at the top query plan. */
		if (root->query_level == 1 && root->glob->subInsertPlans != NULL)
		{
		}
		else if (!root->glob->is_multi_insert)
		{
			/* Normal single insert case */

			/*
			 * Adjust path by injecting a remote subplan, if appropriate, so
			 * that the ModifyTablePath gets properly distributed data.
			 */
			root->mergeActionLists = mergeActionLists;
			if ((!path->distribution && root->distribution && bms_num_members(root->distribution->nodes) > 1) ||
				(path->distribution))
				path = adjust_path_distribution(root, parse, path);
			root->mergeActionLists = NIL;

		}

#ifdef __OPENTENBASE__
		/*
		 * unshippable triggers found on target relation, we have to do DML
		 * on coordinator.
		 */
		if (parse->hasUnshippableDml)
		{
			if (path->distribution)
			{
				path = adjust_modifytable_subpath(root, parse, path);
			}
		}
#endif

		{
			int parallelWorkers = 0;

			if (bms_membership(root->all_result_relids) == BMS_SINGLETON &&
				root->parallel_dml &&
				parse->rowMarks == NIL)
			{
				parallelWorkers = path->parallel_workers;
				(*parallel_modify_partial_path_added) = true;
			}

			path = (Path *)
				create_modifytable_path(root, final_rel,
										path,
										parse->commandType,
										parse->canSetTag,
										parse->resultRelation,
										partitioned_rels,
										root->partColsUpdated,
										resultRelations,
										updateColnosLists,
										withCheckOptionLists,
										returningLists,
										rowMarks,
										parse->onConflict,
										mergeActionLists,
										SS_assign_special_param(root),
										parallelWorkers);
		}
	}
	else
		/* Adjust path by injecting a remote subplan, if appropriate. */
		path = adjust_path_distribution(root, parse, path);
		
	return path;
}

/*
 * Do preprocessing for groupingSets clause and related data.  This handles the
 * preliminary steps of expanding the grouping sets, organizing them into lists
 * of rollups, and preparing annotations which will later be filled in with
 * size estimates.
 */
static grouping_sets_data *
preprocess_grouping_sets(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	List	   *sets;
	int			maxref = 0;
	ListCell   *lc;
	ListCell   *lc_set;
	grouping_sets_data *gd = palloc0(sizeof(grouping_sets_data));

	parse->groupingSets = expand_grouping_sets(parse->groupingSets, -1);

	gd->any_hashable = false;
	gd->unhashable_refs = NULL;
	gd->unsortable_refs = NULL;
	gd->unsortable_sets = NIL;

	if (parse->groupClause)
	{
		ListCell   *lc;

		foreach(lc, parse->groupClause)
		{
			SortGroupClause *gc = lfirst(lc);
			Index		ref = gc->tleSortGroupRef;

			if (ref > maxref)
				maxref = ref;

			if (!gc->hashable)
				gd->unhashable_refs = bms_add_member(gd->unhashable_refs, ref);

			if (!OidIsValid(gc->sortop))
				gd->unsortable_refs = bms_add_member(gd->unsortable_refs, ref);
		}
	}

	/* Allocate workspace array for remapping */
	gd->tleref_to_colnum_map = (int *) palloc((maxref + 1) * sizeof(int));

	/*
	 * If we have any unsortable sets, we must extract them before trying to
	 * prepare rollups. Unsortable sets don't go through
	 * reorder_grouping_sets, so we must apply the GroupingSetData annotation
	 * here.
	 */
	if (!bms_is_empty(gd->unsortable_refs))
	{
		List	   *sortable_sets = NIL;

		foreach(lc, parse->groupingSets)
		{
			List	   *gset = lfirst(lc);

			if (bms_overlap_list(gd->unsortable_refs, gset))
			{
				GroupingSetData *gs = makeNode(GroupingSetData);

				gs->set = gset;
				gd->unsortable_sets = lappend(gd->unsortable_sets, gs);

				/*
				 * We must enforce here that an unsortable set is hashable;
				 * later code assumes this.  Parse analysis only checks that
				 * every individual column is either hashable or sortable.
				 *
				 * Note that passing this test doesn't guarantee we can
				 * generate a plan; there might be other showstoppers.
				 */
				if (bms_overlap_list(gd->unhashable_refs, gset))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("could not implement GROUP BY"),
							 errdetail("Some of the datatypes only support hashing, while others only support sorting.")));
			}
			else
				sortable_sets = lappend(sortable_sets, gset);
		}

		if (sortable_sets)
			sets = extract_rollup_sets(sortable_sets);
		else
			sets = NIL;
	}
	else
		sets = extract_rollup_sets(parse->groupingSets);

	foreach(lc_set, sets)
	{
		List	   *current_sets = (List *) lfirst(lc_set);
		RollupData *rollup = makeNode(RollupData);
		GroupingSetData *gs;

		/*
		 * Reorder the current list of grouping sets into correct prefix
		 * order.  If only one aggregation pass is needed, try to make the
		 * list match the ORDER BY clause; if more than one pass is needed, we
		 * don't bother with that.
		 *
		 * Note that this reorders the sets from smallest-member-first to
		 * largest-member-first, and applies the GroupingSetData annotations,
		 * though the data will be filled in later.
		 */
		current_sets = reorder_grouping_sets(current_sets,
											 (list_length(sets) == 1
											  ? parse->sortClause
											  : NIL));

		/*
		 * Get the initial (and therefore largest) grouping set.
		 */
		gs = linitial(current_sets);

		/*
		 * Order the groupClause appropriately.  If the first grouping set is
		 * empty, then the groupClause must also be empty; otherwise we have
		 * to force the groupClause to match that grouping set's order.
		 *
		 * (The first grouping set can be empty even though parse->groupClause
		 * is not empty only if all non-empty grouping sets are unsortable.
		 * The groupClauses for hashed grouping sets are built later on.)
		 */
		if (gs->set)
			rollup->groupClause = preprocess_groupclause(root, gs->set);
		else
			rollup->groupClause = NIL;

		/*
		 * Is it hashable? We pretend empty sets are hashable even though we
		 * actually force them not to be hashed later. But don't bother if
		 * there's nothing but empty sets (since in that case we can't hash
		 * anything).
		 */
		if (gs->set &&
			!bms_overlap_list(gd->unhashable_refs, gs->set))
		{
			rollup->hashable = true;
			gd->any_hashable = true;
		}

		/*
		 * Now that we've pinned down an order for the groupClause for this
		 * list of grouping sets, we need to remap the entries in the grouping
		 * sets from sortgrouprefs to plain indices (0-based) into the
		 * groupClause for this collection of grouping sets. We keep the
		 * original form for later use, though.
		 */
		rollup->gsets = remap_to_groupclause_idx(rollup->groupClause,
												 current_sets,
												 gd->tleref_to_colnum_map);
		rollup->gsets_data = current_sets;

		gd->rollups = lappend(gd->rollups, rollup);
	}

	if (gd->unsortable_sets)
	{
		/*
		 * We have not yet pinned down a groupclause for this, but we will
		 * need index-based lists for estimation purposes. Construct
		 * hash_sets_idx based on the entire original groupclause for now.
		 */
		gd->hash_sets_idx = remap_to_groupclause_idx(parse->groupClause,
													 gd->unsortable_sets,
													 gd->tleref_to_colnum_map);
		gd->any_hashable = true;
	}

	return gd;
}

/*
 * Given a groupclause and a list of GroupingSetData, return equivalent sets
 * (without annotation) mapped to indexes into the given groupclause.
 */
static List *
remap_to_groupclause_idx(List *groupClause,
						 List *gsets,
						 int *tleref_to_colnum_map)
{
	int			ref = 0;
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, groupClause)
	{
		SortGroupClause *gc = lfirst(lc);

		tleref_to_colnum_map[gc->tleSortGroupRef] = ref++;
	}

	foreach(lc, gsets)
	{
		List	   *set = NIL;
		ListCell   *lc2;
		GroupingSetData *gs = lfirst(lc);

		foreach(lc2, gs->set)
		{
			set = lappend_int(set, tleref_to_colnum_map[lfirst_int(lc2)]);
		}

		result = lappend(result, set);
	}

	return result;
}

/*
 * Detect whether a plan node is a "dummy" plan created when a relation
 * is deemed not to need scanning due to constraint exclusion.
 *
 * Currently, such dummy plans are Result nodes with constant FALSE
 * filter quals (see set_dummy_rel_pathlist and create_append_plan).
 *
 * XXX this probably ought to be somewhere else, but not clear where.
 */
bool
is_dummy_plan(Plan *plan)
{
	if (IsA(plan, Result))
	{
		List	   *rcqual = (List *) ((Result *) plan)->resconstantqual;

		if (list_length(rcqual) == 1)
		{
			Const	   *constqual = (Const *) linitial(rcqual);

			if (constqual && IsA(constqual, Const))
			{
				if (!constqual->constisnull &&
					!DatumGetBool(constqual->constvalue))
					return true;
			}
		}
	}
	return false;
}

/*
 * preprocess_rowmarks - set up PlanRowMarks if needed
 */
void
preprocess_rowmarks(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	Bitmapset  *rels;
	List	   *prowmarks;
	ListCell   *l;
	int			i;

	if (parse->rowMarks)
	{
		/*
		 * We've got trouble if FOR [KEY] UPDATE/SHARE appears inside
		 * grouping, since grouping renders a reference to individual tuple
		 * CTIDs invalid.  This is also checked at parse time, but that's
		 * insufficient because of rule substitution, query pullup, etc.
		 */
		CheckSelectLocking(parse, ((RowMarkClause *)
								   linitial(parse->rowMarks))->strength);

		if (!IS_CENTRALIZED_MODE && parse->jointree)
		{
			Bitmapset  *baserels = get_relids_in_jointree((Node *)
					parse->jointree, false);
			int x, num_rels = 0;
			bool dist_found = false;

			while ((x = bms_first_member(baserels)) >= 0)
			{
				RangeTblEntry *rte = rt_fetch(x, parse->rtable);
				RelationLocInfo *locinfo = NULL;
				if (OidIsValid(rte->relid))
					locinfo = GetRelationLocInfo(rte->relid);
				if (locinfo && !IsRelationReplicated(locinfo))
					dist_found = true;
				num_rels++;
			}

			if (dist_found && num_rels > 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("%s is not allowed with joins",
							 LCS_asString(((RowMarkClause *)
									 linitial(parse->rowMarks))->strength))));
		}

		/* Connectby does not support row-level locking */
		if (!IS_CENTRALIZED_MODE && parse->connectByExpr)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("%s is not allowed with connectby",
						LCS_asString(((RowMarkClause *)
									 linitial(parse->rowMarks))->strength))));
	}
	else
	{
		/*
		 * We only need rowmarks for UPDATE, DELETE, or FOR [KEY]
		 * UPDATE/SHARE.
		 */
		if (parse->commandType != CMD_UPDATE &&
			parse->commandType != CMD_DELETE)
		{
			if (parse->commandType == CMD_MERGE)
			{
				ListCell *lc = NULL;
				bool only_insert = true;

				foreach(lc, parse->mergeActionList)
				{
					MergeAction *action = (MergeAction*)lfirst(lc);
					if (action->commandType != CMD_INSERT)
					{
						only_insert = false;
						break;
					}

				}
				if (only_insert)
					return;
			}
			else
				return;
		}
	}

	/*
	 * We need to have rowmarks for all base relations except the target. We
	 * make a bitmapset of all base rels and then remove the items we don't
	 * need or have FOR [KEY] UPDATE/SHARE marks for.
	 */
	rels = get_relids_in_jointree((Node *) parse->jointree, false);
	if (parse->resultRelation)
		rels = bms_del_member(rels, parse->resultRelation);

	/*
	 * Convert RowMarkClauses to PlanRowMark representation.
	 */
	prowmarks = NIL;
	foreach(l, parse->rowMarks)
	{
		RowMarkClause *rc = (RowMarkClause *) lfirst(l);
		RangeTblEntry *rte = rt_fetch(rc->rti, parse->rtable);
		PlanRowMark *newrc;

		/*
		 * Currently, it is syntactically impossible to have FOR UPDATE et al
		 * applied to an update/delete target rel.  If that ever becomes
		 * possible, we should drop the target from the PlanRowMark list.
		 */
		Assert(rc->rti != parse->resultRelation);

		/*
		 * Ignore RowMarkClauses for subqueries; they aren't real tables and
		 * can't support true locking.  Subqueries that got flattened into the
		 * main query should be ignored completely.  Any that didn't will get
		 * ROW_MARK_COPY items in the next loop.
		 */
		if (rte->rtekind != RTE_RELATION)
			continue;

		rels = bms_del_member(rels, rc->rti);

		newrc = makeNode(PlanRowMark);
		newrc->rti = newrc->prti = rc->rti;
		newrc->rowmarkId = ++(root->glob->lastRowMarkId);
		newrc->markType = select_rowmark_type(rte, rc->strength);
		newrc->allMarkTypes = (1 << newrc->markType);
		newrc->strength = rc->strength;
		newrc->waitPolicy = rc->waitPolicy;
		newrc->waitTimeout = rc->waitTimeout;
		newrc->isParent = false;

		prowmarks = lappend(prowmarks, newrc);
	}

	/*
	 * Now, add rowmarks for any non-target, non-locked base relations.
	 */
	i = 0;
	foreach(l, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
		PlanRowMark *newrc;

		i++;
		if (!bms_is_member(i, rels))
			continue;

		newrc = makeNode(PlanRowMark);
		newrc->rti = newrc->prti = i;
		newrc->rowmarkId = ++(root->glob->lastRowMarkId);
		newrc->markType = select_rowmark_type(rte, LCS_NONE);
		newrc->allMarkTypes = (1 << newrc->markType);
		newrc->strength = LCS_NONE;
		newrc->waitPolicy = LockWaitBlock;	/* doesn't matter */
		newrc->waitTimeout = 0; /* doesn't matter */
		newrc->isParent = false;

		prowmarks = lappend(prowmarks, newrc);
	}

	root->rowMarks = prowmarks;
}

/*
 * Select RowMarkType to use for a given table
 */
RowMarkType
select_rowmark_type(RangeTblEntry *rte, LockClauseStrength strength)
{
	if (rte->rtekind != RTE_RELATION)
	{
		/* If it's not a table at all, use ROW_MARK_COPY */
		return ROW_MARK_COPY;
	}
	else if (rte->relkind == RELKIND_FOREIGN_TABLE)
	{
		/* Let the FDW select the rowmark type, if it wants to */
		FdwRoutine *fdwroutine = GetFdwRoutineByRelId(rte->relid);

		if (fdwroutine->GetForeignRowMarkType != NULL)
			return fdwroutine->GetForeignRowMarkType(rte, strength);
		/* Otherwise, use ROW_MARK_COPY by default */
		return ROW_MARK_COPY;
	}
	else
	{
		/* Regular table, apply the appropriate lock type */
		switch (strength)
		{
			case LCS_NONE:

				/*
				 * We don't need a tuple lock, only the ability to re-fetch
				 * the row.
				 */
				return ROW_MARK_REFERENCE;
				break;
			case LCS_FORKEYSHARE:
				return ROW_MARK_KEYSHARE;
				break;
			case LCS_FORSHARE:
				return ROW_MARK_SHARE;
				break;
			case LCS_FORNOKEYUPDATE:
				return ROW_MARK_NOKEYEXCLUSIVE;
				break;
			case LCS_FORUPDATE:
				return ROW_MARK_EXCLUSIVE;
				break;
		}
		elog(ERROR, "unrecognized LockClauseStrength %d", (int) strength);
		return ROW_MARK_EXCLUSIVE;	/* keep compiler quiet */
	}
}

/*
 * preprocess_limit - do pre-estimation for LIMIT and/or OFFSET clauses
 *
 * We try to estimate the values of the LIMIT/OFFSET clauses, and pass the
 * results back in *count_est and *offset_est.  These variables are set to
 * 0 if the corresponding clause is not present, and -1 if it's present
 * but we couldn't estimate the value for it.  (The "0" convention is OK
 * for OFFSET but a little bit bogus for LIMIT: effectively we estimate
 * LIMIT 0 as though it were LIMIT 1.  But this is in line with the planner's
 * usual practice of never estimating less than one row.)  These values will
 * be passed to create_limit_path, which see if you change this code.
 *
 * The return value is the suitably adjusted tuple_fraction to use for
 * planning the query.  This adjustment is not overridable, since it reflects
 * plan actions that grouping_planner() will certainly take, not assumptions
 * about context.
 */
static double
preprocess_limit(PlannerInfo *root, double tuple_fraction,
				 int64 *offset_est, int64 *count_est, bool *ora_nulllimits)
{
	Query	   *parse = root->parse;
	Node	   *est;
	double		limit_fraction;

	/* Should not be called unless LIMIT or OFFSET */
	Assert(parse->limitCount || parse->limitOffset);

	/*
	 * Try to obtain the clause values.  We use estimate_expression_value
	 * primarily because it can sometimes do something useful with Params.
	 */
	if (parse->limitCount)
	{
		est = estimate_expression_value(root, parse->limitCount);
		if (est && IsA(est, Const))
		{
			if (((Const *) est)->constisnull)
			{
				/* Under the opentenbase_ora mode, it will return 0 rows of data. */
				if (ORA_MODE)
				{
					*ora_nulllimits = true;

					/*
					 * Return the minimum sampling value. When tuple_fraction >= 1,
					 * tuple_fraction represents the absolute number of tuples expected to be retrieved
					 * (i.e., similar to a LIMIT specification).
					 */
					return 1;
				}

				/* NULL indicates LIMIT ALL, ie, no limit */
				*count_est = 0; /* treat as not present */
			}
			else
			{
				*count_est = GetlimitCount(((Const *) est)->constvalue,
				                           ((Const *) est)->consttype == FLOAT8OID,
				                           false);
				if (PG_MODE && *count_est <= 0)
					*count_est = 1; /* force to at least 1 */
			}
		}
		else
			*count_est = -1;	/* can't estimate */
	}
	else
		*count_est = 0;			/* not present */

	if (parse->limitOffset)
	{
		est = estimate_expression_value(root, parse->limitOffset);
		if (est && IsA(est, Const))
		{
			if (((Const *) est)->constisnull)
			{
				/* Under the opentenbase_ora mode, it will return 0 rows of data. */
				if (ORA_MODE)
				{
					*ora_nulllimits = true;

					/*
					 * Return the minimum sampling value. When tuple_fraction >= 1,
					 * tuple_fraction represents the absolute number of tuples expected to be retrieved
					 * (i.e., similar to a LIMIT specification).
					 */
					return 1;
				}

				/* Treat NULL as no offset; the executor will too */
				*offset_est = 0;	/* treat as not present */
			}
			else
			{
				*offset_est = GetlimitCount(((Const *) est)->constvalue,
				                            ((Const *) est)->consttype == FLOAT8OID,
				                            false);
				if (PG_MODE && *offset_est < 0)
					*offset_est = 0; /* treat as not present */
			}
		}
		else
			*offset_est = -1;	/* can't estimate */
	}
	else
		*offset_est = 0;		/* not present */

	if (*count_est != 0)
	{
		/*
		 * A LIMIT clause limits the absolute number of tuples returned.
		 * However, if it's not a constant LIMIT then we have to guess; for
		 * lack of a better idea, assume 10% of the plan's result is wanted.
		 */
		if (*count_est < 0 || *offset_est < 0)
		{
			/* LIMIT or OFFSET is an expression ... punt ... */
			limit_fraction = 0.10;
		}
		else
		{
			/* LIMIT (plus OFFSET, if any) is max number of tuples needed */
			limit_fraction = (double) *count_est + (double) *offset_est;
		}

		/*
		 * If we have absolute limits from both caller and LIMIT, use the
		 * smaller value; likewise if they are both fractional.  If one is
		 * fractional and the other absolute, we can't easily determine which
		 * is smaller, but we use the heuristic that the absolute will usually
		 * be smaller.
		 */
		if (tuple_fraction >= 1.0)
		{
			if (limit_fraction >= 1.0)
			{
				/* both absolute */
				tuple_fraction = Min(tuple_fraction, limit_fraction);
			}
			else
			{
				/* caller absolute, limit fractional; use caller's value */
			}
		}
		else if (tuple_fraction > 0.0)
		{
			if (limit_fraction >= 1.0)
			{
				/* caller fractional, limit absolute; use limit */
				tuple_fraction = limit_fraction;
			}
			else
			{
				/* both fractional */
				tuple_fraction = Min(tuple_fraction, limit_fraction);
			}
		}
		else
		{
			/* no info from caller, just use limit */
			tuple_fraction = limit_fraction;
		}
	}
	else if (*offset_est != 0 && tuple_fraction > 0.0)
	{
		/*
		 * We have an OFFSET but no LIMIT.  This acts entirely differently
		 * from the LIMIT case: here, we need to increase rather than decrease
		 * the caller's tuple_fraction, because the OFFSET acts to cause more
		 * tuples to be fetched instead of fewer.  This only matters if we got
		 * a tuple_fraction > 0, however.
		 *
		 * As above, use 10% if OFFSET is present but unestimatable.
		 */
		if (*offset_est < 0)
			limit_fraction = 0.10;
		else
			limit_fraction = (double) *offset_est;

		/*
		 * If we have absolute counts from both caller and OFFSET, add them
		 * together; likewise if they are both fractional.  If one is
		 * fractional and the other absolute, we want to take the larger, and
		 * we heuristically assume that's the fractional one.
		 */
		if (tuple_fraction >= 1.0)
		{
			if (limit_fraction >= 1.0)
			{
				/* both absolute, so add them together */
				tuple_fraction += limit_fraction;
			}
			else
			{
				/* caller absolute, limit fractional; use limit */
				tuple_fraction = limit_fraction;
			}
		}
		else
		{
			if (limit_fraction >= 1.0)
			{
				/* caller fractional, limit absolute; use caller's value */
			}
			else
			{
				/* both fractional, so add them together */
				tuple_fraction += limit_fraction;
				if (tuple_fraction >= 1.0)
					tuple_fraction = 0.0;	/* assume fetch all */
			}
		}
	}

	return tuple_fraction;
}

/*
 * limit_needed - do we actually need a Limit plan node?
 *
 * If we have constant-zero OFFSET and constant-null LIMIT, we can skip adding
 * a Limit node.  This is worth checking for because "OFFSET 0" is a common
 * locution for an optimization fence.  (Because other places in the planner
 * merely check whether parse->limitOffset isn't NULL, it will still work as
 * an optimization fence --- we're just suppressing unnecessary run-time
 * overhead.)
 *
 * This might look like it could be merged into preprocess_limit, but there's
 * a key distinction: here we need hard constants in OFFSET/LIMIT, whereas
 * in preprocess_limit it's good enough to consider estimated values.
 */
static bool
limit_needed(Query *parse)
{
	Node	   *node;

	node = parse->limitCount;
	if (node)
	{
		if (IsA(node, Const))
		{
			/*
			 * pg mode, NULL indicates LIMIT ALL, ie, no limit
			 * opentenbase_ora mode, NULL indicates LIMIT 0
			 */
			if (ORA_MODE || !((Const *) node)->constisnull)
				return true;
		}
		else
			return true;		/* non-constant LIMIT */
	}

	node = parse->limitOffset;
	if (node)
	{
		if (IsA(node, Const))
		{
			/* Treat NULL as no offset; the executor would too */
			if (((Const *) node)->constisnull)
			{
				/* opentenbase_ora mode, Expected to return 0 rows. */
				if (ORA_MODE)
				{
					return true;
				}
			}
			else
			{
				int64 offset = DatumGetInt64(((Const *) node)->constvalue);

				if (offset != 0)
					return true;	/* OFFSET with a nonzero value */
			}
		}
		else
			return true;		/* non-constant OFFSET */
	}

	return false;				/* don't need a Limit plan node */
}

/*
 * remove_useless_groupby_columns
 *		Remove any columns in the GROUP BY clause that are redundant due to
 *		being functionally dependent on other GROUP BY columns.
 *
 * Since some other DBMSes do not allow references to ungrouped columns, it's
 * not unusual to find all columns listed in GROUP BY even though listing the
 * primary-key columns would be sufficient.  Deleting such excess columns
 * avoids redundant sorting work, so it's worth doing.  When we do this, we
 * must mark the plan as dependent on the pkey constraint (compare the
 * parser's check_ungrouped_columns() and check_functional_grouping()).
 *
 * In principle, we could treat any NOT-NULL columns appearing in a UNIQUE
 * index as the determining columns.  But as with check_functional_grouping(),
 * there's currently no way to represent dependency on a NOT NULL constraint,
 * so we consider only the pkey for now.
 */
static void
remove_useless_groupby_columns(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	Bitmapset **groupbyattnos;
	Bitmapset **surplusvars;
	ListCell   *lc;
	int			relid;

	/* No chance to do anything if there are less than two GROUP BY items */
	if (list_length(parse->groupClause) < 2)
		return;

	/* Don't fiddle with the GROUP BY clause if the query has grouping sets */
	if (parse->groupingSets)
		return;

	/*
	 * Scan the GROUP BY clause to find GROUP BY items that are simple Vars.
	 * Fill groupbyattnos[k] with a bitmapset of the column attnos of RTE k
	 * that are GROUP BY items.
	 */
	groupbyattnos = (Bitmapset **) palloc0(sizeof(Bitmapset *) *
										   (list_length(parse->rtable) + 1));
	foreach(lc, parse->groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);
		Var		   *var = (Var *) tle->expr;

		/*
		 * Ignore non-Vars and Vars from other query levels.
		 *
		 * XXX in principle, stable expressions containing Vars could also be
		 * removed, if all the Vars are functionally dependent on other GROUP
		 * BY items.  But it's not clear that such cases occur often enough to
		 * be worth troubling over.
		 */
		if (!IsA(var, Var) ||
			var->varlevelsup > 0)
			continue;

		/* OK, remember we have this Var */
		relid = var->varno;
		Assert(relid <= list_length(parse->rtable));
		groupbyattnos[relid] = bms_add_member(groupbyattnos[relid],
											  var->varattno - FirstLowInvalidHeapAttributeNumber);
	}

	/*
	 * Consider each relation and see if it is possible to remove some of its
	 * Vars from GROUP BY.  For simplicity and speed, we do the actual removal
	 * in a separate pass.  Here, we just fill surplusvars[k] with a bitmapset
	 * of the column attnos of RTE k that are removable GROUP BY items.
	 */
	surplusvars = NULL;			/* don't allocate array unless required */
	relid = 0;
	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		Bitmapset  *relattnos;
		Bitmapset  *pkattnos;
		Oid			constraintOid;

		relid++;

		/* Only plain relations could have primary-key constraints */
		if (rte->rtekind != RTE_RELATION)
			continue;

		/* Nothing to do unless this rel has multiple Vars in GROUP BY */
		relattnos = groupbyattnos[relid];
		if (bms_membership(relattnos) != BMS_MULTIPLE)
			continue;

		/*
		 * Can't remove any columns for this rel if there is no suitable
		 * (i.e., nondeferrable) primary key constraint.
		 */
		pkattnos = get_primary_key_attnos(rte->relid, false, &constraintOid);
		if (pkattnos == NULL)
			continue;

		/*
		 * If the primary key is a proper subset of relattnos then we have
		 * some items in the GROUP BY that can be removed.
		 */
		if (bms_subset_compare(pkattnos, relattnos) == BMS_SUBSET1)
		{
			/*
			 * To easily remember whether we've found anything to do, we don't
			 * allocate the surplusvars[] array until we find something.
			 */
			if (surplusvars == NULL)
				surplusvars = (Bitmapset **) palloc0(sizeof(Bitmapset *) *
													 (list_length(parse->rtable) + 1));

			/* Remember the attnos of the removable columns */
			surplusvars[relid] = bms_difference(relattnos, pkattnos);

			/* Also, mark the resulting plan as dependent on this constraint */
			parse->constraintDeps = lappend_oid(parse->constraintDeps,
												constraintOid);
		}
	}

	/*
	 * If we found any surplus Vars, build a new GROUP BY clause without them.
	 * (Note: this may leave some TLEs with unreferenced ressortgroupref
	 * markings, but that's harmless.)
	 */
	if (surplusvars != NULL)
	{
		List	   *new_groupby = NIL;

		foreach(lc, parse->groupClause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);
			Var		   *var = (Var *) tle->expr;

			/*
			 * New list must include non-Vars, outer Vars, and anything not
			 * marked as surplus.
			 */
			if (!IsA(var, Var) ||
				var->varlevelsup > 0 ||
				!bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber,
							   surplusvars[var->varno]))
				new_groupby = lappend(new_groupby, sgc);
		}

		parse->groupClause = new_groupby;
	}
}

/*
 * check_union_rte
 *	Check whether the limit clause could be pushed down to union legs.
 */
static int
check_union_rte(Node *node, Query *parse)
{
	if (node == NULL || parse == NULL)
		return -1;

	if (IsA(node, SetOperationStmt))
	{
		if (((SetOperationStmt*)node)->all)
			return -1;

		if (((SetOperationStmt*)node)->op == SETOP_UNION)
		{
			int limit_row_left = 0;
			int limit_row_right = 0;

			limit_row_left = check_union_rte(((SetOperationStmt*)node)->larg, parse);
			limit_row_right = check_union_rte(((SetOperationStmt*)node)->rarg, parse);

			if (limit_row_left == -1 ||
				limit_row_right == -1)
				return -1;

			return limit_row_left + limit_row_right;
		}
		else
			return -1;
	}
	else if (IsA(node, RangeTblRef))
	{
		int rindex = ((RangeTblRef*)node)->rtindex;

		if (rindex > 0 && rindex <= list_length(parse->rtable))
		{
			RangeTblEntry *rte = (RangeTblEntry *) list_nth(parse->rtable, rindex - 1);

			if (rte->rtekind == RTE_SUBQUERY &&
				rte->subquery != NULL &&
				rte->subquery->sortClause == NULL &&
				rte->subquery->limitCount == NULL &&
				rte->subquery->limitOffset == NULL &&
				rte->subquery->windowClause == NULL &&
				rte->subquery->groupClause == NULL &&
				rte->subquery->groupingSets == NULL &&
				rte->subquery->distinctClause == NULL &&
				!rte->subquery->hasAggs &&
				!rte->subquery->hasDistinctOn &&
				!rte->subquery->hasModifyingCTE &&
				!rte->subquery->hasTargetSRFs &&
				!rte->subquery->hasWindowFuncs &&
				!contain_volatile_functions((Node *)rte->subquery))
			{
				return 1;
			}

			return -1;
		}
	}

	return -1;
}

/*
 * check_limit_threshold
 *	Check whether limit and offset row number is lower than threshold.
 */
static int
check_limit_threshold(Node *limitCount, Node *limitOffset)
{
	int		limit_count = 0;
	int		limit_offset = 0;

	if (!IsA(limitCount, Const) ||
		(!ORA_MODE && ((Const *) limitCount)->consttype != INT8OID) ||
		union_limit_pushdown < 0)
		return -1;

	if (!IsA(limitCount, Const) || ((Const *) limitCount)->consttype != INT8OID ||
	    (ORA_MODE && ((Const *) limitCount)->consttype != FLOAT8OID) ||
	    (limitOffset != NULL && ((Const *) limitOffset)->consttype != INT8OID))
		return -1;
	
	limit_count = (int) ((Const*)limitCount)->constvalue;

	if (limitOffset != NULL)
		limit_offset = (int) ((Const*)limitOffset)->constvalue;

	if (union_limit_pushdown == 0 ||
		(limit_count + limit_offset) <= union_limit_pushdown)
		return limit_count + limit_offset;
	
	return -1;
}

/*
 * get_targetentry_by_index
 *	Get target entry depending on ressortgroupref.
 */
static TargetEntry*
get_targetentry_by_index(Index idx, List *targetList)
{
	ListCell	*lc;

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->ressortgroupref == idx)
			return tle;
	}

	return NULL;
}

/*
 * get_targetentry_by_resname
 *	Get target entry depending on resname.
 */
static TargetEntry*
get_targetentry_by_resname(char *tle_name, List *targetList)
{
	ListCell	*lc;

	if (tle_name == NULL)
		return NULL;

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resname == NULL)
			continue;

		if (strcmp(tle_name, tle->resname) == 0)
			return tle;
	}

	return NULL;
}

/*
 * check_sort_clause_validation
 *	Pre-check for sor clause pushed down to avoid potential errors.
 */
static bool
check_sort_clause_validation(Query *parse, Query *rte_parse)
{
	ListCell		*lc;
	ListCell		*lc_sort;
	RangeTblEntry 	*rte;
	TargetEntry		*tle;
	TargetEntry		*tle_sub;

	foreach (lc, rte_parse->rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);

		foreach (lc_sort, parse->sortClause)
		{
			Index tleSortGroupRef = ((SortGroupClause *) lfirst(lc_sort))->tleSortGroupRef;

			if (tleSortGroupRef == 0)
				return false;

			tle = get_targetentry_by_index(tleSortGroupRef, parse->targetList);

			if (tle == NULL || tle->resname == NULL)
				return false;

			tle_sub = get_targetentry_by_resname(tle->resname, rte->subquery->targetList);

			if (tle_sub == NULL)
				return false;
		}
	}

	return true;
}

/*
 * pushdown_union_orderby_limit
 *	Try to push down order by and limit clause to union legs.
 */
static void
pushdown_union_orderby_limit(PlannerInfo *root)
{
	Query	   		*parse = root->parse;
	Query	   		*sub_parse;
	Query	   		*rte_parse;
	int				 union_rte = 0;
	ListCell		*lc = NULL;
	ListCell		*lc_sort = NULL;
	RangeTblEntry 	*rte;
	TargetEntry		*tle;
	TargetEntry		*tle_sub;
	int				 limit_num;

	if (parse->rtable == NULL ||
		parse->limitCount == NULL ||
		parse->groupClause != NULL ||
		parse->distinctClause != NULL)
		return;

	/*
	 * Get limit numbers which is the sum of limit number and offset number.
	 */
	limit_num = check_limit_threshold(parse->limitCount, parse->limitOffset);

	if (limit_num < 0)
		return;

	/*
	 * Check validation of pushing down.
	 */
	if (list_length(parse->rtable) == 1)
	{
		rte = (RangeTblEntry *) linitial(parse->rtable);

		if (rte->rtekind != RTE_SUBQUERY ||
			rte->subquery == NULL ||
			rte->subquery->setOperations == NULL ||
			rte->subquery->limitCount != NULL ||
			rte->subquery->sortClause != NULL)
			return;

		union_rte = check_union_rte(rte->subquery->setOperations, rte->subquery);

		rte_parse = rte->subquery;
	}
	else
	{
		union_rte = check_union_rte(parse->setOperations, parse);
		rte_parse = parse;
	}

	if (union_rte <= 0 || union_rte != list_length(rte_parse->rtable))
		return;

	/*
	 * Pre-check for validation of sort clause.
	 */
	if (!check_sort_clause_validation(parse, rte_parse))
		return;

	/*
	 * Build up sort clause and limit clause for subquery of union.
	 */
	foreach (lc, rte_parse->rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);

		sub_parse = rte->subquery;
		sub_parse->limitCount = copyObject(parse->limitCount);
		((Const*) sub_parse->limitCount)->constvalue = limit_num;

		sub_parse->sortClause = copyObject(parse->sortClause);

		foreach (lc_sort, sub_parse->sortClause)
		{
			Oid			sortop;
			Oid			eqop;
			bool		hashable;
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc_sort);
			tle = get_targetentry_by_index(sortcl->tleSortGroupRef, parse->targetList);
			tle_sub = get_targetentry_by_resname(tle->resname, sub_parse->targetList);
			tle_sub->ressortgroupref = tle->ressortgroupref;

			/* determine the eqop and optional sortop */
			get_sort_group_operators(exprType((Node *)tle_sub->expr),
									 true, true, false,
									 &sortop, &eqop, NULL,
									 &hashable);
			sortcl->sortop = sortop;
			sortcl->eqop = eqop;
			sortcl->hashable = hashable;
		}

		sub_parse->distinctClause = transformDistinctClause(NULL,
															&sub_parse->targetList,
															sub_parse->sortClause,
															false);
		sub_parse->hasDistinctOn = true;
	}
}

/*
 * preprocess_groupclause - do preparatory work on GROUP BY clause
 *
 * The idea here is to adjust the ordering of the GROUP BY elements
 * (which in itself is semantically insignificant) to match ORDER BY,
 * thereby allowing a single sort operation to both implement the ORDER BY
 * requirement and set up for a Unique step that implements GROUP BY.
 *
 * In principle it might be interesting to consider other orderings of the
 * GROUP BY elements, which could match the sort ordering of other
 * possible plans (eg an indexscan) and thereby reduce cost.  We don't
 * bother with that, though.  Hashed grouping will frequently win anyway.
 *
 * Note: we need no comparable processing of the distinctClause because
 * the parser already enforced that that matches ORDER BY.
 *
 * For grouping sets, the order of items is instead forced to agree with that
 * of the grouping set (and items not in the grouping set are skipped). The
 * work of sorting the order of grouping set elements to match the ORDER BY if
 * possible is done elsewhere.
 */
static List *
preprocess_groupclause(PlannerInfo *root, List *force)
{
	Query	   *parse = root->parse;
	List	   *new_groupclause = NIL;
	bool		partial_match;
	ListCell   *sl;
	ListCell   *gl;

	/* For grouping sets, we need to force the ordering */
	if (force)
	{
		foreach(sl, force)
		{
			Index		ref = lfirst_int(sl);
			SortGroupClause *cl = get_sortgroupref_clause(ref, parse->groupClause);

			new_groupclause = lappend(new_groupclause, cl);
		}

		return new_groupclause;
	}

	/* If no ORDER BY, nothing useful to do here */
	if (parse->sortClause == NIL)
		return parse->groupClause;

	/*
	 * Scan the ORDER BY clause and construct a list of matching GROUP BY
	 * items, but only as far as we can make a matching prefix.
	 *
	 * This code assumes that the sortClause contains no duplicate items.
	 */
	foreach(sl, parse->sortClause)
	{
		SortGroupClause *sc = (SortGroupClause *) lfirst(sl);

		foreach(gl, parse->groupClause)
		{
			SortGroupClause *gc = (SortGroupClause *) lfirst(gl);

			if (equal(gc, sc))
			{
				new_groupclause = lappend(new_groupclause, gc);
				break;
			}
		}
		if (gl == NULL)
			break;				/* no match, so stop scanning */
	}

	/* Did we match all of the ORDER BY list, or just some of it? */
	partial_match = (sl != NULL);

	/* If no match at all, no point in reordering GROUP BY */
	if (new_groupclause == NIL)
		return parse->groupClause;

	/*
	 * Add any remaining GROUP BY items to the new list, but only if we were
	 * able to make a complete match.  In other words, we only rearrange the
	 * GROUP BY list if the result is that one list is a prefix of the other
	 * --- otherwise there's no possibility of a common sort.  Also, give up
	 * if there are any non-sortable GROUP BY items, since then there's no
	 * hope anyway.
	 */
	foreach(gl, parse->groupClause)
	{
		SortGroupClause *gc = (SortGroupClause *) lfirst(gl);

		if (list_member_ptr(new_groupclause, gc))
			continue;			/* it matched an ORDER BY item */
		if (partial_match)
			return parse->groupClause;	/* give up, no common sort possible */
		if (!OidIsValid(gc->sortop))
			return parse->groupClause;	/* give up, GROUP BY can't be sorted */
		new_groupclause = lappend(new_groupclause, gc);
	}

	/* Success --- install the rearranged GROUP BY list */
	Assert(list_length(parse->groupClause) == list_length(new_groupclause));
	return new_groupclause;
}

/*
 * Extract lists of grouping sets that can be implemented using a single
 * rollup-type aggregate pass each. Returns a list of lists of grouping sets.
 *
 * Input must be sorted with smallest sets first. Result has each sublist
 * sorted with smallest sets first.
 *
 * We want to produce the absolute minimum possible number of lists here to
 * avoid excess sorts. Fortunately, there is an algorithm for this; the problem
 * of finding the minimal partition of a partially-ordered set into chains
 * (which is what we need, taking the list of grouping sets as a poset ordered
 * by set inclusion) can be mapped to the problem of finding the maximum
 * cardinality matching on a bipartite graph, which is solvable in polynomial
 * time with a worst case of no worse than O(n^2.5) and usually much
 * better. Since our N is at most 4096, we don't need to consider fallbacks to
 * heuristic or approximate methods.  (Planning time for a 12-d cube is under
 * half a second on my modest system even with optimization off and assertions
 * on.)
 */
static List *
extract_rollup_sets(List *groupingSets)
{
	int			num_sets_raw = list_length(groupingSets);
	int			num_empty = 0;
	int			num_sets = 0;	/* distinct sets */
	int			num_chains = 0;
	List	   *result = NIL;
	List	  **results;
	List	  **orig_sets;
	Bitmapset **set_masks;
	int		   *chains;
	short	  **adjacency;
	short	   *adjacency_buf;
	BipartiteMatchState *state;
	int			i;
	int			j;
	int			j_size;
	ListCell   *lc1 = list_head(groupingSets);
	ListCell   *lc;

	/*
	 * Start by stripping out empty sets.  The algorithm doesn't require this,
	 * but the planner currently needs all empty sets to be returned in the
	 * first list, so we strip them here and add them back after.
	 */
	while (lc1 && lfirst(lc1) == NIL)
	{
		++num_empty;
		lc1 = lnext(lc1);
	}

	/* bail out now if it turns out that all we had were empty sets. */
	if (!lc1)
		return list_make1(groupingSets);

	/*----------
	 * We don't strictly need to remove duplicate sets here, but if we don't,
	 * they tend to become scattered through the result, which is a bit
	 * confusing (and irritating if we ever decide to optimize them out).
	 * So we remove them here and add them back after.
	 *
	 * For each non-duplicate set, we fill in the following:
	 *
	 * orig_sets[i] = list of the original set lists
	 * set_masks[i] = bitmapset for testing inclusion
	 * adjacency[i] = array [n, v1, v2, ... vn] of adjacency indices
	 *
	 * chains[i] will be the result group this set is assigned to.
	 *
	 * We index all of these from 1 rather than 0 because it is convenient
	 * to leave 0 free for the NIL node in the graph algorithm.
	 *----------
	 */
	orig_sets = palloc0((num_sets_raw + 1) * sizeof(List *));
	set_masks = palloc0((num_sets_raw + 1) * sizeof(Bitmapset *));
	adjacency = palloc0((num_sets_raw + 1) * sizeof(short *));
	adjacency_buf = palloc((num_sets_raw + 1) * sizeof(short));

	j_size = 0;
	j = 0;
	i = 1;

	for_each_cell(lc, lc1)
	{
		List	   *candidate = lfirst(lc);
		Bitmapset  *candidate_set = NULL;
		ListCell   *lc2;
		int			dup_of = 0;

		foreach(lc2, candidate)
		{
			candidate_set = bms_add_member(candidate_set, lfirst_int(lc2));
		}

		/* we can only be a dup if we're the same length as a previous set */
		if (j_size == list_length(candidate))
		{
			int			k;

			for (k = j; k < i; ++k)
			{
				if (bms_equal(set_masks[k], candidate_set))
				{
					dup_of = k;
					break;
				}
			}
		}
		else if (j_size < list_length(candidate))
		{
			j_size = list_length(candidate);
			j = i;
		}

		if (dup_of > 0)
		{
			orig_sets[dup_of] = lappend(orig_sets[dup_of], candidate);
			bms_free(candidate_set);
		}
		else
		{
			int			k;
			int			n_adj = 0;

			orig_sets[i] = list_make1(candidate);
			set_masks[i] = candidate_set;

			/* fill in adjacency list; no need to compare equal-size sets */

			for (k = j - 1; k > 0; --k)
			{
				if (bms_is_subset(set_masks[k], candidate_set))
					adjacency_buf[++n_adj] = k;
			}

			if (n_adj > 0)
			{
				adjacency_buf[0] = n_adj;
				adjacency[i] = palloc((n_adj + 1) * sizeof(short));
				memcpy(adjacency[i], adjacency_buf, (n_adj + 1) * sizeof(short));
			}
			else
				adjacency[i] = NULL;

			++i;
		}
	}

	num_sets = i - 1;

	/*
	 * Apply the graph matching algorithm to do the work.
	 */
	state = BipartiteMatch(num_sets, num_sets, adjacency);

	/*
	 * Now, the state->pair* fields have the info we need to assign sets to
	 * chains. Two sets (u,v) belong to the same chain if pair_uv[u] = v or
	 * pair_vu[v] = u (both will be true, but we check both so that we can do
	 * it in one pass)
	 */
	chains = palloc0((num_sets + 1) * sizeof(int));

	for (i = 1; i <= num_sets; ++i)
	{
		int			u = state->pair_vu[i];
		int			v = state->pair_uv[i];

		if (u > 0 && u < i)
			chains[i] = chains[u];
		else if (v > 0 && v < i)
			chains[i] = chains[v];
		else
			chains[i] = ++num_chains;
	}

	/* build result lists. */
	results = palloc0((num_chains + 1) * sizeof(List *));

	for (i = 1; i <= num_sets; ++i)
	{
		int			c = chains[i];

		Assert(c > 0);

		results[c] = list_concat(results[c], orig_sets[i]);
	}

	/* push any empty sets back on the first list. */
	while (num_empty-- > 0)
		results[1] = lcons(NIL, results[1]);

	/* make result list */
	for (i = 1; i <= num_chains; ++i)
		result = lappend(result, results[i]);

	/*
	 * Free all the things.
	 *
	 * (This is over-fussy for small sets but for large sets we could have
	 * tied up a nontrivial amount of memory.)
	 */
	BipartiteMatchFree(state);
	pfree(results);
	pfree(chains);
	for (i = 1; i <= num_sets; ++i)
		if (adjacency[i])
			pfree(adjacency[i]);
	pfree(adjacency);
	pfree(adjacency_buf);
	pfree(orig_sets);
	for (i = 1; i <= num_sets; ++i)
		bms_free(set_masks[i]);
	pfree(set_masks);

	return result;
}

/*
 * Reorder the elements of a list of grouping sets such that they have correct
 * prefix relationships. Also inserts the GroupingSetData annotations.
 *
 * The input must be ordered with smallest sets first; the result is returned
 * with largest sets first.  Note that the result shares no list substructure
 * with the input, so it's safe for the caller to modify it later.
 *
 * If we're passed in a sortclause, we follow its order of columns to the
 * extent possible, to minimize the chance that we add unnecessary sorts.
 * (We're trying here to ensure that GROUPING SETS ((a,b,c),(c)) ORDER BY c,b,a
 * gets implemented in one pass.)
 */
static List *
reorder_grouping_sets(List *groupingsets, List *sortclause)
{
	ListCell   *lc;
	ListCell   *lc2;
	List	   *previous = NIL;
	List	   *result = NIL;

	foreach(lc, groupingsets)
	{
		List	   *candidate = lfirst(lc);
		List	   *new_elems = list_difference_int(candidate, previous);
		GroupingSetData *gs = makeNode(GroupingSetData);

		if (list_length(new_elems) > 0)
		{
			while (list_length(sortclause) > list_length(previous))
			{
				SortGroupClause *sc = list_nth(sortclause, list_length(previous));
				int			ref = sc->tleSortGroupRef;

				if (list_member_int(new_elems, ref))
				{
					previous = lappend_int(previous, ref);
					new_elems = list_delete_int(new_elems, ref);
				}
				else
				{
					/* diverged from the sortclause; give up on it */
					sortclause = NIL;
					break;
				}
			}

			foreach(lc2, new_elems)
			{
				previous = lappend_int(previous, lfirst_int(lc2));
			}
		}

		gs->set = list_copy(previous);
		result = lcons(gs, result);
		list_free(new_elems);
	}

	list_free(previous);

	return result;
}

/*
 * Compute query_pathkeys and other pathkeys during plan generation
 */
static void
standard_qp_callback(PlannerInfo *root, void *extra)
{
	Query	   *parse = root->parse;
	standard_qp_extra *qp_extra = (standard_qp_extra *) extra;
	List	   *tlist = root->processed_tlist;
	List	   *activeWindows = qp_extra->activeWindows;

	/*
	 * Calculate pathkeys that represent grouping/ordering requirements.  The
	 * sortClause is certainly sort-able, but GROUP BY and DISTINCT might not
	 * be, in which case we just leave their pathkeys empty.
	 */
	if (qp_extra->groupClause &&
		grouping_is_sortable(qp_extra->groupClause))
		root->group_pathkeys =
			make_pathkeys_for_sortclauses(root,
										  qp_extra->groupClause,
										  tlist);
	else
		root->group_pathkeys = NIL;

	/* We consider only the first (bottom) window in pathkeys logic */
	if (activeWindows != NIL)
	{
		WindowClause *wc = (WindowClause *) linitial(activeWindows);

		root->window_pathkeys = make_pathkeys_for_window(root,
														 wc,
														 tlist);
	}
	else
		root->window_pathkeys = NIL;

	if (parse->distinctClause &&
		grouping_is_sortable(parse->distinctClause))
		root->distinct_pathkeys =
			make_pathkeys_for_sortclauses(root,
										  parse->distinctClause,
										  tlist);
	else
		root->distinct_pathkeys = NIL;

	root->sort_pathkeys =
		make_pathkeys_for_sortclauses(root,
									  parse->sortClause,
									  tlist);

	/*
	 * Figure out whether we want a sorted result from query_planner.
	 *
	 * If we have a sortable GROUP BY clause, then we want a result sorted
	 * properly for grouping.  Otherwise, if we have window functions to
	 * evaluate, we try to sort for the first window.  Otherwise, if there's a
	 * sortable DISTINCT clause that's more rigorous than the ORDER BY clause,
	 * we try to produce output that's sufficiently well sorted for the
	 * DISTINCT.  Otherwise, if there is an ORDER BY clause, we want to sort
	 * by the ORDER BY clause.
	 *
	 * Note: if we have both ORDER BY and GROUP BY, and ORDER BY is a superset
	 * of GROUP BY, it would be tempting to request sort by ORDER BY --- but
	 * that might just leave us failing to exploit an available sort order at
	 * all.  Needs more thought.  The choice for DISTINCT versus ORDER BY is
	 * much easier, since we know that the parser ensured that one is a
	 * superset of the other.
	 */
	if (root->group_pathkeys)
		root->query_pathkeys = root->group_pathkeys;
	else if (root->window_pathkeys)
		root->query_pathkeys = root->window_pathkeys;
	else if (list_length(root->distinct_pathkeys) >
			 list_length(root->sort_pathkeys))
		root->query_pathkeys = root->distinct_pathkeys;
	else if (root->sort_pathkeys)
		root->query_pathkeys = root->sort_pathkeys;
	else
		root->query_pathkeys = NIL;
}

/*
 * Estimate number of groups produced by grouping clauses (1 if not grouping)
 *
 * path_rows: number of output rows from scan/join step
 * gd: grouping sets data including list of grouping sets and their clauses
 * target_list: target list containing group clause references
 *
 * If doing grouping sets, we also annotate the gsets data with the estimates
 * for each set and each individual rollup list, with a view to later
 * determining whether some combination of them could be hashed instead.
 */
static double
get_number_of_groups(PlannerInfo *root,
					 double path_rows,
					 grouping_sets_data *gd,
					 List *target_list)
{
	Query	   *parse = root->parse;
	double		dNumGroups;

	if (parse->groupClause)
	{
		List	   *groupExprs;

		if (parse->groupingSets)
		{
			/* Add up the estimates for each grouping set */
			ListCell   *lc;
			ListCell   *lc2;

			Assert(gd);			/* keep Coverity happy */

			dNumGroups = 0;

			foreach(lc, gd->rollups)
			{
				RollupData *rollup = lfirst(lc);
				ListCell   *lc;

				groupExprs = get_sortgrouplist_exprs(rollup->groupClause,
													 target_list);

				rollup->numGroups = 0.0;

				forboth(lc, rollup->gsets, lc2, rollup->gsets_data)
				{
					List	   *gset = (List *) lfirst(lc);
					GroupingSetData *gs = lfirst(lc2);
					double		numGroups = estimate_num_groups(root,
																groupExprs,
																path_rows,
																&gset);

					gs->numGroups = numGroups;
					rollup->numGroups += numGroups;
				}

				dNumGroups += rollup->numGroups;
			}

			if (gd->hash_sets_idx)
			{
				ListCell   *lc;

				gd->dNumHashGroups = 0;

				groupExprs = get_sortgrouplist_exprs(parse->groupClause,
													 target_list);

				forboth(lc, gd->hash_sets_idx, lc2, gd->unsortable_sets)
				{
					List	   *gset = (List *) lfirst(lc);
					GroupingSetData *gs = lfirst(lc2);
					double		numGroups = estimate_num_groups(root,
																groupExprs,
																path_rows,
																&gset);

					gs->numGroups = numGroups;
					gd->dNumHashGroups += numGroups;
				}

				dNumGroups += gd->dNumHashGroups;
			}
		}
		else
		{
			/* Plain GROUP BY */
			groupExprs = get_sortgrouplist_exprs(parse->groupClause,
												 target_list);

			dNumGroups = estimate_num_groups(root, groupExprs, path_rows,
											 NULL);
		}
	}
	else if (parse->groupingSets)
	{
		/* Empty grouping sets ... one result row for each one */
		dNumGroups = list_length(parse->groupingSets);
	}
	else if (parse->hasAggs || root->hasHavingQual)
	{
		/* Plain aggregation, one result row */
		dNumGroups = 1;
	}
	else
	{
		/* Not grouping */
		dNumGroups = 1;
	}

	return dNumGroups;
}

/*
 * create_grouping_paths
 *
 * Build a new upperrel containing Paths for grouping and/or aggregation.
 * Along the way, we also build an upperrel for Paths which are partially
 * grouped and/or aggregated.  A partially grouped and/or aggregated path
 * needs a FinalizeAggregate node to complete the aggregation.  Currently,
 * the only partially grouped paths we build are also partial paths; that
 * is, they need a Gather and then a FinalizeAggregate.
 *
 * input_rel: contains the source-data Paths
 * target: the pathtarget for the result Paths to compute
 * agg_costs: cost info about all aggregates in query (in AGGSPLIT_SIMPLE mode)
 * rollup_lists: list of grouping sets, or NIL if not doing grouping sets
 * rollup_groupclauses: list of grouping clauses for grouping sets,
 *		or NIL if not doing grouping sets
 *
 * Note: all Paths in input_rel are expected to return the target computed
 * by make_group_input_target.
 */
static RelOptInfo *
create_grouping_paths(PlannerInfo *root,
					  RelOptInfo *input_rel,
					  PathTarget *target,
					  bool target_parallel_safe,
					  const AggClauseCosts *agg_costs,
					  grouping_sets_data *gd)
{
	Query	   *parse = root->parse;
	RelOptInfo *grouped_rel;

	/*
	 * For now, all aggregated paths are added to the (GROUP_AGG, NULL)
	 * upperrel.
	 */
	grouped_rel = fetch_upper_rel(root, UPPERREL_GROUP_AGG, NULL);
	grouped_rel->reltarget = target;

	/*
	 * If the input relation is not parallel-safe, then the grouped relation
	 * can't be parallel-safe, either.  Otherwise, it's parallel-safe if the
	 * target list and HAVING quals are parallel-safe.
	 */
	if (input_rel->consider_parallel && target_parallel_safe &&
		is_parallel_safe(root, (Node *) parse->havingQual))
		grouped_rel->consider_parallel = true;

	/*
	 * If the input rel belongs to a single FDW, so does the grouped rel.
	 */
	grouped_rel->serverid = input_rel->serverid;
	grouped_rel->userid = input_rel->userid;
	grouped_rel->useridiscurrent = input_rel->useridiscurrent;
	grouped_rel->fdwroutine = input_rel->fdwroutine;

	/*
	 * Create either paths for a degenerate grouping or paths for ordinary
	 * grouping, as appropriate.
	 */
	if (is_degenerate_grouping(root) && !(ORA_MODE && root->parse->groupingSets))
		create_degenerate_grouping_paths(root, input_rel, target, grouped_rel);
	else
	{
		int			flags = 0;

		/*
		 * Determine whether it's possible to perform sort-based
		 * implementations of grouping.  (Note that if groupClause is empty,
		 * grouping_is_sortable() is trivially true, and all the
		 * pathkeys_contained_in() tests will succeed too, so that we'll
		 * consider every surviving input path.)
		 *
		 * If we have grouping sets, we might be able to sort some but not all
		 * of them; in this case, we need can_sort to be true as long as we
		 * must consider any sorted-input plan.
		 */
		if ((gd && gd->rollups != NIL)
			|| grouping_is_sortable(parse->groupClause))
			flags |= GROUPING_CAN_USE_SORT;

		/*
		 * Determine whether we should consider hash-based implementations of
		 * grouping.
		 *
		 * Hashed aggregation only applies if we're grouping. If we have
		 * grouping sets, some groups might be hashable but others not; in
		 * this case we set can_hash true as long as there is nothing globally
		 * preventing us from hashing (and we should therefore consider plans
		 * with hashes).
		 *
		 * Executor doesn't support hashed aggregation with DISTINCT or ORDER
		 * BY aggregates.  (Doing so would imply storing *all* the input
		 * values in the hash table, and/or running many sorts in parallel,
		 * either of which seems like a certain loser.)  We similarly don't
		 * support ordered-set aggregates in hashed aggregation, but that case
		 * is also included in the numOrderedAggs count.
		 *
		 * Note: grouping_is_hashable() is much more expensive to check than
		 * the other gating conditions, so we want to do it last.
		 */
		if ((parse->groupClause != NIL &&
			 agg_costs->numOrderedAggs == 0 &&
			 (gd ? gd->any_hashable : grouping_is_hashable(parse->groupClause))))
			flags |= GROUPING_CAN_USE_HASH;

		/*
		 * Determine whether partial aggregation is possible.
		 */
		if (can_partial_agg(root, agg_costs))
			flags |= GROUPING_CAN_PARTIAL_AGG;

		create_ordinary_grouping_paths(root, input_rel, target, grouped_rel,
									   agg_costs, gd, flags);
	}

	set_cheapest(grouped_rel);
	return grouped_rel;
}

/*
 * is_degenerate_grouping
 *
 * A degenerate grouping is one in which the query has a HAVING qual and/or
 * grouping sets, but no aggregates and no GROUP BY (which implies that the
 * grouping sets are all empty).
 */
static bool
is_degenerate_grouping(PlannerInfo *root)
{
	Query	   *parse = root->parse;

	return (root->hasHavingQual || parse->groupingSets) &&
		!parse->hasAggs && parse->groupClause == NIL;
}

/*
 * create_degenerate_grouping_paths
 *
 * When the grouping is degenerate (see is_degenerate_grouping), we are
 * supposed to emit either zero or one row for each grouping set depending on
 * whether HAVING succeeds.  Furthermore, there cannot be any variables in
 * either HAVING or the targetlist, so we actually do not need the FROM table
 * at all! We can just throw away the plan-so-far and generate a Result node.
 * This is a sufficiently unusual corner case that it's not worth contorting
 * the structure of this module to avoid having to generate the earlier paths
 * in the first place.
 */
static void
create_degenerate_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								 PathTarget *target, RelOptInfo *grouped_rel)
{
	Query	   *parse = root->parse;
	int			nrows;
	Path	   *path;

	nrows = list_length(parse->groupingSets);
	if (nrows > 1)
	{
		/*
		 * Doesn't seem worthwhile writing code to cons up a generate_series
		 * or a values scan to emit multiple rows. Instead just make N clones
		 * and append them.  (With a volatile HAVING clause, this means you
		 * might get between 0 and N output rows. Offhand I think that's
		 * desired.)
		 */
		List	   *paths = NIL;

		while (--nrows >= 0)
		{
			path = (Path *)
				create_result_path(root, grouped_rel,
								   target,
								   (List *) parse->havingQual);
			paths = lappend(paths, path);
		}
		path = (Path *)
	        create_append_path(root,
	                           grouped_rel,
							   paths,
							   NIL,
							   NULL,
							   0,
							   false,
							   NIL,
							   -1);
		path->pathtarget = target;
	}
	else
	{
		/* No grouping sets, or just one, so one output row */
		path = (Path *)
			create_result_path(root, grouped_rel,
							   target,
							   (List *) parse->havingQual);
	}

	add_path(grouped_rel, path);
}

/*
 * create_ordinary_grouping_paths
 *
 * Create grouping paths for the ordinary (that is, non-degenerate) case.
 *
 * We need to consider sorted and hashed aggregation in the same function,
 * because otherwise (1) it would be harder to throw an appropriate error
 * message if neither way works, and (2) we should not allow hashtable size
 * considerations to dissuade us from using hashing if sorting is not possible.
 */
static void
create_ordinary_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
							   PathTarget *target, RelOptInfo *grouped_rel,
							   const AggClauseCosts *agg_costs,
							   grouping_sets_data *gd, int flags)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	RelOptInfo *partially_grouped_rel = NULL;
	AggClauseCosts agg_final_costs; /* parallel only */
	double		dNumGroups;
	ListCell   *lc;
	bool		can_hash = (flags & GROUPING_CAN_USE_HASH) != 0;
	bool		can_sort = (flags & GROUPING_CAN_USE_SORT) != 0;
	double		hint_rows = -1;

	/*
	 * Estimate number of groups.
	 */
	dNumGroups = get_number_of_groups(root,
									  cheapest_path->rows,
									  gd,
									  parse->targetList);
	
	/*
	 * Before generating paths for grouped_rel, we first generate any possible
	 * partially grouped paths; that way, later code can easily consider both
	 * parallel and non-parallel approaches to grouping.
	 */
	MemSet(&agg_final_costs, 0, sizeof(AggClauseCosts));
	if (grouped_rel->consider_parallel && input_rel->partial_pathlist != NIL
		&& (flags & GROUPING_CAN_PARTIAL_AGG) != 0)
	{
		partially_grouped_rel =
			create_partial_grouping_paths(root,
										  grouped_rel,
										  input_rel,
										  gd,
										  can_sort,
										  can_hash,
										  &agg_final_costs);
		gather_grouping_paths(root, partially_grouped_rel);
		set_cheapest(partially_grouped_rel);
	}

	/* Build final grouping paths */
	add_paths_to_grouping_rel(root, input_rel, grouped_rel, target,
							  partially_grouped_rel, agg_costs,
							  &agg_final_costs, gd, can_sort, can_hash,
							  dNumGroups, (List *) parse->havingQual);

	/* Give a helpful error if we failed to find any implementation */
	if (grouped_rel->pathlist == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not implement GROUP BY"),
				 errdetail("Some of the datatypes only support hashing, while others only support sorting.")));

	if (set_agg_rows_hook != NULL)
		hint_rows = (*set_agg_rows_hook)(root, FUN_GROUP_BY, -1);

	/*
	 * Loop to set Agg type
	 */
	foreach (lc, grouped_rel->pathlist)
	{
		Path *node_path = (Path *) lfirst(lc);

		if (IsA(node_path, AggPath))
		{
			AggPath *agg_path = (AggPath *) lfirst(lc);
			agg_path->agg_type = FUN_GROUP_BY;

			/* If hint_rows is set, set it for agg node. */
			if (hint_rows > 0)
				agg_path->path.rows = hint_rows;
		}
	}

	/*
	 * If there is an FDW that's responsible for all baserels of the query,
	 * let it consider adding ForeignPaths.
	 */
	if (grouped_rel->fdwroutine &&
		grouped_rel->fdwroutine->GetForeignUpperPaths)
		grouped_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_GROUP_AGG,
													  input_rel, grouped_rel);

	/* Let extensions possibly add some more paths */
	if (create_upper_paths_hook)
		(*create_upper_paths_hook) (root, UPPERREL_GROUP_AGG,
									input_rel, grouped_rel);
}

/*
 * For a given input path, consider the possible ways of doing grouping sets on
 * it, by combinations of hashing and sorting.  This can be called multiple
 * times, so it's important that it not scribble on input.  No result is
 * returned, but any generated paths are added to grouped_rel.
 */
static void
consider_groupingsets_paths(PlannerInfo *root,
							RelOptInfo *grouped_rel,
							Path *path,
							bool is_sorted,
							bool can_hash,
							PathTarget *target,
							grouping_sets_data *gd,
							const AggClauseCosts *agg_costs,
							double dNumGroups)
{
	Query	   *parse = root->parse;
	int			hash_mem = get_hash_mem();

	/*
	 * If we're not being offered sorted input, then only consider plans that
	 * can be done entirely by hashing.
	 *
	 * We can hash everything if it looks like it'll fit in hash_mem. But if
	 * the input is actually sorted despite not being advertised as such, we
	 * prefer to make use of that in order to use less memory.
	 *
	 * If none of the grouping sets are sortable, then ignore the hash_mem
	 * limit and generate a path anyway, since otherwise we'll just fail.
	 */
	if (!is_sorted)
	{
		List	   *new_rollups = NIL;
		RollupData *unhashed_rollup = NULL;
		List	   *sets_data;
		List	   *empty_sets_data = NIL;
		List	   *empty_sets = NIL;
		ListCell   *lc;
		ListCell   *l_start = list_head(gd->rollups);
		AggStrategy strat = AGG_HASHED;
		double		hashsize;
		double		exclude_groups = 0.0;

		Assert(can_hash);

		/*
		 * If the input is coincidentally sorted usefully (which can happen
		 * even if is_sorted is false, since that only means that our caller
		 * has set up the sorting for us), then save some hashtable space by
		 * making use of that. But we need to watch out for degenerate cases:
		 *
		 * 1) If there are any empty grouping sets, then group_pathkeys might
		 * be NIL if all non-empty grouping sets are unsortable. In this case,
		 * there will be a rollup containing only empty groups, and the
		 * pathkeys_contained_in test is vacuously true; this is ok.
		 *
		 * XXX: the above relies on the fact that group_pathkeys is generated
		 * from the first rollup. If we add the ability to consider multiple
		 * sort orders for grouping input, this assumption might fail.
		 *
		 * 2) If there are no empty sets and only unsortable sets, then the
		 * rollups list will be empty (and thus l_start == NULL), and
		 * group_pathkeys will be NIL; we must ensure that the vacuously-true
		 * pathkeys_contain_in test doesn't cause us to crash.
		 */
		if (l_start != NULL &&
			pathkeys_contained_in(root->group_pathkeys, path->pathkeys))
		{
			unhashed_rollup = lfirst(l_start);
			exclude_groups = unhashed_rollup->numGroups;
			l_start = lnext(l_start);
		}

		hashsize = estimate_hashagg_tablesize(path,
											  agg_costs,
											  dNumGroups - exclude_groups);

		/*
		 * gd->rollups is empty if we have only unsortable columns to work
		 * with.  Override hash_mem in that case; otherwise, we'll rely on the
		 * sorted-input case to generate usable mixed paths.
		 */
		if (hashsize > hash_mem * 1024L && gd->rollups)
			return;				/* nope, won't fit */

		/*
		 * We need to burst the existing rollups list into individual grouping
		 * sets and recompute a groupClause for each set.
		 */
		sets_data = list_copy(gd->unsortable_sets);

		for_each_cell(lc, l_start)
		{
			RollupData *rollup = lfirst(lc);

			/*
			 * If we find an unhashable rollup that's not been skipped by the
			 * "actually sorted" check above, we can't cope; we'd need sorted
			 * input (with a different sort order) but we can't get that here.
			 * So bail out; we'll get a valid path from the is_sorted case
			 * instead.
			 *
			 * The mere presence of empty grouping sets doesn't make a rollup
			 * unhashable (see preprocess_grouping_sets), we handle those
			 * specially below.
			 */
			if (!rollup->hashable)
				return;
			else
				sets_data = list_concat(sets_data, list_copy(rollup->gsets_data));
		}
		foreach(lc, sets_data)
		{
			GroupingSetData *gs = lfirst(lc);
			List	   *gset = gs->set;
			RollupData *rollup;

			if (gset == NIL)
			{
				/* Empty grouping sets can't be hashed. */
				empty_sets_data = lappend(empty_sets_data, gs);
				empty_sets = lappend(empty_sets, NIL);
			}
			else
			{
				rollup = makeNode(RollupData);

				rollup->groupClause = preprocess_groupclause(root, gset);
				rollup->gsets_data = list_make1(gs);
				rollup->gsets = remap_to_groupclause_idx(rollup->groupClause,
														 rollup->gsets_data,
														 gd->tleref_to_colnum_map);
				rollup->numGroups = gs->numGroups;
				rollup->hashable = true;
				rollup->is_hashed = true;
				new_rollups = lappend(new_rollups, rollup);
			}
		}

		/*
		 * If we didn't find anything nonempty to hash, then bail.  We'll
		 * generate a path from the is_sorted case.
		 */
		if (new_rollups == NIL)
			return;

		/*
		 * If there were empty grouping sets they should have been in the
		 * first rollup.
		 */
		Assert(!unhashed_rollup || !empty_sets);

		if (unhashed_rollup)
		{
			new_rollups = lappend(new_rollups, unhashed_rollup);
			strat = AGG_MIXED;
		}
		else if (empty_sets)
		{
			RollupData *rollup = makeNode(RollupData);

			rollup->groupClause = NIL;
			rollup->gsets_data = empty_sets_data;
			rollup->gsets = empty_sets;
			rollup->numGroups = list_length(empty_sets);
			rollup->hashable = false;
			rollup->is_hashed = false;
			new_rollups = lappend(new_rollups, rollup);
			strat = AGG_MIXED;
		}

#ifdef __OPENTENBASE_C__
		if (group_optimizer)
		{
			/*
			 * try to convert grouping(rollup/cube/grouping sets) to multi group-by, then
			 * make append plan.
			 * group-by can be pushed down to datanode, and subplans of append can be
			 * executed simultaneously.
			 */
			Path *append_path = create_grouping_append_path(root,
											  grouped_rel,
											  path,
											  target,
											  (List *) parse->havingQual,
											  new_rollups,
											  agg_costs,
											  gd,
											  true);
			if (append_path)
			{
				add_path(grouped_rel, append_path);
			}
			else
			{
				if (!can_push_down_grouping(root, parse, path))
					path = create_remotesubplan_path(root, path, NULL);

				add_path(grouped_rel, (Path *)
						 create_groupingsets_path(root,
												  grouped_rel,
												  path,
												  target,
												  (List *) parse->havingQual,
												  strat,
												  new_rollups,
												  agg_costs,
												  dNumGroups));
			}
		}
		else
		{
#endif
		/*
		 * If the grouping can't be fully pushed down, redistribute the
		 * path on top of the (sorted) path. If if can be pushed down,
		 * disable construction of complex distributed paths.
		 */
		if (!can_push_down_grouping(root, parse, path))
			path = create_remotesubplan_path(root, path, NULL);

		add_path(grouped_rel, (Path *)
				 create_groupingsets_path(root,
										  grouped_rel,
										  path,
										  target,
										  (List *) parse->havingQual,
										  strat,
										  new_rollups,
										  agg_costs,
										  dNumGroups));
#ifdef __OPENTENBASE_C__
		}
#endif
		return;
	}

	/*
	 * If we have sorted input but nothing we can do with it, bail.
	 */
	if (list_length(gd->rollups) == 0)
		return;

	/*
	 * Given sorted input, we try and make two paths: one sorted and one mixed
	 * sort/hash. (We need to try both because hashagg might be disabled, or
	 * some columns might not be sortable.)
	 *
	 * can_hash is passed in as false if some obstacle elsewhere (such as
	 * ordered aggs) means that we shouldn't consider hashing at all.
	 */
	if (can_hash && gd->any_hashable)
	{
		List	   *rollups = NIL;
		List	   *hash_sets = list_copy(gd->unsortable_sets);
		double		availspace = (hash_mem * 1024.0);
		ListCell   *lc;

		/*
		 * Account first for space needed for groups we can't sort at all.
		 */
		availspace -= estimate_hashagg_tablesize(path,
												 agg_costs,
												 gd->dNumHashGroups);

		if (availspace > 0 && list_length(gd->rollups) > 1)
		{
			double		scale;
			int			num_rollups = list_length(gd->rollups);
			int			k_capacity;
			int		   *k_weights = palloc(num_rollups * sizeof(int));
			Bitmapset  *hash_items = NULL;
			int			i;

			/*
			 * We treat this as a knapsack problem: the knapsack capacity
			 * represents hash_mem, the item weights are the estimated memory
			 * usage of the hashtables needed to implement a single rollup,
			 * and we really ought to use the cost saving as the item value;
			 * however, currently the costs assigned to sort nodes don't
			 * reflect the comparison costs well, and so we treat all items as
			 * of equal value (each rollup we hash instead saves us one sort).
			 *
			 * To use the discrete knapsack, we need to scale the values to a
			 * reasonably small bounded range.  We choose to allow a 5% error
			 * margin; we have no more than 4096 rollups in the worst possible
			 * case, which with a 5% error margin will require a bit over 42MB
			 * of workspace. (Anyone wanting to plan queries that complex had
			 * better have the memory for it.  In more reasonable cases, with
			 * no more than a couple of dozen rollups, the memory usage will
			 * be negligible.)
			 *
			 * k_capacity is naturally bounded, but we clamp the values for
			 * scale and weight (below) to avoid overflows or underflows (or
			 * uselessly trying to use a scale factor less than 1 byte).
			 */
			scale = Max(availspace / (20.0 * num_rollups), 1.0);
			k_capacity = (int) floor(availspace / scale);

			/*
			 * We leave the first rollup out of consideration since it's the
			 * one that matches the input sort order.  We assign indexes "i"
			 * to only those entries considered for hashing; the second loop,
			 * below, must use the same condition.
			 */
			i = 0;
			for_each_cell(lc, lnext(list_head(gd->rollups)))
			{
				RollupData *rollup = lfirst(lc);

				if (rollup->hashable)
				{
					double		sz = estimate_hashagg_tablesize(path,
																agg_costs,
																rollup->numGroups);

					/*
					 * If sz is enormous, but hash_mem (and hence scale) is
					 * small, avoid integer overflow here.
					 */
					k_weights[i] = (int) Min(floor(sz / scale),
											 k_capacity + 1.0);
					++i;
				}
			}

			/*
			 * Apply knapsack algorithm; compute the set of items which
			 * maximizes the value stored (in this case the number of sorts
			 * saved) while keeping the total size (approximately) within
			 * capacity.
			 */
			if (i > 0)
				hash_items = DiscreteKnapsack(k_capacity, i, k_weights, NULL);

			if (!bms_is_empty(hash_items))
			{
				rollups = list_make1(linitial(gd->rollups));

				i = 0;
				for_each_cell(lc, lnext(list_head(gd->rollups)))
				{
					RollupData *rollup = lfirst(lc);

					if (rollup->hashable)
					{
						if (bms_is_member(i, hash_items))
							hash_sets = list_concat(hash_sets,
													list_copy(rollup->gsets_data));
						else
							rollups = lappend(rollups, rollup);
						++i;
					}
					else
						rollups = lappend(rollups, rollup);
				}
			}
		}

		if (!rollups && hash_sets)
			rollups = list_copy(gd->rollups);

		foreach(lc, hash_sets)
		{
			GroupingSetData *gs = lfirst(lc);
			RollupData *rollup = makeNode(RollupData);

			Assert(gs->set != NIL);

			rollup->groupClause = preprocess_groupclause(root, gs->set);
			rollup->gsets_data = list_make1(gs);
			rollup->gsets = remap_to_groupclause_idx(rollup->groupClause,
													 rollup->gsets_data,
													 gd->tleref_to_colnum_map);
			rollup->numGroups = gs->numGroups;
			rollup->hashable = true;
			rollup->is_hashed = true;
			rollups = lcons(rollup, rollups);
		}

		if (rollups)
		{
#ifdef __OPENTENBASE_C__
			if (group_optimizer)
			{
				/*
				 * try to convert grouping(rollup/cube/grouping sets) to multi group-by, then
				 * make append plan.
				 * group-by can be pushed down to datanode, and subplans of append can be
				 * executed simultaneously.
				 */
				Path *append_path = create_grouping_append_path(root,
												  grouped_rel,
												  path,
												  target,
												  (List *) parse->havingQual,
												  rollups,
												  agg_costs,
												  gd,
												  true);
				if (append_path)
				{
					add_path(grouped_rel, append_path);
				}
				else
				{
					if (! can_push_down_grouping(root, parse, path))
						path = create_remotesubplan_path(root, path, NULL);

					add_path(grouped_rel, (Path *)
							 create_groupingsets_path(root,
													  grouped_rel,
													  path,
													  target,
													  (List *) parse->havingQual,
													  AGG_MIXED,
													  rollups,
													  agg_costs,
													  dNumGroups));
				}
			}
			else
			{
#endif
			/*
			 * If the grouping can't be fully pushed down, redistribute the
			 * path on top of the (sorted) path. If if can be pushed down,
			 * disable construction of complex distributed paths.
			 */
			if (! can_push_down_grouping(root, parse, path))
				path = create_remotesubplan_path(root, path, NULL);

			add_path(grouped_rel, (Path *)
					 create_groupingsets_path(root,
											  grouped_rel,
											  path,
											  target,
											  (List *) parse->havingQual,
											  AGG_MIXED,
											  rollups,
											  agg_costs,
											  dNumGroups));
#ifdef __OPENTENBASE_C__
			}
#endif
		}
	}

	/*
	 * Now try the simple sorted case.
	 */
	if (!gd->unsortable_sets)
	{
#ifdef __OPENTENBASE_C__
		if (group_optimizer)
		{
			/*
			 * try to convert grouping(rollup/cube/grouping sets) to multi group-by, then
			 * make append plan.
			 * group-by can be pushed down to datanode, and subplans of append can be
			 * executed simultaneously.
			 */
			Path *append_path = create_grouping_append_path(root,
													  grouped_rel,
													  path,
													  target,
													  (List *) parse->havingQual,
													  gd->rollups,
													  agg_costs,
													  gd,
													  false);
			if (append_path)
			{
				add_path(grouped_rel, append_path);
			}
			else
			{
				if (! can_push_down_grouping(root, parse, path))
					path = create_remotesubplan_path(root, path, NULL);

				add_path(grouped_rel, (Path *)
						 create_groupingsets_path(root,
												  grouped_rel,
												  path,
												  target,
												  (List *) parse->havingQual,
												  AGG_SORTED,
												  gd->rollups,
												  agg_costs,
												  dNumGroups));
			}
		}
		else
		{
#endif
		/*
		 * If the grouping can't be fully pushed down, redistribute the
		 * path on top of the (sorted) path. If if can be pushed down,
		 * disable construction of complex distributed paths.
		 */
		if (! can_push_down_grouping(root, parse, path))
			path = create_remotesubplan_path(root, path, NULL);

		add_path(grouped_rel, (Path *)
				 create_groupingsets_path(root,
										  grouped_rel,
										  path,
										  target,
										  (List *) parse->havingQual,
										  AGG_SORTED,
										  gd->rollups,
										  agg_costs,
										  dNumGroups));
#ifdef __OPENTENBASE_C__
		}
#endif
	}
}

/*
 * create_window_paths
 *
 * Build a new upperrel containing Paths for window-function evaluation.
 *
 * input_rel: contains the source-data Paths
 * input_target: result of make_window_input_target
 * output_target: what the topmost WindowAggPath should return
 * wflists: result of find_window_functions
 * activeWindows: result of select_active_windows
 *
 * Note: all Paths in input_rel are expected to return input_target.
 */
static RelOptInfo *
create_window_paths(PlannerInfo *root,
					RelOptInfo *input_rel,
					PathTarget *input_target,
					PathTarget *output_target,
					bool output_target_parallel_safe,
					WindowFuncLists *wflists,
					List *activeWindows)
{
	RelOptInfo *window_rel;
	ListCell   *lc;

	/* For now, do all work in the (WINDOW, NULL) upperrel */
	window_rel = fetch_upper_rel(root, UPPERREL_WINDOW, NULL);

	/*
	 * If the input relation is not parallel-safe, then the window relation
	 * can't be parallel-safe, either.  Otherwise, we need to examine the
	 * target list and active windows for non-parallel-safe constructs.
	 */
	if (input_rel->consider_parallel && output_target_parallel_safe &&
		is_parallel_safe(root, (Node *) activeWindows))
		window_rel->consider_parallel = true;

	/*
	 * If the input rel belongs to a single FDW, so does the window rel.
	 */
	window_rel->serverid = input_rel->serverid;
	window_rel->userid = input_rel->userid;
	window_rel->useridiscurrent = input_rel->useridiscurrent;
	window_rel->fdwroutine = input_rel->fdwroutine;

	/*
	 * Consider computing window functions starting from the existing
	 * cheapest-total path (which will likely require a sort) as well as any
	 * existing paths that satisfy root->window_pathkeys (which won't).
	 */
	foreach(lc, input_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		if (path == input_rel->cheapest_total_path ||
			pathkeys_contained_in(root->window_pathkeys, path->pathkeys))
			create_one_window_path(root,
								   window_rel,
								   path,
								   input_target,
								   output_target,
								   wflists,
								   activeWindows);
	}

	/*
	 * If there is an FDW that's responsible for all baserels of the query,
	 * let it consider adding ForeignPaths.
	 */
	if (window_rel->fdwroutine &&
		window_rel->fdwroutine->GetForeignUpperPaths)
		window_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_WINDOW,
													 input_rel, window_rel);

	/* Let extensions possibly add some more paths */
	if (create_upper_paths_hook)
		(*create_upper_paths_hook) (root, UPPERREL_WINDOW,
									input_rel, window_rel);

	/* Now choose the best path(s) */
	set_cheapest(window_rel);

	return window_rel;
}

/*
 * Stack window-function implementation steps atop the given Path, and
 * add the result to window_rel.
 *
 * window_rel: upperrel to contain result
 * path: input Path to use (must return input_target)
 * input_target: result of make_window_input_target
 * output_target: what the topmost WindowAggPath should return
 * wflists: result of find_window_functions
 * activeWindows: result of select_active_windows
 */
static void
create_one_window_path(PlannerInfo *root,
					   RelOptInfo *window_rel,
					   Path *path,
					   PathTarget *input_target,
					   PathTarget *output_target,
					   WindowFuncLists *wflists,
					   List *activeWindows)
{
	PathTarget *window_target;
	ListCell   *l;

	/*
	 * Since each window clause could require a different sort order, we stack
	 * up a WindowAgg node for each clause, with sort steps between them as
	 * needed.  (We assume that select_active_windows chose a good order for
	 * executing the clauses in.)
	 *
	 * input_target should contain all Vars and Aggs needed for the result.
	 * (In some cases we wouldn't need to propagate all of these all the way
	 * to the top, since they might only be needed as inputs to WindowFuncs.
	 * It's probably not worth trying to optimize that though.)  It must also
	 * contain all window partitioning and sorting expressions, to ensure
	 * they're computed only once at the bottom of the stack (that's critical
	 * for volatile functions).  As we climb up the stack, we'll add outputs
	 * for the WindowFuncs computed at each level.
	 */
	window_target = input_target;

	foreach(l, activeWindows)
	{
		WindowClause *wc = (WindowClause *) lfirst(l);
		List	   *window_pathkeys;
		Path	   *subpath = path;

		window_pathkeys = make_pathkeys_for_window(root,
												   wc,
												   root->processed_tlist);

		/* Sort if necessary */
		if (!pathkeys_contained_in(window_pathkeys, path->pathkeys))
		{
			path = (Path *) create_sort_path(root, window_rel,
											 path,
											 window_pathkeys,
											 -1.0);
		}

		if (lnext(l))
		{
			/*
			 * Add the current WindowFuncs to the output target for this
			 * intermediate WindowAggPath.  We must copy window_target to
			 * avoid changing the previous path's target.
			 *
			 * Note: a WindowFunc adds nothing to the target's eval costs; but
			 * we do need to account for the increase in tlist width.
			 */
			ListCell   *lc2;

			window_target = copy_pathtarget(window_target);
			foreach(lc2, wflists->windowFuncs[wc->winref])
			{
				WindowFunc *wfunc = lfirst_node(WindowFunc, lc2);

				add_column_to_pathtarget(window_target, (Expr *) wfunc, 0);
				window_target->width += get_typavgwidth(wfunc->wintype, -1);
			}
		}
		else
		{
			/* Install the goal target in the topmost WindowAgg */
			window_target = output_target;
		}

#ifdef __OPENTENBASE_C__
		if (!can_push_down_window(root, path))
		{
			if (window_optimizer)
			{
				List *partitionClause = get_partition_clauses(window_target, wc);

				/*
				 * window function without partition by clause could
				 * not be pushed down.
				 */
				if (!partitionClause)
				{
					/* Add replication distribution */
					path = create_remotesubplan_replicated_path(root, path);
				}
				else
				{
					bool match = false;

					/*
					 * If subpath's distribution key refers window function's
					 * partition by key, use subpath directly.
					 */
					if (path->distribution->distributionType == LOCATOR_TYPE_HASH ||
						path->distribution->distributionType == LOCATOR_TYPE_SHARD)
					{
						ListCell *lc;

						/* partition by clause matches distribution clause? */
						foreach(lc, partitionClause)
						{
							Node *node = (Node *)lfirst(lc);

							/* match iff there node is the only distributed column. */
							if (IsSingleColDistribution(path->distribution) &&
								equal(node, path->distribution->disExprs[0]))
							{
								match = true;
								break;
							}
						}
					}

					/* add distribution according to partition clause */
					if (!match)
					{
						Path *rpath =
							create_window_distribution_path(root,
															subpath,
															partitionClause);
						/*
						 * For windowagg, if we need to add sort, distribute keys
						 * must be contained in those sort keys. In that case, we
						 * prefer to do redistribution first, and sort on the
						 * distribution path.
						 */
						if (subpath != path)
							path = (Path *) create_sort_path(root, window_rel,
															 rpath,
															 window_pathkeys,
															 -1.0);
						else
							path = rpath;
					}
					list_free(partitionClause);
				}
			}
			else
			{
				path = create_remotesubplan_path(root, path, NULL);
			}
		}
#endif
		path = (Path *)
			create_windowagg_path(root, window_rel, path, window_target,
								  wflists->windowFuncs[wc->winref],
								  wc,
								  window_pathkeys);
	}

	add_path(window_rel, path);
}

static void
create_distinct_sort_paths(PlannerInfo *root,
						   RelOptInfo *input_rel,
						   Query *parse,
						   double numDistinctRows,
						   RelOptInfo *distinct_rel,
						   Path *cheapest_input_path)
{
	ListCell   *lc;
	Path	   *path;

	/*
	 * First, if we have any adequately-presorted paths, just stick a
	 * Unique node on those.  Then consider doing an explicit sort of the
	 * cheapest input path and Unique'ing that.
	 *
	 * When we have DISTINCT ON, we must sort by the more rigorous of
	 * DISTINCT and ORDER BY, else it won't have the desired behavior.
	 * Also, if we do have to do an explicit sort, we might as well use
	 * the more rigorous ordering to avoid a second sort later.  (Note
	 * that the parser will have ensured that one clause is a prefix of
	 * the other.)
	 */
	List	   *needed_pathkeys;

	if (parse->hasDistinctOn &&
		list_length(root->distinct_pathkeys) <
		list_length(root->sort_pathkeys))
		needed_pathkeys = root->sort_pathkeys;
	else
		needed_pathkeys = root->distinct_pathkeys;

	foreach(lc, input_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		if (pathkeys_contained_in(needed_pathkeys, path->pathkeys))
		{
			/*
			 * Make sure the distribution matches the distinct clause,
			 * needed by the UNIQUE path.
			 *
			 * FIXME This could probably benefit from pushing a UNIQUE
			 * to the remote side, and only doing a merge locally.
			 */
#ifdef __OPENTENBASE_C__
			if (!grouping_distribution_match(root, parse, path,
												parse->distinctClause,
												parse->targetList))
			{
				/* try to push UNIQUE down */
				if (is_local_agg_worth(numDistinctRows, path))
				{
					Path *subpath;
					Path *rpath;

					subpath = (Path *) create_upper_unique_path(root,
																distinct_rel,
																path,
																list_length(root->distinct_pathkeys),
																numDistinctRows);
					rpath = create_distinct_distribution_path(root, subpath,
																parse->targetList,
																parse->distinctClause);
					add_path(distinct_rel, (Path *)
								create_upper_unique_path(root, distinct_rel,
														rpath,
														list_length(root->distinct_pathkeys),
														numDistinctRows));
				}

				path = create_distinct_distribution_path(root, path,
															parse->targetList,
															parse->distinctClause);
			}
#endif
			add_path(distinct_rel, (Path *)
						create_upper_unique_path(root, distinct_rel,
												path,
												list_length(root->distinct_pathkeys),
												numDistinctRows));
		}
	}

	/* For explicit-sort case, always use the more rigorous clause */
	if (list_length(root->distinct_pathkeys) <
		list_length(root->sort_pathkeys))
	{
		needed_pathkeys = root->sort_pathkeys;
#ifdef _PG_ORCL_
	/*
	 * in opentenbase_ora, the number of the expresions following 'order by' could be
	 * greater than the number of the targets after 'select distinct'.
	 */
	if (!ORA_MODE)
#endif
		/* Assert checks that parser didn't mess up... */
		Assert(pathkeys_contained_in(root->distinct_pathkeys,
										needed_pathkeys));
	}
	else
		needed_pathkeys = root->distinct_pathkeys;

	path = cheapest_input_path;
	if (!pathkeys_contained_in(needed_pathkeys, path->pathkeys))
		path = (Path *) create_sort_path(root, distinct_rel,
											path,
											needed_pathkeys,
											-1.0);

#ifdef __OPENTENBASE_C__
	/* If needed, inject RemoteSubplan redistributing the data. */
	if (!grouping_distribution_match(root, parse, path,
										parse->distinctClause,
										parse->targetList))
	{
		/* try to push UNIQUE down */
		if (is_local_agg_worth(numDistinctRows, path))
		{
			Path *subpath;
			Path *rpath;

			subpath = (Path *) create_upper_unique_path(root, distinct_rel,
														path,
														list_length(root->distinct_pathkeys),
														numDistinctRows);
			rpath = create_distinct_distribution_path(root, subpath,
														parse->targetList,
														parse->distinctClause);
			add_path(distinct_rel, (Path *)
						create_upper_unique_path(root, distinct_rel,
												rpath,
												list_length(root->distinct_pathkeys),
												numDistinctRows));
		}

		path = create_distinct_distribution_path(root, path,
													parse->targetList,
													parse->distinctClause);
	}
#endif
	add_path(distinct_rel, (Path *)
				create_upper_unique_path(root, distinct_rel,
										path,
										list_length(root->distinct_pathkeys),
										numDistinctRows));
}

static void
create_distinct_hash_paths(PlannerInfo *root,
						   RelOptInfo *input_rel,
						   Query *parse,
						   double numDistinctRows,
						   RelOptInfo *distinct_rel,
						   Path *cheapest_input_path)
{
	Path	   *path;

#ifdef __OPENTENBASE_C__
	path = cheapest_input_path;

	if (!grouping_distribution_match(root, parse, path,
										parse->distinctClause,
										parse->targetList))
	{
		if (is_local_agg_worth(numDistinctRows, path))
		{
			Path *subpath;
			Path *rpath;

			subpath = (Path *) create_agg_path(root,
												distinct_rel,
												path,
												path->pathtarget,
												AGG_HASHED,
												AGGSPLIT_SIMPLE,
												parse->distinctClause,
												NIL,
												NULL,
												numDistinctRows);
			rpath = create_distinct_distribution_path(root, subpath,
														parse->targetList,
														parse->distinctClause);
			add_path(distinct_rel, (Path *)
						create_agg_path(root,
										distinct_rel,
										rpath,
										rpath->pathtarget,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										parse->distinctClause,
										NIL,
										NULL,
										numDistinctRows));
		}

		path = create_distinct_distribution_path(root, path,
													parse->targetList,
													parse->distinctClause);
	}
#endif
	/* Generate hashed aggregate path --- no sort needed */
	add_path(distinct_rel, (Path *)
				create_agg_path(root,
								distinct_rel,
								path,
								path->pathtarget,
								AGG_HASHED,
								AGGSPLIT_SIMPLE,
								parse->distinctClause,
								NIL,
								NULL,
								numDistinctRows));
}

/*
 * create_distinct_paths
 *
 * Build a new upperrel containing Paths for SELECT DISTINCT evaluation.
 *
 * input_rel: contains the source-data Paths
 *
 * Note: input paths should already compute the desired pathtarget, since
 * Sort/Unique won't project anything.
 */
static RelOptInfo *
create_distinct_paths(PlannerInfo *root,
					  RelOptInfo *input_rel)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_input_path = input_rel->cheapest_total_path;
	RelOptInfo *distinct_rel;
	double		numDistinctRows;
	bool		allow_hash;
	bool		can_sort;
	bool		can_hash;
	ListCell   *lc;
	double		hint_rows = -1;
	int			type_mask = 0;
	bool		hint_can_sort = true;
	bool		hint_can_hash = true;

	/* For now, do all work in the (DISTINCT, NULL) upperrel */
	distinct_rel = fetch_upper_rel(root, UPPERREL_DISTINCT, NULL);

	/*
	 * We don't compute anything at this level, so distinct_rel will be
	 * parallel-safe if the input rel is parallel-safe.  In particular, if
	 * there is a DISTINCT ON (...) clause, any path for the input_rel will
	 * output those expressions, and will not be parallel-safe unless those
	 * expressions are parallel-safe.
	 */
	distinct_rel->consider_parallel = input_rel->consider_parallel;

	/*
	 * If the input rel belongs to a single FDW, so does the distinct_rel.
	 */
	distinct_rel->serverid = input_rel->serverid;
	distinct_rel->userid = input_rel->userid;
	distinct_rel->useridiscurrent = input_rel->useridiscurrent;
	distinct_rel->fdwroutine = input_rel->fdwroutine;

	/* Estimate number of distinct rows there will be */
	if (parse->groupClause || parse->groupingSets || parse->hasAggs ||
		root->hasHavingQual)
	{
		/*
		 * If there was grouping or aggregation, use the number of input rows
		 * as the estimated number of DISTINCT rows (ie, assume the input is
		 * already mostly unique).
		 */
		numDistinctRows = cheapest_input_path->rows;
	}
	else
	{
		/*
		 * Otherwise, the UNIQUE filter has effects comparable to GROUP BY.
		 */
		List	   *distinctExprs;

		distinctExprs = get_sortgrouplist_exprs(parse->distinctClause,
												parse->targetList);
		numDistinctRows = estimate_num_groups(root, distinctExprs,
											  cheapest_input_path->rows,
											  NULL);
	}

	can_sort = grouping_is_sortable(parse->distinctClause);

	if (set_agg_path_hook != NULL)
	{
		type_mask = (*set_agg_path_hook)(root, FUN_DISTINCT);

		if (HINT_AGG_SORT_TEST(type_mask) ^	HINT_AGG_HASH_TEST(type_mask))
		{
			if (HINT_AGG_HASH_TEST(type_mask))
			{
				hint_can_sort = false;
			}
			else if (HINT_AGG_SORT_TEST(type_mask))
			{
				hint_can_hash = false;
			}
		}
	}

	/*
	 * Consider sort-based implementations of DISTINCT, if possible.
	 */
	if (can_sort && hint_can_sort)
	{
		create_distinct_sort_paths(root, input_rel, parse, numDistinctRows,
								   distinct_rel, cheapest_input_path);
	}

	/*
	 * Consider hash-based implementations of DISTINCT, if possible.
	 *
	 * If we were not able to make any other types of path, we *must* hash or
	 * die trying.  If we do have other choices, there are several things that
	 * should prevent selection of hashing: if the query uses DISTINCT ON
	 * (because it won't really have the expected behavior if we hash), or if
	 * enable_hashagg is off, or if it looks like the hashtable will exceed
	 * work_mem.
	 *
	 * Note: grouping_is_hashable() is much more expensive to check than the
	 * other gating conditions, so we want to do it last.
	 */
	if (distinct_rel->pathlist == NIL && !can_sort)
		allow_hash = true;		/* we have no alternatives */
	else if (parse->hasDistinctOn || !enable_hashagg)
		allow_hash = false;		/* policy-based decision not to hash */
	else
	{
		Size		hashentrysize = hash_agg_entry_size(
			0, cheapest_input_path->pathtarget->width, 0);

		allow_hash = !hashagg_avoid_disk_plan ||
			(hashentrysize * numDistinctRows <= work_mem * 1024L);
	}

	can_hash = allow_hash && grouping_is_hashable(parse->distinctClause);

	if (can_hash && hint_can_hash)
	{
		create_distinct_hash_paths(root, input_rel, parse, numDistinctRows,
								   distinct_rel, cheapest_input_path);
	}

	if (distinct_rel->pathlist == NIL && can_sort)
	{
		create_distinct_sort_paths(root, input_rel, parse, numDistinctRows,
								   distinct_rel, cheapest_input_path);
	}

	/* Give a helpful error if we failed to find any implementation */
	if (distinct_rel->pathlist == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not implement DISTINCT"),
				 errdetail("Some of the datatypes only support hashing, while others only support sorting.")));

	/*
	 * If there is an FDW that's responsible for all baserels of the query,
	 * let it consider adding ForeignPaths.
	 */
	if (distinct_rel->fdwroutine &&
		distinct_rel->fdwroutine->GetForeignUpperPaths)
		distinct_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_DISTINCT,
													   input_rel, distinct_rel);

	/* Let extensions possibly add some more paths */
	if (create_upper_paths_hook)
		(*create_upper_paths_hook) (root, UPPERREL_DISTINCT,
									input_rel, distinct_rel);

	/* Now choose the best path(s) */
	set_cheapest(distinct_rel);

	if (set_agg_rows_hook != NULL)
    	hint_rows = (*set_agg_rows_hook)(root, FUN_DISTINCT, -1); 

	/*
	 * Loop to set Agg Type
	 */
	foreach (lc, distinct_rel->pathlist)
	{
		Path *node_path = (Path *) lfirst(lc);

		if (IsA(node_path, UpperUniquePath))
		{
			UpperUniquePath *unique_path = (UpperUniquePath *) node_path;

			/* If hint_rows is set, set it for distinct node. */
			if (hint_rows > 0)
				unique_path->path.rows = hint_rows;
		}
		else if (IsA(node_path, AggPath))
		{
			AggPath *agg_path = (AggPath *) lfirst(lc);
			agg_path->agg_type = FUN_DISTINCT;

			/* If hint_rows is set, set it for agg node. */
			if (hint_rows > 0)
				agg_path->path.rows = hint_rows;
		}
	}

	return distinct_rel;
}

/*
 * create_ordered_paths
 *
 * Build a new upperrel containing Paths for ORDER BY evaluation.
 *
 * All paths in the result must satisfy the ORDER BY ordering.
 * The only new path we need consider is an explicit sort on the
 * cheapest-total existing path.
 *
 * input_rel: contains the source-data Paths
 * target: the output tlist the result Paths must emit
 * limit_tuples: estimated bound on the number of output tuples,
 *		or -1 if no LIMIT or couldn't estimate
 */
static RelOptInfo *
create_ordered_paths(PlannerInfo *root,
					 RelOptInfo *input_rel,
					 PathTarget *target,
					 bool target_parallel_safe,
					 double limit_tuples)
{
	Path	   *cheapest_input_path = input_rel->cheapest_total_path;
	RelOptInfo *ordered_rel;
	ListCell   *lc;

	/* For now, do all work in the (ORDERED, NULL) upperrel */
	ordered_rel = fetch_upper_rel(root, UPPERREL_ORDERED, NULL);

	/*
	 * If the input relation is not parallel-safe, then the ordered relation
	 * can't be parallel-safe, either.  Otherwise, it's parallel-safe if the
	 * target list is parallel-safe.
	 */
	if (input_rel->consider_parallel && target_parallel_safe)
		ordered_rel->consider_parallel = true;

	/*
	 * If the input rel belongs to a single FDW, so does the ordered_rel.
	 */
	ordered_rel->serverid = input_rel->serverid;
	ordered_rel->userid = input_rel->userid;
	ordered_rel->useridiscurrent = input_rel->useridiscurrent;
	ordered_rel->fdwroutine = input_rel->fdwroutine;

	foreach(lc, input_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);
		bool		is_sorted;

		is_sorted = pathkeys_contained_in(root->sort_pathkeys,
										  path->pathkeys);
		if (path == cheapest_input_path || is_sorted)
		{
			if (!is_sorted)
			{
				/* An explicit sort here can take advantage of LIMIT */
				if (root->hasUserDefinedFun && path->pathtarget != NULL &&
					path->pathtarget->exprs != NULL && path->distribution == NULL &&
					IsA(path, ProjectionPath) && ((ProjectionPath*)path)->subpath != NULL && 
					((ProjectionPath*)path)->subpath->pathtarget != NULL &&
					((ProjectionPath*)path)->subpath->pathtarget->exprs != NULL &&
					((ProjectionPath*)path)->subpath->pathtarget->sortgrouprefs != NULL)
				{
					ProjectionPath *pjpath = (ProjectionPath *)path;
					int				i;
					bool			push_sort = true;

					/*
					 * If there are user define functions with DML, the order of sort and projection
					 * would affect the result set. The user define functions may change the sort keys
					 * and it would make different result order due to different process order.
					 *
					 * If the sort keys contains user define functions, the prejection should be
					 * applied before sort and if not, the sort should process in front of
					 * projection.
					 *
					 * So the user define functions are checked whether to be the sort keys to determine
					 * the execution order of sort and projection.
					 */
					for (i = 0; i < list_length(pjpath->path.pathtarget->exprs); i++)
					{
						if (pjpath->path.pathtarget->sortgrouprefs[i] > 0)
						{
							Node *expr_node = list_nth(pjpath->path.pathtarget->exprs, i);

							if (expr_node != NULL && contain_user_defined_functions(expr_node))
							{
								push_sort = false;
								break;
							}
						}
					}

					/* Check whether the subpath covers all the sort keys. */
					if (push_sort)
					{
						ListCell	   *lc;

						foreach(lc, root->sort_pathkeys)
						{
							PathKey* pkey = (PathKey*) lfirst(lc);

							if (pkey->pk_eclass != NULL && pkey->pk_eclass != NULL && pkey->pk_eclass->ec_sortref > 0)
							{
								push_sort = false;

								for (i = 0; i < list_length(pjpath->subpath->pathtarget->exprs); i++)
								{
									if (pjpath->subpath->pathtarget->sortgrouprefs[i] == pkey->pk_eclass->ec_sortref)
									{
										push_sort = true;
										break;
									}
								}
							}

							if (!push_sort)
								break;
						}
					}

					if (push_sort)
					{
						path = (Path *) create_sort_path(root,
										ordered_rel,
										pjpath->subpath,
										root->sort_pathkeys,
										limit_tuples);
						pjpath->subpath = path;
						path = (Path*)pjpath;
					}
					else
					{
						path = (Path *) create_sort_path(root,
										ordered_rel,
										path,
										root->sort_pathkeys,
										limit_tuples);
					}
				}
				else
				{
					path = (Path *) create_sort_path(root,
									ordered_rel,
									path,
									root->sort_pathkeys,
									limit_tuples);
				}
			}

			/* Add projection step if needed */
			if (path->pathtarget != target)
				path = apply_projection_to_path(root, ordered_rel,
												path, target);

			add_path(ordered_rel, path);
		}
	}

	/*
	 * generate_gather_paths() will have already generated a simple Gather
	 * path for the best parallel path, if any, and the loop above will have
	 * considered sorting it.  Similarly, generate_gather_paths() will also
	 * have generated order-preserving Gather Merge plans which can be used
	 * without sorting if they happen to match the sort_pathkeys, and the loop
	 * above will have handled those as well.  However, there's one more
	 * possibility: it may make sense to sort the cheapest partial path
	 * according to the required output order and then use Gather Merge.
	 */
	if (ordered_rel->consider_parallel && root->sort_pathkeys != NIL &&
		input_rel->partial_pathlist != NIL)
	{
		Path	   *cheapest_partial_path;

		cheapest_partial_path = linitial(input_rel->partial_pathlist);

		/*
		 * If cheapest partial path doesn't need a sort, this is redundant
		 * with what's already been tried.
		 */
		if (!pathkeys_contained_in(root->sort_pathkeys,
								   cheapest_partial_path->pathkeys))
		{
			Path	   *path;
			double		total_groups;

			path = (Path *) create_sort_path(root,
											 ordered_rel,
											 cheapest_partial_path,
											 root->sort_pathkeys,
											 limit_tuples);

			total_groups = cheapest_partial_path->rows *
				cheapest_partial_path->parallel_workers;
			path = (Path *)
				create_gather_merge_path(root, ordered_rel,
										 path,
										 path->pathtarget,
										 root->sort_pathkeys, NULL,
										 &total_groups);

			/* Add projection step if needed */
			if (path->pathtarget != target)
				path = apply_projection_to_path(root, ordered_rel,
												path, target);

			add_path(ordered_rel, path);
		}
	}

	/*
	 * If there is an FDW that's responsible for all baserels of the query,
	 * let it consider adding ForeignPaths.
	 */
	if (ordered_rel->fdwroutine &&
		ordered_rel->fdwroutine->GetForeignUpperPaths)
		ordered_rel->fdwroutine->GetForeignUpperPaths(root, UPPERREL_ORDERED,
													  input_rel, ordered_rel);

	/* Let extensions possibly add some more paths */
	if (create_upper_paths_hook)
		(*create_upper_paths_hook) (root, UPPERREL_ORDERED,
									input_rel, ordered_rel);

	/*
	 * No need to bother with set_cheapest here; grouping_planner does not
	 * need us to do it.
	 */
	Assert(ordered_rel->pathlist != NIL);

	return ordered_rel;
}

/*
 * make_group_input_target
 *	  Generate appropriate PathTarget for initial input to grouping nodes.
 *
 * If there is grouping or aggregation, the scan/join subplan cannot emit
 * the query's final targetlist; for example, it certainly can't emit any
 * aggregate function calls.  This routine generates the correct target
 * for the scan/join subplan.
 *
 * The query target list passed from the parser already contains entries
 * for all ORDER BY and GROUP BY expressions, but it will not have entries
 * for variables used only in HAVING clauses; so we need to add those
 * variables to the subplan target list.  Also, we flatten all expressions
 * except GROUP BY items into their component variables; other expressions
 * will be computed by the upper plan nodes rather than by the subplan.
 * For example, given a query like
 *		SELECT a+b,SUM(c+d) FROM table GROUP BY a+b;
 * we want to pass this targetlist to the subplan:
 *		a+b,c,d
 * where the a+b target will be used by the Sort/Group steps, and the
 * other targets will be used for computing the final results.
 *
 * 'final_target' is the query's final target list (in PathTarget form)
 *
 * The result is the PathTarget to be computed by the Paths returned from
 * query_planner().
 */
static PathTarget *
make_group_input_target(PlannerInfo *root, PathTarget *final_target)
{
	Query	   *parse = root->parse;
	PathTarget *input_target;
	List	   *non_group_cols;
	List	   *non_group_vars;
	int			i;
	ListCell   *lc;

	/*
	 * We must build a target containing all grouping columns, plus any other
	 * Vars mentioned in the query's targetlist and HAVING qual.
	 */
	input_target = create_empty_pathtarget();
	non_group_cols = NIL;

	i = 0;
	foreach(lc, final_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(final_target, i);

		if (sgref && parse->groupClause &&
			get_sortgroupref_clause_noerr(sgref, parse->groupClause) != NULL)
		{
			/*
			 * It's a grouping column, so add it to the input target as-is.
			 */
			add_column_to_pathtarget(input_target, expr, sgref);
		}
		else
		{
			/*
			 * Non-grouping column, so just remember the expression for later
			 * call to pull_var_clause.
			 */
			non_group_cols = lappend(non_group_cols, expr);
		}

		i++;
	}

	/*
	 * If there's a HAVING clause, we'll need the Vars it uses, too.
	 */
	if (parse->havingQual)
		non_group_cols = lappend(non_group_cols, parse->havingQual);

	/*
	 * Pull out all the Vars mentioned in non-group cols (plus HAVING), and
	 * add them to the input target if not already present.  (A Var used
	 * directly as a GROUP BY item will be present already.)  Note this
	 * includes Vars used in resjunk items, so we are covering the needs of
	 * ORDER BY and window specifications.  Vars used within Aggrefs and
	 * WindowFuncs will be pulled out here, too.
	 */
	non_group_vars = pull_var_clause((Node *) non_group_cols,
									 PVC_RECURSE_AGGREGATES |
									 PVC_RECURSE_WINDOWFUNCS |
									 PVC_INCLUDE_PLACEHOLDERS |
									 PVC_INCLUDE_CNEXPR);
	add_new_columns_to_pathtarget(input_target, non_group_vars);

	/* clean up cruft */
	list_free(non_group_vars);
	list_free(non_group_cols);

	/* XXX this causes some redundant cost calculation ... */
	return set_pathtarget_cost_width(root, input_target);
}

/*
 * make_partial_grouping_target
 *	  Generate appropriate PathTarget for output of partial aggregate
 *	  (or partial grouping, if there are no aggregates) nodes.
 *
 * A partial aggregation node needs to emit all the same aggregates that
 * a regular aggregation node would, plus any aggregates used in HAVING;
 * except that the Aggref nodes should be marked as partial aggregates.
 *
 * In addition, we'd better emit any Vars and PlaceholderVars that are
 * used outside of Aggrefs in the aggregation tlist and HAVING.  (Presumably,
 * these would be Vars that are grouped by or used in grouping expressions.)
 *
 * grouping_target is the tlist to be emitted by the topmost aggregation step.
 * havingQual represents the HAVING clause.
 */
static PathTarget *
make_partial_grouping_target(PlannerInfo *root,
							 PathTarget *grouping_target,
							 Node *havingQual)
{
	Query	   *parse = root->parse;
	PathTarget *partial_target;
	List	   *non_group_cols;
	List	   *non_group_exprs;
	int			i;
	ListCell   *lc;

	partial_target = create_empty_pathtarget();
	non_group_cols = NIL;

	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);

		if (sgref && parse->groupClause &&
			get_sortgroupref_clause_noerr(sgref, parse->groupClause) != NULL)
		{
			/*
			 * It's a grouping column, so add it to the partial_target as-is.
			 * (This allows the upper agg step to repeat the grouping calcs.)
			 */
			add_column_to_pathtarget(partial_target, expr, sgref);
		}
		else
		{
			/*
			 * Non-grouping column, so just remember the expression for later
			 * call to pull_var_clause.
			 */
			non_group_cols = lappend(non_group_cols, expr);
		}

		i++;
	}

	/*
	 * If there's a HAVING clause, we'll need the Vars/Aggrefs it uses, too.
	 */
	if (havingQual)
		non_group_cols = lappend(non_group_cols, havingQual);

	/*
	 * Pull out all the Vars, PlaceHolderVars, and Aggrefs mentioned in
	 * non-group cols (plus HAVING), and add them to the partial_target if not
	 * already present.  (An expression used directly as a GROUP BY item will
	 * be present already.)  Note this includes Vars used in resjunk items, so
	 * we are covering the needs of ORDER BY and window specifications.
	 */
	non_group_exprs = pull_var_clause((Node *) non_group_cols,
									  PVC_INCLUDE_AGGREGATES |
									  PVC_RECURSE_WINDOWFUNCS |
									  PVC_INCLUDE_PLACEHOLDERS);

	add_new_columns_to_pathtarget(partial_target, non_group_exprs);

	/*
	 * Adjust Aggrefs to put them in partial mode.  At this point all Aggrefs
	 * are at the top level of the target list, so we can just scan the list
	 * rather than recursing through the expression trees.
	 */
	foreach(lc, partial_target->exprs)
	{
		Aggref	   *aggref = (Aggref *) lfirst(lc);

		if (IsA(aggref, Aggref))
		{
			Aggref	   *newaggref;

			/*
			 * We shouldn't need to copy the substructure of the Aggref node,
			 * but flat-copy the node itself to avoid damaging other trees.
			 */
			newaggref = makeNode(Aggref);
			memcpy(newaggref, aggref, sizeof(Aggref));

			/* For now, assume serialization is required */
			mark_partial_aggref(newaggref, AGGSPLIT_INITIAL_SERIAL);

			lfirst(lc) = newaggref;
		}
	}

	/* clean up cruft */
	list_free(non_group_exprs);
	list_free(non_group_cols);

	/* XXX this causes some redundant cost calculation ... */
	return set_pathtarget_cost_width(root, partial_target);
}

/*
 * Exclude any cn expr from origin_target.
 */
static PathTarget *
make_cn_input_target(PlannerInfo *root, PathTarget *origin_target)
{
	PathTarget *input_target;
	Query	   *parse = root->parse;
	List	   *cn_sp_cols = NIL;
	List	   *cn_sp_vars = NIL;
	int			i;
	ListCell   *lc;

	if (IS_CENTRALIZED_MODE && !parse->connectByExpr)
		return origin_target;

	input_target = create_empty_pathtarget();

	i = 0;
	foreach(lc, origin_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(origin_target, i);

		if (!contain_cn_expr((Node *) expr) &&
			!(!IS_CENTRALIZED_MODE && parse->connectByExpr &&
			  contain_subplans((Node *) expr)))
		{
			add_column_to_pathtarget(input_target, expr, sgref);
		}
		else
		{
			/*
			 * It contains a cn special expr, so just remember the expression
			 * for later call to pull_var_clause.
			 *
			 * Also, if query has CONNECT BY, subplan in target list on both
			 * it's inner and outer path will share same planstate, hence, in
			 * cluster mode they have same cursor for lower RemoteSubplan node.
			 * so pullup target containing subplan as a cn-expr.
			 * TODO: use FN to enable same fragment under subplan could send to
			 * different destination as initplan does, so we do better here.
			 */
			cn_sp_cols = lappend(cn_sp_cols, expr);
		}

		i++;
	}

	/*
	 * TODO: having cn special expr.
	 */
	if (parse->havingQual)
		cn_sp_cols = lappend(cn_sp_cols, parse->havingQual);

	/*
	 * Pull out all the Vars mentioned in non cn-process special cols, and
	 * add them to the input target if not already present. Note this
	 * includes Vars used in resjunk items, so we are covering the needs of
	 * ORDER BY and window specifications. Vars used within Aggrefs and
	 * WindowFuncs will be pulled out here, too.
	 */
	cn_sp_vars = pull_var_clause((Node *) cn_sp_cols,
								 PVC_RECURSE_AGGREGATES |
								 PVC_RECURSE_WINDOWFUNCS |
								 PVC_INCLUDE_PLACEHOLDERS);
	add_new_columns_to_pathtarget(input_target, cn_sp_vars);

	/* clean up cruft */
	list_free(cn_sp_vars);
	list_free(cn_sp_cols);

	/* XXX this causes some redundant cost calculation ... */
	return set_pathtarget_cost_width(root, input_target);
}

/*
 * mark_partial_aggref
 *	  Adjust an Aggref to make it represent a partial-aggregation step.
 *
 * The Aggref node is modified in-place; caller must do any copying required.
 */
void
mark_partial_aggref(Aggref *agg, AggSplit aggsplit)
{
	/* aggtranstype should be computed by this point */
	Assert(OidIsValid(agg->aggtranstype));
	/* ... but aggsplit should still be as the parser left it */
	Assert(agg->aggsplit == AGGSPLIT_SIMPLE);

	/* Mark the Aggref with the intended partial-aggregation mode */
	agg->aggsplit = aggsplit;

	/*
	 * Adjust result type if needed.  Normally, a partial aggregate returns
	 * the aggregate's transition type; but if that's INTERNAL and we're
	 * serializing, it returns BYTEA instead.
	 */
	if (DO_AGGSPLIT_SKIPFINAL(aggsplit))
	{
		if (agg->aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(aggsplit))
			agg->aggtype = BYTEAOID;
		else
			agg->aggtype = agg->aggtranstype;
	}
}

/*
 * postprocess_setop_tlist
 *	  Fix up targetlist returned by plan_set_operations().
 *
 * We need to transpose sort key info from the orig_tlist into new_tlist.
 * NOTE: this would not be good enough if we supported resjunk sort keys
 * for results of set operations --- then, we'd need to project a whole
 * new tlist to evaluate the resjunk columns.  For now, just ereport if we
 * find any resjunk columns in orig_tlist.
 */
static List *
postprocess_setop_tlist(List *new_tlist, List *orig_tlist)
{
	ListCell   *l;
	ListCell   *orig_tlist_item = list_head(orig_tlist);

	foreach(l, new_tlist)
	{
		TargetEntry *new_tle = (TargetEntry *) lfirst(l);
		TargetEntry *orig_tle;

		/* ignore resjunk columns in setop result */
		if (new_tle->resjunk)
			continue;

		Assert(orig_tlist_item != NULL);
		orig_tle = (TargetEntry *) lfirst(orig_tlist_item);
		orig_tlist_item = lnext(orig_tlist_item);
		if (orig_tle->resjunk)	/* should not happen */
			elog(ERROR, "resjunk output columns are not implemented");
		Assert(new_tle->resno == orig_tle->resno);
		new_tle->ressortgroupref = orig_tle->ressortgroupref;
	}
	if (orig_tlist_item != NULL)
		elog(ERROR, "resjunk output columns are not implemented");
	return new_tlist;
}

/*
 * select_active_windows
 *		Create a list of the "active" window clauses (ie, those referenced
 *		by non-deleted WindowFuncs) in the order they are to be executed.
 */
static List *
select_active_windows(PlannerInfo *root, WindowFuncLists *wflists)
{
	List	   *result;
	List	   *actives;
	ListCell   *lc;

	/* First, make a list of the active windows */
	actives = NIL;
	foreach(lc, root->parse->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(lc);

		/* It's only active if wflists shows some related WindowFuncs */
		Assert(wc->winref <= wflists->maxWinRef);
		if (wflists->windowFuncs[wc->winref] != NIL)
			actives = lappend(actives, wc);
	}

	/*
	 * Now, ensure that windows with identical partitioning/ordering clauses
	 * are adjacent in the list.  This is required by the SQL standard, which
	 * says that only one sort is to be used for such windows, even if they
	 * are otherwise distinct (eg, different names or framing clauses).
	 *
	 * There is room to be much smarter here, for example detecting whether
	 * one window's sort keys are a prefix of another's (so that sorting for
	 * the latter would do for the former), or putting windows first that
	 * match a sort order available for the underlying query.  For the moment
	 * we are content with meeting the spec.
	 */
	result = NIL;
	while (actives != NIL)
	{
		WindowClause *wc = (WindowClause *) linitial(actives);
		ListCell   *prev;
		ListCell   *next;

		/* Move wc from actives to result */
		actives = list_delete_first(actives);
		result = lappend(result, wc);

		/* Now move any matching windows from actives to result */
		prev = NULL;
		for (lc = list_head(actives); lc; lc = next)
		{
			WindowClause *wc2 = (WindowClause *) lfirst(lc);

			next = lnext(lc);
			/* framing options are NOT to be compared here! */
			if (equal(wc->partitionClause, wc2->partitionClause) &&
				equal(wc->orderClause, wc2->orderClause))
			{
				actives = list_delete_cell(actives, lc, prev);
				result = lappend(result, wc2);
			}
			else
				prev = lc;
		}
	}

	return result;
}

/*
 * make_window_input_target
 *	  Generate appropriate PathTarget for initial input to WindowAgg nodes.
 *
 * When the query has window functions, this function computes the desired
 * target to be computed by the node just below the first WindowAgg.
 * This tlist must contain all values needed to evaluate the window functions,
 * compute the final target list, and perform any required final sort step.
 * If multiple WindowAggs are needed, each intermediate one adds its window
 * function results onto this base tlist; only the topmost WindowAgg computes
 * the actual desired target list.
 *
 * This function is much like make_group_input_target, though not quite enough
 * like it to share code.  As in that function, we flatten most expressions
 * into their component variables.  But we do not want to flatten window
 * PARTITION BY/ORDER BY clauses, since that might result in multiple
 * evaluations of them, which would be bad (possibly even resulting in
 * inconsistent answers, if they contain volatile functions).
 * Also, we must not flatten GROUP BY clauses that were left unflattened by
 * make_group_input_target, because we may no longer have access to the
 * individual Vars in them.
 *
 * Another key difference from make_group_input_target is that we don't
 * flatten Aggref expressions, since those are to be computed below the
 * window functions and just referenced like Vars above that.
 *
 * 'final_target' is the query's final target list (in PathTarget form)
 * 'activeWindows' is the list of active windows previously identified by
 *			select_active_windows.
 *
 * The result is the PathTarget to be computed by the plan node immediately
 * below the first WindowAgg node.
 */
static PathTarget *
make_window_input_target(PlannerInfo *root,
						 PathTarget *final_target,
						 List *activeWindows)
{
	Query	   *parse = root->parse;
	PathTarget *input_target;
	Bitmapset  *sgrefs;
	List	   *flattenable_cols;
	List	   *flattenable_vars;
	int			i;
	ListCell   *lc;

	Assert(parse->hasWindowFuncs);

	/*
	 * Collect the sortgroupref numbers of window PARTITION/ORDER BY clauses
	 * into a bitmapset for convenient reference below.
	 */
	sgrefs = NULL;
	foreach(lc, activeWindows)
	{
		WindowClause *wc = (WindowClause *) lfirst(lc);
		ListCell   *lc2;

		foreach(lc2, wc->partitionClause)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc2);

			sgrefs = bms_add_member(sgrefs, sortcl->tleSortGroupRef);
		}
		foreach(lc2, wc->orderClause)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc2);

			sgrefs = bms_add_member(sgrefs, sortcl->tleSortGroupRef);
		}
	}

	/* Add in sortgroupref numbers of GROUP BY clauses, too */
	foreach(lc, parse->groupClause)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(lc);
		
		sgrefs = bms_add_member(sgrefs, grpcl->tleSortGroupRef);
	}

	/*
	 * Construct a target containing all the non-flattenable targetlist items,
	 * and save aside the others for a moment.
	 */
	input_target = create_empty_pathtarget();
	flattenable_cols = NIL;

	i = 0;
	foreach(lc, final_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(final_target, i);

		/*
		 * Don't want to deconstruct window clauses or GROUP BY items.  (Note
		 * that such items can't contain window functions, so it's okay to
		 * compute them below the WindowAgg nodes.)
		 */
		if (sgref != 0 && bms_is_member(sgref, sgrefs))
		{
			/*
			 * Don't want to deconstruct this value, so add it to the input
			 * target as-is.
			 */
			add_column_to_pathtarget(input_target, expr, sgref);
		}
		else
		{
			/*
			 * Column is to be flattened, so just remember the expression for
			 * later call to pull_var_clause.
			 */
			flattenable_cols = lappend(flattenable_cols, expr);
		}

		i++;
	}

	/*
	 * Pull out all the Vars and Aggrefs mentioned in flattenable columns, and
	 * add them to the input target if not already present.  (Some might be
	 * there already because they're used directly as window/group clauses.)
	 *
	 * Note: it's essential to use PVC_INCLUDE_AGGREGATES here, so that any
	 * Aggrefs are placed in the Agg node's tlist and not left to be computed
	 * at higher levels.  On the other hand, we should recurse into
	 * WindowFuncs to make sure their input expressions are available.
	 */
	flattenable_vars = pull_var_clause((Node *) flattenable_cols,
									   PVC_INCLUDE_AGGREGATES |
									   PVC_RECURSE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS |
									   PVC_INCLUDE_CNEXPR);
	add_new_columns_to_pathtarget(input_target, flattenable_vars);

	/* clean up cruft */
	list_free(flattenable_vars);
	list_free(flattenable_cols);

	/* XXX this causes some redundant cost calculation ... */
	return set_pathtarget_cost_width(root, input_target);
}

/*
 * make_pathkeys_for_window
 *		Create a pathkeys list describing the required input ordering
 *		for the given WindowClause.
 *
 * The required ordering is first the PARTITION keys, then the ORDER keys.
 * In the future we might try to implement windowing using hashing, in which
 * case the ordering could be relaxed, but for now we always sort.
 */
static List *
make_pathkeys_for_window(PlannerInfo *root, WindowClause *wc,
						 List *tlist)
{
	List	   *window_pathkeys;
	List	   *window_sortclauses;

	/* Throw error if can't sort */
	if (!grouping_is_sortable(wc->partitionClause))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not implement window PARTITION BY"),
				 errdetail("Window partitioning columns must be of sortable datatypes.")));
	if (!grouping_is_sortable(wc->orderClause))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("could not implement window ORDER BY"),
				 errdetail("Window ordering columns must be of sortable datatypes.")));

	/* Okay, make the combined pathkeys */
	window_sortclauses = list_concat(list_copy(wc->partitionClause),
									 list_copy(wc->orderClause));
	window_pathkeys = make_pathkeys_for_sortclauses(root,
													window_sortclauses,
													tlist);
	list_free(window_sortclauses);
	return window_pathkeys;
}

/*
 * make_sort_input_target
 *	  Generate appropriate PathTarget for initial input to Sort step.
 *
 * If the query has ORDER BY, this function chooses the target to be computed
 * by the node just below the Sort (and DISTINCT, if any, since Unique can't
 * project) steps.  This might or might not be identical to the query's final
 * output target.
 *
 * The main argument for keeping the sort-input tlist the same as the final
 * is that we avoid a separate projection node (which will be needed if
 * they're different, because Sort can't project).  However, there are also
 * advantages to postponing tlist evaluation till after the Sort: it ensures
 * a consistent order of evaluation for any volatile functions in the tlist,
 * and if there's also a LIMIT, we can stop the query without ever computing
 * tlist functions for later rows, which is beneficial for both volatile and
 * expensive functions.
 *
 * Our current policy is to postpone volatile expressions till after the sort
 * unconditionally (assuming that that's possible, ie they are in plain tlist
 * columns and not ORDER BY/GROUP BY/DISTINCT columns).  We also prefer to
 * postpone set-returning expressions, because running them beforehand would
 * bloat the sort dataset, and because it might cause unexpected output order
 * if the sort isn't stable.  However there's a constraint on that: all SRFs
 * in the tlist should be evaluated at the same plan step, so that they can
 * run in sync in nodeProjectSet.  So if any SRFs are in sort columns, we
 * mustn't postpone any SRFs.  (Note that in principle that policy should
 * probably get applied to the group/window input targetlists too, but we
 * have not done that historically.)  Lastly, expensive expressions are
 * postponed if there is a LIMIT, or if root->tuple_fraction shows that
 * partial evaluation of the query is possible (if neither is true, we expect
 * to have to evaluate the expressions for every row anyway), or if there are
 * any volatile or set-returning expressions (since once we've put in a
 * projection at all, it won't cost any more to postpone more stuff).
 *
 * Another issue that could potentially be considered here is that
 * evaluating tlist expressions could result in data that's either wider
 * or narrower than the input Vars, thus changing the volume of data that
 * has to go through the Sort.  However, we usually have only a very bad
 * idea of the output width of any expression more complex than a Var,
 * so for now it seems too risky to try to optimize on that basis.
 *
 * Note that if we do produce a modified sort-input target, and then the
 * query ends up not using an explicit Sort, no particular harm is done:
 * we'll initially use the modified target for the preceding path nodes,
 * but then change them to the final target with apply_projection_to_path.
 * Moreover, in such a case the guarantees about evaluation order of
 * volatile functions still hold, since the rows are sorted already.
 *
 * This function has some things in common with make_group_input_target and
 * make_window_input_target, though the detailed rules for what to do are
 * different.  We never flatten/postpone any grouping or ordering columns;
 * those are needed before the sort.  If we do flatten a particular
 * expression, we leave Aggref and WindowFunc nodes alone, since those were
 * computed earlier.
 *
 * 'final_target' is the query's final target list (in PathTarget form)
 * 'have_postponed_srfs' is an output argument, see below
 *
 * The result is the PathTarget to be computed by the plan node immediately
 * below the Sort step (and the Distinct step, if any).  This will be
 * exactly final_target if we decide a projection step wouldn't be helpful.
 *
 * In addition, *have_postponed_srfs is set to TRUE if we choose to postpone
 * any set-returning functions to after the Sort.
 */
static PathTarget *
make_sort_input_target(PlannerInfo *root,
					   PathTarget *final_target,
					   bool *have_postponed_srfs,
					   bool *have_postponed_cnfs)
{
	Query	   *parse = root->parse;
	PathTarget *input_target;
	int			ncols;
	bool	   *col_is_srf;
	bool	   *postpone_col;
	bool		have_srf;
	bool		have_volatile;
	bool		have_expensive;
	bool		have_srf_sortcols;
	bool		postpone_srfs;
	List	   *postponable_cols;
	List	   *postponable_vars;
	bool		sort_by_cnfs = false;
	int			i;
	ListCell   *lc;

	/* Shouldn't get here unless query has ORDER BY */
	Assert(parse->sortClause);

	*have_postponed_srfs = false;	/* default result */
	*have_postponed_cnfs = false;   /* default result */

	/* Inspect tlist and collect per-column information */
	ncols = list_length(final_target->exprs);
	col_is_srf = (bool *) palloc0(ncols * sizeof(bool));
	postpone_col = (bool *) palloc0(ncols * sizeof(bool));
	have_srf = have_volatile = have_expensive = have_srf_sortcols = false;

	i = 0;
	foreach(lc, final_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/*
		 * If the column has a sortgroupref, assume it has to be evaluated
		 * before sorting.  Generally such columns would be ORDER BY, GROUP
		 * BY, etc targets.  One exception is columns that were removed from
		 * GROUP BY by remove_useless_groupby_columns() ... but those would
		 * only be Vars anyway.  There don't seem to be any cases where it
		 * would be worth the trouble to double-check.
		 */
		if (get_pathtarget_sortgroupref(final_target, i) == 0)
		{
			/*
			 * Check for SRF or volatile functions.  Check the SRF case first
			 * because we must know whether we have any postponed SRFs.
			 */
			if (parse->hasTargetSRFs &&
				expression_returns_set((Node *) expr))
			{
				/* We'll decide below whether these are postponable */
				col_is_srf[i] = true;
				have_srf = true;
			}
			else if (!IS_PGXC_DATANODE && /* nothing to worry about on datanode */
					 (contain_user_defined_functions((Node *) expr)))
			{
				/*
				 * If I'm a coordinator planning query with pull-up-ed expression
				 * as a final target, we can't consider to postpone it, because
				 * we must project it as soon as possible after RemoteSubplan
				 * receive a tuple. So it must be a "scantarget".
				 */
				*have_postponed_cnfs = true;
			}
			else if (contain_volatile_functions((Node *) expr))
			{
				/* Unconditionally postpone */
				postpone_col[i] = true;
				have_volatile = true;
			}
			else
			{
				/*
				 * Else check the cost.  XXX it's annoying to have to do this
				 * when set_pathtarget_cost_width() just did it.  Refactor to
				 * allow sharing the work?
				 */
				QualCost	cost;

				cost_qual_eval_node(&cost, (Node *) expr, root);

				/*
				 * We arbitrarily define "expensive" as "more than 10X
				 * cpu_operator_cost".  Note this will take in any PL function
				 * with default cost.
				 */
				if (cost.per_tuple > 10 * cpu_operator_cost)
				{
					postpone_col[i] = true;
					have_expensive = true;
				}
			}
		}
		else
		{
			if (!sort_by_cnfs && contain_user_defined_functions((Node *) expr))
				sort_by_cnfs = true;

			/* For sortgroupref cols, just check if any contain SRFs */
			if (!have_srf_sortcols &&
				parse->hasTargetSRFs &&
				expression_returns_set((Node *) expr))
				have_srf_sortcols = true;
		}

		i++;
	}

	/*
	 * We can't tell outside that we have postponed cnexpr, just let it's own
	 * pull-up logic to "postpone" it.
	 */
	if (sort_by_cnfs)
		*have_postponed_cnfs = false;

	/*
	 * We can postpone SRFs if we have some but none are in sortgroupref cols.
	 */
	postpone_srfs = (have_srf && !have_srf_sortcols);

	/*
	 * If we don't need a post-sort projection, just return final_target.
	 */
	if (!(postpone_srfs || have_volatile ||
		  (have_expensive &&
		   (parse->limitCount || root->tuple_fraction > 0))))
		return final_target;

	/*
	 * Report whether the post-sort projection will contain set-returning
	 * functions.  This is important because it affects whether the Sort can
	 * rely on the query's LIMIT (if any) to bound the number of rows it needs
	 * to return.
	 */
	*have_postponed_srfs = postpone_srfs;

	/*
	 * Construct the sort-input target, taking all non-postponable columns and
	 * then adding Vars, PlaceHolderVars, Aggrefs, and WindowFuncs found in
	 * the postponable ones.
	 */
	input_target = create_empty_pathtarget();
	postponable_cols = NIL;

	i = 0;
	foreach(lc, final_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		if (postpone_col[i] || (postpone_srfs && col_is_srf[i]))
			postponable_cols = lappend(postponable_cols, expr);
		else
			add_column_to_pathtarget(input_target, expr,
									 get_pathtarget_sortgroupref(final_target, i));

		i++;
	}

	/*
	 * Pull out all the Vars, Aggrefs, and WindowFuncs mentioned in
	 * postponable columns, and add them to the sort-input target if not
	 * already present.  (Some might be there already.)  We mustn't
	 * deconstruct Aggrefs or WindowFuncs here, since the projection node
	 * would be unable to recompute them.
	 */
	postponable_vars = pull_var_clause((Node *) postponable_cols,
									   PVC_INCLUDE_AGGREGATES |
									   PVC_INCLUDE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS);
	add_new_columns_to_pathtarget(input_target, postponable_vars);

	/* clean up cruft */
	list_free(postponable_vars);
	list_free(postponable_cols);

	/* XXX this represents even more redundant cost calculation ... */
	return set_pathtarget_cost_width(root, input_target);
}

/*
 * get_cheapest_fractional_path
 *	  Find the cheapest path for retrieving a specified fraction of all
 *	  the tuples expected to be returned by the given relation.
 *
 * We interpret tuple_fraction the same way as grouping_planner.
 *
 * We assume set_cheapest() has been run on the given rel.
 */
Path *
get_cheapest_fractional_path(RelOptInfo *rel, double tuple_fraction)
{
	Path	   *best_path = rel->cheapest_total_path;
	ListCell   *l;

	/* If all tuples will be retrieved, just return the cheapest-total path */
	if (tuple_fraction <= 0.0)
		return best_path;

	/* Convert absolute # of tuples to a fraction; no need to clamp to 0..1 */
	if (tuple_fraction >= 1.0 && best_path->rows > 0)
		tuple_fraction /= best_path->rows;

	foreach(l, rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(l);

		if (path == rel->cheapest_total_path ||
			compare_fractional_path_costs(best_path, path, tuple_fraction) <= 0)
			continue;

		best_path = path;
	}

	return best_path;
}

/*
 * adjust_paths_for_srfs
 *		Fix up the Paths of the given upperrel to handle tSRFs properly.
 *
 * The executor can only handle set-returning functions that appear at the
 * top level of the targetlist of a ProjectSet plan node.  If we have any SRFs
 * that are not at top level, we need to split up the evaluation into multiple
 * plan levels in which each level satisfies this constraint.  This function
 * modifies each Path of an upperrel that (might) compute any SRFs in its
 * output tlist to insert appropriate projection steps.
 *
 * The given targets and targets_contain_srfs lists are from
 * split_pathtarget_at_srfs().  We assume the existing Paths emit the first
 * target in targets.
 */
static void
adjust_paths_for_srfs(PlannerInfo *root, RelOptInfo *rel,
					  List *targets, List *targets_contain_srfs)
{
	ListCell   *lc;

	Assert(list_length(targets) == list_length(targets_contain_srfs));
	Assert(!linitial_int(targets_contain_srfs));

	/* If no SRFs appear at this plan level, nothing to do */
	if (list_length(targets) == 1)
		return;

	/*
	 * Stack SRF-evaluation nodes atop each path for the rel.
	 *
	 * In principle we should re-run set_cheapest() here to identify the
	 * cheapest path, but it seems unlikely that adding the same tlist eval
	 * costs to all the paths would change that, so we don't bother. Instead,
	 * just assume that the cheapest-startup and cheapest-total paths remain
	 * so.  (There should be no parameterized paths anymore, so we needn't
	 * worry about updating cheapest_parameterized_paths.)
	 */
	foreach(lc, rel->pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		Path	   *newpath = subpath;
		ListCell   *lc1,
				   *lc2;

		Assert(subpath->param_info == NULL);
		forboth(lc1, targets, lc2, targets_contain_srfs)
		{
			PathTarget *thistarget = (PathTarget *) lfirst(lc1);
			bool		contains_srfs = (bool) lfirst_int(lc2);

			/* If this level doesn't contain SRFs, do regular projection */
			if (contains_srfs)
				newpath = (Path *) create_set_projection_path(root,
															  rel,
															  newpath,
															  thistarget);
			else
				newpath = (Path *) apply_projection_to_path(root,
															rel,
															newpath,
															thistarget);
		}
		lfirst(lc) = newpath;
		if (subpath == rel->cheapest_startup_path)
			rel->cheapest_startup_path = newpath;
		if (subpath == rel->cheapest_total_path)
			rel->cheapest_total_path = newpath;
	}

	/* Likewise for partial paths, if any */
	foreach(lc, rel->partial_pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		Path	   *newpath = subpath;
		ListCell   *lc1,
				   *lc2;

		Assert(subpath->param_info == NULL);
		forboth(lc1, targets, lc2, targets_contain_srfs)
		{
			PathTarget *thistarget = (PathTarget *) lfirst(lc1);
			bool		contains_srfs = (bool) lfirst_int(lc2);

			/* If this level doesn't contain SRFs, do regular projection */
			if (contains_srfs)
				newpath = (Path *) create_set_projection_path(root,
															  rel,
															  newpath,
															  thistarget);
			else
			{
				/* avoid apply_projection_to_path, in case of multiple refs */
				newpath = (Path *) create_projection_path(root,
														  rel,
														  newpath,
														  thistarget);
			}
		}
		lfirst(lc) = newpath;
	}
}

/*
 * expression_planner
 *		Perform planner's transformations on a standalone expression.
 *
 * Various utility commands need to evaluate expressions that are not part
 * of a plannable query.  They can do so using the executor's regular
 * expression-execution machinery, but first the expression has to be fed
 * through here to transform it from parser output to something executable.
 *
 * Currently, we disallow sublinks in standalone expressions, so there's no
 * real "planning" involved here.  (That might not always be true though.)
 * What we must do is run eval_const_expressions to ensure that any function
 * calls are converted to positional notation and function default arguments
 * get inserted.  The fact that constant subexpressions get simplified is a
 * side-effect that is useful when the expression will get evaluated more than
 * once.  Also, we must fix operator function IDs.
 *
 * Note: this must not make any damaging changes to the passed-in expression
 * tree.  (It would actually be okay to apply fix_opfuncids to it, but since
 * we first do an expression_tree_mutator-based walk, what is returned will
 * be a new node tree.)
 */
Expr *
expression_planner(Expr *expr)
{
	Node	   *result;

	/*
	 * Convert named-argument function calls, insert default arguments and
	 * simplify constant subexprs
	 */
	result = eval_const_expressions(NULL, (Node *) expr);

	/* Fill in opfuncid values if missing */
	fix_opfuncids(result);

	return (Expr *) result;
}


/*
 * plan_cluster_use_sort
 *		Use the planner to decide how CLUSTER should implement sorting
 *
 * tableOid is the OID of a table to be clustered on its index indexOid
 * (which is already known to be a btree index).  Decide whether it's
 * cheaper to do an indexscan or a seqscan-plus-sort to execute the CLUSTER.
 * Return TRUE to use sorting, FALSE to use an indexscan.
 *
 * Note: caller had better already hold some type of lock on the table.
 */
bool
plan_cluster_use_sort(Oid tableOid, Oid indexOid)
{
	PlannerInfo *root;
	Query	   *query;
	PlannerGlobal *glob;
	RangeTblEntry *rte;
	RelOptInfo *rel;
	IndexOptInfo *indexInfo;
	QualCost	indexExprCost;
	Cost		comparisonCost;
	Path	   *seqScanPath;
	Path		seqScanAndSortPath;
	IndexPath  *indexScanPath;
	ListCell   *lc;

	/* We can short-circuit the cost comparison if indexscans are disabled */
	if (!enable_indexscan)
		return true;			/* use sort */

	/* Set up mostly-dummy planner state */
	query = makeNode(Query);
	query->commandType = CMD_SELECT;

	glob = makeNode(PlannerGlobal);

	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;
	root->wt_param_id = -1;
	root->recursiveOk = true;

	/* Build a minimal RTE for the rel */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = tableOid;
	rte->relkind = RELKIND_RELATION;	/* Don't be too picky. */
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = true;
	query->rtable = list_make1(rte);

	/* Set up RTE/RelOptInfo arrays */
	setup_simple_rel_arrays(root);

	/* Build RelOptInfo */
	rel = build_simple_rel(root, 1, NULL);

	/* Locate IndexOptInfo for the target index */
	indexInfo = NULL;
	foreach(lc, rel->indexlist)
	{
		indexInfo = (IndexOptInfo *) lfirst(lc);
		if (indexInfo->indexoid == indexOid)
			break;
	}

	/*
	 * It's possible that get_relation_info did not generate an IndexOptInfo
	 * for the desired index; this could happen if it's not yet reached its
	 * indcheckxmin usability horizon, or if it's a system index and we're
	 * ignoring system indexes.  In such cases we should tell CLUSTER to not
	 * trust the index contents but use seqscan-and-sort.
	 */
	if (lc == NULL)				/* not in the list? */
		return true;			/* use sort */

	/*
	 * Rather than doing all the pushups that would be needed to use
	 * set_baserel_size_estimates, just do a quick hack for rows and width.
	 */
	rel->rows = rel->tuples;
	rel->reltarget->width = get_relation_data_width(tableOid, NULL);

	root->total_table_pages = rel->pages;

	/*
	 * Determine eval cost of the index expressions, if any.  We need to
	 * charge twice that amount for each tuple comparison that happens during
	 * the sort, since tuplesort.c will have to re-evaluate the index
	 * expressions each time.  (XXX that's pretty inefficient...)
	 */
	cost_qual_eval(&indexExprCost, indexInfo->indexprs, root);
	comparisonCost = 2.0 * (indexExprCost.startup + indexExprCost.per_tuple);

	/* Estimate the cost of seq scan + sort */
	seqScanPath = create_seqscan_path(root, rel, NULL, 0);
	cost_sort(&seqScanAndSortPath, root, NIL,
			  seqScanPath->total_cost, rel->tuples, rel->reltarget->width,
			  comparisonCost, maintenance_work_mem, -1.0);

	/* Estimate the cost of index scan */
	indexScanPath = create_index_path(root, indexInfo,
									  NIL, NIL, NIL, NIL, NIL,
									  ForwardScanDirection, false,
									  NULL, 1.0, false);
	if (indexScanPath == NULL)
		return true;

	return (seqScanAndSortPath.total_cost < indexScanPath->path.total_cost);
}

/*
 * plan_create_index_workers
 *		Use the planner to decide how many parallel worker processes
 *		CREATE INDEX should request for use
 *
 * tableOid is the table on which the index is to be built.  indexOid is the
 * OID of an index to be created or reindexed (which must be a btree index).
 *
 * Return value is the number of parallel worker processes to request.  It
 * may be unsafe to proceed if this is 0.  Note that this does not include the
 * leader participating as a worker (value is always a number of parallel
 * worker processes).
 *
 * Note: caller had better already hold some type of lock on the table and
 * index.
 */
int
plan_create_index_workers(Oid tableOid, Oid indexOid)
{
	return 0;
// 	PlannerInfo   *root;
// 	Query         *query;
// 	PlannerGlobal *glob;
// 	RangeTblEntry *rte;
// 	Relation       heap;
// 	Relation       index;
// 	RelOptInfo    *rel;
// 	int            parallel_workers;
// 	BlockNumber    heap_blocks;
// 	double         reltuples;
// 	double         allvisfrac;

// 	/*
// 	 * We don't allow performing parallel operation in standalone backend or
// 	 * when parallelism is disabled.
// 	 */
// 	if (!IsUnderPostmaster || max_parallel_workers_per_gather == 0)
// 		return 0;

// 	/* Set up largely-dummy planner state */
// 	query = makeNode(Query);
// 	query->commandType = CMD_SELECT;

// 	glob = makeNode(PlannerGlobal);

// 	root = makeNode(PlannerInfo);
// 	root->parse = query;
// 	root->glob = glob;
// 	root->query_level = 1;
// 	root->planner_cxt = CurrentMemoryContext;
// 	root->wt_param_id = -1;

// 	/*
// 	 * Build a minimal RTE.
// 	 *
// 	 * Mark the RTE with inh = true.  This is a kludge to prevent
// 	 * get_relation_info() from fetching index info, which is necessary
// 	 * because it does not expect that any IndexOptInfo is currently
// 	 * undergoing REINDEX.
// 	 */
// 	rte = makeNode(RangeTblEntry);
// 	rte->rtekind = RTE_RELATION;
// 	rte->relid = tableOid;
// 	rte->relkind = RELKIND_RELATION; /* Don't be too picky. */
// 	rte->lateral = false;
// 	rte->inh = true;
// 	rte->inFromCl = true;
// 	query->rtable = list_make1(rte);

// 	/* Set up RTE/RelOptInfo arrays */
// 	setup_simple_rel_arrays(root);

// 	/* Build RelOptInfo */
// 	rel = build_simple_rel(root, 1, NULL);

// 	/* Rels are assumed already locked by the caller */
// 	heap = heap_open(tableOid, NoLock);
// 	index = index_open(indexOid, NoLock);

// 	/*
// 	 * Determine if it's safe to proceed.
// 	 *
// 	 * Currently, parallel workers can't access the leader's temporary tables.
// 	 * Furthermore, any index predicate or index expressions must be parallel
// 	 * safe.
// 	 */
// 	if (heap->rd_rel->relpersistence == RELPERSISTENCE_TEMP ||
// 	    !is_parallel_safe(root, (Node *) RelationGetIndexExpressions(index)) ||
// 	    !is_parallel_safe(root, (Node *) RelationGetIndexPredicate(index)))
// 	{
// 		parallel_workers = 0;
// 		goto done;
// 	}

// 	/*
// 	 * If parallel_workers storage parameter is set for the table, accept that
// 	 * as the number of parallel worker processes to launch (though still cap
// 	 * at max_parallel_maintenance_workers).  Note that we deliberately do not
// 	 * consider any other factor when parallel_workers is set. (e.g., memory
// 	 * use by workers.)
// 	 */
// 	if (rel->rel_parallel_workers != -1)
// 	{
// 		parallel_workers = Min(rel->rel_parallel_workers, max_parallel_workers_per_gather);
// 		goto done;
// 	}

// 	/*
// 	 * Estimate heap relation size ourselves, since rel->pages cannot be
// 	 * trusted (heap RTE was marked as inheritance parent)
// 	 */
// 	estimate_rel_size(heap, NULL, &heap_blocks, &reltuples, &allvisfrac);

// 	/*
// 	 * Determine number of workers to scan the heap relation using generic
// 	 * model
// 	 */
// 	parallel_workers = compute_parallel_worker(root, rel, heap_blocks, -1);

// 	/*
// 	 * Cap workers based on available maintenance_work_mem as needed.
// 	 *
// 	 * Note that each tuplesort participant receives an even share of the
// 	 * total maintenance_work_mem budget.  Aim to leave participants
// 	 * (including the leader as a participant) with no less than 32MB of
// 	 * memory.  This leaves cases where maintenance_work_mem is set to 64MB
// 	 * immediately past the threshold of being capable of launching a single
// 	 * parallel worker to sort.
// 	 */
// 	while (parallel_workers > 0 && maintenance_work_mem / (parallel_workers + 1) < 32768L)
// 		parallel_workers--;

// done:
// 	index_close(index, NoLock);
// 	heap_close(heap, NoLock);

// 	return parallel_workers;
}

/*
 * grouping_distribution_match
 * 	Check if the path distribution matches grouping distribution.
 *
 * Grouping preserves distribution if the distribution key is on of the
 * grouping keys (arbitrary one). In that case it's guaranteed that groups
 * on different nodes do not overlap, and we can push the aggregation to
 * remote nodes as a whole.
 *
 * Otherwise we need to either fetch all the data to the coordinator and
 * perform the aggregation there, or use two-phase aggregation, with the
 * first phase (partial aggregation) pushed down, and the second phase
 * (combining and finalizing the results) executed on the coordinator.
 *
 * XXX This is used not only for plain aggregation, but also for various
 * other paths, relying on grouping infrastructure (DISTINCT ON, UNIQUE).
 */
static bool
grouping_distribution_match(PlannerInfo *root, Query *parse, Path *path,
							List *clauses, List *targetList)
{
	int		i, j;
	bool    	*dis_match;
	AttrNumber	*groupColIdx;
	int		nmatches = 0;
	int		numGroupCols = list_length(clauses);
	Distribution *distribution = path->distribution;

	/*
	 * With no explicit data distribution or replicated tables, we can simply
	 * push down the whole aggregation to the remote node, without any sort
	 * of redistribution. So consider this to be a match.
	 */
	if ((distribution == NULL) ||
		IsLocatorReplicated(distribution->distributionType))
		return true;

	/*
	 * But no distribution expression means 'no match' ?
	 * In fact, if the data is located on only one datanode, it is a match, but
	 * we would like to re-distribute the data and make agg in parallel on all
	 * datanodes for now.
	 */
	if (!IsValidDistribution(distribution))
		return false;

	groupColIdx = extract_grouping_cols(clauses, targetList);
	dis_match = (bool *)palloc0(sizeof(bool) * distribution->nExprs);
	/*
	 * With distributed data and table distributed using an expression, we
	 * need to check if the distribution expression matches one of the
	 * grouping keys (arbitrary one).
	 */
	for (i = 0; i < numGroupCols; i++)
	{
		TargetEntry *te = (TargetEntry *)list_nth(targetList,
												  groupColIdx[i] - 1);

		if (IsA(te->expr, Var))
		{
			bool found = false;

			if (check_rte_var_nullable((Node*)parse->jointree,
									   ((Var*)te->expr)->varno,
									   false,
									   &found))
			{
				break;
			}
		}

#ifdef __OPENTENBASE_C__
		for (j = 0; j < distribution->nExprs; j++)
		{
			if (dis_match[j])
				continue;

			if (equal(te->expr, distribution->disExprs[j]) ||
				exprs_known_equal(root, (Node *)te->expr, distribution->disExprs[j]))
			{
				dis_match[j] = true;
				nmatches++;
				break;
			}
		}

		if (distribution->nExprs == nmatches)
			break;
#else
		if (equal(te->expr, distribution->distributionExpr))
		{
			matches_key = true;
			break;
		}
#endif
	}
#ifdef __OPENTENBASE_C__
	pfree(groupColIdx);
	pfree(dis_match);
	return (distribution->nExprs == nmatches);
#else
	return matches_key;
#endif
}

/*
 * add_paths_to_grouping_rel
 *
 * Add non-partial paths to grouping relation.
 */
static void
add_paths_to_grouping_rel(PlannerInfo *root, RelOptInfo *input_rel,
						  RelOptInfo *grouped_rel,
						  PathTarget *target,
						  RelOptInfo *partially_grouped_rel,
						  const AggClauseCosts *agg_costs,
						  const AggClauseCosts *agg_final_costs,
						  grouping_sets_data *gd, bool can_sort, bool can_hash,
						  double dNumGroups, List *havingQual)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	ListCell   *lc;
	int			type_mask = 0;
	bool		hint_can_sort = true;
	bool		hint_can_hash = true;

	/*
	 * XL: To minimize the code complexity in general (and diff compared to
	 * PostgreSQL code base), XL generates the paths in two phases.
	 *
	 * First, we generate the "regular" aggregate paths, and either push them
	 * down as a whole, if possible, or inject a RemoteSubplan below them if.
	 * We may produce 2-phase aggregate paths (partial+finalize), but only if
	 * PostgreSQL itself generates them, and if we can push down the whole
	 * aggregation to datanodes (we don't want to run parallel aggregate on
	 * coordinators). That is, we don't generate paths with 2-phase distributed
	 * aggregate like 'FinalizeAgg -> RemoteSubplan -> PartialAgg' here.
	 *
	 * This block is intentionally keps as close to core PostgreSQL as possible,
	 * and is guaranteed to generate at least one valid aggregate path (plain
	 * aggregate on top of RemoteSubplan). Unless even stock PostgreSQL fail
	 * to generate any paths.
	 *
	 * Then, in the second phase, we generage custom XL paths, with distributed
	 * 2-phase aggregation. We don't do this if we've been able to push the
	 * whole aggregation down, as we assume the full push down is the best
	 * possible plan.
	 *
	 * Note: The grouping set variants are currently disabled by a check in
	 * transformGroupClause. Perhaps we could lift that restriction now.
	 */
#ifdef __OPENTENBASE__
	double num_nodes = path_count_datanodes(cheapest_path);
	double dNumPerDnGroups = ROWS_PER_DN(dNumGroups);

	bool try_distribute_aggregation = !(parse->groupingSets ||
										agg_costs->hasNonPartial ||
										agg_costs->hasNonSerial);
	AggClauseCosts agg_local_costs;
	AggClauseCosts agg_remote_costs;
	PathTarget *grouping_local_target = NULL;

	/* prepare for 2-phrase agg if possible */
	if (try_distribute_aggregation)
	{
		grouping_local_target = make_partial_grouping_target(root, target, (Node *)parse->havingQual);

		MemSet(&agg_local_costs, 0, sizeof(AggClauseCosts));
		MemSet(&agg_remote_costs, 0, sizeof(AggClauseCosts));
		if (parse->hasAggs)
		{
			get_agg_clause_costs(root, (Node *) grouping_local_target->exprs,
								 AGGSPLIT_INITIAL_SERIAL,
								 &agg_local_costs);
			get_agg_clause_costs(root, (Node *) target->exprs,
								 AGGSPLIT_FINAL_DESERIAL,
								 &agg_remote_costs);
			get_agg_clause_costs(root, parse->havingQual,
								 AGGSPLIT_FINAL_DESERIAL,
								 &agg_remote_costs);
		}
	}
#endif

	if (set_agg_path_hook != NULL)
	{
		type_mask = (*set_agg_path_hook)(root, FUN_GROUP_BY);

		if (HINT_AGG_SORT_TEST(type_mask) ^	HINT_AGG_HASH_TEST(type_mask))
		{
			if (can_hash && HINT_AGG_HASH_TEST(type_mask))
			{
				hint_can_sort = false;
			}
			else if (can_sort && HINT_AGG_SORT_TEST(type_mask))
			{
				hint_can_hash = false;
			}
		}
	}

	if (can_sort && hint_can_sort)
	{
		/*
		 * Use any available suitably-sorted path as input, and also consider
		 * sorting the cheapest-total path.
		 */
		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);
			bool		is_sorted;

			is_sorted = pathkeys_contained_in(root->group_pathkeys,
											  path->pathkeys);

			/*
			 * XL: Can it happen that the cheapest path can't be pushed down,
			 * while some other path could be? Perhaps we should move the check
			 * if a path can be pushed down up, and add another OR condition
			 * to consider all paths that can be pushed down?
			 *
			 * if (path == cheapest_path || is_sorted || can_push_down)
			 */
			if (path == cheapest_path || is_sorted)
			{
				/* Sort the cheapest-total path if it isn't already sorted */
				if (!is_sorted)
					path = (Path *) create_sort_path(root,
													grouped_rel,
													path,
													root->group_pathkeys,
													-1.0);

				/*
				 * If the grouping can't be fully pushed down, redistribute the
				 * path on top of the (sorted) path. If if can be pushed down,
				 * disable construction of complex distributed paths.
				 */
#ifdef __OPENTENBASE__
				if (!can_push_down_grouping(root, parse, path))
				{
					/* some special aggs cannot be parallel executed */
					if (!try_distribute_aggregation ||
						path->pathtype == T_Agg || path->pathtype == T_Group)
					{
						if (!parse->groupingSets)
						{
							path = create_redistribute_grouping_path(root, parse, path);
						}
						else if (parse->groupingSets && group_optimizer)
						{
							/* grouping sets do not need remotesubplan */
						}
						else
						{
							path = create_remotesubplan_replicated_path(root, path);
						}
					}
					else
					{
						/*
						 * If the grouping can not be fully pushed down, we
						 * adopt another strategy instead.
						 * 1. do grouping on each datanode locally.
						 * 2. re-distribute grouping results among datanodes,
						 *    then do the final grouping.
						 */
						Assert(!parse->groupingSets);

						if (is_local_agg_worth(dNumGroups, path))
						{
							Path *subpath;
							Path *remote_path;

							if (parse->hasAggs)
							{
								subpath = (Path *) create_agg_path(root,
																   grouped_rel,
																   path,
																   grouping_local_target,
																   parse->groupClause ? AGG_SORTED : AGG_PLAIN,
																   AGGSPLIT_INITIAL_SERIAL,
																   parse->groupClause,
																   NIL,
																   &agg_local_costs,
																   dNumGroups);

								remote_path = create_redistribute_grouping_path(root, parse, subpath);

								add_path(grouped_rel, (Path *)
										 create_agg_path(root,
														grouped_rel,
														remote_path,
														target,
														parse->groupClause ? AGG_SORTED : AGG_PLAIN,
														AGGSPLIT_FINAL_DESERIAL,
														parse->groupClause,
														(List *)parse->havingQual,
														&agg_remote_costs,
														dNumPerDnGroups));
							}
							else if (parse->groupClause)
						 	{
								subpath = (Path *) create_group_path(root,
																	 grouped_rel,
																	 path,
																	 grouping_local_target,
																	 parse->groupClause,
																	 NIL,
																	 dNumGroups);

								remote_path = create_redistribute_grouping_path(root, parse, subpath);

								add_path(grouped_rel, (Path *)
										 create_group_path(root,
														   grouped_rel,
														   remote_path,
														   target,
														   parse->groupClause,
														   (List *)parse->havingQual,
														   dNumPerDnGroups));
						 	}
							else
							{
								/* Other cases should have been handled above */
								Assert(false);
							}
						}

						path = create_redistribute_grouping_path(root, parse, path);
					}
				}
#endif
				/* Now decide what to stick atop it */
				if (parse->groupingSets)
				{
					consider_groupingsets_paths(root, grouped_rel,
												path, true, can_hash, target,
												gd, agg_costs, dNumGroups);
				}
				else if (parse->hasAggs)
				{
					/*
					 * We have aggregation, possibly with plain GROUP BY. Make
					 * an AggPath.
					 */
					add_path(grouped_rel, (Path *)
							 create_agg_path(root,
											 grouped_rel,
											 path,
											 target,
											 parse->groupClause ? AGG_SORTED : AGG_PLAIN,
											 AGGSPLIT_SIMPLE,
											 parse->groupClause,
											 (List *)parse->havingQual,
											 agg_costs,
											 dNumPerDnGroups));
				}
				else if (parse->groupClause)
				{
					/*
					 * We have GROUP BY without aggregation or grouping sets.
					 * Make a GroupPath.
					 */
					add_path(grouped_rel, (Path *)
							 create_group_path(root,
											   grouped_rel,
											   path,
											   target,
											   parse->groupClause,
											   (List *)parse->havingQual,
											   dNumPerDnGroups));
				}
				else
				{
					/* Other cases should have been handled above */
					Assert(false);
				}
			}
		}

		/*
		 * Instead of operating directly on the input relation, we can
		 * consider finalizing a partially aggregated path.
		 */
		if (partially_grouped_rel != NULL)
		{
			foreach(lc, partially_grouped_rel->pathlist)
			{
				Path	   *path = (Path *) lfirst(lc);

				/*
				 * Insert a Sort node, if required.  But there's no point in
				 * sorting anything but the cheapest path.
				 */
				if (!pathkeys_contained_in(root->group_pathkeys, path->pathkeys))
				{
					if (path != partially_grouped_rel->cheapest_total_path)
						continue;
					path = (Path *) create_sort_path(root,
													 grouped_rel,
													 path,
													 root->group_pathkeys,
													 -1.0);
				}

				if (!can_push_down_grouping(root, parse, path))
					path = create_redistribute_grouping_path(root, parse, path);

				if (parse->hasAggs)
					add_path(grouped_rel, (Path *)
							 create_agg_path(root,
											 grouped_rel,
											 path,
											 target,
											 parse->groupClause ? AGG_SORTED : AGG_PLAIN,
											 AGGSPLIT_FINAL_DESERIAL,
											 parse->groupClause,
											 havingQual,
											 agg_final_costs,
											 dNumPerDnGroups));
				else
					add_path(grouped_rel, (Path *)
							 create_group_path(root,
											   grouped_rel,
											   path,
											   target,
											   parse->groupClause,
											   havingQual,
											   dNumPerDnGroups));
			}
		}
	}

	if (can_hash && hint_can_hash)
	{
		double		hashaggtablesize;
		/* Don't mess with the cheapest path directly. */
		Path	   *path = cheapest_path;

		/*
		 * If the grouping can't be fully pushed down, we'll push down the
		 * first phase of the aggregate, and redistribute only the partial
		 * results.
		 *
		 * If if can be pushed down, disable construction of complex
		 * distributed paths.
		 */
#ifdef __OPENTENBASE__
		if (!can_push_down_grouping(root, parse, path))
		{
			/* some special aggs cannot be parallel executed */
			if (!try_distribute_aggregation ||
				path->pathtype == T_Agg || path->pathtype == T_Group)
			{
				if (!parse->groupingSets)
				{
					path = create_redistribute_grouping_path(root, parse, path);
				}
				else if (parse->groupingSets && group_optimizer)
				{
					/* grouping sets do not need remotesubplan */
				}
				else
				{
					path = create_remotesubplan_replicated_path(root, path);
				}
			}
			else
			{
				Assert(!parse->groupingSets);

				if (is_local_agg_worth(dNumGroups, path))
				{
					Path *subpath;
					Path *remote_path;

					/* If 2-phrase agg, we use the partial agg for the peak memory */
					hashaggtablesize = estimate_hashagg_tablesize(path,
																  &agg_local_costs,
																  dNumGroups);

					if (!hashagg_avoid_disk_plan ||
						hashaggtablesize < work_mem * 1024L ||
						grouped_rel->pathlist == NIL)
					{
						subpath = (Path *) create_agg_path(root,
														   grouped_rel,
														   path,
														   grouping_local_target,
														   AGG_HASHED,
														   AGGSPLIT_INITIAL_SERIAL,
														   parse->groupClause,
														   NIL,
														   &agg_local_costs,
														   dNumGroups);

						remote_path = create_redistribute_grouping_path(root, parse, subpath);

						add_path(grouped_rel, (Path *)
								 create_agg_path(root, grouped_rel,
												 remote_path,
												 target,
												 AGG_HASHED,
												 AGGSPLIT_FINAL_DESERIAL,
												 parse->groupClause,
												 (List *) parse->havingQual,
												 &agg_remote_costs,
												 dNumPerDnGroups));
					}
				}

				path = create_redistribute_grouping_path(root, parse, path);
			}
		}
#endif
		if (parse->groupingSets)
		{
			/*
			 * Try for a hash-only groupingsets path over unsorted input.
			 */
			consider_groupingsets_paths(root, grouped_rel,
										path, false, true, target,
										gd, agg_costs, dNumGroups);
		}
		else
		{
			hashaggtablesize = estimate_hashagg_tablesize(path,
														  agg_costs,
														  dNumPerDnGroups);

			/*
			 * Provided that the estimated size of the hashtable does not
			 * exceed work_mem, we'll generate a HashAgg Path, although if we
			 * were unable to sort above, then we'd better generate a Path, so
			 * that we at least have one.
			 */
			if (!hashagg_avoid_disk_plan ||
				hashaggtablesize < work_mem * 1024L ||
				grouped_rel->pathlist == NIL)
			{
				/*
				 * We just need an Agg over the cheapest-total input path,
				 * since input order won't matter.
				 */
				add_path(grouped_rel, (Path *)
						 create_agg_path(root, grouped_rel,
										 path,
										 target,
										 AGG_HASHED,
										 AGGSPLIT_SIMPLE,
										 parse->groupClause,
										 (List *) parse->havingQual,
										 agg_costs,
										 dNumPerDnGroups));
			}
		}

		/*
		 * Generate a Finalize HashAgg Path atop of the cheapest partially
		 * grouped path, assuming there is one. Once again, we'll only do this
		 * if it looks as though the hash table won't exceed work_mem.
		 */
		if (partially_grouped_rel && partially_grouped_rel->pathlist)
		{
			Path	   *path = partially_grouped_rel->cheapest_total_path;

			hashaggtablesize = estimate_hashagg_tablesize(path,
														  agg_final_costs,
														  dNumPerDnGroups);

			if (!hashagg_avoid_disk_plan ||
				hashaggtablesize < work_mem * 1024L)
			{
				if (!can_push_down_grouping(root, parse, path))
					path = create_redistribute_grouping_path(root, parse, path);

				add_path(grouped_rel, (Path *)
						 create_agg_path(root,
										 grouped_rel,
										 path,
										 target,
										 AGG_HASHED,
										 AGGSPLIT_FINAL_DESERIAL,
										 parse->groupClause,
										 (List *) parse->havingQual,
										 agg_final_costs,
										 dNumPerDnGroups));
			}
		}
	}
}

/*
 * create_partial_grouping_paths
 *
 * Create a new upper relation representing the result of partial aggregation
 * and populate it with appropriate paths.  Note that we don't finalize the
 * lists of paths here, so the caller can add additional partial or non-partial
 * paths and must afterward call gather_grouping_paths and set_cheapest on
 * the returned upper relation.
 *
 * All paths for this new upper relation -- both partial and non-partial --
 * have been partially aggregated but require a subsequent FinalizeAggregate
 * step.
 */
static RelOptInfo *
create_partial_grouping_paths(PlannerInfo *root,
							  RelOptInfo *grouped_rel,
							  RelOptInfo *input_rel,
							  grouping_sets_data *gd,
							  bool can_sort,
							  bool can_hash,
							  AggClauseCosts *agg_final_costs)
{
	Query	   *parse = root->parse;
	RelOptInfo *partially_grouped_rel;
	AggClauseCosts agg_partial_costs;
	Path	   *cheapest_partial_path = linitial(input_rel->partial_pathlist);
	double		dNumPartialGroups = 0;
	ListCell   *lc;

	/*
	 * Build a new upper relation to represent the result of partially
	 * aggregating the rows from the input relation.
	 */
	partially_grouped_rel = fetch_upper_rel(root,
											UPPERREL_PARTIAL_GROUP_AGG,
											grouped_rel->relids);
	partially_grouped_rel->consider_parallel =
		grouped_rel->consider_parallel;
	partially_grouped_rel->serverid = grouped_rel->serverid;
	partially_grouped_rel->userid = grouped_rel->userid;
	partially_grouped_rel->useridiscurrent = grouped_rel->useridiscurrent;
	partially_grouped_rel->fdwroutine = grouped_rel->fdwroutine;

	/*
	 * Build target list for partial aggregate paths.  These paths cannot just
	 * emit the same tlist as regular aggregate paths, because (1) we must
	 * include Vars and Aggrefs needed in HAVING, which might not appear in
	 * the result tlist, and (2) the Aggrefs must be set in partial mode.
	 */
	partially_grouped_rel->reltarget =
		make_partial_grouping_target(root, grouped_rel->reltarget,
									 (Node *) parse->havingQual);

	/*
	 * Collect statistics about aggregates for estimating costs of performing
	 * aggregation in parallel.
	 */
	MemSet(&agg_partial_costs, 0, sizeof(AggClauseCosts));
	if (parse->hasAggs)
	{
		List	   *partial_target_exprs;

		/* partial phase */
		partial_target_exprs = partially_grouped_rel->reltarget->exprs;
		get_agg_clause_costs(root, (Node *) partial_target_exprs,
							 AGGSPLIT_INITIAL_SERIAL,
							 &agg_partial_costs);

		/* final phase */
		get_agg_clause_costs(root, (Node *) grouped_rel->reltarget->exprs,
							 AGGSPLIT_FINAL_DESERIAL,
							 agg_final_costs);
		get_agg_clause_costs(root, parse->havingQual,
							 AGGSPLIT_FINAL_DESERIAL,
							 agg_final_costs);
	}

	/* Estimate number of partial groups. */
	dNumPartialGroups = get_number_of_groups(root,
											 cheapest_partial_path->rows,
											 gd,
											 parse->targetList);

	if (can_sort)
	{
		/* This was checked before setting try_parallel_aggregation */
		Assert(parse->hasAggs || parse->groupClause);

		/*
			* Use any available suitably-sorted path as input, and also
			* consider sorting the cheapest partial path.
			*/
		foreach(lc, input_rel->partial_pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);
			bool		is_sorted;

			is_sorted = pathkeys_contained_in(root->group_pathkeys,
												path->pathkeys);
			if (path == cheapest_partial_path || is_sorted)
			{
				/* Sort the cheapest partial path, if it isn't already */
				if (!is_sorted)
					path = (Path *) create_sort_path(root,
													 partially_grouped_rel,
													 path,
													 root->group_pathkeys,
													 -1.0);

				if (parse->hasAggs)
					add_partial_path(partially_grouped_rel, (Path *)
									 create_agg_path(root,
													 partially_grouped_rel,
													 path,
													 partially_grouped_rel->reltarget,
													 parse->groupClause ? AGG_SORTED : AGG_PLAIN,
													 AGGSPLIT_INITIAL_SERIAL,
													 parse->groupClause,
													 NIL,
													 &agg_partial_costs,
													 dNumPartialGroups));
				else
					add_partial_path(partially_grouped_rel, (Path *)
									 create_group_path(root,
													   partially_grouped_rel,
													   path,
													   partially_grouped_rel->reltarget,
													   parse->groupClause,
													   NIL,
													   dNumPartialGroups));
			}
		}
	}

	if (can_hash)
	{
		double		hashaggtablesize;

		/* Checked above */
		Assert(parse->hasAggs || parse->groupClause);

		hashaggtablesize =
			estimate_hashagg_tablesize(cheapest_partial_path,
									   &agg_partial_costs,
									   dNumPartialGroups);

		/*
		 * Tentatively produce a partial HashAgg Path, depending on if it
		 * looks as if the hash table will fit in work_mem.
		 */
		if (!hashagg_avoid_disk_plan || hashaggtablesize < work_mem * 1024L)
		{
			add_partial_path(partially_grouped_rel, (Path *)
							 create_agg_path(root,
												partially_grouped_rel,
												cheapest_partial_path,
												partially_grouped_rel->reltarget,
												AGG_HASHED,
												AGGSPLIT_INITIAL_SERIAL,
												parse->groupClause,
												NIL,
												&agg_partial_costs,
												dNumPartialGroups));
		}
	}

	/*
	 * If there is an FDW that's responsible for all baserels of the query,
	 * let it consider adding partially grouped ForeignPaths.
	 */
	if (partially_grouped_rel->fdwroutine &&
		partially_grouped_rel->fdwroutine->GetForeignUpperPaths)
	{
		FdwRoutine *fdwroutine = partially_grouped_rel->fdwroutine;

		fdwroutine->GetForeignUpperPaths(root,
										 UPPERREL_PARTIAL_GROUP_AGG,
										 input_rel, partially_grouped_rel);
	}

	return partially_grouped_rel;
}

/*
 * Generate Gather and Gather Merge paths for a grouping relation or partial
 * grouping relation.
 *
 * generate_gather_paths does most of the work, but we also consider a special
 * case: we could try sorting the data by the group_pathkeys and then applying
 * Gather Merge.
 *
 * NB: This function shouldn't be used for anything other than a grouped or
 * partially grouped relation not only because of the fact that it explcitly
 * references group_pathkeys but we pass "true" as the third argument to
 * generate_gather_paths().
 */
static void
gather_grouping_paths(PlannerInfo *root, RelOptInfo *rel)
{
	Path	   *cheapest_partial_path;

	/* Try Gather for unordered paths and Gather Merge for ordered ones. */
	generate_gather_paths(root, rel, true);

	/* no need to add explicit sort if not prefer sortagg */
	if (!enable_sortagg)
		return;

	/* Try cheapest partial path + explicit Sort + Gather Merge. */
	cheapest_partial_path = linitial(rel->partial_pathlist);
	if (!pathkeys_contained_in(root->group_pathkeys,
							   cheapest_partial_path->pathkeys))
	{
		Path	   *path;
		double		total_groups;

		total_groups =
			cheapest_partial_path->rows * cheapest_partial_path->parallel_workers;
		path = (Path *) create_sort_path(root, rel, cheapest_partial_path,
										 root->group_pathkeys,
										 -1.0);
		path = (Path *)
			create_gather_merge_path(root,
									 rel,
									 path,
									 rel->reltarget,
									 root->group_pathkeys,
									 NULL,
									 &total_groups);

		add_path(rel, path);
	}
}

/*
 * can_partial_agg
 *
 * Determines whether or not partial grouping and/or aggregation is possible.
 * Returns true when possible, false otherwise.
 */
static bool
can_partial_agg(PlannerInfo *root, const AggClauseCosts *agg_costs)
{
	Query	   *parse = root->parse;

	if (!parse->hasAggs && parse->groupClause == NIL)
	{
		/*
		 * We don't know how to do parallel aggregation unless we have either
		 * some aggregates or a grouping clause.
		 */
		return false;
	}
	else if (parse->groupingSets)
	{
		/* We don't know how to do grouping sets in parallel. */
		return false;
	}
	else if (agg_costs->hasNonPartial || agg_costs->hasNonSerial)
	{
		/* Insufficient support for partial mode. */
		return false;
	}

	/* Everything looks good. */
	return true;
}

/*
 * apply_scanjoin_target_to_paths
 *
 * Adjust the final scan/join relation, and recursively all of its children,
 * to generate the final scan/join target.  It would be more correct to model
 * this as a separate planning step with a new RelOptInfo at the toplevel and
 * for each child relation, but doing it this way is noticeably cheaper.
 * Maybe that problem can be solved at some point, but for now we do this.
 *
 * If tlist_same_exprs is true, then the scan/join target to be applied has
 * the same expressions as the existing reltarget, so we need only insert the
 * appropriate sortgroupref information.  By avoiding the creation of
 * projection paths we save effort both immediately and at plan creation time.
 */
static void
apply_scanjoin_target_to_paths(PlannerInfo *root,
							   RelOptInfo *rel,
							   List *scanjoin_targets,
							   List *scanjoin_targets_contain_srfs,
							   bool scanjoin_target_parallel_safe,
							   bool tlist_same_exprs,
							   bool have_postponed_cnfs)
{
	ListCell   *lc;
	PathTarget *scanjoin_target;

	check_stack_depth();

	/*
	 * If the scan/join target is not parallel-safe, then the new partial
	 * pathlist is the empty list.
	 */
	if (!scanjoin_target_parallel_safe)
	{
		rel->partial_pathlist = NIL;
		rel->consider_parallel = false;
	}

	/*
	 * Update the reltarget.  This may not be strictly necessary in all cases,
	 * but it is at least necessary when create_append_path() gets called
	 * below directly or indirectly, since that function uses the reltarget as
	 * the pathtarget for the resulting path.  It seems like a good idea to do
	 * it unconditionally.
	 */
	rel->reltarget = llast_node(PathTarget, scanjoin_targets);

	/* Special case: handly dummy relations separately. */
	if (IS_DUMMY_REL(rel))
	{
		/*
		 * Since this is a dummy rel, it's got a single Append path with no
		 * child paths.  Replace it with a new path having the final scan/join
		 * target.  (Note that since Append is not projection-capable, it
		 * would be bad to handle this using the general purpose code below;
		 * we'd end up putting a ProjectionPath on top of the existing Append
		 * node, which would cause this relation to stop appearing to be a
		 * dummy rel.)
		 */
		rel->pathlist = list_make1(create_append_path(NULL, rel, NIL, NIL, NULL,
													  0, false, NIL, -1));
		rel->partial_pathlist = NIL;
		set_cheapest(rel);
		Assert(IS_DUMMY_REL(rel));

		/*
		 * Forget about any child relations.  There's no point in adjusting
		 * them and no point in using them for later planning stages (in
		 * particular, partitionwise aggregate).
		 */
		rel->nparts = 0;
		rel->part_rels = NULL;
		rel->boundinfo = NULL;

		return;
	}

	if (have_postponed_cnfs)
	{
		foreach(lc, rel->pathlist)
		{
			Path	   *subpath = (Path *) lfirst(lc);
			Path	   *path = subpath;
			bool		is_sorted;

			is_sorted = pathkeys_contained_in(root->sort_pathkeys,
											  path->pathkeys);
			if (!is_sorted)
			{
				/* An explicit sort here can take advantage of LIMIT */
				path = (Path *) create_sort_path(root,
												 rel,
												 path,
												 root->sort_pathkeys,
												 -1);
			}

			/* If we had to add a Result, path is different from subpath */
			if (path != subpath)
			{
				lfirst(lc) = path;
				if (subpath == rel->cheapest_startup_path)
					rel->cheapest_startup_path = path;
				if (subpath == rel->cheapest_total_path)
					rel->cheapest_total_path = path;
			}
		}
	}

	/* Extract SRF-free scan/join target. */
	scanjoin_target = linitial_node(PathTarget, scanjoin_targets);

	/*
	 * Adjust each input path.  If the tlist exprs are the same, we can just
	 * inject the sortgroupref information into the existing pathtarget.
	 * Otherwise, replace each path with a projection path that generates the
	 * SRF-free scan/join target.  This can't change the ordering of paths
	 * within rel->pathlist, so we just modify the list in place.
	 */
	foreach(lc, rel->pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		Path	   *newpath;

		Assert(subpath->param_info == NULL);

		if (tlist_same_exprs)
			subpath->pathtarget->sortgrouprefs =
				scanjoin_target->sortgrouprefs;
		else
		{
			if (IsA(subpath, PartIteratorPath))
			{
				PartIteratorPath *pipath = (PartIteratorPath *) subpath;
				pipath->subPath =
					(Path *) create_projection_path(root, rel, pipath->subPath, scanjoin_target);
				apply_scanjoin_target_cost_partiterator(pipath);
			}
			else
			{
				newpath = (Path *) create_projection_path(root, rel, subpath, scanjoin_target);
				lfirst(lc) = newpath;
			}
		}
	}

	/* Same for partial paths. */
	foreach(lc, rel->partial_pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		Path	   *newpath;

		/* Shouldn't have any parameterized paths anymore */
		Assert(subpath->param_info == NULL);

		if (tlist_same_exprs)
			subpath->pathtarget->sortgrouprefs =
				scanjoin_target->sortgrouprefs;
		else
		{
			if (IsA(subpath, PartIteratorPath))
			{
				PartIteratorPath *pipath = (PartIteratorPath *) subpath;
				pipath->subPath =
					(Path *) create_projection_path(root, rel, pipath->subPath, scanjoin_target);
				apply_scanjoin_target_cost_partiterator(pipath);
			}
			else
			{
				newpath = (Path *) create_projection_path(root, rel, subpath, scanjoin_target);
				lfirst(lc) = newpath;
			}
		}
	}

	/* Now fix things up if scan/join target contains SRFs */
	if (root->parse->hasTargetSRFs)
		adjust_paths_for_srfs(root, rel,
							  scanjoin_targets,
							  scanjoin_targets_contain_srfs);

	/*
	 * If the relation is partitioned, recurseively apply the same changes to
	 * all partitions and generate new Append paths. Since Append is not
	 * projection-capable, that might save a separate Result node, and it also
	 * is important for partitionwise aggregate.
	 */
	if (rel->part_scheme && rel->part_rels)
	{
		int			partition_idx;
		List	   *live_children = NIL;

		/* Adjust each partition. */
		for (partition_idx = 0; partition_idx < rel->nparts; partition_idx++)
		{
			RelOptInfo *child_rel = rel->part_rels[partition_idx];
			ListCell   *lc;
			AppendRelInfo **appinfos;
			int			nappinfos;
			List	   *child_scanjoin_targets = NIL;

			/* Pruned or dummy children can be ignored. */
			if (child_rel == NULL || IS_DUMMY_REL(child_rel) || child_rel->pathlist == NULL)
				continue;

			/* Translate scan/join targets for this child. */
			appinfos = find_appinfos_by_relids(root, child_rel->relids,
											   &nappinfos);
			foreach(lc, scanjoin_targets)
			{
				PathTarget *target = lfirst_node(PathTarget, lc);

				target = copy_pathtarget(target);
				target->exprs = (List *)
					adjust_appendrel_attrs(root,
										   (Node *) target->exprs,
										   nappinfos, appinfos);
				child_scanjoin_targets = lappend(child_scanjoin_targets,
												 target);
			}
			pfree(appinfos);

			/* Recursion does the real work. */
			apply_scanjoin_target_to_paths(root, child_rel,
										   child_scanjoin_targets,
										   scanjoin_targets_contain_srfs,
										   scanjoin_target_parallel_safe,
										   tlist_same_exprs,
										   have_postponed_cnfs);

			/* Save non-dummy children for Append paths. */
			if (!IS_DUMMY_REL(child_rel))
				live_children = lappend(live_children, child_rel);
		}

		/* Build new paths for this relation by appending child paths. */
		if (live_children != NIL)
			add_paths_to_append_rel(root, rel, live_children);
	}

	/*
	 * Consider generating Gather or Gather Merge paths.  We must only do this
	 * if the relation is parallel safe, and we don't do it for child rels to
	 * avoid creating multiple Gather nodes within the same plan. We must do
	 * this after all paths have been generated and before set_cheapest, since
	 * one of the generated paths may turn out to be the cheapest one.
	 */
	if (rel->consider_parallel && !IS_OTHER_REL(rel))
	{
		if (IS_CENTRALIZED_DATANODE && root->parallel_dml)
		{
			Assert(root->glob->parallelModeOK);
			
			/* TODO: need more test */
			if (root->glob->maxParallelHazard == PROPARALLEL_UNSAFE)
			{
				rel->partial_pathlist = NIL;
				rel->consider_parallel = false;
			}
		}
		else
		{
			generate_gather_paths(root, rel, false);
		}
	}

	/*
	 * Reassess which paths are the cheapest, now that we've potentially added
	 * new Gather (or Gather Merge) and/or Append (or MergeAppend) paths to
	 * this relation.
	 */
	set_cheapest(rel);
}

static bool
groupingsets_distribution_match(PlannerInfo *root, Query *parse, Path *path)
{
	Distribution *distribution = path->distribution;

	/*
	 * With no explicit data distribution or replicated tables, we can simply
	 * push down the whole grouping sets to the remote node, without any sort
	 * of redistribution. So consider this to be a match.
	 */
	if ((distribution == NULL) ||
		IsLocatorReplicated(distribution->distributionType))
		return true;

	return false;
}

static List *
buildForeignTableVarList(int relid, Relation ext_rel)
{
	Var *var = NULL;
	List *result = NIL;
	int i = 0;
	TupleDesc tupleDesc = RelationGetDescr(ext_rel);
	
	for (i = 1; i <= tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i - 1);
		
		if (attr->attisdropped)
			continue;
		
		/* Make a Var representing the desired value */
		var = makeVar(relid, i, attr->atttypid, attr->atttypmod, InvalidOid, 0);
		/* ... and add it to the resultlist */
		result = lappend(result, var);
	}
	
	return result;
}

/*
 * adjust_path_distribution
 *		Adjust distribution of the path to match what's expected by ModifyTable.
 *
 * We use root->distribution to communicate distribution expected by a ModifyTable.
 * Currently it's set either in preprocess_targetlist() for simple target relations,
 * or in inheritance_planner() for targets that are inheritance trees.
 *
 * If root->distribution is NULL, we don't need to do anything and we can leave the
 * path distribution as it is. This happens when there is no ModifyTable node, for
 * example.
 *
 * If the root->distribution is set, we need to inspect it and redistribute the data
 * if needed (when it root->distribution does not match path->distribution).
 *
 * We also detect DML (e.g. correlated UPDATE/DELETE or updates of distribution key)
 * that we can't handle at this point.
 *
 * XXX We must not update root->distribution here, because we need to do this on all
 * paths considered by grouping_planner(), and there's no obvious guarantee all the
 * paths will share the same distribution. Postgres-XL 9.5 was allowed to do that,
 * because prior to the pathification (in PostgreSQL 9.6) grouping_planner() picked
 * before the distributions were adjusted.
 *
 * While import data for a shard table through external table(tdx),
 * no need to redistribution.
 */
static Path *
adjust_path_distribution(PlannerInfo *root, Query *parse, Path *path)
{
	RangeTblEntry *rte = NULL;
	RangeTblEntry *ext_rte = NULL;
	Relation ext_rel = NULL;
	bool is_writable_external_table = false;
	bool is_external_scan_path = false;
	bool is_foreignscan_tlist_match_exttable = true;

#ifdef _PG_ORCL_
	/*
	 * opentenbase_ora dblink supports DML by performing on coordinator node.
	 * Thus, we have to check the subpath distribution to see if we
	 * need to add the RemoteSubplan to gather all data to
	 * coordinator node.
	 */
	if (root->parse->resultRelation > 0)
	{
		rte = rt_fetch(root->parse->resultRelation, root->parse->rtable);
		if (rte->relkind == RELKIND_FOREIGN_TABLE && 
				!rel_is_external_table(rte->relid) && 
				!rel_is_hdfsfdw_table(rte->relid))
		{
			if (path->distribution)
				return create_remotesubplan_path(root, path, NULL);
			else
				return path;
		}
		is_writable_external_table = (rel_is_external_table(rte->relid) || rel_is_hdfsfdw_table(rte->relid));
	}
#endif
	
	/* if there is no root distribution, no redistribution is needed */
	if (!root->distribution)
		return path;

	/* and also skip dummy paths */
	if (IS_DUMMY_PATH(path))
		return path;

	/*
	 * Add remote plan when DML statement contains function nextval
	 */
	if (parse->commandType != CMD_SELECT &&
		root->distribution != NULL &&
		root->distribution->distributionType == LOCATOR_TYPE_REPLICATED &&
		path->distribution != NULL &&
		path->distribution->distributionType == LOCATOR_TYPE_REPLICATED &&
		IsA(path, ProjectionPath) &&
		IsA(((ProjectionPath*)path)->subpath, SubqueryScanPath))
	{
		SubqueryScanPath *subquery_scan_path = (SubqueryScanPath *)((ProjectionPath*)path)->subpath;

		if (IsA(subquery_scan_path->subpath, ProjectionPath) &&
				contain_volatile_functions((Node *) subquery_scan_path->subpath->pathtarget->exprs))
		{
			path = create_remotesubplan_path(root, path, NULL);
			return path;
		}
	}

	if (path->parent->relid > 0)
	{
		ext_rte = planner_rt_fetch(path->parent->relid, root);
		is_external_scan_path = rel_is_external_table(ext_rte->relid);
		if (rte && is_external_scan_path)
		{
			List *ext_vlist = NIL;
			ListCell *lc1;
			ListCell *lc2;
			Relation rel = relation_open(rte->relid, AccessShareLock);
			ext_rel = relation_open(ext_rte->relid, AccessShareLock);
			
			/* It is not allowed to import data to tables with triggers through the external table.*/
			if (rel->rd_rel->relkind == RELKIND_RELATION && rel->trigdesc)
				ereport(ERROR,
				        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						        errmsg("cannot import data into table with triggers via tdx")));
			
			ext_vlist = buildForeignTableVarList(path->parent->relid, ext_rel);
			if (list_length(ext_vlist) != list_length(path->pathtarget->exprs))
				is_foreignscan_tlist_match_exttable = false;
			else
			{
				forboth(lc1, ext_vlist, lc2, path->pathtarget->exprs)
				{
					Var *var = (Var *) lfirst(lc1);
					Expr *expr = (Expr *) lfirst(lc2);
					
					if (!IsA(expr, Var) || !equal_var(var, (Var *) expr))
					{
						is_foreignscan_tlist_match_exttable = false;
						break;
					}
				}
			}
			
			relation_close(rel, AccessShareLock);
			relation_close(ext_rel, AccessShareLock);
		}
	}

	/*
	 * Both the path and root have distribution. Let's see if they differ,
	 * and do a redistribution if not.
	 *
	 * It is not necessary to do a redistribution for writable external table or
	 * importing data into a shard table via a readable external table 
	 * (foreignscan's output list must match exttable's columns).
	 */
	if ((equal_distributions(root, path)
#ifdef _PG_ORCL_
			&& root->target_rel_gSeqId == InvalidOid
#endif
			&& (nodeTag(path) != T_ForeignPath)
			&& !(root->parse->commandType == CMD_INSERT &&
				 IsLocatorReplicated(path->distribution->distributionType) &&
				 contain_volatile_functions((Node *) parse->targetList))) ||
			(path->distribution != NULL && is_writable_external_table) ||
			(root->distribution && (root->distribution->distributionType == LOCATOR_TYPE_SHARD) 
								&& is_external_scan_path && is_foreignscan_tlist_match_exttable))
	{
		if (IsLocatorReplicated(path->distribution->distributionType))
		{
			if (contain_volatile_functions((Node *) parse->targetList))
				ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					errmsg("can not update replicated table with result of volatile function")));

			if (is_writable_external_table)
			{
				if (path->distribution->restrictNodes == NULL)
					path->distribution->restrictNodes = bms_make_singleton(GetAnyDataNode(path->distribution->nodes));
			}
		}

		/*
		 * Source tuple will be consumed on the same node where it is
		 * produced, so if it is known that some node does not yield tuples
		 * we do not want to send subquery for execution on these nodes
		 * at all. So copy the restriction to the external distribution.
		 *
		 * XXX Is that ever possible if external restriction is already
		 * defined? If yes we probably should use intersection of the sets,
		 * and if resulting set is empty create dummy plan and set it as
		 * the result_plan. Need to think this over
		 */
		root->distribution->restrictNodes =
				bms_copy(path->distribution->restrictNodes);
	}
	else
	{
		/*
		 * If the planned statement is either UPDATE or DELETE, different
		 * distributions here mean the ModifyTable node will be placed on
		 * top of RemoteSubquery.
		 *
		 * UPDATE and DELETE versions of ModifyTable use TID of incoming
		 * tuple to apply the changes, but the RemoteSubquery plan supplies
		 * RemoteTuples, without such field. Therefore we can't execute
		 * such plan and error-out.
		 *
		 * Most common example is when the UPDATE statement modifies the
		 * distribution column, or when a complex UPDATE or DELETE statement
		 * involves a join. It's difficult to determine the exact reason,
		 * but we assume the first one (correlated UPDATE) is more likely.
		 *
		 * There are two ways of fixing the UPDATE ambiguity:
		 *
		 * 1. Modify the planner to never consider redistribution of the
		 * target table. In this case the planner would find there's no way
		 * to plan the query, and it would throw error somewhere else, and
		 * we'd only be dealing with updates of distribution columns.
		 *
		 * 2. Modify executor to allow distribution column updates. However
		 * there are a lot of issues behind the scene when implementing that
		 * approach, and so it's unlikely to happen soon.
		 *
		 * DELETE statements may only fail because of complex joins.
		 */

#ifdef __FDW__
        if (!(nodeTag(path) == T_ForeignPath))
        {
#endif
		if (parse->commandType == CMD_UPDATE)
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 errmsg("could not plan this distributed update"),
					 errdetail("correlated UPDATE or updating distribution column currently not supported in Postgres-XL.")));

#ifdef __FDW__
        }
#endif

#ifdef __OPENTENBASE__
		/*
		  * If the planned statement is DML, and the target relation has
		  * unshippable triggers, we will add distribution before ModifyTable
		  * instead.
                 */
		if (parse->hasUnshippableDml)
		{
			if (!(parse->commandType == CMD_INSERT ||
				  parse->commandType == CMD_UPDATE ||
				  parse->commandType == CMD_DELETE ||
				  parse->commandType == CMD_MERGE))
		    	elog(ERROR, "unshippable triggers with non-DML statement.");
			return path;
		}
#endif

		/*
		 * We already know the distributions are not equal, but let's see if
		 * the redistribution is actually necessary. We can skip it if we
		 * already have Result path, and if the distribution is one of
		 *
		 * a) 'hash' restricted to a single node
		 * b) 'shard' restricted to a single node
		 * c) 'replicate' without volatile functions in the target list
		 *
		 * In those cases we don't need the RemoteSubplan.
		 *
		 * XXX Not sure what the (result_plan->lefttree == NULL) does.
		 * See planner.c:2730 in 9.5.
		 */
		if (!(path->pathtype == T_Result &&
		      !path->distribution && /* FIXME missing (result_plan->lefttree == NULL) condition */
		      (((root->distribution->distributionType == LOCATOR_TYPE_HASH ||
		         root->distribution->distributionType == LOCATOR_TYPE_SHARD) &&
		        bms_num_members(root->distribution->restrictNodes) == 1) ||
		       (root->distribution->distributionType == LOCATOR_TYPE_REPLICATED &&
		        !contain_mutable_functions((Node *) parse->targetList) && parse->canShip))) ||
		        /* we will create a remote subquery while insert value into table with rowid */
		        (root->target_rel_gSeqId != InvalidOid && root->distribution->distributionType == LOCATOR_TYPE_REPLICATED))
		{
			if (parse->commandType == CMD_MERGE)
			{
				path = create_mergequalproj_path(root,
				                                 path,
				                                 parse->mergeActionList,
				                                 path->distribution);
			}
			path = create_remotesubplan_path(root, path, root->distribution);
		}
	}

	return path;
}

static bool
can_push_down_grouping(PlannerInfo *root, Query *parse, Path *path)
{
	/* only called when constructing grouping paths */
	Assert(parse->hasAggs || parse->groupClause || ORA_MODE);

	if (parse->groupingSets)
		return groupingsets_distribution_match(root, parse, path);

	return grouping_distribution_match(root, parse, path, parse->groupClause, parse->targetList);
}

static bool
can_push_down_window(PlannerInfo *root, Path *path)
{
	if (!path->distribution ||
		IsLocatorReplicated(path->distribution->distributionType))
		return true;

	return false;
}

#ifdef __OPENTENBASE__
static Path *
adjust_modifytable_subpath(PlannerInfo *root, Query *parse, Path *path)
{
	path = create_remotesubplan_path(root, path, NULL);

	return path;
}
#endif

#ifdef __OPENTENBASE_C__
static List *
create_groupby_clause(GroupingSetData *g_set, List *groupClause)
{
	List *gp_clause = NULL;

	if (g_set->set)
	{
		ListCell *lc;
		ListCell *gc;

		foreach(lc, g_set->set)
		{
			bool match = false;
			int sortGroupRef = lfirst_int(lc);

			foreach(gc, groupClause)
			{
				SortGroupClause * sortGroupClause = (SortGroupClause *)lfirst(gc);

				if (sortGroupRef == sortGroupClause->tleSortGroupRef)
				{
					gp_clause = lappend(gp_clause, sortGroupClause);
					match = true;
					break;
				}
			}

			if (!match)
			{
				elog(ERROR, "could not find matched groupby clause");
			}
		}
	}

	return gp_clause;
}

static PathTarget *
create_group_pathtarget(PlannerInfo *root, GroupingSetData *g_set,
						PathTarget *target, int *nagg, bool *groupingFunc,
						List *groupClause)
{
	ListCell   *lc = NULL;
	PathTarget *result = copy_pathtarget(target);
	Bitmapset  *ungroup_refs = NULL;
	List	   *ungroup_clauses = NIL;
	List	   *group_clauses = NIL;
	List	   *target_vars = NIL;
	Query      *qry = root->parse;
	int			ref, i;

	/* copy_pathtarget only do shallow copy, make a deep copy alone */
	result->exprs = copyObject(target->exprs);

	foreach(lc, groupClause)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(grpcl, qry->targetList);
		group_clauses = lappend(group_clauses, tle->expr);
	}
	
	ungroup_clauses = list_difference(qry->groupClause, groupClause);
	foreach(lc, ungroup_clauses)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(grpcl, qry->targetList);

		result->exprs = replace_ungrouped_columns(result->exprs,
												  group_clauses,
												  (Node *) tle->expr);
		ungroup_refs = bms_add_member(ungroup_refs, grpcl->tleSortGroupRef);
	}

	ref = -1;
	while ((ref = bms_next_member(ungroup_refs, ref)) >= 0)
	{
		for (i = 0; i < list_length(result->exprs); i++)
		{
			if (result->sortgrouprefs[i] == ref)
				result->sortgrouprefs[i] = 0;
		}
	}

	foreach(lc, g_set->set)
	{
		bool match = false;
		int sortGroupRef = lfirst_int(lc);

		for (i = 0; i < list_length(target->exprs); i++)
		{
			if (sortGroupRef == target->sortgrouprefs[i])
			{
				List *vars = pull_var_clause((Node *)list_nth(target->exprs, i),
											 PVC_INCLUDE_AGGREGATES |
											 PVC_RECURSE_PLACEHOLDERS |
											 PVC_RECURSE_WINDOWFUNCS);
				target_vars = list_concat(target_vars, vars);
				match = true;
				break;
			}
		}

		if (!match)
			elog(ERROR, "could not find target vars in pathtarget");
	}

	foreach(lc, result->exprs)
	{
		Node *node = (Node *)lfirst(lc);

		if (IsA(node, Aggref))
		{
			if (nagg)
			{
				*nagg = *nagg + 1;
			}
		}
		else if (IsA(node, GroupingFunc))
		{
			if (groupingFunc)
			{
				/*
				 * The fact that g_set->set is empty indicates that we are
				 * dealing with the last branch where all grouping columns
				 * are NULL. In this case, there is actually no need to
				 * compute the GroupingFunc in the executor; instead, we
				 * can directly create a bitset with all bits set to 1.
				 */
				if (list_length(g_set->set) == 0 && !ORA_MODE)
				{
					/*
					 * transformGroupingFunc only allow arguments fewer than 32
					 * which means we can make a const int32 here.
					 */
					GroupingFunc *gf = (GroupingFunc *) node;
					Assert(list_length(gf->refs) < 32);
					lfirst(lc) = makeConst(INT4OID,
										   InvalidOid,
										   InvalidOid,
										   sizeof(int32),
										   (1 << list_length(gf->refs)) - 1,
										   false,
										   true);
				}
				else
					*groupingFunc = true;
			}
		}
		else
		{
			ListCell *var_lc = NULL;
			List *vars = replace_var_clause(&node, target_vars,
											list_length(g_set->set) == 0 && !ORA_MODE);

			foreach(var_lc, vars)
			{
				Node *n = (Node *)lfirst(var_lc);

				if (IsA(n, GroupingFunc))
				{
					if (groupingFunc)
						*groupingFunc = true;
				}
				else if (IsA(n, Aggref))
				{
					if (nagg)
						*nagg = *nagg + 1;
				}
			}
		}
	}

	bms_free(ungroup_refs);
	list_free(ungroup_clauses);
	list_free(target_vars);

	return result;
}

void
fix_grouping_func_refs(PathTarget *target, List *targetlist)
{
	ListCell *var_lc = NULL;
	List *vars = pull_var_clause((Node *)target->exprs, PVC_INCLUDE_AGGREGATES | 
	                               PVC_RECURSE_PLACEHOLDERS |
	                               PVC_RECURSE_WINDOWFUNCS);

	foreach(var_lc, vars)
	{
		Node *n = (Node *)lfirst(var_lc);
		if (IsA(n, GroupingFunc))
		{
			List *cols = NIL;
			ListCell *ref_lc;
			GroupingFunc *gp = (GroupingFunc *)n;

			foreach(ref_lc, gp->refs)
			{	
				bool match = false;
				int fake_resno = list_length(targetlist) + 1;
				int sortgroupref = lfirst_int(ref_lc);
				ListCell *ent_lc;

				foreach(ent_lc, targetlist)
				{
					TargetEntry *tent = (TargetEntry *)lfirst(ent_lc);

					if (tent->ressortgroupref == sortgroupref)
					{
						cols = lappend_int(cols, tent->resno);
						match = true;
						break;
					}
				}

				if (!match)
				{
					cols = lappend_int(cols, fake_resno);
				}

			}

			gp->cols = cols;

			gp->groupingSets = true;
		}
	}
}

bool
set_distinct_aggrefs(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;
		if (aggref->distinct_args)
		{
			if (distinct_with_hash && grouping_is_hashable(aggref->aggdistinct))
				aggref->distinct_num = *(long *)context;
			else
				aggref->distinct_num = -1 * (*(long *)context);
		}
		return false;
	}

	return expression_tree_walker(node, set_distinct_aggrefs, context);
}

/*
 * Create a shared cte subplan for the append path in group optimization.
 */
static Path *
create_sharedctescan_subpath(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
							 Plan **subplan, List *rollups)
{
	Path *path = NULL;

	/* simple scan do not shared cte */
	if (subpath->pathtype == T_SeqScan || subpath->pathtype == T_TidScan ||
		subpath->pathtype == T_IndexScan || subpath->pathtype == T_IndexOnlyScan ||
		subpath->pathtype == T_BitmapHeapScan || subpath->pathtype == T_BitmapIndexScan)
	{
		path = subpath;
	}
	else
	{
		int exec_count = 0;
		ListCell   *lc = NULL;
		Cost inline_cost = 0;
		Cost material_cost = 0;
		int64 material_bytes = 0;

		foreach(lc, rollups)
		{
			RollupData *rollup = lfirst(lc);
			exec_count += list_length(rollup->gsets_data);
		}

		inline_cost = subpath->total_cost * exec_count;

		material_bytes = subpath->rows * subpath->pathtarget->width;
		material_cost = subpath->total_cost +
			((material_bytes / BLCKSZ + 1) * seq_page_cost) * (exec_count + 1);

		if (inline_cost <= material_cost)
		{
			path = subpath;
		}
		else
		{
			/* create shared cte subpath */
			Plan *cteplan;

			/*Use a temporary fake cteplan to create a sharectescan path.*/
			cteplan = makeNode(Plan);

			copy_generic_path_info(cteplan, subpath);
			*subplan = cteplan;

			path = create_sharedctescan_path(root, rel, subpath->pathtarget,
											 subpath->distribution, cteplan, 0);
		}
	}

	return path;
}

static List *
create_sort_group_pathkeys(List *groupClause, List  *group_pathkeys)
{
	ListCell *gc;
	ListCell *pk;
	List *pathkeys = NULL;
	
	foreach(gc, groupClause)
	{
		SortGroupClause * sortGroupClause = (SortGroupClause *)lfirst(gc);

		foreach(pk, group_pathkeys)
		{
			PathKey *pathkey = (PathKey *) lfirst(pk);
			EquivalenceClass *pk_eclass = pathkey->pk_eclass;

			if (pk_eclass->ec_sortref == sortGroupClause->tleSortGroupRef)
			{
				pathkeys = lappend(pathkeys, pathkey);
			}
		}
	}

	return pathkeys;
}

static Path *
create_grouping_append_path(PlannerInfo *root,
							RelOptInfo *rel,
							Path *subpath,
							PathTarget *target,
							List *having_qual,
							List *rollups,
							const AggClauseCosts *agg_costs,
							grouping_sets_data *gd,
							bool can_hash)
{
	int        nGsets = 0;
	int        nAggPlain = 0;
	ListCell   *lc = NULL;
	List	   *paths = NULL;
	Path       *path  = NULL;
	Path       *partial_path  = NULL;
	AppendPath *append_path = NULL;
	Query	   *parse = root->parse;
	Plan       *subplan = NULL;
	AggClauseCosts agg_partial_costs;	/* parallel only */
	AggClauseCosts agg_final_costs; /* parallel only */
	double         num_nodes;
	Path          *ctesubpath;

	/* group columns must be hashable */
	if (gd->unhashable_refs || subpath->param_info || rel->lateral_relids)
	{
		return NULL;
	}

	if (!(subpath->distribution &&
		  (subpath->distribution->distributionType == LOCATOR_TYPE_SHARD ||
		   subpath->distribution->distributionType == LOCATOR_TYPE_HASH)))
	{
		return NULL;
	}

	/* not support set-returning functions in tlist */
	if (root->hasRecursion || root->parse->hasTargetSRFs)
	{
		return NULL;
	}
	/* 
	 * must have group clause
	 * special case "group by grouping sets((),())" is not considered
	 */
	if (!root->parse->groupClause)
	{
		return NULL;
	}

	/* distinct clause and non-partial aggs not considered */
	if (agg_costs && (agg_costs->hasNonPartial || agg_costs->hasNonSerial))
	{
		return NULL;
	}

	/* params not considerd, such as parse/bind */
	if (root->glob->boundParams)
	{
		return NULL;
	}

	/* should not have subplan in subpath */
	foreach(lc, root->glob->subplans)
	{
		Plan *plan = (Plan *)lfirst(lc);

		if (plan && plan->is_subplan)
			return NULL;
	}

	/* TODO: support having quals without Var and Param */
	if (having_qual)
	{
		return NULL;
	}

	/*
	 * For append node added by group optimization, we need to check
	 * if there are any params contained in the target list. These params
	 * can be refered by append node. In that case, we can not use local send
	 * fragment, and get incorrect results with subplan marked execute_on_any.
	 */
	foreach(lc, target->exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		if (IsA(expr, Param))
			return NULL;
	}

	/*
	 * transform subplan into shared-cte if needed,
	 * subplan will execute only once
	 */
	ctesubpath = subpath;
	subpath = create_sharedctescan_subpath(root, rel, subpath, &subplan, rollups);
	num_nodes = path_count_datanodes(subpath);
	if (rel->consider_parallel && subplan && can_hash)
		partial_path =
			create_sharedctescan_partial_path(root, rel, subpath->pathtarget,
											  subpath->distribution, subplan);

	/*
	 * transform rollup/cube/grouping sets into aggregates, then append
	 */
	foreach(lc, rollups)
	{
		ListCell *gset_data = NULL;
		RollupData *rollup = lfirst(lc);

		/*
		 * create agg/group path for each group by
		 */
		foreach(gset_data, rollup->gsets_data)
		{
			int nAgg = 0;
			bool groupingFunc = false;
			List *groupClause = NULL;
			PathTarget *final_target = NULL;
			PathTarget *partial_target = NULL;
			GroupingSetData *g_set = lfirst(gset_data);
			Query *local_parse = NULL;
			List  *group_pathkeys = NULL;
			Path  *sortAgg_subpath = NULL;
			double dNumLocalGroups = g_set->numGroups;
			double dNumFinalGroup = ROWS_PER_DN(dNumLocalGroups);

			nGsets++;
			
			/* make group-by clause if any */
			groupClause = create_groupby_clause(g_set, rollup->groupClause);

			/* make targetlist for group-by */
			final_target = create_group_pathtarget(root, g_set, target, &nAgg, &groupingFunc, groupClause);
		
			if (groupClause || nAgg)
			{
				PathTarget *temp_partial_target = make_partial_grouping_target(root, final_target, (Node *) parse->havingQual);
				partial_target = copy_pathtarget(temp_partial_target);
				partial_target->exprs = copyObject(temp_partial_target->exprs);

				MemSet(&agg_partial_costs, 0, sizeof(AggClauseCosts));
				MemSet(&agg_final_costs, 0, sizeof(AggClauseCosts));
				if (parse->hasAggs)
				{
					/* partial phase */
					get_agg_clause_costs(root, (Node *) partial_target->exprs,
										 AGGSPLIT_INITIAL_SERIAL,
										 &agg_partial_costs);

					/* final phase */
					get_agg_clause_costs(root, (Node *) final_target->exprs,
										 AGGSPLIT_FINAL_DESERIAL,
										 &agg_final_costs);
					get_agg_clause_costs(root, parse->havingQual,
										 AGGSPLIT_FINAL_DESERIAL,
										 &agg_final_costs);
				}

				if (!can_hash)
				{
					sortAgg_subpath = subpath;

					if (groupClause)
					{
						group_pathkeys = create_sort_group_pathkeys(groupClause, root->group_pathkeys);

						if (list_length(group_pathkeys) != list_length(groupClause))
						{
							group_pathkeys = NULL;
						}

						sortAgg_subpath = (Path *) create_sort_path(root,
																	 rel,
																	 subpath,
																	 group_pathkeys,
																	 -1.0);
						if (!group_pathkeys)
						{
							SortPath   *sortpath = (SortPath *)sortAgg_subpath;
							sortpath->groupClause = groupClause;
						}
					}
				}
				
				if (parse->hasAggs)
				{
					/*
					 * We have aggregation, possibly with plain GROUP BY. Make
					 * an AggPath.
					 */
					if (can_hash)
					{
						path = (Path *) create_agg_path(root,
														rel,
														partial_path ? partial_path : subpath,
														partial_target,
														groupClause ? AGG_HASHED : AGG_PLAIN,
														AGGSPLIT_INITIAL_SERIAL,
														groupClause,
														NIL,
														&agg_partial_costs,
														dNumLocalGroups);
					}
					else
					{
						path = (Path *) create_agg_path(root,
														rel,
														sortAgg_subpath,
														partial_target,
														groupClause ? AGG_SORTED : AGG_PLAIN,
														AGGSPLIT_INITIAL_SERIAL,
														groupClause,
														NIL,
														&agg_partial_costs,
														dNumLocalGroups);
					}

					if (groupingFunc)
					{
						AggPath *agg = (AggPath *)path;
						agg->groupingFunc = true;
					}
				}
				else if (parse->groupClause)
				{
					if (can_hash)
					{
						path = (Path *) create_agg_path(root,
														rel,
														partial_path ? partial_path : subpath,
														partial_target,
														AGG_HASHED,
														AGGSPLIT_INITIAL_SERIAL,
														groupClause,
														NIL,
														&agg_partial_costs,
														dNumLocalGroups);

						if (groupingFunc)
						{
							AggPath *agg = (AggPath *)path;
							agg->groupingFunc = true;
						}
					}
					else
					{
						/*
						 * We have GROUP BY without aggregation or grouping sets.
						 * Make a GroupPath.
						 */
						path = (Path *) create_group_path(root,
														  rel,
														  sortAgg_subpath,
														  partial_target,
														  groupClause,
														  NIL,
														  dNumLocalGroups);
					}
				}
				else
				{
					elog(ERROR, "rollup without agg and group");
				}

				local_parse = copyObject(parse);
				local_parse->groupingSets = NULL;
				local_parse->groupClause = groupClause;

				path = create_redistribute_grouping_path(root, local_parse, path);
				if (partial_path)
					path->parallel_safe = false;

				if (parse->hasAggs)
				{
					if (can_hash)
					{
						path = (Path *) create_agg_path(root,
														rel,
														path,
														final_target,
														groupClause ? AGG_HASHED : AGG_PLAIN,
														AGGSPLIT_FINAL_DESERIAL,
														groupClause,
														(List *) having_qual,
														&agg_final_costs,
														dNumFinalGroup);
					}
					else
					{
						if (groupClause)
						{
							path = (Path *) create_sort_path(root,
															 rel,
															 path,
															 group_pathkeys,
															 -1.0);
							if (!group_pathkeys)
							{
								SortPath   *sortpath = (SortPath *)path;
								sortpath->groupClause = groupClause;
							}
						}
						
						path = (Path *)create_agg_path(root,
													   rel,
													   path,
													   final_target,
													   groupClause ? AGG_SORTED : AGG_PLAIN,
													   AGGSPLIT_FINAL_DESERIAL,
													   groupClause,
													   (List *) having_qual,
													   &agg_final_costs,
													   dNumFinalGroup);
					}

					if (groupingFunc)
					{
						AggPath *agg = (AggPath *)path;
						agg->groupingFunc = true;
					}
				}
				else if (parse->groupClause)
				{
					if (can_hash)
					{
						path = (Path *) create_agg_path(root,
														rel,
														path,
														final_target,
														AGG_HASHED,
														AGGSPLIT_FINAL_DESERIAL,
														groupClause,
														(List *) having_qual,
														&agg_final_costs,
														dNumFinalGroup);

						if (groupingFunc)
						{
							AggPath *agg = (AggPath *)path;
							agg->groupingFunc = true;
						}
					}
					else
					{
						path = (Path *) create_sort_path(root,
														 rel,
														 path,
														 group_pathkeys,
														 -1.0);
						
						if (!group_pathkeys)
						{
							SortPath   *sortpath = (SortPath *)path;
							sortpath->groupClause = groupClause;
						}

						path = (Path *) create_group_path(root,
														  rel,
														  path,
														  final_target,
														  groupClause,
														  (List *) having_qual,
														  dNumFinalGroup);
					}
				}
				else
				{
					elog(ERROR, "rollup without agg and group");
				}

				if (!groupClause && nAgg)
				{
					path->execute_on_any = true;
					nAggPlain++;
				}
			}
			else
			{
				if (!ORA_MODE && !groupingFunc)
				{
					path = (Path *)create_result_path(root, rel,
													  final_target,
													  NULL);
				}
				else
				{
					AggPath *agg;

					if (partial_path)
						path = (Path *) create_gather_path(root, rel, partial_path,
														   subpath->pathtarget, NULL, NULL);
					else
						path = subpath;

					if (ORA_MODE)
					{
						Node *limit_count;

						limit_count = (Node *) makeConst(FLOAT8OID, -1,
														 InvalidOid,
														 sizeof(int64),
														 Float8GetDatum((float8) 1),
														 false, FLOAT8PASSBYVAL);

						path = (Path *) create_limit_path(root, rel, path,
														  NULL,
														  limit_count,
														  LIMIT_OPTION_COUNT,
														  0, 1);

						path = create_remotesubplan_replicated_path(root, path);
					}

					path = (Path *) create_agg_path(root,
													rel,
													path,
													final_target,
													AGG_PLAIN,
													AGGSPLIT_SIMPLE,
													NULL,
													NIL,
													agg_costs,
													dNumLocalGroups);

					agg = (AggPath *)path;
					agg->groupingFunc = groupingFunc;
				}

				path->execute_on_any = true;
				nAggPlain++;
			}

			paths = lappend(paths, path);
		}
	}

	append_path = (AppendPath *) create_append_path(root,
													rel,
													paths,
													NIL,
													NULL,
													0,
													false,
													NIL,
													-1);
	append_path->path.pathtarget = target;
	append_path->subpaths = paths;
	append_path->grouping_append = true;

	if (subplan)
	{
		/* If partial path exists, just use it. */
		append_path->sharedPath = partial_path ? partial_path : subpath;
		append_path->sharedPlan = (void *) subplan;
		append_path->sharedCtePath = ctesubpath;
	}

	return (Path *)append_path;
}

static List *
get_partition_clauses(PathTarget *window_target, WindowClause *wc)
{
	ListCell *lc;
	List *result = NULL;
	int len = list_length(window_target->exprs);
	int nPartitionClauses = 0;
	int nMatch = 0;

	foreach(lc, wc->partitionClause)
	{
		int i = 0;
		SortGroupClause *sgclause = (SortGroupClause *)lfirst(lc);

		if (sgclause->tleSortGroupRef != 0 && sgclause->hashable)
		{
			nPartitionClauses++;

			for (i = 0; i < len; i++)
			{
				if (window_target->sortgrouprefs[i] == sgclause->tleSortGroupRef)
				{
					result = lappend(result, list_nth(window_target->exprs, i));
					nMatch++;
					break;
				}
			}
		}
	}

	if (nPartitionClauses != nMatch)
	{
		elog(ERROR, "could not match partition by clause, nPartitionClauses %d, "
			"nMatch %d", nPartitionClauses, nMatch);
	}

	return result;
}

/*
 * create_cn_expr_paths
 *      Create project and qual paths that must be deal at CN side.
 *      1. cn-udf epxr
 *      2. rownum expr (in the future)
 *      3. connect by special exprs
 */
static void
create_cn_expr_paths(PlannerInfo *root, RelOptInfo *current_rel, PathTarget *origin_target,
                     List *cn_process_targets, List *cn_process_targets_contain_srfs,
                     PathTarget *final_target)
{
	Path	   *path;
	ListCell   *lc;

	foreach(lc, current_rel->pathlist)
	{
		path = (Path *) lfirst(lc);

		/* must collect tuple to cn for further processing */
		if (path->distribution != NULL)
			path = create_remotesubplan_path(root, path, NULL);

		/* add rownum projection step */
		path = apply_projection_to_path(root, current_rel,
										path, origin_target);

		/* notice we must evaluate rownum expr first */
		if (current_rel->cn_qual != NIL)
        {
            path = (Path *) create_qual_path(root, path,
                                             current_rel->cn_qual);
            current_rel->partial_pathlist = NIL;
        }

		/* for udf cn-SRF, we need add ProjectSet node */
		if (root->parse->hasTargetSRFs && cn_process_targets != NULL && cn_process_targets_contain_srfs != NULL)
		{
			ListCell *lc1, *lc2;

			forboth(lc1, cn_process_targets, lc2, cn_process_targets_contain_srfs)
			{
				PathTarget *thistarget = (PathTarget *) lfirst(lc1);
				bool        contains_srfs = (bool) lfirst_int(lc2);

				/* If this level doesn't contain SRFs, do regular projection */
				if (contains_srfs)
					path = (Path *) create_set_projection_path(root, current_rel, path, thistarget);
				else
					path = (Path *) apply_projection_to_path(root, current_rel, path, thistarget);
			}
		}

		/* apply final target if no grouping and no post-pone projection */
		if (final_target != NULL)
			path = apply_projection_to_path(root, current_rel,
											path, final_target);

		lfirst(lc) = path;
	}

	set_cheapest(current_rel);
	/* we don't need it anymore */
	current_rel->cn_qual = NIL;
}
#endif
