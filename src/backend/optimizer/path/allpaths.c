/*-------------------------------------------------------------------------
 *
 * allpaths.c
 *	  Routines to find possible search paths for processing a query
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/allpaths.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "catalog/pg_namespace.h"
#include "access/sysattr.h"
#include "access/tsmapi.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#ifdef OPTIMIZER_DEBUG
#include "nodes/print.h"
#endif
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/inherit.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parsetree.h"
#include "pgxc/nodemgr.h"
#include "partitioning/partprune.h"
#include "utils/selfuncs.h"
#include "pgxc/nodemgr.h"
#ifdef PGXC
#include "nodes/makefuncs.h"
#include "miscadmin.h"
#endif /* PGXC */
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"
#ifdef __OPENTENBASE__
#include "optimizer/subselect.h"
#include "optimizer/distribution.h"
#endif

/* results of subquery_is_pushdown_safe */
typedef struct pushdown_safety_info
{
	bool	   *unsafeColumns;	/* which output columns are unsafe to use */
	bool		unsafeVolatile; /* don't push down volatile quals */
	bool		unsafeLeaky;	/* don't push down leaky quals */
	bool		is_pushpred;
} pushdown_safety_info;

/* These parameters are set by GUC */
bool		enable_geqo = false;	/* just in case GUC doesn't set it */
int			geqo_threshold;
int			min_parallel_table_scan_size;
int			min_parallel_index_scan_size;
int			max_global_index_scan_rows_size;
int 		parallel_dml_tuple_num_threshold;
int			planning_dml_sql;

/* Hook for plugins to get control in set_rel_pathlist() */
set_rel_pathlist_hook_type set_rel_pathlist_hook = NULL;
#ifdef __OPENTENBASE_C__
set_rel_size_hook_type set_rel_size_hook = NULL;
#endif
/* Hook for plugins to replace standard_join_search() */
join_search_hook_type join_search_hook = NULL;

set_distribution_hook_type set_distribution_hook = NULL;

static void set_base_rel_consider_startup(PlannerInfo *root);
static void set_base_rel_sizes(PlannerInfo *root);
static void set_base_rel_pathlists(PlannerInfo *root);
void set_rel_size(PlannerInfo *root, RelOptInfo *rel,
			 Index rti, RangeTblEntry *rte);
static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
				 Index rti, RangeTblEntry *rte);
static void set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel,
				   RangeTblEntry *rte);
static void create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel);
static void set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte);
static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
					   RangeTblEntry *rte);
static void set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel,
						 RangeTblEntry *rte);
static void set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 RangeTblEntry *rte);
static void set_foreign_size(PlannerInfo *root, RelOptInfo *rel,
				 RangeTblEntry *rte);
static void set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel,
					 RangeTblEntry *rte);
static void set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
					Index rti, RangeTblEntry *rte);
static void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, RangeTblEntry *rte);
static void generate_mergeappend_paths(PlannerInfo *root, RelOptInfo *rel,
						   List *live_childrels,
						   List *all_child_pathkeys,
						   List *partitioned_rels);
static Path *get_cheapest_parameterized_child_path(PlannerInfo *root,
									  RelOptInfo *rel,
									  Relids required_outer);
static void accumulate_append_subpath(Path *path, bool omit,
						  List **subpaths, List **special_subpaths);
static void set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte);
void set_subquery_param_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte);
static void search_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, Query *subquery);
static void set_function_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  RangeTblEntry *rte);
static void set_values_pathlist(PlannerInfo *root, RelOptInfo *rel,
					RangeTblEntry *rte);
static void set_tablefunc_pathlist(PlannerInfo *root, RelOptInfo *rel,
					   RangeTblEntry *rte);
static void set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel,
				 RangeTblEntry *rte);
static void set_namedtuplestore_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 RangeTblEntry *rte);
static void set_worktable_pathlist(PlannerInfo *root, RelOptInfo *rel,
					   RangeTblEntry *rte);
static RelOptInfo *make_rel_from_joinlist(PlannerInfo *root, List *joinlist);
static bool subquery_is_pushdown_safe(Query *subquery, Query *topquery,
						  pushdown_safety_info *safetyInfo);
static bool recurse_pushdown_safe(Node *setOp, Query *topquery,
					  pushdown_safety_info *safetyInfo);
static void check_output_expressions(Query *subquery,
						 pushdown_safety_info *safetyInfo);
static void compare_tlist_datatypes(List *tlist, List *colTypes,
						pushdown_safety_info *safetyInfo);
static bool targetIsInAllPartitionLists(TargetEntry *tle, Query *query);
static bool qual_is_pushdown_safe(Query *subquery, Index rti, Node *qual,
					  pushdown_safety_info *safetyInfo);
static void subquery_push_qual(Query *subquery,
				   RangeTblEntry *rte, Index rti, Node *qual, int levelsup);
static void recurse_push_qual(Node *setOp, Query *topquery,
				  RangeTblEntry *rte, Index rti, Node *qual, int filter);
static void remove_unused_subquery_outputs(Query *subquery, RelOptInfo *rel);
static bool check_list_contain_all_const(List *list);
static void set_eqclass_information_from_subquery(PlannerInfo *root);
static void bubble_up_partitions(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);

/*
 * make_one_rel
 *	  Finds all possible access paths for executing a query, returning a
 *	  single rel that represents the join of all base rels in the query.
 */
RelOptInfo *
make_one_rel(PlannerInfo *root, List *joinlist)
{
	RelOptInfo *rel;
	Index		rti;
	double		total_pages;

	/*
	 * Construct the all_baserels Relids set.
	 */
	root->all_baserels = NULL;
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (brel == NULL)
			continue;

		Assert(brel->relid == rti); /* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (brel->reloptkind != RELOPT_BASEREL)
			continue;

		root->all_baserels = bms_add_member(root->all_baserels, brel->relid);
	}

	/* Mark base rels as to whether we care about fast-start plans */
	set_base_rel_consider_startup(root);

	/*
	 * Compute size estimates and consider_parallel flags for each base rel.
	 */
	set_base_rel_sizes(root);

	/*
	 * We should now have size estimates for every actual table involved in
	 * the query, and we also know which if any have been deleted from the
	 * query by join removal, pruned by partition pruning, or eliminated by
	 * constraint exclusion.  So we can now compute total_table_pages.
	 *
	 * Note that appendrels are not double-counted here, even though we don't
	 * bother to distinguish RelOptInfos for appendrel parents, because the
	 * parents will have pages = 0.
	 *
	 * XXX if a table is self-joined, we will count it once per appearance,
	 * which perhaps is the wrong thing ... but that's not completely clear,
	 * and detecting self-joins here is difficult, so ignore it for now.
	 */
	total_pages = 0;
	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		if (brel == NULL)
			continue;

		Assert(brel->relid == rti); /* sanity check on array */

		if (IS_DUMMY_REL(brel))
			continue;

		if (IS_SIMPLE_REL(brel))
			total_pages += (double) brel->pages;
	}
	root->total_table_pages = total_pages;

	/*
	 * Generate access paths for each base rel.
	 */
	set_base_rel_pathlists(root);

	/*
	 * Set equal class for const from subquery
	 */
	set_eqclass_information_from_subquery(root);
	/*
	 * Generate access paths for the entire join tree.
	 */
	rel = make_rel_from_joinlist(root, joinlist);

	/*
	 * The result should join all and only the query's base rels.
	 */
	Assert(bms_equal(rel->relids, root->all_baserels));

	return rel;
}

/*
 * set_base_rel_consider_startup
 *	  Set the consider_[param_]startup flags for each base-relation entry.
 *
 * For the moment, we only deal with consider_param_startup here; because the
 * logic for consider_startup is pretty trivial and is the same for every base
 * relation, we just let build_simple_rel() initialize that flag correctly to
 * start with.  If that logic ever gets more complicated it would probably
 * be better to move it here.
 */
static void
set_base_rel_consider_startup(PlannerInfo *root)
{
	/*
	 * Since parameterized paths can only be used on the inside of a nestloop
	 * join plan, there is usually little value in considering fast-start
	 * plans for them.  However, for relations that are on the RHS of a SEMI
	 * or ANTI join, a fast-start plan can be useful because we're only going
	 * to care about fetching one tuple anyway.
	 *
	 * To minimize growth of planning time, we currently restrict this to
	 * cases where the RHS is a single base relation, not a join; there is no
	 * provision for consider_param_startup to get set at all on joinrels.
	 * Also we don't worry about appendrels.  costsize.c's costing rules for
	 * nestloop semi/antijoins don't consider such cases either.
	 */
	ListCell   *lc;

	foreach(lc, root->join_info_list)
	{
		SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);
		int			varno;

		if ((sjinfo->jointype == JOIN_SEMI || sjinfo->jointype == JOIN_ANTI) &&
			bms_get_singleton_member(sjinfo->syn_righthand, &varno))
		{
			RelOptInfo *rel = find_base_rel(root, varno);

			rel->consider_param_startup = true;
		}
	}
}

/*
 * set_base_rel_sizes
 *	  Set the size estimates (rows and widths) for each base-relation entry.
 *	  Also determine whether to consider parallel paths for base relations.
 *
 * We do this in a separate pass over the base rels so that rowcount
 * estimates are available for parameterized path generation, and also so
 * that each rel's consider_parallel flag is set correctly before we begin to
 * generate paths.
 */
static void
set_base_rel_sizes(PlannerInfo *root)
{
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte;

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (rel == NULL)
			continue;

		Assert(rel->relid == rti);	/* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (rel->reloptkind != RELOPT_BASEREL)
			continue;

		rte = root->simple_rte_array[rti];

		/*
		 * If parallelism is allowable for this query in general, see whether
		 * it's allowable for this rel in particular.  We have to do this
		 * before set_rel_size(), because (a) if this rel is an inheritance
		 * parent, set_append_rel_size() will use and perhaps change the rel's
		 * consider_parallel flag, and (b) for some RTE types, set_rel_size()
		 * goes ahead and makes paths immediately.
		 */
		if (root->glob->parallelModeOK)
		{
			set_rel_consider_parallel(root, rel, rte);
		}

#ifdef __OPENTENBASE_C__
		if (set_rel_size_hook != NULL)
		{
			(*set_rel_size_hook)(root, rel, rti, rte);
		}
		else
		{
#endif
			set_rel_size(root, rel, rti, rte);
#ifdef __OPENTENBASE_C__
		}
#endif
	}
}

/*
 * contains_pushdown_unique_path
 *	Check whether there is already add the distinct path.
 */
static bool
contains_pushdown_unique_path(Path *path, PlannerInfo *root)
{
	if (path == NULL)
		return false;

	if (IsA(path, UniquePath) &&
		try_unique_path_exprs(root, path) != NULL)
	{
		return true;
	}

	return path_tree_walker(path, contains_pushdown_unique_path, root);
}

/*
 * is_try_pushdown_unique_path
 *	Check whether it is ready to push distinct down to base or join path.
 */
static bool
is_try_pushdown_unique_path(Path *path, PlannerInfo *root)
{
	if (try_unique_path_exprs(root, path) &&
		!contains_pushdown_unique_path(path, root))
	{
		return true;
	}

	return false;
}

/*
 * set_cheapest_unique_path
 *	Add distinct path for reduce the intermediate result set.
 */
void
set_cheapest_unique_path(PlannerInfo *root, RelOptInfo *rel)
{
	Path		*path;
	ListCell	*lc;

	if (root->parse->distinctClause == NULL ||
		list_length(root->parse->rtable) <= 1 ||
		distinct_pushdown_factor == 0)
		return;

	if (rel->cheapest_total_path &&
		is_try_pushdown_unique_path(rel->cheapest_total_path, root))
	{
		path = (Path*) try_add_unique_path(root, rel, rel->cheapest_total_path);

		if (path != NULL)
		{
			rel->cheapest_total_path = path;
			rel->cheapest_unique_path = path;
		}

		path = (Path*) try_add_unique_path(root, rel, rel->cheapest_startup_path);

		if (path != NULL)
		{
			rel->cheapest_startup_path = path;

			if (rel->cheapest_unique_path == NULL)
				rel->cheapest_unique_path = path;
		}
	}

	foreach(lc, rel->cheapest_parameterized_paths)
	{
		path = (Path*) lfirst(lc);

		if (path && is_try_pushdown_unique_path(path, root))
		{
			path = (Path*) try_add_unique_path(root, rel, lfirst(lc));

			if (path != NULL)
			{
				lfirst(lc) = path;
			}
		}	
	}

	return;
}

/*
 * set_base_rel_pathlists
 *	  Finds all paths available for scanning each base-relation entry.
 *	  Sequential scan and any available indices are considered.
 *	  Each useful path is attached to its relation's 'pathlist' field.
 */
static void
set_base_rel_pathlists(PlannerInfo *root)
{
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];

		/* there may be empty slots corresponding to non-baserel RTEs */
		if (rel == NULL)
			continue;

		Assert(rel->relid == rti);	/* sanity check on array */

		/* ignore RTEs that are "other rels" */
		if (rel->reloptkind != RELOPT_BASEREL)
			continue;

		if (enable_partition_iterator && rel->reloptkind == RELOPT_OTHER_MEMBER_REL &&
		    rel->needPaths == false)
		{
			continue;
		}
		
		set_rel_pathlist(root, rel, rti, root->simple_rte_array[rti]);

		set_cheapest_unique_path(root, rel);
	}
}

/*
 * set_rel_size
 *	  Set size estimates for a base relation
 */
void
set_rel_size(PlannerInfo *root, RelOptInfo *rel,
			 Index rti, RangeTblEntry *rte)
{
	bool isMergeResultRel = (root->parse->commandType == CMD_MERGE &&
							 rel->relid == root->parse->resultRelation &&
							 rel->part_scheme);
	List *origin_restrictinfo = NIL;

	if (rel->reloptkind == RELOPT_BASEREL &&
		relation_excluded_by_constraints(root, rel, rte))
	{
		/*
		 * We proved we don't need to scan the rel via constraint exclusion,
		 * so set up a single dummy path for it.  Here we only check this for
		 * regular baserels; if it's an otherrel, CE was already checked in
		 * set_append_rel_size().
		 *
		 * In this case, we go ahead and set up the relation's path right away
		 * instead of leaving it for set_rel_pathlist to do.  This is because
		 * we don't have a convention for marking a rel as dummy except by
		 * assigning a dummy path to it.
		 */
		set_dummy_rel_pathlist(rel);
	}
	else if (rte->inh || isMergeResultRel)
	{
		/* It's an "append relation", process accordingly */
		set_append_rel_size(root, rel, rti, rte);
	}
	else
	{
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_size(root, rel, rte);
				}
				else if (rte->relkind == RELKIND_PARTITIONED_TABLE)
				{
					/*
					 * We could get here if asked to scan a partitioned table
					 * with ONLY.  In that case we shouldn't scan any of the
					 * partitions, so mark it as a dummy rel.
					 */
					set_dummy_rel_pathlist(rel);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_size(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_size(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:

				/*
				 * Subqueries don't support making a choice between
				 * parameterized and unparameterized paths, so just go ahead
				 * and build their paths immediately.
				 */
				if (enable_push_pred && IS_CENTRALIZED_DATANODE)
					origin_restrictinfo = (List *)copyObject(rel->baserestrictinfo);

				set_subquery_pathlist(root, rel, rti, rte);

				if (enable_push_pred && IS_CENTRALIZED_DATANODE)
				{
					List * modified_restrictinfo = rel->baserestrictinfo;
					rel->baserestrictinfo = origin_restrictinfo;
					set_subquery_param_pathlist(root, rel, rti, rte);
					rel->baserestrictinfo = modified_restrictinfo;
				}
				break;
			case RTE_FUNCTION:
				set_function_size_estimates(root, rel);
				break;
			case RTE_TABLEFUNC:
				set_tablefunc_size_estimates(root, rel);
				break;
			case RTE_VALUES:
				set_values_size_estimates(root, rel);
				break;
			case RTE_CTE:

				/*
				 * CTEs don't support making a choice between parameterized
				 * and unparameterized paths, so just go ahead and build their
				 * paths immediately.
				 */
				if (rte->self_reference)
					set_worktable_pathlist(root, rel, rte);
				else
					set_cte_pathlist(root, rel, rte);
				break;
			case RTE_NAMEDTUPLESTORE:
				set_namedtuplestore_pathlist(root, rel, rte);
				break;
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * We insist that all non-dummy rels have a nonzero rowcount estimate.
	 */
	Assert(rel->rows > 0 || IS_DUMMY_REL(rel));
}

/*
 * set_rel_pathlist
 *	  Build access paths for a base relation
 */
static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
				 Index rti, RangeTblEntry *rte)
{
	PlannerInfo *top_root = root;

	while (top_root->parent_root)
	{
		top_root = top_root->parent_root;
	}

	if (IS_DUMMY_REL(rel))
	{
		/* We already proved the relation empty, so nothing more to do */
	}
	else if (top_root->parse->commandType == CMD_MERGE && rel->rtekind == RTE_RELATION &&
	         rel->part_scheme && rel->relid == top_root->parse->resultRelation &&
	         !CAN_PARTITERATOR(rel, rte))
	{
		/*
		 * It's an "append relation". In 'MERGE' CMD,the partition table
		 * could be both result relation and scan base relation. We did not
		 * set 'inh' because it will produce parallel subquery against each
		 * child result relation, leading join not fully executed on all
		 * tuples.
		 * Thus here we need to perform the additional checking to make sure
		 * we create append plan when scanning the MERGE INTO table.
		 */
		set_append_rel_pathlist(root, rel, rti, rte);
	}
	else if (rte->inh)
	{
		if (CAN_PARTITERATOR(rel, rte))
		{
			bubble_up_partitions(root, rel, rti, rte);
			/* Plain relation */
			set_plain_rel_pathlist(root, rel, rte);
		}
		else
		{
			/* It's an "append relation", process accordingly */
			set_append_rel_pathlist(root, rel, rti, rte);
		}
	}
	else
	{
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_pathlist(root, rel, rte);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_pathlist(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_pathlist(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:
				/* Subquery --- fully handled during set_rel_size */
				break;
			case RTE_FUNCTION:
				/* RangeFunction */
				set_function_pathlist(root, rel, rte);
				break;
			case RTE_TABLEFUNC:
				/* Table Function */
				set_tablefunc_pathlist(root, rel, rte);
				break;
			case RTE_VALUES:
				/* Values list */
				set_values_pathlist(root, rel, rte);
				break;
			case RTE_CTE:
				/* CTE reference --- fully handled during set_rel_size */
				break;
			case RTE_NAMEDTUPLESTORE:
				/* tuplestore reference --- fully handled during set_rel_size */
				break;
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * If this is a baserel, we should normally consider gathering any partial
	 * paths we may have created for it.
	 *
	 * However, if this is an inheritance child, skip it.  Otherwise, we could
	 * end up with a very large number of gather nodes, each trying to grab
	 * its own pool of workers.  Instead, we'll consider gathering partial
	 * paths for the parent appendrel.
	 *
	 * Also, if this is the topmost scan/join rel (that is, the only baserel),
	 * we postpone this until the final scan/join targelist is available (see
	 * grouping_planner).
	 */
	if (rel->reloptkind == RELOPT_BASEREL &&
		bms_membership(root->all_baserels) != BMS_SINGLETON)
		generate_gather_paths(root, rel, false);

	/*
	 * Allow a plugin to editorialize on the set of Paths for this base
	 * relation.  It could add new paths (such as CustomPaths) by calling
	 * add_path(), or delete or modify paths added by the core code.
	 */
	if (set_rel_pathlist_hook)
		(*set_rel_pathlist_hook) (root, rel, rti, rte);

	/* Now find the cheapest of the paths for this rel */
	set_cheapest(rel);

#ifdef OPTIMIZER_DEBUG
	debug_print_rel(root, rel);
#endif
}

/*
 * set_plain_rel_size
 *	  Set size estimates for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}


/*
 * If this relation could possibly be scanned from within a worker, then set
 * its consider_parallel flag.
 */
static void
set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte)
{
	/*
	 * The flag has previously been initialized to false, so we can just
	 * return if it becomes clear that we can't safely set it.
	 */
	Assert(!rel->consider_parallel);

	/* Don't call this if parallelism is disallowed for the entire query. */
	Assert(root->glob->parallelModeOK);

	/* This should only be called for baserels and appendrel children. */
	Assert(IS_SIMPLE_REL(rel));

	/* Assorted checks based on rtekind. */
	switch (rte->rtekind)
	{
		case RTE_RELATION:
			/*
			 * Table sampling can be pushed down to workers if the sample
			 * function and its arguments are safe.
			 */
			if (rte->tablesample != NULL)
			{
				char		proparallel = func_parallel(rte->tablesample->tsmhandler);

				if (proparallel != PROPARALLEL_SAFE)
					return;
				if (!is_parallel_safe(root, (Node *) rte->tablesample->args))
					return;
			}

			/*
			 * Ask FDWs whether they can support performing a ForeignScan
			 * within a worker.  Most often, the answer will be no.  For
			 * example, if the nature of the FDW is such that it opens a TCP
			 * connection with a remote server, each parallel worker would end
			 * up with a separate connection, and these connections might not
			 * be appropriately coordinated between workers and the leader.
			 */
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				Assert(rel->fdwroutine);
				if (!rel->fdwroutine->IsForeignScanParallelSafe)
					return;
				if (!rel->fdwroutine->IsForeignScanParallelSafe(root, rel, rte))
					return;
			}

			/*
			 * There are additional considerations for appendrels, which we'll
			 * deal with in set_append_rel_size and set_append_rel_pathlist.
			 * For now, just set consider_parallel based on the rel's own
			 * quals and targetlist.
			 */
			break;

		case RTE_SUBQUERY:

			/*
			 * There's no intrinsic problem with scanning a subquery-in-FROM
			 * (as distinct from a SubPlan or InitPlan) in a parallel worker.
			 * If the subquery doesn't happen to have any parallel-safe paths,
			 * then flagging it as consider_parallel won't change anything,
			 * but that's true for plain tables, too.  We must set
			 * consider_parallel based on the rel's own quals and targetlist,
			 * so that if a subquery path is parallel-safe but the quals and
			 * projection we're sticking onto it are not, we correctly mark
			 * the SubqueryScanPath as not parallel-safe.  (Note that
			 * set_subquery_pathlist() might push some of these quals down
			 * into the subquery itself, but that doesn't change anything.)
			 */
			break;

		case RTE_JOIN:
			/* Shouldn't happen; we're only considering baserels here. */
			Assert(false);
			return;

		case RTE_FUNCTION:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->functions))
				return;
			break;

		case RTE_TABLEFUNC:
			/* not parallel safe */
			return;

		case RTE_VALUES:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->values_lists))
				return;
			break;

		case RTE_CTE:

			/*
			 * CTE tuplestores aren't shared among parallel workers, so we
			 * force all CTE scans to happen in the leader.  Also, populating
			 * the CTE would require executing a subplan that's not available
			 * in the worker, might be parallel-restricted, and must get
			 * executed only once.
			 */
			return;

		case RTE_NAMEDTUPLESTORE:

			/*
			 * tuplestore cannot be shared, at least without more
			 * infrastructure to support that.
			 */
			return;

		case RTE_REMOTE_DUMMY:
			return;
	}

	/*
	 * If there's anything in baserestrictinfo that's parallel-restricted, we
	 * give up on parallelizing access to this relation.  We could consider
	 * instead postponing application of the restricted quals until we're
	 * above all the parallelism in the plan tree, but it's not clear that
	 * that would be a win in very many cases, and it might be tricky to make
	 * outer join clauses work correctly.  It would likely break equivalence
	 * classes, too.
	 */
	if (!is_parallel_safe(root, (Node *) rel->baserestrictinfo))
		return;

	/*
	 * Likewise, if the relation's outputs are not parallel-safe, give up.
	 * (Usually, they're just Vars, but sometimes they're not.)
	 */
	if (!is_parallel_safe(root, (Node *) rel->reltarget->exprs))
		return;

	/* We have a winner. */
	rel->consider_parallel = true;
}

static Path *
create_partiterator_path(PlannerInfo *root, RelOptInfo *rel, Path *path, int parallel_workers,
                         bool parallel_aware, List *partitioned_rels)
{
	Path *result = NULL;

	switch (path->pathtype)
	{
		case T_SeqScan:
		case T_BitmapHeapScan:
		case T_BitmapIndexScan:
		case T_TidScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		{
			PartIteratorPath *itrpath = makeNode(PartIteratorPath);

			itrpath->subPath = path;
			itrpath->path.pathtype = T_PartIterator;
			itrpath->path.parent = rel;
			itrpath->path.pathtarget = rel->reltarget;
			itrpath->path.param_info = path->param_info;
			itrpath->path.pathkeys = NIL;
			itrpath->path.distribution = copyObject(path->distribution);
			itrpath->leafNum = list_length(rel->partition_leaf_rels);
			itrpath->path.parallel_aware = parallel_aware;
			itrpath->path.parallel_safe = path->parallel_safe;
			itrpath->path.pathkeys = NIL; /* result is always considered unsorted, range
			                                 partitioning will be optimized later. */

			/* scan parttition from lower boundary to upper boundary by default */
			itrpath->direction = ForwardScanDirection;
			itrpath->partitioned_rels = list_copy(partitioned_rels);

			if (parallel_aware)
			{
				int pworkers = parallel_workers;

				pworkers =
					Max(pworkers, fls(list_length(rel->partition_leaf_rels)));
				pworkers = Min(pworkers, max_parallel_workers_per_gather);
				itrpath->path.parallel_workers = pworkers;
			}

			cost_partiterator(itrpath);

			result = (Path *) itrpath;
		}
		break;
		default:
		{
			ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
			                errmsg("unrecognized node type when create partiterator path: %d",
			                       (int) path->pathtype)));
		}
		break;
	}

	return result;
}

/*
 * try to add control operator PartIterator over scan operator, so we can
 * scan all selected partitions
 */
static void
try_add_partiterator(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	ListCell *pathCell = NULL;

	if (rte->relkind != RELKIND_PARTITIONED_TABLE)
	{
		/* do nothing for non-partitioned table */
		return;
	}

	foreach (pathCell, rel->pathlist)
	{
		Path     *path = (Path *) lfirst(pathCell);
		Path     *itrPath = NULL;
		ListCell *cpPathCell = NULL;

		itrPath = create_partiterator_path(root, rel, path, 0, false,
		                                   list_make1(rel->partitioned_child_rels));

		/* replace entry in pathlist */
		lfirst(pathCell) = itrPath;

		if (path == rel->cheapest_startup_path)
		{
			/* replace cheapest_startup_path */
			rel->cheapest_startup_path = itrPath;
		}

		if (path == rel->cheapest_unique_path)
		{
			/* replace cheapest_unique_path */
			rel->cheapest_unique_path = itrPath;
		}

		if (path == rel->cheapest_total_path)
		{
			/* replace cheapest_total_path */
			rel->cheapest_total_path = itrPath;
		}

		/* replace entry in cheapest_parameterized_paths */
		foreach (cpPathCell, rel->cheapest_parameterized_paths)
		{
			/* we add cheapest total into cheapest_parameterized_paths in set_cheapest */
			if (lfirst(cpPathCell) == path)
			{
				lfirst(cpPathCell) = itrPath;
				break;
			}
		}
	}

	foreach (pathCell, rel->partial_pathlist)
	{
		Path     *path = (Path *) lfirst(pathCell);
		Path     *itrPath = NULL;

		itrPath = create_partiterator_path(root, rel, path, path->parallel_workers, true,
		                                   list_make1(rel->partitioned_child_rels));

		/* replace entry in pathlist */
		lfirst(pathCell) = itrPath;
	}

}

/*
 * set_plain_rel_pathlist
 *	  Build access paths for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
#ifdef __OPENTENBASE_C__
	ListCell	*cell;
#endif

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
	add_path(rel, create_seqscan_path(root, rel, required_outer, 0));

#ifdef __OPENTENBASE_C__
	if (rel->consider_latematerial && rel->baserel_attr_masks)
	{
		/* Consider late materialized sequential scan */
		foreach(cell, rel->baserel_attr_masks)
		{
			Bitmapset *attr_mask = lfirst(cell);

			/* Adding to separate pathlist for unmaterialized paths */
			Path *unmaterial_path = create_seqscan_lm_path(root, rel, required_outer, 0, attr_mask);
			add_unmaterial_path(rel, unmaterial_path);
		}
	}
#endif

	/* If appropriate, consider parallel sequential scan */
	if (rel->consider_parallel && required_outer == NULL)
		create_plain_partial_paths(root, rel);

	/* Consider index scans */
	create_index_paths(root, rel);

	/* Consider TID scans */
	create_tidscan_paths(root, rel);
	if (CAN_PARTITERATOR(rel, rte) && rel->needPaths == true)
	{
		PlannerInfo *top_root = root;

		while (top_root->parent_root)
		{
			top_root = top_root->parent_root;
		}

		try_add_partiterator(root, rel, rte);
	}
}

/*
 * create_plain_partial_paths
 *	  Build partial access paths for parallel scan of a plain relation
 */
static void
create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int			parallel_workers;
#ifdef __OPENTENBASE_C__
	ListCell	*cell;
#endif

	parallel_workers = compute_parallel_worker(rel, rel->pages, -1);

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	if (parallel_workers <= 0)
		return;

	/* Add an unordered partial path based on a parallel sequential scan. */
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, parallel_workers));

#ifdef __OPENTENBASE_C__
	if (rel->consider_latematerial && rel->baserel_attr_masks)
	{
		/* Consider late materialized sequential scan in parallel mode */
		foreach(cell, rel->baserel_attr_masks)
		{
			Bitmapset *mask = lfirst(cell);

			/* Adding to separate pathlist for parallel unmaterialized paths */
			Path *unmaterial_path = create_seqscan_lm_path(root, rel, NULL, parallel_workers, mask);
			add_unmaterial_partial_path(rel, unmaterial_path);
		}
	}
#endif
}

/*
 * set_tablesample_rel_size
 *	  Set size estimates for a sampled relation
 */
static void
set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	TableSampleClause *tsc = rte->tablesample;
	TsmRoutine *tsm;
	BlockNumber pages;
	double		tuples;

	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/*
	 * Call the sampling method's estimation function to estimate the number
	 * of pages it will read and the number of tuples it will return.  (Note:
	 * we assume the function returns sane values.)
	 */
	tsm = GetTsmRoutine(tsc->tsmhandler);
	tsm->SampleScanGetSampleSize(root, rel, tsc->args,
								 &pages, &tuples);

	/*
	 * For the moment, because we will only consider a SampleScan path for the
	 * rel, it's okay to just overwrite the pages and tuples estimates for the
	 * whole relation.  If we ever consider multiple path types for sampled
	 * rels, we'll need more complication.
	 */
	rel->pages = pages;
	rel->tuples = tuples;

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/*
 * set_tablesample_rel_pathlist
 *	  Build access paths for a sampled relation
 */
static void
set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	Path	   *path;

	/*
	 * We don't support pushing join clauses into the quals of a samplescan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in its tlist or TABLESAMPLE arguments.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sampled scan */
	path = create_samplescan_path(root, rel, required_outer);

	/*
	 * If the sampling method does not support repeatable scans, we must avoid
	 * plans that would scan the rel multiple times.  Ideally, we'd simply
	 * avoid putting the rel on the inside of a nestloop join; but adding such
	 * a consideration to the planner seems like a great deal of complication
	 * to support an uncommon usage of second-rate sampling methods.  Instead,
	 * if there is a risk that the query might perform an unsafe join, just
	 * wrap the SampleScan in a Materialize node.  We can check for joins by
	 * counting the membership of all_baserels (note that this correctly
	 * counts inheritance trees as single rels).  If we're inside a subquery,
	 * we can't easily check whether a join might occur in the outer query, so
	 * just assume one is possible.
	 *
	 * GetTsmRoutine is relatively expensive compared to the other tests here,
	 * so check repeatable_across_scans last, even though that's a bit odd.
	 */
	if ((root->query_level > 1 ||
		 bms_membership(root->all_baserels) != BMS_SINGLETON) &&
		!(GetTsmRoutine(rte->tablesample->tsmhandler)->repeatable_across_scans))
	{
		path = (Path *) create_material_path(rel, path);
	}

	add_path(rel, path);

	/* For the moment, at least, there are no other paths to consider */
}

/*
 * set_foreign_size
 *		Set size estimates for a foreign table RTE
 */
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Mark rel with estimated output rows, width, etc */
	set_foreign_size_estimates(root, rel);

	/* Let FDW adjust the size estimates, if it can */
	rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

	/* ... but do not let it set the rows estimate to zero */
	rel->rows = clamp_row_est(rel->rows);

	/*
	 * Also, make sure rel->tuples is not insane relative to rel->rows.
	 * Notably, this ensures sanity if pg_class.reltuples contains -1 and the
	 * FDW doesn't do anything to replace that.
	 */
	rel->tuples = Max(rel->tuples, rel->rows);
}

/*
 * set_foreign_pathlist
 *		Build access paths for a foreign table RTE
 */
static void
set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Call the FDW's GetForeignPaths function to generate path(s) */
	rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
}

/*
 * set_append_rel_size
 *	  Set size estimates for a simple "append relation"
 *
 * The passed-in rel and RTE represent the entire append relation.  The
 * relation's contents are computed by appending together the output of the
 * individual member relations.  Note that in the non-partitioned inheritance
 * case, the first member relation is actually the same table as is mentioned
 * in the parent RTE ... but it has a different RTE and RelOptInfo.  This is
 * a good thing because their outputs are not the same size.
 */
static void
set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
					Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	bool		has_live_children;
	double		parent_rows;
	double		parent_size;
	double	   *parent_attrsizes;
	int			nattrs;
	ListCell   *l;

	/* Guard against stack overflow due to overly deep inheritance tree. */
	check_stack_depth();
	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);
	
	Assert(IS_SIMPLE_REL(rel));

	rel->baserestrictcost.per_tuple = 0.0;
	rel->baserestrictcost.startup = 0.0;
	/*
	 * Initialize partitioned_child_rels to contain this RT index.
	 *
	 * Note that during the set_append_rel_pathlist() phase, we will bubble up
	 * the indexes of partitioned relations that appear down in the tree, so
	 * that when we've created Paths for all the children, the root
	 * partitioned table's list will contain all such indexes.
	 */
	if (rte->relkind == RELKIND_PARTITIONED_TABLE)
		rel->partitioned_child_rels = list_make1_int(rti);

	/*
	 * If this is a partitioned baserel, set the consider_partitionwise_join
	 * flag; currently, we only consider partitionwise joins with the baserel
	 * if its targetlist doesn't contain a whole-row Var.
	 */
	if (enable_partitionwise_join &&
		rel->reloptkind == RELOPT_BASEREL &&
		rte->relkind == RELKIND_PARTITIONED_TABLE &&
		rel->attr_needed[InvalidAttrNumber - rel->min_attr] == NULL)
		rel->consider_partitionwise_join = true;

	/*
	 * Initialize to compute size estimates for whole append relation.
	 *
	 * We handle width estimates by weighting the widths of different child
	 * rels proportionally to their number of rows.  This is sensible because
	 * the use of width estimates is mainly to compute the total relation
	 * "footprint" if we have to sort or hash it.  To do this, we sum the
	 * total equivalent size (in "double" arithmetic) and then divide by the
	 * total rowcount estimate.  This is done separately for the total rel
	 * width and each attribute.
	 *
	 * Note: if you consider changing this logic, beware that child rels could
	 * have zero rows and/or width, if they were excluded by constraints.
	 */
	has_live_children = false;
	parent_rows = 0;
	parent_size = 0;
	nattrs = rel->max_attr - rel->min_attr + 1;
	parent_attrsizes = (double *) palloc0(nattrs * sizeof(double));

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;
		ListCell   *parentvars;
		ListCell   *childvars;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];

		/*
		 * The child rel's RelOptInfo was already created during
		 * add_other_rels_to_query.
		 */
		childrel = find_base_rel(root, childRTindex);
		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);

		/*
		 * Copy/Modify targetlist. Even if this child is deemed empty, we need
		 * its targetlist in case it falls on nullable side in a child-join
		 * because of partitionwise join.
		 *
		 * NB: the resulting childrel->reltarget->exprs may contain arbitrary
		 * expressions, which otherwise would not occur in a rel's targetlist.
		 * Code that might be looking at an appendrel child must cope with
		 * such.  (Normally, a rel's targetlist would only include Vars and
		 * PlaceHolderVars.)  XXX we do not bother to update the cost or width
		 * fields of childrel->reltarget; not clear if that would be useful.
		 */
		childrel->reltarget->exprs = (List *)
			adjust_appendrel_attrs(root,
								   (Node *) rel->reltarget->exprs,
								   1, &appinfo);

		/*
		 * We have to make child entries in the EquivalenceClass data
		 * structures as well.  This is needed either if the parent
		 * participates in some eclass joins (because we will want to consider
		 * inner-indexscan joins on the individual children) or if the parent
		 * has useful pathkeys (because we should try to build MergeAppend
		 * paths that produce those sort orderings). Even if this child is
		 * deemed dummy, it may fall on nullable side in a child-join, which
		 * in turn may participate in a MergeAppend, where we will need the
		 * EquivalenceClass data structures.
		 */
		if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
			add_child_rel_equivalences(root, appinfo, rel, childrel);
		childrel->has_eclass_joins = rel->has_eclass_joins;

		/* We may have already proven the child to be dummy. */
		if (IS_DUMMY_REL(childrel))
				continue;

		/*
		 * We have to copy the parent's targetlist and quals to the child,
		 * with appropriate substitution of variables.  However, the
		 * baserestrictinfo quals were already copied/substituted when the
		 * child RelOptInfo was built.  So we don't need any additional setup
		 * before applying constraint exclusion.
		 */
		if (relation_excluded_by_constraints(root, childrel, childRTE))
		{
			/*
			 * This child need not be scanned, so we can omit it from the
			 * appendrel.
			 */
			set_dummy_rel_pathlist(childrel);
			continue;
		}

		/* CE failed, so finish copying/modifying join quals. */
		childrel->joininfo = (List *)
			adjust_appendrel_attrs(root,
								   (Node *) rel->joininfo,
								   1, &appinfo);

		/*
		 * Note: we could compute appropriate attr_needed data for the child's
		 * variables, by transforming the parent's attr_needed through the
		 * translated_vars mapping.  However, currently there's no need
		 * because attr_needed is only examined for base relations not
		 * otherrels.  So we just leave the child's attr_needed empty.
		 */

		/*
		 * If we consider partitionwise joins with the parent rel, do the same
		 * for partitioned child rels.
		 */
		if (rel->consider_partitionwise_join &&
			childRTE->relkind == RELKIND_PARTITIONED_TABLE)
			childrel->consider_partitionwise_join = true;

		/*
		 * If parallelism is allowable for this query in general, see whether
		 * it's allowable for this childrel in particular.  But if we've
		 * already decided the appendrel is not parallel-safe as a whole,
		 * there's no point in considering parallelism for this child.  For
		 * consistency, do this before calling set_rel_size() for the child.
		 */
		if (root->glob->parallelModeOK && rel->consider_parallel)
		{
			set_rel_consider_parallel(root, childrel, childRTE);
		}

		/*
		 * Compute the child's size.
		 */
#ifdef __OPENTENBASE_C__
		if (set_rel_size_hook != NULL)
		{
			(*set_rel_size_hook)(root, childrel, childRTindex, childRTE);
		}
		else
		{
#endif
			set_rel_size(root, childrel, childRTindex, childRTE);
#ifdef __OPENTENBASE_C__
		}
#endif

		if (rel->baserestrictcost.per_tuple < childrel->baserestrictcost.per_tuple)
			rel->baserestrictcost.per_tuple = childrel->baserestrictcost.per_tuple;
		if (rel->baserestrictcost.startup < childrel->baserestrictcost.startup)
			rel->baserestrictcost.startup = childrel->baserestrictcost.startup;

		if (rel->reltarget->cost.startup < childrel->reltarget->cost.startup)
			rel->reltarget->cost.startup = childrel->reltarget->cost.startup;
		if (rel->reltarget->cost.per_tuple < childrel->reltarget->cost.per_tuple)
			rel->reltarget->cost.per_tuple = childrel->reltarget->cost.per_tuple;
		if (rel->allvisfrac < childrel->allvisfrac)
			rel->allvisfrac = childrel->allvisfrac;
		rel->pages += childrel->pages;
		rel->tuples += childrel->tuples;
		/*
		 * It is possible that constraint exclusion detected a contradiction
		 * within a child subquery, even though we didn't prove one above. If
		 * so, we can skip this child.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* We have at least one live child. */
		has_live_children = true;

		/*
		 * If any live child is not parallel-safe, treat the whole appendrel
		 * as not parallel-safe.  In future we might be able to generate plans
		 * in which some children are farmed out to workers while others are
		 * not; but we don't have that today, so it's a waste to consider
		 * partial paths anywhere in the appendrel unless it's all safe.
		 * (Child rels visited before this one will be unmarked in
		 * set_append_rel_pathlist().)
		 */
		if (!childrel->consider_parallel)
			rel->consider_parallel = false;

		/*
		 * Accumulate size information from each live child.
		 */
		Assert(childrel->rows > 0);

		parent_rows += childrel->rows;
		parent_size += childrel->reltarget->width * childrel->rows;

		/*
		 * Accumulate per-column estimates too.  We need not do anything for
		 * PlaceHolderVars in the parent list.  If child expression isn't a
		 * Var, or we didn't record a width estimate for it, we have to fall
		 * back on a datatype-based estimate.
		 *
		 * By construction, child's targetlist is 1-to-1 with parent's.
		 */
		forboth(parentvars, rel->reltarget->exprs,
				childvars, childrel->reltarget->exprs)
		{
			Var		   *parentvar = (Var *) lfirst(parentvars);
			Node	   *childvar = (Node *) lfirst(childvars);

			if (IsA(parentvar, Var) && parentvar->varno == parentRTindex)
			{
				int			pndx = parentvar->varattno - rel->min_attr;
				int32		child_width = 0;

				if (IsA(childvar, Var) &&
					((Var *) childvar)->varno == childrel->relid)
				{
					int			cndx = ((Var *) childvar)->varattno - childrel->min_attr;

					child_width = childrel->attr_widths[cndx];
				}
				if (child_width <= 0)
					child_width = get_typavgwidth(exprType(childvar),
												  exprTypmod(childvar));
				Assert(child_width > 0);
				parent_attrsizes[pndx] += child_width * childrel->rows;
			}
		}
	}

	if (has_live_children)
	{
		/*
		 * Save the finished size estimates.
		 */
		int			i;

		Assert(parent_rows > 0);
		rel->rows = parent_rows;
		rel->reltarget->width = rint(parent_size / parent_rows);
		for (i = 0; i < nattrs; i++)
			rel->attr_widths[i] = rint(parent_attrsizes[i] / parent_rows);

		/*
		 * Set "raw tuples" count equal to "rows" for the appendrel; needed
		 * because some places assume rel->tuples is valid for any baserel.
		 */
		rel->tuples = parent_rows;

		/*
		 * Note that we leave rel->pages as zero; this is important to avoid
		 * double-counting the appendrel tree in total_table_pages.
		 */
	}
	else
	{
		/*
		 * All children were excluded by constraints, so mark the whole
		 * appendrel dummy.  We must do this in this phase so that the rel's
		 * dummy-ness is visible when we generate paths for other rels.
		 */
		set_dummy_rel_pathlist(rel);
	}

	pfree(parent_attrsizes);
}

static void
bubble_up_partitions(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	int       parentRTindex = rti;
	ListCell *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * non-dummy children.
	 */
	foreach (l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int            childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo    *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];
		childrel->needPaths = false;
		if (childRTE->inh && childRTE->relkind == RELKIND_PARTITIONED_TABLE)
			bubble_up_partitions(root, childrel, childRTindex, childRTE);
		else
		{
			rel->partition_leaf_rels = lappend_int(rel->partition_leaf_rels, childRTE->relid);
			rel->partition_leaf_rels_idx = lappend_int(rel->partition_leaf_rels_idx, childRTindex);
		}
		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		if (childrel->part_scheme)
		{
			rel->partition_leaf_rels =
				list_concat(rel->partition_leaf_rels, list_copy(childrel->partition_leaf_rels));
			rel->partition_leaf_rels_idx =
				list_concat(rel->partition_leaf_rels_idx, list_copy(childrel->partition_leaf_rels_idx));
		}
		rel->partitioned_child_rels =
			list_concat(rel->partitioned_child_rels, list_copy(childrel->partitioned_child_rels));
	}
}

/*
 * set_append_rel_pathlist
 *	  Build access paths for an "append relation"
 */
static void
set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, RangeTblEntry *rte)
{
	int			parentRTindex = rti;
	List	   *live_childrels = NIL;
	ListCell   *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * non-dummy children.
	 */
	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		/* Belongs to a heterogeneous partitioned table, inherits this attribute. */
		if (enable_partition_iterator && !enable_partitionwise_join && rel->rtekind == RTE_RELATION &&
		    rte->relkind == RELKIND_PARTITIONED_TABLE)
			childrel->isPartHeterogeneous = true;

		/*
		 * If set_append_rel_size() decided the parent appendrel was
		 * parallel-unsafe at some point after visiting this child rel, we
		 * need to propagate the unsafety marking down to the child, so that
		 * we don't generate useless partial paths for it.
		 */
		if (!rel->consider_parallel)
			childrel->consider_parallel = false;

		/*
		 * Compute the child's access paths.
		 */
		set_rel_pathlist(root, childrel, childRTindex, childRTE);

		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* Bubble up childrel's partitioned children. */
		if (rel->part_scheme)
			rel->partitioned_child_rels =
				list_concat(rel->partitioned_child_rels,
							list_copy(childrel->partitioned_child_rels));

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);
	}

	/* Add paths to the append relation. */
	add_paths_to_append_rel(root, rel, live_childrels);
}


/*
 * add_paths_to_append_rel
 *		Generate paths for the given append relation given the set of non-dummy
 *		child rels.
 *
 * The function collects all parameterizations and orderings supported by the
 * non-dummy children. For every such parameterization or ordering, it creates
 * an append path collecting one path from each non-dummy child with given
 * parameterization or ordering. Similarly it collects partial paths from
 * non-dummy children to create partial append paths.
 */
void
add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel,
						List *live_childrels)
{
	List	   *subpaths = NIL;
	bool		subpaths_valid = true;
	List	   *partial_subpaths = NIL;
	List	   *pa_partial_subpaths = NIL;
	List	   *pa_nonpartial_subpaths = NIL;
	bool		partial_subpaths_valid = true;
	bool		pa_subpaths_valid;
	List	   *all_child_pathkeys = NIL;
	List	   *all_child_outers = NIL;
	ListCell   *l;
	List	   *partitioned_rels = NIL;
	double		partial_rows = -1;
	bool		omit = false;

	/* If appropriate, consider parallel append */
	pa_subpaths_valid = enable_parallel_append && rel->consider_parallel;

	/* force omitting child append path for partitioned table */
	if (rel->relid > 0 && IS_SIMPLE_REL(rel) && rel->rtekind == RTE_RELATION)
		omit = true;

	/*
	 * AppendPath generated for partitioned tables must record the RT indexes
	 * of partitioned tables that are direct or indirect children of this
	 * Append rel.
	 *
	 * AppendPath may be for a sub-query RTE (UNION ALL), in which case, 'rel'
	 * itself does not represent a partitioned relation, but the child sub-
	 * queries may contain references to partitioned relations.  The loop
	 * below will look for such children and collect them in a list to be
	 * passed to the path creation function.  (This assumes that we don't need
	 * to look through multiple levels of subquery RTEs; if we ever do, we
	 * could consider stuffing the list we generate here into sub-query RTE's
	 * RelOptInfo, just like we do for partitioned rels, which would be used
	 * when populating our parent rel with paths.  For the present, that
	 * appears to be unnecessary.)
	 */
	if (rel->part_scheme != NULL)
	{
		if (IS_SIMPLE_REL(rel))
			partitioned_rels = list_make1(rel->partitioned_child_rels);
		else if (IS_JOIN_REL(rel))
		{
			int			relid = -1;
			List	   *partrels = NIL;

			/*
			 * For a partitioned joinrel, concatenate the component rels'
			 * partitioned_child_rels lists.
			 */
			while ((relid = bms_next_member(rel->relids, relid)) >= 0)
			{
				RelOptInfo *component;

				Assert(relid >= 1 && relid < root->simple_rel_array_size);
				component = root->simple_rel_array[relid];
				Assert(component->part_scheme != NULL);
				Assert(list_length(component->partitioned_child_rels) >= 1);
				partrels =
					list_concat(partrels,
								list_copy(component->partitioned_child_rels));
			}

			partitioned_rels = list_make1(partrels);
		}

		Assert(list_length(partitioned_rels) >= 1);
	}

	/*
	 * For every non-dummy child, remember the cheapest path.  Also, identify
	 * all pathkeys (orderings) and parameterizations (required_outer sets)
	 * available for the non-dummy member relations.
	 */
	foreach(l, live_childrels)
	{
		RelOptInfo *childrel = lfirst(l);
		ListCell   *lcp;
		Path	   *cheapest_partial_path = NULL;

		/*
		 * For UNION ALLs with non-empty partitioned_child_rels, accumulate
		 * the Lists of child relations.
		 */
		if (rel->rtekind == RTE_SUBQUERY && childrel->partitioned_child_rels != NIL)
			partitioned_rels = lappend(partitioned_rels,
									   childrel->partitioned_child_rels);

		/*
		 * If child has an unparameterized cheapest-total path, add that to
		 * the unparameterized Append path we are constructing for the parent.
		 * If not, there's no workable unparameterized path.
		 */
		if (childrel->cheapest_total_path->param_info == NULL)
			accumulate_append_subpath(childrel->cheapest_total_path,
									  omit, &subpaths, NULL);
		else
			subpaths_valid = false;

		/* Same idea, but for a partial plan. */
		if (childrel->partial_pathlist != NIL)
		{
			cheapest_partial_path = linitial(childrel->partial_pathlist);
			accumulate_append_subpath(cheapest_partial_path,
									  omit, &partial_subpaths, NULL);
		}
		else
			partial_subpaths_valid = false;

		/*
		 * Same idea, but for a parallel append mixing partial and non-partial
		 * paths.
		 */
		if (pa_subpaths_valid)
		{
			Path	   *nppath = NULL;

			nppath =
				get_cheapest_parallel_safe_total_inner(childrel->pathlist);

			/* do not do parallel for a non-partial path if contains RemoteSubplan */
			if (nppath != NULL)
			{
				int nRemotePlans_child = 0;
				contains_remotesubplan(nppath, &nRemotePlans_child);
				if (nRemotePlans_child > 0)
					nppath = NULL;
			}

			if (cheapest_partial_path == NULL && nppath == NULL)
			{
				/* Neither a partial nor a parallel-safe path?  Forget it. */
				pa_subpaths_valid = false;
			}
			else if (nppath == NULL ||
					 (cheapest_partial_path != NULL &&
					  cheapest_partial_path->total_cost < nppath->total_cost))
			{
				/* Partial path is cheaper or the only option. */
				Assert(cheapest_partial_path != NULL);
				accumulate_append_subpath(cheapest_partial_path, omit,
										  &pa_partial_subpaths,
										  &pa_nonpartial_subpaths);

			}
			else
			{
				/*
				 * Either we've got only a non-partial path, or we think that
				 * a single backend can execute the best non-partial path
				 * faster than all the parallel backends working together can
				 * execute the best partial path.
				 *
				 * It might make sense to be more aggressive here.  Even if
				 * the best non-partial path is more expensive than the best
				 * partial path, it could still be better to choose the
				 * non-partial path if there are several such paths that can
				 * be given to different workers.  For now, we don't try to
				 * figure that out.
				 */
				accumulate_append_subpath(nppath, omit,
										  &pa_nonpartial_subpaths,
										  NULL);
			}
		}

		/*
		 * Collect lists of all the available path orderings and
		 * parameterizations for all the children.  We use these as a
		 * heuristic to indicate which sort orderings and parameterizations we
		 * should build Append and MergeAppend paths for.
		 */
		foreach(lcp, childrel->pathlist)
		{
			Path	   *childpath = (Path *) lfirst(lcp);
			List	   *childkeys = childpath->pathkeys;
			Relids		childouter = PATH_REQ_OUTER(childpath);

			/* Unsorted paths don't contribute to pathkey list */
			if (childkeys != NIL)
			{
				ListCell   *lpk;
				bool		found = false;

				/* Have we already seen this ordering? */
				foreach(lpk, all_child_pathkeys)
				{
					List	   *existing_pathkeys = (List *) lfirst(lpk);

					if (compare_pathkeys(existing_pathkeys,
										 childkeys) == PATHKEYS_EQUAL)
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					/* No, so add it to all_child_pathkeys */
					all_child_pathkeys = lappend(all_child_pathkeys,
												 childkeys);
				}
			}

			/* Unparameterized paths don't contribute to param-set list */
			if (childouter)
			{
				ListCell   *lco;
				bool		found = false;

				/* Have we already seen this param set? */
				foreach(lco, all_child_outers)
				{
					Relids		existing_outers = (Relids) lfirst(lco);

					if (bms_equal(existing_outers, childouter))
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					/* No, so add it to all_child_outers */
					all_child_outers = lappend(all_child_outers,
											   childouter);
				}
			}
		}
	}

	/*
	 * If we found unparameterized paths for all children, build an unordered,
	 * unparameterized Append path for the rel.  (Note: this is correct even
	 * if we have zero or one live subpath due to constraint exclusion.)
	 */
	if (subpaths_valid)
		add_path(rel, (Path *) create_append_path(root, rel, subpaths, NIL,
												  NULL, 0, false,
												  partitioned_rels, -1));

	/*
	 * Consider an append of unordered, unparameterized partial paths.  Make
	 * it parallel-aware if possible.
	 */
	if (partial_subpaths_valid)
	{
		AppendPath *appendpath;
		ListCell   *lc;
		int			parallel_workers = 0;

		/* Find the highest number of workers requested for any subpath. */
		foreach(lc, partial_subpaths)
		{
			Path	   *path = lfirst(lc);

			parallel_workers = Max(parallel_workers, path->parallel_workers);
		}
		Assert(parallel_workers > 0);

		/*
		 * If the use of parallel append is permitted, always request at least
		 * log2(# of children) workers.  We assume it can be useful to have
		 * extra workers in this case because they will be spread out across
		 * the children.  The precise formula is just a guess, but we don't
		 * want to end up with a radically different answer for a table with N
		 * partitions vs. an unpartitioned table with the same data, so the
		 * use of some kind of log-scaling here seems to make some sense.
		 */
		if (enable_parallel_append)
		{
			parallel_workers = Max(parallel_workers,
								   fls(list_length(live_childrels)));
			parallel_workers = Min(parallel_workers,
								   max_parallel_workers_per_gather);
		}
		Assert(parallel_workers > 0);

		/* Generate a partial append path. */
		appendpath = create_append_path(root, rel, NIL, partial_subpaths, NULL,
										parallel_workers,
										enable_parallel_append,
										partitioned_rels, -1);

		/*
		 * Make sure any subsequent partial paths use the same row count
		 * estimate.
		 */
		partial_rows = appendpath->path.rows;

		/* Add the path. */
		add_partial_path(rel, (Path *) appendpath);
	}

	/*
	 * Consider a parallel-aware append using a mix of partial and non-partial
	 * paths.  (This only makes sense if there's at least one child which has
	 * a non-partial path that is substantially cheaper than any partial path;
	 * otherwise, we should use the append path added in the previous step.)
	 */
	if (pa_subpaths_valid && pa_nonpartial_subpaths != NIL)
	{
		AppendPath *appendpath;
		ListCell   *lc;
		int			parallel_workers = 0;

		/*
		 * Find the highest number of workers requested for any partial
		 * subpath.
		 */
		foreach(lc, pa_partial_subpaths)
		{
			Path	   *path = lfirst(lc);

			parallel_workers = Max(parallel_workers, path->parallel_workers);
		}

		/*
		 * Same formula here as above.  It's even more important in this
		 * instance because the non-partial paths won't contribute anything to
		 * the planned number of parallel workers.
		 */
		parallel_workers = Max(parallel_workers,
							   fls(list_length(live_childrels)));
		parallel_workers = Min(parallel_workers,
							   max_parallel_workers_per_gather);
		Assert(parallel_workers > 0);

		appendpath = create_append_path(root, rel, pa_nonpartial_subpaths,
										pa_partial_subpaths,
										NULL, parallel_workers, true,
										partitioned_rels, partial_rows);
		add_partial_path(rel, (Path *) appendpath);
	}

	/*
	 * Also build unparameterized MergeAppend paths based on the collected
	 * list of child pathkeys.
	 */
	if (subpaths_valid)
		generate_mergeappend_paths(root, rel, live_childrels,
								   all_child_pathkeys,
								   partitioned_rels);

	/*
	 * Build Append paths for each parameterization seen among the child rels.
	 * (This may look pretty expensive, but in most cases of practical
	 * interest, the child rels will expose mostly the same parameterizations,
	 * so that not that many cases actually get considered here.)
	 *
	 * The Append node itself cannot enforce quals, so all qual checking must
	 * be done in the child paths.  This means that to have a parameterized
	 * Append path, we must have the exact same parameterization for each
	 * child path; otherwise some children might be failing to check the
	 * moved-down quals.  To make them match up, we can try to increase the
	 * parameterization of lesser-parameterized paths.
	 */
	foreach(l, all_child_outers)
	{
		Relids		required_outer = (Relids) lfirst(l);
		ListCell   *lcr;

		/* Select the child paths for an Append with this parameterization */
		subpaths = NIL;
		subpaths_valid = true;
		foreach(lcr, live_childrels)
		{
			RelOptInfo *childrel = (RelOptInfo *) lfirst(lcr);
			Path	   *subpath;

			subpath = get_cheapest_parameterized_child_path(root,
															childrel,
															required_outer);
			if (subpath == NULL)
			{
				/* failed to make a suitable path for this child */
				subpaths_valid = false;
				break;
			}
			accumulate_append_subpath(subpath, omit, &subpaths, NULL);
		}

		if (subpaths_valid)
			add_path(rel, (Path *)
					 create_append_path(root, rel, subpaths, NIL,
										required_outer, 0, false,
										partitioned_rels, -1));
	}
}

/*
 * generate_mergeappend_paths
 *		Generate MergeAppend paths for an append relation
 *
 * Generate a path for each ordering (pathkey list) appearing in
 * all_child_pathkeys.
 *
 * We consider both cheapest-startup and cheapest-total cases, ie, for each
 * interesting ordering, collect all the cheapest startup subpaths and all the
 * cheapest total paths, and build a MergeAppend path for each case.
 *
 * We don't currently generate any parameterized MergeAppend paths.  While
 * it would not take much more code here to do so, it's very unclear that it
 * is worth the planning cycles to investigate such paths: there's little
 * use for an ordered path on the inside of a nestloop.  In fact, it's likely
 * that the current coding of add_path would reject such paths out of hand,
 * because add_path gives no credit for sort ordering of parameterized paths,
 * and a parameterized MergeAppend is going to be more expensive than the
 * corresponding parameterized Append path.  If we ever try harder to support
 * parameterized mergejoin plans, it might be worth adding support for
 * parameterized MergeAppends to feed such joins.  (See notes in
 * optimizer/README for why that might not ever happen, though.)
 */
static void
generate_mergeappend_paths(PlannerInfo *root, RelOptInfo *rel,
						   List *live_childrels,
						   List *all_child_pathkeys,
						   List *partitioned_rels)
{
	ListCell   *lcp;

	foreach(lcp, all_child_pathkeys)
	{
		List	   *pathkeys = (List *) lfirst(lcp);
		List	   *startup_subpaths = NIL;
		List	   *total_subpaths = NIL;
		bool		startup_neq_total = false;
		ListCell   *lcr;

		/* Select the child paths for this ordering... */
		foreach(lcr, live_childrels)
		{
			RelOptInfo *childrel = (RelOptInfo *) lfirst(lcr);
			Path	   *cheapest_startup,
					   *cheapest_total;

			/* Locate the right paths, if they are available. */
			cheapest_startup =
				get_cheapest_path_for_pathkeys(childrel->pathlist,
											   pathkeys,
											   NULL,
											   STARTUP_COST,
											   false);
			cheapest_total =
				get_cheapest_path_for_pathkeys(childrel->pathlist,
											   pathkeys,
											   NULL,
											   TOTAL_COST,
											   false);

			/*
			 * If we can't find any paths with the right order just use the
			 * cheapest-total path; we'll have to sort it later.
			 */
			if (cheapest_startup == NULL || cheapest_total == NULL)
			{
				cheapest_startup = cheapest_total =
					childrel->cheapest_total_path;
				/* Assert we do have an unparameterized path for this child */
				Assert(cheapest_total->param_info == NULL);
			}

			/*
			 * Notice whether we actually have different paths for the
			 * "cheapest" and "total" cases; frequently there will be no point
			 * in two create_merge_append_path() calls.
			 */
			if (cheapest_startup != cheapest_total)
				startup_neq_total = true;

			accumulate_append_subpath(cheapest_startup, true,
									  &startup_subpaths, NULL);
			accumulate_append_subpath(cheapest_total, true,
									  &total_subpaths, NULL);
		}

		/* ... and build the MergeAppend paths */
		add_path(rel, (Path *) create_merge_append_path(root,
														rel,
														startup_subpaths,
														pathkeys,
														NULL,
														partitioned_rels));
		if (startup_neq_total)
			add_path(rel, (Path *) create_merge_append_path(root,
															rel,
															total_subpaths,
															pathkeys,
															NULL,
															partitioned_rels));
	}
}

/*
 * get_cheapest_parameterized_child_path
 *		Get cheapest path for this relation that has exactly the requested
 *		parameterization.
 *
 * Returns NULL if unable to create such a path.
 */
static Path *
get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel,
									  Relids required_outer)
{
	Path	   *cheapest;
	ListCell   *lc;

	/*
	 * Look up the cheapest existing path with no more than the needed
	 * parameterization.  If it has exactly the needed parameterization, we're
	 * done.
	 */
	cheapest = get_cheapest_path_for_pathkeys(rel->pathlist,
											  NIL,
											  required_outer,
											  TOTAL_COST,
											  false);
	Assert(cheapest != NULL);
	if (bms_equal(PATH_REQ_OUTER(cheapest), required_outer))
		return cheapest;

	/*
	 * Otherwise, we can "reparameterize" an existing path to match the given
	 * parameterization, which effectively means pushing down additional
	 * joinquals to be checked within the path's scan.  However, some existing
	 * paths might check the available joinquals already while others don't;
	 * therefore, it's not clear which existing path will be cheapest after
	 * reparameterization.  We have to go through them all and find out.
	 */
	cheapest = NULL;
	foreach(lc, rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		/* Can't use it if it needs more than requested parameterization */
		if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
			continue;

		/*
		 * Reparameterization can only increase the path's cost, so if it's
		 * already more expensive than the current cheapest, forget it.
		 */
		if (cheapest != NULL &&
			compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
			continue;

		/* Reparameterize if needed, then recheck cost */
		if (!bms_equal(PATH_REQ_OUTER(path), required_outer))
		{
			path = reparameterize_path(root, path, required_outer, 1.0);
			if (path == NULL)
				continue;		/* failed to reparameterize this one */
			Assert(bms_equal(PATH_REQ_OUTER(path), required_outer));

			if (cheapest != NULL &&
				compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
				continue;
		}

		/* We have a new best path */
		cheapest = path;
	}

	/* Return the best path, or NULL if we found no suitable candidate */
	return cheapest;
}

/*
 * accumulate_append_subpath
 *		Add a subpath to the list being built for an Append or MergeAppend.
 *
 * It's possible that the child is itself an Append or MergeAppend path, in
 * which case we can "cut out the middleman" and just add its child paths to
 * our own list.  (We don't try to do this earlier because we need to apply
 * both levels of transformation to the quals.)
 *
 * However, in OpenTenBase, such omission would result in each leg of the appended
 * partition table scan having a Remote operator. Therefore, it's better to
 * keep them in an Append path state.
 *
 * Note that if we omit a child MergeAppend in this way, we are effectively
 * omitting a sort step, which seems fine: if the parent is to be an Append,
 * its result would be unsorted anyway, while if the parent is to be a
 * MergeAppend, there's no point in a separate sort on a child.
 * its result would be unsorted anyway.
 *
 * Normally, either path is a partial path and subpaths is a list of partial
 * paths, or else path is a non-partial plan and subpaths is a list of those.
 * However, if path is a parallel-aware Append, then we add its partial path
 * children to subpaths and the rest to special_subpaths.  If the latter is
 * NULL, we don't flatten the path at all (unless it contains only partial
 * paths).
 */
static void
accumulate_append_subpath(Path *path, bool omit, List **subpaths, List **special_subpaths)
{
	if (omit || path->parent->rtekind != RTE_RELATION || !path->distribution)
	{
		/* only omitting setop append or local partitioned table */
		if (IsA(path, AppendPath))
		{
			AppendPath *apath = (AppendPath *) path;

			if (!apath->path.parallel_aware || apath->first_partial_path == 0)
			{
				/* list_copy is important here to avoid sharing list substructure */
				*subpaths = list_concat(*subpaths, list_copy(apath->subpaths));
				return;
			}
			else if (special_subpaths != NULL)
			{
				List	   *new_special_subpaths;

				/* Split Parallel Append into partial and non-partial subpaths */
				*subpaths = list_concat(*subpaths,
										list_copy_tail(apath->subpaths,
													   apath->first_partial_path));
				new_special_subpaths =
					list_truncate(list_copy(apath->subpaths),
								  apath->first_partial_path);
				*special_subpaths = list_concat(*special_subpaths,
												new_special_subpaths);
				return;
			}
		}
		else if (IsA(path, MergeAppendPath))
		{
			MergeAppendPath *mpath = (MergeAppendPath *) path;

			/* list_copy is important here to avoid sharing list substructure */
			*subpaths = list_concat(*subpaths, list_copy(mpath->subpaths));
			return;
		}
	}

	*subpaths = lappend(*subpaths, path);
}

/*
 * set_dummy_rel_pathlist
 *	  Build a dummy path for a relation that's been excluded by constraints
 *
 * Rather than inventing a special "dummy" path type, we represent this as an
 * AppendPath with no members (see also IS_DUMMY_PATH/IS_DUMMY_REL macros).
 *
 * This is exported because inheritance_planner() has need for it.
 */
void
set_dummy_rel_pathlist(RelOptInfo *rel)
{
	/* Set dummy size estimates --- we leave attr_widths[] as zeroes */
	rel->rows = 0;
	rel->reltarget->width = 0;

	/* Discard any pre-existing paths; no further need for them */
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;

	add_path(rel, (Path *) create_append_path(NULL, rel, NIL, NIL, NULL,
											  0, false, NIL, -1));

	/*
	 * We set the cheapest path immediately, to ensure that IS_DUMMY_REL()
	 * will recognize the relation as dummy if anyone asks.  This is redundant
	 * when we're called from set_rel_size(), but not when called from
	 * elsewhere, and doing it twice is harmless anyway.
	 */
	set_cheapest(rel);
}

/* quick-and-dirty test to see if any joining is needed */
static bool
has_multiple_baserels(PlannerInfo *root)
{
	int			num_base_rels = 0;
	Index		rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RelOptInfo *brel = root->simple_rel_array[rti];

		if (brel == NULL)
			continue;

		/* ignore RTEs that are "other rels" */
		if (brel->reloptkind == RELOPT_BASEREL)
			if (++num_base_rels > 1)
				return true;
	}
	return false;
}

/*
 * set_subquery_pathlist
 *		Generate SubqueryScan access paths for a subquery RTE
 *
 * We don't currently support generating parameterized paths for subqueries
 * by pushing join clauses down into them; it seems too expensive to re-plan
 * the subquery multiple times to consider different alternatives.
 * (XXX that could stand to be reconsidered, now that we use Paths.)
 * So the paths made here will be parameterized if the subquery contains
 * LATERAL references, otherwise not.  As long as that's true, there's no need
 * for a separate set_subquery_size phase: just make the paths right away.
 */
static void
set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	Query	   *subquery = rte->subquery;
	pushdown_safety_info safetyInfo;

	/*
	 * Must copy the Query so that planning doesn't mess up the RTE contents
	 * (really really need to fix the planner to not scribble on its input,
	 * someday ... but see remove_unused_subquery_outputs to start with).
	 */
	subquery = copyObject(subquery);

	/*
	 * Zero out result area for subquery_is_pushdown_safe, so that it can set
	 * flags as needed while recursing.  In particular, we need a workspace
	 * for keeping track of unsafe-to-reference columns.  unsafeColumns[i]
	 * will be set TRUE if we find that output column i of the subquery is
	 * unsafe to use in a pushed-down qual.
	 */
	memset(&safetyInfo, 0, sizeof(safetyInfo));
	safetyInfo.unsafeColumns = (bool *)
		palloc0((list_length(subquery->targetList) + 1) * sizeof(bool));

	/*
	 * If the subquery has the "security_barrier" flag, it means the subquery
	 * originated from a view that must enforce row level security.  Then we
	 * must not push down quals that contain leaky functions.  (Ideally this
	 * would be checked inside subquery_is_pushdown_safe, but since we don't
	 * currently pass the RTE to that function, we must do it here.)
	 */
	safetyInfo.unsafeLeaky = rte->security_barrier;

	/*
	 * If there are any restriction clauses that have been attached to the
	 * subquery relation, consider pushing them down to become WHERE or HAVING
	 * quals of the subquery itself.  This transformation is useful because it
	 * may allow us to generate a better plan for the subquery than evaluating
	 * all the subquery output rows and then filtering them.
	 *
	 * There are several cases where we cannot push down clauses. Restrictions
	 * involving the subquery are checked by subquery_is_pushdown_safe().
	 * Restrictions on individual clauses are checked by
	 * qual_is_pushdown_safe().  Also, we don't want to push down
	 * pseudoconstant clauses; better to have the gating node above the
	 * subquery.
	 *
	 * Non-pushed-down clauses will get evaluated as qpquals of the
	 * SubqueryScan node.
	 *
	 * XXX Are there any cases where we want to make a policy decision not to
	 * push down a pushable qual, because it'd result in a worse plan?
	 */
	if (rel->baserestrictinfo != NIL &&
		subquery_is_pushdown_safe(subquery, subquery, &safetyInfo))
	{
		/* OK to consider pushing down individual quals */
		List	   *upperrestrictlist = NIL;
		ListCell   *l;

		foreach(l, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
			Node	   *clause = (Node *) rinfo->clause;

			if (!rinfo->pseudoconstant &&
				qual_is_pushdown_safe(subquery, rti, clause, &safetyInfo))
			{
				/* Push it down */
				subquery_push_qual(subquery, rte, rti, clause, 0);
			}
			else
			{
				/* Keep it in the upper query */
				upperrestrictlist = lappend(upperrestrictlist, rinfo);
			}
		}
		rel->baserestrictinfo = upperrestrictlist;
		/* We don't bother recomputing baserestrict_min_security */
	}

	pfree(safetyInfo.unsafeColumns);

	return search_subquery_pathlist(root, rel, rti, subquery);
}

#ifdef __OPENTENBASE_C__
/*
 * check if join clause contains function expr
 */
static bool
pushpred_check_func_walker(Node* node, bool* found)
{
	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, FuncExpr))
	{
		*found = true;
		return true;
	}

	return expression_tree_walker(node, (bool (*)())pushpred_check_func_walker, found);
}

static bool
pushpred_check_func(Node* node)
{
	bool found = false;

	(void)pushpred_check_func_walker(node, &found);

	return found;
}

/*
 * Try to push down clauses info subquery
 */
static bool
pushpred_check_qual(PlannerInfo* root,
				RestrictInfo* rinfo,
				RangeTblEntry* rte,
				Index rti,
				Node* clause,
				pushdown_safety_info *safetyInfo)
{
	Query* subquery = rte->subquery;

	if (rinfo->pseudoconstant)
	{
		return false;
	}

	if (!qual_is_pushdown_safe(subquery, rti, clause, safetyInfo))
	{
		return false;
	}

	return true;
}

/*
 * fix the varno/varlevelsup when push the predicate into the subquery
 */
typedef struct trans_lateral_vars_t
{
	List *target_list;
	Query *subquery;
	Index rti;
	int levelsup;
}trans_lateral_vars_t;

static Node*
trans_lateral_vars_mutator(Node *node, trans_lateral_vars_t *lateral_vars)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;
		List *target_list = lateral_vars->target_list;

		/* Inner Var, need to convert to subquery form */
		if (var->varno == lateral_vars->rti)
		{
			Index varattno = var->varattno;
			TargetEntry *tle = get_tle_by_resno(target_list, varattno);

			return (Node *)copyObject(tle->expr);
		}
		else
		{
			/* Outer Var, need to convert to Param*/
			Var *new_var = (Var*)copyObject(var);
			new_var->varlevelsup += lateral_vars->levelsup;

			return (Node *)new_var;
		}
	}

	return expression_tree_mutator(node,
				(Node* (*)(Node*, void*))trans_lateral_vars_mutator, lateral_vars);
}

static Node*
trans_lateral_vars(Query *subquery, Index rti, Node *qual, int levelsup)
{
	trans_lateral_vars_t lateral_vars;
	lateral_vars.subquery = subquery;
	lateral_vars.target_list = subquery->targetList;
	lateral_vars.rti = rti;
	lateral_vars.levelsup = levelsup;

	return query_or_expression_tree_mutator((Node *)qual,
				(Node* (*)(Node*, void*))trans_lateral_vars_mutator, (void *)&lateral_vars, 0);
}

/*
 * collect the lateral vars
 */
typedef struct collect_lateral_vars_t
{
	RelOptInfo *rel;
}collect_lateral_vars_t;

static bool
collect_lateral_vars_walker(Node *node, void *context)
{
	collect_lateral_vars_t *lateral_context = (collect_lateral_vars_t *)context;
	RelOptInfo *rel = lateral_context->rel;

	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		/* Outer Var, need to convert to subquery form */
		if (var->varno != rel->relid)
		{
			rel->lateral_relids = bms_add_member(rel->lateral_relids, var->varno);
			rel->direct_lateral_relids = bms_copy(rel->lateral_relids);
			rel->lateral_vars = lappend(rel->lateral_vars, var);
		}

		return false;
	}
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *restrict_info = (RestrictInfo *)node;
		node = (Node *)restrict_info->clause;
	}

	return expression_tree_walker(node, (bool (*)())collect_lateral_vars_walker, context);
}

static bool
collect_lateral_vars(PlannerInfo *root, Node *restrict_info, RelOptInfo *rel)
{
	collect_lateral_vars_t lateral_context;
	lateral_context.rel = rel;

	return expression_tree_walker(restrict_info,
				(bool (*)())collect_lateral_vars_walker, (void *)&lateral_context);
}

/*
 * Get all the correlation Range Tables to be the candidates, then we can
 * generate join clauses by it.
 */
static Relids
pushpred_get_candidates(PlannerInfo *root, int dest_id)
{
	Relids result = NULL;
	int i = 1;

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *rel = NULL;
		if (dest_id == i)
			continue;

		rel = root->simple_rel_array[i];
		if (rel != NULL && rel->reloptkind == RELOPT_BASEREL)
		{
			result = bms_add_member(result, i);
		}
	}

	return result;
}

/*
 * Generate the join clauses by Equivalence Class.
 */
static List *
pushpred_extract_equalities(PlannerInfo* root, Relids candidates,
										RelOptInfo* rel, Index rti)
{
	List *candidate_clause = NIL;
	int i = 0;
	List * restrict_list = NIL;
	Bitmapset *join_relids = NULL;

	/* ec */
	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *other = root->simple_rel_array[i];

		if (other == NULL || other->reloptkind != RELOPT_BASEREL)
		{
			continue;
		}

		if (candidates != NULL && !bms_is_member(i, candidates))
		{
			continue;
		}

		/* pointer compare */
		if (other == rel)
		{
			continue;
		}

		/* 
		 * if dest subquery constains set operation, it need process subquery
		 * recursively.
		 * if dest subquery is a common subquery, we generate the join clauses
		 * by Equivalence Class directly.
		 */
		if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		{
			ListCell *lc = NULL;
			int parent_rti = 0;
			AppendRelInfo *appinfo = NULL;
			RelOptInfo *parent_rel = NULL;

			foreach(lc, root->append_rel_list)
			{
				appinfo = (AppendRelInfo *) lfirst(lc);

				/* find rti */
				if (appinfo->child_relid == rti)
				{
					parent_rti = appinfo->parent_relid;
					break;
				}
			}

			parent_rel = find_base_rel(root, parent_rti);
			join_relids = bms_union(other->relids, parent_rel->relids);

			restrict_list = generate_join_implied_equalities(root, join_relids,
					other->relids,
					parent_rel);

			restrict_list = (List*)adjust_appendrel_attrs(root,
											(Node*)restrict_list, 1, &appinfo);
		}
		else
		{
			join_relids = bms_union(other->relids, rel->relids);
			restrict_list = generate_join_implied_equalities(root,
							join_relids,
							other->relids,
							rel);
		}

		if (restrict_list == NIL)
			continue;

		candidate_clause = list_concat(candidate_clause, restrict_list);
	}

	return candidate_clause;
}

/* check if the operator of join clauss is '=' */
static bool
isHashableOperator(Node* node)
{
		OpExpr* opexpr = NULL;
		Oid leftArgType = InvalidOid;

		if (!IsA(node, OpExpr))
		{
				return false;
		}

		opexpr = (OpExpr*)node;
		if (2 != list_length(opexpr->args))
		{
				return false;
		}

		leftArgType = exprType((Node*)linitial(opexpr->args));
		/* Only support "=" operator. */
		if (!op_hashjoinable(opexpr->opno, leftArgType) && !op_mergejoinable(opexpr->opno, leftArgType))
		{
				return false;
		}

		return true;
}

/*
 * pushdown the join clauses into subquery
 */
static bool
pushdown_predicate_subquery(PlannerInfo* root, RelOptInfo* rel, Index rti,
				RangeTblEntry *rte, Query *subquery,
				pushdown_safety_info *safetyInfo)
{
	bool has_pushdown = false;
	List *candidate_clause = NIL;
	ListCell *jr = NULL;
	List *joininfo = NULL;

	ListCell *l = NULL;
	List *cannot_pushdown = NULL;
	Relids required_relids = NULL;
	RestrictInfo *rinfo = NULL;

	Relids candidates = pushpred_get_candidates(root, rti);

	candidate_clause = pushpred_extract_equalities(root, candidates, rel, rti);

	/* extarct on join info */
	foreach (jr, rel->joininfo)
	{
		int other_relid = 0;
		RestrictInfo *ri = (RestrictInfo *)lfirst(jr);
		Relids current_and_outer;

		if (!ri->can_join)
		{
			joininfo = lappend(joininfo, ri);
			continue;
		}

		if (!isHashableOperator((Node *)(ri->clause)))
		{
			joininfo = lappend(joininfo, ri);
			continue;
		}

		if (bms_equal(ri->left_relids, rel->relids))
		{
			if (bms_num_members(ri->right_relids) != 1)
			{
				joininfo = lappend(joininfo, ri);
				continue;
			}

			other_relid = bms_singleton_member(ri->right_relids);
		}

		if (bms_equal(ri->right_relids, rel->relids))
		{
			if (bms_num_members(ri->left_relids) != 1)
			{
				joininfo = lappend(joininfo, ri);
				continue;
			}

			other_relid = bms_singleton_member(ri->left_relids);
		}

		if (candidates != NULL && !bms_is_member(other_relid, candidates))
		{
			joininfo = lappend(joininfo, ri);
			continue;
		}

		current_and_outer = bms_copy(rel->relids);
		current_and_outer = bms_add_member(current_and_outer, other_relid);
		if (!join_clause_is_movable_into(ri, rel->relids, current_and_outer))
		{
			joininfo = lappend(joininfo, ri);
			continue;
		}

		/*
		 * To avoid loss of clauses, we check it in advance, there are redundant
		 * check behind, but it is ok.
		 */
		if (!pushpred_check_qual(root, ri, rte, rti,
								(Node *)ri->clause, safetyInfo) ||
			pushpred_check_func((Node *)ri->clause))
		{
			joininfo = lappend(joininfo, ri);
			continue;
		}

		candidate_clause = lappend(candidate_clause, ri);
	}

	rel->joininfo = joininfo;

	/* try to pushdown the predicates */
	foreach (l, candidate_clause)
	{
		Node *clause = NULL;
		rinfo = (RestrictInfo*)lfirst(l);
		clause = (Node*)rinfo->clause;

		if (pushpred_check_qual(root, rinfo, rte, rti, clause, safetyInfo) &&
			!pushpred_check_func(clause))
		{
			/* Mark subquery could predpush */
			has_pushdown = true;

			/* find the lateral vars */
			collect_lateral_vars(root, clause, rel);

			/* Push it down */
			subquery_push_qual(subquery, rte, rti, clause, 1);
			required_relids = bms_union(required_relids, rinfo->required_relids);
		}
		else
		{
			cannot_pushdown = lappend(cannot_pushdown, rinfo);
		}
	}

	/* avoid loss of clauses */
	if (cannot_pushdown != NULL && required_relids != NULL)
	{
		l = NULL;
		rinfo = NULL;
		foreach (l, cannot_pushdown)
		{
			rinfo = (RestrictInfo*)lfirst(l);
			if (bms_is_subset(rinfo->required_relids, required_relids))
			{
				rel->joininfo = lappend(rel->joininfo, rinfo);
			}
		}
	}
	return has_pushdown;
}

/*
 * set_subquery_param_pathlist
 *     Build parameterized path for subquery with join quals. 1.build the join
 *     clauses by equivilence class, we only pushdown the quals which is hashable.
 *     2.check if the join clauses can be pushdown. 3.add the join clauses as the
 *     filter of having subclause.4.call subquery_planner to search the best
 *     parameteriazed path, it may waste some time for some path may enumerate
 *     multiple times. 
 */
void
set_subquery_param_pathlist(PlannerInfo *root, RelOptInfo *rel,
					  Index rti, RangeTblEntry *rte)
{
	Query	   *subquery = rte->subquery;
	pushdown_safety_info safetyInfo;
	bool safe_pushdown = false;
	bool safe_pushpred = false;

	if (rel->reloptkind != RELOPT_BASEREL &&
		rel->reloptkind != RELOPT_OTHER_MEMBER_REL)
		return;

	if (rel->lateral_relids != NULL || rel->direct_lateral_relids != NULL)
		return;

	/*
	 * Must copy the Query so that planning doesn't mess up the RTE contents
	 * (really really need to fix the planner to not scribble on its input,
	 * someday ... but see remove_unused_subquery_outputs to start with).
	 */
	subquery = copyObject(subquery);

	/*
	 * Zero out result area for subquery_is_pushdown_safe, so that it can set
	 * flags as needed while recursing.  In particular, we need a workspace
	 * for keeping track of unsafe-to-reference columns.  unsafeColumns[i]
	 * will be set TRUE if we find that output column i of the subquery is
	 * unsafe to use in a pushed-down qual.
	 */
	memset(&safetyInfo, 0, sizeof(safetyInfo));
	safetyInfo.unsafeColumns = (bool *)
		palloc0((list_length(subquery->targetList) + 1) * sizeof(bool));
	safetyInfo.is_pushpred = true;

	safe_pushdown = subquery_is_pushdown_safe(subquery, subquery, &safetyInfo);

	/*
	 * Try to push predicate to subquery
	 */
	if (safe_pushdown)
	{
		safe_pushpred = pushdown_predicate_subquery(root, rel, rti, rte,
				subquery, &safetyInfo);
	}

	if (!safe_pushdown || !safe_pushpred)
	{
		pfree(safetyInfo.unsafeColumns);
		pfree(subquery);
		return;
	}

	safetyInfo.unsafeLeaky = rte->security_barrier;

	/*
	 * same as set_subquery_pathlist
	 */
	if (rel->baserestrictinfo != NIL && safe_pushdown)
	{
		/* OK to consider pushing down individual quals */
		List	   *upperrestrictlist = NIL;
		ListCell   *l;

		foreach(l, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
			Node	   *clause = (Node *) rinfo->clause;

			if (!rinfo->pseudoconstant &&
				qual_is_pushdown_safe(subquery, rti, clause, &safetyInfo))
			{
				/* Push it down */
				subquery_push_qual(subquery, rte, rti, clause, 0);
			}
			else
			{
				/* Keep it in the upper query */
				upperrestrictlist = lappend(upperrestrictlist, rinfo);
			}
		}

		rel->baserestrictinfo = upperrestrictlist;
		/* We don't bother recomputing baserestrict_min_security */
	}

	pfree(safetyInfo.unsafeColumns);

	return search_subquery_pathlist(root, rel, rti, subquery);
}

#endif

/*
 * subquery_push_qual - push down a qual that we have determined is safe
 */
static void
subquery_push_qual(Query *subquery, RangeTblEntry *rte, Index rti,
				   Node *qual, int levelsup)
{
	if (subquery->setOperations != NULL)
	{
		/* if levelsup is 0 keep old behaviors, otherwise predpush keeps going. */
		if (levelsup != 0)
			levelsup += 1;

		/* Recurse to push it separately to each component query */
		recurse_push_qual(subquery->setOperations, subquery,
						  rte, rti, qual, levelsup);
	}
	else
	{
		/*
		 * We need to replace Vars in the qual (which must refer to outputs of
		 * the subquery) with copies of the subquery's targetlist expressions.
		 * Note that at this point, any uplevel Vars in the qual should have
		 * been replaced with Params, so they need no work.
		 *
		 * This step also ensures that when we are pushing into a setop tree,
		 * each component query gets its own copy of the qual.
		 */
		if (levelsup == 0)
		{
			qual = ReplaceVarsFromTargetList(qual, rti, 0, rte,
							subquery->targetList,
							REPLACEVARS_REPORT_ERROR, 0,
							&subquery->hasSubLinks);
		}
#ifdef __OPENTENBASE_C__
		else
		{
			qual = trans_lateral_vars(subquery, rti, (Node*)qual, levelsup);
		}
#endif
		/*
		 * Now attach the qual to the proper place: normally WHERE, but if the
		 * subquery uses grouping or aggregation, put it in HAVING (since the
		 * qual really refers to the group-result rows).
		 */
		if (subquery->hasAggs || subquery->groupClause ||
			subquery->groupingSets || subquery->havingQual)
			subquery->havingQual = make_and_qual(subquery->havingQual, qual);
		else
			subquery->jointree->quals =
				make_and_qual(subquery->jointree->quals, qual);

		/*
		 * We need not change the subquery's hasAggs or hasSubLinks flags,
		 * since we can't be pushing down any aggregates that weren't there
		 * before, and we don't push down subselects at all.
		 */
	}
}

/*
 * Use subquery_planner to search the cheapest subquery paths.
 */
static void
search_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel,
				Index rti, Query *subquery)
{
	Query	   *parse = root->parse;
	Relids		required_outer;
	double		tuple_fraction;
	PlannerInfo *subroot;
#ifdef XCP
	Distribution *distribution;
#endif
	RelOptInfo *sub_final_rel;
	ListCell   *lc;

	required_outer = rel->lateral_relids;

	/*
	 * The upper query might not use all the subquery's output columns; if
	 * not, we can simplify.
	 */
	remove_unused_subquery_outputs(subquery, rel);

	/*
	 * We can safely pass the outer tuple_fraction down to the subquery if the
	 * outer level has no joining, aggregation, or sorting to do. Otherwise
	 * we'd better tell the subquery to plan for full retrieval. (XXX This
	 * could probably be made more intelligent ...)
	 */
	if (parse->hasAggs ||
		parse->groupClause ||
		parse->groupingSets ||
		parse->havingQual ||
		parse->distinctClause ||
		parse->sortClause ||
		has_multiple_baserels(root))
		tuple_fraction = 0.0;	/* default case */
	else
		tuple_fraction = root->tuple_fraction;

	/* plan_params should not be in use in current query level */
	Assert(root->plan_params == NIL);

	/* Generate a subroot and Paths for the subquery */
	rel->subroot = subquery_planner(root->glob, subquery,
									root,
									false, tuple_fraction);

	/* Isolate the params needed by this specific subplan */
	rel->subplan_params = root->plan_params;
	root->plan_params = NIL;

	/* For convenience. */
	subroot = rel->subroot;

	/*
	 * Temporarily block ORDER BY in subqueries until we can add support
	 * it in Postgres-XL without outputting incorrect results. Should
	 * do this only in normal processing mode though!
     *
	 * The extra conditions below try to handle cases where an ORDER BY
	 * appears in a simple VIEW or INSERT SELECT.
	 */
	if (!IS_CENTRALIZED_MODE && IsUnderPostmaster &&
		list_length(subquery->sortClause) > 1
				&& (subroot->parent_root != root
				|| (subroot->parent_root == root
					&& (root->parse->commandType != CMD_SELECT &&
						root->parse->commandType != CMD_INSERT &&
						!ORA_MODE))))
		elog(ERROR, "Postgres-XL does not currently support ORDER BY in subqueries");

	/*
	 * It's possible that constraint exclusion proved the subquery empty. If
	 * so, it's desirable to produce an unadorned dummy path so that we will
	 * recognize appropriate optimizations at this query level.
	 */
	sub_final_rel = fetch_upper_rel(rel->subroot, UPPERREL_FINAL, NULL);

	if (IS_DUMMY_REL(sub_final_rel))
	{
		set_dummy_rel_pathlist(rel);
		return;
	}

	/*
	 * Mark rel with estimated output rows, width, etc.  Note that we have to
	 * do this before generating outer-query paths, else cost_subqueryscan is
	 * not happy.
	 */
	set_subquery_size_estimates(root, rel);

	/*
	 * For each Path that subquery_planner produced, make a SubqueryScanPath
	 * in the outer query.
	 */
	foreach(lc, sub_final_rel->pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		List	   *pathkeys;

		/*
		 * If subquery has pathkeys but root does not, pull it up to cn
		 * to keep results order for [window func/connect by/rownum].
		 */
		if ((root->parse->connectByExpr ||
			 root->parse->hasWindowFuncs ||
			 root->parse->hasRowNumExpr) &&
			subroot->parent_root == root && subpath->distribution &&
			!pathkeys_contained_in(subroot->sort_pathkeys, root->query_pathkeys))
		{
			subpath = create_remotesubplan_path(root, subpath, NULL);
			lfirst(lc) = subpath;
		}

		/* Convert subpath's pathkeys to outer representation */
		pathkeys = convert_subquery_pathkeys(root,
											 rel,
											 subpath->pathkeys,
											 make_tlist_from_pathtarget(subpath->pathtarget),
											 (rel->reloptkind != RELOPT_OTHER_MEMBER_REL &&
											  subpath->distribution != NULL));

		if (IsValidDistribution(subpath->distribution))
		{
			ListCell *lc;
			int i = 0;
			bool fail = false;

			/* FIXME Could we use pathtarget directly? */
			List *targetlist = make_tlist_from_pathtarget(subpath->pathtarget);

			/*
			 * The distribution expression from the subplan's tlist, but it should
			 * be from the rel, need conversion.
			 */
			distribution = makeNode(Distribution);
			distribution->distributionType = subpath->distribution->distributionType;
			distribution->nodes = bms_copy(subpath->distribution->nodes);
			distribution->restrictNodes = bms_copy(subpath->distribution->restrictNodes);
			distribution->nExprs = subpath->distribution->nExprs;
			distribution->disExprs = (Node **) palloc0(sizeof(Node *) * distribution->nExprs);

			for (i = 0; i < distribution->nExprs; i++)
			{
				foreach(lc, targetlist)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(lc);
					if (equal(tle->expr, subpath->distribution->disExprs[i]))
					{
						distribution->disExprs[i] = (Node *)
								makeVarFromTargetEntry(rel->relid, tle);
						break;
					}
				}
				if (!lc)
				{
					fail = true;
					break;
				}
			}
			if (fail)
			{
				distribution->distributionType = LOCATOR_TYPE_NONE;
				distribution->nExprs = 0;
				distribution->disExprs = NULL;
			}
		}
		else
			distribution = subpath->distribution;

		/* Generate outer path using this subpath */
		add_path(rel, (Path *)
				 create_subqueryscan_path(root, rel, subroot, subpath,
										  pathkeys, required_outer,
										  distribution));
	}

	/* If outer rel allows parallelism, do same for partial paths. */
	if (rel->consider_parallel && bms_is_empty(required_outer))
	{
		/* If consider_parallel is false, there should be no partial paths. */
		Assert(sub_final_rel->consider_parallel ||
			   sub_final_rel->partial_pathlist == NIL);

		/* Same for partial paths. */
		foreach(lc, sub_final_rel->partial_pathlist)
		{
			Path	   *subpath = (Path *) lfirst(lc);
			List	   *pathkeys;

			/*
			 * If subquery has pathkeys but root does not, pull it up to cn
			 * to keep results order for [window func/connect by/rownum].
			 */
			if ((root->parse->connectByExpr ||
				 root->parse->hasWindowFuncs ||
				 root->parse->hasRowNumExpr) &&
				subroot->parent_root == root && subpath->distribution &&
				!pathkeys_contained_in(subroot->sort_pathkeys, root->query_pathkeys))
			{
				subpath = create_remotesubplan_path(root, subpath, NULL);
				lfirst(lc) = subpath;
			}

			/* Convert subpath's pathkeys to outer representation */
			pathkeys = convert_subquery_pathkeys(root,
												 rel,
												 subpath->pathkeys,
												 make_tlist_from_pathtarget(subpath->pathtarget),
												 (rel->reloptkind != RELOPT_OTHER_MEMBER_REL &&
												  subpath->distribution != NULL));

			if (IsValidDistribution(subpath->distribution))
			{
				ListCell *lc;
				int i = 0;
				bool fail = false;

				/* FIXME Could we use pathtarget directly? */
				List *targetlist = make_tlist_from_pathtarget(subpath->pathtarget);

				/*
				 * The distribution expression from the subplan's tlist, but it should
				 * be from the rel, need conversion.
				 */
				distribution = makeNode(Distribution);
				distribution->distributionType = subpath->distribution->distributionType;
				distribution->nodes = bms_copy(subpath->distribution->nodes);
				distribution->restrictNodes = bms_copy(subpath->distribution->restrictNodes);
				distribution->nExprs = subpath->distribution->nExprs;
				distribution->disExprs = (Node **) palloc0(sizeof(Node *) * distribution->nExprs);

				for (i = 0; i < distribution->nExprs; i++)
				{
					foreach(lc, targetlist)
					{
						TargetEntry *tle = (TargetEntry *) lfirst(lc);
						if (equal(tle->expr, subpath->distribution->disExprs[i]))
						{
							distribution->disExprs[i] = (Node *)
									makeVarFromTargetEntry(rel->relid, tle);
							break;
						}
					}
					if (!lc)
					{
						fail = true;
						break;
					}
				}
				if (fail)
				{
					distribution->distributionType = LOCATOR_TYPE_NONE;
					distribution->nExprs = 0;
					distribution->disExprs = NULL;
				}
			}
			else
				distribution = subpath->distribution;

			/* Generate outer path using this subpath */
			add_partial_path(rel, (Path *)
							 create_subqueryscan_path(root, rel,
													  subroot, subpath,
													  pathkeys, required_outer,
													  distribution));
		}
	}
}

/*
 * set_function_pathlist
 *		Build the (single) access path for a function RTE
 */
static void
set_function_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	List	   *pathkeys = NIL;

	/*
	 * We don't support pushing join clauses into the quals of a function
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in the function expression.
	 */
	required_outer = rel->lateral_relids;

	/*
	 * The result is considered unordered unless ORDINALITY was used, in which
	 * case it is ordered by the ordinal column (the last one).  See if we
	 * care, by checking for uses of that Var in equivalence classes.
	 */
	if (rte->funcordinality)
	{
		AttrNumber	ordattno = rel->max_attr;
		Var		   *var = NULL;
		ListCell   *lc;

		/*
		 * Is there a Var for it in rel's targetlist?  If not, the query did
		 * not reference the ordinality column, or at least not in any way
		 * that would be interesting for sorting.
		 */
		foreach(lc, rel->reltarget->exprs)
		{
			Var		   *node = (Var *) lfirst(lc);

			/* checking varno/varlevelsup is just paranoia */
			if (IsA(node, Var) &&
				node->varattno == ordattno &&
				node->varno == rel->relid &&
				node->varlevelsup == 0)
			{
				var = node;
				break;
			}
		}

		/*
		 * Try to build pathkeys for this Var with int8 sorting.  We tell
		 * build_expression_pathkey not to build any new equivalence class; if
		 * the Var isn't already mentioned in some EC, it means that nothing
		 * cares about the ordering.
		 */
		if (var)
			pathkeys = build_expression_pathkey(root,
												(Expr *) var,
												NULL,	/* below outer joins */
												Int8LessOperator,
												rel->relids,
												false);
	}

	/* Generate appropriate path */
	add_path(rel, create_functionscan_path(root, rel,
										   pathkeys, required_outer));
}

/*
 * check_list_contain_all_const
 *      Check the list is contain all consts.
 */
static bool
check_list_contain_all_const(List *list)
{
	ListCell *lc = NULL;
	Node   *node = NULL;

	foreach(lc, list)
	{
		node = lfirst(lc);
		if (IsA(node, List))
		{
			if (!check_list_contain_all_const((List *)node))
			{
				return false;
			}
		}
		else if (!IsA(node, Const))
		{
			return false;
		}
	}

	return true;
}

/*
 * set_values_pathlist
 *		Build the (single) access path for a VALUES RTE
 */
static void
set_values_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	Path        *new_path = NULL;

	/*
	 * We don't support pushing join clauses into the quals of a values scan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in the values expressions.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	new_path = create_valuesscan_path(root, rel, required_outer);

	/* Mark scan as replicated if selected value list is all const */
	if (root->parse->commandType == CMD_SELECT &&
		check_list_contain_all_const(rte->values_lists) &&
		IS_PGXC_COORDINATOR)
	{
		Distribution *targetd;
		int i;

		targetd = makeNode(Distribution);
		targetd->distributionType = LOCATOR_TYPE_REPLICATED;
		for (i = 0; i < NumDataNodes; i++)
			targetd->nodes = bms_add_member(targetd->nodes, i);

		new_path->distribution = targetd;
	}

	add_path(rel, new_path);
}

/*
 * set_tablefunc_pathlist
 *		Build the (single) access path for a table func RTE
 */
static void
set_tablefunc_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a tablefunc
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in the function expression.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_tablefuncscan_path(root, rel,
											required_outer));
}

#ifdef __OPENTENBASE_C__
static Distribution *
get_ctepath_distribution(Distribution *sub_distribution, Index rti)
{
	int i;
	Distribution *cte_distribution;

	if (sub_distribution == NULL)
		return NULL;

	cte_distribution = copyObject(sub_distribution);

	/*
	 * For shared ctescan is used like a real table,
	 * we need to generate its own distribution exprs.
	 */
	for (i = 0; i < sub_distribution->nExprs; i++)
	{
		((Var *)cte_distribution->disExprs[i])->varno = rti;
		((Var *)cte_distribution->disExprs[i])->varnoold = rti;
	}

	return cte_distribution;
}
#endif

/*
 * set_cte_pathlist
 *		Build the (single) access path for a non-self-reference CTE RTE
 *
 * There's no need for a separate set_cte_size phase, since we don't
 * support join-qual-parameterized paths for CTEs.
 */
static void
set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Plan	   *cteplan;
	PlannerInfo *cteroot;
	Index		levelsup;
	int			ndx;
	ListCell   *lc;
	int			plan_id;
	Relids		required_outer;
#ifdef __OPENTENBASE_C__
	int 		scte_refcount = -1;
#endif

	/*
	 * Find the referenced CTE, and locate the plan previously made for it.
	 */
	levelsup = rte->ctelevelsup;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}

	/*
	 * Note: cte_plan_ids can be shorter than cteList, if we are still working
	 * on planning the CTEs (ie, this is a side-reference from another CTE).
	 * So we mustn't use forboth here.
	 */
	ndx = 0;
	foreach(lc, cteroot->parse->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			break;
		ndx++;
	}
	if (lc == NULL)				/* shouldn't happen */
		elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
	if (ndx >= list_length(cteroot->cte_plan_ids))
		elog(ERROR, "could not find plan for CTE \"%s\"", rte->ctename);
	plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
	if (plan_id <= 0)
		elog(ERROR, "no plan was made for CTE \"%s\"", rte->ctename);
	cteplan = (Plan *) list_nth(root->glob->subplans, plan_id - 1);

	/* Mark rel with estimated output rows, width, etc */
	set_cte_size_estimates(root, rel, cteplan->plan_rows);

#ifdef __OPENTENBASE_C__
	if (scte_refcount > 0)
	{
		PlannerInfo *subroot;
		Distribution *dist;

		subroot = (PlannerInfo *) list_nth(root->glob->subroots, plan_id - 1);
		dist = get_ctepath_distribution(subroot->distribution, rel->relid);

		add_path(rel, create_sharedctescan_path(root, rel, rel->reltarget,
												dist, cteplan, 0));

		if (rel->consider_parallel)
		{
			Path *partial_path =
				create_sharedctescan_partial_path(root, rel, rel->reltarget,
												dist, cteplan);
			if (partial_path)
				add_partial_path(rel, partial_path);
		}

		return;
	}
#endif

	/*
	 * We don't support pushing join clauses into the quals of a CTE scan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_ctescan_path(root, rel, required_outer));
}

/*
 * set_namedtuplestore_pathlist
 *		Build the (single) access path for a named tuplestore RTE
 *
 * There's no need for a separate set_namedtuplestore_size phase, since we
 * don't support join-qual-parameterized paths for tuplestores.
 */
static void
set_namedtuplestore_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 RangeTblEntry *rte)
{
	Relids		required_outer;

	/* Mark rel with estimated output rows, width, etc */
	set_namedtuplestore_size_estimates(root, rel);

	/*
	 * We don't support pushing join clauses into the quals of a tuplestore
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_namedtuplestorescan_path(root, rel, required_outer));

	/* Select cheapest path (pretty easy in this case...) */
	set_cheapest(rel);
}

/*
 * set_worktable_pathlist
 *		Build the (single) access path for a self-reference CTE RTE
 *
 * There's no need for a separate set_worktable_size phase, since we don't
 * support join-qual-parameterized paths for CTEs.
 */
static void
set_worktable_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Path	   *ctepath;
	PlannerInfo *cteroot;
	Index		levelsup;
	Relids		required_outer;

	/*
	 * We need to find the non-recursive term's path, which is in the plan
	 * level that's processing the recursive UNION, which is one level *below*
	 * where the CTE comes from.
	 */
	levelsup = rte->ctelevelsup;
	if (levelsup == 0)			/* shouldn't happen */
		elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	levelsup--;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	ctepath = cteroot->non_recursive_path;
	if (!ctepath)				/* shouldn't happen */
		elog(ERROR, "could not find path for CTE \"%s\"", rte->ctename);

	/* Mark rel with estimated output rows, width, etc */
	set_cte_size_estimates(root, rel, ctepath->rows);

	/*
	 * We don't support pushing join clauses into the quals of a worktable
	 * scan, but it could still have required parameterization due to LATERAL
	 * refs in its tlist.  (I'm not sure this is actually possible given the
	 * restrictions on recursive references, but it's easy enough to support.)
	 */
	required_outer = rel->lateral_relids;

	/* Generate appropriate path */
	add_path(rel, create_worktablescan_path(root, rel, required_outer));
}

/*
 * generate_gather_paths
 *		Generate parallel access paths for a relation by pushing a Gather or
 *		Gather Merge on top of a partial path.
 *
 * This must not be called until after we're done creating all partial paths
 * for the specified relation.  (Otherwise, add_partial_path might delete a
 * path that some GatherPath or GatherMergePath has a reference to.)
 *
 * If we're generating paths for a scan or join relation, override_rows will
 * be false, and we'll just use the relation's size estimate.  When we're
 * being called for a partially-grouped path, though, we need to override
 * the rowcount estimate.  (It's not clear that the particular value we're
 * using here is actually best, but the underlying rel has no estimate so
 * we must do something.)
 */
void
generate_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows)
{
	Path	   *cheapest_partial_path;
	Path	   *simple_gather_path;
	ListCell   *lc;
	double		rows;
	double	   *rowsp = NULL;

	/* If there are no partial paths, there's nothing to do here. */
	if (rel->partial_pathlist == NIL)
		return;

	/* Should we override the rel's rowcount estimate? */
	if (override_rows)
		rowsp = &rows;

	/*
	 * The output of Gather is always unsorted, so there's only one partial
	 * path of interest: the cheapest one.  That will be the one at the front
	 * of partial_pathlist because of the way add_partial_path works.
	 */
	cheapest_partial_path = linitial(rel->partial_pathlist);
	rows =
		cheapest_partial_path->rows * cheapest_partial_path->parallel_workers;
	simple_gather_path = (Path *)
		create_gather_path(root, rel, cheapest_partial_path, rel->reltarget,
						   NULL, rowsp);
	add_path(rel, simple_gather_path);

	/*
	 * For each useful ordering, we can consider an order-preserving Gather
	 * Merge.
	 */
	foreach(lc, rel->partial_pathlist)
	{
		Path	   *subpath = (Path *) lfirst(lc);
		GatherMergePath *path;

		if (subpath->pathkeys == NIL)
			continue;

		rows = subpath->rows * subpath->parallel_workers;
		path = create_gather_merge_path(root, rel, subpath, rel->reltarget,
										subpath->pathkeys, NULL, rowsp);
		add_path(rel, &path->path);
	}

	/*
	 * Fix the heap-use-after-free issue by resetting the cheapest path. This is necessary to
	 * prevent apply_scanjoin_target_to_paths from accessing dirty data in subsequent usage.
	 *
	 * Make sure we've identified the cheapest Path for the final rel.  (By
	 * doing this here not in grouping_planner, we include initPlan costs in
	 * the decision, though it's unlikely that will change anything.)
	 */
	set_cheapest(rel);
}

/*
 * make_rel_from_joinlist
 *	  Build access paths using a "joinlist" to guide the join path search.
 *
 * See comments for deconstruct_jointree() for definition of the joinlist
 * data structure.
 */
static RelOptInfo *
make_rel_from_joinlist(PlannerInfo *root, List *joinlist)
{
	int			levels_needed;
	List	   *initial_rels;
	ListCell   *jl;

	/*
	 * Count the number of child joinlist nodes.  This is the depth of the
	 * dynamic-programming algorithm we must employ to consider all ways of
	 * joining the child nodes.
	 */
	levels_needed = list_length(joinlist);

	if (levels_needed <= 0)
		return NULL;			/* nothing to do? */

	/*
	 * Construct a list of rels corresponding to the child joinlist nodes.
	 * This may contain both base rels and rels constructed according to
	 * sub-joinlists.
	 */
	initial_rels = NIL;
	foreach(jl, joinlist)
	{
		Node	   *jlnode = (Node *) lfirst(jl);
		RelOptInfo *thisrel;

		if (IsA(jlnode, RangeTblRef))
		{
			int			varno = ((RangeTblRef *) jlnode)->rtindex;

			thisrel = find_base_rel(root, varno);
		}
		else if (IsA(jlnode, List))
		{
			/* Recurse to handle subproblem */
			thisrel = make_rel_from_joinlist(root, (List *) jlnode);
		}
		else
		{
			elog(ERROR, "unrecognized joinlist node type: %d",
				 (int) nodeTag(jlnode));
			thisrel = NULL;		/* keep compiler quiet */
		}

		initial_rels = lappend(initial_rels, thisrel);
	}

	if (levels_needed == 1)
	{
		/*
		 * Single joinlist node, so we're done.
		 */
		return (RelOptInfo *) linitial(initial_rels);
	}
	else
	{
		/*
		 * Consider the different orders in which we could join the rels,
		 * using a plugin, GEQO, or the regular join search code.
		 *
		 * We put the initial_rels list into a PlannerInfo field because
		 * has_legal_joinclause() needs to look at it (ugly :-().
		 */
		root->initial_rels = initial_rels;

		if (join_search_hook)
			return (*join_search_hook) (root, levels_needed, initial_rels);
		else if (enable_geqo && levels_needed >= geqo_threshold)
			return geqo(root, levels_needed, initial_rels);
		else
			return standard_join_search(root, levels_needed, initial_rels);
	}
}

/*
 * standard_join_search
 *	  Find possible joinpaths for a query by successively finding ways
 *	  to join component relations into join relations.
 *
 * 'levels_needed' is the number of iterations needed, ie, the number of
 *		independent jointree items in the query.  This is > 1.
 *
 * 'initial_rels' is a list of RelOptInfo nodes for each independent
 *		jointree item.  These are the components to be joined together.
 *		Note that levels_needed == list_length(initial_rels).
 *
 * Returns the final level of join relations, i.e., the relation that is
 * the result of joining all the original relations together.
 * At least one implementation path must be provided for this relation and
 * all required sub-relations.
 *
 * To support loadable plugins that modify planner behavior by changing the
 * join searching algorithm, we provide a hook variable that lets a plugin
 * replace or supplement this function.  Any such hook must return the same
 * final join relation as the standard code would, but it might have a
 * different set of implementation paths attached, and only the sub-joinrels
 * needed for these paths need have been instantiated.
 *
 * Note to plugin authors: the functions invoked during standard_join_search()
 * modify root->join_rel_list and root->join_rel_hash.  If you want to do more
 * than one join-order search, you'll probably need to save and restore the
 * original states of those data structures.  See geqo_eval() for an example.
 */
RelOptInfo *
standard_join_search(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	int			lev;
	RelOptInfo *rel;

	/*
	 * This function cannot be invoked recursively within any one planning
	 * problem, so join_rel_level[] can't be in use already.
	 */
	Assert(root->join_rel_level == NULL);

	/*
	 * We employ a simple "dynamic programming" algorithm: we first find all
	 * ways to build joins of two jointree items, then all ways to build joins
	 * of three items (from two-item joins and single items), then four-item
	 * joins, and so on until we have considered all ways to join all the
	 * items into one rel.
	 *
	 * root->join_rel_level[j] is a list of all the j-item rels.  Initially we
	 * set root->join_rel_level[1] to represent all the single-jointree-item
	 * relations.
	 */
	root->join_rel_level = (List **) palloc0((levels_needed + 1) * sizeof(List *));

	root->join_rel_level[1] = initial_rels;

	for (lev = 2; lev <= levels_needed; lev++)
	{
		ListCell   *lc;

		/*
		 * Determine all possible pairs of relations to be joined at this
		 * level, and build paths for making each one from every available
		 * pair of lower-level relations.
		 */
		join_search_one_level(root, lev);

		/*
		 * Run generate_partitionwise_join_paths() and
		 * generate_gather_paths() for each just-processed joinrel.  We could
		 * not do this earlier because both regular and partial paths can get
		 * added to a particular joinrel at multiple times within
		 * join_search_one_level.
		 *
		 * After that, we're done creating paths for the joinrel, so run
		 * set_cheapest().
		 */
		foreach(lc, root->join_rel_level[lev])
		{
			rel = (RelOptInfo *) lfirst(lc);

			/* Create paths for partitionwise joins. */
			generate_partitionwise_join_paths(root, rel);

			/*
			 * Except for the topmost scan/join rel, consider gathering
			 * partial paths.  We'll do the same for the topmost scan/join rel
			 * once we know the final targetlist (see grouping_planner).
			 */
			if (lev < levels_needed)
				generate_gather_paths(root, rel, false);

			/* Find and save the cheapest paths for this rel */
			set_cheapest(rel);

#ifdef OPTIMIZER_DEBUG
			debug_print_rel(root, rel);
#endif
		}
	}

	/*
	 * We should have a single rel at the final level.
	 */
	if (root->join_rel_level[levels_needed] == NIL)
		elog(ERROR, "failed to build any %d-way joins", levels_needed);
	Assert(list_length(root->join_rel_level[levels_needed]) == 1);

	rel = (RelOptInfo *) linitial(root->join_rel_level[levels_needed]);

	root->join_rel_level = NULL;

	return rel;
}

/*****************************************************************************
 *			PUSHING QUALS DOWN INTO SUBQUERIES
 *****************************************************************************/

/*
 * subquery_is_pushdown_safe - is a subquery safe for pushing down quals?
 *
 * subquery is the particular component query being checked.  topquery
 * is the top component of a set-operations tree (the same Query if no
 * set-op is involved).
 *
 * Conditions checked here:
 *
 * 1. If the subquery has a LIMIT clause, we must not push down any quals,
 * since that could change the set of rows returned.
 *
 * 2. If the subquery contains EXCEPT or EXCEPT ALL set ops we cannot push
 * quals into it, because that could change the results.
 *
 * 3. If the subquery uses DISTINCT, we cannot push volatile quals into it.
 * This is because upper-level quals should semantically be evaluated only
 * once per distinct row, not once per original row, and if the qual is
 * volatile then extra evaluations could change the results.  (This issue
 * does not apply to other forms of aggregation such as GROUP BY, because
 * when those are present we push into HAVING not WHERE, so that the quals
 * are still applied after aggregation.)
 *
 * 4. If the subquery contains window functions, we cannot push volatile quals
 * into it.  The issue here is a bit different from DISTINCT: a volatile qual
 * might succeed for some rows of a window partition and fail for others,
 * thereby changing the partition contents and thus the window functions'
 * results for rows that remain.
 *
 * 5. If the subquery contains any set-returning functions in its targetlist,
 * we cannot push volatile quals into it.  That would push them below the SRFs
 * and thereby change the number of times they are evaluated.  Also, a
 * volatile qual could succeed for some SRF output rows and fail for others,
 * a behavior that cannot occur if it's evaluated before SRF expansion.
 *
 * In addition, we make several checks on the subquery's output columns to see
 * if it is safe to reference them in pushed-down quals.  If output column k
 * is found to be unsafe to reference, we set safetyInfo->unsafeColumns[k]
 * to TRUE, but we don't reject the subquery overall since column k might not
 * be referenced by some/all quals.  The unsafeColumns[] array will be
 * consulted later by qual_is_pushdown_safe().  It's better to do it this way
 * than to make the checks directly in qual_is_pushdown_safe(), because when
 * the subquery involves set operations we have to check the output
 * expressions in each arm of the set op.
 *
 * 6. The subquery must not contain CONNECT BY clause or Rownum.
 *
 * Note: pushing quals into a DISTINCT subquery is theoretically dubious:
 * we're effectively assuming that the quals cannot distinguish values that
 * the DISTINCT's equality operator sees as equal, yet there are many
 * counterexamples to that assumption.  However use of such a qual with a
 * DISTINCT subquery would be unsafe anyway, since there's no guarantee which
 * "equal" value will be chosen as the output value by the DISTINCT operation.
 * So we don't worry too much about that.  Another objection is that if the
 * qual is expensive to evaluate, running it for each original row might cost
 * more than we save by eliminating rows before the DISTINCT step.  But it
 * would be very hard to estimate that at this stage, and in practice pushdown
 * seldom seems to make things worse, so we ignore that problem too.
 *
 * Note: likewise, pushing quals into a subquery with window functions is a
 * bit dubious: the quals might remove some rows of a window partition while
 * leaving others, causing changes in the window functions' results for the
 * surviving rows.  We insist that such a qual reference only partitioning
 * columns, but again that only protects us if the qual does not distinguish
 * values that the partitioning equality operator sees as equal.  The risks
 * here are perhaps larger than for DISTINCT, since no de-duplication of rows
 * occurs and thus there is no theoretical problem with such a qual.  But
 * we'll do this anyway because the potential performance benefits are very
 * large, and we've seen no field complaints about the longstanding comparable
 * behavior with DISTINCT.
 */
static bool
subquery_is_pushdown_safe(Query *subquery, Query *topquery,
						  pushdown_safety_info *safetyInfo)
{
	SetOperationStmt *topop;

	/* Check point 1 */
	if (subquery->limitOffset != NULL || subquery->limitCount != NULL)
		return false;

	/* Check point 6 */
	if (subquery->connectByExpr != NULL || subquery->hasRowNumExpr)
		return false;

	/* Check points 3, 4, and 5 */
	if (subquery->distinctClause ||
		subquery->hasWindowFuncs ||
		subquery->hasTargetSRFs)
		safetyInfo->unsafeVolatile = true;

	/*
	 * If we're at a leaf query, check for unsafe expressions in its target
	 * list, and mark any unsafe ones in unsafeColumns[].  (Non-leaf nodes in
	 * setop trees have only simple Vars in their tlists, so no need to check
	 * them.)
	 */
	if (subquery->setOperations == NULL)
		check_output_expressions(subquery, safetyInfo);

	/* Are we at top level, or looking at a setop component? */
	if (subquery == topquery)
	{
		/* Top level, so check any component queries */
		if (subquery->setOperations != NULL)
			if (!recurse_pushdown_safe(subquery->setOperations, topquery,
									   safetyInfo))
				return false;
	}
	else
	{
		/* Setop component must not have more components (too weird) */
		if (subquery->setOperations != NULL)
			return false;
		/* Check whether setop component output types match top level */
		topop = castNode(SetOperationStmt, topquery->setOperations);
		Assert(topop);
		compare_tlist_datatypes(subquery->targetList,
								topop->colTypes,
								safetyInfo);
	}
	return true;
}

/*
 * Helper routine to recurse through setOperations tree
 */
static bool
recurse_pushdown_safe(Node *setOp, Query *topquery,
					  pushdown_safety_info *safetyInfo)
{
	if (IsA(setOp, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) setOp;
		RangeTblEntry *rte = rt_fetch(rtr->rtindex, topquery->rtable);
		Query	   *subquery = rte->subquery;

		Assert(subquery != NULL);
		return subquery_is_pushdown_safe(subquery, topquery, safetyInfo);
	}
	else if (IsA(setOp, SetOperationStmt))
	{
		SetOperationStmt *op = (SetOperationStmt *) setOp;

		/* EXCEPT is no good (point 2 for subquery_is_pushdown_safe) */
		if (op->op == SETOP_EXCEPT || op->op == SETOP_MINUS)
			return false;
		/* Else recurse */
		if (!recurse_pushdown_safe(op->larg, topquery, safetyInfo))
			return false;
		if (!recurse_pushdown_safe(op->rarg, topquery, safetyInfo))
			return false;
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(setOp));
	}
	return true;
}

/*
 * check_output_expressions - check subquery's output expressions for safety
 *
 * There are several cases in which it's unsafe to push down an upper-level
 * qual if it references a particular output column of a subquery.  We check
 * each output column of the subquery and set unsafeColumns[k] to TRUE if
 * that column is unsafe for a pushed-down qual to reference.  The conditions
 * checked here are:
 *
 * 1. We must not push down any quals that refer to subselect outputs that
 * return sets, else we'd introduce functions-returning-sets into the
 * subquery's WHERE/HAVING quals.
 *
 * 2. We must not push down any quals that refer to subselect outputs that
 * contain volatile functions, for fear of introducing strange results due
 * to multiple evaluation of a volatile function.
 *
 * 3. If the subquery uses DISTINCT ON, we must not push down any quals that
 * refer to non-DISTINCT output columns, because that could change the set
 * of rows returned.  (This condition is vacuous for DISTINCT, because then
 * there are no non-DISTINCT output columns, so we needn't check.  Note that
 * subquery_is_pushdown_safe already reported that we can't use volatile
 * quals if there's DISTINCT or DISTINCT ON.)
 *
 * 4. If the subquery has any window functions, we must not push down quals
 * that reference any output columns that are not listed in all the subquery's
 * window PARTITION BY clauses.  We can push down quals that use only
 * partitioning columns because they should succeed or fail identically for
 * every row of any one window partition, and totally excluding some
 * partitions will not change a window function's results for remaining
 * partitions.  (Again, this also requires nonvolatile quals, but
 * subquery_is_pushdown_safe handles that.)
 */
static void
check_output_expressions(Query *subquery, pushdown_safety_info *safetyInfo)
{
	ListCell   *lc;

	foreach(lc, subquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;			/* ignore resjunk columns */

		/* We need not check further if output col is already known unsafe */
		if (safetyInfo->unsafeColumns[tle->resno])
			continue;

		/* Functions returning sets are unsafe (point 1) */
		if (subquery->hasTargetSRFs &&
			expression_returns_set((Node *) tle->expr))
		{
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* Volatile functions are unsafe (point 2) */
		if (contain_volatile_functions((Node *) tle->expr))
		{
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* If subquery uses DISTINCT ON, check point 3 */
		if (subquery->hasDistinctOn &&
			!targetIsInSortList(tle, InvalidOid, subquery->distinctClause))
		{
			/* non-DISTINCT column, so mark it unsafe */
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}

		/* If subquery uses window functions, check point 4 */
		if (subquery->hasWindowFuncs &&
			!targetIsInAllPartitionLists(tle, subquery))
		{
			/* not present in all PARTITION BY clauses, so mark it unsafe */
			safetyInfo->unsafeColumns[tle->resno] = true;
			continue;
		}
	}
}

/*
 * For subqueries using UNION/UNION ALL/INTERSECT/INTERSECT ALL, we can
 * push quals into each component query, but the quals can only reference
 * subquery columns that suffer no type coercions in the set operation.
 * Otherwise there are possible semantic gotchas.  So, we check the
 * component queries to see if any of them have output types different from
 * the top-level setop outputs.  unsafeColumns[k] is set true if column k
 * has different type in any component.
 *
 * We don't have to care about typmods here: the only allowed difference
 * between set-op input and output typmods is input is a specific typmod
 * and output is -1, and that does not require a coercion.
 *
 * tlist is a subquery tlist.
 * colTypes is an OID list of the top-level setop's output column types.
 * safetyInfo->unsafeColumns[] is the result array.
 */
static void
compare_tlist_datatypes(List *tlist, List *colTypes,
						pushdown_safety_info *safetyInfo)
{
	ListCell   *l;
	ListCell   *colType = list_head(colTypes);

	foreach(l, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (tle->resjunk)
			continue;			/* ignore resjunk columns */
		if (colType == NULL)
			elog(ERROR, "wrong number of tlist entries");
		if (exprType((Node *) tle->expr) != lfirst_oid(colType))
			safetyInfo->unsafeColumns[tle->resno] = true;
		colType = lnext(colType);
	}
	if (colType != NULL)
		elog(ERROR, "wrong number of tlist entries");
}

/*
 * targetIsInAllPartitionLists
 *		True if the TargetEntry is listed in the PARTITION BY clause
 *		of every window defined in the query.
 *
 * It would be safe to ignore windows not actually used by any window
 * function, but it's not easy to get that info at this stage; and it's
 * unlikely to be useful to spend any extra cycles getting it, since
 * unreferenced window definitions are probably infrequent in practice.
 */
static bool
targetIsInAllPartitionLists(TargetEntry *tle, Query *query)
{
	ListCell   *lc;

	foreach(lc, query->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(lc);

		if (!targetIsInSortList(tle, InvalidOid, wc->partitionClause))
			return false;
	}
	return true;
}

/*
 * check_target_list_subquery
 *	Check whether variables in the target list contain subplans.
 */
static bool
check_target_list_subquery(Var *var, List *targetList)
{
	TargetEntry		*tle;

	if (var->varattno > list_length(targetList))
	{
		return false;
	}

	tle = (TargetEntry *)list_nth(targetList, var->varattno -1);

	if (tle)
	{
		if (contain_subplans((Node*)tle->expr))
		{
			return false;
		}

		return true;
	}

	return false;
}

/*
 * qual_is_pushdown_safe - is a particular qual safe to push down?
 *
 * qual is a restriction clause applying to the given subquery (whose RTE
 * has index rti in the parent query).
 *
 * Conditions checked here:
 *
 * 1. The qual must not contain any subselects (mainly because I'm not sure
 * it will work correctly: sublinks will already have been transformed into
 * subplans in the qual, but not in the subquery).
 *
 * 2. If unsafeVolatile is set, the qual must not contain any volatile
 * functions.
 *
 * 3. If unsafeLeaky is set, the qual must not contain any leaky functions
 * that are passed Var nodes, and therefore might reveal values from the
 * subquery as side effects.
 *
 * 4. The qual must not refer to the whole-row output of the subquery
 * (since there is no easy way to name that within the subquery itself).
 *
 * 5. The qual must not refer to any subquery output columns that were
 * found to be unsafe to reference by subquery_is_pushdown_safe().
 * 
 * 6. The variables in the qual should not reference target entry of subquery
 * target list which contains subplan.
 */
static bool
qual_is_pushdown_safe(Query *subquery, Index rti, Node *qual,
					  pushdown_safety_info *safetyInfo)
{
	bool		safe = true;
	List	   *vars;
	ListCell   *vl;

	/* Refuse subselects (point 1) */
	if (contain_subplans(qual))
		return false;

	/* Refuse volatile quals if we found they'd be unsafe (point 2) */
	if (safetyInfo->unsafeVolatile &&
		contain_volatile_functions(qual))
		return false;

	/* Refuse leaky quals if told to (point 3) */
	if (safetyInfo->unsafeLeaky &&
		contain_leaked_vars(qual))
		return false;

	/* Refuse rownum expr (point 6) */
	if (contain_rownum(qual))
		return false;

	/*
	 * It would be unsafe to push down window function calls, but at least for
	 * the moment we could never see any in a qual anyhow.  (The same applies
	 * to aggregates, which we check for in pull_var_clause below.)
	 */
	Assert(!contain_window_function(qual));

	/*
	 * Examine all Vars used in clause; since it's a restriction clause, all
	 * such Vars must refer to subselect output columns.
	 */
	vars = pull_var_clause(qual, PVC_INCLUDE_PLACEHOLDERS);
	foreach(vl, vars)
	{
		Var		   *var = (Var *) lfirst(vl);

		/*
		 * XXX Punt if we find any PlaceHolderVars in the restriction clause.
		 * It's not clear whether a PHV could safely be pushed down, and even
		 * less clear whether such a situation could arise in any cases of
		 * practical interest anyway.  So for the moment, just refuse to push
		 * down.
		 */
		if (!IsA(var, Var))
		{
			safe = false;
			break;
		}

		if (safetyInfo->is_pushpred && var->varno != rti)
			continue;
		else
			Assert(var->varno == rti);

		Assert(var->varattno >= 0);

		/* Check point 4 */
		if (var->varattno == 0)
		{
			safe = false;
			break;
		}

		/* Check point 5 */
		if (safetyInfo->unsafeColumns[var->varattno])
		{
			safe = false;
			break;
		}

		/* Check point 6 */
		if (!check_target_list_subquery(var, subquery->targetList))
		{
			safe = false;
			break;
		}
	}

	list_free(vars);

	return safe;
}


/*
 * Helper routine to recurse through setOperations tree
 */
static void
recurse_push_qual(Node *setOp, Query *topquery,
				  RangeTblEntry *rte, Index rti, Node *qual, int filter)
{
	if (IsA(setOp, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) setOp;
		RangeTblEntry *subrte = rt_fetch(rtr->rtindex, topquery->rtable);
		Query	   *subquery = subrte->subquery;

		Assert(subquery != NULL);
		subquery_push_qual(subquery, rte, rti, qual, filter);
	}
	else if (IsA(setOp, SetOperationStmt))
	{
		SetOperationStmt *op = (SetOperationStmt *) setOp;

		recurse_push_qual(op->larg, topquery, rte, rti, qual, filter);
		recurse_push_qual(op->rarg, topquery, rte, rti, qual, filter);
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(setOp));
	}
}

/*****************************************************************************
 *			SIMPLIFYING SUBQUERY TARGETLISTS
 *****************************************************************************/

/*
 * remove_unused_subquery_outputs
 *		Remove subquery targetlist items we don't need
 *
 * It's possible, even likely, that the upper query does not read all the
 * output columns of the subquery.  We can remove any such outputs that are
 * not needed by the subquery itself (e.g., as sort/group columns) and do not
 * affect semantics otherwise (e.g., volatile functions can't be removed).
 * This is useful not only because we might be able to remove expensive-to-
 * compute expressions, but because deletion of output columns might allow
 * optimizations such as join removal to occur within the subquery.
 *
 * To avoid affecting column numbering in the targetlist, we don't physically
 * remove unused tlist entries, but rather replace their expressions with NULL
 * constants.  This is implemented by modifying subquery->targetList.
 */
static void
remove_unused_subquery_outputs(Query *subquery, RelOptInfo *rel)
{
	Bitmapset  *attrs_used = NULL;
	ListCell   *lc;

	/*
	 * Do nothing if subquery has UNION/INTERSECT/EXCEPT: in principle we
	 * could update all the child SELECTs' tlists, but it seems not worth the
	 * trouble presently.
	 */
	if (subquery->setOperations)
		return;

	/*
	 * If subquery has regular DISTINCT (not DISTINCT ON), we're wasting our
	 * time: all its output columns must be used in the distinctClause.
	 */
	if (subquery->distinctClause && !subquery->hasDistinctOn)
		return;

	/*
	 * Collect a bitmap of all the output column numbers used by the upper
	 * query.
	 *
	 * Add all the attributes needed for joins or final output.  Note: we must
	 * look at rel's targetlist, not the attr_needed data, because attr_needed
	 * isn't computed for inheritance child rels, cf set_append_rel_size().
	 * (XXX might be worth changing that sometime.)
	 */
	pull_varattnos((Node *) rel->reltarget->exprs, rel->relid, &attrs_used);

	/* Add all the attributes used by un-pushed-down restriction clauses. */
	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, rel->relid, &attrs_used);
	}

	/*
	 * If there's a whole-row reference to the subquery, we can't remove
	 * anything.
	 */
	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used))
		return;

	/*
	 * Run through the tlist and zap entries we don't need.  It's okay to
	 * modify the tlist items in-place because set_subquery_pathlist made a
	 * copy of the subquery.
	 */
	foreach(lc, subquery->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Node	   *texpr = (Node *) tle->expr;

		/*
		 * If it has a sortgroupref number, it's used in some sort/group
		 * clause so we'd better not remove it.  Also, don't remove any
		 * resjunk columns, since their reason for being has nothing to do
		 * with anybody reading the subquery's output.  (It's likely that
		 * resjunk columns in a sub-SELECT would always have ressortgroupref
		 * set, but even if they don't, it seems imprudent to remove them.)
		 */
		if (tle->ressortgroupref || tle->resjunk)
			continue;

		/*
		 * If it's used by the upper query, we can't remove it.
		 */
		if (bms_is_member(tle->resno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
			continue;

		/*
		 * If it contains a set-returning function, we can't remove it since
		 * that could change the number of rows returned by the subquery.
		 */
		if (subquery->hasTargetSRFs &&
			expression_returns_set(texpr))
			continue;

		/*
		 * If it contains volatile functions, we daren't remove it for fear
		 * that the user is expecting their side-effects to happen.
		 */
		if (contain_volatile_functions(texpr))
			continue;

		/*
		 * OK, we don't need it.  Replace the expression with a NULL constant.
		 * Preserve the exposed type of the expression, in case something
		 * looks at the rowtype of the subquery's result.
		 */
		tle->expr = (Expr *) makeNullConst(exprType(texpr),
										   exprTypmod(texpr),
										   exprCollation(texpr));
	}
}

/*
 * create_partial_bitmap_paths
 *	  Build partial bitmap heap path for the relation
 */
void
create_partial_bitmap_paths(PlannerInfo *root, RelOptInfo *rel,
							Path *bitmapqual)
{
	int			parallel_workers;
	double		pages_fetched;

	/* Compute heap pages for bitmap heap scan */
	pages_fetched = compute_bitmap_pages(root, rel, bitmapqual, 1.0,
										 NULL, NULL);

	parallel_workers = compute_parallel_worker(rel, pages_fetched, -1);

	if (parallel_workers <= 0)
		return;

	add_partial_path(rel, (Path *) create_bitmap_heap_path(root, rel,
														   bitmapqual, rel->lateral_relids, 1.0, parallel_workers));
}

/*
 * Compute the number of parallel workers that should be used to scan a
 * relation.  We compute the parallel workers based on the size of the heap to
 * be scanned and the size of the index to be scanned, then choose a minimum
 * of those.
 *
 * "heap_pages" is the number of pages from the table that we expect to scan, or
 * -1 if we don't expect to scan any.
 *
 * "index_pages" is the number of pages from the index that we expect to scan, or
 * -1 if we don't expect to scan any.
 */
int
compute_parallel_worker(RelOptInfo *rel, double heap_pages, double index_pages)
{
	int			parallel_workers = 0;

	/*
	 * If the user has set the parallel_workers reloption, use that; otherwise
	 * select a default number of workers.
	 */
	if (rel->rel_parallel_workers != -1)
		parallel_workers = rel->rel_parallel_workers;
	else
	{
		if (parallel_dml_tuple_num_threshold > 0 && planning_dml_sql)
			return max_parallel_workers_per_gather;
		/*
		 * If the number of pages being scanned is insufficient to justify a
		 * parallel scan, just return zero ... unless it's an inheritance
		 * child. In that case, we want to generate a parallel path here
		 * anyway.  It might not be worthwhile just for this relation, but
		 * when combined with all of its inheritance siblings it may well pay
		 * off.
		 */
		if ((rel->reloptkind == RELOPT_BASEREL && !IS_PARTITIONED_REL(rel)) &&
			((heap_pages >= 0 && heap_pages < min_parallel_table_scan_size) ||
			 (index_pages >= 0 && index_pages < min_parallel_index_scan_size)))
			return 0;

		if (heap_pages >= 0)
		{
			int			heap_parallel_threshold;
			int			heap_parallel_workers = 1;

			/*
			 * Select the number of workers based on the log of the size of
			 * the relation.  This probably needs to be a good deal more
			 * sophisticated, but we need something here for now.  Note that
			 * the upper limit of the min_parallel_table_scan_size GUC is
			 * chosen to prevent overflow here.
			 */
			heap_parallel_threshold = Max(min_parallel_table_scan_size, 1);
			while (heap_pages >= (BlockNumber) (heap_parallel_threshold * 3))
			{
				heap_parallel_workers++;
				heap_parallel_threshold *= 3;
				if (heap_parallel_threshold > INT_MAX / 3)
					break;		/* avoid overflow */
			}

			parallel_workers = heap_parallel_workers;
		}

		if (index_pages >= 0)
		{
			int			index_parallel_workers = 1;
			int			index_parallel_threshold;

			/* same calculation as for heap_pages above */
			index_parallel_threshold = Max(min_parallel_index_scan_size, 1);
			while (index_pages >= (BlockNumber) (index_parallel_threshold * 3))
			{
				index_parallel_workers++;
				index_parallel_threshold *= 3;
				if (index_parallel_threshold > INT_MAX / 3)
					break;		/* avoid overflow */
			}

			if (parallel_workers > 0)
				parallel_workers = Min(parallel_workers, index_parallel_workers);
			else
				parallel_workers = index_parallel_workers;
		}
	}

	/*
	 * In no case use more than max_parallel_workers_per_gather workers.
	 */
	parallel_workers = Min(parallel_workers, max_parallel_workers_per_gather);

	return parallel_workers;
}

/*
 * generate_partitionwise_join_paths
 * 		Create paths representing partitionwise join for given partitioned
 * 		join relation.
 *
 * This must not be called until after we are done adding paths for all
 * child-joins. Otherwise, add_path might delete a path to which some path
 * generated here has a reference.
 */
void
generate_partitionwise_join_paths(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *live_children = NIL;
	int			cnt_parts;
	int			num_parts;
	RelOptInfo **part_rels;

	/* Handle only join relations here. */
	if (!IS_JOIN_REL(rel))
		return;

    /* We've nothing to do if the relation is not partitioned. */
    if (!IS_PARTITIONED_REL(rel))
		return;

	/* The relation should have consider_partitionwise_join set. */
	Assert(rel->consider_partitionwise_join);

	/* Guard against stack overflow due to overly deep partition hierarchy. */
	check_stack_depth();

	num_parts = rel->nparts;
	part_rels = rel->part_rels;

	/* Collect non-dummy child-joins. */
	for (cnt_parts = 0; cnt_parts < num_parts; cnt_parts++)
	{
		RelOptInfo *child_rel = part_rels[cnt_parts];

		/* If it's been pruned entirely, it's certainly dummy. */
		if (child_rel == NULL)
			continue;

		/* Add partitionwise join paths for partitioned child-joins. */
		generate_partitionwise_join_paths(root, child_rel);

		/* Dummy children will not be scanned, so ingore those. */
		if (IS_DUMMY_REL(child_rel))
			continue;

		set_cheapest(child_rel);

#ifdef OPTIMIZER_DEBUG
		debug_print_rel(root, child_rel);
#endif

		live_children = lappend(live_children, child_rel);
	}

	/* If all child-joins are dummy, parent join is also dummy. */
	if (!live_children)
	{
		mark_dummy_rel(rel);
		return;
	}

	/* Build additional paths for this rel from child-join paths. */
	add_paths_to_append_rel(root, rel, live_children);
	list_free(live_children);
}

/*
 * set_parent_eq_class
 *	set information for parent equal class
 *
 * 1. Whether have const from subquery
 * 2. Clause list from subquery
 */
static void
set_parent_eq_class(PlannerInfo *root, AttrNumber target_attrNumber,
					bool set_const, List *rinfo_list)
{
	ListCell	*lc1;
	ListCell	*lc2;
	int			 rte_seq = root->parse->rte_array_id;

	/*
	 * Check GUC for data skew
	 */
	if (data_skew_option <= 3)
		return;

	if (rte_seq < 1)
		return;

	if (set_const == false && rinfo_list == NULL)
		return;

	/*
	 * Loop set const and claus list
	 */
	foreach(lc1, root->parent_root->eq_classes)
	{
		EquivalenceClass *eqclass = (EquivalenceClass*) lfirst(lc1);

		/*
		 * Avoid single column equal clas which is not used for join
		 */
		if (list_length(eqclass->ec_members) <= 1)
			continue;

		foreach(lc2, eqclass->ec_members)
		{
			EquivalenceMember *eqmember = (EquivalenceMember*) lfirst(lc2);

			if (eqmember->em_expr != NULL &&
				IsA(eqmember->em_expr, Var))
			{
				Var *var_eqmember = (Var*) eqmember->em_expr;

				if (var_eqmember->varno == rte_seq &&
					var_eqmember->varattno == target_attrNumber)
				{
					eqclass->ec_has_const_from_subquery = set_const;
					eqclass->ec_rinfo_list = list_union_ptr(eqclass->ec_rinfo_list, rinfo_list);
				}
			}
		}
	}
}

/*
 * get_basic_filter_list
 *	Get clause list for equal class from base relations
 *
 * Algorithm :
 * 	1. Loop through the relations in the equal class
 *  2. Get the single RelOptInfo for the relation.
 *  3. Loop baserestrictinfo in the RelOptInfo
 */
static List*
get_basic_filter_list(PlannerInfo *root, EquivalenceClass *eqclass)
{
	ListCell	*lc;
	RelOptInfo	*rel;
	OpExpr   	*opclause;
	Node		*other;
	FmgrInfo	 opproc;
	int			 relid = 0;
	bool		 varonleft;
	VariableStatData vardata;
	List		*rinfo_list = NULL;

	if (eqclass == NULL)
		return NULL;

	rinfo_list = eqclass->ec_rinfo_list;

	/*
	 * Loop through the relations in the equal class
	 */
	while ((relid = bms_next_member(eqclass->ec_relids, relid)) >= 0)
	{
		/*
		 * Check the relation is in the simple_rel_array
		 */
		if (relid >= root->simple_rel_array_size)
			break;

		/*
		 * Get the RelOptInfo for relation
		 */
		rel = root->simple_rel_array[relid];
		if (rel == NULL || rel->baserestrictinfo == NULL)
			continue;

		/*
		 * Loop through clause list
		 */
		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (!IsA(rinfo->clause, OpExpr))
				continue;

			opclause = (OpExpr *) rinfo->clause;

			memset(&vardata, 0, sizeof(vardata));
			fmgr_info(get_opcode(opclause->opno), &opproc);

			/*
			* If expression is not variable op something or something op variable,
			* then punt and return a default estimate.
			*/
			if (!get_restriction_variable(root, opclause->args, 0,
										&vardata, &other, &varonleft))
			{
				continue;
			}

			if (vardata.var == NULL || !IsA(vardata.var, Var))
			{
				ReleaseVariableStats(vardata);
				continue;
			}

			/*
			* Can't do anything useful if the something is not a constant, either.
			*/
			if (!IsA(other, Const))
			{
				ReleaseVariableStats(vardata);
				continue;
			}

			/*
			* If the constant is NULL, assume operator is strict and return zero, ie,
			* operator will never return TRUE.
			*/
			if (((Const *) other)->constisnull)
			{
				ReleaseVariableStats(vardata);
				continue;
			}

			rinfo_list = list_append_unique_ptr(rinfo_list, rinfo);

			ReleaseVariableStats(vardata);
		}
	}

	return rinfo_list;
}

/*
 * set_eqclass_information_from_subquery
 *	Set equal calss information for the equal classes of parent query block
 *
 * There are 2 information would be set:
 *  1. Whether subquery equal class has const to pass to equal class in
 *     parent query block.
 *  2. Pass clause list to parent query block.
 */
static void
set_eqclass_information_from_subquery(PlannerInfo *root)
{
	ListCell	*lc1;
	ListCell	*lc2;
	ListCell	*lc3;
	bool		 set_const = false;

	if (root->parse->targetList == NULL ||
	    root->eq_classes == NULL)
		return;

	/*
	 * Build up clause list
	 */
	foreach (lc1, root->eq_classes)
	{
		EquivalenceClass *eqclass = (EquivalenceClass*) lfirst(lc1);
		eqclass->ec_rinfo_list = get_basic_filter_list(root, eqclass);
	}

	/*
	 * Loop target list to check whether to set information for parent query block
	 */
	foreach(lc1, root->parse->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(lc1);

		if (targetEntry->expr == NULL)
			continue;

		if (!IsA(targetEntry->expr, Var))
			continue;

		foreach(lc2, root->eq_classes)
		{
			EquivalenceClass *eqclass = (EquivalenceClass*) lfirst(lc2);

			set_const = eqclass->ec_has_const || eqclass->ec_has_const_from_subquery;

			if (set_const == false && eqclass->ec_rinfo_list == NULL)
				continue;

			foreach(lc3, eqclass->ec_members)
			{
				EquivalenceMember *eqmember = (EquivalenceMember*) lfirst(lc3);

				if (eqmember->em_expr != NULL &&
					IsA(eqmember->em_expr, Var))
				{
					Var *var_target = (Var*) targetEntry->expr;
					Var *var_eqmember = (Var*) eqmember->em_expr;

					if (var_target->varno == var_eqmember->varno &&
					    var_target->varattno == var_eqmember->varattno)
					{
						set_parent_eq_class(root, targetEntry->resno,
											set_const, eqclass->ec_rinfo_list);
						break;
					}
				}
			}
		}
	}
}

/*****************************************************************************
 *			DEBUG SUPPORT
 *****************************************************************************/

#ifdef OPTIMIZER_DEBUG

static void
print_relids(PlannerInfo *root, Relids relids)
{
	int			x;
	bool		first = true;

	x = -1;
	while ((x = bms_next_member(relids, x)) >= 0)
	{
		if (!first)
			printf(" ");
		if (x < root->simple_rel_array_size &&
			root->simple_rte_array[x])
			printf("%s", root->simple_rte_array[x]->eref->aliasname);
		else
			printf("%d", x);
		first = false;
	}
}

static void
print_restrictclauses(PlannerInfo *root, List *clauses)
{
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *c = lfirst(l);

		print_expr((Node *) c->clause, root->parse->rtable);
		if (lnext(l))
			printf(", ");
	}
}

static void
print_path(PlannerInfo *root, Path *path, int indent)
{
	const char *ptype;
	bool		join = false;
	Path	   *subpath = NULL;
	int			i;

	switch (nodeTag(path))
	{
		case T_Path:
			switch (path->pathtype)
			{
				case T_SeqScan:
					ptype = "SeqScan";
					break;
				case T_SampleScan:
					ptype = "SampleScan";
					break;
				case T_SubqueryScan:
					ptype = "SubqueryScan";
					break;
				case T_FunctionScan:
					ptype = "FunctionScan";
					break;
				case T_TableFuncScan:
					ptype = "TableFuncScan";
					break;
				case T_ValuesScan:
					ptype = "ValuesScan";
					break;
				case T_CteScan:
					ptype = "CteScan";
					break;
				case T_WorkTableScan:
					ptype = "WorkTableScan";
					break;
				default:
					ptype = "???Path";
					break;
			}
			break;
		case T_IndexPath:
			ptype = "IdxScan";
			break;
		case T_BitmapHeapPath:
			ptype = "BitmapHeapScan";
			break;
		case T_BitmapAndPath:
			ptype = "BitmapAndPath";
			break;
		case T_BitmapOrPath:
			ptype = "BitmapOrPath";
			break;
		case T_TidPath:
			ptype = "TidScan";
			break;
		case T_SubqueryScanPath:
			ptype = "SubqueryScanScan";
			break;
		case T_ForeignPath:
			ptype = "ForeignScan";
			break;
		case T_AppendPath:
			ptype = "Append";
			break;
		case T_MergeAppendPath:
			ptype = "MergeAppend";
			break;
		case T_ResultPath:
			ptype = "Result";
			break;
		case T_MaterialPath:
			ptype = "Material";
			subpath = ((MaterialPath *) path)->subpath;
			break;
		case T_UniquePath:
			ptype = "Unique";
			subpath = ((UniquePath *) path)->subpath;
			break;
		case T_GatherPath:
			ptype = "Gather";
			subpath = ((GatherPath *) path)->subpath;
			break;
		case T_ProjectionPath:
			ptype = "Projection";
			subpath = ((ProjectionPath *) path)->subpath;
			break;
		case T_ProjectSetPath:
			ptype = "ProjectSet";
			subpath = ((ProjectSetPath *) path)->subpath;
			break;
		case T_QualPath:
			ptype = "Qualification";
			subpath = ((QualPath *) path)->subpath;
			break;
		case T_SortPath:
			ptype = "Sort";
			subpath = ((SortPath *) path)->subpath;
			break;
		case T_GroupPath:
			ptype = "Group";
			subpath = ((GroupPath *) path)->subpath;
			break;
		case T_UpperUniquePath:
			ptype = "UpperUnique";
			subpath = ((UpperUniquePath *) path)->subpath;
			break;
		case T_AggPath:
			ptype = "Agg";
			subpath = ((AggPath *) path)->subpath;
			break;
		case T_GroupingSetsPath:
			ptype = "GroupingSets";
			subpath = ((GroupingSetsPath *) path)->subpath;
			break;
		case T_MinMaxAggPath:
			ptype = "MinMaxAgg";
			break;
		case T_WindowAggPath:
			ptype = "WindowAgg";
			subpath = ((WindowAggPath *) path)->subpath;
			break;
		case T_SetOpPath:
			ptype = "SetOp";
			subpath = ((SetOpPath *) path)->subpath;
			break;
		case T_RecursiveUnionPath:
			ptype = "RecursiveUnion";
			break;
		case T_LockRowsPath:
			ptype = "LockRows";
			subpath = ((LockRowsPath *) path)->subpath;
			break;
		case T_ModifyTablePath:
			ptype = "ModifyTable";
			break;
		case T_LimitPath:
			ptype = "Limit";
			subpath = ((LimitPath *) path)->subpath;
			break;
		case T_NestPath:
			ptype = "NestLoop";
			join = true;
			break;
		case T_MergePath:
			ptype = "MergeJoin";
			join = true;
			break;
		case T_HashPath:
			ptype = "HashJoin";
			join = true;
			break;
		case T_ConnectByPath:
			ptype = "ConnectBy";
			break;
		default:
			ptype = "???Path";
			break;
	}

	for (i = 0; i < indent; i++)
		printf("\t");
	printf("%s", ptype);

	if (path->parent)
	{
		printf("(");
		print_relids(root, path->parent->relids);
		printf(")");
	}
	if (path->param_info)
	{
		printf(" required_outer (");
		print_relids(root, path->param_info->ppi_req_outer);
		printf(")");
	}
	printf(" rows=%.0f cost=%.2f..%.2f\n",
		   path->rows, path->startup_cost, path->total_cost);

	if (path->pathkeys)
	{
		for (i = 0; i < indent; i++)
			printf("\t");
		printf("  pathkeys: ");
		print_pathkeys(path->pathkeys, root->parse->rtable);
	}

	if (join)
	{
		JoinPath   *jp = (JoinPath *) path;

		for (i = 0; i < indent; i++)
			printf("\t");
		printf("  clauses: ");
		print_restrictclauses(root, jp->joinrestrictinfo);
		printf("\n");

		if (IsA(path, MergePath))
		{
			MergePath  *mp = (MergePath *) path;

			for (i = 0; i < indent; i++)
				printf("\t");
			printf("  sortouter=%d sortinner=%d materializeinner=%d\n",
				   ((mp->outersortkeys) ? 1 : 0),
				   ((mp->innersortkeys) ? 1 : 0),
				   ((mp->materialize_inner) ? 1 : 0));
		}

		print_path(root, jp->outerjoinpath, indent + 1);
		print_path(root, jp->innerjoinpath, indent + 1);
	}

	if (subpath)
		print_path(root, subpath, indent + 1);
}

void
debug_print_rel(PlannerInfo *root, RelOptInfo *rel)
{
	ListCell   *l;

	printf("RELOPTINFO (");
	print_relids(root, rel->relids);
	printf("): rows=%.0f width=%d\n", rel->rows, rel->reltarget->width);

	if (rel->baserestrictinfo)
	{
		printf("\opentenbaserestrictinfo: ");
		print_restrictclauses(root, rel->baserestrictinfo);
		printf("\n");
	}

	if (rel->joininfo)
	{
		printf("\tjoininfo: ");
		print_restrictclauses(root, rel->joininfo);
		printf("\n");
	}

	printf("\tpath list:\n");
	foreach(l, rel->pathlist)
		print_path(root, lfirst(l), 1);
	if (rel->cheapest_parameterized_paths)
	{
		printf("\n\tcheapest parameterized paths:\n");
		foreach(l, rel->cheapest_parameterized_paths)
			print_path(root, lfirst(l), 1);
	}
	if (rel->cheapest_startup_path)
	{
		printf("\n\tcheapest startup path:\n");
		print_path(root, rel->cheapest_startup_path, 1);
	}
	if (rel->cheapest_total_path)
	{
		printf("\n\tcheapest total path:\n");
		print_path(root, rel->cheapest_total_path, 1);
	}
	printf("\n");
	fflush(stdout);
}

#endif							/* OPTIMIZER_DEBUG */
