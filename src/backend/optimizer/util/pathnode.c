/*-------------------------------------------------------------------------
 *
 * pathnode.c
 *	  Routines to manipulate pathlists and create path nodes
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/pathnode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_type.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"
#include "rewrite/rewriteManip.h"

#ifdef XCP
#include "access/heapam.h"
#include "nodes/makefuncs.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "utils/rel.h"
#ifdef __OPENTENBASE__
#include "optimizer/distribution.h"
#include "optimizer/planner.h"
#include "optimizer/pgxcship.h"
#include "pgxc/groupmgr.h"
#include "optimizer/subselect.h"
#include "pgxc/pgxcnode.h"
#endif
#ifdef _MIGRATE_
#include "catalog/pgxc_class.h"
#endif
#ifdef __OPENTENBASE_C__
#include "catalog/partition.h"
#include "executor/execFragment.h"
#include "optimizer/prep.h"
#include "optimizer/materialinfo.h"
#endif
#endif

#ifdef __OPENTENBASE__
/*GUC parameter */
/* Max replication level on join to make Query more efficient */
int replication_level;
/* Support fast query shipping for subquery */
bool enable_subquery_shipping = false;

double replication_threshold = 1;

double distinct_pushdown_factor = 0.2;

#define  REPLICATION_FACTOR 0.8
#endif


typedef enum
{
	COSTS_EQUAL,				/* path costs are fuzzily equal */
	COSTS_BETTER1,				/* first path is cheaper than second */
	COSTS_BETTER2,				/* second path is cheaper than first */
	COSTS_DIFFERENT				/* neither path dominates the other on cost */
} PathCostComparison;

/*
 * STD_FUZZ_FACTOR is the normal fuzz factor for compare_path_costs_fuzzily.
 * XXX is it worth making this user-controllable?  It provides a tradeoff
 * between planner runtime and the accuracy of path cost comparisons.
 */
#define STD_FUZZ_FACTOR 1.01

static List *translate_sub_tlist(List *tlist, int relid);
static int	append_total_cost_compare(const void *a, const void *b);
static int	append_startup_cost_compare(const void *a, const void *b);
static List *reparameterize_pathlist_by_child(PlannerInfo *root,
								 List *pathlist,
								 RelOptInfo *child_rel);

#ifdef XCP
static Path *redistribute_path(PlannerInfo *root, Path *subpath, List *pathkeys,
							   char distributionType,
#ifdef __OPENTENBASE_C__
							   int nExprs, Node **disExprs,
#endif
							   Bitmapset *nodes, Bitmapset *restrictNodes);
extern void PoolPingNodes(void);
#endif
#ifdef __OPENTENBASE__
static int calcDistReplications(Distribution *dist, Path *subpath);
static void set_uniquepath_distribution(PlannerInfo *root, UniquePath *path);
#endif
static PathCostComparison compare_path_costs_by_rules_fuzzily(Path *path1, Path *path2);
static void adjust_join_restrict(PlannerInfo *root, JoinPath *path);
extern char* g_index_table_part_name;

/*
 * guc for data skew option
 * 0: disable data skew
 * 1: only enable roundrobin
 * 2: only enable partial broadcast
 * 3: enable all
 */
int data_skew_option = 0;

/*****************************************************************************
 *		MISC. PATH UTILITIES
 *****************************************************************************/

/*
 * compare_path_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for the specified criterion.
 */
int
compare_path_costs(Path *path1, Path *path2, CostSelector criterion)
{
	if (criterion == STARTUP_COST)
	{
		if (path1->startup_cost < path2->startup_cost)
			return -1;
		if (path1->startup_cost > path2->startup_cost)
			return +1;

		/*
		 * If paths have the same startup cost (not at all unlikely), order
		 * them by total cost.
		 */
		if (path1->total_cost < path2->total_cost)
			return -1;
		if (path1->total_cost > path2->total_cost)
			return +1;
	}
	else
	{
		if (path1->total_cost < path2->total_cost)
			return -1;
		if (path1->total_cost > path2->total_cost)
			return +1;

		/*
		 * If paths have the same total cost, order them by startup cost.
		 */
		if (path1->startup_cost < path2->startup_cost)
			return -1;
		if (path1->startup_cost > path2->startup_cost)
			return +1;
	}
	return 0;
}

/*
 * compare_path_fractional_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for fetching the specified fraction
 *	  of the total tuples.
 *
 * If fraction is <= 0 or > 1, we interpret it as 1, ie, we select the
 * path with the cheaper total_cost.
 */
int
compare_fractional_path_costs(Path *path1, Path *path2,
							  double fraction)
{
	Cost		cost1,
				cost2;

	if (fraction <= 0.0 || fraction >= 1.0)
		return compare_path_costs(path1, path2, TOTAL_COST);
	cost1 = path1->startup_cost +
		fraction * (path1->total_cost - path1->startup_cost);
	cost2 = path2->startup_cost +
		fraction * (path2->total_cost - path2->startup_cost);
	if (cost1 < cost2)
		return -1;
	if (cost1 > cost2)
		return +1;
	return 0;
}

/*
 * compare_path_costs_fuzzily
 *	  Compare the costs of two paths to see if either can be said to
 *	  dominate the other.
 *
 * We use fuzzy comparisons so that add_path() can avoid keeping both of
 * a pair of paths that really have insignificantly different cost.
 *
 * The fuzz_factor argument must be 1.0 plus delta, where delta is the
 * fraction of the smaller cost that is considered to be a significant
 * difference.  For example, fuzz_factor = 1.01 makes the fuzziness limit
 * be 1% of the smaller cost.
 *
 * The two paths are said to have "equal" costs if both startup and total
 * costs are fuzzily the same.  Path1 is said to be better than path2 if
 * it has fuzzily better startup cost and fuzzily no worse total cost,
 * or if it has fuzzily better total cost and fuzzily no worse startup cost.
 * Path2 is better than path1 if the reverse holds.  Finally, if one path
 * is fuzzily better than the other on startup cost and fuzzily worse on
 * total cost, we just say that their costs are "different", since neither
 * dominates the other across the whole performance spectrum.
 *
 * This function also enforces a policy rule that paths for which the relevant
 * one of parent->consider_startup and parent->consider_param_startup is false
 * cannot survive comparisons solely on the grounds of good startup cost, so
 * we never return COSTS_DIFFERENT when that is true for the total-cost loser.
 * (But if total costs are fuzzily equal, we compare startup costs anyway,
 * in hopes of eliminating one path or the other.)
 */
static PathCostComparison
compare_path_costs_fuzzily(Path *path1, Path *path2, double fuzz_factor)
{
#define CONSIDER_PATH_STARTUP_COST(p)  \
	((p)->param_info == NULL ? (p)->parent->consider_startup : (p)->parent->consider_param_startup)

	/*
	 * Check total cost first since it's more likely to be different; many
	 * paths have zero startup cost.
	 */
	if (path1->total_cost > path2->total_cost * fuzz_factor)
	{
		/* path1 fuzzily worse on total cost */
		if (CONSIDER_PATH_STARTUP_COST(path1) &&
			path2->startup_cost > path1->startup_cost * fuzz_factor)
		{
			/* ... but path2 fuzzily worse on startup, so DIFFERENT */
			return COSTS_DIFFERENT;
		}
		/* else path2 dominates */
		return COSTS_BETTER2;
	}
	if (path2->total_cost > path1->total_cost * fuzz_factor)
	{
		/* path2 fuzzily worse on total cost */
		if (CONSIDER_PATH_STARTUP_COST(path2) &&
			path1->startup_cost > path2->startup_cost * fuzz_factor)
		{
			/* ... but path1 fuzzily worse on startup, so DIFFERENT */
			return COSTS_DIFFERENT;
		}
		/* else path1 dominates */
		return COSTS_BETTER1;
	}
	/* fuzzily the same on total cost ... */
	if (path1->startup_cost > path2->startup_cost * fuzz_factor)
	{
		/* ... but path1 fuzzily worse on startup, so path2 wins */
		return COSTS_BETTER2;
	}
	if (path2->startup_cost > path1->startup_cost * fuzz_factor)
	{
		/* ... but path2 fuzzily worse on startup, so path1 wins */
		return COSTS_BETTER1;
	}
	/* fuzzily the same on both costs */
	return COSTS_EQUAL;

#undef CONSIDER_PATH_STARTUP_COST
}

/*
 * set_cheapest
 *	  Find the minimum-cost paths from among a relation's paths,
 *	  and save them in the rel's cheapest-path fields.
 *
 * cheapest_total_path is normally the cheapest-total-cost unparameterized
 * path; but if there are no unparameterized paths, we assign it to be the
 * best (cheapest least-parameterized) parameterized path.  However, only
 * unparameterized paths are considered candidates for cheapest_startup_path,
 * so that will be NULL if there are no unparameterized paths.
 *
 * The cheapest_parameterized_paths list collects all parameterized paths
 * that have survived the add_path() tournament for this relation.  (Since
 * add_path ignores pathkeys for a parameterized path, these will be paths
 * that have best cost or best row count for their parameterization.  We
 * may also have both a parallel-safe and a non-parallel-safe path in some
 * cases for the same parameterization in some cases, but this should be
 * relatively rare since, most typically, all paths for the same relation
 * will be parallel-safe or none of them will.)
 *
 * cheapest_parameterized_paths always includes the cheapest-total
 * unparameterized path, too, if there is one; the users of that list find
 * it more convenient if that's included.
 *
 * This is normally called only after we've finished constructing the path
 * list for the rel node.
 */
void
set_cheapest(RelOptInfo *parent_rel)
{
	Path	   *cheapest_startup_path;
	Path	   *cheapest_total_path;
	Path	   *best_param_path;
	List	   *parameterized_paths;
	ListCell   *p;

	Assert(IsA(parent_rel, RelOptInfo));

#ifdef __OPENTENBASE__
	/*
	 * When set_joinpath_distribution() adjusted the strategy for complex
	 * UPDATE/DELETE, the original paths could be give up caused by no proper
	 * distribution found. Which lead to an early error pop up here, thus
	 * we need to provide more accurate error message here. (Before the
	 * complex delete enhancement, this will pop up in group_planner at
	 * final stage.)
	 */
	if (parent_rel->pathlist == NIL &&
		parent_rel->resultRelLoc != RESULT_REL_NONE)
	{
#ifdef _PG_REGRESS_
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 errmsg("could not plan this distributed UPDATE/DELETE"),
					 errdetail("correlated or complex UPDATE/DELETE is currently not supported in Postgres-XL.")));
#else
			ereport(ERROR,
					(errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
					 errmsg("could not plan this distributed UPDATE/DELETE"),
					 errdetail("correlated or complex UPDATE/DELETE is currently not supported in OpenTenBase.")));
#endif
	}
#endif

	if (parent_rel->pathlist == NIL)
		elog(ERROR, "could not devise a query plan for the given query");

	cheapest_startup_path = cheapest_total_path = best_param_path = NULL;
	parameterized_paths = NIL;

	foreach(p, parent_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(p);
		int			cmp;

		if (path->param_info)
		{
			/* Parameterized path, so add it to parameterized_paths */
			parameterized_paths = lappend(parameterized_paths, path);

			/*
			 * If we have an unparameterized cheapest-total, we no longer care
			 * about finding the best parameterized path, so move on.
			 */
			if (cheapest_total_path)
				continue;

			/*
			 * Otherwise, track the best parameterized path, which is the one
			 * with least total cost among those of the minimum
			 * parameterization.
			 */
			if (best_param_path == NULL)
				best_param_path = path;
			else
			{
				switch (bms_subset_compare(PATH_REQ_OUTER(path),
										   PATH_REQ_OUTER(best_param_path)))
				{
					case BMS_EQUAL:
						/* keep the cheaper one */
						if (compare_path_costs(path, best_param_path,
											   TOTAL_COST) < 0)
							best_param_path = path;
						break;
					case BMS_SUBSET1:
						/* new path is less-parameterized */
						best_param_path = path;
						break;
					case BMS_SUBSET2:
						/* old path is less-parameterized, keep it */
						break;
					case BMS_DIFFERENT:

						/*
						 * This means that neither path has the least possible
						 * parameterization for the rel.  We'll sit on the old
						 * path until something better comes along.
						 */
						break;
				}
			}
		}
		else
		{
			/* Unparameterized path, so consider it for cheapest slots */
			if (cheapest_total_path == NULL)
			{
				cheapest_startup_path = cheapest_total_path = path;
				continue;
			}

			/*
			 * If we find two paths of identical costs, try to keep the
			 * better-sorted one.  The paths might have unrelated sort
			 * orderings, in which case we can only guess which might be
			 * better to keep, but if one is superior then we definitely
			 * should keep that one.
			 */
			cmp = compare_path_costs(cheapest_startup_path, path, STARTUP_COST);
			if (cmp > 0 ||
				(cmp == 0 &&
				 compare_pathkeys(cheapest_startup_path->pathkeys,
								  path->pathkeys) == PATHKEYS_BETTER2))
				cheapest_startup_path = path;

			cmp = compare_path_costs(cheapest_total_path, path, TOTAL_COST);
			if (cmp > 0 ||
				(cmp == 0 &&
				 compare_pathkeys(cheapest_total_path->pathkeys,
								  path->pathkeys) == PATHKEYS_BETTER2))
				cheapest_total_path = path;
		}
	}

	/* Add cheapest unparameterized path, if any, to parameterized_paths */
	if (cheapest_total_path)
		parameterized_paths = lcons(cheapest_total_path, parameterized_paths);

	/*
	 * If there is no unparameterized path, use the best parameterized path as
	 * cheapest_total_path (but not as cheapest_startup_path).
	 */
	if (cheapest_total_path == NULL)
		cheapest_total_path = best_param_path;
	Assert(cheapest_total_path != NULL);

	parent_rel->cheapest_startup_path = cheapest_startup_path;
	parent_rel->cheapest_total_path = cheapest_total_path;
	parent_rel->cheapest_unique_path = NULL;	/* computed only if needed */
	parent_rel->cheapest_parameterized_paths = parameterized_paths;
}

/*
 * add_path
 *	  Consider a potential implementation path for the specified parent rel,
 *	  and add it to the rel's pathlist if it is worthy of consideration.
 *	  A path is worthy if it has a better sort order (better pathkeys) or
 *	  cheaper cost (on either dimension), or generates fewer rows, than any
 *	  existing path that has the same or superset parameterization rels.
 *	  We also consider parallel-safe paths more worthy than others.
 *
 *	  We also remove from the rel's pathlist any old paths that are dominated
 *	  by new_path --- that is, new_path is cheaper, at least as well ordered,
 *	  generates no more rows, requires no outer rels not required by the old
 *	  path, and is no less parallel-safe.
 *
 *	  In most cases, a path with a superset parameterization will generate
 *	  fewer rows (since it has more join clauses to apply), so that those two
 *	  figures of merit move in opposite directions; this means that a path of
 *	  one parameterization can seldom dominate a path of another.  But such
 *	  cases do arise, so we make the full set of checks anyway.
 *
 *	  There are two policy decisions embedded in this function, along with
 *	  its sibling add_path_precheck.  First, we treat all parameterized paths
 *	  as having NIL pathkeys, so that they cannot win comparisons on the
 *	  basis of sort order.  This is to reduce the number of parameterized
 *	  paths that are kept; see discussion in src/backend/optimizer/README.
 *
 *	  Second, we only consider cheap startup cost to be interesting if
 *	  parent_rel->consider_startup is true for an unparameterized path, or
 *	  parent_rel->consider_param_startup is true for a parameterized one.
 *	  Again, this allows discarding useless paths sooner.
 *
 *	  The pathlist is kept sorted by total_cost, with cheaper paths
 *	  at the front.  Within this routine, that's simply a speed hack:
 *	  doing it that way makes it more likely that we will reject an inferior
 *	  path after a few comparisons, rather than many comparisons.
 *	  However, add_path_precheck relies on this ordering to exit early
 *	  when possible.
 *
 *	  NOTE: discarded Path objects are immediately pfree'd to reduce planner
 *	  memory consumption.  We dare not try to free the substructure of a Path,
 *	  since much of it may be shared with other Paths or the query tree itself;
 *	  but just recycling discarded Path nodes is a very useful savings in
 *	  a large join tree.  We can recycle the List nodes of pathlist, too.
 *
 *	  As noted in optimizer/README, deleting a previously-accepted Path is
 *	  safe because we know that Paths of this rel cannot yet be referenced
 *	  from any other rel, such as a higher-level join.  However, in some cases
 *	  it is possible that a Path is referenced by another Path for its own
 *	  rel; we must not delete such a Path, even if it is dominated by the new
 *	  Path.  Currently this occurs only for IndexPath objects, which may be
 *	  referenced as children of BitmapHeapPaths as well as being paths in
 *	  their own right.  Hence, we don't pfree IndexPaths when rejecting them.
 *
 * 'parent_rel' is the relation entry to which the path corresponds.
 * 'new_path' is a potential path for parent_rel.
 *
 * Returns nothing, but modifies parent_rel->pathlist.
 */
void
add_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;	/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	List	   *new_path_pathkeys;
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

	/*
	 * This is a convenient place to check for query cancel --- no part of the
	 * planner goes very long without calling add_path().
	 */
	CHECK_FOR_INTERRUPTS();

#ifdef __OPENTENBASE__
	/*
	 * In case we skipped the join paths caused by invalid result rel
	 * distribution.
	 */
	if (!new_path)
		return;
	if (IS_GLOBAL_INDEX_PATH(new_path) && !g_index_table_part_name && IS_PGXC_DATANODE)
		return;
#endif

	/* Pretend parameterized paths have no pathkeys, per comment above */
	new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

	/*
	 * Loop to check proposed new path against old paths.  Note it is possible
	 * for more than one old path to be tossed out because new_path dominates
	 * it.
	 *
	 * We can't use foreach here because the loop body may delete the current
	 * list cell.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->pathlist); p1 != NULL; p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathCostComparison costcmp;
		PathKeysComparison keyscmp;
		BMS_Comparison outercmp;
		bool need_compare_by_costs = true;
		p1_next = lnext(p1);
		
		if (IS_GLOBAL_INDEX_PATH(new_path) || IS_GLOBAL_INDEX_PATH(old_path))
		{
			costcmp = compare_path_costs_by_rules_fuzzily(new_path, old_path);
			if (costcmp == COSTS_BETTER1)
			{
				need_compare_by_costs = false;
				accept_new = true;
				remove_old = true;
			}
			else if (costcmp == COSTS_BETTER2)
			{
				need_compare_by_costs = false;
				accept_new = false;
			}
		}
		if (!need_compare_by_costs)
			goto after_compare_cost;
		/*
		 * Do a fuzzy cost comparison with standard fuzziness limit.
		 */
		costcmp = compare_path_costs_fuzzily(new_path, old_path,
											 STD_FUZZ_FACTOR);

		/*
		 * If the two paths compare differently for startup and total cost,
		 * then we want to keep both, and we can skip comparing pathkeys and
		 * required_outer rels.  If they compare the same, proceed with the
		 * other comparisons.  Row count is checked last.  (We make the tests
		 * in this order because the cost comparison is most likely to turn
		 * out "different", and the pathkeys comparison next most likely.  As
		 * explained above, row count very seldom makes a difference, so even
		 * though it's cheap to compare there's not much point in checking it
		 * earlier.)
		 */
		if (costcmp != COSTS_DIFFERENT)
		{
			/* Similarly check to see if either dominates on pathkeys */
			List	   *old_path_pathkeys;

			old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
			keyscmp = compare_pathkeys(new_path_pathkeys,
									   old_path_pathkeys);
			if (keyscmp != PATHKEYS_DIFFERENT)
			{
				switch (costcmp)
				{
					case COSTS_EQUAL:
						outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
													  PATH_REQ_OUTER(old_path));
						if (keyscmp == PATHKEYS_BETTER1)
						{
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET1) &&
								new_path->rows <= old_path->rows &&
								new_path->parallel_safe >= old_path->parallel_safe)
								remove_old = true;	/* new dominates old */
						}
						else if (keyscmp == PATHKEYS_BETTER2)
						{
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET2) &&
								new_path->rows >= old_path->rows &&
								new_path->parallel_safe <= old_path->parallel_safe)
								accept_new = false; /* old dominates new */
						}
						else	/* keyscmp == PATHKEYS_EQUAL */
						{
							if (outercmp == BMS_EQUAL)
							{
								/*
								 * Same pathkeys and outer rels, and fuzzily
								 * the same cost, so keep just one; to decide
								 * which, first check parallel-safety, then
								 * rows, then do a fuzzy cost comparison with
								 * very small fuzz limit.  (We used to do an
								 * exact cost comparison, but that results in
								 * annoying platform-specific plan variations
								 * due to roundoff in the cost estimates.)	If
								 * things are still tied, arbitrarily keep
								 * only the old path.  Notice that we will
								 * keep only the old path even if the
								 * less-fuzzy comparison decides the startup
								 * and total costs compare differently.
								 */
								if (new_path->parallel_safe >
									old_path->parallel_safe)
									remove_old = true;	/* new dominates old */
								else if (new_path->parallel_safe <
										 old_path->parallel_safe)
									accept_new = false; /* old dominates new */
								else if (new_path->rows < old_path->rows)
									remove_old = true;	/* new dominates old */
								else if (new_path->rows > old_path->rows)
									accept_new = false; /* old dominates new */
								else if (compare_path_costs_fuzzily(new_path,
																	old_path,
																	1.0000000001) == COSTS_BETTER1)
									remove_old = true;	/* new dominates old */
								else
									accept_new = false; /* old equals or
														 * dominates new */
							}
							else if (outercmp == BMS_SUBSET1 &&
									 new_path->rows <= old_path->rows &&
									 new_path->parallel_safe >= old_path->parallel_safe)
								remove_old = true;	/* new dominates old */
							else if (outercmp == BMS_SUBSET2 &&
									 new_path->rows >= old_path->rows &&
									 new_path->parallel_safe <= old_path->parallel_safe)
								accept_new = false; /* old dominates new */
							/* else different parameterizations, keep both */
						}
						break;
					case COSTS_BETTER1:
						if (keyscmp != PATHKEYS_BETTER2)
						{
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
														  PATH_REQ_OUTER(old_path));
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET1) &&
								new_path->rows <= old_path->rows &&
								new_path->parallel_safe >= old_path->parallel_safe)
								remove_old = true;	/* new dominates old */
						}
						break;
					case COSTS_BETTER2:
						if (keyscmp != PATHKEYS_BETTER1)
						{
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
														  PATH_REQ_OUTER(old_path));
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET2) &&
								new_path->rows >= old_path->rows &&
								new_path->parallel_safe <= old_path->parallel_safe)
								accept_new = false; /* old dominates new */
						}
						break;
					case COSTS_DIFFERENT:

						/*
						 * can't get here, but keep this case to keep compiler
						 * quiet
						 */
						break;
				}
			}
		}
after_compare_cost:
		/*
		 * Remove current element from pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->pathlist = list_delete_cell(parent_rel->pathlist,
													p1, p1_prev);

			/*
			 * Delete the data pointed-to by the deleted cell, if possible
			 */
			if (!IsA(old_path, IndexPath))
				pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the pathlist; we will not add new_path, and we assume
		 * new_path cannot dominate any other elements of the pathlist.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place in pathlist */
		if (insert_after)
			lappend_cell(parent_rel->pathlist, insert_after, new_path);
		else
			parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
	}
	else
	{
		/* Reject and recycle the new path */
		if (!IsA(new_path, IndexPath))
			pfree(new_path);
	}
}

/*
 * add_path_precheck
 *	  Check whether a proposed new path could possibly get accepted.
 *	  We assume we know the path's pathkeys and parameterization accurately,
 *	  and have lower bounds for its costs.
 *
 * Note that we do not know the path's rowcount, since getting an estimate for
 * that is too expensive to do before prechecking.  We assume here that paths
 * of a superset parameterization will generate fewer rows; if that holds,
 * then paths with different parameterizations cannot dominate each other
 * and so we can simply ignore existing paths of another parameterization.
 * (In the infrequent cases where that rule of thumb fails, add_path will
 * get rid of the inferior path.)
 *
 * At the time this is called, we haven't actually built a Path structure,
 * so the required information has to be passed piecemeal.
 */
bool
add_path_precheck(RelOptInfo *parent_rel,
				  Cost startup_cost, Cost total_cost,
				  List *pathkeys, Relids required_outer)
{
	List	   *new_path_pathkeys;
	bool		consider_startup;
	ListCell   *p1;

	/* Pretend parameterized paths have no pathkeys, per add_path policy */
	new_path_pathkeys = required_outer ? NIL : pathkeys;

	/* Decide whether new path's startup cost is interesting */
	consider_startup = required_outer ? parent_rel->consider_param_startup : parent_rel->consider_startup;

	foreach(p1, parent_rel->pathlist)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		PathKeysComparison keyscmp;

		/*
		 * We are looking for an old_path with the same parameterization (and
		 * by assumption the same rowcount) that dominates the new path on
		 * pathkeys as well as both cost metrics.  If we find one, we can
		 * reject the new path.
		 *
		 * Cost comparisons here should match compare_path_costs_fuzzily.
		 */
		if (total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
		{
			/* new path can win on startup cost only if consider_startup */
			if (startup_cost > old_path->startup_cost * STD_FUZZ_FACTOR ||
				!consider_startup)
			{
				/* new path loses on cost, so check pathkeys... */
				List	   *old_path_pathkeys;

				old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
				keyscmp = compare_pathkeys(new_path_pathkeys,
										   old_path_pathkeys);
				if (keyscmp == PATHKEYS_EQUAL ||
					keyscmp == PATHKEYS_BETTER2)
				{
					/* new path does not win on pathkeys... */
					if (bms_equal(required_outer, PATH_REQ_OUTER(old_path)))
					{
						/* Found an old path that dominates the new one */
						return false;
					}
				}
			}
		}
		else
		{
			/*
			 * Since the pathlist is sorted by total_cost, we can stop looking
			 * once we reach a path with a total_cost larger than the new
			 * path's.
			 */
			break;
		}
	}

	return true;
}

/*
 * add_partial_path
 *	  Like add_path, our goal here is to consider whether a path is worthy
 *	  of being kept around, but the considerations here are a bit different.
 *	  A partial path is one which can be executed in any number of workers in
 *	  parallel such that each worker will generate a subset of the path's
 *	  overall result.
 *
 *	  As in add_path, the partial_pathlist is kept sorted with the cheapest
 *	  total path in front.  This is depended on by multiple places, which
 *	  just take the front entry as the cheapest path without searching.
 *
 *	  We don't generate parameterized partial paths for several reasons.  Most
 *	  importantly, they're not safe to execute, because there's nothing to
 *	  make sure that a parallel scan within the parameterized portion of the
 *	  plan is running with the same value in every worker at the same time.
 *	  Fortunately, it seems unlikely to be worthwhile anyway, because having
 *	  each worker scan the entire outer relation and a subset of the inner
 *	  relation will generally be a terrible plan.  The inner (parameterized)
 *	  side of the plan will be small anyway.  There could be rare cases where
 *	  this wins big - e.g. if join order constraints put a 1-row relation on
 *	  the outer side of the topmost join with a parameterized plan on the inner
 *	  side - but we'll have to be content not to handle such cases until
 *	  somebody builds an executor infrastructure that can cope with them.
 *
 *	  Because we don't consider parameterized paths here, we also don't
 *	  need to consider the row counts as a measure of quality: every path will
 *	  produce the same number of rows.  Neither do we need to consider startup
 *	  costs: parallelism is only used for plans that will be run to completion.
 *	  Therefore, this routine is much simpler than add_path: it needs to
 *	  consider only pathkeys and total cost.
 *
 *	  As with add_path, we pfree paths that are found to be dominated by
 *	  another partial path; this requires that there be no other references to
 *	  such paths yet.  Hence, GatherPaths must not be created for a rel until
 *	  we're done creating all partial paths for it.  Unlike add_path, we don't
 *	  take an exception for IndexPaths as partial index paths won't be
 *	  referenced by partial BitmapHeapPaths.
 */
void
add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;	/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

#ifdef __OPENTENBASE_C__
	/* TODO: parallel recv fragment is not supported on CN */
	if (IS_PGXC_COORDINATOR && !new_path->distribution)
	{
		pfree(new_path);
		return;
	}
#endif
	/* Check for query cancel. */
	CHECK_FOR_INTERRUPTS();

	/* Path to be added must be parallel safe. */
	Assert(new_path->parallel_safe);

	/* Relation should be OK for parallelism, too. */
	Assert(parent_rel->consider_parallel);

	/*
	 * As in add_path, throw out any paths which are dominated by the new
	 * path, but throw out the new path if some existing path dominates it.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->partial_pathlist); p1 != NULL;
		 p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathKeysComparison keyscmp;

		p1_next = lnext(p1);

		/* Compare pathkeys. */
		keyscmp = compare_pathkeys(new_path->pathkeys, old_path->pathkeys);

		/* Unless pathkeys are incompable, keep just one of the two paths. */
		if (keyscmp != PATHKEYS_DIFFERENT)
		{
			if (new_path->total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
			{
				/* New path costs more; keep it only if pathkeys are better. */
				if (keyscmp != PATHKEYS_BETTER1)
					accept_new = false;
			}
			else if (old_path->total_cost > new_path->total_cost
					 * STD_FUZZ_FACTOR)
			{
				/* Old path costs more; keep it only if pathkeys are better. */
				if (keyscmp != PATHKEYS_BETTER2)
					remove_old = true;
			}
			else if (keyscmp == PATHKEYS_BETTER1)
			{
				/* Costs are about the same, new path has better pathkeys. */
				remove_old = true;
			}
			else if (keyscmp == PATHKEYS_BETTER2)
			{
				/* Costs are about the same, old path has better pathkeys. */
				accept_new = false;
			}
			else if (old_path->total_cost > new_path->total_cost * 1.0000000001)
			{
				/* Pathkeys are the same, and the old path costs more. */
				remove_old = true;
			}
			else
			{
				/*
				 * Pathkeys are the same, and new path isn't materially
				 * cheaper.
				 */
				accept_new = false;
			}
		}

		/*
		 * Remove current element from partial_pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->partial_pathlist =
				list_delete_cell(parent_rel->partial_pathlist, p1, p1_prev);
			pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the partial_pathlist; we will not add new_path, and we
		 * assume new_path cannot dominate any later path.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place */
		if (insert_after)
			lappend_cell(parent_rel->partial_pathlist, insert_after, new_path);
		else
			parent_rel->partial_pathlist =
				lcons(new_path, parent_rel->partial_pathlist);
	}
	else
	{
		/* Reject and recycle the new path */
		pfree(new_path);
	}
}

/*
 * add_partial_path_precheck
 *	  Check whether a proposed new partial path could possibly get accepted.
 *
 * Unlike add_path_precheck, we can ignore startup cost and parameterization,
 * since they don't matter for partial paths (see add_partial_path).  But
 * we do want to make sure we don't add a partial path if there's already
 * a complete path that dominates it, since in that case the proposed path
 * is surely a loser.
 */
bool
add_partial_path_precheck(RelOptInfo *parent_rel, Cost total_cost,
						  List *pathkeys)
{
	ListCell   *p1;

	/*
	 * Our goal here is twofold.  First, we want to find out whether this path
	 * is clearly inferior to some existing partial path.  If so, we want to
	 * reject it immediately.  Second, we want to find out whether this path
	 * is clearly superior to some existing partial path -- at least, modulo
	 * final cost computations.  If so, we definitely want to consider it.
	 *
	 * Unlike add_path(), we always compare pathkeys here.  This is because we
	 * expect partial_pathlist to be very short, and getting a definitive
	 * answer at this stage avoids the need to call add_path_precheck.
	 */
	foreach(p1, parent_rel->partial_pathlist)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		PathKeysComparison keyscmp;

		keyscmp = compare_pathkeys(pathkeys, old_path->pathkeys);
		if (keyscmp != PATHKEYS_DIFFERENT)
		{
			if (total_cost > old_path->total_cost * STD_FUZZ_FACTOR &&
				keyscmp != PATHKEYS_BETTER1)
				return false;
			if (old_path->total_cost > total_cost * STD_FUZZ_FACTOR &&
				keyscmp != PATHKEYS_BETTER2)
				return true;
		}
	}

	/*
	 * This path is neither clearly inferior to an existing partial path nor
	 * clearly good enough that it might replace one.  Compare it to
	 * non-parallel plans.  If it loses even before accounting for the cost of
	 * the Gather node, we should definitely reject it.
	 *
	 * Note that we pass the total_cost to add_path_precheck twice.  This is
	 * because it's never advantageous to consider the startup cost of a
	 * partial path; the resulting plans, if run in parallel, will be run to
	 * completion.
	 */
	if (!add_path_precheck(parent_rel, total_cost, total_cost, pathkeys,
						   NULL))
		return false;

	return true;
}

#ifdef __OPENTENBASE_C__
/*
 * add_unmaterial_path
 *	  Like add_path, our goal here is to consider whether a path is worthy
 *	  of being kept around, but the considerations here are a bit different.
 *
 *	  A late materialized path is one which havn't been fully materialized. To
 *	  utilize these late materialized paths, the upper path need to build
 *	  Late Material and it's corresponding RID Scan node.
 *
 *	  As in add_path, the lm_pathlist is kept sorted with the cheapest
 *	  total path in front.  This is depended on by multiple places, which
 *	  just take the front entry as the cheapest path without searching.
 *
 *    We also need to keep both paths if there materialize status is different.
 *    This provides a more complete diversity of late materialized searching
 *    space.
 *
 *	  As with add_path, we pfree paths that are found to be dominated by
 *	  another un-materialized path; this requires that there be no other
 *	  references to such paths yet.  Hence, GatherPaths must not be created
 *	  for a rel until we're done creating all partial paths for it.  Unlike
 *	  add_path, we don't take an exception for IndexPaths as partial index
 *	  paths won't be referenced by partial BitmapHeapPaths.
 */
void
add_unmaterial_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;	/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	List	   *new_path_pathkeys;
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;


	/*
	 * This is a convenient place to check for query cancel --- no part of the
	 * planner goes very long without calling add_path().
	 */
	CHECK_FOR_INTERRUPTS();

	/* Pretend parameterized paths have no pathkeys, per comment above */
	new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

	/*
	 * Loop to check proposed new path against old paths.  Note it is possible
	 * for more than one old path to be tossed out because new_path dominates
	 * it.
	 *
	 * We can't use foreach here because the loop body may delete the current
	 * list cell.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->lm_pathlist); p1 != NULL; p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathCostComparison costcmp;
		PathKeysComparison keyscmp;
		BMS_Comparison outercmp;
		MaterialComparison umcmp;

		p1_next = lnext(p1);

		/*
		 * Do a fuzzy cost comparison with standard fuzziness limit.
		 */
		costcmp = compare_path_costs_fuzzily(new_path, old_path,
											 STD_FUZZ_FACTOR);

		/*
		 * If the two paths compare differently for startup and total cost,
		 * then we want to keep both, and we can skip comparing pathkeys and
		 * required_outer rels.  If they compare the same, proceed with the
		 * other comparisons.  Row count is checked last.  (We make the tests
		 * in this order because the cost comparison is most likely to turn
		 * out "different", and the pathkeys comparison next most likely.  As
		 * explained above, row count very seldom makes a difference, so even
		 * though it's cheap to compare there's not much point in checking it
		 * earlier.)
		 */
		if (costcmp != COSTS_DIFFERENT)
		{
			/* Similarly check to see if either dominates on pathkeys */
			List	   *old_path_pathkeys;

			old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
			keyscmp = compare_pathkeys(new_path_pathkeys,
									   old_path_pathkeys);

			/*
			 * Do a material status comparison, we keep both path if different
			 */
			umcmp = compare_material_status(parent_rel, new_path, old_path);

			if (umcmp != MATERIAL_EQUAL)
			{
				if (keyscmp != PATHKEYS_DIFFERENT)
				{
					switch (costcmp)
					{
						case COSTS_EQUAL:
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
														  PATH_REQ_OUTER(old_path));
							if (keyscmp == PATHKEYS_BETTER1)
							{
								if ((outercmp == BMS_EQUAL ||
									 outercmp == BMS_SUBSET1) &&
									new_path->rows <= old_path->rows &&
									new_path->parallel_safe >= old_path->parallel_safe)
									remove_old = true;	/* new dominates old */
							}
							else if (keyscmp == PATHKEYS_BETTER2)
							{
								if ((outercmp == BMS_EQUAL ||
									 outercmp == BMS_SUBSET2) &&
									new_path->rows >= old_path->rows &&
									new_path->parallel_safe <= old_path->parallel_safe)
									accept_new = false; /* old dominates new */
							}
							else	/* keyscmp == PATHKEYS_EQUAL */
							{
								if (outercmp == BMS_EQUAL)
								{
									/*
									 * Same pathkeys and outer rels, and fuzzily
									 * the same cost, so keep just one; to decide
									 * which, first check parallel-safety, then
									 * rows, then do a fuzzy cost comparison with
									 * very small fuzz limit.  (We used to do an
									 * exact cost comparison, but that results in
									 * annoying platform-specific plan variations
									 * due to roundoff in the cost estimates.)	If
									 * things are still tied, arbitrarily keep
									 * only the old path.  Notice that we will
									 * keep only the old path even if the
									 * less-fuzzy comparison decides the startup
									 * and total costs compare differently.
									 */
									if (new_path->parallel_safe >
										old_path->parallel_safe)
										remove_old = true;	/* new dominates old */
									else if (new_path->parallel_safe <
											 old_path->parallel_safe)
										accept_new = false; /* old dominates new */
									else if (new_path->rows < old_path->rows)
										remove_old = true;	/* new dominates old */
									else if (new_path->rows > old_path->rows)
										accept_new = false; /* old dominates new */
									else if (compare_path_costs_fuzzily(new_path, old_path,
														1.0000000001) == COSTS_BETTER1)
										remove_old = true;	/* new dominates old */
									else
										accept_new = false; /* old equals or
															 * dominates new */
								}
								else if (outercmp == BMS_SUBSET1 &&
										 new_path->rows <= old_path->rows &&
										 new_path->parallel_safe >= old_path->parallel_safe)
									remove_old = true;	/* new dominates old */
								else if (outercmp == BMS_SUBSET2 &&
										 new_path->rows >= old_path->rows &&
										 new_path->parallel_safe <= old_path->parallel_safe)
									accept_new = false; /* old dominates new */
								/* else different parameterizations, keep both */
							}
							break;
						case COSTS_BETTER1:
							if (keyscmp != PATHKEYS_BETTER2)
							{
								outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
															  PATH_REQ_OUTER(old_path));
								if ((outercmp == BMS_EQUAL ||
									 outercmp == BMS_SUBSET1) &&
									new_path->rows <= old_path->rows &&
									new_path->parallel_safe >= old_path->parallel_safe)
									remove_old = true;	/* new dominates old */
							}
							break;
						case COSTS_BETTER2:
							if (keyscmp != PATHKEYS_BETTER1)
							{
								outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
															  PATH_REQ_OUTER(old_path));
								if ((outercmp == BMS_EQUAL ||
									 outercmp == BMS_SUBSET2) &&
									new_path->rows >= old_path->rows &&
									new_path->parallel_safe <= old_path->parallel_safe)
									accept_new = false; /* old dominates new */
							}
							break;
						case COSTS_DIFFERENT:

							/*
							 * can't get here, but keep this case to keep compiler
							 * quiet
							 */
							break;
					}
				}
			}
		}

		/*
		 * Remove current element from unmaterial_pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->lm_pathlist = list_delete_cell(parent_rel->lm_pathlist,
															   p1, p1_prev);
			/* Free the path any way */
			pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the unmaterial_pathlist; we will not add new_path, and we
		 * assume new_path cannot dominate any other elements of the pathlist.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place in unmaterial_pathlist */
		if (insert_after)
			lappend_cell(parent_rel->lm_pathlist, insert_after, new_path);
		else
			parent_rel->lm_pathlist = lcons(new_path, parent_rel->lm_pathlist);
	}
	else
	{
		/* Free the path any way */
		pfree(new_path);
	}
}

/*
 * add_unmaterial_partial_path
 *	  Like add_path, our goal here is to consider whether a path is worthy
 *	  of being kept around, but the considerations here are a bit different.
 *	  A partial path is one which can be executed in any number of workers in
 *	  parallel such that each worker will generate a subset of the path's
 *	  overall result.
 *
 *	  As in add_path, the partial_pathlist is kept sorted with the cheapest
 *	  total path in front.  This is depended on by multiple places, which
 *	  just take the front entry as the cheapest path without searching.
 *
 *	  We don't generate parameterized partial paths for several reasons.  Most
 *	  importantly, they're not safe to execute, because there's nothing to
 *	  make sure that a parallel scan within the parameterized portion of the
 *	  plan is running with the same value in every worker at the same time.
 *	  Fortunately, it seems unlikely to be worthwhile anyway, because having
 *	  each worker scan the entire outer relation and a subset of the inner
 *	  relation will generally be a terrible plan.  The inner (parameterized)
 *	  side of the plan will be small anyway.  There could be rare cases where
 *	  this wins big - e.g. if join order constraints put a 1-row relation on
 *	  the outer side of the topmost join with a parameterized plan on the inner
 *	  side - but we'll have to be content not to handle such cases until
 *	  somebody builds an executor infrastructure that can cope with them.
 *
 *	  Because we don't consider parameterized paths here, we also don't
 *	  need to consider the row counts as a measure of quality: every path will
 *	  produce the same number of rows.  Neither do we need to consider startup
 *	  costs: parallelism is only used for plans that will be run to completion.
 *	  Therefore, this routine is much simpler than add_path: it needs to
 *	  consider only pathkeys and total cost.
 *
 *	  As with add_path, we pfree paths that are found to be dominated by
 *	  another partial path; this requires that there be no other references to
 *	  such paths yet.  Hence, GatherPaths must not be created for a rel until
 *	  we're done creating all partial paths for it.  Unlike add_path, we don't
 *	  take an exception for IndexPaths as partial index paths won't be
 *	  referenced by partial BitmapHeapPaths.
 */
void
add_unmaterial_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;	/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

	/* Check for query cancel. */
	CHECK_FOR_INTERRUPTS();

	/*
	 * As in add_path, throw out any paths which are dominated by the new
	 * path, but throw out the new path if some existing path dominates it.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->lm_partial_pathlist); p1 != NULL;
		 p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathKeysComparison keyscmp;
		MaterialComparison umcmp;

		p1_next = lnext(p1);

		/* Compare pathkeys. */
		keyscmp = compare_pathkeys(new_path->pathkeys, old_path->pathkeys);

		/*
		 * Do a material status comparison, we keep both path if different
		 */
		umcmp = compare_material_status(parent_rel, new_path, old_path);

		if (umcmp != MATERIAL_EQUAL)
		{
			/* Unless pathkeys are incompable, keep just one of the two paths. */
			if (keyscmp != PATHKEYS_DIFFERENT)
			{
				if (new_path->total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
				{
					/* New path costs more; keep it only if pathkeys are better. */
					if (keyscmp != PATHKEYS_BETTER1)
						accept_new = false;
				}
				else if (old_path->total_cost > new_path->total_cost
						 * STD_FUZZ_FACTOR)
				{
					/* Old path costs more; keep it only if pathkeys are better. */
					if (keyscmp != PATHKEYS_BETTER2)
						remove_old = true;
				}
				else if (keyscmp == PATHKEYS_BETTER1)
				{
					/* Costs are about the same, new path has better pathkeys. */
					remove_old = true;
				}
				else if (keyscmp == PATHKEYS_BETTER2)
				{
					/* Costs are about the same, old path has better pathkeys. */
					accept_new = false;
				}
				else if (old_path->total_cost > new_path->total_cost * 1.0000000001)
				{
					/* Pathkeys are the same, and the old path costs more. */
					remove_old = true;
				}
				else
				{
					/*
					 * Pathkeys are the same, and new path isn't materially
					 * cheaper.
					 */
					accept_new = false;
				}
			}
		}

		/*
		 * Remove current element from lm_partial_pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->lm_partial_pathlist =
				list_delete_cell(parent_rel->lm_partial_pathlist, p1, p1_prev);
			pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the lm_partial_pathlist; we will not add new_path, and we
		 * assume new_path cannot dominate any later path.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place */
		if (insert_after)
			lappend_cell(parent_rel->lm_partial_pathlist, insert_after, new_path);
		else
			parent_rel->lm_partial_pathlist =
				lcons(new_path, parent_rel->lm_partial_pathlist);
	}
	else
	{
		/* Reject and recycle the new path */
		pfree(new_path);
	}
}
#endif

/*****************************************************************************
 *		PATH NODE CREATION ROUTINES
 *****************************************************************************/
#ifdef XCP
static Node *
get_discol_expr(RelOptInfo *rel, Oid	relid, AttrNumber attrNum)
{
	Var 	   *var = NULL;
	ListCell   *lc;

	/* Look if the Var is already in the target list */
	foreach (lc, rel->reltarget->exprs)
	{
		var = (Var *) lfirst(lc);
		if (IsA(var, Var) && var->varno == rel->relid &&
				var->varattno == attrNum)
			break;
	}
	/* If not found we should look up the attribute and make the Var */
	if (!lc)
	{
		Relation 	relation = heap_open(relid, NoLock);
		TupleDesc	tdesc = RelationGetDescr(relation);
		Form_pg_attribute att_tup;

		att_tup = TupleDescAttr(tdesc, attrNum - 1);
		var = makeVar(rel->relid, attrNum,
					  att_tup->atttypid, att_tup->atttypmod,
					  att_tup->attcollation, 0);


		heap_close(relation, NoLock);
	}

	return (Node *)var;
}

/*
 * set_scanpath_distribution
 *	  Assign distribution to the path which is a base relation scan.
 */
void
set_scanpath_distribution(PlannerInfo *root, RelOptInfo *rel, Path *pathnode)
{
	RangeTblEntry   *rte;
	RelationLocInfo *rel_loc_info = NULL;
	bool			is_global_index_path;
	Oid				relid = InvalidOid;

	rte = planner_rt_fetch(rel->relid, root);
	is_global_index_path = IS_GLOBAL_INDEX_PATH(pathnode);
	relid = rte->relid;
#ifdef __OPENTENBASE__
	/*
	 * get group oid which base rel belongs to, and used later at end of planner.
	 * local tables not included.
	 */
	if (IS_PGXC_COORDINATOR)
	{
		if (is_global_index_path)
		{
			IndexOptInfo *indexinfo = ((IndexPath*)pathnode)->indexinfo;
			relid = indexinfo->indexoid;
		}
		rel_loc_info = GetRelationLocInfo(relid);
	}
#endif

	if (IS_PGXC_COORDINATOR && rel_loc_info)
	{
		ListCell *lc;
		bool retry = true;
		Distribution *distribution = makeNode(Distribution);

		distribution->distributionType = rel_loc_info->locatorType;
		/*
		 * for LOCATOR_TYPE_REPLICATED distribution, check if
		 * all of the mentioned nodes are hale and hearty. Remove
		 * those which are not. Do this only for SELECT queries!
		 */
retry_pools:
		if (root->parse->commandType == CMD_SELECT &&
			(distribution->distributionType == LOCATOR_TYPE_REPLICATED ||
			 distribution->distributionType == LOCATOR_TYPE_FOREIGN))
		{
			int i;
			bool *healthmap = NULL;

			healthmap = (bool*)palloc(sizeof(bool) * OPENTENBASE_MAX_DATANODE_NUMBER);
			if (healthmap == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
							errmsg("out of memory for healthmap")));

			PgxcNodeDnListHealth(rel_loc_info->rl_nodeList, healthmap);

			i = 0;
			foreach(lc, rel_loc_info->rl_nodeList)
			{
				if (healthmap[i++] == true)
					distribution->nodes = bms_add_member(distribution->nodes,
														 lfirst_int(lc));
			}

			if (healthmap)
			{
				pfree(healthmap);
				healthmap = NULL;
			}

			if (bms_is_empty(distribution->nodes))
			{
				/*
				 * Try an on-demand pool maintenance just to see if some nodes
				 * have come back.
				 *
				 * Try once and error out if datanodes are still down
				 */
				if (retry)
				{
					PoolPingNodes();
					retry = false;
					goto retry_pools;
				}
				else
					elog(ERROR,
						 "Could not find healthy nodes for replicated table. Exiting!");
			}
			else if (distribution->distributionType == LOCATOR_TYPE_FOREIGN)
			{
				distribution->nodes = bms_make_singleton(GetAnyDataNode(distribution->nodes));
			}
		}
		else
		{
			foreach(lc, rel_loc_info->rl_nodeList)
				distribution->nodes = bms_add_member(distribution->nodes,
													 lfirst_int(lc));
		}

		distribution->restrictNodes = NULL;
		/*
		 * Distribution expression of the base relation is Var representing
		 * respective attribute.
		 */
#ifdef __OPENTENBASE_C__
		distribution->disExprs = NULL;
		distribution->nExprs = 0;
		if (rel_loc_info->nDisAttrs > 0)
		{
			int i = 0;

			distribution->nExprs = rel_loc_info->nDisAttrs;
			distribution->disExprs = (Node **)palloc0(sizeof(Node *) * distribution->nExprs);
			for (i = 0; i < rel_loc_info->nDisAttrs; i++)
			{
				distribution->disExprs[i] = get_discol_expr(rel, rte->relid, rel_loc_info->disAttrNums[i]);
			}
		}
#endif
		pathnode->distribution = distribution;
	}
}

static Path *
create_remotesubplan_gather_path(PlannerInfo *root, Path *subpath,
								 Distribution *distribution)
{
	RelOptInfo	   *rel = subpath->parent;
	RemoteSubPath  *pathnode;
	int				parallel_workers;
	double			rows = subpath->rows;

	/* use remote fragment to replace gather/gathermerge */
	if (IsA(subpath, GatherPath))
	{
		GatherPath *gpath = (GatherPath *)subpath;
		subpath = gpath->subpath;
		parallel_workers = gpath->num_workers;
	}
	else
	{
		GatherMergePath *gpath = (GatherMergePath *)subpath;
		subpath = gpath->subpath;
		parallel_workers = gpath->num_workers;
	}

	pathnode = makeNode(RemoteSubPath);
	pathnode->path.pathtype = T_RemoteSubplan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->path.distribution = distribution;
	pathnode->subpath = subpath;

	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	/* make an even number of parallel_workers */
	parallel_workers += parallel_workers % 2;
	pathnode->path.parallel_workers =
		Min(parallel_workers, max_parallel_workers_per_gather);

	cost_remote_subplan(root,
						(Path *)pathnode,
						subpath->startup_cost,
						subpath->total_cost,
						rows,
						rel->reltarget->width,
						calcDistReplications(distribution, subpath));

	return (Path *) pathnode;
}

/*
 * apply_distribution_exprs
 *   Sometimes the distribution key is not included in the target list of the
 *   Remote Path and needs to be added explicitly. It doesn't have to be a junk
 *   target because the Remote that requires redistribution is definitely not
 *   the one gathering data to the CN. The final result will definitely not
 *   include these additional columns that were added.
 */
static Path *
apply_distribution_exprs(PlannerInfo *root, Path *path, int nExprs, Node **disExprs)
{
	int i;
	PathTarget	*target = NULL;
	RelOptInfo	*rel = path->parent;
	MemoryContext old = MemoryContextSwitchTo(GetMemoryChunkContext(path));

	for (i = 0; i < nExprs; i++)
	{
		/* check original form first */
		Expr *expr = (Expr *) disExprs[i];
		if (list_member(path->pathtarget->exprs, expr))
			continue;

		/* strip out RelabelType then check */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;
		if (list_member(path->pathtarget->exprs, expr))
			continue;

		/* there is no such expr, add it as target */
		if (target == NULL)
			target = copy_pathtarget(path->pathtarget);

		add_new_column_to_pathtarget(target, expr);
	}

	/*
	 * We should enforce the use of create_projection_path instead of
	 * apply_projection_to_path, because we don't want the target of the
	 * original path to be modified, as this could affect other potential
	 * paths.
	 */
	if (target != NULL)
		path = (Path *) create_projection_path(root, rel, path, target);

	MemoryContextSwitchTo(old);
	return path;
}

/*
 * Because Remote Subplan will do merge sort by the pathkeys, we should check
 * the pathkey coming from subpath, and reduce if can not found in targetlist.
 */
List *
get_reasonable_pathkeys(PathTarget *target, List *pathkeys, Relids relids)
{
	ListCell   *lc;
	List	   *real_tlexprs = NIL;
	List	   *new_pathkeys = NIL;

	if (target->exprs == NIL)
		return NIL;

	/* remove RelabelType */
	foreach(lc, target->exprs)
	{
		Expr *tlexpr = (Expr *) lfirst(lc);

		while (tlexpr && IsA(tlexpr, RelabelType))
			tlexpr = ((RelabelType *) tlexpr)->arg;

		real_tlexprs = lappend(real_tlexprs, tlexpr);
	}

	foreach(lc, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *ec = pathkey->pk_eclass;
		ListCell   *lc1 = NULL;

		/* do not check this */
		if (ec->ec_has_volatile)
		{
			new_pathkeys = lappend(new_pathkeys, pathkey);
			continue;
		}

		foreach(lc1, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc1);
			Expr	   *emexpr = NULL;
			List	   *exprvars;
			ListCell   *lc2 = NULL;

			if (em->em_is_const)
				continue;

			if (em->em_is_child &&
				!bms_is_subset(em->em_relids, relids))
				continue;

			emexpr = em->em_expr;
			while (emexpr && IsA(emexpr, RelabelType))
				emexpr = ((RelabelType *) emexpr)->arg;

			if (list_member(real_tlexprs, emexpr))
				break;

			exprvars = pull_var_clause((Node *) emexpr,
									   PVC_INCLUDE_AGGREGATES |
									   PVC_INCLUDE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS);
			foreach(lc2, exprvars)
			{
				Expr *node = (Expr *) lfirst(lc2);

				while (node && IsA(node, RelabelType))
					node = ((RelabelType *) node)->arg;

				if (list_member(real_tlexprs, node))
					break;
			}
			if (lc2)
				break;
		}

		if (lc1)
			new_pathkeys = lappend(new_pathkeys, pathkey);
		else
			break;
	}

	list_free(real_tlexprs);
	return new_pathkeys;
}

/*
 * create_remotesubplan_path
 *	Redistribute the data to match the distribution.
 *
 * Creates a RemoteSubPath on top of the path, redistributing the data
 * according to the specified distribution.
 */
Path *
create_remotesubplan_path(PlannerInfo *root, Path *subpath,
						  Distribution *distribution)
{
	RelOptInfo	   *rel = subpath->parent;
	RemoteSubPath  *pathnode;
	Distribution   *dist = (Distribution *) copyObject(distribution);

	/* apply all dist-exprs into targetlist if there aren't, except for the local fragment*/
	if (IsValidDistribution(distribution) && distribution->distributionType != LOCATOR_TYPE_MIXED &&
	    !equal(subpath->distribution, distribution))
		subpath = apply_distribution_exprs(root, subpath,
										   distribution->nExprs,
										   distribution->disExprs);

	/* remove gather/gathermerge if no project */
	if (IsA(subpath, GatherPath) &&
		subpath->pathtarget == ((GatherPath *) subpath)->subpath->pathtarget)
	{
		return create_remotesubplan_gather_path(root, subpath, dist);
	}
	else if (IsA(subpath, GatherMergePath) &&
			 subpath->pathtarget == ((GatherMergePath *) subpath)->subpath->pathtarget)
	{
		return create_remotesubplan_gather_path(root, subpath, dist);
	}

	pathnode = makeNode(RemoteSubPath);
	pathnode->path.pathtype = T_RemoteSubplan;
	pathnode->path.parent = rel;
	pathnode->path.param_info = subpath->param_info;
	pathnode->subpath = subpath;
	pathnode->path.distribution = dist;

	/* check pathkeys */
	if (subpath->pathkeys)
		pathnode->path.pathkeys = get_reasonable_pathkeys(subpath->pathtarget,
														subpath->pathkeys,
														rel->relids);

	/* just use subpath's parallel info */
	pathnode->path.parallel_aware = subpath->parallel_aware;
	pathnode->path.parallel_safe = subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_safe ? subpath->parallel_workers : 0;

	pathnode->path.pathtarget = subpath->pathtarget;

	cost_remote_subplan(root,
						(Path *)pathnode,
						subpath->startup_cost,
						subpath->total_cost,
						subpath->rows,
						rel->reltarget->width,
						calcDistReplications(distribution, subpath));

	return (Path *) pathnode;
}

Path *
create_remotesubplan_replicated_path(PlannerInfo *root, Path *subpath)
{
	Distribution *targetd;
	int i;

	targetd = makeNode(Distribution);
	targetd->distributionType = LOCATOR_TYPE_REPLICATED;
	for (i = 0; i < NumDataNodes; i++)
		targetd->nodes = bms_add_member(targetd->nodes, i);

	return create_remotesubplan_path(root, subpath, targetd);
}

/*
 * set_distribution_for_union_result_path
 *	Set distribution for result set path when the targets are
 *	all const or extern parameter.
 */
static Path *
set_distribution_for_union_result_path(PlannerInfo *root, Path *path)
{
	Path		*subpath;
	ListCell	*lc;

	/* Check validtion for subquery scan path. */
	if (path == NULL || path->distribution != NULL || root->parse == NULL ||
		!IsA(path, SubqueryScanPath) || path->parent == NULL || path->parent->relid == 0 ||
		path->parent->relid > list_length(root->parse->rtable))
		return path;

	subpath = ((SubqueryScanPath*)path)->subpath;

	/* Check validtion for project path. */
	if (subpath == NULL || !IsA(subpath, ProjectionPath) ||
		subpath->pathtype != T_Result || subpath->pathtarget == NULL ||
		subpath->pathtarget->exprs == NULL)
		return path;

	/* Check whether all the targets are all const or extern parameter. */
	foreach(lc, subpath->pathtarget->exprs)
	{
		Expr	*expr = (Expr*) lfirst(lc);

		while (expr && IsA(expr, RelabelType))
			expr = (Expr *) ((RelabelType *) expr)->arg;

		if (!IsA(expr, Const) &&
			!(IsA(expr, Param) && ((Param *) expr)->paramkind == PARAM_EXTERN))
			return path;
	}

	subpath = ((ProjectionPath*) subpath)->subpath;

	if (!IsA(subpath, ResultPath) ||
		(((ResultPath*) subpath)->quals != NULL && !IsA(((ResultPath*) subpath)->quals, Const)))
	{
		if (IsA(subpath, ResultPath) && ((ResultPath*) subpath)->quals != NULL &&
			IsA(((ResultPath*) subpath)->quals, List))
		{
			foreach(lc, ((ResultPath*) subpath)->quals)
			{
				Expr	*expr = (Expr*) lfirst(lc);

				if (!IsA(expr, Const))
					return path;
			}
		}
		else
		{
			return path;
		}
	}

	subpath = create_remotesubplan_replicated_path(root, path);

	return subpath;
}

/*
 * set_distribution_for_union_leg
 *	Check whether the remote plans for result path are
 *	needed to keep the distribution for union path.
 */
static void
set_distribution_for_union_leg(PlannerInfo *root, List *subpaths)
{
	ListCell	*lc;
	Path		*subpath;

	if (!IS_PGXC_COORDINATOR)
		return;

	/* Check wether there is any distribution path under union. */
	foreach(lc, subpaths)
	{
		subpath = (Path *) lfirst(lc);

		if (subpath->distribution != NULL)
			break;
	}

	if (lc == NULL)
		return;

	/* Add remote plan for result path. */
	foreach(lc, subpaths)
	{
		subpath = (Path *) lfirst(lc);

		subpath = set_distribution_for_union_result_path(root, subpath);

		lfirst(lc) = subpath;
	}
}

/*
 * Append path is used to implement scans of partitioned tables, inherited
 * tables and some "set" operations, like UNION ALL. While all partitioned
 * and inherited tables should have the same distribution, UNION'ed queries
 * may have different.  When paths being appended have the same
 * distribution it is OK to push Append down to the data nodes. If not,
 * perform "coordinator" Append.
 *
 * Since we ensure that all partitions of a partitioned table are always
 * distributed by the same strategy on the same set of nodes, we can push
 * down MergeAppend of partitions of the table.
 * For MergeAppend of non-partitions, it is safe to push down MergeAppend
 * if all subpath distributions are the same and these distributions are
 * Replicated or distribution key is the expression of the first pathkey.
 *
 * Return new subpaths if changed.
 */
static void
set_appendpath_distribution(PlannerInfo *root, Path *path, List *subpaths, bool inh)
{
	RelOptInfo *rel = path->parent;
	ListCell   *l;
	Path *subpath;
	Distribution *subd;
	Distribution *targetd;
	int i;

	/* Special case of the dummy relation, if the subpaths list is empty */
	if (subpaths == NIL)
	{
		targetd = makeNode(Distribution);
		targetd->distributionType = LOCATOR_TYPE_REPLICATED;
		for (i = 0; i < NumDataNodes; i++)
			targetd->nodes = bms_add_member(targetd->nodes, i);

		path->distribution = targetd;
		return;
	}

	/* Take distribution of the first node */
	l = list_head(subpaths);
	subpath = (Path *) lfirst(l);
	targetd = copyObject(subpath->distribution);

	if (inh)
	{
		/* no distribution, nothing to do */
		if (!targetd)
			return;

		/* check if distribution key is in targetlist */
		if (IsValidDistribution(targetd))
		{
			for (i = 0; i < targetd->nExprs; i++)
			{
				Node *expr = targetd->disExprs[i];
				if (!list_member(subpath->pathtarget->exprs, expr))
				{
					/* not found */
					targetd->distributionType = LOCATOR_TYPE_NONE;
					targetd->nExprs = 0;
					targetd->disExprs = NULL;
					break;
				}
			}
		}

		/* reset varno */
		for (i = 0; i < targetd->nExprs; i++)
		{
			((Var *)targetd->disExprs[i])->varno = rel->relid;
			((Var *)targetd->disExprs[i])->varnoold = rel->relid;
		}
		path->distribution = targetd;
		return;
	}

	set_distribution_for_union_leg(root, subpaths);

	/*
	 * Check remaining subpaths, if all distributions equal to the first set
	 * it as a distribution of the Append path; otherwise make up coordinator
	 * Append
	 */
	while ((l = lnext(l)))
	{
		subpath = (Path *) lfirst(l);
		subd = subpath->distribution;

		/*
		 * For Append and MergeAppend paths, we are most often dealing with
		 * different relations, appended together. So its very likely that
		 * the distribution for each relation will have a different varno.
		 * But we should be able to push down Append and MergeAppend as
		 * long as rest of the distribution information matches.
		 */
		if (IsSimilarDistribution(targetd, subd))
		{
			/*
			 * Both distribution and subpath->distribution may be NULL at
			 * this point, or they both are not null.
			 */
			if (targetd)
			{
				if (targetd->restrictNodes && subd->restrictNodes)
					targetd->restrictNodes = bms_union(targetd->restrictNodes,
													   subd->restrictNodes);
				else
					targetd->restrictNodes = NULL;

				targetd->replication_for_update = (subd->replication_for_update ||
													targetd->replication_for_update);
			}
		}
		else
		{
			break;
		}
	}

	if (l == NULL)
	{
		path->distribution = targetd;
		return;
	}

#ifdef __OPENTENBASE_C__
	if (nonunion_optimizer && !root->hasRecursion &&
		g_NodeGroupMgr->shmemNumNodeGroups == 1)
	{
		int distkey = 0;
		Distribution *remoted;

		foreach(l, subpaths)
		{
			subpath = (Path *) lfirst(l);
			if (!subpath->distribution && !subpath->execute_on_any)
			{
				path->distribution = NULL;
				goto poll_up;
			}
		}

		targetd = makeNode(Distribution);
		targetd->distributionType = LOCATOR_TYPE_NONE;
		for (i = 0; i < NumDataNodes; i++)
			targetd->nodes = bms_add_member(targetd->nodes, i);

		/* find a distribute key in case of need */
		foreach(l, path->pathtarget->exprs)
		{
			Node *expr = (Node *) lfirst(l);
			TypeCacheEntry *typentry = lookup_type_cache(exprType(expr), TYPECACHE_HASH_PROC);

			if (OidIsValid(typentry->hash_proc))
				break;

			distkey++;
		}

		if (distkey == list_length(path->pathtarget->exprs))
			goto poll_up;

		/* make remote distribution */
		remoted = makeNode(Distribution);
		remoted->distributionType = LOCATOR_TYPE_SHARD;
		remoted->nodes = bms_copy(targetd->nodes);
		remoted->disExprs = (Node **) palloc(sizeof(Node *));
		remoted->nExprs = 1;

		foreach(l, subpaths)
		{
			subpath = (Path *) lfirst(l);
			subd = subpath->distribution;

			if (subpath->execute_on_any)
			{
				/* use local send for execute_on_any */
				subd = makeNode(Distribution);
				subd->distributionType = LOCATOR_TYPE_NONE;
				subd->restrictNodes = bms_make_singleton(GetAnyDataNode(NULL));
				subd->nodes = bms_copy(targetd->nodes);
				subpath->distribution = subd;

				subpath = create_remotesubplan_path(root, subpath, subd);
				((RemoteSubPath *)subpath)->localSend = true;
				lfirst(l) = subpath;
				pathkey_sanity_check((Path *) subpath);
			}
			else if (IsLocatorReplicated(subd->distributionType))
			{
				/* pick one node to execute */
				if (subd->restrictNodes == NULL)
				{
					if (subd->replication_for_update)
						subd->restrictNodes = bms_copy(targetd->nodes);
					else
						subd->restrictNodes = bms_make_singleton(GetAnyDataNode(subd->nodes));
				}
				subd->nodes = bms_copy(targetd->nodes);

				/* set distribute key */
				remoted->disExprs[0] = (Node *) list_nth(subpath->pathtarget->exprs, distkey);
				subpath = create_remotesubplan_path(root, subpath, remoted);
				lfirst(l) = subpath;
				pathkey_sanity_check((Path *) subpath);
			}
			else if (subd->restrictNodes)
			{
				/* set distribute key */
				remoted->disExprs[0] = (Node *) list_nth(subpath->pathtarget->exprs, distkey);
				subpath = create_remotesubplan_path(root, subpath, remoted);
				lfirst(l) = subpath;
				pathkey_sanity_check((Path *) subpath);
			}
		}

		path->distribution = targetd;
		return;
	}
#endif
poll_up:
	foreach(l, subpaths)
	{
		subpath = (Path *) lfirst(l);
		if (subpath->distribution)
		{
			subpath = create_remotesubplan_path(root, subpath, NULL);
			lfirst(l) = subpath;
			pathkey_sanity_check((Path *) subpath);
		}
	}
}

#ifdef __OPENTENBASE__
static bool
get_discol_value(PlannerInfo *root, RestrictInfo *ri,
				 Node *expr, Datum *value, bool *isnull)
{
	Const		*constExpr = NULL;
	bool		found_key = false;
	OpExpr	   *opexpr;
	Oid 		opno;
	Node	   *leftarg;

	if (!is_opclause(ri->clause))
		return false;

	opexpr = (OpExpr *) ri->clause;
	if (list_length(opexpr->args) != 2)
		return false;

	opno = opexpr->opno;
	leftarg = linitial(opexpr->args);

	if (op_hashjoinable(opno, exprType(leftarg)) &&
		!contain_volatile_functions((Node *) opexpr))
	{
		Expr *arg1 = (Expr *) linitial(opexpr->args);
		Expr *arg2 = (Expr *) lsecond(opexpr->args);
		Expr *other = NULL;
		Var *var1 = (Var *)get_var_from_arg((Node *)arg1);
		Var *var2 = (Var *)get_var_from_arg((Node *)arg2);

		if (var1)
			arg1 = (Expr *)var1;
		if (var2)
			arg2 = (Expr *)var2;

		if (equal(arg1, expr))
			other = arg2;
		else if (equal(arg2, expr))
			other = arg1;

		if (other)
		{
			found_key = true;
			other = (Expr *) coerce_to_target_type(NULL,
												   (Node *) other,
												   exprType((Node *) other),
												   exprType(expr),
												   exprTypmod(expr),
												   COERCION_ASSIGNMENT,
												   COERCE_IMPLICIT_CAST, -1);
			other = (Expr *) eval_const_expressions(root, (Node *) other);
			if (IsA(other, Const))
				constExpr = (Const *) other;
		}

		if (found_key && constExpr)
		{
			*value = constExpr->constvalue;
			*isnull = constExpr->constisnull;
			return true;
		}
	}

	return false;
}

static void
restrict_distribution(PlannerInfo *root, Path *pathnode, List *restrictinfo)
{
	Distribution   *distribution = pathnode->distribution;
	Oid 			reloid	= InvalidOid;
	Oid 			groupid = InvalidOid;
	Datum *disValues = NULL;
	bool *disIsNulls = NULL;
	int ndiscols;
	ListCell *lc = NULL;
	List	   *nodeList = NIL;
	Bitmapset  *tmpset = NULL;
	Bitmapset  *restrictnodes = NULL;
	RangeTblEntry	*rte;
	RelationLocInfo *rel_loc_info;
	Locator    *locator;
	int 	   *nodenums;
	int 		i, count;
	bool		found_key = false;

	/*
	 * Can not restrict - not distributed or key is not defined
	 */
	if (!IsValidDistribution(distribution) || !restrictinfo)
		return;

	ndiscols = distribution->nExprs;
	disValues = (Datum *) palloc(sizeof(Datum) * ndiscols);
	disIsNulls = (bool *) palloc(sizeof(bool) * ndiscols);
	for (i = 0; i < ndiscols; ++i)
		disIsNulls[i] = true;

	foreach (lc, restrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (ri->orclause || ri->pseudoconstant)
			continue;

		for (i = 0; i < distribution->nExprs; i++)
		{
			if (distribution->disExprs[i] &&
				get_discol_value(root, ri, distribution->disExprs[i],
								 &disValues[i], &disIsNulls[i]))
			{
				found_key = true;
				break;
			}
		}
	}

	if (!found_key)
		return;

	tmpset = bms_copy(distribution->nodes);
	while((i = bms_first_member(tmpset)) >= 0)
		nodeList = lappend_int(nodeList, i);
	bms_free(tmpset);
	
	if (!nodeList)
		return;

	if (pathnode->parent->relid < root->simple_rel_array_size)
	{
		i = pathnode->parent->relid;
		if (root->simple_rte_array[i]->rtekind == RTE_RELATION &&
		    (root->simple_rte_array[i]->relkind == 'r' ||
		     root->simple_rte_array[i]->relkind == 'g' ||
		     root->simple_rte_array[i]->relkind == 'G' ||
		     (enable_partition_iterator && root->simple_rte_array[i]->relkind == 'p')))
		{
			reloid = root->simple_rte_array[i]->relid;
		}
	}
	groupid = GetRelGroup(reloid);

	if (!OidIsValid(groupid) && distribution->distributionType == LOCATOR_TYPE_SHARD)
	{
		elog(ERROR, "could not get group info for shard table %u", reloid);
	}

	rte = planner_rt_fetch(pathnode->parent->relid, root);
	rel_loc_info = GetRelationLocInfo(rte->relid);

	locator = createLocator(distribution->distributionType,
							RELATION_ACCESS_READ,
							LOCATOR_LIST_LIST,
							0,
							(void *) nodeList,
							(void **) &nodenums,
							false,
							groupid,
							rel_loc_info->disAttrTypes, rel_loc_info->disAttrNums,
							rel_loc_info->nDisAttrs,
							NULL); /* ora_compatible */

	count = GET_NODES(locator, disValues, disIsNulls, ndiscols, NULL);

	for (i = 0; i < count; i++)
		restrictnodes = bms_add_member(restrictnodes, nodenums[i]);

	if (distribution->restrictNodes)
		distribution->restrictNodes = bms_intersect(distribution->restrictNodes,
													restrictnodes);
	else
		distribution->restrictNodes = restrictnodes;

	list_free(nodeList);
	freeLocator(locator);
	if (disValues)
		pfree(disValues);
	if (disIsNulls)
		pfree(disIsNulls);
}
#endif

/*
 * redistribute_path
 * 	Redistributes the path to match desired distribution parameters.
 *
 * It's also possible to specify desired sort order using pathkeys. If the
 * subpath does not match the order, a Sort node will be added automatically.
 * This is similar to how create_merge_append_path() injects Sort nodes.
 */
static Path *
redistribute_path(PlannerInfo *root, Path *subpath, List *pathkeys,
				  char distributionType,
#ifdef __OPENTENBASE_C__
				  int nExprs, Node **disExprs,
#endif
				  Bitmapset *nodes, Bitmapset *restrictNodes)
{
	Distribution   *distribution = NULL;
	RelOptInfo	   *rel = subpath->parent;
	RemoteSubPath  *pathnode;

	if (distributionType != LOCATOR_TYPE_NONE)
	{
		distribution = makeNode(Distribution);
		distribution->distributionType = distributionType;
		distribution->nodes = nodes;
		distribution->restrictNodes = restrictNodes;
#ifdef __OPENTENBASE_C__
		distribution->nExprs = nExprs;
		distribution->disExprs = disExprs;
#endif
	}
	/* redistribution, and there is no need to pass this information to the upper layer. */
	subpath->distribution->replication_for_update = false;

	/* apply all dist-exprs into targetlist if there aren't */
	if (distributionType != LOCATOR_TYPE_MIXED)
		subpath = apply_distribution_exprs(root, subpath, nExprs, disExprs);

	/*
	 * If inner path node is a MaterialPath pull it up to store tuples on
	 * the destination nodes and avoid sending them over the network.
	 */
	if (IsA(subpath, MaterialPath))
	{
		/* Use a new node but not reuse the old one */
		MaterialPath *mpath = makeNode(MaterialPath);

		memcpy(mpath, subpath, sizeof(MaterialPath));
		pathnode = makeNode(RemoteSubPath);
		/* If subpath is already a RemoteSubPath, just replace distribution */
		if (IsA(mpath->subpath, RemoteSubPath))
		{
			memcpy(pathnode, mpath->subpath, sizeof(RemoteSubPath));
		}
		else
		{
			pathnode->path.pathtype = T_RemoteSubplan;
			pathnode->path.parent = rel;
			pathnode->path.pathtarget = rel->reltarget;
			pathnode->path.param_info = subpath->param_info;
			pathnode->path.pathkeys = subpath->pathkeys;
			pathnode->subpath = mpath->subpath;

#ifdef __OPENTENBASE_C__
			/* Also pop up the late materialize info */
			pathnode->path.attr_masks = subpath->attr_masks;
			/* We can not do local gtidscan if the tuple walk through Remotesubplan */
			pathnode->path.gtidscan_local = false;
			/*
			 * The overall rid sequence can be out of order if the tuple pass
			 * through Remotesubplan. But for each distributed node, the rid
			 * sequence should be the same.
			 */
			pathnode->path.gtidscan_skipsort = subpath->gtidscan_skipsort;
#endif
		}

		subpath = pathnode->subpath;
		pathnode->path.distribution = distribution;
		/* use subpath's parallel info */
		pathnode->path.parallel_aware = subpath->parallel_aware;
		pathnode->path.parallel_safe = subpath->parallel_safe;
		pathnode->path.parallel_workers = subpath->parallel_safe ? subpath->parallel_workers : 0;
		/* (re)calculate costs */
		cost_remote_subplan(root,
							(Path *)pathnode,
							subpath->startup_cost,
							subpath->total_cost,
							subpath->rows,
							rel->reltarget->width,
							calcDistReplications(distribution, subpath));

		mpath->path.distribution = distribution;
		mpath->subpath = (Path *) pathnode;
		cost_material(&mpath->path,
					  pathnode->path.startup_cost,
					  pathnode->path.total_cost,
					  pathnode->path.rows,
					  rel->reltarget->width);
		return (Path *) mpath;
	}
	else if (IsA(subpath, RemoteSubPath))
	{
		RemoteSubPath *rpath = (RemoteSubPath *)subpath;

		if (rpath->localSend)
		{
			/* Only parallel hash can get here, just reset distribution */
			subpath->distribution = distribution;
			rpath->localSend = false;
			return subpath;
		}
		else
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Invalid path with duplicate remote subpath")));
	}
	/* remove gather/gathermerge if no project */
	else if (pathkeys == NULL && IsA(subpath, GatherPath) &&
			 subpath->pathtarget == ((GatherPath *) subpath)->subpath->pathtarget)
	{
		return create_remotesubplan_gather_path(root, subpath, distribution);
	}
	else if (pathkeys == NULL && IsA(subpath, GatherMergePath) &&
			 subpath->pathtarget == ((GatherMergePath *) subpath)->subpath->pathtarget)
	{
		return create_remotesubplan_gather_path(root, subpath, distribution);
	}
	else
	{
		Cost	input_startup_cost = 0;
		Cost	input_total_cost = 0;

		pathnode = makeNode(RemoteSubPath);
		pathnode->path.pathtype = T_RemoteSubplan;
		pathnode->path.parent = rel;
		pathnode->path.pathtarget = rel->reltarget;
		pathnode->path.param_info = subpath->param_info;
		pathnode->path.pathkeys = pathkeys ? pathkeys : subpath->pathkeys;
		pathnode->path.distribution = distribution;

		/*
		 * If we need to insert a Sort node, add it here, so that it gets
		 * pushed down to the remote node.
		 *
		 * This works just like create_merge_append_path, i.e. we only do the
		 * costing here and only actually construct the Sort node later in
		 * create_remotescan_plan.
		 */
		if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
		{
			/* Subpath is adequately ordered, we won't need to sort it */
			input_startup_cost += subpath->startup_cost;
			input_total_cost += subpath->total_cost;
		}
		else
		{
			/* We'll need to insert a Sort node, so include cost for that */
			Path		sort_path;		/* dummy for result of cost_sort */

			cost_sort(&sort_path,
					  root,
					  pathkeys,
					  subpath->total_cost,
					  subpath->parent->tuples,
					  subpath->pathtarget->width,
					  0.0,
					  work_mem,
					  -1.0);

			input_startup_cost += sort_path.startup_cost;
			input_total_cost += sort_path.total_cost;
		}

		pathnode->subpath = subpath;

		/* use subpath's parallel info */
		pathnode->path.parallel_aware = subpath->parallel_aware;
		pathnode->path.parallel_safe = subpath->parallel_safe;
		pathnode->path.parallel_workers = subpath->parallel_safe ? subpath->parallel_workers : 0;

#ifdef __OPENTENBASE_C__
		/* Also pop up the late materialize info */
		pathnode->path.attr_masks = subpath->attr_masks;
		/* We can not do local gtidscan if the tuple walk through Remotesubplan */
		pathnode->path.gtidscan_local = false;
		/*
		 * The overall rid sequence can be out of order if the tuple pass
		 * through Remotesubplan. But for each distributed node, the rid
		 * sequence should be the same.
		 */
		pathnode->path.gtidscan_skipsort = subpath->gtidscan_skipsort;
#endif

		cost_remote_subplan(root,
							(Path *) pathnode,
							input_startup_cost,
							input_total_cost,
							subpath->rows,
							rel->reltarget->width,
							calcDistReplications(distribution, subpath));

		return (Path *) pathnode;
	}
}

static bool
get_joinpath_disexprs(Expr **key, Node *disExpr, Expr *expr, int *match,
					  Expr *cur_expr, Relids source_relids, Relids target_relids)
{
	if (!(*key) && equal(disExpr, expr) &&
		bms_is_subset(source_relids, target_relids))
	{
		*key = cur_expr;
		(*match)++;
		return true;
	}
	return false;
}

static void
get_joinpath_fullexprs(Expr **lkey, Expr **rkey, RestrictInfo *ri,
					   Relids outer_rels, Relids inner_rels,
					   Expr *left, Expr *right)
{
	Expr *left_expr = NULL;
	Expr *right_expr = NULL;
	if (bms_is_subset(ri->left_relids, outer_rels) &&
		bms_is_subset(ri->right_relids, inner_rels))
	{
		left_expr = left;
		right_expr = right;
	}
	else if (bms_is_subset(ri->left_relids, inner_rels) &&
		bms_is_subset(ri->right_relids, outer_rels))
	{
		left_expr = right;
		right_expr = left;
	}
	*lkey = left_expr;
	*rkey = right_expr;
}

/*
 * is_restrictInfo_has_const
 *	Check whether current clause contains const.
 *
 * The method is checking whether equal class for left and right contain
 * const including const from current query block and subquery block.
 */
static bool
is_restrictInfo_has_const(RestrictInfo *ri)
{
	bool is_const = false;

	if (ri == NULL)
		return false;
	
	if (ri->left_ec != NULL)
		is_const = ri->left_ec->ec_has_const ||
				   ri->left_ec->ec_has_const_from_subquery;

	if (is_const == true)
		return true;

	if (ri->right_ec != NULL)
		is_const = ri->right_ec->ec_has_const ||
				   ri->right_ec->ec_has_const_from_subquery;

	return is_const;
}

/*
 * get_rinfo_flat_ratio
 *	Get flat ratio depending on RestrictInfo
 */
static int
get_rinfo_flat_ratio(JoinPath *pathnode, RestrictInfo *rinfo)
{
	int		i;
	Relids	relids;
	EquivalenceClass *equalClass;

	if (rinfo->parent_ec == NULL)
		return -1;

	equalClass = rinfo->parent_ec;

	relids = bms_intersect(rinfo->parent_ec->ec_relids, equalClass->ec_relids);

	if (bms_num_members(relids) <= 0)
		return -1;

	/*
	 * If single clause return flat_ratio_single directly
	 */
	if (bms_num_members(relids) <= 2)
		return rinfo->flat_ratio_single;

	/*
	 * Loop to find join MCV saved as AttStatsSlot
	 */
	if (list_length(equalClass->ec_relid_list) > 0)
	{
		for (i = 0; i < list_length(equalClass->ec_relid_list); i++)
		{
			Relids ec_relids = (Relids) list_nth(equalClass->ec_relid_list, i);

			if (bms_equal(ec_relids, relids))
				return list_nth_int(equalClass->ec_flat_ratio_list, i);
		}
	}

	return -1;
}

/*
 * compare_rinfo_flat_ratio
 *	compare tow clauses depending on flat ratio
 */
static bool
compare_rinfo_flat_ratio(JoinPath *pathnode, RestrictInfo *preferred, RestrictInfo *ri)
{
	int preferred_flat_ratio = -1;
	int ri_float_ratio = -1;

	preferred_flat_ratio = get_rinfo_flat_ratio(pathnode, preferred);
	ri_float_ratio = get_rinfo_flat_ratio(pathnode, ri);

	if (preferred_flat_ratio == -1 && ri_float_ratio == -1)
		return false;

	if (preferred_flat_ratio >= 0 && ri_float_ratio == -1)
		return false;

	if (preferred_flat_ratio == -1 && ri_float_ratio >= 0)
		return true;

	if (preferred_flat_ratio > ri_float_ratio)
		return true;

	return false;
}

/*
 * is_better_restrictInfo
 *	Compare better restrictInfos for join distribution.
 *
 *	Not prefer restrictInfo with const value
 */
static bool
is_better_restrictInfo(JoinPath *pathnode, RestrictInfo *preferred, RestrictInfo *ri)
{
	bool is_const_preferred = false;
	bool is_const_ri = false;

	if (preferred == NULL || ri == NULL || preferred == ri)
		return false;

	is_const_ri = is_restrictInfo_has_const(ri);

	/*
	 * If both sides of the new clause have const, do not replace
	 */
	if (is_const_ri == true)
		return false;

	is_const_preferred = is_restrictInfo_has_const(preferred);

	if (is_const_preferred == true)
		return true;

	is_const_preferred = compare_rinfo_flat_ratio(pathnode, preferred, ri);

	if (is_const_preferred == true)
		return true;

	return false;
}

/*
 * set_mixed_join_roundrobin
 *	Set roundrobin for distribution path
 *
 * Curently, if the chosen dustruibtuib clause has const, the roundrobin
 * method would be chosen.
 */
static bool
set_mixed_join_roundrobin(PlannerInfo *root, JoinPath *pathnode,
						  RestrictInfo *preferred, bool is_roundrobin)
{
	RemoteSubPath  *subplan;
	/*
	 * Check whether enabling guc for enable roundrobin distribution
	 */
	if (( data_skew_option & ENABLE_ROUNDROBIN) == 0)
		return false;

	/*
	 * Check whether setting roundrobin distribution
	 */
	if (is_roundrobin == false)
		return false;

	/*
	* Set up round robin path
	*/
	if (IsA(pathnode->outerjoinpath, RemoteSubPath) &&
		IsA(pathnode->innerjoinpath, RemoteSubPath))
	{
		bool is_roundrobin_left = true;

		/*
		 * Choose smaller side to do replicate
		 */
		if (pathnode->outerjoinpath->rows < pathnode->innerjoinpath->rows)
		{
			is_roundrobin_left = false;
		}

		/*
		 * Set up round robin path
		 */
		if (IsA(pathnode->outerjoinpath, RemoteSubPath))
		{
			subplan = (RemoteSubPath *) pathnode->outerjoinpath;

			if (is_roundrobin_left == true)
			{
				subplan->data_skew_parameter.roundrobin_distributed = true;
				subplan->data_skew_parameter.roundrobin_replicate = false;
			}
			else
			{
				subplan->data_skew_parameter.roundrobin_distributed = false;
				subplan->data_skew_parameter.roundrobin_replicate = true;
			}
		}

		if (IsA(pathnode->innerjoinpath, RemoteSubPath))
		{
			subplan = (RemoteSubPath *) pathnode->innerjoinpath;

			if (is_roundrobin_left == true)
			{
				subplan->data_skew_parameter.roundrobin_distributed = false;
				subplan->data_skew_parameter.roundrobin_replicate = true;
			}
			else
			{
				subplan->data_skew_parameter.roundrobin_distributed = true;
				subplan->data_skew_parameter.roundrobin_replicate = false;
			}

		}

		pathnode->path.distribution->nExprs = 0;
		return true;
	}

	return false;
}

/*
 * calculate_distribution_benefit
 *	Calculate the benefit of replicated value.
 *
 * Use formula to calculate the distribution cost:
 *
 * Keep left (replicate right):
 * 	right_rows * right_record_width * number_dn
 *
 * Keep right (replicate left):
 * 	left_rows * left_record_width * number_dn
 *
 * Redistributed (distribute left and right):
 * 	left_rows * left_record_width + right_rows * right_record_width
 *
 * Punish value : Avoid too sensitive for replication, a punishment
 * 				  value is added to the original value before competing.
 *
 * punish_value = max( original_value * threshold_ratio,
 * 					   original_value + threshold_benenit)
 * The threshold_ratio and threshold_benenit should be adjusted
 * depending on the following test.
 *
 */
static bool
calculate_distribution_benefit(double row1, double row2,
							   int width1, int width2,
							   double *benefit, bool *is_replicate_left)
{
	double	threshold_ratio = 2;
	double	threshold_benenit = 10000;
	double	current_min = 0;
	double	punish_value = 0;

	double	keep_left = row2 * width2 * NumDataNodes;
	double	keep_right = row1 * width1 * NumDataNodes;
	double	distributed = row1 * width1 + row2 * width2;

	/*
	 * Check keep left
	 */
	current_min = Min(keep_right, distributed);
	punish_value = Max(keep_left * threshold_ratio,
					keep_left + threshold_benenit);
	if (punish_value < current_min)
	{
		*benefit = current_min - keep_left;
		*is_replicate_left = true;
		return true;
	}

	/*
	 * Check keep right
	 */
	current_min = Min(keep_left, distributed);
	punish_value = Max(keep_right * threshold_ratio,
					   keep_right + threshold_benenit);
	if (keep_right < current_min)
	{
		*benefit = current_min - keep_right;
		*is_replicate_left = false;
		return true;
	}

	return false;
}

/*
 * check_data_skew_value
 *	Check whether a value is suitable for replication
 *	and build up suitable value list.
 */
static void
check_data_skew_value(double row1, double row2,
					  int width1, int width2,
					  Datum value, RestrictInfo *rinfo, bool single_rinfo)
{
	double			benefit = 0;
	bool 			is_replicate_value = false;
	bool 			is_replicate_left = false;
	Datum			*value_ptr;

	if (rinfo == NULL)
		return;

	/*
	 * Calculate benefit for distribution values
	 */
	is_replicate_value = calculate_distribution_benefit(row1, row2, width1, width2,
														&benefit, &is_replicate_left);

	if (is_replicate_value == false)
		return;

	value_ptr = (Datum *)palloc0fast(sizeof(Datum));
	*value_ptr = value;

	if (is_replicate_left == true)
	{
		if (single_rinfo == true)
		{
			rinfo->retain_value_left = lappend(rinfo->retain_value_left, value_ptr);
		}
		else
		{
			rinfo->retain_value_left_cache = lappend(rinfo->retain_value_left_cache, value_ptr);
		}
	}
	else
	{
		if (single_rinfo == true)
		{
			rinfo->retain_value_right = lappend(rinfo->retain_value_right, value_ptr);
		}
		else
		{
			rinfo->retain_value_right_cache = lappend(rinfo->retain_value_right_cache, value_ptr);
		}
	}

	return;
}

/*
 * build_up_mvc_sslots
 *	Use for join up join MCV as AttStatsSlot. It helps make uniform
 *	interfaces for data skew calculation.
 */
static AttStatsSlot*
build_up_mvc_sslots(AttStatsSlot *sslot, List *value_list,
					List *left_number_list, List *right_number_list,
					float4 col_filter1, float4 col_filter2,
					float4 left_rows1, float4 left_rows2)
{
	int			 	i;
	AttStatsSlot   *result;
	float4			left_number, right_number;
	float4		   *float_ptr;
	float4			number_total = 0;

	int	mcv_size = list_length(value_list);

	if (mcv_size <= 0)
		return NULL;

	result = (AttStatsSlot*)palloc0(sizeof(AttStatsSlot));
 
	result->staop = 0;
	result->valuetype = sslot->valuetype;

	result->values = (Datum *) palloc0(sizeof(Datum) * mcv_size);
	result->nvalues = mcv_size;

	result->numbers = (float4 *) palloc0(sizeof(float4) * mcv_size);
	result->nnumbers = mcv_size;

	/*
	 * Loop to build up column cardinality numbers
	 *
	 * If the left hand and right hand all have values, it means
	 * there are MCV matching.
	 * 
	 * If only one side has value, the other hand is assumed to
	 * use uniform number.
	 */
	for (i = 0; i < mcv_size; i++)
	{
		if (left_rows1 <= 0 && left_rows2 <=0)
			break;

		result->values[i] = (Datum) list_nth(value_list, i);

		float_ptr = (float4*) list_nth(left_number_list, i);
		if (float_ptr != NULL && left_rows2 >= 0)
		{
			left_number = *float_ptr;
		}
		else
		{
			left_number = col_filter1;
			left_rows1--;
		}

		float_ptr = (float4*) list_nth(right_number_list, i);
		if (float_ptr != NULL && left_rows1 >= 0)
		{
			right_number = *float_ptr;
		}
		else
		{
			right_number = col_filter2;
			left_rows2--;
		}

		/*
		 * Calculate the number for MCV and total number
		 */
		result->numbers[i] = left_number * right_number;
		number_total = number_total + result->numbers[i];
	}

	/*
	 * Calculate column rows without MCV in both sides and added to total numbers
	 *
	 * Assume that all the left less rows could match the other side. So use the
	 * formula as
	 * 	Min(distinct rows in left, distinct rows in right) * (matched uniform rows) 
	 */
	if (left_rows1 > 0 && left_rows2 > 0)
	{
		number_total += Min(left_rows1, left_rows2) * col_filter1 * col_filter2;
	}

	/*
	 * Normalization for the MCV number
	 */
	if (number_total < 1)
	{
		for (i = 0; i < mcv_size; i++)
		{
			result->numbers[i] = result->numbers[i] / number_total;
		}
	}

	return result;
}

/*
 * is_filtered_by_rinfo
 *	Check whether the value is filtered by clauses.
 *	Return true if value is qualifed.
 * 
 * Currently, we could check column operator const caluses.
 */
static bool
is_filtered_by_rinfo(PlannerInfo *root, EquivalenceClass *eqclass, Datum checkval)
{
	ListCell	*lc;
	OpExpr   	*opclause;
	Node		*other;
	FmgrInfo	 opproc;
	bool		 varonleft;
	Datum		 constval;
	VariableStatData vardata;

	if (eqclass == NULL)
		return false;

	/*
	 * Loop through clause list
	 */
	foreach(lc, eqclass->ec_rinfo_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
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

		constval = ((Const *) other)->constvalue;

		/*
		 * Check whether the value is qualied by the clause
		 */
		if (varonleft ?
			DatumGetBool(FunctionCall2Coll(&opproc,
										DEFAULT_COLLATION_OID,
										checkval,
										constval)) :
			DatumGetBool(FunctionCall2Coll(&opproc,
										DEFAULT_COLLATION_OID,
										constval,
										checkval)))
		{
			ReleaseVariableStats(vardata);
			continue;
		}
		else
		{
			ReleaseVariableStats(vardata);
			return false;
		}

		ReleaseVariableStats(vardata);
	}

	return true;
}

/*
 * get_distinct_with_filter
 *	Calculate number distinct values exclude qualfied mcv
 *
 * Algorithm :
 *  1. Calculate qualified tuples exclude qualified MCV
 *  2. Calculate relation typles exclude MCV
 *  3. Estimate qualified distinct values by a ratio of qualified tuples
 *     and relation tuples without mcv
 */
static double
get_distinct_with_filter(VariableStatData *vardata, double distinct_value,
						 double match_mcv, double total_mcv)
{
	double		rel_filter = 0;
	double		filter_distinct = 0;

	if (vardata->rel == NULL)
		return distinct_value;

	/*
	 * Qualified tuple ratio
	 */
	rel_filter = vardata->rel->rows / vardata->rel->tuples;

	if (rel_filter < match_mcv)
		return 0;

	/*
	 * R1 : qualified tuple ratio without MCV
	 * R2 : ralation tuple ratio without MCV
	 * filter_distinct = (R1 / R2) *  distinct_value_without_mcv
	 */
	filter_distinct = (rel_filter - match_mcv) / (1 - total_mcv) * distinct_value;
	filter_distinct = Min(filter_distinct, distinct_value);

	return filter_distinct;
}

/*
 * check_data_skew
 *	build up data skew list depending on MCV.
 *
 * parameters:
 *	calculate_data_skew : true calculate retain list, 
 *	                      false calculate join MCV
 *	single_rinfo : true use retain_value_left and retain_value_right
 *	               false use retain_value_left_cache and retain_value_right_cache
 *
 * Process:
 *   1. Check the condition both sides contain MCV.
 *      1.1 If there are matched MCVs, calculate the rows for both sides
 *          value_MCV = tuple_rows * sslot->numbers
 *
 *   1.2 If a value in one side of MCV list but not the other side,
 *       value_MCV = tuple_rows * sslot1->numbers
 *       value_not_in_MCV = tuple_rows * (1 - sum(sslot->numbers) - null_number)
 *
 *   2. Only one side contains MCV, the calculation method works as 1.2
 *
 *   3. If both sides do not contains MCV, return directly.
 */
static AttStatsSlot *
check_data_skew_information(PlannerInfo *root, Oid opfuncoid,
							VariableStatData *vardata1, VariableStatData *vardata2,
							AttStatsSlot *sslot1, AttStatsSlot *sslot2,
							Form_pg_statistic stats1, Form_pg_statistic stats2,
							bool have_mcvs1, bool have_mcvs2,
							RestrictInfo *rinfo, bool calculate_data_skew, bool single_rinfo)
{
	FmgrInfo	eqproc;

	double		left_rel_tuples;
	double		right_rel_tuples;

	int   		left_width = 0;
	int   		right_width = 0;

	double		left_mvc_total = 0;
	double		right_mvc_total = 0;

	double		left_null_value;
	double		right_null_value;

	double		left_rows = 0;
	double		right_rows = 0;
	double		left_ndistinct = 0;
	double		right_ndistinct = 0;
	bool		left_isdefault;
	bool		right_isdefault;

	List		*value_list = NULL;
	List	 	*left_number_list = NULL;
	List	 	*right_number_list = NULL;
	AttStatsSlot *sslot = NULL;
	AttStatsSlot *result = NULL;

	bool		left_valid_value = true;
	bool		right_valid_value = true;
	double		left_valid_mcv_numbers = 0;
	double		right_valid_mcv_numbers = 0;

	/*
	 * Check whether enabling guc for parital broadcast and partial local send
	 */
	if ((data_skew_option & ENABLE_PARTIAL_BROADCAST) == 0)
		return NULL;

	/*
	 * If there is no statistics infomraiotn, return directly
	 */
	if (rinfo == NULL || 
	    stats1 == NULL ||
		stats2 == NULL || 
		vardata1 == NULL ||
		vardata2 == NULL ||
		vardata1->rel == NULL ||
		vardata2->rel == NULL)
		return NULL;

	if (have_mcvs1 == false && have_mcvs2 == false)
		return NULL;

	fmgr_info(opfuncoid, &eqproc);

	/*
	 * The null value frequency
	 */
	left_null_value = stats1->stanullfrac;
	right_null_value = stats2->stanullfrac;

	/*
	 * Number of table tuples
	 */
	left_rel_tuples = vardata1->rel->tuples;
	right_rel_tuples = vardata2->rel->tuples;

	/*
	 * The width of records
	 */
	left_width = vardata1->rel->reltarget->width;
	right_width = vardata2->rel->reltarget->width;

	/*
	 * The number of distinct values for records
	 */
	left_ndistinct = get_variable_numdistinct(vardata1, &left_isdefault);
	right_ndistinct = get_variable_numdistinct(vardata2, &right_isdefault);

	/*
	 * Both sides contains MCV
	 */
	if (have_mcvs1 && have_mcvs2)
	{
		int i;
		bool *hasmatch;

		sslot = sslot1;
		hasmatch = (bool *) palloc0(sslot2->nvalues * sizeof(bool));

		for (i = 0; i < sslot1->nvalues; i++)
		{
			int		j;
			bool	match_value = false;

			/* 
			 * Calculate the summary of MCV for outer table
			 */
			left_mvc_total += sslot1->numbers[i];

			left_valid_value = is_filtered_by_rinfo(root, rinfo->parent_ec, sslot1->values[i]);

			if (left_valid_value)
				left_valid_mcv_numbers = left_valid_mcv_numbers + sslot1->numbers[i];

			for (j = 0; j < sslot2->nvalues; j++)
			{
				/* 
				 * Calculate the summary of MCV for inner table
				 */
				if ( i ==0 )
					right_mvc_total += sslot2->numbers[j];

				if (hasmatch[j] == true)
					continue;

				/*
				 * There are matched value for MCV in both side
				 */
				if (DatumGetBool(FunctionCall2Coll(&eqproc,
												   DEFAULT_COLLATION_OID,
												   sslot1->values[i],
												   sslot2->values[j])))
				{
					match_value = true;
					hasmatch[j] = true;

					if (calculate_data_skew == true && left_valid_value == true)
					{
						check_data_skew_value(left_rel_tuples * sslot1->numbers[i],
											right_rel_tuples * sslot2->numbers[j],
											left_width, right_width,
											sslot1->values[i], rinfo, single_rinfo);
					}

					if ((calculate_data_skew == false || single_rinfo == true) && left_valid_value == true)
					{
						value_list = lappend(value_list, &sslot1->values[i]);
						left_number_list = lappend(left_number_list, &sslot1->numbers[i]);
						right_number_list = lappend(right_number_list, &sslot2->numbers[j]);
					}

					break;
				}
			}

			/*
			 * Check mismatch values
			 */
			if (match_value == false)
			{
				if (right_isdefault == false &&
					(right_ndistinct - sslot2->nvalues) > 0)
				{
					/*
					 * Calculate right_rows only once
					 */
					if (i == 0)
					{
						right_rows = right_rel_tuples *
									 ((1 - right_mvc_total - right_null_value) /
									  (right_ndistinct - sslot2->nvalues));
					}

					if (calculate_data_skew == true && left_valid_value == true)
					{
						check_data_skew_value(left_rel_tuples * sslot1->numbers[i], right_rows,
							left_width, right_width, sslot1->values[i], rinfo, single_rinfo);
					}

					if ((calculate_data_skew == false || single_rinfo == true) && left_valid_value == true)
					{
						value_list = lappend(value_list, &sslot1->values[i]);
						left_number_list = lappend(left_number_list, &sslot1->numbers[i]);
						right_number_list = lappend(right_number_list, NULL);
					}
				}
			}
		}

		/*
		 * Check inner table mismarch mvc
		 */
		if ((left_ndistinct - sslot1->nvalues) > 0)
		{
			left_rows = left_rel_tuples *
						((1 - left_mvc_total - left_null_value) /
						(left_ndistinct - sslot1->nvalues));

			for (i = 0; i < sslot2->nvalues; i++)
			{
				if (hasmatch[i] == false)
				{
					if (left_isdefault == false)
					{
						right_valid_value = is_filtered_by_rinfo(root, rinfo->parent_ec, sslot2->values[i]);

						if (right_valid_value)
							right_valid_mcv_numbers = right_valid_mcv_numbers + sslot2->numbers[i];

						if (calculate_data_skew == true && right_valid_value == true)
						{
							check_data_skew_value(left_rows, right_rel_tuples * sslot2->numbers[i],
								left_width, right_width, sslot2->values[i], rinfo, single_rinfo);
						}

						if ((calculate_data_skew == false || single_rinfo == true) && right_valid_value == true)
						{
							value_list = lappend(value_list, &sslot2->values[i]);
							left_number_list = lappend(left_number_list, NULL);
							right_number_list = lappend(right_number_list, &sslot2->numbers[i]);
						}
					}
				}
			}
		}
	}
	else if (have_mcvs1 && right_ndistinct)
	{
		sslot = sslot1;

		if (right_isdefault == false)
		{
			int    i;

			/*
			 * Calculate the rows in right side by formular
			 *	rows = tuples * (1- null_value) / distinct
			 */
			right_rows = right_rel_tuples * ((1 - right_null_value) / right_ndistinct);

			for (i = 0; i < sslot1->nvalues; i++)
			{
				left_valid_value = is_filtered_by_rinfo(root, rinfo->parent_ec, sslot1->values[i]);

				if (left_valid_value)
					left_valid_mcv_numbers = left_valid_mcv_numbers + sslot1->numbers[i];

				if (calculate_data_skew == true && left_valid_value == true)
				{
					check_data_skew_value(left_rel_tuples * sslot1->numbers[i], right_rows,
						left_width, right_width, sslot1->values[i], rinfo, single_rinfo);
				}

				if ((calculate_data_skew == false || single_rinfo == true) && left_valid_value == true)
				{
					value_list = lappend(value_list, &sslot1->values[i]);
					left_number_list = lappend(left_number_list, &sslot1->numbers[i]);
					right_number_list = lappend(right_number_list, NULL);
				}
			}
		}
	}
	else if (have_mcvs2 && left_ndistinct > 0)
	{
		sslot = sslot2;

		if (left_isdefault == false)
		{
			int    i;

			/*
			 * Calculate the rows in left side by formular
			 *	rows = tuples * (1- null_value) / distinct
			 */
			left_rows = left_rel_tuples * ((1 - left_null_value) / left_ndistinct);

			for (i = 0; i < sslot2->nvalues; i++)
			{
				right_valid_value =	is_filtered_by_rinfo(root, rinfo->parent_ec, sslot2->values[i]);

				if (right_valid_value)
					right_valid_mcv_numbers = right_valid_mcv_numbers + sslot2->numbers[i];

				if (calculate_data_skew == true && right_valid_value == true)
				{
					check_data_skew_value(left_rows, right_rel_tuples * sslot2->numbers[i], 
						left_width, right_width, sslot2->values[i], rinfo, single_rinfo);
				}

				if ((calculate_data_skew == false || single_rinfo == true) && right_valid_value == true)
				{
					value_list = lappend(value_list, &sslot2->numbers[i]);
					left_number_list = lappend(left_number_list, NULL);
					right_number_list = lappend(right_number_list, &sslot2->values[i]);
				}
			}
		}
	}

	if (sslot != NULL && ( calculate_data_skew == false || single_rinfo == true))
	{
		double left_distinct_with_filter = 0;
		double right_distinct_with_filter = 0;

		left_distinct_with_filter = get_distinct_with_filter(vardata1, left_ndistinct - sslot1->nvalues,
									left_valid_mcv_numbers, left_mvc_total);

		right_distinct_with_filter = get_distinct_with_filter(vardata2, right_ndistinct - sslot2->nvalues,
							right_valid_mcv_numbers, right_mvc_total);

		result = build_up_mvc_sslots(sslot, value_list, left_number_list, right_number_list,
									 (left_rows / left_rel_tuples), (right_rows / right_rel_tuples),
									 left_distinct_with_filter, right_distinct_with_filter);
	}
	else
	{
		result = NULL;
	}

	list_free(value_list);
	list_free(left_number_list);
	list_free(right_number_list);

	return result;
}

/*
 * get_groupid_by_rinfo
 *	Get group id depending on RestrictInfo
 */
static Oid
get_groupid_by_rinfo(PlannerInfo *root, RestrictInfo *rinfo)
{
	int relindex = 0;
	int relid = 0;
	Oid groupid = InvalidOid;

	/*
	 * Get relation index
	 */
	relindex = bms_first_member(rinfo->left_relids);

	if (relindex <= 0)
	{
		return  InvalidOid;
	}

	/*
	 * Get relation oid
	 */
	relid = root->simple_rte_array[relindex]->relid;

	if (relid <= 0)
	{
		return  InvalidOid;
	}

	/*
	 * Get group oid
	 */
	groupid = GetRelGroup(relid);

	return groupid;
}

/*
 * calculate_flat_ratio
 *	Calculate the flat ratio by AttStatsSlot
 *
 * Function Process:
 * 	1. Calculate the destination data node
 * 	2. Accumulate MCV number for the data node
 * 	3. Use the max value for flat ratio
 */
static int
calculate_flat_ratio(AttStatsSlot *sslot, Oid groupid)
{
	int 		i;
	int			dn_id = -1;
	long		hash_value;
	int 		len;
	float4	   *dn_mcv = NULL;
	float4		dn_mcv_max = 0;
	float4		flat_ratio = 0;
	bool		is_null = false;

	if (NumDataNodes == 0)
		return -1;

	if (sslot == NULL)
		return -1;

	if (groupid == InvalidOid)
		return -1;

	len = Min(sslot->nvalues, sslot->nnumbers);

	if (len <= 0)
		return -1;

	dn_mcv = (float4 *)palloc0(sizeof(float4) * NumDataNodes);

	/*
	 * Calculate data node numbers accumulated
	 */
	for (i = 0; i < len; i++)
	{
		hash_value = EvaluateHashkey(&(sslot->valuetype), &is_null, &(sslot->values[i]), 1);
		dn_id = GetNodeIndexByHashValue(groupid, hash_value);

		dn_mcv[dn_id] = dn_mcv[dn_id] + sslot->numbers[i];
	}

	for (i = 0; i < NumDataNodes; i++)
	{
		dn_mcv_max = Max(dn_mcv_max, dn_mcv[i]);
	}

	/*
	 * Use max dn mcv as flat ratio
	 */
	flat_ratio = dn_mcv_max;

	pfree(dn_mcv);

	/*
	 * Use int list to save flat ratio
	 * Change flat ratio to int
	 */
	return (int) (flat_ratio * 1000000);
}

/*
 * set_join_mcv_by_rinfo
 *	Calculate retain list for clauses
 */
static AttStatsSlot *
set_join_mcv_by_rinfo(PlannerInfo *root, JoinPath *pathnode, RestrictInfo *rinfo,
					  AttStatsSlot *in_sslot1, AttStatsSlot *in_sslot2, bool calculate_data_skew)
{
	Oid          opfuncoid;
	AttStatsSlot sslot1;
	AttStatsSlot sslot2;
	AttStatsSlot *sslot1_used;
	AttStatsSlot *sslot2_used;
	VariableStatData vardata1;
	VariableStatData vardata2;
	Form_pg_statistic stats1 = NULL;
	Form_pg_statistic stats2 = NULL;
	bool	  have_mcvs1 = false;
	bool	  have_mcvs2 = false;
	bool	  single_rinfo = false;
	Node     *outer = NULL;
	Node     *inner = NULL;
	OpExpr   *opclause;
	AttStatsSlot *result = NULL;
	Relids		relids;
	Relids		outer_rels;
	Relids		inner_rels;

	if (rinfo->parent_ec == NULL)
		return NULL;

	relids = (Relids) bms_intersect(pathnode->path.parent->relids,
									rinfo->parent_ec->ec_relids);

	/*
	 * If the there is no join MCV invovled and single clause
	 * data skew information has been calculated.
	 */
	if (bms_num_members(relids) <= 2 &&
		rinfo->data_skew_calculated == true &&
		calculate_data_skew)
		return NULL;

	memset(&sslot1, 0, sizeof(sslot1));
	memset(&sslot2, 0, sizeof(sslot2));
	memset(&vardata1, 0, sizeof(vardata1));
	memset(&vardata2, 0, sizeof(vardata2));

	/*
	 * Get outer and inner relations
	 */
	outer_rels = pathnode->outerjoinpath->parent->relids;
	inner_rels = pathnode->innerjoinpath->parent->relids;

	opclause = (OpExpr *) rinfo->clause;

	if (bms_is_subset(rinfo->left_relids, outer_rels) &&
		bms_is_subset(rinfo->right_relids, inner_rels))
	{
		outer = (Node *) linitial(opclause->args);
		inner = (Node *) lsecond(opclause->args);
	}
	else if (bms_is_subset(rinfo->left_relids, inner_rels) &&
			bms_is_subset(rinfo->right_relids, outer_rels))
	{
		inner = (Node *) linitial(opclause->args);
		outer = (Node *) lsecond(opclause->args);
	}

	opfuncoid = get_opcode(opclause->opno);

	memset(&vardata1, 0, sizeof(vardata1));
	memset(&vardata2, 0, sizeof(vardata2));
	examine_variable(root, outer, 0, &vardata1);
	examine_variable(root, inner, 0, &vardata2);

	/*
	 * Get statistics from catalog
	 */
	if (HeapTupleIsValid(vardata1.statsTuple))
	{
		/* note we allow use of nullfrac regardless of security check */
		stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
		if (statistic_proc_security_check(&vardata1, opfuncoid))
			have_mcvs1 = get_attstatsslot(&sslot1, vardata1.statsTuple,
										  STATISTIC_KIND_MCV, InvalidOid,
										  ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
	}

	if (HeapTupleIsValid(vardata2.statsTuple))
	{
		/* note we allow use of nullfrac regardless of security check */
		stats2 = (Form_pg_statistic) GETSTRUCT(vardata2.statsTuple);
		if (statistic_proc_security_check(&vardata2, opfuncoid))
			have_mcvs2 = get_attstatsslot(&sslot2, vardata2.statsTuple,
										  STATISTIC_KIND_MCV, InvalidOid,
										  ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
	}

	/*
	 * If there are join MCV, join MCV information is used.
	 */
	if (in_sslot1 != NULL)
	{
		sslot1_used = in_sslot1;
		have_mcvs1 = true;
	}
	else
	{
		sslot1_used = &sslot1;
	}

	if (in_sslot2 != NULL)
	{
		sslot2_used = in_sslot2;
		have_mcvs2 = true;
	}
	else
	{
		sslot2_used = &sslot2;
	}

	/*
	 * initialization if data skew should be calculated
	 */
	if (calculate_data_skew == true)
	{
		if (bms_num_members(relids) <= 2 &&
			rinfo->data_skew_calculated == false)
		{
			list_free(rinfo->retain_value_left);
			list_free(rinfo->retain_value_right);

			rinfo->retain_value_left = NULL;
			rinfo->retain_value_right = NULL;

			single_rinfo = true;
		}
		else
		{
			rinfo->left_relids_cache = 
				bms_intersect(relids, pathnode->outerjoinpath->parent->relids);
			rinfo->right_relids_cache = 
				bms_intersect(relids, pathnode->innerjoinpath->parent->relids);

			list_free(rinfo->retain_value_left_cache);
			list_free(rinfo->retain_value_right_cache);

			rinfo->retain_value_left_cache = NULL;
			rinfo->retain_value_right_cache = NULL;

			single_rinfo = false;
		}
	}

	/*
	 * Calculate infoamtion for data skew
	 *	calculate_data_skew : true calculate retain list, 
	 *	                      false calculate join MCV
	 *	single_rinfo : true use retain_value_left and retain_value_right
	 *	               false use retain_value_left_cache and retain_value_right_cache
	 */
	result = check_data_skew_information(root, opfuncoid,
							&vardata1, &vardata2,
							sslot1_used, sslot2_used,
							stats1, stats2,
							have_mcvs1, have_mcvs2,
							rinfo, calculate_data_skew, single_rinfo);

	/*
	 * Handle single clause
	 */
	if (calculate_data_skew == true &&
		bms_num_members(relids) <= 2)
	{
		/*
		 * Mark already calculating data skew information to 
		 * avoid duplicated calculation
		 */
		rinfo->data_skew_calculated = true;

		if (result != NULL)
		{
			/*
			 * Calculate flat ratio for single clause
			 */
			Oid groupid = get_groupid_by_rinfo(root, rinfo);
			rinfo->flat_ratio_single = calculate_flat_ratio(result, groupid);
			pfree(result);
		}
		else
		{
			rinfo->flat_ratio_single = 0;
		}
	}

	free_attstatsslot(&sslot1);
	free_attstatsslot(&sslot2);

	ReleaseVariableStats(vardata1);
	ReleaseVariableStats(vardata2);

	return result;
}

/*
 * get_join_mcv_list
 *	Get join mcv from equal class
 */
static AttStatsSlot*
get_join_mcv_list(RelOptInfo *rel, EquivalenceClass *equalClass)
{
	int		i;
	Relids	relids;

	relids = bms_intersect(rel->relids, equalClass->ec_relids);

	/*
	 * Only join MCV saved in the equal class
	 */
	if (bms_num_members(relids) <= 1)
		return NULL;

	/*
	 * Loop to find join MCV saved as AttStatsSlot
	 */
	if (list_length(equalClass->ec_relid_list) > 0)
	{
		for (i = 0; i < list_length(equalClass->ec_relid_list); i++)
		{
			Relids ec_relids = (Relids) list_nth(equalClass->ec_relid_list, i);
		
			if (bms_equal(ec_relids, relids))
				return (AttStatsSlot*) list_nth(equalClass->ec_mcv_list, i);
		}
	}

	return NULL;
}

/*
 * set_data_skew_information
 *	Set data skew information by RestrictInfo.
 *
 * The function calculates retain list for data skew.
 * 1. If there are only two relations in current equal class,
 *    use information in retain_value_left and retain_value_right.
 *    
 *    data_skew_calculated indicates whether single RestrictInfo 
 *    data skew is calcuated.
 * 
 *    If data_skew_calculated is false, the retain list should be
 *    calculated.
 * 
 * 2. If there are more than two relations in current equal class,
 *    retain_value_left_cache and retain_value_right_cache are used
 *    for reducing retain list calculation with join MCV.
 * 
 *    left_relids_cache and right_relids_cache indicate relations in
 *    left hand side and right hand side for cached list.
 */
static void
set_data_skew_information(PlannerInfo *root, JoinPath *pathnode, RestrictInfo *rinfo,
						  Oid *outer_oid, Oid *inner_oid,
						  List **retain_value_outer, List **retain_value_inner)
{
	VariableStatData vardata1;
	VariableStatData vardata2;

	Relids			outer_rels;
	Relids			inner_rels;
	Relids			rinfo_outer_rels;
	Relids			rinfo_inner_rels;

	AttStatsSlot	*sslot_outer;
	AttStatsSlot	*sslot_inner;
	Relids			 relids;

	Node     		*left;
	Node     		*right;
	OpExpr   		*opclause;

	if (rinfo->parent_ec == NULL)
		return;

	/*
	 * Get relations for clause equal class
	 */
	relids = (Relids) bms_intersect(pathnode->path.parent->relids,
									rinfo->parent_ec->ec_relids);

	/*
	 * If there are no more than two relations in current path and
	 * retain list is null by calculation, return early.
	 */
	if (bms_num_members(relids) <= 2 &&
		rinfo->retain_value_left == NULL &&
		rinfo->retain_value_right == NULL &&
		rinfo->data_skew_calculated == true)
	{
		*retain_value_outer = NULL;
		*retain_value_inner = NULL;
		return;
	}

	outer_rels = pathnode->outerjoinpath->parent->relids;
	inner_rels = pathnode->innerjoinpath->parent->relids;

	opclause = (OpExpr *) rinfo->clause;

	left = (Node *) linitial(opclause->args);
	right = (Node *) lsecond(opclause->args);

	memset(&vardata1, 0, sizeof(vardata1));
	memset(&vardata2, 0, sizeof(vardata2));
	examine_variable(root, left, 0, &vardata1);
	examine_variable(root, right, 0, &vardata2);

	/*
	 * Set data type for outer path and inner path.
	 */
	if (bms_is_subset(rinfo->left_relids, outer_rels) &&
		bms_is_subset(rinfo->right_relids, inner_rels))
	{
		*outer_oid = vardata1.vartype;
		*inner_oid = vardata2.vartype;
	}
	else if (bms_is_subset(rinfo->left_relids, inner_rels) &&
			bms_is_subset(rinfo->right_relids, outer_rels))
	{
		*outer_oid = vardata2.vartype;
		*inner_oid = vardata1.vartype;
	}

	/*
	 * Handle single clause
	 */
	if (bms_num_members(relids) <= 2)
	{
		if (rinfo->data_skew_calculated == false)
			set_join_mcv_by_rinfo(root, pathnode, rinfo, NULL, NULL, true);

		/*
		* Check the outer table and inner table on which side
		*/
		if (bms_is_subset(rinfo->left_relids, outer_rels) &&
			bms_is_subset(rinfo->right_relids, inner_rels))
		{
			*retain_value_outer = rinfo->retain_value_left;
			*retain_value_inner = rinfo->retain_value_right;
		}
		else if (bms_is_subset(rinfo->left_relids, inner_rels) &&
				bms_is_subset(rinfo->right_relids, outer_rels))
		{
			*retain_value_outer = rinfo->retain_value_right;
			*retain_value_inner = rinfo->retain_value_left;
		}

		ReleaseVariableStats(vardata1);
		ReleaseVariableStats(vardata2);
		return;
	}

	/*
	 * Get outer and inner relations
	 */
	rinfo_outer_rels = bms_intersect(rinfo->parent_ec->ec_relids, outer_rels);
	rinfo_inner_rels = bms_intersect(rinfo->parent_ec->ec_relids, inner_rels);

	/* Check cached information in RestrictInfo */
	if (bms_equal(rinfo->left_relids_cache, rinfo_outer_rels) &&
		bms_equal(rinfo->right_relids_cache, rinfo_inner_rels))
	{
		*retain_value_outer = rinfo->retain_value_left_cache;
		*retain_value_inner = rinfo->retain_value_right_cache;
		ReleaseVariableStats(vardata1);
		ReleaseVariableStats(vardata2);
		return;
	}
	else if (bms_equal(rinfo->left_relids_cache, rinfo_inner_rels) &&
			 bms_equal(rinfo->right_relids_cache, rinfo_outer_rels))
	{
		*retain_value_outer = rinfo->retain_value_right_cache;
		*retain_value_inner = rinfo->retain_value_left_cache;
		ReleaseVariableStats(vardata1);
		ReleaseVariableStats(vardata2);
		return;
	}

	/*
	 * Get join for outer path and inner path
	 */
	sslot_outer = get_join_mcv_list(pathnode->outerjoinpath->parent, rinfo->parent_ec);
	sslot_inner = get_join_mcv_list(pathnode->innerjoinpath->parent, rinfo->parent_ec);

	/*
	 * Calculate retain list depending on join MCV
	 */
	set_join_mcv_by_rinfo(root, pathnode, rinfo, sslot_outer, sslot_inner, true);

	*retain_value_outer = rinfo->retain_value_left_cache;
	*retain_value_inner = rinfo->retain_value_right_cache;

	ReleaseVariableStats(vardata1);
	ReleaseVariableStats(vardata2);
	return;
}

/*
 * set_mixed_join_keep_mcv_in_local
 *	Set partial broadcast and partial local sned path.
 */
static bool
set_mixed_join_keep_mcv_in_local(PlannerInfo *root,
								 JoinPath *pathnode,
								 RestrictInfo *preferred)
{
	RemoteSubPath  *subplan;
	List           *retain_value_outer = NULL;
	List           *retain_value_inner = NULL;
	Oid       		outer_oid = 0;
	Oid       		inner_oid = 0;

	if (preferred->parent_ec == NULL)
		return false;

	set_data_skew_information(root, pathnode, preferred, &outer_oid, &inner_oid,
							  &retain_value_outer, &retain_value_inner);

	if (retain_value_outer == NULL && retain_value_inner == NULL)
		return false;

	/*
	 * Set up retain and replicate list for redistributed path
	 */
	if (IsA(pathnode->outerjoinpath, RemoteSubPath) &&
		IsA(pathnode->innerjoinpath, RemoteSubPath))
	{
		if (IsA(pathnode->outerjoinpath, RemoteSubPath))
		{
			subplan = (RemoteSubPath *) pathnode->outerjoinpath;

			subplan->data_skew_parameter.retain_list = retain_value_outer;
			subplan->data_skew_parameter.replicated_list = retain_value_inner;
			subplan->data_skew_parameter.rel_oid = outer_oid;

			pathnode->path.distribution->nExprs = 0;
		}

		if (IsA(pathnode->innerjoinpath, RemoteSubPath))
		{
			subplan = (RemoteSubPath *) pathnode->innerjoinpath;

			subplan->data_skew_parameter.retain_list = retain_value_inner;
			subplan->data_skew_parameter.replicated_list = retain_value_outer;
			subplan->data_skew_parameter.rel_oid = inner_oid;

			pathnode->path.distribution->nExprs = 0;
		}
	}

	return true;
}

/*
 * set_mixed_join_distribution
 *	Set the potential data skew list for join distribution
 */
static void
set_mixed_join_distribution(PlannerInfo *root,
                            JoinPath *pathnode,
							RestrictInfo *preferred,
							int nouterkeys,
							int ninnerkeys,
							bool replicate_outer,
							bool replicate_inner,
							bool is_roundrobin)
{
	/*
	 * Check GUC for data skew
	 */
	if (data_skew_option <= 3)
		return;

	if (nouterkeys == 0 && ninnerkeys == 0)
		return;

	if (IsA(pathnode->outerjoinpath, RemoteSubPath) == false ||
		IsA(pathnode->innerjoinpath, RemoteSubPath) == false)
		return;

	/*
	 * Check roundrobin method
	 */
	if (set_mixed_join_roundrobin(root, pathnode, preferred, is_roundrobin))
		return;

	/*
	 * Check partial broadcast and partial local sned method
	 */
	if (set_mixed_join_keep_mcv_in_local(root, pathnode, preferred))
		return;

	return;
}

/*
 * set_join_mcv
 *	Set join mcv depending on restrictInfo
 *
 * Join MCV are saved in the EquivalenceClass
 * 	ec_relid_list saves relations.
 *  ec_mcv_list saves join mcv as AttStatsSlot
 */
static AttStatsSlot *
set_join_mcv(PlannerInfo *root, JoinPath *pathnode, RestrictInfo *rinfo)
{
	AttStatsSlot	*joinMCVList = NULL;
	AttStatsSlot	*joinMCVList_left = NULL;
	AttStatsSlot	*joinMCVList_right = NULL;
	AttStatsSlot	*result = NULL;
	Relids			 relids;
	int				 flat_ratio = 0;
	Oid 			 groupid = InvalidOid;

	if (pathnode == NULL)
		return NULL;

	/*
	 * Restriction operator is not equality operator ?
	 */
	if (rinfo == NULL || rinfo->parent_ec == NULL ||
		rinfo->left_ec == NULL || rinfo->right_ec == NULL)
		return NULL;

	/*
	 * A restriction with OR may be compatible if all OR'ed
	 * conditions are compatible. For the moment we do not
	 * check this and skip restriction. The case if multiple
	 * OR'ed conditions are compatible is rare and probably
	 * do not worth doing at all.
	 */
	if (rinfo->orclause)
		return NULL;

	if (!OidIsValid(rinfo->hashjoinoperator))
		return NULL;

	/*
	 * If there are no more than two relations in the equal class,
	 * it means join MCV would be not used in th future calculation.
	 */
	if (bms_num_members(rinfo->parent_ec->ec_relids) <= 2)
	{
		if (rinfo->data_skew_calculated == false)
			set_join_mcv_by_rinfo(root, pathnode, rinfo, NULL, NULL, true);

		return NULL;
	}

	/*
	 * Relations in the current equal class
	 */
	relids = (Relids) bms_intersect(pathnode->path.parent->relids, rinfo->parent_ec->ec_relids);

	/*
	 * Check whether the join MCV has been calcualted.
	 */
	joinMCVList = get_join_mcv_list(pathnode->path.parent, rinfo->parent_ec);

	if (joinMCVList != NULL)
	{
		return NULL;
	}

	/*
	 * Check whether child nodes contain join MCV
	 */
	joinMCVList_left = get_join_mcv_list(pathnode->outerjoinpath->parent,
										 rinfo->parent_ec);
	joinMCVList_right = get_join_mcv_list(pathnode->innerjoinpath->parent,
										  rinfo->parent_ec);

	/*
	 * Build up join MCV as AttStatsSlot
	 *	Save join MCV in AttStatsSlot to make uniform
	 *	calculation interface with single clasue
	 */
	result = set_join_mcv_by_rinfo(root, pathnode, rinfo, joinMCVList_left, joinMCVList_right, false);

	if (result != NULL)
	{
		/*
		 * Calcualte flat ratio for join
		 */
		groupid = get_groupid_by_rinfo(root, rinfo);
		flat_ratio = calculate_flat_ratio(result, groupid);

		/*
		* Add join MCV and flat ratio to equal class
		*/
		rinfo->parent_ec->ec_relid_list = lappend(rinfo->parent_ec->ec_relid_list, relids);
		rinfo->parent_ec->ec_mcv_list = lappend(rinfo->parent_ec->ec_mcv_list, result);
		rinfo->parent_ec->ec_flat_ratio_list = lappend_int(rinfo->parent_ec->ec_flat_ratio_list, flat_ratio);
	}

	return result;
}

/*
 * set_join_mcv_list
 *	Set join MCV by restrict clauses.
 */
static void
set_join_mcv_list(PlannerInfo *root, JoinPath *pathnode, List *restrictClauses)
{
	ListCell	*lc;

	/*
	 * Check GUC for data skew
	 */
	if (data_skew_option <= 3)
		return;

	/*
	 * Set data skew information for each clause
	 */
	foreach (lc, restrictClauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * Avoid set the infomation if equal class has const
		 */
		if (rinfo->parent_ec != NULL &&
			(rinfo->parent_ec->ec_has_const == true ||
			 rinfo->parent_ec->ec_has_const_from_subquery == true))
			return;

		set_join_mcv(root, pathnode, rinfo);
	}

	return;
}

/*
 * Check whether allow replication path for DML statement.
 * When the replicated table is also result path, the other side
 * should be distriution path.
 */
static bool
is_allow_replication_path(PlannerInfo *root, Path *path, bool dml)
{
	if (!dml)
		return true;

	if (root->parse->resultRelation == 0 || path->parent->relid <= 0)
		return true;

	if (root->parse->resultRelation != path->parent->relid)
		return true;

	return false;
}

/*
 * reorder_distribution_keys:
 *   Reorder the distribution keys if there are preferred order for multi distribution keys.
 * 
 * Conditions:
 *   1. The final results contains preferred distribution key order.
 *   2. There are more than one distribution keys.
 */
static bool
reorder_distribution_keys(PlannerInfo *root, Expr **outer_keys, Expr **inner_keys,
						  int *nouterkeys, int *ninnerkeys,
						  ResultRelLocation resultRelLoc, JoinPath *path)
{
	Node **dst = NULL;
	Node **path_dst = NULL;
	Var   *var1 = NULL;
	Var   *var2 = NULL;
	Expr **dst_keys = NULL;
	Expr **outer_key_tmp = NULL;
	Expr **inner_key_tmp = NULL;
	int    *tmp_order;
	int    i, j;
	bool   match = false;

	if (resultRelLoc == RESULT_REL_NONE)
		return true;

	if (resultRelLoc != RESULT_REL_NONE &&
		root->distribution != NULL &&
		root->distribution->distributionType == LOCATOR_TYPE_REPLICATED)
	{
		return false;
	}

	if (root->distribution == NULL ||
		root->distribution->disExprs == NULL ||
		outer_keys == NULL ||
		inner_keys == NULL ||
		*nouterkeys != *ninnerkeys)
	{
		return true;
	}

	if (*nouterkeys < root->distribution->nExprs)
		return false;

	if (resultRelLoc == RESULT_REL_OUTER)
	{
		dst_keys = outer_keys;
	}
	else
	{
		dst_keys = inner_keys;
	}

	dst = root->distribution->disExprs;
	outer_key_tmp = (Expr **)palloc0(sizeof(Expr *) * root->distribution->nExprs);
	inner_key_tmp = (Expr **)palloc0(sizeof(Expr *) * root->distribution->nExprs);
	tmp_order = (int *)palloc0(sizeof(int) * root->distribution->nExprs);

	/*
	 * Check the keys for distination order and save the desired positions.
	 */
	for (i = 0; i < root->distribution->nExprs; i++)
	{
		match = false;
		var1 = (Var*)get_var_from_arg((Node *)dst[i]);

		if (var1 == NULL || !IsA(var1, Var))
		{
			return false;
		}

		for (j = 0; j < *nouterkeys; j++)
		{
			var2 = (Var*)get_var_from_arg((Node *)dst_keys[j]);

			if (var2 == NULL || !IsA(var2, Var))
			{
				return false;
			}

			if (var1->varno == var2->varno &&
				var1->varattno == var2->varattno)
			{
				match = true;
				break;
			}
		}

		if (match)
		{
			tmp_order[i] = j;
		}
		else
		{
			pfree(outer_key_tmp);
			pfree(inner_key_tmp);
			pfree(tmp_order);
			return false;
		}
	}

	/*
	 * Put desired key order to temporary arrays.
	 */
	for (i = 0; i < root->distribution->nExprs; i++)
	{
		outer_key_tmp[i] = outer_keys[tmp_order[i]];
		inner_key_tmp[i] = inner_keys[tmp_order[i]];
	}

	if (path->jointype == JOIN_RIGHT)
	{
		path_dst = (Node**) inner_key_tmp;
	}
	else
	{
		path_dst = (Node**) outer_key_tmp;
	}

	/*
	 * Check whether distribution is the same as the one of
	 * the result table.
	 */
	for (i = 0; i < root->distribution->nExprs; i++)
	{
		Node *expr1 = get_var_from_arg((Node *)dst[i]);
		Node *expr2 = get_var_from_arg((Node *)path_dst[i]);

		if (equal(expr1, expr2))
			continue;

		if (!extend_equal_disExpr(root, (Path *)path,expr1, expr2))
		{
			pfree(outer_key_tmp);
			pfree(inner_key_tmp);
			pfree(tmp_order);

			return false;
		}
	}

	memcpy(outer_keys, outer_key_tmp, sizeof(Expr *) * root->distribution->nExprs);
	memcpy(inner_keys, inner_key_tmp, sizeof(Expr *) * root->distribution->nExprs);

	*nouterkeys = root->distribution->nExprs;
	*ninnerkeys = root->distribution->nExprs;

	pfree(outer_key_tmp);
	pfree(inner_key_tmp);
	pfree(tmp_order);

	return true;
}

static Path *
set_limit_gather_rep_path(PlannerInfo *root, Path *subpath, List *subpathkeys)
{
	int nodeid;
	Bitmapset *nodes = NULL;
	Distribution *subdist = subpath->distribution;

	if (!subdist->replication_for_limit)
		return subpath;

	nodeid = bms_any_member(bms_copy(subdist->nodes));
	nodes = bms_add_member(nodes, nodeid);

	subpath = redistribute_path(
			root,
			subpath,
			subpathkeys,
			LOCATOR_TYPE_REPLICATED,
#ifdef __OPENTENBASE_C__
			0, NULL,
#endif
			nodes,
			NULL);

	return subpath;
}

/*
 * Analyze join parameters and set distribution of the join node.
 * If there are possible alternate distributions the respective pathes are
 * returned as a list so caller can cost all of them and choose cheapest to
 * continue.
 */
List *
set_joinpath_distribution(PlannerInfo *root, JoinPath *pathnode, uint8 dist_mask)
{
	Distribution   *innerd = pathnode->innerjoinpath->distribution;
	Distribution   *outerd = pathnode->outerjoinpath->distribution;
	Distribution   *targetd;
	List		   *alternate = NIL;
	List		   *restrictClauses = NIL;
	List		   *innerpathkeys = pathnode->innerjoinpath->pathkeys;
	List		   *outerpathkeys = pathnode->outerjoinpath->pathkeys;
#ifdef __OPENTENBASE__
	bool			dml = false;
	bool			keepResultRelLoc = false;
	PlannerInfo    *top_root = root;
	ResultRelLocation resultRelLoc = RESULT_REL_NONE;
#endif

	/* Catalog join */
	if (innerd == NULL && outerd == NULL)
		return NIL;

	while (top_root->parent_root)
	{
		top_root = top_root->parent_root;
	}
	if (top_root->parse->commandType == CMD_UPDATE ||
		top_root->parse->commandType == CMD_DELETE ||
		top_root->parse->commandType == CMD_MERGE)
	{
		dml = true;
	}

	/*
	 * Only top root will consider more restrict rules to make sure
	 * UPDATE/DELETE result relation does not redistributed.
	 */
	if (root->parse->commandType == CMD_UPDATE ||
		root->parse->commandType == CMD_DELETE)
	{
		/* Set the result relation location */
		resultRelLoc = getResultRelLocation(root->parse->resultRelation,
											pathnode->innerjoinpath->parent->relids,
											pathnode->outerjoinpath->parent->relids);
		pathnode->path.parent->resultRelLoc = resultRelLoc;
		if (resultRelLoc != RESULT_REL_NONE)
			keepResultRelLoc = true;
	}

	/* for mergejoins, override with outersortkeys, if needed */
	if (IsA(pathnode, MergePath))
	{
		MergePath *mpath = (MergePath*)pathnode;

		if (mpath->innersortkeys)
			innerpathkeys = mpath->innersortkeys;
		if (mpath->outersortkeys)
			outerpathkeys = mpath->outersortkeys;
	}

#ifdef __OPENTENBASE__
	/*
	 * DML may need to push down to datanodes, for example:
	 *   DELETE FROM
	 *   	geocode_settings as gc
	 *   USING geocode_settings_default AS gf
	 *   WHERE
	 *   	gf.name = gc.name and gf.setting = gc.setting;
	 */
	if (root->hasUserDefinedFun && false == dml)
	{
		goto pull_up;
	}
	/* One side on CN */
	if ((innerd == NULL || outerd == NULL) && false == dml)
	{
		goto pull_up;
	}
	/*
	 * If outer or inner subpaths are distributed by shard and they do not exist
	 * in same node set, which means we may need to redistribute tuples to data
	 * nodes which use different router map to producer.
	 * We don't support that, so pull it up to CN to accomplish the join.
	 *
	 * TODO:
	 *      1. if the join is "REPLICATION join SHARD", and node set of SHARD table
	 *      is subset of REPLICATION table, no need to pull up.
	 *      2. find out which side of this join needs to dispatch, and only decide
	 *      whether to pull up by the distributionType of another side subpath.
	 *      3. pass target router map to another group maybe ? thus nothing need to
	 *      pull up to CN.
	 */
	if (innerd && outerd && false == dml &&
		(outerd->distributionType == LOCATOR_TYPE_SHARD ||
		(innerd->distributionType == LOCATOR_TYPE_SHARD)) &&
		!bms_equal(outerd->nodes, innerd->nodes) &&
		g_NodeGroupMgr->shmemNumNodeGroups > 1)
	{
		goto pull_up;
	}
#endif

	/*
	 * If both subpaths are distributed by replication, the resulting
	 * distribution will be replicated on smallest common set of nodes.
	 * Catalog tables are the same on all nodes, so treat them as replicated
	 * on all nodes.
	 */
	if ((innerd && IsLocatorReplicated(innerd->distributionType)) &&
		(outerd && IsLocatorReplicated(outerd->distributionType)) &&
		is_allow_replication_path(top_root, pathnode->innerjoinpath, dml))
	{
		/* Determine common nodes */
		Bitmapset *common;

		common = bms_intersect(innerd->nodes, outerd->nodes);
		if (bms_is_empty(common))
			goto not_allowed_join;

		/*
		 * Join result is replicated on common nodes. Running query on any
		 * of them produce correct result.
		 */
		targetd = makeNode(Distribution);
		targetd->distributionType = LOCATOR_TYPE_REPLICATED;
		targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
		targetd->replication_for_limit = (innerd && innerd->replication_for_limit) ||
										(outerd && outerd->replication_for_limit);
		targetd->nodes = common;
		targetd->restrictNodes = NULL;
		pathnode->path.distribution = targetd;
		return NIL;
	}

	/*
	 * Check if we have inner replicated
	 * The "both replicated" case is already checked, so if innerd
	 * is replicated, then outerd is not replicated and it is not NULL.
	 * This case is not acceptable for some join types. If outer relation is
	 * nullable data nodes will produce joined rows with NULLs for cases when
	 * matching row exists, but on other data node.
	 */
	if (is_allow_replication_path(top_root, pathnode->innerjoinpath, dml) &&
		(innerd && IsLocatorReplicated(innerd->distributionType)) &&
		(pathnode->jointype == JOIN_INNER ||
		 pathnode->jointype == JOIN_SEMI_SCALAR ||
		 pathnode->jointype == JOIN_LEFT ||
		 pathnode->jointype == JOIN_LEFT_SEMI_SCALAR ||
		 pathnode->jointype == JOIN_LEFT_SEMI ||
		 pathnode->jointype == JOIN_SEMI ||
		 pathnode->jointype == JOIN_ANTI))
	{
		/* We need inner relation is defined on all nodes where outer is */
		if (!outerd || !bms_is_subset(outerd->nodes, innerd->nodes))
			goto not_allowed_join;

		if (dml && root->distribution &&
			IsLocatorReplicated(root->distribution->distributionType) &&
			!IsLocatorReplicated(outerd->distributionType))
			goto not_allowed_join;

		pathnode->innerjoinpath = set_limit_gather_rep_path(root,
				pathnode->innerjoinpath, innerpathkeys);

		targetd = makeNode(Distribution);
		targetd->distributionType = outerd->distributionType;
		targetd->nodes = bms_copy(outerd->nodes);
		targetd->restrictNodes = bms_copy(outerd->restrictNodes);
#ifdef __OPENTENBASE_C__
		targetd->nExprs = outerd->nExprs;
		targetd->disExprs = outerd->disExprs;
#endif
		targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
		pathnode->path.distribution = targetd;
		return NIL;
	}

	/*
	 * Check if we have outer replicated
	 * The "both replicated" case is already checked, so if outerd
	 * is replicated, then innerd is not replicated and it is not NULL.
	 * This case is not acceptable for some join types. If inner relation is
	 * nullable data nodes will produce joined rows with NULLs for cases when
	 * matching row exists, but on other data node.
	 */
	if (is_allow_replication_path(top_root, pathnode->outerjoinpath, dml) &&
	    (outerd && IsLocatorReplicated(outerd->distributionType)) &&
		(pathnode->jointype == JOIN_INNER ||
		 pathnode->jointype == JOIN_SEMI_SCALAR ||
		 pathnode->jointype == JOIN_RIGHT ||
		 pathnode->jointype == JOIN_SEMI_RIGHT ||
		 pathnode->jointype == JOIN_ANTI_RIGHT))
	{
		/* We need outer relation is defined on all nodes where inner is */
		if (!innerd || !bms_is_subset(innerd->nodes, outerd->nodes))
			goto not_allowed_join;

		if (dml && root->distribution &&
			IsLocatorReplicated(root->distribution->distributionType) &&
			!IsLocatorReplicated(innerd->distributionType))
			goto not_allowed_join;

		pathnode->outerjoinpath = set_limit_gather_rep_path(root,
				pathnode->outerjoinpath, outerpathkeys);

		targetd = makeNode(Distribution);
		targetd->distributionType = innerd->distributionType;
		targetd->nodes = bms_copy(innerd->nodes);
		targetd->restrictNodes = bms_copy(innerd->restrictNodes);
#ifdef __OPENTENBASE_C__
		targetd->nExprs = innerd->nExprs;
		targetd->disExprs = innerd->disExprs;
#endif
		targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
		pathnode->path.distribution = targetd;
		return NIL;
	}

	/*
	 * If both sides are restricted to the same node, there is no need to check
	 * restrictinfo.
	 */
	if (innerd && outerd &&
		bms_equal(innerd->restrictNodes, outerd->restrictNodes) &&
		bms_num_members(innerd->restrictNodes) == 1)
	{
		if (!bms_equal(innerd->nodes, outerd->nodes))
			goto not_allowed_join;

		if (keepResultRelLoc)
		{
			if (resultRelLoc == RESULT_REL_INNER)
				pathnode->path.distribution = innerd;
			else if (resultRelLoc == RESULT_REL_OUTER)
				pathnode->path.distribution = outerd;
			return NIL;
		}

		/*
		 * In case of outer join distribution key should not refer
		 * distribution key of nullable part.
		 */
		if (pathnode->jointype == JOIN_FULL)
		{
			/* both parts are nullable */
			targetd = makeNode(Distribution);
			targetd->distributionType = LOCATOR_TYPE_NONE;
			targetd->nodes = bms_copy(innerd->nodes);
			targetd->restrictNodes = bms_copy(innerd->restrictNodes);
		}
		else if (pathnode->jointype == JOIN_RIGHT ||
				 pathnode->jointype == JOIN_SEMI_RIGHT ||
				 pathnode->jointype == JOIN_ANTI_RIGHT)
		{
			targetd = copyObject(innerd);
		}
		else
		{
			targetd = copyObject(outerd);
		}
		pathnode->path.distribution = targetd;
		return NIL;
	}

	if (innerd && outerd &&
		bms_num_members(outerd->restrictNodes) == 1 &&
		IsLocatorReplicated(innerd->distributionType) && !innerd->restrictNodes)
	{
		if (!bms_equal(innerd->nodes, outerd->nodes))
			goto not_allowed_join;

		if (keepResultRelLoc)
		{
			if (resultRelLoc == RESULT_REL_INNER)
				goto not_allowed_join;
			else if (resultRelLoc == RESULT_REL_OUTER)
				pathnode->path.distribution = outerd;
			return NIL;
		}

		/*
		 * In case of outer join distribution key should not refer
		 * distribution key of nullable part.
		 */
		if (pathnode->jointype == JOIN_FULL ||
			pathnode->jointype == JOIN_RIGHT ||
			pathnode->jointype == JOIN_SEMI_RIGHT ||
			pathnode->jointype == JOIN_ANTI_RIGHT)
		{
			targetd = makeNode(Distribution);
			targetd->distributionType = LOCATOR_TYPE_NONE;
			targetd->nodes = bms_copy(outerd->nodes);
			targetd->restrictNodes = bms_copy(outerd->restrictNodes);
			targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
		}
		else
		{
			targetd = copyObject(outerd);
		}
		pathnode->path.distribution = targetd;
		return NIL;
	}

	if (innerd && outerd &&
		bms_num_members(innerd->restrictNodes) == 1 &&
		IsLocatorReplicated(outerd->distributionType) && !outerd->restrictNodes)
	{
		if (!bms_equal(innerd->nodes, outerd->nodes))
			goto not_allowed_join;

		if (keepResultRelLoc)
		{
			if (resultRelLoc == RESULT_REL_INNER)
				pathnode->path.distribution = innerd;
			else if (resultRelLoc == RESULT_REL_OUTER)
				goto not_allowed_join;
			return NIL;
		}

		/*
		 * In case of outer join distribution key should not refer
		 * distribution key of nullable part.
		 */
		if (pathnode->jointype == JOIN_FULL ||
			pathnode->jointype == JOIN_LEFT ||
			pathnode->jointype == JOIN_SEMI ||
			pathnode->jointype == JOIN_ANTI ||
			pathnode->jointype == JOIN_LEFT_SEMI ||
			pathnode->jointype == JOIN_LEFT_SEMI_SCALAR ||
			pathnode->jointype == JOIN_SEMI_SCALAR)
		{
			targetd = makeNode(Distribution);
			targetd->distributionType = LOCATOR_TYPE_NONE;
			targetd->nodes = bms_copy(innerd->nodes);
			targetd->restrictNodes = bms_copy(innerd->restrictNodes);
			targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
		}
		else
		{
			targetd = copyObject(innerd);
		}
		pathnode->path.distribution = targetd;
		return NIL;
	}

	restrictClauses = list_copy(pathnode->joinrestrictinfo);
	restrictClauses = list_concat(restrictClauses, pathnode->movedrestrictinfo);

	set_join_mcv_list(root, pathnode, restrictClauses);

	/*
	 * This join is still allowed if inner and outer paths have equivalent
	 * distribution and joined along the distribution keys. Make sure
	 * distribution functions are the same, for now they depend on data type.
	 */
	if (IsValidDistribution(innerd) &&
		IsValidDistribution(outerd) &&
		innerd->distributionType == outerd->distributionType &&
		innerd->nExprs == outerd->nExprs &&
		bms_equal(innerd->nodes, outerd->nodes))
	{
		int			i = 0;
		int			nMatches = 0;
		bool	   *dis_outer = (bool *)palloc0(sizeof(bool) * innerd->nExprs);
		bool	   *dis_inner = (bool *)palloc0(sizeof(bool) * innerd->nExprs);
		ListCell   *lc;
		bool		reset = true;

		/*
		 * Planner already did necessary work and if there is a join
		 * condition like left.key=right.key the key expressions
		 * will be members of the same equivalence class, and both
		 * sides of the corresponding RestrictInfo will refer that
		 * Equivalence Class.
		 * Try to figure out if such restriction exists.
		 */
		foreach(lc, restrictClauses)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			ListCell	 *emc = NULL;

			/* Restriction operator is not equality operator ? */
			if (ri->left_ec == NULL || ri->right_ec == NULL)
				continue;
			/* do not use pseudoconstant conditions */
			if (ri->pseudoconstant)
				continue;
			/*
			 * A restriction with OR may be compatible if all OR'ed
			 * conditions are compatible. For the moment we do not
			 * check this and skip restriction. The case if multiple
			 * OR'ed conditions are compatible is rare and probably
			 * do not worth doing at all.
			 */
			if (ri->orclause)
				continue;
			/*
			 * Can not use RestrictInfo that is NOT the join condition of an
			 * OUTER JOIN as new distribution key!
			 */
			if (IS_OUTER_JOIN(pathnode->jointype) && ri->is_pushed_down)
				continue;
			/* Can not use if not hashable */
			if (!OidIsValid(ri->hashjoinoperator))
				continue;

			/*
			 * To support multi-dist column, we have to reset all these flags
			 * together.
			 */
			if (reset)
			{
				for (i = 0; i < innerd->nExprs; i++)
				{
					dis_outer[i] = false;
					dis_inner[i] = false;
				}
				reset = false;
			}

			/*
			 * If parts belong to the same equivalence member check
			 * if both distribution keys are members of the class.
			 */
			if (ri->left_ec == ri->right_ec)
			{
				foreach(emc, ri->left_ec->ec_members)
				{
					EquivalenceMember *em 	= (EquivalenceMember *) lfirst(emc);
					Expr			  *var 	= (Expr *)em->em_expr;

					while (var && IsA(var, RelabelType))
						var = ((RelabelType *) var)->arg;
					for (i = 0; i < innerd->nExprs; i++)
					{
						if (!dis_outer[i])
							dis_outer[i] = equal(var, outerd->disExprs[i]);
						if (!dis_inner[i])
							dis_inner[i] = equal(var, innerd->disExprs[i]);
					}
				}

				nMatches = 0;
				for (i = 0; i < innerd->nExprs; i++)
				{
					if (!dis_outer[i] || !dis_inner[i])
					{
						dis_outer[i] = false;
						dis_inner[i] = false;
					}
					else
					{
						/* both true */
						nMatches++;
					}
				}

				if (innerd->nExprs == nMatches)
				{
					ListCell *tlc;

					targetd = makeNode(Distribution);
					targetd->distributionType = innerd->distributionType;
					targetd->nodes = bms_copy(innerd->nodes);
					pathnode->path.distribution = targetd;

					targetd->restrictNodes = bms_copy(innerd->restrictNodes);
					targetd->nExprs = innerd->nExprs;
					targetd->disExprs = (Node **) palloc0(sizeof(Node *) * targetd->nExprs);

					nMatches = 0;
					/*
					 * prefer some from the target list, avoid to generate junk TargetEntry
					 */
					foreach(tlc, pathnode->path.parent->reltarget->exprs)
					{
						Expr *var = (Expr *) lfirst(tlc);
						for (i = 0; i < innerd->nExprs; i++)
						{
							if (!targetd->disExprs[i])
							{
								if (equal(var, innerd->disExprs[i]) ||
									equal(var, outerd->disExprs[i]))
								{
									nMatches++;
									targetd->disExprs[i] = (Node *) var;
									break;
								}
							}
						}
						if (nMatches == innerd->nExprs)
							break;
					}
					/* not found, take any */
					if (nMatches != innerd->nExprs)
						targetd->disExprs = innerd->disExprs;
#ifdef __OPENTENBASE__
					/*
					 * For UPDATE/DELETE, make sure we are distributing by
					 * the result relation.
					 */
					if (keepResultRelLoc &&
						!equal_distributions(top_root, (Path *)pathnode))
					{
						pfree(targetd);
						pathnode->path.distribution = targetd = NULL;
						reset = true;
						continue;
					}
#endif
					return alternate;
				}
			}
			/*
			 * Check clause, if both arguments are distribution keys and
			 * operator is an equality operator
			 */
			else
			{
				OpExpr *op_exp;
				Expr   *arg1, *arg2;

				op_exp = (OpExpr *) ri->clause;
				if (!IsA(op_exp, OpExpr) || list_length(op_exp->args) != 2)
					continue;

				arg1 = (Expr *) linitial(op_exp->args);
				arg2 = (Expr *) lsecond(op_exp->args);
				while (arg1 && IsA(arg1, RelabelType))
					arg1 = ((RelabelType *) arg1)->arg;
				while (arg2 && IsA(arg2, RelabelType))
					arg2 = ((RelabelType *) arg2)->arg;

				for(i = 0; i < innerd->nExprs; i++)
				{
					if (!dis_outer[i] && !dis_inner[i])
					{
						if ((equal(arg1, outerd->disExprs[i]) && equal(arg2, innerd->disExprs[i])) ||
							(equal(arg1, innerd->disExprs[i]) && equal(arg2, outerd->disExprs[i])))
						{
							dis_outer[i] = true;
							dis_inner[i] = true;
							break;
						}
					}
				}

				nMatches = 0;
				for (i = 0; i < innerd->nExprs; i++)
				{
					if (!dis_outer[i] || !dis_inner[i])
					{
						dis_outer[i] = false;
						dis_inner[i] = false;
					}
					else
					{
						/* both true */
						nMatches++;
					}
				}

				if (innerd->nExprs == nMatches)
				{
					/*
					 * In case of outer join distribution key should not refer
					 * distribution key of nullable part.
					 */
					if (pathnode->jointype == JOIN_FULL)
					{
						/* both parts are nullable */
						targetd = makeNode(Distribution);
						targetd->distributionType = LOCATOR_TYPE_NONE;
						targetd->nodes = bms_copy(innerd->nodes);
						if (outerd->restrictNodes && innerd->restrictNodes)
							targetd->restrictNodes = bms_union(outerd->restrictNodes,
															   innerd->restrictNodes);
						else
							targetd->restrictNodes = NULL;
					}
					else if (pathnode->jointype == JOIN_RIGHT ||
							 pathnode->jointype == JOIN_SEMI_RIGHT ||
							 pathnode->jointype == JOIN_ANTI_RIGHT)
					{
						targetd = copyObject(innerd);
					}
					else
					{
						targetd = copyObject(outerd);
					}
					targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
					pathnode->path.distribution = targetd;
#ifdef __OPENTENBASE__
					/*
					 * For UPDATE/DELETE, make sure we are distributing by
					 * the result relation.
					 */
					if (keepResultRelLoc &&
						!equal_distributions(top_root, (Path *)pathnode))
					{
						pfree(targetd);
						pathnode->path.distribution = targetd = NULL;
						reset = true;
						continue;
					}
#endif
					return alternate;
				}
			}
		}
	}

	/*
	 * If we could not determine the distribution redistribute the subpathes.
	 */
not_allowed_join:
	/*
	 * Redistribute subplans to make them compatible.
	 * If any of the subplans is a coordinator subplan skip this stuff and do
	 * coordinator join.
	 */
	if (innerd && outerd)
	{
		RestrictInfo   *preferred = NULL;
		int            ninnerkeys = 0;
		int            nouterkeys = 0;
		Expr           **inner_keys = NULL;
		Expr           **outer_keys = NULL;
		char			distType = LOCATOR_TYPE_SHARD;
		ListCell 	   *lc;
		bool			is_better_rinfo;
#ifdef __OPENTENBASE__
		int            i = 0;
		int            nokeys = 0;
		int            nikeys = 0;
		Expr           **dis_okeys = NULL;
		Expr           **dis_ikeys = NULL;
		int            omatches = 0;
		int            imatches = 0;
		int            nlkeys = 0;
		int            nrkeys = 0;
		Expr           **lkeys = NULL;
		Expr           **rkeys = NULL;
		int            len_clauses = list_length(restrictClauses);
		bool           check_dml = true;
#endif

		/*
		 * Look through the join restrictions to find one that is a hashable
		 * operator on two arguments. Choose best restriction acoording to
		 * following criteria:
		 * 1. one argument is already a partitioning key of one subplan.
		 * 2. restriction is cheaper to calculate
		 */
		foreach(lc, restrictClauses)
		{
			RestrictInfo   *ri = (RestrictInfo *) lfirst(lc);

			/* do not use pseudoconstant conditions */
			if (ri->pseudoconstant)
				continue;
			/* can not handle ORed conditions */
			if (ri->orclause)
				continue;
			/*
			 * Can not use RestrictInfo that is NOT the join condition of an
			 * OUTER JOIN as new distribution key!
			 */
			if (IS_OUTER_JOIN(pathnode->jointype) && ri->is_pushed_down)
				continue;

			if (IsA(ri->clause, OpExpr))
			{
				OpExpr *expr = (OpExpr *) ri->clause;
				if (list_length(expr->args) == 2 &&
					op_hashjoinable(expr->opno, exprType(linitial(expr->args))))
				{
					Expr *left = (Expr *) linitial(expr->args);
					Expr *right = (Expr *) lsecond(expr->args);
					Expr *left_expr = left;
					Expr *right_expr = right;
					Relids inner_rels = pathnode->innerjoinpath->parent->relids;
					Relids outer_rels = pathnode->outerjoinpath->parent->relids;
					QualCost cost;

#ifdef __OPENTENBASE_C__
					is_better_rinfo = is_better_restrictInfo(pathnode, preferred, ri);

					if ((IsMultiColsDistribution(outerd)) || IsMultiColsDistribution(innerd))
					{
						nikeys = outerd->nExprs;
						if (!dis_ikeys)
						{
							dis_ikeys = (Expr **) palloc0(sizeof(Expr *) * nikeys);
						}
						for (i = 0; i < nikeys; i++)
						{
							while (left && IsA(left, RelabelType))
								left = ((RelabelType *) left)->arg;

							if (get_joinpath_disexprs(&dis_ikeys[i], outerd->disExprs[i], left, &omatches,
													  right, ri->right_relids, inner_rels))
								break;

							while (right && IsA(right, RelabelType))
								right = ((RelabelType *) right)->arg;

							if (get_joinpath_disexprs(&dis_ikeys[i], outerd->disExprs[i], right, &omatches,
													  left, ri->left_relids, inner_rels))
								break;
						}
						
						nokeys = innerd->nExprs;
						if (!dis_okeys)
						{
							dis_okeys = (Expr **) palloc0(sizeof(Expr *) * nokeys);
						}
						for (i = 0; i < nokeys; i++)
						{
							while (left && IsA(left, RelabelType))
								left = ((RelabelType *) left)->arg;

							if (get_joinpath_disexprs(&dis_okeys[i], innerd->disExprs[i], left, &imatches,
													  right, ri->right_relids, outer_rels))
								break;

							while (right && IsA(right, RelabelType))
								right = ((RelabelType *) right)->arg;

							if (get_joinpath_disexprs(&dis_okeys[i], innerd->disExprs[i], right, &imatches,
													  left, ri->left_relids, outer_rels))
								break;
						}

						if (!lkeys)
							lkeys = (Expr **) palloc0(sizeof(Expr *) * len_clauses);
						if (!rkeys)
							rkeys = (Expr **) palloc0(sizeof(Expr *) * len_clauses);
						get_joinpath_fullexprs(&lkeys[nlkeys], &rkeys[nrkeys], ri, outer_rels, inner_rels,
											   left, right);
						if (lkeys[nlkeys] && rkeys[nrkeys])
						{
							nlkeys++;
							nrkeys++;
						}
					}
#endif
					{
						Expr *expr1 = (Expr *)get_var_from_arg((Node *)left);
						Expr *expr2 = (Expr *)get_var_from_arg((Node *)right);

						if (expr1)
						{
							left_expr = expr1;
						}

						if (expr2)
						{
							right_expr = expr2;
						}

						if (IsA(left_expr, Const) || IsA(right_expr, Const))
						{
							continue;
						}
					}
					/*
					 * Evaluation cost will be needed to choose preferred
					 * distribution
					 */
					cost_qual_eval_node(&cost, (Node *) ri, root);

					if (IsSingleColDistribution(outerd) &&
						resultRelLoc != RESULT_REL_INNER)
					{
						/*
						 * If left side is distribution key of outer subquery
						 * and right expression refers only inner subquery
						 */
						if (equal(outerd->disExprs[0], left_expr) &&
							bms_is_subset(ri->right_relids, inner_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
								(ninnerkeys > 0 && nouterkeys > 0) || /* preferred restriction require redistribution of both parts */
								(cost.per_tuple < preferred->eval_cost.per_tuple) || /* current restriction is cheaper */
								is_better_rinfo)
							{
								/* set new preferred restriction */
								preferred = ri;
								if (!inner_keys)
								{
									ninnerkeys = 1;
									inner_keys = (Expr **)palloc0(sizeof(Expr *) * ninnerkeys);
								}
								inner_keys[0] = right;
								nouterkeys = 0;
								outer_keys = NULL;
								distType = outerd->distributionType;
							}
							continue;
						}
						/*
						 * If right side is distribution key of outer subquery
						 * and left expression refers only inner subquery
						 */
						if (equal(outerd->disExprs[0], right_expr) &&
								bms_is_subset(ri->left_relids, inner_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
								(ninnerkeys > 0 && nouterkeys > 0) || /* preferred restriction require redistribution of both parts */
								(cost.per_tuple < preferred->eval_cost.per_tuple) || /* current restriction is cheaper */
								is_better_rinfo)
							{
								/* set new preferred restriction */
								preferred = ri;
								
								if (!inner_keys)
								{
									ninnerkeys = 1;
									inner_keys = (Expr **)palloc0(sizeof(Expr *) * ninnerkeys);
								}
								inner_keys[0] = left;
								nouterkeys = 0;
								outer_keys = NULL;
								distType = outerd->distributionType;
							}
							continue;
						}
					}
					if (IsSingleColDistribution(innerd) &&
						resultRelLoc != RESULT_REL_OUTER)
					{
						/*
						 * If left side is distribution key of inner subquery
						 * and right expression refers only outer subquery
						 */
						if (equal(innerd->disExprs[0], left_expr) &&
							bms_is_subset(ri->right_relids, outer_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
								(ninnerkeys > 0 && nouterkeys > 0) || /* preferred restriction require redistribution of both parts */
								(cost.per_tuple < preferred->eval_cost.per_tuple) || /* current restriction is cheaper */
								is_better_rinfo)
							{
								/* set new preferred restriction */
								preferred = ri;
								if (!outer_keys)
								{
									nouterkeys = 1;
									outer_keys = (Expr **) palloc0(sizeof(Expr *) * nouterkeys);
								}
								ninnerkeys = 0;
								inner_keys = NULL;
								outer_keys[0] = right;
								distType = innerd->distributionType;
							}
							continue;
						}
						/*
						 * If right side is distribution key of inner subquery
						 * and left expression refers only outer subquery
						 */
						if (equal(innerd->disExprs[0], right_expr) &&
							bms_is_subset(ri->left_relids, outer_rels))
						{
							if (!preferred || /* no preferred restriction yet found */
								(ninnerkeys > 0 && nouterkeys > 0) || /* preferred restriction require redistribution of both parts */
								(cost.per_tuple < preferred->eval_cost.per_tuple) || /* current restriction is cheaper */
								is_better_rinfo)
							{
								/* set new preferred restriction */
								preferred = ri;
								if (!outer_keys)
								{
									nouterkeys = 1;
									outer_keys = (Expr **) palloc0(sizeof(Expr *) * nouterkeys);
								}
								ninnerkeys = 0;
								inner_keys = NULL;
								outer_keys[0] = left;
								distType = innerd->distributionType;
							}
							continue;
						}
					}
					/*
					 * Current restriction recuire redistribution of both parts.
					 * If preferred restriction require redistribution of one,
					 * keep it.
					 */
					if (preferred && (ninnerkeys == 0 || nouterkeys == 0) &&
						is_better_rinfo == false)
						continue;
#ifdef __OPENTENBASE__
					/*
					 * Skip redistribute both side, which will redistribute the
					 * result relation
					 */
					if (keepResultRelLoc)
						continue;
#endif
					/*
					 * If this restriction the first or easier to calculate
					 * then preferred, try to store it as new preferred
					 * restriction to redistribute along it.
					 */
					if (preferred == NULL ||
						(cost.per_tuple < preferred->eval_cost.per_tuple)||
						is_better_rinfo)
					{
						/*
						 * Left expression depends only on outer subpath and
						 * right expression depends only on inner subpath, so
						 * we can redistribute both and make left expression the
						 * distribution key of outer subplan and right
						 * expression the distribution key of inner subplan
						 */
						if (bms_is_subset(ri->left_relids, outer_rels) &&
							bms_is_subset(ri->right_relids, inner_rels))
						{
							preferred = ri;
							if (!inner_keys)
							{
								ninnerkeys = 1;
								inner_keys = (Expr **)palloc0(sizeof(Expr *) * ninnerkeys);
							}
							if (!outer_keys)
							{
								nouterkeys = 1;
								outer_keys = (Expr **)palloc0(sizeof(Expr *) * nouterkeys);
							}
							outer_keys[0] = left;
							inner_keys[0] = right;
						}
						/*
						 * Left expression depends only on inner subpath and
						 * right expression depends only on outer subpath, so
						 * we can redistribute both and make left expression the
						 * distribution key of inner subplan and right
						 * expression the distribution key of outer subplan
						 */
						if (bms_is_subset(ri->left_relids, inner_rels) &&
							bms_is_subset(ri->right_relids, outer_rels))
						{
							preferred = ri;
							if (!inner_keys)
							{
								ninnerkeys = 1;
								inner_keys = (Expr **)palloc0(sizeof(Expr *) * ninnerkeys);
							}
							if (!outer_keys)
							{
								nouterkeys = 1;
								outer_keys = (Expr **)palloc0(sizeof(Expr *) * nouterkeys);
							}
							inner_keys[0] = left;
							outer_keys[0] = right;
						}
					}
				}
			}
		}

		/*
		 * Reorder the distribution keys if there are preferred order for multi distribution keys.
		 */
		if (preferred)
			check_dml = reorder_distribution_keys(root, lkeys, rkeys, &nlkeys, &nrkeys,
												  resultRelLoc, pathnode);
		else
			check_dml = false;

		/* If we have suitable restriction we can repartition accordingly */
		if (preferred && check_dml)
		{
			Bitmapset *nodes = NULL;
			Bitmapset *restrictNodes = NULL;
#ifdef __OPENTENBASE__
			/* consider the outer/inner size when make the redistribute plan */
			bool replicate_inner = false;
			bool replicate_outer = false;
			RelOptInfo *outer_rel = pathnode->outerjoinpath->parent;
			RelOptInfo *inner_rel = pathnode->innerjoinpath->parent;
			double outer_size = outer_rel->rows * outer_rel->reltarget->width;
			double inner_size = inner_rel->rows * inner_rel->reltarget->width;
			int outer_nodes = bms_num_members(outerd->nodes);
			int inner_nodes = bms_num_members(innerd->nodes);
			bool is_roundrobin = false;

			/*
			 * If the only preferred cluase has const, choose round robin method.
			 */
			if (is_restrictInfo_has_const(preferred))
			{
				is_roundrobin = true;
				dist_mask = ENABLE_DISTRIBUTE;
			}

			if (IsMultiColsDistribution(outerd) || IsMultiColsDistribution(innerd))
			{
				bool outer_match = false;
				bool inner_match = false;
				int  lmatch = omatches;
				int  rmatch = imatches;

				if (omatches && resultRelLoc == RESULT_REL_NONE)
				{
					if (omatches == outerd->nExprs)
						outer_match = true;
				}
				if (imatches && resultRelLoc == RESULT_REL_NONE)
				{
					if (imatches == innerd->nExprs)
						inner_match = true;
				}
				if (outer_match && inner_match && resultRelLoc == RESULT_REL_NONE)
				{
					if (lmatch >= rmatch)
					{
						nouterkeys = 0;
						outer_keys = NULL;
						ninnerkeys = nikeys;
						inner_keys = dis_ikeys;
						distType = outerd->distributionType;
					}
					else
					{
						ninnerkeys = 0;
						inner_keys = NULL;
						nouterkeys = nokeys;
						outer_keys = dis_okeys;
						distType = innerd->distributionType;
					}
				}
				else if (outer_match && resultRelLoc == RESULT_REL_NONE)
				{
					nouterkeys = 0;
					outer_keys = NULL;
					ninnerkeys = nikeys;
					inner_keys = dis_ikeys;
					distType = outerd->distributionType;
				}
				else if (inner_match && resultRelLoc == RESULT_REL_NONE)
				{
					ninnerkeys = 0;
					inner_keys = NULL;
					nouterkeys = nokeys;
					outer_keys = dis_okeys;
					distType = innerd->distributionType;
				}
				else if (resultRelLoc == RESULT_REL_NONE)
				{
					nouterkeys = nlkeys;
					outer_keys = lkeys;	
					ninnerkeys = nrkeys;
					inner_keys = rkeys;
					distType = LOCATOR_TYPE_SHARD;
				}
			}
#endif

			/* If we redistribute both parts do join on all nodes ... */
			if (ninnerkeys > 0 && nouterkeys > 0)
			{
#ifdef __OPENTENBASE__
				/*
				 * We should not get both new_inner_key & new_outer_key for
				 * UPDATE/DELETE
				 */
				Assert(!keepResultRelLoc);

				/*
				 * if one of both is smaller enough,
				 * replicate the small one instead of redistribute both
				 */
				if (HINT_REPLICATE_RIGHT(
						inner_size * outer_nodes * replication_threshold < inner_size + outer_size,
						dist_mask) &&
					(pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_RIGHT &&
					 pathnode->jointype != JOIN_SEMI_RIGHT &&
					 pathnode->jointype != JOIN_ANTI_RIGHT) &&
					outerd->distributionType != LOCATOR_TYPE_REPLICATED && !dml)
				{
					replicate_inner = true;

					nodes = bms_copy(outerd->nodes);
					restrictNodes = bms_copy(outerd->restrictNodes);
				}
				else if (HINT_REPLICATE_LEFT(
						outer_size * inner_nodes * replication_threshold < inner_size + outer_size,
						dist_mask) &&
					(pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_LEFT &&
					 pathnode->jointype != JOIN_SEMI &&
					 pathnode->jointype != JOIN_ANTI &&
					 pathnode->jointype != JOIN_LEFT_SEMI &&
					 pathnode->jointype != JOIN_LEFT_SEMI_SCALAR &&
					 pathnode->jointype != JOIN_SEMI_SCALAR) &&
					 innerd->distributionType != LOCATOR_TYPE_REPLICATED && !dml)
				{
					replicate_outer = true;

					nodes = bms_copy(innerd->nodes);
					restrictNodes = bms_copy(innerd->restrictNodes);
				}
				else
				{
					int      i;
					for(i = 0; i < NumDataNodes; i++)
						nodes = bms_add_member(nodes, i);
				}
#endif
			}
			/*
			 * ... if we do only one of them redistribute it on the same nodes
			 * as other.
			 */
			else if (ninnerkeys > 0)
			{
				/*
				 * If inner is smaller than outer, redistribute inner as the
				 * preferred key we picked.
				 * If inner is bigger than outer (inner > inner->nodes * outer),
				 * replicate outer as an optimization to save network costs.
				 */
				if (HINT_REPLICATE_LEFT(
					inner_size > outer_size * inner_nodes * replication_threshold,
					dist_mask) &&
					(pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_LEFT &&
					 pathnode->jointype != JOIN_SEMI &&
					 pathnode->jointype != JOIN_ANTI &&
					 pathnode->jointype != JOIN_LEFT_SEMI &&
					 pathnode->jointype != JOIN_LEFT_SEMI_SCALAR &&
					 pathnode->jointype != JOIN_SEMI_SCALAR) &&
					 innerd->distributionType != LOCATOR_TYPE_REPLICATED && !dml)
				{
					replicate_outer = true;

					/* replicate outer to all inner nodes */
					nodes = bms_copy(innerd->nodes);
					restrictNodes = bms_copy(innerd->restrictNodes);
				}
				else
				{
					if (pathnode->jointype != JOIN_FULL &&
						pathnode->jointype != JOIN_RIGHT &&
						pathnode->jointype != JOIN_SEMI_RIGHT &&
						pathnode->jointype != JOIN_ANTI_RIGHT)
					{
						nodes = bms_copy(outerd->nodes);
						restrictNodes = bms_copy(outerd->restrictNodes);
					}
					else
					{
						int      i;
						for(i = 0; i < NumDataNodes; i++)
							nodes = bms_add_member(nodes, i);
					}
				}
			}
			else /*if (new_outer_key)*/
			{
				/*
				 * If outer is smaller than inner, redistribute outer as the
				 * preferred key we picked.
				 * If outer is bigger than inner (outer > outer->nodes * inner),
				 * replicate inner as an optimization to save network costs.
				 */
				if (HINT_REPLICATE_RIGHT(
						outer_size > inner_size * outer_nodes * replication_threshold,
						dist_mask) &&
					(pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_RIGHT &&
					 pathnode->jointype != JOIN_SEMI_RIGHT &&
					 pathnode->jointype != JOIN_ANTI_RIGHT) &&
					outerd->distributionType != LOCATOR_TYPE_REPLICATED && !dml)
				{
					replicate_inner = true;
				
					/* replicate inner to all outer nodes */
					nodes = bms_copy(outerd->nodes);
					restrictNodes = bms_copy(outerd->restrictNodes);
				}
				else
				{
					if (pathnode->jointype != JOIN_FULL &&
						pathnode->jointype != JOIN_LEFT &&
						pathnode->jointype != JOIN_SEMI &&
						pathnode->jointype != JOIN_ANTI &&
						pathnode->jointype != JOIN_LEFT_SEMI &&
						pathnode->jointype != JOIN_LEFT_SEMI_SCALAR &&
						pathnode->jointype != JOIN_SEMI_SCALAR)
					{
						nodes = bms_copy(innerd->nodes);
						restrictNodes = bms_copy(innerd->restrictNodes);
					}
					else
					{
						int      i;
						for(i = 0; i < NumDataNodes; i++)
							nodes = bms_add_member(nodes, i);
					}
				}
			}

			/*
			 * Redistribute join by hash, and, if jointype allows, create
			 * alternate path where inner subplan is distributed by replication
			 */
			if (ninnerkeys > 0)
			{
#ifdef __OPENTENBASE__
				/* replicate outer rel */
				if (replicate_outer)
				{
					pathnode->outerjoinpath = redistribute_path(
												root,
												pathnode->outerjoinpath,
												outerpathkeys,
												LOCATOR_TYPE_REPLICATED,
#ifdef __OPENTENBASE_C__
												0, NULL,
#endif
												nodes,
												NULL);
					if (IsA(pathnode, MergePath))
						((MergePath*)pathnode)->outersortkeys = NIL;
				}
				else if (!replicate_inner)
				{
#endif
					/* Redistribute inner subquery */
					pathnode->innerjoinpath = redistribute_path(
												root,
												pathnode->innerjoinpath,
												innerpathkeys,
												distType,
#ifdef __OPENTENBASE_C__
												ninnerkeys, (Node **)inner_keys,
#endif
												nodes,
												restrictNodes);

					if (IsA(pathnode, MergePath))
						((MergePath*)pathnode)->innersortkeys = NIL;
#ifdef __OPENTENBASE__
				}
#endif
			}
			/*
			 * Redistribute join by hash, and, if jointype allows, create
			 * alternate path where outer subplan is distributed by replication
			 */
			if (nouterkeys > 0)
			{
#ifdef __OPENTENBASE__
				/* replicate inner rel */
				if (replicate_inner)
				{
					pathnode->innerjoinpath = redistribute_path(
												root,
												pathnode->innerjoinpath,
												innerpathkeys,
												LOCATOR_TYPE_REPLICATED,
#ifdef __OPENTENBASE_C__
												0, NULL,
#endif
												nodes,
												NULL);

					if (IsA(pathnode, MergePath))
						((MergePath*)pathnode)->innersortkeys = NIL;
				}
				else if (!replicate_outer)
				{
#endif
					/* Redistribute outer subquery */
					pathnode->outerjoinpath = redistribute_path(
												root,
												pathnode->outerjoinpath,
												outerpathkeys,
												distType,
#ifdef __OPENTENBASE_C__
												nouterkeys, (Node **)outer_keys,
#endif
												nodes,
												restrictNodes);
				if (IsA(pathnode, MergePath))
					((MergePath*)pathnode)->outersortkeys = NIL;
#ifdef __OPENTENBASE__
				}
#endif
			}

			/*
			 * In case of outer join distribution key should not refer
			 * distribution key of nullable part.
			 * NB: we should not refer innerd and outerd here, subpathes are
			 * redistributed already
			 */
			if (pathnode->jointype == JOIN_FULL)
			{
				/* both parts are nullable */
				targetd = makeNode(Distribution);
				targetd->distributionType = LOCATOR_TYPE_NONE;
				targetd->nodes = nodes;
			}
			else if (pathnode->jointype == JOIN_RIGHT ||
					 pathnode->jointype == JOIN_SEMI_RIGHT ||
					 pathnode->jointype == JOIN_ANTI_RIGHT ||
					 resultRelLoc == RESULT_REL_INNER ||
					 replicate_outer)
			{
				targetd = copyObject(pathnode->innerjoinpath->distribution);
			}
			else
			{
				targetd = copyObject(pathnode->outerjoinpath->distribution);
			}
			targetd->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);
			pathnode->path.distribution = targetd;

			set_mixed_join_distribution(root, pathnode, preferred, nouterkeys, ninnerkeys,
			                            replicate_outer, replicate_inner, is_roundrobin);

			return alternate;
		}

#ifdef __OPENTENBASE__
		if (keepResultRelLoc)
		{
			/*
			 * We didn't got the preferred redistribution plan for UPDATE/DELETE.
			 * Thus, to keeping result relation not redistributed, we replicate
			 * the other subpath.
			 */
			if (resultRelLoc == RESULT_REL_INNER &&
				pathnode->jointype != JOIN_FULL &&
				pathnode->jointype != JOIN_LEFT &&
				pathnode->jointype != JOIN_SEMI &&
				pathnode->jointype != JOIN_ANTI &&
				pathnode->jointype != JOIN_LEFT_SEMI &&
				pathnode->jointype != JOIN_LEFT_SEMI_SCALAR &&
				pathnode->jointype != JOIN_SEMI_SCALAR &&
				(!pathnode->inner_unique || check_dml == false))
			{
				/* Replicate outer */
				pathnode->outerjoinpath = redistribute_path(
											root,
											pathnode->outerjoinpath,
											outerpathkeys,
											LOCATOR_TYPE_REPLICATED,
#ifdef __OPENTENBASE_C__
											0, NULL,
#endif
											innerd->nodes,
											NULL);
				pathnode->path.distribution = innerd;

				if (IsA(pathnode, MergePath))
					((MergePath*)pathnode)->outersortkeys = NIL;
			}
			else if (resultRelLoc == RESULT_REL_OUTER &&
					 pathnode->jointype != JOIN_FULL &&
					 pathnode->jointype != JOIN_RIGHT &&
					 pathnode->jointype != JOIN_SEMI_RIGHT &&
					 pathnode->jointype != JOIN_ANTI_RIGHT &&
					(!pathnode->inner_unique || check_dml == false))
			{
				/* Replicate inner */
				pathnode->innerjoinpath = redistribute_path(
											root,
											pathnode->innerjoinpath,
											innerpathkeys,
											LOCATOR_TYPE_REPLICATED,
#ifdef __OPENTENBASE_C__
											0, NULL,
#endif
											outerd->nodes,
											NULL);
				pathnode->path.distribution = outerd;

				if (IsA(pathnode, MergePath))
					((MergePath*)pathnode)->innersortkeys = NIL;
			}

			return alternate;
		}
	}

	/*
	 * For DELETE/UPDATE, If the other side already been replicated, we directly
	 * inherit the resultRelLoc side distribution.
	 */
	if (keepResultRelLoc)
	{
		if (innerd && resultRelLoc == RESULT_REL_INNER &&
			pathnode->jointype != JOIN_FULL &&
			pathnode->jointype != JOIN_LEFT &&
			pathnode->jointype != JOIN_SEMI &&
			pathnode->jointype != JOIN_ANTI &&
			pathnode->jointype != JOIN_LEFT_SEMI &&
			pathnode->jointype != JOIN_LEFT_SEMI_SCALAR &&
			pathnode->jointype != JOIN_SEMI_SCALAR)
		{
			pathnode->path.distribution = innerd;
			return alternate;
		}
		else if (outerd && resultRelLoc == RESULT_REL_OUTER &&
				 pathnode->jointype != JOIN_FULL &&
				 pathnode->jointype != JOIN_RIGHT &&
				 pathnode->jointype != JOIN_SEMI_RIGHT &&
				 pathnode->jointype != JOIN_ANTI_RIGHT)
		{
			pathnode->path.distribution = outerd;
			return alternate;
		}
#endif
	}

	/*
	 * Build cartesian product, if no hasheable restrictions is found.
	 * Perform coordinator join in such cases. If this join would be a part of
	 * larger join, it will be handled as replicated.
	 * To do that leave join distribution NULL and place a RemoteSubPath node on
	 * top of each subpath to provide access to joined result sets.
	 * Do not redistribute pathes that already have NULL distribution, this is
	 * possible if performing outer join on a coordinator and a datanode
	 * relations.
	 */
pull_up:
	if (innerd)
	{
		pathnode->innerjoinpath = redistribute_path(root,
													pathnode->innerjoinpath,
													innerpathkeys,
													LOCATOR_TYPE_NONE,
#ifdef __OPENTENBASE_C__
													0, NULL,
#endif
													NULL,
													NULL);

		if (IsA(pathnode, MergePath))
			((MergePath*)pathnode)->innersortkeys = NIL;
	}

	if (outerd)
	{
		pathnode->outerjoinpath = redistribute_path(root,
													pathnode->outerjoinpath,
													outerpathkeys,
													LOCATOR_TYPE_NONE,
#ifdef __OPENTENBASE_C__
													0, NULL,
#endif
													NULL,
													NULL);
		if (IsA(pathnode, MergePath))
			((MergePath*)pathnode)->outersortkeys = NIL;
	}

	if (pathnode->path.distribution != NULL)
		pathnode->path.distribution->replication_for_update = (innerd && innerd->replication_for_update) ||
										(outerd && outerd->replication_for_update);;
	return alternate;
}

#ifdef __OPENTENBASE__
/* count remotesubplans in path */
bool
contains_remotesubplan(Path *path, int *number)
{
	if (path == NULL)
		return false;

	if (IsA(path, RemoteSubPath))
		(*number)++;

	return path_tree_walker(path, contains_remotesubplan, number);
}

/* detect whether there is an un-distributed scan path */
bool
reach_scanpath(Path *path, void *useless)
{
	if (path == NULL)
		return false;

	if (IsA(path, RemoteSubPath))
		return false;

	if ((path->pathtype == T_SeqScan || IsA(path, IndexPath) || IsA(path, BitmapHeapPath)) &&
		path->distribution != NULL)
		return true;

	return path_tree_walker(path, reach_scanpath, useless);
}


/*
 * redistribute local grouping results among datanodes, then
 * get the final grouping results. seems more efficient...
 *
 * Tips: we do not check the grouping column's type, directly use that
 * as hash column, but some data types are not supported as hash column now,
 * maybe some errors.
 */
Path *
create_redistribute_grouping_path(PlannerInfo *root, Query *parse, Path *path)
{
	PathTarget *pathtarget = path->pathtarget;
	
	if (parse->groupingSets)
		/* not implement now */
		elog(ERROR, "grouping sets not supported now!");

	if (parse->groupClause)
	{
		/*
		  * current implemention is quite rough, need more work.
		  */
		int i;
		Bitmapset *nodes = NULL;
		int nExprs = 0;
		Node **disExprs = NULL;
		int len = list_length(parse->groupClause);
		
		List *groupExprs = get_sortgrouplist_exprs(parse->groupClause,
												 parse->targetList);
		disExprs = (Node **)palloc0(sizeof(Node *) * len);
		/* choose group key which get max group numbers as distributed key */
		for ( i = 0; i < len; i++)
		{
			Node *groupExpr = list_nth(groupExprs, i);

			disExprs[nExprs++] = groupExpr;
		}

		list_free(groupExprs);

		for (i = 0; i < NumDataNodes; i++)
			nodes = bms_add_member(nodes, i);
		/*
		 * FIXING ME! check hash column's data type to satisfity hash locator func
		 */
		path = redistribute_path(root,
								 path,
								 NULL,
								 LOCATOR_TYPE_SHARD,
								 nExprs, disExprs,
								 nodes,
								 NULL);

		path->pathtarget = pathtarget;
		pathkey_sanity_check((Path*)path);

		return path;
	}

	if(parse->hasAggs)
	{
		if (root->query_level > 1)
			path = create_remotesubplan_replicated_path(root, path);
		else
			path = create_remotesubplan_path(root, path, NULL);

		path->pathtarget = pathtarget;
		pathkey_sanity_check((Path*)path);

		return path;
	}

	return NULL; /* keep compiler quiet */
}
#endif

#ifdef __OPENTENBASE_C__
Path *
generate_distribution_path(PlannerInfo *root, Path *subpath,
						   int nExprs, Node **disExprs)
{
	int i = 0;
	Path *path = NULL;
	Bitmapset *nodes = NULL;

	for (i = 0; i < NumDataNodes; i++)
		nodes = bms_add_member(nodes, i);

	/*
	 * FIXING ME! check hash column's data type to satisfity hash locator func
	 */
	path = redistribute_path(root,
							 subpath,
							 NULL,
							 LOCATOR_TYPE_SHARD,
							 nExprs, disExprs,
							 nodes,
							 NULL);

	path->pathtarget = subpath->pathtarget;
	return path;
}

Path *
create_window_distribution_path(PlannerInfo *root, Path *subpath,
								List *patitionClause)
{
	int i = 0;
	Path *path = NULL;
	int nExprs = 0;
	int len = list_length(patitionClause);
	Node **disExprs = (Node **)palloc0(sizeof(Node *) * len);

	/* choose group key which get max group numbers as distributed key */
	for (i = 0; i < len; i++)
	{
		Node *groupExpr = list_nth(patitionClause, i);
		if (IsTypeHashDistributable(exprType(groupExpr)))
		{
			disExprs[nExprs++] = groupExpr;
		}
	}

	if (nExprs > 0)
		path = generate_distribution_path(root, subpath, nExprs, disExprs);
	else
		path = create_remotesubplan_path(root, subpath, NULL);

	return path;
}

Path *
create_union_distribution_path(PlannerInfo *root, Path *subpath,
							   List *tlist, List *groupList)
{
	int nExprs = 0;
	int len = list_length(groupList);
	Node **disExprs;
	Path *path;
	ListCell *lc, *lg;

	if (len == 0)
	{
		path = create_remotesubplan_path(root, subpath, NULL);
		return path;
	}

	disExprs = (Node **)palloc0(sizeof(Node *) * len);
	lg = list_head(groupList);
	foreach(lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!tle->resjunk)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(lg);

			if (sgc->hashable)
				disExprs[nExprs++] = (Node *) tle->expr;

			lg = lnext(lg);
		}
	}

	if (nExprs > 0)
		path = generate_distribution_path(root, subpath, nExprs, disExprs);
	else
		path = create_remotesubplan_path(root, subpath, NULL);

	pathkey_sanity_check((Path *) path);

	return path;
}

Path *
create_distinct_distribution_path(PlannerInfo *root, Path *subpath,
								  List *tlist, List *clauses)
{
	Path *path = NULL;
	int i = 0;
	int nExprs = 0;
	int len = list_length(clauses);
	AttrNumber *groupColIdx = extract_grouping_cols(clauses, tlist);
	Node **disExprs = (Node **)palloc0(sizeof(Node *) * len);
	ListCell *lg;

	lg = list_head(clauses);
	for (i = 0; i < len; i++)
	{
		SortGroupClause *groupcl = (SortGroupClause *) lfirst(lg);

		if (groupcl->hashable)
		{
			TargetEntry *te = (TargetEntry *) list_nth(tlist,
													   groupColIdx[i] - 1);
			disExprs[nExprs++] = (Node *) te->expr;
		}

		lg = lnext(lg);
	}

	if (nExprs > 0)
		path = generate_distribution_path(root, subpath, nExprs, disExprs);
	else
		path = create_remotesubplan_path(root, subpath, NULL);

	pathkey_sanity_check((Path *) path);
	
	return path;
}

static void
set_uniquepath_distribution(PlannerInfo *root, UniquePath *path)
{
	Distribution *distribution = path->path.distribution;
	int len = list_length(path->uniq_exprs);
	int nExprs = 0;
	Node **disExprs;
	ListCell *lc;

	/* nothing to do if no distribution or single node */
	if (distribution == NULL ||
		IsLocatorReplicated(distribution->distributionType) ||
		bms_num_members(distribution->restrictNodes) == 1)
		return;

	/* nothing to do if matched distribution */
	if (IsSingleColDistribution(distribution))
	{
		foreach(lc, path->uniq_exprs)
		{
			Node *uniqexpr = (Node *)lfirst(lc);
			if (equal(uniqexpr, distribution->disExprs[0]))
				return;
		}
	}

	disExprs = (Node **)palloc0(sizeof(Node *) * len);
	foreach (lc, path->uniq_exprs)
	{
		Node *uniqexpr = (Node *) lfirst(lc);
		if (IsTypeHashDistributable(exprType(uniqexpr)))
		{
			disExprs[nExprs++] = uniqexpr;
		}
	}

	if (nExprs > 0)
		path->subpath = generate_distribution_path(root, path->subpath, nExprs, disExprs);
	else
		path->subpath = create_remotesubplan_path(root, path->subpath, NULL);

	path->path.distribution = path->subpath->distribution;
}
#endif

#endif

/*
 * create_seqscan_path
 *	  Creates a path corresponding to a sequential scan, returning the
 *	  pathnode.
 */
Path *
create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
					Relids required_outer, int parallel_workers)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_SeqScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = parallel_workers > 0 ? true : false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = parallel_workers;
	pathnode->pathkeys = NIL; /* seqscan has unordered result */

#ifdef XCP
	set_scanpath_distribution(root, rel, pathnode);
#endif
#ifdef __OPENTENBASE__
	restrict_distribution(root, pathnode, rel->baserestrictinfo);
#endif
	if (IS_PARTITIONED_REL(rel))
	{
		ListCell *lc;
		Cost      total_cost = 0;
		double    rows = 0;
		double    rows_parallel = 0;
		double    parallel_divisor = 0.0;
		int       cost_idx = 0;
		int       leafNum = list_length(rel->partition_leaf_rels_idx);
		pathnode->startup_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->total_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->rows_part = palloc0(leafNum * sizeof(double));
		foreach (lc, rel->partition_leaf_rels_idx)
		{
			int         childRTindex;
			RelOptInfo *crel;
			childRTindex = lfirst_int(lc);
			crel = root->simple_rel_array[childRTindex];
			pathnode->param_info = get_baserel_parampathinfo(root, crel, required_outer);
			if (pathnode->parallel_aware)
			{
				pathnode->parallel_workers = compute_parallel_worker(crel, crel->pages, -1);
				parallel_divisor = get_parallel_divisor(pathnode);
			}
			cost_seqscan(pathnode, root, crel, pathnode->param_info);

			pathnode->total_cost_part[cost_idx] = pathnode->total_cost;
			pathnode->startup_cost_part[cost_idx] = pathnode->startup_cost;
			pathnode->rows_part[cost_idx] = pathnode->rows;
			total_cost += pathnode->total_cost;
			if (pathnode->parallel_aware)
				rows_parallel += pathnode->rows * parallel_divisor;

			rows += pathnode->rows;
			cost_idx++;
		}
		pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer);
		pathnode->total_cost = total_cost;
		pathnode->rows = rows;
		pathnode->rows_parallel = rows_parallel;
		pathnode->startup_cost = pathnode->startup_cost_part[0];
	}
	else
	{
		cost_seqscan(pathnode, root, rel, pathnode->param_info);
	}
	return pathnode;
}

#ifdef __OPENTENBASE_C__
/*
 * create_seqscan_lm_path
 *	  Creates a path corresponding to a late materialized sequential scan, returning
 *	  the pathnode.
 */
Path *
create_seqscan_lm_path(PlannerInfo *root, RelOptInfo *rel,
					   Relids required_outer, int parallel_workers, Bitmapset *mask)
{
	Path 	*path;
	int		relid;

	/* Reuse create_seqscan_path() to better merge with upper stream code */
	path = create_seqscan_path(root, rel, required_outer, parallel_workers);

	/* Build the path attr mask */
	path->attr_masks = (AttrMask *)
		palloc0(root->simple_rel_array_size * sizeof(AttrMask));

	/* Set the attr mask for singleton base relid slot */
	bms_get_singleton_member(rel->relids, &relid);
	path->attr_masks[relid] = mask;
	/* Set sort and local flags */
	path->gtidscan_local = true;
	path->gtidscan_skipsort = true;

	/* Re-calculate the col-store cost based on un-materialized attr mask */
	cost_seqscan_col(path, root, rel, path->param_info, mask);

	return path;
}
#endif

/*
 * create_samplescan_path
 *	  Creates a path node for a sampled table scan.
 */
Path *
create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_SampleScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* samplescan has unordered result */

#ifdef XCP
	set_scanpath_distribution(root, rel, pathnode);
#endif
#ifdef __OPENTENBASE__
	restrict_distribution(root, pathnode, rel->baserestrictinfo);
#endif

	cost_samplescan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

static IndexOptInfo *
get_part_indexinfo(IndexOptInfo *pindex, RelOptInfo *crel)
{
	ListCell *lc;
	foreach(lc, crel->indexlist)
	{
		IndexOptInfo *cindex = lfirst(lc);
		List		 *ancestors;
		bool		  member;

		ancestors = get_partition_ancestors(cindex->indexoid);
		member = list_member_oid(ancestors, pindex->indexoid);
		list_free(ancestors);

		if (member)
			return cindex;
	}
	return NULL;
}

/*
 * create_index_path
 *	  Creates a path node for an index scan.
 *
 * 'index' is a usable index.
 * 'indexclauses' is a list of RestrictInfo nodes representing clauses
 *			to be used as index qual conditions in the scan.
 * 'indexclausecols' is an integer list of index column numbers (zero based)
 *			the indexclauses can be used with.
 * 'indexorderbys' is a list of bare expressions (no RestrictInfos)
 *			to be used as index ordering operators in the scan.
 * 'indexorderbycols' is an integer list of index column numbers (zero based)
 *			the ordering operators can be used with.
 * 'pathkeys' describes the ordering of the path.
 * 'indexscandir' is ForwardScanDirection or BackwardScanDirection
 *			for an ordered index, or NoMovementScanDirection for
 *			an unordered index.
 * 'indexonly' is true if an index-only scan is wanted.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 * 'partial_path' is true if constructing a parallel index scan path.
 *
 * Returns the new path node.
 */
IndexPath *
create_index_path(PlannerInfo *root,
				  IndexOptInfo *index,
				  List *indexclauses,
				  List *indexclausecols,
				  List *indexorderbys,
				  List *indexorderbycols,
				  List *pathkeys,
				  ScanDirection indexscandir,
				  bool indexonly,
				  Relids required_outer,
				  double loop_count,
				  bool partial_path)
{
	IndexPath  *pathnode = makeNode(IndexPath);
	RelOptInfo *rel = index->rel;
	List	   *indexquals,
			   *indexqualcols;
	Relation index_rel = relation_open(index->indexoid, NoLock);

	pathnode->path.pathtype = indexonly ? T_IndexOnlyScan : T_IndexScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = pathkeys;
	pathnode->path.costs_part_idx = 0;

	/* Convert clauses to indexquals the executor can handle */
	expand_indexqual_conditions(index, indexclauses, indexclausecols,
								&indexquals, &indexqualcols);

	/* Fill in the pathnode */
	pathnode->indexinfo = index;
	pathnode->indexclauses = indexclauses;
	pathnode->indexquals = indexquals;
	pathnode->indexqualcols = indexqualcols;
	pathnode->indexorderbys = indexorderbys;
	pathnode->indexorderbycols = indexorderbycols;
	pathnode->indexscandir = indexscandir;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif
#ifdef __OPENTENBASE__
	restrict_distribution(root, (Path *) pathnode, indexclauses);
	restrict_distribution(root, (Path *) pathnode, rel->baserestrictinfo);
#endif

	pathnode->indexglobal = RelationIsCrossNodeIndex(index_rel->rd_rel);
	relation_close(index_rel, NoLock);
	if (IS_PARTITIONED_REL(rel))
	{
		ListCell   *lc;
		Cost        total_cost = 0;
		Cost        index_total_cost = 0;
		double      rows = 0;
		Selectivity indexselectivity = 0;
		int         leafNum = list_length(rel->partition_leaf_rels_idx);
		int         leaf_idx = 0;
		pathnode->path.startup_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.total_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.rows_part = palloc0(leafNum * sizeof(double));
		pathnode->indexselectivity_part = palloc0(leafNum * sizeof(Selectivity));
		pathnode->indextotalcost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->pages_part = palloc0(leafNum * sizeof(BlockNumber));
		foreach (lc, rel->partition_leaf_rels_idx)
		{
			int           childRTindex;
			RelOptInfo   *crel;
			IndexOptInfo *cindex;
			List         *pindexclauses;
			List         *pindexorderbys;
			List         *pindexquals, *pindexqualcols;
			ListCell     *lcc;
			childRTindex = lfirst_int(lc);
			crel = root->simple_rel_array[childRTindex];
			pathnode->path.param_info = get_baserel_parampathinfo(root, crel, required_outer);
			cindex = get_part_indexinfo(index, crel);
			if (cindex == NULL)
			{
				/* failed to find a child index, can't use this index */
				elog(DEBUG1, "failed to find a child index for parent: %s",
					 get_rel_name(index->indexoid));
				return NULL;
			}
			pathnode->indexinfo = cindex;
			pindexclauses = copyObject(indexclauses);
			foreach (lcc, pindexclauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(lcc);
				ChangeVarNodes((Node *) rinfo->clause, rel->relid, childRTindex, 0);
			}
			pindexorderbys = copyObject(indexorderbys);
			ChangeVarNodes((Node *) pindexorderbys, rel->relid, childRTindex, 0);
			expand_indexqual_conditions(cindex, pindexclauses, indexclausecols, &pindexquals,
			                            &pindexqualcols);
			pathnode->indexclauses = pindexclauses;
			pathnode->indexorderbys = pindexorderbys;
			pathnode->indexquals = pindexquals;
			pathnode->indexqualcols = pindexqualcols;

			cost_index(pathnode, root, loop_count, partial_path);
			total_cost += pathnode->path.total_cost;
			index_total_cost += pathnode->indextotalcost;
			rows += pathnode->path.rows;
			indexselectivity += pathnode->indexselectivity;
			pathnode->path.startup_cost_part[leaf_idx] = pathnode->path.startup_cost;
			pathnode->path.total_cost_part[leaf_idx] = pathnode->path.total_cost;
			pathnode->path.rows_part[leaf_idx] = pathnode->path.rows;
			pathnode->indextotalcost_part[leaf_idx] = pathnode->indextotalcost;
			pathnode->indexselectivity_part[leaf_idx] = pathnode->indexselectivity;
			pathnode->pages_part[leaf_idx] = cindex->pages;
			leaf_idx++;
		}
		pathnode->path.total_cost = total_cost;
		pathnode->path.rows = rows;
		pathnode->path.startup_cost = pathnode->path.startup_cost_part[0];

		pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
		pathnode->indexinfo = index;
		pathnode->indexclauses = indexclauses;
		pathnode->indexquals = indexquals;
		pathnode->indexqualcols = indexqualcols;
		pathnode->indexorderbys = indexorderbys;
		pathnode->indextotalcost = index_total_cost;
		pathnode->indexselectivity = indexselectivity / list_length(rel->partition_leaf_rels_idx);
	}
	else
		cost_index(pathnode, root, loop_count, partial_path);

	return pathnode;
}

/*
 * create_bitmap_heap_path
 *	  Creates a path node for a bitmap scan.
 *
 * 'bitmapqual' is a tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * loop_count should match the value used when creating the component
 * IndexPaths.
 */
BitmapHeapPath *
create_bitmap_heap_path(PlannerInfo *root,
						RelOptInfo *rel,
						Path *bitmapqual,
						Relids required_outer,
						double loop_count,
						int parallel_degree)
{
	BitmapHeapPath *pathnode = makeNode(BitmapHeapPath);

	pathnode->path.pathtype = T_BitmapHeapScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = parallel_degree > 0 ? true : false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = parallel_degree;
	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->bitmapqual = bitmapqual;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif
#ifdef __OPENTENBASE__
	restrict_distribution(root, (Path *) pathnode, rel->baserestrictinfo);
#endif
	
	if (IS_PARTITIONED_REL(rel))
	{
		ListCell *lc;
		Cost      total_cost = 0;
		double    rows = 0;
		double    parallel_divisor = 0.0;
		int       cost_idx = 1;
		double    rows_parallel = 0;
		int       leafNum = list_length(rel->partition_leaf_rels_idx);
		pathnode->path.startup_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.total_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.rows_part = palloc0(leafNum * sizeof(double));

		foreach (lc, rel->partition_leaf_rels_idx)
		{
			int         childRTindex;
			RelOptInfo *crel;
			childRTindex = lfirst_int(lc);
			crel = root->simple_rel_array[childRTindex];
			pathnode->path.param_info = get_baserel_parampathinfo(root, crel, required_outer);
			bitmapqual->costs_part_idx = cost_idx ++;

			if (pathnode->path.parallel_aware)
			{
				pathnode->path.parallel_workers = compute_parallel_worker(crel, crel->pages, -1);
				parallel_divisor = get_parallel_divisor((Path*)pathnode);
			}

			cost_bitmap_heap_scan(&pathnode->path, root, crel, pathnode->path.param_info,
			                      bitmapqual, loop_count);
			pathnode->path.total_cost_part[bitmapqual->costs_part_idx - 1] =
				pathnode->path.total_cost;
			pathnode->path.startup_cost_part[bitmapqual->costs_part_idx - 1] =
				pathnode->path.startup_cost;
			pathnode->path.rows_part[bitmapqual->costs_part_idx - 1] = pathnode->path.rows;
			total_cost += pathnode->path.total_cost;
			
			if (pathnode->path.parallel_aware)
				rows_parallel += pathnode->path.rows * parallel_divisor;
			rows += pathnode->path.rows;
		}
		pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
		pathnode->path.total_cost = total_cost;
		pathnode->path.rows = rows;
		pathnode->path.startup_cost = pathnode->path.startup_cost_part[0];
		pathnode->path.rows_parallel = rows_parallel;
		bitmapqual->costs_part_idx = 0;
	}
	else
	{
		cost_bitmap_heap_scan(&pathnode->path, root, rel, pathnode->path.param_info, bitmapqual,
		                      loop_count);
	}
	return pathnode;
}

/*
 * create_bitmap_and_path
 *	  Creates a path node representing a BitmapAnd.
 */
BitmapAndPath *
create_bitmap_and_path(PlannerInfo *root,
					   RelOptInfo *rel,
					   List *bitmapquals)
{
	BitmapAndPath *pathnode = makeNode(BitmapAndPath);
	Relids		required_outer = NULL;
	ListCell   *lc;

	pathnode->path.pathtype = T_BitmapAnd;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;

	/*
	 * Identify the required outer rels as the union of what the child paths
	 * depend on.  (Alternatively, we could insist that the caller pass this
	 * in, but it's more convenient and reliable to compute it here.)
	 */
	foreach(lc, bitmapquals)
	{
		Path	   *bitmapqual = (Path *) lfirst(lc);

		required_outer = bms_add_members(required_outer,
										 PATH_REQ_OUTER(bitmapqual));
	}
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);

	/*
	 * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
	 * parallel-safe if and only if rel->consider_parallel is set.  So, we can
	 * set the flag for this path based only on the relation-level flag,
	 * without actually iterating over the list of children.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;

	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->bitmapquals = bitmapquals;
	pathnode->path.costs_part_idx = 0;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif
	if (IS_PARTITIONED_REL(rel))
	{
		int leafNum = list_length(rel->partition_leaf_rels_idx);
		int leaf_idx = 1;
		Cost total_cost = 0;
		ListCell *lc;
		pathnode->path.startup_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.total_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->bitmapselectivity_part = palloc0(leafNum * sizeof(Selectivity));
		foreach (lc, rel->partition_leaf_rels_idx)
		{
			pathnode->path.costs_part_idx = leaf_idx++;
			/* this sets bitmapselectivity as well as the regular cost fields: */
			cost_bitmap_and_node(pathnode, root);
			pathnode->bitmapselectivity_part[pathnode->path.costs_part_idx - 1] = pathnode->bitmapselectivity;
			pathnode->path.startup_cost_part[pathnode->path.costs_part_idx - 1] = pathnode->path.total_cost;
			pathnode->path.total_cost_part[pathnode->path.costs_part_idx - 1] = pathnode->path.total_cost;
			total_cost += pathnode->path.total_cost;
		}
		pathnode->path.total_cost = total_cost;
		pathnode->path.costs_part_idx = 0;
	}
	else
		/* this sets bitmapselectivity as well as the regular cost fields: */
		cost_bitmap_and_node(pathnode, root);

	return pathnode;
}

/*
 * create_bitmap_or_path
 *	  Creates a path node representing a BitmapOr.
 */
BitmapOrPath *
create_bitmap_or_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  List *bitmapquals)
{
	BitmapOrPath *pathnode = makeNode(BitmapOrPath);
	Relids		required_outer = NULL;
	ListCell   *lc;

	pathnode->path.pathtype = T_BitmapOr;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;

	/*
	 * Identify the required outer rels as the union of what the child paths
	 * depend on.  (Alternatively, we could insist that the caller pass this
	 * in, but it's more convenient and reliable to compute it here.)
	 */
	foreach(lc, bitmapquals)
	{
		Path	   *bitmapqual = (Path *) lfirst(lc);

		required_outer = bms_add_members(required_outer,
										 PATH_REQ_OUTER(bitmapqual));
	}
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);

	/*
	 * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
	 * parallel-safe if and only if rel->consider_parallel is set.  So, we can
	 * set the flag for this path based only on the relation-level flag,
	 * without actually iterating over the list of children.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;

	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->bitmapquals = bitmapquals;
	pathnode->path.costs_part_idx = 0;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif

	if (IS_PARTITIONED_REL(rel))
	{
		int leafNum = list_length(rel->partition_leaf_rels_idx);
		int leaf_idx = 1;
		Cost total_cost = 0;
		ListCell *lc;
		pathnode->path.startup_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.total_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->bitmapselectivity_part = palloc0(leafNum * sizeof(Selectivity));
		foreach (lc, rel->partition_leaf_rels_idx)
		{
			pathnode->path.costs_part_idx = leaf_idx++;
			/* this sets bitmapselectivity as well as the regular cost fields: */
			cost_bitmap_or_node(pathnode, root);
			pathnode->bitmapselectivity_part[pathnode->path.costs_part_idx - 1] = pathnode->bitmapselectivity;
			pathnode->path.startup_cost_part[pathnode->path.costs_part_idx - 1] = pathnode->path.total_cost;
			pathnode->path.total_cost_part[pathnode->path.costs_part_idx - 1] = pathnode->path.total_cost;
			total_cost += pathnode->path.total_cost;
		}
		pathnode->path.total_cost = total_cost;
		pathnode->path.costs_part_idx = 0;
	}
	else
		/* this sets bitmapselectivity as well as the regular cost fields: */
		cost_bitmap_or_node(pathnode, root);

	return pathnode;
}

/*
 * create_tidscan_path
 *	  Creates a path corresponding to a scan by TID, returning the pathnode.
 */
TidPath *
create_tidscan_path(PlannerInfo *root, RelOptInfo *rel, List *tidquals,
					Relids required_outer)
{
	TidPath    *pathnode = makeNode(TidPath);

	pathnode->path.pathtype = T_TidScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->tidquals = tidquals;

#ifdef XCP
	set_scanpath_distribution(root, rel, (Path *) pathnode);
#endif
	if (IS_PARTITIONED_REL(rel))
	{
		ListCell *lc;
		Cost      total_cost = 0;
		double    rows = 0;
		int       cost_idx = 1;
		int       leafNum = list_length(rel->partition_leaf_rels_idx);
		pathnode->path.startup_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.total_cost_part = palloc0(leafNum * sizeof(Cost));
		pathnode->path.rows_part = palloc0(leafNum * sizeof(double));
		foreach (lc, rel->partition_leaf_rels_idx)
		{
			int         childRTindex;
			RelOptInfo *crel;
			childRTindex = lfirst_int(lc);
			crel = root->simple_rel_array[childRTindex];
			pathnode->path.param_info = get_baserel_parampathinfo(root, crel, required_outer);

			cost_tidscan(&pathnode->path, root, crel, tidquals, pathnode->path.param_info);

			pathnode->path.total_cost_part[cost_idx - 1] = pathnode->path.total_cost;
			pathnode->path.startup_cost_part[cost_idx - 1] = pathnode->path.startup_cost;
			pathnode->path.rows_part[cost_idx - 1] = pathnode->path.rows;
			total_cost += pathnode->path.total_cost;
			rows += pathnode->path.rows;
			cost_idx++;
		}
		pathnode->path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
		pathnode->path.total_cost = total_cost;
		pathnode->path.rows = rows;
		pathnode->path.startup_cost = pathnode->path.startup_cost_part[0];
	}
	else
	{
		cost_tidscan(&pathnode->path, root, rel, tidquals, pathnode->path.param_info);
	}
	return pathnode;
}

/*
 * create_append_path
 *	  Creates a path corresponding to an Append plan, returning the
 *	  pathnode.
 *
 * Note that we must handle subpaths = NIL, representing a dummy access path.
 */
AppendPath *
create_append_path(PlannerInfo *root, RelOptInfo *rel,
				   List *subpaths, List *partial_subpaths,
				   Relids required_outer,
				   int parallel_workers, bool parallel_aware,
				   List *partitioned_rels, double rows)
{
	AppendPath *pathnode = makeNode(AppendPath);
	ListCell   *l;

	Assert(!parallel_aware || parallel_workers > 0);

	pathnode->path.pathtype = T_Append;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;

	/*
	 * When generating an Append path for a partitioned table, there may be
	 * parameters that are useful so we can eliminate certain partitions
	 * during execution.  Here we'll go all the way and fully populate the
	 * parameter info data as we do for normal base relations.  However, we
	 * need only bother doing this for RELOPT_BASEREL rels, as
	 * RELOPT_OTHER_MEMBER_REL's Append paths are merged into the base rel's
	 * Append subpaths.  It would do no harm to do this, we just avoid it to
	 * save wasting effort.
	 */
	if (partitioned_rels != NIL && root && rel->reloptkind == RELOPT_BASEREL)
		pathnode->path.param_info = get_baserel_parampathinfo(root,
															  rel,
															  required_outer);
	else
		pathnode->path.param_info = get_appendrel_parampathinfo(rel,
													 			required_outer);
	pathnode->path.parallel_aware = parallel_aware;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = parallel_workers;
	pathnode->path.pathkeys = NIL;	/* result is always considered unsorted */
	pathnode->partitioned_rels = list_copy(partitioned_rels);

	/*
	 * For parallel append, non-partial paths are sorted by descending total
	 * costs. That way, the total time to finish all non-partial paths is
	 * minimized.  Also, the partial paths are sorted by descending startup
	 * costs.  There may be some paths that require to do startup work by a
	 * single worker.  In such case, it's better for workers to choose the
	 * expensive ones first, whereas the leader should choose the cheapest
	 * startup plan.
	 */
	if (pathnode->path.parallel_aware)
	{
		subpaths = list_qsort(subpaths, append_total_cost_compare);
		partial_subpaths = list_qsort(partial_subpaths,
									  append_startup_cost_compare);
	}
	pathnode->first_partial_path = list_length(subpaths);
	pathnode->subpaths = list_concat(subpaths, partial_subpaths);

	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
			subpath->parallel_safe;

		/* All child paths must have same parameterization */
		Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
	}

	Assert(!parallel_aware || pathnode->path.parallel_safe);

#ifdef XCP
	if (IS_PGXC_COORDINATOR)
	{
		bool		inh = false;

		if (root && pathnode->subpaths && rel->relid > 0 &&
			IS_SIMPLE_REL(rel) && rel->rtekind == RTE_RELATION)
		{
			/* check if table is inherit */
			inh = planner_rt_fetch(rel->relid, root)->inh;
		}
		set_appendpath_distribution(root, (Path *)pathnode,
									pathnode->subpaths, inh);
	}
#endif

	cost_append(pathnode);

	/* If the caller provided a row estimate, override the computed value. */
	if (rows >= 0)
		pathnode->path.rows = rows;

	return pathnode;
}

/*
 * append_total_cost_compare
 *	  qsort comparator for sorting append child paths by total_cost descending
 *
 * For equal total costs, we fall back to comparing startup costs; if those
 * are equal too, break ties using bms_compare on the paths' relids.
 * (This is to avoid getting unpredictable results from qsort.)
 */
static int
append_total_cost_compare(const void *a, const void *b)
{
	Path	   *path1 = (Path *) lfirst(*(ListCell **) a);
	Path	   *path2 = (Path *) lfirst(*(ListCell **) b);
	int			cmp;

	cmp = compare_path_costs(path1, path2, TOTAL_COST);
	if (cmp != 0)
		return -cmp;
	return bms_compare(path1->parent->relids, path2->parent->relids);
}

/*
 * append_startup_cost_compare
 *	  qsort comparator for sorting append child paths by startup_cost descending
 *
 * For equal startup costs, we fall back to comparing total costs; if those
 * are equal too, break ties using bms_compare on the paths' relids.
 * (This is to avoid getting unpredictable results from qsort.)
 */
static int
append_startup_cost_compare(const void *a, const void *b)
{
	Path	   *path1 = (Path *) lfirst(*(ListCell **) a);
	Path	   *path2 = (Path *) lfirst(*(ListCell **) b);
	int			cmp;

	cmp = compare_path_costs(path1, path2, STARTUP_COST);
	if (cmp != 0)
		return -cmp;
	return bms_compare(path1->parent->relids, path2->parent->relids);
}

/*
 * create_merge_append_path
 *	  Creates a path corresponding to a MergeAppend plan, returning the
 *	  pathnode.
 */
MergeAppendPath *
create_merge_append_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 List *subpaths,
						 List *pathkeys,
						 Relids required_outer,
						 List *partitioned_rels)
{
	MergeAppendPath *pathnode = makeNode(MergeAppendPath);
	Cost		input_startup_cost;
	Cost		input_total_cost;
	ListCell   *l;

	pathnode->path.pathtype = T_MergeAppend;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_appendrel_parampathinfo(rel,
															required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = pathkeys;
	pathnode->partitioned_rels = list_copy(partitioned_rels);
	pathnode->subpaths = subpaths;

	/*
	 * Apply query-wide LIMIT if known and path is for sole base relation.
	 * (Handling this at this low level is a bit klugy.)
	 */
	if (bms_equal(rel->relids, root->all_baserels))
		pathnode->limit_tuples = root->limit_tuples;
	else
		pathnode->limit_tuples = -1.0;

	/*
	 * Add up the sizes and costs of the input paths.
	 */
	pathnode->path.rows = 0;
	input_startup_cost = 0;
	input_total_cost = 0;
	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		pathnode->path.rows += subpath->rows;
		pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
			subpath->parallel_safe;

		if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
		{
			/* Subpath is adequately ordered, we won't need to sort it */
			input_startup_cost += subpath->startup_cost;
			input_total_cost += subpath->total_cost;
		}
		else
		{
			/* We'll need to insert a Sort node, so include cost for that */
			Path		sort_path;	/* dummy for result of cost_sort */

			cost_sort(&sort_path,
					  root,
					  pathkeys,
					  subpath->total_cost,
					  subpath->parent->tuples,
					  subpath->pathtarget->width,
					  0.0,
					  work_mem,
					  pathnode->limit_tuples);
			input_startup_cost += sort_path.startup_cost;
			input_total_cost += sort_path.total_cost;
		}

		/* All child paths must have same parameterization */
		Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
	}

#ifdef XCP
	if (IS_PGXC_COORDINATOR)
	{
		bool		inh = false;

		if (root && pathnode->subpaths && rel->relid > 0 &&
			IS_SIMPLE_REL(rel) && rel->rtekind == RTE_RELATION)
		{
			/* check if table is inherit */
			inh = planner_rt_fetch(rel->relid, root)->inh;
		}
		set_appendpath_distribution(root, (Path *)pathnode,
									pathnode->subpaths, inh);
	}
#endif

	/* Now we can compute total costs of the MergeAppend */
	cost_merge_append(&pathnode->path, root,
					  pathkeys, list_length(subpaths),
					  input_startup_cost, input_total_cost,
					  pathnode->path.rows);

	return pathnode;
}

/*
 * create_result_path
 *	  Creates a path representing a Result-and-nothing-else plan.
 *
 * This is only used for degenerate cases, such as a query with an empty
 * jointree.
 */
ResultPath *
create_result_path(PlannerInfo *root, RelOptInfo *rel,
				   PathTarget *target, List *resconstantqual)
{
	ResultPath *pathnode = makeNode(ResultPath);

	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = NULL;	/* there are no other rels... */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;
	pathnode->quals = resconstantqual;

	/* Hardly worth defining a cost_result() function ... just do it */
	pathnode->path.rows = 1;
	pathnode->path.startup_cost = target->cost.startup;
	pathnode->path.total_cost = target->cost.startup +
		cpu_tuple_cost + target->cost.per_tuple;

	/*
	 * Add cost of qual, if any --- but we ignore its selectivity, since our
	 * rowcount estimate should be 1 no matter what the qual is.
	 */
	if (resconstantqual)
	{
		QualCost	qual_cost;

		cost_qual_eval(&qual_cost, resconstantqual, root);
		/* resconstantqual is evaluated once at startup */
		pathnode->path.startup_cost += qual_cost.startup + qual_cost.per_tuple;
		pathnode->path.total_cost += qual_cost.startup + qual_cost.per_tuple;
	}

	return pathnode;
}

/*
 * create_material_path
 *	  Creates a path corresponding to a Material plan, returning the
 *	  pathnode.
 */
MaterialPath *
create_material_path(RelOptInfo *rel, Path *subpath)
{
	MaterialPath *pathnode = makeNode(MaterialPath);

	Assert(subpath->parent == rel);

	pathnode->path.pathtype = T_Material;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->subpath = subpath;

#ifdef XCP
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
#endif

	cost_material(&pathnode->path,
				  subpath->startup_cost,
				  subpath->total_cost,
				  subpath->rows,
				  subpath->pathtarget->width);

	return pathnode;
}

/*
 * create_unique_path
 *	  Creates a path representing elimination of distinct rows from the
 *	  input data.  Distinct-ness is defined according to the needs of the
 *	  semijoin represented by sjinfo.  If it is not possible to identify
 *	  how to make the data unique, NULL is returned.
 *
 * If used at all, this is likely to be called repeatedly on the same rel;
 * and the input subpath should always be the same (the cheapest_total path
 * for the rel).  So we cache the result.
 */
UniquePath *
create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				   SpecialJoinInfo *sjinfo)
{
	UniquePath *pathnode;
	Path		sort_path;		/* dummy for result of cost_sort */
	Path		agg_path;		/* dummy for result of cost_agg */
	MemoryContext oldcontext;
	int			numCols;

	/* Caller made a mistake if subpath isn't cheapest_total ... */
	Assert(subpath == rel->cheapest_total_path);
	Assert(subpath->parent == rel);
	/* ... or if SpecialJoinInfo is the wrong one */
	Assert(sjinfo->jointype == JOIN_SEMI ||
		   sjinfo->jointype == JOIN_LEFT_SEMI ||
		   sjinfo->jointype == JOIN_LEFT_SEMI_SCALAR);
	Assert(bms_equal(rel->relids, sjinfo->syn_righthand));

	/* If result already cached, return it */
	if (rel->cheapest_unique_path)
		return (UniquePath *) rel->cheapest_unique_path;

	/* If it's not possible to unique-ify, return NULL */
	if (!(sjinfo->semi_can_btree || sjinfo->semi_can_hash))
		return NULL;

	/*
	 * When called during GEQO join planning, we are in a short-lived memory
	 * context.  We must make sure that the path and any subsidiary data
	 * structures created for a baserel survive the GEQO cycle, else the
	 * baserel is trashed for future GEQO cycles.  On the other hand, when we
	 * are creating those for a joinrel during GEQO, we don't want them to
	 * clutter the main planning context.  Upshot is that the best solution is
	 * to explicitly allocate memory in the same context the given RelOptInfo
	 * is in.
	 */
	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

	pathnode = makeNode(UniquePath);

	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	/*
	 * Assume the output is unsorted, since we don't necessarily have pathkeys
	 * to represent it.  (This might get overridden below.)
	 */
	pathnode->path.pathkeys = NIL;

	pathnode->subpath = subpath;

	/*
	 * Under GEQO, the sjinfo might be short-lived, so we'd better make copies
	 * of data structures we extract from it.
	 */
	pathnode->in_operators = copyObject(sjinfo->semi_operators);
	pathnode->uniq_exprs = copyObject(sjinfo->semi_rhs_exprs);

#ifdef XCP
	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
#endif
#ifdef __OPENTENBASE_C__
	pathnode->path.attr_masks = subpath->attr_masks;
#endif

	/*
	 * If the input is a relation and it has a unique index that proves the
	 * semi_rhs_exprs are unique, then we don't need to do anything.  Note
	 * that relation_has_unique_index_for automatically considers restriction
	 * clauses for the rel, as well.
	 */
	if (rel->rtekind == RTE_RELATION && sjinfo->semi_can_btree &&
		relation_has_unique_index_for(root, rel, NIL,
									  sjinfo->semi_rhs_exprs,
									  sjinfo->semi_operators))
	{
		pathnode->umethod = UNIQUE_PATH_NOOP;
		pathnode->path.rows = rel->rows;
		pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost = subpath->total_cost;
		pathnode->path.pathkeys = subpath->pathkeys;

		rel->cheapest_unique_path = (Path *) pathnode;

		MemoryContextSwitchTo(oldcontext);

		return pathnode;
	}

	/*
	 * If the input is a subquery whose output must be unique already, then we
	 * don't need to do anything.  The test for uniqueness has to consider
	 * exactly which columns we are extracting; for example "SELECT DISTINCT
	 * x,y" doesn't guarantee that x alone is distinct. So we cannot check for
	 * this optimization unless semi_rhs_exprs consists only of simple Vars
	 * referencing subquery outputs.  (Possibly we could do something with
	 * expressions in the subquery outputs, too, but for now keep it simple.)
	 */
	if (rel->rtekind == RTE_SUBQUERY)
	{
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

		if (query_supports_distinctness(rte->subquery))
		{
			List	   *sub_tlist_colnos;

			sub_tlist_colnos = translate_sub_tlist(sjinfo->semi_rhs_exprs,
												   rel->relid);

			if (sub_tlist_colnos &&
				query_is_distinct_for(rte->subquery,
									  sub_tlist_colnos,
									  sjinfo->semi_operators))
			{
				pathnode->umethod = UNIQUE_PATH_NOOP;
				pathnode->path.rows = rel->rows;
				pathnode->path.startup_cost = subpath->startup_cost;
				pathnode->path.total_cost = subpath->total_cost;
				pathnode->path.pathkeys = subpath->pathkeys;

				rel->cheapest_unique_path = (Path *) pathnode;

				MemoryContextSwitchTo(oldcontext);

				return pathnode;
			}
		}
	}

#ifdef __OPENTENBASE__
	set_uniquepath_distribution(root, pathnode);
#endif

	/* Estimate number of output rows */
	pathnode->path.rows = estimate_num_groups(root,
											  sjinfo->semi_rhs_exprs,
											  rel->rows,
											  NULL);
	numCols = list_length(sjinfo->semi_rhs_exprs);

	if (sjinfo->semi_can_btree)
	{
		/*
		 * Estimate cost for sort+unique implementation
		 */
		cost_sort(&sort_path, root, NIL,
				  subpath->total_cost,
				  rel->rows,
				  subpath->pathtarget->width,
				  0.0,
				  work_mem,
				  -1.0);

		/*
		 * Charge one cpu_operator_cost per comparison per input tuple. We
		 * assume all columns get compared at most of the tuples. (XXX
		 * probably this is an overestimate.)  This should agree with
		 * create_upper_unique_path.
		 */
		sort_path.total_cost += cpu_operator_cost * rel->rows * numCols;
	}

	if (sjinfo->semi_can_hash)
	{
		/*
		 * Estimate the overhead per hashtable entry at 64 bytes (same as in
		 * planner.c).
		 */
		int			hashentrysize = subpath->pathtarget->width + 64;
		int			hash_mem = get_hash_mem();

		if (hashentrysize * pathnode->path.rows > hash_mem * 1024L)
		{
			/*
			 * We should not try to hash.  Hack the SpecialJoinInfo to
			 * remember this, in case we come through here again.
			 */
			sjinfo->semi_can_hash = false;
		}
		else
		{
			cost_agg(&agg_path, root,
					 AGG_HASHED, NULL,
					 numCols, pathnode->path.rows,
					 NIL,
					 subpath->startup_cost,
					 subpath->total_cost,
					 rel->rows,
					 subpath->pathtarget->width);
		}
	}

	if (sjinfo->semi_can_btree && sjinfo->semi_can_hash)
	{
		if (agg_path.total_cost < sort_path.total_cost)
			pathnode->umethod = UNIQUE_PATH_HASH;
		else
			pathnode->umethod = UNIQUE_PATH_SORT;
	}
	else if (sjinfo->semi_can_btree)
		pathnode->umethod = UNIQUE_PATH_SORT;
	else if (sjinfo->semi_can_hash)
		pathnode->umethod = UNIQUE_PATH_HASH;
	else
	{
		/* we can get here only if we abandoned hashing above */
		MemoryContextSwitchTo(oldcontext);
		return NULL;
	}

	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		pathnode->path.startup_cost = agg_path.startup_cost;
		pathnode->path.total_cost = agg_path.total_cost;
	}
	else
	{
		pathnode->path.startup_cost = sort_path.startup_cost;
		pathnode->path.total_cost = sort_path.total_cost;
	}

	rel->cheapest_unique_path = (Path *) pathnode;

	MemoryContextSwitchTo(oldcontext);

	return pathnode;
}

/*
 * em_contains_distinct
 *	Check whether any member in the equal calss is in the distinct clause.
 */
static bool
em_contains_distinct(PlannerInfo *root, EquivalenceClass *ec)
{
	ListCell		*lc1;
	ListCell		*lc2;
	Var				*var;
	TargetEntry 	*targetEntry;

	if (list_length(root->parse->targetList) != list_length(root->parse->distinctClause))
		return false;

	/*
	 * Loop through eual class to check whether it is also in the distinct clause.
	 */
	foreach(lc1, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc1);
		if (em->em_is_child)
			continue;		/* ignore children here */
		/* EM must be a Var, possibly with RelabelType */
		var = (Var *) em->em_expr;
		while (var && IsA(var, RelabelType))
			var = (Var *) ((RelabelType *) var)->arg;
		if (!(var && IsA(var, Var)))
			continue;
		
		foreach(lc2, root->parse->targetList)
		{
			targetEntry = (TargetEntry*) lfirst(lc2);
			if (!IsA(targetEntry->expr, Var))
				return false;
			
			if (equal_var(var, (Var*)targetEntry->expr))
			{
				return true;
			}
		}
	}
	return false;
}

/*
 * get_eclasee_by_var
 *	Get equal class depending on variable.
 */
static EquivalenceClass *
get_eclasee_by_var(PlannerInfo *root, Var *var_check)
{
	ListCell	*lc1;
	ListCell	*lc2;
	Var			*var;
	foreach(lc1, root->eq_classes)
	{
		EquivalenceClass *ec = (EquivalenceClass *) lfirst(lc1);
		/* Never match to a volatile EC */
		if (ec->ec_has_volatile)
			continue;
		
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			if (em->em_is_child)
				continue;		/* ignore children here */
			/* EM must be a Var, possibly with RelabelType */
			var = (Var *) em->em_expr;
			while (var && IsA(var, RelabelType))
				var = (Var *) ((RelabelType *) var)->arg;
			if (!(var && IsA(var, Var)))
				continue;
			
			if (equal(var, var_check))
			{
				return ec;
			}
		}
	}
	return NULL;
}

/*
 * has_distinct_eclass
 *	Check whether any member in the same equal class is in the distinct clause.
 */
static bool
has_distinct_eclass(PlannerInfo *root, Var *var)
{
	EquivalenceClass *ec;

	ec = get_eclasee_by_var(root, var);

	if (ec == NULL)
		return false;

	if (em_contains_distinct(root, ec))
		return true;

	return false;
}

/*
 * try_unique_path_exprs
 *	Check wthether the distinct could be pushed down to the table.
 *	The variables in the target list or member in the same equal class shoule cover
 *	distinct clause.
 */
List*
try_unique_path_exprs(PlannerInfo *root, Path *subpath)
{
	ListCell		*lc_subpath = NULL;
	ListCell		*lc_parse = NULL;
	List			*exprs = NULL;
	Node			*node = NULL;
	TargetEntry		*targetEntry = NULL;
	bool			find_distinct_var = false;

	if (list_length(root->parse->targetList) != list_length(subpath->pathtarget->exprs) ||
		list_length(root->parse->targetList) != list_length(root->parse->distinctClause))
	{
		return NULL;
	}

	/*
	 * Check whether variables in the target list or members in the same equal class
	 * cover distinct clause.
	 */
	foreach(lc_subpath, subpath->pathtarget->exprs)
	{
		node = (Node *) lfirst(lc_subpath);

		if (!IsA(node, Var))
		{
			list_free(exprs);
			return NULL;
		}

		find_distinct_var = false;

		foreach(lc_parse, root->parse->targetList)
		{
			targetEntry = (TargetEntry*) lfirst(lc_parse);

			if (targetEntry->ressortgroupref == 0 ||
				!IsA(targetEntry->expr, Var))
			{
				list_free(exprs);
				return NULL;				
			}

			if (equal_var((Var*)targetEntry->expr, (Var*)node) ||
				has_distinct_eclass(root, (Var*)node))
			{
				find_distinct_var = true;
				break;
			}
		}
		if (!find_distinct_var)
		{
			list_free(exprs);
			return NULL;	
		}

		exprs = lappend(exprs, node);
	}

	return exprs;
}

/*
 * try_add_unique_path
 *	Try to push distinct down to the single table.
 */
UniquePath *
try_add_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath)
{
	UniquePath 	   *pathnode;
	Path			sort_path;		/* dummy for result of cost_sort */
	Path			agg_path;		/* dummy for result of cost_agg */
	MemoryContext 	oldcontext;
	int				numCols;
	double			rows;
	Query		   *subselect = NULL;
	bool			can_hash = enable_hashagg;
	bool			can_btree = true;
	Oid				operOid;
	Oid				operType;
	List		   *exprs;
	ListCell	   *lc;
	Var			   *var;

	/* Caller made a mistake if subpath isn't cheapest_total ... */
	if (rel == NULL || subpath == NULL)
		return NULL;

	subselect = root->parse;

	/*
	 * What we can not optimize.
	 */
	if (subselect->commandType != CMD_SELECT ||
		subselect->hasAggs || subselect->hasDistinctOn ||
		subselect->setOperations || subselect->groupingSets ||
		subselect->groupClause || subselect->hasWindowFuncs ||
		subselect->hasTargetSRFs || subselect->hasModifyingCTE ||
		subselect->havingQual || subselect->limitOffset ||
		subselect->limitCount || subselect->rowMarks ||
		subselect->cteList || subselect->sortClause ||
		!subselect->distinctClause)
	{
		return NULL;
	}

	exprs = try_unique_path_exprs(root, subpath);

	if (exprs == NULL)
		return NULL;

	rows= estimate_num_groups(root, exprs, rel->rows, NULL);

	/*
	 * Check wether the reduction ration is below the threshold.
	 */
	if ((rows / rel->rows) > distinct_pushdown_factor)
		return NULL;

	/*
	 * When called during GEQO join planning, we are in a short-lived memory
	 * context.  We must make sure that the path and any subsidiary data
	 * structures created for a baserel survive the GEQO cycle, else the
	 * baserel is trashed for future GEQO cycles.  On the other hand, when we
	 * are creating those for a joinrel during GEQO, we don't want them to
	 * clutter the main planning context.  Upshot is that the best solution is
	 * to explicitly allocate memory in the same context the given RelOptInfo
	 * is in.
	 */
	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));
	pathnode = makeNode(UniquePath);
	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	/*
	 * Assume the output is unsorted, since we don't necessarily have pathkeys
	 * to represent it.  (This might get overridden below.)
	 */
	pathnode->path.pathkeys = NIL;
	pathnode->subpath = subpath;

	foreach(lc, exprs)
	{
		var = (Var*) lfirst(lc);
		operType = var->vartype;
		operOid = 	get_operid(DatumGetCString("="), operType, operType, 11);
		pathnode->in_operators = lappend_oid(pathnode->in_operators, operOid);
		/* all operators must be btree equality or hash equality */
		if (can_btree)
		{
			/* oprcanmerge is considered a hint... */
			if (!op_mergejoinable(operOid, operType) ||
				get_mergejoin_opfamilies(operOid) == NIL)
				can_btree = false;
		}
		if (can_hash)
		{
			/* ... but oprcanhash had better be correct */
			if (!op_hashjoinable(operOid, operType))
				can_hash = false;
		}
		if (!(can_btree || can_hash))
		{
			MemoryContextSwitchTo(oldcontext);
			return NULL;
		}
	}

	pathnode->uniq_exprs = exprs;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);
	pathnode->path.attr_masks = subpath->attr_masks;

	/*
	 * If the input is a relation and it has a unique index that proves the
	 * semi_rhs_exprs are unique, then we don't need to do anything.  Note
	 * that relation_has_unique_index_for automatically considers restriction
	 * clauses for the rel, as well.
	 */
	if (rel->rtekind == RTE_RELATION && can_hash &&
		relation_has_unique_index_for(root, rel, NIL, exprs, pathnode->in_operators))
	{
		pathnode->umethod = UNIQUE_PATH_NOOP;
		pathnode->path.rows = rel->rows;
		pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost = subpath->total_cost;
		pathnode->path.pathkeys = subpath->pathkeys;
		MemoryContextSwitchTo(oldcontext);
		return pathnode;
	}

	/*
	 * If the input is a subquery whose output must be unique already, then we
	 * don't need to do anything.  The test for uniqueness has to consider
	 * exactly which columns we are extracting; for example "SELECT DISTINCT
	 * x,y" doesn't guarantee that x alone is distinct. So we cannot check for
	 * this optimization unless semi_rhs_exprs consists only of simple Vars
	 * referencing subquery outputs.  (Possibly we could do something with
	 * expressions in the subquery outputs, too, but for now keep it simple.)
	 */
	if (rel->rtekind == RTE_SUBQUERY)
	{
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
		if (query_supports_distinctness(rte->subquery))
		{
			List	   *sub_tlist_colnos;
			sub_tlist_colnos = translate_sub_tlist(subpath->pathtarget->exprs,
												   rel->relid);
			if (sub_tlist_colnos &&
				query_is_distinct_for(rte->subquery,
									  sub_tlist_colnos,
									  pathnode->in_operators))
			{
				pathnode->umethod = UNIQUE_PATH_NOOP;
				pathnode->path.rows = rel->rows;
				pathnode->path.startup_cost = subpath->startup_cost;
				pathnode->path.total_cost = subpath->total_cost;
				pathnode->path.pathkeys = subpath->pathkeys;
				MemoryContextSwitchTo(oldcontext);
				return pathnode;
			}
		}
	}

	set_uniquepath_distribution(root, pathnode);

	if (pathnode->path.distribution == NULL &&
		IsA(pathnode->subpath, RemoteSubPath))
	{
		MemoryContextSwitchTo(oldcontext);
		return NULL;
	}

	/* Estimate number of output rows */
	pathnode->path.rows = estimate_num_groups(root,
											  subpath->pathtarget->exprs,
											  rel->rows,
											  NULL);

	numCols = list_length(subpath->pathtarget->exprs);

	if (can_btree)
	{
		/*
		 * Estimate cost for sort+unique implementation
		 */
		cost_sort(&sort_path, root, NIL,
				  subpath->total_cost,
				  rel->rows,
				  subpath->pathtarget->width,
				  0.0,
				  work_mem,
				  -1.0);
		/*
		 * Charge one cpu_operator_cost per comparison per input tuple. We
		 * assume all columns get compared at most of the tuples. (XXX
		 * probably this is an overestimate.)  This should agree with
		 * create_upper_unique_path.
		 */
		sort_path.total_cost += cpu_operator_cost * rel->rows * numCols;
	}

	if (can_hash)
	{
		/*
		 * Estimate the overhead per hashtable entry at 64 bytes (same as in
		 * planner.c).
		 */
		int			hashentrysize = subpath->pathtarget->width + 64;
		int			hash_mem = get_hash_mem();
		if (hashentrysize * pathnode->path.rows > hash_mem * 1024L)
		{
			/*
			 * We should not try to hash.  Hack the SpecialJoinInfo to
			 * remember this, in case we come through here again.
			 */
			can_hash = false;
		}
		else
		{
			cost_agg(&agg_path, root,
					 AGG_HASHED, NULL,
					 numCols, pathnode->path.rows,
					 NIL,
					 subpath->startup_cost,
					 subpath->total_cost,
					 rel->rows,
					 subpath->pathtarget->width);
		}
	}

	if (can_btree && can_hash)
	{
		if (agg_path.total_cost < sort_path.total_cost)
			pathnode->umethod = UNIQUE_PATH_HASH;
		else
			pathnode->umethod = UNIQUE_PATH_SORT;
	}
	else if (can_btree)
			pathnode->umethod = UNIQUE_PATH_SORT;
	else if (can_hash)
		pathnode->umethod = UNIQUE_PATH_HASH;
	else
	{
		/* we can get here only if we abandoned hashing above */
		MemoryContextSwitchTo(oldcontext);
		return NULL;
	}

	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		pathnode->path.startup_cost = agg_path.startup_cost;
		pathnode->path.total_cost = agg_path.total_cost;
	}
	else
	{
		pathnode->path.startup_cost = sort_path.startup_cost;
		pathnode->path.total_cost = sort_path.total_cost;
	}

	MemoryContextSwitchTo(oldcontext);

	return pathnode;	
}

/*
 * create_gather_merge_path
 *
 *	  Creates a path corresponding to a gather merge scan, returning
 *	  the pathnode.
 */
GatherMergePath *
create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
						 PathTarget *target, List *pathkeys,
						 Relids required_outer, double *rows)
{
	GatherMergePath *pathnode = makeNode(GatherMergePath);
	Cost		input_startup_cost = 0;
	Cost		input_total_cost = 0;

	Assert(subpath->parallel_safe);
	Assert(pathkeys);

	pathnode->path.pathtype = T_GatherMerge;
	pathnode->path.parent = rel;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	pathnode->subpath = subpath;
	pathnode->num_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->path.pathtarget = target ? target : rel->reltarget;
	pathnode->path.rows += subpath->rows;

	if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
	{
		/* Subpath is adequately ordered, we won't need to sort it */
		input_startup_cost += subpath->startup_cost;
		input_total_cost += subpath->total_cost;
	}
	else
	{
		/* We'll need to insert a Sort node, so include cost for that */
		Path		sort_path;	/* dummy for result of cost_sort */

		cost_sort(&sort_path,
				  root,
				  pathkeys,
				  subpath->total_cost,
				  subpath->rows,
				  subpath->pathtarget->width,
				  0.0,
				  work_mem,
				  -1);
		input_startup_cost += sort_path.startup_cost;
		input_total_cost += sort_path.total_cost;
	}

	cost_gather_merge(pathnode, root, rel, pathnode->path.param_info,
					  input_startup_cost, input_total_cost, rows);

	return pathnode;
}

/*
 * translate_sub_tlist - get subquery column numbers represented by tlist
 *
 * The given targetlist usually contains only Vars referencing the given relid.
 * Extract their varattnos (ie, the column numbers of the subquery) and return
 * as an integer List.
 *
 * If any of the tlist items is not a simple Var, we cannot determine whether
 * the subquery's uniqueness condition (if any) matches ours, so punt and
 * return NIL.
 */
static List *
translate_sub_tlist(List *tlist, int relid)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, tlist)
	{
		Var		   *var = (Var *) lfirst(l);

		if (!var || !IsA(var, Var) ||
			var->varno != relid)
			return NIL;			/* punt */

		result = lappend_int(result, var->varattno);
	}
	return result;
}

/*
 * create_gather_path
 *	  Creates a path corresponding to a gather scan, returning the
 *	  pathnode.
 *
 * 'rows' may optionally be set to override row estimates from other sources.
 */
GatherPath *
create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				   PathTarget *target, Relids required_outer, double *rows)
{
	GatherPath *pathnode = makeNode(GatherPath);

	// Assert(subpath->parallel_safe);

	pathnode->path.pathtype = T_Gather;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;	/* Gather has unordered result */

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	pathnode->subpath = subpath;
	pathnode->num_workers = subpath->parallel_workers;
	pathnode->single_copy = false;

	if (pathnode->num_workers == 0)
	{
		pathnode->path.pathkeys = subpath->pathkeys;
		pathnode->num_workers = 1;
		pathnode->single_copy = true;
	}

	cost_gather(pathnode, root, rel, pathnode->path.param_info, rows);

	return pathnode;
}

/*
 * create_subqueryscan_path
 *	  Creates a path corresponding to a scan of a subquery,
 *	  returning the pathnode.
 */
SubqueryScanPath *
#ifdef XCP
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel,
						 PlannerInfo *subroot, Path *subpath,
						 List *pathkeys, Relids required_outer,
						 Distribution *distribution)
#else
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel,
						 PlannerInfo *subroot, Path *subpath,
						 List *pathkeys, Relids required_outer)
#endif
{
	SubqueryScanPath *pathnode = makeNode(SubqueryScanPath);

#ifdef XCP
	pathnode->path.distribution = distribution;
#endif
	pathnode->path.pathtype = T_SubqueryScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->subpath = subpath;
	pathnode->subroot = subroot;

	cost_subqueryscan(pathnode, root, rel, pathnode->path.param_info);

	return pathnode;
}

/*
 * create_functionscan_path
 *	  Creates a path corresponding to a sequential scan of a function,
 *	  returning the pathnode.
 */
Path *
create_functionscan_path(PlannerInfo *root, RelOptInfo *rel,
						 List *pathkeys, Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_FunctionScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = pathkeys;

	cost_functionscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_tablefuncscan_path
 *	  Creates a path corresponding to a sequential scan of a table function,
 *	  returning the pathnode.
 */
Path *
create_tablefuncscan_path(PlannerInfo *root, RelOptInfo *rel,
						  Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_TableFuncScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	cost_tablefuncscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_valuesscan_path
 *	  Creates a path corresponding to a scan of a VALUES list,
 *	  returning the pathnode.
 */
Path *
create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel,
					   Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_ValuesScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	cost_valuesscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

#ifdef __OPENTENBASE_C__
/*
 * create_sharedctescan_path
 *	  Creates a path corresponding to a scan of a shared CTE,
 *	  returning the pathnode.
 */
Path *
create_sharedctescan_path(PlannerInfo *root, RelOptInfo *rel,
						  PathTarget *target, Distribution *dist, Plan *cteplan,
						  int parallel_workers)
{
	Path	   *pathnode = makeNode(Path);

	/* set rel consider_parallel if have not */
	if (root->glob->parallelModeOK &&
		!rel->consider_parallel && bms_is_empty(rel->lateral_relids))
	{
		rel->consider_parallel =
			is_parallel_safe(root, (Node *) rel->baserestrictinfo) &&
			is_parallel_safe(root, (Node *) rel->reltarget->exprs);
	}

	pathnode->pathtype = T_CteScan;
	pathnode->parent = rel;
	pathnode->pathtarget = target;
	pathnode->param_info = NULL;
	pathnode->parallel_aware = parallel_workers > 0 ? true : false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = parallel_workers;
	pathnode->pathkeys = NIL;	/* XXX for now, result is always unordered */

	pathnode->distribution = dist;

	cost_sharedctescan(pathnode, root, rel, cteplan);

	return pathnode;
}

/*
 * create_sharedctescan_partial_path
 *	  Creates a partial path corresponding to a scan of a shared CTE,
 *	  returning the pathnode.
 */
Path *
create_sharedctescan_partial_path(PlannerInfo *root, RelOptInfo *rel,
								  PathTarget *target, Distribution *dist,
								  Plan *cteplan)
{
	int		parallel_workers = -1;

	if (rel->pages > 0)
	{
		int		heap_parallel_workers = 1;
		int		heap_parallel_threshold = Max(min_parallel_table_scan_size, 1);

		while (rel->pages >= (BlockNumber) (heap_parallel_threshold * 3))
		{
			heap_parallel_workers++;
			heap_parallel_threshold *= 3;
			if (heap_parallel_threshold > INT_MAX / 3)
				break;		/* avoid overflow */
		}
		parallel_workers = Min(heap_parallel_workers,
							   max_parallel_workers_per_gather);
	}

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	if (parallel_workers <= 0)
		return NULL;

	return create_sharedctescan_path(root, rel, target, dist,
									 cteplan, parallel_workers);
}
#endif

/*
 * create_ctescan_path
 *	  Creates a path corresponding to a scan of a non-self-reference CTE,
 *	  returning the pathnode.
 */
Path *
create_ctescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_CteScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* XXX for now, result is always unordered */

	cost_ctescan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_namedtuplestorescan_path
 *	  Creates a path corresponding to a scan of a named tuplestore, returning
 *	  the pathnode.
 */
Path *
create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
								Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_NamedTuplestoreScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	cost_namedtuplestorescan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_worktablescan_path
 *	  Creates a path corresponding to a scan of a self-reference CTE,
 *	  returning the pathnode.
 */
Path *
create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel,
						  Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_WorkTableScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	/* Cost is the same as for a regular CTE scan */
	cost_ctescan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_foreignscan_path
 *	  Creates a path corresponding to a scan of a foreign table, foreign join,
 *	  or foreign upper-relation processing, returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignPaths, GetForeignJoinPaths, or
 * GetForeignUpperPaths function of a foreign data wrapper.  We make the FDW
 * supply all fields of the path, since we do not have any way to calculate
 * them in core.  However, there is a usually-sane default for the pathtarget
 * (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *
create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
						PathTarget *target,
						double rows, Cost startup_cost, Cost total_cost,
						List *pathkeys,
						Relids required_outer,
						Path *fdw_outerpath,
						List *fdw_private)
{
	ForeignPath *pathnode = makeNode(ForeignPath);

	pathnode->path.pathtype = T_ForeignScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target ? target : rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.rows = rows;
	pathnode->path.startup_cost = startup_cost;
	pathnode->path.total_cost = total_cost;
	pathnode->path.pathkeys = pathkeys;

	pathnode->fdw_outerpath = fdw_outerpath;
	pathnode->fdw_private = fdw_private;

	return pathnode;
}

/*
 * calc_nestloop_required_outer
 *	  Compute the required_outer set for a nestloop join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_nestloop_required_outer(Relids outerrelids,
							 Relids outer_paramrels,
							 Relids innerrelids,
							 Relids inner_paramrels)
{
	Relids		required_outer;

	/* inner_path can require rels from outer path, but not vice versa */
	Assert(!bms_overlap(outer_paramrels, innerrelids));
	/* easy case if inner path is not parameterized */
	if (!inner_paramrels)
		return bms_copy(outer_paramrels);
	/* else, form the union ... */
	required_outer = bms_union(outer_paramrels, inner_paramrels);
	/* ... and remove any mention of now-satisfied outer rels */
	required_outer = bms_del_members(required_outer,
									 outerrelids);
	/* maintain invariant that required_outer is exactly NULL if empty */
	if (bms_is_empty(required_outer))
	{
		bms_free(required_outer);
		required_outer = NULL;
	}
	return required_outer;
}

/*
 * calc_non_nestloop_required_outer
 *	  Compute the required_outer set for a merge or hash join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path)
{
	Relids		outer_paramrels = PATH_REQ_OUTER(outer_path);
	Relids		inner_paramrels = PATH_REQ_OUTER(inner_path);
	Relids		innerrelids PG_USED_FOR_ASSERTS_ONLY;
	Relids		outerrelids PG_USED_FOR_ASSERTS_ONLY;
	Relids		required_outer;
	RelOptInfo *innerrel = inner_path->parent;
	RelOptInfo *outerrel = outer_path->parent;

	/*
	 * Paths are parameterized by top-level parents, so run parameterization
	 * tests on the parent relids.
	 */
	if (innerrel->top_parent_relids)
		innerrelids = innerrel->top_parent_relids;
	else
		innerrelids = innerrel->relids;

	if (outerrel->top_parent_relids)
		outerrelids = outerrel->top_parent_relids;
	else
		outerrelids = outerrel->relids;

	/* neither path can require rels from the other */
	Assert(!bms_overlap(outer_paramrels, innerrelids));
	Assert(!bms_overlap(inner_paramrels, outerrelids));

	/* form the union ... */
	required_outer = bms_union(outer_paramrels, inner_paramrels);
	/* we do not need an explicit test for empty; bms_union gets it right */
	return required_outer;
}

/*
 * create_nestloop_path
 *	  Creates a pathnode corresponding to a nestloop join between two
 *	  relations.
 *
 * 'joinrel' is the join relation.
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_nestloop
 * 'extra' contains various information about the join
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 *
 * Returns the resulting path node.
 */
NestPath *
create_nestloop_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 JoinCostWorkspace *workspace,
					 JoinPathExtraData *extra,
					 Path *outer_path,
					 Path *inner_path,
					 List *restrict_clauses,
					 List *pathkeys,
					 Relids required_outer)
{
	NestPath   *pathnode = makeNode(NestPath);
	Relids		inner_req_outer = PATH_REQ_OUTER(inner_path);
#ifdef XCP
	List	   *alternate;
	ListCell   *lc;
	List	   *mclauses = NIL;
#endif

	/*
	 * If the inner path is parameterized by the outer, we must drop any
	 * restrict_clauses that are due to be moved into the inner path.  We have
	 * to do this now, rather than postpone the work till createplan time,
	 * because the restrict_clauses list can affect the size and cost
	 * estimates for this path.
	 */
	if (bms_overlap(inner_req_outer, outer_path->parent->relids))
	{
		Relids		inner_and_outer = bms_union(inner_path->parent->relids,
												inner_req_outer);
		List	   *jclauses = NIL;
		ListCell   *lc;

		foreach(lc, restrict_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (!join_clause_is_movable_into(rinfo,
											 inner_path->parent->relids,
											 inner_and_outer))
				jclauses = lappend(jclauses, rinfo);
#ifdef XCP
			else if (!contain_subplans((Node *) rinfo->clause))
				mclauses = lappend(mclauses, rinfo);
#endif
		}
		restrict_clauses = jclauses;
	}

	pathnode->path.pathtype = T_NestLoop;
	pathnode->path.parent = joinrel;
	pathnode->path.pathtarget = joinrel->reltarget;
	pathnode->path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = joinrel->consider_parallel &&
		outer_path->parallel_safe && inner_path->parallel_safe;
	/* This is a foolish way to estimate parallel_workers, but for now... */
	pathnode->path.parallel_workers = outer_path->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->jointype = jointype;
	pathnode->inner_unique = extra->inner_unique;
	pathnode->outerjoinpath = outer_path;
	pathnode->innerjoinpath = inner_path;
	pathnode->joinrestrictinfo = restrict_clauses;

#ifdef XCP
	pathnode->movedrestrictinfo = mclauses;

	if(IS_CENTRALIZED_MODE)
		alternate = NULL;
	else if(set_distribution_hook)
		alternate = (*set_distribution_hook)(root, pathnode);
	else
		alternate = set_joinpath_distribution(root, pathnode, NOT_SET_DISTRIBUTION);

#endif

	adjust_join_restrict(root, (JoinPath *) pathnode);
	
#ifdef __OPENTENBASE__
	/*
	 * Since set_joinpath_distribution() could add additional pathnode such as
	 * RemoteSubplan, the result of initial_cost_nestloop() needs to be
	 * recalculated.
	 */
	initial_cost_nestloop(root, workspace, jointype,
						  pathnode->outerjoinpath,
						  pathnode->innerjoinpath,
						  extra);
#endif

	final_cost_nestloop(root, pathnode, workspace, extra);

#ifdef XCP
	/*
	 * Also calculate costs of all alternates and return cheapest path
	 */
	foreach(lc, alternate)
	{
		NestPath *altpath = (NestPath *) lfirst(lc);

#ifdef __OPENTENBASE__
		/*
		 * Recalculate the initial cost of alternate path
		 */
		initial_cost_nestloop(root, workspace, jointype,
							  altpath->outerjoinpath,
							  altpath->innerjoinpath,
							  extra);
#endif

		final_cost_nestloop(root, altpath, workspace, extra);
		if (altpath->path.total_cost < pathnode->path.total_cost)
			pathnode = altpath;
	}
#endif

	/* For DELETE, check if the path distribution satisfy resultRel distribution */
	if (!SatisfyResultRelDist(root, &pathnode->path))
		return NULL;

	return pathnode;
}

/*
 * create_mergejoin_path
 *	  Creates a pathnode corresponding to a mergejoin join between
 *	  two relations
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_mergejoin
 * 'extra' contains various information about the join
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 * 'mergeclauses' are the RestrictInfo nodes to use as merge clauses
 *		(this should be a subset of the restrict_clauses list)
 * 'outersortkeys' are the sort varkeys for the outer relation
 * 'innersortkeys' are the sort varkeys for the inner relation
 */
MergePath *
create_mergejoin_path(PlannerInfo *root,
					  RelOptInfo *joinrel,
					  JoinType jointype,
					  JoinCostWorkspace *workspace,
					  JoinPathExtraData *extra,
					  Path *outer_path,
					  Path *inner_path,
					  List *restrict_clauses,
					  List *pathkeys,
					  Relids required_outer,
					  List *mergeclauses,
					  List *outersortkeys,
					  List *innersortkeys,
					  bool is_not_full)
{
	MergePath  *pathnode = makeNode(MergePath);
#ifdef XCP
	List	   *alternate;
	ListCell   *lc;
#endif

	pathnode->jpath.path.pathtype = T_MergeJoin;
	pathnode->jpath.path.parent = joinrel;
	pathnode->jpath.path.pathtarget = joinrel->reltarget;
	pathnode->jpath.path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->jpath.path.parallel_aware = false;
	pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
		outer_path->parallel_safe && inner_path->parallel_safe;
	/* This is a foolish way to estimate parallel_workers, but for now... */
	pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;
	pathnode->jpath.path.pathkeys = pathkeys;
	pathnode->jpath.jointype = jointype;
	pathnode->jpath.inner_unique = extra->inner_unique;
	pathnode->jpath.outerjoinpath = outer_path;
	pathnode->jpath.innerjoinpath = inner_path;
	pathnode->jpath.joinrestrictinfo = restrict_clauses;
	pathnode->path_mergeclauses = mergeclauses;
	pathnode->outersortkeys = outersortkeys;
	pathnode->innersortkeys = innersortkeys;
	pathnode->is_not_full = is_not_full;
	/* pathnode->skip_mark_restore will be set by final_cost_mergejoin */
	/* pathnode->materialize_inner will be set by final_cost_mergejoin */

#ifdef XCP
	if(IS_CENTRALIZED_MODE)
		alternate = NULL;
	else if(set_distribution_hook)
		alternate = (*set_distribution_hook)(root, (JoinPath *) pathnode);
	else
		alternate = set_joinpath_distribution(root, (JoinPath *) pathnode, NOT_SET_DISTRIBUTION);

#endif
	
	adjust_join_restrict(root, (JoinPath *) pathnode);
#ifdef __OPENTENBASE__
	/*
	 * Since set_joinpath_distribution() could add additional pathnode such as
	 * RemoteSubplan, the result of initial_cost_mergejoin() needs to be
	 * recalculated.
	 */
	initial_cost_mergejoin(root, workspace, jointype, mergeclauses,
						   pathnode->jpath.outerjoinpath,
						   pathnode->jpath.innerjoinpath,
						   outersortkeys, innersortkeys,
						   extra);
#endif

	final_cost_mergejoin(root, pathnode, workspace, extra);

#ifdef XCP
	/*
	 * Also calculate costs of all alternates and return cheapest path
	 */
	foreach(lc, alternate)
	{
		MergePath *altpath = (MergePath *) lfirst(lc);

#ifdef __OPENTENBASE__
		/*
		 * Recalculate the initial cost of alternate path
		 */
		initial_cost_mergejoin(root, workspace, jointype, mergeclauses,
							   altpath->jpath.outerjoinpath,
							   altpath->jpath.innerjoinpath,
							   outersortkeys, innersortkeys,
							   extra);
#endif

		final_cost_mergejoin(root, altpath, workspace, extra);
		if (altpath->jpath.path.total_cost < pathnode->jpath.path.total_cost)
			pathnode = altpath;
	}
#endif

	/* For DELETE, check if the path distribution satisfy resultRel distribution */
	if (!SatisfyResultRelDist(root, &pathnode->jpath.path))
		return NULL;

	return pathnode;
}

/*
 * adjust_join_restrict
 *
 * If the restrictinfo contains an cn expr that equal one side relid, must handle it before join
 * 
*/
static void
adjust_join_restrict(PlannerInfo *root, JoinPath *path)
{
	List     *lqual = NIL;
	List     *rqual = NIL;
	List     *res_restrict = NIL;
	List     *restrict_clauses;
	ListCell *lc = NULL;

	if (path->path.distribution != NULL)
		return;

	restrict_clauses = path->joinrestrictinfo;

	foreach(lc, restrict_clauses)
	{
		RestrictInfo *r = (RestrictInfo *) lfirst(lc);
		if (bms_equal(r->required_relids, path->outerjoinpath->parent->relids))
		{
			Assert(contain_cn_expr((Node*)r->clause));
			lqual = lappend(lqual, r->clause);
		}
		else if (bms_equal(r->required_relids, path->innerjoinpath->parent->relids))
		{
			Assert(contain_cn_expr((Node*)r->clause));
			rqual = lappend(rqual, r->clause);
		}
		else
			continue;
		res_restrict = lappend(res_restrict, r);
	}

	path->joinrestrictinfo = list_difference(path->joinrestrictinfo, res_restrict);

	if (lqual != NULL)
		path->outerjoinpath = (Path *) create_qual_path(root, path->outerjoinpath, lqual);
	if (rqual != NULL)
		path->innerjoinpath = (Path *) create_qual_path(root, path->innerjoinpath, rqual);
}
/*
 * create_hashjoin_path
 *	  Creates a pathnode corresponding to a hash join between two relations.
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_hashjoin
 * 'extra' contains various information about the join
 * 'outer_path' is the cheapest outer path
 * 'inner_path' is the cheapest inner path
 * 'parallel_hash' to select Parallel Hash of inner path (shared hash table)
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'required_outer' is the set of required outer rels
 * 'hashclauses' are the RestrictInfo nodes to use as hash clauses
 *		(this should be a subset of the restrict_clauses list)
 */
HashPath *
create_hashjoin_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 JoinCostWorkspace *workspace,
					 JoinPathExtraData *extra,
					 Path *outer_path,
					 Path *inner_path,
					 bool parallel_hash,
					 List *restrict_clauses,
					 Relids required_outer,
					 List *hashclauses)
{
	HashPath   *pathnode = makeNode(HashPath);
#ifdef XCP
	List	   *alternate;
	ListCell   *lc;
#endif

	pathnode->jpath.path.pathtype = T_HashJoin;
	pathnode->jpath.path.parent = joinrel;
	pathnode->jpath.path.pathtarget = joinrel->reltarget;
	pathnode->jpath.path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->jpath.path.parallel_aware =
		joinrel->consider_parallel && parallel_hash;
	pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
		outer_path->parallel_safe && inner_path->parallel_safe;
	/* This is a foolish way to estimate parallel_workers, but for now... */
	pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;

	/*
	 * A hashjoin never has pathkeys, since its output ordering is
	 * unpredictable due to possible batching.  XXX If the inner relation is
	 * small enough, we could instruct the executor that it must not batch,
	 * and then we could assume that the output inherits the outer relation's
	 * ordering, which might save a sort step.  However there is considerable
	 * downside if our estimate of the inner relation size is badly off. For
	 * the moment we don't risk it.  (Note also that if we wanted to take this
	 * seriously, joinpath.c would have to consider many more paths for the
	 * outer rel than it does now.)
	 */
	pathnode->jpath.path.pathkeys = NIL;
	pathnode->jpath.jointype = jointype;
	pathnode->jpath.inner_unique = extra->inner_unique;
	pathnode->jpath.outerjoinpath = outer_path;
	pathnode->jpath.innerjoinpath = inner_path;
	pathnode->jpath.joinrestrictinfo = restrict_clauses;
	pathnode->path_hashclauses = hashclauses;

#ifdef __OPENTENBASE_C__
	/*
	 * Create the join attr_masks before final_cost_hashjoin, late materialize
	 * may cause fewer memory footprint in hash table. The cost calculation will
	 * be affected.
	 */
	pathnode->jpath.path.attr_masks =
			create_hashjoin_attr_masks(root, joinrel, inner_path, outer_path);
#endif

#ifdef XCP

	if(IS_CENTRALIZED_MODE)
		alternate = NULL;
	else if(set_distribution_hook)
		alternate = (*set_distribution_hook)(root, (JoinPath *) pathnode);
	else
		alternate = set_joinpath_distribution(root, (JoinPath *) pathnode, NOT_SET_DISTRIBUTION);
#endif

	adjust_join_restrict(root, (JoinPath *) pathnode);

#ifdef __OPENTENBASE__
	/*
	 * Since set_joinpath_distribution() could add additional pathnode such as
	 * RemoteSubplan, the result of initial_cost_hashjoin() needs to be
	 * recalculated.
	 */
	initial_cost_hashjoin(root,
						  workspace,
						  jointype,
						  hashclauses,
						  pathnode->jpath.outerjoinpath,
						  pathnode->jpath.innerjoinpath,
						  extra,
						  parallel_hash);
#endif

	/* final_cost_hashjoin will fill in pathnode->num_batches */
	/* TODO(LM): The cost calculation might be affected */
	final_cost_hashjoin(root, pathnode, workspace, extra);

#ifdef XCP
	/*
	 * Calculate costs of all alternates and return cheapest path
	 */
	foreach(lc, alternate)
	{
		HashPath *altpath = (HashPath *) lfirst(lc);

#ifdef __OPENTENBASE__
		/*
		 * Recalculate the initial cost of alternate path
		 */
		initial_cost_hashjoin(root,
							  workspace,
							  jointype,
							  hashclauses,
							  altpath->jpath.outerjoinpath,
							  altpath->jpath.innerjoinpath,
							  extra,
							  parallel_hash);
#endif

		final_cost_hashjoin(root, altpath, workspace, extra);
		if (altpath->jpath.path.total_cost < pathnode->jpath.path.total_cost)
			pathnode = altpath;
	}
#endif

#ifdef __OPENTENBASE_C__
	/*
	 * Try with Sideways Information Passing(SIP) optimization, which will push
	 * down inner hash filter(HashTable/BloomFilter) to outer path as deep as
	 * possible. A new SIP Filter node will be injected to early filter more
	 * tuples in outer path.
	 *
	 * TODO:
	 * 1. temporarily disable the logic here since we need to copy outer
	 * path to avoid messed up the shared path nodes. But PG does not support
	 * copy path nodes.
	 * 2. If we want to adjust cost to the hashjoin path, then we HAVE TO inject
	 * SipFilter pathnode here instead of simply adjust best_path.
	 *
	 */

	/*
	 * Adjust the late materialize related strategies, which is based on the
	 * num_batches calculated in final_cost_hashjoin. Set the strategy after
	 * XCP alternate path choose because the inserted RemoteSubpath could
	 * affect the strategy decision.
	 */
	if (!pathnode->jpath.path.attr_masks ||
		(pathnode->jpath.innerjoinpath->attr_masks &&
		 !pathnode->jpath.innerjoinpath->gtidscan_local) ||
		(pathnode->jpath.outerjoinpath->attr_masks &&
		 !pathnode->jpath.outerjoinpath->gtidscan_local))
	{
		pathnode->jpath.path.gtidscan_local = false;
	}
	else
	{
		/*
		 * gtidscan_local is enabled only if both LM sub-paths are
		 * gtidscan_local
		 */
		pathnode->jpath.path.gtidscan_local = true;
	}

	/*
	 * gtidscan_skipsort is enabled only if both LM sub-paths are
	 * set to gtidscan_skipsort=true. Also the num_batches should be 1,
	 * otherwise the hybrid-hashjoin algorithm might break the rid
	 * sequence.
	 * If the inner path need late materialize, the output rid
	 * also need to be sorted since the sequence is determined by
	 * outer plan.
	 */
	if (!pathnode->jpath.path.attr_masks ||
		 pathnode->num_batches > 1 ||
		 pathnode->jpath.innerjoinpath->attr_masks ||
		(pathnode->jpath.outerjoinpath->attr_masks &&
		 !pathnode->jpath.outerjoinpath->gtidscan_skipsort))
	{
		pathnode->jpath.path.gtidscan_skipsort = false;
	}
	else
	{
		pathnode->jpath.path.gtidscan_skipsort = true;
	}
#endif

	/* For DELETE, check if the path distribution satisfy resultRel distribution */
	if (!SatisfyResultRelDist(root, &pathnode->jpath.path))
		return NULL;

	return pathnode;
}

/*
 * check_eclass_in_pathtarget
 *	Check whether there is target key to support the eclass.
 */
static bool
check_eclass_in_pathtarget(EquivalenceClass *ec,
						   List *exprs,
						   Relids relids)
{
	Expr	   *expr;
	ListCell   *lc_ec;
	ListCell   *lc_expr;

	foreach(lc_ec, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc_ec);
		Expr	   *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they belong to the rel being sorted.
		 */
		if (em->em_is_child &&
			!bms_is_subset(em->em_relids, relids))
			continue;

		/* Match if same expression (after stripping relabel) */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		foreach(lc_expr, exprs)
		{
			expr = (Expr*) lfirst(lc_expr);

			while (expr && IsA(expr, RelabelType))
				expr = ((RelabelType *) expr)->arg;

			if (equal(emexpr, expr))
				return true;
		}
	}

	return false;
}

/*
 * redistribute_sanity_check
 *	Check problem for conditons when the target list is smaller than pathkeys.
 *	It means that the path could not support the pathkeys, and the redundant
 *	pathkeys should be removed.
 */
void
pathkey_sanity_check(Path *path)
{
	ListCell	*lc = NULL;
	PathKey		*pathkey = NULL;
	int		numPath_key = 0;

	/*
	 * Find the first unqualified path key.
	 */
	foreach(lc, path->pathkeys)
	{
		pathkey = (PathKey *)lfirst(lc);

		if (!check_eclass_in_pathtarget(pathkey->pk_eclass,
							path->pathtarget->exprs,
							path->parent->relids))
		{
			if (numPath_key == 0)
			{
				path->pathkeys = NIL;
				return;
			}
			break;
		}
		++numPath_key;
	}

	/*
	 * Remove path keys from the first unqualified path key.
	 */
	if (list_length(path->pathkeys) > numPath_key)
	{
		path->pathkeys = list_truncate(list_copy(path->pathkeys), numPath_key);
	}
}

/*
 * create_projection_path
 *	  Creates a pathnode that represents performing a projection.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
ProjectionPath *
create_projection_path(PlannerInfo *root,
					   RelOptInfo *rel,
					   Path *subpath,
					   PathTarget *target)
{
	ProjectionPath *pathnode = makeNode(ProjectionPath);
	PathTarget *oldtarget = subpath->pathtarget;

	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe &&
		is_parallel_safe(root, (Node *) target->exprs);
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Projection does not change the sort order */
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	pathnode->subpath = subpath;

	/*
	 * We might not need a separate Result node.  If the input plan node type
	 * can project, we can just tell it to project something else.  Or, if it
	 * can't project but the desired target has the same expression list as
	 * what the input will produce anyway, we can still give it the desired
	 * tlist (possibly changing its ressortgroupref labels, but nothing else).
	 * Note: in the latter case, create_projection_plan has to recheck our
	 * conclusion; see comments therein.
	 */
	if (is_projection_capable_path(subpath) ||
		equal(oldtarget->exprs, target->exprs))
	{
		/* No separate Result node needed */
		pathnode->dummypp = true;

		/*
		 * Set cost of plan as subpath's cost, adjusted for tlist replacement.
		 */
		pathnode->path.rows = subpath->rows;
		pathnode->path.startup_cost = subpath->startup_cost +
			(target->cost.startup - oldtarget->cost.startup);
		pathnode->path.total_cost = subpath->total_cost +
			(target->cost.startup - oldtarget->cost.startup) +
			(target->cost.per_tuple - oldtarget->cost.per_tuple) * subpath->rows;
	}
	else
	{
		/* We really do need the Result node */
		pathnode->dummypp = false;

		/*
		 * The Result node's cost is cpu_tuple_cost per row, plus the cost of
		 * evaluating the tlist.  There is no qual to worry about.
		 */
		pathnode->path.rows = subpath->rows;
		pathnode->path.startup_cost = subpath->startup_cost +
			target->cost.startup;
		pathnode->path.total_cost = subpath->total_cost +
			target->cost.startup +
			(cpu_tuple_cost + target->cost.per_tuple) * subpath->rows;
	}

	return pathnode;
}

/*
 * apply_projection_to_path
 *	  Add a projection step, or just apply the target directly to given path.
 *
 * This has the same net effect as create_projection_path(), except that if
 * a separate Result plan node isn't needed, we just replace the given path's
 * pathtarget with the desired one.  This must be used only when the caller
 * knows that the given path isn't referenced elsewhere and so can be modified
 * in-place.
 *
 * If the input path is a GatherPath or GatherMergePath, we try to push the
 * new target down to its input as well; this is a yet more invasive
 * modification of the input path, which create_projection_path() can't do.
 *
 * Note that we mustn't change the source path's parent link; so when it is
 * add_path'd to "rel" things will be a bit inconsistent.  So far that has
 * not caused any trouble.
 *
 * 'rel' is the parent relation associated with the result
 * 'path' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
Path *
apply_projection_to_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *path,
						 PathTarget *target)
{
	QualCost	oldcost;

	/*
	 * If given path can't project, we might need a Result node, so make a
	 * separate ProjectionPath.
	 */
	if (!is_projection_capable_path(path))
		return (Path *) create_projection_path(root, rel, path, target);

	/*
	 * We can just jam the desired tlist into the existing path, being sure to
	 * update its cost estimates appropriately.
	 */
	oldcost = path->pathtarget->cost;
	path->pathtarget = target;

	path->startup_cost += target->cost.startup - oldcost.startup;
	path->total_cost += target->cost.startup - oldcost.startup +
		(target->cost.per_tuple - oldcost.per_tuple) * path->rows;

	/*
	 * If the path happens to be a Gather or GatherMerge path, we'd like to
	 * arrange for the subpath to return the required target list so that
	 * workers can help project.  But if there is something that is not
	 * parallel-safe in the target expressions, then we can't.
	 */
	if ((IsA(path, GatherPath) || IsA(path, GatherMergePath)) &&
		is_parallel_safe(root, (Node *) target->exprs))
	{
		/*
		 * We always use create_projection_path here, even if the subpath is
		 * projection-capable, so as to avoid modifying the subpath in place.
		 * It seems unlikely at present that there could be any other
		 * references to the subpath, but better safe than sorry.
		 *
		 * Note that we don't change the parallel path's cost estimates; it might
		 * be appropriate to do so, to reflect the fact that the bulk of the
		 * target evaluation will happen in workers.
		 */
		if (IsA(path, GatherPath))
		{
			GatherPath *gpath = (GatherPath *) path;

			gpath->subpath = (Path *)
				create_projection_path(root,
									   gpath->subpath->parent,
									   gpath->subpath,
									   target);
		}
		else
		{
			GatherMergePath *gmpath = (GatherMergePath *) path;

			gmpath->subpath = (Path *)
				create_projection_path(root,
									   gmpath->subpath->parent,
									   gmpath->subpath,
									   target);
		}
	}
	else if (path->parallel_safe &&
			 !is_parallel_safe(root, (Node *) target->exprs))
	{
		/*
		 * We're inserting a parallel-restricted target list into a path
		 * currently marked parallel-safe, so we have to mark it as no longer
		 * safe.
		 */
		path->parallel_safe = false;
	}

	return path;
}

/*
 * create_set_projection_path
 *	  Creates a pathnode that represents performing a projection that
 *	  includes set-returning functions.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
ProjectSetPath *
create_set_projection_path(PlannerInfo *root,
						   RelOptInfo *rel,
						   Path *subpath,
						   PathTarget *target)
{
	ProjectSetPath *pathnode = makeNode(ProjectSetPath);
	double		tlist_rows;
	ListCell   *lc;

	pathnode->path.pathtype = T_ProjectSet;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe &&
		is_parallel_safe(root, (Node *) target->exprs);
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Projection does not change the sort order XXX? */
	pathnode->path.pathkeys = subpath->pathkeys;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = copyObject(subpath->distribution);

	pathnode->subpath = subpath;

	/*
	 * Estimate number of rows produced by SRFs for each row of input; if
	 * there's more than one in this node, use the maximum.
	 */
	tlist_rows = 1;
	foreach(lc, target->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);
		double		itemrows;

		itemrows = expression_returns_set_rows(node);
		if (tlist_rows < itemrows)
			tlist_rows = itemrows;
	}

	/*
	 * In addition to the cost of evaluating the tlist, charge cpu_tuple_cost
	 * per input row, and half of cpu_tuple_cost for each added output row.
	 * This is slightly bizarre maybe, but it's what 9.6 did; we may revisit
	 * this estimate later.
	 */
	pathnode->path.rows = subpath->rows * tlist_rows;
	pathnode->path.startup_cost = subpath->startup_cost +
		target->cost.startup;
	pathnode->path.total_cost = subpath->total_cost +
		target->cost.startup +
		(cpu_tuple_cost + target->cost.per_tuple) * subpath->rows +
		(pathnode->path.rows - subpath->rows) * cpu_tuple_cost / 2;

	return pathnode;
}

/*
 * create_sort_path
 *	  Creates a pathnode that represents performing an explicit sort.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'pathkeys' represents the desired sort order
 * 'limit_tuples' is the estimated bound on the number of output tuples,
 *		or -1 if no LIMIT or couldn't estimate
 */
SortPath *
create_sort_path(PlannerInfo *root,
				 RelOptInfo *rel,
				 Path *subpath,
				 List *pathkeys,
				 double limit_tuples)
{
	SortPath   *pathnode = makeNode(SortPath);

	pathnode->path.pathtype = T_Sort;
	pathnode->path.parent = rel;
	/* Sort doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = pathkeys;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = copyObject(subpath->distribution);

	pathnode->subpath = subpath;

	cost_sort(&pathnode->path, root, pathkeys,
			  subpath->total_cost,
			  subpath->rows,
			  subpath->pathtarget->width,
			  0.0,				/* XXX comparison_cost shouldn't be 0? */
			  work_mem, limit_tuples);

	return pathnode;
}

/*
 * create_group_path
 *	  Creates a pathnode that represents performing grouping of presorted input
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'numGroups' is the estimated number of groups
 */
GroupPath *
create_group_path(PlannerInfo *root,
				  RelOptInfo *rel,
				  Path *subpath,
				  PathTarget *target,
				  List *groupClause,
				  List *qual,
				  double numGroups)
{
	GroupPath  *pathnode = makeNode(GroupPath);

	pathnode->path.pathtype = T_Group;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Group doesn't change sort ordering */
	pathnode->path.pathkeys = subpath->pathkeys;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	pathnode->subpath = subpath;

	pathnode->groupClause = groupClause;
	pathnode->qual = qual;

	cost_group(&pathnode->path, root,
			   list_length(groupClause),
			   numGroups,
			   qual,
			   subpath->startup_cost, subpath->total_cost,
			   subpath->rows);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}

/*
 * create_upper_unique_path
 *	  Creates a pathnode that represents performing an explicit Unique step
 *	  on presorted input.
 *
 * This produces a Unique plan node, but the use-case is so different from
 * create_unique_path that it doesn't seem worth trying to merge the two.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'numCols' is the number of grouping columns
 * 'numGroups' is the estimated number of groups
 *
 * The input path must be sorted on the grouping columns, plus possibly
 * additional columns; so the first numCols pathkeys are the grouping columns
 */
UpperUniquePath *
create_upper_unique_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *subpath,
						 int numCols,
						 double numGroups)
{
	UpperUniquePath *pathnode = makeNode(UpperUniquePath);

	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	/* Unique doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Unique doesn't change the input ordering */
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	pathnode->subpath = subpath;
	pathnode->numkeys = numCols;

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.  (XXX probably this is
	 * an overestimate.)
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost +
		cpu_operator_cost * subpath->rows * numCols;
	pathnode->path.rows = numGroups;

	return pathnode;
}

/*
 * create_agg_path
 *	  Creates a pathnode that represents performing aggregation/grouping
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'aggstrategy' is the Agg node's basic implementation strategy
 * 'aggsplit' is the Agg node's aggregate-splitting mode
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'aggcosts' contains cost info about the aggregate functions to be computed
 * 'numGroups' is the estimated number of groups (1 if not grouping)
 */
AggPath *
create_agg_path(PlannerInfo *root,
				RelOptInfo *rel,
				Path *subpath,
				PathTarget *target,
				AggStrategy aggstrategy,
				AggSplit aggsplit,
				List *groupClause,
				List *qual,
				const AggClauseCosts *aggcosts,
				double numGroups)
{
	AggPath    *pathnode = makeNode(AggPath);

	pathnode->path.pathtype = T_Agg;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	if (aggstrategy == AGG_SORTED)
		pathnode->path.pathkeys = subpath->pathkeys;	/* preserves order */
	else
		pathnode->path.pathkeys = NIL;	/* output is unordered */
	pathnode->subpath = subpath;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	pathnode->aggstrategy = aggstrategy;
	pathnode->aggsplit = aggsplit;
	pathnode->numGroups = numGroups;
	pathnode->transitionSpace = aggcosts ? aggcosts->transitionSpace : 0;
	pathnode->groupClause = groupClause;
	pathnode->qual = qual;

	cost_agg(&pathnode->path, root,
			 aggstrategy, aggcosts,
			 list_length(groupClause), numGroups,
			 qual,
			 subpath->startup_cost, subpath->total_cost,
			 subpath->rows, subpath->pathtarget->width);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}

/*
 * create_groupingsets_path
 *	  Creates a pathnode that represents performing GROUPING SETS aggregation
 *
 * GroupingSetsPath represents sorted grouping with one or more grouping sets.
 * The input path's result must be sorted to match the last entry in
 * rollup_groupclauses.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'having_qual' is the HAVING quals if any
 * 'rollups' is a list of RollupData nodes
 * 'agg_costs' contains cost info about the aggregate functions to be computed
 * 'numGroups' is the estimated total number of groups
 */
GroupingSetsPath *
create_groupingsets_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *subpath,
						 PathTarget *target,
						 List *having_qual,
						 AggStrategy aggstrategy,
						 List *rollups,
						 const AggClauseCosts *agg_costs,
						 double numGroups)
{
	GroupingSetsPath *pathnode = makeNode(GroupingSetsPath);
	ListCell   *lc;
	bool		is_first = true;
	bool		is_first_sort = true;

	/* The topmost generated Plan node will be an Agg */
	pathnode->path.pathtype = T_Agg;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->subpath = subpath;

	/* distribution is the same as in the subpath */
	pathnode->path.distribution = (Distribution *) copyObject(subpath->distribution);

	/*
	 * Simplify callers by downgrading AGG_SORTED to AGG_PLAIN, and AGG_MIXED
	 * to AGG_HASHED, here if possible.
	 */
	if (aggstrategy == AGG_SORTED &&
		list_length(rollups) == 1 &&
		((RollupData *) linitial(rollups))->groupClause == NIL)
		aggstrategy = AGG_PLAIN;

	if (aggstrategy == AGG_MIXED &&
		list_length(rollups) == 1)
		aggstrategy = AGG_HASHED;

	/*
	 * Output will be in sorted order by group_pathkeys if, and only if, there
	 * is a single rollup operation on a non-empty list of grouping
	 * expressions.
	 */
	if (aggstrategy == AGG_SORTED && list_length(rollups) == 1)
		pathnode->path.pathkeys = root->group_pathkeys;
	else
		pathnode->path.pathkeys = NIL;

	pathnode->aggstrategy = aggstrategy;
	pathnode->rollups = rollups;
	pathnode->qual = having_qual;
	pathnode->transitionSpace = agg_costs ? agg_costs->transitionSpace : 0;

	Assert(rollups != NIL);
	Assert(aggstrategy != AGG_PLAIN || list_length(rollups) == 1);
	Assert(aggstrategy != AGG_MIXED || list_length(rollups) > 1);

	foreach(lc, rollups)
	{
		RollupData *rollup = lfirst(lc);
		List	   *gsets = rollup->gsets;
		int			numGroupCols = list_length(linitial(gsets));

		/*
		 * In AGG_SORTED or AGG_PLAIN mode, the first rollup takes the
		 * (already-sorted) input, and following ones do their own sort.
		 *
		 * In AGG_HASHED mode, there is one rollup for each grouping set.
		 *
		 * In AGG_MIXED mode, the first rollups are hashed, the first
		 * non-hashed one takes the (already-sorted) input, and following ones
		 * do their own sort.
		 */
		if (is_first)
		{
			cost_agg(&pathnode->path, root,
					 aggstrategy,
					 agg_costs,
					 numGroupCols,
					 rollup->numGroups,
					 having_qual,
					 subpath->startup_cost,
					 subpath->total_cost,
					 subpath->rows,
					 subpath->pathtarget->width);
			is_first = false;
			if (!rollup->is_hashed)
				is_first_sort = false;
		}
		else
		{
			Path		sort_path;	/* dummy for result of cost_sort */
			Path		agg_path;	/* dummy for result of cost_agg */

			if (rollup->is_hashed || is_first_sort)
			{
				/*
				 * Account for cost of aggregation, but don't charge input
				 * cost again
				 */
				cost_agg(&agg_path, root,
						 rollup->is_hashed ? AGG_HASHED : AGG_SORTED,
						 agg_costs,
						 numGroupCols,
						 rollup->numGroups,
						 having_qual,
						 0.0, 0.0,
						 subpath->rows,
						 subpath->pathtarget->width);
				if (!rollup->is_hashed)
					is_first_sort = false;
			}
			else
			{
				/* Account for cost of sort, but don't charge input cost again */
				cost_sort(&sort_path, root, NIL,
						  0.0,
						  subpath->rows,
						  subpath->pathtarget->width,
						  0.0,
						  work_mem,
						  -1.0);

				/* Account for cost of aggregation */

				cost_agg(&agg_path, root,
						 AGG_SORTED,
						 agg_costs,
						 numGroupCols,
						 rollup->numGroups,
						 having_qual,
						 sort_path.startup_cost,
						 sort_path.total_cost,
						 sort_path.rows,
						 subpath->pathtarget->width);
			}

			pathnode->path.total_cost += agg_path.total_cost;
			pathnode->path.rows += agg_path.rows;
		}
	}

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}

/*
 * create_minmaxagg_path
 *	  Creates a pathnode that represents computation of MIN/MAX aggregates
 *
 * 'rel' is the parent relation associated with the result
 * 'target' is the PathTarget to be computed
 * 'mmaggregates' is a list of MinMaxAggInfo structs
 * 'quals' is the HAVING quals if any
 */
MinMaxAggPath *
create_minmaxagg_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  PathTarget *target,
					  List *mmaggregates,
					  List *quals)
{
	MinMaxAggPath *pathnode = makeNode(MinMaxAggPath);
	Cost		initplan_cost;
	ListCell   *lc;

	/* The topmost generated Plan node will be a Result */
	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	/* A MinMaxAggPath implies use of subplans, so cannot be parallel-safe */
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	/* Result is one unordered row */
	pathnode->path.rows = 1;
	pathnode->path.pathkeys = NIL;

	pathnode->mmaggregates = mmaggregates;
	pathnode->quals = quals;

	/* Calculate cost of all the initplans ... */
	initplan_cost = 0;
	foreach(lc, mmaggregates)
	{
		MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);

		initplan_cost += mminfo->pathcost;
	}

	/* add tlist eval cost for each output row, plus cpu_tuple_cost */
	pathnode->path.startup_cost = initplan_cost + target->cost.startup;
	pathnode->path.total_cost = initplan_cost + target->cost.startup +
		target->cost.per_tuple + cpu_tuple_cost;

	/*
	 * Add cost of qual, if any --- but we ignore its selectivity, since our
	 * rowcount estimate should be 1 no matter what the qual is.
	 */
	if (quals)
	{
		QualCost	qual_cost;

		cost_qual_eval(&qual_cost, quals, root);
		pathnode->path.startup_cost += qual_cost.startup;
		pathnode->path.total_cost += qual_cost.startup + qual_cost.per_tuple;
	}

	return pathnode;
}

/*
 * create_windowagg_path
 *	  Creates a pathnode that represents computation of window functions
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'windowFuncs' is a list of WindowFunc structs
 * 'winclause' is a WindowClause that is common to all the WindowFuncs
 *
 * The input must be sorted according to the WindowClause's PARTITION keys
 * plus ORDER BY keys.
 */
WindowAggPath *
create_windowagg_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  Path *subpath,
					  PathTarget *target,
					  List *windowFuncs,
					  WindowClause *winclause,
					  List *winpathkeys)
{
	WindowAggPath *pathnode = makeNode(WindowAggPath);

	pathnode->path.pathtype = T_WindowAgg;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* WindowAgg preserves the input sort order */
	pathnode->path.pathkeys = subpath->distribution ? winpathkeys : subpath->pathkeys;

	pathnode->subpath = subpath;
	pathnode->winclause = winclause;

	pathnode->path.distribution = copyObject(subpath->distribution);

	/*
	 * For costing purposes, assume that there are no redundant partitioning
	 * or ordering columns; it's not worth the trouble to deal with that
	 * corner case here.  So we just pass the unmodified list lengths to
	 * cost_windowagg.
	 */
	cost_windowagg(&pathnode->path, root,
				   windowFuncs,
				   list_length(winclause->partitionClause),
				   list_length(winclause->orderClause),
				   subpath->startup_cost,
				   subpath->total_cost,
				   subpath->rows);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}

/*
 * create_setop_path
 *	  Creates a pathnode that represents computation of INTERSECT or EXCEPT
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'cmd' is the specific semantics (INTERSECT or EXCEPT, with/without ALL)
 * 'strategy' is the implementation strategy (sorted or hashed)
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'flagColIdx' is the column number where the flag column will be, if any
 * 'firstFlag' is the flag value for the first input relation when hashing;
 *		or -1 when sorting
 * 'numGroups' is the estimated number of distinct groups
 * 'outputRows' is the estimated number of output rows
 */
SetOpPath *
create_setop_path(PlannerInfo *root,
				  RelOptInfo *rel,
				  Path *subpath,
				  SetOpCmd cmd,
				  SetOpStrategy strategy,
				  List *distinctList,
				  AttrNumber flagColIdx,
				  int firstFlag,
				  double numGroups,
				  double outputRows)
{
	SetOpPath  *pathnode = makeNode(SetOpPath);

	pathnode->path.pathtype = T_SetOp;
	pathnode->path.parent = rel;
	/* SetOp doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* SetOp preserves the input sort order if in sort mode */
	pathnode->path.pathkeys =
		(strategy == SETOP_SORTED) ? subpath->pathkeys : NIL;

	pathnode->path.distribution = copyObject(subpath->distribution);

	pathnode->subpath = subpath;
	pathnode->cmd = cmd;
	pathnode->strategy = strategy;
	pathnode->distinctList = distinctList;
	pathnode->flagColIdx = flagColIdx;
	pathnode->firstFlag = firstFlag;
	pathnode->numGroups = numGroups;

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost +
		cpu_operator_cost * subpath->rows * list_length(distinctList);
	pathnode->path.rows = outputRows;

	return pathnode;
}

/*
 * create_recursiveunion_path
 *	  Creates a pathnode that represents a recursive UNION node
 *
 * 'rel' is the parent relation associated with the result
 * 'leftpath' is the source of data for the non-recursive term
 * 'rightpath' is the source of data for the recursive term
 * 'target' is the PathTarget to be computed
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'wtParam' is the ID of Param representing work table
 * 'numGroups' is the estimated number of groups
 *
 * For recursive UNION ALL, distinctList is empty and numGroups is zero
 */
RecursiveUnionPath *
create_recursiveunion_path(PlannerInfo *root,
						   RelOptInfo *rel,
						   Path *leftpath,
						   Path *rightpath,
						   PathTarget *target,
						   List *distinctList,
						   int wtParam,
						   double numGroups)
{
	RecursiveUnionPath *pathnode = makeNode(RecursiveUnionPath);

	pathnode->path.pathtype = T_RecursiveUnion;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		leftpath->parallel_safe && rightpath->parallel_safe;
	/* Foolish, but we'll do it like joins for now: */
	pathnode->path.parallel_workers = leftpath->parallel_workers;
	/* RecursiveUnion result is always unsorted */
	pathnode->path.pathkeys = NIL;

	/*
	 * FIXME This assumes left/right path have the same distribution, or one
	 * of them is NULL. This is related to the subquery_planner() assuming all
	 * tables are replicated on the same group of nodes, which may or may not
	 * be the case, and we need to be more careful about it.
	 */
	if (leftpath->distribution)
		pathnode->path.distribution = copyObject(leftpath->distribution);
	else
		pathnode->path.distribution = copyObject(rightpath->distribution);

	pathnode->leftpath = leftpath;
	pathnode->rightpath = rightpath;
	pathnode->distinctList = distinctList;
	pathnode->wtParam = wtParam;
	pathnode->numGroups = numGroups;

	cost_recursive_union(&pathnode->path, leftpath, rightpath);

	return pathnode;
}

/*
 * create_lockrows_path
 *	  Creates a pathnode that represents acquiring row locks
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'rowMarks' is a list of PlanRowMark's
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
LockRowsPath *
create_lockrows_path(PlannerInfo *root, RelOptInfo *rel,
					 Path *subpath, List *rowMarks, int epqParam)
{
	LockRowsPath *pathnode = makeNode(LockRowsPath);
	List		*rtable = root->parse->rtable;
	ListCell	*rowMark = NULL;
	int		replication_table = 0;
	int		rowMark_num = 0;

	pathnode->path.pathtype = T_LockRows;
	pathnode->path.parent = rel;
	/* LockRows doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	pathnode->path.rows = subpath->rows;

	foreach(rowMark, rowMarks)
	{
		PlanRowMark	*mark = lfirst_node(PlanRowMark, rowMark);
		Oid			relid;

		if (mark->strength == LCS_NONE)
			continue;
		rowMark_num++;

		relid = getrelid(mark->rti, rtable);
		if (GetRelationLocType(relid) == LOCATOR_TYPE_REPLICATED)
			replication_table++;
	}

	/* all table is replication table */
	if (replication_table > 0 && replication_table == rowMark_num &&
		subpath->distribution != NULL)
		subpath->distribution->replication_for_update = true;
	pathnode->path.distribution = copyObject(subpath->distribution);

	/*
	 * The result cannot be assumed sorted, since locking might cause the sort
	 * key columns to be replaced with new values.
	 */
	pathnode->path.pathkeys = NIL;

	pathnode->subpath = subpath;
	pathnode->rowMarks = rowMarks;
	pathnode->epqParam = epqParam;

	/*
	 * We should charge something extra for the costs of row locking and
	 * possible refetches, but it's hard to say how much.  For now, use
	 * cpu_tuple_cost per row.
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost +
		cpu_tuple_cost * subpath->rows;

	return pathnode;
}

/*
 * create_modifytable_path
 *	  Creates a pathnode that represents performing INSERT/UPDATE/DELETE mods
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is a Path producing source data
 * 'operation' is the operation type
 * 'canSetTag' is true if we set the command tag/es_processed
 * 'nominalRelation' is the parent RT index for use of EXPLAIN
 * 'partitioned_rels' is an integer list of RT indexes of non-leaf tables in
 *		the partition tree, if this is an UPDATE/DELETE to a partitioned table.
 *		Otherwise NIL.
 * 'partColsUpdated' is true if any partitioning columns are being updated,
 *		either from the target relation or a descendent partitioned table.
 * 'resultRelations' is an integer list of actual RT indexes of target rel(s)
 * 'updateColnosLists' is a list of UPDATE target column number lists
 *		(one sublist per rel); or NIL if not an UPDATE
 * 'withCheckOptionLists' is a list of WCO lists (one per rel)
 * 'returningLists' is a list of RETURNING tlists (one per rel)
 * 'rowMarks' is a list of PlanRowMarks (non-locking only)
 * 'onconflict' is the ON CONFLICT clause, or NULL
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
ModifyTablePath *
create_modifytable_path(PlannerInfo *root, RelOptInfo *rel,
						Path *subpath,
						CmdType operation, bool canSetTag,
						Index nominalRelation, List *partitioned_rels,
						bool partColsUpdated,
						List *resultRelations,
						List *updateColnosLists,
						List *withCheckOptionLists, List *returningLists,
						List *rowMarks, OnConflictExpr *onconflict,
						List *mergeActionLists, int epqParam, int parallelWorkers)
{
	ModifyTablePath *pathnode = makeNode(ModifyTablePath);

	Assert(operation == CMD_MERGE ||
	       (operation == CMD_UPDATE ? list_length(resultRelations) == list_length(updateColnosLists)
	                                : updateColnosLists == NIL));
	Assert(withCheckOptionLists == NIL ||
		   list_length(resultRelations) == list_length(withCheckOptionLists));
	Assert(returningLists == NIL ||
		   list_length(resultRelations) == list_length(returningLists));

	pathnode->path.pathtype = T_ModifyTable;
	pathnode->path.parent = rel;
	/* pathtarget is not interesting, just make it minimally valid */
	pathnode->path.pathtarget = rel->reltarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;

	pathnode->path.parallel_safe = 
		rel->consider_parallel && parallelWorkers > 0 && subpath->parallel_safe;

	pathnode->path.parallel_workers = parallelWorkers;
	pathnode->path.pathkeys = NIL;

	pathnode->path.parallel_aware = parallelWorkers > 0 ? true : false;

	/*
	 * Compute cost & rowcount as subpath cost & rowcount (if RETURNING)
	 *
	 * Currently, we don't charge anything extra for the actual table
	 * modification work, nor for the WITH CHECK OPTIONS or RETURNING
	 * expressions if any.  It would only be window dressing, since
	 * ModifyTable is always a top-level node and there is no way for the
	 * costs to change any higher-level planning choices.  But we might want
	 * to make it look better sometime.
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost;
	if (returningLists != NIL)
	{
		pathnode->path.rows = subpath->rows;

		/*
		 * Set width to match the subpath output.  XXX this is totally wrong:
		 * we should return an average of the RETURNING tlist widths.  But
		 * it's what happened historically, and improving it is a task for
		 * another day.  (Again, it's mostly window dressing.)
		 */
		pathnode->path.pathtarget->width = subpath->pathtarget->width;
	}
	else
	{
		pathnode->path.rows = 0;
		pathnode->path.pathtarget->width = 0;
	}

	pathnode->subpath = subpath;
	pathnode->operation = operation;
	pathnode->canSetTag = canSetTag;
	pathnode->nominalRelation = nominalRelation;
	pathnode->partitioned_rels = list_copy(partitioned_rels);
	pathnode->partColsUpdated = partColsUpdated;
	pathnode->resultRelations = resultRelations;
	pathnode->updateColnosLists = updateColnosLists;
	pathnode->withCheckOptionLists = withCheckOptionLists;
	pathnode->returningLists = returningLists;
	pathnode->rowMarks = rowMarks;
	pathnode->onconflict = onconflict;
	pathnode->epqParam = epqParam;
	pathnode->mergeActionLists = mergeActionLists;

    if (IS_CENTRALIZED_DATANODE && root -> parallel_dml)
	    cost_modifytable(pathnode, true);
                
	return pathnode;
}

/*
 * create_limit_path
 *	  Creates a pathnode that represents performing LIMIT/OFFSET
 *
 * In addition to providing the actual OFFSET and LIMIT expressions,
 * the caller must provide estimates of their values for costing purposes.
 * The estimates are as computed by preprocess_limit(), ie, 0 represents
 * the clause not being present, and -1 means it's present but we could
 * not estimate its value.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'limitOffset' is the actual OFFSET expression, or NULL
 * 'limitCount' is the actual LIMIT expression, or NULL
 * 'offset_est' is the estimated value of the OFFSET expression
 * 'count_est' is the estimated value of the LIMIT expression
 */
LimitPath *
create_limit_path(PlannerInfo *root, RelOptInfo *rel,
				  Path *subpath,
				  Node *limitOffset, Node *limitCount,
				  LimitOption limitOption,
				  int64 offset_est, int64 count_est)
{
	LimitPath  *pathnode = makeNode(LimitPath);

	pathnode->path.pathtype = T_Limit;
	pathnode->path.parent = rel;
	/* Limit doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.rows = subpath->rows;
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost;
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->subpath = subpath;
	pathnode->limitOffset = limitOffset;
	pathnode->limitCount = limitCount;
	pathnode->limitOption = limitOption;

	pathnode->path.distribution = copyObject(subpath->distribution);

	/*
	 * Adjust the output rows count and costs according to the offset/limit.
	 * This is only a cosmetic issue if we are at top level, but if we are
	 * building a subquery then it's important to report correct info to the
	 * outer planner.
	 *
	 * When the offset or count couldn't be estimated, use 10% of the
	 * estimated number of rows emitted from the subpath.
	 *
	 * XXX we don't bother to add eval costs of the offset/limit expressions
	 * themselves to the path costs.  In theory we should, but in most cases
	 * those expressions are trivial and it's just not worth the trouble.
	 */
	if (offset_est != 0)
	{
		double		offset_rows;

		if (offset_est > 0)
			offset_rows = (double) offset_est;
		else
			offset_rows = clamp_row_est(subpath->rows * 0.10);
		if (offset_rows > pathnode->path.rows)
			offset_rows = pathnode->path.rows;
		if (subpath->rows > 0)
			pathnode->path.startup_cost +=
				(subpath->total_cost - subpath->startup_cost)
				* offset_rows / subpath->rows;
		pathnode->path.rows -= offset_rows;
		if (pathnode->path.rows < 1)
			pathnode->path.rows = 1;
	}

	if (count_est != 0)
	{
		double		count_rows;

		if (count_est > 0)
			count_rows = (double) count_est;
		else
			count_rows = clamp_row_est(subpath->rows * 0.10);
		if (count_rows > pathnode->path.rows)
			count_rows = pathnode->path.rows;
		if (subpath->rows > 0)
		{
			int64 dead_tuple = 0;

			/*
			 * Notice: Current case only cover query block with only 1 table
			 *         Only nest loop join could pass the order key.
			 *
			 * The limit punish for index on column store table would add dead tuples
			 * 
			 * The factor for normal formula is count_rows/subpath->rows
			 * New formula is (count_rows+dead_tuple*0.5)/(subpath->rows+dead_tuple*0.5)
			 * The facor 0.5 is used to assume the dedire rows could be found in the medium
			 * to avoid too aggresive punishment on the indexes on column store tables. 
			 * 
			 * Todo : Extend to multi tables.
			 */

			pathnode->path.total_cost = pathnode->path.startup_cost +
				(subpath->total_cost - subpath->startup_cost)
				* (count_rows + dead_tuple) / (subpath->rows + dead_tuple);
		}
		pathnode->path.rows = count_rows;
		if (pathnode->path.rows < 1)
			pathnode->path.rows = 1;
	}

	return pathnode;
}

/*
 * create_connect_by_path
 *	  Creates a pathnode that represents performing CONNECT BY
 *	  (potentially START WITH)
 *	  
 *	  related special expression
 *	  1. PriorExpr for "prior"
 *	  2. LevelExpr for "level"
 *	  3. CBRootExpr for "connect_by_root"
 *	  4. CBPathExpr for "sys_connect_by_path"
 *	  5. CBIsLeafExpr for "connect_by_isleaf"
 *	  6. CBIsCycleExpr for "connect_by_iscycle" (not implemented yet!)
 *
 * 'rel' is the parent relation associated with the result
 * 'outer' is the path representing the root path with START WITH qual
 * 'inner' is the path representing the lower path, with push-down CONNECT BY qual
 * 'nocycle' is the flag provided whether error out when encounter cycle
 * 'quals' is the pull-up CONNECT BY qual, which must be evaluated on this node
 * 'cbparam' is the param information generated by preprocess_connectClause
 */

QualPath *
create_qual_path(PlannerInfo *root, Path *subpath, List *quals)
{
	QualPath   *pathnode = makeNode(QualPath);
	RelOptInfo *rel = subpath->parent;
	QualCost    qual_cost;
	Cost        run_cost;

	cost_qual_eval(&qual_cost, quals, root);

	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = subpath->pathtarget;
	pathnode->path.parallel_safe = false;
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->quals = quals;
	pathnode->subpath = subpath;

	pathnode->path.rows = subpath->rows;
	run_cost = subpath->total_cost - subpath->startup_cost;
	run_cost += (cpu_operator_cost + qual_cost.per_tuple) * pathnode->path.rows;

	pathnode->path.startup_cost = subpath->startup_cost + qual_cost.startup;
	pathnode->path.total_cost = subpath->total_cost + run_cost;
	pathnode->path.parallel_workers = subpath->parallel_safe ? subpath->parallel_workers : 0;

	return pathnode;
}

/*
 * reparameterize_path
 *		Attempt to modify a Path to have greater parameterization
 *
 * We use this to attempt to bring all child paths of an appendrel to the
 * same parameterization level, ensuring that they all enforce the same set
 * of join quals (and thus that that parameterization can be attributed to
 * an append path built from such paths).  Currently, only a few path types
 * are supported here, though more could be added at need.  We return NULL
 * if we can't reparameterize the given path.
 *
 * Note: we intentionally do not pass created paths to add_path(); it would
 * possibly try to delete them on the grounds of being cost-inferior to the
 * paths they were made from, and we don't want that.  Paths made here are
 * not necessarily of general-purpose usefulness, but they can be useful
 * as members of an append path.
 */
Path *
reparameterize_path(PlannerInfo *root, Path *path,
					Relids required_outer,
					double loop_count)
{
	RelOptInfo *rel = path->parent;

	/* Can only increase, not decrease, path's parameterization */
	if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
		return NULL;
	switch (path->pathtype)
	{
		case T_SeqScan:
			return create_seqscan_path(root, rel, required_outer, 0);
		case T_SampleScan:
			return (Path *) create_samplescan_path(root, rel, required_outer);
		case T_IndexScan:
		case T_IndexOnlyScan:
			{
				IndexPath  *ipath = (IndexPath *) path;
				IndexPath  *newpath = makeNode(IndexPath);

				/*
				 * We can't use create_index_path directly, and would not want
				 * to because it would re-compute the indexqual conditions
				 * which is wasted effort.  Instead we hack things a bit:
				 * flat-copy the path node, revise its param_info, and redo
				 * the cost estimate.
				 */
				memcpy(newpath, ipath, sizeof(IndexPath));
				newpath->path.param_info =
					get_baserel_parampathinfo(root, rel, required_outer);
				cost_index(newpath, root, loop_count, false);
				return (Path *) newpath;
			}
		case T_BitmapHeapScan:
			{
				BitmapHeapPath *bpath = (BitmapHeapPath *) path;

				return (Path *) create_bitmap_heap_path(root,
														rel,
														bpath->bitmapqual,
														required_outer,
														loop_count, 0);
			}
		case T_SubqueryScan:
			{
				SubqueryScanPath *spath = (SubqueryScanPath *) path;

				return (Path *) create_subqueryscan_path(root,
														 rel,
														 NULL,
														 spath->subpath,
														 spath->path.pathkeys,
#ifdef XCP
														 required_outer,
														 path->distribution);
#else
														 required_outer);
#endif
			}
		case T_Append:
			{
				AppendPath *apath = (AppendPath *) path;
				List	   *childpaths = NIL;
				List	   *partialpaths = NIL;
				int			i;
				ListCell   *lc;

				/* Reparameterize the children */
				i = 0;
				foreach(lc, apath->subpaths)
				{
					Path	   *spath = (Path *) lfirst(lc);

					spath = reparameterize_path(root, spath,
												required_outer,
												loop_count);
					if (spath == NULL)
						return NULL;
					/* We have to re-split the regular and partial paths */
					if (i < apath->first_partial_path)
						childpaths = lappend(childpaths, spath);
					else
						partialpaths = lappend(partialpaths, spath);
					i++;
				}
				return (Path *)
					create_append_path(root, rel, childpaths, partialpaths,
									   required_outer,
									   apath->path.parallel_workers,
									   apath->path.parallel_aware,
									   apath->partitioned_rels,
									   -1);
			}
		default:
			break;
	}
	return NULL;
}

/*
 * reparameterize_path_by_child
 * 		Given a path parameterized by the parent of the given child relation,
 * 		translate the path to be parameterized by the given child relation.
 *
 * The function creates a new path of the same type as the given path, but
 * parameterized by the given child relation.  Most fields from the original
 * path can simply be flat-copied, but any expressions must be adjusted to
 * refer to the correct varnos, and any paths must be recursively
 * reparameterized.  Other fields that refer to specific relids also need
 * adjustment.
 *
 * The cost, number of rows, width and parallel path properties depend upon
 * path->parent, which does not change during the translation. Hence those
 * members are copied as they are.
 *
 * If the given path can not be reparameterized, the function returns NULL.
 */
Path *
reparameterize_path_by_child(PlannerInfo *root, Path *path,
							 RelOptInfo *child_rel)
{

#define FLAT_COPY_PATH(newnode, node, nodetype)  \
	( (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define ADJUST_CHILD_ATTRS(node) \
	((node) = \
	 (List *) adjust_appendrel_attrs_multilevel(root, (Node *) (node), \
												child_rel->relids, \
												child_rel->top_parent_relids))

#define REPARAMETERIZE_CHILD_PATH(path) \
do { \
	(path) = reparameterize_path_by_child(root, (path), child_rel); \
	if ((path) == NULL) \
		return NULL; \
} while(0);

#define REPARAMETERIZE_CHILD_PATH_LIST(pathlist) \
do { \
	if ((pathlist) != NIL) \
	{ \
		(pathlist) = reparameterize_pathlist_by_child(root, (pathlist), \
													  child_rel); \
		if ((pathlist) == NIL) \
			return NULL; \
	} \
} while(0);

	Path	   *new_path;
	ParamPathInfo *new_ppi;
	ParamPathInfo *old_ppi;
	Relids		required_outer;

	/*
	 * If the path is not parameterized by parent of the given relation, it
	 * doesn't need reparameterization.
	 */
	if (!path->param_info ||
		!bms_overlap(PATH_REQ_OUTER(path), child_rel->top_parent_relids))
		return path;

	/*
	 * If possible, reparameterize the given path, making a copy.
	 *
	 * This function is currently only applied to the inner side of a nestloop
	 * join that is being partitioned by the partitionwise-join code.  Hence,
	 * we need only support path types that plausibly arise in that context.
	 * (In particular, supporting sorted path types would be a waste of code
	 * and cycles: even if we translated them here, they'd just lose in
	 * subsequent cost comparisons.)  If we do see an unsupported path type,
	 * that just means we won't be able to generate a partitionwise-join plan
	 * using that path type.
	 */
	switch (nodeTag(path))
	{
		case T_Path:
			FLAT_COPY_PATH(new_path, path, Path);
			break;

		case T_IndexPath:
			{
				IndexPath  *ipath;

				FLAT_COPY_PATH(ipath, path, IndexPath);
				ADJUST_CHILD_ATTRS(ipath->indexclauses);
				ADJUST_CHILD_ATTRS(ipath->indexquals);
				new_path = (Path *) ipath;
			}
			break;

		case T_BitmapHeapPath:
			{
				BitmapHeapPath *bhpath;

				FLAT_COPY_PATH(bhpath, path, BitmapHeapPath);
				REPARAMETERIZE_CHILD_PATH(bhpath->bitmapqual);
				new_path = (Path *) bhpath;
			}
			break;

		case T_BitmapAndPath:
			{
				BitmapAndPath *bapath;

				FLAT_COPY_PATH(bapath, path, BitmapAndPath);
				REPARAMETERIZE_CHILD_PATH_LIST(bapath->bitmapquals);
				new_path = (Path *) bapath;
			}
			break;

		case T_BitmapOrPath:
			{
				BitmapOrPath *bopath;

				FLAT_COPY_PATH(bopath, path, BitmapOrPath);
				REPARAMETERIZE_CHILD_PATH_LIST(bopath->bitmapquals);
				new_path = (Path *) bopath;
			}
			break;

		case T_ForeignPath:
			{
				ForeignPath *fpath;
				ReparameterizeForeignPathByChild_function rfpc_func;

				FLAT_COPY_PATH(fpath, path, ForeignPath);
				if (fpath->fdw_outerpath)
					REPARAMETERIZE_CHILD_PATH(fpath->fdw_outerpath);

				/* Hand over to FDW if needed. */
				rfpc_func =
					path->parent->fdwroutine->ReparameterizeForeignPathByChild;
				if (rfpc_func)
					fpath->fdw_private = rfpc_func(root, fpath->fdw_private,
												   child_rel);
				new_path = (Path *) fpath;
			}
			break;

		case T_CustomPath:
			{
				CustomPath *cpath;

				FLAT_COPY_PATH(cpath, path, CustomPath);
				REPARAMETERIZE_CHILD_PATH_LIST(cpath->custom_paths);
				if (cpath->methods &&
					cpath->methods->ReparameterizeCustomPathByChild)
					cpath->custom_private =
						cpath->methods->ReparameterizeCustomPathByChild(root,
																		cpath->custom_private,
																		child_rel);
				new_path = (Path *) cpath;
			}
			break;

		case T_NestPath:
			{
				JoinPath   *jpath;

				FLAT_COPY_PATH(jpath, path, NestPath);

				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				new_path = (Path *) jpath;
			}
			break;

		case T_MergePath:
			{
				JoinPath   *jpath;
				MergePath  *mpath;

				FLAT_COPY_PATH(mpath, path, MergePath);

				jpath = (JoinPath *) mpath;
				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				ADJUST_CHILD_ATTRS(mpath->path_mergeclauses);
				new_path = (Path *) mpath;
			}
			break;

		case T_HashPath:
			{
				JoinPath   *jpath;
				HashPath   *hpath;

				FLAT_COPY_PATH(hpath, path, HashPath);

				jpath = (JoinPath *) hpath;
				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				ADJUST_CHILD_ATTRS(hpath->path_hashclauses);
				new_path = (Path *) hpath;
			}
			break;

		case T_AppendPath:
			{
				AppendPath *apath;

				FLAT_COPY_PATH(apath, path, AppendPath);
				REPARAMETERIZE_CHILD_PATH_LIST(apath->subpaths);
				new_path = (Path *) apath;
			}
			break;

		case T_GatherPath:
			{
				GatherPath *gpath;

				FLAT_COPY_PATH(gpath, path, GatherPath);
				REPARAMETERIZE_CHILD_PATH(gpath->subpath);
				new_path = (Path *) gpath;
			}
			break;

		case T_RemoteSubPath:
			{
				RemoteSubPath *remotepath;

				FLAT_COPY_PATH(remotepath, path, RemoteSubPath);
				REPARAMETERIZE_CHILD_PATH(remotepath->subpath);
				new_path = (Path *) remotepath;
			}
			break;

		default:

			/* We don't know how to reparameterize this path. */
			return NULL;
	}

	/*
	 * Adjust the parameterization information, which refers to the topmost
	 * parent. The topmost parent can be multiple levels away from the given
	 * child, hence use multi-level expression adjustment routines.
	 */
	old_ppi = new_path->param_info;
	required_outer =
		adjust_child_relids_multilevel(root, old_ppi->ppi_req_outer,
									   child_rel->relids,
									   child_rel->top_parent_relids);

	/* If we already have a PPI for this parameterization, just return it */
	new_ppi = find_param_path_info(new_path->parent, required_outer);

	/*
	 * If not, build a new one and link it to the list of PPIs. For the same
	 * reason as explained in mark_dummy_rel(), allocate new PPI in the same
	 * context the given RelOptInfo is in.
	 */
	if (new_ppi == NULL)
	{
		MemoryContext oldcontext;
		RelOptInfo *rel = path->parent;

		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

		new_ppi = makeNode(ParamPathInfo);
		new_ppi->ppi_req_outer = bms_copy(required_outer);
		new_ppi->ppi_rows = old_ppi->ppi_rows;
		new_ppi->ppi_clauses = old_ppi->ppi_clauses;
		ADJUST_CHILD_ATTRS(new_ppi->ppi_clauses);
		rel->ppilist = lappend(rel->ppilist, new_ppi);

		MemoryContextSwitchTo(oldcontext);
	}
	bms_free(required_outer);

	new_path->param_info = new_ppi;

	/*
	 * Adjust the path target if the parent of the outer relation is
	 * referenced in the targetlist. This can happen when only the parent of
	 * outer relation is laterally referenced in this relation.
	 */
	if (bms_overlap(path->parent->lateral_relids,
					child_rel->top_parent_relids))
	{
		new_path->pathtarget = copy_pathtarget(new_path->pathtarget);
		ADJUST_CHILD_ATTRS(new_path->pathtarget->exprs);
	}

	return new_path;
}

/*
 * reparameterize_pathlist_by_child
 * 		Helper function to reparameterize a list of paths by given child rel.
 */
static List *
reparameterize_pathlist_by_child(PlannerInfo *root,
								 List *pathlist,
								 RelOptInfo *child_rel)
{
	ListCell   *lc;
	List	   *result = NIL;

	foreach(lc, pathlist)
	{
		Path	   *path = reparameterize_path_by_child(root, lfirst(lc),
														child_rel);

		if (path == NULL)
		{
			list_free(result);
			return NIL;
		}

		result = lappend(result, path);
	}

	return result;
}

#ifdef __OPENTENBASE__
/*
 * Count datanode number for given path, consider replication table as 1
 * because we use this function to figure out how many parts that data
 * had been separated into, when we estimating costs of a plan. Therefore
 * to get more accurate estimating result as in a distributed system.
 */
double
path_count_datanodes(Path *path)
{
	Distribution *dist = path->distribution;

	if (dist && !IsLocatorReplicated(dist->distributionType))
	{
		double nodes;
		
		nodes = bms_num_members(dist->restrictNodes);
		if (nodes > 0)
			return nodes;
		
		nodes = bms_num_members(dist->nodes);
		if (nodes > 0)
			return nodes;
	}
	
	return 1;
}

/*
 * create_mergequalproj_path
 *
 */
Path *
create_mergequalproj_path(PlannerInfo *root, Path *subpath, List *mergeActionLists,
                          Distribution *distribution)
{
	RelOptInfo        *rel = subpath->parent;
	MergeQualProjPath *pathnode;
	ListCell          *lc;
	ListCell          *lcn;
	MergeAction       *action;
	MergeAction       *qualprojAction;
	List              *mergeActionList;

	pathnode = makeNode(MergeQualProjPath);
	pathnode->path.pathtype = T_MergeQualProj;
	pathnode->path.parent = rel;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->subpath = subpath;
	pathnode->path.distribution = (Distribution *) copyObject(distribution);

	pathnode->path.parallel_aware = subpath->parallel_aware;
	pathnode->path.parallel_safe = subpath->parallel_safe;
	pathnode->path.parallel_safe = subpath->parallel_workers;

	pathnode->path.pathtarget = subpath->pathtarget;

	/* for INSERT action, we use the node's toplevel resultRel action*/
	mergeActionList = linitial(root->mergeActionLists);
	foreach(lc, mergeActionList)
	{
		action = lfirst_node(MergeAction, lc);
		qualprojAction = makeNode(MergeAction);
		qualprojAction->commandType = action->commandType;
		qualprojAction->matched = action->matched;
		if (action->commandType == CMD_INSERT)
		{
			qualprojAction->qual = action->qual;
			qualprojAction->targetList = action->targetList;
			lcn = list_nth_cell(root->processed_tlist, list_length(qualprojAction->targetList));
			while (lcn)
			{
				qualprojAction->targetList =
					lappend(qualprojAction->targetList, copyObject(lfirst(lcn)));
				lcn = lcn->next;
			}
			action->actionOnly = true;
		}
		pathnode->mergeActionLists = lappend(pathnode->mergeActionLists, qualprojAction);
	}

	cost_mergequalproj((Path *) pathnode,
	                   subpath->startup_cost,
	                   subpath->total_cost,
	                   subpath->rows,
	                   rel->reltarget->width);

	return (Path *) pathnode;
}

/*
 * Calculate the tuple replication times based on distribution and subpath
 */
static int
calcDistReplications(Distribution *dist, Path *subpath)
{
	bool is_dist;
	bool is_sub_dist;
	Distribution *subdist = subpath->distribution;

	is_dist = (dist && IsLocatorColumnDistributed(dist->distributionType));
	is_sub_dist = (subdist && IsLocatorColumnDistributed(subdist->distributionType));

	if (is_sub_dist && !is_dist)
		return (int) path_count_datanodes(subpath);
	else
		return 1;
}

PathCostComparison
compare_path_costs_by_rules_fuzzily(Path *path1, Path *path2)
{
	bool is_path1_global_index = IS_GLOBAL_INDEX_PATH(path1);
	bool is_path2_global_index = IS_GLOBAL_INDEX_PATH(path2);

	if (!is_path1_global_index && is_path2_global_index)
	{
		if (IS_CENTRALIZED_MODE)
		{
			if (path2->rows <= max_global_index_scan_rows_size && path2->param_info == NULL)
				return COSTS_BETTER2;
			else
				return COSTS_BETTER1;
		}

		if (IS_PGXC_DATANODE)
		{
			/*
			 * if generate an execution plan on a datanode and use the global index attribute
			 * to restrict the node, must use the global index where the limit attribute is located,
			 * otherwise, the global index cannot be used
			 */
			IndexPath *path = (IndexPath*) path2;
			return (strcmp(g_index_table_part_name, get_rel_name(path->indexinfo->indexoid)) == 0 ) ?
					COSTS_BETTER2 : COSTS_BETTER1;
		}

		if (path2->distribution == NULL || path2->param_info != NULL ||
			(path2->distribution->restrictNodes == NULL) ||
			 path2->rows > max_global_index_scan_rows_size)
		{
			/*
			 * the global index cannot determine which specific node to execute
			 * or the planner estimates rows larger than max_global_index_scan_rows_size,
			 * do not choose this global index
			 */
			return COSTS_BETTER1;
		}
		else if (path1->distribution != NULL && path1->distribution->restrictNodes != NULL && IsA(path1, IndexPath))
		{
			/*
			 * another path can determine the specific node to execute,
			 * the another path is better than global index path
			 */
			return COSTS_BETTER1;
		}
		else
		{
			/*
			 * another path cannot determine which specific node to execute,
			 * but the global index can, choose this global index
			 */
			return COSTS_BETTER2;
		}
	}
	else if (is_path1_global_index && !is_path2_global_index)
	{
		if (IS_CENTRALIZED_MODE)
		{
			if (path1->rows <= max_global_index_scan_rows_size && path1->param_info == NULL)
				return COSTS_BETTER1;
			else
				return COSTS_BETTER2;
		}

		if (IS_PGXC_DATANODE)
		{
			/*
			 * if generate an execution plan on a datanode and use the global index attribute
			 * to restrict the node, must use the global index where the limit attribute is located,
			 * otherwise, the global index cannot be used
			 */
			IndexPath *path = (IndexPath*) path1;
			return (strcmp(g_index_table_part_name, get_rel_name(path->indexinfo->indexoid)) == 0) ?
					COSTS_BETTER1 : COSTS_BETTER2;
		}

		if (path1->distribution == NULL || path1->param_info != NULL ||
			(path1->distribution->restrictNodes == NULL) ||
			 path1->rows > max_global_index_scan_rows_size)
		{
			/*
			 * the global index cannot determine which specific node to execute,
			 * or the planner estimates rows larger than max_global_index_scan_rows_size
			 * do not choose this global index
			 */
			return COSTS_BETTER2;
		}
		else if (path2->distribution != NULL && path2->distribution->restrictNodes != NULL && IsA(path2, IndexPath))
		{
			/*
			 * another path can determine the specific node to execute,
			 * the another path is better than global index path
			 */
			return COSTS_BETTER2;
		}
		else
		{
			/*
			 * another path cannot determine which specific node to execute,
			 * but the global index can, choose this global index
			 */
			return COSTS_BETTER1;
		}
	}
	else
	{
		if (IS_CENTRALIZED_MODE)
			return COSTS_EQUAL;

		if (IS_PGXC_DATANODE && is_path1_global_index && is_path2_global_index)
		{
			/*
			 * if global index attr is query_part_attr, choose it
			 */
			IndexPath *path = (IndexPath*) path1;
			Assert(path->indexinfo->ncolumns == 1);
			if (strcmp(g_index_table_part_name, get_rel_name(path->indexinfo->indexoid)) == 0)
			{
				return COSTS_BETTER1;
			}

			path = (IndexPath*) path2;
			if (strcmp(g_index_table_part_name, get_rel_name(path->indexinfo->indexoid)) == 0)
			{
				return COSTS_BETTER2;
			}
		}

		/*
		 * if all are global indexes or none of them are global indexes,
		 * need to compare by costs, return COSTS_EQUAL here
		 */
		return COSTS_EQUAL;
	}
}
#endif
