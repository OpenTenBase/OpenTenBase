/*-------------------------------------------------------------------------
 *
 * paths.h
 *	  prototypes for various files in optimizer/path
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/paths.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PATHS_H
#define PATHS_H

#include "nodes/relation.h"

/*
 * allpaths.c
 */
extern PGDLLIMPORT bool enable_geqo;
extern PGDLLIMPORT int	geqo_threshold;
extern PGDLLIMPORT int	min_parallel_table_scan_size;
extern PGDLLIMPORT int	min_parallel_index_scan_size;
extern PGDLLIMPORT int	parallel_dml_tuple_num_threshold;
extern PGDLLIMPORT int	planning_dml_sql;

#ifdef __OPENTENBASE_C__

/*
 * In PostgreSQL, the row count estimate of a base rel scan, like a Seq Scan
 * or an Index Scan, can be directly copied from RelOptInfo->rows/tuples. In
 * OpenTenBase, it's not that straightforward as a Scan runs in parallel in the
 * DNs, and the number of rows scanned by each Scan is RelOptInfo->rows /
 * number of DN.
 *
 * That's pretty straightforward, too, but it means that we'd have to modify
 * all the cost_seqscan, cost_index, etc. functions to take that into
 * account. That's prone to bugs, because it is easy to miss references to
 * rel->rows/tuples/pages. Even if we fix them all now, more can be
 * introduced in merges with PostgreSQL, and it's not easy to notice because
 * the only consequence is a bad cost estimate.
 *
 * To make that more robust with PostgreSQL merges, we do a little switcheroo
 * with the RelOptInfo. The RelOptInfoDataNode struct is a "proxy" of
 * RelOptInfo, containing the same fields, except that the rows/pages/tuple
 * have already been divided by the number of data nodes. The costing functions
 * have been modified so that on entry, they construct a RelOptInfoDataNode and
 * use it in place of the RelOptInfo. That way, the formulas in the costing
 * functions can still refer to "rel->pages", "rel->tuples" and so forth in
 * the source code, keeping them unchanged from upstream, but will actually
 * use the adjusted values.
 *
 * The RelOptInfoDataNode struct doesn't contain all the fields from RelOptInfo,
 * only the ones commonly used in the cost_*() functions. If a reference to a
 * new field is added in uptream, and it's not handled either by adding it to
 * the RelOptInfoDataNode, or by modifying the reference to explictly point to
 * the original RelOptInfo, you'll get a compiler error. That's good: it forces
 * you to think whether the value needs to be divided by nDNs or not.
 */
#define PAGES_PER_DN(pages) \
	(ceil((double) (pages) / num_nodes))

#define ROWS_PER_DN(rows) \
	(clamp_row_est((rows) / num_nodes))

#define TUPLES_PER_DN(tuples) \
	(clamp_row_est((tuples) / num_nodes))
extern int  max_global_index_scan_rows_size;

#endif

/* Hook for plugins to get control in set_rel_pathlist() */
typedef void (*set_rel_pathlist_hook_type) (PlannerInfo *root,
											RelOptInfo *rel,
											Index rti,
											RangeTblEntry *rte);
extern PGDLLIMPORT set_rel_pathlist_hook_type set_rel_pathlist_hook;

/* Hook for plugins to get control in add_paths_to_joinrel() */
typedef void (*set_join_pathlist_hook_type) (PlannerInfo *root,
											 RelOptInfo *joinrel,
											 RelOptInfo *outerrel,
											 RelOptInfo *innerrel,
											 JoinType jointype,
											 JoinPathExtraData *extra);
extern PGDLLIMPORT set_join_pathlist_hook_type set_join_pathlist_hook;

/* Hook for plugins to replace standard_join_search() */
typedef RelOptInfo *(*join_search_hook_type) (PlannerInfo *root,
											  int levels_needed,
											  List *initial_rels);

/* Hook for plugins to replace set_joinpath_distribution() */
typedef List * (*set_distribution_hook_type) (PlannerInfo *root, JoinPath *pathnode);

extern PGDLLIMPORT join_search_hook_type join_search_hook;
extern PGDLLIMPORT set_distribution_hook_type set_distribution_hook;

#ifdef __OPENTENBASE_C__
/* Hook for plugins to get control in set_rel_size() */
typedef void (*set_rel_size_hook_type) (PlannerInfo *root,
											RelOptInfo *rel,
											Index rti,
											RangeTblEntry *rte);
extern PGDLLIMPORT set_rel_size_hook_type set_rel_size_hook;
#endif

extern RelOptInfo *make_one_rel(PlannerInfo *root, List *joinlist);
extern void set_dummy_rel_pathlist(RelOptInfo *rel);
extern RelOptInfo *standard_join_search(PlannerInfo *root, int levels_needed,
					 List *initial_rels);

extern void generate_gather_paths(PlannerInfo *root, RelOptInfo *rel,
					  bool override_rows);
extern int compute_parallel_worker(RelOptInfo *rel, double heap_pages,
						double index_pages);
extern void create_partial_bitmap_paths(PlannerInfo *root, RelOptInfo *rel,
							Path *bitmapqual);
extern void generate_partitionwise_join_paths(PlannerInfo *root,
								   RelOptInfo *rel);
#ifdef OPTIMIZER_DEBUG
extern void debug_print_rel(PlannerInfo *root, RelOptInfo *rel);
#endif

/*
 * indxpath.c
 *	  routines to generate index paths
 */
extern void create_index_paths(PlannerInfo *root, RelOptInfo *rel);
extern bool relation_has_unique_index_for(PlannerInfo *root, RelOptInfo *rel,
							  List *restrictlist,
							  List *exprlist, List *oprlist);
extern bool indexcol_is_bool_constant_for_query(IndexOptInfo *index,
									int indexcol);
extern bool match_index_to_operand(Node *operand, int indexcol,
					   IndexOptInfo *index);
extern void expand_indexqual_conditions(IndexOptInfo *index,
							List *indexclauses, List *indexclausecols,
							List **indexquals_p, List **indexqualcols_p);
extern void check_index_predicates(PlannerInfo *root, RelOptInfo *rel);
extern Expr *adjust_rowcompare_for_index(RowCompareExpr *clause,
							IndexOptInfo *index,
							int indexcol,
							List **indexcolnos,
							bool *var_on_left_p);

/*
 * tidpath.h
 *	  routines to generate tid paths
 */
extern void create_tidscan_paths(PlannerInfo *root, RelOptInfo *rel);

/*
 * joinpath.c
 *	   routines to create join paths
 */
extern void add_paths_to_joinrel(PlannerInfo *root, RelOptInfo *joinrel,
					 RelOptInfo *outerrel, RelOptInfo *innerrel,
					 JoinType jointype, SpecialJoinInfo *sjinfo,
					 List *restrictlist);

/*
 * joinrels.c
 *	  routines to determine which relations to join
 */
extern void join_search_one_level(PlannerInfo *root, int level);
extern RelOptInfo *make_join_rel(PlannerInfo *root,
			  RelOptInfo *rel1, RelOptInfo *rel2);
extern bool have_join_order_restriction(PlannerInfo *root,
							RelOptInfo *rel1, RelOptInfo *rel2);
extern bool have_dangerous_phv(PlannerInfo *root,
				   Relids outer_relids, Relids inner_params);
extern void mark_dummy_rel(RelOptInfo *rel);
extern bool have_partkey_equi_join(RelOptInfo *joinrel, RelOptInfo *rel1, RelOptInfo *rel2,
                                          JoinType jointype, List *restrictlist);
extern bool have_partkey_in_groupby(PlannerInfo *root, RelOptInfo *rel1, List *groupExprs);

#ifdef __OPENTENBASE_C__
/*
 * materialpath.c
 *	  routines to create materialization paths
 */
typedef enum
{
	MATERIAL_EQUAL,			/* AttrMasks are identical */
	MATERIAL_BETTER1,		/* AttrMasks 1 is a superset of AttrMasks 2 */
	MATERIAL_BETTER2,		/* vice versa */
	MATERIAL_DIFFERENT		/* neither AttrMasks includes the other */
} MaterialComparison;

/* Macro for checking whether the path is still not fully materialized */
#define IS_UNMATERIAL_PATH(path)  \
	((path)->attr_masks ? TRUE : FALSE)

extern MaterialComparison compare_material_status(RelOptInfo *parent_rel,
								  	  Path *path1, Path *path2);
extern int get_path_lm_width(PlannerInfo *root, RelOptInfo *baserel,
							 AttrMask masks);
extern List * generate_joinrestrict_satisfied_lm_paths(PlannerInfo *root,
								List *pathlist, JoinPathExtraData *extra);
extern AttrMask * create_hashjoin_attr_masks(PlannerInfo *root,
							 RelOptInfo *joinrel,
							 Path *inner_path,
							 Path *outer_path);
#endif

/*
 * equivclass.c
 *	  routines for managing EquivalenceClasses
 */
typedef bool (*ec_matches_callback_type) (PlannerInfo *root,
										  RelOptInfo *rel,
										  EquivalenceClass *ec,
										  EquivalenceMember *em,
										  void *arg);

extern bool process_equivalence(PlannerInfo *root,
					RestrictInfo **p_restrictinfo,
					bool below_outer_join);
extern Expr *canonicalize_ec_expression(Expr *expr,
						   Oid req_type, Oid req_collation);
extern void reconsider_outer_join_clauses(PlannerInfo *root);
extern EquivalenceClass *get_eclass_for_sort_expr(PlannerInfo *root,
						 Expr *expr,
						 Relids nullable_relids,
						 List *opfamilies,
						 Oid opcintype,
						 Oid collation,
						 Index sortref,
						 Relids rel,
						 bool create_it);
extern void generate_base_implied_equalities(PlannerInfo *root);
extern List *generate_join_implied_equalities(PlannerInfo *root,
								 Relids join_relids,
								 Relids outer_relids,
								 RelOptInfo *inner_rel);
extern List *generate_join_implied_equalities_for_ecs(PlannerInfo *root,
										 List *eclasses,
										 Relids join_relids,
										 Relids outer_relids,
										 RelOptInfo *inner_rel);
extern bool exprs_known_equal(PlannerInfo *root, Node *item1, Node *item2);
extern EquivalenceClass *match_eclasses_to_foreign_key_col(PlannerInfo *root,
								  ForeignKeyOptInfo *fkinfo,
								  int colno);
extern void add_child_rel_equivalences(PlannerInfo *root,
						   AppendRelInfo *appinfo,
						   RelOptInfo *parent_rel,
						   RelOptInfo *child_rel);
extern List *generate_implied_equalities_for_column(PlannerInfo *root,
									   RelOptInfo *rel,
									   ec_matches_callback_type callback,
									   void *callback_arg,
									   Relids prohibited_rels);
extern bool have_relevant_eclass_joinclause(PlannerInfo *root,
								RelOptInfo *rel1, RelOptInfo *rel2);
extern bool has_relevant_eclass_joinclause(PlannerInfo *root,
							   RelOptInfo *rel1);
extern bool eclass_useful_for_merging(PlannerInfo *root,
						  EquivalenceClass *eclass,
						  RelOptInfo *rel);
extern bool is_redundant_derived_clause(RestrictInfo *rinfo, List *clauselist);

/*
 * pathkeys.c
 *	  utilities for matching and building path keys
 */
typedef enum
{
	PATHKEYS_EQUAL,				/* pathkeys are identical */
	PATHKEYS_BETTER1,			/* pathkey 1 is a superset of pathkey 2 */
	PATHKEYS_BETTER2,			/* vice versa */
	PATHKEYS_DIFFERENT			/* neither pathkey includes the other */
} PathKeysComparison;

extern PathKeysComparison compare_pathkeys(List *keys1, List *keys2);
extern bool pathkeys_contained_in(List *keys1, List *keys2);
extern Path *get_cheapest_path_for_pathkeys(List *paths, List *pathkeys,
							   Relids required_outer,
							   CostSelector cost_criterion,
							   bool require_parallel_safe);
extern Path *get_cheapest_fractional_path_for_pathkeys(List *paths,
										  List *pathkeys,
										  Relids required_outer,
										  double fraction);
extern Path *get_cheapest_parallel_safe_total_inner(List *paths);
extern List *build_index_pathkeys(PlannerInfo *root, IndexOptInfo *index,
					 ScanDirection scandir);
extern List *build_expression_pathkey(PlannerInfo *root, Expr *expr,
						 Relids nullable_relids, Oid opno,
						 Relids rel, bool create_it);
extern List *convert_subquery_pathkeys(PlannerInfo *root, RelOptInfo *rel,
									   List *subquery_pathkeys,
									   List *subquery_tlist,
									   bool create_it);
extern List *build_join_pathkeys(PlannerInfo *root,
					RelOptInfo *joinrel,
					JoinType jointype,
					List *outer_pathkeys);
extern List *make_pathkeys_for_sortclauses(PlannerInfo *root,
							  List *sortclauses,
							  List *tlist);
extern void initialize_mergeclause_eclasses(PlannerInfo *root,
								RestrictInfo *restrictinfo);
extern void update_mergeclause_eclasses(PlannerInfo *root,
							RestrictInfo *restrictinfo);
extern List *find_mergeclauses_for_pathkeys(PlannerInfo *root,
							   List *pathkeys,
							   bool outer_keys,
							   List *restrictinfos);
extern List *select_outer_pathkeys_for_merge(PlannerInfo *root,
								List *mergeclauses,
								RelOptInfo *joinrel);
extern List *make_inner_pathkeys_for_merge(PlannerInfo *root,
							  List *mergeclauses,
							  List *outer_pathkeys);
extern List *truncate_useless_pathkeys(PlannerInfo *root,
						  RelOptInfo *rel,
						  List *pathkeys);
extern bool has_useful_pathkeys(PlannerInfo *root, RelOptInfo *rel);
extern PathKey *make_canonical_pathkey(PlannerInfo *root,
					   EquivalenceClass *eclass, Oid opfamily,
					   int strategy, bool nulls_first);
extern void add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel,
									List *live_childrels);
#ifdef __OPENTENBASE__
extern double path_count_datanodes(Path *path);
/*
 * We think a local agg is possible worth when the input is larger than output,
 * and we could do agg remove duplicated tuples and reduce amount of data
 * transferred in network.
 * But if the data is located on only one node, do re-distribute first to avoid
 * a potential single-node bottleneck.
 */
#define is_local_agg_worth(localrows, path) \
	((localrows) < (path)->rows && path_count_datanodes(path) > 1)
#endif
#endif							/* PATHS_H */
