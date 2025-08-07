/*-------------------------------------------------------------------------
 *
 * planner.h
 *	  prototypes for planner.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"


#define HINT_AGG_HASH_MASK 0x01
#define HINT_AGG_SORT_MASK 0x02
#define HINT_AGG_HASH_SORT_MASK HINT_AGG_HASH_MASK | HINT_AGG_SORT_MASK

#define HINT_AGG_HASH_TEST(type_mask) (type_mask & HINT_AGG_HASH_MASK)
#define HINT_AGG_SORT_TEST(type_mask) (type_mask & HINT_AGG_SORT_MASK)


#define MERGE_JOIN_FULL 0x01
#define MERGE_JOIN_NOT_FULL 0x02

#define MERGE_JOIN_FULL_TEST(type_mask) (type_mask & MERGE_JOIN_FULL)
#define MERGE_JOIN_NOT_FULL_TEST(type_mask) (type_mask & MERGE_JOIN_NOT_FULL)

#ifdef __OPENTENBASE_C__
extern bool window_optimizer;
extern bool group_optimizer;
extern bool distinct_optimizer;
extern bool distinct_with_hash;
extern bool enable_qualification_pushdown;
#endif

/* Hook for plugins to get control in planner() */
typedef PlannedStmt *(*planner_hook_type) (Query *parse,
										   int cursorOptions,
										   ParamListInfo boundParams,
										   int cached_param_num, 
										   bool explain);
extern PGDLLIMPORT planner_hook_type planner_hook;

/* Hook for plugins to get control when grouping_planner() plans upper rels */
typedef void (*create_upper_paths_hook_type) (PlannerInfo *root,
											  UpperRelationKind stage,
											  RelOptInfo *input_rel,
											  RelOptInfo *output_rel);
extern PGDLLIMPORT create_upper_paths_hook_type create_upper_paths_hook;

typedef double (* set_agg_rows_hook_type) (PlannerInfo *root, AggType type, double dNumGroups);
extern PGDLLIMPORT set_agg_rows_hook_type set_agg_rows_hook;

typedef int (* set_agg_path_hook_type) (PlannerInfo *root, AggType type);
typedef int (* set_merge_join_key_hook_type) (PlannerInfo *root, Relids joinrelids);
extern PGDLLIMPORT set_agg_path_hook_type set_agg_path_hook;
extern PGDLLIMPORT set_merge_join_key_hook_type set_merge_join_key_hook;

extern PlannedStmt *planner(Query *parse, int cursorOptions,
		ParamListInfo boundParams, int cached_param_num, bool explain);
extern PlannedStmt *standard_planner(Query *parse, int cursorOptions,
				 ParamListInfo boundParams);

extern PlannerInfo *subquery_planner(PlannerGlobal *glob, Query *parse,
				 PlannerInfo *parent_root,
				 bool hasRecursion, double tuple_fraction);

extern bool is_dummy_plan(Plan *plan);

extern RowMarkType select_rowmark_type(RangeTblEntry *rte,
					LockClauseStrength strength);

extern void mark_partial_aggref(Aggref *agg, AggSplit aggsplit);

extern Path *get_cheapest_fractional_path(RelOptInfo *rel,
							 double tuple_fraction);

extern Expr *expression_planner(Expr *expr);
extern Expr *preprocess_phv_expression(PlannerInfo *root, Expr *expr);

extern bool plan_cluster_use_sort(Oid tableOid, Oid indexOid);

extern void preprocess_rowmarks(PlannerInfo *root);

extern void set_rte_seq_info(PlannerInfo *root);

#ifdef __OPENTENBASE_C__
extern void fix_grouping_func_refs(PathTarget *target, List *targetlist);
extern bool set_distinct_aggrefs(Node *node, void *context);
#endif
extern int plan_create_index_workers(Oid tableOid, Oid indexOid);

#endif							/* PLANNER_H */
