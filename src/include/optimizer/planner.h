/*-------------------------------------------------------------------------
 *
 * planner.h
 *      prototypes for planner.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/optimizer/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANNER_H
#define PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"


/* Hook for plugins to get control in planner() */
typedef PlannedStmt *(*planner_hook_type) (Query *parse,
                                           int cursorOptions,
                                           ParamListInfo boundParams);
extern PGDLLIMPORT planner_hook_type planner_hook;

/* Hook for plugins to get control when grouping_planner() plans upper rels */
typedef void (*create_upper_paths_hook_type) (PlannerInfo *root,
                                              UpperRelationKind stage,
                                              RelOptInfo *input_rel,
                                              RelOptInfo *output_rel);
extern PGDLLIMPORT create_upper_paths_hook_type create_upper_paths_hook;


extern PlannedStmt *planner(Query *parse, int cursorOptions,
        ParamListInfo boundParams);
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
#ifdef PGXC
extern void GetHashExecNodes(RelationLocInfo *rel_loc_info, 
                            ExecNodes **exec_nodes, const Expr *expr);
#endif

extern Expr *preprocess_phv_expression(PlannerInfo *root, Expr *expr);

extern bool plan_cluster_use_sort(Oid tableOid, Oid indexOid);

extern void preprocess_rowmarks(PlannerInfo *root);

#ifdef __OPENTENBASE__
extern bool olap_optimizer;
extern Size estimate_hashagg_entrysize(Path *path, const AggClauseCosts *agg_costs,
						   						double dNumGroups);
#endif


#endif                            /* PLANNER_H */
