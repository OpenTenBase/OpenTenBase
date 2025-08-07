/*-------------------------------------------------------------------------
 *
 * prep.h
 *	  prototypes for files in optimizer/prep/
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/prep.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREP_H
#define PREP_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#ifdef __OPENTENBASE_C__
extern bool nonunion_optimizer;
#endif

/*
 * prototypes for prepjointree.c
 */
extern void transform_MERGE_to_join(Query *parse);
extern void pull_up_sublinks(PlannerInfo *root);
extern void inline_set_returning_functions(PlannerInfo *root);
extern void pull_up_subqueries(PlannerInfo *root);
extern void flatten_simple_union_all(PlannerInfo *root);
extern void reduce_outer_joins(PlannerInfo *root);
extern Relids get_relids_in_jointree(Node *jtnode, bool include_joins);
extern Relids get_relids_for_join(PlannerInfo *root, int joinrelid);

/*
 * prototypes for prepqual.c
 */
extern Node *negate_clause(Node *node);
extern Expr *canonicalize_qual(Expr *qual, bool is_check);

/*
 * prototypes for preptlist.c
 */
extern void preprocess_targetlist(PlannerInfo *root);

extern PlanRowMark *get_plan_rowmark(List *rowmarks, Index rtindex);

/*
 * prototypes for prepunion.c
 */
extern RelOptInfo *plan_set_operations(PlannerInfo *root);
extern PlannerInfo *get_top_root(PlannerInfo *root);
extern bool is_multi_insert(PlannerInfo *root);
extern void preprocess_rowid_tlist(PlannerInfo *root, List **targetList);

#endif							/* PREP_H */
