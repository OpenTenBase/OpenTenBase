/*-------------------------------------------------------------------------
 *
 * subselect.h
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/optimizer/subselect.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SUBSELECT_H
#define SUBSELECT_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

#ifdef __OPENTENBASE__
extern bool  enable_pullup_subquery;
#endif

extern void SS_process_ctes(PlannerInfo *root);
#ifdef __OPENTENBASE__
extern JoinExpr *convert_ANY_sublink_to_join(PlannerInfo *root, SubLink *sublink,
                            Relids available_rels, bool under_not);
#else
extern JoinExpr *convert_ANY_sublink_to_join(PlannerInfo *root,
                            SubLink *sublink,
                            Relids available_rels);
#endif
extern JoinExpr *convert_EXISTS_sublink_to_join(PlannerInfo *root,
                               SubLink *sublink,
                               bool under_not,
                               Relids available_rels);
#ifdef __OPENTENBASE__
extern JoinExpr *convert_EXPR_sublink_to_join(PlannerInfo *root, OpExpr *expr,
							Relids available_rels, Node **filter);
extern JoinExpr *convert_ALL_sublink_to_join(PlannerInfo *root, SubLink *sublink,
                               Relids available_rels);
extern bool check_or_exist_sublink_pullupable(PlannerInfo *root,Node *node);
extern bool check_or_exist_qual_pullupable(PlannerInfo *root, Node *node);
extern List *convert_OR_EXIST_sublink_to_join_recurse(PlannerInfo *root, Node *node,
									Node **jtlink);
extern TargetEntry *convert_TargetList_sublink_to_join(PlannerInfo *root, TargetEntry *entry);
#endif
extern Node *SS_replace_correlation_vars(PlannerInfo *root, Node *expr);
extern Node *SS_process_sublinks(PlannerInfo *root, Node *expr, bool isQual);
extern void SS_identify_outer_params(PlannerInfo *root);
extern void SS_charge_for_initplans(PlannerInfo *root, RelOptInfo *final_rel);
extern void SS_attach_initplans(PlannerInfo *root, Plan *plan);
extern void SS_finalize_plan(PlannerInfo *root, Plan *plan);
extern Param *SS_make_initplan_output_param(PlannerInfo *root,
                              Oid resulttype, int32 resulttypmod,
                              Oid resultcollation);
extern void SS_make_initplan_from_plan(PlannerInfo *root,
                           PlannerInfo *subroot, Plan *plan,
                           Param *prm);
extern Param *assign_nestloop_param_var(PlannerInfo *root, Var *var);
extern Param *assign_nestloop_param_placeholdervar(PlannerInfo *root,
                                     PlaceHolderVar *phv);
extern int    SS_assign_special_param(PlannerInfo *root);

extern void SS_remote_attach_initplans(PlannerInfo *root, Plan *plan);
#ifdef __OPENTENBASE__
extern bool has_correlation_in_funcexpr_rte(List *rtable);
#endif

#endif							/* SUBSELECT_H */
