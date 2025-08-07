/*-------------------------------------------------------------------------
 *
 * subselect.h
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

#ifdef __OPENTENBASE_C__
extern bool  enable_pullup_target_casewhen;
extern bool  enable_pullup_expr_agg;
extern bool  enable_pullup_expr_distinct;
extern bool  enable_pullup_expr_agg_update;
extern bool  enable_pullup_expr_agg_update_noqual;
extern bool	 enable_check_scalar_join;
extern int	pullup_target_casewhen_filter;
#endif

extern void SS_process_ctes(PlannerInfo *root);
#ifdef __OPENTENBASE__
extern JoinExpr *convert_ANY_sublink_to_join(PlannerInfo *root, SubLink *sublink,
							bool under_not, Relids available_rels, bool sclar_join, Relids all_rels);
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
extern JoinExpr *convert_EXPR_sublink_to_join(PlannerInfo *root, Node **jtlink1,
								OpExpr *expr, Relids available_rels, Node **filter,
								Node *allQuals, bool left_semi_scalar);
extern JoinExpr *convert_ALL_sublink_to_join(PlannerInfo *root, SubLink *sublink,
							   Relids available_rels);
extern bool check_or_exist_sublink_pullupable(PlannerInfo *root,Node *node);
extern bool check_or_exist_qual_pullupable(PlannerInfo *root, Node *node);
extern List * convert_OR_EXIST_sublink_to_join_recurse(PlannerInfo *root, Node *node, 
									Node **jtlink);
extern TargetEntry *convert_TargetList_sublink_to_join(PlannerInfo *root, 
													   TargetEntry *entry,
													   bool *is_pull_up,
													   List **sublink_case_list,
													   List **qual_case_list);
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
extern int	SS_assign_special_param(PlannerInfo *root);

#ifdef __OPENTENBASE__
extern bool has_correlation_in_funcexpr_rte(List *rtable);
#endif
#ifdef __OPENTENBASE_C__
extern Plan *SS_adjust_plan(Plan *plan, bool under_subplan);
extern void convert_ORCLAUSE_to_join(PlannerInfo *root, BoolExpr *or_clause,
									Node **jtlink1, Relids *available_rels1);
extern void convert_CASE_to_join(PlannerInfo *root, OpExpr *or_clause,
									Node **jtlink1, Relids *available_rels1);
extern void SS_set_subplan(PlannerInfo *root, Plan *plan);
extern bool check_rte_var_nullable(Node *node, int relid, bool is_nullable_side, bool *found);
#endif

/* ora_compatible */
extern void SS_process_sub_insert(PlannerInfo *root, Query *parse);

#endif							/* SUBSELECT_H */
