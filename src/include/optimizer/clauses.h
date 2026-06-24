/*-------------------------------------------------------------------------
 *
 * clauses.h
 *	  prototypes for clauses.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/clauses.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLAUSES_H
#define CLAUSES_H

#include "access/htup.h"
#include "nodes/relation.h"

#define is_opclause(clause)		((clause) != NULL && IsA(clause, OpExpr))
#define is_funcclause(clause)	((clause) != NULL && IsA(clause, FuncExpr))

#define IS_RELABELFORMAT_CAST(expr)	\
	(expr && \
	IsA(expr, RelabelType) &&	\
	(((RelabelType *) expr)->relabelformat == COERCE_EXPLICIT_CAST || \
	((RelabelType *) expr)->relabelformat == COERCE_IMPLICIT_CAST))

#define IS_FUNCFORMAT_CAST(expr) \
	(expr && \
	IsA(expr, FuncExpr) && \
	(((FuncExpr *) expr)->funcformat == COERCE_IMPLICIT_CAST || \
	((FuncExpr *) expr)->funcformat == COERCE_EXPLICIT_CAST))

typedef struct
{
	int			numWindowFuncs; /* total number of WindowFuncs found */
	Index		maxWinRef;		/* windowFuncs[] is indexed 0 .. maxWinRef */
	List	  **windowFuncs;	/* lists of WindowFuncs for each winref */
} WindowFuncLists;

/*
 * Information about a table-related object which could affect the safety of
 * parallel data modification on table.
 */
typedef struct safety_object
{
	Oid objid;			/* OID of object itself */
	Oid classid;		/* OID of its catalog */
	char proparallel;	/* parallel safety of the object */
} safety_object;

extern Expr *make_opclause(Oid opno, Oid opresulttype, bool opretset,
			  Expr *leftop, Expr *rightop,
			  Oid opcollid, Oid inputcollid);
extern Node *get_leftop(const Expr *clause);
extern Node *get_rightop(const Expr *clause);

extern bool not_clause(Node *clause);
extern Expr *make_notclause(Expr *notclause);
extern Expr *get_notclausearg(Expr *notclause);
extern Expr *make_null_test_clause(Expr *notclause, bool isnull);
extern Expr *make_notclause_with_null(Expr *clause);

extern bool or_clause(Node *clause);
extern Expr *make_orclause(List *orclauses);
#ifdef __OPENTENBASE__
extern bool OpExpr_clause(Node *clause);
#endif
extern bool and_clause(Node *clause);
extern Expr *make_andclause(List *andclauses);
extern Node *make_and_qual(Node *qual1, Node *qual2);
extern Expr *make_ands_explicit(List *andclauses);
extern List *make_ands_implicit(Expr *clause);

extern bool contain_agg_clause(Node *clause);
extern void get_agg_clause_costs(PlannerInfo *root, Node *clause,
					 AggSplit aggsplit, AggClauseCosts *costs);

extern bool contain_window_function(Node *clause);
extern WindowFuncLists *find_window_functions(Node *clause, Index maxWinRef);

extern double expression_returns_set_rows(Node *clause);

extern bool contain_subplans(Node *clause);

#ifdef _PG_ORCL_
extern bool contain_rownum(Node *clause);
#endif /* _PG_ORCL_ */

extern bool contain_user_defined_functions(Node *clause);
extern bool contain_user_defined_functions_warning(Node *clause);
extern bool contain_plpgsql_functions(Node *clause);
extern bool contain_mutable_functions(Node *clause);
extern bool contain_mutable_not_collect_funcs(Node *clause);
extern bool contain_volatile_functions(Node *clause);
extern bool contain_sql_functions(Node *clause);
extern bool contain_volatile_functions_not_nextval(Node *clause);
extern char max_parallel_hazard(Query *parse, bool *nextval_parallel_safe);
extern bool is_parallel_safe(PlannerInfo *root, Node *node);
extern bool is_parallel_dml_safe(Query *parse);
extern bool contain_nonstrict_functions(Node *clause);
extern bool contain_nonstrict_functions_with_checkagg(Node *clause);
extern bool contain_leaked_vars(Node *clause);

extern Relids find_nonnullable_rels(Node *clause);
extern List *find_nonnullable_vars(Node *clause);
extern List *find_forced_null_vars(Node *clause);
extern Var *find_forced_null_var(Node *clause);

extern bool is_pseudo_constant_clause(Node *clause);
extern bool is_pseudo_constant_clause_relids(Node *clause, Relids relids);
extern bool contain_opentenbaseLO_functions(Node *clause);
extern bool contain_opentenbaseLO_functions_checker(Oid func_id, void *context, Node *node);

extern int	NumRelids(Node *clause);

extern void CommuteOpExpr(OpExpr *clause);
extern void CommuteRowCompareExpr(RowCompareExpr *clause);

extern Node *eval_const_expressions(PlannerInfo *root, Node *node);
extern Node *eval_const_expressions_with_params(ParamListInfo boundParams, Node *node);
extern Node *estimate_const_expressions_with_params(ParamListInfo boundParams, Node *node);

extern Node *estimate_expression_value(PlannerInfo *root, Node *node);

extern Query *inline_set_returning_function(PlannerInfo *root,
							  RangeTblEntry *rte);

extern void inline_target_functions(PlannerInfo *root);

extern bool is_parallel_allowed_for_modify(Query *parse);

extern List *target_rel_parallel_hazard(Oid relOid, bool findall,
										char max_interesting,
										char *max_hazard);

extern List *expand_function_arguments(List *args, Oid result_type,
                                                HeapTuple func_tuple);


#ifdef __OPENTENBASE__
extern Node *substitute_sublink_with_node(Node *expr, SubLink *sublink,
										 Node *node);

extern bool find_sublink_walker(Node *node, List **list);

extern bool find_estore_pushdown_clause(PlannerInfo* root, Expr* clause);

extern Node *replace_distribkey_func(Node *node);

extern Node *replace_sql_value_function_mutator(Node *node);

extern bool unsupport_quals_for_global_index(Node *clause);

extern bool contain_prior_expr(Node *clause);
extern bool contain_cn_expr(Node *clause);
extern bool contain_plpgsql_local_functions(Node *clause);
extern Node *connectby_split_jquals(PlannerInfo *root, Node *node, bool single);
extern bool contain_user_defined_functions_checker(Oid func_id, void *context, Node *node);
extern bool contain_plpgsql_functions_checker(Oid func_id, void *context, Node *node);
extern bool contain_pullup_range_table(Node *node);
extern bool contain_unshippable_functions(Node *clause);
extern bool contain_dml(Node *node);

extern bool contain_nonstrict_functions_before_sublink(Node *clause);

#endif

#endif							/* CLAUSES_H */
