/*-------------------------------------------------------------------------
 *
 * clauses.c
 *	  routines to manipulate qualification clauses
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/clauses.c
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Nov 3, 1994		clause.c and clauses.c combined
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_language.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/functions.h"
#include "executor/execExpr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/execnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "catalog/partition.h"
#include "rewrite/rewriteHandler.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "rewrite/rewriteManip.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#ifdef __OPENTENBASE__
#include "catalog/pgxc_class.h"
#include "commands/proclang.h"
#include "catalog/pg_inherits_fn.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "tcop/pquery.h"
#endif

#ifdef _PG_ORCL_
#include "catalog/pg_proc_fn.h"
#endif

typedef struct
{
	PlannerInfo *root;
	AggSplit	aggsplit;
	AggClauseCosts *costs;
} get_agg_clause_costs_context;

typedef struct
{
	ParamListInfo boundParams;
	PlannerInfo *root;
	List	   *active_fns;
	Node	   *case_val;
	bool		estimate;
} eval_const_expressions_context;

typedef struct
{
	int			nargs;
	List	   *args;
	int		   *usecounts;
} substitute_actual_parameters_context;

typedef struct
{
	int			nargs;
	List	   *args;
	int			sublevels_up;
} substitute_actual_srf_parameters_context;

typedef struct
{
	char	   *proname;
	char	   *prosrc;
} inline_error_callback_arg;

typedef struct
{
	char		max_hazard;		/* worst proparallel hazard found so far */
	char		max_interesting;	/* worst proparallel hazard of interest */
	List	   *safe_param_ids; /* PARAM_EXEC Param IDs to treat as safe */
	bool		check_all;	/* whether collect all the unsafe/restricted objects */
    bool        nextval_parallel_safe; /* whether nextval function is parallel safe */
	List	   *objects;	/* parallel unsafe/restricted objects */
} max_parallel_hazard_context;

#ifdef __OPENTENBASE__
typedef struct
{
	PlannerInfo	*root;
	Query		*curr_query;
} inline_target_function_context;

typedef struct
{
	SubLink   *sublink;
	Node      *node;
} substitute_sublink_with_node_context;

typedef struct
{
	bool check_agg;
	bool stop_before_sublink;
} not_strict_context;

typedef struct
{
	bool	is_or_cond;
	bool	replace_singleton; /* If find singleton arg and replace it */
} connectby_split_quals_context;

typedef struct
{
	check_function_callback checker;
	bool warning;
} contain_check_functions_context;
#endif

static bool contain_agg_clause_walker(Node *node, void *context);
static bool get_agg_clause_costs_walker(Node *node,
							get_agg_clause_costs_context *context);
static bool find_window_functions_walker(Node *node, WindowFuncLists *lists);
static bool contain_subplans_walker(Node *node, void *context);
static bool contain_mutable_functions_walker(Node *node, void *context);
static bool contain_volatile_functions_walker(Node *node, void *context);
static bool contain_volatile_functions_not_nextval_walker(Node *node, void *context);
static bool max_parallel_hazard_walker(Node *node,
						   max_parallel_hazard_context *context);
static bool target_rel_parallel_hazard_recurse(Relation relation,
											   max_parallel_hazard_context *context,
											   bool is_partition,
											   bool check_column_default);
static bool target_rel_trigger_parallel_hazard(Relation rel,
											   max_parallel_hazard_context *context);
static bool index_expr_parallel_hazard(Relation index_rel,
									   List *ii_Expressions,
									   List *ii_Predicate,
									   max_parallel_hazard_context *context);
static bool target_rel_index_parallel_hazard(Relation rel,
											 max_parallel_hazard_context *context);
static bool target_rel_domain_parallel_hazard(Oid typid,
											  max_parallel_hazard_context *context);
static bool target_rel_chk_constr_parallel_hazard(Relation rel,
												  max_parallel_hazard_context *context);
static bool contain_nonstrict_functions_walker(Node *node, void *context);

#ifdef _PG_ORCL_
static bool contain_rownum_walker(Node *node, void *context);
#endif

static bool contain_context_dependent_node(Node *clause);
static bool contain_context_dependent_node_walker(Node *node, int *flags);
static bool contain_leaked_vars_walker(Node *node, void *context);
static bool contain_plpgsql_local_functions_checker(Oid func_id, void *context, Node *node);
static bool contain_check_functions_walker(Node *node, contain_check_functions_context *context);
static Relids find_nonnullable_rels_walker(Node *node, bool top_level);
static List *find_nonnullable_vars_walker(Node *node, bool top_level);
static bool is_strict_saop(ScalarArrayOpExpr *expr, bool falseOK);
static Node *eval_const_expressions_mutator(Node *node,
							   eval_const_expressions_context *context);
static List *simplify_or_arguments(List *args,
					  eval_const_expressions_context *context,
					  bool *haveNull, bool *forceTrue);
static List *simplify_and_arguments(List *args,
					   eval_const_expressions_context *context,
					   bool *haveNull, bool *forceFalse);
static Node *simplify_boolean_equality(Oid opno, List *args);
static Expr *simplify_function(Oid funcid,
				  Oid result_type, int32 result_typmod,
				  Oid result_collid, Oid input_collid, List **args_p,
				  bool funcvariadic, bool process_args, bool allow_non_const,
				  eval_const_expressions_context *context
#ifdef _PG_ORCL_
				  , List *with_funcs, int with_funcid
#endif
				  );
static List *reorder_function_arguments(List *args, HeapTuple func_tuple);
static List *add_function_defaults(List *args, HeapTuple func_tuple);
static List *fetch_function_defaults(HeapTuple func_tuple);
static void recheck_cast_function_args(List *args, Oid result_type,
						   HeapTuple func_tuple);
static Expr *evaluate_function(Oid funcid, Oid result_type, int32 result_typmod,
				  Oid result_collid, Oid input_collid, List *args,
				  bool funcvariadic,
				  HeapTuple func_tuple,
				  eval_const_expressions_context *context
#ifdef _PG_ORCL_
				  , List *withfunc, int with_funcid
#endif
				  );
static Expr *inline_function(Oid funcid, Oid result_type, Oid result_collid,
				Oid input_collid, List *args,
				bool funcvariadic,
				HeapTuple func_tuple,
				eval_const_expressions_context *context);
static Node *substitute_actual_parameters(Node *expr, int nargs, List *args,
							 int *usecounts);
static Node *substitute_actual_parameters_mutator(Node *node,
									 substitute_actual_parameters_context *context);
static void sql_inline_error_callback(void *arg);
static Expr *evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
			  Oid result_collation);
static Query *substitute_actual_srf_parameters(Query *expr,
								 int nargs, List *args);
static Node *substitute_actual_srf_parameters_mutator(Node *node,
										 substitute_actual_srf_parameters_context *context);
static bool tlist_matches_coltypelist(List *tlist, List *coltypelist);

static bool can_operator_be_pushed_down(Oid opno);
static bool can_operator_be_pushed_down_eq(Oid opno);
static bool is_operator_external_param_const(PlannerInfo* root, Node* node);
static bool is_operator_simple_in_any(PlannerInfo* root, Node* node);
static bool is_var_node(Node* node);
static bool max_parallel_hazard_test(char proparallel, max_parallel_hazard_context *context);
static safety_object *make_safety_object(Oid objid, Oid classid, char proparallel);
static char max_parallel_dml_hazard(Query *parse, max_parallel_hazard_context *context);

#ifdef __OPENTENBASE__
static Node *
substitute_sublink_with_node_mutator(Node *node,
									substitute_sublink_with_node_context *context);
#endif

	/*****************************************************************************
 *		OPERATOR clause functions
 *****************************************************************************/

	/*
 * make_opclause
 *	  Creates an operator clause given its operator info, left operand
 *	  and right operand (pass NULL to create single-operand clause),
 *	  and collation info.
 */
	Expr *make_opclause(Oid opno, Oid opresulttype, bool opretset,
						Expr *leftop, Expr *rightop,
						Oid opcollid, Oid inputcollid)
{
	OpExpr	   *expr = makeNode(OpExpr);

	expr->opno = opno;
	expr->opfuncid = InvalidOid;
	expr->opresulttype = opresulttype;
	expr->opretset = opretset;
	expr->opcollid = opcollid;
	expr->inputcollid = inputcollid;
	if (rightop)
		expr->args = list_make2(leftop, rightop);
	else
		expr->args = list_make1(leftop);
	expr->location = -1;
	return (Expr *) expr;
}

/*
 * get_leftop
 *
 * Returns the left operand of a clause of the form (op expr expr)
 *		or (op expr)
 */
Node *
get_leftop(const Expr *clause)
{
	const OpExpr *expr = (const OpExpr *) clause;

	if (expr->args != NIL)
		return linitial(expr->args);
	else
		return NULL;
}

/*
 * get_rightop
 *
 * Returns the right operand in a clause of the form (op expr expr).
 * NB: result will be NULL if applied to a unary op clause.
 */
Node *
get_rightop(const Expr *clause)
{
	const OpExpr *expr = (const OpExpr *) clause;

	if (list_length(expr->args) >= 2)
		return lsecond(expr->args);
	else
		return NULL;
}

/*****************************************************************************
 *		NOT clause functions
 *****************************************************************************/

/*
 * not_clause
 *
 * Returns t iff this is a 'not' clause: (NOT expr).
 */
bool
not_clause(Node *clause)
{
	return (clause != NULL &&
			IsA(clause, BoolExpr) &&
			((BoolExpr *) clause)->boolop == NOT_EXPR);
}

/*
 * make_notclause
 *
 * Create a 'not' clause given the expression to be negated.
 */
Expr *
make_notclause(Expr *notclause)
{
	BoolExpr   *expr = makeNode(BoolExpr);

	expr->boolop = NOT_EXPR;
	expr->args = list_make1(notclause);
	expr->location = -1;
	return (Expr *) expr;
}

/*
 * make_null_test_clause
 *
 * Create a 'null test' clause given the expression to be negated.
 */
Expr *
make_null_test_clause(Expr *notclause, bool isnull)
{
	NullTest   *expr = makeNode(NullTest);
	expr->arg = notclause;

	if (isnull)
		expr->nulltesttype = IS_NULL;
	else
		expr->nulltesttype = IS_NOT_NULL;

	expr->argisrow = false;
	expr->location = -1;
	return (Expr *)expr;
}

/*
 * make_notclause_with_null
 *
 * Create a 'not' clause given the expression to be negated with null test.
 */
Expr *
make_notclause_with_null(Expr *clause)
{
	return make_orclause(list_make2(make_notclause((Expr *)copyObject(clause)),
									make_null_test_clause(copyObject(clause), true)));
}

/*
 * get_notclausearg
 *
 * Retrieve the clause within a 'not' clause
 */
Expr *
get_notclausearg(Expr *notclause)
{
	return linitial(((BoolExpr *) notclause)->args);
}

/*****************************************************************************
 *		OR clause functions
 *****************************************************************************/

/*
 * or_clause
 *
 * Returns t iff the clause is an 'or' clause: (OR { expr }).
 */
bool
or_clause(Node *clause)
{
	return (clause != NULL &&
			IsA(clause, BoolExpr) &&
			((BoolExpr *) clause)->boolop == OR_EXPR);
}

/*
 * make_orclause
 *
 * Creates an 'or' clause given a list of its subclauses.
 */
Expr *
make_orclause(List *orclauses)
{
	BoolExpr   *expr = makeNode(BoolExpr);

	expr->boolop = OR_EXPR;
	expr->args = orclauses;
	expr->location = -1;
	return (Expr *) expr;
}

#ifdef __OPENTENBASE__
bool
OpExpr_clause(Node *clause)
{
	return (clause != NULL && IsA(clause, OpExpr));	
}
#endif
/*****************************************************************************
 *		AND clause functions
 *****************************************************************************/


/*
 * and_clause
 *
 * Returns t iff its argument is an 'and' clause: (AND { expr }).
 */
bool
and_clause(Node *clause)
{
	return (clause != NULL &&
			IsA(clause, BoolExpr) &&
			((BoolExpr *) clause)->boolop == AND_EXPR);
}

/*
 * make_andclause
 *
 * Creates an 'and' clause given a list of its subclauses.
 */
Expr *
make_andclause(List *andclauses)
{
	BoolExpr   *expr = makeNode(BoolExpr);

	expr->boolop = AND_EXPR;
	expr->args = andclauses;
	expr->location = -1;
	return (Expr *) expr;
}

/*
 * make_and_qual
 *
 * Variant of make_andclause for ANDing two qual conditions together.
 * Qual conditions have the property that a NULL nodetree is interpreted
 * as 'true'.
 *
 * NB: this makes no attempt to preserve AND/OR flatness; so it should not
 * be used on a qual that has already been run through prepqual.c.
 */
Node *
make_and_qual(Node *qual1, Node *qual2)
{
	if (qual1 == NULL)
		return qual2;
	if (qual2 == NULL)
		return qual1;
	return (Node *) make_andclause(list_make2(qual1, qual2));
}

/*
 * The planner frequently prefers to represent qualification expressions
 * as lists of boolean expressions with implicit AND semantics.
 *
 * These functions convert between an AND-semantics expression list and the
 * ordinary representation of a boolean expression.
 *
 * Note that an empty list is considered equivalent to TRUE.
 */
Expr *
make_ands_explicit(List *andclauses)
{
	if (andclauses == NIL)
		return (Expr *) makeBoolConst(true, false);
	else if (list_length(andclauses) == 1)
		return (Expr *) linitial(andclauses);
	else
		return make_andclause(andclauses);
}

List *
make_ands_implicit(Expr *clause)
{
	/*
	 * NB: because the parser sets the qual field to NULL in a query that has
	 * no WHERE clause, we must consider a NULL input clause as TRUE, even
	 * though one might more reasonably think it FALSE.  Grumble. If this
	 * causes trouble, consider changing the parser's behavior.
	 */
	if (clause == NULL)
		return NIL;				/* NULL -> NIL list == TRUE */
	else if (and_clause((Node *) clause))
		return ((BoolExpr *) clause)->args;
	else if (IsA(clause, Const) &&
			 !((Const *) clause)->constisnull &&
			 DatumGetBool(((Const *) clause)->constvalue))
		return NIL;				/* constant TRUE input -> NIL list */
	else
		return list_make1(clause);
}


/*****************************************************************************
 *		Aggregate-function clause manipulation
 *****************************************************************************/

/*
 * contain_agg_clause
 *	  Recursively search for Aggref/GroupingFunc nodes within a clause.
 *
 *	  Returns true if any aggregate found.
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans, or in contexts where it's known there
 * are no subqueries.  There mustn't be outer-aggregate references either.
 *
 * (If you want something like this but able to deal with subqueries,
 * see rewriteManip.c's contain_aggs_of_level().)
 */
bool
contain_agg_clause(Node *clause)
{
	return contain_agg_clause_walker(clause, NULL);
}

static bool
contain_agg_clause_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Assert(((Aggref *) node)->agglevelsup == 0);
		return true;			/* abort the tree traversal and return true */
	}
	if (IsA(node, GroupingFunc))
	{
		Assert(((GroupingFunc *) node)->agglevelsup == 0);
		return true;			/* abort the tree traversal and return true */
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, contain_agg_clause_walker, context);
}

/*
 * get_agg_clause_costs
 *	  Recursively find the Aggref nodes in an expression tree, and
 *	  accumulate cost information about them.
 *
 * 'aggsplit' tells us the expected partial-aggregation mode, which affects
 * the cost estimates.
 *
 * NOTE that the counts/costs are ADDED to those already in *costs ... so
 * the caller is responsible for zeroing the struct initially.
 *
 * We count the nodes, estimate their execution costs, and estimate the total
 * space needed for their transition state values if all are evaluated in
 * parallel (as would be done in a HashAgg plan).  Also, we check whether
 * partial aggregation is feasible.  See AggClauseCosts for the exact set
 * of statistics collected.
 *
 * In addition, we mark Aggref nodes with the correct aggtranstype, so
 * that that doesn't need to be done repeatedly.  (That makes this function's
 * name a bit of a misnomer.)
 *
 * This does not descend into subqueries, and so should be used only after
 * reduction of sublinks to subplans, or in contexts where it's known there
 * are no subqueries.  There mustn't be outer-aggregate references either.
 */
void
get_agg_clause_costs(PlannerInfo *root, Node *clause, AggSplit aggsplit,
					 AggClauseCosts *costs)
{
	get_agg_clause_costs_context context;

	context.root = root;
	context.aggsplit = aggsplit;
	context.costs = costs;
	(void) get_agg_clause_costs_walker(clause, &context);
}

static bool
get_agg_clause_costs_walker(Node *node, get_agg_clause_costs_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref	   *aggref = (Aggref *) node;
		AggClauseCosts *costs = context->costs;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		Oid			aggtransfn;
		Oid			aggfinalfn;
		Oid			aggcombinefn;
		Oid			aggserialfn;
		Oid			aggdeserialfn;
		Oid			aggtranstype;
		int32		aggtransspace;
		QualCost	argcosts;

		Assert(aggref->agglevelsup == 0);

		/*
		 * Fetch info about aggregate from pg_aggregate.  Note it's correct to
		 * ignore the moving-aggregate variant, since what we're concerned
		 * with here is aggregates not window functions.
		 */
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
		aggtransfn = aggform->aggtransfn;
		aggfinalfn = aggform->aggfinalfn;
		aggcombinefn = aggform->aggcombinefn;
		aggserialfn = aggform->aggserialfn;
		aggdeserialfn = aggform->aggdeserialfn;
		aggtranstype = aggform->aggtranstype;
		aggtransspace = aggform->aggtransspace;
		ReleaseSysCache(aggTuple);

		/*
		 * Resolve the possibly-polymorphic aggregate transition type, unless
		 * already done in a previous pass over the expression.
		 */
		if (OidIsValid(aggref->aggtranstype))
			aggtranstype = aggref->aggtranstype;
		else
		{
			Oid			inputTypes[FUNC_MAX_ARGS];
			int			numArguments;

			/* extract argument types (ignoring any ORDER BY expressions) */
			numArguments = get_aggregate_argtypes(aggref, inputTypes);

			/* resolve actual type of transition state, if polymorphic */
			aggtranstype = resolve_aggregate_transtype(aggref->aggfnoid,
													   aggtranstype,
													   inputTypes,
													   numArguments);
			aggref->aggtranstype = aggtranstype;
		}

		/*
		 * Count it, and check for cases requiring ordered input.  Note that
		 * ordered-set aggs always have nonempty aggorder.  Any ordered-input
		 * case also defeats partial aggregation.
		 */
		costs->numAggs++;
#ifdef __OPENTENBASE_C__
		if (aggref->aggorder != NIL)
#else
		if (aggref->aggorder != NIL || aggref->aggdistinct != NIL)
#endif
		{
			costs->numOrderedAggs++;
			costs->hasNonPartial = true;
		}
#ifdef __OPENTENBASE_C__
		else if (aggref->aggdistinct != NIL)
		{
			/* currently we only support count distinct */
			if (distinct_optimizer && (aggref->aggfnoid == ANYCOUNTOID || aggref->aggfnoid == ORA_ANYCOUNTOID))
			{
				/* replace with distinct related functions except finalfn */
				HeapTuple distTuple = SearchSysCache1(AGGFNOID,
										ObjectIdGetDatum(DISTINCT_FUNC_OID));
				if (!HeapTupleIsValid(distTuple))
					elog(ERROR, "cache lookup failed for distinct aggregate %u",
						 DISTINCT_FUNC_OID);
				aggform = (Form_pg_aggregate) GETSTRUCT(distTuple);
				aggtransfn = aggform->aggtransfn;
				aggfinalfn = aggform->aggfinalfn;
				aggcombinefn = aggform->aggcombinefn;
				aggserialfn = aggform->aggserialfn;
				aggdeserialfn = aggform->aggdeserialfn;
				aggtranstype = aggform->aggtranstype;
				aggtransspace = aggform->aggtransspace;
				ReleaseSysCache(distTuple);
				/* aggref->aggtranstype will be used in planning */
				aggref->aggtranstype = aggtranstype;
				aggref->distinct_args = aggref->args;
			}
			else
			{
				costs->numOrderedAggs++;
				costs->hasNonPartial = true;
			}
		}
#endif

		/*
		 * Check whether partial aggregation is feasible, unless we already
		 * found out that we can't do it.
		 */
		if (!costs->hasNonPartial)
		{
			/*
			 * If there is no combine function, then partial aggregation is
			 * not possible.
			 */
			if (!OidIsValid(aggcombinefn))
				costs->hasNonPartial = true;

			/*
			 * If we have any aggs with transtype INTERNAL then we must check
			 * whether they have serialization/deserialization functions; if
			 * not, we can't serialize partial-aggregation results.
			 */
			else if (aggtranstype == INTERNALOID &&
					 (!OidIsValid(aggserialfn) || !OidIsValid(aggdeserialfn)))
				costs->hasNonSerial = true;
		}

		/*
		 * Add the appropriate component function execution costs to
		 * appropriate totals.
		 */
		if (DO_AGGSPLIT_COMBINE(context->aggsplit))
		{
			/* charge for combining previously aggregated states */
			costs->transCost.per_tuple += get_func_cost(aggcombinefn) * cpu_operator_cost;
		}
		else
			costs->transCost.per_tuple += get_func_cost(aggtransfn) * cpu_operator_cost;
		if (DO_AGGSPLIT_DESERIALIZE(context->aggsplit) &&
			OidIsValid(aggdeserialfn))
			costs->transCost.per_tuple += get_func_cost(aggdeserialfn) * cpu_operator_cost;
		if (DO_AGGSPLIT_SERIALIZE(context->aggsplit) &&
			OidIsValid(aggserialfn))
			costs->finalCost += get_func_cost(aggserialfn) * cpu_operator_cost;
		if (!DO_AGGSPLIT_SKIPFINAL(context->aggsplit) &&
			OidIsValid(aggfinalfn))
			costs->finalCost += get_func_cost(aggfinalfn) * cpu_operator_cost;

		/*
		 * These costs are incurred only by the initial aggregate node, so we
		 * mustn't include them again at upper levels.
		 */
		if (!DO_AGGSPLIT_COMBINE(context->aggsplit))
		{
			/* add the input expressions' cost to per-input-row costs */
			cost_qual_eval_node(&argcosts, (Node *) aggref->args, context->root);
			costs->transCost.startup += argcosts.startup;
			costs->transCost.per_tuple += argcosts.per_tuple;

			/*
			 * Add any filter's cost to per-input-row costs.
			 *
			 * XXX Ideally we should reduce input expression costs according
			 * to filter selectivity, but it's not clear it's worth the
			 * trouble.
			 */
			if (aggref->aggfilter)
			{
				cost_qual_eval_node(&argcosts, (Node *) aggref->aggfilter,
									context->root);
				costs->transCost.startup += argcosts.startup;
				costs->transCost.per_tuple += argcosts.per_tuple;
			}
		}

		/*
		 * If there are direct arguments, treat their evaluation cost like the
		 * cost of the finalfn.
		 */
		if (aggref->aggdirectargs)
		{
			cost_qual_eval_node(&argcosts, (Node *) aggref->aggdirectargs,
								context->root);
			costs->transCost.startup += argcosts.startup;
			costs->finalCost += argcosts.per_tuple;
		}

		/*
		 * If the transition type is pass-by-value then it doesn't add
		 * anything to the required size of the hashtable.  If it is
		 * pass-by-reference then we have to add the estimated size of the
		 * value itself, plus palloc overhead.
		 */
		if (!get_typbyval(aggtranstype))
		{
			int32		avgwidth;

			/* Use average width if aggregate definition gave one */
			if (aggtransspace > 0)
				avgwidth = aggtransspace;
			else if (aggtransfn == F_ARRAY_APPEND)
			{
				/*
				 * If the transition function is array_append(), it'll use an
				 * expanded array as transvalue, which will occupy at least
				 * ALLOCSET_SMALL_INITSIZE and possibly more.  Use that as the
				 * estimate for lack of a better idea.
				 */
				avgwidth = ALLOCSET_SMALL_INITSIZE;
			}
			else
			{
				/*
				 * If transition state is of same type as first aggregated
				 * input, assume it's the same typmod (same width) as well.
				 * This works for cases like MAX/MIN and is probably somewhat
				 * reasonable otherwise.
				 */
				int32		aggtranstypmod = -1;

				if (aggref->args)
				{
					TargetEntry *tle = (TargetEntry *) linitial(aggref->args);

					if (aggtranstype == exprType((Node *) tle->expr))
						aggtranstypmod = exprTypmod((Node *) tle->expr);
				}

				avgwidth = get_typavgwidth(aggtranstype, aggtranstypmod);
			}

			avgwidth = MAXALIGN(avgwidth);
			costs->transitionSpace += avgwidth + 2 * sizeof(void *);
		}
		else if (aggtranstype == INTERNALOID)
		{
			/*
			 * INTERNAL transition type is a special case: although INTERNAL
			 * is pass-by-value, it's almost certainly being used as a pointer
			 * to some large data structure.  The aggregate definition can
			 * provide an estimate of the size.  If it doesn't, then we assume
			 * ALLOCSET_DEFAULT_INITSIZE, which is a good guess if the data is
			 * being kept in a private memory context, as is done by
			 * array_agg() for instance.
			 */
			if (aggtransspace > 0)
				costs->transitionSpace += aggtransspace;
			else
				costs->transitionSpace += ALLOCSET_DEFAULT_INITSIZE;
		}

		/*
		 * We assume that the parser checked that there are no aggregates (of
		 * this level anyway) in the aggregated arguments, direct arguments,
		 * or filter clause.  Hence, we need not recurse into any of them.
		 */
		return false;
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, get_agg_clause_costs_walker,
								  (void *) context);
}


/*****************************************************************************
 *		Window-function clause manipulation
 *****************************************************************************/

/*
 * contain_window_function
 *	  Recursively search for WindowFunc nodes within a clause.
 *
 * Since window functions don't have level fields, but are hard-wired to
 * be associated with the current query level, this is just the same as
 * rewriteManip.c's function.
 */
bool
contain_window_function(Node *clause)
{
	return contain_windowfuncs(clause);
}

/*
 * find_window_functions
 *	  Locate all the WindowFunc nodes in an expression tree, and organize
 *	  them by winref ID number.
 *
 * Caller must provide an upper bound on the winref IDs expected in the tree.
 */
WindowFuncLists *
find_window_functions(Node *clause, Index maxWinRef)
{
	WindowFuncLists *lists = palloc(sizeof(WindowFuncLists));

	lists->numWindowFuncs = 0;
	lists->maxWinRef = maxWinRef;
	lists->windowFuncs = (List **) palloc0((maxWinRef + 1) * sizeof(List *));
	(void) find_window_functions_walker(clause, lists);
	return lists;
}

static bool
find_window_functions_walker(Node *node, WindowFuncLists *lists)
{
	if (node == NULL)
		return false;
	if (IsA(node, WindowFunc))
	{
		WindowFunc *wfunc = (WindowFunc *) node;

		/* winref is unsigned, so one-sided test is OK */
		if (wfunc->winref > lists->maxWinRef)
			elog(ERROR, "WindowFunc contains out-of-range winref %u",
				 wfunc->winref);
		/* eliminate duplicates, so that we avoid repeated computation */
		if (!list_member(lists->windowFuncs[wfunc->winref], wfunc))
		{
			lists->windowFuncs[wfunc->winref] =
				lappend(lists->windowFuncs[wfunc->winref], wfunc);
			lists->numWindowFuncs++;
		}

		/*
		 * We assume that the parser checked that there are no window
		 * functions in the arguments or filter clause.  Hence, we need not
		 * recurse into them.  (If either the parser or the planner screws up
		 * on this point, the executor will still catch it; see ExecInitExpr.)
		 */
		return false;
	}
	Assert(!IsA(node, SubLink));
	return expression_tree_walker(node, find_window_functions_walker,
								  (void *) lists);
}


/*****************************************************************************
 *		Support for expressions returning sets
 *****************************************************************************/

/*
 * expression_returns_set_rows
 *	  Estimate the number of rows returned by a set-returning expression.
 *	  The result is 1 if it's not a set-returning expression.
 *
 * We should only examine the top-level function or operator; it used to be
 * appropriate to recurse, but not anymore.  (Even if there are more SRFs in
 * the function's inputs, their multipliers are accounted for separately.)
 *
 * Note: keep this in sync with expression_returns_set() in nodes/nodeFuncs.c.
 */
double
expression_returns_set_rows(Node *clause)
{
	if (clause == NULL)
		return 1.0;
	if (IsA(clause, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) clause;

#ifdef _PG_ORCL_
		if (expr->withfuncnsp != NULL)
		{
			Assert(expr->funcid != InvalidOid);
			if (expr->funcretset)
				return clamp_row_est(get_func_rows_withfuncs(expr->withfuncnsp,
															 expr->withfuncid));
		}
		else /* Normal case */
#endif
		if (expr->funcretset)
			return clamp_row_est(get_func_rows(expr->funcid));
	}
	if (IsA(clause, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) clause;

		if (expr->opretset)
		{
			set_opfuncid(expr);
			return clamp_row_est(get_func_rows(expr->opfuncid));
		}
	}
	return 1.0;
}


/*****************************************************************************
 *		Subplan clause manipulation
 *****************************************************************************/

/*
 * contain_subplans
 *	  Recursively search for subplan nodes within a clause.
 *
 * If we see a SubLink node, we will return TRUE.  This is only possible if
 * the expression tree hasn't yet been transformed by subselect.c.  We do not
 * know whether the node will produce a true subplan or just an initplan,
 * but we make the conservative assumption that it will be a subplan.
 *
 * Returns true if any subplan found.
 */
bool
contain_subplans(Node *clause)
{
	return contain_subplans_walker(clause, NULL);
}

static bool
contain_subplans_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, SubPlan) ||
		IsA(node, AlternativeSubPlan) ||
		IsA(node, SubLink))
		return true;			/* abort the tree traversal and return true */
	return expression_tree_walker(node, contain_subplans_walker, context);
}

#ifdef _PG_ORCL_
bool contain_rownum(Node *clause)
{
	if (!ORA_MODE)
		return false;
	return contain_rownum_walker(clause, NULL);
}

static bool contain_rownum_walker(Node *node, void *context)
{
	if(node == NULL)
		return false;
	if(IsA(node, RownumExpr))
		return true;
	return expression_tree_walker(node, contain_rownum_walker, context);
}
#endif /* _PG_ORCL_ */


/*****************************************************************************
 *		Check clauses for user defined functions
 *****************************************************************************/

bool
contain_user_defined_functions(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_user_defined_functions_checker;
	context.warning = false;

	return contain_check_functions_walker(clause, &context);
}

bool
contain_user_defined_functions_warning(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_user_defined_functions_checker;
	context.warning = true;

	return contain_check_functions_walker(clause, &context);
}

bool
contain_user_defined_functions_checker(Oid func_id, void *context, Node *node)
{
	bool warning = context ? *(bool *) context : false;
	bool pullup;

	
	pullup = func_is_pullup(func_id, node);
 
	if (warning && pullup)
		elog(WARNING, "Detect function that need to be executed in CN: %s",
			 get_func_name(func_id));

	return pullup;
}

bool
contain_plpgsql_functions(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_plpgsql_functions_checker;
	context.warning = false;

	return contain_check_functions_walker(clause, &context);
}

bool
contain_plpgsql_functions_checker(Oid func_id, void *context, Node *node)
{
	switch (func_id)
	{
		case 4217: /* opentenbase_ora_collect_func */
		case 4216: /* opentenbase_ora_collect_exists */
		case 4215: /* opentenbase_ora_collect_proc */
		case 4219: /* opentenbase_ora_collect_func_text */
		case 4220: /* opentenbase_ora_collect_exists_text */
		case 4221: /* opentenbase_ora_collect_proc_text */
		case 4222: /* opentenbase_ora_collect_get_subscript */
		case 4223: /* opentenbase_ora_collect_get_subscript */
		case 4235: /* opentenbase_ora_collect_get_subscript */
		case 4236: /* opentenbase_ora_collect_func */
		case 4237: /* opentenbase_ora_collect_exists */
		case 4238: /* opentenbase_ora_collect_proc */
		case 4239: /* opentenbase_ora_collect_get_subscript */
		case 4240: /* opentenbase_ora_collect_func_text */
		case 4241: /* opentenbase_ora_collect_exists_text */
		case 4242: /* opentenbase_ora_collect_proc_text */
		case 4243: /* opentenbase_ora_collect_get_subscript */
		{
			return true;
		}
		default:
		{
			break;
		}
	}
	return false;
}

bool
contain_plpgsql_local_functions(Node *clause)
{
	contain_check_functions_context	context;

	context.checker = &contain_plpgsql_local_functions_checker;
	context.warning = false;

	return contain_check_functions_walker(clause, &context);
}

static bool
contain_sql_function_checker(Oid func_id, void *context, Node *node)
{
	if (OidIsValid(func_id) && get_func_lang(func_id) == SQLlanguageId)
		return true;

	return false;
}

bool
contain_sql_functions(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_sql_function_checker;
	context.warning = false;

	return contain_check_functions_walker(clause, &context);
}

static bool
contain_plpgsql_local_functions_checker(Oid func_id, void *context, Node *node)
{
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;
		HeapTuple	tp;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid local function internal ID");

		tp = GetWithFunctionTupleById(expr->withfuncnsp, expr->withfuncid);

		if (((Form_pg_proc) GETSTRUCT(tp))->pronamespace != InvalidOid)
			return true;
	}

	return false;
}

static bool
contain_check_functions_walker(Node *node, contain_check_functions_context *context)
{
	check_function_callback checker = context->checker;
	bool warning = context->warning;

	if (node == NULL)
		return false;

	if (check_functions_in_node(node, checker, &warning))
		return true;

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 contain_check_functions_walker,
								 context, QTW_IGNORE_DUMMY_RTE);
	}
	return expression_tree_walker(node, contain_check_functions_walker,
								  context);
}

/*****************************************************************************
 *		Check clauses for mutable functions
 *****************************************************************************/

/*
 * contain_mutable_functions
 *	  Recursively search for mutable functions within a clause.
 *
 * Returns true if any mutable function (or operator implemented by a
 * mutable function) is found.  This test is needed so that we don't
 * mistakenly think that something like "WHERE random() < 0.5" can be treated
 * as a constant qualification.
 *
 * We will recursively look into Query nodes (i.e., SubLink sub-selects)
 * but not into SubPlans.  See comments for contain_volatile_functions().
 */
bool
contain_mutable_functions(Node *clause)
{
	return contain_mutable_functions_walker(clause, NULL);
}

bool
contain_mutable_not_collect_funcs(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_plpgsql_functions_checker;
	context.warning = false;

	return contain_mutable_functions_walker(clause, &context);
}

static bool
contain_mutable_functions_checker(Oid func_id, void *context
#ifdef _PG_ORCL_
						, Node *node
#endif
						)
{
#ifdef _PG_ORCL_
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid WITH function internal ID");

		return (func_volatile_withfuncs(expr->withfuncnsp, expr->withfuncid)
								!= PROVOLATILE_IMMUTABLE);
	}
	else if (ORA_MODE && node && context)
		return (func_volatile(func_id) != PROVOLATILE_IMMUTABLE &&
				!((contain_check_functions_context *) context)->checker(func_id, context, node));
	else /* Normal case */
#endif
	return (func_volatile(func_id) != PROVOLATILE_IMMUTABLE);
}

static bool
contain_mutable_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	/* Check for mutable functions in node itself */
	if (check_functions_in_node(node, contain_mutable_functions_checker,
								context))
		return true;

	if (IsA(node, SQLValueFunction))
	{
		/* all variants of SQLValueFunction are stable */
		return true;
	}

	if (IsA(node, NextValueExpr))
	{
		/* NextValueExpr is volatile */
		return true;
	}

	/*
	 * It should be safe to treat MinMaxExpr as immutable, because it will
	 * depend on a non-cross-type btree comparison function, and those should
	 * always be immutable.  Treating XmlExpr as immutable is more dubious,
	 * and treating CoerceToDomain as immutable is outright dangerous.  But we
	 * have done so historically, and changing this would probably cause more
	 * problems than it would fix.  In practice, if you have a non-immutable
	 * domain constraint you are in for pain anyhow.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 contain_mutable_functions_walker,
								 context, 0);
	}
	return expression_tree_walker(node, contain_mutable_functions_walker,
								  context);
}

bool
contain_opentenbaseLO_functions(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_opentenbaseLO_functions_checker;
	context.warning = false;

	return contain_check_functions_walker(clause, &context);
}

#ifdef _PG_ORCL_
bool
contain_opentenbaseLO_functions_checker(Oid func_id, void *context, Node *node)
{
	switch (func_id)
	{
		case 715:  /* lo_create */
		case 952:  /* lo_open */
		case 953:  /* lo_close */
		case 954:  /* loread */
		case 955:  /* lowrite */
		case 956:  /* lo_lseek */
		case 957:  /* lo_creat */
		case 958:  /* lo_tell */
		case 964:  /* lo_unlink */
		case 1004: /* lo_truncate */
		case 3170: /* lo_lseek64 */
		case 3171: /* lo_tell64 */
		case 3172: /* lo_truncate64 */
		case 3457: /* lo_from_bytea */
		case 3460: /* lo_put */
		{
			return true;
		}
		default:
		{
			break;
		}
	}
	return false;
}
#endif

static bool
contain_unshippable_functions_checker(Oid func_id, void *context, Node *node)
{
	return !pgxc_is_func_shippable(func_id);
}

bool
contain_unshippable_functions(Node *clause)
{
	contain_check_functions_context context;

	context.checker = &contain_unshippable_functions_checker;
	context.warning = false;

	return contain_check_functions_walker(clause, &context);
}

/*****************************************************************************
 *		Check clauses for volatile functions
 *****************************************************************************/

/*
 * contain_volatile_functions
 *	  Recursively search for volatile functions within a clause.
 *
 * Returns true if any volatile function (or operator implemented by a
 * volatile function) is found. This test prevents, for example,
 * invalid conversions of volatile expressions into indexscan quals.
 *
 * We will recursively look into Query nodes (i.e., SubLink sub-selects)
 * but not into SubPlans.  This is a bit odd, but intentional.  If we are
 * looking at a SubLink, we are probably deciding whether a query tree
 * transformation is safe, and a contained sub-select should affect that;
 * for example, duplicating a sub-select containing a volatile function
 * would be bad.  However, once we've got to the stage of having SubPlans,
 * subsequent planning need not consider volatility within those, since
 * the executor won't change its evaluation rules for a SubPlan based on
 * volatility.
 */
bool
contain_volatile_functions(Node *clause)
{
	return contain_volatile_functions_walker(clause, NULL);
}

static bool
contain_volatile_functions_checker(Oid func_id, void *context
#ifdef _PG_ORCL_
									, Node *node
#endif
									)
{
#ifdef _PG_ORCL_
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid WITH function internal ID");

		return (func_volatile_withfuncs(expr->withfuncnsp, expr->withfuncid)
											== PROVOLATILE_VOLATILE);
	}
	else /* Normal case */
#endif
	return (func_volatile(func_id) == PROVOLATILE_VOLATILE);
}

static bool
contain_volatile_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	/* Check for volatile functions in node itself */
	if (check_functions_in_node(node, contain_volatile_functions_checker,
								context))
		return true;

	if (IsA(node, NextValueExpr))
	{
		/* NextValueExpr is volatile */
		return true;
	}

	/*
	 * See notes in contain_mutable_functions_walker about why we treat
	 * MinMaxExpr, XmlExpr, and CoerceToDomain as immutable, while
	 * SQLValueFunction is stable.  Hence, none of them are of interest here.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 contain_volatile_functions_walker,
								 context, 0);
	}
	return expression_tree_walker(node, contain_volatile_functions_walker,
								  context);
}

/*
 * Special purpose version of contain_volatile_functions() for use in COPY:
 * ignore nextval(), but treat all other functions normally.
 */
bool
contain_volatile_functions_not_nextval(Node *clause)
{
	return contain_volatile_functions_not_nextval_walker(clause, NULL);
}

static bool
contain_volatile_functions_not_nextval_checker(Oid func_id, void *context
#ifdef _PG_ORCL_
												, Node *node
#endif
												)
{
#ifdef _PG_ORCL_
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid WITH function internal ID");

		return (func_volatile_withfuncs(expr->withfuncnsp, expr->withfuncid)
										== PROVOLATILE_VOLATILE);
	}
	else /* Normal case */
#endif
	return (func_id != F_NEXTVAL_OID &&
			func_volatile(func_id) == PROVOLATILE_VOLATILE);
}

static bool
contain_volatile_functions_not_nextval_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	/* Check for volatile functions in node itself */
	if (check_functions_in_node(node,
								contain_volatile_functions_not_nextval_checker,
								context))
		return true;

	/*
	 * See notes in contain_mutable_functions_walker about why we treat
	 * MinMaxExpr, XmlExpr, and CoerceToDomain as immutable, while
	 * SQLValueFunction is stable.  Hence, none of them are of interest here.
	 * Also, since we're intentionally ignoring nextval(), presumably we
	 * should ignore NextValueExpr.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 contain_volatile_functions_not_nextval_walker,
								 context, 0);
	}
	return expression_tree_walker(node,
								  contain_volatile_functions_not_nextval_walker,
								  context);
}


/*****************************************************************************
 *		Check queries for parallel unsafe and/or restricted constructs
 *****************************************************************************/

/*
 * max_parallel_hazard
 *		Find the worst parallel-hazard level in the given query
 *
 * Returns the worst function hazard property (the earliest in this list:
 * PROPARALLEL_UNSAFE, PROPARALLEL_RESTRICTED, PROPARALLEL_SAFE) that can
 * be found in the given parsetree.  We use this to find out whether the query
 * can be parallelized at all.  The caller will also save the result in
 * PlannerGlobal so as to short-circuit checks of portions of the querytree
 * later, in the common case where everything is SAFE.
 */
char
max_parallel_hazard(Query *parse, bool *nextval_parallel_safe)
{
	max_parallel_hazard_context context;

	context.max_hazard = PROPARALLEL_SAFE;
	context.max_interesting = PROPARALLEL_UNSAFE;
	context.safe_param_ids = NIL;
	context.check_all = false;
	context.nextval_parallel_safe = false;
	context.objects = NIL;

	(void) max_parallel_hazard_walker((Node *) parse, &context);
	*nextval_parallel_safe = context.nextval_parallel_safe;
	/* if max_hazard is unsafe, nexval can't parallel, there is no need to get top TransactionId*/
	if (context.max_hazard == PROPARALLEL_UNSAFE)
		*nextval_parallel_safe = false;
	return context.max_hazard;
}

bool
is_parallel_dml_safe(Query *parse)
{
	max_parallel_hazard_context context;

	if (!IsModifySupportedInParallelMode(parse->commandType))
		return false;
	
	if (!parse->resultRelation || parse->multi_inserts || parse->hasRowNumExpr)
		return false;
	
	context.max_hazard = PROPARALLEL_SAFE;
	context.max_interesting = PROPARALLEL_UNSAFE;
	context.safe_param_ids = NIL;
	context.check_all = false;
	context.objects = NIL;

	(void) max_parallel_dml_hazard(parse, &context);
	
	return context.max_hazard == PROPARALLEL_SAFE;
}

/* Check the safety of parallel data modification */
static char
max_parallel_dml_hazard(Query *parse,
						max_parallel_hazard_context *context)
{
	RangeTblEntry  *rte;
	Relation		target_rel;
	char			hazard;
	
	if (!IsModifySupportedInParallelMode(parse->commandType))
		return context->max_hazard;

	/*
	 * The target table is already locked by the caller (this is done in the
	 * parse/analyze phase), and remains locked until end-of-transaction.
	 */
	rte = rt_fetch(parse->resultRelation, parse->rtable);
	target_rel = relation_open(rte->relid, NoLock);

	/*
	 * If user set specific parallel dml safety safe/restricted/unsafe, we
	 * respect what user has set. If not set, for non-partitioned table, check
	 * the safety automatically, for partitioned table, consider it as unsafe.
	 */
	hazard = reloptions_get_parallel_dml_safety(target_rel->rd_options);
	
	if (target_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE &&
		hazard == PROPARALLEL_DEFAULT)
		hazard = PROPARALLEL_UNSAFE;

	if (target_rel->rd_rel->relhassubclass && has_subclass_exact(target_rel->rd_id))
		hazard = PROPARALLEL_UNSAFE;

	if (hazard != PROPARALLEL_DEFAULT)
		(void) max_parallel_hazard_test(hazard, context);
	/* Do parallel safety check for the target relation */
	else
	{
		bool max_hazard_found;
		char pre_max_hazard = context->max_hazard;
		
		context->max_hazard = PROPARALLEL_SAFE;
		max_hazard_found = target_rel_parallel_hazard_recurse(target_rel,
															  context,
															  false,
															  false);
		if (!max_hazard_found)
			(void) max_parallel_hazard_test(pre_max_hazard, context);
	}
	
	relation_close(target_rel, NoLock);
	
	return context->max_hazard;
}

/*
 * is_parallel_safe
 *		Detect whether the given expr contains only parallel-safe functions
 *
 * root->glob->maxParallelHazard must previously have been set to the
 * result of max_parallel_hazard() on the whole query.
 */
bool
is_parallel_safe(PlannerInfo *root, Node *node)
{
	max_parallel_hazard_context context;
	PlannerInfo *proot;
	ListCell   *l;

	/*
	 * Even if the original querytree contained nothing unsafe, we need to
	 * search the expression if we have generated any PARAM_EXEC Params while
	 * planning, because those are parallel-restricted and there might be one
	 * in this expression.  But otherwise we don't need to look.
	 */
	if (root->glob->maxParallelHazard == PROPARALLEL_SAFE &&
		root->glob->paramExecTypes == NIL)
		return true;
	/* Else use max_parallel_hazard's search logic, but stop on RESTRICTED */
	context.max_hazard = PROPARALLEL_SAFE;
	context.max_interesting = PROPARALLEL_RESTRICTED;
	context.safe_param_ids = NIL;
	context.check_all = false;
	context.objects = NIL;

	/*
	 * The params that refer to the same or parent query level are considered
	 * parallel-safe.  The idea is that we compute such params at Gather or
	 * Gather Merge node and pass their value to workers.
	 */
	for (proot = root; proot != NULL; proot = proot->parent_root)
	{
		foreach(l, proot->init_plans)
		{
			SubPlan    *initsubplan = (SubPlan *) lfirst(l);
			ListCell   *l2;

			foreach(l2, initsubplan->setParam)
				context.safe_param_ids = lcons_int(lfirst_int(l2),
											context.safe_param_ids);
		}
	}

	return !max_parallel_hazard_walker(node, &context);
}

/* core logic for all parallel-hazard checks */
static bool
max_parallel_hazard_test(char proparallel, max_parallel_hazard_context *context)
{
	switch (proparallel)
	{
		case PROPARALLEL_SAFE:
			/* nothing to see here, move along */
			break;
		case PROPARALLEL_RESTRICTED:
			/* increase max_hazard to RESTRICTED */
			Assert(context->check_all || context->max_hazard != PROPARALLEL_UNSAFE);
			context->max_hazard = proparallel;
			/* done if we are not expecting any unsafe functions */
			if (context->max_interesting == proparallel)
				return true;
			break;
		case PROPARALLEL_UNSAFE:
			context->max_hazard = proparallel;
			/* we're always done at the first unsafe construct */
			return true;
		default:
			elog(ERROR, "unrecognized proparallel value \"%c\"", proparallel);
			break;
	}
	return false;
}

/* check_functions_in_node callback */
static bool
max_parallel_hazard_checker(Oid func_id, void *context
#ifdef _PG_ORCL_
							, Node *node
#endif
							)
{
#ifdef _PG_ORCL_
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid WITH function internal ID");

		return max_parallel_hazard_test(
					func_parallel_withfuncs(expr->withfuncnsp,
											expr->withfuncid),
					(max_parallel_hazard_context *) context);
	}
	else /* Normal case */
#endif
    {
        char parallel;
        if ((func_id == F_ORCL_NEXTVAL_OID || func_id == F_NEXTVAL_OID) &&
             enable_parallel_sequence && IS_CENTRALIZED_MODE)
        {
            parallel = PROPARALLEL_SAFE;
            ((max_parallel_hazard_context *) context)->nextval_parallel_safe = true;
        }
        else
        {
            parallel = func_parallel(func_id);
        }
        return max_parallel_hazard_test(parallel,
                                        (max_parallel_hazard_context *) context);
    }
}

/*
 * make_safety_object
 *
 * Creates a safety_object, given object id, class id and parallel safety.
 */
static safety_object *
make_safety_object(Oid objid, Oid classid, char proparallel)
{
	safety_object *object = (safety_object *) palloc(sizeof(safety_object));
	
	object->objid = objid;
	object->classid = classid;
	object->proparallel = proparallel;
	
	return object;
}

/* check_functions_in_node callback */
static bool
parallel_hazard_checker(Oid func_id, void *context, Node *node)
{
	char	proparallel;
	max_parallel_hazard_context *cont = (max_parallel_hazard_context *) context;
	
	proparallel = func_parallel(func_id);
	
	if (max_parallel_hazard_test(proparallel, cont) && !cont->check_all)
		return true;
	else if (proparallel != PROPARALLEL_SAFE)
	{
		safety_object *object = make_safety_object(func_id,
												   ProcedureRelationId,
												   proparallel);
		cont->objects = lappend(cont->objects, object);
	}
	
	return false;
}

/*
 * parallel_hazard_walker
 *
 * Recursively search an expression tree which is defined as partition key or
 * index or constraint or column default expression for PARALLEL
 * UNSAFE/RESTRICTED table-related objects.
 *
 * If context->find_all is true, then detect all PARALLEL UNSAFE/RESTRICTED
 * table-related objects.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
parallel_hazard_walker(Node *node, max_parallel_hazard_context *context)
{
	if (node == NULL)
		return false;
	
	/* Check for hazardous functions in node itself */
	if (check_functions_in_node(node, parallel_hazard_checker,
								context))
		return true;
	
	if (IsA(node, CoerceToDomain))
	{
		CoerceToDomain *domain = (CoerceToDomain *) node;
		
		if (target_rel_domain_parallel_hazard(domain->resulttype, context))
			return true;
	}
	
	/* Recurse to check arguments */
	return expression_tree_walker(node,
								  parallel_hazard_walker,
								  context);
}

static bool
max_parallel_hazard_walker(Node *node, max_parallel_hazard_context *context)
{
	if (node == NULL)
		return false;

	/* Check for hazardous functions in node itself */
	if (check_functions_in_node(node, max_parallel_hazard_checker,
								context))
		return true;

	/*
	 * It should be OK to treat MinMaxExpr as parallel-safe, since btree
	 * opclass support functions are generally parallel-safe.  XmlExpr is a
	 * bit more dubious but we can probably get away with it.  We err on the
	 * side of caution by treating CoerceToDomain as parallel-restricted.
	 * (Note: in principle that's wrong because a domain constraint could
	 * contain a parallel-unsafe function; but useful constraints probably
	 * never would have such, and assuming they do would cripple use of
	 * parallel query in the presence of domain types.)  SQLValueFunction
	 * should be safe in all cases.  NextValueExpr is parallel-unsafe.
	 */
	if (IsA(node, CoerceToDomain))
	{
		if (max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context))
			return true;
	}

	if (IsA(node, NextValueExpr))
	{
		if (max_parallel_hazard_test(PROPARALLEL_UNSAFE, context))
			return true;
	}

	else if (IsA(node, RownumExpr))
	{
		if (max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context))
			return true;
	}

	/*
	 * As a notational convenience for callers, look through RestrictInfo.
	 */
	else if (IsA(node, RestrictInfo))
	{
		RestrictInfo *rinfo = (RestrictInfo *) node;

		return max_parallel_hazard_walker((Node *) rinfo->clause, context);
	}

	/*
	 * Really we should not see SubLink during a max_interesting == restricted
	 * scan, but if we do, return true.
	 */
	else if (IsA(node, SubLink))
	{
		if (max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context))
			return true;
	}

	/*
	 * Only parallel-safe SubPlans can be sent to workers.  Within the
	 * testexpr of the SubPlan, Params representing the output columns of the
	 * subplan can be treated as parallel-safe, so temporarily add their IDs
	 * to the safe_param_ids list while examining the testexpr.
	 */
	else if (IsA(node, SubPlan))
	{
		SubPlan    *subplan = (SubPlan *) node;
		List	   *save_safe_param_ids;

		if (!subplan->parallel_safe &&
			max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context))
			return true;
		save_safe_param_ids = context->safe_param_ids;
		context->safe_param_ids = list_concat(list_copy(subplan->paramIds),
											  context->safe_param_ids);
		if (max_parallel_hazard_walker(subplan->testexpr, context))
			return true;		/* no need to restore safe_param_ids */
		context->safe_param_ids = save_safe_param_ids;
		/* we must also check args, but no special Param treatment there */
		if (max_parallel_hazard_walker((Node *) subplan->args, context))
			return true;
		/* don't want to recurse normally, so we're done */
		return false;
	}

	/*
	 * We can't pass Params to workers at the moment either, so they are also
	 * parallel-restricted, unless they are PARAM_EXTERN Params or are
	 * PARAM_EXEC Params listed in safe_param_ids, meaning they could be
	 * either generated within the worker or can be computed in master and
	 * then their value can be passed to the worker.
	 */
	else if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN)
			return false;

		if (param->paramkind != PARAM_EXEC ||
			!list_member_int(context->safe_param_ids, param->paramid))
		{
			if (max_parallel_hazard_test(PROPARALLEL_RESTRICTED, context))
				return true;
		}
		return false;			/* nothing to recurse to */
	}

	/*
	 * When we're first invoked on a completely unplanned tree, we must
	 * recurse into subqueries so to as to locate parallel-unsafe constructs
	 * anywhere in the tree.
	 */
	else if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		/* SELECT FOR UPDATE/SHARE must be treated as unsafe */
		if (query->rowMarks != NULL)
		{
			context->max_hazard = PROPARALLEL_UNSAFE;
			return true;
		}

		/* Recurse into subselects */
		return query_tree_walker(query,
								 max_parallel_hazard_walker,
								 context, 0);
	}

	/* Recurse to check arguments */
	return expression_tree_walker(node,
								  max_parallel_hazard_walker,
								  context);
}

/*
 * target_rel_parallel_hazard
 *
 * If context->find_all is true, then detect all PARALLEL UNSAFE/RESTRICTED
 * table-related objects.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
List*
target_rel_parallel_hazard(Oid relOid, bool findall,
						   char max_interesting, char *max_hazard)
{
	max_parallel_hazard_context context;
	Relation	targetRel;

	context.check_all = findall;
	context.objects = NIL;
	context.max_hazard = PROPARALLEL_SAFE;
	context.max_interesting = max_interesting;
	context.safe_param_ids = NIL;

	targetRel = relation_open(relOid, AccessShareLock);

	(void) target_rel_parallel_hazard_recurse(targetRel, &context, false, true);

	relation_close(targetRel, AccessShareLock);

	*max_hazard = context.max_hazard;

	return context.objects;
}

/*
 * target_rel_parallel_hazard_recurse
 *
 * Recursively search all table-related objects for PARALLEL UNSAFE/RESTRICTED
 * objects.
 *
 * If context->find_all is true, then detect all PARALLEL UNSAFE/RESTRICTED
 * table-related objects.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
target_rel_parallel_hazard_recurse(Relation rel,
								   max_parallel_hazard_context *context,
								   bool is_partition,
								   bool check_column_default)
{
	TupleDesc	tupdesc;
	int			attnum;

	/*
	 * We can't support table modification in a parallel worker if it's a
	 * foreign table/partition (no FDW API for supporting parallel access) or
	 * a temporary table.
	 */
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		if (max_parallel_hazard_test(PROPARALLEL_UNSAFE, context) &&
			!context->check_all)
			return true;
		else
		{
			safety_object *object = make_safety_object(RelationGetRelid(rel),
													   RelationRelationId,
													   PROPARALLEL_UNSAFE);
			context->objects = lappend(context->objects, object);
		}
	}

	/*
	 * If there are any index expressions or index predicate, check that they
	 * are parallel-mode safe.
	 */
	if (target_rel_index_parallel_hazard(rel, context))
		return true;

	/*
	 * If any triggers exist, check that they are parallel-safe.
	 */
	if (target_rel_trigger_parallel_hazard(rel, context))
		return true;

	/*
	 * Column default expressions are only applicable to INSERT and UPDATE.
	 * Note that even though column defaults may be specified separately for
	 * each partition in a partitioned table, a partition's default value is
	 * not applied when inserting a tuple through a partitioned table.
	 */

	tupdesc = RelationGetDescr(rel);
	for (attnum = 0; attnum < tupdesc->natts; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attnum);

		/* We don't need info for dropped or generated attributes */
		if (att->attisdropped)
			continue;

		if (att->atthasdef && check_column_default)
		{
			Node	   *defaultexpr;

			defaultexpr = build_column_default(rel, attnum + 1);
			if (parallel_hazard_walker((Node *) defaultexpr, context))
				return true;
		}

		/*
		 * If the column is of a DOMAIN type, determine whether that
		 * domain has any CHECK expressions that are not parallel-mode
		 * safe.
		 */
		if (get_typtype(att->atttypid) == TYPTYPE_DOMAIN)
		{
			if (target_rel_domain_parallel_hazard(att->atttypid, context))
				return true;
		}
	}

	/*
	 * CHECK constraints are only applicable to INSERT and UPDATE. If any
	 * CHECK constraints exist, determine if they are parallel-safe.
	 */
	if (target_rel_chk_constr_parallel_hazard(rel, context))
		return true;

	return false;
}

/*
 * target_rel_trigger_parallel_hazard
 *
 * If context->find_all is true, then find all the PARALLEL UNSAFE/RESTRICTED
 * objects for the specified relation's trigger data.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
target_rel_trigger_parallel_hazard(Relation rel,
								   max_parallel_hazard_context *context)
{
	int		i;
	char	proparallel = PROPARALLEL_UNSAFE;

	if (rel->trigdesc == NULL)
		return false;

	max_parallel_hazard_test(PROPARALLEL_UNSAFE, context);
	
	/*
	 * Care is needed here to avoid using the same relcache TriggerDesc field
	 * across other cache accesses, because relcache doesn't guarantee that it
	 * won't move.
	 */
	for (i = 0; i < rel->trigdesc->numtriggers; i++)
	{
		Oid			tgfoid = rel->trigdesc->triggers[i].tgfoid;
		Oid			tgoid = rel->trigdesc->triggers[i].tgoid;
		safety_object  *object;
		safety_object  *parent_object;

		object = make_safety_object(tgfoid, ProcedureRelationId,
									proparallel);
		parent_object = make_safety_object(tgoid, TriggerRelationId,
										   proparallel);

		context->objects = lappend(context->objects, object);
		context->objects = lappend(context->objects, parent_object);
		
	}
	
	return true;
}

/*
 * index_expr_parallel_hazard
 *
 * If context->find_all is true, then find all the PARALLEL UNSAFE/RESTRICTED
 * objects for the input index expression and index predicate.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
index_expr_parallel_hazard(Relation index_rel,
						   List *ii_Expressions,
						   List *ii_Predicate,
						   max_parallel_hazard_context *context)
{
	int				i;
	Form_pg_index	indexStruct;
	ListCell	   *index_expr_item;

	indexStruct = index_rel->rd_index;
	index_expr_item = list_head(ii_Expressions);

	/* Check parallel-safety of index expression */
	for (i = 0; i < indexStruct->indnatts; i++)
	{
		int			keycol = indexStruct->indkey.values[i];

		if (keycol == 0)
		{
			/* Found an index expression */
			Node	   *index_expr;

			Assert(index_expr_item != NULL);
			if (index_expr_item == NULL)	/* shouldn't happen */
				elog(ERROR, "too few entries in indexprs list");

			index_expr = (Node *) lfirst(index_expr_item);

			if (parallel_hazard_walker(index_expr, context))
				return true;

			index_expr_item = lnext(index_expr_item);
		}
	}

	/* Check parallel-safety of index predicate */
	if (parallel_hazard_walker((Node *) ii_Predicate, context))
		return true;

	return false;
}

/*
 * target_rel_index_parallel_hazard
 *
 * If context->find_all is true, then find all the PARALLEL UNSAFE/RESTRICTED
 * objects for any existing index expressions or index predicate of a specified
 * relation.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
target_rel_index_parallel_hazard(Relation rel,
								 max_parallel_hazard_context *context)
{
	List	   *index_oid_list;
	ListCell   *lc;
	LOCKMODE	lockmode = AccessShareLock;
	bool		max_hazard_found;

	index_oid_list = RelationGetIndexList(rel);
	foreach(lc, index_oid_list)
	{
		Relation	index_rel;
		List	   *ii_Expressions;
		List	   *ii_Predicate;
		List	   *temp_objects;
		char		temp_hazard;
		Oid			index_oid = lfirst_oid(lc);

		temp_objects = context->objects;
		context->objects = NIL;
		temp_hazard = context->max_hazard;
		context->max_hazard = PROPARALLEL_SAFE;

		index_rel = index_open(index_oid, lockmode);

		/* Check index expression */
		ii_Expressions = RelationGetIndexExpressions(index_rel);
		ii_Predicate = RelationGetIndexPredicate(index_rel);

		max_hazard_found = index_expr_parallel_hazard(index_rel,
													  ii_Expressions,
													  ii_Predicate,
													  context);

		index_close(index_rel, lockmode);

		if (max_hazard_found)
			return true;

		/* Add the index itself to the objects list */
		else if (context->objects != NIL)
		{
			safety_object  *object;

			object = make_safety_object(index_oid, IndexRelationId,
										context->max_hazard);
			context->objects = lappend(context->objects, object);
		}

		(void) max_parallel_hazard_test(temp_hazard, context);

		context->objects = list_concat(context->objects, temp_objects);
		list_free(temp_objects);
	}

	list_free(index_oid_list);

	return false;
}

/*
 * target_rel_domain_parallel_hazard
 *
 * If context->find_all is true, then find all the PARALLEL UNSAFE/RESTRICTED
 * objects for the specified DOMAIN type. Only any CHECK expressions are
 * examined for parallel-safety.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
target_rel_domain_parallel_hazard(Oid typid,
								  max_parallel_hazard_context *context)
{
	ListCell   *lc;
	List	   *domain_list;
	List       *temp_objects;
	char		temp_hazard;

	domain_list = GetDomainConstraints(typid);

	foreach(lc, domain_list)
	{
		DomainConstraintState *r = (DomainConstraintState *) lfirst(lc);

		temp_objects = context->objects;
		context->objects = NIL;
		temp_hazard = context->max_hazard;
		context->max_hazard = PROPARALLEL_SAFE;

		if (parallel_hazard_walker((Node *) r->check_expr, context))
			return true;

		/* Add the constraint itself to the objects list */
		else if (context->objects != NIL)
		{
			safety_object  *object;
			Oid				constr_oid = get_domain_constraint_oid(typid,
																   r->name,
																   false);

			object = make_safety_object(constr_oid,
										ConstraintRelationId,
										context->max_hazard);
			context->objects = lappend(context->objects, object);
		}

		(void) max_parallel_hazard_test(temp_hazard, context);

		context->objects = list_concat(context->objects, temp_objects);
		list_free(temp_objects);
	}

	return false;

}

/*
 * target_rel_chk_constr_parallel_hazard
 *
 * If context->find_all is true, then find all the PARALLEL UNSAFE/RESTRICTED
 * objects for any CHECK expressions or CHECK constraints related to the
 * specified relation.
 *
 * If context->find_all is false, then find the worst parallel-hazard level.
 */
static bool
target_rel_chk_constr_parallel_hazard(Relation rel,
									  max_parallel_hazard_context *context)
{
	char			temp_hazard;
	int				i;
	TupleDesc		tupdesc;
	List		   *temp_objects;
	ConstrCheck	   *check;

	tupdesc = RelationGetDescr(rel);

	if (tupdesc->constr == NULL)
		return false;

	check = tupdesc->constr->check;

	/*
	 * Determine if there are any CHECK constraints which are not
	 * parallel-safe.
	 */
	for (i = 0; i < tupdesc->constr->num_check; i++)
	{
		Expr	   *check_expr = stringToNode(check[i].ccbin);

		temp_objects = context->objects;
		context->objects = NIL;
		temp_hazard = context->max_hazard;
		context->max_hazard = PROPARALLEL_SAFE;

		if (parallel_hazard_walker((Node *) check_expr, context))
			return true;

		/* Add the constraint itself to the objects list */
		if (context->objects != NIL)
		{
			Oid constr_oid;
			safety_object *object;

			constr_oid = get_relation_constraint_oid(RelationGetRelid(rel),
													 check->ccname,
													 true);

			object = make_safety_object(constr_oid,
										ConstraintRelationId,
										context->max_hazard);

			context->objects = lappend(context->objects, object);
		}

		(void) max_parallel_hazard_test(temp_hazard, context);

		context->objects = list_concat(context->objects, temp_objects);
		list_free(temp_objects);
	}

	return false;
}

/*
 * is_parallel_allowed_for_modify
 *
 * Check at a high-level if parallel mode is able to be used for the specified
 * table-modification statement.
 * Currently, we support only Simple Insert Into Select, and Update.
 */
bool
is_parallel_allowed_for_modify(Query *parse)
{
	bool		hasSubQuery;
	bool		isSimpleRel;
	RangeTblEntry *rte;
	ListCell   *lc;
	Relation	rel;

	if (!enable_parallel_insert && !enable_parallel_update)
		return false;

	/* INSERT and UPDATE are supported */
	if ((parse->commandType != CMD_INSERT || parse->onConflict != NULL) && parse->commandType != CMD_UPDATE)
		return false;

    /* TODO: INSERT ALL/FIRST do NOT support parallel? */
    if (parse->commandType == CMD_INSERT && parse->multi_inserts != NULL)
        return false;

    /* Currently only support ordinary table(not index, temp table) without trigger/constraint */
    rte = rt_fetch(parse->resultRelation, parse->rtable);
    rel = heap_open(rte->relid, NoLock);

    isSimpleRel = (rel->rd_rel->relkind == RELKIND_RELATION &&
				   rel->trigdesc == NULL && rel->rd_rules == NULL);

    heap_close(rel, NoLock);


    if(!isSimpleRel)
        return false;

    /* If sql is update, do not need check subquery.*/
	if (parse->commandType == CMD_UPDATE)
		return enable_parallel_update;

	/* Currently only support INSERT INTO SELECT */
	hasSubQuery = false;
	foreach(lc, parse->rtable)
	{
		rte = lfirst_node(RangeTblEntry, lc);
		if (rte->rtekind == RTE_SUBQUERY)
		{
			hasSubQuery = true;
			break;
		}
	}
	if (!hasSubQuery)
		return false;

    return enable_parallel_insert;
}


/*****************************************************************************
 *		Check clauses for nonstrict functions
 *****************************************************************************/

/*
 * contain_nonstrict_functions
 *	  Recursively search for nonstrict functions within a clause.
 *
 * Returns true if any nonstrict construct is found --- ie, anything that
 * could produce non-NULL output with a NULL input.
 *
 * The idea here is that the caller has verified that the expression contains
 * one or more Var or Param nodes (as appropriate for the caller's need), and
 * now wishes to prove that the expression result will be NULL if any of these
 * inputs is NULL.  If we return false, then the proof succeeded.
 */
bool
contain_nonstrict_functions(Node *clause)
{
	return contain_nonstrict_functions_walker(clause, NULL);
}

#ifdef __OPENTENBASE__
bool
contain_nonstrict_functions_with_checkagg(Node *clause)
{
    not_strict_context context;

    context.check_agg = true;
	context.stop_before_sublink = false;
	return contain_nonstrict_functions_walker(clause, &context);
}

bool
contain_nonstrict_functions_before_sublink(Node *clause)
{
	not_strict_context context;

	context.check_agg = false;
	context.stop_before_sublink = true;
	return contain_nonstrict_functions_walker(clause, &context);
}
#endif

static bool
contain_nonstrict_functions_checker(Oid func_id, void *context
#ifdef _PG_ORCL_
									, Node *node
#endif
									)
{
#ifdef _PG_ORCL_
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid WITH function internal ID");

		return !func_strict_withfuncs(expr->withfuncnsp, expr->withfuncid);
	}
	else /* Normal case */
#endif
	return !func_strict(func_id);
}

static bool
contain_nonstrict_functions_walker(Node *node, void *context)
{
	bool check_agg = context && ((not_strict_context*)context)->check_agg;
	bool stop_before_sublink = context && ((not_strict_context*)context)->stop_before_sublink;

	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		if (check_agg)
		{
			Aggref* expr = (Aggref*)node;
			/* ANYCOUNTOID && COUNTOID are not strict functions*/
			if (expr->aggfnoid == ANYCOUNTOID  || expr->aggfnoid == COUNTOID ||
			expr->aggfnoid == ORA_ANYCOUNTOID  || expr->aggfnoid == ORA_COUNTOID)
			{
				return true;
			}
		}
		else
		{
			/* an aggregate could return non-null with null input */
			return true;
		}
	}
	if (IsA(node, GroupingFunc))
	{
		/*
		 * A GroupingFunc doesn't evaluate its arguments, and therefore must
		 * be treated as nonstrict.
		 */
		return true;
	}
	if (IsA(node, WindowFunc))
	{
		/* a window function could return non-null with null input */
		return true;
	}
	if (IsA(node, ArrayRef))
	{
		/* array assignment is nonstrict, but subscripting is strict */
		if (((ArrayRef *) node)->refassgnexpr != NULL)
			return true;
		/* else fall through to check args */
	}
	if (IsA(node, DistinctExpr))
	{
		/* IS DISTINCT FROM is inherently non-strict */
		return true;
	}
	if (IsA(node, NullIfExpr))
	{
		/* NULLIF is inherently non-strict */
		return true;
	}
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		switch (expr->boolop)
		{
			case AND_EXPR:
			case OR_EXPR:
				/* AND, OR are inherently non-strict */
				return true;
			default:
				break;
		}
	}
	if (IsA(node, SubLink))
	{
		if (stop_before_sublink)
			return false;
		/* In some cases a sublink might be strict, but in general not */
		return true;
	}
	if (IsA(node, SubPlan))
		return true;
	if (IsA(node, AlternativeSubPlan))
		return true;
	if (IsA(node, FieldStore))
		return true;
	if (IsA(node, ArrayCoerceExpr))
	{
		/*
		 * ArrayCoerceExpr is strict at the array level, regardless of what
		 * the per-element expression is; so we should ignore elemexpr and
		 * recurse only into the arg.
		 */
		return expression_tree_walker((Node *) ((ArrayCoerceExpr *) node)->arg,
									  contain_nonstrict_functions_walker,
									  context);
	}
	if (IsA(node, CaseExpr))
		return true;
	if (IsA(node, ArrayExpr))
		return true;
	if (IsA(node, RowExpr))
		return true;
	if (IsA(node, RowCompareExpr))
		return true;
	if (IsA(node, CoalesceExpr))
		return true;
	if (IsA(node, MinMaxExpr))
		return true;
	if (IsA(node, XmlExpr))
		return true;
	if (IsA(node, NullTest))
		return true;
	if (IsA(node, BooleanTest))
		return true;

	/* Check other function-containing nodes */
	if (!(IsA(node, Aggref) && check_agg) &&
		check_functions_in_node(node, contain_nonstrict_functions_checker,
								context))
		return true;

	return expression_tree_walker(node, contain_nonstrict_functions_walker,
								  context);
}

/*****************************************************************************
 *		Check clauses for context-dependent nodes
 *****************************************************************************/

/*
 * contain_context_dependent_node
 *	  Recursively search for context-dependent nodes within a clause.
 *
 * CaseTestExpr nodes must appear directly within the corresponding CaseExpr,
 * not nested within another one, or they'll see the wrong test value.  If one
 * appears "bare" in the arguments of a SQL function, then we can't inline the
 * SQL function for fear of creating such a situation.
 *
 * CoerceToDomainValue would have the same issue if domain CHECK expressions
 * could get inlined into larger expressions, but presently that's impossible.
 * Still, it might be allowed in future, or other node types with similar
 * issues might get invented.  So give this function a generic name, and set
 * up the recursion state to allow multiple flag bits.
 */
static bool
contain_context_dependent_node(Node *clause)
{
	int			flags = 0;

	return contain_context_dependent_node_walker(clause, &flags);
}

#define CCDN_IN_CASEEXPR	0x0001	/* CaseTestExpr okay here? */

static bool
contain_context_dependent_node_walker(Node *node, int *flags)
{
	if (node == NULL)
		return false;
	if (IsA(node, CaseTestExpr))
		return !(*flags & CCDN_IN_CASEEXPR);
	if (IsA(node, CaseExpr))
	{
		CaseExpr   *caseexpr = (CaseExpr *) node;

		/*
		 * If this CASE doesn't have a test expression, then it doesn't create
		 * a context in which CaseTestExprs should appear, so just fall
		 * through and treat it as a generic expression node.
		 */
		if (caseexpr->arg)
		{
			int			save_flags = *flags;
			bool		res;

			/*
			 * Note: in principle, we could distinguish the various sub-parts
			 * of a CASE construct and set the flag bit only for some of them,
			 * since we are only expecting CaseTestExprs to appear in the
			 * "expr" subtree of the CaseWhen nodes.  But it doesn't really
			 * seem worth any extra code.  If there are any bare CaseTestExprs
			 * elsewhere in the CASE, something's wrong already.
			 */
			*flags |= CCDN_IN_CASEEXPR;
			res = expression_tree_walker(node,
										 contain_context_dependent_node_walker,
										 (void *) flags);
			*flags = save_flags;
			return res;
		}
	}
	return expression_tree_walker(node, contain_context_dependent_node_walker,
								  (void *) flags);
}

/*****************************************************************************
 *		  Check clauses for Vars passed to non-leakproof functions
 *****************************************************************************/

/*
 * contain_leaked_vars
 *		Recursively scan a clause to discover whether it contains any Var
 *		nodes (of the current query level) that are passed as arguments to
 *		leaky functions.
 *
 * Returns true if the clause contains any non-leakproof functions that are
 * passed Var nodes of the current query level, and which might therefore leak
 * data.  Such clauses must be applied after any lower-level security barrier
 * clauses.
 */
bool
contain_leaked_vars(Node *clause)
{
	return contain_leaked_vars_walker(clause, NULL);
}

static bool
contain_leaked_vars_checker(Oid func_id, void *context
#ifdef _PG_ORCL_
						, Node *node
#endif
						)
{
#ifdef _PG_ORCL_
	if (func_id == InvalidOid && node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr	*expr = (FuncExpr *) node;

		if (expr->withfuncnsp == NULL || expr->withfuncid == -1)
			elog(ERROR, "invalid WITH function internal ID");

		return !get_func_leakproof_withfuncs(expr->withfuncnsp,
												expr->withfuncid);
	}
	else /* Normal case */
#endif
	return !get_func_leakproof(func_id);
}

static bool
contain_leaked_vars_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_Var:
		case T_Const:
		case T_Param:
		case T_ArrayRef:
		case T_ArrayExpr:
		case T_FieldSelect:
		case T_FieldStore:
		case T_NamedArgExpr:
		case T_BoolExpr:
		case T_RelabelType:
		case T_CollateExpr:
		case T_CaseExpr:
		case T_CaseTestExpr:
		case T_RowExpr:
		case T_MinMaxExpr:
		case T_SQLValueFunction:
		case T_NullTest:
		case T_BooleanTest:
		case T_NextValueExpr:
		case T_List:

			/*
			 * We know these node types don't contain function calls; but
			 * something further down in the node tree might.
			 */
			break;

		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_ScalarArrayOpExpr:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:

			/*
			 * If node contains a leaky function call, and there's any Var
			 * underneath it, reject.
			 */
			if (check_functions_in_node(node, contain_leaked_vars_checker,
										context) &&
				contain_var_clause(node))
				return true;
			break;

		case T_RowCompareExpr:
			{
				/*
				 * It's worth special-casing this because a leaky comparison
				 * function only compromises one pair of row elements, which
				 * might not contain Vars while others do.
				 */
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				ListCell   *opid;
				ListCell   *larg;
				ListCell   *rarg;

				forthree(opid, rcexpr->opnos,
						 larg, rcexpr->largs,
						 rarg, rcexpr->rargs)
				{
					Oid			funcid = get_opcode(lfirst_oid(opid));

					if (!get_func_leakproof(funcid) &&
						(contain_var_clause((Node *) lfirst(larg)) ||
						 contain_var_clause((Node *) lfirst(rarg))))
						return true;
				}
			}
			break;

		case T_CurrentOfExpr:

			/*
			 * WHERE CURRENT OF doesn't contain leaky function calls.
			 * Moreover, it is essential that this is considered non-leaky,
			 * since the planner must always generate a TID scan when CURRENT
			 * OF is present -- c.f. cost_tidscan.
			 */
			return false;

		default:

			/*
			 * If we don't recognize the node tag, assume it might be leaky.
			 * This prevents an unexpected security hole if someone adds a new
			 * node type that can call a function.
			 */
			return true;
	}
	return expression_tree_walker(node, contain_leaked_vars_walker,
								  context);
}

/*
 * find_nonnullable_rels
 *		Determine which base rels are forced nonnullable by given clause.
 *
 * Returns the set of all Relids that are referenced in the clause in such
 * a way that the clause cannot possibly return TRUE if any of these Relids
 * is an all-NULL row.  (It is OK to err on the side of conservatism; hence
 * the analysis here is simplistic.)
 *
 * The semantics here are subtly different from contain_nonstrict_functions:
 * that function is concerned with NULL results from arbitrary expressions,
 * but here we assume that the input is a Boolean expression, and wish to
 * see if NULL inputs will provably cause a FALSE-or-NULL result.  We expect
 * the expression to have been AND/OR flattened and converted to implicit-AND
 * format.
 *
 * Note: this function is largely duplicative of find_nonnullable_vars().
 * The reason not to simplify this function into a thin wrapper around
 * find_nonnullable_vars() is that the tested conditions really are different:
 * a clause like "t1.v1 IS NOT NULL OR t1.v2 IS NOT NULL" does not prove
 * that either v1 or v2 can't be NULL, but it does prove that the t1 row
 * as a whole can't be all-NULL.
 *
 * top_level is TRUE while scanning top-level AND/OR structure; here, showing
 * the result is either FALSE or NULL is good enough.  top_level is FALSE when
 * we have descended below a NOT or a strict function: now we must be able to
 * prove that the subexpression goes to NULL.
 *
 * We don't use expression_tree_walker here because we don't want to descend
 * through very many kinds of nodes; only the ones we can be sure are strict.
 */
Relids
find_nonnullable_rels(Node *clause)
{
	return find_nonnullable_rels_walker(clause, true);
}

static Relids
find_nonnullable_rels_walker(Node *node, bool top_level)
{
	Relids		result = NULL;
	ListCell   *l;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == 0)
			result = bms_make_singleton(var->varno);
	}
	else if (IsA(node, List))
	{
		/*
		 * At top level, we are examining an implicit-AND list: if any of the
		 * arms produces FALSE-or-NULL then the result is FALSE-or-NULL. If
		 * not at top level, we are examining the arguments of a strict
		 * function: if any of them produce NULL then the result of the
		 * function must be NULL.  So in both cases, the set of nonnullable
		 * rels is the union of those found in the arms, and we pass down the
		 * top_level flag unmodified.
		 */
		foreach(l, (List *) node)
		{
			result = bms_join(result,
							  find_nonnullable_rels_walker(lfirst(l),
														   top_level));
		}
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

#ifdef _PG_ORCL_
		if (expr->withfuncnsp != NULL)
		{
			Assert(expr->funcid == InvalidOid);

			if (func_strict_withfuncs(expr->withfuncnsp, expr->withfuncid))
				result = find_nonnullable_rels_walker((Node *) expr->args, false);
		}
		else /* Normal case */
#endif
		if (func_strict(expr->funcid))
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		set_opfuncid(expr);
		if (func_strict(expr->opfuncid))
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

		if (is_strict_saop(expr, true))
			result = find_nonnullable_rels_walker((Node *) expr->args, false);
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		switch (expr->boolop)
		{
			case AND_EXPR:
				/* At top level we can just recurse (to the List case) */
				if (top_level)
				{
					result = find_nonnullable_rels_walker((Node *) expr->args,
														  top_level);
					break;
				}

				/*
				 * Below top level, even if one arm produces NULL, the result
				 * could be FALSE (hence not NULL).  However, if *all* the
				 * arms produce NULL then the result is NULL, so we can take
				 * the intersection of the sets of nonnullable rels, just as
				 * for OR.  Fall through to share code.
				 */
				/* FALL THRU */
			case OR_EXPR:

				/*
				 * OR is strict if all of its arms are, so we can take the
				 * intersection of the sets of nonnullable rels for each arm.
				 * This works for both values of top_level.
				 */
				foreach(l, expr->args)
				{
					Relids		subresult;

					subresult = find_nonnullable_rels_walker(lfirst(l),
															 top_level);
					if (result == NULL) /* first subresult? */
						result = subresult;
					else
						result = bms_int_members(result, subresult);

					/*
					 * If the intersection is empty, we can stop looking. This
					 * also justifies the test for first-subresult above.
					 */
					if (bms_is_empty(result))
						break;
				}
				break;
			case NOT_EXPR:
				/* NOT will return null if its arg is null */
				result = find_nonnullable_rels_walker((Node *) expr->args,
													  false);
				break;
			default:
				elog(ERROR, "unrecognized boolop: %d", (int) expr->boolop);
				break;
		}
	}
	else if (IsA(node, RelabelType))
	{
		RelabelType *expr = (RelabelType *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, CoerceViaIO))
	{
		/* not clear this is useful, but it can't hurt */
		CoerceViaIO *expr = (CoerceViaIO *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, ArrayCoerceExpr))
	{
		/* ArrayCoerceExpr is strict at the array level; ignore elemexpr */
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, ConvertRowtypeExpr))
	{
		/* not clear this is useful, but it can't hurt */
		ConvertRowtypeExpr *expr = (ConvertRowtypeExpr *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, CollateExpr))
	{
		CollateExpr *expr = (CollateExpr *) node;

		result = find_nonnullable_rels_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, NullTest))
	{
		/* IS NOT NULL can be considered strict, but only at top level */
		NullTest   *expr = (NullTest *) node;

		if (top_level && expr->nulltesttype == IS_NOT_NULL && !expr->argisrow)
			result = find_nonnullable_rels_walker((Node *) expr->arg, false);
	}
	else if (IsA(node, BooleanTest))
	{
		/* Boolean tests that reject NULL are strict at top level */
		BooleanTest *expr = (BooleanTest *) node;

		if (top_level &&
			(expr->booltesttype == IS_TRUE ||
			 expr->booltesttype == IS_FALSE ||
			 expr->booltesttype == IS_NOT_UNKNOWN))
			result = find_nonnullable_rels_walker((Node *) expr->arg, false);
	}
	else if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		result = find_nonnullable_rels_walker((Node *) phv->phexpr, top_level);
	}
	return result;
}

/*
 * find_nonnullable_vars
 *		Determine which Vars are forced nonnullable by given clause.
 *
 * Returns a list of all level-zero Vars that are referenced in the clause in
 * such a way that the clause cannot possibly return TRUE if any of these Vars
 * is NULL.  (It is OK to err on the side of conservatism; hence the analysis
 * here is simplistic.)
 *
 * The semantics here are subtly different from contain_nonstrict_functions:
 * that function is concerned with NULL results from arbitrary expressions,
 * but here we assume that the input is a Boolean expression, and wish to
 * see if NULL inputs will provably cause a FALSE-or-NULL result.  We expect
 * the expression to have been AND/OR flattened and converted to implicit-AND
 * format.
 *
 * The result is a palloc'd List, but we have not copied the member Var nodes.
 * Also, we don't bother trying to eliminate duplicate entries.
 *
 * top_level is TRUE while scanning top-level AND/OR structure; here, showing
 * the result is either FALSE or NULL is good enough.  top_level is FALSE when
 * we have descended below a NOT or a strict function: now we must be able to
 * prove that the subexpression goes to NULL.
 *
 * We don't use expression_tree_walker here because we don't want to descend
 * through very many kinds of nodes; only the ones we can be sure are strict.
 */
List *
find_nonnullable_vars(Node *clause)
{
	return find_nonnullable_vars_walker(clause, true);
}

static List *
find_nonnullable_vars_walker(Node *node, bool top_level)
{
	List	   *result = NIL;
	ListCell   *l;

	if (node == NULL)
		return NIL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == 0)
			result = list_make1(var);
	}
	else if (IsA(node, List))
	{
		/*
		 * At top level, we are examining an implicit-AND list: if any of the
		 * arms produces FALSE-or-NULL then the result is FALSE-or-NULL. If
		 * not at top level, we are examining the arguments of a strict
		 * function: if any of them produce NULL then the result of the
		 * function must be NULL.  So in both cases, the set of nonnullable
		 * vars is the union of those found in the arms, and we pass down the
		 * top_level flag unmodified.
		 */
		foreach(l, (List *) node)
		{
			result = list_concat(result,
								 find_nonnullable_vars_walker(lfirst(l),
															  top_level));
		}
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *expr = (FuncExpr *) node;

#ifdef _PG_ORCL_
		if (expr->withfuncnsp != NULL)
		{
			Assert(expr->funcid == InvalidOid);

			if (func_strict_withfuncs(expr->withfuncnsp, expr->withfuncid))
				result = find_nonnullable_vars_walker((Node *) expr->args, false);
		}
		else /* Normal case */
#endif
		if (func_strict(expr->funcid))
			result = find_nonnullable_vars_walker((Node *) expr->args, false);
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *expr = (OpExpr *) node;

		set_opfuncid(expr);
		if (func_strict(expr->opfuncid))
			result = find_nonnullable_vars_walker((Node *) expr->args, false);
	}
	else if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) node;

		if (is_strict_saop(expr, true))
			result = find_nonnullable_vars_walker((Node *) expr->args, false);
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		switch (expr->boolop)
		{
			case AND_EXPR:
				/* At top level we can just recurse (to the List case) */
				if (top_level)
				{
					result = find_nonnullable_vars_walker((Node *) expr->args,
														  top_level);
					break;
				}

				/*
				 * Below top level, even if one arm produces NULL, the result
				 * could be FALSE (hence not NULL).  However, if *all* the
				 * arms produce NULL then the result is NULL, so we can take
				 * the intersection of the sets of nonnullable vars, just as
				 * for OR.  Fall through to share code.
				 */
				/* FALL THRU */
			case OR_EXPR:

				/*
				 * OR is strict if all of its arms are, so we can take the
				 * intersection of the sets of nonnullable vars for each arm.
				 * This works for both values of top_level.
				 */
				foreach(l, expr->args)
				{
					List	   *subresult;

					subresult = find_nonnullable_vars_walker(lfirst(l),
															 top_level);
					if (result == NIL)	/* first subresult? */
						result = subresult;
					else
						result = list_intersection(result, subresult);

					/*
					 * If the intersection is empty, we can stop looking. This
					 * also justifies the test for first-subresult above.
					 */
					if (result == NIL)
						break;
				}
				break;
			case NOT_EXPR:
				/* NOT will return null if its arg is null */
				result = find_nonnullable_vars_walker((Node *) expr->args,
													  false);
				break;
			default:
				elog(ERROR, "unrecognized boolop: %d", (int) expr->boolop);
				break;
		}
	}
	else if (IsA(node, RelabelType))
	{
		RelabelType *expr = (RelabelType *) node;

		result = find_nonnullable_vars_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, CoerceViaIO))
	{
		/* not clear this is useful, but it can't hurt */
		CoerceViaIO *expr = (CoerceViaIO *) node;

		result = find_nonnullable_vars_walker((Node *) expr->arg, false);
	}
	else if (IsA(node, ArrayCoerceExpr))
	{
		/* ArrayCoerceExpr is strict at the array level; ignore elemexpr */
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) node;

		result = find_nonnullable_vars_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, ConvertRowtypeExpr))
	{
		/* not clear this is useful, but it can't hurt */
		ConvertRowtypeExpr *expr = (ConvertRowtypeExpr *) node;

		result = find_nonnullable_vars_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, CollateExpr))
	{
		CollateExpr *expr = (CollateExpr *) node;

		result = find_nonnullable_vars_walker((Node *) expr->arg, top_level);
	}
	else if (IsA(node, NullTest))
	{
		/* IS NOT NULL can be considered strict, but only at top level */
		NullTest   *expr = (NullTest *) node;

		if (top_level && expr->nulltesttype == IS_NOT_NULL && !expr->argisrow)
			result = find_nonnullable_vars_walker((Node *) expr->arg, false);
	}
	else if (IsA(node, BooleanTest))
	{
		/* Boolean tests that reject NULL are strict at top level */
		BooleanTest *expr = (BooleanTest *) node;

		if (top_level &&
			(expr->booltesttype == IS_TRUE ||
			 expr->booltesttype == IS_FALSE ||
			 expr->booltesttype == IS_NOT_UNKNOWN))
			result = find_nonnullable_vars_walker((Node *) expr->arg, false);
	}
	else if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		result = find_nonnullable_vars_walker((Node *) phv->phexpr, top_level);
	}
	return result;
}

/*
 * find_forced_null_vars
 *		Determine which Vars must be NULL for the given clause to return TRUE.
 *
 * This is the complement of find_nonnullable_vars: find the level-zero Vars
 * that must be NULL for the clause to return TRUE.  (It is OK to err on the
 * side of conservatism; hence the analysis here is simplistic.  In fact,
 * we only detect simple "var IS NULL" tests at the top level.)
 *
 * The result is a palloc'd List, but we have not copied the member Var nodes.
 * Also, we don't bother trying to eliminate duplicate entries.
 */
List *
find_forced_null_vars(Node *node)
{
	List	   *result = NIL;
	Var		   *var;
	ListCell   *l;

	if (node == NULL)
		return NIL;
	/* Check single-clause cases using subroutine */
	var = find_forced_null_var(node);
	if (var)
	{
		result = list_make1(var);
	}
	/* Otherwise, handle AND-conditions */
	else if (IsA(node, List))
	{
		/*
		 * At top level, we are examining an implicit-AND list: if any of the
		 * arms produces FALSE-or-NULL then the result is FALSE-or-NULL.
		 */
		foreach(l, (List *) node)
		{
			result = list_concat(result,
								 find_forced_null_vars(lfirst(l)));
		}
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr   *expr = (BoolExpr *) node;

		/*
		 * We don't bother considering the OR case, because it's fairly
		 * unlikely anyone would write "v1 IS NULL OR v1 IS NULL". Likewise,
		 * the NOT case isn't worth expending code on.
		 */
		if (expr->boolop == AND_EXPR)
		{
			/* At top level we can just recurse (to the List case) */
			result = find_forced_null_vars((Node *) expr->args);
		}
	}
	return result;
}

/*
 * find_forced_null_var
 *		Return the Var forced null by the given clause, or NULL if it's
 *		not an IS NULL-type clause.  For success, the clause must enforce
 *		*only* nullness of the particular Var, not any other conditions.
 *
 * This is just the single-clause case of find_forced_null_vars(), without
 * any allowance for AND conditions.  It's used by initsplan.c on individual
 * qual clauses.  The reason for not just applying find_forced_null_vars()
 * is that if an AND of an IS NULL clause with something else were to somehow
 * survive AND/OR flattening, initsplan.c might get fooled into discarding
 * the whole clause when only the IS NULL part of it had been proved redundant.
 */
Var *
find_forced_null_var(Node *node)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, NullTest))
	{
		/* check for var IS NULL */
		NullTest   *expr = (NullTest *) node;

		if (expr->nulltesttype == IS_NULL && !expr->argisrow)
		{
			Var		   *var = (Var *) expr->arg;

			if (var && IsA(var, Var) &&
				var->varlevelsup == 0)
				return var;
		}
	}
	else if (IsA(node, BooleanTest))
	{
		/* var IS UNKNOWN is equivalent to var IS NULL */
		BooleanTest *expr = (BooleanTest *) node;

		if (expr->booltesttype == IS_UNKNOWN)
		{
			Var		   *var = (Var *) expr->arg;

			if (var && IsA(var, Var) &&
				var->varlevelsup == 0)
				return var;
		}
	}
	return NULL;
}

/*
 * Can we treat a ScalarArrayOpExpr as strict?
 *
 * If "falseOK" is true, then a "false" result can be considered strict,
 * else we need to guarantee an actual NULL result for NULL input.
 *
 * "foo op ALL array" is strict if the op is strict *and* we can prove
 * that the array input isn't an empty array.  We can check that
 * for the cases of an array constant and an ARRAY[] construct.
 *
 * "foo op ANY array" is strict in the falseOK sense if the op is strict.
 * If not falseOK, the test is the same as for "foo op ALL array".
 */
static bool
is_strict_saop(ScalarArrayOpExpr *expr, bool falseOK)
{
	Node	   *rightop;

	/* The contained operator must be strict. */
	set_sa_opfuncid(expr);
	if (!func_strict(expr->opfuncid))
		return false;
	/* If ANY and falseOK, that's all we need to check. */
	if (expr->useOr && falseOK)
		return true;
	/* Else, we have to see if the array is provably non-empty. */
	Assert(list_length(expr->args) == 2);
	rightop = (Node *) lsecond(expr->args);
	if (rightop && IsA(rightop, Const))
	{
		Datum		arraydatum = ((Const *) rightop)->constvalue;
		bool		arrayisnull = ((Const *) rightop)->constisnull;
		ArrayType  *arrayval;
		int			nitems;

		if (arrayisnull)
			return false;
		arrayval = DatumGetArrayTypeP(arraydatum);
		nitems = ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval));
		if (nitems > 0)
			return true;
	}
	else if (rightop && IsA(rightop, ArrayExpr))
	{
		ArrayExpr  *arrayexpr = (ArrayExpr *) rightop;

		if (arrayexpr->elements != NIL && !arrayexpr->multidims)
			return true;
	}
	return false;
}


/*****************************************************************************
 *		Check for "pseudo-constant" clauses
 *****************************************************************************/

/*
 * is_pseudo_constant_clause
 *	  Detect whether an expression is "pseudo constant", ie, it contains no
 *	  variables of the current query level and no uses of volatile functions.
 *	  Such an expr is not necessarily a true constant: it can still contain
 *	  Params and outer-level Vars, not to mention functions whose results
 *	  may vary from one statement to the next.  However, the expr's value
 *	  will be constant over any one scan of the current query, so it can be
 *	  used as, eg, an indexscan key.
 *
 * CAUTION: this function omits to test for one very important class of
 * not-constant expressions, namely aggregates (Aggrefs).  In current usage
 * this is only applied to WHERE clauses and so a check for Aggrefs would be
 * a waste of cycles; but be sure to also check contain_agg_clause() if you
 * want to know about pseudo-constness in other contexts.  The same goes
 * for window functions (WindowFuncs).
 */
bool
is_pseudo_constant_clause(Node *clause)
{
	/*
	 * We could implement this check in one recursive scan.  But since the
	 * check for volatile functions is both moderately expensive and unlikely
	 * to fail, it seems better to look for Vars first and only check for
	 * volatile functions if we find no Vars.
	 */
	if (!contain_var_clause(clause) &&
		!contain_volatile_functions(clause))
		return true;
	return false;
}

/*
 * is_pseudo_constant_clause_relids
 *	  Same as above, except caller already has available the var membership
 *	  of the expression; this lets us avoid the contain_var_clause() scan.
 */
bool
is_pseudo_constant_clause_relids(Node *clause, Relids relids)
{
	if (bms_is_empty(relids) &&
		!contain_volatile_functions(clause))
		return true;
	return false;
}


/*****************************************************************************
 *																			 *
 *		General clause-manipulating routines								 *
 *																			 *
 *****************************************************************************/

/*
 * NumRelids
 *		(formerly clause_relids)
 *
 * Returns the number of different relations referenced in 'clause'.
 */
int
NumRelids(Node *clause)
{
	Relids		varnos = pull_varnos(clause);
	int			result = bms_num_members(varnos);

	bms_free(varnos);
	return result;
}

/*
 * CommuteOpExpr: commute a binary operator clause
 *
 * XXX the clause is destructively modified!
 */
void
CommuteOpExpr(OpExpr *clause)
{
	Oid			opoid;
	Node	   *temp;

	/* Sanity checks: caller is at fault if these fail */
	if (!is_opclause(clause) ||
		list_length(clause->args) != 2)
		elog(ERROR, "cannot commute non-binary-operator clause");

	opoid = get_commutator(clause->opno);

	if (!OidIsValid(opoid))
		elog(ERROR, "could not find commutator for operator %u",
			 clause->opno);

	/*
	 * modify the clause in-place!
	 */
	clause->opno = opoid;
	clause->opfuncid = InvalidOid;
	/* opresulttype, opretset, opcollid, inputcollid need not change */

	temp = linitial(clause->args);
	linitial(clause->args) = lsecond(clause->args);
	lsecond(clause->args) = temp;
}

/*
 * CommuteRowCompareExpr: commute a RowCompareExpr clause
 *
 * XXX the clause is destructively modified!
 */
void
CommuteRowCompareExpr(RowCompareExpr *clause)
{
	List	   *newops;
	List	   *temp;
	ListCell   *l;

	/* Sanity checks: caller is at fault if these fail */
	if (!IsA(clause, RowCompareExpr))
		elog(ERROR, "expected a RowCompareExpr");

	/* Build list of commuted operators */
	newops = NIL;
	foreach(l, clause->opnos)
	{
		Oid			opoid = lfirst_oid(l);

		opoid = get_commutator(opoid);
		if (!OidIsValid(opoid))
			elog(ERROR, "could not find commutator for operator %u",
				 lfirst_oid(l));
		newops = lappend_oid(newops, opoid);
	}

	/*
	 * modify the clause in-place!
	 */
	switch (clause->rctype)
	{
		case ROWCOMPARE_LT:
			clause->rctype = ROWCOMPARE_GT;
			break;
		case ROWCOMPARE_LE:
			clause->rctype = ROWCOMPARE_GE;
			break;
		case ROWCOMPARE_GE:
			clause->rctype = ROWCOMPARE_LE;
			break;
		case ROWCOMPARE_GT:
			clause->rctype = ROWCOMPARE_LT;
			break;
		default:
			elog(ERROR, "unexpected RowCompare type: %d",
				 (int) clause->rctype);
			break;
	}

	clause->opnos = newops;

	/*
	 * Note: we need not change the opfamilies list; we assume any btree
	 * opfamily containing an operator will also contain its commutator.
	 * Collations don't change either.
	 */

	temp = clause->largs;
	clause->largs = clause->rargs;
	clause->rargs = temp;
}

/*
 * Helper for eval_const_expressions: check that datatype of an attribute
 * is still what it was when the expression was parsed.  This is needed to
 * guard against improper simplification after ALTER COLUMN TYPE.  (XXX we
 * may well need to make similar checks elsewhere?)
 *
 * rowtypeid may come from a whole-row Var, and therefore it can be a domain
 * over composite, but for this purpose we only care about checking the type
 * of a contained field.
 */
static bool
rowtype_field_matches(Oid rowtypeid, int fieldnum,
					  Oid expectedtype, int32 expectedtypmod,
					  Oid expectedcollation)
{
	TupleDesc	tupdesc;
	Form_pg_attribute attr;

	/* No issue for RECORD, since there is no way to ALTER such a type */
	if (rowtypeid == RECORDOID)
		return true;
	tupdesc = lookup_rowtype_tupdesc_domain(rowtypeid, -1, false);
	if (fieldnum <= 0 || fieldnum > tupdesc->natts)
	{
		ReleaseTupleDesc(tupdesc);
		return false;
	}
	attr = TupleDescAttr(tupdesc, fieldnum - 1);
	if (attr->attisdropped ||
		attr->atttypid != expectedtype ||
		attr->atttypmod != expectedtypmod ||
		attr->attcollation != expectedcollation)
	{
		ReleaseTupleDesc(tupdesc);
		return false;
	}
	ReleaseTupleDesc(tupdesc);
	return true;
}


/*--------------------
 * eval_const_expressions
 *
 * Reduce any recognizably constant subexpressions of the given
 * expression tree, for example "2 + 2" => "4".  More interestingly,
 * we can reduce certain boolean expressions even when they contain
 * non-constant subexpressions: "x OR true" => "true" no matter what
 * the subexpression x is.  (XXX We assume that no such subexpression
 * will have important side-effects, which is not necessarily a good
 * assumption in the presence of user-defined functions; do we need a
 * pg_proc flag that prevents discarding the execution of a function?)
 *
 * We do understand that certain functions may deliver non-constant
 * results even with constant inputs, "nextval()" being the classic
 * example.  Functions that are not marked "immutable" in pg_proc
 * will not be pre-evaluated here, although we will reduce their
 * arguments as far as possible.
 *
 * Whenever a function is eliminated from the expression by means of
 * constant-expression evaluation or inlining, we add the function to
 * root->glob->invalItems.  This ensures the plan is known to depend on
 * such functions, even though they aren't referenced anymore.
 *
 * We assume that the tree has already been type-checked and contains
 * only operators and functions that are reasonable to try to execute.
 *
 * NOTE: "root" can be passed as NULL if the caller never wants to do any
 * Param substitutions nor receive info about inlined functions.
 *
 * NOTE: the planner assumes that this will always flatten nested AND and
 * OR clauses into N-argument form.  See comments in prepqual.c.
 *
 * NOTE: another critical effect is that any function calls that require
 * default arguments will be expanded, and named-argument calls will be
 * converted to positional notation.  The executor won't handle either.
 *--------------------
 */
Node *
eval_const_expressions(PlannerInfo *root, Node *node)
{
	eval_const_expressions_context context;

	if (root)
		context.boundParams = root->glob->boundParams;	/* bound Params */
	else
		context.boundParams = NULL;
	context.root = root;		/* for inlined-function dependencies */
	context.active_fns = NIL;	/* nothing being recursively simplified */
	context.case_val = NULL;	/* no CASE being examined */
	context.estimate = false;	/* safe transformations only */
	return eval_const_expressions_mutator(node, &context);
}

/*--------------------
 * eval_const_expressions_with_params
 * see eval_const_expressions 's comments, this function directly use params as the parameter
 *--------------------
 */
Node *
eval_const_expressions_with_params(ParamListInfo boundParams, Node *node)
{
	eval_const_expressions_context context;
	
	context.boundParams = boundParams;  /* bound Params */
	context.root = NULL;		/* for inlined-function dependencies */
	context.active_fns = NIL;	/* nothing being recursively simplified */
	context.case_val = NULL;	/* no CASE being examined */
	context.estimate = false;	/* safe transformations only */
	return eval_const_expressions_mutator(node, &context);
}

/*--------------------
 * estimate_const_expressions_with_params
 * This function is in essence a more aggressive version of eval_const_expressions_with_params().
 * see estimate_expression_value for more detail.
 *--------------------
 */
Node *
estimate_const_expressions_with_params(ParamListInfo boundParams, Node *node)
{
	eval_const_expressions_context context;

	context.boundParams = boundParams; /* bound Params */
	context.root = NULL;               /* for inlined-function dependencies */
	context.active_fns = NIL;          /* nothing being recursively simplified */
	context.case_val = NULL;           /* no CASE being examined */
	context.estimate = true;           /* safe transformations only */
	return eval_const_expressions_mutator(node, &context);
}

/*--------------------
 * estimate_expression_value
 *
 * This function attempts to estimate the value of an expression for
 * planning purposes.  It is in essence a more aggressive version of
 * eval_const_expressions(): we will perform constant reductions that are
 * not necessarily 100% safe, but are reasonable for estimation purposes.
 *
 * Currently the extra steps that are taken in this mode are:
 * 1. Substitute values for Params, where a bound Param value has been made
 *	  available by the caller of planner(), even if the Param isn't marked
 *	  constant.  This effectively means that we plan using the first supplied
 *	  value of the Param.
 * 2. Fold stable, as well as immutable, functions to constants.
 * 3. Reduce PlaceHolderVar nodes to their contained expressions.
 *--------------------
 */
Node *
estimate_expression_value(PlannerInfo *root, Node *node)
{
	eval_const_expressions_context context;

	if (root != NULL)
		context.boundParams = root->glob->boundParams;	/* bound Params */
	else
		context.boundParams = NULL;
	/* we do not need to mark the plan as depending on inlined functions */
	context.root = NULL;
	context.active_fns = NIL;	/* nothing being recursively simplified */
	context.case_val = NULL;	/* no CASE being examined */
	context.estimate = true;	/* unsafe transformations OK */
	return eval_const_expressions_mutator(node, &context);
}

/* Generic macro for applying evaluate_expr */
#define ece_evaluate_expr(node) \
        ((Node *) evaluate_expr((Expr *) (node), \
                                                        exprType((Node *) (node)), \
                                                        exprTypmod((Node *) (node)), \
                                                        exprCollation((Node *) (node))))

static Node *
eval_const_expressions_mutator(Node *node,
							   eval_const_expressions_context *context)
{
	if (node == NULL)
		return NULL;
	switch (nodeTag(node))
	{
		case T_Param:
			{
				Param	   *param = (Param *) node;
				ParamListInfo paramLI = context->boundParams;

				/* Look to see if we've been given a value for this Param */
				if (param->paramkind == PARAM_EXTERN &&
					paramLI != NULL &&
					param->paramid > 0 &&
					param->paramid <= paramLI->numParams)
				{
					ParamExternData *prm;
					ParamExternData prmdata;

					/*
					 * Give hook a chance in case parameter is dynamic.  Tell
					 * it that this fetch is speculative, so it should avoid
					 * erroring out if parameter is unavailable.
					 */
					if (paramLI->paramFetch != NULL)
						prm = paramLI->paramFetch(paramLI, param->paramid,
												  true, &prmdata);
					else
						prm = &paramLI->params[param->paramid - 1];

                    /*
                     * We don't just check OidIsValid, but insist that the
                     * fetched type match the Param, just in case the hook did
                     * something unexpected.  No need to throw an error here
                     * though; leave that for runtime.
                     */
                    if (OidIsValid(prm->ptype) &&
                        prm->ptype == param->paramtype)
					{
						/* OK to substitute parameter value? */
						if (context->estimate ||
							(prm->pflags & PARAM_FLAG_CONST))
						{
							/*
							 * Return a Const representing the param value.
							 * Must copy pass-by-ref datatypes, since the
							 * Param might be in a memory context
							 * shorter-lived than our output plan should be.
							 */
							int16		typLen;
							bool		typByVal;
							Datum		pval;

							get_typlenbyval(param->paramtype,
											&typLen, &typByVal);
							if (prm->isnull || typByVal)
								pval = prm->value;
							else
								pval = datumCopy(prm->value, typByVal, typLen);
							return (Node *) makeConst(param->paramtype,
													  param->paramtypmod,
													  param->paramcollid,
													  (int) typLen,
													  pval,
													  prm->isnull,
													  typByVal);
						}
					}
				}

				/*
				 * Not replaceable, so just copy the Param (no need to
				 * recurse)
				 */
				return (Node *) copyObject(param);
			}
		case T_WindowFunc:
			{
				WindowFunc *expr = (WindowFunc *) node;
				Oid			funcid = expr->winfnoid;
				List	   *args;
				Expr	   *aggfilter;
				HeapTuple	func_tuple;
				WindowFunc *newexpr;

				/*
				 * We can't really simplify a WindowFunc node, but we mustn't
				 * just fall through to the default processing, because we
				 * have to apply expand_function_arguments to its argument
				 * list.  That takes care of inserting default arguments and
				 * expanding named-argument notation.
				 */
				func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
				if (!HeapTupleIsValid(func_tuple))
					elog(ERROR, "cache lookup failed for function %u", funcid);

				args = expand_function_arguments(expr->args, expr->wintype,
												 func_tuple);

				ReleaseSysCache(func_tuple);

				/* Now, recursively simplify the args (which are a List) */
				args = (List *)
					expression_tree_mutator((Node *) args,
											eval_const_expressions_mutator,
											(void *) context);
				/* ... and the filter expression, which isn't */
				aggfilter = (Expr *)
					eval_const_expressions_mutator((Node *) expr->aggfilter,
												   context);

				/* And build the replacement WindowFunc node */
				newexpr = makeNode(WindowFunc);
				newexpr->winfnoid = expr->winfnoid;
				newexpr->wintype = expr->wintype;
				newexpr->wincollid = expr->wincollid;
				newexpr->inputcollid = expr->inputcollid;
				newexpr->args = args;
				newexpr->aggfilter = aggfilter;
				newexpr->winref = expr->winref;
				newexpr->winstar = expr->winstar;
				newexpr->winagg = expr->winagg;
				newexpr->windistinct = expr->windistinct;
				newexpr->winaggargtype = expr->winaggargtype;
				newexpr->location = expr->location;

				return (Node *) newexpr;
			}
		case T_FuncExpr:
			{
				FuncExpr   *expr = (FuncExpr *) node;
				List	   *args = expr->args;
				Expr	   *simple;
				FuncExpr   *newexpr;

				/*
				 * Code for op/func reduction is pretty bulky, so split it out
				 * as a separate function.  Note: exprTypmod normally returns
				 * -1 for a FuncExpr, but not when the node is recognizably a
				 * length coercion; we want to preserve the typmod in the
				 * eventual Const if so.
				 */
				simple = simplify_function(expr->funcid,
										   expr->funcresulttype,
										   exprTypmod(node),
										   expr->funccollid,
										   expr->inputcollid,
										   &args,
										   expr->funcvariadic,
										   true,
										   true,
										   context
#ifdef _PG_ORCL_
										   , expr->withfuncnsp
										   , expr->withfuncid
#endif
										   );
				if (simple)		/* successfully simplified it */
					return (Node *) simple;

				/*
				 * The expression cannot be simplified any further, so build
				 * and return a replacement FuncExpr node using the
				 * possibly-simplified arguments.  Note that we have also
				 * converted the argument list to positional notation.
				 */
				newexpr = makeNode(FuncExpr);
				newexpr->funcid = expr->funcid;
#ifdef _PG_ORCL_
				newexpr->withfuncid = expr->withfuncid;
				newexpr->withfuncnsp = expr->withfuncnsp;
#endif
				newexpr->funcresulttype = expr->funcresulttype;
				newexpr->funcretset = expr->funcretset;
				newexpr->funcvariadic = expr->funcvariadic;
				newexpr->funcformat = expr->funcformat;
				newexpr->funccollid = expr->funccollid;
				newexpr->inputcollid = expr->inputcollid;
				newexpr->args = args;
				newexpr->location = expr->location;
				return (Node *) newexpr;
			}
		case T_OpExpr:
			{
				OpExpr	   *expr = (OpExpr *) node;
				List	   *args = expr->args;
				Expr	   *simple;
				OpExpr	   *newexpr;

				/*
				 * Need to get OID of underlying function.  Okay to scribble
				 * on input to this extent.
				 */
				set_opfuncid(expr);

				/*
				 * Code for op/func reduction is pretty bulky, so split it out
				 * as a separate function.
				 */
				simple = simplify_function(expr->opfuncid,
										   expr->opresulttype, -1,
										   expr->opcollid,
										   expr->inputcollid,
										   &args,
										   false,
										   true,
										   true,
										   context
#ifdef _PG_ORCL_
										   , NULL
										   , -1
#endif
										   );
				if (simple)		/* successfully simplified it */
					return (Node *) simple;

				/*
				 * If the operator is boolean equality or inequality, we know
				 * how to simplify cases involving one constant and one
				 * non-constant argument.
				 */
				if (expr->opno == BooleanEqualOperator ||
					expr->opno == BooleanNotEqualOperator)
				{
					simple = (Expr *) simplify_boolean_equality(expr->opno,
																args);
					if (simple) /* successfully simplified it */
						return (Node *) simple;
				}

				/*
				 * The expression cannot be simplified any further, so build
				 * and return a replacement OpExpr node using the
				 * possibly-simplified arguments.
				 */
				newexpr = makeNode(OpExpr);
				newexpr->opno = expr->opno;
				newexpr->opfuncid = expr->opfuncid;
				newexpr->opresulttype = expr->opresulttype;
				newexpr->opretset = expr->opretset;
				newexpr->opcollid = expr->opcollid;
				newexpr->inputcollid = expr->inputcollid;
				newexpr->args = args;
				newexpr->location = expr->location;
				return (Node *) newexpr;
			}
		case T_DistinctExpr:
			{
				DistinctExpr *expr = (DistinctExpr *) node;
				List	   *args;
				ListCell   *arg;
				bool		has_null_input = false;
				bool		all_null_input = true;
				bool		has_nonconst_input = false;
				Expr	   *simple;
				DistinctExpr *newexpr;

				/*
				 * Reduce constants in the DistinctExpr's arguments.  We know
				 * args is either NIL or a List node, so we can call
				 * expression_tree_mutator directly rather than recursing to
				 * self.
				 */
				args = (List *) expression_tree_mutator((Node *) expr->args,
														eval_const_expressions_mutator,
														(void *) context);

				/*
				 * We must do our own check for NULLs because DistinctExpr has
				 * different results for NULL input than the underlying
				 * operator does.
				 */
				foreach(arg, args)
				{
					if (IsA(lfirst(arg), Const))
					{
						has_null_input |= ((Const *) lfirst(arg))->constisnull;
						all_null_input &= ((Const *) lfirst(arg))->constisnull;
					}
					else
						has_nonconst_input = true;
				}

				/* all constants? then can optimize this out */
				if (!has_nonconst_input)
				{
					/* all nulls? then not distinct */
					if (all_null_input)
						return makeBoolConst(false, false);

					/* one null? then distinct */
					if (has_null_input)
						return makeBoolConst(true, false);

					/* otherwise try to evaluate the '=' operator */
					/* (NOT okay to try to inline it, though!) */

					/*
					 * Need to get OID of underlying function.  Okay to
					 * scribble on input to this extent.
					 */
					set_opfuncid((OpExpr *) expr);	/* rely on struct
													 * equivalence */

					/*
					 * Code for op/func reduction is pretty bulky, so split it
					 * out as a separate function.
					 */
					simple = simplify_function(expr->opfuncid,
											   expr->opresulttype, -1,
											   expr->opcollid,
											   expr->inputcollid,
											   &args,
											   false,
											   false,
											   false,
											   context
#ifdef _PG_ORCL_
											   , NULL
											   , -1
#endif
											   );
					if (simple) /* successfully simplified it */
					{
						/*
						 * Since the underlying operator is "=", must negate
						 * its result
						 */
						Const	   *csimple = castNode(Const, simple);

						csimple->constvalue =
							BoolGetDatum(!DatumGetBool(csimple->constvalue));
						return (Node *) csimple;
					}
				}

				/*
				 * The expression cannot be simplified any further, so build
				 * and return a replacement DistinctExpr node using the
				 * possibly-simplified arguments.
				 */
				newexpr = makeNode(DistinctExpr);
				newexpr->opno = expr->opno;
				newexpr->opfuncid = expr->opfuncid;
				newexpr->opresulttype = expr->opresulttype;
				newexpr->opretset = expr->opretset;
				newexpr->opcollid = expr->opcollid;
				newexpr->inputcollid = expr->inputcollid;
				newexpr->args = args;
				newexpr->location = expr->location;
				return (Node *) newexpr;
			}
		case T_BoolExpr:
			{
				BoolExpr   *expr = (BoolExpr *) node;

				switch (expr->boolop)
				{
					case OR_EXPR:
						{
							List	   *newargs;
							bool		haveNull = false;
							bool		forceTrue = false;

							newargs = simplify_or_arguments(expr->args,
															context,
															&haveNull,
															&forceTrue);
							if (forceTrue)
								return makeBoolConst(true, false);
							if (haveNull)
								newargs = lappend(newargs,
												  makeBoolConst(false, true));
							/* If all the inputs are FALSE, result is FALSE */
							if (newargs == NIL)
								return makeBoolConst(false, false);

							/*
							 * If only one nonconst-or-NULL input, it's the
							 * result
							 */
							if (list_length(newargs) == 1)
								return (Node *) linitial(newargs);
							/* Else we still need an OR node */
							return (Node *) make_orclause(newargs);
						}
					case AND_EXPR:
						{
							List	   *newargs;
							bool		haveNull = false;
							bool		forceFalse = false;

							newargs = simplify_and_arguments(expr->args,
															 context,
															 &haveNull,
															 &forceFalse);
							if (forceFalse)
								return makeBoolConst(false, false);
							if (haveNull)
								newargs = lappend(newargs,
												  makeBoolConst(false, true));
							/* If all the inputs are TRUE, result is TRUE */
							if (newargs == NIL)
								return makeBoolConst(true, false);

							/*
							 * If only one nonconst-or-NULL input, it's the
							 * result
							 */
							if (list_length(newargs) == 1)
								return (Node *) linitial(newargs);
							/* Else we still need an AND node */
							return (Node *) make_andclause(newargs);
						}
					case NOT_EXPR:
						{
							Node	   *arg;

							Assert(list_length(expr->args) == 1);
							arg = eval_const_expressions_mutator(linitial(expr->args),
																 context);

							/*
							 * Use negate_clause() to see if we can simplify
							 * away the NOT.
							 */
							return negate_clause(arg);
						}
					default:
						elog(ERROR, "unrecognized boolop: %d",
							 (int) expr->boolop);
						break;
				}
				break;
			}
		case T_SubPlan:
		case T_AlternativeSubPlan:

			/*
			 * Return a SubPlan unchanged --- too late to do anything with it.
			 *
			 * XXX should we ereport() here instead?  Probably this routine
			 * should never be invoked after SubPlan creation.
			 */
			return node;
		case T_RelabelType:
			{
				/*
				 * If we can simplify the input to a constant, then we don't
				 * need the RelabelType node anymore: just change the type
				 * field of the Const node.  Otherwise, must copy the
				 * RelabelType node.
				 */
				RelabelType *relabel = (RelabelType *) node;
				Node	   *arg;

				arg = eval_const_expressions_mutator((Node *) relabel->arg,
													 context);

				/*
				 * If we find stacked RelabelTypes (eg, from foo :: int ::
				 * oid) we can discard all but the top one.
				 */
				while (arg && IsA(arg, RelabelType))
					arg = (Node *) ((RelabelType *) arg)->arg;

				if (arg && IsA(arg, Const))
				{
					Const	   *con = (Const *) arg;

					con->consttype = relabel->resulttype;
					con->consttypmod = relabel->resulttypmod;
					con->constcollid = relabel->resultcollid;
					return (Node *) con;
				}
				else
				{
					RelabelType *newrelabel = makeNode(RelabelType);

					newrelabel->arg = (Expr *) arg;
					newrelabel->resulttype = relabel->resulttype;
					newrelabel->resulttypmod = relabel->resulttypmod;
					newrelabel->resultcollid = relabel->resultcollid;
					newrelabel->relabelformat = relabel->relabelformat;
					newrelabel->location = relabel->location;
					return (Node *) newrelabel;
				}
			}
		case T_CoerceViaIO:
			{
				CoerceViaIO *expr = (CoerceViaIO *) node;
				List	   *args;
				Oid			outfunc;
				bool		outtypisvarlena;
				Oid			infunc;
				Oid			intypioparam;
				Expr	   *simple;
				CoerceViaIO *newexpr;
                int32       typmod;
				/* Make a List so we can use simplify_function */
				args = list_make1(expr->arg);

				/*
				 * CoerceViaIO represents calling the source type's output
				 * function then the result type's input function.  So, try to
				 * simplify it as though it were a stack of two such function
				 * calls.  First we need to know what the functions are.
				 *
				 * Note that the coercion functions are assumed not to care
				 * about input collation, so we just pass InvalidOid for that.
				 */
				getTypeOutputInfo(exprType((Node *) expr->arg),
								  &outfunc, &outtypisvarlena);
				getTypeInputInfo(expr->resulttype,
								 &infunc, &intypioparam);

                if (ORA_MODE && expr->resulttype == NUMERICOID)
                    typmod = expr->resulttypmod;
                else
                    typmod = -1;
				simple = simplify_function(outfunc,
										   CSTRINGOID, -1,
										   InvalidOid,
										   InvalidOid,
										   &args,
										   false,
										   true,
										   true,
										   context
#ifdef _PG_ORCL_
											, NULL
											, -1
#endif
										   );
				if (simple)		/* successfully simplified output fn */
				{
					/*
					 * Input functions may want 1 to 3 arguments.  We always
					 * supply all three, trusting that nothing downstream will
					 * complain.
					 */
					args = list_make3(simple,
									  makeConst(OIDOID,
												-1,
												InvalidOid,
												sizeof(Oid),
												ObjectIdGetDatum(intypioparam),
												false,
												true),
									  makeConst(INT4OID,
												-1,
												InvalidOid,
												sizeof(int32),
												Int32GetDatum(typmod),
												false,
												true));

					simple = simplify_function(infunc,
											   expr->resulttype, typmod,
											   expr->resultcollid,
											   InvalidOid,
											   &args,
											   false,
											   false,
											   true,
											   context
#ifdef _PG_ORCL_
												, NULL
												, -1
#endif
											   );
					if (simple) /* successfully simplified input fn */
						return (Node *) simple;
				}

				/*
				 * The expression cannot be simplified any further, so build
				 * and return a replacement CoerceViaIO node using the
				 * possibly-simplified argument.
				 */
				newexpr = makeNode(CoerceViaIO);
				newexpr->arg = (Expr *) linitial(args);
				newexpr->resulttype = expr->resulttype;
				newexpr->resulttypmod = expr->resulttypmod;
				newexpr->resultcollid = expr->resultcollid;
				newexpr->coerceformat = expr->coerceformat;
				newexpr->location = expr->location;
				return (Node *) newexpr;
			}
		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *expr = (ArrayCoerceExpr *) node;
				Expr	   *arg;
				Expr	   *elemexpr;
				ArrayCoerceExpr *newexpr;
				Node	   *save_case_val;

				/*
				 * Reduce constants in the ArrayCoerceExpr's argument and
				 * per-element expressions, then build a new ArrayCoerceExpr.
				 */
				arg = (Expr *) eval_const_expressions_mutator((Node *) expr->arg,
															  context);


				/*
				 * Set up for the CaseTestExpr node contained in the elemexpr.
				 * We must prevent it from absorbing any outer CASE value.
				 */
				save_case_val = context->case_val;
				context->case_val = NULL;
				elemexpr = (Expr *) eval_const_expressions_mutator((Node *) expr->elemexpr,
																   context);
				context->case_val = save_case_val;

				newexpr = makeNode(ArrayCoerceExpr);
				newexpr->arg = arg;
				newexpr->elemexpr = elemexpr;
				newexpr->resulttype = expr->resulttype;
				newexpr->resulttypmod = expr->resulttypmod;
				newexpr->resultcollid = expr->resultcollid;
				newexpr->coerceformat = expr->coerceformat;
				newexpr->location = expr->location;

				/*
				 * If constant argument and per-element expression is
				 * immutable, we can simplify the whole thing to a constant.
				 * Exception: although contain_mutable_functions considers
				 * CoerceToDomain immutable for historical reasons, let's not
				 * do so here; this ensures coercion to an array-over-domain
				 * does not apply the domain's constraints until runtime.
				 */
				if (arg && IsA(arg, Const) &&
					elemexpr && !IsA(elemexpr, CoerceToDomain) &&
					!contain_mutable_functions((Node *) elemexpr))
					return (Node *) evaluate_expr((Expr *) newexpr,
												  newexpr->resulttype,
												  newexpr->resulttypmod,
												  newexpr->resultcollid);

				/* Else we must return the partially-simplified node */
				return (Node *) newexpr;
			}
		case T_CollateExpr:
			{
				/*
				 * If we can simplify the input to a constant, then we don't
				 * need the CollateExpr node at all: just change the
				 * constcollid field of the Const node.  Otherwise, replace
				 * the CollateExpr with a RelabelType. (We do that so as to
				 * improve uniformity of expression representation and thus
				 * simplify comparison of expressions.)
				 */
				CollateExpr *collate = (CollateExpr *) node;
				Node	   *arg;

				arg = eval_const_expressions_mutator((Node *) collate->arg,
													 context);

				if (arg && IsA(arg, Const))
				{
					Const	   *con = (Const *) arg;

					con->constcollid = collate->collOid;
					return (Node *) con;
				}
				else if (collate->collOid == exprCollation(arg))
				{
					/* Don't need a RelabelType either... */
					return arg;
				}
				else
				{
					RelabelType *relabel = makeNode(RelabelType);

					relabel->resulttype = exprType(arg);
					relabel->resulttypmod = exprTypmod(arg);
					relabel->resultcollid = collate->collOid;
					relabel->relabelformat = COERCE_IMPLICIT_CAST;
					relabel->location = collate->location;

					/* Don't create stacked RelabelTypes */
					while (arg && IsA(arg, RelabelType))
						arg = (Node *) ((RelabelType *) arg)->arg;
					relabel->arg = (Expr *) arg;

					return (Node *) relabel;
				}
			}
		case T_CaseExpr:
			{
				/*----------
				 * CASE expressions can be simplified if there are constant
				 * condition clauses:
				 *		FALSE (or NULL): drop the alternative
				 *		TRUE: drop all remaining alternatives
				 * If the first non-FALSE alternative is a constant TRUE,
				 * we can simplify the entire CASE to that alternative's
				 * expression.  If there are no non-FALSE alternatives,
				 * we simplify the entire CASE to the default result (ELSE).
				 *
				 * If we have a simple-form CASE with constant test
				 * expression, we substitute the constant value for contained
				 * CaseTestExpr placeholder nodes, so that we have the
				 * opportunity to reduce constant test conditions.  For
				 * example this allows
				 *		CASE 0 WHEN 0 THEN 1 ELSE 1/0 END
				 * to reduce to 1 rather than drawing a divide-by-0 error.
				 * Note that when the test expression is constant, we don't
				 * have to include it in the resulting CASE; for example
				 *		CASE 0 WHEN x THEN y ELSE z END
				 * is transformed by the parser to
				 *		CASE 0 WHEN CaseTestExpr = x THEN y ELSE z END
				 * which we can simplify to
				 *		CASE WHEN 0 = x THEN y ELSE z END
				 * It is not necessary for the executor to evaluate the "arg"
				 * expression when executing the CASE, since any contained
				 * CaseTestExprs that might have referred to it will have been
				 * replaced by the constant.
				 *----------
				 */
				CaseExpr   *caseexpr = (CaseExpr *) node;
				CaseExpr   *newcase;
				Node	   *save_case_val;
				Node	   *newarg;
				List	   *newargs;
				bool		const_true_cond;
				Node	   *defresult = NULL;
				ListCell   *arg;

				/* Simplify the test expression, if any */
				newarg = eval_const_expressions_mutator((Node *) caseexpr->arg,
														context);

				/* Set up for contained CaseTestExpr nodes */
				save_case_val = context->case_val;
				if (newarg && IsA(newarg, Const))
				{
					context->case_val = newarg;
					newarg = NULL;	/* not needed anymore, see above */
				}
				else
					context->case_val = NULL;

				/* Simplify the WHEN clauses */
				newargs = NIL;
				const_true_cond = false;
				foreach(arg, caseexpr->args)
				{
					CaseWhen   *oldcasewhen = lfirst_node(CaseWhen, arg);
					Node	   *casecond;
					Node	   *caseresult;

					/* Simplify this alternative's test condition */
					casecond = eval_const_expressions_mutator((Node *) oldcasewhen->expr,
															  context);

					/*
					 * If the test condition is constant FALSE (or NULL), then
					 * drop this WHEN clause completely, without processing
					 * the result.
					 */
					if (casecond && IsA(casecond, Const))
					{
						Const	   *const_input = (Const *) casecond;

						if (const_input->constisnull ||
							!DatumGetBool(const_input->constvalue))
							continue;	/* drop alternative with FALSE cond */
						/* Else it's constant TRUE */
						const_true_cond = true;
					}

					/* Simplify this alternative's result value */
					caseresult = eval_const_expressions_mutator((Node *) oldcasewhen->result,
																context);

					/* If non-constant test condition, emit a new WHEN node */
					if (!const_true_cond)
					{
						CaseWhen   *newcasewhen = makeNode(CaseWhen);

						newcasewhen->expr = (Expr *) casecond;
						newcasewhen->result = (Expr *) caseresult;
						newcasewhen->location = oldcasewhen->location;
						newargs = lappend(newargs, newcasewhen);
						continue;
					}

					/*
					 * Found a TRUE condition, so none of the remaining
					 * alternatives can be reached.  We treat the result as
					 * the default result.
					 */
					defresult = caseresult;
					break;
				}

				/* Simplify the default result, unless we replaced it above */
				if (!const_true_cond)
					defresult = eval_const_expressions_mutator((Node *) caseexpr->defresult,
															   context);

				context->case_val = save_case_val;

				/*
				 * If no non-FALSE alternatives, CASE reduces to the default
				 * result
				 */
				if (newargs == NIL)
					return defresult;
				/* Otherwise we need a new CASE node */
				newcase = makeNode(CaseExpr);
				newcase->casetype = caseexpr->casetype;
				newcase->casecollid = caseexpr->casecollid;
				newcase->arg = (Expr *) newarg;
				newcase->args = newargs;
				newcase->defresult = (Expr *) defresult;
				newcase->location = caseexpr->location;
				return (Node *) newcase;
			}
		case T_CaseTestExpr:
			{
				/*
				 * If we know a constant test value for the current CASE
				 * construct, substitute it for the placeholder.  Else just
				 * return the placeholder as-is.
				 */
				if (context->case_val)
					return copyObject(context->case_val);
				else
					return copyObject(node);
			}
		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				ArrayExpr  *newarray;
				bool		all_const = true;
				List	   *newelems;
				ListCell   *element;

				newelems = NIL;
				foreach(element, arrayexpr->elements)
				{
					Node	   *e;

					e = eval_const_expressions_mutator((Node *) lfirst(element),
													   context);
					if (!IsA(e, Const))
						all_const = false;
					newelems = lappend(newelems, e);
				}

				newarray = makeNode(ArrayExpr);
				newarray->array_typeid = arrayexpr->array_typeid;
				newarray->array_collid = arrayexpr->array_collid;
				newarray->element_typeid = arrayexpr->element_typeid;
				newarray->elements = newelems;
				newarray->multidims = arrayexpr->multidims;
				newarray->location = arrayexpr->location;

				if (all_const)
					return (Node *) evaluate_expr((Expr *) newarray,
												  newarray->array_typeid,
												  exprTypmod(node),
												  newarray->array_collid);

				return (Node *) newarray;
			}
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesceexpr = (CoalesceExpr *) node;
				CoalesceExpr *newcoalesce;
				List	   *newargs;
				ListCell   *arg;

				newargs = NIL;
				foreach(arg, coalesceexpr->args)
				{
					Node	   *e;

					e = eval_const_expressions_mutator((Node *) lfirst(arg),
													   context);

					/*
					 * We can remove null constants from the list. For a
					 * non-null constant, if it has not been preceded by any
					 * other non-null-constant expressions then it is the
					 * result. Otherwise, it's the next argument, but we can
					 * drop following arguments since they will never be
					 * reached.
					 */
					if (IsA(e, Const))
					{
						if (((Const *) e)->constisnull)
							continue;	/* drop null constant */
						if (newargs == NIL)
							return e;	/* first expr */
						newargs = lappend(newargs, e);
						break;
					}
					newargs = lappend(newargs, e);
				}

				/*
				 * If all the arguments were constant null, the result is just
				 * null
				 */
				if (newargs == NIL)
					return (Node *) makeNullConst(coalesceexpr->coalescetype,
												  -1,
												  coalesceexpr->coalescecollid);

				newcoalesce = makeNode(CoalesceExpr);
				newcoalesce->coalescetype = coalesceexpr->coalescetype;
				newcoalesce->coalescecollid = coalesceexpr->coalescecollid;
				newcoalesce->args = newargs;
				newcoalesce->location = coalesceexpr->location;
				return (Node *) newcoalesce;
			}
		case T_SQLValueFunction:
			{
				/*
				 * All variants of SQLValueFunction are stable, so if we are
				 * estimating the expression's value, we should evaluate the
				 * current function value.  Otherwise just copy.
				 */
				SQLValueFunction *svf = (SQLValueFunction *) node;

				if (context->estimate)
					return (Node *) evaluate_expr((Expr *) svf,
												  svf->type,
												  svf->typmod,
												  InvalidOid);
				else
				{
					if (IS_PGXC_COORDINATOR && enable_unshippable_function_rewrite &&
							context->root && context->root->glob && !context->root->glob->has_val_func)
					{
						context->root->glob->has_val_func = true;
					}

					return copyObject((Node *) svf);
				}
			}
		case T_FieldSelect:
			{
				/*
				 * We can optimize field selection from a whole-row Var into a
				 * simple Var.  (This case won't be generated directly by the
				 * parser, because ParseComplexProjection short-circuits it.
				 * But it can arise while simplifying functions.)  Also, we
				 * can optimize field selection from a RowExpr construct.
				 *
				 * However, replacing a whole-row Var in this way has a
				 * pitfall: if we've already built the rel targetlist for the
				 * source relation, then the whole-row Var is scheduled to be
				 * produced by the relation scan, but the simple Var probably
				 * isn't, which will lead to a failure in setrefs.c.  This is
				 * not a problem when handling simple single-level queries, in
				 * which expression simplification always happens first.  It
				 * is a risk for lateral references from subqueries, though.
				 * To avoid such failures, don't optimize uplevel references.
				 *
				 * We must also check that the declared type of the field is
				 * still the same as when the FieldSelect was created --- this
				 * can change if someone did ALTER COLUMN TYPE on the rowtype.
				 */
				FieldSelect *fselect = (FieldSelect *) node;
				FieldSelect *newfselect;
				Node	   *arg;

				arg = eval_const_expressions_mutator((Node *) fselect->arg,
													 context);
				if (arg && IsA(arg, Var) &&
					((Var *) arg)->varattno == InvalidAttrNumber &&
					((Var *) arg)->varlevelsup == 0)
				{
					if (rowtype_field_matches(((Var *) arg)->vartype,
											  fselect->fieldnum,
											  fselect->resulttype,
											  fselect->resulttypmod,
											  fselect->resultcollid))
						return (Node *) makeVar(((Var *) arg)->varno,
												fselect->fieldnum,
												fselect->resulttype,
												fselect->resulttypmod,
												fselect->resultcollid,
												((Var *) arg)->varlevelsup);
				}
				if (arg && IsA(arg, RowExpr))
				{
					RowExpr    *rowexpr = (RowExpr *) arg;

					if (fselect->fieldnum > 0 &&
						fselect->fieldnum <= list_length(rowexpr->args))
					{
						Node	   *fld = (Node *) list_nth(rowexpr->args,
															fselect->fieldnum - 1);

						if (rowtype_field_matches(rowexpr->row_typeid,
												  fselect->fieldnum,
												  fselect->resulttype,
												  fselect->resulttypmod,
												  fselect->resultcollid) &&
							fselect->resulttype == exprType(fld) &&
							fselect->resulttypmod == exprTypmod(fld) &&
							fselect->resultcollid == exprCollation(fld))
							return fld;
					}
				}
				newfselect = makeNode(FieldSelect);
				newfselect->arg = (Expr *) arg;
				newfselect->fieldnum = fselect->fieldnum;
				newfselect->resulttype = fselect->resulttype;
				newfselect->resulttypmod = fselect->resulttypmod;
				newfselect->resultcollid = fselect->resultcollid;
				return (Node *) newfselect;
			}
		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;
				NullTest   *newntest;
				Node	   *arg;

				arg = eval_const_expressions_mutator((Node *) ntest->arg,
													 context);
				if (ntest->argisrow && arg && IsA(arg, RowExpr))
				{
					/*
					 * We break ROW(...) IS [NOT] NULL into separate tests on
					 * its component fields.  This form is usually more
					 * efficient to evaluate, as well as being more amenable
					 * to optimization.
					 */
					RowExpr    *rarg = (RowExpr *) arg;
					List	   *newargs = NIL;
					ListCell   *l;

					foreach(l, rarg->args)
					{
						Node	   *relem = (Node *) lfirst(l);

						/*
						 * A constant field refutes the whole NullTest if it's
						 * of the wrong nullness; else we can discard it.
						 */
						if (relem && IsA(relem, Const))
						{
							Const	   *carg = (Const *) relem;

							if (carg->constisnull ?
								(ntest->nulltesttype == IS_NOT_NULL) :
								(ntest->nulltesttype == IS_NULL))
								return makeBoolConst(false, false);
							continue;
						}

						/*
						 * Else, make a scalar (argisrow == false) NullTest
						 * for this field.  Scalar semantics are required
						 * because IS [NOT] NULL doesn't recurse; see comments
						 * in ExecEvalRowNullInt().
						 */
						newntest = makeNode(NullTest);
						newntest->arg = (Expr *) relem;
						newntest->nulltesttype = ntest->nulltesttype;
						newntest->argisrow = false;
						newntest->location = ntest->location;
						newargs = lappend(newargs, newntest);
					}
					/* If all the inputs were constants, result is TRUE */
					if (newargs == NIL)
						return makeBoolConst(true, false);
					/* If only one nonconst input, it's the result */
					if (list_length(newargs) == 1)
						return (Node *) linitial(newargs);
					/* Else we need an AND node */
					return (Node *) make_andclause(newargs);
				}
				if (!ntest->argisrow && arg && IsA(arg, Const))
				{
					Const	   *carg = (Const *) arg;
					bool		result;

					switch (ntest->nulltesttype)
					{
						case IS_NULL:
							result = carg->constisnull;
							break;
						case IS_NOT_NULL:
							result = !carg->constisnull;
							break;
						default:
							elog(ERROR, "unrecognized nulltesttype: %d",
								 (int) ntest->nulltesttype);
							result = false; /* keep compiler quiet */
							break;
					}

					return makeBoolConst(result, false);
				}

				newntest = makeNode(NullTest);
				newntest->arg = (Expr *) arg;
				newntest->nulltesttype = ntest->nulltesttype;
				newntest->argisrow = ntest->argisrow;
				newntest->location = ntest->location;
				return (Node *) newntest;
			}
		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;
				BooleanTest *newbtest;
				Node	   *arg;

				arg = eval_const_expressions_mutator((Node *) btest->arg,
													 context);
				if (arg && IsA(arg, Const))
				{
					Const	   *carg = (Const *) arg;
					bool		result;

					switch (btest->booltesttype)
					{
						case IS_TRUE:
							result = (!carg->constisnull &&
									  DatumGetBool(carg->constvalue));
							break;
						case IS_NOT_TRUE:
							result = (carg->constisnull ||
									  !DatumGetBool(carg->constvalue));
							break;
						case IS_FALSE:
							result = (!carg->constisnull &&
									  !DatumGetBool(carg->constvalue));
							break;
						case IS_NOT_FALSE:
							result = (carg->constisnull ||
									  DatumGetBool(carg->constvalue));
							break;
						case IS_UNKNOWN:
							result = carg->constisnull;
							break;
						case IS_NOT_UNKNOWN:
							result = !carg->constisnull;
							break;
						default:
							elog(ERROR, "unrecognized booltesttype: %d",
								 (int) btest->booltesttype);
							result = false; /* keep compiler quiet */
							break;
					}

					return makeBoolConst(result, false);
				}

				newbtest = makeNode(BooleanTest);
				newbtest->arg = (Expr *) arg;
				newbtest->booltesttype = btest->booltesttype;
				newbtest->location = btest->location;
				return (Node *) newbtest;
			}
		case T_PlaceHolderVar:

			/*
			 * In estimation mode, just strip the PlaceHolderVar node
			 * altogether; this amounts to estimating that the contained value
			 * won't be forced to null by an outer join.  In regular mode we
			 * just use the default behavior (ie, simplify the expression but
			 * leave the PlaceHolderVar node intact).
			 */
			if (context->estimate)
			{
				PlaceHolderVar *phv = (PlaceHolderVar *) node;

				return eval_const_expressions_mutator((Node *) phv->phexpr,
													  context);
			}
			break;
		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *cre = castNode(ConvertRowtypeExpr, node);
				Node		   *arg;
				ConvertRowtypeExpr *newcre;

				arg = eval_const_expressions_mutator((Node *) cre->arg,
													 context);

				newcre = makeNode(ConvertRowtypeExpr);
				newcre->resulttype = cre->resulttype;
				newcre->convertformat = cre->convertformat;
				newcre->location = cre->location;

				/*
				 * In case of a nested ConvertRowtypeExpr, we can convert the
				 * leaf row directly to the topmost row format without any
				 * intermediate conversions. (This works because
				 * ConvertRowtypeExpr is used only for child->parent
				 * conversion in inheritance trees, which works by exact match
				 * of column name, and a column absent in an intermediate
				 * result can't be present in the final result.)
				 *
				 * No need to check more than one level deep, because the
				 * above recursion will have flattened anything else.
				 */
				if (arg != NULL && IsA(arg, ConvertRowtypeExpr))
				{
					ConvertRowtypeExpr *argcre = (ConvertRowtypeExpr *) arg;

					arg = (Node *) argcre->arg;

					/*
					 * Make sure an outer implicit conversion can't hide an
					 * inner explicit one.
					 */
					if (newcre->convertformat == COERCE_IMPLICIT_CAST)
						newcre->convertformat = argcre->convertformat;
				}

				newcre->arg = (Expr *) arg;

				if (arg != NULL && IsA(arg, Const))
					return ece_evaluate_expr((Node *) newcre);
				return (Node *) newcre;
			}
		default:
			break;
	}

	/*
	 * For any node type not handled above, we recurse using
	 * expression_tree_mutator, which will copy the node unchanged but try to
	 * simplify its arguments (if any) using this routine. For example: we
	 * cannot eliminate an ArrayRef node, but we might be able to simplify
	 * constant expressions in its subscripts.
	 */
	return expression_tree_mutator(node, eval_const_expressions_mutator,
								   (void *) context);
}

/*
 * Subroutine for eval_const_expressions: process arguments of an OR clause
 *
 * This includes flattening of nested ORs as well as recursion to
 * eval_const_expressions to simplify the OR arguments.
 *
 * After simplification, OR arguments are handled as follows:
 *		non constant: keep
 *		FALSE: drop (does not affect result)
 *		TRUE: force result to TRUE
 *		NULL: keep only one
 * We must keep one NULL input because OR expressions evaluate to NULL when no
 * input is TRUE and at least one is NULL.  We don't actually include the NULL
 * here, that's supposed to be done by the caller.
 *
 * The output arguments *haveNull and *forceTrue must be initialized FALSE
 * by the caller.  They will be set TRUE if a null constant or true constant,
 * respectively, is detected anywhere in the argument list.
 */
static List *
simplify_or_arguments(List *args,
					  eval_const_expressions_context *context,
					  bool *haveNull, bool *forceTrue)
{
	List	   *newargs = NIL;
	List	   *unprocessed_args;

	/*
	 * We want to ensure that any OR immediately beneath another OR gets
	 * flattened into a single OR-list, so as to simplify later reasoning.
	 *
	 * To avoid stack overflow from recursion of eval_const_expressions, we
	 * resort to some tenseness here: we keep a list of not-yet-processed
	 * inputs, and handle flattening of nested ORs by prepending to the to-do
	 * list instead of recursing.  Now that the parser generates N-argument
	 * ORs from simple lists, this complexity is probably less necessary than
	 * it once was, but we might as well keep the logic.
	 */
	unprocessed_args = list_copy(args);
	while (unprocessed_args)
	{
		Node	   *arg = (Node *) linitial(unprocessed_args);

		unprocessed_args = list_delete_first(unprocessed_args);

		/* flatten nested ORs as per above comment */
		if (or_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			/* overly tense code to avoid leaking unused list header */
			if (!unprocessed_args)
				unprocessed_args = subargs;
			else
			{
				List	   *oldhdr = unprocessed_args;

				unprocessed_args = list_concat(subargs, unprocessed_args);
				pfree(oldhdr);
			}
			continue;
		}

		/* If it's not an OR, simplify it */
		arg = eval_const_expressions_mutator(arg, context);

		/*
		 * It is unlikely but not impossible for simplification of a non-OR
		 * clause to produce an OR.  Recheck, but don't be too tense about it
		 * since it's not a mainstream case. In particular we don't worry
		 * about const-simplifying the input twice.
		 */
		if (or_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			unprocessed_args = list_concat(subargs, unprocessed_args);
			continue;
		}

		/*
		 * OK, we have a const-simplified non-OR argument.  Process it per
		 * comments above.
		 */
		if (IsA(arg, Const))
		{
			Const	   *const_input = (Const *) arg;

			if (const_input->constisnull)
				*haveNull = true;
			else if (DatumGetBool(const_input->constvalue))
			{
				*forceTrue = true;

				/*
				 * Once we detect a TRUE result we can just exit the loop
				 * immediately.  However, if we ever add a notion of
				 * non-removable functions, we'd need to keep scanning.
				 */
				return NIL;
			}
			/* otherwise, we can drop the constant-false input */
			continue;
		}

		/* else emit the simplified arg into the result list */
		newargs = lappend(newargs, arg);
	}

	return newargs;
}

/*
 * Subroutine for eval_const_expressions: process arguments of an AND clause
 *
 * This includes flattening of nested ANDs as well as recursion to
 * eval_const_expressions to simplify the AND arguments.
 *
 * After simplification, AND arguments are handled as follows:
 *		non constant: keep
 *		TRUE: drop (does not affect result)
 *		FALSE: force result to FALSE
 *		NULL: keep only one
 * We must keep one NULL input because AND expressions evaluate to NULL when
 * no input is FALSE and at least one is NULL.  We don't actually include the
 * NULL here, that's supposed to be done by the caller.
 *
 * The output arguments *haveNull and *forceFalse must be initialized FALSE
 * by the caller.  They will be set TRUE if a null constant or false constant,
 * respectively, is detected anywhere in the argument list.
 */
static List *
simplify_and_arguments(List *args,
					   eval_const_expressions_context *context,
					   bool *haveNull, bool *forceFalse)
{
	List	   *newargs = NIL;
	List	   *unprocessed_args;

	/* See comments in simplify_or_arguments */
	unprocessed_args = list_copy(args);
	while (unprocessed_args)
	{
		Node	   *arg = (Node *) linitial(unprocessed_args);

		unprocessed_args = list_delete_first(unprocessed_args);

		/* flatten nested ANDs as per above comment */
		if (and_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			/* overly tense code to avoid leaking unused list header */
			if (!unprocessed_args)
				unprocessed_args = subargs;
			else
			{
				List	   *oldhdr = unprocessed_args;

				unprocessed_args = list_concat(subargs, unprocessed_args);
				pfree(oldhdr);
			}
			continue;
		}

		/* If it's not an AND, simplify it */
		arg = eval_const_expressions_mutator(arg, context);

		/*
		 * It is unlikely but not impossible for simplification of a non-AND
		 * clause to produce an AND.  Recheck, but don't be too tense about it
		 * since it's not a mainstream case. In particular we don't worry
		 * about const-simplifying the input twice.
		 */
		if (and_clause(arg))
		{
			List	   *subargs = list_copy(((BoolExpr *) arg)->args);

			unprocessed_args = list_concat(subargs, unprocessed_args);
			continue;
		}

		/*
		 * OK, we have a const-simplified non-AND argument.  Process it per
		 * comments above.
		 */
		if (IsA(arg, Const))
		{
			Const	   *const_input = (Const *) arg;

			if (const_input->constisnull)
				*haveNull = true;
			else if (!DatumGetBool(const_input->constvalue))
			{
				*forceFalse = true;

				/*
				 * Once we detect a FALSE result we can just exit the loop
				 * immediately.  However, if we ever add a notion of
				 * non-removable functions, we'd need to keep scanning.
				 */
				return NIL;
			}
			/* otherwise, we can drop the constant-true input */
			continue;
		}

		/* else emit the simplified arg into the result list */
		newargs = lappend(newargs, arg);
	}

	return newargs;
}

/*
 * Subroutine for eval_const_expressions: try to simplify boolean equality
 * or inequality condition
 *
 * Inputs are the operator OID and the simplified arguments to the operator.
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the expression.
 *
 * The idea here is to reduce "x = true" to "x" and "x = false" to "NOT x",
 * or similarly "x <> true" to "NOT x" and "x <> false" to "x".
 * This is only marginally useful in itself, but doing it in constant folding
 * ensures that we will recognize these forms as being equivalent in, for
 * example, partial index matching.
 *
 * We come here only if simplify_function has failed; therefore we cannot
 * see two constant inputs, nor a constant-NULL input.
 */
static Node *
simplify_boolean_equality(Oid opno, List *args)
{
	Node	   *leftop;
	Node	   *rightop;

	Assert(list_length(args) == 2);
	leftop = linitial(args);
	rightop = lsecond(args);
	if (leftop && IsA(leftop, Const))
	{
		Assert(!((Const *) leftop)->constisnull);
		if (opno == BooleanEqualOperator)
		{
			if (DatumGetBool(((Const *) leftop)->constvalue))
				return rightop; /* true = foo */
			else
				return negate_clause(rightop);	/* false = foo */
		}
		else
		{
			if (DatumGetBool(((Const *) leftop)->constvalue))
				return negate_clause(rightop);	/* true <> foo */
			else
				return rightop; /* false <> foo */
		}
	}
	if (rightop && IsA(rightop, Const))
	{
		Assert(!((Const *) rightop)->constisnull);
		if (opno == BooleanEqualOperator)
		{
			if (DatumGetBool(((Const *) rightop)->constvalue))
				return leftop;	/* foo = true */
			else
				return negate_clause(leftop);	/* foo = false */
		}
		else
		{
			if (DatumGetBool(((Const *) rightop)->constvalue))
				return negate_clause(leftop);	/* foo <> true */
			else
				return leftop;	/* foo <> false */
		}
	}
	return NULL;
}

/*
 * Subroutine for eval_const_expressions: try to simplify a function call
 * (which might originally have been an operator; we don't care)
 *
 * Inputs are the function OID, actual result type OID (which is needed for
 * polymorphic functions), result typmod, result collation, the input
 * collation to use for the function, the original argument list (not
 * const-simplified yet, unless process_args is false), and some flags;
 * also the context data for eval_const_expressions.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function call.
 *
 * This function is also responsible for converting named-notation argument
 * lists into positional notation and/or adding any needed default argument
 * expressions; which is a bit grotty, but it avoids extra fetches of the
 * function's pg_proc tuple.  For this reason, the args list is
 * pass-by-reference.  Conversion and const-simplification of the args list
 * will be done even if simplification of the function call itself is not
 * possible.
 */
static Expr *
simplify_function(Oid funcid, Oid result_type, int32 result_typmod,
				  Oid result_collid, Oid input_collid, List **args_p,
				  bool funcvariadic, bool process_args, bool allow_non_const,
				  eval_const_expressions_context *context
#ifdef _PG_ORCL_
				  , List *with_funcs, int with_funcid
#endif
				  )
{
	List	   *args = *args_p;
	HeapTuple	func_tuple;
	Form_pg_proc func_form;
	Expr	   *newexpr;
	ResourceOwner oldowner;

	/*
	 * We have three strategies for simplification: execute the function to
	 * deliver a constant result, use a transform function to generate a
	 * substitute node tree, or expand in-line the body of the function
	 * definition (which only works for simple SQL-language functions, but
	 * that is a common case).  Each case needs access to the function's
	 * pg_proc tuple, so fetch it just once.
	 *
	 * Note: the allow_non_const flag suppresses both the second and third
	 * strategies; so if !allow_non_const, simplify_function can only return a
	 * Const or NULL.  Argument-list rewriting happens anyway, though.
	 */
#ifdef _PG_ORCL_
	if (with_funcs != NULL)
		func_tuple = GetWithFunctionTupleById(with_funcs, with_funcid);
	else
	{
#endif
		oldowner = CurrentResourceOwner;
		func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
		if (!HeapTupleIsValid(func_tuple))
			elog(ERROR, "cache lookup failed for function %u", funcid);
#ifdef _PG_ORCL_
	}
#endif
	func_form = (Form_pg_proc) GETSTRUCT(func_tuple);

	/*
	 * Process the function arguments, unless the caller did it already.
	 *
	 * Here we must deal with named or defaulted arguments, and then
	 * recursively apply eval_const_expressions to the whole argument list.
	 */
	if (process_args)
	{
		args = expand_function_arguments(args, result_type, func_tuple);
		args = (List *) expression_tree_mutator((Node *) args,
												eval_const_expressions_mutator,
												(void *) context);
		/* Argument processing done, give it back to the caller */
		*args_p = args;
	}

	/* Now attempt simplification of the function call proper. */

	newexpr = evaluate_function(funcid, result_type, result_typmod,
								result_collid, input_collid,
								args, funcvariadic,
								func_tuple, context
#ifdef _PG_ORCL_
								, with_funcs, with_funcid
#endif
								);

	if (!newexpr && allow_non_const && OidIsValid(func_form->protransform))
	{
		/*
		 * Build a dummy FuncExpr node containing the simplified arg list.  We
		 * use this approach to present a uniform interface to the transform
		 * function regardless of how the function is actually being invoked.
		 */
		FuncExpr	fexpr;

		fexpr.xpr.type = T_FuncExpr;
		fexpr.funcid = funcid;
#ifdef _PG_ORCL_
		fexpr.withfuncnsp = with_funcs;
		fexpr.withfuncid = with_funcid;
#endif
		fexpr.funcresulttype = result_type;
		fexpr.funcretset = func_form->proretset;
		fexpr.funcvariadic = funcvariadic;
		fexpr.funcformat = COERCE_EXPLICIT_CALL;
		fexpr.funccollid = result_collid;
		fexpr.inputcollid = input_collid;
		fexpr.args = args;
		fexpr.location = -1;

		newexpr = (Expr *)
			DatumGetPointer(OidFunctionCall1(func_form->protransform,
											 PointerGetDatum(&fexpr)));
	}

	if (!newexpr && allow_non_const
#ifdef _PG_ORCL_
		&& with_funcs == NULL
#endif
		)
		newexpr = inline_function(funcid, result_type, result_collid,
								  input_collid, args, funcvariadic,
								  func_tuple, context);

#ifdef _PG_ORCL_
	if (with_funcs == NULL)
	{
#endif
		CurrentResourceOwner = oldowner;
		ReleaseSysCache(func_tuple);
#ifdef _PG_ORCL_
	}
#endif

	return newexpr;
}

/*
 * expand_function_arguments: convert named-notation args to positional args
 * and/or insert default args, as needed
 *
 * If we need to change anything, the input argument list is copied, not
 * modified.
 *
 * Note: this gets applied to operator argument lists too, even though the
 * cases it handles should never occur there.  This should be OK since it
 * will fall through very quickly if there's nothing to do.
 */
List *
expand_function_arguments(List *args, Oid result_type, HeapTuple func_tuple)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	bool		has_named_args = false;
	ListCell   *lc;

	/* Do we have any named arguments? */
	foreach(lc, args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		if (IsA(arg, NamedArgExpr))
		{
			has_named_args = true;
			break;
		}
	}

	/* If so, we must apply reorder_function_arguments */
	if (has_named_args)
	{
		args = reorder_function_arguments(args, func_tuple);
		/* Recheck argument types and add casts if needed */
		recheck_cast_function_args(args, result_type, func_tuple);
	}
	else if (list_length(args) < funcform->pronargs)
	{
		/* No named args, but we seem to be short some defaults */
		args = add_function_defaults(args, func_tuple);
		/* Recheck argument types and add casts if needed */
		recheck_cast_function_args(args, result_type, func_tuple);
	}

	return args;
}

/*
 * reorder_function_arguments: convert named-notation args to positional args
 *
 * This function also inserts default argument values as needed, since it's
 * impossible to form a truly valid positional call without that.
 */
static List *
reorder_function_arguments(List *args, HeapTuple func_tuple)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	int			pronargs = funcform->pronargs;
	int			nargsprovided = list_length(args);
	Node	   *argarray[FUNC_MAX_ARGS];
	ListCell   *lc;
	int			i;

	Assert(nargsprovided <= pronargs);
	if (pronargs > FUNC_MAX_ARGS)
		elog(ERROR, "too many function arguments");
	memset(argarray, 0, pronargs * sizeof(Node *));

	/* Deconstruct the argument list into an array indexed by argnumber */
	i = 0;
	foreach(lc, args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		if (!IsA(arg, NamedArgExpr))
		{
			/* positional argument, assumed to precede all named args */
			Assert(argarray[i] == NULL);
			argarray[i++] = arg;
		}
		else
		{
			NamedArgExpr *na = (NamedArgExpr *) arg;

			Assert(argarray[na->argnumber] == NULL);
			argarray[na->argnumber] = (Node *) na->arg;
		}
	}

	/*
	 * Fetch default expressions, if needed, and insert into array at proper
	 * locations (they aren't necessarily consecutive or all used)
	 */
	if (nargsprovided < pronargs)
	{
		bool		expandDefaults = false;
		List	   *defaults = fetch_function_defaults(func_tuple);

		if (ORA_MODE)
		{
			foreach(lc, defaults)
			{
				if (lfirst(lc) == NIL)
				{
					expandDefaults = true;
					break;
				}
			}

			/* 
			 * If Nil exists, it must be extended proargdefaults.
			 * If Nil does not exist:
			 *   1. Compatible with old compact proargdefaults.
			 *   2. Use extended proargdefaults, but all parameters have default values, 
			 *      equivalent to compact proargdefaults.
			 */
			if (expandDefaults)
			{
				Datum		proargmodes;
				ArrayType  *arr = NULL;
				bool		isNull = true;
				char		*p_argmodes = NULL;
					
				proargmodes = SysCacheGetAttr(PROCOID, func_tuple, Anum_pg_proc_proargmodes, &isNull);
				if (!isNull)
				{
					arr = DatumGetArrayTypeP(proargmodes);	/* ensure not toasted */

					p_argmodes = (char *) palloc(pronargs * sizeof(char));
					memcpy(p_argmodes, ARR_DATA_PTR(arr), pronargs * sizeof(char));
				}

				i = 0;
				foreach(lc, defaults)
				{
					/* ignore out parameter if running in opentenbase_ora mode */
					if (p_argmodes != NULL && i < pronargs && p_argmodes[i] == FUNC_PARAM_OUT)
					{
						i++;
						continue;
					}

					if (argarray[i] == NULL && lfirst(lc) != NIL)
						argarray[i] = (Node *) lfirst(lc);
					else if (argarray[i] == NULL && lfirst(lc) == NIL)
						elog(ERROR, "missing parameter at position %d", i + 1);
					
					i++;
				}

				if (p_argmodes)
					pfree(p_argmodes);
			}
		}

		/* in pg mode */
		if (!expandDefaults)
		{
			i = pronargs - funcform->pronargdefaults;
			foreach(lc, defaults)
			{
				if (argarray[i] == NULL)
					argarray[i] = (Node *) lfirst(lc);
				i++;
			}
		}
	}

	/* Now reconstruct the args list in proper order */
	args = NIL;
	for (i = 0; i < pronargs; i++)
	{
		Assert(argarray[i] != NULL);
		args = lappend(args, argarray[i]);
	}

	return args;
}

/*
 * add_function_defaults: add missing function arguments from its defaults
 *
 * This is used only when the argument list was positional to begin with,
 * and so we know we just need to add defaults at the end.
 */
static List *
add_function_defaults(List *args, HeapTuple func_tuple)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	int			nargsprovided = list_length(args);
	List	   *defaults;
	int			ndelete;

	/* Get all the default expressions from the pg_proc tuple */
	defaults = fetch_function_defaults(func_tuple);

	/* Delete any unused defaults from the list */
	ndelete = nargsprovided + list_length(defaults) - funcform->pronargs;
	if (ndelete < 0)
		elog(ERROR, "not enough default arguments");
	while (ndelete-- > 0)
		defaults = list_delete_first(defaults);

	if (ORA_MODE)
	{
		ListCell	*lc;

		foreach(lc, defaults)
		{
			if (lfirst(lc) == NIL)
				elog(ERROR, "not enough default arguments");
		}
	}

	/* And form the combined argument list, not modifying the input list */
	return list_concat(list_copy(args), defaults);
}

/*
 * fetch_function_defaults: get function's default arguments as expression list
 */
static List *
fetch_function_defaults(HeapTuple func_tuple)
{
	List	   *defaults;
	Datum		proargdefaults;
	bool		isnull;
	char	   *str;

	/* The error cases here shouldn't happen, but check anyway */
	proargdefaults = SysCacheGetAttr(PROCOID, func_tuple,
									 Anum_pg_proc_proargdefaults,
									 &isnull);
	if (isnull)
		elog(ERROR, "not enough default arguments");
	str = TextDatumGetCString(proargdefaults);
	defaults = castNode(List, stringToNode(str));
	pfree(str);
	return defaults;
}

/*
 * recheck_cast_function_args: recheck function args and typecast as needed
 * after adding defaults.
 *
 * It is possible for some of the defaulted arguments to be polymorphic;
 * therefore we can't assume that the default expressions have the correct
 * data types already.  We have to re-resolve polymorphics and do coercion
 * just like the parser did.
 *
 * This should be a no-op if there are no polymorphic arguments,
 * but we do it anyway to be sure.
 *
 * Note: if any casts are needed, the args list is modified in-place;
 * caller should have already copied the list structure.
 */
static void
recheck_cast_function_args(List *args, Oid result_type, HeapTuple func_tuple)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	int			nargs;
	Oid			actual_arg_types[FUNC_MAX_ARGS];
	Oid			declared_arg_types[FUNC_MAX_ARGS];
	Oid			rettype;
	ListCell   *lc;

	if (list_length(args) > FUNC_MAX_ARGS)
		elog(ERROR, "too many function arguments");
	nargs = 0;
	foreach(lc, args)
	{
		actual_arg_types[nargs++] = exprType((Node *) lfirst(lc));
	}
	Assert(nargs == funcform->pronargs);
	memcpy(declared_arg_types, funcform->proargtypes.values,
		   funcform->pronargs * sizeof(Oid));
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   nargs,
											   funcform->prorettype,
											   false);
	/* let's just check we got the same answer as the parser did ... */
	if (rettype != result_type)
		elog(ERROR, "function's resolved result type changed during planning");

	/* perform any necessary typecasting of arguments */
	make_fn_arguments(NULL, args, actual_arg_types, declared_arg_types);
}

static inline bool
function_cannot_pre_evaluate(Form_pg_proc funcform, eval_const_expressions_context *context)
{
	/*
	 * Check if the current function is _st_expand in postgis. This
	 * function is special. If SQL is pushed down, the function
	 * cannot be pre-calculated in CN.
	 */
	if (IS_PGXC_COORDINATOR &&
		funcform &&
		context->root &&
		context->root->parse &&
		context->root->parse->rtable &&
		funcform->prolang == ClanguageId &&
		strncmp(NameStr(funcform->proname), "_st_expand", 10) == 0)
		return true;

	return false;
}

/*
 * evaluate_function: try to pre-evaluate a function call
 *
 * We can do this if the function is strict and has any constant-null inputs
 * (just return a null constant), or if the function is immutable and has all
 * constant inputs (call it and return the result as a Const node).  In
 * estimation mode we are willing to pre-evaluate stable functions too.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function.
 */
static Expr *
evaluate_function(Oid funcid, Oid result_type, int32 result_typmod,
				  Oid result_collid, Oid input_collid, List *args,
				  bool funcvariadic,
				  HeapTuple func_tuple,
				  eval_const_expressions_context *context
#ifdef _PG_ORCL_
				  , List *with_funcs, int with_funcid
#endif
				  )
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	bool		has_nonconst_input = false;
	bool		has_null_input = false;
	ListCell   *arg;
	FuncExpr   *newexpr;

	/*
	 * Can't simplify if it returns a set.
	 */
	if (funcform->proretset)
		return NULL;

	/*
	 * Functions that cannot be precomputed should be filtered.
	 */
	if (function_cannot_pre_evaluate(funcform, context))
		return NULL;

	/*
	 * Can't simplify if it returns RECORD.  The immediate problem is that it
	 * will be needing an expected tupdesc which we can't supply here.
	 *
	 * In the case where it has OUT parameters, it could get by without an
	 * expected tupdesc, but we still have issues: get_expr_result_type()
	 * doesn't know how to extract type info from a RECORD constant, and in
	 * the case of a NULL function result there doesn't seem to be any clean
	 * way to fix that.  In view of the likelihood of there being still other
	 * gotchas, seems best to leave the function call unreduced.
	 */
	if (funcform->prorettype == RECORDOID)
		return NULL;

	/*
	 * Check for constant inputs and especially constant-NULL inputs.
	 */
	foreach(arg, args)
	{
		if (IsA(lfirst(arg), Const))
			has_null_input |= ((Const *) lfirst(arg))->constisnull;
		else
			has_nonconst_input = true;
	}

	/*
	 * If the function is strict and has a constant-NULL input, it will never
	 * be called at all, so we can replace the call by a NULL constant, even
	 * if there are other inputs that aren't constant, and even if the
	 * function is not otherwise immutable.
	 */
	if (funcform->proisstrict && has_null_input)
		return (Expr *) makeNullConst(result_type, result_typmod,
									  result_collid);

	/*
	 * Otherwise, can simplify only if all inputs are constants. (For a
	 * non-strict function, constant NULL inputs are treated the same as
	 * constant non-NULL inputs.)
	 */
	if (has_nonconst_input)
		return NULL;

	/*
	 * Ordinarily we are only allowed to simplify immutable functions. But for
	 * purposes of estimation, we consider it okay to simplify functions that
	 * are merely stable; the risk that the result might change from planning
	 * time to execution time is worth taking in preference to not being able
	 * to estimate the value at all.
	 */
	if (funcform->provolatile == PROVOLATILE_IMMUTABLE)
		 /* okay */ ;
	else if (context->estimate && 
			(funcform->provolatile == PROVOLATILE_STABLE ||
			funcform->provolatile == PROVOLATILE_CACHABLE))
		 /* okay */ ;
	else
		return NULL;

	/*
	 * OK, looks like we can simplify this operator/function.
	 *
	 * Build a new FuncExpr node containing the already-simplified arguments.
	 */
	newexpr = makeNode(FuncExpr);
	newexpr->funcid = funcid;
#ifdef _PG_ORCL_
	newexpr->withfuncnsp = with_funcs;
	newexpr->withfuncid = with_funcid;
#endif
	newexpr->funcresulttype = result_type;
	newexpr->funcretset = false;
	newexpr->funcvariadic = funcvariadic;
	newexpr->funcformat = COERCE_EXPLICIT_CALL; /* doesn't matter */
	newexpr->funccollid = result_collid;	/* doesn't matter */
	newexpr->inputcollid = input_collid;
	newexpr->args = args;
	newexpr->location = -1;

	return evaluate_expr((Expr *) newexpr, result_type, result_typmod,
						 result_collid);
}

/*
 * inline_function: try to expand a function call inline
 *
 * If the function is a sufficiently simple SQL-language function
 * (just "SELECT expression"), then we can inline it and avoid the rather
 * high per-call overhead of SQL functions.  Furthermore, this can expose
 * opportunities for constant-folding within the function expression.
 *
 * We have to beware of some special cases however.  A directly or
 * indirectly recursive function would cause us to recurse forever,
 * so we keep track of which functions we are already expanding and
 * do not re-expand them.  Also, if a parameter is used more than once
 * in the SQL-function body, we require it not to contain any volatile
 * functions (volatiles might deliver inconsistent answers) nor to be
 * unreasonably expensive to evaluate.  The expensiveness check not only
 * prevents us from doing multiple evaluations of an expensive parameter
 * at runtime, but is a safety value to limit growth of an expression due
 * to repeated inlining.
 *
 * We must also beware of changing the volatility or strictness status of
 * functions by inlining them.
 *
 * Also, at the moment we can't inline functions returning RECORD.  This
 * doesn't work in the general case because it discards information such
 * as OUT-parameter declarations.
 *
 * Also, context-dependent expression nodes in the argument list are trouble.
 *
 * Returns a simplified expression if successful, or NULL if cannot
 * simplify the function.
 */
static Expr *
inline_function(Oid funcid, Oid result_type, Oid result_collid,
				Oid input_collid, List *args,
				bool funcvariadic,
				HeapTuple func_tuple,
				eval_const_expressions_context *context)
{
	Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
	char	   *src;
	Datum		tmp;
	bool		isNull;
	bool		modifyTargetList;
	MemoryContext oldcxt;
	MemoryContext mycxt;
	inline_error_callback_arg callback_arg;
	ErrorContextCallback sqlerrcontext;
	FuncExpr   *fexpr;
	SQLFunctionParseInfoPtr pinfo;
	ParseState *pstate;
	List	   *raw_parsetree_list;
	Query	   *querytree;
	Node	   *newexpr;
	int		   *usecounts;
	ListCell   *arg;
	int			i;

	/*
	 * Forget it if the function is not SQL-language or has other showstopper
	 * properties.  (The prokind and nargs checks are just paranoia.)
	 */
	if (funcform->prolang != SQLlanguageId ||
		funcform->prokind != PROKIND_FUNCTION ||
		funcform->prosecdef ||
		funcform->proretset ||
		funcform->prorettype == RECORDOID ||
		!heap_attisnull(func_tuple, Anum_pg_proc_proconfig, NULL) ||
		funcform->pronargs != list_length(args))
		return NULL;

	/* Check for recursive function, and give up trying to expand if so */
	if (list_member_oid(context->active_fns, funcid))
		return NULL;

	/* Check permission to call function (fail later, if not) */
	if (pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
		return NULL;

	/* Check whether a plugin wants to hook function entry/exit */
	if (FmgrHookIsNeeded(funcid))
		return NULL;

	/*
	 * Make a temporary memory context, so that we don't leak all the stuff
	 * that parsing might create.
	 */
	mycxt = AllocSetContextCreate(CurrentMemoryContext,
								  "inline_function",
								  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(mycxt);

	/* Fetch the function body */
	tmp = SysCacheGetAttr(PROCOID,
						  func_tuple,
						  Anum_pg_proc_prosrc,
						  &isNull);
	if (isNull)
		elog(ERROR, "null prosrc for function %u", funcid);
	src = TextDatumGetCString(tmp);

	/*
	 * Setup error traceback support for ereport().  This is so that we can
	 * finger the function that bad information came from.
	 */
	callback_arg.proname = NameStr(funcform->proname);
	callback_arg.prosrc = src;

	sqlerrcontext.callback = sql_inline_error_callback;
	sqlerrcontext.arg = (void *) &callback_arg;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	/*
	 * Set up to handle parameters while parsing the function body.  We need a
	 * dummy FuncExpr node containing the already-simplified arguments to pass
	 * to prepare_sql_fn_parse_info.  (It is really only needed if there are
	 * some polymorphic arguments, but for simplicity we always build it.)
	 */
	fexpr = makeNode(FuncExpr);
	fexpr->funcid = funcid;
	fexpr->funcresulttype = result_type;
	fexpr->funcretset = false;
	fexpr->funcvariadic = funcvariadic;
	fexpr->funcformat = COERCE_EXPLICIT_CALL;	/* doesn't matter */
	fexpr->funccollid = result_collid;	/* doesn't matter */
	fexpr->inputcollid = input_collid;
	fexpr->args = args;
	fexpr->location = -1;

	pinfo = prepare_sql_fn_parse_info(func_tuple,
									  (Node *) fexpr,
									  input_collid);

	/*
	 * We just do parsing and parse analysis, not rewriting, because rewriting
	 * will not affect table-free-SELECT-only queries, which is all that we
	 * care about.  Also, we can punt as soon as we detect more than one
	 * command in the function body.
	 */
	raw_parsetree_list = pg_parse_query(src);
	if (list_length(raw_parsetree_list) != 1)
		goto fail;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = src;
	sql_fn_parser_setup(pstate, pinfo);

	querytree = transformTopLevelStmt(pstate, linitial(raw_parsetree_list));

	free_parsestate(pstate);

	/*
	 * The single command must be a simple "SELECT expression".
	 *
	 * Note: if you change the tests involved in this, see also plpgsql's
	 * exec_simple_check_plan().  That generally needs to have the same idea
	 * of what's a "simple expression", so that inlining a function that
	 * previously wasn't inlined won't change plpgsql's conclusion.
	 */
	if (!IsA(querytree, Query) ||
		querytree->commandType != CMD_SELECT ||
		querytree->hasAggs ||
		querytree->hasWindowFuncs ||
		querytree->hasTargetSRFs ||
		querytree->hasSubLinks ||
		querytree->cteList ||
		querytree->rtable ||
		querytree->jointree->fromlist ||
		querytree->jointree->quals ||
		querytree->groupClause ||
		querytree->groupingSets ||
		querytree->havingQual ||
		querytree->windowClause ||
		querytree->distinctClause ||
		querytree->sortClause ||
		querytree->limitOffset ||
		querytree->limitCount ||
		querytree->setOperations ||
		list_length(querytree->targetList) != 1)
		goto fail;

	/*
	 * Make sure the function (still) returns what it's declared to.  This
	 * will raise an error if wrong, but that's okay since the function would
	 * fail at runtime anyway.  Note that check_sql_fn_retval will also insert
	 * a RelabelType if needed to make the tlist expression match the declared
	 * type of the function.
	 *
	 * Note: we do not try this until we have verified that no rewriting was
	 * needed; that's probably not important, but let's be careful.
	 */
	if (check_sql_fn_retval(funcid, result_type, list_make1(querytree),
							&modifyTargetList, NULL))
		goto fail;				/* reject whole-tuple-result cases */

	/* Now we can grab the tlist expression */
	newexpr = (Node *) ((TargetEntry *) linitial(querytree->targetList))->expr;

	/*
	 * If the SQL function returns VOID, we can only inline it if it is a
	 * SELECT of an expression returning VOID (ie, it's just a redirection to
	 * another VOID-returning function).  In all non-VOID-returning cases,
	 * check_sql_fn_retval should ensure that newexpr returns the function's
	 * declared result type, so this test shouldn't fail otherwise; but we may
	 * as well cope gracefully if it does.
	 */
	if (exprType(newexpr) != result_type)
		goto fail;

	/* check_sql_fn_retval couldn't have made any dangerous tlist changes */
	Assert(!modifyTargetList);

	/*
	 * Additional validity checks on the expression.  It mustn't be more
	 * volatile than the surrounding function (this is to avoid breaking hacks
	 * that involve pretending a function is immutable when it really ain't).
	 * If the surrounding function is declared strict, then the expression
	 * must contain only strict constructs and must use all of the function
	 * parameters (this is overkill, but an exact analysis is hard).
	 */
	if (funcform->provolatile == PROVOLATILE_IMMUTABLE &&
		contain_mutable_functions(newexpr))
		goto fail;
	else if ((funcform->provolatile == PROVOLATILE_STABLE || 
			funcform->provolatile == PROVOLATILE_CACHABLE) &&
			 contain_volatile_functions(newexpr))
		goto fail;

	if (funcform->proisstrict &&
		contain_nonstrict_functions(newexpr))
		goto fail;

	/*
	 * If any parameter expression contains a context-dependent node, we can't
	 * inline, for fear of putting such a node into the wrong context.
	 */
	if (contain_context_dependent_node((Node *) args))
		goto fail;

	/*
	 * We may be able to do it; there are still checks on parameter usage to
	 * make, but those are most easily done in combination with the actual
	 * substitution of the inputs.  So start building expression with inputs
	 * substituted.
	 */
	usecounts = (int *) palloc0(funcform->pronargs * sizeof(int));
	newexpr = substitute_actual_parameters(newexpr, funcform->pronargs,
										   args, usecounts);

	/* Now check for parameter usage */
	i = 0;
	foreach(arg, args)
	{
		Node	   *param = lfirst(arg);

		if (usecounts[i] == 0)
		{
			/* Param not used at all: uncool if func is strict */
			if (funcform->proisstrict)
				goto fail;
		}
		else if (usecounts[i] != 1)
		{
			/* Param used multiple times: uncool if expensive or volatile */
			QualCost	eval_cost;

			/*
			 * We define "expensive" as "contains any subplan or more than 10
			 * operators".  Note that the subplan search has to be done
			 * explicitly, since cost_qual_eval() will barf on unplanned
			 * subselects.
			 */
			if (contain_subplans(param))
				goto fail;
			cost_qual_eval(&eval_cost, list_make1(param), NULL);
			if (eval_cost.startup + eval_cost.per_tuple >
				10 * cpu_operator_cost)
				goto fail;

			/*
			 * Check volatility last since this is more expensive than the
			 * above tests
			 */
			if (contain_volatile_functions(param))
				goto fail;
		}
		i++;
	}

	/*
	 * Whew --- we can make the substitution.  Copy the modified expression
	 * out of the temporary memory context, and clean up.
	 */
	MemoryContextSwitchTo(oldcxt);

	newexpr = copyObject(newexpr);

	MemoryContextDelete(mycxt);

	/*
	 * If the result is of a collatable type, force the result to expose the
	 * correct collation.  In most cases this does not matter, but it's
	 * possible that the function result is used directly as a sort key or in
	 * other places where we expect exprCollation() to tell the truth.
	 */
	if (OidIsValid(result_collid))
	{
		Oid			exprcoll = exprCollation(newexpr);

		if (OidIsValid(exprcoll) && exprcoll != result_collid)
		{
			CollateExpr *newnode = makeNode(CollateExpr);

			newnode->arg = (Expr *) newexpr;
			newnode->collOid = result_collid;
			newnode->location = -1;

			newexpr = (Node *) newnode;
		}
	}

	/*
	 * Since there is now no trace of the function in the plan tree, we must
	 * explicitly record the plan's dependency on the function.
	 */
	if (context->root)
		record_plan_function_dependency(context->root, funcid);

	/*
	 * Recursively try to simplify the modified expression.  Here we must add
	 * the current function to the context list of active functions.
	 */
	context->active_fns = lcons_oid(funcid, context->active_fns);
	newexpr = eval_const_expressions_mutator(newexpr, context);
	context->active_fns = list_delete_first(context->active_fns);

	error_context_stack = sqlerrcontext.previous;

	return (Expr *) newexpr;

	/* Here if func is not inlinable: release temp memory and return NULL */
fail:
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;

	return NULL;
}

/*
 * Replace Param nodes by appropriate actual parameters
 */
static Node *
substitute_actual_parameters(Node *expr, int nargs, List *args,
							 int *usecounts)
{
	substitute_actual_parameters_context context;

	context.nargs = nargs;
	context.args = args;
	context.usecounts = usecounts;

	return substitute_actual_parameters_mutator(expr, &context);
}

static Node *
substitute_actual_parameters_mutator(Node *node,
									 substitute_actual_parameters_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind != PARAM_EXTERN)
			elog(ERROR, "unexpected paramkind: %d", (int) param->paramkind);
		if (param->paramid <= 0 || param->paramid > context->nargs)
			elog(ERROR, "invalid paramid: %d", param->paramid);

		/* Count usage of parameter */
		context->usecounts[param->paramid - 1]++;

		/* Select the appropriate actual arg and replace the Param with it */
		/* We don't need to copy at this time (it'll get done later) */
		return list_nth(context->args, param->paramid - 1);
	}
	return expression_tree_mutator(node, substitute_actual_parameters_mutator,
								   (void *) context);
}

/*
 * error context callback to let us supply a call-stack traceback
 */
static void
sql_inline_error_callback(void *arg)
{
	inline_error_callback_arg *callback_arg = (inline_error_callback_arg *) arg;
	int			syntaxerrposition;

	/* If it's a syntax error, convert to internal syntax error report */
	syntaxerrposition = geterrposition();
	if (syntaxerrposition > 0)
	{
		errposition(0);
		internalerrposition(syntaxerrposition);
		internalerrquery(callback_arg->prosrc);
	}

	errcontext("SQL function \"%s\" during inlining", callback_arg->proname);
}

/*
 * evaluate_expr: pre-evaluate a constant expression
 *
 * We use the executor's routine ExecEvalExpr() to avoid duplication of
 * code and ensure we get the same result as the executor would get.
 */
static Expr *
evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
			  Oid result_collation)
{
	EState	   *estate;
	ExprState  *exprstate;
	MemoryContext oldcontext;
	Datum		const_val = (Datum)NULL;
	bool		const_is_null;
	int16		resultTypLen;
	bool		resultTypByVal;
	bool		eval_expr_error = false;
	MemoryContext context_before_trycatch = NULL;
	ResourceOwner owner_before_trycatch = NULL;

	/*
	 * To use the executor, we need an EState.
	 */
	estate = CreateExecutorState();

	/* We can use the estate's working context to avoid memory leaks. */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/* Make sure any opfuncids are filled in. */
	fix_opfuncids((Node *) expr);

	/*
	 * Prepare expr for execution.  (Note: we can't use ExecPrepareExpr
	 * because it'd result in recursively invoking eval_const_expressions.)
	 */
	exprstate = ExecInitExpr(expr, NULL);

	/*
	 * And evaluate it.
	 *
	 * It is OK to use a default econtext because none of the ExecEvalExpr()
	 * code used in this situation will use econtext.  That might seem
	 * fortuitous, but it's not so unreasonable --- a constant expression does
	 * not depend on context, by definition, n'est ce pas?
	 */
	context_before_trycatch = CurrentMemoryContext;
	owner_before_trycatch = CurrentResourceOwner;
	PG_TRY();
	{
		const_val = ExecEvalExprSwitchContext(exprstate,
											  GetPerTupleExprContext(estate),
											  &const_is_null);
	}
	PG_CATCH();
	{
		if (!ORA_MODE && !enable_lightweight_ora_syntax)
		{
			PG_RE_THROW();
		}
		else
		{
			HOLD_INTERRUPTS();
			FlushErrorState();
			RESUME_INTERRUPTS();
			eval_expr_error = true;
			MemoryContextSwitchTo(context_before_trycatch);
			CurrentResourceOwner = owner_before_trycatch;
		}
	}
	PG_END_TRY();

	if (!eval_expr_error)
	{
		/* Get info needed about result datatype */
		get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);
	}

	/* Get back to outer memory context */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Must copy result out of sub-context used by expression eval.
	 *
	 * Also, if it's varlena, forcibly detoast it.  This protects us against
	 * storing TOAST pointers into plans that might outlive the referenced
	 * data.  (makeConst would handle detoasting anyway, but it's worth a few
	 * extra lines here so that we can do the copy and detoast in one step.)
	 */
	if (!const_is_null && !eval_expr_error)
	{
		if (resultTypLen == -1)
			const_val = PointerGetDatum(PG_DETOAST_DATUM_COPY(const_val));
		else
			const_val = datumCopy(const_val, resultTypByVal, resultTypLen);
	}

	/* Release all the junk we just created */
	FreeExecutorState(estate);

	if (eval_expr_error)
		return expr;

	/*
	 * Make the constant result node.
	 */
	return (Expr *) makeConst(result_type, result_typmod, result_collation,
							  resultTypLen,
							  const_val, const_is_null,
							  resultTypByVal);
}


/*
 * inline_set_returning_function
 *		Attempt to "inline" a set-returning function in the FROM clause.
 *
 * "rte" is an RTE_FUNCTION rangetable entry.  If it represents a call of a
 * set-returning SQL function that can safely be inlined, expand the function
 * and return the substitute Query structure.  Otherwise, return NULL.
 *
 * This has a good deal of similarity to inline_function(), but that's
 * for the non-set-returning case, and there are enough differences to
 * justify separate functions.
 */
Query *
inline_set_returning_function(PlannerInfo *root, RangeTblEntry *rte)
{
	RangeTblFunction *rtfunc;
	FuncExpr   *fexpr;
	Oid			func_oid;
	HeapTuple	func_tuple;
	Form_pg_proc funcform;
	char	   *src;
	Datum		tmp;
	bool		isNull;
	bool		modifyTargetList;
	MemoryContext oldcxt;
	MemoryContext mycxt;
	List	   *saveInvalItems;
	inline_error_callback_arg callback_arg;
	ErrorContextCallback sqlerrcontext;
	SQLFunctionParseInfoPtr pinfo;
	List	   *raw_parsetree_list;
	List	   *querytree_list;
	Query	   *querytree;

	Assert(rte->rtekind == RTE_FUNCTION);

	/*
	 * It doesn't make a lot of sense for a SQL SRF to refer to itself in its
	 * own FROM clause, since that must cause infinite recursion at runtime.
	 * It will cause this code to recurse too, so check for stack overflow.
	 * (There's no need to do more.)
	 */
	check_stack_depth();

	/* Fail if the RTE has ORDINALITY - we don't implement that here. */
	if (rte->funcordinality)
		return NULL;

	/* Fail if RTE isn't a single, simple FuncExpr */
	if (list_length(rte->functions) != 1)
		return NULL;
	rtfunc = (RangeTblFunction *) linitial(rte->functions);

	if (!IsA(rtfunc->funcexpr, FuncExpr))
		return NULL;
	fexpr = (FuncExpr *) rtfunc->funcexpr;

#ifdef _PG_ORCL_
	/* Optimize it in the future */
	if (fexpr->withfuncnsp != NULL)
		return NULL;
#endif

	func_oid = fexpr->funcid;

	/*
	 * The function must be declared to return a set, else inlining would
	 * change the results if the contained SELECT didn't return exactly one
	 * row.
	 */
	if (!fexpr->funcretset)
		return NULL;

	/*
	 * Refuse to inline if the arguments contain any volatile functions or
	 * sub-selects.  Volatile functions are rejected because inlining may
	 * result in the arguments being evaluated multiple times, risking a
	 * change in behavior.  Sub-selects are rejected partly for implementation
	 * reasons (pushing them down another level might change their behavior)
	 * and partly because they're likely to be expensive and so multiple
	 * evaluation would be bad.
	 */
	if (contain_volatile_functions((Node *) fexpr->args) ||
		contain_subplans((Node *) fexpr->args))
		return NULL;

	/* Check permission to call function (fail later, if not) */
	if (pg_proc_aclcheck(func_oid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
		return NULL;

	/* Check whether a plugin wants to hook function entry/exit */
	if (FmgrHookIsNeeded(func_oid))
		return NULL;

	/*
	 * OK, let's take a look at the function's pg_proc entry.
	 */
	func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
	if (!HeapTupleIsValid(func_tuple))
		elog(ERROR, "cache lookup failed for function %u", func_oid);
	funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

	/*
	 * Forget it if the function is not SQL-language or has other showstopper
	 * properties.  In particular it mustn't be declared STRICT, since we
	 * couldn't enforce that.  It also mustn't be VOLATILE, because that is
	 * supposed to cause it to be executed with its own snapshot, rather than
     * sharing the snapshot of the calling query.  We also disallow returning
     * SETOF VOID, because inlining would result in exposing the actual result
     * of the function's last SELECT, which should not happen in that case.
     * (Rechecking prokind and proretset is just paranoia.)
	 */
	if (funcform->prolang != SQLlanguageId ||
		funcform->prokind != PROKIND_FUNCTION ||
		funcform->proisstrict ||
		funcform->provolatile == PROVOLATILE_VOLATILE ||
		funcform->prorettype == VOIDOID ||
		funcform->prosecdef ||
		!funcform->proretset ||
		!heap_attisnull(func_tuple, Anum_pg_proc_proconfig, NULL))
	{
		ReleaseSysCache(func_tuple);
		return NULL;
	}

	/*
	 * Make a temporary memory context, so that we don't leak all the stuff
	 * that parsing might create.
	 */
	mycxt = AllocSetContextCreate(CurrentMemoryContext,
								  "inline_set_returning_function",
								  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(mycxt);

	/*
	 * When we call eval_const_expressions below, it might try to add items to
	 * root->glob->invalItems.  Since it is running in the temp context, those
	 * items will be in that context, and will need to be copied out if we're
	 * successful.  Temporarily reset the list so that we can keep those items
	 * separate from the pre-existing list contents.
	 */
	saveInvalItems = root->glob->invalItems;
	root->glob->invalItems = NIL;

	/* Fetch the function body */
	tmp = SysCacheGetAttr(PROCOID,
						  func_tuple,
						  Anum_pg_proc_prosrc,
						  &isNull);
	if (isNull)
		elog(ERROR, "null prosrc for function %u", func_oid);
	src = TextDatumGetCString(tmp);

	/*
	 * Setup error traceback support for ereport().  This is so that we can
	 * finger the function that bad information came from.
	 */
	callback_arg.proname = NameStr(funcform->proname);
	callback_arg.prosrc = src;

	sqlerrcontext.callback = sql_inline_error_callback;
	sqlerrcontext.arg = (void *) &callback_arg;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	/*
	 * Run eval_const_expressions on the function call.  This is necessary to
	 * ensure that named-argument notation is converted to positional notation
	 * and any default arguments are inserted.  It's a bit of overkill for the
	 * arguments, since they'll get processed again later, but no harm will be
	 * done.
	 */
	fexpr = (FuncExpr *) eval_const_expressions(root, (Node *) fexpr);

	/* It should still be a call of the same function, but let's check */
	if (!IsA(fexpr, FuncExpr) ||
		fexpr->funcid != func_oid)
		goto fail;

	/* Arg list length should now match the function */
	if (list_length(fexpr->args) != funcform->pronargs)
		goto fail;

	/*
	 * Set up to handle parameters while parsing the function body.  We can
	 * use the FuncExpr just created as the input for
	 * prepare_sql_fn_parse_info.
	 */
	pinfo = prepare_sql_fn_parse_info(func_tuple,
									  (Node *) fexpr,
									  fexpr->inputcollid);

	/*
	 * Parse, analyze, and rewrite (unlike inline_function(), we can't skip
	 * rewriting here).  We can fail as soon as we find more than one query,
	 * though.
	 */
	raw_parsetree_list = pg_parse_query(src);
	if (list_length(raw_parsetree_list) != 1)
		goto fail;

	querytree_list = pg_analyze_and_rewrite_params(linitial(raw_parsetree_list),
												   src,
												   (ParserSetupHook) sql_fn_parser_setup,
												   pinfo, NULL,
												   false,
												   false);
	if (list_length(querytree_list) != 1)
		goto fail;
	querytree = linitial(querytree_list);

	/*
	 * The single command must be a plain SELECT.
	 */
	if (!IsA(querytree, Query) ||
		querytree->commandType != CMD_SELECT)
		goto fail;

	/*
	 * Make sure the function (still) returns what it's declared to.  This
	 * will raise an error if wrong, but that's okay since the function would
	 * fail at runtime anyway.  Note that check_sql_fn_retval will also insert
	 * RelabelType(s) and/or NULL columns if needed to make the tlist
	 * expression(s) match the declared type of the function.
	 *
	 * If the function returns a composite type, don't inline unless the check
	 * shows it's returning a whole tuple result; otherwise what it's
	 * returning is a single composite column which is not what we need. (Like
	 * check_sql_fn_retval, we deliberately exclude domains over composite
	 * here.)
	 */
	if (!check_sql_fn_retval(func_oid, fexpr->funcresulttype,
							 querytree_list,
							 &modifyTargetList, NULL) &&
		(get_typtype(fexpr->funcresulttype) == TYPTYPE_COMPOSITE ||
		 fexpr->funcresulttype == RECORDOID))
		goto fail;				/* reject not-whole-tuple-result cases */

	/*
	 * If we had to modify the tlist to make it match, and the statement is
	 * one in which changing the tlist contents could change semantics, we
	 * have to punt and not inline.
	 */
	if (modifyTargetList)
		goto fail;

	/*
	 * If it returns RECORD, we have to check against the column type list
	 * provided in the RTE; check_sql_fn_retval can't do that.  (If no match,
	 * we just fail to inline, rather than complaining; see notes for
	 * tlist_matches_coltypelist.)	We don't have to do this for functions
	 * with declared OUT parameters, even though their funcresulttype is
	 * RECORDOID, so check get_func_result_type too.
	 */
	if (fexpr->funcresulttype == RECORDOID &&
		get_func_result_type(func_oid, NULL, NULL) == TYPEFUNC_RECORD &&
		!tlist_matches_coltypelist(querytree->targetList,
								   rtfunc->funccoltypes))
		goto fail;

	/*
	 * Looks good --- substitute parameters into the query.
	 */
	querytree = substitute_actual_srf_parameters(querytree,
												 funcform->pronargs,
												 fexpr->args);

	/*
	 * Copy the modified query out of the temporary memory context, and clean
	 * up.
	 */
	MemoryContextSwitchTo(oldcxt);

	querytree = copyObject(querytree);

	/* copy up any new invalItems, too */
	root->glob->invalItems = list_concat(saveInvalItems,
										 copyObject(root->glob->invalItems));

	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;
	ReleaseSysCache(func_tuple);

	/*
	 * We don't have to fix collations here because the upper query is already
	 * parsed, ie, the collations in the RTE are what count.
	 */

	/*
	 * Since there is now no trace of the function in the plan tree, we must
	 * explicitly record the plan's dependency on the function.
	 */
	record_plan_function_dependency(root, func_oid);

	return querytree;

	/* Here if func is not inlinable: release temp memory and return NULL */
fail:
	MemoryContextSwitchTo(oldcxt);
	root->glob->invalItems = saveInvalItems;
	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;
	ReleaseSysCache(func_tuple);

	return NULL;
}

static SubLink *
inline_target_function(PlannerInfo *root, FuncExpr *fexpr)
{
	int							i;
	int							target_columns = 0;
	Oid							func_oid;
	char						*src;
	Oid							*argtypes;
	char						**argnames;
	char						*argmodes;
	long						const_val = 0;
	List						*saveInvalItems;
	bool						isNull;
	bool						modifyTargetList;
	List						*raw_parsetree_list;
	List						*querytree_list;
	Node						*limit_const_expr;
	Datum						tmp;
	Query						*querytree;
	ListCell					*lc;
	SubLink						*sublink;
	HeapTuple					func_tuple;
	Form_pg_proc 				funcform;
	MemoryContext				oldcxt;
	MemoryContext				mycxt;
	inline_error_callback_arg	callback_arg;
	ErrorContextCallback		sqlerrcontext;
	SQLFunctionParseInfoPtr		pinfo;

	/*
	 * It doesn't make a lot of sense for a SQL SRF to refer to itself in its
	 * own from clause, since that must cause infinite recursion at runtime.
	 */
	check_stack_depth();

	func_oid = fexpr->funcid;

	/*
	 * Refuse to inline if the arguments contain any volatile functions or
	 * sub-selects. At the same time, if any parameter expression contains
	 * a context-dependent node, or the parameters in the function include
	 * the refactor in connect by, then it should not be inline, we can't 
	 * inline, for fear of putting such a node into the wrong context.
	 */
	if(contain_volatile_functions((Node *) fexpr->args) ||
	   contain_subplans((Node *) fexpr->args) ||
	   contain_context_dependent_node((Node *) fexpr->args) ||
	   contain_cn_expr((Node *) fexpr->args))
	   return NULL;

	/* Reject both nested functions and with functions */
	if (fexpr->withfuncnsp != NULL)
		return NULL;

	if (pg_proc_aclcheck(func_oid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
		return NULL;

	/* Check whether a plugin wants to hook function entry/exit */
	if (FmgrHookIsNeeded(func_oid))
		return NULL;

	/*
	 * Ok, let's take a look at the function's pg_proc entry.
	 */
	func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
	if (!HeapTupleIsValid(func_tuple))
		elog(ERROR, "cache lookup failed for function %u", func_oid);
	funcform = (Form_pg_proc) GETSTRUCT(func_tuple);

	/*
	 * The current optimization is only for SQL language,
	 * and cannot be declared as STRICT and VOLATILE.
	 */
	if (funcform->prolang != SQLlanguageId ||
		funcform->prokind != PROKIND_FUNCTION ||
		funcform->proisstrict ||
		funcform->prosecdef ||
		funcform->provolatile == PROVOLATILE_VOLATILE ||
		funcform->proretset ||
		funcform->prorettype == VOIDOID ||
		!heap_attisnull(func_tuple, Anum_pg_proc_proconfig, NULL))
	{
		ReleaseSysCache(func_tuple);
		return NULL;
	}

	/* Currently optimized functions cannot have out parameters */
	get_func_arg_info(func_tuple, &argtypes, &argnames, &argmodes);
	if (argmodes != NULL)
	{
		for (i = 0; i < funcform->pronargs; i++)
		{
			if (argmodes[i] == PROARGMODE_OUT ||
				argmodes[i] == PROARGMODE_INOUT)
			{
				ReleaseSysCache(func_tuple);
				return NULL;
			}
		}
	}

	/*
	 * Avoid the situation that the function included in the
	 * subplan cannot run in the CN.
	 */
	foreach(lc, fexpr->args)
	{
		Node	*arg = (Node *) lfirst(lc);

		if (!IS_CENTRALIZED_MODE && IsA(arg, FuncExpr))
		{
			ReleaseSysCache(func_tuple);
			return NULL;
		}
	}

	/*
	 * Make a temporary memory context, so that we don't leak all the stuff
	 * that parsing might create.
	 */
	mycxt = AllocSetContextCreate(CurrentMemoryContext,
								  "inline_set_returning_function",
								  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(mycxt);

	/*
	 * When we call eval_const_expressions blow, it might try to add items to 
	 * root->glob->invalItems. Since it is runing in the temp context, those
	 * items will be in that context, and will need to be copied out if we're
	 * successful.
	 */
	saveInvalItems = root->glob->invalItems;
	root->glob->invalItems = NIL;

	/* Fetch the function body */
	tmp = SysCacheGetAttr(PROCOID,
						  func_tuple,
						  Anum_pg_proc_prosrc,
						  &isNull);

	if (isNull)
		elog(ERROR, "null prosrc for function %u", func_oid);
	src = TextDatumGetCString(tmp);

	/*
	 * Setup error traceback support for ereport(). This is so that we can
	 * finger the function that bad information came from.
	 */
	callback_arg.proname = NameStr(funcform->proname);
	callback_arg.prosrc = src;

	sqlerrcontext.callback = sql_inline_error_callback;
	sqlerrcontext.arg = (void *) &callback_arg;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	/*
	 * Run eval_const_expressions on the function call. This is necessary to
	 * ensure that named-argument notations is converted to positional notation
	 * and any default arguments are inserted. Overkill ???
	 */
	fexpr = (FuncExpr *) eval_const_expressions(root, (Node *) fexpr);

	if (!IsA(fexpr, FuncExpr) || fexpr->funcid != func_oid ||
		list_length(fexpr->args) != funcform->pronargs)
		goto fail;

	/*
	 * Set up to handle parameters while parsing the function body.
	 */
	pinfo = prepare_sql_fn_parse_info(func_tuple,
									 (Node *) fexpr,
									 fexpr->inputcollid);

	/*
	 * Parse, analyze and rewrite.
	 */
	raw_parsetree_list = pg_parse_query(src);
	if (list_length(raw_parsetree_list) != 1)
		goto fail;

	querytree_list = pg_analyze_and_rewrite_params(linitial(raw_parsetree_list),
												  src,
												  (ParserSetupHook) sql_fn_parser_setup,
												  pinfo, NULL,
												  false,
												  false);

	if (list_length(querytree_list) != 1)
		goto fail;

	querytree = linitial(querytree_list);

	/*
	 * The single command must be a plain SELECT
	 */
	if (!IsA(querytree, Query) ||
		querytree->commandType != CMD_SELECT)
		goto fail;

	foreach(lc, querytree->targetList)
	{
		TargetEntry	*target = (TargetEntry *) lfirst(lc);

		if (target->resjunk == false)
			target_columns++;
	}

	if (target_columns != 1)
		goto fail;

	/*
	 * The sublink contains the call of the cn function,
	 * and if the rtable is not empty, it should not be
	 * optimized.
	 */
	if (contain_user_defined_functions((Node *) querytree) &&
		querytree->rtable)
		goto fail;

	limit_const_expr = eval_const_expressions(root, querytree->limitCount);

	if (limit_const_expr != NULL)
	{
		if (IsA(limit_const_expr, Const) &&
			!((Const *) limit_const_expr)->constisnull)
			const_val = DatumGetInt64(((Const *) limit_const_expr)->constvalue);
		else
			goto fail;
	}

	/* Limit the number of rows returned by the inner query to 1 */
	if (const_val > 1 || limit_const_expr == NULL)
		querytree->limitCount = (Node *) makeConst(INT8OID, -1, InvalidOid,
												   sizeof(int64),
												   Int64GetDatum(1), false,
												   FLOAT8PASSBYVAL);

	/*
	 * Make sure the function returns what it's declared to.
	 */
	check_sql_fn_retval(func_oid, fexpr->funcresulttype, querytree_list, &modifyTargetList, NULL);

	/*
	 * If we had to modify the targetlist to make it match, we
	 * have to punt and not inline.
	 */

	if (modifyTargetList)
		goto fail;

	/*
	 * Substitute parameters into the query.
	 */
	querytree = substitute_actual_srf_parameters(querytree,
												 funcform->pronargs,
												 fexpr->args);

	/*
	 * Copy the modified query out of the temporary memory context, and clean up.
	 */
	MemoryContextSwitchTo(oldcxt);
	querytree = copyObject(querytree);
	root->glob->invalItems = list_concat(saveInvalItems, copyObject(root->glob->invalItems));

	sublink = makeNode(SubLink);
	sublink->subLinkType = EXPR_SUBLINK;
	sublink->subselect = (Node *) querytree;

	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;
	ReleaseSysCache(func_tuple);

	/*
	 * Must explicitly record the plan's dependency on the function.
	 */
	record_plan_function_dependency(root, func_oid);

	return  sublink;

fail:
	MemoryContextSwitchTo(oldcxt);
	root->glob->invalItems = saveInvalItems;
	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;
	ReleaseSysCache(func_tuple);

	return NULL;
}

static Node *
inline_target_function_mutator(Node *node, inline_target_function_context *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, FuncExpr))
	{
		FuncExpr	*funcexpr = (FuncExpr *) node;
		SubLink		*sublink = NULL;

		sublink = inline_target_function(context->root, funcexpr);
		if (sublink != NULL)
		{
			context->curr_query->hasSubLinks = true;
			return (Node *) sublink;
		}
		else
		{
			ListCell	*lc;

			/*
			 * For situations where an external function cannot be inlined,
			 * consider inlining its parameters.
			 */
			foreach(lc, funcexpr->args)
				lfirst(lc) = inline_target_function_mutator(((Node *) lfirst(lc)), context);

			return node;
		}
	}
	else if (IsA(node, Query))
	{
		List		*targetList;
		List		*rtable;
		ListCell	*lc;
		Query		*tmp_query;

		/* update the current Query */
		tmp_query = context->curr_query;
		context->curr_query = (Query *) node;

		targetList = ((Query *) node)->targetList;
		foreach(lc, targetList)
		{
			TargetEntry	*targetentry = (TargetEntry *) lfirst(lc);

			targetentry->expr = (Expr *) inline_target_function_mutator((Node *) targetentry->expr, context);
		}

		rtable = ((Query *) node)->rtable;
		foreach(lc, rtable)
		{
			RangeTblEntry	*rt = (RangeTblEntry *) lfirst(lc);

			/* Handle the case where the range table is a subquery */
			if (rt->rtekind == RTE_SUBQUERY)
				inline_target_function_mutator((Node *) rt->subquery, context);
		}

		/* reset the current Query */
		context->curr_query = tmp_query;
	}

	return expression_tree_mutator(node, inline_target_function_mutator, context);
}

void
inline_target_functions(PlannerInfo *root)
{
	inline_target_function_context	context;

	context.root = root;
	context.curr_query = root->parse;
	inline_target_function_mutator((Node *) root->parse, &context);
}

/*
 * Replace Param nodes by appropriate actual parameters
 *
 * This is just enough different from substitute_actual_parameters()
 * that it needs its own code.
 */
static Query *
substitute_actual_srf_parameters(Query *expr, int nargs, List *args)
{
	substitute_actual_srf_parameters_context context;

	context.nargs = nargs;
	context.args = args;
	context.sublevels_up = 1;

	return query_tree_mutator(expr,
							  substitute_actual_srf_parameters_mutator,
							  &context,
							  0);
}

static Node *
substitute_actual_srf_parameters_mutator(Node *node,
										 substitute_actual_srf_parameters_context *context)
{
	Node	   *result;

	if (node == NULL)
		return NULL;
	if (IsA(node, Query))
	{
		context->sublevels_up++;
		result = (Node *) query_tree_mutator((Query *) node,
											 substitute_actual_srf_parameters_mutator,
											 (void *) context,
											 0);
		context->sublevels_up--;
		return result;
	}
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN)
		{
			if (param->paramid <= 0 || param->paramid > context->nargs)
				elog(ERROR, "invalid paramid: %d", param->paramid);

			/*
			 * Since the parameter is being inserted into a subquery, we must
			 * adjust levels.
			 */
			result = copyObject(list_nth(context->args, param->paramid - 1));
			IncrementVarSublevelsUp(result, context->sublevels_up, 0);
			return result;
		}
	}
	return expression_tree_mutator(node,
								   substitute_actual_srf_parameters_mutator,
								   (void *) context);
}

/*
 * Check whether a SELECT targetlist emits the specified column types,
 * to see if it's safe to inline a function returning record.
 *
 * We insist on exact match here.  The executor allows binary-coercible
 * cases too, but we don't have a way to preserve the correct column types
 * in the correct places if we inline the function in such a case.
 *
 * Note that we only check type OIDs not typmods; this agrees with what the
 * executor would do at runtime, and attributing a specific typmod to a
 * function result is largely wishful thinking anyway.
 */
static bool
tlist_matches_coltypelist(List *tlist, List *coltypelist)
{
	ListCell   *tlistitem;
	ListCell   *clistitem;

	clistitem = list_head(coltypelist);
	foreach(tlistitem, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tlistitem);
		Oid			coltype;

		if (tle->resjunk)
			continue;			/* ignore junk columns */

		if (clistitem == NULL)
			return false;		/* too many tlist items */

		coltype = lfirst_oid(clistitem);
		clistitem = lnext(clistitem);

		if (exprType((Node *) tle->expr) != coltype)
			return false;		/* column type mismatch */
	}

	if (clistitem != NULL)
		return false;			/* too few tlist items */

	return true;
}

#ifdef __OPENTENBASE__
/*
 * Replace Param nodes by appropriate actual parameters
 */
Node *
substitute_sublink_with_node(Node *expr, SubLink *sublink, Node *node)
{
	substitute_sublink_with_node_context context;

	context.node = node;
	context.sublink = sublink;

	return substitute_sublink_with_node_mutator(expr, &context);
}

static Node *
substitute_sublink_with_node_mutator(Node *node,
									substitute_sublink_with_node_context *context)
{
	if (node == NULL)
		return NULL;

	if ((void *)node == (void *)context->sublink)
		return (Node *)context->node;

	return expression_tree_mutator(node, substitute_sublink_with_node_mutator,
								   (void *)context);
}

bool find_sublink_walker(Node *node, List **list)
{
	if(node == NULL)
		return false;

	if (IsA(node, SubLink))
	{
		*list = lappend(*list, node);
		return true;
	}

	return expression_tree_walker(node, find_sublink_walker, list);
}

bool
find_estore_pushdown_clause(PlannerInfo* root, Expr* clause)
{
	bool is_simple_plain_operator = false;

	if (clause && IsA(clause, OpExpr))
	{
		Node* leftop = NULL;
		Node* rightop = NULL;
		OpExpr* op_clause = (OpExpr*)clause;

		if (list_length(op_clause->args) != 2 || !can_operator_be_pushed_down(op_clause->opno))
			return is_simple_plain_operator;

		leftop = get_leftop(clause);
		rightop = get_rightop(clause);

		if (leftop && is_var_node(leftop))
		{
			if (rightop && (IsA(rightop, Const) || is_operator_external_param_const(root, rightop)))
				is_simple_plain_operator = true;
		}

		if (rightop && is_var_node(rightop))
		{
			if (leftop && (IsA(leftop, Const) || is_operator_external_param_const(root, leftop)))
			{
				is_simple_plain_operator = true;
				CommuteOpExpr(op_clause);
				set_opfuncid(op_clause);
			}
		}
	}

	if (clause && IsA(clause, NullTest))
	{
		Node *leftop = NULL;
		NullTest *null_test = (NullTest *) clause;

		leftop = (Node *) null_test->arg;

		if (leftop && is_var_node(leftop))
		{
			/* consider check location as well */
			if (!null_test->argisrow)
				is_simple_plain_operator = true;
		}
	}

	if (clause && IsA(clause, ScalarArrayOpExpr))
	{
		Node* leftop = NULL;
		Node* rightop = NULL;
		ScalarArrayOpExpr* op_clause = (ScalarArrayOpExpr*)clause;

		/* Should only be an equal case for ScalarArrayOpExpr, but check nonetheless */
		if (list_length(op_clause->args) != 2 || !op_clause->useOr ||
			!can_operator_be_pushed_down_eq(op_clause->opno))
			return is_simple_plain_operator;

		leftop = linitial(op_clause->args);
		rightop = lsecond(op_clause->args);

		/* Var must be the first argument in ScalarArrayOpExpr */
		if (leftop && is_var_node(leftop))
		{
			Var *var = (Var *) leftop;

			if (var->varlevelsup == 0 && (is_operator_simple_in_any(root, rightop)))
				is_simple_plain_operator = true;
		}
	}

	return is_simple_plain_operator;
}


/*
 * replace_distribkey_func:
 * evaluate the result of a function that returns only
 * one value and replace as certain value.
 */
Node*
replace_distribkey_func(Node *node)
{
	if (node == NULL)
		return NULL;

	if (node->type == T_FuncExpr)
	{
		FuncExpr *func = (FuncExpr *) node;

		if (!func->funcretset)
		{
			Node *evalNode = (Node *) evaluate_expr((Expr *) func,
			                              func->funcresulttype,
			                              exprTypmod(node),
			                              func->funccollid);
			return evalNode;
		}
	}

	return expression_tree_mutator(node, 
								   replace_distribkey_func, 
								   NULL);
}

#endif

Node*
replace_sql_value_function_mutator(Node *node)
{
	if (node == NULL)
		return NULL;

	if (node->type == T_SQLValueFunction)
	{
		SQLValueFunction *svf = (SQLValueFunction *) node;

		return (Node *) evaluate_expr((Expr *) svf,
		                              svf->type,
		                              svf->typmod,
		                              InvalidOid);
	}

	return expression_tree_mutator_internal(node, replace_sql_value_function_mutator, NULL, false);
}

static bool
can_operator_be_pushed_down(Oid opno)
{
	bool can_be_pushed_down = false;
	HeapTuple tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_operator op_tuple = (Form_pg_operator)GETSTRUCT(tuple);
		if ((strncmp(NameStr(op_tuple->oprname), "<", NAMEDATALEN) == 0) ||
			(strncmp(NameStr(op_tuple->oprname), ">", NAMEDATALEN) == 0) ||
			(strncmp(NameStr(op_tuple->oprname), "=", NAMEDATALEN) == 0) ||
			(strncmp(NameStr(op_tuple->oprname), ">=", NAMEDATALEN) == 0) ||
			(strncmp(NameStr(op_tuple->oprname), "<=", NAMEDATALEN) == 0))
			can_be_pushed_down = true;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				(errmsg("cache lookup failed for operator oid %u while "
						"determine predicate push down", opno))));
	}

	ReleaseSysCache(tuple);
	return can_be_pushed_down;
}

static bool
can_operator_be_pushed_down_eq(Oid opno)
{
	bool can_be_pushed_down = false;
	HeapTuple tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_operator op_tuple = (Form_pg_operator)GETSTRUCT(tuple);
		if ((strncmp(NameStr(op_tuple->oprname), "=", NAMEDATALEN) == 0))
			can_be_pushed_down = true;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				(errmsg("cache lookup failed for operator oid %u while "
						"determine predicate push down", opno))));
	}

	ReleaseSysCache(tuple);
	return can_be_pushed_down;
}


static bool
is_operator_external_param_const(PlannerInfo* root, Node* node)
{
	bool is_param_const = false;

	if (node && nodeTag(node) == T_Param)
	{
		Param* param = (Param*)node;
		if (param->paramkind == PARAM_EXTERN || param->paramkind == PARAM_EXEC)
			is_param_const = true;
	}

	return is_param_const;
}

static bool
is_operator_simple_in_any(PlannerInfo* root, Node* node)
{
	bool is_param_const = false;

	if (is_pseudo_constant_clause(node))
	{
			is_param_const = true;
	}

	return is_param_const;
}


static bool is_var_node(Node* node)
{
	bool is_var = false;

	if (IsA(node, Var) && ((Var*)node)->varattno > 0)
	{
		is_var = true;
	}
	else if (IsA(node, RelabelType))
	{
		RelabelType* reltype = (RelabelType*)node;
		if (IsA(reltype->arg, Var) && ((Var*)reltype->arg)->varattno > 0)
			is_var = true;
	}

	return is_var;
}

static bool
unsupport_quals_for_global_index_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, SubLink))
	{
		return true;
	}
	else if (IsA(node, SubPlan))
	{
		return true;
	}

	return expression_tree_walker(node, unsupport_quals_for_global_index_walker, context);
}

/*
 * unsupport_quals_for_global_index
 *	  Recursively search for Subplan/SubLink nodes within a clause.
 *
 *	  Returns false if any unsupported node is found.
 */
bool
unsupport_quals_for_global_index(Node *clause)
{
	return unsupport_quals_for_global_index_walker(clause, NULL);
}

static bool
contain_prior_expr_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, PriorExpr))
		return true;
	return expression_tree_walker(node, contain_prior_expr_walker, context);
}

bool
contain_prior_expr(Node *clause)
{
	if (!ORA_MODE)
		return false;
	return contain_prior_expr_walker(clause, NULL);
}

static bool
contain_cn_expr_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_RownumExpr:
		case T_LevelExpr:
		case T_CBIsLeafExpr:
		case T_CBIsCycleExpr:
		case T_CBRootExpr:
		case T_CBPathExpr:
			return true;
		default:
			break;
	}

	if (!IS_CENTRALIZED_DATANODE && check_functions_in_node(node,
						contain_user_defined_functions_checker, NULL))
		return true;

	return expression_tree_walker(node,
								  contain_cn_expr_walker, context);
}

bool
contain_cn_expr(Node *clause)
{
	return contain_cn_expr_walker(clause, NULL);
}

static Node *
connectby_split_jquals_walker(Node *node, connectby_split_quals_context *context)
{
	ListCell   *l = NULL;
	List	   *args = NIL;

	/* if OR, replace args to FALSE, if AND, replace args to TRUE */
	bool	value = !context->is_or_cond;

	if (node == NULL)
		return NULL;

	if (IsA(node, BoolExpr))
	{
		BoolExpr	*bexpr;

		bexpr = (BoolExpr *) node;
		if (bexpr->boolop == AND_EXPR)
		{
			bool	saved_is_or_cond = context->is_or_cond;

			context->is_or_cond = false;
			foreach(l, bexpr->args)
				lfirst(l) = connectby_split_jquals_walker(lfirst(l), context);
			context->is_or_cond = saved_is_or_cond;
		}
		else if (bexpr->boolop == OR_EXPR)
		{
			bool	saved_is_or_cond = context->is_or_cond;

			context->is_or_cond = true;
			foreach(l, bexpr->args)
				lfirst(l) = connectby_split_jquals_walker(lfirst(l), context);
			context->is_or_cond = saved_is_or_cond;
		}
		else
		{
			Assert(bexpr->boolop == NOT_EXPR);
			foreach(l, bexpr->args)
				lfirst(l) = connectby_split_jquals_walker(lfirst(l), context);
		}

		args = bexpr->args;
	}
	else if (IsA(node, List))
	{
		foreach(l, (List *) node)
			lfirst(l) = connectby_split_jquals_walker(lfirst(l), context);
		args = (List *) node;
	}
	else
	{
		bool	is_multi;
		Relids	relids = pull_varnos(node);

		is_multi = (bms_membership(relids) == BMS_MULTIPLE &&
					!contain_cn_expr(node) &&
					!contain_prior_expr(node) &&
					!contain_subplans(node));

		/* 
		 * Note:
		 * Replace the unused sub expr with BOOL value, to extract the expr we need.
		 * The replace value comes from the above judgement.
		 * e.g.
		 * 		SELECT * FROM t1, t2 WHERE t1.a = t2.b OR t2.b > 3 CONNECT BY ...
		 * When replace_singleton is TRUE, replace the t2.b > 3 with FALSE,
		 * so we can identified 't1.a = t2.b OR FALSE' as join qual.
		 * When replace_singleton is FALSE, replace the t1.a = t2.b with FALSE,
		 * so we can identified 'FALSE OR t2.b > 3' as other qual.
		 */
		if (context->replace_singleton && !is_multi)
			return makeBoolConst(value, false);
		else if (!context->replace_singleton && is_multi)
			return makeBoolConst(value, false);
		else
			return node;
	}

	/*
	 * Special case, if all args are replaced, will return a single
	 * const.
	 */
	l = NULL;
	foreach(l, args)
	{
		Node	*n = (Node *) lfirst(l);
		if (!IsA(n, Const) || exprType(n) != BOOLOID)
			break;
	}
	if (l == NULL)
		return makeBoolConst(value, false);

	return node;
}

Node *
connectby_split_jquals(PlannerInfo *root, Node *node, bool single)
{
	connectby_split_quals_context	context;
	Node	*newnode = copyObject(node);

	context.is_or_cond = false;
	context.replace_singleton = single;

	connectby_split_jquals_walker(newnode, &context);

	newnode = (Node *) make_ands_explicit((List *) newnode);
	newnode = (Node *) canonicalize_qual((Expr *) newnode, false);
	newnode = (Node *) make_ands_implicit((Expr *) newnode);
	return newnode;
}

bool
contain_pullup_range_table(Node *node)
{
	RangeTblEntry *rte;
	char locator_type;

	if (node == NULL)
		return false;

	if (IsA(node, RangeTblEntry))
	{
		rte = (RangeTblEntry *)node;
		if (rte->rtekind == RTE_RELATION)
		{
			if (rte->relkind == RELKIND_RELATION || rte->relkind == RELKIND_PARTITIONED_TABLE)
			{
				locator_type = GetRelLocatorType(rte->relid);
				if (IsLocatorNone(locator_type) || IsLocatorColumnDistributed(locator_type) ||
					IsLocatorDistributedByValue(locator_type))
				{
					return true;
				}
			}
			else
			{
				return true;
			}
		}
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *)node;
		if (query->hasSubLinks || query->cteList || query->setOperations)
		{
			/* Need to recursively search the query tree. */
			return query_tree_walker(query,
									 contain_pullup_range_table,
									 NULL,
									 QTW_EXAMINE_RTES_BEFORE);
		}
		else
		{
			/* For most cases, the tables are in the rtable of query tree. */
			return range_table_walker(query->rtable,
									  contain_pullup_range_table,
									  NULL,
									  QTW_EXAMINE_RTES_BEFORE);
		}
	}
	else
	{
		return expression_tree_walker(node, contain_pullup_range_table,
									  NULL);
	}

	return false;
}
