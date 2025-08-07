/*-------------------------------------------------------------------------
 *
 * parse_func.c
 *		handle function calls in parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_func.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "catalog/pg_package.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/orcl_datetime.h"
#include "utils/syscache.h"
#include "catalog/pg_namespace.h"

#include "catalog/pg_compile.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#ifdef _PG_ORCL_
#include "catalog/pg_proc_fn.h"
#include "optimizer/clauses.h"
#endif
#include "catalog/pg_type_object.h"
#include "catalog/pg_cast.h"


static void unify_hypothetical_args(ParseState *pstate,
						List *fargs, int numAggregatedArgs,
						Oid *actual_arg_types, Oid *declared_arg_types);
static Oid	FuncNameAsType(List *funcname);
static Node *ParseComplexProjection(ParseState *pstate, char *funcname,
					   Node *first_arg, int location);
#ifdef _PG_ORCL_
static List *get_synonym_funnames(List *funcname, TypObjSynonInfo *info);
static bool is_ora_listagg_func(Oid funcid);
#endif
static int GetPriority(Oid typeoid);
static int GetCategoryPriority(TYPCATEGORY categoryoid);
static bool check_system_function_pullup(Oid func_id);

/*
 *	Parse a function call
 *
 *	For historical reasons, Postgres tries to treat the notations tab.col
 *	and col(tab) as equivalent: if a single-argument function call has an
 *	argument of complex type and the (unqualified) function name matches
 *	any attribute of the type, we take it as a column projection.  Conversely
 *	a function of a single complex-type argument can be written like a
 *	column reference, allowing functions to act like computed columns.
 *
 *	Hence, both cases come through here.  If fn is null, we're dealing with
 *	column syntax not function syntax, but in principle that should not
 *	affect the lookup behavior, only which error messages we deliver.
 *	The FuncCall struct is needed however to carry various decoration that
 *	applies to aggregate and window functions.
 *
 *	Also, when fn is null, we return NULL on failure rather than
 *	reporting a no-such-function error.
 *
 *	The argument expressions (in fargs) must have been transformed
 *	already.  However, nothing in *fn has been transformed.
 *
 *	last_srf should be a copy of pstate->p_last_srf from just before we
 *	started transforming fargs.  If the caller knows that fargs couldn't
 *	contain any SRF calls, last_srf can just be pstate->p_last_srf.
 *
 *	proc_call is true if we are considering a CALL statement, so that the
 *	name must resolve to a procedure name, not anything else.
 */
Node *
ParseFuncOrColumn(ParseState *pstate, List *funcname, List *fargs,
				  Node *last_srf, FuncCall *fn, bool proc_call, int location)
{
	bool		is_column = (fn == NULL);
	List	   *agg_order = (fn ? fn->agg_order : NIL);
	Expr	   *agg_filter = NULL;
	bool		agg_within_group = (fn ? fn->agg_within_group : false);
	bool		agg_star = (fn ? fn->agg_star : false);
	bool		agg_distinct = (fn ? fn->agg_distinct : false);
	bool		func_variadic = (fn ? fn->func_variadic : false);
	WindowDef	*over = (fn ? fn->over : NULL);
	KeepDef		*keep = (fn ? fn->keep : NULL);
	Oid			rettype;
	Oid			funcid;
	ListCell   *l;
	ListCell   *nextl;
	Node	   *first_arg = NULL;
	int			nargs;
	int			nargsplusdefs;
	Oid			actual_arg_types[FUNC_MAX_ARGS];
	Oid		   *declared_arg_types;
	List	   *argnames;
	List	   *argdefaults;
	Node	   *retval;
	bool		retset;
	int			nvargs;
	Oid			vatype;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	char		aggkind = 0;
	ParseCallbackState pcbstate;
#ifdef _PG_ORCL_
	List	*old_funcname = funcname;
	bool	found_in_with = false;
	int		with_funcid = -1;
	List*	with_funcs = pstate->with_funcs;
#endif

	/*
	 * If there's an aggregate filter, transform it using transformWhereClause
	 */
	if (fn && fn->agg_filter != NULL)
		agg_filter = (Expr *) transformWhereClause(pstate, fn->agg_filter,
												   EXPR_KIND_FILTER,
												   "FILTER");

	/*
	 * Most of the rest of the parser just assumes that functions do not have
	 * more than FUNC_MAX_ARGS parameters.  We have to test here to protect
	 * against array overruns, etc.  Of course, this may not be a function,
	 * but the test doesn't hurt.
	 */
	if (list_length(fargs) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS),
				 parser_errposition(pstate, location)));

	/*
	 * Extract arg type info in preparation for function lookup.
	 *
	 * If any arguments are Param markers of type VOID, we discard them from
	 * the parameter list. This is a hack to allow the JDBC driver to not have
	 * to distinguish "input" and "output" parameter symbols while parsing
	 * function-call constructs.  Don't do this if dealing with column syntax,
	 * nor if we had WITHIN GROUP (because in that case it's critical to keep
	 * the argument count unchanged).  We can't use foreach() because we may
	 * modify the list ...
	 */
	nargs = 0;
	for (l = list_head(fargs); l != NULL; l = nextl)
	{
		Node	   *arg = lfirst(l);
		Oid			argtype = exprType(arg);

		nextl = lnext(l);

		if (argtype == VOIDOID && IsA(arg, Param) &&
			!is_column && !agg_within_group)
		{
			fargs = list_delete_ptr(fargs, arg);
			continue;
		}

		actual_arg_types[nargs++] = argtype;
	}

	/*
	 * Check for named arguments; if there are any, build a list of names.
	 *
	 * We allow mixed notation (some named and some not), but only with all
	 * the named parameters after all the unnamed ones.  So the name list
	 * corresponds to the last N actual parameters and we don't need any extra
	 * bookkeeping to match things up.
	 */
	argnames = NIL;
	foreach(l, fargs)
	{
		Node	   *arg = lfirst(l);

		if (IsA(arg, NamedArgExpr))
		{
			NamedArgExpr *na = (NamedArgExpr *) arg;
			ListCell   *lc;

			/* Reject duplicate arg names */
			foreach(lc, argnames)
			{
				if (strcmp(na->name, (char *) lfirst(lc)) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("argument name \"%s\" used more than once",
									na->name),
							 parser_errposition(pstate, na->location)));
			}
			argnames = lappend(argnames, na->name);
		}
		else
		{
			if (argnames != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("positional argument cannot follow named argument"),
						 parser_errposition(pstate, exprLocation(arg))));
		}
	}

	if (fargs)
	{
		first_arg = linitial(fargs);
		Assert(first_arg != NULL);
	}

	/*
	 * Check for column projection: if function has one argument, and that
	 * argument is of complex type, and function name is not qualified, then
	 * the "function call" could be a projection.  We also check that there
	 * wasn't any aggregate or variadic decoration, nor an argument name.
	 */
	if (nargs == 1 && !proc_call &&
		agg_order == NIL && agg_filter == NULL && !agg_star &&
		!agg_distinct && over == NULL && !func_variadic && argnames == NIL &&
		list_length(funcname) == 1)
	{
		Oid			argtype = actual_arg_types[0];

		if (argtype == RECORDOID || ISCOMPLEX(argtype))
		{
			retval = ParseComplexProjection(pstate,
											strVal(linitial(funcname)),
											first_arg,
											location);
			if (retval)
				return retval;

			/*
			 * If ParseComplexProjection doesn't recognize it as a projection,
			 * just press on.
			 */
		}
	}

	/*
	 * Okay, it's not a column projection, so it must really be a function.
	 * func_get_detail looks up the function in the catalogs, does
	 * disambiguation for polymorphic functions, handles inheritance, and
	 * returns the funcid and type and set or singleton status of the
	 * function's return value.  It also returns the true argument types to
	 * the function.
	 *
	 * Note: for a named-notation or variadic function call, the reported
	 * "true" types aren't really what is in pg_proc: the types are reordered
	 * to match the given argument order of named arguments, and a variadic
	 * argument is replaced by a suitable number of copies of its element
	 * type.  We'll fix up the variadic case below.  We may also have to deal
	 * with default arguments.
	 */

	setup_parser_errposition_callback(&pcbstate, pstate, location);

#ifdef _PG_ORCL_
	if (!ORA_MODE && pstate->with_funcs != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("only opentenbase_ora mode support WITH FUNCTION feature")));
	}

	/* Only for function or package replacement of synonym */
	if (ORA_MODE)
	{
		List		*funcnames;
		bool     keep_looking = true;
		bool	is_pkg_func	= false;


		/* If the length of the funcname is 1, just search it with the search_path */
		if (!found_in_with && keep_looking && list_length(funcname) == 1)
		{
			fdresult = func_get_detail(funcname, fargs, argnames, nargs,
										actual_arg_types,
										!func_variadic, true,
										&funcid, &rettype, &retset,
										&nvargs, &vatype,
										&declared_arg_types, &argdefaults,
										pstate);

			/* Yep, we find it, so we don't need to keep looking */
			if (fdresult != FUNCDETAIL_NOTFOUND && fdresult != FUNCDETAIL_MULTIPLE)
				keep_looking = false;
		}

		/* Try deconstrct funcname to pkg.func && schema.pkg.func && db.schema.pkg.func */
		if (!found_in_with && keep_looking)
		{
			List *pkgfuncname = NIL;

			pkgfuncname = DeconstructPackageFunctionName(funcname);
			if (pkgfuncname)
			{
				fdresult = func_get_detail(pkgfuncname, fargs, argnames, nargs,
											actual_arg_types,
											!func_variadic, true,
											&funcid, &rettype, &retset,
											&nvargs, &vatype,
											&declared_arg_types, &argdefaults
#ifdef _PG_ORCL_
											, pstate
#endif
										);

				is_pkg_func = true;
			}
		}

		/*
		 * If not found in WITH FUNCTION or null, try regular way.
		 *
		 * but if we already got a function with same name(with a unique prefix)
		 * in the with_func list, and still got here means the type of arguments
		 * not qualified, stop looking as opentenbase_ora dose
		 */
		if (!is_pkg_func && !found_in_with && keep_looking)
		{
			TypObjSynonInfo info;

			funcnames = get_synonym_funnames(funcname, &info);
			fdresult = FUNCDETAIL_NOTFOUND;
			foreach(l, funcnames)
			{
				funcname = lfirst(l);

				/*
				 * Mock type object self param
				 * if funcname length > 1, self argument is mock somewhere else
				 */
				if (info.info == OBJ_FUNC_CONSTRUCTOR)
				{
					int i;

					mockTypObjConsSelfParam(&fargs, info.typeOid);
					nargs++;
					Assert(nargs < FUNC_MAX_ARGS);

					for(i = nargs - 1; i > 0; i--)
					{
						actual_arg_types[i] = actual_arg_types[i - 1];
					}
					actual_arg_types[0] = info.typeOid;
				}
				fdresult = func_get_detail(funcname, fargs, argnames, nargs,
								   actual_arg_types,
								   !func_variadic, true,
								   &funcid, &rettype, &retset,
								   &nvargs, &vatype,
								   &declared_arg_types, &argdefaults, pstate);
				/* It's a valid identifier, give up other candicates */
				if (fdresult != FUNCDETAIL_NOTFOUND)
				{
					/*
					 * TAPD: 114400655
					 * Suppose we has a synonym 'syn for s1', and a package 's1',
					 * In case 'select syn.f()', if we replace the synonym 'syn'
					 * as package name say 's1', then we get 'select s1.f()'
					 * we here check if f is package 's1's subprogram
					 * if not, throw an error.
					 * Only package set will report error, otherwise may try
					 * other possible synonym.
					 */
					if (!(info.repinfo ^ SYNON_AS_PKG) &&
						!OidIsValid(get_pkgId_by_function(funcid)))
						elog(ERROR, "schema \"%s\" does not exist",
										strVal(linitial(old_funcname)));
					break;
				}
			}
		}
	}
	else
#endif
	fdresult = func_get_detail(funcname, fargs, argnames, nargs,
							   actual_arg_types,
							   !func_variadic, true,
							   &funcid, &rettype, &retset,
							   &nvargs, &vatype,
							   &declared_arg_types, &argdefaults
#ifdef _PG_ORCL_
							   , pstate
#endif
							   );

	cancel_parser_errposition_callback(&pcbstate);

	funcname = old_funcname;

	if ((pstate->p_is_prior || pstate->p_is_connect_by || pstate->p_is_start_with) &&
		(funcid == NEXTVAL_OID || funcid == CURRVAL_OID || funcid == F_ORCL_NEXTVAL_OID || funcid == F_ORCL_CURRVAL_OID
		))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("sequence number not allowed here"),
				 parser_errposition(pstate, location)));

	if (!InPlpgsqlFunc() && contain_plpgsql_functions_checker(funcid, NULL, NULL))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("direct call to opentenbase_ora_collect_* hook function is not allowed"),
				 parser_errposition(pstate, location)));
	}

	/*
	 * Check for various wrong-kind-of-routine cases.
	 */

	/* If this is a CALL, reject things that aren't procedures */
	if (proc_call &&
		(fdresult == FUNCDETAIL_NORMAL ||
		 fdresult == FUNCDETAIL_AGGREGATE ||
		 fdresult == FUNCDETAIL_WINDOWFUNC ||
		 fdresult == FUNCDETAIL_COERCION))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not a procedure",
						func_signature_string(funcname, nargs,
											  argnames,
											  actual_arg_types)),
				 errhint("To call a function, use SELECT."),
				 parser_errposition(pstate, location)));
	/* Conversely, if not a CALL, reject procedures */
	if (fdresult == FUNCDETAIL_PROCEDURE && !proc_call)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is a procedure",
						func_signature_string(funcname, nargs,
											  argnames,
											  actual_arg_types)),
				 errhint("To call a procedure, use CALL."),
				 parser_errposition(pstate, location)));

	if (fdresult == FUNCDETAIL_NORMAL ||
		fdresult == FUNCDETAIL_PROCEDURE ||
		fdresult == FUNCDETAIL_COERCION)
	{
		/*
		 * In these cases, complain if there was anything indicating it must
		 * be an aggregate or window function.
		 */
		if (agg_star)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) specified, but %s is not an aggregate function",
							NameListToString(funcname),
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_distinct)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("DISTINCT specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("WITHIN GROUP specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_order != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("ORDER BY specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_filter)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("FILTER specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (over)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("OVER specified, but %s is not a window function nor an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
	}

	/* keep can only works with plain aggregate function */
	if (ORA_MODE && keep && fdresult != FUNCDETAIL_AGGREGATE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("KEEP specified, but %s is not an aggregate function",
						NameListToString(funcname)),
				 parser_errposition(pstate, location)));

	/*
	 * So far so good, so do some routine-type-specific processing.
	 */
	if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE)
	{
		/* Nothing special to do for these cases. */
	}
	else if (fdresult == FUNCDETAIL_AGGREGATE)
	{
		/*
		 * It's an aggregate; fetch needed info from the pg_aggregate entry.
		 */
		HeapTuple	tup;
		Form_pg_aggregate classForm;
		int			catDirectArgs;

#ifdef _PG_ORCL_
		Assert(!found_in_with);
#endif
		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid));
		if (!HeapTupleIsValid(tup)) /* should not happen */
			elog(ERROR, "cache lookup failed for aggregate %u", funcid);
		classForm = (Form_pg_aggregate) GETSTRUCT(tup);
		aggkind = classForm->aggkind;
		catDirectArgs = classForm->aggnumdirectargs;
		ReleaseSysCache(tup);

		/* Now check various disallowed cases. */
		if (AGGKIND_IS_ORDERED_SET(aggkind))
		{
			int			numAggregatedArgs;
			int			numDirectArgs;

			if (!agg_within_group)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("WITHIN GROUP is required for ordered-set aggregate %s",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
			if (over)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("OVER is not supported for ordered-set aggregate %s",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));

			if (keep)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("KEEP is not supported for ordered-set aggregate %s",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));

			/* gram.y rejects DISTINCT + WITHIN GROUP */
			Assert(!agg_distinct);
			/* gram.y rejects VARIADIC + WITHIN GROUP */
			Assert(!func_variadic);

			/*
			 * Since func_get_detail was working with an undifferentiated list
			 * of arguments, it might have selected an aggregate that doesn't
			 * really match because it requires a different division of direct
			 * and aggregated arguments.  Check that the number of direct
			 * arguments is actually OK; if not, throw an "undefined function"
			 * error, similarly to the case where a misplaced ORDER BY is used
			 * in a regular aggregate call.
			 */
			numAggregatedArgs = list_length(agg_order);
			numDirectArgs = nargs - numAggregatedArgs;
			Assert(numDirectArgs >= 0);

			if (!OidIsValid(vatype))
			{
				/* Test is simple if aggregate isn't variadic */
				if (numDirectArgs != catDirectArgs)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("function %s does not exist",
									func_signature_string(funcname, nargs,
														  argnames,
														  actual_arg_types)),
							 errhint("There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
									 NameListToString(funcname),
									 catDirectArgs, numDirectArgs),
							 parser_errposition(pstate, location)));
			}
			else
			{
				/*
				 * If it's variadic, we have two cases depending on whether
				 * the agg was "... ORDER BY VARIADIC" or "..., VARIADIC ORDER
				 * BY VARIADIC".  It's the latter if catDirectArgs equals
				 * pronargs; to save a catalog lookup, we reverse-engineer
				 * pronargs from the info we got from func_get_detail.
				 */
				int			pronargs;

				pronargs = nargs;
				if (nvargs > 1)
					pronargs -= nvargs - 1;
				if (catDirectArgs < pronargs)
				{
					/* VARIADIC isn't part of direct args, so still easy */
					if (numDirectArgs != catDirectArgs)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_FUNCTION),
								 errmsg("function %s does not exist",
										func_signature_string(funcname, nargs,
															  argnames,
															  actual_arg_types)),
								 errhint("There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
										 NameListToString(funcname),
										 catDirectArgs, numDirectArgs),
								 parser_errposition(pstate, location)));
				}
				else
				{
					/*
					 * Both direct and aggregated args were declared variadic.
					 * For a standard ordered-set aggregate, it's okay as long
					 * as there aren't too few direct args.  For a
					 * hypothetical-set aggregate, we assume that the
					 * hypothetical arguments are those that matched the
					 * variadic parameter; there must be just as many of them
					 * as there are aggregated arguments.
					 */
					if (aggkind == AGGKIND_HYPOTHETICAL)
					{
						if (nvargs != 2 * numAggregatedArgs)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_FUNCTION),
									 errmsg("function %s does not exist",
											func_signature_string(funcname, nargs,
																  argnames,
																  actual_arg_types)),
									 errhint("To use the hypothetical-set aggregate %s, the number of hypothetical direct arguments (here %d) must match the number of ordering columns (here %d).",
											 NameListToString(funcname),
											 nvargs - numAggregatedArgs, numAggregatedArgs),
									 parser_errposition(pstate, location)));
					}
					else
					{
						if (nvargs <= numAggregatedArgs)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_FUNCTION),
									 errmsg("function %s does not exist",
											func_signature_string(funcname, nargs,
																  argnames,
																  actual_arg_types)),
									 errhint("There is an ordered-set aggregate %s, but it requires at least %d direct arguments.",
											 NameListToString(funcname),
											 catDirectArgs),
									 parser_errposition(pstate, location)));
					}
				}
			}

			/* Check type matching of hypothetical arguments */
			if (aggkind == AGGKIND_HYPOTHETICAL)
				unify_hypothetical_args(pstate, fargs, numAggregatedArgs,
										actual_arg_types, declared_arg_types);
		}
		else
		{
			/* Normal aggregate, so it can't have WITHIN GROUP */
			if (agg_within_group)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
			/* Do not support call distinct directly */
			if (funcid == DISTINCT_FUNC_OID)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("%s is not supported to be called directly",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
		}
	}
	else if (fdresult == FUNCDETAIL_WINDOWFUNC)
	{
		/*
		 * True window functions must be called with a window definition.
		 */
		if (!over)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("window function %s requires an OVER clause",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		/* And, per spec, WITHIN GROUP isn't allowed */
		if (agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("window function %s cannot have WITHIN GROUP",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
	}
	else if (fdresult == FUNCDETAIL_COERCION)
	{
		/*
		 * We interpreted it as a type coercion. coerce_type can handle these
		 * cases, so why duplicate code...
		 */
		return coerce_type(pstate, linitial(fargs),
						   actual_arg_types[0], rettype, -1,
						   COERCION_EXPLICIT, COERCE_EXPLICIT_CALL, location);
	}
	else
	{
		/*
		 * Oops.  Time to die.
		 *
		 * If we are dealing with the attribute notation rel.function, let the
		 * caller handle failure.
		 */
		if (is_column)
			return NULL;

		/*
		 * Else generate a detailed complaint for a function
		 */
		if (fdresult == FUNCDETAIL_MULTIPLE)
		{
            if (proc_call)
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                     errmsg("procedure %s is not unique",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("Could not choose a best candidate procedure. "
                             "You might need to add explicit type casts."),
                     parser_errposition(pstate, location)));
            else
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                     errmsg("function %s is not unique",
                            func_signature_string(funcname, nargs, argnames,
                                                  actual_arg_types)),
                     errhint("Could not choose a best candidate function. "
                             "You might need to add explicit type casts."),
                     parser_errposition(pstate, location)));
		}
		else if (list_length(agg_order) > 1 && !agg_within_group)
		{
			/* It's agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY */
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No aggregate function matches the given name and argument types. "
							 "Perhaps you misplaced ORDER BY; ORDER BY must appear "
							 "after all regular arguments of the aggregate."),
					 parser_errposition(pstate, location)));
		}
		else if (proc_call)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("procedure %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No procedure matches the given name and argument types. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No function matches the given name and argument types. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
	}

	/*
	 * If there are default arguments, we have to include their types in
	 * actual_arg_types for the purpose of checking generic type consistency.
	 * However, we do NOT put them into the generated parse node, because
	 * their actual values might change before the query gets run.  The
	 * planner has to insert the up-to-date values at plan time.
	 */
	nargsplusdefs = nargs;
	foreach(l, argdefaults)
	{
		Node	   *expr = (Node *) lfirst(l);

		/* probably shouldn't happen ... */
		if (nargsplusdefs >= FUNC_MAX_ARGS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
					 errmsg_plural("cannot pass more than %d argument to a function",
								   "cannot pass more than %d arguments to a function",
								   FUNC_MAX_ARGS,
								   FUNC_MAX_ARGS),
					 parser_errposition(pstate, location)));

		actual_arg_types[nargsplusdefs++] = exprType(expr);
	}

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   nargsplusdefs,
											   rettype,
											   false);

	/* perform the necessary typecasting of arguments */
	make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

	/*
	 * If the function isn't actually variadic, forget any VARIADIC decoration
	 * on the call.  (Perhaps we should throw an error instead, but
	 * historically we've allowed people to write that.)
	 */
	if (!OidIsValid(vatype))
	{
		Assert(nvargs == 0);
		func_variadic = false;
	}

	/*
	 * If it's a variadic function call, transform the last nvargs arguments
	 * into an array --- unless it's an "any" variadic.
	 */
	if (nvargs > 0 && vatype != ANYOID)
	{
		ArrayExpr  *newa = makeNode(ArrayExpr);
		int			non_var_args = nargs - nvargs;
		List	   *vargs;

		Assert(non_var_args >= 0);
		vargs = list_copy_tail(fargs, non_var_args);
		fargs = list_truncate(fargs, non_var_args);

		newa->elements = vargs;
		/* assume all the variadic arguments were coerced to the same type */
		newa->element_typeid = exprType((Node *) linitial(vargs));
		newa->array_typeid = get_array_type(newa->element_typeid);
		if (!OidIsValid(newa->array_typeid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find array type for data type %s",
							format_type_be(newa->element_typeid)),
					 parser_errposition(pstate, exprLocation((Node *) vargs))));
		/* array_collid will be set by parse_collate.c */
		newa->multidims = false;
		newa->location = exprLocation((Node *) vargs);

		fargs = lappend(fargs, newa);

		/* We could not have had VARIADIC marking before ... */
		Assert(!func_variadic);
		/* ... but now, it's a VARIADIC call */
		func_variadic = true;
	}

	/*
	 * If an "any" variadic is called with explicit VARIADIC marking, insist
	 * that the variadic parameter be of some array type.
	 */
	if (nargs > 0 && vatype == ANYOID && func_variadic)
	{
		Oid			va_arr_typid = actual_arg_types[nargs - 1];

		if (!OidIsValid(get_base_element_type(va_arr_typid)))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("VARIADIC argument must be an array"),
					 parser_errposition(pstate,
										exprLocation((Node *) llast(fargs)))));
	}

	/* if it returns a set, check that's OK */
	if (retset)
		check_srf_call_placement(pstate, last_srf, location);

	/* build the appropriate output structure */
	if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE)
	{
		FuncExpr   *funcexpr = makeNode(FuncExpr);

		if (ORA_MODE && found_in_with)
		{
			Assert(with_funcid != -1);
			funcexpr->withfuncnsp = with_funcs;
			funcexpr->withfuncid = with_funcid;
		}
		if (ORA_MODE && funcid == INTERVAL_TO_TEXT_OID)
		{
			Const	   *cons = NULL;
			int32		typmod = -1;
			Assert(list_length(fargs) == 1);
			typmod = exprTypmod(linitial(fargs));
			cons = makeConst(INT4OID,
							-1,
							InvalidOid,
							sizeof(int32),
							typmod,
							false,
							true);
			fargs = lappend(fargs, cons);
			funcid = INTERVAL_TYPMOD_TO_TEXT_OID;
		}

		funcexpr->funcid = funcid;
		funcexpr->funcresulttype = rettype;
		funcexpr->funcretset = retset;
		funcexpr->funcvariadic = func_variadic;
		funcexpr->funcformat = COERCE_EXPLICIT_CALL;
		/* funccollid and inputcollid will be set by parse_collate.c */
		funcexpr->args = fargs;
		funcexpr->location = location;

		retval = (Node *) funcexpr;

		if (fn != NULL && list_length(fn->return_columnref) != 0)
		{
			Oid			ret_typoid = funcexpr->funcresulttype;
			ListCell	*lc;
			HeapTuple	tuple;

			foreach(lc, fn->return_columnref)
			{
				Node	*node = (Node *) lfirst(lc);
				char	*str;
				int		j = 0;
				int		natts;
				Oid		typrelid;

				if (!IsA(node, String))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("references to function return value should be an attribute"),
							 parser_errposition(pstate, location)));
				str = strVal(node);

				if (!type_is_rowtype(ret_typoid))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("function return type does not contain attribute %s",
							 str)));

				/* Look up composite types in pg_type */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(ret_typoid));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for type %u", ret_typoid);
				typrelid = ((Form_pg_type) GETSTRUCT(tuple))->typrelid;
				ReleaseSysCache(tuple);

				/*
				 * Find the number of attributes corresponding to the 
				 * composite types in pg_class.
				 */
				tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(typrelid));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for the number of attributes "
								"corresponding to the composite type %u", ret_typoid);
				natts = ((Form_pg_class) GETSTRUCT(tuple))->relnatts;
				ReleaseSysCache(tuple);

				for (; j < natts; j++)
				{
					Form_pg_attribute att;

					tuple = SearchSysCache2(ATTNUM,
											ObjectIdGetDatum(typrelid),
											Int16GetDatum(j + 1));
					if (!HeapTupleIsValid(tuple))
						elog(ERROR, "cache lookup failed for composite type %u attributes", ret_typoid);
					
					att = (Form_pg_attribute) GETSTRUCT(tuple);
					if(strcmp(NameStr(att->attname), str) == 0)
					{
						FieldSelect	*fselect = makeNode(FieldSelect);

						fselect->arg = (Expr *) retval;
						fselect->fieldnum = j + 1;
						fselect->resulttype = att->atttypid;
						fselect->resulttypmod = att->atttypmod;
						fselect->resultcollid = att->attcollation;
						retval = (Node *) fselect;

						/* Iterative parsing of nested composite types */
						ret_typoid = att->atttypid;

						ReleaseSysCache(tuple);
						break;
					}
					ReleaseSysCache(tuple);
				}

				if (j == natts)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("function return type does not contain attribute %s",
							 str)));
			}
		}
	}
	else if (fdresult == FUNCDETAIL_AGGREGATE && !over)
	{
		/* aggregate function */
		Aggref	   *aggref = makeNode(Aggref);

		Assert(!found_in_with);
		aggref->aggfnoid = funcid;
		aggref->aggtype = rettype;
		/* aggcollid and inputcollid will be set by parse_collate.c */
		aggref->aggtranstype = InvalidOid;	/* will be set by planner */
		/* aggargtypes will be set by transformAggregateCall */
		/* aggdirectargs and args will be set by transformAggregateCall */
		/* aggorder and aggdistinct will be set by transformAggregateCall */
		aggref->aggfilter = agg_filter;
		aggref->aggstar = agg_star;
		aggref->aggvariadic = func_variadic;
		aggref->aggkind = aggkind;
		/* agglevelsup will be set by transformAggregateCall */
		aggref->aggsplit = AGGSPLIT_SIMPLE; /* planner might change this */
		aggref->location = location;

		/*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.  This is mere pedantry but some folks insisted ...
		 */
		if (fargs == NIL && !agg_star && !agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));

		if (retset)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("aggregates cannot return sets"),
					 parser_errposition(pstate, location)));

		/*
		 * We might want to support named arguments later, but disallow it for
		 * now.  We'd need to figure out the parsed representation (should the
		 * NamedArgExprs go above or below the TargetEntry nodes?) and then
		 * teach the planner to reorder the list properly.  Or maybe we could
		 * make transformAggregateCall do that?  However, if you'd also like
		 * to allow default arguments for aggregates, we'd need to do it in
		 * planning to avoid semantic problems.
		 */
		if (argnames != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates cannot use named arguments"),
					 parser_errposition(pstate, location)));

		/* parse_agg.c does additional aggregate-specific processing */
		transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);

		retval = (Node *) aggref;
	}
	else
	{
		/* window function */
		WindowFunc *wfunc = makeNode(WindowFunc);

		/* For KEEP aggregate function with OVER we do:
		 * 
		 * pass orderClause to original OVER sort list of PG for instead
		 * and during execution time, stop to advance trans state when
		 * we reach another group.
		 * 
		 * Note:
		 *  OVER clause should not have sort list, that's compatible with
		 *  opentenbase_ora, and should support plain aggregate function only
		 */
		if (ORA_MODE && keep != NULL)
		{
			/* opentenbase_ora window function with keep_clause */
			Assert(fdresult == FUNCDETAIL_AGGREGATE);

			if (over->orderClause != NULL)
				elog(ERROR, "ORDER BY is not allowed in over_clause after keep_clause");

			over->orderClause = keep->orderClause;
			over->frameOptions |= FRAMEOPTION_KEEP;

			/* have to reverse sort order, if choose LAST in dense_rank */
			if (!keep->first)
			{
				ListCell *lc;

				foreach (lc, over->orderClause)
				{
					SortBy *sb = lfirst_node(SortBy, lc);

					if (sb->sortby_nulls == SORTBY_NULLS_FIRST)
						sb->sortby_nulls = SORTBY_NULLS_LAST;
					else if (sb->sortby_nulls == SORTBY_NULLS_LAST)
						sb->sortby_nulls = SORTBY_NULLS_FIRST;

					/* default null ordering is LAST for ASC, FIRST for DESC */
					/* default ordering is ASC */
					if (sb->sortby_dir == SORTBY_DESC)
						sb->sortby_dir = SORTBY_ASC;
					else if (sb->sortby_dir == SORTBY_ASC ||
							 sb->sortby_dir == SORTBY_DEFAULT)
						sb->sortby_dir = SORTBY_DESC;
				}
			}
		} /* KEEP syntax end */

#ifdef _PG_ORCL_
		Assert(!found_in_with);
#endif
		Assert(over);			/* lack of this was checked above */
		Assert(!agg_within_group);	/* also checked above */

		wfunc->winfnoid = funcid;
		wfunc->wintype = rettype;
		/* wincollid and inputcollid will be set by parse_collate.c */
		wfunc->args = fargs;
		/* winref will be set by transformWindowFuncCall */
		wfunc->winstar = agg_star;
		wfunc->winagg = (fdresult == FUNCDETAIL_AGGREGATE);
		wfunc->aggfilter = agg_filter;
		wfunc->location = location;

		/*
		 * agg_star is allowed for aggregate functions but distinct isn't
		 */
		if (agg_distinct)
		{
			if (!ORA_MODE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DISTINCT is not implemented for window functions"),
						 parser_errposition(pstate, location)));
			if (!wfunc->winagg)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("DISTINCT is only allowed in aggregate window functions"),
						 parser_errposition(pstate, location)));
			wfunc->windistinct = true;
			agg_distinct = false;
			wfunc->winaggargtype = actual_arg_types[0];
		}

		/*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.  This is mere pedantry but some folks insisted ...
		 */
		if (wfunc->winagg && fargs == NIL && !agg_star)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));

#ifdef _PG_ORCL_
		/*To compate to opentenbase_ora, opentenbase_ora.listagg is a aggregate function with over-clause*/
		if (funcid != InvalidOid && ORA_MODE && is_ora_listagg_func(funcid))
		{
			/*do nothing, it just to avoid to report error by checking 'agg_order'*/
		}
		else
#endif
		{
			/*
			 * ordered aggs not allowed in windows yet
			 */
			if (agg_order != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("aggregate ORDER BY is not implemented for window functions"),
						 parser_errposition(pstate, location)));
		}

		/*
		 * FILTER is not yet supported with true window functions
		 */
		if (!wfunc->winagg && agg_filter)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("FILTER is not implemented for non-aggregate window functions"),
					 parser_errposition(pstate, location)));

		/*
		 * Window functions can't either take or return sets
		 */
		if (pstate->p_last_srf != last_srf)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("window function calls cannot contain set-returning function calls"),
					 errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
					 parser_errposition(pstate,
										exprLocation(pstate->p_last_srf))));

		if (retset)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("window functions cannot return sets"),
					 parser_errposition(pstate, location)));

		/* parse_agg.c does additional window-func-specific processing */
		transformWindowFuncCall(pstate, wfunc, over);

		retval = (Node *) wfunc;
	}

	/* if it returns a set, remember it for error checks at higher levels */
	if (retset)
		pstate->p_last_srf = retval;

	return retval;
}

#ifdef _PG_ORCL_
static List *
get_synonym_funnames(List *funcname, TypObjSynonInfo *info)
{
	List	*syn_nspnames = NIL;
	List	*syn_fnames = NIL;
	char	*schemaname = NULL,
			*fname = NULL,
			*dblink = NULL;
	List	*cand_fnames = NIL;
	ListCell	*ls,
				*lf;

	if (info != NULL)
	{
		info->info = OBJ_FUNC_OTHER;
		info->repinfo = SYNON_AS_OTHER;
	}

	DeconstructQualifiedName(funcname, &schemaname, &fname);
	syn_fnames = lappend(syn_fnames, fname);

	/* Check if using synonym for package */
	if (schemaname != NULL)
	{
		char	*pkg_name = schemaname,
				*pkg_spcname = NULL;
		Oid		pkg_spcId;

		/*
		 * Change list of char* to list of list. For some case, we have to
		 * store schema name also.
		 * For Example:
		 * We have a schema named s1, and a function s1.func1. We also have a
		 * package named: s1, and package have a public member function func1.
		 * And a synonym 'synon' point to s1. query 'select synon.func1' will be
		 * translated to 'select s1.func1', which will lead to using schema s1's
		 * func1. But 'select public.s1.func1' will using package's func1 which
		 * is also expected.
		 */
		syn_nspnames = lappend(syn_nspnames, list_make1(makeString(schemaname)));

		resolve_synonym(pkg_spcname, pkg_name, &pkg_spcname, &pkg_name, &dblink);
		if (pkg_name != NULL && dblink == NULL)
		{
			RangeVar	*tmp = makeNode(RangeVar);

			tmp->schemaname = pkg_spcname;
			tmp->relname = pkg_name;
			pkg_spcId = RangeVarGetCreationNamespace(tmp);
			if (OidIsValid(pkg_spcId))
			{
				/* Check if a valid package definition */

				HeapTuple	tuple;
				char		*pkgnsname = get_namespace_name(pkg_spcId);

				tuple = SearchSysCache2(PGPACKAGENMNS, CStringGetDatum(pkg_name), ObjectIdGetDatum(pkg_spcId));
				if (HeapTupleIsValid(tuple))
				{
					/*
					 * Put the package/schema as candidata looking up identifier
					 * also add schema name of package name.
					 */
					syn_nspnames = lappend(syn_nspnames, 
						list_make2(makeString(pkgnsname), makeString(pkg_name)));
					if (info != NULL)
						info->repinfo |= SYNON_AS_PKG;
					ReleaseSysCache(tuple);
				}
			}
		}
	}

	/* Check if using synonym for function */
	resolve_synonym(schemaname, fname, &schemaname, &fname, &dblink);
	if (fname != NULL && dblink == NULL)
	{
		if (info != NULL)
				info->repinfo |= SYNON_AS_FUNC;
		syn_fnames = lappend(syn_fnames, fname);
		if (schemaname != NULL)
			syn_nspnames = lappend(syn_nspnames, list_make1(makeString(schemaname)));
	}

	if (list_length(syn_nspnames) == 0)
	{
		foreach(lf, syn_fnames)
		{
			cand_fnames = lappend(cand_fnames, list_make1(makeString(lfirst(lf))));
		}
	}
	else
	{
		/*
		 * Put original schema/funcname first, and then change schema(package) name.
		 */
		foreach(ls, syn_nspnames)
		{
			bool	ins_front = false;
			List 	*lnsn = lfirst(ls);
			char	*n_spcname = strVal(linitial(lnsn));

			if (get_namespace_oid(n_spcname, true) != InvalidOid)
				ins_front = true;
			foreach(lf, syn_fnames)
			{
				char	*n_fname = lfirst(lf);
				List	*ln_ns_name = list_copy(lnsn);
				ln_ns_name = lappend(ln_ns_name, makeString(n_fname));

				/*
				 * Function parse logic will report "schema does not exist" to skip
				 * try the following candidates. So put this 'invalid' names at the
				 * end of the list.
				 */
				if (ins_front)
					cand_fnames = lcons(ln_ns_name, cand_fnames);
				else
					cand_fnames = lappend(cand_fnames, ln_ns_name);
			}
		}
	}

	return cand_fnames;
}

/*
 * check the func is opentenbase_ora.listagg, if it is, it will be special processed. 
 */
static bool 
is_ora_listagg_func(Oid func_oid)
{
	HeapTuple tp;
	Form_pg_proc fm_prc = NULL;
	bool ret = false;
	tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
	if(HeapTupleIsValid(tp))
	{
		fm_prc = (Form_pg_proc)GETSTRUCT(tp);
		if (fm_prc && fm_prc->pronamespace == OPENTENBASE_ORA_NAMESPACE &&
			strcasecmp(fm_prc->proname.data, "listagg") == 0)
		{
			ret = true;
		}
		ReleaseSysCache(tp);
	}
	return ret;
}

#endif


/* func_match_argtypes()
 *
 * Given a list of candidate functions (having the right name and number
 * of arguments) and an array of input datatype OIDs, produce a shortlist of
 * those candidates that actually accept the input datatypes (either exactly
 * or by coercion), and return the number of such candidates.
 *
 * Note that can_coerce_type will assume that UNKNOWN inputs are coercible to
 * anything, so candidates will not be eliminated on that basis.
 *
 * NB: okay to modify input list structure, as long as we find at least
 * one match.  If no match at all, the list must remain unmodified.
 */
int
func_match_argtypes(int nargs,
					Oid *input_typeids,
					FuncCandidateList raw_candidates,
					FuncCandidateList *candidates
#ifdef _PG_ORCL_
					, List *fargs
					, ParseState *pstate,
#endif
					bool use_ora_implicit)	/* return value */
{
	FuncCandidateList current_candidate;
	FuncCandidateList next_candidate;
	int			ncandidates = 0;

	*candidates = NULL;

	for (current_candidate = raw_candidates;
		 current_candidate != NULL;
		 current_candidate = next_candidate)
	{
		next_candidate = current_candidate->next;
		if (can_coerce_type(nargs, input_typeids, current_candidate->args,
							COERCION_IMPLICIT, use_ora_implicit))
		{
#ifdef _PG_ORCL_
			if (ORA_MODE && pstate)
			{
				if (!CompiledUDTMatched(current_candidate->oid, fargs, pstate->p_pl_state))
					continue;
			}
#endif
			current_candidate->next = *candidates;
			*candidates = current_candidate;
			ncandidates++;
		}
	}

	return ncandidates;
}								/* func_match_argtypes() */


/* func_select_candidate()
 *		Given the input argtype array and more than one candidate
 *		for the function, attempt to resolve the conflict.
 *
 * Returns the selected candidate if the conflict can be resolved,
 * otherwise returns NULL.
 *
 * Note that the caller has already determined that there is no candidate
 * exactly matching the input argtypes, and has pruned away any "candidates"
 * that aren't actually coercion-compatible with the input types.
 *
 * This is also used for resolving ambiguous operator references.  Formerly
 * parse_oper.c had its own, essentially duplicate code for the purpose.
 * The following comments (formerly in parse_oper.c) are kept to record some
 * of the history of these heuristics.
 *
 * OLD COMMENTS:
 *
 * This routine is new code, replacing binary_oper_select_candidate()
 * which dates from v4.2/v1.0.x days. It tries very hard to match up
 * operators with types, including allowing type coercions if necessary.
 * The important thing is that the code do as much as possible,
 * while _never_ doing the wrong thing, where "the wrong thing" would
 * be returning an operator when other better choices are available,
 * or returning an operator which is a non-intuitive possibility.
 * - thomas 1998-05-21
 *
 * The comments below came from binary_oper_select_candidate(), and
 * illustrate the issues and choices which are possible:
 * - thomas 1998-05-20
 *
 * current wisdom holds that the default operator should be one in which
 * both operands have the same type (there will only be one such
 * operator)
 *
 * 7.27.93 - I have decided not to do this; it's too hard to justify, and
 * it's easy enough to typecast explicitly - avi
 * [the rest of this routine was commented out since then - ay]
 *
 * 6/23/95 - I don't complete agree with avi. In particular, casting
 * floats is a pain for users. Whatever the rationale behind not doing
 * this is, I need the following special case to work.
 *
 * In the WHERE clause of a query, if a float is specified without
 * quotes, we treat it as float8. I added the float48* operators so
 * that we can operate on float4 and float8. But now we have more than
 * one matching operator if the right arg is unknown (eg. float
 * specified with quotes). This break some stuff in the regression
 * test where there are floats in quotes not properly casted. Below is
 * the solution. In addition to requiring the operator operates on the
 * same type for both operands [as in the code Avi originally
 * commented out], we also require that the operators be equivalent in
 * some sense. (see equivalentOpersAfterPromotion for details.)
 * - ay 6/95
 */
FuncCandidateList
func_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates)
{
	FuncCandidateList current_candidate,
				first_candidate,
				last_candidate;
	Oid		   *current_typeids;
	Oid			current_type;
	int			i;
	int			ncandidates;
	int			nbestMatch,
				nmatch,
				nunknowns;
	Oid			input_base_typeids[FUNC_MAX_ARGS];
	TYPCATEGORY slot_category[FUNC_MAX_ARGS],
				current_category;
	bool		current_is_preferred;
	bool		slot_has_preferred_type[FUNC_MAX_ARGS];
	bool		resolved_unknowns;

	/* protect local fixed-size arrays */
	if (nargs > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS)));

	/*
	 * If any input types are domains, reduce them to their base types. This
	 * ensures that we will consider functions on the base type to be "exact
	 * matches" in the exact-match heuristic; it also makes it possible to do
	 * something useful with the type-category heuristics. Note that this
	 * makes it difficult, but not impossible, to use functions declared to
	 * take a domain as an input datatype.  Such a function will be selected
	 * over the base-type function only if it is an exact match at all
	 * argument positions, and so was already chosen by our caller.
	 *
	 * While we're at it, count the number of unknown-type arguments for use
	 * later.
	 */
	nunknowns = 0;
	for (i = 0; i < nargs; i++)
	{
		if (input_typeids[i] != UNKNOWNOID)
			input_base_typeids[i] = getBaseType(input_typeids[i]);
		else
		{
			/* no need to call getBaseType on UNKNOWNOID */
			input_base_typeids[i] = UNKNOWNOID;
			nunknowns++;
		}
	}

	/*
	 * Run through all candidates and keep those with the most matches on
	 * exact types. Keep all candidates if none match.
	 */
	ncandidates = 0;
	nbestMatch = 0;
	last_candidate = NULL;
	for (current_candidate = candidates;
		 current_candidate != NULL;
		 current_candidate = current_candidate->next)
	{
		current_typeids = current_candidate->args;
		nmatch = 0;
		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] != UNKNOWNOID &&
				current_typeids[i] == input_base_typeids[i])
				nmatch++;
		}

		/* take this one as the best choice so far? */
		if ((nmatch > nbestMatch) || (last_candidate == NULL))
		{
			nbestMatch = nmatch;
			candidates = current_candidate;
			last_candidate = current_candidate;
			ncandidates = 1;
		}
		/* no worse than the last choice, so keep this one too? */
		else if (nmatch == nbestMatch)
		{
			last_candidate->next = current_candidate;
			last_candidate = current_candidate;
			ncandidates++;
		}
		/* otherwise, don't bother keeping this one... */
	}

	if (last_candidate)			/* terminate rebuilt list */
		last_candidate->next = NULL;

	if (ncandidates == 1)
		return candidates;

	/*
	 * Still too many candidates? Now look for candidates which have either
	 * exact matches or preferred types at the args that will require
	 * coercion. (Restriction added in 7.4: preferred type must be of same
	 * category as input type; give no preference to cross-category
	 * conversions to preferred types.)  Keep all candidates if none match.
	 */
	for (i = 0; i < nargs; i++) /* avoid multiple lookups */
		slot_category[i] = TypeCategory(input_base_typeids[i]);
	ncandidates = 0;
	nbestMatch = 0;
	last_candidate = NULL;
	for (current_candidate = candidates;
		 current_candidate != NULL;
		 current_candidate = current_candidate->next)
	{
		current_typeids = current_candidate->args;
		nmatch = 0;
		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] != UNKNOWNOID)
			{
				if (current_typeids[i] == input_base_typeids[i] ||
					IsPreferredType(slot_category[i], current_typeids[i]))
					nmatch++;
			}
		}

		if ((nmatch > nbestMatch) || (last_candidate == NULL))
		{
			nbestMatch = nmatch;
			candidates = current_candidate;
			last_candidate = current_candidate;
			ncandidates = 1;
		}
		else if (nmatch == nbestMatch)
		{
			last_candidate->next = current_candidate;
			last_candidate = current_candidate;
			ncandidates++;
		}
	}

	if (last_candidate)			/* terminate rebuilt list */
		last_candidate->next = NULL;

	if (ncandidates == 1)
		return candidates;

	/*
	 * The next step examines each unknown argument position to see if we can
	 * determine a "type category" for it.  If any candidate has an input
	 * datatype of STRING category, use STRING category (this bias towards
	 * STRING is appropriate since unknown-type literals look like strings).
	 * Otherwise, if all the candidates agree on the type category of this
	 * argument position, use that category.  Otherwise, fail because we
	 * cannot determine a category.
	 *
	 * If we are able to determine a type category, also notice whether any of
	 * the candidates takes a preferred datatype within the category.
	 *
	 * Having completed this examination, remove candidates that accept the
	 * wrong category at any unknown position.  Also, if at least one
	 * candidate accepted a preferred type at a position, remove candidates
	 * that accept non-preferred types.  If just one candidate remains, return
	 * that one.  However, if this rule turns out to reject all candidates,
	 * keep them all instead.
	 */
	resolved_unknowns = false;
	for (i = 0; i < nargs; i++)
	{
		bool		have_conflict;

		if (input_base_typeids[i] != UNKNOWNOID)
			continue;
		resolved_unknowns = true;	/* assume we can do it */
		slot_category[i] = TYPCATEGORY_INVALID;
		slot_has_preferred_type[i] = false;
		have_conflict = false;
		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			current_typeids = current_candidate->args;
			current_type = current_typeids[i];
			get_type_category_preferred(current_type,
										&current_category,
										&current_is_preferred);
			if (slot_category[i] == TYPCATEGORY_INVALID)
			{
				/* first candidate */
				slot_category[i] = current_category;
				slot_has_preferred_type[i] = current_is_preferred;
			}
			else if (current_category == slot_category[i])
			{
				/* more candidates in same category */
				slot_has_preferred_type[i] |= current_is_preferred;
			}
			else
			{
				/* category conflict! */
				if (current_category == TYPCATEGORY_STRING)
				{
					/* STRING always wins if available */
					slot_category[i] = current_category;
					slot_has_preferred_type[i] = current_is_preferred;
				}
				else
				{
					/*
					 * Remember conflict, but keep going (might find STRING)
					 */
					have_conflict = true;
				}
			}
		}
		if (have_conflict && slot_category[i] != TYPCATEGORY_STRING)
		{
			/* Failed to resolve category conflict at this position */
			resolved_unknowns = false;
			break;
		}
	}

	if (resolved_unknowns)
	{
		/* Strip non-matching candidates */
		ncandidates = 0;
		first_candidate = candidates;
		last_candidate = NULL;
		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			bool		keepit = true;

			current_typeids = current_candidate->args;
			for (i = 0; i < nargs; i++)
			{
				if (input_base_typeids[i] != UNKNOWNOID)
					continue;
				current_type = current_typeids[i];
				get_type_category_preferred(current_type,
											&current_category,
											&current_is_preferred);
				if (current_category != slot_category[i])
				{
					keepit = false;
					break;
				}
				if (slot_has_preferred_type[i] && !current_is_preferred)
				{
					keepit = false;
					break;
				}
			}
			if (keepit)
			{
				/* keep this candidate */
				last_candidate = current_candidate;
				ncandidates++;
			}
			else
			{
				/* forget this candidate */
				if (last_candidate)
					last_candidate->next = current_candidate->next;
				else
					first_candidate = current_candidate->next;
			}
		}

		/* if we found any matches, restrict our attention to those */
		if (last_candidate)
		{
			candidates = first_candidate;
			/* terminate rebuilt list */
			last_candidate->next = NULL;
		}

		if (ncandidates == 1)
			return candidates;
	}

	/*
	 * Last gasp: if there are both known- and unknown-type inputs, and all
	 * the known types are the same, assume the unknown inputs are also that
	 * type, and see if that gives us a unique match.  If so, use that match.
	 *
	 * NOTE: for a binary operator with one unknown and one non-unknown input,
	 * we already tried this heuristic in binary_oper_exact().  However, that
	 * code only finds exact matches, whereas here we will handle matches that
	 * involve coercion, polymorphic type resolution, etc.
	 */
	if (nunknowns > 0 && nunknowns < nargs)
	{
		Oid			known_type = UNKNOWNOID;

		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] == UNKNOWNOID)
				continue;
			if (known_type == UNKNOWNOID)	/* first known arg? */
				known_type = input_base_typeids[i];
			else if (known_type != input_base_typeids[i])
			{
				/* oops, not all match */
				known_type = UNKNOWNOID;
				break;
			}
		}

		if (known_type != UNKNOWNOID)
		{
			/* okay, just one known type, apply the heuristic */
			for (i = 0; i < nargs; i++)
				input_base_typeids[i] = known_type;
			ncandidates = 0;
			last_candidate = NULL;
			for (current_candidate = candidates;
				 current_candidate != NULL;
				 current_candidate = current_candidate->next)
			{
				current_typeids = current_candidate->args;
				if (can_coerce_type(nargs, input_base_typeids, current_typeids,
									COERCION_IMPLICIT, false))
				{
					if (++ncandidates > 1)
						break;	/* not unique, give up */
					last_candidate = current_candidate;
				}
			}
			if (ncandidates == 1)
			{
				/* successfully identified a unique match */
				last_candidate->next = NULL;
				return last_candidate;
			}
		}
	}

	/*
	 * The opentenbase_ora implicit coercion context has lower priority than common
	 * implicit coercion context to compatible with v5.06.5.1. We will choose
	 * candidates which not contain the opentenbase_ora implicit coercion.
	 *
	 * this would be enabled also in none-opentenbase_ora mode if enable_lightweight_ora_syntax is on
	 */
	if (ENABLE_ORA_IMPLICIT_COERCION)
	{
		ncandidates = 0;
		last_candidate = NULL;

		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			bool	has_ora_imp_coer = false;

			current_typeids = current_candidate->args;

			for (i = 0; i < nargs; i++)
			{
				/* It contains an opentenbase_ora implicit coercion */
				if (input_base_typeids[i] != UNKNOWNOID &&
					getCastContext(input_base_typeids[i], current_typeids[i]) == COERCION_CODE_ORA_IMPLICIT)
				{
					has_ora_imp_coer = true;
					break;
				}
			}

			/* We just pick these candidates which don't contain the opentenbase_ora implicit coercion */
			if (!has_ora_imp_coer)
			{
				if (++ncandidates > 1)
					break;	/* not unique, give up */

				last_candidate = current_candidate;
			}
		}

		if (ncandidates == 1)
		{
			/* successfully identified a unique match */
			last_candidate->next = NULL;
			return last_candidate;
		}
		else
		{
			/*
			 * We can't use the origin rules like version 5651 to find
			 * the best candidate, so we will use the new rules.
			 * We will try our best to remove the assigment coercions
			 * which are unsafe.
			 */
			int	least_a_coercion_num = nargs;

			last_candidate = NULL;
			ncandidates = 0;

			for (current_candidate = candidates;
				 current_candidate != NULL;
				 current_candidate = current_candidate->next)
			{
				int	a_coercion_num = 0;

				current_typeids = current_candidate->args;

				for (i = 0; i < nargs; i++)
				{
					/* It contains an assignment coercion (castcontext is assignment or \0) */
					if (input_base_typeids[i] != UNKNOWNOID)
					{
						char	castcontext = getCastContext(input_base_typeids[i], current_typeids[i]);

						if (castcontext == COERCION_CODE_ASSIGNMENT)
							a_coercion_num++;
					}
				}

				if ((a_coercion_num < least_a_coercion_num) || (last_candidate == NULL))
				{
					least_a_coercion_num = a_coercion_num;
					candidates = current_candidate;
					last_candidate = current_candidate;
					ncandidates = 1;
				}
				else if (a_coercion_num == least_a_coercion_num)
				{
					last_candidate->next = current_candidate;
					last_candidate = current_candidate;
					ncandidates++;
				}
			}

			if (last_candidate)			/* terminate rebuilt list */
				last_candidate->next = NULL;

			if (ncandidates == 1)
				return candidates;
		}
	}

	/* Port gregsun's v5651 commit, but it is so rude, move it to the here */
	if (ORA_MODE || enable_lightweight_ora_syntax)
	{
		/*
		 * Still more candidates? try if the catagory of input argtype is same as the
		 * catagory of function argtype.
		 */
		ncandidates = 0;
		nbestMatch = 0;
		last_candidate = NULL;
		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			current_typeids = current_candidate->args;
			nmatch = 0;
			for (i = 0; i < nargs; i++)
			{
				if (input_base_typeids[i] != UNKNOWNOID)
				{
					if (TypeCategory(current_typeids[i]) == slot_category[i])
						nmatch++;
				}
			}

			if ((nmatch > nbestMatch) || (last_candidate == NULL))
			{
				nbestMatch = nmatch;
				candidates = current_candidate;
				last_candidate = current_candidate;
				ncandidates = 1;
			}
			else if (nmatch == nbestMatch)
			{
				last_candidate->next = current_candidate;
				last_candidate = current_candidate;
				ncandidates++;
			}
		}

		if (last_candidate)			/* terminate rebuilt list */
			last_candidate->next = NULL;

		if (ncandidates == 1)
			return candidates;
	}

	/* still too many candidates, try to use category priority */
	if (ncandidates > 1 && (!ORA_MODE || enable_type_priority_in_ora_mode))
	{
		TYPCATEGORY maxCatalog[FUNC_MAX_ARGS];
		Oid         maxTyp[FUNC_MAX_ARGS];
		int         type_priority[FUNC_MAX_ARGS];
		last_candidate = NULL;
		ncandidates = 0;
		nbestMatch = 0;

		for (i = 0; i < nargs; i++)
		{
			input_base_typeids[i] = getBaseType(input_typeids[i]);
			slot_category[i] = TypeCategory(input_base_typeids[i]);
			maxCatalog[i] = 'X';
			maxTyp[i] = UNKNOWNOID;
		}

		/* Find out in type category which has the highest priority*/
		for (current_candidate = candidates; current_candidate != NULL;
		     current_candidate = current_candidate->next)
		{
			current_typeids = current_candidate->args;
			for (i = 0; i < nargs; i++)
			{
				if (GetCategoryPriority(TypeCategory(current_typeids[i])) >
				    GetCategoryPriority(maxCatalog[i]))
				{
					maxCatalog[i] = TypeCategory(current_typeids[i]);
					maxTyp[i] = current_typeids[i];
				}
				else if ((TypeCategory(current_typeids[i]) == maxCatalog[i]) &&
				         (GetPriority(maxTyp[i]) < GetPriority(current_typeids[i])))
				{
					maxTyp[i] = current_typeids[i];
				}
			}
		}

		/* If the input parameter's priority is higher than the biggest priority, the biggest
		 * priority shall prevail*/
		for (i = 0; i < nargs; i++)
		{
			if ((GetCategoryPriority(slot_category[i]) > GetCategoryPriority(maxCatalog[i])) ||
			    (slot_category[i] == 'X'))
			{

				slot_category[i] = maxCatalog[i];
				input_base_typeids[i] = maxTyp[i];
			}
			else if ((slot_category[i] == maxCatalog[i]) &&
			         (GetPriority(input_base_typeids[i]) > GetPriority(maxTyp[i])))
			{
				input_base_typeids[i] = maxTyp[i];
			}

			type_priority[i] = GetPriority(input_base_typeids[i]);
		}

		for (current_candidate = candidates; current_candidate != NULL; current_candidate = current_candidate->next) {

            current_typeids = current_candidate->args;
            nmatch = 0;

            for (i = 0; i < nargs; i++) {
                /* If the type has higher priority is chosen */
                if (GetCategoryPriority(slot_category[i]) < GetCategoryPriority(TypeCategory(current_typeids[i]))) {
                    nmatch++;
                } else if (GetCategoryPriority(slot_category[i]) ==
                           GetCategoryPriority(TypeCategory(current_typeids[i]))) {
                    if (type_priority[i] <= GetPriority(current_typeids[i])) {
                        nmatch++;
                    }
                }
            }

			if ((nmatch > nbestMatch) || (last_candidate == NULL))
			{
				nbestMatch = nmatch;
				candidates = current_candidate;
				last_candidate = current_candidate;
				ncandidates = 1;
			}
			else if (nmatch == nbestMatch)
			{
				last_candidate->next = current_candidate;
				last_candidate = current_candidate;
				ncandidates++;
			}
        }
        if (last_candidate) /* terminate rebuilt list */
            last_candidate->next = NULL;
	}

	if (ncandidates == 1)
        return candidates;

	/* Here, we have no idea, how about to get the only opentenbase_ora.xxx in opentenbase_ora mode */
	if (ENABLE_ORA_IMPLICIT_COERCION)
	{
		last_candidate = NULL;
		ncandidates = 0;
		/* Pick the opentenbase_ora.xxx if there are only one */
		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			if (current_candidate->nspid == OPENTENBASE_ORA_NAMESPACE)
			{
				if (++ncandidates > 1)
					break;	/* not unique, give up */

				last_candidate = current_candidate;
			}
		}

		if (ncandidates == 1)
		{
			/* successfully identified a unique match */
			last_candidate->next = NULL;
			return last_candidate;
		}
	}

	return NULL;				/* failed to select a best candidate */
}								/* func_select_candidate() */

/* GetCategoryPriority()
 *		get category priority
 *
 * for a given categoryoid of any
 * 'X': unknown
 * 'U': User
 * 'B' Boolean
 * 'G' Geometric
 * 'I' Network
 * the category priority should be 0
 */
static int
GetCategoryPriority(TYPCATEGORY categoryoid)
{
	int result = 0;

	switch (categoryoid)
	{
		case ('D'): /*Datetime*/
			result = 1;
			break;
		case ('T'): /*Timespan*/
			result = 2;
			break;
		case ('N'): /*Numeric*/
			result = 3;
			break;
		case ('S'): /*String*/
			result = 4;
			break;
		default:
			result = 0;
			break;
	}

	return result;
}

/* GetPriority()
 *		get type priority
 */
static int
GetPriority(Oid typeoid)
{
	int result = 0;

	switch (typeoid)
	{
		/* bool */
		case (BOOLOID):
			result = 0;
			break;

		/* string */
		case (CHAROID):
		case (RIDOID):
			result = 0;
			break;
		case (NAMEOID):
			result = 1;
			break;
		case (BPCHAROID):
			result = 2;
			break;
		case (VARCHAROID):
			result = 3;
			break;
		/*
		 * CHARACTER VARYING/CHAR VARYING is varchar in opentenbase now,
		 * but is varchar2 in opentenbase_ora.
		 * NATIONAL CHARACTER VARYING/NATIONAL CHAR VARYING/NCHAR VARYING
		 * is varchar in opentenbase now, but is nvarchar2 in opentenbase_ora.
		 * We prefer varchar2 or nvarchar2 in opentenbase_ora mode to be best canidate,
		 * so they are higher priority.
		 */
		case (NVARCHAR2OID):
		case (VARCHAR2OID):
			result = 4;
			break;
		case (TEXTOID):
			result = 5;
			break;

		/* bitstring */
		case (BITOID):
			result = 0;
			break;
		case (VARBITOID):
			result = 1;
			break;

		/* numeric */
		case (CASHOID):
			result = 0;
			break;
		case (INT2OID):
			result = 1;
			break;
		case (INT4OID):
			result = 2;
			break;
		case (OIDOID):
			result = 3;
			break;
		case (INT8OID):
			result = 4;
			break;
		case (FLOAT4OID):
			result = 5;
			break;
		case (FLOAT8OID):
			result = 6;
			break;
		case (NUMERICOID):
			result = 7;
			break;

		/* datetime */
		case (TIMEOID):
			result = 0;
			break;
		case (TIMETZOID):
			result = 1;
			break;
		case (ABSTIMEOID):
			result = 2;
			break;
		case (DATEOID):
			result = 3;
			break;
		case (TIMESTAMPOID):
			result = 5;
			break;
		case (TIMESTAMPTZOID):
			result = 7;
			break;

		/* timespan */
		case (RELTIMEOID):
			result = 0;
			break;
		case (TINTERVALOID):
			result = 1;
			break;
		case (INTERVALOID):
			result = 2;
			break;

		default:
			result = 0;
			break;
	}
	return result;
}

/* func_get_detail()
 *
 * Find the named function in the system catalogs.
 *
 * Attempt to find the named function in the system catalogs with
 * arguments exactly as specified, so that the normal case (exact match)
 * is as quick as possible.
 *
 * If an exact match isn't found:
 *	1) check for possible interpretation as a type coercion request
 *	2) apply the ambiguous-function resolution rules
 *
 * Return values *funcid through *true_typeids receive info about the function.
 * If argdefaults isn't NULL, *argdefaults receives a list of any default
 * argument expressions that need to be added to the given arguments.
 *
 * When processing a named- or mixed-notation call (ie, fargnames isn't NIL),
 * the returned true_typeids and argdefaults are ordered according to the
 * call's argument ordering: first any positional arguments, then the named
 * arguments, then defaulted arguments (if needed and allowed by
 * expand_defaults).  Some care is needed if this information is to be compared
 * to the function's pg_proc entry, but in practice the caller can usually
 * just work with the call's argument ordering.
 *
 * We rely primarily on fargnames/nargs/argtypes as the argument description.
 * The actual expression node list is passed in fargs so that we can check
 * for type coercion of a constant.  Some callers pass fargs == NIL indicating
 * they don't need that check made.  Note also that when fargnames isn't NIL,
 * the fargs list must be passed if the caller wants actual argument position
 * information to be returned into the NamedArgExpr nodes.
 */
#ifdef _PG_ORCL_
FuncDetailCode
func_get_detail(List *funcname,
				List *fargs,
				List *fargnames,
				int nargs,
				Oid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				Oid *funcid,	/* return value */
				Oid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				Oid *vatype,	/* return value */
				Oid **true_typeids, /* return value */
				List **argdefaults, /* optional return value */
				ParseState *pstate)
{
	FuncDetailCode	func_code;

	push_current_package_namespace(); /* If a package function? */
	func_code = func_get_detail_with_funcs(funcname, fargs, fargnames, nargs,
										argtypes, expand_variadic,
										expand_defaults, funcid, rettype,
										retset, nvargs, vatype, true_typeids,
										argdefaults, NULL, NULL, NULL, pstate);
	pop_current_package_namespace();

	return func_code;
}

FuncDetailCode
func_get_detail_with_funcs(List *funcname,
				List *fargs,
				List *fargnames,
				int nargs,
				Oid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				Oid *funcid,	/* return value */
				Oid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				Oid *vatype,	/* return value */
				Oid **true_typeids, /* return value */
				List **argdefaults, /* optional return value */
				List *with_funcs,
				int *with_funcid,
				bool *keep_looking,
				ParseState *pstate)
#else
FuncDetailCode
func_get_detail(List *funcname,
				List *fargs,
				List *fargnames,
				int nargs,
				Oid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				Oid *funcid,	/* return value */
				Oid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				Oid *vatype,	/* return value */
				Oid **true_typeids, /* return value */
				List **argdefaults,
				ParseState *pstate) /* optional return value */
#endif
{
	FuncCandidateList raw_candidates;
	FuncCandidateList best_candidate;

	/* Passing NULL for argtypes is no longer allowed */
	Assert(argtypes);

	/* initialize output arguments to silence compiler warnings */
	*funcid = InvalidOid;
	*rettype = InvalidOid;
	*retset = false;
	*nvargs = 0;
	*vatype = InvalidOid;
	*true_typeids = NULL;
	if (argdefaults)
		*argdefaults = NIL;

	/* Get list of possible candidates from namespace search */
#ifdef _PG_ORCL_
	if (with_funcid)
		*with_funcid = -1;
	raw_candidates
		= FuncnameGetCandidatesWithFuncs(funcname, nargs, fargnames,
										   expand_variadic, expand_defaults,
										   false, with_funcs, keep_looking);
#else
	raw_candidates = FuncnameGetCandidates(funcname, nargs, fargnames,
										   expand_variadic, expand_defaults,
										   false);
#endif

	/*
	 * Quickly check if there is an exact match to the input datatypes (there
	 * can be only one)
	 */
	for (best_candidate = raw_candidates;
		 best_candidate != NULL;
		 best_candidate = best_candidate->next)
	{
		if (memcmp(argtypes, best_candidate->args, nargs * sizeof(Oid)) == 0)
			break;
	}

	if (best_candidate == NULL)
	{
		/*
		 * If we didn't find an exact match, next consider the possibility
		 * that this is really a type-coercion request: a single-argument
		 * function call where the function name is a type name.  If so, and
		 * if the coercion path is RELABELTYPE or COERCEVIAIO, then go ahead
		 * and treat the "function call" as a coercion.
		 *
		 * This interpretation needs to be given higher priority than
		 * interpretations involving a type coercion followed by a function
		 * call, otherwise we can produce surprising results. For example, we
		 * want "text(varchar)" to be interpreted as a simple coercion, not as
		 * "text(name(varchar))" which the code below this point is entirely
		 * capable of selecting.
		 *
		 * We also treat a coercion of a previously-unknown-type literal
		 * constant to a specific type this way.
		 *
		 * The reason we reject COERCION_PATH_FUNC here is that we expect the
		 * cast implementation function to be named after the target type.
		 * Thus the function will be found by normal lookup if appropriate.
		 *
		 * The reason we reject COERCION_PATH_ARRAYCOERCE is mainly that you
		 * can't write "foo[] (something)" as a function call.  In theory
		 * someone might want to invoke it as "_foo (something)" but we have
		 * never supported that historically, so we can insist that people
		 * write it as a normal cast instead.
		 *
		 * We also reject the specific case of COERCEVIAIO for a composite
		 * source type and a string-category target type.  This is a case that
		 * find_coercion_pathway() allows by default, but experience has shown
		 * that it's too commonly invoked by mistake.  So, again, insist that
		 * people use cast syntax if they want to do that.
		 *
		 * NB: it's important that this code does not exceed what coerce_type
		 * can do, because the caller will try to apply coerce_type if we
		 * return FUNCDETAIL_COERCION.  If we return that result for something
		 * coerce_type can't handle, we'll cause infinite recursion between
		 * this module and coerce_type!
		 */
		if (nargs == 1 && fargs != NIL && fargnames == NIL)
		{
			Oid			targetType = FuncNameAsType(funcname);

			if (OidIsValid(targetType))
			{
				Oid			sourceType = argtypes[0];
				Node	   *arg1 = linitial(fargs);
				bool		iscoercion;

				if (sourceType == UNKNOWNOID && IsA(arg1, Const))
				{
					/* always treat typename('literal') as coercion */
					iscoercion = true;
				}
				else
				{
					CoercionPathType cpathtype;
					Oid			cfuncid;

					cpathtype = find_coercion_pathway(targetType, sourceType,
													  COERCION_EXPLICIT,
													  &cfuncid);
					switch (cpathtype)
					{
						case COERCION_PATH_RELABELTYPE:
							iscoercion = true;
							break;
						case COERCION_PATH_COERCEVIAIO:
							if ((sourceType == RECORDOID ||
								 ISCOMPLEX(sourceType)) &&
								TypeCategory(targetType) == TYPCATEGORY_STRING)
								iscoercion = false;
							else
								iscoercion = true;
							break;
						default:
							iscoercion = false;
							break;
					}
				}

				if (iscoercion)
				{
					/* Treat it as a type coercion */
					*funcid = InvalidOid;
					*rettype = targetType;
					*retset = false;
					*nvargs = 0;
					*vatype = InvalidOid;
					*true_typeids = argtypes;
					return FUNCDETAIL_COERCION;
				}
			}
		}

		/*
		 * didn't find an exact match, so now try to match up candidates...
		 */
		if (raw_candidates != NULL)
		{
			FuncCandidateList current_candidates;
			int			ncandidates;

			ncandidates = func_match_argtypes(nargs,
											  argtypes,
											  raw_candidates,
											  &current_candidates,
											  fargs, pstate,
											  (!ORA_MODE && enable_lightweight_ora_syntax));
			if (ORA_MODE && ncandidates == 0)
			{
				ncandidates = func_match_argtypes(nargs,
												  argtypes,
												  raw_candidates,
												  &current_candidates,
												  fargs, pstate, true);
			}

			/* one match only? then run with it... */
			if (ncandidates == 1)
				best_candidate = current_candidates;

			/*
			 * multiple candidates? then better decide or throw an error...
			 */
			else if (ncandidates > 1)
			{
				best_candidate = func_select_candidate(nargs,
													   argtypes,
													   current_candidates);

				/*
				 * If we were able to choose a best candidate, we're done.
				 * Otherwise, ambiguous function call.
				 */
				if (!best_candidate)
					return FUNCDETAIL_MULTIPLE;
			}
		}
	}

	if (best_candidate)
	{
		HeapTuple	ftup;
		Form_pg_proc pform;
		FuncDetailCode result;

		/*
		 * If processing named args or expanding variadics or defaults, the
		 * "best candidate" might represent multiple equivalently good
		 * functions; treat this case as ambiguous.
		 */
#ifdef _PG_ORCL_
		if (with_funcs == NULL)
		{
			/* Not WITH FUNCTION case */
			if (!OidIsValid(best_candidate->oid))
				return FUNCDETAIL_MULTIPLE;
		}
		else if (best_candidate->with_funcid == -1)
			return FUNCDETAIL_MULTIPLE;
#else
		if (!OidIsValid(best_candidate->oid))
			return FUNCDETAIL_MULTIPLE;
#endif

		/* LIGHTWEIGHT_ORA modes require invoking the TO_DATE(text, text) function in the opentenbase-ora-style manner. */
		if (enable_lightweight_ora_syntax && best_candidate->oid == PG_TODATE_TEXTTEXT_OID)
			best_candidate->oid = ORA_TODATE_TEXTTEXT_OID;

		/*
		 * We disallow VARIADIC with named arguments unless the last argument
		 * (the one with VARIADIC attached) actually matched the variadic
		 * parameter.  This is mere pedantry, really, but some folks insisted.
		 */
		if (fargnames != NIL && !expand_variadic && nargs > 0 &&
			best_candidate->argnumbers[nargs - 1] != nargs - 1)
			return FUNCDETAIL_NOTFOUND;

		*funcid = best_candidate->oid;
		*nvargs = best_candidate->nvargs;
		*true_typeids = best_candidate->args;
#ifdef _PG_ORCL_
		if (with_funcid)
			*with_funcid = best_candidate->with_funcid;
#endif

		/*
		 * If processing named args, return actual argument positions into
		 * NamedArgExpr nodes in the fargs list.  This is a bit ugly but not
		 * worth the extra notation needed to do it differently.
		 */
		if (best_candidate->argnumbers != NULL)
		{
			int			i = 0;
			ListCell   *lc;

			foreach(lc, fargs)
			{
				NamedArgExpr *na = (NamedArgExpr *) lfirst(lc);

				if (IsA(na, NamedArgExpr))
					na->argnumber = best_candidate->argnumbers[i];
				i++;
			}
		}

#ifdef _PG_ORCL_
		if (with_funcs != NULL)
		{
			Assert(best_candidate->oid == InvalidOid);
			ftup = GetWithFunctionTupleById(with_funcs,
									best_candidate->with_funcid);
		}
		else
		{
#endif
			ftup = SearchSysCache1(PROCOID,
								   ObjectIdGetDatum(best_candidate->oid));
			if (!HeapTupleIsValid(ftup))	/* should not happen */
				elog(ERROR, "cache lookup failed for function %u",
					 best_candidate->oid);
#ifdef _PG_ORCL_
		}
#endif
		pform = (Form_pg_proc) GETSTRUCT(ftup);
		*rettype = pform->prorettype;
		*retset = pform->proretset;
		*vatype = pform->provariadic;

		/* fetch default args if caller wants 'em */
		if (argdefaults && best_candidate->ndargs > 0)
		{
			Datum		proargdefaults;
			bool		isnull;
			char	   *str;
			List	   *defaults;

			/* shouldn't happen, FuncnameGetCandidates messed up */
			if (best_candidate->ndargs > pform->pronargdefaults)
				elog(ERROR, "not enough default arguments");

			proargdefaults = SysCacheGetAttr(PROCOID, ftup,
											 Anum_pg_proc_proargdefaults,
											 &isnull);
			Assert(!isnull);
			str = TextDatumGetCString(proargdefaults);
			defaults = castNode(List, stringToNode(str));
			pfree(str);

			/* Delete any unused defaults from the returned list */
			if (best_candidate->argnumbers != NULL)
			{
				/*
				 * This is a bit tricky in named notation, since the supplied
				 * arguments could replace any subset of the defaults.  We
				 * work by making a bitmapset of the argnumbers of defaulted
				 * arguments, then scanning the defaults list and selecting
				 * the needed items.  (This assumes that defaulted arguments
				 * should be supplied in their positional order.)
				 */
				Bitmapset  *defargnumbers;
				int		   *firstdefarg;
				List	   *newdefaults;
				ListCell   *lc;
				int			i;
				bool		expandDefaults = false;

				if (ORA_MODE && defaults != NIL)
				{
					foreach(lc, defaults)
					{
						if (lfirst(lc) == NIL)
						{
							expandDefaults = true;
							break;
						}
							
					}
				}

				defargnumbers = NULL;
				firstdefarg = &best_candidate->argnumbers[best_candidate->nargs - best_candidate->ndargs];
				for (i = 0; i < best_candidate->ndargs; i++)
					defargnumbers = bms_add_member(defargnumbers,
												   firstdefarg[i]);
				newdefaults = NIL;
				if (expandDefaults)
				{
					i = 0;
					foreach(lc, defaults)
					{
						if (bms_is_member(i, defargnumbers))
						{
							if (lfirst(lc) != NIL)
								newdefaults = lappend(newdefaults, lfirst(lc));
							else
								elog(ERROR, "%dth default parameter not found, check proargdefaults", i);
						}
						i++;
					}
				}
				else
				{
					i = pform->pronargs - pform->pronargdefaults;
					foreach(lc, defaults)
					{
						if (lfirst(lc) != NIL)
						{
							if (bms_is_member(i, defargnumbers))
								newdefaults = lappend(newdefaults, lfirst(lc));
							i++;
						}
					}
				}
				Assert(list_length(newdefaults) == best_candidate->ndargs);
				bms_free(defargnumbers);
				*argdefaults = newdefaults;
			}
			else
			{
				/*
				 * Defaults for positional notation are lots easier; just
				 * remove any unwanted ones from the front.
				 */
				int			ndelete;

				ndelete = list_length(defaults) - best_candidate->ndargs;
				while (ndelete-- > 0)
					defaults = list_delete_first(defaults);
				*argdefaults = defaults;
			}
		}

		switch (pform->prokind)
		{
			case PROKIND_AGGREGATE:
				result = FUNCDETAIL_AGGREGATE;
				break;
			case PROKIND_FUNCTION:
				result = FUNCDETAIL_NORMAL;
				break;
			case PROKIND_PROCEDURE:
				result = FUNCDETAIL_PROCEDURE;
				break;
			case PROKIND_WINDOW:
				result = FUNCDETAIL_WINDOWFUNC;
				break;
			default:
				elog(ERROR, "unrecognized prokind: %c", pform->prokind);
				result = FUNCDETAIL_NORMAL; /* keep compiler quiet */
				break;
		}

#ifdef _PG_ORCL_
		if (with_funcs == NULL)
#endif
			ReleaseSysCache(ftup);
		return result;
	}

	return FUNCDETAIL_NOTFOUND;
}


/*
 * unify_hypothetical_args()
 *
 * Ensure that each hypothetical direct argument of a hypothetical-set
 * aggregate has the same type as the corresponding aggregated argument.
 * Modify the expressions in the fargs list, if necessary, and update
 * actual_arg_types[].
 *
 * If the agg declared its args non-ANY (even ANYELEMENT), we need only a
 * sanity check that the declared types match; make_fn_arguments will coerce
 * the actual arguments to match the declared ones.  But if the declaration
 * is ANY, nothing will happen in make_fn_arguments, so we need to fix any
 * mismatch here.  We use the same type resolution logic as UNION etc.
 */
static void
unify_hypothetical_args(ParseState *pstate,
						List *fargs,
						int numAggregatedArgs,
						Oid *actual_arg_types,
						Oid *declared_arg_types)
{
	Node	   *args[FUNC_MAX_ARGS];
	int			numDirectArgs,
				numNonHypotheticalArgs;
	int			i;
	ListCell   *lc;

	numDirectArgs = list_length(fargs) - numAggregatedArgs;
	numNonHypotheticalArgs = numDirectArgs - numAggregatedArgs;
	/* safety check (should only trigger with a misdeclared agg) */
	if (numNonHypotheticalArgs < 0)
		elog(ERROR, "incorrect number of arguments to hypothetical-set aggregate");

	/* Deconstruct fargs into an array for ease of subscripting */
	i = 0;
	foreach(lc, fargs)
	{
		args[i++] = (Node *) lfirst(lc);
	}

	/* Check each hypothetical arg and corresponding aggregated arg */
	for (i = numNonHypotheticalArgs; i < numDirectArgs; i++)
	{
		int			aargpos = numDirectArgs + (i - numNonHypotheticalArgs);
		Oid			commontype;

		/* A mismatch means AggregateCreate didn't check properly ... */
		if (declared_arg_types[i] != declared_arg_types[aargpos])
			elog(ERROR, "hypothetical-set aggregate has inconsistent declared argument types");

		/* No need to unify if make_fn_arguments will coerce */
		if (declared_arg_types[i] != ANYOID)
			continue;

		/*
		 * Select common type, giving preference to the aggregated argument's
		 * type (we'd rather coerce the direct argument once than coerce all
		 * the aggregated values).
		 */
		commontype = select_common_type(pstate,
										list_make2(args[aargpos], args[i]),
										"WITHIN GROUP",
										NULL);

		/*
		 * Perform the coercions.  We don't need to worry about NamedArgExprs
		 * here because they aren't supported with aggregates.
		 */
		args[i] = coerce_type(pstate,
							  args[i],
							  actual_arg_types[i],
							  commontype, -1,
							  COERCION_IMPLICIT,
							  COERCE_IMPLICIT_CAST,
							  -1);
		actual_arg_types[i] = commontype;
		args[aargpos] = coerce_type(pstate,
									args[aargpos],
									actual_arg_types[aargpos],
									commontype, -1,
									COERCION_IMPLICIT,
									COERCE_IMPLICIT_CAST,
									-1);
		actual_arg_types[aargpos] = commontype;
	}

	/* Reconstruct fargs from array */
	i = 0;
	foreach(lc, fargs)
	{
		lfirst(lc) = args[i++];
	}
}


/*
 * make_fn_arguments()
 *
 * Given the actual argument expressions for a function, and the desired
 * input types for the function, add any necessary typecasting to the
 * expression tree.  Caller should already have verified that casting is
 * allowed.
 *
 * Caution: given argument list is modified in-place.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
void
make_fn_arguments(ParseState *pstate,
				  List *fargs,
				  Oid *actual_arg_types,
				  Oid *declared_arg_types)
{
	ListCell   *current_fargs;
	int			i = 0;

	foreach(current_fargs, fargs)
	{
		/* types don't match? then force coercion using a function call... */
		if (actual_arg_types[i] != declared_arg_types[i])
		{
			Node	   *node = (Node *) lfirst(current_fargs);
			CoercionContext ccontext = COERCION_IMPLICIT;

			if ((ORA_MODE || enable_lightweight_ora_syntax) &&
				is_ora_implicit_coercion(declared_arg_types[i],
										 actual_arg_types[i]))
				ccontext = COERCION_ASSIGNMENT;
			/*
			 * If arg is a NamedArgExpr, coerce its input expr instead --- we
			 * want the NamedArgExpr to stay at the top level of the list.
			 */
			if (IsA(node, NamedArgExpr))
			{
				NamedArgExpr *na = (NamedArgExpr *) node;

				node = coerce_type(pstate,
								   (Node *) na->arg,
								   actual_arg_types[i],
								   declared_arg_types[i], -1,
								   ccontext,
								   COERCE_IMPLICIT_CAST,
								   -1);
				na->arg = (Expr *) node;
			}
			else
			{
				node = coerce_type(pstate,
								   node,
								   actual_arg_types[i],
								   declared_arg_types[i], -1,
								   ccontext,
								   COERCE_IMPLICIT_CAST,
								   -1);
				lfirst(current_fargs) = node;
			}
		}
		i++;
	}
}

/*
 * FuncNameAsType -
 *	  convenience routine to see if a function name matches a type name
 *
 * Returns the OID of the matching type, or InvalidOid if none.  We ignore
 * shell types and complex types.
 */
static Oid
FuncNameAsType(List *funcname)
{
	Oid			result;
	Type		typtup;

	/*
	 * temp_ok=false protects the <refsect1 id="sql-createfunction-security">
	 * contract for writing SECURITY DEFINER functions safely.
	 */
	typtup = LookupTypeNameExtended(NULL, makeTypeNameFromNameList(funcname),
									NULL, false, false);
	if (typtup == NULL)
		return InvalidOid;

	if (((Form_pg_type) GETSTRUCT(typtup))->typisdefined &&
		!OidIsValid(typeTypeRelid(typtup)))
		result = typeTypeId(typtup);
	else
		result = InvalidOid;

	ReleaseSysCache(typtup);
	return result;
}

/*
 * ParseComplexProjection -
 *	  handles function calls with a single argument that is of complex type.
 *	  If the function call is actually a column projection, return a suitably
 *	  transformed expression tree.  If not, return NULL.
 */
static Node *
ParseComplexProjection(ParseState *pstate, char *funcname, Node *first_arg,
					   int location)
{
	TupleDesc	tupdesc;
	int			i;

	/*
	 * Special case for whole-row Vars so that we can resolve (foo.*).bar even
	 * when foo is a reference to a subselect, join, or RECORD function. A
	 * bonus is that we avoid generating an unnecessary FieldSelect; our
	 * result can omit the whole-row Var and just be a Var for the selected
	 * field.
	 *
	 * This case could be handled by expandRecordVariable, but it's more
	 * efficient to do it this way when possible.
	 */
	if (IsA(first_arg, Var) &&
		((Var *) first_arg)->varattno == InvalidAttrNumber)
	{
		RangeTblEntry *rte;

		rte = GetRTEByRangeTablePosn(pstate,
									 ((Var *) first_arg)->varno,
									 ((Var *) first_arg)->varlevelsup);
		/* Return a Var if funcname matches a column, else NULL */
		return scanRTEForColumn(pstate, rte, funcname, location, 0, NULL);
	}

	/*
	 * Else do it the hard way with get_expr_result_tupdesc().
	 *
	 * If it's a Var of type RECORD, we have to work even harder: we have to
	 * find what the Var refers to, and pass that to get_expr_result_tupdesc.
	 * That task is handled by expandRecordVariable().
	 */
	if (IsA(first_arg, Var) &&
		((Var *) first_arg)->vartype == RECORDOID)
		tupdesc = expandRecordVariable(pstate, (Var *) first_arg, 0);
	else
		tupdesc = get_expr_result_tupdesc(first_arg, true);
	if (!tupdesc)
		return NULL;			/* unresolvable RECORD type */

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (strcmp(funcname, NameStr(att->attname)) == 0 &&
			!att->attisdropped)
		{
			/* Success, so generate a FieldSelect expression */
			FieldSelect *fselect = makeNode(FieldSelect);

			fselect->arg = (Expr *) first_arg;
			fselect->fieldnum = i + 1;
			fselect->resulttype = att->atttypid;
			fselect->resulttypmod = att->atttypmod;
			/* save attribute's collation for parse_collate.c */
			fselect->resultcollid = att->attcollation;
			return (Node *) fselect;
		}
	}

	return NULL;				/* funcname does not match any column */
}

/*
 * funcname_signature_string
 *		Build a string representing a function name, including arg types.
 *		The result is something like "foo(integer)".
 *
 * If argnames isn't NIL, it is a list of C strings representing the actual
 * arg names for the last N arguments.  This must be considered part of the
 * function signature too, when dealing with named-notation function calls.
 *
 * This is typically used in the construction of function-not-found error
 * messages.
 */
const char *
funcname_signature_string(const char *funcname, int nargs,
						  List *argnames, const Oid *argtypes)
{
	StringInfoData argbuf;
	int			numposargs;
	ListCell   *lc;
	int			i;

	initStringInfo(&argbuf);

	appendStringInfo(&argbuf, "%s(", funcname);

	numposargs = nargs - list_length(argnames);
	lc = list_head(argnames);

	for (i = 0; i < nargs; i++)
	{
		if (i)
			appendStringInfoString(&argbuf, ", ");
		if (i >= numposargs)
		{
			appendStringInfo(&argbuf, "%s => ", (char *) lfirst(lc));
			lc = lnext(lc);
		}
		appendStringInfoString(&argbuf, format_type_be(argtypes[i]));
	}

	appendStringInfoChar(&argbuf, ')');

	return argbuf.data;			/* return palloc'd string buffer */
}

/*
 * func_signature_string
 *		As above, but function name is passed as a qualified name list.
 */
const char *
func_signature_string(List *funcname, int nargs,
					  List *argnames, const Oid *argtypes)
{
	return funcname_signature_string(NameListToString(funcname),
									 nargs, argnames, argtypes);
}

/*
 * LookupFuncName
 *
 * Given a possibly-qualified function name and optionally a set of argument
 * types, look up the function.  Pass nargs == -1 to indicate that no argument
 * types are specified.
 *
 * If the function name is not schema-qualified, it is sought in the current
 * namespace search path.
 *
 * If the function is not found, we return InvalidOid if noError is true,
 * else raise an error.
 */
Oid
LookupFuncName(List *funcname, int nargs, const Oid *argtypes, bool noError)
{
	FuncCandidateList clist;

	/* Passing NULL for argtypes is no longer allowed */
	Assert(argtypes);

	clist = FuncnameGetCandidates(funcname, nargs, NIL, false, false, noError);

	/*
	 * If no arguments were specified, the name must yield a unique candidate.
	 */
	if (nargs == -1)
	{
		if (clist)
		{
			if (clist->next)
			{
				if (!noError)
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
							 errmsg("function name \"%s\" is not unique",
									NameListToString(funcname)),
							 errhint("Specify the argument list to select the function unambiguously.")));
			}
			else
				return clist->oid;
		}
		else
		{
			if (!noError)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not find a function named \"%s\"",
								NameListToString(funcname))));
		}

		return InvalidOid;
	}

	while (clist)
	{
		if (memcmp(argtypes, clist->args, nargs * sizeof(Oid)) == 0)
			return clist->oid;
		clist = clist->next;
	}

	if (!noError)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function %s does not exist",
						func_signature_string(funcname, nargs,
											  NIL, argtypes))));

	return InvalidOid;
}

#ifdef _PG_ORCL_
Oid
LookupSynonFuncWithArgs(ObjectType objtype, ObjectWithArgs *func)
{
	List	*funnames = get_synonym_funnames(func->objname, NULL);
	ListCell	*l;
	Oid		funcid = InvalidOid;

	foreach(l, funnames)
	{
		ObjectWithArgs	*tmp_func = (ObjectWithArgs *) copyObject(func);

		tmp_func->objname = lfirst(l);
		funcid = LookupFuncWithArgs(objtype, tmp_func, true);
		if (OidIsValid(funcid))
			return funcid;
	}

	/*
	 * No valid function found, use this duplicated call for more detailed
	 * error message.
	 */
	if (l == NULL)
		funcid = LookupFuncWithArgs(objtype, func, false);

	return funcid;
}
#endif

/*
 * LookupFuncWithArgs
 *
 * Like LookupFuncName, but the argument types are specified by a
 * ObjectWithArgs node.  Also, this function can check whether the result is a
 * function, procedure, or aggregate, based on the objtype argument.  Pass
 * OBJECT_ROUTINE to accept any of them.
 *
 * For historical reasons, we also accept aggregates when looking for a
 * function.
 */
Oid
LookupFuncWithArgs(ObjectType objtype, ObjectWithArgs *func, bool noError)
{
	Oid			argoids[FUNC_MAX_ARGS];
	int			argcount;
	int			i;
	ListCell   *args_item;
	Oid			oid;

	Assert(objtype == OBJECT_AGGREGATE ||
		   objtype == OBJECT_FUNCTION ||
		   objtype == OBJECT_PROCEDURE ||
		   objtype == OBJECT_ROUTINE);

	argcount = list_length(func->objargs);
	if (argcount > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("functions cannot have more than %d argument",
							   "functions cannot have more than %d arguments",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS)));

	args_item = list_head(func->objargs);
	for (i = 0; i < argcount; i++)
	{
		bool		found = false;
		TypeName   *t = (TypeName *) lfirst(args_item);

		argoids[i] = InvalidOid;
		if (ORA_MODE)
		{
			Oid		pkg_oid;
			char	*udtname;

			/*
			 * When the package and table have the same name, the package will
			 * be matched first.
			 */
			found = LookupPackageAnyType(t, &pkg_oid, &udtname, InvalidOid,
										 &argoids[i], false, NULL);
		}

		if (!found)
			argoids[i] = LookupTypeNameOid(NULL, t, noError || !check_function_pludt);

		if (!OidIsValid(argoids[i]))
		{
			if (!check_function_pludt)
			{
				/*
				 * When check_function_pludt is off (usually when pg_dump is called),
				 * the type cannot be found, so the type is treated as a PLUDT in
				 * the package.
				 */
				argoids[i] = PLUDTOID;
			}
			else if (!noError)
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist",
							TypeNameToString(t)),
					 parser_errposition(NULL, t->location)));
		}

		args_item = lnext(args_item);
	}

	/*
	 * When looking for a function or routine, we pass noError through to
	 * LookupFuncName and let it make any error messages.  Otherwise, we make
	 * our own errors for the aggregate and procedure cases.
	 */
	oid = LookupFuncName(func->objname, func->args_unspecified ? -1 : argcount, argoids,
						 (objtype == OBJECT_FUNCTION || objtype == OBJECT_ROUTINE) ? noError : true);

	if (objtype == OBJECT_FUNCTION)
	{
		/* Make sure it's a function, not a procedure */
		if (oid && get_func_prokind(oid) == PROKIND_PROCEDURE)
		{
			if (noError)
				return InvalidOid;
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s is not a function",
							func_signature_string(func->objname, argcount,
												  NIL, argoids))));
		}
	}
	else if (objtype == OBJECT_PROCEDURE)
	{
		if (!OidIsValid(oid))
		{
			if (noError)
				return InvalidOid;
			else if (func->args_unspecified)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not find a procedure named \"%s\"",
								NameListToString(func->objname))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("procedure %s does not exist",
								func_signature_string(func->objname, argcount,
													  NIL, argoids))));
		}

		/* Make sure it's a procedure */
		if (get_func_prokind(oid) != PROKIND_PROCEDURE)
		{
			if (noError)
				return InvalidOid;
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s is not a procedure",
							func_signature_string(func->objname, argcount,
												  NIL, argoids))));
		}
	}
	else if (objtype == OBJECT_AGGREGATE)
	{
		if (!OidIsValid(oid))
		{
			if (noError)
				return InvalidOid;
			else if (func->args_unspecified)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("could not find a aggregate named \"%s\"",
								NameListToString(func->objname))));
			else if (argcount == 0)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("aggregate %s(*) does not exist",
								NameListToString(func->objname))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("aggregate %s does not exist",
								func_signature_string(func->objname, argcount,
													  NIL, argoids))));
		}

		/* Make sure it's an aggregate */
		if (get_func_prokind(oid) != PROKIND_AGGREGATE)
		{
			if (noError)
				return InvalidOid;
			/* we do not use the (*) notation for functions... */
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("function %s is not an aggregate",
							func_signature_string(func->objname, argcount,
												  NIL, argoids))));
		}
	}

	return oid;
}

/*
 * check_srf_call_placement
 *		Verify that a set-returning function is called in a valid place,
 *		and throw a nice error if not.
 *
 * A side-effect is to set pstate->p_hasTargetSRFs true if appropriate.
 *
 * last_srf should be a copy of pstate->p_last_srf from just before we
 * started transforming the function's arguments.  This allows detection
 * of whether the SRF's arguments contain any SRFs.
 */
void
check_srf_call_placement(ParseState *pstate, Node *last_srf, int location)
{
	const char *err;
	bool		errkind;

	/*
	 * Check to see if the set-returning function is in an invalid place
	 * within the query.  Basically, we don't allow SRFs anywhere except in
	 * the targetlist (which includes GROUP BY/ORDER BY expressions), VALUES,
	 * and functions in FROM.
	 *
	 * For brevity we support two schemes for reporting an error here: set
	 * "err" to a custom message, or set "errkind" true if the error context
	 * is sufficiently identified by what ParseExprKindName will return, *and*
	 * what it will return is just a SQL keyword.  (Otherwise, use a custom
	 * message to avoid creating translation problems.)
	 */
	err = NULL;
	errkind = false;
	switch (pstate->p_expr_kind)
	{
		case EXPR_KIND_NONE:
			Assert(false);		/* can't happen */
			break;
		case EXPR_KIND_OTHER:
			/* Accept SRF here; caller must throw error if wanted */
			break;
		case EXPR_KIND_JOIN_ON:
		case EXPR_KIND_JOIN_USING:
			err = _("set-returning functions are not allowed in JOIN conditions");
			break;
		case EXPR_KIND_FROM_SUBSELECT:
			/* can't get here, but just in case, throw an error */
			errkind = true;
			break;
		case EXPR_KIND_FROM_FUNCTION:
			/* okay, but we don't allow nested SRFs here */
			/* errmsg is chosen to match transformRangeFunction() */
			/* errposition should point to the inner SRF */
			if (pstate->p_last_srf != last_srf)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("set-returning functions must appear at top level of FROM"),
						 parser_errposition(pstate,
											exprLocation(pstate->p_last_srf))));
			break;
		case EXPR_KIND_WHERE:
			errkind = true;
			break;
		case EXPR_KIND_POLICY:
			err = _("set-returning functions are not allowed in policy expressions");
			break;
		case EXPR_KIND_HAVING:
			errkind = true;
			break;
		case EXPR_KIND_FILTER:
			errkind = true;
			break;
		case EXPR_KIND_WINDOW_PARTITION:
		case EXPR_KIND_WINDOW_ORDER:
			/* okay, these are effectively GROUP BY/ORDER BY */
			pstate->p_hasTargetSRFs = true;
			break;
		case EXPR_KIND_WINDOW_FRAME_RANGE:
		case EXPR_KIND_WINDOW_FRAME_ROWS:
			err = _("set-returning functions are not allowed in window definitions");
			break;
		case EXPR_KIND_SELECT_TARGET:
		case EXPR_KIND_INSERT_TARGET:
			/* okay */
			pstate->p_hasTargetSRFs = true;
			break;
		case EXPR_KIND_UPDATE_SOURCE:
		case EXPR_KIND_UPDATE_TARGET:
			/* disallowed because it would be ambiguous what to do */
			errkind = true;
			break;
		case EXPR_KIND_GROUP_BY:
		case EXPR_KIND_ORDER_BY:
			/* okay */
			pstate->p_hasTargetSRFs = true;
			break;
		case EXPR_KIND_DISTINCT_ON:
			/* okay */
			pstate->p_hasTargetSRFs = true;
			break;
		case EXPR_KIND_LIMIT:
		case EXPR_KIND_OFFSET:
			errkind = true;
			break;
		case EXPR_KIND_RETURNING:
			errkind = true;
			break;
		case EXPR_KIND_VALUES:
			/* SRFs are presently not supported by nodeValuesscan.c */
			errkind = true;
			break;
		case EXPR_KIND_VALUES_SINGLE:
			/* okay, since we process this like a SELECT tlist */
			pstate->p_hasTargetSRFs = true;
			break;
		case EXPR_KIND_MERGE_WHEN:
			err = _("set-returning functions are not allowed in MERGE WHEN "
					"conditions");
			break;
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			err = _("set-returning functions are not allowed in check constraints");
			break;
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:
			err = _("set-returning functions are not allowed in DEFAULT expressions");
			break;
		case EXPR_KIND_INDEX_EXPRESSION:
			err = _("set-returning functions are not allowed in index expressions");
			break;
		case EXPR_KIND_INDEX_PREDICATE:
			err = _("set-returning functions are not allowed in index predicates");
			break;
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			err = _("set-returning functions are not allowed in transform expressions");
			break;
		case EXPR_KIND_EXECUTE_PARAMETER:
			err = _("set-returning functions are not allowed in EXECUTE parameters");
			break;
		case EXPR_KIND_TRIGGER_WHEN:
			err = _("set-returning functions are not allowed in trigger WHEN conditions");
			break;
		case EXPR_KIND_PARTITION_EXPRESSION:
			err = _("set-returning functions are not allowed in partition key expressions");
			break;
		case EXPR_KIND_CALL_ARGUMENT:
			err = _("set-returning functions are not allowed in CALL arguments");
			break;

			/*
			 * There is intentionally no default: case here, so that the
			 * compiler will warn if we add a new ParseExprKind without
			 * extending this switch.  If we do see an unrecognized value at
			 * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
			 * which is sane anyway.
			 */
	}
	if (err)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, location)));
	if (errkind)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/* translator: %s is name of a SQL construct, eg GROUP BY */
				 errmsg("set-returning functions are not allowed in %s",
						ParseExprKindName(pstate->p_expr_kind)),
				 parser_errposition(pstate, location)));
}

bool
func_is_pullup(Oid func_id, Node *node)
{
	float cost;

	if (IS_CENTRALIZED_DATANODE)
		return false;

	/* we consider user-defined-function only */
	if (func_id >= FirstNormalObjectId)
	{
		/*
		 * A set returning function is not supposed to be in targetlist
		 * so ignore it.
		 *
		 * It is unsafe to pushdown the set-valued function and may cause
		 * data inconsistency. However, in some cases, an error will be
		 * reported when pulling it up, which needs to be solved in future
		 * versions.
		 */
		if (enable_unsafe_function_pushdown && get_func_retset(func_id))
			return false;

		/* skip functions created within extensions */
		if (OidIsValid(getExtensionOfObject(ProcedureRelationId, func_id)))
			return false;

		/* An immutable function surely can be pushed down to DN */
		if (func_volatile(func_id) == PROVOLATILE_IMMUTABLE)
			return false;

		cost = get_func_cost_with_sign(func_id);
		if (cost >= 0)
			return true;
	}

	if (node != NULL && IsA(node, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) node;
		if (expr->withfuncnsp != NULL &&
			expr->withfuncid >= 0 &&
			expr->withfuncid < list_length(expr->withfuncnsp))
		{
			if (get_func_retset_withfuncs(expr->withfuncnsp, expr->withfuncid))
				return false;

			if (func_volatile_withfuncs(expr->withfuncnsp, expr->withfuncid) == PROVOLATILE_IMMUTABLE)
				return false;

			cost = get_func_cost_withfuncs(expr->withfuncnsp, expr->withfuncid);
			if (cost >= 0)
				return true;
		}
	}

	/*
	 * Some system functions cannot be pushed down.
	 */
	if (check_system_function_pullup(func_id))
		return true;

	return false;
}

static bool
check_system_function_pullup(Oid func_id)
{
	switch (func_id)
	{
		case SYS_CONTEXT_OID:
		{
			Assert(ORA_MODE);
		}
		case 2322: /* pg_tablespace_size_oid */
		case 2323: /* pg_tablespace_size_name */
		case 2324: /* pg_database_size_oid */
		case 2168: /* pg_database_size_name */
		case 2325: /* pg_relation_size($1, ''main'') */
		case 2332: /* pg_relation_size */
		case 2286: /* pg_total_relation_size */
		case 2997: /* pg_table_size */
		case 2998: /* pg_indexes_size */
		case 8060: /* pg_allocated_tablespace_size_oid */
		case 8061: /* pg_allocated_tablespace_size_name */
		case 8062: /* pg_allocated_database_size_oid */
		case 8063: /* pg_allocated_database_size_name */
		case 8064: /* pg_allocated_relation_size($1, ''main'') */
		case 8065: /* pg_allocated_relation_size */
		case 8066: /* pg_allocated_total_relation_size */
		case 8067: /* pg_allocated_table_size */
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
