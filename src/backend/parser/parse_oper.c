/*-------------------------------------------------------------------------
 *
 * parse_oper.c
 *		handle operator things for parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_oper.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#ifdef _PG_ORCL_
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "executor/execExpr.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "utils/orcl_datetime.h"
#endif

/*
 * The lookup key for the operator lookaside hash table.  Unused bits must be
 * zeroes to ensure hashing works consistently --- in particular, oprname
 * must be zero-padded and any unused entries in search_path must be zero.
 *
 * search_path contains the actual search_path with which the entry was
 * derived (minus temp namespace if any), or else the single specified
 * schema OID if we are looking up an explicitly-qualified operator name.
 *
 * search_path has to be fixed-length since the hashtable code insists on
 * fixed-size keys.  If your search path is longer than that, we just punt
 * and don't cache anything.
 */

/* If your search_path is longer than this, sucks to be you ... */
#define MAX_CACHED_PATH_LEN		16

#define IS_PATTERN_MATCH_OP(opname) (strcmp(opname, "~~") == 0 || strcmp(opname, "!~~") == 0 || strcmp(opname, "~~*") == 0 || strcmp(opname, "!~~*") == 0)

typedef struct OprCacheKey
{
	char		oprname[NAMEDATALEN];
	Oid			left_arg;		/* Left input OID, or 0 if prefix op */
	Oid			right_arg;		/* Right input OID, or 0 if postfix op */
	Oid			search_path[MAX_CACHED_PATH_LEN];
} OprCacheKey;

typedef struct OprCacheEntry
{
	/* the hash lookup key MUST BE FIRST */
	OprCacheKey key;

	Oid			opr_oid;		/* OID of the resolved operator */
} OprCacheEntry;


static Oid	binary_oper_exact(List *opname, Oid arg1, Oid arg2);
static FuncDetailCode oper_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates,
					  Oid *operOid);
static const char *op_signature_string(List *op, char oprkind,
					Oid arg1, Oid arg2);
static void op_error(ParseState *pstate, List *op, char oprkind,
		 Oid arg1, Oid arg2,
		 FuncDetailCode fdresult, int location);
static bool make_oper_cache_key(ParseState *pstate, OprCacheKey *key,
					List *opname, Oid ltypeId, Oid rtypeId,
					int location);
static Oid	find_oper_cache_entry(OprCacheKey *key);
static void make_oper_cache_entry(OprCacheKey *key, Oid opr_oid);
static void InvalidateOprCacheCallBack(Datum arg, int cacheid, uint32 hashvalue);
static Node *transformStrArrayToNumericArray(ParseState *pstate, Node *a);

#ifdef _PG_ORCL_
static Oid get_opentenbase_ora_compatible_oper(Oid operOid);
#endif

static void determine_unknown_type(ParseState *pstate, Oid *left_type, Oid *right_type, Node **ltree, Node **rtree, char *opername);
static inline void update_op_arg_type(ParseState *pstate, Oid *source_type, Oid target_type, int target_typmod, Node **tree);
static void process_concat_op(ParseState *pstate, Oid *left_type, Oid *right_type, Node **ltree, Node **rtree, List *opname, int location);
static void process_arith_and_comp_op(ParseState *pstate, Oid *left_type, Oid *right_type, Node **ltree, Node **rtree, char *opername);
/*
 * LookupOperName
 *		Given a possibly-qualified operator name and exact input datatypes,
 *		look up the operator.
 *
 * Pass oprleft = InvalidOid for a prefix op, oprright = InvalidOid for
 * a postfix op.
 *
 * If the operator name is not schema-qualified, it is sought in the current
 * namespace search path.
 *
 * If the operator is not found, we return InvalidOid if noError is true,
 * else raise an error.  pstate and location are used only to report the
 * error position; pass NULL/-1 if not available.
 */
Oid
LookupOperName(ParseState *pstate, List *opername, Oid oprleft, Oid oprright,
			   bool noError, int location)
{
	Oid			result;

	result = OpernameGetOprid(opername, oprleft, oprright);
	if (OidIsValid(result))
		return result;

	/* we don't use op_error here because only an exact match is wanted */
	if (!noError)
	{
		char		oprkind;

		if (!OidIsValid(oprleft))
			oprkind = 'l';
		else if (!OidIsValid(oprright))
			oprkind = 'r';
		else
			oprkind = 'b';

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator does not exist: %s",
						op_signature_string(opername, oprkind,
											oprleft, oprright)),
				 parser_errposition(pstate, location)));
	}

	return InvalidOid;
}

/*
 * LookupOperWithArgs
 *		Like LookupOperName, but the argument types are specified by
 *		a ObjectWithArg node.
 */
Oid
LookupOperWithArgs(ObjectWithArgs *oper, bool noError)
{
	TypeName   *oprleft,
			   *oprright;
	Oid			leftoid,
				rightoid;

	Assert(list_length(oper->objargs) == 2);
	oprleft = linitial(oper->objargs);
	oprright = lsecond(oper->objargs);

	if (oprleft == NULL)
		leftoid = InvalidOid;
	else
		leftoid = LookupTypeNameOid(NULL, oprleft, noError);

	if (oprright == NULL)
		rightoid = InvalidOid;
	else
		rightoid = LookupTypeNameOid(NULL, oprright, noError);

	return LookupOperName(NULL, oper->objname, leftoid, rightoid,
						  noError, -1);
}

/*
 * get_sort_group_operators - get default sorting/grouping operators for type
 *
 * We fetch the "<", "=", and ">" operators all at once to reduce lookup
 * overhead (knowing that most callers will be interested in at least two).
 * However, a given datatype might have only an "=" operator, if it is
 * hashable but not sortable.  (Other combinations of present and missing
 * operators shouldn't happen, unless the system catalogs are messed up.)
 *
 * If an operator is missing and the corresponding needXX flag is true,
 * throw a standard error message, else return InvalidOid.
 *
 * In addition to the operator OIDs themselves, this function can identify
 * whether the "=" operator is hashable.
 *
 * Callers can pass NULL pointers for any results they don't care to get.
 *
 * Note: the results are guaranteed to be exact or binary-compatible matches,
 * since most callers are not prepared to cope with adding any run-time type
 * coercion steps.
 */
void
get_sort_group_operators(Oid argtype, bool needLT, bool needEQ, bool needGT, Oid *ltOpr, Oid *eqOpr,
                         Oid *gtOpr, bool *isHashable)
{
	get_sort_group_operators_impl(argtype, needLT, needEQ, needGT, ltOpr, eqOpr, gtOpr, isHashable, false);
}
void
get_sort_group_operators_impl(Oid argtype,
						 bool needLT, bool needEQ, bool needGT,
						 Oid *ltOpr, Oid *eqOpr, Oid *gtOpr,
						 bool *isHashable, bool is_agg)
{
	TypeCacheEntry *typentry;
	int			cache_flags;
	Oid			lt_opr;
	Oid			eq_opr;
	Oid			gt_opr;
	bool		hashable;

	/* CLOB, BLOB, and NCLOB are not supported. */
	if (ORA_MODE && !is_agg && (argtype == BLOBOID || argtype == CLOBOID || argtype == NCLOBOID))
		ereport(ERROR,
		        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
		         errmsg("inconsistent datatypes: expected - got %s", TypidGetTypename(argtype))));
	/*
	 * Look up the operators using the type cache.
	 *
	 * Note: the search algorithm used by typcache.c ensures that the results
	 * are consistent, ie all from matching opclasses.
	 */
	if (isHashable != NULL)
		cache_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR |
			TYPECACHE_HASH_PROC;
	else
		cache_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR;

	typentry = lookup_type_cache(argtype, cache_flags);
	lt_opr = typentry->lt_opr;
	eq_opr = typentry->eq_opr;
	gt_opr = typentry->gt_opr;
	hashable = OidIsValid(typentry->hash_proc);

	/* Report errors if needed */
	if ((needLT && !OidIsValid(lt_opr)) ||
		(needGT && !OidIsValid(gt_opr)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an ordering operator for type %s",
						format_type_be(argtype)),
				 errhint("Use an explicit ordering operator or modify the query.")));
	if (needEQ && !OidIsValid(eq_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an equality operator for type %s",
						format_type_be(argtype))));

	/* Return results as needed */
	if (ltOpr)
		*ltOpr = lt_opr;
	if (eqOpr)
		*eqOpr = eq_opr;
	if (gtOpr)
		*gtOpr = gt_opr;
	if (isHashable)
		*isHashable = hashable;
}


/* given operator tuple, return the operator OID */
Oid
oprid(Operator op)
{
	return HeapTupleGetOid(op);
}

/* given operator tuple, return the underlying function's OID */
Oid
oprfuncid(Operator op)
{
	Form_pg_operator pgopform = (Form_pg_operator) GETSTRUCT(op);

	return pgopform->oprcode;
}


/* binary_oper_exact()
 * Check for an "exact" match to the specified operand types.
 *
 * If one operand is an unknown literal, assume it should be taken to be
 * the same type as the other operand for this purpose.  Also, consider
 * the possibility that the other operand is a domain type that needs to
 * be reduced to its base type to find an "exact" match.
 */
static Oid
binary_oper_exact(List *opname, Oid arg1, Oid arg2)
{
	Oid			result;
	bool		was_unknown = false;

	/* Unspecified type for one of the arguments? then use the other */
	if ((arg1 == UNKNOWNOID) && (arg2 != InvalidOid))
	{
		arg1 = arg2;
		was_unknown = true;
	}
	else if ((arg2 == UNKNOWNOID) && (arg1 != InvalidOid))
	{
		arg2 = arg1;
		was_unknown = true;
	}

	result = OpernameGetOprid(opname, arg1, arg2);
	if (OidIsValid(result))
		return result;

	if (was_unknown)
	{
		/* arg1 and arg2 are the same here, need only look at arg1 */
		Oid			basetype = getBaseType(arg1);

		if (basetype != arg1)
		{
			result = OpernameGetOprid(opname, basetype, basetype);
			if (OidIsValid(result))
				return result;
		}
	}

	return InvalidOid;
}


/* oper_select_candidate()
 *		Given the input argtype array and one or more candidates
 *		for the operator, attempt to resolve the conflict.
 *
 * Returns FUNCDETAIL_NOTFOUND, FUNCDETAIL_MULTIPLE, or FUNCDETAIL_NORMAL.
 * In the success case the Oid of the best candidate is stored in *operOid.
 *
 * Note that the caller has already determined that there is no candidate
 * exactly matching the input argtype(s).  Incompatible candidates are not yet
 * pruned away, however.
 */
static FuncDetailCode
oper_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates,
					  Oid *operOid) /* output argument */
{
	int			ncandidates;

	/*
	 * Delete any candidates that cannot actually accept the given input
	 * types, whether directly or by coercion.
	 */
	ncandidates = func_match_argtypes(nargs, input_typeids,
									  candidates, &candidates,
									  NIL, NULL, false);

	/* Done if no candidate or only one candidate survives */
	if (ncandidates == 0)
	{
		*operOid = InvalidOid;
		return FUNCDETAIL_NOTFOUND;
	}
	if (ncandidates == 1)
	{
		*operOid = candidates->oid;
		return FUNCDETAIL_NORMAL;
	}

	/*
	 * Use the same heuristics as for ambiguous functions to resolve the
	 * conflict.
	 */
	candidates = func_select_candidate(nargs, input_typeids, candidates);

	if (candidates)
	{
		/* LIGHTWEIGHT_ORA modes require invoking the TO_DATE(text, text) function in the opentenbase-ora-style manner. */
		if (enable_lightweight_ora_syntax && candidates->oid == PG_TODATE_TEXTTEXT_OID)
			candidates->oid = ORA_TODATE_TEXTTEXT_OID;

		*operOid = candidates->oid;
		return FUNCDETAIL_NORMAL;
	}

	*operOid = InvalidOid;
	return FUNCDETAIL_MULTIPLE; /* failed to select a best candidate */
}

#ifdef _PG_ORCL_
/*
 * get_opentenbase_ora_compatible_oper()
 * Get an opentenbase_ora compatible operId from pg_operator
 */
static Oid
get_opentenbase_ora_compatible_oper(Oid operOid)
{
	if (!ORA_MODE)
	{
		if (operOid == OID_ORCL_INT4_DIV_OP)
			return OID_INT4_DIV_OP;
		return operOid;
	}
	/*
	 * use opentenbase_ora.int4div which returns numeric instead of integer
	 * when evaluate expression int4/int4 such as select 1/2 .
	 */
	if (operOid == OID_INT4_DIV_OP)
		return OID_ORCL_INT4_DIV_OP;

	return operOid;
}
#endif

/* oper() -- search for a binary operator
 * Given operator name, types of arg1 and arg2, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatypes.  Do not use this if
 * you need an exact- or binary-compatible match; see compatible_oper.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * NOTE: on success, the returned object is a syscache entry.  The caller
 * must ReleaseSysCache() the entry when done with it.
 */
Operator
oper(ParseState *pstate, List *opname, Oid ltypeId, Oid rtypeId,
	 bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(pstate, &key, opname, ltypeId, rtypeId, location);

	if (key_ok)
	{
		operOid = find_oper_cache_entry(&key);
		if (OidIsValid(operOid))
		{
#ifdef _PG_ORCL_
			operOid = get_opentenbase_ora_compatible_oper(operOid);
#endif
			tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
			if (HeapTupleIsValid(tup))
				return (Operator) tup;
		}
	}

	/*
	 * First try for an "exact" match.
	 */
	operOid = binary_oper_exact(opname, ltypeId, rtypeId);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get binary operators of given name */
		clist = OpernameGetCandidates(opname, 'b', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * Unspecified type for one of the arguments? then use the other
			 * (XXX this is probably dead code?)
			 */
			Oid			inputOids[2];

			if (rtypeId == InvalidOid)
				rtypeId = ltypeId;
			else if (ltypeId == InvalidOid)
				ltypeId = rtypeId;
			inputOids[0] = ltypeId;
			inputOids[1] = rtypeId;
			fdresult = oper_select_candidate(2, inputOids, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
#ifdef _PG_ORCL_
	{
		operOid = get_opentenbase_ora_compatible_oper(operOid);
#endif
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
#ifdef _PG_ORCL_
	}
#endif

	if (HeapTupleIsValid(tup))
	{
		if (key_ok)
			make_oper_cache_entry(&key, operOid);
	}
	else if (!noError)
		op_error(pstate, opname, 'b', ltypeId, rtypeId, fdresult, location);

	return (Operator) tup;
}

/* compatible_oper()
 *	given an opname and input datatypes, find a compatible binary operator
 *
 *	This is tighter than oper() because it will not return an operator that
 *	requires coercion of the input datatypes (but binary-compatible operators
 *	are accepted).  Otherwise, the semantics are the same.
 */
Operator
compatible_oper(ParseState *pstate, List *op, Oid arg1, Oid arg2,
				bool noError, int location)
{
	Operator	optup;
	Form_pg_operator opform;

	/* oper() will find the best available match */
	optup = oper(pstate, op, arg1, arg2, noError, location);
	if (optup == (Operator) NULL)
		return (Operator) NULL; /* must be noError case */

	/* but is it good enough? */
	opform = (Form_pg_operator) GETSTRUCT(optup);
	if (IsBinaryCoercible(arg1, opform->oprleft) &&
		IsBinaryCoercible(arg2, opform->oprright))
		return optup;

	/* nope... */
	ReleaseSysCache(optup);

	if (!noError)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator requires run-time type coercion: %s",
						op_signature_string(op, 'b', arg1, arg2)),
				 parser_errposition(pstate, location)));

	return (Operator) NULL;
}

/* compatible_oper_opid() -- get OID of a binary operator
 *
 * This is a convenience routine that extracts only the operator OID
 * from the result of compatible_oper().  InvalidOid is returned if the
 * lookup fails and noError is true.
 */
Oid
compatible_oper_opid(List *op, Oid arg1, Oid arg2, bool noError)
{
	Operator	optup;
	Oid			result;

	optup = compatible_oper(NULL, op, arg1, arg2, noError, -1);
	if (optup != NULL)
	{
		result = oprid(optup);
		ReleaseSysCache(optup);
		return result;
	}
	return InvalidOid;
}


/* right_oper() -- search for a unary right operator (postfix operator)
 * Given operator name and type of arg, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatype.  Do not use this if
 * you need an exact- or binary-compatible match.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * NOTE: on success, the returned object is a syscache entry.  The caller
 * must ReleaseSysCache() the entry when done with it.
 */
Operator
right_oper(ParseState *pstate, List *op, Oid arg, bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(pstate, &key, op, arg, InvalidOid, location);

	if (key_ok)
	{
		operOid = find_oper_cache_entry(&key);
		if (OidIsValid(operOid))
		{
			tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
			if (HeapTupleIsValid(tup))
				return (Operator) tup;
		}
	}

	/*
	 * First try for an "exact" match.
	 */
	operOid = OpernameGetOprid(op, arg, InvalidOid);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get postfix operators of given name */
		clist = OpernameGetCandidates(op, 'r', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * We must run oper_select_candidate even if only one candidate,
			 * otherwise we may falsely return a non-type-compatible operator.
			 */
			fdresult = oper_select_candidate(1, &arg, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));

	if (HeapTupleIsValid(tup))
	{
		if (key_ok)
			make_oper_cache_entry(&key, operOid);
	}
	else if (!noError)
		op_error(pstate, op, 'r', arg, InvalidOid, fdresult, location);

	return (Operator) tup;
}


/* left_oper() -- search for a unary left operator (prefix operator)
 * Given operator name and type of arg, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatype.  Do not use this if
 * you need an exact- or binary-compatible match.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * NOTE: on success, the returned object is a syscache entry.  The caller
 * must ReleaseSysCache() the entry when done with it.
 */
Operator
left_oper(ParseState *pstate, List *op, Oid arg, bool noError, int location)
{
	Oid			operOid;
	OprCacheKey key;
	bool		key_ok;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/*
	 * Try to find the mapping in the lookaside cache.
	 */
	key_ok = make_oper_cache_key(pstate, &key, op, InvalidOid, arg, location);

	if (key_ok)
	{
		operOid = find_oper_cache_entry(&key);
		if (OidIsValid(operOid))
		{
			tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
			if (HeapTupleIsValid(tup))
				return (Operator) tup;
		}
	}

	/*
	 * First try for an "exact" match.
	 */
	operOid = OpernameGetOprid(op, InvalidOid, arg);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get prefix operators of given name */
		clist = OpernameGetCandidates(op, 'l', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * The returned list has args in the form (0, oprright). Move the
			 * useful data into args[0] to keep oper_select_candidate simple.
			 * XXX we are assuming here that we may scribble on the list!
			 */
			FuncCandidateList clisti;

			for (clisti = clist; clisti != NULL; clisti = clisti->next)
			{
				clisti->args[0] = clisti->args[1];
			}

			/*
			 * We must run oper_select_candidate even if only one candidate,
			 * otherwise we may falsely return a non-type-compatible operator.
			 */
			fdresult = oper_select_candidate(1, &arg, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));

	if (HeapTupleIsValid(tup))
	{
		if (key_ok)
			make_oper_cache_entry(&key, operOid);
	}
	else if (!noError)
		op_error(pstate, op, 'l', InvalidOid, arg, fdresult, location);

	return (Operator) tup;
}

/*
 * op_signature_string
 *		Build a string representing an operator name, including arg type(s).
 *		The result is something like "integer + integer".
 *
 * This is typically used in the construction of operator-not-found error
 * messages.
 */
static const char *
op_signature_string(List *op, char oprkind, Oid arg1, Oid arg2)
{
	StringInfoData argbuf;

	initStringInfo(&argbuf);

	if (oprkind != 'l')
		appendStringInfo(&argbuf, "%s ", format_type_be(arg1));

	appendStringInfoString(&argbuf, NameListToString(op));

	if (oprkind != 'r')
		appendStringInfo(&argbuf, " %s", format_type_be(arg2));

	return argbuf.data;			/* return palloc'd string buffer */
}

/*
 * op_error - utility routine to complain about an unresolvable operator
 */
static void
op_error(ParseState *pstate, List *op, char oprkind,
		 Oid arg1, Oid arg2,
		 FuncDetailCode fdresult, int location)
{
	if (fdresult == FUNCDETAIL_MULTIPLE)
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
				 errmsg("operator is not unique: %s",
						op_signature_string(op, oprkind, arg1, arg2)),
				 errhint("Could not choose a best candidate operator. "
						 "You might need to add explicit type casts."),
				 parser_errposition(pstate, location)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator does not exist: %s",
						op_signature_string(op, oprkind, arg1, arg2)),
				 errhint("No operator matches the given name and argument type(s). "
						 "You might need to add explicit type casts."),
				 parser_errposition(pstate, location)));
}

static bool
IsIntegerType(Oid type)
{
	if (type == INT2OID ||
		type == INT4OID ||
		type == INT8OID)
	{
		return true;
	}
	return false;
}

static void
checkBinaryType(Oid ltypeId, char *opername)
{
	switch (ltypeId) {
		case LRAWOID:
		case LONGOID:
			if (IS_COMPARE_OP(opername))
				ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					errmsg("illegal use of LONG datatype")));
			else if (IS_ARITHMETIC_OP(opername))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("inconsistent datatypes: expected NUMBER got %s", TypidGetTypename(ltypeId))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("inconsistent datatypes: expected CHAR got %s", TypidGetTypename(ltypeId))));
			break;
		case BLOBOID:
			if (IS_COMPARE_OP(opername))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("inconsistent datatypes: expected - got %s", TypidGetTypename(ltypeId))));
			else if (IS_ARITHMETIC_OP(opername))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								errmsg("inconsistent datatypes: expected NUMBER got %s", TypidGetTypename(ltypeId))));
			else
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								errmsg("inconsistent datatypes: expected CHAR got %s", TypidGetTypename(ltypeId))));
			break;
		default:
			return;
	}
}

/*
 * make_op()
 *		Operator expression construction.
 *
 * Transform operator expression ensuring type compatibility.
 * This is where some type conversion happens.
 *
 * last_srf should be a copy of pstate->p_last_srf from just before we
 * started transforming the operator's arguments; this is used for nested-SRF
 * detection.  If the caller will throw an error anyway for a set-returning
 * expression, it's okay to cheat and just pass pstate->p_last_srf.
 */
Expr *
make_op(ParseState *pstate, List *opname, Node *ltree, Node *rtree,
		Node *last_srf, int location)
{
	Oid			ltypeId,
				rtypeId;
	Operator	tup;
	Form_pg_operator opform;
	Oid			actual_arg_types[2];
	Oid			declared_arg_types[2];
	int			nargs;
	List	   *args;
	Oid			rettype;
	OpExpr	   *result;

	/* Select the operator */
	if (rtree == NULL)
	{
		/* right operator */
		ltypeId = exprType(ltree);
		rtypeId = InvalidOid;
		tup = right_oper(pstate, opname, ltypeId, false, location);
	}
	else if (ltree == NULL)
	{
		/* left operator */
		rtypeId = exprType(rtree);
		ltypeId = InvalidOid;
		tup = left_oper(pstate, opname, rtypeId, false, location);
	}
	else
	{
		char *schemaname = NULL;
		char *opername = NULL;

		/* otherwise, binary operator */
		ltypeId = exprType(ltree);
		rtypeId = exprType(rtree);

		/* handle nested table types */
		if (ORA_MODE)
		{
			Oid ldisguisetype = InvalidOid;
			Oid rdisguisetype = InvalidOid;

			ldisguisetype = NestedTableGetDisguiseType(ltypeId);
			rdisguisetype = NestedTableGetDisguiseType(rtypeId);

			if (ldisguisetype != InvalidOid)
				ltypeId = ldisguisetype;
			if (rdisguisetype != InvalidOid)
				rtypeId = rdisguisetype;
		}

		/* deconstruct the name list */
		if (opname)
		{
			DeconstructQualifiedName(opname, &schemaname, &opername);

			/*
			 * opentenbase_ora compatible feature: Outside of plpgsql,
			 * LONG, LONG RAW, BLOB type can't support all
			 * operator now, so just error report it like opentenbase_ora.
			 * Note: If you want to support some operators for them,
			 * please add some code in checkBinaryType.
			 */
			if (ORA_MODE && !InPlpgsqlFunc())
			{
				checkBinaryType(ltypeId, opername);
				checkBinaryType(rtypeId, opername);
			}
		}

		/*
		 * try coerce TYPCATEGORY_STRING to TYPCATEGORY_NUMERIC
		 * Normally, for "xxx::float8||yyy::float8", it should add a operator '||' with two float8
		 * parameters, such as float8cat.
		 * But the type float8 has the attribute 'typispreferred = true', it leads that
		 * opentenbase choose a candicated function float8cat to process a var typed float8 concat to others,
		 * for exmaple: select 'abc' || 111.111::float8 , opentenbase using float8cat to concat these.
		 * So, it handle this case as a special one.
		 */
		if (opname != NIL &&
			(ltypeId != rtypeId || (FLOAT8OID == ltypeId && FLOAT8OID == rtypeId) ||
			(FLOAT4OID == ltypeId && FLOAT4OID == rtypeId) ||
			(TypeCategory(ltypeId) == TYPCATEGORY_UNKNOWN &&
			TypeCategory(rtypeId) == TYPCATEGORY_UNKNOWN) ||
			(TypeCategory(ltypeId) == TYPCATEGORY_STRING &&
			 TypeCategory(rtypeId) == TYPCATEGORY_STRING)))
		{
			if (schemaname != NULL)
			{
				/* search only in exact schema given */
				Oid	namespaceId = LookupExplicitNamespace(schemaname, true);
				if (!OidIsValid(namespaceId) || namespaceId != PG_CATALOG_NAMESPACE)
				{
					opername = NULL;
				}
			}

			/* For "xxx::float8||yyy::float8", it try to convert like "xxx::text || yyy::text" */
			if (opername != NULL && strcmp(opername, "||") == 0)
				process_concat_op(pstate, &ltypeId, &rtypeId, &ltree, &rtree, opname, location);
			else if (ORA_MODE && opername != NULL &&
				(IS_COMPARE_OP(opername) || IS_ARITHMETIC_OP(opername)))
					process_arith_and_comp_op(pstate, &ltypeId, &rtypeId, &ltree, &rtree, opername);
			else if (ORA_MODE && opername != NULL && ltypeId != rtypeId && IS_PATTERN_MATCH_OP(opername))
			{
				if (TYPCATEGORY_STRING != TypeCategory(ltypeId))
					update_op_arg_type(pstate, &ltypeId, TEXTOID, -1, &ltree);
				if (TYPCATEGORY_STRING != TypeCategory(rtypeId))
					update_op_arg_type(pstate, &rtypeId, TEXTOID, -1, &rtree);
			}
		}

		if (enable_lightweight_ora_syntax)
		{
			char *schemaname = NULL;
			char *opername = NULL;

			/* deconstruct the name list */
			DeconstructQualifiedName(opname, &schemaname, &opername);
			if (schemaname != NULL)
			{
				/* search only in exact schema given */
				Oid namespaceId = LookupExplicitNamespace(schemaname, true);
				if (!OidIsValid(namespaceId) || namespaceId != PG_CATALOG_NAMESPACE)
				{
					opername = NULL;
				}
			}

			if (opername != NULL)
			{
				/*
				 * timestamp + '1 day'
				 *
				 * (timestamp + unknown) has multiple candidates operator when
				 * enable_lightweight_ora_syntax is on, in which case, for the literal like
				 * '1 day' for type unknown, let us see if it could be decoded
				 * as an interval first by checking if it contains any
				 * characters other than numeric characters
				 *
				 * note that this is a feature of LIGHTWEIGHT_ORA but not opentenbase_ora's
				 */
				if (strcmp(opername, "+") == 0 &&
					ltypeId == TIMESTAMPOID && rtypeId == UNKNOWNOID)
				{
					Datum rdatum;
					const char *rliteral = "";

					Assert(IsA(rtree, Const));
					rdatum = ((Const *) rtree)->constvalue;
					if (rdatum)
						rliteral = DatumGetCString(rdatum);
					while (*rliteral != '\0')
					{
						/*
						 * take it as an interval if it contains extra
						 * characters to avoid casting it to a numeric type
						 */
						if (!isdigit(*rliteral) && !isspace(*rliteral) &&
							*rliteral != '.')
						{
							Node *rtarget;

							rtarget = coerce_to_target_type(pstate,
															rtree,
															rtypeId,
															INTERVALOID,
															-1,
															COERCION_EXPLICIT,
															COERCE_EXPLICIT_CAST,
															-1);
							if (rtarget != NULL)
							{
								rtree = rtarget;
								rtypeId = exprType(rtree);
							}
							break;
						}
						rliteral++;
					}
				}
				else if (strcmp(opername, "/") == 0 &&
						 IsIntegerType(ltypeId) && IsIntegerType(rtypeId))
				{
					update_op_arg_type(pstate, &ltypeId, FLOAT8OID, -1, &ltree);
					update_op_arg_type(pstate, &rtypeId, FLOAT8OID, -1, &rtree);
				}
			}
		}

		tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
	}

	opform = (Form_pg_operator) GETSTRUCT(tup);

	/* Check it's not a shell */
	if (!RegProcedureIsValid(opform->oprcode))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator is only a shell: %s",
						op_signature_string(opname,
											opform->oprkind,
											opform->oprleft,
											opform->oprright)),
				 parser_errposition(pstate, location)));

	/* Do typecasting and build the expression tree */
	if (rtree == NULL)
	{
		/* right operator */
		args = list_make1(ltree);
		actual_arg_types[0] = ltypeId;
		declared_arg_types[0] = opform->oprleft;
		nargs = 1;
	}
	else if (ltree == NULL)
	{
		/* left operator */
		args = list_make1(rtree);
		actual_arg_types[0] = rtypeId;
		declared_arg_types[0] = opform->oprright;
		nargs = 1;
	}
	else
	{
		/* otherwise, binary operator */
		args = list_make2(ltree, rtree);
		actual_arg_types[0] = ltypeId;
		actual_arg_types[1] = rtypeId;
		declared_arg_types[0] = opform->oprleft;
		declared_arg_types[1] = opform->oprright;
		nargs = 2;
	}

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   nargs,
											   opform->oprresult,
											   false);

	/* perform the necessary typecasting of arguments */
	make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

	/* and build the expression node */
	result = makeNode(OpExpr);
	result->opno = oprid(tup);
#ifdef _PG_ORCL_
	if (ORA_MODE &&
		(strcmp(NameStr(opform->oprname), "||") == 0) &&
		(((TEXTOID == opform->oprleft) &&	(TEXTOID == opform->oprright)) ||
		((NUMERICOID == opform->oprleft) &&	(NUMERICOID == opform->oprright))))
	{
		/*
		 * if 'xxx' || 'yyy' or null || 'xxx' or 'xxx' || null, 
		 * it change the opfuncid, replace "textcat(oid=1258)"  by "concat(oid=3058)"
		 */
		result->opfuncid = CONCAT_FUNC_OID;
	}
	else
#endif
	{
		result->opfuncid = opform->oprcode;
	}

	result->opresulttype = rettype;
	result->opretset = get_func_retset(opform->oprcode);
	/* opcollid and inputcollid will be set by parse_collate.c */
	result->args = args;
	result->location = location;

	/* if it returns a set, check that's OK */
	if (result->opretset)
	{
		check_srf_call_placement(pstate, last_srf, location);
		/* ... and remember it for error checks at higher levels */
		pstate->p_last_srf = (Node *) result;
	}

	ReleaseSysCache(tup);

	return (Expr *) result;
}



static Node *
transformStrArrayToNumericArray(ParseState *pstate, Node *a)
{
	ArrayExpr	*newa = makeNode(ArrayExpr);
	List		*newelems = NIL;
	ArrayExpr	*atree;
	ListCell	*lc;

	Assert(IsA(a, ArrayExpr));

	atree = (ArrayExpr *) a;
	newa->multidims = false;

	foreach(lc, atree->elements)
	{
		Node	*e = (Node *) lfirst(lc);
		Node	*newe = e;

		if (IsA(e, ArrayExpr))
		{
			newe = transformStrArrayToNumericArray(pstate, e);
			newa->multidims = true;
		}
		else
		{
			newe = coerce_to_target_type(pstate, e,
										 exprType(e),
										 NUMERICOID,
										 -1,
										 COERCION_EXPLICIT,
										 COERCE_EXPLICIT_CAST,
										 -1);

			if (newe == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_CANNOT_COERCE),
						errmsg("cannot cast type %s to %s",
						format_type_be(exprType(e)),
						format_type_be(NUMERICOID)),
						parser_errposition(pstate, exprLocation(e))));
		}

		newelems = lappend(newelems, newe);
	}

	newa->elements = newelems;
	newa->element_typeid = NUMERICOID;
	newa->array_collid = 0;
	newa->array_typeid = get_array_type(NUMERICOID);
	newa->location = atree->location;

	return (Node *) newa;
}

/*
 * make_scalar_array_op()
 *		Build expression tree for "scalar op ANY/ALL (array)" construct.
 */
Expr *
make_scalar_array_op(ParseState *pstate, List *opname,
					 bool useOr,
					 Node *ltree, Node *rtree,
					 int location)
{
	Oid			ltypeId,
				rtypeId,
				atypeId,
				res_atypeId;
	Operator	tup;
	Form_pg_operator opform;
	Oid			actual_arg_types[2];
	Oid			declared_arg_types[2];
	List	   *args;
	Oid			rettype;
	ScalarArrayOpExpr *result;

	ltypeId = exprType(ltree);
	atypeId = exprType(rtree);

	/*
	 * The right-hand input of the operator will be the element type of the
	 * array.  However, if we currently have just an untyped literal on the
	 * right, stay with that and hope we can resolve the operator.
	 */
	if (atypeId == UNKNOWNOID)
		rtypeId = UNKNOWNOID;
	else
	{
		rtypeId = get_base_element_type(atypeId);
		if (!OidIsValid(rtypeId))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("op ANY/ALL (array) requires array on right side"),
					 parser_errposition(pstate, location)));
	}

	/*
	 * try coerce TYPCATEGORY_STRING to TYPCATEGORY_NUMERIC
	 */
	if (ORA_MODE && opname != NIL && (ltypeId != rtypeId))
	{
		char *schemaname = NULL;
		char *opername = NULL;

		DeconstructQualifiedName(opname, &schemaname, &opername);
		if (schemaname != NULL)
		{
			/* search only in exact schema given */
			Oid	namespaceId = LookupExplicitNamespace(schemaname, true);
			if (!OidIsValid(namespaceId) || namespaceId != PG_CATALOG_NAMESPACE)
				opername = NULL;
		}

		if (opername != NULL && ltypeId != rtypeId &&
		   (strcmp(opername, "=") == 0 || strcmp(opername, "<>") == 0 ||
			strcmp(opername, ">") == 0 || strcmp(opername, ">=") == 0 ||
			strcmp(opername, "<") == 0 || strcmp(opername, "<=") == 0))
		{
			TYPCATEGORY	lcate = TypeCategory(ltypeId);
			TYPCATEGORY	rcate = TypeCategory(rtypeId);

			/* string op all(123, 456) */
			if (lcate == TYPCATEGORY_STRING && rcate == TYPCATEGORY_NUMERIC)
			{
				Node	*ltarget = coerce_to_target_type(pstate,
														 ltree,
														 ltypeId,
														 NUMERICOID,
														 -1,
														 COERCION_EXPLICIT,
														 COERCE_EXPLICIT_CAST,
														 -1);
				if (ltarget != NULL)
				{
					ltree = ltarget;
					ltypeId = exprType(ltree);
				}
			}
			/* ltree op all('123', '456') */
			else if (lcate == TYPCATEGORY_NUMERIC && rcate == TYPCATEGORY_STRING)
			{
				rtypeId = NUMERICOID;
				rtree = transformStrArrayToNumericArray(pstate, rtree);
				atypeId = exprType(rtree);
			}
		}
	}

	/* Now resolve the operator */
	tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
	opform = (Form_pg_operator) GETSTRUCT(tup);

	/* Check it's not a shell */
	if (!RegProcedureIsValid(opform->oprcode))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator is only a shell: %s",
						op_signature_string(opname,
											opform->oprkind,
											opform->oprleft,
											opform->oprright)),
				 parser_errposition(pstate, location)));

	args = list_make2(ltree, rtree);
	actual_arg_types[0] = ltypeId;
	actual_arg_types[1] = rtypeId;
	declared_arg_types[0] = opform->oprleft;
	declared_arg_types[1] = opform->oprright;

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   2,
											   opform->oprresult,
											   false);

	/*
	 * Check that operator result is boolean
	 */
	if (rettype != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("op ANY/ALL (array) requires operator to yield boolean"),
				 parser_errposition(pstate, location)));
	if (get_func_retset(opform->oprcode))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("op ANY/ALL (array) requires operator not to return a set"),
				 parser_errposition(pstate, location)));

	/*
	 * Now switch back to the array type on the right, arranging for any
	 * needed cast to be applied.  Beware of polymorphic operators here;
	 * enforce_generic_type_consistency may or may not have replaced a
	 * polymorphic type with a real one.
	 */
	if (IsPolymorphicType(declared_arg_types[1]))
	{
		/* assume the actual array type is OK */
		res_atypeId = atypeId;
	}
	else
	{
		res_atypeId = get_array_type(declared_arg_types[1]);
		if (!OidIsValid(res_atypeId))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find array type for data type %s",
							format_type_be(declared_arg_types[1])),
					 parser_errposition(pstate, location)));
	}
	actual_arg_types[1] = atypeId;
	declared_arg_types[1] = res_atypeId;

	/* perform the necessary typecasting of arguments */
	make_fn_arguments(pstate, args, actual_arg_types, declared_arg_types);

	/* and build the expression node */
	result = makeNode(ScalarArrayOpExpr);
	result->opno = oprid(tup);
	result->opfuncid = opform->oprcode;
	result->useOr = useOr;
	/* inputcollid will be set by parse_collate.c */
	result->args = args;
	result->location = location;

	ReleaseSysCache(tup);

	return (Expr *) result;
}


/*
 * Lookaside cache to speed operator lookup.  Possibly this should be in
 * a separate module under utils/cache/ ?
 *
 * The idea here is that the mapping from operator name and given argument
 * types is constant for a given search path (or single specified schema OID)
 * so long as the contents of pg_operator and pg_cast don't change.  And that
 * mapping is pretty expensive to compute, especially for ambiguous operators;
 * this is mainly because there are a *lot* of instances of popular operator
 * names such as "=", and we have to check each one to see which is the
 * best match.  So once we have identified the correct mapping, we save it
 * in a cache that need only be flushed on pg_operator or pg_cast change.
 * (pg_cast must be considered because changes in the set of implicit casts
 * affect the set of applicable operators for any given input datatype.)
 *
 * XXX in principle, ALTER TABLE ... INHERIT could affect the mapping as
 * well, but we disregard that since there's no convenient way to find out
 * about it, and it seems a pretty far-fetched corner-case anyway.
 *
 * Note: at some point it might be worth doing a similar cache for function
 * lookups.  However, the potential gain is a lot less since (a) function
 * names are generally not overloaded as heavily as operator names, and
 * (b) we'd have to flush on pg_proc updates, which are probably a good
 * deal more common than pg_operator updates.
 */

/* The operator cache hashtable */
static HTAB *OprCacheHash = NULL;


/*
 * make_oper_cache_key
 *		Fill the lookup key struct given operator name and arg types.
 *
 * Returns TRUE if successful, FALSE if the search_path overflowed
 * (hence no caching is possible).
 *
 * pstate/location are used only to report the error position; pass NULL/-1
 * if not available.
 */
static bool
make_oper_cache_key(ParseState *pstate, OprCacheKey *key, List *opname,
					Oid ltypeId, Oid rtypeId, int location)
{
	char	   *schemaname;
	char	   *opername;

	/* deconstruct the name list */
	DeconstructQualifiedName(opname, &schemaname, &opername);

	/* ensure zero-fill for stable hashing */
	MemSet(key, 0, sizeof(OprCacheKey));

	/* save operator name and input types into key */
	strlcpy(key->oprname, opername, NAMEDATALEN);
	key->left_arg = ltypeId;
	key->right_arg = rtypeId;

	if (schemaname)
	{
		ParseCallbackState pcbstate;

		/* search only in exact schema given */
		setup_parser_errposition_callback(&pcbstate, pstate, location);
		key->search_path[0] = LookupExplicitNamespace(schemaname, false);
		cancel_parser_errposition_callback(&pcbstate);
	}
	else
	{
		/* get the active search path */
		if (fetch_search_path_array(key->search_path,
									MAX_CACHED_PATH_LEN) > MAX_CACHED_PATH_LEN)
			return false;		/* oops, didn't fit */
	}

	return true;
}

/*
 * find_oper_cache_entry
 *
 * Look for a cache entry matching the given key.  If found, return the
 * contained operator OID, else return InvalidOid.
 */
static Oid
find_oper_cache_entry(OprCacheKey *key)
{
	OprCacheEntry *oprentry;

	if (OprCacheHash == NULL)
	{
		/* First time through: initialize the hash table */
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(OprCacheKey);
		ctl.entrysize = sizeof(OprCacheEntry);
		OprCacheHash = hash_create("Operator lookup cache", 256,
								   &ctl, HASH_ELEM | HASH_BLOBS);

		/* Arrange to flush cache on pg_operator and pg_cast changes */
		CacheRegisterSyscacheCallback(OPERNAMENSP,
									  InvalidateOprCacheCallBack,
									  (Datum) 0);
		CacheRegisterSyscacheCallback(CASTSOURCETARGET,
									  InvalidateOprCacheCallBack,
									  (Datum) 0);
	}

	/* Look for an existing entry */
	oprentry = (OprCacheEntry *) hash_search(OprCacheHash,
											 (void *) key,
											 HASH_FIND, NULL);
	if (oprentry == NULL)
		return InvalidOid;

	return oprentry->opr_oid;
}

/*
 * make_oper_cache_entry
 *
 * Insert a cache entry for the given key.
 */
static void
make_oper_cache_entry(OprCacheKey *key, Oid opr_oid)
{
	OprCacheEntry *oprentry;

	Assert(OprCacheHash != NULL);

	oprentry = (OprCacheEntry *) hash_search(OprCacheHash,
											 (void *) key,
											 HASH_ENTER, NULL);
	oprentry->opr_oid = opr_oid;
}

/*
 * Callback for pg_operator and pg_cast inval events
 */
static void
InvalidateOprCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	OprCacheEntry *hentry;

	Assert(OprCacheHash != NULL);

	/* Currently we just flush all entries; hard to be smarter ... */
	hash_seq_init(&status, OprCacheHash);

	while ((hentry = (OprCacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(OprCacheHash,
						(void *) &hentry->key,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted");
	}
}

/*
 * Determine the unknown type to numeric type while the other type is int2/int4/int8.
 *
 * Note: The integer, int, smallint type have different meaning between opentenbase_ora
 * and OPENTENBASE. They are int4, int4, int2 in OPENTENBASE while they are number in opentenbase_ora,
 * so we need to coerce the unknown type to numeric when the other type can't be
 * treated as int2/int4/int8.
 */
static void
determine_unknown_type(ParseState *pstate,
					Oid *left_type, Oid *right_type,
					Node **ltree, Node **rtree,
					char *opername)
{
	Oid other_type = InvalidOid;
	Oid *unknown_type = NULL;
	Node **unknown_node = NULL;

	if (*left_type == UNKNOWNOID)
	{
		unknown_type = left_type;
		unknown_node = ltree;
		other_type = *right_type;
	}
	else if (*right_type == UNKNOWNOID)
	{
		unknown_type = right_type;
		unknown_node = rtree;
		other_type = *left_type;
	}
	else
		return;

	if (IsA(*unknown_node, Const) &&
		IsIntegerType(other_type) &&
		(IS_ARITHMETIC_OP(opername) || is_unknown_a_numeric(*unknown_node, other_type)))
	{
		*unknown_type = NUMERICOID;
		*unknown_node = coerce_to_target_type(pstate,
											  *unknown_node, UNKNOWNOID,
											  NUMERICOID, -1,
											  COERCION_IMPLICIT,
											  COERCE_IMPLICIT_CAST,
											  -1);
	}
}

/*
 * Whether the type will be coerced to text for || operator.
 *
 * Note: This function is just inherits from last version.
 * And there are existing instance, please keep the origin action.
 */
static inline bool
use_text_concat(Oid type)
{
	return TypeCategory(type) == TYPCATEGORY_NUMERIC ||
			type == RAWOID || type == LRAWOID ||
			((ORA_MODE || enable_lightweight_ora_syntax) &&
			(TypeCategory(type) == TYPCATEGORY_DATETIME || type == INTERVALOID));
}

/*
 * Process the || operator.
 *
 * Coerce the type to text and use the text || operator.
 */
static void
process_concat_op(ParseState *pstate, Oid *left_type, Oid *right_type,
				Node **ltree, Node **rtree, List *opname, int location)
{
	if (use_text_concat(*left_type))
	{
		/* Find the text || right type operator */
		Operator tmp_tup = oper(pstate, opname, TEXTOID, *right_type, true, location);

		if (ORA_MODE || HeapTupleIsValid(tmp_tup))
		{
			update_op_arg_type(pstate, left_type, TEXTOID, -1, ltree);

			if (HeapTupleIsValid(tmp_tup))
				ReleaseSysCache(tmp_tup);
		}
	}

	if (use_text_concat(*right_type))
	{
		/* Find the left || text type operator */
		Operator tmp_tup = oper(pstate, opname, *left_type, TEXTOID, true, location);

		if (ORA_MODE || HeapTupleIsValid(tmp_tup))
		{
			update_op_arg_type(pstate, right_type, TEXTOID, -1, rtree);

			if (HeapTupleIsValid(tmp_tup))
				ReleaseSysCache(tmp_tup);
		}
	}
}

/* Coerce the source type to target type and update source type and tree. */
static inline void
update_op_arg_type(ParseState *pstate, Oid *source_type, Oid target_type,
						int target_typmod, Node **tree)
{
	Node *target = coerce_to_target_type(pstate, *tree, *source_type, target_type,
										target_typmod, COERCION_EXPLICIT,
										COERCE_EXPLICIT_CAST, -1);

	if (target != NULL)
	{
		*tree = target;
		*source_type = exprType(*tree);
	}
}

/*
 * Process arithmetic and comparison operator as follows:
 * +/- between opentenbase_ora.date/timestamp/timestamptz/timestampltz and string/unknown
 * + between string/unknown and opentenbase_ora.date/timestamp/timestamptz/timestampltz
 * arithmetic operator between string/unknown and string/unknown
 * comparison operator between string/unknown and date/timestamp/timestamptz/timestampltz
 * comparison operator between RAWOID and TEXTOID
 * comparison operator between date/timestamp/timestamptz/timestampltz and string/unknown
 * comparison operator between TEXTOID and RAWOID
 * comparison operator between varchar/varchar2/nvarchar2 and char
 * comparison operator between char and varchar/varchar2/nvarchar2
 * comparison operator and arithmetic operator between numeric and string
 * comparison operator and arithmetic operator between string and numeric
 * comparison operator and arithmetic operator between int2/int4/int8 and unknown
 */
static void
process_arith_and_comp_op(ParseState *pstate, Oid *left_type, Oid *right_type,
						  Node **ltree, Node **rtree, char *opername)
{
#define IS_STR_TYPE(cat) (TYPCATEGORY_STRING == (cat) || TYPCATEGORY_UNKNOWN == (cat))
#define IS_DATE_AND_CSTR(cat1, cat2) ((cat1) == TYPCATEGORY_DATETIME && IS_STR_TYPE(cat2))
#define IS_CHAR_CMP_VCHAR(typ1, typ2) ((typ1) == BPCHAROID && ((typ2) == VARCHAROID || (typ2) == VARCHAR2OID || (typ2) == NVARCHAR2OID))
	TYPCATEGORY left_typ_cat = TypeCategory(*left_type);
	TYPCATEGORY right_typ_cat = TypeCategory(*right_type);

	/* deal with arithmetic operator */
	if (IS_ARITHMETIC_OP(opername))
	{
		/* Datetime +/- string */
		if (IS_ORA_DATETIME(*left_type) &&
			IS_STR_TYPE(right_typ_cat) &&
			(strcmp(opername, "-") == 0 ||
			 strcmp(opername, "+") == 0))
			update_op_arg_type(pstate, right_type, NUMERICOID, -1, rtree);
			/* string + Datetime */
		else if (IS_STR_TYPE(left_typ_cat) &&
				 IS_ORA_DATETIME(*right_type) &&
				 strcmp(opername, "+") == 0)
			update_op_arg_type(pstate, left_type, NUMERICOID, -1, ltree);
			/* text arithmetic text */
		else if (IS_STR_TYPE(left_typ_cat) &&
				 IS_STR_TYPE(right_typ_cat))
		{
			update_op_arg_type(pstate, left_type, NUMERICOID, -1, ltree);
			update_op_arg_type(pstate, right_type, NUMERICOID, -1, rtree);
		}
	}
	/* deal with compare operator */
	else if (IS_COMPARE_OP(opername))
	{
		/*
		 * Datetime <=.<=.<.>.=.<> string
		 * or
		 * RAWOID <=.<=.<.>.=.<> TEXTOID (just TEXTOID now)
		 */
		if (IS_DATE_AND_CSTR(left_typ_cat, right_typ_cat) ||
			(*right_type == RAWOID && *left_type == TEXTOID))
			update_op_arg_type(pstate, right_type, *left_type, -1, rtree);
		/*
		 * string <=.<=.<.>.=.<> Datetime
		 * or
		 * RAWOID <=.<=.<.>.=.<> TEXTOID (just TEXTOID now)
		 */
		else if (IS_DATE_AND_CSTR(right_typ_cat, left_typ_cat) ||
				 (*left_type == RAWOID && *right_type == TEXTOID))
			update_op_arg_type(pstate, left_type, *right_type, -1, ltree);
		/* Convert char to varchar while compare char type with varchar type like opentenbase_ora */
		else if (IS_CHAR_CMP_VCHAR(*left_type, *right_type))
			update_op_arg_type(pstate, left_type, TEXTOID, -1, ltree);
		else if (IS_CHAR_CMP_VCHAR(*right_type, *left_type))
			update_op_arg_type(pstate, right_type, TEXTOID, -1, rtree);
		/*
		 * The TEXT type and CHAR type need to be converted to CHAR type together,
		 * otherwise there will be inconsistent spaces at the end of the string.
		 */
		else if (IS_CHAR_TYPE(*left_type) && *right_type == TEXTOID)
			update_op_arg_type(pstate, right_type, *left_type, -1, rtree);
		else if (IS_CHAR_TYPE(*right_type) && *left_type == TEXTOID)
			update_op_arg_type(pstate, left_type, *right_type, -1, ltree);
	}

	/* '123' = 1 and '123' + 1 */
	if (TYPCATEGORY_STRING == left_typ_cat &&
		TYPCATEGORY_NUMERIC == right_typ_cat )
		update_op_arg_type(pstate, left_type, NUMERICOID, -1, ltree);
	/* 1 = '123' and 1 + '123' */
	else if (TYPCATEGORY_NUMERIC == left_typ_cat &&
			 TYPCATEGORY_STRING == right_typ_cat)
		update_op_arg_type(pstate, right_type, NUMERICOID, -1, rtree);

	/* Coerce unknown type to numeric while another type is int2 or int4 or int8 */
	determine_unknown_type(pstate, left_type, right_type, ltree, rtree, opername);
}
