/*-------------------------------------------------------------------------
 *
 * parse_func.h
 *
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_func.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_FUNC_H
#define PARSER_FUNC_H

#include "catalog/namespace.h"
#include "parser/parse_node.h"


/* Result codes for func_get_detail */
typedef enum
{
	FUNCDETAIL_NOTFOUND,		/* no matching function */
	FUNCDETAIL_MULTIPLE,		/* too many matching functions */
	FUNCDETAIL_NORMAL,			/* found a matching regular function */
	FUNCDETAIL_PROCEDURE,		/* found a matching procedure */
	FUNCDETAIL_AGGREGATE,		/* found a matching aggregate function */
	FUNCDETAIL_WINDOWFUNC,		/* found a matching window function */
	FUNCDETAIL_COERCION			/* it's a type coercion request */
} FuncDetailCode;

typedef enum
{
  SYNON_AS_OTHER = 0,
  SYNON_AS_PKG = (1 << 0),
  SYNON_AS_FUNC = (1 << 1),
  SYNON_AS_TYPE = (1 << 2),
} SynonRepInfo;

typedef struct
{
	FunctionMemberInfo info;
	Oid		typeOid;
	Oid		creatednsOid;
	Oid		typensOid;
  	bits32	repinfo;
} TypObjSynonInfo;

extern Node *ParseFuncOrColumn(ParseState *pstate, List *funcname, List *fargs,
							   Node *last_srf, FuncCall *fn, bool proc_call,
							   int location);

extern FuncDetailCode func_get_detail(List *funcname,
				List *fargs, List *fargnames,
				int nargs, Oid *argtypes,
				bool expand_variadic, bool expand_defaults,
				Oid *funcid, Oid *rettype,
				bool *retset, int *nvargs, Oid *vatype,
				Oid **true_typeids, List **argdefaults, ParseState *pstate);

#ifdef _PG_ORCL_
extern FuncDetailCode func_get_detail_with_funcs(List *funcname, List *fargs,
									List *fargnames, int nargs, Oid *argtypes,
									bool expand_variadic, bool expand_defaults,
									Oid *funcid, Oid *rettype, bool *retset,
									int *nvargs, Oid *vatype, Oid **true_typeids,
									List **argdefaults, List *with_funcs,
									int *with_funcid, bool *keep_looking,
									ParseState *pstate);
#endif
extern int func_match_argtypes(int nargs,
					Oid *input_typeids,
					FuncCandidateList raw_candidates,
					FuncCandidateList *candidates,
#ifdef _PG_ORCL_
					List *fargs
					, ParseState *pstate,
#endif
					bool use_ora_implicit);

extern FuncCandidateList func_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates);

extern void make_fn_arguments(ParseState *pstate,
				  List *fargs,
				  Oid *actual_arg_types,
				  Oid *declared_arg_types);

extern const char *funcname_signature_string(const char *funcname, int nargs,
						  List *argnames, const Oid *argtypes);
extern const char *func_signature_string(List *funcname, int nargs,
					  List *argnames, const Oid *argtypes);

extern Oid LookupFuncName(List *funcname, int nargs, const Oid *argtypes,
			   bool noError);
extern Oid LookupFuncWithArgs(ObjectType objtype, ObjectWithArgs *func,
				   bool noError);

#ifdef _PG_ORCL_
extern Oid LookupSynonFuncWithArgs(ObjectType objtype, ObjectWithArgs *func);
#endif
extern void check_srf_call_placement(ParseState *pstate, Node *last_srf,
						 int location);

extern void check_pg_get_expr_args(ParseState *pstate, Oid fnoid, List *args);
extern bool func_is_pullup(Oid func_id, Node *node);
#endif							/* PARSE_FUNC_H */
