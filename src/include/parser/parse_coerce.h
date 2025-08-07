/*-------------------------------------------------------------------------
 *
 * parse_coerce.h
 *	Routines for type coercion.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_coerce.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_COERCE_H
#define PARSE_COERCE_H

#include "parser/parse_node.h"


/* Type categories (see TYPCATEGORY_xxx symbols in catalog/pg_type.h) */
typedef char TYPCATEGORY;

/* Result codes for find_coercion_pathway */
typedef enum CoercionPathType
{
	COERCION_PATH_NONE,			/* failed to find any coercion pathway */
	COERCION_PATH_FUNC,			/* apply the specified coercion function */
	COERCION_PATH_RELABELTYPE,	/* binary-compatible cast, no function */
	COERCION_PATH_ARRAYCOERCE,	/* need an ArrayCoerceExpr node */
	COERCION_PATH_COERCEVIAIO	/* need a CoerceViaIO node */
} CoercionPathType;

/* The implicit coercion in the opentenbase_ora */
typedef struct ora_implicit_coercion_rec
{
	Oid source_typoid;
	Oid target_typoid;
} ora_implicit_coercion_rec;

extern bool IsBinaryCoercible(Oid srctype, Oid targettype);
extern bool IsPreferredType(TYPCATEGORY category, Oid type);
extern TYPCATEGORY TypeCategory(Oid type);

extern Node *coerce_to_target_type(ParseState *pstate,
					  Node *expr, Oid exprtype,
					  Oid targettype, int32 targettypmod,
					  CoercionContext ccontext,
					  CoercionForm cformat,
					  int location);
extern bool can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
				CoercionContext ccontext, bool use_ora_implicit);
extern Node *coerce_type(ParseState *pstate, Node *node,
			Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
			CoercionContext ccontext, CoercionForm cformat, int location);
extern Node *coerce_to_domain(Node *arg, Oid baseTypeId, int32 baseTypeMod,
				 Oid typeId,
				 CoercionContext ccontext, CoercionForm cformat, int location,
				 bool hideInputCoercion);

extern Node *coerce_to_boolean(ParseState *pstate, Node *node,
				  const char *constructName);
extern Node *coerce_to_specific_type(ParseState *pstate, Node *node,
						Oid targetTypeId,
						const char *constructName);

extern Node *coerce_to_specific_type_typmod(ParseState *pstate, Node *node,
							   Oid targetTypeId, int32 targetTypmod,
							   const char *constructName);

extern int parser_coercion_errposition(ParseState *pstate,
							int coerce_location,
							Node *input_expr);

extern Oid select_common_type(ParseState *pstate, List *exprs,
				   const char *context, Node **which_expr);
extern Node *ora_coerce_to_common_type(ParseState *pstate, Node *node,
					Oid targetTypeId, const char *context);
extern Node *coerce_to_common_type(ParseState *pstate, Node *node,
					  Oid targetTypeId,
					  const char *context);

extern bool check_generic_type_consistency(Oid *actual_arg_types,
							   Oid *declared_arg_types,
							   int nargs);
extern Oid enforce_generic_type_consistency(Oid *actual_arg_types,
								 Oid *declared_arg_types,
								 int nargs,
								 Oid rettype,
								 bool allow_poly);
extern Oid resolve_generic_type(Oid declared_type,
					 Oid context_actual_type,
					 Oid context_declared_type);

extern CoercionPathType find_coercion_pathway(Oid targetTypeId,
					  Oid sourceTypeId,
					  CoercionContext ccontext,
					  Oid *funcid);
extern CoercionPathType find_typmod_coercion_function(Oid typeId,
							  Oid *funcid);
/* Is it implicit coercion in opentenbase_ora */
extern bool is_ora_implicit_coercion(Oid targetTypeId, Oid sourceTypeId);

#ifdef _PG_ORCL_
extern bool setOpTypeCanImplicitCast(ParseState *pstate, Node *node, Oid ltype, Oid rtype);
#endif
extern char getCastContext(Oid sourceTypeId, Oid targetTypeId);

/*
 * enable opentenbase_ora implicit coercion if enable_opentenbase_ora_implicit_coercion is on in
 * opentenbase_ora mode or enable_lightweight_ora_syntax is on in none-opentenbase_ora mode
 */
#define ENABLE_ORA_IMPLICIT_COERCION \
	((ORA_MODE && enable_opentenbase_ora_implicit_coercion) || \
	 (!ORA_MODE && enable_lightweight_ora_syntax))
#endif							/* PARSE_COERCE_H */
