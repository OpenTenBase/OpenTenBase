/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *		Declarations for ruleutils.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/ruleutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RULEUTILS_H
#define RULEUTILS_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"


extern char *format_explicit_trans(Oid type, int32 typmod);
extern char *pg_get_indexdef_string(Oid indexrelid);
extern char *pg_get_indexdef_columns(Oid indexrelid, bool pretty);

extern char *pg_get_partkeydef_columns(Oid relid, bool pretty);

extern char *pg_get_constraintdef_command(Oid constraintId);
extern char *deparse_expression(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit);
extern char *deparse_expression_jumble_const(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit);
extern List *deparse_context_for(const char *aliasname, Oid relid);
extern List *deparse_context_for_plan_rtable(List *rtable, List *rtable_names);
extern List *set_deparse_context_planstate(List *dpcontext,
							  Node *planstate, List *ancestors);
extern List *select_rtable_names_for_explain(List *rtable,
								Bitmapset *rels_used);
extern char *generate_collation_name(Oid collid);
extern char *get_range_partbound_string(List *bound_datums);
extern const char *get_simple_binary_op_name(OpExpr *expr);
extern const char *get_binary_op_name(OpExpr *expr);

#ifdef __OPENTENBASE__
extern char *pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn);

extern char* pg_get_functiondef_worker(Oid funcid, Datum dat);
#endif

#endif							/* RULEUTILS_H */
