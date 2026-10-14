/*-------------------------------------------------------------------------
 *
 * opentenbase_mysql_compat.c
 *		Common infrastructure for the opentenbase_mysql_compat extension.
 *
 * Provides MySQL-compatible functions to ease migration of MySQL based
 * applications to OpenTenBase.  This file holds the module glue and a
 * shared helper that renders an arbitrary SQL value (an argument declared
 * as "any") into its text representation, mirroring MySQL's implicit
 * string conversion.
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/opentenbase_mysql_compat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/*
 * mysql_any_to_cstring
 *
 * Convert the (already checked non-NULL) argument argno of a function
 * whose parameters are declared "any" into a palloc'ed C string, using
 * the type's output function.  This mirrors how MySQL implicitly turns
 * numbers, dates and other scalars into strings.
 */
char *
mysql_any_to_cstring(FunctionCallInfo fcinfo, int argno)
{
	Oid			typoutput;
	bool		typIsVarlena;
	FmgrInfo   *finfo;

	Assert(!PG_ARGISNULL(argno));

	finfo = palloc0(sizeof(FmgrInfo));

	getTypeOutputInfo(get_fn_expr_argtype(fcinfo->flinfo, argno),
					  &typoutput, &typIsVarlena);
	fmgr_info(typoutput, finfo);

	return OutputFunctionCall(finfo, PG_GETARG_DATUM(argno));
}
