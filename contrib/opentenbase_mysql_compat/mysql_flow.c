/*-------------------------------------------------------------------------
 *
 * mysql_flow.c
 *		MySQL-compatible flow-control functions for
 *		opentenbase_mysql_compat.
 *
 * Implements ISNULL().  IF() and IFNULL() are declared as SQL-language
 * polymorphic functions in the extension script; CASE and COALESCE
 * already provide the correct lazy evaluation semantics.
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/mysql_flow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(mysql_isnull);

/*
 * mysql_isnull(any) -> int
 *
 * MySQL ISNULL(expr) returns 1 when the expression is NULL and 0
 * otherwise.
 */
Datum
mysql_isnull(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PG_ARGISNULL(0) ? 1 : 0);
}
