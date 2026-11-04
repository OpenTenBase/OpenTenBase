/*-------------------------------------------------------------------------
 *
 * mysql_flow.c
 *	  MySQL-compatible flow control functions.
 *
 * Functions: IF(condition, true_value, false_value)
 *
 * Copyright (c) 2026, OpenTenBase Contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

Datum mysql_if_func(PG_FUNCTION_ARGS);


/*
 * IF(expr1, expr2, expr3)
 * If expr1 is TRUE (non-zero and non-NULL), returns expr2; otherwise returns expr3.
 * MySQL behavior: expr1 is evaluated as a boolean.
 */
PG_FUNCTION_INFO_V1(mysql_if_func);
Datum
mysql_if_func(PG_FUNCTION_ARGS)
{
	bool		cond;

	if (PG_ARGISNULL(0))
		PG_RETURN_DATUM(PG_GETARG_DATUM(2));

	cond = PG_GETARG_BOOL(0);

	if (cond)
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(2));
}
