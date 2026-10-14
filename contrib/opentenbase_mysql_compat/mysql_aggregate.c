/*-------------------------------------------------------------------------
 *
 * mysql_aggregate.c
 *		MySQL-compatible aggregate functions for opentenbase_mysql_compat.
 *
 * Implements the GROUP_CONCAT transition functions.  NULL values are
 * skipped like MySQL does, and a group whose values are all NULL
 * yields NULL (not the empty string).
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/mysql_aggregate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "opentenbase_mysql_compat.h"

PG_FUNCTION_INFO_V1(mysql_group_concat_trans);
PG_FUNCTION_INFO_V1(mysql_group_concat_sep_trans);

/*
 * mysql_group_concat_trans(state text, value anyelement) -> text
 *
 * Transition function for GROUP_CONCAT(value), joining with commas.
 */
Datum
mysql_group_concat_trans(PG_FUNCTION_ARGS)
{
	StringInfoData str;

	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		PG_RETURN_TEXT_P(PG_GETARG_TEXT_PP(0));
	}

	initStringInfo(&str);

	if (!PG_ARGISNULL(0))
	{
		appendStringInfoString(&str, text_to_cstring(PG_GETARG_TEXT_PP(0)));
		appendStringInfoChar(&str, ',');
	}

	appendStringInfoString(&str, mysql_any_to_cstring(fcinfo, 1));

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/*
 * mysql_group_concat_sep_trans(state text, value anyelement, sep text) -> text
 *
 * Transition function for GROUP_CONCAT(value SEPARATOR sep) written in
 * the two-argument form GROUP_CONCAT(value, sep).
 */
Datum
mysql_group_concat_sep_trans(PG_FUNCTION_ARGS)
{
	StringInfoData str;

	if (PG_ARGISNULL(1))
	{
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		PG_RETURN_TEXT_P(PG_GETARG_TEXT_PP(0));
	}

	initStringInfo(&str);

	if (!PG_ARGISNULL(0))
	{
		appendStringInfoString(&str, text_to_cstring(PG_GETARG_TEXT_PP(0)));
		if (!PG_ARGISNULL(2))
			appendStringInfoString(&str, text_to_cstring(PG_GETARG_TEXT_PP(2)));
	}

	appendStringInfoString(&str, mysql_any_to_cstring(fcinfo, 1));

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}
