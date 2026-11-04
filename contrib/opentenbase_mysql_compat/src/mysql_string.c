/*-------------------------------------------------------------------------
 *
 * mysql_string.c
 *	  MySQL-compatible string functions.
 *
 * Functions: IFNULL, CONCAT_WS, FIND_IN_SET, ELT, FIELD, INSERT
 *
 * Copyright (c) 2026, OpenTenBase Contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"
#include <ctype.h>

/* Forward declarations */
Datum mysql_ifnull(PG_FUNCTION_ARGS);
Datum mysql_concat_ws(PG_FUNCTION_ARGS);
Datum mysql_find_in_set(PG_FUNCTION_ARGS);
Datum mysql_elt(PG_FUNCTION_ARGS);
Datum mysql_field(PG_FUNCTION_ARGS);
Datum mysql_insert_func(PG_FUNCTION_ARGS);


/*
 * IFNULL(expr1, expr2)
 * Returns the first non-NULL argument.
 * Equivalent to COALESCE(expr1, expr2) in standard SQL.
 */
PG_FUNCTION_INFO_V1(mysql_ifnull);
Datum
mysql_ifnull(PG_FUNCTION_ARGS)
{
	if (!PG_ARGISNULL(0))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	else if (!PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));
	else
		PG_RETURN_NULL();
}


/*
 * CONCAT_WS(separator, str1, str2, ...)
 * Concatenates strings with separator, skipping NULL values.
 * MySQL behavior: NULL separator returns NULL.
 */
PG_FUNCTION_INFO_V1(mysql_concat_ws);
Datum
mysql_concat_ws(PG_FUNCTION_ARGS)
{
	text	   *sep;
	text	   *result;
	StringInfoData buf;
	int			i;
	bool		first = true;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	sep = PG_GETARG_TEXT_P(0);
	initStringInfo(&buf);

	for (i = 1; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			text   *arg = PG_GETARG_TEXT_P(i);

			if (!first)
				appendBinaryStringInfo(&buf, VARDATA_ANY(sep), VARSIZE_ANY_EXHDR(sep));
			appendBinaryStringInfo(&buf, VARDATA_ANY(arg), VARSIZE_ANY_EXHDR(arg));
			first = false;
		}
	}

	if (first)
		PG_RETURN_NULL();

	result = cstring_to_text_with_len(buf.data, buf.len);
	pfree(buf.data);
	PG_RETURN_TEXT_P(result);
}


/*
 * FIND_IN_SET(str, strlist)
 * Returns the position of str in the comma-separated list strlist.
 * Returns 0 if not found or if either argument is NULL.
 * MySQL behavior: 1-indexed positions.
 */
PG_FUNCTION_INFO_V1(mysql_find_in_set);
Datum
mysql_find_in_set(PG_FUNCTION_ARGS)
{
	text	   *str;
	text	   *strlist;
	char	   *needle;
	char	   *haystack;
	char	   *token;
	char	   *saveptr;
	int			pos = 1;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_INT32(0);

	str = PG_GETARG_TEXT_P(0);
	strlist = PG_GETARG_TEXT_P(1);

	needle = text_to_cstring(str);
	haystack = text_to_cstring(strlist);

	token = strtok_r(haystack, ",", &saveptr);
	while (token != NULL)
	{
		/* Trim leading whitespace */
		while (*token && isspace((unsigned char) *token))
			token++;
		/* Trim trailing whitespace */
		char *end = token + strlen(token) - 1;
		while (end > token && isspace((unsigned char) *end))
			*end-- = '\0';

		if (strcmp(token, needle) == 0)
		{
			pfree(needle);
			pfree(haystack);
			PG_RETURN_INT32(pos);
		}
		token = strtok_r(NULL, ",", &saveptr);
		pos++;
	}

	pfree(needle);
	pfree(haystack);
	PG_RETURN_INT32(0);
}


/*
 * ELT(N, str1, str2, str3, ...)
 * Returns the N-th string from the argument list.
 * Returns NULL if N < 1 or N > number of arguments beyond N.
 * MySQL behavior: 1-indexed.
 */
PG_FUNCTION_INFO_V1(mysql_elt);
Datum
mysql_elt(PG_FUNCTION_ARGS)
{
	int32		n;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	n = PG_GETARG_INT32(0);
	if (n < 1 || n >= PG_NARGS())
		PG_RETURN_NULL();

	if (PG_ARGISNULL(n))
		PG_RETURN_NULL();

	PG_RETURN_DATUM(PG_GETARG_DATUM(n));
}


/*
 * FIELD(str, str1, str2, str3, ...)
 * Returns the index (1-indexed) of str in the str1, str2, ... list.
 * Returns 0 if not found.
 * Comparison is case-insensitive for strings (MySQL behavior).
 */
PG_FUNCTION_INFO_V1(mysql_field);
Datum
mysql_field(PG_FUNCTION_ARGS)
{
	int			i;
	text	   *needle;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT32(0);

	needle = PG_GETARG_TEXT_P(0);

	for (i = 1; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			text   *candidate = PG_GETARG_TEXT_P(i);

			if (VARSIZE_ANY_EXHDR(needle) == VARSIZE_ANY_EXHDR(candidate) &&
				strncasecmp(VARDATA_ANY(needle), VARDATA_ANY(candidate),
							VARSIZE_ANY_EXHDR(needle)) == 0)
			{
				PG_RETURN_INT32(i);
			}
		}
	}

	PG_RETURN_INT32(0);
}


/*
 * INSERT(original_string, position, length, replacement_string)
 * Inserts replacement_string into original_string at the given position,
 * replacing 'length' characters.
 * 1-indexed like MySQL.
 * MySQL behavior: if position > length of original, returns original.
 */
PG_FUNCTION_INFO_V1(mysql_insert_func);
Datum
mysql_insert_func(PG_FUNCTION_ARGS)
{
	text	   *orig;
	int32		pos;
	int32		len;
	text	   *repl;
	char	   *orig_str;
	char	   *repl_str;
	int			orig_len;
	int			repl_len;
	char	   *result_str;
	text	   *result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) ||
		PG_ARGISNULL(2) || PG_ARGISNULL(3))
		PG_RETURN_NULL();

	orig = PG_GETARG_TEXT_P(0);
	pos = PG_GETARG_INT32(1);
	len = PG_GETARG_INT32(2);
	repl = PG_GETARG_TEXT_P(3);

	orig_str = text_to_cstring(orig);
	repl_str = text_to_cstring(repl);
	orig_len = strlen(orig_str);
	repl_len = strlen(repl_str);

	/* 1-indexed to 0-indexed */
	pos--;

	if (pos < 0)
		pos = 0;
	if (pos > orig_len)
	{
		pfree(orig_str);
		pfree(repl_str);
		PG_RETURN_TEXT_P(orig);
	}

	/* Calculate result length */
	result_str = palloc(orig_len - len + repl_len + 1);
	memcpy(result_str, orig_str, pos);
	memcpy(result_str + pos, repl_str, repl_len);
	if (pos + len < orig_len)
		memcpy(result_str + pos + repl_len,
			   orig_str + pos + len,
			   orig_len - pos - len);
	result_str[orig_len - len + repl_len] = '\0';

	result = cstring_to_text(result_str);
	pfree(orig_str);
	pfree(repl_str);
	pfree(result_str);
	PG_RETURN_TEXT_P(result);
}
