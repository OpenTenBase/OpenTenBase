/*-------------------------------------------------------------------------
 *
 * mysql_math.c
 *		MySQL-compatible numeric functions for opentenbase_mysql_compat.
 *
 * Implements: FORMAT (number formatting with thousands separators),
 * BIN and OCT (radix conversions on 64-bit integers).
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/mysql_math.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

PG_FUNCTION_INFO_V1(mysql_format);
PG_FUNCTION_INFO_V1(mysql_bin);
PG_FUNCTION_INFO_V1(mysql_oct);

/*
 * mysql_format(numeric, scale int) -> text
 *
 * Round the value to scale decimal digits and render it with
 * comma thousands separators, like MySQL FORMAT(x, d).
 */
Datum
mysql_format(PG_FUNCTION_ARGS)
{
	Numeric		val;
	Numeric		rounded;
	char	   *digits;
	char	   *dp;
	StringInfoData res;
	char	   *p;
	int			intdigits;
	int			i;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	val = PG_GETARG_NUMERIC(0);

	/* Clamp the scale to MySQL's allowed range [0, 30]. */
	{
		int32		scale = PG_GETARG_INT32(1);

		if (scale < 0)
			scale = 0;
		if (scale > 30)
			scale = 30;

		rounded = DatumGetNumeric(DirectFunctionCall2(numeric_round,
													  NumericGetDatum(val),
													  Int32GetDatum(scale)));
	}

	digits = DatumGetCString(DirectFunctionCall1(numeric_out,
												NumericGetDatum(rounded)));

	initStringInfo(&res);

	/* Split off the sign. */
	p = digits;
	if (*p == '-')
	{
		appendStringInfoChar(&res, '-');
		p++;
	}

	dp = strchr(p, '.');
	intdigits = (dp != NULL) ? (int) (dp - p) : (int) strlen(p);

	/* Leading zeros count as digits for grouping purposes. */
	for (i = 0; i < intdigits; i++)
	{
		if (i > 0 && (intdigits - i) % 3 == 0)
			appendStringInfoChar(&res, ',');
		appendStringInfoChar(&res, p[i]);
	}

	if (intdigits == 0)
		appendStringInfoChar(&res, '0');

	if (dp != NULL)
		appendStringInfoString(&res, dp);

	PG_RETURN_TEXT_P(cstring_to_text(res.data));
}

/*
 * Shared implementation for BIN()/OCT(): render the 64-bit argument in
 * the given base.  Negative values follow MySQL's two's-complement
 * rendering of unsigned 64-bit integers.
 */
static text *
mysql_radix(uint64 value, int base)
{
	char		buf[64 + 1];
	int			pos = 64;

	buf[64] = '\0';
	if (value == 0)
		return cstring_to_text("0");

	while (value > 0)
	{
		int			digit = (int) (value % (uint64) base);

		buf[--pos] = (digit < 10) ? ('0' + digit) : ('a' + digit - 10);
		value /= (uint64) base;
	}

	return cstring_to_text(buf + pos);
}

Datum
mysql_bin(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(mysql_radix((uint64) PG_GETARG_INT64(0), 2));
}

Datum
mysql_oct(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(mysql_radix((uint64) PG_GETARG_INT64(0), 8));
}
