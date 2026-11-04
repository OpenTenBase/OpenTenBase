/*-------------------------------------------------------------------------
 *
 * mysql_math.c
 *	  MySQL-compatible math functions.
 *
 * Functions: TRUNCATE, FORMAT
 *
 * Copyright (c) 2026, OpenTenBase Contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include <math.h>

/* Forward declarations */
Datum mysql_truncate_num(PG_FUNCTION_ARGS);
Datum mysql_format_num(PG_FUNCTION_ARGS);


/*
 * TRUNCATE(X, D)
 * Returns X truncated to D decimal places.
 * MySQL behavior: if D is 0, returns integer part only.
 * If D is negative, zeros to the left of the decimal point.
 */
PG_FUNCTION_INFO_V1(mysql_truncate_num);
Datum
mysql_truncate_num(PG_FUNCTION_ARGS)
{
	Numeric		x;
	int32		d;
	Numeric		result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	x = PG_GETARG_NUMERIC(0);
	d = PG_GETARG_INT32(1);

	/* Use PostgreSQL's trunc() which does the same thing */
	result = DatumGetNumeric(DirectFunctionCall2(numeric_trunc,
												 NumericGetDatum(x),
												 Int32GetDatum(d)));
	PG_RETURN_NUMERIC(result);
}


/*
 * FORMAT(X, D[, locale])
 * Formats X to a string with D decimal places and thousands separators.
 * MySQL behavior: uses commas for thousands, '.' for decimal (en_US style).
 * The locale parameter is accepted for compatibility but only en_US is supported.
 */
PG_FUNCTION_INFO_V1(mysql_format_num);
Datum
mysql_format_num(PG_FUNCTION_ARGS)
{
	Numeric		x;
	int32		d;
	char	   *num_str;
	char	   *result_str;
	int			len;
	int			decimal_pos;
	int			int_part_len;
	int			result_len;
	int			i, j;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	x = PG_GETARG_NUMERIC(0);
	d = PG_GETARG_INT32(1);

	/* Use numeric to get decimal string */
	{
		Numeric		rounded;

		rounded = DatumGetNumeric(DirectFunctionCall2(numeric_round,
													  NumericGetDatum(x),
													  Int32GetDatum(d)));
		num_str = DatumGetCString(DirectFunctionCall1(numeric_out,
													  NumericGetDatum(rounded)));
	}

	len = strlen(num_str);

	/* Find decimal point */
	decimal_pos = 0;
	for (i = 0; i < len; i++)
	{
		if (num_str[i] == '.')
		{
			decimal_pos = i;
			break;
		}
	}
	if (decimal_pos == 0)
		decimal_pos = len;

	/* Count sign */
	int sign_len = (num_str[0] == '-') ? 1 : 0;
	int_part_len = decimal_pos - sign_len;

	/* Calculate result length: int_part + commas + decimal part + null terminator */
	result_len = int_part_len + (int_part_len - 1) / 3 + (len - decimal_pos) + sign_len + 1;
	if (int_part_len == 0)
		result_len++; /* for '0' before decimal */

	result_str = palloc(result_len);
	j = 0;

	/* Copy sign */
	if (num_str[0] == '-')
	{
		result_str[j++] = '-';
		i = 1;
	}
	else
		i = 0;

	/* Format integer part with commas */
	if (int_part_len == 0)
	{
		result_str[j++] = '0';
	}
	else
	{
		int first_group = int_part_len % 3;
		if (first_group == 0) first_group = 3;

		memcpy(result_str + j, num_str + sign_len, first_group);
		j += first_group;

		for (int g = first_group; g < int_part_len; g += 3)
		{
			result_str[j++] = ',';
			memcpy(result_str + j, num_str + sign_len + g, 3);
			j += 3;
		}
	}

	/* Copy decimal part */
	if (decimal_pos < len)
	{
		/* Pad with zeros if needed */
		result_str[j++] = '.';
		int dec_len = len - decimal_pos - 1;
		memcpy(result_str + j, num_str + decimal_pos + 1, dec_len);
		j += dec_len;
		while (dec_len < d)
		{
			result_str[j++] = '0';
			dec_len++;
		}
	}
	else if (d > 0)
	{
		result_str[j++] = '.';
		for (int k = 0; k < d; k++)
			result_str[j++] = '0';
	}

	result_str[j] = '\0';

	pfree(num_str);
	PG_RETURN_TEXT_P(cstring_to_text(result_str));
}
