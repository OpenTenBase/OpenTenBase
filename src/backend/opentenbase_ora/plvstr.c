/*
  This code implements one part of functonality of
  free available library PL/Vision. Please look www.quest.com

  Original author: Steven Feuerstein, 1996 - 2002
  PostgreSQL implementation author: Pavel Stehule, 2006

  This module is under BSD Licence

  History:
    1.0. first public version 13. March 2006
*/


#include "postgres.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "string.h"
#include "stdlib.h"
#include "utils/pg_locale.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "opentenbase_ora/opentenbase_ora.h"

#define PARAMETER_ERROR(detail) \
	ereport(ERROR, \
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
		 errmsg("invalid parameter"), \
		 errdetail(detail)));

#ifndef _pg_mblen
#define _pg_mblen	pg_mblen
#endif

typedef enum
{
	POSITION,
	FIRST,
	LAST
}  position_mode;

static int orcl_instr(text *txt, text *pattern, int start, int nth, bool in_byte);

/*
 * Make substring, can handle negative start
 *
 */
static int
orcl_mb_strlen(text *str, char **sizes, int **positions)
{
	int r_len;
	int cur_size = 0;
	int sz;
	char *p;
	int cur = 0;

	p = VARDATA_ANY(str);
	r_len = VARSIZE_ANY_EXHDR(str);

	if (NULL != sizes)
		*sizes = palloc(r_len * sizeof(char));
	if (NULL != positions)
		*positions = palloc(r_len * sizeof(int));

	while (cur < r_len)
	{
		sz = _pg_mblen(p);
		if (sizes)
			(*sizes)[cur_size] = sz;
		if (positions)
			(*positions)[cur_size] = cur;
		cur += sz;
		p += sz;
		cur_size += 1;
	}

	return cur_size;
}

/* simply search algorhitm - can be better */

static int
orcl_instr_mb(text *txt, text *pattern, int start, int nth)
{
	int			c_len_txt, c_len_pat;
	int			b_len_pat;
	int		   *pos_txt;
	const char *str_txt, *str_pat;
	int			beg, end, i, dx;

	str_txt = VARDATA_ANY(txt);
	c_len_txt = orcl_mb_strlen(txt, NULL, &pos_txt);
	str_pat = VARDATA_ANY(pattern);
	b_len_pat = VARSIZE_ANY_EXHDR(pattern);
	c_len_pat = pg_mbstrlen_with_len(str_pat, b_len_pat);

	if (start > 0)
	{
		dx = 1;
		beg = start - 1;
		end = c_len_txt - c_len_pat + 1;
		if (beg >= end)
			return 0;	/* out of range */
	}
	else if (start == 0)
	{
		return 0;
	}
	else
	{
		dx = -1;
		beg = Min(c_len_txt + start, c_len_txt - c_len_pat);
		end = -1;
		if (beg <= end)
			return 0;	/* out of range */
	}

	for (i = beg; i != end; i += dx)
	{
		if (memcmp(str_txt + pos_txt[i], str_pat, b_len_pat) == 0)
		{
			if (--nth == 0)
				return i + 1;
		}
	}

	return 0;
}

static int
orcl_instr(text *txt, text *pattern, int start, int nth, bool in_byte)
{
	int			len_txt, len_pat;
	const char *str_txt, *str_pat;
	int			beg, end, i, dx;

	if (nth <= 0)
		PARAMETER_ERROR("Four parameter isn't positive.");

	/* Forward for multibyte strings */
	if (!in_byte && pg_database_encoding_max_length() > 1)
		return orcl_instr_mb(txt, pattern, start, nth);

	str_txt = VARDATA_ANY(txt);
	len_txt = VARSIZE_ANY_EXHDR(txt);
	str_pat = VARDATA_ANY(pattern);
	len_pat = VARSIZE_ANY_EXHDR(pattern);

	if (start > 0)
	{
		dx = 1;
		beg = start - 1;
		end = len_txt - len_pat + 1;
		if (beg >= end)
			return 0;	/* out of range */
	}
	else if (start == 0)
	{
		return 0;
	}
	else
	{
		dx = -1;
		beg = Min(len_txt + start, len_txt - len_pat);
		end = -1;
		if (beg <= end)
			return 0;	/* out of range */
	}

	for (i = beg; i != end; i += dx)
	{
		if (memcmp(str_txt + i, str_pat, len_pat) == 0)
		{
			if (--nth == 0)
				return i + 1;
		}
	}

	return 0;
}

/****************************************************************
 * opentenbase_ora.instr
 *
 * Syntax:
 *   FUNCTION opentenbase_ora.instr (string_in VARCHAR, pattern VARCHAR)
 *   FUNCTION opentenbase_ora.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER)
 *   FUNCTION opentenbase_ora.instr (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER, nth INTEGER)
 *            RETURN INT;
 *
 * Purpouse:
 *   Search pattern in string.
 *
 ****************************************************************/
Datum
orcl_instr2(PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
	PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

	PG_RETURN_INT32(orcl_instr(arg1, arg2, 1, 1, false));
}

Datum
orcl_instr3(PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);
	int arg3 = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													PG_GETARG_DATUM(2),
													Int32GetDatum(0))));

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
	PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

	PG_RETURN_INT32(orcl_instr(arg1, arg2, arg3, 1, false));
}

Datum
orcl_instr4(PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);
	int arg3 = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													PG_GETARG_DATUM(2),
													Int32GetDatum(0))));
	int arg4 = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													PG_GETARG_DATUM(3),
													Int32GetDatum(0))));

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
	PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

	PG_RETURN_INT32(orcl_instr(arg1, arg2, arg3, arg4, false));
}


/****************************************************************
 * instrb
 *
 * Syntax:
 *   FUNCTION instrb (string_in VARCHAR, pattern VARCHAR)
 *   FUNCTION instrb (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER)
 *   FUNCTION instrb (string_in VARCHAR, pattern VARCHAR,
 *            start_in INTEGER, nth INTEGER)
 *            RETURN INT;
 *
 * Purpouse:
 *   Search pattern in string, use bytes instead of characters.
 *
 ****************************************************************/
Datum
orcl_instrb2 (PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);

	PG_RETURN_INT32(orcl_instr(arg1, arg2, 1, 1, true));
}

Datum
orcl_instrb3 (PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);
	int arg3 = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													PG_GETARG_DATUM(2),
													Int32GetDatum(0))));

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
	PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

	PG_RETURN_INT32(orcl_instr(arg1, arg2, arg3, 1, true));
}

Datum
orcl_instrb4 (PG_FUNCTION_ARGS)
{
	text *arg1 = PG_GETARG_TEXT_PP(0);
	text *arg2 = PG_GETARG_TEXT_PP(1);
	int arg3 = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													PG_GETARG_DATUM(2),
													Int32GetDatum(0))));
	int arg4 = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													PG_GETARG_DATUM(3),
													Int32GetDatum(0))));

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg1);
	PG_RETURN_NULL_IF_EMPTY_TEXT(arg2);

	PG_RETURN_INT32(orcl_instr(arg1, arg2, arg3, arg4, true));
}
