/*-------------------------------------------------------------------------
 * opentenbase_ora_compat.c
 *	opentenbase_ora compatible functions.
 *
 * Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 *	Author: Edmund Mergl <E.Mergl@bawue.de>
 *	Multibyte enhancement: Tatsuo Ishii <ishii@postgresql.org>
 *
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/opentenbase_ora_compat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/int.h"
#include "opentenbase_ora/opentenbase_ora.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/numeric.h"
#include "mb/pg_wchar.h"
#ifdef _PG_ORCL_
#include "utils/guc.h"
#endif

static text *dotrim(const char *string, int stringlen,
	   const char *set, int setlen,
	   bool doltrim, bool dortrim);


/********************************************************************
 *
 * lower
 *
 * Syntax:
 *
 *	 text lower(text string)
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to lowercase.
 *
 ********************************************************************/

Datum
lower(PG_FUNCTION_ARGS)
{
	text	   *in_string = PG_GETARG_TEXT_PP(0);
	char	   *out_string;
	text	   *result;

	out_string = str_tolower(VARDATA_ANY(in_string),
							 VARSIZE_ANY_EXHDR(in_string),
							 PG_GET_COLLATION());
	result = cstring_to_text(out_string);
	pfree(out_string);

	PG_RETURN_TEXT_P(result);
}


/********************************************************************
 *
 * upper
 *
 * Syntax:
 *
 *	 text upper(text string)
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to uppercase.
 *
 ********************************************************************/

Datum
upper(PG_FUNCTION_ARGS)
{
	text	   *in_string = PG_GETARG_TEXT_PP(0);
	char	   *out_string;
	text	   *result;

	out_string = str_toupper(VARDATA_ANY(in_string),
							 VARSIZE_ANY_EXHDR(in_string),
							 PG_GET_COLLATION());
	result = cstring_to_text(out_string);
	pfree(out_string);

	PG_RETURN_TEXT_P(result);
}


/********************************************************************
 *
 * initcap
 *
 * Syntax:
 *
 *	 text initcap(text string)
 *
 * Purpose:
 *
 *	 Returns string, with first letter of each word in uppercase, all
 *	 other letters in lowercase. A word is defined as a sequence of
 *	 alphanumeric characters, delimited by non-alphanumeric
 *	 characters.
 *
 ********************************************************************/

Datum
initcap(PG_FUNCTION_ARGS)
{
	text	   *in_string = PG_GETARG_TEXT_PP(0);
	char	   *out_string;
	text	   *result;

	out_string = str_initcap(VARDATA_ANY(in_string),
							 VARSIZE_ANY_EXHDR(in_string),
							 PG_GET_COLLATION());
	result = cstring_to_text(out_string);
	pfree(out_string);

	PG_RETURN_TEXT_P(result);
}


/********************************************************************
 *
 * lpad
 *
 * Syntax:
 *
 *	 text lpad(text string1, int4 len, text string2)
 *
 * Purpose:
 *
 *	 Returns string1, left-padded to length len with the sequence of
 *	 characters in string2.  If len is less than the length of string1,
 *	 instead truncate (on the right) to len.
 *
 ********************************************************************/

Datum
lpad(PG_FUNCTION_ARGS)
{
	text	   *string1;
	int32		len;
	text	   *string2;
	text	   *ret;
	char	   *ptr1,
			   *ptr2,
			   *ptr2start,
			   *ptr2end,
			   *ptr_ret;
	int			m,
				s1len,
				s2len;

	int			bytelen;

	/* In PG, arg2 means number of characters, while means display length in opentenbase_ora. */
	if (ORA_MODE)
	{
		int32 output_width = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											DirectFunctionCall2(numeric_trunc,
																PG_GETARG_DATUM(1),
																Int32GetDatum(0))));
		/* while display length is 0, return null. */
		if (output_width <= 0)
			PG_RETURN_NULL();
		else
			return DirectFunctionCall3(orcl_lpad,
									   PG_GETARG_DATUM(0),
									   Int32GetDatum(output_width),
									   PG_GETARG_DATUM(2));
	}

	string1 = PG_GETARG_TEXT_PP(0);
	len = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											DirectFunctionCall2(numeric_trunc,
																PG_GETARG_DATUM(1),
																Int32GetDatum(0))));
	string2 = PG_GETARG_TEXT_PP(2);

	/* Negative len is silently taken as zero */
	if (len < 0)
		len = 0;

	s1len = VARSIZE_ANY_EXHDR(string1);
	if (s1len < 0)
		s1len = 0;				/* shouldn't happen */

	s2len = VARSIZE_ANY_EXHDR(string2);
	if (s2len < 0)
		s2len = 0;				/* shouldn't happen */

	s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), s1len);

	if (s1len > len)
		s1len = len;			/* truncate string1 to len chars */

	if (s2len <= 0)
		len = s1len;			/* nothing to pad with, so don't pad */

	bytelen = pg_database_encoding_max_length() * len;

	/* check for integer overflow */
	if (len != 0 && bytelen / pg_database_encoding_max_length() != len)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	ret = (text *) palloc(VARHDRSZ + bytelen);

	m = len - s1len;

	ptr2 = ptr2start = VARDATA_ANY(string2);
	ptr2end = ptr2 + s2len;
	ptr_ret = VARDATA(ret);

	while (m--)
	{
		int			mlen = pg_mblen(ptr2);

		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;
		if (ptr2 == ptr2end)	/* wrap around at end of s2 */
			ptr2 = ptr2start;
	}

	ptr1 = VARDATA_ANY(string1);

	while (s1len--)
	{
		int			mlen = pg_mblen(ptr1);

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;
	}

	SET_VARSIZE(ret, ptr_ret - (char *) ret);

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}


/********************************************************************
 *
 * rpad
 *
 * Syntax:
 *
 *	 text rpad(text string1, int4 len, text string2)
 *
 * Purpose:
 *
 *	 Returns string1, right-padded to length len with the sequence of
 *	 characters in string2.  If len is less than the length of string1,
 *	 instead truncate (on the right) to len.
 *
 ********************************************************************/

Datum
rpad(PG_FUNCTION_ARGS)
{
	text	   *string1;
	int32		len;
	text	   *string2;
	text	   *ret;
	char	   *ptr1,
			   *ptr2,
			   *ptr2start,
			   *ptr2end,
			   *ptr_ret;
	int			m,
				s1len,
				s2len;

	int			bytelen;

	/* In PG, arg2 means number of characters, while means display length in opentenbase_ora. */
	if (ORA_MODE)
	{
		int32 output_width = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											DirectFunctionCall2(numeric_trunc,
																PG_GETARG_DATUM(1),
																Int32GetDatum(0))));
		/* while display length is 0, return null. */
		if (output_width <= 0)
			PG_RETURN_NULL();
		else
			return DirectFunctionCall3(orcl_rpad,
									   PG_GETARG_DATUM(0),
									   Int32GetDatum(output_width),
									   PG_GETARG_DATUM(2));
	}

	string1 = PG_GETARG_TEXT_PP(0);
	len = DatumGetInt32(DirectFunctionCall1(numeric_int4,
											DirectFunctionCall2(numeric_trunc,
																PG_GETARG_DATUM(1),
																Int32GetDatum(0))));
	string2 = PG_GETARG_TEXT_PP(2);

	/* Negative len is silently taken as zero */
	if (len < 0)
		len = 0;

	s1len = VARSIZE_ANY_EXHDR(string1);
	if (s1len < 0)
		s1len = 0;				/* shouldn't happen */

	s2len = VARSIZE_ANY_EXHDR(string2);
	if (s2len < 0)
		s2len = 0;				/* shouldn't happen */

	s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), s1len);

	if (s1len > len)
		s1len = len;			/* truncate string1 to len chars */

	if (s2len <= 0)
		len = s1len;			/* nothing to pad with, so don't pad */

	bytelen = pg_database_encoding_max_length() * len;

	/* Check for integer overflow */
	if (len != 0 && bytelen / pg_database_encoding_max_length() != len)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	ret = (text *) palloc(VARHDRSZ + bytelen);
	m = len - s1len;

	ptr1 = VARDATA_ANY(string1);
	ptr_ret = VARDATA(ret);

	while (s1len--)
	{
		int			mlen = pg_mblen(ptr1);

		memcpy(ptr_ret, ptr1, mlen);
		ptr_ret += mlen;
		ptr1 += mlen;
	}

	ptr2 = ptr2start = VARDATA_ANY(string2);
	ptr2end = ptr2 + s2len;

	while (m--)
	{
		int			mlen = pg_mblen(ptr2);

		memcpy(ptr_ret, ptr2, mlen);
		ptr_ret += mlen;
		ptr2 += mlen;
		if (ptr2 == ptr2end)	/* wrap around at end of s2 */
			ptr2 = ptr2start;
	}

	SET_VARSIZE(ret, ptr_ret - (char *) ret);

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * btrim3
 *
 * Syntax:
 *
 *	 text btrim(text string, text set, text strict_check)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 ********************************************************************/

Datum
btrim3(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *set = PG_GETARG_TEXT_PP(1);
	text	   *ora_strict_check = PG_GETARG_TEXT_PP(2);
	text	   *ret;

	/* check set num, no more than 1 */
	if (ORA_MODE && ora_strict_check &&
		(*(char *)VARDATA_ANY(ora_strict_check) == 't') &&
		(pg_mbstrlen_with_len(VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set)) > 1))
	{
		ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("trim set should have only one character")));
	}

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set),
				 true, true);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * btrim
 *
 * Syntax:
 *
 *	 text btrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 ********************************************************************/

Datum
btrim(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *set = PG_GETARG_TEXT_PP(1);
	text	   *ret;

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set),
				 true, true);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * btrim1 --- btrim with set fixed as ' '
 *
 ********************************************************************/

Datum
btrim1(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *ret;

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 " ", 1,
				 true, true);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}

/*
 * Common implementation for btrim, ltrim, rtrim
 */
static text *
dotrim(const char *string, int stringlen,
	   const char *set, int setlen,
	   bool doltrim, bool dortrim)
{
	int			i;

	/* Nothing to do if either string or set is empty */
	if (stringlen > 0 && setlen > 0)
	{
		if (pg_database_encoding_max_length() > 1)
		{
			/*
			 * In the multibyte-encoding case, build arrays of pointers to
			 * character starts, so that we can avoid inefficient checks in
			 * the inner loops.
			 */
			const char **stringchars;
			const char **setchars;
			int		   *stringmblen;
			int		   *setmblen;
			int			stringnchars;
			int			setnchars;
			int			resultndx;
			int			resultnchars;
			const char *p;
			int			len;
			int			mblen;
			const char *str_pos;
			int			str_len;

			stringchars = (const char **) palloc(stringlen * sizeof(char *));
			stringmblen = (int *) palloc(stringlen * sizeof(int));
			stringnchars = 0;
			p = string;
			len = stringlen;
			while (len > 0)
			{
				stringchars[stringnchars] = p;
				stringmblen[stringnchars] = mblen = pg_mblen(p);
				stringnchars++;
				p += mblen;
				len -= mblen;
			}

			setchars = (const char **) palloc(setlen * sizeof(char *));
			setmblen = (int *) palloc(setlen * sizeof(int));
			setnchars = 0;
			p = set;
			len = setlen;
			while (len > 0)
			{
				setchars[setnchars] = p;
				setmblen[setnchars] = mblen = pg_mblen(p);
				setnchars++;
				p += mblen;
				len -= mblen;
			}

			resultndx = 0;		/* index in stringchars[] */
			resultnchars = stringnchars;

			if (doltrim)
			{
				while (resultnchars > 0)
				{
					str_pos = stringchars[resultndx];
					str_len = stringmblen[resultndx];
					for (i = 0; i < setnchars; i++)
					{
						if (str_len == setmblen[i] &&
							memcmp(str_pos, setchars[i], str_len) == 0)
							break;
					}
					if (i >= setnchars)
						break;	/* no match here */
					string += str_len;
					stringlen -= str_len;
					resultndx++;
					resultnchars--;
				}
			}

			if (dortrim)
			{
				while (resultnchars > 0)
				{
					str_pos = stringchars[resultndx + resultnchars - 1];
					str_len = stringmblen[resultndx + resultnchars - 1];
					for (i = 0; i < setnchars; i++)
					{
						if (str_len == setmblen[i] &&
							memcmp(str_pos, setchars[i], str_len) == 0)
							break;
					}
					if (i >= setnchars)
						break;	/* no match here */
					stringlen -= str_len;
					resultnchars--;
				}
			}

			pfree(stringchars);
			pfree(stringmblen);
			pfree(setchars);
			pfree(setmblen);
		}
		else
		{
			/*
			 * In the single-byte-encoding case, we don't need such overhead.
			 */
			if (doltrim)
			{
				while (stringlen > 0)
				{
					char		str_ch = *string;

					for (i = 0; i < setlen; i++)
					{
						if (str_ch == set[i])
							break;
					}
					if (i >= setlen)
						break;	/* no match here */
					string++;
					stringlen--;
				}
			}

			if (dortrim)
			{
				while (stringlen > 0)
				{
					char		str_ch = string[stringlen - 1];

					for (i = 0; i < setlen; i++)
					{
						if (str_ch == set[i])
							break;
					}
					if (i >= setlen)
						break;	/* no match here */
					stringlen--;
				}
			}
		}
	}

	if (ORA_MODE && stringlen == 0)
	{
		return NULL;
	}

	/* Return selected portion of string */
	return cstring_to_text_with_len(string, stringlen);
}

/********************************************************************
 *
 * byteatrim
 *
 * Syntax:
 *
 *	 bytea byteatrim(byta string, bytea set)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 * Cloned from btrim and modified as required.
 ********************************************************************/

Datum
byteatrim(PG_FUNCTION_ARGS)
{
	bytea	   *string = PG_GETARG_BYTEA_PP(0);
	bytea	   *set = PG_GETARG_BYTEA_PP(1);
	bytea	   *ret;
	char	   *ptr,
			   *end,
			   *ptr2,
			   *ptr2start,
			   *end2;
	int			m,
				stringlen,
				setlen;

	stringlen = VARSIZE_ANY_EXHDR(string);
	setlen = VARSIZE_ANY_EXHDR(set);

	if (stringlen <= 0 || setlen <= 0)
		PG_RETURN_BYTEA_P(string);

	m = stringlen;
	ptr = VARDATA_ANY(string);
	end = ptr + stringlen - 1;
	ptr2start = VARDATA_ANY(set);
	end2 = ptr2start + setlen - 1;

	while (m > 0)
	{
		ptr2 = ptr2start;
		while (ptr2 <= end2)
		{
			if (*ptr == *ptr2)
				break;
			++ptr2;
		}
		if (ptr2 > end2)
			break;
		ptr++;
		m--;
	}

	while (m > 0)
	{
		ptr2 = ptr2start;
		while (ptr2 <= end2)
		{
			if (*end == *ptr2)
				break;
			++ptr2;
		}
		if (ptr2 > end2)
			break;
		end--;
		m--;
	}

	ret = (bytea *) palloc(VARHDRSZ + m);
	SET_VARSIZE(ret, VARHDRSZ + m);
	memcpy(VARDATA(ret), ptr, m);

	PG_RETURN_BYTEA_P(ret);
}

/********************************************************************
 *
 * ltrim3
 *
 * Syntax:
 *
 *	 text ltrim(text string, text set, text strict_check)
 *
 * Purpose:
 *
 *	 Returns string with initial characters removed up to the first
 *	 character not in set.
 *
 ********************************************************************/

Datum
ltrim3(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *set = PG_GETARG_TEXT_PP(1);
	text	   *ora_strict_check = PG_GETARG_TEXT_PP(2);
	text	   *ret;

	/* check set num, no more than 1 */
	if (ORA_MODE && ora_strict_check &&
		(*(char *)VARDATA_ANY(ora_strict_check) == 't') &&
		(pg_mbstrlen_with_len(VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set)) > 1))
	{
		ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("trim set should have only one character")));
	}

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set),
				 true, false);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * ltrim
 *
 * Syntax:
 *
 *	 text ltrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with initial characters removed up to the first
 *	 character not in set.
 *
 ********************************************************************/

Datum
ltrim(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *set = PG_GETARG_TEXT_PP(1);
	text	   *ret;

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set),
				 true, false);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * ltrim1 --- ltrim with set fixed as ' '
 *
 ********************************************************************/

Datum
ltrim1(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *ret;

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 " ", 1,
				 true, false);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * rtrim3
 *
 * Syntax:
 *
 *	 text rtrim(text string, text set, text strict_check)
 *
 * Purpose:
 *
 *	 Returns string with final characters removed after the last
 *	 character not in set.
 *
 ********************************************************************/

Datum
rtrim3(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *set = PG_GETARG_TEXT_PP(1);
	text	   *ora_strict_check = PG_GETARG_TEXT_PP(2);
	text	   *ret;

	/* check set num, no more than 1 */
	if (ORA_MODE && ora_strict_check &&
		(*(char *)VARDATA_ANY(ora_strict_check) == 't') &&
		(pg_mbstrlen_with_len(VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set)) > 1))
	{
		ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("trim set should have only one character")));
	}

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set),
				 false, true);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * rtrim
 *
 * Syntax:
 *
 *	 text rtrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with final characters removed after the last
 *	 character not in set.
 *
 ********************************************************************/

Datum
rtrim(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *set = PG_GETARG_TEXT_PP(1);
	text	   *ret;

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set),
				 false, true);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * rtrim1 --- rtrim with set fixed as ' '
 *
 ********************************************************************/

Datum
rtrim1(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *ret;

	ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string),
				 " ", 1,
				 false, true);
	if (ret == NULL)
	{
		PG_RETURN_NULL();
	}

	if (ORA_MODE &&
		pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(ret), VARSIZE_ANY_EXHDR(ret)) == 0)
	{
		pfree(ret);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(ret);
}


/********************************************************************
 *
 * translate
 *
 * Syntax:
 *
 *	 text translate(text string, text from, text to)
 *
 * Purpose:
 *
 *	 Returns string after replacing all occurrences of characters in from
 *	 with the corresponding character in to.  If from is longer than to,
 *	 occurrences of the extra characters in from are deleted.
 *	 Improved by Edwin Ramirez <ramirez@doc.mssm.edu>.
 *
 ********************************************************************/

Datum
translate(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	text	   *from = PG_GETARG_TEXT_PP(1);
	text	   *to = PG_GETARG_TEXT_PP(2);
	text	   *result;
	char	   *from_ptr,
			   *to_ptr;
	char	   *source,
			   *target;
	int			m,
				fromlen,
				tolen,
				retlen,
				i;
	int			worst_len;
	int			len;
	int			source_len;
	int			from_index;

	m = VARSIZE_ANY_EXHDR(string);
	if (m <= 0)
		PG_RETURN_TEXT_P(string);
	source = VARDATA_ANY(string);

	fromlen = VARSIZE_ANY_EXHDR(from);
	from_ptr = VARDATA_ANY(from);
	tolen = VARSIZE_ANY_EXHDR(to);
	to_ptr = VARDATA_ANY(to);

	/*
	 * The worst-case expansion is to substitute a max-length character for a
	 * single-byte character at each position of the string.
	 */
	worst_len = pg_database_encoding_max_length() * m;

	/* check for integer overflow */
	if (worst_len / pg_database_encoding_max_length() != m)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	result = (text *) palloc(worst_len + VARHDRSZ);
	target = VARDATA(result);
	retlen = 0;

	while (m > 0)
	{
		source_len = pg_mblen(source);
		from_index = 0;

		for (i = 0; i < fromlen; i += len)
		{
			len = pg_mblen(&from_ptr[i]);
			if (len == source_len &&
				memcmp(source, &from_ptr[i], len) == 0)
				break;

			from_index++;
		}
		if (i < fromlen)
		{
			/* substitute */
			char	   *p = to_ptr;

			for (i = 0; i < from_index; i++)
			{
				p += pg_mblen(p);
				if (p >= (to_ptr + tolen))
					break;
			}
			if (p < (to_ptr + tolen))
			{
				len = pg_mblen(p);
				memcpy(target, p, len);
				target += len;
				retlen += len;
			}

		}
		else
		{
			/* no match, so copy */
			memcpy(target, source, source_len);
			target += source_len;
			retlen += source_len;
		}

		source += source_len;
		m -= source_len;
	}

	if (0 == retlen && (enable_lightweight_ora_syntax || ORA_MODE))
		PG_RETURN_NULL();
	else
	{
		SET_VARSIZE(result, retlen + VARHDRSZ);
		/*
		 * The function result is probably much bigger than needed, if we're using
		 * a multibyte encoding, but it's not worth reallocating it; the result
		 * probably won't live long anyway.
		 */

		PG_RETURN_TEXT_P(result);
	}
}

/********************************************************************
 *
 * ascii
 *
 * Syntax:
 *
 *	 int ascii(text string)
 *
 * Purpose:
 *
 *	 Returns the decimal representation of the first character from
 *	 string.
 *	 If the string is empty we return 0.
 *	 If the database encoding is UTF8, we return the Unicode codepoint.
 *	 If the database encoding is any other multi-byte encoding, we
 *	 return the value of the first byte if it is an ASCII character
 *	 (range 1 .. 127), or raise an error.
 *	 For all other encodings we return the value of the first byte,
 *	 (range 1..255).
 *
 ********************************************************************/

Datum
ascii(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	int			encoding = GetDatabaseEncoding();
	unsigned char *data;

	if (VARSIZE_ANY_EXHDR(string) <= 0)
		PG_RETURN_INT32(0);

	data = (unsigned char *) VARDATA_ANY(string);

	if (encoding == PG_UTF8 && *data > 127)
	{
		/* return the code point for Unicode */

		int			result = 0,
					tbytes = 0,
					i;

		if (*data >= 0xF0)
		{
			result = *data & 0x07;
			tbytes = 3;
		}
		else if (*data >= 0xE0)
		{
			result = *data & 0x0F;
			tbytes = 2;
		}
		else
		{
			/* TODO a bug Assert(*data > 0xC0) */
			result = *data & 0x1f;
			tbytes = 1;
		}

		Assert(tbytes > 0);

		for (i = 1; i <= tbytes; i++)
		{
			/* TODO a bug Assert((data[i] & 0xC0) == 0x80); */
			result = (result << 6) + (data[i] & 0x3f);
		}

		PG_RETURN_INT32(result);
	}
	else
	{
		if (pg_encoding_max_length(encoding) > 1 && *data > 127)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character too large")));


		PG_RETURN_INT32((int32) *data);
	}
}

/********************************************************************
 *
 * ascii
 *
 * Syntax:
 *
 *	 int ascii(rowid id)
 *
 * Purpose:
 *
 *	Return the ascii code for the rowid.
 *
 *	eg:	ascii(rowid) = ascii(1998) = ascii(1) = 49
 *
 ********************************************************************/
Datum
ascii_rowid(PG_FUNCTION_ARGS)
{
	Datum arg = PG_GETARG_DATUM(0);
	Datum text_arg;

	Assert(!PG_ARGISNULL(0));
	Assert(arg != 0);

	text_arg = DirectFunctionCall1(rowid_to_text, arg);
	return DirectFunctionCall1(ascii, text_arg);
}

/********************************************************************
 *
 * ascii
 *
 * Syntax:
 *
 *	 int ascii(numeric num)
 *
 * Purpose:
 *
 *	Return the ascii code for the first number of the param.
 *
 *	eg:	ascii(1998) = ascii(1) = 49
 *		ascii(1.9) = ascii(1) = 49
 *
 *	special(opentenbase_ora compatible):
 * 		ascii(.9) = ascii(0.9) = ascii('.') = 46 not ascii(0.9)= ascii('0')
 *
 ********************************************************************/
Datum
ascii_numeric(PG_FUNCTION_ARGS)
{
	Numeric num = PG_GETARG_NUMERIC(0);
	uint16 numFlags = NUMERIC_NB_FLAGBITS(num);
	char *str_num = NULL;

	if (NUMERIC_FLAG_IS_NAN(numFlags))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("got a NaN")));
	}

    if (NUMERIC_IS_INF(num))
    {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("got a Inf or -Inf")));
    }

	if (NUMERIC_FLAG_IS_BI(numFlags))
	{
		/* Handle big integer */
		num = makeNumericNormal(num);
	}

	str_num = numeric_normalize(num);

	if (strlen(str_num) > 2 && '0' == str_num[0]  && '.' == str_num[1])
	{
		PG_RETURN_INT32((int32)'.');
	}

	return DirectFunctionCall1(ascii, CStringGetTextDatum(str_num));
}

static NumericDigit const_num_zero_data[1] = {0};
static NumericVar const_num_zero =
{0, 0, NUMERIC_POS, 0, NULL, const_num_zero_data, NUMERIC_NUMBER};
static NumericDigit const_num_one_data[1] = {1};
static NumericVar const_num_one =
{1, 0, NUMERIC_POS, 0, NULL, const_num_one_data, NUMERIC_NUMBER};
/* 4294967296: sign: NUMERIC_POS, weight: 3, digit: {42, 9496, 7296} */
static NumericDigit const_num_uint32_max_data[3] = {42, 9496, 7296};
static NumericVar const_num_uint32_max =
{3, 2, NUMERIC_POS, 0, NULL, const_num_uint32_max_data, NUMERIC_NUMBER};
/* 2147483647: sign: NUMERIC_POS, weight: 3, digit: {21, 4748, 3647} */
static NumericDigit const_num_int32_max_data[3] = {21, 4748, 3647};
static NumericVar const_num_int32_max =
{3, 2, NUMERIC_POS, 0, NULL, const_num_int32_max_data, NUMERIC_NUMBER};
/* -2147483648: sign: NUMERIC_NEG, weight: 3, digit: {21, 4748, 3648} */
static NumericDigit const_num_int32_min_data[3] = {21, 4748, 3648};
static NumericVar const_num_int32_min =
{3, 2, NUMERIC_NEG, 0, NULL, const_num_int32_min_data, NUMERIC_NUMBER};

static Datum
orcl_chr_impl(Numeric num, bool is_chr)
{
	/*
	 *	chr(n), n ∈ [0, UINT32_MAX]，If it is not within this range, an error will be reported.
	 *	numeric overflow. when 0 < n < 1，opentenbase_ora reportes an error,numeric overflow
	 */
	int64 large_value = 0;
	uint32 value = 0;
	char buf[5] = {0};	/* 5 is MAX_UINT32_CHR_LEN */
	int32 buf_length, i;
	static const int32 ASCII_SPACE = 0x20;

	Numeric	num_zero = make_result(&const_num_zero);
	Numeric	num_one = make_result(&const_num_one);
	Numeric num_max_uint32 = make_result(&const_num_uint32_max);

	if ((cmp_numerics(num, num_zero) > 0 && cmp_numerics(num, num_one) < 0)	||
		(cmp_numerics(num, num_zero) < 0) || (cmp_numerics(num, num_max_uint32) >= 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					errmsg("numeric overflow")));
	}
	large_value = DatumGetInt64(DirectFunctionCall1(numeric_int8,
							DirectFunctionCall2(numeric_trunc,
												NumericGetDatum(num),
												Int32GetDatum(0))));
	/* chr(0) we translate to space ' ' */
	large_value = (large_value == 0) ? ASCII_SPACE : large_value;
	value = (uint32)large_value;
	if (value & 0xFF000000UL)
		buf_length = 4;
	else if (value & 0xFF0000UL)
		buf_length = 3;
	else if (value & 0xFF00UL)
		buf_length = 2;
	else
		buf_length = 1;
	buf[buf_length] = '\0';
	for (i = 0; i < buf_length; ++i)
		buf[buf_length - i - 1] = (char)(value >> (8 * i));
	if (is_chr)
		PG_RETURN_VARCHAR_P(cstring_to_text_with_len(buf, buf_length));

	/*
	 *  If we have done the encoding and decoding of nchr characters,
	 *  We just need to let go of these codes
	 * 
	 *  int32 varchar2_atttypmod = 0;
	 *	data = buf_length > 2 ? (char*)buf + buf_length - 2 : buf;
	 *	buf_length = buf_length > 2 ? 2 : buf_length;
	 *	varchar2_atttypmod = buf_length + VARHDRSZ;
	 *	return DirectFunctionCall3(
	 *	nvarchar2in, PointerGetDatum(data), Int32GetDatum(InvalidOid), Int32GetDatum(varchar2_atttypmod));
	 */

	/*
	 * In order to ensure consistent display, we have transcoded
	 */
	return chr_encoding(value);
}

Datum
orcl_chr(PG_FUNCTION_ARGS)
{
	Numeric	num = PG_GETARG_NUMERIC(0);
	text *t = NULL;
	int32 tlen = 0;
	char *using_nchar_cs = NULL;
	char *ori_nchar_cs = "USING NCHAR_CS";
	int32 len = strlen(ori_nchar_cs);
		
	if (PG_NARGS() == 1) /* chr */
		return orcl_chr_impl(num, true);

	/* nchr */
	t = PG_GETARG_TEXT_PP(1);
	using_nchar_cs = (char*)text_to_cstring(t);
	tlen = VARSIZE_ANY_EXHDR(t);

	if ((tlen == len) && (strncmp(using_nchar_cs, ori_nchar_cs, tlen) == 0))
		return orcl_chr_impl(num, false);

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid chr args")));	
}


Datum
chr_encoding(uint32 cvalue)
{
	text	   *result;
	int			encoding = GetDatabaseEncoding();

	if ((encoding == PG_UTF8 || encoding == PG_EUC_CN) && cvalue > 127)
	{
		/* for Unicode we treat the argument as a code point */
		int			bytes;
		unsigned char *wch;

		/*
		 * We only allow valid Unicode code points; per RFC3629 that stops at
		 * U+10FFFF, even though 4-byte UTF8 sequences can hold values up to
		 * U+1FFFFF.
		 */
		if (cvalue > 0x0010ffff)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character too large for encoding: %d",
							cvalue)));

		if (cvalue > 0xffff)
			bytes = 4;
		else if (cvalue > 0x07ff)
			bytes = 3;
		else
			bytes = 2;

		result = (text *) palloc(VARHDRSZ + bytes);
		SET_VARSIZE(result, VARHDRSZ + bytes);
		wch = (unsigned char *) VARDATA(result);

		if (bytes == 2)
		{
			wch[0] = 0xC0 | ((cvalue >> 6) & 0x1F);
			wch[1] = 0x80 | (cvalue & 0x3F);
		}
		else if (bytes == 3)
		{
			wch[0] = 0xE0 | ((cvalue >> 12) & 0x0F);
			wch[1] = 0x80 | ((cvalue >> 6) & 0x3F);
			wch[2] = 0x80 | (cvalue & 0x3F);
		}
		else
		{
			wch[0] = 0xF0 | ((cvalue >> 18) & 0x07);
			wch[1] = 0x80 | ((cvalue >> 12) & 0x3F);
			wch[2] = 0x80 | ((cvalue >> 6) & 0x3F);
			wch[3] = 0x80 | (cvalue & 0x3F);
		}

		/*
		 * The preceding range check isn't sufficient, because UTF8 excludes
		 * Unicode "surrogate pair" codes.  Make sure what we created is valid
		 * UTF8.
		 */
		if (!pg_utf8_islegal(wch, bytes))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character not valid for encoding: %d",
							cvalue)));
	}
	else
	{
		bool		is_mb;

		/*
		 * Error out on arguments that make no sense or that we can't validly
		 * represent in the encoding.
		 */
		if (cvalue == 0)
		{
			/* if cvalue is 0, we assign it to 0x20 which is ascii code of character ' ' */
			if (ORA_MODE)
				cvalue = 0x20;
			else
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("null character not permitted")));
		}

		is_mb = pg_encoding_max_length(encoding) > 1;

		if ((is_mb && (cvalue > 127)) || (!is_mb && (cvalue > 255)))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("requested character too large for encoding: %d",
							cvalue)));

		result = (text *) palloc(VARHDRSZ + 1);
		SET_VARSIZE(result, VARHDRSZ + 1);
		*VARDATA(result) = (char) cvalue;
	}

	PG_RETURN_TEXT_P(result);
}

/********************************************************************
 *
 * chr
 *
 * Syntax:
 *
 *	 text chr(int val)
 *
 * Purpose:
 *
 *	Returns the character having the binary equivalent to val.
 *
 * For UTF8 we treat the argumwent as a Unicode code point.
 * For other multi-byte encodings we raise an error for arguments
 * outside the strict ASCII range (1..127).
 *
 * It's important that we don't ever return a value that is not valid
 * in the database encoding, so that this doesn't become a way for
 * invalid data to enter the database.
 *
 ********************************************************************/

Datum
chr			(PG_FUNCTION_ARGS)
{
	uint32		cvalue = 0;

	if (ORA_MODE)
	{
		int32 cvalue = PG_GETARG_INT32(0);
		Numeric num = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum((int64)cvalue)));
		return DirectFunctionCall1(orcl_chr, NumericdGetDatum(num));
	}

	cvalue = PG_GETARG_UINT32(0);

	return chr_encoding(cvalue);
}

/********************************************************************
 *
 * orcl_nchr
 *
 * Syntax:
 *
 *	 text orcl_nchr(numeric val)
 *
 * Purpose:
 *
 *	Returns the character having the binary equivalent to val.
 *
 ********************************************************************/
Datum
orcl_nchr(PG_FUNCTION_ARGS)
{
	int cvalue = DatumGetInt32(DirectFunctionCall1(numeric_int4,
							DirectFunctionCall2(numeric_trunc,
												PG_GETARG_DATUM(0),
												Int32GetDatum(0))));
	return chr_encoding(cvalue);
}

/********************************************************************
 *
 * repeat
 *
 * Syntax:
 *
 *	 text repeat(text string, int val)
 *
 * Purpose:
 *
 *	Repeat string by val.
 *
 ********************************************************************/

Datum
repeat(PG_FUNCTION_ARGS)
{
	text	   *string = PG_GETARG_TEXT_PP(0);
	int32		count = PG_GETARG_INT32(1);
	text	   *result;
	int			slen,
				tlen;
	int			i;
	char	   *cp,
			   *sp;

	if (count < 0)
		count = 0;

	slen = VARSIZE_ANY_EXHDR(string);

	if (unlikely(pg_mul_s32_overflow(count, slen, &tlen)) ||
		unlikely(pg_add_s32_overflow(tlen, VARHDRSZ, &tlen)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("requested length too large")));

	result = (text *) palloc(tlen);

	SET_VARSIZE(result, tlen);
	cp = VARDATA(result);
	sp = VARDATA_ANY(string);
	for (i = 0; i < count; i++)
	{
		memcpy(cp, sp, slen);
		cp += slen;
	}

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(result), VARSIZE_ANY_EXHDR(result)) == 0)
	{
		pfree(result);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(result);
}

/*
 * translate the invalid byte to space, for example utf8
 * [{e4, b8, ad}, {e4, b8, ad}, {e4, b8, ad}] before substrb
 *         [{ad}, {e4, b8, ad}, {e4}] 		  after  substrb
 * 		   [{20}, {e4, b8, ad}, {20}]		  ad、e4  =>    0x20
 */
static int
reset_invalid_byte(char *ptr, const int ptr_len,
					int start, int len, char reset_char)
{
	int ret = 0, i;
	int well_formatted_start = start;
	int well_formatted_len = ptr_len - start;
    int boundary_pos = 0;
    int boundary_len = 0;

	if (ptr == NULL || ptr_len <= 0 || start >= ptr_len)
		return ret;

	ret = get_well_formatted_boundary(ptr, ptr_len, start, &boundary_pos, &boundary_len);
	if (0 == ret)
	{
		if (boundary_len >= 0)
		{
			if (start > boundary_pos)
				well_formatted_start = boundary_pos + boundary_len;
			if (well_formatted_start >= ptr_len)
				boundary_len = 0;
			else
			{
				boundary_len = ptr_len - well_formatted_start;
				ret = get_well_formatted_boundary(ptr + well_formatted_start, ptr_len - well_formatted_start,
													len - well_formatted_start + start, &boundary_pos, &boundary_len);
				if (0 == ret)
				{
					if (boundary_len >= 0)
						well_formatted_len = boundary_pos;
				}
			}
		}
	}

	if (0 == ret)
	{
		for (i = start; i < well_formatted_start; i++)
			ptr[i] = reset_char;
		for (i = well_formatted_start + well_formatted_len; i < start + len; i++)
			ptr[i] = reset_char;
	}

	return ret;
}


Datum
orcl_substrb(PG_FUNCTION_ARGS)
{
	text	*str = PG_GETARG_TEXT_PP(0);
	text 	*ret_str = NULL;
	int		ori_len = 0;
	int 	left_len = 0;
	char	*result;
	Numeric npos = PG_GETARG_NUMERIC(1);
	int 	pos = 0;
	Numeric nlen = NULL;
	int 	len = 0;
	Numeric	num_one = make_result(&const_num_one);
	Numeric num_max_int32 = make_result(&const_num_int32_max);
	Numeric num_min_int32 = make_result(&const_num_int32_min);

	ori_len = VARSIZE_ANY_EXHDR(str);
	if (PG_NARGS() == 3)
	{
		nlen = PG_GETARG_NUMERIC(2);
		if (cmp_numerics(nlen, num_one) < 0)
			PG_RETURN_NULL();
		else if (cmp_numerics(nlen, num_max_int32) > 0)
			nlen = num_max_int32;
		len = DatumGetInt32(DirectFunctionCall1(numeric_int4,
							DirectFunctionCall2(numeric_trunc,
												NumericGetDatum(nlen),
												Int32GetDatum(0))));
	}
	else
		len = ori_len;

	if (cmp_numerics(npos, num_max_int32) > 0)
		npos = num_max_int32;
	else if (cmp_numerics(npos, num_min_int32) < 0)
		npos = num_min_int32;
	pos = DatumGetInt32(DirectFunctionCall1(numeric_int4,
								DirectFunctionCall2(numeric_trunc,
													NumericGetDatum(npos),
													Int32GetDatum(0))));
	if (pos < 0)
	{
		if (pos + ori_len < 0)
			PG_RETURN_NULL();
		else
			pos = pos + ori_len + 1;
	}
	else if (pos > ori_len)
		PG_RETURN_NULL();
	if (pos == 0)
		pos = 1;
	left_len = ori_len - pos + 1;
	len = len > left_len ? left_len : len;

	result = (char *) palloc(ori_len + 1);
	result[ori_len] = '\0';
	memcpy(result, (char*)(VARDATA_ANY(str)), ori_len);
	/* opentenbase_ora transforms ilegal charset to space */
	if (0 != reset_invalid_byte(result, ori_len, pos-1, len, ' '))
	{
		ereport(ERROR,
			(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
			 errmsg("invalid byte sequence for encoding \"%s\": %s",
					pg_enc2name_tbl[GetDatabaseEncoding()].name,
					result)));
	}
	ret_str = cstring_to_text_with_len(result + (pos-1), len);
	pfree(result);
	PG_RETURN_TEXT_P(ret_str);
}

/********************************************************************
 *
 * orcl_substrb_3
 *
 * Syntax:
 *
 *	 text orcl_substrb_3(text str, numeric position, numeric length)
 *
 * Purpose:
 *
 *	Returns the sub string.
 *
 ********************************************************************/
Datum
orcl_substrb_3(PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	int position = 0, length = 0;

	if (ORA_MODE)
	{
		Datum  result;
		CHECK_RETNULL_INIT();

		result = CHECK_RETNULL_DIRECTCALL3(orcl_substrb, PointerGetDatum(str), 
									NumericdGetDatum(PG_GETARG_NUMERIC(1)), 
									NumericdGetDatum(PG_GETARG_NUMERIC(2)));
		CHECK_RETNULL_RETURN_DATUM(result);
	}

	position = DatumGetInt32(DirectFunctionCall1(numeric_int4,
							DirectFunctionCall2(numeric_trunc,
												PG_GETARG_DATUM(1),
												Int32GetDatum(0))));
	length = DatumGetInt32(DirectFunctionCall1(numeric_int4,
							DirectFunctionCall2(numeric_trunc,
												PG_GETARG_DATUM(2),
												Int32GetDatum(0))));

	return DirectFunctionCall3(text_substr, PointerGetDatum(str), position, length);
}

/********************************************************************
 *
 * orcl_substrb_2
 *
 * Syntax:
 *
 *	 text orcl_substrb_2(text str, numeric position)
 *
 * Purpose:
 *
 *	Returns the sub string.
 *
 ********************************************************************/
Datum
orcl_substrb_2(PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_PP(0);
	int position = 0;

	if (ORA_MODE)
	{
		Datum  result;
		CHECK_RETNULL_INIT();

		result = CHECK_RETNULL_DIRECTCALL2(orcl_substrb, PointerGetDatum(str),  NumericdGetDatum((PG_GETARG_NUMERIC(1))));
		CHECK_RETNULL_RETURN_DATUM(result);
	}

	position = DatumGetInt32(DirectFunctionCall1(numeric_int4,
						DirectFunctionCall2(numeric_trunc,
											PG_GETARG_DATUM(1),
											Int32GetDatum(0))));

	return DirectFunctionCall2(text_substr_no_len, PointerGetDatum(str), position);
}

Datum
pseudo_proc_pkg(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("must create package body")));
}
