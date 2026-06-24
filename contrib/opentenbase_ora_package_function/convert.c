#include <float.h>
#include <math.h>
#include <limits.h>

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "utils/formatting.h"
#include "utils/bytea.h"
#include "utils/varlena.h"
#include "utils/date.h"

#include "opentenbase_ora.h"
#include "builtins.h"

#define VAL(CH) ((CH) - '0')
#define DIG(VAL) ((VAL) + '0')

PG_FUNCTION_INFO_V1(orafce_to_char_int4);
PG_FUNCTION_INFO_V1(orafce_to_char_int8);
PG_FUNCTION_INFO_V1(orafce_to_char_float4);
PG_FUNCTION_INFO_V1(orafce_to_char_float8);
PG_FUNCTION_INFO_V1(orafce_to_char_numeric);
PG_FUNCTION_INFO_V1(orafce_to_char_timestamp);
PG_FUNCTION_INFO_V1(orafce_to_number);
PG_FUNCTION_INFO_V1(orafce_to_multi_byte);
PG_FUNCTION_INFO_V1(orafce_to_single_byte);

static int getindex(const char **map, char *mbchar, int mblen);

Datum orcl_to_date_with_fmt_nls_internal(PG_FUNCTION_ARGS);
Datum orcl_to_timestamp_with_fmt_nls_internal(PG_FUNCTION_ARGS);
Datum orcl_to_timestamp_tz_with_fmt_nls_internal(PG_FUNCTION_ARGS);

static Datum mybyteain(char *inputtext, size_t len);

Datum
orafce_to_char_int4(PG_FUNCTION_ARGS)
{
	int32		arg0 = PG_GETARG_INT32(0);
	StringInfo	buf = makeStringInfo();

	appendStringInfo(buf, "%d", arg0);

	PG_RETURN_TEXT_P(cstring_to_text(buf->data));
}

Datum
orafce_to_char_int8(PG_FUNCTION_ARGS)
{
	int64		arg0 = PG_GETARG_INT64(0);
	StringInfo	buf = makeStringInfo();

	appendStringInfo(buf, INT64_FORMAT, arg0);

	PG_RETURN_TEXT_P(cstring_to_text(buf->data));
}

Datum
orafce_to_char_float4(PG_FUNCTION_ARGS)
{
	char	   *p;
	char	   *result;
	struct lconv *lconv = PGLC_localeconv();

	result = DatumGetCString(DirectFunctionCall1(float4out, PG_GETARG_DATUM(0)));

	for (p = result; *p; p++)
		if (*p == '.')
			*p = lconv->decimal_point[0];

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
orafce_to_char_float8(PG_FUNCTION_ARGS)
{
	char	   *p;
	char	   *result;
	struct lconv *lconv = PGLC_localeconv();

	result = DatumGetCString(DirectFunctionCall1(float8out, PG_GETARG_DATUM(0)));

	for (p = result; *p; p++)
		if (*p == '.')
			*p = lconv->decimal_point[0];

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
orafce_to_char_numeric(PG_FUNCTION_ARGS)
{
	Numeric		arg0 = PG_GETARG_NUMERIC(0);
	StringInfo	buf = makeStringInfo();
	struct lconv *lconv = PGLC_localeconv();
	char	   *p;
	char       *decimal = NULL;

	appendStringInfoString(buf, DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(arg0))));

	for (p = buf->data; *p; p++)
		if (*p == '.')
		{
			*p = lconv->decimal_point[0];
			decimal = p; /* save decimal point position for the next loop */
		}

	/* Simulate the default OpenTenBase_Ora to_char template (TM9 - Text Minimum)
	   by removing unneeded digits after the decimal point;
	   if no digits are left, then remove the decimal point too
	*/
	for (p = buf->data + buf->len - 1; decimal && p >= decimal; p--)
	{
		if (*p == '0' || *p == lconv->decimal_point[0])
			*p = 0;
		else
			break; /* non-zero digit found, exit the loop */
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf->data));
}

PG_FUNCTION_INFO_V1(number_to_char_fmt_nls);
Datum
number_to_char_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum   data;
	text    *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	text *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data = DirectFunctionCall2(numeric_to_char, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1));
	return DirectFunctionCall3(numeric_to_char_nls,
					data,
					PG_GETARG_DATUM(1),
					PG_GETARG_DATUM(2));
}

PG_FUNCTION_INFO_V1(numeric_to_number_def_on_conv_err);
Datum
numeric_to_number_def_on_conv_err(PG_FUNCTION_ARGS)
{
	Datum        res;
	bool    hasError = false;
	text        *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text        *def = PG_GETARG_TEXT_PP_IF_NULL(1);

	if(!arg)
		PG_RETURN_NULL();
	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);

	res = DirectFunctionCall1WithSubTran(orcl_text_tonumber, PG_GETARG_DATUM(0), &hasError);
	if (hasError)
	{
		if(!def)
			PG_RETURN_NULL();
		PG_RETURN_NULL_IF_EMPTY_TEXT(def);

		res = DirectFunctionCall1(orcl_text_tonumber, PG_GETARG_DATUM(1));
	}
	return res;
}

PG_FUNCTION_INFO_V1(numeric_to_number_def_on_conv_err_fmt);
Datum
numeric_to_number_def_on_conv_err_fmt(PG_FUNCTION_ARGS)
{
	Datum        res;
	bool    hasError = false;
	text        *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text        *def = PG_GETARG_TEXT_PP_IF_NULL(1);
	text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	if(!arg)
		PG_RETURN_NULL();
	if (!fmt)
		elog(ERROR, "This argument must be a literal");

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	res = DirectFunctionCall2WithSubTran(numeric_to_number,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(2),
						&hasError);
	if (hasError)
	{
		if(!def)
			PG_RETURN_NULL();
		PG_RETURN_NULL_IF_EMPTY_TEXT(def);

		res = DirectFunctionCall2(numeric_to_number, PG_GETARG_DATUM(1), PG_GETARG_DATUM(2));
	}
	return res;
}

PG_FUNCTION_INFO_V1(numeric_to_number_def_on_conv_err_fmt_nls);
Datum
numeric_to_number_def_on_conv_err_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum        res;
	bool    hasError = false;
	Datum      data1;
	Datum      data2;
	text        *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text        *def = PG_GETARG_TEXT_PP_IF_NULL(1);
	text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(2);
	text     *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(3);
	char fmtstr[2] = {0, 0};

	if (!arg || !fmt)
		PG_RETURN_NULL();

	if (!nlsVal)
		elog(ERROR, "This argument must be a literal");

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data1 = DirectFunctionCall3WithSubTran(numeric_to_number_nls,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3),
						&hasError);
	if (!hasError)
		res = DirectFunctionCall2WithSubTran(numeric_to_number,
							data1,
							PG_GETARG_DATUM(2),
							&hasError);
	if (hasError)
	{
		if (!def)
			PG_RETURN_NULL();
		PG_RETURN_NULL_IF_EMPTY_TEXT(def);

		data2 = DirectFunctionCall3(numeric_to_number_nls,
						PG_GETARG_DATUM(1),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3));
		res = DirectFunctionCall2(numeric_to_number, data2, PG_GETARG_DATUM(2));
		if (data2)
			pfree((char *)data2);
	}
	if (data1)
		pfree((char* )data1);
	return res;
}

PG_FUNCTION_INFO_V1(numeric_to_number_fmt_nls);
Datum
numeric_to_number_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum    res;
	Datum   data;
	text    *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text    *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	text *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data = DirectFunctionCall3(numeric_to_number_nls,
					PG_GETARG_DATUM(0),
					PG_GETARG_DATUM(1),
					PG_GETARG_DATUM(2));
	res = DirectFunctionCall2(numeric_to_number, data, PG_GETARG_DATUM(1));
	if (data)
		pfree((char *)data);
	return res;
}

/********************************************************************
 *
 * orafec_to_char_timestamp
 *
 * Syntax:
 *
 * text to_date(timestamp date_txt)
 *
 * Purpose:
 *
 * Returns date and time format w.r.t NLS_DATE_FORMAT GUC
 *
 *********************************************************************/

Datum
orafce_to_char_timestamp(PG_FUNCTION_ARGS)
{
	Timestamp ts = PG_GETARG_TIMESTAMP(0);
	text *result = NULL;

	if(nls_date_format && strlen(nls_date_format) > 0)
	{
		/* it will return the DATE in nls_date_format*/
		result = DatumGetTextP(DirectFunctionCall2(timestamp_to_char,
							TimestampGetDatum(ts),
								CStringGetDatum(cstring_to_text(nls_date_format))));
	}
	else
	{
		result = cstring_to_text(DatumGetCString(DirectFunctionCall1(timestamp_out,
									TimestampGetDatum(ts))));
	}

	PG_RETURN_TEXT_P(result);
}

Datum
orafce_to_number(PG_FUNCTION_ARGS)
{
	text	   *arg0 = PG_GETARG_TEXT_PP(0);
	char	   *buf;
	struct lconv *lconv = PGLC_localeconv();
	Numeric		res;
	char	   *p;

	buf = text_to_cstring(arg0);

	for (p = buf; *p; p++)
		if (*p == lconv->decimal_point[0] && lconv->decimal_point[0])
			*p = '.';
		else if (*p == lconv->thousands_sep[0] && lconv->thousands_sep[0])
			*p = ',';

	res = DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(buf), 0, -1));

	PG_RETURN_NUMERIC(res);
}

/* 3 is enough, but it is defined as 4 in backend code. */
#ifndef MAX_CONVERSION_GROWTH
#define MAX_CONVERSION_GROWTH  4
#endif

/*
 * Convert a tilde (~) to ...
 *	1: a full width tilde. (same as JA16EUCTILDE in opentenbase_ora)
 *	0: a full width overline. (same as JA16EUC in opentenbase_ora)
 *
 * Note - there is a difference with OpenTenBase_Ora - it returns \342\210\274
 * what is a tilde char. Orafce returns fullwidth tilde. If it is a
 * problem, fix it for sef in code.
 */
#define JA_TO_FULL_WIDTH_TILDE	1

static const char *
TO_MULTI_BYTE_UTF8[95] =
{
	"\343\200\200",
	"\357\274\201",
	"\342\200\235",
	"\357\274\203",
	"\357\274\204",
	"\357\274\205",
	"\357\274\206",
	"\342\200\231",
	"\357\274\210",
	"\357\274\211",
	"\357\274\212",
	"\357\274\213",
	"\357\274\214",
	"\357\274\215",
	"\357\274\216",
	"\357\274\217",
	"\357\274\220",
	"\357\274\221",
	"\357\274\222",
	"\357\274\223",
	"\357\274\224",
	"\357\274\225",
	"\357\274\226",
	"\357\274\227",
	"\357\274\230",
	"\357\274\231",
	"\357\274\232",
	"\357\274\233",
	"\357\274\234",
	"\357\274\235",
	"\357\274\236",
	"\357\274\237",
	"\357\274\240",
	"\357\274\241",
	"\357\274\242",
	"\357\274\243",
	"\357\274\244",
	"\357\274\245",
	"\357\274\246",
	"\357\274\247",
	"\357\274\250",
	"\357\274\251",
	"\357\274\252",
	"\357\274\253",
	"\357\274\254",
	"\357\274\255",
	"\357\274\256",
	"\357\274\257",
	"\357\274\260",
	"\357\274\261",
	"\357\274\262",
	"\357\274\263",
	"\357\274\264",
	"\357\274\265",
	"\357\274\266",
	"\357\274\267",
	"\357\274\270",
	"\357\274\271",
	"\357\274\272",
	"\357\274\273",
	"\357\274\274",
	"\357\274\275",
	"\357\274\276",
	"\357\274\277",
	"\342\200\230",
	"\357\275\201",
	"\357\275\202",
	"\357\275\203",
	"\357\275\204",
	"\357\275\205",
	"\357\275\206",
	"\357\275\207",
	"\357\275\210",
	"\357\275\211",
	"\357\275\212",
	"\357\275\213",
	"\357\275\214",
	"\357\275\215",
	"\357\275\216",
	"\357\275\217",
	"\357\275\220",
	"\357\275\221",
	"\357\275\222",
	"\357\275\223",
	"\357\275\224",
	"\357\275\225",
	"\357\275\226",
	"\357\275\227",
	"\357\275\230",
	"\357\275\231",
	"\357\275\232",
	"\357\275\233",
	"\357\275\234",
	"\357\275\235",
#if JA_TO_FULL_WIDTH_TILDE
	"\357\275\236"
#else
	"\357\277\243"
#endif
};

static const char *
TO_MULTI_BYTE_EUCJP[95] =
{
	"\241\241",
	"\241\252",
	"\241\311",
	"\241\364",
	"\241\360",
	"\241\363",
	"\241\365",
	"\241\307",
	"\241\312",
	"\241\313",
	"\241\366",
	"\241\334",
	"\241\244",
	"\241\335",
	"\241\245",
	"\241\277",
	"\243\260",
	"\243\261",
	"\243\262",
	"\243\263",
	"\243\264",
	"\243\265",
	"\243\266",
	"\243\267",
	"\243\270",
	"\243\271",
	"\241\247",
	"\241\250",
	"\241\343",
	"\241\341",
	"\241\344",
	"\241\251",
	"\241\367",
	"\243\301",
	"\243\302",
	"\243\303",
	"\243\304",
	"\243\305",
	"\243\306",
	"\243\307",
	"\243\310",
	"\243\311",
	"\243\312",
	"\243\313",
	"\243\314",
	"\243\315",
	"\243\316",
	"\243\317",
	"\243\320",
	"\243\321",
	"\243\322",
	"\243\323",
	"\243\324",
	"\243\325",
	"\243\326",
	"\243\327",
	"\243\330",
	"\243\331",
	"\243\332",
	"\241\316",
	"\241\357",
	"\241\317",
	"\241\260",
	"\241\262",
	"\241\306",		/* OpenTenBase_Ora returns different value \241\307 */
	"\243\341",
	"\243\342",
	"\243\343",
	"\243\344",
	"\243\345",
	"\243\346",
	"\243\347",
	"\243\350",
	"\243\351",
	"\243\352",
	"\243\353",
	"\243\354",
	"\243\355",
	"\243\356",
	"\243\357",
	"\243\360",
	"\243\361",
	"\243\362",
	"\243\363",
	"\243\364",
	"\243\365",
	"\243\366",
	"\243\367",
	"\243\370",
	"\243\371",
	"\243\372",
	"\241\320",
	"\241\303",
	"\241\321",
#if JA_TO_FULL_WIDTH_TILDE
	"\241\301"
#else
	"\241\261"
#endif
};

Datum
orafce_to_multi_byte(PG_FUNCTION_ARGS)
{
	text	   *src;
	text	   *dst;
	const char *s;
	char	   *d;
	int			srclen;

#if defined(_MSC_VER) && (defined(_M_X64) || defined(__amd64__))

	__int64			dstlen;

#else

	int			dstlen;

	#endif

	int			i;
	const char **map;

	switch (GetDatabaseEncoding())
	{
		case PG_UTF8:
			map = TO_MULTI_BYTE_UTF8;
			break;
		case PG_EUC_JP:
		case PG_EUC_JIS_2004:
			map = TO_MULTI_BYTE_EUCJP;
			break;
		/*
		 * TODO: Add converter for encodings.
		 */
		default:	/* no need to convert */
			PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	src = PG_GETARG_TEXT_PP(0);
	s = VARDATA_ANY(src);
	srclen = VARSIZE_ANY_EXHDR(src);
	dst = (text *) palloc(VARHDRSZ + srclen * MAX_CONVERSION_GROWTH);
	d = VARDATA(dst);

	for (i = 0; i < srclen; i++)
	{
		unsigned char	u = (unsigned char) s[i];
		if (0x20 <= u && u <= 0x7e)
		{
			const char *m = map[u - 0x20];
			while (*m)
			{
				*d++ = *m++;
			}
		}
		else
		{
			*d++ = s[i];
		}
	}

	dstlen = d - VARDATA(dst);
	SET_VARSIZE(dst, VARHDRSZ + dstlen);

	PG_RETURN_TEXT_P(dst);
}

static int
getindex(const char **map, char *mbchar, int mblen)
{
	int		i;

	for (i = 0; i < 95; i++)
	{
		if (!memcmp(map[i], mbchar, mblen))
			return i;
	}

	return -1;
}

Datum
orafce_to_single_byte(PG_FUNCTION_ARGS)
{
	text	   *src;
	text	   *dst;
	char	   *s;
	char	   *d;
	int			srclen;

#if defined(_MSC_VER) && (defined(_M_X64) || defined(__amd64__))

	__int64			dstlen;

#else
	
	int			dstlen;

#endif

	const char **map;

	switch (GetDatabaseEncoding())
	{
		case PG_UTF8:
			map = TO_MULTI_BYTE_UTF8;
			break;
		case PG_EUC_JP:
		case PG_EUC_JIS_2004:
			map = TO_MULTI_BYTE_EUCJP;
			break;
		/*
		 * TODO: Add converter for encodings.
		 */
		default:	/* no need to convert */
			PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}

	src = PG_GETARG_TEXT_PP(0);
	s = VARDATA_ANY(src);
	srclen = VARSIZE_ANY_EXHDR(src);

	/* XXX - The output length should be <= input length */
	dst = (text *) palloc0(VARHDRSZ + srclen);
	d = VARDATA(dst);

	while (*s && (s - VARDATA_ANY(src) < srclen))
	{
		char   *u = s;
		int		clen;
		int		mapindex;

		clen = pg_mblen(u);
		s += clen;

		if (clen == 1)
			*d++ = *u;
		else if ((mapindex = getindex(map, u, clen)) >= 0)
		{
			const char m = 0x20 + mapindex;
			*d++ = m;
		}
		else
		{
			memcpy(d, u, clen);
			d += clen;
		}
	}

	dstlen = d - VARDATA(dst);
	SET_VARSIZE(dst, VARHDRSZ + dstlen);

	PG_RETURN_TEXT_P(dst);
}

/*
 * OpenTenBase_Ora rowtype support.
 */
PG_FUNCTION_INFO_V1(rowidtou);
Datum
rowidtou(PG_FUNCTION_ARGS)
{
	char	*str;
	text	*ret;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	str = DatumGetCString(DirectFunctionCall1(rowid_out, PG_GETARG_DATUM(0)));
	ret = cstring_to_text(str);
	PG_RETURN_TEXT_P(ret);
}

PG_FUNCTION_INFO_V1(utorowid);
Datum
utorowid(PG_FUNCTION_ARGS)
{
	char	*str;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	str = text_to_cstring(PG_GETARG_TEXT_P(0));
	return DirectFunctionCall1(rowid_in, CStringGetDatum(str));
}

PG_FUNCTION_INFO_V1(unirowid);
Datum
unirowid(PG_FUNCTION_ARGS)
{
	VarChar    *source = PG_GETARG_VARCHAR_PP(0);
	int32		typmod = PG_GETARG_INT32(1);
	bool		isExplicit = PG_GETARG_BOOL(2);
	int32		len,
				maxlen;
	size_t		maxmblen;
	int			i;
	char	   *s_data;

	len = VARSIZE_ANY_EXHDR(source);
	s_data = VARDATA_ANY(source);
	maxlen = typmod - VARHDRSZ;

	/* No work if typmod is invalid or supplied data fits it already */
	if (maxlen < 0 || len <= maxlen)
		PG_RETURN_VARCHAR_P(source);

	/* only reach here if string is too long... */

	/* truncate multibyte string preserving multibyte boundary */
	maxmblen = pg_mbcharcliplen(s_data, len, maxlen);

	if (!isExplicit)
	{
		for (i = maxmblen; i < len; i++)
			if (s_data[i] != ' ')
				ereport(ERROR,
						(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
						 errmsg("value too long for type urowid(%d)",
								maxlen)));
	}

	PG_RETURN_VARCHAR_P((VarChar *) cstring_to_text_with_len(s_data,
															 maxmblen));
}

/*
 * Support SIMPLE_INTEGER.
 *
 * INT2/4/8: Define INT4 operator with SIMPLE_INTEGER:
 * - Result of SIMPLE_INTEGER op INT2/4 with INT2/4 is INT4.
 * - Result of SIMPLE_INTEGER op INT8 with INT8 is INT8. Assignment back to
 *   SIMPLE_INTEGER may be overflow exception.
 * - Result of SIMPLE_INTEGER op FLOAT(n)/NUMERIC is float point. Assign it to
 *   SIMPLE_INTEGER causes overflow exception.
 *
 * Therefore, do not try to rewrite the operators with FLOAT or NUMERIC.
 */

/*
 * round_simpint
 *   Overflow exception will be handled as wrapped-around in range from
 * -2,147,483,648(INT_MIN) through 2,147,483,647(INT_MAX).
 */
static int32
round_simpint(int64 result)
{
	int64 nres = result;

	while (nres < INT_MIN || nres > INT_MAX) /* out of range */
	{
		if (nres < INT_MIN)
		{
			int64	delta;

			delta = INT_MIN - nres - 1; /* including INT_MAX */
			Assert(delta >= 0);
			nres = INT_MAX - delta;
		}
		else
		{
			int64	delta;

			delta = nres - INT_MAX - 1; /* including INT_MIN */
			Assert(delta >= 0);
			nres = INT_MIN + delta;
		}
	}

	return nres;
}

/*
 * +: int4 + simple_int or simple_int + int4.
 */
PG_FUNCTION_INFO_V1(simpint_int4_pl);
Datum
simpint_int4_pl(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int32	a2 = PG_GETARG_INT32(1);
	int64	result;

	result = (int64) a1 + a2;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * -: int4 - simple_int or simple_int - int4.
 */
PG_FUNCTION_INFO_V1(simpint_int4_mi);
Datum
simpint_int4_mi(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int32	a2 = PG_GETARG_INT32(1);
	int64	result;

	result = (int64) a1 - a2;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * *: int4 * simple_int or simple_int * int4
 */
PG_FUNCTION_INFO_V1(simpint_int4_ml);
Datum
simpint_int4_ml(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int32	a2 = PG_GETARG_INT32(1);
	int64	result;

	result = (int64) a1 * (int64) a2;

	PG_RETURN_INT32(round_simpint(result));
}

/*
 * /: simple_int / int4 or int4 / simple_int
 */
PG_FUNCTION_INFO_V1(simpint_int4_div);
Datum
simpint_int4_div(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int32	a2 = PG_GETARG_INT32(1);
	int32	result;

	if (a2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	result = a1 / a2; /* Never over/underflow */
	PG_RETURN_INT32(result);
}

/*
 * -(neg): - simple_int
 */
PG_FUNCTION_INFO_V1(simpint_um);
Datum
simpint_um(PG_FUNCTION_ARGS)
{
	int32		arg = PG_GETARG_INT32(0);
	int64		result;

	result = -arg;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * simple_integer and int2
 */
/*
 * +: simple_int + int2.
 */
PG_FUNCTION_INFO_V1(simpint_int2_pl);
Datum
simpint_int2_pl(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int16	a2 = PG_GETARG_INT16(1);
	int64	result;

	result = (int64) a1 + (int64) a2;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * -: simple_int - int2.
 */
PG_FUNCTION_INFO_V1(simpint_int2_mi);
Datum
simpint_int2_mi(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int16	a2 = PG_GETARG_INT16(1);
	int64	result;

	result = (int64) a1 - (int64) a2;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * *: simple_int * int2
 */
PG_FUNCTION_INFO_V1(simpint_int2_ml);
Datum
simpint_int2_ml(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int16	a2 = PG_GETARG_INT16(1);
	int64	result;

	result = (int64) a1 * (int64) a2;

	PG_RETURN_INT32(round_simpint(result));
}

/*
 * /: simple_int / int2
 */
PG_FUNCTION_INFO_V1(simpint_int2_div);
Datum
simpint_int2_div(PG_FUNCTION_ARGS)
{
	int32	a1 = PG_GETARG_INT32(0);
	int16	a2 = PG_GETARG_INT16(1);
	int32	result;

	if (a2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	result = a1 / a2; /* Never over/underflow */
	PG_RETURN_INT32(result);
}

/*
 * int2 and simple_integer
 */
/*
 * +: int2 + simple_int.
 */
PG_FUNCTION_INFO_V1(int2_simpint_pl);
Datum
int2_simpint_pl(PG_FUNCTION_ARGS)
{
	int16	a1 = PG_GETARG_INT16(0);
	int32	a2 = PG_GETARG_INT32(1);
	int64	result;

	result = (int64) a1 + (int64) a2;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * -: int2 - simple_int.
 */
PG_FUNCTION_INFO_V1(int2_simpint_mi);
Datum
int2_simpint_mi(PG_FUNCTION_ARGS)
{
	int16	a1 = PG_GETARG_INT16(0);
	int32	a2 = PG_GETARG_INT32(1);
	int64	result;

	result = (int64) a1 - (int64) a2;
	PG_RETURN_INT32(round_simpint(result));
}

/*
 * *: int2 * simple_int
 */
PG_FUNCTION_INFO_V1(int2_simpint_ml);
Datum
int2_simpint_ml(PG_FUNCTION_ARGS)
{
	int16	a1 = PG_GETARG_INT16(0);
	int32	a2 = PG_GETARG_INT32(1);
	int64	result;

	result = (int64) a1 * (int64) a2;

	PG_RETURN_INT32(round_simpint(result));
}

/*
 * /: int2 / simple_int
 */
PG_FUNCTION_INFO_V1(int2_simpint_div);
Datum
int2_simpint_div(PG_FUNCTION_ARGS)
{
	int16	a1 = PG_GETARG_INT16(0);
	int32	a2 = PG_GETARG_INT32(1);
	int32	result;

	if (a2 == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));
		PG_RETURN_NULL();
	}

	result = a1 / a2; /* Never over/underflow */
	PG_RETURN_INT32(result);
}

/*
 * opentenbase_ora function to_binary_float(text, text)
 * 		convert string to float4(binary_float)
 */
PG_FUNCTION_INFO_V1(to_binary_float);
Datum
to_binary_float(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	char fmtstr[2] = {0, 0};

	if (!txt || !fmt)
		PG_RETURN_NULL();

	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	if (!fmt)
	{
		char *txtstr = text_to_cstring(txt);
		Datum result = DirectFunctionCall1(float4in, CStringGetDatum(txtstr));
		pfree(txtstr);

		return DirectFunctionCall1(float4_numeric, result);
	}
	else
	{
		Datum val = DirectFunctionCall2(numeric_to_number,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(1));
		return NumericGetDatum(val);
	}
}

/*
 * opentenbase_ora function to_binary_double(text, text)
 * 		convert string to float8(to_binary_double)
 */
PG_FUNCTION_INFO_V1(to_binary_double);
Datum
to_binary_double(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	char fmtstr[2] = {0, 0};

	if (!txt || !fmt)
		PG_RETURN_NULL();

	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	if (!fmt)
	{
		char *txtstr = text_to_cstring(txt);
		Datum result = DirectFunctionCall1(float8in, CStringGetDatum(txtstr));
		pfree(txtstr);

		return DirectFunctionCall2(float8_numeric, result, Int32GetDatum(ORACL_BINARY_DOUBLE_TYPEMOD));
	}
	else
	{
		Datum val = DirectFunctionCall2(numeric_to_number,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(1));
		return NumericGetDatum(val);
	}
}

/*
 * opentenbase_ora function to_binary_float_def_on_conv_err(text, text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_float_def_on_conv_err);
Datum
to_binary_float_def_on_conv_err(PG_FUNCTION_ARGS)
{
	Datum	ret;
	char	*defValStr = NULL;
	char	*floatValStr = NULL;
	bool	hasError = true;

	text *floatVal = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *defVal = PG_GETARG_TEXT_PP_IF_NULL(1);

	if (!floatVal)
		PG_RETURN_NULL();

	floatValStr	= text_to_cstring(floatVal);
	ret = DirectFunctionCall1WithSubTran(float4in, CStringGetDatum(floatValStr), &hasError);

	if (hasError)
	{
		if (!defVal)
		{
			if (floatValStr)
				pfree(floatValStr);

			PG_RETURN_NULL();
		}

		defValStr = text_to_cstring(defVal);
		ret = DirectFunctionCall1(float4in, CStringGetDatum(defValStr));
	}

	if (floatValStr)
		pfree(floatValStr);

	if (defValStr)
		pfree(defValStr);

	return DirectFunctionCall1(float4_numeric, ret);
}

/*
 * opentenbase_ora function to_binary_float_default_fmt(text, text, text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_float_def_on_conv_err_with_fmt);
Datum
to_binary_float_def_on_conv_err_with_fmt(PG_FUNCTION_ARGS)
{
	Datum	ret;
	bool	hasError = true;

	text *floatVal = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *defVal = PG_GETARG_TEXT_PP_IF_NULL(1);
	text *fmtVal = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	if (!floatVal)
		PG_RETURN_NULL();

	if (!fmtVal)
		elog(ERROR, "This argument must be a literal");

	text_to_cstring_buffer(fmtVal, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	ret = DirectFunctionCall2WithSubTran(to_binary_float,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(2),
						&hasError);

	if (hasError)
	{
		if (!defVal)
			PG_RETURN_NULL();

		ret = DirectFunctionCall2(to_binary_float, PG_GETARG_DATUM(1), PG_GETARG_DATUM(2));
	}

	return NumericGetDatum(ret);
}

/*
 * opentenbase_ora function to_binary_float_def_on_conv_err(numeric, text, text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_float_def_on_conv_err_with_fmt_error);
Datum
to_binary_float_def_on_conv_err_with_fmt_error(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("too many arguments for function")));
}

/*
 * opentenbase_ora function to_binary_float_with_fmt_nls(arg text, fmt text, nls text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_float_with_fmt_nls);
Datum
to_binary_float_with_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum    res;
	Datum   data;
	text    *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text    *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	text *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data = DirectFunctionCall3(numeric_to_number_nls,
					PG_GETARG_DATUM(0),
					PG_GETARG_DATUM(1),
					PG_GETARG_DATUM(2));
	res = DirectFunctionCall2(to_binary_float, data, PG_GETARG_DATUM(1));
	if (data)
		pfree((char *)data);
	return res;
}

/*
 * opentenbase_ora function to_binary_float_def_on_conv_err_with_fmt_nls(arg text, def text, fmt text, nls text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_float_def_on_conv_err_with_fmt_nls);
Datum
to_binary_float_def_on_conv_err_with_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum        res;
	bool    hasError = false;
	Datum      data1;
	Datum      data2;
	text        *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text        *def = PG_GETARG_TEXT_PP_IF_NULL(1);
	text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(2);
	text     *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(3);
	char fmtstr[2] = {0, 0};

	if (!arg)
		PG_RETURN_NULL();

	if (!fmt || !nlsVal)
		elog(ERROR, "This argument must be a literal");

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data1 = DirectFunctionCall3WithSubTran(numeric_to_number_nls,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3),
						&hasError);
	if (!hasError)
		res = DirectFunctionCall2WithSubTran(to_binary_float, data1, PG_GETARG_DATUM(2), &hasError);
	if (hasError)
	{
		if (!def)
			PG_RETURN_NULL();
		PG_RETURN_NULL_IF_EMPTY_TEXT(def);

		data2 = DirectFunctionCall3(numeric_to_number_nls,
						PG_GETARG_DATUM(1),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3));
		res = DirectFunctionCall2(to_binary_float, data2, PG_GETARG_DATUM(2));
		if (data2)
			pfree((char *)data2);
	}
	if (data1)
		pfree((char* )data1);
	return res;
}

/*
 * opentenbase_ora function to_binary_double_default_fmt(text, text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_double_def_on_conv_err);
Datum
to_binary_double_def_on_conv_err(PG_FUNCTION_ARGS)
{
	Datum	ret;
	char	*defValStr = NULL;
	char	*floatValStr = NULL;
	bool	hasError = true;

	text *floatVal = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *defVal = PG_GETARG_TEXT_PP_IF_NULL(1);

	if (!floatVal)
		PG_RETURN_NULL();

	floatValStr	= text_to_cstring(floatVal);
	ret = DirectFunctionCall1WithSubTran(float8in, CStringGetDatum(floatValStr), &hasError);

	if (hasError)
	{
		if (!defVal)
		{
			if (floatValStr)
				pfree(floatValStr);

			PG_RETURN_NULL();
		}

		defValStr = text_to_cstring(defVal);
		ret = DirectFunctionCall1(float8in, CStringGetDatum(defValStr));
	}

	if (floatValStr)
		pfree(floatValStr);

	if (defValStr)
		pfree(defValStr);

	return DirectFunctionCall2(float8_numeric, ret, Int32GetDatum(ORACL_BINARY_DOUBLE_TYPEMOD));
}

/*
 * opentenbase_ora function to_binary_double_def_on_conv_err_with_fmt(text, text, text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_double_def_on_conv_err_with_fmt);
Datum
to_binary_double_def_on_conv_err_with_fmt(PG_FUNCTION_ARGS)
{
	Datum	ret;
	bool	hasError = true;

	text *floatVal = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *defVal = PG_GETARG_TEXT_PP_IF_NULL(1);
	text *fmtVal = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	if (!floatVal)
		PG_RETURN_NULL();

	if (!fmtVal)
		elog(ERROR, "This argument must be a literal");

	text_to_cstring_buffer(fmtVal, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	ret = DirectFunctionCall2WithSubTran(to_binary_double,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(2),
						&hasError);
	if (hasError)
	{
		if (!defVal)
			PG_RETURN_NULL();

		ret = DirectFunctionCall2(to_binary_double, PG_GETARG_DATUM(1), PG_GETARG_DATUM(2));
	}

	return NumericGetDatum(ret);
}

/*
 * opentenbase_ora function to_binary_double_with_fmt_nls(arg text, fmt text, nls text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_double_with_fmt_nls);
Datum
to_binary_double_with_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum    res;
	Datum   data;
	text    *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text    *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	text *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(2);
	char fmtstr[2] = {0, 0};

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data = DirectFunctionCall3(numeric_to_number_nls,
					PG_GETARG_DATUM(0),
					PG_GETARG_DATUM(1),
					PG_GETARG_DATUM(2));
	res = DirectFunctionCall2(to_binary_double, data, PG_GETARG_DATUM(1));
	if (data)
		pfree((char *)data);
	return res;
}

/*
 * opentenbase_ora function to_binary_double_def_on_conv_err_with_fmt_nls(arg text, def text, fmt text, nls text)
 * 		convert string to numeric
 */
PG_FUNCTION_INFO_V1(to_binary_double_def_on_conv_err_with_fmt_nls);
Datum
to_binary_double_def_on_conv_err_with_fmt_nls(PG_FUNCTION_ARGS)
{
	Datum        res;
	bool    hasError = false;
	Datum      data1;
	Datum      data2;
	text        *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text        *def = PG_GETARG_TEXT_PP_IF_NULL(1);
	text        *fmt = PG_GETARG_TEXT_PP_IF_NULL(2);
	text     *nlsVal = PG_GETARG_TEXT_PP_IF_NULL(3);
	char fmtstr[2] = {0, 0};

	if (!arg)
		PG_RETURN_NULL();

	if (!fmt || !nlsVal)
		elog(ERROR, "This argument must be a literal");

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nlsVal);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	data1 = DirectFunctionCall3WithSubTran(numeric_to_number_nls,
						PG_GETARG_DATUM(0),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3),
						&hasError);
	if (!hasError)
		res = DirectFunctionCall2WithSubTran(to_binary_double, data1, PG_GETARG_DATUM(2), &hasError);
	if (hasError)
	{
		if (!def)
			PG_RETURN_NULL();
		PG_RETURN_NULL_IF_EMPTY_TEXT(def);

		data2 = DirectFunctionCall3(numeric_to_number_nls,
						PG_GETARG_DATUM(1),
						PG_GETARG_DATUM(2),
						PG_GETARG_DATUM(3));
		res = DirectFunctionCall2(to_binary_double, data2, PG_GETARG_DATUM(2));
		if (data2)
			pfree((char *)data2);
	}
	if (data1)
		pfree((char* )data1);
	return res;
}
/*
 * Converts a VARCHAR2 represented using n data bytes into a RAW with n data bytes.
 */
PG_FUNCTION_INFO_V1(cast_to_raw);
Datum
cast_to_raw(PG_FUNCTION_ARGS)
{
	char *old_str = NULL,
		*new_str = NULL;
	text *arg1 = PG_GETARG_TEXT_PP_IF_EXISTS(0);
	Datum result;
	int arglen = 0;
	int new_strlen = 0;

	old_str = TextDatumGetCString(arg1);

	arglen = VARSIZE_ANY_EXHDR(arg1);

	/* handle backslashes */
	new_str =  pre_process_rawstr(old_str, arglen, &new_strlen);

	result = mybyteain(new_str, new_strlen);
	pfree(old_str);
	pfree(new_str);
	return result;
}

static Datum
mybyteain(char *inputText, size_t len)
{
	char	   *tp;
	char	   *rp;
	int			bc;
	bytea	   *result;
	char 		*end = inputText + len;

	/* Recognize hex input */
	if (inputText[0] == '\\' && inputText[1] == 'x')
	{
		bc = (len - 2) / 2 + VARHDRSZ;	/* maximum possible length */
		result = palloc(bc);
		bc = hex_decode(inputText + 2, len - 2, VARDATA(result));
		SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

		PG_RETURN_BYTEA_P(result);
	}


	/* Else, it's the traditional escaped style */
	for (bc = 0, tp = inputText; tp < end; bc++)
	{
		if (tp[0] != '\\')
			tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
			tp += 4;
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
			tp += 2;
		else
		{
			/*
			 * one backslash, not followed by another or ### valid octal
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}
	}

	bc += VARHDRSZ;

	result = (bytea *) palloc(bc);
	SET_VARSIZE(result, bc);

	tp = inputText;
	rp = VARDATA(result);
	while (tp < end)
	{
		if (tp[0] != '\\')
			*rp++ = *tp++;
		else if ((tp[0] == '\\') &&
				 (tp[1] >= '0' && tp[1] <= '3') &&
				 (tp[2] >= '0' && tp[2] <= '7') &&
				 (tp[3] >= '0' && tp[3] <= '7'))
		{
			bc = VAL(tp[1]);
			bc <<= 3;
			bc += VAL(tp[2]);
			bc <<= 3;
			*rp++ = bc + VAL(tp[3]);

			tp += 4;
		}
		else if ((tp[0] == '\\') &&
				 (tp[1] == '\\'))
		{
			*rp++ = '\\';
			tp += 2;
		}
		else
		{
			/*
			 * We should never get here. The first pass should not allow it.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "bytea")));
		}
	}

	PG_RETURN_BYTEA_P(result);
}


/* cast_to_varchar2: Output processing */
static bytea*
ora_raw_output_str_handle(bytea *vlena)
{
	const int ASCIINULL = 0x000;
	const int ASCIIUS = 0x01F;
	const int ASCIIDEL = 0x07F;
	const int TO_HEX = 16;
	int i, j, idx, tmp, olen;
	char *new_str = NULL;
	char *old_str = NULL;

	if (!vlena)
	{
		return NULL;
	}

	old_str = ByteaToHex(vlena, false);
	olen = strlen(old_str);

	/* Necessary check, must be even length. */
	if (olen % 2 != 0 || olen <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("raw_output_str :invalid params len[%d]", olen)));
	}

	new_str = (char *)palloc0(olen + 1);
	for (i = 0, j = 0, idx = 0; i + 1 < olen;)
	{
		tmp = 0;
		tmp = (old_str[i++] - '0') * TO_HEX;
		tmp += (old_str[i++] - '0');

		/* ASCII control characters are not output */
		if ((tmp >= ASCIINULL && tmp <= ASCIIUS) || tmp == ASCIIDEL)
		{
			j++;
			j++;
			continue;
		}
		else
		{
			new_str[idx++] = old_str[j++];
			new_str[idx++] = old_str[j++];
		}
	}
	new_str[idx] = '\0';

	return DatumGetByteaP(DirectFunctionCall1(rawin, PointerGetDatum(new_str)));
}

/*
 * Converts a VARCHAR2 represented using n data bytes into a RAW with n data bytes.
 */
PG_FUNCTION_INFO_V1(cast_to_varchar2);
Datum
cast_to_varchar2(PG_FUNCTION_ARGS)
{
	bytea *vlena = PG_GETARG_BYTEA_P_IF_EXISTS(0);
	int32 new_strlen, varchar2_atttypmod;
	char *new_str = NULL;
	Datum result = (Datum) NULL;

	if (!vlena || VARSIZE_ANY_EXHDR(vlena) <= 0)
		PG_RETURN_NULL();

	vlena = ora_raw_output_str_handle(vlena);
	new_strlen = VARSIZE_ANY_EXHDR(vlena);
	new_str = (char*)palloc(sizeof(char) * (new_strlen + 1));
	strncpy(new_str, VARDATA_ANY(vlena), new_strlen);
	new_str[new_strlen] = '\0';
	varchar2_atttypmod = new_strlen + VARHDRSZ;
	result =  DirectFunctionCall3(
		varchar2in, PointerGetDatum(new_str), Int32GetDatum(InvalidOid), Int32GetDatum(varchar2_atttypmod));
	pfree(new_str);
	return result;
}

/*
 * Converts a VARCHAR2 represented using n data bytes into a text with n data bytes.
 */
PG_FUNCTION_INFO_V1(cast_text_to_varchar2);
Datum
cast_text_to_varchar2(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	bytea *r1 = NULL;
	Datum result = (Datum) NULL;

	if (arg1 == (Datum)NULL)
		PG_RETURN_NULL();

	r1 = DatumGetByteaP(DirectFunctionCall1(texttoraw, arg1));
	result = DirectFunctionCall1(cast_to_varchar2, (Datum)r1);
	pfree(r1);
	return result;
}

/*
 * Returns the length in bytes of a RAW r.
 */
PG_FUNCTION_INFO_V1(raw_strlen);
Datum
raw_strlen(PG_FUNCTION_ARGS)
{
	bytea* data = PG_GETARG_BYTEA_P_IF_EXISTS(0);
	int data_len;

	if (!data)
		PG_RETURN_NULL();

	data_len = VARSIZE(data) - VARHDRSZ;
	PG_RETURN_INT32(data_len);
}

static char*
hex2binary(char*src, int src_len)
{
#define HEXBASE 16
#define HEX_PART_LEN 4
	int src_value;
	char ref_hex2binary[HEXBASE][HEX_PART_LEN + 1] = {"0000", "0001", "0010", "0011", "0100", "0101", "0110", "0111",
		"1000", "1001", "1010", "1011", "1100", "1101", "1110", "1111"};
	char *dst = NULL,
		*head = NULL;

	if (src == NULL || src_len <= 0 || strlen(src) < src_len)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid params")));
		return NULL;
	}

	head = dst = (char*)palloc(sizeof(char) * (src_len * HEX_PART_LEN + 1));

	while (src && *src != '\0')
	{
		if (*src >= '0' && *src <= '9')
		{
			src_value = VAL(*src);
		}
		else if (*src >= 'A' && *src <= 'F')
		{
			src_value = *src - 'A' + 10;
		}
		else if (*src >= 'a' && *src <= 'f')
		{
			src_value = *src - 'a' + 10;
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid params")));
		}
		strncpy(dst, ref_hex2binary[src_value], HEX_PART_LEN);
		dst += HEX_PART_LEN;
		src++;
	}
	*dst = '\0';
	return head;
}

static char*
binary2hex(char*src, int src_len)
{
#define HEX_PART_LEN 4
	int i, dst_value, tmp;
	const char *ref_digits = "0123456789ABCDEF";
	char *dst = NULL,
		*head = NULL;

	if (src == NULL || src_len <= 0 || strlen(src) < src_len || (strlen(src) % 4 != 0))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid params")));
		return NULL;
	}

	head = dst = (char*)palloc(sizeof(char) * (src_len / HEX_PART_LEN + 1));

	while (src && *src != '\0')
	{
		for (i = 0, dst_value = 0; i < HEX_PART_LEN; i++)
		{
			tmp = VAL(src[i]);
			dst_value += tmp << (HEX_PART_LEN - i - 1);
		}

		*dst = ref_digits[dst_value];
		dst++;
		src += HEX_PART_LEN;
	}
	*dst = '\0';
	return head;
}

typedef enum RawBitOpr
{
	RAW_BITAND = 0,
	RAW_BITOR,
	RAW_BITXOR
} RawBitOpr;

static Datum
raw_bitopr(char *r1_str, int r1_len, char* r2_str, int r2_len, RawBitOpr opr)
{
#define HEX_PART_LEN 4
	Datum result = (Datum) NULL;
	int i = 0,
		long_ptr_len = 0,
		short_ptr_len = 0,
		compare_len = 0,
		res_len = 0;
	char *long_ptr = NULL,
		*short_ptr = NULL,
		*res = NULL,
		*head = NULL;

	if (r1_len > r2_len)
	{
		long_ptr = r1_str;
		short_ptr = r2_str;
		long_ptr_len = r1_len;
		short_ptr_len = r2_len;
		compare_len = r2_len * HEX_PART_LEN;
		res_len = r1_len * HEX_PART_LEN;
	}
	else
	{
		long_ptr = r2_str;
		short_ptr = r1_str;
		long_ptr_len = r2_len;
		short_ptr_len = r1_len;
		compare_len = r1_len * HEX_PART_LEN;
		res_len = r2_len * HEX_PART_LEN;
	}

	head = res = (char*)palloc(sizeof(char) * (res_len + 1));
	long_ptr = hex2binary(long_ptr, long_ptr_len);
	short_ptr = hex2binary(short_ptr, short_ptr_len);

	if (opr == RAW_BITAND)
	{
		for (i = 0; i < compare_len; i++)
		{
			*res++ = DIG(VAL(*long_ptr++) & VAL(*short_ptr++));
		}
	}
	else if (opr == RAW_BITOR)
	{
		for (i = 0; i < compare_len; i++)
		{
			*res++ = DIG(VAL(*long_ptr++) | VAL(*short_ptr++));
		}
	}
	else if (opr == RAW_BITXOR)
	{
		for (i = 0; i < compare_len; i++)
		{
			*res++ = DIG(VAL(*long_ptr++) ^ VAL(*short_ptr++));
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid params")));
	}
	
	/* the unprocessed portion of the longer RAW is appended to the partial result. */
	while (long_ptr && *long_ptr != '\0')
	{
		*res++ = *long_ptr++;
	}
	*res = '\0';

	res = binary2hex(head, res_len);
	pfree(head);
	result = DirectFunctionCall1(rawin, PointerGetDatum(res));
	return result;
}

/*
 * This function performs bitwise logical "and" of the values in RAW r1 with RAW r2 and returns the "anded" result RAW.
 */
PG_FUNCTION_INFO_V1(raw_bitand);
Datum
raw_bitand(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0);
	Datum r2 = PG_GETARG_DATUM(1);
	char *r1_str = NULL,
		*r2_str = NULL;
	int r1_len, r2_len;
	Datum result = (Datum) NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	r2_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r2));
	r1_len = strlen(r1_str);
	r2_len = strlen(r2_str);
	if (r1_len == 0 || r2_len == 0)
	{
		pfree(r1_str);
		pfree(r2_str);
		PG_RETURN_NULL();
	}

	result = raw_bitopr(r1_str, r1_len, r2_str, r2_len, RAW_BITAND);
	pfree(r1_str);
	pfree(r2_str);
	return result;
}

/*
 * This function performs bitwise logical "or" of the values in RAW r1 with RAW r2 and returns the or'd result RAW.
 */
PG_FUNCTION_INFO_V1(raw_bitor);
Datum
raw_bitor(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0);
	Datum r2 = PG_GETARG_DATUM(1);
	char *r1_str = NULL,
		*r2_str = NULL;
	int r1_len, r2_len;
	Datum result = (Datum) NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	r2_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r2));
	r1_len = strlen(r1_str);
	r2_len = strlen(r2_str);
	if (r1_len == 0 || r2_len == 0)
	{
		pfree(r1_str);
		pfree(r2_str);
		PG_RETURN_NULL();
	}

	result = raw_bitopr(r1_str, r1_len, r2_str, r2_len, RAW_BITOR);
	pfree(r1_str);
	pfree(r2_str);
	return result;
}

/*
 * This function performs bitwise logical "exclusive or" of the values in RAW r1 with RAW r2
 * and returns the xor'd result RAW.
 */
PG_FUNCTION_INFO_V1(raw_bitxor);
Datum
raw_bitxor(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0);
	Datum r2 = PG_GETARG_DATUM(1);
	char *r1_str = NULL,
		*r2_str = NULL;
	int r1_len, r2_len;
	Datum result = (Datum) NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	r2_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r2));
	r1_len = strlen(r1_str);
	r2_len = strlen(r2_str);
	if (r1_len == 0 || r2_len == 0)
	{
		pfree(r1_str);
		pfree(r2_str);
		PG_RETURN_NULL();
	}

	result = raw_bitopr(r1_str, r1_len, r2_str, r2_len, RAW_BITXOR);
	pfree(r1_str);
	pfree(r2_str);
	return result;
}

/*
 * Compares raw r1 against raw r2. Returns 0 if r1 and r2 are identical, otherwise, 
 * returns the position of the first byte from r1 that does not match r2
 */
PG_FUNCTION_INFO_V1(raw_compare);
Datum
raw_compare(PG_FUNCTION_ARGS)
{
	bytea *r1 = PG_GETARG_BYTEA_P_IF_EXISTS(0);
	bytea *r2 = PG_GETARG_BYTEA_P_IF_EXISTS(1);
	char *r1_str = NULL,
		*r2_str = NULL;
	int r1_len, r2_len,
		result = 0,
		index = 1;

	if (!r1 && !r2)
	{
		result = 0;
	}
	else if ((!r1 && r2) || (r1 && !r2))
	{
		result = index;
	}
	else
	{
		r1_str = VARDATA_ANY(r1);
		r2_str = VARDATA_ANY(r2);
		r1_len = VARSIZE_ANY_EXHDR(r1);
		r2_len = VARSIZE_ANY_EXHDR(r2);
		while (r1_str
			&& r2_str
			&& *r1_str != '\0'
			&& *r2_str != '\0'
			&& *r1_str == *r2_str
			&& index <= r1_len
			&& index <= r2_len)
		{
			r1_str++;
			r2_str++;
			index++;
		}

		if (index > r1_len && index > r2_len)
		{
			result = 0;
		}
		else
		{
			result = index;
		}
	}

	PG_RETURN_INT32(result);
}

/*
 * Perform bitwise logical "complement" of the values in raw and return the "complement'ed" result raw.
 */
PG_FUNCTION_INFO_V1(raw_bitcomplement);
Datum
raw_bitcomplement(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0),
		result = (Datum) NULL;
	char *r1_str = NULL,
		*res = NULL,
		*head = NULL,
		*head1_ptr = NULL;
	int r1_len, i, new_strlen;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	head1_ptr = r1_str;
	r1_len = strlen(r1_str);
	if (r1_len == 0)
	{
		pfree(r1_str);
		PG_RETURN_NULL();
	}

	head = res = hex2binary(r1_str, r1_len);
	new_strlen = strlen(head);
	for (i = 0; i < new_strlen; i++)
	{
		*res = DIG(1 - VAL(*res));
		res++;
	}

	res = binary2hex(head, new_strlen);
	pfree(head);

	result = DirectFunctionCall1(rawin, PointerGetDatum(res));
	pfree(head1_ptr);
	return result;
}

/*
 * Perform bitwise logical "complement" of the values in raw and return the "complement'ed" result raw.
 */
PG_FUNCTION_INFO_V1(raw_reverse);
Datum
raw_reverse(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0),
		result = (Datum) NULL;
	char *r1_str = NULL,
		*new_str = NULL,
		tmp[3] = {0};
	int i, j, r1_len;
	const int elements_per_byte = 2;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("numeric or value error.")));

	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	r1_len = strlen(r1_str);
	if (r1_len == 0)
	{
		pfree(r1_str);
		PG_RETURN_NULL();
	}

	new_str = r1_str;
	i = 0;
	j = r1_len - 2;
	while (j - i >= elements_per_byte)
	{
		strncpy(tmp, new_str + i, elements_per_byte);
		strncpy(new_str + i, new_str + j, elements_per_byte);
		strncpy(new_str + j, tmp, elements_per_byte);
		i += elements_per_byte;
		j -= elements_per_byte;
	}

	result = DirectFunctionCall1(rawin, PointerGetDatum(new_str));
	pfree(r1_str);
	return result;
}

/*
 * Return a substring portion of raw r beginning at pos for len bytes.
 */
PG_FUNCTION_INFO_V1(raw_substr);
Datum
raw_substr(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0);
	int32 pos = PG_GETARG_INT32_0_IF_EXISTS(1),
		len = PG_GETARG_INT32_0_IF_EXISTS(2),
		min_required_len;
	char *r1_str = NULL,
		*new_str = NULL;
	int r1_len, new_strlen, new_pos;
	const int elements_per_byte = 2;
	Datum result = (Datum) NULL;
	bool is_valid = true;
	bool len_null = false;

	/* If len is not given, print all. */
	if (PG_NARGS() < 3 || PG_ARGISNULL(2))
	{
		len_null = true;
	}

	if (r1 == (Datum)NULL ||
		((!len_null) && len <= 0) ||
		PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("numeric or value error.")));
	}

	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	r1_len = strlen(r1_str);

	/* Check the validity of input parameters */
	if (pos > 0)
	{
		min_required_len = (pos - 1 + len) * elements_per_byte;
		if (min_required_len < 0)
		{
			min_required_len = 0;
		}
		new_pos = (pos - 1) * elements_per_byte;
		if (new_pos < 0)
		{
			is_valid = false;
		}
		else
		{
			is_valid = min_required_len <= r1_len ? true : false;
		}
	}
	else if (pos == 0)
	{
		min_required_len = len * elements_per_byte;
		new_pos = pos;
		is_valid = min_required_len <= r1_len ? true : false;
	}
	else
	{
		min_required_len = len * elements_per_byte;
		new_pos = r1_len - (abs(pos) * elements_per_byte);
		if (abs(pos) < len || new_pos < 0 || min_required_len > r1_len || new_pos > r1_len)
		{
			is_valid = false;
		}
	}

	new_strlen = len_null ? (r1_len - new_pos) : len * elements_per_byte;
	if (!is_valid || new_strlen <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("numeric or value error.")));
	}

	new_str = (char*)palloc(sizeof(char) * (new_strlen + 1));
	strncpy(new_str, r1_str + new_pos, new_strlen);
	new_str[new_strlen] = '\0';

	result = DirectFunctionCall1(rawin, PointerGetDatum(new_str));
	pfree(r1_str);
	pfree(new_str);
	return result;
}

/*
 * This function returns n copies of r concatenated together.
 */
PG_FUNCTION_INFO_V1(raw_copies);
Datum
raw_copies(PG_FUNCTION_ARGS)
{
	Datum r1 = PG_GETARG_DATUM(0);
	Datum num = PG_GETARG_DATUM(1);
	float4 n;
	char *r1_str = NULL,
		*new_str = NULL;
	int i, n_round, r1_len, new_strlen;
	Datum result = (Datum) NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("numeric or value error.")));

	n = DatumGetFloat4(DirectFunctionCall1(numeric_float4, num));
	r1_str = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), r1));
	n_round = (int)round(n);
	r1_len = strlen(r1_str);
	if (n_round <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("numeric or value error.")));
	}

	new_strlen = r1_len * n_round;
	new_str = (char*)palloc(sizeof(char) * (new_strlen + 1));
	for (i = 0; i < new_strlen;)
	{
		strncpy(new_str + i, r1_str, r1_len);
		i += r1_len;
	}
	new_str[new_strlen] = '\0';

	result = DirectFunctionCall1(rawin, PointerGetDatum(new_str));
	pfree(r1_str);
	pfree(new_str);
	return result;
}

/*
 * Concatenate a set of 12 raws into a single raw.
 */
PG_FUNCTION_INFO_V1(raw_concat);
Datum
raw_concat(PG_FUNCTION_ARGS)
{
#define SUPPORT_CONCAT_NUMS 12
	bytea *arg = NULL;
	char *r_str[SUPPORT_CONCAT_NUMS] = {0},
		*new_str = NULL;
	int i, j,
		new_strlen = 0,
		r_len[SUPPORT_CONCAT_NUMS] = {0};
	Datum result = (Datum) NULL;

	for (i = 0; i < PG_NARGS(); i++)
	{
		if ((!PG_ARGISNULL(i)) && PG_GETARG_DATUM(i))
		{
			arg = PG_GETARG_BYTEA_P(i);
			if (VARSIZE_ANY_EXHDR(arg) > 0)
			{
				r_str[i] = DatumGetCString(DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), PointerGetDatum(arg)));
				r_len[i] = strlen(r_str[i]);
				new_strlen += r_len[i];
			}
		}
	}

	if (new_strlen == 0)
	{
		PG_RETURN_NULL();
	}

	new_str = (char*)palloc(sizeof(char) * (new_strlen + 1));
	for (i = 0, j = 0; i < PG_NARGS(); i++)
	{
		if (r_str[i])
		{
			strncpy(new_str + j, r_str[i], r_len[i]);
			j += r_len[i];
		}
	}
	new_str[new_strlen] = '\0';

	result = DirectFunctionCall1(rawin, PointerGetDatum(new_str));

	/* Clean up resource. */
	for (i = 0, j = 0; i < PG_NARGS(); i++)
	{
		if (r_str[i])
		{
			pfree(r_str[i]);
			r_len[i] = 0;
		}
	}
	pfree(new_str);
	return result;
}

/*
 * Converts a VARCHAR2 represented using n data bytes into a RAW with n data bytes.
 */
PG_FUNCTION_INFO_V1(longraw_nosupport);
Datum
longraw_nosupport(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("illegal use of LONG datatype.")));
}
/*
 * opentenbase_ora function orcl_timestampltz_to_char_with_fmt_nls(text, text, text)
 * 		resolve parameter conflicts
 */
PG_FUNCTION_INFO_V1(orcl_character_to_char_with_fmt_nls);
Datum
orcl_character_to_char_with_fmt_nls(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		errmsg("invalid input syntax for type numeric")));
	PG_RETURN_NULL();
}
