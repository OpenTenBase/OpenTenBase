/*-------------------------------------------------------------------------
 *
 * raw.c
 *	  Functions for the variable-length built-in types.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/raw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include "access/hash.h"
#include "access/tuptoaster.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "common/md5.h"
#include "lib/hyperloglog.h"
#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "port/pg_bswap.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/sortsupport.h"
#include "utils/formatting.h"
#include "utils/varlena.h"
#include "utils/array.h"

extern Datum textin(PG_FUNCTION_ARGS);
extern Datum binary_decode(PG_FUNCTION_ARGS);

#define VAL(CH)   ((CH) - '0')
#define DIG(VAL)  ((VAL) + '0')
#define LIKE_TRUE 1

/*
 * input interface of RAW type
 */
Datum
rawin(PG_FUNCTION_ARGS)
{
	Datum fmt = DirectFunctionCall1(textin, CStringGetDatum(pstrdup("HEX")));
	char *cstring_arg1 = PG_GETARG_CSTRING(0);
	char *tmp = NULL;
	int   len = 0;
	Datum arg1;

	len = strlen(cstring_arg1);
	if ((len % 2) != 0)
	{
		tmp = (char *) palloc0(len + 2);
		tmp[0] = '0';
		strncat(tmp, cstring_arg1, len + 1);
		arg1 = DirectFunctionCall1(textin, CStringGetDatum(tmp));
	}
	else
	{
		arg1 = DirectFunctionCall1(textin, PG_GETARG_DATUM(0));
	}

	return DirectFunctionCall2(binary_decode, arg1, fmt);
}

/*
 * output interface of RAW type
 */
Datum
rawout(PG_FUNCTION_ARGS)
{
	bytea *data = PG_GETARG_BYTEA_P(0);
	text  *ans = NULL;
	int    datalen = 0;
	int    resultlen = 0;
	int    ans_len = 0;
	char  *out_string = NULL;

	/*
	 * fcinfo->fncollation is set to 0 when calling Macro FuncCall1,
	 * so the collation value needs to be reset.
	 */
	if (!OidIsValid(fcinfo->fncollation))
		fcinfo->fncollation = C_COLLATION_OID;

	datalen = VARSIZE(data) - VARHDRSZ;
	resultlen = datalen << 1;

	if (resultlen < (int) (MaxAllocSize - VARHDRSZ))
	{
		ans = (text *) palloc(VARHDRSZ + resultlen);
		ans_len = hex_encode(VARDATA(data), datalen, VARDATA(ans));
		/* Make this FATAL 'cause we've trodden on memory ... */
		if (ans_len > resultlen)
			ereport(
				FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED), errmsg("overflow - encode estimate too small")));

		SET_VARSIZE(ans, VARHDRSZ + ans_len);

		out_string = ToUpperForRaw(VARDATA_ANY(ans), VARSIZE_ANY_EXHDR(ans), PG_GET_COLLATION());
		pfree(ans);
		ans = NULL;
	}
	else
	{
		ereport(
			ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY), errmsg("blob length: %d ,out of memory", resultlen)));
	}
	PG_RETURN_CSTRING(out_string);
}

Datum
rawmodin(PG_FUNCTION_ARGS)
{
	ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32     *tl = NULL;
	int        n;

	/* RAW is used to store variable-length binary data. The maximum size is 2000 bytes */
	tl = ArrayGetIntegerTypmods(ta, &n);

	if (tl && (*tl > MAX_RAW_SIZE))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		         errmsg("specified length too long for its datatype.")));
	}

	PG_RETURN_INT32(anychar_typmodin(ta, "raw"));
}

Datum
rawmodout(PG_FUNCTION_ARGS)
{
	int32 typmod = PG_GETARG_INT32(0);

	PG_RETURN_CSTRING(anychar_typmodout(typmod));
}

/*
 * Implements interface of rawtohex(text)
 */
Datum
rawtotext(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	Datum cstring_result;
	Datum result;

	cstring_result = DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), arg1);
	result = DirectFunctionCall1(textin, cstring_result);
	PG_RETURN_TEXT_P(result);
}

/*
 *  Implements interface of hextoraw(raw)
 */
Datum
texttoraw(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	Datum result;
	Datum cstring_arg1;

	cstring_arg1 = DirectFunctionCall1(textout, arg1);
	result = DirectFunctionCall1(rawin, cstring_arg1);

	PG_RETURN_BYTEA_P(result);
}

/*
 * The behavior of this function is slightly more complex, so let's start with a set of use cases
 * eg.
 * select rawtohex(01) from dual;
 * C1 02
 * select rawtohex(02) from dual;
 * C1 03
 * select rawtohex(11) from dual;
 * C1 0C
 * select rawtohex(12) from dual;
 * C1 0D
 * select rawtohex(01.01) from dual;
 * C1 02 02
 * select rawtohex(02.02) from dual;
 * C1 03 03
 * select rawtohex(11.11) from dual;
 * C1 0C 0C
 *
 * The result is a hexadecimal number string.
 * Starting with 0xC0 plus the part1 of the word's number,
 * then the decimal numbers are then converted to hexadecimal one by one and appended to the main
 * string
 *
 * A formula to calculate：
 * (0xC0 + part1_words) + (hex number + 1)...
 */
static char *
ProcessPositiveNum(char *part1, int part1_len, char *part2, int part2_len)
{
	const int prefix = 0xC0;
	const int per_word_len = 2;
	int       word_decimal;
	int       i;
	char     *head = NULL;
	char     *dst = NULL;

	/* contain prefix word len C0 */
	dst = (char *) palloc0((part1_len + part2_len + per_word_len + 1) * sizeof(char));
	head = dst;

	/* step1: set prefix, number of words */
	sprintf(head, "%02x", prefix + (part1_len >> 1));
	head += strlen(head);

	/* step2: transform part1 number to hexadecimal */
	for (i = 0; (i + 1 < part1_len) && *(part1 + i + 1) != '\0'; i += 2)
	{
		word_decimal = VAL(*(part1 + i)) * 10 + VAL(*(part1 + i + 1));
		sprintf(head, "%02x", word_decimal + 1);
		head += per_word_len;
	}

	/* step3: transform part2 number to hexadecimal */
	for (i = 0; (i + 1 < part2_len) && *(part2 + i + 1) != '\0'; i += 2)
	{
		word_decimal = VAL(*(part2 + i)) * 10 + VAL(*(part2 + i + 1));
		sprintf(head, "%02x", word_decimal + 1);
		head += per_word_len;
	}

	return dst;
}

/*
 * The behavior of this function is slightly more complex, so let's start with a set of use cases
 * eg.
 * select rawtohex(-01) from dual;
 * 3E 64 66
 * select rawtohex(-02) from dual;
 * 3E 63 66
 * select rawtohex(-11) from dual;
 * 3E 5A 66
 * select rawtohex(-12) from dual;
 * 3E 59 66
 * select rawtohex(-01.01) from dual;
 * 3E 64 64 66
 * select rawtohex(-02.02) from dual;
 * 3E 64 64 66
 * select rawtohex(-11.11) from dual;
 * 3E 5A 5A 66
 *
 * The result is a hexadecimal number string with two arrays a group
 * Starting with 0x3F sub the part1 of the word's number,ending with 0x66.
 *
 * A formula to calculate：
 * (0x3F - part1_words) + (0x65 - decimal number) ... + 0x66
 */
static char *
ProcessNegativeNum(char *part1, int part1_len, char *part2, int part2_len)
{
	const int prefix = 0x3F, /* display number of digits */
		suffix = 0x66,       /* nagative suffix */
		word_base = 0x65, per_word_len = 2;
	int   word_decimal, i;
	char *head = NULL, *dst = NULL;

	/* contain prefix word len C0 */
	dst = (char *) palloc0((part1_len + part2_len + per_word_len * 2 + 1) * sizeof(char));
	head = dst;

	/* step1: set prefix, number of words */
	sprintf(head, "%02x", prefix - (part1_len >> 1));
	head += strlen(head);

	/* step2: transform part1 number to hexadecimal */
	for (i = 0; (i + 1 < part1_len) && *(part1 + i + 1) != '\0'; i += 2)
	{
		word_decimal = VAL(*(part1 + i)) * 10 + VAL(*(part1 + i + 1));
		sprintf(head, "%02x", word_base - word_decimal);
		head += per_word_len;
	}

	/* step3: transform part2 number to hexadecimal */
	for (i = 0; (i + 1 < part2_len) && *(part2 + i + 1) != '\0'; i += 2)
	{
		word_decimal = VAL(*(part2 + i)) * 10 + VAL(*(part2 + i + 1));
		sprintf(head, "%02x", word_base - word_decimal);
		head += per_word_len;
	}

	/* step4: set suffix, end of word */
	sprintf(head, "%02x", suffix);
	head += strlen(head);

	return dst;
}

/*
 * Parse raw number, eg. part1.part2
 */
static void
ParseRawNumber(char *src, char **part1, int *part1_len, char **part2, int *part2_len)
{
	char *pos = NULL, *out_part1 = NULL, *out_part2 = NULL;
	int   src_len = strlen(src), len1, len2, new_part1_len = 0, new_part2_len = 0;

	/* A special case: input 0 */
	if (src_len == 1 && *src == '0')
	{
		return;
	}

	/* step1: find the pos of '.' */
	pos = strchr(src, '.');
	if (pos == NULL)
	{
		len1 = src_len;
		len2 = 0;
	}
	else
	{
		len1 = pos - src;
		len2 = src_len - len1 - 1;
	}

	if (ORA_MODE && len1 == 0)
		len1 = 1;

	/* will never happened */
	if (len1 <= 0 || len2 < 0)
		elog(ERROR, "ParseRawNumber: invalid params len1[%d], len2[%d]", len1, len2);

	/* step2: complete 0 to the left of part1 */
	if (len1 == 1 && (*src == '0' || *src == '.'))
	{
		/* we ignore single zero. eg, select rawtohex(-0.15) */
		new_part1_len = 0;
	}
	else
	{
		if (len1 & 1)
		{
			new_part1_len = len1 + 1;
			out_part1 = (char *) palloc0((new_part1_len + 1) * sizeof(char));
			out_part1[0] = '0';
			strncpy(out_part1 + 1, src, len1);
		}
		else
		{
			new_part1_len = len1;
			out_part1 = (char *) palloc0((new_part1_len + 1) * sizeof(char));
			strncpy(out_part1, src, len1);
		}
	}

	/* step2: complete 0 to the right of part2 */
	if (len2 > 0)
	{
		if (len2 & 1)
		{
			new_part2_len = len2 + 1;
			out_part2 = (char *) palloc0((new_part2_len + 1) * sizeof(char));
			strncpy(out_part2, pos + 1, len2);
			out_part2[len2] = '0';
		}
		else
		{
			new_part2_len = len2;
			out_part2 = (char *) palloc0((new_part2_len + 1) * sizeof(char));
			strncpy(out_part2, pos + 1, len2);
		}
	}

	*part1 = out_part1;
	*part2 = out_part2;
	*part1_len = new_part1_len;
	*part2_len = new_part2_len;
}

static Datum
NumberOutToHex(char *src)
{
	char *dst = NULL;
	char *part1 = NULL;
	char *part2 = NULL;
	int   src_len = 0;
	int   part1_len = 0;
	int   part2_len = 0;
	Datum result;
	bool  is_negative = false;

	if (*src == '-')
	{
		is_negative = true;
		src += 1;
	}

	src_len = strlen(src);

	/* step1: parse float/int number */
	ParseRawNumber(src, &part1, &part1_len, &part2, &part2_len);

	if ((part1 != NULL && strlen(part1) != part1_len) ||
		(part2 != NULL && strlen(part2) != part2_len))
	{
		elog(ERROR,
			 "numrawtotext: invalid params part1_reallen[%d], par1_len[%d], part2_reallen[%d], "
			 "part2_len[%d]",
			 part1 ? (int) strlen(part1) : 0,
			 part1_len,
			 part2 ? (int) strlen(part2) : 0,
			 part2_len);
	}

	/* step2: transform different input number */
	if (is_negative)
	{
		dst = ProcessNegativeNum(part1, part1_len, part2, part2_len);
	}
	else
	{
		if (src_len == 1 && *src == '0')
		{
			dst = pstrdup("80");
		}
		else
		{
			dst = ProcessPositiveNum(part1, part1_len, part2, part2_len);
		}
	}

	result = DirectFunctionCall1Coll(upper, C_COLLATION_OID, CStringGetTextDatum(dst));

	if (part1)
	{
		pfree(part1);
	}
	if (part2)
	{
		pfree(part2);
	}
	pfree(dst);
	return result;
}

/*
 * Function: numrawtotext
 * Input: int/float
 * Return:text
 * Description: cast raw(int/float) to text
 */
Datum
numrawtotext(PG_FUNCTION_ARGS)
{
	return NumberOutToHex(
		DatumGetCString(DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0))));
}

/*
 * Function: float4rawtohex
 * Input:  float4
 * Return: hex
 * Description: cast raw(float4) to hex
 */
Datum
float4rawtohex(PG_FUNCTION_ARGS)
{
	return NumberOutToHex(
		DatumGetCString(DirectFunctionCall1(float4out, PG_GETARG_DATUM(0))));
}

/*
 * Function: float8rawtohex
 * Input:  float8
 * Return: hex
 * Description: cast raw(float8) to hex
 */
Datum
float8rawtohex(PG_FUNCTION_ARGS)
{
	return NumberOutToHex(
		DatumGetCString(DirectFunctionCall1(float8out, PG_GETARG_DATUM(0))));
}

Datum
numerictoraw(PG_FUNCTION_ARGS)
{
	text *text_num;
	Datum tmp = DirectFunctionCall1(numeric_out, PG_GETARG_DATUM(0));

	text_num = cstring_to_text(DatumGetCString(tmp));
	return DirectFunctionCall1(texttoraw, (Datum) text_num);
}

static char *
ProcessRawstrPre(char *str)
{
	int   slen, newlen, backslash_cnt = 0;
	char *head = str, *new_str = NULL;

	if (str == NULL || (str && *str == '\0'))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid params str is null.")));
	}

	slen = strlen(str);
	while (head && *head != '\0')
	{
		if (*head == '\\')
		{
			backslash_cnt++;
		}
		head++;
	}

	if (backslash_cnt == 0)
	{
		new_str = pstrdup(str);
	}
	else
	{
		char *new_head = NULL;

		head = str;
		newlen = slen + backslash_cnt;
		new_str = (char *) palloc0(newlen + 1);
		new_head = new_str;

		while (head && *head != '\0')
		{
			if (*head == '\\')
			{
				*(new_head++) = '\\';
			}

			*(new_head++) = *(head++);
		}
	}

	return new_str;
}

/*
 * convert string to hexadecimal
 */
Datum
rawtohex(PG_FUNCTION_ARGS)
{
	char *      str = TextDatumGetCString(PG_GETARG_TEXT_P(0)), *new_str = NULL;
	const char *fmt = "HEX";
	text *      result_upper = NULL, *result = NULL;

	/* handle backslashes */
	new_str = ProcessRawstrPre(str);

	result =
		DatumGetTextP(DirectFunctionCall2(binary_encode,
	                                      DirectFunctionCall1(byteain, CStringGetDatum(new_str)),
	                                      DirectFunctionCall1(textin, CStringGetDatum(fmt))));

	result_upper = DatumGetTextP(
		DirectFunctionCall1Coll(upper, C_COLLATION_OID, PointerGetDatum(result)));

	pfree(new_str);
	pfree(result);
	PG_RETURN_TEXT_P(result_upper);
}

Datum
raw_larger(PG_FUNCTION_ARGS)
{
	/*
	 * If not specified, use the default sorting strategy.
	 */
	if (!OidIsValid(fcinfo->fncollation))
		fcinfo->fncollation = C_COLLATION_OID;

	return text_larger(fcinfo);
}

Datum
raw_smaller(PG_FUNCTION_ARGS)
{
	/*
	 * If not specified, use the default sorting strategy.
	 */
	if (!OidIsValid(fcinfo->fncollation))
		fcinfo->fncollation = C_COLLATION_OID;

	return text_smaller(fcinfo);
}

Datum
raweq(PG_FUNCTION_ARGS)
{
	return byteaeq(fcinfo);
}

Datum
rawne(PG_FUNCTION_ARGS)
{
	return byteane(fcinfo);
}

Datum
rawlt(PG_FUNCTION_ARGS)
{
	return bytealt(fcinfo);
}

Datum
rawle(PG_FUNCTION_ARGS)
{
	return byteale(fcinfo);
}

Datum
rawgt(PG_FUNCTION_ARGS)
{
	return byteagt(fcinfo);
}

Datum
rawge(PG_FUNCTION_ARGS)
{
	return byteage(fcinfo);
}

Datum
rawcmp(PG_FUNCTION_ARGS)
{
	return byteacmp(fcinfo);
}

Datum
rawcat(PG_FUNCTION_ARGS)
{
	return byteacat(fcinfo);
}

Datum
rawlike(PG_FUNCTION_ARGS)
{
	return textlike(fcinfo);
}

Datum
rawnlike(PG_FUNCTION_ARGS)
{
	return textnlike(fcinfo);
}
