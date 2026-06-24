#include "c.h"
#include "postgres.h"
#include <stdlib.h>
#include <locale.h>
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "common/md5.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/nabstime.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "utils/guc.h"
#include "utils/formatting.h"
#include "opentenbase_ora/opentenbase_ora.h"
#include "mb/pg_wchar.h"
#ifdef __OPENTENBASE_C__
#endif
#include "miscadmin.h"

#define DEFAULT_NAMESPACE "USERENV"
#define XS_SESSION_NAMESPACE "XS$SESSION"
#define DEFAULT_RETURN_BUFF_LENGTH 256
#define MIN_RETURN_BUFF_LENGTH 1
#define MAX_RETURN_BUFF_LENGTH 4000

static text *_nls_get_collate(text *locale);
static text *_nls_run_strxfrm(text *string, text *locale);
static Datum compute_orcl_hash_extended(Oid type, Datum value, uint32 seed);
static int64 orcl_ora_hash(Oid type, Datum value, int64 max_bucket, int64 seed);

/*
 * Source code for nlssort is taken from postgresql-nls-string
 * package by Jan Pazdziora
 */

static char *lc_collate_cache = NULL;
static int multiplication = 1;

/* regexp_replace('NLS_Sort = SCHINESE_PINYIN_M', '(^\s*|\s*NLS_SORT\s*=\s*|\s*$)', '', 'gi'); */
static text*
_nls_get_collate(text *locale)
{
	pg_re_flags flags;
	regex_t    *re;
    text* result = NULL;

	if (locale == NULL || VARSIZE_ANY_EXHDR(locale) < 1)
		return locale;

	parse_re_flags(&flags, cstring_to_text("gi"));

	re = RE_compile_and_cache(cstring_to_text("(^\\s*|\\s*NLS_SORT\\s*=\\s*|\\s*$)"), flags.cflags, C_COLLATION_OID);

	result = replace_text_regexp(locale, (void *) re, cstring_to_text(""), flags.glob);

	return result;
}

Datum
orcl_set_nls_sort(PG_FUNCTION_ARGS)
{
	text *arg = PG_GETARG_TEXT_P(0);
	arg = _nls_get_collate(arg);

	set_config_option("nls_sort_locale", text_to_cstring(arg),
				  PGC_SUSET, PGC_S_SESSION,
				  GUC_ACTION_SET, true, 0, 0);

	PG_RETURN_VOID();
}

static text*
_nls_run_strxfrm(text *string, text *locale)
{
	char *string_str;
	int string_len;

	char *locale_str = NULL;
	int locale_len = 0;

	text *result;
	char *tmp = NULL;
	size_t size = 0;
	size_t rest = 0;
	int changed_locale = 0;

	/*
	 * Save the default, server-wide locale setting.
	 * It should not change during the life-span of the server so it
	 * is safe to save it only once, during the first invocation.
	 */
	if (!lc_collate_cache)
	{
		if ((lc_collate_cache = setlocale(LC_COLLATE, NULL)))
			/* Make a copy of the locale name string. */
#ifdef _MSC_VER
			lc_collate_cache = _strdup(lc_collate_cache);
#else
			lc_collate_cache = strdup(lc_collate_cache);
#endif
		if (!lc_collate_cache)
			elog(ERROR, "failed to retrieve the default LC_COLLATE value");
	}

	/*
	 * To run strxfrm, we need a zero-terminated strings.
	 */
	string_len = VARSIZE_ANY_EXHDR(string);
	if (string_len < 0)
		return NULL;
	string_str = palloc(string_len + 1);
	memcpy(string_str, VARDATA_ANY(string), string_len);

	*(string_str + string_len) = '\0';

	if (locale)
	{
		locale = _nls_get_collate(locale);
		locale_len = VARSIZE_ANY_EXHDR(locale);
	}

	if (locale == NULL)
	{
		elog(ERROR, "get collate is null");
	}

	/*
	 * If different than default locale is requested, call setlocale.
	 */
	if (locale_len > 0
		&& (strncmp(lc_collate_cache, VARDATA_ANY(locale), locale_len)
			|| *(lc_collate_cache + locale_len) != '\0'))
	{
		locale_str = text_to_cstring(locale);

		/*
		 * Try to set correct locales.
		 * If setlocale failed, we know the default stayed the same,
		 * co we can safely elog.
		 */
		/* adapt opentenbase_ora SCHINESE_PINYIN_M keyword set default encoding zh_CN.GB18030 */
		/* TODO: support SCHINESE_STROKE_M SCHINESE_RADICAL_M by local locale */
		if ((strcmp(locale_str,"SCHINESE_PINYIN_M") == 0) ||
			(strcmp(locale_str,"SCHINESE_STROKE_M") == 0) ||
			(strcmp(locale_str,"SCHINESE_RADICAL_M") == 0))
		{
			/* local no support encoding */
			if(!setlocale(LC_COLLATE, "zh_CN"))
				elog(ERROR,"failed to set the requested LC_COLLATE value zh_CN.GB18030");
		}
		else if (!setlocale(LC_COLLATE, locale_str))
			elog(ERROR, "failed to set the requested LC_COLLATE value [%s]", locale_str);

		changed_locale = 1;
	}

	/*
	 * We do TRY / CATCH / END_TRY to catch ereport / elog that might
	 * happen during palloc. Ereport during palloc would not be
	 * nice since it would leave the server with changed locales
	 * setting, resulting in bad things.
	 */
	PG_TRY();
	{

		/*
		 * Text transformation.
		 * Increase the buffer until the strxfrm is able to fit.
		 */
		size = string_len * multiplication + 1;
		tmp = palloc(size + VARHDRSZ);

		rest = strxfrm(tmp + VARHDRSZ, string_str, size);
		while (rest >= size)
		{
			pfree(tmp);
			size = rest + 1;
			tmp = palloc(size + VARHDRSZ);
			rest = strxfrm(tmp + VARHDRSZ, string_str, size);
			/*
			 * Cache the multiplication factor so that the next
			 * time we start with better value.
			 */
			if (string_len)
				multiplication = (rest / string_len) + 2;
		}
	}
	PG_CATCH ();
	{
		if (changed_locale) {
			/*
			 * Set original locale
			 */
			if (!setlocale(LC_COLLATE, lc_collate_cache))
				elog(FATAL, "failed to set back the default LC_COLLATE value [%s]", lc_collate_cache);
		}
	}
	PG_END_TRY ();

	if (changed_locale)
	{
		/*
		 * Set original locale
		 */
		if (!setlocale(LC_COLLATE, lc_collate_cache))
			elog(FATAL, "failed to set back the default LC_COLLATE value [%s]", lc_collate_cache);
		pfree(locale_str);
	}
	pfree(string_str);

	/*
	 * If the multiplication factor went down, reset it.
	 */
	if (string_len && rest < string_len * multiplication / 4)
		multiplication = (rest / string_len) + 1;

#ifdef _PG_ORCL_
	/* fix: Dereference of null pointer */
	AssertArg(tmp);
#endif
	result = (text *) tmp;
	SET_VARSIZE(result, rest + VARHDRSZ);
	return result;
}

Datum
orcl_nlssort(PG_FUNCTION_ARGS)
{
	text *locale;
	text *result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	if (PG_ARGISNULL(1))
	{
		if (nls_sort_locale != NULL)
			locale = cstring_to_text(nls_sort_locale);
		else
		{
			locale = palloc(VARHDRSZ);
			SET_VARSIZE(locale, VARHDRSZ);
		}
	}
	else
	{
		locale = PG_GETARG_TEXT_PP(1);
	}

	result = _nls_run_strxfrm(PG_GETARG_TEXT_PP(0), locale);

	if (! result)
		PG_RETURN_NULL();

	PG_RETURN_BYTEA_P(result);
}

/*
 * Returns current version etc, PostgreSQL 9.6, PostgreSQL 10, ..
 */
Datum
orcl_get_major_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PACKAGE_STRING));
}

/*
 * Returns major version number 9.5, 9.6, 10, 11, ..
 */
Datum
orcl_get_major_version_num(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PG_MAJORVERSION));
}

/*
 * Returns version number string - 9.5.1, 10.2, ..
 */
Datum
orcl_get_full_version_num(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION));
}

/*
 * 32bit, 64bit
 */
Datum
orcl_get_platform(PG_FUNCTION_ARGS)
{
#ifdef USE_FLOAT8_BYVAL
	PG_RETURN_TEXT_P(cstring_to_text("64bit"));
#else
	PG_RETURN_TEXT_P(cstring_to_text("32bit"));
#endif
}

/*
 * Production | Debug
 */
Datum
orcl_get_status(PG_FUNCTION_ARGS)
{
#ifdef USE_ASSERT_CHECKING
	PG_RETURN_TEXT_P(cstring_to_text("Debug"));
#else
	PG_RETURN_TEXT_P(cstring_to_text("Production"));
#endif
}

Datum
orcl_nvl(PG_FUNCTION_ARGS)
{
	if (!PG_ARGISNULL(0))
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	if (!PG_ARGISNULL(1))
		PG_RETURN_DATUM(PG_GETARG_DATUM(1));

	PG_RETURN_NULL();
}

Datum
orcl_nvl2(PG_FUNCTION_ARGS)
{
	if (!PG_ARGISNULL(0))
	{
		if (!PG_ARGISNULL(1))
			PG_RETURN_DATUM(PG_GETARG_DATUM(1));
	}
	else
	{
		if (!PG_ARGISNULL(2))
			PG_RETURN_DATUM(PG_GETARG_DATUM(2));
	}

	PG_RETURN_NULL();
}

Datum
orcl_lnnvl(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(true);

	PG_RETURN_BOOL(!PG_GETARG_BOOL(0));
}

static void
appendDatum(StringInfo str, const void *ptr, size_t length, int format, int start_pos, int printlen)
{
	if (!PointerIsValid(ptr))
    {
        appendStringInfoChar(str, ':');
    }
	else
	{
		const unsigned char *s = (const unsigned char *) ptr;
		const char *formatstr;
		size_t	i;

		switch (format)
		{
			case 8:
            case 1008:
				formatstr = "%ho";
				break;
			case 10:
            case 1010:
				formatstr = "%hu";
				break;
			case 16:
            case 1016:
				formatstr = "%hx";
				break;
			case 17:
            case 1017:
				formatstr = "%hc";
				break;
			default:
				elog(ERROR, "unknown format ( %d ) which not in ( 8, 10, 16, 17, 1008, 1010, 1016, 1017 ) ", format);
				formatstr  = NULL; 	/* quite compiler */
		}

		/* append a byte array with the specified format */
		for (i = start_pos; i < start_pos + printlen; i++)
		{
			if (i > start_pos)
				appendStringInfoChar(str, ',');

			/* print only ANSI visible chars */
			if ((format == 17 || format == 1017) && (iscntrl(s[i]) || !isascii(s[i])))
				appendStringInfoChar(str, '?');
			else
				appendStringInfo(str, formatstr, s[i]);
		}
	}
}

Datum
orcl_dump(PG_FUNCTION_ARGS)
{
	Oid		valtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	List	*args;
	int16	typlen;
	bool	typbyval;
	Size	length;
	Datum	value;
	int		format;
    int start_pos = 0;
    int print_len = 0;
	StringInfoData	str;

	if (!fcinfo->flinfo || !fcinfo->flinfo->fn_expr)
		elog(ERROR, "function is called from invalid context");

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_TEXT_P(cstring_to_text("NULL"));
	}

	value = PG_GETARG_DATUM(0);
	format = PG_GETARG_IF_EXISTS(1, INT32, 10);

	args = ((FuncExpr *) fcinfo->flinfo->fn_expr)->args;
	valtype = exprType((Node *) list_nth(args, 0));

	get_typlenbyval(valtype, &typlen, &typbyval);
	length = datumGetSize(value, typbyval, typlen);

    start_pos = PG_GETARG_IF_EXISTS(2, INT32, 1);
    if (start_pos == 0)
    {
        start_pos = 1;
    }
    else if (abs(start_pos) > length)
    {
        PG_RETURN_NULL();
    }
    else if (start_pos < 0)
    {
        start_pos = start_pos + length + 1;
    }
    else
    {
        /* do nothing */
    }
    Assert((start_pos >= 1) && (start_pos <= length) );

    print_len = PG_GETARG_IF_EXISTS(3, INT32, length - start_pos + 1);
    if ((print_len == 0) || (abs(print_len) > (length - start_pos + 1)))
    {
        print_len = length - start_pos + 1;
    }
    else
    {
        print_len = abs(print_len);
    }
    Assert(print_len > 0 && print_len <= length - start_pos + 1);

	initStringInfo(&str);
    if (format > 1000)
    {
        appendStringInfo(&str, "Typ=%d Len=%d CharacterSet=%s: ", valtype, (int) length, GetDatabaseEncodingName());
    } else {
        appendStringInfo(&str, "Typ=%d Len=%d: ", valtype, (int) length);
    }

	if (!typbyval)
	{
		appendDatum(&str, DatumGetPointer(value), length, format, start_pos - 1, print_len);
	}
	else if (length <= 1)
	{
		char	v = DatumGetChar(value);
		appendDatum(&str, &v, sizeof(char), format, start_pos - 1, print_len);
	}
	else if (length <= 2)
	{
		int16	v = DatumGetInt16(value);
		appendDatum(&str, &v, sizeof(int16), format, start_pos - 1, print_len);
	}
	else if (length <= 4)
	{
		int32	v = DatumGetInt32(value);
		appendDatum(&str, &v, sizeof(int32), format, start_pos - 1, print_len);
	}
	else
	{
		int64	v = DatumGetInt64(value);
		appendDatum(&str, &v, sizeof(int64), format, start_pos - 1, print_len);
	}

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/********************************************************************
 *
 * nls_lower
 *
 * Syntax:
 *
 *	 text nls_lower(text string,'nls_sort=value')
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to lowercase.
 *
 ********************************************************************/
Datum
orcl_lower(PG_FUNCTION_ARGS)
{
	text	   *in_string = PG_GETARG_TEXT_PP(0);
	char	   *out_string;
	text	   *result;
	text	   *locale;
	char 	   *locale_str = NULL;
	Oid 	   collid = DEFAULT_COLLATION_OID;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	/* is use orcl_set_nls_sort function set goalbal nls_params */
	if (PG_ARGISNULL(1))
	{
		if (nls_sort_locale != NULL)
			locale = cstring_to_text(nls_sort_locale);
		else
		{
			locale = palloc(VARHDRSZ);
			SET_VARSIZE(locale, VARHDRSZ);
		}
	}
	else
	{
		locale = PG_GETARG_TEXT_PP(1);
	}


	if (locale)
	{
		locale = _nls_get_collate(locale);
	}

	if (locale == NULL)
	{
		elog(ERROR, "get collate is null");
	}

	locale_str = text_to_cstring(locale);

	/* adapt opentenbase_ora SCHINESE_PINYIN_M keyword set default encoding zh_CN.GB18030 */
	/* TODO: support SCHINESE_STROKE_M SCHINESE_RADICAL_M by local locale */
	if ((strcmp(locale_str,"SCHINESE_PINYIN_M") == 0) ||
		(strcmp(locale_str,"SCHINESE_STROKE_M") == 0) ||
		(strcmp(locale_str,"SCHINESE_RADICAL_M") == 0))
	{
		/* get collation oid */
		collid = CollationGetCollid("zh_CN");
	}
	/*
	 * Try to set correct locales.
	 * If setlocale failed, we know the default stayed the same,
	 * co we can safely elog.
	 */
	else
	{
		/* get collation oid */
		collid = CollationGetCollid(locale_str);
	}

	if (!OidIsValid(collid))
	{
		collid = DEFAULT_COLLATION_OID;
	}
	out_string = str_tolower(VARDATA_ANY(in_string),
							 VARSIZE_ANY_EXHDR(in_string),
							 collid);

	result = cstring_to_text(out_string);

	pfree(out_string);

	pfree(locale_str);

	PG_RETURN_TEXT_P(result);
}


/********************************************************************
 *
 * nls_upper
 *
 * Syntax:
 *
 *	 text nls_upper(text string,'nls_sort=value')
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to uppercase.
 *
 ********************************************************************/

Datum
orcl_upper(PG_FUNCTION_ARGS)
{
	text	   *in_string = PG_GETARG_TEXT_PP(0);
	char	   *out_string;
	text	   *result;
	text	   *locale;
	char 	   *locale_str = NULL;
	Oid 	   collid = DEFAULT_COLLATION_OID;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	/* is use orcl_set_nls_sort function set goalbal nls_params */
	if (PG_ARGISNULL(1))
	{
		if (nls_sort_locale != NULL)
			locale = cstring_to_text(nls_sort_locale);
		else
		{
			locale = palloc(VARHDRSZ);
			SET_VARSIZE(locale, VARHDRSZ);
		}
	}
	else
	{
		locale = PG_GETARG_TEXT_PP(1);
	}


	if (locale)
	{
		locale = _nls_get_collate(locale);
	}

	if (locale == NULL)
	{
		elog(ERROR, "get collate is null");
	}

	locale_str = text_to_cstring(locale);

	/* adapt opentenbase_ora SCHINESE_PINYIN_M keyword set default encoding zh_CN.GB18030 */
	/* TODO: support SCHINESE_STROKE_M SCHINESE_RADICAL_M by local locale */
	if ((strcmp(locale_str,"SCHINESE_PINYIN_M") == 0) ||
		(strcmp(locale_str,"SCHINESE_STROKE_M") == 0) ||
		(strcmp(locale_str,"SCHINESE_RADICAL_M") == 0))
	{
		/* get collation oid */
		collid = CollationGetCollid("zh_CN");
	}
	/*
	 * Try to set correct locales.
	 * If setlocale failed, we know the default stayed the same,
	 * co we can safely elog.
	 */
	else
	{
		/* get collation oid */
		collid = CollationGetCollid(locale_str);
	}

	if (!OidIsValid(collid))
	{
		collid = DEFAULT_COLLATION_OID;
	}
	out_string = str_toupper(VARDATA_ANY(in_string),
							 VARSIZE_ANY_EXHDR(in_string),
							 collid);

	result = cstring_to_text(out_string);

	pfree(out_string);

	pfree(locale_str);

	PG_RETURN_TEXT_P(result);

}

/********************************************************************
 *
 * nls_initcap
 *
 * Syntax:
 *
 *	 text nls_initcap(text string,'nls_sort=value')
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to uppercase.
 *
 ********************************************************************/
Datum
orcl_initcap(PG_FUNCTION_ARGS)
{
	text	   *in_string = PG_GETARG_TEXT_PP(0);
	char	   *out_string;
	text	   *result;
	text	   *locale;
	char 	   *locale_str = NULL;
	Oid 	   collid = DEFAULT_COLLATION_OID;

	/* Init default encoding */
	if(!setlocale(LC_COLLATE, NULL))
	{
		elog(ERROR, "failed to set the requested LC_COLLATE value default");
	}

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* is use orcl_set_nls_sort function set goalbal nls_params */
	if (PG_ARGISNULL(1))
	{
		if (nls_sort_locale != NULL)
			locale = cstring_to_text(nls_sort_locale);
		else
		{
			locale = palloc(VARHDRSZ);
			SET_VARSIZE(locale, VARHDRSZ);
		}
	}
	else
	{
		locale = PG_GETARG_TEXT_PP(1);
	}

	if (locale)
	{
		locale = _nls_get_collate(locale);
	}

	if (locale == NULL)
	{
		elog(ERROR, "get collate is null");
	}

	locale_str = text_to_cstring(locale);

	/* adapt opentenbase_ora SCHINESE_PINYIN_M keyword set default encoding zh_CN.GB18030 */
	/* TODO: support SCHINESE_STROKE_M SCHINESE_RADICAL_M by local locale */
	if( (strcmp(locale_str,"SCHINESE_PINYIN_M") == 0) ||
		(strcmp(locale_str,"SCHINESE_STROKE_M") == 0) ||
		(strcmp(locale_str,"SCHINESE_RADICAL_M") == 0))
	{
		/* local no support encoding */
		if(!setlocale(LC_COLLATE, "zh_CN"))
			elog(ERROR, "failed to set the requested LC_COLLATE value zh_CN.GB18030");

		/* get collation oid */
		collid = CollationGetCollid("zh_CN");
		elog(DEBUG5, "collation zh_CN oid is %d", collid);
	}
	/*
	 * Try to set correct locales.
	 * If setlocale failed, we know the default stayed the same,
	 * co we can safely elog.
	 */
	else
	{
		if (!setlocale(LC_COLLATE, locale_str))
			elog(ERROR, "failed to set the requested LC_COLLATE value [%s]", locale_str);

		/* get collation oid */
		collid = CollationGetCollid(locale_str);
		if (!OidIsValid(collid))
		{
			elog(DEBUG2, "collation %s oid is %d", locale_str, collid);

			/* use default collation oid */
			collid = DEFAULT_COLLATION_OID;
		}
		elog(DEBUG5, "collation %s oid is %d", locale_str, collid);
	}

	out_string = str_initcap(VARDATA_ANY(in_string),
							 VARSIZE_ANY_EXHDR(in_string),
							 collid);

	result = cstring_to_text(out_string);

	pfree(out_string);

	pfree(locale_str);

	PG_RETURN_TEXT_P(result);
}

/*
  *ora_hash function define: ora_hash(expr, [max_bucket, [seed]])
 * ora_hash(expr)	call orcl_ora_hash1, max_bucket is default(MAX_UINT32), seed is default(0);
 * ora_hash(expr, max_bucket) call orcl_ora_hash2, seed is default(MAX_UINT32);
 * ora_hash(expr, max_bucket, seed) call orcl_ora_hash3(expr, max_bucket, seed);
 *
 * expr is any type expression;
 * max_bucket: the maximum bucket value returned by the hash function
 *	0~4294967295, default is 4294967295
 * seed:produce many different results for the same set of data
 *	0~4294967295, default is 0
 */
 #define ORA_HASH_MAX_BUCKET (4294967295)
 #define ORA_HASH_MAX_SEED (4294967295)
 #define ORA_HASH_DEF_BUCKET (4294967295)
 #define ORA_HASH_DEF_SEED (0)

static Datum
compute_orcl_hash_extended(Oid type, Datum value, uint32 seed)
{
	int32	tmp32;
	char		tmpch;
	Datum	dat_seed = Int64GetDatum(seed);

	switch (type)
	{
		case INT2OID:
			return DirectFunctionCall2(hashint2extended, value, dat_seed);
		case INT4OID:
			return DirectFunctionCall2(hashint4extended, value,  dat_seed);
		case INT8OID:
			return DirectFunctionCall2(hashint8extended, value, dat_seed);
		case OIDOID:
			return DirectFunctionCall2(hashoidextended, value, dat_seed);
		case BOOLOID:
			tmpch = DatumGetBool(value);
			return DirectFunctionCall2(hashcharextended, tmpch, dat_seed);
		case CHAROID:
			return DirectFunctionCall2(hashcharextended, value, dat_seed);
		case NAMEOID:
			return DirectFunctionCall2(hashnameextended, value, dat_seed);

		case VARCHAROID:
		case TEXTOID:
		case LONGOID:
		case VARCHAR2OID:
		case NVARCHAR2OID:
			return DirectFunctionCall2(hashtextextended, value, dat_seed);
		case OIDVECTOROID:
			return DirectFunctionCall2(hashoidvectorextended, value, dat_seed);
		case FLOAT4OID:
			return DirectFunctionCall2(hashfloat4extended, value, dat_seed);
		case FLOAT8OID:
			return DirectFunctionCall2(hashfloat8extended, value, dat_seed);
		case ABSTIMEOID:
			tmp32 = DatumGetAbsoluteTime(value);
			return DirectFunctionCall2(hashint4extended, tmp32, dat_seed);
		case RELTIMEOID:
			tmp32 = DatumGetRelativeTime(value);
			return DirectFunctionCall2(hashint4extended, tmp32, dat_seed);
		case CASHOID:
			return DirectFunctionCall2(hashint8extended, value, dat_seed);
		case BPCHAROID:
			return DirectFunctionCall2(hashbpcharextended, value, dat_seed);
		case BYTEAOID:
			return DirectFunctionCall2(hashvarlena, value, dat_seed);
		case DATEOID:
			tmp32 = DatumGetDateADT(value);
			return DirectFunctionCall2(hashint4extended, tmp32, dat_seed);
		case TIMEOID:
			return DirectFunctionCall2(time_hash_extended, value, dat_seed);
		case TIMESTAMPOID:
			return DirectFunctionCall2(timestamp_hash_extended, value, dat_seed);
		case TIMESTAMPTZOID:
			return DirectFunctionCall2(timestamp_hash_extended, value, dat_seed);
		case INTERVALOID:
			return DirectFunctionCall2(interval_hash_extended, value, dat_seed);
		case TIMETZOID:
			return DirectFunctionCall2(timetz_hash_extended, value, dat_seed);
		case NUMERICOID:
			return DirectFunctionCall2(hash_numeric_extended, value, dat_seed);
		case JSONBOID:
			return DirectFunctionCall2(jsonb_hash_extended, value, dat_seed);
		case RIDOID:
			return DirectFunctionCall2(rowid_hash_extended, value, dat_seed);
		default:
			ereport(ERROR, (errmsg("Unhandled datatype:%d "
				"for modulo or hash distribution in compute_hash", type)));
	}

	return (Datum)0;
 }

static int64
orcl_ora_hash(Oid type, Datum value, int64 max_bucket, int64 seed)
{
	Datum ret_val;

	if (0 == max_bucket)
	{
		return 0;
	}

	if (seed > ORA_HASH_MAX_SEED || seed < 0)
	{
		elog(ERROR, "illegal argument for function: seed");
	}

	if (max_bucket > ORA_HASH_MAX_BUCKET || max_bucket < 0)
	{
		elog(ERROR, "illegal argument for function: max_bucket");
	}

	ret_val = compute_orcl_hash_extended(type, value, (uint32)seed);

	return (uint32)(DatumGetUInt64(ret_val) % max_bucket);
}

/*
 * ora_hash(expr)
 * 	call orcl_ora_hash(expr, 4294967295, 0)
 */
Datum
orcl_ora_hash1(PG_FUNCTION_ARGS)
{
	Oid		valtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	Datum	value = PG_GETARG_DATUM(0);

	PG_RETURN_INT64(orcl_ora_hash(valtype,
									value,
									ORA_HASH_DEF_BUCKET,
									ORA_HASH_DEF_SEED));
}

/*
 * ora_hash(expr, max_bucket)
 * 	call orcl_ora_hash(expr, max_bucket, 0)
 */
Datum
orcl_ora_hash2(PG_FUNCTION_ARGS)
{
	Oid		valtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	Datum	value = PG_GETARG_DATUM(0);

	PG_RETURN_INT64(orcl_ora_hash(valtype,
									value,
									PG_GETARG_INT64(1),
									ORA_HASH_DEF_SEED));
}

/*
 * ora_hash(expr, max_bucket, seed)
 * 	call orcl_ora_hash(expr, max_bucket, seed)
 */
Datum
orcl_ora_hash3(PG_FUNCTION_ARGS)
{
	Oid		valtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	Datum	value = PG_GETARG_DATUM(0);

	PG_RETURN_INT64(orcl_ora_hash(valtype,
									value,
									PG_GETARG_INT64(1),
									PG_GETARG_INT64(2)));
}

/*
 * opentenbase_ora sys_context
 *
 * Port from 8a1466c114c6f39880471936a9bb19c6fd3a5318
 * in v5 authored by sigmalin.
 */
Datum
sys_context(PG_FUNCTION_ARGS)
{
	text *namespace_text = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *attribute_text = PG_GETARG_TEXT_PP_IF_NULL(1);
	char *namespace = NULL;
	char *attribute = NULL;
	uint32 length = DEFAULT_RETURN_BUFF_LENGTH;
	struct Port *port = MyProcPort;

	if (namespace_text == NULL || attribute_text == NULL)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	if (!PG_ARGISNULL(2))
	{
		/* if there is a legal length set, use that setting */
		int len = DatumGetInt32(DirectFunctionCall1(numeric_int4, PG_GETARG_DATUM(2)));

		if (len >= MIN_RETURN_BUFF_LENGTH && len <= MAX_RETURN_BUFF_LENGTH)
		{
			length = len;
		}
	}

	namespace = text_to_cstring(namespace_text);
	attribute = text_to_cstring(attribute_text);

	/* if it is not the default namespace, return */
	if (pg_strcasecmp(namespace, DEFAULT_NAMESPACE) != 0)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	if (pg_strcasecmp(attribute, "CURRENT_USER") == 0)
	{
		if (port && port->user_name)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->user_name, Min(strlen(port->user_name), length)));
	}
	else if (pg_strcasecmp(attribute, "SESSION_USER") == 0)
	{
		char *sess_uname = GetUserNameFromId(GetSessionUserId(), false);

		if (sess_uname)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(sess_uname, Min(strlen(sess_uname), length)));
	}
	else if (pg_strcasecmp(attribute, "CURRENT_USERID") == 0)
	{
		char buf[DEFAULT_RETURN_BUFF_LENGTH];

		pg_ltoa(GetUserId(), buf);
		PG_RETURN_TEXT_P(cstring_to_text_with_len(buf, Min(strlen(buf), length)));
	}
	else if (pg_strcasecmp(attribute, "DB_NAME") == 0)
	{
		if (port && port->database_name)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->database_name, Min(strlen(port->database_name), length)));
	}
	else if (pg_strcasecmp(attribute, "IP_ADDRESS") == 0)
	{
		/* get client ip address */
		if (port && port->remote_host)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->remote_host, Min(strlen(port->remote_host), length)));
	}
	else if (pg_strcasecmp(attribute, "ISDBA") == 0)
	{
		char *isdba = superuser() ? "TRUE" : "FALSE";

		PG_RETURN_TEXT_P(cstring_to_text_with_len(isdba, Min(strlen(isdba), length)));
	}
	else if (pg_strcasecmp(attribute, "HOST") == 0)
	{
		char hostname[DEFAULT_RETURN_BUFF_LENGTH];

		if (!gethostname(hostname, sizeof(hostname)))
			PG_RETURN_TEXT_P(cstring_to_text_with_len(hostname, Min(strlen(hostname), length)));
	}
	else if(pg_strcasecmp(attribute, "INSTANCE_NAME") == 0)
	{
		if (PGXCNodeName)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(PGXCNodeName, Min(strlen(PGXCNodeName), length)));
	}
	else if(pg_strcasecmp(attribute, "INSTANCE") == 0)
	{
		char buf[DEFAULT_RETURN_BUFF_LENGTH];

		pg_ltoa(PGXCNodeId, buf);
		PG_RETURN_TEXT_P(cstring_to_text_with_len(buf, Min(strlen(buf), length)));
	}
	else if(pg_strcasecmp(attribute, "OPENTENBASE_ORA_HOME") == 0)
	{
		if (DataDir)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(DataDir, Min(strlen(DataDir), length)));
	}
	else if(pg_strcasecmp(attribute, "CURRENT_SCHEMA") == 0)
	{
		char *cur_schema = NULL;
		FunctionCallInfoData fcinfo;

		InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
		cur_schema = DatumGetCString(current_schema(&fcinfo));

		if (cur_schema)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(cur_schema, Min(strlen(cur_schema), length)));
	}

	PG_RETURN_TEXT_P(cstring_to_text(""));
}

/*
 * opentenbase_ora xs_sys_context
 *
 * Port from 9eb013b8b8ded3a18573d80f4469551beed8ade1
 * in v5 authored by guanhuawang.
 */
Datum
xs_sys_context(PG_FUNCTION_ARGS)
{
	text *namespace_text = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *attribute_text = PG_GETARG_TEXT_PP_IF_NULL(1);
	char *namespace = NULL;
	char *attribute = NULL;
	uint32 length = DEFAULT_RETURN_BUFF_LENGTH;
	struct Port *port = MyProcPort;
	char buf[DEFAULT_RETURN_BUFF_LENGTH];

	if (namespace_text == NULL || attribute_text == NULL)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	if (!PG_ARGISNULL(2))
	{
		/* if there is a legal length set, use that setting */
		int len = DatumGetInt32(DirectFunctionCall1(numeric_int4, PG_GETARG_DATUM(2)));

		if (len >= MIN_RETURN_BUFF_LENGTH && len <= MAX_RETURN_BUFF_LENGTH)
			length = len;
	}

	/* input unified to uppercase */
	namespace = text_to_cstring(namespace_text);
	attribute = text_to_cstring(attribute_text);

	if (pg_strcasecmp(namespace, XS_SESSION_NAMESPACE) != 0)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	/*
	 * if we cannot find corresponding keyword in opentenbase, we
	 * just return an empty string.
	*/
	if (pg_strcasecmp(attribute, "CREATE_BY") == 0)
	{
		if (port && port->user_name)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->user_name, Min(strlen(port->user_name), length)));
	}
	else if (pg_strcasecmp(attribute, "CREATE_TIME") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "COOKIE") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "CURRENT_XS_USER") == 0)
	{
		if (port && port->user_name)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->user_name, Min(strlen(port->user_name), length)));
	}
	else if (pg_strcasecmp(attribute, "CURRENT_XS_USER_GUID") == 0)
	{
		pg_ltoa(GetUserId(), buf);
		PG_RETURN_TEXT_P(cstring_to_text_with_len(buf, Min(strlen(buf), length)));
	}
	else if (pg_strcasecmp(attribute, "INACTIVITY_TIMEOUT") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "LAST_ACCESS_TIME") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "LAST_AUTHENTICATION_TIME") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "LAST_AUTHENTICATION_TIME") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "PROXY_GUID") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "SESSION_ID") == 0)
	{
#define MD5_HASH_LEN 33
		char hexsum[MD5_HASH_LEN];
		char new_str[NAMEDATALEN];
		uint32 global_sid_len = strlen(PGXCQueryId);

		if (0 == global_sid_len)
			snprintf(new_str, NAMEDATALEN, "%u", MyProcPid);
		else
			StrNCpy(new_str, PGXCQueryId, global_sid_len);

		if (!pg_md5_hash(new_str, global_sid_len, hexsum))
			elog(ERROR, "failed to calc md5 hash");
		else
		{
			hexsum[24] = 0;
			PG_RETURN_TEXT_P(cstring_to_text_upper(hexsum));
		}
	}
	else if (pg_strcasecmp(attribute, "SESSION_SIZE") == 0)
	{
	}
	else if (pg_strcasecmp(attribute, "SESSION_XS_USER") == 0)
	{
		if (port && port->user_name)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->user_name, Min(strlen(port->user_name), length)));
	}
	else if (pg_strcasecmp(attribute, "SESSION_XS_USER_GUID") == 0)
	{
		pg_ltoa(GetUserId(), buf);
		PG_RETURN_TEXT_P(cstring_to_text_with_len(buf, Min(strlen(buf), length)));
	}
	else if (pg_strcasecmp(attribute, "USERNAME") == 0)
	{
		if (port && port->user_name)
			PG_RETURN_TEXT_P(cstring_to_text_with_len(port->user_name, Min(strlen(port->user_name), length)));
	}
	else if (pg_strcasecmp(attribute, "USER_ID") == 0)
	{
		pg_ltoa(GetUserId(), buf);
		PG_RETURN_TEXT_P(cstring_to_text_with_len(buf, Min(strlen(buf), length)));
	}

	/* Return empty string if we cannot find a proper value */
	PG_RETURN_TEXT_P(cstring_to_text(""));
}
