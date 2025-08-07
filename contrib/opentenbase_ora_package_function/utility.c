/*
  This code implements one part of functonality of
  free available library PL/Vision. Please look www.quest.com

  Original author: Steven Feuerstein, 1996 - 2002
  PostgreSQL implementation author: Pavel Stehule, 2006-2018

  This module is under BSD Licence

  History:
    1.0. first public version 22. September 2006

*/

#include "postgres.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "string.h"
#include "stdlib.h"
#include "utils/pg_locale.h"
#include "mb/pg_wchar.h"
#include "lib/stringinfo.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "access/tupmacs.h"
#include "opentenbase_ora.h"
#include "builtins.h"
#include "access/hash.h"
#include "utils/numeric.h"
#include "utils/elog.h"
#include "utils/formatting.h"
#include "executor/spi.h"
#include "utils/regproc.h"

#define ORA_FUNCTION_PREFIX "PL/pgSQL function "
#define ORA_PACKAGE_PREFIX "PL/pgSQL package "

PG_FUNCTION_INFO_V1(dbms_utility_format_call_stack0);
PG_FUNCTION_INFO_V1(dbms_utility_format_call_stack1);
PG_FUNCTION_INFO_V1(get_hash_value_ora);
PG_FUNCTION_INFO_V1(dbms_utility_get_time);
PG_FUNCTION_INFO_V1(format_error_backtrace);
PG_FUNCTION_INFO_V1(format_error_stack);

static char*
dbms_utility_format_call_stack(char mode)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	ErrorData *edata;
	ErrorContextCallback *econtext;
	StringInfo   sinfo;

#if PG_VERSION_NUM >= 130000

	errstart(ERROR, TEXTDOMAIN);

#else

	errstart(ERROR, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN);

#endif

	MemoryContextSwitchTo(oldcontext);

	for (econtext = error_context_stack;
		 econtext != NULL;
		 econtext = econtext->previous)
		(*econtext->callback) (econtext->arg);

	edata = CopyErrorData();

	FlushErrorState();

	/* Now I wont to parse edata->context to more traditional format */
	/* I am not sure about order */

	sinfo = makeStringInfo();

	switch (mode)
	{
		case 'o':
			appendStringInfoString(sinfo, "----- PL/pgSQL Call Stack -----\n");
			appendStringInfoString(sinfo, "  object     line  object\n");
			appendStringInfoString(sinfo, "  handle   number  name\n");
			break;
	}

	if (!edata->context)
	{
		if (SPI_connect_level() < 0)
		{
			appendStringInfoString(sinfo, "	0	1	anonymous block\n");
		}
	}
	else
	{
		char *start = edata->context;
		while (*start)
		{
			char *oname_prefix = "function ";
			char *oname =  "anonymous object";
			char *line  = "";
			char *eol = strchr(start, '\n');
			Oid fnoid = InvalidOid;
			bool is_valid_line = false;

			/* first, solve multilines */
			if (eol)
				*eol = '\0';

			/* first know format */
			if (strncmp(start, ORA_FUNCTION_PREFIX, strlen(ORA_FUNCTION_PREFIX)) == 0)
			{
				char *p1, *p2;
				is_valid_line = true;
				if ((p1 = strstr(start, "function \"")))
				{
					p1 += strlen("function \"");
					if ((p2 = strchr(p1, '"')))
					{
						*p2++ = '\0';
						oname = p1;
						start = p2;
					}
				}
				else if ((p1 = strstr(start, "function ")))
				{
					p1 += strlen("function ");
					if ((p2 = strchr(p1, ')')))
					{
						char c = *++p2;
						*p2 = '\0';

						oname = pstrdup(p1);
						fnoid = regprocedurein_ext(oname, true);
						*p2 = c;
						start = p2;
					}
				}

				if ((p1 = strstr(start, "line ")))
				{
					size_t p2i;
					char c;

					p1 += strlen("line ");
					p2i = strspn(p1, "0123456789");

					/* safe separator */
					c = p1[p2i];

					p1[p2i] = '\0';
					line = pstrdup(p1);
					p1[p2i] = c;
				}
			}
			else if (strncmp(start, ORA_PACKAGE_PREFIX, strlen(ORA_PACKAGE_PREFIX)) == 0)
			{
				char *p1, *p2;
				is_valid_line = true;
				oname_prefix = "package body ";
				if ((p1 = strstr(start, oname_prefix)))
				{
					p1 += strlen(oname_prefix);
					if ((p2 = strchr(p1, ')')))
					{
						char *p3 = NULL;
						char c = *++p2;
						*p2 = '\0';

						oname = pstrdup(p1);
						fnoid = regprocedurein_ext(oname, true);
						*p2 = c;
						start = p2;

						if ((p3 = strchr(p1, '(')))
							oname = pnstrdup(oname, p3 - p1);
					}
				}

				if ((p1 = strstr(start, "line ")))
				{
					size_t p2i;
					char c;

					p1 += strlen("line ");
					p2i = strspn(p1, "0123456789");

					/* safe separator */
					c = p1[p2i];

					p1[p2i] = '\0';
					line = pstrdup(p1);
					p1[p2i] = c;
				}
			}

			if (is_valid_line)
			{
				switch (mode)
				{
					case 'o':
						appendStringInfo(sinfo, "%8x    %5s  %s%s", (int)fnoid, line, oname_prefix, oname);
						break;
					case 'p':
						appendStringInfo(sinfo, "%8d    %5s  %s%s", (int)fnoid, line, oname_prefix, oname);
						break;
					case 's':
						appendStringInfo(sinfo, "%d,%s,%s", (int)fnoid, line, oname);
						break;
				}
				appendStringInfoChar(sinfo, '\n');
			}

			if (eol)
			{
				start = eol + 1;
			}
			else
				break;
		}

	}

	return sinfo->data;
}


Datum
dbms_utility_format_call_stack0(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(dbms_utility_format_call_stack('o')));
}

Datum
dbms_utility_format_call_stack1(PG_FUNCTION_ARGS)
{
	text *arg = PG_GETARG_TEXT_P(0);
	char mode;

	if ((1 != VARSIZE(arg) - VARHDRSZ))
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid parameter"),
			 errdetail("Allowed only chars [ops].")));

	mode = *VARDATA(arg);
	switch (mode)
	{
		case 'o':
		case 'p':
		case 's':
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid parameter"),
				 errdetail("Allowed only chars: ['o', 'p', 's'].")));
	}

	PG_RETURN_TEXT_P(cstring_to_text(dbms_utility_format_call_stack(mode)));
}

/*
 *  dbms_utility.get_hash_value(
 *  name VARCHAR2,
 *  base NUMBER,
 *  hash_size NUMBER)
 *  the function name is the same as get_hash_value in dynahash.c, get_hash_value_ora is used internally
 */
Datum
get_hash_value_ora(PG_FUNCTION_ARGS)
{
    char *name;
    int base;
    int result;
    int hash_size;

    if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("bad argument")));

    base = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(PG_GETARG_NUMERIC(1))));
    hash_size = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(PG_GETARG_NUMERIC(2))));
    if (hash_size == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("bad argument")));

    if (PG_ARGISNULL(0))
    {
        /* if name is null, return the base */
        PG_RETURN_NUMERIC(DirectFunctionCall1(int4_numeric, base));
    }
    else
    {
        name = TextDatumGetCString(PG_GETARG_TEXT_PP(0));
        /* if hash_size is a negative number, use a positive number to mod */
        result = base + (DatumGetUInt32(hash_any((unsigned char *) name, strlen(name))) %
                                (uint32)(hash_size < 0 ? (-hash_size) : hash_size));
        PG_RETURN_NUMERIC(DirectFunctionCall1(int4_numeric, result));
    }
}

/*
 * dbms_utility.get_time()
 * Return the number of hundredths of a second
 * from a time epoch.
 */
Datum
dbms_utility_get_time(PG_FUNCTION_ARGS)
{
	/* The result is in the range -2147483648 to 2147483647, so get it by int32 */
	int32 result = DbmsUtilityGetTime();

	PG_RETURN_NUMERIC(DirectFunctionCall1(int4_numeric, result));
}

static void
get_sch_and_func_name(char *str, int str_len,
					  char **sch_name, char **func_name,
					  int *sch_name_size, int *func_name_size)
{
	if (str && str[0] == '\"')
	{
		/* The schema name contains double quotes */
		char	*quotes_end = NULL;

		quotes_end = strchr(str + 1, '\"');
		Assert(quotes_end != NULL);
		*func_name = strchr(quotes_end + 1, '.');
	}
	else
		*func_name = strchr(str, '.');

	if (*func_name)
	{
		*sch_name = str;
		*sch_name_size = *func_name - *sch_name;
		(*func_name)++;
		*func_name_size = str_len - (str - *func_name);
	}
	else
	{
		*func_name = str;
		*func_name_size = str_len;
	}

	/*
	 * Remove the args list like (a int, b text).
	 * I don't know whether it is necessary,
	 * but OpenTenBase_Ora doesn't print the args list.
	 */
	if ((*func_name)[0] == '\"')
	{
		char *func_end = strchr((*func_name) + 1, '\"');

		Assert(func_end);
		/* It contains the double quotes */
		*func_name_size = func_end - *func_name + 1;
	}
	else
	{
		char *func_end = strchr((*func_name), '(');

		if (func_end)
			*func_name_size = func_end - *func_name;
	}
}

static char *
get_upper_obj_name(char *obj_name, size_t size)
{
	char *upper_obj_name;

	/* It is not a case sensitive object name */
	if (obj_name[0] != '\"')
		upper_obj_name = asc_toupper(obj_name, size);
	else
	{
		/* Remove the double quotes */
		Assert(size > 2);
		upper_obj_name = pnstrdup(obj_name + 1, size - 2);
	}

	return upper_obj_name;
}

/*
 * The size of src is always larger than dst:
 * 1. Length of "ORA-06512: at " is 14 and "PL/pgSQL function " is 18.
 * 2. Result like "ORA-06512: at \"$function_name\", line $line_no"
 * will add two double quotes and one ", ".
 * 3. Other cases is always less than src.
 */
static char *
parse_error_backtrace(ErrorData *edata, int trace_level, bool is_error_stack)
{
#define ORA_BACKTRACE_ERRM_PREFIX "ORA-06512: at "
#define ORA_INLINE_CODE_BLOCK "inline_code_block"

	char *result = NULL;
	char *dst;
	char *trace, *ltrace;
	char *src;
	int level = 0;
	int inline_code_block_len = strlen(ORA_INLINE_CODE_BLOCK);

	char *function_name = NULL;
	char *schema_name = NULL;
	int sch_name_size = 0;
	int func_name_size = 0;
	char *upper_func_name = NULL;
	char *upper_sch_name = NULL;

	if (!edata || !edata->context)
		return NULL;

	src = pstrdup(edata->context);
	result = dst = palloc0(strlen(src) + 1);
	trace = strtok(src, "\n");

	while(trace)
	{
		char	*token;
		bool is_inline_code_block = true;

		ltrace = strstr(trace, ORA_PACKAGE_PREFIX);
		if (ltrace)
		{
			char *pos;

			level++;
			if (is_error_stack && level > trace_level)
				break;

			strcpy(dst, ORA_BACKTRACE_ERRM_PREFIX);
			dst += strlen(ORA_BACKTRACE_ERRM_PREFIX);

			ltrace += strlen(ORA_PACKAGE_PREFIX);
			if ((pos = strstr(ltrace, " package body")))
			{
				strncpy(dst, ltrace, pos - ltrace);
				dst += pos - ltrace;
				is_inline_code_block = false;
			}

			if ((pos = strstr(ltrace, "line ")))
			{
				if (!is_inline_code_block)
				{
					strcpy(dst, ", ");
					dst += 2;
				}

				/* Now the errcontext are always line %d %s */
				token = strchr((pos + 5), ' ');
				Assert(token);

				strncpy(dst, pos, token - pos);
				dst += token - pos;
			}

			strcpy(dst, "\n");
			dst ++;
		}
		else if ((trace = strstr(trace, ORA_FUNCTION_PREFIX)))
		{
			level++;
			if (is_error_stack && level > trace_level)
				break;

			trace += strlen(ORA_FUNCTION_PREFIX);
	
			/* Get the function name */
			token = trace;
			while (strchr(token, '\"'))
			{
				token = strchr(token, '\"');
				token = token + 1;
			}
			token = strchr(token, ' ');
			
			/* PL/pgSQL function %s %s */
			if (token)
			{
				char *fnname = NULL;

				/* It is not a inline code block */
				if (strncmp(trace, ORA_INLINE_CODE_BLOCK, inline_code_block_len) != 0)
				{
					get_sch_and_func_name(trace, token - trace, &schema_name, &function_name, &sch_name_size, &func_name_size);

					/* ignore RAISE_APPLICATION_ERROR procedure */
					fnname = strchr(function_name, '(');
					if (fnname && strncasecmp(function_name, "RAISE_APPLICATION_ERROR", fnname - function_name) == 0)
					{
						trace = strtok(NULL, "\n");
						continue;
					}

					/* Result is ORA-06512: at */
					strcpy(dst, ORA_BACKTRACE_ERRM_PREFIX);
					dst += strlen(ORA_BACKTRACE_ERRM_PREFIX);
					is_inline_code_block = false;

					/* Result is ORA-06512: at "$schema.$function_name" */
					strcpy(dst, "\"");
					dst ++;
					/* Copy the upper schema name */
					if (schema_name)
					{
						upper_sch_name = get_upper_obj_name(schema_name, sch_name_size);
						strncpy(dst, upper_sch_name, strlen(upper_sch_name));
						dst += strlen(upper_sch_name);
						pfree(upper_sch_name);
						strcpy(dst, ".");
						dst ++;
					}
					/* Copy the upper function name */
					upper_func_name = get_upper_obj_name(function_name, func_name_size);
					strncpy(dst, upper_func_name, strlen(upper_func_name));
					dst += strlen(upper_func_name);
					pfree(upper_func_name);
					/* Copy " */
					strcpy(dst, "\"");
					dst ++;
				}

				/* PL/pgSQL function %s line %d */
				if (!is_inline_code_block)
					trace = strstr(trace, " line ") + 1;
				else
					trace = token + 1;

				token = strchr(trace, ' ');

				if (token && (strncmp(trace, "line ", 5) == 0))
				{
					if (!fnname)
					{
						strcpy(dst, ORA_BACKTRACE_ERRM_PREFIX);
						dst += strlen(ORA_BACKTRACE_ERRM_PREFIX);
					}

					/* Result is ORA-06512: at "$schema.$function_name", */
					if (!is_inline_code_block)
					{
						strcpy(dst, ", ");
						dst += 2;
					}

					/* Now the errcontext are always line %d %s */
					token = strchr((trace + 5), ' ');
					Assert(token);

					strncpy(dst, trace, token - trace);
					dst += token - trace;
				}
			}
			else
			{
				/* Result is ORA-06512: at */
				strcpy(dst, ORA_BACKTRACE_ERRM_PREFIX);
				dst += strlen(ORA_BACKTRACE_ERRM_PREFIX);

				/* PL/pgSQL function %s */
				strncpy(dst, trace, strlen(trace));
			}
			/* Add a new line break */
			strcpy(dst, "\n");
			dst ++;
		}

		trace = strtok(NULL, "\n");
	}

	/* Remove the last line break */
	*(dst) = '\0';
	return result;
}

/*
 * dbms_utility.format_error_backtrace()
 * Return the call stack at the point where an exception was raised.
 */
Datum
format_error_backtrace(PG_FUNCTION_ARGS)
{
	ErrorData *edata;
	char *result = NULL;

	edata = get_curr_error_data();
	if (edata && edata->context)
		result = parse_error_backtrace(edata, -1, false);

	if (result)
		PG_RETURN_TEXT_P(cstring_to_text(result));

	PG_RETURN_NULL();
}

Datum
format_error_stack(PG_FUNCTION_ARGS)
{
	ErrorData *edata;
	StringInfoData buf;
	char *backtrace;
	text *result = NULL;
	int32 ecode = -1;

	edata = get_curr_error_data();
	if (!edata)
		PG_RETURN_NULL();

	initStringInfo(&buf);
	ecode = edata->sqlerrcode;
	if(IS_ORA_SQLSTATE(ecode))
	{
		char *unpacked_sql_state = unpack_sql_state(ecode);
		if (strcmp(unpacked_sql_state, "NNNNN") != 0)
		{
			char sql_code_usr[7];
			sql_code_usr[0] = '-';
			strncpy(&sql_code_usr[1], unpacked_sql_state, 5);
			sql_code_usr[6] = '\0';
			ecode = pg_atoi(sql_code_usr, 4, 0);
		}
	}

	/* output user define errcode */
	if (ecode <= -20000 && ecode >= -20999)
		appendStringInfo(&buf, "ORA%d: %s", ecode, edata->message);
	else
		appendStringInfo(&buf, "ORA-01476: %s", edata->message);

	backtrace = parse_error_backtrace(edata, edata->spi_connected - SPI_connect_level(), true);
	if (backtrace)
		appendStringInfo(&buf, "\n%s", backtrace);

	result = cstring_to_text(buf.data);
	pfree(buf.data);

	PG_RETURN_TEXT_P(result);
}
