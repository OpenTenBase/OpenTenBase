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
#include "pgxc/pgxc.h"
#include "catalog/namespace.h"
#include "miscadmin.h"

#include "opentenbase_ora.h"
#include "builtins.h"

typedef struct
{
	char param[64];
	Datum (*fptr)(void);
} UserEnvParam;

static Datum get_current_schema_id(void);
static Datum get_ora_lang(void);
static Datum get_ora_language(void);
static Datum get_ora_sid(void);

UserEnvParam EnvParam [] = 
{
	{ "schemaid", 		get_current_schema_id },
	{ "lang", 			get_ora_lang },
	{ "language", 		get_ora_language },
	{ "sid", 			get_ora_sid },
};

PG_FUNCTION_INFO_V1(orafce_userenv);

static Datum
get_current_schema_id(void)
{
	char *cur_schema = NULL;
	Oid schema_id = InvalidOid;
	FunctionCallInfoData fcinfo;
	StringInfoData buf;
	initStringInfo(&buf);

	InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
	cur_schema = DatumGetCString(current_schema(&fcinfo));
	if (cur_schema)
	{
		schema_id = get_namespace_oid(cur_schema, true);
	}

	appendStringInfo(&buf, "%d", schema_id);
	PG_RETURN_TEXT_P(cstring_to_text_with_len(buf.data, buf.len));
}


static char *
get_ora_language_impl(void)
{
	const char *name = "lc_collate";
	const char *varname = NULL;
	char	   *value = NULL;

	/* Get the value and canonical spelling of name */
	value = GetConfigOptionByName(name, &varname, false);
	if (!value)
	{
		elog(ERROR, "failed to get lc_collate guc");
	}
	return value;
}

static Datum
get_ora_lang(void)
{
	char *lang = NULL;
	char *val = NULL;

	val = get_ora_language_impl();
	lang = strstr(val, "_");
	if (lang)
	{
		PG_RETURN_TEXT_P(cstring_to_text_upper_with_len(val, lang - val));
	}
	else
	{
		ereport(ERROR,
				(ERRCODE_INTERNAL_ERROR,
				 errmsg("unexpected lc_collate format: %s", val)));
	}
}

static Datum
get_ora_language(void)
{
	char *val = get_ora_language_impl();
	PG_RETURN_TEXT_P(cstring_to_text_upper(val));
}

static Datum
get_ora_sid(void)
{
	StringInfoData buf;
	Datum hash_val;

	initStringInfo(&buf);
	if (0 == PGXCQueryId[0])
	{
		appendStringInfo(&buf, "%u", MyProcPid);
	}
	else
	{
		hash_val = DirectFunctionCall1(hashtext,
						 PointerGetDatum(cstring_to_text(PGXCQueryId)));
		appendStringInfo(&buf, "%u", DatumGetUInt32(hash_val));
	}
	PG_RETURN_TEXT_P(cstring_to_text_with_len(buf.data, buf.len));
}

/*
 * support opentenbase_ora compatibale userenv function.
 * only support 4 parameters: schemaid, lang, language and sid
 */
Datum
orafce_userenv(PG_FUNCTION_ARGS)
{
	int i = 0;
	bool found = false;
	Datum result = PointerGetDatum(cstring_to_text(""));
	text *param_text = NULL;
	char *param = NULL;

	if (!ORA_MODE)
	{
		elog(ERROR, "not supported");
	}

	if (PG_ARGISNULL(0))
		elog(ERROR, "invalid USERENV parameter");

	param_text = PG_GETARG_TEXT_PP_IF_EXISTS(0);
	if (!param_text)
	{
		return result;
	}
	else
	{
		param = text_to_cstring(param_text);
		if (param)
		{
			for (i = 0; i < sizeof(EnvParam) / sizeof(UserEnvParam); i++)
			{
				if (0 == pg_strncasecmp(EnvParam[i].param, param, sizeof(EnvParam[i].param)))
				{
					result = EnvParam[i].fptr();
					found = true;
					break;
				}
			}
			if (!found)
			{
				elog(ERROR, "invalid USERENV parameter");
			}
		}
	}
	return result;
}
