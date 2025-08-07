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

typedef struct
{
	char param[64];
	Datum (*fptr)(void);
} UserEnvParam;

static Datum get_current_schema_id(void);
static Datum get_ora_lang(void);
static Datum get_ora_language(void);
static Datum get_ora_sid(void);
static Datum get_ora_client_info(void);
static Datum get_ora_entryid(void);
static Datum is_ora_dba(void);
static Datum get_ora_terminal(void);

UserEnvParam EnvParam [] =
{
	{"schemaid", 		get_current_schema_id},
	{"lang", 			get_ora_lang},
	{"language", 		get_ora_language},
	{"sessionid", 		get_ora_sid},
	{"sid", 			get_ora_sid},
	{"client_info", 	get_ora_client_info},
	{"entryid", 		get_ora_entryid},
	{"isdba", 			is_ora_dba},
	{"terminal", 		get_ora_terminal},
};

static Datum
get_ora_client_info(void)
{
	PG_RETURN_TEXT_P(cstring_to_text_with_len("", 0));
}

static Datum
get_ora_entryid(void)
{
	char *eid = "0";
	
	PG_RETURN_TEXT_P(cstring_to_text_with_len(eid, strlen(eid)));
}

static Datum
is_ora_dba(void)
{
	char *isdba = superuser() ? "TRUE" : "FALSE";

	PG_RETURN_TEXT_P(cstring_to_text_with_len(isdba, strlen(isdba)));
}

static Datum
get_ora_terminal(void)
{
	char *terminal = "";

	PG_RETURN_TEXT_P(cstring_to_text_with_len(terminal, strlen(terminal)));
}

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
		schema_id = get_namespace_oid(cur_schema, true);

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
		elog(ERROR, "failed to get lc_collate guc");

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
		PG_RETURN_TEXT_P(cstring_to_text_upper_with_len(val, lang - val));
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
		appendStringInfo(&buf, "%u", MyProcPid);
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
orcl_userenv(PG_FUNCTION_ARGS)
{
	int		i = 0;
	bool	found = false;
	Datum	result = PointerGetDatum(cstring_to_text(""));
	text   *param_text = NULL;
	char   *param = NULL;

	if (PG_ARGISNULL(0))
		elog(ERROR, "invalid USERENV parameter");

	param_text = PG_GETARG_TEXT_PP_IF_EXISTS(0);
	if (!param_text)
		return result;
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
				elog(ERROR, "invalid USERENV parameter");
		}
	}

	return result;
}
