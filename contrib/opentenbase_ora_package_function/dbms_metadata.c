/*----------------------------------------------------------------------------
 *
 *     dbms_metadata.c
 *     Adapt opentenbase_ora's dbms_metadata package
 *
 *----------------------------------------------------------------------------
 */

#include "postgres.h"
#include <ctype.h>
#include <limits.h>
#include "access/hash.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/varlena.h"
#include "utils/numeric.h"
#include "fmgr.h"
#include "port.h"
#include "dbms_md_itf.h"

PG_FUNCTION_INFO_V1(dbms_metadata_get_ddl);
PG_FUNCTION_INFO_V1(dbms_metadata_session_transform);
PG_FUNCTION_INFO_V1(dbms_md_set_transform_param_p3char);
PG_FUNCTION_INFO_V1(dbms_md_set_transform_param_p3bool);
PG_FUNCTION_INFO_V1(dbms_md_set_transform_param_p3number);


typedef text* (*DBMS_MD_EXPORT_FUNC)(const char* object_name, const char* schema_name, const char* transform);

typedef struct
{
	char* object_name;
	DBMS_MD_EXPORT_FUNC ex_func;
}Dbms_Md_Export_Obj_Item;

static text* dbms_md_export_table(const char* object_name, const char* schema_name, const char* transform);
static text* dbms_md_export_index(const char* object_name, const char* schema_name, const char* transform);
static text* dbms_md_export_type(const char* object_name, const char* schema_name, const char* transform);
static text* dbms_md_export_function(const char* object_name, const char* schema_name, const char* transform);


const Dbms_Md_Export_Obj_Item g_dbms_md_export_objs[] = {
	{"TABLE", dbms_md_export_table},
	{"INDEX", dbms_md_export_index},
	{"TYPE", dbms_md_export_type},
	{"FUNCTION",dbms_md_export_function}};

const int g_dbms_md_export_obj_num = 
	sizeof(g_dbms_md_export_objs)/sizeof(g_dbms_md_export_objs[0]);

static DBMS_MD_EXPORT_FUNC get_object_export_func(text *object_type)
{
	int loop = 0;
	const char* type = VARDATA_ANY(object_type);
	
	for (loop = 0; loop < g_dbms_md_export_obj_num; loop++)
	{
		const Dbms_Md_Export_Obj_Item *item = &g_dbms_md_export_objs[loop];
		if (0 == pg_strncasecmp(type, item->object_name, strlen(item->object_name)))
			return item->ex_func;
	}

	return NULL;
}

#define DBMS_MD_SESSION_TRANSFORM (0x123456789)
/*
SESSION_TRANSFORM
*/
Datum
dbms_metadata_session_transform(PG_FUNCTION_ARGS)
{
	Numeric result = DatumGetNumeric(DirectFunctionCall1(int4_numeric, 
		Int32GetDatum(DBMS_MD_SESSION_TRANSFORM)));
	
	PG_RETURN_NUMERIC(result);
}

/*
DBMS_METADATA.SET_TRANSFORM_PARAM (
   transform_handle   IN NUMBER,
   name               IN VARCHAR2,
   value              IN VARCHAR2,
   object_type        IN VARCHAR2 DEFAULT NULL);

DBMS_METADATA.SET_TRANSFORM_PARAM (
   transform_handle   IN NUMBER,
   name               IN VARCHAR2,
   value              IN BOOLEAN DEFAULT TRUE,
   object_type        IN VARCHAR2 DEFAULT NULL);

DBMS_METADATA.SET_TRANSFORM_PARAM (
   transform_handle   IN NUMBER,
   name               IN VARCHAR2,
   value              IN NUMBER,
   object_type        IN VARCHAR2 DEFAULT NULL);

*/
Datum 
dbms_md_set_transform_param_p3char(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum 
dbms_md_set_transform_param_p3bool(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum 
dbms_md_set_transform_param_p3number(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}


/*
GET_DDL (object_type IN VARCHAR2, 					  
		   name 		  IN VARCHAR2, 					  
		   schema_name IN VARCHAR2 DEFAULT NULL,					  
		   version     IN VARCHAR2 DEFAULT 'COMPATIBLE',					  
		   model           IN VARCHAR2 DEFAULT 'OPENTENBASE_ORA',					  
		   transform       IN VARCHAR2 DEFAULT 'DDL')RETURN CLOB;
*/

Datum
dbms_metadata_get_ddl(PG_FUNCTION_ARGS)
{
	text *object_type = NULL;
	text *object_name = NULL;
	text *schema_name = NULL;
	text *transform = NULL;
	text *ret_data = NULL;
	char* str_object = NULL;
	char* str_schema = NULL;
	char* str_trans = NULL;
	DBMS_MD_EXPORT_FUNC ex_func = NULL;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("The parameter of object type is null")));

	object_type =  PG_GETARG_TEXT_P(0);

	if (!PG_ARGISNULL(1))
		object_name = PG_GETARG_TEXT_P(1);

	if (!PG_ARGISNULL(2))
		schema_name = PG_GETARG_TEXT_P(2);

	if (!PG_ARGISNULL(5))
		transform = PG_GETARG_TEXT_P(5);

	ex_func = get_object_export_func(object_type);
	if (NULL == ex_func)
		ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("The parameter of object type is not support to get_ddl")));

	if (NULL != object_name)
		str_object = text_to_cstring(object_name);

	if (NULL!= schema_name)
		str_schema = text_to_cstring(schema_name);

	if (NULL!= transform)
		str_trans = text_to_cstring(transform);
	
	ret_data = (*ex_func)(str_object, str_schema, str_trans);

	if (NULL != str_object)
		pfree(str_object);

	if (NULL != str_schema)
		pfree(str_schema);

	if (NULL != str_trans)
		pfree(str_trans);

	PG_RETURN_TEXT_P(ret_data);
}

static text* 
dbms_md_export_table(const char* object_name, const char* schema_name, const char* transform)
{
	StringInfoData buf;
	initStringInfo(&buf);
	
	dump_main(DBMS_MD_DOBJ_TABLE, schema_name, object_name, &buf);
	
	return cstring_to_text(buf.data);
}

static text* 
dbms_md_export_index(const char* object_name, const char* schema_name, const char* transform)
{
	StringInfoData buf;
	initStringInfo(&buf);
	
	dump_main(DBMS_MD_DOBJ_INDEX, schema_name, object_name, &buf);
	
	return cstring_to_text(buf.data);
}

static text* 
dbms_md_export_type(const char* object_name, const char* schema_name, const char* transform)
{
	StringInfoData buf;
	initStringInfo(&buf);
	
	dump_main(DBMS_MD_DOBJ_TYPE, schema_name, object_name, &buf);
	
	return cstring_to_text(buf.data);
}

static text* 
dbms_md_export_function(const char* object_name, const char* schema_name, const char* transform)
{
	StringInfoData buf;
	initStringInfo(&buf);
	
	dump_main(DBMS_MD_DOBJ_FUNC, schema_name, object_name, &buf);
	
	return cstring_to_text(buf.data);
}
