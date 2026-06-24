#include "postgres.h"
#include "access/hash.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "executor/spi.h"

#include "stdlib.h"
#include <errno.h>

#include "opentenbase_ora.h"
#include "builtins.h"

PG_FUNCTION_INFO_V1(dbms_obfuscation_toolkit_md5);
PG_FUNCTION_INFO_V1(dbms_obfuscation_toolkit_md5_raw);

Datum
dbms_obfuscation_toolkit_md5(PG_FUNCTION_ARGS)
{
	VarChar    *source = PG_GETARG_VARCHAR_PP(0);
	Datum result;
	text *val;
	char *n_data;
	int n_data_size;

	n_data = VARDATA_ANY(source);
	n_data_size = VARSIZE_ANY_EXHDR(source);

	val = cstring_to_text_with_len(n_data, n_data_size);
	result = DirectFunctionCall1(md5_text, PointerGetDatum(val)); 
	return DirectFunctionCall1(texttoraw, result);	
}

Datum
dbms_obfuscation_toolkit_md5_raw(PG_FUNCTION_ARGS)
{
	Datum result;
	bytea* data = PG_GETARG_BYTEA_P_IF_EXISTS(0);
	result = DirectFunctionCall1(md5_bytea, PointerGetDatum(data));
	
	return DirectFunctionCall1(texttoraw, result);	
}
