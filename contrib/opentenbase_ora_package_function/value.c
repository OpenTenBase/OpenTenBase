#include "postgres.h"
#include "access/hash.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"

#include "stdlib.h"
#include <errno.h>

#include "opentenbase_ora.h"
#include "builtins.h"

PG_FUNCTION_INFO_V1(orcl_value);

Datum
orcl_value(PG_FUNCTION_ARGS)
{
	Datum 	arg = PG_GETARG_DATUM(0);
	Oid		argTypOid;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("invalid user.table.column, table.column, or column specification")));

	argTypOid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	if (!OidIsValid(argTypOid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid user.table.column, table.column, or column specification")));

	return arg;
}
