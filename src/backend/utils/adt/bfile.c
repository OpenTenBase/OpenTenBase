/*-------------------------------------------------------------------------
 *
 * bfile.c
 *	  Functions for the built-in types bfile.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/bfile.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/hash.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "mb/pg_wchar.h"

#include "utils/datamask.h"

Datum
bfilename(PG_FUNCTION_ARGS)
{
	char *dirname = PG_GETARG_CSTRING(0);
	char *filename = PG_GETARG_CSTRING(1);
	int len;
	int tmplen = 0;
	int fixlen = 8; /* quotes/comma/space */
	char *dir_file;
	char *prefix = "bfilename";

	if (!dirname)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("directory should not be null")));
	if (!filename)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("filename should not be null")));

	len = strlen(prefix) + strlen(dirname) + strlen(filename) + fixlen + 1;
	dir_file = (char*)palloc(len);
	tmplen += snprintf(dir_file + tmplen, len, "%s", prefix);
	tmplen += snprintf(dir_file + tmplen, len - tmplen, "('%s', ", dirname);
	tmplen += snprintf(dir_file + tmplen, len - tmplen, "'%s')", filename);

	PG_RETURN_BFILE_P(dir_file);
}

Datum
bfilein(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_BFILE(0);
	if (str)
		PG_RETURN_BFILE_P(pstrdup(str));

	PG_RETURN_NULL();
}

Datum
bfileout(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_BFILE(0);
	if (str)
		PG_RETURN_CSTRING(pstrdup(str));

	PG_RETURN_NULL();
}
