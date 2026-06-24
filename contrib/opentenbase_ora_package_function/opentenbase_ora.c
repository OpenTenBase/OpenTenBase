#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "commands/variable.h"

#include "opentenbase_ora.h"
#include "builtins.h"
#include "pipe.h"
#include "oraplsql.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "funcapi.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/regproc.h"


extern void _PG_fini(void);

/*  default value */
extern char *nls_date_format;
char  *orafce_timezone = NULL;
void
_PG_init(void)
{
#if PG_VERSION_NUM < 90600

	RequestAddinLWLocks(1);

#endif

	RequestAddinShmemSpace(SHMEMMSGSZ);

	/* Define custom GUC variables. */

	DefineCustomStringVariable("orafce.timezone",
									"Specify timezone used for sysdate function.",
									NULL,
									&orafce_timezone,
									"GMT",
									PGC_USERSET,
									0,
									check_timezone, NULL, show_timezone);

	DefineCustomBoolVariable("orafce.varchar2_null_safe_concat",
									"Specify timezone used for sysdate function.",
									NULL,
									&orafce_varchar2_null_safe_concat,
									false,
									PGC_USERSET,
									0,
									NULL, NULL, NULL);
	EmitWarningsOnPlaceholders("orafce");
}
