#include "postgres.h"
#include "access/hash.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "pgxc/pgxc.h"
#include "common/md5.h"
#include "miscadmin.h"

#include "stdlib.h"
#include "time.h"
#include <math.h>
#include <errno.h>

#include "opentenbase_ora.h"
#include "builtins.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "commands/prepare.h"

PG_FUNCTION_INFO_V1(dbms_session_unique_session_id);
PG_FUNCTION_INFO_V1(dbms_session_reset_package);
PG_FUNCTION_INFO_V1(dbms_session_free_unused_user_memory);

Datum
dbms_session_unique_session_id(PG_FUNCTION_ARGS)
{
#define MD5_HASH_LEN 33
    char hexsum[MD5_HASH_LEN];
    char new_str[NAMEDATALEN];
    uint32 global_sid_len = strlen(PGXCQueryId);
    if (0 == global_sid_len)
    {
        snprintf(new_str, NAMEDATALEN, "%u", MyProcPid);
    }
    else
    {
        StrNCpy(new_str, PGXCQueryId, global_sid_len);
    }

    if (!pg_md5_hash(new_str, global_sid_len, hexsum))
    {
        elog(ERROR, "failed to calc md5 hash");
        PG_RETURN_NULL();
    }
    else
    {
        hexsum[24] = 0;
        PG_RETURN_TEXT_P(cstring_to_text_upper(hexsum));
    }
}

Datum
dbms_session_reset_package(PG_FUNCTION_ARGS)
{
	/* Set dbms_session.reset_package moment */
	if (!InPlpgsqlFunc())
		reset_package_moment = RESET_PKG_BEGIN;
	else
		reset_package_moment = RESET_PKG_END;

	PG_RETURN_VOID();
}

Datum
dbms_session_free_unused_user_memory(PG_FUNCTION_ARGS)
{
	DropAllPreparedStatements();
	PG_RETURN_VOID();
}
