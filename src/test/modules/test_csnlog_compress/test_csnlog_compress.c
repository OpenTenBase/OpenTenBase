


#include "postgres.h"
#include "fmgr.h"
#include "access/csnlog.h"
#include "access/lru.h"
#include "access/xact.h"
#include "postmaster/bgwriter.h"
#include "storage/procarray.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_compress_csnlog_file);
Datum
test_compress_csnlog_file(PG_FUNCTION_ARGS)
{
    int old_value = compress_config;
    TransactionId newFrozenXid = GetOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM);
    if (newFrozenXid < CSNLOG_XACTS_PER_PAGE * CSNLOG_XACTS_PER_LSN_GROUP)
    {   
        ereport(WARNING, (errmsg("xid(%u) is too small!!!", newFrozenXid)));
        PG_RETURN_BOOL(false);
    }
    compress_config = REMOVED_WHEN_ERROR;
    bool ret = TestCsnlogTruncateCompress(newFrozenXid);
    compress_config = old_value;
    PG_RETURN_BOOL(ret);
}

PG_FUNCTION_INFO_V1(test_rename_csnlog_file);
Datum
test_rename_csnlog_file(PG_FUNCTION_ARGS)
{
    int old_value = compress_config;
    TransactionId newFrozenXid = GetOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM);
    if (newFrozenXid < CSNLOG_XACTS_PER_PAGE * CSNLOG_XACTS_PER_LSN_GROUP)
    {   
        ereport(WARNING, (errmsg("xid(%u) is too small!!!", newFrozenXid)));
        PG_RETURN_BOOL(false);
    }

    compress_config = RENAME_FOR_BACKUP;
    bool ret = TestCsnlogTruncateRename(newFrozenXid);
    compress_config = old_value;
    PG_RETURN_BOOL(ret);
}


