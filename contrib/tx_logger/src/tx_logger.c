#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "access/xact.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(log_transaction_start);
PG_FUNCTION_INFO_V1(log_transaction_commit);
PG_FUNCTION_INFO_V1(log_transaction_rollback);

Datum log_transaction_start(PG_FUNCTION_ARGS)
{
    TransactionId xid = GetCurrentTransactionId();
    const char *query = "INSERT INTO tx_logger (xid, action, timestamp) VALUES ($1, 'start', now())";

    SPI_connect();

    // 执行SPI查询以记录事务开始
    SPI_execute_with_args(query,
                          1,
                          NULL,
                          &(Datum){xid, XIDOID},
                          NULL,
                          NULL,
                          SPI_OK_UTILITY | SPI_OK_INSERT);

    SPI_finish();

    PG_RETURN_VOID();
}

Datum log_transaction_commit(PG_FUNCTION_ARGS)
{
    TransactionId xid = GetCurrentTransactionId();
    const char *query = "INSERT INTO tx_logger (xid, action, timestamp) VALUES ($1, 'commit', now())";

    SPI_connect();

    // 执行SPI查询以记录事务提交
    SPI_execute_with_args(query,
                          1,
                          NULL,
                          &(Datum){xid, XIDOID},
                          NULL,
                          NULL,
                          SPI_OK_UTILITY | SPI_OK_INSERT);

    SPI_finish();

    PG_RETURN_VOID();
}

Datum log_transaction_rollback(PG_FUNCTION_ARGS)
{
    TransactionId xid = GetCurrentTransactionId();
    const char *query = "INSERT INTO tx_logger (xid, action, timestamp) VALUES ($1, 'rollback', now())";

    SPI_connect();

    // 执行SPI查询以记录事务回滚
    SPI_execute_with_args(query,
                          1,
                          NULL,
                          &(Datum){xid, XIDOID},
                          NULL,
                          NULL,
                          SPI_OK_UTILITY | SPI_OK_INSERT);

    SPI_finish();

    PG_RETURN_VOID();
}