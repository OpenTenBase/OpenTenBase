#include "postgres.h"
#include "hdfs_fdw.h"
#include "hdfs_client.h"
#include "utils/elog.h"

/*
 * Execute a query on the remote server
 */
bool
hdfs_query_execute(int con_index, hdfs_opt *opt, char *query)
{
    char       *err_buf = NULL;

    if (opt->log_remote_sql)
        elog(LOG, "hdfs_fdw: execute remote SQL: [%s] [%d]", query, opt->fetch_size);

    if (DBExecute(con_index, query, opt->fetch_size, &err_buf) < 0)
    {
        if (err_buf)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to execute query: %s", err_buf)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to execute query")));
    }

    return true;
}

/*
 * Fetch next row
 */
int
hdfs_fetch(int con_index)
{
    int         rc;
    char       *err_buf = NULL;

    rc = DBFetch(con_index, &err_buf);
    if (rc < -1)
    {
        if (err_buf)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to fetch data from Hive/Spark server: %s", err_buf)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to fetch data from Hive/Spark server")));
    }

    return rc;
}

/*
 * Get column count
 */
int
hdfs_get_column_count(int con_index)
{
    int         count;
    char       *err_buf = NULL;

    count = DBGetColumnCount(con_index, &err_buf);
    if (count < 0)
    {
        if (err_buf)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to get column count from Hive/Spark server: %s", err_buf)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to get column count from Hive/Spark server")));
    }

    return count;
}

/*
 * Get field value as string
 */
char *
hdfs_get_field_as_cstring(int con_index, int idx, bool *is_null)
{
    int         size;
    char       *value = NULL;
    char       *err_buf = NULL;

    size = DBGetFieldAsCString(con_index, idx, &value, &err_buf);
    if (size < -1)
    {
        if (err_buf)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to fetch field from Hive/Spark Server: %s", err_buf)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                     errmsg("failed to fetch field from Hive/Spark Server")));
    }

    if (size == -1)
        *is_null = true;
    else
        *is_null = false;

    return value;
}
