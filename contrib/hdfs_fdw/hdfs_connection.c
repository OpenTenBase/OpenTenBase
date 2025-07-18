#include "postgres.h"
#include "hdfs_fdw.h"
#include "hdfs_connection.h"
#include "foreign/foreign.h"
#include "utils/elog.h"

/*
 * hdfs_get_connection
 *      Get a connection to Hive/Spark server
 */
int
hdfs_get_connection(ForeignServer *server, hdfs_opt *opt)
{
    int         conn;
    char       *err_buf = NULL;

    elog(DEBUG1, "hdfs_fdw: connecting to %s:%d", opt->host, opt->port);

    conn = DBOpenConnection(opt->host, opt->port, opt->dbname, 
                           opt->username, opt->password,
                           opt->connect_timeout, opt->receive_timeout,
                           opt->auth_type, opt->client_type, &err_buf);
    
    if (conn < 0)
    {
        if (err_buf)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                     errmsg("failed to initialize the connection: %s", err_buf)));
        else
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
                     errmsg("failed to initialize the connection")));
    }

    elog(DEBUG1, "hdfs_fdw: new connection(%d) opened for server \"%s\"", 
         conn, server->servername);

    return conn;
}

/*
 * hdfs_rel_connection
 *      Release a connection
 */
void
hdfs_rel_connection(int con_index)
{
    int r;

    r = DBCloseConnection(con_index);
    if (r < 0)
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg("failed to close the connection(%d)", con_index)));

    elog(DEBUG1, "hdfs_fdw: connection(%d) closed", con_index);
}
