/*
 * src/test/examples/testlibpq4.c
 *
 *
 * testlibpq4.c
 *        this test program shows to use LIBPQ to make multiple backend
 * connections
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"

static void
exit_nicely(PGconn *conn1, PGconn *conn2)
{
    // 退出程序前关闭数据库连接并退出
    if (conn1)
        PQfinish(conn1);
    if (conn2)
        PQfinish(conn2);
    exit(1);
}

static void
check_conn(PGconn *conn, const char *dbName)
{
    /* check to see that the backend connection was successfully made */
    /*  检查数据库连接状态，如果连接失败则打印错误信息并退出程序 */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database \"%s\" failed: %s",
                dbName, PQerrorMessage(conn));
        exit(1);
    }
}

int
main(int argc, char **argv)
{
    char       *pghost,
               *pgport,
               *pgoptions,
               *pgtty;
    char       *dbName1,
               *dbName2;
    char       *tblName;
    int            nFields;
    int            i,
                j;

    PGconn       *conn1,
               *conn2;

    /*
     * PGresult   *res1, *res2;
     */
    PGresult   *res1;

    if (argc != 4)
    {
        // 参数数量不符合要求，打印使用说明并退出程序
        fprintf(stderr, "usage: %s tableName dbName1 dbName2\n", argv[0]);
        fprintf(stderr, "      compares two tables in two databases\n");
        exit(1);
    }
    tblName = argv[1];
    dbName1 = argv[2];
    dbName2 = argv[3];


    /*
     * begin, by setting the parameters for a backend connection if the
     * parameters are null, then the system will try to use reasonable
     * defaults by looking up environment variables or, failing that, using
     * hardwired constants
     */
    pghost = NULL;                /* host name of the backend */
    pgport = NULL;                /* port of the backend */
    pgoptions = NULL;            /* special options to start up the backend
                                 * server */
    pgtty = NULL;                /* debugging tty for the backend */

    /* make a connection to the database */
    conn1 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName1);
    check_conn(conn1, dbName1);

    // 连接到第二个数据库
    conn2 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName2);
    check_conn(conn2, dbName2);

    /* start a transaction block */
    // 在第一个数据库上开始一个事务
    res1 = PQexec(conn1, "BEGIN");
    if (PQresultStatus(res1) != PGRES_COMMAND_OK)
    {
        // "BEGIN"命令执行失败，打印错误信息并退出程序
        fprintf(stderr, "BEGIN command failed\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }

    /*
     * make sure to PQclear() a PGresult whenever it is no longer needed to
     * avoid memory leaks
     */
    PQclear(res1);

    /*
     * fetch instances from the pg_database, the system catalog of databases
     */
    // 声明一个游标
    res1 = PQexec(conn1, "DECLARE myportal CURSOR FOR select * from pg_database");
    if (PQresultStatus(res1) != PGRES_COMMAND_OK)
    {
        // 声明游标命令执行失败，打印错误信息并退出程序
        fprintf(stderr, "DECLARE CURSOR command failed\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }
    PQclear(res1);

    // 获取游标中的所有结果
    res1 = PQexec(conn1, "FETCH ALL in myportal");
    if (PQresultStatus(res1) != PGRES_TUPLES_OK)
    {
        // 获取游标结果命令执行失败，打印错误信息并退出程序
        fprintf(stderr, "FETCH ALL command didn't return tuples properly\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }

    /* first, print out the attribute names */
    // 获取查询结果的列数
    nFields = PQnfields(res1);
    for (i = 0; i < nFields; i++)
        // 打印每个字段的名称
        printf("%-15s", PQfname(res1, i));
    printf("\n\n");

    /* next, print out the instances */
    for (i = 0; i < PQntuples(res1); i++)
    {
        for (j = 0; j < nFields; j++)
            // 打印每行记录的值
            printf("%-15s", PQgetvalue(res1, i, j));
        printf("\n");
    }

    PQclear(res1);

    /* close the portal */
    // 关闭游标
    res1 = PQexec(conn1, "CLOSE myportal");
    PQclear(res1);

    /* end the transaction */
    // 结束事务
    res1 = PQexec(conn1, "END");
    PQclear(res1);

    /* close the connections to the database and cleanup */
    // 关闭第一个数据库连接
    PQfinish(conn1);
    // 关闭第二个数据库连接
    PQfinish(conn2);

/*     fclose(debug); */
    return 0;
}