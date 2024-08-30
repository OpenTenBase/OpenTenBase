/*
 * src/test/examples/testlibpq4.c
 *
 *
 * testlibpq4.c
 *        此测试程序演示如何使用 LIBPQ 来建立多个后端连接
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"

static void
exit_nicely(PGconn *conn1, PGconn *conn2)
{
    if (conn1)
        PQfinish(conn1);
    if (conn2)
        PQfinish(conn2);
    exit(1);
}

static void
check_conn(PGconn *conn, const char *dbName)
{
    /* 检查后端连接是否成功建立 */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "连接到数据库 \"%s\" 失败：%s",
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
        fprintf(stderr, "用法：%s 表名 数据库名1 数据库名2\n", argv[0]);
        fprintf(stderr, "      比较两个数据库中的两个表\n");
        exit(1);
    }
    tblName = argv[1];
    dbName1 = argv[2];
    dbName2 = argv[3];


    /*
     * 开始，设置后端连接的参数 如果参数为null，则系统将尝试使用合理的默认值 通过查找环境变量或者使用硬编码的常量
     */
    pghost = NULL;                /* 后端的主机名 */
    pgport = NULL;                /* 后端的端口 */
    pgoptions = NULL;            /* 启动后端服务器的特殊选项 */
    pgtty = NULL;                /* 后端的调试 tty */

    /* 建立与数据库的连接 */
    conn1 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName1);
    check_conn(conn1, dbName1);

    conn2 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName2);
    check_conn(conn2, dbName2);

    /* 开始一个事务块 */
    res1 = PQexec(conn1, "BEGIN");
    if (PQresultStatus(res1) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "BEGIN 命令失败\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }

    /*
     * 确保在不再需要时 PQclear() PGresult 以避免内存泄漏
     */
    PQclear(res1);

    /*
     * 从 pg_database 中获取实例，pg_database 是数据库的系统目录
     */
    res1 = PQexec(conn1, "DECLARE myportal CURSOR FOR select * from pg_database");
    if (PQresultStatus(res1) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "DECLARE CURSOR 命令失败\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }
    PQclear(res1);

    res1 = PQexec(conn1, "FETCH ALL in myportal");
    if (PQresultStatus(res1) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "FETCH ALL 命令未正确返回元组\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }

    /* 首先，打印属性名 */
    nFields = PQnfields(res1);
    for (i = 0; i < nFields; i++)
        printf("%-15s", PQfname(res1, i));
    printf("\n\n");

    /* 接下来，打印实例 */
    for (i = 0; i < PQntuples(res1); i++)
    {
        for (j = 0; j < nFields; j++)
            printf("%-15s", PQgetvalue(res1, i, j));
        printf("\n");
    }

    PQclear(res1);

    /* 关闭游标 */
    res1 = PQexec(conn1, "CLOSE myportal");
    PQclear(res1);

    /* 结束事务 */
    res1 = PQexec(conn1, "END");
    PQclear(res1);

    /* 关闭与数据库的连接并进行清理 */
    PQfinish(conn1);
    PQfinish(conn2);

/*     fclose(debug); */
    return 0;
}
