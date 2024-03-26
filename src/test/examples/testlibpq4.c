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
    // 检查数据库连接状态，如果连接失败则打印错误信息并退出程序
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

    pghost = NULL;                // 连接数据库时使用默认主机
    pgport = NULL;                // 连接数据库时使用默认端口
    pgoptions = NULL;             // 连接数据库时使用默认选项
    pgtty = NULL;                 // 连接数据库时使用默认TTY设备

    conn1 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName1);  // 连接到第一个数据库
    check_conn(conn1, dbName1);

    conn2 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName2);  // 连接到第二个数据库
    check_conn(conn2, dbName2);

    res1 = PQexec(conn1, "BEGIN");  // 在第一个数据库上开始一个事务
    if (PQresultStatus(res1) != PGRES_COMMAND_OK)
    {
        // "BEGIN"命令执行失败，打印错误信息并退出程序
        fprintf(stderr, "BEGIN command failed\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }
    PQclear(res1);

    res1 = PQexec(conn1, "DECLARE myportal CURSOR FOR select * from pg_database");  // 声明一个游标
    if (PQresultStatus(res1) != PGRES_COMMAND_OK)
    {
        // 声明游标命令执行失败，打印错误信息并退出程序
        fprintf(stderr, "DECLARE CURSOR command failed\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }
    PQclear(res1);

    res1 = PQexec(conn1, "FETCH ALL in myportal");  // 获取游标中的所有结果
    if (PQresultStatus(res1) != PGRES_TUPLES_OK)
    {
        // 获取游标结果命令执行失败，打印错误信息并退出程序
        fprintf(stderr, "FETCH ALL command didn't return tuples properly\n");
        PQclear(res1);
        exit_nicely(conn1, conn2);
    }

    nFields = PQnfields(res1);  // 获取查询结果的列数
    for (i = 0; i < nFields; i++)
        printf("%-15s", PQfname(res1, i));  // 打印每个字段的名称
    printf("\n\n");

    for (i = 0; i < PQntuples(res1); i++)
    {
        for (j = 0; j < nFields; j++)
            printf("%-15s", PQgetvalue(res1, i, j));  // 打印每行记录的值
        printf("\n");
    }

    PQclear(res1);

    res1 = PQexec(conn1, "CLOSE myportal");  // 关闭游标
    PQclear(res1);

    res1 = PQexec(conn1, "END");  // 结束事务
    PQclear(res1);

    PQfinish(conn1);  // 关闭第一个数据库连接
    PQfinish(conn2);  // 关闭第二个数据库连接

    return 0;
}
``````c
static void
exit_nicely(PGconn *conn1, PGconn *conn2)
{
    if (conn1)
        PQfinish(conn1); // 关闭连接1
    if (conn2)
        PQfinish(conn2); // 关闭连接2
    exit(1); // 退出程序
}

static void
check_conn(PGconn *conn, const char *dbName)
{
    if (PQstatus(conn) != CONNECTION_OK) // 检查连接状态
    {
        fprintf(stderr, "Connection to database \"%s\" failed: %s",
                dbName, PQerrorMessage(conn)); // 输出连接失败信息
        exit(1); // 退出程序
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

    PGresult   *res1;

    if (argc != 4) // 检查命令行参数
    {
        fprintf(stderr, "usage: %s tableName dbName1 dbName2\n", argv[0]); // 输出程序用法
        fprintf(stderr, "      compares two tables in two databases\n");
        exit(1); // 退出程序
    }
    tblName = argv[1]; // 获取表名
    dbName1 = argv[2]; // 获取数据库名1
    dbName2 = argv[3]; // 获取数据库名2

    pghost = NULL;                // PostgreSQL服务器地址
    pgport = NULL;                // PostgreSQL服务器端口
    pgoptions = NULL;             // 配置选项
    pgtty = NULL;                 // tty

    conn1 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName1); // 建立与数据库1的连接
    check_conn(conn1, dbName1); // 检查连接状态

    conn2 = PQsetdb(pghost, pgport, pgoptions, pgtty, dbName2); // 建立与数据库2的连接
    check_conn(conn2, dbName2); // 检查连接状态

    res1 = PQexec(conn1, "BEGIN"); // 执行事务开始命令
    if (PQresultStatus(res1) != PGRES_COMMAND_OK) // 检查执行结果状态
    {
        fprintf(stderr, "BEGIN command failed\n"); // 输出事务开始失败信息
        PQclear(res1);
        exit_nicely(conn1, conn2); // 退出程序
    }

    PQclear(res1); // 释放结果资源

    res1 = PQexec(conn1, "DECLARE myportal CURSOR FOR select * from pg_database"); // 声明游标
    if (PQresultStatus(res1) != PGRES_COMMAND_OK) // 检查执行结果状态
    {
        fprintf(stderr, "DECLARE CURSOR command failed\n"); // 输出声明游标失败信息
        PQclear(res1);
        exit_nicely(conn1, conn2); // 退出程序
    }
    PQclear(res1); // 释放结果资源

    res1 = PQexec(conn1, "FETCH ALL in myportal"); // 检索所有数据
    if (PQresultStatus(res1) != PGRES_TUPLES_OK) // 检查执行结果状态
    {
        fprintf(stderr, "FETCH ALL command didn't return tuples properly\n"); // 输出检索数据失败信息
        PQclear(res1);
        exit_nicely(conn1, conn2); // 退出程序
    }

    nFields = PQnfields(res1); // 获取字段数量
    for (i = 0; i < nFields; i++) // 遍历字段
        printf("%-15s", PQfname(res1, i)); // 输出字段名
    printf("\n\n");

    for (i = 0; i < PQntuples(res1); i++) // 遍历结果集
    {
        for (j = 0; j < nFields; j++) // 遍历字段
            printf("%-15s", PQgetvalue(res1, i, j)); // 输出字段值
        printf("\n");
    }

    PQclear(res1); // 释放结果资源

    res1 = PQexec(conn1, "CLOSE myportal"); // 关闭游标
    PQclear(res1); // 释放结果资源

    res1 = PQexec(conn1, "END"); // 执行事务结束命令
    PQclear(res1); // 释放结果资源

    PQfinish(conn1); // 关闭连接1
    PQfinish(conn2); // 关闭连接2

    return 0; // 返回0，表示正常退出
}