/*
 * src/test/examples/testlibpq2.c
 *
 *
 * testlibpq2.c
 *        Test of the asynchronous notification interface
 *
 * Start this program, then from psql in another window do
 *     NOTIFY TBL2;
 * Repeat four times to get this program to exit.
 *
 * Or, if you want to get fancy, try this:
 * populate a database with the following commands
 * (provided in src/test/examples/testlibpq2.sql):
 *
 *     CREATE TABLE TBL1 (i int4);
 *
 *     CREATE TABLE TBL2 (i int4);
 *
 *     CREATE RULE r1 AS ON INSERT TO TBL1 DO
 *       (INSERT INTO TBL2 VALUES (new.i); NOTIFY TBL2);
 *
 * and do this four times:
 *
 *     INSERT INTO TBL1 VALUES (10);
 */

#ifdef WIN32
#include <windows.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "libpq-fe.h"
// 定义一个静态函数exit_nicely，用于关闭数据库连接并退出程序
static void
exit_nicely(PGconn *conn)
{
    // 关闭数据库连接
    PQfinish(conn);
    // 退出程序
    exit(1);
}

int
main(int argc, char **argv)
{
    // 用于存储数据库连接信息
    const char *conninfo;
    // 用于表示数据库连接
    PGconn       *conn;
    // 用于存储查询结果
    PGresult   *res;
    // 用于表示通知
    PGnotify   *notify;
    // 用于计数通知的数量
    int            nnotifies;

    // 从命令行参数获取数据库连接信息
    if (argc > 1)
        conninfo = argv[1];
    // 默认的数据库连接信息
    else
        conninfo = "dbname = postgres";

    // 建立到数据库的连接
    conn = PQconnectdb(conninfo);

    // 检查数据库连接是否成功
    if (PQstatus(conn) != CONNECTION_OK)
    {
        // 输出连接失败信息
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        // 退出程序
        exit_nicely(conn);
    }

    // 发送LISTEN命令以监听通知
    res = PQexec(conn, "LISTEN TBL2");
    // 检查命令执行结果
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        // 输出失败信息
        fprintf(stderr, "LISTEN command failed: %s", PQerrorMessage(conn));
        // 释放查询结果对象
        PQclear(res);
        // 退出程序
        exit_nicely(conn);
    }

    // 释放查询结果对象
    PQclear(res);
    // 初始化通知计数
    nnotifies = 0;
    // 当通知数量小于4时循环
    while (nnotifies < 4)
    {
        // 用于存储套接字
        int            sock;
        // 用于select函数的文件描述符集合
        fd_set        input_mask;
        // 获取数据库连接的套接字
        sock = PQsocket(conn);
        // 如果套接字无效，跳出循环
        if (sock < 0)
            break;               
        // 清空文件描述符集合
        FD_ZERO(&input_mask);
        // 将套接字加入文件描述符集合
        FD_SET(sock, &input_mask);
        // 使用select函数等待套接字就绪
        if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
        {
            // 输出失败信息
            fprintf(stderr, "select() failed: %s\n", strerror(errno));
            // 退出程序
            exit_nicely(conn);
        }

        // 处理接收到的数据
        PQconsumeInput(conn);
        // 处理接收到的通知
        while ((notify = PQnotifies(conn)) != NULL)
        {
            fprintf(stderr,
                    "ASYNC NOTIFY of '%s' received from backend PID %d\n",
                    notify->relname, notify->be_pid);
            // 输出接收到的通知信息
            PQfreemem(notify);
            // 递增通知计数
            nnotifies++;
        }
    }
    // 输出完成信息
    fprintf(stderr, "Done.\n");
    // 关闭数据库连接
    PQfinish(conn);

    return 0;
}
