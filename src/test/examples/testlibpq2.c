/*
 * src/test/examples/testlibpq2.c
 *
 *
 * testlibpq2.c
 *        异步通知接口测试
 *
 * 运行此程序后，在另一个窗口的 psql 中执行以下操作：
 *     NOTIFY TBL2;
 * 重复四次以使该程序退出。
 *
 * 或者，如果你想更加复杂一些，可以尝试以下操作：
 * 使用以下命令在数据库中创建表（在 src/test/examples/testlibpq2.sql 中提供）：
 *
 *     CREATE TABLE TBL1 (i int4);
 *
 *     CREATE TABLE TBL2 (i int4);
 *
 *     CREATE RULE r1 AS ON INSERT TO TBL1 DO
 *       (INSERT INTO TBL2 VALUES (new.i); NOTIFY TBL2);
 *
 * 并且执行以下操作四次：
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

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

int
main(int argc, char **argv)
{
    const char *conninfo;
    PGconn       *conn;
    PGresult   *res;
    PGnotify   *notify;
    int            nnotifies;

    /*
     * 如果用户在命令行上提供了参数，则将其作为 conninfo 字符串使用；
     * 否则默认为设置 dbname=postgres，并使用环境变量或默认值设置所有其他连接参数。
     */
    if (argc > 1)
        conninfo = argv[1];
    else
        conninfo = "dbname = postgres";

    /* 连接到数据库 */
    conn = PQconnectdb(conninfo);

    /* 检查后端连接是否成功建立 */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "连接到数据库失败：%s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    /*
     * 发送 LISTEN 命令以启用规则的 NOTIFY 通知。
     */
    res = PQexec(conn, "LISTEN TBL2");
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        fprintf(stderr, "LISTEN 命令失败：%s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    /*
     * 使用 PQclear 清理 PGresult，以避免内存泄漏。
     */
    PQclear(res);

    /* 收到四次通知后退出。 */
    nnotifies = 0;
    while (nnotifies < 4)
    {
        /*
         * 等待连接上发生事件。我们使用 select(2) 来等待输入，
         * 但也可以使用 poll() 或类似的功能。
         */
        int            sock;
        fd_set        input_mask;

        sock = PQsocket(conn);

        if (sock < 0)
            break;                /* 不应该发生 */

        FD_ZERO(&input_mask);
        FD_SET(sock, &input_mask);

        if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
        {
            fprintf(stderr, "select() 失败：%s\n", strerror(errno));
            exit_nicely(conn);
        }

        /* 检查是否有输入 */
        PQconsumeInput(conn);
        while ((notify = PQnotifies(conn)) != NULL)
        {
            fprintf(stderr,
                    "从后端 PID %d 接收到关于 '%s' 的异步通知\n",
                    notify->be_pid, notify->relname);
            PQfreemem(notify);
            nnotifies++;
        }
    }

    fprintf(stderr, "完成。\n");

    /* 关闭与数据库的连接并清理 */
    PQfinish(conn);

    return 0;
}
