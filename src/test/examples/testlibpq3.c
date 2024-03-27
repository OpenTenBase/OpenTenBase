/*
 * src/test/examples/testlibpq3.c
 *
 *
 * testlibpq3.c
 *        Test out-of-line parameters and binary I/O.
 *
 * Before running this, populate a database with the following commands
 * (provided in src/test/examples/testlibpq3.sql):
 *
 * CREATE TABLE test1 (i int4, t text, b bytea);
 *
 * INSERT INTO test1 values (1, 'joe''s place', '\\000\\001\\002\\003\\004');
 * INSERT INTO test1 values (2, 'ho there', '\\004\\003\\002\\001\\000');
 *
 * The expected output is:
 *
 * tuple 0: got
 *    i = (4 bytes) 1
 *    t = (11 bytes) 'joe's place'
 *    b = (5 bytes) \000\001\002\003\004
 *
 * tuple 0: got
 *    i = (4 bytes) 2
 *    t = (8 bytes) 'ho there'
 *    b = (5 bytes) \004\003\002\001\000
 */

#ifdef WIN32
#include <windows.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include "libpq-fe.h"

/* for ntohl/htonl */
#include <netinet/in.h>
#include <arpa/inet.h>


static void
exit_nicely(PGconn *conn)
{
    // 关闭数据库连接
    PQfinish(conn);
    // 退出程序
    exit(1);
}

/*
 * This function prints a query result that is a binary-format fetch from
 * a table defined as in the comment above.  We split it out because the
 * main() function uses it twice.
 */
// 显示二进制结果
static void
show_binary_results(PGresult *res)
{
    int            i,
                j;
    int            i_fnum,
                t_fnum,
                b_fnum;

    /* Use PQfnumber to avoid assumptions about field order in result */
    i_fnum = PQfnumber(res, "i");// 获取字段 "i" 的索引号
    t_fnum = PQfnumber(res, "t");// 获取字段 "t" 的索引号
    b_fnum = PQfnumber(res, "b");// 获取字段 "b" 的索引号

    // 遍历每一行
    for (i = 0; i < PQntuples(res); i++)
    {
        // 用于存储字段值的指针
        char       *iptr;
        char       *tptr;
        char       *bptr;
        int            blen;
        int            ival;

        /* Get the field values (we ignore possibility they are null!) */
        iptr = PQgetvalue(res, i, i_fnum);// 获取字段 "i" 的值作为字符串
        tptr = PQgetvalue(res, i, t_fnum);// 获取字段 "t" 的值作为字符串
        bptr = PQgetvalue(res, i, b_fnum);// 获取字段 "b" 的值作为字符串


        /*
         * The binary representation of INT4 is in network byte order, which
         * we'd better coerce to the local byte order.
         */
        ival = ntohl(*((uint32_t *) iptr));

        /*
         * The binary representation of TEXT is, well, text, and since libpq
         * was nice enough to append a zero byte to it, it'll work just fine
         * as a C string.
         *
         * The binary representation of BYTEA is a bunch of bytes, which could
         * include embedded nulls so we have to pay attention to field length.
         */
        blen = PQgetlength(res, i, b_fnum);

        blen = PQgetlength(res, i, b_fnum);// 获取字段 "b" 的长度

        printf("tuple %d: got\n", i);// 输出行号
        printf(" i = (%d bytes) %d\n",
               PQgetlength(res, i, i_fnum), ival);// 输出字段 "i" 的值
        printf(" t = (%d bytes) '%s'\n",
               PQgetlength(res, i, t_fnum), tptr); // 输出字段 "t" 的值
        printf(" b = (%d bytes) ", blen);// 输出字段 "b" 的长度
        for (j = 0; j < blen; j++)
            printf("\\%03o", bptr[j]);// 输出字段 "b" 的二进制内容
        printf("\n\n");
    }
}

int
main(int argc, char **argv)
{
    const char *conninfo; // 用于存储数据库连接信息
    PGconn       *conn;// 表示数据库连接
    PGresult   *res;// 存储查询结果
    const char *paramValues[1];// 查询参数值数组
    int            paramLengths[1]; // 查询参数长度数组
    int            paramFormats[1];// 查询参数格式数组
    uint32_t    binaryIntVal;// 用于存储二进制整数值

    /*
     * If the user supplies a parameter on the command line, use it as the
     * conninfo string; otherwise default to setting dbname=postgres and using
     * environment variables or defaults for all other connection parameters.
     */
    if (argc > 1)
        conninfo = argv[1];// 从命令行参数获取数据库连接信息
    else
        conninfo = "dbname = postgres";// 默认的数据库连接信息

    /* Make a connection to the database */
    conn = PQconnectdb(conninfo);

    /* Check to see that the backend connection was successfully made */
    // 检查数据库连接是否成功
    if (PQstatus(conn) != CONNECTION_OK)
    {
        // 输出连接失败信息
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        // 退出程序
        exit_nicely(conn);
    }

    /*
     * The point of this program is to illustrate use of PQexecParams() with
     * out-of-line parameters, as well as binary transmission of data.
     *
     * This first example transmits the parameters as text, but receives the
     * results in binary format.  By using out-of-line parameters we can avoid
     * a lot of tedious mucking about with quoting and escaping, even though
     * the data is text.  Notice how we don't have to do anything special with
     * the quote mark in the parameter value.
     */

    /* Here is our out-of-line parameter value */
   // 设置查询参数值
    paramValues[0] = "joe's place";

    res = PQexecParams(conn,
                       "SELECT * FROM test1 WHERE t = $1",
                       1,        /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       NULL,    /* don't need param lengths since text */
                       NULL,    /* default to all text params */
                       1);        /* ask for binary results */

    // 检查查询结果
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        // 输出查询失败信息
        fprintf(stderr, "SELECT failed: %s", PQerrorMessage(conn));
        // 释放查询结果对象
        PQclear(res);
        // 退出程序
        exit_nicely(conn);
    }

    // 显示二进制结果
    show_binary_results(res);

    // 释放查询结果对象
    PQclear(res);

    /*
     * In this second example we transmit an integer parameter in binary form,
     * and again retrieve the results in binary form.
     *
     * Although we tell PQexecParams we are letting the backend deduce
     * parameter type, we really force the decision by casting the parameter
     * symbol in the query text.  This is a good safety measure when sending
     * binary parameters.
     */

    /* Convert integer value "2" to network byte order */
    // 转换整数值为网络字节顺序
    binaryIntVal = htonl((uint32_t) 2);

    /* Set up parameter arrays for PQexecParams */
    // 设置二进制整数值参数
    paramValues[0] = (char *) &binaryIntVal;
    // 设置参数长度
    paramLengths[0] = sizeof(binaryIntVal);
    // 设置参数格式
    paramFormats[0] = 1;         /* binary */

    res = PQexecParams(conn,
                       "SELECT * FROM test1 WHERE i = $1::int4",
                       1,        /* one param */
                       NULL,    /* let the backend deduce param type */
                       paramValues,
                       paramLengths,
                       paramFormats,
                       1);        /* ask for binary results */

    // 检查查询结果
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        // 输出查询失败信息
        fprintf(stderr, "SELECT failed: %s", PQerrorMessage(conn));
        // 释放查询结果对象
        PQclear(res);
        // 退出程序
        exit_nicely(conn);
    }

    // 显示二进制结果
    show_binary_results(res);

    // 释放查询结果对象
    PQclear(res);

    /* close the connection to the database and cleanup */
    // 关闭数据库连接
    PQfinish(conn);

    return 0;
}