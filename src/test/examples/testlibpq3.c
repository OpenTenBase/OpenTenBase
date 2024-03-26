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

// 显示二进制结果
static void
show_binary_results(PGresult *res)
{
    int            i,
                j;
    int            i_fnum,
                t_fnum,
                b_fnum;

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


        iptr = PQgetvalue(res, i, i_fnum);// 获取字段 "i" 的值作为字符串
        tptr = PQgetvalue(res, i, t_fnum);// 获取字段 "t" 的值作为字符串
        bptr = PQgetvalue(res, i, b_fnum);// 获取字段 "b" 的值作为字符串


        ival = ntohl(*((uint32_t *) iptr)); // 将网络字节顺序转换为主机字节顺序


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


    if (argc > 1)
        conninfo = argv[1];// 从命令行参数获取数据库连接信息
    else
        conninfo = "dbname = postgres";// 默认的数据库连接信息

    conn = PQconnectdb(conninfo);// 建立到数据库的连接

    // 检查数据库连接是否成功
    if (PQstatus(conn) != CONNECTION_OK)
    {
        // 输出连接失败信息
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        // 退出程序
        exit_nicely(conn);
    }

   // 设置查询参数值
    paramValues[0] = "joe's place";

    res = PQexecParams(conn,
                       "SELECT * FROM test1 WHERE t = $1",  // 执行带参数的查询
                       1,                                   // 参数个数
                       NULL,                                // 参数类型数组
                       paramValues,                         // 参数值数组        
                       NULL,                                // 参数长度数组
                       NULL,                                // 参数格式数组
                       1);                                  // 二进制结果标志

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

    // 转换整数值为网络字节顺序
    binaryIntVal = htonl((uint32_t) 2);

    // 设置二进制整数值参数
    paramValues[0] = (char *) &binaryIntVal;
    // 设置参数长度
    paramLengths[0] = sizeof(binaryIntVal);
    // 设置参数格式
    paramFormats[0] = 1;        

    res = PQexecParams(conn,
                       "SELECT * FROM test1 WHERE i = $1::int4",    // 执行带参数的查询
                       1,                                           // 参数个数
                       NULL,                                        // 参数类型数组            
                       paramValues,                                 // 参数值数组
                       paramLengths,                                // 参数长度数组
                       paramFormats,                                // 参数格式数组
                       1);                                          // 二进制结果标志

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

    // 关闭数据库连接
    PQfinish(conn);

    return 0;
}
