/*-------------------------------------------------------------------------
 *
 * testlo64.c
 *      test using large objects with libpq using 64-bit APIs
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *      src/test/examples/testlo64.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "libpq-fe.h"
#include "libpq/libpq-fs.h"

#define BUFSIZE            1024
static Oid
importFile(PGconn *conn, char *filename)
{
    Oid            lobjId;                // 声明变量lobjId为Oid类型
    int            lobj_fd;                // 声明变量lobj_fd为整型
    char        buf[BUFSIZE];            // 声明字符数组buf, 长度为BUFSIZE
    int            nbytes,                // 声明整型变量nbytes
                tmp;                    // 声明整型变量tmp
    int            fd;                    // 声明整型变量fd

    fd = open(filename, O_RDONLY, 0666);    // 打开指定文件，只读模式
    if (fd < 0)                                // 如果打开文件失败
    {
        fprintf(stderr, "cannot open unix file \"%s\"\n", filename);    // 输出错误信息
    }


    lobjId = lo_creat(conn, INV_READ | INV_WRITE);    // 在数据库中创建一个大型对象，并返回其Oid
    if (lobjId == 0)                                // 如果创建失败
        fprintf(stderr, "cannot create large object");    // 输出错误信息

    lobj_fd = lo_open(conn, lobjId, INV_WRITE);        // 打开指定的大型对象，以便写入

   
    while ((nbytes = read(fd, buf, BUFSIZE)) > 0)        // 循环读取文件内容到buf中
    {
        tmp = lo_write(conn, lobj_fd, buf, nbytes);        // 将buf中的数据写入到大型对象中
        if (tmp < nbytes)                            // 如果写入失败
            fprintf(stderr, "error while reading \"%s\"", filename);    // 输出错误信息
    }

    close(fd);                                    // 关闭文件
    lo_close(conn, lobj_fd);                    // 关闭大型对象

    return lobjId;                                // 返回大型对象的Oid
}

static void
pickout(PGconn *conn, Oid lobjId, pg_int64 start, int len)
{
    int            lobj_fd;                // 声明整型变量lobj_fd
    char       *buf;                    // 声明字符指针buf
    int            nbytes;                // 声明整型变量nbytes
    int            nread;                // 声明整型变量nread

    lobj_fd = lo_open(conn, lobjId, INV_READ);    // 打开指定的大型对象，以便读取
    if (lobj_fd < 0)                            // 如果打开失败
        fprintf(stderr, "cannot open large object %u", lobjId);    // 输出错误信息

    if (lo_lseek64(conn, lobj_fd, start, SEEK_SET) < 0)    // 移动大型对象的读写指针到指定位置
        fprintf(stderr, "error in lo_lseek64: %s", PQerrorMessage(conn));    // 输出错误信息

    if (lo_tell64(conn, lobj_fd) != start)        // 如果移动失败
        fprintf(stderr, "error in lo_tell64: %s", PQerrorMessage(conn));    // 输出错误信息

    buf = malloc(len + 1);                    // 为buf分配内存空间

    nread = 0;                                // 初始化nread为0
    while (len - nread > 0)                    // 循环读取大型对象的内容
    {
        nbytes = lo_read(conn, lobj_fd, buf, len - nread);    // 从大型对象中读取数据到buf中
        buf[nbytes] = '\0';                    // 在buf末尾添加字符串结束符
        fprintf(stderr, ">>> %s", buf);        // 输出buf中的内容
        nread += nbytes;                        // 更新已读取的字节数
        if (nbytes <= 0)                        // 如果读取失败
            break;                                // 退出循环
    }
    free(buf);                                // 释放buf所占用的内存空间
    fprintf(stderr, "\n");                    // 输出换行符
    lo_close(conn, lobj_fd);                    // 关闭大型对象
}

static void
overwrite(PGconn *conn, Oid lobjId, pg_int64 start, int len)
{
    int            lobj_fd;                // 声明整型变量lobj_fd
    char       *buf;                    // 声明字符指针buf
    int            nbytes;                // 声明整型变量nbytes
    int            nwritten;                // 声明整型变量nwritten
    int            i;                        // 声明整型变量i

    lobj_fd = lo_open(conn, lobjId, INV_WRITE);    // 打开指定的大型对象，以便写入
    if (lobj_fd < 0)                            // 如果打开失败
        fprintf(stderr, "cannot open large object %u", lobjId);    // 输出错误信息

    if (lo_lseek64(conn, lobj_fd, start, SEEK_SET) < 0)    // 移动大型对象的读写指针到指定位置
        fprintf(stderr, "error in lo_lseek64: %s", PQerrorMessage(conn));    // 输出错误信息

    buf = malloc(len + 1);                    // 为buf分配内存空间

    for (i = 0; i < len; i++)                    // 循环初始化buf为'X'
        buf[i] = 'X';
    buf[i] = '\0';                            // 在buf末尾添加字符串结束符

    nwritten = 0;                            // 初始化nwritten为0
    while (len - nwritten > 0)                    // 循环向大型对象中写入内容
    {
        nbytes = lo_write(conn, lobj_fd, buf + nwritten, len - nwritten);    // 向大型对象中写入数据
        nwritten += nbytes;                        // 更新已写入的字节数
        if (nbytes <= 0)                        // 如果写入失败
        {
            fprintf(stderr, "\nWRITE FAILED!\n");    // 输出错误信息
            break;                                // 退出循环
        }
    }
    free(buf);                                // 释放buf所占用的内存空间
    fprintf(stderr, "\n");                    // 输出换行符
    lo_close(conn, lobj_fd);                    // 关闭大型对象
}

static void
my_truncate(PGconn *conn, Oid lobjId, pg_int64 len)
{
    int            lobj_fd;                // 声明整型变量lobj_fd

    lobj_fd = lo_open(conn, lobjId, INV_READ | INV_WRITE);    // 打开指定的大型对象，以便读写
    if (lobj_fd < 0)                            // 如果打开失败
        fprintf(stderr, "cannot open large object %u", lobjId);    // 输出错误信息

    if (lo_truncate64(conn, lobj_fd, len) < 0)    // 截断大型对象至指定长度
        fprintf(stderr, "error in lo_truncate64: %s", PQerrorMessage(conn));    // 输出错误信息

    lo_close(conn, lobj_fd);                    // 关闭大型对象
}

static void
exportFile(PGconn *conn, Oid lobjId, char *filename)
{
    int            lobj_fd;                // 声明整型变量lobj_fd
    char        buf[BUFSIZE];            // 声明字符数组buf, 长度为BUFSIZE
    int            nbytes,                // 声明整型变量nbytes
                tmp;                    // 声明整型变量tmp
    int            fd;                    // 声明整型变量fd

    lobj_fd = lo_open(conn, lobjId, INV_READ);    // 打开指定的大型对象，以便读取
    if (lobj_fd < 0)                            // 如果打开失败
        fprintf(stderr, "cannot open large object %u", lobjId);    // 输出错误信息

    fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0666);    // 创建或打开指定文件，可写并清空文件内容
    if (fd < 0)                                // 如果打开失败
    {                           
        fprintf(stderr, "cannot open unix file \"%s\"",
                filename);                // 输出错误信息
    }

    
    while ((nbytes = lo_read(conn, lobj_fd, buf, BUFSIZE)) > 0)    // 循环从大型对象中读取内容到buf中
    {
        tmp = write(fd, buf, nbytes);            // 将buf中的数据写入到文件中
        if (tmp < nbytes)                        // 如果写入失败
        {
            fprintf(stderr, "error while writing \"%s\"",
                    filename);                // 输出错误信息
        }
    }

    lo_close(conn, lobj_fd);                    // 关闭大型对象
    close(fd);                                // 关闭文件

    return;
}

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);                            // 关闭数据库连接
    exit(1);                                // 退出程序
}

int
main(int argc, char **argv)
{
    char       *in_filename,                // 声明字符指针in_filename
               *out_filename,                // 声明字符指针out_filename
               *out_filename2;            // 声明字符指针out_filename2
    char       *database;                    // 声明字符指针database
    Oid            lobjOid;                // 声明变量lobjOid为Oid类型
    PGconn       *conn;                    // 声明指向PGconn类型对象的指针conn
    PGresult   *res;                        // 声明指向PGresult类型对象的指针res

    if (argc != 5)                            // 如果参数个数不为5
    {
        fprintf(stderr, "Usage: %s database_name in_filename out_filename out_filename2\n",
                argv[0]);                    // 输出错误信息
        exit(1);                            // 退出程序
    }

    database = argv[1];                        // 获取数据库名
    in_filename = argv[2];                    // 获取输入文件名
    out_filename = argv[3];                    // 获取输出文件名
    out_filename2 = argv[4];                    // 获取第二个输出文件名


    conn = PQsetdb(NULL, NULL, NULL, NULL, database);    // 连接数据库

    if (PQstatus(conn) != CONNECTION_OK)        // 如果连接失败
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));        // 输出错误信息
        exit_nicely(conn);                    // 退出程序
    }

    res = PQexec(conn, "begin");                // 开始事务
    PQclear(res);                            // 释放结果对象
    printf("importing file \"%s\" ...\n", in_filename);    // 输出提示信息
    lobjOid = lo_import(conn, in_filename);    // 导入文件到数据库中作为大型对象
    if (lobjOid == 0)                            // 如果导入失败
        fprintf(stderr, "%s\n", PQerrorMessage(conn));    // 输出错误信息
    else
    {
        printf("\tas large object %u.\n", lobjOid);    // 输出成功信息

        printf("picking out bytes 4294967000-4294968000 of the large object\n");    // 输出提示信息
        pickout(conn, lobjOid, 4294967000U, 1000);    // 选择大型对象的一部分内容并输出

        printf("overwriting bytes 4294967000-4294968000 of the large object with X's\n");    // 输出提示信息
        overwrite(conn, lobjOid, 4294967000U, 1000);    // 用'X'覆盖大型对象的一部分内容

        printf("exporting large object to file \"%s\" ...\n", out_filename);    // 输出提示信息
        if (lo_export(conn, lobjOid, out_filename) < 0)    // 将大型对象导出到文件
            fprintf(stderr, "%s\n", PQerrorMessage(conn));    // 输出错误信息

        printf("truncating to 3294968000 bytes\n");    // 输出提示信息
        my_truncate(conn, lobjOid, 3294968000U);    // 截断大型对象至指定长度

        printf("exporting truncated large object to file \"%s\" ...\n", out_filename2);    // 输出提示信息
        if (lo_export(conn, lobjOid, out_filename2) < 0)    // 将截断后的大型对象导出到文件
            fprintf(stderr, "%s\n", PQerrorMessage(conn));    // 输出错误信息
    }

    res = PQexec(conn, "end");                    // 提交事务
    PQclear(res);                            // 释放结果对象
    PQfinish(conn);                            // 关闭数据库连接
    return 0;                                // 返回0表示执行成功
}