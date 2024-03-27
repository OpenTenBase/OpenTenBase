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

/*
 * importFile -
 *      import file "in_filename" into database as large object "lobjOid"
 *
 */
static Oid
importFile(PGconn *conn, char *filename)
{
    Oid            lobjId;
    int            lobj_fd;
    char        buf[BUFSIZE];
    int            nbytes,
                tmp;
    int            fd;

    /*
     * open the file to be read in
     */
     // 打开指定文件，只读模式
    fd = open(filename, O_RDONLY, 0666);
    // 如果打开文件失败
    if (fd < 0)
    {                            /* error */
        // 输出错误信息
        fprintf(stderr, "cannot open unix file\"%s\"\n", filename);
    }

    /*
     * create the large object
     */
     // 在数据库中创建一个大型对象，并返回其Oid
    lobjId = lo_creat(conn, INV_READ | INV_WRITE);
     // 如果创建失败
    if (lobjId == 0)
        // 输出错误信息
        fprintf(stderr, "cannot create large object");

     // 打开指定的大型对象，以便写入
    lobj_fd = lo_open(conn, lobjId, INV_WRITE);

    /*
     * read in from the Unix file and write to the inversion file
     */
     // 循环读取文件内容到buf中
    while ((nbytes = read(fd, buf, BUFSIZE)) > 0)
    {
        // 将buf中的数据写入到大型对象中
        tmp = lo_write(conn, lobj_fd, buf, nbytes);
        // 如果写入失败，输出错误信息
        if (tmp < nbytes)
            fprintf(stderr, "error while reading \"%s\"", filename);
    }

     // 关闭文件
    close(fd);
    // 关闭大型对象
    lo_close(conn, lobj_fd);

    return lobjId;
}

static void
pickout(PGconn *conn, Oid lobjId, pg_int64 start, int len)
{
    int            lobj_fd;
    char       *buf;
    int            nbytes;
    int            nread;

    // 打开指定的大型对象，以便读取
    lobj_fd = lo_open(conn, lobjId, INV_READ);
    // 如果打开失败，则输出错误信息
    if (lobj_fd < 0)
        fprintf(stderr, "cannot open large object %u", lobjId);

    if (lo_lseek64(conn, lobj_fd, start, SEEK_SET) < 0)
        fprintf(stderr, "error in lo_lseek64: %s", PQerrorMessage(conn));

    // 如果移动失败，输出错误信息
    if (lo_tell64(conn, lobj_fd) != start)
        fprintf(stderr, "error in lo_tell64: %s", PQerrorMessage(conn));

    // 为buf分配内存空间
    buf = malloc(len + 1);

    // 初始化nread为0
    nread = 0;
    // 循环读取大型对象的内容
    while (len - nread > 0)
    {
        // 从大型对象中读取数据到buf中
        nbytes = lo_read(conn, lobj_fd, buf, len - nread);
        // 在buf末尾添加字符串结束符
        buf[nbytes] = '\0';
        // 输出buf中的内容
        fprintf(stderr, ">>> %s", buf);
        // 更新已读取的字节数
        nread += nbytes;
        // 如果读取失败
        if (nbytes <= 0)
            break;                /* no more data? */
    }
    // 释放buf所占用的内存空间
    free(buf);
    fprintf(stderr, "\n");
    // 关闭大型对象
    lo_close(conn, lobj_fd);
}

static void
overwrite(PGconn *conn, Oid lobjId, pg_int64 start, int len)
{
    int            lobj_fd;
    char       *buf;
    int            nbytes;
    int            nwritten;
    int            i;

    // 打开指定的大型对象，以便写入
    lobj_fd = lo_open(conn, lobjId, INV_WRITE);
    // 如果打开失败， 就输出错误信息
    if (lobj_fd < 0)
        fprintf(stderr, "cannot open large object %u", lobjId);

    if (lo_lseek64(conn, lobj_fd, start, SEEK_SET) < 0)
        fprintf(stderr, "error in lo_lseek64: %s", PQerrorMessage(conn));

    // 为buf分配内存空间
    buf = malloc(len + 1);

    // 循环初始化buf为'X'
    for (i = 0; i < len; i++)
        buf[i] = 'X';
    // 在buf末尾添加字符串结束符
    buf[i] = '\0';

    // 初始化nwritten为0
    nwritten = 0;
    // 循环向大型对象中写入内容
    while (len - nwritten > 0)
    {
        // 向大型对象中写入数据
        nbytes = lo_write(conn, lobj_fd, buf + nwritten, len - nwritten);
        // 更新已写入的字节数
        nwritten += nbytes;
        // 如果写入失败
        if (nbytes <= 0)
        {
            // 输出错误信息
            fprintf(stderr, "\nWRITE FAILED!\n");
            break;
        }
    }
    // 释放buf所占用的内存空间
    free(buf);
    // 输出换行符
    fprintf(stderr, "\n");
    // 关闭大型对象
    lo_close(conn, lobj_fd);
}

static void
my_truncate(PGconn *conn, Oid lobjId, pg_int64 len)
{
    int            lobj_fd;

    // 打开指定的大型对象，以便读写
    lobj_fd = lo_open(conn, lobjId, INV_READ | INV_WRITE);
    // 如果打开失败的话， 输出错误信息
    if (lobj_fd < 0)
        fprintf(stderr, "cannot open large object %u", lobjId);

    if (lo_truncate64(conn, lobj_fd, len) < 0)
        fprintf(stderr, "error in lo_truncate64: %s", PQerrorMessage(conn));

    // 关闭大型对象
    lo_close(conn, lobj_fd);
}


/*
 * exportFile -
 *      export large object "lobjOid" to file "out_filename"
 *
 */
static void
exportFile(PGconn *conn, Oid lobjId, char *filename)
{
    int            lobj_fd;
    char        buf[BUFSIZE];
    int            nbytes,
                tmp;
    int            fd;

    /*
     * open the large object
     */
    // 打开指定的大型对象，以便读取
    lobj_fd = lo_open(conn, lobjId, INV_READ);
    // 如果打开失败， 输出错误信息
    if (lobj_fd < 0)
        fprintf(stderr, "cannot open large object %u", lobjId);

    /*
     * open the file to be written to
     */
    fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd < 0)
    {                            /* error */
        fprintf(stderr, "cannot open unix file\"%s\"",
                filename);
    }

    /*
     * read in from the inversion file and write to the Unix file
     */
    // 循环从大型对象中读取内容到buf中
    while ((nbytes = lo_read(conn, lobj_fd, buf, BUFSIZE)) > 0)
    {
        // 将buf中的数据写入到文件中
        tmp = write(fd, buf, nbytes);
        // 如果写入失败
        if (tmp < nbytes)
        {
            // 输出错误信息
            fprintf(stderr, "error while writing \"%s\"",
                    filename);
        }
    }

    // 关闭大型对象
    lo_close(conn, lobj_fd);
    // 关闭文件
    close(fd);

    return;
}

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
    char       *in_filename,
               *out_filename,
               *out_filename2;
    char       *database;
    Oid            lobjOid;
    PGconn       *conn;
    PGresult   *res;

    // 如果参数个数不为5，输出错误信息，退出程序
    if (argc != 5)
    {
        fprintf(stderr, "Usage: %s database_name in_filename out_filename out_filename2\n",
                argv[0]);
        exit(1);
    }

    // 获取数据库名
    database = argv[1];
    // 获取输入文件名
    in_filename = argv[2];
    // 获取输出文件名
    out_filename = argv[3];
    // 获取第二个输出文件名
    out_filename2 = argv[4];

    /*
     * set up the connection
     */
     // 连接数据库
    conn = PQsetdb(NULL, NULL, NULL, NULL, database);

    /* check to see that the backend connection was successfully made */
    // 如果连接失败
    if (PQstatus(conn) != CONNECTION_OK)
    {
        // 输出错误信息，退出程序
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    // 开始事务
    res = PQexec(conn, "begin");
   // 释放结果对象
    PQclear(res);
    // 输出提示信息
    printf("importing file \"%s\" ...\n", in_filename);
/*    lobjOid = importFile(conn, in_filename); */
    // 导入文件到数据库中作为大型对象
    lobjOid = lo_import(conn, in_filename);
    // 如果导入失败
    if (lobjOid == 0)
        // 输出错误信息
        fprintf(stderr, "%s\n", PQerrorMessage(conn));
    else
    {
        // 输出成功信息
        printf("\tas large object %u.\n", lobjOid);

        // 输出提示信息，挑出大对象的4294967000-4294968000字节
        printf("picking out bytes 4294967000-4294968000 of the large object\n");
        // 选择大型对象的一部分内容并输出
        pickout(conn, lobjOid, 4294967000U, 1000);

        // 输出提示信息，用 X 覆盖大型对象的字节 4294967000-4294968000
        printf("overwriting bytes 4294967000-4294968000 of the large object with X's\n");
        // 用'X'覆盖大型对象的一部分内容
        overwrite(conn, lobjOid, 4294967000U, 1000);

        // 输出提示信息，将截断的大型对象导出到文件
        printf("exporting large object to file \"%s\" ...\n", out_filename);
/*        exportFile(conn, lobjOid, out_filename); */
        if (lo_export(conn, lobjOid, out_filename) < 0)
            fprintf(stderr, "%s\n", PQerrorMessage(conn));

        // 输出提示信息，截断到 3294968000 字节
        printf("truncating to 3294968000 bytes\n");
        // 截断大型对象至指定长度
        my_truncate(conn, lobjOid, 3294968000U);

        // 输出提示信息，将截断的大型对象导出到文件
        printf("exporting truncated large object to file \"%s\" ...\n", out_filename2);
        if (lo_export(conn, lobjOid, out_filename2) < 0)
            fprintf(stderr, "%s\n", PQerrorMessage(conn));
    }

     // 提交事务
    res = PQexec(conn, "end");
    // 释放结果对象
    PQclear(res);
    // 关闭数据库连接
    PQfinish(conn);
    return 0;
}