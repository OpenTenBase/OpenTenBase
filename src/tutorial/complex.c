/*
 * src/tutorial/complex.c
 *
 ******************************************************************************
 * 本文件包含可以绑定到Postgres后端并由后端在处理查询过程中调用的例程。
 * 这些例程的调用格式由Postgres架构决定。
 ******************************************************************************
 */

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"        /* 需要用于发送/接收函数 */

PG_MODULE_MAGIC;  // 声明模块魔术代码，用于动态加载模块

typedef struct Complex
{
    double        x;  // 实部
    double        y;  // 虚部
} Complex;  // 定义复数结构体



/*****************************************************************************
 * 输入/输出函数
 *****************************************************************************/

PG_FUNCTION_INFO_V1(complex_in);

Datum
complex_in(PG_FUNCTION_ARGS)
{
    char       *str = PG_GETARG_CSTRING(0);  // 获取输入的字符串
    double        x,
                y;
    Complex    *result;

    // 从输入字符串中读取实部和虚部
    if (sscanf(str, " ( %lf , %lf )", &x, &y) != 2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for complex: \"%s\"",
                        str)));

    result = (Complex *) palloc(sizeof(Complex));  // 为结果分配内存空间
    result->x = x;  // 设置实部
    result->y = y;  // 设置虚部
    PG_RETURN_POINTER(result);  // 返回指向结果的指针
}

PG_FUNCTION_INFO_V1(complex_out);

Datum
complex_out(PG_FUNCTION_ARGS)
{
    Complex    *complex = (Complex *) PG_GETARG_POINTER(0);  // 获取输入的复数指针
    char       *result;

    // 格式化输出字符串
    result = psprintf("(%g,%g)", complex->x, complex->y);
    PG_RETURN_CSTRING(result);  // 返回格式化后的字符串
}

/*****************************************************************************
 * 二进制输入/输出函数
 *
 * 这些是可选的。
 *****************************************************************************/

PG_FUNCTION_INFO_V1(complex_recv);

Datum
complex_recv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);  // 获取输入的二进制数据
    Complex    *result;

    // 从二进制数据中读取实部和虚部
    result = (Complex *) palloc(sizeof(Complex));
    result->x = pq_getmsgfloat8(buf);
    result->y = pq_getmsgfloat8(buf);
    PG_RETURN_POINTER(result);  // 返回指向结果的指针
}

PG_FUNCTION_INFO_V1(complex_send);

Datum
complex_send(PG_FUNCTION_ARGS)
{
    Complex    *complex = (Complex *) PG_GETARG_POINTER(0);  // 获取输入的复数指针
    StringInfoData buf;

    // 开始构建二进制数据
    pq_begintypsend(&buf);
    pq_sendfloat8(&buf, complex->x);  // 发送实部
    pq_sendfloat8(&buf, complex->y);  // 发送虚部
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));  // 返回二进制数据
}

/*****************************************************************************
 * 新操作符
 *
 * 一个实用的复数数据类型当然会提供更多功能。
 *****************************************************************************/

PG_FUNCTION_INFO_V1(complex_add);

Datum
complex_add(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);  // 获取第一个复数
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);  // 获取第二个复数
    Complex    *result;

    // 计算两个复数的和
    result = (Complex *) palloc(sizeof(Complex));
    result->x = a->x + b->x;  // 实部相加
    result->y = a->y + b->y;  // 虚部相加
    PG_RETURN_POINTER(result);  // 返回指向结果的指针
}



/*****************************************************************************
 * Operator class for defining B-tree index
 *
 * It's essential that the comparison operators and support function for a
 * B-tree index opclass always agree on the relative ordering of any two
 * data values.  Experience has shown that it's depressingly easy to write
 * unintentionally inconsistent functions.  One way to reduce the odds of
 * making a mistake is to make all the functions simple wrappers around
 * an internal three-way-comparison function, as we do here.
 *****************************************************************************/

#define Mag(c)    ((c)->x*(c)->x + (c)->y*(c)->y)

static int
complex_abs_cmp_internal(Complex * a, Complex * b)
{
    double        amag = Mag(a),
                bmag = Mag(b);

    if (amag < bmag)
        return -1;
    if (amag > bmag)
        return 1;
    return 0;
}


PG_FUNCTION_INFO_V1(complex_abs_lt);

Datum
complex_abs_lt(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(complex_abs_le);

Datum
complex_abs_le(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(complex_abs_eq);

Datum
complex_abs_eq(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(complex_abs_ge);

Datum
complex_abs_ge(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(complex_abs_gt);

Datum
complex_abs_gt(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(complex_abs_cmp);

Datum
complex_abs_cmp(PG_FUNCTION_ARGS)
{
    Complex    *a = (Complex *) PG_GETARG_POINTER(0);
    Complex    *b = (Complex *) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(complex_abs_cmp_internal(a, b));
}
