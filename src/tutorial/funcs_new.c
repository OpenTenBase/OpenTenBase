/* src/tutorial/funcs_new.c */

/******************************************************************************
  这些是用户定义的函数，可以绑定到 Postgres 后端，并由 Postgres 调用以执行相同名称的 SQL 函数。

  这些函数的调用格式由将它们绑定到后端的 CREATE FUNCTION SQL 语句定义。

  注意：此文件显示了“新样式”函数调用约定的示例。
  有关“旧样式”的示例，请参见 funcs.c。
*****************************************************************************/

#include "postgres.h"            /* 通用 Postgres 声明 */

#include "executor/executor.h"    /* GetAttributeByName() */
#include "utils/geo_decls.h"    /* point 类型 */

PG_MODULE_MAGIC;


/* 值传递 */

PG_FUNCTION_INFO_V1(add_one);

Datum
add_one(PG_FUNCTION_ARGS)
{
    int32        arg = PG_GETARG_INT32(0);

    PG_RETURN_INT32(arg + 1);
}

/* 引用传递，固定长度 */

PG_FUNCTION_INFO_V1(add_one_float8);

Datum
add_one_float8(PG_FUNCTION_ARGS)
{
    /* FLOAT8 的宏隐藏了其传递引用的特性 */
    float8        arg = PG_GETARG_FLOAT8(0);

    PG_RETURN_FLOAT8(arg + 1.0);
}

PG_FUNCTION_INFO_V1(makepoint);

Datum
makepoint(PG_FUNCTION_ARGS)
{
    Point       *pointx = PG_GETARG_POINT_P(0);
    Point       *pointy = PG_GETARG_POINT_P(1);
    Point       *new_point = (Point *) palloc(sizeof(Point));

    new_point->x = pointx->x;
    new_point->y = pointy->y;

    PG_RETURN_POINT_P(new_point);
}

/* 引用传递，可变长度 */

PG_FUNCTION_INFO_V1(copytext);

Datum
copytext(PG_FUNCTION_ARGS)
{
    text       *t = PG_GETARG_TEXT_PP(0);

    /*
     * VARSIZE_ANY_EXHDR 是结构的字节大小，减去其头部的 VARHDRSZ 或 VARHDRSZ_SHORT。使用完整长度的头部构造副本。
     */
    text       *new_t = (text *) palloc(VARSIZE_ANY_EXHDR(t) + VARHDRSZ);

    SET_VARSIZE(new_t, VARSIZE_ANY_EXHDR(t) + VARHDRSZ);

    /*
     * VARDATA 是新结构的数据区域的指针。源可能是短数据，因此通过 VARDATA_ANY 检索其数据。
     */
    memcpy((void *) VARDATA(new_t), /* 目标 */
           (void *) VARDATA_ANY(t), /* 源 */
           VARSIZE_ANY_EXHDR(t));    /* 多少字节 */
    PG_RETURN_TEXT_P(new_t);
}

PG_FUNCTION_INFO_V1(concat_text);

Datum
concat_text(PG_FUNCTION_ARGS)
{
    text       *arg1 = PG_GETARG_TEXT_PP(0);
    text       *arg2 = PG_GETARG_TEXT_PP(1);
    int32        arg1_size = VARSIZE_ANY_EXHDR(arg1);
    int32        arg2_size = VARSIZE_ANY_EXHDR(arg2);
    int32        new_text_size = arg1_size + arg2_size + VARHDRSZ;
    text       *new_text = (text *) palloc(new_text_size);

    SET_VARSIZE(new_text, new_text_size);
    memcpy(VARDATA(new_text), VARDATA_ANY(arg1), arg1_size);
    memcpy(VARDATA(new_text) + arg1_size, VARDATA_ANY(arg2), arg2_size);
    PG_RETURN_TEXT_P(new_text);
}

/* 复合类型 */

PG_FUNCTION_INFO_V1(c_overpaid);

Datum
c_overpaid(PG_FUNCTION_ARGS)
{
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    int32        limit = PG_GETARG_INT32(1);
    bool        isnull;
    int32        salary;

    salary = DatumGetInt32(GetAttributeByName(t, "salary", &isnull));
    if (isnull)
        PG_RETURN_BOOL(false);

    /*
     * 或者，我们可能更喜欢对空工资执行 PG_RETURN_NULL()
     */

    PG_RETURN_BOOL(salary > limit);
}
