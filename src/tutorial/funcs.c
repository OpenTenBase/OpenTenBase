/* src/tutorial/funcs.c */

/******************************************************************************
  这些是用户定义的函数，可以绑定到Postgres后端，并由Postgres调用执行同名的SQL函数。

  这些函数的调用格式由将它们绑定到后端的CREATE FUNCTION SQL语句定义。

  注意：此文件显示了“旧风格”函数调用约定的示例。请参阅 funcs_new.c 以查看“新风格”的示例。
*****************************************************************************/

#include "postgres.h"            /* 一般Postgres声明 */

#include "executor/executor.h"    /* 用于GetAttributeByName() */
#include "utils/geo_decls.h"    /* 用于点类型 */

PG_MODULE_MAGIC;

/* 这些原型只是为了防止可能的gcc警告。 */

int            add_one(int arg);
float8       *add_one_float8(float8 *arg);
Point       *makepoint(Point *pointx, Point *pointy);
text       *copytext(text *t);
text       *concat_text(text *arg1, text *arg2);
bool c_overpaid(HeapTupleHeader t,        /* EMP的当前实例 */
           int32 limit);


/* 传值 */

int
add_one(int arg)
{
    return arg + 1;
}

/* 引用传递，固定长度 */

float8 *
add_one_float8(float8 *arg)
{
    float8       *result = (float8 *) palloc(sizeof(float8));

    *result = *arg + 1.0;

    return result;
}

Point *
makepoint(Point *pointx, Point *pointy)
{
    Point       *new_point = (Point *) palloc(sizeof(Point));

    new_point->x = pointx->x;
    new_point->y = pointy->y;

    return new_point;
}

/* 引用传递，可变长度 */

text *
copytext(text *t)
{
    /*
     * VARSIZE是结构的总大小（以字节为单位）。
     */
    text       *new_t = (text *) palloc(VARSIZE(t));

    SET_VARSIZE(new_t, VARSIZE(t));

    /*
     * VARDATA是结构的数据区域的指针。
     */
    memcpy((void *) VARDATA(new_t), /* 目标 */
           (void *) VARDATA(t), /* 源 */
           VARSIZE(t) - VARHDRSZ);    /* 复制的字节数 */
    return new_t;
}

text *
concat_text(text *arg1, text *arg2)
{
    int32        arg1_size = VARSIZE(arg1) - VARHDRSZ;
    int32        arg2_size = VARSIZE(arg2) - VARHDRSZ;
    int32        new_text_size = arg1_size + arg2_size + VARHDRSZ;
    text       *new_text = (text *) palloc(new_text_size);

    SET_VARSIZE(new_text, new_text_size);
    memcpy(VARDATA(new_text), VARDATA(arg1), arg1_size);
    memcpy(VARDATA(new_text) + arg1_size, VARDATA(arg2), arg2_size);
    return new_text;
}

/* 复合类型 */

bool
c_overpaid(HeapTupleHeader t,    /* EMP的当前实例 */
           int32 limit)
{
    bool        isnull;
    int32        salary;

    salary = DatumGetInt32(GetAttributeByName(t, "salary", &isnull));
    if (isnull)
        return false;
    return salary > limit;
}
