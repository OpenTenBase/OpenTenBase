/*
 * contrib/btree_gist/btree_date.c
 */
#include "postgres.h"

#include "btree_gist.h"
#include "btree_utils_num.h"
#include "utils/builtins.h"
#include "utils/date.h"

typedef struct
{
    DateADT        lower;
    DateADT        upper;
} dateKEY;

/*
** date ops
*/
PG_FUNCTION_INFO_V1(gbt_date_compress);
PG_FUNCTION_INFO_V1(gbt_date_fetch);
PG_FUNCTION_INFO_V1(gbt_date_union);
PG_FUNCTION_INFO_V1(gbt_date_picksplit);
PG_FUNCTION_INFO_V1(gbt_date_consistent);
PG_FUNCTION_INFO_V1(gbt_date_distance);
PG_FUNCTION_INFO_V1(gbt_date_penalty);
PG_FUNCTION_INFO_V1(gbt_date_same);

/*
 * gbt_dategt: 比较两个日期值是否大于
 *
 * 参数：
 *   - a: 第一个日期值
 *   - b: 第二个日期值
 *   - flinfo: 函数管理信息
 *
 * 返回值：
 *   - 布尔值，指示第一个日期值是否大于第二个日期值
 */
static bool
gbt_dategt(const void* a, const void* b, FmgrInfo* flinfo)
{
    return DatumGetBool(
        DirectFunctionCall2(date_gt, DateADTGetDatum(*((const DateADT*)a)), DateADTGetDatum(*((const DateADT*)b)))
    );
}

/*
 * gbt_datege: 比较两个日期值是否大于或等于
 *
 * 参数：
 *   - a: 第一个日期值
 *   - b: 第二个日期值
 *   - flinfo: 函数管理信息
 *
 * 返回值：
 *   - 布尔值，指示第一个日期值是否大于或等于第二个日期值
 */
static bool
gbt_datege(const void* a, const void* b, FmgrInfo* flinfo)
{
    return DatumGetBool(
        DirectFunctionCall2(date_ge, DateADTGetDatum(*((const DateADT*)a)), DateADTGetDatum(*((const DateADT*)b)))
    );
}

/*
 * gbt_dateeq: 比较两个日期值是否相等
 *
 * 参数：
 *   - a: 第一个日期值
 *   - b: 第二个日期值
 *   - flinfo: 函数管理信息
 *
 * 返回值：
 *   - 布尔值，指示两个日期值是否相等
 */
static bool
gbt_dateeq(const void* a, const void* b, FmgrInfo* flinfo)
{
    return DatumGetBool(
        DirectFunctionCall2(date_eq, DateADTGetDatum(*((const DateADT*)a)), DateADTGetDatum(*((const DateADT*)b)))
    );
}

/*
 * gbt_datele: 比较两个日期值是否小于或等于
 *
 * 参数：
 *   - a: 第一个日期值
 *   - b: 第二个日期值
 *   - flinfo: 函数管理信息
 *
 * 返回值：
 *   - 布尔值，指示第一个日期值是否小于或等于第二个日期值
 */
static bool
gbt_datele(const void* a, const void* b, FmgrInfo* flinfo)
{
    return DatumGetBool(
        DirectFunctionCall2(date_le, DateADTGetDatum(*((const DateADT*)a)), DateADTGetDatum(*((const DateADT*)b)))
    );
}

/*
 * gbt_datelt: 比较两个日期值是否小于
 *
 * 参数：
 *   - a: 第一个日期值
 *   - b: 第二个日期值
 *   - flinfo: 函数管理信息
 *
 * 返回值：
 *   - 布尔值，指示第一个日期值是否小于第二个日期值
 */
static bool
gbt_datelt(const void* a, const void* b, FmgrInfo* flinfo)
{
    return DatumGetBool(
        DirectFunctionCall2(date_lt, DateADTGetDatum(*((const DateADT*)a)), DateADTGetDatum(*((const DateADT*)b)))
    );
}

/*
 * gbt_datekey_cmp: 比较两个日期范围索引项的排序
 *
 * 参数：
 *   - a: 第一个日期范围索引项
 *   - b: 第二个日期范围索引项
 *   - flinfo: 函数管理信息
 *
 * 返回值：
 *   - 整数值，表示排序结果
 */
static int
gbt_datekey_cmp(const void* a, const void* b, FmgrInfo* flinfo)
{
    dateKEY* ia = (dateKEY*)(((const Nsrt*)a)->t);
    dateKEY* ib = (dateKEY*)(((const Nsrt*)b)->t);
    int            res;

    res = DatumGetInt32(DirectFunctionCall2(date_cmp, DateADTGetDatum(ia->lower), DateADTGetDatum(ib->lower)));
    if (res == 0)
        return DatumGetInt32(DirectFunctionCall2(date_cmp, DateADTGetDatum(ia->upper), DateADTGetDatum(ib->upper)));

    return res;
}

static float8
gdb_date_dist(const void *a, const void *b, FmgrInfo *flinfo)
{
    /* we assume the difference can't overflow */
    Datum        diff = DirectFunctionCall2(date_mi,
                                           DateADTGetDatum(*((const DateADT *) a)),
                                           DateADTGetDatum(*((const DateADT *) b)));

    return (float8) Abs(DatumGetInt32(diff));
}


static const gbtree_ninfo tinfo =
{
    gbt_t_date,
    sizeof(DateADT),
    8,                            /* sizeof(gbtreekey8) */
    gbt_dategt,
    gbt_datege,
    gbt_dateeq,
    gbt_datele,
    gbt_datelt,
    gbt_datekey_cmp,
    gdb_date_dist
};


PG_FUNCTION_INFO_V1(date_dist);
Datum
date_dist(PG_FUNCTION_ARGS)
{
    /* we assume the difference can't overflow */
    Datum        diff = DirectFunctionCall2(date_mi,
                                           PG_GETARG_DATUM(0),
                                           PG_GETARG_DATUM(1));

    PG_RETURN_INT32(Abs(DatumGetInt32(diff)));
}


/**************************************************
 * date ops
 **************************************************/



Datum
gbt_date_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);

    PG_RETURN_POINTER(gbt_num_compress(entry, &tinfo));
}

Datum
gbt_date_fetch(PG_FUNCTION_ARGS)
{
    GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);

    PG_RETURN_POINTER(gbt_num_fetch(entry, &tinfo));
}

Datum
gbt_date_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    DateADT        query = PG_GETARG_DATEADT(1);
    StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);

    /* Oid        subtype = PG_GETARG_OID(3); */
    bool       *recheck = (bool *) PG_GETARG_POINTER(4);
    dateKEY    *kkk = (dateKEY *) DatumGetPointer(entry->key);
    GBT_NUMKEY_R key;

    /* All cases served by this function are exact */
    *recheck = false;

    key.lower = (GBT_NUMKEY *) &kkk->lower;
    key.upper = (GBT_NUMKEY *) &kkk->upper;

    PG_RETURN_BOOL(
                   gbt_num_consistent(&key, (void *) &query, &strategy, GIST_LEAF(entry), &tinfo, fcinfo->flinfo)
        );
}


Datum
gbt_date_distance(PG_FUNCTION_ARGS)
{
    GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    DateADT        query = PG_GETARG_DATEADT(1);

    /* Oid        subtype = PG_GETARG_OID(3); */
    dateKEY    *kkk = (dateKEY *) DatumGetPointer(entry->key);
    GBT_NUMKEY_R key;

    key.lower = (GBT_NUMKEY *) &kkk->lower;
    key.upper = (GBT_NUMKEY *) &kkk->upper;

    PG_RETURN_FLOAT8(
                     gbt_num_distance(&key, (void *) &query, GIST_LEAF(entry), &tinfo, fcinfo->flinfo)
        );
}


/*
 * gbt_date_union: 计算日期范围索引项的并集
 *
 * 参数：
 *   - PG_FUNCTION_ARGS: PostgreSQL函数参数列表
 *
 * 返回值：
 *   - 指向计算得到的日期范围索引项的指针
 */
Datum
gbt_date_union(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    void* out = palloc(sizeof(dateKEY));

    *(int*)PG_GETARG_POINTER(1) = sizeof(dateKEY);
    PG_RETURN_POINTER(gbt_num_union((void*)out, entryvec, &tinfo, fcinfo->flinfo));
}

/*
 * gbt_date_penalty: 计算日期范围索引项的惩罚值
 *
 * 参数：
 *   - PG_FUNCTION_ARGS: PostgreSQL函数参数列表
 *
 * 返回值：
 *   - 指向惩罚值的指针
 */
Datum
gbt_date_penalty(PG_FUNCTION_ARGS)
{
    dateKEY* origentry = (dateKEY*)DatumGetPointer(((GISTENTRY*)PG_GETARG_POINTER(0))->key);
    dateKEY* newentry = (dateKEY*)DatumGetPointer(((GISTENTRY*)PG_GETARG_POINTER(1))->key);
    float* result = (float*)PG_GETARG_POINTER(2);
    int32        diff,
        res;

    diff = DatumGetInt32(DirectFunctionCall2(
        date_mi,
        DateADTGetDatum(newentry->upper),
        DateADTGetDatum(origentry->upper)));

    res = Max(diff, 0);

    diff = DatumGetInt32(DirectFunctionCall2(
        date_mi,
        DateADTGetDatum(origentry->lower),
        DateADTGetDatum(newentry->lower)));

    res += Max(diff, 0);

    *result = 0.0;

    if (res > 0)
    {
        diff = DatumGetInt32(DirectFunctionCall2(
            date_mi,
            DateADTGetDatum(origentry->upper),
            DateADTGetDatum(origentry->lower)));
        *result += FLT_MIN;
        *result += (float)(res / ((double)(res + diff)));
        *result *= (FLT_MAX / (((GISTENTRY*)PG_GETARG_POINTER(0))->rel->rd_att->natts + 1));
    }

    PG_RETURN_POINTER(result);
}

/*
 * gbt_date_picksplit: 选择日期范围索引项的分裂点
 *
 * 参数：
 *   - PG_FUNCTION_ARGS: PostgreSQL函数参数列表
 *
 * 返回值：
 *   - 指向分裂向量的指针
 */
Datum
gbt_date_picksplit(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(gbt_num_picksplit(
        (GistEntryVector*)PG_GETARG_POINTER(0),
        (GIST_SPLITVEC*)PG_GETARG_POINTER(1),
        &tinfo, fcinfo->flinfo
    ));
}

/*
 * gbt_date_same: 检查两个日期范围索引项是否相同
 *
 * 参数：
 *   - PG_FUNCTION_ARGS: PostgreSQL函数参数列表
 *
 * 返回值：
 *   - 指向布尔值的指针，指示两个索引项是否相同
 */
Datum
gbt_date_same(PG_FUNCTION_ARGS)
{
    dateKEY* b1 = (dateKEY*)PG_GETARG_POINTER(0);
    dateKEY* b2 = (dateKEY*)PG_GETARG_POINTER(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);

    *result = gbt_num_same((void*)b1, (void*)b2, &tinfo, fcinfo->flinfo);
    PG_RETURN_POINTER(result);
}