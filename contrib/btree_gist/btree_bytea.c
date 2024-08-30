/*
 * contrib/btree_gist/btree_bytea.c
 */
#include "postgres.h"

#include "btree_gist.h"
#include "btree_utils_var.h"
#include "utils/builtins.h"
#include "utils/bytea.h"


/*
** Bytea ops
*/
PG_FUNCTION_INFO_V1(gbt_bytea_compress);
PG_FUNCTION_INFO_V1(gbt_bytea_union);
PG_FUNCTION_INFO_V1(gbt_bytea_picksplit);
PG_FUNCTION_INFO_V1(gbt_bytea_consistent);
PG_FUNCTION_INFO_V1(gbt_bytea_penalty);
PG_FUNCTION_INFO_V1(gbt_bytea_same);


/* define for comparison */

// 判断字节数组a是否大于字节数组b，使用byteagt函数实现
static bool
gbt_byteagt(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(byteagt,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 判断字节数组a是否大于等于字节数组b，使用byteage函数实现
static bool
gbt_byteage(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(byteage,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 判断字节数组a是否等于字节数组b，使用byteaeq函数实现
static bool
gbt_byteaeq(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(byteaeq,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 判断字节数组a是否小于等于字节数组b，使用byteale函数实现
static bool
gbt_byteale(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(byteale,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 判断字节数组a是否小于字节数组b，使用bytealt函数实现
static bool
gbt_bytealt(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bytealt,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 比较字节数组a和字节数组b的大小，使用byteacmp函数实现
static int32
gbt_byteacmp(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetInt32(DirectFunctionCall2(byteacmp,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 字节数组的 GiST 变量信息
static const gbtree_vinfo tinfo =
{
    gbt_t_bytea,   // 数据类型标识符为字节数组类型
    0,             // 不需要任何附加标识
    TRUE,          // 允许下推操作符（pushdown）
    gbt_byteagt,   // 大于操作的函数指针
    gbt_byteage,   // 大于等于操作的函数指针
    gbt_byteaeq,   // 等于操作的函数指针
    gbt_byteale,   // 小于等于操作的函数指针
    gbt_bytealt,   // 小于操作的函数指针
    gbt_byteacmp,  // 比较操作的函数指针
    NULL           // 可选的支持函数指针，这里为NULL
};


/**************************************************
 * Text ops
 **************************************************/


Datum
gbt_bytea_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);

    PG_RETURN_POINTER(gbt_var_compress(entry, &tinfo));
}



Datum
gbt_bytea_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    void       *query = (void *) DatumGetByteaP(PG_GETARG_DATUM(1));
    StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);

    /* Oid        subtype = PG_GETARG_OID(3); */
    bool       *recheck = (bool *) PG_GETARG_POINTER(4);
    bool        retval;
    GBT_VARKEY *key = (GBT_VARKEY *) DatumGetPointer(entry->key);
    GBT_VARKEY_R r = gbt_var_key_readable(key);

    /* All cases served by this function are exact */
    *recheck = false;

    retval = gbt_var_consistent(&r, query, strategy, PG_GET_COLLATION(),
                                GIST_LEAF(entry), &tinfo, fcinfo->flinfo);
    PG_RETURN_BOOL(retval);
}



// 计算字节数组类型的 GiST 索引中两个项的并集
Datum
gbt_bytea_union(PG_FUNCTION_ARGS)
{
    // 获取 GiST 入口向量和大小
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    int32* size = (int*)PG_GETARG_POINTER(1);

    // 返回字节数组的并集
    PG_RETURN_POINTER(gbt_var_union(entryvec, size, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo));
}

// 在字节数组类型的 GiST 索引中选择一个分割点
Datum
gbt_bytea_picksplit(PG_FUNCTION_ARGS)
{
    // 获取 GiST 入口向量和分割向量
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    GIST_SPLITVEC* v = (GIST_SPLITVEC*)PG_GETARG_POINTER(1);

    // 选择分割点
    gbt_var_picksplit(entryvec, v, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo);

    // 返回分割向量
    PG_RETURN_POINTER(v);
}

// 检查字节数组类型的 GiST 索引中的两个项是否相同
Datum
gbt_bytea_same(PG_FUNCTION_ARGS)
{
    // 获取输入参数
    Datum        d1 = PG_GETARG_DATUM(0);
    Datum        d2 = PG_GETARG_DATUM(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);

    // 检查两个项是否相同
    *result = gbt_var_same(d1, d2, PG_GET_COLLATION(), &tinfo, fcinfo->flinfo);

    // 返回结果
    PG_RETURN_POINTER(result);
}

// 计算字节数组类型的 GiST 索引中的惩罚值
Datum
gbt_bytea_penalty(PG_FUNCTION_ARGS)
{
    // 获取输入参数
    GISTENTRY* o = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* n = (GISTENTRY*)PG_GETARG_POINTER(1);
    float* result = (float*)PG_GETARG_POINTER(2);

    // 计算惩罚值
    PG_RETURN_POINTER(gbt_var_penalty(result, o, n, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo));
}
