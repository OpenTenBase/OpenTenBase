/*
 * contrib/btree_gist/btree_bit.c
 */
#include "postgres.h"

#include "btree_gist.h"
#include "btree_utils_var.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/varbit.h"


/*
** Bit ops
*/
PG_FUNCTION_INFO_V1(gbt_bit_compress);
PG_FUNCTION_INFO_V1(gbt_bit_union);
PG_FUNCTION_INFO_V1(gbt_bit_picksplit);
PG_FUNCTION_INFO_V1(gbt_bit_consistent);
PG_FUNCTION_INFO_V1(gbt_bit_penalty);
PG_FUNCTION_INFO_V1(gbt_bit_same);


/* define for comparison */

// 比较两个位字符串 a 和 b 是否大于
static bool
gbt_bitgt(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitgt,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 比较两个位字符串 a 和 b 是否大于等于
static bool
gbt_bitge(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitge,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 比较两个位字符串 a 和 b 是否等于
static bool
gbt_biteq(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(biteq,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 比较两个位字符串 a 和 b 是否小于等于
static bool
gbt_bitle(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitle,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 比较两个位字符串 a 和 b 是否小于
static bool
gbt_bitlt(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitlt,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// 比较两个位字符串 a 和 b 的字节大小
static int32
gbt_bitcmp(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetInt32(DirectFunctionCall2(byteacmp,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}


// 将位字符串 leaf 转换为标准格式
static bytea*
gbt_bit_xfrm(bytea* leaf)
{
    bytea* out = leaf;
    int            sz = VARBITBYTES(leaf) + VARHDRSZ;
    int            padded_sz = INTALIGN(sz);

    out = (bytea*)palloc(padded_sz);
    // 初始化填充字节为零
    while (sz < padded_sz)
        ((char*)out)[sz++] = 0;
    SET_VARSIZE(out, padded_sz);
    memcpy((void*)VARDATA(out), (void*)VARBITS(leaf), VARBITBYTES(leaf));
    return out;
}

// 将叶子节点的值从列表格式转换为标准格式
static GBT_VARKEY*
gbt_bit_l2n(GBT_VARKEY* leaf, FmgrInfo* flinfo)
{
    GBT_VARKEY* out = leaf;
    GBT_VARKEY_R r = gbt_var_key_readable(leaf);
    bytea* o;

    o = gbt_bit_xfrm(r.lower);
    r.upper = r.lower = o;
    out = gbt_var_key_copy(&r);
    pfree(o);

    return out;
}

// 定义位字符串的 GBT 比较函数信息
static const gbtree_vinfo tinfo =
{
    gbt_t_bit,  // 数据类型标识符
    0,          // 类型修饰符
    TRUE,       // 用于支持相等性检查的标志
    gbt_bitgt,  // 大于操作符函数
    gbt_bitge,  // 大于等于操作符函数
    gbt_biteq,  // 等于操作符函数
    gbt_bitle,  // 小于等于操作符函数
    gbt_bitlt,  // 小于操作符函数
    gbt_bitcmp, // 比较函数
    gbt_bit_l2n // 转换函数
};


/**************************************************
 * Bit ops
 **************************************************/

 // 位字符串的 GiST 压缩函数
Datum
gbt_bit_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);

    // 调用通用压缩函数 gbt_var_compress
    PG_RETURN_POINTER(gbt_var_compress(entry, &tinfo));
}

// 位字符串的 GiST 一致性检查函数
Datum
gbt_bit_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    void* query = (void*)DatumGetByteaP(PG_GETARG_DATUM(1));
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    /* Oid        subtype = PG_GETARG_OID(3); */
    bool* recheck = (bool*)PG_GETARG_POINTER(4);
    bool        retval;
    GBT_VARKEY* key = (GBT_VARKEY*)DatumGetPointer(entry->key);
    GBT_VARKEY_R r = gbt_var_key_readable(key);

    /* 所有情况都由这个函数处理，都是精确匹配 */
    *recheck = false;

    if (GIST_LEAF(entry))
        // 叶子节点情况下，调用 gbt_var_consistent 进行一致性检查
        retval = gbt_var_consistent(&r, query, strategy, PG_GET_COLLATION(),
            TRUE, &tinfo, fcinfo->flinfo);
    else
    {
        // 内部节点情况下，先将查询值转换为标准格式，然后调用 gbt_var_consistent 进行一致性检查
        bytea* q = gbt_bit_xfrm((bytea*)query);

        retval = gbt_var_consistent(&r, q, strategy, PG_GET_COLLATION(),
            FALSE, &tinfo, fcinfo->flinfo);
    }
    PG_RETURN_BOOL(retval);
}

// 位字符串的 GiST 联合函数
Datum
gbt_bit_union(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    int32* size = (int*)PG_GETARG_POINTER(1);

    // 调用通用联合函数 gbt_var_union
    PG_RETURN_POINTER(gbt_var_union(entryvec, size, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo));
}

// 位字符串的 GiST 分裂选择函数
Datum
gbt_bit_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    GIST_SPLITVEC* v = (GIST_SPLITVEC*)PG_GETARG_POINTER(1);

    // 调用通用分裂选择函数 gbt_var_picksplit
    gbt_var_picksplit(entryvec, v, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo);
    PG_RETURN_POINTER(v);
}

// 位字符串的 GiST 相等性检查函数
Datum
gbt_bit_same(PG_FUNCTION_ARGS)
{
    Datum        d1 = PG_GETARG_DATUM(0);
    Datum        d2 = PG_GETARG_DATUM(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);

    // 调用通用相等性检查函数 gbt_var_same
    *result = gbt_var_same(d1, d2, PG_GET_COLLATION(), &tinfo, fcinfo->flinfo);
    PG_RETURN_POINTER(result);
}

// 位字符串的 GiST 惩罚函数
Datum
gbt_bit_penalty(PG_FUNCTION_ARGS)
{
    GISTENTRY* o = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* n = (GISTENTRY*)PG_GETARG_POINTER(1);
    float* result = (float*)PG_GETARG_POINTER(2);

    // 调用通用惩罚函数 gbt_var_penalty
    PG_RETURN_POINTER(gbt_var_penalty(result, o, n, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo));
}
