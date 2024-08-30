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

// �Ƚ�����λ�ַ��� a �� b �Ƿ����
static bool
gbt_bitgt(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitgt,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// �Ƚ�����λ�ַ��� a �� b �Ƿ���ڵ���
static bool
gbt_bitge(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitge,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// �Ƚ�����λ�ַ��� a �� b �Ƿ����
static bool
gbt_biteq(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(biteq,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// �Ƚ�����λ�ַ��� a �� b �Ƿ�С�ڵ���
static bool
gbt_bitle(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitle,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// �Ƚ�����λ�ַ��� a �� b �Ƿ�С��
static bool
gbt_bitlt(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetBool(DirectFunctionCall2(bitlt,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}

// �Ƚ�����λ�ַ��� a �� b ���ֽڴ�С
static int32
gbt_bitcmp(const void* a, const void* b, Oid collation, FmgrInfo* flinfo)
{
    return DatumGetInt32(DirectFunctionCall2(byteacmp,
        PointerGetDatum(a),
        PointerGetDatum(b)));
}


// ��λ�ַ��� leaf ת��Ϊ��׼��ʽ
static bytea*
gbt_bit_xfrm(bytea* leaf)
{
    bytea* out = leaf;
    int            sz = VARBITBYTES(leaf) + VARHDRSZ;
    int            padded_sz = INTALIGN(sz);

    out = (bytea*)palloc(padded_sz);
    // ��ʼ������ֽ�Ϊ��
    while (sz < padded_sz)
        ((char*)out)[sz++] = 0;
    SET_VARSIZE(out, padded_sz);
    memcpy((void*)VARDATA(out), (void*)VARBITS(leaf), VARBITBYTES(leaf));
    return out;
}

// ��Ҷ�ӽڵ��ֵ���б��ʽת��Ϊ��׼��ʽ
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

// ����λ�ַ����� GBT �ȽϺ�����Ϣ
static const gbtree_vinfo tinfo =
{
    gbt_t_bit,  // �������ͱ�ʶ��
    0,          // �������η�
    TRUE,       // ����֧������Լ��ı�־
    gbt_bitgt,  // ���ڲ���������
    gbt_bitge,  // ���ڵ��ڲ���������
    gbt_biteq,  // ���ڲ���������
    gbt_bitle,  // С�ڵ��ڲ���������
    gbt_bitlt,  // С�ڲ���������
    gbt_bitcmp, // �ȽϺ���
    gbt_bit_l2n // ת������
};


/**************************************************
 * Bit ops
 **************************************************/

 // λ�ַ����� GiST ѹ������
Datum
gbt_bit_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);

    // ����ͨ��ѹ������ gbt_var_compress
    PG_RETURN_POINTER(gbt_var_compress(entry, &tinfo));
}

// λ�ַ����� GiST һ���Լ�麯��
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

    /* �������������������������Ǿ�ȷƥ�� */
    *recheck = false;

    if (GIST_LEAF(entry))
        // Ҷ�ӽڵ�����£����� gbt_var_consistent ����һ���Լ��
        retval = gbt_var_consistent(&r, query, strategy, PG_GET_COLLATION(),
            TRUE, &tinfo, fcinfo->flinfo);
    else
    {
        // �ڲ��ڵ�����£��Ƚ���ѯֵת��Ϊ��׼��ʽ��Ȼ����� gbt_var_consistent ����һ���Լ��
        bytea* q = gbt_bit_xfrm((bytea*)query);

        retval = gbt_var_consistent(&r, q, strategy, PG_GET_COLLATION(),
            FALSE, &tinfo, fcinfo->flinfo);
    }
    PG_RETURN_BOOL(retval);
}

// λ�ַ����� GiST ���Ϻ���
Datum
gbt_bit_union(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    int32* size = (int*)PG_GETARG_POINTER(1);

    // ����ͨ�����Ϻ��� gbt_var_union
    PG_RETURN_POINTER(gbt_var_union(entryvec, size, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo));
}

// λ�ַ����� GiST ����ѡ����
Datum
gbt_bit_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    GIST_SPLITVEC* v = (GIST_SPLITVEC*)PG_GETARG_POINTER(1);

    // ����ͨ�÷���ѡ���� gbt_var_picksplit
    gbt_var_picksplit(entryvec, v, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo);
    PG_RETURN_POINTER(v);
}

// λ�ַ����� GiST ����Լ�麯��
Datum
gbt_bit_same(PG_FUNCTION_ARGS)
{
    Datum        d1 = PG_GETARG_DATUM(0);
    Datum        d2 = PG_GETARG_DATUM(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);

    // ����ͨ������Լ�麯�� gbt_var_same
    *result = gbt_var_same(d1, d2, PG_GET_COLLATION(), &tinfo, fcinfo->flinfo);
    PG_RETURN_POINTER(result);
}

// λ�ַ����� GiST �ͷ�����
Datum
gbt_bit_penalty(PG_FUNCTION_ARGS)
{
    GISTENTRY* o = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* n = (GISTENTRY*)PG_GETARG_POINTER(1);
    float* result = (float*)PG_GETARG_POINTER(2);

    // ����ͨ�óͷ����� gbt_var_penalty
    PG_RETURN_POINTER(gbt_var_penalty(result, o, n, PG_GET_COLLATION(),
        &tinfo, fcinfo->flinfo));
}
