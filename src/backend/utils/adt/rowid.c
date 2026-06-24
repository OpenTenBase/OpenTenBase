
#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "storage/itemptr.h"
#include "utils/builtins.h"
#include "opentenbase_ora/opentenbase_ora.h"
#include "access/hash.h"


#define Uint32B64Len()	(b64_enc_len(NULL, sizeof(uint32)))
#define RowIdB64Len()	(b64_enc_len(NULL, sizeof(RowId)) - 1)

#define OrclRowIdTextLen() (Uint32B64Len() + Uint32B64Len() + RowIdB64Len())

static int rowid_compare(const OpenTenBaseOraRowID *l, const OpenTenBaseOraRowID *r);

Datum
rowid_in(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*rowid;
	const char	*str;

	str = PG_GETARG_CSTRING(0);
	Assert(str);
	if(strlen(str) != OrclRowIdTextLen())
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid argument string length")));

	rowid = palloc(sizeof(OpenTenBaseOraRowID));
	b64_decode_safe(str, Uint32B64Len(), (char*) &(rowid->gx_node_id), sizeof(rowid->gx_node_id));
	b64_decode_safe(str + Uint32B64Len(), Uint32B64Len(), (char*) &(rowid->reloid), sizeof(rowid->reloid));
	b64_decode_safe(str + Uint32B64Len() + Uint32B64Len(), RowIdB64Len(), (char*) &(rowid->tuple_id), sizeof(rowid->tuple_id));

	PG_RETURN_POINTER(rowid);
}

Datum
rowid_out(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*rowid = (OpenTenBaseOraRowID*) PG_GETARG_POINTER(0);
	char		*output = palloc0(OrclRowIdTextLen() + 1);

	b64_encode_safe((char*) &(rowid->gx_node_id), sizeof(rowid->gx_node_id), output, Uint32B64Len());
	b64_encode_safe((char*) &(rowid->reloid), sizeof(rowid->reloid),
					output + Uint32B64Len(), Uint32B64Len());
	b64_encode_safe((char*) &(rowid->tuple_id), sizeof(rowid->tuple_id),
					output + Uint32B64Len() + Uint32B64Len(), RowIdB64Len());

	PG_RETURN_CSTRING(output);
}

Datum
rowid_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID	*rowid = palloc(sizeof(OpenTenBaseOraRowID));

	rowid->gx_node_id = pq_getmsgint(buf, sizeof(rowid->gx_node_id));
	rowid->reloid = pq_getmsgint(buf, sizeof(rowid->reloid));
	rowid->tuple_id = (RowId) pq_getmsgint64(buf);

	PG_RETURN_POINTER(rowid);
}

Datum
rowid_send(PG_FUNCTION_ARGS)
{
	StringInfoData	buf;
	OpenTenBaseOraRowID		*rowid = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);

	pq_begintypsend(&buf);
	pq_sendint32(&buf, rowid->gx_node_id);
	pq_sendint32(&buf, rowid->reloid);
	pq_sendint64(&buf, rowid->tuple_id);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
rowid_eq(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID	*r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(rowid_compare(l,r) == 0);
}

Datum
rowid_ne(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID *r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(rowid_compare(l,r) != 0);
}

Datum
rowid_lt(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID	*r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(rowid_compare(l,r) < 0);
}

Datum
rowid_le(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID	*r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(rowid_compare(l,r) <= 0);
}

Datum
rowid_gt(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID *r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(rowid_compare(l,r) > 0);
}

Datum
rowid_ge(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID *r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(rowid_compare(l,r) >= 0);
}

Datum
rowid_larger(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID *r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);
	OpenTenBaseOraRowID *result = palloc(sizeof(OpenTenBaseOraRowID));

	memcpy(result, rowid_compare(l, r) > 0 ? l:r, sizeof(OpenTenBaseOraRowID));
	PG_RETURN_POINTER(result);
}

Datum
rowid_smaller(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*l = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID	*r = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);
	OpenTenBaseOraRowID	*result = palloc(sizeof(OpenTenBaseOraRowID));

	memcpy(result, rowid_compare(l, r) < 0 ? l : r, sizeof(OpenTenBaseOraRowID));
	PG_RETURN_POINTER(result);
}

Datum
btrowidcmp(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *arg1 = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(0);
	OpenTenBaseOraRowID *arg2 = (OpenTenBaseOraRowID *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(rowid_compare(arg1, arg2));
}

Datum
rowid_make(uint32 xc_node_id, Oid reloid, RowId id)
{
	OpenTenBaseOraRowID *rowid;

	rowid = palloc(sizeof(*rowid));
	rowid->gx_node_id = (uint32) xc_node_id;
	rowid->tuple_id = id;
	rowid->reloid = reloid;

	return PointerGetDatum(rowid);
}

Datum
rowid_hash(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *rowid = (OpenTenBaseOraRowID*)PG_GETARG_POINTER(0);
	AssertArg(rowid);

	return DatumGetUInt32(hash_uint32((uint32) rowid->gx_node_id) ^
		hash_uint32((uint32) rowid->reloid) ^
		hash_uint32((uint32) rowid->tuple_id) ^
		hash_uint32((uint32) (rowid->tuple_id >> 32)));
}

Datum
rowid_hash_extended(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *rowid = (OpenTenBaseOraRowID*)PG_GETARG_POINTER(0);
	uint64 seed = DatumGetInt64(1);
	AssertArg(rowid);

	return DatumGetUInt32(hash_uint32_extended((uint32) rowid->gx_node_id, seed) ^
		hash_uint32_extended((uint32) rowid->tuple_id, seed) ^
		hash_uint32_extended((uint32) (rowid->tuple_id >> 32), seed));
}

Datum
rowid_hash_new(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID *rowid = (OpenTenBaseOraRowID*)PG_GETARG_POINTER(0);
	AssertArg(rowid);

	return DatumGetUInt32(hash_uint32_new((uint32) rowid->gx_node_id) ^
		hash_uint32_new((uint32) rowid->reloid) ^
		hash_uint64_new(rowid->tuple_id));
}

static int
rowid_compare(const OpenTenBaseOraRowID *l, const OpenTenBaseOraRowID *r)
{
	AssertArg(l && r);

	if(l->gx_node_id < r->gx_node_id)
		return -1;
	else if(l->gx_node_id > r->gx_node_id)
		return 1;
	else if (l->reloid < r->reloid)
		return -1;
	else if (l->reloid > r->reloid)
		return 1;
	else if(l->tuple_id < r->tuple_id)
		return -1;
	else if(l->tuple_id > r->tuple_id)
		return 1;
	return 0;
}

Datum
rowid_to_raw_text(PG_FUNCTION_ARGS)
{
	OpenTenBaseOraRowID	*rowid = (OpenTenBaseOraRowID*) PG_GETARG_POINTER(0);
	StringInfoData	strinfo;

	initStringInfo(&strinfo);
	appendStringInfo(&strinfo, "(%u, %u, %lu)", rowid->gx_node_id,
						rowid->reloid,
						rowid->tuple_id);

	return PointerGetDatum(cstring_to_text(strinfo.data));
}

Datum
text_to_rowid(PG_FUNCTION_ARGS)
{
	char	*str = text_to_cstring(PG_GETARG_TEXT_P(0));
	return DirectFunctionCall1(rowid_in, CStringGetDatum(str));
}

Datum
rowid_to_text(PG_FUNCTION_ARGS)
{
	char	*str = DatumGetCString(DirectFunctionCall1(rowid_out,
									PG_GETARG_DATUM(0)));

	PG_RETURN_TEXT_P(cstring_to_text(str));
}

RowId
OpenTenBaseOraRowIdGetIdent(Datum d)
{
	OpenTenBaseOraRowID	*orid = (OpenTenBaseOraRowID *) DatumGetPointer(d);

	return orid->tuple_id;
}
