/*
 * contrib/age/age.c
 *
 * Distributed GraphID primitives for OpenTenBase AGE adaptation.
 * Encodes a 64-bit graphid as: [16-bit label_id | 48-bit entry_id],
 * matching Apache AGE's graphid binary layout.
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

#define ENTRY_ID_BITS      48
#define ENTRY_ID_MASK      INT64CONST(0x0000FFFFFFFFFFFF)
#define LABEL_ID_MIN       1
#define LABEL_ID_MAX       65535

PG_FUNCTION_INFO_V1(age_make_graphid);
PG_FUNCTION_INFO_V1(age_get_label_id);
PG_FUNCTION_INFO_V1(age_get_entry_id);

Datum
age_make_graphid(PG_FUNCTION_ARGS)
{
    int32 label_id = PG_GETARG_INT32(0);
    int64 entry_id = PG_GETARG_INT64(1);
    int64 gid;

    if (label_id < LABEL_ID_MIN || label_id > LABEL_ID_MAX)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("label_id %d out of range [%d, %d]",
                        label_id, LABEL_ID_MIN, LABEL_ID_MAX)));

    if (entry_id < 0 || entry_id > ENTRY_ID_MASK)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("entry_id " INT64_FORMAT " out of 48-bit range", entry_id)));

    gid = (((int64) label_id) << ENTRY_ID_BITS) | (entry_id & ENTRY_ID_MASK);
    PG_RETURN_INT64(gid);
}

Datum
age_get_label_id(PG_FUNCTION_ARGS)
{
    int64 gid = PG_GETARG_INT64(0);
    int32 label_id = (int32) (((uint64) gid) >> ENTRY_ID_BITS);

    PG_RETURN_INT32(label_id);
}

Datum
age_get_entry_id(PG_FUNCTION_ARGS)
{
    int64 gid = PG_GETARG_INT64(0);
    int64 entry_id = gid & ENTRY_ID_MASK;

    PG_RETURN_INT64(entry_id);
}
