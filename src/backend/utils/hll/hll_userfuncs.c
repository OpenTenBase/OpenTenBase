#include "postgres.h"

#include <stdint.h>
#include <math.h>
#include <string.h>

#include "access/hash.h"
#include "access/tupmacs.h"
#include "catalog/pg_type.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/numeric.h"

#ifdef __HLL__

typedef struct HllContext 
{
    Oid     element_type;    
    int16   typlen;           
    bool    typbyval;
    char    typalign;
    hyperLogLogState    state;
} HllContext;  

/*
 * init_hll_context - initialize an empty HllContext
 *
 *    element_type is the input element type
 *    rcontext is where to keep working state
 */
static HllContext *
init_hll_context(Oid element_type, MemoryContext rcontext)
{
    HllContext  *context = NULL;
    MemoryContext   oldcontext;

    context = (HllContext *) MemoryContextAlloc(rcontext, sizeof(HllContext));

    context->element_type = element_type;
    get_typlenbyvalalign(element_type,
                        &context->typlen,
                        &context->typbyval,
                        &context->typalign);

    oldcontext = MemoryContextSwitchTo(rcontext);

    initHyperLogLog(&context->state, 14);

    MemoryContextSwitchTo(oldcontext);

    return context;
}

/*
 * copy_data - copy data from source to destination
 *
 *    src represents the data to be copied
 *    dest is the destination the data will be copied to
 *    typlen, typbyval, typalign: storage parameters of element datatype.
 */
static void 
copy_data(Datum src, char *dest, int typlen, bool typbyval, char typalign)
{
    int inc;

    if (typlen > 0)
    {
        if (typbyval)
            store_att_byval(dest, src, typlen);
        else
            memmove(dest, DatumGetPointer(src), typlen);
    }
    else
    {
        Assert(!typbyval);
        inc = att_addlength_datum(0, typlen, src);
        memmove(dest, DatumGetPointer(src), inc);
        inc = att_align_nominal(inc, typalign);
    }
}

/*
 * accue_hll_result - accumulate one Datum for a hyperloglog context
 *
 *    context is the hyperloglog working state
 *    elem represent the new Datum to add to the hyperloglog context
 */
static void 
accue_hll_result(HllContext *context, Datum elem)
{
    char    *buf = NULL;
    int len = 0;
    uint32  hash;

    len = att_addlength_datum(0, context->typlen, elem);
    buf = palloc(len);
    memset(buf, 0, len);

    /* copy data */
    copy_data(elem, buf, context->typlen, context->typbyval, context->typalign);

    /* hash value */
    hash = DatumGetUInt32(hash_any((unsigned char *) buf, Min(len, PG_CACHE_LINE_SIZE)));

    /* add data to hll context */
    addHyperLogLog(&context->state, hash);

    pfree(buf);
}

/*
 * approx_count_distinct() aggregate function
 */
Datum
hll_transfn(PG_FUNCTION_ARGS)
{
    Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    HllContext  *ctx = NULL;
    MemoryContext   aggcontext;
    Datum   elem;

    if (unlikely(arg1_typeid == InvalidOid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not determine input data type")));

    if (unlikely(!AggCheckCallContext(fcinfo, &aggcontext)))
    {
        /* cannot be called directly because of internal-type argument */
        elog(ERROR, "hyperloglog called in non-aggregate context");
    }

    if (unlikely(PG_ARGISNULL(0)))
        ctx = init_hll_context(arg1_typeid, aggcontext);
    else
        ctx = (HllContext *) PG_GETARG_POINTER(0);

    if (unlikely(PG_ARGISNULL(1)))
        PG_RETURN_POINTER(ctx);

    elem = PG_ARGISNULL(1) ? (Datum) 0 : PG_GETARG_DATUM(1);
    accue_hll_result(ctx, elem);

    PG_RETURN_POINTER(ctx);
}

Datum
hll_finalfn(PG_FUNCTION_ARGS)
{
    HllContext  *ctx;
    MemoryContext   aggcontext;
    Datum num_datum;
    double  result; 

    if (!AggCheckCallContext(fcinfo,  &aggcontext))
        elog(ERROR, "hyperloglog function called in non-aggregate context");
    ctx = PG_ARGISNULL(0) ? NULL : (HllContext *) PG_GETARG_POINTER(0);

    if (ctx == NULL)
        PG_RETURN_NUMERIC(DirectFunctionCall1(int4_numeric, Int32GetDatum(0)));

    result = estimateHyperLogLog(&ctx->state);

    num_datum = DirectFunctionCall1(float8_numeric, Float8GetDatumFast(result));
    PG_RETURN_NUMERIC(DirectFunctionCall1(numeric_floor, NumericGetDatum(num_datum)));
}

/*
 * Combine function for hyperloglog aggregates which parallel aggregate
 */
Datum
hll_combinefn(PG_FUNCTION_ARGS)
{
    HllContext  *ctx1 = PG_ARGISNULL(0) ? NULL : (HllContext *) PG_GETARG_POINTER(0);
    HllContext  *ctx2 = PG_ARGISNULL(1) ? NULL : (HllContext *) PG_GETARG_POINTER(1);
    HllContext  *result = NULL;
    hyperLogLogState    *state1 = NULL;
    hyperLogLogState    *state2 = NULL;
    hyperLogLogState    *rstate = NULL;
    MemoryContext   aggcontext;
    int i;

    if (unlikely(!AggCheckCallContext(fcinfo, &aggcontext)))
    {
        /* cannot be called directly because of internal-type argument */
        elog(ERROR, "hyperloglog called in non-aggregate context");
    }

    /* all the contexts are null */
    if (unlikely(ctx1 == NULL && ctx2 == NULL))
        PG_RETURN_POINTER(NULL);

    if (unlikely(ctx1 == NULL)) 
    {
        result = init_hll_context(ctx2->element_type, aggcontext);
        rstate = &result->state;
        state2 = &ctx2->state;
        /* copy ctx2 to result */
        for (i = 0; i < rstate->nRegisters; i++)
        {
            rstate->hashesArr[i] = state2->hashesArr[i];
        }

        PG_RETURN_POINTER(result);
    }

    if (unlikely(ctx2 == NULL))
    {
        result = init_hll_context(ctx1->element_type, aggcontext);
        rstate = &result->state;
        state1 = &ctx1->state;
        /* copy ctx1 to result */
        for (i = 0; i < rstate->nRegisters; i++)
        {
            rstate->hashesArr[i] = state1->hashesArr[i];
        }

        PG_RETURN_POINTER(result);
    }

    result = init_hll_context(ctx1->element_type, aggcontext);
    rstate = &result->state;
    state1 = &ctx1->state;
    state2 = &ctx2->state;

    /* combine two contexts */
    for (i = 0; i < rstate->nRegisters; i++) 
        rstate->hashesArr[i] = Max(state1->hashesArr[i], state2->hashesArr[i]);

    PG_RETURN_POINTER(result);
}

/*
 * hll_serialize
 *        Serialize HllContext into bytea for approx_count_distinct
 */
Datum
hll_serialize(PG_FUNCTION_ARGS)
{
    HllContext  *ctx;
    StringInfoData  buf;
    bytea   *result;
    hyperLogLogState    *state;
    int i;

    ctx = (HllContext *) PG_GETARG_POINTER(0);

    pq_begintypsend(&buf);

    if (ctx != NULL)
    {
        state = &ctx->state;

        pq_sendint(&buf, ctx->element_type, 4);

        for (i = 0; i < state->nRegisters; i++)
        {
            pq_sendint(&buf,  state->hashesArr[i], 1);
        }
    }

    result = pq_endtypsend(&buf);

    PG_RETURN_BYTEA_P(result);
}

/*
 * hll_deserialize
 *        Deserialize bytea back into HllContext.
 */
Datum
hll_deserialize(PG_FUNCTION_ARGS)
{
    bytea   *sstate;
    HllContext  *ctx;
    StringInfoData  buf;
    Oid element_type;
    hyperLogLogState    *state;
    int i;

    if (!AggCheckCallContext(fcinfo, NULL))
        elog(ERROR, "aggregate function called in non-aggregate context");

    sstate = PG_GETARG_BYTEA_PP(0);

    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

    element_type = pq_getmsgint(&buf, 4);
    ctx = init_hll_context(element_type, CurrentMemoryContext);
    state = &ctx->state;

    for (i = 0; i < state->nRegisters; i++) 
    {
        state->hashesArr[i] = pq_getmsgint(&buf, 1);
    }

    pq_getmsgend(&buf);

    PG_RETURN_POINTER(ctx);
}

#endif