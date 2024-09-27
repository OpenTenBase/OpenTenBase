#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "libpq/pqformat.h"
#include "access/tupmacs.h"
#include "c.h"
#include "pg_config_manual.h" 
#include "utils/palloc.h"


#ifdef __HLL__

#define HLL_P 14
#define HLL_Q (64 - HLL_P)
#define HLL_REGISTERS (1 << HLL_P) 
#define HLL_P_MASK (HLL_REGISTERS-1)
#define HLL_ALPHA_INF 0.721347520444481703680

typedef struct HllContext {
  Oid element_type;
  uint32_t result;     /* 用于存储计算的结果 */
  int16        typlen;           
  bool        typbyval;
  char        typalign;
  uint8_t registers[HLL_REGISTERS]; /* 寄存器数组 */
} HllContext;  

static uint64 hll_hash(const void * key, int len, unsigned int seed);
static void hll_add_element(uint8_t *registers, char *element, size_t elesize);
static int hll_pattern_len(char *element, size_t size, long *regp);
static void hll_count(HllContext *context);
static void hll_dense_reg_histo(uint8_t *registers, int* reghisto);
static double hll_tau(double x);
static double hll_sigma(double x);
static HllContext *init_hll_context(Oid element_type, MemoryContext rcontext);
static void accue_hll_result(HllContext *context, Datum elem);
static void copy_data(Datum src, char *dest, int typlen, bool typbyval, char typalign);


static uint64 hll_hash(const void * key, int len, unsigned int seed)
{
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (len * m);
  const uint8_t *data = (const uint8_t *) key;
  const uint8_t *end = data + (len - (len & 7));

  while(data != end) {
    uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
    k = *((uint64_t*)data);
#else
    k = (uint64_t) data[0];
    k |= (uint64_t) data[1] << 8;
    k |= (uint64_t) data[2] << 16;
    k |= (uint64_t) data[3] << 24;
    k |= (uint64_t) data[4] << 32;
    k |= (uint64_t) data[5] << 40;
    k |= (uint64_t) data[6] << 48;
    k |= (uint64_t) data[7] << 56;
#endif

    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    data += 8;
  }

  switch(len & 7) {
  case 7: h ^= (uint64_t)data[6] << 48;
  case 6: h ^= (uint64_t)data[5] << 40;
  case 5: h ^= (uint64_t)data[4] << 32; 
  case 4: h ^= (uint64_t)data[3] << 24; 
  case 3: h ^= (uint64_t)data[2] << 16;
  case 2: h ^= (uint64_t)data[1] << 8; 
  case 1: h ^= (uint64_t)data[0];
          h *= m; 
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

static void hll_add_element(uint8_t *registers, char *element, size_t elesize)
{
  long index;
  uint8_t count = hll_pattern_len(element, elesize, &index);
  if (count > registers[index]) 
    registers[index] = count;
}

static int hll_pattern_len(char *element, size_t size, long *regp) 
{
  uint64_t hash, bit, index;
  int count;

  hash = hll_hash(element, size, 0xadc83b19ULL);
  index = hash & HLL_P_MASK;    /* 寄存器索引 */
  hash >>= HLL_P;               /* 移除哈希值的索引位 */
  hash |= ((uint64_t)1 << HLL_Q); /* 确保循环会终结 */
  bit = 1;
  count = 1; 
  while((hash & bit) == 0) {
      count++;
      bit <<= 1;
  }
  *regp = (int) index;
  return count;
}

static void hll_dense_reg_histo(uint8_t *registers, int* reghisto)
{
  uint8_t *r = registers;

  for (int j = 0; j < 1024; j++) {
    reghisto[r[0]]++;
    reghisto[r[1]]++;
    reghisto[r[2]]++;
    reghisto[r[3]]++;
    reghisto[r[4]]++;
    reghisto[r[5]]++;
    reghisto[r[6]]++;
    reghisto[r[7]]++;
    reghisto[r[8]]++;
    reghisto[r[9]]++;
    reghisto[r[10]]++;
    reghisto[r[11]]++;
    reghisto[r[12]]++;
    reghisto[r[13]]++;
    reghisto[r[14]]++;
    reghisto[r[15]]++;
    r += 16;
  }
}

static double hll_tau(double x)
{
  double zPrime;
  double y = 1.0;
  double z = 1 - x;

  if (x == 0. || x == 1.) 
    return 0.;

  do {
    x = sqrt(x);
    zPrime = z;
    y *= 0.5;
    z -= pow(1 - x, 2)*y;
  } while(zPrime != z);
  return z / 3;
}



static double hll_sigma(double x)
{
  double zPrime;
  double y = 1;
  double z = x;

  if (x == 1.) 
    return INFINITY;
    
  do {
    x *= x;
    zPrime = z;
    z += x * y;
    y += y;
  } while(zPrime != z);

  return z;
}

static void hll_count(struct HllContext *context)
{
  double m = HLL_REGISTERS;
  double E;
  int j;

  int reghisto[64] = {0};

  hll_dense_reg_histo(context->registers, reghisto);

  double z = m * hll_tau((m-reghisto[HLL_Q+1])/(double)m);
  for (j = HLL_Q; j >= 1; --j) {
      z += reghisto[j];
      z *= 0.5;
  }
  z += m * hll_sigma(reghisto[0]/(double)m);
  E = llroundl(HLL_ALPHA_INF * m * m / z);

  context->result = (uint64_t) E;
}

static HllContext *init_hll_context(Oid element_type, MemoryContext rcontext)
{
  HllContext *context = NULL;

  context = (HllContext *) MemoryContextAlloc(rcontext, sizeof(HllContext));

  context->result = 0;
  context->element_type = element_type;
  for (int i = 0; i < HLL_REGISTERS; i++)
    context->registers[i] = 0;

  get_typlenbyvalalign(element_type,
                       &context->typlen,
                       &context->typbyval,
                       &context->typalign);

  return context;
}

static void copy_data(Datum src, char *dest, int typlen, bool typbyval, char typalign)
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

static void accue_hll_result(HllContext *context, Datum elem)
{
  char *buf = NULL;
  int len = 0;

  len = att_addlength_datum(0, context->typlen, elem);
  buf = palloc(len);
  memset(buf, 0, len);

  // copy data
  copy_data(elem, buf, context->typlen, context->typbyval, context->typalign);

  // add data to hll context
  hll_add_element(context->registers, buf, len);

  pfree(buf);
}


Datum
hll_transfn(PG_FUNCTION_ARGS)
{
  Oid arg1_typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
  HllContext *ctx = NULL;
  MemoryContext aggcontext;
  Datum elem;

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
  HllContext *ctx;
  MemoryContext aggcontext;

  if (!AggCheckCallContext(fcinfo, &aggcontext))
        elog(ERROR, "hyperloglog function called in non-aggregate context");
  ctx = PG_ARGISNULL(0) ? NULL : (HllContext *) PG_GETARG_POINTER(0);

  if (ctx == NULL)
    PG_RETURN_INT64(0);

  hll_count(ctx);
  PG_RETURN_INT64(ctx->result);
}

Datum
hll_combinefn(PG_FUNCTION_ARGS)
{
  HllContext *ctx1 = PG_ARGISNULL(0) ? NULL : (HllContext *) PG_GETARG_POINTER(0);
  HllContext *ctx2 = PG_ARGISNULL(1) ? NULL : (HllContext *) PG_GETARG_POINTER(1);
  HllContext *result = NULL;
  MemoryContext aggcontext;

  if (unlikely(!AggCheckCallContext(fcinfo, &aggcontext)))
  {
      /* cannot be called directly because of internal-type argument */
      elog(ERROR, "hyperloglog called in non-aggregate context");
  }

  if (unlikely(PG_ARGISNULL(0) && PG_ARGISNULL(1)))
    PG_RETURN_POINTER(NULL);

  if (unlikely(ctx1 == NULL)) 
  {
    result = init_hll_context(ctx2->element_type, aggcontext);
    for (int i = 0; i < HLL_REGISTERS; i++)
    {
      result->registers[i] = ctx2->registers[i];
    }

    PG_RETURN_POINTER(result);
  }

  if (unlikely(ctx2 == NULL))
  {
    result = init_hll_context(ctx1->element_type, aggcontext);
    for (int i = 0; i < HLL_REGISTERS; i++)
    {
      result->registers[i] = ctx1->registers[i];
    }

    PG_RETURN_POINTER(result);
  }
    
  result = init_hll_context(ctx1->element_type, aggcontext);
  for (int i = 0; i < HLL_REGISTERS; i++) 
    result->registers[i] = Max(ctx1->registers[i], ctx2->registers[i]);

  PG_RETURN_POINTER(result);
}

Datum
hll_serialize(PG_FUNCTION_ARGS)
{
  HllContext *ctx;
  StringInfoData buf;
	bytea	   *result;

  ctx = (HllContext *) PG_GETARG_POINTER(0);

  pq_begintypsend(&buf);

  if (ctx != NULL) {
    pq_sendint(&buf, ctx->element_type, 4);

    for (int i = 0; i < HLL_REGISTERS; i++) 
    {
      pq_sendint(&buf, ctx->registers[i], 1);
    }
  }

  result = pq_endtypsend(&buf);
  PG_RETURN_BYTEA_P(result);
}

Datum
hll_deserialize(PG_FUNCTION_ARGS)
{
  bytea       *sstate;
  HllContext *ctx;
  StringInfoData buf;
  Oid element_type;

  if (!AggCheckCallContext(fcinfo, NULL))
    elog(ERROR, "aggregate function called in non-aggregate context");

  sstate = PG_GETARG_BYTEA_PP(0);

  initStringInfo(&buf);
  appendBinaryStringInfo(&buf,
                         VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

  element_type = pq_getmsgint(&buf, 4);
  ctx = init_hll_context(element_type, CurrentMemoryContext);
  
  for (int i = 0; i < HLL_REGISTERS; i++) 
  {
    ctx->registers[i] = pq_getmsgint(&buf, 1);
  }

  pq_getmsgend(&buf);
  PG_RETURN_POINTER(ctx);
}

#endif