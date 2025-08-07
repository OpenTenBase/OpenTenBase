
/*-------------------------------------------------------------------------
 *
 * catcache.c
 *	  Result cache for stable query matching a key.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/reultCache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "executor/resultCache.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "nodes/nodeFuncs.h"
#include "access/hash.h"
#include "utils/hashutils.h"

int max_result_cache_memory = 16 * 1024 *1024;
bool enable_scalar_subquery_cache = false;

/*
 * Given a hash value and the size of the hash table, find the bucket
 * in which the hash value belongs. Since the hash table must contain
 * a power-of-2 number of elements, this is a simple bitmask.
 */
#define HASH_INDEX(h, sz) ((Index) ((h) & ((sz) - 1)))

static void ResultCacheCopyKeysValue(ResultCache *result_cache, bool *argnull, 
										Datum *srckeys, 
										ResultCacheValue *srcvalue, 
										ResultCacheTup *rct);
static Size ResultCacheComputeKeysValueSize(ResultCache *result_cache, 
											Datum *arguments, bool *argnull, 
											Datum value, bool valuenull);
static Size ResultCacheEstimateKeysValueSize(short nargs, Oid *argtypes, 
												Oid rettype);
static Size ResultCacheEstimateBuckets(short nargs, Oid *argtypes, 
										Oid rettype);
static uint32 ResultCacheComputeHashValue(ResultCache *cache, int nkeys, 
											Datum *args, bool *argnull);
static inline bool ResultCacheCompareTuple(const ResultCache *cache, int nkeys,
											const Datum *cachekeys, const bool *cachekeysnull,
											const Datum *searchkeys, const bool *searchkeysnull);
static ResultCache *ResultCacheInitCache(MemoryContext context, Oid id, 
											char *name, short nargs, 
											Oid *argtypes, Oid rettype);
static bool CheckKeyHashable(Oid keytype);
static void FreeCachedKeysValue(ResultCacheTup *rct, ResultCache *result_cache);

/*
 * Hash and equality functions for system types that are used as cache key
 * fields.  In some cases, we just call the regular SQL-callable functions for
 * the appropriate data type, but that tends to be a little slow, and the
 * speed of these functions is performance-critical.  Therefore, for data
 * types that frequently occur as catcache keys, we hard-code the logic here.
 * Avoiding the overhead of DirectFunctionCallN(...) is a substantial win, and
 * in certain cases (like int4) we can adopt a faster hash algorithm as well.
 */

static bool
chareqfast(Datum a, Datum b)
{
	return DatumGetChar(a) == DatumGetChar(b);
}

static uint32
charhashfast(Datum datum)
{
	return murmurhash32((int32) DatumGetChar(datum));
}

static bool
bpchareqfast(Datum a, Datum b)
{
	return DatumGetBool(DirectFunctionCall2(bpchareq, a, b));
}

static uint32
bpcharhashfast(Datum datum)
{
	return DatumGetInt32(DirectFunctionCall1(hashbpchar, datum));
}

static bool
nameeqfast(Datum a, Datum b)
{
	char	   *ca = NameStr(*DatumGetName(a));
	char	   *cb = NameStr(*DatumGetName(b));

	return strncmp(ca, cb, NAMEDATALEN) == 0;
}

static uint32
namehashfast(Datum datum)
{
	char	   *key = NameStr(*DatumGetName(datum));

	return hash_any((unsigned char *) key, strlen(key));
}

static bool
int2eqfast(Datum a, Datum b)
{
	return DatumGetInt16(a) == DatumGetInt16(b);
}

static uint32
int2hashfast(Datum datum)
{
	return murmurhash32((int32) DatumGetInt16(datum));
}

static bool
int4eqfast(Datum a, Datum b)
{
	return DatumGetInt32(a) == DatumGetInt32(b);
}

static uint32
int4hashfast(Datum datum)
{
	return murmurhash32((int32) DatumGetInt32(datum));
}

static bool
int8eqfast(Datum a, Datum b)
{
	return DatumGetInt64(a) == DatumGetInt64(b);
}

static uint32
int8hashfast(Datum datum)
{
	return (uint32) murmurhash64((int64) DatumGetInt64(datum));
}

static bool
float4eqfast(Datum a, Datum b)
{
	return DatumGetBool(DirectFunctionCall2(float4eq, a, b));
}

static uint32
float4hashfast(Datum datum)
{
	return DatumGetInt32(DirectFunctionCall1(hashfloat4, datum));
}

static bool
float8eqfast(Datum a, Datum b)
{
	return DatumGetBool(DirectFunctionCall2(float8eq, a, b));
}

static uint32
float8hashfast(Datum datum)
{
	return DatumGetInt32(DirectFunctionCall1(hashfloat8, datum));
}

static bool
numbericeqfast(Datum a, Datum b)
{
	return DatumGetBool(DirectFunctionCall2(numeric_eq, a, b));
}

static uint32
numberichashfast(Datum datum)
{
	return DatumGetInt32(DirectFunctionCall1(hash_numeric, datum));
}

static bool
texteqfast(Datum a, Datum b)
{
	return DatumGetBool(DirectFunctionCall2(texteq, a, b));
}

static uint32
texthashfast(Datum datum)
{
	return DatumGetInt32(DirectFunctionCall1(hashtext, datum));
}

static bool
oidvectoreqfast(Datum a, Datum b)
{
	return DatumGetBool(DirectFunctionCall2(oidvectoreq, a, b));
}

static uint32
oidvectorhashfast(Datum datum)
{
	return DatumGetInt32(DirectFunctionCall1(hashoidvector, datum));
}

/* Lookup support functions for a type. */
static bool
GetCCHashEqFuncs(Oid keytype, RCHashFN *hashfunc, RCFastEqualFN *fasteqfunc)
{
	switch (keytype)
	{
		case BOOLOID:
			*hashfunc = charhashfast;
			*fasteqfunc = chareqfast;
			break;
		case CHAROID:
			*hashfunc = charhashfast;
			*fasteqfunc = chareqfast;
			break;
		case BPCHAROID:
			*hashfunc = bpcharhashfast;
			*fasteqfunc = bpchareqfast;
			break;
		case NAMEOID:
			*hashfunc = namehashfast;
			*fasteqfunc = nameeqfast;
			break;
		case INT2OID:
			*hashfunc = int2hashfast;
			*fasteqfunc = int2eqfast;
			break;
		case INT4OID:
		case DATEOID:
			*hashfunc = int4hashfast;
			*fasteqfunc = int4eqfast;
			break;
		case INT8OID:
			*hashfunc = int8hashfast;
			*fasteqfunc = int8eqfast;
			break;
		case FLOAT4OID:
			*hashfunc = float4hashfast;
			*fasteqfunc = float4eqfast;
			break;
		case FLOAT8OID:
			*hashfunc = float8hashfast;
			*fasteqfunc = float8eqfast;
			break;
		case TEXTOID:
		case VARCHAROID:
#ifdef _PG_ORCL_
		case  VARCHAR2OID:
		case  NVARCHAR2OID:
#endif
			*hashfunc = texthashfast;
			*fasteqfunc = texteqfast;
			break;
		case NUMERICOID:
			*hashfunc = numberichashfast;
			*fasteqfunc = numbericeqfast;
			break;
		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case REGCONFIGOID:
		case REGDICTIONARYOID:
		case REGROLEOID:
		case REGNAMESPACEOID:
			*hashfunc = int4hashfast;
			*fasteqfunc = int4eqfast;
			break;
		case OIDVECTOROID:
			*hashfunc = oidvectorhashfast;
			*fasteqfunc = oidvectoreqfast;
			break;
		default:
			elog(DEBUG2, "type %u not supported as catcache key", keytype);
			*hashfunc = NULL;	/* keep compiler quiet */
			return false;
	}
	return true;
}

/*
 * Copy keys and value to current memory context.
 */
static void
ResultCacheCopyKeysValue(ResultCache *result_cache, bool *argnull, 
							Datum *srckeys,
							ResultCacheValue *srcvalue,
							ResultCacheTup *rct)
{
	int     i = 0;
	bool    rettypByVal;
    int16	rettypLen;
    short	nkey = result_cache->rc_nkeys;
	Oid     *keytypes = result_cache->rc_key_types;
	Oid     rettype = result_cache->rc_rettype;

	for (i = 0; i < nkey; i++)
	{
		bool    typByVal;
		int16     typLen;
		Datum		src = srckeys[i];
		NameData	srcname;

		rct->keynull[i] = argnull[i];
		if (argnull[i])
		{
			rct->keys[i] = 0;
			continue;
		}

		get_typlenbyval(keytypes[i], &typLen, &typByVal);
		/*
		 * Must be careful in case the caller passed a C string where a
		 * NAME is wanted: convert the given argument to a correctly
		 * padded NAME.  Otherwise the memcpy() done by datumCopy() could
		 * fall off the end of memory.
		 */
		if (keytypes[i] == NAMEOID)
		{
			namestrcpy(&srcname, DatumGetCString(src));
			src = NameGetDatum(&srcname);
		}

		rct->keys[i] = datumCopy(src,
								typByVal,
								typLen);
	}

	rct->value.valuenull = srcvalue->valuenull;
	if (rct->value.valuenull)
	{
		rct->value.value = 0;
		return;
	}

    get_typlenbyval(rettype, &rettypLen, &rettypByVal);
    rct->value.value = datumCopy(srcvalue->value,
                            rettypByVal,
                            rettypLen);
    
}

/*
 * Compute the size of keys and value according types.
 */
static Size
ResultCacheComputeKeysValueSize(ResultCache *result_cache, Datum *arguments, bool *argnull, Datum value, bool valuenull)
{
    int     i = 0;
    short	nkey = 0;
	Oid     *keytypes = NULL;
	Oid     rettype;
    bool    rettypByVal;
    int16	rettypLen;
    Size    sz = 0;

	rettype = result_cache->rc_rettype;
	nkey = result_cache->rc_nkeys;
	keytypes = result_cache->rc_key_types;
	for (i = 0; i < nkey; i++)
    {
        bool    typByVal;
		int16	typLen;

		get_typlenbyval(keytypes[i], &typLen, &typByVal);
        sz += datumEstimateSpace(arguments[i], argnull[i], typByVal, typLen);
    }
    get_typlenbyval(rettype, &rettypLen, &rettypByVal);
    sz += datumEstimateSpace(value, valuenull, rettypByVal, rettypLen);
    return sz;
}

/*
 * Estimate the size of keys and value. 
 * For variable length, we assume that its lens is 64,
 * so we can estimate the total size of keys and values in a record,
 * then we can estimate the number of buckets.
 */
static Size
ResultCacheEstimateKeysValueSize(short nargs, Oid *argtypes, Oid rettype)
{
#define ESTIMATE_VARLENA_LEN 64
    int     i = 0;
    bool    rettypByVal;
    int16	rettypLen;
    Size    sz = 0;

	for (i = 0; i < nargs; i++)
    {
        bool    typByVal;
		int16	typLen;

		get_typlenbyval(argtypes[i], &typLen, &typByVal);
        if (typLen > 0)
        {
            sz += typLen;
        }
        else
        {
            sz += ESTIMATE_VARLENA_LEN;
        }
    }
    get_typlenbyval(rettype, &rettypLen, &rettypByVal);
    if (rettypLen > 0)
    {
        sz += rettypLen;
    }
    else
    {
        sz += ESTIMATE_VARLENA_LEN;
    }
    return sz;
}

/*
 * estimate buckets number
 */
static Size
ResultCacheEstimateBuckets(short nargs, Oid *argtypes, Oid rettype)
{
	Size estimate_item_sz = ResultCacheEstimateKeysValueSize(nargs, argtypes, rettype);

	return max_result_cache_memory/estimate_item_sz;
}

/*
 *		ResultCacheComputeHashValue
 *
 * Compute the hash value associated with a given set of lookup keys
 */
static uint32
ResultCacheComputeHashValue(ResultCache *cache, int nkeys, Datum *args, bool *argnull)
{
	uint32		hashValue = 0;
	uint32		oneHash;
	RCHashFN   *rc_hashfunc = cache->rc_hashfunc;

	switch (nkeys)
	{
		case 4:
			if (!argnull[3])
			{
				oneHash = (rc_hashfunc[3]) (args[3]);

				hashValue ^= oneHash << 24;
				hashValue ^= oneHash >> 8;
			}
			/* FALLTHROUGH */
		case 3:
			if (!argnull[2])
			{
				oneHash = (rc_hashfunc[2]) (args[2]);

				hashValue ^= oneHash << 16;
				hashValue ^= oneHash >> 16;
			}
			/* FALLTHROUGH */
		case 2:
			if (!argnull[1])
			{
				oneHash = (rc_hashfunc[1]) (args[1]);

				hashValue ^= oneHash << 8;
				hashValue ^= oneHash >> 24;
			}
			/* FALLTHROUGH */
		case 1:
			if (!argnull[0])
			{
				oneHash = (rc_hashfunc[0]) (args[0]);

				hashValue ^= oneHash;
			}
			break;
		default:
			break;
	}

	return hashValue;
}

/*
 *		ResultCacheCompareTuple
 *
 * Compare a tuple to the passed arguments.
 */
static inline bool
ResultCacheCompareTuple(const ResultCache *cache, int nkeys,
						 const Datum *cachekeys,
						 const bool *cachekeysnull,
						 const Datum *searchkeys,
						 const bool *searchkeysnull)
{
	const RCFastEqualFN *rc_fastequal = cache->rc_fastequal;
	int			i;

	for (i = 0; i < nkeys; i++)
	{
		if (cachekeysnull[i] != searchkeysnull[i])
			return false;
		if (cachekeysnull[i])
			continue;
		if (!(rc_fastequal[i]) (cachekeys[i], searchkeys[i]))
			return false;
	}
	return true;
}

/*
 * Free the keys and value in ResultCacheTup for lru
 */
static void
FreeCachedKeysValue(ResultCacheTup *rct, ResultCache *result_cache)
{
	int					i = 0;
	Datum				key;
	Datum				value;
	Oid					typeid;
	bool				typbyval;
	ResultCacheValue	rcvalue;

	for (i = 0; i < result_cache->rc_nkeys; i++)
	{
		if (rct->keynull[i])
			continue;

		typeid = result_cache->rc_key_types[i];
		typbyval = get_typbyval(typeid);
		if (typbyval)
			continue;

		key = rct->keys[i];
		pfree(DatumGetPointer(key));
	}

	typeid = result_cache->rc_rettype;
	typbyval = get_typbyval(typeid);
	if (typbyval)
		return;

	rcvalue = rct->value;
	if (rcvalue.valuenull)
		return;

	value = rcvalue.value;
	pfree(DatumGetPointer(value));
}

/*
 * Create result_cache entry in context
 */
ResultCacheTup *
ResultCacheCreateEntry(MemoryContext context, uint64 *total_rc_memory, 
						ResultCache *result_cache,  
						Datum *arguments, bool *argnull, 
						Datum value, bool valuenull)
{
	MemoryContext oldContext;
    Size        keyvalue_size = 0;
    uint32		hashValue;
	Index		hashIndex;
    LRUNode         *lru_node = NULL;
    ResultCacheTup  *rct = NULL;
    ResultCacheValue tmp_value;

	/* max_result_cache_memory very small, no result cached */
	if (result_cache->rc_nbuckets == 0)
	{
		return NULL;
	}

	keyvalue_size = ResultCacheComputeKeysValueSize(result_cache, arguments, argnull, value, valuenull);
    /* LRU */
    while (!dlist_is_empty(result_cache->rc_lru) &&
			*total_rc_memory + (keyvalue_size + sizeof(ResultCacheTup)) > max_result_cache_memory)
    {
		dlist_node      *lru_tail = dlist_tail_node(result_cache->rc_lru);
		LRUNode         *tail_lru = dlist_container(LRUNode, lru_elem, lru_tail);
		dlist_node      *tail = dlist_tail_node(&result_cache->rc_bucket[tail_lru->buckte_num]);
		ResultCacheTup  *tail_rct = dlist_container(ResultCacheTup, cache_elem, tail);
		Size            sz = ResultCacheComputeKeysValueSize(result_cache, tail_rct->keys, tail_rct->keynull, tail_rct->value.value, tail_rct->value.valuenull);

        dlist_delete(lru_tail) ;
        pfree(tail_lru);
        dlist_delete(tail);
        FreeCachedKeysValue(tail_rct, result_cache);
        pfree(tail_rct);
		result_cache->rc_lru_count++;
		result_cache->rc_memory -= (sz + sizeof(ResultCacheTup));
        *total_rc_memory -= (sz + sizeof(ResultCacheTup));
    }

	if (*total_rc_memory + (keyvalue_size + sizeof(ResultCacheTup)) > max_result_cache_memory)
	{
		return NULL;
	}

    hashValue = ResultCacheComputeHashValue(result_cache, result_cache->rc_nkeys, arguments, argnull);
	hashIndex = HASH_INDEX(hashValue, result_cache->rc_nbuckets);

	oldContext = MemoryContextSwitchTo(context);
    lru_node = (LRUNode *)palloc(sizeof(LRUNode));
	rct = (ResultCacheTup *) palloc(sizeof(ResultCacheTup));
    lru_node->buckte_num = hashIndex;
	rct->hash_value = hashValue;
    tmp_value.valuenull = valuenull;
    tmp_value.value = value;
	ResultCacheCopyKeysValue(result_cache, argnull, arguments, &tmp_value, rct);
    rct->lru_node = lru_node;

	MemoryContextSwitchTo(oldContext);   

    result_cache->rc_memory += (keyvalue_size + sizeof(ResultCacheTup));
	*total_rc_memory += (keyvalue_size + sizeof(ResultCacheTup));
    dlist_push_head(&result_cache->rc_bucket[hashIndex], &rct->cache_elem);
    dlist_push_head(result_cache->rc_lru, &lru_node->lru_elem);
    result_cache->rc_nentrys++;
	elog(DEBUG2, "[result_cache] %s create result entry", result_cache->rc_obj_name);
	return rct;
}

/*
 * Work-horse for SearchResultCache.
 */
ResultCacheValue *
SearchResultCacheInternal(ResultCache *cache,
					   int nkeys,
                       Datum *args,
					   bool *argnull)
{
	Datum		arguments[RESULTCACHE_MAXKEYS];
	uint32		hashValue;
	Index		hashIndex;
	dlist_iter	iter;
	dlist_head *bucket;
	ResultCacheTup      *ct;
    int                 i = 0;

	cache->rc_total_search++;
	/* max_result_cache_memory very small, no result cached */
	if (cache->rc_nbuckets == 0)
	{
		return NULL;
	}

    Assert(nkeys <= RESULTCACHE_MAXKEYS);
	/* Initialize local parameter array */
    for (i = 0; i < nkeys; i++)
    {
        arguments[i] = args[i];
    }

	/*
	 * find the hash bucket in which to look for the result
	 */
	hashValue = ResultCacheComputeHashValue(cache, nkeys, args, argnull);
	hashIndex = HASH_INDEX(hashValue, cache->rc_nbuckets);

	/*
	 * scan the hash bucket until we find a match or exhaust our tuples
	 *
	 * Note: it's okay to use dlist_foreach here, even though we modify the
	 * dlist within the loop, because we don't continue the loop afterwards.
	 */
	bucket = &cache->rc_bucket[hashIndex];
	dlist_foreach(iter, bucket)
	{
		ct = dlist_container(ResultCacheTup, cache_elem, iter.cur);

		if (ct->hash_value != hashValue)
			continue;			/* quickly skip entry if wrong hash val */

		if (!ResultCacheCompareTuple(cache, nkeys, ct->keys, ct->keynull, arguments, argnull))
			continue;

		/*
		 * We found a match in the cache.  Move it to the front of the list
		 * for its hashbucket, in order to speed subsequent searches.  (The
		 * most frequently accessed elements in any hashbucket will tend to be
		 * near the front of the hashbucket's list.)
		 */
		dlist_move_head(bucket, &ct->cache_elem);
        dlist_move_head(cache->rc_lru, &ct->lru_node->lru_elem);

        cache->rc_hits++;
		elog(DEBUG2, "[result_cache] %s get cached result", cache->rc_obj_name);

        return &ct->value;
	}

    return NULL;
}

static ResultCache *
ResultCacheInitCache(MemoryContext context, Oid id, char *name, short nargs, Oid *argtypes, Oid rettype)
{
	ResultCache *result;
	MemoryContext oldcxt;
	int i = 0;

	oldcxt = MemoryContextSwitchTo(context);
	result = (ResultCache *)palloc0(sizeof(ResultCache));
	result->rc_lru_count = 0;
	result->rc_rettype = rettype;
	result->rc_hits = 0;
	result->rc_total_search = 0;
	result->rc_id = id;
	result->rc_nentrys = 0;
	result->rc_nkeys = nargs;

	if (name)
		result->rc_obj_name = pstrdup(name);
	else
		result->rc_obj_name = NULL;
	if (nargs == 0)
		result->rc_nbuckets = 1;
	else
		result->rc_nbuckets = ResultCacheEstimateBuckets(nargs, argtypes, rettype);
	if (result->rc_nbuckets == 0)
	{
		elog(LOG, "[result_cache] bucket number is 0, cache nothding for %s.", name);
		MemoryContextSwitchTo(oldcxt);
		return result;
	}

	result->rc_bucket = palloc0(result->rc_nbuckets * sizeof(dlist_head));
    result->rc_lru = palloc0(sizeof(dlist_head));
	result->rc_key_types = palloc0(result->rc_nkeys * sizeof(Oid));

	for (i = 0; i < result->rc_nkeys; ++i)
	{
		Oid keytype = argtypes[i];
		GetCCHashEqFuncs(keytype,
							&result->rc_hashfunc[i],
							&result->rc_fastequal[i]);
		result->rc_key_types[i] = keytype;
	}

	MemoryContextSwitchTo(oldcxt);
    return result;
}

HTAB *
InitResultCacheHashTable(MemoryContext context)
{
    HTAB		*result;
    HASHCTL		info;
    memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ResultCacheEntry);
	info.hcxt = context;

	result = hash_create("Result cache Lookup Table",
                            8,
                            &info,
                            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    return result;
}

/*
 * Record cachable function or sublink in hashtable
 */
void
RecordScalarSubInResultCache(Oid id, char *name, short nargs, Oid *argtypes, Oid rettype, EState *estate)
{
	ResultCacheEntry	*entry = NULL;
	MemoryContext		result_cache_context;
	ResultCache			*result_cache = NULL;
	bool				found = false;

	result_cache_context = estate->es_query_cxt;

    if (estate->es_rc_hash_table == NULL)
    {
        estate->es_rc_hash_table = InitResultCacheHashTable(result_cache_context);
		estate->es_rc_total_memory = 0;
    }

    entry = (ResultCacheEntry *) hash_search(estate->es_rc_hash_table,
                                                                &id,
                                                                HASH_ENTER,
                                                                &found);
	if (!found)
	{
		result_cache = ResultCacheInitCache(result_cache_context, id, name, nargs, argtypes, rettype);
		entry->rc = result_cache;
	}					
}

/*
 * Get result from es_rc_hash_table instead of execution.
 */
ResultCache *
GetScalarSubResultCache(Oid id, EState *estate)
{
    ResultCacheEntry *entry = (ResultCacheEntry *) hash_search(estate->es_rc_hash_table,
                                                                &id,
                                                                HASH_FIND,
                                                                NULL);
    if (entry != NULL)
    {
        return entry->rc;
    }
    return NULL;
}

static bool
CheckKeyHashable(Oid keytype)
{
	RCHashFN		hashfucn;
	RCFastEqualFN	fastequal;

	return GetCCHashEqFuncs(keytype,
							&hashfucn,
							&fastequal);
}

/*
 * Check whether udf can be cached or not:
 * 1. not return set.
 * 2. nargs <= RESULTCACHE_MAXKEYS
 * 3. all args can be hashed
 */
void
CheckFuncResultCachable(char procvolatile, bool proretset, int16 pronargs, oidvector *proargtypes)
{
	int i = 0;

	if (procvolatile != PROVOLATILE_CACHABLE)
		return;
	if (proretset)
		elog(ERROR, "Function that returns a set cannot be cached.");
	if (pronargs > RESULTCACHE_MAXKEYS)
		elog(ERROR, "Function with more than %d parameters cannot be cached.", RESULTCACHE_MAXKEYS);
	for (i = 0; i < pronargs; i++)
	{
		Oid keytype = proargtypes->values[i];

		if (!CheckKeyHashable(keytype))
		{
			elog(ERROR, "Function with %s type cannot be cached.", get_typename(keytype));
		}
	}
}

/*
 * Check whether sublink can be cached or not:
 * 1. EXPR_SUBLINK
 * 2. nargs <= RESULTCACHE_MAXKEYS
 * 3. all args can be hashed
 */
bool
SubLinkResultCachable(SubLinkType type, List *args, short *nargs, Oid **argtypes)
{
	ListCell	*lc;
	int			i = 0;

	if (!enable_scalar_subquery_cache ||
		!(max_result_cache_memory > 0) ||
		type != EXPR_SUBLINK)
		return false;

	*nargs = list_length(args);
	if (*nargs > RESULTCACHE_MAXKEYS)
		return false;

	*argtypes = (Oid *) palloc(*nargs * sizeof(Oid));
	foreach(lc, args)
	{
		Oid keytype = exprType(lfirst(lc));

		(*argtypes)[i++] = keytype;
		if (!CheckKeyHashable(keytype))
		{
			pfree(*argtypes);
			return false;
		}
	}

	return true;
}


/*
 * ResultCacheInstrOut
 *
 * Serialize result cache information with the format
 * "1/0<val,val,...,val>", and 1/0 indicates if values are valid or not.
 *
 * NOTE: The function should be modified if the corresponding data structure
 * has been changed.
 */
void
ResultCacheInstrOut(StringInfo buf, HTAB *rc_hash_table, bool *explained)
{
	HASH_SEQ_STATUS seq;
	ResultCacheEntry *entry = NULL;

	if (!rc_hash_table || *explained)
	{
		appendStringInfo(buf, "0<>");
		return;
	}

	appendStringInfo(buf, "1<");
	/* cache nums */
	appendStringInfo(buf, "%ld,", hash_get_num_entries(rc_hash_table));

	hash_seq_init(&seq, rc_hash_table);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		ResultCache *rc = entry->rc;

		if (rc->rc_obj_name)
		{
			double hit_ratio = 0;
			if (rc->rc_total_search != 0)
			{
				hit_ratio = (double)rc->rc_hits/rc->rc_total_search;
			}
			appendStringInfo(buf, "%ld,", strlen(rc->rc_obj_name));
			appendStringInfo(buf, "%s:", rc->rc_obj_name);
			appendStringInfo(buf, "%ld,%ld,%ld,%.2f;", rc->rc_memory, rc->rc_lru_count, rc->rc_total_search, hit_ratio);
		}
	}
	appendStringInfo(buf, ">");
	*explained = true;
}

/*
 * print result cache information when explain analyze in centralized mode 
 */
void
PrintResultCache(StringInfo buf, int indent_count, HTAB *rc_hash_table, bool *explained)
{
	HASH_SEQ_STATUS seq;
	ResultCacheEntry *entry = NULL;

	if (!rc_hash_table || *explained)
	{
		return;
	}

	hash_seq_init(&seq, rc_hash_table);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		ResultCache *rc = entry->rc;

		if (rc->rc_obj_name)
		{
			double hit_ratio = 0;

			if (rc->rc_total_search != 0)
			{
				hit_ratio = (double)rc->rc_hits/rc->rc_total_search;
			}
			appendStringInfoSpaces(buf, indent_count * 2);
			appendStringInfo(buf, "Result_Cache (");
			appendStringInfo(buf, "rc_name: %s, ", rc->rc_obj_name);
			appendStringInfo(buf, "rc_memory: %ld, ", rc->rc_memory);
			appendStringInfo(buf, "rc_lru_count: %ld, ", rc->rc_lru_count);
			appendStringInfo(buf, "rc_total_search: %ld, ", rc->rc_total_search);
			appendStringInfo(buf, "rc_hit_ratio: %.2f", hit_ratio);
			appendStringInfo(buf, ")");
			appendStringInfo(buf, "\n");
		}
	}
	*explained = true;
}
