/*-------------------------------------------------------------------------
 *
 * resultCache.h
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/ResultCache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESULTCACHE_H
#define RESULTCACHE_H


#include "lib/ilist.h"
#include "nodes/execnodes.h"

#define RESULTCACHE_MAXKEYS		4

extern bool enable_scalar_subquery_cache;
extern int max_result_cache_memory;

/* function computing a datum's hash */
typedef uint32 (*RCHashFN) (Datum datum);

/* function computing equality of two datums */
typedef bool (*RCFastEqualFN) (Datum a, Datum b);

typedef struct ResultCacheValue
{
	bool        valuenull;
    Datum       value;
} ResultCacheValue;

typedef struct LRUNode
{
	Size        buckte_num;		/* bucket index */
    dlist_node	lru_elem;		/* list member of lru list */
} LRUNode;

typedef struct ResultCacheTup
{
	uint32		hash_value;		/* hash value for keys */

	Datum               keys[RESULTCACHE_MAXKEYS];		/* keys stored in specified memory context */
	bool				keynull[RESULTCACHE_MAXKEYS];
    ResultCacheValue    value;
    LRUNode             *lru_node;

	/*
	 * Each entry in a cache is a member of a dlist that stores the elements
	 * of its hash bucket.  We keep each dlist in LRU order to speed repeated
	 * lookups.
	 */
	dlist_node          cache_elem;		/* list member of per-bucket list */
} ResultCacheTup;

typedef struct ResultCache
{
	Oid			rc_id;			/* cache identifier: function id or (subquery id?) */
	char		*rc_obj_name;	/* function or sublink name */
	int			rc_nbuckets;	/* # of hash buckets in this cache */
	dlist_head *rc_bucket;		/* hash buckets */
    dlist_head *rc_lru;            /* lru list */
	RCHashFN	rc_hashfunc[RESULTCACHE_MAXKEYS];	/* hash function for each key */
	RCFastEqualFN rc_fastequal[RESULTCACHE_MAXKEYS];	/* fast equal function for
													 	* each key */
	short		rc_nkeys;		/* # of keys (1..CATCACHE_MAXKEYS) */
	Oid			*rc_key_types;
	Oid			rc_rettype;
    int         rc_nentrys;
    Size        rc_memory;		/* total memory the rc_id used */
	long		rc_hits;		/* the count of hits */
	long		rc_total_search;		/* the count of search */
	long		rc_lru_count;	/* lru count */
} ResultCache;

typedef struct ResultCacheEntry
{
	Oid         id;			/* cache identifier: function id or (subquery id?) */
	ResultCache *rc;
} ResultCacheEntry;



extern HTAB *InitResultCacheHashTable(MemoryContext context);
extern void RecordScalarSubInResultCache(Oid id, char *name, short nargs, 
											Oid *argtypes, Oid rettype, 
											EState *estate);
extern ResultCache *GetScalarSubResultCache(Oid id, EState *estate);
extern ResultCacheValue *SearchResultCacheInternal(ResultCache *cache, 
													int nkeys, Datum *args, 
													bool *argnull);
extern ResultCacheTup *ResultCacheCreateEntry(MemoryContext context, 
												uint64 *total_rc_memory, 
												ResultCache *result_cache, 
												Datum *arguments, 
												bool *argnull, Datum value, 
												bool valuenull);
extern bool SubLinkResultCachable(SubLinkType type, List *args, 
									short *nargs, Oid **argtypes);
extern void ResultCacheInstrOut(StringInfo buf, HTAB *rc_hash_table, bool *explained);
extern void PrintResultCache(StringInfo buf, int indent_count, 
								HTAB *rc_hash_table, bool *explained);

extern void CheckFuncResultCachable(char procvolatile, bool proretset, 
									int16 pronargs, oidvector *proargtypes);

#endif							/* RESULTCACHE_H */