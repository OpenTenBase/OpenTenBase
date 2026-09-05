#ifndef IVFGLOBALCACHE_H
#define IVFGLOBALCACHE_H

#include "postgres.h"
#include "fmgr.h"
#include "storage/itemptr.h"

#define GLOBAL_SUBTREE_CACHE_NAME "/opentenbase_vector_subtree_cache"
#define GLOBAL_SUBTREE_CACHE_MAGIC 0x56454353 /* 'VECS' */
#define GLOBAL_SUBTREE_CACHE_VERSION 1
#define GLOBAL_SUBTREE_CACHE_MAX_SLOTS 4096
#define GLOBAL_SUBTREE_MAX_ITEMS 128

typedef struct GlobalSubtreeSlot
{
	uint64		vectorHash;
	Oid			indexRelOid;
	int32		probes;
	int32		numItems;
	uint64		accessCount;
	uint64		lastAccessSeq;
	ItemPointerData tids[GLOBAL_SUBTREE_MAX_ITEMS];
	double		distances[GLOBAL_SUBTREE_MAX_ITEMS];
	uint32		isValid;
} GlobalSubtreeSlot;

typedef struct GlobalSubtreeCacheHeader
{
	pthread_rwlock_t rwlock;
	uint32		magic;
	uint32		version;
	uint32		maxSlots;
	uint32		activeSlots;
	uint64		clockSeq;
	uint64		totalLookups;
	uint64		totalHits;
	uint64		totalInserts;
	uint64		totalEvictions;
	int32		hashTable[GLOBAL_SUBTREE_CACHE_MAX_SLOTS];
	int32		nextSlot[GLOBAL_SUBTREE_CACHE_MAX_SLOTS];
	GlobalSubtreeSlot slots[GLOBAL_SUBTREE_CACHE_MAX_SLOTS];
} GlobalSubtreeCacheHeader;

/* Public API */
bool		IvfflatGlobalCacheInit(void);
bool		IvfflatGlobalCacheLookup(Oid indexRelOid, uint64 vectorHash, int probes,
									 ItemPointerData *tids, double *distances, int *numItems);
void		IvfflatGlobalCacheInsert(Oid indexRelOid, uint64 vectorHash, int probes,
									 const ItemPointerData *tids, const double *distances, int numItems);
void		IvfflatGlobalCacheStats(uint32 *maxSlots, uint32 *activeSlots,
									uint64 *totalLookups, uint64 *totalHits,
									uint64 *totalInserts, uint64 *totalEvictions);
void		IvfflatGlobalCacheClear(void);

/* SQL callable functions */
Datum		ivfflat_global_cache_stats(PG_FUNCTION_ARGS);
Datum		ivfflat_global_cache_clear(PG_FUNCTION_ARGS);

#endif							/* IVFGLOBALCACHE_H */
