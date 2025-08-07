/*------------------------------------------------------------------------
 *
 * spm.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/spm/spm_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "optimizer/spm_cache.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#define DEFAULT_EVICT_PARTITION 10
#define DEFAULT_EVICT_PERCENT 20
#define SPM_PLAN_MAX_USAGE_COUNT	10
#define SPM_HISTORY_MAX_USAGE_COUNT	5
#define	SPM_CACHE_TOTAL_COUNT_RESET_NUM	100

const int spm_cache_invalid_factor = 10;

/* To record the SPM cache in the current transaction for deletion during transaction rollback. */
List *g_spm_cache_plan_pending_list;
List *g_spm_cache_history_pending_list;

static HTAB *g_SPMPlanHashTab;
static HTAB *g_SPMPlanIdHashTab;
static SPMPlanCachedItem *g_SPMPlanItemBuffers;
static SPMCacheNode *g_SPMPlanCacheArray;
static SPMCacheNodeDesc *g_SPMPlanCacheDesc;
static SPMCacheCount *g_SPMCacheCount;

static HTAB *g_SPMHistoryHashTab;
static HTAB *g_SPMHistoryIdHashTab;
static SPMHistoryCachedItem *g_SPMHistoryItemBuffers;
static SPMCacheNode *g_SPMHistoryCacheArray;
static SPMCacheNodeDesc *g_SPMHistoryCacheDesc;

static SPMPlanCachedEntry *SPMPlanCacheSearchInternal(SPMCachedKey *key, 
														uint32 hash_value,
														bool recordcount);
static SPMPlanCachedItem *DeleteCacheValueItem(SPMPlanCachedItem *list, 
												SPMPlanCachedItem *item);
static void InitSPMPlanCachedEntryList(SPMPlanCachedEntry *entry);
static int GetSPMItemBufferId(SPMPlanCachedItem *item);
static void SPMPlanHashTblDeleteInternal(Oid dboid, int64 sql_id, 
										int64 plan_id, bool delentries,
										bool pinnode, bool unpinnode);
static void SPMHistoryHashTblDeleteInternal(Oid dboid, int64 sql_id, 
											int64 plan_id, bool delentries,
											bool pinnode, bool unpinnode);
static SPMHistoryCachedEntry *SPMHistoryCacheSearchInternal(SPMCachedKey *key, 
															uint32 hashcode,
															bool recordcount);
static uint32 ClockSweepTick(SPMCacheNodeDesc *cachedesc, SPMPlanCacheType type);

typedef void (*SPMCacheHashTblDeleteFuncPtr)(Oid, int64, int64, bool, bool, bool);

#define GET_SPM_PLAN_ITEM_BUFFER(idx)	\
	((&(g_SPMPlanItemBuffers[idx])))

#define GET_SPM_PLAN_NODE_ARRAY(idx)	\
	((&(g_SPMPlanCacheArray[idx])))

#define GET_SPM_HISTORY_ITEM_BUFFER(idx)	\
	((&(g_SPMHistoryItemBuffers[idx])))

#define GET_SPM_HISTORY_NODE_ARRAY(idx)	\
	((&(g_SPMHistoryCacheArray[idx])))

#define SPM_PLAN_PARTITION(hashcode)	\
	((hashcode) % NUM_SPM_PLAN_PARTITIONS)

#define SPM_PLAN_PARTITION_LOCK(hashcode)	\
	(&MainLWLockArray[SPM_PLAN_MAPPING_LWLOCK_OFFSET + \
		SPM_PLAN_PARTITION(hashcode)].lock)

#define SPM_PLAN_PARTITION_LOCK_BY_INDEX(i) \
	(&MainLWLockArray[SPM_PLAN_MAPPING_LWLOCK_OFFSET + (i)].lock)

#define SPM_PLAN_ID_PARTITION(hashcode)	\
	((hashcode) % NUM_SPM_PLAN_ID_PARTITIONS)

#define SPM_PLAN_ID_PARTITION_LOCK(hashcode)	\
	(&MainLWLockArray[SPM_PLAN_ID_MAPPING_LWLOCK_OFFSET + \
		SPM_PLAN_ID_PARTITION(hashcode)].lock)

#define SPM_PLAN_ID_PARTITION_LOCK_BY_INDEX(i) \
	(&MainLWLockArray[SPM_PLAN_ID_MAPPING_LWLOCK_OFFSET + (i)].lock)

#define SPM_HISTORY_PARTITION(hashcode)	\
	((hashcode) % NUM_SPM_HISTORY_PARTITIONS)

#define SPM_HISTORY_PARTITION_LOCK(hashcode)	\
	(&MainLWLockArray[SPM_HISTORY_MAPPING_LWLOCK_OFFSET + \
		SPM_HISTORY_PARTITION(hashcode)].lock)

#define SPM_HISTORY_PARTITION_LOCK_BY_INDEX(i) \
	(&MainLWLockArray[SPM_HISTORY_MAPPING_LWLOCK_OFFSET + (i)].lock)

#define SPM_HISTORY_ID_PARTITION(hashcode)	\
	((hashcode) % NUM_SPM_HISTORY_ID_PARTITIONS)

#define SPM_HISTORY_ID_PARTITION_LOCK(hashcode)	\
	(&MainLWLockArray[SPM_HISTORY_ID_MAPPING_LWLOCK_OFFSET + \
		SPM_HISTORY_ID_PARTITION(hashcode)].lock)

#define SPM_HISTORY_ID_PARTITION_LOCK_BY_INDEX(i) \
	(&MainLWLockArray[SPM_HISTORY_ID_MAPPING_LWLOCK_OFFSET + (i)].lock)

static uint32
CalcuteSPMhashCode(SPMCachedKey *key)
{
	return tag_hash(key, sizeof(SPMCachedKey));;
}

static uint32
CalcuteSPMIdhashCode(SPMCachedUniqueKey *key)
{
	return tag_hash(key, sizeof(SPMCachedUniqueKey));;
}

static LWLock*
GetSPMPlanPartitonLock(SPMCachedKey *key, uint32 *hashcode)
{
	*hashcode = CalcuteSPMhashCode(key);
	return SPM_PLAN_PARTITION_LOCK(*hashcode);
}

static LWLock*
GetSPMPlanIdPartitonLock(SPMCachedUniqueKey *key, uint32 *hashcode)
{
	*hashcode = CalcuteSPMIdhashCode(key);
	return SPM_PLAN_ID_PARTITION_LOCK(*hashcode);
}

static LWLock*
GetSPMHistoryPartitonLock(SPMCachedKey *key, uint32 *hashcode)
{
	*hashcode = CalcuteSPMhashCode(key);
	return SPM_HISTORY_PARTITION_LOCK(*hashcode);
}

static LWLock*
GetSPMHistoryIdPartitonLock(SPMCachedUniqueKey *key, uint32 *hashcode)
{
	*hashcode = CalcuteSPMIdhashCode(key);
	return SPM_HISTORY_ID_PARTITION_LOCK(*hashcode);
}

static int 
GetSPMItemBufferId(SPMPlanCachedItem *item)
{
	return item - g_SPMPlanItemBuffers;
}

static int
GetSPMHistoryItemBufferId(SPMHistoryCachedItem *item)
{
	return item - g_SPMHistoryItemBuffers;
}

/*
 * NodeHashTableShmemSize
 *	Get the size of Node Definition hash table
 */
Size 
SPMPlanShmemSize(void)
{
	Size sz = 0;
	int nentries;

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_PLAN_PARTITIONS;
	sz = hash_estimate_size(nentries, sizeof(SPMPlanCachedEntry));	/* g_SPMPlanHashTab */

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_PLAN_ID_PARTITIONS;
	sz = add_size(sz, hash_estimate_size(nentries, sizeof(SPMCachedIdEntry)));	/* g_SPMPlanIdHashTab */

	sz = add_size(sz, mul_size(space_budget_limit, sizeof(SPMPlanCachedItem)));	/* g_SPMPlanItemBuffers */

	sz = add_size(sz, mul_size(space_budget_limit * spm_cache_invalid_factor, sizeof(SPMCacheNode)));	/* g_SPMPlanCacheArray */

	sz = add_size(sz, sizeof(SPMCacheNodeDesc));	/*g_SPMPlanCacheDesc */

	sz = add_size(sz, sizeof(SPMCacheCount));	/* g_SPMCacheCount */

	return sz;
}

void
SPMPlanShmemInit(void)
{
	bool 		foundItems;
	bool		foundArray;
	bool		foundfree;
	bool		foundCount;
	HASHCTL		info;
	HASHCTL		idinfo;
	int			i;
	int			nentries;

	info.keysize = sizeof(SPMCachedKey);
	info.entrysize = sizeof(SPMPlanCachedEntry);
	info.num_partitions = NUM_SPM_PLAN_PARTITIONS;
	info.hash = tag_hash;

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_PLAN_PARTITIONS;
	g_SPMPlanHashTab = ShmemInitHash("SPM plan hash table",
								  nentries, 
								  nentries,
								  &info,
								  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);
	if (!g_SPMPlanHashTab)
	{
		elog(FATAL, "[SPM] invalid shmem status when creating SPM plan hash table");
	}

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_PLAN_ID_PARTITIONS;
	idinfo.keysize   = sizeof(SPMCachedUniqueKey);
	idinfo.entrysize = sizeof(SPMCachedIdEntry);
	idinfo.num_partitions = NUM_SPM_PLAN_ID_PARTITIONS;
	idinfo.hash 	   = tag_hash;
	g_SPMPlanIdHashTab = ShmemInitHash("SPM plan id hash table",
								  nentries, 
								  nentries,
								  &idinfo,
								  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);

	if (!g_SPMPlanIdHashTab)
	{
		elog(FATAL, "[SPM] invalid shmem status when creating SPM plan id hash table");
	}

	g_SPMPlanItemBuffers = (SPMPlanCachedItem *)
				ShmemInitStruct("SPMPlan Items", 
				space_budget_limit * sizeof(SPMPlanCachedItem), 
				&foundItems);

	g_SPMPlanCacheArray = (SPMCacheNode *)
				ShmemInitStruct("SPMPlan Desc Array", 
				space_budget_limit * spm_cache_invalid_factor * sizeof(SPMCacheNode), 
				&foundArray);

	g_SPMPlanCacheDesc = (SPMCacheNodeDesc *)
						ShmemInitStruct("SPMPlan Desc", 
						sizeof(SPMCacheNodeDesc), 
						&foundfree);

	g_SPMCacheCount = (SPMCacheCount *)
						ShmemInitStruct("SPMPlan cache counts", 
						sizeof(SPMCacheCount), 
						&foundCount);

	if (foundItems || foundArray || foundfree || foundCount)
	{
		/* should find all of these, or none of them */
		Assert(foundItems && foundArray && foundfree && foundCount);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		/* init cache node array */
		for (i = 0; i < space_budget_limit * spm_cache_invalid_factor; i++)
		{
			SPMCacheNode *node = &(g_SPMPlanCacheArray[i]);

			node->tag.dboid = InvalidOid;
			node->tag.sql_id = INVALID_SPM_CACHE_SQL_ID;
			node->tag.plan_id = INVALID_SPM_CACHE_PLAN_ID;
			node->valid = false;
			pg_atomic_init_u32(&node->refcount, 0);
			pg_atomic_init_u32(&node->usecount, 0);
		}
		pg_atomic_init_u32(&g_SPMPlanCacheDesc->next_victim, 0);
		pg_atomic_init_u32(&g_SPMPlanCacheDesc->next_invalid_victim, space_budget_limit);
	}
	g_SPMCacheCount->plan_total_count = 0;
	g_SPMCacheCount->plan_hint_count = 0;
	g_SPMCacheCount->history_total_count = 0;
	g_SPMCacheCount->history_hint_count = 0;
	g_SPMCacheCount->plan_resettime = 0;
	g_SPMCacheCount->history_resettime = 0;
	SpinLockInit(&g_SPMCacheCount->slock);
}

Size 
SPMHistoryShmemSize(void)
{
	Size sz = 0;
	int nentries;

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_HISTORY_PARTITIONS;
	sz = hash_estimate_size(nentries, sizeof(SPMHistoryCachedEntry));	/* g_SPMHistoryHashTab */

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_HISTORY_ID_PARTITIONS;
	sz = add_size(sz, hash_estimate_size(nentries, sizeof(SPMCachedIdEntry)));	/* g_SPMHistoryIdHashTab */

	sz = add_size(sz, mul_size(space_budget_limit, sizeof(SPMHistoryCachedItem)));	/* g_SPMHistoryItemBuffers */

	sz = add_size(sz, mul_size(space_budget_limit * spm_cache_invalid_factor, sizeof(SPMCacheNode)));	/* g_SPMHistoryCacheArray */

	sz = add_size(sz, sizeof(SPMCacheNodeDesc)); /*g_SPMHistoryCacheDesc */

	return sz;
}

void
SPMHistoryShmemInit(void)
{
	bool 		foundItems;
	bool		foundArray;
	bool		foundfree;
	HASHCTL		info;
	HASHCTL		idinfo;
	int			i;
	int			nentries;

	info.keysize   = sizeof(SPMCachedKey);
	info.entrysize = sizeof(SPMHistoryCachedEntry);
	info.num_partitions = NUM_SPM_HISTORY_PARTITIONS;
	info.hash 	   = tag_hash;

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_HISTORY_PARTITIONS;
	g_SPMHistoryHashTab = ShmemInitHash("SPM history hash table",
										nentries, 
										nentries,
										&info,
										HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);

	if (!g_SPMHistoryHashTab)
	{
		elog(FATAL, "[SPM] invalid shmem status when creating SPM history hash table");
	}

	nentries = space_budget_limit * spm_cache_invalid_factor + NUM_SPM_HISTORY_ID_PARTITIONS;
	idinfo.keysize   = sizeof(SPMCachedUniqueKey);
	idinfo.entrysize = sizeof(SPMCachedIdEntry);
	info.num_partitions = NUM_SPM_HISTORY_ID_PARTITIONS;
	idinfo.hash 	   = tag_hash;
	g_SPMHistoryIdHashTab = ShmemInitHash("SPM history id hash table",
								  nentries, 
								  nentries,
								  &idinfo,
								  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE | HASH_PARTITION);

	if (!g_SPMHistoryIdHashTab)
	{
		elog(FATAL, "[SPM] invalid shmem status when creating SPM history id hash table");
	}

	g_SPMHistoryItemBuffers = (SPMHistoryCachedItem *)
								ShmemInitStruct("SPM history Items", 
								space_budget_limit * sizeof(SPMHistoryCachedItem), 
								&foundItems);

	g_SPMHistoryCacheArray = (SPMCacheNode *)
							ShmemInitStruct("SPM history Desc Array", 
							space_budget_limit * spm_cache_invalid_factor * sizeof(SPMCacheNode), 
							&foundArray);

	g_SPMHistoryCacheDesc = (SPMCacheNodeDesc *)
							ShmemInitStruct("SPM history freelist Desc", 
							sizeof(SPMCacheNodeDesc), 
							&foundfree);

	if (foundItems || foundArray || foundfree)
	{
		/* should find all of these, or none of them */
		Assert(foundItems && foundArray && foundfree);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		/* init cache node array */
		for (i = 0; i < space_budget_limit * spm_cache_invalid_factor; i++)
		{
			SPMCacheNode *node = &(g_SPMHistoryCacheArray[i]);

			node->tag.dboid = InvalidOid;
			node->tag.sql_id = INVALID_SPM_CACHE_SQL_ID;
			node->tag.plan_id = INVALID_SPM_CACHE_PLAN_ID;
			node->valid = false;
			pg_atomic_init_u32(&node->refcount, 0);
			pg_atomic_init_u32(&node->usecount, 0);
		}
		pg_atomic_init_u32(&g_SPMHistoryCacheDesc->next_victim, 0);
		pg_atomic_init_u32(&g_SPMHistoryCacheDesc->next_invalid_victim, space_budget_limit);
	}
}

static void
InitSPMPlanCachedEntryList(SPMPlanCachedEntry *entry)
{
	LWLockRegisterTranche(LWTRANCHE_SPMPLAN_ENTRY, "spmplan_entry");
	LWLockInitialize(&(entry->lock), LWTRANCHE_SPMPLAN_ENTRY);

	entry->value = NULL;
}

static SPMPlanCachedItem * 
DeleteCacheValueItem(SPMPlanCachedItem *list, SPMPlanCachedItem *item)
{
	SPMPlanCachedItem *cur = list;
	SPMPlanCachedItem *pre = list;
	int                level = LOG;

	if (cur == NULL)
	{
		elog(level, "[SPM] SPMPlanCachedItem list is null, no delete executed");
		return NULL;
	}

	if (cur == item)
	{
		SPMPlanCachedItem *result = cur->next;
		cur->next = NULL;
		return result;
	}

	while (cur)
	{
		if (cur == item)
		{
			pre->next = cur->next;
			cur->next = NULL;
			return list;
		}
		pre = cur;
		cur = cur->next;
	}
	elog(level, "[SPM] The specified item does not exists in SPMPlanCachedItem list, no delete executed");
	return list;
}

static int
SPMCacheIdTabLookup(SPMCachedUniqueKey *key, SPMPlanCacheType type)
{
	SPMCachedIdEntry	*entry;
	bool				found = false;
	int					id = -1;
	uint32				hash_value = 0;
	LWLock				*partiton;
	HTAB *				htab;

	if (type == SPM_PLAN_CACHE_PLAN || type == SPM_PLAN_CACHE_INVALID_PLAN)
	{
		partiton = GetSPMPlanIdPartitonLock(key, &hash_value);
		htab = g_SPMPlanIdHashTab;
	}
	else if (type == SPM_PLAN_CACHE_HISTORY || type == SPM_PLAN_CACHE_INVALID_HISTORY)
	{
		partiton = GetSPMHistoryIdPartitonLock(key, &hash_value);
		htab = g_SPMHistoryIdHashTab;
	}
	
	LWLockAcquire(partiton, LW_SHARED);
	entry = hash_search_with_hash_value(htab, key, hash_value, 
											HASH_FIND, &found);
	if (found)
		id = entry->id;
	LWLockRelease(partiton);
	return id;
}

static void
SPMCacheIdTabInsert(SPMCachedUniqueKey *key, int freeid, bool *found, SPMPlanCacheType type)
{
	SPMCachedIdEntry	*entry;
	uint32				hash_value = 0;
	LWLock				*partiton;
	HTAB *				htab;

	if (type == SPM_PLAN_CACHE_PLAN || type == SPM_PLAN_CACHE_INVALID_PLAN)
	{
		partiton = GetSPMPlanIdPartitonLock(key, &hash_value);
		htab = g_SPMPlanIdHashTab;
	}
	else if (type == SPM_PLAN_CACHE_HISTORY || type == SPM_PLAN_CACHE_INVALID_HISTORY)
	{
		partiton = GetSPMHistoryIdPartitonLock(key, &hash_value);
		htab = g_SPMHistoryIdHashTab;
	}

	LWLockAcquire(partiton, LW_EXCLUSIVE);

	entry = hash_search_with_hash_value(htab, key, hash_value, 
											HASH_ENTER, found);
	if (*found == false)
	{
		entry->id = freeid;
	}

	LWLockRelease(partiton);
}

static int
SPMCacheIdTabRemove(SPMCachedUniqueKey *key, SPMPlanCacheType type)
{
	SPMCachedIdEntry	*entry;
	bool				found = false;
	int					id = -1;
	uint32				hash_value = 0;
	LWLock				*partiton;
	HTAB *				htab;

	if (type == SPM_PLAN_CACHE_PLAN || type == SPM_PLAN_CACHE_INVALID_PLAN)
	{
		partiton = GetSPMPlanIdPartitonLock(key, &hash_value);
		htab = g_SPMPlanIdHashTab;
	}
	else if (type == SPM_PLAN_CACHE_HISTORY || type == SPM_PLAN_CACHE_INVALID_HISTORY)
	{
		partiton = GetSPMHistoryIdPartitonLock(key, &hash_value);
		htab = g_SPMHistoryIdHashTab;
	}

	LWLockAcquire(partiton, LW_EXCLUSIVE);
	entry = hash_search_with_hash_value(htab, key, hash_value, 
											HASH_REMOVE, &found);
	if (found)
		id = entry->id;
	LWLockRelease(partiton);
	return id;
}

static void
SPMCacheSetUseCount(int id, uint32 value, SPMCacheNode *base)
{
	SPMCacheNode	*node = NULL;

	node = base + id;
	pg_atomic_write_u32(&node->usecount, value);
	pg_write_barrier();
}

static uint32
SPMCacheGetUseCount(int id, SPMCacheNode *base)
{
	SPMCacheNode	*node = NULL;

	node = base + id;
	return pg_atomic_read_u32(&node->usecount);
}

static void
SPMCacheAddUseCount(int id, SPMCacheNode *base, SPMPlanCacheType type)
{
	uint32			usecount = 0;
	SPMCacheNode	*node = NULL;
	uint32			usecount_limit = SPM_HISTORY_MAX_USAGE_COUNT;

	if (type == SPM_PLAN_CACHE_PLAN)
	{
		usecount_limit = SPM_PLAN_MAX_USAGE_COUNT;
		SpinLockAcquire(&g_SPMCacheCount->slock);
		g_SPMCacheCount->plan_hint_count = g_SPMCacheCount->plan_hint_count + 1;
		SpinLockRelease(&g_SPMCacheCount->slock);
	}
	else
	{
		SpinLockAcquire(&g_SPMCacheCount->slock);
		g_SPMCacheCount->history_hint_count = g_SPMCacheCount->history_hint_count + 1;
		SpinLockRelease(&g_SPMCacheCount->slock);
	}

	node = base + id;
	usecount = pg_atomic_read_u32(&node->usecount);
	if (usecount < usecount_limit)
		pg_atomic_fetch_add_u32(&node->usecount, 1);
}

static void
SPMCacheSubUseCount(int id, uint32 value, SPMCacheNode *base)
{
	uint32			usecount = 0;
	SPMCacheNode	*node = NULL;

	node = base + id;
	usecount = pg_atomic_read_u32(&node->usecount);
	if (usecount >= value)
		pg_atomic_fetch_sub_u32(&node->usecount, value);
	else
		SPMCacheSetUseCount(id, 0, base);
}

static bool
SPMCachePinNodeArray(int id, SPMCacheNode *base)
{
	uint32			invalid_ref = 0;
	uint32			pin_ref = 1;
	SPMCacheNode	*node = NULL;

	node = base + id;
	return pg_atomic_compare_exchange_u32(&node->refcount, &invalid_ref, pin_ref);
}

static void
SPMCacheSetNodeArray(int id, bool valid, Oid dboid, int64 sql_id, int64 plan_id, SPMCacheNode *base)
{
	SPMCacheNode	*node = NULL;

	node = base + id;
	node->tag.dboid = dboid;
	node->tag.sql_id = sql_id;
	node->tag.plan_id = plan_id;
	node->valid = valid;
}

static void inline
SPMCacheUnPinNodeArray(int id, SPMCacheNode *base)
{
	SPMCacheNode	*node = NULL;

	node = base + id;
	pg_atomic_exchange_u32(&node->refcount, 0);
}

static void
SPMCacheCheckAndRemoveOldCache(int id, SPMCacheNode *base, SPMCacheHashTblDeleteFuncPtr func)
{
	SPMCacheNode		*node = NULL;
	SPMCachedUniqueKey	tag;

	node = base + id;
	if (node->valid)
	{
		memset(&tag, 0, sizeof(SPMCachedUniqueKey));
		tag.dboid = node->tag.dboid;
		tag.sql_id = node->tag.sql_id;
		tag.plan_id = node->tag.plan_id;

		func(tag.dboid, tag.sql_id, tag.plan_id, false, false, false);
	}
}

static int
SPMCacheGetFreeid(Oid dboid, int64 sql_id, int64 plan_id, bool *found, SPMPlanCacheType type)
{
	int		id = -1;
	bool	success;
	int		level = LOG;
	int		trycounter = 0;
	int		trycounter_limit = space_budget_limit;
	SPMCachedUniqueKey	key;
	SPMCacheNode		*cachearray;
	SPMCacheNodeDesc	*cachedesc;
	SPMCacheHashTblDeleteFuncPtr func;
	uint32	subnum = debug_print_spm ? 4 : 3;

	memset(&key, 0, sizeof(SPMCachedUniqueKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	key.plan_id = plan_id;

	id = SPMCacheIdTabLookup(&key, type);
	if (id >= 0)
	{
		*found = true;
		return id;
	}

	if (type == SPM_PLAN_CACHE_PLAN || type == SPM_PLAN_CACHE_INVALID_PLAN)
	{
		subnum = debug_print_spm ? 3 : 2;
		cachearray = g_SPMPlanCacheArray;
		cachedesc = g_SPMPlanCacheDesc;
		func = SPMPlanHashTblDeleteInternal;
	}
	else if (type == SPM_PLAN_CACHE_HISTORY || type == SPM_PLAN_CACHE_INVALID_HISTORY)
	{
		subnum = 1;
		cachearray = g_SPMHistoryCacheArray;
		cachedesc = g_SPMHistoryCacheDesc;
		func = SPMHistoryHashTblDeleteInternal;
	}

	if (type == SPM_PLAN_CACHE_INVALID_PLAN || type == SPM_PLAN_CACHE_INVALID_HISTORY)
		trycounter_limit = space_budget_limit * spm_cache_invalid_factor - space_budget_limit;

	trycounter = trycounter_limit;
	for (;;)
	{
		id = ClockSweepTick(cachedesc, type);
		success = SPMCachePinNodeArray(id, cachearray);
		if (success)
		{
			if (SPMCacheGetUseCount(id, cachearray) > 0)
			{
				SPMCacheSubUseCount(id, subnum, cachearray);
				SPMCacheUnPinNodeArray(id, cachearray);
				trycounter = trycounter_limit;
			}
			else
			{
				break;
			}
		}
		else if (--trycounter == 0)
		{
			elog(level, "[SPM] [CACHE] no unpinned array available");
			*found = true;
			return -1;
		}

		CHECK_FOR_INTERRUPTS();
	}

	SPMCacheCheckAndRemoveOldCache(id, cachearray, func);
	SPMCacheSetNodeArray(id, true, MyDatabaseId, sql_id, plan_id, cachearray);
	SPMCacheIdTabInsert(&key, id, found, type);
	if (*found)
	{
		SPMCacheSetNodeArray(id, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, cachearray);
		SPMCacheUnPinNodeArray(id, cachearray);
		return -1;
	}

	return id;
}

static SPMPlanCachedEntry *
SPMPlanCacheSearchInternal(SPMCachedKey *key, uint32 hash_value, bool recordcount)
{
	bool found = false;
	SPMPlanCachedEntry *entry = NULL;

	if (recordcount)
	{
		uint64 origin;

		SpinLockAcquire(&g_SPMCacheCount->slock);
		origin = g_SPMCacheCount->plan_total_count;

		if (origin == UINT64_MAX)
		{
			uint64 hint = g_SPMCacheCount->plan_hint_count;
			double ratio = (double) hint / (double) origin;
			hint = SPM_CACHE_TOTAL_COUNT_RESET_NUM * ratio;

			g_SPMCacheCount->plan_total_count = SPM_CACHE_TOTAL_COUNT_RESET_NUM;
			g_SPMCacheCount->plan_hint_count = hint;
			g_SPMCacheCount->plan_resettime = GetCurrentTimestamp();
		}
		else
		{
			g_SPMCacheCount->plan_total_count = g_SPMCacheCount->plan_total_count + 1;
		}
		SpinLockRelease(&g_SPMCacheCount->slock);
	}

	entry = (SPMPlanCachedEntry *) hash_search_with_hash_value(g_SPMPlanHashTab, (void *)key, hash_value, HASH_FIND, &found);
	if (!found)
	{
		return NULL;
	}

	return entry;
}

SPMPlanCacheRetNo
SPMPlanCacheSearchByPlanId(Oid dboid, int64 sql_id, int64 plan_id, Datum *values, bool *nulls)
{
	SPMPlanCachedEntry *entry;
	SPMPlanCachedItem *item;
	SPMCachedKey key;
	uint32		hash_value = 0;
	LWLock		*partitionLock;
	int			itemid = -1;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	partitionLock = GetSPMPlanPartitonLock(&key, &hash_value);
	LWLockAcquire(partitionLock, LW_SHARED);

	entry = SPMPlanCacheSearchInternal(&key, hash_value, (values == NULL));
	if (!entry)
	{
		if (nulls)
		{
			int i;
			for (i = 0; i < SPM_PLAN_CACHE_COLS; i++)
				nulls[i] = true;
		}
		
		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_NO_ENTRY;
	}

	if (values)
	{
		values[SPM_PLAN_CACHE_DBOID - 1] = ObjectIdGetDatum(entry->key.dboid);
		values[SPM_PLAN_CACHE_SQL_ID - 1] = Int64GetDatum(entry->key.sql_id);
	}

	if (plan_id == INVALID_SPM_CACHE_PLAN_ID)
	{
		Assert(!entry->valid);
		if (nulls)
		{
			nulls[SPM_PLAN_CACHE_PLAN_ID - 1] = true;
			nulls[SPM_PLAN_CACHE_HINT - 1] = true;
			nulls[SPM_PLAN_CACHE_PRIORITY - 1] = true;
			nulls[SPM_PLAN_CACHE_ENABLED - 1] = true;
			nulls[SPM_PLAN_CACHE_ITEM_ID - 1] = true;
		}
		else
		{
			SPMCacheAddUseCount(entry->id, g_SPMPlanCacheArray, SPM_PLAN_CACHE_PLAN);
		}

		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_MATCHED;
	}

	if (!entry->valid)
	{
		if (nulls)
		{
			nulls[SPM_PLAN_CACHE_PLAN_ID - 1] = true;
			nulls[SPM_PLAN_CACHE_HINT - 1] = true;
			nulls[SPM_PLAN_CACHE_PRIORITY - 1] = true;
			nulls[SPM_PLAN_CACHE_ENABLED - 1] = true;
			nulls[SPM_PLAN_CACHE_ITEM_ID - 1] = true;
		}
		else
		{
			SPMCacheAddUseCount(entry->id, g_SPMPlanCacheArray, SPM_PLAN_CACHE_PLAN);
		}

		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_NO_VALID;
	}

	LWLockAcquire(&(entry->lock), LW_SHARED);

	item = entry->value;
	while (item)
	{
		itemid = GetSPMItemBufferId(item);

		if (item->plan_id == plan_id)
		{
			if (!values)
				SPMCacheAddUseCount(itemid, g_SPMPlanCacheArray, SPM_PLAN_CACHE_PLAN);

			LWLockRelease(&(entry->lock));
			LWLockRelease(partitionLock);

			if (values)
			{
				values[SPM_PLAN_CACHE_PLAN_ID - 1] = Int64GetDatum(item->plan_id);
				values[SPM_PLAN_CACHE_HINT - 1] = CStringGetTextDatum(item->hint);
				values[SPM_PLAN_CACHE_PRIORITY - 1] = Int32GetDatum(item->priority);
				values[SPM_PLAN_CACHE_ENABLED - 1] = BoolGetDatum(item->enabled);
				values[SPM_PLAN_CACHE_ITEM_ID - 1] = Int32GetDatum(itemid);
			}

			return SPM_PLAN_CACHE_MATCHED;
		}

		item = item->next;
	}

	LWLockRelease(&(entry->lock));
	LWLockRelease(partitionLock);

	return SPM_PLAN_CACHE_NO_MATCHED;
}

SPMPlanCacheRetNo 
GetSPMBindHint(int64 sql_id, int64 *plan_id, char **hint)
{
	SPMPlanCachedEntry *entry;
	SPMPlanCachedItem *item;
	SPMCachedKey key;
	uint32	hash_value = 0;
	LWLock *partitionLock;
	int		itemid = -1;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = MyDatabaseId;
	key.sql_id = sql_id;
	partitionLock = GetSPMPlanPartitonLock(&key, &hash_value);
	LWLockAcquire(partitionLock, LW_SHARED);

	entry = SPMPlanCacheSearchInternal(&key, hash_value, true);
	if (!entry)
	{
		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_NO_ENTRY;
	}

	if (!entry->valid)
	{
		SPMCacheAddUseCount(entry->id, g_SPMPlanCacheArray, SPM_PLAN_CACHE_PLAN);
		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_NO_VALID;
	}

	LWLockAcquire(&(entry->lock), LW_SHARED);

	item = entry->value;
	while(item)
	{
		itemid = GetSPMItemBufferId(item);
		SPMCacheAddUseCount(itemid, g_SPMPlanCacheArray, SPM_PLAN_CACHE_PLAN);
		if (item->priority == SPM_PLAN_FIXED && item->enabled)
		{
			if (plan_id)
				*plan_id = item->plan_id;
			if (hint)
				*hint = pstrdup(item->hint);

			LWLockRelease(&(entry->lock));
			LWLockRelease(partitionLock);

			return SPM_PLAN_CACHE_FIXED;
		}

		item = item->next;
	}

	LWLockRelease(&(entry->lock));
	LWLockRelease(partitionLock);

	return SPM_PLAN_CACHE_NO_MATCHED;
}

static void
SPMPlanHashTblDeleteInternal(Oid dboid, int64 sql_id, int64 plan_id, 
								bool delentries, bool pinnode, bool unpinnode)
{
	SPMPlanCachedEntry *entry;
	SPMPlanCachedItem *item;
	SPMCachedKey key;
	SPMCachedUniqueKey ukey;
	uint32	hash_value = 0;
	LWLock *partitionLock;
	int		freeid;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	partitionLock = GetSPMPlanPartitonLock(&key, &hash_value);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	if (debug_print_spm)
		elog(LOG, "[SPM] [CACHE] SPMPlanHashTblDeleteInternal enter, sql_id: %ld, plan_id:%ld", sql_id, plan_id);

	entry = SPMPlanCacheSearchInternal(&key, hash_value, false);
	if (!entry)
	{
		LWLockRelease(partitionLock);
		return;
	}

	if (!entry->valid && (plan_id == INVALID_SPM_CACHE_PLAN_ID || delentries))
	{
		memset(&ukey, 0, sizeof(SPMCachedUniqueKey));
		ukey.dboid = dboid;
		ukey.sql_id = sql_id;
		ukey.plan_id = INVALID_SPM_CACHE_PLAN_ID;

		if (pinnode)
		{
			SPMCachePinNodeArray(entry->id, g_SPMPlanCacheArray);
		}
		SPMCacheIdTabRemove(&ukey, SPM_PLAN_CACHE_INVALID_PLAN);
		hash_search_with_hash_value(g_SPMPlanHashTab, &key, hash_value, HASH_REMOVE, NULL);
		SPMCacheSetUseCount(entry->id, 0, g_SPMPlanCacheArray);
		SPMCacheSetNodeArray(entry->id, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMPlanCacheArray);
		if (pinnode || unpinnode)
		{
			SPMCacheUnPinNodeArray(entry->id, g_SPMPlanCacheArray);
		}
		LWLockRelease(partitionLock);
		return;
	}

	LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);

	item = entry->value;
	while (item)
	{
		SPMPlanCachedItem *cur = item;
		item = cur->next;

		if (delentries || cur->plan_id == plan_id)
		{
			memset(&ukey, 0, sizeof(SPMCachedUniqueKey));
			ukey.dboid = dboid;
			ukey.sql_id = sql_id;
			ukey.plan_id = cur->plan_id;

			freeid = GetSPMItemBufferId(cur);
			if (cur->plan_id == plan_id)
			{
				if (pinnode)
				{
					SPMCachePinNodeArray(freeid, g_SPMPlanCacheArray);
				}
				SPMCacheIdTabRemove(&ukey, SPM_PLAN_CACHE_PLAN);
				entry->value = DeleteCacheValueItem(entry->value, cur);
				SPMCacheSetUseCount(freeid, 0, g_SPMPlanCacheArray);
				SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMPlanCacheArray);
				if (pinnode || unpinnode)
				{
					SPMCacheUnPinNodeArray(freeid, g_SPMPlanCacheArray);
				}
				break;
			}
			else
			{
				if (pinnode)
				{
					SPMCachePinNodeArray(freeid, g_SPMPlanCacheArray);
				}
				SPMCacheIdTabRemove(&ukey, SPM_PLAN_CACHE_PLAN);
				memset(cur, 0, sizeof(SPMPlanCachedItem));
				entry->value = NULL;
				SPMCacheSetUseCount(freeid, 0, g_SPMPlanCacheArray);
				SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMPlanCacheArray);
				if (pinnode || unpinnode)
				{
					SPMCacheUnPinNodeArray(freeid, g_SPMPlanCacheArray);
				}
			}
		}
	}

	if (!entry->value)
	{
		if (debug_print_spm)
			elog(LOG, "[SPM] [CACHE] SPMPlanHashTblDeleteInternal remove entry, sqlid: %ld, dboid: %u", sql_id, dboid);
		LWLockRelease(&(entry->lock));
		hash_search_with_hash_value(g_SPMPlanHashTab, &key, hash_value, HASH_REMOVE, NULL);
	}
	else
	{
		LWLockRelease(&(entry->lock));
	}
	
	LWLockRelease(partitionLock);
}

void
SPMPlanHashTblDelete(Oid dboid, int64 sql_id, int64 plan_id)
{
	SPMPlanHashTblDeleteInternal(dboid, sql_id, plan_id, false, true, false);
}

void
SPMPlanHashTblDeleteEntry(Oid dboid, int64 sql_id)
{
	SPMPlanHashTblDeleteInternal(dboid, sql_id, 0, true, true, false);
}

static bool
SPMPlanHashTabInsertInternal(int64 sql_id, int64 plan_id, char *hint, int32 priority, 
						bool enabled, int64 executions)
{
	int		freeid;
	bool	found = false;
	bool	need_retry = false;
	SPMPlanCachedEntry	*entry;
	SPMPlanCachedItem	*item;
	SPMCachedKey	key;
	SPMCachedUniqueKey	*ukey;
	MemoryContext	old_cxt   = NULL;
	uint32			hash_value = 0;
	LWLock			*partitionLock;
	SPMPlanCacheType	type;

	/* When the hint length exceeds SPM_PLAN_HINT_MAX_len, do not cache SPM plan information. */
	if (hint != NULL && strlen(hint) >= SPM_PLAN_HINT_MAX_LEN)
		return false;

	if (plan_id == INVALID_SPM_CACHE_PLAN_ID)
		type = SPM_PLAN_CACHE_INVALID_PLAN;
	else
		type = SPM_PLAN_CACHE_PLAN;

	freeid = SPMCacheGetFreeid(MyDatabaseId, sql_id, plan_id, &found, type);
	if (found)
	{
		return false;
	}

	if (debug_print_spm)
	{
		elog(LOG, "[SPM] [CACHE] SPMPlanHashTabInsert get freeid: %d, insert sql_id: %ld, plan_id: %ld", 
						freeid, sql_id, plan_id);
	}

	found = false;
	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = MyDatabaseId;
	key.sql_id = sql_id;
	partitionLock = GetSPMPlanPartitonLock(&key, &hash_value);
	LWLockAcquire(partitionLock, LW_SHARED);
	entry = (SPMPlanCachedEntry *) 
				hash_search_with_hash_value(g_SPMPlanHashTab, &key, 
												hash_value, HASH_FIND, &found);
	if (!found)
	{
		LWLockRelease(partitionLock);
		LWLockAcquire(partitionLock, LW_EXCLUSIVE);
		if (debug_print_spm)
		{
			elog(LOG, "[SPM] SPMPlanHashTabInsert first get freeid: %d, insert sql_id: %ld, plan_id: %ld", 
							freeid, sql_id, plan_id);
		}
		entry = (SPMPlanCachedEntry *) hash_search(g_SPMPlanHashTab, &key, HASH_ENTER, &found);
		if (entry == NULL)
		{
			SPMCachedUniqueKey idkey;

			LWLockRelease(partitionLock);
			memset(&idkey, 0, sizeof(SPMCachedUniqueKey));
			idkey.dboid = MyDatabaseId;
			idkey.sql_id = sql_id;
			idkey.plan_id = plan_id;
			SPMCacheIdTabRemove(&idkey, SPM_PLAN_CACHE_PLAN);
			SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMPlanCacheArray);
			SPMCacheUnPinNodeArray(freeid, g_SPMPlanCacheArray);
			elog(WARNING, "[SPM] SPM plan cache insert: insert entry failed");
			return false;
		}

		if (plan_id == INVALID_SPM_CACHE_PLAN_ID)
		{
			if (!found)
			{
				entry->valid = false;
				entry->id = freeid;
				SPMCacheUnPinNodeArray(freeid, g_SPMPlanCacheArray);
			}
			LWLockRelease(partitionLock);
			return false;
		}

		if (!found)
		{
			InitSPMPlanCachedEntryList(entry);
		}
	}
	
	if (found)
	{
		/* If an invalid entry is attempted to be inserted when a record already exists, abandon the
		 * insertion. */
		if (plan_id == INVALID_SPM_CACHE_PLAN_ID ||
		    (plan_id != INVALID_SPM_CACHE_PLAN_ID && !entry->valid))
		{
			SPMCachedUniqueKey idkey;

			/* If inserting a valid entry and finding an existing invalid entry, delete the invalid
			 * one before proceeding with the insertion. */
			if (plan_id != INVALID_SPM_CACHE_PLAN_ID && !entry->valid)
			{
				need_retry = true;
			}

			LWLockRelease(partitionLock);

			memset(&idkey, 0, sizeof(SPMCachedUniqueKey));
			idkey.dboid = MyDatabaseId;
			idkey.sql_id = sql_id;
			idkey.plan_id = plan_id;
			SPMCacheIdTabRemove(&idkey, SPM_PLAN_CACHE_PLAN);
			SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMPlanCacheArray);
			SPMCacheUnPinNodeArray(freeid, g_SPMPlanCacheArray);
			if (debug_print_spm)
				elog(LOG,
				     "[SPM] [CACHE] SPM plan cache insert aborted: due to found that another "
				     "record already exists.");
			return need_retry;
		}
	}

	LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);
	/* the item stored in g_SPMPlanCacheArray */
	item = GET_SPM_PLAN_ITEM_BUFFER(freeid);
	memset(item, 0, sizeof(SPMPlanCachedItem));
	item->plan_id = plan_id;
	if (hint)
		StrNCpy(item->hint, hint, SPM_PLAN_HINT_MAX_LEN);
	item->priority = priority;
	item->enabled = enabled;
	item->executions = executions;
	item->next = entry->value;
	entry->value = item;
	entry->valid = true;
	SPMCacheUnPinNodeArray(freeid, g_SPMPlanCacheArray);

	LWLockRelease(&(entry->lock));
	LWLockRelease(partitionLock);

	/* Insert into pending list */
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	ukey = (SPMCachedUniqueKey *) palloc0(sizeof(SPMCachedUniqueKey));
	ukey->dboid = MyDatabaseId;
	ukey->sql_id = sql_id;
	ukey->plan_id = plan_id;
	g_spm_cache_plan_pending_list = lappend(g_spm_cache_plan_pending_list, ukey);
	MemoryContextSwitchTo(old_cxt);
	return need_retry;
}

void
SPMPlanHashTabInsert(int64 sql_id, int64 plan_id, char *hint, int32 priority, 
						bool enabled, int64 executions)
{
	uint32 retry_cnt = 0;

	while (SPMPlanHashTabInsertInternal(sql_id, plan_id, hint, priority, enabled, executions))
	{
		SPMPlanHashTblDeleteInternal(MyDatabaseId,
		                                sql_id,
		                                INVALID_SPM_CACHE_PLAN_ID,
		                                false,
		                                true,
		                                false);
		retry_cnt++;
		CHECK_FOR_INTERRUPTS();
	}

	if (debug_print_spm && retry_cnt > 0)
		elog(LOG,
		     "[SPM] SPMPlanHashTabInsert success[retry %u cnts], found that another record "
		     "already exists",
		     retry_cnt);
}

static void
SPMCacheClearCount(SPMPlanCacheType type)
{
	SpinLockAcquire(&g_SPMCacheCount->slock);

	if (type == SPM_PLAN_CACHE_PLAN)
	{
		g_SPMCacheCount->plan_hint_count = 0;
		g_SPMCacheCount->plan_total_count = 0;
		g_SPMCacheCount->plan_resettime = GetCurrentTimestamp();
	}
	else if (type == SPM_PLAN_CACHE_HISTORY)
	{
		g_SPMCacheCount->history_hint_count = 0;
		g_SPMCacheCount->history_total_count = 0;
		g_SPMCacheCount->history_resettime = GetCurrentTimestamp();;
	}

	SpinLockRelease(&g_SPMCacheCount->slock);
}

static void 
SPMCacheHashTabClearInternal(SPMPlanCacheType type)
{
	int                          i;
	SPMCacheNode                *cachearray = NULL;
	SPMCacheHashTblDeleteFuncPtr func = SPMPlanHashTblDeleteInternal;
	int                          limit = space_budget_limit * spm_cache_invalid_factor;

	if (type == SPM_PLAN_CACHE_PLAN)
	{
		cachearray = g_SPMPlanCacheArray;
		func = SPMPlanHashTblDeleteInternal;
	}
	else if (type == SPM_PLAN_CACHE_HISTORY)
	{
		cachearray = g_SPMHistoryCacheArray;
		func = SPMHistoryHashTblDeleteInternal;
	}

	for (i = 0; i < limit; i++)
	{
		bool success = false;

		while (!success)
		{
			success = SPMCachePinNodeArray(i, cachearray);
		}
		SPMCacheCheckAndRemoveOldCache(i, cachearray, func);
	}

	SPMCacheClearCount(type);

	for (i = 0; i < limit; i++)
	{
		SPMCacheUnPinNodeArray(i, cachearray);
	}

	elog(LOG, "[SPM] SPM plan cache has been successfully destroyed");
}

void 
SPMCacheHashTabClear(void)
{
	SPMCacheHashTabClearInternal(SPM_PLAN_CACHE_PLAN);
	SPMCacheHashTabClearInternal(SPM_PLAN_CACHE_HISTORY);
}

void
SPMCacheShowHintRatio(TupleDesc tupdesc, Tuplestorestate *tupstore)
{
	Datum			values[SPM_CACHE_COUNTS_COL];
	bool			nulls[SPM_CACHE_COUNTS_COL];
	uint32			plan_counts;
	uint32			plan_hint_counts;
	uint32			history_counts;
	uint32			history_hint_count;
	float8			plan_hint_ratio = 0.0;
	float8			history_hint_ratio = 0.0;		

	if (!g_SPMCacheCount)
	{
		int i;
		for (i = 0; i < SPM_CACHE_COUNTS_COL; i++)
			nulls[i] = false;
		return;
	}

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	SpinLockAcquire(&g_SPMCacheCount->slock);

	plan_counts = g_SPMCacheCount->plan_total_count;
	plan_hint_counts = g_SPMCacheCount->plan_hint_count;
	history_counts = g_SPMCacheCount->history_total_count;
	history_hint_count = g_SPMCacheCount->history_hint_count;
	SpinLockRelease(&g_SPMCacheCount->slock);

	if (plan_counts > 0)
		plan_hint_ratio = ((double) plan_hint_counts) / ((double) plan_counts);
	if (history_counts > 0)
		history_hint_ratio = ((double) history_hint_count) / ((double) history_counts);
	values[SPM_CACHE_PLAN_COUNTS - 1] = Int64GetDatum(plan_counts);
	values[SPM_CACHE_PLAN_HINT_COUNTS - 1] = Int64GetDatum(plan_hint_counts);
	values[SPM_CACHE_HISTORY_COUNTS - 1] = Int64GetDatum(history_counts);
	values[SPM_CACHE_HISTORY_HINT_COUNTS - 1] = Int64GetDatum(history_hint_count);
	values[SPM_CACHE_PLAN_HINT_RATIO - 1] = Float8GetDatum(plan_hint_ratio);
	values[SPM_CACHE_HISTORY_HINT_RATIO - 1] = Float8GetDatum(history_hint_ratio);
}

void
SPMPlanSearchAll(TupleDesc tupdesc, Tuplestorestate *tupstore)
{
	int				i;
	int				limit = space_budget_limit * spm_cache_invalid_factor;
	SPMCacheNode	*cachearray = g_SPMPlanCacheArray;
	Datum			values[SPM_PLAN_CACHE_COLS];
	bool			nulls[SPM_PLAN_CACHE_COLS];
	SPMCacheNode	*node = NULL;

	for (i = 0; i < limit; i++)
	{
		bool	success = false;
		int		trypin = 100;

		while (!success && trypin--)
		{
			CHECK_FOR_INTERRUPTS();
			success = SPMCachePinNodeArray(i, cachearray);
		}

		node = GET_SPM_PLAN_NODE_ARRAY(i);
		if (!success)
		{
			elog(WARNING, "Pin array[%d]: valid: %d, sql_id: %ld, plan_id: %ld, dboid: %u falied, skip", 
					i, node->valid, node->tag.sql_id, node->tag.plan_id, node->tag.dboid);
			continue;
		}

		if (!node->valid)
		{
			SPMCacheUnPinNodeArray(i, cachearray);
			continue;
		}
		
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		SPMPlanCacheSearchByPlanId(node->tag.dboid, node->tag.sql_id, node->tag.plan_id, values, nulls);
		values[SPM_PLAN_CACHE_ARRAY_ID - 1] = Int32GetDatum(i);
		values[SPM_PLAN_CACHE_USECOUNT - 1] = Int32GetDatum(pg_atomic_read_u32(&node->usecount));
		SPMCacheUnPinNodeArray(i, cachearray);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
}

void
SPMPlanSearchAllBySqlId(Oid dboid, int64 sql_id, TupleDesc tupdesc, Tuplestorestate *tupstore)
{
	Datum			values[SPM_PLAN_CACHE_COLS];
	bool			nulls[SPM_PLAN_CACHE_COLS];
	SPMCacheNode	*node = NULL;
	SPMPlanCachedEntry	*entry;
	SPMCachedKey		key;
	LWLock				*partitionLock;
	uint32				hash_value = 0;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	partitionLock = GetSPMPlanPartitonLock(&key, &hash_value);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);
	entry = SPMPlanCacheSearchInternal(&key, hash_value, false);
	if (!entry)
	{
		int i;
		for(i = 0; i < SPM_PLAN_CACHE_COLS; i++)
			nulls[i] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		LWLockRelease(partitionLock);
		return;
	}

	if (!entry->valid)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		nulls[SPM_PLAN_CACHE_PLAN_ID - 1] = true;
		nulls[SPM_PLAN_CACHE_HINT - 1] = true;
		nulls[SPM_PLAN_CACHE_PRIORITY - 1] = true;
		nulls[SPM_PLAN_CACHE_ENABLED - 1] = true;
		nulls[SPM_PLAN_CACHE_ITEM_ID - 1] = true;
		values[SPM_PLAN_CACHE_ARRAY_ID - 1] = Int32GetDatum(entry->id);
		values[SPM_PLAN_CACHE_DBOID - 1] = ObjectIdGetDatum(entry->key.dboid);
		values[SPM_PLAN_CACHE_SQL_ID - 1] = Int64GetDatum(entry->key.sql_id);
		node = GET_SPM_PLAN_NODE_ARRAY(entry->id);
		values[SPM_PLAN_CACHE_USECOUNT - 1] = Int32GetDatum(pg_atomic_read_u32(&node->usecount));

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	else
	{
		SPMPlanCachedItem *item;
		int	itemid;

		item = entry->value;
		while (item)
		{
			itemid = GetSPMItemBufferId(item);

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));
			values[SPM_PLAN_CACHE_PLAN_ID] = Int64GetDatum(item->plan_id);
			values[SPM_PLAN_CACHE_HINT] = CStringGetTextDatum(item->hint);
			values[SPM_PLAN_CACHE_PRIORITY] = Int32GetDatum(item->priority);
			values[SPM_PLAN_CACHE_ENABLED] = BoolGetDatum(item->enabled);
			values[SPM_PLAN_CACHE_ITEM_ID] = Int32GetDatum(itemid);
			values[SPM_PLAN_CACHE_ARRAY_ID - 1] = Int32GetDatum(itemid);
			values[SPM_PLAN_CACHE_DBOID - 1] = ObjectIdGetDatum(entry->key.dboid);
			values[SPM_PLAN_CACHE_SQL_ID - 1] = Int64GetDatum(entry->key.sql_id);
			node = GET_SPM_PLAN_NODE_ARRAY(itemid);
			values[SPM_PLAN_CACHE_USECOUNT - 1] = Int32GetDatum(pg_atomic_read_u32(&node->usecount));
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			item = item->next;

			CHECK_FOR_INTERRUPTS();
		}
	}

	LWLockRelease(partitionLock);
}

void
SPMPlanRelaseBySqlId(Oid dboid, int64 sql_id, int64 plan_id)
{
	SPMPlanHashTblDeleteInternal(dboid, sql_id, plan_id, false, false, true);
}

/*
 * Scan all the tuples in the cacheArray to check if there is any slot that 
 * cannot be pinned. If there is, it is considered an abnormal scenario caused 
 * by a slot that was not correctly unpinned, and compensatory handling should 
 * be performed here-force unpin. Note that it is possible for a slot in use 
 * by other caches to be mistakenly deleted during busy business hours, but 
 * since this function is only used for compensation, it is acceptable to 
 * delete some cache.
 */
int
SPMPlanRelaseCrushed(int trycount)
{
	SPMCacheNode	*cachearray = g_SPMPlanCacheArray;
	SPMCacheNode	*node = NULL;
	int				limit = space_budget_limit * spm_cache_invalid_factor;
	int				i = 0;
	int				nreleased = 0;

	for (i = 0; i < limit; i++)
	{
		bool	success = false;
		int		trypin = trycount;

		while (!success && trypin--)
		{
			CHECK_FOR_INTERRUPTS();
			success = SPMCachePinNodeArray(i, cachearray);
		}

		node = GET_SPM_PLAN_NODE_ARRAY(i);
		if (!success)
		{
			elog(WARNING, "Pin array[%d]: valid: %d, sql_id: %ld, plan_id: %ld, dboid: %u falied, release", 
					i, node->valid, node->tag.sql_id, node->tag.plan_id, node->tag.dboid);
			if (node->valid)
			{
				SPMPlanHashTblDeleteInternal(node->tag.dboid, node->tag.sql_id, node->tag.plan_id, false, false, true);
			}
			else
			{
				SPMCacheUnPinNodeArray(i, cachearray);
			}
			nreleased++;
		}
		else
		{
			SPMCacheUnPinNodeArray(i, cachearray);
			continue;
		}
	}

	return nreleased;
}

static void
AbortSPMPlanPendingList(void)
{
	ListCell	*cell 		 = NULL;

	if (g_spm_cache_plan_pending_list == NULL)
		return;

	foreach(cell, g_spm_cache_plan_pending_list)
	{
		SPMCachedUniqueKey *ukey = lfirst(cell);

		SPMPlanHashTblDelete(ukey->dboid, ukey->sql_id, ukey->plan_id);
	}

	list_free_deep(g_spm_cache_plan_pending_list);
	g_spm_cache_plan_pending_list = NULL;
}

static void
CommitSPMPlanPendingList(void)
{
	if (g_spm_cache_plan_pending_list == NULL)
		return;

	list_free_deep(g_spm_cache_plan_pending_list);
	g_spm_cache_plan_pending_list = NULL;
}

static void
InitSPMHistoryCachedEntryList(SPMHistoryCachedEntry *entry)
{
	LWLockRegisterTranche(LWTRANCHE_SPM_HISTORY_ENTRY, "spm_history_entry");
	LWLockInitialize(&(entry->lock), LWTRANCHE_SPM_HISTORY_ENTRY);

	entry->value = NULL;
}

static SPMHistoryCachedItem * 
DeleteHistoryCacheValueItem(SPMHistoryCachedItem *list, SPMHistoryCachedItem *item)
{
	SPMHistoryCachedItem *cur = list;
	SPMHistoryCachedItem *pre = list;
	int                   level = LOG;

	if (cur == NULL)
	{
		elog(level, "[SPM] SPMHistoryCachedItem list is null, no delete executed");
		return NULL;
	}

	if (cur == item)
	{
		SPMHistoryCachedItem *result = cur->next;
		cur->next = NULL;
		return result;
	}

	while (cur)
	{
		if (cur == item)
		{
			pre->next = cur->next;
			cur->next = NULL;
			return list;
		}
		pre = cur;
		cur = cur->next;
	}
	elog(level, "[SPM] The specified item does not exists in SPMPlanCachedItem list, no delete executed");
	return list;
}

/*
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static uint32
ClockSweepTick(SPMCacheNodeDesc *cachedesc, SPMPlanCacheType type)
{
	uint32 victim = 0;
	uint32 limit = 0;
	uint32 base = 0;

	/*
	 * Atomically move hand ahead one slot - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	if (type == SPM_PLAN_CACHE_PLAN || type == SPM_PLAN_CACHE_HISTORY)
	{
		victim =
			pg_atomic_fetch_add_u32(&cachedesc->next_victim, 1);
		limit = space_budget_limit;
		base = 0;
	}
	else if (type == SPM_PLAN_CACHE_INVALID_PLAN || type == SPM_PLAN_CACHE_INVALID_HISTORY)
	{
		victim =
			pg_atomic_fetch_add_u32(&cachedesc->next_invalid_victim, 1);
		limit = space_budget_limit * spm_cache_invalid_factor;
		base = space_budget_limit;
	}

	if (victim >= limit)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in descriptors */
		victim = victim % limit + base;

		if (victim == base)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				wrapped = expected % limit;
				if (type == SPM_PLAN_CACHE_PLAN || type == SPM_PLAN_CACHE_HISTORY)
				{
					success = pg_atomic_compare_exchange_u32(&cachedesc->next_victim,
																&expected, wrapped);
				}
				else if (type == SPM_PLAN_CACHE_INVALID_PLAN || type == SPM_PLAN_CACHE_INVALID_HISTORY)
				{
					success = pg_atomic_compare_exchange_u32(&cachedesc->next_invalid_victim,
																&expected, wrapped);
				}
			}
		}
	}

	return victim;
}

static SPMHistoryCachedEntry *
SPMHistoryCacheSearchInternal(SPMCachedKey *key, uint32 hashcode, bool recordcount)
{
	bool found = false;
	SPMHistoryCachedEntry *entry = NULL;
	int   level = (debug_print_spm) ? LOG : DEBUG5;

	if (recordcount)
	{
		uint64 origin;

		SpinLockAcquire(&g_SPMCacheCount->slock);
		origin = g_SPMCacheCount->history_total_count;
		if (origin == UINT64_MAX)
		{
			uint64 hint = g_SPMCacheCount->history_hint_count;
			double ratio = (double) hint / (double) origin;
			hint = SPM_CACHE_TOTAL_COUNT_RESET_NUM * ratio;

			g_SPMCacheCount->history_total_count = SPM_CACHE_TOTAL_COUNT_RESET_NUM;
			g_SPMCacheCount->history_hint_count = hint;
			g_SPMCacheCount->history_resettime = GetCurrentTimestamp();
		}
		else
		{
			g_SPMCacheCount->history_total_count = g_SPMCacheCount->history_total_count + 1;
		}
		SpinLockRelease(&g_SPMCacheCount->slock);
	}

	entry = (SPMHistoryCachedEntry *) hash_search_with_hash_value(g_SPMHistoryHashTab, (void *)key, hashcode, HASH_FIND, &found);
	if (!found)
	{
		elog(level, "[SPM] SPM history cache dont found sql_id: %ld, %u", 
			key->sql_id, key->dboid);
		return NULL;
	}

	return entry;
}

List *
SPMHistoryCacheSearchBySqlid(int64 sql_id)
{
	SPMHistoryCachedEntry *entry;
	SPMHistoryCachedItem  *item;
	SPMCachedKey           key;
	List                  *plan_list = NIL;
	LWLock                *partitionLock;
	uint32                 hashcode = 0;

	key.dboid = MyDatabaseId;
	key.sql_id = sql_id;
	partitionLock = GetSPMHistoryPartitonLock(&key, &hashcode);

	LWLockAcquire(partitionLock, LW_SHARED);
	entry = SPMHistoryCacheSearchInternal(&key, hashcode, false);
	if (!entry)
	{
		LWLockRelease(partitionLock);
		return NIL;
	}

	if (!entry->valid)
	{
		LWLockRelease(partitionLock);
		return NIL;
	}

	LWLockAcquire(&(entry->lock), LW_SHARED);

	item = entry->value;
	while (item)
	{
		SPMHistoryCachedItemForTblSync *item_sync = (SPMHistoryCachedItemForTblSync *) palloc0(sizeof(SPMHistoryCachedItemForTblSync));

		item_sync->plan_id = item->plan_id;
		item_sync->executions = pg_atomic_read_u64(&item->executions);
		item_sync->elapsed_time = pg_atomic_read_u64(&item->elapsed_time);
		plan_list = lappend(plan_list, item_sync);
		item = item->next;
	}

	LWLockRelease(&(entry->lock));
	LWLockRelease(partitionLock);

	return plan_list;
}


SPMPlanCacheRetNo
SPMHistoryCacheSearchByPlanId(Oid dboid, int64 sql_id, int64 plan_id, Datum *values, bool *nulls, int64 execute_time)
{
	SPMHistoryCachedEntry *entry;
	SPMHistoryCachedItem *item;
	SPMCachedKey key;
	LWLock *partitionLock;
	uint32	hashcode = 0;
	int		freeid = -1;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	partitionLock = GetSPMHistoryPartitonLock(&key, &hashcode);
	LWLockAcquire(partitionLock, LW_SHARED);

	entry = SPMHistoryCacheSearchInternal(&key, hashcode, (values == NULL));
	if (!entry)
	{
		if (nulls)
		{
			int i;
			for (i = 0; i < SPM_PLAN_CACHE_COLS; i++)
				nulls[i] = true;
		}

		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_NO_ENTRY;
	}

	if (values)
	{
		values[SPM_HISTORY_CACHE_DBOID - 1] = ObjectIdGetDatum(entry->key.dboid);
		values[SPM_HISTORY_CACHE_SQL_ID - 1] = Int64GetDatum(entry->key.sql_id);
	}

	if (plan_id == INVALID_SPM_CACHE_PLAN_ID)
	{
		Assert(!entry->valid);
		if (nulls)
		{
			nulls[SPM_HISTORY_CACHE_PLAN_ID - 1] = true;
			nulls[SPM_HISTORY_CACHE_ITEM_ID - 1] = true;
		}
		else
		{
			SPMCacheAddUseCount(entry->id, g_SPMHistoryCacheArray, SPM_PLAN_CACHE_HISTORY);
		}

		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_MATCHED;
	}

	if (!entry->valid)
	{
		if (nulls)
		{
			nulls[SPM_HISTORY_CACHE_PLAN_ID - 1] = true;
			nulls[SPM_HISTORY_CACHE_ITEM_ID - 1] = true;
		}
		else
		{
			SPMCacheAddUseCount(entry->id, g_SPMHistoryCacheArray, SPM_PLAN_CACHE_HISTORY);
		}

		LWLockRelease(partitionLock);
		return SPM_PLAN_CACHE_NO_VALID;
	}

	LWLockAcquire(&(entry->lock), LW_SHARED);

	item = entry->value;
	while (item != NULL)
	{
		freeid = GetSPMHistoryItemBufferId(item);
		if (item->plan_id == plan_id)
		{
			if (!values)
			{
				if (execute_time > 0)
				{
					int64 elapsed_time = pg_atomic_read_u64(&item->elapsed_time);
					int64 executions = pg_atomic_fetch_add_u64(&item->executions, 1);
					double new_executions;
					double new_elapsed_time;

					/*
					 * Forward compatibility: Adjust the values of elapsed_time and executions to
					 * avoid division by zero.
					 */
					if (elapsed_time < 0)
						elapsed_time = 0;

					if (executions < 0)
						executions = 0;

					if (execute_time < 0)
						execute_time = 0;

					new_executions = executions + 1;
					new_elapsed_time = ((double) elapsed_time * executions + execute_time) / (double) new_executions;
					pg_atomic_write_u64(&item->executions, (int64) new_executions);
					pg_atomic_write_u64(&item->elapsed_time, (int64) new_elapsed_time);
				}

				SPMCacheAddUseCount(freeid, g_SPMHistoryCacheArray, SPM_PLAN_CACHE_HISTORY);
			}

			LWLockRelease(&(entry->lock));
			LWLockRelease(partitionLock);

			if (values)
			{
				values[SPM_HISTORY_CACHE_PLAN_ID - 1] = Int64GetDatum(item->plan_id);
				values[SPM_HISTORY_CACHE_ITEM_ID - 1] = Int32GetDatum(freeid);
			}
			return SPM_PLAN_CACHE_MATCHED;
		}
		item = item->next;
	}

	LWLockRelease(&(entry->lock));
	LWLockRelease(partitionLock);

	return SPM_PLAN_CACHE_NO_MATCHED;
}


static void
SPMHistoryHashTblDeleteInternal(Oid dboid, int64 sql_id, int64 plan_id, 
								 bool delentries, bool pinnode, bool unpinnode)
{
	SPMHistoryCachedEntry *entry;
	SPMHistoryCachedItem *item;
	SPMCachedKey key;
	LWLock *partitionLock;
	uint32	hashcode = 0;
	int		freeid;
	SPMCachedUniqueKey ukey;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	partitionLock = GetSPMHistoryPartitonLock(&key, &hashcode);
	if (debug_print_spm)
		elog(LOG, "[SPM] [CACHE] SPMHistoryHashTblDeleteInternal enter, sql_id: %ld, plan_id:%ld", sql_id, plan_id);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	entry = SPMHistoryCacheSearchInternal(&key, hashcode, false);
	if (!entry)
	{
		LWLockRelease(partitionLock);
		return;
	}

	if (!entry->valid && (plan_id == INVALID_SPM_CACHE_PLAN_ID || delentries))
	{
		memset(&ukey, 0, sizeof(SPMCachedUniqueKey));
		ukey.dboid = dboid;
		ukey.sql_id = sql_id;
		ukey.plan_id = INVALID_SPM_CACHE_PLAN_ID;

		if (pinnode)
		{
			SPMCachePinNodeArray(entry->id, g_SPMHistoryCacheArray);
		}
		SPMCacheIdTabRemove(&ukey, SPM_PLAN_CACHE_INVALID_HISTORY);
		hash_search_with_hash_value(g_SPMHistoryHashTab, &key, hashcode, HASH_REMOVE, NULL);
		SPMCacheSetUseCount(entry->id, 0, g_SPMHistoryCacheArray);
		SPMCacheSetNodeArray(entry->id, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMHistoryCacheArray);
		if (pinnode || unpinnode)
		{
			SPMCacheUnPinNodeArray(entry->id, g_SPMHistoryCacheArray);
		}
		LWLockRelease(partitionLock);
		return;
	}

	LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);
	item = entry->value;
	while (item)
	{
		SPMHistoryCachedItem *cur = item;
		item = cur->next;

		if (delentries || cur->plan_id == plan_id)
		{
			memset(&ukey, 0, sizeof(SPMCachedUniqueKey));
			ukey.dboid = dboid;
			ukey.sql_id = sql_id;
			ukey.plan_id = cur->plan_id;

			freeid = GetSPMHistoryItemBufferId(cur);
			if (cur->plan_id == plan_id)
			{
				if (pinnode)
				{
					SPMCachePinNodeArray(freeid, g_SPMHistoryCacheArray);
				}
				SPMCacheIdTabRemove(&ukey, SPM_PLAN_CACHE_HISTORY);
				entry->value = DeleteHistoryCacheValueItem(entry->value, cur);
				SPMCacheSetUseCount(freeid, 0, g_SPMHistoryCacheArray);
				SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMHistoryCacheArray);
				if (pinnode || unpinnode)
				{
					SPMCacheUnPinNodeArray(freeid, g_SPMHistoryCacheArray);
				}
				break;
			}
			else
			{
				if (pinnode)
				{
					SPMCachePinNodeArray(freeid, g_SPMHistoryCacheArray);
				}
				SPMCacheIdTabRemove(&ukey, SPM_PLAN_CACHE_HISTORY);
				memset(cur, 0, sizeof(SPMHistoryCachedItem));
				entry->value = NULL;
				SPMCacheSetUseCount(freeid, 0, g_SPMHistoryCacheArray);
				SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMHistoryCacheArray);
				if (pinnode || unpinnode)
				{
					SPMCacheUnPinNodeArray(freeid, g_SPMHistoryCacheArray);
				}
			}
			
			if (debug_print_spm)
				elog(DEBUG1, "[SPM] SPMHistoryHashTblDeleteInternal remove item, sql_id: %ld, plan_id: %ld, freeid: %d", sql_id, plan_id, freeid);
		}
	}

	if (entry->value == NULL)
	{
		if (debug_print_spm)
			elog(LOG, "[SPM] [CACHE] SPMHistoryHashTblDeleteInternal remove entry");
		LWLockRelease(&(entry->lock));
		hash_search_with_hash_value(g_SPMHistoryHashTab, &key, hashcode, HASH_REMOVE, NULL);
	}
	else
	{
		LWLockRelease(&(entry->lock));
	}

	LWLockRelease(partitionLock);
}

void
SPMHistoryHashTblDelete(Oid dboid, int64 sql_id, int64 plan_id)
{
	SPMHistoryHashTblDeleteInternal(dboid, sql_id, plan_id, false, true, false);
}

void
SPMHistoryHashTblDeleteEntry(Oid dboid, int64 sql_id)
{
	SPMHistoryHashTblDeleteInternal(dboid, sql_id, 0, true, true, false);
}

static bool
SPMHistoryHashTabInsertInternal(int64 sql_id, int64 plan_id, int64 executions, int64 execute_time)
{
	int                    freeid;
	bool                   found = false;
	bool                   need_retry = false;
	SPMHistoryCachedEntry *entry;
	SPMHistoryCachedItem  *item;
	SPMCachedKey           key;
	SPMCachedUniqueKey    *ukey;
	MemoryContext          old_cxt;
	LWLock                *partitionLock;
	uint32                 hashcode = 0;
	SPMPlanCacheType       type;

	if (plan_id == INVALID_SPM_CACHE_PLAN_ID)
		type = SPM_PLAN_CACHE_INVALID_HISTORY;
	else
		type = SPM_PLAN_CACHE_HISTORY;

	found = false;
	freeid = SPMCacheGetFreeid(MyDatabaseId, sql_id, plan_id, &found, type);
	if (found)
		return false;

	if (debug_print_spm)
	{
		elog(LOG, "[SPM] [CACHE] SPMHistoryHashTabInsert freeid: %d, sql_id: %ld, plan_id: %ld, dboid: %u", 
				freeid, sql_id, plan_id, MyDatabaseId);
	}

	found = false;
	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = MyDatabaseId;
	key.sql_id = sql_id;
	partitionLock = GetSPMHistoryPartitonLock(&key, &hashcode);
	LWLockAcquire(partitionLock, LW_SHARED);
	entry = (SPMHistoryCachedEntry *) hash_search_with_hash_value(g_SPMHistoryHashTab, &key, hashcode, HASH_FIND, &found);
	if (!found)
	{
		LWLockRelease(partitionLock);
		LWLockAcquire(partitionLock, LW_EXCLUSIVE);
		entry = (SPMHistoryCachedEntry *) hash_search_with_hash_value(g_SPMHistoryHashTab, &key, hashcode, HASH_ENTER, &found);
		if (entry == NULL)
		{
			SPMCachedUniqueKey idkey;
			LWLockRelease(partitionLock);

			memset(&idkey, 0, sizeof(SPMCachedUniqueKey));
			idkey.dboid = MyDatabaseId;
			idkey.sql_id = sql_id;
			idkey.plan_id = plan_id;
			SPMCacheIdTabRemove(&idkey, SPM_PLAN_CACHE_HISTORY);
			SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMHistoryCacheArray);
			SPMCacheUnPinNodeArray(freeid, g_SPMHistoryCacheArray);
			elog(WARNING, "[SPM] [CACHE] SPM history cache insert: insert entry failed");
			return false;
		}

		if (plan_id == INVALID_SPM_CACHE_PLAN_ID)
		{
			if (!found)
			{
				entry->valid = false;
				entry->id = freeid;
				SPMCacheUnPinNodeArray(freeid, g_SPMHistoryCacheArray);
			}
			LWLockRelease(partitionLock);
			return false;
		}

		if (!found)
		{
			InitSPMHistoryCachedEntryList(entry);
		}
	}

	if (found)
	{
		/* If an invalid entry is attempted to be inserted when a record already exists, abandon the
		 * insertion. */
		if (plan_id == INVALID_SPM_CACHE_PLAN_ID ||
		    (plan_id != INVALID_SPM_CACHE_PLAN_ID && !entry->valid))
		{
			SPMCachedUniqueKey idkey;

			/* If inserting a valid entry and finding an existing invalid entry, delete the invalid
			 * one before proceeding with the insertion. */
			if (plan_id != INVALID_SPM_CACHE_PLAN_ID && !entry->valid)
			{
				need_retry = true;
			}

			LWLockRelease(partitionLock);

			memset(&idkey, 0, sizeof(SPMCachedUniqueKey));
			idkey.dboid = MyDatabaseId;
			idkey.sql_id = sql_id;
			idkey.plan_id = plan_id;
			SPMCacheIdTabRemove(&idkey, SPM_PLAN_CACHE_HISTORY);
			SPMCacheSetNodeArray(freeid, false, InvalidOid, INVALID_SPM_CACHE_SQL_ID, INVALID_SPM_CACHE_PLAN_ID, g_SPMHistoryCacheArray);
			SPMCacheUnPinNodeArray(freeid, g_SPMHistoryCacheArray);
			if (debug_print_spm)
				elog(LOG,
				     "[SPM] [CACHE] SPM history cache insert aborted: due to found that another "
				     "record "
				     "already exists.");
			return need_retry;
		}
	}

	LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);

	/* the item stored in g_SPMHistoryCacheArray */
	item = GET_SPM_HISTORY_ITEM_BUFFER(freeid);
	memset(item, 0, sizeof(SPMHistoryCachedItem));
	item->plan_id = plan_id;
	pg_atomic_write_u64(&item->executions, executions);
	pg_atomic_write_u64(&item->elapsed_time, execute_time);
	item->next = entry->value;
	entry->value = item;
	entry->valid = true;

	SPMCacheSetNodeArray(freeid, true, MyDatabaseId, sql_id, plan_id, g_SPMHistoryCacheArray);
	SPMCacheSetUseCount(freeid, 0, g_SPMHistoryCacheArray);
	SPMCacheUnPinNodeArray(freeid, g_SPMHistoryCacheArray);

	LWLockRelease(&(entry->lock));
	LWLockRelease(partitionLock);

	/* Insert into pending list */
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	ukey = (SPMCachedUniqueKey *) palloc0(sizeof(SPMCachedUniqueKey));
	ukey->dboid = MyDatabaseId;
	ukey->sql_id = sql_id;
	ukey->plan_id = plan_id;
	g_spm_cache_history_pending_list = lappend(g_spm_cache_history_pending_list, ukey);
	MemoryContextSwitchTo(old_cxt);
	return need_retry;
}

void
SPMHistoryHashTabInsert(int64 sql_id, int64 plan_id, int64 executions, int64 execute_time)
{
	uint32 retry_cnt = 0;

	while (SPMHistoryHashTabInsertInternal(sql_id, plan_id, executions, execute_time))
	{
		SPMHistoryHashTblDeleteInternal(MyDatabaseId,
		                                sql_id,
		                                INVALID_SPM_CACHE_PLAN_ID,
		                                false,
		                                true,
		                                false);
		retry_cnt++;
		CHECK_FOR_INTERRUPTS();
	}

	if (debug_print_spm && retry_cnt > 0)
		elog(LOG,
		     "[SPM] SPMHistoryHashTabInsert success[retry %u cnts], found that another record "
		     "already exists",
		     retry_cnt);
}

void
SPMHistorySearchAll(TupleDesc tupdesc, Tuplestorestate *tupstore)
{
	int				i;
	int				limit = space_budget_limit * spm_cache_invalid_factor;
	SPMCacheNode	*cachearray = g_SPMHistoryCacheArray;
	Datum			values[SPM_HISTORY_CACHE_COLS];
	bool			nulls[SPM_HISTORY_CACHE_COLS];
	SPMCacheNode	*node = NULL;

	for (i = 0; i < limit; i++)
	{
		bool	success = false;
		int		trypin = 100;

		while (!success && trypin--)
		{
			CHECK_FOR_INTERRUPTS();
			success = SPMCachePinNodeArray(i, cachearray);
		}

		node = GET_SPM_HISTORY_NODE_ARRAY(i);
		if (!success)
		{
			elog(WARNING, "Pin array[%d]: valid: %d, sql_id: %ld, plan_id: %ld, dboid: %u falied, skip", 
					i, node->valid, node->tag.sql_id, node->tag.plan_id, node->tag.dboid);
			continue;
		}

		if (!node->valid)
		{
			SPMCacheUnPinNodeArray(i, cachearray);
			continue;
		}
		
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		SPMHistoryCacheSearchByPlanId(node->tag.dboid, node->tag.sql_id, node->tag.plan_id, values, nulls, -1);
		values[SPM_HISTORY_CACHE_ARRAY_ID - 1] = Int32GetDatum(i);
		values[SPM_HISTORY_CACHE_USECOUNT - 1] = Int32GetDatum(pg_atomic_read_u32(&node->usecount));
		SPMCacheUnPinNodeArray(i, cachearray);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
}

void
SPMHistorySearchAllBySqlId(Oid dboid, int64 sql_id, TupleDesc tupdesc, Tuplestorestate *tupstore)
{
	Datum			values[SPM_HISTORY_CACHE_COLS];
	bool			nulls[SPM_HISTORY_CACHE_COLS];
	SPMCacheNode	*node = NULL;
	SPMHistoryCachedEntry	*entry;
	SPMCachedKey			key;
	LWLock					*partitionLock;
	uint32					hash_value = 0;

	memset(&key, 0, sizeof(SPMCachedKey));
	key.dboid = dboid;
	key.sql_id = sql_id;
	partitionLock = GetSPMHistoryPartitonLock(&key, &hash_value);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);
	entry = SPMHistoryCacheSearchInternal(&key, hash_value, false);
	if (!entry)
	{
		int i;
		for(i = 0; i < SPM_HISTORY_CACHE_COLS; i++)
			nulls[i] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		LWLockRelease(partitionLock);
		return;
	}

	if (!entry->valid)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		nulls[SPM_HISTORY_CACHE_PLAN_ID - 1] = true;
		nulls[SPM_HISTORY_CACHE_ITEM_ID - 1] = true;
		values[SPM_HISTORY_CACHE_ARRAY_ID - 1] = Int32GetDatum(entry->id);
		values[SPM_HISTORY_CACHE_DBOID - 1] = ObjectIdGetDatum(entry->key.dboid);
		values[SPM_HISTORY_CACHE_SQL_ID - 1] = Int64GetDatum(entry->key.sql_id);
		node = GET_SPM_HISTORY_NODE_ARRAY(entry->id);
		values[SPM_HISTORY_CACHE_USECOUNT - 1] = Int32GetDatum(pg_atomic_read_u32(&node->usecount));

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	else
	{
		SPMHistoryCachedItem *item;
		int	itemid;

		item = entry->value;
		while (item)
		{
			itemid = GetSPMHistoryItemBufferId(item);

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));
			values[SPM_HISTORY_CACHE_PLAN_ID] = Int64GetDatum(item->plan_id);
			values[SPM_HISTORY_CACHE_ITEM_ID] = Int32GetDatum(itemid);
			values[SPM_HISTORY_CACHE_ARRAY_ID - 1] = Int32GetDatum(itemid);
			values[SPM_HISTORY_CACHE_DBOID - 1] = ObjectIdGetDatum(entry->key.dboid);
			values[SPM_HISTORY_CACHE_SQL_ID - 1] = Int64GetDatum(entry->key.sql_id);
			node = GET_SPM_HISTORY_NODE_ARRAY(itemid);
			values[SPM_HISTORY_CACHE_USECOUNT - 1] = Int32GetDatum(pg_atomic_read_u32(&node->usecount));

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			item = item->next;

			CHECK_FOR_INTERRUPTS();
		}
	}

	LWLockRelease(partitionLock);
}

void
SPMHistoryRelaseBySqlId(Oid dboid, int64 sql_id, int64 plan_id)
{
	SPMHistoryHashTblDeleteInternal(dboid, sql_id, plan_id, false, false, true);
}

int
SPMHistoryRelaseCrushed(int trycount)
{
	SPMCacheNode	*cachearray = g_SPMHistoryCacheArray;
	SPMCacheNode	*node = NULL;
	int				limit = space_budget_limit * spm_cache_invalid_factor;
	int				i = 0;
	int				nreleased = 0;

	for (i = 0; i < limit; i++)
	{
		bool	success = false;
		int		trypin = trycount;

		while (!success && trypin--)
		{
			CHECK_FOR_INTERRUPTS();
			success = SPMCachePinNodeArray(i, cachearray);
		}

		node = GET_SPM_HISTORY_NODE_ARRAY(i);
		if (!success)
		{
			elog(WARNING, "Pin array[%d]: valid: %d, sql_id: %ld, plan_id: %ld, dboid: %u falied, release", 
					i, node->valid, node->tag.sql_id, node->tag.plan_id, node->tag.dboid);
			if (node->valid)
			{
				SPMHistoryHashTblDeleteInternal(node->tag.dboid, node->tag.sql_id, node->tag.plan_id, false, false, true);
			}
			else
			{
				SPMCacheUnPinNodeArray(i, cachearray);
			}

			nreleased++;
		}
		else
		{
			SPMCacheUnPinNodeArray(i, cachearray);
			continue;
		}
	}

	return nreleased;
}

static void
AbortSPMHistoryPendingList(void)
{
	ListCell *cell = NULL;
	int       level = (debug_print_spm) ? LOG : DEBUG5;

	if (g_spm_cache_history_pending_list == NULL)
		return;

	foreach (cell, g_spm_cache_history_pending_list)
	{
		SPMCachedUniqueKey *ukey = lfirst(cell);

		SPMHistoryHashTblDelete(ukey->dboid, ukey->sql_id, ukey->plan_id);

		elog(level,
			     "[SPM] Capture terminated, due to rollback txn. dboid: %u, sql_id: %ld, plan_id: %ld",
			     ukey->dboid,
			     ukey->sql_id,
			     ukey->plan_id);
	}

	list_free_deep(g_spm_cache_history_pending_list);
	g_spm_cache_history_pending_list = NULL;
}

static void
CommitSPMHistoryPendingList(void)
{
	if (g_spm_cache_history_pending_list == NULL)
		return;

	list_free_deep(g_spm_cache_history_pending_list);
	g_spm_cache_history_pending_list = NULL;
}

void
AbortSPMPendingList(void)
{
	AbortSPMPlanPendingList();
	AbortSPMHistoryPendingList();
}

void
CommitSPMPendingList(void)
{
	CommitSPMPlanPendingList();
	CommitSPMHistoryPendingList();
}

