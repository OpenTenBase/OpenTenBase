#include "postgres.h"

#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "ivfglobalcache.h"
#include "utils/builtins.h"

static GlobalSubtreeCacheHeader *global_cache_hdr = NULL;
static bool global_cache_init_attempted = false;

bool
IvfflatGlobalCacheInit(void)
{
	int			fd;
	struct stat	st;
	bool		is_creator = false;
	Size		shm_size = sizeof(GlobalSubtreeCacheHeader);

	if (global_cache_hdr != NULL)
		return true;

	if (global_cache_init_attempted && global_cache_hdr == NULL)
		return false;

	global_cache_init_attempted = true;

	/* Open or create shared memory */
	fd = shm_open(GLOBAL_SUBTREE_CACHE_NAME, O_RDWR | O_CREAT, 0666);
	if (fd < 0)
	{
		elog(WARNING, "IvfflatGlobalCache: shm_open failed: %m");
		return false;
	}

	if (fstat(fd, &st) < 0)
	{
		elog(WARNING, "IvfflatGlobalCache: fstat failed: %m");
		close(fd);
		return false;
	}

	if (st.st_size < (off_t) shm_size)
	{
		if (ftruncate(fd, shm_size) < 0)
		{
			elog(WARNING, "IvfflatGlobalCache: ftruncate failed: %m");
			close(fd);
			return false;
		}
		is_creator = true;
	}

	global_cache_hdr = (GlobalSubtreeCacheHeader *) mmap(NULL, shm_size,
														  PROT_READ | PROT_WRITE,
														  MAP_SHARED, fd, 0);
	close(fd);

	if (global_cache_hdr == MAP_FAILED)
	{
		global_cache_hdr = NULL;
		elog(WARNING, "IvfflatGlobalCache: mmap failed: %m");
		return false;
	}

	/* If we created it or magic is missing, initialize the shared structure */
	if (is_creator || global_cache_hdr->magic != GLOBAL_SUBTREE_CACHE_MAGIC)
	{
		pthread_rwlockattr_t attr;

		pthread_rwlockattr_init(&attr);
		pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
		pthread_rwlock_init(&global_cache_hdr->rwlock, &attr);
		pthread_rwlockattr_destroy(&attr);

		global_cache_hdr->version = GLOBAL_SUBTREE_CACHE_VERSION;
		global_cache_hdr->maxSlots = GLOBAL_SUBTREE_CACHE_MAX_SLOTS;
		global_cache_hdr->activeSlots = 0;
		global_cache_hdr->clockSeq = 0;
		global_cache_hdr->totalLookups = 0;
		global_cache_hdr->totalHits = 0;
		global_cache_hdr->totalInserts = 0;
		global_cache_hdr->totalEvictions = 0;

		for (int i = 0; i < GLOBAL_SUBTREE_CACHE_MAX_SLOTS; i++)
		{
			global_cache_hdr->hashTable[i] = -1;
			global_cache_hdr->nextSlot[i] = -1;
			global_cache_hdr->slots[i].isValid = 0;
		}

		/* Set magic at the very end to signal initialization completion */
		global_cache_hdr->magic = GLOBAL_SUBTREE_CACHE_MAGIC;
	}

	return true;
}

bool
IvfflatGlobalCacheLookup(Oid indexRelOid, uint64 vectorHash, int probes,
						 ItemPointerData *tids, double *distances, int *numItems)
{
	uint32		bucket;
	int32		slotIdx;
	bool		hit = false;

	if (global_cache_hdr == NULL && !IvfflatGlobalCacheInit())
		return false;

	bucket = (uint32) (vectorHash % global_cache_hdr->maxSlots);

	if (pthread_rwlock_rdlock(&global_cache_hdr->rwlock) != 0)
		return false;

	__sync_fetch_and_add(&global_cache_hdr->totalLookups, 1);

	slotIdx = global_cache_hdr->hashTable[bucket];
	while (slotIdx >= 0 && slotIdx < (int32) global_cache_hdr->maxSlots)
	{
		GlobalSubtreeSlot *slot = &global_cache_hdr->slots[slotIdx];

		if (slot->isValid &&
			slot->indexRelOid == indexRelOid &&
			slot->vectorHash == vectorHash &&
			slot->probes == probes)
		{
			/* Cache Hit! */
			int			count = slot->numItems;

			if (count > GLOBAL_SUBTREE_MAX_ITEMS)
				count = GLOBAL_SUBTREE_MAX_ITEMS;

			memcpy(tids, slot->tids, count * sizeof(ItemPointerData));
			memcpy(distances, slot->distances, count * sizeof(double));
			*numItems = count;

			slot->accessCount++;
			slot->lastAccessSeq = __sync_fetch_and_add(&global_cache_hdr->clockSeq, 1);
			__sync_fetch_and_add(&global_cache_hdr->totalHits, 1);
			hit = true;
			break;
		}
		slotIdx = global_cache_hdr->nextSlot[slotIdx];
	}

	pthread_rwlock_unlock(&global_cache_hdr->rwlock);
	return hit;
}

void
IvfflatGlobalCacheInsert(Oid indexRelOid, uint64 vectorHash, int probes,
						 const ItemPointerData *tids, const double *distances, int numItems)
{
	uint32		bucket;
	int32		slotIdx;
	int32		targetSlot = -1;

	if (numItems <= 0)
		return;

	if (numItems > GLOBAL_SUBTREE_MAX_ITEMS)
		numItems = GLOBAL_SUBTREE_MAX_ITEMS;

	if (global_cache_hdr == NULL && !IvfflatGlobalCacheInit())
		return;

	bucket = (uint32) (vectorHash % global_cache_hdr->maxSlots);

	if (pthread_rwlock_wrlock(&global_cache_hdr->rwlock) != 0)
		return;

	/* Double check if already inserted while waiting for write lock */
	slotIdx = global_cache_hdr->hashTable[bucket];
	while (slotIdx >= 0 && slotIdx < (int32) global_cache_hdr->maxSlots)
	{
		GlobalSubtreeSlot *slot = &global_cache_hdr->slots[slotIdx];

		if (slot->isValid &&
			slot->indexRelOid == indexRelOid &&
			slot->vectorHash == vectorHash &&
			slot->probes == probes)
		{
			/* Update existing slot */
			memcpy(slot->tids, tids, numItems * sizeof(ItemPointerData));
			memcpy(slot->distances, distances, numItems * sizeof(double));
			slot->numItems = numItems;
			slot->lastAccessSeq = ++global_cache_hdr->clockSeq;
			slot->accessCount++;
			pthread_rwlock_unlock(&global_cache_hdr->rwlock);
			return;
		}
		slotIdx = global_cache_hdr->nextSlot[slotIdx];
	}

	/* Find a free slot or evict the oldest LRU slot */
	if (global_cache_hdr->activeSlots < global_cache_hdr->maxSlots)
	{
		targetSlot = (int32) global_cache_hdr->activeSlots;
		global_cache_hdr->activeSlots++;
	}
	else
	{
		/* Cache full: find victim slot with minimum lastAccessSeq */
		uint64		minSeq = UINT64_MAX;
		int32		victimBucket = -1;
		int32		prevSlot = -1;

		targetSlot = 0;
		for (uint32 i = 0; i < global_cache_hdr->maxSlots; i++)
		{
			if (global_cache_hdr->slots[i].lastAccessSeq < minSeq)
			{
				minSeq = global_cache_hdr->slots[i].lastAccessSeq;
				targetSlot = (int32) i;
			}
		}

		/* Remove victim from its existing hash chain */
		victimBucket = (uint32) (global_cache_hdr->slots[targetSlot].vectorHash % global_cache_hdr->maxSlots);
		slotIdx = global_cache_hdr->hashTable[victimBucket];
		prevSlot = -1;

		while (slotIdx >= 0 && slotIdx < (int32) global_cache_hdr->maxSlots)
		{
			if (slotIdx == targetSlot)
			{
				if (prevSlot == -1)
					global_cache_hdr->hashTable[victimBucket] = global_cache_hdr->nextSlot[slotIdx];
				else
					global_cache_hdr->nextSlot[prevSlot] = global_cache_hdr->nextSlot[slotIdx];
				break;
			}
			prevSlot = slotIdx;
			slotIdx = global_cache_hdr->nextSlot[slotIdx];
		}
		global_cache_hdr->totalEvictions++;
	}

	/* Populate target slot */
	{
		GlobalSubtreeSlot *slot = &global_cache_hdr->slots[targetSlot];

		slot->indexRelOid = indexRelOid;
		slot->vectorHash = vectorHash;
		slot->probes = probes;
		slot->numItems = numItems;
		slot->accessCount = 1;
		slot->lastAccessSeq = ++global_cache_hdr->clockSeq;
		memcpy(slot->tids, tids, numItems * sizeof(ItemPointerData));
		memcpy(slot->distances, distances, numItems * sizeof(double));
		slot->isValid = 1;

		/* Prepend to hash bucket chain */
		global_cache_hdr->nextSlot[targetSlot] = global_cache_hdr->hashTable[bucket];
		global_cache_hdr->hashTable[bucket] = targetSlot;
		global_cache_hdr->totalInserts++;
	}

	pthread_rwlock_unlock(&global_cache_hdr->rwlock);
}

void
IvfflatGlobalCacheStats(uint32 *maxSlots, uint32 *activeSlots,
						uint64 *totalLookups, uint64 *totalHits,
						uint64 *totalInserts, uint64 *totalEvictions)
{
	if (global_cache_hdr == NULL && !IvfflatGlobalCacheInit())
	{
		*maxSlots = 0;
		*activeSlots = 0;
		*totalLookups = 0;
		*totalHits = 0;
		*totalInserts = 0;
		*totalEvictions = 0;
		return;
	}

	pthread_rwlock_rdlock(&global_cache_hdr->rwlock);
	*maxSlots = global_cache_hdr->maxSlots;
	*activeSlots = global_cache_hdr->activeSlots;
	*totalLookups = global_cache_hdr->totalLookups;
	*totalHits = global_cache_hdr->totalHits;
	*totalInserts = global_cache_hdr->totalInserts;
	*totalEvictions = global_cache_hdr->totalEvictions;
	pthread_rwlock_unlock(&global_cache_hdr->rwlock);
}

void
IvfflatGlobalCacheClear(void)
{
	if (global_cache_hdr == NULL && !IvfflatGlobalCacheInit())
		return;

	pthread_rwlock_wrlock(&global_cache_hdr->rwlock);
	global_cache_hdr->activeSlots = 0;
	global_cache_hdr->clockSeq = 0;
	global_cache_hdr->totalLookups = 0;
	global_cache_hdr->totalHits = 0;
	global_cache_hdr->totalInserts = 0;
	global_cache_hdr->totalEvictions = 0;

	for (int i = 0; i < GLOBAL_SUBTREE_CACHE_MAX_SLOTS; i++)
	{
		global_cache_hdr->hashTable[i] = -1;
		global_cache_hdr->nextSlot[i] = -1;
		global_cache_hdr->slots[i].isValid = 0;
	}
	pthread_rwlock_unlock(&global_cache_hdr->rwlock);
}

PG_FUNCTION_INFO_V1(ivfflat_global_cache_stats);
Datum
ivfflat_global_cache_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[7];
	bool		nulls[7] = {false};
	HeapTuple	tuple;
	uint32		maxSlots = 0, activeSlots = 0;
	uint64		totalLookups = 0, totalHits = 0, totalInserts = 0, totalEvictions = 0;
	double		hitRatio = 0.0;

	IvfflatGlobalCacheStats(&maxSlots, &activeSlots, &totalLookups, &totalHits, &totalInserts, &totalEvictions);

	if (totalLookups > 0)
		hitRatio = ((double) totalHits / (double) totalLookups) * 100.0;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	values[0] = Int32GetDatum((int32) maxSlots);
	values[1] = Int32GetDatum((int32) activeSlots);
	values[2] = Int64GetDatum((int64) totalLookups);
	values[3] = Int64GetDatum((int64) totalHits);
	values[4] = Int64GetDatum((int64) totalInserts);
	values[5] = Int64GetDatum((int64) totalEvictions);
	values[6] = Float8GetDatum(hitRatio);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

PG_FUNCTION_INFO_V1(ivfflat_global_cache_clear);
Datum
ivfflat_global_cache_clear(PG_FUNCTION_ARGS)
{
	IvfflatGlobalCacheClear();
	PG_RETURN_BOOL(true);
}
