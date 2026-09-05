#include "postgres.h"

#include <float.h>

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "lib/pairingheap.h"
#include "ivfflat.h"
#include "ivfglobalcache.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#define GetScanList(ptr) pairingheap_container(IvfflatScanList, ph_node, ptr)
#define GetScanListConst(ptr) pairingheap_const_container(IvfflatScanList, ph_node, ptr)

#define IVFFLAT_CACHE_HASH_SIZE 1024
#define IVFFLAT_CACHE_MAX_ITEMS 128

typedef struct IvfflatCacheItem
{
	ItemPointerData heaptid;
	double		distance;
} IvfflatCacheItem;

typedef struct IvfflatCacheEntry
{
	Oid			indexRelOid;
	uint64		vectorHash;
	int			probes;
	int			numItems;
	IvfflatCacheItem items[IVFFLAT_CACHE_MAX_ITEMS];
	struct IvfflatCacheEntry *prev;
	struct IvfflatCacheEntry *next;
	struct IvfflatCacheEntry *hashNext;
} IvfflatCacheEntry;

static MemoryContext ivfflat_cache_ctx = NULL;
static IvfflatCacheEntry *cache_hash_table[IVFFLAT_CACHE_HASH_SIZE] = {NULL};
static IvfflatCacheEntry *cache_lru_head = NULL;
static IvfflatCacheEntry *cache_lru_tail = NULL;
static int	cache_entry_count = 0;

static uint64
IvfflatVectorHash64(Datum value)
{
	Vector	   *v = (Vector *) DatumGetPointer(value);
	int			dim = v->dim;
	uint64		hash = 0xcbf29ce484222325ULL;
	const uint32 *p = (const uint32 *) v->x;

	for (int i = 0; i < dim; i++)
	{
		hash ^= (uint64) p[i];
		hash *= 0x100000001b3ULL;
	}
	return hash;
}

static IvfflatCacheEntry *
IvfflatCacheLookup(Oid indexRelOid, uint64 vectorHash, int probes)
{
	uint32		bucket;
	IvfflatCacheEntry *entry;

	if (cache_lru_head == NULL)
		return NULL;

	bucket = (uint32) (vectorHash % IVFFLAT_CACHE_HASH_SIZE);
	entry = cache_hash_table[bucket];

	while (entry != NULL)
	{
		if (entry->indexRelOid == indexRelOid &&
			entry->vectorHash == vectorHash &&
			entry->probes == probes)
		{
			/* Move hit entry to LRU head */
			if (entry != cache_lru_head)
			{
				if (entry->prev)
					entry->prev->next = entry->next;
				if (entry->next)
					entry->next->prev = entry->prev;
				if (entry == cache_lru_tail)
					cache_lru_tail = entry->prev;

				entry->prev = NULL;
				entry->next = cache_lru_head;
				if (cache_lru_head)
					cache_lru_head->prev = entry;
				cache_lru_head = entry;
				if (cache_lru_tail == NULL)
					cache_lru_tail = entry;
			}
			return entry;
		}
		entry = entry->hashNext;
	}
	return NULL;
}

static void
IvfflatCacheInsert(Oid indexRelOid, uint64 vectorHash, int probes, const ItemPointerData *tids, const double *distances, int numItems)
{
	uint32		bucket;
	IvfflatCacheEntry *entry = NULL;

	if (numItems <= 0)
		return;

	if (numItems > IVFFLAT_CACHE_MAX_ITEMS)
		numItems = IVFFLAT_CACHE_MAX_ITEMS;

	if (ivfflat_cache_ctx == NULL)
	{
		ivfflat_cache_ctx = AllocSetContextCreateInternal(TopMemoryContext,
														   "Ivfflat Query LRU Cache Context",
														   ALLOCSET_DEFAULT_SIZES);
	}

	bucket = (uint32) (vectorHash % IVFFLAT_CACHE_HASH_SIZE);

	/* If cache is full, evict the LRU tail */
	if (cache_entry_count >= ivfflat_query_cache_size && cache_lru_tail != NULL)
	{
		IvfflatCacheEntry *evict = cache_lru_tail;
		uint32		evict_bucket = (uint32) (evict->vectorHash % IVFFLAT_CACHE_HASH_SIZE);
		IvfflatCacheEntry **prev_hash = &cache_hash_table[evict_bucket];

		while (*prev_hash != NULL && *prev_hash != evict)
			prev_hash = &((*prev_hash)->hashNext);

		if (*prev_hash == evict)
			*prev_hash = evict->hashNext;

		if (evict->prev)
			evict->prev->next = NULL;
		cache_lru_tail = evict->prev;
		if (cache_lru_head == evict)
			cache_lru_head = NULL;

		entry = evict;
	}
	else
	{
		entry = (IvfflatCacheEntry *) MemoryContextAllocZero(ivfflat_cache_ctx, sizeof(IvfflatCacheEntry));
		cache_entry_count++;
	}

	entry->indexRelOid = indexRelOid;
	entry->vectorHash = vectorHash;
	entry->probes = probes;
	entry->numItems = numItems;
	for (int i = 0; i < numItems; i++)
	{
		entry->items[i].heaptid = tids[i];
		entry->items[i].distance = distances[i];
	}

	/* Insert at head of LRU */
	entry->prev = NULL;
	entry->next = cache_lru_head;
	if (cache_lru_head)
		cache_lru_head->prev = entry;
	cache_lru_head = entry;
	if (cache_lru_tail == NULL)
		cache_lru_tail = entry;

	/* Insert into hash table */
	entry->hashNext = cache_hash_table[bucket];
	cache_hash_table[bucket] = entry;
}

/*
 * Compare list distances
 */
static int
CompareLists(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (GetScanListConst(a)->distance > GetScanListConst(b)->distance)
		return 1;

	if (GetScanListConst(a)->distance < GetScanListConst(b)->distance)
		return -1;

	return 0;
}

/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
	int			listCount = 0;
	double		maxDistance = DBL_MAX;

	/* Search all list pages */
	while (BlockNumberIsValid(nextblkno))
	{
		Buffer		cbuf;
		Page		cpage;
		OffsetNumber maxoffno;

		cbuf = ReadBuffer(scan->indexRelation, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);

		for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
		{
			IvfflatList list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
			double		distance;

			/* Use procinfo from the index instead of scan key for performance */
			distance = DatumGetFloat8(so->distfunc(so->procinfo, so->collation, PointerGetDatum(&list->center), value));

			if (listCount < so->maxProbes)
			{
				IvfflatScanList *scanlist;

				scanlist = &so->lists[listCount];
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				listCount++;

				/* Add to heap */
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Calculate max distance */
				if (listCount == so->maxProbes)
					maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
			}
			else if (distance < maxDistance)
			{
				IvfflatScanList *scanlist;

				/* Remove */
				scanlist = GetScanList(pairingheap_remove_first(so->listQueue));

				/* Reuse */
				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				pairingheap_add(so->listQueue, &scanlist->ph_node);

				/* Update max distance */
				maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
			}
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

	for (int i = listCount - 1; i >= 0; i--)
	{
		IvfflatScanList *top = GetScanList(pairingheap_remove_first(so->listQueue));

		so->listPages[i] = top->startPage;
		so->listDistances[i] = top->distance;
	}

	Assert(pairingheap_is_empty(so->listQueue));
}

/*
 * Get items
 */
static void
GetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	TupleTableSlot *slot = so->vslot;
	int			batchProbes = 0;
	double		bestDist = DBL_MAX;
	int			candidateCount = 0;

	tuplesort_reset(so->sortstate);

	/* Search closest probes lists */
	while (so->listIndex < so->maxProbes && (++batchProbes) <= so->probes)
	{
		BlockNumber searchPage = so->listPages[so->listIndex++];

		/* Search all entry pages for list */
		while (BlockNumberIsValid(searchPage))
		{
			Buffer		buf;
			Page		page;
			OffsetNumber maxoffno;

			buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->bas);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			maxoffno = PageGetMaxOffsetNumber(page);

			for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
			{
				IndexTuple	itup;
				Datum		datum;
				bool		isnull;
				ItemId		itemid = PageGetItemId(page, offno);
				double		itemDist;

				itup = (IndexTuple) PageGetItem(page, itemid);
				datum = index_getattr(itup, 1, tupdesc, &isnull);

				/*
				 * Add virtual tuple
				 *
				 * Use procinfo from the index instead of scan key for
				 * performance
				 */
				ExecClearTuple(slot);
				slot->tts_values[0] = so->distfunc(so->procinfo, so->collation, datum, value);
				slot->tts_isnull[0] = false;
				slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
				slot->tts_isnull[1] = false;
				ExecStoreVirtualTuple(slot);

				itemDist = DatumGetFloat8(slot->tts_values[0]);
				if (itemDist < bestDist)
					bestDist = itemDist;
				candidateCount++;

				tuplesort_puttupleslot(so->sortstate, slot);
			}

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}

		/*
		 * Adaptive dynamic early stopping:
		 * If enabled and sufficient probes/candidates checked, prune remaining distant clusters.
		 */
		if (ivfflat_adaptive_scan && batchProbes >= ivfflat_min_probes && candidateCount >= 10 && bestDist < DBL_MAX)
		{
			if (so->listIndex < so->maxProbes)
			{
				double nextCentroidDist = so->listDistances[so->listIndex];

				if (nextCentroidDist > bestDist * ivfflat_target_recall * ivfflat_adaptive_threshold)
					break;
			}
		}
	}

	tuplesort_performsort(so->sortstate);

#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}

/*
 * Zero distance
 */
static Datum
ZeroDistance(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
{
	return Float8GetDatum(0.0);
}

/*
 * Get scan value
 */
static Datum
GetScanValue(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	Datum		value;

	if (scan->orderByData->sk_flags & SK_ISNULL)
	{
		value = PointerGetDatum(NULL);
		so->distfunc = ZeroDistance;
	}
	else
	{
		value = scan->orderByData->sk_argument;
		so->distfunc = FunctionCall2Coll;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Normalize if needed */
		if (so->normprocinfo != NULL)
		{
			MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

			value = IvfflatNormValue(so->typeInfo, so->collation, value);

			MemoryContextSwitchTo(oldCtx);
		}
	}

	return value;
}

/*
 * Initialize scan sort state
 */
static Tuplesortstate *
InitScanSortState(TupleDesc tupdesc)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	return tuplesort_begin_heap(tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
ivfflatbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	IvfflatScanOpaque so;
	int			lists;
	int			dimensions;
	int			probes = ivfflat_probes;
	int			maxProbes;
	MemoryContext oldCtx;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	/* Get lists and dimensions from metapage */
	IvfflatGetMetaPageInfo(index, &lists, &dimensions);

	if (ivfflat_iterative_scan != IVFFLAT_ITERATIVE_SCAN_OFF)
		maxProbes = Max(ivfflat_max_probes, probes);
	else
		maxProbes = probes;

	if (probes > lists)
		probes = lists;

	if (maxProbes > lists)
		maxProbes = lists;

	so = palloc_object(IvfflatScanOpaqueData);
	so->typeInfo = IvfflatGetTypeInfo(index);
	so->first = true;
	so->probes = probes;
	so->maxProbes = maxProbes;
	so->dimensions = dimensions;
	so->value = PointerGetDatum(NULL);

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Ivfflat scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/* Create tuple description for sorting */
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(so->tupdesc);
#endif

	/* Prep sort */
	so->sortstate = InitScanSortState(so->tupdesc);

	/* Need separate slots for puttuple and gettuple */
	so->vslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->mslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);

	/*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	so->bas = GetAccessStrategy(BAS_BULKREAD);

	so->listQueue = pairingheap_allocate(CompareLists, scan);
	so->listPages = palloc_array_checked(BlockNumber, (Size) maxProbes);
	so->listDistances = palloc_array_checked(double, (Size) maxProbes);
	so->listIndex = 0;
	so->lists = palloc_array_checked(IvfflatScanList, (Size) maxProbes);

	so->fromCache = false;
	so->vectorHash = 0;
	so->cacheItemCount = 0;

	MemoryContextSwitchTo(oldCtx);

	scan->opaque = so;

	return scan;
}

/*
 * Start or restart an index scan
 */
void
ivfflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	so->first = true;
	so->fromCache = false;
	so->vectorHash = 0;
	so->cacheItemCount = 0;
	pairingheap_reset(so->listQueue);
	so->listIndex = 0;

	if (so->normprocinfo != NULL && DatumGetPointer(so->value) != NULL)
	{
		pfree(DatumGetPointer(so->value));
		so->value = PointerGetDatum(NULL);
	}

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, (Size) scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, (Size) scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
bool
ivfflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	ItemPointer heaptid;
	bool		isnull;

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->first)
	{
		Datum		value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);
#if PG_VERSION_NUM >= 180000
		if (scan->instrument)
			scan->instrument->nsearches++;
#endif

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan ivfflat index without order");

		/* Requires MVCC-compliant snapshot as not able to pin during sorting */
		/* https://www.postgresql.org/docs/current/index-locking.html */
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with ivfflat");

		value = GetScanValue(scan);

		/* Check query result LRU cache (L1 Local + L2 Global Shared Memory) */
		if ((ivfflat_query_cache || ivfflat_global_cache) && value != PointerGetDatum(NULL))
		{
			so->vectorHash = IvfflatVectorHash64(value);

			/* Tier 1: Process-local L1 cache lookup */
			if (ivfflat_query_cache)
			{
				IvfflatCacheEntry *entry = IvfflatCacheLookup(scan->indexRelation->rd_id, so->vectorHash, so->probes);
				if (entry != NULL)
				{
					for (int i = 0; i < entry->numItems; i++)
					{
						ExecClearTuple(so->vslot);
						so->vslot->tts_values[0] = Float8GetDatum(entry->items[i].distance);
						so->vslot->tts_isnull[0] = false;
						so->vslot->tts_values[1] = PointerGetDatum(&entry->items[i].heaptid);
						so->vslot->tts_isnull[1] = false;
						ExecStoreVirtualTuple(so->vslot);
						tuplesort_puttupleslot(so->sortstate, so->vslot);
					}
					tuplesort_performsort(so->sortstate);
					so->first = false;
					so->value = value;
					so->fromCache = true;
					goto fetch_tuple_entry;
				}
			}

			/* Tier 2: Cross-process global shared memory subtree cache lookup */
			if (ivfflat_global_cache)
			{
				ItemPointerData globalTids[GLOBAL_SUBTREE_MAX_ITEMS];
				double globalDistances[GLOBAL_SUBTREE_MAX_ITEMS];
				int globalNumItems = 0;

				if (IvfflatGlobalCacheLookup(scan->indexRelation->rd_id, so->vectorHash, so->probes,
											globalTids, globalDistances, &globalNumItems))
				{
					for (int i = 0; i < globalNumItems; i++)
					{
						ExecClearTuple(so->vslot);
						so->vslot->tts_values[0] = Float8GetDatum(globalDistances[i]);
						so->vslot->tts_isnull[0] = false;
						so->vslot->tts_values[1] = PointerGetDatum(&globalTids[i]);
						so->vslot->tts_isnull[1] = false;
						ExecStoreVirtualTuple(so->vslot);
						tuplesort_puttupleslot(so->sortstate, so->vslot);
					}
					tuplesort_performsort(so->sortstate);

					/* Backfill L1 local cache for future queries in this session */
					if (ivfflat_query_cache)
						IvfflatCacheInsert(scan->indexRelation->rd_id, so->vectorHash, so->probes,
										   globalTids, globalDistances, globalNumItems);

					so->first = false;
					so->value = value;
					so->fromCache = true;
					goto fetch_tuple_entry;
				}
			}
		}

		IvfflatBench("GetScanLists", GetScanLists(scan, value));
		IvfflatBench("GetScanItems", GetScanItems(scan, value));
		so->first = false;
		so->value = value;
	}

fetch_tuple_entry:
	while (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
	{
		if (so->listIndex == so->maxProbes)
			return false;

		IvfflatBench("GetScanItems", GetScanItems(scan, so->value));
	}

	heaptid = (ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull));

	/* Collect top candidates for LRU cache insertion */
	if (!so->fromCache && (ivfflat_query_cache || ivfflat_global_cache) && so->cacheItemCount < IVFFLAT_CACHE_MAX_ITEMS)
	{
		double dist = DatumGetFloat8(slot_getattr(so->mslot, 1, &isnull));
		so->cacheTids[so->cacheItemCount] = *heaptid;
		so->cacheDistances[so->cacheItemCount] = dist;
		so->cacheItemCount++;
	}

	scan->xs_heaptid = *heaptid;
	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;
	return true;
}

/*
 * End a scan and release resources
 */
void
ivfflatendscan(IndexScanDesc scan)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;

	/* Save candidate items to LRU cache upon query completion */
	if (!so->fromCache && so->cacheItemCount > 0 && so->vectorHash != 0)
	{
		if (ivfflat_query_cache)
			IvfflatCacheInsert(scan->indexRelation->rd_id, so->vectorHash, so->probes, so->cacheTids, so->cacheDistances, so->cacheItemCount);

		if (ivfflat_global_cache)
			IvfflatGlobalCacheInsert(scan->indexRelation->rd_id, so->vectorHash, so->probes, so->cacheTids, so->cacheDistances, so->cacheItemCount);
	}

	/* Free any temporary files */
	tuplesort_end(so->sortstate);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
