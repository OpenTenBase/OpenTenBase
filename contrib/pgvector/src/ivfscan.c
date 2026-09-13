#include "postgres.h"

#include <float.h>

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "halfutils.h"
#include "halfvec.h"
#include "lib/pairingheap.h"
#include "ivfflat.h"
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
 * Compute distance with a type-aware fast path when available
 */
static inline double
ComputeScanDistance(IvfflatScanOpaque so, Datum a, Datum b)
{
	if (DatumGetPointer(b) == NULL)
		return 0.0;

	switch (so->distanceKind)
	{
		case IVFFLAT_DISTANCE_VECTOR_L2:
		{
			Vector	   *va = DatumGetVector(a);
			Vector	   *vb = DatumGetVector(b);

			return (double) VectorL2SquaredDistance(so->dimensions, va->x, vb->x);
		}
		case IVFFLAT_DISTANCE_VECTOR_IP:
		{
			Vector	   *va = DatumGetVector(a);
			Vector	   *vb = DatumGetVector(b);

			return -(double) VectorInnerProduct(so->dimensions, va->x, vb->x);
		}
		case IVFFLAT_DISTANCE_HALFVEC_L2:
		{
			HalfVector *ha = DatumGetHalfVector(a);
			HalfVector *hb = DatumGetHalfVector(b);

			return (double) HalfvecL2SquaredDistance(so->dimensions, ha->x, hb->x);
		}
		case IVFFLAT_DISTANCE_HALFVEC_IP:
		{
			HalfVector *ha = DatumGetHalfVector(a);
			HalfVector *hb = DatumGetHalfVector(b);

			return -(double) HalfvecInnerProduct(so->dimensions, ha->x, hb->x);
		}
		case IVFFLAT_DISTANCE_FMGR:
			break;
	}

	return DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, a, b));
}

/* Add a list to the bounded pairing heap. */
static void
AddScanList(IvfflatScanOpaque so, int *listCount, BlockNumber startPage,
				double distance, double *maxDistance)
{
	if (*listCount < so->maxProbes)
	{
		IvfflatScanList *scanlist;

		scanlist = &so->lists[(*listCount)++];
		scanlist->startPage = startPage;
		scanlist->distance = distance;
		pairingheap_add(so->listQueue, &scanlist->ph_node);

		if (*listCount == so->maxProbes)
			*maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
	}
	else if (distance < *maxDistance)
	{
		IvfflatScanList *scanlist;

		scanlist = GetScanList(pairingheap_remove_first(so->listQueue));
		scanlist->startPage = startPage;
		scanlist->distance = distance;
		pairingheap_add(so->listQueue, &scanlist->ph_node);
		*maxDistance = GetScanList(pairingheap_first(so->listQueue))->distance;
	}
}

/*
 * Check dimensions once before using a direct distance kernel
 */
static void
CheckScanValueDimensions(IvfflatScanOpaque so, Datum value)
{
	switch (so->distanceKind)
	{
		case IVFFLAT_DISTANCE_VECTOR_L2:
		case IVFFLAT_DISTANCE_VECTOR_IP:
		{
			Vector	   *vector = DatumGetVector(value);

			if (vector->dim != so->dimensions)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("different vector dimensions %d and %d", so->dimensions, vector->dim)));
			break;
		}
		case IVFFLAT_DISTANCE_HALFVEC_L2:
		case IVFFLAT_DISTANCE_HALFVEC_IP:
		{
			HalfVector *halfvec = DatumGetHalfVector(value);

			if (halfvec->dim != so->dimensions)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("different halfvec dimensions %d and %d", so->dimensions, halfvec->dim)));
			break;
		}
		case IVFFLAT_DISTANCE_FMGR:
			break;
	}
}

/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	IvfflatListCache cache;
	BlockNumber nextblkno;
	int			listCount = 0;
	double		maxDistance = DBL_MAX;

	cache = IvfflatGetListCache(scan->indexRelation, so->typeInfo);
	if (cache != NULL)
	{
		for (int i = 0; i < cache->lists; i++)
		{
			double distance;

			distance = ComputeScanDistance(so,
										   PointerGetDatum(IvfflatListCacheGetCenter(cache, i)),
										   value);
			AddScanList(so, &listCount,
						 IvfflatListCacheGetEntry(cache, i)->startPage,
						 distance, &maxDistance);
		}

		goto finish_list_selection;
	}

	nextblkno = IVFFLAT_HEAD_BLKNO;

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
			distance = ComputeScanDistance(so, PointerGetDatum(&list->center), value);

			AddScanList(so, &listCount, list->startPage, distance, &maxDistance);
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

finish_list_selection:
	for (int i = listCount - 1; i >= 0; i--)
		so->listPages[i] = GetScanList(pairingheap_remove_first(so->listQueue))->startPage;

	Assert(pairingheap_is_empty(so->listQueue));
	if (cache != NULL && !cache->cacheable)
		pfree(cache);
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
	int			listsInBatch = 0;
	int			batchListLimit = so->batchListTarget;

	Assert(batchListLimit > 0);

	tuplesort_reset(so->sortstate);

	/* Search the next batch of closest lists. */
	while (so->listIndex < so->maxProbes && listsInBatch < batchListLimit)
	{
		BlockNumber searchPage = so->listPages[so->listIndex++];

		listsInBatch++;

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

				itup = (IndexTuple) PageGetItem(page, itemid);
				datum = index_getattr(itup, 1, tupdesc, &isnull);

				/*
				 * Add virtual tuple
				 *
				 * Use procinfo from the index instead of scan key for
				 * performance
				 */
				ExecClearTuple(slot);
				slot->tts_values[0] = Float8GetDatum(ComputeScanDistance(so, datum, value));
				slot->tts_isnull[0] = false;
				slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
				slot->tts_isnull[1] = false;
				ExecStoreVirtualTuple(slot);

				tuplesort_puttupleslot(so->sortstate, slot);
			}

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
	}

	/*
	 * Grow only subsequent relaxed-order batches. Reaching this point does not
	 * mean more lists will be scanned: the larger value is used only if the
	 * executor consumes this entire batch and asks for more tuples.
	 */
	if (ivfflat_iterative_scan == IVFFLAT_ITERATIVE_SCAN_RELAXED &&
		so->listIndex < so->maxProbes)
	{
		int			remaining = so->maxProbes - so->listIndex;
		int			grown = so->batchListTarget * 2;
		int			growthLimit = Min(so->maxProbes,
								  so->probes * IVFFLAT_BATCH_GROWTH_LIMIT);

		so->batchListTarget = Min(Min(grown, growthLimit), remaining);
	}

	tuplesort_performsort(so->sortstate);

#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
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
		value = PointerGetDatum(NULL);
	else
	{
		value = scan->orderByData->sk_argument;

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

		CheckScanValueDimensions(so, value);
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
	so->batchListTarget = probes;
	so->dimensions = dimensions;
	so->value = PointerGetDatum(NULL);

	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, IVFFLAT_DISTANCE_PROC);
	so->normprocinfo = IvfflatOptionalProcInfo(index, IVFFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

	/* Select a type-aware distance fast path */
	so->distanceKind = IVFFLAT_DISTANCE_FMGR;
	if (so->procinfo->fn_addr == vector_l2_squared_distance)
		so->distanceKind = IVFFLAT_DISTANCE_VECTOR_L2;
	else if (so->procinfo->fn_addr == vector_negative_inner_product)
		so->distanceKind = IVFFLAT_DISTANCE_VECTOR_IP;
	else if (so->procinfo->fn_addr == halfvec_l2_squared_distance)
		so->distanceKind = IVFFLAT_DISTANCE_HALFVEC_L2;
	else if (so->procinfo->fn_addr == halfvec_negative_inner_product)
		so->distanceKind = IVFFLAT_DISTANCE_HALFVEC_IP;

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
	so->listPages = palloc_array_checked(BlockNumber, maxProbes);
	so->listIndex = 0;
	so->lists = palloc_array_checked(IvfflatScanList, maxProbes);

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
	pairingheap_reset(so->listQueue);
	so->listIndex = 0;
	so->batchListTarget = so->probes;

	if (so->normprocinfo != NULL && DatumGetPointer(so->value) != NULL)
	{
		pfree(DatumGetPointer(so->value));
		so->value = PointerGetDatum(NULL);
	}

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
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
		IvfflatBench("GetScanLists", GetScanLists(scan, value));
		IvfflatBench("GetScanItems", GetScanItems(scan, value));
		so->first = false;
		so->value = value;
	}

	while (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
	{
		if (so->listIndex == so->maxProbes)
			return false;

		IvfflatBench("GetScanItems", GetScanItems(scan, so->value));
	}

	heaptid = (ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull));

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

	/* Free any temporary files */
	tuplesort_end(so->sortstate);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}
