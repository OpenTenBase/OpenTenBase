#include "postgres.h"

#include <limits.h>

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/float.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

/*
 * Compare list distances
 */
static int
CompareLists(const IvfflatScanList *a, const IvfflatScanList *b)
{
	int			cmp = float8_cmp_internal(a->distance, b->distance);

	if (cmp != 0)
		return cmp;
	if (a->startPage < b->startPage)
		return -1;
	if (a->startPage > b->startPage)
		return 1;

	return 0;
}

/*
 * Restore the max-heap property for list selection
 */
static void
SiftDownLists(IvfflatScanList *heap, int count, int root)
{
	for (;;)
	{
		int			largest = root;
		int			left = root * 2 + 1;
		int			right = left + 1;

		if (left < count && CompareLists(&heap[left], &heap[largest]) > 0)
			largest = left;
		if (right < count && CompareLists(&heap[right], &heap[largest]) > 0)
			largest = right;
		if (largest == root)
			break;

		{
			IvfflatScanList tmp = heap[root];

			heap[root] = heap[largest];
			heap[largest] = tmp;
		}
		root = largest;
	}
}

/*
 * Build a max-heap for the closest lists seen so far
 */
static void
BuildListHeap(IvfflatScanList *heap, int count)
{
	for (int i = count / 2 - 1; i >= 0; i--)
		SiftDownLists(heap, count, i);
}

/*
 * Calculate distance, bypassing fmgr for built-in vector support functions
 */
static inline double
GetDistance(IvfflatScanOpaque so, Datum a, Datum b)
{
	if (DatumGetPointer(b) == NULL)
		return 0.0;

	if (so->vectorDistance != NULL)
	{
		Vector	   *va = DatumGetVector(a);
		Vector	   *vb = DatumGetVector(b);
		float		distance = so->vectorDistance(so->dimensions, va->x, vb->x);

		return so->negateVectorDistance ? -(double) distance : (double) distance;
	}

	return DatumGetFloat8(FunctionCall2Coll(so->procinfo, so->collation, a, b));
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
			distance = GetDistance(so, PointerGetDatum(&list->center), value);

			if (listCount < so->maxProbes)
			{
				IvfflatScanList *scanlist = &so->listHeap[listCount];

				scanlist->startPage = list->startPage;
				scanlist->distance = distance;
				listCount++;

				if (listCount == so->maxProbes)
					BuildListHeap(so->listHeap, listCount);
			}
			else
			{
				IvfflatScanList scanlist;

				scanlist.startPage = list->startPage;
				scanlist.distance = distance;

				if (CompareLists(&scanlist, &so->listHeap[0]) < 0)
				{
					so->listHeap[0] = scanlist;
					SiftDownLists(so->listHeap, listCount, 0);
				}
			}
		}

		nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;

		UnlockReleaseBuffer(cbuf);
	}

	Assert(listCount == so->maxProbes);
	for (int i = listCount - 1; i >= 0; i--)
	{
		so->listPages[i] = so->listHeap[0].startPage;
		if (i > 0)
		{
			so->listHeap[0] = so->listHeap[i];
			SiftDownLists(so->listHeap, i, 0);
		}
	}
}

/*
 * Compare candidates by distance and then TID for deterministic ties
 */
static int
CompareCandidates(const IvfflatScanCandidate *a, const IvfflatScanCandidate *b)
{
	int			cmp = float8_cmp_internal(a->distance, b->distance);

	if (cmp != 0)
		return cmp;
	return ItemPointerCompare(&a->heaptid, &b->heaptid);
}

/*
 * Restore the min-heap property for candidate output
 */
static void
SiftDownCandidates(IvfflatScanCandidate *heap, int count, int root)
{
	for (;;)
	{
		int			smallest = root;
		int			left = root * 2 + 1;
		int			right = left + 1;

		if (left < count && CompareCandidates(&heap[left], &heap[smallest]) < 0)
			smallest = left;
		if (right < count && CompareCandidates(&heap[right], &heap[smallest]) < 0)
			smallest = right;
		if (smallest == root)
			break;

		{
			IvfflatScanCandidate tmp = heap[root];

			heap[root] = heap[smallest];
			heap[smallest] = tmp;
		}
		root = smallest;
	}
}

/*
 * Build a min-heap in linear time
 */
static void
BuildCandidateHeap(IvfflatScanOpaque so)
{
	for (int i = so->candidateCount / 2 - 1; i >= 0; i--)
		SiftDownCandidates(so->candidates, so->candidateCount, i);
}

/*
 * Add a candidate to tuplesort
 */
static void
PutSortCandidate(IvfflatScanOpaque so, const IvfflatScanCandidate *candidate)
{
	TupleTableSlot *slot = so->vslot;

	ExecClearTuple(slot);
	slot->tts_values[0] = Float8GetDatum(candidate->distance);
	slot->tts_isnull[0] = false;
	slot->tts_values[1] = PointerGetDatum(&candidate->heaptid);
	slot->tts_isnull[1] = false;
	ExecStoreVirtualTuple(slot);
	tuplesort_puttupleslot(so->sortstate, slot);
}

/*
 * Switch to tuplesort when the bounded candidate array is full
 */
static void
EnableSortFallback(IvfflatScanOpaque so)
{
	for (int i = 0; i < so->candidateCount; i++)
		PutSortCandidate(so, &so->candidates[i]);

	so->candidateCount = 0;
	so->sortFallback = true;
}

/*
 * Add a candidate to the in-memory array or tuplesort fallback
 */
static void
AddCandidate(IvfflatScanOpaque so, double distance, ItemPointer heaptid)
{
	IvfflatScanCandidate candidate;

	candidate.distance = distance;
	candidate.heaptid = *heaptid;

	if (!so->sortFallback && so->candidateCount == so->maxCandidates)
		EnableSortFallback(so);

	if (so->sortFallback)
	{
		PutSortCandidate(so, &candidate);
		return;
	}

	if (so->candidateCount == so->candidateCapacity)
	{
		int			newCapacity = Min(so->maxCandidates, so->candidateCapacity * 2);

		so->candidates = repalloc_array(so->candidates, IvfflatScanCandidate, newCapacity);
		so->candidateCapacity = newCapacity;
	}

	so->candidates[so->candidateCount++] = candidate;
}

/*
 * Get items from the next batch of lists
 */
static void
GetScanItems(IndexScanDesc scan, Datum value)
{
	IvfflatScanOpaque so = (IvfflatScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	int			batchProbes = 0;

	so->candidateCount = 0;
	so->sortFallback = false;
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

				itup = (IndexTuple) PageGetItem(page, itemid);
				datum = index_getattr(itup, 1, tupdesc, &isnull);
				Assert(!isnull);

				AddCandidate(so, GetDistance(so, datum, value), &itup->t_tid);
			}

			searchPage = IvfflatPageGetOpaque(page)->nextblkno;

			UnlockReleaseBuffer(buf);
		}
	}

	if (so->sortFallback)
		IvfflatBench("tuplesort_performsort", tuplesort_performsort(so->sortstate));
	else
		IvfflatBench("BuildCandidateHeap", BuildCandidateHeap(so));

#if defined(IVFFLAT_MEMORY)
	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
#endif
}

/*
 * Get the next candidate from the active output path
 */
static bool
GetNextCandidate(IvfflatScanOpaque so, ItemPointer heaptid)
{
	if (so->sortFallback)
	{
		bool		isnull;
		ItemPointer sortedTid;

		if (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
			return false;

		sortedTid = (ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull));
		Assert(!isnull);
		*heaptid = *sortedTid;
		return true;
	}

	if (so->candidateCount == 0)
		return false;

	*heaptid = so->candidates[0].heaptid;
	so->candidateCount--;
	if (so->candidateCount > 0)
	{
		so->candidates[0] = so->candidates[so->candidateCount];
		SiftDownCandidates(so->candidates, so->candidateCount, 0);
	}

	return true;
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

		if (so->vectorDistance != NULL)
		{
			Vector	   *vector = DatumGetVector(value);

			if (vector->dim != so->dimensions)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("different vector dimensions %d and %d", so->dimensions, vector->dim)));
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
	AttrNumber	attNums[] = {1, 2};
	Oid			sortOperators[] = {Float8LessOperator, TIDLessOperator};
	Oid			sortCollations[] = {InvalidOid, InvalidOid};
	bool		nullsFirstFlags[] = {false, false};

	return tuplesort_begin_heap(tupdesc, 2, attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);
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
	Size		candidateBudget;
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
	so->vectorDistance = NULL;
	so->negateVectorDistance = false;
	if (so->procinfo->fn_addr == vector_l2_squared_distance)
		so->vectorDistance = VectorL2SquaredDistance;
	else if (so->procinfo->fn_addr == vector_negative_inner_product)
	{
		so->vectorDistance = VectorInnerProduct;
		so->negateVectorDistance = true;
	}

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
	so->sortFallback = false;

	/* Keep the fast path conservative when it later falls back to tuplesort */
	candidateBudget = Min((Size) work_mem * 1024 / 4, (Size) MaxAllocSize);
	so->maxCandidates = (int) Min((Size) INT_MAX,
									Max((Size) 1, candidateBudget / sizeof(IvfflatScanCandidate)));
	so->candidateCapacity = Min(1024, so->maxCandidates);
	so->candidateCount = 0;
	so->candidates = palloc_array_checked(IvfflatScanCandidate, so->candidateCapacity);

	/*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	so->bas = GetAccessStrategy(BAS_BULKREAD);

	so->listPages = palloc_array_checked(BlockNumber, maxProbes);
	so->listIndex = 0;
	so->listHeap = palloc_array_checked(IvfflatScanList, maxProbes);

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
	so->listIndex = 0;
	so->candidateCount = 0;
	so->sortFallback = false;

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
	ItemPointerData heaptid;

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

	while (!GetNextCandidate(so, &heaptid))
	{
		if (so->listIndex == so->maxProbes)
			return false;

		IvfflatBench("GetScanItems", GetScanItems(scan, so->value));
	}

	scan->xs_heaptid = heaptid;
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
