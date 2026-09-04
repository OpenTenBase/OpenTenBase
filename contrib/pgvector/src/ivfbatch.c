#include "postgres.h"

#include <float.h>

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/tupdesc.h"
#include "catalog/pg_type_d.h"
#include "funcapi.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

/*
 * Structure for tracking Top-K candidates per query in batch
 */
typedef struct BatchItem
{
	ItemPointerData heaptid;
	double		distance;
} BatchItem;

typedef struct BatchQueryTopK
{
	int			k;
	int			count;
	BatchItem  *items;		/* 1-indexed heap of size k + 1 */
} BatchQueryTopK;

static void
BatchTopKInit(BatchQueryTopK *topk, int k)
{
	topk->k = k;
	topk->count = 0;
	topk->items = palloc_array_checked(BatchItem, (Size) (k + 1));
}

static void
BatchTopKInsert(BatchQueryTopK *topk, ItemPointer heaptid, double dist)
{
	int			i;
	BatchItem  *items = topk->items;

	if (topk->count < topk->k)
	{
		/* Insert into max-heap */
		topk->count++;
		i = topk->count;
		while (i > 1 && items[i / 2].distance < dist)
		{
			items[i] = items[i / 2];
			i /= 2;
		}
		items[i].heaptid = *heaptid;
		items[i].distance = dist;
	}
	else if (dist < items[1].distance)
	{
		/* Replace max root and sift down */
		int			child;

		i = 1;
		while ((child = 2 * i) <= topk->count)
		{
			if (child < topk->count && items[child + 1].distance > items[child].distance)
				child++;

			if (dist >= items[child].distance)
				break;

			items[i] = items[child];
			i = child;
		}
		items[i].heaptid = *heaptid;
		items[i].distance = dist;
	}
}

/* Sort top-k items in ascending distance order for output */
static void
BatchTopKSort(BatchQueryTopK *topk)
{
	int			n = topk->count;

	for (int i = 1; i <= n; i++)
	{
		for (int j = i + 1; j <= n; j++)
		{
			if (topk->items[j].distance < topk->items[i].distance)
			{
				BatchItem	tmp = topk->items[i];

				topk->items[i] = topk->items[j];
				topk->items[j] = tmp;
			}
		}
	}
}

typedef struct CentroidCandidate
{
	BlockNumber startPage;
	double		distance;
} CentroidCandidate;

/*
 * Batch Vector KNN Search:
 * Evaluates multiple query vectors simultaneously, reusing centroid pages and shared inverted list buffers.
 */
PG_FUNCTION_INFO_V1(ivfflat_batch_knn);
Datum
ivfflat_batch_knn(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcontext;

	Oid			indexOid = PG_GETARG_OID(0);
	ArrayType  *queriesArray = PG_GETARG_ARRAYTYPE_P(1);
	int32		k = PG_GETARG_INT32(2);
	int32		probes = (PG_NARGS() > 3 && !PG_ARGISNULL(3)) ? PG_GETARG_INT32(3) : ivfflat_probes;

	Relation	indexRel;
	int			lists;
	int			dimensions;
	FmgrInfo   *distprocinfo;
	Oid			collation;
	BufferAccessStrategy bas;

	Datum	   *queryDatums;
	bool	   *queryNulls;
	int			numQueries;
	Vector	  **qvecs;

	BatchQueryTopK *topk_per_query;
	CentroidCandidate **query_candidates;

	/* Deduplication map for unique inverted list buckets */
	BlockNumber *uniquePages;
	int			numUniquePages = 0;
	int		  **pageSubscribers;
	int		   *numSubscribers;

	if (k <= 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("k must be greater than 0")));

	if (probes <= 0)
		probes = 1;

	/* Check return set info */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required")));

	/* Deconstruct queries array */
	deconstruct_array(queriesArray, InvalidOid, -1, false, TYPALIGN_DOUBLE, &queryDatums, &queryNulls, &numQueries);

	if (numQueries <= 0)
	{
		rsinfo->returnMode = SFRM_Materialize;
		PG_RETURN_NULL();
	}

	indexRel = index_open(indexOid, AccessShareLock);
	IvfflatGetMetaPageInfo(indexRel, &lists, &dimensions);

	if (probes > lists)
		probes = lists;

	distprocinfo = index_getprocinfo(indexRel, 1, IVFFLAT_DISTANCE_PROC);
	collation = indexRel->rd_indcollation[0];
	bas = GetAccessStrategy(BAS_BULKREAD);

	qvecs = palloc_array_checked(Vector *, numQueries);
	topk_per_query = palloc_array_checked(BatchQueryTopK, numQueries);
	query_candidates = palloc_array_checked(CentroidCandidate *, numQueries);

	for (int q = 0; q < numQueries; q++)
	{
		if (queryNulls[q])
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("query vector cannot be null")));

		qvecs[q] = DatumGetVector(queryDatums[q]);
		if (qvecs[q]->dim != dimensions)
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("vector dimension mismatch: expected %d, got %d", dimensions, qvecs[q]->dim)));

		BatchTopKInit(&topk_per_query[q], k);
		query_candidates[q] = palloc_array_checked(CentroidCandidate, lists);
	}

	/*
	 * Phase 1: Shared Centroid Distance Calculation
	 * Reads all centroid pages ONCE for all queries in batch.
	 */
	{
		BlockNumber nextblkno = IVFFLAT_HEAD_BLKNO;
		int			cIdx = 0;

		while (BlockNumberIsValid(nextblkno))
		{
			Buffer		cbuf;
			Page		cpage;
			OffsetNumber maxoffno;

			cbuf = ReadBuffer(indexRel, nextblkno);
			LockBuffer(cbuf, BUFFER_LOCK_SHARE);
			cpage = BufferGetPage(cbuf);
			maxoffno = PageGetMaxOffsetNumber(cpage);

			for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
			{
				IvfflatList list = (IvfflatList) PageGetItem(cpage, PageGetItemId(cpage, offno));
				Datum		centerDatum = PointerGetDatum(&list->center);

				/* Compute distance against all queries in batch */
				for (int q = 0; q < numQueries; q++)
				{
					double		d = DatumGetFloat8(FunctionCall2Coll(distprocinfo, collation, centerDatum, PointerGetDatum(qvecs[q])));

					query_candidates[q][cIdx].startPage = list->startPage;
					query_candidates[q][cIdx].distance = d;
				}
				cIdx++;
			}

			nextblkno = IvfflatPageGetOpaque(cpage)->nextblkno;
			UnlockReleaseBuffer(cbuf);
		}

		/* Sort centroid candidates for each query to pick top-probes */
		for (int q = 0; q < numQueries; q++)
		{
			for (int i = 0; i < probes; i++)
			{
				int			minIdx = i;

				for (int j = i + 1; j < cIdx; j++)
				{
					if (query_candidates[q][j].distance < query_candidates[q][minIdx].distance)
						minIdx = j;
				}
				if (minIdx != i)
				{
					CentroidCandidate tmp = query_candidates[q][i];

					query_candidates[q][i] = query_candidates[q][minIdx];
					query_candidates[q][minIdx] = tmp;
				}
			}
		}
	}

	/*
	 * Phase 2: Inverted List Bucket Deduplication & Grouping
	 */
	{
		int			maxUnique = numQueries * probes;

		uniquePages = palloc_array_checked(BlockNumber, maxUnique);
		pageSubscribers = palloc_array_checked(int *, maxUnique);
		numSubscribers = palloc_array_checked(int, maxUnique);

		for (int q = 0; q < numQueries; q++)
		{
			for (int p = 0; p < probes; p++)
			{
				BlockNumber sp = query_candidates[q][p].startPage;
				int			foundIdx = -1;

				for (int u = 0; u < numUniquePages; u++)
				{
					if (uniquePages[u] == sp)
					{
						foundIdx = u;
						break;
					}
				}

				if (foundIdx >= 0)
				{
					pageSubscribers[foundIdx][numSubscribers[foundIdx]++] = q;
				}
				else
				{
					uniquePages[numUniquePages] = sp;
					pageSubscribers[numUniquePages] = palloc_array_checked(int, numQueries);
					pageSubscribers[numUniquePages][0] = q;
					numSubscribers[numUniquePages] = 1;
					numUniquePages++;
				}
			}
		}
	}

	/*
	 * Phase 3: Single-Pass Shared Inverted List Buffer Scan
	 * Each bucket page is read and pinned EXACTLY ONCE for all subscribed queries.
	 */
	{
		TupleDesc	indextupdesc = RelationGetDescr(indexRel);

		for (int u = 0; u < numUniquePages; u++)
		{
			BlockNumber searchPage = uniquePages[u];
			int			subCount = numSubscribers[u];
			int		   *subs = pageSubscribers[u];

			while (BlockNumberIsValid(searchPage))
			{
				Buffer		buf;
				Page		page;
				OffsetNumber maxoffno;

				buf = ReadBufferExtended(indexRel, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);
				maxoffno = PageGetMaxOffsetNumber(page);

				for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
				{
					IndexTuple	itup;
					Datum		itemDatum;
					bool		isnull;
					ItemId		itemid = PageGetItemId(page, offno);

					itup = (IndexTuple) PageGetItem(page, itemid);
					itemDatum = index_getattr(itup, 1, indextupdesc, &isnull);

					/* For each query subscribed to this bucket, evaluate distance */
					for (int s = 0; s < subCount; s++)
					{
						int			q = subs[s];
						double		dist = DatumGetFloat8(FunctionCall2Coll(distprocinfo, collation, itemDatum, PointerGetDatum(qvecs[q])));

						BatchTopKInsert(&topk_per_query[q], &itup->t_tid, dist);
					}
				}

				searchPage = IvfflatPageGetOpaque(page)->nextblkno;
				UnlockReleaseBuffer(buf);
			}
		}
	}

	index_close(indexRel, AccessShareLock);

	/*
	 * Phase 4: Construct Return Tuplestore
	 */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "query_id", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "distance", FLOAT8OID, -1, 0);
#if PG_VERSION_NUM >= 190000
	TupleDescFinalize(tupdesc);
#endif

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (int q = 0; q < numQueries; q++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};

		BatchTopKSort(&topk_per_query[q]);

		for (int i = 1; i <= topk_per_query[q].count; i++)
		{
			values[0] = Int32GetDatum(q);
			values[1] = PointerGetDatum(&topk_per_query[q].items[i].heaptid);
			values[2] = Float8GetDatum(topk_per_query[q].items[i].distance);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	PG_RETURN_NULL();
}
