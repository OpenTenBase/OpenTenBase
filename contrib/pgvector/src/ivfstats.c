#include "postgres.h"

#include <math.h>

#include "access/relation.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#define IVFFLAT_LIST_STATS_COLUMNS 13
#define IVFFLAT_INDEX_STATS_COLUMNS 12

typedef enum IvfflatStatsPageKind
{
	IVFFLAT_STATS_PAGE_UNKNOWN = 0,
	IVFFLAT_STATS_PAGE_META,
	IVFFLAT_STATS_PAGE_LIST,
	IVFFLAT_STATS_PAGE_ENTRY
} IvfflatStatsPageKind;

typedef struct IvfflatListStatsData
{
	int			listId;
	BlockNumber listPage;
	OffsetNumber listOffset;
	BlockNumber startPage;
	BlockNumber insertPage;
	int64		entryPages;
	int64		physicalTuples;
	int64		usableBytes;
	int64		usedBytes;
	int64		freeBytes;
} IvfflatListStatsData;

typedef struct IvfflatIndexStatsData
{
	int			formatVersion;
	int			dimensions;
	int			declaredLists;
	int64		listPages;
	int64		entryPages;
	int64		physicalTuples;
	int64		emptyLists;
	int64		usableBytes;
	int64		usedBytes;
	int64		freeBytes;
	int64		scannedPages;
} IvfflatIndexStatsData;

PGDLLEXPORT Datum ivfflat_list_stats(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum ivfflat_index_stats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(ivfflat_list_stats);
PG_FUNCTION_INFO_V1(ivfflat_index_stats);

static void
IvfflatStatsError(Relation index, const char *message, BlockNumber blkno)
{
	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg("invalid ivfflat index \"%s\": %s at block %u",
					RelationGetRelationName(index), message, blkno)));
}

static void
IvfflatStatsCheckBlock(Relation index, BlockNumber blkno, BlockNumber nblocks)
{
	if (!BlockNumberIsValid(blkno) || blkno >= nblocks)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid ivfflat index \"%s\": block %u is out of range",
						RelationGetRelationName(index), blkno)));
}

static void
IvfflatStatsCheckPage(Relation index, Page page, BlockNumber blkno)
{
	PageHeader	header = (PageHeader) page;

	if (PageIsNew(page))
		IvfflatStatsError(index, "page is uninitialized", blkno);
	if (PageGetSpecialSize(page) < MAXALIGN(sizeof(IvfflatPageOpaqueData)))
		IvfflatStatsError(index, "page special area is too small", blkno);
	if (IvfflatPageGetOpaque(page)->page_id != IVFFLAT_PAGE_ID)
		IvfflatStatsError(index, "page identifier does not match", blkno);
	if (header->pd_lower < SizeOfPageHeaderData ||
		header->pd_lower > header->pd_upper ||
		header->pd_upper > header->pd_special ||
		header->pd_special > BLCKSZ)
		IvfflatStatsError(index, "page header bounds are invalid", blkno);
}

static void
IvfflatStatsClaimPage(Relation index, IvfflatStatsPageKind *pageKinds,
					   BlockNumber blkno, IvfflatStatsPageKind kind)
{
	if (pageKinds[blkno] != IVFFLAT_STATS_PAGE_UNKNOWN)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid ivfflat index \"%s\": block %u is referenced more than once",
						RelationGetRelationName(index), blkno)));
	pageKinds[blkno] = kind;
}

static bool
IvfflatStatsIncludeList(int listId, float4 sampleRate)
{
	uint32		hash;
	double		fraction;

	if (sampleRate >= 1.0f)
		return true;

	/* Stable, versioned multiplicative hash; list_id is zero-based. */
	hash = ((uint32) listId + ((uint32) 0x9e3779b9U)) * ((uint32) 2654435761U);
	fraction = ((double) hash) / ((double) UINT32_MAX + 1.0);
	return fraction < sampleRate;
}

static Relation
IvfflatStatsOpenIndex(Oid indexOid)
{
	Relation	index;
	char	   *amname;
	AclResult	aclresult;
	Oid			heapOid;

	index = relation_open(indexOid, AccessShareLock);
	if (index->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(index))));

	amname = get_am_name(index->rd_rel->relam);
	if (amname == NULL || strcmp(amname, "ivfflat") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an ivfflat index",
						RelationGetRelationName(index))));
	pfree(amname);

	if (RELATION_IS_OTHER_TEMP(index))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot inspect temporary indexes of other sessions")));

	heapOid = index->rd_index->indrelid;
	aclresult = pg_class_aclcheck(heapOid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(heapOid));

	return index;
}

static void
IvfflatStatsPutList(Tuplestorestate *tupstore, TupleDesc tupdesc,
					 const IvfflatIndexStatsData *indexStats,
					 const IvfflatListStatsData *listStats)
{
	Datum		values[IVFFLAT_LIST_STATS_COLUMNS] = {0};
	bool		nulls[IVFFLAT_LIST_STATS_COLUMNS] = {0};

	values[0] = Int32GetDatum(indexStats->formatVersion);
	values[1] = Int32GetDatum(indexStats->dimensions);
	values[2] = Int32GetDatum(indexStats->declaredLists);
	values[3] = Int32GetDatum(listStats->listId);
	values[4] = Int64GetDatum((int64) listStats->listPage);
	values[5] = Int32GetDatum((int32) listStats->listOffset);
	if (BlockNumberIsValid(listStats->startPage))
		values[6] = Int64GetDatum((int64) listStats->startPage);
	else
		nulls[6] = true;
	if (BlockNumberIsValid(listStats->insertPage))
		values[7] = Int64GetDatum((int64) listStats->insertPage);
	else
		nulls[7] = true;
	values[8] = Int64GetDatum(listStats->entryPages);
	values[9] = Int64GetDatum(listStats->physicalTuples);
	values[10] = Int64GetDatum(listStats->usableBytes);
	values[11] = Int64GetDatum(listStats->usedBytes);
	values[12] = Int64GetDatum(listStats->freeBytes);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

static void
IvfflatStatsScanEntryChain(Relation index, BlockNumber nblocks,
						IvfflatStatsPageKind *pageKinds,
						BufferAccessStrategy strategy,
						IvfflatListStatsData *listStats)
{
	BlockNumber blkno = listStats->startPage;
	bool		insertPageSeen = !BlockNumberIsValid(listStats->insertPage);

	while (BlockNumberIsValid(blkno))
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoffno;
		OffsetNumber offno;
		Size		usable;
		Size		freeSpace;
		BlockNumber nextblkno;

		CHECK_FOR_INTERRUPTS();
		IvfflatStatsCheckBlock(index, blkno, nblocks);
		IvfflatStatsClaimPage(index, pageKinds, blkno, IVFFLAT_STATS_PAGE_ENTRY);

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		IvfflatStatsCheckPage(index, page, blkno);

		usable = ((PageHeader) page)->pd_special - SizeOfPageHeaderData;
		freeSpace = PageGetExactFreeSpace(page);
		if (freeSpace > usable)
			IvfflatStatsError(index, "free space exceeds usable space", blkno);

		maxoffno = PageGetMaxOffsetNumber(page);
		for (offno = FirstOffsetNumber; offno <= maxoffno;
			 offno = OffsetNumberNext(offno))
		{
			ItemId		itemid = PageGetItemId(page, offno);

			if (ItemIdIsNormal(itemid))
				listStats->physicalTuples++;
		}

		listStats->entryPages++;
		listStats->usableBytes += usable;
		listStats->freeBytes += freeSpace;
		listStats->usedBytes += usable - freeSpace;
		if (blkno == listStats->insertPage)
			insertPageSeen = true;
		nextblkno = IvfflatPageGetOpaque(page)->nextblkno;
		UnlockReleaseBuffer(buf);
		blkno = nextblkno;
	}

	if (!insertPageSeen)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid ivfflat index \"%s\": insert page %u is not in list %d entry chain",
						RelationGetRelationName(index), listStats->insertPage,
						listStats->listId)));
}

static void
IvfflatCollectStats(Relation index, float4 sampleRate,
					Tuplestorestate *tupstore, TupleDesc tupdesc,
					IvfflatIndexStatsData *stats)
{
	BlockNumber nblocks = RelationGetNumberOfBlocks(index);
	IvfflatStatsPageKind *pageKinds;
	BufferAccessStrategy strategy;
	Buffer		metabuf;
	Page		metapage;
	IvfflatMetaPage meta;
	BlockNumber listBlkno = IVFFLAT_HEAD_BLKNO;
	int			listId = 0;

	if (nblocks <= IVFFLAT_METAPAGE_BLKNO)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid ivfflat index \"%s\": metapage is missing",
						RelationGetRelationName(index))));

	pageKinds = palloc0_array_checked(IvfflatStatsPageKind, nblocks);
	pageKinds[IVFFLAT_METAPAGE_BLKNO] = IVFFLAT_STATS_PAGE_META;
	strategy = GetAccessStrategy(BAS_BULKREAD);

	metabuf = ReadBufferExtended(index, MAIN_FORKNUM,
							 IVFFLAT_METAPAGE_BLKNO, RBM_NORMAL, strategy);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	IvfflatStatsCheckPage(index, metapage, IVFFLAT_METAPAGE_BLKNO);
	meta = IvfflatPageGetMeta(metapage);
	if (meta->magicNumber != IVFFLAT_MAGIC_NUMBER)
		IvfflatStatsError(index, "metapage magic number does not match",
						   IVFFLAT_METAPAGE_BLKNO);
	if (meta->version != IVFFLAT_VERSION)
		IvfflatStatsError(index, "metapage format version is unsupported",
						   IVFFLAT_METAPAGE_BLKNO);
	stats->formatVersion = meta->version;
	stats->dimensions = meta->dimensions;
	stats->declaredLists = meta->lists;
	stats->scannedPages = 1;
	UnlockReleaseBuffer(metabuf);

	while (BlockNumberIsValid(listBlkno))
	{
		Buffer		listbuf;
		Page		listpage;
		OffsetNumber maxoffno;
		OffsetNumber offno;
		BlockNumber nextListBlkno;

		CHECK_FOR_INTERRUPTS();
		IvfflatStatsCheckBlock(index, listBlkno, nblocks);
		IvfflatStatsClaimPage(index, pageKinds, listBlkno, IVFFLAT_STATS_PAGE_LIST);
		listbuf = ReadBufferExtended(index, MAIN_FORKNUM, listBlkno,
								 RBM_NORMAL, strategy);
		LockBuffer(listbuf, BUFFER_LOCK_SHARE);
		listpage = BufferGetPage(listbuf);
		IvfflatStatsCheckPage(index, listpage, listBlkno);
		maxoffno = PageGetMaxOffsetNumber(listpage);
		nextListBlkno = IvfflatPageGetOpaque(listpage)->nextblkno;
		stats->listPages++;
		stats->scannedPages++;

		for (offno = FirstOffsetNumber; offno <= maxoffno;
			 offno = OffsetNumberNext(offno))
		{
			ItemId		itemid = PageGetItemId(listpage, offno);
			IvfflatList list;
			IvfflatListStatsData listStats = {0};
			bool		include;

			if (listId >= stats->declaredLists)
				IvfflatStatsError(index, "more list tuples than declared",
								   listBlkno);
			if (!ItemIdIsNormal(itemid) ||
				ItemIdGetLength(itemid) < offsetof(IvfflatListData, center))
				IvfflatStatsError(index, "invalid list tuple", listBlkno);

			list = (IvfflatList) PageGetItem(listpage, itemid);
			listStats.listId = listId;
			listStats.listPage = listBlkno;
			listStats.listOffset = offno;
			listStats.startPage = list->startPage;
			listStats.insertPage = list->insertPage;
			include = IvfflatStatsIncludeList(listId, sampleRate);
			listId++;

			if (BlockNumberIsValid(listStats.startPage))
				IvfflatStatsCheckBlock(index, listStats.startPage, nblocks);
			if (BlockNumberIsValid(listStats.insertPage))
				IvfflatStatsCheckBlock(index, listStats.insertPage, nblocks);

			if (include)
			{
				IvfflatStatsScanEntryChain(index, nblocks, pageKinds, strategy,
										&listStats);
				if (listStats.physicalTuples == 0)
					stats->emptyLists++;
				stats->entryPages += listStats.entryPages;
				stats->physicalTuples += listStats.physicalTuples;
				stats->usableBytes += listStats.usableBytes;
				stats->usedBytes += listStats.usedBytes;
				stats->freeBytes += listStats.freeBytes;
				stats->scannedPages += listStats.entryPages;
				if (tupstore != NULL)
					IvfflatStatsPutList(tupstore, tupdesc, stats, &listStats);
			}
		}

		UnlockReleaseBuffer(listbuf);
		listBlkno = nextListBlkno;
	}

	if (listId != stats->declaredLists)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid ivfflat index \"%s\": found %d lists, expected %d",
						RelationGetRelationName(index), listId,
						stats->declaredLists)));

	FreeAccessStrategy(strategy);
	pfree(pageKinds);
}

Datum
ivfflat_list_stats(PG_FUNCTION_ARGS)
{
	Oid			indexOid = PG_GETARG_OID(0);
	float4		sampleRate = PG_GETARG_FLOAT4(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	index;
	IvfflatIndexStatsData stats = {0};

	if (isnan(sampleRate) || sampleRate <= 0.0f || sampleRate > 1.0f)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sample_rate must be greater than 0 and less than or equal to 1")));

	InitMaterializedSRF(fcinfo, 0);
	index = IvfflatStatsOpenIndex(indexOid);
	IvfflatCollectStats(index, sampleRate, rsinfo->setResult, rsinfo->setDesc,
						 &stats);
	relation_close(index, AccessShareLock);

	return (Datum) 0;
}

Datum
ivfflat_index_stats(PG_FUNCTION_ARGS)
{
	Oid			indexOid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	index;
	IvfflatIndexStatsData stats = {0};
	TimestampTz started;
	TimestampTz finished;
	Datum		values[IVFFLAT_INDEX_STATS_COLUMNS] = {0};
	bool		nulls[IVFFLAT_INDEX_STATS_COLUMNS] = {0};

	InitMaterializedSRF(fcinfo, 0);
	index = IvfflatStatsOpenIndex(indexOid);
	started = GetCurrentTimestamp();
	IvfflatCollectStats(index, 1.0f, NULL, NULL, &stats);
	finished = GetCurrentTimestamp();
	relation_close(index, AccessShareLock);

	values[0] = Int32GetDatum(stats.formatVersion);
	values[1] = Int32GetDatum(stats.dimensions);
	values[2] = Int32GetDatum(stats.declaredLists);
	values[3] = Int64GetDatum(stats.listPages);
	values[4] = Int64GetDatum(stats.entryPages);
	values[5] = Int64GetDatum(stats.physicalTuples);
	values[6] = Int64GetDatum(stats.emptyLists);
	values[7] = Int64GetDatum(stats.usableBytes);
	values[8] = Int64GetDatum(stats.usedBytes);
	values[9] = Int64GetDatum(stats.freeBytes);
	values[10] = Int64GetDatum(stats.scannedPages);
	values[11] = Float8GetDatum(((double) (finished - started)) / 1000.0);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum) 0;
}
