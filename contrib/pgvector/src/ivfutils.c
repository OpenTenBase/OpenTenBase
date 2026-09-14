#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/table.h"
#include "bitvec.h"
#include "catalog/index.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "funcapi.h"
#include "halfutils.h"
#include "halfvec.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * Allocate a vector array
 */
VectorArray
VectorArrayInit(int maxlen, int dimensions, Size itemsize)
{
	VectorArray res = palloc(sizeof(VectorArrayData));

	/* Ensure items are aligned to prevent UB */
	itemsize = MAXALIGN(itemsize);

	res->length = 0;
	res->maxlen = maxlen;
	res->dim = dimensions;
	res->itemsize = itemsize;
	res->items = palloc_extended(maxlen * itemsize, MCXT_ALLOC_ZERO | MCXT_ALLOC_HUGE);
	return res;
}

/*
 * Free a vector array
 */
void
VectorArrayFree(VectorArray arr)
{
	pfree(arr->items);
	pfree(arr);
}

/*
 * Get the number of lists in the index
 */
int
IvfflatGetLists(Relation index)
{
	IvfflatOptions *opts = (IvfflatOptions *) index->rd_options;

	if (opts)
		return opts->lists;

	return IVFFLAT_DEFAULT_LISTS;
}

/*
 * Get proc
 */
FmgrInfo *
IvfflatOptionalProcInfo(Relation index, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(index, 1, procnum)))
		return NULL;

	return index_getprocinfo(index, 1, procnum);
}

/*
 * Normalize value
 */
Datum
IvfflatNormValue(const IvfflatTypeInfo * typeInfo, Oid collation, Datum value)
{
	return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

/*
 * Normalize vectors
 *
 * NOTE: this function is declared in ivfflat.h but was inadvertently
 * dropped from src/ivfutils.c in v0.8.6.  The implementation below is
 * the v0.8.5 version, restored here so that the link-time reference
 * from IvfflatKmeans() / IvfflatBuild() does not produce an "undefined
 * symbol" error when loading vector.so.
 */
void
IvfflatNormVectors(const IvfflatTypeInfo * typeInfo, Oid collation, VectorArray arr, MemoryContext tmpCtx)
{
	MemoryContext oldCtx = MemoryContextSwitchTo(tmpCtx);

	for (int i = 0; i < arr->length; i++)
	{
		Datum		value = PointerGetDatum(VectorArrayGet(arr, i));
		Datum		newValue = IvfflatNormValue(typeInfo, collation, value);

		VectorArraySet(arr, i, DatumGetPointer(newValue));
		MemoryContextReset(tmpCtx);
	}

	MemoryContextSwitchTo(oldCtx);
}

/*
 * Check if non-zero norm
 */
bool
IvfflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
	return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

/*
 * New buffer
 */
Buffer
IvfflatNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * Init page
 */
void
IvfflatInitPage(Buffer buf, Page page)
{
#if PG_VERSION_NUM >= 180000
	PageInit(page, BufferGetPageSize(buf), sizeof(IvfflatPageOpaqueData));
#else
	PageInit(page, BufferGetPageSize(buf), sizeof(IvfflatPageOpaqueData), true);
#endif
	IvfflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	IvfflatPageGetOpaque(page)->page_id = IVFFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void
IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	IvfflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
IvfflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void
IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
	/* Get new buffer */
	Buffer		newbuf = IvfflatNewBuffer(index, forkNum);
	Page		newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Update the previous buffer */
	IvfflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Init new page */
	IvfflatInitPage(newbuf, newpage);

	/* Commit */
	GenericXLogFinish(*state);

	/* Unlock */
	UnlockReleaseBuffer(*buf);

	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
	*buf = newbuf;
}

/*
 * Get the metapage info
 */
void
IvfflatGetMetaPageInfo(Relation index, int *lists, int *dimensions)
{
	Buffer		buf;
	Page		page;
	IvfflatMetaPage metap;

	buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = IvfflatPageGetMeta(page);

	if (unlikely(metap->magicNumber != IVFFLAT_MAGIC_NUMBER))
		elog(ERROR, "ivfflat index is not valid");

	if (lists != NULL)
		*lists = metap->lists;

	if (dimensions != NULL)
		*dimensions = metap->dimensions;

	UnlockReleaseBuffer(buf);
}

/*
 * Check memory usage
 *
 * NOTE: this function is declared in ivfflat.h but was inadvertently
 * dropped from src/ivfflat.c in v0.8.6.  The implementation below is
 * the v0.8.5 version, restored here so that the link-time reference
 * from IvfflatKmeans() / IvfflatBuildState accounting does not produce
 * an "undefined symbol" error when loading vector.so.
 */
void
IvfflatCheckMemoryUsage(Size totalSize)
{
	/* Add one to error message to ceil */
	if (totalSize > maintenance_work_mem * (Size) 1024)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("memory required is %zu MB, maintenance_work_mem is %d MB",
						totalSize / (1024 * 1024) + 1, maintenance_work_mem / 1024)));
}

/*
 * Update the start or insert page of a list
 */
void
IvfflatUpdateList(Relation index, ListInfo listInfo,
				  BlockNumber insertPage, BlockNumber originalInsertPage,
				  BlockNumber startPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	IvfflatList list;
	bool		changed = false;

	buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	list = (IvfflatList) PageGetItem(page, PageGetItemId(page, listInfo.offno));

	if (BlockNumberIsValid(insertPage) && insertPage != list->insertPage)
	{
		/* Skip update if insert page is lower than original insert page  */
		/* This is needed to prevent insert from overwriting vacuum */
		if (!BlockNumberIsValid(originalInsertPage) || insertPage >= originalInsertPage)
		{
			list->insertPage = insertPage;
			changed = true;
		}
	}

	if (BlockNumberIsValid(startPage) && startPage != list->startPage)
	{
		list->startPage = startPage;
		changed = true;
	}

	/* Only commit if changed */
	if (changed)
		IvfflatCommitBuffer(buf, state);
	else
	{
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
	}
}

PGDLLEXPORT Datum l2_normalize(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum halfvec_l2_normalize(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum sparsevec_l2_normalize(PG_FUNCTION_ARGS);

static Size
VectorItemSize(int dimensions)
{
	return VECTOR_SIZE(dimensions);
}

static Size
HalfvecItemSize(int dimensions)
{
	return HALFVEC_SIZE(dimensions);
}

static Size
BitItemSize(int dimensions)
{
	return VARBITTOTALLEN(dimensions);
}

static void
VectorUpdateCenter(Pointer v, int dimensions, float *x)
{
	Vector	   *vec = (Vector *) v;

	SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
	vec->dim = dimensions;

	for (int k = 0; k < dimensions; k++)
		vec->x[k] = x[k];
}

static void
HalfvecUpdateCenter(Pointer v, int dimensions, float *x)
{
	HalfVector *vec = (HalfVector *) v;

	SET_VARSIZE(vec, HALFVEC_SIZE(dimensions));
	vec->dim = dimensions;

	for (int k = 0; k < dimensions; k++)
		vec->x[k] = Float4ToHalfUnchecked(x[k]);
}

static void
BitUpdateCenter(Pointer v, int dimensions, float *x)
{
	VarBit	   *vec = (VarBit *) v;
	unsigned char *nx = VARBITS(vec);

	SET_VARSIZE(vec, VARBITTOTALLEN(dimensions));
	VARBITLEN(vec) = dimensions;

	for (uint32 k = 0; k < VARBITBYTES(vec); k++)
		nx[k] = 0;

	for (int k = 0; k < dimensions; k++)
		nx[k / 8] |= (x[k] > 0.5 ? 1 : 0) << (7 - (k % 8));
}

static void
VectorSumCenter(Pointer v, float *x)
{
	Vector	   *vec = (Vector *) v;

	for (int k = 0; k < vec->dim; k++)
		x[k] += vec->x[k];
}

static void
HalfvecSumCenter(Pointer v, float *x)
{
	HalfVector *vec = (HalfVector *) v;

	for (int k = 0; k < vec->dim; k++)
		x[k] += HalfToFloat4(vec->x[k]);
}

static void
BitSumCenter(Pointer v, float *x)
{
	VarBit	   *vec = (VarBit *) v;

	for (int k = 0; k < VARBITLEN(vec); k++)
		x[k] += (float) (((VARBITS(vec)[k / 8]) >> (7 - (k % 8))) & 0x01);
}

/*
 * Get type info
 */
const		IvfflatTypeInfo *
IvfflatGetTypeInfo(Relation index)
{
	FmgrInfo   *procinfo = IvfflatOptionalProcInfo(index, IVFFLAT_TYPE_INFO_PROC);

	if (procinfo == NULL)
	{
		static const IvfflatTypeInfo typeInfo = {
			.maxDimensions = IVFFLAT_MAX_DIM,
			.normalize = l2_normalize,
			.itemSize = VectorItemSize,
			.updateCenter = VectorUpdateCenter,
			.sumCenter = VectorSumCenter
		};

		return (&typeInfo);
	}
	else
		return (const IvfflatTypeInfo *) DatumGetPointer(FunctionCall0Coll(procinfo, InvalidOid));
}

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_halfvec_support);
Datum
ivfflat_halfvec_support(PG_FUNCTION_ARGS)
{
	static const IvfflatTypeInfo typeInfo = {
		.maxDimensions = IVFFLAT_MAX_DIM * 2,
		.normalize = halfvec_l2_normalize,
		.itemSize = HalfvecItemSize,
		.updateCenter = HalfvecUpdateCenter,
		.sumCenter = HalfvecSumCenter
	};

	PG_RETURN_POINTER(&typeInfo);
};

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_bit_support);
Datum
ivfflat_bit_support(PG_FUNCTION_ARGS)
{
	static const IvfflatTypeInfo typeInfo = {
		.maxDimensions = IVFFLAT_MAX_DIM * 32,
		.normalize = NULL,
		.itemSize = BitItemSize,
		.updateCenter = BitUpdateCenter,
		.sumCenter = BitSumCenter
	};

	PG_RETURN_POINTER(&typeInfo);
};

/*
 * Diagnostic helper: return the key runtime parameters of an ivfflat index
 * (lists, dimensions, opclass name, current session probes) as a single
 * row.  Intended for "recall low" diagnostics — see project notes §v0.1 §5.
 *
 * Usage: SELECT * FROM ivfflat_index_info('my_ivfflat_idx'::regclass);
 *
 * Note: this is a single-row set-returning function; we materialise the row
 * into a tuplestore during the first (and only) call.
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_index_info);
Datum
ivfflat_index_info(PG_FUNCTION_ARGS)
{
	Oid			indexOid = PG_GETARG_OID(0);
	Relation	index;
	char	   *amname;
	int			lists;
	int			dimensions;
	Oid			opclassOid;
	char	   *opclassName;
	int			probes = IVFFLAT_DEFAULT_PROBES;
	char	   *probes_str;
	HeapTuple	indextup;
	Datum		indclassDatum;
	bool		isnull;
	oidvector  *indclass;
	HeapTuple	classtup;
	Form_pg_opclass opclassForm;
	ReturnSetInfo *rsinfo;
	Tuplestorestate *tupstore;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};

	/* Open and validate the relation is an ivfflat index */
	index = index_open(indexOid, AccessShareLock);

	amname = get_am_name(index->rd_rel->relam);
	if (amname == NULL || strcmp(amname, "ivfflat") != 0)
	{
		if (amname != NULL)
			pfree(amname);
		index_close(index, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not an ivfflat index",
						get_rel_name(indexOid))));
	}
	pfree(amname);

	/* Read metapage: lists and dimensions */
	IvfflatGetMetaPageInfo(index, &lists, &dimensions);

	/*
	 * Read current session's ivfflat.probes GUC.  If missing (extension not
	 * loaded?) or invalid, fall back to the compile-time default.  This makes
	 * the function safe to call from any context where the GUC is accessible.
	 *
	 * Note: GetConfigOptionByName() takes (name, &varname, missing_ok).  The
	 * varname out-param is only used for error messages inside the GUC machinery.
	 */
	{
		const char *varname;

		probes_str = GetConfigOptionByName("ivfflat.probes", &varname, true);
		(void) varname;
	}
	if (probes_str != NULL)
	{
		int			tmp = atoi(probes_str);

		if (tmp >= IVFFLAT_MIN_LISTS && tmp <= IVFFLAT_MAX_LISTS)
			probes = tmp;
		pfree(probes_str);
	}

	/*
	 * Look up opclass name from the first index column's opclass.
	 *
	 * PG18 hides pg_index.indclass inside #ifdef CATALOG_VARLEN, so accessing
	 * rd_index->indclass directly fails for extensions.  Read it via syscache
	 * instead.
	 */
	indextup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexOid));
	if (!HeapTupleIsValid(indextup))
	{
		index_close(index, AccessShareLock);
		elog(ERROR, "cache lookup failed for index %u", indexOid);
	}
	indclassDatum = SysCacheGetAttr(INDEXRELID, indextup,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(indclassDatum);
	opclassOid = indclass->values[0];
	ReleaseSysCache(indextup);

	/* Resolve opclass name via pg_opclass syscache */
	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassOid));
	if (!HeapTupleIsValid(classtup))
	{
		index_close(index, AccessShareLock);
		elog(ERROR, "cache lookup failed for opclass %u", opclassOid);
	}
	opclassForm = (Form_pg_opclass) GETSTRUCT(classtup);
	opclassName = pstrdup(NameStr(opclassForm->opcname));
	ReleaseSysCache(classtup);

	/*
	 * Materialise a single-row SRF.  InitMaterializedSRF() builds and stores
	 * a tuplestore in rsinfo->setResult for us -- we just push our row into
	 * it.  Do NOT create a second tuplestore or overwrite rsinfo->setResult,
	 * as that would orphan the one already set up.
	 */
	InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	tupstore = rsinfo->setResult;

	values[0] = Int32GetDatum(lists);
	values[1] = Int32GetDatum(dimensions);
	values[2] = CStringGetTextDatum(opclassName);
	values[3] = Int32GetDatum(probes);
	tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);

	pfree(opclassName);
	index_close(index, AccessShareLock);

	PG_RETURN_NULL();
}
