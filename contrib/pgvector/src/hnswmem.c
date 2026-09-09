/*
 * Memory estimation for HNSW index builds.
 *
 * The point of this module is to answer, before CREATE INDEX is even started,
 * how much memory the build is going to need and what maintenance_work_mem
 * should be set to. Without it the only feedback a user gets is the notice the
 * build emits once the graph has already outgrown the budget, at which point
 * the build has switched to its slow on-disk path.
 *
 * The model mirrors what InsertTuple() and HnswInitElement() really allocate
 * for every element:
 *
 *	1. the vector itself						HnswAlloc(allocator, valueSize)
 *	2. an HnswElementData						HnswInitElement()
 *	3. an array of per-level neighbor pointers	HnswInitNeighbors()
 *	4. one HnswNeighborArray per level			HnswInitNeighbors()
 *
 * Accuracy is measured in bench/report.md (reproduce with bench/run_bench.sh);
 * doc/vector-index-tuning.md has the derivation. Across the swept
 * configurations (10k-500k rows, 128 and 768 dimensions, m = 16 and 32) the
 * raw model lands within 0.09% and 2.63% of the observed graph size — under 1%
 * for all but the smallest configuration. The safety margin below then makes
 * the published estimate larger than the measured peak on purpose, so a user
 * who follows the advice should not spill.
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "fmgr.h"
#include "funcapi.h"
#include "hnsw.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils_memorychunk.h"
#include "utils/rel.h"

PG_FUNCTION_INFO_V1(vector_index_memory_estimate);
PG_FUNCTION_INFO_V1(vector_index_memory_advise);

/*
 * MemoryContextAlloc() puts a MemoryChunk header in front of every allocation
 * and rounds the result up to MAXIMUM_ALIGNOF, so mirror that to reflect what
 * the build will really consume rather than the raw request size.
 */
#define HNSW_ALLOC_SIZE(size)	MAXALIGN(add_size((Size) (size), sizeof(MemoryChunk)))

/*
 * The graph lives in a generation memory context that hands out memory in
 * blocks, so the total ends up a little above the sum of the individual
 * allocations. Measured builds sit within ~1% of the raw model; this margin
 * keeps the estimate on the safe side, because under-estimating would leave
 * the user hitting the slow on-disk path anyway.
 */
#define HNSW_MEM_SAFETY_MARGIN	0.10

/*
 * Bytes per dimension, by stored type. `vector` keeps one float4 (4 bytes) per
 * dimension, `halfvec` keeps a half precision value (2 bytes). `sparsevec` is
 * not modelled: its size depends on how many components are non-zero, which is
 * a property of the data rather than of the dimensions.
 */
#define HNSW_BYTES_PER_DIM_VECTOR		((int) sizeof(float4))
#define HNSW_BYTES_PER_DIM_HALFVEC		2

static void HnswFormatBytes(Size bytes, char *buf, size_t buflen);
static void HnswFormatWorkMem(int kb, char *buf, size_t buflen);
static int	HnswVectorBytesPerDim(Oid typid);

/*
 * Estimate the graph memory needed to build an HNSW index.
 */
static Size
HnswEstimateBuildMemory(int64 rows, int dims, int m, int bytesPerDim)
{
	Size		vectorSize;
	Size		elementSize;
	Size		neighborListSize;
	Size		layer0Size;
	Size		extraLevelSize;
	Size		perElement;
	double		avgExtraLevels;
	double		total;

	if (rows <= 0 || dims <= 0 || m <= 0)
		return 0;

	if (bytesPerDim != HNSW_BYTES_PER_DIM_VECTOR &&
		bytesPerDim != HNSW_BYTES_PER_DIM_HALFVEC)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("bytes_per_dim must be %d (vector) or %d (halfvec)",
						HNSW_BYTES_PER_DIM_VECTOR, HNSW_BYTES_PER_DIM_HALFVEC)));

	/* The vector itself: a varlena header plus the stored width per dimension */
	vectorSize = HNSW_ALLOC_SIZE(VARHDRSZ + ((Size) dims) * bytesPerDim);

	/* The in-memory representation of the element */
	elementSize = HNSW_ALLOC_SIZE(sizeof(HnswElementData));

	/* The array of per-level neighbor array pointers */
	neighborListSize = HNSW_ALLOC_SIZE(sizeof(HnswNeighborArrayPtr));

	/* Level 0 holds HnswGetLayerM(m, 0), i.e. 2 * m candidates */
	layer0Size = HNSW_ALLOC_SIZE(HNSW_NEIGHBOR_ARRAY_SIZE(HnswGetLayerM(m, 0)));

	/* Every further level adds one pointer slot plus another neighbor array */
	extraLevelSize = sizeof(HnswNeighborArrayPtr)
		+ HNSW_ALLOC_SIZE(HNSW_NEIGHBOR_ARRAY_SIZE(HnswGetLayerM(m, 1)));

	/*
	 * level = floor(-log(U) * ml) with ml = 1 / ln(m) (see HnswInitElement).
	 * Therefore P(level >= k) = m^-k, so the expected number of levels above
	 * the first is 1 / (m - 1).
	 */
	avgExtraLevels = (m > 1) ? 1.0 / (m - 1) : 0.0;

	perElement = vectorSize + elementSize + neighborListSize + layer0Size
		+ (Size) (avgExtraLevels * extraLevelSize);

	total = perElement * (double) rows;

	/* Allow for block overhead and fragmentation */
	total *= (1.0 + HNSW_MEM_SAFETY_MARGIN);

	return (Size) total;
}

/*
 * Suggest a maintenance_work_mem setting, in KB, large enough for the given
 * estimate. Rounded up to the next power of two in MB so the advice is a value
 * people would actually type, with a sensible floor of 64MB.
 */
static int
HnswSuggestMaintenanceWorkMemKB(Size estimatedBytes)
{
	int64		neededMb;
	int64		suggestedMb = 64;		/* minimum sensible setting */

	if (estimatedBytes <= 0)
		return (int) suggestedMb * 1024;

	neededMb = ((int64) estimatedBytes + (1024 * 1024) - 1) / (1024 * 1024);

	while (suggestedMb < neededMb && suggestedMb < (1 << 20))
		suggestedMb *= 2;

	return (int) Min(suggestedMb * 1024, (int64) INT_MAX);
}

/*
 * Emit a notice before the build starts when the graph is unlikely to fit in
 * the current maintenance_work_mem. Called from BuildIndex().
 */
void
HnswReportMemoryAdvice(Relation heap, Relation index)
{
	double		reltuples;
	int			dims;
	int			bytesPerDim;
	int			m;
	Size		estimated;
	Size		budget;
	char		estBuf[64];
	char		sugBuf[64];

	if (heap == NULL)
		return;

	reltuples = heap->rd_rel->reltuples;
	if (reltuples <= 0)
		return;					/* nothing sensible to estimate from */

	dims = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (dims <= 0)
		return;

	bytesPerDim = HnswVectorBytesPerDim(TupleDescAttr(index->rd_att, 0)->atttypid);
	if (bytesPerDim == 0)
		return;					/* not a type the model covers */

	m = HnswGetM(index);
	estimated = HnswEstimateBuildMemory((int64) reltuples, dims, m, bytesPerDim);
	budget = (Size) maintenance_work_mem * 1024;

	if (estimated <= budget)
		return;

	HnswFormatBytes(estimated, estBuf, sizeof(estBuf));
	HnswFormatWorkMem(HnswSuggestMaintenanceWorkMemKB(estimated), sugBuf, sizeof(sugBuf));

	ereport(NOTICE,
			(errmsg("hnsw index build is estimated to need %s for " INT64_FORMAT " rows of %d dimensions (m = %d)",
					estBuf, (int64) reltuples, dims, m),
			 errdetail("The current maintenance_work_mem setting is %d kB, which is below the estimate.",
					   maintenance_work_mem),
			 errhint("Set maintenance_work_mem to at least %s before creating the index to avoid "
					 "materializing the graph to disk, which makes the build significantly slower.",
					 sugBuf)));
}

/*
 * Format a byte count for human consumption.
 */
static void
HnswFormatBytes(Size bytes, char *buf, size_t buflen)
{
	/*
	 * Binary units, labelled as such. Note this is deliberately different from
	 * PostgreSQL's pg_size_pretty(), which is decimal; the byte column in the
	 * benchmark report is the authoritative value.
	 */
	if (bytes >= (Size) 1024 * 1024 * 1024)
		snprintf(buf, buflen, "%.1f GiB", bytes / (1024.0 * 1024.0 * 1024.0));
	else if (bytes >= (Size) 1024 * 1024)
		snprintf(buf, buflen, "%.1f MiB", bytes / (1024.0 * 1024.0));
	else
		snprintf(buf, buflen, "%.1f KiB", bytes / 1024.0);
}

/*
 * Format a maintenance_work_mem value for human consumption.
 */
static void
HnswFormatWorkMem(int kb, char *buf, size_t buflen)
{
	if (kb >= 1024 * 1024)
		snprintf(buf, buflen, "%dGB", kb / (1024 * 1024));
	else
		snprintf(buf, buflen, "%dMB", kb / 1024);
}

/*
 * How many bytes per dimension a type stores, or 0 when the type is not
 * modelled. Recognises `vector` (float4) and `halfvec` (half precision), both
 * of which HNSW can index. Anything else, including `sparsevec`, returns 0.
 */
static int
HnswVectorBytesPerDim(Oid typid)
{
	char	   *tname = format_type_be(typid);
	char	   *base;
	char	   *dot;

	/*
	 * format_type_be() schema-qualifies the name unless the type is visible in
	 * the search path, and it does so for index columns in particular —
	 * "public.vector(32)" rather than "vector(32)".  Compare the bare name.
	 */
	dot = strrchr(tname, '.');
	base = (dot != NULL) ? dot + 1 : tname;

	if (strncmp(base, "vector", 6) == 0 && (base[6] == '\0' || base[6] == '('))
		return HNSW_BYTES_PER_DIM_VECTOR;

	if (strncmp(base, "halfvec", 7) == 0 && (base[7] == '\0' || base[7] == '('))
		return HNSW_BYTES_PER_DIM_HALFVEC;

	return 0;
}

/*
 * SQL: vector_index_memory_estimate(rows, dims, m [, bytes_per_dim])
 *
 * Pure arithmetic, so it can be called without building anything.
 * bytes_per_dim defaults to 4 (`vector`); pass 2 for `halfvec`.
 */
Datum
vector_index_memory_estimate(PG_FUNCTION_ARGS)
{
	int64		rows = PG_GETARG_INT64(0);
	int32		dims = PG_GETARG_INT32(1);
	int32		m = PG_GETARG_INT32(2);
	int32		bytesPerDim = PG_GETARG_INT32(3);
	Size		estimated;
	int			suggested;
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3];
	char		estBuf[64];
	char		sugBuf[64];

	if (rows <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("rows must be positive")));

	if (dims <= 0 || dims > HNSW_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dims must be between 1 and %d", HNSW_MAX_DIM)));

	if (m < HNSW_MIN_M || m > HNSW_MAX_M)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("m must be between %d and %d", HNSW_MIN_M, HNSW_MAX_M)));

	estimated = HnswEstimateBuildMemory(rows, dims, m, bytesPerDim);
	suggested = HnswSuggestMaintenanceWorkMemKB(estimated);

	HnswFormatBytes(estimated, estBuf, sizeof(estBuf));
	HnswFormatWorkMem(suggested, sugBuf, sizeof(sugBuf));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum((int64) estimated);
	values[1] = CStringGetTextDatum(estBuf);
	values[2] = CStringGetTextDatum(sugBuf);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * SQL: vector_index_memory_advise(table, m)
 *
 * Works out the answer for a real table: takes the row count from pg_class and
 * the dimension from the vector column, then compares the estimate against the
 * current maintenance_work_mem.
 */
Datum
vector_index_memory_advise(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int32		m = PG_GETARG_INT32(1);
	Relation	rel;
	TupleDesc	tupdesc;
	int			dims = 0;
	int			bytesPerDim = 0;
	double		reltuples;
	Size		estimated;
	Size		budget;
	int			suggested;
	TupleDesc	restupdesc;
	Datum		values[7];
	bool		nulls[7];
	char		estBuf[64];
	char		sugBuf[64];
	char		curBuf[64];

	if (m < HNSW_MIN_M || m > HNSW_MAX_M)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("m must be between %d and %d", HNSW_MIN_M, HNSW_MAX_M)));

	rel = relation_open(relid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		bytesPerDim = HnswVectorBytesPerDim(att->atttypid);
		if (bytesPerDim != 0)
		{
			dims = att->atttypmod;
			break;
		}
	}

	reltuples = rel->rd_rel->reltuples;
	relation_close(rel, AccessShareLock);

	if (dims <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("table does not have a vector or halfvec column")));

	if (reltuples <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("row count unknown"),
				 errhint("Run ANALYZE on the table first, or call "
						 "vector_index_memory_estimate(rows, dims, m) directly.")));

	estimated = HnswEstimateBuildMemory((int64) reltuples, dims, m, bytesPerDim);
	suggested = HnswSuggestMaintenanceWorkMemKB(estimated);
	budget = (Size) maintenance_work_mem * 1024;

	HnswFormatBytes(estimated, estBuf, sizeof(estBuf));
	HnswFormatWorkMem(suggested, sugBuf, sizeof(sugBuf));
	HnswFormatWorkMem(maintenance_work_mem, curBuf, sizeof(curBuf));

	if (get_call_result_type(fcinfo, NULL, &restupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	restupdesc = BlessTupleDesc(restupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum((int64) reltuples);
	values[1] = Int32GetDatum(dims);
	values[2] = Int64GetDatum((int64) estimated);
	values[3] = CStringGetTextDatum(estBuf);
	values[4] = CStringGetTextDatum(curBuf);
	values[5] = CStringGetTextDatum(sugBuf);
	values[6] = BoolGetDatum(estimated <= budget);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(restupdesc, values, nulls)));
}
