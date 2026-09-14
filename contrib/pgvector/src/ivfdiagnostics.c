#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "fmgr.h"
#include "ivfflat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * Conservative thresholds for heuristic diagnostics.
 *
 * pgvector documents lists and probes as starting points. These thresholds
 * intentionally allow broad deviation before warning to avoid false-positive
 * spam for legal tradeoffs.
 */
#define DIAG_RATIO_WARNING 4.0
#define DIAG_PROBES_LOW_RATIO 0.5
#define DIAG_PROBES_HIGH_RATIO 0.75
#define DIAG_LARGE_BUILD_ROWS 100000.0

FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_index_diagnostics);

static int
CeilSqrtInt(int value)
{
	int			result;

	if (value <= 1)
		return 1;

	result = (int) ceil(sqrt((double) value));
	return Max(result, 1);
}

static int
RecommendedLists(double rows)
{
	int			result;

	if (rows <= 0)
		return 0;

	if (rows <= 1000000.0)
		result = (int) ceil(rows / 1000.0);
	else
		result = (int) ceil(sqrt(rows));

	result = Max(result, IVFFLAT_MIN_LISTS);
	result = Min(result, IVFFLAT_MAX_LISTS);

	return result;
}

static void
AddDiagnostic(ReturnSetInfo *rsinfo, const char *severity, const char *issue,
			  const char *currentValue, const char *recommendedValue, const char *detail)
{
	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};

	values[0] = CStringGetTextDatum(severity);
	values[1] = CStringGetTextDatum(issue);
	values[2] = CStringGetTextDatum(currentValue);
	values[3] = CStringGetTextDatum(recommendedValue);
	values[4] = CStringGetTextDatum(detail);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

Datum
ivfflat_index_diagnostics(PG_FUNCTION_ARGS)
{
	Oid			indexOid = PG_GETARG_OID(0);
	Oid			ivfflatOid;
	Relation	index;
	Relation	heap;
	ReturnSetInfo *rsinfo;
	double		estimatedRows;
	bool		statsKnown;
	BlockNumber heapBlocks;
	bool		heapAppearsEmpty;
	bool		hasRepresentativeData;
	int			lists;
	int			dimensions;
	int			currentProbes;
	int			effectiveMaxProbes;
	int			recommendedLists;
	int			recommendedProbes;
	double		rowsPerList = 0;
	int			maxParallelMaintenanceWorkers = max_parallel_maintenance_workers;
	int			maxParallelWorkers = max_parallel_workers;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	index = index_open(indexOid, AccessShareLock);

	if (index->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index", RelationGetRelationName(index))));

	ivfflatOid = get_index_am_oid("ivfflat", false);
	if (index->rd_rel->relam != ivfflatOid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an IVFFlat index", RelationGetRelationName(index))));

	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	heapBlocks = RelationGetNumberOfBlocks(heap);

	IvfflatGetMetaPageInfo(index, &lists, &dimensions);

	estimatedRows = heap->rd_rel->reltuples;
	statsKnown = estimatedRows >= 0;
	heapAppearsEmpty = !statsKnown && heapBlocks == 0;
	hasRepresentativeData = !heapAppearsEmpty && (!statsKnown || estimatedRows > 0);
	currentProbes = Min(ivfflat_probes, lists);
	effectiveMaxProbes = ivfflat_iterative_scan == IVFFLAT_ITERATIVE_SCAN_OFF ? currentProbes : Min(Max(ivfflat_max_probes, currentProbes), lists);
	recommendedLists = statsKnown ? RecommendedLists(estimatedRows) : 0;
	recommendedProbes = Min(CeilSqrtInt(lists), lists);

	if (statsKnown && lists > 0)
		rowsPerList = estimatedRows / lists;

	AddDiagnostic(rsinfo, "INFO", "parameter_summary",
				  psprintf("estimated_rows=%.0f, lists=%d, probes=%d, iterative_scan=%s, max_probes=%d, dimensions=%d",
						   statsKnown ? estimatedRows : -1, lists, currentProbes,
						   ivfflat_iterative_scan == IVFFLAT_ITERATIVE_SCAN_OFF ? "off" : "enabled",
						   effectiveMaxProbes, dimensions),
				  psprintf("recommended_lists_start=%s, recommended_probes_start=%d",
						   statsKnown ? psprintf("%d", recommendedLists) : "unknown",
						   recommendedProbes),
				  "Recommendations are heuristic starting points from pgvector guidance; validate recall against an exact-search baseline for your workload.");

	if (!statsKnown)
	{
		if (heapAppearsEmpty)
			AddDiagnostic(rsinfo, "INFO", "empty_table",
						  psprintf("estimated_rows=unknown, heap_blocks=%u, lists=%d", heapBlocks, lists),
						  "build IVFFlat after representative data is loaded",
						  "The heap has no blocks and table row statistics are unknown. IVFFlat uses a training step; pgvector recommends creating the index after the table has data.");

		AddDiagnostic(rsinfo, "INFO", "stats_missing",
					  "estimated_rows=unknown",
					  "ANALYZE table",
					  "The table row estimate is unknown, so list-count guidance cannot be derived without scanning the table.");
	}
	else if (estimatedRows <= 0)
	{
		AddDiagnostic(rsinfo, "INFO", "empty_table",
					  psprintf("estimated_rows=%.0f, lists=%d", estimatedRows, lists),
					  "build IVFFlat after representative data is loaded",
					  "IVFFlat uses a training step; pgvector recommends creating the index after the table has data.");
	}
	else
	{
		if (estimatedRows < lists)
			AddDiagnostic(rsinfo, "WARNING", "too_little_data_for_lists",
						  psprintf("estimated_rows=%.0f, lists=%d, rows_per_list=%.2f", estimatedRows, lists, rowsPerList),
						  psprintf("consider lists around %d as a starting point", recommendedLists),
						  "There are fewer estimated rows than lists. The index may be over-partitioned and pgvector reports low recall when too little data is available for the number of lists.");

		if (recommendedLists > 0 && lists >= (int) ceil(recommendedLists * DIAG_RATIO_WARNING) && lists > recommendedLists)
			AddDiagnostic(rsinfo, "WARNING", "lists_high",
						  psprintf("lists=%d, estimated_rows=%.0f, rows_per_list=%.2f", lists, estimatedRows, rowsPerList),
						  psprintf("recommended starting point about %d lists", recommendedLists),
						  "High list counts can increase k-means work, build memory, and build time, and too little data per list may hurt IVFFlat quality.");
		else if (recommendedLists > 0 && (double) lists * DIAG_RATIO_WARNING <= recommendedLists)
			AddDiagnostic(rsinfo, "INFO", "lists_low",
						  psprintf("lists=%d, estimated_rows=%.0f", lists, estimatedRows),
						  psprintf("recommended starting point about %d lists", recommendedLists),
						  "Low list counts create coarser partitions and may require more probes for the desired recall/performance tradeoff.");
	}

	if (hasRepresentativeData && currentProbes < recommendedProbes && recommendedProbes >= 4 && currentProbes < (int) ceil(recommendedProbes * DIAG_PROBES_LOW_RATIO))
	{
		const char *iterativeDetail = ivfflat_iterative_scan == IVFFLAT_ITERATIVE_SCAN_OFF ?
			"Only a small subset of lists is searched; recall may be lower. Consider increasing ivfflat.probes and validate recall against exact search." :
			"Only a small subset of lists is searched initially; iterative scans may scan more lists up to ivfflat.max_probes. Validate recall against exact search.";

		AddDiagnostic(rsinfo, "WARNING", "low_recall_risk",
					  psprintf("probes=%d, lists=%d", currentProbes, lists),
					  psprintf("recommended probes starting point about %d", recommendedProbes),
					  iterativeDetail);
	}

	if (hasRepresentativeData && currentProbes >= lists)
		AddDiagnostic(rsinfo, "INFO", "all_lists_searched",
					  psprintf("probes=%d, lists=%d", currentProbes, lists),
					  "lower probes if approximate search speed is preferred",
					  "All lists are searched. This is an exact-style full-list search, and pgvector documents that the planner may not use the IVFFlat index in this case.");
	else if (hasRepresentativeData && (double) currentProbes >= lists * DIAG_PROBES_HIGH_RATIO)
		AddDiagnostic(rsinfo, "INFO", "high_query_cost",
					  psprintf("probes=%d, lists=%d", currentProbes, lists),
					  psprintf("recommended probes starting point about %d", recommendedProbes),
					  "Higher probes typically improve recall but increase query work. Treat this as a workload tradeoff, not an error.");

	if (ivfflat_iterative_scan != IVFFLAT_ITERATIVE_SCAN_OFF)
		AddDiagnostic(rsinfo, "INFO", "iterative_scan",
					  psprintf("probes=%d, max_probes=%d, effective_max_probes=%d", currentProbes, ivfflat_max_probes, effectiveMaxProbes),
					  "review ivfflat.max_probes for filtered ANN queries",
					  "Iterative scans can scan more lists after the initial probes until enough results are found or the effective max is reached.");

	if (statsKnown && estimatedRows >= DIAG_LARGE_BUILD_ROWS && maxParallelMaintenanceWorkers <= 1)
		AddDiagnostic(rsinfo, "INFO", "build_parallelism",
					  psprintf("estimated_rows=%.0f, max_parallel_maintenance_workers=%d, max_parallel_workers=%d",
							   estimatedRows, maxParallelMaintenanceWorkers, maxParallelWorkers),
					  "consider increasing max_parallel_maintenance_workers if CPU and worker capacity allow",
					  "For large IVFFlat builds, pgvector documents that increasing max_parallel_maintenance_workers can speed index creation.");

	AddDiagnostic(rsinfo, "INFO", "build_memory",
				  psprintf("maintenance_work_mem=%d kB, lists=%d", maintenance_work_mem, lists),
				  "increase maintenance_work_mem if build memory checks fail",
				  "Build memory feasibility is checked during IVFFlat k-means. This diagnostic reports risk factors but does not predict build time.");

	table_close(heap, AccessShareLock);
	index_close(index, AccessShareLock);

	return (Datum) 0;
}
