#include "postgres.h"

#include <float.h>

#include "access/amapi.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "fmgr.h"
#include "funcapi.h"
#include "ivfflat.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/relcache.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"
#include "vector.h"

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

int			ivfflat_probes;
int			ivfflat_iterative_scan;
int			ivfflat_max_probes;
bool		ivfflat_adaptive_scan = false;
double		ivfflat_target_recall = 0.95;
int			ivfflat_min_probes = 1;
double		ivfflat_adaptive_threshold = 1.20;
bool		ivfflat_query_cache = true;
int			ivfflat_query_cache_size = 1024;
bool		ivfflat_global_cache = true;
static relopt_kind ivfflat_relopt_kind;

static const struct config_enum_entry ivfflat_iterative_scan_options[] = {
	{"off", IVFFLAT_ITERATIVE_SCAN_OFF, false},
	{"relaxed_order", IVFFLAT_ITERATIVE_SCAN_RELAXED, false},
	{NULL, 0, false}
};

/*
 * Initialize index options and variables
 */
void
IvfflatInit(void)
{
	ivfflat_relopt_kind = add_reloption_kind();
	add_int_reloption(ivfflat_relopt_kind, "lists", "Number of inverted lists",
					  IVFFLAT_DEFAULT_LISTS, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, AccessExclusiveLock);

	DefineCustomIntVariable("ivfflat.probes", "Sets the number of probes",
							"Valid range is 1..lists.", &ivfflat_probes,
							IVFFLAT_DEFAULT_PROBES, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable("ivfflat.iterative_scan", "Sets the mode for iterative scans",
							 NULL, &ivfflat_iterative_scan,
							 IVFFLAT_ITERATIVE_SCAN_OFF, ivfflat_iterative_scan_options, PGC_USERSET, 0, NULL, NULL, NULL);

	/* If this is less than probes, probes is used */
	DefineCustomIntVariable("ivfflat.max_probes", "Sets the max number of probes for iterative scans",
							NULL, &ivfflat_max_probes,
							IVFFLAT_MAX_LISTS, IVFFLAT_MIN_LISTS, IVFFLAT_MAX_LISTS, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ivfflat.adaptive_scan",
							 "Enable adaptive dynamic pruning and early stopping during IVFFlat scan",
							 NULL,
							 &ivfflat_adaptive_scan,
							 false,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("ivfflat.target_recall",
							 "Target quality threshold factor for dynamic early exit",
							 NULL,
							 &ivfflat_target_recall,
							 0.95, 0.50, 2.00,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ivfflat.min_probes",
							"Minimum number of probes before adaptive early stopping can trigger",
							NULL,
							&ivfflat_min_probes,
							1, 1, IVFFLAT_MAX_LISTS,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("ivfflat.adaptive_threshold",
							 "Distance multiplier threshold for adaptive early exit",
							 NULL,
							 &ivfflat_adaptive_threshold,
							 1.20, 0.10, 10.00,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ivfflat.query_cache",
							 "Enable query result LRU cache to eliminate redundant computation",
							 NULL,
							 &ivfflat_query_cache,
							 true,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ivfflat.query_cache_size",
							"Maximum number of entries in IVFFlat query LRU cache",
							NULL,
							&ivfflat_query_cache_size,
							1024, 16, 65536,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ivfflat.global_cache",
							 "Enable cross-process global shared memory vector subtree cache",
							 NULL,
							 &ivfflat_global_cache,
							 true,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("ivfflat");
}

/*
 * SQL-callable function: Recommend Pareto-optimal parameters for IVFFlat
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_recommend_params);
Datum
ivfflat_recommend_params(PG_FUNCTION_ARGS)
{
	int64		rows = PG_GETARG_INT64(0);
	int32		dim = PG_GETARG_INT32(1);
	text	   *metric_text = PG_GETARG_TEXT_PP(2);
	float8		target_recall = PG_GETARG_FLOAT8(3);
	bool		apply_to_session = (PG_NARGS() >= 5) ? PG_GETARG_BOOL(4) : false;
	char	   *metric = text_to_cstring(metric_text);

	(void) dim;

	int			lists;
	int			probes;
	int			min_probes;
	double		adaptive_threshold;
	double		sqrt_lists;

	TupleDesc	tupdesc;
	Datum		values[8];
	bool		nulls[8] = {false};
	HeapTuple	tuple;
	char		index_clause[128];
	char		set_cmds[512];

	if (rows <= 1000000)
		lists = Max(10, Min(32768, (int) round((double) rows / 1000.0)));
	else
		lists = Max(100, Min(32768, (int) round(sqrt((double) rows))));

	sqrt_lists = sqrt((double) lists);

	if (target_recall <= 0.90)
		probes = Max(1, (int) round(0.5 * sqrt_lists));
	else if (target_recall <= 0.96)
		probes = Max(1, (int) round(1.0 * sqrt_lists));
	else
		probes = Max(1, (int) round(2.0 * sqrt_lists));

	min_probes = Max(1, (int) round(probes * 0.3));
	adaptive_threshold = (pg_strcasecmp(metric, "cosine") == 0) ? 1.15 : 1.20;

	/* Directly apply configuration to current session if requested (default: true) */
	if (apply_to_session)
	{
		char		probes_str[32];
		char		recall_str[32];
		char		min_probes_str[32];
		char		thresh_str[32];

		snprintf(probes_str, sizeof(probes_str), "%d", probes);
		snprintf(recall_str, sizeof(recall_str), "%.2f", target_recall);
		snprintf(min_probes_str, sizeof(min_probes_str), "%d", min_probes);
		snprintf(thresh_str, sizeof(thresh_str), "%.2f", adaptive_threshold);

		SetConfigOption("ivfflat.probes", probes_str, PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("ivfflat.adaptive_scan", "on", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("ivfflat.target_recall", recall_str, PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("ivfflat.min_probes", min_probes_str, PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("ivfflat.adaptive_threshold", thresh_str, PGC_USERSET, PGC_S_SESSION);
	}

	tupdesc = CreateTemplateTupleDesc(8);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "lists", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "probes", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "adaptive_scan", BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "target_recall", FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "min_probes", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "adaptive_threshold", FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "sql_index_clause", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "sql_set_commands", TEXTOID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int32GetDatum(lists);
	values[1] = Int32GetDatum(probes);
	values[2] = BoolGetDatum(true);
	values[3] = Float8GetDatum(target_recall);
	values[4] = Int32GetDatum(min_probes);
	values[5] = Float8GetDatum(adaptive_threshold);

	snprintf(index_clause, sizeof(index_clause), "WITH (lists = %d)", lists);
	values[6] = CStringGetTextDatum(index_clause);

	snprintf(set_cmds, sizeof(set_cmds),
			 "SET ivfflat.probes = %d; SET ivfflat.adaptive_scan = on; SET ivfflat.target_recall = %.2f; SET ivfflat.min_probes = %d; SET ivfflat.adaptive_threshold = %.2f;",
			 probes, target_recall, min_probes, adaptive_threshold);
	values[7] = CStringGetTextDatum(set_cmds);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * SQL-callable function: Automatically apply adaptive session GUC settings
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflat_auto_tune);
Datum
ivfflat_auto_tune(PG_FUNCTION_ARGS)
{
	float8		target_recall = PG_GETARG_FLOAT8(0);
	int32		probes = PG_GETARG_INT32(1);
	int			min_probes;
	char		probes_str[32];
	char		recall_str[32];
	char		min_probes_str[32];

	if (probes <= 0)
		probes = 10;

	min_probes = Max(1, (int) round(probes * 0.3));

	snprintf(probes_str, sizeof(probes_str), "%d", probes);
	snprintf(recall_str, sizeof(recall_str), "%.2f", target_recall);
	snprintf(min_probes_str, sizeof(min_probes_str), "%d", min_probes);

	SetConfigOption("ivfflat.probes", probes_str, PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("ivfflat.adaptive_scan", "on", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("ivfflat.target_recall", recall_str, PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("ivfflat.min_probes", min_probes_str, PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("ivfflat.adaptive_threshold", "1.20", PGC_USERSET, PGC_S_SESSION);

	PG_RETURN_TEXT_P(cstring_to_text("IVFFlat adaptive configuration successfully applied to session."));
}

/*
 * Get the name of index build phase
 */
static char *
ivfflatbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_IVFFLAT_PHASE_KMEANS:
			return "performing k-means";
		case PROGRESS_IVFFLAT_PHASE_ASSIGN:
			return "assigning tuples";
		case PROGRESS_IVFFLAT_PHASE_LOAD:
			return "loading tuples";
		default:
			return NULL;
	}
}

/*
 * Estimate the cost of an index scan
 */
static void
ivfflatcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
	GenericCosts costs;
	int			lists;
	double		ratio;
	double		sequentialRatio = 0.5;
	double		startupPages;
	double		spc_seq_page_cost;
	Relation	index;

	/* Never use index without order */
	if (path->indexorderbys == NIL)
	{
		*indexStartupCost = get_float8_infinity();
		*indexTotalCost = get_float8_infinity();
		*indexSelectivity = 0;
		*indexCorrelation = 0;
		*indexPages = 0;
#if PG_VERSION_NUM >= 180000
		/* See "On disable_cost" thread on pgsql-hackers */
		path->path.disabled_nodes = 2;
#endif
		return;
	}

	MemSet(&costs, 0, sizeof(costs));

	genericcostestimate(root, path, loop_count, &costs);

	index = index_open(path->indexinfo->indexoid, NoLock);
	IvfflatGetMetaPageInfo(index, &lists, NULL);
	index_close(index, NoLock);

	/* Get the ratio of lists that we need to visit */
	ratio = ((double) ivfflat_probes) / lists;
	if (ratio > 1.0)
		ratio = 1.0;

	get_tablespace_page_costs(path->indexinfo->reltablespace, NULL, &spc_seq_page_cost);

	/* Change some page cost from random to sequential */
	costs.indexTotalCost -= sequentialRatio * costs.numIndexPages * (costs.spc_random_page_cost - spc_seq_page_cost);

	/* Startup cost is cost before returning the first row */
	costs.indexStartupCost = costs.indexTotalCost * ratio;

	/* Adjust cost if needed since TOAST not included in seq scan cost */
	startupPages = costs.numIndexPages * ratio;
	if (startupPages > path->indexinfo->rel->pages && ratio < 0.5)
	{
		/* Change rest of page cost from random to sequential */
		costs.indexStartupCost -= (1 - sequentialRatio) * startupPages * (costs.spc_random_page_cost - spc_seq_page_cost);

		/* Remove cost of extra pages */
		costs.indexStartupCost -= (startupPages - path->indexinfo->rel->pages) * spc_seq_page_cost;
	}

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
ivfflatoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"lists", RELOPT_TYPE_INT, offsetof(IvfflatOptions, lists)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  ivfflat_relopt_kind,
									  sizeof(IvfflatOptions),
									  tab, lengthof(tab));
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
ivfflatvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(ivfflathandler);
Datum
ivfflathandler(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 190000
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,
		.amstrategies = 0,
		.amsupport = 5,
		.amoptsprocnum = 0,
		.amcanorder = false,
		.amcanorderbyop = true,
		.amcanhash = false,
		.amconsistentequality = false,
		.amconsistentordering = false,
		.amcanbackward = false,
		.amcanunique = false,
		.amcanmulticol = false,
		.amoptionalkey = true,
		.amsearcharray = false,
		.amsearchnulls = false,
		.amstorage = false,
		.amclusterable = false,
		.ampredlocks = false,
		.amcanparallel = false,
		.amcanbuildparallel = true,
		.amcaninclude = false,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL,
		.amkeytype = InvalidOid,

		.ambuild = ivfflatbuild,
		.ambuildempty = ivfflatbuildempty,
		.aminsert = ivfflatinsert,
		.aminsertcleanup = NULL,
		.ambulkdelete = ivfflatbulkdelete,
		.amvacuumcleanup = ivfflatvacuumcleanup,
		.amcanreturn = NULL,
		.amcostestimate = ivfflatcostestimate,
		.amgettreeheight = NULL,
		.amoptions = ivfflatoptions,
		.amproperty = NULL,
		.ambuildphasename = ivfflatbuildphasename,
		.amvalidate = ivfflatvalidate,
		.amadjustmembers = NULL,
		.ambeginscan = ivfflatbeginscan,
		.amrescan = ivfflatrescan,
		.amgettuple = ivfflatgettuple,
		.amgetbitmap = NULL,
		.amendscan = ivfflatendscan,
		.ammarkpos = NULL,
		.amrestrpos = NULL,
		.amestimateparallelscan = NULL,
		.aminitparallelscan = NULL,
		.amparallelrescan = NULL,
		.amtranslatestrategy = NULL,
		.amtranslatecmptype = NULL,
	};

	PG_RETURN_POINTER(&amroutine);
#else
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 5;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
#if PG_VERSION_NUM >= 180000
	amroutine->amcanhash = false;
	amroutine->amconsistentequality = false;
	amroutine->amconsistentordering = false;
#endif
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
	amroutine->amcanbuildparallel = true;
#endif
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
#if PG_VERSION_NUM >= 160000
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = ivfflatbuild;
	amroutine->ambuildempty = ivfflatbuildempty;
	amroutine->aminsert = ivfflatinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = ivfflatbulkdelete;
	amroutine->amvacuumcleanup = ivfflatvacuumcleanup;
	amroutine->amcanreturn = NULL;	/* tuple not included in heapsort */
	amroutine->amcostestimate = ivfflatcostestimate;
#if PG_VERSION_NUM >= 180000
	amroutine->amgettreeheight = NULL;
#endif
	amroutine->amoptions = ivfflatoptions;
	amroutine->amproperty = NULL;	/* TODO AMPROP_DISTANCE_ORDERABLE */
	amroutine->ambuildphasename = ivfflatbuildphasename;
	amroutine->amvalidate = ivfflatvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = ivfflatbeginscan;
	amroutine->amrescan = ivfflatrescan;
	amroutine->amgettuple = ivfflatgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = ivfflatendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

#if PG_VERSION_NUM >= 180000
	amroutine->amtranslatestrategy = NULL;
	amroutine->amtranslatecmptype = NULL;
#endif

	PG_RETURN_POINTER(amroutine);
#endif
}
