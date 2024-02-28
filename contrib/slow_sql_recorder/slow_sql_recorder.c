#include "slow_sql_recorder.h"
#include "tcop/utility.h"
PG_MODULE_MAGIC;

/* GUC variables */
// static int    auto_explain_log_min_duration = -1; /* msec or -1 */

// static const struct config_enum_entry format_options[] = {
//     {"text", EXPLAIN_FORMAT_TEXT, false},
//     {"xml", EXPLAIN_FORMAT_XML, false},
//     {"json", EXPLAIN_FORMAT_JSON, false},
//     {"yaml", EXPLAIN_FORMAT_YAML, false},
//     {NULL, 0, false}
// };

/* Current nesting depth of ExecutorRun calls */
static int    nesting_level = 0;
static const double max_query_duration = 10000.0;

/* Saved hook values in case of unload */
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* Is the current query sampled, per backend */
// static bool current_query_sampled = true;

#define slow_log_record_enabled(level) \
	(!IsParallelWorker() && \
	(level) == 0)

void        _PG_init(void);
void        _PG_fini(void);


static void ssl_ProcessUtility(PlannedStmt *pstmt,
                    const char *queryString, ProcessUtilityContext context,
                    ParamListInfo params,
                    QueryEnvironment *queryEnv,
                    DestReceiver *dest,
                    bool sentToRemote,
                    char *completionTag);


static void ssl_ExecutorEnd(QueryDesc *queryDesc);
static void ssl_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
                    uint64 count, bool execute_once);
static void ssl_ExecutorFinish(QueryDesc *queryDesc);
static void ssl_ExecutorStart(QueryDesc *queryDesc, int eflags);

/*
 * Module load callback
 */
void
_PG_init(void)
{
    /* Define custom GUC variables. */
    // DefineCustomRealVariable("auto_explain.sample_rate",
    //                          "Fraction of queries to process.",
    //                          NULL,
    //                          &auto_explain_sample_rate,
    //                          1.0,
    //                          0.0,
    //                          1.0,
    //                          PGC_SUSET,
    //                          0,
    //                          NULL,
    //                          NULL,
    //                          NULL);

    // EmitWarningsOnPlaceholders("auto_explain");

    /* Install hooks. */

    prev_ProcessUtility_hook = ProcessUtility_hook; 
    ProcessUtility_hook = ssl_ProcessUtility;

    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = ssl_ExecutorStart;

    prev_ExecutorRun_hook = ExecutorRun_hook;
    ExecutorRun_hook = ssl_ExecutorRun;

    prev_ExecutorFinish_hook = ExecutorFinish_hook;
    ExecutorFinish_hook = ssl_ExecutorFinish;

    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = ssl_ExecutorEnd;

    // prev_ExecutorEnd = ExecutorEnd_hook;
    // ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
    /* Uninstall hooks. */
    ProcessUtility_hook = prev_ProcessUtility_hook; 
    ExecutorStart_hook = prev_ExecutorStart_hook;
    ExecutorRun_hook = prev_ExecutorRun_hook;
    ExecutorFinish_hook = prev_ExecutorFinish_hook;
    ExecutorEnd_hook = prev_ExecutorEnd_hook;
    // ExecutorFinish_hook = prev_ExecutorFinish;
    // ExecutorEnd_hook = prev_ExecutorEnd;
}


/**
 * slow_sql_recorder_process_utiliti_hook
*/
static void 
ssl_ProcessUtility(PlannedStmt *pstmt,
                    const char *queryString, ProcessUtilityContext context,
                    ParamListInfo params,
                    QueryEnvironment *queryEnv,
                    DestReceiver *dest,
                    bool sentToRemote,
                    char *completionTag){

}

static void 
ssl_ExecutorStart(QueryDesc *queryDesc, int eflags)
{

    if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

    /*
	 * If query has queryId zero, don't track it.  This prevents double
	 * counting of optimizable statements that are directly contained in
	 * utility statements.
	 */
	if (slow_log_record_enabled(nesting_level) && queryDesc->plannedstmt->queryId != UINT64CONST(0))
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;
			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}


/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
ssl_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
                    uint64 count, bool execute_once)
{
    nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorRun_hook)
            prev_ExecutorRun_hook(queryDesc, direction, count, execute_once);
        else
            standard_ExecutorRun(queryDesc, direction, count, execute_once);
        nesting_level--;
    }
    PG_CATCH();
    {
        nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
ssl_ExecutorFinish(QueryDesc *queryDesc)
{
    nesting_level++;
    PG_TRY();
    {
        if (prev_ExecutorFinish_hook)
            prev_ExecutorFinish_hook(queryDesc);
        else
            standard_ExecutorFinish(queryDesc);
        nesting_level--;
    }
    PG_CATCH();
    {
        nesting_level--;
        PG_RE_THROW();
    }
    PG_END_TRY();
}


static void
ssl_ExecutorEnd(QueryDesc *queryDesc){
    if (queryDesc->totaltime && slow_log_record_enabled(nesting_level))
    {
        double        msec;

        /*
         * Make sure stats accumulation is done.  (Note: it's okay if several
         * levels of hook all do this.)
         */
        InstrEndLoop(queryDesc->totaltime);

        /* Log plan if duration is exceeded. */
        msec = queryDesc->totaltime->total * 1000.0;
        if (msec >= max_query_duration)
        {
            ExplainState *es = NewExplainState();

            es->analyze = queryDesc->instrument_options;
            es->verbose = false;
            es->buffers = es->analyze;
            es->timing = es->analyze;
            es->summary = es->analyze;
            es->format = EXPLAIN_FORMAT_TEXT;

            ExplainBeginOutput(es);
            ExplainQueryText(es, queryDesc);
            ExplainPrintPlan(es, queryDesc);
            if (es->analyze)
                ExplainPrintTriggers(es, queryDesc);
            ExplainEndOutput(es);

            /* Remove last line break */
            if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
                es->str->data[--es->str->len] = '\0';

            /*
             * Note: we rely on the existing logging of context or
             * debug_query_string to identify just which statement is being
             * reported.  This isn't ideal but trying to do it here would
             * often result in duplication.
             */
            ereport(LOG,
                    (errmsg("[slow_sql_recorder] execution duration: %.3f, ms:\n%s", 
                        msec, es->str->data),
                     errhidestmt(true)));

            pfree(es->str->data);
        }
    }

    if (prev_ExecutorEnd_hook)
        prev_ExecutorEnd_hook(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}