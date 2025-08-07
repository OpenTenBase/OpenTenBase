/*-------------------------------------------------------------------------
 *
 * explain.c
 *	  Explain query execution plans
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/prepare.h"
#include "executor/nodeHash.h"
#include "foreign/fdwapi.h"
#include "jit/jit.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "catalog/pgxc_node.h"
#include "nodes/makefuncs.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "nodes/plannodes.h"
#include "commands/explain_dist.h"
#include "executor/execFragment.h"
#include "executor/resultCache.h"
#include "optimizer/planmain.h"
#include "optimizer/memctl.h"
#include "optimizer/spm.h"
#include "optimizer/spm_cache.h"
#include "utils/queryjumble.h"


/* Hook for plugins to get control in ExplainOneQuery() */
ExplainOneQuery_hook_type ExplainOneQuery_hook = NULL;

/* Hook for plugins to get control in explain_get_index_name() */
explain_get_index_name_hook_type explain_get_index_name_hook = NULL;


/* OR-able flags for ExplainXMLTag() */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4

#define WARNING_STR_LEN 512

/* indicate generating data node plan */ 
bool remote_session = false;
int nestloop_deviation_factor = 3;
int nestloop_deviation_threshold = 100000;
int nestloop_outer_rows_threshold = 100000;
int nestloop_inner_rows_threshold = 10000;
int hashjoin_inner_rows_factor = 10;
int hashjoin_inner_rows_threshold = 100000;

static void ExplainOneQuery(Query *query, int cursorOptions,
							IntoClause *into, ExplainState *es,
							const char *queryString, ParamListInfo params,
							QueryEnvironment *queryEnv);
static void ExplainPrintJIT(ExplainState *es, int jit_flags,
							JitInstrumentation *ji);
static void report_triggers(ResultRelInfo *rInfo, bool show_relname,
				ExplainState *es);
static double elapsed_time(instr_time *starttime);
static bool ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used);
static void show_plan_tlist(PlanState *planstate, List *ancestors,
				ExplainState *es);
static void show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es);
static void show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es);
static void show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es);
static void show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es);
static void show_sort_keys(SortState *sortstate, List *ancestors,
			   ExplainState *es);
static void show_simple_sort(PlanState *remotestate,
			   List *ancestors, ExplainState *es);
static void show_mem_usage(PlanState *planstate, ExplainState *es);
static void show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ExplainState *es);
static void show_agg_keys(AggState *astate, List *ancestors,
			  ExplainState *es);
static void show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ExplainState *es);
static void show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ExplainState *es);
static void show_group_keys(GroupState *gstate, List *ancestors,
				ExplainState *es);
static void show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es);
static void show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst);
static void show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ExplainState *es);
static void show_sort_info(SortState *sortstate, ExplainState *es);
static void show_hash_info(HashState *hashstate, ExplainState *es);
static void show_hashagg_info(AggState *hashstate, ExplainState *es);
static void show_tidbitmap_info(BitmapHeapScanState *planstate,
					ExplainState *es);
static void show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es);
static void show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es);
static void show_eval_params(Bitmapset *bms_params, ExplainState *es);
static void show_buffer_usage(ExplainState *es, const BufferUsage *usage,
							  bool planning);
static void ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es);
static void ExplainScanTarget(Scan *plan, ExplainState *es);
static void ExplainModifyTarget(ModifyTable *plan, ExplainState *es);
static void ExplainTargetRel(Plan *plan, Index rti, ExplainState *es);
static void show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
								  ExplainState *es);
static void show_modifytable_merge_info(Plan *plan, PlanState* planstate,
										List *ancestors, ExplainState* es);
static ExplainWorkersState *ExplainCreateWorkersState(int num_workers);
static void ExplainOpenWorker(int n, ExplainState *es);
static void ExplainCloseWorker(int n, ExplainState *es);
static void ExplainFlushWorkersState(ExplainState *es);
static void ExplainProperty(const char *qlabel, const char *unit,
							const char *value, bool numeric, ExplainState *es, 
							bool warning);
static void ExplainOpenSetAsideGroup(const char *objtype, const char *labelname,
									 bool labeled, int depth, ExplainState *es);
static void ExplainSaveGroup(ExplainState *es, int depth, int *state_save);
static void ExplainRestoreGroup(ExplainState *es, int depth, int *state_save);
static void ExplainDummyGroup(const char *objtype, const char *labelname,
				  ExplainState *es);
static void ExplainExecNodes(ExecNodes *en, ExplainState *es);
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es);
static void ExplainXMLTagInternal(const char *tagname, int flags, 
									StringInfo str, int indent);
static void ExplainIndentText(ExplainState *es);
static void ExplainIndentTextInternal(StringInfo str, ExplainFormat format, 
										int indent);
static void ExplainJSONLineEnding(ExplainState *es);
static void ExplainJSONLineEndingInternal(StringInfo str, 
											ExplainFormat format, 
											List *grouping_stack);
static void ExplainYAMLLineStarting(ExplainState *es);
static void ExplainYAMLLineStartingInternal(StringInfo str, 
											ExplainFormat format, 
											List *grouping_stack, 
											int indent);
static void escape_yaml(StringInfo buf, const char *str);
static void show_mergequalproj_info(MergeQualProjState *mqpstate, List *ancestors,
                                                    ExplainState *es);
static void show_remotemodifytable_info(RemoteModifyTableState *mtstate,
										List *ancestors, ExplainState *es);
static void ExplainPropertyTextInternal(const char *qlabel, const char *value, 
							ExplainState *es, bool warning);
static void check_mergejoin_all_sorted(MergeJoin *plan, ExplainState *es);
static void check_nestloop_joinkey(NestLoop *plan, ExplainState *es);
static void check_nestloop_outer_row(Plan *plan, ExplainState *es);
static void check_instrument_rows(PlanState *planstate, ExplainState *es);
static void ExplainSPMWork(ExplainState *es);
/* end ora_compatible */

/*
 * ExplainQuery -
 *	  execute an EXPLAIN command
 */
void
ExplainQuery(ParseState *pstate, ExplainStmt *stmt, const char *queryString,
			 ParamListInfo params, QueryEnvironment *queryEnv,
			 DestReceiver *dest)
{
	ExplainState *es = NewExplainState();
	TupOutputState *tstate;
	List	   *rewritten;
	ListCell   *lc;
	bool		timing_set = false;
	bool		summary_set = false;

	/* Parse options list. */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "analyze") == 0)
			es->analyze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "verbose") == 0)
			es->verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "costs") == 0)
			es->costs = defGetBoolean(opt);
		else if (strcmp(opt->defname, "buffers") == 0)
			es->buffers = defGetBoolean(opt);
		else if (strcmp(opt->defname, "spm") == 0)
		{
			char *sval;
			if (opt->arg == NULL)
				es->spm = SPM_ONLY;
			else
			{
				sval = defGetString(opt);

				if (pg_strcasecmp(sval, "true") == 0)
					es->spm = SPM_ONLY;
				if (pg_strcasecmp(sval, "false") == 0)
					es->spm = SPM_OFF;
				if (pg_strcasecmp(sval, "on") == 0)
					es->spm = SPM_ONLY;
				if (pg_strcasecmp(sval, "off") == 0)
					es->spm = SPM_OFF;
				if (pg_strcasecmp(sval, "apply") == 0)
				{
					es->spm = SPM_APPLY;
					save_spmplan_state |= SAVE_SPMPLAN_APPLY;
				}
				if (pg_strcasecmp(sval, "capture") == 0)
				{
					es->spm = SPM_CAPTURE;
					save_spmplan_state |= SAVE_SPMPLAN_CAPTURE;
				}
			}
		}
		else if (strcmp(opt->defname, "hint") == 0)
		/* Allow hint option for pg_plan_advsr */
		{} 
#ifdef PGXC
		else if (strcmp(opt->defname, "nodes") == 0)
			es->nodes = defGetBoolean(opt);
		else if (strcmp(opt->defname, "num_nodes") == 0)
			es->num_nodes = defGetBoolean(opt);
#endif /* PGXC */
		else if (strcmp(opt->defname, "settings") == 0)
			es->settings = defGetBoolean(opt);
		else if (strcmp(opt->defname, "timing") == 0)
		{
			timing_set = true;
			es->timing = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "summary") == 0)
		{
			summary_set = true;
			es->summary = defGetBoolean(opt);
		}
		else if (strcmp(opt->defname, "format") == 0)
		{
			char	   *p = defGetString(opt);

			if (strcmp(p, "text") == 0)
				es->format = EXPLAIN_FORMAT_TEXT;
			else if (strcmp(p, "xml") == 0)
				es->format = EXPLAIN_FORMAT_XML;
			else if (strcmp(p, "json") == 0)
				es->format = EXPLAIN_FORMAT_JSON;
			else if (strcmp(p, "yaml") == 0)
				es->format = EXPLAIN_FORMAT_YAML;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognized value for EXPLAIN option \"%s\": \"%s\"",
								opt->defname, p),
						 parser_errposition(pstate, opt->location)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized EXPLAIN option \"%s\"",
							opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	save_spmplan_state |= SAVE_SPMPLAN_EXPLAIN;

	/* if the timing was not set explicitly, set default value */
	es->timing = (timing_set) ? es->timing : es->analyze;

	/* check that timing is used with EXPLAIN ANALYZE */
	if (es->timing && !es->analyze)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("EXPLAIN option TIMING requires ANALYZE")));

	/* if the summary was not set explicitly, set default value */
	es->summary = (summary_set) ? es->summary : es->analyze;

	/*
	 * Parse analysis was done already, but we still have to run the rule
	 * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
	 * came straight from the parser, or suitable locks were acquired by
	 * plancache.c.
	 *
	 * Because the rewriter and planner tend to scribble on the input, we make
	 * a preliminary copy of the source querytree.  This prevents problems in
	 * the case that the EXPLAIN is in a portal or plpgsql function and is
	 * executed repeatedly.  (See also the same hack in DECLARE CURSOR and
	 * PREPARE.)  XXX FIXME someday.
	 */
	rewritten = QueryRewrite(castNode(Query, copyObject(stmt->query)));

	/* emit opening boilerplate */
	ExplainBeginOutput(es);

	if (rewritten == NIL)
	{
		/*
		 * In the case of an INSTEAD NOTHING, tell at least that.  But in
		 * non-text format, the output is delimited, so this isn't necessary.
		 */
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, "Query rewrites to nothing\n");
	}
	else
	{
		ListCell   *l;

		/* Explain every plan */
		foreach(l, rewritten)
		{
			Query *query = lfirst_node(Query, l);

			ExplainOneQuery(query,
							CURSOR_OPT_PARALLEL_OK, NULL, es,
							queryString, params, queryEnv);

			/* Separate plans with an appropriate separator */
			if (lnext(l) != NULL)
				ExplainSeparatePlans(es);
		}
	}

	/* emit closing boilerplate */
	ExplainEndOutput(es);
	Assert(es->indent == 0);

	/* output tuples */
	tstate = begin_tup_output_tupdesc(dest, ExplainResultDesc(stmt));
	if (es->format == EXPLAIN_FORMAT_TEXT)
		do_text_output_multiline(tstate, es->str->data);
	else
		do_text_output_oneline(tstate, es->str->data);
	end_tup_output(tstate);

	pfree(es->str->data);
}

#ifdef __OPENTENBASE__
void
ExplainQueryFastCachedError(QueryDesc *queryDesc, bool warning)
{
	PG_TRY();
	{
		ExplainQueryFast(queryDesc, warning);
	}
	PG_CATCH();
	{
		HOLD_INTERRUPTS();
		EmitErrorReport();
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
}
/*
 * ExplainQueryFast -
 *	  Explain the queryDesc and print to log file.
 */
void
ExplainQueryFast(QueryDesc *queryDesc, bool warning)
{
	ExplainState *es = NULL;
	PlannedStmt *stmt = NULL;

	if (!queryDesc || !queryDesc->planstate)
		return;

	stmt = queryDesc->plannedstmt;

	/*
	 * Skip printing FQS plan, since it will cause remote execution.
	 * We just print local plan here
	 */
	if (stmt && stmt->planTree && IsA(stmt->planTree, RemoteQuery))
		return;

	es = NewExplainState();
	if (warning)
	{
		if (queryDesc->instrument_options & INSTRUMENT_SQL_WARN)	
			es->warning_str = makeStringInfo();
		else
			return;
	}

	/*
	 * Currently, this is only used for printing query plan when got error, so
	 * the ExplainState configurations are hard coded. Improve the flexibility
	 * of this API if the scope is extended.
	 */
	es->analyze = false;
	es->verbose = true;
	es->buffers = false;
	es->timing = false;
	es->summary = false;
	es->format = EXPLAIN_FORMAT_TEXT;

	ExplainBeginOutput(es);
	ExplainQueryText(es, queryDesc);
	ExplainPrintPlan(es, queryDesc);
	ExplainEndOutput(es);

	/* Print to log file without duplicate statement printed. */
	if (es->warning_str)
	{
		ereport(LOG,
			(errmsg("explain query plan:\n%s%s", es->str->data, 
					es->warning_str->data), errhidestmt(true)));
		pfree(es->warning_str->data);
	}
	else
	{
		ereport(LOG,
				(errmsg("explain query plan:\n%s", es->str->data),
				errhidestmt(true)));
		pfree(es->str->data);
	}		
}
#endif

/*
 * Create a new ExplainState struct initialized with default options.
 */
ExplainState *
NewExplainState(void)
{
	ExplainState *es = (ExplainState *) palloc0(sizeof(ExplainState));

	/* Set default options (most fields can be left as zeroes). */
	es->costs = true;
#ifdef PGXC
	es->nodes = true;
#endif /* PGXC */
	/* Prepare output buffer. */
	es->str = makeStringInfo();
	es->warning_str = NULL;
	es->jumble_const = false;

	return es;
}

/*
 * ExplainResultDesc -
 *	  construct the result tupledesc for an EXPLAIN
 */
TupleDesc
ExplainResultDesc(ExplainStmt *stmt)
{
	TupleDesc	tupdesc;
	ListCell   *lc;
	Oid			result_type = TEXTOID;

	/* Check for XML format option */
	foreach(lc, stmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		if (strcmp(opt->defname, "format") == 0)
		{
			char	   *p = defGetString(opt);

			if (strcmp(p, "xml") == 0)
				result_type = XMLOID;
			else if (strcmp(p, "json") == 0)
				result_type = JSONOID;
			else
				result_type = TEXTOID;
			/* don't "break", as ExplainQuery will use the last value */
		}
	}

	/* Need a tuple descriptor representing a single TEXT or XML column */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "QUERY PLAN",
					   result_type, -1, 0);
	return tupdesc;
}

/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
static void
ExplainOneQuery(Query *query, int cursorOptions,
				IntoClause *into, ExplainState *es,
				const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv)
{
	/* planner will not cope with utility statements */
	if (query->commandType == CMD_UTILITY)
	{
		/*
		 * If we are running EXPLAIN ANALYZE, transform the CTAS such that the
		 * target table is created first and select result is inserted into the
		 * table. The EXPLAIN ANALYZE would really just show the plan for the
		 * INSERT INTO generated by QueryRewriteCTAS, but that's OK.
		 */
		if (es->analyze && IsA(query->utilityStmt, CreateTableAsStmt))
		{
			CreateTableAsStmt *ctas = (CreateTableAsStmt *) query->utilityStmt;
			List	   *rewritten = NULL;

			if (ctas->relkind == OBJECT_MATVIEW)
				elog(ERROR, "explain analyze create materialized view is not supported");

			/*
			* Check if the relation exists or not.  This is done at this stage to
			* avoid query planning or execution.
			*/
			if (CreateTableAsRelExists(ctas))
			{
				if (ctas->relkind == OBJECT_TABLE)
					ExplainDummyGroup("CREATE TABLE AS", NULL, es);
				else
					elog(ERROR, "unexpected object type: %d",
						(int) ctas->relkind);
				return;
			}

			rewritten = QueryRewriteCTAS(query, NULL, NULL, queryEnv, NULL, 0);
			Assert(list_length(rewritten) == 1);
			ExplainOneQuery((Query *) linitial(rewritten), cursorOptions,
					into, es, queryString, params, queryEnv);
		}
		else
			ExplainOneUtility(query->utilityStmt, into, es,
					queryString, params, queryEnv);
		return;
	}

	/* if an advisor plugin is present, let it manage things */
	if (ExplainOneQuery_hook)
		(*ExplainOneQuery_hook) (query, cursorOptions, into, es,
								 queryString, params);
	else
	{
		PlannedStmt *plan;
		instr_time	planstart,
					planduration;
		BufferUsage bufusage_start,
					bufusage;

		if (es->buffers)
			bufusage_start = pgBufferUsage;
		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		plan = pg_plan_query(query, cursorOptions, params, 
								params? params->numParams  :0, 
								!es->analyze);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);

		/* calc differences of buffer counters. */
		if (es->buffers)
		{
			memset(&bufusage, 0, sizeof(BufferUsage));
			BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
		}

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
					   &planduration, (es->buffers ? &bufusage : NULL));
	}
}

/*
 * ExplainOneUtility -
 *	  print out the execution plan for one utility statement
 *	  (In general, utility statements don't have plans, but there are some
 *	  we treat as special cases)
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case.
 */
void
ExplainOneUtility(Node *utilityStmt, IntoClause *into, ExplainState *es,
				  const char *queryString, ParamListInfo params,
				  QueryEnvironment *queryEnv)
{
	if (utilityStmt == NULL)
		return;

	if (IsA(utilityStmt, CreateTableAsStmt))
	{
		/*
		 * We have to rewrite the contained SELECT and then pass it back to
		 * ExplainOneQuery.  It's probably not really necessary to copy the
		 * contained parsetree another time, but let's be safe.
		 */
		CreateTableAsStmt *ctas = (CreateTableAsStmt *) utilityStmt;
		List	   *rewritten;

		rewritten = QueryRewrite(castNode(Query, copyObject(ctas->query)));
		Assert(list_length(rewritten) == 1);
		ExplainOneQuery(linitial_node(Query, rewritten),
						CURSOR_OPT_PARALLEL_OK, ctas->into, es,
						queryString, params, queryEnv);
	}
	else if (IsA(utilityStmt, DeclareCursorStmt))
	{
		/*
		 * Likewise for DECLARE CURSOR.
		 *
		 * Notice that if you say EXPLAIN ANALYZE DECLARE CURSOR then we'll
		 * actually run the query.  This is different from pre-8.3 behavior
		 * but seems more useful than not running the query.  No cursor will
		 * be created, however.
		 */
		DeclareCursorStmt *dcs = (DeclareCursorStmt *) utilityStmt;
		List	   *rewritten;

		rewritten = QueryRewrite(castNode(Query, copyObject(dcs->query)));
		Assert(list_length(rewritten) == 1);
		ExplainOneQuery(linitial_node(Query, rewritten),
						dcs->options, NULL, es,
						queryString, params, queryEnv);
	}
	else if (IsA(utilityStmt, ExecuteStmt))
		ExplainExecuteQuery((ExecuteStmt *) utilityStmt, into, es,
							queryString, params, queryEnv);
	else if (IsA(utilityStmt, NotifyStmt))
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str, "NOTIFY\n");
		else
			ExplainDummyGroup("Notify", NULL, es);
	}
	else
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfoString(es->str,
								   "Utility statements have no plan structure\n");
		else
			ExplainDummyGroup("Utility Statement", NULL, es);
	}
}

/*
 * ExplainOnePlan -
 *		given a planned query, execute it if needed, and then print
 *		EXPLAIN output
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt,
 * in which case executing the query should result in creating that table.
 *
 * This is exported because it's called back from prepare.c in the
 * EXPLAIN EXECUTE case, and because an index advisor plugin would need
 * to call it.
 */
void
ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es,
			   const char *queryString, ParamListInfo params,
			   QueryEnvironment *queryEnv, const instr_time *planduration,
			   const BufferUsage *bufusage)
{
	DestReceiver *dest;
	QueryDesc  *queryDesc;
	instr_time	starttime;
	double		totaltime = 0;
	int			eflags;
	int			instrument_option = 0;

	Assert(plannedstmt->commandType != CMD_UTILITY);

	if (es->analyze && es->timing)
		instrument_option |= INSTRUMENT_TIMER;
	else if (es->analyze)
		instrument_option |= INSTRUMENT_ROWS;

	if (es->buffers)
		instrument_option |= INSTRUMENT_BUFFERS;

	/*
	 * We always collect timing for the entire statement, even when node-level
	 * timing is off, so we don't look at es->timing here.  (We could skip
	 * this if !es->summary, but it's hardly worth the complication.)
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/*
	 * Normally we discard the query's output, but if explaining CREATE TABLE
	 * AS, we'd better use the appropriate tuple receiver.
	 */
	if (into)
		dest = CreateIntoRelDestReceiver(into);
	else
		dest = None_Receiver;

	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(plannedstmt, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, instrument_option);

	/* Select execution options */
	if (es->analyze)
		eflags = 0;				/* default run-to-completion flags */
	else
		eflags = EXEC_FLAG_EXPLAIN_ONLY;
	if (into)
		eflags |= GetIntoRelEFlags(into);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, eflags);

#ifdef __OPENTENBASE_C__
	if (es->analyze && queryDesc->estate->es_distributed &&
		IS_PGXC_COORDINATOR && (es->format == EXPLAIN_FORMAT_TEXT || es->format == EXPLAIN_FORMAT_JSON))
		/* Display remote instrument from DN, support text only */
		InitRemoteInstr();
	else
		/* In case it is not reset for error last time */
		ResetRemoteInstr();
#endif

	PG_TRY();
	{
		/* Execute the plan for statistics if asked for */
		if (es->analyze)
		{
			ScanDirection dir;

			/* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
			if (into && into->skipData)
				dir = NoMovementScanDirection;
			else
				dir = ForwardScanDirection;

			/* run the plan */
			ExecutorRun(queryDesc, dir, 0L, true);

			/* run cleanup too */
			ExecutorFinish(queryDesc);

			/* We can't run ExecutorEnd 'till we're done printing the stats... */
			totaltime += elapsed_time(&starttime);
		}

		ExplainOpenGroup("Query", NULL, true, es);

		/* Create textual dump of plan tree */
		ExplainPrintPlan(es, queryDesc);

		/* Show buffer usage in planning */
		if (bufusage)
		{
			ExplainOpenGroup("Planning", "Planning", true, es);
			show_buffer_usage(es, bufusage, true);
			ExplainCloseGroup("Planning", "Planning", true, es);
		}

		if (es->summary && planduration)
		{
			double plantime = INSTR_TIME_GET_DOUBLE(*planduration);

			ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
		}

		/* Print info about runtime of triggers */
		if (es->analyze)
			ExplainPrintTriggers(es, queryDesc);

		/*
		 * Print info about JITing. Tied to es->costs because we don't want to
		 * display this in regression tests, as it'd cause output differences
		 * depending on build options.  Might want to separate that out from COSTS
		 * at a later stage.
		 */
		if (es->costs)
			ExplainPrintJITSummary(es, queryDesc);

		/* Show query_mem info */
		if (es->summary)
			ExplainQueryMem(es, queryDesc);

		if (es->spm > SPM_OFF)
		{
			ExplainSpmHints(es, queryDesc);
		}

		ExplainSPMWork(es);

		/*
		 * Close down the query and free resources.  Include time for this in the
		 * total execution time (although it should be pretty minimal).
		 */
		INSTR_TIME_SET_CURRENT(starttime);

		ExecutorEnd(queryDesc);
	}
	PG_CATCH();
	{
		ResetRemoteInstr();
		PG_RE_THROW();
	}
	PG_END_TRY();

	ResetRemoteInstr();
	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	/* We need a CCI just in case query expanded to multiple plans */
	if (es->analyze)
		CommandCounterIncrement();

	totaltime += elapsed_time(&starttime);

	/*
	 * We only report execution time if we actually ran the query (that is,
	 * the user specified ANALYZE), and if summary reporting is enabled (the
	 * user can set SUMMARY OFF to not have the timing information included in
	 * the output).  By default, ANALYZE sets SUMMARY to true.
	 */
	if (es->summary && es->analyze)
		ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3,
							 es);

	ExplainCloseGroup("Query", NULL, true, es);
}

/*
 * ExplainPrintSettings -
 *    Print summary of modified settings affecting query planning.
 */
static void
ExplainPrintSettings(ExplainState *es)
{
	int		num;
	struct config_generic **gucs;

	/* bail out if information about settings not requested */
	if (!es->settings)
		return;

	/* request an array of relevant settings */
	gucs = get_explain_guc_options(&num);

	/* also bail out of there are no options */
	if (!num)
		return;

	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		int		i;

		ExplainOpenGroup("Settings", "Settings", true, es);

		for (i = 0; i < num; i++)
		{
			char *setting;
			struct config_generic *conf = gucs[i];

			setting = GetConfigOptionByName(conf->name, NULL, true);

			ExplainPropertyText(conf->name, setting, es);
		}

		ExplainCloseGroup("Settings", "Settings", true, es);
	}
	else
	{
		int		i;
		StringInfoData	str;

		initStringInfo(&str);

		for (i = 0; i < num; i++)
		{
			char *setting;
			struct config_generic *conf = gucs[i];

			if (i > 0)
				appendStringInfoString(&str, ", ");

			setting = GetConfigOptionByName(conf->name, NULL, true);

			if (setting)
				appendStringInfo(&str, "%s = '%s'", conf->name, setting);
			else
				appendStringInfo(&str, "%s = NULL", conf->name);
		}

		if (num > 0)
			ExplainPropertyText("Settings", str.data, es);
	}
}

/*
 * InitPrintedInitplanHtab
 *
 * Initialize the hash table for initplans from remote.
 * Current memory context is PortalHeapMemory.
 */
HTAB *
InitPrintedInitplanHtab(void)
{
	HTAB	*printed_initplan_htab;
	HASHCTL hash_ctl;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	/* key is fid */
	hash_ctl.keysize = sizeof(int);
	/* entry is a bitmap that recoreded printed plans pan_node_id */
	hash_ctl.entrysize = sizeof(PrintedInitPlanEntry);
	hash_ctl.hcxt = CurrentMemoryContext;
	printed_initplan_htab = hash_create("Remote Instruments", 8, &hash_ctl,
									 HASH_ELEM | HASH_CONTEXT);
	return printed_initplan_htab;
}

/*
 * CheckInitplanPrinted
 *
 * Check whether the plan be printed or not
 */
bool
CheckInitplanPrinted(int fid, int plan_node_id, HTAB *printed_initplan_htab)
{
	bool					found = false;
	PrintedInitPlanEntry	*entry;

	if (printed_initplan_htab == NULL)
		return false;

	entry = (PrintedInitPlanEntry *) hash_search(printed_initplan_htab, &fid, 
													HASH_FIND, &found);

	if (found)
	{
		return bms_is_member(plan_node_id, entry->printed_initplans);
	}

	return false;
}

/*
 * RecordInitplanPrinted
 *
 * Record the plan in hash table
 */
void
RecordInitplanPrinted(int fid, int plan_node_id, HTAB *printed_initplan_htab)
{
	bool					found = false;
	PrintedInitPlanEntry	*entry;
	MemoryContext			oldcontext;

	if (printed_initplan_htab == NULL)
		return;

	entry = (PrintedInitPlanEntry *) hash_search(printed_initplan_htab, &fid, 
													HASH_ENTER, &found);

	if (!found)
		entry->printed_initplans = NULL;

	oldcontext = MemoryContextSwitchTo(hash_get_memcxt(printed_initplan_htab));
	entry->printed_initplans = bms_add_member(entry->printed_initplans, plan_node_id);
	MemoryContextSwitchTo(oldcontext);
}

/*
 * ResetPrintedInitplanHtab
 * Remove all the entrys in printed_initplan_htab
 */
void
ResetPrintedInitplanHtab(HTAB *printed_initplan_htab)
{
	HASH_SEQ_STATUS seq;
	PrintedInitPlanEntry	*entry;

	if (printed_initplan_htab == NULL)
		return;

	/* walk over cache */
	hash_seq_init(&seq, printed_initplan_htab);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		/* Now we can remove the hash table entry */
		hash_search(printed_initplan_htab, &entry->fid, HASH_REMOVE, NULL);
		bms_free(entry->printed_initplans);
	}
}

/*
 * ExplainPrintPlan -
 *	  convert a QueryDesc's plan tree to text and append it to es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Also, output formatting state
 * such as the indent level is assumed valid.  Plan-tree-specific fields
 * in *es are initialized here.
 *
 * NB: will not work on utility statements
 */
void
ExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc)
{
	Bitmapset  *rels_used = NULL;
	PlanState  *ps;

	/* Set up ExplainState fields associated with this plan tree */
	Assert(queryDesc->plannedstmt != NULL);
	es->pstmt = queryDesc->plannedstmt;
	es->rtable = queryDesc->plannedstmt->rtable;
	ExplainPreScanNode(queryDesc->planstate, &rels_used);
	es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_rtable(es->rtable,
													  es->rtable_names);
	es->printed_subplans = NULL;
	es->itable = NULL;
	es->printed_initplan_htab = InitPrintedInitplanHtab();

	/*
	 * Sometimes we mark a Gather node as "invisible", which means that it's
	 * not to be displayed in EXPLAIN output.  The purpose of this is to allow
	 * running regression tests with force_parallel_mode=regress to get the
	 * same results as running the same tests with force_parallel_mode=off.
	 * Such marking is currently only supported on a Gather at the top of the
	 * plan.  We skip that node, and we must also hide per-worker detail data
	 * further down in the plan tree.
	 */
	ps = queryDesc->planstate;
	if (IsA(ps, GatherState) &&((Gather *) ps->plan)->invisible)
	{
		ps = outerPlanState(ps);
		es->hide_workers = true;
	}
	ExplainNode(ps, NIL, NULL, NULL, es);

	/*
	 * If requested, include information about GUC parameters with values
	 * that don't match the built-in defaults.
	 */
	ExplainPrintSettings(es);
}

/*
 * ExplainPrintTriggers -
 *	  convert a QueryDesc's trigger statistics to text and append it to
 *	  es->str
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.  Other fields in *es are
 * initialized here.
 */
void
ExplainPrintTriggers(ExplainState *es, QueryDesc *queryDesc)
{
	ResultRelInfo *rInfo;
	bool		show_relname;
	int			numrels = queryDesc->estate->es_num_result_relations;
	List	   *targrels = queryDesc->estate->es_trig_target_relations;
	int			nr;
	ListCell   *l;

	ExplainOpenGroup("Triggers", "Triggers", false, es);

	show_relname = (numrels > 1 || targrels != NIL);
	rInfo = queryDesc->estate->es_result_relations;
	for (nr = 0; nr < numrels; rInfo++, nr++)
		report_triggers(rInfo, show_relname, es);

	foreach(l, targrels)
	{
		rInfo = (ResultRelInfo *) lfirst(l);
		report_triggers(rInfo, show_relname, es);
	}

	ExplainCloseGroup("Triggers", "Triggers", false, es);
}

/*
 * ExplainPrintJITSummary -
 *    Print summarized JIT instrumentation from leader and workers
 */
void
ExplainPrintJITSummary(ExplainState *es, QueryDesc *queryDesc)
{
	JitInstrumentation ji = {0};

	if (!(queryDesc->estate->es_jit_flags & PGJIT_PERFORM))
		return;

	/*
	 * Work with a copy instead of modifying the leader state, since this
	 * function may be called twice
	 */
	if (queryDesc->estate->es_jit)
		InstrJitAgg(&ji, &queryDesc->estate->es_jit->instr);

	/* If this process has done JIT in parallel workers, merge stats */
	if (queryDesc->estate->es_jit_worker_instr)
		InstrJitAgg(&ji, queryDesc->estate->es_jit_worker_instr);

	ExplainPrintJIT(es, queryDesc->estate->es_jit_flags, &ji);
}

/*
 * ExplainPrintJIT -
 *	  Append information about JITing to es->str.
 */
static void
ExplainPrintJIT(ExplainState *es, int jit_flags, JitInstrumentation *ji)
{
	instr_time	total_time;

	/* don't print information if no JITing happened */
	if (!ji || ji->created_functions == 0)
		return;

	/* calculate total time */
	INSTR_TIME_SET_ZERO(total_time);
	INSTR_TIME_ADD(total_time, ji->generation_counter);
	INSTR_TIME_ADD(total_time, ji->inlining_counter);
	INSTR_TIME_ADD(total_time, ji->optimization_counter);
	INSTR_TIME_ADD(total_time, ji->emission_counter);

	ExplainOpenGroup("JIT", "JIT", true, es);

	/* for higher density, open code the text output format */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		ExplainIndentText(es);
		appendStringInfoString(es->str, "JIT:\n");
		es->indent++;

		ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

		ExplainIndentText(es);
		appendStringInfo(es->str, "Options: %s %s, %s %s, %s %s, %s %s\n",
						 "Inlining", jit_flags & PGJIT_INLINE ? "true" : "false",
						 "Optimization", jit_flags & PGJIT_OPT3 ? "true" : "false",
						 "Expressions", jit_flags & PGJIT_EXPR ? "true" : "false",
						 "Deforming", jit_flags & PGJIT_DEFORM ? "true" : "false");

		if (es->analyze && es->timing)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Timing: %s %.3f ms, %s %.3f ms, %s %.3f ms, %s %.3f ms, %s %.3f ms\n",
							 "Generation", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
							 "Inlining", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
							 "Optimization", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
							 "Emission", 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
							 "Total", 1000.0 * INSTR_TIME_GET_DOUBLE(total_time));
		}

		es->indent--;
	}
	else
	{
		ExplainPropertyInteger("Functions", NULL, ji->created_functions, es);

		ExplainOpenGroup("Options", "Options", true, es);
		ExplainPropertyBool("Inlining", jit_flags & PGJIT_INLINE, es);
		ExplainPropertyBool("Optimization", jit_flags & PGJIT_OPT3, es);
		ExplainPropertyBool("Expressions", jit_flags & PGJIT_EXPR, es);
		ExplainPropertyBool("Deforming", jit_flags & PGJIT_DEFORM, es);
		ExplainCloseGroup("Options", "Options", true, es);

		if (es->analyze && es->timing)
		{
			ExplainOpenGroup("Timing", "Timing", true, es);

			ExplainPropertyFloat("Generation", "ms", 
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->generation_counter),
								 3, es);
			ExplainPropertyFloat("Inlining", "ms", 
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->inlining_counter),
								 3, es);
			ExplainPropertyFloat("Optimization", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->optimization_counter),
								 3, es);
			ExplainPropertyFloat("Emission", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(ji->emission_counter),
								 3, es);
			ExplainPropertyFloat("Total", "ms",
								 1000.0 * INSTR_TIME_GET_DOUBLE(total_time),
								 3, es);

			ExplainCloseGroup("Timing", "Timing", true, es);
		}
	}

	ExplainCloseGroup("JIT", "JIT", true, es);
}

/*
 * ExplainQueryText -
 *	  add a "Query Text" node that contains the actual text of the query
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.
 *
 */
void
ExplainQueryText(ExplainState *es, QueryDesc *queryDesc)
{
	if (queryDesc->sourceText)
		ExplainPropertyText("Query Text", queryDesc->sourceText, es);
}

/*
 * ExplainQueryParameters -
 *	  add a "Query Parameters" node that describes the parameters of the query
 *
 * The caller should have set up the options fields of *es, as well as
 * initializing the output buffer es->str.
 *
 */
void
ExplainQueryParameters(ExplainState *es, ParamListInfo params, int maxlen)
{
	char	   *str;

	/* This check is consistent with errdetail_params() */
	if (params == NULL || params->numParams <= 0 || maxlen == 0)
		return;

	str = BuildParamLogString(params, NULL, maxlen);
	if (str && str[0] != '\0')
		ExplainPropertyText("Query Parameters", str, es);
}

/*
 * report_triggers -
 *		report execution stats for a single relation's triggers
 */
static void
report_triggers(ResultRelInfo *rInfo, bool show_relname, ExplainState *es)
{
	int			nt;

	if (!rInfo->ri_TrigDesc || !rInfo->ri_TrigInstrument)
		return;
	for (nt = 0; nt < rInfo->ri_TrigDesc->numtriggers; nt++)
	{
		Trigger    *trig = rInfo->ri_TrigDesc->triggers + nt;
		Instrumentation *instr = rInfo->ri_TrigInstrument + nt;
		char	   *relname;
		char	   *conname = NULL;

		instr_time	starttimespan;
		double		total;
		double		ntuples;
		double		ncalls;

		if (!es->runtime)
		{
		/* Must clean up instrumentation state */
		InstrEndLoop(instr);
		}

		/* Collect statistic variables */
		if (!INSTR_TIME_IS_ZERO(instr->starttime))
		{
			INSTR_TIME_SET_CURRENT(starttimespan);
			INSTR_TIME_SUBTRACT(starttimespan, instr->starttime);
		}
		else
			INSTR_TIME_SET_ZERO(starttimespan);

		total = instr->total + INSTR_TIME_GET_DOUBLE(instr->counter)
							 + INSTR_TIME_GET_DOUBLE(starttimespan);
		ntuples = instr->ntuples + instr->tuplecount;
		if (INSTR_TIME_IS_ZERO(starttimespan) == false)
		{
			ncalls = ntuples + 1;
		}
		else
		{
			ncalls = ntuples;
		}
		

		/*
		 * We ignore triggers that were never invoked; they likely aren't
		 * relevant to the current query type.
		 */
		if (ncalls == 0)
			continue;

		ExplainOpenGroup("Trigger", NULL, true, es);

		relname = RelationGetRelationName(rInfo->ri_RelationDesc);
		if (OidIsValid(trig->tgconstraint))
			conname = get_constraint_name(trig->tgconstraint);

		/*
		 * In text format, we avoid printing both the trigger name and the
		 * constraint name unless VERBOSE is specified.  In non-text formats
		 * we just print everything.
		 */
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->verbose || conname == NULL)
				appendStringInfo(es->str, "Trigger %s", trig->tgname);
			else
				appendStringInfoString(es->str, "Trigger");
			if (conname)
				appendStringInfo(es->str, " for constraint %s", conname);
			if (show_relname)
				appendStringInfo(es->str, " on %s", relname);
			if (es->timing)
				appendStringInfo(es->str, ": time=%.3f calls=%.0f\n",
								 1000.0 * total, ncalls);
			else
				appendStringInfo(es->str, ": calls=%.0f\n", ncalls);
		}
		else
		{
			ExplainPropertyText("Trigger Name", trig->tgname, es);
			if (conname)
				ExplainPropertyText("Constraint Name", conname, es);
			ExplainPropertyText("Relation", relname, es);
			if (es->timing)
				ExplainPropertyFloat("Time", "ms", 1000.0 * total, 3, es);
			ExplainPropertyFloat("Calls", NULL, ncalls, 0, es);
		}

		if (conname)
			pfree(conname);

		ExplainCloseGroup("Trigger", NULL, true, es);
	}
}

/* Compute elapsed time in seconds since given timestamp */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}

/*
 * ExplainPreScanNode -
 *	  Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
static bool
ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, ExplainPreScanNode, rels_used);
}

/*
 * ExplainNode -
 *	  Appends a description of a plan tree to es->str
 *
 * planstate points to the executor state node for the current plan node.
 * We need to work from a PlanState node, not just a Plan node, in order to
 * get at the instrumentation data (if any) as well as the list of subplans.
 *
 * ancestors is a list of parent PlanState nodes, most-closely-nested first.
 * These are needed in order to interpret PARAM_EXEC Params.
 *
 * relationship describes the relationship of this plan node to its parent
 * (eg, "Outer", "Inner"); it can be null at top level.  plan_name is an
 * optional name to be attached to the node.
 *
 * In text format, es->indent is controlled in this function since we only
 * want it to change at plan-node boundaries (but a few subroutines will
 * transiently increment it).  In non-text formats, es->indent corresponds
 * to the nesting depth of logical output groups, and therefore is controlled
 * by ExplainOpenGroup/ExplainCloseGroup.
 */
void
ExplainNode(PlanState *planstate, List *ancestors,
			const char *relationship, const char *plan_name,
			ExplainState *es)
{
	Plan	   *plan = NULL;
	const char *pname = NULL;			/* node type name for text output */
	const char *sname = NULL;			/* node type name for non-text output */
	const char *strategy = NULL;
	const char *partialmode = NULL;
	const char *operation = NULL;
	const char *custom_name = NULL;
	ExplainWorkersState *save_workers_state = es->workers_state;
	int			save_indent = es->indent;
	bool		haschildren;
#ifdef __OPENTENBASE_C__
	RemoteInstrumentation *remote_instrs = NULL;
#endif

#ifdef __OPENTENBASE__
	if (planstate == NULL)
		return;
	plan = planstate->plan;
#endif

	/*
	 * Prepare per-worker output buffers, if needed.  We'll append the data in
	 * these to the main output string further down.
	 */
	if (planstate->worker_instrument && es->analyze && !es->hide_workers)
		es->workers_state = ExplainCreateWorkersState(planstate->worker_instrument->num_workers);
	else
		es->workers_state = NULL;

	/* Identify plan node type, and print generic details */
	switch (nodeTag(plan))
	{
		case T_Result:
			pname = sname = "Result";
			break;
		case T_ProjectSet:
			pname = sname = "ProjectSet";
			break;
		case T_ModifyTable:
			sname = "ModifyTable";
			switch (((ModifyTable *) plan)->operation)
			{
				case CMD_INSERT:
					pname = operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = operation = "Update";
					break;
				case CMD_DELETE:
					pname = operation = "Delete";
					break;
                case CMD_MERGE:
                    pname = operation = "Merge";
                    break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_RemoteModifyTable:
			sname = IS_CENTRALIZED_MODE ? "ModifyTable" : "Remote ModifyTable";
			switch (((RemoteModifyTable *) plan)->operation)
			{
				case CMD_UPDATE:
					pname = operation = IS_CENTRALIZED_MODE ? "Update" : "Remote Update";
					break;
				case CMD_INSERT:
					pname = operation = IS_CENTRALIZED_MODE ? "Insert" : "Remote Insert";
					break;
				case CMD_DELETE:
					pname = operation = IS_CENTRALIZED_MODE ? "Delete" : "Remote Delete";
					break;
				case CMD_MERGE:
					pname = operation = "Remote Merge";
					break;
				default:
					pname = "???";
					break;
			}
			break;
		case T_PartIterator:
			pname = sname = "PartIterator";
			break;
		case T_Append:
			pname = sname = "Append";
			break;
		case T_MergeAppend:
			pname = sname = "Merge Append";
			break;
		case T_RecursiveUnion:
			pname = sname = "Recursive Union";
			break;
		case T_BitmapAnd:
			pname = sname = "BitmapAnd";
			break;
		case T_BitmapOr:
			pname = sname = "BitmapOr";
			break;
		case T_NestLoop:
			pname = sname = "Nested Loop";
			break;
		case T_MergeJoin:
			pname = "Merge";	/* "Join" gets added by jointype switch */
			sname = "Merge Join";
			break;
		case T_HashJoin:
			pname = "Hash";		/* "Join" gets added by jointype switch */
			sname = "Hash Join";
			break;
		case T_SeqScan:
			pname = sname = "Seq Scan";
			break;
		case T_SampleScan:
			pname = sname = "Sample Scan";
			break;
		case T_Gather:
			pname = sname = "Gather";
			break;
		case T_GatherMerge:
			pname = sname = "Gather Merge";
			break;
		case T_IndexScan:
			pname = sname = "Index Scan";
			break;
		case T_IndexOnlyScan:
			pname = sname = "Index Only Scan";
			break;
		case T_BitmapIndexScan:
			pname = sname = "Bitmap Index Scan";
			break;
		case T_BitmapHeapScan:
			pname = sname = "Bitmap Heap Scan";
			break;
		case T_TidScan:
			if (!((TidScan *) plan)->remote || IS_CENTRALIZED_MODE)
				pname = sname = "Tid Scan";
			else
				pname = sname = "Remote Tid Scan";
			break;

		case T_SubqueryScan:
			pname = sname = "Subquery Scan";
			break;
		case T_FunctionScan:
			pname = sname = "Function Scan";
			break;
		case T_TableFuncScan:
			pname = sname = "Table Function Scan";
			break;
		case T_ValuesScan:
			pname = sname = "Values Scan";
			break;
		case T_CteScan:
			pname = sname = "CTE Scan";
			break;
		case T_NamedTuplestoreScan:
			pname = sname = "Named Tuplestore Scan";
			break;
		case T_WorkTableScan:
			pname = sname = "WorkTable Scan";
			break;
#ifdef PGXC
		case T_RemoteQuery:
			pname = sname = "Remote Fast Query Execution";
            if(((RemoteQuery *)plan)->fqs_mode > FQS_MODE_FQS_PLANER)
                pname = psprintf("%s Mode: %s", pname, fqs_mode_string[((RemoteQuery *)plan)->fqs_mode]);
			break;
#endif
		case T_ForeignScan:
			sname = "Foreign Scan";
			switch (((ForeignScan *) plan)->operation)
			{
				case CMD_SELECT:
					pname = "Foreign Scan";
					operation = "Select";
					break;
				case CMD_INSERT:
					pname = "Foreign Insert";
					operation = "Insert";
					break;
				case CMD_UPDATE:
					pname = "Foreign Update";
					operation = "Update";
					break;
				case CMD_DELETE:
					pname = "Foreign Delete";
					operation = "Delete";
					break;
				default:
					pname = "???";
					break;
			}
			break;
#ifdef XCP
		case T_RemoteSubplan:
			pname = sname = "Remote Subquery Scan";
#ifdef __OPENTENBASE_C__
			if (HasRemoteInstr())
			{
				RemoteSubplan *rs = ((RemoteSubplan *) plan);
				/* the first fid of multi-dest should be local_fid of fragment */
				int fid = list_length(rs->dests) > 0 ?
						  linitial_node(FragmentDest, rs->dests)->fid : rs->fid;
				es->fids = lappend_int(es->fids, fid);
			}
#endif
			break;
#endif /* XCP */
		case T_CustomScan:
			sname = "Custom Scan";
			custom_name = ((CustomScan *) plan)->methods->CustomName;
			if (custom_name)
				pname = psprintf("Custom Scan (%s)", custom_name);
			else
				pname = sname;
			break;
		case T_Material:
			pname = sname = "Materialize";
			break;
		case T_Sort:
			pname = sname = "Sort";
			break;
		case T_Group:
			pname = sname = "Group";
			break;
		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;

				sname = "Aggregate";
				switch (agg->aggstrategy)
				{
					case AGG_PLAIN:
						pname = "Aggregate";
						strategy = "Plain";
						break;
					case AGG_SORTED:
						pname = "GroupAggregate";
						strategy = "Sorted";
						break;
					case AGG_HASHED:
						pname = "HashAggregate";
						strategy = "Hashed";
						break;
					case AGG_MIXED:
						pname = "MixedAggregate";
						strategy = "Mixed";
						break;
					default:
						pname = "Aggregate ???";
						strategy = "???";
						break;
				}

				if (DO_AGGSPLIT_SKIPFINAL(agg->aggsplit))
				{
					partialmode = "Partial";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else if (DO_AGGSPLIT_COMBINE(agg->aggsplit))
				{
					partialmode = "Finalize";
					pname = psprintf("%s %s", partialmode, pname);
				}
				else
					partialmode = "Simple";
			}
			break;
		case T_WindowAgg:
			pname = sname = "WindowAgg";
			break;
		case T_Unique:
			pname = sname = "Unique";
			break;
		case T_SetOp:
			sname = "SetOp";
			switch (((SetOp *) plan)->strategy)
			{
				case SETOP_SORTED:
					pname = "SetOp";
					strategy = "Sorted";
					break;
				case SETOP_HASHED:
					pname = "HashSetOp";
					strategy = "Hashed";
					break;
				default:
					pname = "SetOp ???";
					strategy = "???";
					break;
			}
			break;
		case T_LockRows:
			pname = sname = "LockRows";
			break;
		case T_Limit:
			pname = sname = "Limit";
			break;
		case T_Hash:
			pname = sname = "Hash";
			break;
		case T_MergeQualProj:
			pname = sname = "MergeQualProj";
			break;
		default:
			pname = sname = "???";
			break;
	}

	ExplainOpenGroup("Plan",
					 relationship ? NULL : "Plan",
					 true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (plan_name)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "%s\n", plan_name);

			es->indent++;
		}
		if (es->indent)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "->  ");
			es->indent += 2;
		}
#ifdef __OPENTENBASE_C__
		if (planstate->plan->parallel_num < 0)
			appendStringInfoString(es->str, "Dop ");
#endif
		if (plan->parallel_aware)
			appendStringInfoString(es->str, "Parallel ");
		appendStringInfoString(es->str, pname);
		es->indent++;
	}
	else
	{
		ExplainPropertyText("Node Type", sname, es);
		if (strategy)
			ExplainPropertyText("Strategy", strategy, es);
		if (partialmode)
			ExplainPropertyText("Partial Mode", partialmode, es);
		if (operation)
			ExplainPropertyText("Operation", operation, es);
		if (relationship)
			ExplainPropertyText("Parent Relationship", relationship, es);
		if (plan_name)
			ExplainPropertyText("Subplan Name", plan_name, es);
		if (custom_name)
			ExplainPropertyText("Custom Plan Provider", custom_name, es);
		ExplainPropertyBool("Parallel Aware", plan->parallel_aware, es);
	}

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
			ExplainScanTarget((Scan *) plan, es);
			break;
		case T_ForeignScan:
		case T_CustomScan:
			if (((Scan *) plan)->scanrelid > 0)
				ExplainScanTarget((Scan *) plan, es);
			break;
#ifdef PGXC
		case T_RemoteQuery:
			/* Emit node execution list */
			ExplainExecNodes(((RemoteQuery *)plan)->exec_nodes, es);
			break;
#endif
#ifdef XCP
		case T_RemoteSubplan:
			{
				RemoteSubplan  *rsubplan = (RemoteSubplan *) plan;
				List *nodeNameList = NIL;
				ListCell *lc;

				/* print out fid */
				if (es->format == EXPLAIN_FORMAT_TEXT && es->verbose)
				{
					appendStringInfoChar(es->str, '[');
					if (list_length(rsubplan->dests) > 0)
					{
						foreach(lc, rsubplan->dests)
						{
							FragmentDest *dest = lfirst_node(FragmentDest, lc);
							appendStringInfo(es->str, "%d", dest->fid);
							if (lnext(lc) != NULL)
								appendStringInfoChar(es->str, ',');
						}
					}
					else
						appendStringInfo(es->str, "%d", rsubplan->fid);
					appendStringInfoChar(es->str, ']');

					if (rsubplan->param_fid != 0)
						appendStringInfo(es->str, " %s[%d]",
							rsubplan->under_subplan ? "param" : "msg",
							rsubplan->param_fid);
				}

				foreach(lc, rsubplan->nodeList)
				{
					char *nodename = get_pgxc_nodename(
							PGXCNodeGetNodeOid(lfirst_int(lc),
											   PGXC_NODE_DATANODE));
					nodeNameList = lappend(nodeNameList, nodename);
				}

				/* print out execute nodes */
				if (es->format == EXPLAIN_FORMAT_TEXT)
				{
					if (nodeNameList)
					{
						if (es->nodes && es->verbose)
						{
							bool 			first = true;
							ListCell 	   *lc;
							foreach(lc, nodeNameList)
							{
								char *nodename = (char *) lfirst(lc);
								if (first)
								{
									appendStringInfo(es->str, " on %s (%s",
												 rsubplan->execOnAll ? "all" : "any",
												 nodename);

									first = false;
								}
								else
									appendStringInfo(es->str, ",%s", nodename);
							}
							appendStringInfoChar(es->str, ')');
						}
						else
						{
							appendStringInfo(es->str, " on %s (datanodes %d)",
										 rsubplan->execOnAll ? "all" : "any",
										 list_length(nodeNameList));
						}
					}
					else
					{
						appendStringInfo(es->str, " on local node");
					}
				}
				else
				{
					ExplainPropertyText("Replicated",
										rsubplan->execOnAll ? "no" : "yes",
										es);
					if (es->nodes)
						ExplainPropertyList("Node List", nodeNameList, es);
				}
			}
			break;
#endif /* XCP */
		case T_IndexScan:
			{
				IndexScan  *indexscan = (IndexScan *) plan;

				ExplainIndexScanDetails(indexscan->indexid,
										indexscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexscan, es);
			}
			break;
		case T_IndexOnlyScan:
			{
				IndexOnlyScan *indexonlyscan = (IndexOnlyScan *) plan;

				ExplainIndexScanDetails(indexonlyscan->indexid,
										indexonlyscan->indexorderdir,
										es);
				ExplainScanTarget((Scan *) indexonlyscan, es);
			}
			break;
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapindexscan = (BitmapIndexScan *) plan;
				const char *indexname =
				explain_get_index_name(bitmapindexscan->indexid);

				es->itable = list_append_unique_oid(es->itable, bitmapindexscan->indexid);

				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " on %s", indexname);
				else
					ExplainPropertyText("Index Name", indexname, es);
			}
			break;
		case T_ModifyTable:
			ExplainModifyTarget((ModifyTable *) plan, es);
			break;
		case T_RemoteModifyTable:
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				const char *jointype;

				switch (((Join *) plan)->jointype)
				{
					case JOIN_INNER:
						jointype = "Inner";
						break;
					case JOIN_LEFT:
						jointype = "Left";
						break;
					case JOIN_LEFT_SEMI_SCALAR:
						jointype = "Left Scalar";
						break;
					case JOIN_SEMI_SCALAR:
						jointype = "Inner Scalar";
						break;
					case JOIN_LEFT_SEMI:
						jointype = "Left Semi";
						break;
					case JOIN_FULL:
						jointype = "Full";
						break;
					case JOIN_RIGHT:
						jointype = "Right";
						break;
					case JOIN_SEMI:
						jointype = "Semi";
						break;
					case JOIN_ANTI:
						jointype = "Anti";
						break;
					case JOIN_SEMI_RIGHT:
						jointype = "Right Semi";
						break;
					case JOIN_ANTI_RIGHT:
						jointype = "Right Anti";
						break;
					default:
						jointype = "???";
						break;
				}
				if (es->format == EXPLAIN_FORMAT_TEXT)
				{
					/*
					 * For historical reasons, the join type is interpolated
					 * into the node type name...
					 */
					if (((Join *) plan)->jointype != JOIN_INNER)
						appendStringInfo(es->str, " %s Join", jointype);
					else if (!IsA(plan, NestLoop))
						appendStringInfoString(es->str, " Join");
				}
				else
					ExplainPropertyText("Join Type", jointype, es);
			}
			break;
		case T_SetOp:
			{
				const char *setopcmd;

				switch (((SetOp *) plan)->cmd)
				{
					case SETOPCMD_INTERSECT:
						setopcmd = "Intersect";
						break;
					case SETOPCMD_INTERSECT_ALL:
						setopcmd = "Intersect All";
						break;
					case SETOPCMD_EXCEPT:
						setopcmd = "Except";
						break;
					case SETOPCMD_EXCEPT_ALL:
						setopcmd = "Except All";
						break;
                    case SETOPCMD_MINUS:
                        setopcmd = "Minus";
                        break;
                    case SETOPCMD_MINUS_ALL:
                        setopcmd = "Minus All";
                        break;
					default:
						setopcmd = "???";
						break;
				}
				if (es->format == EXPLAIN_FORMAT_TEXT)
					appendStringInfo(es->str, " %s", setopcmd);
				else
					ExplainPropertyText("Command", setopcmd, es);
			}
			break;
		default:
			break;
	}

	if (es->costs && plan->total_cost > 0)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(es->str, "  (cost=%.2f..%.2f rows=%.0f width=%d)",
							 plan->startup_cost, plan->total_cost,
							 plan->plan_rows, plan->plan_width);
		}
		else
		{
			ExplainPropertyFloat("Startup Cost", NULL, plan->startup_cost,
								 2, es);
			ExplainPropertyFloat("Total Cost", NULL, plan->total_cost,
								 2, es);
			ExplainPropertyFloat("Plan Rows", NULL, plan->plan_rows,
								 0, es);
			ExplainPropertyInteger("Plan Width", NULL, plan->plan_width,
								   es);
		}
	}

	/*
	 * OpenTenBase-V3: Construct remote instruments for display later.
	 * FID is empty when the node is executed on CN.
	 */
	if (es->analyze && HasRemoteInstr() && list_length(es->fids) > 0)
	{
		remote_instrs = ConstructRemoteInstr(plan, llast_int(es->fids));
	}

	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 *
	 * Note: contrib/auto_explain could cause instrumentation to be set up
	 * even though we didn't ask for it here.  Be careful not to print any
	 * instrumentation results the user didn't ask for.  But we do the
	 * InstrEndLoop call anyway, if possible, to reduce the number of cases
	 * auto_explain has to contend with.
 	 */
	if (planstate->instrument && !es->runtime)
		InstrEndLoop(planstate->instrument);

	if (es->analyze &&
		planstate->instrument && planstate->instrument->nloops > 0)
	{
		double		nloops = planstate->instrument->nloops;
		double		startup_ms = 1000.0 * planstate->instrument->startup / nloops;
		double		total_ms = 1000.0 * planstate->instrument->total / nloops;
		double		rows = planstate->instrument->ntuples / nloops;
		double 		exec_startup = 1000.0 * planstate->instrument->exec_firsttuple;
		double 		exec_total =  1000.0 * planstate->instrument->exec_total;
		double 		exec_enter = 1000.0 * planstate->instrument->exec_enter;
		double      foreign_total = 0.0;

		if (nodeTag(plan) == T_ForeignScan)
		{
			foreign_total = 1000.0 * INSTR_TIME_GET_DOUBLE(((ForeignScanState*)planstate)->f_counter) / nloops;
		}
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->timing)
            {
				appendStringInfo(es->str,
									" (actual time=%.3f..%.3f rows=%.0f loops=%.0f)",
									startup_ms, total_ms, rows, nloops);
	            if (nodeTag(plan) == T_ForeignScan)
	            {
					if (foreign_total > 0)
		            	appendStringInfo(es->str,
		                             " (foreign fetch time=%.3f)", foreign_total);
				}
			}
			else
			{
				appendStringInfo(es->str,
									" (actual rows=%.0f loops=%.0f)",
									rows, nloops);
            }
        }
		else
		{
			if (es->timing)
			{
				ExplainPropertyFloat("Actual Startup Time", "s", startup_ms,
									 3, es);
				ExplainPropertyFloat("Actual Total Time", "s", total_ms,
									 3, es);
				if (nodeTag(plan) == T_ForeignScan)
                {
                    if (foreign_total > 0)
                        ExplainPropertyFloat("Foreign Fetch Time", "s", foreign_total, 3, es);
                }
				if (es->format == EXPLAIN_FORMAT_JSON)
				{
					ExplainPropertyFloat("exec_enter", "s", exec_enter,
										 3, es);
					ExplainPropertyFloat("exec_startup", "s", exec_startup,
										 3, es);
					ExplainPropertyFloat("exec_total", "s", exec_total,
										 3, es);
				}
			}
			ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, nloops, 0, es);
		}
	}
	else if (es->analyze && !es->runtime)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			/* OpenTenBase-V3: Consider remote explain results */
			if (remote_instrs == NULL)
				appendStringInfoString(es->str, " (never executed)");
		}
		else
		{
			if (es->timing)
			{
				ExplainPropertyFloat("Actual Startup Time", "ms", 0.0, 3, es);
				ExplainPropertyFloat("Actual Total Time", "ms", 0.0, 3, es);
				if (es->format == EXPLAIN_FORMAT_JSON)
				{
					ExplainPropertyFloat("exec_enter", "ms", 0.0, 3, es);
					ExplainPropertyFloat("exec_startup", "ms", 0.0, 3, es);
					ExplainPropertyFloat("exec_total", "ms", 0.0, 3, es);
				}
			}
			ExplainPropertyFloat("Actual Rows", NULL, 0.0, 0, es);
			ExplainPropertyFloat("Actual Loops", NULL, 0.0, 0, es);
		}
	}

	/*
	 * Print the progress of node execution at current loop.
	 */
	if (planstate->instrument && es->analyze && es->runtime)
	{
		instr_time	starttimespan;
		double	startup_sec;
		double	total_sec;
		double	rows;
		double	loop_num;
		bool 	finished;

		if (!INSTR_TIME_IS_ZERO(planstate->instrument->starttime))
		{
			INSTR_TIME_SET_CURRENT(starttimespan);
			INSTR_TIME_SUBTRACT(starttimespan, planstate->instrument->starttime);
		}
		else
			INSTR_TIME_SET_ZERO(starttimespan);
		startup_sec = 1000.0 * planstate->instrument->firsttuple;
		total_sec = 1000.0 * (INSTR_TIME_GET_DOUBLE(planstate->instrument->counter)
							+ INSTR_TIME_GET_DOUBLE(starttimespan));
		rows = planstate->instrument->tuplecount;
		loop_num = planstate->instrument->nloops + 1;

		finished = planstate->instrument->nloops > 0
				&& !planstate->instrument->running
				&& INSTR_TIME_IS_ZERO(starttimespan);

		if (!finished)
		{
			ExplainOpenGroup("Current loop", "Current loop", true, es);
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				if (es->timing)
				{
					if (planstate->instrument->running)
						appendStringInfo(es->str,
								" (Current loop: actual time=%.3f..%.3f rows=%.0f, loop number=%.0f)",
								startup_sec, total_sec, rows, loop_num);
					else
						appendStringInfo(es->str,
								" (Current loop: running time=%.3f actual rows=0, loop number=%.0f)",
								total_sec, loop_num);
				}
				else
					appendStringInfo(es->str,
							" (Current loop: actual rows=%.0f, loop number=%.0f)",
							rows, loop_num);
			}
			else
			{
				ExplainPropertyFloat("Actual Loop Number", NULL, loop_num, 0, es);
				if (es->timing)
				{
					if (planstate->instrument->running)
					{
						ExplainPropertyFloat("Actual Startup Time", NULL, startup_sec, 3, es);
						ExplainPropertyFloat("Actual Total Time", NULL, total_sec, 3, es);
					}
					else
						ExplainPropertyFloat("Running Time", NULL, total_sec, 3, es);
				}
				ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
			}
			ExplainCloseGroup("Current loop", "Current loop", true, es);
		}
	}

	/* in text format, first line ends here */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		appendStringInfoChar(es->str, '\n');

	/* display result cache when cached in CN or centralized mode */
	if (!remote_instrs && es->analyze && 
		planstate->state->es_finished)
	{
		PrintResultCache(es->str, es->indent, planstate->state->es_rc_hash_table,
		                 &planstate->state->es_rc_explained);
	}
	
	if (plan->type == T_SeqScan || plan->type == T_IndexScan ||
	    plan->type == T_IndexOnlyScan || plan->type == T_BitmapHeapScan ||
	    plan->type == T_BitmapIndexScan || plan->type == T_TidScan)
	{
		Scan *scan = (Scan *) plan;
		if (scan->isPartTbl && es->verbose)
		{
			ListCell          *cell = NULL;
			PartIteratorState *pistate = NULL;
			StringInfoData     remvoed_part;
			int                part_idx = 0;

			foreach(cell, ancestors)
			{
				if (IsA(lfirst(cell), PartIteratorState))
				{
					pistate = lfirst_node(PartIteratorState, cell);
					break;
				}
			}

			initStringInfo(&remvoed_part);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfoString(es->str, "Part Iterator:");
			}

			foreach (cell, scan->partition_leaf_rels)
			{
				Oid   relid = lfirst_oid(cell);
				char *objectname = NULL;
				char *namespace = NULL;

				objectname = get_rel_name(relid);
				if (es->verbose)
				{
					namespace = get_namespace_name(get_rel_namespace(relid));
				}
				if (!bms_is_member(part_idx, pistate->pis_valid_parts))
				{
					if (es->format == EXPLAIN_FORMAT_TEXT)
					{
						if (namespace != NULL)
							appendStringInfo(&remvoed_part, " %s.%s", quote_identifier(namespace),
							                 quote_identifier(objectname));
						else if (objectname != NULL)
							appendStringInfo(&remvoed_part, " %s", quote_identifier(objectname));
					}
					else
					{
						if (objectname != NULL)
							ExplainPropertyText("Partition(Removed)", objectname, es);
						if (namespace != NULL)
							ExplainPropertyText("Schema(Removed)", namespace, es);
					}
				}
				else
				{
					if (es->format == EXPLAIN_FORMAT_TEXT)
					{
						appendStringInfoString(es->str, " ");
						if (namespace != NULL)
							appendStringInfo(es->str, " %s.%s", quote_identifier(namespace),
							                 quote_identifier(objectname));
						else if (objectname != NULL)
							appendStringInfo(es->str, " %s", quote_identifier(objectname));
					}
					else
					{
						if (objectname != NULL)
							ExplainPropertyText("Partition", objectname, es);
						if (namespace != NULL)
							ExplainPropertyText("Schema", namespace, es);
					}
				}
				part_idx ++;
			}
			appendStringInfoChar(es->str, '\n');
			
			if (pistate->pis_liveNum < pistate->pis_leafNum){
				ExplainPropertyInteger("Part Removed", NULL,
				                       pistate->pis_leafNum - pistate->pis_liveNum, es);
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfo(es->str, "Part Removed Detail: %s\n", remvoed_part.data);
			}
		}
	}

	/* OpenTenBase-V3: Display remote instruments for common info */
	if (remote_instrs)
	{
		/* For fragment, show recv instrument first. */
		if (IsA(plan, RemoteSubplan))
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				if (list_length(es->fids) > 1)
					ExplainCommonRemoteInstrRecv(plan, es);
				else
					show_mem_usage(planstate, es);

				show_simple_sort(planstate, ancestors, es);
			}
			else if (es->format == EXPLAIN_FORMAT_JSON)
			{
				if (list_length(es->fids) > 1)
					ExplainCommonRemoteInstrRecvJson(plan, es);
			}
		}
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			ExplainResultCacheInstr(remote_instrs, es);
			ExplainCommonRemoteInstr(remote_instrs, es, NULL);
		}
		else if (es->format == EXPLAIN_FORMAT_JSON)
		{
			ExplainCommonRemoteInstrJson(remote_instrs, es, NULL);
		}
	}

	/* prepare per-worker general execution details */
	if (es->workers_state && es->verbose)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;
		int n;

		for (n = 0; n < w->num_workers; n++)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;
			double		startup_ms;
			double		total_ms;
			double		rows;
			double		exec_enter;
			double		exec_start;
			double		exec_total;
			if (nloops <= 0)
				continue;
			startup_ms = 1000.0 * instrument->startup / nloops;
			total_ms = 1000.0 * instrument->total / nloops;
			rows = instrument->ntuples / nloops;
			exec_enter = 1000.0 * instrument->exec_enter;
			exec_start = 1000.0 * instrument->exec_firsttuple;
			exec_total = 1000.0 * instrument->exec_total;

			ExplainOpenWorker(n, es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				if (es->timing)
					appendStringInfo(es->str,
									 "actual time=%.3f..%.3f rows=%.0f loops=%.0f\n",
									 startup_ms, total_ms, rows, nloops);
				else
					appendStringInfo(es->str,
									 "actual rows=%.0f loops=%.0f\n",
									 rows, nloops);
			}
			else
			{
				if (es->timing)
				{
					ExplainPropertyFloat("Actual Startup Time", "ms",
										 startup_ms, 3, es);
					ExplainPropertyFloat("Actual Total Time", "ms",
										 total_ms, 3, es);
					if (es->format == EXPLAIN_FORMAT_JSON)
					{
						ExplainPropertyFloat("exec_enter", "ms", exec_enter, 3, es);
						ExplainPropertyFloat("exec_startup", "ms", exec_start, 3, es);
						ExplainPropertyFloat("exec_total", "ms", exec_total, 3, es);
					}
				}
				ExplainPropertyFloat("Actual Rows", NULL, rows, 0, es);
				ExplainPropertyFloat("Actual Loops", NULL, nloops, 0, es);
			}

			ExplainCloseWorker(n, es);
		}
	}

	/* target list */
	if (es->verbose)
		show_plan_tlist(planstate, ancestors, es);

	/* unique join */
	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* try not to be too chatty about this in text mode */
			if (es->format != EXPLAIN_FORMAT_TEXT ||
				(es->verbose && ((Join *) plan)->inner_unique))
				ExplainPropertyBool("Inner Unique",
									((Join *) plan)->inner_unique,
									es);
			break;
		default:
			break;
	}

	/* quals, sort keys, etc */
	switch (nodeTag(plan))
	{
		case T_IndexScan:
			show_scan_qual(((IndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexScan *) plan)->indexqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexScan *) plan)->indexorderbyorig,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_IndexOnlyScan:
			show_scan_qual(((IndexOnlyScan *) plan)->indexqual,
						   "Index Cond", planstate, ancestors, es);
			if (((IndexOnlyScan *) plan)->indexqual)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(((IndexOnlyScan *) plan)->indexorderby,
						   "Order By", planstate, ancestors, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				ExplainPropertyFloat("Heap Fetches", NULL,
									 planstate->instrument->ntuples2, 0, es);
			break;
		case T_BitmapIndexScan:
			show_scan_qual(((BitmapIndexScan *) plan)->indexqualorig,
						   "Index Cond", planstate, ancestors, es);
			break;
#ifdef PGXC
		case T_RemoteQuery:
			/* Remote query */
			ExplainRemoteQuery((RemoteQuery *)plan, planstate, ancestors, es);
			Assert(!plan->qual);
			break;
#endif
#ifdef XCP
		case T_RemoteSubplan:
			{
				if (remote_session)
					break;

				/* display sort info */
				if (remote_instrs == NULL)
					show_simple_sort(planstate, ancestors, es);
				/* display distribute keys */
				if (es->format == EXPLAIN_FORMAT_TEXT)
					ExplainRemoteSubplan(plan, planstate, ancestors, es);
			}
			break;
#endif
		case T_BitmapHeapScan:
			show_scan_qual(((BitmapHeapScan *) plan)->bitmapqualorig,
						   "Recheck Cond", planstate, ancestors, es);
			if (((BitmapHeapScan *) plan)->bitmapqualorig)
				show_instrumentation_count("Rows Removed by Index Recheck", 2,
										   planstate, es);
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			if (es->analyze)
				show_tidbitmap_info((BitmapHeapScanState *) planstate, es);
			break;
		case T_SampleScan:
			show_tablesample(((SampleScan *) plan)->tablesample,
							 planstate, ancestors, es);
			/* FALL THRU to print additional fields the same as SeqScan */
		case T_SeqScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_SubqueryScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Gather:
			{
				Gather	   *gather = (Gather *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ExplainPropertyInteger("Workers Planned", NULL,
									   gather->num_workers, es);

				/* Show params evaluated at gather node */
				if (gather->initParam)
					show_eval_params(gather->initParam, es);

				if (es->analyze && remote_instrs == NULL)
				{
					int			nworkers;

					nworkers = ((GatherState *) planstate)->nworkers_launched;
					ExplainPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}

				if (gather->single_copy || es->format != EXPLAIN_FORMAT_TEXT)
					ExplainPropertyBool("Single Copy", gather->single_copy, es);
			}
			break;
		case T_GatherMerge:
			{
				GatherMerge *gm = (GatherMerge *) plan;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				ExplainPropertyInteger("Workers Planned", NULL,
									   gm->num_workers, es);

				/* Show params evaluated at gather-merge node */
				if (gm->initParam)
					show_eval_params(gm->initParam, es);

				if (es->analyze && remote_instrs == NULL)
				{
					int			nworkers;

					nworkers = ((GatherMergeState *) planstate)->nworkers_launched;
					ExplainPropertyInteger("Workers Launched", NULL,
										   nworkers, es);
				}
			}
			break;
		case T_FunctionScan:
			if (es->verbose)
			{
				List	   *fexprs = NIL;
				ListCell   *lc;

				foreach(lc, ((FunctionScan *) plan)->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

					fexprs = lappend(fexprs, rtfunc->funcexpr);
				}
				/* We rely on show_expression to insert commas as needed */
				show_expression((Node *) fexprs,
								"Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TableFuncScan:
			if (es->verbose)
			{
				TableFunc  *tablefunc = ((TableFuncScan *) plan)->tablefunc;

				show_expression((Node *) tablefunc,
								"Table Function Call", planstate, ancestors,
								es->verbose, es);
			}
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_TidScan:
			{
				/*
				 * The tidquals list has OR semantics, so be sure to show it
				 * as an OR condition.
				 */
				List	   *tidquals = ((TidScan *) plan)->tidquals;

				if (list_length(tidquals) > 1)
					tidquals = list_make1(make_orclause(tidquals));
				show_scan_qual(tidquals, "TID Cond", planstate, ancestors, es);
				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				show_scan_qual(((TidScan *) plan)->remote_indexqual,
						   "Recheck Cond (EPQ)", planstate, ancestors, es);
			}
			break;
		case T_ForeignScan:
			show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			show_foreignscan_info((ForeignScanState *) planstate, es);
			break;
		case T_CustomScan:
			{
				CustomScanState *css = (CustomScanState *) planstate;

				show_scan_qual(plan->qual, "Filter", planstate, ancestors, es);
				if (plan->qual)
					show_instrumentation_count("Rows Removed by Filter", 1,
											   planstate, es);
				if (css->methods->ExplainCustomScan)
					css->methods->ExplainCustomScan(css, ancestors, es);
			}
			break;
		case T_NestLoop:
			check_nestloop_outer_row(plan, es);
			show_upper_qual(((NestLoop *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((NestLoop *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			else
				check_nestloop_joinkey((NestLoop *) plan, es);

			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_MergeJoin:
			show_upper_qual(((MergeJoin *) plan)->mergeclauses,
							"Merge Cond", planstate, ancestors, es);
			show_upper_qual(((MergeJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((MergeJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			check_mergejoin_all_sorted((MergeJoin *) plan, es);
			break;
		case T_HashJoin:
			show_upper_qual(((HashJoin *) plan)->hashclauses,
							"Hash Cond", planstate, ancestors, es);
			show_upper_qual(((HashJoin *) plan)->join.joinqual,
							"Join Filter", planstate, ancestors, es);
			if (((HashJoin *) plan)->join.joinqual)
				show_instrumentation_count("Rows Removed by Join Filter", 1,
										   planstate, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 2,
										   planstate, es);
			break;
		case T_Agg:
			show_agg_keys(castNode(AggState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			show_hashagg_info((AggState *) planstate, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Group:
			show_group_keys(castNode(GroupState, planstate), ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_Sort:
			show_sort_keys(castNode(SortState, planstate), ancestors, es);
			show_sort_info(castNode(SortState, planstate), es);
			break;
		case T_MergeAppend:
			show_merge_append_keys(castNode(MergeAppendState, planstate),
								   ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			break;
		case T_Result:
			show_upper_qual((List *) ((Result *) plan)->resconstantqual,
							"One-Time Filter", planstate, ancestors, es);
			show_upper_qual(plan->qual, "Filter", planstate, ancestors, es);
			if (plan->qual)
				show_instrumentation_count("Rows Removed by Filter", 1,
										   planstate, es);
			break;
		case T_ModifyTable:
            /* Remote query planning on DMLs */
            if (((ModifyTable*)plan)->operation == CMD_MERGE)
                show_modifytable_merge_info(plan, planstate, ancestors, es);
            else
                /* non-merge cases */
			    show_modifytable_info(castNode(ModifyTableState, planstate), ancestors,
								  es);
			break;
		case T_RemoteModifyTable:
			show_remotemodifytable_info(
				castNode(RemoteModifyTableState, planstate), ancestors, es);
			break;
		case T_Hash:
			show_hash_info(castNode(HashState, planstate), es);
			break;
		case T_MergeQualProj:
			show_mergequalproj_info(castNode(MergeQualProjState, planstate), ancestors, es);
			break;
		default:
			break;
	}

	/*
	 * Prepare per-worker JIT instrumentation.  As with the overall JIT
	 * summary, this is printed only if printing costs is enabled.
	 */
	if (es->workers_state && es->costs && es->verbose)
	{
		SharedJitInstrumentation *w = planstate->worker_jit_instrument;
		int n;

		if (w)
		{
			for (n = 0; n < w->num_workers; n++)
			{
				ExplainOpenWorker(n, es);
				ExplainPrintJIT(es, planstate->state->es_jit_flags,
								&w->jit_instr[n]);
				ExplainCloseWorker(n, es);
			}
		}
	}

	/* Show buffer usage */
	if (es->buffers && planstate->instrument)
		show_buffer_usage(es, &planstate->instrument->bufusage, false);

	/* Prepare per-worker buffer usage */
	if (es->workers_state && es->buffers && es->verbose)
	if (es->analyze && es->verbose && planstate->worker_instrument && !es->runtime)
	{
		WorkerInstrumentation *w = planstate->worker_instrument;
		int n;

		for (n = 0; n < w->num_workers; n++)
		{
			Instrumentation *instrument = &w->instrument[n];
			double		nloops = instrument->nloops;

			if (nloops <= 0)
				continue;

			ExplainOpenWorker(n, es);
			if (es->buffers)
				show_buffer_usage(es, &instrument->bufusage, false);
			ExplainCloseWorker(n, es);
		}
	}

	/* Show per-worker details for this plan node, then pop that stack */
	if (es->workers_state)
		ExplainFlushWorkersState(es);
	es->workers_state = save_workers_state;

#ifdef __OPENTENBASE_C__
	/* display remote instruments for specific info */
	if (remote_instrs)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			ExplainSpecialRemoteInstr(remote_instrs, es, nodeTag(plan));

			if (es->verbose)
				ExplainRemoteWorkerInstr(remote_instrs, es, nodeTag(plan));
		}
		else if (es->format == EXPLAIN_FORMAT_JSON)
		{
			if (es->verbose)
				ExplainRemoteWorkerInstrJson(remote_instrs, es, nodeTag(plan), false);
		}

		/* clean up remote instruments before go into child plans */
		DestroyRemoteInstr(remote_instrs);
		remote_instrs = NULL;
	}
#endif

	/* Get ready to display the child plans */
	haschildren = planstate->initPlan ||
		outerPlanState(planstate) ||
		innerPlanState(planstate) ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(planstate, CustomScanState) &&
		 ((CustomScanState *) planstate)->custom_ps != NIL) ||
		planstate->subPlan;
	if (haschildren)
	{
		ExplainOpenGroup("Plans", "Plans", false, es);
		/* Pass current PlanState as head of ancestors list for children */
		ancestors = lcons(planstate, ancestors);
	}

	if (es->analyze && list_length(plan->exec_subplan) > 0)
	{
		ListCell	*lc;
		List        *saved_fids = NULL;

		foreach(lc, plan->exec_subplan)
		{
			int		fid = 0;
			SubPlan *sp = lfirst_node(SubPlan, lc);
			PlanState *sps =
				list_nth(planstate->state->es_subplanstates, sp->plan_id - 1);

			if (es->fids)
				fid = llast_int(es->fids);

			if (!sp->isInitPlan ||
				CheckInitplanPrinted(fid, sps->plan->plan_node_id, es->printed_initplan_htab))
				continue;

			RecordInitplanPrinted(fid, sps->plan->plan_node_id, es->printed_initplan_htab);

			if (IsA(sps->plan, RemoteSubplan) && sp->subLinkType == CTE_SUBLINK)
			{
				es->printed_subplans = bms_add_member(es->printed_subplans,
				                                      sp->plan_id);
				saved_fids = es->fids;
				es->fids = NULL;
			}

			ExplainNode(sps, ancestors, "InitPlan", sp->plan_name, es);
			if (IsA(sps->plan, RemoteSubplan) && sp->subLinkType == CTE_SUBLINK)
				es->fids = saved_fids;
		}
	}

	/* initPlan-s */
	if (planstate->initPlan)
		ExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", es);

	/* lefttree */
	if (outerPlanState(planstate))
		ExplainNode(outerPlanState(planstate), ancestors,
					"Outer", NULL, es);

	/* righttree */
	if (innerPlanState(planstate))
		ExplainNode(innerPlanState(planstate), ancestors,
					"Inner", NULL, es);

	check_instrument_rows(planstate, es);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_Append:
			ExplainMemberNodes(((AppendState *) planstate)->appendplans,
							   ((AppendState *) planstate)->as_nplans,
							   list_length(((Append *) plan)->appendplans),
							   ancestors, es);
			break;
		case T_MergeAppend:
			ExplainMemberNodes(((MergeAppendState *) planstate)->mergeplans,
							   ((MergeAppendState *) planstate)->ms_nplans,
							   list_length(((MergeAppend *) plan)->mergeplans),
							   ancestors, es);
			break;
		case T_BitmapAnd:
			ExplainMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
							   ((BitmapAndState *) planstate)->nplans,
							   list_length(((BitmapAnd *) plan)->bitmapplans),
							   ancestors, es);
			break;
		case T_BitmapOr:
			ExplainMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
							   ((BitmapOrState *) planstate)->nplans,
							   list_length(((BitmapOr *) plan)->bitmapplans),
							   ancestors, es);
			break;
		case T_SubqueryScan:
			ExplainNode(((SubqueryScanState *) planstate)->subplan, ancestors,
						"Subquery", NULL, es);
			break;
		case T_CustomScan:
			ExplainCustomChildren((CustomScanState *) planstate,
								  ancestors, es);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		ExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es);

	/* end of child plans */
	if (haschildren)
	{
		ancestors = list_delete_first(ancestors);
		ExplainCloseGroup("Plans", "Plans", false, es);
	}

	/* in text format, undo whatever indentation we added */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		es->indent = save_indent;

	ExplainCloseGroup("Plan",
					  relationship ? NULL : "Plan",
					  true, es);

#ifdef __OPENTENBASE_C__
	/* delete fid for it is finished already */
	if (IsA(plan, RemoteSubplan) && HasRemoteInstr())
		es->fids = list_delete_last(es->fids);
#endif
}

/*
 * Show the targetlist of a plan node
 */
static void
show_plan_tlist(PlanState *planstate, List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	bool		useprefix;
	ListCell   *lc;

	/* No work if empty tlist (this occurs eg in bitmap indexscans) */
	if (plan->targetlist == NIL)
		return;
	/* The tlist of an Append isn't real helpful, so suppress it */
	if (IsA(plan, Append))
		return;
	/* Likewise for MergeAppend and RecursiveUnion */
	if (IsA(plan, MergeAppend))
		return;
	if (IsA(plan, RecursiveUnion))
		return;

	/*
	 * Likewise for ForeignScan that executes a direct INSERT/UPDATE/DELETE
	 *
	 * Note: the tlist for a ForeignScan that executes a direct INSERT/UPDATE
	 * might contain subplan output expressions that are confusing in this
	 * context.  The tlist for a ForeignScan that executes a direct UPDATE/
	 * DELETE always contains "junk" target columns to identify the exact row
	 * to update or delete, which would be confusing in this context.  So, we
	 * suppress it in all the cases.
	 */
	if (IsA(plan, ForeignScan) &&
		((ForeignScan *) plan)->operation != CMD_SELECT)
		return;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Deparse each result column (we now include resjunk ones) */
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (es->jumble_const)
			result = lappend(result, deparse_expression_jumble_const((Node *) tle->expr, context,
			                                                         useprefix, false));
		else
			result =
				lappend(result, deparse_expression((Node *) tle->expr, context, useprefix, false));
	}

	/* Print results */
	ExplainPropertyList("Output", result, es);
}

/*
 * Show a generic expression
 */
static void
show_expression(Node *node, const char *qlabel,
				PlanState *planstate, List *ancestors,
				bool useprefix, ExplainState *es)
{
	List	   *context;
	char	   *exprstr;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);

	/* Deparse the expression */
	if (es->jumble_const)
		exprstr = deparse_expression_jumble_const(node, context, useprefix, false);
	else
		exprstr = deparse_expression(node, context, useprefix, false);

	/* And add to es->str */
	ExplainPropertyText(qlabel, exprstr, es);
}

/*
 * Show a qualifier expression (which is a List with implicit AND semantics)
 */
static void
show_qual(List *qual, const char *qlabel,
		  PlanState *planstate, List *ancestors,
		  bool useprefix, ExplainState *es)
{
	Node	   *node;

	if (remote_session)
		return;

	/* No work if empty qual */
	if (qual == NIL)
		return;

	/* Convert AND list to explicit AND */
	node = (Node *) make_ands_explicit(qual);

	/* And show it */
	show_expression(node, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for a scan plan node
 */
static void
show_scan_qual(List *qual, const char *qlabel,
			   PlanState *planstate, List *ancestors,
			   ExplainState *es)
{
	bool		useprefix;

	useprefix = (IsA(planstate->plan, SubqueryScan) ||es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

/*
 * Show a qualifier expression for an upper-level plan node
 */
static void
show_upper_qual(List *qual, const char *qlabel,
				PlanState *planstate, List *ancestors,
				ExplainState *es)
{
	bool		useprefix;

	useprefix = (list_length(es->rtable) > 1 || es->verbose);
	show_qual(qual, qlabel, planstate, ancestors, useprefix, es);
}

static void
check_mergejoin_all_sorted(MergeJoin *plan, ExplainState *es)
{
	if (!es->warning_str)
		return;
	if (plan->is_not_full)
		ExplainPropertyTextInternal("Sql Warning", 
									"Not all items are sorted in MergeJoin", 
									es, true);
}

static void
check_nestloop_joinkey(NestLoop *plan, ExplainState *es)
{
	if (!es->warning_str)
		return;
	if (plan->join.joinqual)
		return;

	ExplainPropertyTextInternal("Sql Warning", "No joinkey in NestLoop", 
									es, true);
}

static void
check_nestloop_outer_row(Plan *plan, ExplainState *es)
{
	Plan *lplan;

	if (!es->warning_str)
		return;

	lplan = plan->lefttree;
	if (lplan->plan_rows == 1)
		ExplainPropertyTextInternal("Sql Warning", 
									"Outer estimated 1 rows in Nestloop", 
									es, true);
}

static void
check_instrument_rows(PlanState *planstate, ExplainState *es)
{
	Plan *plan = planstate->plan;
	PlanState	*lplanstate = outerPlanState(planstate);
	PlanState	*rplanstate = innerPlanState(planstate);
	Plan 		*lplan;
	Plan		*rplan;
	char		*warning_str;

	if (!(es->warning_str && planstate->instrument && 
		planstate->instrument->nloops > 0))
		return;

	warning_str = palloc0(WARNING_STR_LEN);
	lplan = lplanstate ? lplanstate->plan : NULL;
	rplan = rplanstate ? rplanstate->plan : NULL;

	switch (nodeTag(plan))
	{
		case T_NestLoop:
			{
				if (lplanstate->instrument && lplanstate->instrument->nloops > 0)
				{
					double		nloops = lplanstate->instrument->nloops;
					double		rows = lplanstate->instrument->ntuples / nloops;

					if (rows > Max((lplan->plan_rows) * nestloop_deviation_factor, 
								nestloop_deviation_threshold))
					{
						snprintf(warning_str, WARNING_STR_LEN, 
								"Count Deviation in Nestloop, estimate rows: %f, actual rows: %f", 
								lplan->plan_rows, rows);
						ExplainPropertyTextInternal("Sql Warning", 
													warning_str, 
													es, true);
						
					}
					if (rows > nestloop_outer_rows_threshold)
					{
						snprintf(warning_str, WARNING_STR_LEN, 
								"Large Table is outer in Nestloop, actual rows: %f", 
								rows);
						ExplainPropertyTextInternal("Sql Warning", 
													warning_str, 
													es, true);
					}
				}
				if (IsA(rplan, SeqScan) && 
					rplanstate->instrument && rplanstate->instrument->nloops > 0)
				{
					double		nloops = rplanstate->instrument->nloops;
					double		rows = rplanstate->instrument->ntuples / nloops;

					if (rows > nestloop_inner_rows_threshold)
					{
						snprintf(warning_str, WARNING_STR_LEN, 
								"Large Table is inner in Nestloop, actual rows: %f", 
								rows);
						ExplainPropertyTextInternal("Sql Warning", 
													warning_str, 
													es, true);
					}
				}
			}
			break;
		case T_HashJoin:
			{
				double		nlloops = lplanstate->instrument->nloops;
				double		lrows = lplanstate->instrument->ntuples / nlloops;
				double		nrloops = rplanstate->instrument->nloops;
				double		rrows = rplanstate->instrument->ntuples / nrloops;

				if (rrows > Max(lrows * hashjoin_inner_rows_factor, 
								hashjoin_inner_rows_threshold))
				{
					snprintf(warning_str, WARNING_STR_LEN, 
							"Large Table is inner in HashJoin, actual rows: %f", 
							rrows);
					ExplainPropertyTextInternal("Sql Warning", 
												warning_str, 
												es, true);
				}
			}
			break;
		default:
			break;
	}
	pfree(warning_str);
}

/*
 * Show the sort keys for a Sort node.
 */
static void
show_sort_keys(SortState *sortstate, List *ancestors, ExplainState *es)
{
	Sort	   *plan = (Sort *) sortstate->ss.ps.plan;

	show_sort_group_keys((PlanState *) sortstate, "Sort Key",
						 plan->numCols, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Show the sort keys for a SimpleSort node.
 */
static void
show_simple_sort(PlanState *planstate, List *ancestors, ExplainState *es)
{
	RemoteSubplan  *remoteplan = (RemoteSubplan *)planstate->plan;
	SimpleSort *sort = (SimpleSort *)remoteplan->sort;

	/* if remote subplan does not sort the results */
	if (!sort)
		return;

	show_sort_group_keys(planstate, "Sort Key",
						 sort->numCols, sort->sortColIdx,
						 sort->sortOperators, sort->sortCollations,
						 sort->nullsFirst,
						 ancestors, es);
}

/*
 * Show memory usage.
 */
static void
show_mem_usage(PlanState *planstate, ExplainState *es)
{
	if (!es->analyze)
		return;

	if (IsA(planstate, RemoteFragmentState))
	{
		ExplainFormat old_format;
		/* show cn fragment info */
		Size cxt_mem;
		RemoteFragmentState *fstate = (RemoteFragmentState *)planstate;

		old_format = es->format;
		es->format = EXPLAIN_FORMAT_TEXT;
		ExplainIndentText(es);
		es->format = old_format;

		cxt_mem = MemoryContextMemAllocated(fstate->cxt, true);
		appendStringInfo(es->str, "Recv Memory Usage: %ldkB",
			(cxt_mem + 1023) / 1024);

		appendStringInfo(es->str, "  Total Pages: %d", fstate->total_pages);
		if (fstate->disk_pages > 0)
		{
			appendStringInfo(es->str, "  Spilled Pages: %d", fstate->disk_pages);
		}
		
		appendStringInfoChar(es->str, '\n');
		return;
	}
}

/*
 * Likewise, for a MergeAppend node.
 */
static void
show_merge_append_keys(MergeAppendState *mstate, List *ancestors,
					   ExplainState *es)
{
	MergeAppend *plan = (MergeAppend *) mstate->ps.plan;

	show_sort_group_keys((PlanState *) mstate, "Sort Key",
						 plan->numCols, plan->sortColIdx,
						 plan->sortOperators, plan->collations,
						 plan->nullsFirst,
						 ancestors, es);
}

/*
 * Show the grouping keys for an Agg node.
 */
static void
show_agg_keys(AggState *astate, List *ancestors,
			  ExplainState *es)
{
	Agg		   *plan = (Agg *) astate->ss.ps.plan;

	if (plan->numCols > 0 || plan->groupingSets)
	{
		/* The key columns refer to the tlist of the child plan */
		ancestors = lcons(astate, ancestors);

		if (plan->groupingSets)
			show_grouping_sets(outerPlanState(astate), plan, ancestors, es);
		else
			show_sort_group_keys(outerPlanState(astate), "Group Key",
								 plan->numCols, plan->grpColIdx,
								 NULL, NULL, NULL,
								 ancestors, es);

		ancestors = list_delete_first(ancestors);
	}
}

static void
show_grouping_sets(PlanState *planstate, Agg *agg,
				   List *ancestors, ExplainState *es)
{
	List	   *context;
	bool		useprefix;
	ListCell   *lc;

	if (remote_session)
		return;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	ExplainOpenGroup("Grouping Sets", "Grouping Sets", false, es);

	show_grouping_set_keys(planstate, agg, NULL,
						   context, useprefix, ancestors, es);

	foreach(lc, agg->chain)
	{
		Agg		   *aggnode = lfirst(lc);
		Sort	   *sortnode = (Sort *) aggnode->plan.lefttree;

		show_grouping_set_keys(planstate, aggnode, sortnode,
							   context, useprefix, ancestors, es);
	}

	ExplainCloseGroup("Grouping Sets", "Grouping Sets", false, es);
}

static void
show_grouping_set_keys(PlanState *planstate,
					   Agg *aggnode, Sort *sortnode,
					   List *context, bool useprefix,
					   List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	char	   *exprstr;
	ListCell   *lc;
	List	   *gsets = aggnode->groupingSets;
	AttrNumber *keycols = aggnode->grpColIdx;
	const char *keyname;
	const char *keysetname;

	if (aggnode->aggstrategy == AGG_HASHED || aggnode->aggstrategy == AGG_MIXED)
	{
		keyname = "Hash Key";
		keysetname = "Hash Keys";
	}
	else
	{
		keyname = "Group Key";
		keysetname = "Group Keys";
	}

	ExplainOpenGroup("Grouping Set", NULL, true, es);

	if (sortnode)
	{
		show_sort_group_keys(planstate, "Sort Key",
							 sortnode->numCols, sortnode->sortColIdx,
							 sortnode->sortOperators, sortnode->collations,
							 sortnode->nullsFirst,
							 ancestors, es);
		if (es->format == EXPLAIN_FORMAT_TEXT)
			es->indent++;
	}

	ExplainOpenGroup(keysetname, keysetname, false, es);

	foreach(lc, gsets)
	{
		List	   *result = NIL;
		ListCell   *lc2;

		foreach(lc2, (List *) lfirst(lc))
		{
			Index		i = lfirst_int(lc2);
			AttrNumber	keyresno = keycols[i];
			TargetEntry *target = get_tle_by_resno(plan->targetlist,
												   keyresno);

			if (!target)
				elog(ERROR, "no tlist entry for key %d", keyresno);
			/* Deparse the expression, showing any top-level cast */
			if (es->jumble_const)
				exprstr = deparse_expression_jumble_const((Node *) target->expr, context, useprefix,
				                                          true);
			else
				exprstr = deparse_expression((Node *) target->expr, context, useprefix, true);

			result = lappend(result, exprstr);
		}

		if (!result && es->format == EXPLAIN_FORMAT_TEXT)
			ExplainPropertyText(keyname, "()", es);
		else
			ExplainPropertyListNested(keyname, result, es);
	}

	ExplainCloseGroup(keysetname, keysetname, false, es);

	if (sortnode && es->format == EXPLAIN_FORMAT_TEXT)
		es->indent--;

	ExplainCloseGroup("Grouping Set", NULL, true, es);
}

/*
 * Show the grouping keys for a Group node.
 */
static void
show_group_keys(GroupState *gstate, List *ancestors,
				ExplainState *es)
{
	Group	   *plan = (Group *) gstate->ss.ps.plan;

	/* The key columns refer to the tlist of the child plan */
	ancestors = lcons(gstate, ancestors);
	show_sort_group_keys(outerPlanState(gstate), "Group Key",
						 plan->numCols, plan->grpColIdx,
						 NULL, NULL, NULL,
						 ancestors, es);
	ancestors = list_delete_first(ancestors);
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
static void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (remote_session)
		return;

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		if (es->jumble_const)
			exprstr =
				deparse_expression_jumble_const((Node *) target->expr, context, useprefix, true);
		else
			exprstr = deparse_expression((Node *) target->expr, context, useprefix, true);

		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/* Append sort order information, if relevant */
		if (sortOperators != NULL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno]);
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
	}

	ExplainPropertyList(qlabel, result, es);
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier_as_pg(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Show TABLESAMPLE properties
 */
static void
show_tablesample(TableSampleClause *tsc, PlanState *planstate,
				 List *ancestors, ExplainState *es)
{
	List	   *context;
	bool		useprefix;
	char	   *method_name;
	List	   *params = NIL;
	char	   *repeatable;
	ListCell   *lc;

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = list_length(es->rtable) > 1;

	/* Get the tablesample method name */
	method_name = get_func_name(tsc->tsmhandler);

	/* Deparse parameter expressions */
	foreach(lc, tsc->args)
	{
		Node	   *arg = (Node *) lfirst(lc);

		if (es->jumble_const)
			params =
				lappend(params, deparse_expression_jumble_const(arg, context, useprefix, false));
		else
			params = lappend(params, deparse_expression(arg, context, useprefix, false));
	}
	if (tsc->repeatable)
	{
		if (es->jumble_const)
			repeatable = deparse_expression_jumble_const((Node *) tsc->repeatable, context,
			                                             useprefix, false);
		else
			repeatable = deparse_expression((Node *) tsc->repeatable, context, useprefix, false);
	}
	else
		repeatable = NULL;

	/* Print results */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		first = true;

		ExplainIndentText(es);
		appendStringInfo(es->str, "Sampling: %s (", method_name);
		foreach(lc, params)
		{
			if (!first)
				appendStringInfoString(es->str, ", ");
			appendStringInfoString(es->str, (const char *) lfirst(lc));
			first = false;
		}
		appendStringInfoChar(es->str, ')');
		if (repeatable)
			appendStringInfo(es->str, " REPEATABLE (%s)", repeatable);
		appendStringInfoChar(es->str, '\n');
	}
	else
	{
		ExplainPropertyText("Sampling Method", method_name, es);
		ExplainPropertyList("Sampling Parameters", params, es);
		if (repeatable)
			ExplainPropertyText("Repeatable Seed", repeatable, es);
	}
}

/*
 * If it's EXPLAIN ANALYZE, show tuplesort stats for a sort node
 */
static void
show_sort_info(SortState *sortstate, ExplainState *es)
{
	bool warning = es->warning_str ? true : false;

	if (!es->analyze && !warning)
		return;

	if (sortstate->sort_Done && sortstate->tuplesortstate != NULL)
	{
		Tuplesortstate *state = (Tuplesortstate *) sortstate->tuplesortstate;
		TuplesortInstrumentation stats;
		const char *sortMethod;
		const char *spaceType;
		long		spaceUsed;

		tuplesort_get_stats(state, &stats);
		sortMethod = tuplesort_method_name(stats.sortMethod);
		spaceType = tuplesort_space_type_name(stats.spaceType);
		spaceUsed = stats.spaceUsed;

		if (warning && stats.spaceType == SORT_SPACE_TYPE_DISK)
		{
			ExplainPropertyTextInternal("Sql Warning", "Sort on disk", es, true);
		}

		if (es->analyze)
		{
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				appendStringInfo(es->str, "Sort Method: %s  %s: %ldkB\n",
								sortMethod, spaceType, spaceUsed);
			}
			else
			{
				ExplainPropertyText("Sort Method", sortMethod, es);
				ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
				ExplainPropertyText("Sort Space Type", spaceType, es);
			}
		}		
	}

	/*
	 * You might think we should just skip this stanza entirely when
	 * es->hide_workers is true, but then we'd get no sort-method output at
	 * all.  We have to make it look like worker 0's data is top-level data.
	 * This is easily done by just skipping the OpenWorker/CloseWorker calls.
	 * Currently, we don't worry about the possibility that there are multiple
	 * workers in such a case; if there are, duplicate output fields will be
	 * emitted.
	 */
	if (sortstate->shared_info != NULL)
	{
		int			n;

		for (n = 0; n < sortstate->shared_info->num_workers; n++)
		{
			TuplesortInstrumentation *sinstrument;
			const char *sortMethod;
			const char *spaceType;
			long		spaceUsed;

			sinstrument = &sortstate->shared_info->sinstrument[n];
			if (sinstrument->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
				continue;		/* ignore any unfilled slots */
			sortMethod = tuplesort_method_name(sinstrument->sortMethod);
			spaceType = tuplesort_space_type_name(sinstrument->spaceType);
			spaceUsed = sinstrument->spaceUsed;

			if (warning && sinstrument->spaceType == SORT_SPACE_TYPE_DISK)
			{
				ExplainPropertyTextInternal("Sql Warning", 
											"Parallel worker Sort on disk", 
											es, true);
			}

			if (!es->analyze)
				continue;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				appendStringInfo(es->str,
								 "Sort Method: %s  %s: %ldkB\n",
								 sortMethod, spaceType, spaceUsed);
			}
			else
			{
				ExplainPropertyText("Sort Method", sortMethod, es);
				ExplainPropertyInteger("Sort Space Used", "kB", spaceUsed, es);
				ExplainPropertyText("Sort Space Type", spaceType, es);
			}

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * Show information on hash buckets/batches.
 */
static void
show_hash_info(HashState *hashstate, ExplainState *es)
{
	HashInstrumentation hinstrument = {0};

	/*
	 * Collect stats from the local process, even when it's a parallel query.
	 * In a parallel query, the leader process may or may not have run the
	 * hash join, and even if it did it may not have built a hash table due to
	 * timing (if it started late it might have seen no tuples in the outer
	 * relation and skipped building the hash table).  Therefore we have to be
	 * prepared to get instrumentation data from all participants.
	 */
	if (hashstate->hinstrument)
		memcpy(&hinstrument, hashstate->hinstrument,
			   sizeof(HashInstrumentation));

	if (hashstate->hashtable)
	{
		ExecHashAccumInstrumentation(&hinstrument, hashstate->hashtable);
	}

	/*
	 * Merge results from workers.  In the parallel-oblivious case, the
	 * results from all participants should be identical, except where
	 * participants didn't run the join at all so have no data.  In the
	 * parallel-aware case, we need to consider all the results.  Each worker
	 * may have seen a different subset of batches and we want to report the
	 * highest memory usage across all batches.  We take the maxima of other
	 * values too, for the same reasons as in ExecHashAccumInstrumentation.
	 */
	if (hashstate->shared_info)
	{
		SharedHashInfo *shared_info = hashstate->shared_info;
		int			i;

		for (i = 0; i < shared_info->num_workers; ++i)
		{
			HashInstrumentation *worker_hi = &shared_info->hinstrument[i];

			hinstrument.nbuckets = Max(hinstrument.nbuckets,
									   worker_hi->nbuckets);
			hinstrument.nbuckets_original = Max(hinstrument.nbuckets_original,
												worker_hi->nbuckets_original);
			hinstrument.nbatch = Max(hinstrument.nbatch,
									 worker_hi->nbatch);
			hinstrument.nbatch_original = Max(hinstrument.nbatch_original,
											  worker_hi->nbatch_original);
			hinstrument.space_peak = Max(hinstrument.space_peak,
										 worker_hi->space_peak);
			hinstrument.rowsFiltered = Max(hinstrument.rowsFiltered,
										worker_hi->rowsFiltered);
		}
	}

	if (hinstrument.nbatch > 0)
	{
		long		spacePeakKb = (hinstrument.space_peak + 1023) / 1024;

		if (es->format != EXPLAIN_FORMAT_TEXT)
		{
			ExplainPropertyInteger("Hash Buckets", NULL,
								   hinstrument.nbuckets, es);
			ExplainPropertyInteger("Original Hash Buckets", NULL,
								   hinstrument.nbuckets_original, es);
			ExplainPropertyInteger("Hash Batches", NULL,
								   hinstrument.nbatch, es);
			ExplainPropertyInteger("Original Hash Batches", NULL,
								   hinstrument.nbatch_original, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB",
								   spacePeakKb, es);
			ExplainPropertyInteger("Rows Filtered", "rows",
								   hinstrument.rowsFiltered, es);
		}
		else if (hinstrument.nbatch_original != hinstrument.nbatch ||
				 hinstrument.nbuckets_original != hinstrument.nbuckets)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Buckets: %d (originally %d)  Batches: %d (originally %d)  Memory Usage: %ldkB  Rows Filtered: %ld\n",
							 hinstrument.nbuckets,
							 hinstrument.nbuckets_original,
							 hinstrument.nbatch,
							 hinstrument.nbatch_original,
							 spacePeakKb,
							 hinstrument.rowsFiltered);
		}
		else
		{
			ExplainIndentText(es);
			appendStringInfo(es->str,
							 "Buckets: %d  Batches: %d  Memory Usage: %ldkB  Rows Filtered: %ld\n",
							 hinstrument.nbuckets, hinstrument.nbatch,
							 spacePeakKb, hinstrument.rowsFiltered);
		}
	}
}

/*
 * Show information on hash aggregate memory usage and batches.
 */
static void
show_hashagg_info(AggState *aggstate, ExplainState *es)
{
	Agg		*agg	   = (Agg *)aggstate->ss.ps.plan;
	int64	 memPeakKb = (aggstate->hash_mem_peak + 1023) / 1024;

	if (agg->aggstrategy != AGG_HASHED &&
		agg->aggstrategy != AGG_MIXED)
		return;

	if (es->format != EXPLAIN_FORMAT_TEXT)
	{

		if (es->costs)
			ExplainPropertyInteger("Planned Partitions", NULL,
								   aggstate->hash_planned_partitions, es);

		/*
		 * During parallel query the leader may have not helped out.  We
		 * detect this by checking how much memory it used.  If we find it
		 * didn't do any work then we don't show its properties.
		 */
		if (es->analyze && aggstate->hash_mem_peak > 0)
		{
			ExplainPropertyInteger("HashAgg Batches", NULL,
								   aggstate->hash_batches_used, es);
			ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb, es);
			ExplainPropertyInteger("Disk Usage", "kB",
								   aggstate->hash_disk_used, es);
		}
	}
	else
	{
		bool		gotone = false;

		if (es->costs && aggstate->hash_planned_partitions > 0)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "Planned Partitions: %d",
							 aggstate->hash_planned_partitions);
			gotone = true;
		}

		/*
		 * During parallel query the leader may have not helped out.  We
		 * detect this by checking how much memory it used.  If we find it
		 * didn't do any work then we don't show its properties.
		 */
		if (es->analyze && aggstate->hash_mem_peak > 0)
		{
			if (!gotone)
				ExplainIndentText(es);
			else
				appendStringInfoString(es->str, "  ");

			appendStringInfo(es->str, "Batches: %d  Memory Usage: " INT64_FORMAT "kB",
							 aggstate->hash_batches_used, memPeakKb);
			gotone = true;

			/* Only display disk usage if we spilled to disk */
			if (aggstate->hash_batches_used > 1)
			{
				appendStringInfo(es->str, "  Disk Usage: " UINT64_FORMAT "kB",
					aggstate->hash_disk_used);
			}
		}

		if (gotone)
			appendStringInfoChar(es->str, '\n');
	}

	/* Display stats for each parallel worker */
	if (es->analyze && aggstate->shared_info != NULL)
	{
		int n;
		for (n = 0; n < aggstate->shared_info->num_workers; n++)
		{
			AggregateInstrumentation *sinstrument;
			uint64		hash_disk_used;
			int			hash_batches_used;

			sinstrument = &aggstate->shared_info->sinstrument[n];
			/* Skip workers that didn't do anything */
			if (sinstrument->hash_mem_peak == 0)
				continue;
			hash_disk_used = sinstrument->hash_disk_used;
			hash_batches_used = sinstrument->hash_batches_used;
			memPeakKb = (sinstrument->hash_mem_peak + 1023) / 1024;

			if (es->workers_state)
				ExplainOpenWorker(n, es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);

				appendStringInfo(es->str, "Batches: %d  Memory Usage: " INT64_FORMAT "kB",
								 hash_batches_used, memPeakKb);

				/* Only display disk usage if we spilled to disk */
				if (hash_batches_used > 1)
					appendStringInfo(es->str, "  Disk Usage: " UINT64_FORMAT "kB",
									 hash_disk_used);
				appendStringInfoChar(es->str, '\n');
			}
			else
			{
				ExplainPropertyInteger("HashAgg Batches", NULL,
									   hash_batches_used, es);
				ExplainPropertyInteger("Peak Memory Usage", "kB", memPeakKb,
									   es);
				ExplainPropertyInteger("Disk Usage", "kB", hash_disk_used, es);
			}

			if (es->workers_state)
				ExplainCloseWorker(n, es);
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show exact/lossy pages for a BitmapHeapScan node
 */
static void
show_tidbitmap_info(BitmapHeapScanState *planstate, ExplainState *es)
{
	if (es->format != EXPLAIN_FORMAT_TEXT)
	{
		ExplainPropertyInteger("Exact Heap Blocks", NULL,
							   planstate->exact_pages, es);
		ExplainPropertyInteger("Lossy Heap Blocks", NULL,
							   planstate->lossy_pages, es);
	}
	else
	{
		if (planstate->exact_pages > 0 || planstate->lossy_pages > 0)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "Heap Blocks:");
			if (planstate->exact_pages > 0)
				appendStringInfo(es->str, " exact=%ld", planstate->exact_pages);
			if (planstate->lossy_pages > 0)
				appendStringInfo(es->str, " lossy=%ld", planstate->lossy_pages);
			appendStringInfoChar(es->str, '\n');
		}
	}
}

/*
 * If it's EXPLAIN ANALYZE, show instrumentation information for a plan node
 *
 * "which" identifies which instrumentation counter to print
 */
static void
show_instrumentation_count(const char *qlabel, int which,
						   PlanState *planstate, ExplainState *es)
{
	double		nfiltered;
	double		nloops;

	if (!es->analyze || !planstate->instrument)
		return;

	nloops = planstate->instrument->nloops;
	if (which == 2)
		nfiltered = ((nloops > 0) ? planstate->instrument->nfiltered2 / nloops : 0);
	else
		nfiltered = ((nloops > 0) ? planstate->instrument->nfiltered1 / nloops : 0);
	nloops = planstate->instrument->nloops;

	/* In text mode, suppress zero counts; they're not interesting enough */
	if (nfiltered > 0 || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyFloat(qlabel, NULL, nfiltered, 0, es);
}

/*
 * Show extra information for a ForeignScan node.
 */
static void
show_foreignscan_info(ForeignScanState *fsstate, ExplainState *es)
{
	FdwRoutine *fdwroutine = fsstate->fdwroutine;

	/* Let the FDW emit whatever fields it wants */
	if (((ForeignScan *) fsstate->ss.ps.plan)->operation != CMD_SELECT)
	{
		if (fdwroutine->ExplainDirectModify != NULL)
			fdwroutine->ExplainDirectModify(fsstate, es);
	}
	else
	{
		if (fdwroutine->ExplainForeignScan != NULL)
			fdwroutine->ExplainForeignScan(fsstate, es);
	}
}

/*
 * Show initplan params evaluated at Gather or Gather Merge node.
 */
static void
show_eval_params(Bitmapset *bms_params, ExplainState *es)
{
	int			paramid = -1;
	List	   *params = NIL;

	Assert(bms_params);

	while ((paramid = bms_next_member(bms_params, paramid)) >= 0)
	{
		char		param[32];

		snprintf(param, sizeof(param), "$%d", paramid);
		params = lappend(params, pstrdup(param));
	}

	if (params)
		ExplainPropertyList("Params Evaluated", params, es);
}

/*
 * Fetch the name of an index in an EXPLAIN
 *
 * We allow plugins to get control here so that plans involving hypothetical
 * indexes can be explained.
 */
const char *
explain_get_index_name(Oid indexId)
{
	const char *result;

	if (explain_get_index_name_hook)
		result = (*explain_get_index_name_hook) (indexId);
	else
		result = NULL;
	if (result == NULL)
	{
		/* default behavior: look in the catalogs and quote it */
		result = get_rel_name(indexId);
		if (result == NULL)
			elog(ERROR, "cache lookup failed for index %u", indexId);
		result = quote_identifier(result);
	}
	return result;
}

/*
 * Show buffer usage details.
 */
static void
show_buffer_usage(ExplainState *es, const BufferUsage *usage, bool planning)
{
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		bool		has_shared = (usage->shared_blks_hit > 0 ||
								  usage->shared_blks_read > 0 ||
								  usage->shared_blks_dirtied > 0 ||
								  usage->shared_blks_written > 0);
		bool		has_local = (usage->local_blks_hit > 0 ||
								 usage->local_blks_read > 0 ||
								 usage->local_blks_dirtied > 0 ||
								 usage->local_blks_written > 0);
		bool		has_temp = (usage->temp_blks_read > 0 ||
								usage->temp_blks_written > 0);
		bool		has_timing = (!INSTR_TIME_IS_ZERO(usage->blk_read_time) ||
								  !INSTR_TIME_IS_ZERO(usage->blk_write_time));
		bool		show_planning = (planning && (has_shared ||
												  has_local || has_temp || has_timing));

		if (show_planning)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "Planning:\n");
			es->indent++;
		}

		/* Show only positive counter values. */
		if (has_shared || has_local || has_temp)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "Buffers:");

			if (has_shared)
			{
				appendStringInfoString(es->str, " shared");
				if (usage->shared_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
									 usage->shared_blks_hit);
				if (usage->shared_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->shared_blks_read);
				if (usage->shared_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
									 usage->shared_blks_dirtied);
				if (usage->shared_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->shared_blks_written);
				if (has_local || has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_local)
			{
				appendStringInfoString(es->str, " local");
				if (usage->local_blks_hit > 0)
					appendStringInfo(es->str, " hit=%ld",
									 usage->local_blks_hit);
				if (usage->local_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->local_blks_read);
				if (usage->local_blks_dirtied > 0)
					appendStringInfo(es->str, " dirtied=%ld",
									 usage->local_blks_dirtied);
				if (usage->local_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->local_blks_written);
				if (has_temp)
					appendStringInfoChar(es->str, ',');
			}
			if (has_temp)
			{
				appendStringInfoString(es->str, " temp");
				if (usage->temp_blks_read > 0)
					appendStringInfo(es->str, " read=%ld",
									 usage->temp_blks_read);
				if (usage->temp_blks_written > 0)
					appendStringInfo(es->str, " written=%ld",
									 usage->temp_blks_written);
			}
			appendStringInfoChar(es->str, '\n');
		}

		/* As above, show only positive counter values. */
		if (has_timing)
		{
			ExplainIndentText(es);
			appendStringInfoString(es->str, "I/O Timings:");
			if (!INSTR_TIME_IS_ZERO(usage->blk_read_time))
				appendStringInfo(es->str, " read=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time));
			if (!INSTR_TIME_IS_ZERO(usage->blk_write_time))
				appendStringInfo(es->str, " write=%0.3f",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time));
			appendStringInfoChar(es->str, '\n');
		}

		if (show_planning)
			es->indent--;
	}
	else
	{
		ExplainPropertyInteger("Shared Hit Blocks", NULL,
							   usage->shared_blks_hit, es);
		ExplainPropertyInteger("Shared Read Blocks", NULL,
							   usage->shared_blks_read, es);
		ExplainPropertyInteger("Shared Dirtied Blocks", NULL,
							   usage->shared_blks_dirtied, es);
		ExplainPropertyInteger("Shared Written Blocks", NULL,
							   usage->shared_blks_written, es);
		ExplainPropertyInteger("Local Hit Blocks", NULL,
							   usage->local_blks_hit, es);
		ExplainPropertyInteger("Local Read Blocks", NULL,
							   usage->local_blks_read, es);
		ExplainPropertyInteger("Local Dirtied Blocks", NULL,
							   usage->local_blks_dirtied, es);
		ExplainPropertyInteger("Local Written Blocks", NULL,
							   usage->local_blks_written, es);
		ExplainPropertyInteger("Temp Read Blocks", NULL,
							   usage->temp_blks_read, es);
		ExplainPropertyInteger("Temp Written Blocks", NULL,
							   usage->temp_blks_written, es);
		if (track_io_timing)
		{
			ExplainPropertyFloat("I/O Read Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_read_time),
								 3, es);
			ExplainPropertyFloat("I/O Write Time", "ms",
								 INSTR_TIME_GET_MILLISEC(usage->blk_write_time),
								 3, es);
		}
	}
}

/*
 * Add some additional details about an IndexScan or IndexOnlyScan
 */
static void
ExplainIndexScanDetails(Oid indexid, ScanDirection indexorderdir,
						ExplainState *es)
{
	const char *indexname = explain_get_index_name(indexid);

	es->itable = list_append_unique_oid(es->itable, indexid);
	
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (ScanDirectionIsBackward(indexorderdir))
			appendStringInfoString(es->str, " Backward");
		appendStringInfo(es->str, " using %s", indexname);
	}
	else
	{
		const char *scandir;

		switch (indexorderdir)
		{
			case BackwardScanDirection:
				scandir = "Backward";
				break;
			case NoMovementScanDirection:
				scandir = "NoMovement";
				break;
			case ForwardScanDirection:
				scandir = "Forward";
				break;
			default:
				scandir = "???";
				break;
		}
		ExplainPropertyText("Scan Direction", scandir, es);
		ExplainPropertyText("Index Name", indexname, es);
	}
}

/*
 * Show the target of a Scan node
 */
static void
ExplainScanTarget(Scan *plan, ExplainState *es)
{
	ExplainTargetRel((Plan *) plan, plan->scanrelid, es);
}

/*
 * Show the target of a ModifyTable node
 *
 * Here we show the nominal target (ie, the relation that was named in the
 * original query).  If the actual target(s) is/are different, we'll show them
 * in show_modifytable_info().
 */
static void
ExplainModifyTarget(ModifyTable *plan, ExplainState *es)
{
	ExplainTargetRel((Plan *) plan, plan->nominalRelation, es);
}


/*
 * Show the target relation of a scan or modify node
 */
static void
ExplainTargetRel(Plan *plan, Index rti, ExplainState *es)
{
	char	   *objectname = NULL;
	char	   *namespace = NULL;
	const char *objecttag = NULL;
	RangeTblEntry *rte = NULL;
	char	   *refname = NULL;

	/* Assert the index is greater than 0 */
	if (rti > 0)
	{
		rte = rt_fetch(rti, es->rtable);
		refname = (char *) list_nth(es->rtable_names, rti - 1);
		if (refname == NULL)
			refname = rte->eref->aliasname;
	}

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_ForeignScan:
		case T_CustomScan:
		case T_ModifyTable:
			/* Assert it's on a real relation */
			Assert(rte->rtekind == RTE_RELATION);
			objectname = get_rel_name(rte->relid);
			if (es->verbose &&
			    !(es->jumble_const && (IsTempTable(rte->relid) || IsGlobalTempRelId(rte->relid))))
			namespace = get_namespace_name(get_rel_namespace(rte->relid));
			objecttag = "Relation Name";
			break;
		case T_FunctionScan:
			{
				FunctionScan *fscan = (FunctionScan *) plan;

				/* Assert it's on a RangeFunction */
				Assert(rte->rtekind == RTE_FUNCTION);

				/*
				 * If the expression is still a function call of a single
				 * function, we can get the real name of the function.
				 * Otherwise, punt.  (Even if it was a single function call
				 * originally, the optimizer could have simplified it away.)
				 */
				if (list_length(fscan->functions) == 1)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(fscan->functions);

					if (IsA(rtfunc->funcexpr, FuncExpr))
					{
						FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
						Oid			funcid = funcexpr->funcid;

#ifdef _PG_ORCL_
						if (funcexpr->withfuncnsp != NULL)
						{
							objectname
								= get_func_name_withfuncs(funcexpr->withfuncnsp,
															funcexpr->withfuncid);
						}
						else /* Normal case */
#endif
							objectname = get_func_name(funcid);
						if (es->verbose)
						{
#ifdef _PG_ORCL_
							if (funcexpr->withfuncnsp != NULL)
								namespace = "<with function>";
							else /* Normal case */
#endif
							namespace =
								get_namespace_name(get_func_namespace(funcid));
						}
					}
				}
				objecttag = "Function Name";
			}
			break;
		case T_TableFuncScan:
			Assert(rte->rtekind == RTE_TABLEFUNC);
			objectname = "xmltable";
			objecttag = "Table Function Name";
			break;
		case T_ValuesScan:
			Assert(rte->rtekind == RTE_VALUES);
			break;
		case T_CteScan:
			/* Assert it's on a non-self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(!rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		case T_NamedTuplestoreScan:
			Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);
			objectname = rte->enrname;
			objecttag = "Tuplestore Name";
			break;
		case T_WorkTableScan:
			/* Assert it's on a self-reference CTE */
			Assert(rte->rtekind == RTE_CTE);
			Assert(rte->self_reference);
			objectname = rte->ctename;
			objecttag = "CTE Name";
			break;
		case T_RemoteQuery:
			/* get the object name from RTE itself */
			Assert(rte->rtekind == RTE_REMOTE_DUMMY);
			objectname = get_rel_name(rte->relid);
			objecttag = "RemoteQuery name";
			break;
		default:
			break;
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoString(es->str, " on");
		if (namespace != NULL)
			appendStringInfo(es->str, " %s.%s", quote_identifier(namespace),
							 quote_identifier(objectname));
		else if (objectname != NULL)
			appendStringInfo(es->str, " %s", quote_identifier(objectname));
		if (objectname == NULL || strcmp(refname, objectname) != 0)
			appendStringInfo(es->str, " %s", quote_identifier(refname));
	}
	else
	{
		if (objecttag != NULL && objectname != NULL)
			ExplainPropertyText(objecttag, objectname, es);
		if (namespace != NULL)
			ExplainPropertyText("Schema", namespace, es);
		ExplainPropertyText("Alias", refname, es);
	}
}

/*
 * Show extra information for a ModifyTable node
 *
 * We have three objectives here.  First, if there's more than one target
 * table or it's different from the nominal target, identify the actual
 * target(s).  Second, give FDWs a chance to display extra info about foreign
 * targets.  Third, show information about ON CONFLICT.
 */
static void
show_modifytable_info(ModifyTableState *mtstate, List *ancestors,
					  ExplainState *es)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	const char *operation;
	const char *foperation;
	bool		labeltargets;
	int			j;
	List	   *idxNames = NIL;
	ListCell   *lst;

	switch (node->operation)
	{
		case CMD_INSERT:
			operation = "Insert";
			foperation = "Foreign Insert";
			break;
		case CMD_UPDATE:
			operation = "Update";
			foperation = "Foreign Update";
			break;
		case CMD_DELETE:
			operation = "Delete";
			foperation = "Foreign Delete";
			break;
		default:
			operation = "???";
			foperation = "Foreign ???";
			break;
	}

	/* Should we explicitly label target relations? */
	labeltargets = (mtstate->mt_nrels > 1 ||
					(mtstate->mt_nrels == 1 &&
					 mtstate->resultRelInfo->ri_RangeTableIndex != node->nominalRelation));

	if (labeltargets)
		ExplainOpenGroup("Target Tables", "Target Tables", false, es);

	for (j = 0; j < mtstate->mt_nrels; j++)
	{
		ResultRelInfo *resultRelInfo = mtstate->resultRelInfo + j;
		FdwRoutine *fdwroutine = resultRelInfo->ri_FdwRoutine;

		if (labeltargets)
		{
			/* Open a group for this target */
			ExplainOpenGroup("Target Table", NULL, true, es);

			/*
			 * In text mode, decorate each target with operation type, so that
			 * ExplainTargetRel's output of " on foo" will read nicely.
			 */
			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				ExplainIndentText(es);
				appendStringInfoString(es->str,
									   fdwroutine ? foperation : operation);
			}

			/* Identify target */
			ExplainTargetRel((Plan *) node,
							 resultRelInfo->ri_RangeTableIndex,
							 es);

			if (es->format == EXPLAIN_FORMAT_TEXT)
			{
				appendStringInfoChar(es->str, '\n');
				es->indent++;
			}
		}

		/* Give FDW a chance if needed */
		if (!resultRelInfo->ri_usesFdwDirectModify &&
			fdwroutine != NULL &&
			fdwroutine->ExplainForeignModify != NULL)
		{
			List	   *fdw_private = (List *) list_nth(node->fdwPrivLists, j);

			fdwroutine->ExplainForeignModify(mtstate,
											 resultRelInfo,
											 fdw_private,
											 j,
											 es);
		}

		if (labeltargets)
		{
			/* Undo the indentation we added in text format */
			if (es->format == EXPLAIN_FORMAT_TEXT)
				es->indent--;

			/* Close the group */
			ExplainCloseGroup("Target Table", NULL, true, es);
		}
	}

	/* Gather names of ON CONFLICT arbiter indexes */
	foreach(lst, node->arbiterIndexes)
	{
		char	   *indexname = get_rel_name(lfirst_oid(lst));

		idxNames = lappend(idxNames, indexname);
	}

	if (node->onConflictAction != ONCONFLICT_NONE)
	{
		ExplainPropertyText("Conflict Resolution",
							node->onConflictAction == ONCONFLICT_NOTHING ?
							"NOTHING" : "UPDATE",
							es);

		/*
		 * Don't display arbiter indexes at all when DO NOTHING variant
		 * implicitly ignores all conflicts
		 */
		if (idxNames)
			ExplainPropertyList("Conflict Arbiter Indexes", idxNames, es);

		/* ON CONFLICT DO UPDATE WHERE qual is specially displayed */
		if (node->onConflictWhere)
		{
			show_upper_qual((List *) node->onConflictWhere, "Conflict Filter",
							&mtstate->ps, ancestors, es);
			show_instrumentation_count("Rows Removed by Conflict Filter", 1, &mtstate->ps, es);
		}

		/* EXPLAIN ANALYZE display of actual outcome for each tuple proposed */
		if (es->analyze && mtstate->ps.instrument)
		{
			double		total;
			double		insert_path;
			double		other_path;

			if (!es->runtime)
				InstrEndLoop(outerPlanState(mtstate)->instrument);

			/* count the number of source rows */
			other_path = mtstate->ps.instrument->nfiltered2;
			
			if (!es->runtime)
			{
				total = outerPlanState(mtstate)->instrument->ntuples;
				insert_path = total - other_path;
				ExplainPropertyFloat("Tuples Inserted", NULL, insert_path, 0, es);
			}

			ExplainPropertyFloat("Conflicting Tuples", NULL,
								 other_path, 0, es);
		}
	}

	if (labeltargets)
		ExplainCloseGroup("Target Tables", "Target Tables", false, es);
}

/*
 * Show extra information for a RemoteModifyTable node
 *
 * Note: only show MERGE info now.
 */
static void
show_remotemodifytable_info(RemoteModifyTableState *mtstate, List *ancestors,
							ExplainState *es)
{
	RemoteModifyTable *node = (RemoteModifyTable *)mtstate->ps.plan;
	if (node->operation == CMD_MERGE)
	{
		ListCell  *actCell;
		StringInfo actionLabel = makeStringInfo();
		int		   actIdx	   = 1;
		if (es->verbose)
		{
			RemoteQuery *rqplan = linitial_node(RemoteQuery, node->dist_update_inserts);
			ExplainPropertyText("Insert Statement ", rqplan->sql_statement, es);
		}
		foreach (actCell, node->remoteMergeActionList)
		{
			MergeAction *action	= (MergeAction *)lfirst(actCell);
			char			  *qlabel	= NULL;
			char			  *actlabel = NULL;
			ListCell		  *tcell;
			List			  *tlist = NIL;
			List			  *context;
			Assert(!action->matched);
			qlabel = "Insert Cond";
			actlabel = "Insert";
			resetStringInfo(actionLabel);
			appendStringInfo(actionLabel, "Action %d Type", actIdx++);
			ExplainPropertyText(actionLabel->data, actlabel, es);

			if (es->verbose)
			{
				es->indent++;
				/* Set up deparsing context */
				context = set_deparse_context_planstate(
					es->deparse_cxt, (Node*)mtstate,
					ancestors);
				
				show_scan_qual((List *) action->qual,
				               qlabel,
				               ((PlanState *) mtstate)->lefttree->lefttree,
				               ancestors,
				               es);
				/* Deparse each result column (we now include resjunk ones) */
				foreach (tcell, action->targetList)
				{
					TargetEntry *tle = (TargetEntry *) lfirst(tcell);

					if (es->jumble_const)
						tlist = lappend(tlist, deparse_expression_jumble_const(
												   (Node *) tle->expr, context, true, false));
					else
						tlist = lappend(
							tlist, deparse_expression((Node *) tle->expr, context, true, false));
				}
				ExplainPropertyList(actlabel, tlist, es);
				es->indent--;
			}
		}
	}
}

/*
 * Show extra information of MERGE for a ModifyTable node
 */
static void
show_modifytable_merge_info(Plan *plan, PlanState* planstate, List *ancestors, ExplainState* es)
{
    ModifyTable* mt = (ModifyTable*)plan;
    ListCell* lc = NULL;
    if (es->analyze)
    {
        show_instrumentation_count("Merge Inserted", 1, planstate, es);
        show_instrumentation_count("Merge Updated", 2, planstate, es);
        show_instrumentation_count("Merge Deleted", 3, planstate, es);
    }

    foreach (lc, mt->mergeActionLists)
	{
		List *mergeActionList = lfirst(lc);
		ListCell *l;

		foreach (l, mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(l);
			char        *qlabel = NULL;

			if (action->commandType == CMD_NOTHING)
				continue;

			if (action->matched)
			{
				if (action->commandType == CMD_UPDATE)
				{
					qlabel = "Update Cond";
				}
				else
				{
					qlabel = "Delete Cond";
				}
			}
			else if (!action->matched)
			{
				qlabel = "Insert Cond";
			}

			show_scan_qual((List *) action->qual, qlabel, planstate, ancestors, es);
		}
	}
}

/*
 * Explain the constituent plans of an Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 *
 * Note: we don't actually need to examine the Plan list members, but
 * we need the list in order to determine the length of the PlanState array.
 */
void
ExplainMemberNodes(PlanState **planstates, int nsubnodes,
				   int nplans, List *ancestors, ExplainState *es)
{
	int			j;

	/*
	 * The number of subnodes being lower than the number of subplans that was
	 * specified in the plan means that some subnodes have been ignored per
	 * instruction for the partition pruning code during the executor
	 * initialization.  To make this a bit less mysterious, we'll indicate
	 * here that this has happened.
	 */
	if (nsubnodes < nplans)
		ExplainPropertyInteger("Subplans Removed", NULL, nplans - nsubnodes, es);

	for (j = 0; j < nsubnodes; j++)
		ExplainNode(planstates[j], ancestors,
					"Member", NULL, es);
}

/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlanStates.
 */
void 
ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *es)
{
	ListCell   *lst;
	List        *saved_fids;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		SubPlan    *sp = sps->subplan;
		char       *plan_name = sp->plan_name;

		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
		saved_fids = NULL;
		if (bms_is_member(sp->plan_id, es->printed_subplans))
			continue;
		es->printed_subplans = bms_add_member(es->printed_subplans,
											  sp->plan_id);

		/* the actual initplan that needs to be executed has been processed */
		if (plan_name == sp->plan_name && sp->isInitPlan && es->analyze &&
			sps->planstate && sps->planstate->state->es_distributed)
			continue;

		if (IsA(sps->planstate->plan, RemoteSubplan) && sp->subLinkType == CTE_SUBLINK)
		{
			RemoteSubplan *rplan = (RemoteSubplan*)sps->planstate->plan;
			RecordInitplanPrinted(rplan->fid, sps->planstate->plan->plan_node_id, es->printed_initplan_htab);
			saved_fids = es->fids;
			es->fids = NULL;
		}
		ExplainNode(sps->planstate, ancestors,
					relationship, plan_name, es);
		if (IsA(sps->planstate->plan, RemoteSubplan) && sp->subLinkType == CTE_SUBLINK)
			es->fids = saved_fids;
#ifdef __OPENTENBASE_C__
		if (plan_name != sp->plan_name)
		{
			pfree(plan_name);
			if (HasRemoteInstr())
				es->fids = list_delete_last(es->fids);
		}
#endif
	}
}

/*
 * Explain a list of children of a CustomScan.
 */
void
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es)
{
	ListCell   *cell;
	const char *label =
	(list_length(css->custom_ps) != 1 ? "children" : "child");

	foreach(cell, css->custom_ps)
		ExplainNode((PlanState *) lfirst(cell), ancestors, label, NULL, es);
}

/*
 * Create a per-plan-node workspace for collecting per-worker data.
 *
 * Output related to each worker will be temporarily "set aside" into a
 * separate buffer, which we'll merge into the main output stream once
 * we've processed all data for the plan node.  This makes it feasible to
 * generate a coherent sub-group of fields for each worker, even though the
 * code that produces the fields is in several different places in this file.
 * Formatting of such a set-aside field group is managed by
 * ExplainOpenSetAsideGroup and ExplainSaveGroup/ExplainRestoreGroup.
 */
static ExplainWorkersState *
ExplainCreateWorkersState(int num_workers)
{
	ExplainWorkersState *wstate;

	wstate = (ExplainWorkersState *) palloc(sizeof(ExplainWorkersState));
	wstate->num_workers = num_workers;
	wstate->worker_inited = (bool *) palloc0(num_workers * sizeof(bool));
	wstate->worker_str = (StringInfoData *)
		palloc0(num_workers * sizeof(StringInfoData));
	wstate->worker_state_save = (int *) palloc(num_workers * sizeof(int));
	return wstate;
}

/*
 * Begin or resume output into the set-aside group for worker N.
 */
static void
ExplainOpenWorker(int n, ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	Assert(wstate);
	Assert(n >= 0 && n < wstate->num_workers);

	/* Save prior output buffer pointer */
	wstate->prev_str = es->str;

	if (!wstate->worker_inited[n])
	{
		/* First time through, so create the buffer for this worker */
		initStringInfo(&wstate->worker_str[n]);
		es->str = &wstate->worker_str[n];

		/*
		 * Push suitable initial formatting state for this worker's field
		 * group.  We allow one extra logical nesting level, since this group
		 * will eventually be wrapped in an outer "Workers" group.
		 */
		ExplainOpenSetAsideGroup("Worker", NULL, true, 2, es);

		/*
		 * In non-TEXT formats we always emit a "Worker Number" field, even if
		 * there's no other data for this worker.
		 */
		if (es->format != EXPLAIN_FORMAT_TEXT)
			ExplainPropertyInteger("Worker Number", NULL, n, es);

		wstate->worker_inited[n] = true;
	}
	else
	{
		/* Resuming output for a worker we've already emitted some data for */
		es->str = &wstate->worker_str[n];

		/* Restore formatting state saved by last ExplainCloseWorker() */
		ExplainRestoreGroup(es, 2, &wstate->worker_state_save[n]);
	}

	/*
	 * In TEXT format, prefix the first output line for this worker with
	 * "Worker N:".  Then, any additional lines should be indented one more
	 * stop than the "Worker N" line is.
	 */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		if (es->str->len == 0)
		{
			ExplainIndentText(es);
			appendStringInfo(es->str, "Worker %d:  ", n);
		}

		es->indent++;
	}
}

/*
 * End output for worker N --- must pair with previous ExplainOpenWorker call
 */
static void
ExplainCloseWorker(int n, ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;

	Assert(wstate);
	Assert(n >= 0 && n < wstate->num_workers);
	Assert(wstate->worker_inited[n]);

	/*
	 * Save formatting state in case we do another ExplainOpenWorker(), then
	 * pop the formatting stack.
	 */
	ExplainSaveGroup(es, 2, &wstate->worker_state_save[n]);

	/*
	 * In TEXT format, if we didn't actually produce any output line(s) then
	 * truncate off the partial line emitted by ExplainOpenWorker.  (This is
	 * to avoid bogus output if, say, show_buffer_usage chooses not to print
	 * anything for the worker.)  Also fix up the indent level.
	 */
	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		while (es->str->len > 0 && es->str->data[es->str->len - 1] != '\n')
			es->str->data[--(es->str->len)] = '\0';

		es->indent--;
	}

	/* Restore prior output buffer pointer */
	es->str = wstate->prev_str;
}

/*
 * Print per-worker info for current node, then free the ExplainWorkersState.
 */
static void
ExplainFlushWorkersState(ExplainState *es)
{
	ExplainWorkersState *wstate = es->workers_state;
	int i;

	ExplainOpenGroup("Workers", "Workers", false, es);
	for (i = 0; i < wstate->num_workers; i++)
	{
		if (wstate->worker_inited[i])
		{
			/* This must match previous ExplainOpenSetAsideGroup call */
			ExplainOpenGroup("Worker", NULL, true, es);
			appendStringInfoString(es->str, wstate->worker_str[i].data);
			ExplainCloseGroup("Worker", NULL, true, es);

			pfree(wstate->worker_str[i].data);
		}
	}
	ExplainCloseGroup("Workers", "Workers", false, es);

	pfree(wstate->worker_inited);
	pfree(wstate->worker_str);
	pfree(wstate->worker_state_save);
	pfree(wstate);
}

/*
 * Explain a property, such as sort keys or targets, that takes the form of
 * a list of unlabeled items.  "data" is a list of C strings.
 */
void
ExplainPropertyList(const char *qlabel, List *data, ExplainState *es)
{
	ListCell   *lc;
	bool		first = true;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			ExplainIndentText(es);
			appendStringInfo(es->str, "%s: ", qlabel);

			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				appendStringInfoString(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, '\n');
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(qlabel, X_OPENING, es);
			foreach(lc, data)
			{
				char	   *str;

				appendStringInfoSpaces(es->str, es->indent * 2 + 2);
				appendStringInfoString(es->str, "<Item>");
				str = escape_xml((const char *) lfirst(lc));
				appendStringInfoString(es->str, str);
				pfree(str);
				appendStringInfoString(es->str, "</Item>\n");
			}
			ExplainXMLTag(qlabel, X_CLOSING, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, es->indent * 2);
			escape_json(es->str, qlabel);
			appendStringInfoString(es->str, ": [");
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				escape_json(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, ']');
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			appendStringInfo(es->str, "%s: ", qlabel);
			foreach(lc, data)
			{
				appendStringInfoChar(es->str, '\n');
				appendStringInfoSpaces(es->str, es->indent * 2 + 2);
				appendStringInfoString(es->str, "- ");
				escape_yaml(es->str, (const char *) lfirst(lc));
			}
			break;
	}
}

/*
 * Explain a property that takes the form of a list of unlabeled items within
 * another list.  "data" is a list of C strings.
 */
void
ExplainPropertyListNested(const char *qlabel, List *data, ExplainState *es)
{
	ListCell   *lc;
	bool		first = true;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
		case EXPLAIN_FORMAT_XML:
			ExplainPropertyList(qlabel, data, es);
			return;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfoChar(es->str, '[');
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				escape_json(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, ']');
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			appendStringInfoString(es->str, "- [");
			foreach(lc, data)
			{
				if (!first)
					appendStringInfoString(es->str, ", ");
				escape_yaml(es->str, (const char *) lfirst(lc));
				first = false;
			}
			appendStringInfoChar(es->str, ']');
			break;
	}
}

/*
 * Explain a simple property.
 *
 * If "numeric" is true, the value is a number (or other value that
 * doesn't need quoting in JSON).
 *
 * If unit is is non-NULL the text format will display it after the value.
 *
 * This usually should not be invoked directly, but via one of the datatype
 * specific routines ExplainPropertyText, ExplainPropertyInteger, etc.
 */
static void
ExplainProperty(const char *qlabel, const char *unit, const char *value,
				bool numeric, ExplainState *es, bool warning)
{
	StringInfo	strinfo = es->str;
	int			indent = es->indent;

	if (warning)
	{
		strinfo = es->warning_str;
		indent = 0;
	}

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			ExplainIndentTextInternal(strinfo, es->format, indent);
			if (unit)
				appendStringInfo(strinfo, "%s: %s %s\n", qlabel, value, unit);
			else
				appendStringInfo(strinfo, "%s: %s\n", qlabel, value);

			break;

		case EXPLAIN_FORMAT_XML:
			{
				char	   *str;

				appendStringInfoSpaces(strinfo, indent * 2);
				ExplainXMLTagInternal(qlabel, X_OPENING | X_NOWHITESPACE, strinfo, indent);
				str = escape_xml(value);
				appendStringInfoString(strinfo, str);
				pfree(str);
				ExplainXMLTagInternal(qlabel, X_CLOSING | X_NOWHITESPACE, strinfo, indent);
				appendStringInfoChar(strinfo, '\n');
			}
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEndingInternal(strinfo, es->format, es->grouping_stack);
			appendStringInfoSpaces(strinfo, indent * 2);
			escape_json(strinfo, qlabel);
			appendStringInfoString(strinfo, ": ");
			if (numeric)
				appendStringInfoString(strinfo, value);
			else
				escape_json(strinfo, value);
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStartingInternal(strinfo, es->format, es->grouping_stack, indent);
			appendStringInfo(strinfo, "%s: ", qlabel);
			if (numeric)
				appendStringInfoString(strinfo, value);
			else
				escape_yaml(strinfo, value);
			break;
	}
}

/*
 * Explain a string-valued property.
 */
static void
ExplainPropertyTextInternal(const char *qlabel, const char *value, ExplainState *es, bool warning)
{
	ExplainProperty(qlabel, NULL, value, false, es, warning);
}

void
ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es)
{
	ExplainPropertyTextInternal(qlabel, value, es, false);
}

/*
 * Explain an integer-valued property.
 */
void
ExplainPropertyInteger(const char *qlabel, const char *unit, int64 value,
					   ExplainState *es)
{
	char		buf[32];

	snprintf(buf, sizeof(buf), INT64_FORMAT, value);
	ExplainProperty(qlabel, unit, buf, true, es, false);
}

/*
 * Explain a float-valued property, using the specified number of
 * fractional digits.
 */
void
ExplainPropertyFloat(const char *qlabel, const char *unit, double value,
					 int ndigits, ExplainState *es)
{
	char	   *buf;

	buf = psprintf("%.*f", ndigits, value);
	ExplainProperty(qlabel, unit, buf, true, es, false);
	pfree(buf);
}

/*
 * Explain a bool-valued property.
 */
void
ExplainPropertyBool(const char *qlabel, bool value, ExplainState *es)
{
	ExplainProperty(qlabel, NULL, value ? "true" : "false", true, es, false);
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
void
ExplainOpenGroup(const char *objtype, const char *labelname,
				 bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(objtype, X_OPENING, es);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, 2 * es->indent);
			if (labelname)
			{
				escape_json(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			appendStringInfoChar(es->str, labeled ? '{' : '[');

			/*
			 * In JSON format, the grouping_stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level, 1 means we've
			 * emitted something (and so the next item needs a comma). See
			 * ExplainJSONLineEnding().
			 */
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_YAML:

			/*
			 * In YAML format, the grouping stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level AND this grouping
			 * level is unlabelled and must be marked with "- ".  See
			 * ExplainYAMLLineStarting().
			 */
			ExplainYAMLLineStarting(es);
			if (labelname)
			{
				appendStringInfo(es->str, "%s: ", labelname);
				es->grouping_stack = lcons_int(1, es->grouping_stack);
			}
			else
			{
				appendStringInfoString(es->str, "- ");
				es->grouping_stack = lcons_int(0, es->grouping_stack);
			}
			es->indent++;
			break;
	}
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding ExplainOpenGroup call.
 */
void
ExplainCloseGroup(const char *objtype, const char *labelname,
				  bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent--;
			ExplainXMLTag(objtype, X_CLOSING, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent--;
			appendStringInfoChar(es->str, '\n');
			appendStringInfoSpaces(es->str, 2 * es->indent);
			appendStringInfoChar(es->str, labeled ? '}' : ']');
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->indent--;
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * Open a group of related objects, without emitting actual data.
 *
 * Prepare the formatting state as though we were beginning a group with
 * the identified properties, but don't actually emit anything.  Output
 * subsequent to this call can be redirected into a separate output buffer,
 * and then eventually appended to the main output buffer after doing a
 * regular ExplainOpenGroup call (with the same parameters).
 *
 * The extra "depth" parameter is the new group's depth compared to current.
 * It could be more than one, in case the eventual output will be enclosed
 * in additional nesting group levels.  We assume we don't need to track
 * formatting state for those levels while preparing this group's output.
 *
 * There is no ExplainCloseSetAsideGroup --- in current usage, we always
 * pop this state with ExplainSaveGroup.
 */
static void
ExplainOpenSetAsideGroup(const char *objtype, const char *labelname,
						 bool labeled, int depth, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_JSON:
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_YAML:
			if (labelname)
				es->grouping_stack = lcons_int(1, es->grouping_stack);
			else
				es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent += depth;
			break;
	}
}

/*
 * Pop one level of grouping state, allowing for a re-push later.
 *
 * This is typically used after ExplainOpenSetAsideGroup; pass the
 * same "depth" used for that.
 *
 * This should not emit any output.  If state needs to be saved,
 * save it at *state_save.  Currently, an integer save area is sufficient
 * for all formats, but we might need to revisit that someday.
 */
static void
ExplainSaveGroup(ExplainState *es, int depth, int *state_save)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent -= depth;
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent -= depth;
			*state_save = linitial_int(es->grouping_stack);
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->indent -= depth;
			*state_save = linitial_int(es->grouping_stack);
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * Re-push one level of grouping state, undoing the effects of ExplainSaveGroup.
 */
static void
ExplainRestoreGroup(ExplainState *es, int depth, int *state_save)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_JSON:
			es->grouping_stack = lcons_int(*state_save, es->grouping_stack);
			es->indent += depth;
			break;

		case EXPLAIN_FORMAT_YAML:
			es->grouping_stack = lcons_int(*state_save, es->grouping_stack);
			es->indent += depth;
			break;
	}
}

/*
 * Emit a "dummy" group that never has any members.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 */
static void
ExplainDummyGroup(const char *objtype, const char *labelname, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, 2 * es->indent);
			if (labelname)
			{
				escape_json(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			escape_json(es->str, objtype);
			break;

		case EXPLAIN_FORMAT_YAML:
			ExplainYAMLLineStarting(es);
			if (labelname)
			{
				escape_yaml(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			else
			{
				appendStringInfoString(es->str, "- ");
			}
			escape_yaml(es->str, objtype);
			break;
	}
}

/*
 * Emit the start-of-output boilerplate.
 *
 * This is just enough different from processing a subgroup that we need
 * a separate pair of subroutines.
 */
void
ExplainBeginOutput(ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			appendStringInfoString(es->str,
								   "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n");
			es->indent++;
			break;

		case EXPLAIN_FORMAT_JSON:
			/* top-level structure is an array of plans */
			appendStringInfoChar(es->str, '[');
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_YAML:
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			break;
	}
}

/*
 * Emit the end-of-output boilerplate.
 */
void
ExplainEndOutput(ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent--;
			appendStringInfoString(es->str, "</explain>");
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent--;
			appendStringInfoString(es->str, "\n]");
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}

/*
 * Put an appropriate separator between multiple plans
 */
void
ExplainSeparatePlans(ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* add a blank line */
			appendStringInfoChar(es->str, '\n');
			break;

		case EXPLAIN_FORMAT_XML:
		case EXPLAIN_FORMAT_JSON:
		case EXPLAIN_FORMAT_YAML:
			/* nothing to do */
			break;
	}
}

#ifdef PGXC
/*
 * Emit execution node list number.
 */
static void
ExplainExecNodes(ExecNodes *en, ExplainState *es)
{
	int primary_node_count = en ? list_length(en->primarynodelist) : 0;
	int node_count = en ? list_length(en->nodeList) : 0;

	if (!es->num_nodes)
		return;

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfo(es->str, " (primary node count=%d, node count=%d)",
						 primary_node_count, node_count);
	}
	else
	{
		ExplainPropertyInteger("Primary node count", NULL, primary_node_count, es);
		ExplainPropertyInteger("Node count", NULL, node_count, es);
	}
}

/*
 * Emit remote query planning details
 */
void
ExplainRemoteQuery(RemoteQuery *plan, PlanState *planstate, List *ancestors, ExplainState *es)
{
	ExecNodes	*en = plan->exec_nodes;
	char nodetype = plan->exec_type == EXEC_ON_COORDS ? PGXC_NODE_COORDINATOR : PGXC_NODE_DATANODE;

	if (remote_session)
		return;

	/* add names of the nodes if they exist */
	if (en && es->nodes)
	{
		StringInfo node_names = makeStringInfo();
		ListCell *lcell;
		char	*sep;
		int		node_no;
		if (en->primarynodelist)
		{
			sep = "";
			foreach(lcell, en->primarynodelist)
			{
				node_no = lfirst_int(lcell);
				appendStringInfo(node_names, "%s%s", sep,
									get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, nodetype)));
				sep = ", ";
			}
			ExplainPropertyText("Primary node/s", node_names->data, es);
		}
		if (en->nodeList)
		{
			resetStringInfo(node_names);
			sep = "";
			foreach(lcell, en->nodeList)
			{
				node_no = lfirst_int(lcell);
				appendStringInfo(node_names, "%s%s", sep,
									get_pgxc_nodename(PGXCNodeGetNodeOid(node_no, nodetype)));
				sep = ", ";
			}
			ExplainPropertyText("Node/s", node_names->data, es);
		}
	}
		
	if (en && en->nExprs > 0 && en->dis_exprs[0])
	{
		int i = 0;
		for (i = 0; i < en->nExprs; i++)
		{
			show_expression((Node *)en->dis_exprs[i], "Node expr", planstate, ancestors,
			                es->verbose, es);
		}
	}

	/* Remote query statement */
	if (es->verbose)
		ExplainPropertyText("Remote query", plan->sql_statement, es);

	if (!es->analyze)
	{
		RemoteQuery *step = makeNode(RemoteQuery);
		StringInfoData explainQuery;
		StringInfoData explainResult;
		EState *estate;
		RemoteQueryState *node;
		Var *dummy;
		MemoryContext oldcontext;
		TupleTableSlot *result;
		bool firstline = true;

		initStringInfo(&explainQuery);
		initStringInfo(&explainResult);

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			if (es->indent)
			{
				appendStringInfoSpaces(es->str, es->indent * 2);
				appendStringInfoString(es->str, "->  ");
				es->indent += 2;
			}
		}

		appendStringInfo(&explainQuery, "EXPLAIN (");
		switch (es->format)
		{
			case EXPLAIN_FORMAT_TEXT:
				appendStringInfo(&explainQuery, "FORMAT TEXT");
				break;
			case EXPLAIN_FORMAT_YAML:
				appendStringInfo(&explainQuery, "FORMAT YAML");
				break;
			case EXPLAIN_FORMAT_JSON:
				appendStringInfo(&explainQuery, "FORMAT JSON");
				break;
			case EXPLAIN_FORMAT_XML:
				appendStringInfo(&explainQuery, "FORMAT XML");
				break;
		}
		appendStringInfo(&explainQuery, ", VERBOSE %s", es->verbose ? "ON" : "OFF");
		appendStringInfo(&explainQuery, ", COSTS %s", es->costs ? "ON" : "OFF");
		appendStringInfo(&explainQuery, ", TIMING %s", es->timing ? "ON" : "OFF");
		appendStringInfo(&explainQuery, ", BUFFERS %s", es->buffers ? "ON" : "OFF");
		appendStringInfo(&explainQuery, ") %s", plan->sql_statement);

		step->sql_statement = explainQuery.data;
		step->combine_type = COMBINE_TYPE_NONE;
		step->exec_nodes = copyObject(plan->exec_nodes);
		step->exec_nodes->en_param_expr = NULL;
		step->exec_nodes->primarynodelist = NIL;

		if (plan->exec_nodes && plan->exec_nodes->nodeList)
			step->exec_nodes->nodeList =
				list_make1_int(linitial_int(plan->exec_nodes->nodeList));

		step->exec_type = plan->exec_type;

		dummy = makeVar(1, 1, TEXTOID, -1, InvalidOid, 0);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
				makeTargetEntry((Expr *) dummy, 1, "QUERY PLAN", false));

		estate = planstate->state;
		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		node = ExecInitRemoteQuery(step, estate, EXEC_FLAG_EXPLAIN_ONLY);
		MemoryContextSwitchTo(oldcontext);
		result = ExecRemoteQuery((PlanState *) node);
		while (result != NULL && !TupIsNull(result))
		{
			Datum 	value;
			bool	isnull;
			value = slot_getattr(result, 1, &isnull);
			if (!isnull)
			{
				if (!firstline)
					appendStringInfoSpaces(&explainResult, 2 * es->indent);
				appendStringInfo(&explainResult, "%s\n", TextDatumGetCString(value));
				firstline = false;
			}

			/* fetch next */
			result = ExecRemoteQuery((PlanState *) node);
		}
		ExecEndRemoteQuery(node);

		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfo(es->str, "%s", explainResult.data);
		else if (es->format == EXPLAIN_FORMAT_JSON ||
				 es->format == EXPLAIN_FORMAT_YAML)
		{
			StringInfo str;
			int i;

			/*
			 * For fqs query, the content of explainResult.data already contain
			 * newline(\n), in escape_json routine it will be replace by \\n, so
			 * the output seems not gracefully.
			 * Here we directly call ExplainProperty with numeric argument true,
			 * and add some identations after newline(\n).
			 */
			str = makeStringInfo();
			for (i = 0; i < explainResult.len; i++)
			{
				appendStringInfoCharMacro(str, explainResult.data[i]);
				if (explainResult.data[i] == '\n')
					appendStringInfoSpaces(str, 2 * es->indent);
			}
			ExplainProperty("Remote plan", NULL, str->data, true, es, false);
			pfree(str->data);
			pfree(str);
		}
		else
			ExplainPropertyText("Remote plan", explainResult.data, es);
	}
}
#endif

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML restricts tag names more than our other output formats, eg they can't
 * contain white space or slashes.  Replace invalid characters with dashes,
 * so that for example "I/O Read Time" becomes "I-O-Read-Time".
 */
static void
ExplainXMLTagInternal(const char *tagname, int flags, StringInfo str, int indent)
{
	const char *s;
	const char *valid = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.";

	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoSpaces(str, 2 * indent);
	appendStringInfoCharMacro(str, '<');
	if ((flags & X_CLOSING) != 0)
		appendStringInfoCharMacro(str, '/');
	for (s = tagname; *s; s++)
		appendStringInfoChar(str, strchr(valid, *s) ? *s : '-');
	if ((flags & X_CLOSE_IMMEDIATE) != 0)
		appendStringInfoString(str, " /");
	appendStringInfoCharMacro(str, '>');
	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoCharMacro(str, '\n');
}

static void
ExplainXMLTag(const char *tagname, int flags, ExplainState *es)
{
	ExplainXMLTagInternal(tagname, flags, es->str, es->indent);
}

/*
 * Indent a text-format line.
 *
 * We indent by two spaces per indentation level.  However, when emitting
 * data for a parallel worker there might already be data on the current line
 * (cf. ExplainOpenWorker); in that case, don't indent any more.
 */
static void
ExplainIndentTextInternal(StringInfo str, ExplainFormat format, int indent)
{
	Assert(format == EXPLAIN_FORMAT_TEXT);
	if (str->len == 0 || str->data[str->len - 1] == '\n')
		appendStringInfoSpaces(str, indent * 2);
}

static void
ExplainIndentText(ExplainState *es)
{
	ExplainIndentTextInternal(es->str, es->format, es->indent);
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void
ExplainJSONLineEndingInternal(StringInfo str, ExplainFormat format, List *grouping_stack)
{
	Assert(format == EXPLAIN_FORMAT_JSON);
	if (linitial_int(grouping_stack) != 0)
		appendStringInfoChar(str, ',');
	else
		linitial_int(grouping_stack) = 1;
	appendStringInfoChar(str, '\n');
}

static void
ExplainJSONLineEnding(ExplainState *es)
{
	ExplainJSONLineEndingInternal(es->str, es->format, es->grouping_stack);
}

/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabelled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
static void
ExplainYAMLLineStartingInternal(StringInfo str, ExplainFormat format, 
								List *grouping_stack, int indent)
{
	Assert(format == EXPLAIN_FORMAT_YAML);
	if (linitial_int(grouping_stack) == 0)
	{
		linitial_int(grouping_stack) = 1;
	}
	else
	{
		appendStringInfoChar(str, '\n');
		appendStringInfoSpaces(str, indent * 2);
	}
}

static void
ExplainYAMLLineStarting(ExplainState *es)
{
	ExplainYAMLLineStartingInternal(es->str, es->format, es->grouping_stack, 
									es->indent);
}

/*
 * YAML is a superset of JSON; unfortunately, the YAML quoting rules are
 * ridiculously complicated -- as documented in sections 5.3 and 7.3.3 of
 * http://yaml.org/spec/1.2/spec.html -- so we chose to just quote everything.
 * Empty strings, strings with leading or trailing whitespace, and strings
 * containing a variety of special characters must certainly be quoted or the
 * output is invalid; and other seemingly harmless strings like "0xa" or
 * "true" must be quoted, lest they be interpreted as a hexadecimal or Boolean
 * constant rather than a string.
 */
static void
escape_yaml(StringInfo buf, const char *str)
{
	escape_json(buf, str);
}

#ifdef XCP
/*
 * Print out distribute keys for RemoteSubplan
 */
void
ExplainRemoteSubplan(Plan *plan, PlanState *planstate, List *ancestors,
					 ExplainState *es)
{
	RemoteSubplan *rsubplan = (RemoteSubplan *) plan;
	char           label[32];
	TargetEntry   *tle = NULL;
	List *context;
	char *exprstr;
	StringInfoData buf;
	int i;

	if (rsubplan->num_virtualdop > 0)
	{
		ExplainIndentText(es);
		appendStringInfo(es->str, "Workers Planned: %d/%d\n",
			rsubplan->num_workers, rsubplan->num_virtualdop);
	}
	else if (rsubplan->num_workers > 0)
		ExplainPropertyInteger("Workers Planned", NULL,
							   rsubplan->num_workers, es);

	if (rsubplan->initParam)
		show_eval_params(rsubplan->initParam, es);

	if (rsubplan->localSend)
	{
		ExplainIndentText(es);
		appendStringInfo(es->str, "Distribute results by Local\n");
		return;
	}

	if (rsubplan->distributionType == LOCATOR_TYPE_NONE)
		return;

	sprintf(label, "Distribute results by %c", rsubplan->distributionType);
	if (rsubplan->ndiskeys <= 0)
	{
		ExplainIndentText(es);
		appendStringInfo(es->str, "%s\n", label);
		return;
	}

	/* normal case */
	initStringInfo(&buf);
	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate, ancestors);

	for (i = 0; i < rsubplan->ndiskeys; i++)
	{
		tle = (TargetEntry *) list_nth(plan->targetlist, rsubplan->diskeys[i] - 1);
		if (IsA(tle, TargetEntry))
		{
			/* Deparse the expression */
			if (es->jumble_const)
				exprstr =
					deparse_expression_jumble_const((Node *) tle->expr, context, false, false);
			else
				exprstr = deparse_expression((Node *) tle->expr, context, false, false);
			appendStringInfo(&buf, (i > 0) ? ", ":"");
			appendStringInfo(&buf, "%s", exprstr);
		}
	}

	/* Mark roundrobin */
	if (rsubplan->roundrobin_distributed == true)
	{
		appendStringInfo(&buf, " (roundrobin distribute)");
	}
	else if (rsubplan->roundrobin_replicate == true)
	{
		appendStringInfo(&buf, " (roundrobin replicate)");
	}
	else if (list_length(rsubplan->retain_list) > 0 ||
			 list_length(rsubplan->replicated_list) > 0)
	{
		appendStringInfo(&buf, " (");

		if (list_length(rsubplan->retain_list) > 0)
		{
			appendStringInfo(&buf, "retain values:%d", list_length(rsubplan->retain_list));
		}

		if (list_length(rsubplan->retain_list) > 0 &&
			 list_length(rsubplan->replicated_list) > 0)
			appendStringInfo(&buf, " and ");

		if (list_length(rsubplan->replicated_list) > 0)
		{
			appendStringInfo(&buf, "replicate values:%d", list_length(rsubplan->replicated_list));
		}

		appendStringInfo(&buf, ")");
	}

	ExplainPropertyText(label, buf.data, es);
	pfree(buf.data);
}

#endif

/*
 * Show extra information for a MergetQualProj node
 *
 */
static void
show_mergequalproj_info(MergeQualProjState *mqpstate, List *ancestors, ExplainState *es)
{
	MergeQualProj *node = (MergeQualProj *) mqpstate->ps.plan;
	ListCell      *actCell;
	StringInfoData actionLabel;
	int            actIdx = 1;

	initStringInfo(&actionLabel);
	foreach (actCell, node->mergeActionList)
	{
		MergeAction *action = (MergeAction *) lfirst(actCell);
		char        *qlabel = NULL;
		char        *actlabel = NULL;
		ListCell    *tcell;
		List        *tlist = NIL;
		List        *context;
		if (action->matched)
		{
			if (action->commandType == CMD_UPDATE)
			{
				qlabel = "Update Cond";
				actlabel = "Update";
			}
			else
			{
				qlabel = "Delete Cond";
				actlabel = "Delete";
			}
		}
		else
		{
			qlabel = "Insert Cond";
			actlabel = "Insert";
		}
		resetStringInfo(&actionLabel);
		appendStringInfo(&actionLabel, "Action %d Type", actIdx++);
		ExplainPropertyText(actionLabel.data, actlabel, es);
		if (action->commandType != CMD_INSERT)
			continue;
		es->indent++;
		show_scan_qual((List *) action->qual, qlabel, ((PlanState *) mqpstate), ancestors, es);

		if (es->verbose)
		{
			/* Set up deparsing context */
			context = set_deparse_context_planstate(es->deparse_cxt, (Node *) mqpstate, ancestors);
			/* Deparse each result column (we now include resjunk ones) */
			foreach (tcell, action->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tcell);

				if (es->jumble_const)
					tlist = lappend(tlist, deparse_expression_jumble_const((Node *) tle->expr,
					                                                       context, true, false));
				else
					tlist = lappend(tlist,
					                deparse_expression((Node *) tle->expr, context, true, false));
			}
			ExplainPropertyList(actlabel, tlist, es);
		}
		es->indent--;
	}
}


void
ExplainSpmHints(ExplainState *es, QueryDesc *queryDesc)
{
	int hint_start_len = 0;
	int hint_end_len = 0;

	es->spm_hints_state = spm_gen_hints(queryDesc);

	appendStringInfo(es->str, "\n---- Hints Data ----------\n");

	hint_start_len = es->str->len;
	if (es->spm_hints_state->leadcxt->lead_str)
		appendStringInfo(es->str, "%s", es->spm_hints_state->leadcxt->lead_str->data);
	if (es->spm_hints_state->join_str)
		appendStringInfo(es->str, "%s", es->spm_hints_state->join_str->data);
	if (es->spm_hints_state->scan_str)
		appendStringInfo(es->str, "%s", es->spm_hints_state->scan_str->data);
	if (es->spm_hints_state->parallel_str)
		appendStringInfo(es->str, "%s", es->spm_hints_state->parallel_str->data);
	if (es->spm_hints_state->distribution_str)
		appendStringInfo(es->str, "%s", es->spm_hints_state->distribution_str->data);
	if (es->spm_hints_state->aggpath_str && es->spm_hints_state->aggpath_str->len > 0)
		appendStringInfo(es->str, "%s", es->spm_hints_state->aggpath_str->data);
	if (es->spm_hints_state->mergejoin_key_str && es->spm_hints_state->mergejoin_key_str->len > 0)
		appendStringInfo(es->str, "%s", es->spm_hints_state->mergejoin_key_str->data);
	if (es->spm_hints_state->guc_str && es->spm_hints_state->guc_str->len > 0)
		appendStringInfo(es->str, "%s", es->spm_hints_state->guc_str->data);
	hint_end_len = es->str->len;

	if (hint_end_len > hint_start_len)
		SPMFillHint(es->str->data + hint_start_len,
		            hint_end_len - hint_start_len,
		            es->spm_hints_state->table_seq_no_str->data,
		            es->spm_hints_state->table_seq_no_str->len);

	SPMFillPlan(es->spm_hints_state->plan_str->data, es->spm_hints_state->plan_str->len);

	appendStringInfo(es->str, "\n---- Table Info ----------\n");
	appendStringInfo(es->str, "%s", es->spm_hints_state->table_seq_no_str->data);

	if (SPM_ENABLED)
	{
		// Assert(pre_spmplan->sql_id == (int64) queryDesc->plannedstmt->queryId);
		appendStringInfo(es->str, "\n--------------------------");
		appendStringInfo(es->str, "\nSql  ID: %-u", queryDesc->plannedstmt->queryId);
		appendStringInfo(es->str, "\nPlan ID: %-u", es->spm_hints_state->plan_id);
		appendStringInfo(es->str, "\n--------------------------");
		pre_spmplan->plan_id = (int64) es->spm_hints_state->plan_id;
		SPMFillIndex(es->itable);
		(void) ValidateSPMPlan(pre_spmplan->sql_id, (int64) es->spm_hints_state->plan_id);
	}
}

static void
ExplainSPMWork(ExplainState *es)
{
	if (!SPM_ENABLED || !(save_spmplan_state & SAVE_SPMPLAN_APPLYED))
		return;

	if (ChooseSPMPlan(pre_spmplan->sql_id, false))
	{
		int64 cache_planid;

		(void) GetSPMBindHint(pre_spmplan->sql_id, &cache_planid, NULL);

		appendStringInfo(es->str,
		                 "\nSQL plan baseline sql_id[%ld], plan_id[%ld] used for this statement.\n",
		                 pre_spmplan->sql_id,
		                 cache_planid);
	}
}
