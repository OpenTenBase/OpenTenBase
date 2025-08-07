/*-------------------------------------------------------------------------
 *
 * analyze.c
 *	  the Postgres statistics generator
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/analyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/multixact.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tupconvert.h"
#include "access/tuptoaster.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/local_statistics.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "statistics/extended_stats_internal.h"
#include "statistics/statistics.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/attoptcache.h"
#include "utils/bitmap.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/plancache.h"
#include "utils/sampling.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"

#ifdef XCP
#include "catalog/pg_operator.h"
#include "nodes/makefuncs.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#include "utils/snapmgr.h"
#endif
#ifdef __OPENTENBASE__
#include "funcapi.h"
#include "nodes/nodes.h"
#include "utils/ruleutils.h"
#include "nodes/pg_list.h"

#endif
#ifdef __OPENTENBASE_C__
#include "access/relscan.h"
#endif

#define MAX_SAMPLE_LENGTH (256)

#define ANALYZE_LOCAL(options) \
	((bool)((options & VACOPT_ANALYZE_LOCAL) == VACOPT_ANALYZE_LOCAL))

/* Per-index data for ANALYZE */
typedef struct AnlIndexData
{
	IndexInfo  *indexInfo;		/* BuildIndexInfo result */
	double		tupleFract;		/* fraction of rows for partial index */
	VacAttrStats **vacattrstats;	/* index attrs to analyze */
	int			attr_cnt;
} AnlIndexData;

/* Default statistics target (GUC parameter) */
int			default_statistics_target = 100;

static BufferAccessStrategy vac_strategy;

bool		enable_simple_analyze;

static void do_analyze_rel(Relation onerel, int options,
						   VacuumParams *params, List *va_cols,
						   AcquireSampleRowsFunc acquirefunc, BlockNumber relpages,
						   bool inh, bool in_outer_xact, int elevel);
static void compute_index_stats(Relation onerel, double totalrows,
								AnlIndexData *indexdata, int nindexes,
								HeapTuple *rows, int numrows,
								MemoryContext col_context,
								MemoryContext anl_context);
static VacAttrStats *examine_attribute(Relation onerel, int attnum,
									   Node *index_expr, MemoryContext anl_context);
static int acquire_sample_rows(Relation onerel, int elevel,
							   HeapTuple *rows, int targrows,
							   double *totalrows, double *totaldeadrows);

static int	compare_rows(const void *a, const void *b);
static int acquire_inherited_sample_rows(Relation onerel, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows, double *totaldeadrows,
							  bool update_catalog);
static void update_attstats(Oid relid, bool inh,
				int natts, VacAttrStats **vacattrstats, char relpersistence,
				bool local_update);
static Datum std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);
static Datum ind_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);

#ifdef XCP
#ifndef __OPENTENBASE__
static void analyze_rel_coordinator(Relation onerel, bool inh, int attr_cnt,
									VacAttrStats **vacattrstats, int nindexes,
									Relation *indexes, AnlIndexData *indexdata);
#endif
#endif
#ifdef __OPENTENBASE__
static void get_rel_pages_visiblepages(Relation onerel, 
						   BlockNumber *pages, 
						   BlockNumber *visiblepages);
static int acquire_coordinator_sample_rows(Relation onerel, int elevel,
												HeapTuple *rows, int targrows,
												double *totalrows, double *totaldeadrows,
												int64 *totalpages, int64 *visiblepages);
#define SILO_HAS_NULL_VALUE(info) ((info) & Silo_Info_Has_Null ? true : false)
#endif
static void update_pgstatistic(Oid relid, int natts, bool inh,
	AnalyzeRelStats *analyzeStats, int stats_starts);
static void update_pgstatistic_ext(AnalyzeRelStats *analyzeStats);
static void update_ext_stats(Name nspname, Name name, 
									bytea *ndistinct, bytea *dependencies);
static void clear_analyze_stat(AnalyzeRelStats *analyzeStats);

/*
 *	analyze_rel() -- analyze one relation
 */
void
analyze_rel(Oid relid, RangeVar *relation, int options,
			VacuumParams *params, List *va_cols, bool in_outer_xact,
			BufferAccessStrategy bstrategy)
{
	Relation	onerel;
	int			elevel;
	AcquireSampleRowsFunc acquirefunc = NULL;
	BlockNumber relpages = 0;
	LOCKMODE	lockmode;

	/* Ignore ANALYZE (LOCAL) if we are not in outer transaction */
	if (!in_outer_xact && ANALYZE_LOCAL(options))
		return;

	/* Select logging level */
	if (options & VACOPT_VERBOSE)
	{
		elevel = INFO;
	}
	else
	{
		elevel = DEBUG2;
	}

	/* Set up static variables */
	vac_strategy = bstrategy;
	/*
	 * Check for user-requested abort.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Open the relation, getting ShareUpdateExclusiveLock to ensure that two
	 * ANALYZEs don't run on it concurrently.  (This also locks out a
	 * concurrent VACUUM, which doesn't matter much at the moment but might
	 * matter if we ever try to accumulate stats on dead tuples.) If the rel
	 * has been dropped since we last saw it, we don't need to process it.
	 *
	 * ANALYZE (LOCAL) do not update pg_class and pg_statistic heap data, so we
	 * hold RowExclusiveLock to allow concurrent ANALYZE (LOCAL).
	 */

	lockmode = ANALYZE_LOCAL(options) ? RowExclusiveLock : ShareUpdateExclusiveLock;

	if (!(options & VACOPT_NOWAIT))
	{
		onerel = try_relation_open(relid, lockmode);
	}
	else if (ConditionalLockRelationOid(relid, lockmode))
	{
		onerel = try_relation_open(relid, NoLock);
	}
	else
	{
		onerel = NULL;

		if (IsAutoVacuumWorkerProcess() && params->log_min_duration >= 0)
			ereport(LOG,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("skipping analyze of \"%s\" --- lock not available",
							relation->relname)));
	}

	if (!onerel)
	{
		return;
	}

	/*
	 * A role has privileges to vacuum or analyze the relation if any of the
	 * following are true:
	 *   - the role is a superuser
	 *   - the role owns the relation
	 *   - the role owns the current database and the relation is not shared
	 *   - the role has been granted the MAINTAIN privilege on the relation
	 */
	if (!(pg_class_ownercheck(RelationGetRelid(onerel), GetUserId()) ||
		  (pg_database_ownercheck(MyDatabaseId, GetUserId()) && !onerel->rd_rel->relisshared) ||
		  pg_class_aclcheck(relid, GetUserId(), ACL_MAINTAIN) == ACLCHECK_OK ||
			has_partition_ancestor_privs(relid, GetUserId(), ACL_MAINTAIN)))
	{
		/* No need for a WARNING if we already complained during VACUUM */
		if (!(options & VACOPT_VACUUM))
		{
			if (onerel->rd_rel->relisshared)
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser can analyze it",
								RelationGetRelationName(onerel))));

#ifdef _PG_ORCL_
			else if (IsSystemNamespace(onerel->rd_rel->relnamespace))
#else
			else if (onerel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
#endif
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser or database owner can analyze it",
								RelationGetRelationName(onerel))));
			else
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only table or database owner can analyze it",
								RelationGetRelationName(onerel))));
		}

		relation_close(onerel, lockmode);
		return;
	}

	/*
	 * Silently ignore tables that are temp tables of other backends ---
	 * trying to analyze these is rather pointless, since their contents are
	 * probably not up-to-date on disk.  (We don't throw a warning here; it
	 * would just lead to chatter during a database-wide ANALYZE.)
	 */
	if (RELATION_IS_OTHER_TEMP(onerel))
	{
		if (onerel->rd_backend == InvalidBackendId)
			elog(LOG,
			     "Failed to open the temporary-table because the temp-namespace has been removed.");

		relation_close(onerel, lockmode);
		return;
	}

	/*
	 * We can ANALYZE any table except pg_statistic. See update_attstats
	 */
	if (RelationGetRelid(onerel) == StatisticRelationId)
	{
		relation_close(onerel, lockmode);
		return;
	}

	/*
	 * Check that it's a plain table, materialized view, or foreign table; we
	 * used to do this in get_rel_oids() but seems safer to check after we've
	 * locked the relation.
	 */
	if (onerel->rd_rel->relkind == RELKIND_RELATION ||
		onerel->rd_rel->relkind == RELKIND_MATVIEW||
		onerel->rd_rel->relkind == RELKIND_GLOBAL_INDEX||
		onerel->rd_rel->relkind == RELKIND_PARTITIONED_GLOBAL_INDEX)
	{
			acquirefunc = acquire_sample_rows;
			/* Also get regular table's size */
			relpages = RelationGetNumberOfBlocks(onerel);

	}
	else if (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * For a foreign table, call the FDW's hook function to see whether it
		 * supports analysis.
		 */
		FdwRoutine *fdwroutine;
		bool		ok = false;
		fdwroutine = GetFdwRoutineForRelation(onerel, false);

		if (fdwroutine->AnalyzeForeignTable != NULL)
			ok = fdwroutine->AnalyzeForeignTable(onerel,
												 &acquirefunc,
												 &relpages);

		if (!ok)
		{
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot analyze this foreign table",
							RelationGetRelationName(onerel))));
			relation_close(onerel, lockmode);
			return;
		}
	}
	else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		/*
		 * For partitioned tables, we want to do the recursive ANALYZE below.
		 */
	}
	else
	{
		/* No need for a WARNING if we already complained during VACUUM */
		if (!(options & VACOPT_VACUUM))
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot analyze non-tables or special system tables",
							RelationGetRelationName(onerel))));

		relation_close(onerel, lockmode);
		return;
	}

	/*
	 * OK, let's do it.  First let other backends know I'm in ANALYZE.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags |= PROC_IN_ANALYZE;
	LWLockRelease(ProcArrayLock);

		/*
		 * Do the normal non-recursive ANALYZE.  We can skip this for partitioned
		 * tables, which don't contain any rows.
		 */
		if (onerel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
			do_analyze_rel(onerel, options, params, va_cols, acquirefunc,
						   relpages, false, in_outer_xact, elevel);

		/*
		 * If there are child tables, do recursive ANALYZE.
		 */
		if (onerel->rd_rel->relhassubclass)
			do_analyze_rel(onerel, options, params, va_cols, acquirefunc, relpages,
						   true, in_outer_xact, elevel);

	/*
	 * Close source relation now, but keep lock so that no one deletes it
	 * before we commit.  (If someone did, they'd fail to clean up the entries
	 * we made in pg_statistic.  Also, releasing the lock before commit would
	 * expose us to concurrent-update failures in update_attstats.)
	 */
	relation_close(onerel, NoLock);
	/*
	 * Reset my PGXACT flag.  Note: we need this here, and not in vacuum_rel,
	 * because the vacuum flag is cleared by the end-of-xact code.
	 */
	LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
	MyPgXact->vacuumFlags &= ~PROC_IN_ANALYZE;
	LWLockRelease(ProcArrayLock);
}

static void
store_analyze_stat_info(AnalyzeRelStats *analyzeStat, int attnum, VacAttrStats **vacattrstats, int store_idx)
{
	int attidx = 0;
	int i = store_idx;
	int j, m;

	for (attidx = 0; attidx < attnum; attidx++)
	{
		VacAttrStats *stats = vacattrstats[attidx];
		if (!stats->stats_valid)
		{
			continue;
		}

		analyzeStat->pgstat[i].attnum = stats->attr->attnum;
		analyzeStat->pgstat[i].stanullfrac = stats->stanullfrac;
		analyzeStat->pgstat[i].stawidth = stats->stawidth;
		analyzeStat->pgstat[i].stadistinct = stats->stadistinct;

		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			analyzeStat->pgstat[i].stakind[j] = stats->stakind[j];
			analyzeStat->pgstat[i].staop[j] = stats->staop[j];
			analyzeStat->pgstat[i].numnumbers[j] = stats->numnumbers[j];
			analyzeStat->pgstat[i].numvalues[j] = stats->numvalues[j];
			analyzeStat->pgstat[i].statypid[j] = stats->statypid[j];
			analyzeStat->pgstat[i].statyplen[j] = stats->statyplen[j];
			analyzeStat->pgstat[i].statypbyval[j] = stats->statypbyval[j];
			analyzeStat->pgstat[i].statypalign[j] = stats->statypalign[j];
			if (stats->numnumbers[j] > 0)
			{
				analyzeStat->pgstat[i].stanumbers[j] =
					(float4*)palloc0(stats->numnumbers[j] * sizeof(float4));
			}
			else
			{
				analyzeStat->pgstat[i].stanumbers[j] = NULL;
			}

			if (stats->numvalues[j] > 0)
			{
				analyzeStat->pgstat[i].stavalues[j] =
					(Datum*)palloc0(stats->numvalues[j] * sizeof(Datum));
				analyzeStat->pgstat[i].stavalueslen[j] =
					(int32*)palloc0(stats->numvalues[j] * sizeof(int32));
			}
			else
			{
				analyzeStat->pgstat[i].stavalues[j] = NULL;
				analyzeStat->pgstat[i].stavalueslen[j] = NULL;
			}

			for(m = 0; m < stats->numvalues[j]; m++)
			{
				analyzeStat->pgstat[i].stavalues[j][m] = stats->stavalues[j][m];
				analyzeStat->pgstat[i].stavalueslen[j][m] =
					datumGetSize(stats->stavalues[j][m],
									stats->statypbyval[j],
									stats->statyplen[j]);
			}
			for(m = 0; m < stats->numnumbers[j]; m++)
			{
				analyzeStat->pgstat[i].stanumbers[j][m] = stats->stanumbers[j][m];
			}
		}
		i++;
	}
}

int 
update_analyze_stat(AnalyzeRelStats *analyzeStats)
{
	Relation onerel;
	Relation *Irel = NULL;
	Oid		relid = InvalidOid;
	Oid		namespaceId;
	int 	nindexes = 0;
	int 	i, j;
	int 	last_natts;

	namespaceId = LookupNamespaceNoError(analyzeStats->namespacename);
	relid = get_relname_relid(analyzeStats->relname, namespaceId);

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errmsg("could not open relation with Oid 0, namespace %s, relname %s",
					analyzeStats->namespacename, analyzeStats->relname)));
	if (ConditionalLockRelationOid(relid, ShareUpdateExclusiveLock))
	{
		onerel = try_relation_open(relid, NoLock);
	}
	else
	{
		return -1;
	}

	if (onerel == NULL)
	{
		ereport(ERROR, 
				(errmsg("could not open relation %s.%s", 
					analyzeStats->namespacename, analyzeStats->relname)));
	}

	if (!analyzeStats->inh)
	{
		vac_open_indexes(onerel, AccessShareLock, &nindexes, &Irel);

		vac_update_relstats(onerel, analyzeStats->relpages[0],
								analyzeStats->reltuples[0],
								analyzeStats->relallvisible,
								analyzeStats->relhasindex,
								InvalidTransactionId,
								InvalidMultiXactId,
								false);
	}

	/* update pg_statistic info */
	update_pgstatistic(relid, analyzeStats->stat_att_num[0], analyzeStats->inh,
								analyzeStats, 0);

	last_natts = analyzeStats->stat_att_num[0];
	if (nindexes > 0 && analyzeStats->class_info_num > 1)
	{
		for(i = 0; i < analyzeStats->class_info_num - 1; i++)
		{
			for (j = 0; j < nindexes; j++)
			{
				if (strcmp(analyzeStats->indexName[i].data, Irel[j]->rd_rel->relname.data) == 0)
				{
					vac_update_relstats(Irel[j], analyzeStats->relpages[i+1],
									analyzeStats->reltuples[i+1],
									0,
									false,
									InvalidTransactionId,
									InvalidMultiXactId,
									false);
					if (analyzeStats->stat_att_num[i+1] > 0)
					{
						update_pgstatistic(RelationGetRelid(Irel[j]),
											analyzeStats->stat_att_num[i+1],
											false, analyzeStats, last_natts);
						last_natts += analyzeStats->stat_att_num[i+1];
					}
					break;
				}
			}
		}
	}

	update_pgstatistic_ext(analyzeStats);
	CommandCounterIncrement();
	if (!analyzeStats->inh)
	{
		pgstat_report_analyze(onerel, analyzeStats->reltuples[0], analyzeStats->deadrows, analyzeStats->resetcounter);
	}
	else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		pgstat_report_analyze(onerel, analyzeStats->reltuples[0], analyzeStats->deadrows, analyzeStats->resetcounter);
	}

	vac_close_indexes(nindexes, Irel, NoLock);
	relation_close(onerel, NoLock);
	return 0;
}

static void
clear_analyze_stat(AnalyzeRelStats *analyzeStats)
{
	int i, j;

	pfree(analyzeStats->relname);
	pfree(analyzeStats->namespacename);
	pfree(analyzeStats->relpages);
	pfree(analyzeStats->reltuples);

	if (analyzeStats->indexName != NULL)
	{
		pfree(analyzeStats->indexName);
	}
	if (analyzeStats->stat_att_num != NULL)
	{
		pfree(analyzeStats->stat_att_num);
	}

	for (i = 0; i < analyzeStats->stat_info_num; i++)
	{
		for (j = 0; j < STATISTIC_NUM_SLOTS; j++)
		{
			if (analyzeStats->pgstat[i].stanumbers[j] != NULL)
				pfree(analyzeStats->pgstat[i].stanumbers[j]);

			if (analyzeStats->pgstat[i].stavalues[j] != NULL)
			{
				pfree(analyzeStats->pgstat[i].stavalues[j]);
				pfree(analyzeStats->pgstat[i].stavalueslen[j]);
			}
		}
	}
	if (analyzeStats->stat_info_num > 0)
	{
		pfree(analyzeStats->pgstat);
	}
	
	for (i = 0; i < analyzeStats->ext_info_num; i++)
	{
		if (analyzeStats->pgstatext[i].distinct != NULL) 
			pfree(analyzeStats->pgstatext[i].distinct);
		if (analyzeStats->pgstatext[i].depend != NULL)
			pfree(analyzeStats->pgstatext[i].depend);
	}
	if (analyzeStats->ext_info_num > 0)
	{
		pfree(analyzeStats->pgstatext);
	}
	pfree(analyzeStats);
}

/*
 *	do_analyze_rel() -- analyze one relation, recursively or not
 *
 * Note that "acquirefunc" is only relevant for the non-inherited case.
 * For the inherited case, acquire_inherited_sample_rows() determines the
 * appropriate acquirefunc for each child table.
 */
static void
do_analyze_rel(Relation onerel, int options, VacuumParams *params,
			   List *va_cols, AcquireSampleRowsFunc acquirefunc,
			   BlockNumber relpages, bool inh, bool in_outer_xact,
			   int elevel)
{
	int			attr_cnt,
				tcnt,
				i,
				ind;
	Relation   *Irel;
	int			nindexes;
	bool		hasindex;
	VacAttrStats **vacattrstats;
	AnlIndexData *indexdata;
	int			targrows,
				numrows;
	double		totalrows,
				totaldeadrows;
	HeapTuple  *rows;
	PGRUsage	ru0;
	TimestampTz starttime = 0;
	MemoryContext caller_context;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	bool		iscoordinator = false;
	int64		coordpages = 0;
	int64		coordvisiblepages = 0;
	MemoryContext anl_context;
	AnalyzeRelStats *analyzeStat = NULL;

	if (inh)
		ereport(elevel,
				(errmsg("analyzing \"%s.%s\" inheritance tree",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));
	else
		ereport(elevel,
				(errmsg("analyzing \"%s.%s\"",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));

	/*
	 * Set up a working context so that we can easily free whatever junk gets
	 * created.
	 */
	anl_context = AllocSetContextCreate(CurrentMemoryContext,
										"Analyze",
										ALLOCSET_DEFAULT_SIZES);
	caller_context = MemoryContextSwitchTo(anl_context);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(onerel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/* measure elapsed time iff autovacuum logging requires it */
	if ((IsAutoVacuumWorkerProcess()) &&
		 params->log_min_duration >= 0)
	{
		pg_rusage_init(&ru0);

		if (params->log_min_duration > 0)
		{
			starttime = GetCurrentTimestamp();
		}
	}

	/*
	 * Determine which columns to analyze
	 *
	 * Note that system attributes are never analyzed.
	 */
	if (va_cols != NIL)
	{
		ListCell   *le;
		vacattrstats = (VacAttrStats **) palloc(list_length(va_cols) *
												sizeof(VacAttrStats *));
		tcnt = 0;

		foreach (le, va_cols)
		{
			char	   *col = strVal(lfirst(le));
			i = attnameAttNum(onerel, col, false);

			if (i == InvalidAttrNumber)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" of relation \"%s\" does not exist",
								col, RelationGetRelationName(onerel))));

			vacattrstats[tcnt] = examine_attribute(onerel, i, NULL, anl_context);

			if (vacattrstats[tcnt] != NULL)
			{
				tcnt++;
			}
		}

		attr_cnt = tcnt;
	}
	else
	{
		attr_cnt = onerel->rd_att->natts;
		vacattrstats = (VacAttrStats **)
					   palloc(attr_cnt * sizeof(VacAttrStats *));
		tcnt = 0;

		for (i = 1; i <= attr_cnt; i++)
		{
			vacattrstats[tcnt] = examine_attribute(onerel, i, NULL, anl_context);

			if (vacattrstats[tcnt] != NULL)
			{
				tcnt++;
			}
		}

		attr_cnt = tcnt;
	}

	/*
	 * Open all indexes of the relation, and see if there are any analyzable
	 * columns in the indexes.  We do not analyze index columns if there was
	 * an explicit column list in the ANALYZE command, however.  If we are
	 * doing a recursive scan, we don't want to touch the parent's indexes at
	 * all.
	 */
	if (!inh)
	{
		vac_open_indexes(onerel, AccessShareLock, &nindexes, &Irel);
	}
	else
	{
		Irel = NULL;
		nindexes = 0;
	}

	hasindex = (nindexes > 0);
	indexdata = NULL;

	if (hasindex)
	{
		indexdata = (AnlIndexData *) palloc0(nindexes * sizeof(AnlIndexData));

		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];
			IndexInfo  *indexInfo;
			thisdata->indexInfo = indexInfo = BuildIndexInfo(Irel[ind]);
			thisdata->tupleFract = 1.0; /* fix later if partial */

			if (indexInfo->ii_Expressions != NIL && va_cols == NIL)
			{
				ListCell   *indexpr_item = list_head(indexInfo->ii_Expressions);
				thisdata->vacattrstats = (VacAttrStats **)
										 palloc(indexInfo->ii_NumIndexAttrs * sizeof(VacAttrStats *));
				tcnt = 0;

				for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
				{
					int			keycol = indexInfo->ii_KeyAttrNumbers[i];

					if (keycol == 0)
					{
						/* Found an index expression */
						Node	   *indexkey;

						if (indexpr_item == NULL)	/* shouldn't happen */
						{
							elog(ERROR, "too few entries in indexprs list");
						}

						indexkey = (Node *) lfirst(indexpr_item);
						indexpr_item = lnext(indexpr_item);
						thisdata->vacattrstats[tcnt] =
							examine_attribute(Irel[ind], i + 1, indexkey, anl_context);

						if (thisdata->vacattrstats[tcnt] != NULL)
						{
							tcnt++;
						}
					}
				}

				thisdata->attr_cnt = tcnt;
			}
		}
	}
	iscoordinator = (IS_PGXC_COORDINATOR && 
					 onerel->rd_locator_info && 
					 !RELATION_IS_COORDINATOR_LOCAL(onerel));

#ifndef __OPENTENBASE__
#ifdef XCP
	if (iscoordinator)
	{
		/*
		 * Fetch relation statistics from remote nodes and update
		 */
		vacuum_rel_coordinator(onerel, in_outer_xact, params);

		/*
		 * Fetch attribute statistics from remote nodes.
		 */
		analyze_rel_coordinator(onerel, inh, attr_cnt, vacattrstats,
								nindexes, Irel, indexdata);
		/*
		 * Skip acquiring local stats. Coordinator does not store data of
		 * distributed tables.
		 */
		goto cleanup;
	}
#endif
#endif
	/*
	 * Determine how many rows we need to sample, using the worst case from
	 * all analyzable columns.  We use a lower bound of 100 rows to avoid
	 * possible overflow in Vitter's algorithm.  (Note: that will also be the
	 * target in the corner case where there are no analyzable columns.)
	 */
	targrows = 100;

	for (i = 0; i < attr_cnt; i++)
	{
		if (targrows < vacattrstats[i]->minrows)
		{
			targrows = vacattrstats[i]->minrows;
		}
	}

	for (ind = 0; ind < nindexes; ind++)
	{
		AnlIndexData *thisdata = &indexdata[ind];

		for (i = 0; i < thisdata->attr_cnt; i++)
		{
			if (targrows < thisdata->vacattrstats[i]->minrows)
			{
				targrows = thisdata->vacattrstats[i]->minrows;
			}
		}
	}

	/*
	 * Acquire the sample rows
	 */
	rows = (HeapTuple *) palloc0(targrows * sizeof(HeapTuple));
#ifdef __OPENTENBASE__
	if (iscoordinator)
	{
		numrows = acquire_coordinator_sample_rows(onerel, elevel,
												rows, targrows,
												&totalrows, &totaldeadrows,
												&coordpages, &coordvisiblepages);
	}
	else
#endif
	/*
	 * It's not safe for ANALYZE (LOCAL) to update catalog because we are holding
	 * RowExclusiveLock. See commend about relhassubclass in DefineRelation.
	 */
	if (inh)
		numrows = acquire_inherited_sample_rows(onerel, elevel,
												rows, targrows,
												&totalrows, &totaldeadrows,
												!ANALYZE_LOCAL(options));
	else
		numrows = (*acquirefunc)(onerel, elevel,
								 rows, targrows,
								 &totalrows, &totaldeadrows);

	if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
		onerel->rd_rel->relpersistence != RELPERSISTENCE_TEMP &&
		!IsSystemRelation(onerel) &&
		!ANALYZE_LOCAL(options))
	{
		analyzeStat = (AnalyzeRelStats*)palloc0(sizeof(AnalyzeRelStats));

		analyzeStat->class_info_num = 1 + nindexes;
		analyzeStat->relpages = (int32*)palloc0(sizeof(int32) * analyzeStat->class_info_num);
		analyzeStat->reltuples = (float4*)palloc0(sizeof(float4) * analyzeStat->class_info_num);
		analyzeStat->stat_att_num = (int32*)palloc0(sizeof(int32) * analyzeStat->class_info_num);
		if (nindexes > 0)
		{
			analyzeStat->indexName = (NameData*)palloc0(sizeof(NameData) * nindexes);
		}
		analyzeStat->stat_info_num = 0;
		analyzeStat->ext_info_num = 0;
		analyzeStat->relname = pstrdup(RelationGetRelationName(onerel));
		analyzeStat->namespacename = get_namespace_name(RelationGetNamespace(onerel));
		analyzeStat->inh = inh;
	}

	/*
	 * Compute the statistics.  Temporary results during the calculations for
	 * each column are stored in a child context.  The calc routines are
	 * responsible to make sure that whatever they store into the VacAttrStats
	 * structure is allocated in anl_context.
	 */
	if (numrows > 0)
	{
		MemoryContext col_context,
					  old_context;
		col_context = AllocSetContextCreate(anl_context,
											"Analyze Column",
											ALLOCSET_DEFAULT_SIZES);
		old_context = MemoryContextSwitchTo(col_context);

		for (i = 0; i < attr_cnt; i++)
		{
			VacAttrStats *stats = vacattrstats[i];
			AttributeOpts *aopt;
			stats->rows = rows;
			stats->tupDesc = onerel->rd_att;

			stats->compute_stats(stats,
						 	 	 	 std_fetch_func,
									 numrows,
									 totalrows);

			/*
			 * If the appropriate flavor of the n_distinct option is
			 * specified, override with the corresponding value.
			 */
			aopt = get_attribute_options(onerel->rd_id, stats->attr->attnum);

			if (aopt != NULL)
			{
				float8		n_distinct;
				n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;

				if (n_distinct != 0.0)
				{
					stats->stadistinct = n_distinct;
				}
			}

			MemoryContextResetAndDeleteChildren(col_context);
		}

		if (hasindex)
			compute_index_stats(onerel, totalrows,
								indexdata, nindexes,
								rows, numrows,
								col_context,
								anl_context);

		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(col_context);

		/*
		 * Emit the completed stats rows into pg_statistic, replacing any
		 * previous statistics for the target columns.  (If there are stats in
		 * pg_statistic for columns we didn't process, we leave them alone.)
		 */
		update_attstats(RelationGetRelid(onerel), inh,
						attr_cnt, vacattrstats, RelationGetRelPersistence(onerel),
						ANALYZE_LOCAL(options));

		if (analyzeStat != NULL)
		{
			analyzeStat->stat_info_num = attr_cnt;
			analyzeStat->stat_att_num[0] = attr_cnt;
			for (ind = 0; ind < nindexes; ind++)
			{
				analyzeStat->stat_info_num += indexdata[ind].attr_cnt;
				analyzeStat->stat_att_num[ind+1] = indexdata[ind].attr_cnt;
			}
			analyzeStat->pgstat = (AnalyzePgStats*)palloc0(sizeof(AnalyzePgStats) * analyzeStat->stat_info_num);
			store_analyze_stat_info(analyzeStat, attr_cnt, vacattrstats, 0);
		}

		i = attr_cnt;
		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];
			update_attstats(RelationGetRelid(Irel[ind]), false,
							thisdata->attr_cnt, thisdata->vacattrstats,
							RelationGetRelPersistence(Irel[ind]), ANALYZE_LOCAL(options));

			if (analyzeStat != NULL)
			{
				store_analyze_stat_info(analyzeStat, thisdata->attr_cnt, thisdata->vacattrstats, i);
				i += thisdata->attr_cnt;
			}

		}

		/*
		 * Build extended statistics (if there are any).
		 *
		 * For now we only build extended statistics on individual relations,
		 * not for relations representing inheritance trees.
		 */
		if (!inh)
			BuildRelationExtStatistics(onerel, totalrows, numrows, rows, NULL, attr_cnt,
                                                                   vacattrstats, analyzeStat);
	}

	/*
	 * Update pages/tuples stats in pg_class ... but not if we're doing
	 * inherited stats.
	 */
	if (!inh)
	{
		BlockNumber relallvisible;
#ifdef __OPENTENBASE__
		if (iscoordinator)
		{
			relpages = coordpages;
			relallvisible = coordvisiblepages;
		}
		else 
		{
#endif
		visibilitymap_count(onerel, &relallvisible, NULL);

#ifdef __OPENTENBASE__
		}
#endif

		if (ANALYZE_LOCAL(options))
			LocalStatisticSetRelStats(onerel, relpages, relallvisible, totalrows, totaldeadrows, true);
		else
			vac_update_relstats(onerel,
								relpages,
								totalrows,
								relallvisible,
								hasindex,
								InvalidTransactionId,
								InvalidMultiXactId,
								in_outer_xact);

		if (analyzeStat != NULL)
		{
			analyzeStat->relpages[0] = relpages;
			analyzeStat->reltuples[0] = totalrows;
			analyzeStat->relallvisible = relallvisible;
			analyzeStat->relhasindex = hasindex;
		}
	}

	/*
	 * Same for indexes. Vacuum always scans all indexes, so if we're part of
	 * VACUUM ANALYZE, don't overwrite the accurate count already inserted by
	 * VACUUM.
	 */
	if (!inh && !(options & VACOPT_VACUUM))
	{
		for (ind = 0; ind < nindexes; ind++)
		{
			AnlIndexData *thisdata = &indexdata[ind];
			double		totalindexrows;
			int 		indexpages;
			totalindexrows = ceil(thisdata->tupleFract * totalrows);

			if (RelationIsCrossNodeIndex(Irel[ind]->rd_rel))
			{
				do_analyze_rel(Irel[ind], options, params, va_cols, acquirefunc, relpages, inh, in_outer_xact, elevel);
			}
			else if (IS_PGXC_COORDINATOR && onerel->rd_locator_info)
            {
                int  idx_nodes;
                bool replicated;
                char *nspname;
                char *relname;
                int32 idx_allvisible_pages;
                float4 idx_tuples;
                TransactionId idx_frozenxid;
                
                replicated = IsLocatorReplicated(RelationGetLocatorType(onerel));
                relname = RelationGetRelationName(Irel[ind]);
                nspname = get_namespace_name(RelationGetNamespace(Irel[ind]));
                idx_nodes = get_remote_relstat(nspname, relname, replicated,
                                               &indexpages, &idx_allvisible_pages,
                                               &idx_tuples, &idx_frozenxid);
                totalindexrows = idx_tuples;
                if (idx_nodes > 0)
                {
                    /* save changes */
					if (ANALYZE_LOCAL(options))
						LocalStatisticSetRelStats(Irel[ind], indexpages, idx_allvisible_pages, totalindexrows, -1, true);
					else
						vac_update_relstats(Irel[ind],
											(BlockNumber) indexpages,
											(double) totalindexrows,
											0,
											false,
											InvalidTransactionId,
											InvalidMultiXactId,
											in_outer_xact);
                    if (analyzeStat != NULL)
                    {
                        analyzeStat->relpages[ind+1] = indexpages;
                        analyzeStat->reltuples[ind+1] = totalindexrows;
                        memcpy(NameStr(analyzeStat->indexName[ind]), RelationGetRelationName(Irel[ind]), NAMEDATALEN);
                    }
                }
            }
            else
			{
				indexpages = RelationGetNumberOfBlocks(Irel[ind]);
				if (ANALYZE_LOCAL(options))
					LocalStatisticSetRelStats(Irel[ind], indexpages, 0, totalindexrows, -1, true);
				else
					vac_update_relstats(Irel[ind],
										indexpages,
										totalindexrows,
										0,
										false,
										InvalidTransactionId,
										InvalidMultiXactId,
										in_outer_xact);
				if (analyzeStat != NULL)
				{
					analyzeStat->relpages[ind+1] = indexpages;
					analyzeStat->reltuples[ind+1] = totalindexrows;
					memcpy(NameStr(analyzeStat->indexName[ind]), RelationGetRelationName(Irel[ind]), NAMEDATALEN);
				}
			}
		}
	}
	if (analyzeStat != NULL)
	{
		analyzeStat->reltuples[0] = totalrows;
		analyzeStat->deadrows = totaldeadrows;
		analyzeStat->resetcounter = (va_cols == NIL);
		ExecSendStats(analyzeStat);
		clear_analyze_stat(analyzeStat);
	}

#ifdef XCP
	/*
	 * Coordinator skips getting local stats of distributed table up to here
	 */
#endif

	/*
	 * Report ANALYZE to the stats collector, too.  However, if doing
	 * inherited stats we shouldn't report, because the stats collector only
	 * tracks per-table stats.  Reset the changes_since_analyze counter only
	 * if we analyzed all columns; otherwise, there is still work for
	 * auto-analyze to do.
	 *
	 * If doing ANALYZE (LOCAL), shouldn't report.
	 */
	if (!ANALYZE_LOCAL(options))
	{
		if (!inh)
		{
			pgstat_report_analyze(onerel, totalrows, totaldeadrows,
								(va_cols == NIL));
		}
		else if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			pgstat_report_analyze(onerel, totalrows, totaldeadrows,
								(va_cols == NIL));
		}
	}
	/* If this isn't part of VACUUM ANALYZE, let index AMs do cleanup */
	if (!(options & VACOPT_VACUUM))
	{
		for (ind = 0; ind < nindexes; ind++)
		{
			IndexBulkDeleteResult *stats;
			IndexVacuumInfo ivinfo;

			memset(&ivinfo, 0, sizeof(IndexVacuumInfo));
			ivinfo.index = Irel[ind];
			ivinfo.analyze_only = true;
			ivinfo.estimated_count = true;
			ivinfo.message_level = elevel;
			ivinfo.num_heap_tuples = onerel->rd_rel->reltuples;
			ivinfo.strategy = vac_strategy;
			stats = index_vacuum_cleanup(&ivinfo, NULL);

			if (stats)
			{
				pfree(stats);
			}
		}
	}

	/* Done with indexes */
	vac_close_indexes(nindexes, Irel, NoLock);

	/* Log the action if appropriate */
	if ((IsAutoVacuumWorkerProcess()) &&
		  params->log_min_duration >= 0)
	{
		if (params->log_min_duration == 0 ||
			TimestampDifferenceExceeds(starttime, GetCurrentTimestamp(),
									   params->log_min_duration))
			ereport(LOG,
					(errmsg("automatic analyze of table \"%s.%s.%s\" system usage: %s",
							get_database_name(MyDatabaseId),
							get_namespace_name(RelationGetNamespace(onerel)),
							RelationGetRelationName(onerel),
							pg_rusage_show(&ru0))));
	}

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);
	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);
	/* Restore current context and release memory */
	MemoryContextSwitchTo(caller_context);
	MemoryContextDelete(anl_context);
	anl_context = NULL;
}

/*
 * Compute statistics about indexes of a relation
 */
static void
compute_index_stats(Relation onerel, double totalrows,
					AnlIndexData *indexdata, int nindexes,
					HeapTuple *rows, int numrows,
					MemoryContext col_context,
					MemoryContext anl_context)
{
	MemoryContext ind_context,
				  old_context;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	int			ind,
				i;
	ind_context = AllocSetContextCreate(anl_context,
										"Analyze Index",
										ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(ind_context);

	for (ind = 0; ind < nindexes; ind++)
	{
		AnlIndexData *thisdata = &indexdata[ind];
		IndexInfo  *indexInfo = thisdata->indexInfo;
		int			attr_cnt = thisdata->attr_cnt;
		TupleTableSlot *slot;
		EState	   *estate;
		ExprContext *econtext;
		ExprState  *predicate;
		Datum	   *exprvals;
		bool	   *exprnulls;
		int			numindexrows,
					tcnt,
					rowno;
		double		totalindexrows;

		/* Ignore index if no columns to analyze and not partial */
		if (attr_cnt == 0 && indexInfo->ii_Predicate == NIL)
		{
			continue;
		}

		/*
		 * Need an EState for evaluation of index expressions and
		 * partial-index predicates.  Create it in the per-index context to be
		 * sure it gets cleaned up at the bottom of the loop.
		 */
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);
		/* Need a slot to hold the current heap tuple, too */
		slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel));
		/* Arrange for econtext's scan tuple to be the tuple under test */
		econtext->ecxt_scantuple = slot;
		/* Set up execution state for predicate. */
		predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
		/* Compute and save index expression values */
		exprvals = (Datum *) palloc(numrows * attr_cnt * sizeof(Datum));
		exprnulls = (bool *) palloc(numrows * attr_cnt * sizeof(bool));
		numindexrows = 0;
		tcnt = 0;

		for (rowno = 0; rowno < numrows; rowno++)
		{
			HeapTuple	heapTuple = rows[rowno];
			vacuum_delay_point();
			/*
			 * Reset the per-tuple context each time, to reclaim any cruft
			 * left behind by evaluating the predicate or index expressions.
			 */
			ResetExprContext(econtext);
			/* Set up for predicate or expression evaluation */
			ExecStoreHeapTuple(heapTuple, slot, false);

			/* If index is partial, check predicate */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
				{
					continue;
				}
			}

			numindexrows++;

			if (attr_cnt > 0)
			{
				/*
				 * Evaluate the index row to compute expression values. We
				 * could do this by hand, but FormIndexDatum is convenient.
				 */
				FormIndexDatum(indexInfo,
							   slot,
							   estate,
							   values,
							   isnull);

				/*
				 * Save just the columns we care about.  We copy the values
				 * into ind_context from the estate's per-tuple context.
				 */
				for (i = 0; i < attr_cnt; i++)
				{
					VacAttrStats *stats = thisdata->vacattrstats[i];
					int			attnum = stats->attr->attnum;

					if (isnull[attnum - 1])
					{
						exprvals[tcnt] = (Datum) 0;
						exprnulls[tcnt] = true;
					}
					else
					{
						exprvals[tcnt] = datumCopy(values[attnum - 1],
												   stats->attrtype->typbyval,
												   stats->attrtype->typlen);
						exprnulls[tcnt] = false;
					}

					tcnt++;
				}
			}
		}

		/*
		 * Having counted the number of rows that pass the predicate in the
		 * sample, we can estimate the total number of rows in the index.
		 */
		thisdata->tupleFract = (double) numindexrows / (double) numrows;
		totalindexrows = ceil(thisdata->tupleFract * totalrows);

		/*
		 * Now we can compute the statistics for the expression columns.
		 */
		if (numindexrows > 0)
		{
			MemoryContextSwitchTo(col_context);

			for (i = 0; i < attr_cnt; i++)
			{
				VacAttrStats *stats = thisdata->vacattrstats[i];
				AttributeOpts *aopt =
					get_attribute_options(stats->attr->attrelid,
										  stats->attr->attnum);
				stats->exprvals = exprvals + i;
				stats->exprnulls = exprnulls + i;
				stats->rowstride = attr_cnt;
				stats->compute_stats(stats,
								 	 	 ind_fetch_func,
										 numindexrows,
										 totalindexrows);

				/*
				 * If the n_distinct option is specified, it overrides the
				 * above computation.  For indices, we always use just
				 * n_distinct, not n_distinct_inherited.
				 */
				if (aopt != NULL && aopt->n_distinct != 0.0)
				{
					stats->stadistinct = aopt->n_distinct;
				}

				MemoryContextResetAndDeleteChildren(col_context);
			}
		}

		/* And clean up */
		MemoryContextSwitchTo(ind_context);
		ExecDropSingleTupleTableSlot(slot);
		FreeExecutorState(estate);
		MemoryContextResetAndDeleteChildren(ind_context);
	}

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(ind_context);
}

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to analyze an expression index,
 * and index_expr is the expression tree representing the column's data.
 */
static VacAttrStats *
examine_attribute(Relation onerel, int attnum, Node *index_expr, MemoryContext anl_context)
{
	Form_pg_attribute attr;
	HeapTuple	typtuple;
	VacAttrStats *stats;
	int			i;
	bool		ok;

	attr = TupleDescAttr(onerel->rd_att, attnum - 1);

	/* Never analyze dropped columns */
	if (attr->attisdropped)
	{
		return NULL;
	}

	/* Don't analyze column if user has specified not to */
	if (attr->attstattarget == 0)
	{
		return NULL;
	}

	if (ORA_MODE &&
	    (attr->atttypid == BLOBOID || attr->atttypid == CLOBOID || attr->atttypid == NCLOBOID))
	{
		return NULL;
	}

	/*
	 * Create the VacAttrStats struct.  Note that we only have a copy of the
	 * fixed fields of the pg_attribute tuple.
	 */
	stats = (VacAttrStats *) palloc0(sizeof(VacAttrStats));
	stats->attr = (Form_pg_attribute) palloc(ATTRIBUTE_FIXED_PART_SIZE);
	memcpy(stats->attr, attr, ATTRIBUTE_FIXED_PART_SIZE);

	/*
	 * When analyzing an expression index, believe the expression tree's type
	 * not the column datatype --- the latter might be the opckeytype storage
	 * type of the opclass, which is not interesting for our purposes.  (Note:
	 * if we did anything with non-expression index columns, we'd need to
	 * figure out where to get the correct type info from, but for now that's
	 * not a problem.)	It's not clear whether anyone will care about the
	 * typmod, but we store that too just in case.
	 */
	if (index_expr)
	{
		stats->attrtypid = exprType(index_expr);
		stats->attrtypmod = exprTypmod(index_expr);
	}
	else
	{
		stats->attrtypid = attr->atttypid;
		stats->attrtypmod = attr->atttypmod;
	}

	typtuple = SearchSysCacheCopy1(TYPEOID,
								   ObjectIdGetDatum(stats->attrtypid));

	if (!HeapTupleIsValid(typtuple))
	{
		elog(ERROR, "cache lookup failed for type %u", stats->attrtypid);
	}

	stats->attrtype = (Form_pg_type) GETSTRUCT(typtuple);
	stats->anl_context = anl_context;
	stats->tupattnum = attnum;

	/*
	 * The fields describing the stats->stavalues[n] element types default to
	 * the type of the data being analyzed, but the type-specific typanalyze
	 * function can change them if it wants to store something else.
	 */
	for (i = 0; i < STATISTIC_NUM_SLOTS; i++)
	{
		stats->statypid[i] = stats->attrtypid;
		stats->statyplen[i] = stats->attrtype->typlen;
		stats->statypbyval[i] = stats->attrtype->typbyval;
		stats->statypalign[i] = stats->attrtype->typalign;
	}

	/*
	 * Call the type-specific typanalyze function.  If none is specified, use
	 * std_typanalyze().
	 */
	if (OidIsValid(stats->attrtype->typanalyze))
		ok = DatumGetBool(OidFunctionCall1(stats->attrtype->typanalyze,
										   PointerGetDatum(stats)));
	else
	{
		ok = std_typanalyze(stats);
	}

	if (!ok || stats->compute_stats == NULL || stats->minrows <= 0)
	{
		heap_freetuple(typtuple);
		pfree(stats->attr);
		pfree(stats);
		return NULL;
	}

	return stats;
}

/*
 * acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[], which
 * must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also estimate the total numbers of live and dead rows in the table,
 * and return them into *totalrows and *totaldeadrows, respectively.
 *
 * The returned list of tuples is in order by physical position in the table.
 * (We will rely on this later to derive correlation estimates.)
 *
 * As of May 2004 we use a new two-stage method:  Stage one selects up
 * to targrows random blocks (or all blocks, if there aren't so many).
 * Stage two scans these blocks and uses the Vitter algorithm to create
 * a random sample of targrows rows (or less, if there are less in the
 * sample of blocks).  The two stages are executed simultaneously: each
 * block is processed as soon as stage one returns its number and while
 * the rows are read stage two controls which ones are to be inserted
 * into the sample.
 *
 * Although every row has an equal chance of ending up in the final
 * sample, this sampling method is not perfect: not every possible
 * sample has an equal chance of being selected.  For large relations
 * the number of different blocks represented by the sample tends to be
 * too small.  We can live with that for now.  Improvements are welcome.
 *
 * An important property of this sampling method is that because we do
 * look at a statistically unbiased set of blocks, we should get
 * unbiased estimates of the average numbers of live and dead rows per
 * block.  The previous sampling method put too much credence in the row
 * density near the start of the table.
 */
static int
acquire_sample_rows(Relation onerel, int elevel,
					HeapTuple *rows, int targrows,
					double *totalrows, double *totaldeadrows)
{
	int			numrows = 0;	/* # rows now in reservoir */
	double		samplerows = 0; /* total # rows collected */
	double		liverows = 0;	/* # live rows seen */
	double		deadrows = 0;	/* # dead rows seen */
	double		rowstoskip = -1;	/* -1 means not set yet */
	BlockNumber totalblocks;
	TransactionId OldestXmin;
	BlockSamplerData bs;
	ReservoirStateData rstate;
	Assert(targrows > 0);
	totalblocks = RelationGetNumberOfBlocks(onerel);
	/* Need a cutoff xmin for HeapTupleSatisfiesVacuum */
	OldestXmin = GetOldestXmin(onerel, PROCARRAY_FLAGS_VACUUM);
	/* Prepare for sampling block numbers */
	BlockSampler_Init(&bs, totalblocks, targrows, random());
	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	/* Outer loop over blocks to sample */
	while (BlockSampler_HasMore(&bs))
	{
		BlockNumber targblock = BlockSampler_Next(&bs);
		Buffer		targbuffer;
		Page		targpage;
		OffsetNumber targoffset,
					 maxoffset;
		vacuum_delay_point();
		/*
		 * We must maintain a pin on the target page's buffer to ensure that
		 * the maxoffset value stays good (else concurrent VACUUM might delete
		 * tuples out from under us).  Hence, pin the page until we are done
		 * looking at it.  We also choose to hold sharelock on the buffer
		 * throughout --- we could release and re-acquire sharelock for each
		 * tuple, but since we aren't doing much work per tuple, the extra
		 * lock traffic is probably better avoided.
		 */
		targbuffer = ReadBufferExtended(onerel, MAIN_FORKNUM, targblock,
										RBM_NORMAL, vac_strategy);
		LockBuffer(targbuffer, BUFFER_LOCK_SHARE);
		targpage = BufferGetPage(targbuffer);
		maxoffset = PageGetMaxOffsetNumber(targpage);

		/* Inner loop over all tuples on the selected page */
		for (targoffset = FirstOffsetNumber; targoffset <= maxoffset; targoffset++)
		{
			ItemId		itemid;
			HeapTupleData targtuple;
			bool		sample_it = false;
			HeapTuple   newTuple = &targtuple;
			itemid = PageGetItemId(targpage, targoffset);

			/*
			 * We ignore unused and redirect line pointers.  DEAD line
			 * pointers should be counted as dead, because we need vacuum to
			 * run to get rid of them.  Note that this rule agrees with the
			 * way that heap_page_prune() counts things.
			 */
			if (!ItemIdIsNormal(itemid))
			{
				if (ItemIdIsDead(itemid))
				{
					deadrows += 1;
				}

				continue;
			}

			ItemPointerSet(&targtuple.t_self, targblock, targoffset);
			targtuple.t_tableOid = RelationGetRelid(onerel);
			targtuple.t_data = (HeapTupleHeader) PageGetItem(targpage, itemid);
			targtuple.t_len = ItemIdGetLength(itemid);

			switch (HeapTupleSatisfiesVacuum(&targtuple,
											 OldestXmin,
											 targbuffer))
			{
				case HEAPTUPLE_LIVE:
					sample_it = true;
					liverows += 1;
					break;

				case HEAPTUPLE_DEAD:
				case HEAPTUPLE_RECENTLY_DEAD:
					/* Count dead and recently-dead rows */
					deadrows += 1;
					break;

				case HEAPTUPLE_INSERT_IN_PROGRESS:

					/*
					 * Insert-in-progress rows are not counted.  We assume
					 * that when the inserting transaction commits or aborts,
					 * it will send a stats message to increment the proper
					 * count.  This works right only if that transaction ends
					 * after we finish analyzing the table; if things happen
					 * in the other order, its stats update will be
					 * overwritten by ours.  However, the error will be large
					 * only if the other transaction runs long enough to
					 * insert many tuples, so assuming it will finish after us
					 * is the safer option.
					 *
					 * A special case is that the inserting transaction might
					 * be our own.  In this case we should count and sample
					 * the row, to accommodate users who load a table and
					 * analyze it in one transaction.  (pgstat_report_analyze
					 * has to adjust the numbers we send to the stats
					 * collector to make this come out right.)
					 */
					if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(targtuple.t_data)))
					{
						sample_it = true;
						liverows += 1;
					}

					break;

				case HEAPTUPLE_DELETE_IN_PROGRESS:

					/*
					 * We count delete-in-progress rows as still live, using
					 * the same reasoning given above; but we don't bother to
					 * include them in the sample.
					 *
					 * If the delete was done by our own transaction, however,
					 * we must count the row as dead to make
					 * pgstat_report_analyze's stats adjustments come out
					 * right.  (Note: this works out properly when the row was
					 * both inserted and deleted in our xact.)
					 */
					if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(targtuple.t_data)))
					{
						deadrows += 1;
					}
					else
					{
						liverows += 1;
					}

					break;

				default:
					elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
					break;
			}

			if (sample_it && !enable_simple_analyze)
			{
			
				/*
				 * If connection is from Coordinator on datanodes, we discard TOAST fields in sample,
				 * which will lighten the load of memory usage on coordinator.
				 */
				if (!HeapTupleAllFixed(&targtuple) &&
					(targtuple.t_len > MAX_SAMPLE_LENGTH || 
					HeapTupleHasExternal(&targtuple)))
				{
					Datum		*values;
					bool		*nulls;
					TupleDesc	tupdesc = NULL;
					int 	nattrs;
					int 	i;

					tupdesc = RelationGetDescr(onerel);
					nattrs = tupdesc->natts;

					values = (Datum *) palloc0(nattrs * sizeof(Datum));
					nulls = (bool *) palloc0(nattrs * sizeof(bool));

					heap_deform_tuple(&targtuple, tupdesc, values, nulls);

					for(i = 0; i < nattrs; i++)
					{
						if(!TupleDescAttr(tupdesc, i)->attbyval && 
							TupleDescAttr(tupdesc, i)->attlen < 0 &&
							nulls[i] == false)
						{
							/* varlena */
							if (VARSIZE_ANY(DatumGetPointer(values[i])) > MAX_SAMPLE_LENGTH || 
								toast_raw_datum_size(values[i]) > MAX_SAMPLE_LENGTH)
							{
								nulls[i] = true;
							}
						}
					}

					newTuple = heap_form_tuple(tupdesc, values, nulls);

					pfree(values);
					pfree(nulls);

					/*
					 * copy the identification info of the old tuple: t_ctid, t_self, and OID
					 * (if any)
					 */
					newTuple->t_data->t_ctid = targtuple.t_data->t_ctid;
					newTuple->t_self = targtuple.t_self;
					newTuple->t_tableOid = targtuple.t_tableOid;
#ifdef PGXC
					newTuple->t_xc_node_id = targtuple.t_xc_node_id;
#endif
					if (tupdesc->tdhasoid)
						HeapTupleSetOid(newTuple, HeapTupleGetOid(&targtuple));


				}
				/*
				 * The first targrows sample rows are simply copied into the
				 * reservoir. Then we start replacing tuples in the sample
				 * until we reach the end of the relation.  This algorithm is
				 * from Jeff Vitter's paper (see full citation below). It
				 * works by repeatedly computing the number of tuples to skip
				 * before selecting a tuple, which replaces a randomly chosen
				 * element of the reservoir (current set of tuples).  At all
				 * times the reservoir is a true random sample of the tuples
				 * we've passed over so far, so when we fall off the end of
				 * the relation we're done.
				 */
				if (numrows < targrows)
				{
					rows[numrows++] = heap_copytuple(newTuple);
				}
				else
				{
					/*
					 * t in Vitter's paper is the number of records already
					 * processed.  If we need to compute a new S value, we
					 * must use the not-yet-incremented value of samplerows as
					 * t.
					 */
					if (rowstoskip < 0)
					{
						rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);
					}

					if (rowstoskip <= 0)
					{
						/*
						 * Found a suitable tuple, so save it, replacing one
						 * old tuple at random
						 */
						int			k = (int)(targrows * sampler_random_fract(rstate.randstate));
						Assert(k >= 0 && k < targrows);
						heap_freetuple(rows[k]);
						rows[k] = heap_copytuple(newTuple);
					}

					rowstoskip -= 1;
				}
				if (newTuple != &targtuple)
				{
					heap_freetuple_ext(newTuple);
				}
				samplerows += 1;
			}
		}

		/* Now release the lock and pin on the page */
		UnlockReleaseBuffer(targbuffer);
	}

	/*
	 * If we didn't find as many tuples as we wanted then we're done. No sort
	 * is needed, since they're already in order.
	 *
	 * Otherwise we need to sort the collected tuples by position
	 * (itempointer). It's not worth worrying about corner cases where the
	 * tuples are already sorted.
	 */
	if (numrows == targrows)
	{
		qsort((void *) rows, numrows, sizeof(HeapTuple), compare_rows);
	}

	/*
	 * Estimate total numbers of live and dead rows in relation, extrapolating
	 * on the assumption that the average tuple density in pages we didn't
	 * scan is the same as in the pages we did scan.  Since what we scanned is
	 * a random sample of the pages in the relation, this should be a good
	 * assumption.
	 */
	if (bs.m > 0)
	{
		*totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
		*totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
	}
	else
	{
		*totalrows = 0.0;
		*totaldeadrows = 0.0;
	}

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": scanned %d of %u pages, "
					"containing %.0f live rows and %.0f dead rows; "
					"%d rows in sample, %.0f estimated total rows",
					RelationGetRelationName(onerel),
					bs.m, totalblocks,
					liverows, deadrows,
					numrows, *totalrows)));
	return numrows;
}


typedef struct sample_tuple_cell_struct {
    Datum* values;
    bool* nulls;
} sample_tuple_cell;

/*
 * qsort comparator for sorting rows[] array
 */
static int
compare_rows(const void *a, const void *b)
{
	HeapTuple	ha = *(const HeapTuple *) a;
	HeapTuple	hb = *(const HeapTuple *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(&ha->t_self);
	OffsetNumber oa = ItemPointerGetOffsetNumber(&ha->t_self);
	BlockNumber bb = ItemPointerGetBlockNumber(&hb->t_self);
	OffsetNumber ob = ItemPointerGetOffsetNumber(&hb->t_self);

	if (ba < bb)
	{
		return -1;
	}

	if (ba > bb)
	{
		return 1;
	}

	if (oa < ob)
	{
		return -1;
	}

	if (oa > ob)
	{
		return 1;
	}

	return 0;
}


/*
 * acquire_inherited_sample_rows -- acquire sample rows from inheritance tree
 *
 * This has the same API as acquire_sample_rows, except that rows are
 * collected from all inheritance children as well as the specified table.
 * We fail and return zero if there are no inheritance children, or if all
 * children are foreign tables that don't support ANALYZE.
 */
static int
acquire_inherited_sample_rows(Relation onerel, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows, double *totaldeadrows,
							  bool update_catalog)
{
	List	   *tableOIDs;
	Relation   *rels;
	AcquireSampleRowsFunc *acquirefuncs;
	double	   *relblocks;
	double		totalblocks;
	int			numrows,
				nrels,
				i;
	ListCell   *lc;
	bool		has_child;

	/*
	 * Find all members of inheritance set.  We only need AccessShareLock on
	 * the children.
	 */
	tableOIDs =
		find_all_inheritors(RelationGetRelid(onerel), AccessShareLock, NULL);

	/*
	 * Check that there's at least one descendant, else fail.  This could
	 * happen despite analyze_rel's relhassubclass check, if table once had a
	 * child but no longer does.  In that case, we can clear the
	 * relhassubclass field so as not to make the same mistake again later.
	 * (This is safe for normal ANALYZE because we hold ShareUpdateExclusiveLock,
	 * but it's not safe for ANALYZE (LOCAL) because we hold RowExclusiveLock.
	 * See commend about relhassubclass in DefineRelation.)
	 */
	if (list_length(tableOIDs) < 2)
	{
		if (update_catalog)
		{
			/* CCI because we already updated the pg_class row in this command */
			CommandCounterIncrement();
			SetRelationHasSubclass(RelationGetRelid(onerel), false);
		}
		ereport(elevel,
				(errmsg("skipping analyze of \"%s.%s\" inheritance tree --- this inheritance tree contains no child tables",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));
		return 0;
	}

	/*
	 * Identify acquirefuncs to use, and count blocks in all the relations.
	 * The result could overflow BlockNumber, so we use double arithmetic.
	 */
	rels = (Relation *) palloc(list_length(tableOIDs) * sizeof(Relation));
	acquirefuncs = (AcquireSampleRowsFunc *)
				   palloc(list_length(tableOIDs) * sizeof(AcquireSampleRowsFunc));
	relblocks = (double *) palloc(list_length(tableOIDs) * sizeof(double));
	totalblocks = 0;
	nrels = 0;
	has_child = false;

	foreach (lc, tableOIDs)
	{
		Oid			childOID = lfirst_oid(lc);
		Relation	childrel;
		AcquireSampleRowsFunc acquirefunc = NULL;
		BlockNumber relpages = 0;

		/* We already got the needed lock */
		childrel = heap_open(childOID, NoLock);

		/* Ignore if temp table of another backend */
		if (RELATION_IS_OTHER_TEMP(childrel))
		{
			/* ... but release the lock on it */
			Assert(childrel != onerel);
			heap_close(childrel, AccessShareLock);
			continue;
		}
		if (childrel->rd_rel->relkind == RELKIND_RELATION ||
			childrel->rd_rel->relkind == RELKIND_MATVIEW)
		{
			/* Regular table, so use the regular row acquisition function */
			acquirefunc = acquire_sample_rows;
			relpages = RelationGetNumberOfBlocks(childrel);
		}
		else if (childrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		{
			/*
			 * For a foreign table, call the FDW's hook function to see
			 * whether it supports analysis.
			 */
			FdwRoutine *fdwroutine;
			bool		ok = false;
			fdwroutine = GetFdwRoutineForRelation(childrel, false);

			if (fdwroutine->AnalyzeForeignTable != NULL)
				ok = fdwroutine->AnalyzeForeignTable(childrel,
													 &acquirefunc,
													 &relpages);

			if (!ok)
			{
				/* ignore, but release the lock on it */
				Assert(childrel != onerel);
				heap_close(childrel, AccessShareLock);
				continue;
			}
		}
		else
		{
			/*
			 * ignore, but release the lock on it.  don't try to unlock the
			 * passed-in relation
			 */
			Assert(childrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

			if (childrel != onerel)
			{
				heap_close(childrel, AccessShareLock);
			}
			else
			{
				heap_close(childrel, NoLock);
			}

			continue;
		}

		/* OK, we'll process this child */
		has_child = true;
		rels[nrels] = childrel;
		acquirefuncs[nrels] = acquirefunc;
		relblocks[nrels] = (double) relpages;
		totalblocks += (double) relpages;
		nrels++;
	}

	/*
	 * If we don't have at least one child table to consider, fail.  If the
	 * relation is a partitioned table, it's not counted as a child table.
	 */
	if (!has_child)
	{
		ereport(elevel,
				(errmsg("skipping analyze of \"%s.%s\" inheritance tree --- this inheritance tree contains no analyzable child tables",
						get_namespace_name(RelationGetNamespace(onerel)),
						RelationGetRelationName(onerel))));
		return 0;
	}

	/*
	 * Now sample rows from each relation, proportionally to its fraction of
	 * the total block count.  (This might be less than desirable if the child
	 * rels have radically different free-space percentages, but it's not
	 * clear that it's worth working harder.)
	 */
	numrows = 0;
	*totalrows = 0;
	*totaldeadrows = 0;

	for (i = 0; i < nrels; i++)
	{
		Relation	childrel = rels[i];
		AcquireSampleRowsFunc acquirefunc = acquirefuncs[i];
		double		childblocks = relblocks[i];

		if (childblocks > 0)
		{
			int			childtargrows;
			childtargrows = (int) rint(targrows * childblocks / totalblocks);
			/* Make sure we don't overrun due to roundoff error */
			childtargrows = Min(childtargrows, targrows - numrows);

			if (childtargrows > 0)
			{
				int			childrows;
				double		trows,
							tdrows;
				/* Fetch a random sample of the child's rows */
				childrows = (*acquirefunc)(childrel, elevel,
										   rows + numrows, childtargrows,
										   &trows, &tdrows);

				/* We may need to convert from child's rowtype to parent's */
				if (childrows > 0 &&
					!equalTupleDescs(RelationGetDescr(childrel),
									 RelationGetDescr(onerel)))
				{
					TupleConversionMap *map;
					map = convert_tuples_by_name(RelationGetDescr(childrel),
												 RelationGetDescr(onerel),
												 gettext_noop("could not convert row type"));

					if (map != NULL)
					{
						int			j;

						for (j = 0; j < childrows; j++)
						{
							HeapTuple	newtup;

							newtup = execute_attr_map_tuple(rows[numrows + j], map, onerel);
							heap_freetuple(rows[numrows + j]);
							rows[numrows + j] = newtup;
						}

						free_conversion_map(map);
					}
				}

				/* And add to counts */
				numrows += childrows;
				*totalrows += trows;
				*totaldeadrows += tdrows;
			}
		}

		/*
		 * Note: we cannot release the child-table locks, since we may have
		 * pointers to their TOAST tables in the sampled rows.
		 */
		heap_close(childrel, NoLock);
	}

	return numrows;
}

static void
update_pgstatistic(Oid relid, int natts, bool inh, AnalyzeRelStats *analyzeStats, int stats_starts)
{
	Relation	sd;
	int			attno;

	if (natts <= 0)
	{
		return;    /* nothing to do */
	}

	sd = heap_open(StatisticRelationId, RowExclusiveLock);

	for (attno = stats_starts; attno < stats_starts + natts; attno++)
	{
		AnalyzePgStats *stats = &analyzeStats->pgstat[attno];
		HeapTuple	stup,
					oldtup;
		int			i,
					k,
					n;
		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];
		bool		replaces[Natts_pg_statistic];

		/* Ignore attr if we weren't able to collect stats */
		if (stats->attnum == 0)
			continue;

		/*
		 * Construct a new pg_statistic tuple
		 */
		for (i = 0; i < Natts_pg_statistic; ++i)
		{
			nulls[i] = false;
			replaces[i] = true;
		}

		values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
		values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(stats->attnum);
		values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
		i = Anum_pg_statistic_stakind1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
		}

		i = Anum_pg_statistic_staop1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->staop[k]);	/* staopN */
		}

		i = Anum_pg_statistic_stanumbers1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int			nnum = stats->numnumbers[k];

			if (nnum > 0)
			{
				Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
				ArrayType  *arry;

				for (n = 0; n < nnum; n++)
				{
					numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
				}

				/* XXX knows more than it should about type float4: */
				arry = construct_array(numdatums, nnum,
									   FLOAT4OID,
									   sizeof(float4), FLOAT4PASSBYVAL, 'i');
				values[i++] = PointerGetDatum(arry);	/* stanumbersN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		i = Anum_pg_statistic_stavalues1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			if (stats->numvalues[k] > 0)
			{
				ArrayType  *arry;
				arry = construct_array(stats->stavalues[k],
									   stats->numvalues[k],
									   stats->statypid[k],
									   stats->statyplen[k],
									   stats->statypbyval[k],
									   stats->statypalign[k]);
				values[i++] = PointerGetDatum(arry);	/* stavaluesN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		/* Is there already a pg_statistic tuple for this attribute? */
		oldtup = SearchSysCache3(STATRELATTINH,
								 ObjectIdGetDatum(relid),
								 Int16GetDatum(stats->attnum),
								 BoolGetDatum(inh));

		if (HeapTupleIsValid(oldtup))
		{
			/* Yes, replace it */
			stup = heap_modify_tuple(oldtup,
									 RelationGetDescr(sd),
									 values,
									 nulls,
									 replaces);
			ReleaseSysCache(oldtup);
			CatalogTupleUpdate(sd, &stup->t_self, stup);
		}
		else
		{
			/* No, insert new tuple */
			stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);
			CatalogTupleInsert(sd, stup);
		}

		heap_freetuple(stup);
	}

	heap_close(sd, RowExclusiveLock);
}

static void
update_pgstatistic_ext(AnalyzeRelStats *analyzeStats)
{
	int 		i;
	bytea		*depend = NULL;
	bytea		*distinct = NULL;
	int			len;
	NameData 	statname;
	NameData	namespacename;

	memset(namespacename.data, 0, NAMEDATALEN);
	memcpy(namespacename.data, analyzeStats->namespacename, strlen(analyzeStats->namespacename));

	for (i = 0; i < analyzeStats->ext_info_num; i++)
	{
		len = analyzeStats->pgstatext[i].distinct_len;
		if (len > 0)
		{
			distinct = (bytea*)palloc0(len + VARHDRSZ);
			SET_VARSIZE(distinct, (len + VARHDRSZ));
			memcpy(VARDATA(distinct), analyzeStats->pgstatext[i].distinct, len);
		}

		len = analyzeStats->pgstatext[i].depend_len;
		
		if (len > 0)
		{
			depend = (bytea*)palloc0(len + VARHDRSZ);
			SET_VARSIZE(depend, (len + VARHDRSZ));
			memcpy(VARDATA(depend), analyzeStats->pgstatext[i].depend, len);
		}

		memset(statname.data, 0, NAMEDATALEN);
		memcpy(statname.data, analyzeStats->pgstatext[i].statname, strlen(analyzeStats->pgstatext[i].statname));
		update_ext_stats(&namespacename, &statname, distinct, depend);

		if (depend != NULL)
		{
			pfree(depend);
			depend = NULL;
		}
			
		if (distinct != NULL)
		{
			pfree(distinct);
			distinct = NULL;
		}
	}
}


/*
 *	update_attstats() -- update attribute statistics for one relation
 *
 *		Statistics are stored in several places: the pg_class row for the
 *		relation has stats about the whole relation, and there is a
 *		pg_statistic row for each (non-system) attribute that has ever
 *		been analyzed.  The pg_class values are updated by VACUUM, not here.
 *
 *		pg_statistic rows are just added or updated normally.  This means
 *		that pg_statistic will probably contain some deleted rows at the
 *		completion of a vacuum cycle, unless it happens to get vacuumed last.
 *
 *		To keep things simple, we punt for pg_statistic, and don't try
 *		to compute or store rows for pg_statistic itself in pg_statistic.
 *		This could possibly be made to work, but it's not worth the trouble.
 *		Note analyze_rel() has seen to it that we won't come here when
 *		vacuuming pg_statistic itself.
 *
 *		Note: there would be a race condition here if two backends could
 *		ANALYZE the same table concurrently.  Presently, we lock that out
 *		by taking a self-exclusive lock on the relation in analyze_rel().
 */
static void
update_attstats(Oid relid, bool inh, int natts, VacAttrStats **vacattrstats, char relpersistence, bool local_update)
{
	Relation	sd;
	int			attno;

	if (natts <= 0)
	{
		return;    /* nothing to do */
	}

	sd = heap_open(StatisticRelationId, RowExclusiveLock);

	for (attno = 0; attno < natts; attno++)
	{
		VacAttrStats *stats = vacattrstats[attno];
		HeapTuple	stup,
					oldtup;
		int			i,
					k,
					n;
		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];
		bool		replaces[Natts_pg_statistic];

		/* Ignore attr if we weren't able to collect stats */
		if (!stats->stats_valid)
		{
			continue;
		}

		/*
		 * Construct a new pg_statistic tuple
		 */
		for (i = 0; i < Natts_pg_statistic; ++i)
		{
			nulls[i] = false;
			replaces[i] = true;
		}

		values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
		values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(stats->attr->attnum);
		values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
		i = Anum_pg_statistic_stakind1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
		}

		i = Anum_pg_statistic_staop1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->staop[k]);	/* staopN */
		}

		i = Anum_pg_statistic_stanumbers1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int			nnum = stats->numnumbers[k];

			if (nnum > 0)
			{
				Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
				ArrayType  *arry;

				for (n = 0; n < nnum; n++)
				{
					numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
				}

				/* XXX knows more than it should about type float4: */
				arry = construct_array(numdatums, nnum,
									   FLOAT4OID,
									   sizeof(float4), FLOAT4PASSBYVAL, 'i');
				values[i++] = PointerGetDatum(arry);	/* stanumbersN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		i = Anum_pg_statistic_stavalues1 - 1;

		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			if (stats->numvalues[k] > 0)
			{
				ArrayType  *arry;
				arry = construct_array(stats->stavalues[k],
									   stats->numvalues[k],
									   stats->statypid[k],
									   stats->statyplen[k],
									   stats->statypbyval[k],
									   stats->statypalign[k]);
				values[i++] = PointerGetDatum(arry);	/* stavaluesN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		if (local_update)
		{
			LocalStatisticUpdateAttrStats(relid,
										  stats->attr->attnum,
										  natts,
										  RelationGetDescr(sd),
										  inh,
										  values,
										  nulls);
		}
		else
		{
			/* Is there already a pg_statistic tuple for this attribute? */
			oldtup = SearchSysCache3(STATRELATTINH,
									 ObjectIdGetDatum(relid),
									 Int16GetDatum(stats->attr->attnum),
									 BoolGetDatum(inh));

			if (HeapTupleIsValid(oldtup))
			{
				/* Yes, replace it */
				stup = heap_modify_tuple(oldtup,
										 RelationGetDescr(sd),
										 values,
										 nulls,
										 replaces);
				ReleaseSysCache(oldtup);
				CatalogTupleUpdate(sd, &stup->t_self, stup);
			}
			else
			{
				/* No, insert new tuple */
				stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);
				CatalogTupleInsert(sd, stup);
			}

			heap_freetuple(stup);
		}
	}

	heap_close(sd, RowExclusiveLock);
}
/*
 *	update_ext_stats() -- update extended statistics
 */
static void
update_ext_stats(Name nspname, Name name,
				 bytea *ndistinct, bytea *dependencies)
{
	Oid			nspoid;
	Relation	sd;
	HeapTuple	stup,
				oldtup;
	int			i;
	Datum		values[Natts_pg_statistic_ext];
	bool		nulls[Natts_pg_statistic_ext];
	bool		replaces[Natts_pg_statistic_ext];
	nspoid = get_namespace_oid(NameStr(*nspname), false);
	sd = heap_open(StatisticExtRelationId, RowExclusiveLock);

	/*
	 * Construct a new pg_statistic_ext tuple
	 */
	for (i = 0; i < Natts_pg_statistic_ext; ++i)
	{
		nulls[i] = false;
		replaces[i] = false;
	}

	replaces[Anum_pg_statistic_ext_stxndistinct - 1] = true;
	replaces[Anum_pg_statistic_ext_stxdependencies - 1] = true;

	/* ndistinct */
	if (ndistinct)
	{
		values[Anum_pg_statistic_ext_stxndistinct - 1] = PointerGetDatum(ndistinct);
	}
	else
	{
		nulls[Anum_pg_statistic_ext_stxndistinct - 1] = true;
	}

	/* dependencies */
	if (dependencies)
	{
		values[Anum_pg_statistic_ext_stxdependencies - 1] = PointerGetDatum(dependencies);
	}
	else
	{
		nulls[Anum_pg_statistic_ext_stxdependencies - 1] = true;
	}

	/* Is there already a pg_statistic_ext tuple for this attribute? */
	oldtup = SearchSysCache2(STATEXTNAMENSP,
							 NameGetDatum(name),
							 ObjectIdGetDatum(nspoid));

	/*
	 * We only expect data for extended statistics already defined on
	 * the coordinator, so fail if we got something unexpected.
	 */
	if (!HeapTupleIsValid(oldtup))
	{
		elog(ERROR, "unknown extended statistic");
	}

	/* Yes, replace it */
	stup = heap_modify_tuple(oldtup,
							 RelationGetDescr(sd),
							 values,
							 nulls,
							 replaces);
	ReleaseSysCache(oldtup);
	CatalogTupleUpdate(sd, &stup->t_self, stup);
	heap_freetuple(stup);
	heap_close(sd, RowExclusiveLock);
}

/*
 * Standard fetch function for use by compute_stats subroutines.
 *
 * This exists to provide some insulation between compute_stats routines
 * and the actual storage of the sample data.
 */
static Datum
std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			attnum = stats->tupattnum;
	HeapTuple	tuple = stats->rows[rownum];
	TupleDesc	tupDesc = stats->tupDesc;
	return heap_getattr(tuple, attnum, tupDesc, isNull);
}

/*
 * Fetch function for analyzing index expressions.
 *
 * We have not bothered to construct index tuples, instead the data is
 * just in Datum arrays.
 */
static Datum
ind_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
{
	int			i;
	/* exprvals and exprnulls are already offset for proper column */
	i = rownum * stats->rowstride;
	*isNull = stats->exprnulls[i];
	return stats->exprvals[i];
}


/*==========================================================================
 *
 * Code below this point represents the "standard" type-specific statistics
 * analysis algorithms.  This code can be replaced on a per-data-type basis
 * by setting a nonzero value in pg_type.typanalyze.
 *
 *==========================================================================
 */


/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 */
#define WIDTH_THRESHOLD  1024

#define swapInt(a,b)	do {int _tmp; _tmp=a; a=b; b=_tmp;} while(0)
#define swapDatum(a,b)	do {Datum _tmp; _tmp=a; a=b; b=_tmp;} while(0)

/*
 * Extra information used by the default analysis routines
 */
typedef struct
{
	int			count;			/* # of duplicates */
	int			first;			/* values[] index of first occurrence */
} ScalarMCVItem;

typedef struct
{
	SortSupport ssup;
	int		   *tupnoLink;
} CompareScalarsContext;


static void compute_trivial_stats(VacAttrStatsP stats,
								  AnalyzeAttrFetchFunc fetchfunc,
								  int samplerows,
								  double totalrows);
static void compute_distinct_stats(VacAttrStatsP stats,
								   AnalyzeAttrFetchFunc fetchfunc,
								   int samplerows,
								   double totalrows);
static void compute_scalar_stats(VacAttrStatsP stats,
								 AnalyzeAttrFetchFunc fetchfunc,
								 int samplerows,
								 double totalrows);
static int	compare_scalars(const void *a, const void *b, void *arg);
static int	compare_mcvs(const void *a, const void *b);
static int analyze_mcv_list(int *mcv_counts,
				 int num_mcv,
				 double stadistinct,
				 double stanullfrac,
				 int samplerows,
				 double totalrows);


/*
 * std_typanalyze -- the default type-specific typanalyze function
 */
bool
std_typanalyze(VacAttrStats *stats)
{
	Form_pg_attribute attr = stats->attr;
	Oid			ltopr;
	Oid			eqopr;
	StdAnalyzeData *mystats;

	/* If the attstattarget column is negative, use the default value */
	/* NB: it is okay to scribble on stats->attr since it's a copy */
	if (attr->attstattarget < 0)
	{
		attr->attstattarget = default_statistics_target;
	}

	/* Look for default "<" and "=" operators for column's type */
	get_sort_group_operators(stats->attrtypid,
							 false, false, false,
							 &ltopr, &eqopr, NULL,
							 NULL);
	/* Save the operator info for compute_stats routines */
	mystats = (StdAnalyzeData *) palloc(sizeof(StdAnalyzeData));
	mystats->eqopr = eqopr;
	mystats->eqfunc = OidIsValid(eqopr) ? get_opcode(eqopr) : InvalidOid;
	mystats->ltopr = ltopr;
	stats->extra_data = mystats;

	/*
	 * Determine which standard statistics algorithm to use
	 */
	if (OidIsValid(eqopr) && OidIsValid(ltopr))
	{
		/* Seems to be a scalar datatype */
		stats->compute_stats = compute_scalar_stats;
		/*--------------------
		 * The following choice of minrows is based on the paper
		 * "Random sampling for histogram construction: how much is enough?"
		 * by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
		 * Proceedings of ACM SIGMOD International Conference on Management
		 * of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
		 * says that for table size n, histogram size k, maximum relative
		 * error in bin size f, and error probability gamma, the minimum
		 * random sample size is
		 *		r = 4 * k * ln(2*n/gamma) / f^2
		 * Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain
		 *		r = 305.82 * k
		 * Note that because of the log function, the dependence on n is
		 * quite weak; even at n = 10^12, a 300*k sample gives <= 0.66
		 * bin size error with probability 0.99.  So there's no real need to
		 * scale for n, which is a good thing because we don't necessarily
		 * know it at this point.
		 *--------------------
		 * For OpenTenBase-V3, we are targeting for huge amount of data which could
		 * over 10 billion rows store on distributed nodes. So we have to update
		 * the calculation as follow
		 *		r = 4 * k * ln(2*n/gamma) / f^2
		 * Taking f = 0.5, gamma = 0.01, n = 10^10 rows, we obtain
		 *		r = 416.34 * k
		 *--------------------
		 */
		stats->minrows = 420 * attr->attstattarget;
	}
	else if (OidIsValid(eqopr))
	{
		/* We can still recognize distinct values */
		stats->compute_stats = compute_distinct_stats;
		/* Might as well use the same minrows as above */
		stats->minrows = 300 * attr->attstattarget;
	}
	else
	{
		/* Can't do much but the trivial stuff */
		stats->compute_stats = compute_trivial_stats;
		/* Might as well use the same minrows as above */
		stats->minrows = 300 * attr->attstattarget;
	}

	return true;
}


/*
 *	compute_trivial_stats() -- compute very basic column statistics
 *
 *	We use this when we cannot find a hash "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows and the average datum width.
 */
static void
compute_trivial_stats(VacAttrStatsP stats,
					  AnalyzeAttrFetchFunc fetchfunc,
					  int samplerows,
					  double totalrows)
{
	int			i;
	int			null_cnt = 0;
	int			nonnull_cnt = 0;
	double		total_width = 0;
	bool		is_varlena = (!stats->attrtype->typbyval &&
							  stats->attrtype->typlen == -1);
	bool		is_varwidth = (!stats->attrtype->typbyval &&
							   stats->attrtype->typlen < 0);

	for (i = 0; i < samplerows; i++)
	{
		Datum		value;
		bool		isnull;
		vacuum_delay_point();
		value = fetchfunc(stats, i, &isnull);

		/* Check for null/nonnull */
		if (isnull)
		{
			null_cnt++;
			continue;
		}

		nonnull_cnt++;

		/*
		 * If it's a variable-width field, add up widths for average width
		 * calculation.  Note that if the value is toasted, we use the toasted
		 * width.  We don't bother with this calculation if it's a fixed-width
		 * type.
		 */
		if (is_varlena)
		{
			total_width += VARSIZE_ANY(DatumGetPointer(value));
		}
		else if (is_varwidth)
		{
			/* must be cstring */
			total_width += strlen(DatumGetCString(value)) + 1;
		}
	}

	/* We can only compute average width if we found some non-null values. */
	if (nonnull_cnt > 0)
	{
		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;

		if (is_varwidth)
		{
			stats->stawidth = total_width / (double) nonnull_cnt;
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		stats->stadistinct = 0.0;	/* "unknown" */
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;

		if (is_varwidth)
		{
			stats->stawidth = 0;    /* "unknown" */
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		stats->stadistinct = 0.0;	/* "unknown" */
	}
}


/*
 *	compute_distinct_stats() -- compute column statistics including ndistinct
 *
 *	We use this when we can find only an "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, and the (estimated) number of distinct values.
 *
 *	The most common values are determined by brute force: we keep a list
 *	of previously seen values, ordered by number of times seen, as we scan
 *	the samples.  A newly seen value is inserted just after the last
 *	multiply-seen value, causing the bottommost (oldest) singly-seen value
 *	to drop off the list.  The accuracy of this method, and also its cost,
 *	depend mainly on the length of the list we are willing to keep.
 */
static void
compute_distinct_stats(VacAttrStatsP stats,
					   AnalyzeAttrFetchFunc fetchfunc,
					   int samplerows,
					   double totalrows)
{
	int			i;
	int			null_cnt = 0;
	int			nonnull_cnt = 0;
	int			toowide_cnt = 0;
	double		total_width = 0;
	bool		is_varlena = (!stats->attrtype->typbyval &&
							  stats->attrtype->typlen == -1);
	bool		is_varwidth = (!stats->attrtype->typbyval &&
							   stats->attrtype->typlen < 0);
	FmgrInfo	f_cmpeq;
	typedef struct
	{
		Datum		value;
		int			count;
	} TrackItem;
	TrackItem  *track;
	int			track_cnt,
				track_max;
	int			num_mcv = stats->attr->attstattarget;
	StdAnalyzeData *mystats = (StdAnalyzeData *) stats->extra_data;
	/*
	 * We track up to 2*n values for an n-element MCV list; but at least 10
	 */
	track_max = 2 * num_mcv;

	if (track_max < 10)
	{
		track_max = 10;
	}

	track = (TrackItem *) palloc(track_max * sizeof(TrackItem));
	track_cnt = 0;
	fmgr_info(mystats->eqfunc, &f_cmpeq);

	for (i = 0; i < samplerows; i++)
	{
		Datum		value;
		bool		isnull;
		bool		match;
		int			firstcount1,
					j;
		vacuum_delay_point();
		value = fetchfunc(stats, i, &isnull);

		/* Check for null/nonnull */
		if (isnull)
		{
			null_cnt++;
			continue;
		}

		nonnull_cnt++;

		/*
		 * If it's a variable-width field, add up widths for average width
		 * calculation.  Note that if the value is toasted, we use the toasted
		 * width.  We don't bother with this calculation if it's a fixed-width
		 * type.
		 */
		if (is_varlena)
		{
			total_width += VARSIZE_ANY(DatumGetPointer(value));

			/*
			 * If the value is toasted, we want to detoast it just once to
			 * avoid repeated detoastings and resultant excess memory usage
			 * during the comparisons.  Also, check to see if the value is
			 * excessively wide, and if so don't detoast at all --- just
			 * ignore the value.
			 */
			if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
			{
				toowide_cnt++;
				continue;
			}

			value = PointerGetDatum(PG_DETOAST_DATUM(value));
		}
		else if (is_varwidth)
		{
			/* must be cstring */
			total_width += strlen(DatumGetCString(value)) + 1;
		}

		/*
		 * See if the value matches anything we're already tracking.
		 */
		match = false;
		firstcount1 = track_cnt;

		for (j = 0; j < track_cnt; j++)
		{
			/* We always use the default collation for statistics */
			if (DatumGetBool(FunctionCall2Coll(&f_cmpeq,
											   DEFAULT_COLLATION_OID,
											   value, track[j].value)))
			{
				match = true;
				break;
			}

			if (j < firstcount1 && track[j].count == 1)
			{
				firstcount1 = j;
			}
		}

		if (match)
		{
			/* Found a match */
			track[j].count++;

			/* This value may now need to "bubble up" in the track list */
			while (j > 0 && track[j].count > track[j - 1].count)
			{
				swapDatum(track[j].value, track[j - 1].value);
				swapInt(track[j].count, track[j - 1].count);
				j--;
			}
		}
		else
		{
			/* No match.  Insert at head of count-1 list */
			if (track_cnt < track_max)
			{
				track_cnt++;
			}

			for (j = track_cnt - 1; j > firstcount1; j--)
			{
				track[j].value = track[j - 1].value;
				track[j].count = track[j - 1].count;
			}

			if (firstcount1 < track_cnt)
			{
				track[firstcount1].value = value;
				track[firstcount1].count = 1;
			}
		}
	}

	/* We can only compute real stats if we found some non-null values. */
	if (nonnull_cnt > 0)
	{
		int			nmultiple,
					summultiple;
		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;

		if (is_varwidth)
		{
			stats->stawidth = total_width / (double) nonnull_cnt;
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		/* Count the number of values we found multiple times */
		summultiple = 0;

		for (nmultiple = 0; nmultiple < track_cnt; nmultiple++)
		{
			if (track[nmultiple].count == 1)
			{
				break;
			}

			summultiple += track[nmultiple].count;
		}

		if (nmultiple == 0)
		{
			/*
			 * If we found no repeated non-null values, assume it's a unique
			 * column; but be sure to discount for any nulls we found.
			 */
			stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
		}
		else if (track_cnt < track_max && toowide_cnt == 0 &&
				 nmultiple == track_cnt)
		{
			/*
			 * Our track list includes every value in the sample, and every
			 * value appeared more than once.  Assume the column has just
			 * these values.  (This case is meant to address columns with
			 * small, fixed sets of possible values, such as boolean or enum
			 * columns.  If there are any values that appear just once in the
			 * sample, including too-wide values, we should assume that that's
			 * not what we're dealing with.)
			 */
			stats->stadistinct = track_cnt;
		}
		else
		{
			/*----------
			 * Estimate the number of distinct values using the estimator
			 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
			 *		n*d / (n - f1 + f1*n/N)
			 * where f1 is the number of distinct values that occurred
			 * exactly once in our sample of n rows (from a total of N),
			 * and d is the total number of distinct values in the sample.
			 * This is their Duj1 estimator; the other estimators they
			 * recommend are considerably more complex, and are numerically
			 * very unstable when n is much smaller than N.
			 *
			 * In this calculation, we consider only non-nulls.  We used to
			 * include rows with null values in the n and N counts, but that
			 * leads to inaccurate answers in columns with many nulls, and
			 * it's intuitively bogus anyway considering the desired result is
			 * the number of distinct non-null values.
			 *
			 * We assume (not very reliably!) that all the multiply-occurring
			 * values are reflected in the final track[] list, and the other
			 * nonnull values all appeared but once.  (XXX this usually
			 * results in a drastic overestimate of ndistinct.  Can we do
			 * any better?)
			 *----------
			 */
			int			f1 = nonnull_cnt - summultiple;
			int			d = f1 + nmultiple;
			double		n = samplerows - null_cnt;
			double		N = totalrows * (1.0 - stats->stanullfrac);
			double		stadistinct;

			/* N == 0 shouldn't happen, but just in case ... */
			if (N > 0)
			{
				stadistinct = (n * d) / ((n - f1) + f1 * n / N);
			}
			else
			{
				stadistinct = 0;
			}

			/* Clamp to sane range in case of roundoff error */
			if (stadistinct < d)
			{
				stadistinct = d;
			}

			if (stadistinct > N)
			{
				stadistinct = N;
			}

			/* And round to integer */
			stats->stadistinct = floor(stadistinct + 0.5);
		}

		/*
		 * If we estimated the number of distinct values at more than 10% of
		 * the total row count (a very arbitrary limit), then assume that
		 * stadistinct should scale with the row count rather than be a fixed
		 * value.
		 */
		if (stats->stadistinct > 0.1 * totalrows)
		{
			stats->stadistinct = -(stats->stadistinct / totalrows);
		}

		/*
		 * Decide how many values are worth storing as most-common values. If
		 * we are able to generate a complete MCV list (all the values in the
		 * sample will fit, and we think these are all the ones in the table),
		 * then do so.  Otherwise, store only those values that are
		 * significantly more common than the values not in the list.
		 *
		 * Note: the first of these cases is meant to address columns with
		 * small, fixed sets of possible values, such as boolean or enum
		 * columns.  If we can *completely* represent the column population by
		 * an MCV list that will fit into the stats target, then we should do
		 * so and thus provide the planner with complete information.  But if
		 * the MCV list is not complete, it's generally worth being more
		 * selective, and not just filling it all the way up to the stats
		 * target.
		 */
		if (track_cnt < track_max && toowide_cnt == 0 &&
			stats->stadistinct > 0 &&
			track_cnt <= num_mcv)
		{
			/* Track list includes all values seen, and all will fit */
			num_mcv = track_cnt;
		}
		else
		{
			int		   *mcv_counts;

			/* Incomplete list; decide how many values are worth keeping */
			if (num_mcv > track_cnt)
				num_mcv = track_cnt;

			if (num_mcv > 0)
			{
				mcv_counts = (int *) palloc(num_mcv * sizeof(int));
				for (i = 0; i < num_mcv; i++)
					mcv_counts[i] = track[i].count;

				num_mcv = analyze_mcv_list(mcv_counts, num_mcv,
										   stats->stadistinct,
										   stats->stanullfrac,
										   samplerows, totalrows);
			}
		}

		/* Generate MCV slot entry */
		if (num_mcv > 0)
		{
			MemoryContext old_context;
			Datum	   *mcv_values;
			float4	   *mcv_freqs;
			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			mcv_values = (Datum *) palloc(num_mcv * sizeof(Datum));
			mcv_freqs = (float4 *) palloc(num_mcv * sizeof(float4));

			for (i = 0; i < num_mcv; i++)
			{
				mcv_values[i] = datumCopy(track[i].value,
										  stats->attrtype->typbyval,
										  stats->attrtype->typlen);
				mcv_freqs[i] = (double) track[i].count / (double) samplerows;
			}

			MemoryContextSwitchTo(old_context);
			stats->stakind[0] = STATISTIC_KIND_MCV;
			stats->staop[0] = mystats->eqopr;
			stats->stanumbers[0] = mcv_freqs;
			stats->numnumbers[0] = num_mcv;
			stats->stavalues[0] = mcv_values;
			stats->numvalues[0] = num_mcv;
			/*
			 * Accept the defaults for stats->statypid and others. They have
			 * been set before we were called (see vacuum.h)
			 */
		}
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;

		if (is_varwidth)
		{
			stats->stawidth = 0;    /* "unknown" */
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		stats->stadistinct = 0.0;	/* "unknown" */
	}

	/* We don't need to bother cleaning up any of our temporary palloc's */
}


/*
 *	compute_scalar_stats() -- compute column statistics
 *
 *	We use this when we can find "=" and "<" operators for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, the (estimated) number of distinct values, the
 *	distribution histogram, and the correlation of physical to logical order.
 *
 *	The desired stats can be determined fairly easily after sorting the
 *	data values into order.
 */
static void
compute_scalar_stats(VacAttrStatsP stats,
					 AnalyzeAttrFetchFunc fetchfunc,
					 int samplerows,
					 double totalrows)
{
	int			i;
	int			null_cnt = 0;
	int			nonnull_cnt = 0;
	int			toowide_cnt = 0;
	double		total_width = 0;
	bool		is_varlena = (!stats->attrtype->typbyval &&
							  stats->attrtype->typlen == -1);
	bool		is_varwidth = (!stats->attrtype->typbyval &&
							   stats->attrtype->typlen < 0);
	double		corr_xysum;
	SortSupportData ssup;
	ScalarItem *values;
	int			values_cnt = 0;
	int		   *tupnoLink;
	ScalarMCVItem *track;
	int			track_cnt = 0;
	int			num_mcv = stats->attr->attstattarget;
	int			num_bins = stats->attr->attstattarget;

	StdAnalyzeData *mystats = (StdAnalyzeData *) stats->extra_data;
	values = (ScalarItem *) palloc(samplerows * sizeof(ScalarItem));
	tupnoLink = (int *) palloc(samplerows * sizeof(int));
	track = (ScalarMCVItem *) palloc(num_mcv * sizeof(ScalarMCVItem));
	memset(&ssup, 0, sizeof(ssup));
	ssup.ssup_cxt = CurrentMemoryContext;
	/* We always use the default collation for statistics */
	ssup.ssup_collation = DEFAULT_COLLATION_OID;
	ssup.ssup_nulls_first = false;
	/*
	 * For now, don't perform abbreviated key conversion, because full values
	 * are required for MCV slot generation.  Supporting that optimization
	 * would necessitate teaching compare_scalars() to call a tie-breaker.
	 */
	ssup.abbreviate = false;
	PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

	/* Initial scan to find sortable values */
	for (i = 0; i < samplerows; i++)
	{
		Datum		value = 0;
		bool		isnull = true;
		vacuum_delay_point();

		value = fetchfunc(stats, i, &isnull);

		/* Check for null/nonnull */
		if (isnull)
		{
			null_cnt++;
			continue;
		}

		nonnull_cnt++;

		/*
		 * If it's a variable-width field, add up widths for average width
		 * calculation.  Note that if the value is toasted, we use the toasted
		 * width.  We don't bother with this calculation if it's a fixed-width
		 * type.
		 */
		if (is_varlena)
		{
			total_width += VARSIZE_ANY(DatumGetPointer(value));

			/*
			 * If the value is toasted, we want to detoast it just once to
			 * avoid repeated detoastings and resultant excess memory usage
			 * during the comparisons.  Also, check to see if the value is
			 * excessively wide, and if so don't detoast at all --- just
			 * ignore the value.
			 */
			if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
			{
				toowide_cnt++;
				continue;
			}

			value = PointerGetDatum(PG_DETOAST_DATUM(value));
		}
		else if (is_varwidth)
		{
			/* must be cstring */
			total_width += strlen(DatumGetCString(value)) + 1;
		}

		/* Add it to the list to be sorted */
		values[values_cnt].value = value;
		values[values_cnt].tupno = values_cnt;
		tupnoLink[values_cnt] = values_cnt;
		values_cnt++;
	}

	/* We can only compute real stats if we found some sortable values. */
	if (values_cnt > 0)
	{
		int			ndistinct,	/* # distinct values in sample */
					nmultiple,	/* # that appear multiple times */
					num_hist,
					dups_cnt;
		int			slot_idx = 0;
		CompareScalarsContext cxt;
		/* Sort the collected values */
		cxt.ssup = &ssup;
		cxt.tupnoLink = tupnoLink;
		qsort_arg((void *) values, values_cnt, sizeof(ScalarItem),
				  compare_scalars, (void *) &cxt);
		/*
		 * Now scan the values in order, find the most common ones, and also
		 * accumulate ordering-correlation statistics.
		 *
		 * To determine which are most common, we first have to count the
		 * number of duplicates of each value.  The duplicates are adjacent in
		 * the sorted list, so a brute-force approach is to compare successive
		 * datum values until we find two that are not equal. However, that
		 * requires N-1 invocations of the datum comparison routine, which are
		 * completely redundant with work that was done during the sort.  (The
		 * sort algorithm must at some point have compared each pair of items
		 * that are adjacent in the sorted order; otherwise it could not know
		 * that it's ordered the pair correctly.) We exploit this by having
		 * compare_scalars remember the highest tupno index that each
		 * ScalarItem has been found equal to.  At the end of the sort, a
		 * ScalarItem's tupnoLink will still point to itself if and only if it
		 * is the last item of its group of duplicates (since the group will
		 * be ordered by tupno).
		 */
		corr_xysum = 0;
		ndistinct = 0;
		nmultiple = 0;
		dups_cnt = 0;

		for (i = 0; i < values_cnt; i++)
		{
			int			tupno = values[i].tupno;
			corr_xysum += ((double) i) * ((double) tupno);
			dups_cnt++;

			if (tupnoLink[tupno] == tupno)
			{
				/* Reached end of duplicates of this value */
				ndistinct++;

				if (dups_cnt > 1)
				{
					nmultiple++;

					if (track_cnt < num_mcv ||
						dups_cnt > track[track_cnt - 1].count)
					{
						/*
						 * Found a new item for the mcv list; find its
						 * position, bubbling down old items if needed. Loop
						 * invariant is that j points at an empty/ replaceable
						 * slot.
						 */
						int			j;

						if (track_cnt < num_mcv)
						{
							track_cnt++;
						}

						for (j = track_cnt - 1; j > 0; j--)
						{
							if (dups_cnt <= track[j - 1].count)
							{
								break;
							}

							track[j].count = track[j - 1].count;
							track[j].first = track[j - 1].first;
						}

						track[j].count = dups_cnt;
						track[j].first = i + 1 - dups_cnt;
					}
				}

				dups_cnt = 0;
			}
		}

		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;

		if (is_varwidth)
		{
			stats->stawidth = total_width / (double) nonnull_cnt;
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		if (nmultiple == 0)
		{
			/*
			 * If we found no repeated non-null values, assume it's a unique
			 * column; but be sure to discount for any nulls we found.
			 */
			stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
		}
		else if (toowide_cnt == 0 && nmultiple == ndistinct)
		{
			/*
			 * Every value in the sample appeared more than once.  Assume the
			 * column has just these values.  (This case is meant to address
			 * columns with small, fixed sets of possible values, such as
			 * boolean or enum columns.  If there are any values that appear
			 * just once in the sample, including too-wide values, we should
			 * assume that that's not what we're dealing with.)
			 */
			stats->stadistinct = ndistinct;
		}
		else
		{
			/*----------
			 * Estimate the number of distinct values using the estimator
			 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
			 *		n*d / (n - f1 + f1*n/N)
			 * where f1 is the number of distinct values that occurred
			 * exactly once in our sample of n rows (from a total of N),
			 * and d is the total number of distinct values in the sample.
			 * This is their Duj1 estimator; the other estimators they
			 * recommend are considerably more complex, and are numerically
			 * very unstable when n is much smaller than N.
			 *
			 * In this calculation, we consider only non-nulls.  We used to
			 * include rows with null values in the n and N counts, but that
			 * leads to inaccurate answers in columns with many nulls, and
			 * it's intuitively bogus anyway considering the desired result is
			 * the number of distinct non-null values.
			 *
			 * Overwidth values are assumed to have been distinct.
			 *----------
			 */
			int			f1 = ndistinct - nmultiple + toowide_cnt;
			int			d = f1 + nmultiple;
			double		n = samplerows - null_cnt;
			double		N = totalrows * (1.0 - stats->stanullfrac);
			double		stadistinct;

			/* N == 0 shouldn't happen, but just in case ... */
			if (N > 0)
			{
				stadistinct = (n * d) / ((n - f1) + f1 * n / N);
			}
			else
			{
				stadistinct = 0;
			}

			/* Clamp to sane range in case of roundoff error */
			if (stadistinct < d)
			{
				stadistinct = d;
			}

			if (stadistinct > N)
			{
				stadistinct = N;
			}

			/* And round to integer */
			stats->stadistinct = floor(stadistinct + 0.5);
		}

		/*
		 * If we estimated the number of distinct values at more than 10% of
		 * the total row count (a very arbitrary limit), then assume that
		 * stadistinct should scale with the row count rather than be a fixed
		 * value.
		 */
		if (stats->stadistinct > 0.1 * totalrows)
		{
			stats->stadistinct = -(stats->stadistinct / totalrows);
		}

		/*
		 * Decide how many values are worth storing as most-common values. If
		 * we are able to generate a complete MCV list (all the values in the
		 * sample will fit, and we think these are all the ones in the table),
		 * then do so.  Otherwise, store only those values that are
		 * significantly more common than the values not in the list.
		 *
		 * Note: the first of these cases is meant to address columns with
		 * small, fixed sets of possible values, such as boolean or enum
		 * columns.  If we can *completely* represent the column population by
		 * an MCV list that will fit into the stats target, then we should do
		 * so and thus provide the planner with complete information.  But if
		 * the MCV list is not complete, it's generally worth being more
		 * selective, and not just filling it all the way up to the stats
		 * target.
		 */
		if (track_cnt == ndistinct && toowide_cnt == 0 &&
			stats->stadistinct > 0 &&
			track_cnt <= num_mcv)
		{
			/* Track list includes all values seen, and all will fit */
			num_mcv = track_cnt;
		}
		else
		{
			int		   *mcv_counts;

			/* Incomplete list; decide how many values are worth keeping */
			if (num_mcv > track_cnt)
				num_mcv = track_cnt;

			if (num_mcv > 0)
			{
				mcv_counts = (int *) palloc(num_mcv * sizeof(int));
				for (i = 0; i < num_mcv; i++)
					mcv_counts[i] = track[i].count;

				num_mcv = analyze_mcv_list(mcv_counts, num_mcv,
										   stats->stadistinct,
										   stats->stanullfrac,
										   samplerows, totalrows);
			}
		}

		/* Generate MCV slot entry */
		if (num_mcv > 0)
		{
			MemoryContext old_context;
			Datum	   *mcv_values;
			float4	   *mcv_freqs;
			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			mcv_values = (Datum *) palloc(num_mcv * sizeof(Datum));
			mcv_freqs = (float4 *) palloc(num_mcv * sizeof(float4));

			for (i = 0; i < num_mcv; i++)
			{
				mcv_values[i] = datumCopy(values[track[i].first].value,
										  stats->attrtype->typbyval,
										  stats->attrtype->typlen);
				mcv_freqs[i] = (double) track[i].count / (double) samplerows;
			}

			MemoryContextSwitchTo(old_context);
			stats->stakind[slot_idx] = STATISTIC_KIND_MCV;
			stats->staop[slot_idx] = mystats->eqopr;
			stats->stanumbers[slot_idx] = mcv_freqs;
			stats->numnumbers[slot_idx] = num_mcv;
			stats->stavalues[slot_idx] = mcv_values;
			stats->numvalues[slot_idx] = num_mcv;
			/*
			 * Accept the defaults for stats->statypid and others. They have
			 * been set before we were called (see vacuum.h)
			 */
			slot_idx++;
		}

		/*
		 * Generate a histogram slot entry if there are at least two distinct
		 * values not accounted for in the MCV list.  (This ensures the
		 * histogram won't collapse to empty or a singleton.)
		 */
		num_hist = ndistinct - num_mcv;

		if (num_hist > num_bins)
		{
			num_hist = num_bins + 1;
		}

		if (num_hist >= 2)
		{
			MemoryContext old_context;
			Datum	   *hist_values;
			int			nvals;
			int			pos,
						posfrac,
						delta,
						deltafrac;
			/* Sort the MCV items into position order to speed next loop */
			qsort((void *) track, num_mcv,
				  sizeof(ScalarMCVItem), compare_mcvs);

			/*
			 * Collapse out the MCV items from the values[] array.
			 *
			 * Note we destroy the values[] array here... but we don't need it
			 * for anything more.  We do, however, still need values_cnt.
			 * nvals will be the number of remaining entries in values[].
			 */
			if (num_mcv > 0)
			{
				int			src,
							dest;
				int			j;
				src = dest = 0;
				j = 0;			/* index of next interesting MCV item */

				while (src < values_cnt)
				{
					int			ncopy;

					if (j < num_mcv)
					{
						int			first = track[j].first;

						if (src >= first)
						{
							/* advance past this MCV item */
							src = first + track[j].count;
							j++;
							continue;
						}

						ncopy = first - src;
					}
					else
					{
						ncopy = values_cnt - src;
					}

					memmove(&values[dest], &values[src],
							ncopy * sizeof(ScalarItem));
					src += ncopy;
					dest += ncopy;
				}

				nvals = dest;
			}
			else
			{
				nvals = values_cnt;
			}

			Assert(nvals >= num_hist);
			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			hist_values = (Datum *) palloc(num_hist * sizeof(Datum));
			/*
			 * The object of this loop is to copy the first and last values[]
			 * entries along with evenly-spaced values in between.  So the
			 * i'th value is values[(i * (nvals - 1)) / (num_hist - 1)].  But
			 * computing that subscript directly risks integer overflow when
			 * the stats target is more than a couple thousand.  Instead we
			 * add (nvals - 1) / (num_hist - 1) to pos at each step, tracking
			 * the integral and fractional parts of the sum separately.
			 */
			delta = (nvals - 1) / (num_hist - 1);
			deltafrac = (nvals - 1) % (num_hist - 1);
			pos = posfrac = 0;

			for (i = 0; i < num_hist; i++)
			{
				hist_values[i] = datumCopy(values[pos].value,
										   stats->attrtype->typbyval,
										   stats->attrtype->typlen);
				pos += delta;
				posfrac += deltafrac;

				if (posfrac >= (num_hist - 1))
				{
					/* fractional part exceeds 1, carry to integer part */
					pos++;
					posfrac -= (num_hist - 1);
				}
			}

			MemoryContextSwitchTo(old_context);
			stats->stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
			stats->staop[slot_idx] = mystats->ltopr;
			stats->stavalues[slot_idx] = hist_values;
			stats->numvalues[slot_idx] = num_hist;
			/*
			 * Accept the defaults for stats->statypid and others. They have
			 * been set before we were called (see vacuum.h)
			 */
			slot_idx++;
		}

		/* Generate a correlation entry if there are multiple values */
		if (values_cnt > 1)
		{
			MemoryContext old_context;
			float4	   *corrs;
			double		corr_xsum,
						corr_x2sum;
			/* Must copy the target values into anl_context */
			old_context = MemoryContextSwitchTo(stats->anl_context);
			corrs = (float4 *) palloc(sizeof(float4));
			MemoryContextSwitchTo(old_context);
			/*----------
			 * Since we know the x and y value sets are both
			 *		0, 1, ..., values_cnt-1
			 * we have sum(x) = sum(y) =
			 *		(values_cnt-1)*values_cnt / 2
			 * and sum(x^2) = sum(y^2) =
			 *		(values_cnt-1)*values_cnt*(2*values_cnt-1) / 6.
			 *----------
			 */
			corr_xsum = ((double)(values_cnt - 1)) *
						((double) values_cnt) / 2.0;
			corr_x2sum = ((double)(values_cnt - 1)) *
						 ((double) values_cnt) * (double)(2 * values_cnt - 1) / 6.0;
			/* And the correlation coefficient reduces to */
			corrs[0] = (values_cnt * corr_xysum - corr_xsum * corr_xsum) /
					   (values_cnt * corr_x2sum - corr_xsum * corr_xsum);
			stats->stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
			stats->staop[slot_idx] = mystats->ltopr;
			stats->stanumbers[slot_idx] = corrs;
			stats->numnumbers[slot_idx] = 1;
			slot_idx++;
		}
	}
	else if (nonnull_cnt > 0)
	{
		/* We found some non-null values, but they were all too wide */
		Assert(nonnull_cnt == toowide_cnt);
		stats->stats_valid = true;
		/* Do the simple null-frac and width stats */
		stats->stanullfrac = (double) null_cnt / (double) samplerows;

		if (is_varwidth)
		{
			stats->stawidth = total_width / (double) nonnull_cnt;
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		/* Assume all too-wide values are distinct, so it's a unique column */
		stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
	}
	else if (null_cnt > 0)
	{
		/* We found only nulls; assume the column is entirely null */
		stats->stats_valid = true;
		stats->stanullfrac = 1.0;

		if (is_varwidth)
		{
			stats->stawidth = 0;    /* "unknown" */
		}
		else
		{
			stats->stawidth = stats->attrtype->typlen;
		}

		stats->stadistinct = 0.0;	/* "unknown" */
	}

	/* We don't need to bother cleaning up any of our temporary palloc's */
}

/*
 * qsort_arg comparator for sorting ScalarItems
 *
 * Aside from sorting the items, we update the tupnoLink[] array
 * whenever two ScalarItems are found to contain equal datums.  The array
 * is indexed by tupno; for each ScalarItem, it contains the highest
 * tupno that that item's datum has been found to be equal to.  This allows
 * us to avoid additional comparisons in compute_scalar_stats().
 */
static int
compare_scalars(const void *a, const void *b, void *arg)
{
	Datum		da = ((const ScalarItem *) a)->value;
	int			ta = ((const ScalarItem *) a)->tupno;
	Datum		db = ((const ScalarItem *) b)->value;
	int			tb = ((const ScalarItem *) b)->tupno;
	CompareScalarsContext *cxt = (CompareScalarsContext *) arg;
	int			compare;
	compare = ApplySortComparator(da, false, db, false, cxt->ssup);

	if (compare != 0)
	{
		return compare;
	}

	/*
	 * The two datums are equal, so update cxt->tupnoLink[].
	 */
	if (cxt->tupnoLink[ta] < tb)
	{
		cxt->tupnoLink[ta] = tb;
	}

	if (cxt->tupnoLink[tb] < ta)
	{
		cxt->tupnoLink[tb] = ta;
	}

	/*
	 * For equal datums, sort by tupno
	 */
	return ta - tb;
}

/*
 * qsort comparator for sorting ScalarMCVItems by position
 */
static int
compare_mcvs(const void *a, const void *b)
{
	int			da = ((const ScalarMCVItem *) a)->first;
	int			db = ((const ScalarMCVItem *) b)->first;
	return da - db;
}

/*
 * Analyze the list of common values in the sample and decide how many are
 * worth storing in the table's MCV list.
 *
 * mcv_counts is assumed to be a list of the counts of the most common values
 * seen in the sample, starting with the most common.  The return value is the
 * number that are significantly more common than the values not in the list,
 * and which are therefore deemed worth storing in the table's MCV list.
 */
static int
analyze_mcv_list(int *mcv_counts,
				 int num_mcv,
				 double stadistinct,
				 double stanullfrac,
				 int samplerows,
				 double totalrows)
{
	double		ndistinct_table;
	double		sumcount;
	int			i;

	/*
	 * If the entire table was sampled, keep the whole list.  This also
	 * protects us against division by zero in the code below.
	 */
	if (samplerows == totalrows || totalrows <= 1.0)
		return num_mcv;

	/* Re-extract the estimated number of distinct nonnull values in table */
	ndistinct_table = stadistinct;
	if (ndistinct_table < 0)
		ndistinct_table = -ndistinct_table * totalrows;

	/*
	 * Exclude the least common values from the MCV list, if they are not
	 * significantly more common than the estimated selectivity they would
	 * have if they weren't in the list.  All non-MCV values are assumed to be
	 * equally common, after taking into account the frequencies of all the
	 * the values in the MCV list and the number of nulls (c.f. eqsel()).
	 *
	 * Here sumcount tracks the total count of all but the last (least common)
	 * value in the MCV list, allowing us to determine the effect of excluding
	 * that value from the list.
	 *
	 * Note that we deliberately do this by removing values from the full
	 * list, rather than starting with an empty list and adding values,
	 * because the latter approach can fail to add any values if all the most
	 * common values have around the same frequency and make up the majority
	 * of the table, so that the overall average frequency of all values is
	 * roughly the same as that of the common values.  This would lead to any
	 * uncommon values being significantly overestimated.
	 */
	sumcount = 0.0;
	for (i = 0; i < num_mcv - 1; i++)
		sumcount += mcv_counts[i];

	while (num_mcv > 0)
	{
		double		selec,
					otherdistinct,
					N,
					n,
					K,
					variance,
					stddev;

		/*
		 * Estimated selectivity the least common value would have if it
		 * wasn't in the MCV list (c.f. eqsel()).
		 */
		selec = 1.0 - sumcount / samplerows - stanullfrac;
		if (selec < 0.0)
			selec = 0.0;
		if (selec > 1.0)
			selec = 1.0;
		otherdistinct = ndistinct_table - (num_mcv - 1);
		if (otherdistinct > 1)
			selec /= otherdistinct;

		/*
		 * If the value is kept in the MCV list, its population frequency is
		 * assumed to equal its sample frequency.  We use the lower end of a
		 * textbook continuity-corrected Wald-type confidence interval to
		 * determine if that is significantly more common than the non-MCV
		 * frequency --- specifically we assume the population frequency is
		 * highly likely to be within around 2 standard errors of the sample
		 * frequency, which equates to an interval of 2 standard deviations
		 * either side of the sample count, plus an additional 0.5 for the
		 * continuity correction.  Since we are sampling without replacement,
		 * this is a hypergeometric distribution.
		 *
		 * XXX: Empirically, this approach seems to work quite well, but it
		 * may be worth considering more advanced techniques for estimating
		 * the confidence interval of the hypergeometric distribution.
		 */
		N = totalrows;
		n = samplerows;
		K = N * mcv_counts[num_mcv - 1] / n;
		variance = n * K * (N - K) * (N - n) / (N * N * (N - 1));
		stddev = sqrt(variance);

		if (mcv_counts[num_mcv - 1] > selec * samplerows + 2 * stddev + 0.5)
		{
			/*
			 * The value is significantly more common than the non-MCV
			 * selectivity would suggest.  Keep it, and all the other more
			 * common values in the list.
			 */
			break;
		}
		else
		{
			/* Discard this value and consider the next least common value */
			num_mcv--;
			if (num_mcv == 0)
				break;
			sumcount -= mcv_counts[num_mcv - 1];
		}
	}
	return num_mcv;
}

#ifndef __OPENTENBASE__
#ifdef XCP
/*
 * coord_accu_distinct_stat
 *		Accumulate the distinct statistics for the attribute.
 *
 * Three kinds of distinct statistics value should be considered:
 * -1: all the values of the attribute are distinct. Then the relation tuple count is the distinct value;
 * (-1, 0): the ratio of the distinct attribute. Tuble count * (-1) * ratio is the count of distinct value;
 * >=0: the actual count of distinct value.
 *
 *
 */
static void
coord_accu_distinct_stat(float4 src_dist_value, float4 distinct_value, float4 rel_tuple, float4 *tar_dist_value)
{
	if ((int)rel_tuple == 0)
	{
		*tar_dist_value = src_dist_value;
		return;
	}

	if ((int)distinct_value == -1)
	{
		*tar_dist_value = src_dist_value + rel_tuple;
	}
	else if (distinct_value > -1 && distinct_value < 0)
	{
		*tar_dist_value = src_dist_value + rel_tuple * (-1) * distinct_value;
	}
	else
	{
		*tar_dist_value = src_dist_value + distinct_value;
	}
}

/*
 * coord_calc_distinct_stat
 *		Caculate the distinct statistics for the attribute.
 *
 * We already get the total count of the distinct value. But we must convert the count to the three kinds
 * of distinct statistics value for further use:
 * 1, If the distinct count equal with tuple count, the final result must be -1.
 * 2, If the the ratio of the distinct value is begger than 0.1, the final result should be -1 * ratio.
 * 3, if the ratio of the distinct value is less than 0.1, the final result is the actual distinct value.
 *
 */
static void
coord_calc_distinct_stat(float4 total_dist_value, float4 total_tuples, float4 *distinct_result)
{
	float4 dist_rate = 0;

	if ((int)total_dist_value == (int)total_tuples)
	{
		*distinct_result = -1;
	}
	else
	{
		dist_rate = total_dist_value / total_tuples;

		if (dist_rate < 0.1)
		{
			*distinct_result = total_tuples * dist_rate;
		}
		else
		{
			*distinct_result = (-1) * dist_rate;
		}
	}
}

/*
 * coord_collect_fit_simple_stats
 *		Collect and fit simple stats for a relation (pg_statistic contents).
 *
 * Collects statistics from the datanodes, and then try to fit the recived
 * statistics for each attribute.
 *
 * XXX We try to build statistics covering data from all the nodes, by collecting
 * fresh sample of rows or merging the statistics somehow.
 * We try to use the same formula in the single data node to fit the statistics
 * in corordinator. However, for some statistics we cannot fit them without raw
 * data, so we try to pick up the data with more sample.
 *
 */
static void
coord_collect_fit_simple_stats(Relation onerel, bool inh, int attr_cnt,
							   VacAttrStats **vacattrstats)
{
	char 		   *nspname;
	char 		   *relname;
	/* Fields to run query to read statistics from data nodes */
	StringInfoData  query;
	EState 		   *estate;
	MemoryContext 	oldcontext;
	RemoteQuery	    *step;
	RemoteQueryState *node;
	TupleTableSlot *result;
	int 			i;
	/* Number of data nodes from which attribute statistics are received. */
	int			   *numnodes;
	/* Number of tuples from which attribute statistics are received. */
	int            *atttuples;
	/* Number of maximum non-null count of each attribute*/
	int		*nonnull_max_count;
	/* Get the relation identifier */
	relname = RelationGetRelationName(onerel);
	nspname = get_namespace_name(RelationGetNamespace(onerel));
	/* Make up query string */
	initStringInfo(&query);
	/* Generic statistic fields */
	appendStringInfoString(&query, "SELECT s.staattnum, "
// assume the number of tuples approximately the same on all nodes
// to build more precise statistics get this number
						   "c.reltuples, "
						   "s.stanullfrac, "
						   "s.stawidth, "
						   "s.stadistinct");

	/* Detailed statistic slots */
	for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
		appendStringInfo(&query, ", s.stakind%d"
						 ", o%d.oprname"
						 ", no%d.nspname"
						 ", t%dl.typname"
						 ", nt%dl.nspname"
						 ", t%dr.typname"
						 ", nt%dr.nspname"
						 ", s.stanumbers%d"
						 ", s.stavalues%d",
						 i, i, i, i, i, i, i, i, i);

	/* Common part of FROM clause */
	appendStringInfoString(&query, " FROM pg_statistic s JOIN pg_class c "
						   "    ON s.starelid = c.oid "
						   "JOIN pg_namespace nc "
						   "    ON c.relnamespace = nc.oid ");

	/* Info about involved operations */
	for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
		appendStringInfo(&query, "LEFT JOIN (pg_operator o%d "
						 "           JOIN pg_namespace no%d "
						 "               ON o%d.oprnamespace = no%d.oid "
						 "           JOIN pg_type t%dl "
						 "               ON o%d.oprleft = t%dl.oid "
						 "           JOIN pg_namespace nt%dl "
						 "               ON t%dl.typnamespace = nt%dl.oid "
						 "           JOIN pg_type t%dr "
						 "               ON o%d.oprright = t%dr.oid "
						 "           JOIN pg_namespace nt%dr "
						 "               ON t%dr.typnamespace = nt%dr.oid) "
						 "    ON s.staop%d = o%d.oid ",
						 i, i, i, i, i, i, i, i, i,
						 i, i, i, i, i, i, i, i, i);

	appendStringInfo(&query, "WHERE nc.nspname = '%s' "
					 "AND c.relname = '%s'",
					 nspname, relname);
	/* Build up RemoteQuery */
	step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_NONE;
	step->exec_nodes = NULL;
	step->sql_statement = query.data;
	step->force_autocommit = true;
	step->exec_type = EXEC_ON_DATANODES;
	/* Add targetlist entries */
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "staattnum"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(RelationRelationId,
												 "pg_class",
												 "reltuples"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "stanullfrac"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "stawidth"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "stadistinct"));

	for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
	{
		/* 16 characters would be enough */
		char 	colname[16];
		sprintf(colname, "stakind%d", i);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(StatisticRelationId,
													 "pg_statistic",
													 colname));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(OperatorRelationId,
													 "pg_operator",
													 "oprname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(NamespaceRelationId,
													 "pg_namespace",
													 "nspname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(TypeRelationId,
													 "pg_type",
													 "typname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(NamespaceRelationId,
													 "pg_namespace",
													 "nspname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(TypeRelationId,
													 "pg_type",
													 "typname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(NamespaceRelationId,
													 "pg_namespace",
													 "nspname"));
		sprintf(colname, "stanumbers%d", i);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(StatisticRelationId,
													 "pg_statistic",
													 colname));
		sprintf(colname, "stavalues%d", i);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(StatisticRelationId,
													 "pg_statistic",
													 colname));
	}

	/* Execute query on the data nodes */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	/*
	 * Take a fresh snapshot so that we see the effects of the ANALYZE command
	 * on the datanode. That command is run in auto-commit mode hence just
	 * bumping up the command ID is not good enough
	 */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
	estate->es_snapshot = GetActiveSnapshot();
	node = ExecInitRemoteQuery(step, estate, 0);
	MemoryContextSwitchTo(oldcontext);
	/* get ready to combine results */
	numnodes = (int *) palloc(attr_cnt * sizeof(int));

	for (i = 0; i < attr_cnt; i++)
	{
		numnodes[i] = 0;
	}

	atttuples = (int *) palloc(attr_cnt * sizeof(int));
	nonnull_max_count = (int *)palloc(attr_cnt * sizeof(int));

	for (i = 0; i < attr_cnt; i++)
	{
		atttuples[i] = -1;
		nonnull_max_count[i] = -1;
	}

	result = ExecRemoteQuery((PlanState *) node);
	PopActiveSnapshot();

	while (result != NULL && !TupIsNull(result))
	{
		Datum 			value;
		bool			isnull;
		int 			colnum = 1;
		int16			attnum;
		float4			reltuples;
		float4			nullfrac;
		int32 			width;
		float4			distinct;
		VacAttrStats   *stats = NULL;
		/* Process statistics from the data node */
		value = slot_getattr(result, colnum++, &isnull); /* staattnum */
		attnum = DatumGetInt16(value);

		for (i = 0; i < attr_cnt; i++)
			if (vacattrstats[i]->attr->attnum == attnum)
			{
				stats = vacattrstats[i];
				stats->stats_valid = true;
				numnodes[i]++;
				break;
			}

		value = slot_getattr(result, colnum++, &isnull); /* reltuples */
		reltuples = DatumGetFloat4(value);

		/* Record the total tuple number for each attribute */
		if (atttuples[attnum - 1] != -1)
		{
			atttuples[attnum - 1] += reltuples;
		}
		else
		{
			atttuples[attnum - 1] = reltuples;
		}

		if (stats)
		{
			value = slot_getattr(result, colnum++, &isnull); /* stanullfrac */
			nullfrac = DatumGetFloat4(value);
			stats->stanullfrac += nullfrac * reltuples;

			if (reltuples * (1 - nullfrac) > nonnull_max_count[attnum - 1])
			{
				nonnull_max_count[attnum - 1] = reltuples * (1 - nullfrac);
			}

			value = slot_getattr(result, colnum++, &isnull); /* stawidth */
			width = DatumGetInt32(value);
			stats->stawidth += width * reltuples * (1 - nullfrac);
			value = slot_getattr(result, colnum++, &isnull); /* stadistinct */
			distinct = DatumGetFloat4(value);
			coord_accu_distinct_stat(stats->stadistinct, distinct, reltuples, &stats->stadistinct);

			/* Detailed statistics */
			for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
			{
				int16 		kind;
				float4	   *numbers;
				Datum	   *values;
				int			nnumbers, nvalues;
				int 		k;
				value = slot_getattr(result, colnum++, &isnull); /* kind */
				kind = DatumGetInt16(value);

				if (kind == 0)
				{
					/*
					 * Empty slot - skip next 8 fields: 6 fields of the
					 * operation identifier and two data fields (numbers and
					 * values)
					 */
					colnum += 8;
					continue;
				}
				else
				{
					Oid			oprid;
					/* Get operator */
					value = slot_getattr(result, colnum++, &isnull); /* oprname */

					if (isnull)
					{
						/*
						 * Operator is not specified for that kind, skip remaining
						 * fields to lookup the operator
						 */
						oprid = InvalidOid;
						colnum += 5; /* skip operation nsp and types */
					}
					else
					{
						char	   *oprname;
						char	   *oprnspname;
						Oid			ltypid, rtypid;
						char	   *ltypname,
								   *rtypname;
						char	   *ltypnspname,
								   *rtypnspname;
						oprname = DatumGetCString(value);
						value = slot_getattr(result, colnum++, &isnull); /* oprnspname */
						oprnspname = DatumGetCString(value);
						/* Get left operand data type */
						value = slot_getattr(result, colnum++, &isnull); /* typname */
						ltypname = DatumGetCString(value);
						value = slot_getattr(result, colnum++, &isnull); /* typnspname */
						ltypnspname = DatumGetCString(value);
						ltypid = get_typname_typid(ltypname,
												   get_namespaceid(ltypnspname));
						/* Get right operand data type */
						value = slot_getattr(result, colnum++, &isnull); /* typname */
						rtypname = DatumGetCString(value);
						value = slot_getattr(result, colnum++, &isnull); /* typnspname */
						rtypnspname = DatumGetCString(value);
						rtypid = get_typname_typid(rtypname,
												   get_namespaceid(rtypnspname));
						/* lookup operator */
						oprid = get_operid(oprname, ltypid, rtypid,
										   get_namespaceid(oprnspname));
					}

					/*
					 * Look up a statistics slot. If there is an entry of the
					 * same kind already, leave it, assuming the statistics
					 * is approximately the same on all nodes, so values from
					 * one node are representing entire relation well.
					 * If empty slot is found store values here. If no more
					 * slots skip remaining values.
					 */
					for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
					{
						if (stats->stakind[k] == 0 ||
							(stats->stakind[k] == kind && stats->staop[k] == oprid))
						{
							break;
						}
					}

					if (k >= STATISTIC_NUM_SLOTS)
					{
						/* No empty slots */
						break;
					}

					/*
					 * If it is an existing slot which has numbers or values
					 * continue to the next set. If slot exists but without
					 * numbers and values, try to acquire them now
					 */
					if (stats->stakind[k] != 0 && (stats->numnumbers[k] > 0 ||
												   stats->numvalues[k] > 0))
					{
						if (reltuples * (1 - nullfrac) < nonnull_max_count[i])
						{
							colnum += 2; /* skip numbers and values */
							continue;
						}
					}

					/*
					 * Initialize slot
					 */
					stats->stakind[k] = kind;
					stats->staop[k] = oprid;
					stats->numnumbers[k] = 0;
					stats->stanumbers[k] = NULL;
					stats->numvalues[k] = 0;
					stats->stavalues[k] = NULL;
					stats->statypid[k] = InvalidOid;
					stats->statyplen[k] = -1;
					stats->statypalign[k] = 'i';
					stats->statypbyval[k] = true;
				}

				/* get numbers */
				value = slot_getattr(result, colnum++, &isnull); /* numbers */

				if (!isnull)
				{
					ArrayType  *arry = DatumGetArrayTypeP(value);
					/*
					 * We expect the array to be a 1-D float4 array; verify that. We don't
					 * need to use deconstruct_array() since the array data is just going
					 * to look like a C array of float4 values.
					 */
					nnumbers = ARR_DIMS(arry)[0];

					if (ARR_NDIM(arry) != 1 || nnumbers <= 0 ||
						ARR_HASNULL(arry) ||
						ARR_ELEMTYPE(arry) != FLOAT4OID)
					{
						elog(ERROR, "stanumbers is not a 1-D float4 array");
					}

					numbers = (float4 *) palloc(nnumbers * sizeof(float4));
					memcpy(numbers, ARR_DATA_PTR(arry),
						   nnumbers * sizeof(float4));

					/*
					 * Free arry if it's a detoasted copy.
					 */
					if ((Pointer) arry != DatumGetPointer(value))
					{
						pfree(arry);
					}

					stats->numnumbers[k] = nnumbers;
					stats->stanumbers[k] = numbers;
				}

				/* get values */
				value = slot_getattr(result, colnum++, &isnull); /* values */

				if (!isnull)
				{
					int 		j;
					ArrayType  *arry;
					int16		elmlen;
					bool		elmbyval;
					char		elmalign;
					arry = DatumGetArrayTypeP(value);
					/* We could cache this data, but not clear it's worth it */
					get_typlenbyvalalign(ARR_ELEMTYPE(arry),
										 &elmlen, &elmbyval, &elmalign);
					/* Deconstruct array into Datum elements; NULLs not expected */
					deconstruct_array(arry,
									  ARR_ELEMTYPE(arry),
									  elmlen, elmbyval, elmalign,
									  &values, NULL, &nvalues);

					/*
					 * If the element type is pass-by-reference, we now have a bunch of
					 * Datums that are pointers into the syscache value.  Copy them to
					 * avoid problems if syscache decides to drop the entry.
					 */
					if (!elmbyval)
					{
						for (j = 0; j < nvalues; j++)
						{
							values[j] = datumCopy(values[j], elmbyval, elmlen);
						}
					}

					/*
					 * Free statarray if it's a detoasted copy.
					 */
					if ((Pointer) arry != DatumGetPointer(value))
					{
						pfree(arry);
					}

					stats->numvalues[k] = nvalues;
					stats->stavalues[k] = values;
					/* store details about values data type */
					stats->statypid[k] = ARR_ELEMTYPE(arry);
					stats->statyplen[k] = elmlen;
					stats->statypalign[k] = elmalign;
					stats->statypbyval[k] = elmbyval;
				}
			}
		}

		/* fetch next */
		result = ExecRemoteQuery((PlanState *) node);
	}

	ExecEndRemoteQuery(node);

	for (i = 0; i < attr_cnt; i++)
	{
		VacAttrStats *stats = vacattrstats[i];

		if (numnodes[i] > 0)
		{
			stats->stanullfrac /= atttuples[stats->attr->attnum - 1];

			if ((int)stats->stanullfrac == 1)
			{
				stats->stawidth = 0;
			}
			else
			{
				stats->stawidth /= atttuples[stats->attr->attnum - 1] * (1 - stats->stanullfrac);
			}

			coord_calc_distinct_stat(stats->stadistinct, atttuples[stats->attr->attnum - 1], &stats->stadistinct);
		}
	}

	update_attstats(RelationGetRelid(onerel), inh, attr_cnt, vacattrstats,
					RelationGetRelPersistence(onerel), ANALYZE_LOCAL(options));
}

/*
 * coord_collect_simple_stats
 *		Collect simple stats for a relation (pg_statistic contents).
 *
 * Collects statistics from the datanodes, and then keeps the one of the
 * received statistics for each attribute (the first one we receive, but
 * it's mostly random).
 *
 * XXX We do not try to build statistics covering data fro all the nodes,
 * either by collecting fresh sample of rows or merging the statistics
 * somehow. The current approach is very simple and cheap, but may have
 * negative impact on estimate accuracy as the stats only covers data
 * from a single node, and we may end up with stats from different node
 * for each attribute.
 */
static void
coord_collect_simple_stats(Relation onerel, bool inh, int attr_cnt,
						   VacAttrStats **vacattrstats)
{
	char 		   *nspname;
	char 		   *relname;
	/* Fields to run query to read statistics from data nodes */
	StringInfoData  query;
	EState 		   *estate;
	MemoryContext 	oldcontext;
	RemoteQuery	    *step;
	RemoteQueryState *node;
	TupleTableSlot *result;
	int 			i;
	/* Number of data nodes from which attribute statistics are received. */
	int			   *numnodes;
	/* Get the relation identifier */
	relname = RelationGetRelationName(onerel);
	nspname = get_namespace_name(RelationGetNamespace(onerel));
	/* Make up query string */
	initStringInfo(&query);
	/* Generic statistic fields */
	appendStringInfoString(&query, "SELECT s.staattnum, "
// assume the number of tuples approximately the same on all nodes
// to build more precise statistics get this number
//										  "c.reltuples, "
						   "s.stanullfrac, "
						   "s.stawidth, "
						   "s.stadistinct");

	/* Detailed statistic slots */
	for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
		appendStringInfo(&query, ", s.stakind%d"
						 ", o%d.oprname"
						 ", no%d.nspname"
						 ", t%dl.typname"
						 ", nt%dl.nspname"
						 ", t%dr.typname"
						 ", nt%dr.nspname"
						 ", s.stanumbers%d"
						 ", s.stavalues%d",
						 i, i, i, i, i, i, i, i, i);

	/* Common part of FROM clause */
	appendStringInfoString(&query, " FROM pg_statistic s JOIN pg_class c "
						   "    ON s.starelid = c.oid "
						   "JOIN pg_namespace nc "
						   "    ON c.relnamespace = nc.oid ");

	/* Info about involved operations */
	for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
		appendStringInfo(&query, "LEFT JOIN (pg_operator o%d "
						 "           JOIN pg_namespace no%d "
						 "               ON o%d.oprnamespace = no%d.oid "
						 "           JOIN pg_type t%dl "
						 "               ON o%d.oprleft = t%dl.oid "
						 "           JOIN pg_namespace nt%dl "
						 "               ON t%dl.typnamespace = nt%dl.oid "
						 "           JOIN pg_type t%dr "
						 "               ON o%d.oprright = t%dr.oid "
						 "           JOIN pg_namespace nt%dr "
						 "               ON t%dr.typnamespace = nt%dr.oid) "
						 "    ON s.staop%d = o%d.oid ",
						 i, i, i, i, i, i, i, i, i,
						 i, i, i, i, i, i, i, i, i);

	appendStringInfo(&query, "WHERE nc.nspname = '%s' "
					 "AND c.relname = '%s'",
					 nspname, relname);
	/* Build up RemoteQuery */
	step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_NONE;
	step->exec_nodes = NULL;
	step->sql_statement = query.data;
	step->force_autocommit = true;
	step->exec_type = EXEC_ON_DATANODES;
	/* Add targetlist entries */
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "staattnum"));
//	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
//										 make_relation_tle(RelationRelationId,
//														   "pg_class",
//														   "reltuples"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "stanullfrac"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "stawidth"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticRelationId,
												 "pg_statistic",
												 "stadistinct"));

	for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
	{
		/* 16 characters would be enough */
		char 	colname[16];
		sprintf(colname, "stakind%d", i);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(StatisticRelationId,
													 "pg_statistic",
													 colname));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(OperatorRelationId,
													 "pg_operator",
													 "oprname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(NamespaceRelationId,
													 "pg_namespace",
													 "nspname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(TypeRelationId,
													 "pg_type",
													 "typname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(NamespaceRelationId,
													 "pg_namespace",
													 "nspname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(TypeRelationId,
													 "pg_type",
													 "typname"));
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(NamespaceRelationId,
													 "pg_namespace",
													 "nspname"));
		sprintf(colname, "stanumbers%d", i);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(StatisticRelationId,
													 "pg_statistic",
													 colname));
		sprintf(colname, "stavalues%d", i);
		step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
											 make_relation_tle(StatisticRelationId,
													 "pg_statistic",
													 colname));
	}

	/* Execute query on the data nodes */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	/*
	 * Take a fresh snapshot so that we see the effects of the ANALYZE command
	 * on the datanode. That command is run in auto-commit mode hence just
	 * bumping up the command ID is not good enough
	 */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
	estate->es_snapshot = GetActiveSnapshot();
	node = ExecInitRemoteQuery(step, estate, 0);
	MemoryContextSwitchTo(oldcontext);
	/* get ready to combine results */
	numnodes = (int *) palloc(attr_cnt * sizeof(int));

	for (i = 0; i < attr_cnt; i++)
	{
		numnodes[i] = 0;
	}

	result = ExecRemoteQuery((PlanState *) node);
	PopActiveSnapshot();

	while (result != NULL && !TupIsNull(result))
	{
		Datum 			value;
		bool			isnull;
		int 			colnum = 1;
		int16			attnum;
//		float4			reltuples;
		float4			nullfrac;
		int32 			width;
		float4			distinct;
		VacAttrStats   *stats = NULL;
		/* Process statistics from the data node */
		value = slot_getattr(result, colnum++, &isnull); /* staattnum */
		attnum = DatumGetInt16(value);

		for (i = 0; i < attr_cnt; i++)
			if (vacattrstats[i]->attr->attnum == attnum)
			{
				stats = vacattrstats[i];
				stats->stats_valid = true;
				numnodes[i]++;
				break;
			}

//		value = slot_getattr(result, colnum++, &isnull); /* reltuples */
//		reltuples = DatumGetFloat4(value);

		if (stats)
		{
			value = slot_getattr(result, colnum++, &isnull); /* stanullfrac */
			nullfrac = DatumGetFloat4(value);
			stats->stanullfrac += nullfrac;
			value = slot_getattr(result, colnum++, &isnull); /* stawidth */
			width = DatumGetInt32(value);
			stats->stawidth += width;
			value = slot_getattr(result, colnum++, &isnull); /* stadistinct */
			distinct = DatumGetFloat4(value);
			stats->stadistinct += distinct;

			/* Detailed statistics */
			for (i = 1; i <= STATISTIC_NUM_SLOTS; i++)
			{
				int16 		kind;
				float4	   *numbers;
				Datum	   *values;
				int			nnumbers, nvalues;
				int 		k;
				value = slot_getattr(result, colnum++, &isnull); /* kind */
				kind = DatumGetInt16(value);

				if (kind == 0)
				{
					/*
					 * Empty slot - skip next 8 fields: 6 fields of the
					 * operation identifier and two data fields (numbers and
					 * values)
					 */
					colnum += 8;
					continue;
				}
				else
				{
					Oid			oprid;
					/* Get operator */
					value = slot_getattr(result, colnum++, &isnull); /* oprname */

					if (isnull)
					{
						/*
						 * Operator is not specified for that kind, skip remaining
						 * fields to lookup the operator
						 */
						oprid = InvalidOid;
						colnum += 5; /* skip operation nsp and types */
					}
					else
					{
						char	   *oprname;
						char	   *oprnspname;
						Oid			ltypid, rtypid;
						char	   *ltypname,
								   *rtypname;
						char	   *ltypnspname,
								   *rtypnspname;
						oprname = DatumGetCString(value);
						value = slot_getattr(result, colnum++, &isnull); /* oprnspname */
						oprnspname = DatumGetCString(value);
						/* Get left operand data type */
						value = slot_getattr(result, colnum++, &isnull); /* typname */
						ltypname = DatumGetCString(value);
						value = slot_getattr(result, colnum++, &isnull); /* typnspname */
						ltypnspname = DatumGetCString(value);
						ltypid = get_typname_typid(ltypname,
												   get_namespaceid(ltypnspname));
						/* Get right operand data type */
						value = slot_getattr(result, colnum++, &isnull); /* typname */
						rtypname = DatumGetCString(value);
						value = slot_getattr(result, colnum++, &isnull); /* typnspname */
						rtypnspname = DatumGetCString(value);
						rtypid = get_typname_typid(rtypname,
												   get_namespaceid(rtypnspname));
						/* lookup operator */
						oprid = get_operid(oprname, ltypid, rtypid,
										   get_namespaceid(oprnspname));
					}

					/*
					 * Look up a statistics slot. If there is an entry of the
					 * same kind already, leave it, assuming the statistics
					 * is approximately the same on all nodes, so values from
					 * one node are representing entire relation well.
					 * If empty slot is found store values here. If no more
					 * slots skip remaining values.
					 */
					for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
					{
						if (stats->stakind[k] == 0 ||
							(stats->stakind[k] == kind && stats->staop[k] == oprid))
						{
							break;
						}
					}

					if (k >= STATISTIC_NUM_SLOTS)
					{
						/* No empty slots */
						break;
					}

					/*
					 * If it is an existing slot which has numbers or values
					 * continue to the next set. If slot exists but without
					 * numbers and values, try to acquire them now
					 */
					if (stats->stakind[k] != 0 && (stats->numnumbers[k] > 0 ||
												   stats->numvalues[k] > 0))
					{
						colnum += 2; /* skip numbers and values */
						continue;
					}

					/*
					 * Initialize slot
					 */
					stats->stakind[k] = kind;
					stats->staop[k] = oprid;
					stats->numnumbers[k] = 0;
					stats->stanumbers[k] = NULL;
					stats->numvalues[k] = 0;
					stats->stavalues[k] = NULL;
					stats->statypid[k] = InvalidOid;
					stats->statyplen[k] = -1;
					stats->statypalign[k] = 'i';
					stats->statypbyval[k] = true;
				}

				/* get numbers */
				value = slot_getattr(result, colnum++, &isnull); /* numbers */

				if (!isnull)
				{
					ArrayType  *arry = DatumGetArrayTypeP(value);
					/*
					 * We expect the array to be a 1-D float4 array; verify that. We don't
					 * need to use deconstruct_array() since the array data is just going
					 * to look like a C array of float4 values.
					 */
					nnumbers = ARR_DIMS(arry)[0];

					if (ARR_NDIM(arry) != 1 || nnumbers <= 0 ||
						ARR_HASNULL(arry) ||
						ARR_ELEMTYPE(arry) != FLOAT4OID)
					{
						elog(ERROR, "stanumbers is not a 1-D float4 array");
					}

					numbers = (float4 *) palloc(nnumbers * sizeof(float4));
					memcpy(numbers, ARR_DATA_PTR(arry),
						   nnumbers * sizeof(float4));

					/*
					 * Free arry if it's a detoasted copy.
					 */
					if ((Pointer) arry != DatumGetPointer(value))
					{
						pfree(arry);
					}

					stats->numnumbers[k] = nnumbers;
					stats->stanumbers[k] = numbers;
				}

				/* get values */
				value = slot_getattr(result, colnum++, &isnull); /* values */

				if (!isnull)
				{
					int 		j;
					ArrayType  *arry;
					int16		elmlen;
					bool		elmbyval;
					char		elmalign;
					arry = DatumGetArrayTypeP(value);
					/* We could cache this data, but not clear it's worth it */
					get_typlenbyvalalign(ARR_ELEMTYPE(arry),
										 &elmlen, &elmbyval, &elmalign);
					/* Deconstruct array into Datum elements; NULLs not expected */
					deconstruct_array(arry,
									  ARR_ELEMTYPE(arry),
									  elmlen, elmbyval, elmalign,
									  &values, NULL, &nvalues);

					/*
					 * If the element type is pass-by-reference, we now have a bunch of
					 * Datums that are pointers into the syscache value.  Copy them to
					 * avoid problems if syscache decides to drop the entry.
					 */
					if (!elmbyval)
					{
						for (j = 0; j < nvalues; j++)
						{
							values[j] = datumCopy(values[j], elmbyval, elmlen);
						}
					}

					/*
					 * Free statarray if it's a detoasted copy.
					 */
					if ((Pointer) arry != DatumGetPointer(value))
					{
						pfree(arry);
					}

					stats->numvalues[k] = nvalues;
					stats->stavalues[k] = values;
					/* store details about values data type */
					stats->statypid[k] = ARR_ELEMTYPE(arry);
					stats->statyplen[k] = elmlen;
					stats->statypalign[k] = elmalign;
					stats->statypbyval[k] = elmbyval;
				}
			}
		}

		/* fetch next */
		result = ExecRemoteQuery((PlanState *) node);
	}

	ExecEndRemoteQuery(node);

	for (i = 0; i < attr_cnt; i++)
	{
		VacAttrStats *stats = vacattrstats[i];

		if (numnodes[i] > 0)
		{
			stats->stanullfrac /= numnodes[i];
			stats->stawidth /= numnodes[i];
			stats->stadistinct /= numnodes[i];
		}
	}

	update_attstats(RelationGetRelid(onerel), inh, attr_cnt, vacattrstats,
					RelationGetRelPersistence(onerel), ANALYZE_LOCAL(options));
}

/*
 * coord_collect_extended_stats
 *		Collect extended stats for a relation (pg_statistic_ext contents).
 *
 * Collects statistics from the datanodes, and then keeps the one of the
 * received statistics for each attribute (the first one we receive, but
 * it's mostly random).
 *
 * XXX This has similar issues as coord_collect_simple_stats.
 */
static void
coord_collect_extended_stats(Relation onerel, int attr_cnt)
{
	char 		   *nspname;
	char 		   *relname;
	/* Fields to run query to read statistics from data nodes */
	StringInfoData  query;
	EState 		   *estate;
	MemoryContext 	oldcontext;
	RemoteQuery	    *step;
	RemoteQueryState *node;
	TupleTableSlot *result;
	int 			i;
	/* Number of data nodes from which attribute statistics are received. */
	int			   *numnodes;
	List		   *stat_oids;
	/* Get the relation identifier */
	relname = RelationGetRelationName(onerel);
	nspname = get_namespace_name(RelationGetNamespace(onerel));
	/*
	 * Build extended statistics on the coordinator.
	 *
	 * We take an approach similar to the simple per-attribute stats by
	 * fetching the already-built extended statistics, and pick data
	 * from a random datanode on the assumption that the datanodes are
	 * fairly similar in terms of data volume and distribution.
	 *
	 * That seems to be working fairly well, although there are likely
	 * some weaknesses too - e.g. on distribution keys it may easily
	 * neglect large portions of the data.
	 */
	/* Make up query string fetching data from pg_statistic_ext */
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT ns.nspname, "
					 "stxname, "
					 "stxndistinct::bytea AS stxndistinct, "
					 "stxdependencies::bytea AS stxdependencies "
					 " FROM pg_statistic_ext s JOIN pg_class c "
					 "    ON s.stxrelid = c.oid "
					 "JOIN pg_namespace nc "
					 "    ON c.relnamespace = nc.oid "
					 "JOIN pg_namespace ns "
					 "    ON s.stxnamespace = ns.oid "
					 "WHERE nc.nspname = '%s' AND c.relname = '%s'",
					 nspname, relname);
	/* Build up RemoteQuery */
	step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_NONE;
	step->exec_nodes = NULL;
	step->sql_statement = query.data;
	step->force_autocommit = true;
	step->exec_type = EXEC_ON_DATANODES;
	/* Add targetlist entries */
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(NamespaceRelationId,
												 "pg_namespace",
												 "nspname"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticExtRelationId,
												 "pg_statistic_ext",
												 "stxname"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticExtRelationId,
												 "pg_statistic_ext",
												 "stxndistinct"));
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 make_relation_tle(StatisticExtRelationId,
												 "pg_statistic_ext",
												 "stxdependencies"));
	/* Execute query on the data nodes */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	/*
	 * Take a fresh snapshot so that we see the effects of the ANALYZE
	 * command on datanodes. That command is run in auto-commit mode
	 * hence just bumping up the command ID is not good enough.
	 */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
	estate->es_snapshot = GetActiveSnapshot();
	node = ExecInitRemoteQuery(step, estate, 0);
	MemoryContextSwitchTo(oldcontext);
	/* get ready to combine results */
	numnodes = (int *) palloc(attr_cnt * sizeof(int));

	for (i = 0; i < attr_cnt; i++)
	{
		numnodes[i] = 0;
	}

	result = ExecRemoteQuery((PlanState *) node);
	PopActiveSnapshot();
	/*
	 * We only want to update each statistics once, as we'd get errors
	 * about self-updated tuples otherwise. So keep a list of OIDs for
	 * stats we already updated, and check before each update.
	 */
	stat_oids = NIL;

	while (result != NULL && !TupIsNull(result))
	{
		Datum 			value;
		bool			isnull;
		Name			nspname;
		Name			stxname;
		bytea		   *stxndistinct = NULL;
		bytea		   *stxdependencies = NULL;
		HeapTuple		htup;
		Oid				nspoid;
		Oid				stat_oid;
		bool			updated;
		ListCell	   *lc;
		/* Process statistics from the data node */
		value = slot_getattr(result, 1, &isnull); /* nspname */
		nspname = DatumGetName(value);
		value = slot_getattr(result, 2, &isnull); /* stxname */
		stxname = DatumGetName(value);
		value = slot_getattr(result, 3, &isnull); /* stxndistinct */

		if (!isnull)
		{
			stxndistinct = DatumGetByteaP(value);
		}

		value = slot_getattr(result, 4, &isnull); /* stxdependencies */

		if (!isnull)
		{
			stxdependencies = DatumGetByteaP(value);
		}

		nspoid = get_namespace_oid(NameStr(*nspname), false);
		/* get OID of the statistics */
		htup = SearchSysCache2(STATEXTNAMENSP,
							   NameGetDatum(stxname),
							   ObjectIdGetDatum(nspoid));

		/* if relation is already dropped */
		if (!htup)
        {
		    continue;
        }

		stat_oid = HeapTupleGetOid(htup);
		ReleaseSysCache(htup);
		/* see if we already updated this pg_statistic_ext tuple */
		updated = false;

		foreach (lc, stat_oids)
		{
			Oid oid = lfirst_oid(lc);

			if (stat_oid == oid)
			{
				updated = true;
				break;
			}
		}

		/* if not, update it (with all the available data) */
		if (!updated)
		{
			update_ext_stats(nspname, stxname, stxndistinct, stxdependencies);
			stat_oids = lappend_oid(stat_oids, stat_oid);
		}

		/* fetch stats from next node */
		result = ExecRemoteQuery((PlanState *) node);
	}

	ExecEndRemoteQuery(node);
}

/*
 * analyze_rel_coordinator
 *		Collect all statistics for a particular relation.
 *
 * We collect three types of statistics for each table:
 *
 * - simple statistics (pg_statistic)
 * - extended statistics (pg_statistic_ext)
 * - index statistics (including expression indexes)
 */
static void
analyze_rel_coordinator(Relation onerel, bool inh, int attr_cnt,
						VacAttrStats **vacattrstats, int nindexes,
						Relation *indexes, AnlIndexData *indexdata)
{
	int i;

	if (random_collect_stats)
	{
		/* simple statistics (pg_statistic) for the relation */
		coord_collect_simple_stats(onerel, inh, attr_cnt, vacattrstats);

		/* simple statistics (pg_statistic) for all indexes */
		for (i = 0; i < nindexes; i++)
			coord_collect_simple_stats(indexes[i], false,
									   indexdata[i].attr_cnt,
									   indexdata[i].vacattrstats);
	}
	else
	{
		/* collect and fit simple statistics (pg_statistic) for the relation */
		coord_collect_fit_simple_stats(onerel, inh, attr_cnt, vacattrstats);

		/* collect and fit simple statistics (pg_statistic) for all indexes */
		for (i = 0; i < nindexes; i++)
			coord_collect_fit_simple_stats(indexes[i], false,
										   indexdata[i].attr_cnt,
										   indexdata[i].vacattrstats);
	}

	/* extended statistics (pg_statistic) for the relation */
	coord_collect_extended_stats(onerel, attr_cnt);
}
#endif
#endif

#ifdef __OPENTENBASE__
static void
get_rel_pages_visiblepages(Relation onerel,
						   BlockNumber *pages, 
						   BlockNumber *visiblepages)
{
	if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
		onerel->rd_rel->relhassubclass)
	{
		List * childs;
		ListCell * lc;

		childs = find_all_inheritors(RelationGetRelid(onerel), NoLock, NULL);

		*pages = 0;
		*visiblepages = 0;

		foreach (lc, childs)
		{
			Oid			childOID = lfirst_oid(lc);
			Relation	childrel;
			BlockNumber visible;

			/* We already got the needed lock */
			childrel = heap_open(childOID, AccessShareLock);

			/* Ignore if temp table of another backend */
			if (RELATION_IS_OTHER_TEMP(childrel))
			{
				/* ... but release the lock on it */
				Assert(childrel != onerel);
				heap_close(childrel, AccessShareLock);
				continue;
			}

			/* Check table type (MATVIEW can't happen, but might as well allow) */
			if (childrel->rd_rel->relkind == RELKIND_RELATION ||
				childrel->rd_rel->relkind == RELKIND_MATVIEW)
			{
				*pages += RelationGetNumberOfBlocks(childrel);

				visibilitymap_count(childrel, &visible, NULL);
				*visiblepages += visible;
				heap_close(childrel, AccessShareLock);
			}
			else
			{
				Assert(childrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
				heap_close(childrel, AccessShareLock);
				continue;
			}
		}
		list_free(childs);
	}
	else
	{
		*pages = RelationGetNumberOfBlocks(onerel);
		visibilitymap_count(onerel, visiblepages, NULL);
	}
}

#define SAMPLE_ATTR_NUM 6

void
ExecSample(SampleStmt *stmt, DestReceiver *dest)
{
	Oid			 relid;
	Relation	 onerel;
	TupleDesc 	 tupdesc;
	TupleDesc	 rowdesc;
	HeapTuple 	*rows;
	int 		 targetrows = stmt->rownum;
	SampleRowsContext *context;
	TupOutputState *tstate;
	int 		 index = 0;
	bool 		 nulls[SAMPLE_ATTR_NUM];
	Datum 		 values[SAMPLE_ATTR_NUM];

	if (!IS_PGXC_COORDINATOR && !IS_PGXC_DATANODE)
	{
		elog(ERROR, "SAMPLE only support on Coordinator or Datanode");
	}

	if (stmt->rownum <= 0)
	{
		elog(ERROR, "SAMPLE row number must larger than 0");
	}

	relid = RangeVarGetRelid(stmt->relation, NoLock, false);
	onerel = try_relation_open(relid, AccessShareLock);

	if (!onerel)
		elog(ERROR, "could not open relation with OID %u", relid);

	/*
	 * Check permissions --- this should match vacuum's check!
	 */
	if (!(pg_class_ownercheck(RelationGetRelid(onerel), GetUserId()) ||
		  (pg_database_ownercheck(MyDatabaseId, GetUserId()) && !onerel->rd_rel->relisshared) ||
		  pg_class_aclcheck(relid, GetUserId(), ACL_MAINTAIN) == ACLCHECK_OK ||
			has_partition_ancestor_privs(relid, GetUserId(), ACL_MAINTAIN)))
	{
		relation_close(onerel, AccessShareLock);
		elog(ERROR, "Permission denied for ownership");
		return;
	}

	/*
	 * Silently ignore tables that are temp tables of other backends ---
	 * trying to analyze these is rather pointless, since their contents are
	 * probably not up-to-date on disk.  (We don't throw a warning here; it
	 * would just lead to chatter during a database-wide ANALYZE.)
	 */
	if (RELATION_IS_OTHER_TEMP(onerel))
	{
		relation_close(onerel, AccessShareLock);
		elog(ERROR, "SAMPLE do not suppport temp table");
		return;
	}

	/*
	 * We can ANALYZE any table except pg_statistic. See update_attstats
	 */
	if (RelationGetRelid(onerel) == StatisticRelationId)
	{
		relation_close(onerel, AccessShareLock);
		elog(ERROR, "SAMPLE do not support pg_statistic");
		return;
	}

	/* Check that it's a plain table, materialized view */
	if (onerel->rd_rel->relkind != RELKIND_RELATION &&
		onerel->rd_rel->relkind != RELKIND_MATVIEW &&
		onerel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		onerel->rd_rel->relkind != RELKIND_GLOBAL_INDEX &&
		onerel->rd_rel->relkind != RELKIND_PARTITIONED_GLOBAL_INDEX)
	{
		relation_close(onerel, AccessShareLock);
		elog(ERROR, "SAMPLE only support Tables or Materialized Views");
		return;

	}

	rowdesc = BlessTupleDesc(RelationGetDescr(onerel));
	rows = (HeapTuple *) palloc0(targetrows * sizeof(HeapTuple));
	context = (SampleRowsContext *)palloc0(sizeof(SampleRowsContext));
	context->rows = rows;
	vac_strategy = GetAccessStrategy(BAS_VACUUM);

	/* initialize acquire sample rows */
	if (IS_PGXC_COORDINATOR && onerel->rd_locator_info && 
		!RELATION_IS_COORDINATOR_LOCAL(onerel))
	{
		context->samplenum = acquire_coordinator_sample_rows(onerel, DEBUG2,
 														   rows, stmt->rownum, 
 														   &context->totalnum, 
 														   &context->deadnum,
														   &context->totalpages,
														   &context->visiblepages);
	}
	else
	{
		BlockNumber relpages;
		BlockNumber relallvisible;
		get_rel_pages_visiblepages(onerel, &relpages,
								   &relallvisible);

		context->totalpages = relpages;
		context->visiblepages = relallvisible;

			if (onerel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ||
				onerel->rd_rel->relhassubclass)
			{
				/* do not update catalog because we are not holding self-exclusive
				 * lock, see commend about relhassubclass in DefineRelation.
				 */
				context->samplenum = acquire_inherited_sample_rows(onerel, DEBUG2, 
																rows, stmt->rownum, 
																&context->totalnum, 
																&context->deadnum,
																false);
			}
			else 
			{
				context->samplenum = acquire_sample_rows(onerel, DEBUG2, 
														rows, stmt->rownum, 
														&context->totalnum, 
														&context->deadnum);
			}
		}

	tupdesc = CreateTemplateTupleDesc(SAMPLE_ATTR_NUM, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "samplenum",
					   FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "totalnum",
					   FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "deadnum",
					   FLOAT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "totalpages",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "visiblepages",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "rows",
					   onerel->rd_rel->reltype, -1, 0);

	tstate = begin_tup_output_tupdesc(dest, tupdesc);
	MemSet(nulls, 0, sizeof(nulls));
	nulls[5] = true;
	values[0] = Float8GetDatum(context->samplenum);
	values[1] = Float8GetDatum(context->totalnum);
	values[2] = Float8GetDatum(context->deadnum);
	values[3] = Int64GetDatum(context->totalpages);
	values[4] = Int64GetDatum(context->visiblepages);

	do_tup_output(tstate, values, nulls);
	
	if (context->samplenum > 0)
	{
		for (index = 0; index < context->samplenum; index++)
		{
			nulls[0] = true;
			nulls[1] = true;
			nulls[2] = true;
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = false;
			values[5] = heap_copy_tuple_as_datum(context->rows[index], rowdesc);
			do_tup_output(tstate, values, nulls);
		}

	}

	end_tup_output(tstate);

	pfree(rows);
	pfree(context);
	
	relation_close(onerel, AccessShareLock);
	return;
}

static int 
acquire_coordinator_sample_rows(Relation onerel, int elevel,
								HeapTuple *rows, int targrows,
								double *totalrows, double *totaldeadrows,
								int64 *totalpages, int64 *visiblepages)
{
	char		   *nspname;
	char		   *relname;
	/* Fields to run query to read statistics from data nodes */
	StringInfoData	query;
	EState		   *estate;
	MemoryContext	oldcontext;
	RemoteQuery 	*step;
	RemoteQueryState *node;
	RelationLocInfo *rellocinfo;
	TupleTableSlot *result;
	bool			isreplica = false;
	Var			   *dummy;
	double			samplenum = 0;
	double			totalnum = 0;
	double			deadnum = 0;
	int				numrows = 0;
	double			samplerows = 0;
	double			rowstoskip = -1;	/* -1 means not set yet */
	ReservoirStateData rstate;
	int64			totalpagesnum = 0;
	int64			visiblepagesnum = 0;
	int				nodenum = 1;

	/* Get the relation identifier */
	relname = RelationGetRelationName(onerel);
	nspname = get_namespace_name(RelationGetNamespace(onerel));
	rellocinfo = onerel->rd_locator_info;
	isreplica = IsRelationReplicated(rellocinfo);

	/* Get the distribution node num of current relation */
	if (list_length(rellocinfo->rl_nodeList) > 0)
	{
		nodenum = list_length(rellocinfo->rl_nodeList);
	}

	/* Make up query string */
	initStringInfo(&query);

	/*
	 * We assume all tuples are evenly distributed on all nodes. The target
	 * sample row number is calculated for table with over 10 billion rows.
	 */
	if (onerel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
	{
		appendStringInfo(&query,
						 "SAMPLE %s.%s(%d)",
						 quote_identifier("pg_temp"),
						 quote_identifier(relname),
						 targrows / nodenum + 1);
	}
	else
	{
		appendStringInfo(&query,
						 "SAMPLE %s.%s(%d)",
						 quote_identifier(nspname),
						 quote_identifier(relname),
						 targrows / nodenum + 1);
	}

	/* Build up RemoteQuery */
	step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_NONE;
	step->exec_nodes = makeNode(ExecNodes);
	step->exec_nodes->nodeList = NULL;
	step->read_only = true; /* the statistics collection routine should not reuse the "write" network connection. */
	if (isreplica)
	{
		step->exec_nodes->nodeList = lappend_int(step->exec_nodes->nodeList, linitial_int(rellocinfo->rl_nodeList));
	}
	else 
	{
		step->exec_nodes->nodeList = rellocinfo->rl_nodeList;
	}
	
	step->sql_statement = query.data;
	step->force_autocommit = false;
	step->exec_type = EXEC_ON_DATANODES;

	dummy = makeVar(1, 0, FLOAT8OID, 0, InvalidOid, 0);
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 0, "samplenum", false));
	dummy = makeVar(1, 1, FLOAT8OID, 0, InvalidOid, 0);
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 1, "totalnum", false));
	dummy = makeVar(1, 2, FLOAT8OID, 0, InvalidOid, 0);
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 2, "deadnum", false));
	dummy = makeVar(1, 3, INT8OID, 0, InvalidOid, 0);
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 3, "totalpages", false));
	dummy = makeVar(1, 4, INT8OID, 0, InvalidOid, 0);
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 4, "visiblepages", false));
	dummy = makeVar(1, 5, onerel->rd_rel->reltype, 0, InvalidOid, 0);
	step->scan.plan.targetlist = lappend(step->scan.plan.targetlist,
										 makeTargetEntry((Expr *) dummy, 5, "rows", false));

	/* Execute query on the data nodes */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	node = ExecInitRemoteQuery(step, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	result = ExecRemoteQuery((PlanState *) node);
	
	while (result != NULL && !TupIsNull(result))
	{
		slot_getallattrs(result);
		
		if (result->tts_isnull[0] == false)
		{
			samplenum += DatumGetFloat8(result->tts_values[0]);
		}

		if (result->tts_isnull[1] == false)
		{
			totalnum += DatumGetFloat8(result->tts_values[1]);
		}

		if (result->tts_isnull[2] == false)
		{
			deadnum += DatumGetFloat8(result->tts_values[2]);
		}

		if (result->tts_isnull[3] == false)
		{
			totalpagesnum += DatumGetInt64(result->tts_values[3]);
		}

		if (result->tts_isnull[4] == false)
		{
			visiblepagesnum += DatumGetInt64(result->tts_values[4]);
		}

		if (result->tts_isnull[5] == false && !enable_simple_analyze)
		{
			if (numrows < targrows)
			{
				HeapTupleHeader td = DatumGetHeapTupleHeader(result->tts_values[5]);
				HeapTupleData tmptup;

				/* Build a temporary HeapTuple control structure */
				tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
				ItemPointerSetInvalid(&(tmptup.t_self));
				tmptup.t_tableOid = InvalidOid;
				tmptup.t_data = td;

				/* Build a copy and return it */
				rows[numrows++] = heap_copytuple(&tmptup);
			}
			else
			{
				/*
					* t in Vitter's paper is the number of records already
					* processed.  If we need to compute a new S value, we
					* must use the not-yet-incremented value of samplerows as
					* t.
					*/
				if (rowstoskip < 0)
					rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

				if (rowstoskip <= 0)
				{
					/*
						* Found a suitable tuple, so save it, replacing one
						* old tuple at random
						*/
					int			k = (int) (targrows * sampler_random_fract(rstate.randstate));
					HeapTupleHeader td = DatumGetHeapTupleHeader(result->tts_values[5]);
					HeapTupleData tmptup;

					/* Build a temporary HeapTuple control structure */
					tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
					ItemPointerSetInvalid(&(tmptup.t_self));
					tmptup.t_tableOid = InvalidOid;
					tmptup.t_data = td;
					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = heap_copytuple(&tmptup);
				}

				rowstoskip -= 1;
			}
			samplerows += 1;
		}
		
		result = ExecRemoteQuery((PlanState *) node);
	}

	ExecEndRemoteQuery(node);
	
	*totalrows = totalnum;
	*totaldeadrows = deadnum;
	*totalpages = totalpagesnum;
	*visiblepages = visiblepagesnum;

	return numrows;
}

#endif

/*
 * Centralized mode, the partiton info of partitioned table has been
 * record in pgstat_count_heap_xxx, this function only record the normal
 * table changes.
 * Distributed mode, record the parent table and normal table in function
 * pgstat_count_remote_xxx.
 */
void
CentralizedRecordRelChangesInfo(EState *estate, CmdType operation)
{
	char relkind = estate->es_result_relations[0].ri_RelationDesc->rd_rel->relkind;
	bool relispartition = estate->es_result_relations[0].ri_RelationDesc->rd_rel->relispartition;

	Assert(ENABLE_AUTO_ANALYZE_IN_XACT());

	/* normal table */
	if (estate->es_num_result_relations == 1 &&
		(relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW) && !relispartition)
	{
		Relation rel = estate->es_result_relations[0].ri_RelationDesc;
		XactRelStats *relstat_info = rel->xact_relstat_info;

		if (relstat_info != NULL)
		{
			if (operation == CMD_INSERT)
				relstat_info->insert_tuples += estate->es_processed;
			else if (operation == CMD_UPDATE)
				relstat_info->update_tuples += estate->es_processed;
			else
				relstat_info->delete_tuples += estate->es_processed;

			if (relation_need_autoanalyze(rel->rd_id, false))
			{
				/* Invalidate cached plan if needed. For 'use' mode, we delay autoanalyze before use. */
				PlanCacheRelCallback(0, rel->rd_id);

				if (xact_autoanalyze_mode == ANALYZE_AFTER_DML || xact_autoanalyze_mode == ANALYZE_TWO_CONDITION)
					xact_do_autoanalyze(rel);
			}
		}
		return;
	}

	/* For update and delete operations, the estate records the partition information*/
	if (estate->es_num_result_relations > 1 
		&& (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW) && relispartition)
	{
		int i;
		for (i = 0; i < estate->es_num_result_relations; i++)
		{
			Relation rel = estate->es_result_relations[i].ri_RelationDesc;

			if (rel->xact_relstat_info != NULL)
			{
				if (relation_need_autoanalyze(rel->rd_id, false))
				{
					/* Invalidate cached plan if needed. For 'use' mode, we delay autoanalyze before use. */
					PlanCacheRelCallback(0, rel->rd_id);

					if ((xact_autoanalyze_mode == ANALYZE_AFTER_DML || xact_autoanalyze_mode == ANALYZE_TWO_CONDITION))
						xact_do_autoanalyze(rel);
				}
			}
		}
		return;
	}

	/* For insert, the estate records the main table information, need get all partition table */
	if (estate->es_num_result_relations == 1 && relkind == RELKIND_PARTITIONED_TABLE)
	{
		Relation rel = estate->es_result_relations[0].ri_RelationDesc;
		ListCell *cell;
		List *relid_list = NIL;

		find_all_children(rel->rd_id, &relid_list);

		foreach(cell, relid_list)
		{
			Oid child_relid = lfirst_oid(cell);
			Relation child_rel = relation_open(child_relid, AccessShareLock);

			if (child_rel->xact_relstat_info != NULL)
			{
				if (relation_need_autoanalyze(child_rel->rd_id, false))
				{
					/* Invalidate cached plan if needed. For 'use' mode, we delay autoanalyze before use. */
					PlanCacheRelCallback(0, rel->rd_id);

					if (xact_autoanalyze_mode == ANALYZE_AFTER_DML || xact_autoanalyze_mode == ANALYZE_TWO_CONDITION)
						xact_do_autoanalyze(child_rel);
				}
			}

			relation_close(child_rel, AccessShareLock);
		}
	}
}

void
DistributeRecordRelChangesInfo(EState *estate)
{
	if (estate->es_num_result_relations > 0 && estate->es_processed > 0)
	{
		Relation rel = estate->es_result_relations[0].ri_RelationDesc;
		bool need_close = false;

		switch (estate->es_plannedstmt->commandType)
		{
			case CMD_INSERT:
				/* One statement can insert into only one relation */
				pgstat_count_remote_insert(
						estate->es_result_relations[0].ri_RelationDesc,
						estate->es_processed);
				break;
			case CMD_UPDATE:
			case CMD_DELETE:
				{
					/*
					 * We can not determine here how many row were updated
					 * or delete in each table, so assume same number of
					 * affected row in each table.
					 * If resulting number of rows is 0 because of rounding,
					 * increment each counter at least on 1.
					 */
					bool 	update;

					update = (estate->es_plannedstmt->commandType == CMD_UPDATE);
					if (estate->es_num_result_relations > 1 &&
						estate->es_result_relations[0].ri_RelationDesc->rd_rel->relispartition)
					{
						Oid relid = get_partition_parent(estate->es_result_relations[0].ri_RelationDesc->rd_id);
						while (get_rel_relispartition(relid))
							relid = get_partition_parent(relid);

						rel = relation_open(relid, AccessShareLock);
						need_close = true;
						if (update)
							pgstat_count_remote_update(rel, estate->es_processed);
						else
							pgstat_count_remote_delete(rel, estate->es_processed);
					}
					else
					{
						if (update)
							pgstat_count_remote_update(rel, estate->es_processed);
						else
							pgstat_count_remote_delete(rel, estate->es_processed);
					}
				}
				break;
			default:
				/* nothing to count */
				break;
		}

		if (rel->xact_relstat_info != NULL &&
			ENABLE_AUTO_ANALYZE_IN_XACT() && (xact_autoanalyze_mode == ANALYZE_AFTER_DML || xact_autoanalyze_mode == ANALYZE_TWO_CONDITION) &&
			relation_need_autoanalyze(rel->rd_id, false))
		{
			PushActiveSnapshot(GetLocalTransactionSnapshot());
			xact_do_autoanalyze(rel);
			PopActiveSnapshot();
		}

		if (need_close)
			relation_close(rel, AccessShareLock);
	}
}
