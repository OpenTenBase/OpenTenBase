/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#include "postgres.h"
#include "stddef.h"

#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "common/ip.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/nodemgr.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/squeue.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/predicate_internals.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


PG_MODULE_MAGIC;

/*
 * Number of columns in the result of the dist_pg_stat_get_activity() function.
 * This must be kept in sync with the function's TuplDesc and SQL definition.
 */
#define PG_DIST_STAT_ACTIVITY_COLS 28

/* ----------
 * Total number of backends including auxiliary
 *
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.) MaxBackends
 * includes autovacuum workers and background workers as well.
 * ----------
 */
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)

#define UINT32_ACCESS_ONCE(var)		 ((uint32)(*((volatile uint32 *)&(var))))

/*
 * PgDistStatStatus extends the standard PgBackendStatus (see pgstat.c) with
 * additional, semi-persistent context about a backend's LAST executed
 * distributed query within the OpenTenBase cluster.
 *
 * Each entry in the shared DistStatArray corresponds to a backend process.
 * The information stored here (such as the process's 'role' in a plan) persists
 * until the backend executes a new top-level query. The primary key for
 * associating activities across the cluster is:
 * - 'global_query_id': unique identifier for each distributed query execution
 *
 * This hook-collected context, when joined with real-time process state
 * and global_query_id, allows the final view to provide a comprehensive
 * view of the current distributed activity. The global transaction ID (gxid)
 * is fetched separately from the PGPROC structure when needed.
 */
typedef struct PgDistStatStatus
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments changecount before modifying its entry, and again after
	 * finishing a modification. A would-be reader should note the value of
	 * changecount, copy the entry into private memory, then check
	 * changecount again. If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.
	 *
	 * This protocol requires memory barriers to ensure the intended order
	 * of operations.
	 */
	int			changecount;

	/*
	 * Fields below are persistent for the backend's lifecycle.
	 */
	bool		valid;				/* If false, this entry is not in use. */
	char		nodename[NAMEDATALEN];	/* Name of the node this backend runs on. */
	
	/*
	 * Fields below are transient and specific to a single distributed query.
	 * They are populated at the start of a top-level query (ExecutorStart hook).
	 */
	char		sessionid[NAMEDATALEN];	/* Global session ID for the cluster. */

	/* 
	 * Global Query ID (GID) tracking for distributed query correlation:
	 * - global_query_id_hash: hash value of GID for fast comparison and filtering in C code
	 * - global_query_id: complete string representation for display in views
	 * 
	 * The GID is generated on the coordinator and propagated to all involved nodes,
	 * serving as the core identifier for correlating query activities across the cluster.
	 */
	uint64		global_query_id_hash; 
	char		global_query_id[256];	/* Reserve sufficient space for GID string */

	char		role[NAMEDATALEN];		/* Role in the distributed plan (e.g., coordinator, datanode). */
	char		sqname[NAMEDATALEN];	/* Name of the Share Queue, if any. */
	bool		sqdone;				/* True if the Share Queue is finished. */
	char		planstate[4096];	/* Text representation of the plan fragment being executed. */
	
	/*
	 * portal name: The name of the current portal, typically set by an upper node.
	 * cursors: Space-separated list of cursor names found in RemoteSubplan nodes,
	 *          representing connections to lower-level nodes.
	 *
	 * Note: These fields, along with 'nodename' and 'global_query_id', can be used 
	 *       to reconstruct the execution tree of a distributed query.
	 */
	char		portal[NAMEDATALEN];
	char		cursors[NAMEDATALEN * 64];
} PgDistStatStatus;

/*
 * Working status for the get_dist_pg_locks_raw() SRF.
 *
 * This struct is used to hold the state of the function across multiple
 * calls. It strictly imitates the PG_Lock_Status struct used by the kernel's
 * pg_lock_status() function to ensure safe and correct iteration over
 * the lock data snapshots.
 */
typedef struct DistLockStatus
{
    LockData   *lockData;        /* A safe copy of the regular lock status data from lmgr. */
    int         currIdx;        /* Index of the current LockInstanceData being processed. */

    PredicateLockData *predLockData; /* A safe copy of the predicate lock status data. */
    int         predLockIdx;    /* Index of the current predicate lock being processed. */
} DistLockStatus;

static PgDistStatStatus *DistStatArray = NULL;
static PgDistStatStatus *MyDistStatEntry = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static pgstat_report_hook_type prev_pgstat_report_hook = NULL;
static PortalStart_hook_type prev_PortalStart = NULL;
static PortalDrop_hook_type prev_PortalDrop = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static bool pgds_enable_planstate; /* GUC variable: enable/disable planstate collection. */
static int pgds_nesting_level = 0; /* Current query nesting depth for GID generation control */
static char *pgds_gid_guc_string = NULL; /* Stores the Global Query ID string for cross-node propagation */

/*
 * Macros to load and store st_changecount with the memory barriers.
 *
 * increment_changecount_before() and
 * increment_changecount_after() need to be called before and after
 * entries are modified, respectively. This makes sure that st_changecount
 * is incremented around the modification.
 *
 * Also save_changecount_before() and save_changecount_after()
 * need to be called before and after entries are copied into private memory
 * respectively.
 */
#define increment_changecount_before(status)	\
	do {	\
		status->changecount++;	\
		pg_write_barrier(); \
	} while (0)

#define increment_changecount_after(status) \
	do {	\
		pg_write_barrier(); \
		status->changecount++;	\
		Assert((status->changecount & 1) == 0); \
	} while (0)

#define save_changecount_before(status, save_changecount)	\
	do {	\
		save_changecount = status->changecount; \
		pg_read_barrier();	\
	} while (0)

#define save_changecount_after(status, save_changecount)	\
	do {	\
		pg_read_barrier();	\
		save_changecount = status->changecount; \
	} while (0)

Datum dist_pg_stat_get_activity(PG_FUNCTION_ARGS);

void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(dist_pg_stat_get_activity);
PG_FUNCTION_INFO_V1(get_dist_pg_locks);

static ParamListInfo
EvaluateSessionIDParam(const char *sessionid)
{
	int num_params = 1;
	ParamListInfo paramLI = (ParamListInfo)
		palloc0(offsetof(ParamListInfoData, params) +
		        num_params * sizeof(ParamExternData));
	
	ParamExternData *prm;
	
	/* we have static list of params, so no hooks needed */
	paramLI->paramFetch = NULL;
	paramLI->paramFetchArg = NULL;
	paramLI->parserSetup = NULL;
	paramLI->parserSetupArg = NULL;
	paramLI->numParams = num_params;
	paramLI->paramMask = NULL;
	
	prm = &paramLI->params[0];
	prm->ptype = TEXTOID;
	prm->pflags = PARAM_FLAG_CONST;
	if (sessionid != NULL)
	{
		prm->value = CStringGetTextDatum(sessionid);
		prm->isnull = false;
	}
	else
	{
		prm->isnull = true;
	}
	
	return paramLI;
}

/*
 * walk through planstate tree and gets cursors it contains in
 * RemoteSubplan node, formed as a single string delimited each
 * cursor by a space (one cursor stands for a RemoteSubplan node).
 */
static bool
cursorCollectWalker(PlanState *planstate, StringInfo str)
{
	if (IsA(planstate, RemoteSubplanState))
	{
		RemoteSubplan *plan = (RemoteSubplan *) planstate->plan;
		if (plan->cursor != NULL)
		{
			appendStringInfoString(str, plan->cursor);
			if (plan->unique)
				appendStringInfo(str, "_"INT64_FORMAT, plan->unique);
			/* add a space as delimiter */
			appendStringInfoString(str, " ");
		}
	}
	
	return planstate_tree_walker(planstate, cursorCollectWalker, str);
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
static void
CreateSharedDistStatus(void)
{
	Size		size;
	bool        found;
	
	/* Create or attach to the shared array */
	size = mul_size(sizeof(PgDistStatStatus), NumBackendStatSlots);
	DistStatArray = (PgDistStatStatus *)
		ShmemInitStruct("Distributed Status Array", size, &found);
	
	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(DistStatArray, 0, size);
	}
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 */
static void
pgds_shutdown_hook(int code, Datum arg)
{
	volatile PgDistStatStatus *entry = MyDistStatEntry;
	
	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	increment_changecount_before(entry);
	
	entry->valid = false;	/* mark invalid to hide this entry */

	increment_changecount_after(entry);
}

/* ----------
 * pgds_entry_initialize() -
 *
 *	Initialize my cluster status entry, and set up our on-proc-exit hook.
 *	as an extension but we don't have hook during process startup, so called
 *	each time the backend try to report something.
 * ----------
 */
static void
pgds_entry_initialize(void)
{
	/* already initialized */
	if (MyDistStatEntry != NULL)
		return;

	if (DistStatArray == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("shared memory for pg_dist_stat_view is not prepared"),
			        errhint("maybe you need to set shared_preload_libraries in postgresql.conf file")));
		return;
	}
	
	/* Initialize MyCSEntry */
	if (MyBackendId != InvalidBackendId)
	{
		Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);
		MyDistStatEntry = &DistStatArray[MyBackendId - 1];
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);
		
		/*
		 * Assign the MyDistStatEntry for an auxiliary process. Since it doesn't
		 * have a BackendId, the slot is statically allocated based on the
		 * auxiliary process type (MyAuxProcType).  Backends use slots indexed
		 * in the range from 1 to MaxBackends (inclusive), so we use
		 * MaxBackends + AuxBackendType + 1 as the index of the slot for an
		 * auxiliary process.
		 */
		MyDistStatEntry = &DistStatArray[MaxBackends + MyAuxProcType];
	}
	
	/*
	 * Ensure the entry starts in a clean state. This is crucial for backends
	 * that are recycled from a connection pool, preventing stale data from a
	 * previous query from persisting.
	 */

	/* Reset GID information during initialization */
    MyDistStatEntry->global_query_id_hash = 0;
    MyDistStatEntry->global_query_id[0] = '\0';

	/* also set nodename here, it won't change anyway */
	memcpy(MyDistStatEntry->nodename, PGXCNodeName, strlen(PGXCNodeName) + 1);
	
	/* Set up a process-exit hook to clean up */
	on_shmem_exit(pgds_shutdown_hook, 0);
}

/* ----------
 * pgds_report_common
 * 
 *  Report common fileds of cluster backend status activity,
 *  called by pgds_report_query_activity and pgds_report_activity.
 * ----------
 */
static void
pgds_report_common(PgDistStatStatus *entry)
{
	strncpy((char *) entry->sessionid, PGXCSessionId, NAMEDATALEN);
	
	entry->sqdone = false;
	entry->valid = true;
}

/* ----------
 * pgds_report_role
 * 
 *  Report role, sqname, also if this backend become consumer, remove
 *  previous planstate and cursor.
 * ----------
 */
static void
pgds_report_role(PgDistStatStatus *entry, QueryDesc *desc)
{
	/* fields need queryDesc */
	if (IS_PGXC_DATANODE)
	{
		if (desc != NULL && desc->squeue)
		{
			strncpy((char *) entry->sqname, SqueueName(desc->squeue), NAMEDATALEN);
			if (IsSqueueProducer())
			{
				strncpy((char *) entry->role, "producer", NAMEDATALEN);
			}
			else if (IsSqueueConsumer())
			{
				strncpy((char *) entry->role, "consumer", NAMEDATALEN);
				/* consumer does not know of planstate */
				entry->planstate[0] = '\0';
				entry->cursors[0] = '\0';
			}
			else
			{
				/* do not support */
				entry->role[0] = '\0';
			}
		}
		else if (IsParallelWorker())
		{
			strncpy((char *) entry->role, "parallel worker", NAMEDATALEN);
		}
		else
		{
			strncpy((char *) entry->role, "datanode", NAMEDATALEN);
		}
	}
	else if (IS_PGXC_COORDINATOR)
	{
		strncpy((char *) entry->role, "coordinator", NAMEDATALEN);
	}
	else
	{
		/* do not support */
		entry->role[0] = '\0';
	}
}

/* ----------
 * pgds_report_query_activity
 *
 *  Do nothing but set common field, just enable this cluster entry
 *  to make it visible in the same time as pg_stat_activity. Hooked
 *  in pgstat_report_activity, args are redundant.
 * 	
 */
static void
pgds_report_query_activity(BackendState state, const char *cmd_str)
{
	volatile PgDistStatStatus *entry;
	
	pgds_entry_initialize();
	entry = MyDistStatEntry;
	pgds_report_common((PgDistStatStatus *) entry);

	if (prev_pgstat_report_hook)
		prev_pgstat_report_hook(state, cmd_str);
}

/*
 * pgds_report_executor_activity
 *
 * ExecutorStart hook to capture and record all transient, query-specific
 * distributed status information into the backend's shared memory entry.
 *
 * This function is the primary mechanism for our distributed tracking. It is
 * responsible for:
 *  1. Identifying the start of a top-level distributed query using a
 *     nesting-level counter.
 *  2. Generating and propagating Global Query ID (GID) on coordinator nodes.
 *  3. Recording context (role, planstate, cursors, GID) into the shared memory
 *     slot (PgDistStatStatus) for cross-node query correlation.
 *
 * This function guarantees that all context for a distributed query is
 * captured atomically at the beginning of its execution, with GID serving
 * as the primary correlation key across the cluster.
 */
static void
pgds_report_executor_activity(QueryDesc *desc, int eflags)
{
	volatile PgDistStatStatus *entry;
	StringInfo planstate_str = NULL;
	StringInfo cursors = NULL;
	MemoryContext oldcxt;
	char gid_string[256];
	char *gid_to_write;
	
	if (prev_ExecutorStart)
		prev_ExecutorStart(desc, eflags);
	else
		standard_ExecutorStart(desc, eflags);

	pgds_nesting_level++;	/* Increment nesting level */

	if (!desc)
		return;

	if (pgds_nesting_level == 1)	/* Only top-level queries generate new GID, subqueries do not */
    {
        if (IS_PGXC_COORDINATOR && (pgds_gid_guc_string == NULL || pgds_gid_guc_string[0] == '\0'))
        {
            snprintf(gid_string, sizeof(gid_string), "%s-%d-%lu",
                     PGXCNodeName, MyProcPid, (unsigned long)GetCurrentTimestamp());

            (void)set_config_option("pg_dist_stat_views.global_query_id", gid_string,
                                    PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SET, true, 0, false);
        }

		/*
		* On all nodes (CN and DN), atomically write all transient status
		* information to our shared memory slot for GXID-based correlation.
		*/
		pgds_entry_initialize();
		entry = MyDistStatEntry;
		gid_to_write = (char *)pgds_gid_guc_string;
		/*
		* On all nodes (CN and DN), atomically write all transient status
		* information to our shared memory slot.
		*/
		increment_changecount_before(entry);

		if (gid_to_write && gid_to_write[0] != '\0')
		{	
			snprintf((char *)entry->global_query_id, 
					sizeof(entry->global_query_id), 
					"%s", 
					gid_to_write);
			entry->global_query_id_hash = string_hash(gid_to_write, strlen(gid_to_write));

		}

		pgds_report_common((PgDistStatStatus *) entry);
		pgds_report_role((PgDistStatStatus *)entry, desc);

		/* Collect and write planstate and cursors if applicable. */
		if (desc->already_executed)
		{
			entry->sqdone = true;
		}
		else if (desc->planstate != NULL)
		{
			oldcxt = MemoryContextSwitchTo(desc->estate->es_query_cxt);
			cursors = makeStringInfo();
			cursorCollectWalker(desc->planstate, cursors);
			if (cursors->len > 0)
				snprintf((char *)entry->cursors, sizeof(entry->cursors), "%s", cursors->data);
			if (pgds_enable_planstate)
			{
				ExplainState es;
				planstate_str = makeStringInfo();
				
				memset(&es, 0, sizeof(es));
				es.str = planstate_str;
				es.costs = false;
				es.skip_remote_query = true;
				
				ExplainBeginOutput(&es);
				ExplainPrintPlan(&es, desc);
				ExplainEndOutput(&es);
				
				if (planstate_str->len > 0)
					snprintf((char *)entry->planstate, sizeof(entry->planstate), "%s", planstate_str->data);
			}
			else
			{
				snprintf((char *)entry->planstate, sizeof(entry->planstate), "disabled");
			}
			pfree(cursors->data);
			pfree(cursors);
			if (planstate_str)
			{
				pfree(planstate_str->data);
				pfree(planstate_str);
			}
			MemoryContextSwitchTo(oldcxt);
		}
		
		increment_changecount_after(entry);
	}

}

/*
 * pgds_executor_end_hook
 *
 * ExecutorEnd hook to clean up the transient, query-specific distributed
 * status information from the backend's shared memory entry.
 *
 * This function acts as the counterpart to pgds_report_executor_activity().
 * It is responsible for:
 *  1. Decrementing the nesting-level counter.
 *  2. When the top-level query finishes (nesting level returns to 0),
 *     it clears all transient fields in the PgDistStatStatus entry.
 *
 * This cleanup is crucial to prevent stale data from a completed query
 * being associated with a new query that reuses the same backend process.
 */
static void
pgds_executor_end_hook(QueryDesc *queryDesc)
{
	volatile PgDistStatStatus *v_entry;
	PgDistStatStatus *entry;
	pgds_nesting_level--;

	if (pgds_nesting_level == 0)
    {
        /* --- A. 在CN节点上，清理GUC传播信道 --- */
        if (IS_PGXC_COORDINATOR)
        {
            if (pgds_gid_guc_string && pgds_gid_guc_string[0] != '\0')
            {
                (void)set_config_option("pg_dist_stat_views.global_query_id", "",
                                        PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SET, true, 0, false);
            }
        }

        if (MyDistStatEntry)
        {
            v_entry = MyDistStatEntry;
			entry = (PgDistStatStatus *)v_entry;

            increment_changecount_before(v_entry);
            MemSet(&entry->global_query_id_hash, 0, 
                   sizeof(PgDistStatStatus) - offsetof(PgDistStatStatus, global_query_id_hash));
            
            increment_changecount_after(v_entry);

        }
	}

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

/*
 * pgds_portal_hook
 *
 * Generic hook function for PortalStart_hook and PortalDrop_hook. It is
 * responsible for managing portal-specific information in our shared
 * memory entry.
 *
 * Its primary and ONLY responsibility is to set the portal name on portal
 * creation (is_drop=false) and clear it on portal destruction (is_drop=true).
 *
 * It does NOT handle any other transient, query-specific state, as that
 * is managed exclusively by the ExecutorStart/ExecutorEnd hooks to ensure
 * logical consistency.
 */
static void
pgds_report_activity(Portal portal)
{
	volatile PgDistStatStatus *entry;
	QueryDesc *desc = portal->queryDesc;
	
	pgds_entry_initialize();
	entry = MyDistStatEntry;
	
	/* if query already done, just report sqdone and return */
	if (desc != NULL && desc->already_executed)
	{
		increment_changecount_before(entry);
		entry->sqdone = true;
		increment_changecount_after(entry);
		return;
	}
	
	increment_changecount_before(entry);

	pgds_report_common((PgDistStatStatus *) entry);
	pgds_report_role((PgDistStatStatus *) entry, desc);
    strncpy((char *)entry->portal, portal->name, NAMEDATALEN);
    
    increment_changecount_after(entry);
}

/* ----------
 * pgstat_fetch_stat_local_csentry
 * 
 *  Given a backend id, find particular cluster status entry, copy valid
 *  entry into local memory, loop around changecount to ensure concurrency.
 * ----------
 */
static PgDistStatStatus *
pgstat_fetch_stat_local_dsentry(int beid)
{
	PgDistStatStatus *dsentry;
	PgDistStatStatus *local = palloc(sizeof(PgDistStatStatus));
	local->valid = false;
	
	if (DistStatArray == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("shared memory for pg_stat_cluster_activity is not prepared"),
			        errhint("maybe you need to set shared_preload_libraries in postgresql.conf")));
		return NULL;
	}
	
	if (beid < 1)
		return NULL;
	
	dsentry = &DistStatArray[beid - 1];
	
	for (;;)
	{
		int			before_changecount;
		int			after_changecount;
		
		save_changecount_before(dsentry, before_changecount);
		if (dsentry->valid)
		{
			memcpy(local, dsentry, sizeof(PgDistStatStatus));
		}
		save_changecount_after(dsentry, after_changecount);
		if (before_changecount == after_changecount &&
		    (before_changecount & 1) == 0)
			break;
		
		/* Make sure we can break out of loop if stuck... */
		CHECK_FOR_INTERRUPTS();
	}
	
	return local;
}

/* ----------
 * dist_pg_get_remote_activity
 * 
 *  Execute dist_pg_stat_get_activity query remotely and save
 *  results in the given tuplestore.
 */
static void
dist_pg_get_remote_activity(const char *sessionid, bool coordonly, Tuplestorestate *tupstore, TupleDesc tupdesc)
{
#define QUERY_LEN 1024
	char    query[QUERY_LEN];
	EState              *estate;
	MemoryContext		oldcontext;
	RemoteQuery 		*plan;
	RemoteQueryState    *pstate;
	TupleTableSlot		*result = NULL;
	
	/*
	* Here we call dist_pg_stat_get_activity remotely with args:
	* coordonly = false, localonly = true, to prevent recursive calls on remote nodes.
	*/
	snprintf(query, QUERY_LEN, "select * from dist_pg_stat_get_activity($1, false, true)");
	
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	/*
	 * set exec_nodes to NULL makes ExecRemoteQuery send query to all nodes
	 * (local CN nodes won't recieved query again).
	 */
	plan->exec_nodes = NULL;
	plan->exec_type = EXEC_ON_ALL_NODES;
	plan->sql_statement = (char *) query;
	plan->force_autocommit = false;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_nodes->missing_ok = true;
	
	if (coordonly)
	{
		plan->exec_nodes->nodeList = GetAllCoordNodes();
		plan->exec_type = EXEC_ON_COORDS;
	}
	
	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = EvaluateSessionIDParam(sessionid);
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	ExecAssignResultType((PlanState *) pstate, tupdesc);
	MemoryContextSwitchTo(oldcontext);
	
	result = ExecRemoteQuery((PlanState *) pstate);
	
	while (result != NULL && !TupIsNull(result))
	{
		slot_getallattrs(result);
		
		tuplestore_puttupleslot(tupstore, result);
		result = ExecRemoteQuery((PlanState *) pstate);
	}
	
	ExecEndRemoteQuery(pstate);
	FreeExecutorState(estate);
}

/*
 * dist_pg_stat_get_activity
 *
 * SQL-callable SRF (Set-Returning Function) that serves as the main entry
 * point for the distributed activity view.
 *
 * This function implements a "materialized" SRF pattern. It collects activity
 * information from all relevant nodes (both remote and local), materializes
 * the entire result set into a Tuplestore in a single execution, and then
 * returns the Tuplestore to the executor.
 *
 * The core logic involves two stages:
 *  1. Remote data collection: If executed on a coordinator and not in
 *     'localonly' mode, it dispatches a recursive call to all other nodes.
 *  2. Local data collection: It then iterates through the local backends,
 *     fusing several data sources for each process:
 *      a) Real-time status from the pgstat system (state, query, etc.).
 *      b) The Global Transaction ID (gxid) from the PGPROC structure.
 *      c) The semi-persistent distributed context (role, planstate, etc.)
 *         from our custom shared memory array (DistStatArray).
 *
 * Arguments:
 *  sessionid (text, optional): Filters the result to a specific global session ID.
 *  coordonly (bool, optional): If true, dispatches remote requests only to
 *                              other coordinator nodes.
 *  localonly (bool, optional): If true, skips the remote data collection stage.
 *                              Used by the recursive calls to prevent
 *                              infinite loops.
 */
Datum
dist_pg_stat_get_activity(PG_FUNCTION_ARGS)
{
	int              num_backends = pgstat_fetch_stat_numbackends();
	int			     curr_backend;
	bool             with_sessionid = !PG_ARGISNULL(0);
	bool             coordonly = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	bool             localonly = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	const char      *sessionid = with_sessionid ? text_to_cstring(PG_GETARG_TEXT_P(0)) : NULL;
	ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	     tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext    per_query_ctx;
	MemoryContext    oldcontext;
	
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	
	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	
	/* switch to query's memory context to save results during execution */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	
	MemoryContextSwitchTo(oldcontext);
	
	/* dispatch query to remote if needed */
	if (!localonly && IS_PGXC_COORDINATOR)
		dist_pg_get_remote_activity(sessionid, coordonly, tupstore, tupdesc);
	
	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		/* for each row */
		Datum		values[PG_DIST_STAT_ACTIVITY_COLS];
		bool		nulls[PG_DIST_STAT_ACTIVITY_COLS];
		
		/* same as pg_stat_get_activity */
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		PGPROC	   *proc;
		const char *wait_event_type = NULL;
		const char *wait_event = NULL;
		
		/* cluster information */
		PgDistStatStatus *local_dsentry;
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		
		/* Get the next one in the list */
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		local_dsentry = pgstat_fetch_stat_local_dsentry(local_beentry->backend_id);
		if (!local_beentry || !local_dsentry)
		{
			int			i;
			
			/* Ignore missing entries if looking for specific sessionid */
			if (with_sessionid)
				continue;
			
			for (i = 0; i < lengthof(nulls); i++)
				nulls[i] = true;
			
			nulls[13] = false;
			values[13] = CStringGetTextDatum("<backend information not available>");
			
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			continue;
		}
		
		if (!local_dsentry->valid)
			continue;
		
		beentry = &local_beentry->backendStatus;
		/* If looking for specific sessionid, ignore all the others */
		if (with_sessionid && strcmp(sessionid, local_dsentry->sessionid) != 0)
			continue;
		
		/* Values available to all callers */
		values[0] = CStringGetTextDatum(local_dsentry->sessionid);
		values[1] = Int32GetDatum(beentry->st_procpid);
		
		if (beentry->st_databaseid != InvalidOid)
		{
			char *dbname = get_database_name(beentry->st_databaseid);
			if (dbname != NULL)
				values[7] = CStringGetTextDatum(dbname);
			else
				nulls[7] = true;
		}
		else
			nulls[7] = true;
		
		if (beentry->st_userid != InvalidOid)
		{
			char *usename = GetUserNameFromId(beentry->st_userid, true);
			if (usename != NULL)
				values[8] = CStringGetTextDatum(usename);
			else
				nulls[8] = true;
		}
		else
			nulls[8] = true;
		
		/* Values only available to owner or superuser or pg_read_all_stats */
		if (has_privs_of_role(GetUserId(), beentry->st_userid) ||
		    is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
		{
			SockAddr	zero_clientaddr;
			
			/* A zeroed client addr means we don't know */
			memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
			if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr,
			           sizeof(zero_clientaddr)) == 0)
			{
				nulls[2] = true;
				nulls[3] = true;
				nulls[4] = true;
			}
			else
			{
				if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
				    || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
					)
				{
					char		remote_host[NI_MAXHOST];
					char		remote_port[NI_MAXSERV];
					int			ret;
					
					remote_host[0] = '\0';
					remote_port[0] = '\0';
					ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
					                         beentry->st_clientaddr.salen,
					                         remote_host, sizeof(remote_host),
					                         remote_port, sizeof(remote_port),
					                         NI_NUMERICHOST | NI_NUMERICSERV);
					if (ret == 0)
					{
						clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
						values[2] = DirectFunctionCall1(inet_in,
						                                CStringGetDatum(remote_host));
						if (beentry->st_clienthostname &&
						    beentry->st_clienthostname[0])
							values[3] = CStringGetTextDatum(beentry->st_clienthostname);
						else
							nulls[3] = true;
						values[4] = Int32GetDatum(atoi(remote_port));
					}
					else
					{
						nulls[2] = true;
						nulls[3] = true;
						nulls[4] = true;
					}
				}
				else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX)
				{
					/*
					 * Unix sockets always reports NULL for host and -1 for
					 * port, so it's possible to tell the difference to
					 * connections we have no permissions to view, or with
					 * errors.
					 */
					nulls[2] = true;
					nulls[3] = true;
					values[4] = DatumGetInt32(-1);
				}
				else
				{
					/* Unknown address type, should never happen */
					nulls[2] = true;
					nulls[3] = true;
					nulls[4] = true;
				}
			}
			
			values[5] = CStringGetTextDatum(local_dsentry->nodename);
			values[6] = CStringGetTextDatum(local_dsentry->role);
			
			proc = BackendPidGetProc(beentry->st_procpid);
			if (proc != NULL)
			{
				uint32		raw_wait_event;
				
				raw_wait_event = UINT32_ACCESS_ONCE(proc->wait_event_info);
				wait_event_type = pgstat_get_wait_event_type(raw_wait_event);
				wait_event = pgstat_get_wait_event(raw_wait_event);
			}
			else if (beentry->st_backendType != B_BACKEND)
			{
				/*
				 * For an auxiliary process, retrieve process info from
				 * AuxiliaryProcs stored in shared-memory.
				 */
				proc = AuxiliaryPidGetProc(beentry->st_procpid);
				
				if (proc != NULL)
				{
					uint32		raw_wait_event;
					
					raw_wait_event =
						UINT32_ACCESS_ONCE(proc->wait_event_info);
					wait_event_type =
						pgstat_get_wait_event_type(raw_wait_event);
					wait_event = pgstat_get_wait_event(raw_wait_event);
				}
			}
			
			if (wait_event_type)
				values[9] = CStringGetTextDatum(wait_event_type);
			else
				nulls[9] = true;
			
			if (wait_event)
				values[10] = CStringGetTextDatum(wait_event);
			else
				nulls[10] = true;
			
			switch (beentry->st_state)
			{
				case STATE_IDLE:
					values[11] = CStringGetTextDatum("idle");
					break;
				case STATE_RUNNING:
					values[11] = CStringGetTextDatum("active");
					break;
				case STATE_IDLEINTRANSACTION:
					values[11] = CStringGetTextDatum("idle in transaction");
					break;
				case STATE_FASTPATH:
					values[11] = CStringGetTextDatum("fastpath function call");
					break;
				case STATE_IDLEINTRANSACTION_ABORTED:
					values[11] = CStringGetTextDatum("idle in transaction (aborted)");
					break;
				case STATE_DISABLED:
					values[11] = CStringGetTextDatum("disabled");
					break;
				case STATE_UNDEFINED:
					nulls[11] = true;
					break;
			}
			
			values[12] = CStringGetTextDatum(local_dsentry->sqname);
			values[13] = BoolGetDatum(local_dsentry->sqdone);
			values[14] = CStringGetTextDatum(beentry->st_activity);
			values[15] = CStringGetTextDatum(local_dsentry->planstate);
			values[16] = CStringGetTextDatum(local_dsentry->portal);
			values[17] = CStringGetTextDatum(local_dsentry->cursors);
			
			if (beentry->st_proc_start_timestamp != 0)
				values[18] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
			else
				nulls[18] = true;
			
			if (beentry->st_xact_start_timestamp != 0)
				values[19] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
			else
				nulls[19] = true;
			
			if (beentry->st_activity_start_timestamp != 0)
				values[20] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
			else
				nulls[20] = true;
			
			if (beentry->st_state_start_timestamp != 0)
				values[21] = TimestampTzGetDatum(beentry->st_state_start_timestamp);
			else
				nulls[21] = true;

			if (beentry->st_appname)
				values[22] = CStringGetTextDatum(beentry->st_appname);
			else
				nulls[22] = true;
			
			if (TransactionIdIsValid(local_beentry->backend_xid))
				values[23] = TransactionIdGetDatum(local_beentry->backend_xid);
			else
				nulls[23] = true;

			if (TransactionIdIsValid(local_beentry->backend_xmin))
				values[24] = TransactionIdGetDatum(local_beentry->backend_xmin);
			else
				nulls[24] = true;

			if (beentry->st_backendType)
				values[25] = CStringGetTextDatum(pgstat_get_backend_desc(beentry->st_backendType));
			else
				nulls[25] = true;

			if (proc->hasGlobalXid && proc->globalXid[0] != '\0')
				values[26] = CStringGetTextDatum(proc->globalXid);
			else
				nulls[26] = true;
			if (local_dsentry->global_query_id[0] != '\0')
				values[27] = CStringGetTextDatum(local_dsentry->global_query_id);
			else
				nulls[27] = true;
		}
		else
		{
			values[14] = CStringGetTextDatum("<insufficient privilege>");
			nulls[2] = true;
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = true;
			nulls[6] = true;
			nulls[9] = true;
			nulls[10] = true;
			nulls[11] = true;
			nulls[12] = true;
			nulls[13] = true;
			nulls[15] = true;
			nulls[16] = true;
			nulls[17] = true;
			nulls[18] = true;
			nulls[19] = true;
			nulls[20] = true;
			nulls[21] = true;
			nulls[22] = true;
			nulls[23] = true;
			nulls[24] = true;
			nulls[25] = true;
			nulls[26] = true;
			nulls[27] = true;
		}
		
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	
	return (Datum) 0;
}

/*
 * ===================================================================
 *                    LOCKS INFORMATION FUNCTIONS
 * ===================================================================
 */

/*
 * Helper function to format a VirtualTransactionId into a text Datum.
 * Copied from lockfuncs.c for self-containment.
 */
 static Datum
VXIDGetDatum(BackendId bid, LocalTransactionId lxid)
{
    char vxidstr[32];
    snprintf(vxidstr, sizeof(vxidstr), "%d/%u", bid, lxid);
    return CStringGetTextDatum(vxidstr);
}

/*
 * dist_pg_get_local_locks
 *
 * Collects all regular and predicate lock information on the local node,
 * enhances it with distributed context (GXID), identifies the blocking
 * process for any waiting locks, and populates the results into a Tuplestore.
 *
 * This function is a plain C function, not an SRF. It reads the lock manager
 * snapshots safely and performs analysis to provide a complete picture of
 * the local lock status.
 */

static void
dist_pg_get_local_locks(Tuplestorestate *tupstore, TupleDesc tupdesc)
{
    LockData   *lockData;
    PredicateLockData *predLockData;
    int         i;
	static const char *const PredicateLockTagTypeNames[] = {"relation", "page", "tuple"};

#define NUM_DIST_LOCKS_RAW_COLS 19
    Datum       values[NUM_DIST_LOCKS_RAW_COLS];
    bool        nulls[NUM_DIST_LOCKS_RAW_COLS];

    const char *const LockTagTypeNames[] = {
        "relation", "extend", "page", "tuple", "transactionid",
        "virtualxid", "speculative token", "object",
#ifdef _SHARDING_
        "shard",
#endif
        "userlock", "advisory"
    };

    /*
	 * Stage 1: Process Regular Locks
	 */
    lockData = GetLockStatusData();
    
    for (i = 0; i < lockData->nelements; i++)
    {
        LockInstanceData *instance = &(lockData->locks[i]);
        PGPROC     *proc = BackendPidGetProc(instance->pid);
        uint32      holdMask = instance->holdMask;
        LOCKMODE    mode;
        
        if (!proc) 
            continue;

        for (mode = 0; mode < MAX_LOCKMODES; mode++)
        {
            if (holdMask & LOCKBIT_ON(mode))
            {
                MemSet(values, 0, sizeof(values));
                MemSet(nulls, false, sizeof(nulls));
                
                values[0] = CStringGetTextDatum(PGXCNodeName);
                if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
                    values[1] = CStringGetTextDatum(LockTagTypeNames[instance->locktag.locktag_type]);
                else
                    values[1] = CStringGetTextDatum("unknown");
                switch ((LockTagType) instance->locktag.locktag_type)
                {
                    case LOCKTAG_RELATION:
                    case LOCKTAG_RELATION_EXTEND:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        nulls[4]=nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_PAGE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TUPLE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        values[5] = UInt16GetDatum(instance->locktag.locktag_field4);
                        nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TRANSACTION:
                        values[7] = TransactionIdGetDatum(instance->locktag.locktag_field1);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_VIRTUALTRANSACTION:
                        values[6] = VXIDGetDatum(instance->locktag.locktag_field1, instance->locktag.locktag_field2);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_OBJECT:
                    case LOCKTAG_USERLOCK:
                    case LOCKTAG_ADVISORY:
                    default:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[8] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[9] = ObjectIdGetDatum(instance->locktag.locktag_field3);
                        values[10] = Int16GetDatum(instance->locktag.locktag_field4);
                        nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[7]=true;
                        break;
                }
                values[11] = VXIDGetDatum(instance->backend, instance->lxid);
                values[12] = Int32GetDatum(instance->pid);
                values[13] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, mode));
                values[14] = BoolGetDatum(true);
                values[15] = BoolGetDatum(instance->fastpath);
                if (proc->hasGlobalXid && proc->globalXid[0] != '\0')
                    values[16] = CStringGetTextDatum(proc->globalXid);
                else
                    nulls[16] = true;
				nulls[17] = true;
                nulls[18] = true;
                tuplestore_putvalues(tupstore, tupdesc, values, nulls);
            }
        }
        
    	/* --- 1b. Report the WAITING lock mode for this instance, if any --- */
        if (instance->waitLockMode != NoLock)
        {
			PGPROC *blocker_proc = NULL;

            if (proc->waitStatus == STATUS_WAITING && proc->waitLock != NULL)
            {
                LOCK *lock_obj = proc->waitLock;
                const LockMethod lockMethodTable = GetLockTagsMethodTable(&(lock_obj->tag));
                
                for (int j = 0; j < lockData->nelements; j++)
                {
                    LockInstanceData *holder_instance = &(lockData->locks[j]);
                    
                    if (memcmp(&instance->locktag, &holder_instance->locktag, sizeof(LOCKTAG)) == 0)
                    {
                        if ((holder_instance->holdMask & lockMethodTable->conflictTab[instance->waitLockMode]) != 0)
                        {
                            blocker_proc = BackendPidGetProc(holder_instance->pid);
                            if (blocker_proc)
                                break;
                        }
                    }
                }
            }

            MemSet(values, 0, sizeof(values));
            MemSet(nulls, false, sizeof(nulls));
            
            values[0] = CStringGetTextDatum(PGXCNodeName);
            if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
                values[1] = CStringGetTextDatum(LockTagTypeNames[instance->locktag.locktag_type]);
            else
                values[1] = CStringGetTextDatum("unknown");
            switch ((LockTagType) instance->locktag.locktag_type)
            {
                    case LOCKTAG_RELATION:
                    case LOCKTAG_RELATION_EXTEND:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        nulls[4]=nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_PAGE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TUPLE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        values[5] = UInt16GetDatum(instance->locktag.locktag_field4);
                        nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TRANSACTION:
                        values[7] = TransactionIdGetDatum(instance->locktag.locktag_field1);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_VIRTUALTRANSACTION:
                        values[6] = VXIDGetDatum(instance->locktag.locktag_field1, instance->locktag.locktag_field2);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_OBJECT:
                    case LOCKTAG_USERLOCK:
                    case LOCKTAG_ADVISORY:
                    default:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[8] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[9] = ObjectIdGetDatum(instance->locktag.locktag_field3);
                        values[10] = Int16GetDatum(instance->locktag.locktag_field4);
                        nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[7]=true;
                        break;
            }
            values[11] = VXIDGetDatum(instance->backend, instance->lxid);
            values[12] = Int32GetDatum(instance->pid);
            values[13] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, instance->waitLockMode));
            values[14] = BoolGetDatum(false);
            values[15] = BoolGetDatum(instance->fastpath);
            if (proc->hasGlobalXid && proc->globalXid[0] != '\0')
                values[16] = CStringGetTextDatum(proc->globalXid);
            else
                nulls[16] = true;

			if (blocker_proc)
            {
                values[17] = Int32GetDatum(blocker_proc->pid);
                if (blocker_proc->hasGlobalXid && blocker_proc->globalXid[0] != '\0')
                    values[18] = CStringGetTextDatum(blocker_proc->globalXid);
                else
                    nulls[18] = true;
            }
            else
            {
                nulls[17] = true;
                nulls[18] = true;
            }
            tuplestore_putvalues(tupstore, tupdesc, values, nulls);
        }
    }

    /*
	 * Stage 2: Process Predicate Locks
	 */
    predLockData = GetPredicateLockStatusData();

    for (i = 0; i < predLockData->nelements; i++)
    {
        PredicateLockTargetType lockType;
        PREDICATELOCKTARGETTAG *predTag = &(predLockData->locktags[i]);
        SERIALIZABLEXACT *xact = &(predLockData->xacts[i]);
        PGPROC *proc;

        proc = BackendPidGetProc(xact->pid);

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));

        values[0] = CStringGetTextDatum(PGXCNodeName);
        
        lockType = GET_PREDICATELOCKTARGETTAG_TYPE(*predTag);
        values[1] = CStringGetTextDatum(PredicateLockTagTypeNames[lockType]);

        values[2] = GET_PREDICATELOCKTARGETTAG_DB(*predTag);
        values[3] = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag);

        if (lockType == PREDLOCKTAG_TUPLE)
            values[5] = GET_PREDICATELOCKTARGETTAG_OFFSET(*predTag);
        else
            nulls[5] = true;

        if (lockType == PREDLOCKTAG_TUPLE || lockType == PREDLOCKTAG_PAGE)
            values[4] = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);
        else
            nulls[4] = true;
            
        nulls[6] = true;
        nulls[7] = true;
        nulls[8] = true;
        nulls[9] = true;
        nulls[10] = true;

        values[11] = VXIDGetDatum(xact->vxid.backendId, xact->vxid.localTransactionId);
        if (xact->pid != 0)
            values[12] = Int32GetDatum(xact->pid);
        else
            nulls[12] = true;

        values[13] = CStringGetTextDatum("SIReadLock");
        values[14] = BoolGetDatum(true);
        values[15] = BoolGetDatum(false);
        
        if (proc && proc->hasGlobalXid && proc->globalXid[0] != '\0')
            values[16] = CStringGetTextDatum(proc->globalXid);
        else
            nulls[16] = true;

		nulls[17] = true;
        nulls[18] = true;

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
}

/*
 * dist_pg_get_remote_locks
 *
 * A static helper function that dispatches the get_dist_pg_locks(true) call
 * to all remote nodes in the cluster and stores their results in the given
 * Tuplestore.
 *
 * This implementation uses a PARALLEL dispatch model. It constructs a single
 * RemoteQuery plan targeting all remote nodes and executes it concurrently.
 * The remote executor handles the parallel connections and fetches results
 * as they become available from any node.
 *
 * This approach significantly reduces the data collection latency in a large
 * cluster compared to a serial model, as the total execution time is
 * determined by the slowest responding node, not the sum of all response times.
 */
static void
dist_pg_get_remote_locks(Tuplestorestate *tupstore, TupleDesc tupdesc)
{
    char			 query[256];
	RemoteQuery		*plan;
	EState			*estate;
	RemoteQueryState *pstate;
	TupleTableSlot	*result;
	MemoryContext	 oldcontext;

	snprintf(query, sizeof(query), "SELECT * FROM get_dist_pg_locks(true)");

	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	plan->sql_statement = query;
	plan->force_autocommit = false;

	plan->exec_type = EXEC_ON_ALL_NODES;

	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_nodes->missing_ok = true;

	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = NULL;
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	ExecAssignResultType((PlanState *) pstate, tupdesc);
	
	MemoryContextSwitchTo(oldcontext);

	while ((result = ExecRemoteQuery((PlanState *) pstate)) != NULL && !TupIsNull(result))
	{
		tuplestore_puttupleslot(tupstore, result);
	}
	
	ExecEndRemoteQuery(pstate);
	FreeExecutorState(estate);
}

/*
 * get_dist_pg_locks
 *
 * The main SQL-callable entry point for the distributed locks view. It serves
 * as the central coordinator for data collection across the cluster.
 *
 * This function is implemented as a materialized Set-Returning Function (SRF).
 * In a single execution, it performs the following steps:
 *  1. Initializes a Tuplestore to hold the aggregated results.
 *  2. If executed on a coordinator node (and not in 'localonly' mode), it
 *     calls the static helper 'dist_pg_get_remote_locks' to dispatch
 *     collection tasks to all remote nodes.
 *  3. It then calls the static helper 'dist_pg_get_local_locks' to collect
 *     and analyze lock information from the local node.
 *  4. Finally, it returns the fully populated Tuplestore to the executor
 *     for consumption.
 *
 * Arguments:
 *  localonly (boolean, default false): When true, the function skips the
 *    remote data collection stage. This is a crucial mechanism used by the
 *    remote calls themselves to prevent infinite recursion.
 */
Datum
get_dist_pg_locks(PG_FUNCTION_ARGS)
{
    bool localonly = PG_GETARG_BOOL(0);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx, oldcontext;

    if (!IsA(rsinfo, ReturnSetInfo) || !(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    if (!localonly && IS_PGXC_COORDINATOR)
        dist_pg_get_remote_locks(tupstore, tupdesc);

    dist_pg_get_local_locks(tupstore, tupdesc);
    
    tuplestore_donestoring(tupstore);
    MemoryContextSwitchTo(oldcontext);

    return (Datum) 0;
}

/*
 * Hooked as shmem_startup_hook
 */
static void
pgds_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	
	CreateSharedDistStatus();
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgds_memsize(void)
{
	return mul_size(sizeof(PgDistStatStatus), NumBackendStatSlots);
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	
	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomBoolVariable("pg_dist_stat_views.enable_planstate",
	                         "whether to show planstate in result sets.",
	                         NULL,
	                         &pgds_enable_planstate,
	                         true,
	                         PGC_SUSET,
	                         0,
	                         NULL,
	                         NULL,
	                         NULL);

	/*
	 * GUC variable 'pg_dist_stat_views.global_query_id'
	 * 
	 * Purpose: Internal variable for propagating Global Query ID across nodes
	 * - Generated on coordinator and propagated to all datanodes
	 * - Enables cross-node query correlation in distributed execution
	 *
	 * Context: PGC_SUSET (superuser-only, not for direct config file use)
	 * Default: Empty string
	 * 
	 * Note: This is an internal variable, set automatically during query execution
	 */
	DefineCustomStringVariable(
							"pg_dist_stat_views.global_query_id",
							"Internal GUC to propagate Global Query ID.",
							NULL,
							&pgds_gid_guc_string,
							"",
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL
	);
	
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgds_shmem_startup().
	 */
	RequestAddinShmemSpace(pgds_memsize());
	
	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgds_shmem_startup;
	prev_pgstat_report_hook = pgstat_report_hook;
	pgstat_report_hook = pgds_report_query_activity;
	prev_PortalStart = PortalStart_hook;
	PortalStart_hook = pgds_report_activity;
	prev_PortalDrop = PortalDrop_hook;
	PortalDrop_hook = pgds_report_activity;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgds_report_executor_activity;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgds_executor_end_hook;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	pgstat_report_hook = prev_pgstat_report_hook;
	PortalStart_hook = prev_PortalStart;
	PortalDrop_hook = prev_PortalDrop;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorEnd_hook = prev_ExecutorEnd;
}
