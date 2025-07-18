#include "postgres.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "nodes/makefuncs.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "hdfs_fdw.h"
#include "hdfs_client.h"

PG_MODULE_MAGIC;

/* FDW callback function declarations */
static void hdfsGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, 
                                       ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
static void hdfsBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *hdfsIterateForeignScan(ForeignScanState *node);
static void hdfsReScanForeignScan(ForeignScanState *node);
static void hdfsEndForeignScan(ForeignScanState *node);

/* Export functions */
PG_FUNCTION_INFO_V1(hdfs_fdw_handler);
PG_FUNCTION_INFO_V1(hdfs_fdw_validator);

/*
 * FDW handler function entry point
 */
Datum
hdfs_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *routine = makeNode(FdwRoutine);

    /* Scan related functions */
    routine->GetForeignRelSize = hdfsGetForeignRelSize;
    routine->GetForeignPaths = hdfsGetForeignPaths;
    routine->GetForeignPlan = hdfsGetForeignPlan;
    routine->BeginForeignScan = hdfsBeginForeignScan;
    routine->IterateForeignScan = hdfsIterateForeignScan;
    routine->ReScanForeignScan = hdfsReScanForeignScan;
    routine->EndForeignScan = hdfsEndForeignScan;

    PG_RETURN_POINTER(routine);
}

/*
 * Option validation function
 */
Datum
hdfs_fdw_validator(PG_FUNCTION_ARGS)
{
    List       *options_list = (List *) PG_GETARG_POINTER(0);
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *lc;

    /* Validate each option */
    foreach(lc, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(lc);
        
        /* Check if the option is valid */
        if (strcmp(def->defname, "host") != 0 &&
            strcmp(def->defname, "port") != 0 &&
            strcmp(def->defname, "dbname") != 0 &&
            strcmp(def->defname, "username") != 0 &&
            strcmp(def->defname, "password") != 0 &&
            strcmp(def->defname, "table_name") != 0 &&
            strcmp(def->defname, "connect_timeout") != 0 &&
            strcmp(def->defname, "receive_timeout") != 0 &&
            strcmp(def->defname, "fetch_size") != 0 &&
            strcmp(def->defname, "log_remote_sql") != 0 &&
            strcmp(def->defname, "client_type") != 0 &&
            strcmp(def->defname, "auth_type") != 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\"", def->defname)));
        }
    }

    PG_RETURN_VOID();
}

/*
 * hdfsGetForeignRelSize
 *      Estimate # of rows and width of the result of the scan
 */
static void
hdfsGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    hdfs_opt   *options;
    
    elog(DEBUG1, "hdfsGetForeignRelSize called");
    
    /* Get options */
    options = hdfs_get_options(foreigntableid);
    
    /* For now, use a hard-coded estimate */
    baserel->rows = 1000;
    baserel->reltarget->width = 100;
    
    /* Store options in baserel for later use */
    baserel->fdw_private = options;
}

/*
 * hdfsGetForeignPaths
 *      Create possible scan paths for a scan on the foreign table
 */
static void
hdfsGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    ForeignPath *path;
    
    elog(DEBUG1, "hdfsGetForeignPaths called");
    
    /* Create a basic foreign path */
    path = create_foreignscan_path(root, baserel, 
                                               NULL,    /* default pathtarget */
                                               baserel->rows, 
                                               10,      /* startup cost */
                                               100,     /* total cost */
                                               NIL,     /* no pathkeys */
                                               NULL,    /* no outer rel */
                                               NULL,    /* no extra plan */
                                               NIL);    /* no fdw_private */
    
    add_path(baserel, (Path *) path);
}

/*
 * hdfsGetForeignPlan
 *      Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
hdfsGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
    List       *remote_conds = NIL;
    List       *local_conds = NIL;
    
    elog(DEBUG1, "hdfsGetForeignPlan called");
    
    /* Separate clauses into remote and local */
    
    /* For now, treat all clauses as local (basic implementation) */
    local_conds = scan_clauses;
    
    /* Create the ForeignScan node with deparse info */
    return make_foreignscan(tlist,
                           local_conds,
                           baserel->relid,
                           NIL,     /* no expressions to evaluate */
                           NIL,     /* no private data yet */
                           NIL,     /* no custom tlist */
                           NIL,     /* no remote quals yet */
                           outer_plan);
}

/*
 * hdfsBeginForeignScan
 *      Initialize scan state
 */
static void
hdfsBeginForeignScan(ForeignScanState *node, int eflags)
{
    hdfsFdwExecutionState *festate;
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    EState     *estate = node->ss.ps.state;
    RangeTblEntry *rte;
    Oid         userid;
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user;
    hdfs_opt   *options;
    int         rtindex;
    StringInfoData buf;
    List       *retrieved_attrs = NIL;

    elog(DEBUG1, "hdfs_fdw: hdfsBeginForeignScan");

    /* Get foreign table info */
    rtindex = fsplan->scan.scanrelid;
    rte = rt_fetch(rtindex, estate->es_range_table);
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    table = GetForeignTable(rte->relid);
    server = GetForeignServer(table->serverid);
    user = GetUserMapping(userid, server->serverid);

    /* Get options */
    options = hdfs_get_options(rte->relid);

    /* Create execution state */
    festate = (hdfsFdwExecutionState *) palloc0(sizeof(hdfsFdwExecutionState));
    node->fdw_state = festate;

    /* Store options */
    festate->options = options;

    /* Establish connection */
    festate->con_index = hdfs_get_connection(server, options);

    /* Build actual SQL query using deparse function */    
    initStringInfo(&buf);
    /* For now, build a simple SELECT * query */
    if (options->table_name)
        appendStringInfo(&buf, "SELECT * FROM %s", options->table_name);
    else
        appendStringInfo(&buf, "SELECT * FROM unknown_table");
    
    festate->query = buf.data;
    festate->query_executed = false;
    festate->retrieved_attrs = retrieved_attrs;

    /* Create memory context */
    festate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                              "hdfs_fdw temporary data",
                                              ALLOCSET_DEFAULT_SIZES);

    /* Get attribute metadata */
    festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
}

/*
 * hdfsIterateForeignScan
 *      Fetch next row from the result set
 */
static TupleTableSlot *
hdfsIterateForeignScan(ForeignScanState *node)
{
    hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    MemoryContext oldcontext;
    int         rc;
    int         column_count;
    char      **values;
    int         i;
    HeapTuple   tuple;

    elog(DEBUG1, "hdfs_fdw: hdfsIterateForeignScan");

    /* Clear slot */
    ExecClearTuple(slot);

    /* Switch to batch memory context */
    oldcontext = MemoryContextSwitchTo(festate->batch_cxt);

    /* Execute query on first call */
    if (!festate->query_executed)
    {
        hdfs_query_execute(festate->con_index, festate->options, festate->query);
        festate->query_executed = true;
    }

    /* Fetch next row */
    rc = hdfs_fetch(festate->con_index);
    if (rc != 0)
    {
        /* No more data */
        MemoryContextSwitchTo(oldcontext);
        return NULL;
    }

    /* Get column count and data */
    column_count = hdfs_get_column_count(festate->con_index);
    values = (char **) palloc(sizeof(char *) * column_count);
    
    for (i = 0; i < column_count; i++)
    {
        bool is_null;
        char *value = hdfs_get_field_as_cstring(festate->con_index, i, &is_null);
        values[i] = is_null ? NULL : value;
    }

    /* Build tuple */
    tuple = BuildTupleFromCStrings(festate->attinmeta, values);
    
    /* Restore memory context */
    MemoryContextSwitchTo(oldcontext);

    /* Store tuple in slot */
    ExecStoreTuple(tuple, slot, InvalidBuffer, false);

    return slot;
}

/*
 * hdfsReScanForeignScan
 *      Restart the scan from the beginning
 */
static void
hdfsReScanForeignScan(ForeignScanState *node)
{
    hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;
    
    elog(DEBUG1, "hdfsReScanForeignScan called");
    
    /* Reset query execution state */
    festate->query_executed = false;
}

/*
 * hdfsEndForeignScan
 *      End the scan and release resources
 */
static void
hdfsEndForeignScan(ForeignScanState *node)
{
    hdfsFdwExecutionState *festate = (hdfsFdwExecutionState *) node->fdw_state;

    elog(DEBUG1, "hdfs_fdw: hdfsEndForeignScan");

    if (festate)
    {
        /* Close connection */
        if (festate->con_index >= 0)
            hdfs_rel_connection(festate->con_index);

        /* Delete memory context */
        if (festate->batch_cxt)
            MemoryContextDelete(festate->batch_cxt);
    }
}
