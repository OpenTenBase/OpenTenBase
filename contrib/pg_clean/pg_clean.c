#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "storage/procarray.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/varlena.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/builtins.h"

#include "executor/tuptable.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxcnode.h"
#include "access/tupdesc.h"
#include "access/htup_details.h"
#include "lib/stringinfo.h"

#include "access/gtm.h"
#include "datatype/timestamp.h"
#include "access/xact.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "utils/timestamp.h"
#include "catalog/pg_control.h"
#include "commands/dbcommands.h"

#include "utils/memutils.h"
#include "nodes/memnodes.h"

#ifdef XCP
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/snapmgr.h"
#endif
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#endif

#include "storage/fd.h"
#include "pgstat.h"
#include "access/xact.h"
#include "access/twophase.h"
#include "access/hash.h"

/*hash_create hash_search*/
#include "utils/hsearch.h"

#define TWOPHASE_RECORD_DIR "pg_2pc"
int  transaction_threshold = 200000;
#define MAXIMUM_CLEAR_FILE 10000
#define MAXIMUM_OUTPUT_FILE 1000
#define XIDPREFIX "_$XC$"
#define DEFAULT_CLEAN_TIME_INTERVAL 120
#define DEFAULT_CHECK_TIME_INTERVAL 5

#ifdef __TWO_PHASE_TESTS__
#define LEAST_CLEAN_TIME_INTERVAL     1 /* should not clean twophase trans prepared in 1s */
#define LEAST_CHECK_TIME_INTERVAL     1 /* should not check twophase trans prepared in 1s */
#else
#define LEAST_CLEAN_TIME_INTERVAL     10 /* should not clean twophase trans prepared in 10s */
#define LEAST_CHECK_TIME_INTERVAL     3  /* should not check twophase trans prepared in 3s */
#endif

GlobalTimestamp clean_time_interval = DEFAULT_CLEAN_TIME_INTERVAL * USECS_PER_SEC;

PG_MODULE_MAGIC;

#define MAX_GID        GIDSIZE      /* gid max length */
#define MAX_DBNAME     NAMEDATALEN  /* database name max length */
#define MAX_FIELD_LEN  100          /* other feild max length */

#define GET_START_XID "startxid:"
#define GET_PREPARE_TIMESTAMP "global_prepare_timestamp:"
#define GET_COMMIT_TIMESTAMP "global_commit_timestamp:"
#define GET_START_NODE "startnode:"
#define GET_NODE "nodes:"
#define GET_XID "\nxid:"
#define GET_DATABASE "database:"
#define GET_USER "user:"
#define GET_READONLY "readonly"
#define ROLLBACK_POSTFIX ".rollback" /* 2pc file postfix when the 2pc is rollbacked */
#define GET_GLOBAL_TRACE_ID "gtraceid:"

#define MAX_TWOPC_TXN 1000
#define STRING_BUFF_LEN 1024

#define MAX_CMD_LENGTH (GIDSIZE + 120)

#define LONG_CMD_LENGTH 1024

#define XIDFOUND 1
#define XIDNOTFOUND -1
#define XIDEXECFAIL -2

#define FILEFOUND 1
#define FILEUNKOWN -1
#define FILENOTFOUND -2

#define INIT(x)\
do{\
	x = NULL;\
	x##_count = 0;\
	x##_size = 0;\
}while(0);

#define RPALLOC(x)\
do{\
    if (x##_size < x##_count+1)\
    {\
        int temp_size = (x##_size > 0) ? x##_size : 1;\
        if (NULL == x)\
        {\
			x = palloc0(2*temp_size*sizeof(*x));\
		}\
        else\
        {\
        	x = repalloc(x, 2*temp_size*sizeof(*x));\
        }\
    	x##_size = 2*temp_size;\
    }\
}while(0);

#define PALLOC(x, y)\
do{\
    RPALLOC(x);\
    x[x##_count] = y;\
    x##_count++;\
}while(0);

#define RFREE(x)\
do{\
    if (x##_size > 0)\
    {\
        pfree(x);\
    }\
    x = NULL;\
    x##_count = 0;\
    x##_size = 0;\
}while(0);
	
#define ENUM_TOCHAR_CASE(x)   case x: return(#x);

/*data structures*/
typedef enum TXN_STATUS
{
	TXN_STATUS_INITIAL = 0,	/* Initial */
	TXN_STATUS_PREPARED,
	TXN_STATUS_COMMITTED,
	TXN_STATUS_ABORTED,
	TXN_STATUS_INPROGRESS,
	TXN_STATUS_FAILED,		/* Error detected while interacting with the node */
	TXN_STATUS_UNKNOWN	/* Unknown: Frozen, running, or not started */
} TXN_STATUS;


typedef enum 
{
	UNDO = 0,
	ABORT,
	COMMIT
} OPERATION;

typedef enum
{
    TWOPHASE_FILE_EXISTS = 0,
    TWOPHASE_FILE_NOT_EXISTS,
    TWOPHASE_FILE_OLD, 
    TWOPHASE_FILE_ERROR
}TWOPHASE_FILE_STATUS;
	
typedef struct txn_info
{
	char			gid[MAX_GID];
	uint32			*xid;				/* xid used in prepare */
	TimestampTz		*prepare_timestamp;
	char			*database;
	char			*owner;
    char            *participants;
	Oid				origcoord;			/* Original coordinator who initiated the txn */
    bool            after_first_phase;
    uint32          startxid;           /* xid in Original coordinator */
	bool			isorigcoord_part;	/* Is original coordinator a
										   participant? */
	int				num_dnparts;		/* Number of participant datanodes */
	int				num_coordparts;		/* Number of participant coordinators */
	int				*dnparts;			/* Whether a node was participant in the txn */
	int				*coordparts;
	TXN_STATUS		*txn_stat;			/* Array for each nodes */
	char			*msg;				/* Notice message for this txn. */
	GlobalTimestamp  global_commit_timestamp;	/* get global_commit_timestamp from node once it is committed*/
	GlobalTimestamp  global_prepare_timestamp;	/* get global_prepare_timestamp from node once it is prepared*/

	TXN_STATUS		global_txn_stat;
	OPERATION		op;
	bool			op_issuccess;
    bool            is_readonly;
    bool            belong_abnormal_node;
}txn_info;

typedef struct database_info
{
	struct database_info *next;
	char *database_name;

    HTAB *all_txn_info;
#if 0 
	txn_info *head_txn_info;
	txn_info *last_txn_info;
#endif
} database_info;

typedef struct 
{
	int index;
	txn_info **txn;
	int txn_count;
	int txn_size;
	MemoryContext mycontext;
} print_txn_info;

typedef struct
{
	int index;
	int count;
	char **gid;
	int gid_count;
	int gid_size;
	char **database;
	int database_count;
	int database_size;
	char **global_status;
	int global_status_count;
	int global_status_size;
	char **status;
	int status_count;
	int status_size;
	MemoryContext mycontext;
} print_status;

typedef struct 
{
	char ***slot;	/*slot[i][j] stores value of row i, colum j*/
	int slot_count;	/*number of rows*/
	int slot_size;
	int attnum;
}TupleTableSlots;

/*global variable*/
static Oid	        *cn_node_list = NULL;
static Oid	        *dn_node_list = NULL;
static bool         *cn_health_map = NULL;
static bool         *dn_health_map = NULL;
static int	        cn_nodes_num = 0;
static int	        dn_nodes_num = 0;
static int	        pgxc_clean_node_count = 0;
static Oid	        my_nodeoid;
static 
database_info       *head_database_info = NULL;
static 
database_info       *last_database_info = NULL;
bool		        execute = false;
int                 total_twopc_txn = 0;

TimestampTz         current_time = 0;
TimestampTz         abnormal_time = 0;
GlobalTimestamp     current_gts = InvalidGlobalTimestamp;   /* use to save current gts */
GlobalTimestamp     abnormal_gts = InvalidGlobalTimestamp;  /* use to save abnormal gts, clean 2PCs which prepare gts less than abnormal gts */
char                *abnormal_nodename = NULL;
Oid                 abnormal_nodeoid = InvalidOid;
bool                clear_2pc_belong_node = false;

static bool         is_for_node_removed = false;

static bool         is_for_deadlock = false;

/*function list*/
	/*plugin entry function*/

static bool check_node_health(Oid node_oid);
static Datum 
	 execute_query_on_single_node(Oid node, const char * query, int attnum, TupleTableSlots * tuples);
void DestroyTxnHash(void);
static void ResetGlobalVariables(void);

static Oid  
	 getMyNodeoid(void);
static void 
	 getDatabaseList(void);
static char* TTSgetvalue(TupleTableSlots *result, int tup_num, int field_num);
static void DropTupleTableSlots(TupleTableSlots *
Slots);
static void 
	 getTxnInfoOnNodesAll(void);
void getTxnInfoOnNode(Oid node);
txn_info *add_txn_info(char * dbname, Oid node_oid, uint32 xid, char * gid,
					  char * owner, TimestampTz prepared_time, TXN_STATUS status);
TWOPHASE_FILE_STATUS GetTransactionPartNodes(txn_info * txn, Oid node_oid);
static txn_info *
	 find_txn(char *gid);
txn_info*	
	 make_txn_info(char * dbname, char * gid, char * owner);
database_info*	
	 find_database_info(char *database_name);
database_info*
	 add_database_info(char *database_name);
int	 find_node_index(Oid node_oid);
Oid  find_node_oid(int node_idx);
void getTxnInfoOnOtherNodesAll(void);
void getTxnInfoOnOtherNodesForDatabase(database_info *database);
void getTxnInfoOnOtherNodes(txn_info *txn);
int Get2PCXidByGid(Oid node_oid, char * gid, uint32 * transactionid);

char *get2PCInfo(const char *tid);

void getTxnStatus(txn_info * txn, int node_idx);
void recover2PCForDatabaseAll(void);
void recover2PCForDatabase(database_info * db_info);
#if 0    
static bool 
	 setMaintenanceMode(bool status);
#endif
bool send_query_clean_transaction(PGXCNodeHandle * conn, txn_info * txn, const char * finish_cmd);
bool check_2pc_belong_node(txn_info * txn);
bool check_node_participate(txn_info * txn, int node_idx);

bool check_2pc_start_from_node(txn_info *txn);

void recover2PC(txn_info * txn);
TXN_STATUS 
	 check_txn_global_status(txn_info *txn);
bool clean_2PC_iscommit(txn_info *txn, bool is_commit, bool is_check);
bool clean_2PC_files(txn_info *txn);
void Init_print_txn_info(print_txn_info *print_txn);
void Init_print_stats_all(print_status *pstatus);
void Init_print_stats(txn_info * txn, char * database, print_status * pstatus);
static const char *
	 txn_status_to_string(TXN_STATUS status);
static const char *
	 txn_op_to_string(OPERATION op);
static void 
     CheckFirstPhase(txn_info *txn);
static void 
     get_transaction_handles(PGXCNodeAllHandles **pgxc_handles, txn_info *txn);
static void 
     get_node_handles(PGXCNodeAllHandles ** pgxc_handles, Oid nodeoid);

uint32 get_start_xid_from_gid(char *gid);
char *get_start_node_from_gid(char *gid);
Oid get_start_node_oid_from_gid(char *gid);

bool is_xid_running_on_node(uint32 xid, Oid node_oid);
bool is_gid_start_xid_running(char *gid);
bool is_txn_start_xid_running(txn_info *txn);

void getAllTxnInfoFrom2pcFiles(void);
void getTxnInfoFrom2pcFile(char *file_name, Oid node_oid);
static bool GetGlobalTraceIdOnPartNodes(txn_info *txn, Oid node_oid);
static bool GetGlobalTraceIdImpl(txn_info *txn, char *traceid);
static bool GetGlobalTraceIdOnNode(const txn_info *txn);

Datum	pg_clean_execute(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute);
Datum	pg_clean_execute(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 	*funcctx;
	HeapTuple			tuple;		
	print_txn_info		*print_txn = NULL;
	txn_info 			*temp_txn;
	char				txn_gid[MAX_GID];
	char				txn_status[MAX_FIELD_LEN];
	char				txn_op[MAX_FIELD_LEN];
	char				txn_op_issuccess[MAX_FIELD_LEN];
	
	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];
	
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;
	
		
		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);
		
        /*clear Global*/
        ResetGlobalVariables();
        execute = true;

        clean_time_interval = PG_GETARG_INT32(0);
        if (LEAST_CLEAN_TIME_INTERVAL > clean_time_interval)
        {
            elog(WARNING, "least clean time interval is %ds",
                LEAST_CLEAN_TIME_INTERVAL);
            clean_time_interval = LEAST_CLEAN_TIME_INTERVAL;
        }
        clean_time_interval *= USECS_PER_SEC;
        
		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);
		
		print_txn->mycontext = mycontext;
		
		MemoryContextSwitchTo(oldcontext);

		pgxc_node_set_global_traceid_regenerate(true);

	}
	
	funcctx = SRF_PERCALL_SETUP();	
	print_txn = (print_txn_info *) funcctx->user_fctx;
	
	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, MAX_GID);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat),
			MAX_FIELD_LEN);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), MAX_FIELD_LEN);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", MAX_FIELD_LEN);
		else
			strncpy(txn_op_issuccess, "fail", MAX_FIELD_LEN);
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		
		//MemoryContextDelete(print_txn->mycontext);
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * only use when deadlock
  */
Datum	pg_clean_execute_for_deadlock(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute_for_deadlock);
Datum	pg_clean_execute_for_deadlock(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext     *funcctx;
	HeapTuple            tuple;
	print_txn_info      *print_txn = NULL;
	txn_info            *temp_txn;
	char                 txn_gid[MAX_GID];
	char                 txn_status[MAX_FIELD_LEN];
	char                 txn_op[MAX_FIELD_LEN];
	char                 txn_op_issuccess[MAX_FIELD_LEN];
	Datum                values[ACCESS_CONTROL_ATTR_NUM];
	bool                 nulls[ACCESS_CONTROL_ATTR_NUM];

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;

		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);

		/*clear Global*/
		ResetGlobalVariables();
		is_for_deadlock = true;
		execute = true;

		clean_time_interval = PG_GETARG_INT32(0);
		if (LEAST_CLEAN_TIME_INTERVAL > clean_time_interval)
		{
			elog(WARNING, "least clean time interval is %ds",
				LEAST_CLEAN_TIME_INTERVAL);
			clean_time_interval = LEAST_CLEAN_TIME_INTERVAL;
		}
		clean_time_interval *= USECS_PER_SEC;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		current_time = GetCurrentTimestamp();
		current_gts = GetGlobalTimestampGTM();
		if (!GlobalTimestampIsValid(current_gts))
		{
			elog(ERROR, "get invalid gts");
		}

		/*get txn info from 2pc files*/
		getAllTxnInfoFrom2pcFiles();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);

		print_txn->mycontext = mycontext;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	print_txn = (print_txn_info *) funcctx->user_fctx;

	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, MAX_GID);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat),
			MAX_FIELD_LEN);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), MAX_FIELD_LEN);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", MAX_FIELD_LEN);
		else
			strncpy(txn_op_issuccess, "fail", MAX_FIELD_LEN);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		//MemoryContextDelete(print_txn->mycontext);
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * only use after node remove from the cluster
 */
Datum	pg_clean_execute_for_node_removed(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute_for_node_removed);
Datum	pg_clean_execute_for_node_removed(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext *funcctx;
	HeapTuple        tuple;
	print_txn_info  *print_txn = NULL;
	txn_info        *temp_txn;
	char             txn_gid[MAX_GID];
	char             txn_status[MAX_FIELD_LEN];
	char             txn_op[MAX_FIELD_LEN];
	char             txn_op_issuccess[MAX_FIELD_LEN];

	Datum       values[ACCESS_CONTROL_ATTR_NUM];
	bool        nulls[ACCESS_CONTROL_ATTR_NUM];

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;

		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);

		/*clear Global*/
		ResetGlobalVariables();
		execute = true;
		is_for_node_removed = true;

		clean_time_interval = PG_GETARG_INT32(0);
		if (LEAST_CLEAN_TIME_INTERVAL > clean_time_interval)
		{
			elog(WARNING, "least clean time interval is %ds",
				LEAST_CLEAN_TIME_INTERVAL);
			clean_time_interval = LEAST_CLEAN_TIME_INTERVAL;
		}
		clean_time_interval *= USECS_PER_SEC;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);

		print_txn->mycontext = mycontext;

		MemoryContextSwitchTo(oldcontext);

		pgxc_node_set_global_traceid_regenerate(true);
	}

	funcctx = SRF_PERCALL_SETUP();
	print_txn = (print_txn_info *) funcctx->user_fctx;

	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, MAX_GID);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat),
			MAX_FIELD_LEN);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), MAX_FIELD_LEN);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", MAX_FIELD_LEN);
		else
			strncpy(txn_op_issuccess, "fail", MAX_FIELD_LEN);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		//MemoryContextDelete(print_txn->mycontext);
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * clear 2pc after oss detect abnormal node and restart it , 
 * only clear 2pc belong the abnormal node and before the abnormal time
 */
Datum	pg_clean_execute_on_node(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute_on_node);
Datum	pg_clean_execute_on_node(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 	*funcctx;
	HeapTuple			tuple;		
	print_txn_info		*print_txn = NULL;
	txn_info 			*temp_txn;
	char				txn_gid[MAX_GID];
	char				txn_status[MAX_FIELD_LEN];
	char				txn_op[MAX_FIELD_LEN];
	char				txn_op_issuccess[MAX_FIELD_LEN];
	int64				time_gap = 0;

	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];
	
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;
	
		
		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);
		
        /*clear Global*/
        ResetGlobalVariables();
        execute = true;
        clear_2pc_belong_node = true;

        if (0 == PG_GETARG_DATUM(0))
        {
            elog(ERROR, "pg_clean_execute_on_node: node name is empty");
        }
        abnormal_nodename = text_to_cstring(PG_GETARG_TEXT_P(0));
        abnormal_nodeoid = get_pgxc_nodeoid(abnormal_nodename);
        if (!OidIsValid(abnormal_nodeoid))
        {
            elog(ERROR, "pg_clean_execute_on_node, cannot clear 2pc of "
                "invalid nodename '%s'", abnormal_nodename);
        }

        abnormal_time = PG_GETARG_INT64(1);
        current_time = GetCurrentTimestamp();
        if (abnormal_time > current_time)
        {
            /*abnormal time later than current time, can not clean*/
            elog(ERROR, "pg_clean_execute_on_node, abnormal time: "
                INT64_FORMAT " later than current time: " INT64_FORMAT,
                abnormal_time, current_time);
        }

        time_gap = current_time - abnormal_time;
        if (time_gap < LEAST_CLEAN_TIME_INTERVAL * USECS_PER_SEC)
        {
            /*time gap less than LEAST_CLEAN_TIME_INTERVAL, can not clean*/
            elog(ERROR, "pg_clean_execute_on_node, least clean interval is %ds, "
                "abnormal time: " INT64_FORMAT ", current time: " INT64_FORMAT,
                LEAST_CLEAN_TIME_INTERVAL, abnormal_time, current_time);
        }

        clean_time_interval = time_gap;

        current_gts = GetGlobalTimestampGTM();
        if (!GlobalTimestampIsValid(current_gts))
        {
            /*get invalid gts, can not clean*/
            elog(ERROR, "pg_clean_execute_on_node, get invalid gts");
        }
        abnormal_gts = current_gts - time_gap;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);
		
		print_txn->mycontext = mycontext;
		
		MemoryContextSwitchTo(oldcontext);

		pgxc_node_set_global_traceid_regenerate(true);

	}
	
	funcctx = SRF_PERCALL_SETUP();	
	print_txn = (print_txn_info *) funcctx->user_fctx;
	
	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, MAX_GID);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat),
			MAX_FIELD_LEN);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), MAX_FIELD_LEN);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", MAX_FIELD_LEN);
		else
			strncpy(txn_op_issuccess, "fail", MAX_FIELD_LEN);
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		DestroyTxnHash();
        pfree(abnormal_nodename);
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * only use when deadlock
  */
Datum	pg_clean_execute_on_node_for_deadlock(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_execute_on_node_for_deadlock);
Datum	pg_clean_execute_on_node_for_deadlock(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext     *funcctx;
	HeapTuple            tuple;
	print_txn_info      *print_txn = NULL;
	txn_info            *temp_txn;
	char                 txn_gid[MAX_GID];
	char                 txn_status[MAX_FIELD_LEN];
	char                 txn_op[MAX_FIELD_LEN];
	char                 txn_op_issuccess[MAX_FIELD_LEN];
	int64                time_gap = 0;
	Datum                values[ACCESS_CONTROL_ATTR_NUM];
	bool                 nulls[ACCESS_CONTROL_ATTR_NUM];

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		MemoryContext mycontext;
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "operation",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "operation_status",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = (print_txn_info *)palloc0(sizeof(print_txn_info));
		print_txn = (print_txn_info *) funcctx->user_fctx;

		MemoryContextSwitchTo(oldcontext);
		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);

		/*clear Global*/
		ResetGlobalVariables();
		is_for_deadlock = true;
		execute = true;
		clear_2pc_belong_node = true;

		if (0 == PG_GETARG_DATUM(0))
		{
			elog(ERROR, "pg_clean_execute_on_node: node name is empty");
		}
		abnormal_nodename = text_to_cstring(PG_GETARG_TEXT_P(0));
		abnormal_nodeoid = get_pgxc_nodeoid(abnormal_nodename);
        if (!OidIsValid(abnormal_nodeoid))
		{
			elog(ERROR, "pg_clean_execute_on_node, cannot clear 2pc of "
				"invalid nodename '%s'", abnormal_nodename);
		}

		abnormal_time = PG_GETARG_INT64(1);
		current_time = GetCurrentTimestamp();
		if (abnormal_time > current_time)
		{
			/*abnormal time later than current time, can not clean*/
			elog(ERROR, "pg_clean_execute_on_node, abnormal time: "
				INT64_FORMAT " later than current time: " INT64_FORMAT,
				abnormal_time, current_time);
		}

		time_gap = current_time - abnormal_time;
		if (time_gap < LEAST_CLEAN_TIME_INTERVAL * USECS_PER_SEC)
		{
			/*time gap less than LEAST_CLEAN_TIME_INTERVAL, can not clean*/
			elog(ERROR, "pg_clean_execute_on_node, least clean interval is %ds, "
				"abnormal time: " INT64_FORMAT ", current time: " INT64_FORMAT,
				LEAST_CLEAN_TIME_INTERVAL, abnormal_time, current_time);
		}

		clean_time_interval = time_gap;

		current_gts = GetGlobalTimestampGTM();
		if (!GlobalTimestampIsValid(current_gts))
		{
			/*get invalid gts, can not clean*/
			elog(ERROR, "pg_clean_execute_on_node, get invalid gts");
		}
		abnormal_gts = current_gts - time_gap;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*add my database info*/
		add_database_info(get_database_name(MyDatabaseId));

		/*get txn info from 2pc files*/
		getAllTxnInfoFrom2pcFiles();

		/*recover all 2PC transactions*/
		recover2PCForDatabaseAll();

		Init_print_txn_info(print_txn);

		print_txn->mycontext = mycontext;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	print_txn = (print_txn_info *) funcctx->user_fctx;

	if (print_txn->index < print_txn->txn_count)
	{
		temp_txn = print_txn->txn[print_txn->index];
		strncpy(txn_gid, temp_txn->gid, MAX_GID);
		strncpy(txn_status, txn_status_to_string(temp_txn->global_txn_stat),
			MAX_FIELD_LEN);
		strncpy(txn_op, txn_op_to_string(temp_txn->op), MAX_FIELD_LEN);
		if (temp_txn->op_issuccess)
			strncpy(txn_op_issuccess, "success", MAX_FIELD_LEN);
		else
			strncpy(txn_op_issuccess, "fail", MAX_FIELD_LEN);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(txn_gid));
		values[1] = PointerGetDatum(cstring_to_text(txn_status));
		values[2] = PointerGetDatum(cstring_to_text(txn_op));
		values[3] = PointerGetDatum(cstring_to_text(txn_op_issuccess));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		print_txn->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		DestroyTxnHash();
		pfree(abnormal_nodename);
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

Datum	pg_clean_check_txn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_check_txn);
Datum	pg_clean_check_txn(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 	*funcctx;
	HeapTuple			tuple;		
	print_status		*pstatus = NULL;
	
	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];
	execute = false;
    
	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MemoryContext mycontext;
		TupleDesc	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "database",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "transaction_status_on_allnodes",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		funcctx->user_fctx = (print_status *)palloc0(sizeof(print_status));
		pstatus = (print_status *) funcctx->user_fctx;
		pstatus->index = pstatus->count = 0;
		pstatus->gid = NULL;
		pstatus->global_status = pstatus->status = (char **)NULL;
		pstatus->database = NULL;
		pstatus->mycontext = NULL;
	

		MemoryContextSwitchTo(oldcontext);

		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);

        /*clear Global*/
        ResetGlobalVariables();
        
        clean_time_interval = PG_GETARG_INT32(0);
        if (LEAST_CHECK_TIME_INTERVAL > clean_time_interval)
        {
            elog(WARNING, "least check time interval is %ds",
				LEAST_CHECK_TIME_INTERVAL);
            clean_time_interval = LEAST_CHECK_TIME_INTERVAL;
        }
        clean_time_interval *= USECS_PER_SEC;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,  
						&cn_nodes_num, &dn_nodes_num, true);
        if (cn_node_list == NULL || dn_node_list == NULL)
            elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*get all database info*/
		getDatabaseList();

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		Init_print_stats_all(pstatus);
	
		pstatus->mycontext = mycontext;
	
		MemoryContextSwitchTo(oldcontext);

		pgxc_node_set_global_traceid_regenerate(true);

	}
	
	funcctx = SRF_PERCALL_SETUP();	
	pstatus = (print_status *) funcctx->user_fctx;
	
	if (pstatus->index < pstatus->count)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(pstatus->gid[pstatus->index]));
		values[1] = PointerGetDatum(cstring_to_text(pstatus->database[pstatus->index]));
		values[2] = PointerGetDatum(cstring_to_text(pstatus->global_status[pstatus->index]));
		values[3] = PointerGetDatum(cstring_to_text(pstatus->status[pstatus->index]));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		pstatus->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/*
		MemoryContextDelete(pstatus->mycontext);
		DropDatabaseInfo();
		*/
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * only use when deadlock
  */
Datum	pg_clean_check_txn_for_deadlock(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_check_txn_for_deadlock);
Datum	pg_clean_check_txn_for_deadlock(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext     *funcctx;
	HeapTuple            tuple;
	print_status        *pstatus = NULL;
	Datum                values[ACCESS_CONTROL_ATTR_NUM];
	bool                 nulls[ACCESS_CONTROL_ATTR_NUM];

	execute = false;

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MemoryContext mycontext;
		TupleDesc	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "database",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "transaction_status_on_allnodes",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = (print_status *)palloc0(sizeof(print_status));
		pstatus = (print_status *) funcctx->user_fctx;
		pstatus->index = pstatus->count = 0;
		pstatus->gid = NULL;
		pstatus->global_status = pstatus->status = (char **)NULL;
		pstatus->database = NULL;
		pstatus->mycontext = NULL;

		MemoryContextSwitchTo(oldcontext);

		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);

		/*clear Global*/
		ResetGlobalVariables();
		is_for_deadlock = true;

		clean_time_interval = PG_GETARG_INT32(0);
		if (LEAST_CHECK_TIME_INTERVAL > clean_time_interval)
		{
			elog(WARNING, "least check time interval is %ds",
				LEAST_CHECK_TIME_INTERVAL);
			clean_time_interval = LEAST_CHECK_TIME_INTERVAL;
		}
		clean_time_interval *= USECS_PER_SEC;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		if (cn_node_list == NULL || dn_node_list == NULL)
			elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		current_time = GetCurrentTimestamp();
		current_gts = GetGlobalTimestampGTM();
		if (!GlobalTimestampIsValid(current_gts))
		{
			elog(ERROR, "get invalid gts");
		}

		/*get txn info from 2pc files*/
		getAllTxnInfoFrom2pcFiles();

		/*recover all 2PC transactions*/
		Init_print_stats_all(pstatus);

		pstatus->mycontext = mycontext;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	pstatus = (print_status *) funcctx->user_fctx;

	if (pstatus->index < pstatus->count)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(pstatus->gid[pstatus->index]));
		values[1] = PointerGetDatum(cstring_to_text(pstatus->database[pstatus->index]));
		values[2] = PointerGetDatum(cstring_to_text(pstatus->global_status[pstatus->index]));
		values[3] = PointerGetDatum(cstring_to_text(pstatus->status[pstatus->index]));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		pstatus->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * only use after node remove from the cluster
 */
Datum	pg_clean_check_txn_for_node_removed(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_clean_check_txn_for_node_removed);
Datum	pg_clean_check_txn_for_node_removed(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext *funcctx;
	HeapTuple        tuple;
	print_status    *pstatus = NULL;

	Datum            values[ACCESS_CONTROL_ATTR_NUM];
	bool             nulls[ACCESS_CONTROL_ATTR_NUM];
	execute = false;

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		MemoryContext mycontext;
		TupleDesc	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "gid",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "database",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "global_transaction_status",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "transaction_status_on_allnodes",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = (print_status *)palloc0(sizeof(print_status));
		pstatus = (print_status *) funcctx->user_fctx;
		pstatus->index = pstatus->count = 0;
		pstatus->gid = NULL;
		pstatus->global_status = pstatus->status = (char **)NULL;
		pstatus->database = NULL;
		pstatus->mycontext = NULL;

		MemoryContextSwitchTo(oldcontext);

		mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
												  "clean_check",
												  ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(mycontext);

		/*clear Global*/
		ResetGlobalVariables();
		is_for_node_removed = true;

		clean_time_interval = PG_GETARG_INT32(0);
		if (LEAST_CHECK_TIME_INTERVAL > clean_time_interval)
		{
			elog(WARNING, "least check time interval is %ds",
				LEAST_CHECK_TIME_INTERVAL);
			clean_time_interval = LEAST_CHECK_TIME_INTERVAL;
		}
		clean_time_interval *= USECS_PER_SEC;

		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		if (cn_node_list == NULL || dn_node_list == NULL)
			elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
		pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
		my_nodeoid = getMyNodeoid();
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*get all database info*/
		getDatabaseList();

		/*get all info of 2PC transactions*/
		getTxnInfoOnNodesAll();

		/*get txn info on other nodes all*/
		getTxnInfoOnOtherNodesAll();

		/*recover all 2PC transactions*/
		Init_print_stats_all(pstatus);

		pstatus->mycontext = mycontext;

		MemoryContextSwitchTo(oldcontext);

		pgxc_node_set_global_traceid_regenerate(true);
	}

	funcctx = SRF_PERCALL_SETUP();
	pstatus = (print_status *) funcctx->user_fctx;

	if (pstatus->index < pstatus->count)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(cstring_to_text(pstatus->gid[pstatus->index]));
		values[1] = PointerGetDatum(cstring_to_text(pstatus->database[pstatus->index]));
		values[2] = PointerGetDatum(cstring_to_text(pstatus->global_status[pstatus->index]));
		values[3] = PointerGetDatum(cstring_to_text(pstatus->status[pstatus->index]));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		pstatus->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/*
		MemoryContextDelete(pstatus->mycontext);
		DropDatabaseInfo();
		*/
		DestroyTxnHash();
		ResetGlobalVariables();
		SRF_RETURN_DONE(funcctx);
	}
}

void DestroyTxnHash(void)
{
    database_info *dbinfo = head_database_info;
    while (dbinfo)
    {
        hash_destroy(dbinfo->all_txn_info);
        dbinfo = dbinfo->next;
    }
}

static void ResetGlobalVariables(void)
{
	cn_node_list = NULL;
	dn_node_list = NULL;
	cn_health_map = NULL;
	dn_health_map = NULL;
	cn_nodes_num = 0;
	dn_nodes_num = 0;
	pgxc_clean_node_count = 0;
	execute = false;
    total_twopc_txn = 0;

	head_database_info = last_database_info = NULL;

    current_time = 0;
    abnormal_time = 0;
    current_gts = InvalidGlobalTimestamp;
    abnormal_gts = InvalidGlobalTimestamp;
    abnormal_nodename = NULL;
    abnormal_nodeoid = InvalidOid;
    clear_2pc_belong_node = false;
    is_for_node_removed = false;
    is_for_deadlock = false;
}

static Oid getMyNodeoid(void)
{
	Oid my_oid = get_pgxc_nodeoid(PGXCNodeName);
	if (!OidIsValid(my_oid))
	{
		elog(WARNING, "Get local node(%s) oid failed", PGXCNodeName);
	}

	return my_oid;
}

/* 
 * execute_query_on_single_node -- execute query on certain node and get results
 * input: 	node oid, execute query, number of attribute in results, results
 * return:	(Datum) 0
 */
static Datum
execute_query_on_single_node(Oid node, const char *query, int attnum, TupleTableSlots *tuples)  //delete numnodes, delete nodelist, insert node
{
	int 		ii;
	bool		issuccess = false;

	/*check health of node*/
	bool ishealthy = check_node_health(node);

#ifdef XCP
	EState				*estate;
	MemoryContext		oldcontext;
	RemoteQuery			*plan;
	RemoteQueryState	*pstate;
	TupleTableSlot		*result = NULL;
	Var			   		*dummy;
	char ntype = PGXC_NODE_NONE;

	/*
	 * Make up RemoteQuery plan node
	 */
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_type = EXEC_ON_NONE;

	plan->exec_nodes->nodeList = lappend_int(plan->exec_nodes->nodeList,
		PGXCNodeGetNodeId(node, &ntype));
	if (ntype == PGXC_NODE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unknown node Oid: %u", node)));
	else if (ntype == PGXC_NODE_COORDINATOR) 
	{
		plan->exec_type = EXEC_ON_COORDS;
	}
	else
	{
		plan->exec_type = EXEC_ON_DATANODES;
	}

	plan->sql_statement = (char *)query;
	plan->force_autocommit = false;
	/*
	 * We only need the target entry to determine result data type.
	 * So create dummy even if real expression is a function.
	 */
	for (ii = 1; ii <= attnum; ii++)
	{
		dummy = makeVar(1, ii, TEXTOID, 0, InvalidOid, 0);
		plan->scan.plan.targetlist = lappend(plan->scan.plan.targetlist,
										  makeTargetEntry((Expr *) dummy, ii, NULL, false));
	}
	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	MemoryContextSwitchTo(oldcontext);

	/*execute query on node when node is healthy*/
	INIT(tuples->slot);
	tuples->attnum = 0;	
	if (ishealthy)
	{
		int i_tuple = 0;
		int i_attnum = 0;
		issuccess = true;
		result = ExecRemoteQuery((PlanState *) pstate);
		tuples->attnum = attnum;
		while (result != NULL && !TupIsNull(result))
		{
			slot_getallattrs(result); 
			RPALLOC(tuples->slot);
			tuples->slot[i_tuple] = (char **) palloc0(attnum * sizeof(char *));
		
			for (i_attnum = 0; i_attnum < attnum; i_attnum++)
			{
				/*if (result->tts_values[i_attnum] != (Datum)0)*/
				if (result->tts_isnull[i_attnum] == false)
				{
					tuples->slot[i_tuple][i_attnum] = text_to_cstring(DatumGetTextP(result->tts_values[i_attnum]));
				}
				else
				{
					tuples->slot[i_tuple][i_attnum] = NULL;
				}
			}
			tuples->slot_count++;

			result = ExecRemoteQuery((PlanState *) pstate);
			i_tuple++;
		}
	}
	else
	{
		elog(LOG, "pg_clean: node %s is not healthy", get_pgxc_nodename(node));
	}
	ExecEndRemoteQuery(pstate);
#endif
	return issuccess == true ? (Datum) 1 : (Datum) 0;
}

static bool check_node_health(Oid node_oid)
{
	int i;
	bool ishealthy = false;
	
	PoolPingNodeRecheck(node_oid);
	PgxcNodeGetHealthMap(cn_node_list, dn_node_list,
	                     &cn_nodes_num, &dn_nodes_num,
	                     cn_health_map, dn_health_map);
	if (get_pgxc_nodetype(node_oid) == 'C')
	{
		for (i = 0; i < cn_nodes_num; i++)
		{
			if (cn_node_list[i] == node_oid)
			{
				ishealthy = cn_health_map[i];
			}
		}
	}
	else
	{
		for (i = 0; i < dn_nodes_num; i++)
		{
			if (dn_node_list[i] == node_oid)
			{
				ishealthy = dn_health_map[i];
			}
		}
	}
	return ishealthy;
}

static void getDatabaseList(void)
{
	int i;
	TupleTableSlots result_db;
	const char *query_db = "select datname::text from pg_catalog.pg_database";
	/*add datname into tail of head_database_info*/
	if (execute_query_on_single_node(my_nodeoid, query_db, 1, &result_db) == (Datum) 1)
	{
		for (i = 0; i < result_db.slot_count; i++)
		{
			if (TTSgetvalue(&result_db, i, 0))
			{
				add_database_info(TTSgetvalue(&result_db, i, 0));
			}
		}
	}
	else
	{
		elog(ERROR, "pg_clean: failed to query database list on node %s, sql: %s",
			get_pgxc_nodename(my_nodeoid), query_db);
	}
	DropTupleTableSlots(&result_db);
}

/* 
 * TTSgetvalue -- get attribute from TupleTableSlots
 * input: 	result, index of tuple, index of field
 * return:	attribute result
 */
static char * TTSgetvalue(TupleTableSlots *result, int tup_num, int field_num)
{
	return result->slot[tup_num][field_num];
}

static void DropTupleTableSlots(TupleTableSlots *
Slots)
{
	int i;
	int j;
	for (i = 0; i < Slots->slot_count; i++)
	{
		if (Slots->slot[i])
		{
			for (j = 0; j < Slots->attnum; j++)
			{
				if (Slots->slot[i][j])
				{
					pfree(Slots->slot[i][j]);
				}
			}
			pfree(Slots->slot[i]);
		}
	}
	RFREE(Slots->slot);
	Slots->attnum = 0;
	return;
}

static void getTxnInfoOnNodesAll(void)
{
	int i;
	current_time = GetCurrentTimestamp();
	current_gts = GetGlobalTimestampGTM();
	if (!GlobalTimestampIsValid(current_gts))
	{
		/*get invalid gts, get txn info error*/
		elog(ERROR, "getTxnInfoOnNodesAll, get invalid gts");
	}
	/*upload 2PC transaction from CN*/
	for (i = 0; i < cn_nodes_num; i++)
	{
        if (total_twopc_txn >= MAX_TWOPC_TXN)
            return;
		getTxnInfoOnNode(cn_node_list[i]);
	}

	/*upload 2PC transaction from DN*/
	for (i = 0; i < dn_nodes_num; i++)
	{
        if (total_twopc_txn >= MAX_TWOPC_TXN)
            return;
		getTxnInfoOnNode(dn_node_list[i]);
	}
}

void getTxnInfoOnNode(Oid node)
{
	int i;
	TupleTableSlots result_txn;
	Datum execute_res;
	char query_txn_status[LONG_CMD_LENGTH];
	int interval_sec = DEFAULT_CHECK_TIME_INTERVAL;

	if (interval_sec < clean_time_interval/USECS_PER_SEC/2)
	{
		interval_sec = clean_time_interval/USECS_PER_SEC/2;
	}

	if (execute)
	{
		snprintf(query_txn_status, LONG_CMD_LENGTH,
			"select transaction::text, gid::text, owner::text, database::text, "
			"timestamptz_out(prepared)::text from pg_catalog.pg_prepared_xacts "
			"where prepared < clock_timestamp() - interval '%d secs' "
			"and database = '%s' order by prepared", interval_sec,
			get_database_name(MyDatabaseId));
	}
	else
	{
		snprintf(query_txn_status, LONG_CMD_LENGTH,
			"select transaction::text, gid::text, owner::text, database::text, "
			"timestamptz_out(prepared)::text from pg_catalog.pg_prepared_xacts "
			"where prepared < clock_timestamp() - interval '%d secs' "
			"order by prepared", interval_sec);
	}

	execute_res = execute_query_on_single_node(node, query_txn_status, 5, &result_txn);
	if (execute_res == (Datum) 1)
	{
		for (i = 0; i < result_txn.slot_count; i++)
		{
			uint32	xid;
			char*	gid;
			char*	owner;
			char*	datname;
			TimestampTz	prepared_time;
			
			/*read results from each tuple*/
			xid		= strtoul(TTSgetvalue(&result_txn, i, 0), NULL, 10);
			gid		= TTSgetvalue(&result_txn, i, 1);
			owner	= TTSgetvalue(&result_txn, i, 2);
			datname	= TTSgetvalue(&result_txn, i, 3);
			prepared_time = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
												CStringGetDatum(TTSgetvalue(&result_txn, i, 4)),
												ObjectIdGetDatum(InvalidOid),
												Int32GetDatum(-1)));

			if (gid == NULL)
			{
				elog(ERROR, "node(%d) gid is null, xid: %d", node, xid);
			}
			else if (owner == NULL)
			{
				elog(ERROR, "node(%d) owner is null, xid: %d, gid: %s",
					node, xid, gid);
			}
			else if (datname == NULL)
			{
				elog(ERROR, "node(%d) db name is null, xid: %d, gid: %s, owner: %s",
					node, xid, gid, owner);
			}

			/*add txn to database*/
			add_txn_info(datname, node, xid, gid, owner, prepared_time, TXN_STATUS_PREPARED);
            if (total_twopc_txn >= MAX_TWOPC_TXN)
            {
                break;
            }
		}
	}
	else
	{
		elog(ERROR, "pg_clean: failed to query prepared xacts on node %s, sql: %s",
			get_pgxc_nodename(node), query_txn_status);
	}
	DropTupleTableSlots(&result_txn);
}

txn_info *add_txn_info(char* dbname, Oid node_oid, uint32 xid, char * gid,
						char * owner, TimestampTz prepared_time, TXN_STATUS status)
{
	txn_info *txn = NULL;
	int	nodeidx;

	if ((txn = find_txn(gid)) == NULL)
	{
		txn = make_txn_info(dbname, gid, owner);
        total_twopc_txn++;
		if (txn == NULL)
		{
			/*no more memory*/
			elog(ERROR, "there is no more memory for palloc a 2PC transaction");
		}
	}
	nodeidx = find_node_index(node_oid);
	txn->txn_stat[nodeidx] = status;
	txn->xid[nodeidx] = xid;
	txn->prepare_timestamp[nodeidx] = prepared_time;
	if (nodeidx < cn_nodes_num)
	{
		txn->coordparts[nodeidx] = 1;
		txn->num_coordparts++;
	}
	else
	{
		txn->dnparts[nodeidx-cn_nodes_num] = 1;
		txn->num_dnparts++;
	}
	return txn;
}

/* search the first prepared node, then fetch trace id from this node */
static bool
GetGlobalTraceIdOnNode(const txn_info *txn)
{
	int i = 0;
	Oid oid = InvalidOid;
	txn_info dummy_info;
	bool res = false;
	if (NULL != txn)
	{
		for (i = 0; i < cn_nodes_num; i++)
		{
			// if true means this cn node is prepared, and will have a xlog file
			if (txn->coordparts[i])
			{
				oid = cn_node_list[i];
				break;
			}
		}
		for (i = 0; i < dn_nodes_num && InvalidOid == oid; i++)
		{
			if (txn->dnparts[i])
			{
				oid = dn_node_list[i];
				break;
			}
		}
		if (InvalidOid == oid)
		{
			elog(WARNING, "2pc clean faile to get node oid, gid: %s", txn->gid);
		}
		else
		{
			strncpy(dummy_info.gid, txn->gid, strlen(txn->gid) + 1);
			res = GetGlobalTraceIdOnPartNodes(&dummy_info, oid);
			elog(DEBUG5, "2pc get globaltraceid:cn oid: %u,gid: %s,traceid: %s",
				 cn_node_list[i], txn->gid, GlobalTraceId);
		}
	}
	return res;
}

static bool
GetGlobalTraceIdImpl(txn_info *txn, char *traceid)
{
	int sz = 0;
	bool res = false;
	if (NULL != traceid && NULL != txn)
	{
		traceid += strlen(GET_GLOBAL_TRACE_ID);
		traceid = strtok(traceid, "\n");
		if (NULL != traceid)
		{
			sz = strlen(traceid) + 1;
			if (NAMEDATALEN * 2 <= sz)
			{
				elog(WARNING, "g_trace id inside 2pc file is illegal, \
					 size is: %d, it is: %s", sz, traceid);
			}
			else
			{
				pgxc_node_set_global_traceid(traceid);
				/* make the trace id do not be overwrited */
				pgxc_node_set_global_traceid_regenerate(false);
				res = true;
			}
			elog(DEBUG5, "2pc recover global trace id 1: %s-%s, sz: %d",
				 traceid, GlobalTraceId, sz);
		}
	}
	return res;
}

static bool
GetGlobalTraceIdOnPartNodes(txn_info *txn, Oid node_oid)
{
	TupleTableSlots result;
    char *file_content = NULL;
	char *traceid = NULL;
	bool res = false;
	StringInfoData query_2pc_file;
	initStringInfo(&query_2pc_file);
	appendStringInfo(&query_2pc_file,"select public.pgxc_get_2pc_file('%s')::text",
					 txn->gid);
    
	elog(DEBUG5, "2pc recover stmt-gid : %s-%s, node oid: %d",
	     query_2pc_file.data, txn->gid, node_oid);
	if (execute_query_on_single_node(node_oid, query_2pc_file.data, 1, &result) == (Datum) 1)
	{
		if (result.slot_count && TTSgetvalue(&result, 0, 0))
		{
            file_content = TTSgetvalue(&result, 0, 0);    
			traceid = strstr(file_content, GET_GLOBAL_TRACE_ID);
			res = GetGlobalTraceIdImpl(txn, traceid);
		}
		DropTupleTableSlots(&result);
	}
	return res;
}

TWOPHASE_FILE_STATUS GetTransactionPartNodes(txn_info *txn, Oid node_oid)
{
	/*get all the participates and initiate to each transactions*/
	TWOPHASE_FILE_STATUS res = TWOPHASE_FILE_NOT_EXISTS;
	TupleTableSlots result;
	char *partnodes = NULL;
    char *startnode = NULL;
    char *file_content = NULL;
    uint32 startxid = 0;
    char *str_startxid = NULL;
    char *str_prepare_gts = NULL;
    char *str_timestamp = NULL;
	char *temp = NULL;
	char *save_ptr = NULL;
	Oid	 temp_nodeoid;
	char temp_nodetype;
	int  temp_nodeidx;
	char stmt[MAX_CMD_LENGTH];
	static const char *STMT_FORM = "select public.pgxc_get_2pc_file('%s')::text";
	snprintf(stmt, MAX_CMD_LENGTH, STMT_FORM, txn->gid);
    
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) == (Datum) 1)
	{
		if (result.slot_count && TTSgetvalue(&result, 0, 0))
#if 0
            TTSgetvalue(&result, 0, 0) && 
            TTSgetvalue(&result, 0, 1) && 
            TTSgetvalue(&result, 0, 2))
#endif
		{
            file_content = TTSgetvalue(&result, 0, 0);    

            if (strlen(file_content) == 0)
            {
                elog(LOG, "gid: %s, 2pc file is not exist", txn->gid);
	            return TWOPHASE_FILE_NOT_EXISTS;
            }

            if (!IsXidImplicit(txn->gid) && strstr(file_content, GET_READONLY))
            {
                txn->is_readonly = true;
                txn->global_txn_stat = TXN_STATUS_COMMITTED;
                DropTupleTableSlots(&result);
	            return TWOPHASE_FILE_EXISTS;
            }
            startnode = strstr(file_content, GET_START_NODE);
            str_startxid = strstr(file_content, GET_START_XID);
            str_prepare_gts = strstr(file_content, GET_PREPARE_TIMESTAMP);
            partnodes = strstr(file_content, GET_NODE);
            temp = strstr(file_content, GET_COMMIT_TIMESTAMP);
            
            /* get the last global_commit_timestamp */
            while (temp)
            {
                str_timestamp = temp;
                temp += strlen(GET_COMMIT_TIMESTAMP);
                temp = strstr(temp, GET_COMMIT_TIMESTAMP);
            }

            /* get start node name */
            if (startnode)
            {
                startnode += strlen(GET_START_NODE);
                startnode = strtok(startnode, "\n");
                txn->origcoord = get_pgxc_nodeoid(startnode);
                if (!OidIsValid(txn->origcoord))
                {
                    elog(WARNING, "2pc(%s) get start node(%s) oid failed "
                        "on node(%s), maybe node(%s) is removed from the cluster",
                        txn->gid, startnode, get_pgxc_nodename(node_oid), startnode);
                }
            }

            /* get start xid */
            if (str_startxid)
            {
                str_startxid += strlen(GET_START_XID);
                str_startxid = strtok(str_startxid, "\n");
                startxid = strtoul(str_startxid, NULL, 10);
                txn->startxid = startxid;
            }

            /* get participated nodes */
            if (partnodes)
            {
                partnodes += strlen(GET_NODE);
                partnodes = strtok(partnodes, "\n");
                txn->participants = (char *) palloc0(strlen(partnodes) + 1);
                strncpy(txn->participants, partnodes, strlen(partnodes) + 1);
            }
            
            if (NULL == startnode || NULL == str_startxid)
            {
                res = TWOPHASE_FILE_OLD;
                DropTupleTableSlots(&result);
                return res;
            }

            if (NULL == partnodes)
            {
                res = TWOPHASE_FILE_ERROR;
                DropTupleTableSlots(&result);
                return res;
            }

            /* get prepare gts */
            if (str_prepare_gts)
            {
                str_prepare_gts += strlen(GET_PREPARE_TIMESTAMP);
                str_prepare_gts = strtok(str_prepare_gts, "\n");
                txn->global_prepare_timestamp = strtoull(str_prepare_gts, NULL, 10);
            }
            else
            {
                txn->global_prepare_timestamp = InvalidGlobalTimestamp;
            }

            /* get commit gts */
            if (str_timestamp)
            {
                str_timestamp += strlen(GET_COMMIT_TIMESTAMP);
                str_timestamp = strtok(str_timestamp, "\n");
                txn->global_commit_timestamp = strtoull(str_timestamp, NULL, 10);
            }
            else
            {
                txn->global_commit_timestamp = InvalidGlobalTimestamp;
            }

            elog(DEBUG1, "get 2pc txn: %s, partnodes in nodename: %s(nodeoid:%u), "
                "partnodes: (%s), startnode: %s(startnodeoid: %u), startxid: %u, "
                "global_prepare_timestamp: %ld, global_commit_timestamp: %ld", 
                txn->gid, get_pgxc_nodename(node_oid), node_oid,
                partnodes, startnode, txn->origcoord, startxid,
                txn->global_prepare_timestamp, txn->global_commit_timestamp);

            /* in explicit transaction startnode participate the transaction */
            if (strstr(partnodes, startnode) || !IsXidImplicit(txn->gid))
            {
                txn->isorigcoord_part = true;
            }
            else
            {
                txn->isorigcoord_part = false;
            }
            
			res = TWOPHASE_FILE_EXISTS;
			txn->num_coordparts = 0;
			txn->num_dnparts = 0;
			temp = strtok_r(partnodes, ",", &save_ptr);
			while(temp)
			{
				/*check node type*/
				temp_nodeoid = get_pgxc_nodeoid(temp);
				if (!OidIsValid(temp_nodeoid))
				{
					if (is_for_node_removed)
					{
						if (execute)
						{
							elog(WARNING, "2pc(%s) get node(%s) oid failed "
								"on node(%s), maybe node(%s) is removed "
								"from the cluster, skip node(%s) when using "
								"pg_clean_execute_for_node_removed(%ld)",
								txn->gid, temp, get_pgxc_nodename(node_oid), temp,
								temp, clean_time_interval/USECS_PER_SEC);
						}
						else
						{
							elog(WARNING, "2pc(%s) get node(%s) oid failed "
								"on node(%s), maybe node(%s) is removed "
								"from the cluster, skip node(%s) when using "
								"pg_clean_check_txn_for_node_removed(%ld)",
								txn->gid, temp, get_pgxc_nodename(node_oid), temp,
								temp, clean_time_interval/USECS_PER_SEC);
						}

						temp = strtok_r(NULL, ",", &save_ptr);
						continue;
					}

					if (execute)
					{
						elog(WARNING, "2pc(%s) get node(%s) oid failed "
							"on node(%s), if node(%s) is removed "
							"from the cluster, you can use "
							"pg_clean_execute_for_node_removed(%ld) "
							"to clean the 2pc",
							txn->gid, temp, get_pgxc_nodename(node_oid),
							temp, clean_time_interval/USECS_PER_SEC);
					}
					else
					{
						elog(WARNING, "2pc(%s) get node(%s) oid failed "
							"on node(%s), if node(%s) is removed "
							"from the cluster, you can use "
							"pg_clean_check_txn_for_node_removed(%ld) "
							"to check the 2pc",
							txn->gid, temp, get_pgxc_nodename(node_oid),
							temp, clean_time_interval/USECS_PER_SEC);
					}

					res = TWOPHASE_FILE_ERROR;
					break;
				}
				temp_nodetype = get_pgxc_nodetype(temp_nodeoid);
				temp_nodeidx = find_node_index(temp_nodeoid);
				
				switch (temp_nodetype)
				{
					case 'C':
						txn->coordparts[temp_nodeidx] = 1;
						txn->num_coordparts++;
						break;
					case 'D':
						txn->dnparts[temp_nodeidx-cn_nodes_num] = 1;
						txn->num_dnparts++;
						break;
					default:
						elog(ERROR,"nodetype of %s is not 'C' or 'D'", temp);
						break;
				}
				temp = strtok_r(NULL, ",", &save_ptr);
			}
		}
	}
	else
	{
		elog(ERROR, "pg_clean: failed to query txn info on node %s, sql: %s",
			get_pgxc_nodename(node_oid), stmt);
	}
	DropTupleTableSlots(&result);
	return res;
}

static txn_info *find_txn(char *gid)
{
  bool found;
  database_info *cur_db;
  txn_info *txn;

  for (cur_db = head_database_info; cur_db; cur_db = cur_db->next)
  {
#if 0
	  for (cur_txn = cur_db->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	  {
		  if (0 == strcmp(cur_txn->gid, gid))
			  return cur_txn;
	  }
#endif
      txn = (txn_info *)hash_search(cur_db->all_txn_info, (void *)gid, HASH_FIND, &found);
      if (found)
        return txn;
  }
  return NULL;
}

txn_info* make_txn_info(char* dbname, char* gid, char* owner)
{
    bool found;
    txn_info *txn_insert_pos = NULL;
	database_info *dbinfo;
	txn_info *txn;

	dbinfo = add_database_info(dbname);
	txn = (txn_info *)palloc0(sizeof(txn_info));
	if (txn == NULL)
		return NULL;

	/* Normally gid and owner should be valid for every prepared txn, here we check for that */
	if (gid == NULL || owner == NULL)
	{
		elog(ERROR, "pg_clean always expect valid gid and owner on every transaction.");
		/* silence the compiler, should not get here */
		return NULL;
	}
	
	if (strlen(gid) >= MAX_GID)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("transaction 2pc gid is too long: \"%s\"", gid)));

	strncpy(txn->gid, gid, strlen(gid)+1);
	txn->owner = (char *)palloc0(strlen(owner)+1);
	strncpy(txn->owner, owner, strlen(owner)+1);
	
	txn->database = pstrdup(dbname);

	txn->txn_stat = (TXN_STATUS *)palloc0(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->xid = (uint32 *)palloc0(sizeof(uint32) * pgxc_clean_node_count);
	txn->prepare_timestamp = (TimestampTz *)palloc0(sizeof(TimestampTz) * pgxc_clean_node_count);
	txn->coordparts = (int *)palloc0(cn_nodes_num * sizeof(int));
	
	txn->dnparts = (int *)palloc0(dn_nodes_num * sizeof(int));
	if (txn->gid == NULL || txn->owner == NULL || txn->txn_stat == NULL || txn->database == NULL
		|| txn->xid == NULL || txn->coordparts == NULL || txn->dnparts == NULL || txn->prepare_timestamp == NULL)
	{
		pfree(txn);
		return(NULL);
	}

    txn_insert_pos = (txn_info *)hash_search(dbinfo->all_txn_info, 
                   (void *)txn->gid, HASH_ENTER, &found);
    if (!found)
        memcpy(txn_insert_pos, txn, sizeof(txn_info));

#if 0        
	if (dbinfo->head_txn_info == NULL)
	{
		dbinfo->head_txn_info = dbinfo->last_txn_info = txn;
	}
	else
	{
		dbinfo->last_txn_info->next = txn;
		dbinfo->last_txn_info = txn;
	}
#endif

	return txn_insert_pos;
}

database_info *find_database_info(char *database_name)
{
	database_info *cur_database_info = head_database_info;

	for (;cur_database_info; cur_database_info = cur_database_info->next)
	{
		if(cur_database_info->database_name &&
		   database_name && 
		   strcmp(cur_database_info->database_name, database_name) == 0)
			return(cur_database_info);
	}
	return(NULL);
}

database_info *add_database_info(char *database_name)
{
	database_info *rv;
    HASHCTL txn_ctl;
    char tabname[STRING_BUFF_LEN];

	if ((rv = find_database_info(database_name)) != NULL)
		return rv;		/* Already in the list */
	rv = (database_info *)palloc0(sizeof(database_info));
	if (rv == NULL)
		return NULL;
	rv->next = NULL;
	rv->database_name = (char *)palloc0(strlen(database_name) + 1);
	strncpy(rv->database_name, database_name, strlen(database_name) + 1);
	if (rv->database_name == NULL)
	{
		pfree(rv);
		return NULL;
	}
#if 0    
	rv->head_txn_info = NULL;
	rv->last_txn_info = NULL;
#endif

    snprintf(tabname, STRING_BUFF_LEN, "%s txn info", rv->database_name);
    txn_ctl.keysize = MAX_GID;
    txn_ctl.entrysize = sizeof(txn_info); 
    rv->all_txn_info = hash_create(tabname, 64, 
                                   &txn_ctl, HASH_ELEM);
	if (head_database_info == NULL)
	{
		head_database_info = last_database_info = rv;
		return rv;
	}
	else
	{
		last_database_info->next = rv;
		last_database_info = rv;
		return rv;
	}
}

int find_node_index(Oid node_oid)
{
	int res = -1;
	int i;
	if (get_pgxc_nodetype(node_oid) == 'C')
	{
		for (i = 0; i < cn_nodes_num; i++)
		{
			if (node_oid == cn_node_list[i])
			{
				res = i;
				break;
			}
		}
	}
	else
	{
		for (i = 0; i < dn_nodes_num; i++)
		{
			if (node_oid == dn_node_list[i])
			{
				res = i+cn_nodes_num;
				break;
			}
		}
	}
	return res;
}

Oid find_node_oid(int node_idx)
{
	return (node_idx < cn_nodes_num) ? cn_node_list[node_idx] :
									   dn_node_list[node_idx-cn_nodes_num];
}


/*
 * getAllTxnInfoFrom2pcFiles
 * Get all 2pc txn info from 2pc files
 */
void getAllTxnInfoFrom2pcFiles(void)
{
	int i = 0;
	TupleTableSlots *result;
	const char *stmt = "select public.pgxc_get_record_list()::text";
	char *twopcfiles = NULL;
	char *ptr = NULL;
	char *save_ptr = NULL;

	result = (TupleTableSlots *)palloc0(sizeof(TupleTableSlots) *
										pgxc_clean_node_count);

	/* collect the 2pc files on nodes */
	for (i = 0; i < cn_nodes_num; i++)
	{
		execute_query_on_single_node(cn_node_list[i], stmt, 1, result + i);
	}

	for (i = 0; i < dn_nodes_num; i++)
	{
		execute_query_on_single_node(dn_node_list[i], stmt, 1,
									result + cn_nodes_num + i);
	}

	/* read 2pc files in each cn */
	for (i = 0; i < cn_nodes_num; i++)
	{
		if (0 == result[i].slot_count)
		{
			continue;
		}

		twopcfiles = TTSgetvalue(result + i, 0, 0);
		if (twopcfiles == NULL)
		{
			continue;
		}

		/* iterate through all 2pc files */
		ptr = strtok_r(twopcfiles, ",", &save_ptr);
		for (; ptr != NULL; ptr = strtok_r(NULL, ",", &save_ptr))
		{
			elog(DEBUG2, "2pc file(%s) on node %s",
				ptr, get_pgxc_nodename(cn_node_list[i]));

			/* whether 2pc is rollbacked? */
			if (strstr(ptr, ROLLBACK_POSTFIX) != NULL)
			{
				/* 2pc is rollbacked */
				continue;
			}

			/* query txn info from 2pc file */
			getTxnInfoFrom2pcFile(ptr, cn_node_list[i]);
		}
	}

	/* read 2pc files in each dn */
	for (i = 0; i < dn_nodes_num; i++)
	{
		if (0 == result[cn_nodes_num + i].slot_count)
		{
			continue;
		}

		twopcfiles = TTSgetvalue(result + cn_nodes_num + i, 0, 0);
		if (twopcfiles == NULL)
		{
			continue;
		}

		/*iterate through all 2pc files*/
		ptr = strtok_r(twopcfiles, ",", &save_ptr);
		for (; ptr != NULL; ptr = strtok_r(NULL, ",", &save_ptr))
		{
			elog(DEBUG2, "2pc file(%s) on node %s",
				ptr, get_pgxc_nodename(dn_node_list[i]));

			/* whether 2pc is rollbacked? */
			if (strstr(ptr, ROLLBACK_POSTFIX) != NULL)
			{
				/* 2pc is rollbacked */
				continue;
			}

			/*query txn info from 2pc file*/
			getTxnInfoFrom2pcFile(ptr, dn_node_list[i]);
		}
	}

	for (i = 0; i < pgxc_clean_node_count; i++)
	{
		DropTupleTableSlots(result+i);
	}
}

void getTxnInfoFrom2pcFile(char *file_name, Oid node_oid)
{
	TupleTableSlots result;
	txn_info *txn = NULL;
	char *file_content = NULL;

	char *startnode = NULL;
	char *str_startxid = NULL;
	uint32 startxid = 0;
	char *partnodes = NULL;
	char *str_xid = NULL;
	uint32 xid = 0;
	char *database = NULL;
	char *user = NULL;
	char *str_prepare_gts = NULL;
	GlobalTimestamp prepare_gts = InvalidGlobalTimestamp;
	char *str_timestamp = NULL;
	GlobalTimestamp commit_gts = InvalidGlobalTimestamp;

	Oid temp_nodeoid = InvalidOid;
	char *temp = NULL;
	char temp_nodetype;
	int temp_nodeidx;

	char *gid = file_name;

	char stmt[MAX_CMD_LENGTH];
	TXN_STATUS txn_status = TXN_STATUS_PREPARED;

	char *node_name = NULL;

	static const char *stmt_fmt = "select public.pgxc_get_2pc_file('%s')::text";

	node_name = get_pgxc_nodename(node_oid);
	if (node_name == NULL)
	{
		elog(ERROR, "failed to get node name, file: %s, oid: %d",
			file_name, node_oid);
		return;
	}

	snprintf(stmt, MAX_CMD_LENGTH, stmt_fmt, file_name);
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) != (Datum) 1)
	{
		elog(WARNING, "failed to get 2pc file(%s) on node %s", file_name, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	if (result.slot_count == 0 || TTSgetvalue(&result, 0, 0) == NULL)
	{
		int elevel = is_for_deadlock ? LOG : ERROR;
		elog(elevel, "2pc file(%s) on node %s result is null", file_name, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	file_content = TTSgetvalue(&result, 0, 0);
	if (strlen(file_content) == 0)
	{
		elog(ERROR, "2pc file(%s) on node %s is empty", file_name, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	if (strstr(file_content, GET_READONLY))
	{
		elog(NOTICE, "2pc(%s) is readonly on node %s", gid, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	if (!IsXidImplicit(gid))
	{
		elog(LOG, "2pc(%s) is explicit on node %s", gid, node_name);
	}

	startnode = strstr(file_content, GET_START_NODE);
	str_startxid = strstr(file_content, GET_START_XID);
	partnodes = strstr(file_content, GET_NODE);
	str_xid = strstr(file_content, GET_XID);
	str_prepare_gts = strstr(file_content, GET_PREPARE_TIMESTAMP);
	database = strstr(file_content, GET_DATABASE);
	user = strstr(file_content, GET_USER);
	temp = strstr(file_content, GET_COMMIT_TIMESTAMP);

	/* get the last global_commit_timestamp */
	while (temp)
	{
		str_timestamp = temp;
		temp += strlen(GET_COMMIT_TIMESTAMP);
		temp = strstr(temp, GET_COMMIT_TIMESTAMP);
	}

	/* get start node name */
	if (startnode)
	{
		startnode += strlen(GET_START_NODE);
		startnode = strtok(startnode, "\n");
	}

	/* get start xid */
	if (str_startxid)
	{
		str_startxid += strlen(GET_START_XID);
		str_startxid = strtok(str_startxid, "\n");
		startxid = strtoul(str_startxid, NULL, 10);
	}

	if (NULL == startnode || NULL == str_startxid)
	{
		elog(ERROR, "2pc file(%s) on node %s lost startnode or startxid",
			file_name, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	/* get participated nodes */
	if (partnodes)
	{
		partnodes += strlen(GET_NODE);
		partnodes = strtok(partnodes, "\n");
	}

	/* get xid */
	if (str_xid)
	{
		str_xid += strlen(GET_XID);
		str_xid = strtok(str_xid, "\n");
		xid = strtoul(str_xid, NULL, 10);
	}

	if (NULL == partnodes || NULL == str_xid)
	{
		elog(ERROR, "2pc file(%s) on node %s lost nodes or xid",
			file_name, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	/* get database */
	if (database)
	{
		database += strlen(GET_DATABASE);
		database = strtok(database, "\n");
	}

	/* get user */
	if (user)
	{
		user += strlen(GET_USER);
		user = strtok(user, "\n");
	}

	if (NULL == database || NULL == user)
	{
		elog(WARNING, "2pc file(%s) on node %s lost database or user",
			file_name, node_name);
		DropTupleTableSlots(&result);
		return;
	}

	if (0 == startxid || 0 == xid)
	{
		elog(WARNING, "2pc file(%s) on node %s startxid(%d) or xid(%d) invalid",
			file_name, node_name, startxid, xid);
		DropTupleTableSlots(&result);
		return;
	}

	/* get prepare gts */
	if (str_prepare_gts)
	{
		str_prepare_gts += strlen(GET_PREPARE_TIMESTAMP);
		str_prepare_gts = strtok(str_prepare_gts, "\n");
		prepare_gts = strtoull(str_prepare_gts, NULL, 10);
	}
	else
	{
		prepare_gts = InvalidGlobalTimestamp;
	}

	/* get commit gts */
	if (str_timestamp)
	{
		str_timestamp += strlen(GET_COMMIT_TIMESTAMP);
		str_timestamp = strtok(str_timestamp, "\n");
		commit_gts = strtoull(str_timestamp, NULL, 10);
	}
	else
	{
		commit_gts = InvalidGlobalTimestamp;
	}

	elog(DEBUG2, "2pc(%s) on %s(nodeoid:%u): startnode(%s), startxid(%u), "
		"partnodes(%s), xid(%u), prepare_gts(%ld), "
		"database(%s), user(%s), commit_gts(%ld)",
		gid, node_name, node_oid, startnode, startxid,
		partnodes, xid, prepare_gts, database, user, commit_gts);

	txn = add_txn_info(database, node_oid, xid, gid, user, 0, txn_status);

	txn->origcoord = get_pgxc_nodeoid(startnode);
	if (!OidIsValid(txn->origcoord))
	{
		elog(WARNING, "2pc(%s) get start node(%s) oid failed on node(%s), "
			"maybe node(%s) is removed from the cluster",
			txn->gid, startnode, node_name, startnode);
	}
	txn->startxid = startxid;
	txn->participants = pstrdup(partnodes);
	if (prepare_gts != InvalidGlobalTimestamp)
	{
		txn->global_prepare_timestamp = prepare_gts;
	}
	if (commit_gts != InvalidGlobalTimestamp)
	{
		txn->global_commit_timestamp = commit_gts;
	}

	/* in explicit transaction startnode participate the transaction */
	if (strstr(partnodes, startnode) || !IsXidImplicit(txn->gid))
	{
		txn->isorigcoord_part = true;
	}
	else
	{
		txn->isorigcoord_part = false;
	}

	txn->num_coordparts = 0;
	txn->num_dnparts = 0;
	temp = strtok(partnodes, ",");
	while(temp)
	{
		/*check node type*/
		temp_nodeoid = get_pgxc_nodeoid(temp);
		if (!OidIsValid(temp_nodeoid))
		{
			elog(WARNING, "2pc(%s) get node(%s) oid failed on node(%s)",
				gid, temp, node_name);
			break;
		}
		temp_nodetype = get_pgxc_nodetype(temp_nodeoid);
		temp_nodeidx = find_node_index(temp_nodeoid);

		switch (temp_nodetype)
		{
			case 'C':
				txn->coordparts[temp_nodeidx] = 1;
				txn->num_coordparts++;
				break;
			case 'D':
				txn->dnparts[temp_nodeidx - cn_nodes_num] = 1;
				txn->num_dnparts++;
				break;
			default:
				elog(ERROR, "nodetype of %s is not 'C' or 'D'", temp);
				break;
		}
		temp = strtok(NULL, ",");
	}

	DropTupleTableSlots(&result);
}

void getTxnInfoOnOtherNodesAll(void)
{
	database_info *cur_database;

	for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
	{
		getTxnInfoOnOtherNodesForDatabase(cur_database);
	}
}

void getTxnInfoOnOtherNodesForDatabase(database_info *database)
{
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn = database->all_txn_info;
	hash_seq_init(&status, txn);

    while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
    {
		getTxnInfoOnOtherNodes(cur_txn);
    }
#if 0
	for (cur_txn = database->head_txn_info; cur_txn; cur_txn = cur_txn->next)
	{
		getTxnInfoOnOtherNodes(cur_txn);
	}
#endif
}

void getTxnInfoOnOtherNodes(txn_info *txn)
{
    int ii;
    int ret;
    char node_type;
    TWOPHASE_FILE_STATUS status = TWOPHASE_FILE_NOT_EXISTS;
    Oid node_oid = InvalidOid;
    uint32 transactionid = 0;
    char gid[MAX_GID];
    char *ptr = NULL;

	if (!GetGlobalTraceIdOnNode(txn))
	{
		elog(WARNING, "failed to get global trace id inside pg clean, gid: %s", txn->gid);
	}
	else
	{
		elog(DEBUG5, "get global trace id inside pg clean, gid: %s, trace id: %s-%s",
			 txn->gid, pgxc_get_global_trace_id(), GlobalTraceId);
	}

	if (IsXidImplicit(txn->gid))
	{
		strncpy(gid, txn->gid, strlen(txn->gid) + 1);
		ptr = strtok(gid, ":");
		ptr = strtok(NULL, ":");
		node_oid = get_pgxc_nodeoid(ptr);
		if (OidIsValid(node_oid))
		{
			status = GetTransactionPartNodes(txn, node_oid);
		}
		else
		{
			elog(WARNING, "2pc(%s) get start node(%s) oid failed on node(%s), "
				"maybe node(%s) is removed from the cluster",
				txn->gid, ptr, PGXCNodeName, ptr);

			status = TWOPHASE_FILE_NOT_EXISTS;
		}
	}

    if (status == TWOPHASE_FILE_NOT_EXISTS)
    {
        for (ii = 0; ii < cn_nodes_num + dn_nodes_num; ii++)
        {
            if (ii < cn_nodes_num)
            {
                status = GetTransactionPartNodes(txn, cn_node_list[ii]);
                if (TWOPHASE_FILE_EXISTS == status || 
                    TWOPHASE_FILE_OLD == status || 
                    TWOPHASE_FILE_ERROR == status)
                {
                    node_oid = cn_node_list[ii];
                    break;
                }
            }
            else
            {
                status = GetTransactionPartNodes(txn, dn_node_list[ii - cn_nodes_num]);
                if (TWOPHASE_FILE_EXISTS == status || 
                    TWOPHASE_FILE_OLD == status || 
                    TWOPHASE_FILE_ERROR == status)
                {
                    node_oid = dn_node_list[ii - cn_nodes_num];
                    break;
                }
            }
        }
        
        /* since there may be explicit readonly  twophase transactions */
        if (txn->is_readonly)
        {
            return;
        }

		if (TWOPHASE_FILE_EXISTS == status &&
			InvalidGlobalTimestamp == txn->global_commit_timestamp &&
			node_oid != txn->origcoord)
		{
			if (!is_for_node_removed)
			{
				status = GetTransactionPartNodes(txn, txn->origcoord);
			}
			else
			{
				if (OidIsValid(txn->origcoord))
				{
					elog(WARNING, "2pc(%s) start node oid for %s is valid",
						txn->gid, get_pgxc_nodename(txn->origcoord));

					status = GetTransactionPartNodes(txn, txn->origcoord);
				}
			}
		}
    }
    
    if (TWOPHASE_FILE_EXISTS != status)
    {
        /*
         * if 2pc file not exists in all nodes, the trans did not pass the prepared phase, 
         * 
         */
        txn->global_txn_stat = (TWOPHASE_FILE_NOT_EXISTS == status) ? 
                                TXN_STATUS_ABORTED : TXN_STATUS_UNKNOWN;
        return;
    }


    /* judge the range of global status */
    CheckFirstPhase(txn);

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		if (txn->txn_stat[ii] == TXN_STATUS_INITIAL)
		{
			/*check node ii is 'C' or 'D'*/
            node_oid = find_node_oid(ii);
            if (node_oid == txn->origcoord)
                continue;
			node_type = get_pgxc_nodetype(node_oid);
			if (node_type == 'C' && txn->coordparts[ii] != 1)
				continue;
			if (node_type == 'D' && txn->dnparts[ii - cn_nodes_num] != 1)
				continue;
			/*check coordparts or dnparts*/
			if (txn->xid[ii] == 0)
			{
                ret = Get2PCXidByGid(node_oid, txn->gid, &transactionid);
                if (ret == XIDFOUND)
                {
                    txn->xid[ii] = transactionid;
                    if (txn->xid[ii] > 0)
                        getTxnStatus(txn, ii);
                }
                else if (ret == XIDNOTFOUND)
                {
                    if (txn->after_first_phase)
                        txn->txn_stat[ii] = TXN_STATUS_COMMITTED;
                }
                else
                    txn->txn_stat[ii] = TXN_STATUS_UNKNOWN;

			}
		}
	}
}

/*get xid by gid on node_oid*/
int Get2PCXidByGid(Oid node_oid, char *gid, uint32 *transactionid)
{
    int ret = XIDFOUND;
	TupleTableSlots result;
	uint32 xid = 0;
	static const char *STMT_FORM = "select public.pgxc_get_2pc_xid('%s')::text;";
	char stmt[MAX_CMD_LENGTH];
	snprintf(stmt, MAX_CMD_LENGTH, STMT_FORM, gid);
	/*if exist get xid by gid on node_oid*/
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) != (Datum) 0)
	{
		if (result.slot_count)
		{
			if (TTSgetvalue(&result, 0, 0))
			{
				xid = strtoul(TTSgetvalue(&result, 0, 0), NULL, 10);
                *transactionid = xid;
                if (xid == 0)
                    ret = XIDNOTFOUND;
			}
            else
                ret = XIDNOTFOUND;
		}
		else
			ret = XIDNOTFOUND;
	}
	else
	{
		ret = XIDEXECFAIL;
		elog(ERROR, "pg_clean: failed to query xid on node %s, sql: %s",
			get_pgxc_nodename(node_oid), stmt);
	}
	DropTupleTableSlots(&result);
	return ret;
}

void getTxnStatus(txn_info *txn, int node_idx)
{
	Oid				node_oid;
	char			stmt[MAX_CMD_LENGTH];
	char			*att1;
	TupleTableSlots result;

	static const char *STMT_FORM = "select pg_catalog.pgxc_is_committed('%d'::xid)::text";
	snprintf(stmt, MAX_CMD_LENGTH, STMT_FORM, txn->xid[node_idx]);

	node_oid = find_node_oid(node_idx);
	if (execute_query_on_single_node(node_oid, stmt, 1, &result) != (Datum) 0)
	{
		att1 = TTSgetvalue(&result, 0, 0);
		
		if (att1)
		{
			if (strcmp(att1, "true") == 0)
			{
				txn->txn_stat[node_idx] = TXN_STATUS_COMMITTED;
				elog(DEBUG2, "gid: %s, xid(%d) is committed, att1: %s",
					txn->gid, txn->xid[node_idx], att1);
			}
			else
			{
				txn->txn_stat[node_idx] = TXN_STATUS_ABORTED;
				elog(DEBUG2, "gid: %s, xid(%d) is aborted, att1: %s",
					txn->gid, txn->xid[node_idx], att1);
			}
		}
		else
		{
            txn->txn_stat[node_idx] = TXN_STATUS_INITIAL;
		}
	}
	else
	{
		txn->txn_stat[node_idx] = TXN_STATUS_UNKNOWN;
		elog(ERROR, "pg_clean: failed to query txn status on node %s, sql: %s",
			get_pgxc_nodename(node_oid), stmt);
	}

	elog(DEBUG2, "gid: %s, xid(%d) status: %d",
		txn->gid, txn->xid[node_idx], txn->txn_stat[node_idx]);

	DropTupleTableSlots(&result);
}

char *get2PCInfo(const char *tid)
{
    char *result = NULL;
    char *info = NULL;
    int size = 0;
    File fd = -1;
    int ret = -1;
    struct stat filestate;
    char path[MAXPGPATH];

    if (strlen(tid) >= MAX_GID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("transaction 2pc gid is too long: \"%s\"", tid)));

    info = get_2pc_info_from_cache(tid);
    if (NULL != info)
    {
        size = strlen(info);
        result = (char *)palloc0(size + 1);
        memcpy(result, info, size);
        return result;
    }

    elog(DEBUG1, "try to get 2pc info from disk, tid: %s", tid);
    
    snprintf(path, MAXPGPATH, TWOPHASE_RECORD_DIR "/%s", tid);
    if(access(path, F_OK) == 0)
    {
    	if(stat(path, &filestate) == -1)
    	{
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not get status of file \"%s\"", path)));
    	}
        
        size = filestate.st_size;

        if (0 == size) 
        {
            return NULL;
        }

        result = (char *)palloc0(size + 1);

        fd = PathNameOpenFile(path, O_RDONLY);
    	if (fd < 0)
    	{   
            pfree(result);
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not open file \"%s\" for read", path)));
    	} 

        ret = FileRead(fd, result, size, WAIT_EVENT_BUFFILE_READ);
        if(ret != size)
    	{
            pfree(result);
    		ereport(ERROR,
    			(errcode_for_file_access(),
    			errmsg("could not read file \"%s\"", path)));
    	}

        FileClose(fd);
        return result;
    }

    return NULL;
}

/*
 * pgxc_get_2pc_file
 * Get 2pc file content
 */
Datum pgxc_get_2pc_file(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_file);
Datum pgxc_get_2pc_file(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    text *t_result = NULL;

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        t_result = cstring_to_text(result);
        pfree(result);
        return PointerGetDatum(t_result);
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_get_2pc_nodes
 * Get 2pc participants
 */
Datum pgxc_get_2pc_nodes(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_nodes);
Datum pgxc_get_2pc_nodes(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *nodename = NULL;
    text *t_result = NULL;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        nodename = strstr(result, GET_NODE);
        if (NULL != nodename)
        {
            nodename += strlen(GET_NODE);
            nodename = strtok(nodename, "\n");
            t_result = cstring_to_text(nodename);
            pfree(result);
            return PointerGetDatum(t_result);
        }
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_get_2pc_startnode
 * Get 2pc start node
 */
Datum pgxc_get_2pc_startnode(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_startnode);
Datum pgxc_get_2pc_startnode(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *nodename = NULL;
    text *t_result = NULL;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        nodename = strstr(result, GET_START_NODE);
        if (NULL != nodename)
        {
            nodename += strlen(GET_START_NODE);
            nodename = strtok(nodename, "\n");
            t_result = cstring_to_text(nodename);
            pfree(result);
            return PointerGetDatum(t_result);
        }
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_get_2pc_startxid
 * Get 2pc start xid
 */
Datum pgxc_get_2pc_startxid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_startxid);
Datum pgxc_get_2pc_startxid(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *startxid = NULL;
    text *t_result = NULL;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        startxid = strstr(result, GET_START_XID);
        if (NULL != startxid)
        {
            startxid += strlen(GET_START_XID);
            startxid = strtok(startxid, "\n");
            t_result = cstring_to_text(startxid);
            pfree(result);
            return PointerGetDatum(t_result);
        }
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_get_2pc_prepare_timestamp
 * Get 2pc prepare timestamp
 */
Datum pgxc_get_2pc_prepare_timestamp(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_prepare_timestamp);
Datum pgxc_get_2pc_prepare_timestamp(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *prepare_timestamp = NULL;
    text *t_result = NULL;

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        prepare_timestamp = strstr(result, GET_PREPARE_TIMESTAMP);
        if (NULL != prepare_timestamp)
        {
            prepare_timestamp += strlen(GET_PREPARE_TIMESTAMP);
            prepare_timestamp = strtok(prepare_timestamp, "\n");
            t_result = cstring_to_text(prepare_timestamp);
            pfree(result);
            return PointerGetDatum(t_result);
        }
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_get_2pc_commit_timestamp
 * Get 2pc commit timestamp
 */
Datum pgxc_get_2pc_commit_timestamp(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_commit_timestamp);
Datum pgxc_get_2pc_commit_timestamp(PG_FUNCTION_ARGS)
{
    char *tid = NULL;
    char *result = NULL;
    char *commit_timestamp = NULL;
    text *t_result = NULL;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        commit_timestamp = strstr(result, GET_COMMIT_TIMESTAMP);
        if (NULL != commit_timestamp)
        {
            commit_timestamp += strlen(GET_COMMIT_TIMESTAMP);
            commit_timestamp = strtok(commit_timestamp, "\n");
            t_result = cstring_to_text(commit_timestamp);
            pfree(result);
            return PointerGetDatum(t_result);
        }
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_get_2pc_xid
 * Get 2pc local xid
 */
Datum pgxc_get_2pc_xid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_2pc_xid);
Datum pgxc_get_2pc_xid(PG_FUNCTION_ARGS)
{
    GlobalTransactionId xid;
    char *tid = NULL;
    char *result = NULL;
    char *str_xid = NULL;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    result = get2PCInfo(tid);
    if (NULL != result)
    {
        str_xid = strstr(result, GET_XID);
        if (NULL != str_xid)
        {
            str_xid += strlen(GET_XID);
            str_xid = strtok(str_xid, "\n");
            xid = strtoul(str_xid, NULL, 10);
            pfree(result);
            PG_RETURN_UINT32(xid);
        }
    }
    PG_RETURN_NULL();
}

/*
 * pgxc_remove_2pc_records
 * Remove a 2pc file
 */
Datum pgxc_remove_2pc_records(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_remove_2pc_records);
Datum pgxc_remove_2pc_records(PG_FUNCTION_ARGS)
{
    char *tid = NULL;

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "2PC gid is empty");
    }
    tid = text_to_cstring(PG_GETARG_TEXT_P(0));
    remove_2pc_records(tid, true);
    pfree(tid);
    PG_RETURN_BOOL(true);
}

/*
 * pgxc_clear_2pc_records
 * Clear all 2pc files which are not running
 */
Datum pgxc_clear_2pc_records(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_clear_2pc_records);
Datum pgxc_clear_2pc_records(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext;
	MemoryContext mycontext;
	
	int i = 0;
    int count = 0;
    TupleTableSlots *result;
    TupleTableSlots clear_result;
    const char *query = "select public.pgxc_get_record_list()::text";
    const char *CLEAR_STMT = "select public.pgxc_remove_2pc_records('%s')::text";
    char clear_query[MAX_CMD_LENGTH];
    char *twopcfiles = NULL;
    char *ptr = NULL;
    char *save_ptr = NULL;
    bool res = true;

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}

	elog(LOG, "clear 2pc files");
	
	mycontext = AllocSetContextCreate(CurrentMemoryContext,
											  "clean_check",
											  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(mycontext);

    ResetGlobalVariables();

	/*get node list*/
	PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
					&cn_nodes_num, &dn_nodes_num, true);
	pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
	my_nodeoid = getMyNodeoid();
	cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
	dn_health_map = palloc0(dn_nodes_num * sizeof(bool));
    result = (TupleTableSlots *)palloc0(pgxc_clean_node_count * sizeof(TupleTableSlots));

    /*collect the 2pc file in nodes*/
    for (i = 0; i < cn_nodes_num; i++)
    {
        if (execute_query_on_single_node(cn_node_list[i],
            query, 1, result + i) == (Datum) 0)
        {
            elog(WARNING, "pgxc_clear_2pc_records: failed to query 2pc file list "
                "on node %s, sql: %s", get_pgxc_nodename(cn_node_list[i]), query);
        }
    }

    for (i = 0; i < dn_nodes_num; i++)
    {
        if (execute_query_on_single_node(dn_node_list[i],
            query, 1, result + cn_nodes_num + i) == (Datum) 0)
        {
            elog(WARNING, "pgxc_clear_2pc_records: failed to query 2pc file list "
                "on node %s, sql: %s", get_pgxc_nodename(dn_node_list[i]), query);
        }
    }

	/*get all database info*/
	getDatabaseList();
	
	/*get all info of 2PC transactions*/
	getTxnInfoOnNodesAll();

    /*delete all rest 2pc files in each cn*/
    for (i = 0; i < cn_nodes_num; i++)
    {
        if (0 == result[i].slot_count)
        {
            continue;
        }
        if (!(twopcfiles = TTSgetvalue(result+i, 0, 0)))
        {
            continue;
        }

        /*iterate through all 2pc files, delete rest ones*/
        ptr = strtok_r(twopcfiles, ",", &save_ptr);
        for (; ptr != NULL; ptr = strtok_r(NULL, ",", &save_ptr))
        {
            if (count >= MAXIMUM_CLEAR_FILE)
            {
                break;
            }

            /*whether 2pc is running?*/
            if (find_txn(ptr))
            {
                /*2pc is running, do not delete its file*/
                continue;
            }

            /*whether 2pc is rollbacked?*/
            if (strstr(ptr, ROLLBACK_POSTFIX) == NULL)
            {
                /*2pc is not rollbacked*/

                /*whether 2pc start xid transaction is running?*/
                if (is_gid_start_xid_running(ptr))
                {
                    /*2pc start xid transaction is running, do not delete its file*/
                    elog(LOG, "2PC '%s' is running", ptr);
                    continue;
                }
            }

            /*2pc is not running, delete its file*/
            snprintf(clear_query, MAX_CMD_LENGTH, CLEAR_STMT, ptr);
            elog(LOG, "clear 2pc file: %s", ptr);
            if (execute_query_on_single_node(cn_node_list[i],
                clear_query, 1, &clear_result) == (Datum) 0)
            {
                res = false;
                elog(WARNING, "pgxc_clear_2pc_records: failed to remove 2pc file "
                    "on node %s, sql: %s", get_pgxc_nodename(cn_node_list[i]),
                    clear_query);
            }
            DropTupleTableSlots(&clear_result);
            count++;
        }
    }

    /*delete all rest 2pc files in each dn*/
    for (i = 0; i < dn_nodes_num; i++)
    {
        if (0 == result[cn_nodes_num+i].slot_count)
        {
            continue;
        }
        if (!(twopcfiles = TTSgetvalue(result+cn_nodes_num+i, 0, 0)))
        {
            continue;
        }

        /*iterate through all 2pc files, delete rest ones*/
        ptr = strtok_r(twopcfiles, ",", &save_ptr);
        for (; ptr != NULL; ptr = strtok_r(NULL, ",", &save_ptr))
        {
            if (count >= MAXIMUM_CLEAR_FILE)
            {
                break;
            }

            /*whether 2pc is running?*/
            if (find_txn(ptr))
            {
                /*2pc is running, do not delete its file*/
                continue;
            }

            /*whether 2pc is rollbacked?*/
            if (strstr(ptr, ROLLBACK_POSTFIX) == NULL)
            {
                /*2pc is not rollbacked*/

                /*whether 2pc start xid transaction is running?*/
                if (is_gid_start_xid_running(ptr))
                {
                    /*2pc start xid transaction is running, do not delete its file*/
                    elog(LOG, "2PC '%s' is running", ptr);
                    continue;
                }
            }

            /*2pc is not running, delete its file*/
            snprintf(clear_query, MAX_CMD_LENGTH, CLEAR_STMT, ptr);
            elog(LOG, "clear 2pc file: %s", ptr);
            if (execute_query_on_single_node(dn_node_list[i],
                clear_query, 1, &clear_result) == (Datum) 0)
            {
                res = false;
                elog(WARNING, "pgxc_clear_2pc_records: failed to remove 2pc file "
                    "on node %s, sql: %s", get_pgxc_nodename(dn_node_list[i]),
                    clear_query);
            }
            DropTupleTableSlots(&clear_result);
            count++;
        }
    }

    for (i = 0; i < pgxc_clean_node_count; i++)
        DropTupleTableSlots(result+i);
    
    DestroyTxnHash();
	ResetGlobalVariables();

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(mycontext);
	
	
    PG_RETURN_BOOL(res);
}

/*
 * pgxc_get_record_list
 * Get 2pc files list
 */
Datum pgxc_get_record_list(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_get_record_list);
Datum pgxc_get_record_list(PG_FUNCTION_ARGS)
{
    int count = 0;
    DIR *dir = NULL;
    struct dirent *ptr = NULL;
    char *recordList = NULL;
    text *t_recordList = NULL;

    /* get from hash table */
    recordList = get_2pc_list_from_cache(&count);
    if (count >= MAXIMUM_OUTPUT_FILE)
    {
        if (NULL == recordList)
        {
            elog(PANIC, "recordList is NULL");
        }

        t_recordList = cstring_to_text(recordList);
        return PointerGetDatum(t_recordList);
    }

    /* get from disk */
    if(!(dir = opendir(TWOPHASE_RECORD_DIR)))
    {
        if(NULL == recordList)
        {
            PG_RETURN_NULL();
        }

        t_recordList = cstring_to_text(recordList);
        return PointerGetDatum(t_recordList);
    }

    while((ptr = readdir(dir)) != NULL)
    {
        if(strcmp(ptr->d_name,".") == 0 || strcmp(ptr->d_name,"..") == 0)
        {
            continue;
        }       
        if (count >= MAXIMUM_OUTPUT_FILE)
        {
            break;
        }
        
        if(!recordList)
        {
            recordList = (char *)palloc0(strlen(ptr->d_name) + 1);
            sprintf(recordList, "%s", ptr->d_name);
        }
        else
        {
            recordList = (char *) repalloc(recordList,
                                    strlen(ptr->d_name) + strlen(recordList) + 2);
            sprintf(recordList + strlen(recordList), ",%s", ptr->d_name);
        }
        count++;
    }

    closedir(dir);
    
    if(!recordList)
    {
        PG_RETURN_NULL();
    }
    else
    {
    	t_recordList = cstring_to_text(recordList);
        return PointerGetDatum(t_recordList);
    }
}

Datum pgxc_commit_on_node(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_commit_on_node);
Datum pgxc_commit_on_node(PG_FUNCTION_ARGS)
{
    /* nodename, gid */
    char *nodename;
    Oid  nodeoid;
    char *gid;
    txn_info *txn;
    char command[MAX_CMD_LENGTH];
    PGXCNodeHandle **connections = NULL;
    int					conn_count = 0;
    ResponseCombiner	combiner;
    PGXCNodeAllHandles *pgxc_handles = NULL;
    PGXCNodeHandle *conn = NULL;
    
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();

    /*clear Global*/
    ResetGlobalVariables();
    /*get node list*/
    PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
                    &cn_nodes_num, &dn_nodes_num, true);
    if (cn_node_list == NULL || dn_node_list == NULL)
        elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
    pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
    my_nodeoid = getMyNodeoid();
    cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
    dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "pgxc_commit_on_node: node name is empty");
    }
    nodename = text_to_cstring(PG_GETARG_TEXT_P(0));

    if (0 == PG_GETARG_DATUM(1))
    {
        elog(ERROR, "pgxc_commit_on_node: gid is empty");
    }
    gid = text_to_cstring(PG_GETARG_TEXT_P(1));

    nodeoid = get_pgxc_nodeoid(nodename);
    if (!OidIsValid(nodeoid))
    {
        elog(ERROR, "Invalid nodename '%s'", nodename);
    }
    
	txn = (txn_info *)palloc0(sizeof(txn_info));
	if (txn == NULL)
	{
		PG_RETURN_BOOL(false);
	}
	txn->txn_stat = (TXN_STATUS *)palloc0(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->xid = (uint32 *)palloc0(sizeof(uint32) * pgxc_clean_node_count);
	txn->prepare_timestamp = (TimestampTz *)palloc0(sizeof(TimestampTz) * pgxc_clean_node_count);
	txn->coordparts = (int *)palloc0(cn_nodes_num * sizeof(int));
	txn->dnparts = (int *)palloc0(dn_nodes_num * sizeof(int));

	strncpy(txn->gid, gid, strlen(gid)+1);
    getTxnInfoOnOtherNodes(txn);
	snprintf(command, MAX_CMD_LENGTH, "commit prepared '%s'", txn->gid);


    if (InvalidGlobalTimestamp == txn->global_commit_timestamp)
    {
        if (!txn->is_readonly)
        {
            elog(ERROR, "in pg_clean, fail to get global_commit_timestamp for transaction '%s' on", gid);
        }
        else
        {
            txn->global_commit_timestamp = GetGlobalTimestampGTM();
            if (!GlobalTimestampIsValid(current_gts))
            {
                elog(ERROR, "pgxc_commit_on_node, get invalid gts");
            }
        }
    }
    
	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*));
    get_node_handles(&pgxc_handles, nodeoid);

    conn = (PGXC_NODE_COORDINATOR == get_pgxc_nodetype(nodeoid)) ? 
            pgxc_handles->coord_handles[0] : pgxc_handles->datanode_handles[0];
    if (!send_query_clean_transaction(conn, txn, command))
    {
        elog(ERROR, "pgxc_commit_on_node: send sql '%s' from '%s' to '%s' failed",
            command, get_pgxc_nodename(my_nodeoid) , nodename);
    }
    else
    {
        elog(LOG, "pgxc_commit_on_node: send sql '%s' from '%s' to '%s'",
            command, get_pgxc_nodename(my_nodeoid) , nodename);
        connections[conn_count++] = conn;
    }
    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    /*clear Global*/
    ResetGlobalVariables();
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;
    pfree(connections);
    connections = NULL;

    PG_RETURN_BOOL(true);
}

Datum pgxc_abort_on_node(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pgxc_abort_on_node);
Datum pgxc_abort_on_node(PG_FUNCTION_ARGS)
{
    /* nodename, gid */
    char *nodename;
    Oid  nodeoid;
    char *gid;
    txn_info *txn;
    char command[MAX_CMD_LENGTH];
    PGXCNodeHandle **connections = NULL;
    int					conn_count = 0;
    ResponseCombiner	combiner;
    PGXCNodeAllHandles *pgxc_handles = NULL;
    PGXCNodeHandle *conn = NULL;
    
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();

    /*clear Global*/
    ResetGlobalVariables();
    /*get node list*/
    PgxcNodeGetOids(&cn_node_list, &dn_node_list, 
                    &cn_nodes_num, &dn_nodes_num, true);
    if (cn_node_list == NULL || dn_node_list == NULL)
        elog(ERROR, "pg_clean:fail to get cn_node_list and dn_node_list");
    pgxc_clean_node_count = cn_nodes_num + dn_nodes_num;
    my_nodeoid = getMyNodeoid();
    cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
    dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

    if (0 == PG_GETARG_DATUM(0))
    {
        elog(ERROR, "pgxc_abort_on_node: node name is empty");
    }
    nodename = text_to_cstring(PG_GETARG_TEXT_P(0));

    if (0 == PG_GETARG_DATUM(1))
    {
        elog(ERROR, "pgxc_abort_on_node: gid is empty");
    }
    gid = text_to_cstring(PG_GETARG_TEXT_P(1));

    nodeoid = get_pgxc_nodeoid(nodename);
    if (!OidIsValid(nodeoid))
    {
        elog(ERROR, "Invalid nodename '%s'", nodename);
    }
    
	txn = (txn_info *)palloc0(sizeof(txn_info));
	if (txn == NULL)
	{
		PG_RETURN_BOOL(false);
	}
	txn->txn_stat = (TXN_STATUS *)palloc0(sizeof(TXN_STATUS) * pgxc_clean_node_count);
	txn->xid = (uint32 *)palloc0(sizeof(uint32) * pgxc_clean_node_count);
	txn->prepare_timestamp = (TimestampTz *)palloc0(sizeof(TimestampTz) * pgxc_clean_node_count);
	txn->coordparts = (int *)palloc0(cn_nodes_num * sizeof(int));
	txn->dnparts = (int *)palloc0(dn_nodes_num * sizeof(int));

	strncpy(txn->gid, gid, strlen(gid)+1);
	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*));
    getTxnInfoOnOtherNodes(txn);
	snprintf(command, MAX_CMD_LENGTH, "rollback prepared '%s'", txn->gid);
#if 0    
	if (!setMaintenanceMode(true))
	{
		elog(ERROR, "Error: fail to set maintenance mode on in pg_clean");
	}
#endif    

    get_node_handles(&pgxc_handles, nodeoid);

    conn = (PGXC_NODE_COORDINATOR == get_pgxc_nodetype(nodeoid)) ? 
            pgxc_handles->coord_handles[0] : pgxc_handles->datanode_handles[0];
    if (!send_query_clean_transaction(conn, txn, command))
    {
        elog(ERROR, "pgxc_abort_on_node: send sql '%s' from '%s' to '%s' failed",
            command, get_pgxc_nodename(my_nodeoid) , nodename);
    }
    else
    {
        elog(LOG, "pgxc_abort_on_node: send sql '%s' from '%s' to '%s'",
            command, get_pgxc_nodename(my_nodeoid) , nodename);
        connections[conn_count++] = conn;
    }
    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    /*clear Global*/
    ResetGlobalVariables();
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;
    pfree(connections);
    connections = NULL;

    PG_RETURN_BOOL(true);
}



void recover2PCForDatabaseAll(void)
{
	database_info *cur_db = head_database_info;
	while (cur_db)
	{
		recover2PCForDatabase(cur_db);
		cur_db = cur_db->next;
	}
	//clean_old_2PC_files();
}

void recover2PCForDatabase(database_info * db_info)
{
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn = db_info->all_txn_info;

	hash_seq_init(&status, txn);
	while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
	{
		recover2PC(cur_txn);
    }
}

bool send_query_clean_transaction(PGXCNodeHandle* conn, txn_info *txn, const char *finish_cmd)
{
#ifdef __TWO_PHASE_TESTS__
    if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case &&
        PG_CLEAN_SEND_QUERY >= twophase_exception_case)
    {
        twophase_in = IN_PG_CLEAN;
    }
#endif
	if (!GlobalTimestampIsValid(txn->global_commit_timestamp) && 
        TXN_STATUS_COMMITTED == txn->global_txn_stat &&
        !txn->is_readonly)
		return false;

    if (pgxc_node_send_coord_info(conn, MyProcPid, MyProc->lxid))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("in pg_clean failed to send pid(%d) for %s PREPARED command",
                        MyProcPid, TXN_STATUS_COMMITTED == txn->global_txn_stat ?
                        "COMMIT" : "ROLLBACK")));
        return false;
    }
    if (pgxc_node_send_clean(conn))
    {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION ),
                 errmsg("in pg_clean failed to send pg_clean flag for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }
    if (txn->is_readonly && pgxc_node_send_readonly(conn, true))
    {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION ),
                 errmsg("in pg_clean failed to send readonly flag (true) for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }

    if (txn->after_first_phase && pgxc_node_send_after_prepare(conn))
    {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION ),
                 errmsg("in pg_clean failed to send after prepare flag for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }
    
    /* 
     * only transaction finished in commit prepared/rollback prepared phase send timestamp 
     * partial prepared transaction has no need to send other information
     */
	if (InvalidGlobalTimestamp != txn->global_commit_timestamp && 
        pgxc_node_send_global_timestamp(conn, txn->global_commit_timestamp))
	{
		ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("in pg_clean failed to send global committs for %s PREPARED command",
						TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
	}
    if (!txn->is_readonly)
    {
        if (InvalidOid != txn->origcoord && pgxc_node_send_starter(conn, get_pgxc_nodename(txn->origcoord)))
        {
    		ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
    				 errmsg("in pg_clean failed to send start node for %s PREPARED command",
    						TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }

        if (InvalidTransactionId != txn->startxid && pgxc_node_send_startxid(conn, txn->startxid))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                     errmsg("in pg_clean failed to send start xid for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }

        if (InvalidGlobalTimestamp != txn->global_prepare_timestamp &&
			pgxc_node_send_prepare_timestamp(conn, txn->global_prepare_timestamp))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("in pg_clean failed to send prepare timestamp for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }

        if (NULL != txn->participants && pgxc_node_send_partnodes(conn, txn->participants))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                     errmsg("in pg_clean failed to send participants for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }

        if (NULL != txn->participants && pgxc_node_send_database(conn, txn->database))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("in pg_clean failed to send database for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }

        if (NULL != txn->participants && pgxc_node_send_user(conn, txn->owner))
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("in pg_clean failed to send user for %s PREPARED command",
                            TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        }
    }
    
    if (pgxc_node_send_query_with_internal_flag(conn, finish_cmd))
    {
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("in pg_clean failed to send query for %s PREPARED command",
                        TXN_STATUS_COMMITTED == txn->global_txn_stat ? "COMMIT" : "ROLLBACK")));
        return false;
    }
	return true;
}

bool check_2pc_belong_node(txn_info * txn)
{
    int node_index = 0;
    char node_type;
    node_index = find_node_index(abnormal_nodeoid);

    /* abnormal node oid must be valid here */
    if (InvalidOid == abnormal_nodeoid)
    {
        elog(PANIC, "abnormal_nodeoid is invalid");
    }

    if (abnormal_nodeoid == txn->origcoord)
    {
        txn->belong_abnormal_node = true;
        return true;
    }
    node_type = get_pgxc_nodetype(abnormal_nodeoid);
    if (node_type == 'C' && txn->coordparts[node_index] == 1)
    {
        txn->belong_abnormal_node = true;
        return true;
    }
    if (node_type == 'D' && txn->dnparts[node_index - cn_nodes_num] == 1)
    {
        txn->belong_abnormal_node = true;
        return true;
    }

    if (InvalidOid == txn->origcoord)
    {
        int node_oid = InvalidOid;

        if (!IsXidImplicit(txn->gid))
        {
            txn->belong_abnormal_node = true;
            return true;
        }

        /* Get start node oid from gid */
        node_oid = get_start_node_oid_from_gid(txn->gid);
        if (node_oid == InvalidOid)
        {
            elog(WARNING, "Get invalid start node oid from gid(%s)", txn->gid);
            txn->belong_abnormal_node = false;
            return false;
        }

        elog(DEBUG1, "Get start node oid(%d) from gid(%s)", node_oid, txn->gid);

        if (abnormal_nodeoid == node_oid)
        {
            txn->belong_abnormal_node = true;
            return true;
        }
    }

    txn->belong_abnormal_node = false;
    return false;
}

bool check_node_participate(txn_info * txn, int node_idx)
{
    char node_type = get_pgxc_nodetype(abnormal_nodeoid);
    if (PGXC_NODE_COORDINATOR == node_type) 
    {
        return txn->coordparts[node_idx] == 1 ? true : false;
    } else if (PGXC_NODE_DATANODE == node_type)
    {
        return txn->dnparts[node_idx] == 1 ? true : false;
    }
    return false;
}

static void recovery_a_txn(txn_info *txn, bool commit)
{
    MemoryContext current_context = NULL;
    ErrorData* edata = NULL;
    bool is_running = true;
    char *print_string = commit ? "Commit" : "Rollback";

    /* check whether the 2pc start xid is still running on start node */
	if (is_txn_start_xid_running(txn))
	{
		elog(WARNING, "%s 2PC '%s' start xid %d is running", print_string, txn->gid, txn->startxid);
		txn->op_issuccess = false;
		return;
	}

	/* check whether the 2pc is still running on participants */
	is_running = false;
	current_context = CurrentMemoryContext;
	PG_TRY();
	{
		if (!clean_2PC_iscommit(txn, commit, true))
		{
			is_running = true;
			elog(WARNING, "%s 2PC '%s' check failed", print_string, txn->gid);
		}
	}
	PG_CATCH();
	{
		is_running = true;
		(void)MemoryContextSwitchTo(current_context);
		edata = CopyErrorData();
		FlushErrorState();

		elog(WARNING, "%s 2PC '%s' check error: %s",
			print_string, txn->gid, edata->message);

		if (is_for_deadlock && edata->sqlerrcode == ERRCODE_UNDEFINED_OBJECT)
		{
			elog(LOG, "%s 2PC '%s' ignore check error: %s",
				print_string, txn->gid, edata->message);
			is_running = false;
		}
	}
	PG_END_TRY();

	/* 2pc is still running, do not try to clean */
	if (is_running)
	{
		txn->op_issuccess = false;
		return;
	}

	/*free hash record from gtm*/
	FinishGIDGTM(txn->gid);

	/* after free gid from gtm, check again */
	PG_TRY();
	{
		if (!clean_2PC_iscommit(txn, commit, true))
		{
			is_running = true;
			elog(WARNING, "again %s 2PC '%s' check failed", print_string, txn->gid);
		}
	}
	PG_CATCH();
	{
		is_running = true;
		(void)MemoryContextSwitchTo(current_context);
		edata = CopyErrorData();
		FlushErrorState();

		elog(WARNING, "again %s 2PC '%s' check error: %s",
			print_string, txn->gid, edata->message);

		if (is_for_deadlock && edata->sqlerrcode == ERRCODE_UNDEFINED_OBJECT)
		{
			elog(LOG, "again %s 2PC '%s' ignore check error: %s",
				print_string, txn->gid, edata->message);
			is_running = false;
		}
	}
	PG_END_TRY();

	/* 2pc is still running, do not try to clean */
	if (is_running)
	{
		txn->op_issuccess = false;
		return;
	}

	/* send commit prepared to all nodes */
	if (!clean_2PC_iscommit(txn, commit, false))
	{
		txn->op_issuccess = false;
		elog(WARNING, "%s 2PC '%s' failed", print_string, txn->gid);
		return;
	}
	txn->op_issuccess = true;
}

void recover2PC(txn_info *txn)
{
	TXN_STATUS txn_stat;
	txn_stat = check_txn_global_status(txn);
	txn->global_txn_stat = txn_stat;

	if (is_for_deadlock)
	{
		if (strcmp(txn->database, get_database_name(MyDatabaseId)) != 0)
		{
			elog(NOTICE, "2PC(%s) belongs to another database: %s, "
				"current database: %s", txn->gid, txn->database,
				get_database_name(MyDatabaseId));
			return;
		}

		if (!IsXidImplicit(txn->gid))
		{
			elog(NOTICE, "2pc(%s) is explicit, skip", txn->gid);
			return;
		}
	}

#ifdef DEBUG_EXECABORT
	txn_stat = TXN_STATUS_ABORTED;
#endif

	switch (txn_stat)
	{
		case TXN_STATUS_FAILED:
			elog(DEBUG1, "cannot recover 2PC transaction %s for TXN_STATUS_FAILED", txn->gid);
			txn->op = UNDO;
			txn->op_issuccess = true;
			break;

		case TXN_STATUS_UNKNOWN:
			elog(DEBUG1, "cannot recover 2PC transaction %s for TXN_STATUS_UNKNOWN", txn->gid);
			txn->op = UNDO;
			txn->op_issuccess = true;
			break;

		case TXN_STATUS_PREPARED:
			elog(DEBUG1, "2PC recovery of transaction %s not needed for TXN_STATUS_PREPARED", txn->gid);
			txn->op = UNDO;
			txn->op_issuccess = true;
			break;

		case TXN_STATUS_COMMITTED:
			if (txn->is_readonly)
			{
				txn->op = UNDO;
				txn->op_issuccess = true;
				break;
			}

			if (!OidIsValid(txn->origcoord))
			{
				elog(LOG, "2pc(%s) get start node oid failed, "
				"maybe it is removed from the cluster", txn->gid);

				if (!is_for_node_removed)
				{
					txn->op = UNDO;
					txn->op_issuccess = true;
					break;
				}
			}

			txn->op = COMMIT;
			elog(LOG, "Commit 2PC '%s'", txn->gid);

			/* check whether the 2pc start xid is 0 */
			if (txn->startxid == 0 && IsXidImplicit(txn->gid))
			{
				elog(LOG, "Commit 2PC '%s' start xid is 0", txn->gid);
				txn->op_issuccess = false;
				return;
			}

			recovery_a_txn(txn, true);

			break;

		case TXN_STATUS_ABORTED:
			if (!OidIsValid(txn->origcoord))
			{
				elog(LOG, "2pc(%s) get start node oid failed, "
					"maybe it is removed from the cluster", txn->gid);

				if (!is_for_node_removed)
				{
					txn->op = UNDO;
					txn->op_issuccess = true;
					break;
				}
			}

			txn->op = ABORT;

			elog(LOG, "Rollback 2PC '%s'", txn->gid);

			/* check whether the 2pc start xid is 0 */
			if (txn->startxid == 0 && IsXidImplicit(txn->gid))
			{
				elog(LOG, "Rollback 2PC '%s' start xid is 0", txn->gid);
			}

			recovery_a_txn(txn, false);

			break;
		
		case TXN_STATUS_INPROGRESS:
			elog(DEBUG1, "2PC recovery of transaction %s not needed for TXN_STATUS_INPROGRESS", txn->gid);
			txn->op = UNDO;
			txn->op_issuccess = true;
			break;
		
		default:
			elog(ERROR, "cannot recover 2PC transaction %s for unkown status", txn->gid);
			break;
	}

}

TXN_STATUS check_txn_global_status(txn_info *txn)
{
#define TXN_PREPARED 	0x0001
#define TXN_COMMITTED 	0x0002
#define TXN_ABORTED		0x0004
#define TXN_UNKNOWN		0x0008
#define TXN_INITIAL		0x0010
#define TXN_INPROGRESS	0X0020
	int ii;
	int check_flag = 0;
	TimestampTz prepared_time = 0;
	TimestampTz time_gap = clean_time_interval;

    if (!IsXidImplicit(txn->gid) && txn->is_readonly)
    {
        return TXN_STATUS_COMMITTED;
    }
    if (txn->global_txn_stat == TXN_STATUS_UNKNOWN)
    {
        check_flag |= TXN_UNKNOWN;
    }
    if (txn->global_txn_stat == TXN_STATUS_ABORTED)
    {
        check_flag |= TXN_ABORTED;
    }

	/*check dn participates*/
	for (ii = 0; ii < dn_nodes_num; ii++)
	{
		if (txn->dnparts[ii] == 1)
		{
			if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_INITIAL)
				check_flag |= TXN_INITIAL;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_UNKNOWN)
				check_flag |= TXN_UNKNOWN;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_PREPARED)
			{
				check_flag |= TXN_PREPARED;
				prepared_time = txn->prepare_timestamp[ii + cn_nodes_num] > prepared_time ? 
								txn->prepare_timestamp[ii + cn_nodes_num] : prepared_time;
			}
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_INPROGRESS)
				check_flag |= TXN_INPROGRESS;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_COMMITTED)
				check_flag |= TXN_COMMITTED;
			else if (txn->txn_stat[ii + cn_nodes_num] == TXN_STATUS_ABORTED)
				check_flag |= TXN_ABORTED;
			else
				return TXN_STATUS_FAILED;
		}
	}
	/*check cn participates*/
	for (ii = 0; ii < cn_nodes_num; ii++)
	{
		if (txn->coordparts[ii] == 1)
		{
			if (txn->txn_stat[ii] == TXN_STATUS_INITIAL)
				check_flag |= TXN_ABORTED;
			else if (txn->txn_stat[ii] == TXN_STATUS_UNKNOWN)
				check_flag |= TXN_UNKNOWN;
			else if (txn->txn_stat[ii] == TXN_STATUS_PREPARED)
			{
				check_flag |= TXN_PREPARED;
				prepared_time = txn->prepare_timestamp[ii] > prepared_time ? 
								txn->prepare_timestamp[ii] : prepared_time;
			}
			else if (txn->txn_stat[ii] == TXN_STATUS_INPROGRESS)
				check_flag |= TXN_INPROGRESS;
			else if (txn->txn_stat[ii] == TXN_STATUS_COMMITTED)
				check_flag |= TXN_COMMITTED;
			else if (txn->txn_stat[ii] == TXN_STATUS_ABORTED)
				check_flag |= TXN_ABORTED;
			else
				return TXN_STATUS_FAILED;
		}
	}

    /*
     * first check the prepare timestamp of both implicit and explicit trans within the time_gap or not
     * if not, check the commit timestamp explicit trans within the time_gap or not 
     */
#if 0     
    if ((check_flag & TXN_INPROGRESS) ||
        (IsXidImplicit(txn->gid) && current_time - prepared_time <= time_gap) ||
        (!IsXidImplicit(txn->gid) && 
            ((!txn->after_first_phase && current_time - prepared_time <= time_gap) ||
            (txn->after_first_phase && 
                (InvalidGlobalTimestamp != commit_time && 
                current_time - commit_time <= time_gap)))))
    {
		/* transaction inprogress */
        return TXN_STATUS_INPROGRESS;
    }
#endif                

    /* start xid is 0, maybe at the beginning of the 2pc */
    if (txn->startxid == 0)
    {
        /* prepare timestamp must be invalid */
        if (GlobalTimestampIsValid(txn->global_prepare_timestamp))
        {
            elog(PANIC, "gid: %s, start xid is 0, global_prepare_timestamp: %ld",
                txn->gid, txn->global_prepare_timestamp);
        }

        elog(DEBUG2, "2PC '%s' start xid is 0", txn->gid);

        if (check_flag & TXN_INPROGRESS
            || current_time - prepared_time <= time_gap)
        {
            /* inprogress or less than time gap, do not clean it */
            elog(LOG, "2PC '%s' start xid is 0, inprogress, "
                "current_time: %ld, prepared_time: %ld, "
                "time_gap: %ld, time_diff: %ld",
                txn->gid, current_time, prepared_time,
                time_gap, current_time - prepared_time);

            return TXN_STATUS_INPROGRESS;
        }
        else
        {
            /* otherwise, abort it */
            elog(LOG, "2PC '%s' start xid is 0, "
                "current_time: %ld, prepared_time: %ld, "
                "time_gap: %ld, time_diff: %ld",
                txn->gid, current_time, prepared_time,
                time_gap, current_time - prepared_time);

            return TXN_STATUS_ABORTED;
        }
    }

    if (!GlobalTimestampIsValid(txn->global_prepare_timestamp))
    {
        if (IsXidImplicit(txn->gid))
        {
            /* upgrade from old version, no prepare gts in old version */
            elog(WARNING, "implicit gid: %s, start xid is %d, "
                "global_prepare_timestamp is invalid",
                txn->gid, txn->startxid);
        }
        else
        {
            /* explicit 2pc prepare gts is invalid */
            elog(LOG, "explicit gid: %s, start xid is %d, "
                "global_prepare_timestamp is invalid",
                txn->gid, txn->startxid);
        }

        if (check_flag & TXN_INPROGRESS
            || current_time - prepared_time <= time_gap)
        {
            /* inprogress or less than time gap, do not clean it */
            elog(WARNING, "gid: %s, start xid is %d, inprogress, "
                "current_time: %ld, prepared_time: %ld, "
                "time_gap: %ld, time_diff: %ld",
                txn->gid, txn->startxid, current_time, prepared_time,
                time_gap, current_time - prepared_time);

            return TXN_STATUS_INPROGRESS;
        }
        else if (IsXidImplicit(txn->gid))
        {
            /* otherwise, set prepare timestamp */
            if (clear_2pc_belong_node)
            {
                txn->global_prepare_timestamp = abnormal_gts;
            }
            else
            {
                txn->global_prepare_timestamp = current_gts - time_gap;
            }

            elog(WARNING, "implicit gid: %s, start xid is %d, "
                "current_time: %ld, prepared_time: %ld, "
                "time_gap: %ld, time_diff: %ld, "
                "set global_prepare_timestamp: %ld",
                txn->gid, txn->startxid, current_time, prepared_time,
                time_gap, current_time - prepared_time,
                txn->global_prepare_timestamp);
        }
        else
        {
            /* explicit 2pc prepare gts is invalid */
            elog(LOG, "explicit gid: %s, start xid is %d, "
                "current_time: %ld, prepared_time: %ld, "
                "time_gap: %ld, time_diff: %ld, "
                "global_prepare_timestamp: %ld",
                txn->gid, txn->startxid, current_time, prepared_time,
                time_gap, current_time - prepared_time,
                txn->global_prepare_timestamp);
        }
    }

    if (clear_2pc_belong_node)
    {
        if (!check_2pc_belong_node(txn))
        {
            return TXN_STATUS_INPROGRESS;
        }

        if (!check_2pc_start_from_node(txn))
        {
            return TXN_STATUS_INPROGRESS;
        }

        /* abnormal gts must be valid */
        if (!GlobalTimestampIsValid(abnormal_gts))
        {
            elog(PANIC, "gid: %s, abnormal_gts is invalid gts", txn->gid);
        }

        if (GlobalTimestampIsValid(txn->global_prepare_timestamp))
        {
            /* abnormal gts less than prepare gts, do not clean it */
            if (abnormal_gts < txn->global_prepare_timestamp)
            {
                elog(LOG, "gid: %s, abnormal gts: " INT64_FORMAT
                    ", prepare gts: " INT64_FORMAT, txn->gid,
                    abnormal_gts, txn->global_prepare_timestamp);
                return TXN_STATUS_INPROGRESS;
            }
        }
        else
        {
            int node_idx = 0;
            if (IsXidImplicit(txn->gid))
            {
                /*
                 * implicit 2pc prepare gts must be valid,
                 * expect upgrade from old version 
                */
                elog(WARNING, "implicit gid: %s, global_prepare_timestamp is invalid",
                    txn->gid);
            }
            else
            {
                /* explicit 2pc prepare gts is invalid */
                elog(LOG, "explicit gid: %s, global_prepare_timestamp is invalid",
                    txn->gid);
            }

            /* no valid prepare gts, compare time */
            node_idx = find_node_index(abnormal_nodeoid);
            if (node_idx >= 0)
            {
                if (abnormal_time < txn->prepare_timestamp[node_idx])
                {
                    elog(WARNING, "gid: %s, abnormal time: " INT64_FORMAT
                        ", prepare timestamp[%d]: " INT64_FORMAT, txn->gid,
                        abnormal_time, node_idx, txn->prepare_timestamp[node_idx]);
                    return TXN_STATUS_INPROGRESS;
                }
            }
            else
            {
                elog(WARNING, "gid: %s, node_idx: %d", txn->gid, node_idx);
            }

            if (abnormal_time < prepared_time)
            {
                elog(WARNING, "gid: %s, abnormal time: " INT64_FORMAT
                    ", prepared time: " INT64_FORMAT, txn->gid,
                    abnormal_time, prepared_time);
                return TXN_STATUS_INPROGRESS;
            }
        }

        if (GlobalTimestampIsValid(txn->global_commit_timestamp))
        {
            /* abnormal gts less than commit gts, do not clean it */
            if (abnormal_gts < txn->global_commit_timestamp)
            {
                elog(LOG, "gid: %s, abnormal gts: " INT64_FORMAT
                    ", commit gts: " INT64_FORMAT, txn->gid,
                    abnormal_gts, txn->global_commit_timestamp);
                return TXN_STATUS_INPROGRESS;
            }
        }
    }
    else
    {
        if (check_flag & TXN_INPROGRESS || current_time - prepared_time <= time_gap)
        {
            /* transaction inprogress */
            return TXN_STATUS_INPROGRESS;
        }

        /* current gts must be valid */
        if (!GlobalTimestampIsValid(current_gts))
        {
            elog(PANIC, "gid: %s, current_gts is invalid gts", txn->gid);
        }

        if (GlobalTimestampIsValid(txn->global_prepare_timestamp))
        {
            /* 2pc prepare gts gap less than time gap, do not clean it */
            if (current_gts - txn->global_prepare_timestamp < time_gap)
            {
                elog(LOG, "gid: %s, current gts: " INT64_FORMAT
                    ", prepare gts: " INT64_FORMAT ", time gap: " INT64_FORMAT,
                    txn->gid, current_gts, txn->global_prepare_timestamp, time_gap);
                return TXN_STATUS_INPROGRESS;
            }
        }
        else
        {
            if (IsXidImplicit(txn->gid))
            {
                /*
                 * implicit 2pc prepare gts must be valid,
                 * expect upgrade from old version 
                */
                elog(WARNING, "implicit gid: %s, global_prepare_timestamp is invalid",
                    txn->gid);
            }
            else
            {
                /* explicit 2pc prepare gts is invalid */
                elog(LOG, "explicit gid: %s, global_prepare_timestamp is invalid",
                    txn->gid);
            }
        }

        if (GlobalTimestampIsValid(txn->global_commit_timestamp))
        {
            /* 2pc commit gts gap less than time gap, do not clean it */
            if (current_gts - txn->global_commit_timestamp <= time_gap)
            {
                elog(LOG, "gid: %s, current gts: " INT64_FORMAT
                    ", commit gts: " INT64_FORMAT ", time gap: " INT64_FORMAT,
                    txn->gid, current_gts, txn->global_commit_timestamp, time_gap);
                return TXN_STATUS_INPROGRESS;
            }
        }
    }

    if (!IsXidImplicit(txn->gid) && txn->after_first_phase && (TXN_PREPARED == check_flag))
    {
        return TXN_STATUS_PREPARED;
    }

	if (check_flag & TXN_UNKNOWN)
		return TXN_STATUS_UNKNOWN;
    
	if ((check_flag & TXN_COMMITTED) && (check_flag & TXN_ABORTED))
		/* Mix of committed and aborted. This should not happen. */
		return TXN_STATUS_UNKNOWN;
    
	if ((check_flag & TXN_PREPARED) == 0)
		/* Should be at least one "prepared statement" in nodes */
		return TXN_STATUS_FAILED;
		
	if (check_flag & TXN_COMMITTED)
		/* Some 2PC transactions are committed.  Need to commit others. */
		return TXN_STATUS_COMMITTED;

	/* If 2PC commit gts is valid, must commit it. */
	if (GlobalTimestampIsValid(txn->global_commit_timestamp))
	{
		elog(LOG, "'%s' global_commit_timestamp: %ld",
			txn->gid, txn->global_commit_timestamp);

		if (!(check_flag & TXN_PREPARED))
		{
			elog(PANIC, "gid: %s, check_flag: %d", txn->gid, check_flag);
		}

		return TXN_STATUS_COMMITTED;
	}

	/* All the transactions remain prepared.   No need to recover. */
	return TXN_STATUS_ABORTED;
}

bool clean_2PC_iscommit(txn_info *txn, bool is_commit, bool is_check)
{
	int ii;
	static const char *STMT_FORM = "%s prepared '%s';";
	static const char *STMT_FORM_CHECK = "%s prepared '%s' for check only;";
	char command[MAX_CMD_LENGTH];
	int node_idx;
    Oid node_oid;
	PGXCNodeHandle **connections = NULL;
	int					conn_count = 0;
	ResponseCombiner	combiner;
	PGXCNodeAllHandles *pgxc_handles = NULL;

	if (is_commit)
	{
		if (is_check)
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM_CHECK, "commit", txn->gid);
		}
		else
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM, "commit", txn->gid);
		}
	}
	else
	{
		if (is_check)
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM_CHECK, "rollback", txn->gid);
		}
		else
		{
			snprintf(command, MAX_CMD_LENGTH, STMT_FORM, "rollback", txn->gid);
		}
	}
	if (is_commit && InvalidGlobalTimestamp == txn->global_commit_timestamp)
	{
		elog(ERROR, "twophase transaction '%s' has InvalidGlobalCommitTimestamp", txn->gid);
	}

	connections = (PGXCNodeHandle**)palloc(sizeof(PGXCNodeHandle*) * (txn->num_dnparts + txn->num_coordparts));
	if (connections == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for connections")));
    }
    get_transaction_handles(&pgxc_handles, txn);
#ifdef __TWO_PHASE_TESTS__
    if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case && 
        PG_CLEAN_ELOG_ERROR >= twophase_exception_case)
    {
        exception_count = 0;
    }
#endif
	for (ii = 0; ii < pgxc_handles->dn_conn_count; ii++)
	{
        node_oid = pgxc_handles->datanode_handles[ii]->nodeoid;
        node_idx = find_node_index(node_oid);
        if (node_idx < 0 || node_idx >= cn_nodes_num + dn_nodes_num)
        {
            elog(PANIC, "gid: %s, node_idx(%d) is invalid", txn->gid, node_idx);
        }

        if (TXN_STATUS_PREPARED != txn->txn_stat[node_idx])
        {
            continue;
        }
		/*send global timestamp to dn_node_list[ii]*/
		if (!send_query_clean_transaction(pgxc_handles->datanode_handles[ii], txn, command))
		{
			elog(WARNING, "pg_clean: send sql '%s' from '%s' to '%s' failed",
				command, get_pgxc_nodename(my_nodeoid),
				pgxc_handles->datanode_handles[ii]->nodename);
			return false;
		}
        else
        {
            elog(LOG, "pg_clean: send sql '%s' from '%s' to '%s'",
                command, get_pgxc_nodename(my_nodeoid),
                pgxc_handles->datanode_handles[ii]->nodename);
            connections[conn_count++] = pgxc_handles->datanode_handles[ii];
#ifdef __TWO_PHASE_TESTS__
            if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case && 
                PG_CLEAN_ELOG_ERROR >= twophase_exception_case)
            {
                exception_count++;
                if (1 == exception_count && 
                    PG_CLEAN_ELOG_ERROR == twophase_exception_case)
                {
                    elog(ERROR, "PG_CLEAN_ELOG_ERROR complish");
                }
            }
#endif
        }
	}

	for (ii = 0; ii < pgxc_handles->co_conn_count; ii++)
	{
        node_oid = pgxc_handles->coord_handles[ii]->nodeoid;
        node_idx = find_node_index(node_oid);
        if (node_idx < 0 || node_idx >= cn_nodes_num + dn_nodes_num)
        {
            elog(PANIC, "gid: %s, node_idx(%d) is invalid", txn->gid, node_idx);
        }

        if (TXN_STATUS_PREPARED != txn->txn_stat[node_idx])
        {
            continue;
        }
		/*send global timestamp to dn_node_list[ii]*/
		if (!send_query_clean_transaction(pgxc_handles->coord_handles[ii], txn, command))
		{
			elog(WARNING, "pg_clean: send sql '%s' from '%s' to '%s' failed",
				command, get_pgxc_nodename(my_nodeoid),
				pgxc_handles->coord_handles[ii]->nodename);
			return false;
		}
        else
        {
            elog(LOG, "pg_clean: send sql '%s' from '%s' to '%s'",
                command, get_pgxc_nodename(my_nodeoid),
                pgxc_handles->coord_handles[ii]->nodename);
            connections[conn_count++] = pgxc_handles->coord_handles[ii];
#ifdef __TWO_PHASE_TESTS__
            if (PG_CLEAN_SEND_CLEAN <= twophase_exception_case && 
                PG_CLEAN_ELOG_ERROR >= twophase_exception_case)
            {
                exception_count++;
                if (1 == exception_count && 
                    PG_CLEAN_ELOG_ERROR == twophase_exception_case)
                {
                    elog(ERROR, "PG_CLEAN_ELOG_ERROR complish");
                }
            }
#endif
        }
	}

    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }
    if (enable_distri_print)
    {
        for (ii = 0; ii < conn_count; ii++)
        {
            if (!IsConnectionStateIdle(connections[ii]))
            {
                elog(WARNING, "IN pg_clean node:%s invalid stauts:%s", 
					connections[ii]->nodename, GetConnectionStateTag(connections[ii]));
            }
        }
    }
    conn_count = 0;
	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;

    /*last commit or rollback on origcoord if it participate this txn, since after commit the 2pc file is deleted on origcoord*/
    if (txn->origcoord != InvalidOid)
    {
        node_idx = find_node_index(txn->origcoord);
        if (node_idx < 0 || node_idx >= cn_nodes_num + dn_nodes_num)
        {
            elog(PANIC, "gid: %s, node_idx(%d) is invalid", txn->gid, node_idx);
        }

        if (txn->coordparts[node_idx] == 1)
        {
            /*send global timestamp to dn_node_list[ii]*/
            if (txn->txn_stat[node_idx] == TXN_STATUS_PREPARED)
            {
                get_node_handles(&pgxc_handles, txn->origcoord);
                if (!send_query_clean_transaction(pgxc_handles->coord_handles[0], txn, command))
                {
                    elog(WARNING, "pg_clean: send sql '%s' from %s to %s failed", 
                        command, get_pgxc_nodename(my_nodeoid),
                        pgxc_handles->coord_handles[0]->nodename);
                    return false;
                }
                else
                {
                    elog(LOG, "pg_clean: send sql '%s' from '%s' to '%s'",
                        command, get_pgxc_nodename(my_nodeoid),
                        pgxc_handles->coord_handles[0]->nodename);
                    connections[conn_count++] = pgxc_handles->coord_handles[0];
                }
            }
        }
    }

    /* receive response */
    if (conn_count)
    {
        InitResponseCombiner(&combiner, conn_count, COMBINE_TYPE_NONE);
        if (pgxc_node_receive_responses(conn_count, connections, NULL, &combiner) ||
                !validate_combiner(&combiner))
        {
            if (combiner.errorMessage)
                pgxc_node_report_error(&combiner);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                         errmsg("Failed to FINISH the transaction on one or more nodes")));
        }
        else
            CloseCombiner(&combiner);
    }

	pfree_pgxc_all_handles(pgxc_handles);
    pgxc_handles = NULL;
    pfree(connections);
    connections = NULL;
	return true;
}

bool clean_2PC_files(txn_info * txn)
{
	int ii;
	TupleTableSlots result;
	bool issuccess = true;
	const char *STMT_FORM = "select public.pgxc_remove_2pc_records('%s')::text";
	char query[MAX_CMD_LENGTH];
	
	snprintf(query, MAX_CMD_LENGTH, STMT_FORM, txn->gid);

	for (ii = 0; ii < dn_nodes_num; ii++)
	{
		if (execute_query_on_single_node(dn_node_list[ii], query, 1, &result) == (Datum) 1)
		{
			if (TTSgetvalue(&result, 0, 0) == NULL)
			{
				elog(LOG, "pg_clean: delete 2PC file failed of transaction %s on node %s",
						  txn->gid, get_pgxc_nodename(txn->dnparts[ii]));
				issuccess = false;
			}
		}
		else
		{
			elog(WARNING, "pg_clean: failed to remove 2pc file on node %s, gid: %s, "
				"sql: %s", get_pgxc_nodename(dn_node_list[ii]), txn->gid, query);
			issuccess = false;
		}
		DropTupleTableSlots(&result);
		if (!issuccess)
			return false;
	}

	for (ii = 0; ii < cn_nodes_num; ii++)
	{
		if (execute_query_on_single_node(cn_node_list[ii], query, 1, &result) == (Datum) 1)
		{
			if (TTSgetvalue(&result, 0, 0) == NULL)
			{
				elog(LOG, "pg_clean: delete 2PC file failed of transaction %s on node %s",
						  txn->gid, get_pgxc_nodename(txn->coordparts[ii]));
				issuccess = false;
			}
		}
		else
		{
			elog(WARNING, "pg_clean: failed to remove 2pc file on node %s, gid: %s, "
				"sql: %s", get_pgxc_nodename(cn_node_list[ii]), txn->gid, query);
			issuccess = false;
		}
		DropTupleTableSlots(&result);
		if (!issuccess)
			return false;
	}
	return true;
}

void Init_print_txn_info(print_txn_info * print_txn)
{
	database_info *cur_database = head_database_info;
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn;

	print_txn->index = 0;
	INIT(print_txn->txn);

	for (; cur_database; cur_database = cur_database->next)
	{
        txn = cur_database->all_txn_info;
        hash_seq_init(&status, txn);
        while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
        {
            if (clear_2pc_belong_node && !cur_txn->belong_abnormal_node)
            {
                continue;
            }
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				PALLOC(print_txn->txn, cur_txn);
        }
        
#if 0
		cur_txn = cur_database->head_txn_info;
		for (; cur_txn; cur_txn = cur_txn->next)
		{
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				PALLOC(print_txn->txn, cur_txn);
		}
#endif
	}
}

void Init_print_stats_all(print_status *pstatus)
{
	database_info *cur_database;
	txn_info *cur_txn;
	HASH_SEQ_STATUS status;
    HTAB *txn;

	pstatus->index = 0;
	pstatus->count = 0;
	INIT(pstatus->gid);
	INIT(pstatus->global_status);
	INIT(pstatus->status);
	INIT(pstatus->database);

	for (cur_database = head_database_info; cur_database; cur_database = cur_database->next)
	{
        txn = cur_database->all_txn_info;
        hash_seq_init(&status, txn);
        while ((cur_txn = (txn_info *) hash_seq_search(&status)) != NULL)
        {
			cur_txn->global_txn_stat = check_txn_global_status(cur_txn);
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				Init_print_stats(cur_txn, cur_database->database_name, pstatus);
        }
#if 0
		for (cur_txn = cur_database->head_txn_info; cur_txn; cur_txn = cur_txn->next)
		{
			cur_txn->global_txn_stat = check_txn_global_status(cur_txn);
			if (cur_txn->global_txn_stat != TXN_STATUS_INPROGRESS)
				Init_print_stats(cur_txn, cur_database->database_name, pstatus);
		}
#endif
	}
}

void Init_print_stats(txn_info *txn, char *database, print_status * pstatus)
{
	int ii;
	StringInfoData	query;	
	initStringInfo(&query);

	RPALLOC(pstatus->gid);
	RPALLOC(pstatus->global_status);
	RPALLOC(pstatus->status);
	RPALLOC(pstatus->database);

	pstatus->gid[pstatus->count] = (char *)palloc0(MAX_GID);
	pstatus->database[pstatus->count] = (char *)palloc0(MAX_DBNAME);
	pstatus->global_status[pstatus->count] = (char *)palloc0(MAX_FIELD_LEN);

	strncpy(pstatus->gid[pstatus->count], txn->gid, MAX_GID);
	strncpy(pstatus->database[pstatus->count], database, MAX_DBNAME);
	strncpy(pstatus->global_status[pstatus->count],
		txn_status_to_string(check_txn_global_status(txn)), MAX_FIELD_LEN);

	for (ii = 0; ii < pgxc_clean_node_count; ii++)
	{
		appendStringInfo(&query, "%-12s:%-15s", get_pgxc_nodename(find_node_oid(ii)), 
						txn_status_to_string(txn->txn_stat[ii]));
		if (ii < pgxc_clean_node_count - 1)
		{
			appendStringInfoChar(&query, '\n');
		}
	}

	pstatus->status[pstatus->count] = (char *)palloc0((strlen(query.data)+1));
	strncpy(pstatus->status[pstatus->count], query.data, strlen(query.data)+1);
	pstatus->gid_count++;
	pstatus->database_count++;
	pstatus->global_status_count++;
	pstatus->status_count++;
	pstatus->count++;
}

static const char *txn_status_to_string(TXN_STATUS status)
{
	switch (status)
	{
		ENUM_TOCHAR_CASE(TXN_STATUS_INITIAL)
	    ENUM_TOCHAR_CASE(TXN_STATUS_UNKNOWN)
	    ENUM_TOCHAR_CASE(TXN_STATUS_PREPARED)
	    ENUM_TOCHAR_CASE(TXN_STATUS_COMMITTED)       
	    ENUM_TOCHAR_CASE(TXN_STATUS_ABORTED)
	    ENUM_TOCHAR_CASE(TXN_STATUS_INPROGRESS)
	    ENUM_TOCHAR_CASE(TXN_STATUS_FAILED)
	}
	return NULL;
}

static const char *txn_op_to_string(OPERATION op)
{
	switch (op)
	{
		ENUM_TOCHAR_CASE(UNDO)
	    ENUM_TOCHAR_CASE(ABORT)
	    ENUM_TOCHAR_CASE(COMMIT)
	}
	return NULL;
}


static void 
CheckFirstPhase(txn_info *txn)
{
//    int ret;
    Oid orignode = txn->origcoord;
    uint32 startxid = txn->startxid;
//    uint32 transactionid;
    int nodeidx;

    /*
     * if the twophase trans does not success in prepare phase, the orignode == InvalidOid.
     */
    if (InvalidOid == orignode)
    {
        return;
    }
    nodeidx = find_node_index(orignode);
    if (0 == txn->xid[nodeidx])
    {
        txn->xid[nodeidx] = startxid;
    }
    /* start node participate */
    if (txn->isorigcoord_part)
    {
        if (0 == txn->coordparts[nodeidx])
        {
            txn->coordparts[nodeidx] = 1;
            txn->num_coordparts++;
        }
        if (txn->txn_stat[nodeidx] == TXN_STATUS_INITIAL)
        {
            /*select * from pgxc_is_committed...*/
            getTxnStatus(txn, nodeidx);
        }
        if (txn->txn_stat[nodeidx] == TXN_STATUS_PREPARED && txn->global_commit_timestamp != InvalidGlobalTimestamp)
        {
            txn->after_first_phase = true;
        }
    }
    /* start node node participate */
    else
    {
        if (txn->global_commit_timestamp != InvalidGlobalTimestamp)
        {
            txn->after_first_phase = true;
        } else {
            txn->after_first_phase = false;
        }
    }
}

void get_transaction_handles(PGXCNodeAllHandles **pgxc_handles, txn_info *txn)
{
    int dn_index = 0;
    int cn_index = 0;
    int  nodeIndex;
    char nodetype;
	List *coordlist = NIL;
	List *nodelist = NIL;
    
    while (dn_index < dn_nodes_num)
    {

        /* Get node type and index */
        nodetype = PGXC_NODE_NONE;
        if (TXN_STATUS_PREPARED != txn->txn_stat[dn_index + cn_nodes_num])
        {
            dn_index++;
            continue;
        }
        nodeIndex = PGXCNodeGetNodeIdFromName(get_pgxc_nodename(dn_node_list[dn_index]), &nodetype);
        if (nodetype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            get_pgxc_nodename(dn_node_list[dn_index]))));

        /* Check if node is requested is the self-node or not */
        if (nodetype == PGXC_NODE_DATANODE)
        {
            nodelist = lappend_int(nodelist, nodeIndex);
        }
        dn_index++;

    }

    while (cn_index < cn_nodes_num)
    {
        /* Get node type and index */
        nodetype = PGXC_NODE_NONE;
        if (TXN_STATUS_PREPARED != txn->txn_stat[cn_index] || cn_node_list[cn_index] == txn->origcoord)
        {
            cn_index++;
            continue;
        }
        nodeIndex = PGXCNodeGetNodeIdFromName(get_pgxc_nodename(cn_node_list[cn_index]), &nodetype);
        if (nodetype == PGXC_NODE_NONE)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("PGXC Node %s: object not defined",
                            get_pgxc_nodename(cn_node_list[cn_index]))));

        /* Check if node is requested is the self-node or not */
        if (nodetype == PGXC_NODE_COORDINATOR)
        {
            coordlist = lappend_int(coordlist, nodeIndex);
        }
        cn_index++;
    }
    *pgxc_handles = get_handles(nodelist, coordlist, false, true, FIRST_LEVEL, InvalidFid, true, false);
}

void get_node_handles(PGXCNodeAllHandles **pgxc_handles, Oid nodeoid)
{
    char nodetype = PGXC_NODE_NONE;
	int nodeIndex;
	List *coordlist = NIL;
	List *nodelist = NIL;

	nodeIndex = PGXCNodeGetNodeIdFromName(get_pgxc_nodename(nodeoid), &nodetype);
	if (nodetype == PGXC_NODE_COORDINATOR)
	{
		coordlist = lappend_int(coordlist, nodeIndex);
	}
    else
    {
        nodelist = lappend_int(nodelist, nodeIndex);
    }
	*pgxc_handles = get_handles(nodelist, coordlist, false, true, FIRST_LEVEL, InvalidFid, true, false);
}

bool check_2pc_start_from_node(txn_info *txn)
{
	char node_type;

	if (InvalidOid == abnormal_nodeoid)
	{
		elog(PANIC, "gid: %s, abnormal_nodeoid is invalid", txn->gid);
	}

	if (abnormal_nodeoid == txn->origcoord)
	{
		return true;
	}

	node_type = get_pgxc_nodetype(abnormal_nodeoid);
	if (node_type == 'D')
	{
		return false;
	}

	if (InvalidOid == txn->origcoord)
	{
		int node_oid = InvalidOid;

		if (!IsXidImplicit(txn->gid))
		{
			return true;
		}

		/* Get start node oid from gid */
		node_oid = get_start_node_oid_from_gid(txn->gid);
		if (InvalidOid == node_oid)
		{
			elog(WARNING, "Get invalid start node oid from gid(%s)", txn->gid);
			return false;
		}

		elog(DEBUG1, "Get start node oid(%d) from gid(%s)", node_oid, txn->gid);

		if (abnormal_nodeoid == node_oid)
		{
			return true;
		}
	}

	return false;
}

/*
 * get_start_node_from_gid
 * Get start node name from gid
 * gid: 2pc gid
 */
char *get_start_node_from_gid(char *gid)
{
	char *str_start_node = NULL;

	if (!IsXidImplicit(gid))
	{
		elog(WARNING, "2PC '%s' is not implicit", gid);
		return NULL;
	}

	/* Get start node name from gid */
	str_start_node = strtok(gid, ":");
	if (str_start_node == NULL)
	{
		elog(WARNING, "Get start node from gid(%s) failed", gid);
		return NULL;
	}

	str_start_node = strtok(NULL, ":");
	if (str_start_node == NULL)
	{
		elog(WARNING, "Get start node from gid(%s) failed", gid);
		return NULL;
	}

	return str_start_node;
}

/*
 * get_start_node_oid_from_gid
 * Get start node oid from gid
 * gid: 2pc gid
 */
Oid get_start_node_oid_from_gid(char *gid)
{
	Oid start_node_oid = 0;
	char *str_start_node = NULL;
	char gid_buf[MAX_GID];

	/* Get start node oid from gid */
	strcpy(gid_buf, gid);
	str_start_node = get_start_node_from_gid(gid_buf);
	if (str_start_node == NULL)
	{
		elog(WARNING, "Get start node from gid(%s) failed", gid);
		return 0;
	}

	elog(LOG, "Get start node(%s) from gid(%s)", str_start_node, gid);

	start_node_oid = get_pgxc_nodeoid(str_start_node);
	if (!OidIsValid(start_node_oid))
	{
		elog(WARNING, "Get invalid oid for start node(%s) from gid(%s)",
			str_start_node, gid);
		return 0;
	}

	return start_node_oid;
}

/*
 * get_start_xid_from_gid
 * Get start xid from gid
 * gid: 2pc gid
 */
uint32 get_start_xid_from_gid(char *gid)
{
	uint32 start_xid = 0;
	char *str_start_xid = NULL;
	char gid_buf[MAX_GID];

	if (!IsXidImplicit(gid))
	{
		elog(WARNING, "2PC '%s' is not implicit", gid);
		return 0;
	}

	/* Get start xid from gid */
	strcpy(gid_buf, gid);
	str_start_xid = gid_buf + strlen(XIDPREFIX);
	str_start_xid = strtok(str_start_xid, ":");
	start_xid = strtoul(str_start_xid, NULL, 10);
	if (start_xid == 0)
	{
		elog(WARNING, "Get start xid from gid(%s) failed", gid);
		return 0;
	}

	return start_xid;
}

/*
 * is_xid_running_on_node
 * Whether the transaction with the xid is still running on the node
 * xid: transaction id
 * node_oid: node oid
 */
bool is_xid_running_on_node(uint32 xid, Oid node_oid)
{
	bool is_running = true;

	Datum execute_res;
	TupleTableSlots result;
	char command[MAX_CMD_LENGTH];

	if (xid == 0 || node_oid == InvalidOid)
	{
		elog(PANIC, "2PC xid: %d, node oid: %d", xid, node_oid);
		return true;
	}

	snprintf(command, MAX_CMD_LENGTH, "select pid::text, backend_xid::text "
			"from pg_catalog.pg_stat_activity where backend_xid=%d", xid);

	execute_res = execute_query_on_single_node(node_oid, command, 2, &result);
	if (execute_res == (Datum) 1)
	{
		if (result.slot_count == 0)
		{
			is_running = false;
		}
		else
		{
			is_running = true;

			if (result.slot_count != 1)
			{
				elog(PANIC, "Get %d resules for xid: %d", result.slot_count, xid);
			}
		}
	}
	else
	{
		is_running = true;
		elog(ERROR, "pg_clean: failed to query txn activity on node %s, sql: %s",
			get_pgxc_nodename(node_oid), command);
	}
	DropTupleTableSlots(&result);

	return is_running;
}

/*
 * is_gid_start_xid_running
 * Whether the transaction with the start xid is still running on start node
 * gid: 2pc gid
 */
bool is_gid_start_xid_running(char *gid)
{
	uint32 start_xid = 0;
	Oid start_node_oid = InvalidOid;

	if (!IsXidImplicit(gid))
	{
		elog(LOG, "Explicit 2PC '%s'", gid);
		return true;
	}

	/* Get start xid from gid */
	start_xid = get_start_xid_from_gid(gid);
	if (start_xid == 0)
	{
		elog(ERROR, "Get start xid from gid(%s) failed", gid);
		return true;
	}

	elog(LOG, "Get start xid(%d) from gid(%s)", start_xid, gid);

	/* Get start node oid from gid */
	start_node_oid = get_start_node_oid_from_gid(gid);
	if (!OidIsValid(start_node_oid))
	{
		elog(is_for_node_removed ? LOG: ERROR,
			"2pc(%s) get start node oid failed, "
			"maybe it is removed from the cluster", gid);

		return false;
	}

	elog(LOG, "Get start node oid(%d) from gid(%s)", start_node_oid, gid);

	return is_xid_running_on_node(start_xid, start_node_oid);
}

/*
 * is_txn_start_xid_running
 * Whether the transaction with the start xid is still running on start node
 * txn: 2pc transaction info
 */
bool is_txn_start_xid_running(txn_info *txn)
{
	if (txn->startxid != 0)
	{
		if (OidIsValid(txn->origcoord))
		{
			return is_xid_running_on_node(txn->startxid, txn->origcoord);
		}

		Assert(!OidIsValid(txn->origcoord));

		elog(is_for_node_removed ? LOG: ERROR,
			"2pc(%s) get start node oid failed, "
			"maybe it is removed from the cluster, "
			"start xid is %d", txn->gid, txn->startxid);

		return false;
	}

	Assert(txn->startxid == 0);

	if (!OidIsValid(txn->origcoord))
	{
		elog(is_for_node_removed ? LOG: ERROR,
			"2pc(%s) get start node oid failed, "
			"maybe it is removed from the cluster, "
			"start xid is %d", txn->gid, txn->startxid);

		return false;
	}

	Assert(OidIsValid(txn->origcoord));

	if (!IsXidImplicit(txn->gid))
	{
		elog(LOG, "Explicit 2PC '%s' start xid is %d", txn->gid, txn->startxid);
		return false;
	}

	return is_gid_start_xid_running(txn->gid);
}


