#include "postgres.h"
#include "fmgr.h"
#include "c.h"
#include "funcapi.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

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
#include "pgxc/poolmgr.h"
#include "access/tupdesc.h"
#include "access/twophase.h"
#include "access/htup_details.h"
#include "lib/stringinfo.h"
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
#include "utils/gdd_detector.h"
#include "utils/gdd_common.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

#define MAX_GID GIDSIZE
#define MAX_DBNAME	64
#define MAX_RELNAME 64
#define MAX_MODE 30
#define MAX_DEADLOCK 10000
#define MAX_DEADLOCK_CHECKLOOP (10)

/*data structures*/
	/*about lock*/
typedef enum
{
	Lockmode_ASL = 0,	/*AccessShareLock*/
	Lockmode_RSL,		/*RowShareLock*/
	Lockmode_REL,		/*RowExclusiveLock*/
	Lockmode_SUEL,		/*ShareUpdateExclusiveLock*/
	Lockmode_SL,			/*ShareLock*/
	Lockmode_SREL,		/*ShareRowExclusiveLock*/
	Lockmode_EL,			/*ExclusiveLock*/
	Lockmode_AEL 		/*AccessExclusiveLock*/
} MODE;

typedef enum
{
	Locktype_Relation = 0,
	Locktype_Page,
	Locktype_Tuple,
	Locktype_Transactionid,
	Locktype_Object,
 	Locktype_virtualxact,
	Locktype_Userlock, 
	Locktype_Advisory
} LOCKTYPE;

typedef struct
{
	LOCKTYPE	m_locktype;
	char		m_dbname[MAX_DBNAME];
	char		m_relname[MAX_RELNAME];
	uint32 		m_page;
	uint16 		m_tuple;
	MODE		m_mode;
	bool		m_granted;
	uint32		m_transactionid;
	Oid			m_node;		
	uint32		m_pid;
    char *      m_query;
    char *      m_vxid; /* for virutal xid */
} lockinfo;

	/*about deadlock*/
typedef struct
{
	int*	txns;
	int		txns_count;
	int		txns_size;
	bool	killed;
} deadlock;

	/*about transactions*/
typedef struct
{
	int			pre;
	int		 	post;
}Edge;

typedef struct 
{
	char		gid[MAX_GID];	/*globla transactionid*/
	uint32		*pid;			/*Local pid on each node*/
	int			pid_count;
	int			pid_size;
	Oid		 	*node;			/*a global transaction corresponding to multiple nodes*/
	int			node_count;
	int			node_size;
	Oid  		initiator;		/*node initiating the transaction*/
	lockinfo	*hold;			/*hold lock list of the transaction*/
	int			hold_count;
	int			hold_size;
	lockinfo	*wait;			/*wait lock list of the transaction*/
	int			wait_count;
	int			wait_size;
	bool	 	searched;		/*transaction travesal status during deadlock detection*/
	bool		alive;			/*whether the transaction is killed*/
	int*		deadlock;		/*belonging deadlocks*/
	int 		deadlock_count;	/*deadlock count of the transaction*/
	int			deadlock_size;
	Edge*		out;
	int			out_count;
	int			out_size;
	int			wait_txn;
    char*       query;
}transaction;

typedef struct 
{
	int*	stack;			/*stack during depth-first search*/
	int		stack_count;
	int		stack_size;
	int*	stackpre;		/*stores parents of transactions in stack*/
	int		stackpre_count;
	int		stackpre_size;
	int*	path;			/*extended path in depth-first search*/
	int		path_count;
	int		path_size;
	int*	txn_exist;		/*stores index of trasaction[i] in path, 
							txn_exist[txnid] = i; (path[i] = txnid or txn_exist[txnid] = -1;)*/
} deeplist;

	/*about output results*/
typedef struct 
{
	int index;
	char **edge;
	int edge_count;
	int edge_size;
    
    char **nodes;
    int nodes_count;
    int nodes_size;
    
    char **querys;
    int querys_count;
    int querys_size;
} PrintEdge;

typedef struct 
{
	int index;
	char **deadlock;
    char **nodename;
    char **query;
	int deadlock_count;
	int *per_size;
} PrintDeadlock;

typedef struct 
{
	int index;
	char **txn;
	int txn_count;
	int txn_size;
    
    char **cancel_query;
    int cancel_query_count;
    int cancel_query_size;
    
    char **nodename;
    int nodename_count;
    int nodename_size;
} PrintRollbackTxn;

typedef struct 
{
	int index;
	PrintRollbackTxn *Ptxns;
	int	Ptxns_count;
	int Ptxns_size;
} PrintAllRollbackTxns;

typedef struct GddWaitStatusCtx
{
    LockData	*lockData;

    int			waiter;
    int			holder;

} GddWaitStatusCtx;
	
/*function list*/
static void ResetGlobalVariables(void);

	/*plugin entry function*/
Datum	pg_unlock_execute(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_execute);

Datum	pg_unlock_check_deadlock(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_check_deadlock);

Datum	pg_unlock_check_dependency(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_check_dependency);

Datum	pg_unlock_wait_status(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_wait_status);

Datum	pg_unlock_execute_bak(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_execute_bak);

Datum	pg_unlock_check_deadlock_bak(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_check_deadlock_bak);

Datum	pg_unlock_check_dependency_bak(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_check_dependency_bak);

Datum	pg_unlock_killbypid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_unlock_killbypid);

Datum pg_findgxid(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_findgxid);

Datum pg_cancel_backend_msg(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_cancel_backend_msg);

	/*get all the transaction info*/
void	GetAllTransInfo(void);
void	LoadTransaction(Oid node);
void	InitTransaction(int txn_index);
void	add_pid_node(int txn_index, uint32 pid, Oid node);
LOCKTYPE	
		find_locktype(char *locktype);
MODE	find_mode(char *mode);

	/*build transaction dependency gragh*/
void	InitAllEdge(void);
void	InitEdge(int pre, int post);
bool	is_conflict_withtxn(lockinfo *wait, int post_txn);
bool	is_conflict_withlock(lockinfo *wait, lockinfo *hold);
bool	check_include(lockinfo *wait, lockinfo *hold);
void	DropTransaction(int i);
void	DropAlltransactions(void);
void	DropEdge(int id);

	/*find all deadlocks*/
void	InitDeadlock(void);
void	DropDeadlock(deadlock *loop);
void	DropAlldeadlocks(void);
void	DetectDeadlock(void);
void	traverse(deeplist* list);
void	path_deadlock(deeplist * list, int start);
void	InitDeeplist(deeplist* list);
void	DropDeeplist(deeplist* list); 
void	ClearDeeplist(deeplist* list); 

	/*recover all deadlocks*/
void	RecoverDeadlock(void);
void	CountDeadlocks(void);
void	CountWaitTxn(void);
void	SortByDeadlock(int *sort_txnid);
void	quiksort(int *sort_txnid, int low, int high);
void	KillDeadlockByTxn(int txnid);
bool	DeadlockExists(int id);

	/*output results*/
void	InitPrintEdge(PrintEdge *Pedge);
void	DropPrintEdge(PrintEdge *Pedge);
void	InitPrintDeadlock(PrintDeadlock *Pdeadlock);
void	DropPrintDeadlock(PrintDeadlock *Pdeadlock);
void	InitPrinttxn(PrintRollbackTxn *Ptxn);
void	DropPrinttxn(PrintRollbackTxn *Ptxn);
char	*GetGxid(Oid node, uint32 pid);
int		check_node_pid(char *nodename, uint32 pid);
bool	check_exist_gid(char *gid);
void	KillTxn(void *context, int txnid);
static bool transaction_is_atxact(GddCtx *ctx, int txid);

/*global variables*/
static Oid		*cn_node_list = NULL;
static Oid		*dn_node_list = NULL;
static bool		*cn_health_map = NULL;
static bool		*dn_health_map = NULL;
static int		cn_nodes_num;
static int		dn_nodes_num;

static transaction	*
				pgxc_transaction = NULL;	/*stores all transactions*/
static int		pgxc_transaction_count = 0;	/*transaction count*/
static int		pgxc_transaction_size = 0;	/*records capacity of pgxc_transaction*/
static int		**pgxc_edge = NULL;
static deadlock *
				pgxc_deadlock = NULL;
static int		pgxc_deadlock_count = 0;
static int		pgxc_deadlock_size = 0;

static int m_matrix[8][8] = /*conflict info among lock modes*/
{
	{0, 0, 0, 0, 0, 0, 0, 1},
	{0, 0, 0, 0, 0, 0, 1, 1},
	{0, 0, 1, 0, 1, 1, 1, 1},
	{0, 0, 0, 1, 1, 1, 1, 1},
	{0, 0, 1, 1, 0, 1, 1, 1},
	{0, 0, 1, 1, 1, 1, 1, 1},
	{0, 1, 1, 1, 1, 1, 1, 1},
	{1, 1, 1, 1, 1, 1, 1, 1}
};

static void ResetGlobalVariables(void)
{
    cn_node_list = NULL;
    dn_node_list = NULL;
    cn_health_map = NULL;
    dn_health_map = NULL;
    cn_nodes_num = 0;
    dn_nodes_num = 0;

    pgxc_transaction = NULL;	/*stores all transactions*/
    pgxc_transaction_count = 0;	/*transaction count*/
    pgxc_transaction_size = 0;	/*records capacity of pgxc_transaction*/
    pgxc_edge = NULL;

    pgxc_deadlock = NULL;
    pgxc_deadlock_count = 0;
    pgxc_deadlock_size = 0;
}


/* 
 * pg_unlock_execute -- detect and recover deadlocks
 * input: 	no
 * output:	info of rollback transactions
 */
Datum
pg_unlock_execute_bak(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  5
		FuncCallContext 		*funcctx;
		PrintAllRollbackTxns	*Partxns;
		char					**rec;
        char                    **nodename;
        char                    **query;
		HeapTuple				tuple;		
	
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
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "executetime",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "txnindex",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "rollbacktxn(ip:port)",
						   TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "nodename",
						   TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "cancel_query",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = palloc0(sizeof(PrintAllRollbackTxns));
		Partxns = (PrintAllRollbackTxns *)funcctx->user_fctx;
		INIT(Partxns->Ptxns);
		Partxns->index = 0; 

		ResetGlobalVariables();
        /*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));
		do
		{
			/*get all transaction info and associat it to global xid*/
			GetAllTransInfo();
			if (pgxc_transaction_count == 0)
			{
				elog(DEBUG1, "pg_unlock: there is no transaction");
				break;
			}
			
			/*build transaction dependency graph*/
			InitAllEdge();
			
			/*detect deadlocks*/
			DetectDeadlock();
			if (pgxc_deadlock_count == 0)
			{
				/*program ends until there is no deadlock*/
				elog(DEBUG1, "pg_unlock: there is no deadlock");
				break;
			}
			/*recover deadlocks through killing one transaction*/
			RecoverDeadlock();

			/*record output info*/
			RPALLOC(Partxns->Ptxns);
			InitPrinttxn(&(Partxns->Ptxns[Partxns->Ptxns_count]));
			if (Partxns->Ptxns[Partxns->Ptxns_count].txn_count > 0)
			{
				Partxns->Ptxns_count++;
				if (Partxns->Ptxns_count >= MAX_DEADLOCK_CHECKLOOP)
                {
				    /* avoid deadlock all the time */
				    break;
                }
			}
			DropAlldeadlocks();
			DropAlltransactions();
		}while(true);
		MemoryContextSwitchTo(oldcontext);
	}
	
	funcctx = SRF_PERCALL_SETUP();	
	Partxns = (PrintAllRollbackTxns *) funcctx->user_fctx;
	
	if (Partxns->index < Partxns->Ptxns_count)
	{
		PrintRollbackTxn *temp = &(Partxns->Ptxns[Partxns->index]);
		rec = Partxns->Ptxns[Partxns->index].txn;
		nodename = Partxns->Ptxns[Partxns->index].nodename;
		query = Partxns->Ptxns[Partxns->index].cancel_query;
        
		while (temp->index < temp->txn_count)
		{
			MemSet(values, 0, sizeof(values));
			MemSet(nulls, 0, sizeof(nulls));

			if (temp->index == 0)
			{
				values[0] = Int32GetDatum(Partxns->index);
			}
			values[1] = Int32GetDatum(temp->index);
			values[2] = PointerGetDatum(cstring_to_text(rec[temp->index]));
            values[3] = PointerGetDatum(cstring_to_text(nodename[temp->index]));
            values[4] = PointerGetDatum(cstring_to_text(query[temp->index]));
			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			temp->index++;
            if (temp->index < temp->txn_count)
            {
			    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
            }
		}
		Partxns->index++;
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		RFREE(Partxns->Ptxns);
		Partxns->index = 0;               
		DropAlldeadlocks();
		DropAlltransactions();
		pfree(cn_health_map);
		pfree(dn_health_map);
		if (cn_node_list)
		{
			pfree(cn_node_list);
			cn_nodes_num = 0;
		}
		if (dn_node_list)
		{
			pfree(dn_node_list);
			dn_nodes_num = 0;
		}
		SRF_RETURN_DONE(funcctx);
	}
}

/* 
 * pg_unlock_check_deadlock -- detect deadlocks without recover
 * input: 	no
 * output:	info of deadlocks
 */
Datum	pg_unlock_check_deadlock_bak(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 		*funcctx;
	PrintDeadlock			*Pdeadlock;
	char					**rec;
    char                    **nodes;
    char                    **querys;
	HeapTuple				tuple;		

	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "deadlockid",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "deadlocks",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "nodename",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "query",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = palloc0(sizeof(PrintDeadlock));
		Pdeadlock = (PrintDeadlock*)funcctx->user_fctx;
		
		ResetGlobalVariables();
		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));
		
		/*get all transaction info and associat it to global xid*/
		GetAllTransInfo();

		/*build transaction dependency graph*/
		InitAllEdge();

		/*detect deadlocks*/
		DetectDeadlock();

		/*record output info*/
		InitPrintDeadlock(Pdeadlock);
		MemoryContextSwitchTo(oldcontext);
	}
	
	funcctx = SRF_PERCALL_SETUP();	
	Pdeadlock = (PrintDeadlock *) funcctx->user_fctx;
	rec = Pdeadlock->deadlock;
    nodes = Pdeadlock->nodename;
    querys = Pdeadlock->query;
	
	if (Pdeadlock->index < Pdeadlock->deadlock_count)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(Pdeadlock->index);
		values[1] = PointerGetDatum(cstring_to_text(rec[Pdeadlock->index]));
        values[2] = PointerGetDatum(cstring_to_text(nodes[Pdeadlock->index]));
        values[3] = PointerGetDatum(cstring_to_text(querys[Pdeadlock->index]));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		Pdeadlock->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		DropPrintDeadlock(Pdeadlock);
		DropAlldeadlocks();
		DropAlltransactions();
		pfree(cn_health_map);
		pfree(dn_health_map);
		if (cn_node_list)
		{
			pfree(cn_node_list);
			cn_nodes_num = 0;
		}
		if (dn_node_list)
		{
			pfree(dn_node_list);
			dn_nodes_num = 0;
		}
		SRF_RETURN_DONE(funcctx);
	}
}

/* 
 * pg_unlock_check_dependency -- only detect transaction dependency
 * input: 	no
 * output:	info of transaction dependency
 */
Datum	pg_unlock_check_dependency_bak(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
	FuncCallContext 		*funcctx;
	PrintEdge				*Pedge;
	char					**rec;
    char                    **nodes;
    char                    **querys;
	HeapTuple				tuple;		

	Datum		values[ACCESS_CONTROL_ATTR_NUM];
	bool		nulls[ACCESS_CONTROL_ATTR_NUM];

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();
		
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "dependencyid",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dependency",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "nodename",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "query",
						   TEXTOID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		funcctx->user_fctx = palloc0(sizeof(PrintEdge));
		Pedge = (PrintEdge*)funcctx->user_fctx;
		
		ResetGlobalVariables();
		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*get all transaction info and associat it to global xid*/
		GetAllTransInfo();

		/*build transaction dependency graph*/
		InitAllEdge();

		/*record output info*/
		InitPrintEdge(Pedge);
		MemoryContextSwitchTo(oldcontext);
	}
	
	funcctx = SRF_PERCALL_SETUP();	
	Pedge = (PrintEdge *) funcctx->user_fctx;
	rec = Pedge->edge;
    nodes = Pedge->nodes;
    querys = Pedge->querys;
	
	if (Pedge->index < Pedge->edge_count)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(Pedge->index);
		values[1] = PointerGetDatum(cstring_to_text(rec[Pedge->index]));
        values[2] = PointerGetDatum(cstring_to_text(nodes[Pedge->index]));
        values[3] = PointerGetDatum(cstring_to_text(querys[Pedge->index]));
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		Pedge->index++;
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		DropPrintEdge(Pedge);
		DropAlltransactions();
		pfree(cn_health_map);
		pfree(dn_health_map);
		if (cn_node_list)
		{
			pfree(cn_node_list);
			cn_nodes_num = 0;
		}
		if (dn_node_list)
		{
			pfree(dn_node_list);
			dn_nodes_num = 0;
		}
		SRF_RETURN_DONE(funcctx);
	}

}

/* 
 * pg_unlock_killbypid -- kill certain transaction by user
 * input: 	nodename, pid
 * output:	execute result success of error info
 */
Datum	pg_unlock_killbypid(PG_FUNCTION_ARGS)
{
	char	*Kstatus;
	char	*nodename = NULL;
	uint32	kpid;
	int		size = sizeof(char) * 100;
	char	gid[MAX_GID];
	text	*t_status = NULL;
	int		txnindex;
	
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		elog(ERROR, "nodename and pid can not be null");
	}
	nodename = text_to_cstring(PG_GETARG_TEXT_P(0));
	kpid = PG_GETARG_UINT32(1);
	Kstatus = (char *)palloc0(size);

	if(!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "can only called on coordinator");
	}
	
	do
	{
		ResetGlobalVariables();
		/*get node list*/
		PgxcNodeGetOids(&cn_node_list, &dn_node_list,
						&cn_nodes_num, &dn_nodes_num, true);
		cn_health_map = palloc0(cn_nodes_num * sizeof(bool));
		dn_health_map = palloc0(dn_nodes_num * sizeof(bool));

		/*get all transaction info and associat it to global xid*/
		GetAllTransInfo();

		/*find global transaction according to nodename and pid*/
		txnindex = check_node_pid(nodename, kpid);
		if (txnindex < 0)
		{
			snprintf(Kstatus, size, "Fail:error not exists node:%s or pid:%u on node %s", nodename, kpid, nodename);
			break;
		}
		if (get_pgxc_nodetype(get_pgxc_nodeoid(nodename)) != 'C')
		{
			snprintf(Kstatus, size, "Fail:error node:%s is not coordinator", nodename);
			break;
		}
		memcpy(gid, pgxc_transaction[txnindex].gid, sizeof(gid));

		/*kill the transaction*/
		KillTxn(NULL, txnindex);
		DropAlltransactions();
		
		/*check whether this transaction is existed*/
		LoadTransaction(get_pgxc_nodeoid(nodename));
		if(!check_exist_gid(gid))
		{
			snprintf(Kstatus, size, "Success: pid:%u on node %s is killed", kpid, nodename);
			break;
		}
		else
		{
			snprintf(Kstatus, size, "Fail:error pid:%u on node %s is not killed", kpid, nodename);
			break;
		}
	}while(0);
	DropAlltransactions();
	pfree(nodename);
	pfree(cn_health_map);
	pfree(dn_health_map);
	if (cn_node_list)
	{
		pfree(cn_node_list);
		cn_nodes_num = 0;
	}
	if (dn_node_list)
	{
		pfree(dn_node_list);
		dn_nodes_num = 0;
	}
	t_status = cstring_to_text(Kstatus);
	pfree(Kstatus);
	return PointerGetDatum(t_status);
}


/* 
 * GetAllTransInfo -- get all transactions from all nodes and stores them in pgxc_transaction
 * input: 	no
 * return:	no
 */
void GetAllTransInfo(void)
{	
	int	i;
	for (i = 0; i < cn_nodes_num; i++)
	{
		LoadTransaction(cn_node_list[i]);
	}
	for (i = 0; i < dn_nodes_num; i++)
	{
		LoadTransaction(dn_node_list[i]);
	}
}

/*
 * BinarySearchGid -- Binary search gid in pgxc_transaction
 * input: 	gid
 * return:	gid pos or insert pos, was gid found
 */
static int
BinarySearchGid(char *gid, bool *found)
{
    int low = 0;
    int high = pgxc_transaction_count - 1;
    int mid = 0;
    int cmp_result = 0;
    *found = false;

    while (low <= high)
    {
        mid = (low + high) / 2;
        cmp_result = strcmp(gid, pgxc_transaction[mid].gid);
        if (cmp_result == 0)
        {
            /* gid == pgxc_transaction[mid].gid */
            *found = true;
            return mid;
        }
        else if (cmp_result > 0)
        {
            /* gid > pgxc_transaction[mid].gid */
            low = mid + 1;
        }
        else
        {
            /* gid < pgxc_transaction[mid].gid */
            high = mid - 1;
        }
    }

    /* return insert pos */
    return high + 1;
}

/* 
 * LoadTransaction -- get transactions from certain node and stores them in pgxc_transaction
 * input: 	node oid
 * return:	no
 */
void LoadTransaction(Oid node)
{	
	const char *query_stmt = "select a1.pid::text, a1.locktype::text, a2.datname::text, a2.relname::text, "
						 "a1.page::text, a1.tuple::text, a1.mode::text, a1.granted::text, a1.transactionid::text, a3.query::text, pg_findgxid(a1.pid::int)::text, virtualxid::text "
						 "from (select locktype::text, database, relation, page::text, "
									  "tuple::text, mode::text, granted::text, pid::text, transactionid::text, virtualxid "
									  "from pg_locks where (locktype = 'relation' or locktype = 'page' or locktype = 'tuple' or locktype = 'transactionid' or locktype = 'virtualxid')"
									  " and (pid is not null))a1 "
 							  "left join "
 							  "(select distinct pg_database.datname::text, pg_class.relname::text, "
				  								"pg_locks.database, pg_locks.relation "
				  								"from pg_database, pg_class, pg_locks, pg_namespace "
				  								"where pg_database.oid = pg_locks.database and pg_class.oid = pg_locks.relation "
				  									  "and pg_namespace.oid = pg_class.relnamespace and pg_namespace.nspname "
				 									  "not in ('pg_catalog','information_schema'))a2 "
								  "on a1.database = a2.database and a1.relation = a2.relation "
								  "left join "
								  "(select pid::text, query::text from pg_stat_activity)a3 on a1.pid = a3.pid and a3.pid != '%d' "
								  	"order by a1.pid;";

    char query_txnid[2048];

    /*stores tuples in result_txnid*/
	TupleTableSlots	result_txnid;
	int i;
	int i_txn;
	int ntuples; 
	uint32 pid;
	char *temp = NULL;
	char *rel_name = NULL;
	char *db_name = NULL;
	char *ptr = NULL;
	char *gid = NULL;
    int nodeid = 0;
	lockinfo templock;
	bool found = false;

    sprintf(query_txnid, query_stmt, MyProcPid);
	execute_on_single_node(node, query_txnid, 12, &result_txnid);
	if (result_txnid.slot == NULL) 
	{
		elog(DEBUG1, "pg_unlock: there is no transaction on node %s", get_pgxc_nodename(node));
		return;
	}
	
	ntuples = result_txnid.slot_count;
	for (i = 0; i < ntuples; i++)
	{
		pid = strtoul(TTSgetvalue(&result_txnid, i, 0), NULL, 10);
		/*get global xid of pid on node*/
        gid = TTSgetvalue(&result_txnid, i, 10);
		/*select for update apply for transactionid without global xid*/
		if (gid == NULL)
		{
			continue;
		}
		
		/*check whether the gid is already existed*/
        i_txn = BinarySearchGid(gid, &found);
		/*insert this new transaction when gid is not find in pgxc_transaction*/
		if (!found)
		{
			RPALLOC(pgxc_transaction);
            memmove(&pgxc_transaction[i_txn + 1], &pgxc_transaction[i_txn], (pgxc_transaction_count - i_txn) * sizeof(transaction));
			InitTransaction(i_txn);
			memcpy(pgxc_transaction[i_txn].gid, gid, sizeof(char) * MAX_GID);
			pgxc_transaction_count++;
		}
		add_pid_node(i_txn, pid, node);
		ptr = strtok(gid, ":");
        nodeid = atoi(ptr);
        pgxc_transaction[i_txn].initiator = get_nodeoid_from_node_id(nodeid, PGXC_NODE_COORDINATOR);
		//pgxc_transaction[i_txn].initiator = get_pgxc_nodeoid(ptr);

		/*read lockinfo from result_txnid*/
		templock.m_pid = pid;
		templock.m_node = node;
		templock.m_locktype = find_locktype(TTSgetvalue(&result_txnid, i, 1));
		
		/*we only consider the first four locktypes*/
		if (templock.m_locktype > Locktype_virtualxact)
		{
			continue;
		}

		if (templock.m_locktype == Locktype_virtualxact)
		{
			char	*vxid = TTSgetvalue(&result_txnid, i, 11);

			if (vxid == NULL)
				elog(ERROR, "emptry lock object for virtual xid");
			templock.m_vxid = pstrdup(vxid);
		}
 		else
			templock.m_vxid = "";

		db_name = TTSgetvalue(&result_txnid, i, 2);
		if (db_name)
		{
			memcpy(templock.m_dbname, db_name, strlen(db_name)+1);
		}
		else
		{
			MemSet(templock.m_dbname, 0, sizeof(templock.m_dbname));
		}
		rel_name = TTSgetvalue(&result_txnid, i, 3);
		if (rel_name)
		{
			memcpy(templock.m_relname, rel_name, strlen(rel_name)+1);
		}
		else
		{
			MemSet(templock.m_relname, 0, sizeof(templock.m_relname));
		}
		if (TTSgetvalue(&result_txnid, i, 4) != NULL)
		{
			templock.m_page = strtoul(TTSgetvalue(&result_txnid, i, 4), NULL, 10);
		}
		else
		{
			templock.m_page = 0;
		}
		if (TTSgetvalue(&result_txnid, i, 5) != NULL)
		{
			templock.m_tuple = strtoul(TTSgetvalue(&result_txnid, i, 5), NULL, 10);
		}
		else
		{
			templock.m_tuple = 0;
		}
		templock.m_mode = find_mode(TTSgetvalue(&result_txnid, i, 6));
		if (TTSgetvalue(&result_txnid, i, 8) != NULL)
		{
			templock.m_transactionid = strtoul(TTSgetvalue(&result_txnid, i, 8), NULL, 10);
		}
		else
		{
			templock.m_transactionid = 0;
		}
		temp = TTSgetvalue(&result_txnid, i, 7);

        if (TTSgetvalue(&result_txnid, i, 9)) 
        {
            if (strlen(TTSgetvalue(&result_txnid, i, 9)) <= 1024) 
            {
                templock.m_query = (char *)pstrdup(TTSgetvalue(&result_txnid, i, 9));
            } 
            else
            {
                templock.m_query = (char *)palloc0(1025);
                strncpy(templock.m_query, TTSgetvalue(&result_txnid, i, 9), 1024);
            }
        } 
        else 
        {
            templock.m_query = NULL;
        }
		/*put templock into transaction hold list or wait list due to granted*/
		if (strcmp(temp, "true") == 0)
		{
			templock.m_granted = true;
			PALLOC(pgxc_transaction[i_txn].hold, templock); 
		}
		else
		{
			templock.m_granted = false;
			PALLOC(pgxc_transaction[i_txn].wait, templock); 
		}
        if (pgxc_transaction[i_txn].initiator == node) 
        {
            if (templock.m_query) 
            {
                pgxc_transaction[i_txn].query = pstrdup(templock.m_query);
            } 
            else 
            {
                pgxc_transaction[i_txn].query = pstrdup("unknown");
            }
        }

	}
	DropTupleTableSlots(&result_txnid);
}

void InitTransaction(int txn_index)
{
	transaction *temp;
	temp = pgxc_transaction;
	if (temp == NULL)
	{
		elog(LOG, "pg_unlock: error pgxc_transaction is null");
		exit(1);
	}
	INIT(temp[txn_index].pid);
	INIT(temp[txn_index].node);
	INIT(temp[txn_index].hold);
	INIT(temp[txn_index].wait);
	INIT(temp[txn_index].out);
	temp[txn_index].searched = false;
	temp[txn_index].alive = true;
	INIT(temp[txn_index].deadlock);
	temp[txn_index].wait_txn = 0;
	temp[txn_index].query = NULL;
    temp[txn_index].initiator = 0;
}

/* 
 * add_pid_node -- add pid and node to certain transaction
 * input: 	index of transaction, pid, node oid
 * return:	void
 */
void add_pid_node(int txn_index, uint32 pid, Oid node)
{
	transaction *temp;
	temp = pgxc_transaction;

	PALLOC(temp[txn_index].pid, pid);
	PALLOC(temp[txn_index].node, node);
}

LOCKTYPE find_locktype(char * locktype)
{
	LOCKTYPE j;
	if (strcmp(locktype, "relation") == 0)
	{
		j = Locktype_Relation;
	}
	else if (strcmp(locktype, "page") == 0)
	{
		j = Locktype_Page;
	}
	else if (strcmp(locktype, "tuple") == 0)
	{
		j = Locktype_Tuple;
	}
	else if (strcmp(locktype, "transactionid") == 0)
	{
		j = Locktype_Transactionid;
	}
	else if (strcmp(locktype, "object") == 0)
	{
		j = Locktype_Object;
	}
	else if (strcmp(locktype, "userlock") == 0)
	{
		j = Locktype_Userlock;
	}
	else if (strcmp(locktype, "advisory") == 0)
	{
		j = Locktype_Advisory;
	}
	else if (strcmp(locktype, "virtualxid") == 0)
	{
		j = Locktype_virtualxact;
	}
	else
	{
		elog(LOG, "pg_unlock: unknown locktype: %s", locktype);
		exit (1);
	}
	return j;
}

MODE find_mode(char *mode)
{
	MODE i;
	if (strcmp(mode, "AccessShareLock") == 0)
	{
		i = Lockmode_ASL;
	}
	else if (strcmp(mode, "RowShareLock") == 0)
	{
		i = Lockmode_RSL;
	}
	else if (strcmp(mode, "RowExclusiveLock") == 0)
	{
		i = Lockmode_REL;
	}
	else if (strcmp(mode, "ShareUpdateExclusiveLock") == 0)
	{
		i = Lockmode_SUEL;
	}
	else if (strcmp(mode, "ShareLock") == 0)
	{
		i = Lockmode_SL;
	}
	else if (strcmp(mode, "ShareRowExclusiveLock") == 0)
	{
		i = Lockmode_SREL;
	}
	else if (strcmp(mode, "ExclusiveLock") == 0)
	{
		i = Lockmode_EL;
	}
	else if (strcmp(mode, "AccessExclusiveLock") == 0)
	{
		i = Lockmode_AEL;
	}
	else
	{
		elog(LOG, "pg_unlock: unkown lock mode %s", mode);
		exit (1);
	}
	return i; 
}

/* 
 * InitAllEdge -- build all transaction dependency graph and stores in pgxc_transaction, pgxc_edge
 * input: 	no
 * return:	no
 */
void InitAllEdge(void)
{
	int i;
	int j;
	pgxc_edge = (int **)palloc(pgxc_transaction_count * sizeof(int *));
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		pgxc_edge[i] = (int *)palloc(pgxc_transaction_count * sizeof(int));
		for (j = 0; j < pgxc_transaction_count; j++)
		{
			pgxc_edge[i][j] = 0;
		}
	}
	
	/*search for all edges*/
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		for (j = 0; j < pgxc_transaction_count; j++)
		{
			if (i == j)
			{
				continue;
			}
			InitEdge(i, j);
		}
	}
}

/* 
 * InitEdge -- build dependency between two transactions and stores it in pgxc_transaction, pgxc_edge
 * input: 	pre transaction index, post transaction index
 * return:	no
 */
void InitEdge(int pre, int post) 
{
	int i;
	int out_count;
	Edge *out = NULL;
	int	pre_end = pgxc_transaction[pre].wait_count;
	lockinfo *pre_wait = pgxc_transaction[pre].wait;
	
	for (i = 0; i < pre_end; i++)
	{
		/*if lock pre_wait[i] conflict with pgxc_transaction[post]*/
		if (is_conflict_withtxn(pre_wait+i, post))
		{
			RPALLOC(pgxc_transaction[pre].out);
			out = pgxc_transaction[pre].out;
			out_count = pgxc_transaction[pre].out_count;
			out[out_count].pre = pre;
			out[out_count].post = post;
			pgxc_transaction[pre].out_count++;
			pgxc_edge[pre][post] = 1;
			break;
		}
	}
}

/* 
 * is_conflict_withtxn -- build dependency between two transactions and stores it in pgxc_transaction, pgxc_edge
 * input: 	pre transaction index, post transaction index
 * return:	conflict or not
 */
bool is_conflict_withtxn(lockinfo *wait, int post_txn)
{
	bool conflict = false;
	lockinfo *hold = pgxc_transaction[post_txn].hold;
	int hold_count = pgxc_transaction[post_txn].hold_count;
	int i;
	for (i = 0; i < hold_count; i++)
	{
		if (is_conflict_withlock(wait, hold + i))
		{
			conflict = true;
			break;
		}
	}
	return conflict;
}

/* 
 * is_conflict_withlock -- build dependency between two locks
 * input: 	pre lockinfo, post lockinfo
 * return:	conflict or not
 */
bool is_conflict_withlock(lockinfo *wait, lockinfo *hold)
{
	bool conflict = false;
	bool sameobject = true;
	
	/*locks of same granted will not conflict*/
	if (wait->m_node != hold->m_node || wait->m_granted == hold->m_granted)
	{
		return conflict;
	}
	
	/*locks of different locktype will not conflict*/
	if ((wait->m_locktype < Locktype_Transactionid) ^ (hold->m_locktype < Locktype_Transactionid))
	{
		sameobject = false;
	}
	
	/*check locktype among relation, page and tuple*/
	else if(wait->m_locktype < Locktype_Transactionid && hold->m_locktype < Locktype_Transactionid) 
	{
		if ((strcmp(wait->m_dbname, hold->m_dbname) == 0) && !check_include(wait, hold))
		{
			sameobject = false;
		}
	}
	
	/*check between transactionid*/
	else if(wait->m_locktype == Locktype_Transactionid && hold->m_locktype == Locktype_Transactionid)
	{
		if (wait->m_node != hold->m_node || wait->m_transactionid != hold->m_transactionid)
		{
			sameobject = false;
		}	
	}
	else if (wait->m_locktype == Locktype_virtualxact && hold->m_locktype == Locktype_virtualxact)
	{
		if (wait->m_node != hold->m_node || strcmp(wait->m_vxid, hold->m_vxid) != 0)
		{
			sameobject = false;
		}
	}
	else
		sameobject = false;

	/*check locktype among relation, page and tuple*/
	if (sameobject == true)
	{
		conflict = (m_matrix[(int)wait->m_mode][(int)hold->m_mode] == 1);
	}
	return conflict;
}

bool check_include(lockinfo *wait, lockinfo *hold)
{
	bool include = false;
	LOCKTYPE i = wait->m_locktype;
	LOCKTYPE j = hold->m_locktype;
	int min;
	int max;
	
	if ((i >= Locktype_Transactionid) || (j >= Locktype_Transactionid))
	{
		return include;
	}
	min = i <= j ? i : j;
	max = i <= j ? j : i;
	switch (min)
	{
		case Locktype_Relation:
			if (strcmp(wait->m_relname, hold->m_relname) == 0)
			{
				include = true;
			}
			break;
		case Locktype_Page:
			if (strcmp(wait->m_relname, hold->m_relname) == 0)
			{
				/*locks in same relation and page or 
				relation lock and page lock of the same relation*/
				if ((i != j) || (wait->m_page == hold->m_page))
				{
					include = true;
				}
			}
			break;
		case Locktype_Tuple:
			if (strcmp(wait->m_relname, hold->m_relname) == 0)
			{
				if (max == Locktype_Relation)
				{
					include = true;
					break;
				}
				if (wait->m_page == hold->m_page)
				{
					if (max == Locktype_Page)
					{
						include = true;
						break;
					}
					if (wait->m_tuple == hold->m_tuple)
					{
						if (max == Locktype_Tuple)
						{
							include = true;
							break;
						}
					}
				}
			}
			break;
		default:
			elog(LOG, "pg_unlock: could not match locktype %d to relation, page or tuple", min);
			break;
	}
	return include;
}

void InitDeadlock(void)
{
	RPALLOC(pgxc_deadlock);
	INIT(pgxc_deadlock[pgxc_deadlock_count].txns);
	RPALLOC(pgxc_deadlock[pgxc_deadlock_count].txns);
	pgxc_deadlock[pgxc_deadlock_count].killed = false;
	return;
}

void DropDeadlock(deadlock *loop)
{
	RFREE(loop->txns);
	loop->killed = false;
	return;
}

void DropAlldeadlocks(void)
{
	int i;
	for (i = pgxc_deadlock_count - 1; i >= 0; i--)
	{
		DropDeadlock(pgxc_deadlock+i);
	}
	RFREE(pgxc_deadlock);
}

/* 
 * DetectDeadlock -- detect deadlock according to transaction dependency and store them in pgxc_deadlock
 * input: 	no
 * return:	no
 */
void DetectDeadlock(void)
{
	int i;
	deeplist dfs;
    
	InitDeeplist(&dfs);
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		if (pgxc_deadlock_count > MAX_DEADLOCK)
		{
			break;
		}
		
		/*we can find all the deadlocks that conclude the transaction through tranvers it*/
		if (pgxc_transaction[i].searched == true)
		{
			continue;
		}
		else
		{
			/*push i into stack*/
			PALLOC(dfs.stack, i);
			PALLOC(dfs.stackpre, -1);
		}
		while (dfs.stack_count != 0 )
		{
    		if (pgxc_deadlock_count > MAX_DEADLOCK)
    		{
    			break;
    		}

			traverse(&dfs);
		}
		ClearDeeplist(&dfs);
	}
	DropDeeplist(&dfs);
}

/* 
 * traverse -- traverse according to transaction dependency and store them in list->path
 * input: 	deeplist
 * return:	index of deadlock start transaction in path
 */
void traverse(deeplist* list)
{
	/*pop the last element in stack*/
	int i;
	int post;
	int start = list->stack[list->stack_count - 1]; 
	int startpre = list->stackpre[list->stackpre_count - 1];
	
	list->stack_count--;
	list->stackpre_count--;
	pgxc_transaction[start].searched = true;

	/*delete element in path, if the pop element in stack is not its post*/
	if (list->path_count > 0)
	{
		while(list->path[list->path_count-1] != startpre)
		{
			list->path_count--;
			list->txn_exist[list->path[list->path_count]] = -1;
		}
	}
	
	/*push the pop element into path*/
	PALLOC(list->path, start);
	list->txn_exist[start] = list->path_count-1;
	
	/*find all the outedge of the above pop element*/
	for (i = 0; i < pgxc_transaction[start].out_count; i++)
	{
		post = pgxc_transaction[start].out[i].post;
		
		/*if the transaction post does not exit in path*/
		if (list->txn_exist[post] < 0)
		{
			PALLOC(list->stack, post);
			PALLOC(list->stackpre, start);
		}
		/*or return the index of path according to the transaction*/
		else
		{
            path_deadlock(list, list->txn_exist[post]);
		}
	}
	return;
}

/* 
 * path_deadlock -- add element in path to pgxc_deadlock
 * input: 	deeplist, index of deadlock start element in path
 * return:	no
 */
void path_deadlock(deeplist *list, int start)
{
	deadlock *loop = NULL;
	int i;
	int ii;
	int ij;
	int total_count = list->path_count - start;
	bool isexist = false;
	int ii_txns_count;
	int ij_txns_count;
	
	InitDeadlock();
	loop = pgxc_deadlock+pgxc_deadlock_count;
	
	for (i = start; i < list->path_count; i++)
	{
		PALLOC(loop->txns, list->path[i]);
	}
	/*first check whether the deadlock is exits*/
	for (i = 0; i < pgxc_deadlock_count; i++)
	{
		if (pgxc_deadlock[i].txns_count == total_count)
		{
			isexist = true;
			ii_txns_count = pgxc_deadlock[i].txns_count;
			ij_txns_count = loop->txns_count * 2 - 1;
			for (ii = 0, ij = 0; ii < ii_txns_count && ij < ij_txns_count;)
			{
				if (pgxc_deadlock[i].txns[ii] != loop->txns[ij % loop->txns_count])
				{
					if (ii == 0 && ij < loop->txns_count)
					{
						ij++;
					}
					else
					{
						/*deadlock not exist*/
						isexist = false;	
						break;
					}
				}
				else
				{
					ii++;
					ij++;
				}
			}
			if (isexist == true)
			{
				break;
			}
			/*deadlock in list[start~path_count-1] is already exist*/
		}
	}
	
	if (isexist == false)
	{
		pgxc_deadlock_count++;
	}
	else
	{
		RFREE(loop->txns);
	}
	/*if not existed then insert into pgxc_deadlock*/
	return;
}

void InitDeeplist(deeplist* list)
{
	int i; 
	INIT(list->stack);
	INIT(list->stackpre);
	INIT(list->path);
	list->txn_exist = (int *)palloc(pgxc_transaction_count * sizeof(int));
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		list->txn_exist[i] = -1;
	}
	return;
}

void ClearDeeplist(deeplist * list)
{
	int i = 0;
	list->stack_count = 0;
	list->stackpre_count = 0;
	list->path_count = 0;
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		list->txn_exist[i] = -1;
	}
	return;
}

void DropDeeplist(deeplist * list)
{
	RFREE(list->stack);
	RFREE(list->stackpre);
	RFREE(list->path);
	pfree(list->txn_exist);
	list->txn_exist = NULL;
	return;
}

/*
 * Normally, atxact is hung up, and could not accept cancel signal. Then
 * will pick up a active transaction to kill.
 */
static int
pickup_nonatxact(int *sort_txnid)
{
	int	j = 0;
	int	tidx = -1;
	bool	has_atxact = false;

	/* Loop for atxact */
	for (; j < pgxc_transaction_count; j++)
	{
		int	txnid = sort_txnid[j];
		uint32	*pid = pgxc_transaction[txnid].pid;
		int		i = 0;

		if (DeadlockExists(txnid) == false)
			continue;

		for (i = 0; i < pgxc_transaction[txnid].node_count; i++)
		{
			if ((int) pid[i] < 0)
			{
				has_atxact = true;
				break;
			}
		}

		/* record the first active xact */
		if (i == pgxc_transaction[txnid].node_count && tidx == -1)
			tidx = txnid;

		if (tidx != -1 && has_atxact)
			break;
	}

	if (!has_atxact)
		return sort_txnid[0];

	if (tidx == -1)
		tidx = sort_txnid[0];
	return tidx;
}

/* 
 * RecoverDeadlock -- kill at most one transaction in each deadlock
 * input: 	no
 * return:	no
 */
void RecoverDeadlock(void)
{
	int* sort_txnid = NULL;
	if (pgxc_deadlock_count == 0)
	{
		return;
	}

	sort_txnid = (int *)palloc(pgxc_transaction_count * sizeof(int));
	/*Count deadlocks belong to each transactions*/
	CountDeadlocks();
	CountWaitTxn();

	/*sort transaction index by deadlock count*/
	SortByDeadlock(sort_txnid);
	/*first kill transaction with the most deadlocks*/
	KillDeadlockByTxn(pickup_nonatxact(sort_txnid));
	pfree(sort_txnid);
	return;
}

void CountDeadlocks(void)
{
	int i;
	int j;
	
	for (i = 0; i < pgxc_deadlock_count; i++)
	{
		for (j = 0; j < pgxc_deadlock[i].txns_count; j++)
		{
			PALLOC(pgxc_transaction[pgxc_deadlock[i].txns[j]].deadlock, i);
		}
	}
	return;
}

void SortByDeadlock(int *sort_txnid)
{
	int i;
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		sort_txnid[i] = i;
	}
	quiksort(sort_txnid, 0, pgxc_transaction_count-1);
}

void quiksort(int *sort_txnid, int low, int high)
{
	int i = low;
	int j = high;  
	int temp = sort_txnid[i]; 
  
	if( low > high)
	{		   
	   return ;
	}
	while(i < j) 
	{
		while(((pgxc_transaction[sort_txnid[j]].deadlock_count
			< pgxc_transaction[temp].deadlock_count) 
				|| ((pgxc_transaction[sort_txnid[j]].deadlock_count
					== pgxc_transaction[temp].deadlock_count)
					&& (pgxc_transaction[sort_txnid[j]].wait_txn
						<= pgxc_transaction[temp].wait_txn))) 
			&& (i < j))
		{ 
			j--; 
		}
		sort_txnid[i] = sort_txnid[j];
		while(((pgxc_transaction[sort_txnid[i]].deadlock_count
			> pgxc_transaction[temp].deadlock_count)
				|| ((pgxc_transaction[sort_txnid[j]].deadlock_count
					== pgxc_transaction[temp].deadlock_count) 
					&& (pgxc_transaction[sort_txnid[j]].wait_txn
						>= pgxc_transaction[temp].wait_txn)))
			&& (i < j))
		{
			i++; 
		}  
		sort_txnid[j]= sort_txnid[i];
	}
	 sort_txnid[i] = temp;
	 quiksort(sort_txnid,low,i-1);
	 quiksort(sort_txnid,j+1,high);
}


/* 
 * KillDeadlockByTxn -- kill certain transaction
 * input: 	transaction index
 * return:	no
 */
void KillDeadlockByTxn(int txnid)
{
	int i;
	transaction *txn = pgxc_transaction;
	Oid* node = pgxc_transaction[txnid].node;
	uint32* pid = pgxc_transaction[txnid].pid;
	char query[500];
	TupleTableSlots result;

	if (DeadlockExists(txnid) == false)
	{
		return;
	}
	
	txn[txnid].alive = false;
	for (i = 0; i < txn[txnid].deadlock_count; i++)
	{
		pgxc_deadlock[txn[txnid].deadlock[i]].killed = true;
	}
	
	for (i = 0; i < pgxc_transaction[txnid].node_count; i++)
	{
		if ((int) pid[i] < 0) /* Never a negative value */
			continue;
		snprintf(query, 500,"select pg_cancel_backend_msg(%u, 'distributed deadlock detected');", pid[i]);
		execute_on_single_node(node[i], query, 0, &result);
		DropTupleTableSlots(&result);
	}
	return;
}

bool DeadlockExists(int id)
{
	bool res = false;
	transaction *txn = pgxc_transaction;
	int i;
	for (i = 0; i < txn[id].deadlock_count; i++)
	{
		if (pgxc_deadlock[txn[id].deadlock[i]].killed == false)
		{
			res = true;
		}
	}
	return res;
}

void DropTransaction(int i)
{
	transaction *txn = pgxc_transaction;
    
	txn[i].gid[0] = '\0';
	txn[i].searched = false;
	txn[i].alive = true;
	txn[i].wait_txn = 0;
    
	RFREE(txn[i].pid);
	RFREE(txn[i].node);
	if (txn[i].hold_size && txn[i].hold->m_query)
	{
		pfree(txn[i].hold->m_query);
		txn[i].hold->m_query = NULL;
	}
	RFREE(txn[i].hold);
	if (txn[i].wait_size && txn[i].wait->m_query)
	{
		pfree(txn[i].wait->m_query);
		txn[i].wait->m_query = NULL;
	}
	RFREE(txn[i].wait);
	RFREE(txn[i].deadlock);
	RFREE(txn[i].out);
	if (txn[i].query)
	{
		pfree(txn[i].query);
		txn[i].query = NULL;
	}
}

void DropAlltransactions(void)
{
	int i;
	
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		DropTransaction(i);
	}

	if (pgxc_edge != NULL)
	{
		for (i = 0; i < pgxc_transaction_count; i++)
		{
			pfree(pgxc_edge[i]);
		}
		if (pgxc_transaction_count)
		{
			pfree(pgxc_edge);
		}
		pgxc_edge = NULL;
	}
    
	RFREE(pgxc_transaction);
}

void InitPrintEdge(PrintEdge *Pedge)
{
	int i;
	int j;
	int index1;
	int index2;
    int len = 0;
	
    Pedge->index = 0;
	INIT(Pedge->edge);
    INIT(Pedge->nodes);
    INIT(Pedge->querys);
	RPALLOC(Pedge->edge);
	RPALLOC(Pedge->nodes);
	RPALLOC(Pedge->querys);
	
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		for (j = 0; j < pgxc_transaction[i].out_count; j++)
		{
			RPALLOC(Pedge->edge);
			Pedge->edge[Pedge->edge_count] = (char *) palloc(2*MAX_GID*sizeof(char) + 10);
			
			index1 = pgxc_transaction[i].out[j].pre;
			index2 = pgxc_transaction[i].out[j].post;
			snprintf(Pedge->edge[Pedge->edge_count], 2*MAX_GID*sizeof(char) + 10, "%s --> %s", 
					pgxc_transaction[index1].gid, pgxc_transaction[index2].gid);
            
            RPALLOC(Pedge->nodes);
			Pedge->nodes[Pedge->nodes_count] = (char *) palloc(2*NAMEDATALEN*sizeof(char) + 10);
            snprintf(Pedge->nodes[Pedge->nodes_count], 2*NAMEDATALEN*sizeof(char) + 10, "%s --> %s", 
					get_pgxc_nodename(pgxc_transaction[index1].initiator), 
					get_pgxc_nodename(pgxc_transaction[index2].initiator));
            
            RPALLOC(Pedge->querys);
            len = 0;
            if (pgxc_transaction[index1].query) 
            {
                len += strlen(pgxc_transaction[index1].query);
            }
            if (pgxc_transaction[index2].query) 
            {
                len += strlen(pgxc_transaction[index2].query);
            }
			Pedge->querys[Pedge->querys_count] = (char *) palloc(len+ 10);
			snprintf(Pedge->querys[Pedge->querys_count], len+10, "%s --> %s", 
					pgxc_transaction[index1].query, pgxc_transaction[index2].query);

            Pedge->edge_count++;
			Pedge->nodes_count++;
			Pedge->querys_count++;
		}
	}
}

void DropPrintEdge(PrintEdge *Pedge)
{
	int i;
    if (NULL == Pedge)
    {
        return;
    }
	for (i = 0; i < Pedge->edge_count; i++)
	{
		pfree(Pedge->edge[i]);
	}
	RFREE(Pedge->edge);
    
	for (i = 0; i < Pedge->nodes_count; i++)
	{
		pfree(Pedge->nodes[i]);
	}
	RFREE(Pedge->nodes);

    for (i = 0; i < Pedge->querys_count; i++)
	{
		pfree(Pedge->querys[i]);
	}
	RFREE(Pedge->querys);
    Pedge->index = 0;
	Pedge = NULL;
}

void InitPrintDeadlock(PrintDeadlock *Pdeadlock)
{
	int i;
	int j;		
	StringInfoData	query;
    StringInfoData  nodename;
    StringInfoData  deadlock_query;
	
	Pdeadlock->index = 0;
	Pdeadlock->deadlock = NULL;
	Pdeadlock->deadlock_count = pgxc_deadlock_count;
	Pdeadlock->per_size = (int *)palloc(pgxc_deadlock_count * sizeof(int));
	Pdeadlock->deadlock = (char **)palloc(pgxc_deadlock_count * sizeof(char *));
    Pdeadlock->nodename = (char **)palloc(pgxc_deadlock_count * sizeof(char *));
    Pdeadlock->query = (char **)palloc(pgxc_deadlock_count * sizeof(char *));

	for (i = 0; i < pgxc_deadlock_count; i++)
	{
		Pdeadlock->per_size[i] = pgxc_deadlock[i].txns_count*(MAX_GID+10)*sizeof(char);
		Pdeadlock->deadlock[i] = (char *) palloc(Pdeadlock->per_size[i]);
        Pdeadlock->nodename[i] = (char *) palloc(pgxc_deadlock[i].txns_count * NAMEDATALEN);
		
		initStringInfo(&query);
        initStringInfo(&nodename);
        initStringInfo(&deadlock_query);

		for (j = 0; j < pgxc_deadlock[i].txns_count; j++)
		{
			appendStringInfo(&query, "%-15s(%-15s:%-12d)", pgxc_transaction[pgxc_deadlock[i].txns[j]].gid, 
					get_pgxc_nodehost(pgxc_transaction[pgxc_deadlock[i].txns[j]].initiator), 
					get_pgxc_nodeport(pgxc_transaction[pgxc_deadlock[i].txns[j]].initiator));
            appendStringInfo(&nodename, "%s", get_pgxc_nodename(pgxc_transaction[pgxc_deadlock[i].txns[j]].initiator));
            appendStringInfo(&deadlock_query, "%s", pgxc_transaction[pgxc_deadlock[i].txns[j]].query);
			if (j < pgxc_deadlock[i].txns_count-1)
			{
				appendStringInfoChar(&query, '\n');
				appendStringInfoChar(&nodename, '\n');
				appendStringInfoChar(&deadlock_query, '\n');
			}
		}
		snprintf(Pdeadlock->deadlock[i], Pdeadlock->per_size[i], "%s", query.data);
        snprintf(Pdeadlock->nodename[i], pgxc_deadlock[i].txns_count * NAMEDATALEN, "%s", nodename.data);
        
        Pdeadlock->query[i] = (char *) palloc(deadlock_query.len + 1);
        snprintf(Pdeadlock->query[i], deadlock_query.len + 1, "%s", deadlock_query.data);
	}
}

void DropPrintDeadlock(PrintDeadlock *Pdeadlock)
{
	int i;
	for (i = 0; i < Pdeadlock->deadlock_count; i++)
	{
		pfree(Pdeadlock->deadlock[i]);
        pfree(Pdeadlock->nodename[i]);
        pfree(Pdeadlock->query[i]);
	}
	pfree(Pdeadlock->deadlock);
	pfree(Pdeadlock->nodename);
	pfree(Pdeadlock->query);
	pfree(Pdeadlock->per_size);
	Pdeadlock->deadlock = NULL;
	Pdeadlock->nodename = NULL;
	Pdeadlock->query = NULL;
	Pdeadlock->per_size = NULL;
	Pdeadlock->index = 0;
	Pdeadlock->deadlock_count = 0;
}

void InitPrinttxn(PrintRollbackTxn *Ptxn)
{
	int i;
    int len;
	
	Ptxn->index = 0;
	INIT(Ptxn->txn);
	INIT(Ptxn->nodename);
	INIT(Ptxn->cancel_query);

	for (i = 0; i < pgxc_transaction_count; i++)
	{
		if (pgxc_transaction[i].alive == false)
		{
			RPALLOC(Ptxn->txn);
			Ptxn->txn[Ptxn->txn_count] = (char *) palloc((MAX_GID+10) * sizeof(char));
			sprintf(Ptxn->txn[Ptxn->txn_count], "%-15s(%-15s:%-15d)", pgxc_transaction[i].gid,
											get_pgxc_nodehost(pgxc_transaction[i].initiator), 
											get_pgxc_nodeport(pgxc_transaction[i].initiator));
			RPALLOC(Ptxn->nodename);
			Ptxn->nodename[Ptxn->nodename_count] = (char *) palloc(NAMEDATALEN);
            sprintf(Ptxn->nodename[Ptxn->nodename_count], "%s", get_pgxc_nodename(pgxc_transaction[i].initiator));
            
			RPALLOC(Ptxn->cancel_query);
            len = 0;
            if (pgxc_transaction[i].query) 
            {
                len += strlen(pgxc_transaction[i].query);
                Ptxn->cancel_query[Ptxn->cancel_query_count] = (char *) palloc0(len + 1);
                sprintf(Ptxn->cancel_query[Ptxn->cancel_query_count], "%s", pgxc_transaction[i].query);
            } 
            else
            {
                Ptxn->cancel_query[Ptxn->cancel_query_count] = (char *) palloc0(10);
                sprintf(Ptxn->cancel_query[Ptxn->cancel_query_count], "unknown");
            }
            
			Ptxn->txn_count++;
            Ptxn->nodename_count++;
            Ptxn->cancel_query_count++;
		}
	}
}

void DropPrinttxn(PrintRollbackTxn *Ptxn)
{
	int i;
	for (i = 0; i < Ptxn->txn_count; i++)
	{
		pfree(Ptxn->txn[i]);
	}

	for (i = 0; i < Ptxn->cancel_query_count; i++)
	{
		pfree(Ptxn->cancel_query[i]);
	}

	for (i = 0; i < Ptxn->nodename_count; i++)
	{
		pfree(Ptxn->nodename[i]);
	}
	RFREE(Ptxn->txn);
	RFREE(Ptxn->cancel_query);
	RFREE(Ptxn->nodename);
	Ptxn->index = 0;
	Ptxn = NULL;
}

/* 
 * GetGxid -- get global xid of certain pid on certain node
 * input: 	node oid, pid
 * return:	global xid
 */
char *GetGxid(Oid node, uint32 pid)
{
	char *res = NULL;
	char *temp = NULL;
	TupleTableSlots result;	
	char query[100];
	
	snprintf(query, 100, "select pg_findgxid(%u)", pid); 
	execute_on_single_node(node, query, 1, &result);
	if (result.slot == NULL) 
	{
		elog(LOG, "pg_unlock: could not obtain global transactionid from pid %u on node %s", pid, get_pgxc_nodename(node));
		return res;
	}
	temp = TTSgetvalue(&result, 0, 0);
	if (temp != NULL)
	{
		res = (char *)palloc(MAX_GID * sizeof(char));
		strncpy(res, temp, MAX_GID * sizeof(char));
	}
	DropTupleTableSlots(&result);
	return res;
}

/* 
 * pg_findgxid -- get global xid of certain pid
 * input: 	pid
 * return:	global xid
 */
Datum pg_findgxid(PG_FUNCTION_ARGS)
{
	uint32	pid = PG_GETARG_UINT32(0);
	char *globalXid = GetGlobalTransactionId((int) pid);
	text *t_gxid = NULL;
	if (globalXid != NULL)
	{
		t_gxid = cstring_to_text(globalXid);
		return	PointerGetDatum(t_gxid);
	}
	PG_RETURN_NULL();
}

Datum
pg_cancel_backend_msg(PG_FUNCTION_ARGS)
{
	pid_t	pid;
	char	*msg = NULL;
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_BOOL(0);
	}
	pid = PG_GETARG_INT32(0);
	if (!PG_ARGISNULL(1))
	{
		msg = text_to_cstring(PG_GETARG_TEXT_PP(1));
	}

	PG_RETURN_BOOL(pg_cancel_backend_internal(pid, msg));
}

/* 
 * check_node_pid -- check whether certain pid on certain node exists
 * input: 	nodename, pid
 * return:	exist or not
 */
int check_node_pid(char *nodename, uint32 pid)
{
	int res = -1;
	int i;
	int j;
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		if (strcmp(get_pgxc_nodename(pgxc_transaction[i].initiator) , nodename) == 0)
		{
			for (j = 0; j < pgxc_transaction[i].pid_count; j++)
			{
				if (pid == pgxc_transaction[i].pid[j])
				{
					res = i;
				}
			}
		}
	}
	return res;
}

/* 
 * KillTxn -- kill certain transaction
 * input: 	transaction index
 * return:	no
 */
void KillTxn(void *context, int txnid)
{
	int i;
	TupleTableSlots result;
	char query[500];
	Oid* node = pgxc_transaction[txnid].node;
	uint32* pids = pgxc_transaction[txnid].pid;
	
	for (i = 0; i < pgxc_transaction[txnid].node_count; i++)
	{
		int	pid = (int) pids[i];

		if (pid < 0)
			continue;
		snprintf(query, 500,"select pg_cancel_backend_msg(%d, 'cancelled by pg_unlock');", pid);
		execute_on_single_node(node[i], query, 0, &result);
		DropTupleTableSlots(&result);
	}
    pgxc_transaction[txnid].alive = false;
	return;
}

/* 
 * check_exist_gid -- check whether certain transaction exists
 * input: 	transaction global xid
 * return:	exist or not
 */
bool check_exist_gid(char *gid)
{
	bool res = false;
	int i;
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		if (strcmp(pgxc_transaction[i].gid, gid) == 0)
		{
			res = true;
		}
	}
	return res;
}

void CountWaitTxn(void)
{
	int i;
	int j;
	for (i = 0; i < pgxc_transaction_count; i++)
	{
		for (j = 0; j < pgxc_transaction_count; j++)
		{
			if (pgxc_edge[i][j] == 1)
			{
				pgxc_transaction[j].wait_txn++;
			}
		}
	}
}

/*
 * is the lock held
 */
static bool
IsGranted(LockInstanceData *lock)
{
	return lock->holdMask != 0;
}

/*
 * wait for a lock
 */
static bool
WaitForLock(LockInstanceData *lock)
{
	return lock->waitLockMode != 0;
}

/*
 * is the same lock
 */
static bool
LockEqual(LockInstanceData *lock1, LockInstanceData *lock2)
{
    LOCKTAG		*tag1 = &lock1->locktag;
    LOCKTAG		*tag2 = &lock2->locktag;

    return memcmp(tag1, tag2, sizeof(LOCKTAG)) == 0;
}

/*
 * is the lock hold until end of xact
 */
static bool
LockIsHoldTillEndXact(LockInstanceData *lock)
{
    LOCKTAG		*tag = &lock->locktag;

    if (lock->holdTillEndXact)
        return true;

    if (tag->locktag_type == LOCKTAG_TRANSACTION)
        return true;

    return false;
}

/*
 * lock is the LOCKTAG_RELATION|LOCKTAG_PAGE|LOCKTAG_TUPLE|LOCKTAG_TRANSACTION
 * only handle these types now
 */
static bool
LockIsGetWaitType(LockInstanceData *lock)
{
    LOCKTAG		*tag = &lock->locktag;
    if (tag->locktag_type == LOCKTAG_RELATION || tag->locktag_type == LOCKTAG_PAGE ||
        tag->locktag_type == LOCKTAG_TUPLE || tag->locktag_type == LOCKTAG_TRANSACTION ||
		tag->locktag_type == LOCKTAG_VIRTUALTRANSACTION)
    {
        return true;
    }
    return false;
}

/*
 * pg_unlock_wait_status - produce a view with one row per waiting relation
 */
Datum
pg_unlock_wait_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple	tuple;
	Datum		result;
	Datum		values[8];
	bool		nulls[8];
	char        tnbuf[32];
	GddWaitStatusCtx *ctx;

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(8, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "node_name",
						TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "waiter_gxid",
						TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "holder_gxid",
						TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "holdTillEndXact",
						BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "waiter_lpid",
						INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "holder_lpid",
						INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "waiter_lockmode",
						TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "waiter_locktype",
						TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		ctx = palloc(sizeof(*ctx));
		funcctx->user_fctx = ctx;

		ctx->waiter = 0;
		ctx->holder = 0;

		/*
		 * QD information must be collected after the dispatch.
		 * Thus the wait relations on QD returned by this function
		 * guarantee to be more later than the wait relations on QEs.
		 * And this information can help while reducing edges on QD.
		 *
		 * Rule: When we are to reduce a dotted edge on QD, we have to think
		 * more. If the lock-holding vertex(transaction) of this QD-edge,
		 * is blocked on any QE. We should not reduce the edge at least
		 * now.
		 *
		 * If any QEs wait relation is later than QD's, we cannot use the
		 * above rule.
		 */
		ctx->lockData = GetLockStatusData();

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = funcctx->user_fctx;

	/*
	 * Find out all the waiting relations
	 *
	 * A relation is that:
	 * (waiter.granted == false &&
	 *  holder.granted == true &&
	 *  waiter.waitLockMode conflict with holder.holdMask &&
	 *  waiter.pid != holder.pid &&
	 *  waiter.locktype == holder.locktype &&
	 *  waiter.locktags == holder.locktags)
	 *
	 * The actual logic here is to perform a nested loop like this:
	 *
	 *     for (waiter = 0; waiter < nelements; waiter++)
	 *         for (holder = 0; holder < nelements; holder++)
	 *             if (is_a_relation(waiter, holder))
	 *                 record the relation;
	 *
	 * But as we can only return one relation per call, we need to
	 * simulate the loops manually.
	 *
	 * One transaction can wait for more than one transaction on the same
	 * segment. For example, Tx A, B, and C all open a relation with shared
	 * mode, and Tx D try to open the same relation with exclusive mode, D
	 * will be blocked. There are 3 waiting edges(D-->A, D--->B, D--->C).
	 */
	while (ctx->waiter < ctx->lockData->nelements)
	{
		int			waiter = ctx->waiter;
		LockInstanceData	   *w_lock = &ctx->lockData->locks[waiter];

		/* A waiter, the waitLockMode should not be 0 */
		if (!WaitForLock(w_lock)|| !LockIsGetWaitType(w_lock))
		{
			ctx->waiter++;
			ctx->holder = 0;
			continue;
		}

		while (ctx->holder < ctx->lockData->nelements)
		{
			char* w_dxid;
			char* h_dxid;
			const char	*locktypename;
			int			holder = ctx->holder++;
			LockInstanceData	*h_lock = &ctx->lockData->locks[holder];
			bool	tillxact;

			if (holder == waiter)
				continue;
			if (!IsGranted(h_lock) && !WaitForLock(w_lock))
				continue;
			if (w_lock->pid == h_lock->pid)
				continue;
			/* If waiter and holder have different lock methods, they must not be conflict */
			if (w_lock->locktag.locktag_lockmethodid != h_lock->locktag.locktag_lockmethodid)
				continue;
			/* If waiter and holder are not conflict, should skip this edge */
			if (!CheckWaitLockModeConflictHoldMask(w_lock->locktag,
													w_lock->waitLockMode,
													h_lock->holdMask,
													h_lock->waitLockMode))
				continue;
			if (!LockEqual(w_lock, h_lock))
				continue;
			/* A valid waiting relation is found */

			/*
			 * Find out dxid differently on QD a nd QE.
			 */
			w_dxid = GetGlobalTransactionId(w_lock->pid);
			h_dxid = GetGlobalTransactionId(h_lock->pid);

			if (w_dxid == NULL || h_dxid == NULL)
			{
				continue;
			}

			if (strcmp(w_dxid, h_dxid) == 0 &&
				h_lock->locktag.locktag_type == LOCKTAG_VIRTUALTRANSACTION &&
				(w_lock->pid < 0) && (h_lock->holdMask & LOCKBIT_ON(ExclusiveLock)))
			{
				if (!IsLockExists(&h_lock->locktag, ExclusiveLock))
					continue;
			}

			tillxact = (LockIsHoldTillEndXact(w_lock) ||
						w_lock->locktag.locktag_type == LOCKTAG_VIRTUALTRANSACTION);
			values[0] = CStringGetTextDatum(PGXCNodeName);
			values[1] = CStringGetTextDatum(w_dxid);
			values[2] = CStringGetTextDatum(h_dxid);
			values[3] = BoolGetDatum(tillxact);
			values[4] = Int32GetDatum(w_lock->pid);
			values[5] = Int32GetDatum(h_lock->pid);
			values[6] = CStringGetTextDatum(GetLockmodeName(w_lock->locktag.locktag_lockmethodid,
						w_lock->waitLockMode));

			if (w_lock->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
				locktypename = LockTagTypeNames[w_lock->locktag.locktag_type];
			else
			{
				snprintf(tnbuf, sizeof(tnbuf), "unknown %d",
							(int) w_lock->locktag.locktag_type);
				locktypename = tnbuf;
			}
			values[7] = CStringGetTextDatum(locktypename);

			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);

			SRF_RETURN_NEXT(funcctx, result);
		}

		ctx->waiter++;
		ctx->holder = 0;
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * store gxid in pgxc_transaction, and transfer gxid to txnid
 */
static int
TransferGxid2Txnid(char* gxid)
{
    int i_txn;

    /*check whether the gid is already existed*/
    for (i_txn = 0; i_txn < pgxc_transaction_count; i_txn++)
    {
        if (strcmp(gxid, pgxc_transaction[i_txn].gid) == 0)
        {
            break;
        }
    }

    /*insert this new transaction when gid is not find in pgxc_transaction*/
    if (i_txn >= pgxc_transaction_count)
    {
        RPALLOC(pgxc_transaction);
        InitTransaction(pgxc_transaction_count);
        memcpy(pgxc_transaction[pgxc_transaction_count].gid, gxid, strlen(gxid) + 1);
        i_txn = pgxc_transaction_count++;
    }

    return i_txn;
}

/*
 * init necessary information on pgxc_transaction use the gdd context
 */
static void
InitGlobalTxnInfo(GddCtx *ctx, bool is_add_pid_node)
{
    int out_count;
    Edge *out = NULL;
    GddMapIter	graphiter;
    GddMapIter	vertiter;
    GddListIter	edgeiter;
    int nodeid;

    gdd_ctx_foreach_vert(graphiter, vertiter, ctx)
    {
        GddVert *vert = gdd_map_iter_get_ptr(vertiter);
        if (is_add_pid_node)
        {
            add_pid_node(vert->id, vert->pid, gdd_map_iter_get_key(graphiter));
        }
        else
        {
            gdd_vert_foreach_out_edge(edgeiter, vert)
            {
                GddEdge		*edge = gdd_list_iter_get_ptr(edgeiter);
                RPALLOC(pgxc_transaction[vert->id].out);
                out = pgxc_transaction[vert->id].out;
                out_count = pgxc_transaction[vert->id].out_count;
                out[out_count].pre = vert->id;
                out[out_count].post = edge->to->id;
                pgxc_transaction[vert->id].out_count++;
            }
        }

        if (pgxc_transaction[vert->id].initiator == 0)
        {
            nodeid = atoi(pgxc_transaction[vert->id].gid);
            pgxc_transaction[vert->id].initiator = get_nodeoid_from_node_id(nodeid, PGXC_NODE_COORDINATOR);
        }
    }
}


/*
 * handle each wait relation tuple, and put the transaction info
 * into our global variable pgxc_transaction instead of gddcontex,
 * because we will provide it to other functions later
 */
static void
HandleWaitRelationTuple(void *context, TupleTableRecord *tuple)
{
    GddCtx *ctx = (GddCtx *)context;
    char *temp = NULL;
    int from_txnid;
    int to_txnid;
    char* query;
    char*  waiter_gxid;
    char*  holder_gxid;
    int  waiter_pid;
    int  holder_pid;
    bool   solidedge;
    GddEdge *edge;
    char* node_name;
    int node;
    Assert(ctx != NULL);

    /* 1. nodename */
    node_name = RecordGetvalue(tuple, 0);
    node = get_pgxc_nodeoid(node_name);

    /* 2. waiter gxid */
    waiter_gxid = RecordGetvalue(tuple, 1);
    /* 3. hloder gxid */
    holder_gxid = RecordGetvalue(tuple, 2);
    from_txnid = TransferGxid2Txnid(waiter_gxid);
    to_txnid = TransferGxid2Txnid(holder_gxid);

    /* 4. holdtillendxact */
    temp = RecordGetvalue(tuple, 3);
    solidedge =  (strcmp(temp, "true") == 0) ? true : false;

    /* 5. waiter pid */
    waiter_pid = strtoul(RecordGetvalue(tuple, 4), NULL, 10);
    /* 6. holder pid */
    holder_pid = strtoul(RecordGetvalue(tuple, 5), NULL, 10);

    /* waiter lockmode and waiter locktype not used here */

    if (pgxc_transaction[from_txnid].query == NULL)
    {
        /* 9. waiter query sql */
        query = RecordGetvalue(tuple, 8);
        if (query)
        {
            if (strlen(query) <= 1024)
            {
                pgxc_transaction[from_txnid].query = (char *)pstrdup(query);
            }
            else
            {
                pgxc_transaction[from_txnid].query = (char *)palloc0(1025);
                strncpy(pgxc_transaction[from_txnid].query, query, 1024);
            }
        }
    }

    /* store wait relation into gdd context */
    edge = GddCtxAddEdge(ctx, node, from_txnid, to_txnid, solidedge);
    edge->from->pid = waiter_pid;
    edge->to->pid = holder_pid;
}

/*
 * pg_unlock_check_dependency -- only detect transaction dependency
 * input: 	no
 * output:	info of transaction dependency
 */
Datum	pg_unlock_check_dependency(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
    FuncCallContext 		*funcctx;
    PrintEdge				*Pedge;
    char					**rec;
    char                    **nodes;
    char                    **querys;
    HeapTuple				tuple;
    GddCtx		 *ctx;

    Datum		values[ACCESS_CONTROL_ATTR_NUM];
    bool		nulls[ACCESS_CONTROL_ATTR_NUM];

	if (!IS_PGXC_COORDINATOR)
		elog(ERROR, "pg_unlock can only be called on coordinator!");

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        TupleDesc	tupdesc;
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "dependencyid",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dependency",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "nodename",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "query",
                           TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(PrintEdge));
        Pedge = (PrintEdge*)funcctx->user_fctx;

        ResetGlobalVariables();

        ctx = GddCtxNew();
		ctx->is_atxact_transaction = transaction_is_atxact;

        BuildWaitGraph(ctx, HandleWaitRelationTuple);

        if (!GddCtxEmpty(ctx))
        {
            InitGlobalTxnInfo(ctx, false);
        }

        /*record output info*/
        InitPrintEdge(Pedge);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Pedge = (PrintEdge *) funcctx->user_fctx;
    rec = Pedge->edge;
    nodes = Pedge->nodes;
    querys = Pedge->querys;

    if (Pedge->index < Pedge->edge_count)
    {
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[0] = Int32GetDatum(Pedge->index);
        values[1] = PointerGetDatum(cstring_to_text(rec[Pedge->index]));
        values[2] = PointerGetDatum(cstring_to_text(nodes[Pedge->index]));
        values[3] = PointerGetDatum(cstring_to_text(querys[Pedge->index]));
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        Pedge->index++;
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * pg_unlock_check_deadlock -- detect deadlocks without recover
 * input: 	no
 * output:	info of deadlocks
 */
Datum
pg_unlock_check_deadlock(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  4
    FuncCallContext 		*funcctx;
    PrintDeadlock			*Pdeadlock;
    char					**rec;
    char                    **nodes;
    char                    **querys;
    HeapTuple				tuple;
    GddCtx		 *ctx;

    Datum		values[ACCESS_CONTROL_ATTR_NUM];
    bool		nulls[ACCESS_CONTROL_ATTR_NUM];

	if (!IS_PGXC_COORDINATOR)
		elog(ERROR, "pg_unlock can only be called on coordinator!");

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        TupleDesc	tupdesc;
        funcctx = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(ACCESS_CONTROL_ATTR_NUM, false);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "deadlockid",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "deadlocks",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "nodename",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "query",
                           TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(PrintDeadlock));
        Pdeadlock = (PrintDeadlock*)funcctx->user_fctx;

        ResetGlobalVariables();

        ctx = GddCtxNew();
		ctx->is_atxact_transaction = transaction_is_atxact;

        BuildWaitGraph(ctx, HandleWaitRelationTuple);

        GddCtxReduce(ctx);

        if (!GddCtxEmpty(ctx))
        {
            /*
             * At least one deadlock cycle is detected, and as all the invalid
             * verts and edges were filtered out at the beginning, the left
             * deadlock cycles are all valid ones.
             */
            InitGlobalTxnInfo(ctx, false);
            DetectDeadlock();
        }

        /*record output info*/
        InitPrintDeadlock(Pdeadlock);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Pdeadlock = (PrintDeadlock *) funcctx->user_fctx;
    rec = Pdeadlock->deadlock;
    nodes = Pdeadlock->nodename;
    querys = Pdeadlock->query;

    if (Pdeadlock->index < Pdeadlock->deadlock_count)
    {
        MemSet(values, 0, sizeof(values));
        MemSet(nulls, 0, sizeof(nulls));

        values[0] = Int32GetDatum(Pdeadlock->index);
        values[1] = PointerGetDatum(cstring_to_text(rec[Pdeadlock->index]));
        values[2] = PointerGetDatum(cstring_to_text(nodes[Pdeadlock->index]));
        values[3] = PointerGetDatum(cstring_to_text(querys[Pdeadlock->index]));
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        Pdeadlock->index++;
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/* 
 * pg_unlock_execute -- detect and recover deadlocks
 * input: 	no
 * output:	info of rollback transactions
 */
Datum
pg_unlock_execute(PG_FUNCTION_ARGS)
{
#ifdef ACCESS_CONTROL_ATTR_NUM
#undef ACCESS_CONTROL_ATTR_NUM
#endif
#define ACCESS_CONTROL_ATTR_NUM  5
    FuncCallContext 		*funcctx;
    PrintAllRollbackTxns	*Partxns;
    char					**rec;
    char                    **nodename;
    char                    **query;
    HeapTuple				tuple;
    GddCtx		 *ctx;

    Datum		values[ACCESS_CONTROL_ATTR_NUM];
    bool		nulls[ACCESS_CONTROL_ATTR_NUM];

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
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "executetime",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "txnindex",
                           INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "rollbacktxn(ip:port)",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "nodename",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "cancel_query",
                           TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        funcctx->user_fctx = palloc0(sizeof(PrintAllRollbackTxns));
        Partxns = (PrintAllRollbackTxns *)funcctx->user_fctx;
        INIT(Partxns->Ptxns);
        Partxns->index = 0;

        mycontext = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
                                          "GddContext",
                                          ALLOCSET_DEFAULT_SIZES);

        ResetGlobalVariables();

        do
        {
            MemoryContextSwitchTo(mycontext);

            ctx = GddCtxNew();
			ctx->is_atxact_transaction = transaction_is_atxact;

            BuildWaitGraph(ctx, HandleWaitRelationTuple);

            if (pgxc_transaction_count == 0)
            {
                elog(DEBUG1, "pg_unlock: there is no transaction");
                break;
            }

            GddCtxReduce(ctx);

            if (GddCtxEmpty(ctx))
            {
                /*program ends until there is no deadlock*/
                elog(DEBUG1, "pg_unlock: there is no deadlock");
                break;
            }

            /*
             * At least one deadlock cycle is detected, and as all the invalid
             * verts and edges were filtered out at the beginning, the left
             * deadlock cycles are all valid ones.
             */
            InitGlobalTxnInfo(ctx, true);

            BreakDeadLock(ctx, KillTxn);

            MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

            /*record output info*/
            RPALLOC(Partxns->Ptxns);
            InitPrinttxn(&(Partxns->Ptxns[Partxns->Ptxns_count]));
            if (Partxns->Ptxns[Partxns->Ptxns_count].txn_count > 0)
            {
                Partxns->Ptxns_count++;
                if (Partxns->Ptxns_count >= MAX_DEADLOCK_CHECKLOOP)
                {
                    /* avoid deadlock all the time */
                    elog(LOG, "pg_unlock: check deadlock more than %d times, break now", MAX_DEADLOCK_CHECKLOOP);
                    break;
                }
            }

            DropAlldeadlocks();
            DropAlltransactions();
            /* avoid  too much memory due to detecting multiple times, reset here*/
            MemoryContextReset(mycontext);
        } while(true);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    Partxns = (PrintAllRollbackTxns *) funcctx->user_fctx;

    if (Partxns->index < Partxns->Ptxns_count)
    {
        PrintRollbackTxn *temp = &(Partxns->Ptxns[Partxns->index]);
        rec = Partxns->Ptxns[Partxns->index].txn;
        nodename = Partxns->Ptxns[Partxns->index].nodename;
        query = Partxns->Ptxns[Partxns->index].cancel_query;

        while (temp->index < temp->txn_count)
        {
            MemSet(values, 0, sizeof(values));
            MemSet(nulls, 0, sizeof(nulls));

            if (temp->index == 0)
            {
                values[0] = Int32GetDatum(Partxns->index);
            }
            values[1] = Int32GetDatum(temp->index);
            values[2] = PointerGetDatum(cstring_to_text(rec[temp->index]));
            values[3] = PointerGetDatum(cstring_to_text(nodename[temp->index]));
            values[4] = PointerGetDatum(cstring_to_text(query[temp->index]));
            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
            temp->index++;
            if (temp->index < temp->txn_count)
            {
                SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
            }
        }
        Partxns->index++;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

static bool
transaction_is_atxact(GddCtx *ctx, int txid)
{
	transaction	*tx = &pgxc_transaction[txid];
	int	i;

	for (i = 0; i < tx->pid_count; i++)
	{
		if ((int) tx->pid[i] < 0)
			return true;
	}

	return false;
}
