/*-------------------------------------------------------------------------
 *
 * poolmgr.c
 *
 *	  Connection pool manager handles connections to Datanodes
 *
 * The pooler runs as a separate process and is forked off from a
 * Coordinator postmaster. If the Coordinator needs a connection from a
 * Datanode, it asks for one from the pooler, which maintains separate
 * pools for each Datanode. A group of connections can be requested in
 * a single request, and the pooler returns a list of file descriptors
 * to use for the connections.
 *
 * Note the current implementation does not yet shrink the pool over time
 * as connections are idle.  Also, it does not queue requests; if a
 * connection is unavailable, it will simply fail. This should be implemented
 * one day, although there is a chance for deadlocks. For now, limiting
 * connections should be done between the application and Coordinator.
 * Still, this is useful to avoid having to re-establish connections to the
 * Datanodes all the time for multiple Coordinator backend sessions.
 *
 * The term "agent" here refers to a session manager, one for each backend
 * Coordinator connection to the pooler. It will contain a list of connections
 * allocated to a session, at most one per Datanode.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"
#include "postgres.h"
#include <poll.h>
#include <sys/epoll.h>
#include <signal.h>
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/atxact.h"
#include "access/xact.h"
#include "access/hash.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "pgxc/squeue.h"
#include "libpq/libpq-be.h"
#include "../interfaces/libpq/libpq-fe.h"
#include "../interfaces/libpq/libpq-int.h"
#include "postmaster/postmaster.h"		/* For Unix_socket_directories */
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "utils/varlena.h"
#include "port.h"
#include <math.h>
#ifdef __OPENTENBASE__
#include "access/xlog.h"
#endif

/* the mini use conut of a connection */
#define  MINI_USE_COUNT    10

/* Configuration options */
int			MaxPoolSize  = 300;
int			MinFreeSize  = 50;
int			MinFreeSizePerDb  = 5;

bool	    PoolerStatelessReuse   = true;
int			PoolerPort             = 6667;
int			PoolConnKeepAlive      = 600;
int			PoolMaintenanceTimeout = 30;
int			PoolCancelQueryTimeout = 60;
int			PoolSizeCheckGap       = 120;  /* max check memory size gap, in seconds */
int			PoolConnMaxLifetime    = 0;    /* max lifetime of a pooled connection, in seconds */
int			PoolMaxMemoryLimit     = 10;
int    		PoolConnectTimeOut     = 10;
int    		PoolScaleFactor        = 2;
int 		PoolDNSetTimeout       = 10;
int 	    PoolPrintStatTimeout   = -1;

char        *g_unpooled_database     = "template1";
char        *g_unpooled_user         = "mls_admin";
char        *PoolConnectDB           = "postgres";


bool         PoolConnectDebugPrint  = false; /* Pooler connect debug print */
bool		 PoolerStuckExit 		= true;  /* Pooler exit when stucked */
bool         PoolSubThreadLogPrint  = true;  /* Pooler sub thread log print */
bool         PoolSizeCheck          = false; /* enable connection pool size check */

#define      POOL_ASYN_WARM_PIPE_LEN      32   /* length of asyn warm pipe */
#define      POOL_SYN_CONNECTION_NUM      512   /* original was 128. To avoid can't get pooled connection */

PGPipe      *g_AsynUtilityPipeSender = NULL; /* filled with connections */
PGPipe      *g_AsynUtilityPipeRcver  = NULL; /* used for return warmed connections */
ThreadSema   g_AsnyUtilitysem;				  /* used for async warm thread */

#define      IS_ASYNC_PIPE_FULL()   (PipeIsFull(g_AsynUtilityPipeSender))
#define      MAX_FREE_CONNECTION_NUM 100


#define      POOL_SYN_REQ_CONNECTION_NUM   32
#define      PARAMS_LEN 4096

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t shutdown_requested = false;
static volatile sig_atomic_t got_SIGHUP = false;

/* used to track connection slot info */
static int32    g_Slot_Seqnum 			   = 0;
static int32    g_Connection_Acquire_Count = 0;
static int      g_epollfd;

typedef struct PoolerStatistics
{
	int32 client_request_conn_total;    		/* total acquire connection num by client */
	int32 client_request_from_hashtab;			/* client get all conn from hashtab */
	int32 client_request_from_thread;			/* client get at least part of conn from thread */
	int32 acquire_conn_from_hashtab;    /* immediate get conn from hashtab */
	int32 acquire_conn_from_hashtab_and_set_reuse; /* get conn from hashtab, but need to set for reuse by sync thread */
	int32 acquire_conn_from_thread;	    /* can't get conn from hashtab, need to conn by sync thread */
	unsigned long acquire_conn_time;	/* time cost for all conn process by sync thread */
}PoolerStatistics;

PoolerStatistics g_pooler_stat;


/* Flag to tell if we are Postgres-XC pooler process */
static bool am_pgxc_pooler = false;

typedef enum
{
	COMMAND_JUDGE_CONNECTION_MEMSIZE 		 = 0,
	COMMAND_CONNECTION_NEED_CLOSE          	 = 1, /* we should close the connection */
	COMMAND_CONNECTION_BUILD     			 = 2, /* async connection build */
	COMMAND_CONNECTION_CLOSE     			 = 3, /* async connection close */
	COMMAND_PING_NODE						 = 4, /* ping node */
	COMMAND_BUYYT
}PoolAsyncCmd;

/* used for async warm a connection */
typedef struct
{
	int32              cmd;  	  /* PoolAsyncCmd */
	int32              nodeindex;
	Oid 			   node;
	DatabasePool      *dbPool;
	PGXCNodePoolSlot  *slot;
	int32              size; 	  /* session memory context size */
	NameData		   nodename;   /* used by async ping node */
	NameData		   nodehost;   /* used by async ping node */
	int				   nodeport;   /* used by async ping node */
	int 			   nodestatus; /* used by async ping node, 0 means ok */
}PGXCAsyncCmdInfo;

/* Concurrently connection build info */
typedef struct
{
	int32             cmd;  	  /* PoolAsyncCmd */
	bool              bCoord;
	DatabasePool     *dbPool;
	int32             nodeindex;
	PGXCUserPool 	 *userPool;
	Oid				  nodeoid;	  /* Node Oid related to this pool */

	int32             m_version;  /* version of node pool */
	int32   		  size;  	  /* total pool size */
	int32         	  validSize;  /* valid data element number */
	bool        	  failed;
	PGXCNodePoolSlot  slot[1];    /* var length array */
} PGXCPoolConnectReq;

typedef enum
{
	PoolAsyncStatus_idle = 0,
	PoolAsyncStatus_busy = 1,
	PoolAsyncStatus_butty
}PoolAsyncStatus;
#define      MAX_SYNC_NETWORK_PIPE_LEN      1024    /* length of SYNC network pipe */
#define 	 MAX_SYNC_NETWORK_THREAD        PoolScaleFactor

typedef struct
{
	PGPipe     **request;  		/* request pipe */
	PGPipe     **response; 		/* response pipe */
	ThreadSema *sem;       		/* response sem */

	int32      *nodeindex;		/* nodeindex we are processing */
	int32      *status;    		/* worker thread status, busy or not */
	pg_time_t  *start_stamp;   	/* job start stamp */

	int32	   *remote_backend_pid;		/* dn's backend pid */
	Oid		   *remote_nodeoid;			/* dn's node oid */
	char	   **remote_ip; /* dn's ip */
	int32	   *remote_port;			/* dn's port */
	char	   **message;				/* you can put some note to print */
	int	   		*cmdtype;				/* cmdtype current processing */
}PGXCPoolSyncNetWorkControl;

typedef struct
{
	int32   threadIndex;
}PGXCPoolConnThreadParam;

static PGXCPoolSyncNetWorkControl g_PoolConnControl;
static PGXCPoolSyncNetWorkControl g_PoolSyncNetworkControl;

/* The root memory context */
static MemoryContext PoolerMemoryContext = NULL;
/*
 * Allocations of core objects: Datanode connections, upper level structures,
 * connection strings, etc.
 */
static MemoryContext PoolerCoreContext = NULL;
/*
 * Memory to store Agents
 */
static MemoryContext PoolerAgentContext = NULL;

/* Pool to all the databases (linked list) */
static DatabasePool *databasePoolsHead = NULL;
static DatabasePool *databasePoolsTail = NULL;

/* PoolAgents */
#define INIT_VERSION    0
typedef struct
{
	uint32  m_nobject;
    uint64  *m_fsm;
    uint32  m_fsmlen;
	uint32  m_version;
}BitmapMgr;

static int				agentCount   	  = 0;
static int              poolAgentSize     = 0;
static uint32           mgrVersion		  = INIT_VERSION;
static int32            *agentIndexes     = NULL;
static int              usedAgentSize     = 0;

static BitmapMgr 	   *poolAgentMgr      = NULL;
static PoolAgent 	   **poolAgents  	  = NULL;

static PoolHandle 	   *poolHandle 		  = NULL;

typedef struct PGXCMapNode
{
	Oid			nodeoid;	/* Node Oid */
	int32       nodeidx;    /* Node index*/
	char		node_type;
	char		node_name[NAMEDATALEN];
} PGXCMapNode;

static void  create_node_map(void);
static void  refresh_node_map(void);
static int32 get_node_index_by_nodeoid(Oid node);
static char* get_node_name_by_nodeoid(Oid node);
static bool  connection_need_pool(char *filter, const char *name);

typedef enum
{
	PoolNodeTypeCoord 	 = 0,
	PoolNodeTypeDatanode = 1,
	PoolNodeTypeButty
}PoolNodeTypeEnum;

/* status used to control parallel managment */
typedef enum
{
	PoolTaskStatus_success 	 	 = 0,
	PoolTaskStatus_error  	     = 1,
}PoolTaskStatusEnum;

typedef struct ConnStrKey
{
	Oid nodeoid;
	DatabasePool *dbpool;
} ConnStrKey;

typedef struct ConnStrEntry
{
	ConnStrKey key;
	char	connstr[1024];		/* connstr max len is 1024 */
} ConnStrEntry;


#define  OPENTENBASE_NODE_TYPE_COUNT  PoolNodeTypeButty /* coordinator, datanode */

static void pooler_subthread_write_log(int elevel, int lineno, const char *filename, const char *funcname, const char *fmt, ...)__attribute__((format(printf, 5, 6)));

/* Use this macro when a sub thread needs to print logs */
#define pooler_thread_logger(elevel, ...) \
    do { \
        pooler_subthread_write_log(elevel, __LINE__, __FILE__, PG_FUNCNAME_MACRO, __VA_ARGS__); \
    } while(0)

#define FORMATTED_TS_LEN                (128)                                          /* format timestamp buf length */
#define POOLER_WRITE_LOG_ONCE_LIMIT     (5)                                            /* number of logs written at a time */
#define MAX_THREAD_LOG_PIPE_LEN         (2 * 1024)                                     /* length of thread log pipe */
#define DEFAULT_LOG_BUF_LEN             (1024)                                         /* length of thread log length */
PGPipe  *g_ThreadLogQueue = NULL;
#ifdef __OPENTENBASE__
bool g_allow_distri_query_on_standby_node = false;
#endif
HTAB *g_NodeSingletonInfo = NULL;

static inline void RebuildAgentIndex(void);

static inline PGXCPoolAsyncBatchTask *create_async_batch_request(int32 cmd, PoolAgent *agent, int32 max_batch_size);
static inline void init_connection_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask, int32 reqseq,
							                     List *datanodelist, List *coordlist, int32 *fd_result, int32 *pid_result);
static inline void init_cancel_query_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask, int signal);
static inline void destroy_async_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask);
static inline bool dispatch_connection_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask,
													 PGXCNodePoolSlot  *slot,
													 PGXCUserPool      *userpool,
													 int                node_type,
													 int32              nodeindex,
													 bool               dispatched);
static inline bool dispatch_batch_request(PGXCPoolAsyncBatchTask   *asyncBatchReqTask,
										  PGXCNodePoolSlot         *slot,
										  int                       node_type,
										  int32                     nodeindex,
										  bool                      dispatched);

static inline bool  pooler_is_async_task_done(void);
static inline bool  pooler_wait_for_async_task_done(void);
static inline void  pooler_async_task_start(PGXCPoolSyncNetWorkControl *control, int32 thread, int32 nodeindex, PGXCNodePoolSlot *slot, Oid nodeoid, int32 cmdtype);
static inline void 	pooler_async_task_done(PGXCPoolSyncNetWorkControl *control, int32 thread);
static inline int32 pooler_async_task_pick_thread(PGXCPoolSyncNetWorkControl *control, int32 nodeindex);


static HTAB	 *g_nodemap = NULL; /* used to map nodeOid to nodeindex */

static int	is_pool_locked = false;
static int	server_fd = -1;

static int	node_info_check(PoolAgent *agent);
static void agent_init(PoolAgent *agent, const char *database, const char *user_name,
	                   const char *pgoptions);
static void agent_destroy(PoolAgent *agent);
static PoolAgent *agent_create(int new_fd);
static void agent_handle_input(PoolAgent *agent, StringInfo s);

static DatabasePool *create_database_pool(const char *database, const char *user_name, const char *pgoptions, int user_id);
static void insert_database_pool(DatabasePool *pool);
static PGXCNodePoolSlot *get_connection_from_used_pool(PoolAgent *agent, int nodeid, int pid);

static void reload_database_pools(PoolAgent *agent, bool check_pool_size);
static void cancel_old_connection(void);

static DatabasePool *find_database_pool(const char *database, const char *user_name, const char *pgoptions);
static void move_node_connection_into_used_pool(PoolAgent *agent, int node);
static int agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist, int32 *num, int **fd_result, int **pid_result);
static int cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *pidlist, List *coordlist, int signal);
static PGXCNodePoolSlot *acquire_connection(PoolAgent *agent, DatabasePool *dbPool, PGXCUserPool **userpool, int32 nodeidx, Oid node, bool bCoord);
static void agent_release_all_connections(PoolAgent *agent, bool force_destroy);
static void agent_release_connections(PoolAgent *agent, List *datanodelist, List *dn_pidlist, List *coordlist, int force_destroy);
static void change_slot_user_pool(PoolAgent *agent, PGXCUserPool *newUserPool, PGXCNodePoolSlot *slot);
static void pre_processing_slot_in_sync_response_queue(PoolAgent *agent, PGXCNodePoolSlot *slot, int node, bool release);
static void release_connection(PoolAgent *agent, DatabasePool *dbPool, PGXCNodePoolSlot *slot,
							   int32 nodeidx, Oid node, bool force_destroy, bool bCoord);
static void destroy_slot_ex(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot, char *file, int32 line);
#define  destroy_slot(nodeidx, node, slot) destroy_slot_ex(nodeidx, node, slot, __FILE__, __LINE__)

static void close_slot(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot);

static PGXCNodePool *grow_pool(DatabasePool *dbPool, int32 nodeidx, Oid node, bool bCoord);
static void destroy_node_pool(PGXCNodePool *node_pool);
static bool destroy_node_pool_free_slots(PGXCNodePool *node_pool, DatabasePool *db_pool, const char *user_name, bool rebuild_connstr);

static void reset_pooler_statistics(void);
static void print_pooler_statistics(void);
static void record_task_message(PGXCPoolSyncNetWorkControl* control, int32 thread, char* message);
static void record_time(struct timeval start_time, struct timeval end_time);

static void PoolerLoop(void);
static int clean_connection(List *node_discard,
							const char *database,
							const char *user_name);
static int *abort_pids(int *count,
					   int pid,
					   const char *database,
					   const char *user_name);
static char *build_node_conn_str(Oid node, DatabasePool *dbPool, PGXCNodeSingletonInfo *nodeInfo);
static char *build_node_conn_str_by_user(PGXCNodeSingletonInfo *nodeInfo, DatabasePool *dbPool, char *userName, char *pgoptions, char *out);

/* Signal handlers */
static void pooler_die(SIGNAL_ARGS);
static void pooler_quickdie(SIGNAL_ARGS);
static void pools_maintenance(void);
static bool shrink_pool(DatabasePool *pool);
static void pooler_sync_connections_to_nodepool(void);
static void pooler_handle_sync_response_queue(void);
static void pooler_async_query_connection(DatabasePool *pool, PGXCNodePoolSlot *slot, int32 nodeidx, Oid node);

static void  *pooler_async_utility_thread(void *arg);
static void  *pooler_async_connection_management_thread(void *arg);
static void  *pooler_sync_remote_operator_thread(void *arg);

static bool   pooler_async_build_connection(DatabasePool *pool, PGXCNodePool *nodePool, int32 nodeidx, Oid node,
											int32 size, bool bCoord);
static BitmapMgr *BmpMgrCreate(uint32 objnum);
static int        BmpMgrAlloc(BitmapMgr *mgr);
static void 	  BmpMgrFree(BitmapMgr *mgr, int index);
static uint32 	  BmpMgrGetVersion(BitmapMgr *mgr);
static int 		  BmpMgrGetUsed(BitmapMgr *mgr, int32 *indexes, int32 indexlen);
static void       pooler_sig_hup_handler(SIGNAL_ARGS);
static bool       BmpMgrHasIndexAndClear(BitmapMgr *mgr, int index);
static inline  void  agent_increase_ref_count(PoolAgent *agent);
static inline  void  agent_decrease_ref_count(PoolAgent *agent);
static inline  bool  agent_can_destory(PoolAgent *agent);
static inline  void  agent_pend_destory(PoolAgent *agent);
static inline  void  agent_set_destory(PoolAgent *agent);
static inline  bool  agent_pending(PoolAgent *agent);
static inline  void  agent_handle_pending_agent(PoolAgent *agent);
static inline  void  pooler_init_sync_control(PGXCPoolSyncNetWorkControl *control);
static inline  int32 pooler_get_slot_seq_num(void);
static inline  int32 pooler_get_connection_acquire_num(void);
static void  record_slot_info(PGXCPoolSyncNetWorkControl *control, int32 thread, PGXCNodePoolSlot *slot, Oid nodeoid);
static void TryPingUnhealthyNode(Oid nodeoid);
static void handle_abort(PoolAgent * agent, StringInfo s);
static void handle_connect(PoolAgent * agent, StringInfo s);
static void handle_clean_connection(PoolAgent * agent, StringInfo s);
static void handle_get_connections(PoolAgent * agent, StringInfo s);
static void handle_query_cancel(PoolAgent * agent, StringInfo s);
static void handle_release_connections(PoolAgent * agent, StringInfo s);
static bool remove_all_agent_references(Oid nodeoid);
static int  refresh_database_pools(PoolAgent *agent);

static void pooler_async_ping_node(Oid node);
static bool match_databasepool(DatabasePool *databasePool, const char* user_name, const char* database);
static int handle_close_pooled_connections(PoolAgent * agent, StringInfo s);
static void ConnectPoolManager(void);

#define isNodePoolCoord(nodePool) ((nodePool)->nodeInfo->coord)
#define isUserPoolCoord(userPool) (isNodePoolCoord((userPool)->nodePool))
#define nodePoolNodeName(nodePool) ((nodePool)->nodeInfo->node_name)
#define userPoolNodeName(userPool) (nodePoolNodeName((userPool)->nodePool))
#define nodePoolTotalSize(nodePool) ((nodePool)->nodeInfo->size)
#define nodePoolTotalFreeSize(nodePool) ((nodePool)->nodeInfo->freeSize)
#define nodePoolNodeHost(nodePool) ((nodePool)->nodeInfo->node_host)
#define nodePoolNodePort(nodePool) ((nodePool)->nodeInfo->node_port)
#define isSlotNeedPreProcessing(slot) ((slot != NULL) && (slot->need_move_to_usedpool || slot->set4reuse))

#define IncreaseSlotRefCount(slot,filename,linenumber)\
do\
{\
	if (slot)\
	{\
		(slot->refcount)++;\
		if (slot->refcount < 1)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid slot reference count:%d", filename, linenumber, slot->refcount);\
		}\
		slot->file	 =	filename;\
		slot->lineno =  linenumber;\
	}\
}while(0)

#define DecreaseSlotRefCount(slot,filename,linenumber)\
do\
{\
	if (slot)\
	{\
		(slot->refcount)--;\
		if (slot->refcount < 0)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid slot reference count:%d", filename, linenumber, slot->refcount);\
		}\
		slot->file	 = filename;\
		slot->lineno = linenumber;\
	}\
}while(0)

#define IncreaseUserPoolRefCount(userPool, filename, linenumber)\
do\
{\
	if (userPool)\
	{\
		(userPool->refcount)++;\
		if (userPool->refcount < 1)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid userPool reference count:%d", filename, linenumber, userPool->refcount);\
		}\
	}\
}while(0)

#define DecreaseUserPoolRefCount(userPool, filename, linenumber)\
do\
{\
	if (userPool)\
	{\
		(userPool->refcount)--;\
		if (userPool->refcount < 0)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid userPool reference count:%d", filename, linenumber, userPool->refcount);\
		}\
	}\
}while(0)

#define DecreaseUserPoolerSizeAsync(userpool, seqnum, file,lineno) \
	do\
	{\
		(userpool->size)--;\
		(userpool->nodePool->size)--;\
		(userpool->nodePool->nodeInfo->size)--;\
		if (userpool->size < 0)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, node size: %d, freesize:%d, node:%u, seq_num:%d", file, lineno, userpool->size, userpool->freeSize, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid, seqnum);\
		}\
		if (PoolConnectDebugPrint)\
		{\
			elog(LOG, POOL_MGR_PREFIX"[%s:%d] decrease user pool %p size: %d, freesize:%d, node size: %d, freesize:%d, node:%u, seq_num:%d", file, lineno, userpool, userpool->size, userpool->freeSize, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid, seqnum);\
		}\
		if (userpool->freeSize > userpool->size && PoolConnectDebugPrint)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, node size: %d, freesize:%d, node:%u, seq_num:%d", file, lineno, userpool->size, userpool->freeSize, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid, seqnum);\
		}\
	}while(0)

#define DecreaseUserPoolerSize(userpool,file,lineno) \
do\
{\
	(userpool->size)--;\
	(userpool->nodePool->size)--;\
	(userpool->nodePool->nodeInfo->size)--;\
	if (userpool->size < 0)\
	{\
		elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
	if (PoolConnectDebugPrint)\
	{\
		elog(LOG, POOL_MGR_PREFIX"[%s:%d] decrease user pool %p size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
	if (userpool->freeSize > userpool->size && PoolConnectDebugPrint)\
	{\
		elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
}while(0)

#define DecreaseUserPoolerFreesize(userpool,file,lineno) \
do\
{\
	(userpool->freeSize)--;\
	(userpool->nodePool->freeSize)--;\
	(userpool->nodePool->nodeInfo->freeSize)--;\
	if (userpool->freeSize < 0)\
	{\
		elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
	if (PoolConnectDebugPrint)\
	{\
		elog(LOG, POOL_MGR_PREFIX"[%s:%d] decrease freesize user pool %p size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
	if (userpool->freeSize > userpool->size && PoolConnectDebugPrint)\
	{\
		elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
}while(0)

#define IncreaseUserPoolerSize(userpool,file,lineno) \
	do\
	{\
		(userpool->size)++;\
		(userpool->nodePool->size)++;\
		(userpool->nodePool->nodeInfo->size)++;\
		if (PoolConnectDebugPrint)\
		{\
			elog(LOG, POOL_MGR_PREFIX"[%s:%d] increase user pool %p size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
		}\
		if (userpool->freeSize > userpool->size && PoolConnectDebugPrint)\
		{\
			elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
		}\
	}while(0)

#define IncreaseUserPoolerFreesize(userpool,file,lineno) \
do\
{\
	(userpool->freeSize)++;\
	(userpool->nodePool->freeSize)++;\
	(userpool->nodePool->nodeInfo->freeSize)++;\
	if (PoolConnectDebugPrint)\
	{\
		elog(LOG, POOL_MGR_PREFIX"[%s:%d] increase freesize user pool %p size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
	if (userpool->freeSize > userpool->size && PoolConnectDebugPrint)\
	{\
		elog(PANIC, POOL_MGR_PREFIX"[%s:%d] invalid user pool size: %d, freesize:%d, user:%s, node size %d, node freesize:%d, node:%u", file, lineno, userpool->size, userpool->freeSize, userpool->user_name, userpool->nodePool->size, userpool->nodePool->freeSize, userpool->nodePool->nodeoid);\
	}\
}while(0)



void
PGXCPoolerProcessIam(void)
{
	am_pgxc_pooler = true;
}

bool
IsPGXCPoolerProcess(void)
{
    return am_pgxc_pooler;
}

/*
 * Initialize internal structures
 */
int
PoolManagerInit()
{
	elog(DEBUG1, POOL_MGR_PREFIX"Pooler process is started: %d", getpid());

	/*
	 * Set up memory contexts for the pooler objects
	 */
	PoolerMemoryContext = AllocSetContextCreate(TopMemoryContext,
												"PoolerMemoryContext",
												ALLOCSET_DEFAULT_SIZES);
	PoolerCoreContext = AllocSetContextCreate(PoolerMemoryContext,
											  "PoolerCoreContext",
											  ALLOCSET_DEFAULT_SIZES);
	PoolerAgentContext = AllocSetContextCreate(PoolerMemoryContext,
											   "PoolerAgentContext",
											   ALLOCSET_DEFAULT_SIZES);

	ForgetLockFiles();

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(pool manager probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(DEBUG1, POOL_MGR_PREFIX"setsid() failed: %m");
		//elog(FATAL, POOL_MGR_PREFIX"setsid() failed: %m");
#endif
	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGINT,  pooler_die);
	pqsignal(SIGTERM, pooler_die);
	pqsignal(SIGQUIT, pooler_quickdie);
	pqsignal(SIGHUP,  pooler_sig_hup_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/* TODO other signal handlers */

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/* Allocate pooler structures in the Pooler context */
	MemoryContextSwitchTo(PoolerMemoryContext);
	poolAgentSize = ALIGN_UP(MaxConnections, BITS_IN_LONGLONG);
	poolAgents = (PoolAgent **) palloc0(poolAgentSize * sizeof(PoolAgent *));
	if (poolAgents == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg(POOL_MGR_PREFIX"out of memory")));
	}

	poolAgentMgr = BmpMgrCreate(poolAgentSize);
	if (poolAgentMgr == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg(POOL_MGR_PREFIX"out of memory")));
	}

	agentIndexes = (int32*) palloc0(poolAgentSize * sizeof(int32));
	if (agentIndexes == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg(POOL_MGR_PREFIX"out of memory")));
	}

	if (MaxConnections > MaxPoolSize)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				    errmsg(POOL_MGR_PREFIX"max_pool_size can't be smaller than max_connections")));
	}
	PoolerLoop();
	return 0;
}


/*
 * Check connection info consistency with system catalogs
 */
static int
node_info_check(PoolAgent *agent)
{
	DatabasePool   *dbPool = databasePoolsHead;
	List 		   *checked = NIL;
	int 			res = POOL_CHECK_SUCCESS;
	Oid			   *coOids;
	Oid			   *dnOids;
	int				numCo;
	int				numDn;

	/*
	 * First check if agent's node information matches to current content of the
	 * shared memory table.
	 */
	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	if (agent->num_coord_connections != numCo ||
			agent->num_dn_connections != numDn ||
			memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
			memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid)))
	{
		res = POOL_CHECK_FAILED;
	}

	/* Release palloc'ed memory */
	pfree(coOids);
	pfree(dnOids);

	/*
	 * Iterate over all dbnode pools and check if connection strings
	 * are matching node definitions.
	 */
	while (res == POOL_CHECK_SUCCESS && dbPool)
	{
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, dbPool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char 		   *connstr_chk;

			/* No need to check same Datanode twice */
			if (list_member_oid(checked, nodePool->nodeoid))
				continue;
			checked = lappend_oid(checked, nodePool->nodeoid);

			connstr_chk = build_node_conn_str(nodePool->nodeoid, dbPool, NULL);
			if (connstr_chk == NULL)
			{
				/* Problem of constructing connection string */
				hash_seq_term(&hseq_status);
				res = POOL_CHECK_FAILED;
				break;
			}
			/* return error if there is difference */
			if (strcmp(connstr_chk, nodePool->connstr))
			{
				pfree(connstr_chk);
				hash_seq_term(&hseq_status);
				res = POOL_CHECK_FAILED;
				break;
			}

			pfree(connstr_chk);
		}
		dbPool = dbPool->next;
	}
	list_free(checked);
	return res;
}

/*
 * Destroy internal structures
 */
int
PoolManagerDestroy(void)
{
	int			status = 0;

	if (PoolerMemoryContext)
	{
		MemoryContextDelete(PoolerMemoryContext);
		PoolerMemoryContext = NULL;
	}

	return status;
}


/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 */
PoolHandle *
GetPoolManagerHandle(void)
{
	PoolHandle *handle;
	int			fdsock;
	struct timeval	timeout = {60, 0};

	/* Connect to the pooler */
	fdsock = pool_connect(PoolerPort, Unix_socket_directories);
	if (fdsock < 0)
	{
		int			saved_errno = errno;

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg(POOL_MGR_PREFIX"failed to connect to pool manager: %m")));
		errno = saved_errno;
		return NULL;
	}

	/* Allocate handle */
	/*
	 * XXX we may change malloc here to palloc but first ensure
	 * the CurrentMemoryContext is properly set.
	 * The handle allocated just before new session is forked off and
	 * inherited by the session process. It should remain valid for all
	 * the session lifetime.
	 */
	handle = (PoolHandle *) malloc(sizeof(PoolHandle));
	if (!handle)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg(POOL_MGR_PREFIX"out of memory")));
		return NULL;
	}

	/* set timeout for blocking recv()*/
	if (setsockopt(fdsock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
	{
		elog(LOG, "Set socket receive timeout option failed error code: %d", errno);	
		return NULL;
	}

	/* set timeout for blocking send()*/
	if (setsockopt(fdsock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0)
	{
		elog(LOG, "Set socket send timeout option failed error code: %d", errno);	
		return NULL;
	}

	handle->port.fdsock = fdsock;
	handle->port.RecvLength = 0;
	handle->port.RecvPointer = 0;
	handle->port.SendPointer = 0;

	return handle;
}


/*
 * Close handle
 */
void
PoolManagerCloseHandle(PoolHandle *handle)
{
	close(Socket(handle->port));
	free(handle);
}


/*
 * Create agent
 */
static PoolAgent *
agent_create(int new_fd)
{
	int32         agentindex = 0;
	MemoryContext oldcontext;
	PoolAgent  *agent;

	agentindex = BmpMgrAlloc(poolAgentMgr);
	if (-1 == agentindex)
	{
		ereport(PANIC,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				    errmsg(POOL_MGR_PREFIX"out of agent index")));
	}

	oldcontext = MemoryContextSwitchTo(PoolerAgentContext);

	/* Allocate agent */
	agent = (PoolAgent *) palloc0(sizeof(PoolAgent));
	if (!agent)
	{
		close(new_fd);
		BmpMgrFree(poolAgentMgr, agentindex);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg(POOL_MGR_PREFIX"out of memory")));
		return NULL;
	}

	agent->port.fdsock = new_fd;
	agent->port.RecvLength = 0;
	agent->port.RecvPointer = 0;
	agent->port.SendPointer = 0;
	agent->pool = NULL;
	agent->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"Agent",
										ALLOCSET_DEFAULT_SIZES);
	agent->num_dn_connections = 0;
	agent->num_coord_connections = 0;

	agent->dn_conn_oids = NULL;
	agent->coord_conn_oids = NULL;

	agent->dn_connections = NULL;
	agent->coord_connections = NULL;
	agent->dn_connections_used_pool = NULL;

	agent->pid = 0;
	agent->agentindex = agentindex;

	/* Append new agent to the list */
	poolAgents[agentindex] = agent;

	agentCount++;

	MemoryContextSwitchTo(oldcontext);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"agent_create end, agentCount:%d, fd:%d", agentCount, new_fd);
	}

	return agent;
}

/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char *session_options(void)
{
	int				 i;
	char			*pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle", "lc_monetary"};
	StringInfoData	 options;
	List			*value_list;
	ListCell		*l;

	initStringInfo(&options);

	for (i = 0; i < sizeof(pgoptions)/sizeof(char*); i++)
	{
		const char		*value;

		appendStringInfo(&options, " -c %s=", pgoptions[i]);

		value = GetConfigOptionResetString(pgoptions[i]);

		/* lc_monetary does not accept lower case values */
		if (strcmp(pgoptions[i], "lc_monetary") == 0)
		{
			appendStringInfoString(&options, value);
			continue;
		}

		if (SplitIdentifierString(strdup(value), ',', &value_list) < 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("parse session options failed.")));
        }

		foreach(l, value_list)
		{
			char *value = (char *) lfirst(l);
			appendStringInfoString(&options, value);
			if (lnext(l))
				appendStringInfoChar(&options, ',');
		}
	}

	return options.data;
}

/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void
PoolManagerConnect(PoolHandle *handle,
	               const char *database, const char *user_name,
	               char *pgoptions)
{
	int n32;
	char msgtype = 'c';
	int userid = GetAuthenticatedUserId();

	Assert(handle);
	Assert(database);
	Assert(user_name);

	/* Save the handle */
	poolHandle = handle;

	/* Message type */
	pool_putbytes(&handle->port, &msgtype, 1);

	/* Message length */
	n32 = pg_hton32(strlen(database) + strlen(user_name) + strlen(pgoptions) + 23 + sizeof(userid));
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* PID number */
	n32 = pg_hton32(MyProcPid);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	n32 = pg_hton32(userid);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = pg_hton32(strlen(database) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send database name followed by \0 terminator */
	pool_putbytes(&handle->port, database, strlen(database) + 1);
	pool_flush(&handle->port);

	/* Length of user name string */
	n32 = pg_hton32(strlen(user_name) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send user name followed by \0 terminator */
	pool_putbytes(&handle->port, user_name, strlen(user_name) + 1);
	pool_flush(&handle->port);

	/* Length of pgoptions string */
	n32 = pg_hton32(strlen(pgoptions) + 1);
	pool_putbytes(&handle->port, (char *) &n32, 4);

	/* Send pgoptions followed by \0 terminator */
	pool_putbytes(&handle->port, pgoptions, strlen(pgoptions) + 1);
	pool_flush(&handle->port);

}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void
PoolManagerReconnect(void)
{
	HOLD_POOLER_RELOAD();

	if (poolHandle)
	{
		PoolManagerDisconnect();
	}
	
	ConnectPoolManager();

	RESUME_POOLER_RELOAD();
}

/*
 * Lock/unlock pool manager
 * During locking, the only operations not permitted are abort, connection and
 * connection obtention.
 */
void
PoolManagerLock(bool is_lock)
{
	char msgtype = 'o';
	int n32;
	int msglen = 8;

	HOLD_POOLER_RELOAD();

	if (poolHandle == NULL)
	{
		ConnectPoolManager();
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = pg_hton32(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = pg_hton32((int) is_lock);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);

	RESUME_POOLER_RELOAD();
}

/*
 * Create a hash table of used dn connections
 */
static void
create_dn_connections_used_pool(PoolAgent *agent)
{
	HASHCTL			hinfo;
	int				hflags;

	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(DnConnectionKey);
	hinfo.entrysize = sizeof(DnConnection);
	hflags |= HASH_ELEM;

	hinfo.hcxt = agent->mcxt;
	hflags |= HASH_CONTEXT;

	hinfo.hash = tag_hash;
	hflags |= HASH_FUNCTION;

	agent->dn_connections_used_pool = hash_create("dn connections pool",
												  64,
												  &hinfo, hflags);
	return;
}

/*
 * Init PoolAgent
 */
static void
agent_init(PoolAgent *agent, const char *database, const char *user_name,
           const char *pgoptions)
{
	MemoryContext oldcontext;
	char sql[PARAMS_LEN] = {0};

	Assert(agent);
	Assert(database);
	Assert(user_name);

	/* disconnect if we are still connected */
	if (agent->pool)
	{
		agent_release_all_connections(agent, false);
	}

	oldcontext = MemoryContextSwitchTo(agent->mcxt);

	/* Get needed info and allocate memory */
	PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
					&agent->num_coord_connections, &agent->num_dn_connections, false);

	agent->coord_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
	agent->dn_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));

	agent->user_name = pstrdup(user_name);
	agent->pgoptions = pstrdup(pgoptions);

	/*
	 * To reduce the size of transmission packets, only username
	 * and pgoptions are passed here.
	 * "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;" reserved in set_backend_to_resue
	 */
	snprintf(sql, sizeof(sql), "%s@%s", agent->user_name, pgoptions);

	agent->set_conn_reuse_sql = pstrdup(sql);

	/* Init dn connections hashtable */
	create_dn_connections_used_pool(agent);

	/* find database */
	agent->pool = find_database_pool(database, user_name, pgoptions);

	/* create if not found */
	if (agent->pool == NULL)
	{
		agent->pool = create_database_pool(database, user_name, pgoptions, agent->user_id);
	}

	MemoryContextSwitchTo(oldcontext);

	agent->query_count     = 0;
	agent->destory_pending = false;
	agent->breconnecting   = false;
	agent->locked_pooler = false;
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"agent_init pid:%d, fd:%d", agent->pid, Socket(agent->port));
	}
}

static void
destroy_pend_agent(PoolAgent *agent)
{
	int fd = Socket(agent->port);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"destroy_pend_agent enter");
	}

	if (fd >= 0)
	{
		close(fd);
		Socket(agent->port) = -1;
	}

	/* Discard connections if any remaining */
	if (agent->pool)
	{
		agent_release_all_connections(agent, true);
	}

	if (agent->locked_pooler)
	{
		is_pool_locked = false;
		elog(LOG, POOL_MGR_PREFIX"destroy_pend_agent destroy agent with unlock pool, backend pid:%d", agent->pid);
	}

	MemoryContextDelete(agent->mcxt);
	agent_set_destory(agent);
	pfree(agent);
}


/*
 * Destroy PoolAgent
 */
static void
agent_destroy(PoolAgent *agent)
{
	int32  agentindex;
	int32  fd;
	Assert(agent);

	agentindex = agent->agentindex;
	fd         = Socket(agent->port);

	if (epoll_ctl(g_epollfd, EPOLL_CTL_DEL, fd, NULL) == -1)
	{
		close(g_epollfd);
		elog(ERROR, POOL_MGR_PREFIX"Pooler:remove fd %d from epoll failed, %m", fd);
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"agent_destroy close fd:%d, pid:%d, num_dn_connections:%d, num_coord_connections:%d",
			fd, agent->pid, agent->num_dn_connections, agent->num_coord_connections);
	}

	/* check whether we can destory the agent */
	if (agent_can_destory(agent))
	{
		destroy_pend_agent(agent);
	}
	else
	{
		agent_pend_destory(agent);
	}

    /* Destory shadow */
    if (BmpMgrHasIndexAndClear(poolAgentMgr, agentindex))
    {
        --agentCount;
    }

	poolAgents[agentindex] = NULL;

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"agent_destroy end, agentCount:%d", agentCount);
	}
}

/*
 * release the pending agent's connections
 */
static void
release_pend_agent_connections(PoolAgent *agent)
{
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"release_pend_agent_connections enter, release_pending_type %d", agent->release_pending_type);
	}

	if (agent_can_destory(agent))
	{
		/* Discard connections if any remaining */
		if (agent->pool)
		{
			if (agent->release_pending_type == RELEASE_PARTITAL)
			{
				ReleaseContext *ctx = agent->release_ctx;
				agent_release_connections(agent, ctx->datanodelist, ctx->dn_pidlist, ctx->coordlist, ctx->force);
				list_free(ctx->datanodelist);
				list_free(ctx->dn_pidlist);
				list_free(ctx->coordlist);
				pfree(ctx);
				agent->release_ctx = NULL;
			}
			else
			{
				agent_release_all_connections(agent, (agent->release_pending_type == FORCE_RELEASE_ALL));
			}
		}

		agent->release_pending_type = RELEASE_NOTHING;
		return;
	}
}


/*
 * Release handle to pool manager
 */
void
PoolManagerDisconnect(void)
{
	HOLD_POOLER_RELOAD();
	if (poolHandle)
	{
		Assert(poolHandle);

		pool_putmessage(&poolHandle->port, 'd', NULL, 0);
		pool_flush(&poolHandle->port);

		PoolManagerCloseHandle(poolHandle);
		poolHandle = NULL;
	}

	RESUME_POOLER_RELOAD();
}


/*
 * Get pooled connections
 */
int *
PoolManagerGetConnections(List *datanodelist, List *coordlist, int **pids, StringInfo *error_msg)
{
	int			i = 0;
	ListCell   *nodelist_item;
	int		   *fds;
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + OPENTENBASE_NODE_TYPE_COUNT + 1]; /* totlen + datanode_count + coord_count + seq */
	int 		pool_recvpids_num = 0;
	int 		pool_recvfds_ret;
	bool        need_receive_errmsg = false;
	int 		j = 0;
	uint32		send_seq = (uint32)random() + 1;
#ifdef __OPENTENBASE__
	/*
	 * if it is the standby node of the main plane, the distributed query will be connected to
	 * the main data node, and the standby cn may generate the same global xid as the main cn,
	 * so disable the distributed query of the standby node on the main plane
	 */
	if (g_allow_distri_query_on_standby_node == false && IS_PGXC_MAINCLUSTER_SLAVENODE)
    {
        elog(ERROR, "can't do distributed query because it is the main plane standby node.");
    }
#endif

	if (!IS_PGXC_COORDINATOR)
		elog(ERROR, "getting remote connections on the datanode is not allowed");

	HOLD_POOLER_RELOAD();

	if (poolHandle == NULL)
	{
		ConnectPoolManager();
	}

	/*
	 * Prepare end send message to pool manager.
	 * First with Datanode list.
	 * This list can be NULL for a query that does not need
	 * Datanode Connections (Sequence DDLs)
	 */
	nodes[i++] = pg_hton32(send_seq);
	nodes[i++] = pg_hton32(list_length(datanodelist));
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = pg_hton32(lfirst_int(nodelist_item));
		}
	}

	/* Then with Coordinator list (can be nul) */
	nodes[i++] = pg_hton32(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = pg_hton32(lfirst_int(nodelist_item));
		}
	}

    /* Receive response */
	fds = (int *) palloc(sizeof(int) * totlen);
	if (fds == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg(POOL_MGR_PREFIX"out of memory")));
	}

	pool_putmessage(&poolHandle->port, 'g', (char *) nodes, sizeof(int) * (totlen + OPENTENBASE_NODE_TYPE_COUNT + 1));

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"backend required %d connections, cn_count:%d, dn_count:%d, pooler_fd %d, MyProcPid:%d, seq:%u",
								totlen, list_length(coordlist), list_length(datanodelist), poolHandle->port.fdsock, MyProcPid, send_seq);
	}

	pool_flush(&poolHandle->port);

	PG_TRY();
	{
		HOLD_CANCEL_INTERRUPTS();
		pool_recvfds_ret = pool_recvfds(&poolHandle->port, fds, totlen, &need_receive_errmsg, send_seq);
		RESUME_CANCEL_INTERRUPTS();
		if (pool_recvfds_ret)
		{
			pfree(fds);
			if (PoolConnectDebugPrint)
			{
				elog(LOG, "[PoolManagerGetConnections]pool_recvfds_ret=%d, failed to pool_recvfds. return NULL.", pool_recvfds_ret);
			}

			if (need_receive_errmsg)
			{
				*error_msg = makeStringInfo();
				pool_getmessage(&poolHandle->port, *error_msg, POOLER_ERROR_MSG_LEN + 4);
			}

			/* Disconnect off pooler. */
			PoolManagerDisconnect();
			handle_close_and_reset_all();
			fds = NULL;
		}
		else
		{
			*pids = NULL;
			HOLD_CANCEL_INTERRUPTS();
			pool_recvpids_num = pool_recvpids(&poolHandle->port, pids, send_seq);
			RESUME_CANCEL_INTERRUPTS();
			if (pool_recvpids_num != totlen)
			{
				if(*pids)
				{
					pfree(*pids);
					*pids = NULL;
				}
				/* Disconnect off pooler. */
				PoolManagerDisconnect();
				handle_close_and_reset_all();
				elog(LOG, "[PoolManagerGetConnections]pool_recvpids_num=%d, totlen=%d. failed to pool_recvpids. return NULL.", pool_recvpids_num, totlen);
				if (fds)
					pfree(fds);
				fds = NULL;
			}
		}
	}
	PG_CATCH();
	{
		PoolManagerDisconnect();
		handle_close_and_reset_all();
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (PoolConnectDebugPrint && (*pids != NULL))
	{
		for (j = 0; j < pool_recvpids_num; j++)
		{
			elog(LOG, "[PoolManagerGetConnections] PoolManagerGetConnections cnt:%d Proc:%d get conns pid:%d, fd:%d, seq:%u", j + 1, MyProcPid, (*pids)[j], fds[j], send_seq);
		}
	}

	RESUME_POOLER_RELOAD();
	return fds;
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int
PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids)
{
	int		num_proc_ids = 0;
	int		n32, msglen;
	char		msgtype = 'a';
	int		dblen = dbname ? strlen(dbname) + 1 : 0;
	int		userlen = username ? strlen(username) + 1 : 0;

	if (poolHandle == NULL)
	{
		ConnectPoolManager();
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = dblen + userlen + 12;
	n32 = pg_hton32(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = pg_hton32(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = pg_hton32(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Then Get back Pids from Pooler */
	num_proc_ids = pool_recvpids(&poolHandle->port, proc_pids, 0);

	return num_proc_ids;
}


/*
 * Clean up Pooled connections
 */
void
PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username)
{
	int			totlen = list_length(datanodelist) + list_length(coordlist);
	int			nodes[totlen + 2];
	ListCell		*nodelist_item;
	int			i, n32, msglen;
	char			msgtype = 'f';
	int			userlen = username ? strlen(username) + 1 : 0;
	int			dblen = dbname ? strlen(dbname) + 1 : 0;

	HOLD_POOLER_RELOAD();

	if (poolHandle == NULL)
	{
		ConnectPoolManager();
	}

	nodes[0] = pg_hton32(list_length(datanodelist));
	i = 1;
	if (list_length(datanodelist) != 0)
	{
		foreach(nodelist_item, datanodelist)
		{
			nodes[i++] = pg_hton32(lfirst_int(nodelist_item));
		}
	}
	/* Then with Coordinator list (can be nul) */
	nodes[i++] = pg_hton32(list_length(coordlist));
	if (list_length(coordlist) != 0)
	{
		foreach(nodelist_item, coordlist)
		{
			nodes[i++] = pg_hton32(lfirst_int(nodelist_item));
		}
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = sizeof(int) * (totlen + 2) + dblen + userlen + 12;
	n32 = pg_hton32(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send list of nodes */
	pool_putbytes(&poolHandle->port, (char *) nodes, sizeof(int) * (totlen + 2));

	/* Length of Database string */
	n32 = pg_hton32(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = pg_hton32(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	RESUME_POOLER_RELOAD();

	/* Receive result message */
	if (pool_recvres(&poolHandle->port, true) != CLEAN_CONNECTION_COMPLETED)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg(POOL_MGR_PREFIX"Clean connections not completed")));
	}
}


/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool
PoolManagerCheckConnectionInfo(void)
{
	int res;

	/*
	 * New connection may be established to clean connections to
	 * specified nodes and databases.
	 */
	if (poolHandle == NULL)
	{
		ConnectPoolManager();
	}

	PgxcNodeListAndCountWrapTransaction(false, false);
	pool_putmessage(&poolHandle->port, 'q', NULL, 0);
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port, true);

	if (res == POOL_CHECK_SUCCESS)
		return true;

	return false;
}


/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void
PoolManagerReloadConnectionInfo(void)
{
	Assert(poolHandle);
	PgxcNodeListAndCountWrapTransaction(false, true);
	pool_putmessage(&poolHandle->port, 'p', (const char *) &PoolSizeCheck, 1);
	pool_flush(&poolHandle->port);
}


/*
 * Handle messages to agent
 */
static void
agent_handle_input(PoolAgent * agent, StringInfo s)
{
	int			qtype;

	qtype = pool_getbyte(&agent->port);
	/*
	 * We can have multiple messages, so handle them all
	 */
	for (;;)
	{
		int			res;

		/*
		 * During a pool cleaning, Abort, Connect and Get Connections messages
		 * are not allowed on pooler side.
		 * It avoids to have new backends taking connections
		 * while remaining transactions are aborted during FORCE and then
		 * Pools are being shrinked.
		 */
		if (is_pool_locked && (qtype == 'a' || qtype == 'c' || qtype == 'g'))
		{
			elog(WARNING,POOL_MGR_PREFIX"Pool operation cannot run during pool lock");
		}

		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"get qtype=%c from backend pid:%d", qtype, agent->pid);
		}

		switch (qtype)
		{
			case 'a':			/* ABORT */
				handle_abort(agent, s);
				break;
			case 'c':			/* CONNECT */
				handle_connect(agent, s);
				break;
			case 'd':			/* DISCONNECT */
				pool_getmessage(&agent->port, s, 4);
				agent_destroy(agent);
				pq_getmsgend(s);
				/* agent destroyed, no need to loop, just return to avoid invalid agent read */
				return;
			case 'f':			/* CLEAN CONNECTION */
				handle_clean_connection(agent, s);
				break;
			case 'g':			/* GET CONNECTIONS */
				handle_get_connections(agent, s);
				break;

			case 'h':			/* Cancel SQL Command in progress on specified connections */
				handle_query_cancel(agent, s);
				break;
			case 'o':			/* Lock/unlock pooler */
				pool_getmessage(&agent->port, s, 8);
				is_pool_locked = pq_getmsgint(s, 4);
				agent->locked_pooler = is_pool_locked;
				pq_getmsgend(s);
				break;
			case 'p':			/* Reload connection info */
				{
					/*
					 * Connection information reloaded concerns all the database pools.
					 * A database pool is reloaded as follows for each remote node:
					 * - node pool is deleted if the node has been deleted from catalog.
					 *   Subsequently all its connections are dropped.
					 * - node pool is deleted if its port or host information is changed.
					 *   Subsequently all its connections are dropped.
					 * - node pool is kept unchanged with existing connection information
					 *   is not changed. However its index position in node pool is changed
					 *   according to the alphabetical order of the node name in new
					 *   cluster configuration.
					 * Backend sessions are responsible to reconnect to the pooler to update
					 * their agent with newest connection information.
					 * The session invocating connection information reload is reconnected
					 * and uploaded automatically after database pool reload.
					 * Other server sessions are signaled to reconnect to pooler and update
					 * their connection information separately.
					 * During reload process done internally on pooler, pooler is locked
					 * to forbid new connection requests.
					 */
					bool check_pool_size;
					pool_getmessage(&agent->port, s, 5);
					check_pool_size = pq_getmsgbyte(s);
					pq_getmsgend(s);

					/* First update all the pools */
					reload_database_pools(agent, check_pool_size);
				}
				break;

			case 'q':			/* Check connection info consistency */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);

				/* Check cached info consistency */
				res = node_info_check(agent);

				/* Send result */
				pool_sendres(&agent->port, res, NULL, 0, true);
				break;
			case 'r':			/* RELEASE ALL CONNECTIONS */
				{
					bool destroy;

					pool_getmessage(&agent->port, s, 8);
					destroy = (bool) pq_getmsgint(s, 4);
					pq_getmsgend(s);
					if (PoolConnectDebugPrint)
					{
						elog(LOG, POOL_MGR_PREFIX"receive command %c from agent:%d. destory=%d", qtype, agent->pid, destroy);
					}
					agent_release_all_connections(agent, destroy);
				}
				break;
			case 'u':			/* RELEASE CONNECTIONS */
				{
					if (PoolConnectDebugPrint)
					{
						elog(LOG, POOL_MGR_PREFIX"receive command %c from agent:%d.", qtype, agent->pid);
					}
					handle_release_connections(agent, s);
				}
				break;
			case 'R':			/* Refresh connection info */
				pool_getmessage(&agent->port, s, 4);
				pq_getmsgend(s);
				res = refresh_database_pools(agent);
				pool_sendres(&agent->port, res, NULL, 0, true);
				break;

			case 't':
				res = handle_close_pooled_connections(agent ,s);
				/* Send result */
				pool_sendres(&agent->port, res, NULL, 0, true);
				break;

			case EOF:			/* EOF */
				agent_destroy(agent);
				return;
			default:			/* EOF or protocol violation */
				elog(WARNING, POOL_MGR_PREFIX"invalid request tag:%c", qtype);
				agent_destroy(agent);
				return;
		}

		/* avoid reading from connection */
		if ((qtype = pool_pollbyte(&agent->port)) == EOF)
			break;
	}
}

/*
 * acquire connection
 * return -1: error happen
 * return 0 : when fd_result and pid_result set to NULL, async acquire connection will be done in parallel threads
 * return 0 : when fd_result and pid_result is not NULL, acquire connection is done(acquire from freeslot in pool).
 */
static int
agent_acquire_connections(PoolAgent *agent, List *datanodelist, List *coordlist, int32 *num, int **fd_result, int **pid_result)
{
	int32			  i           = 0;
	int32             acquire_seq = 0;
	int			      node        = 0;
	int32             acquire_succeed_num = 0;
	int32             acquire_failed_num  = 0;
	int32             set_request_num     = 0;
	int32			  connect_num  = 0;
	int32			  node_num	   = 0;
	bool              succeed      = false;
	PGXCUserPool     *userPool;
	PGXCNodePoolSlot *slot         = NULL;
	ListCell         *nodelist_item = NULL;
	MemoryContext     oldcontext    = NULL;
	PGXCPoolAsyncBatchTask *asyncBatchReqTask = NULL;
	int 	                max_batch_size    = 0;

	Assert(agent);

	acquire_seq = pooler_get_connection_acquire_num();
	if (PoolPrintStatTimeout > 0)
	{
		g_pooler_stat.client_request_conn_total++;
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]agent pid=%d, NumCoords=%d, NumDataNodes=%d, "
								 "num_coord_connections=%d, num_dn_connections=%d, "
								 "datanodelist len=%d, coordlist len=%d",
								agent->pid, NumCoords, NumDataNodes,
								agent->num_coord_connections, agent->num_dn_connections,
								list_length(datanodelist), list_length(coordlist));
	}

	/* we have scaled out the nodes, fresh node info */
	if (agent->num_coord_connections < NumCoords || agent->num_dn_connections < NumDataNodes)
	{
		int32  orig_cn_number = agent->num_coord_connections;
		int32  orig_dn_number = agent->num_dn_connections;
		PGXCNodePoolSlot **orig_cn_connections = agent->coord_connections;
		PGXCNodePoolSlot **orig_dn_connections = agent->dn_connections;

		elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]Pooler found node number extension pid:%d, acquire_seq:%d, refresh node info now", agent->pid, acquire_seq);
		pfree((void*)(agent->dn_conn_oids));
		agent->dn_conn_oids = NULL;
		pfree((void*)(agent->coord_conn_oids));
		agent->coord_conn_oids = NULL;

		/* fix memleak */
		oldcontext = MemoryContextSwitchTo(agent->mcxt);

		PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
						&agent->num_coord_connections, &agent->num_dn_connections, false);

		agent->coord_connections = (PGXCNodePoolSlot **)
				palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
		agent->dn_connections = (PGXCNodePoolSlot **)
				palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));

		/* fix memleak */
		MemoryContextSwitchTo(oldcontext);

		/* index of newly added nodes must be biggger, so memory copy can hanle node extension */
		memcpy(agent->coord_connections, orig_cn_connections, sizeof(sizeof(PGXCNodePoolSlot *)) * orig_cn_number);
		memcpy(agent->dn_connections, orig_dn_connections, sizeof(sizeof(PGXCNodePoolSlot *)) * orig_dn_number);
		pfree(orig_cn_connections);
		pfree(orig_dn_connections);
	}

	/* Check if pooler can accept those requests */
	if (list_length(datanodelist) > agent->num_dn_connections    ||
		list_length(coordlist) > agent->num_coord_connections)
	{
		elog(LOG, "[agent_acquire_connections]agent_acquire_connections called with invalid arguments -"
				  "list_length(datanodelist) %d, num_dn_connections %d, "
				  "list_length(coordlist) %d, num_coord_connections %d, ",
			 list_length(datanodelist), agent->num_dn_connections,
			 list_length(coordlist), agent->num_coord_connections);

		return -1;
	}

	if (agent->release_pending_type != RELEASE_NOTHING)
	{
		elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d last request not finish yet, has pending release handles, type %d", agent->pid, agent->release_pending_type);
		goto FATAL_ERROR;
	}

	node_num =	list_length(datanodelist) + list_length(coordlist);
	/*
	 * Allocate memory
	 * File descriptors of Datanodes and Coordinators are saved in the same array,
	 * This array will be sent back to the postmaster.
	 * It has a length equal to the length of the Datanode list
	 * plus the length of the Coordinator list.
	 * Datanode fds are saved first, then Coordinator fds are saved.
	 */
	*fd_result = (int *) palloc(node_num * sizeof(int));
	if (*fd_result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	*pid_result = (int *) palloc(node_num * sizeof(int));
	if (*pid_result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}

	/*
	 * There are possible memory allocations in the core pooler, we want
	 * these allocations in the contect of the database pool
	 */
	oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

	max_batch_size = list_length(datanodelist) + list_length(coordlist);

	pg_read_barrier();
	/* Initialize fd_result */
	i = 0;
	/* Save in array fds of Datanodes first */
	foreach(nodelist_item, datanodelist)
	{
		node = lfirst_int(nodelist_item);

		/* valid check */
		if (node >= agent->num_dn_connections)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					        errmsg("invalid node index:%d, num_dn_connections is %d, pid:%d", node, agent->num_dn_connections, agent->pid)));
		}

		if (isSlotNeedPreProcessing(agent->dn_connections[node]))
		{
			slot = agent->dn_connections[node];

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++slot need prepare processing when agent_acquire_connections pid:%d nodeid:%d backend_pid:%d, need_move_to_usedpool %d, set4reuse %d++++", agent->pid, node, slot->backend_pid, slot->need_move_to_usedpool, slot->set4reuse);
			}

			pre_processing_slot_in_sync_response_queue(agent, slot, node, false);
		}

		slot = NULL;
		/* Acquire from the pool if none */
		if (NULL == agent->dn_connections[node])
		{
			slot = acquire_connection(agent, agent->pool, &userPool, node, 
									  agent->dn_conn_oids[node], false);

			/* Handle failure */
			if (slot == NULL)
			{
				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]acquire_connection can't get conn of node:%s from free slots", userPoolNodeName(userPool));
				}

				acquire_failed_num++;
				if (!asyncBatchReqTask)
				{
					asyncBatchReqTask = create_async_batch_request('g', agent, max_batch_size);
					init_connection_batch_request(asyncBatchReqTask, acquire_seq, datanodelist, coordlist, *fd_result, *pid_result);
				}

				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]going to acquire conn by sync thread for node:%s.", userPoolNodeName(userPool));
				}

				/* dispatch build connection request */
				succeed = dispatch_connection_batch_request(asyncBatchReqTask,
															NULL, /* we need to build a new connection*/
															userPool,
															PoolNodeTypeDatanode,
															node,
															false/* whether we are the last request */
				);
				if (!succeed)
				{
					goto FATAL_ERROR;
				}

				if (PoolPrintStatTimeout > 0)
				{
					g_pooler_stat.acquire_conn_from_thread++;
				}
				continue;
			}
			else
			{
				acquire_succeed_num++;
				if (PoolConnectDebugPrint)
				{
					/* double check, to ensure no double destory and multiple agents for one slot */
					elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d get datanode connection nodeindex:%d nodename:%s backend_pid:%d slot_seq:%d refcount:%d from hash table", agent->pid, node, slot->node_name, slot->backend_pid, slot->seqnum, slot->refcount);
					if (slot->bdestoryed || slot->pid != -1)
					{
						abort();
					}
				}

				/* Store in the descriptor */
				slot->pid = agent->pid;
				agent->dn_connections[node] = slot;
				if (slot->set4reuse)
				{
					set_request_num++;
					if (!asyncBatchReqTask)
					{
						asyncBatchReqTask = create_async_batch_request('g', agent, max_batch_size);
						init_connection_batch_request(asyncBatchReqTask, acquire_seq, datanodelist, coordlist, *fd_result, *pid_result);
					}

					if (PoolConnectDebugPrint)
					{
						elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] datanode node:%s nodeindex:%d pid:%d slot_seq:%d backend_pid:%d will change user from %s to %s, slot->pgoptions:%s, agent->pgoptions:%s", slot->node_name, node, agent->pid, slot->seqnum, slot->backend_pid, slot->user_name, agent->user_name, slot->pgoptions, agent->pgoptions);
					}

					/* dispatch set param request */
					succeed = dispatch_connection_batch_request(asyncBatchReqTask,
																slot, /* we already had a connection*/
																userPool,
																PoolNodeTypeDatanode,
																node,
																false/* whether we are the last request */
					);
					if (!succeed)
					{
						goto FATAL_ERROR;
					}

					if (PoolPrintStatTimeout > 0)
					{
						g_pooler_stat.acquire_conn_from_hashtab_and_set_reuse++;
					}
				}
				else
				{
					if (PoolPrintStatTimeout > 0)
					{
						g_pooler_stat.acquire_conn_from_hashtab++;
					}
				}
			}
		}
		else
		{
			if (PoolConnectDebugPrint)
			{
				slot = agent->dn_connections[node];
				elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] datanode node:%s nodeindex:%d pid:%d already got a slot_seq:%d backend_pid:%d in agent", slot->node_name, node, agent->pid, slot->seqnum, slot->backend_pid);
			}
		}
	}

	/* Save then in the array fds for Coordinators */
	foreach(nodelist_item, coordlist)
	{
		node = lfirst_int(nodelist_item);

		/* valid check */
		if (node >= agent->num_coord_connections)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					        errmsg(POOL_MGR_PREFIX"invalid node index:%d, num_dn_connections is %d, pid:%d", node, agent->num_dn_connections, agent->pid)));
		}

		/* Acquire from the pool if none */
		if (NULL == agent->coord_connections[node])
		{
			slot = acquire_connection(agent, agent->pool, &userPool, node, agent->coord_conn_oids[node], true);

			/* Handle failure */
			if (slot == NULL)
			{
				acquire_failed_num++;

				if (!asyncBatchReqTask)
				{
					asyncBatchReqTask = create_async_batch_request('g', agent, max_batch_size);
					init_connection_batch_request(asyncBatchReqTask, acquire_seq, datanodelist, coordlist, *fd_result, *pid_result);
				}

				/* dispatch build connection request */
				succeed = dispatch_connection_batch_request(asyncBatchReqTask,
															NULL, /* we need to build a new connection*/
															userPool,
															PoolNodeTypeCoord,
															node,
															false/* whether we are the last request */
				);
				if (!succeed)
				{
					goto FATAL_ERROR;
				}

				if (PoolPrintStatTimeout > 0)
				{
					g_pooler_stat.acquire_conn_from_thread++;
				}
			}
			else
			{
				acquire_succeed_num++;
				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d get coord connection nodeindex:%d nodename:%s backend_pid:%d slot_seq:%d from hash table", agent->pid, node, slot->node_name, slot->backend_pid, slot->seqnum);
					/* double check, to ensure no double destory and multiple agents for one slot */
					if (slot->bdestoryed || slot->pid != -1)
					{
						abort();
					}
				}
				/*
                 * Update newly-acquired slot with session parameters.
				 * Local parameters are fired only once BEGIN has been launched on
				 * remote nodes.
				*/
				slot->pid = agent->pid;
				agent->coord_connections[node] = slot;
				if (slot->set4reuse)
				{
					set_request_num++;

					if (!asyncBatchReqTask)
					{
						asyncBatchReqTask = create_async_batch_request('g', agent, max_batch_size);
						init_connection_batch_request(asyncBatchReqTask, acquire_seq, datanodelist, coordlist, *fd_result, *pid_result);
					}

					if (PoolConnectDebugPrint)
					{
						elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] datanode node:%s nodeindex:%d pid:%d slot_seq:%d backend_pid:%d will change user from %s to %s, slot->pgoptions:%s, agent->pgoptions:%s", slot->node_name, node, agent->pid, slot->seqnum, slot->backend_pid, slot->user_name, agent->user_name, slot->pgoptions, agent->pgoptions);
					}

					/* dispatch set param request */
					succeed = dispatch_connection_batch_request(asyncBatchReqTask,
																slot, /* we already had a connection*/
																userPool,
																PoolNodeTypeCoord,
																node,
																false/* whether we are the last request */
					);
					if (!succeed)
					{
						goto FATAL_ERROR;
					}

					if (PoolPrintStatTimeout > 0)
					{
						g_pooler_stat.acquire_conn_from_hashtab_and_set_reuse++;
					}
				}
				else
				{
					if (PoolPrintStatTimeout > 0)
					{
						g_pooler_stat.acquire_conn_from_hashtab++;
					}
				}
			}
		}
		else
		{
			if (PoolConnectDebugPrint)
			{
				slot = agent->coord_connections[node];
				elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] coordinator node:%s nodeindex:%d pid:%d already got a slot_seq:%d backend_pid:%d in agent", slot->node_name, node, agent->pid, slot->seqnum, slot->backend_pid);
			}
		}
	}

	MemoryContextSwitchTo(oldcontext);

	if (NULL == asyncBatchReqTask)
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d get all connections from hashtab", agent->pid);
		}

		i = 0;
		/* Save in array fds of Datanodes first */
		foreach(nodelist_item, datanodelist)
		{
			node = lfirst_int(nodelist_item);
			(*fd_result)[i] = PQsocket((PGconn *) agent->dn_connections[node]->conn);
			(*pid_result)[i] = ((PGconn *) agent->dn_connections[node]->conn)->be_pid;
			agent->dn_connections[node]->need_move_to_usedpool = true;
			connect_num++;
			i++;
		}

		/* Save then in the array fds for Coordinators */
		foreach(nodelist_item, coordlist)
		{
			node = lfirst_int(nodelist_item);
			(*fd_result)[i] = PQsocket((PGconn *) agent->coord_connections[node]->conn);
			(*pid_result)[i] = ((PGconn *) agent->coord_connections[node]->conn)->be_pid;
			connect_num++;
			i++;
		}

		/* set the number */
		*num = connect_num;
	}
	else
	{
		/* use async thread to process the request, dispatch last request */
		if (set_request_num || acquire_failed_num)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]send dispatch package. set_request_num=%d, acquire_failed_num=%d",
										set_request_num, acquire_failed_num);
			}

			/* dispatch set param request */
			succeed = dispatch_connection_batch_request(asyncBatchReqTask,
														NULL,
														userPool,
														PoolNodeTypeCoord,
														node,
														true/* we are the last request */
			);
		}

		if (!succeed)
		{
			goto FATAL_ERROR;
		}


		/* just return NULL*/
		if (PoolConnectDebugPrint)
		{
			if (set_request_num)
			{
				elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d acquire_succeed_num:%d acquire_failed_num:%d set_request_num:%d use parallel thread to process set request", agent->pid, acquire_succeed_num, acquire_failed_num, set_request_num);
			}
			else
			{
				elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections] pid:%d can't get all connections from hashtab, acquire_succeed_num:%d acquire_failed_num:%d set_request_num:%d use parallel thread to process", agent->pid, acquire_succeed_num, acquire_failed_num, set_request_num);
			}
		}

		*num = 0;
		*fd_result = NULL;
		*pid_result = NULL;
	}

	if (PoolConnectDebugPrint)
	{
		if (*fd_result == NULL)
		{
			elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]return fd_result = NULL");
		}
		else
		{
			elog(LOG, POOL_MGR_PREFIX"[agent_acquire_connections]return fd_result NOT NULL");
		}
	}

	return 0;

FATAL_ERROR:
	/* Task failed, there is only one batch task at this time, freeing memory */
	list_free(datanodelist);
	list_free(coordlist);
	destroy_async_batch_request(asyncBatchReqTask);
	elog(LOG, POOL_MGR_PREFIX"agent_acquire_connections pid:%d failed to get connections, acquire_succeed_num:%d acquire_failed_num:%d set_request_num:%d", agent->pid, acquire_succeed_num, acquire_failed_num, set_request_num);
	*num = 0;
	return -1;
}

/*
 * move one node connection to the hash table dn_connections_used_pool
 */
static void
move_node_connection_into_used_pool(PoolAgent *agent, int node)
{
	DnConnectionKey   key;
	bool              found;
	PGXCNodePoolSlot *slot;
	DnConnection     *dn_conn;
	int               is_pqready;

	slot = agent->dn_connections[node];
	if (slot && slot->need_move_to_usedpool)
	{
		key.node = node;
		key.backend_pid = slot->backend_pid;
		dn_conn = (DnConnection *) hash_search(agent->dn_connections_used_pool, &key, HASH_FIND, &found);
		if (unlikely(found))
		{
			is_pqready = pqReadReady((PGconn *) dn_conn->slot->conn);
			/* Clean up invalid connections */
			elog(LOG,
					POOL_MGR_PREFIX
					"++++move_node_connection_into_used_pool,found same backend pid, pid:%d release "
					"slot_seq:%d nodename:%s backend_pid:%d user_id:%d++++, is_pqready is %d",
					slot->backend_pid,
					dn_conn->slot->seqnum,
					dn_conn->slot->node_name,
					dn_conn->slot->backend_pid,
					dn_conn->slot->user_id,
					is_pqready);
				hash_search(agent->dn_connections_used_pool, &dn_conn->key, HASH_REMOVE, NULL);
				release_connection(agent,
									agent->pool,
									dn_conn->slot,
									dn_conn->key.node,
									agent->dn_conn_oids[dn_conn->key.node],
									true,
									false);
		}


		dn_conn = (DnConnection *) hash_search(agent->dn_connections_used_pool, &key, HASH_ENTER, &found);
		dn_conn->slot = slot;
		agent->dn_connections[node] = NULL;
		slot->need_move_to_usedpool = false;
		if (PoolConnectDebugPrint)
		{
			elog(LOG,
			     POOL_MGR_PREFIX "++++move_connections_into_used_pool pid:%d slot_seq:%d "
			                     "refcount:%d nodename:%s nodeid:%d backend_pid:%d++++",
			     agent->pid,
			     slot->seqnum,
			     slot->refcount,
			     slot->node_name,
			     node,
			     slot->backend_pid);
		}
	}
}

/*
 * After the dn connection is obtained by the backend process,
 * move the connection to the hash table dn_connections_used_pool
 */
static void
move_connections_into_used_pool(PoolAgent *agent, List *datanodelist)
{
	ListCell *nodelist_item = NULL;
	int		  node          = 0;

	if (agent->dn_connections_used_pool == NULL)
		return;

	foreach(nodelist_item, datanodelist)
	{
		node = lfirst_int(nodelist_item);

		move_node_connection_into_used_pool(agent, node);
	}
}

/*
 * Because the connection is first returned to the backend before processing the response
 * in pooler_handle_sync_response_queue, some operations need to be done on the main
 * thread before the next request comes
 */
static void
pre_processing_slot_in_sync_response_queue(PoolAgent *agent, PGXCNodePoolSlot *slot, int nodeid, bool release)
{
	/*
	 * if the connection has already been used but has not been
	 * moved to usedpool, move it now
	 */
	if (slot->need_move_to_usedpool)
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"[pre_processing_slot_in_sync_response_queue] move connections into used pool pid:%d slot_seq:%d refcount:%d nodename:%s nodeid:%d backend_pid:%d++++", agent->pid, slot->seqnum, slot->refcount, slot->node_name, nodeid, slot->backend_pid);
		}

		/* if release connection, just reset the status */
		if (release)
			slot->need_move_to_usedpool = false;
		else
			move_node_connection_into_used_pool(agent, nodeid);
	}

	/*
	 * It is possible that the agent released the connection before change_slot_user_pool
	 * in pooler_handle_sync_response_queue after applying for the connection.
	 * It is necessary to check the set4reuse status here
	 */
	if (slot->set4reuse)
	{
		bool found;
		PGXCUserPool *newUserPool;
		PGXCNodePool *nodePool = slot->userPool->nodePool;

		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"[pre_processing_slot_in_sync_response_queue] change slot user pool pid:%d slot_seq:%d refcount:%d nodename:%s nodeid:%d backend_pid:%d++++", agent->pid, slot->seqnum, slot->refcount, slot->node_name, nodeid, slot->backend_pid);
		}

		newUserPool = (PGXCUserPool *) hash_search(nodePool->userPools,
												  &agent->user_id, HASH_FIND, &found);
		Assert(found);
		change_slot_user_pool(agent, newUserPool, slot);
	}

	return;
}

/*
 * Obtain connections based on nodes and pid
 */
static PGXCNodePoolSlot *
get_connection_from_used_pool(PoolAgent *agent, int nodeid, int pid)
{
	DnConnectionKey   key;
	DnConnection     *dn_conn;
	bool 			  found;

	if (agent->dn_connections_used_pool == NULL)
		return NULL;

	key.node        = nodeid;
	key.backend_pid = pid;

	dn_conn = (DnConnection *) hash_search(agent->dn_connections_used_pool, &key,
										   HASH_FIND, &found);
	if (found)
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"++++get_connection_from_used_pool pid:%d slot_seq:%d refcount:%d nodename:%s nodeid:%d backend_pid:%d++++", agent->pid, dn_conn->slot->seqnum, dn_conn->slot->refcount, dn_conn->slot->node_name, nodeid, dn_conn->slot->backend_pid);
		}

		return dn_conn->slot;
	}
	else
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"++++get_connection_from_used_pool null pid:%d nodeid:%d backend_pid:%d++++", agent->pid, nodeid, pid);
		}

		return NULL;
	}
}

/*
 * Cancel query
 */
static int
cancel_query_on_connections(PoolAgent *agent, List *datanodelist, List *pidlist, List *coordlist, int signal)
{
	bool		ret;
	int 		node = 0;
	int 		pid = 0;
	ListCell   *nodelist_item;
	ListCell   *pidlist_item;
	int 		nCount = 0;
	int 		max_batch_size = list_length(datanodelist) + list_length(coordlist);
	PGXCNodePoolSlot *slot = NULL;
	PGXCPoolAsyncBatchTask *asyncBatchReqTask = NULL;

	if (agent == NULL)
		return 0;

	pg_read_barrier();
	/* Send cancel on Datanodes first */
	forboth(nodelist_item, datanodelist, pidlist_item, pidlist)
	{
		node = lfirst_int(nodelist_item);
		pid  = lfirst_int(pidlist_item);

		if (node < 0 || node >= agent->num_dn_connections)
			continue;

		if (agent->dn_connections == NULL)
			break;

		if (agent->dn_connections[node] != NULL)
		{
			if (agent->dn_connections[node]->backend_pid == pid)
			{
				/* backend has been received the slot, but the response has not been processd,
				 * we can put this slot into use pool, and can send cancel after that.
				 */
				move_node_connection_into_used_pool(agent, node);
			}
		}

		slot = get_connection_from_used_pool(agent, node, pid);

		if(slot != NULL)
		{
			nCount++;
			if (!asyncBatchReqTask)
			{
				asyncBatchReqTask = create_async_batch_request('h', agent, max_batch_size);
				init_cancel_query_batch_request(asyncBatchReqTask, signal);
			}
			ret = dispatch_batch_request(asyncBatchReqTask,
										 slot,
										 PoolNodeTypeDatanode,
										 node,
										 false);
			if (!ret)
				goto FATAL_ERROR;
		}
		else
		{
			elog(WARNING, POOL_MGR_PREFIX"cancel_query_on_connections slot not found, pid %d, node %d, remote pid %d, slot used %s",
				agent->pid, node, pid, agent->dn_connections[node] ? "true" : "false");
		}
	}

	/* Send cancel to Coordinators too, e.g. if DDL was in progress */
	foreach(nodelist_item, coordlist)
	{
		node = lfirst_int(nodelist_item);

		if (node < 0 || node >= agent->num_coord_connections)
			continue;

		if (agent->coord_connections == NULL)
			break;

		slot = agent->coord_connections[node];
		if(slot)
		{
			nCount++;
			if (!asyncBatchReqTask)
			{
				asyncBatchReqTask = create_async_batch_request('h', agent, max_batch_size);
				init_cancel_query_batch_request(asyncBatchReqTask, signal);
			}
			ret = dispatch_batch_request(asyncBatchReqTask,
					                     slot,
										 PoolNodeTypeCoord,
										 node,
										 false);
			if (!ret)
				goto FATAL_ERROR;
		}
	}

	if (nCount)
	{
		ret = dispatch_batch_request(asyncBatchReqTask,
									 NULL,
									 PoolNodeTypeCoord,
									 0,
									 true);
		if (!ret)
			goto FATAL_ERROR;
	}
	return nCount;

FATAL_ERROR:
	/* Task failed, there is only one batch task at this time, freeing memory */
	destroy_async_batch_request(asyncBatchReqTask);
	elog(LOG, POOL_MGR_PREFIX"cancel_query_on_connections failed pid:%d ", agent->pid);
	return -1;
}

/*
 * Return connections back to the pool
 */
void
PoolManagerReleaseAllConnections(bool force)
{
	char msgtype = 'r';
	int n32;
	int msglen = 8;

	/* If disconnected from pooler all the connections already released */
	if (!poolHandle)
	{
		return;
	}

	elog(DEBUG1, "PoolManagerReleaseAllConnections returning connections back to the pool");
	HOLD_CANCEL_INTERRUPTS();
	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	n32 = pg_hton32(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Lock information */
	n32 = pg_hton32((int) force);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);
	pool_flush(&poolHandle->port);
	RESUME_CANCEL_INTERRUPTS();
}

/*
 * Return specific connections back to the pool
 */
void
PoolManagerReleaseConnections(int dn_count, int *dn_list, int *dn_pid_list, int co_count, int *co_list, bool force)
{
#define BUFFER_LEN (2 + dn_count + dn_count + co_count + 1)
	uint32		n32 = 0;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32      buf[BUFFER_LEN]; /* dnlength + dnnodeid + dnpid + cnlength + cnnodeid + force */
	uint32      offset = 0;
	int 		i;

	/* If disconnected from pooler all the connections already released */
	if (!poolHandle)
	{
		return;
	}
	if (enable_distri_print)
		elog(LOG, "PoolManagerReleaseConnections returning connections back to the pool, cn:%d,dn:%d", co_count, dn_count);

	if (dn_count == 0 && co_count == 0)
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerReleaseConnections no node speicfied");
		return;
	}

	if (dn_count != 0 && (dn_list == NULL || dn_pid_list == NULL))
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerReleaseConnections invalid dn_count:%d, null dn_list or dn_pid_list", dn_count);
		return;
	}

	if (co_count != 0 && co_list == NULL)
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerReleaseConnections invalid co_count:%d, null co_list", co_count);
		return;
	}
	HOLD_CANCEL_INTERRUPTS();
	/* Insert the list of Datanodes in buffer */
	n32 = pg_hton32((uint32) dn_count);
	buf[offset++] = n32;/* node count */

	for (i = 0; i < dn_count; i++)
	{
		n32 = pg_hton32((uint32) dn_list[i]);
		buf[offset++] = n32;
	}

	for (i = 0; i < dn_count; i++)
	{
		n32 = pg_hton32((uint32) dn_pid_list[i]);
		buf[offset++] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = pg_hton32((uint32) co_count);
	buf[offset++] = n32;

	for (i = 0; i < co_count; i++)
	{
		n32 = pg_hton32((uint32) co_list[i]);
		buf[offset++] = n32;
	}

	n32 = pg_hton32((uint32) force);
	buf[offset++] = n32;

	Assert(offset == BUFFER_LEN);

	pool_putmessage(&poolHandle->port, 'u', (char *) buf, offset * sizeof(uint32));
	pool_flush(&poolHandle->port);
	RESUME_CANCEL_INTERRUPTS();
}

/*
 * Cancel Query
 */
bool
PoolManagerCancelQuery(int dn_count, int* dn_list, int* dn_pid_list, int co_count, int* co_list, int signal)
{
#define BUFFER_LEN (2 + dn_count + dn_count + co_count + 1)
	uint32		n32 = 0;
	int32       res = 0;
	/*
	 * Buffer contains the list of both Coordinator and Datanodes, as well
	 * as the number of connections
	 */
	uint32      buf[BUFFER_LEN]; /* dnlength + dnnodeid + dnpid + cnlength + cnnodeid + signal */
	uint32      offset = 0;
	int 		i;
	if (NULL == poolHandle)
	{
		PoolManagerReconnect();
		/* After reconnect, recheck the handle. */
		if (NULL == poolHandle)
		{
			elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery poolHandle is NULL");
			return false;
		}
	}

	if (dn_count == 0 && co_count == 0)
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery no node speicfied");
		return true;
	}

	if (dn_count != 0 && (dn_list == NULL || dn_pid_list == NULL))
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery invalid dn_count:%d, null dn_list or dn_pid_list", dn_count);
		return false;
	}

	if (co_count != 0 && co_list == NULL)
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery invalid co_count:%d, null co_list", co_count);
		return false;
	}

	if (signal > SIGNAL_SIGUSR2 || signal < SIGNAL_SIGINT)
	{
		elog(LOG, POOL_MGR_PREFIX"PoolManagerCancelQuery invalid signal:%d", signal);
		return false;
	}

	HOLD_CANCEL_INTERRUPTS();
	/* Insert the list of Datanodes in buffer */
	n32 = pg_hton32((uint32) dn_count);
	buf[offset++] = n32;/* node count */

	for (i = 0; i < dn_count; i++)
	{
		n32 = pg_hton32((uint32) dn_list[i]);
		buf[offset++] = n32;
		
		if (enable_distri_print)
			elog(LOG, "pooler cancel query dn %d %d", dn_list[i], dn_pid_list[i]);
	}

	for (i = 0; i < dn_count; i++)
	{
		n32 = pg_hton32((uint32) dn_pid_list[i]);
		buf[offset++] = n32;
	}

	/* Insert the list of Coordinators in buffer */
	n32 = pg_hton32((uint32) co_count);
	buf[offset++] = n32;

	for (i = 0; i < co_count; i++)
	{
		n32 = pg_hton32((uint32) co_list[i]);
		buf[offset++] = n32;

		if (enable_distri_print)
			elog(LOG, "cancel query cn %d", co_list[i]);
	}

	/* signal type */
	n32 = pg_hton32((uint32) signal);
	buf[offset++] = n32;

	Assert(offset == BUFFER_LEN);
	
	pool_putmessage(&poolHandle->port, 'h', (char *) buf, offset * sizeof(uint32));
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port, false);

	if (res != (dn_count + co_count))
	{
		/* Set to LOG to avoid regress test failure. */
		elog(LOG, POOL_MGR_PREFIX"cancel query on remote nodes required:%d return:%d", dn_count + co_count, res);
	}
	RESUME_CANCEL_INTERRUPTS();
	return res == (dn_count + co_count);
}

/*
 * Release all connections for Datanodes and Coordinators
 */
static void
agent_release_all_connections(PoolAgent *agent, bool force_destroy)
{
	int			  i;
	MemoryContext oldcontext;

	/* increase query count */
	agent->query_count++;
	if (!agent->dn_connections && !agent->coord_connections)
	{
		return;
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"++++agent_release_all_connections pid:%d begin++++", agent->pid);
	}

	/*
	 * similar to agent destroy, it requires check whether we
	 * can release the connections now, perhaps some tasks are
	 * still being processed in the background and cannot be
	 * directly released the slot
 	*/
	if (!agent_can_destory(agent))
	{
		agent->release_pending_type = (force_destroy) ? FORCE_RELEASE_ALL : RELEASE_ALL;
		elog(LOG, POOL_MGR_PREFIX"pend release all connections, ref_count:%d, pid:%d, force_destroy:%d", agent->ref_count, agent->pid, force_destroy);
		return;
	}

	/*
	 * There are possible memory allocations in the core pooler, we want
	 * these allocations in the content of the database pool
	 */
	oldcontext = MemoryContextSwitchTo(agent->pool->mcxt);

	/*
	 * Remaining connections are assumed to be clean.
	 * First clean up for Datanodes
	 */
	for (i = 0; i < agent->num_dn_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->dn_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++agent_release_all_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d user_id:%d++++",
						 agent->pid, slot->seqnum, slot->node_name, slot->backend_pid, slot->user_id);
			}

			release_connection(agent, agent->pool, slot, i, agent->dn_conn_oids[i], force_destroy, false);
		}
		agent->dn_connections[i] = NULL;
	}

	/* Then clean up for Coordinator connections */
	for (i = 0; i < agent->num_coord_connections; i++)
	{
		PGXCNodePoolSlot *slot = agent->coord_connections[i];

		/*
		 * Release connection.
		 * If connection has temporary objects on it, destroy connection slot.
		 */
		if (slot)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++agent_release_all_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d++++", agent->pid, slot->seqnum, slot->node_name, slot->backend_pid);
			}
			release_connection(agent, agent->pool, slot, i, agent->coord_conn_oids[i], force_destroy, true);
		}
		agent->coord_connections[i] = NULL;
	}

	if (agent->dn_connections_used_pool)
	{
		HASH_SEQ_STATUS hseq_status;
		DnConnection   *dn_conn;

		hash_seq_init(&hseq_status, agent->dn_connections_used_pool);
		while ((dn_conn = (DnConnection *) hash_seq_search(&hseq_status)))
		{
			PGXCNodePoolSlot *slot = dn_conn->slot;
			hash_search(agent->dn_connections_used_pool, &dn_conn->key, HASH_REMOVE, NULL);

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++agent_release_all_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d++++", agent->pid, slot->seqnum, slot->node_name, slot->backend_pid);
			}
			release_connection(agent, agent->pool, slot, dn_conn->key.node, agent->dn_conn_oids[dn_conn->key.node], force_destroy, false);
		}
	}

	if (!force_destroy && agent->pool->oldest_idle == (time_t) 0)
	{
		agent->pool->oldest_idle = time(NULL);
	}
	MemoryContextSwitchTo(oldcontext);

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"++++agent_release_all_connections done++++, pid:%d", agent->pid);
	}
}

/*
 * Release specific connections for Datanodes and Coordinators
 */
static void
agent_release_connections(PoolAgent *agent, List *datanodelist, List *dn_pidlist, List *coordlist, int force_destroy)
{
	int 		node = 0;
	int 		pid = 0;
	ListCell   *nodelist_item;
	ListCell   *pidlist_item;
	PGXCNodePoolSlot *slot = NULL;
	DnConnectionKey  key;

	/* Send cancel on Datanodes first */
	forboth(nodelist_item, datanodelist, pidlist_item, dn_pidlist)
	{
		node = lfirst_int(nodelist_item);
		pid  = lfirst_int(pidlist_item);

		if (node < 0 || node >= agent->num_dn_connections)
			continue;

		slot = get_connection_from_used_pool(agent, node, pid);
		if (slot)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d user_id:%d++++",
					 agent->pid, slot->seqnum, slot->node_name, slot->backend_pid, slot->user_id);
			}

			key.node = node;
			key.backend_pid = pid;
			hash_search(agent->dn_connections_used_pool, &key, HASH_REMOVE, NULL);
			release_connection(agent, agent->pool, slot, node, agent->dn_conn_oids[node], force_destroy, false);

			
		}
	}

	/* Send cancel to Coordinators too, e.g. if DDL was in progress */
	foreach(nodelist_item, coordlist)
	{
		node = lfirst_int(nodelist_item);

		if (node < 0 || node >= agent->num_coord_connections)
			continue;

		if (agent->coord_connections == NULL)
			break;

		slot = agent->coord_connections[node];
		if(slot)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d++++", agent->pid, slot->seqnum, slot->node_name, slot->backend_pid);
			}
			release_connection(agent, agent->pool, slot, node, agent->coord_conn_oids[node], force_destroy, true);
		}
		agent->coord_connections[node] = NULL;
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"++++agent_release_connections done++++, pid:%d", agent->pid);
	}

	return;
}

/*
 * Create new empty pool for a database.
 * By default Database Pools have a size null so as to avoid interactions
 * between PGXC nodes in the cluster (Co/Co, Dn/Dn and Co/Dn).
 * Pool is increased at the first GET_CONNECTION message received.
 * Returns POOL_OK if operation succeed POOL_FAIL in case of OutOfMemory
 * error and POOL_WEXIST if poll for this database already exist.
 */
static DatabasePool *
create_database_pool(const char *database, const char *user_name, const char *pgoptions, int user_id)
{
#define     NODE_POOL_NAME_LEN    256
	bool 			need_pool = true;
	MemoryContext	oldcontext;
	MemoryContext	dbcontext;
	DatabasePool   *databasePool;
	HASHCTL			hinfo;
	int				hflags;
	char            hash_name[NODE_POOL_NAME_LEN];

	dbcontext = AllocSetContextCreate(PoolerCoreContext,
									  "DB Context",
									  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(dbcontext);
	/* Allocate memory */
	databasePool = (DatabasePool *) palloc(sizeof(DatabasePool));
	if (!databasePool)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		return NULL;
	}

	databasePool->mcxt = dbcontext;
	 /* Copy the database name */
	databasePool->database = pstrdup(database);
	 /* Copy the user name */
	databasePool->user_name = pstrdup(user_name);
	 /* Copy the pgoptions */
	databasePool->pgoptions = pstrdup(pgoptions);

	databasePool->user_id = user_id;

	/* Reset the oldest_idle value */
	databasePool->oldest_idle = (time_t) 0;

	if (!databasePool->database)
	{
		/* out of memory */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
		pfree(databasePool);
		return NULL;
	}

	/* Init prev,next reference */
	databasePool->next = NULL;
	databasePool->prev = NULL;

	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCNodePool);
	hflags |= HASH_ELEM;

	hinfo.hcxt = dbcontext;
	hflags |= HASH_CONTEXT;

	hinfo.hash = oid_hash;
	hflags |= HASH_FUNCTION;

	snprintf(hash_name, NODE_POOL_NAME_LEN, "%s_%s_Node_Pool", database, user_name);
	databasePool->nodePools = hash_create(hash_name,
										  OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER,
										  &hinfo, hflags);
	MemoryContextSwitchTo(oldcontext);

	/* Insert into the list */
	insert_database_pool(databasePool);
	need_pool = connection_need_pool(g_unpooled_database, database);
	if (need_pool)
	{
		need_pool =  connection_need_pool(g_unpooled_user, user_name);
	}

	databasePool->bneed_pool 	  = need_pool;
	databasePool->version         = 0;
	return databasePool;
}


/*
 * Insert new database pool to the list
 */
static void
insert_database_pool(DatabasePool *databasePool)
{
	Assert(databasePool);

	/* Reference existing list or null the tail */
	if (databasePoolsHead)
	{
		databasePool->next = databasePoolsHead;
		databasePoolsHead->prev = databasePool;
		databasePoolsHead = databasePool;
	}
	else
		databasePoolsTail = databasePoolsHead = databasePool;
}

/*
 * Node pool size consistency check
 */
static bool
node_pool_size_check(DatabasePool *databasePool, PGXCNodePool *nodePool)
{
	int              i, j;
	int32            index;
	PoolAgent        *agent;
	int              useSize = 0;

	RebuildAgentIndex();

	for (i = 0; i < agentCount; i++)
	{
		index = agentIndexes[i];
		agent = poolAgents[index];

		if (agent->pool != databasePool)
			continue;

		if (isNodePoolCoord(nodePool))
		{
			for (j = 0; j < agent->num_coord_connections; j++)
			{
				if (agent->coord_conn_oids[j] == nodePool->nodeoid)
				{
					if (agent->coord_connections[j])
						++useSize;
					break;
				}
			}
		}
		else
		{
			for (j = 0; j < agent->num_dn_connections; j++)
			{
				if (agent->dn_conn_oids[j] == nodePool->nodeoid)
				{
					if (agent->dn_connections[j])
						++useSize;
					break;
				}
			}

			if (agent->dn_connections_used_pool)
			{
				HASH_SEQ_STATUS hseq_status;
				DnConnection   *dn_conn;

				hash_seq_init(&hseq_status, agent->dn_connections_used_pool);
				while ((dn_conn = (DnConnection *) hash_seq_search(&hseq_status)))
				{
					if (dn_conn->key.node == nodePool->nodeoid)
					{
						if (dn_conn->slot)
							++useSize;
					}
				}
			}
		}
	}

	if (nodePool->size != nodePool->freeSize + useSize)
	{
		elog(LOG, POOL_MGR_PREFIX"nodePool:%s of node (%u, %s) size inconsistency!!! "
				  "size:%d, freeSize:%d, useSize:%d.", nodePool->connstr,
			 nodePool->nodeoid, nodePoolNodeName(nodePool),
			 nodePool->size, nodePool->freeSize, useSize);
		return false;
	}

	return true;
}

/*
 * Node all connections pool size consistency check
 */
static void
node_total_pool_size_check(void)
{
	DatabasePool *databasePool;
	HASH_SEQ_STATUS hseq_status;
	PGXCNodePool *nodePool;
	PGXCNodeSingletonInfo *nodeInfo;
	int freeSize;
	int size;
	int ref_count;

	hash_seq_init(&hseq_status, g_NodeSingletonInfo);
	while ((nodeInfo = (PGXCNodeSingletonInfo *) hash_seq_search(&hseq_status)))
	{
		freeSize = 0;
		size = 0;
		ref_count = 0;
		databasePool = databasePoolsHead;
		while (databasePool)
		{
			nodePool = (PGXCNodePool *) hash_search(databasePool->nodePools, &nodeInfo->nodeoid, HASH_FIND, NULL);
			if (nodePool)
			{
				freeSize += nodePool->freeSize;
				size += nodePool->size;
				ref_count += 1;
			}
			databasePool = databasePool->next;
		}

		if (freeSize != nodeInfo->freeSize || size != nodeInfo->size || ref_count != nodeInfo->ref_count)
		{
			elog(WARNING, POOL_MGR_PREFIX"nodePool:%u 's nodeInfo check size failure. "
					  "freeSize:%d, nodeInfo->freeSize:%d, size:%d, nodeInfo->size:%d, "
					  "ref_count:%d, freeSize:%d", nodeInfo->nodeoid, freeSize, nodeInfo->freeSize,
				 size, nodeInfo->size, ref_count, nodeInfo->ref_count);

			nodeInfo->freeSize = freeSize;
			nodeInfo->size = size;
			nodeInfo->ref_count = ref_count;
		}
	}
}

/*
 * Rebuild information of database pools
 */
static void
reload_database_pools(PoolAgent *agent, bool check_pool_size)
{
	bool          bsucceed = false;
	DatabasePool *databasePool;

	/*
	 * Release node connections if any held. It is not guaranteed client session
	 * does the same so don't ever try to return them to pool and reuse
	 */
	agent_release_all_connections(agent, true);

	/* before destory nodepool, just wait for all async task is done */
	bsucceed = pooler_wait_for_async_task_done();
	if (!bsucceed)
	{
		elog(WARNING, POOL_MGR_PREFIX"async task not finish before reload all database pools");
	}

	/* Forget previously allocated node info */
	MemoryContextReset(agent->mcxt);

	/* and allocate new */
	PgxcNodeGetOids(&agent->coord_conn_oids, &agent->dn_conn_oids,
					&agent->num_coord_connections, &agent->num_dn_connections, false);

	agent->coord_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot *));
	agent->dn_connections = (PGXCNodePoolSlot **)
			palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot *));
	create_dn_connections_used_pool(agent);

	cancel_old_connection();
	/*
	 * Scan the list and destroy any altered pool. They will be recreated
	 * upon subsequent connection acquisition.
	 */
	databasePool = databasePoolsHead;
	while (databasePool)
	{
		/* Update each database pool slot with new connection information */
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, databasePool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char *connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool, nodePool->nodeInfo);

			if (connstr_chk == NULL || strcmp(connstr_chk, nodePool->connstr))
			{
				/* Node has been removed or altered */
				if (nodePool->size == nodePool->freeSize && !nodePool->asyncInProgress)
				{
					elog(LOG, POOL_MGR_PREFIX"nodePool:%s has been changed, size:%d, freeSize:%d nodeName:%s, destory it now", nodePool->connstr, nodePool->size, nodePool->freeSize, nodePoolNodeName(nodePool));
					destroy_node_pool(nodePool);
					if ((--nodePool->nodeInfo->ref_count) == 0)
						hash_search(g_NodeSingletonInfo, &nodePool->nodeoid, HASH_REMOVE, NULL);
					hash_search(databasePool->nodePools, &nodePool->nodeoid,HASH_REMOVE, NULL);
				}
				else
				{
					if (check_pool_size && !node_pool_size_check(databasePool, nodePool))
					{
						elog(WARNING, POOL_MGR_PREFIX"nodePool:%s of node (%u, %s) is removed"
								  " due to size check failure .", nodePool->connstr,
							 nodePool->nodeoid, nodePoolNodeName(nodePool));

						destroy_node_pool(nodePool);
						if ((--nodePool->nodeInfo->ref_count) == 0)
							hash_search(g_NodeSingletonInfo, &nodePool->nodeoid, HASH_REMOVE, NULL);
						hash_search(databasePool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);

						if (connstr_chk)
						{
							pfree(connstr_chk);
						}
						continue;
					}

					destroy_node_pool_free_slots(nodePool, databasePool, NULL, true);

					/* fresh the connect string so that new coming connection will connect to the new node */
					if (connstr_chk)
					{
						if (nodePool->connstr)
						{
							pfree(nodePool->connstr);
						}
						nodePool->connstr = pstrdup(connstr_chk);
					}
				}
			}
			else if (check_pool_size && !node_pool_size_check(databasePool, nodePool))
			{
				elog(WARNING, POOL_MGR_PREFIX"nodePool:%s of node (%u, %s) is removed"
						  " due to size check failure .", nodePool->connstr,
					 nodePool->nodeoid, nodePoolNodeName(nodePool));

				destroy_node_pool(nodePool);
				if ((--nodePool->nodeInfo->ref_count) == 0)
					hash_search(g_NodeSingletonInfo, &nodePool->nodeoid, HASH_REMOVE, NULL);
				hash_search(databasePool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
			}

			if (connstr_chk)
			{
				pfree(connstr_chk);
			}
		}

		databasePool = databasePool->next;
	}

	if (check_pool_size)
		node_total_pool_size_check();

	refresh_node_map();
}


static uint32
connstrhashfunc(const void *key, Size keysize)
{
	uint32 hashval;

	hashval = DatumGetUInt32(hash_any(key, keysize));

	return hashval;
}

static void
cancel_old_connection(void)
{
	int		i;
	int		count;
	int32	index;
	PoolAgent *agent;
	char	*connstr_chk = NULL;
	HTAB	*exp_conn;
	HASHCTL	hctl;

	RebuildAgentIndex();
	hctl.hash = connstrhashfunc;
	hctl.keysize = sizeof(ConnStrKey);
	hctl.entrysize = sizeof(ConnStrEntry);
	exp_conn = hash_create("exp conn str", 64, &hctl, HASH_ELEM | HASH_FUNCTION);

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = 0; count < agentCount; count++)
	{
		HASH_SEQ_STATUS	hseq_status;
		DnConnection	*dn_conn;
		bool	send_kill = false;
		ConnStrKey	key;
		ConnStrEntry *entry;
		bool	found = false;
		PGPROC	*proc;

		index = agentIndexes[count];
		agent = poolAgents[index];

		/* The agent may have just been created without initialization */
		if (agent->pid <= 0)
			continue;

		for (i = 0; i < agent->num_coord_connections; i++)
		{
			if (agent->coord_connections[i] != NULL)
			{
				key.nodeoid = agent->coord_conn_oids[i];
				key.dbpool = agent->pool;

				entry = hash_search(exp_conn, &key, HASH_FIND, &found);
				if (found)
					connstr_chk = entry->connstr;
				else
				{
					connstr_chk = build_node_conn_str(agent->coord_conn_oids[i], agent->pool, NULL);
					if (connstr_chk != NULL)
					{
						entry = hash_search(exp_conn, &key, HASH_ENTER, &found);
						StrNCpy(entry->connstr, connstr_chk, strlen(connstr_chk));
						pfree(connstr_chk);
						connstr_chk = entry->connstr;
					}
				}

				if (connstr_chk == NULL ||
					strcmp(connstr_chk, agent->coord_connections[i]->userPool->connstr) != 0)
				{
					elog(LOG, POOL_MGR_PREFIX"coordinator %d has been changed, teminate the pid %d ",
						agent->coord_conn_oids[i], agent->pid);

					proc = BackendPidGetProc(agent->pid);
					if (proc != NULL)
					{
						if (kill(agent->pid, SIGTERM))
							elog(WARNING, POOL_MGR_PREFIX"kill(%d,%d) failed: %m",
										agent->pid, SIGTERM);
						send_kill = true;
						/* no need check other connection */
						break;
					}
				}
			}
		}

		/*
		 * If there is no DN connection or SIGTERM signal has already been sent, 
		 * there is no need to continue checking the DN connection.
		 */
		if (agent->dn_connections_used_pool == NULL || send_kill)
			continue;

		hash_seq_init(&hseq_status, agent->dn_connections_used_pool);
		while ((dn_conn = (DnConnection *) hash_seq_search(&hseq_status)))
		{
			key.nodeoid = dn_conn->key.node;
			key.dbpool = agent->pool;
			entry = hash_search(exp_conn, &key, HASH_FIND, &found);
			if (found)
				connstr_chk = entry->connstr;
			else
			{
				connstr_chk = build_node_conn_str(dn_conn->key.node, agent->pool, NULL);
				if (connstr_chk != NULL)
				{
					entry = hash_search(exp_conn, &key, HASH_ENTER, &found);
					StrNCpy(entry->connstr, connstr_chk, strlen(connstr_chk));
					pfree(connstr_chk);
					connstr_chk = entry->connstr;
				}
			}
			if (connstr_chk == NULL ||
				strcmp(connstr_chk, dn_conn->slot->userPool->connstr) != 0)
			{
				elog(LOG, POOL_MGR_PREFIX"datanode %d has been changed, teminate the pid %d ",
					dn_conn->key.node, agent->pid);

				proc = BackendPidGetProc(agent->pid);
				if (proc != NULL)
				{
					if (kill(agent->pid, SIGTERM))
						elog(WARNING, POOL_MGR_PREFIX"kill(%d,%d) failed: %m",
									agent->pid, SIGTERM);
					hash_seq_term(&hseq_status);
					/* no need check other connection */
					break;
				}
			}
		}
	}

	hash_destroy(exp_conn);
}

/*
 * Find pool for specified database and username in the list
 */
static DatabasePool *
find_database_pool(const char *database, const char *user_name, const char *pgoptions)
{
	DatabasePool *databasePool;

	/* Scan the list */
	databasePool = databasePoolsHead;

	if (PoolerStatelessReuse)
	{
		while (databasePool)
		{
			if (strcmp(database, databasePool->database) == 0)
				break;

			databasePool = databasePool->next;
		}
	}
	else
	{
		while (databasePool)
		{
			if (strcmp(database, databasePool->database) == 0 &&
				strcmp(user_name, databasePool->user_name) == 0 &&
				strcmp(pgoptions, databasePool->pgoptions) == 0)
				break;

			databasePool = databasePool->next;
		}
	}
	return databasePool;
}

/*
 * Acquire connection
 */
static PGXCNodePoolSlot *
acquire_connection_from_pool(PoolAgent *agent, PGXCNodePool *nodePool, PGXCUserPool *userPool, int32 nodeidx, Oid node, int *loop)
{
	int poll_result;
	PGXCNodePoolSlot *slot = NULL;
	
	while (userPool && userPool->freeSize > 0)
	{
		(*loop)++;

		DecreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);
		
		slot = userPool->slot[userPool->freeSize];
		userPool->slot[userPool->freeSize] = NULL;

		if (PoolConnectDebugPrint)
		{
			Assert(slot->userPool == userPool);
			elog(LOG, POOL_MGR_PREFIX"acquire_connection alloc a connection to node:%s "
					"user_name:%s backend_pid:%d nodeidx:%d userPool size:%d freeSize:%d",
				 	nodePoolNodeName(nodePool), slot->user_name, slot->backend_pid, nodeidx,
				    userPool->size, userPool->freeSize);
		}

retry:
		/* only pick up the connections that matches the latest version */
		if (slot->m_version == nodePool->m_version)
		{
			int fd = PQsocket((PGconn *) slot->conn);
			if (fd > 0)
			{
				/*
				 * Make sure connection is ok, destroy connection slot if there is a
				 * problem.
				 */
				poll_result = pqReadReady((PGconn *) slot->conn);

				if (poll_result == 0)
				{
					/* increase use count */
					slot->usecount++;
					return slot; 		/* ok, no data */
				}
				else if (poll_result < 0)
				{
					if (errno == EAGAIN || errno == EINTR)
	                {
					    errno = 0;
					    goto retry;
				     }


					elog(WARNING, POOL_MGR_PREFIX"Error in checking connection, errno = %d", errno);
				}
				else
				{
					elog(WARNING, POOL_MGR_PREFIX"Unexpected data on connection, cleaning.");
				}
			}
			else
			{
				elog(WARNING, POOL_MGR_PREFIX"connection to node %u contains invalid fd:%d", node, fd);
			}
		}

		DecreaseUserPoolerSize(userPool, __FILE__, __LINE__);

		destroy_slot(nodeidx, node, slot);
		slot = NULL;

		/* Ensure we are not below minimum size */
		if (0 == userPool->freeSize)
		{
			return slot;
		}
	}

	return slot;
}

/*
 * get the user connections pool
 */
static PGXCUserPool*
get_user_pool(PGXCNodePool *nodePool, DatabasePool *dbPool, Oid user_id, char *user_name, char *pgoptions, bool *out_found)
{
	PGXCUserPool *userPool;
	bool          found;

	if (PoolerStatelessReuse || nodePool->userPoolListHead == NULL)
	{
		userPool = (PGXCUserPool *) hash_search(nodePool->userPools, &user_id, HASH_ENTER,
												&found);
		if (!found)
		{
			userPool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
			if (!userPool->slot)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						errmsg(POOL_MGR_PREFIX"out of memory")));
			}
			userPool->user_id   = user_id;
			userPool->user_name = pstrdup(user_name);
			userPool->pgoptions = pstrdup(pgoptions);
			userPool->connstr   = build_node_conn_str_by_user(nodePool->nodeInfo, dbPool, userPool->user_name, userPool->pgoptions, NULL);
			userPool->freeSize  = 0;
			userPool->size      = 0;
			userPool->refcount  = 0;
			userPool->nodePool  = nodePool;
			userPool->prev      = NULL;
			userPool->next      = nodePool->userPoolListHead;

			/* UserPool is active and placed at the head of the linked list */
			if (nodePool->userPoolListHead == NULL)
				nodePool->userPoolListTail = userPool;
			else
				nodePool->userPoolListHead->prev = userPool;
			nodePool->userPoolListHead = userPool;

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"get_user_pool(%p) create new userPool user_id:%d, user_name:%s, connstr:%s", userPool, userPool->user_id, userPool->user_name, userPool->connstr);
			}
		}
		else
		{
			/* UserPool is active, move it to the head of the linked list */
			if (userPool != nodePool->userPoolListHead)
			{
				if (userPool == nodePool->userPoolListTail)
				{
					userPool->prev->next = NULL;
					nodePool->userPoolListTail = userPool->prev;
				}
				else
				{
					userPool->prev->next = userPool->next;
					userPool->next->prev = userPool->prev;
				}
				userPool->prev = NULL;
				userPool->next = nodePool->userPoolListHead;
				nodePool->userPoolListHead->prev = userPool;
				nodePool->userPoolListHead = userPool;
			}
		}

		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"get_user_pool(%p) from userPools user_id:%d, user_name:%s, connstr:%s", userPool, userPool->user_id, userPool->user_name, userPool->connstr);
		}
	}
	else
	{
		found    = true;
		userPool = nodePool->userPoolListHead;
	}

	/* Active db moves to the head of the linked list */
	if (dbPool != databasePoolsHead)
	{
		if (dbPool == databasePoolsTail)
		{
			dbPool->prev->next = NULL;
			databasePoolsTail = dbPool->prev;
		}
		else
		{
			dbPool->prev->next = dbPool->next;
			dbPool->next->prev = dbPool->prev;
		}
		dbPool->prev = NULL;
		dbPool->next = databasePoolsHead;
		databasePoolsHead->prev = dbPool;
		databasePoolsHead = dbPool;
	}

	if (out_found)
		*out_found = found;

	return userPool;
}

/*
 * Acquire connection
 */
static PGXCNodePoolSlot *
acquire_connection(PoolAgent *agent, DatabasePool *dbPool, PGXCUserPool **userpool, int32 nodeidx, Oid node, bool bCoord)
{
	int32              loop = 0;
	PGXCNodePool	   *nodePool;
	PGXCNodePoolSlot   *slot;
	PGXCUserPool	   *userPool;
	bool found;

	Assert(dbPool);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND,
											NULL);

	/*
	 * When a Coordinator pool is initialized by a Coordinator Postmaster,
	 * it has a NULL size and is below minimum size that is 1
	 * This is to avoid problems of connections between Coordinators
	 * when creating or dropping Databases.
	 */
	if (nodePool == NULL || nodePool->freeSize == 0)
	{
		if (PoolConnectDebugPrint)
		{
			if (nodePool)
			{
				elog(LOG, POOL_MGR_PREFIX"node:%u no free connection, nodeindex:%d, size:%d, freeSize:%d begin grow in async mode", nodePool->nodeoid, nodeidx, nodePool->size, nodePool->freeSize);
			}
		}
		/* here, we try to build entry of the hash table */
		nodePool = grow_pool(dbPool, nodeidx, node, bCoord);
	}


	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"node:%u nodeidx:%d size:%d, freeSize:%d", nodePool->nodeoid, nodeidx, nodePool->size, nodePool->freeSize);
	}

	slot = NULL;

	userPool = get_user_pool(nodePool, dbPool, agent->user_id, agent->user_name, agent->pgoptions, &found);

	/* get the userpool */
	*userpool = userPool;

	/* Check available connections */
	if (nodePool && nodePool->freeSize > 0)
	{
		PGXCUserPool *otherUserPool;
		if (found)
		{
			slot = acquire_connection_from_pool(agent, nodePool, userPool, nodeidx, node, &loop);
			if (slot != NULL)
				goto acquired;
		}

		/* reuse from inactive user connection pool */
		for (otherUserPool = nodePool->userPoolListTail; otherUserPool; otherUserPool = otherUserPool->prev)
		{
			if (otherUserPool != userPool && otherUserPool->freeSize > 0)
			{
				slot = acquire_connection_from_pool(agent, nodePool, otherUserPool, nodeidx, node, &loop);
				if (slot != NULL)
					goto acquired;
			}
		}
	}

acquired:
	if (PoolConnectDebugPrint)
	{
		if (NULL == slot)
		{
			if (0 == loop)
			{
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg(POOL_MGR_PREFIX"no more free connection to node:%u nodeidx:%d", node, nodeidx)));
			}
			else
			{
				ereport(WARNING,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg(POOL_MGR_PREFIX"no valid free connection to node:%u nodeidx:%d", node, nodeidx)));
			}
		}
	}

	if (slot)
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"acquire_connection from userPools user_id:%d, user_name:%s, slot->user_id:%d, slot->user_name:%s "
							"agent->user_id:%d, agent->user_name:%s slot->pgoptions:%s agent->pgoptions:%s, backend_pid:%d",
					userPool->user_id, userPool->user_name, slot->user_id, slot->user_name, agent->user_id,
					agent->user_name, slot->pgoptions, agent->pgoptions, slot->backend_pid);
		}

		if (slot->user_id != agent->user_id || !slot->pgoptions || strcmp(slot->pgoptions, agent->pgoptions) != 0)
		{
			Assert(PoolerStatelessReuse);
			slot->set4reuse = true;
		}
		else
		{
			Assert(slot->userPool == userPool);
		}
		PgxcNodeUpdateHealth(node, true);
	}

	/* prebuild connection before next acquire */
	nodePool = grow_pool(dbPool, nodeidx, node, bCoord);
	IncreaseSlotRefCount(slot,__FILE__,__LINE__);
	return slot;
}


/*
 * release connection from specified pool and slot
 */
static void
release_connection(PoolAgent *agent, DatabasePool *dbPool, PGXCNodePoolSlot *slot,
				   int32 nodeidx, Oid node, bool force_destroy, bool bCoord)
{
	PGXCNodePool *nodePool;
	PGXCUserPool *userPool;
	time_t        now;

	Assert(dbPool);
	Assert(slot);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node, HASH_FIND, NULL);
	if (nodePool == NULL)
	{
		/*
		 * The node may be altered or dropped.
		 * In any case the slot is no longer valid.
		 */

		elog(WARNING, POOL_MGR_PREFIX"release_connection connection to nodeidx:%d, backend_pid:%d, can not find nodepool, just destory it", nodeidx, slot->backend_pid);
		destroy_slot(nodeidx, node, slot);
		return;
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"release_connection connection to nodename:%s backend_pid:%d user_id:%d nodeidx:%d size:%d freeSize:%d slot_seq:%d begin to release",
			 nodePoolNodeName(nodePool), slot->backend_pid, slot->user_id, nodeidx, nodePool->size, nodePool->freeSize, slot->seqnum);
	}

	/* force destroy the connection when pool not enabled */
	if (!force_destroy && !dbPool->bneed_pool)
	{
		force_destroy = true;
	}

	/* destory the slot of former nodePool */
	if (slot->m_version != nodePool->m_version)
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"release_connection connection to node:%s backend_pid:%d nodeidx:%d agentCount:%d size:%d freeSize:%d node version:%d slot version:%d not match", nodePoolNodeName(nodePool), slot->backend_pid, nodeidx, agentCount, nodePool->size, nodePool->freeSize, nodePool->m_version, slot->m_version);
		}

		userPool = slot->userPool;
		destroy_slot(nodeidx, node, slot);

		DecreaseUserPoolerSize(userPool,__FILE__,__LINE__);
		return;
	}

	if (isSlotNeedPreProcessing(slot))
	{
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"++++slot need prepare processing when release_connection pid:%d nodeid:%d backend_pid:%d, need_move_to_usedpool %d, set4reuse %d++++", agent->pid, node, slot->backend_pid, slot->need_move_to_usedpool, slot->set4reuse);
		}

		pre_processing_slot_in_sync_response_queue(agent, slot, nodeidx, true);
	}

	if (!force_destroy)
	{
		now = time(NULL);
		if (PoolConnMaxLifetime > 0 && difftime(now, slot->created) >= PoolConnMaxLifetime)
		{
			force_destroy = true;
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"connection to node:%s backend_pid:%d nodeidx:%d lifetime expired, closed it, size:%d freeSize:%d, nquery:%d", nodePoolNodeName(nodePool), slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize, nodePool->nquery);
			}
		}
	}

	/* return or discard */
	if (!force_destroy)
	{
		if ((difftime(now, slot->checked) >=  PoolSizeCheckGap) && !IS_ASYNC_PIPE_FULL())
		{
			/* increase the warm connection count */
			DecreaseSlotRefCount(slot,__FILE__,__LINE__);
			if (slot->refcount == 0)
			{
				nodePool->nquery++;
				pooler_async_query_connection(dbPool, slot, nodeidx, node);
				if (!nodePool->asyncInProgress && dbPool->bneed_pool)
					grow_pool(dbPool, nodeidx, node, bCoord);
			}
		}
		else
		{
			/* Insert the slot into the array and increase pool free size */
			DecreaseSlotRefCount(slot,__FILE__,__LINE__);
			if (slot->refcount == 0)
			{
				userPool = slot->userPool;
				slot->pid = -1;
				userPool->slot[userPool->freeSize] = slot;
				IncreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);
				slot->released = now;
				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"release_connection return connection to node:%s backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d", nodePoolNodeName(nodePool), slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize);
				}
			}
		}
	}
	else
	{
		DecreaseSlotRefCount(slot,__FILE__,__LINE__);
		if (slot->refcount == 0)
		{
			userPool = slot->userPool;
			elog(DEBUG1, POOL_MGR_PREFIX"Cleaning up connection from pool %s, closing", userPool->connstr);
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"release_connection destory connection to node:%s backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d", nodePoolNodeName(nodePool), slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize);
			}
			destroy_slot(nodeidx, node, slot);

			DecreaseUserPoolerSize(userPool,__FILE__,__LINE__);

			/* Ensure we are not below minimum size, here we don't need sync build */
			/* only grow pool when pool needed. */
			if (!nodePool->asyncInProgress && dbPool->bneed_pool)
			{
				grow_pool(dbPool, nodeidx, node, bCoord);
			}
		}
	}
}

static inline void
init_node_pool(DatabasePool *dbPool, PGXCNodePool *nodePool, Oid node, bool bCoord)
{
	HASHCTL			hinfo;
	int				hflags;
	char			*name_str;
	char            hash_name[NODE_POOL_NAME_LEN];
	PGXCUserPool	*userPool;
	PGXCNodeSingletonInfo *nodeInfo;
	bool			found;
	MemoryContext oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

	nodeInfo = (PGXCNodeSingletonInfo *) hash_search(g_NodeSingletonInfo, &nodePool->nodeoid, HASH_ENTER,
																&found);
	if (!found)
	{
		nodeInfo->coord     = bCoord;
		nodeInfo->freeSize  = 0;
		nodeInfo->size      = 0;
		nodeInfo->ref_count = 1;

		name_str = get_node_name_by_nodeoid(node);
		if (NULL == name_str)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg(POOL_MGR_PREFIX"get node %u name failed",
								   node)));
		}
		snprintf(nodeInfo->node_name, NAMEDATALEN, "%s", name_str);
	}
	else
		++nodeInfo->ref_count;
	nodePool->nodeInfo = nodeInfo;
	nodePool->connstr = build_node_conn_str(node, dbPool, nodePool->nodeInfo);
	if (!nodePool->connstr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
					errmsg(POOL_MGR_PREFIX"could not build connection "
						"string for node %u", node)));
	}

	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCUserPool);
	hflags |= HASH_ELEM;

	hinfo.hcxt = dbPool->mcxt;
	hflags |= HASH_CONTEXT;

	hinfo.hash = oid_hash;
	hflags |= HASH_FUNCTION;

	snprintf(hash_name, NODE_POOL_NAME_LEN, "%s_%d_User_Pool", dbPool->database, node);

	nodePool->userPools = hash_create(hash_name, 1024, &hinfo, hflags);
	userPool = (PGXCUserPool *) hash_search(nodePool->userPools, &dbPool->user_id, HASH_ENTER,
											&found);
	if (!found)
	{
		userPool->slot = (PGXCNodePoolSlot **) palloc0(MaxPoolSize * sizeof(PGXCNodePoolSlot *));
		if (!userPool->slot)
		{
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg(POOL_MGR_PREFIX"out of memory")));
		}
		userPool->user_id   = dbPool->user_id;
		userPool->user_name = pstrdup(dbPool->user_name);
		userPool->pgoptions = pstrdup(dbPool->pgoptions);
		userPool->connstr   = build_node_conn_str_by_user(nodePool->nodeInfo, dbPool, userPool->user_name, userPool->pgoptions, NULL);
		userPool->freeSize  = 0;
		userPool->size      = 0;
		userPool->refcount  = 0;
		userPool->nodePool  = nodePool;
		userPool->next = NULL;
		userPool->prev = NULL;
		nodePool->userPoolListHead = userPool;
		nodePool->userPoolListTail = userPool;

		elog(LOG, POOL_MGR_PREFIX"init_node_pool create new userPool(%p) user_id:%d, user_name:%s, connstr:%s", userPool, userPool->user_id, userPool->user_name, userPool->connstr);
	}
	else
	{
		Assert(0);
	}

	nodePool->freeSize = 0;
	nodePool->size     = 0;
	nodePool->nquery   = 0;
	/* increase the node pool version */
	nodePool->m_version = dbPool->version++;
	nodePool->asyncInProgress = false;
	MemoryContextSwitchTo(oldcontext);
}

/*
 * Increase database pool size, create new if does not exist
 */
static PGXCNodePool *
grow_pool(DatabasePool *dbPool, int32 nodeidx, Oid node, bool bCoord)
{
	PGXCNodePool   *nodePool;
	bool			found;

	Assert(dbPool);

	nodePool = (PGXCNodePool *) hash_search(dbPool->nodePools, &node,
											HASH_ENTER, &found);

	if (!found)
		init_node_pool(dbPool, nodePool, node, bCoord);

	/* here, we move the connection build work to async threads */
	if (!nodePool->asyncInProgress && dbPool->bneed_pool)
	{
		/* async build connection to other nodes, at least keep MinFreeSizePerDb free connection in the pool */
		if (nodePool->freeSize < MinFreeSizePerDb && nodePoolTotalSize(nodePool) < MaxPoolSize)
		{
			/* total pool size CAN NOT be larger than agentCount too much, to avoid occupying idle connection slot of datanode */
			//if (nodePool->size < agentCount + MinFreeSizePerDb)
			{
				int32 size = nodePool->freeSize < MinFreeSizePerDb ? MinFreeSizePerDb - nodePool->freeSize : 0;

				if (size)
				{
					if (pooler_async_build_connection(dbPool, nodePool, nodeidx, node, size, bCoord))
					{
						nodePool->asyncInProgress = true;
					}
				}
			}
		}
	}

	if (nodePoolTotalSize(nodePool) >= MaxPoolSize)
	{
		elog(LOG, POOL_MGR_PREFIX"pool %s Size:%d exceed MaxPoolSize:%d",
			 nodePoolNodeName(nodePool),
			 nodePoolTotalSize(nodePool),
			 MaxPoolSize);
	}
	return nodePool;
}


/*
 * Destroy pool slot, including slot itself.
 */
static void
destroy_slot_ex(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot, char *file, int32 line)
{
	int32  				threadid = 0;
	uint64              pipeput_loops = 0;
	PGXCPoolConnectReq *connReq;
	MemoryContext		oldcontext;

	struct pg_conn*  tmp_conn = NULL;

	if (!slot)
	{
		return;
	}

	if (PoolConnectDebugPrint)
	{
		/* should never happened */
		if (slot->bdestoryed)
		{
			abort();
		}
	}

	/* record last time destory position */
	slot->file   = file;
	slot->lineno = line;

	if (slot->conn == NULL || slot->xc_cancelConn == NULL)
	{
		elog(LOG, POOL_MGR_PREFIX"destroy_slot invalid slot status, null pointer conn:%p xc_cancelConn:%p slot_seq:%d", slot->conn, slot->xc_cancelConn, slot->seqnum);
		if (slot->conn == NULL && slot->xc_cancelConn == NULL)
		{
			/* conns are all NULL, no need async cancel,just free memory */
			slot->bdestoryed = true;
			if (slot->pgoptions != NULL)
				pfree(slot->pgoptions);
			pfree(slot);
			return;
		}
	}

	/* if no free pipe line avaliable, just do it sync */
	threadid = pooler_async_task_pick_thread(&g_PoolConnControl, nodeidx);
	if (-1 == threadid)
	{
		elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex no pipeline avaliable, sync close connection node:%u nodeidx:%d usecount:%d slot_seq:%d", node, nodeidx, slot->usecount, slot->seqnum);
		PQfreeCancel((PGcancel *)slot->xc_cancelConn);
		PGXCNodeClose(slot->conn);
		slot->bdestoryed = true;
		if (slot->pgoptions)
			pfree(slot->pgoptions);
		pfree(slot);
		return;
	}

	oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
	connReq            = (PGXCPoolConnectReq*)palloc0(sizeof(PGXCPoolConnectReq));
	MemoryContextSwitchTo(oldcontext);

	connReq->cmd       = COMMAND_CONNECTION_CLOSE;
	connReq->nodeoid   = node;
	connReq->validSize = 0;
	connReq->nodeindex = nodeidx;
	connReq->slot[0].xc_cancelConn = slot->xc_cancelConn;
	connReq->slot[0].conn          = slot->conn;
	connReq->slot[0].seqnum        = slot->seqnum;
	connReq->slot[0].bdestoryed    = slot->bdestoryed;
	connReq->slot[0].file          = slot->file;
	connReq->slot[0].lineno        = slot->lineno;

	if (PoolConnectDebugPrint && slot->conn != NULL)
	{
		tmp_conn = (struct pg_conn*)slot->conn;
		elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex close conn, file:%s line:%d refcount:%d fd:%d remote backendid:%d remoteip:%s %s pgport:%s dbname:%s dbuser:%s",
				slot->file, slot->lineno, slot->refcount, tmp_conn->sock,
				tmp_conn->be_pid, tmp_conn->pghost, tmp_conn->pghostaddr, tmp_conn->pgport,
				tmp_conn->dbName, tmp_conn->pguser);
	}

	while (-1 == PipePut(g_PoolConnControl.request[threadid % MAX_SYNC_NETWORK_THREAD], (void*)connReq))
	{
		pipeput_loops++;
	}
	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex fail to async close connection node:%u for loops %lu", node, pipeput_loops);
	}
	/* signal thread to start build job */
	ThreadSemaUp(&g_PoolConnControl.sem[threadid % MAX_SYNC_NETWORK_THREAD]);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"destroy_slot_ex async close connection node:%u nodeidx:%d threadid:%d usecount:%d slot_seq:%d", node, nodeidx, threadid, slot->usecount, slot->seqnum);
	}

	/* set destroy flag */
	slot->bdestoryed = true;
	if (slot->pgoptions)
		pfree(slot->pgoptions);
	pfree(slot);
}


/*
 * Close pool slot, don't free the slot itself.
 */
static void
close_slot(int32 nodeidx, Oid node, PGXCNodePoolSlot *slot)
{
	int32 threadid;
	uint64 pipeput_loops = 0;
	PGXCPoolConnectReq *connReq;
	if (!slot)
	{
		return;
	}

	if (PoolConnectDebugPrint)
	{
		if (slot->bdestoryed)
		{
			abort();
		}
	}

	if (!slot->conn || !slot->xc_cancelConn)
	{
		elog(LOG, POOL_MGR_PREFIX"close_slot invalid slot status, null pointer conn:%p xc_cancelConn:%p", slot->conn, slot->xc_cancelConn);
	}

	/* if no free pipe line avaliable, just do it sync */
	threadid = pooler_async_task_pick_thread(&g_PoolConnControl, nodeidx);
	if (-1 == threadid)
	{
		elog(LOG, POOL_MGR_PREFIX"no pipeline avaliable, sync close connection node:%u nodeidx:%d usecount:%d slot_seq:%d", node, nodeidx, slot->usecount, slot->seqnum);
		PQfreeCancel((PGcancel *)slot->xc_cancelConn);
		PGXCNodeClose(slot->conn);
		return;
	}


	connReq            = (PGXCPoolConnectReq*)palloc0(sizeof(PGXCPoolConnectReq));
	connReq->cmd       = COMMAND_CONNECTION_CLOSE;
	connReq->nodeoid   = node;
	connReq->validSize = 0;
	connReq->slot[0].xc_cancelConn = slot->xc_cancelConn;
	connReq->slot[0].conn          = slot->conn;
	while (-1 == PipePut(g_PoolConnControl.request[threadid % MAX_SYNC_NETWORK_THREAD], (void*)connReq))
	{
		pipeput_loops++;
	}
	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async close connection node:%u for loops %lu", node, pipeput_loops);
	}
	/* signal thread to start build job */
	ThreadSemaUp(&g_PoolConnControl.sem[threadid % MAX_SYNC_NETWORK_THREAD]);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"async close connection node:%u nodeidx:%d threadid:%d usecount:%d slot_seq:%d", node, nodeidx, threadid, slot->usecount, slot->seqnum);
	}
	/* set destroy flag */
	slot->bdestoryed = true;
}


/*
 * Destroy node pool
 */
static void
destroy_node_pool(PGXCNodePool *node_pool)
{
	int			i;
	PGXCUserPool *userPool;
	int   reducedFreeSize = 0;

	if (!node_pool)
	{
		return;
	}

	/*
	 * At this point all agents using connections from this pool should be already closed
	 * If this not the connections to the Datanodes assigned to them remain open, this will
	 * consume Datanode resources.
	 */
	elog(DEBUG1, POOL_MGR_PREFIX"About to destroy node pool %s node_pool %p, current size is %d, %d connections are in use",
		 node_pool->connstr, node_pool, node_pool->freeSize, node_pool->size - node_pool->freeSize);
	if (node_pool->connstr)
	{
		pfree(node_pool->connstr);
	}

	if (!node_pool->userPoolListHead)
		return;

	userPool = node_pool->userPoolListHead;
	while (userPool)
	{
		Oid user_id;
		if (userPool->slot)
		{
			int32 nodeidx;
			Assert(userPool->freeSize == userPool->size);
			nodeidx = get_node_index_by_nodeoid(node_pool->nodeoid);
			for (i = 0; i < userPool->freeSize; i++)
			{
				destroy_slot(nodeidx, node_pool->nodeoid, userPool->slot[i]);
			}
			reducedFreeSize += userPool->freeSize;
		}

		elog(DEBUG1, POOL_MGR_PREFIX"remove node:%d user pool user_id:%d, user_name:%s, current size is %d, %d connections are in use",
			 node_pool->nodeoid, userPool->user_id, userPool->user_name, userPool->freeSize, userPool->size - userPool->freeSize);

		pfree(userPool->user_name);
		pfree(userPool->pgoptions);
		pfree(userPool->connstr);
		pfree(userPool->slot);

		user_id  = userPool->user_id;
		userPool = userPool->next;

		hash_search(node_pool->userPools, &user_id, HASH_REMOVE, NULL);
	}

	if (node_pool->userPools != NULL)
		hash_destroy(node_pool->userPools);
	Assert(reducedFreeSize == node_pool->freeSize);
	nodePoolTotalSize(node_pool) -= node_pool->size;
	nodePoolTotalFreeSize(node_pool) -= node_pool->freeSize;
}

/*
 * Destroy free slot of the node pool
 */
static bool
destroy_node_pool_free_slots(PGXCNodePool *node_pool, DatabasePool *db_pool, const char *user_name, bool rebuild_connstr)
{
	int	  i;
	int   reducedSize     = 0;
	bool  has_conn_in_use = false;
	int32 nodeidx;
	PGXCUserPool *userPool;

	if (!node_pool)
	{
		return has_conn_in_use;
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"About to destroy slots of node pool %s, agentCount is %d, node_pool version:%d current size is %d, freeSize is %d, %d connections are in use, user_name %s",
			 node_pool->connstr, node_pool->m_version, agentCount, node_pool->size, node_pool->freeSize, node_pool->size - node_pool->freeSize, user_name);
	}

	for (userPool = node_pool->userPoolListHead; userPool; userPool = userPool->next)
	{
		if (user_name && strcmp(user_name, userPool->user_name) != 0)
			continue;

		if (userPool->slot)
		{
			/* Check if connections are in use */
			if (userPool->freeSize < userPool->size)
			{
				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"Pool of Database %s is using Datanode %u connections, %p, freeSize:%d, size:%d",
						 db_pool->database, node_pool->nodeoid, userPool, userPool->freeSize, userPool->size);
				}

				has_conn_in_use = true;
			}

			nodeidx = get_node_index_by_nodeoid(node_pool->nodeoid);
			for (i = 0; i < userPool->freeSize; i++)
			{
				destroy_slot(nodeidx, node_pool->nodeoid, userPool->slot[i]);
				userPool->slot[i] = NULL;
			}

			reducedSize += userPool->freeSize;
			userPool->size -= userPool->freeSize;
			userPool->freeSize = 0;
		}

		if (rebuild_connstr)
		{
			if (userPool->connstr)
				pfree(userPool->connstr);
			userPool->connstr = build_node_conn_str_by_user(node_pool->nodeInfo, db_pool, userPool->user_name, userPool->pgoptions, NULL);
			node_pool->m_version = db_pool->version++;
		}
	}

	node_pool->size -= reducedSize;
	node_pool->freeSize -= reducedSize;

	nodePoolTotalSize(node_pool) -= reducedSize;
	nodePoolTotalFreeSize(node_pool) -= reducedSize;

	return has_conn_in_use;
}

/*
 * setup current log time
 */
static void
setup_formatted_current_log_time(char* formatted_current_log_time)
{
    pg_time_t	stamp_time;
    char		msbuf[13];
    struct timeval timeval;

    gettimeofday(&timeval, NULL);
    stamp_time = (pg_time_t) timeval.tv_sec;

    /*
     * Note: we expect that guc.c will ensure that log_timezone is set up (at
     * least with a minimal GMT value) before Log_line_prefix can become
     * nonempty or CSV mode can be selected.
     */
    pg_strftime(formatted_current_log_time, FORMATTED_TS_LEN,
            /* leave room for milliseconds... */
                "%Y-%m-%d %H:%M:%S     %Z",
                pg_localtime(&stamp_time, log_timezone));

    /* 'paste' milliseconds into place... */
    sprintf(msbuf, ".%03d", (int) (timeval.tv_usec / 1000));
    memcpy(formatted_current_log_time + 19, msbuf, 4);
}

/*
 * write pooler's subthread log into thread log queue
 * only call by pooler's subthread in elog
 */
static void
pooler_subthread_write_log(int elevel, int lineno, const char *filename, const char *funcname, const char *fmt, ...)
{
    char *buf = NULL;
    int buf_len = 0;
    int offset = 0;
    char formatted_current_log_time[FORMATTED_TS_LEN];

    if (!PoolSubThreadLogPrint)
    {
        /* not enable sun thread log print, return */
        return;
    }

    if (PipeIsFull(g_ThreadLogQueue))
    {
        return;
    }

    /* use malloc in sub thread */
    buf_len = strlen(filename) + strlen(funcname) + DEFAULT_LOG_BUF_LEN;
    buf = (char*)malloc(buf_len);
    if (buf == NULL)
    {
        /* no log */
        return;
    }

    /* construction log, format: elevel | lineno | filename | funcname | log content */
    *(int*)(buf + offset) = elevel;
    offset += sizeof(elevel);
    *(int*)(buf + offset) = lineno;
    offset += sizeof(lineno);
    memcpy(buf + offset, filename, strlen(filename) + 1);
    offset += (strlen(filename) + 1);
    memcpy(buf + offset, funcname, strlen(funcname) + 1);
    offset += (strlen(funcname) + 1);

    /*
     * because the main thread writes the log of the sub thread asynchronously,
     * record the actual log writing time here
     */
    setup_formatted_current_log_time(formatted_current_log_time);
    memcpy(buf + offset, formatted_current_log_time, strlen(formatted_current_log_time));
    offset += strlen(formatted_current_log_time);
    *(char*)(buf + offset) = ' ';
    offset += sizeof(char);

    /* Generate actual output --- have to use appendStringInfoVA */
    for (;;)
    {
        va_list		args;
        int			avail;
        int			nprinted;

        avail = buf_len - offset - 1;
        va_start(args, fmt);
        nprinted = vsnprintf(buf + offset, avail, fmt, args);
        va_end(args);
        if (nprinted >= 0 && nprinted < avail - 1)
        {
            offset += nprinted;
            *(char*)(buf + offset) = '\0';
			offset += sizeof(char);
            break;
        }

        buf_len = (buf_len * 2 > (int) MaxAllocSize) ? MaxAllocSize : buf_len * 2;
        buf = (char *) realloc(buf, buf_len);
        if (buf == NULL)
        {
            /* no log */
            return;
        }
    }

    /* put log into thread log queue, drop log if queue is full */
    if (-1 == PipePut(g_ThreadLogQueue, buf))
    {
        free(buf);
    }
}

/*
 * write subthread log in main thread
 */
static void
pooler_handle_subthread_log(bool is_pooler_exit)
{
    int write_log_cnt = 0;
    int offset = 0;
    int elevel = LOG;
    int lineno = 0;
    char *log_buf = NULL;
    char *filename = NULL;
    char *funcname = NULL;
    char *log_content = NULL;

    while ((log_buf = (char*)PipeGet(g_ThreadLogQueue)) != NULL)
    {
        /* elevel | lineno | filename | funcname | log content */
        elevel = *(int*)log_buf;
        offset = sizeof(elevel);
        lineno = *(int*)(log_buf + offset);
        offset += sizeof(lineno);
        filename = log_buf + offset;
        offset += (strlen(filename) + 1);
        funcname = log_buf + offset;
        offset += (strlen(funcname) + 1);
        log_content = log_buf + offset;

        /* write log here */
        elog_start(filename, lineno,
#ifdef USE_MODULE_MSGIDS
                PGXL_MSG_MODULE, PGXL_MSG_FILEID, __COUNTER__,
#endif
                funcname);
        elog_finish(elevel, "%s", log_content);

        free(log_buf);

        /*
         * if the number of logs written at one time exceeds POOLER_WRITE_LOG_ONCE_LIMIT,
         * in order not to block the main thread, return here
         */
        if (write_log_cnt++ >= POOLER_WRITE_LOG_ONCE_LIMIT && !is_pooler_exit)
        {
            return;
        }
    }
}

/*
 * Create a hash table of node singleton info
 */
static HTAB *
create_node_singleton_info_hashtable()
{
	HASHCTL  hinfo;
	int		 hflags;

	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCNodeSingletonInfo);
	hflags |= HASH_ELEM;

	hinfo.hcxt = PoolerMemoryContext;
	hflags |= HASH_CONTEXT;

	hinfo.hash = oid_hash;
	hflags |= HASH_FUNCTION;

	return hash_create("singleton nodeInfo", 1024, &hinfo, hflags);
}

/*
 * Main handling loop
 */
static void
PoolerLoop(void)
{
	StringInfoData input_message;
	int            maxfd       = MaxConnections + 1024;
	int 		   i;
	int            ret;
	time_t		   last_maintenance = (time_t) 0;
	int			   timeout_val      = 0;
	PGXCPoolConnThreadParam 	connParam[MAX_SYNC_NETWORK_THREAD];
	struct epoll_event	event;
	struct epoll_event *events;

#ifdef HAVE_UNIX_SOCKETS
	if (Unix_socket_directories)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int 		success = 0;

		/* Need a modifiable copy of Unix_socket_directories */
		rawstring = pstrdup(Unix_socket_directories);

		/* Parse string into list of directories */
		if (!SplitDirectoriesString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"unix_socket_directories")));
		}

		foreach(l, elemlist)
		{
			char	   *socketdir = (char *) lfirst(l);
			int 		saved_errno;

			/* Connect to the pooler */
			server_fd = pool_listen(PoolerPort, socketdir);
			if (server_fd < 0)
			{
				saved_errno = errno;
				ereport(WARNING,
						(errmsg("could not create Unix-domain socket in directory \"%s\", errno %d, server_fd %d",
								socketdir, saved_errno, server_fd)));
			}
			else
			{
				success++;
			}
		}

		if (!success && elemlist != NIL)
			ereport(ERROR,
					(errmsg("failed to start listening on Unix-domain socket for pooler: %m")));

		list_free_deep(elemlist);
		pfree(rawstring);
	}
#endif

	g_NodeSingletonInfo = create_node_singleton_info_hashtable();

    /* create log queue */
    g_ThreadLogQueue = CreatePipe(MAX_THREAD_LOG_PIPE_LEN);

	/* create utility thread */
	g_AsynUtilityPipeSender = CreatePipe(POOL_ASYN_WARM_PIPE_LEN);
	ThreadSemaInit(&g_AsnyUtilitysem, 0, false);
	g_AsynUtilityPipeRcver  = CreatePipe(POOL_ASYN_WARM_PIPE_LEN);
	CreateThread(pooler_async_utility_thread, NULL, MT_THR_DETACHED, &ret);
	if (ret)
	{
		elog(ERROR, POOL_MGR_PREFIX"Pooler:create utility thread failed");
	}

	/* Create concurrent connection pipes for build batch connection request and close connection request */
	pooler_init_sync_control(&g_PoolConnControl);
	for (i = 0; i < MAX_SYNC_NETWORK_THREAD; i++)
	{
		g_PoolConnControl.request[i]  = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
		g_PoolConnControl.response[i] = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
		ThreadSemaInit(&g_PoolConnControl.sem[i], 0, false);
		connParam[i].threadIndex = i;
		CreateThread(pooler_async_connection_management_thread, (void*)&connParam[i], MT_THR_DETACHED, &ret);
		if (ret)
		{
			elog(ERROR, POOL_MGR_PREFIX"Pooler:create connection manage thread failed");
		}
	}


	/* Create sync network operation pipes for remote nodes */
	pooler_init_sync_control(&g_PoolSyncNetworkControl);
	for (i = 0; i < MAX_SYNC_NETWORK_THREAD; i++)
	{
		g_PoolSyncNetworkControl.request[i]  = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
		g_PoolSyncNetworkControl.response[i] = CreatePipe(MAX_SYNC_NETWORK_PIPE_LEN);
		ThreadSemaInit(&g_PoolSyncNetworkControl.sem[i], 0, false);
		connParam[i].threadIndex = i;
		CreateThread(pooler_sync_remote_operator_thread, (void*)&connParam[i], MT_THR_DETACHED, &ret);
		if (ret)
		{
			elog(ERROR, POOL_MGR_PREFIX"Pooler:create connection manage thread failed");
		}
	}

	if (server_fd == -1)
	{
		/* log error */
		return;
	}

	elog(LOG, POOL_MGR_PREFIX"PoolerLoop begin server_fd:%d", server_fd);

	initStringInfo(&input_message);

	g_epollfd = epoll_create1(0);
	if (g_epollfd == -1)
	{
		elog(ERROR, POOL_MGR_PREFIX"Pooler failed to create pooler epoll");
	}

	event.data.fd = server_fd;
	event.events  = EPOLLIN | EPOLLERR | EPOLLHUP;
	events        = palloc0(maxfd * sizeof(struct epoll_event));

	if(epoll_ctl(g_epollfd, EPOLL_CTL_ADD, server_fd, &event) == -1)
	{
		close(server_fd);
		close(g_epollfd);
		elog(ERROR, POOL_MGR_PREFIX"Pooler:add server fd to epoll failed, %m");
	}

	reset_pooler_statistics();

	for (;;)
	{
		int retval;
		int i;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
		{
            pooler_handle_subthread_log(true);
			exit(1);
		}

		/* watch for incoming messages */
		RebuildAgentIndex();

		if (shutdown_requested)
		{
			/*
			 *  Just close the socket and exit. Linux will help to release the resouces.
             */
			close(server_fd);
            pooler_handle_subthread_log(true);
			exit(0);
		}

		if (PoolMaintenanceTimeout > 0)
		{
			double			timediff;

			/*
			 * Decide the timeout value based on when the last
			 * maintenance activity was carried out. If the last
			 * maintenance was done quite a while ago schedule the select
			 * with no timeout. It will serve any incoming activity
			 * and if there's none it will cause the maintenance
			 * to be scheduled as soon as possible
			 */
			timediff = difftime(time(NULL), last_maintenance);
			if (timediff > PoolMaintenanceTimeout)
			{
				timeout_val = 0;
			}
			else
			{
				timeout_val = PoolMaintenanceTimeout - rint(timediff);
			}

			/* wait for event */
			retval = epoll_wait(g_epollfd, events, maxfd, timeout_val * 1000);
		}
		else
		{
			retval = epoll_wait(g_epollfd, events, maxfd, -1);
		}

		if (retval < 0)
		{

			if (errno == EINTR)
				continue;
			elog(FATAL, POOL_MGR_PREFIX"poll returned with error %d", retval);

		}

		/* add warmed connection to node pool before agent acquiring new connection */
		pooler_sync_connections_to_nodepool();
		pooler_handle_sync_response_queue();
		if (retval > 0)
		{
			for (i = 0; i < retval; i++)
			{
				if (!(events[i].events & EPOLLIN))
				{
					continue;
				}

				if (events[i].data.fd == server_fd)
				{
					int new_fd = accept(server_fd, NULL, NULL);

					if (new_fd < 0)
					{
						int saved_errno = errno;
						ereport(LOG,
								(errcode(ERRCODE_CONNECTION_FAILURE), errmsg(POOL_MGR_PREFIX"Pooler manager failed to accept connection: %m")));
						errno = saved_errno;
					}
					else
					{
						struct epoll_event event2;
						PoolAgent *poolAgent = agent_create(new_fd);
						event2.data.ptr      = poolAgent;
						event2.events        = EPOLLIN | EPOLLERR | EPOLLHUP;

						if (epoll_ctl(g_epollfd, EPOLL_CTL_ADD, new_fd, &event2) == -1)
						{
							close(new_fd);
							close(g_epollfd);
							elog(ERROR, POOL_MGR_PREFIX"Pooler:add new fd %d to epoll failed, %m", new_fd);
						}
					}
				}
				else
				{
					PoolAgent *agent = (PoolAgent *) events[i].data.ptr;

					/* If there is still async request, do not handle agent input */
					if (agent->ref_count == 0)
						agent_handle_input(agent, &input_message);
				}
			}
		}

		/* maintaince time out */
		if (0 == timeout_val && PoolMaintenanceTimeout > 0)
		{
			/* maintain the connection pool */
			pools_maintenance();
			PoolAsyncPingNodes();
			last_maintenance = time(NULL);
		}

		print_pooler_statistics();

		/* handle sub thread's log */
        pooler_handle_subthread_log(false);
	}
}


/*
 * Clean Connection in all Database Pools for given Datanode and Coordinator list
 */
int
clean_connection(List *node_discard, const char *database, const char *user_name)
{
	DatabasePool *databasePool;
	int			res = CLEAN_CONNECTION_COMPLETED;

	databasePool = databasePoolsHead;

	while (databasePool)
	{
		ListCell *lc;

		if ((database && strcmp(database, databasePool->database)) ||
				(!PoolerStatelessReuse && user_name && strcmp(user_name, databasePool->user_name)))
		{
			/* The pool does not match to request, skip */
			databasePool = databasePool->next;
			continue;
		}

		/*
		 * Clean each requested node pool
		 */
		foreach(lc, node_discard)
		{
			PGXCNodePool *nodePool;
			Oid node = lfirst_oid(lc);

			nodePool = hash_search(databasePool->nodePools, &node, HASH_FIND,
								   NULL);

			if (nodePool)
			{
				/* Destroy connections currently in Node Pool */
				if (destroy_node_pool_free_slots(nodePool, databasePool, user_name, false))
				{
					elog(WARNING, POOL_MGR_PREFIX"Pool of Database %s is using Datanode %u connections, freeSize:%d, size:%d",
						 databasePool->database, node, nodePool->freeSize, nodePool->size);
					res = CLEAN_CONNECTION_NOT_COMPLETED;
				}
			}
		}

		databasePool = databasePool->next;
	}

	return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
int *
abort_pids(int *len, int pid, const char *database, const char *user_name)
{
	int   *pids = NULL;
	int   i = 0;
	int   count;
	int32 index;

	Assert(!is_pool_locked);
	Assert(agentCount > 0);

	RebuildAgentIndex();

	pids = (int *) palloc((agentCount - 1) * sizeof(int));

	/* Send a SIGTERM signal to all processes of Pooler agents except this one */
	for (count = 0; count < agentCount; count++)
	{
		index = agentIndexes[count];
		/* The agent may have just been created without initialization */
		if (poolAgents[index]->pid == 0 || poolAgents[index]->pid == pid)
			continue;

		if (database && strcmp(poolAgents[index]->pool->database, database) != 0)
			continue;

		if (user_name && strcmp(poolAgents[index]->user_name, user_name) != 0)
			continue;

		if (kill(poolAgents[index]->pid, SIGTERM) < 0)
			elog(ERROR, POOL_MGR_PREFIX"kill(%ld,%d) failed: %m",
						(long) poolAgents[index]->pid, SIGTERM);

		pids[i++] = poolAgents[index]->pid;
	}

	*len = i;

	return pids;
}

/*
 *
 */
static void
pooler_die(SIGNAL_ARGS)
{
	shutdown_requested = true;
}


static void
pooler_sig_hup_handler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/*
 *
 */
static void
pooler_quickdie(SIGNAL_ARGS)
{
	sigaddset(&BlockSig, SIGQUIT);	/* prevent nested calls */
	PG_SETMASK(&BlockSig);
	_exit(2);

}

bool
IsPoolHandle(void)
{
	return poolHandle != NULL;
}


/*
 * Given node identifier, dbname and user name build connection string.
 * Get node connection details from the shared memory node table
 */
static char *
build_node_conn_str(Oid node, DatabasePool *dbPool, PGXCNodeSingletonInfo *nodeInfo)
{
	NodeDefinition *nodeDef;
	char 		   *connstr;

	nodeDef = PgxcNodeGetDefinition(node);
	if (nodeDef == NULL)
	{
		/* No such definition, node is dropped? */
		return NULL;
	}

	/* refresh node ip and port */
	if (NULL != nodeInfo)
	{
		if (strcmp(nodeInfo->node_host, NameStr(nodeDef->nodehost)) != 0)
			StrNCpy(nodeInfo->node_host, NameStr(nodeDef->nodehost), NAMEDATALEN);
		if (nodeInfo->node_port != nodeDef->nodeport)
			nodeInfo->node_port = nodeDef->nodeport;
	}
	
	connstr = PGXCNodeConnStr(NameStr(nodeDef->nodehost),
							  nodeDef->nodeport,
							  dbPool->database,
							  dbPool->user_name,
							  dbPool->pgoptions,
							  IS_PGXC_COORDINATOR ? "coordinator" : "datanode",
							  PGXCNodeName,
							  NULL);
	pfree(nodeDef);

	return connstr;
}

static char *
build_node_conn_str_by_user(PGXCNodeSingletonInfo *nodeInfo, DatabasePool *dbPool, char *userName, char *pgoptions, char *out)
{
	return PGXCNodeConnStr(nodeInfo->node_host,
						   nodeInfo->node_port,
						   dbPool->database,
						   userName,
						   pgoptions,
						   IS_PGXC_COORDINATOR ? "coordinator" : "datanode",
						   PGXCNodeName,
						   out);
}

/*
 * Check all pooled connections, and close which have been released more then
 * PooledConnKeepAlive seconds ago.
 * Return true if shrink operation closed all the connections and pool can be
 * ddestroyed, false if there are still connections or pool is in use.
 */
static bool
shrink_pool(DatabasePool *pool)
{
	int32           freeCount = 0;
	time_t 			now = time(NULL);
	HASH_SEQ_STATUS hseq_status;
	PGXCNodePool   *nodePool;
	int 			i;
	int32 		    nodeidx;
	bool			empty = true;

	/* Negative PooledConnKeepAlive disables automatic connection cleanup */
	if (PoolConnKeepAlive < 0)
	{
		return false;
	}

	pool->oldest_idle = (time_t) 0;
	hash_seq_init(&hseq_status, pool->nodePools);
	while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
	{
		PGXCUserPool *userPool;

		/*
		 *	Go thru the free slots and destroy those that are free too long, free a long free connection.
		 *  Use MAX_FREE_CONNECTION_NUM to control the loop number.
		*/
		freeCount = 0;
		nodeidx = get_node_index_by_nodeoid(nodePool->nodeoid);

		/* Release from inactive userPool */
		for (userPool = nodePool->userPoolListTail; userPool;)
		{
			for (i = 0; i < userPool->freeSize && freeCount < MAX_FREE_CONNECTION_NUM && (nodePool->freeSize > MinFreeSizePerDb || nodePoolTotalFreeSize(nodePool) > MinFreeSize); )
			{
				PGXCNodePoolSlot *slot = userPool->slot[i];
				if (slot)
				{
					/* no need to shrik warmed slot, only discard them when they use too much memroy */
					if (!slot->bwarmed && ((difftime(now, slot->released) > PoolConnKeepAlive) ||
										(PoolConnMaxLifetime > 0 && difftime(now, slot->created) >= PoolConnMaxLifetime)))
					{
						if (PoolConnectDebugPrint)
						{
							elog(LOG, POOL_MGR_PREFIX"shrink_pool destroy a connection to node:%s backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d. "
									"reason release difftime:%f > %d, life time:%f > %d",
								 	nodePoolNodeName(nodePool), slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize,
									difftime(now, slot->released), PoolConnKeepAlive,
									difftime(now, slot->created), PoolConnMaxLifetime);
						}
						/* connection is idle for long, close it */
						destroy_slot(nodeidx, nodePool->nodeoid, slot);

						/* reduce pool size and total number of connections */
						DecreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);
						DecreaseUserPoolerSize(userPool,__FILE__,__LINE__);

						/* move last connection in place, if not at last already */
						if (i < userPool->freeSize)
						{
							userPool->slot[i] = userPool->slot[userPool->freeSize];
							userPool->slot[userPool->freeSize] = NULL;
						}
						else if (i == userPool->freeSize)
						{
							userPool->slot[userPool->freeSize] = NULL;
						}
						freeCount++;
					}
					/*
					 * if there is no memory check done when releasing the connection
					 * to pool, perform another check here and only need to do it once
					 * when connection is in the pool
					 */
					else if ((difftime(slot->released, slot->checked) > 0) &&
							 (difftime(now, slot->checked) >= PoolSizeCheckGap) &&
						      !IS_ASYNC_PIPE_FULL())
					{
						DecreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);

						/* move last connection in place, if not at last already */
						if (i < userPool->freeSize)
						{
							userPool->slot[i] = userPool->slot[userPool->freeSize];
							userPool->slot[userPool->freeSize] = NULL;
						}
						else if (i == userPool->freeSize)
						{
							userPool->slot[userPool->freeSize] = NULL;
						}

						nodePool->nquery++;
						pooler_async_query_connection(pool, slot, nodeidx, nodePool->nodeoid);
					}
					else
					{
						if (pool->oldest_idle == (time_t) 0 ||
								difftime(pool->oldest_idle, slot->released) > 0)
						{
							pool->oldest_idle = slot->released;
						}
						i++;
					}
				}
				else
				{
					elog(WARNING, POOL_MGR_PREFIX"invalid NULL index %d node:%u, poolsize:%d, freeSize:%d", i,
																							nodePool->nodeoid,
																							nodePool->size,
																							nodePool->freeSize);
					DecreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);
					DecreaseUserPoolerSize(userPool,__FILE__,__LINE__);
					if (i < userPool->freeSize)
					{
						userPool->slot[i] = userPool->slot[userPool->freeSize];
						userPool->slot[userPool->freeSize] = NULL;
					}
					else if (i == userPool->freeSize)
					{
						userPool->slot[userPool->freeSize] = NULL;
					}
				}
			}

			if (userPool->size == 0 && userPool->refcount == 0 && !nodePool->asyncInProgress &&
				userPool != nodePool->userPoolListHead && hash_get_num_entries(nodePool->userPools) > 5)
			{
				Oid user_id = userPool->user_id;

				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"remove unused userPool(%p) %s from nodePool %s database %s.", userPool, userPool->user_name, nodePoolNodeName(nodePool), pool->database);
				}

				if (userPool == nodePool->userPoolListTail)
				{
					if (userPool->prev)
						userPool->prev->next = NULL;
					nodePool->userPoolListTail = userPool->prev;
				}
				else
				{
					userPool->prev->next = userPool->next;
					userPool->next->prev = userPool->prev;
				}

				pfree(userPool->user_name);
				pfree(userPool->pgoptions);
				pfree(userPool->connstr);
				pfree(userPool->slot);

				userPool = userPool->prev;
				hash_search(nodePool->userPools, &user_id, HASH_REMOVE, NULL);
			}
			else
			{
				userPool = userPool->prev;
			}
		}

		if (freeCount)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"close %d long time free node:%u, poolsize:%d, freeSize:%d ", freeCount, nodePool->nodeoid,
																					   nodePool->size,
																					   nodePool->freeSize);
			}

			/* only grow pool when pool needed. */
			if (pool->bneed_pool && nodePool->freeSize < MinFreeSizePerDb && nodePoolTotalFreeSize(nodePool) < MinFreeSize)
			{
				grow_pool(pool, nodeidx, nodePool->nodeoid, isNodePoolCoord(nodePool));
			}
		}

		if (nodePool->size == 0 && !nodePool->asyncInProgress)
		{
			destroy_node_pool(nodePool);
			if ((--nodePool->nodeInfo->ref_count) == 0)
				hash_search(g_NodeSingletonInfo, &nodePool->nodeoid, HASH_REMOVE, NULL);
			hash_search(pool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
		}
		else
		{
			empty = false;
		}
	}

	/*
	 * Last check, if any active agent is referencing the pool do not allow to
	 * destroy it, because there will be a problem if session wakes up and try
	 * to get a connection from non existing pool.
	 * If all such sessions will eventually disconnect the pool will be
	 * destroyed during next maintenance procedure.
	 */
	if (empty)
	{
		int32 index = 0;
		RebuildAgentIndex();

		for (i = 0; i < agentCount; i++)
		{
			index = agentIndexes[i];
			if (poolAgents[index]->pool == pool)
			{
				return false;
			}
		}
	}

	return empty;
}

/*
 * Scan connection pools and release connections which are idle for long.
 * If pool gets empty after releasing connections it is destroyed.
 */
static void
pools_maintenance(void)
{
	bool            bresult = false;
	DatabasePool   *curr = databasePoolsTail;
	time_t			now = time(NULL);
	int				count = 0;


	/* Iterate over the pools */
	while (curr)
	{
		/*
		 * If current pool has connections to close and it is emptied after
		 * shrink remove the pool and free memory.
		 * Otherwithe move to next pool.
		 */
		bresult = shrink_pool(curr);
		if (bresult)
		{
			MemoryContext mem = curr->mcxt;

			if (databasePoolsTail == curr || databasePoolsHead == curr)
			{
				if (databasePoolsTail == curr)
				{
					if (curr->prev)
						curr->prev->next = NULL;
					databasePoolsTail = curr->prev;
				}

				if (databasePoolsHead == curr)
				{
					if (curr->next)
						curr->next->prev = NULL;
					databasePoolsHead = curr->next;
				}
			}
			else
			{
				curr->next->prev = curr->prev;
				curr->prev->next = curr->next;
			}

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"pools_maintenance, recyle dbpool %s", curr->database);
			}

			curr = curr->prev;
			MemoryContextDelete(mem);
			count++;
		}
		else
		{
			curr = curr->prev;
		}
	}
	elog(DEBUG1, POOL_MGR_PREFIX"Pool maintenance, done in %f seconds, removed %d pools",
			difftime(time(NULL), now), count);
}

/*
 * change slot to new user pool,
 * or update the pgoptions
 */
static void
change_slot_user_pool(PoolAgent *agent, PGXCUserPool *newUserPool, PGXCNodePoolSlot *slot)
{
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"change_slot_user_pool node:%s backend_pid:%d "
				  "change user_name %s to %s, user_id %d to %d",
			 userPoolNodeName(slot->userPool), slot->backend_pid, slot->user_name,
			 newUserPool->user_name, slot->user_id, newUserPool->user_id);
	}

	slot->set4reuse = false;
	DecreaseUserPoolRefCount(newUserPool, __FILE__, __LINE__);

	if (strcmp(slot->pgoptions, agent->pgoptions) != 0)
	{
		MemoryContext oldcontext;

		pfree(slot->pgoptions);
		oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
		slot->pgoptions = pstrdup(agent->pgoptions);
		MemoryContextSwitchTo(oldcontext);
	}

	if (slot->user_id != agent->user_id)
	{
		slot->user_id   = agent->user_id;
		slot->user_name = newUserPool->user_name;
		Assert(newUserPool->user_id == slot->user_id);

		/*
		 * increase new UserPoolerSize and decrease old UserPoolerSize,
		 * if not match version will release without update XXXXsize
		 * in release_connections
		 */
		if (newUserPool->nodePool->m_version == slot->m_version)
			IncreaseUserPoolerSize(newUserPool, __FILE__, __LINE__);
		if (slot->userPool->nodePool->m_version == slot->m_version)
			DecreaseUserPoolerSize(slot->userPool, __FILE__, __LINE__);
		slot->userPool = newUserPool;
	}
	else
	{
		Assert(slot->userPool == newUserPool);
	}
}

/* Process async msg from async threads */
static void pooler_handle_sync_response_queue(void)
{
	int32              addcount      = 0;
	int32              threadIndex   = 0;
	PGXCPoolAsyncBatchTask *bconnRsp = NULL;
	PoolAgent 		       *agent	 = NULL;
	PGXCNodePoolSlot       *slot     = NULL;
	PGXCNodePoolSlot      **bslot    = NULL;
	int                     i        = 0;
	int 			        bcount   = 0;
	int32                   node_type = 0;
	int32                   nodeindex = 0;

	/* sync new connection node into hash table */
	threadIndex = 0;
	while (threadIndex < MAX_SYNC_NETWORK_THREAD)
	{
		for (addcount = 0; addcount < POOL_SYN_REQ_CONNECTION_NUM; addcount++)
		{
			bconnRsp = (PGXCPoolAsyncBatchTask*)PipeGet(g_PoolSyncNetworkControl.response[threadIndex]);
			if (NULL == bconnRsp)
			{
				break;
			}

			agent  = bconnRsp->agent;
			bslot  = bconnRsp->m_batch_slot;
			bcount = bconnRsp->m_batch_count;

			pg_read_barrier();

			switch (bconnRsp->cmd)
			{
				case 'g':	/* acquire connection */
				{
					int need_connect_count    = bconnRsp->m_need_connect_count;
					PGXCUserPool **buserpool  = bconnRsp->m_batch_userpool;
					int           *bnodeindex = bconnRsp->m_batch_nodeindex;
					PGXCUserPool  *userpool   = NULL;
					PGXCNodePoolSlot **bslots = bconnRsp->m_batch_slot;

					record_time(bconnRsp->start_time, bconnRsp->end_time);

					if (PoolConnectDebugPrint || bconnRsp->m_task_status != PoolTaskStatus_success)
					{
						elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue acquire request pid:%d req_seq:%d finish, status:%d, tasks num:%d, ERROR_MSG:%s",  agent->pid, bconnRsp->req_seq, bconnRsp->m_task_status, bconnRsp->m_batch_count, bconnRsp->errmsg);
					}

					if (bconnRsp->m_task_status != PoolTaskStatus_success)
					{
						Oid nodeoid = InvalidOid;

						for (i = 0; i < need_connect_count; i++)
						{
							if (bconnRsp->m_batch_status[i] != PoolTaskStatus_success)
							{
								slot      = bslot[i];
								userpool  = buserpool[i];
								nodeindex = bnodeindex[i];

								/* free the slot */
								elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue acquire connections ERROR!! node:%u nodeindex:%d pid:%d req_seq:%d ERROR_MSG:%s", userpool->nodePool->nodeoid, nodeindex, agent->pid, bconnRsp->req_seq, bconnRsp->m_batch_errmsg[i].errmsg);

								DecreaseUserPoolerSizeAsync(userpool, bconnRsp->req_seq, __FILE__,__LINE__);
								if (slot)
								{
									if (slot->conn)
									{
										destroy_slot(nodeindex, userpool->nodePool->nodeoid, slot);
									}
									else
									{
										if (slot->pgoptions)
											pfree(slot->pgoptions);
										pfree(slot);
									}
								}
							}
						}

						for (i = need_connect_count; i < bcount; i++)
						{
							userpool  = buserpool[i];
							nodeindex = bnodeindex[i];
							slot = bslots[i];

							/* release the connections when set error. */
							if (bconnRsp->m_batch_status[i] != PoolTaskStatus_success ||
									(slot && slot->m_version != userpool->nodePool->m_version))
							{
								if (isUserPoolCoord(userpool))
								{
									nodeoid = agent->coord_conn_oids[nodeindex];
									slot = agent->coord_connections[nodeindex];
									agent->coord_connections[nodeindex] = NULL;
								}
								else
								{
									nodeoid = agent->dn_conn_oids[nodeindex];
									slot = agent->dn_connections[nodeindex];
									agent->dn_connections[nodeindex] = NULL;
								}

								elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue set session params ERROR!!"
									"node:%u node_type:%d nodeindex:%d pid:%d when acquire, ERROR_MSG:%s",
									nodeoid, node_type, nodeindex, agent->pid, bconnRsp->m_batch_errmsg[i].errmsg);

								/* Force to close the connection. */
								if (slot)
								{
									if (slot->set4reuse)
									{
										slot->set4reuse = false;
										DecreaseUserPoolRefCount(userpool, __FILE__, __LINE__);
									}
									release_connection(agent, agent->pool, slot, nodeindex, nodeoid, true, isUserPoolCoord(userpool));
								}
							}
							else if (slot && slot->set4reuse)
								change_slot_user_pool(agent, userpool, slot);
						}
					}
					else
					{
						for (i = need_connect_count; i < bcount; i++)
						{
							userpool  = buserpool[i];
							nodeindex = bnodeindex[i];
							slot = bslots[i];

							if (slot && slot->set4reuse)
								change_slot_user_pool(agent, userpool, slot);
						}

						move_connections_into_used_pool(agent, bconnRsp->m_datanodelist);
					}

					list_free(bconnRsp->m_datanodelist);
					list_free(bconnRsp->m_coordlist);
					pfree(bconnRsp->m_result);
					pfree(bconnRsp->m_pidresult);
					break;
				}

				case 'h':
				{
					/* Cancel SQL Command in progress on specified connections */
					if (PoolConnectDebugPrint || bconnRsp->m_task_status != PoolTaskStatus_success)
					{
						elog(LOG, POOL_MGR_PREFIX"++++pooler_handle_sync_response_queue CANCLE_QUERY pid:%d finish, status:%d, tasks num:%d ERROR_MSG:%s", agent->pid, bconnRsp->m_task_status, bconnRsp->m_batch_count, bconnRsp->errmsg);
					}

					if (bconnRsp->m_task_status != PoolTaskStatus_success)
					{
						for (i = 0; i < bcount; i++)
						{
							if (bconnRsp->m_batch_status[i] != PoolTaskStatus_success)
							{
								slot      = bslot[i];
								node_type = bconnRsp->m_batch_node_type[i];
								nodeindex = bconnRsp->m_batch_nodeindex[i];

								if (slot)
								{
									elog(LOG, POOL_MGR_PREFIX"pooler_handle_sync_response_queue CANCLE_QUERY node_type:%d nodeindex:%d nodename:%s backend_pid:%d session_pid:%d ERROR!! ERROR_MSG:%s", node_type, nodeindex, slot->node_name,slot->backend_pid, agent->pid, bconnRsp->m_batch_errmsg[i].errmsg);
								}
							}
						}
					}

					break;
				}

				default:
				{
					/* should never happens */
					abort();
				}
			}

			destroy_async_batch_request(bconnRsp);
			bconnRsp = NULL;

			/* decrease agent ref count */
			agent_decrease_ref_count(agent);
			
			/* handle pending agent, if any */
			agent_handle_pending_agent(agent);
		}
		threadIndex++;
	}
	return;
}

static void pooler_sync_connections_to_nodepool(void)
{
	bool               found;
	int32              nodeidx     = 0;
	int32              addcount    = 0;
	int32              threadIndex = 0;
	int32              connIndex   = 0;

	int32              nClose       = 0;
	int32              nReset       = 0;
	int32              nWarm        = 0;
	int32			   nJudge       = 0;


	PGXCNodePool       *nodePool    = NULL;
	PGXCUserPool       *userPool    = NULL;
	PGXCAsyncCmdInfo  *asyncInfo   = NULL;
	PGXCPoolConnectReq *connRsp     = NULL;
	PGXCNodePoolSlot   *slot 	    = NULL;
	MemoryContext     oldcontext;

	/* sync async connection command from connection manage thread */
	while (addcount < POOL_SYN_CONNECTION_NUM)
	{
		asyncInfo = (PGXCAsyncCmdInfo*)PipeGet(g_AsynUtilityPipeRcver);
		if (NULL == asyncInfo)
		{
			break;
		}

		switch (asyncInfo->cmd)
		{
			case COMMAND_CONNECTION_NEED_CLOSE:
			{
				nodePool = (PGXCNodePool *) hash_search(asyncInfo->dbPool->nodePools, &asyncInfo->node,
									HASH_FIND, &found);
				if (!found)
				{
					elog(LOG, POOL_MGR_PREFIX"Pooler:no entry inside the hashtable of Oid:%u", asyncInfo->node);
				}

				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"Pooler:connection of node:%u has %d MBytes, close now", asyncInfo->node, asyncInfo->size);
				}

				if (asyncInfo->slot->bwarmed)
				{
					elog(LOG, POOL_MGR_PREFIX"Pooler:warmed connection of node:%u has %d MBytes, colse now", asyncInfo->node, asyncInfo->size);
				}

				userPool = asyncInfo->slot->userPool;

				if (nodePool)
				{
					DecreaseUserPoolerSize(userPool, __FILE__, __LINE__);
					nodePool->nquery--;
				}
				/* time to close the connection */
				destroy_slot(asyncInfo->nodeindex, asyncInfo->node, asyncInfo->slot);
				nClose++;
			}
			break;

			case COMMAND_JUDGE_CONNECTION_MEMSIZE:
			{
				nodePool = (PGXCNodePool *) hash_search(asyncInfo->dbPool->nodePools, &asyncInfo->node,
									HASH_ENTER, &found);

				if (!found)
					init_node_pool(asyncInfo->dbPool, nodePool, asyncInfo->node, false);

				/* fresh the touch timestamp */
				asyncInfo->slot->released  = time(NULL);

				/* fresh the create timestamp to stop immediate check size */
				asyncInfo->slot->checked = time(NULL);

				if (asyncInfo->slot->m_version == nodePool->m_version)
				{
					/* Not Increase count of pool size, just the free size */
					PGXCUserPool *userPool = asyncInfo->slot->userPool;

					asyncInfo->slot->pid = -1;
					userPool->slot[userPool->freeSize] = asyncInfo->slot;
					IncreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);
					if (PoolConnectDebugPrint)
					{
						elog(LOG,
							 POOL_MGR_PREFIX"pooler_sync_connections_to_nodepool return connection to node:%s backend_pid:%d nodeidx:%d nodepool size:%d freeSize:%d slot: %p slots: %p", nodePoolNodeName(nodePool), asyncInfo->slot->backend_pid, nodeidx, nodePool->size, nodePool->freeSize, asyncInfo->slot, userPool->slot);
					}
				}
				else
				{
					nodeidx = get_node_index_by_nodeoid(asyncInfo->node);
					destroy_slot(nodeidx, asyncInfo->node, asyncInfo->slot);
					if (PoolConnectDebugPrint)
					{
						elog(LOG, POOL_MGR_PREFIX"destory connection to node:%u nodeidx:%d nodepool size:%d freeSize:%d for unmatch version, slot->m_version:%d, nodePool->m_version:%d", asyncInfo->node, nodeidx, nodePool->size, nodePool->freeSize, asyncInfo->slot->m_version, nodePool->m_version);
					}
				}

				nJudge++;
				nodePool->nquery--;
				if (PoolConnectDebugPrint)
				{
					elog(LOG, POOL_MGR_PREFIX"async query node:%u succeed, poolsize:%d, freeSize:%d, nquery:%d, node:%u %d Mbytes, slot_seq:%d", nodePool->nodeoid, nodePool->size, nodePool->freeSize, nodePool->nquery, asyncInfo->node, asyncInfo->size, asyncInfo->slot->seqnum);
				}

				if (nodePool->nquery < 0)
				{
					elog(LOG, POOL_MGR_PREFIX"node pool:%u invalid count nquery:%d ", asyncInfo->node, nodePool->nquery);
				}
			}
			break;

			case COMMAND_PING_NODE:
			{
				elog(DEBUG1, "Node (%s) back online!", NameStr(asyncInfo->nodename));
				if (!PgxcNodeUpdateHealth(asyncInfo->node, true))
					elog(WARNING, "Could not update health status of node (%s)",
						 NameStr(asyncInfo->nodename));
				else
					elog(LOG, "Health map updated to reflect HEALTHY node (%s)",
						 NameStr(asyncInfo->nodename));
			}
			break;


			default:
			{
				elog(LOG, POOL_MGR_PREFIX"invalid async command %d", asyncInfo->cmd);
			}
			break;
		}
		pfree((void*)asyncInfo);
		addcount++;
	}

	if (addcount > 100)
	{
		elog(LOG, POOL_MGR_PREFIX"async connection process:Total:%d nClose:%d  nReset:%d nWarm:%d nJudge:%d", addcount, nClose, nReset, nWarm, nJudge);
	}

	/* sync new connection node into hash table */
	threadIndex = 0;
	while (threadIndex < MAX_SYNC_NETWORK_THREAD)
	{
		time_t now = time(NULL);
		for (addcount = 0; addcount < POOL_SYN_REQ_CONNECTION_NUM; addcount++)
		{
			connRsp = (PGXCPoolConnectReq*)PipeGet(g_PoolConnControl.response[threadIndex]);
			if (NULL == connRsp)
			{
				break;
			}
			switch (connRsp->cmd)
			{
				case COMMAND_CONNECTION_BUILD:
				{
					/* search node pool hash to find the pool */
					nodePool = (PGXCNodePool *) hash_search(connRsp->dbPool->nodePools, &connRsp->nodeoid,
											HASH_ENTER, &found);

					if (!found)
						init_node_pool(connRsp->dbPool, nodePool, connRsp->nodeoid, connRsp->bCoord);

					/* add connection to hash table */
					for (connIndex = 0; connIndex < connRsp->validSize; connIndex++)
					{
						oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
						slot = (PGXCNodePoolSlot *) palloc0(sizeof(PGXCNodePoolSlot));
						if (slot == NULL)
						{
							ereport(ERROR,
									(errcode(ERRCODE_OUT_OF_MEMORY),
									 errmsg(POOL_MGR_PREFIX"Pooler out of memory")));
						}

						/* assign value to slot */
						*slot = connRsp->slot[connIndex];

						/* give the slot a seq number */
						slot->seqnum = pooler_get_slot_seq_num();
						slot->pid 	 = -1;

						/* store the connection info into node pool, if space is enough */
						if (nodePoolTotalSize(nodePool) < MaxPoolSize && connRsp->m_version == nodePool->m_version)
						{
							PGXCUserPool *userPool = connRsp->userPool;

							slot->created   = now;
							slot->released  = now;
							slot->checked   = now;
							slot->m_version = nodePool->m_version;
							slot->user_name = userPool->user_name;
							slot->node_name = nodePoolNodeName(nodePool);
							slot->pgoptions = pstrdup(userPool->pgoptions);
							slot->user_id   = userPool->user_id;
							slot->userPool  = userPool;
							slot->backend_pid = ((PGconn *) slot->conn)->be_pid;
							userPool->slot[userPool->freeSize] = slot;
							IncreaseUserPoolerSize(userPool, __FILE__, __LINE__);
							IncreaseUserPoolerFreesize(userPool,__FILE__,__LINE__);
							if (PoolConnectDebugPrint)
							{
								PGconn *tmp_conn = (PGconn *)slot->conn;
								elog(LOG, POOL_MGR_PREFIX"pooler_sync_connections_to_nodepool add new connection to node:%s "
														 "backend_pid:%d fd:%d refcount:%d nodepool size:%d freeSize:%d connIndex:%d validSize:%d",
									 						nodePoolNodeName(nodePool), slot->backend_pid, tmp_conn? tmp_conn->sock : -1,
															slot->refcount, nodePool->size, nodePool->freeSize,
															connIndex, connRsp->validSize);
								// print_pooler_slot(slot);
							}
						}
						else
						{
							if (PoolConnectDebugPrint)
							{
								elog(LOG, POOL_MGR_PREFIX"destroy slot poolsize:%d, freeSize:%d, node:%u, nodename:%s backend_pid:%d refcount:%d"
										" MaxPoolSize:%d, connRsp->m_version:%d, nodePool->m_version:%d, ",
									 	 nodePoolTotalSize(nodePool), nodePoolTotalFreeSize(nodePool), nodePool->nodeoid,
										 nodePoolNodeName(nodePool), slot->backend_pid, slot->refcount,
										 MaxPoolSize, connRsp->m_version, nodePool->m_version);
							}
							destroy_slot(connRsp->nodeindex, connRsp->nodeoid, slot);
						}
						MemoryContextSwitchTo(oldcontext);
					}
					nodePool->asyncInProgress = false;

					if (PoolConnectDebugPrint)
					{
						elog(LOG, POOL_MGR_PREFIX"async build succeed, poolsize:%d, freeSize:%d, node:%u", nodePool->size,
																							nodePool->freeSize,
																							nodePool->nodeoid);
					}

					/* check if some node failed to connect, just release last socket */
					if (connRsp->validSize < connRsp->size && connRsp->failed)
					{
						ereport(LOG,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg(POOL_MGR_PREFIX"failed to connect to "
													   "Datanode:[%s], validSize:%d, size:%d, errmsg:%s",
													   nodePool->connstr,
													   connRsp->validSize,
													   connRsp->size,
													   PQerrorMessage((PGconn *)(connRsp->slot[connRsp->validSize].conn)))));

						if (!PGXCNodeConnected(connRsp->slot[connRsp->validSize].conn))
						{
							/* here, we need only close the connection */
							close_slot(connRsp->nodeindex, connRsp->nodeoid, &connRsp->slot[connRsp->validSize]);
						}
					}

					/* free the memory */
					pfree((void*)connRsp);
					break;
				}

				case COMMAND_CONNECTION_CLOSE:
				{
					/* just free the request */
					pfree((void*)connRsp);
					break;
				}

				default:
				{
					/* should never happens */
					abort();
				}
			}
		}
		threadIndex++;
	}
	return;
}

/* async query the memrory usage of a conection */
static void pooler_async_query_connection(DatabasePool *pool, PGXCNodePoolSlot *slot, int32 nodeidx, Oid node)
{
	uint64 pipeput_loops = 0;
	PGXCAsyncCmdInfo *asyncInfo = NULL;

	MemoryContext     oldcontext;
	oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

	asyncInfo         		= (PGXCAsyncCmdInfo*)palloc0(sizeof(PGXCAsyncCmdInfo));
	asyncInfo->cmd    		= COMMAND_JUDGE_CONNECTION_MEMSIZE;
	asyncInfo->dbPool 		= pool;
	asyncInfo->slot   		= slot;
	asyncInfo->nodeindex    = nodeidx;
	asyncInfo->node   		= node;
	while (-1 == PipePut(g_AsynUtilityPipeSender, (void*)asyncInfo))
	{
		pipeput_loops++;
	}
	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async query connection db:%s user:%s node:%u for loops %lu", pool->database, pool->user_name, node, pipeput_loops);
	}

	ThreadSemaUp(&g_AsnyUtilitysem);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"async query connection db:%s user:%s node:%u", pool->database, pool->user_name, node);
	}
	MemoryContextSwitchTo(oldcontext);
}

/* async ping a node */
static void pooler_async_ping_node(Oid node)
{
	uint64 pipeput_loops = 0;
	PGXCAsyncCmdInfo *asyncInfo = NULL;
	MemoryContext     oldcontext;
	NodeDefinition 	  *nodeDef   = NULL;

	if (IS_ASYNC_PIPE_FULL())
		return;

	nodeDef = PgxcNodeGetDefinition(node);
	if (nodeDef == NULL)
	{
		/* No such definition, node dropped? */
		elog(DEBUG1, "Could not find node (%u) definition,"
			 " skipping health check", node);
		return;
	}
	if (nodeDef->nodeishealthy)
	{
		/* hmm, can this happen? */
		elog(DEBUG1, "node (%u) healthy!"
			 " skipping health check", node);
		if (nodeDef)
			pfree(nodeDef);
		return;
	}

	elog(LOG, "node (%s:%u) down! Trying ping",
		 NameStr(nodeDef->nodename), node);


	oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
	asyncInfo         		= (PGXCAsyncCmdInfo*)palloc0(sizeof(PGXCAsyncCmdInfo));
	asyncInfo->cmd    		= COMMAND_PING_NODE;
	asyncInfo->node   		= node;
	memcpy(NameStr(asyncInfo->nodename), NameStr(nodeDef->nodename), NAMEDATALEN);
	memcpy(NameStr(asyncInfo->nodehost), NameStr(nodeDef->nodehost), NAMEDATALEN);
	asyncInfo->nodeport		= nodeDef->nodeport;

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"async ping node, param: node=%u, nodehost=%s, nodeport=%d",
					node, NameStr(asyncInfo->nodehost), asyncInfo->nodeport);
	}

	while (-1 == PipePut(g_AsynUtilityPipeSender, (void*)asyncInfo))
	{
		pipeput_loops++;
	}
	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async ping node:%u for loops %lu", node, pipeput_loops);
	}
	ThreadSemaUp(&g_AsnyUtilitysem);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"async ping node:%u", node);
	}
	MemoryContextSwitchTo(oldcontext);

	if (nodeDef)
		pfree(nodeDef);
}



/* async batch connection build  */
static bool pooler_async_build_connection(DatabasePool *pool, PGXCNodePool *nodePool, int32 nodeidx, Oid node, int32 size, bool bCoord)
{
	int32 threadid;
	uint64 pipeput_loops = 0;
	PGXCPoolConnectReq *connReq = NULL;
	int32 pool_version = nodePool->m_version;

	MemoryContext     oldcontext;

	/* if no free pipe line avaliable, do nothing */
	threadid = pooler_async_task_pick_thread(&g_PoolConnControl, nodeidx);
	if (-1 == threadid)
	{
		elog(LOG, POOL_MGR_PREFIX"no pipeline avaliable, pooler_async_build_connection node:%u nodeidx:%d", node, nodeidx);
		return false;
	}

	oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
	connReq  = (PGXCPoolConnectReq*)palloc0(sizeof(PGXCPoolConnectReq) + (size - 1) * sizeof(PGXCNodePoolSlot));
	connReq->cmd       = COMMAND_CONNECTION_BUILD;
	connReq->userPool  = nodePool->userPoolListHead;
	connReq->nodeindex = nodeidx;
	connReq->nodeoid   = node;
	connReq->dbPool    = pool;
	connReq->bCoord    = bCoord;
	connReq->size      = size;
	connReq->validSize = 0;
	connReq->m_version = pool_version;

	while (-1 == PipePut(g_PoolConnControl.request[threadid], (void*)connReq))
	{
		pipeput_loops++;
	}
	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async build connection db:%s user:%s node:%u size:%d for loops %lu", pool->database, pool->user_name, node, size, pipeput_loops);
	}
	/* signal thread to start build job */
	ThreadSemaUp(&g_PoolConnControl.sem[threadid]);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"async build connection db:%s user:%s node:%u size:%d", pool->database, pool->user_name, node, size);
	}
	MemoryContextSwitchTo(oldcontext);
	return true;
}
/*
 * Set cancel socket keepalive and user_timeout.
 * We can use this to detect the broken connection quickly.
 * see SetSockKeepAlive
 */
static void
set_cancel_conn_keepalive(PGcancel *cancelConn)
{
	uint32 user_timeout = 0;

	if (cancelConn == NULL)
	{
		return;
	}

	if (tcp_user_timeout == 0)
	{
		if (tcp_keepalives_idle > 0)
		{
			int time_offset = 0;
			/*
			 * set user_timeout to slightly less than idle + count * interval,
			 * tcp will be aborted after tcp_keepalives_count probes
			 */
			if (tcp_keepalives_count >= 1)
				time_offset = (int)(((float)tcp_keepalives_count - 0.5) * tcp_keepalives_interval);
			else
				time_offset = 1;

			user_timeout = time_offset + tcp_keepalives_idle;
			if ((UINT32_MAX / 1000) > user_timeout)
			{
				user_timeout = user_timeout * (uint32)1000;
			}
		}
	}
	else
	{
		user_timeout = tcp_user_timeout;
	}

	/*
	 * If the connection did not use the connection option
	 * set the option here
	 * */
	if (cancelConn->keepalives == -1)
	{
		/* use TCP keepalives */
		cancelConn->keepalives = 1;

		if (tcp_keepalives_idle > 0)
		{
			/* time between TCP keepalives */
			cancelConn->keepalives_idle = tcp_keepalives_idle;
		}

		if (tcp_keepalives_interval > 0)
		{
			/*time between TCP keepalive retransmits */
			cancelConn->keepalives_interval = tcp_keepalives_interval;
		}

		if (tcp_keepalives_count > 0)
		{
			/* maximum number of TCP keepalive retransmits */
			cancelConn->keepalives_count = tcp_keepalives_count;
		}
	}

	if (cancelConn->pgtcp_user_timeout == -1 && user_timeout > 0)
	{
		/* tcp user timeout */
		cancelConn->pgtcp_user_timeout = user_timeout;
	}
}


/* aync acquire connection */
static bool dispatch_async_network_operation(PGXCPoolAsyncBatchTask *req)
{
	int32 threadid = 0;
	uint64 pipeput_loops = 0;

	/* choose a thread to handle the msg */
	threadid = pooler_async_task_pick_thread(&g_PoolSyncNetworkControl, -1);
	/* failed to pick up a thread */
	if (-1 == threadid)
	{
		elog(LOG, POOL_MGR_PREFIX"pid:%d fail to pick a thread for operation cmd:%d req_seq:%d", req->agent->pid, req->cmd, req->req_seq);
		return false;
	}


	/* dispatch the msg to the handling thread */
	while (-1 == PipePut(g_PoolSyncNetworkControl.request[threadid % MAX_SYNC_NETWORK_THREAD], (void*)req))
	{
		pipeput_loops++;
	}
	if (pipeput_loops > 0)
	{
		elog(LOG, POOL_MGR_PREFIX"fail to async network operation pid:%d cmd:%d thread:%d req_seq:%d for loops %lu", req->agent->pid, req->cmd, threadid, req->req_seq, pipeput_loops);
	}

	/* increase agent ref count */
	agent_increase_ref_count(req->agent);

	/* signal thread to start build job */
	ThreadSemaUp(&g_PoolSyncNetworkControl.sem[threadid % MAX_SYNC_NETWORK_THREAD]);
	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"async network operation pid:%d cmd:%d succeed thread:%d req_seq:%d", req->agent->pid, req->cmd, threadid, req->req_seq);
	}

	return true;
}

/*
 * Thread that will build connection async
 */
void *pooler_async_connection_management_thread(void *arg)
{
	int32               i           = 0;
	int32 				threadIndex = 0;
	PGXCPoolConnectReq  *request 	= NULL;
	PGXCNodePoolSlot 	*slot		= NULL;

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

	ThreadSigmask();

	threadIndex = ((PGXCPoolConnThreadParam*)arg)->threadIndex;
	while (1)
	{
		/* wait for signal */
		ThreadSemaDown(&g_PoolConnControl.sem[threadIndex]);

		/* create connect as needed */
		request = (PGXCPoolConnectReq*)PipeGet(g_PoolConnControl.request[threadIndex]);
		if (request)
		{
			/* record status of the task */
			pooler_async_task_start(&g_PoolConnControl, threadIndex, request->nodeindex, NULL, InvalidOid, request->cmd);

			switch (request->cmd)
			{
				case COMMAND_CONNECTION_BUILD:
				{
					PGXCUserPool *userPool = request->userPool;
					for (i = 0; i < request->size; i++, request->validSize++)
					{
						slot =  &request->slot[i];
						/* If connection fails, be sure that slot is destroyed cleanly */
						slot->xc_cancelConn = NULL;

						/* Establish connection TODO: use PQconnectdbParallel */
						slot->conn = PGXCNodeConnectBarely(userPool->connstr);
						if (!PGXCNodeConnected(slot->conn))
						{
							request->failed = true;
							break;
						}
						slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
						slot->bwarmed       = false;
						SetSockKeepAlive(((PGconn *)slot->conn)->sock, tcp_keepalives_interval, tcp_keepalives_count, tcp_keepalives_idle);
						set_cancel_conn_keepalive((PGcancel *)slot->xc_cancelConn);
					}
					break;
				}

				case COMMAND_CONNECTION_CLOSE:
				{
					PQfreeCancel((PGcancel *)request->slot[0].xc_cancelConn);
					PGXCNodeClose(request->slot[0].conn);
					break;
				}

				default:
				{
					/* should never happen */
					abort();
				}
			}

			/* clear the work status */
			pooler_async_task_done(&g_PoolConnControl, threadIndex);

			/* return the request to main thread, so that the memory can be freed */
			while(-1 == PipePut(g_PoolConnControl.response[threadIndex], request))
			{
				pg_usleep(1000L);
			}
		}
	}
	return NULL;
}

/*
 * parallel set query for PGXCNodeSendSetQuery
 * return fail tasks count
 */
static int
agent_send_setquery_parallel(PGXCNodePoolSlot *bslot[], int count, const char *sql_command, bool set4reuse,
		BatchTaskErrMsg *batch_errmsg, int32 buf_len, int *status, char **first_errmsg)
{
	int          res_status;
	int 	     i;
	PGresult	*result;
	int          error      = 0;
	time_t		 now        = time(NULL);
	int          fail_count = 0;

	for (i = 0; i < count; ++i)
	{
		if (status[i] != PoolTaskStatus_success)
			continue;

		if (set4reuse)
		{
			if (!bslot[i]->set4reuse)
				continue;

			if (!PQsendQueryPoolerStatelessReuse((PGconn *) bslot[i]->conn, sql_command))
			{
				if (batch_errmsg && buf_len)
				{
					snprintf(batch_errmsg[i].errmsg, buf_len, "node %s error:%s !", bslot[i]->node_name, PQerrorMessage((PGconn *) bslot[i]->conn));

					if (first_errmsg && *first_errmsg == NULL)
						*first_errmsg = batch_errmsg[i].errmsg;
				}

				status[i] = PoolTaskStatus_error;
				++fail_count;
			}
		}
		else if (!PQsendQuery((PGconn *) bslot[i]->conn, sql_command))
		{
			if (batch_errmsg && buf_len)
			{
				snprintf(batch_errmsg[i].errmsg, buf_len, "node %s error:%s !", bslot[i]->node_name, PQerrorMessage((PGconn *) bslot[i]->conn));

				if (first_errmsg && *first_errmsg == NULL)
					*first_errmsg = batch_errmsg[i].errmsg;
			}

			status[i] = PoolTaskStatus_error;
			++fail_count;
		}
	}

	for (i = 0; i < count; ++i)
	{
		if (status[i] != PoolTaskStatus_success)
			continue;

		if (set4reuse && !bslot[i]->set4reuse)
			continue;

		error = 0;

		/* Consume results from SET commands */
		while ((result = PQgetResultTimed((PGconn *) bslot[i]->conn, now + PGXC_RESULT_TIME_OUT)) != NULL)
		{
			res_status = PQresultStatus(result);
			if (res_status != PGRES_TUPLES_OK && res_status != PGRES_COMMAND_OK)
			{
				if (batch_errmsg && buf_len)
				{
					snprintf(batch_errmsg[i].errmsg, buf_len, "node %s error:%s !", bslot[i]->node_name, PQerrorMessage((PGconn *) bslot[i]->conn));

					if (first_errmsg && *first_errmsg == NULL)
						*first_errmsg = batch_errmsg[i].errmsg;
				}
				error++;
			}

			/* TODO: Check that results are of type 'S' */
			PQclear(result);
		}

		if (error)
		{
			status[i] = PoolTaskStatus_error;
			++fail_count;
		}
	}

	return fail_count;
}

/*
 * parallel set query for PQcancel_timeout
 * return fail tasks count
 */
static int
agent_cancel_query_on_connections(int signal, PGXCNodePoolSlot *bslot[], pgsocket bsock[], int count, BatchTaskErrMsg *batch_errmsg, int32 buf_len, int* status)
{
	int 	     i;
	NODE_CANCEL	*cancelConn;
	int  fail_count    = 0;
	bool end_query     = (signal == SIGNAL_SIGUSR2 ? true: false);
	int  timeout       = PoolCancelQueryTimeout;

	for (i = 0; i < count; ++i)
	{
		cancelConn = bslot[i]->xc_cancelConn;
		if (cancelConn == NULL || !PQcancelSend((PGcancel *) cancelConn, end_query, batch_errmsg[i].errmsg, buf_len, timeout, &bsock[i]))
		{
			status[i] = PoolTaskStatus_error;
			++fail_count;
		}
	}

	for (i = 0; i < count; ++i)
	{
		if (status[i] != PoolTaskStatus_success)
			continue;

		PQcancelRecv(bsock[i]);
	}

	return fail_count;
}

/*
 * Thread that will handle sync network operation
 */
void *pooler_sync_remote_operator_thread(void *arg)
{
	int32  				ret            = 0;
	int32               res			   = 0;
	int32 				threadIndex    = 0;
	PGXCPoolAsyncBatchTask *brequest   = NULL;
	PGXCNodePoolSlot 	   *slot       = NULL;
	PoolAgent              *agent      = NULL;
	PGXCNodePoolSlot      **bslot      = NULL;
	int                    *bnodeindex = NULL;
	int 			        bcount     = 0;

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

	ThreadSigmask();

	threadIndex = ((PGXCPoolConnThreadParam*)arg)->threadIndex;
	while (1)
	{
		/* wait for signal */
		ThreadSemaDown(&g_PoolSyncNetworkControl.sem[threadIndex]);

		/* create connect as needed */
		brequest = (PGXCPoolAsyncBatchTask*)PipeGet(g_PoolSyncNetworkControl.request[threadIndex]);
		if (brequest)
		{
			/* set task status */
			pooler_async_task_start(&g_PoolSyncNetworkControl, threadIndex, -1, NULL, InvalidOid, brequest->cmd);

			agent      = brequest->agent;
			bslot      = brequest->m_batch_slot;
			bnodeindex = brequest->m_batch_nodeindex;
			bcount     = brequest->m_batch_count;

			switch (brequest->cmd)
			{
				case 'g':
				{
					/* GET CONNECTIONS */
					PGXCUserPool    **buserpool          = brequest->m_batch_userpool;
					char            **bconninfo          = brequest->m_batch_conninfo;
					NODE_CONNECTION **bconn              = brequest->m_batch_conn;
					int 		      need_connect_count = brequest->m_need_connect_count;
					PGXCUserPool     *userpool           = NULL;
					int32             nodeindex          = -1;
					int32		      ret2               = 0;
					int               i                  = 0;
					int               batch_fail_count   = 0;
					char 			 *first_errmsg       = NULL;
					time_t            created_time;

					/* make continuous storage of new and non new connections, facilitating batch interface operations */
					if (need_connect_count != bcount)
					{
						int gap = brequest->m_max_batch_size - bcount;
						for (i = need_connect_count; i < bcount; i++)
						{
							buserpool[i]  = buserpool[i + gap];
							bnodeindex[i] = bnodeindex[i + gap];
							bslot[i]      = bslot[i + gap];
						}
					}

					/* record message */
					record_task_message(&g_PoolSyncNetworkControl, threadIndex, "Start to PQconnectdbParallel");

					for (i = 0; i < need_connect_count; i++)
					{
						userpool = buserpool[i];
						if (strcmp(userpool->pgoptions, agent->pgoptions) != 0)
							bconninfo[i] = build_node_conn_str_by_user(userpool->nodePool->nodeInfo, agent->pool, userpool->user_name, agent->pgoptions, brequest->m_batch_connstr_buf[i].connstr);
						else
							bconninfo[i] = userpool->connstr;
					}

					/* 1. connect */
					if (need_connect_count)
						PQconnectdbParallel((char const* const*)bconninfo, need_connect_count, (PGconn **)bconn);

					for (i = 0; i < need_connect_count; i++)
					{
						userpool    = buserpool[i];
						nodeindex   = bnodeindex[i];
						slot        = bslot[i];
						slot->conn  = bconn[i];

						if (!PGXCNodeConnected(slot->conn))
						{
							snprintf(brequest->m_batch_errmsg[i].errmsg, POOLER_ERROR_MSG_LEN, "connect for %s(%s:%d) failed, errmsg: %s",
									 userPoolNodeName(userpool), nodePoolNodeHost(userpool->nodePool), nodePoolNodePort(userpool->nodePool),
									 PQerrorMessage((PGconn *)(slot->conn)));

							if (first_errmsg == NULL)
								first_errmsg = brequest->m_batch_errmsg[i].errmsg;

							brequest->m_batch_status[i] = PoolTaskStatus_error;
							++batch_fail_count;

							pooler_thread_logger(LOG, "connect for [%s] failed errmsg %s", bconninfo[i], PQerrorMessage((PGconn *)(slot->conn)));
						}
					}

					/* 2. set connections to reuse if need */
					if (brequest->m_need_set4reuse_count)
					{
						/* record message */
						record_task_message(&g_PoolSyncNetworkControl, threadIndex, agent->set_conn_reuse_sql);

						batch_fail_count += agent_send_setquery_parallel(&bslot[need_connect_count], bcount - need_connect_count,
																		agent->set_conn_reuse_sql, true, brequest->m_batch_errmsg,
																		POOLER_ERROR_MSG_LEN, brequest->m_batch_status, &first_errmsg);
					}

					created_time = time(NULL);

					/* the need_connect_count is only when we build the connection here	*/
					for (i = 0; i < need_connect_count; i++)
					{
						userpool     = buserpool[i];
						nodeindex    = bnodeindex[i];
						slot         = bslot[i];

						/* Error, free the connection here only when we build the connection here */
						if (brequest->m_batch_status[i] != PoolTaskStatus_success)
						{
							if (slot->conn)
							{
								PGXCNodeClose(slot->conn);
								slot->conn = NULL;
							}
						}
						else
						{
							/* job succeed */
							slot->xc_cancelConn = (NODE_CANCEL *) PQgetCancel((PGconn *)slot->conn);
							slot->bwarmed       = false;
							SetSockKeepAlive(((PGconn *)slot->conn)->sock, tcp_keepalives_interval, tcp_keepalives_count, tcp_keepalives_idle);
							set_cancel_conn_keepalive((PGcancel *)slot->xc_cancelConn);

							/* set the time flags */
							slot->released = created_time;
							slot->checked  = slot->released;
							slot->created  = slot->released;

							/* increase usecount */
							slot->usecount++;
							slot->backend_pid = ((PGconn *) slot->conn)->be_pid;

							if (isUserPoolCoord(userpool))
								agent->coord_connections[nodeindex] = slot;
							else
								agent->dn_connections[nodeindex] = slot;
						}
					}

					if (batch_fail_count == 0)
					{
						int32      node_number   = 0;
						ListCell  *nodelist_item = NULL;

						/* Save in array fds of Datanodes first */
						foreach(nodelist_item, brequest->m_datanodelist)
						{
							int			node = lfirst_int(nodelist_item);
							if (agent->dn_connections[node])
							{
								brequest->m_result[node_number] = PQsocket((PGconn *) agent->dn_connections[node]->conn);
								brequest->m_pidresult[node_number] = ((PGconn *) agent->dn_connections[node]->conn)->be_pid;
								/*
								 * pooler's backend thread cannot allocate memory, so it is not possible
								 * to directly move the connection to dn_connections_used_pool,
								 * set the flag need_move_to_usedpool to true first
								 */
								agent->dn_connections[node]->need_move_to_usedpool = true;
								if (PoolConnectDebugPrint)
									pooler_thread_logger(LOG, "pooler_sync_remote_operator_thread req_seq:%d datanode %s: fd:%d, pid:%d be seq:%d",
										brequest->req_seq, agent->dn_connections[node]->node_name, brequest->m_result[node_number],
										brequest->m_pidresult[node_number], agent->rcv_seq);
								node_number++;
							}
						}

						/* Save then in the array fds for Coordinators */
						foreach(nodelist_item, brequest->m_coordlist)
						{
							int			node = lfirst_int(nodelist_item);
							if (agent->coord_connections[node])
							{
								brequest->m_result[node_number] = PQsocket((PGconn *) agent->coord_connections[node]->conn);
								brequest->m_pidresult[node_number] = ((PGconn *) agent->coord_connections[node]->conn)->be_pid;
								if (PoolConnectDebugPrint)
									pooler_thread_logger(LOG, "pooler_sync_remote_operator_thread req_seq:%d coord %s: fd:%d, pid:%d be seq:%d",
										brequest->req_seq, agent->coord_connections[node]->node_name, brequest->m_result[node_number],
										brequest->m_pidresult[node_number], agent->rcv_seq);
								node_number++;
							}
						}

						pg_write_barrier();

						Assert(node_number > 0);
						ret = pool_sendfds(&agent->port, brequest->m_result, node_number, brequest->errmsg, POOLER_ERROR_MSG_LEN, agent->rcv_seq);
						/*
						 * Also send the PIDs of the remote backend processes serving
						 * these connections
						 */
						ret2 = pool_sendpids(&agent->port, brequest->m_pidresult, node_number, brequest->errmsg, POOLER_ERROR_MSG_LEN, agent->rcv_seq);

						if (ret || ret2)
						{
							/* reset need_move_to_usedpool */
							foreach(nodelist_item, brequest->m_datanodelist)
							{
								int	node = lfirst_int(nodelist_item);
								PGXCNodePoolSlot *tmp_slot = agent->dn_connections[node];
								if (tmp_slot != NULL)
									tmp_slot->need_move_to_usedpool = false;
							}

							/* error */
							brequest->m_task_status = PoolTaskStatus_error;
						}
						else
						{
							brequest->m_task_status = PoolTaskStatus_success;
						}
					}
					else
					{
						/* failed to acquire connection */
						pool_sendfds(&agent->port, NULL, 0, brequest->errmsg, POOLER_ERROR_MSG_LEN, agent->rcv_seq);

						if (first_errmsg == NULL)
							first_errmsg = "unknown error";

						/* send the error msg */
						pool_putmessage_without_msgtype(&agent->port, first_errmsg, strlen(first_errmsg) + 1);
						pool_flush(&agent->port);

						brequest->m_task_status = PoolTaskStatus_error;
					}

					break;
				}

				case 'h':
				{
					/* Cancel SQL Command in progress on specified connections */
					res = agent_cancel_query_on_connections(brequest->signal, bslot, brequest->m_batch_sock, bcount, brequest->m_batch_errmsg, POOLER_ERROR_MSG_LEN, brequest->m_batch_status);

					ret = pool_sendres(&agent->port, bcount - res, brequest->errmsg, POOLER_ERROR_MSG_LEN, false);

					if (ret || res)
					{
						brequest->m_task_status = PoolTaskStatus_error;
					}
					else
					{
						brequest->m_task_status = PoolTaskStatus_success;
					}
					break;
				}

				default:
				{
					/* should never happen */
					abort();
				}
			}

			/* record each conn request end time */
			if (PoolPrintStatTimeout > 0 && 'g' == brequest->cmd)
			{
				gettimeofday(&brequest->end_time, NULL);
			}
			pg_write_barrier();
			/* clear task status */
			pooler_async_task_done(&g_PoolSyncNetworkControl, threadIndex);

			/* return the request to main thread, so that the memory can be freed */
			while(-1 == PipePut(g_PoolSyncNetworkControl.response[threadIndex], brequest))
			{
				pg_usleep(1000L);
			}
		}
	}
	return NULL;
}

/*
 * Thread used to process async connection requeset
 */
void *pooler_async_utility_thread(void *arg)
{
	PGXCAsyncCmdInfo *pCmdInfo = NULL;

	/* add sub thread longjmp check */
	SUB_THREAD_LONGJMP_DEFAULT_CHECK();

	ThreadSigmask();

	while (1)
	{
		ThreadSemaDown(&g_AsnyUtilitysem);
		pCmdInfo = (PGXCAsyncCmdInfo*)PipeGet(g_AsynUtilityPipeSender);
		if (pCmdInfo)
		{
			switch (pCmdInfo->cmd)
			{
				case COMMAND_JUDGE_CONNECTION_MEMSIZE:
				{
					int   mbytes = 0;
					char *size = NULL;
					size = PGXCNodeSendShowQuery((NODE_CONNECTION *)pCmdInfo->slot->conn, "show session_memory_size;");
					pCmdInfo->cmd = COMMAND_JUDGE_CONNECTION_MEMSIZE;
					mbytes = atoi(size);
					if (mbytes >= PoolMaxMemoryLimit)
					{
						pCmdInfo->cmd = COMMAND_CONNECTION_NEED_CLOSE;
					}
					pCmdInfo->size = mbytes;
				}
				break;

				case COMMAND_PING_NODE:
				{
					char connstr[MAXPGPATH * 2 + 256] = {0};
					sprintf(connstr, "host=%s port=%d dbname=%s", NameStr(pCmdInfo->nodehost),
							pCmdInfo->nodeport, PoolConnectDB);
					pCmdInfo->nodestatus = PGXCNodePing(connstr);
				}
				break;

				default:
				{
					/* should never happen */
					abort();
				}
			}

			/* loop and try to put the warmed connection back into queue */
			while (-1 == PipePut(g_AsynUtilityPipeRcver, (PGXCAsyncCmdInfo*)pCmdInfo))
			{
				pg_usleep(1000);
			}
		}

	}

	return NULL;
}

/*
 * Create a handle to a batch task
 */
static inline PGXCPoolAsyncBatchTask *
create_async_batch_request(int32 cmd, PoolAgent *agent, int max_batch_size)
{
	PGXCPoolAsyncBatchTask *asyncBatchReqTask;
	MemoryContext           oldcontext;

	//TODO: Let the lifecycle of the task follow the agent without the need to apply for release every time
	oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

	asyncBatchReqTask = (PGXCPoolAsyncBatchTask *) palloc0(sizeof(PGXCPoolAsyncBatchTask));
	asyncBatchReqTask->cmd				 = cmd;
	asyncBatchReqTask->agent 		 	 = agent;
	asyncBatchReqTask->m_max_batch_size  = max_batch_size;
	asyncBatchReqTask->m_batch_slot      = (PGXCNodePoolSlot **) palloc0(sizeof(PGXCNodePoolSlot *) * max_batch_size);
	asyncBatchReqTask->m_batch_nodeindex = (int *) palloc0(sizeof(int) * max_batch_size);
	asyncBatchReqTask->m_batch_node_type = (int *) palloc0(sizeof(int) * max_batch_size);
	asyncBatchReqTask->m_batch_status    = (int *) palloc0(sizeof(int) * max_batch_size);
	asyncBatchReqTask->m_batch_errmsg    = (BatchTaskErrMsg *) palloc0(sizeof(BatchTaskErrMsg) * max_batch_size);

	switch (cmd)
	{
		case 'g':
		{
			asyncBatchReqTask->m_batch_userpool = (PGXCUserPool **) palloc0(sizeof(PGXCUserPool *) * max_batch_size);
			break;
		}
		case 'h':
		{
			asyncBatchReqTask->m_batch_sock     = (pgsocket *) palloc0(sizeof(pgsocket) * max_batch_size);
		}
		default:
			break;
	}

	MemoryContextSwitchTo(oldcontext);

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"create batch request succeed, cmd %c", cmd);
	}
	return asyncBatchReqTask;
}

/*
 * Initialize batch task handles for get connections tasks
 */
static inline void
init_connection_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask, int32 reqseq,
				List *datanodelist, List *coordlist, int32 *fd_result, int32 *pid_result)
{
	Assert(asyncBatchReqTask->cmd = 'g');

	asyncBatchReqTask->m_datanodelist   = datanodelist;
	asyncBatchReqTask->m_coordlist      = coordlist;
	asyncBatchReqTask->m_result		    = fd_result;
	asyncBatchReqTask->m_pidresult	    = pid_result;
	asyncBatchReqTask->req_seq          = reqseq;
}

/*
 * Initialize batch task handles for cancel query tasks
 */
static inline void
init_cancel_query_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask, int signal)
{
	Assert(asyncBatchReqTask->cmd = 'h');

	asyncBatchReqTask->signal = signal;
}

/*
 * destroy batch task handles
 */
static inline void
destroy_async_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask)
{
	if (asyncBatchReqTask == NULL)
		return;

	pfree(asyncBatchReqTask->m_batch_nodeindex);
	pfree(asyncBatchReqTask->m_batch_node_type);
	pfree(asyncBatchReqTask->m_batch_status);
	pfree(asyncBatchReqTask->m_batch_slot);
	pfree(asyncBatchReqTask->m_batch_errmsg);

	switch (asyncBatchReqTask->cmd)
	{
		case 'g':
		{
			pfree(asyncBatchReqTask->m_batch_userpool);
			if (asyncBatchReqTask->m_batch_conninfo)
				pfree(asyncBatchReqTask->m_batch_conninfo);
			if (asyncBatchReqTask->m_batch_connstr_buf)
				pfree(asyncBatchReqTask->m_batch_connstr_buf);
			if (asyncBatchReqTask->m_batch_conn)
				pfree(asyncBatchReqTask->m_batch_conn);
			break;
		}
		case 'h':
		{
			pfree(asyncBatchReqTask->m_batch_sock);
		}
		default:
			break;
	}

	pfree(asyncBatchReqTask);
}

/*
 * dispatch bulk connection tasks
 */
static inline bool dispatch_connection_batch_request(PGXCPoolAsyncBatchTask *asyncBatchReqTask,
													 PGXCNodePoolSlot *slot,
													 PGXCUserPool     *userpool,
													 int               node_type,
													 int32             nodeindex,
													 bool              dispatched)

{
	bool 	   ret         = true;
	bool       needConnect = false;
	PoolAgent *agent       = asyncBatchReqTask->agent;
	MemoryContext oldcontext;

	if (!dispatched)
	{
		if (slot == NULL && nodePoolTotalSize(userpool->nodePool) >= MaxPoolSize)
		{
			elog(LOG, POOL_MGR_PREFIX"[dispatch_connection_batch_request] nodepool size:%d equal or larger than MaxPoolSize:%d, "
					  "pid:%d node:%s nodeindex:%d req_seq:%d, freesize:%d",
				 nodePoolTotalSize(userpool->nodePool), MaxPoolSize,
				 agent->pid, userPoolNodeName(userpool), nodeindex, asyncBatchReqTask->req_seq, userpool->nodePool->freeSize);
			/* return failed */
			ret = false;
			goto fail;
		}

		/* need to alloc a slot */
		if (slot == NULL)
		{
			oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

			/* Allocate new slot */
			slot = (PGXCNodePoolSlot *) palloc0(sizeof(PGXCNodePoolSlot));

			/* set seqnum */
			slot->seqnum     = pooler_get_slot_seq_num();
			slot->pid	     = agent->pid;
			slot->user_id    = userpool->user_id;
			slot->user_name  = userpool->user_name;
			slot->node_name  = userPoolNodeName(userpool);
			slot->pgoptions  = pstrdup(agent->pgoptions);
			slot->userPool   = userpool;

			MemoryContextSwitchTo(oldcontext);

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"dispatch async connection pid:%d node:%s nodeindex:%d slot_seq:%d req_seq:%d", agent->pid, userPoolNodeName(userpool), nodeindex, slot->seqnum, asyncBatchReqTask->req_seq);
			}

			/* set slot to null */
			userpool->slot[userpool->size] = NULL;

			IncreaseUserPoolerSize(userpool,__FILE__,__LINE__);
			IncreaseSlotRefCount(slot,__FILE__,__LINE__);

			/* use version to tag every slot */
			slot->m_version = userpool->nodePool->m_version;

			needConnect = true;
		}

		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"dispatch async connection pid:%d node:%s nodeindex:%d req_seq:%d, needConnect:%d", agent->pid, userPoolNodeName(userpool), nodeindex, asyncBatchReqTask->req_seq, needConnect);
		}

		/* just add task into batch */
		if (needConnect)
		{
			/* nodes that need to establish connections are continuously placed in front */
			asyncBatchReqTask->m_batch_userpool[asyncBatchReqTask->m_need_connect_count]  = userpool;
			asyncBatchReqTask->m_batch_nodeindex[asyncBatchReqTask->m_need_connect_count] = nodeindex;
			asyncBatchReqTask->m_batch_slot[asyncBatchReqTask->m_need_connect_count]      = slot;
			++asyncBatchReqTask->m_need_connect_count;
		}
		else
		{
			int index = asyncBatchReqTask->m_max_batch_size - (asyncBatchReqTask->m_batch_count - asyncBatchReqTask->m_need_connect_count) - 1;
			asyncBatchReqTask->m_batch_userpool[index]  = userpool;
			asyncBatchReqTask->m_batch_nodeindex[index] = nodeindex;
			asyncBatchReqTask->m_batch_slot[index]      = slot;

			Assert(slot->set4reuse);
			++asyncBatchReqTask->m_need_set4reuse_count;
			IncreaseUserPoolRefCount(userpool, __FILE__, __LINE__);
		}
		++asyncBatchReqTask->m_batch_count;
	}
	else
	{
		/* last request of the session */
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"dispatch last connection request!! pid:%d node:%s connection, request_num:%d req_seq:%d",agent->pid, userPoolNodeName(userpool), asyncBatchReqTask->m_batch_count, asyncBatchReqTask->req_seq);
		}

		if (asyncBatchReqTask->m_need_connect_count)
		{
			oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

			asyncBatchReqTask->m_batch_conninfo = (char **) palloc0(sizeof(char *) * asyncBatchReqTask->m_need_connect_count);
			asyncBatchReqTask->m_batch_connstr_buf = (BatchConnStr *) palloc0(sizeof(BatchConnStr) * asyncBatchReqTask->m_need_connect_count);
			asyncBatchReqTask->m_batch_conn = (NODE_CONNECTION **) palloc0(sizeof(NODE_CONNECTION *) * asyncBatchReqTask->m_need_connect_count);

			MemoryContextSwitchTo(oldcontext);
		}

		/*record request begin time*/
		if (PoolPrintStatTimeout > 0)
		{
			gettimeofday(&asyncBatchReqTask->start_time, NULL);
		}

		ret = dispatch_async_network_operation(asyncBatchReqTask);
		if (!ret)
		{
			elog(LOG, POOL_MGR_PREFIX"dispatch connection request failed!! pid:%d node:%s connection, request_num:%d req_seq:%d", agent->pid, userPoolNodeName(userpool), asyncBatchReqTask->m_batch_count, asyncBatchReqTask->req_seq);
			goto fail;
		}
	}

	return ret;

fail:
	{
		int i = 0;
		Oid node;
		int tmp_nodeindex;
		PGXCNodePoolSlot *tmp_slot;

		for (i = 0; i < asyncBatchReqTask->m_need_connect_count; i++)
		{
			tmp_slot = asyncBatchReqTask->m_batch_slot[i];
			if (tmp_slot->pgoptions)
				pfree(tmp_slot->pgoptions);
			pfree(tmp_slot);

			asyncBatchReqTask->m_batch_slot[i] = NULL;

			/* failed, decrease count */
			DecreaseUserPoolerSize(asyncBatchReqTask->m_batch_userpool[i],__FILE__,__LINE__);
		}

		i = asyncBatchReqTask->m_max_batch_size - (asyncBatchReqTask->m_batch_count - asyncBatchReqTask->m_need_connect_count);
		for (; i < asyncBatchReqTask->m_max_batch_size; i++)
		{
			tmp_nodeindex = asyncBatchReqTask->m_batch_nodeindex[i];
			tmp_slot      = asyncBatchReqTask->m_batch_slot[i];
			Assert(tmp_slot->set4reuse);

			tmp_slot->set4reuse = false;

			DecreaseUserPoolRefCount(asyncBatchReqTask->m_batch_userpool[i], __FILE__, __LINE__);

			if (isUserPoolCoord(tmp_slot->userPool))
			{
				node = agent->coord_conn_oids[tmp_nodeindex];
				agent->coord_connections[tmp_nodeindex] = NULL;
			}
			else
			{
				node = agent->dn_conn_oids[tmp_nodeindex];
				agent->dn_connections[tmp_nodeindex] = NULL;
			}

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"++++dispatch connection request failed!! pid:%d release slot_seq:%d set4reuse++++", agent->pid, tmp_slot->seqnum);
			}

			release_connection(agent, agent->pool, tmp_slot, tmp_nodeindex, node, false, isUserPoolCoord(tmp_slot->userPool));
		}
	};

	return ret;
}

static inline bool dispatch_batch_request(PGXCPoolAsyncBatchTask   *asyncBatchReqTask,
										  PGXCNodePoolSlot         *slot,
										  int                      node_type,
										  int32                    nodeindex,
										  bool                     dispatched)

{
	bool         ret = true;
	PoolAgent *agent = asyncBatchReqTask->agent;

	if (!dispatched)
	{
		if (PoolConnectDebugPrint)
		{
			if (slot)
			{
				elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async request cmd_type:%c node_type:%d nodeindex:%d connection nodename:%s backend_pid:%d into batch", agent->pid, asyncBatchReqTask->cmd, node_type, nodeindex, slot->node_name, slot->backend_pid);
			}
			else
			{
				elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch async request cmd_type:%c node_type:%d nodeindex:%d into batch", agent->pid, asyncBatchReqTask->cmd, node_type, nodeindex);
			}
		}

		/* just add task into batch handle */
		asyncBatchReqTask->m_batch_slot[asyncBatchReqTask->m_batch_count]      = slot;
		asyncBatchReqTask->m_batch_nodeindex[asyncBatchReqTask->m_batch_count] = nodeindex;
		asyncBatchReqTask->m_batch_node_type[asyncBatchReqTask->m_batch_count] = node_type;
		asyncBatchReqTask->m_batch_count++;
	}
	else
	{
		/* last request of the session */
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch last async request cmd_type:%c node_type:%d nodeindex:%d request_num:%d", agent->pid, asyncBatchReqTask->cmd, node_type, nodeindex, asyncBatchReqTask->m_batch_count);
		}

		ret = dispatch_async_network_operation(asyncBatchReqTask);
		if (!ret)
		{
			elog(LOG, POOL_MGR_PREFIX"pid:%d dispatch last async request failed!! cmd_type:%c node_type:%d nodeindex:%d request_num:%d", agent->pid, asyncBatchReqTask->cmd, node_type, nodeindex, asyncBatchReqTask->m_batch_count);
		}
	}

	return ret;
}

static void create_node_map(void)
{
	MemoryContext	oldcontext;
	PGXCMapNode     *node	  = NULL;
	NodeDefinition  *node_def = NULL;
	HASHCTL			hinfo;
	int				hflags;

	Oid			   *coOids;
	Oid			   *dnOids;
	int				numCo;
	int				numDn;
	int             nodeindex;
	bool			found;

	oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
	/* Init node hashtable */
	MemSet(&hinfo, 0, sizeof(hinfo));
	hflags = 0;

	hinfo.keysize = sizeof(Oid);
	hinfo.entrysize = sizeof(PGXCMapNode);
	hflags |= HASH_ELEM;

	hinfo.hcxt = PoolerMemoryContext;
	hflags |= HASH_CONTEXT;

	hinfo.hash = oid_hash;
	hflags |= HASH_FUNCTION;

	g_nodemap = hash_create("Node Map Hash",
							OPENTENBASE_MAX_DATANODE_NUMBER + OPENTENBASE_MAX_COORDINATOR_NUMBER,
							&hinfo, hflags);

	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	for (nodeindex = 0; nodeindex < numCo; nodeindex++)
	{
		node = (PGXCMapNode *) hash_search(g_nodemap, &coOids[nodeindex],
										    HASH_ENTER, &found);
		if (found)
		{
			elog(ERROR, POOL_MGR_PREFIX"node:%u duplicate in hash table", coOids[nodeindex]);
		}

		if (node)
		{
			node->node_type = PGXC_NODE_COORDINATOR;
			node->nodeidx = nodeindex;

			node_def = PgxcNodeGetDefinition(coOids[nodeindex]);
			snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
		}
	}

	for (nodeindex = 0; nodeindex < numDn; nodeindex++)
	{

		node = (PGXCMapNode *) hash_search(g_nodemap, &dnOids[nodeindex],
										    HASH_ENTER, &found);
		if (found)
		{
			elog(ERROR, POOL_MGR_PREFIX"node:%u duplicate in hash table", dnOids[nodeindex]);
		}

		if (node)
		{
			node->node_type = PGXC_NODE_DATANODE;
			node->nodeidx = nodeindex;

			node_def = PgxcNodeGetDefinition(dnOids[nodeindex]);
			snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
		}
	}

	MemoryContextSwitchTo(oldcontext);
	/* Release palloc'ed memory */
	pfree(coOids);
	pfree(dnOids);
}

static void refresh_node_map(void)
{
	PGXCMapNode 	*node;
	Oid 		   *coOids;
	Oid 		   *dnOids;
	NodeDefinition  *node_def = NULL;
	int 			numCo;
	int 			numDn;
	int 			nodeindex;
	bool			found;
#ifdef __OPENTENBASE__
	bool           inited = false;
#endif

	if (NULL == g_nodemap)
	{
		/* init node map */
		create_node_map();
#ifdef __OPENTENBASE__
		inited = true;
#endif
	}
#ifdef __OPENTENBASE__
	/* reset hashtab */
	if (!inited)
	{
		HASH_SEQ_STATUS scan_status;
		PGXCMapNode  *item;

		hash_seq_init(&scan_status, g_nodemap);
		while ((item = (PGXCMapNode *) hash_seq_search(&scan_status)) != NULL)
		{
			if (hash_search(g_nodemap, (const void *) &item->nodeoid,
							HASH_REMOVE, NULL) == NULL)
			{
				elog(ERROR, POOL_MGR_PREFIX"node map hash table corrupted");
			}
		}
	}
#endif
	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	for (nodeindex = 0; nodeindex < numCo; nodeindex++)
	{
		node = (PGXCMapNode *) hash_search(g_nodemap, &coOids[nodeindex],
											HASH_ENTER, &found);
		if (node)
		{
			node->nodeidx   = nodeindex;
			node->node_type = PGXC_NODE_COORDINATOR;

			node_def = PgxcNodeGetDefinition(coOids[nodeindex]);
			snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
		}
	}

	for (nodeindex = 0; nodeindex < numDn; nodeindex++)
	{

		node = (PGXCMapNode *) hash_search(g_nodemap, &dnOids[nodeindex],
											HASH_ENTER, &found);
		if (node)
		{
			node->nodeidx = nodeindex;
			node->node_type = PGXC_NODE_DATANODE;

			node_def = PgxcNodeGetDefinition(dnOids[nodeindex]);
			snprintf(node->node_name, NAMEDATALEN, "%s", NameStr(node_def->nodename));
		}
	}

	/* Release palloc'ed memory */
	pfree(coOids);
	pfree(dnOids);
}

static int32 get_node_index_by_nodeoid(Oid node)
{
	PGXCMapNode 	*entry = NULL;
	bool			found  = false;

	if (NULL == g_nodemap)
	{
		/* init node map */
		create_node_map();
	}

	entry = (PGXCMapNode *) hash_search(g_nodemap, &node, HASH_FIND, &found);
	if (found)
	{
		return entry->nodeidx;
	}
	else
	{
		elog(ERROR, POOL_MGR_PREFIX"query node type by oid:%u failed", node);
		return -1;
	}
}


static char* get_node_name_by_nodeoid(Oid node)
{
	PGXCMapNode 	*entry = NULL;
	bool			found  = false;

	if (NULL == g_nodemap)
	{
		/* init node map */
		create_node_map();
	}

	entry = (PGXCMapNode *) hash_search(g_nodemap, &node, HASH_FIND, &found);
	if (found)
	{
		return entry->node_name;
	}
	else
	{
		elog(LOG, POOL_MGR_PREFIX"query node name by oid:%u failed", node);
		return NULL;
	}
}

static inline bool pooler_is_async_task_done(void)
{
	int32 threadIndex = 0;

	if (!IsEmpty(g_AsynUtilityPipeRcver))
	{
		return false;
	}

	threadIndex = 0;
	while (threadIndex < MAX_SYNC_NETWORK_THREAD)
	{
		if (!IsEmpty(g_PoolConnControl.response[threadIndex]))
		{
			return false;
		}
		threadIndex++;
	}

	threadIndex = 0;
	while (threadIndex < MAX_SYNC_NETWORK_THREAD)
	{
		if (!IsEmpty(g_PoolSyncNetworkControl.response[threadIndex]))
		{
			return false;
		}
		threadIndex++;
	}
	return true;
}

static inline bool pooler_wait_for_async_task_done(void)
{
	bool  bdone    = false;
	int32 loop_num = 0;

	/* wait a little while */
	do
	{
		bdone = pooler_is_async_task_done();
		if (bdone)
		{
			break;
		}
		pg_usleep(1000L);
		pooler_sync_connections_to_nodepool();
		pooler_handle_sync_response_queue();
		loop_num++;
	}while (!bdone && loop_num < 100);

	return bdone;
}

static inline void pooler_async_task_start(PGXCPoolSyncNetWorkControl *control, int32 thread, int32 nodeindex, PGXCNodePoolSlot *slot, Oid nodeoid, int cmdtype)
{
	control->status[thread]      = PoolAsyncStatus_busy;
	control->nodeindex[thread]	 = nodeindex;
	control->start_stamp[thread] = time(NULL);
	control->cmdtype[thread]	 = cmdtype;

	record_slot_info(control, thread, slot, nodeoid);
}

static inline void pooler_async_task_done(PGXCPoolSyncNetWorkControl *control, int32 thread)
{
	control->status[thread] 	 = PoolAsyncStatus_idle;
	control->nodeindex[thread]	 = -1;
	control->start_stamp[thread] = 0;

	if (control->remote_ip[thread])
	{
		free(control->remote_ip[thread]);
		control->remote_ip[thread] = NULL;
	}

	control->remote_backend_pid[thread]  = -1;
	control->remote_port[thread]		 = -1;
	control->remote_nodeoid[thread]		 = InvalidOid;
	control->cmdtype[thread]			 = -1;
	if (control->message[thread])
	{
		free(control->message[thread]);
		control->message[thread] = NULL;
	}
}

static inline int32 pooler_async_task_pick_thread(PGXCPoolSyncNetWorkControl *control, int32 nodeindex)
{
	int32 loop_count   = 0;
	pg_time_t gap      = 0;
	pg_time_t now      = 0;
	pg_time_t stamp    = 0;
	static int32 thread_index = 0;

	/* Switch threads to achieve balanced task distribution */
	thread_index++;
	thread_index = thread_index % MAX_SYNC_NETWORK_THREAD;

	/* find a avaliable thread */
	now = time(NULL);
	while (loop_count < MAX_SYNC_NETWORK_THREAD)
	{

		/* for test */
		if (PoolConnectDebugPrint)
		{
			elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread loop_count=%d, thread_index=%d, control->status[thread_index]=%d, "
									 "request_pipelength=%d, response_pipelength=%d, "
									 "remote_ip:%s, remote_port:%d, remote_nodeoid:%d, remote_backend_pid:%d",
									         loop_count,
											 thread_index,
											 control->status[thread_index],
											 PipeLength(control->request[thread_index]),
											 PipeLength(control->response[thread_index]),
											 control->remote_ip[thread_index],
											 control->remote_port[thread_index],
											 control->remote_nodeoid[thread_index],
											 control->remote_backend_pid[thread_index]);
		}

		/* pipe is not full and in idle status, OK to return */
		if (PoolAsyncStatus_idle == control->status[thread_index] && !PipeIsFull(control->request[thread_index]))
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread use thread index:%d in idle status ", thread_index);
			}
			return thread_index;
		}
		else
		{
			stamp = control->start_stamp[thread_index];
			gap   = stamp ? (now - stamp) : 0;
			if (gap  >= PoolConnectTimeOut)
			{
				elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread thread index:%d got stuck for %ld seconds, cmdtype=%d, message=%s",
										 thread_index, gap, control->cmdtype[thread_index], control->message[thread_index]);
			}
			else if (!PipeIsFull(control->request[thread_index]))
			{
				/* not stuck too long and the pipe has space, choose it */
				if (PoolConnectDebugPrint && gap)
				{
					elog(LOG, POOL_MGR_PREFIX"pooler_async_task_pick_thread use thread index:%d in busy status, duration:%ld", thread_index, gap);
				}
				return thread_index;
			}
		}
		loop_count++;

		thread_index++;
		thread_index = thread_index % MAX_SYNC_NETWORK_THREAD;
	}

	/* can't find a avaliable thread*/
	if (PoolerStuckExit)
	{
		elog(FATAL, POOL_MGR_PREFIX"fail to pick a thread for operation nodeindex:%d, pooler exit", nodeindex);
	}
	else
	{
		elog(LOG, POOL_MGR_PREFIX"fail to pick a thread for operation nodeindex:%d", nodeindex);
	}
	return -1;
}

static inline void agent_increase_ref_count(PoolAgent *agent)
{
	 agent->ref_count++;
}

static inline void agent_decrease_ref_count(PoolAgent *agent)
{
	 agent->ref_count--;
}

static inline bool agent_can_destory(PoolAgent *agent)
{
	 return 0 == agent->ref_count;
}

static inline void agent_pend_destory(PoolAgent *agent)
{
	 elog(LOG, POOL_MGR_PREFIX"agent_pend_destory end, ref_count:%d, pid:%d", agent->ref_count, agent->pid);
	 agent->destory_pending = true;
}


static inline void agent_set_destory(PoolAgent *agent)
{
	 agent->destory_pending = false;
}

static inline bool agent_pending(PoolAgent *agent)
{
	 return agent->destory_pending;
}

static inline void agent_handle_pending_agent(PoolAgent *agent)
{
	if (agent)
	{
		if (agent_pending(agent) && agent_can_destory(agent))
		{
			destroy_pend_agent(agent);
		}
		else if (agent->release_pending_type != RELEASE_NOTHING && agent_can_destory(agent))
		{
			release_pend_agent_connections(agent);
		}
	}
}

static inline void pooler_init_sync_control(PGXCPoolSyncNetWorkControl *control)
{
	control->request        = (PGPipe**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(PGPipe*));
	control->response       = (PGPipe**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(PGPipe*));
	control->sem            = (ThreadSema*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(ThreadSema));
	control->nodeindex      = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
	control->status         = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
	control->start_stamp    = (pg_time_t*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(pg_time_t));

	control->remote_backend_pid = (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
	control->remote_nodeoid		= (Oid*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(Oid));
	control->remote_ip			= (char**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(char*));
	control->remote_port 		= (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
	control->message			= (char**)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(char*));
	control->cmdtype			= (int32*)palloc0(MAX_SYNC_NETWORK_THREAD * sizeof(int32));
}
/* generate a sequence number for slot */
static inline int32 pooler_get_slot_seq_num(void)
{
	return g_Slot_Seqnum++;
}
/* get connection acquire sequence number */
static inline int32 pooler_get_connection_acquire_num(void)
{
	return g_Connection_Acquire_Count++;
}

/* bitmap index occupytation management */
BitmapMgr *BmpMgrCreate(uint32 objnum)
{
	BitmapMgr *mgr = NULL;

	mgr = (BitmapMgr*)palloc0(sizeof(BitmapMgr));

    mgr->m_fsmlen  = DIVIDE_UP(objnum, BITS_IN_LONGLONG);
    mgr->m_nobject = mgr->m_fsmlen * BITS_IN_LONGLONG;  /* align the object number to 64 */

    mgr->m_fsm = palloc(sizeof(uint64) * mgr->m_fsmlen);

    /* zero the FSM memory */
    memset(mgr->m_fsm, 0X00, sizeof(uint64) * mgr->m_fsmlen);

	mgr->m_version  = INIT_VERSION;
	return mgr;
};

/* find an unused index and return it */
int BmpMgrAlloc(BitmapMgr *mgr)
{
	uint32  i       = 0;
	uint32  j 		= 0;
	uint32  k		= 0;
    int     offset  = 0;
    uint8   *ucaddr = NULL;
    uint32  *uiaddr = NULL;

    /* sequencial scan the FSM map */
    for (i = 0; i < mgr->m_fsmlen; i++)
    {
        /* got free space */
        if (mgr->m_fsm[i] < MAX_UINT64)
        {
            /* first word has free space */
            uiaddr = (uint32*)&(mgr->m_fsm[i]);
            if (uiaddr[0] < MAX_UINT32)
            {
                ucaddr = (uint8*)&uiaddr[0];
                offset = 0;
            }
            else
            {
                /* in second word */
                ucaddr = (uint8*)&uiaddr[1];
                offset = BITS_IN_WORD;
            }

            /* find free space */
            for (j = 0; j < sizeof(uint32); j++)
            {
                if (ucaddr[j] < MAX_UINT8)
                {
                    for (k = 0; k < BITS_IN_BYTE; k++)
                    {
                        if (BIT_CLEAR(ucaddr[j], k))
                        {
                            SET_BIT(ucaddr[j], k);
							mgr->m_version++;
                            return offset + i * BITS_IN_LONGLONG + j * BITS_IN_BYTE + k;
                        }
                    }
                }
            }
        }
    }

    /* out of space */
    return -1;
}

/* free index */
void BmpMgrFree(BitmapMgr *mgr, int index)
{
    uint32  fsmidx = 0;
    uint32  offset = 0;
    uint8  *pAddr  = NULL;

    if (index < 0 || (uint32)index >= mgr->m_nobject)
    {
        elog(PANIC, POOL_MGR_PREFIX"invalid index:%d", index);
    }

    offset = index % BITS_IN_LONGLONG;
    fsmidx = index / BITS_IN_LONGLONG;

    pAddr  = (uint8*)&(mgr->m_fsm[fsmidx]);
    CLEAR_BIT(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE);
    mgr->m_version++;
}

bool BmpMgrHasIndexAndClear(BitmapMgr *mgr, int index)
{
    uint32  fsmidx = 0;
    uint32  offset = 0;
    uint8  *pAddr  = NULL;

    if (index < 0 || (uint32)index >= mgr->m_nobject)
    {
        elog(PANIC, POOL_MGR_PREFIX"invalid index:%d", index);
    }

    offset = index % BITS_IN_LONGLONG;
    fsmidx = index / BITS_IN_LONGLONG;

    pAddr  = (uint8*)&(mgr->m_fsm[fsmidx]);
    if( BIT_SET(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE))
    {
        CLEAR_BIT(pAddr[offset / BITS_IN_BYTE], offset % BITS_IN_BYTE);
        mgr->m_version++;
        return true;
    }
    return false;
}


uint32 BmpMgrGetVersion(BitmapMgr *mgr)
{
	return mgr->m_version;
}

int BmpMgrGetUsed(BitmapMgr *mgr, int32 *indexes, int32 indexlen)
{
	uint32   u64num  = 0;
	uint32   u32num  = 0;
	uint32   u8num   = 0;
	uint32   ubitnum = 0;
	int32    number  = 0;
    uint8   *ucaddr  = NULL;
    uint32  *uiaddr  = NULL;

    if (indexlen != mgr->m_nobject)
	{
		return -1;
	}

    /* scan the array */
    for (u64num = 0; u64num < mgr->m_fsmlen; u64num++)
    {
        if (mgr->m_fsm[u64num])
        {
            uiaddr = (uint32*)&(mgr->m_fsm[u64num]);
			for (u32num = 0; u32num < sizeof(uint64)/sizeof(uint32); u32num++)
			{
				if (uiaddr[u32num])
				{
		            ucaddr = (uint8*)&uiaddr[u32num];

		            for (u8num = 0; u8num < sizeof(uint32); u8num++)
		            {
		                if (ucaddr[u8num])
		                {
		                    for (ubitnum = 0; ubitnum < BITS_IN_BYTE; ubitnum++)
		                    {
		                        if (BIT_SET(ucaddr[u8num], ubitnum))
		                        {
		                            indexes[number] = u64num * BITS_IN_LONGLONG + BITS_IN_WORD * u32num + u8num * BITS_IN_BYTE + ubitnum;
									number++;
		                        }
		                    }
		                }
		            }
				}
			}
        }
    }

    return number;
}

static void record_slot_info(PGXCPoolSyncNetWorkControl *control, int32 thread, PGXCNodePoolSlot *slot, Oid nodeoid)
{
	struct pg_conn* conn = NULL;

	if (!control || thread < 0)
		return;

	if (slot && slot->conn)
	{
		conn = (struct pg_conn*)slot->conn;
		control->remote_backend_pid[thread] = conn->be_pid;
		control->remote_port[thread]		= atoi(conn->pgport);
		control->remote_nodeoid[thread]		= nodeoid;

		if (conn->pghostaddr && conn->pghostaddr[0] != '\0')
		{
			control->remote_ip[thread] = strdup(conn->pghostaddr);
		}
		else if (conn->pghost && conn->pghost[0] != '\0')
		{
			control->remote_ip[thread] = strdup(conn->pghost);
		}
	}
}

/*
 * Ping an UNHEALTHY node and if it succeeds, update SHARED node
 * information
 */
static void
TryPingUnhealthyNode(Oid nodeoid)
{
	int status;
	NodeDefinition *nodeDef;
	char connstr[MAXPGPATH * 2 + 256];

	nodeDef = PgxcNodeGetDefinition(nodeoid);
	if (nodeDef == NULL)
	{
		/* No such definition, node dropped? */
		elog(DEBUG1, "Could not find node (%u) definition,"
			 " skipping health check", nodeoid);
		return;
	}
	if (nodeDef->nodeishealthy)
	{
		/* hmm, can this happen? */
		elog(DEBUG1, "node (%u) healthy!"
			 " skipping health check", nodeoid);
		return;
	}

	elog(LOG, "node (%s:%u) down! Trying ping",
		 NameStr(nodeDef->nodename), nodeoid);
	sprintf(connstr,
			"host=%s port=%d dbname=%s", NameStr(nodeDef->nodehost),
			nodeDef->nodeport, PoolConnectDB);
	status = PGXCNodePing(connstr);
	if (status != 0)
	{
		pfree(nodeDef);
		return;
	}

	elog(DEBUG1, "Node (%s) back online!", NameStr(nodeDef->nodename));
	if (!PgxcNodeUpdateHealth(nodeoid, true))
		elog(WARNING, "Could not update health status of node (%s)",
			 NameStr(nodeDef->nodename));
	else
		elog(LOG, "Health map updated to reflect HEALTHY node (%s)",
			 NameStr(nodeDef->nodename));
	pfree(nodeDef);

	return;
}

/*
 * Check if a node is indeed down and if it is update its UNHEALTHY
 * status, return healthy status
 */
bool
PoolPingNodeRecheck(Oid nodeoid)
{
	int status;
	NodeDefinition *nodeDef;
	char connstr[MAXPGPATH * 2 + 256];
	bool	healthy = false;

	nodeDef = PgxcNodeGetDefinition(nodeoid);
	if (nodeDef == NULL)
	{
		/* No such definition, node dropped? */
		elog(DEBUG1, "Could not find node (%u) definition,"
			 " skipping health check", nodeoid);
		return healthy;
	}

	sprintf(connstr,
			"host=%s port=%d dbname=%s", NameStr(nodeDef->nodehost),
			nodeDef->nodeport, PoolConnectDB);
	status = PGXCNodePing(connstr);
	healthy = (status == 0);

	/* if no change in health bit, return */
	if (healthy == nodeDef->nodeishealthy)
	{
		pfree(nodeDef);
		return healthy;
	}

	if (!PgxcNodeUpdateHealth(nodeoid, healthy))
		elog(WARNING, "Could not update health status of node (%s)",
			 NameStr(nodeDef->nodename));
	else
		elog(LOG, "Health map updated to reflect (%s) node (%s)",
			 healthy ? "HEALTHY" : "UNHEALTHY", NameStr(nodeDef->nodename));
	pfree(nodeDef);

	return healthy;
}

void
PoolAsyncPingNodes()
{
	Oid				*coOids = NULL;
	Oid				*dnOids = NULL;
	bool			*coHealthMap = NULL;
	bool			*dnHealthMap = NULL;
	int				numCo;
	int				numDn;
	int				i;

	/* pipe is full, no need to continue */
	if (IS_ASYNC_PIPE_FULL())
	{
		return;
	}

	coOids = (Oid*)palloc(sizeof(Oid) * OPENTENBASE_MAX_COORDINATOR_NUMBER);
    if (coOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coOids")));
    }

	dnOids = (Oid*)palloc(sizeof(Oid) * OPENTENBASE_MAX_DATANODE_NUMBER);
    if (dnOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnOids")));
    }

	coHealthMap = (bool*)palloc(sizeof(bool) * OPENTENBASE_MAX_COORDINATOR_NUMBER);
    if (coHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coHealthMap")));
    }

	dnHealthMap = (bool*)palloc(sizeof(bool) * OPENTENBASE_MAX_DATANODE_NUMBER);
    if (dnHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnHealthMap")));
	}

	PgxcNodeGetHealthMap(coOids, dnOids, &numCo, &numDn,
						 coHealthMap, dnHealthMap);

	/*
	 * Find unhealthy datanodes and try to re-ping them
	 */
	for (i = 0; i < numDn; i++)
	{
		if (!dnHealthMap[i])
		{
			Oid	 nodeoid = dnOids[i];
			pooler_async_ping_node(nodeoid);
		}
	}
	/*
	 * Find unhealthy coordinators and try to re-ping them
	 */
	for (i = 0; i < numCo; i++)
	{
		if (!coHealthMap[i])
		{
			Oid	 nodeoid = coOids[i];
			pooler_async_ping_node(nodeoid);
		}
	}

	if (coOids)
	{
		pfree(coOids);
		coOids = NULL;
	}

	if (dnOids)
	{
		pfree(dnOids);
		dnOids = NULL;
	}

	if (coHealthMap)
	{
		pfree(coHealthMap);
		coHealthMap = NULL;
	}

	if (dnHealthMap)
	{
		pfree(dnHealthMap);
		dnHealthMap = NULL;
	}
}


/*
 * Ping UNHEALTHY nodes as part of the maintenance window
 */
void
PoolPingNodes()
{
	Oid				*coOids = NULL;
	Oid				*dnOids = NULL;
	bool			*coHealthMap = NULL;
	bool			*dnHealthMap = NULL;
	int				numCo;
	int				numDn;
	int				i;

	coOids = (Oid*)palloc(sizeof(Oid) * OPENTENBASE_MAX_COORDINATOR_NUMBER);
    if (coOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coOids")));
    }

	dnOids = (Oid*)palloc(sizeof(Oid) * OPENTENBASE_MAX_DATANODE_NUMBER);
    if (dnOids == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnOids")));
    }

	coHealthMap = (bool*)palloc(sizeof(bool) * OPENTENBASE_MAX_COORDINATOR_NUMBER);
    if (coHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for coHealthMap")));
    }

	dnHealthMap = (bool*)palloc(sizeof(bool) * OPENTENBASE_MAX_DATANODE_NUMBER);
    if (dnHealthMap == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory for dnHealthMap")));
	}

	PgxcNodeGetHealthMap(coOids, dnOids, &numCo, &numDn,
						 coHealthMap, dnHealthMap);

	/*
	 * Find unhealthy datanodes and try to re-ping them
	 */
	for (i = 0; i < numDn; i++)
	{
		if (!dnHealthMap[i])
		{
			Oid	 nodeoid = dnOids[i];
			TryPingUnhealthyNode(nodeoid);
		}
	}
	/*
	 * Find unhealthy coordinators and try to re-ping them
	 */
	for (i = 0; i < numCo; i++)
	{
		if (!coHealthMap[i])
		{
			Oid	 nodeoid = coOids[i];
			TryPingUnhealthyNode(nodeoid);
		}
	}

	if (coOids)
	{
		pfree(coOids);
		coOids = NULL;
	}

	if (dnOids)
	{
		pfree(dnOids);
		dnOids = NULL;
	}

	if (coHealthMap)
	{
		pfree(coHealthMap);
		coHealthMap = NULL;
	}

	if (dnHealthMap)
	{
		pfree(dnHealthMap);
		dnHealthMap = NULL;
	}
}


static void
handle_abort(PoolAgent * agent, StringInfo s)
{
	int		len;
	int	   *pids;
	const char *database = NULL;
	const char *user_name = NULL;

	pool_getmessage(&agent->port, s, 0);
	len = pq_getmsgint(s, 4);
	if (len > 0)
		database = pq_getmsgbytes(s, len);

	len = pq_getmsgint(s, 4);
	if (len > 0)
		user_name = pq_getmsgbytes(s, len);

	pq_getmsgend(s);

	pids = abort_pids(&len, agent->pid, database, user_name);

	pool_sendpids(&agent->port, pids, len, NULL, 0, 0);
	if (pids)
		pfree(pids);
}

static void
handle_connect(PoolAgent * agent, StringInfo s)
{
	int	len;
	const char *database = NULL;
	const char *user_name = NULL;
	const char *pgoptions = NULL;

	pool_getmessage(&agent->port, s, 0);
	agent->pid = pq_getmsgint(s, 4);

	agent->user_id = pq_getmsgint(s, 4);

	len = pq_getmsgint(s, 4);
	database = pq_getmsgbytes(s, len);

	len = pq_getmsgint(s, 4);
	user_name = pq_getmsgbytes(s, len);

	len = pq_getmsgint(s, 4);
	pgoptions = pq_getmsgbytes(s, len);

	/*
	 * Coordinator pool is not initialized.
	 * With that it would be impossible to create a Database by default.
	 */
	agent_init(agent, database, user_name, pgoptions);
	pq_getmsgend(s);
}

static void
handle_clean_connection(PoolAgent * agent, StringInfo s)
{
	int i, len, res;
	int	datanodecount, coordcount;
	const char *database = NULL;
	const char *user_name = NULL;
	List	   *nodelist = NIL;

	pool_getmessage(&agent->port, s, 0);

	/* It is possible to clean up only datanode connections */
	datanodecount = pq_getmsgint(s, 4);
	for (i = 0; i < datanodecount; i++)
	{
		/* Translate index to Oid */
		int index = pq_getmsgint(s, 4);
		Oid node = agent->dn_conn_oids[index];
		nodelist = lappend_oid(nodelist, node);
	}

	/* It is possible to clean up only coordinator connections */
	coordcount = pq_getmsgint(s, 4);
	for (i = 0; i < coordcount; i++)
	{
		/* Translate index to Oid */
		int index = pq_getmsgint(s, 4);
		Oid node = agent->coord_conn_oids[index];
		nodelist = lappend_oid(nodelist, node);
	}

	len = pq_getmsgint(s, 4);
	if (len > 0)
		database = pq_getmsgbytes(s, len);

	len = pq_getmsgint(s, 4);
	if (len > 0)
		user_name = pq_getmsgbytes(s, len);

	pq_getmsgend(s);

	/* Clean up connections here */
	res = clean_connection(nodelist, database, user_name);

	list_free(nodelist);

	/* Send success result */
	pool_sendres(&agent->port, res, NULL, 0, true);
}

static void
handle_get_connections(PoolAgent * agent, StringInfo s)
{
	int		i;
	int	   *fds = NULL;
	int    *pids = NULL;
	int 	ret;
	int		datanodecount = 0;
	int		coordcount    = 0;
	List   *datanodelist = NIL;
	List   *coordlist = NIL;
	int 	connect_num = 0;
	/*
	 * Length of message is caused by:
	 * - Message header = 4bytes
	 * - List of Datanodes = NumPoolDataNodes * 4bytes (max)
	 * - List of Coordinators = NumPoolCoords * 4bytes (max)
	 * - Number of Datanodes sent = 4bytes
	 * - Number of Coordinators sent = 4bytes
	 * It is better to send in a same message the list of Co and Dn at the same
	 * time, this permits to reduce interactions between postmaster and pooler
	 */
	pool_getmessage(&agent->port, s, 4 * agent->num_dn_connections + 4 * agent->num_coord_connections + 4 + sizeof(int) * OPENTENBASE_NODE_TYPE_COUNT + 4);
	agent->rcv_seq = pq_getmsgint(s, 4);
	datanodecount = pq_getmsgint(s, 4);
	for (i = 0; i < datanodecount; i++)
	{
		datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"backend required %d datanode connections, pid:%d, rcv seq:%u", datanodecount, agent->pid, agent->rcv_seq);
	}

	coordcount = pq_getmsgint(s, 4);
	/* It is possible that no Coordinators are involved in the transaction */
	for (i = 0; i < coordcount; i++)
	{
		coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));
	}

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"backend required %d coordinator connections, pid:%d", coordcount, agent->pid);
	}
	pq_getmsgend(s);

	if(!is_pool_locked)
	{

		/*
		 * In case of error agent_acquire_connections will log
		 * the error and return -1
		 */
		ret = agent_acquire_connections(agent, datanodelist, coordlist, &connect_num, &fds, &pids);
		/* async acquire connection will be done in parallel threads */
		if (0 == ret && fds && pids)
		{
			int ret1, ret2;

			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"return %d database connections pid:%d, seq:%u", connect_num, agent->pid, agent->rcv_seq);
			}
			ret1 = pool_sendfds(&agent->port, fds, fds ? connect_num : 0, NULL, 0, agent->rcv_seq);
			if (fds)
			{
				pfree(fds);
				fds = NULL;
			}

			/*
			 * Also send the PIDs of the remote backend processes serving
			 * these connections
			 */
			ret2 = pool_sendpids(&agent->port, pids, pids ? connect_num : 0, NULL, 0, agent->rcv_seq);
			if (pids)
			{
				pfree(pids);
				pids = NULL;
			}

			if (!ret1 && !ret2)
				move_connections_into_used_pool(agent, datanodelist);
			else
			{
				ListCell  *nodelist_item = NULL;
				/* reset need_move_to_usedpool */
				foreach(nodelist_item, datanodelist)
				{
					int	node = lfirst_int(nodelist_item);
					if (agent->dn_connections[node])
						agent->dn_connections[node]->need_move_to_usedpool = false;
				}
			}

			list_free(datanodelist);
			list_free(coordlist);

			if (PoolPrintStatTimeout > 0)
			{
				g_pooler_stat.client_request_from_hashtab++;
			}
		}
		else if (0 == ret)
		{
			if (PoolConnectDebugPrint)
			{
				elog(LOG, POOL_MGR_PREFIX"cannot get conn immediately. thread will do the work. pid:%d, seq:%u", agent->pid, agent->rcv_seq);
			}
			if (PoolPrintStatTimeout > 0)
			{
				g_pooler_stat.client_request_from_thread++;
			}
		}
		else
		{
			if (fds)
			{
				pfree(fds);
				fds = NULL;
			}
			if (pids)
			{
				pfree(pids);
				pids = NULL;
			}
			pool_sendfds(&agent->port, NULL, 0, NULL, 0, agent->rcv_seq);

			/* send the error msg */
			pool_putmessage_without_msgtype(&agent->port, "Pooler internal error.",
											strlen("Pooler internal error.") + 1);
			pool_flush(&agent->port);
			elog(LOG, POOL_MGR_PREFIX"error happen when agent_acquire_connections. pid:%d, ret=%d, seq:%u", agent->pid, ret, agent->rcv_seq);
		}
	}
	else
	{
		/* pooler is locked, just refuse the 'g' request */
		elog(WARNING,POOL_MGR_PREFIX"Pool connection get request cannot run during pool lock, seq:%u", agent->rcv_seq);
		list_free(datanodelist);
		list_free(coordlist);
		pool_sendfds(&agent->port, NULL, 0, NULL, 0, agent->rcv_seq);

		/* send the error msg */
		pool_putmessage_without_msgtype(&agent->port, "Pooler is locked now.", strlen("Pooler is locked now.") + 1);
		pool_flush(&agent->port);
	}
}

static void
handle_query_cancel(PoolAgent * agent, StringInfo s)
{
	int		i;
	int		datanodecount = 0;
	int		coordcount    = 0;
	List   *datanodelist  = NIL;
	List   *pidlist       = NIL;
	List   *coordlist     = NIL;
	int 	res;
	int     signal;

	/*
	 * Length of message is caused by:
	 * - Message header = 4bytes
	 * - Number of Datanodes sent = 4bytes
	 * - Number of Coordinators sent = 4bytes
	 * - List of Datanodes and pids = MaxConnections * 8bytes
	 * - List of Coordinators = NumPoolCoords * 4bytes (max)
	 * - signal type = 4bytes
	 */
	pool_getmessage(&agent->port, s, 4 + 4 * (MaxConnections * 2 + agent->num_coord_connections + OPENTENBASE_NODE_TYPE_COUNT + 1));

	datanodecount = pq_getmsgint(s, 4);
	for (i = 0; i < datanodecount; i++)
		datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));

	for (i = 0; i < datanodecount; i++)
		pidlist = lappend_int(pidlist, pq_getmsgint(s, 4));

	coordcount = pq_getmsgint(s, 4);
	/* It is possible that no Coordinators are involved in the transaction */
	for (i = 0; i < coordcount; i++)
		coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));

	/* get signal type */
	signal = pq_getmsgint(s, 4);
	pq_getmsgend(s);

	res = cancel_query_on_connections(agent, datanodelist, pidlist, coordlist, signal);
	list_free(datanodelist);
	list_free(pidlist);
	list_free(coordlist);

	/* just send result. when res > 0, thread will exec pool_sendres. */
	if (res <= 0)
		pool_sendres(&agent->port, res, NULL, 0, true);
}

static void
handle_release_connections(PoolAgent * agent, StringInfo s)
{
	int		i;
	int		datanodecount = 0;
	int		coordcount    = 0;
	List   *datanodelist  = NIL;
	List   *dn_pidlist    = NIL;
	List   *coordlist     = NIL;
	int     force;
	MemoryContext oldcontext;

	/*
	 * Length of message is caused by:
	 * - Message header = 4bytes
	 * - Number of Datanodes sent = 4bytes
	 * - Number of Coordinators sent = 4bytes
	 * - List of Datanodes and pids = MaxConnections * 8bytes
	 * - List of Coordinators = NumPoolCoords * 4bytes (max)
	 * - force = 4bytes
	 */
	pool_getmessage(&agent->port, s, 4 + 4 * (MaxConnections * 2 + agent->num_coord_connections + OPENTENBASE_NODE_TYPE_COUNT + 1));

	oldcontext = MemoryContextSwitchTo(agent->mcxt);

	datanodecount = pq_getmsgint(s, 4);
	for (i = 0; i < datanodecount; i++)
		datanodelist = lappend_int(datanodelist, pq_getmsgint(s, 4));

	for (i = 0; i < datanodecount; i++)
		dn_pidlist = lappend_int(dn_pidlist, pq_getmsgint(s, 4));

	coordcount = pq_getmsgint(s, 4);
	/* It is possible that no Coordinators are involved in the transaction */
	for (i = 0; i < coordcount; i++)
		coordlist = lappend_int(coordlist, pq_getmsgint(s, 4));

	force = pq_getmsgint(s, 4);
	pq_getmsgend(s);

	if (PoolConnectDebugPrint)
	{
		elog(LOG, POOL_MGR_PREFIX"++++handle_release_connections++++, pid:%d, dn connections count %d, cn connections count %d, force %d ", agent->pid, datanodecount, coordcount, force);
	}

	/*
	 * similar to agent destroy, it requires check whether we
	 * can release the connections now, perhaps some tasks are
	 * still being processed in the background and cannot be
	 * directly released the slot
	 */
	if (!agent_can_destory(agent))
	{
		agent->release_pending_type = RELEASE_PARTITAL;
		agent->release_ctx = (ReleaseContext *)palloc(sizeof(ReleaseContext));
		agent->release_ctx->datanodelist = datanodelist;
		agent->release_ctx->dn_pidlist = dn_pidlist;
		agent->release_ctx->coordlist = coordlist;
		agent->release_ctx->force = force;
		MemoryContextSwitchTo(oldcontext);
		elog(LOG, POOL_MGR_PREFIX"pend release connections, ref_count:%d, pid:%d, dn connections count %d, cn connections count %d, force %d ", agent->ref_count, agent->pid, datanodecount, coordcount, force);
		return;
	}

	MemoryContextSwitchTo(oldcontext);

	agent_release_connections(agent, datanodelist, dn_pidlist, coordlist, force);

	list_free(datanodelist);
	list_free(dn_pidlist);
	list_free(coordlist);
}

static bool
remove_all_agent_references(Oid nodeoid)
{
	int i, j;
	bool res = true;

	/*
	 * Identify if it's a coordinator or datanode first
	 * and get its index
	 */
	for (i = 0; i < agentCount; i++)
	{
		bool found = false;

		PoolAgent *agent = poolAgents[agentIndexes[i]];
		for (j = 0; j < agent->num_dn_connections; j++)
		{
			if (agent->dn_conn_oids[j] == nodeoid)
			{
				found = true;
				break;
			}
		}
		if (found)
		{
			PGXCNodePoolSlot *slot = agent->dn_connections[j];
			if (slot)
				release_connection(agent, agent->pool, slot, j, agent->dn_conn_oids[j], false, false);
			agent->dn_connections[j] = NULL;

			if (agent->dn_connections_used_pool)
			{
				HASH_SEQ_STATUS hseq_status;
				DnConnection   *dn_conn;

				hash_seq_init(&hseq_status, agent->dn_connections_used_pool);
				while ((dn_conn = (DnConnection *) hash_seq_search(&hseq_status)))
				{
					if (dn_conn->key.node == j)
					{
						PGXCNodePoolSlot *slot = dn_conn->slot;
						hash_search(agent->dn_connections_used_pool, &dn_conn->key, HASH_REMOVE, NULL);
						/*
						 * Release connection.
						 */
						if (slot)
						{
							if (PoolConnectDebugPrint)
							{
								elog(LOG, POOL_MGR_PREFIX"++++agent_release_all_connections pid:%d release slot_seq:%d nodename:%s backend_pid:%d++++", agent->pid, slot->seqnum, slot->node_name, slot->backend_pid);
							}
							release_connection(agent, agent->pool, slot, dn_conn->key.node, agent->dn_conn_oids[dn_conn->key.node], false, false);
						}
					}
				}
			}
		}
		else
		{
			for (j = 0; j < agent->num_coord_connections; j++)
			{
				if (agent->coord_conn_oids[j] == nodeoid)
				{
					found = true;
					break;
				}
			}
			if (found)
			{
				PGXCNodePoolSlot *slot = agent->coord_connections[j];
				if (slot)
					release_connection(agent, agent->pool, slot, j, agent->coord_conn_oids[j], true, true);
				agent->coord_connections[j] = NULL;
			}
			else
			{
				elog(LOG, "Node not found! (%u)", nodeoid);
				res = false;
			}
		}
	}
	return res;
}


/*
 * refresh_database_pools
 *		refresh information for all database pools
 *
 * Connection information refresh concerns all the database pools.
 * A database pool is refreshed as follows for each remote node:
 *
 * - node pool is deleted if its port or host information is changed.
 *   Subsequently all its connections are dropped.
 *
 * If any other type of activity is found, we error out.
 *
 * XXX I don't see any cases that would error out. Isn't the comment
 * simply obsolete?
 */
static int
refresh_database_pools(PoolAgent *agent)
{
	DatabasePool *databasePool;
	Oid			   *coOids;
	Oid			   *dnOids;
	int				numCo;
	int				numDn;
	int 			res = POOL_REFRESH_SUCCESS;

	elog(LOG, "Refreshing database pools");

	/*
	 * re-check if agent's node information matches current contents of the
	 * shared memory table.
	 */
	PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

	if (agent->num_coord_connections != numCo ||
			agent->num_dn_connections != numDn ||
			memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
			memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid)))
		res = POOL_REFRESH_FAILED;

	/* Release palloc'ed memory */
	pfree(coOids);
	pfree(dnOids);

	/*
	 * Scan the list and destroy any altered pool. They will be recreated
	 * upon subsequent connection acquisition.
	 */
	databasePool = databasePoolsHead;
	while (res == POOL_REFRESH_SUCCESS && databasePool)
	{
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		hash_seq_init(&hseq_status, databasePool->nodePools);
		while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
		{
			char *connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool, NULL);

			/*
			 * Since we re-checked the numbers above, we should not get
			 * the case of an ADDED or a DELETED node here..
			 */
			if (connstr_chk == NULL)
			{
				elog(LOG, "Found a deleted node (%u)", nodePool->nodeoid);
				hash_seq_term(&hseq_status);
				res = POOL_REFRESH_FAILED;
				break;
			}

			if (strcmp(connstr_chk, nodePool->connstr))
			{
				elog(LOG, "Found an altered node (%u)", nodePool->nodeoid);
				/*
				 * Node has been altered. First remove
				 * all references to this node from ALL the
				 * agents before destroying it..
				 */
				if (!remove_all_agent_references(nodePool->nodeoid))
				{
					res = POOL_REFRESH_FAILED;
					break;
				}

				destroy_node_pool(nodePool);
				if ((--nodePool->nodeInfo->ref_count) == 0)
					hash_search(g_NodeSingletonInfo, &nodePool->nodeoid, HASH_REMOVE, NULL);
				hash_search(databasePool->nodePools, &nodePool->nodeoid,HASH_REMOVE, NULL);
			}

			if (connstr_chk)
				pfree(connstr_chk);
		}

		databasePool = databasePool->next;
	}
	return res;
}

/*
 * Refresh connection data in pooler and drop connections for those nodes
 * that have changed. Thus, this operation is less destructive as compared
 * to PoolManagerReloadConnectionInfo and should typically be called when
 * NODE ALTER has been performed
 */
int
PoolManagerRefreshConnectionInfo(void)
{
	int res;

	HOLD_POOLER_RELOAD();

	Assert(poolHandle);
	PgxcNodeListAndCountWrapTransaction(false, false);
	pool_putmessage(&poolHandle->port, 'R', NULL, 0);
	pool_flush(&poolHandle->port);

	res = pool_recvres(&poolHandle->port, true);

	RESUME_POOLER_RELOAD();

	if (res == POOL_CHECK_SUCCESS)
		return true;

	return false;
}


static void reset_pooler_statistics(void)
{
	g_pooler_stat.acquire_conn_from_hashtab = 0;
	g_pooler_stat.acquire_conn_from_hashtab_and_set_reuse = 0;
	g_pooler_stat.acquire_conn_from_thread = 0;
	g_pooler_stat.client_request_conn_total = 0;
	g_pooler_stat.client_request_from_hashtab = 0;
	g_pooler_stat.client_request_from_thread = 0;
	g_pooler_stat.acquire_conn_time = 0;
}

static void print_pooler_statistics(void)
{
	static time_t last_print_stat_time = 0;
	time_t 		  now ;
	double 		  timediff;

	if (PoolPrintStatTimeout > 0)
	{
		if (0 == last_print_stat_time)
		{
			last_print_stat_time = time(NULL);
		}

		now = time(NULL);
		timediff = difftime(now, last_print_stat_time);

		if (timediff >= PoolPrintStatTimeout)
		{
			last_print_stat_time = time(NULL);

			elog(LOG, "[pooler stat]client_request_conn_total=%d, client_request_from_hashtab=%d, "
					  "client_request_from_thread=%d, acquire_conn_from_hashtab=%d, "
			          "acquire_conn_from_hashtab_and_set_reuse=%d, "
			          "acquire_conn_from_thread=%d, acquire_conn_time=%lu, "
			          "each_client_conn_request_cost_time=%f us",
				  g_pooler_stat.client_request_conn_total,
				  g_pooler_stat.client_request_from_hashtab,
				  g_pooler_stat.client_request_from_thread,
				  g_pooler_stat.acquire_conn_from_hashtab,
				  g_pooler_stat.acquire_conn_from_hashtab_and_set_reuse,
				  g_pooler_stat.acquire_conn_from_thread,
				  g_pooler_stat.acquire_conn_time,
				  (double)g_pooler_stat.acquire_conn_time / (double)g_pooler_stat.client_request_conn_total);
			reset_pooler_statistics();
		}
	}
}


static void record_task_message(PGXCPoolSyncNetWorkControl* control, int32 thread, char* message)
{
	if (control->message[thread])
	{
		free(control->message[thread]);
		control->message[thread] = NULL;
	}
	control->message[thread] = strdup(message);
}


static void record_time(struct timeval start_time, struct timeval end_time)
{
	unsigned  long diff = 0;

	if (PoolPrintStatTimeout <= 0)
		return;

	if (start_time.tv_sec == 0 && start_time.tv_usec == 0)
		return;

	diff = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
	g_pooler_stat.acquire_conn_time += diff;
}

static inline void RebuildAgentIndex(void)
{
	if (mgrVersion != BmpMgrGetVersion(poolAgentMgr))
	{
		usedAgentSize= BmpMgrGetUsed(poolAgentMgr, agentIndexes, poolAgentSize);
		if (usedAgentSize != agentCount)
		{
			elog(PANIC, POOL_MGR_PREFIX"invalid BmpMgr status");
		}
		mgrVersion = BmpMgrGetVersion(poolAgentMgr);
	}
}

static bool connection_need_pool(char *filter, const char *name)
{
	/* format "xxx:yyy" */
	#define FILTER_ELEMENT_SEP ","
	#define TEMP_PATH_LEN 1024
	char    *token   = NULL;
	char    *next   = NULL;
	char    str[TEMP_PATH_LEN] = {0};

	/* no filter, need pool */
	if (NULL == filter)
	{
		return true;
	}

	/* no name, no  need pool */
	if (NULL == name)
	{
		return false;
	}

	snprintf(str, TEMP_PATH_LEN, "%s", filter);
	token = str;
	next  = token;
	do
	{
		token = next;
		next = strstr(token, FILTER_ELEMENT_SEP);
		if (NULL == next)
		{
			/* not pool if listed */
			if (0 == strcmp(token, name))
			{
				return false;
			}
			else
			{
				return true;
			}
		}
		*next = '\0';
		next += 1;
		/* not pool if listed */
		if (0 == strcmp(token, name))
		{
			return false;
		}
	} while(*next != '\0');

	return true;
}

/* close pooled connection */
int
PoolManagerClosePooledConnections(const char *dbname, const char *username)
{
	int		n32 = 0;
	int 	msglen = 0;
	char	msgtype = 't';
	int 	res = 0;
	int		dblen = dbname ? strlen(dbname) + 1 : 0;
	int		userlen = username ? strlen(username) + 1 : 0;

	HOLD_POOLER_RELOAD();

	if (poolHandle == NULL)
	{
		ConnectPoolManager();
	}

	/* Message type */
	pool_putbytes(&poolHandle->port, &msgtype, 1);

	/* Message length */
	msglen = dblen + userlen + 12;
	n32 = pg_hton32(msglen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Length of Database string */
	n32 = pg_hton32(dblen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send database name, followed by \0 terminator if necessary */
	if (dbname)
		pool_putbytes(&poolHandle->port, dbname, dblen);

	/* Length of Username string */
	n32 = pg_hton32(userlen);
	pool_putbytes(&poolHandle->port, (char *) &n32, 4);

	/* Send user name, followed by \0 terminator if necessary */
	if (username)
		pool_putbytes(&poolHandle->port, username, userlen);

	pool_flush(&poolHandle->port);

	/* Then Get back Pids from Pooler */
	res = pool_recvres(&poolHandle->port, true);
	elog(LOG, "PoolManagerClosePooledConnections res:%d", res);

	RESUME_POOLER_RELOAD();

	return res;
}


static int
handle_close_pooled_connections(PoolAgent * agent, StringInfo s)
{
	int				len = 0;
	const char 		*database = NULL;
	const char 		*user_name = NULL;
	DatabasePool 	*databasePool = NULL;
	int 			res = POOL_CONN_RELEASE_SUCCESS;

	pool_getmessage(&agent->port, s, 0);
	len = pq_getmsgint(s, 4);
	if (len > 0)
		database = pq_getmsgbytes(s, len);

	len = pq_getmsgint(s, 4);
	if (len > 0)
		user_name = pq_getmsgbytes(s, len);

	pq_getmsgend(s);

	/*
	 * Scan the list and destroy any altered pool. They will be recreated
	 * upon subsequent connection acquisition.
	 */
	databasePool = databasePoolsHead;
	while (databasePool)
	{
		/* Update each database pool slot with new connection information */
		HASH_SEQ_STATUS hseq_status;
		PGXCNodePool   *nodePool;

		if (match_databasepool(databasePool, user_name, database))
		{
			hash_seq_init(&hseq_status, databasePool->nodePools);
			while ((nodePool = (PGXCNodePool *) hash_seq_search(&hseq_status)))
			{
				destroy_node_pool_free_slots(nodePool, databasePool, user_name, false);
			}
		}

		databasePool = databasePool->next;
	}

	return res;
}

static bool match_databasepool(DatabasePool *databasePool, const char* user_name, const char* database)
{
	if (!databasePool)
		return false;

	if (!PoolerStatelessReuse)
	{
		if (!user_name && !database)
			return false;

		if (user_name && strcmp(databasePool->user_name, user_name) != 0)
			return false;
	}

	if (database && strcmp(databasePool->database, database) != 0)
		return false;

	return true;
}


static void
ConnectPoolManager(void)
{
	bool need_abort = false;
	PoolHandle *handle = NULL;
	
	handle = GetPoolManagerHandle();
	if (IsTransactionIdle())
	{
		StartTransactionCommand();
		need_abort = true;
	}
	else if (!IsTransactionState() && !IsSubTransaction())
	{
		elog(WARNING, "unexpected transaction stat %s blockState %s", 
			 CurrentTransactionTransStateAsString(), 
			 CurrentTransactionBlockStateAsString());
	}
	
	PoolManagerConnect(handle, GetClusterDBName(),
					   GetClusterUserName(), session_options());
	if (need_abort)
	{
		AbortCurrentTransaction();
	}

}
