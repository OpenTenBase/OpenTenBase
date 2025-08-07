/*-------------------------------------------------------------------------
 *
 * poolmgr.h
 *
 *	  Definitions for the Datanode connection pool.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/poolmgr.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POOLMGR_H
#define POOLMGR_H
#include <sys/time.h>
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "poolcomm.h"
#include "storage/pmsignal.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/guc.h"


#ifdef __OPENTENBASE_C__
typedef enum
{
	SIGNAL_SIGINT   = 0,
	SIGNAL_SIGUSR2  = 1
}   SignalType;
#endif

struct PGXCUserPool;

/* Connection pool entry */
typedef struct
{
	/* stamp elements */
	time_t released;/* timestamp when the connection last time release */
	time_t checked; /* timestamp when the connection last time check */
	time_t created; /* timestamp when the connection created */
	bool   bwarmed;

	bool    set4reuse;
	Oid		user_id;
	char    *pgoptions;		/* Connection options */

	int32  usecount;
	NODE_CONNECTION *conn;
	NODE_CANCEL	*xc_cancelConn;

	/* trace info */	
	int32  refcount;   /* reference count */
	int32  m_version;  /* version of node slot */
	int32  pid;		   /* agent pid that contains the slot */
	int32  seqnum;	   /* slot seqnum for the slot, unique for one slot */
	bool   bdestoryed; /* used to show whether we are destoryed */
	char   *file;      /* file where destroy the slot */
	int32  lineno;	   /* lineno where destroy the slot */
	char   *node_name; /* connection node name , pointer to datanode_pool node_name, no memory allocated*/
	char   *user_name; /* connection user name , pointer to PGXCUserPool user_name, no memory allocated*/
	int32  backend_pid;/* backend pid of remote connection */
	/*
	 * the connection needs to be moved to the dn_connections_used_pool,
	 * indicating that the connection has already been used and cannot
	 * be directly used when obtaining the connection in the future
	 */
	bool   need_move_to_usedpool;
	struct PGXCUserPool *userPool;
} PGXCNodePoolSlot;


typedef struct PGXCNodeConnCounter
{
	Oid			nodeoid;	/* Node Oid related to this pool */
	bool        coord;      /* whether am I coordinator */
	char		node_name[NAMEDATALEN]; /* name of the node.*/
	char		node_host[NAMEDATALEN];
	int			node_port;
	int			freeSize;	/* available connections */
	int			size;  		/* total pool size */
	int 		ref_count;  /* refer count by node pool */
} PGXCNodeSingletonInfo;

/* Pool of connections to specified pgxc node */
typedef struct
{
	Oid			nodeoid;	/* Node Oid related to this pool */
	bool        asyncInProgress;/* whether am in asyn building */
	char	   *connstr;
	int         nquery;     /* connection number query memory size in progress */
	int			freeSize;	/* available connections */
	int			size;  		/* total pool size */
	int32       m_version;	/* version of node pool */
	HTAB	   *userPools; 		/* Hashtable of PGXCUserPool, one entry for each Coordinator or DataNode */
	struct PGXCUserPool *userPoolListHead;
	struct PGXCUserPool *userPoolListTail;
	PGXCNodeSingletonInfo *nodeInfo; /* Node information, db insensitive */
} PGXCNodePool;

typedef struct PGXCUserPool
{
	Oid			 user_id;
	char		*user_name;
	char		*pgoptions;		/* default Connection options */
	char        *connstr;       /* default Connection str */
	int			 freeSize;	    /* available connections */
	int			 size;  		/* total pool size */
	int          refcount;      /* reference by set for reuse tasks count */
	PGXCNodePool *nodePool;
	struct PGXCUserPool *prev; 	/* Reference to prev to organize linked list */
	struct PGXCUserPool *next; 	/* Reference to next to organize linked list */
	PGXCNodePoolSlot **slot;
} PGXCUserPool;

/* All pools for specified database */
typedef struct databasepool
{
	char	   *database;
	char	   *user_name;
	Oid			user_id;
	char	   *pgoptions;		/* Connection options */
	HTAB	   *nodePools; 		/* Hashtable of PGXCNodePool, one entry for each
								 * Coordinator or DataNode */
	time_t		oldest_idle;
	bool        bneed_pool;		/* check whether need  connect pool */
	int64		version;        /* used to generate node_pool's version */
	MemoryContext mcxt;
	struct databasepool *prev; 	/* Reference to prev to organize linked list */
	struct databasepool *next; 	/* Reference to next to organize linked list */
} DatabasePool;

typedef enum
{
	RELEASE_NOTHING   = 0,
	RELEASE_PARTITAL  = 1,
	RELEASE_ALL       = 2,
	FORCE_RELEASE_ALL = 3
} ReleaseType;

typedef struct
{
	List   *datanodelist;
	List   *dn_pidlist;
	List   *coordlist;
	int     force;
} ReleaseContext;

/*
 * Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 * and managing session parameters
 */
typedef struct
{
	/* Process ID of postmaster child process associated to pool agent */
	int				pid;
	/* communication channel */
	PoolPort		port;
	DatabasePool   *pool;

	/* In pooler stateless reuse mode, user_name and pgoptions save in PoolAgent */
	Oid 		     user_id;
	char			*user_name;
	char			*pgoptions;              /* Connection options */
	char			*set_conn_reuse_sql;

	MemoryContext	mcxt;
	int				num_dn_connections;
	int				num_coord_connections;
	Oid		   	   *dn_conn_oids;		/* one for each Datanode */
	Oid		   	   *coord_conn_oids;	/* one for each Coordinator */

	PGXCNodePoolSlot **dn_connections; /* one for each Datanode */
	PGXCNodePoolSlot **coord_connections; /* one for each Coordinator */
	/*
	 * the connection that has been acquired by the backend,
	 * there may be multiple connections for the same dn
	 */
	HTAB	       *dn_connections_used_pool;
	int             query_count;   /* query count, if exceed, need to reconnect database */
	bool            breconnecting; /* whether we are reconnecting */
	int             agentindex;

	bool            destory_pending; /* whether we have been ordered to destory */
	bool            locked_pooler;
	int32			ref_count;		 /* reference count */
	ReleaseType     release_pending_type; /* whether we have been ordered to release connections */
	ReleaseContext *release_ctx;
	uint32			rcv_seq;
} PoolAgent;

/* Handle to the pool manager (Session's side) */
typedef struct
{
	/* communication channel */
	PoolPort	port;
} PoolHandle;

#define     POOLER_ERROR_MSG_LEN  256

typedef struct
{
	char errmsg[POOLER_ERROR_MSG_LEN];
} BatchTaskErrMsg;

typedef struct
{
	char connstr[1024];
} BatchConnStr;

typedef struct PGXCPoolAsyncBatchTask
{
	int32               cmd;  	                 /* refer to handle_agent_input command tag */
	PoolAgent          *agent;                   /* agent for this batch task */
	PGXCNodePoolSlot  **m_batch_slot;            /* connection slot , no need to free the slot */
	int                *m_batch_nodeindex;       /* node index of the remote peer, end_nodeid when end query for batch */
	int                *m_batch_node_type;       /* PoolNodeTypeEnum node type*/
	int 			   *m_batch_status;          /* PoolTaskStatusEnum task status for each task */
	int                 m_task_status;           /* PoolTaskStatusEnum task status for this batch task */
	int32			    m_batch_count;			 /* number of tasks in this batch task */
	int 			    m_max_batch_size;        /* the m_batch_XXX array max size */

	/* acquire connections */
	int32              *m_result;                /* fd array */
	int32			   *m_pidresult;	         /* pid array */
	List 			   *m_datanodelist;          /* datanodelist to get connections */
	List 			   *m_coordlist;             /* coordlist to get connections */
	PGXCUserPool      **m_batch_userpool;	     /* user pool for each node */
	char              **m_batch_conninfo;        /* connection info for each node */
	NODE_CONNECTION	  **m_batch_conn;            /* PGconn for each new connection */
	int 			    m_need_connect_count;    /* the count we need to build a new connection , we acquire new connections */
	int                 m_need_set4reuse_count;  /* the count we need to set for reuse */
	int32			    req_seq;		         /* req sequence number */
	struct  timeval     start_time;		         /* when acquire conn by sync thread, the time begin request */
	struct  timeval     end_time;			     /* when acquire conn by sync thread, the time finish request */

	/* end or cancel query */
	pgsocket 	       *m_batch_sock;            /* batch socket handle for batch operations */
	int                 signal;                  /* signal type */

	BatchConnStr        *m_batch_connstr_buf;
	BatchTaskErrMsg     *m_batch_errmsg;         /* errmsg for each task */
	char                errmsg[POOLER_ERROR_MSG_LEN]; /* errmsg for this batch task */
} PGXCPoolAsyncBatchTask;

typedef struct
{
	Oid			node;	         /* Node Id related to this connection */
	int32       backend_pid;     /* backend pid of remote connection */
} DnConnectionKey;

typedef struct
{
	DnConnectionKey  key;        /* dn connection key in dn_connections_used_pool */
	PGXCNodePoolSlot *slot;      /* dn connection in dn_connections_used_pool */
} DnConnection;

extern int	MaxPoolSize;
extern int	MinFreeSizePerDb;
extern int	MinFreeSize;

extern bool PoolerStatelessReuse;

extern int	PoolerPort;
extern int	PoolConnKeepAlive;
extern int	PoolMaintenanceTimeout;
extern int	PoolCancelQueryTimeout;

extern char *g_PoolerWarmBufferInfo;
extern char *g_unpooled_database;
extern char *g_unpooled_user;

extern char *PoolConnectDB;

extern int	PoolSizeCheckGap; 
extern int	PoolConnMaxLifetime; 
extern int	PoolMaxMemoryLimit;
extern int	PoolConnectTimeOut;
extern int  PoolScaleFactor;
extern int  PoolDNSetTimeout;
extern int  PoolCheckSlotTimeout;
extern int  PoolPrintStatTimeout;
extern bool PoolConnectDebugPrint; 
extern bool PoolSubThreadLogPrint;
extern bool PoolSizeCheck;

/* Status inquiry functions */
extern void PGXCPoolerProcessIam(void);
extern bool IsPGXCPoolerProcess(void);

/* Initialize internal structures */
extern int	PoolManagerInit(void);

/* Destroy internal structures */
extern int	PoolManagerDestroy(void);

/*
 * Get handle to pool manager. This function should be called just before
 * forking off new session. It creates PoolHandle, PoolAgent and a pipe between
 * them. PoolAgent is stored within Postmaster's memory context and Session
 * closes it later. PoolHandle is returned and should be store in a local
 * variable. After forking off it can be stored in global memory, so it will
 * only be accessible by the process running the session.
 */
extern PoolHandle *GetPoolManagerHandle(void);

/*
 * Called from Postmaster(Coordinator) after fork. Close one end of the pipe and
 * free memory occupied by PoolHandler
 */
extern void PoolManagerCloseHandle(PoolHandle *handle);

/*
 * Gracefully close connection to the PoolManager
 */
extern void PoolManagerDisconnect(void);

extern char *session_options(void);

/*
 * Called from Session process after fork(). Associate handle with session
 * for subsequent calls. Associate session with specified database and
 * initialize respective connection pool
 */
extern void PoolManagerConnect(PoolHandle *handle,
	                           const char *database, const char *user_name,
	                           char *pgoptions);

/*
 * Reconnect to pool manager
 * This simply does a disconnection followed by a reconnection.
 */
extern void PoolManagerReconnect(void);

/* Get pooled connections */
extern int *PoolManagerGetConnections(List *datanodelist, List *coordlist, int **pids, StringInfo *error_msg);


/* Clean pool connections */
extern void PoolManagerCleanConnection(List *datanodelist, List *coordlist, char *dbname, char *username);

/* Check consistency of connection information cached in pooler with catalogs */
extern bool PoolManagerCheckConnectionInfo(void);

/* Reload connection data in pooler and drop all the existing connections of pooler */
extern void PoolManagerReloadConnectionInfo(void);

/* Send Abort signal to transactions being run */
extern int	PoolManagerAbortTransactions(char *dbname, char *username, int **proc_pids);

/* Return connections back to the pool, for both Coordinator and Datanode connections */
extern void PoolManagerReleaseAllConnections(bool force);

extern void PoolManagerReleaseConnections(int dn_count, int *dn_list, int *dn_pid_list, int co_count, int *co_list, bool force);

/* Cancel a running query on Datanodes as well as on other Coordinators */
extern bool PoolManagerCancelQuery(int dn_count, int* dn_list, int* dn_pid_list, int co_count, int* co_list, int signal);

/* Lock/unlock pool manager */
extern void PoolManagerLock(bool is_lock);

/* Check if pool has a handle */
extern bool IsPoolHandle(void);

/* Do pool health check activity */
extern void PoolAsyncPingNodes(void);
extern void PoolPingNodes(void);
extern bool PoolPingNodeRecheck(Oid nodeoid);

/* Refresh connection data in pooler and drop connections of altered nodes in pooler */
extern int PoolManagerRefreshConnectionInfo(void);
extern int PoolManagerClosePooledConnections(const char *dbname, const char *username);

#endif
