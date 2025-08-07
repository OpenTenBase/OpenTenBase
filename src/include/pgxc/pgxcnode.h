/*-------------------------------------------------------------------------
 *
 * pgxcnode.h
 *
 *	  Utility functions to communicate to Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxcnode.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGXCNODE_H
#define PGXCNODE_H
#include "postgres.h"
#include "gtm/gtm_c.h"
#include "utils/timestamp.h"
#include "nodes/pg_list.h"
#include "nodes/nodes.h"
#include "utils/snapshot.h"
#include <unistd.h>


#define NO_SOCKET -1
#define DEFAULT_MAX_HANDLES 100
#define HISTORY_MSG_QUEUE_SIZE 150
#define MAX_HISTORY_MSG_LEN 50
#define COMMIT_CMD_LEN 256

/* Connection to Datanode maintained by Pool Manager */
typedef struct PGconn NODE_CONNECTION;
typedef struct PGcancel NODE_CANCEL;

/* Helper structure to access Datanode from Session */
typedef enum
{
	DN_CONNECTION_STATE_IDLE,			/* idle, ready for query */
	DN_CONNECTION_STATE_BIND,			/* bind is sent, response expected (not for FQS path) */
	DN_CONNECTION_STATE_QUERY,			/* query is sent, response expected */
	DN_CONNECTION_STATE_CLOSE,			/* close is sent, confirmation expected */
	DN_CONNECTION_STATE_ERROR,			/* recoverable error */
	DN_CONNECTION_STATE_FATAL,			/* fatal error */
	DN_CONNECTION_STATE_COPY_IN,
	DN_CONNECTION_STATE_COPY_OUT
} DNConnectionState;

#define IsConnectionStateIdle(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_IDLE)
#define IsConnectionStateQuery(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_QUERY)
#define IsConnectionStateError(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_ERROR)
#define IsConnectionStateFatal(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_FATAL)
#define IsConnectionStateClose(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_CLOSE)
#define IsConnectionStateCopyIn(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_COPY_IN)
#define IsConnectionStateCopyOut(handle) \
			((handle)->state.connection_state == DN_CONNECTION_STATE_COPY_OUT)
#define IsConnectionStateCopy(handle) \
			(((handle)->state.connection_state == DN_CONNECTION_STATE_COPY_IN) || \
				((handle)->state.connection_state == DN_CONNECTION_STATE_COPY_OUT)) 

#define GetConnectionState(handle) \
			((handle)->state.connection_state)

extern const char *ConnectionStateTag[];
#define GetConnectionStateTag(handle) \
			(ConnectionStateTag[(handle)->state.connection_state])



#ifdef __OPENTENBASE__
typedef enum
{
	DNStatus_OK      = 0, 
	DNStatus_ERR     = 1,
	DNStatus_EXPIRED = 2,
	DNStatus_BUTTY
}DNStateEnum;

typedef enum
{
	SendSetQuery_OK					= 0,
	SendSetQuery_EXPIRED			= 1,
	SendSetQuery_SendQuery_ERROR	= 2,
	SendSetQuery_Set_ERROR			= 3,
	SendSetQuery_BUTTY
} SendSetQueryStatus;

typedef enum
{
	TransactionHold,
	SessionHold
} HandleHoldFlag;

#define     MAX_ERROR_MSG_LENGTH 		 256
#endif


#ifdef __OPENTENBASE_C__
#define FIRST_LEVEL 1

#define InvalidFid (-1)
#define FidIsInvalid(fid) ((fid) == InvalidFid)

extern int max_handles;

#endif

#define DN_CONNECTION_ERROR(dnconn) \
		(IsConnectionStateError(dnconn) || IsConnectionStateFatal(dnconn) || IsTxnStateError(dnconn))

#define HAS_MESSAGE_BUFFERED(conn) \
		((conn)->inCursor + 4 < (conn)->inEnd \
			&& (conn)->inCursor + pg_ntoh32(*((uint32_t *) ((conn)->inBuffer + (conn)->inCursor + 1))) < (conn)->inEnd)

#define PGXC_RESULT_TIME_OUT PoolDNSetTimeout /* in seconds */

#define TXN_STATE_IDLE 'I'		 /* transaction state of connected node is idle */
#define TXN_STATE_ERROR 'E'		 /* transaction state of connected node is error */
#define TXN_STATE_INPROGRESS 'T' /* transaction state of connected node is in progress */

#define IsTxnStateIdle(conn) ((conn)->state.transaction_state == TXN_STATE_IDLE)
#define IsTxnStateError(conn) ((conn)->state.transaction_state == TXN_STATE_ERROR)
#define IsTxnStateInprogress(conn) ((conn)->state.transaction_state == TXN_STATE_INPROGRESS)
#define SetTxnState(conn, tstate) ((conn)->state.transaction_state = (tstate))
#define GetTxnState(conn) ((conn)->state.transaction_state)
#define GetSubTransactionId(conn) ((conn)->state.subtrans_id)
#define GetCurrentGucCid(conn) ((conn)->state.cur_guc_cid)
#define IsExecExtendedQuery(conn) ((conn)->state.in_extended_query)

typedef struct _HandleState
{
	DNConnectionState	connection_state;
	char	transaction_state;
	int		subtrans_id;
	bool	in_extended_query;
	uint8	sequence_number;
	uint8	received_sno;
	bool	dirty;
	uint64	cur_guc_cid;
} HandleState;

struct pgxc_node_handle
{
	/* base info */
	char		nodename[NAMEDATALEN];
	int		backend_pid;           /* pid of the remote backend process */
	int		sock;                  /* fd of the connection */
	int		fid;
	int		fragmentlevel;
	int		ftype;
	/* other info */
	Oid		nodeoid;
	int		nodeid;
	int 		nodeidx;
	char		nodehost[NAMEDATALEN];
	int		nodeport;

	/* states */
	volatile bool used;		        /* handle in use? */
	bool	      initializing;
	bool	      registered;	        /* registed on transaction handle list? */

	/* handle state, transition state */
	HandleState     state;
	HandleState     transitionState;
	HandleHoldFlag	holdFlag;
	bool		read_only;

	/* support 2pc state update */
	int			twophase_sindex;		/* 2pc state array index */
	int			twophase_cindex;		/* 2pc connection arrary index */

	/* 
	 * the handle is obtained by this sub-transaction, but the begin subtransaction cmd
	 * has not yet been sent to the datanode.
	 */	
	int			registered_subtranid;

	struct ResponseCombiner *combiner;
#ifdef DN_CONNECTION_DEBUG
	bool		have_row_desc;
#endif
#ifndef __USE_GLOBAL_SNAPSHOT__
	uint64		sendGxidVersion;
#endif
	char		error[MAX_ERROR_MSG_LENGTH];
	/* Output buffer */
	char		*outBuffer;
	size_t		outSize;
	size_t		outEnd;
	/* Input buffer */
	char		*inBuffer;
	size_t		inSize;
	size_t		inStart;
	size_t		inEnd;
	size_t		inCursor;
	/*
	 * Have a variable to enable/disable response checking and
	 * if enable then read the result of response checking
	 *
	 * For details see comments of RESP_ROLLBACK
	 */
	bool		ck_resp_rollback;

	bool		needSync; /* set when error and extend query. */

	bool		stream_closed; /* Whether replicate stream is closed on proxy? */

	bool		sock_fatal_occurred; /*Network failure occurred, and sock descriptor was closed */
	char		last_command;		  /*last command we processed. */
	char		node_type;
	bool		needClose;
	/*
	 * whether to perform strict message dislocation check, if true,
	 * the handle status will be set to FATAL when dislocation occurs
	 */
	bool cmd_strict;

	Oid			current_user;
	int			autoxact_level;
	int			session_id;
	bool		block_connection;

	/* History message queue for debug purpose */
	char		history[HISTORY_MSG_QUEUE_SIZE][MAX_HISTORY_MSG_LEN];
	int		    msg_index;
};

typedef struct pgxc_node_handle PGXCNodeHandle;

/* Structure used to get all the handles involved in a transaction */
typedef struct
{
	int					dn_conn_count;			/* number of Datanode Handles including primary handle */
	PGXCNodeHandle	  **datanode_handles;		/* an array of Datanode handles */
	int					co_conn_count;			/* number of Coordinator handles */
	PGXCNodeHandle	  **coord_handles;			/* an array of Coordinator handles */
} PGXCNodeAllHandles;

typedef struct
{
	NameData	username;
	int		sec_context;
	bool	insec_context;
	bool	isquery;
} BackendEnv;

extern PGXCNodeAllHandles *current_transaction_handles;

extern volatile bool HandlesInvalidatePending;
extern bool record_history_messages;

extern void InitMultinodeExecutor(bool is_force);
extern void ReInitHandles(int new_handles_num);
extern Oid get_nodeoid_from_nodeid(int nodeid, char node_type);
extern Oid get_nodeoid_from_node_id(int node_id, char node_type);


/* Open/close connection routines (invoked from Pool Manager) */
extern char *PGXCNodeConnStr(char *host, int port, char *dbname, char *user,
							 char *pgoptions,
							 char *remote_type, char *parent_node, char *out);
extern NODE_CONNECTION *PGXCNodeConnect(char *connstr);
extern void PGXCNodeClose(NODE_CONNECTION * conn);
extern int PGXCNodeConnected(NODE_CONNECTION * conn);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);
extern int PGXCNodePing(const char *connstr);
#ifdef __OPENTENBASE__
extern NODE_CONNECTION *PGXCNodeConnectBarely(char *connstr);
extern char* PGXCNodeSendShowQuery(NODE_CONNECTION *conn, const char *sql_command);
extern int  PGXCNodeSendSetQuery(NODE_CONNECTION *conn, const char *sql_command, char *errmsg_buf,
								 int32 buf_len, SendSetQueryStatus* status, CommandId *cmdId);
#endif

extern PGXCNodeHandle *get_any_handle(List *datanodelist, int level, int fid, bool write);
/* Look at information cached in node handles */
extern int PGXCNodeGetNodeId(Oid nodeoid, char *node_type);
extern Oid PGXCGetLocalNodeOid(Oid nodeoid);
extern Oid PGXCGetMainNodeOid(Oid nodeoid);
extern int PGXCNodeGetNodeIdFromName(char *node_name, char *node_type);
extern Oid PGXCNodeGetNodeOid(int nodeid, char node_type);

extern PGXCNodeAllHandles *get_handles(List *datanodelist, List *coordlist, bool is_coord_only_query,
									   bool is_global_session, int level, int fid, bool write, bool reserve_write);

extern PGXCNodeAllHandles *get_current_handles(void);

extern void handle_reset(PGXCNodeHandle *handle);
extern void handle_close_and_reset_all(void);

#ifdef __OPENTENBASE__
extern PGXCNodeAllHandles *get_current_txn_handles(bool *has_error);
extern PGXCNodeAllHandles *get_current_subtxn_handles(int subtrans_id, bool all);
extern PGXCNodeAllHandles *get_current_write_handles(void);
extern PGXCNodeAllHandles *get_releasable_handles(bool force, bool waitOnResGroup, bool *all_handles);
extern PGXCNodeAllHandles *get_sock_fatal_handles(void);
extern void init_transaction_handles(void);
/* the function register_transaction_handles is only used in pgxcnode.c */
extern void reset_transaction_handles(void);
extern void register_transaction_handles_all(PGXCNodeAllHandles *handles, bool readonly);

#endif
extern void pfree_pgxc_all_handles(PGXCNodeAllHandles *handles);

extern void release_handles(PGXCNodeAllHandles *handles, bool all_handles);

extern bool validate_handles(void);

extern int	ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);
extern int	ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle * handle);

extern int	pgxc_node_send_query(PGXCNodeHandle * handle, const char *query);
extern int	pgxc_node_send_query_with_internal_flag(PGXCNodeHandle * handle, const char *query);
extern int	pgxc_node_send_rollback(PGXCNodeHandle * handle, const char *query);
extern int	pgxc_node_send_describe(PGXCNodeHandle * handle, bool is_statement,
						const char *name);
extern int	pgxc_node_send_execute(PGXCNodeHandle * handle, const char *portal, int fetch, bool rewind);
extern int	pgxc_node_send_close(PGXCNodeHandle * handle, bool is_statement,
					 const char *name);
extern int	pgxc_node_send_sync_internal(PGXCNodeHandle * handle, const char *filename, int lineno);
extern int	pgxc_node_send_my_sync(PGXCNodeHandle * handle);

#define pgxc_node_send_sync(handle) \
    pgxc_node_send_sync_internal(handle, __FILE__, __LINE__)


#ifdef __SUBSCRIPTION__
extern int pgxc_node_send_apply(PGXCNodeHandle * handle, char * buf, int len, bool ignore_pk_conflict);
#endif
extern int pgxc_node_send_proxy_flag(PGXCNodeHandle *handle, int flag);
extern int pgxc_node_send_on_proxy(PGXCNodeHandle *handle, int firstchar,
									StringInfo inBuf);
extern int	pgxc_node_send_bind(PGXCNodeHandle * handle, const char *portal,
								const char *statement, int paramlen, char *params, 
								int16 *pformats, int n_pformats, 
								int16 *rformats, int n_rformats);
extern int	pgxc_node_send_parse(PGXCNodeHandle * handle, const char* statement,
								 const char *query, short num_params, Oid *param_types);
extern int	pgxc_node_send_flush(PGXCNodeHandle * handle);
extern int	pgxc_node_send_query_extended(PGXCNodeHandle *handle, const char *query,
							  const char *statement, const char *portal,
							  int num_params, Oid *param_types,
							  int paramlen, char *params, int16 *pformats, 
							  int n_pformats, bool send_describe, 
							  int fetch_size, int16 *formats, int n_formats);
extern int	pgxc_node_send_batch_execute(PGXCNodeHandle *handle, const char *query,
                                           const char *statement, int num_params, Oid *param_types,
                                           StringInfo batch_message);
extern int  pgxc_node_send_plan(PGXCNodeHandle * handle, const char *statement,
					const char *query, const char *planstr,
					int num_params, Oid *param_types, int instrument_options);
extern int
pgxc_node_send_gid(PGXCNodeHandle *handle, char* gid);

#ifdef __TWO_PHASE_TRANS__
extern int pgxc_node_send_starter(PGXCNodeHandle *handle, char* startnode);
extern int pgxc_node_send_startxid(PGXCNodeHandle *handle, GlobalTransactionId transactionid);
extern int pgxc_node_send_partnodes(PGXCNodeHandle *handle, char* partnodes);
extern int pgxc_node_send_database(PGXCNodeHandle *handle, char* database);
extern int pgxc_node_send_user(PGXCNodeHandle *handle, char* user);
extern int pgxc_node_send_clean(PGXCNodeHandle *handle);
extern int pgxc_node_send_readonly(PGXCNodeHandle *handle, bool read_only);
extern int pgxc_node_send_after_prepare(PGXCNodeHandle *handle);
#endif
extern int pgxc_node_send_gxid(PGXCNodeHandle *handle);
#ifdef __OPENTENBASE_C__
extern int pgxc_node_send_fid_flevel(PGXCNodeHandle *handle, int fid, int flevel);
#endif

extern int pgxc_node_send_clean_distribute(PGXCNodeHandle *handle);

#ifdef __RESOURCE_QUEUE__
typedef struct ResQueueInfo ResQueueInfo;
extern int pgxc_node_send_resqinfo(PGXCNodeHandle *handle,
									ResQueueInfo * resqinfo,
									int64 network_bytes_limit);
#endif
extern int pgxc_node_send_resgreq(PGXCNodeHandle *handle);
extern int pgxc_node_send_resginfo(PGXCNodeHandle *handle);

extern int	pgxc_node_send_cmd_id(PGXCNodeHandle *handle, CommandId cid);
extern int	pgxc_node_send_cmd_seq(PGXCNodeHandle *handle);

extern int	pgxc_node_send_no_tuple_descriptor(PGXCNodeHandle *handle);
extern int	pgxc_node_send_dn_direct_printtup(PGXCNodeHandle *handle);
extern int	pgxc_node_send_internal_cmd_flag(PGXCNodeHandle *handle);
extern int	pgxc_node_send_option(PGXCNodeHandle *handle, char *option);
extern int	pgxc_node_send_snapshot(PGXCNodeHandle * handle, Snapshot snapshot);
extern int	pgxc_node_send_timestamp(PGXCNodeHandle * handle, TimestampTz timestamp);
extern int pgxc_node_send_queryid(PGXCNodeHandle *handle);

extern int
pgxc_node_send_prepare_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int
pgxc_node_send_prefinish_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int
pgxc_node_send_global_timestamp(PGXCNodeHandle *handle, GlobalTimestamp timestamp);
extern int pgxc_node_send_exec_env(PGXCNodeHandle *handle, bool insec_context,
										bool isquery);
extern int pgxc_node_batch_set_user(PGXCNodeHandle *connections[], int count, bool insec_context, bool error);
extern int pgxc_node_cleanup_exec_env(PGXCNodeHandle *handle);
extern int pgxc_node_send_exec_env_if(PGXCNodeHandle *handle);
extern int pgxc_node_send_autoxact_sessionid(PGXCNodeHandle *handle, bool need_reply);
#ifdef __OPENTENBASE__
extern int	pgxc_node_send_coord_info(PGXCNodeHandle * handle, int coord_pid, TransactionId coord_vxid);
extern int	pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout);
extern bool node_ready_for_query(PGXCNodeHandle *conn);

#else
extern bool	pgxc_node_receive(const int conn_count,
				  PGXCNodeHandle ** connections, struct timeval * timeout);
#endif
extern int	pgxc_node_read_data(PGXCNodeHandle * conn, bool close_if_error);
extern int	pgxc_node_is_data_enqueued(PGXCNodeHandle *conn);

extern int  send_some(PGXCNodeHandle *handle, int len);
extern int  pgxc_node_send_msg(PGXCNodeHandle *handle, char *ptr, int len);
extern int	pgxc_node_flush(PGXCNodeHandle *handle);

extern int pgxc_node_flush_safe(PGXCNodeHandle *handle);
extern Datum pgxc_execute_on_nodes_for_max(int numnodes, Oid *nodelist, char *query);

extern char get_message(PGXCNodeHandle *conn, int *len, char **msg);

extern void add_error_message(PGXCNodeHandle * handle, const char *message);

extern Datum pgxc_execute_on_nodes(int numnodes, Oid *nodelist, char *query);

extern bool session_param_list_has_param(bool local, const char *name, const char *value,
										 int flags);

extern void session_param_list_set(bool local, const char *name, const char *value, int flags, uint64 guc_cid);
extern void session_param_list_reset(bool only_local);
extern char *session_param_list_get_str(uint64 start_cid);
//extern int pgxc_node_exec_on_current_txn_handles(const char *query);
extern void RequestRefreshRemoteHandles(void);
extern bool PoolerMessagesPending(void);
extern void UpdateConnectionState_internal(PGXCNodeHandle *handle,
		DNConnectionState new_state, const char* filename, int lineno);
#define UpdateConnectionState(handle, new_state) \
			UpdateConnectionState_internal(handle, new_state, __FILE__, __LINE__)
extern bool PgxcNodeDiffBackendHandles(List **nodes_alter,
			   List **nodes_delete, List **nodes_add);
extern void PgxcNodeRefreshBackendHandlesShmem(List *nodes_alter);
extern void HandlePoolerMessages(void);
extern void pgxc_print_pending_data(PGXCNodeHandle *handle, bool reset);
extern void RequestInvalidateRemoteHandles(void);
extern Param *pgxc_make_param(int param_num, Oid param_type);

#ifdef __OPENTENBASE__
void add_error_message_from_combiner(PGXCNodeHandle *handle, void *combiner_input);
void CheckInvalidateRemoteHandles(void);
int PGXCGetAllMasterDnOid(Oid *nodelist);
int PGXCGetOtherMasterCnOid(Oid *nodelist);
PGXCNodeHandle* find_ddl_leader_cn(void);
bool  is_ddl_leader_cn(PGXCNodeHandle *handle);
PGXCNodeHandle *find_leader_cn_with_name(char *name);
PGXCNodeHandle *find_leader_cn(void);
bool is_leader_cn_avaialble(PGXCNodeHandle * handle);
extern void pgxc_set_coordinator_proc_pid(int proc_pid);
extern int pgxc_get_coordinator_proc_pid(void);
extern void pgxc_set_coordinator_proc_vxid(TransactionId proc_vxid);
extern TransactionId pgxc_get_coordinator_proc_vxid(void);
bool  is_gdd_runner_cn(void);
extern char* pgxc_get_global_trace_id(void);
void pgxc_copy_global_trace_id_to_history(void);
int pgxc_node_send_global_traceid(PGXCNodeHandle *handle);
int pgxc_node_set_global_traceid(char *traceid);
void pgxc_node_set_global_traceid_regenerate(bool regen);
#endif

#ifdef __AUDIT__
extern const char * PGXCNodeTypeString(char node_type);
#endif

#ifdef __AUDIT_FGA__
extern void PGXCGetCoordOidOthers(Oid *nodelist);
extern void PGXCGetAllDnOid(Oid *nodelist);
#endif

#ifdef __OPENTENBASE_C__
extern char * PGXCNodeGetNodename(int nodeid, char node_type);
extern void RefreshQueryId(void);
extern void CreateSharedPgxcQueryId(void);
extern Size PgxcQueryIdShmemSize(void);
#endif

extern void CNSendResGroupReq(PGXCNodeHandle *handle);
extern char *get_nodename_from_nodeid(int nodeid, char node_type);
extern int pgxc_node_send_dml(PGXCNodeHandle *handle, CmdType cmdtype, char *relname,
							char *space, HeapTuple ituple, HeapTuple dtuple, ItemPointer target_ctid);

extern void register_sock_fatal_handles(PGXCNodeHandle *handle);
extern void report_sock_fatal_handles(void);
extern void reset_sock_fatal_handles(void);

extern void reset_error_handles(bool only_clear);
extern void register_error_handles(PGXCNodeHandle *handle);

extern int pgxc_node_send_tran_command(PGXCNodeHandle *handle, TransactionStmtKind kind, char *opt);
extern const char *getTranStmtKindTag(TransactionStmtKind kind);
extern void SetCurrentHandlesReadonly(void);


/* new API */
extern void InitHandleState(PGXCNodeHandle *handle);
extern void ResetTransitionState(PGXCNodeHandle *handle);
extern void StabilizeHandleState(PGXCNodeHandle *handle);
extern void UpdateTransitionConnectionState(PGXCNodeHandle *handle, DNConnectionState newstate);
extern uint8 GetNewSequenceNum(PGXCNodeHandle *handle);
extern void SetTransitionInExtendedQuery(PGXCNodeHandle *handle, bool exq);
extern int FlushAndStoreState(PGXCNodeHandle *handle);
extern int SendRequest(PGXCNodeHandle *handle);
extern int SendFlushRequest(PGXCNodeHandle *handle);
extern void DecreaseSubtranNestingLevel(PGXCNodeHandle *handle);
extern void SetSubTransactionId(PGXCNodeHandle *handle, int level);
extern void SetCurrentGucCid(PGXCNodeHandle *handle, int guc_cid);
extern void UpdateTransitionTxnState(PGXCNodeHandle *handle, char state);
extern void RecordOneMessage(PGXCNodeHandle *handle, char type, const char *content, int size, bool send);
extern bool checkSequenceMatch(PGXCNodeHandle *handle, uint8 seqno);
extern PGXCNodeAllHandles *GetCorruptedHandles(bool *all_handles);
extern void PrintHandleState(PGXCNodeHandle *handle);
extern void SetHandleUsed_internal(PGXCNodeHandle *handle, bool used, const char *filename, int lineno);
extern bool HandledAllReplay(PGXCNodeHandle *handle);
extern void DestroyHandle_internal(PGXCNodeHandle *handle, const char *file, const int line);
#define DestroyHandle(handle) \
		DestroyHandle_internal(handle, __FILE__, __LINE__);

#define SetHandleUsed(handle, used) \
			SetHandleUsed_internal(handle, used, __FILE__, __LINE__)

#define RecordSendMessage(handle, type, content, size)  \
	do { \
		if (record_history_messages) \
			RecordOneMessage(handle, type, content, size, true); \
	} while(0)

#define RecordRecvMessage(handle, type, content, size)  \
	do { \
		if (record_history_messages) \
			RecordOneMessage(handle, type, content, size, false); \
	} while(0)
#endif /* PGXCNODE_H */
