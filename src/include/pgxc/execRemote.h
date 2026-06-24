/*-------------------------------------------------------------------------
 *
 * execRemote.h
 *
 *	  Functions to execute commands on multiple Datanodes
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/execRemote.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "planner.h"
#ifdef XCP
#include "remotecopy.h"
#endif
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/snapshot.h"
#ifdef __OPENTENBASE__
#include "access/parallel.h"
#endif
#include "access/xact.h"

/* Outputs of handle_response() */
#define RESPONSE_EOF EOF
#define RESPONSE_COMPLETE 0
#define RESPONSE_SUSPENDED 1
#define RESPONSE_TUPDESC 2
#define RESPONSE_DATAROW 3
#define RESPONSE_COPY 4
#define RESPONSE_BARRIER_OK 5
#ifdef XCP
#define RESPONSE_ERROR 6
#define ERSPONSE_ANALYZE 7
#define RESPONSE_READY 10
#define RESPONSE_WAITXIDS 11
#define RESPONSE_ASSIGN_GXID 12
#define RESPONSE_READY_MISMATCH 13
#endif
#define RESPONSE_BIND_COMPLETE 14

#ifdef __OPENTENBASE__
#define     UINT32_BITS_NUM			  32
#define     WORD_NUMBER_FOR_NODES	  (MAX_NODES_NUMBER / UINT32_BITS_NUM)

#define CLEAR_BIT(data, bit) data = (~(1 << (bit)) & (data))
#define SET_BIT(data, bit)   data = ((1 << (bit)) | (data))
#define BIT_CLEAR(data, bit) (0 == ((1 << (bit)) & (data))) 
#define BIT_SET(data, bit)   ((1 << (bit)) & (data)) 

extern int DataRowBufferSize;
#endif

extern bool exec_event_trigger_stmt;
extern bool temp_object_included;

typedef enum
{
	REQUEST_TYPE_NOT_DEFINED,	/* not determined yet */
	REQUEST_TYPE_COMMAND,		/* OK or row count response */
	REQUEST_TYPE_QUERY,			/* Row description response */
	REQUEST_TYPE_COPY_IN,		/* Copy In response */
	REQUEST_TYPE_COPY_OUT,		/* Copy Out response */
	REQUEST_TYPE_ERROR			/* Error, ignore responses */
}	RequestType;

/*
 * Type of requests associated to a remote COPY OUT
 */
typedef enum
{
	REMOTE_COPY_NONE,		/* Not defined yet */
	REMOTE_COPY_STDOUT,		/* Send back to client */
	REMOTE_COPY_FILE,		/* Write in file */
	REMOTE_COPY_TUPLESTORE	/* Store data in tuplestore */
} RemoteCopyType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag
{
	CmdType cmdType;						/* DML command type */
	char	data[COMPLETION_TAG_BUFSIZE];	/* execution result combination data */
} CombineTag;

typedef struct ResponseCombiner ResponseCombiner;

typedef struct RemoteFragmentController
{
	MemoryContext context;
	ResourceOwner owner;
	int           nConnections;		 /* number of connections */
	List	      *connections;
	List	      *free_connections;
	EState        *estate; 			 /* needed for state counting */
	ResponseCombiner	*combiner;
	struct epoll_event *events;
	int 		   efd;
	bool          external_fd;

	void         *libpq;
} RemoteFragmentController;

/*
 * Common part for all plan state nodes needed to access remote datanodes
 * ResponseCombiner must be the first field of the plan state node so we can
 * typecast
 */
typedef struct ResponseCombiner
{
	ScanState	ss;						/* its first field is NodeTag */
	int			node_count;				/* total count of participating nodes */
	PGXCNodeHandle **connections;		/* Datanode connections being combined */
	int			conn_count;				/* count of active connections */
	int			current_conn;			/* used to balance load when reading from connections */
	long		current_conn_rows_consumed;
	CombineType combine_type;			/* see CombineType enum */
#ifdef __OPENTENBASE_C__
	bool        DML_replicated_return;  /* DML on replication table returned */
	bool        handle_ftable_resp;  	/* when true, means call by HandleFtableResponse */
#endif
	int			command_complete_count; /* count of received CommandComplete messages */
	RequestType request_type;			/* see RequestType enum */
	TupleDesc	tuple_desc;				/* tuple descriptor to be referenced by emitted tuples */
	int			description_count;		/* count of received RowDescription messages */
	int			copy_in_count;			/* count of received CopyIn messages */
	int			copy_out_count;			/* count of received CopyOut messages */
	FILE	   *copy_file;      		/* used if copy_dest == COPY_FILE */
	uint64		processed;				/* count of data rows handled */
	char		errorCode[5];			/* error code to send back to client */
	char	   *errorMessage;			/* error message to send back to client */
	char	   *errorDetail;			/* error detail to send back to client */
	char	   *errorHint;				/* error hint to send back to client */
	Oid			returning_node;			/* returning replicated node */
	RemoteDataRow currentRow;			/* next data ro to be wrapped into a tuple */
	/* TODO use a tuplestore as a rowbuffer */
	List 	   *rowBuffer;				/* buffer where rows are stored when connection
										 * should be cleaned for reuse by other RemoteQuery */
	/*
	 * To handle special case - if there is a simple sort and sort connection
	 * is buffered. If EOF is reached on a connection it should be removed from
	 * the array, but we need to know node number of the connection to find
	 * messages in the buffer. So we store nodenum to that array if reach EOF
	 * when buffering
	 */
	Oid 	   *tapenodes;
	/*
	 * If some tape (connection) is buffered, contains a reference on the cell
	 * right before first row buffered from this tape, needed to speed up
	 * access to the data
	 */
	ListCell  **tapemarks;
	/*
	 * The node_count may change, record tapemarks count for repalloc
	 */
	int			tapemarks_count;
#ifdef __OPENTENBASE__
	List            **prerowBuffers;    /* used for each connection in prefetch with merge_sort,
										 * put datarows in each rowbuffer in order */
	Tuplestorestate **dataRowBuffer;    /* used for prefetch */
	long             *dataRowMemSize;   /* size of datarow in memory */
	int             *nDataRows;         /* number of datarows in tuplestore */
	TupleTableSlot  *tmpslot;           
	char*            errorNode;		    /* node Oid, who raise an error, set when handle_response */
	int              backend_pid;	    /* backend_pid, who raise an error, set when handle_response */
	bool             is_abort;
#endif
	bool		ignore_datarow;
	bool		merge_sort;             /* perform mergesort of node tuples */
	bool		extended_query;         /* running extended query protocol */
	bool		probing_primary;		/* trying replicated on primary node */
	void	   *tuplesortstate;			/* for merge sort */
	/* COPY support */
	RemoteCopyType remoteCopyType;
	Tuplestorestate *tuplestorestate;
	/* cursor support */
	char	   *cursor;					/* cursor name */
	char	   *update_cursor;			/* throw this cursor current tuple can be updated */
	int			cursor_count;			/* total count of participating nodes */
	PGXCNodeHandle **cursor_connections;/* data node connections being combined */
#ifdef __OPENTENBASE__
	/* statistic information for debug */
	int         recv_node_count;       /* number of recv nodes */
	uint64      recv_tuples;           /* number of recv tuples */
	TimestampTz recv_total_time;       /* total time to recv tuples */
	/* used for remoteDML */
	int32      DML_processed;         /* count of DML data rows handled on remote nodes */
	PGXCNodeHandle **conns;		
	int			     ccount;	
	uint64     recv_datarows;
	ItemPointerData simpledml_newctid;
	MemoryContext context;		/* memory context for holding tuples */
    ResourceOwner resowner;		/* resowner for holding temp files */
#endif
    bool         waitforC;
    bool         waitforE;
	bool         waitfor2;

	RemoteFragmentController *controller;
}	ResponseCombiner;

typedef struct RemoteQueryState
{
	ResponseCombiner combiner;			/* see ResponseCombiner struct */
	bool		query_Done;				/* query has been sent down to Datanodes */

	/* Support for parameters */
	char	   *paramval_data;		/* parameter data, format is like in BIND */
	int			paramval_len;		/* length of parameter values data */
	Oid		   *rqs_param_types;	/* Types of the remote params */
	int			rqs_num_params;
	int16	   *rqs_param_formats;	/* a format code for each param */

	int			eflags;			/* capability flags to pass to tuplestore */
#ifdef __OPENTENBASE__
	ParallelWorkerStatus *parallel_status; /*Shared storage for parallel worker .*/

	/* parameters for insert...on conflict do update */
	char	   *ss_paramval_data;		
	int			ss_paramval_len;		
	Oid		   *ss_param_types;
	int			ss_num_params;

    char	   *su_paramval_data;
	int			su_paramval_len;		
	Oid		   *su_param_types;	
	int			su_num_params;

	uint32		dml_prepared_mask[WORD_NUMBER_FOR_NODES]; 
#endif
}	RemoteQueryState;

/*
 * Data needed to set up a PreparedStatement on the remote node and other data
 * for the remote executor
 */
typedef struct RemoteStmt
{
	NodeTag		type;

	CmdType		commandType;	/* select|insert|update|delete */

	bool		hasReturning;	/* is it insert|update|delete RETURNING? */
	int		jitFlags;		/* which forms of JIT should be performed */

#ifdef __OPENTENBASE__
	bool        parallelModeNeeded;     /* is parallel needed? */
#endif

	struct Plan *planTree;				/* tree of Plan nodes */

	List	   *rtable;					/* list of RangeTblEntry nodes */

	/* rtable indexes of target relations for INSERT/UPDATE/DELETE */
	List	   *resultRelations;	/* integer list of RT indexes, or NIL */

    /*
	 * rtable indexes of non-leaf target relations for UPDATE/DELETE on all
	 * the partitioned tables mentioned in the query.
	 */
	List	   *nonleafResultRelations;

	/*
	 * rtable indexes of root target relations for UPDATE/DELETE; this list
	 * maintains a subset of the RT indexes in nonleafResultRelations,
	 * indicating the roots of the respective partition hierarchies.
	 */
	List	   *rootResultRelations;

	List	   *subplans;		/* Plan trees for SubPlan expressions */

	Bitmapset  *rewindPlanIDs;	/* indices of subplans that require REWIND */

	List       *paramExecTypes; /* type OIDs for PARAM_EXEC Params */

	int			nParamRemote;	/* number of params sent from the master node */

	RemoteParam *remoteparams;  /* parameter descriptors */

	List	   *rowMarks;
#ifdef __OPENTENBASE_C__
	int         ndiskeys;

	AttrNumber  *diskeys;
#endif
#ifdef __OPENTENBASE_C__
	int			fragmentType;
	Bitmapset	*sharedCtePlanIds;
	int			fragment_work_mem;	/* work_mem used by this fragment */
	int			fragment_num;		/* number of fragments */
#endif

#ifdef __AUDIT__
	const char  *queryString;
	Query 		*parseTree;
#endif

	/* begin ora_compatible */
	List	   *multi_dist;
	int			startno;
	int			endno;
	bool		has_else;
	/* end ora_compatible */
	FNQueryId	queryid;	    	/* transformed from SnowFlakeGetId */
} RemoteStmt;

#ifdef __OPENTENBASE__
typedef enum
{
	TXN_TYPE_CommitTxn,
	TXN_TYPE_CommitSubTxn,
	TXN_TYPE_RollbackTxn,
	TXN_TYPE_RollbackSubTxn,
	TXN_TYPE_CleanConnection,

	TXN_TYPE_Butt
}TranscationType;
#endif

struct find_params_context
{
	RemoteParam *rparams;
	Bitmapset *defineParams;
	List *subplans;
};

extern int PGXLRemoteFetchSize;
typedef struct AnalyzeRelStats AnalyzeRelStats;

#ifdef __OPENTENBASE__
extern PGDLLIMPORT int g_in_plpgsql_exec_fun;
#endif

typedef void (*xact_callback) (bool isCommit, void *args);

/* Copy command just involves Datanodes */
extern void DataNodeCopyBegin(RemoteCopyData *rcstate);
extern int DataNodeCopyIn(char *data_row, int len, const char *eol, int conn_count,
						  PGXCNodeHandle** copy_connections,
						  bool binary);
extern uint64 DataNodeCopyOut(PGXCNodeHandle** copy_connections,
							  int conn_count, FILE* copy_file);
extern uint64 DataNodeCopyStore(PGXCNodeHandle** copy_connections,
								int conn_count, Tuplestorestate* store);
extern void DataNodeCopyFinish(int conn_count, PGXCNodeHandle** connections);
extern int DataNodeCopyInBinaryForAll(char *msg_buf, int len, int conn_count,
									  PGXCNodeHandle** connections);
extern bool DataNodeCopyEnd(PGXCNodeHandle *handle, bool is_error);

extern RemoteQueryState *ExecInitRemoteQuery(RemoteQuery *node, EState *estate, int eflags);
extern TupleTableSlot* ExecRemoteQuery(PlanState *pstate);
extern void ExecReScanRemoteQuery(RemoteQueryState *node);
extern void ExecEndRemoteQuery(RemoteQueryState *step);
extern bool determine_param_types(Plan *plan,  struct find_params_context *context);
extern void ExecRemoteUtility(RemoteQuery *node);

extern bool	is_data_node_ready(PGXCNodeHandle * conn);

extern int pgxc_node_begin(int conn_count, PGXCNodeHandle ** connections,
						   GlobalTransactionId gxid, bool need_tran_block, bool is_global_session);

extern void pgxc_node_cleanup_and_release_handles(bool force, bool waitOnResGroup);
extern void pgxc_sync_connections(PGXCNodeAllHandles *all_handles, bool ignore_invalid);
extern void pgxc_node_clean_all_handles_distribute_msg(void);
extern int handle_response_internal(PGXCNodeHandle *conn, ResponseCombiner *combiner, char *file, int line);
#define handle_response(conn, combiner) \
		handle_response_internal(conn, combiner, __FILE__, __LINE__)
extern int handle_response_on_proxy(PGXCNodeHandle *conn, ResponseCombiner *combiner);
extern void HandleCmdComplete(CmdType commandType, CombineTag *combine, const char *msg_body,
									size_t len);
extern TupleTableSlot *FetchTuple(ResponseCombiner *combiner);
extern void InitResponseCombiner_internal(ResponseCombiner *combiner, int node_count,
					   CombineType combine_type, char* file, int line);
#define InitResponseCombiner(combiner, node_count, combine_type) \
			InitResponseCombiner_internal(combiner, node_count, combine_type, __FILE__, __LINE__)


extern bool validate_combiner(ResponseCombiner *combiner);
extern int pgxc_node_receive_on_proxy(PGXCNodeHandle *handle);
extern void CloseCombiner(ResponseCombiner *combiner);
extern bool ValidateAndCloseCombiner(ResponseCombiner *combiner);

extern void BufferConnection(PGXCNodeHandle *conn);
extern bool PreFetchConnection(PGXCNodeHandle *conn, int32 node_index);

extern void SetDataRowForExtParams(ParamListInfo params, RemoteQueryState *rq_state);

extern void ExecCloseRemoteStatement(const char *stmt_name, PGXCNodeAllHandles *handles);
extern char *PrePrepare_Remote(char *prepareGID, bool localNode, bool implicit);
extern void PreCommit_Remote(char *prepareGID, char *nodestring, bool preparedLocalNode);
extern bool	PreAbort_Remote(TranscationType txn_type, bool need_release_handle);
#ifdef __OPENTENBASE__
extern void SubTranscation_PreCommit_Remote(void);
extern void SubTranscation_PreAbort_Remote(void);
#endif
extern void AtEOXact_Remote(void);
extern void AtStart_Remote(void);
extern bool IsTwoPhaseCommitRequired(bool localWrite);
extern bool FinishRemotePreparedTransaction(char *prepareGID, bool commit);
extern char *GetImplicit2PCGID(const char *implicit2PC_head, bool localWrite);

extern void pgxc_all_success_nodes(ExecNodes **d_nodes, ExecNodes **c_nodes, char **failednodes_msg);
extern void AtEOXact_DBCleanup(bool isCommit);

extern void set_dbcleanup_callback(xact_callback function, void *paraminfo, int paraminfo_size);
#ifdef __OPENTENBASE__
extern void ExecRemoteQueryInitializeDSM(RemoteQueryState *node, ParallelContext *pcxt);
extern void ExecRemoteQueryInitializeDSMWorker(RemoteQueryState *node,
											   ParallelWorkerContext *pwcxt);

extern bool ExecRemoteDML(ModifyTableState *mtstate, ItemPointer tupleid, HeapTuple oldtuple,
		      TupleTableSlot *slot, TupleTableSlot *planSlot, EState *estate, EPQState *epqstate,
		      bool canSetTag, TupleTableSlot **returning, UPSERT_ACTION *result,
		      ResultRelInfo *resultRelInfo, int rel_index);
extern bool ExecSimpleRemoteDML(HeapTuple ituple, HeapTuple dtuple, Datum *diskey, bool *disnull,
								int ndiscols, char *relname, char *space, Locator *locator,
								CmdType cmdtype, GlobalTimestamp private_gts, ItemPointer target_ctid);
extern TupleTableSlot *ExecRemoteTidFetch(TidScanState *node, int nodeid, Datum ctid, Oid tableOid);
extern TupleTableSlot *ExecLocalTidFetch(TidScanState *node, Datum ctid, Oid tableOid);

extern TupleDesc create_tuple_desc(char *msg_body, size_t len);
#endif
extern void CopyDataRowTupleToSlot(ResponseCombiner *combiner, TupleTableSlot *slot);

#ifdef __SUBSCRIPTION__
extern void pgxc_node_report_error(ResponseCombiner *combiner);
extern int pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle ** connections,
						 struct timeval * timeout, ResponseCombiner *combiner);
extern bool validate_combiner(ResponseCombiner *combiner);
#endif

#ifdef __TWO_PHASE_TRANS__
extern void InitLocalTwoPhaseState(void);
extern void CheckLocalTwoPhaseState(void);
extern void SetLocalTwoPhaseStateHandles(PGXCNodeAllHandles * handles);
extern void UpdateLocalTwoPhaseState(int result, PGXCNodeHandle * response_handle, int conn_index, char * errmsg);
extern void ClearLocalTwoPhaseState(void);
extern char *GetTransStateString(TwoPhaseTransState state);
extern char *GetConnStateString(ConnState state);
extern void get_partnodes(PGXCNodeAllHandles * handles, StringInfo participants);
extern void clean_stat_transaction(void);
extern void pgxc_connections_cleanup(ResponseCombiner *combiner, bool abort);
#endif

extern void ExecSendStats(AnalyzeRelStats *analyzeStats);
extern void check_for_release_handles(bool need_release_handle);
extern bool ProcessResponseMessage(ResponseCombiner *combiner, struct timeval *timeout, int wait_event, bool ignore_error);
extern void DrainConnectionAndSetNewCombiner(PGXCNodeHandle *conn, ResponseCombiner *combiner);
extern void SendGucsToExistedConnections(char *gucs);

#endif
