/*-------------------------------------------------------------------------
 *
 * gtm_client.h
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_CLIENT_H
#define GTM_CLIENT_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_msg.h"
#include "gtm/register.h"
#include "gtm/libpq-fe.h"
#include "access/xlogdefs.h"
#include "gtm/gtm_stat.h"
#ifdef __OPENTENBASE_C__
#include "gtm/gtm_fid.h"
#endif

#ifdef __RESOURCE_QUEUE__
#include "gtm/gtm_resqueue.h"
#endif

#define MAX_HOSTADDR_LEN 32
#define MAX_PORT_LEN     8

typedef union GTM_ResultData
{
	GTM_TransactionHandle		grd_txnhandle;	/* TXN_BEGIN */

	bool 						backup_result;		/* BEGIN_BACKUP result */
	struct
	{
		GlobalTransactionId		gxid;
		GTM_Timestamp			timestamp;
	} grd_gxid_tp;								/* TXN_BEGIN_GETGXID */				
	

	struct
	{		
		GTM_Timestamp		        grd_gts;            /* GETGTS or when CHECK_GTM  GTS from primary GTM. */
		bool                        gtm_readonly;       /* read only mode for gtm */
		int							node_status;		/* Master or Slave, 0:master, 1 slave */
#ifndef __XLOG__
		GTM_Timestamp		        grd_gts_standby;    /* CHECK_GTM, GTS from standby. */
		char                        standbyhost[MAX_HOSTADDR_LEN];
		char						standbyport[MAX_PORT_LEN];
#else
        XLogRecPtr                  master_flush;

        int                         standby_count;
		int                         *slave_is_sync;
        char                        *application_name[GTM_MAX_WALSENDER];
		XLogRecPtr                  *slave_flush_ptr;
		GTM_Timestamp               *slave_timestamp;
#endif
	}grd_gts;

#ifdef __XLOG__

	struct
	{		
		XLogRecPtr             flush;
		XLogRecPtr             write;
		XLogRecPtr             apply;
	} grd_replication;

	struct
	{
    	XLogRecPtr             pos;
    	int                    length;
    	char*                  xlog_data;
    	int                    reply;
    	XLogRecPtr             flush;
        int                    status;
    } grd_xlog_data;
	
#endif

	GlobalTransactionId			grd_gxid;			/* TXN_PREPARE		
													 * TXN_START_PREPARED
													 * TXN_ROLLBACK
													 */
	struct {
		GlobalTransactionId			gxid;
													/* TXN_COMMIT
													 * TXN_COMMIT_PREPARED
													 */
		int							status;
	} grd_eof_txn;

	GlobalTransactionId			grd_next_gxid;

	struct
	{
		GTM_TransactionHandle	txnhandle;
		GlobalTransactionId		gxid;
	} grd_txn;									/* TXN_GET_GXID */

	GTM_SequenceKeyData			grd_seqkey;		/* SEQUENCE_INIT
												 * SEQUENCE_RESET
												 * SEQUENCE_CLOSE */
	struct
	{
		GTM_SequenceKeyData		seqkey;
		GTM_Sequence			seqval;
		GTM_Sequence			rangemax;
	} grd_seq;									/* SEQUENCE_GET_CURRENT
												 * SEQUENCE_GET_NEXT */
	struct
	{
		int32					seq_count;
		GTM_SeqInfo			   *seq;
	} grd_seq_list;								/* SEQUENCE_GET_LIST */

	struct
	{  
		int32			     	txn_count; 				/* TXN_BEGIN_GETGXID_MULTI */
		GlobalTransactionId		txn_gxid[GTM_MAX_GLOBAL_TRANSACTIONS];
		GTM_Timestamp			timestamp;
	} grd_txn_get_multi;

	struct
	{
		int				ts_count; 				/* GETGTS_MULTI */
		GTM_Timestamp		gts[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_gts_get_multi;

	struct
	{
		int				txn_count;				/* TXN_COMMIT_MULTI */
		int				status[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_txn_rc_multi;

	struct
	{
		GTM_TransactionHandle	txnhandle;		/* SNAPSHOT_GXID_GET */
		GlobalTransactionId		gxid;			/* SNAPSHOT_GET */
		int						txn_count;		/* SNAPSHOT_GET_MULTI */
		int						status[GTM_MAX_GLOBAL_TRANSACTIONS];
	} grd_txn_snap_multi;

	struct
	{
		GlobalTransactionId		gxid;
		GlobalTransactionId		prepared_gxid;
		int				nodelen;
		char			*nodestring;
	} grd_txn_get_gid_data;					/* TXN_GET_GID_DATA_RESULT */

	struct
	{
		char				*ptr;
		int  				len;
	} grd_txn_gid_list;						/* TXN_GXID_LIST_RESULT */

	struct
	{
		GTM_PGXCNodeType	type;			/* NODE_REGISTER */
		int  				len;
		char				*node_name;		/* NODE_UNREGISTER */
		GlobalTransactionId xmin;
	} grd_node;

	struct
	{
		int				num_node;
		GTM_PGXCNodeInfo		*nodeinfo[MAX_NODES];
	} grd_node_list;

	struct
	{
		GlobalTransactionId		latest_completed_xid;
		GlobalTransactionId		global_xmin;
		int						errcode;
	} grd_report_xmin;						/* REPORT_XMIN */

#ifdef __RESOURCE_QUEUE__
	GTMStorageResQueueStatus	grd_resqret;	/* RESQUEUE_INIT
												 * RESQUEUE_RESET
												 * RESQUEUE_CLOSE 
												 * RESQUEUE_IF_EXISTS
												 */
#endif

	GTM_StatisticsResult statistic_result;
	/*
	 * TODO
	 * 	TXN_GET_STATUS
	 * 	TXN_GET_ALL_PREPARED
	 */
} GTM_ResultData;

#define GTM_RESULT_COMM_ERROR (-2) /* Communication error */
#define GTM_RESULT_ERROR      (-1)
#define GTM_RESULT_OK         (0)
/*
 * This error is used ion the case where allocated buffer is not large
 * enough to store the errors. It may happen of an allocation failed
 * so it's status is considered as unknown.
 */
#define GTM_RESULT_UNKNOWN    (1)

typedef struct GTM_Result
{
	GTM_ResultType		gr_type;
	int					gr_msglen;
	int					gr_status;
	GTM_ProxyMsgHeader	gr_proxyhdr;
	GTM_ResultData		gr_resdata;
	
#ifdef __OPENTENBASE__	
	struct
	{
		uint32					org_len;
		uint32					c_len;
		char				   *data;
#ifdef __XLOG__
		XLogRecPtr              start_pos;
		TimeLineID              time_line;
#endif
	} grd_storage_data; 					/* STORAGE_TRANSFER_RESULT */
	int							gr_finish_status;	/* TXN_FINISH_GID_RESULT result */
	GTMStorageStatus            gtm_status;
	
	struct 
	{
		int32					count;
		GTM_StoredSeqInfo       *seqs;
	}grd_store_seq;

	struct 
	{
		int32							 count;
		GTM_StoredTransactionInfo       *txns;
	}grd_store_txn;


	struct 
	{
		int32							count;
		GTMStorageSequneceStatus       *seqs;
	}grd_store_check_seq;

	struct 
	{
		int32							count;
		GTMStorageTransactionStatus     *txns;
	}grd_store_check_txn;
	
	struct
    {
        int len;
        char* errlog;
    } grd_errlog;
	
#endif
#ifdef __OPENTENBASE_C__
	struct
	{
		int32							fid_count;
		int 							*fids;
		GTM_Fid							*fidstatus;
	}grd_store_fid;
#endif

#ifdef __RESOURCE_QUEUE__
	struct 
	{
		int32							count;
		GTM_StoredResQueueDataInfo		*resqs;
	}grd_store_resq;							/* MSG_LIST_GTM_STORE_RESQUEUE */

	struct 
	{
		int32							count;
		GTMStorageResQueueStatus		*resqs;
	}grd_store_check_resq;						/* MSG_CHECK_GTM_STORE_RESQUEUE */

	struct
	{
		int32							count;
		GTM_ResQueueInfo				*resqs;
	} grd_resq_list;							/* RESQUEUE_GET_LIST */

	struct
	{
		int32							count;
		GTM_ResQUsageInfo				*usages;
	} grd_resq_usage;							/* MSG_LIST_RESQUEUE_USAGE */
#endif

	/*
	 * We keep these two items outside the union to avoid repeated malloc/free
	 * of the xip array. If these items are pushed inside the union, they may
	 * get overwritten by other members in the union
	 */
	int					gr_xip_size;
	GTM_SnapshotData	gr_snapshot;

	/*
	 * Similarly, keep the buffer for proxying data outside the union
	 */
	char		*gr_proxy_data;
	int			gr_proxy_datalen;
} GTM_Result;

typedef struct Get_GTS_Result {

    GTM_Timestamp		        gts;    /* GETGTS or when CHECK_GTM  GTS from primary GTM. */
    bool                        gtm_readonly;   /* read only mode for gtm */
} Get_GTS_Result;

typedef struct _GetStoreFileSt
{
	uint32					org_len;
	uint32					c_len;
	char				   *data;
#ifdef __XLOG__
	XLogRecPtr              start_pos;
	TimeLineID              time_line;
#endif
} GetStoreFileSt;

/*
 * Connection Management API
 */
GTM_Conn *connect_gtm(const char *connect_string);
void disconnect_gtm(GTM_Conn *conn);

int begin_replication_initial_sync(GTM_Conn *);
int end_replication_initial_sync(GTM_Conn *);

size_t get_node_list(GTM_Conn *, GTM_PGXCNodeInfo *, size_t);
GlobalTransactionId get_next_gxid(GTM_Conn *);
uint32 get_txn_gxid_list(GTM_Conn *, GTM_Transactions *);
size_t get_sequence_list(GTM_Conn *, GTM_SeqInfo **);

/*
 * Transaction Management API
 */
GlobalTransactionId begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel,
						  const char *global_sessionid,
						  GTM_Timestamp *timestamp);


int bkup_begin_transaction(GTM_Conn *conn, GTM_IsolationLevel isolevel,
						   bool read_only, const char *global_sessionid,
						   uint32 client_id, GTM_Timestamp timestamp);
#ifdef __OPENTENBASE__
Get_GTS_Result get_global_timestamp(GTM_Conn *conn);
#ifdef __XLOG__
int check_gtm_status(GTM_Conn *conn, int *status, GTM_Timestamp *master,XLogRecPtr *master_ptr,
					 int *standby_count,int **slave_is_sync, GTM_Timestamp **standby ,
					 XLogRecPtr **slave_flush_ptr,char **application_name[GTM_MAX_WALSENDER],int timeout_seconds);
#else
int check_gtm_status(GTM_Conn *conn, int *status, GTM_Timestamp *master, GTM_Timestamp *standby, char *standbyhost, char *standbyport, int32 buflen);
#endif
int bkup_global_timestamp(GTM_Conn *conn, GlobalTimestamp timestamp);
int get_gtm_statistics(GTM_Conn *conn, int clear_flag, int timeout_seconds, GTM_StatisticsResult** result);
int get_gtm_errlog(GTM_Conn *conn, int timeout_seconds, char** errlog, int* len);

#endif

int bkup_begin_transaction_gxid(GTM_Conn *conn, GlobalTransactionId gxid,
								GTM_IsolationLevel isolevel, bool read_only,
								const char *global_sessionid,
								uint32 client_id, GTM_Timestamp timestamp);

GlobalTransactionId begin_transaction_autovacuum(GTM_Conn *conn, GTM_IsolationLevel isolevel);
int bkup_begin_transaction_autovacuum(GTM_Conn *conn, GlobalTransactionId gxid,
									  GTM_IsolationLevel isolevel,
									  uint32 client_id);
int commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid,
					   int waited_xid_count,
					   GlobalTransactionId *waited_xids);
int bkup_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid,
								GlobalTransactionId prepared_gxid,
								int waited_xid_count,
								GlobalTransactionId *waited_xids);
int bkup_commit_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid);
int abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int bkup_abort_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int start_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
							   char *nodestring);
int
log_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid,const char *gid,
						   const char *nodestring, int node_count, bool isGlobal, bool isCommit, 
						   GlobalTimestamp prepare_ts, GlobalTimestamp commit_ts);
int
log_scan_transaction(GTM_Conn *conn,
							 GlobalTransactionId gxid, 
 							 const char *node_string, 
 							 GlobalTimestamp	 start_ts,
 							 GlobalTimestamp	 local_start_ts,
 							 GlobalTimestamp	 local_complete_ts,
 							 int scan_type,
 							 const char *rel_name,
							 int64  scan_number);


int backup_start_prepared_transaction(GTM_Conn *conn, GlobalTransactionId gxid, char *gid,
									  char *nodestring);
int prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int bkup_prepare_transaction(GTM_Conn *conn, GlobalTransactionId gxid);
int get_gid_data(GTM_Conn *conn, GTM_IsolationLevel isolevel, char *gid,
				 GlobalTransactionId *gxid,
				 GlobalTransactionId *prepared_gxid,
				 char **nodestring);
/*
 * Multiple Transaction Management API
 */
int
begin_transaction_multi(GTM_Conn *conn, int txn_count, GTM_IsolationLevel *txn_isolation_level,
			bool *txn_read_only, GTMProxy_ConnID *txn_connid,
			int *txn_count_out, GlobalTransactionId *gxid_out, GTM_Timestamp *ts_out);
int
bkup_begin_transaction_multi(GTM_Conn *conn, int txn_count,
							 GlobalTransactionId *gxid, GTM_IsolationLevel *isolevel,
							 bool *read_only,
							 const char *txn_global_sessionid[], 
							 uint32 *client_id,
							 GTMProxy_ConnID *txn_connid);
int
commit_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
						 int *txn_count_out, int *status_out);
int
bkup_commit_transaction_multi(GTM_Conn *conn, int txn_count,
		GlobalTransactionId *gxid);
int
abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
			int *txn_count_out, int *status_out);
int
bkup_abort_transaction_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid);
int
snapshot_get_multi(GTM_Conn *conn, int txn_count, GlobalTransactionId *gxid,
		   int *txn_count_out, int *status_out,
		   GlobalTransactionId *xmin_out, GlobalTransactionId *xmax_out,
		   GlobalTransactionId *recent_global_xmin_out, int32 *xcnt_out);

/*
 * Snapshot Management API
 */
GTM_SnapshotData *get_snapshot(GTM_Conn *conn, GlobalTransactionId gxid,
		bool canbe_grouped);

/*
 * Node Registering management API
 */
int node_register(GTM_Conn *conn,
				  GTM_PGXCNodeType type,
				  GTM_PGXCNodePort port,
				  char *node_name,
				  char *datafolder);
int node_register(GTM_Conn *conn, GTM_PGXCNodeType type, GTM_PGXCNodePort port,
		char *node_name, char *datafolder);
int node_register_internal(GTM_Conn *conn, GTM_PGXCNodeType type, const char *host,	GTM_PGXCNodePort port, char *node_name,
						   char *datafolder, GTM_PGXCNodeStatus status);
int bkup_node_register_internal(GTM_Conn *conn, GTM_PGXCNodeType type, const char *host, GTM_PGXCNodePort port,
								char *node_name, char *datafolder,
								GTM_PGXCNodeStatus status);

int node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char *node_name);
int bkup_node_unregister(GTM_Conn *conn, GTM_PGXCNodeType type, const char * node_name);
int backend_disconnect(GTM_Conn *conn, bool is_postmaster, GTM_PGXCNodeType type, char *node_name);
char *node_get_local_addr(GTM_Conn *conn, char *buf, size_t buflen, int *rc);
int register_session(GTM_Conn *conn, const char *coord_name, int coord_procid,
				 int coord_backendid);
int report_global_xmin(GTM_Conn *conn, const char *node_name,
		GTM_PGXCNodeType type, GlobalTransactionId gxid,
		GlobalTransactionId *global_xmin,
		GlobalTransactionId *latest_completed_xid,
		int *errcode);

/*
 * Sequence Management API
 */
int open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				  GTM_Sequence minval, GTM_Sequence maxval,
				  GTM_Sequence startval, bool cycle,
				  GlobalTransactionId gxid, bool nocache, bool is_order);
int bkup_open_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
					   GTM_Sequence minval, GTM_Sequence maxval,
					   GTM_Sequence startval, bool cycle,
					   GlobalTransactionId gxid, bool nocache, bool is_order);
int alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
				   GTM_Sequence minval, GTM_Sequence maxval,
				   GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart, bool nocache, bool is_order);
int bkup_alter_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_Sequence increment,
						GTM_Sequence minval, GTM_Sequence maxval,
						GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart, bool nocache, bool is_order);
int close_sequence(GTM_Conn *conn, GTM_SequenceKey key, GlobalTransactionId gxid);
int bkup_close_sequence(GTM_Conn *conn, GTM_SequenceKey key, GlobalTransactionId gxid);
int rename_sequence(GTM_Conn *conn, GTM_SequenceKey key,
						GTM_SequenceKey newkey, GlobalTransactionId gxid);
int copy_database_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey,
                           GlobalTransactionId gxid);
int bkup_rename_sequence(GTM_Conn *conn, GTM_SequenceKey key,
						GTM_SequenceKey newkey, GlobalTransactionId gxid);
int get_current(GTM_Conn *conn, GTM_SequenceKey key,
			char *coord_name, int coord_procid, GTM_Sequence *result);
int get_next(GTM_Conn *conn, GTM_SequenceKey key,
		 char *coord_name, int coord_procid,
		 GTM_Sequence range, GTM_Sequence *result, GTM_Sequence *rangemax);
int bkup_get_next(GTM_Conn *conn, GTM_SequenceKey key,
		 char *coord_name, int coord_procid,
		 GTM_Sequence range, GTM_Sequence *result, GTM_Sequence *rangemax);
int set_val(GTM_Conn *conn, GTM_SequenceKey key, char *coord_name,
		int coord_procid, GTM_Sequence nextval, bool iscalled);
int bkup_set_val(GTM_Conn *conn, GTM_SequenceKey key, char *coord_name,
			 int coord_procid, GTM_Sequence nextval, bool iscalled);
int reset_sequence(GTM_Conn *conn, GTM_SequenceKey key);
int bkup_reset_sequence(GTM_Conn *conn, GTM_SequenceKey key);

int clean_session_sequence(GTM_Conn *conn, char *coord_name, int coord_procid);


/*
 * Barrier
 */
int report_barrier(GTM_Conn *conn, const char *barier_id);
int bkup_report_barrier(GTM_Conn *conn, char *barrier_id);

/*
 * GTM-Standby
 */
int set_begin_end_backup(GTM_Conn *conn, bool begin);

int set_begin_backup(GTM_Conn *conn, int64 identifier, int64 lsn, GlobalTimestamp gts);
int set_end_backup(GTM_Conn *conn, bool begin);
int gtm_sync_standby(GTM_Conn *conn);

#ifdef __XLOG__
int set_begin_replication(GTM_Conn *conn,const char *application_name,const char *node_name);
#endif


#ifdef __OPENTENBASE__
/*
 * GTM-Storage
 */
int get_storage_file(GTM_Conn *conn, GetStoreFileSt *store_file_info);
int finish_gid_gtm(GTM_Conn *conn, char *gid);
int get_gtm_store_status(GTM_Conn *conn, GTMStorageStatus *header);
int32 get_storage_sequence_list(GTM_Conn *conn, GTM_StoredSeqInfo **store_seq);
int32 get_storage_transaction_list(GTM_Conn *conn, GTM_StoredTransactionInfo **store_txn);
int32 check_storage_sequence(GTM_Conn *conn, GTMStorageSequneceStatus **store_seq, bool need_fix);
int32 check_storage_transaction(GTM_Conn *conn, GTMStorageTransactionStatus **store_txn, bool need_fix);
int   rename_db_sequence(GTM_Conn *conn, GTM_SequenceKey key, GTM_SequenceKey newkey, GlobalTransactionId gxid);
#endif
void gtmpqFreeResultResource(GTM_Result *result);

#endif
#ifdef __OPENTENBASE_C__
int32 acquire_fragment_id(GTM_Conn *conn, int *fids, int nodeId, int size, time_t nodeStartTs);
int32 release_fragment_id(GTM_Conn *conn, int *fids, int nodeId, int size);
int32 release_node_fragment_id(GTM_Conn *conn, int nodeId);
int32 list_fragment_id(GTM_Conn *conn, int **fids, int nodeId);
int32 keepalive_fragment_id(GTM_Conn *conn, int nodeId, time_t nodeStartTs);
int32 list_alive_fragment_id_status(GTM_Conn *conn, GTM_Fid **fids);
int32 list_all_fragment_id_status(GTM_Conn *conn, GTM_Fid **fids);

#endif

#ifdef __RESOURCE_QUEUE__
int
resqueue_open(GTM_Conn *conn,
				NameData * resq_name,
				NameData * resq_group,
				int64	resq_memory_limit,
				int64	resq_network_limit,
				int32	resq_active_stmts,
				int16	resq_wait_overload,
				int16	resq_priority);
int
resqueue_alter(GTM_Conn *conn,
				NameData * resq_name,
				NameData * resq_group,
				int64	resq_memory_limit,
				int64	resq_network_limit,
				int32	resq_active_stmts,
				int16	resq_wait_overload,
				int16	resq_priority,
				char alter_name,
				char alter_group,
				char alter_memory_limit,
				char alter_network_limit,
				char alter_active_stmts,
				char alter_wait_overload,
				char alter_priority);
int
resqueue_close(GTM_Conn *conn, NameData * resq_name);

int
resqueue_check_if_exists(GTM_Conn *conn, NameData * resq_name, bool * exists);

int 
resqueue_move_conn(GTM_Conn *conn, const char *node_name, int node_procid,
				 		int node_backendid, int32 *move_success);
int 
resqueue_acquire(GTM_Conn * conn, 
						const char * node_name,
						int node_procid,
				 		int node_backendid,
				 		NameData * resq_name,
				 		int64 memory_Bytes,
						NameData * ret_name,
						NameData * ret_group,
						int64 * ret_memory_limit,
						int64 * ret_network_limit,
						int32 * ret_active_stmts,
						int16 * ret_wait_overload,
						int16 * ret_priority,
						int32 * errcode);
int
resqueue_release(GTM_Conn *conn, 
						const char *node_name,
						int node_procid,
				 		int node_backendid,
				 		NameData * resq_name, 
				 		int64 memory_Bytes,
				 		int32 * errcode);

int32
get_resqueue_list(GTM_Conn *conn, GTM_ResQueueInfo **resq_list);

int32
get_storage_resqueue_list(GTM_Conn *conn, GTM_StoredResQueueDataInfo **store_resq);

int32
check_storage_resqueue(GTM_Conn *conn, GTMStorageResQueueStatus **store_resq, bool need_fix);

int32
get_resqueue_usage(GTM_Conn *conn, NameData * resq_name, GTM_ResQUsageInfo **usages);

#endif
