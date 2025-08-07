/*-------------------------------------------------------------------------
 *
 * gtm.h
 * 
 *	  Module interfacing with GTM definitions
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACCESS_GTM_H
#define ACCESS_GTM_H

#include "gtm/gtm_c.h"
#include "storage/s_lock.h"
#ifdef __OPENTENBASE__
#include "fmgr.h"
#define GTM_NAME_LEN     SEQ_KEY_MAX_LENGTH
typedef struct
{
	char new_[GTM_NAME_LEN];
	char old[GTM_NAME_LEN];
	int	 nestLevel;		/* xact nesting level of request */
}RenameInfo;

typedef struct
{
	int32 gsk_type;
	char  new_[GTM_NAME_LEN];
	char  old[GTM_NAME_LEN];
	int	  nestLevel;		/* xact nesting level of request */
}DropInfo;

typedef struct
{
	int32 gsk_type;
	char  name[GTM_NAME_LEN];
}CreateInfo;

typedef uint32 Bitmap;

typedef struct
{
	slock_t track_lock;
	bool	inited;
	int		pgxc_nodeid;
	time_t	node_start_ts;
}FragmentIdTrack;
#endif

/* Configuration variables */
extern char *GtmHost;
extern int GtmPort;
extern bool gtm_backup_barrier;

extern bool IsXidFromGTM;
extern GlobalTransactionId currentGxid;

#ifdef __OPENTENBASE__
extern char *NewGtmHost;
extern int	 NewGtmPort;

extern void ResetGtmInfo(void);
extern void GetMasterGtmInfo(void);
#endif

#ifdef __RESOURCE_QUEUE__
extern void ResQueue_CloseGTM(void);
#endif

extern int reconnect_gtm_retry_times;
extern int reconnect_gtm_retry_interval;
extern bool feedbackGts;
extern bool IsGTMConnected(void);
extern void InitGTM(void);
extern void CloseGTM(void);
extern GTM_Timestamp
GetGlobalTimestampLocal(void);
extern GTM_Timestamp 
GetGlobalTimestampGTM(void);
extern GTM_Timestamp
GetGlobalTimestampGTMDirectly(void);
GTM_Timestamp
GetGlobalTimestampGTMForCommit(void);
void
UpdatelocalCacheGtsUseFeedbackGts(uint64 feedbackGts);
extern GlobalTransactionId BeginTranGTM(GTM_Timestamp *timestamp, const char *globalSession);
extern GlobalTransactionId BeginTranAutovacuumGTM(void);
extern int CommitTranGTM(GlobalTransactionId gxid, int waited_xid_count,
		GlobalTransactionId *waited_xids);
extern int RollbackTranGTM(GlobalTransactionId gxid);
extern int StartPreparedTranGTM(GlobalTransactionId gxid,
								char *gid,
								char *nodestring);
extern int
LogCommitTranGTM(GlobalTransactionId gxid,
					 const char *gid,
					 const char *nodestring,
					 int  node_count,
					 bool isGlobal,
					 bool isCommit,
					 GlobalTimestamp prepare_timestamp,
					 GlobalTimestamp commit_timestamp);
extern int
LogScanGTM( GlobalTransactionId gxid, 
 							 const char *node_string, 
 							 GlobalTimestamp	 start_ts,
 							 GlobalTimestamp	 local_start_ts,
 							 GlobalTimestamp	 local_complete_ts,
 							 int	scan_type,
 							 const char *rel_name,
							 int64  scan_number);
extern int PrepareTranGTM(GlobalTransactionId gxid);
extern int GetGIDDataGTM(char *gid,
						 GlobalTransactionId *gxid,
						 GlobalTransactionId *prepared_gxid,
						 char **nodestring);
extern int CommitPreparedTranGTM(GlobalTransactionId gxid,
								 GlobalTransactionId prepared_gxid,
								 int waited_xid_count,
								 GlobalTransactionId *waited_xids);

extern GTM_Snapshot GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped);

/* Node registration APIs with GTM */
extern int RegisterGTM(GTM_PGXCNodeType type);
extern int UnregisterGTM(GTM_PGXCNodeType type);

/* Sequence interface APIs with GTM */
extern GTM_Sequence GetCurrentValGTM(char *seqname);
extern GTM_Sequence GetNextValGTM(char *seqname,
					GTM_Sequence range, GTM_Sequence *rangemax);
extern int SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled);
extern int CreateSequenceGTM(char *seqname, GTM_Sequence increment, 
		GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
		bool cycle, bool nocache, bool is_order);
extern int AlterSequenceGTM(char *seqname, GTM_Sequence increment,
		GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
							GTM_Sequence lastval, bool cycle, bool is_restart, bool nocache, bool is_order);
extern int DropSequenceGTM(char *name, GTM_SequenceKeyType type);
extern int RenameSequenceGTM(char *seqname, const char *newseqname);
extern int CopyDataBaseSequenceGTM(char *src_dbname, char *dest_dbname);
extern void CleanGTMSeq(void);
/* Barrier */
extern int ReportBarrierGTM(const char *barrier_id);
extern int ReportGlobalXmin(GlobalTransactionId gxid,
		GlobalTransactionId *global_xmin,
		GlobalTransactionId *latest_completed_xid);

#ifdef __OPENTENBASE__
extern void  RegisterSeqCreate(char *name, int32 type);
extern void  RegisterSeqDrop(char *name, int32 type);
extern void  RegisterSeqRename(char *new_, char *old);
extern void  FinishSeqOp(bool commit);
extern void AtSubCommit_seq(void);
extern void PostPrepare_seq(void);
extern int32 GetGTMCreateSeq(char **create_info);
extern int32 GetGTMDropSeq(char **drop_info);
extern int32 GetGTMRenameSeq(char **rename_info);
extern void  RestoreSeqCreate(CreateInfo *create_info, int32 count, bool iscommit);
extern void  RestoreSeqDrop(DropInfo *drop_info_array, int32 count, bool iscommit);
extern void  RestoreSeqRename(RenameInfo *rename_info_array, int32 count, bool iscommit);
extern int   FinishGIDGTM(char *gid);
extern Datum pg_list_gtm_store(PG_FUNCTION_ARGS);
extern Datum pg_list_storage_sequence(PG_FUNCTION_ARGS);
extern Datum pg_list_storage_transaction(PG_FUNCTION_ARGS);
extern Datum pg_check_storage_sequence(PG_FUNCTION_ARGS);
extern Datum pg_check_storage_transaction(PG_FUNCTION_ARGS);
extern void CheckGTMConnection(void);
extern int32 RenameDBSequenceGTM(const char *seqname, const char *newseqname);
#endif
#ifdef __OPENTENBASE_C__
extern bool enable_gts_check; /*guc */
#endif

#ifdef __RESOURCE_QUEUE__
typedef struct ResQueueInfo ResQueueInfo;
extern int CreateResQueueGTM(ResQueueInfo * resq_info);
extern int AlterResQueueGTM(ResQueueInfo * resq_info,
								char alter_name,
								char alter_group,
								char alter_memory_limit,
								char alter_network_limit,
								char alter_active_stmts,
								char alter_wait_overload,
								char alter_priority);
extern int DropResQueueGTM(NameData * resq_name);
extern int CheckIfExistsResQueueGTM(NameData * resq_name, bool * exists);
extern int AcquireResQueueGTM(NameData * resq_name, int64 memory_Bytes, ResQueueInfo * result, int32 * errcode);
extern int ReleaseResQueueGTM(NameData * resq_name, int64 memory_Bytes, int32 * errcode);
#endif

#endif /* ACCESS_GTM_H */
