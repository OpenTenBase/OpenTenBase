/*-------------------------------------------------------------------------
 *
 * gtm.h
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 *      Module interfacing with GTM definitions
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACCESS_GTM_H
#define ACCESS_GTM_H

#include "gtm/gtm_c.h"
#ifdef __OPENTENBASE__
#include "fmgr.h"
#define GTM_NAME_LEN     SEQ_KEY_MAX_LENGTH
typedef struct
{
    char new[GTM_NAME_LEN];
    char old[GTM_NAME_LEN];
}RenameInfo;

typedef struct
{
    int32 gsk_type;
    char  new[GTM_NAME_LEN];
    char  old[GTM_NAME_LEN];
}DropInfo;

typedef struct
{
    int32 gsk_type;
    char  name[GTM_NAME_LEN];    
}CreateInfo;
#endif

/* Configuration variables */
extern char *GtmHost;
extern int GtmPort;
extern bool gtm_backup_barrier;

extern bool IsXidFromGTM;
extern GlobalTransactionId currentGxid;

#ifdef __OPENTENBASE__
extern char *NewGtmHost;
extern int     NewGtmPort;
#endif

extern int reconnect_gtm_retry_times;
extern int reconnect_gtm_retry_interval;

extern bool IsGTMConnected(void);
extern void InitGTM(void);
extern void CloseGTM(void);
extern GTM_Timestamp 
GetGlobalTimestampGTM(void);
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
                              GlobalTimestamp     start_ts,
                              GlobalTimestamp     local_start_ts,
                              GlobalTimestamp     local_complete_ts,
                              int    scan_type,
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
        bool cycle);
extern int AlterSequenceGTM(char *seqname, GTM_Sequence increment,
        GTM_Sequence minval, GTM_Sequence maxval, GTM_Sequence startval,
                            GTM_Sequence lastval, bool cycle, bool is_restart);
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
extern void  RegisterRenameSequence(char *new, char *old);
extern void  FinishSeqOp(bool commit);
extern int32 GetGTMCreateSeq(char **create_info);
extern int32 GetGTMDropSeq(char **drop_info);
extern int32 GetGTMRenameSeq(char **rename_info);
extern void  RestoreSeqCreate(CreateInfo *create_info, int32 count);
extern void  RestoreSeqDrop(DropInfo *drop_info_array, int32 count);
extern void  RestoreSeqRename(RenameInfo *rename_info_array, int32 count);
extern int   FinishGIDGTM(char *gid);
extern Datum pg_list_gtm_store(PG_FUNCTION_ARGS);
extern Datum pg_list_storage_sequence(PG_FUNCTION_ARGS);
extern Datum pg_list_storage_transaction(PG_FUNCTION_ARGS);
extern Datum pg_check_storage_sequence(PG_FUNCTION_ARGS);
extern Datum pg_check_storage_transaction(PG_FUNCTION_ARGS);
extern void  CheckGTMConnection(void);
extern int32 RenameDBSequenceGTM(const char *seqname, const char *newseqname);
#endif
#endif /* ACCESS_GTM_H */
