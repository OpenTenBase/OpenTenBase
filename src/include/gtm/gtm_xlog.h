/*-------------------------------------------------------------------------
 *
 * gtm_xlog.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/gtm_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_XLOG_H
#define GTM_XLOG_H

#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_checkpoint.h"
#include "gtm/heap.h"
#include "access/xlogdefs.h"
#include "gtm/gtm_xlog_internal.h"

#define NUM_XLOGINSERT_LOCKS  8

#define XLOG_KEEP_ALIVE_TIME 10
typedef uint64 offset_t;

typedef struct XLogCtlInsert
{    
    s_lock_t    insertpos_lck;           /* protects CurrBytePos and PrevBytePos */     
    uint64      CurrBytePos;             /* usable bytes which exclude page header */    
    uint64      PrevBytePos;    
    char        pad[PG_CACHE_LINE_SIZE]; /* improve preformance in cpu cache line */
} XLogCtlInsert;

typedef struct XLogInsertLock
{
    GTM_MutexLock    l;
    s_lock_t         m;                /* protect start, end */
    XLogRecPtr       start;
    XLogRecPtr       end;
    char             pad[PG_CACHE_LINE_SIZE];
} XLogInsertLock;

typedef struct XLogwrtResult
{
    XLogRecPtr    Write;            /* last byte + 1 written out */
    XLogRecPtr    Flush;            /* last byte + 1 flushed */
} XLogwrtResult;

enum XLogSendResult
{
    Send_OK                   = 1,
    Send_No_data              = 0,
    Send_Data_Not_Found       = -1,
    Send_Error                = -2,
    Send_XlogFile_Not_Found   = -3
};

typedef struct XLogCtlData
{
    XLogCtlInsert  Insert;

    char           writerBuff[XLOG_SEG_SIZE];

    XLogInsertLock insert_lck[NUM_XLOGINSERT_LOCKS];
    
    GTM_RWLock     segment_lck;
    uint64         currentSegment;

    s_lock_t         segment_gts_lck;
    GlobalTimestamp  segment_max_gts;
    pg_time_t        segment_max_timestamp;

    GTM_MutexLock  walwrite_lck;
    uint64         last_write_idx;
    
    s_lock_t       walwirte_info_lck;
    XLogwrtResult  LogwrtResult;

    int            xlog_fd;

    s_lock_t       timeline_lck;
    TimeLineID     thisTimeLineID;

    /* standby mode */
    GTM_RWLock     standby_info_lck;
    XLogRecPtr     write_buff_pos;   /* how far we write xlog in buff */
    XLogRecPtr     apply;            /* how far we redo xlog */
    
} XLogCtlData;

typedef struct XLogSyncConfig {
	GTM_MutexLock    lck;
    volatile sig_atomic_t  required_sync_num;
    gtm_List *sync_application_targets;
} XLogSyncConfig;

typedef struct XLogSyncStandby
{
    gtm_List *sync_standbys; /* alive standbys which means they are in walsenders */

    GTM_MutexLock check_mutex;
    int           head_xlog_hints;
    XLogRecPtr    head_ptr;   /* temp of the head of wait_queue , not always correct */
	XLogRecPtr    synced_lsn;

    GTM_MutexLock wait_queue_mutex;
    heap wait_queue;
} XLogSyncStandby;

extern XLogCtlData     *XLogCtl;
extern ControlFileData *ControlData;
extern ssize_t          g_GTMControlDataSize;
extern GTM_RWLock       ControlDataLock;
extern XLogSyncStandby  *XLogSync;
extern XLogSyncConfig   *SyncConfig;
extern volatile bool  SyncReady;
extern const char *g_gtm_state_string[];


extern XLogRecPtr GetStandbyWriteBuffPos(void);
extern XLogRecPtr GetStandbyApplyPos(void);

extern void UpdateStandbyWriteBuffPos(XLogRecPtr pos);
extern void UpdateStandbyApplyPos(XLogRecPtr pos);

extern void WaitXLogWriteUntil(XLogRecPtr write_pos);

XLogRecPtr GetXLogFlushRecPtr(void);

extern bool  CopyXLogRecordToBuff(char *data,XLogRecPtr start,XLogRecPtr end,uint64 size);
extern void  GTM_ThreadWalRedoer_Internal();

/*
 * Request xlog status
 */
extern bool GTM_SendGetReplicationStatusRequest(GTM_Conn *conn);

extern int GTM_GetReplicationResultIfAny(GTM_StandbyReplication *replication,Port *port);

extern bool SendXLogContext(GTM_StandbyReplication *replication,Port *port);

extern void ProcessStartReplicationCommand(Port *myport, StringInfo message);

extern bool GTM_HasXLogToSend(GTM_StandbyReplication *replication);

extern bool GTM_GetReplicationXLogData();

extern XLogwrtResult GetCurrentXLogwrtResult(void);

/*
 * Xlog Init Function.
 */
extern int  GTM_ControlDataInit(void);
extern void GTM_XLogRecovery(XLogRecPtr startPos, const char *data_dir, bool after_overwrite);
extern void GTM_XLogCtlDataInit(void);
extern void GTM_XLogFileInit(char *data_dir);
extern void ControlDataSync(bool update_time);
extern void GTM_XLogSyncDataInit(void);

extern TimeLineID GetCurrentTimeLineID(void);

extern void NotifyReplication(XLogRecPtr ptr);
extern void WaitSyncComplete(XLogRecPtr ptr);
extern void CheckSyncReplication(GTM_StandbyReplication *replication,XLogRecPtr ptr);

extern void BeforeReplyToClientXLogTrigger(void);
extern void XLogInsterInit(void);
extern void XLogCtlShutDown(void);

/*
 * Xlog command registers.
 */ 
extern void XLogRegisterRangeOverwrite(offset_t offset,
                                  int32_t len,char *data);
extern void XLogRegisterCheckPoint(void);
extern void XLogRegisterTimeStamp(void);

extern void DoCheckPoint(bool shutdown);
extern bool XLogBackgroundFlush(void);
/*
 * Xlog insert related command.
 */ 
extern void XLogBeginInsert(void);
extern XLogRecPtr XLogInsert(void);
extern void XLogFlush(XLogRecPtr ptr);

extern void OpenMapperFile(const char *data_dir);
extern void CloseMapperFile(void);
bool CheckMapperFile(const char *file_path, size_t file_size);

extern void DoSlaveCheckPoint(bool  write_check_point);
extern void DoMasterCheckPoint(bool shutdown);

extern uint32 PrintRedoRangeOverwrite(XLogCmdRangerOverWrite *cmd);
extern uint32 PrintRedoCheckPoint(XLogCmdCheckPoint *cmd);
extern uint32 PrintRedoTimestamp(XLogRecGts *cmd);

extern TimeLineID GetCurrentTimeLineID(void);
extern void SetCurrentTimeLineID(TimeLineID timeline);
extern bool XLogInCurrentSegment(XLogRecPtr pos);
extern void SwitchXLogFile(void);
extern void NewXLogFile(XLogSegNo segment_no);
extern XLogSegNo  GetSegmentNo(XLogRecPtr ptr);

extern GTM_StandbyReplication *g_StandbyReplication;

extern GTM_Conn *GTM_ActiveConn;

void GTM_StandbyBaseinit(void);

extern bool gtm_standby_resign_to_walsender(Port *port,const char *node_name,const char *replication_name);
extern GTM_StandbyReplication * register_self_to_standby_replication(void);

extern void gtm_close_replication(GTM_StandbyReplication *replication);

extern void GTM_ThreadWalRedoer_Internal(void);

extern XLogRecPtr GetReplicationFlushPtr(GTM_StandbyReplication *replication);

extern XLogSegNo  GetCurrentSegmentNo(void);
extern bool       IsXLogFileExist(const char *file);

extern void ValidXLogRecoveryCondition(void);
extern XLogRecPtr GetMinReplicationRequiredLocation(void);

extern char* GetFormatedCommandLine(char *buff,int size,const char *data,char *file_name,char *relative_path);

extern bool IsInSyncStandbyList(const char *application_name);
extern void RegisterNewSyncStandby(GTM_StandbyReplication *replication);
extern void RemoveSyncStandby(GTM_StandbyReplication *replication);
extern void load_sync_structures(void);
extern void GTM_RecoveryUpdateMetaData(XLogRecPtr redo_end_pos,XLogRecPtr preXLogRecord, uint64 segment_no, int idx);
extern uint64 XLogRecPtrToBytePos(XLogRecPtr ptr);

#endif /* GTM_XLOG_H */

