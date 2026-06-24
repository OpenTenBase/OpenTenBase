#ifndef GTM_XLOG_INTERNAL_H
#define GTM_XLOG_INTERNAL_H

#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h> 
#include <sys/types.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_checkpoint.h"
#include "libpq-be.h"
#include "utils/pg_crc.h"

#define GTM_XLOG_PAGE_MAGIC     0xFFFF
#define GTM_XLOG_SEG_SIZE       (1024 * 1024 * 2) /* 2M */
#define GTM_XLOG_BLCKSZ         (4 * 1024) /* 4k */

#define GTM_MAX_COMMAND_SIZE 1024

#define FIRST_TIMELINE_ID   0x01

#define FIRST_XLOG_REC      GTM_XLOG_BLCKSZ
#define FIRST_USABLE_BYTE   UsableBytesInPage

#define GTMXLogSegmentsPerXLogId	(UINT64CONST(0x100000000) / GTM_XLOG_SEG_SIZE)

#define GTMArchiverCheckInterval (1000 * 200) /* 200ms */

#define XLOG_FNAME_LEN	   24

#define MAXFNAMELEN    	64

#define MAX_COMMAND_LEN 2048

#define XLOG_DIR    "gtm_xlog"

#define GTMXLogFileName(fname, tli, logSegNo)	\
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/%08X%08X%08X", tli,		\
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMRawXLogFileName(fname, tli, logSegNo)	\
        snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,      \
                (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
                (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMXLogFileNameWithoutGtmDir(fname, tli, logSegNo)	\
    snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define GTMXLogFileStatusReadyName(fname, tli, logSegNo)	\
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/archive_status/%08X%08X%08X.ready", tli,		\
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))
#define GTMXLogFileStatusDoneName(fname, tli, logSegNo)	\
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/archive_status/%08X%08X%08X.done", tli,		\
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))
#define GTMXLogFileGtsName(fname, tli, logSegNo)	\
    snprintf(fname, MAXFNAMELEN, "gtm_xlog/archive_status/%08X%08X%08X.gts", tli,		\
            (uint32) ((logSegNo) / GTMXLogSegmentsPerXLogId), \
            (uint32) ((logSegNo) % GTMXLogSegmentsPerXLogId))

#define IsXLogFileName(fname) \
	(strlen(fname) == XLOG_FNAME_LEN && \
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN)

#define IsPartialXLogFileName(fname)	\
	(strlen(fname) == XLOG_FNAME_LEN + strlen(".partial") &&	\
	 strspn(fname, "0123456789ABCDEF") == XLOG_FNAME_LEN &&		\
	 strcmp((fname) + XLOG_FNAME_LEN, ".partial") == 0)


#define RECOVERY_CONF_NAME "recovery.conf"
#define RECOVERY_CONF_NAME_DONE "recovery.done"

#define GTM_XLOG_RECOVERY_GTS_VALIDATE_NUM 5

typedef enum  
{ 
    XLOG_CMD_RANGE_OVERWRITE,   /* OverWrite range for map file */ 
    XLOG_CMD_CHECK_POINT,       /* CheckPoint record,Redo will find this record and start */
    XLOG_REC_GTS                /* Record gts in case of gts overflip */
} XlogRecType;

/*
 * The overall layout of an XLOG record is:
 *        Fixed-size header (XLogRecord struct)
 *        XLogCommand struct
 *        XLogCommand struct
 *        ...
 */
typedef struct XLogRecord
{
    uint32          xl_tot_len;     /* total len of entire record */
    XLogRecPtr      xl_prev;         /* ptr to previous record in log */
    uint8           xl_info;         /* flag bits, for further use,useless for now */
    pg_time_t       xl_time;
    GlobalTimestamp xl_timestamp;
    /* 2 bytes of padding here, initialize to zero */
    pg_crc32c     xl_crc;         /* CRC for this record */
} XLogRecord; 

#define MAX_XLOG_RECORD_SIZE (((Size) 0x3fffffff) - GTM_XLOG_BLCKSZ) /* define max gtm xlog record size as a little smaller than max size can alloced */

typedef struct XLogPageHeaderData
{
    uint16        xlp_magic;            /* magic value for correctness checks */
    uint16        xlp_info;             /* flag bits, see below */
    XLogRecPtr    xlp_pageaddr;         /* XLOG address of this page */
} XLogPageHeaderData;

#define UsableBytesInPage    (GTM_XLOG_BLCKSZ - sizeof(XLogPageHeaderData))
#define UsableBytesInSegment ((GTM_XLOG_SEG_SIZE/GTM_XLOG_BLCKSZ) * UsableBytesInPage)

/*
 * Data structure stored in xlog files.
 * structure in xlog file arrage looks like this exclude page header
 * XLogRecord XLogCommand XLogCommand XLogCommand
 */
typedef struct
{
    int32_t type;     /* indicate XlogRecType */
    int32_t len;      /* len of xlog cmd data */
    char data[0];
} XLogCommand;

#define XLogCommadLength(cmd) (sizeof(XLogCommand) + cmd->len)

typedef struct GTM_CheckStandbyStatusResult
{
    char                ip[MAXFNAMELEN];
    char                port[MAXFNAMELEN];

    XLogRecPtr          flush_ptr;
    XLogRecPtr          write_ptr;
    XLogRecPtr          replay_ptr;

    GTM_Timestamp timestamp;

} GTM_CheckStandbyStatusResult;

/*
 * The functions in gtm_xlog.c construct a chain of XLogRecData structs
 * to represent the final WAL record.
 */
typedef struct XLogRecData
{
    struct XLogRecData *next;    /* next struct in chain, or NULL */
    char      *data;             /* start of data to include */
    uint32    len;               /* length of data to include */
} XLogRecData;

/*
 * Record xlog data chain list.
 */
typedef struct
{
    uint32        rdata_len;        /* total length of data in rdata chain */
    XLogRecData   *rdata_head;      /* head of the chain of data registered with this block */
    XLogRecData   *rdata_rear;      /* end of the chain of data registered with this block */
} XLogRegisterBuff;
/*
 * Command header for all xlog command types.
 */
typedef struct
{
    uint32 type;
} XLogCmdHeader;

typedef struct
{
    XLogCmdHeader hdr;
    int32_t offset;     /* offset of the map file */
    int32_t bytes;
    char data[0];
} XLogCmdRangerOverWrite;

typedef struct
{
    XLogCmdHeader hdr;
    GlobalTimestamp gts;
    pg_time_t  time;
    TimeLineID timeline;
} XLogCmdCheckPoint;

typedef struct
{
    XLogCmdHeader hdr;
    GlobalTimestamp gts;
} XLogRecGts;

typedef struct
{
    uint64  segment_no;
    int     total_length;    /* indicate the total length of xlog ,-1 mean not read */
    char    buff[GTM_XLOG_SEG_SIZE];
} GTM_XLogSegmentBuff;

typedef struct GTM_ThreadInfo GTM_ThreadInfo ;

typedef struct
{
     GTM_MutexLock       lock;
     bool                is_use;
     char                application_name[MAXFNAMELEN];
     char                node_name[MAXFNAMELEN];

     /* protects flush_ptr,write_ptr,replay_ptr,time_line */
     GTM_MutexLock       pos_status_lck;
     GTM_CV              pos_update_cv;
     XLogRecPtr          flush_ptr;
     XLogRecPtr          write_ptr;
     XLogRecPtr          replay_ptr;
     TimeLineID          time_line;


     XLogRecPtr          send_ptr;

     GTM_CV              xlog_to_send;
     GTM_MutexLock       send_request_lck;
     XLogRecPtr          send_request;

     GTM_ThreadInfo      *walsender_thread;
     Port                *port;

     GTM_XLogSegmentBuff xlog_read_buff;

     bool                is_sync;

     XLogRecPtr          next_sync_pos;
     bool                sync_hint;

} GTM_StandbyReplication;

typedef struct XLogWaiter {
    XLogRecPtr pos;
    GTM_CV  cv;
    GTM_MutexLock lock;
    bool  finished;
} XLogWaiter;
#endif
