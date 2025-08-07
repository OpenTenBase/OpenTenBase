/*
 * commit_ts.h
 *
 * PostgreSQL commit timestamp manager
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/commit_ts.h
 */
#ifndef COMMIT_TS_H
#define COMMIT_TS_H

#include "access/xlog.h"
#include "datatype/timestamp.h"
#include "replication/origin.h"
#include "utils/guc.h"

/*
 * Defines for CommitTs page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CommitTs page numbering also wraps around at
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE, and CommitTs segment numbering at
 * 0xFFFFFFFF/COMMIT_TS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCommitTs (see CommitTsPagePrecedes).
 */

/*
 * We need 8+2 bytes per xact.  Note that enlarging this struct might mean
 * the largest possible file name is more than 5 chars long; see
 * SlruScanDirectory.
 */
typedef struct CommitTimestampEntry
{
    TimestampTz global_timestamp;
    TimestampTz time;
    RepOriginId nodeid;
} CommitTimestampEntry;

#define SizeOfCommitTimestampEntry (offsetof(CommitTimestampEntry, nodeid) + \
									sizeof(RepOriginId))

#define COMMIT_TS_XACTS_PER_PAGE \
	(BLCKSZ / SizeOfCommitTimestampEntry)

#define TransactionIdToCTsPage(xid) \
	((xid) / (TransactionId) COMMIT_TS_XACTS_PER_PAGE)
#define TransactionIdToCTsEntry(xid)	\
	((xid) % (TransactionId) COMMIT_TS_XACTS_PER_PAGE)

#ifdef __OPENTENBASE__
/* We store the latest async LSN for each group of transactions */
#define TLOG_XACTS_PER_LSN_GROUP	32	/* keep this a power of 2 */
#define TLOG_LSNS_PER_PAGE	(COMMIT_TS_XACTS_PER_PAGE / TLOG_XACTS_PER_LSN_GROUP)

#define CTSGetLSNIndex(slotno, xid)	((slotno) * TLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) TLOG_LSNS_PER_PAGE))
#endif

extern bool track_commit_timestamp;

extern bool check_track_commit_timestamp(bool *newval, void **extra,
							 GucSource source);

extern void TransactionTreeSetCommitTsData(TransactionId xid, int nsubxids,
							   TransactionId *subxids, TimestampTz global_timestamp, TimestampTz timestamp, 
							   RepOriginId nodeid, bool write_xlog, XLogRecPtr lsn);
extern bool TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *gts,  
							 RepOriginId *nodeid);
extern bool TransactionIdGetLocalCommitTsData(TransactionId xid, TimestampTz *gts,  
							 RepOriginId *nodeid);


extern TransactionId GetLatestCommitTsData(TimestampTz *ts,
					  RepOriginId *nodeid);

extern Size CommitTsShmemBuffers(void);
extern Size CommitTsShmemSize(void);
extern void CommitTsShmemInit(void);
extern void BootStrapCommitTs(void);
extern void StartupCommitTs(void);
extern void TrimCommitTs(void);
extern void CommitTsParameterChange(bool xlrecvalue, bool pgcontrolvalue);
extern void CompleteCommitTsInitialization(void);
extern void ShutdownCommitTs(void);
extern void CheckPointCommitTs(void);
extern void ExtendCommitTs(TransactionId newestXact);
extern void TruncateCommitTs(TransactionId oldestXact);
extern void SetCommitTsLimit(TransactionId oldestXact,
				 TransactionId newestXact);
extern void AdvanceOldestCommitTsXid(TransactionId oldestXact);

/* XLOG stuff */
#define COMMIT_TS_ZEROPAGE		0x00
#define COMMIT_TS_TRUNCATE		0x10
#define COMMIT_TS_SETTS			0x20

typedef struct xl_commit_ts_set
{
	TimestampTz global_timestamp;
	TimestampTz timestamp;
	RepOriginId nodeid;
	TransactionId mainxid;
	/* subxact Xids follow */
} xl_commit_ts_set;

#define SizeOfCommitTsSet	(offsetof(xl_commit_ts_set, mainxid) + \
							 sizeof(TransactionId))

typedef struct xl_commit_ts_truncate
{
	int			pageno;
	TransactionId oldestXid;
} xl_commit_ts_truncate;

#define SizeOfCommitTsTruncate	(offsetof(xl_commit_ts_truncate, oldestXid) + \
								 sizeof(TransactionId))

extern void commit_ts_redo(XLogReaderState *record);
extern void commit_ts_desc(StringInfo buf, XLogReaderState *record);
extern const char *commit_ts_identify(uint8 info);

/* GUC parameter */
extern bool enable_committs_print;


#endif							/* COMMIT_TS_H */
