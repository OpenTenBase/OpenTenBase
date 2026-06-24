/*
 * csnlog.h based on csnlog.h
 *
 * Commit-Timestamp log.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/csnlog.h
 */
#ifndef CSNLOG_H
#define CSNLOG_H

#include "access/xlog.h"

/*
 * Defines for CSNLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CSNLOG page numbering also wraps around at 0xFFFFFFFF/CSNLOG_XACTS_PER_PAGE,
 * and CSNLOG segment numbering at
 * 0xFFFFFFFF/CLOG_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateCSNLOG (see CSNLOGPagePrecedes).
 */

/* We store the commit LSN for each xid */
#define CSNLOG_XACTS_PER_PAGE (BLCKSZ / sizeof(CommitSeqNo))

#define TransactionIdToCSNPage(xid)	((xid) / (TransactionId) CSNLOG_XACTS_PER_PAGE)
#define TransactionIdToCSNPgIndex(xid) ((xid) % (TransactionId) CSNLOG_XACTS_PER_PAGE)

/*
 * add WAL for CSNLog to support distributed transactions
 * written by Junbin Kang
 */

/* We store the latest async LSN for each group of transactions */
#define CSNLOG_XACTS_PER_LSN_GROUP	32	/* keep this a power of 2 */
#define CSNLOG_LSNS_PER_PAGE	(CSNLOG_XACTS_PER_PAGE / CSNLOG_XACTS_PER_LSN_GROUP)

#define CSNGetLSNIndex(slotno, xid)	((slotno) * CSNLOG_LSNS_PER_PAGE + \
	((xid) % (TransactionId) CSNLOG_XACTS_PER_PAGE) / CSNLOG_XACTS_PER_LSN_GROUP)

/* We allocate new log pages in batches */
#define BATCH_SIZE 128

extern void CSNLogSetCSN(TransactionId xid, int nsubxids,
                         TransactionId *subxids, XLogRecPtr lsn, bool write_xlog, CommitSeqNo csn);
extern CommitSeqNo CSNLogAssignCSN(TransactionId xid, int nxids, TransactionId *xids, bool fromCoordinator);

extern CommitSeqNo CSNLogGetCSNAdjusted(TransactionId xid);
extern CommitSeqNo CSNLogGetCSNRaw(TransactionId xid);
extern TransactionId CSNLogGetNextActiveXid(TransactionId start,
					   TransactionId end);
extern XLogRecPtr CSNLogGetLSN(TransactionId xid);

extern Size CSNLOGShmemBuffers(void);
extern Size CSNLOGHashTbaleEntryNum(void);
extern Size CSNLOGShmemSize(void);
extern void CSNLOGShmemInit(void);
extern void BootStrapCSNLOG(void);
extern void StartupCSNLOG(TransactionId oldestActiveXID);
extern void RecoverCSNLOG(TransactionId oldestActiveXID);
extern void TrimCSNLOG(void);
extern void ShutdownCSNLOG(void);
extern void CheckPointCSNLOG(void);
extern void ExtendCSNLOG(TransactionId newestXact);
extern void TruncateCSNLOG(TransactionId oldestXact);

extern void CSNSubTransSetParent(TransactionId xid, TransactionId parent);
extern TransactionId CSNSubTransGetParent(TransactionId xid);
extern TransactionId CSNSubTransGetTopmostTransaction(TransactionId xid);

/* XLOG stuff */
#define CSNLOG_ZEROPAGE		0x00
#define CSNLOG_TRUNCATE		0x10
#define CSNLOG_SETCSN		0x20

typedef struct xl_csnlog_truncate
{
    int			pageno;
    TransactionId oldestXact;
}			xl_csnlog_truncate;

typedef struct xl_csn_set
{
	CommitSeqNo	csn;
	TransactionId mainxid;
	/* subxact Xids follow */
}			xl_csn_set;

#define SizeOfCSNSet	(offsetof(xl_csn_set, mainxid) + \
							 sizeof(TransactionId))


extern void csnlog_redo(XLogReaderState *record);
extern void csnlog_desc(StringInfo buf, XLogReaderState *record);
extern const char *csnlog_identify(uint8 info);
#ifdef _PG_REGRESS_	
bool TestCsnlogTruncateCompress(TransactionId xid);
bool TestCsnlogTruncateRename(TransactionId xid);
#endif

#endif							/* CSNLOG_H */
