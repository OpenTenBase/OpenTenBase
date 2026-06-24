/*
 * subtrans.h
 *
 * PostgreSQL subtransaction-log manager
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/subtrans.h
 */
#ifndef SUBTRANS_H
#define SUBTRANS_H

/* Number of SLRU buffers to use for subtrans */
#define NUM_SUBTRANS_BUFFERS	32

/*
 * Defines for SubTrans page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * SubTrans page numbering also wraps around at
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE, and segment numbering at
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need take no
 * explicit notice of that fact in this module, except when comparing segment
 * and page numbers in TruncateSUBTRANS (see SubTransPagePrecedes) and zeroing
 * them in StartupSUBTRANS.
 */

/* We need four bytes per xact */
#define SUBTRANS_XACTS_PER_PAGE (BLCKSZ / sizeof(TransactionId))

#define TransactionIdToSubTransPage(xid) ((xid) / (TransactionId) SUBTRANS_XACTS_PER_PAGE)
#define TransactionIdToSubtransEntry(xid) ((xid) % (TransactionId) SUBTRANS_XACTS_PER_PAGE)

extern void SubTransSetParent(TransactionId xid, TransactionId parent);
extern TransactionId SubTransGetParent(TransactionId xid);
extern TransactionId SubTransGetTopmostTransaction(TransactionId xid);

extern Size SUBTRANSShmemSize(void);
extern void SUBTRANSShmemInit(void);
extern void BootStrapSUBTRANS(void);
extern void StartupSUBTRANS(TransactionId oldestActiveXID);
extern void ShutdownSUBTRANS(void);
extern void CheckPointSUBTRANS(void);
extern void ExtendSUBTRANS(TransactionId newestXact);
extern void TruncateSUBTRANS(TransactionId oldestXact);

#endif							/* SUBTRANS_H */
