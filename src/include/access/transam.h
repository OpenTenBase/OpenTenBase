/*-------------------------------------------------------------------------
 *
 * transam.h
 *	  postgres transaction access method support code
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/access/transam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRANSAM_H
#define TRANSAM_H

#include "access/csn.h"
#include "access/xlogdefs.h"
#ifdef PGXC
#include "gtm/gtm_c.h"
#endif

typedef int XidStatus;

/* ----------------
 *		Special transaction ID values
 *
 * BootstrapTransactionId is the XID for "bootstrap" operations, and
 * FrozenTransactionId is used for very old tuples.  Both should
 * always be considered valid.
 *
 * FirstNormalTransactionId is the first "normal" transaction id.
 * Note: if you need to change it, you must change pg_class.h as well.
 * ----------------
 */
#define InvalidTransactionId		((TransactionId) 0)
#define BootstrapTransactionId		((TransactionId) 1)
#define FrozenTransactionId			((TransactionId) 2)
#define FirstNormalTransactionId	((TransactionId) 3)
#define MaxTransactionId			((TransactionId) 0xFFFFFFFF)

/* ----------------
 *		transaction ID manipulation macros
 * ----------------
 */
#define TransactionIdIsValid(xid)		((xid) != InvalidTransactionId)
#define TransactionIdIsNormal(xid)		((xid) >= FirstNormalTransactionId)
#define TransactionIdEquals(id1, id2)	((id1) == (id2))
#define TransactionIdStore(xid, dest)	(*(dest) = (xid))
#define StoreInvalidTransactionId(dest) (*(dest) = InvalidTransactionId)

/* advance a transaction ID variable, handling wraparound correctly */
#define TransactionIdAdvance(dest)	\
	do { \
		(dest)++; \
		if ((dest) < FirstNormalTransactionId) \
			(dest) = FirstNormalTransactionId; \
	} while(0)

/* back up a transaction ID variable, handling wraparound correctly */
#define TransactionIdRetreat(dest)	\
	do { \
		(dest)--; \
	} while ((dest) < FirstNormalTransactionId)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdPrecedes(id1, id2) \
	(AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), \
	(int32) ((id1) - (id2)) < 0)

/* compare two XIDs already known to be normal; this is a macro for speed */
#define NormalTransactionIdFollows(id1, id2) \
	(AssertMacro(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2)), \
	(int32) ((id1) - (id2)) > 0)

/* ----------
 *		Object ID (OID) zero is InvalidOid.
 *
 *		OIDs 1-9999 are reserved for manual assignment (see the files
 *		in src/include/catalog/).
 *
 *		OIDS 10000-16383 are reserved for assignment during initdb
 *		using the OID generator.  (We start the generator at 10000.)
 *
 *		OIDs beginning at 32786 are assigned from the OID generator
 *		during normal multiuser operation.  (We force the generator up to
 *		32786 as soon as we are in normal operation.)
 *
 * The choices of 10000 and 32786 are completely arbitrary, and can be moved
 * if we run low on OIDs in either category.  Changing the macros below
 * should be sufficient to do this.
 *
 * NOTE: if the OID generator wraps around, we skip over OIDs 0-32785
 * and resume with 32786.  This minimizes the odds of OID conflict, by not
 * reassigning OIDs that might have been assigned during initdb.
 * ----------
 */
#define FirstBootstrapObjectId	10000
#define FirstNormalObjectId		16384

#define IsSystemObjOid(id) ((OidIsValid(id)) && (id < FirstBootstrapObjectId))

typedef enum
{
    XID_COMMITTED,
    XID_ABORTED,
    XID_INPROGRESS
} TransactionIdStatus;

/* ----------------
 *		extern declarations
 * ----------------
 */

/* in transam/xact.c */
extern bool TransactionStartedDuringRecovery(void);

/*
 * prototypes for functions in transam/transam.c
 */
extern bool TransactionIdGetCSNAndCommitStat(TransactionId xid, CommitSeqNo *gts, RepOriginId *nodeid);
extern CommitSeqNo TransactionIdGetCSN(TransactionId transactionId);
extern XidStatus TransactionIdGetStatusCSN(TransactionId transactionId);
extern bool TransactionIdDidCommit(TransactionId transactionId);
extern bool TransactionIdDidAbort(TransactionId transactionId);
extern bool TransactionIdIsKnownCompleted(TransactionId transactionId);
extern void TransactionIdAbort(TransactionId transactionId);
extern void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids, CommitSeqNo csn);
extern void TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids, XLogRecPtr lsn, CommitSeqNo csn);
extern void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids);
extern bool TransactionIdPrecedes(TransactionId id1, TransactionId id2);
extern bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2);
extern bool TransactionIdFollows(TransactionId id1, TransactionId id2);
extern bool TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2);
extern TransactionId TransactionIdLatest(TransactionId mainxid,
					int nxids, const TransactionId *xids);
extern XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);

/* in transam/varsup.c */
#ifdef PGXC  /* PGXC_DATANODE */
extern void SetNextTransactionId(TransactionId xid);
extern void SetForceXidFromGTM(bool value);
extern bool GetForceXidFromGTM(void);
#ifdef __USE_GLOBAL_SNAPSHOT__
extern TransactionId GetNewTransactionId(bool isSubXact, bool *timestamp_received, GTM_Timestamp *timestamp);
#else
extern TransactionId GetNewTransactionId(bool isSubXact);
extern void SetLocalTransactionId(TransactionId xid);
extern void StoreGlobalXid(const char *globalXid, bool isprimary);
extern void StoreLocalGlobalXid(const char *globalXid);
#endif

#ifdef __TWO_PHASE_TRANS__
extern void StoreStartNode(const char *startnode);
extern void StorePartNodes(const char *partNodes);
extern void StoreStartXid(TransactionId transactionid);
extern void StoreDatabase(const char *database);
extern void StoreUser(const char *user);
#endif

#else
extern TransactionId GetNewTransactionId(bool isSubXact);
#endif /* PGXC */
#ifdef XCP
extern bool TransactionIdIsCurrentGlobalTransactionId(TransactionId xid);
extern TransactionId GetNextTransactionId(void);
extern void ExtendLogs(TransactionId xid);
extern int GetNumSubTransactions(void);
extern TransactionId *GetSubTransactions(void);
#endif
extern TransactionId ReadNewTransactionId(void);
extern void SetTransactionIdLimit(TransactionId oldest_datfrozenxid,
					  Oid oldest_datoid);
extern void AdvanceOldestClogXid(TransactionId oldest_datfrozenxid);
extern bool ForceTransactionIdLimitUpdate(void);
extern Oid	GetNewObjectId(bool is_toast_rel);
extern bool is_distri_report;
/* GUC parameter */
extern bool enable_distri_print;
extern bool enable_distri_debug;
extern bool enable_distri_visibility_print;
extern int  delay_before_acquire_committs;
extern int  delay_after_acquire_committs;

#define DEBUG_VISIBILE(A) \
do { \
	if (unlikely(enable_distri_visibility_print)) \
	{ \
		A; \
	} \
} while (0)


#endif							/* TRAMSAM_H */
