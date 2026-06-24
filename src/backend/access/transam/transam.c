/*-------------------------------------------------------------------------
 *
 * transam.c
 *	  postgres transaction (commit) log interface routines
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/transam.c
 *
 * NOTES
 *	  This file contains the high level access-method interface to the
 *	  transaction system.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/csnlog.h"
#include "access/mvccvars.h"
#include "storage/lmgr.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "utils/snapmgr.h"

#ifdef PGXC
#include "storage/procarray.h"
#include "utils/builtins.h"
#endif

/*
 * Single-item cache for results of TransactionLogFetch.  It's worth having
 * such a cache because we frequently find ourselves repeatedly checking the
 * same XID, for example when scanning a table just after a bulk insert,
 * update, or delete.
 */
static TransactionId cachedFetchXid = InvalidTransactionId;
static CommitSeqNo cachedCSN;

/*
 * Also have a (separate) cache for CLogGetCommitLSN()
 */
static TransactionId cachedLSNFetchXid = InvalidTransactionId;
static XLogRecPtr cachedCommitLSN;

#ifdef PGXC
/* It is not really necessary to make it appear in header file */
Datum pgxc_is_committed(PG_FUNCTION_ARGS);
Datum pgxc_is_inprogress(PG_FUNCTION_ARGS);
#endif

bool
TransactionIdGetCSNAndCommitStat(TransactionId xid, CommitSeqNo *gts, RepOriginId *nodeid)
{
    *gts = CSNLogGetCSNAdjusted(xid);
	if (CSN_IS_COMMITTED(*gts))
	{
		return true;
	}
	else
	{
		return false;
	}
}

CommitSeqNo
TransactionIdGetCSN(TransactionId transactionId)
{
    CommitSeqNo csn;

	/*
	 * Before going to the commit log manager, check our single item cache to
	 * see if we didn't just check the transaction status a moment ago.
	 */
	if (TransactionIdEquals(transactionId, cachedFetchXid) &&
	    cachedFetchXid != InvalidTransactionId)
		return cachedCSN;

	/*
	 * Also, check to see if the transaction ID is a permanent one.
	 */
	if (!TransactionIdIsNormal(transactionId))
	{
		if (TransactionIdEquals(transactionId, BootstrapTransactionId))
			return CSN_FROZEN;
		if (TransactionIdEquals(transactionId, FrozenTransactionId))
			return CSN_FROZEN;
		return CSN_ABORTED;
	}

    csn = CSNLogGetCSNAdjusted(transactionId);

    if (csn == CSN_COMMITTING)
    {
        /*
         * If the transaction is committing at this very instant, and
         * hasn't set its CSN yet, wait for it to finish doing so.
         *
         * XXX: Alternatively, we could wait on the heavy-weight lock on
         * the XID. that'd make TransactionIdCommitTree() slightly
         * cheaper, as it wouldn't need to acquire CommitSeqNoLock (even
         * in shared mode).
         */
        
        if (enable_distri_print)
            elog(LOG, "wait for committing transaction xid %d to complete", transactionId);
        XactLockTableWait(transactionId, NULL, NULL, XLTW_None);

        csn = CSNLogGetCSNAdjusted(transactionId);
        Assert(csn != CSN_COMMITTING);
    }

    /*
     * Cache it, but DO NOT cache status for unfinished transactions! We only
     * cache status that is guaranteed not to change.
     */
    if (CSN_IS_COMMITTED(csn) ||
        CSN_IS_ABORTED(csn))
    {
        cachedFetchXid = transactionId;
        cachedCSN = csn;
    }
	return csn;
}

#ifdef PGXC
/*
 * For given Transaction ID, check if transaction is committed or aborted
 */
Datum
pgxc_is_committed(PG_FUNCTION_ARGS)
{
	TransactionId	tid = (TransactionId) PG_GETARG_UINT32(0);
    CommitSeqNo   csn;

    csn = TransactionIdGetCSN(tid);

	if (CSN_IS_COMMITTED(csn))
		PG_RETURN_BOOL(true);
	else if (CSN_IS_ABORTED(csn))
		PG_RETURN_BOOL(false);
	else
		PG_RETURN_NULL();
}

/*
 * For given Transaction ID, check if transaction is committed or aborted
 */
Datum
pgxc_is_inprogress(PG_FUNCTION_ARGS)
{
	TransactionId	tid = (TransactionId) PG_GETARG_UINT32(0);
    CommitSeqNo   csn;

    csn = TransactionIdGetCSN(tid);

    if (CSN_IS_COMMITTED(csn))
		PG_RETURN_BOOL(false);
    else if (CSN_IS_ABORTED(csn))
		PG_RETURN_BOOL(false);
	else
		PG_RETURN_BOOL(true);
}
#endif

/*
 * Returns the status of the tranaction.
 *
 * Note that this treats a a crashed transaction as still in-progress,
 * until it falls off the xmin horizon.
 */
XidStatus
TransactionIdGetStatusCSN(TransactionId transactionId)
{
    CommitSeqNo csn;
    TransactionIdStatus status;

    csn = TransactionIdGetCSN(transactionId);

    if (CSN_IS_COMMITTED(csn))
        status = XID_COMMITTED;
    else if (CSN_IS_ABORTED(csn))
        status = XID_ABORTED;
    else
        status = XID_INPROGRESS;

    return status;
}

/* ----------------------------------------------------------------
 *						Interface functions
 *
 *		TransactionIdDidCommit
 *		TransactionIdDidAbort
 *		========
 *		   these functions test the transaction status of
 *		   a specified transaction id.
 *
 *		TransactionIdCommitTree
 *		TransactionIdAsyncCommitTree
 *		TransactionIdAbortTree
 *		========
 *		   these functions set the transaction status of the specified
 *		   transaction tree.
 *
 * See also TransactionIdIsInProgress, which once was in this module
 * but now lives in procarray.c.
 * ----------------------------------------------------------------
 */

/*
 * TransactionIdDidCommit
 *		True iff transaction associated with the identifier did commit.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
bool							/* true if given transaction committed */
TransactionIdDidCommit(TransactionId transactionId)
{
    CommitSeqNo csn;

    csn = TransactionIdGetCSN(transactionId);

    if (CSN_IS_COMMITTED(csn))
        return true;
    else
        return false;
}

/*
 * TransactionIdDidAbort
 *		True iff transaction associated with the identifier did abort.
 *
 * Note:
 *		Assumes transaction identifier is valid and exists in clog.
 */
bool							/* true if given transaction aborted */
TransactionIdDidAbort(TransactionId transactionId)
{
    CommitSeqNo csn;

    csn = TransactionIdGetCSN(transactionId);

    if (CSN_IS_ABORTED(csn))
        return true;
    else
        return false;
}

/*
 * TransactionIdIsKnownCompleted
 *		True iff transaction associated with the identifier is currently
 *		known to have either committed or aborted.
 *
 * This does NOT look into pg_xact but merely probes our local cache
 * (and so it's not named TransactionIdDidComplete, which would be the
 * appropriate name for a function that worked that way).  The intended
 * use is just to short-circuit TransactionIdIsInProgress calls when doing
 * repeated tqual.c checks for the same XID.  If this isn't extremely fast
 * then it will be counterproductive.
 *
 * Note:
 *		Assumes transaction identifier is valid.
 */
bool
TransactionIdIsKnownCompleted(TransactionId transactionId)
{
	if (TransactionIdEquals(transactionId, cachedFetchXid))
	{
		/* If it's in the cache at all, it must be completed. */
		return true;
	}

	return false;
}

/*
 * TransactionIdCommitTree
 *		Marks the given transaction and children as committed
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * This commit operation is not guaranteed to be atomic, but if not, subxids
 * are correctly marked subcommit first.
 */
void
TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids, CommitSeqNo csn)
{
    TransactionIdAsyncCommitTree(xid, nxids, xids, InvalidXLogRecPtr, csn);
}

/*
 * TransactionIdAsyncCommitTree
 *		Same as above, but for async commits.  The commit record LSN is needed.
 */
void
TransactionIdAsyncCommitTree(TransactionId xid, int nxids, TransactionId *xids,
							 XLogRecPtr lsn, CommitSeqNo csn)
{
    {
        TransactionId latestXid;
        TransactionId currentLatestCompletedXid;

        latestXid = TransactionIdLatest(xid, nxids, xids);

        /*
         * Grab the CommitSeqNoLock, in shared mode. This is only used to provide
         * a way for a concurrent transaction to wait for us to complete (see
         * TransactionIdGetCSN()).
         *
         * XXX: We could reduce the time the lock is held, by only setting the CSN
         * on the top-XID while holding the lock, and updating the sub-XIDs later.
         * But it doesn't matter much, because we're only holding it in shared
         * mode, and it's rare for it to be acquired in exclusive mode.
         */

        /*
         * First update latestCompletedXid to cover this xid. We do this before
         * assigning a CSN, so that if someone acquires a new snapshot at the same
         * time, the xmax it computes is sure to cover our XID.
         */
        currentLatestCompletedXid = GetLatestCompletedXid();
        while (TransactionIdFollows(latestXid, currentLatestCompletedXid))
        {
            if (pg_atomic_compare_exchange_u32(&ShmemVariableCache->latestCompletedXid,
                                               &currentLatestCompletedXid,
                                               latestXid))
                break;
        }

        CSNLogSetCSN(xid, nxids, xids, lsn, false, csn);
    }
}

/*
 * TransactionIdAbortTree
 *		Marks the given transaction and children as aborted.
 *
 * "xid" is a toplevel transaction commit, and the xids array contains its
 * committed subtransactions.
 *
 * We don't need to worry about the non-atomic behavior, since any onlookers
 * will consider all the xacts as not-yet-committed anyway.
 */
void
TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids)
{
    TransactionId latestXid;
    TransactionId currentLatestCompletedXid;

    latestXid = TransactionIdLatest(xid, nxids, xids);

    currentLatestCompletedXid = GetLatestCompletedXid();
    while (TransactionIdFollows(latestXid, currentLatestCompletedXid))
    {
        if (pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&ShmemVariableCache->latestCompletedXid,
                                           &currentLatestCompletedXid,
                                           latestXid))
            break;
    }
    if (enable_distri_print)
        elog(LOG, "abort transaction xid %d", xid);

    CSNLogSetCSN(xid, nxids, xids, InvalidXLogRecPtr, false, CSN_ABORTED);
}

/*
 * TransactionIdPrecedes --- is id1 logically < id2?
 */
bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	/*
	 * If either ID is a permanent XID then we can just do unsigned
	 * comparison.  If both are normal, do a modulo-2^32 comparison.
	 */
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 < id2);

	diff = (int32) (id1 - id2);
	return (diff < 0);
}

/*
 * TransactionIdPrecedesOrEquals --- is id1 logically <= id2?
 */
bool
TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 <= id2);

	diff = (int32) (id1 - id2);
	return (diff <= 0);
}

/*
 * TransactionIdFollows --- is id1 logically > id2?
 */
bool
TransactionIdFollows(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 > id2);

	diff = (int32) (id1 - id2);
	return (diff > 0);
}

/*
 * TransactionIdFollowsOrEquals --- is id1 logically >= id2?
 */
bool
TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2)
{
	int32		diff;

	if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
		return (id1 >= id2);

	diff = (int32) (id1 - id2);
	return (diff >= 0);
}


/*
 * TransactionIdLatest --- get latest XID among a main xact and its children
 */
TransactionId
TransactionIdLatest(TransactionId mainxid,
					int nxids, const TransactionId *xids)
{
	TransactionId result;

	/*
	 * In practice it is highly likely that the xids[] array is sorted, and so
	 * we could save some cycles by just taking the last child XID, but this
	 * probably isn't so performance-critical that it's worth depending on
	 * that assumption.  But just to show we're not totally stupid, scan the
	 * array back-to-front to avoid useless assignments.
	 */
	result = mainxid;
	while (--nxids >= 0)
	{
		if (TransactionIdPrecedes(result, xids[nxids]))
			result = xids[nxids];
	}
	return result;
}


/*
 * TransactionIdGetCommitLSN
 *
 * This function returns an LSN that is late enough to be able
 * to guarantee that if we flush up to the LSN returned then we
 * will have flushed the transaction's commit record to disk.
 *
 * The result is not necessarily the exact LSN of the transaction's
 * commit record!  For example, for long-past transactions (those whose
 * clog pages already migrated to disk), we'll return InvalidXLogRecPtr.
 * Also, because we group transactions on the same clog page to conserve
 * storage, we might return the LSN of a later transaction that falls into
 * the same group.
 */
XLogRecPtr
TransactionIdGetCommitLSN(TransactionId xid)
{
	XLogRecPtr	result;

	/*
	 * Currently, all uses of this function are for xids that were just
	 * reported to be committed by TransactionLogFetch, so we expect that
	 * checking TransactionLogFetch's cache will usually succeed and avoid an
	 * extra trip to shared memory.
	 */
	if (TransactionIdEquals(xid, cachedLSNFetchXid))
		return cachedCommitLSN;

	/* Special XIDs are always known committed */
	if (!TransactionIdIsNormal(xid))
		return InvalidXLogRecPtr;

	/*
	 * Get the transaction status.
	 */

    result = CSNLogGetLSN(xid);

    cachedLSNFetchXid = xid;
    cachedCommitLSN = result;

	return result;
}
