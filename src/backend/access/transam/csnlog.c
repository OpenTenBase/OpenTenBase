/*-------------------------------------------------------------------------
 *
 * csnlog.c based on csnlog.c
 *		Tracking Commit-Timestamps and in-progress subtransactions
 *
 * The pg_csnlog manager is a pg_clog-like manager that stores the commit
 * sequence number, or parent transaction Id, for each transaction.  It is
 * a fundamental part of distributed MVCC.
 *
 * To support distributed transaction, XLOG is added for csnlog to preserve
 * data across crashes. During database startup, we would apply xlog records
 * to csnlog.
 * Author: Junbin Kang, 2020-01-11
 *
 * Implement the scalable CSN (Commit Sequence) that adopts multi-partition LRU
 * and use lock-free algorithms as far as possible, i.e., Get/Set commit timestamp
 * in LRU cached pages with only shared-lock being held.
 * Author: Junbin Kang, 2020-06-19
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/csnlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csnlog.h"
#include "access/gtm.h"
#include "access/mvccvars.h"
#include "access/lru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "utils/snapmgr.h"
#include "funcapi.h"
#include "utils/timestamp.h"

/*
 * Link to shared-memory data structures for CLOG control
 */
static LruCtlData CSNLogCtlData;

#define CSNLogCtl (&CSNLogCtlData)

/* local xlog stuff */
static void WriteZeroPageXlogRec(int pageno);
static void WriteTruncateXlogRec(int pageno, TransactionId oldestXact);
static XLogRecPtr WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, CommitSeqNo csn);

static int	ZeroCSNLOGPage(int pageno, int partitionno, bool writeXlog);
static bool CSNLOGPagePrecedes(int page1, int page2);
static void CSNLogSetPageStatus(TransactionId xid, int nsubxids,
					TransactionId *subxids,
					CommitSeqNo csn, XLogRecPtr lsn, int pageno);
static void CSNLogPageSetCSN(TransactionId xid, int partitionno, CommitSeqNo csn, XLogRecPtr lsn, int slotno);
static CommitSeqNo InternalGetCSN(TransactionId xid, int partitionno);
static CommitSeqNo RecursiveGetCSNLocked(TransactionId xid, int partitionno);
static CommitSeqNo RecursiveGetXidCSN(TransactionId xid);
/*
 * CSNLogSetCommitSeqNo
 *
 * Record the status and CSN of transaction entries in the commit log for a
 * transaction and its subtransaction tree. Take care to ensure this is
 * efficient, and as atomic as possible.
 *
 * xid is a single xid to set status for. This will typically be the
 * top level transactionid for a top level commit or abort. It can
 * also be a subtransaction when we record transaction aborts.
 *
 * subxids is an array of xids of length nsubxids, representing subtransactions
 * in the tree of xid. In various cases nsubxids may be zero.
 *
 * csn is the commit sequence number of the transaction. It should be
 * InvalidCommitSeqNo for abort cases.
 *
 * Note: This doesn't guarantee atomicity. The caller can use the
 * CSN_COMMITTING special value for that.
 */
void
CSNLogSetCSN(TransactionId xid, int nsubxids,
             TransactionId *subxids, XLogRecPtr lsn, bool write_xlog, CommitSeqNo csn)
{
	int			nextSubxid;
	int			topPage;
	TransactionId topXid;
	XLogRecPtr	max_lsn = lsn;

	if (csn == InvalidCommitSeqNo || xid == BootstrapTransactionId)
	{
		if (IsBootstrapProcessingMode())
			csn = CSN_FROZEN;
		else
			elog(ERROR, "cannot mark transaction committed without CSN");
	}
    
	/*
	 * Comply with the WAL-before-data rule: if caller specified it wants this
	 * value to be recorded in WAL, do so before touching the data. write xlog
	 * is only needed when the modification is not recorded in xlog before
	 * calling this function.
	 */
	if (write_xlog)
	{
		max_lsn = WriteSetTimestampXlogRec(xid, nsubxids, subxids, csn);
		if (lsn > max_lsn)
		{
			max_lsn = lsn;
		}
	}

	/*
	 * We set the status of child transaction before the status of parent
	 * transactions, so that another process can correctly determine the
	 * resulting status of a child transaction. See RecursiveGetCommitSeqNo().
	 */
	topXid = InvalidTransactionId;
	topPage = TransactionIdToCSNPage(xid);
	nextSubxid = nsubxids - 1;
	do
	{
		int			currentPage = topPage;
		int			subxidsOnPage = 0;

		for (; nextSubxid >= 0; nextSubxid--)
		{
			int			subxidPage = TransactionIdToCSNPage(subxids[nextSubxid]);

			if (subxidsOnPage == 0)
				currentPage = subxidPage;

			if (currentPage != subxidPage)
				break;

			subxidsOnPage++;
		}

		if (currentPage == topPage)
		{
			Assert(topXid == InvalidTransactionId);
			topXid = xid;
		}

		CSNLogSetPageStatus(topXid, subxidsOnPage, subxids + nextSubxid + 1,
							csn, max_lsn, currentPage);
	}
	while (nextSubxid >= 0);

	if (topXid == InvalidTransactionId)
	{
		/*
		 * No subxids were on the same page as the main xid; we have to update
		 * it separately
		 */

		CSNLogSetPageStatus(xid, 0, NULL, csn, max_lsn, topPage);
	}
}

CommitSeqNo
CSNLogAssignCSN(TransactionId xid, int nxids, TransactionId *xids, bool fromCoordinator)
{
	CommitSeqNo	csn;
	TransactionId latestXid;
	TransactionId currentLatestCompletedXid;

	latestXid = TransactionIdLatest(xid, nxids, xids);

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
    
    csn = GetGlobalTimestampGTMForCommit();
	Assert(!CSN_IS_SUBTRANS(csn));

	if (!CSN_IS_NORMAL(csn))
		elog(ERROR, "invalid commit ts " UINT64_FORMAT, csn);

	Assert(csn >= CSN_FIRST_NORMAL);
	if (enable_distri_print)
		elog(LOG, "xid %d assign commit timestamp " UINT64_FORMAT, xid, csn);
	return csn;
}
/*
 * Record the final state of transaction entries in the csn log for
 * all entries on a single page.  Atomic only on this page.
 *
 * Otherwise API is same as TransactionIdSetTreeStatus()
 */
static void
CSNLogSetPageStatus(TransactionId xid, int nsubxids,
					TransactionId *subxids,
					CommitSeqNo csn, XLogRecPtr lsn, int pageno)
{
	int			slotno;
	int			i;
	int			partitionno;
	LWLock	   *partitionLock;	/* buffer partition lock for it */

	if (enable_distri_print)
		elog(LOG, "CSNLogSetPageStatus xid %d csn %lu lsn" UINT64_FORMAT UINT64_FORMAT, xid, csn, lsn, XactLastRecEnd);

	partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
	LWLockAcquire(partitionLock, LW_SHARED);

	/* Try to find the page while holding only shared lock */

	slotno = LruReadPage_ReadOnly_Locked(CSNLogCtl, partitionno, pageno, XLogRecPtrIsInvalid(lsn), xid);

	/*
	 * We set the status of child transaction before the status of parent
	 * transactions, so that another process can correctly determine the
	 * resulting status of a child transaction. See RecursiveGetCommitSeqNo().
	 */
	for (i = nsubxids - 1; i >= 0; i--)
	{
		Assert(CSNLogCtl->shared[partitionno]->page_number[slotno] == TransactionIdToCSNPage(subxids[i]));
        CSNLogPageSetCSN(subxids[i], partitionno, csn, lsn, slotno);
		pg_write_barrier();
	}

	if (TransactionIdIsValid(xid))
        CSNLogPageSetCSN(xid, partitionno, csn, lsn, slotno);

	CSNLogCtl->shared[partitionno]->page_dirty[slotno] = true;

	LWLockRelease(partitionLock);

}



/*
 * Record the parent of a subtransaction in the subtrans log.
 *
 * In some cases we may need to overwrite an existing value.
 */
void
CSNSubTransSetParent(TransactionId xid, TransactionId parent)
{
	int			pageno = TransactionIdToCSNPage(xid);
	int			entryno = TransactionIdToCSNPgIndex(xid);
	int			slotno;
	CommitSeqNo   *ptr;
	CommitSeqNo	newcsn;
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);


	Assert(TransactionIdIsValid(parent));
	Assert(TransactionIdFollows(xid, parent));

    if (enable_distri_print)
        elog(LOG, "CSNSubTransSetParent xid %u parent %u", xid, parent);
    
    newcsn = MASK_SUBTRANS_BIT((uint64) parent);

	/*
	 * Shared page access is enough to set the subtransaction parent. It is
	 * set when the subtransaction is assigned an xid, and can be read only
	 * later, after the subtransaction have modified some tuples.
	 */
	slotno = LruReadPage_ReadOnly(CSNLogCtl, partitionno, pageno, xid);
	ptr = (CommitSeqNo *) CSNLogCtl->shared[partitionno]->page_buffer[slotno];
	ptr += entryno;

	/*
	 * It's possible we'll try to set the parent xid multiple times but we
	 * shouldn't ever be changing the xid from one valid xid to another valid
	 * xid, which would corrupt the data structure.
	 */
	if (*ptr != newcsn)
	{
		Assert(RecoveryInProgress() || *ptr == CSN_INPROGRESS);
		*ptr = newcsn;
		CSNLogCtl->shared[partitionno]->page_dirty[slotno] = true;
    }

	LWLockRelease(partitionLock);
}

/*
 * Interrogate the parent of a transaction in the csnlog.
 */
TransactionId
CSNSubTransGetParent(TransactionId xid)
{
	CommitSeqNo	csn;
	int			pageno = TransactionIdToCSNPage(xid);
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);

	LWLockAcquire(partitionLock, LW_SHARED);

	csn = InternalGetCSN(xid, partitionno);

	LWLockRelease(partitionLock);

	if (CSN_IS_SUBTRANS(csn))
		return CSN_SUBTRANS_PARENT_XID(csn);
	else
		return InvalidTransactionId;
}

/*
 * CSNSubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 */
TransactionId
CSNSubTransGetTopmostTransaction(TransactionId xid)
{
	TransactionId parentXid = xid,
				previousXid = xid;

	/* Can't ask about stuff that might not be around anymore */
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	while (TransactionIdIsValid(parentXid))
	{
		previousXid = parentXid;
		if (TransactionIdPrecedes(parentXid, TransactionXmin))
			break;
		parentXid = CSNSubTransGetParent(parentXid);

		/*
		 * By convention the parent xid gets allocated first, so should always
		 * precede the child xid. Anything else points to a corrupted data
		 * structure that could lead to an infinite loop, so exit.
		 */
		if (!TransactionIdPrecedes(parentXid, previousXid))
			elog(ERROR, "pg_csnlog contains invalid entry: xid %u points to parent xid %u",
				 previousXid, parentXid);
	}

	Assert(TransactionIdIsValid(previousXid));

	return previousXid;
}


/*
 * Sets the commit status of a single transaction.
 *
 * Must be called with CSNLogControlLock held
 */
static void
CSNLogPageSetCSN(TransactionId xid, int partitionno, CommitSeqNo csn, XLogRecPtr lsn, int slotno)
{
	int			entryno = TransactionIdToCSNPgIndex(xid);
	CommitSeqNo   *ptr;
    
	/*
	 * Update the group LSN if the transaction completion LSN is higher.
	 *
	 * Note: lsn will be invalid when supplied during InRecovery processing,
	 * so we don't need to do anything special to avoid LSN updates during
	 * recovery. After recovery completes the next csnlog change will set the
	 * LSN correctly.
	 *
	 * We must first set LSN before CSN is written so that we can guarantee
	 * that CSNLogGetLSN() always reflects the fresh LSN corresponding to the
	 * asynchronous commit xid.
	 *
	 * In other words, if we see a async committed xid, the corresponding LSN
	 * has already been updated.
	 *
	 * The consideration is due to lack of locking protection when setting and
	 * fetching LSN.
	 *
	 * Written by Junbin Kang, 2020-09-03
	 *
	 */

	if (!XLogRecPtrIsInvalid(lsn))
	{
		int			lsnindex = CSNGetLSNIndex(slotno, xid);

		if (CSNLogCtl->shared[partitionno]->group_lsn[lsnindex] < lsn)
			CSNLogCtl->shared[partitionno]->group_lsn[lsnindex] = lsn;
        if (enable_distri_print)
            elog(LOG, "CSNLogSetPageStatus xid %d csn %lu lsn" UINT64_FORMAT UINT64_FORMAT, xid, csn, lsn, XactLastRecEnd);

    }

    ptr = (CommitSeqNo *) (CSNLogCtl->shared[partitionno]->page_buffer[slotno] + entryno * sizeof(CommitSeqNo));

	/*
	 * Current state change should be from 0 to target state. (Allow setting
	 * it again to same value.)
	 */
	Assert(CSN_IS_INPROGRESS(*ptr) ||
		   CSN_IS_SUBTRANS(*ptr) ||
		   CSN_IS_PREPARED(*ptr) ||
		   *ptr == csn);

	*ptr = csn;
    if (enable_distri_print)
        elog(LOG, "CSNLogSetPageStatus xid %d csn %lu lsn" UINT64_FORMAT, xid, csn, XactLastRecEnd);


}

CommitSeqNo
CSNLogGetCSNRaw(TransactionId xid)
{
	int			  pageno = TransactionIdToCSNPage(xid);
	int			  partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	     *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
	CommitSeqNo	  csn;

	LWLockAcquire(partitionLock, LW_SHARED);

	csn = InternalGetCSN(xid, partitionno);

	LWLockRelease(partitionLock);

	return csn;
}

CommitSeqNo
RecursiveGetXidCSN(TransactionId xid)
{
	int pageno = TransactionIdToCSNPage(xid);
	int partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
	CommitSeqNo csn;

	LWLockAcquire(partitionLock, LW_SHARED);

	csn = RecursiveGetCSNLocked(xid, partitionno);

	LWLockRelease(partitionLock);
	
	return csn;
}
/*
 * Interrogate the state of a transaction in the commit log.
 *
 * Aside from the actual commit status, this function returns (into *lsn)
 * an LSN that is late enough to be able to guarantee that if we flush up to
 * that LSN then we will have flushed the transaction's commit record to disk.
 * The result is not necessarily the exact LSN of the transaction's commit
 * record!	For example, for long-past transactions (those whose clog pages
 * already migrated to disk), we'll return InvalidXLogRecPtr.  Also, because
 * we group transactions on the same clog page to conserve storage, we might
 * return the LSN of a later transaction that falls into the same group.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionIdGetCSN() in transam.c is the intended caller.
 */
CommitSeqNo
CSNLogGetCSNAdjusted(TransactionId xid)
{
	CommitSeqNo	  csn;
	TransactionId oldestXid = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);

	csn = RecursiveGetXidCSN(xid);

	/*
	 * As the csn status of crashed transactions may not be set, it would be
	 * regarding as in-progress by mistaken. Note that
	 * TransactionIdIsInProgress() is removed by CSN to avoid proc array
	 * walking. As a result, we need to perform further checking: If the xid
	 * is below TransactionXmin and does not have csn, it should be crashed or
	 * aborted transaction.
	 * ShmemVariableCache->oldestActiveXid is 0 when we restore TwoPhaseData
	 */

	if (TransactionIdPrecedes(xid, oldestXid))
	{
		if (!CSN_IS_COMMITTED(csn) && !CSN_IS_ABORTED(csn))
		{
			csn = CSN_ABORTED;
			elog(LOG, "find xid not committed before oldestActiveXid xid:oldestActiveXid %u:%u %lu", xid, oldestXid, csn);
		}
	}

	return csn;
}

/* Determine the CSN of a transaction, walking the subtransaction tree if needed */
static CommitSeqNo
RecursiveGetCSNLocked(TransactionId xid, int partitionno)
{
	CommitSeqNo	csn;

	csn = InternalGetCSN(xid, partitionno);

	if (CSN_IS_SUBTRANS(csn))
	{
		CommitSeqNo    parentCsn;
		CommitSeqNo    curCsn = csn;
		int		       parentPageno;
		int		   	   curPartitionno;
		LWLock	      *curPartitionLock;
		int		   	   parentPartitionno;
		LWLock	      *parentPartitionLock;
		TransactionId  parentXid;

		curPartitionno = partitionno;
		curPartitionLock = GetPartitionLock(CSNLogCtl, curPartitionno);
		
		while(CSN_IS_SUBTRANS(curCsn))
		{
			parentXid = CSN_SUBTRANS_PARENT_XID(curCsn);
			parentPageno = TransactionIdToCSNPage(parentXid);
			parentPartitionno = PagenoMappingPartitionno(CSNLogCtl, parentPageno);
			parentPartitionLock = GetPartitionLock(CSNLogCtl, parentPartitionno);
			
			if (parentPartitionno != curPartitionno)
			{
				LWLockRelease(curPartitionLock);
				LWLockAcquire(parentPartitionLock, LW_SHARED);
				curPartitionno = parentPartitionno;
				curPartitionLock = parentPartitionLock;
			}

			parentCsn = InternalGetCSN(parentXid, parentPartitionno);
			curCsn = parentCsn;
		}

		if (parentPartitionno != partitionno)
		{
			LWLock	      *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
			
			LWLockRelease(parentPartitionLock);
			LWLockAcquire(partitionLock, LW_SHARED);
		}

		Assert(!CSN_IS_SUBTRANS(parentCsn));

		/*
		 * The parent and child transaction status update is not atomic. We
		 * must take care not to use the updated parent status with the old
		 * child status, or else we can wrongly see a committed subtransaction
		 * as aborted. This happens when the parent is already marked as
		 * committed and the child is not yet marked.
		 */
		pg_read_barrier();

		csn = InternalGetCSN(xid, partitionno);

		if (CSN_IS_SUBTRANS(csn))
		{
			if (CSN_IS_PREPARED(parentCsn) || 
                CSN_IS_INPROGRESS(parentCsn) ||
				DoingStartupXLOG)
				csn = parentCsn;
			else
				elog(ERROR, "xid %u has unexpected parent csn "UINT64_FORMAT" committed %d aborted %d parent_xid %u",
                     xid, parentCsn, CSN_IS_COMMITTED(parentCsn), CSN_IS_ABORTED(parentCsn), parentXid);
		}
	}

	return csn;
}

/*
 * Get the raw CSN value.
 */
static CommitSeqNo
InternalGetCSN(TransactionId xid, int partitionno)
{
	int			pageno = TransactionIdToCSNPage(xid);
	int			entryno = TransactionIdToCSNPgIndex(xid);
	int			slotno;

	/* Can't ask about stuff that might not be around anymore */
	/* Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin)); */

	if (!TransactionIdIsNormal(xid))
	{
		if (xid == InvalidTransactionId)
			return CSN_ABORTED;
		if (xid == FrozenTransactionId || xid == BootstrapTransactionId)
			return CSN_FROZEN;
	}

	slotno = LruReadPage_ReadOnly_Locked(CSNLogCtl, partitionno, pageno, true, xid);
	return *(CommitSeqNo *) (CSNLogCtl->shared[partitionno]->page_buffer[slotno]
						  + entryno * sizeof(XLogRecPtr));
}

XLogRecPtr
CSNLogGetLSN(TransactionId xid)
{
	int			pageno = TransactionIdToCSNPage(xid);
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
	int			lsnindex;
	int			slotno;
	XLogRecPtr	lsn;

	LWLockAcquire(partitionLock, LW_SHARED);

	slotno = LruLookupSlotno_Locked(CSNLogCtl, partitionno, pageno);

	/*
	 * We do not need to really read the page. If the page is not buffered, it
	 * indicates it is written out to disk. Under such situation, the xlog
	 * records of the async transactions have been durable.
	 */
	if (slotno == -1)
	{
		LWLockRelease(partitionLock);
		return InvalidXLogRecPtr;
	}

	lsnindex = CSNGetLSNIndex(slotno, xid);

	/*
	 * We asume 8-byte atomic CPU to guarantee correctness with no locking.
	 */
	lsn = CSNLogCtl->shared[partitionno]->group_lsn[lsnindex];

	LWLockRelease(partitionLock);

	return lsn;
}

/*
 * Find the next xid that is in progress.
 * We do not care about the subtransactions, they are accounted for
 * by their respective top-level transactions.
 */
TransactionId
CSNLogGetNextActiveXid(TransactionId xid,
					   TransactionId end)
{
	int			saved_partitionno = -1;
	LWLock	   *partitionLock = NULL;

	Assert(TransactionIdIsValid(TransactionXmin));


	for (;;)
	{
		int			pageno;
		int			partitionno;
		int			slotno;
		int			entryno;

		if (!TransactionIdPrecedes(xid, end))
			goto end;

		pageno = TransactionIdToCSNPage(xid);
		partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);

		if (partitionno != saved_partitionno)
		{
			if (saved_partitionno >= 0)
				LWLockRelease(partitionLock);

			partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
			saved_partitionno = partitionno;
			LWLockAcquire(partitionLock, LW_SHARED);
		}

		slotno = LruReadPage_ReadOnly_Locked(CSNLogCtl, partitionno, pageno, true, xid);

		for (entryno = TransactionIdToCSNPgIndex(xid); entryno < CSNLOG_XACTS_PER_PAGE;
			 entryno++)
		{
			CommitSeqNo	csn;

			if (!TransactionIdPrecedes(xid, end))
				goto end;

			csn = *(XLogRecPtr *) (CSNLogCtl->shared[partitionno]->page_buffer[slotno] + entryno * sizeof(XLogRecPtr));

			if (CSN_IS_INPROGRESS(csn)
				|| CSN_IS_PREPARED(csn))			
			{
				goto end;
			}

			TransactionIdAdvance(xid);
		}


	}

end:
	if (saved_partitionno >= 0)
		LWLockRelease(partitionLock);

	return xid;
}

/*
 * Number of shared CSNLOG buffers.
 */
Size
CSNLOGShmemBuffers(void)
{
	return Min(32, Max(BATCH_SIZE, NBuffers / 512));
}

Size
CSNLOGHashTbaleEntryNum(void)
{
    return NUM_PARTITIONS * CSNLOGShmemBuffers() + NUM_PARTITIONS;
}
/*
 * Shared memory sizing for CSNLOG
 */
Size
CSNLOGShmemSize(void)
{
    int 	max_entry_no = CSNLOGHashTbaleEntryNum();

    return NUM_PARTITIONS * CACHELINEALIGN(LruShmemSize(CSNLOGShmemBuffers(), CSNLOG_LSNS_PER_PAGE))
           + CACHELINEALIGN(sizeof(GlobalLruSharedData)) + LruBufTableShmemSize(max_entry_no);
}

/*
 * Initialization of shared memory for CSNLOG
 */
void
CSNLOGShmemInit(void)
{
    int 		max_entry_no = CSNLOGHashTbaleEntryNum();

	CSNLogCtl->PagePrecedes = CSNLOGPagePrecedes;

	LruInit(CSNLogCtl, "CSNLOG Ctl", CSNLOGShmemBuffers(), CSNLOG_LSNS_PER_PAGE, max_entry_no,
			CSNLogControlLock, "pg_csnlog",
			LWTRANCHE_CSNLOG_BUFFERS);
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CSNLOG segment.  (The pg_csnlog directory is assumed to
 * have been created by initdb, and CSNLOGShmemInit must have been
 * called already.)
 */
void
BootStrapCSNLOG(void)
{
	int			slotno;
	int			pageno = 0;
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);

	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCSNLOGPage(0, partitionno, false);

	/* Make sure it's written out */
	LruWritePage(CSNLogCtl, partitionno, slotno);
	Assert(!CSNLogCtl->shared[partitionno]->page_dirty[slotno]);

	LWLockRelease(partitionLock);
}


/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCSNLOGPage(int pageno, int partitionno, bool writeXlog)
{
	int			slotno;

	slotno = LruZeroPage(CSNLogCtl, partitionno, pageno);

	if (writeXlog && !RecoveryInProgress())
		WriteZeroPageXlogRec(pageno);

	return slotno;
}

void
RecoverCSNLOG(TransactionId oldestActiveXID)
{
	elog(LOG, "start recover CSN log oldestActivXid %u nextXid %u", oldestActiveXID,
		 ShmemVariableCache->nextXid);

	if (TransactionIdIsNormal(oldestActiveXID))
	{
		int				start_xid = ShmemVariableCache->nextXid;
		int				end_xid = oldestActiveXID;
		CommitSeqNo		csn;
		int 			n = 0;
		StringInfoData	debug_str;
		
		initStringInfo(&debug_str);
		
		/*
		 * For each xid within [oldestActiveXid, nextXid), in-progress status
		 * in CSNLog indicates it must be crash aborted.
		 */
		TransactionIdRetreat(start_xid);
		while (TransactionIdFollowsOrEquals(start_xid, end_xid))
		{
			csn = RecursiveGetXidCSN(start_xid);
			if (CSN_IS_INPROGRESS(csn))
			{
                CSNLogSetCSN(start_xid, 0, NULL, InvalidXLogRecPtr, false, CSN_ABORTED);
				
				appendStringInfo(&debug_str, "%u ", start_xid);
				++n;
				if (n == 100)
				{
					elog(LOG, "recover crash aborted %d xids: %s", n, debug_str.data);
					n = 0;
					resetStringInfo(&debug_str);
				}
				
			}
			TransactionIdRetreat(start_xid);
		}
		
		if (n > 0)
		{
			elog(LOG, "recover crash aborted %d xids: %s", n, debug_str.data);
		}
	}

}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
void
StartupCSNLOG(TransactionId oldestActiveXID)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToCSNPage(xid);
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionlock;

	/*
	 * Initialize our idea of the latest page number.
	 */
	LWLockAcquire(CSNLogControlLock, LW_EXCLUSIVE);
	CSNLogCtl->global_shared->latest_page_number = pageno;
	LWLockRelease(CSNLogControlLock);

	partitionlock = GetPartitionLock(CSNLogCtl, partitionno);
	LWLockAcquire(partitionlock, LW_EXCLUSIVE);
	CSNLogCtl->shared[partitionno]->latest_page_number = pageno;
	LWLockRelease(partitionlock);

	elog(LOG, "Startup multi-core scaling CSN");
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCSNLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely as a debugging aid.
	 */
	LruFlush(CSNLogCtl, false);

	/*
	 * fsync pg_csnlog to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_csnlog", true);
}

/*
 * This must be called ONCE at the end of startup/recovery.
 */
void
TrimCSNLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToCSNPage(xid);
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionlock;

	LWLockAcquire(CSNLogCtl->global_shared->ControlLock, LW_EXCLUSIVE);
	CSNLogCtl->global_shared->latest_page_number = pageno;
	LWLockRelease(CSNLogCtl->global_shared->ControlLock);

	/*
	 * Re-Initialize our idea of the latest page number.
	 */
	partitionlock = GetPartitionLock(CSNLogCtl, partitionno);
	LWLockAcquire(partitionlock, LW_EXCLUSIVE);
	CSNLogCtl->shared[partitionno]->latest_page_number = pageno;

	/*
	 * Zero out the remainder of the current clog page.  Under normal
	 * circumstances it should be zeroes already, but it seems at least
	 * theoretically possible that XLOG replay will have settled on a nextXID
	 * value that is less than the last XID actually used and marked by the
	 * previous database lifecycle (since subtransaction commit writes clog
	 * but makes no WAL entry).  Let's just be safe. (We need not worry about
	 * pages beyond the current one, since those will be zeroed when first
	 * used.  For the same reason, there is no need to do anything when
	 * nextXid is exactly at a page boundary; and it's likely that the
	 * "current" page doesn't exist yet in that case.)
	 */
	if (TransactionIdToCSNPgIndex(xid) != 0)
	{
		int			entryno = TransactionIdToCSNPgIndex(xid);
		int			byteno = entryno * sizeof(XLogRecPtr);
		int			slotno;
		char	   *byteptr;


		slotno = LruReadPage(CSNLogCtl, partitionno, pageno, false, xid);

		byteptr = CSNLogCtl->shared[partitionno]->page_buffer[slotno] + byteno;
        
		/* Zero the rest of the page */
		MemSet(byteptr, 0, BLCKSZ - byteno);

        elog(LOG, "Trim csnlog start from %d size %d next xid %u", byteno, BLCKSZ - byteno, xid);

		CSNLogCtl->shared[partitionno]->page_dirty[slotno] = true;
	}

	LWLockRelease(partitionlock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCSNLOG(void)
{
	/*
	 * Flush dirty CLOG pages to disk
	 *
	 * This is not actually necessary from a correctness point of view. We do
	 * it merely to improve the odds that writing of dirty pages is done by
	 * the checkpoint process and not by backends.
	 */
	LruFlush(CSNLogCtl, true);

	/*
	 * fsync pg_csnlog to ensure that any files flushed previously are durably
	 * on disk.
	 */
	fsync_fname("pg_csnlog", true);
}


/*
 * Make sure that CSNLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCSNLOG(TransactionId newestXact)
{

	int			pageno;
	int			partitionno;
#ifdef ENABLE_BATCH
	int			pre_partitionno = -1;
	int			i;
#endif
	LWLock	   *partitionlock;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToCSNPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToCSNPage(newestXact);
#ifdef ENABLE_BATCH
	if (pageno % BATCH_SIZE)
		return;

	/*
	 * Acquire global lock to protect global latest page number that would be
	 * modified in ZeroCSNLOGPage(). We keep the locking order of global lock
	 * -> partition lock.
	 */

	LWLockAcquire(CSNLogCtl->global_shared->ControlLock, LW_EXCLUSIVE);
	for (i = pageno; i < pageno + BATCH_SIZE; i++)
	{
		partitionno = PagenoMappingPartitionno(CSNLogCtl, i);
		if (partitionno != pre_partitionno)
		{
			if (pre_partitionno >= 0)
				LWLockRelease(partitionlock);
			partitionlock = GetPartitionLock(CSNLogCtl, partitionno);
			pre_partitionno = partitionno;
			LWLockAcquire(partitionlock, LW_EXCLUSIVE);
		}

		/* Zero the page and make an XLOG entry about it */
		ZeroCSNLOGPage(i, partitionno, true);
	}

	if (pre_partitionno >= 0)
		LWLockRelease(partitionlock);
	LWLockRelease(CSNLogCtl->global_shared->ControlLock);
#else
	LWLockAcquire(CSNLogCtl->global_shared->ControlLock, LW_EXCLUSIVE);
	partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	partitionlock = GetPartitionLock(CSNLogCtl, partitionno);

	LWLockAcquire(partitionlock, LW_EXCLUSIVE);
	ZeroCSNLOGPage(pageno, partitionno, true);
	LWLockRelease(partitionlock);
	LWLockRelease(CSNLogCtl->global_shared->ControlLock);
#endif


}


/*
 * Remove all CSNLOG segments before the one holding the passed transaction ID
 *
 * This is normally called during checkpoint, with oldestXact being the
 * oldest TransactionXmin of any running transaction.
 */
void
TruncateCSNLOG(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact. We
	 * pass the *page* containing oldestXact to LruTruncate.
	 */
	cutoffPage = TransactionIdToCSNPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!LruScanDirectory(CSNLogCtl, LruScanDirCbReportPresence, &cutoffPage))
		return;					/* nothing to remove */

	/*
	 * Write XLOG record and flush XLOG to disk. We record the oldest xid
	 * we're keeping information about here so we can ensure that it's always
	 * ahead of clog truncation in case we crash, and so a standby finds out
	 * the new valid xid before the next checkpoint.
	 */
	WriteTruncateXlogRec(cutoffPage, oldestXact);

	elog(LOG, "truncate cutoffpage %d", cutoffPage);
	LruTruncate(CSNLogCtl, cutoffPage);
}


/*
 * Decide which of two CLOG page numbers is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CSNLOGPagePrecedes(int page1, int page2)
{
    TransactionId xid1;
    TransactionId xid2;

    xid1 = ((TransactionId) page1) * CSNLOG_XACTS_PER_PAGE;
    xid1 += FirstNormalTransactionId + 1;
    xid2 = ((TransactionId) page2) * CSNLOG_XACTS_PER_PAGE;
    xid2 += FirstNormalTransactionId + 1;

    return (TransactionIdPrecedes(xid1, xid2) &&
            TransactionIdPrecedes(xid1, xid2 + CSNLOG_XACTS_PER_PAGE - 1));
}


PG_FUNCTION_INFO_V1(pg_xact_get_csn);
/*
 * function api to get csn for given xid
 */
Datum
pg_xact_get_csn(PG_FUNCTION_ARGS)
{
	TransactionId xid = PG_GETARG_UINT32(0);
	CommitSeqNo	csn;
    
	csn = CSNLogGetCSNAdjusted(xid);
    
	PG_RETURN_UINT64(csn);
}

/*
 * Write a ZEROPAGE xlog record
 */
static void
WriteZeroPageXlogRec(int pageno)
{
	XLogBeginInsert();
	XLogRegisterData((char *) (&pageno), sizeof(int));
	(void) XLogInsert(RM_CSNLOG_ID, CSNLOG_ZEROPAGE);
}

/*
 * Write a TRUNCATE xlog record
 *
 * We must flush the xlog record to disk before returning --- see notes
 * in TruncateCLOG().
 */
static void
WriteTruncateXlogRec(int pageno, TransactionId oldestXact)
{
	XLogRecPtr	recptr;
	xl_csnlog_truncate xlrec;

	xlrec.pageno = pageno;
	xlrec.oldestXact = oldestXact;

	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), sizeof(xl_csnlog_truncate));
	recptr = XLogInsert(RM_CSNLOG_ID, CSNLOG_TRUNCATE);
	XLogFlush(recptr);
}

/*
 * Write a SETCSN xlog record
 */
static XLogRecPtr
WriteSetTimestampXlogRec(TransactionId mainxid, int nsubxids,
						 TransactionId *subxids, CommitSeqNo csn)
{
	xl_csn_set	record;

	record.csn = csn;
	record.mainxid = mainxid;

	XLogBeginInsert();
	XLogRegisterData((char *) &record,
                     SizeOfCSNSet);
	XLogRegisterData((char *) subxids, nsubxids * sizeof(TransactionId));
	return XLogInsert(RM_CSNLOG_ID, CSNLOG_SETCSN);
}


/*
 * CSNLOG resource manager's routines
 */
void
csnlog_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* Backup blocks are not used in clog records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == CSNLOG_ZEROPAGE)
	{
		int			pageno;
		int			slotno;
		int			partitionno;
		LWLock	   *partitionlock;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);

		partitionlock = GetPartitionLock(CSNLogCtl, partitionno);
		elog(DEBUG1, "redo csnlog: zero partitionno %d pageno %d", partitionno, pageno);
		LWLockAcquire(partitionlock, LW_EXCLUSIVE);
		slotno = ZeroCSNLOGPage(pageno, partitionno, false);
		LruWritePage(CSNLogCtl, partitionno, slotno);
		Assert(!CSNLogCtl->shared[partitionno]->page_dirty[slotno]);

		LWLockRelease(partitionlock);
	}
	else if (info == CSNLOG_TRUNCATE)
	{
		int			partitionno;
		xl_csnlog_truncate xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xl_csnlog_truncate));

		partitionno = PagenoMappingPartitionno(CSNLogCtl, xlrec.pageno);

		/*
		 * During XLOG replay, latest_page_number isn't set up yet; insert a
		 * suitable value to bypass the sanity test in LruTruncate.
		 */
		CSNLogCtl->shared[partitionno]->latest_page_number = xlrec.pageno;
		CSNLogCtl->global_shared->latest_page_number = xlrec.pageno;

		LruTruncate(CSNLogCtl, xlrec.pageno);
	}
	else if (info == CSNLOG_SETCSN)
	{
		xl_csn_set *setcsn = (xl_csn_set *) XLogRecGetData(record);
		int			nsubxids;
		TransactionId *subxids;

		nsubxids = ((XLogRecGetDataLen(record) - SizeOfCSNSet) /
                    sizeof(TransactionId));

		if (nsubxids > 0)
		{
			subxids = palloc(sizeof(TransactionId) * nsubxids);
			memcpy(subxids,
                   XLogRecGetData(record) + SizeOfCSNSet,
				   sizeof(TransactionId) * nsubxids);
		}
		else
			subxids = NULL;

        CSNLogSetCSN(setcsn->mainxid, nsubxids, subxids, InvalidXLogRecPtr, false, setcsn->csn);
		elog(DEBUG1, "csnlog_redo: set xid %d csn " INT64_FORMAT, setcsn->mainxid, setcsn->csn);
		if (subxids)
			pfree(subxids);
	}
	else
		elog(PANIC, "csnlog_redo: unknown op code %u", info);
}

#ifdef _PG_REGRESS_	

static bool
CheckPageIsReadOk(TransactionId xid)
{
    int			pageno = TransactionIdToCSNPage(xid);
	int			partitionno = PagenoMappingPartitionno(CSNLogCtl, pageno);
	LWLock	   *partitionLock = GetPartitionLock(CSNLogCtl, partitionno);
	bool status;
	int slotno;

	LWLockAcquire(partitionLock, LW_SHARED);

	slotno = LruReadPage_ReadOnly_Locked(CSNLogCtl, partitionno, pageno, true, xid);
	status = CSNLogCtl->shared[partitionno]->page_status[slotno] == LRU_PAGE_VALID;
	LWLockRelease(partitionLock);

	return status;
}

bool 
TestCsnlogTruncateCompress(TransactionId xid)
{
    LruCtl ctl = CSNLogCtl;
    int pageno = xid / CSNLOG_XACTS_PER_PAGE / CSNLOG_XACTS_PER_LSN_GROUP * CSNLOG_XACTS_PER_LSN_GROUP;
    int segno = pageno / LRU_PAGES_PER_SEGMENT - 1;
    bool ret = LruBackupLogFile(ctl, segno);
    TransactionId tmp;
    bool status = true;
    if (!ret)
        return false;

    ret = LruTestTruncateCompress(ctl, pageno, segno);
    if (!ret)
        return false;
    tmp = xid - CSNLOG_XACTS_PER_PAGE * CSNLOG_XACTS_PER_LSN_GROUP - 1;

    if (tmp > xid)
        return false;

    for (; tmp < xid; tmp += CSNLOG_XACTS_PER_PAGE)
    {
        status = CheckPageIsReadOk(tmp);
        if (!status)
        {
            ereport(LOG, (errmsg("could not find xid %u of page", tmp)));
            break;
        }
    }

    LruRestoreBackupFile(ctl, segno);
    return status;
}

bool 
TestCsnlogTruncateRename(TransactionId xid)
{
    LruCtl ctl = CSNLogCtl;
    int pageno = xid / CSNLOG_XACTS_PER_PAGE / CSNLOG_XACTS_PER_LSN_GROUP * CSNLOG_XACTS_PER_LSN_GROUP;
    int segno = pageno / LRU_PAGES_PER_SEGMENT - 1;
    bool ret = LruBackupLogFile(ctl, segno);
    if (!ret)
        return false;

    ret = LruTestTruncateRename(ctl, pageno, segno);
    if (!ret)
        return false;
    TransactionId tmp = xid - CSNLOG_XACTS_PER_PAGE * CSNLOG_XACTS_PER_LSN_GROUP - 1;

    if (tmp > xid)
        return false;
    bool status = true;
    for (; tmp < xid; tmp += CSNLOG_XACTS_PER_PAGE)
    {
        status = CheckPageIsReadOk(tmp);
        if (!status)
        {
            ereport(LOG, (errmsg("could not find xid %u of page", tmp)));
            break;
        }
    }

    LruRestoreBackupFile(ctl, segno);
    return status;
}

#endif
