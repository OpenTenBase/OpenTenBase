/*-------------------------------------------------------------------------
 *
 * tqual.c
 *	  POSTGRES "time qualification" code, ie, tuple visibility rules.
 *
 * NOTE: all the HeapTupleSatisfies routines will update the tuple's
 * "hint" status bits if we see that the inserting or deleting transaction
 * has now committed or aborted (and it is safe to set the hint bits).
 * If the hint bits are changed, MarkBufferDirtyHint is called on
 * the passed-in buffer.  The caller must hold not only a pin, but at least
 * shared buffer content lock on the buffer containing the tuple.
 *
 * NOTE: When using a non-MVCC snapshot, we must check
 * TransactionIdIsInProgress (which looks in the PGXACT array)
 * before TransactionIdDidCommit/TransactionIdDidAbort (which look in
 * pg_xact).  Otherwise we have a race condition: we might decide that a
 * just-committed transaction crashed, because none of the tests succeed.
 * xact.c is careful to record commit/abort in pg_xact before it unsets
 * MyPgXact->xid in the PGXACT array.  That fixes that problem, but it
 * also means there is a window where TransactionIdIsInProgress and
 * TransactionIdDidCommit will both return true.  If we check only
 * TransactionIdDidCommit, we could consider a tuple committed when a
 * later GetSnapshotData call will still think the originating transaction
 * is in progress, which leads to application-level inconsistency.  The
 * upshot is that we gotta check TransactionIdIsInProgress first in all
 * code paths, except for a few cases where we are looking at
 * subtransactions of our own main transaction and so there can't be any
 * race condition.
 *
 * When using an MVCC snapshot, we rely on XidInMVCCSnapshot rather than
 * TransactionIdIsInProgress, but the logic is otherwise the same: do not
 * check pg_xact until after deciding that the xact is no longer in progress.
 *
 *
 * Summary of visibility functions:
 *
 *	 HeapTupleSatisfiesMVCC()
 *		  visible to supplied snapshot, excludes current command
 *	 HeapTupleSatisfiesUpdate()
 *		  visible to instant snapshot, with user-supplied command
 *		  counter and more complex result
 *	 HeapTupleSatisfiesSelf()
 *		  visible to instant snapshot and current command
 *	 HeapTupleSatisfiesDirty()
 *		  like HeapTupleSatisfiesSelf(), but includes open transactions
 *	 HeapTupleSatisfiesVacuum()
 *		  visible to any running transaction, used by VACUUM
 *	 HeapTupleSatisfiesToast()
 *		  visible unless part of interrupted vacuum, used for TOAST
 *	 HeapTupleSatisfiesAny()
 *		  all tuples are visible
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/time/tqual.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "storage/lmgr.h"
#include "storage/buf_internals.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "access/htup_details.h"


extern bool intransaction_purne;

/* Static variables representing various special snapshot semantics */
SnapshotData SnapshotSelfData = {HeapTupleSatisfiesSelf};
SnapshotData SnapshotAnyData = {HeapTupleSatisfiesAny};

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static bool IsXidCommitedBeforeCSN(HeapTupleHeader tuple, TransactionId xid, Snapshot snapshot, Buffer buffer, bool *need_retry,
                                   uint16 old_infomask, bool is_xmin, TransactionIdStatus *hintstatus);
static bool HintedXminVisibleByCSN(HeapTuple htup, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer);

static bool HintedXmaxVisibleByCSN(HeapTuple htup, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer);
#ifdef __SNAPSHOT_CHECK__
static bool SnapshotCheck(TransactionId xid, Snapshot snapshot, int target_res, GlobalTimestamp target_committs);
#else

#define SnapshotCheck(xid, snapshot, target_res, target_committs)

#endif

#endif
/*
#ifdef _MIGRATE_
SnapshotData SnapshotNowData = {HeapTupleSatisfiesNow};
#endif
*/

#ifdef _MIGRATE_
int g_ShardVisibleMode = SHARD_VISIBLE_MODE_VISIBLE;
#endif


/* local functions */
static bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);


GlobalTimestamp 
HeapTupleHeaderGetXminTimestampAtomic(HeapTupleHeader tuple)
{
	return HeapTupleHeaderGetXminTimestamp(tuple);
}

GlobalTimestamp 
HeapTupleHeaderGetXmaxTimestampAtomic(HeapTupleHeader tuple)
{
	return HeapTupleHeaderGetXmaxTimestamp(tuple);
}

void HeapTupleHeaderSetXminTimestampAtomic(HeapTupleHeader tuple, GlobalTimestamp committs)
{
	HeapTupleHeaderSetXminTimestamp(tuple, committs);
	__atomic_or_fetch(&tuple->t_infomask2, (uint16)HEAP_XMIN_TIMESTAMP_UPDATED, __ATOMIC_RELEASE);
}

void HeapTupleHeaderSetXmaxTimestampAtomic(HeapTupleHeader tuple, GlobalTimestamp committs)
{
	if (HeapTupleGtsXmaxIsCmax(tuple))
		tuple->t_infomask &= ~HEAP_GTSXMAX_COMBOCID;
	HeapTupleHeaderSetXmaxTimestamp(tuple, committs);
	__atomic_or_fetch(&tuple->t_infomask2, (uint16)HEAP_XMAX_TIMESTAMP_UPDATED, __ATOMIC_RELEASE);
}


/*
 * SetHintBits()
 *
 * Set commit/abort hint bits on a tuple, if appropriate at this time.
 *
 * It is only safe to set a transaction-committed hint bit if we know the
 * transaction's commit record is guaranteed to be flushed to disk before the
 * buffer, or if the table is temporary or unlogged and will be obliterated by
 * a crash anyway.  We cannot change the LSN of the page here, because we may
 * hold only a share lock on the buffer, so we can only use the LSN to
 * interlock this if the buffer's LSN already is newer than the commit LSN;
 * otherwise we have to just refrain from setting the hint bit until some
 * future re-examination of the tuple.
 *
 * We can always set hint bits when marking a transaction aborted.  (Some
 * code in heapam.c relies on that!)
 *
 * Normal commits may be asynchronous, so for those we need to get the LSN
 * of the transaction and then check whether this is flushed.
 *
 * The caller should pass xid as the XID of the transaction to check, or
 * InvalidTransactionId if no check is needed.
 */

#define SetHintBits(a, b, c, d) SetHintBits_internal(a, b, c, d, __FILE__, __FUNCTION__, __LINE__)

static inline void
SetHintBits_internal(HeapTupleHeader tuple, Buffer buffer,
			uint16 infomask, TransactionId xid, const char *file, const char *func, int line)
{
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	TransactionId xmin;
	TransactionId xmax;
	CommitSeqNo global_timestamp;
#endif

	if (enable_committs_print)
		elog(LOG,
			"SetHintBits: ctid %hu/%hu/%hu, xmin %d, xmax %d, xmin_gts %lu, xmax_gts %lu, infomask %x infomask2 %x, %s %s %d",
			tuple->t_ctid.ip_blkid.bi_hi,
			tuple->t_ctid.ip_blkid.bi_lo,
			tuple->t_ctid.ip_posid,
			HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
			HeapTupleHeaderGetXminTimestampAtomic(tuple), HeapTupleHeaderGetXmaxTimestampAtomic(tuple),
			tuple->t_infomask, tuple->t_infomask2, func, file, line);

	if (TransactionIdIsValid(xid))
	{
		/* NB: xid must be known committed here! */
		XLogRecPtr	commitLSN = TransactionIdGetCommitLSN(xid);

		if (BufferIsPermanent(buffer) && XLogNeedsFlush(commitLSN) &&
			BufferGetLSNAtomic(buffer) < commitLSN)
		{
			if (enable_committs_print)
				elog(LOG,
					"SetHintBits: ctid %hu/%hu/%hu, xmin %d, xmax %d, xmin_gts %lu, xmax_gts %lu, infomask %x infomask2 %x, %s %s %d"
					"buffer lsn %lu, commitLSN %lu",
					tuple->t_ctid.ip_blkid.bi_hi,
					tuple->t_ctid.ip_blkid.bi_lo,
					tuple->t_ctid.ip_posid,
					HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
					HeapTupleHeaderGetXminTimestampAtomic(tuple), HeapTupleHeaderGetXmaxTimestampAtomic(tuple),
					tuple->t_infomask, tuple->t_infomask2, func, file, line, BufferGetLSNAtomic(buffer), commitLSN);
			/* not flushed and no LSN interlock, so don't set hint */
			return;
		}
	}
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	
	if (infomask & HEAP_XMIN_COMMITTED)
	{

		if (!CSN_IS_COMMITTED(HeapTupleHeaderGetXminTimestampAtomic(tuple)))
		{
			xmin = HeapTupleHeaderGetRawXmin(tuple);
			global_timestamp = CSNLogGetCSNAdjusted(xmin);
			if (CSN_IS_COMMITTED(global_timestamp))
				HeapTupleHeaderSetXminTimestampAtomic(tuple, global_timestamp);
			else
				elog(PANIC, "xmin %d should have commit ts but not "UINT64_FORMAT,
						xmin, global_timestamp);

			if (enable_committs_print)
			{
				BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
				RelFileNode *rnode = &bufHdr->tag.rnode;

				elog(LOG,
					"SetHintBits: relfilenode %u pageno %u "
					"CTID %hu/%hu/%hu "
					"infomask %d xmin %u xmin_gts %lu",
					rnode->relNode, bufHdr->tag.blockNum,
					tuple->t_ctid.ip_blkid.bi_hi,
					tuple->t_ctid.ip_blkid.bi_lo,
					tuple->t_ctid.ip_posid,
					tuple->t_infomask, xmin, global_timestamp);
			}
		}
	}
	else if (infomask & HEAP_XMAX_COMMITTED)
	{
		if (enable_committs_print)
			elog(LOG,
				"SetHintBits 1: ctid %hu/%hu/%hu, xmin %d, xmax %d, xmin_gts %lu, xmax_gts %lu, infomask %x infomask2 %x, %s %s %d",
				tuple->t_ctid.ip_blkid.bi_hi,
				tuple->t_ctid.ip_blkid.bi_lo,
				tuple->t_ctid.ip_posid,
				HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
				HeapTupleHeaderGetXminTimestampAtomic(tuple), HeapTupleHeaderGetXmaxTimestampAtomic(tuple),
				tuple->t_infomask, tuple->t_infomask2, func, file, line);

		if (!CSN_IS_COMMITTED(HeapTupleHeaderGetXmaxTimestampAtomic(tuple)))
		{
			Page page = BufferGetPage(buffer);
			xmax = HeapTupleHeaderGetRawXmax(tuple);

			global_timestamp = CSNLogGetCSNAdjusted(xmax);
			if (CSN_IS_COMMITTED(global_timestamp))
				HeapTupleHeaderSetXmaxTimestampAtomic(tuple, global_timestamp);
			else
				elog(PANIC, "xmax %d should have commit ts but not commit ts "UINT64_FORMAT,
							xmax, global_timestamp);
			/* set page prunable hint */
			PageSetPrunableTs(page, global_timestamp);

			if (enable_committs_print)
			{
				BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
				RelFileNode *rnode = &bufHdr->tag.rnode;

				elog(LOG,
					"SetHintBits: relfilenode %u pageno %u "
					"CTID %hu/%hu/%hu "
					"infomask %d multixact %d "
					"xid %u xmax %u xmax_gts "INT64_FORMAT,
					rnode->relNode, bufHdr->tag.blockNum,
					tuple->t_ctid.ip_blkid.bi_hi,
					tuple->t_ctid.ip_blkid.bi_lo,
					tuple->t_ctid.ip_posid,
					tuple->t_infomask, tuple->t_infomask & HEAP_XMAX_IS_MULTI,
					HeapTupleHeaderGetUpdateXid(tuple), xmax, global_timestamp);
			}
		}
	}
		
#endif

	tuple->t_infomask |= infomask;
	MarkBufferDirtyHint(buffer, true);
	if (enable_committs_print)
		elog(LOG,
			"SetHintBits 2: ctid %hu/%hu/%hu, xmin %d, xmax %d, xmin_gts %lu, xmax_gts %lu, infomask %x infomask2 %x, %s %s %d",
			tuple->t_ctid.ip_blkid.bi_hi,
			tuple->t_ctid.ip_blkid.bi_lo,
			tuple->t_ctid.ip_posid,
			HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
			HeapTupleHeaderGetXminTimestampAtomic(tuple), HeapTupleHeaderGetXmaxTimestampAtomic(tuple),
			tuple->t_infomask, tuple->t_infomask2, func, file, line);
}

/*
 * HeapTupleSetHintBits --- exported version of SetHintBits()
 *
 * This must be separate because of C99's brain-dead notions about how to
 * implement inline functions.
 */
void
HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
					 uint16 infomask, TransactionId xid)
{
	SetHintBits(tuple, buffer, infomask, xid);
}


/*
 * HeapTupleSatisfiesSelf
 *		True iff heap tuple is valid "for itself".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "itself" requires the following:
 *
 * ((Xmin == my-transaction &&				the row was updated by the current transaction, and
 *		(Xmax is null						it was not deleted
 *		 [|| Xmax != my-transaction)])			[or it was deleted by another transaction]
 * ||
 *
 * (Xmin is committed &&					the row was modified by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *			(Xmax != my-transaction &&			the row was deleted by another transaction
 *			 Xmax is not committed)))			that has not been committed
 */
bool
HeapTupleSatisfiesSelf(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	bool need_retry;
    TransactionIdStatus	hintstatus;
    
retry:
	need_retry = false;
	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef _MIGRATE_
		
		if (IS_PGXC_DATANODE && ShardIDIsValid(tuple->t_shardid) && SnapshotGetShardTable(snapshot))
		{
			bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
													SnapshotGetShardTable(snapshot));
	
			if (!IsConnFromApp())
			{
				if (!shard_is_visible)
					return false;
			}
			else if (g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
			{
				if ((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
					|| (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
				{
					return false;
				}
			}
		}
#endif

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/*
				 * It is advisable to avoid modifying the state of the tuple in
				 * the sub-process that is connected from datanode. This is due
				 * to the ambiguity of the sub-transaction state, which may
				 * either abort or not yet be synchronized.
				 */
				if (!IsConnFromDatanode())
					SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
								InvalidTransactionId);
				return true;
			}

			return false;
		}
		else
		{
			IsXidCommitedBeforeCSN(tuple, HeapTupleHeaderGetXmin(tuple), snapshot, buffer,
								&need_retry, tuple->t_infomask, true, &hintstatus);
			if (need_retry)
				goto retry;
			if (hintstatus == XID_COMMITTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
			if (hintstatus == XID_ABORTED)
			{
				/*
				 * It is advisable to avoid modifying the state of the tuple in
				 * the sub-process that is connected from datanode. This is due
				 * to the ambiguity of the sub-transaction state, which may
				 * either abort or not yet be synchronized.
				 */
				if (!IsConnFromDatanode())
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			}
			if (hintstatus != XID_COMMITTED)
				return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		IsXidCommitedBeforeCSN(tuple, xmax, snapshot, buffer, &need_retry,
				tuple->t_infomask, false, &hintstatus);

		if (need_retry)
			goto retry;

		if (hintstatus != XID_COMMITTED)
		{
			/* it must have aborted or crashed */
			return true;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	IsXidCommitedBeforeCSN(tuple, HeapTupleHeaderGetRawXmax(tuple), snapshot, buffer,
							&need_retry, tuple->t_infomask, false, &hintstatus);

	if (need_retry)
		goto retry;

	if (hintstatus == XID_ABORTED)
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
		InvalidTransactionId);
	}

	if (hintstatus != XID_COMMITTED)
		return true;

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return false;
}

/*
 * HeapTupleSatisfiesAny
 *		Dummy "satisfies" routine: any tuple satisfies SnapshotAny.
 */
bool
HeapTupleSatisfiesAny(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	return true;
}

/*
 * HeapTupleSatisfiesToast
 *		True iff heap tuple is valid as a TOAST row.
 *
 * This is a simplified version that only checks for VACUUM moving conditions.
 * It's appropriate for TOAST usage because TOAST really doesn't want to do
 * its own time qual checks; if you can see the main table row that contains
 * a TOAST reference, you should be able to see the TOASTed value.  However,
 * vacuuming a TOAST table is independent of the main table, and in case such
 * a vacuum fails partway through, we'd better do this much checking.
 *
 * Among other things, this means you can't do UPDATEs of rows in a TOAST
 * table.
 */
bool
HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot,
						Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		/*
		 * An invalid Xmin can be left behind by a speculative insertion that
		 * is canceled by super-deleting the tuple.  This also applies to
		 * TOAST tuples created during speculative insertion.
		 */
		if (!TransactionIdIsValid(HeapTupleHeaderGetXmin(tuple)))
			return false;
	}

    /* otherwise assume the tuple is valid for TOAST. */
    return true;
}

/*
 * HeapTupleSatisfiesUpdate
 *
 *	This function returns a more detailed result code than most of the
 *	functions in this file, since UPDATE needs to know more than "is it
 *	visible?".  It also allows for user-supplied CommandId rather than
 *	relying on CurrentCommandId.
 *
 *	The possible return codes are:
 *
 *	HeapTupleInvisible: the tuple didn't exist at all when the scan started,
 *	e.g. it was created by a later CommandId.
 *
 *	HeapTupleMayBeUpdated: The tuple is valid and visible, so it may be
 *	updated.
 *
 *	HeapTupleSelfUpdated: The tuple was updated by the current transaction,
 *	after the current scan started.
 *
 *	HeapTupleUpdated: The tuple was updated by a committed transaction.
 *
 *	HeapTupleBeingUpdated: The tuple is being updated by an in-progress
 *	transaction other than the current transaction.  (Note: this includes
 *	the case where the tuple is share-locked by a MultiXact, even if the
 *	MultiXact includes the current transaction.  Callers that want to
 *	distinguish that case must test for it themselves.)
 */
HTSU_Result
HeapTupleSatisfiesUpdate(HeapTuple htup, CommandId curcid,
						 Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionIdStatus	xidstatus;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return HeapTupleInvisible;

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= curcid)
				return HeapTupleInvisible;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HeapTupleMayBeUpdated;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			{
				TransactionId xmax;

				xmax = HeapTupleHeaderGetRawXmax(tuple);

				/*
				 * Careful here: even though this tuple was created by our own
				 * transaction, it might be locked by other transactions, if
				 * the original version was key-share locked when we updated
				 * it.
				 */

				if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
				{
					if (MultiXactIdIsRunning(xmax, true))
						return HeapTupleBeingUpdated;
					else
						return HeapTupleMayBeUpdated;
				}

				/*
				 * If the locker is gone, then there is nothing of interest
				 * left in this Xmax; otherwise, report the tuple as
				 * locked/updated.
				 */
				xidstatus = TransactionIdGetStatusCSN(xmax);
				if (xidstatus != XID_INPROGRESS)
					return HeapTupleMayBeUpdated;
				else
					return HeapTupleBeingUpdated;
			}

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* deleting subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
											 false))
						return HeapTupleBeingUpdated;
					return HeapTupleMayBeUpdated;
				}
				else
				{
					if (HeapTupleHeaderGetCmax(tuple) >= curcid)
						return HeapTupleSelfUpdated;	/* updated after scan
														 * started */
					else
						return HeapTupleInvisible;	/* updated before scan
													 * started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return HeapTupleMayBeUpdated;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return HeapTupleSelfUpdated;	/* updated after scan started */
			else
				return HeapTupleInvisible;	/* updated before scan started */
		}
		else
		{
			xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmin(tuple));
			if (xidstatus == XID_COMMITTED)
			{
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
							HeapTupleHeaderGetRawXmin(tuple));
			}
			else
			{
				if (xidstatus == XID_ABORTED)
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
								InvalidTransactionId);
				return HeapTupleInvisible;
			}
		}
	}

	/* by here, the inserting transaction has committed */

	/*
	 * If the committed xmin is a relatively recent transaction, we want to
	 * make sure that the GTM sees its commit before it sees our
	 * commit since our execution assumes that xmin is committed and hence that
	 * ordering must be followed. There is a small race condition which may
	 * violate this ordering and hence we record such dependencies and ensure
	 * ordering at the commit time
	 */
	if (TransactionIdPrecedesOrEquals(GetRecentXmin(), HeapTupleHeaderGetRawXmin(tuple)))
		TransactionRecordXidWait(HeapTupleHeaderGetRawXmin(tuple));

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return HeapTupleMayBeUpdated;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return HeapTupleMayBeUpdated;
		return HeapTupleUpdated;	/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
			return HeapTupleMayBeUpdated;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		{
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), true))
				return HeapTupleBeingUpdated;

			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			return HeapTupleMayBeUpdated;
		}

		xmax = HeapTupleGetUpdateXid(tuple);
		if (!TransactionIdIsValid(xmax))
		{
			if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
				return HeapTupleBeingUpdated;
		}

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= curcid)
				return HeapTupleSelfUpdated;	/* updated after scan started */
			else
				return HeapTupleInvisible;	/* updated before scan started */
		}

		if (MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
			return HeapTupleBeingUpdated;

		if (TransactionIdDidCommit(xmax))
			return HeapTupleUpdated;

		/*
		 * By here, the update in the Xmax is either aborted or crashed, but
		 * what about the other members?
		 */

		if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * There's no member, even just a locker, alive anymore, so we can
			 * mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HeapTupleMayBeUpdated;
		}
		else
		{
			/* There are lockers running */
			return HeapTupleBeingUpdated;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return HeapTupleBeingUpdated;
		if (HeapTupleHeaderGetCmax(tuple) >= curcid)
			return HeapTupleSelfUpdated;	/* updated after scan started */
		else
			return HeapTupleInvisible;	/* updated before scan started */
	}

	xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmax(tuple));
	switch (xidstatus)
	{
		case XID_INPROGRESS:
			return HeapTupleBeingUpdated;
		case XID_ABORTED:
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HeapTupleMayBeUpdated;
		case XID_COMMITTED:
			break;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return HeapTupleMayBeUpdated;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return HeapTupleUpdated;	/* updated by other */
}

/*
 * HeapTupleSatisfiesDirty
 *		True iff heap tuple is valid including effects of open transactions.
 *
 *	Here, we consider the effects of:
 *		all committed and in-progress transactions (as of the current instant)
 *		previous commands of this transaction
 *		changes made by the current command
 *
 * This is essentially like HeapTupleSatisfiesSelf as far as effects of
 * the current transaction and committed/aborted xacts are concerned.
 * However, we also include the effects of other xacts still in progress.
 *
 * A special hack is that the passed-in snapshot struct is used as an
 * output argument to return the xids of concurrent xacts that affected the
 * tuple.  snapshot->xmin is set to the tuple's xmin if that is another
 * transaction that's still in progress; or to InvalidTransactionId if the
 * tuple's xmin is committed good, committed dead, or my own xact.
 * Similarly for snapshot->xmax and the tuple's xmax.  If the tuple was
 * inserted speculatively, meaning that the inserter might still back down
 * on the insertion without aborting the whole transaction, the associated
 * token is also returned in snapshot->speculativeToken.
 */
bool
HeapTupleSatisfiesDirty(HeapTuple htup, Snapshot snapshot,
						Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionIdStatus xidstatus;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = 0;

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/*
				 * It is advisable to avoid modifying the state of the tuple in
				 * the sub-process that is connected from datanode. This is due
				 * to the ambiguity of the sub-transaction state, which may
				 * either abort or not yet be synchronized.
				 */
				if (!IsConnFromDatanode())
					SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
								InvalidTransactionId);
				return true;
			}

			return false;
		}
		else
		{
			xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmin(tuple));
			switch (xidstatus)
			{
				case XID_INPROGRESS:
					/*
					 * Return the speculative token to caller.  Caller can worry about
					 * xmax, since it requires a conclusively locked row version, and
					 * a concurrent update to this tuple is a conflict of its
					 * purposes.
					 */
					if (HeapTupleHeaderIsSpeculative(tuple))
					{
						snapshot->speculativeToken =
							HeapTupleHeaderGetSpeculativeToken(tuple);

						Assert(snapshot->speculativeToken != 0);
					}

					snapshot->xmin = HeapTupleHeaderGetRawXmin(tuple);
					/* XXX shouldn't we fall through to look at xmax? */
					return true;		/* in insertion by other */
				case XID_COMMITTED:
					SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
								HeapTupleHeaderGetRawXmin(tuple));
					break;
				case XID_ABORTED:
				{
					/*
					 * It is advisable to avoid modifying the state of the tuple in
					 * the sub-process that is connected from datanode. This is due
					 * to the ambiguity of the sub-transaction state, which may
					 * either abort or not yet be synchronized.
					 */
					if (!IsConnFromDatanode())
						SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
					return false;
				}
			}
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;			/* updated by other */
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
			return false;
		
		xidstatus = TransactionIdGetStatusCSN(xmax);
		switch (xidstatus)
		{
			case XID_INPROGRESS:
				snapshot->xmax = xmax;
				return true;
			case XID_COMMITTED:
				return false;
			default:
				/* it must have aborted or crashed */
				return true;
		}
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}


	xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmax(tuple));
	switch (xidstatus)
	{
		case XID_INPROGRESS:
			if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
				snapshot->xmax = HeapTupleHeaderGetRawXmax(tuple);
			return true;
		case XID_ABORTED:
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		case XID_COMMITTED:
			break;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return false;				/* updated by other */
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static bool
HintedXminVisibleByCSN(HeapTuple htup, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{
	GlobalTimestamp global_committs;
	TransactionId xid = HeapTupleHeaderGetRawXmin(tuple);
	bool 	res;

	global_committs = HeapTupleHeaderGetXminTimestampAtomic(tuple);

	if (snapshot->local)
	{
		res =  XidInMVCCSnapshot(xid, snapshot);
		SnapshotCheck(xid, snapshot, res, 0);
		DEBUG_VISIBILE(elog(DEBUG12, "xmin local snapshot ts " INT64_FORMAT " res %d xid %d committs " INT64_FORMAT,
						snapshot->start_ts, res, xid, global_committs));
		return !res;
	}

	Assert(CSN_IS_COMMITTED(global_committs));

	/* logically this is unnecessary, may be removed in the future */
	if (!CSN_IS_COMMITTED(global_committs))
	{

		global_committs = CSNLogGetCSNAdjusted(xid);
		if (!CSN_IS_COMMITTED(global_committs))
		{
			elog(ERROR, "tuple may be corrupted tableOid %u tself %hu/%hu/%hu xmin %d xmax %d infomask %x infomask2 %x",
				 htup->t_tableOid,
				 htup->t_self.ip_blkid.bi_hi,
				 htup->t_self.ip_blkid.bi_lo,
				 htup->t_self.ip_posid,
				 HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
				 tuple->t_infomask, tuple->t_infomask2);
		}
		else
		{
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, xid);
		}
	}

	if (enable_distri_debug)
	{
		snapshot->scanned_tuples_after_committed++;
	}

	DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT " false xid %d committs "INT64_FORMAT" 21.",
				snapshot->start_ts, xid, global_committs));

	if (snapshot->start_ts >= global_committs)
	{
		SnapshotCheck(xid, snapshot, false, global_committs);
		return true;
	}
	else
	{
		SnapshotCheck(xid, snapshot, true, global_committs);
		return false;
	}
}

static bool
HintedXmaxVisibleByCSN(HeapTuple htup, HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{
	GlobalTimestamp global_committs;
	TransactionId xid = HeapTupleHeaderGetRawXmax(tuple);
	bool 	res;

	global_committs = HeapTupleHeaderGetXmaxTimestampAtomic(tuple);
	if (snapshot->local)
	{
		res =  XidInMVCCSnapshot(xid, snapshot);
		SnapshotCheck(xid, snapshot, res, 0);
		DEBUG_VISIBILE(elog(DEBUG12, "xmin local snapshot ts " INT64_FORMAT " res %d xid %d committs " INT64_FORMAT,
					snapshot->start_ts, res, xid, global_committs));
		return res;
	}

	// Assert(CSN_IS_COMMITTED(global_committs));

	/* logically this is unnecessary, may be removed in the future */
	if (!CSN_IS_COMMITTED(global_committs))
	{

		global_committs = CSNLogGetCSNAdjusted(xid);
		if (!CSN_IS_COMMITTED(global_committs))
		{
			elog(ERROR, "tuple may be corrupted tableOid %u tself %hu/%hu/%hu xmin %d xmax %d infomask %x infomask2 %x",
				 htup->t_tableOid,
				 htup->t_self.ip_blkid.bi_hi,
				 htup->t_self.ip_blkid.bi_lo,
				 htup->t_self.ip_posid,
				 HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
				 tuple->t_infomask, tuple->t_infomask2);
		}
		else
		{
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid);
		}
	}
	
	if (enable_distri_debug)
	{
		snapshot->scanned_tuples_after_committed++;
	}

	DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT " xid %d committs "INT64_FORMAT" 22.",
					snapshot->start_ts, xid, global_committs));

	if (snapshot->start_ts >= global_committs)
	{
		SnapshotCheck(xid, snapshot, false, global_committs);
		return false;
	}
	else
	{
		SnapshotCheck(xid, snapshot, true, global_committs);
		return true;
	}
}

/*
 * HeapTupleSatisfiesMVCC
 *		True iff heap tuple is valid for the given MVCC snapshot.
 *
 *	Here, we consider the effects of:
 *		all transactions committed as of the time of the given snapshot
 *		previous commands of this transaction
 *
 *	Does _not_ include:
 *		transactions shown as in-progress by the snapshot
 *		transactions started after the snapshot was taken
 *		changes made by the current command
 *
 * Notice that here, we will not update the tuple status hint bits if the
 * inserting/deleting transaction is still running according to our snapshot,
 * even if in reality it's committed or aborted by now.  This is intentional.
 * Checking the true transaction state would require access to high-traffic
 * shared data structures, creating contention we'd rather do without, and it
 * would not change the result of our visibility check anyway.  The hint bits
 * will be updated by the first visitor that has a snapshot new enough to see
 * the inserting/deleting transaction as done.  In the meantime, the cost of
 * leaving the hint bits unset is basically that each HeapTupleSatisfiesMVCC
 * call will need to run TransactionIdIsCurrentTransactionId in addition to
 * XidInMVCCSnapshot (but it would have to do the latter anyway).  In the old
 * coding where we tried to set the hint bits as soon as possible, we instead
 * did TransactionIdIsInProgress in each call --- to no avail, as long as the
 * inserting/deleting transaction was still running --- which was more cycles
 * and more contention on the PGXACT array.
 */

bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	bool need_retry;
	bool before;
	TransactionIdStatus	hintstatus;

	if (CSN_IS_USE_LATEST(snapshot->start_ts))
	{
		elog(ERROR, "Invalid snapshot, gts: " INT64_FORMAT, snapshot->start_ts);
	}

retry:

	if (enable_committs_print)
		elog(LOG,
			 "HeapTupleSatisfiesMVCC: tself %hu/%hu/%hu, ctid %hu/%hu/%hu, xmin %d, xmax %d, infomask %x infomask2 %x "
			 "t_tableOid %u start_ts "UINT64_FORMAT,
			 htup->t_self.ip_blkid.bi_hi,
			 htup->t_self.ip_blkid.bi_lo,
			 htup->t_self.ip_posid,
			 tuple->t_ctid.ip_blkid.bi_hi,
			 tuple->t_ctid.ip_blkid.bi_lo,
			 tuple->t_ctid.ip_posid,
			 HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple),
			 tuple->t_infomask, tuple->t_infomask2,
			 htup->t_tableOid,snapshot->start_ts
			 );
	
	need_retry = false;
	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

#ifdef _MIGRATE_
	if (IS_PGXC_DATANODE && ShardIDIsValid(tuple->t_shardid) && SnapshotGetShardTable(snapshot))
	{
		bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
											  SnapshotGetShardTable(snapshot));

		if (!IsConnFromApp())
		{
			if (!shard_is_visible)
				return false;
		}
		else if (g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
		{
			if ((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
				|| (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
			{
				return false;
			}
		}
	}
#endif
	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
		{
			if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
			{
				elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmin invalid.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
			}
			return false;
		}

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d current 1.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				return false;	/* inserted after scan started */
			}
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 2.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;
			}
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 3.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;
			}
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
					{
						elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 3.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
					}
					if (enable_distri_debug)
					{
						snapshot->number_visible_tuples++;
					}
					return true;
				}
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				{
					if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
					{
						elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 4.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
					}
					if (enable_distri_debug)
					{
						snapshot->number_visible_tuples++;
					}
					return true;	/* updated after scan started */
				}
				else
				{
					if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
					{
						elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d current 5.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
					}
					return false;	/* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/*
				 * It is advisable to avoid modifying the state of the tuple in
				 * the sub-process that is connected from datanode. This is due
				 * to the ambiguity of the sub-transaction state, which may
				 * either abort or not yet be synchronized.
				 */
				if (!IsConnFromDatanode())
					SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
								InvalidTransactionId);
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 6.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 7.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;	/* deleted after scan started */
			}
			else
			{
				TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple);
				TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);
				TransactionId parent = InvalidTransactionId;

				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d current 8.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}

				if (!intransaction_purne || HeapTupleHeaderSurelyDead(tuple))
					return false;

				parent = FindTransactionParent(xmin);

				/* could not find parent one */
				if (!TransactionIdIsValid(parent))
					return false;

				/*
				 * Mark tuples that no other snapshot will see as surely dead to let index scan skip.
				 * Using FindTransactionParent filtering transactions to identify for RegisterSnapshots.
				 */
				if (parent == FindTransactionParent(xmax))
				{
					if (enable_distri_visibility_print)
						elog(LOG, "Tuple surely dead xmin: %d xmax:%d cmin:%d cmax:%d",
						 HeapTupleHeaderGetRawXmin(tuple),
						 HeapTupleHeaderGetRawXmax(tuple),
						 HeapTupleHeaderGetCmin(tuple) ,HeapTupleHeaderGetCmax(tuple));

					tuple->t_infomask3 |= HEAP_SURELYDEAD;
					MarkBufferDirtyHint(buffer, true);
				}
				return false;	/* deleted before scan started */
			}
		}
		else
		{
			before = IsXidCommitedBeforeCSN(tuple, HeapTupleHeaderGetXmin(tuple), snapshot, buffer, 
							&need_retry, tuple->t_infomask, true, &hintstatus);
			if (need_retry)
				goto retry;
			if (hintstatus == XID_COMMITTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
			if (hintstatus == XID_ABORTED)
			{
				/*
				 * It is advisable to avoid modifying the state of the tuple in
				 * the sub-process that is connected from datanode. This is due
				 * to the ambiguity of the sub-transaction state, which may
				 * either abort or not yet be synchronized.
				 */
				if (!IsConnFromDatanode())
					SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			}
			if (!before)
				return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) )
		{
			if (!HintedXminVisibleByCSN(htup, tuple, snapshot, buffer))
				return false;		/* treat as still in progress */
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
	{
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		return true;
	}
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)){
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		return true;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;	/* deleted after scan started */
			}
			else
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmax %d deleted after scan.", 
												snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple), xmax);
				}
				return false;	/* deleted before scan started */
			}
		}

		before = IsXidCommitedBeforeCSN(tuple, xmax, snapshot, buffer, &need_retry, 
						tuple->t_infomask, false, &hintstatus);
		if (need_retry)
		{
			goto retry;
		}
		/* it must have aborted or crashed */
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		if (before)
			return false;
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;	/* deleted after scan started */
			}
			else
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmax deleted before scan.", 
								snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				return false;	/* deleted before scan started */
			}
		}

		before = IsXidCommitedBeforeCSN(tuple, HeapTupleHeaderGetRawXmax(tuple), snapshot, buffer, 
							&need_retry, tuple->t_infomask, false, &hintstatus);

		if (need_retry)
		{
			goto retry;
		}
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		if (hintstatus == XID_COMMITTED)
		{
			/* xmax transaction committed */
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
		}
		if (hintstatus == XID_ABORTED)
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		}
		if (!before)
			return true;
	}
	else
	{	
		/* xmax is committed, but maybe not according to our snapshot */
		if (HintedXmaxVisibleByCSN(htup, tuple, snapshot, buffer))
		{
			if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
			{
				elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
			}
			if (enable_distri_debug)
			{
				snapshot->number_visible_tuples++;
			}
			return true;		/* treat as still in progress */
		}
	}

	/* xmax transaction committed */
	if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
	{
		elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmax %d committed last.", 
				snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple));
	}
	return false;
}

#ifdef __STORAGE_SCALABLE__
bool
HeapTupleSatisfiesUnshard(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	bool need_retry;
	bool before;
	TransactionIdStatus	hintstatus;
retry:
	need_retry = false;
	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	if (IS_PGXC_DATANODE && tuple->t_shardid < 0)
		return false;

	if (IS_PGXC_DATANODE && tuple->t_shardid >= 0)
	{	
		if (g_DatanodeShardgroupBitmap == NULL)
		{
			elog(ERROR, "shard map in share memory has not been initialized yet.");
		}
		LWLockAcquire(ShardMapLock, LW_SHARED);	
		if (bms_is_member(tuple->t_shardid, g_DatanodeShardgroupBitmap))
		{
			LWLockRelease(ShardMapLock);
			return false;
		}
		LWLockRelease(ShardMapLock);
	}

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
		{
			if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
			{
				elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmin invalid.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
			}
			return false;
		}

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d current 1.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				return false;	/* inserted after scan started */
			}
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 2.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;
			}
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 3.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;
			}
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
				{
					if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
					{
						elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 3.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
					}
					if (enable_distri_debug)
					{
						snapshot->number_visible_tuples++;
					}
					return true;
				}
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				{
					if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
					{
						elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 4.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
					}
					if (enable_distri_debug)
					{
						snapshot->number_visible_tuples++;
					}
					return true;	/* updated after scan started */
				}
				else
				{
					if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
					{
						elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d current 5.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
					}
					return false;	/* updated before scan started */
				}
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 6.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d current 7.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;	/* deleted after scan started */
			}
			else
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d current 8.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				return false;	/* deleted before scan started */
			}
		}
		else
		{
			before = IsXidCommitedBeforeCSN(tuple, HeapTupleHeaderGetXmin(tuple), snapshot, buffer,
									&need_retry, tuple->t_infomask, true, &hintstatus);
			if (need_retry)
				goto retry;
			if (hintstatus == XID_COMMITTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED, HeapTupleHeaderGetRawXmin(tuple));
			if (hintstatus == XID_ABORTED)
				SetHintBits(tuple, buffer, HEAP_XMIN_INVALID, InvalidTransactionId);
			if (!before)
				return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) )
		{
			if (!HintedXminVisibleByCSN(htup, tuple, snapshot, buffer))
				return false;		/* treat as still in progress */
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
	{
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		return true;
	}
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)){
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		return true;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;	/* deleted after scan started */
			}
			else
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmax %d deleted after scan.", 
												snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple), xmax);
				}
				return false;	/* deleted before scan started */
			}
		}
        
        before = IsXidCommitedBeforeCSN(tuple, xmax, snapshot, buffer, &need_retry,
                                        tuple->t_infomask, false, &hintstatus);
        if (need_retry)
        {
            goto retry;
        }
        /* it must have aborted or crashed */
        if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
        {
            elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
        }
        if (enable_distri_debug)
        {
            snapshot->number_visible_tuples++;
        }
        if (before)
            return false;
        return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				if (enable_distri_debug)
				{
					snapshot->number_visible_tuples++;
				}
				return true;	/* deleted after scan started */
			}
			else
			{
				if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
				{
					elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmax deleted before scan.", 
								snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
				}
				return false;	/* deleted before scan started */
			}
		}

		before = IsXidCommitedBeforeCSN(tuple, HeapTupleHeaderGetRawXmax(tuple), snapshot, buffer,
									&need_retry, tuple->t_infomask, false, &hintstatus);

		if (need_retry)
		{
			goto retry;
		}
		if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
		{
			elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
		}
		if (enable_distri_debug)
		{
			snapshot->number_visible_tuples++;
		}
		if (hintstatus == XID_COMMITTED)
		{
			/* xmax transaction committed */
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
		}
		if (hintstatus == XID_ABORTED)
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		}
		if (!before)
			return true;
	}
	else
	{	
		/* xmax is committed, but maybe not according to our snapshot */
		if (HintedXmaxVisibleByCSN(htup, tuple, snapshot, buffer))
		{
			if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
			{
				elog(LOG, "MVCC ts " INT64_FORMAT " true xmin %d.", snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple));
			}
			if (enable_distri_debug)
			{
				snapshot->number_visible_tuples++;
			}
			return true;		/* treat as still in progress */
		}
	}

	/* xmax transaction committed */
	if (enable_distri_visibility_print && TransactionIdIsNormal(HeapTupleHeaderGetRawXmin(tuple)))
	{
		elog(LOG, "MVCC ts " INT64_FORMAT " false xmin %d xmax %d committed last.", 
				snapshot->start_ts, HeapTupleHeaderGetRawXmin(tuple), HeapTupleHeaderGetRawXmax(tuple));
	}
	return false;
}

#endif

#else
#ifdef __STORAGE_SCALABLE__
bool
HeapTupleSatisfiesUnshard(HeapTuple htup, Snapshot snapshot, Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);
	
	if (IS_PGXC_DATANODE && tuple->t_shardid < 0)
		return false;

	if (IS_PGXC_DATANODE && tuple->t_shardid >= 0)
	{	
		if (g_DatanodeShardgroupBitmap == NULL)
		{
			elog(ERROR, "shard map in share memory has not been initialized yet.");
		}
		LWLockAcquire(ShardMapLock, LW_SHARED);	
		if (bms_is_member(tuple->t_shardid, g_DatanodeShardgroupBitmap))
		{
			LWLockRelease(ShardMapLock);
			return false;
		}
		LWLockRelease(ShardMapLock);
	}

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true;	/* updated after scan started */
				else
					return false;	/* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;		/* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		if (XidInMVCCSnapshot(xmax, snapshot))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;		/* updating transaction committed */
		/* it must have aborted or crashed */
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}

		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
					HeapTupleHeaderGetRawXmax(tuple));
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}
#endif

bool
HeapTupleSatisfiesMVCC(HeapTuple htup, Snapshot snapshot,
					   Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);
	
#ifdef _MIGRATE_
	if (IS_PGXC_DATANODE && ShardIDIsValid(tuple->t_shardid) && SnapshotGetShardTable(snapshot))
	{
		bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
												SnapshotGetShardTable(snapshot));

		if (!IsConnFromApp())
		{
			if (!shard_is_visible)
				return false;
		}
		else if (g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
		{
			if ((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
				|| (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
			{
				return false;
			}
		}
	}
#endif

	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return false;

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= snapshot->curcid)
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);

				/* not LOCKED_ONLY, so it has to have an xmax */
				Assert(TransactionIdIsValid(xmax));

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
					return true;	/* updated after scan started */
				else
					return false;	/* updated before scan started */
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetRawXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}
	else
	{
		/* xmin is committed, but maybe not according to our snapshot */
		if (!HeapTupleHeaderXminFrozen(tuple) &&
			XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot))
			return false;		/* treat as still in progress */
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		xmax = HeapTupleGetUpdateXid(tuple);

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		if (XidInMVCCSnapshot(xmax, snapshot))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;		/* updating transaction committed */
		/* it must have aborted or crashed */
		return true;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}

		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;

		if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return true;
		}

		/* xmax transaction committed */
		SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
					HeapTupleHeaderGetRawXmax(tuple));
	}
	else
	{
		/* xmax is committed, but maybe not according to our snapshot */
		if (XidInMVCCSnapshot(HeapTupleHeaderGetRawXmax(tuple), snapshot))
			return true;		/* treat as still in progress */
	}

	/* xmax transaction committed */

	return false;
}
#endif

/*
 * HeapTupleSatisfiesVacuum
 *
 *	Determine the status of tuples for VACUUM purposes.  Here, what
 *	we mainly want to know is if a tuple is potentially visible to *any*
 *	running transaction.  If so, it can't be removed yet by VACUUM.
 *
 * OldestXmin is a cutoff XID (obtained from GetOldestXmin()).  Tuples
 * deleted by XIDs >= OldestXmin are deemed "recently dead"; they might
 * still be visible to some open transaction, so we can't remove them,
 * even if we see that the deleting transaction has committed.
 */
HTSV_Result
HeapTupleSatisfiesVacuum(HeapTuple htup, TransactionId OldestXmin,
						 Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionIdStatus	xidstatus;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);


	if (enable_committs_print)
		elog(LOG, 
				"HeapTupleSatisfiesVacuum: tself %hu/%hu/%hu, ctid %hu/%hu/%hu, xmin %d, xmax %d,"
				" infomask %x infomask2 %x tableOid %u",
				htup->t_self.ip_blkid.bi_hi,
				htup->t_self.ip_blkid.bi_lo,
				htup->t_self.ip_posid,
				tuple->t_ctid.ip_blkid.bi_hi,
				tuple->t_ctid.ip_blkid.bi_lo,
				tuple->t_ctid.ip_posid,
				HeapTupleHeaderGetRawXmin(tuple), 
				HeapTupleHeaderGetRawXmax(tuple), 
				tuple->t_infomask, 
				tuple->t_infomask2,
				htup->t_tableOid);

	/*
	 * Has inserting transaction committed?
	 *
	 * If the inserting transaction aborted, then the tuple was never visible
	 * to any other transaction, so we can delete it immediately.
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
	{
		if (HeapTupleHeaderXminInvalid(tuple))
			return HEAPTUPLE_DEAD;
		else if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tuple)))
		{
			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* only locked? run infomask-only check first, for performance */
			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) ||
				HeapTupleHeaderIsOnlyLocked(tuple))
				return HEAPTUPLE_INSERT_IN_PROGRESS;
			/* inserted and then deleted by same xact */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tuple)))
				return HEAPTUPLE_DELETE_IN_PROGRESS;
			/* deleting subtransaction must have aborted */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		}

		xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmin(tuple));

		if (xidstatus == XID_INPROGRESS)
		{
			/*
			 * It'd be possible to discern between INSERT/DELETE in progress
			 * here by looking at xmax - but that doesn't seem beneficial for
			 * the majority of callers and even detrimental for some. We'd
			 * rather have callers look at/wait for xmin than xmax. It's
			 * always correct to return INSERT_IN_PROGRESS because that's
			 * what's happening from the view of other backends.
			 */
			return HEAPTUPLE_INSERT_IN_PROGRESS;
		}
		else if (xidstatus == XID_COMMITTED)
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetRawXmin(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return HEAPTUPLE_DEAD;
		}

		/*
		 * At this point the xmin is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Okay, the inserter committed, so it was good at some point.  Now what
	 * about the deleting transaction?
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return HEAPTUPLE_LIVE;

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		/*
		 * "Deleting" xact really only locked it, so the tuple is live in any
		 * case.  However, we should make sure that either XMAX_COMMITTED or
		 * XMAX_INVALID gets set once the xact is gone, to reduce the costs of
		 * examining the tuple for future xacts.
		 */
		if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		{
			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				/*
				 * If it's a pre-pg_upgrade tuple, the multixact cannot
				 * possibly be running; otherwise have to check.
				 */
				if (!HEAP_LOCKED_UPGRADED(tuple->t_infomask) &&
					MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple),
										 true))
					return HEAPTUPLE_LIVE;
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
			}
			else
			{
				xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmax(tuple));
				if (xidstatus == XID_INPROGRESS)
					return HEAPTUPLE_LIVE;
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
			}
		}

		/*
		 * We don't really care whether xmax did commit, abort or crash. We
		 * know that xmax did lock the tuple, but it did not and will never
		 * actually update it.
		 */

		return HEAPTUPLE_LIVE;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax = HeapTupleGetUpdateXid(tuple);
		CommitSeqNo committs;

		/* already checked above */
		Assert(!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		/* not LOCKED_ONLY, so it has to have an xmax */
		Assert(TransactionIdIsValid(xmax));

		/*
		* TransactionIdIsActive() does not consider subtransactions and
		* prepared transactions, which would however mark running or 
		* committed transactions as aborted by mistaken.
		* 
		* BUG:  diverged version chain found under the mixed TPCC workload
		* and consistency checking.
		* 
		* FIX: Use the CTS status as the ground truth for transaction status.
		* 
		* Improve: Only fetch CTS once for both status and prunable check.
		*/
		committs = TransactionIdGetCSN(xmax);

		Assert(CSN_IS_INPROGRESS(committs)
				|| CSN_IS_PREPARED(committs)
				|| CSN_IS_COMMITTED(committs)
				|| CSN_IS_ABORTED(committs));

		if (CSN_IS_INPROGRESS(committs)|| CSN_IS_PREPARED(committs))
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (CSN_IS_COMMITTED(committs))
		{
			/*
			* The multixact might still be running due to lockers.  If the
			* updater is below the xid horizon, we have to return DEAD
			* regardless -- otherwise we could end up with a tuple where the
			* updater has to be removed due to the horizon, but is not pruned
			* away.  It's not a problem to prune that tuple, because any
			* remaining lockers will also be present in newer tuple versions.
			*/
			if (!TransactionIdPrecedes(xmax, OldestXmin))
				return HEAPTUPLE_RECENTLY_DEAD;

			if (!CSN_IS_COMMITTED(committs))
			{
				elog(ERROR, "xmax %d should have committs "UINT64_FORMAT, xmax, committs);
			}

			if (!TestForOldTimestampForOriginRecentDataTs(committs, (htup->t_tableOid == RelationRelationId)))
				return HEAPTUPLE_RECENTLY_DEAD;

			return HEAPTUPLE_DEAD;
		}
		else if (!MultiXactIdIsRunning(HeapTupleHeaderGetRawXmax(tuple), false))
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed.
			 * Mark the Xmax as invalid.
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
		}
		
		return HEAPTUPLE_LIVE;
	}

	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
	{
		xidstatus = TransactionIdGetStatusCSN(HeapTupleHeaderGetRawXmax(tuple));

		if (xidstatus == XID_INPROGRESS)
			return HEAPTUPLE_DELETE_IN_PROGRESS;
		else if (xidstatus == XID_COMMITTED)
			SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, HeapTupleHeaderGetRawXmax(tuple));
		else
		{
			/*
			 * Not in Progress, Not Committed, so either Aborted or crashed
			 */
			SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
						InvalidTransactionId);
			return HEAPTUPLE_LIVE;
		}

		/*
		 * At this point the xmax is known committed, but we might not have
		 * been able to set the hint bit yet; so we can no longer Assert that
		 * it's set.
		 */
	}

	/*
	 * Deleter committed, but perhaps it was recent enough that some open
	 * transactions could still see the tuple.
	 */

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
		return HEAPTUPLE_RECENTLY_DEAD;

	{
		CommitSeqNo committs = HeapTupleHeaderGetXmaxTimestampAtomic(tuple);

		if (!CSN_IS_COMMITTED(committs))
		{
			committs = TransactionIdGetCSN(HeapTupleHeaderGetRawXmax(tuple));
		}

		if (!CSN_IS_COMMITTED(committs))
		{
			elog(ERROR, "xmax %d should have committs "UINT64_FORMAT, HeapTupleHeaderGetRawXmax(tuple), committs);
		}

		if (!TestForOldTimestampForOriginRecentDataTs(committs, (htup->t_tableOid == RelationRelationId)))
		{
			if (vacuum_debug_print)
				elog(LOG, "vacuum RECENTLY DEAD committs "INT64_FORMAT "RecentDataTs "INT64_FORMAT, committs, GetRecentRecentDataTs());
			return HEAPTUPLE_RECENTLY_DEAD;
		}
		if (vacuum_debug_print)
			elog(LOG, "vacuum  DEAD committs "INT64_FORMAT "RecentDataTs "INT64_FORMAT, committs, GetRecentRecentDataTs());
			
	}
	
	return HEAPTUPLE_DEAD;
#else
	if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
		return HEAPTUPLE_RECENTLY_DEAD;

	/* Otherwise, it's dead and removable */
	return HEAPTUPLE_DEAD;
#endif
}

/*
 * HeapTupleIsSurelyDead
 *
 *	Cheaply determine whether a tuple is surely dead to all onlookers.
 *	We sometimes use this in lieu of HeapTupleSatisfiesVacuum when the
 *	tuple has just been tested by another visibility routine (usually
 *	HeapTupleSatisfiesMVCC) and, therefore, any hint bits that can be set
 *	should already be set.  We assume that if no hint bits are set, the xmin
 *	or xmax transaction is still running.  This is therefore faster than
 *	HeapTupleSatisfiesVacuum, because we don't consult PGXACT nor CLOG.
 *	It's okay to return FALSE when in doubt, but we must return TRUE only
 *	if the tuple is removable.
 */
bool
HeapTupleIsSurelyDead(HeapTuple htup, TransactionId OldestXmin,
					  bool within_transaction)
{
	HeapTupleHeader tuple = htup->t_data;

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/*
	 * If no recovery running, skip surely dead tuple.
	 */
	if (!RecoveryInProgress() && within_transaction
		&& HeapTupleHeaderSurelyDead(tuple))
		return true;

	/*
	 * If the inserting transaction is marked invalid, then it aborted, and
	 * the tuple is definitely dead.  If it's marked neither committed nor
	 * invalid, then we assume it's still alive (since the presumption is that
	 * all relevant hint bits were just set moments ago).
	 */
	if (!HeapTupleHeaderXminCommitted(tuple))
        return HeapTupleHeaderXminInvalid(tuple) ? true : false;

	/*
	 * If the inserting transaction committed, but any deleting transaction
	 * aborted, the tuple is still alive.
	 */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return false;

	/*
	 * If the XMAX is just a lock, the tuple is still alive.
	 */
	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return false;

	/*
	 * If the Xmax is a MultiXact, it might be dead or alive, but we cannot
	 * know without checking pg_multixact.
	 */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		return false;

	/* If deleter isn't known to have committed, assume it's still running. */
	if (!(tuple->t_infomask & HEAP_XMAX_COMMITTED))
		return false;

	/* Deleter committed, so tuple is dead if the XID is old enough. */
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	{
		if (!TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin))
			return false;
		else 
		{
			CommitSeqNo committs = HeapTupleHeaderGetXmaxTimestampAtomic(tuple);

			if (!CSN_IS_COMMITTED(committs))
			{
				committs = TransactionIdGetCSN(HeapTupleHeaderGetRawXmax(tuple));
			}

			if (!CSN_IS_COMMITTED(committs))
			{
					elog(ERROR, "xmax %d should have committs "UINT64_FORMAT, HeapTupleHeaderGetRawXmax(tuple), committs);
			}

			if (TestForOldTimestampForOriginRecentDataTs(committs, true))
				return true;

			return false;
		}
	}
#endif
	return TransactionIdPrecedes(HeapTupleHeaderGetRawXmax(tuple), OldestXmin);
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__


#ifdef __OPENTENBASE_DEBUG__

static HTAB *SharedSnapHash;


typedef struct
{
	GlobalTimestamp start_ts;
	int64 			xid;
} SnapTag;

typedef struct
{
	SnapTag			tag;			/* Tag of a snapshot */
	int				res;				/* Snapshot Result */
	GlobalTimestamp 	committs;			/* Committs if committed or preparets if prepared */
} SnapLookupEnt;

#define INIT_SNAPTAG(a, ts, xid) \
( \
	(a).start_ts = (ts),\
	(a).xid = (xid)\
)


static uint32
SnapTableHashCode(SnapTag *tagPtr)
{
	return get_hash_value(SharedSnapHash, (void *) tagPtr);
}
int
SnapTableLookup(SnapTag *tagPtr, uint32 hashcode, GlobalTimestamp *committs);
int
SnapTableInsert(SnapTag *tagPtr, uint32 hashcode, int res, GlobalTimestamp committs);
void
SnapTableDelete(SnapTag *tagPtr, uint32 hashcode);

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
SnapTableLookup(SnapTag *tagPtr, uint32 hashcode, GlobalTimestamp *committs)
{
	SnapLookupEnt *result;

	result = (SnapLookupEnt *)
		hash_search_with_hash_value(SharedSnapHash,
									(void *) tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	if (committs)
		*committs = result->committs;
	return result->res;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
SnapTableInsert(SnapTag *tagPtr, uint32 hashcode, int res, GlobalTimestamp committs)
{
	SnapLookupEnt *result;
	bool		found;

	Assert(res >= 0);		/* -1 is reserved for not-in-table */

	result = (SnapLookupEnt *)
		hash_search_with_hash_value(SharedSnapHash,
									(void *) tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (found)					/* found something already in the table */
		return result->res;

	result->res = res;
	result->committs = committs; 
	if (enable_distri_print)
	{
		elog(LOG, "snaptable insert xid %u ts "INT64_FORMAT" hashcode %u res %d ts " INT64_FORMAT, 
			tagPtr->xid, tagPtr->start_ts, hashcode, res, committs);
	}
	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
SnapTableDelete(SnapTag *tagPtr, uint32 hashcode)
{
	SnapLookupEnt *result;

	result = (SnapLookupEnt *)
		hash_search_with_hash_value(SharedSnapHash,
									(void *) tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);
	if (enable_distri_print)
	{
		elog(LOG, "delete xid %u "INT64_FORMAT, tagPtr->xid, tagPtr->start_ts);
	}
	if (!result)				/* shouldn't happen */
	{
		elog(ERROR, "shared snap buffer hash table corrupted xid %u "INT64_FORMAT, tagPtr->xid, tagPtr->start_ts);
	}
}



#define NUM_SNAP_PARTITIONS 128

typedef struct SnapSharedData
{
	/* LWLocks */
	int			lwlock_tranche_id;
	LWLockPadded locks[NUM_SNAP_PARTITIONS];
} SnapSharedData;

typedef SnapSharedData *SnapShared;

SnapShared snapShared;

#define SnapHashPartition(hashcode) \
	((hashcode) % NUM_SNAP_PARTITIONS)

#define MAX_SIZE MaxTransactionId >> 10
Size
SnapTableShmemSize(void)
{
	int size = MAX_SIZE;
	
	return add_size(hash_estimate_size(size, sizeof(SnapLookupEnt)), MAXALIGN(sizeof(SnapSharedData)));
}

static uint32
snap_hash(const void *key, Size keysize)
{
	const SnapTag *tagPtr = key;
	return tagPtr->xid;
}

static int snap_cmp (const void *key1, const void *key2,
								Size keysize)
{
	const SnapTag *tagPtr1 = key1, *tagPtr2 = key2;

	if (tagPtr1->start_ts == tagPtr2->start_ts 
		&& tagPtr1->xid == tagPtr2->xid)
		return 0;

	return 1;

}


void
InitSnapBufTable(void)
{
	HASHCTL		info;
	long max_size = MAX_SIZE;
	int size = MAX_SIZE;
	bool found;
	int i;
	/* assume no locking is needed yet */

	/* BufferTag maps to Buffer */
	info.keysize = sizeof(SnapTag);
	info.entrysize = sizeof(SnapLookupEnt);
	info.num_partitions = NUM_SNAP_PARTITIONS;
	info.hash 		= snap_hash;
	info.match		= snap_cmp;

	SharedSnapHash = ShmemInitHash("Shared Snapshot Lookup Table",
								  size, size,
								  &info,
								  HASH_ELEM  | HASH_COMPARE | HASH_PARTITION | HASH_FUNCTION);

	snapShared = (SnapShared) ShmemInitStruct("Global Snapshot Shared Data",
										  sizeof(SnapSharedData),
										  &found);

	if (!found)
	{
		snapShared->lwlock_tranche_id = LWTRANCHE_SNAPSHOT;
		for (i = 0; i < NUM_SNAP_PARTITIONS; i++)
			LWLockInitialize(&snapShared->locks[i].lock,
								 snapShared->lwlock_tranche_id);
	}
	LWLockRegisterTranche(snapShared->lwlock_tranche_id,
							  "snapshot");	
	
}

bool LookupPreparedXid(TransactionId xid, GlobalTimestamp *prepare_timestamp)
{
	SnapTag 			tag;
	uint32				hash;		/* hash value for newTag */
	int 				partitionno;
	LWLock	   			*partitionLock;	/* buffer partition lock for it */
	int 				res;
	
	INIT_SNAPTAG(tag, 0, xid);
	hash = SnapTableHashCode(&tag);
	partitionno = SnapHashPartition(hash);
	
	/* Try to find the page while holding only shared lock */
	partitionLock = &snapShared->locks[partitionno].lock;
	LWLockAcquire(partitionLock, LW_SHARED);
	res = SnapTableLookup(&tag, hash, prepare_timestamp);
	LWLockRelease(partitionLock);
	if (res == -1)
		res = 0;

	if (res == 0)
	{
		if (!TransactionIdIsInProgress(xid))
			res = 1;
	}else
	{
		if (enable_distri_print)
		{
			elog(LOG, "xid %d prepared transaction prep ts " INT64_FORMAT " abort %d commit %d", 
									xid,  prepare_timestamp, TransactionIdDidAbort(xid), TransactionIdDidCommit(xid));
		}
	}
	return res;

}
void DeletePreparedXid(TransactionId xid)
{
	SnapTag 			tag;
	uint32				hash;		/* hash value for newTag */
	int 				partitionno;
	LWLock	   			*partitionLock;	/* buffer partition lock for it */
	int 				res;
	GlobalTimestamp		ts;
	
	INIT_SNAPTAG(tag, 0, xid);
	hash = SnapTableHashCode(&tag);
	partitionno = SnapHashPartition(hash);
	/* Try to find the page while holding only shared lock */
	partitionLock = &snapShared->locks[partitionno].lock;
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	SnapTableDelete(&tag, hash);
	LWLockRelease(partitionLock);
	return;
}

void InsertPreparedXid(TransactionId xid, GlobalTimestamp prepare_timestamp)
{

	SnapTag 			tag;
	uint32				hash;		/* hash value for newTag */
	int 				partitionno;
	LWLock	   			*partitionLock;	/* buffer partition lock for it */
	int 				res;
	GlobalTimestamp 	ts;
	
	INIT_SNAPTAG(tag, 0, xid);
	hash = SnapTableHashCode(&tag);
	partitionno = SnapHashPartition(hash);
	
	/* Try to find the page while holding only shared lock */
	partitionLock = &snapShared->locks[partitionno].lock;
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);
	if (SnapTableInsert(&tag, hash, 1, prepare_timestamp) != -1)
		elog(ERROR, "insert fails");

	LWLockRelease(partitionLock);


}

#ifdef __SNAPSHOT_CHECK__

static bool SnapshotCheck(TransactionId xid, Snapshot snapshot, int target_res, GlobalTimestamp target_committs)
{
	SnapTag 			tag;
	uint32				hash;		/* hash value for newTag */
	int 				partitionno;
	LWLock	   			*partitionLock;	/* buffer partition lock for it */
	int 				res;
	GlobalTimestamp		res_committs;

	if (snapshot->local)
		return true;
	
	INIT_SNAPTAG(tag, snapshot->start_ts, xid);
	hash = SnapTableHashCode(&tag);
	partitionno = SnapHashPartition(hash);
	
	/* Try to find the page while holding only shared lock */
	partitionLock = &snapShared->locks[partitionno].lock;
	LWLockAcquire(partitionLock, LW_SHARED);
	res = SnapTableLookup(&tag, hash, &res_committs);
	if (res >= 0)
	{
		if (res != target_res)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("snapshot check fails xid %d snapshot start_ts " INT64_FORMAT
					 		"autovacuum %d res %d target_res %d res committs " INT64_FORMAT
					 		"target committs " INT64_FORMAT,
					 		xid,
					 		snapshot->start_ts,
					 		IsAutoVacuumWorkerProcess(),
					 		res,
					 		target_res,
					 		res_committs,
					 		target_committs)));
		
		if (GlobalTimestampIsValid(res_committs))
		{
			if (res_committs != target_committs)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("snapshot check fails xid %d snapshot start_ts " INT64_FORMAT
								"autovacuum %d res %d target_res %d res committs " INT64_FORMAT
								"target committs " INT64_FORMAT,
								xid,
								snapshot->start_ts,
								IsAutoVacuumWorkerProcess(),
								res,
								target_res,
								res_committs,
								target_committs)));
		}
		else if (GlobalTimestampIsValid(target_committs))
		{
			LWLockRelease(partitionLock);
			LWLockAcquire(partitionLock, LW_EXCLUSIVE);
			SnapTableDelete(&tag, hash);
			if (SnapTableInsert(&tag, hash, res, target_committs) != -1)
				elog(ERROR, "insert fails");		

		}
		LWLockRelease(partitionLock);
		elog(DEBUG12, "snapshot check succeeds xid %d snapshot start_ts " INT64_FORMAT " autovacuum %d res %d target_res %d res committs " INT64_FORMAT " target committs " INT64_FORMAT , 
						 xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(), res, target_res, res_committs, target_committs);
	}
	else
	{
		LWLockRelease(partitionLock);
		LWLockAcquire(partitionLock, LW_EXCLUSIVE);
		if (SnapTableInsert(&tag, hash, target_res, target_committs) != -1)
			elog(ERROR, "insert fails");
		LWLockRelease(partitionLock);
		elog(DEBUG12, "snapshot check inserts xid %d snapshot start_ts " INT64_FORMAT " autovacuum %d res %d target_res %d res committs " INT64_FORMAT " target committs " INT64_FORMAT, 
						 xid, snapshot->start_ts, IsAutoVacuumWorkerProcess(), res, target_res, res_committs, target_committs);
	}

	return true;

}


#endif /* __SNAPSHOT_CHECK__ */

#endif /* __OPENTENBASE_DEBUG__ */

#endif  /* __SUPPORT_DISTRIBUTED_TRANSACTION__ */

/*
 * XidInMVCCSnapshot
 *		Is the given XID still-in-progress according to the snapshot?
 *
 * Note: GetSnapshotData never stores either top xid or subxids of our own
 * backend into a snapshot, so these xids will not be reported as "running"
 * by this function.  This is OK for current uses, because we always check
 * TransactionIdIsCurrentTransactionId first, except for known-committed
 * XIDs which could not be ours anyway.
 */
static bool
XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot)
{
	uint32		i;

	/*
	 * Make a quick range check to eliminate most XIDs without looking at the
	 * xip arrays.  Note that this is OK even if we convert a subxact XID to
	 * its parent below, because a subxact with XID < xmin has surely also got
	 * a parent with XID < xmin, while one with XID >= xmax must belong to a
	 * parent that was not yet committed at the time of this snapshot.
	 */

	/* Any xid < xmin is not in-progress */
	if (TransactionIdPrecedes(xid, snapshot->xmin))
		return false;
	/* Any xid >= xmax is in-progress */
	if (TransactionIdFollowsOrEquals(xid, snapshot->xmax))
		return true;

	/*
	 * Snapshot information is stored slightly differently in snapshots taken
	 * during recovery.
	 */
	if (!snapshot->takenDuringRecovery)
	{
		/*
		 * If the snapshot contains full subxact data, the fastest way to
		 * check things is just to compare the given XID against both subxact
		 * XIDs and top-level XIDs.  If the snapshot overflowed, we have to
		 * use pg_subtrans to convert a subxact XID to its parent XID, but
		 * then we need only look at top-level XIDs not subxacts.
		 */
		if (!snapshot->suboverflowed)
		{
			/* we have full data, so search subxip */
			int32		j;

			for (j = 0; j < snapshot->subxcnt; j++)
			{
				if (TransactionIdEquals(xid, snapshot->subxip[j]))
					return true;
			}

			/* not there, fall through to search xip[] */
		}
		else
		{
			/*
			 * Snapshot overflowed, so convert xid to top-level.  This is safe
			 * because we eliminated too-old XIDs above.
			 */
			xid = CSNSubTransGetTopmostTransaction(xid);

			/*
			 * If xid was indeed a subxact, we might now have an xid < xmin,
			 * so recheck to avoid an array scan.  No point in rechecking
			 * xmax.
			 */
			if (TransactionIdPrecedes(xid, snapshot->xmin))
				return false;
		}

		for (i = 0; i < snapshot->xcnt; i++)
		{
			if (TransactionIdEquals(xid, snapshot->xip[i]))
				return true;
		}
	}
	else
	{
		int32		j;

		/*
		 * In recovery we store all xids in the subxact array because it is by
		 * far the bigger array, and we mostly don't know which xids are
		 * top-level and which are subxacts. The xip array is empty.
		 *
		 * We start by searching subtrans, if we overflowed.
		 */
		if (snapshot->suboverflowed)
		{
			/*
			 * Snapshot overflowed, so convert xid to top-level.  This is safe
			 * because we eliminated too-old XIDs above.
			 */
			xid = CSNSubTransGetTopmostTransaction(xid);

			/*
			 * If xid was indeed a subxact, we might now have an xid < xmin,
			 * so recheck to avoid an array scan.  No point in rechecking
			 * xmax.
			 */
			if (TransactionIdPrecedes(xid, snapshot->xmin))
				return false;
		}

		/*
		 * We now have either a top-level xid higher than xmin or an
		 * indeterminate xid. We don't know whether it's top level or subxact
		 * but it doesn't matter. If it's present, the xid is visible.
		 */
		for (j = 0; j < snapshot->subxcnt; j++)
		{
			if (TransactionIdEquals(xid, snapshot->subxip[j]))
				return true;
		}
	}

	return false;
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static bool
IsXidCommitedBeforeCSN(HeapTupleHeader tuple, TransactionId xid, Snapshot snapshot, Buffer buffer, bool *need_retry,
                       uint16 old_infomask, bool is_xmin, TransactionIdStatus *hintstatus)
{
	CommitSeqNo committs;

	*hintstatus = XID_INPROGRESS;
	*need_retry = false;

	/* now only used by subscription to copy data */
	if (snapshot->local)
	{

		if (XidInMVCCSnapshot(xid, snapshot))
			return false;
		else
		{
			committs = TransactionIdGetCSN(xid);
			if (CSN_IS_COMMITTED(committs))
			*hintstatus = XID_COMMITTED;
			else if (CSN_IS_ABORTED(committs))
			*hintstatus = XID_ABORTED;

			return true;
		}
	}

retry:
	committs = TransactionIdGetCSN(xid);

	/* 
	 * For OpenTenBase, we propose a concurrency control mechanism
	 * based on global timestamp to maintain distributed transaction consistency.
	 * 
	 * Rule: T2 can see T1's modification only if T2.start > T1.commit.
	 * For read-committed isolation, T2.start is the executing statement's start timestmap.
	 * 
	 */

	if (CSN_IS_SUBTRANS(committs))
	{
		xid = CSNSubTransGetTopmostTransaction(xid);
		goto retry;
	}
	else if (CSN_IS_PREPARED(committs))
	{
		int         lock_type = -1;
		BufferDesc *buf;
		CommitSeqNo preparets = UNMASK_PREPARE_BIT(committs);

		if (snapshot->is_catalog)
			return false;

		if (enable_distri_debug)
		{
			snapshot->scanned_tuples_after_prepare++;
		}

		/* avoid unnecessary wait */
		if (snapshot->start_ts <= preparets || !CSN_IS_NORMAL(preparets))
		{
			SnapshotCheck(xid, snapshot, true, 0);

			DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT " true xid %d"
						" preparets " INT64_FORMAT" committs "UINT64_FORMAT" .", snapshot->start_ts, xid, preparets, committs));
			return false;
		}
		else
		{
				DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT " wait xid %d preparets "UINT64_FORMAT" committs "UINT64_FORMAT" .",
				snapshot->start_ts, xid, preparets, committs));
		}

		buf = GetBufferDescriptor(buffer - 1);

		if (LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_EXCLUSIVE))
		{
			lock_type = BUFFER_LOCK_EXCLUSIVE;
		}
		else if (LWLockHeldByMeInMode(BufferDescriptorGetContentLock(buf), LW_SHARED))
		{
			lock_type = BUFFER_LOCK_SHARE;
		}

		/* Wait for regular transaction to end; but unlock buffer first and re-lock later */
		if (lock_type != -1)
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		XactLockTableWait(xid, NULL, NULL, XLTW_None);
		if (lock_type != -1)
		{
			TransactionId new_xid = xid;

			LockBuffer(buffer, lock_type);

			if (is_xmin)
			{
				new_xid = HeapTupleHeaderGetRawXmin(tuple);
				if (!TransactionIdEquals(xid, new_xid))
				{
					*need_retry = true;
					return false;
				}
				}
				else
				{
					new_xid = HeapTupleHeaderGetRawXmax(tuple);
					if (!TransactionIdEquals(xid, new_xid) || xmax_infomask_changed(tuple->t_infomask, old_infomask))
					{
						*need_retry = true;
						return false;
					}
				}
				/* Avoid deadlock */
				if (TransactionIdDidAbort(xid))
				{
					DEBUG_VISIBILE(elog(LOG, "abort snapshot ts " INT64_FORMAT
						"false xid %d .", snapshot->start_ts, xid));
					if (enable_distri_debug)
					{
						snapshot->scanned_tuples_after_abort++;
					}

				*need_retry = false;
				return false;
			}
		}

		committs = TransactionIdGetCSN(xid);

		if (!(CSN_IS_COMMITTED(committs) || CSN_IS_ABORTED(committs)))
			abort();
	}

	if (CSN_IS_COMMITTED(committs))
	{
		*hintstatus = XID_COMMITTED;

		if (enable_distri_debug)
		{
			snapshot->scanned_tuples_after_committed++;
		}

		if (snapshot->start_ts >= committs)
		{
			SnapshotCheck(xid, snapshot, false, committs);

			DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT
							", xid %d commit_ts " INT64_FORMAT " 1.",
			snapshot->start_ts, xid, committs));
			return true;
		}
		else
		{
			SnapshotCheck(xid, snapshot, true, committs);
			DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT
							", xid %d committs" INT64_FORMAT " 2.", snapshot->start_ts,
							xid, committs));
			return false;
		}
	}
	else if (CSN_IS_ABORTED(committs))
	{
		if (enable_distri_debug)
		{
			snapshot->scanned_tuples_after_abort++;
		}
		SnapshotCheck(xid, snapshot, false, 0);

		DEBUG_VISIBILE(elog(LOG, "abort snapshot ts " INT64_FORMAT " false"
						" xid %d .", snapshot->start_ts, xid));
		*hintstatus = XID_ABORTED;
		return false;
	}

	if (enable_distri_debug)
	{
		snapshot->scanned_tuples_before_prepare++;
	}

	/*
	 * For non-prepared transaction, its commit timestamp must be larger than
	 * the current running transaction/statement's start timestamp. This is
	 * because that as T1's commit timestamp has not yet been aquired on CN,
	 * T2.start < T1.commit is always being held.
	 */
	SnapshotCheck(xid, snapshot, true, 0);

	DEBUG_VISIBILE(elog(LOG, "snapshot ts " INT64_FORMAT " true xid %d 5.",
					snapshot->start_ts, xid));
	return false;
}

#endif

/*
 * Is the tuple really only locked?  That is, is it not updated?
 *
 * It's easy to check just infomask bits if the locker is not a multi; but
 * otherwise we need to verify that the updating transaction has not aborted.
 *
 * This function is here because it follows the same time qualification rules
 * laid out at the top of this file.
 */
bool
HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple)
{
	TransactionId xmax;
	TransactionIdStatus	xidstatus;

	/* if there's no valid Xmax, then there's obviously no update either */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;

	if (tuple->t_infomask & HEAP_XMAX_LOCK_ONLY)
		return true;

	/* invalid xmax means no update */
	if (!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	/*
	 * if HEAP_XMAX_LOCK_ONLY is not set and not a multi, then this must
	 * necessarily have been updated
	 */
	if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI))
		return false;

	/* ... but if it's a multi, then perhaps the updating Xid aborted. */
	xmax = HeapTupleGetUpdateXid(tuple);

	/* not LOCKED_ONLY, so it has to have an xmax */
	Assert(TransactionIdIsValid(xmax));

	if (TransactionIdIsCurrentTransactionId(xmax))
		return false;

	xidstatus = TransactionIdGetStatusCSN(xmax);
	if (xidstatus == XID_INPROGRESS)
		return false;
	if (xidstatus == XID_COMMITTED)
		return false;

	/*
	 * not current, not in progress, not committed -- must have aborted or
	 * crashed
	 */
	return true;
}

/*
 * check whether the transaction id 'xid' is in the pre-sorted array 'xip'.
 */
static bool
TransactionIdInArray(TransactionId xid, TransactionId *xip, Size num)
{
	return bsearch(&xid, xip, num,
				   sizeof(TransactionId), xidComparator) != NULL;
}

/*
 * See the comments for HeapTupleSatisfiesMVCC for the semantics this function
 * obeys.
 *
 * Only usable on tuples from catalog tables!
 *
 * We don't set any hint bits in here as it seems unlikely to be beneficial as
 * those should already be set by normal access and it seems to be too
 * dangerous to do so as the semantics of doing so during timetravel are more
 * complicated than when dealing "only" with the present.
 */
bool
HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot,
							   Buffer buffer)
{
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

	Assert(ItemPointerIsValid(&htup->t_self));
	Assert(htup->t_tableOid != InvalidOid);

	/* inserting transaction aborted */
	if (HeapTupleHeaderXminInvalid(tuple))
	{
		Assert(!TransactionIdDidCommit(xmin));
		return false;
	}
	/* check if it's one of our txids, toplevel is also in there */
	else if (TransactionIdInArray(xmin, snapshot->subxip, snapshot->subxcnt))
	{
		bool		resolved;
		CommandId	cmin = HeapTupleHeaderGetRawCommandId(tuple);
		CommandId	cmax = InvalidCommandId;

		/*
		 * another transaction might have (tried to) delete this tuple or
		 * cmin/cmax was stored in a combocid. So we need to lookup the actual
		 * values externally.
		 */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
												 htup, buffer,
												 &cmin, &cmax);

		if (!resolved)
			elog(ERROR, "could not resolve cmin/cmax of catalog tuple");

		Assert(cmin != InvalidCommandId);

		if (cmin >= snapshot->curcid)
			return false;		/* inserted after scan started */
		/* fall through */
	}
	/* committed before our xmin horizon. Do a normal visibility check. */
	else if (TransactionIdPrecedes(xmin, snapshot->xmin))
	{
		Assert(!(HeapTupleHeaderXminCommitted(tuple) &&
				 !TransactionIdDidCommit(xmin)));

		/* check for hint bit first, consult clog afterwards */
		if (!HeapTupleHeaderXminCommitted(tuple) &&
			!TransactionIdDidCommit(xmin))
			return false;
		/* fall through */
	}
	/* beyond our xmax horizon, i.e. invisible */
	else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax))
	{
		return false;
	}
	/* check if it's a committed transaction in [xmin, xmax) */
	else if (TransactionIdInArray(xmin, snapshot->xip, snapshot->xcnt))
	{
		/* fall through */
	}

	/*
	 * none of the above, i.e. between [xmin, xmax) but hasn't committed. I.e.
	 * invisible.
	 */
	else
	{
		return false;
	}

	/* at this point we know xmin is visible, go on to check xmax */

	/* xid invalid or aborted */
	if (tuple->t_infomask & HEAP_XMAX_INVALID)
		return true;
	/* locked tuples are always visible */
	else if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
		return true;

	/*
	 * We can see multis here if we're looking at user tables or if somebody
	 * SELECT ... FOR SHARE/UPDATE a system table.
	 */
	else if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		xmax = HeapTupleGetUpdateXid(tuple);
	}

	/* check if it's one of our txids, toplevel is also in there */
	if (TransactionIdInArray(xmax, snapshot->subxip, snapshot->subxcnt))
	{
		bool		resolved;
		CommandId	cmin;
		CommandId	cmax = HeapTupleHeaderGetRawCommandId(tuple);

		/* Lookup actual cmin/cmax values */
		resolved = ResolveCminCmaxDuringDecoding(HistoricSnapshotGetTupleCids(), snapshot,
												 htup, buffer,
												 &cmin, &cmax);

		if (!resolved)
			elog(ERROR, "could not resolve combocid to cmax");

		Assert(cmax != InvalidCommandId);

		if (cmax >= snapshot->curcid)
			return true;		/* deleted after scan started */
		else
			return false;		/* deleted before scan started */
	}
	/* below xmin horizon, normal transaction state is valid */
	else if (TransactionIdPrecedes(xmax, snapshot->xmin))
	{
		Assert(!(tuple->t_infomask & HEAP_XMAX_COMMITTED &&
				 !TransactionIdDidCommit(xmax)));

		/* check hint bit first */
		if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
			return false;

		/* check clog */
		return !TransactionIdDidCommit(xmax);
	}
	/* above xmax horizon, we cannot possibly see the deleting transaction */
	else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
		return true;
	/* xmax is between [xmin, xmax), check known committed array */
	else if (TransactionIdInArray(xmax, snapshot->xip, snapshot->xcnt))
		return false;
	/* xmax is between [xmin, xmax), but known not to have committed yet */
	else
		return true;
}

bool
IsShardClusterInvisible(Relation rel, Snapshot snap, ShardClusterId sc_id)
{
	return (IS_PGXC_DATANODE && IsSnapshotNeedCheckScVisibility(snap) &&
			RelationIsSharded(rel) && SnapshotGetShardClusterTable(snap) &&
			g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL &&
			!bms_is_member(sc_id, SnapshotGetShardClusterTable(snap)));
}

#if 0
#ifdef _MIGRATE_
/*
 * HeapTupleSatisfiesNow
 *		True iff heap tuple is valid "now".
 *
 *	Here, we consider the effects of:
 *		all committed transactions (as of the current instant)
 *		previous commands of this transaction
 *
 * Note we do _not_ include changes made by the current command.  This
 * solves the "Halloween problem" wherein an UPDATE might try to re-update
 * its own output tuples, http://en.wikipedia.org/wiki/Halloween_Problem.
 *
 * Note:
 *		Assumes heap tuple is valid.
 *
 * The satisfaction of "now" requires the following:
 *
 * ((Xmin == my-transaction &&				inserted by the current transaction
 *	 Cmin < my-command &&					before this command, and
 *	 (Xmax is null ||						the row has not been deleted, or
 *	  (Xmax == my-transaction &&			it was deleted by the current transaction
 *	   Cmax >= my-command)))				but not before this command,
 * ||										or
 *	(Xmin is committed &&					the row was inserted by a committed transaction, and
 *		(Xmax is null ||					the row has not been deleted, or
 *		 (Xmax == my-transaction &&			the row is being deleted by this transaction
 *		  Cmax >= my-command) ||			but it's not deleted "yet", or
 *		 (Xmax != my-transaction &&			the row was deleted by another transaction
 *		  Xmax is not committed))))			that has not been committed
 *
 */
bool
HeapTupleSatisfiesNow(HeapTupleHeader tuple, Snapshot snapshot, Buffer buffer)
{
//#ifdef _PG_SHARDING_
	if (IS_PGXC_DATANODE && tuple->t_shardid >= 0 && SnapshotGetShardTable(snapshot))
	{
		bool shard_is_visible = bms_is_member(tuple->t_shardid/snapshot->groupsize,
												SnapshotGetShardTable(snapshot));

		if (!IsConnFromApp())
		{
			if (!shard_is_visible)
				return false;
		}
		else if (g_ShardVisibleMode != SHARD_VISIBLE_MODE_ALL)
		{
			if ((!shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_VISIBLE)
				|| (shard_is_visible && g_ShardVisibleMode == SHARD_VISIBLE_MODE_HIDDEN))
			{
				return false;
			}
		}
	}
//#endif

	if (!(tuple->t_infomask & HEAP_XMIN_COMMITTED))
	{
		if (tuple->t_infomask & HEAP_XMIN_INVALID)
			return false;

		if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple)))
		{
			if (HeapTupleHeaderGetCmin(tuple) >= GetCurrentCommandId(false))
				return false;	/* inserted after scan started */

			if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid */
				return true;

			if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))	/* not deleter */
				return true;

			if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			{
				TransactionId xmax;

				xmax = HeapTupleGetUpdateXid(tuple);
				if (!TransactionIdIsValid(xmax))
					return true;

				/* updating subtransaction must have aborted */
				if (!TransactionIdIsCurrentTransactionId(xmax))
					return true;
				else
					return false;
			}

			if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
			{
				/* deleting subtransaction must have aborted */
				SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
							InvalidTransactionId);
				return true;
			}

			if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (TransactionIdIsInProgress(HeapTupleHeaderGetXmin(tuple)))
			return false;
		else if (TransactionIdDidCommit(HeapTupleHeaderGetXmin(tuple)))
			SetHintBits(tuple, buffer, HEAP_XMIN_COMMITTED,
						HeapTupleHeaderGetXmin(tuple));
		else
		{
			/* it must have aborted or crashed */
			SetHintBits(tuple, buffer, HEAP_XMIN_INVALID,
						InvalidTransactionId);
			return false;
		}
	}

	/* by here, the inserting transaction has committed */

	if (tuple->t_infomask & HEAP_XMAX_INVALID)	/* xid invalid or aborted */
		return true;

	if (tuple->t_infomask & HEAP_XMAX_COMMITTED)
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		return false;
	}

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId xmax;

		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;

		xmax = HeapTupleGetUpdateXid(tuple);
		if (!TransactionIdIsValid(xmax))
			return true;
		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		if (TransactionIdIsInProgress(xmax))
			return true;
		if (TransactionIdDidCommit(xmax))
			return false;
		return true;
	}

	if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmax(tuple)))
	{
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return true;
		if (HeapTupleHeaderGetCmax(tuple) >= GetCurrentCommandId(false))
			return true;		/* deleted after scan started */
		else
			return false;		/* deleted before scan started */
	}

	if (TransactionIdIsInProgress(HeapTupleHeaderGetRawXmax(tuple)))
		return true;

	if (!TransactionIdDidCommit(HeapTupleHeaderGetRawXmax(tuple)))
	{
		/* it must have aborted or crashed */
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	/* xmax transaction committed */

	if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		SetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
					InvalidTransactionId);
		return true;
	}

	SetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
				HeapTupleHeaderGetRawXmax(tuple));
	return false;
}
#endif

#endif
