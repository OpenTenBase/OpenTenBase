/*
 * fnbufmgr.c
 *	 The forward node(FN) buffer management wrapper. It provides the 
 *	 internal interface to allocate and free buffer dynamically.
 *
 * Copyright (c) 1996-2022, OpenTenBase Development Group
 *
 * IDENTIFICATION
 *	  src/backend/forward/storage/fnbufmgr.c
 */
#include "postgres.h"

#include "access/parallel.h"
#include "executor/execFragment.h"
#include "forward/fnbufmgr.h"
#include "forward/fnconn.h"
#include "pgxc/groupmgr.h"
#include "postmaster/forward.h"
#include "storage/spin.h"
#include "utils/resowner_private.h"
#include "miscadmin.h"

/*
 * Lock buffer header - set BM_LOCKED in buffer state.
 */
uint32
LockFnBufHdr(FnBufferDesc *desc)
{
	SpinDelayStatus delayStatus;
	uint32		old_buf_state;
	
	init_local_spin_delay(&delayStatus);
	
	while (true)
	{
		/* set BM_LOCKED flag */
		old_buf_state = pg_atomic_fetch_or_u32(&desc->state, BM_LOCKED);
		/* if it wasn't set before we're OK */
		if (!(old_buf_state & BM_LOCKED))
			break;
		perform_spin_delay_nopanic(&delayStatus);
	}
	finish_spin_delay(&delayStatus);
	return old_buf_state | BM_LOCKED;
}

/*
 * Wait until the BM_LOCKED flag isn't set anymore and return the buffer's
 * state at that point.
 *
 * Obviously the buffer could be locked by the time the value is returned, so
 * this is primarily useful in CAS style loops.
 */
static uint32
WaitBufHdrUnlocked(FnBufferDesc *buf)
{
	SpinDelayStatus delayStatus;
	uint32		buf_state;

	init_local_spin_delay(&delayStatus);

	buf_state = pg_atomic_read_u32(&buf->state);

	while (buf_state & BM_LOCKED)
	{
		perform_spin_delay_nopanic(&delayStatus);
		buf_state = pg_atomic_read_u32(&buf->state);
	}

	finish_spin_delay(&delayStatus);

	return buf_state;
}

/*
 * PinFnBuffer -- increment fn buffer reference count
 *
 * Since buffers are pinned/unpinned very frequently, pin buffers without
 * taking the buffer header lock; instead update the state variable in loop of
 * CAS operations. Hopefully it's just a single CAS.
 */
void
PinFnBuffer(FnBufferDesc *buf)
{
	uint32		buf_state;
	uint32		old_buf_state;

	old_buf_state = pg_atomic_read_u32(&buf->state);
	for (;;)
	{
		if (old_buf_state & BM_LOCKED)
			old_buf_state = WaitBufHdrUnlocked(buf);

		buf_state = old_buf_state;

		/* increase refcount */
		buf_state += BUF_REFCOUNT_ONE;

		if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
										   buf_state))
			break;
	}
}

/*
 * PinFnBuffer_Locked -- as above, but caller already locked the buffer header.
 * The spinlock is released before return.
 *
 * Note: use of this routine is frequently mandatory, not just an optimization
 * to save a spin lock/unlock cycle, because we need to pin a buffer before
 * its state can change under us.
 */
static void
PinFnBuffer_Locked(FnBufferDesc *buf)
{
	uint32		buf_state;

	/*
	 * Since we hold the buffer spinlock, we can update the buffer state and
	 * release the lock in one operation.
	 */
	buf_state = pg_atomic_read_u32(&buf->state);
	Assert(buf_state & BM_LOCKED);
	buf_state += BUF_REFCOUNT_ONE;
	UnlockFnBufHdr(buf, buf_state);
}

/*
 * UnpinFnBuffer -- make buffer available for replacement.
 *
 * Most but not all callers want CurrentResourceOwner to be adjusted.
 * Those that don't should pass fixOwner = FALSE.
 */
static int
UnpinFnBuffer(FnBufferDesc *buf)
{
	uint32		buf_state;
	uint32		old_buf_state;

	/*
	 * Decrement the shared reference count.
	 *
	 * Since buffer spinlock holder can update status using just write,
	 * it's not safe to use atomic decrement here; thus use a CAS loop.
	 */
	old_buf_state = pg_atomic_read_u32(&buf->state);
	for (;;)
	{
		if (old_buf_state & BM_LOCKED)
			old_buf_state = WaitBufHdrUnlocked(buf);

		/*
		 * if old_buf_state become 0, someone else also unpin this buffer,
		 * return non-zero to prevent from freeing it again
		 */
		if ((old_buf_state & BUF_REFCOUNT_MASK) == 0)
			return 1;

		buf_state = old_buf_state;

		/* decrement refcount */
		buf_state -= BUF_REFCOUNT_ONE;

		if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
										   buf_state))
			break;
	}

	return BUF_STATE_GET_REFCOUNT(buf_state);
}

/*
 * FnBufferAlloc -- Handles lookup of a shared fn buffer.  If no buffer
 *                  exists already, selects one from freelist.
 *
 * The returned buffer is pinned and is already marked as holding the
 * desired page.
 *
 * No locks are held either at entry or exit.
 */
FnBufferDesc *
FnBufferAlloc(fn_page_kind kind, FNQueryId queryid)
{
	FnBufferDesc   *buf;

	/*
	 * Select a victim buffer.  The buffer is returned with its header
	 * spinlock still held!
	 */
	buf = FnStrategyGetBuffer(kind, queryid);

	/* Pin the buffer and then release the buffer spinlock */
	PinFnBuffer_Locked(buf);

	/*
	 * Okay, buffer acquire succeed.
	 */
	return buf;
}

void
FnBufferFree(FnBufferDesc *buf)
{
	if (UnpinFnBuffer(buf) == 0)
		FnStrategyFreeBuffer(buf);
	else
		Assert(buf->kind == FNPAGE_SEND);
}

/*
 * FlushFnBuffer
 *		Physically write out a shared buffer to net socket.
 *
 * NOTE: this actually just passes the buffer contents to the kernel; the
 * real write to socket won't happen until the kernel feels like it.
 *
 * The caller must hold a pin on the buffer and have share-locked the
 * buffer contents.  (Note: a share-lock does not prevent updates of
 * hint bits in the buffer, so the page could change while the write
 * is in progress, but we assume that that will not invalidate the data
 * written.)
 */
static bool
FlushFnBuffer(FnBufferDesc *buf, bool use_multithread)
{
	uint32      buf_state;
	FnPage      page;
	bool        result = false;
	FnPageHeader head;

	/* only sender buffer need flush */
	page = FnBufferDescriptorGetPage(buf);
	head = (FnPageHeader) page;

	if (head->flag & FNPAGE_END)
		DEBUG_FRAG(elog(LOG, "FlushFnBuffer: send %d EOF to %d. "
							 "qid_ts_node:%ld, qid_seq:%ld, "
							 "fid:%d, node:%d, worker:%d",
						buf->buf_id, buf->dst_node_id,
						head->queryid.timestamp_nodeid,
						head->queryid.sequence, head->fid,
						head->nodeid, head->workerid));

	if (FnPageIsEmpty(page))
	{
		FnPageReset(page);

		buf_state = LockFnBufHdr(buf);
		/* clear the dirty and other flags */
		buf_state &= ~(BM_DIRTY | BM_FLUSH);
		UnlockFnBufHdr(buf, buf_state);

		return true;
	}

	if (buf->broadcast_node_num > 0)
	{
		int	nodes[OPENTENBASE_MAX_DATANODE_NUMBER];
		int	i, num;
		pg_atomic_uint32 *num_to_send = NULL;

		num = GetGroupNodeListByIndex(buf->node_group_id, (int *) &nodes);

		if (use_multithread)
		{
			num_to_send = malloc(sizeof(pg_atomic_uint32));
			pg_atomic_init_u32(num_to_send, num);
		}

		for (i = 0; i < num; i++)
		{
			result = SendFnPage(page, nodes[i], buf, num_to_send);

			if (!result)
			{
				/* if failed, terminate all other backend and return */
				TermFidBackendProc("tcp fails to send data");
				return false;
			}
		}

		if (!use_multithread)
		{
			FnPageReset(page);

			buf_state = LockFnBufHdr(buf);
			/* clear the dirty and other flags */
			buf_state &= ~(BM_DIRTY | BM_FLUSH);
			UnlockFnBufHdr(buf, buf_state);
		}

		DEBUG_FRAG(elog(DEBUG1, "FlushFnBuffer: broadcast %d. qid_ts_node:%ld, qid_seq:%ld, "
							 "fid:%d, node:%d, worker:%d, node_groupid:%d, nodenum:%d",
						buf->buf_id, head->queryid.timestamp_nodeid,
						head->queryid.sequence, head->fid, head->nodeid,
						head->workerid, buf->node_group_id, num));

		return true;
	}
	else
	{
		result = SendFnPage(page, buf->dst_node_id, buf, NULL);

		if (result)
		{
			if (!use_multithread)
			{
				FnPageReset(page);

				buf_state = LockFnBufHdr(buf);
				/* clear the dirty and other flags */
				buf_state &= ~(BM_DIRTY | BM_FLUSH);
				UnlockFnBufHdr(buf, buf_state);
			}

			DEBUG_FRAG(elog(DEBUG1, "FlushFnBuffer: send %d to %d. "
									"qid_ts_node:%ld, qid_seq:%ld, "
									"fid:%d, node:%d, worker:%d",
							buf->buf_id, buf->dst_node_id,
							head->queryid.timestamp_nodeid,
							head->queryid.sequence, head->fid,
							head->nodeid, head->workerid));
		}
		else
		{
			/* if failed, terminate all other backend and return */
			TermFidBackendProc("tcp fails to send data");
		}

		return result;
	}

	Assert(0); /* keep compiler happy */
	return result;
}

/*
 * FnBufferSync -- Write out some dirty buffers in the pool.
 *
 * This is called periodically by the forward sender process.
 *
 * Returns true if it's appropriate for the fn sender process to go into
 * low-power hibernation mode. This happens if no one touch the freelist.
 */
bool
FnBufferSync(void)
{
	bool		use_multithread = fn_send_thread_num >= 0;

	for (;;)
	{
		bool	res;
		int 	buf_id = fn_desc_dequeue(ForwardSenderQueue);
		FnBufferDesc *buf;

		if (buf_id < 0)
			return true;

		buf = GetFnBufferDescriptor(buf_id, FNPAGE_SEND);
		res = FlushFnBuffer(buf, use_multithread);
		if (!use_multithread)
			FnBufferFree(buf);

		if (unlikely(!res))
			return false;

		if (forward_is_got_sigusr1())
			return false;		
	}
}

/*
 * MarkFnBufferDirty
 *
 *		Marks buffer contents as dirty (actual write happens later).
 *		Also consider mark buffer as other flag.
 *
 * Buffer must be pinned and exclusive-locked.  (If caller does not hold
 * exclusive lock, then somebody could be in process of writing the buffer,
 * leading to re-send of data.)
 */
void
MarkFnBufferDirty(FnBufferDesc *buf, uint32 flag)
{
	uint32		buf_state;
	uint32		old_buf_state;

	Assert(LWLockHeldByMeInMode(FnBufferDescriptorGetContentLock(buf),
								LW_EXCLUSIVE));

	old_buf_state = pg_atomic_read_u32(&buf->state);
	for (;;)
	{
		if (old_buf_state & BM_LOCKED)
			old_buf_state = WaitBufHdrUnlocked(buf);

		buf_state = old_buf_state;

		Assert(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
		buf_state |= BM_DIRTY;
		buf_state |= flag;

		if (pg_atomic_compare_exchange_u32(&buf->state, &old_buf_state,
										   buf_state))
			break;
	}
}

/*
 * Helper routine to issue warnings when a buffer is unexpectedly pinned
 */
void
PrintFnBufferLeakWarning(FnBuffer buffer)
{
	FnBufferDesc	*buf;
	uint32			 buf_state;
	FnPageHeader	 head;

	Assert(!FnBufferIsInvalid(buffer));
	buf = GetFnBufferDescriptor(buffer - 1, FNPAGE_SEND);
	head = (FnPageHeader) FnBufferDescriptorGetPage(buf);

	/* theoretically we should lock the bufhdr here */
	buf_state = pg_atomic_read_u32(&buf->state);
	elog(WARNING,
		 "fn sender buffer refcount leak: [%03d] "
		 "(qid_ts_node=%ld, qid_seq=%ld, fid=%d, worker=%d, nodeid=%d, refcount=%d)",
		 buffer, head->queryid.timestamp_nodeid, head->queryid.sequence,
		 head->fid, head->workerid, head->nodeid,
		 BUF_STATE_GET_REFCOUNT(buf_state));
}

void
ClearFnSndQueueEntry(FnSndQueueEntry *entry)
{
	DEBUG_FRAG(elog(LOG, "ClearFnSndQueueEntry: clean entry of "
						 "qid_ts_node:%ld, qid_seq:%ld, fid:%d, node:%d, worker:%d",
					entry->key.queryid.timestamp_nodeid, entry->key.queryid.sequence,
					entry->key.fid, PGXCNodeId - 1, entry->key.workerid));

	if (entry->owner != MyProcPid)
	{
		elog(WARNING, "ClearFnSndQueueEntry attempts to clear the "
					  "entry that does not belong to itself, owner %d", entry->owner);
		return;
	}

	LWLockAcquire(FnSndBufferLock, LW_EXCLUSIVE);
	/* remove hash entry */
	entry = (FnSndQueueEntry *) hash_search(FnSndBufferHash,
											(const void *) &entry->key,
											HASH_REMOVE, NULL);
	Assert(entry != NULL);
	LWLockRelease(FnSndBufferLock);
}

void
ClearFnRcvQueueEntry(FnRcvQueueEntry *entry)
{
	int 	i;
	int 	nqueue;
	uint32	hashvalue;
	LWLock *partitionLock;

	/* we can access multiple queues in parallel leader even in dop mode */
	if (IsParallelWorker())
		return;

	if (entry->owner != MyProcPid)
	{
		elog(WARNING, "ClearFnRcvQueueEntry attempts to clear the "
					  "entry that does not belong to itself, owner %d", entry->owner);
		return;
	}

	nqueue = Max(1, entry->nqueue); /* at least we should check #0 */
	hashvalue = get_hash_value(FnRcvBufferHash, &entry->key);
	partitionLock = RecvBufHashPartitionLock(hashvalue);

	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	for (i = 0; i < nqueue; i++)
	{
		/* drain out the rest pages */
		while (!SHMQueueEmpty(&entry->queue[i]))
		{
			FnBufferDesc *buf;

			SpinLockAcquireNoPanic(&entry->mutex[i]);
			buf = (FnBufferDesc *)SHMQueueNext(&entry->queue[i], &entry->queue[i],
											   offsetof(FnBufferDesc, elem));
			if (buf != NULL)
			{
				SHMQueueDelete(&buf->elem);
				SpinLockRelease(&entry->mutex[i]);

				/* free the buffer */
				FnBufferFree(buf);
			}
			else
				SpinLockRelease(&entry->mutex[i]);
		}
	}

	if (enable_exec_fragment_critical_print)
		elog(LOG, "ClearFnRcvQueueEntry "
				  "qid_ts_node:%ld, qid_seq:%ld, "
				  "fid:%d, worker:%d",
			 entry->key.queryid.timestamp_nodeid,
			 entry->key.queryid.sequence,
			 entry->key.fid, entry->key.workerid);

	/* remove hash entry */
	entry = (FnRcvQueueEntry *) hash_search_with_hash_value(FnRcvBufferHash,
															(const void *) &entry->key,
															hashvalue,
															HASH_REMOVE, NULL);
	LWLockRelease(partitionLock);
}

/*
 * fn_desc_enqueue: put a buf_id into the queue, use a CAS loop to sync
 * between multiple producers (DProcesses), notice that we do not need
 * to consider the case where the queue is full. We allocate the size of
 * the queue to be FnSendNBuffers during initialization, which is enough
 * to hold all FN send buffers at the same time.
 */
void
fn_desc_enqueue(fn_desc_queue *q, int buf_id)
{
	uint32 old_tail;
	uint32 tail;

	pg_memory_barrier();
	old_tail = pg_atomic_read_u32(&q->tail);

	do
	{
		tail = old_tail + 1;
		pg_memory_barrier();
	} while (!pg_atomic_compare_exchange_u32(&q->tail, &old_tail, tail));

	q->queue[(tail - 1) % q->size] = buf_id;
}

int
fn_desc_dequeue(fn_desc_queue *q)
{
	uint32 head;
	uint32 tail;
	int buf_id;

	pg_memory_barrier();
	head = pg_atomic_read_u32(&q->head);
	pg_memory_barrier();
	tail = pg_atomic_read_u32(&q->tail);

	/*
	 * Since there is only one consumer, "head" is always pushed by one
	 * process, so we only need to consider the case where head catches up
	 * with tail, that is, when they are equal, so there is no need to
	 * consider uint32 wraparound.
	 */
	if (head == tail)
		return -1; /* the queue is empty */

	/*
	 * Remember that only one process (i.e., myself) can change the head,
	 * so it can be used very casually.
	 */
	head = head % q->size;
	if (q->queue[head] <= -1)
		return -1; /* the value has not been written yet. */

	pg_memory_barrier();
	head = pg_atomic_fetch_add_u32(&q->head, 1) % q->size;
	buf_id = q->queue[head];
	/* don't forget to reset the slot */
	q->queue[head] = -1;

	return buf_id;
}
