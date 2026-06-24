/*-------------------------------------------------------------------------
 *
 * fnfreelist.c
 *	  routines for managing the forward node buffer pool's get and flushing
 *	  strategy. It is very simple for now, only a freelist like normal
 *	  disk buffer. When the pool is full, link the PGPROC waiting for a
 *	  free buffer into a share-memory queue, and wake them one by one when
 *	  there is free pages, it behavior like lock manager though.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * IDENTIFICATION
 *	  src/backend/forward/storage/fnfreelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"

#include "executor/execDispatchFragment.h"
#include "forward/fnbufmgr.h"
#include "miscadmin.h"

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

#define MAX_NUM_FREELIST (8) /* must be a power of 2 */

/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	int			firstFreeBuffer;	/* Head of list of unused buffers */
	int			lastFreeBuffer;     /* Tail of list of unused buffers */
	/*
	 * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
	 * when the list is empty)
	 */

	/*
	 * Forward sender process to be notified upon activity or -1 if none. See
	 * FnStrategyNotifySender, useless in receiver.
	 */
	int			bgwprocno;
} FnBufferStrategyControl;

/* Pointers to shared state */
static FnBufferStrategyControl *FnStrategyControl[FNPAGE_MAX][MAX_NUM_FREELIST] = {};

/*
 * FnStrategyGetBuffer
 *
 *	Called by the fnbufmgr to get the next candidate buffer to use in
 *	FnBufferAlloc(). The only hard requirement FnBufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
FnBufferDesc *
FnStrategyGetBuffer(fn_page_kind kind, FNQueryId queryid)
{
	bool        waiting = false;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */
	FnBufferStrategyControl *control;
	FnBufferDesc *buf;
	int			control_num = (int)queryid.sequence & (MAX_NUM_FREELIST - 1);
	int			try_next = 0;

next_control:
	control = FnStrategyControl[kind][control_num];

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 * 
	 * If there's no buffer available, wait forever.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 */
	while (true)
	{
		CHECK_FOR_INTERRUPTS();

		/* Acquire the spinlock to remove element from the freelist */
		SpinLockAcquireNoPanic(&control->buffer_strategy_lock);

		if (unlikely(control->firstFreeBuffer < 0))
		{
			/* Nothing on the freelist, prepare waiting on empty buffer */
			SpinLockRelease(&control->buffer_strategy_lock);

			if (++try_next < MAX_NUM_FREELIST)
			{
				control_num = (control_num + 1) % MAX_NUM_FREELIST;
				goto next_control;
			}

			if (!waiting)
				pgstat_report_wait_start(WAIT_EVENT_ALLOCATE_FNPAGE);

			/* set flag to avoid insert myself twice */
			waiting = true;
			try_next = 0;
			pg_usleep(1000L);
			continue;
		}

		if (unlikely(waiting))
			pgstat_report_wait_end();
		waiting = false;

		buf = GetFnBufferDescriptor(control->firstFreeBuffer, kind);
		Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

		/* Unconditionally remove buffer from freelist */
		control->firstFreeBuffer = buf->freeNext;
		buf->freeNext = FREENEXT_NOT_IN_LIST;

		/*
		 * Release the lock so someone else can access the freelist while
		 * we check out this buffer.
		 */
		SpinLockRelease(&control->buffer_strategy_lock);

		/*
		 * If the buffer is pinned or has a nonzero usage_count, we cannot
		 * use it; discard it and retry.  (This can only happen if SYNC
		 * put a valid buffer in the freelist and then someone else used
		 * it before we got to it.  It's probably impossible altogether as
		 * of 8.3, but we'd better check anyway.)
		 */
		local_buf_state = LockFnBufHdr(buf);
		if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
			return buf;	/* return with hdr lock */
		UnlockFnBufHdr(buf, local_buf_state);
	}

	/* should not be here */
	Assert(0);
	return NULL;
}

/*
 * FnStrategyFreeBuffer
 *
 *  Put a buffer on the freelist, then wake up next PGPROC
 *  that waiting for a free buffr.
 */
void
FnStrategyFreeBuffer(FnBufferDesc *buf)
{
	FnBufferStrategyControl *control;
	int control_id = ((buf->buf_id * MAX_NUM_FREELIST) / FnGetNBuffers(buf->kind));

	buf->next = -1;

	if (buf->kind != FNPAGE_RECV)
	{
		/*
		 * Clear the dirty and other flags, no one can touch this buffer now,
		 * so don't need lock
		 */
		uint32 buf_state = pg_atomic_read_u32(&buf->state);
		buf_state &= ~(BM_DIRTY | BM_FLUSH);
		pg_atomic_unlocked_write_u32(&buf->state, buf_state);
	}

	control = FnStrategyControl[buf->kind][control_id];
	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		/*
		 * There is a window period before the next allocation to this page, we
		 * first set freeNext to FREENEXT_NOT_IN_LIST, and then set queryid to
		 * the new one. Therefore, before releasing the page, it is necessary
		 * to clear queryid here to prevent fn_stat_clear_page from mistakenly
		 * clearing this page during this period.
		 */
		FnPageHeader header = (FnPageHeader) FnBufferDescriptorGetPage(buf);

		header->queryid.timestamp_nodeid = 0;
		header->queryid.sequence = 0;

		SpinLockAcquireNoPanic(&control->buffer_strategy_lock);

		buf->freeNext = control->firstFreeBuffer;
		if (buf->freeNext < 0)
			control->lastFreeBuffer = buf->buf_id;
		control->firstFreeBuffer = buf->buf_id;

		SpinLockRelease(&control->buffer_strategy_lock);
	}
}

/*
 * FnStrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 */
Size
FnStrategyShmemSize(void)
{
	Size		size = 0;

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(FnBufferStrategyControl)));
	/* multiple freelist */
	size = mul_size(size, MAX_NUM_FREELIST);
	/* for sender, receiver and local */
	return mul_size(size, FNPAGE_MAX);
}

/*
 * FnStrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
FnStrategyInitialize(void)
{
	bool			found;
	fn_page_kind	kind;
	int				i, first, last, page_per_list;

	for (kind = FNPAGE_SEND; kind < FNPAGE_MAX; kind++)
	{
		page_per_list = FnGetNBuffers(kind) / MAX_NUM_FREELIST;

		for (i = 0; i < MAX_NUM_FREELIST; i++)
		{
			char name[NAMEDATALEN];
			FnBufferStrategyControl *control;
			FnBufferDesc *buf;

			sprintf(name, "FnBuffer Strategy Status %d-%d", kind, i);
			FnStrategyControl[kind][i] =
				ShmemInitStruct(name, sizeof(FnBufferStrategyControl), &found);
			control = FnStrategyControl[kind][i];

			first = i * page_per_list;
			last = first + page_per_list - 1;
			/* GUC must make sure that last bigger than first */
			Assert(last > first);

			if (!found)
			{
				/*
				 * Only done once, usually in postmaster
				 */
				SpinLockInit(&control->buffer_strategy_lock);

				/*
				 * Grab the whole linked list of free buffers for our strategy. We
				 * assume it was previously set up by InitBufferPool().
				 */
				control->firstFreeBuffer = first;
				control->lastFreeBuffer = last;
			}

			/* Correct last entry of linked list */
			buf = GetFnBufferDescriptor(last, kind);
			buf->freeNext = FREENEXT_END_OF_LIST;
		}
	}
}

/*
 * FnStrategyNotifySender -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of FnStrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the fnsender process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
FnStrategyNotifySender(int bgwprocno)
{
	int i;

	for (i = 0; i < MAX_NUM_FREELIST; i++)
	{
		FnBufferStrategyControl *control = FnStrategyControl[FNPAGE_SEND][i];
		control->bgwprocno = bgwprocno;
	}
}

void
FnStrategyWakeSender(int buf_id)
{
	int bgwprocno;
	int control_id = ((buf_id * MAX_NUM_FREELIST) / FnGetNBuffers(FNPAGE_SEND));
	FnBufferStrategyControl *control;

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	control = FnStrategyControl[FNPAGE_SEND][control_id];
	bgwprocno = INT_ACCESS_ONCE(control->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		control->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}
}

/*
 * FnStrategyNumUsed -- get the number of buffers not in freelist.
 */
int
FnStrategyNumUsed(fn_page_kind kind, int64 ts_node, int64 seq, uint16 fid)
{
	int		i, nbuf = FnGetNBuffers(kind);
	int		ret = 0;
	FnBufferDesc *desc = GetFnBufferDescriptor(0, kind);

	/*
	 * We currently don't bother with acquiring mutexes or locks; it's only
	 * sensible to call this function if you've got lock already.
	 */
	for (i = 0; i < nbuf; i++)
	{
		if (desc[i].freeNext == FREENEXT_NOT_IN_LIST)
		{
			FnPageHeader head;

			/* not specific query and fragment, count it */
			if (ts_node == 0 && seq == 0 && fid == 0)
				ret++;

			head = (FnPageHeader)FnBufferDescriptorGetPage(desc);
			if (head->queryid.timestamp_nodeid == ts_node &&
				head->queryid.sequence == seq && head->fid == fid)
				ret++;
		}
	}

	return ret;
}
