/*-------------------------------------------------------------------------
 *
 * local_mq.c
 *	  single-reader, single-writer local memory message queue
 *
 * Both the sender and the receiver must have a PGPROC; their respective
 * process latches are used for synchronization.  Only the sender may send,
 * and only the receiver may receive.  This is intended to allow a user
 * backend to communicate with worker backends that it has registered.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/local_mq.h
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"

#include "utils/resowner_private.h"
#include "executor/execDispatchFragment.h"
#include "storage/local_mq.h"
#include "pgxc/squeue.h"
#include "miscadmin.h"

/*
 * This structure represents the actual queue, stored in local memory.
 *
 * Some notes on synchronization:
 *
 * mq_receiver and mq_bytes_read can only be changed by the receiver; and
 * mq_sender and mq_bytes_written can only be changed by the sender.
 * mq_receiver and mq_sender are protected by mq_mutex, although, importantly,
 * they cannot change once set, and thus may be read without a lock once this
 * is known to be the case.
 *
 * mq_bytes_read and mq_bytes_written are not protected by the mutex.  Instead,
 * they are written atomically using 8 byte loads and stores.  Memory barriers
 * must be carefully used to synchronize reads and writes of these values with
 * reads and writes of the actual data in mq_ring.
 *
 * mq_detached needs no locking.  It can be set by either the sender or the
 * receiver, but only ever from false to true, so redundant writes don't
 * matter.  It is important that if we set mq_detached and then set the
 * counterparty's latch, the counterparty must be certain to see the change
 * after waking up.  Since SetLatch begins with a memory barrier and ResetLatch
 * ends with one, this should be OK.
 *
 * mq_ring_size and mq_ring_offset never change after initialization, and
 * can therefore be read without the lock.
 *
 * Importantly, mq_ring can be safely read and written without a lock.
 * At any given time, the difference between mq_bytes_read and
 * mq_bytes_written defines the number of bytes within mq_ring that contain
 * unread data, and mq_bytes_read defines the position where those bytes
 * begin.  The sender can increase the number of unread bytes at any time,
 * but only the receiver can give license to overwrite those bytes, by
 * incrementing mq_bytes_read.  Therefore, it's safe for the receiver to read
 * the unread bytes it knows to be present without the lock.  Conversely,
 * the sender can write to the unused portion of the ring buffer without
 * the lock, because nobody else can be reading or writing those bytes.  The
 * receiver could be making more bytes unused by incrementing mq_bytes_read,
 * but that's OK.  Note that it would be unsafe for the receiver to read any
 * data it's already marked as read, or to write any data; and it would be
 * unsafe for the sender to reread any data after incrementing
 * mq_bytes_written, but fortunately there's no need for any of that.
 */
struct local_mq
{
	ThreadSema       mq_sema;
	pg_atomic_uint64 mq_num_read;
	pg_atomic_uint64 mq_num_written;
	uint32		mq_ring_size;
	bool		mq_detached;
	bool		mq_force_detached;
	int			mq_efd;
	volatile  bool   mq_client_wait;
	uint64 		mq_remain;
	uint64 		mq_last_notify_write;
	uint64 		mq_wc;
	uint64 		mq_rc;
	void	  **mq_ring;
};

/*
 * This structure is a backend-private handle for access to a queue.
 *
 * mqh_queue is a pointer to the queue we've attached, and mqh_segment is
 * an optional pointer to the dynamic local memory segment that contains it.
 * (If mqh_segment is provided, we register an on_dsm_detach callback to
 * make sure we detach from the queue before detaching from DSM.)
 *
 * If this queue is intended to connect the current process with a background
 * worker that started it, the user can pass a pointer to the worker handle
 * to local_mq_attach(), and we'll store it in mqh_handle.  The point of this
 * is to allow us to begin sending to or receiving from that queue before the
 * process we'll be communicating with has even been started.  If it fails
 * to start, the handle will allow us to notice that and fail cleanly, rather
 * than waiting forever; see local_mq_wait_internal.  This is mostly useful in
 * simple cases - e.g. where there are just 2 processes communicating; in
 * more complex scenarios, every process may not have a BackgroundWorkerHandle
 * available, or may need to watch for the failure of more than one other
 * process at a time.
 *
 * When a message exists as a contiguous chunk of bytes in the queue - that is,
 * it is smaller than the size of the ring buffer and does not wrap around
 * the end - we return the message to the caller as a pointer into the buffer.
 * For messages that are larger or happen to wrap, we reassemble the message
 * locally by copying the chunks into a backend-local buffer.  mqh_buffer is
 * the buffer, and mqh_buflen is the number of bytes allocated for it.
 *
 * mqh_partial_bytes, mqh_expected_bytes, and mqh_length_word_complete
 * are used to track the state of non-blocking operations.  When the caller
 * attempts a non-blocking operation that returns LOCAL_MQ_WOULD_BLOCK, they
 * are expected to retry the call at a later time with the same argument;
 * we need to retain enough state to pick up where we left off.
 * mqh_length_word_complete tracks whether we are done sending or receiving
 * (whichever we're doing) the entire length word.  mqh_partial_bytes tracks
 * the number of bytes read or written for either the length word or the
 * message itself, and mqh_expected_bytes - which is used only for reads -
 * tracks the expected total size of the payload.
 *
 * mqh_counterparty_attached tracks whether we know the counterparty to have
 * attached to the queue at some previous point.  This lets us avoid some
 * mutex acquisitions.
 */
struct local_mq_handle
{
	int			 fid;
	local_mq	*mqh_queue;
};

static void local_mq_detach_internal(local_mq *mq, bool force);
static void local_mq_inc_read(local_mq *mq);
static void local_mq_inc_written(local_mq *mq);

#define MQH_INITIAL_BUFSIZE				8192

local_mq *
mqh_get_mq(local_mq_handle *mqh)
{
	return mqh->mqh_queue;
}

static void
notify_eventfd(local_mq *mq, uint64 count)
{
	if (mq->mq_efd == -1 || count == 0)
		return ;

	write(mq->mq_efd, &count, sizeof(uint64));
}

void
notify_write_num(local_mq *mq)
{
	uint64 num_written = pg_atomic_read_u64(&mq->mq_num_written);
	uint64 sendcount = num_written - mq->mq_last_notify_write;

	notify_eventfd(mq, sendcount);

	mq->mq_last_notify_write = num_written;
	mq->mq_wc += sendcount;
}

/*
 * Initialize a new local message queue.
 */
local_mq *
local_mq_create(uint32 size, int efd)
{
	local_mq	*mq = palloc(sizeof(local_mq));

	/* If the size isn't MAXALIGN'd, just discard the odd bytes. */
	size = MAXALIGN_DOWN(size);

	/* Initialize queue header. */
	ThreadSemaInit(&mq->mq_sema, 0, true);
	pg_atomic_init_u64(&mq->mq_num_read, 0);
	pg_atomic_init_u64(&mq->mq_num_written, 0);
	mq->mq_client_wait = false;
	mq->mq_ring_size = size;
	mq->mq_detached = false;
	mq->mq_force_detached = false;
	mq->mq_ring = palloc0(size * sizeof(void *));
	mq->mq_efd = efd;
	mq->mq_rc = 0;
	mq->mq_wc = 0;
	mq->mq_last_notify_write = 0;
	mq->mq_remain = 0;

	return mq;
}

/*
 * Attach to a local message queue so we can send or receive messages.
 *
 * The memory context in effect at the time this function is called should
 * be one which will last for at least as long as the message queue itself.
 * We'll allocate the handle in that context, and future allocations that
 * are needed to buffer incoming data will happen in that context as well.
 *
 * If seg != NULL, the queue will be automatically detached when that dynamic
 * local memory segment is detached.
 *
 * If handle != NULL, the queue can be read or written even before the
 * other process has attached.  We'll wait for it to do so if needed.  The
 * handle must be for a background worker initialized with bgw_notify_pid
 * equal to our PID.
 *
 * local_mq_detach() should be called when done.  This will free the
 * local_mq_handle and mark the queue itself as detached, so that our
 * counterpart won't get stuck waiting for us to fill or drain the queue
 * after we've already lost interest.
 */
local_mq_handle *
local_mq_attach(local_mq *mq, int fid)
{
	local_mq_handle *mqh = palloc(sizeof(local_mq_handle));

	mqh->mqh_queue = mq;
	mqh->fid = fid;

	return mqh;
}


/*
 * Write a message into a local message queue, gathered from multiple
 * addresses.
 *
 * When nowait = false, we'll wait on our process latch when the ring buffer
 * fills up, and then continue writing once the receiver has drained some data.
 * The process latch is reset after each wait.
 *
 * When nowait = true, we do not manipulate the state of the process latch;
 * instead, if the buffer becomes full, we return LOCAL_MQ_WOULD_BLOCK.  In
 * this case, the caller should call this function again, with the same
 * arguments, each time the process latch is set.  (Once begun, the sending
 * of a message cannot be aborted except by detaching from the queue; changing
 * the length or payload will corrupt the queue.)
 */
local_mq_result
local_mq_send(local_mq_handle *mqh, void *data, bool nowait)
{
	local_mq	*mq = mqh->mqh_queue;
	uint64		used;
	uint32		ringsize = mq->mq_ring_size;
	uint64		rn;
	uint64		wn;

retry:
	pg_read_barrier();
	/* Compute number of ring buffer bytes used. */
	rn = pg_atomic_read_u64(&mq->mq_num_read);
	wn = pg_atomic_read_u64(&mq->mq_num_written);
	Assert(wn >= rn);
	used = wn - rn;
	Assert(used <= ringsize);

	/*
	 * Bail out if the queue has been detached.  Note that we would be in
	 * trouble if the compiler decided to cache the value of
	 * mq->mq_detached in a register or on the stack across loop
	 * iterations.  It probably shouldn't do that anyway since we'll
	 * always return, call an external function that performs a system
	 * call, or reach a memory barrier at some point later in the loop,
	 * but just to be sure, insert a compiler barrier here.
	 */
	pg_compiler_barrier();
	if (mq->mq_force_detached)
		return LOCAL_MQ_DETACHED;

	if ((uint64) ringsize == used)
	{
		/* Skip sleeping if nowait = true. */
		if (nowait)
			return LOCAL_MQ_WOULD_BLOCK;

		ThreadSemaUp(&mq->mq_sema);
		notify_write_num(mq);
		pg_usleep(100L);
		goto retry;
	}
	else
	{
		/*
		 * Write as much data as we can via a single memcpy(). Make sure
		 * these writes happen after the read of mq_num_read, above.
		 * This barrier pairs with the one in local_mq_inc_bytes_read.
		 * (Since we're separating the read of mq_num_read from a
		 * subsequent write to mq_ring, we need a full barrier here.)
		 */
		pg_memory_barrier();
		mq->mq_ring[wn % ringsize] = data;

		/*
		 * Update count of bytes written, with alignment padding.  Note
		 * that this will never actually insert any padding except at the
		 * end of a run of bytes, because the buffer size is a multiple of
		 * MAXIMUM_ALIGNOF, and each read is as well.
		 */
		local_mq_inc_written(mq);

		/* tell the receiver we have data */
		if (used == 0)
		{
			ThreadSemaUp(&mq->mq_sema);
			notify_write_num(mq);
		}
		else if (mq->mq_client_wait)
			notify_write_num(mq);

		return LOCAL_MQ_SUCCESS;
	}
}

int
local_mq_get_receive_pipe(local_mq_handle *mqh)
{
	return mqh->mqh_queue->mq_efd;
}

void
local_mq_set_wait(local_mq_handle *mqh, bool res)
{
	mqh->mqh_queue->mq_client_wait = res;
}

uint64
local_mq_get_remain(local_mq_handle *mqh)
{
	return mqh->mqh_queue->mq_remain;
}

/*
 * Receive a message from a local message queue.
 *
 * We set *nbytes to the message length and *data to point to the message
 * payload.  If the entire message exists in the queue as a single,
 * contiguous chunk, *data will point directly into local memory; otherwise,
 * it will point to a temporary buffer.  This mostly avoids data copying in
 * the hoped-for case where messages are short compared to the buffer size,
 * while still allowing longer messages.  In either case, the return value
 * remains valid until the next receive operation is performed on the queue.
 *
 * When nowait = false, we'll wait on our process latch when the ring buffer
 * is empty and we have not yet received a full message.  The sender will
 * set our process latch after more data has been written, and we'll resume
 * processing.  Each call will therefore return a complete message
 * (unless the sender detaches the queue).
 *
 * When nowait = true, we do not manipulate the state of the process latch;
 * instead, whenever the buffer is empty and we need to read from it, we
 * return LOCAL_MQ_WOULD_BLOCK.  In this case, the caller should call this
 * function again after the process latch has been set.
 */
local_mq_result
local_mq_receive(local_mq_handle *mqh, void **datap, bool nowait)
{
	local_mq	*mq = mqh->mqh_queue;
	Size		ringsize = mq->mq_ring_size;
	uint64		used;
	uint64		written;

	for (;;)
	{
		uint64		readx;

		pg_read_barrier();
		/* Get bytes written, so we can compute what's available to read. */
		written = pg_atomic_read_u64(&mq->mq_num_written);
		readx = pg_atomic_read_u64(&mq->mq_num_read);
		used = written - readx;
		Assert(used <= ringsize);

		/* If we have enough space, we're done. */
		if (used > 0)
		{
			*datap = mq->mq_ring[readx % ringsize];

			local_mq_inc_read(mq);

			/*
			 * Separate the read of mq_num_written, above, from caller's
			 * attempt to read the data itself.  Pairs with the barrier in
			 * local_mq_inc_bytes_written.
			 */
			pg_read_barrier();

			if (mq->mq_efd != -1)
			{
				if (mq->mq_remain == 0)
					read(mq->mq_efd, &mq->mq_remain, sizeof(uint64));
				mq->mq_remain--;
			}

			return LOCAL_MQ_SUCCESS;
		}

		/*
		 * Fall out before waiting if the queue has been detached.
		 *
		 * Note that we don't check for this until *after* considering whether
		 * the data already available is enough, since the receiver can finish
		 * receiving a message stored in the buffer even after the sender has
		 * detached.
		 */
		if (mq->mq_force_detached)
		{
			/*
			 * If the writer advanced mq_num_written and then set
			 * mq_detached, we might not have read the final value of
			 * mq_num_written above.  Insert a read barrier and then check
			 * again if mq_num_written has advanced.
			 */
			pg_read_barrier();
			if (written != pg_atomic_read_u64(&mq->mq_num_written))
				continue;

			return LOCAL_MQ_DETACHED;
		}

		/* Skip manipulation of our latch if nowait = true. */
		if (nowait)
			return LOCAL_MQ_WOULD_BLOCK;

		wait_event_fn_start(mqh->fid, WAIT_EVENT_RECEIVE_DATA);
		/* decrement sema and wait */
		while (ThreadSemaDown(&mqh->mqh_queue->mq_sema) > 0)
		{
			CHECK_FOR_INTERRUPTS();
			if (end_query_requested)
			{
				wait_event_fn_end();
				return LOCAL_MQ_DETACHED;
			}

			/*
			 * If the writer advanced mq_num_written, we might not have read
			 * the final value of mq_num_written above.  Insert a read barrier
			 * and then check again if mq_num_written has advanced.
			 */
			pg_read_barrier();
			if (written != pg_atomic_read_u64(&mq->mq_num_written))
			{
				wait_event_fn_end();
				break;    /* and continue the for loop */
			}

			/*
			 * Fall out before waiting if the queue has been detached.
			 *
			 * Note that we don't check for this until *after* considering whether
			 * the data already available is enough, since the receiver can finish
			 * receiving a message stored in the buffer even after the sender has
			 * detached.
			 */
			if (mq->mq_force_detached)
			{
				wait_event_fn_end();
				/*
				 * If the writer advanced mq_num_written and then set
				 * mq_detached, we might not have read the final value of
				 * mq_num_written above.  Insert a read barrier and then check
				 * again if mq_num_written has advanced.
				 */
				pg_read_barrier();
				if (written != pg_atomic_read_u64(&mq->mq_num_written))
					break; /* and continue the for loop */

				return LOCAL_MQ_DETACHED;
			}
		}
	}

	/* should not be here */
	return LOCAL_MQ_SUCCESS;
}

bool
local_mq_full(local_mq_handle *mqh, double ratio)
{
	local_mq	*mq = mqh->mqh_queue;
	uint32		ringsize = mq->mq_ring_size;
	uint64		rn;
	uint64		wn;

	pg_read_barrier();
	/* Compute number of ring buffer bytes used. */
	rn = pg_atomic_read_u64(&mq->mq_num_read);
	wn = pg_atomic_read_u64(&mq->mq_num_written);

	return ((uint64) ringsize * ratio) <= (wn - rn);
}

/*
 * Detach from a local message queue, and destroy the local_mq_handle.
 */
void
local_mq_detach(local_mq_handle *mqh, bool force)
{
	/* Notify counterparty that we're outta here. */
	local_mq_detach_internal(mqh->mqh_queue, force);
	/* Let memory goes with context. */
}

void
local_mq_pipe_release(local_mq_handle *mqh)
{
	local_mq	*mq = mqh->mqh_queue;
	int efd = mq->mq_efd;

	if (efd != -1)
	{
		close(efd);
		mq->mq_efd = -1;
	}
}
/*
 * Notify counterparty that we're detaching from local message queue.
 *
 * The purpose of this function is to make sure that the process
 * with which we're communicating doesn't block forever waiting for us to
 * fill or drain the queue once we've lost interest.  When the sender
 * detaches, the receiver can read any messages remaining in the queue;
 * further reads will return LOCAL_MQ_DETACHED.  If the receiver detaches,
 * further attempts to send messages will likewise return LOCAL_MQ_DETACHED.
 *
 * This is separated out from local_mq_detach() because if the on_dsm_detach
 * callback fires, we only want to do this much.  We do not try to touch
 * the local local_mq_handle, as it may have been pfree'd already.
 */
static void
local_mq_detach_internal(local_mq *mq, bool force)
{
	mq->mq_detached = true;
	mq->mq_force_detached = force;

	ThreadSemaUp(&mq->mq_sema);
	notify_write_num(mq);
	/* in case there is no tuple */
	notify_eventfd(mq, 1);
}

bool
local_mq_detached(local_mq_handle *mqh)
{
	return mqh->mqh_queue->mq_detached;
}

bool
local_mq_force_detached(local_mq_handle *mqh)
{
	return mqh->mqh_queue->mq_force_detached;
}

/*
 * Get the local_mq from handle.
 */
local_mq *
local_mq_get_queue(local_mq_handle *mqh)
{
	return mqh->mqh_queue;
}

/*
 * Increment the number of bytes read.
 */
static void
local_mq_inc_read(local_mq *mq)
{
	pg_write_barrier();
	/*
	 * There's no need to use pg_atomic_fetch_add_u64 here, because nobody
	 * else can be changing this value.  This method should be cheaper.
	 */
	pg_atomic_write_u64(&mq->mq_num_read,
						pg_atomic_read_u64(&mq->mq_num_read) + 1);
}

/*
 * Increment the number of bytes written.
 */
static void
local_mq_inc_written(local_mq *mq)
{
	pg_write_barrier();
	/*
	 * There's no need to use pg_atomic_fetch_add_u64 here, because nobody
	 * else can be changing this value.  This method avoids taking the bus
	 * lock unnecessarily.
	 */
	pg_atomic_write_u64(&mq->mq_num_written,
						pg_atomic_read_u64(&mq->mq_num_written) + 1);
}
