/*-------------------------------------------------------------------------
 *
 * tqueueThread.c
 *	  Use local_mq to send & receive tuples between threads in one process
 *
 * A TupleQueueAccess to reads and writes tuples with a local_mq. Notice we
 * use local palloced memory as "local_mq" since this tool is designed for
 * tuple transfer between threads.
 *
 * The only scene using this tool is a dprocess keeps draining data from
 * FN. Thus, the draining thread is created here too.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * IDENTIFICATION
 *	  src/backend/executor/tqueueThread.c
 *
 *-------------------------------------------------------------------------
 */

#include <pthread.h>
#include <limits.h>
#include <sys/eventfd.h>

#include "postgres.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/execDispatchFragment.h"
#include "executor/tqueueThread.h"
#include "pgxc/nodemgr.h"
#include "pgxc/squeue.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"
#include "miscadmin.h"

/* GUC parameters */
bool fn_recv_disk_cache = false;

extern int debug_thread_count;

typedef struct tqueue_thread_args
{
	int 				 num_workers;
	uint32 				 queue_idx;
	CombineType			 combine_type;
	FnRcvQueueEntry     *entry;
	TupleQueueSender    *sender;
	MemoryContext        context;
	MemoryContext        err_context;
	FnBufferDesc 		*buf;
} tqueue_thread_args;

static void *TupleQueueThread(void *args);

static inline void
myBufFileSeek(BufFile *file, int fileno, off_t offset, int whence)
{
	if (!file)
		return;

	if (BufFileSeek(file, fileno, offset, whence))
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not rewind tqueue temporary file, errno: %d", errno)));
}

static inline size_t
myBufFileRead(BufFile *file, void *ptr, size_t size)
{
	size_t nread;
	
	if (!file)
		return 0;

	nread = BufFileRead(file, ptr, size);
	if (nread != 0 && nread != size)
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not read from tqueue temporary file, errno: %d", errno)));
	return nread;
}

static inline size_t
myBufFileWrite(BufFile *file, void *ptr, size_t size)
{
	size_t nwrite;

	if (!file)
		return 0;

	nwrite = BufFileWrite(file, ptr, size);
	if (nwrite != size)
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not write to tqueue temporary file, errno: %d", errno)));
	return nwrite;
}

static inline void
myBufFileTell(BufFile *file, int *fileno, off_t *offset)
{
	if (file)
		BufFileTell(file, fileno, offset);
}

/*
 * Send tuple to the designated local_mq.
 *
 * Returns TRUE if successful, FALSE if local_mq has been detached.
 */
static bool
tqueueSendMessage(TupleQueueSender *access, void *data, int tapenum)
{
	local_mq_result		result;
	local_mq_handle	   *queue = access->queue[tapenum];
	BufFileAccess	   *disk = access->disk[tapenum];
	BufFile 		   *file = disk->storage;
	bool	qfull = local_mq_full(queue, 1);
	uint32	length;
	size_t	nread;

	if (fn_recv_disk_cache && file)
	{
		if (!qfull)	/* queue is not full, prepare to push the data from the disk */
			myBufFileSeek(file, disk->read_file, disk->read_offset, SEEK_SET);

		/* consume the data from the disk until it is empty or the queue is full */
		while (!qfull && (nread = myBufFileRead(file, &length, sizeof(uint32))) != 0)
		{
			void   *sdata;

			SpinLockAcquireNoPanic(access->mutex);
			sdata = palloc(length);
			SpinLockRelease(access->mutex);

			*(uint32 *)sdata = length;
			(void) myBufFileRead(file, (char *)sdata + sizeof(uint32),
								 length - sizeof(uint32));

			result = local_mq_send(queue, sdata, true);

			if (result == LOCAL_MQ_DETACHED)
				return false;
			else if (result == LOCAL_MQ_WOULD_BLOCK)
			{
				SpinLockAcquireNoPanic(access->mutex);
				pfree(sdata);
				SpinLockRelease(access->mutex);

				/* queue is full, don't forget to rewind the read pointer */
				myBufFileSeek(file, disk->read_file, disk->read_offset, SEEK_SET);
				qfull = true;	/* will break */
			}
			/* else LOCAL_MQ_SUCCESS, go on */

			/* record where we read */
			myBufFileTell(file, &disk->read_file, &disk->read_offset);
		}

		/*
		 * We have done read from file, and put all items into the queue, but
		 * beware of checking queue full again to avoid a rare condition that
		 * we read the last item from file and put it into the last slot of
		 * queue, causing the queue full again.
		 */
		if (!qfull)
			qfull = local_mq_full(queue, 1);
	}

	if (fn_recv_disk_cache && qfull)
	{
		if (unlikely(file == NULL))
		{
			SpinLockAcquireNoPanic(access->mutex);
			file = disk->storage = BufFileCreateTempInSet(true, NULL);
			SpinLockRelease(access->mutex);
		}

		/* the queue is full, write data into file */
		myBufFileSeek(file, disk->write_file, disk->write_offset, SEEK_SET);
		(void) myBufFileWrite(file, data, *(uint32 *)data);
		myBufFileTell(file, &disk->write_file, &disk->write_offset);
		/* data was copied, free the original */

		SpinLockAcquireNoPanic(access->mutex);
		pfree(data);
		SpinLockRelease(access->mutex);
	}
	else
	{
		/* Send the tuple itself. */
		result = local_mq_send(queue, data, false);

		/* Check for failure. */
		if (result == LOCAL_MQ_DETACHED)
			return false;
		else if (unlikely(result != LOCAL_MQ_SUCCESS))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not send tuple to local-memory queue")));
	}

	return true;
}

FnRcvQueueEntry *
GetFnRcvQueueEntry(FNQueryId queryid, uint16 fid, int workerid, int num_node,
				   int num_workers, CombineType combine_type, uint32 *queue_idx)
{
	LWLock			*partitionLock;
	FnRcvQueueEntry *entry;
	bool		found;
	uint32		hashvalue;

	FragmentKey key = {};
	key.queryid = queryid;
	key.fid = fid;
	key.workerid = workerid;

	hashvalue = get_hash_value(FnRcvBufferHash, &key);
	partitionLock = RecvBufHashPartitionLock(hashvalue);

	LWLockAcquire(partitionLock, LW_EXCLUSIVE);
	entry = (FnRcvQueueEntry *)hash_search_with_hash_value(FnRcvBufferHash,
														   (const void *) &key,
														   hashvalue,
														   HASH_ENTER, &found);
	if (!found)
	{
		/* we are new and under RecvBufHashPartitionLock protection */
		Assert(!IsParallelWorker());
		SpinLockInit(&entry->mutex[0]);
		SHMQueueInit(&entry->queue[0]);
		entry->owner = MyProcPid;
		entry->valid[0] = true;
		entry->nqueue = 0;

		if (combine_type == COMBINE_TYPE_SAME)
		{
			Assert(IS_PGXC_COORDINATOR);
			Assert(num_workers == 1);
			pg_atomic_write_u32(&entry->num_active, 1);
		}
		else
			pg_atomic_write_u32(&entry->num_active, num_node * num_workers);

		if (enable_exec_fragment_critical_print)
			elog(LOG, "GetFnRcvQueueEntry initialize for "
					  "qid_ts_node:%ld, qid_seq:%ld, fid:%d, workerid:%d, "
					  "num_node:%d, num_workers:%d",
				 key.queryid.timestamp_nodeid, key.queryid.sequence,
				 key.fid, key.workerid, num_node, num_workers);
	}

	/* get dummy entry for idle parallel leader, just return */
	if (queue_idx == NULL)
	{
		LWLockRelease(partitionLock);
		return entry;
	}

	if (entry->nqueue > 0)
	{
		/* someone else enters this entry, so just init my queue */
		SpinLockInit(&entry->mutex[entry->nqueue]);
		SHMQueueInit(&entry->queue[entry->nqueue]);
		entry->valid[entry->nqueue] = true;
		entry->nqueue++;
	}
	else
	{
		Assert(entry->nqueue == 0);
		/* maybe parallel leader did this for me, just increase nqueue */
		entry->nqueue++;
	}

	*queue_idx = entry->nqueue - 1;

	LWLockRelease(partitionLock);
	return entry;
}

/*
 * Thread initialization
 */
TupleQueueReceiver *
TupleQueueThreadInit(int maxsize, FNQueryId queryid, uint16 fid, int num_node,
					 int num_workers, int workerid, bool mul_tapes,
					 CombineType combine_type)
{
	tqueue_thread_args *args;
	TupleQueueReceiver *receiver;
	TupleQueueSender   *sender;
	pthread_attr_t      t_atts;
	FnRcvQueueEntry    *entry;
	local_mq		   *mq;
	int     err, i;
	uint32	queue_idx = 0;
	MemoryContext old, context;
	ResourceOwner owner = IsParallelWorker() ?
						  CurrentResourceOwner : TopTransactionResourceOwner;

	ResourceOwnerEnlargeFnRecv(owner);

	context = AllocSetContextCreate(TopTransactionContext,
									"Thread TupleQueue Context",
									ALLOCSET_DEFAULT_SIZES);
	MemoryContextAllowInCriticalSection(context, true);
	old = MemoryContextSwitchTo(context);

	sender = palloc0(sizeof(TupleQueueSender));
	receiver = palloc0(sizeof(TupleQueueReceiver));
	ResourceOwnerRememberFnRecv(owner, receiver);

	receiver->context = context;
	receiver->disconnected = false;

	receiver->nqueue = sender->nqueue = (mul_tapes ? num_node * num_workers : 1);
	maxsize = maxsize / 8 / receiver->nqueue;

	receiver->queue = palloc0(sizeof(local_mq_handle *) * receiver->nqueue);
	receiver->last = palloc0(sizeof(void *) * receiver->nqueue);

	sender->queue = palloc(sizeof(local_mq_handle *) * sender->nqueue);
	sender->disk = palloc(sizeof(BufFileAccess *) * sender->nqueue);
	sender->mutex = palloc(sizeof(slock_t)); /* used between thread so palloc is fine */

	receiver->disk = sender->disk;
	receiver->mutex = sender->mutex;
	SpinLockInit(receiver->mutex);

	for (i = 0; i < receiver->nqueue; i++)
	{
		int efd = -1;

		if (IS_PGXC_COORDINATOR)
		{
            efd = eventfd(0, 0);
            if (efd == -1)
            {
                elog(ERROR,"system error when creating efd %s",
                     strerror(errno));
            }
		}

		mq = local_mq_create(maxsize, efd);
		receiver->queue[i] = local_mq_attach(mq, fid);
		sender->queue[i] = local_mq_attach(mq, fid);
		sender->disk[i] = palloc0(sizeof(BufFileAccess));
	}

	args = palloc0(sizeof(tqueue_thread_args));

	entry = GetFnRcvQueueEntry(queryid, fid, workerid, num_node, num_workers,
							   combine_type, &queue_idx);

	receiver->entry = entry;

	args->entry = entry;
	args->num_workers = num_workers;
	args->queue_idx = queue_idx;
	args->combine_type = combine_type;
	args->sender = sender;
	args->context = context;

	args->err_context = AllocSetContextCreate(context,
											  "Thread TupleQueue ErrorContext",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextAllowInCriticalSection(args->err_context, true);

	/*
	 * save ourselves some memory: the defaults for thread stack size are
	 * large (1M+)
	 */
	pthread_attr_init(&t_atts);
	pthread_attr_setstacksize(&t_atts, Max(PTHREAD_STACK_MIN, (128 * 1024)));
	err = pthread_create(&receiver->threadid, &t_atts,
						 TupleQueueThread, (void *) args);
	pthread_attr_destroy(&t_atts);

	if (err != 0)
	{
		receiver->created = false;
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("TupleQueueThreadInit: failed to create thread"),
					errdetail("pthread_create() failed with err %d", err)));
	}

	receiver->created = true;
	receiver->owner = owner;
	debug_thread_count++;

	MemoryContextSwitchTo(old);

	return receiver;
}

/*
 * Destroy a tuple queue receiver.
 *
 * Note: cleaning up the underlying local_mq is the caller's responsibility.
 * We won't access it here, as it may be detached already.
 */
void
DestroyTupleQueueReceiver(TupleQueueReceiver *access, ResourceOwner owner, bool cleanup)
{
	int		i;
	local_mq_handle **queue = access->queue;

	/*
	 * If the receiver is explicitly disconnected (for rescan), we can't just
	 * detach it, but need to wait until it has received enough end messages(-1)
	 * before exiting, otherwise there may be data left in shared memory.
	 */
	if (access->queue != NULL)
	{
		bool force = !(cleanup && access->disconnected);
		for (i = 0; i < access->nqueue; i++)
		{
			if (access->queue[i] != NULL)
				local_mq_detach(access->queue[i], force);
		}
		access->queue = NULL;
	}

	if (access->created)
	{
		pthread_join(access->threadid, NULL);
		access->created = false;
		debug_thread_count--;

		/*
		 * Release event fd here. Since there is a concurrency issue that
		 * it have to be released after thread exits else TupleQueue thread
		 * may reuse fd which is released already.
		 */
		if (queue != NULL )
		{
			for (i = 0; i < access->nqueue; i++)
			{
				if (queue[i] != NULL)
					local_mq_pipe_release(queue[i]);
			}
		}
	}

	if (access->entry)
	{
		ClearFnRcvQueueEntry(access->entry);
	}
	else
	{
		ListCell *lc;
		foreach(lc, access->wentry)
			ClearFnRcvQueueEntry((FnRcvQueueEntry *) lfirst(lc));
	}

	ResourceOwnerForgetFnRecv(owner, access);

	/* Note that access itself is also freed here */
	if (access->context)
	{
		/*
		 * Before deleting the memory context, CHECK_FOR_INTERRUPTS is
		 * required, as the error message of tqthread is pstrdup in it.
		 */
		CHECK_FOR_INTERRUPTS();
		MemoryContextDelete(access->context);
	}
}

static bool
TupleQueueDetached(TupleQueueSender *access)
{
	int	i;

	for (i = 0; i < access->nqueue; i++)
	{
		if (!local_mq_detached(access->queue[i]))
			return false;
	}

	return true;
}

static bool
TupleQueueForceDetached(TupleQueueSender *access)
{
	int     i;

	for (i = 0; i < access->nqueue; i++)
	{
		if (!local_mq_force_detached(access->queue[i]))
			return false;
	}

	return true;
}

/*
 * Destroy a tuple queue receiver.
 *
 * Note: cleaning up the underlying local_mq is the caller's responsibility.
 * We won't access it here, as it may be detached already.
 */
static void
DestroyTupleQueueSender(tqueue_thread_args *tqargs, bool force)
{
	TupleQueueSender *access = tqargs->sender;
	FnRcvQueueEntry  *entry = tqargs->entry;

	/* We probably already detached from queue, but let's be sure */
	if (access->queue != NULL)
	{
		/* we are detaching from receiver, but don't forget the data on disk */
		int     i = 0;
		int 	nfull = 0;
		int 	nfinish = force ? access->nqueue : 0;
		bool   *finish;
		bool   *qfull;

		SpinLockAcquireNoPanic(access->mutex);
		finish = palloc0(sizeof(bool) * access->nqueue);
		qfull = palloc0(sizeof(bool) * access->nqueue);
		SpinLockRelease(access->mutex);

		while (nfinish < access->nqueue)
		{
			BufFileAccess *disk;
			BufFile 	  *file;
			local_mq_handle *queue;
			size_t	nread;
			uint32	length;

			if (end_query_requested ||	/* the CN tell us to end */
				IsQueryCancelPending() ||	/* pending cancel signal */
				SubThreadErrorPending ||	/* pending sub thread error signal */
				ProcDiePending ||			/* pending die signal */
				ClientConnectionLost ||		/* loose of client connection */
				TupleQueueDetached(access))	/* the main thread tells us to end */
			{
				break;
			}

			i %= access->nqueue;
			queue = access->queue[i];

			if (finish[i])
			{
				i++;
				continue;
			}

			if (qfull[i])
			{
				if ((qfull[i] = local_mq_full(queue, 0.5)) == true)
				{
					if (nfull == access->nqueue - nfinish)
					{
						notify_write_num(mqh_get_mq(queue));
						pg_usleep(1000L);
					}
					i++;
					continue;
				}
				nfull--;
				/* go on */
			}

			disk = access->disk[i];
			file = disk->storage;

			myBufFileSeek(file, disk->read_file, disk->read_offset, SEEK_SET);
			while ((nread = myBufFileRead(file, &length, sizeof(uint32))) != 0)
			{
				local_mq_result result;
				void   *sdata;

				SpinLockAcquireNoPanic(access->mutex);
				sdata = palloc(length);
				SpinLockRelease(access->mutex);

				*(uint32 *)sdata = length;
				(void) myBufFileRead(file, (char *)sdata + sizeof(uint32),
									 length - sizeof(uint32));

				result = local_mq_send(queue, sdata, true);
				if (result == LOCAL_MQ_DETACHED)
				{
					finish[i] = true;
					nfinish++;
					break;
				}
				else if (result == LOCAL_MQ_WOULD_BLOCK)
				{
					SpinLockAcquireNoPanic(access->mutex);
					pfree(sdata);
					SpinLockRelease(access->mutex);

					nfull++;
					qfull[i] = true;

					/* queue is full, don't forget to rewind the read pointer */
					myBufFileSeek(file, disk->read_file, disk->read_offset, SEEK_SET);
					break;
				}
				/* else LOCAL_MQ_SUCCESS */

				myBufFileTell(file, &disk->read_file, &disk->read_offset);
			}

			if (nread == 0)
			{
				finish[i] = true;
				nfinish++;
			}

			if (finish[i])
				local_mq_detach(access->queue[i], true);

			i++; /* next tape */
		}

		for (i = 0; i < access->nqueue; i++)
		{
			if (!finish[i])
				local_mq_detach(access->queue[i], true);
		}
		access->queue = NULL;

		SpinLockAcquireNoPanic(access->mutex);
		pfree(finish);
		SpinLockRelease(access->mutex);
	}

	if (IsParallelWorker() && entry->nqueue > 1)
	{
		/* parallel worker in non-virtual-dop mode */
		FnBufferDesc *buf;
		FnPageHeader  head;
		int     queue_idx = tqargs->queue_idx;

		SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
		entry->valid[queue_idx] = false;
		SpinLockRelease(&entry->mutex[queue_idx]);

		/*
		 * Now FN receiver won't insert into queue_idx, check if I have
		 * complete message and drop others.
		 */
		for (;;)
		{
			SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
			buf = (FnBufferDesc *)SHMQueueNext(&entry->queue[queue_idx],
											   &entry->queue[queue_idx],
											   offsetof(FnBufferDesc, elem));
			if (buf != NULL)
			{
				SHMQueueDelete(&buf->elem);
				SpinLockRelease(&entry->mutex[queue_idx]);
			}
			else
			{
				SpinLockRelease(&entry->mutex[queue_idx]);
				break;
			}

			head = (FnPageHeader) FnBufferDescriptorGetPage(buf);
			if (head->flag & FNPAGE_END)
				(void) pg_atomic_sub_fetch_u32(&entry->num_active, 1);
			FnBufferFree(buf);
		}
	}

	if (fn_recv_disk_cache)
	{
		CleanupTempFiles(true);
		FreeFileAccess();
	}
}

/*
 * Fetch a minimal tuple from a tuple queue receiver.
 *
 * The return value is NULL if there are no remaining tuples.
 *
 * The returned tuple, if any, is still in local_mq. So should not try to
 * free it. Note that this routine must not leak memory!
 */
void *
TupleQueueReceiveMessage(TupleQueueReceiver *receiver, int tapenum, bool *block)
{
	local_mq_result result;
	void	   *data = NULL;

	if (receiver->last[tapenum] != NULL)
	{
		SpinLockAcquireNoPanic(receiver->mutex);
		pfree(receiver->last[tapenum]);
		SpinLockRelease(receiver->mutex);
		receiver->last[tapenum] = NULL;
	}

	/* Attempt to read a message. */
	result = local_mq_receive(receiver->queue[tapenum], &data, block != NULL);

	/* If queue is detached, return NULL. */
	if (result == LOCAL_MQ_DETACHED)
	{
		return NULL;
	}
	else if (result == LOCAL_MQ_WOULD_BLOCK)
	{
		Assert(block != NULL);
		/* set trash to NULL so I won't send it twice */
		receiver->last[tapenum] = NULL;
		*block = true;
		return NULL;
	}

	receiver->last[tapenum] = data;
	Assert(result == LOCAL_MQ_SUCCESS);
	return data;
}

static void
TupleQueueThreadLoop(tqueue_thread_args *tqargs)
{
	FnBufferDesc     *bufferDesc = NULL;
	FnRcvQueueEntry  *entry = tqargs->entry;
	TupleQueueSender *sender = tqargs->sender;
	pg_atomic_uint32 *num_active = &entry->num_active;
	int 	i;
	int 	num_tape = 1;
	int 	which_tape = 1;
	uint32 	queue_idx = tqargs->queue_idx;
	int   **tape_idx = NULL;

	/* used when combine_type is COMBINE_TYPE_SAME */
	int		returning_node = -1;

	Pointer	**huge;	/* two-dimension array of a pointer for huge data */
	uint32	**huge_left;
	uint32	**huge_offset;

	SpinLockAcquireNoPanic(sender->mutex);

	if (sender->nqueue > 1)
	{
		tape_idx = palloc(sizeof(int *) * tqargs->num_workers);
		for (i = 0; i < tqargs->num_workers; i++)
			tape_idx[i] = palloc0(sizeof(int) * NumDataNodes);
	}

	/* initialize support for huge data */
	huge = palloc(sizeof(Pointer *) * tqargs->num_workers);
	huge_left = palloc(sizeof(int *) * tqargs->num_workers);
	huge_offset = palloc(sizeof(int *) * tqargs->num_workers);
	for (i = 0; i < tqargs->num_workers; i++)
	{
		huge[i] = palloc0(sizeof(Pointer) * NumDataNodes);
		huge_left[i] = palloc0(sizeof(int) * NumDataNodes);
		huge_offset[i] = palloc0(sizeof(int) * NumDataNodes);
	}

	SpinLockRelease(sender->mutex);

	for (;;)
	{
		FnPage			page;
		FnPageHeader	header;
		FnPageIterator	iter;
		uint16			workerid;
		uint16			nodeid;

		if (end_query_requested ||		/* the CN tell us to end */
			IsQueryCancelPending() ||	/* pending cancel signal */
			SubThreadErrorPending ||	/* pending sub thread error signal */
			ProcDiePending ||			/* pending die signal */
			ClientConnectionLost ||		/* loose of client connection */
			TupleQueueForceDetached(sender))	/* the main thread tells us to end */
		{
			DestroyTupleQueueSender(tqargs, true);
			return;
		}

		SpinLockAcquireNoPanic(&entry->mutex[queue_idx]);
		bufferDesc = (FnBufferDesc *)SHMQueueNext(&entry->queue[queue_idx],
												  &entry->queue[queue_idx],
												  offsetof(FnBufferDesc, elem));
		if (bufferDesc != NULL)
		{
			/* remember buf we are holding, so can free it if error occur */
			tqargs->buf = bufferDesc;
			SHMQueueDelete(&bufferDesc->elem);
			SpinLockRelease(&entry->mutex[queue_idx]);
		}
		else
		{
			SpinLockRelease(&entry->mutex[queue_idx]);
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
			pg_read_barrier();
#endif
			if (pg_atomic_read_u32(num_active) == 0)
			{
				DestroyTupleQueueSender(tqargs, false);
				return;
			}

			pg_usleep(1000L);
			continue;
		}

		page = FnBufferDescriptorGetPage(bufferDesc);
		header = (FnPageHeader) page;
		workerid = header->workerid;
		nodeid = header->nodeid;

		/*
		 * Replicated INSERT/UPDATE/DELETE with RETURNING: receive only tuples
		 * from one node, skip others as duplicates
		 */
		if (tqargs->combine_type == COMBINE_TYPE_SAME)
		{
			if (returning_node == -1)
				returning_node = nodeid;

			if (returning_node != nodeid)
			{
				tqargs->buf = NULL;
				FnBufferFree(bufferDesc);
				continue;
			}
		}

		if (sender->nqueue > 1)
		{
			/*
			 * The receiving thread was told to separate into multiple queues for
			 * merge-sort, so we should choose a unique tape number for specific
			 * nodeid and workerid.
			 */
			if (tape_idx[workerid][nodeid] == 0)
				tape_idx[workerid][nodeid] = num_tape++;

			which_tape = tape_idx[workerid][nodeid];
		}

		if (header->flag & FNPAGE_END)
		{
			(void) pg_atomic_sub_fetch_u32(num_active, 1);

			if (enable_exec_fragment_print)
				fprintf(stderr, "TupleQueueThread: pid %d recv EOF for "
								"qid_ts_node:%ld, qid_seq:%ld, fid:%d, node:%d, worker:%d\n",
						MyProcPid, header->queryid.timestamp_nodeid,
						header->queryid.sequence, header->fid,
						header->nodeid, header->workerid);
		}

		if (unlikely(header->flag & FNPAGE_VECTOR))
			elog(ERROR, "receive corrupted data");

		/* no one should conflict with us, so don't acquire content lock */
		InitFnPageIterator(&iter);

		if ((header->flag & FNPAGE_HUGE) && huge_left[workerid][nodeid] != 0)
		{
			uint32	left = Min(huge_left[workerid][nodeid], BLCKSZ - iter.offset);
			Pointer	data = ((Pointer) page) + iter.offset;

			/*
			 * More data on the way, preserve what we've got. Notice we
			 * have to distinguish which process sent this message, the
			 * sender should keep themselves in order.
			 */
			memcpy(huge[workerid][nodeid] + huge_offset[workerid][nodeid],
				   data, left);
			huge_offset[workerid][nodeid] += left;
			huge_left[workerid][nodeid] -= left;
			Assert(huge_left[workerid][nodeid] >= 0);

			/* move our iterator carefully with aligned size */
			iter.offset += MAXALIGN(left);

			if (huge_left[workerid][nodeid] == 0)
			{
				/* there is no more data, we can send it to local mq */
				if (!tqueueSendMessage(sender, huge[workerid][nodeid],
									   which_tape - 1))
				{
					/* receiver detached (should not happen) */
					tqargs->buf = NULL;
					FnBufferFree(bufferDesc);

					sender->queue = NULL;
					DestroyTupleQueueSender(tqargs, true);
					return;
				}
				/* reset offset to serve next huge data */
				huge_offset[workerid][nodeid] = 0;
			}
			else /* or the iter.offset must set to the end */
				Assert(FnPageIterateDone(page, &iter));
		}

		while (!FnPageIterateDone(page, &iter))
		{
			Pointer	data;
			Pointer	send;
			uint32	len, rcv;
			uint16	offset = iter.offset;

			FnPageIterateNext(page, &iter, len, data);

			if (len == MAX_UINT32)
				break;

			rcv = Min(len, BLCKSZ - offset);
			if (rcv < len)
			{
				/*
				 * More data on the way, preserve what we've got. Notice we
				 * have to distinguish which process sent this message, the
				 * sender should keep themselves in order.
				 */
				if (unlikely(len > MaxAllocSize))
				{
					SpinLockAcquireNoPanic(sender->mutex);
					huge[workerid][nodeid] = MemoryContextAllocHuge(CurrentMemoryContext, len);
					SpinLockRelease(sender->mutex);
				}
				else
				{
					SpinLockAcquireNoPanic(sender->mutex);
					huge[workerid][nodeid] = palloc(len);
					SpinLockRelease(sender->mutex);
				}

				memcpy(huge[workerid][nodeid], data, rcv);
				huge_left[workerid][nodeid] = len - rcv;
				huge_offset[workerid][nodeid] += rcv;
				break;
			}
			else /* rcv == len */
			{
				/* a regular size of data, just copy and send */
				SpinLockAcquireNoPanic(sender->mutex);
				send = palloc(len);
				SpinLockRelease(sender->mutex);

				memcpy(send, data, len);
				if (!tqueueSendMessage(sender, send, which_tape - 1))
				{
					/* receiver detached (should not happen) */
					tqargs->buf = NULL;
					FnBufferFree(bufferDesc);

					sender->queue = NULL;
					DestroyTupleQueueSender(tqargs, true);
					return;
				}
			}
		}

		tqargs->buf = NULL;
		FnBufferFree(bufferDesc);
	}
}

static void *
TupleQueueThread(void *args)
{
	tqueue_thread_args  *tqargs = (tqueue_thread_args *) args;
	slock_t *mutex = tqargs->sender->mutex;

	/* use local_sigjmp_buf to handle thread local longjmp */
	sigjmp_buf local_sigjmp_buf;

	am_sub_thread = true;

	ErrorContext = tqargs->err_context;
	SubThreadLocalContext = tqargs->context;

	/* CurrentMemoryContext is thread local variable */
	MemoryContextSwitchTo(tqargs->context);

	ThreadSigmask();

	if (fn_recv_disk_cache)
	{
		InitFileAccess();
		PrepareTempTablespaces();
	}

	/*
	 * If an exception is encountered, processing here.
	 * Processing a long jump cause by error in the sub thread.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		char *sub_thread_err_msg;

		MemoryContextSwitchTo(tqargs->context);

		/*
		 * We cannot use spinlock here, because we don't know whether it has
		 * been acquired when an error occurs. Therefore, it is more reasonable
		 * to copy the error message using strdup.
		 */
		sub_thread_err_msg = strdup(GetErrorMessage());

		/* for the same reason, must release mutex again. */
		SpinLockRelease(mutex);

		if (tqargs->buf != NULL)
		{
			FnBufferDesc *buf = tqargs->buf;
			tqargs->buf = NULL;
			FnBufferFree(buf);
		}

		elog(WARNING, "got error in tuple queue thread: %s", sub_thread_err_msg);
		free(sub_thread_err_msg);

		FlushErrorState();

		SubThreadErrorPending = true;
		InterruptPending = true;
		DestroyTupleQueueSender(tqargs, true);

		return NULL;
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* loop till receive NULL tuple (len is -1) */
	TupleQueueThreadLoop(tqargs);

	return NULL;
}
