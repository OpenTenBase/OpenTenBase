/*-------------------------------------------------------------------------
 *
 * fnbuf_init.c
 *	  buffer manager of forward node initialization routines
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * IDENTIFICATION
 *	  src/backend/forward/storage/fnbuf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "forward/fnbufmgr.h"
#include "pgxc/pgxc.h"

FnBufferDesc **FnBufferDescriptors;
char		 **FnBufferBlocks;
HTAB		  *FnRcvBufferHash;
HTAB		  *FnSndBufferHash;
fn_desc_queue *ForwardSenderQueue;

/*
 * send + recv = FnNBuffers
 * send : recv is 7 : 3
 */
int	FnSendNBuffers = 0;
int FnRecvNBuffers = 0;

/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Synchronization/Locking:
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a FnBufferAlloc().
 *		Pins must be released before end of transaction.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization.
 */
void
InitFnBufferPool(void)
{
	bool		foundSndDescs,
				foundRcvDescs,
				foundSndBufs,
				foundRcvBufs;
	HASHCTL		info;
	long		table_size;

	if (IS_CENTRALIZED_MODE)
		return;

	Assert(FnSendNBuffers > 0 && FnRecvNBuffers > 0);
	/* Align descriptors to a cacheline boundary. */
	FnBufferDescriptors = (FnBufferDesc **)
		ShmemAlloc(sizeof(FnBufferDesc *) * FNPAGE_MAX);
	FnBufferDescriptors[FNPAGE_SEND] = (FnBufferDesc *)
		ShmemInitStruct("FN Sender Buffer Descriptors",
						FnSendNBuffers * sizeof(FnBufferDesc),
						&foundSndDescs);
	FnBufferDescriptors[FNPAGE_RECV] = (FnBufferDesc *)
		ShmemInitStruct("FN Receiver Buffer Descriptors",
						FnRecvNBuffers * sizeof(FnBufferDesc),
						&foundRcvDescs);

	FnBufferBlocks = (char **) ShmemAlloc(sizeof(char *) * FNPAGE_MAX);
	FnBufferBlocks[FNPAGE_SEND] = (char *)
		ShmemInitStruct("FN Sender Buffer Blocks",
						FnSendNBuffers * (Size) BLCKSZ, &foundSndBufs);
	FnBufferBlocks[FNPAGE_RECV] = (char *)
		ShmemInitStruct("FN Receiver Buffer Blocks",
						FnRecvNBuffers * (Size) BLCKSZ, &foundRcvBufs);

	LWLockRegisterTranche(LWTRANCHE_FNBUFFER_CONTENT, "fn_buffer_content");

	if (foundSndDescs || foundRcvDescs || foundSndBufs || foundRcvBufs)
	{
		/* should find all of these, or none of them */
		Assert(foundSndDescs && foundRcvDescs && foundSndBufs && foundRcvBufs);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		int			i;
		int         kind;

		/*
		 * Initialize all the buffer headers.
		 */
		for (kind = FNPAGE_SEND; kind < FNPAGE_MAX; kind++)
		{
			int nbuf = FnGetNBuffers(kind);

			for (i = 0; i < nbuf; i++)
			{
				FnBufferDesc *buf = GetFnBufferDescriptor(i, kind);

				pg_atomic_init_u32(&buf->state, 0);

				buf->buf_id = i;
				buf->dst_node_id = 0;
				buf->broadcast_node_num = 0;
				buf->next = -1;
				buf->kind = kind;

				/*
				 * Initially link all the buffers together as unused. Subsequent
				 * management of this list is done by freelist.c.
				 */
				buf->freeNext = i + 1;

				LWLockInitialize(FnBufferDescriptorGetContentLock(buf),
								 LWTRANCHE_FNBUFFER_CONTENT);
			}

			/* Correct last entry of linked list */
			GetFnBufferDescriptor(nbuf - 1, kind)->freeNext = FREENEXT_END_OF_LIST;
		}
	}

	/* Init other shared buffer-management stuff */
	FnStrategyInitialize();

	/* Init shared hash table of buffers for receiver dispatching */

	/*
	 * Compute max size to request for recv hashtables.
	 */
	table_size = FnRecvNBuffers;

	/*
	 * Allocate hash table for FN receiver structs. This stores per-fragment
	 * information.
	 */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(FragmentKey);
	info.entrysize = sizeof(FnRcvQueueEntry);
	info.num_partitions = NUM_RECVBUF_PARTITIONS;

	FnRcvBufferHash = ShmemInitHash("Forward Receiver Hash",
									table_size,
									table_size,
									&info,
									HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE | HASH_PARTITION);
	/*
	 * Compute max size to request for send hashtables.
	 */
	table_size = FnSendNBuffers;

	/*
	 * Allocate hash table for FN sender structs. This stores per-fragment
	 * information. Different from above, sender hash table distinguish
	 * worker id in FragmentKey.
	 */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(FragmentKey);
	info.entrysize = sizeof(FnSndQueueEntry);

	FnSndBufferHash = ShmemInitHash("Forward Sender Hash",
									table_size,
									table_size,
									&info,
									HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);

	/* initialize shared sender flushing queue */
	ForwardSenderQueue = ShmemAlloc(offsetof(fn_desc_queue, queue) +
									mul_size(FnSendNBuffers, sizeof(int)));
	pg_atomic_init_u32(&ForwardSenderQueue->head, 0);
	pg_atomic_init_u32(&ForwardSenderQueue->tail, 0);
	ForwardSenderQueue->size = FnSendNBuffers;
	memset(ForwardSenderQueue->queue, -1, FnSendNBuffers * sizeof(int));
}

/*
 * FnBufferShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, etc.
 */
Size
FnBufferShmemSize(void)
{
	Size		size = 0;

	if (IS_CENTRALIZED_MODE)
		return size;

	size = add_size(size, mul_size(sizeof(FnBufferDesc *), FNPAGE_MAX));

	/* size of buffer descriptors */
	size = add_size(size, mul_size(FnNBuffers, sizeof(FnBufferDesc)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages, for sender, receiver */
	size = add_size(size, mul_size(sizeof(char *), FNPAGE_MAX));
	size = add_size(size, mul_size(FnNBuffers, BLCKSZ));

	if (IS_PGXC_COORDINATOR)
	{
		/* CN: send : recv is 9 : 1 */
		int chunk = FnNBuffers / 10;
		FnRecvNBuffers = chunk * 9;
		FnSendNBuffers = chunk;
		/* the reset belong to receiver */
		FnRecvNBuffers += FnNBuffers - (FnSendNBuffers + FnRecvNBuffers);
	}
	else
	{
		/* DN: send : recv is 7 : 3 */
		int chunk = FnNBuffers / 10;
		FnRecvNBuffers = chunk * 3;
		FnSendNBuffers = chunk * 7;
		/* the reset belong to sender */
		FnSendNBuffers += FnNBuffers - (FnSendNBuffers + FnRecvNBuffers);
	}

	/* size of stuff controlled by fnfreelist.c */
	size = add_size(size, FnStrategyShmemSize());

	/* receiver and sender hash table size */
	size = add_size(size, hash_estimate_size(FnRecvNBuffers, sizeof(FnRcvQueueEntry)));
	size = add_size(size, hash_estimate_size(FnSendNBuffers, sizeof(FnSndQueueEntry)));

	size = add_size(size, offsetof(fn_desc_queue, queue));
	size = add_size(size, mul_size(FnSendNBuffers, sizeof(int)));

	return size;
}
