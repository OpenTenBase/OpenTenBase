/*-------------------------------------------------------------------------
 *
 * fnbuf_internals.h
 *	  Internal definitions for buffer manager and the buffer replacement
 *	  strategy of forward node.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * src/include/forward/fnbuf_internals.h
 *
 *-------------------------------------------------------------------------
 */
#include "access/parallel.h"
#include "datatype/timestamp.h"
#include "forward/fnbufpage.h"
#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

#ifndef FNBUFMGR_INTERNALS_H
#define FNBUFMGR_INTERNALS_H

/*
 * Buffer state is a single 32-bit variable where following data is combined.
 *
 * - 18 bits refcount
 * - 4 bits usage count (but useless in fnbuffer)
 * - 10 bits of flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 */
#define BUF_REFCOUNT_ONE 1
#define BUF_REFCOUNT_MASK ((1U << 18) - 1)
#define BUF_FLAG_MASK 0xFFC00000U

/* Get refcount from buffer state */
#define BUF_STATE_GET_REFCOUNT(state) ((state) & BUF_REFCOUNT_MASK)

/*
 * Flags for buffer descriptors
 *
 * Note: TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
 */
#define BM_LOCKED				(1U << 22)	/* buffer header is locked */
#define BM_DIRTY				(1U << 23)	/* data needs writing */
#define BM_FLUSH                (1U << 24)  /* data needs flushing */

typedef enum fn_page_kind
{
	FNPAGE_SEND = 0,
	FNPAGE_RECV,
	FNPAGE_MAX
} fn_page_kind;

/*
 *	FnBufferDesc -- shared descriptor/state data for a single shared buffer
 *	of forward node.
 *
 * Note: Buffer header lock (BM_LOCKED flag) must be held to examine or change
 * the tag, state or wait_backend_pid fields.  In general, buffer header lock
 * is a spinlock which is combined with flags, refcount and usagecount into
 * single atomic variable.  This layout allow us to do some operations in a
 * single atomic operation, without actually acquiring and releasing spinlock;
 * for instance, increase or decrease refcount.  buf_id field never changes
 * after initialization, so does not need locking.  freeNext is protected by
 * the buffer_strategy_lock not buffer header lock.  The LWLock can take care
 * of itself.  The buffer header lock is *not* used to control access to the
 * data in the buffer!
 *
 * It's assumed that nobody changes the state field while buffer header lock
 * is held.  Thus buffer header lock holder can do complex updates of the
 * state variable in single write, simultaneously with lock release (cleaning
 * BM_LOCKED flag).  On the other hand, updating of state without holding
 * buffer header lock is restricted to CAS, which insure that BM_LOCKED flag
 * is not set.  Atomic increment/decrement, OR/AND etc. are not allowed.
 *
 * An exception is that if we have the buffer pinned, its tag can't change
 * underneath us, so we can examine the tag without locking the buffer header.
 * Also, in places we do one-time reads of the flags without bothering to
 * lock the buffer header; this is generally for situations where we don't
 * expect the flag bit being tested to be changing.
 *
 * Be careful to avoid increasing the size of the struct when adding or
 * reordering members.  Keeping it below 64 bytes (the most common CPU
 * cache line size) is fairly important for performance.
 *
 * Current size: 56 bytes.
 */
typedef struct FnBufferDesc
{
	int     buf_id;     /* buffer's index number (from 0) */
	int     freeNext;	/* link in freelist chain */

	/* link in shared memory queue for dispatching purpose on receiver side */
	SHM_QUEUE elem;

	LWLock  content_lock;	/* to lock access to buffer contents */

	/* state of the tag, containing flags, refcount and usagecount */
	pg_atomic_uint32 state;

	/* the next ptr of a fragment available on the sender's page */
	int		next;

	/* send or recv */
	fn_page_kind kind;

	/* sender specific fields */
	union
	{
		/* converted pgxc_node_id, if the node is cn, then the highest bit is 1 */
		uint16  dst_node_id;
		/* broadcast target node group id index */
		uint16  node_group_id;
	};
	uint16	 broadcast_node_num;	/* broadcast page flag, 0 means not broadcast */
} FnBufferDesc;

/*
 * fn_desc_queue is a simple lock-free, multi-producer, single-consumer queue
 * used to pass data from DProcess to FN sender through shared memory.
 */
typedef struct fn_desc_queue
{
	pg_atomic_uint32 head;
	pg_atomic_uint32 tail;
	int size;
	int queue[FLEXIBLE_ARRAY_MEMBER];
} fn_desc_queue;

/*
 * We use the highest bit of node to distinguish coordinator nodes
 * from data nodes
 */
#define MaskCoordinatorNode(node) \
	(((uint16)(node)) | (1 << 15))

#define NodeIsCoordinator(node)	\
	((((uint16)(node)) & (1 << 15)) != 0)

#define UnMaskCoordinatorNode(node) \
	(((uint16)(node)) & (~(1 << 15)))

/* must distinguish sender or receiver */
#define GetFnBufferDescriptor(id, kind) (&(FnBufferDescriptors[(kind)][(id)]))

#define FnBufferDescriptorGetContentLock(bdesc) \
	((LWLock*) (&(bdesc)->content_lock))

#define FnBufferDescriptorGetBuffer(bdesc) ((bdesc)->buf_id + 1)

/* must distinguish sender or receiver or local */
#define FnBufferDescriptorGetPage(bufHdr) \
	((FnPage) (FnBufferBlocks[(bufHdr)->kind] + ((Size) (bufHdr)->buf_id) * BLCKSZ))

/*
 * The freeNext field is either the index of the next freelist entry,
 * or one of these special values:
 */
#define FREENEXT_END_OF_LIST	(-1)
#define FREENEXT_NOT_IN_LIST	(-2)

/*
 * Functions for acquiring/releasing a shared buffer header's spinlock.
 */
extern uint32 LockFnBufHdr(FnBufferDesc *desc);
#define UnlockFnBufHdr(desc, s)	\
	do {	\
		pg_write_barrier(); \
		pg_atomic_write_u32(&(desc)->state, (s) & (~BM_LOCKED)); \
	} while (0)

/*
 * The recv-buffer shared hash tables are partitioned to reduce contention.
 * To determine which partition a given key belongs to, compute the tag's
 * hash code with get_hash_value(), then apply one of these macros.
 */
#define RecvBufHashPartition(hashcode) \
	((hashcode) % NUM_RECVBUF_PARTITIONS)
#define RecvBufHashPartitionLock(hashcode) \
	(&MainLWLockArray[RECVBUF_LWLOCK_OFFSET + \
		RecvBufHashPartition(hashcode)].lock)
#define RecvBufHashPartitionLockByIndex(i) \
	(&MainLWLockArray[RECVBUF_LWLOCK_OFFSET + (i)].lock)

/* key of FnRcvQueueEntry, see below */
typedef struct FragmentKey
{
	FNQueryId	queryid;        /* query ID of this buffer queue */
	uint16		fid;            /* fid of this buffer queue */
	int 		workerid;       /* worker ID of this buffer queue */
	/* Notice: workerid id also used in recv with virtual dop opt */
} FragmentKey;

/*
 * Entry in shared memory HTAB: FnRcvBufferHash.
 * 
 * This is designed for purpose of dispatching a received fn buffer to
 * specific fragment of a query. A receiver process put allocate a shared
 * page and link it into shared memory queue of this entry by the queryid
 * and fid the page have.
 */
typedef struct FnRcvQueueEntry
{
	FragmentKey	key;	/* entry hash key */

	int 		owner;	/* pid of owner */

	uint32 		nqueue;	/* max_parallel_worker_per_gather + main, of RECEIVE side */
	SHM_QUEUE	queue[MAX_PARALLEL_WORKERS + 1];	/* shmem queue of page buffer */
	slock_t		mutex[MAX_PARALLEL_WORKERS + 1];	/* protects the queue */
	bool		valid[MAX_PARALLEL_WORKERS + 1];

	/*
	 * Different workers of same fragment share same data flow and compete
	 * the complete receiving message, notice the initial value of this should
	 * be MAX_UINT32, to avoid race condition. This is also used in single
	 * process without workers.
	 */
	pg_atomic_uint32 num_active;
} FnRcvQueueEntry;

/* in fnbuf_init.c */
extern PGDLLIMPORT FnBufferDesc **FnBufferDescriptors;
extern int FnSendNBuffers;
extern int FnRecvNBuffers;
static inline int
FnGetNBuffers(fn_page_kind kind)
{
	switch (kind)
	{
		case FNPAGE_SEND:
			return FnSendNBuffers;
		case FNPAGE_RECV:
			return FnRecvNBuffers;
		case FNPAGE_MAX:
			elog(PANIC, "unknown fn page kind %d", kind);
	}

	return 0;
}

/* in fnfreelist.c */
extern FnBufferDesc *FnStrategyGetBuffer(fn_page_kind kind, FNQueryId queryid);
extern void FnStrategyFreeBuffer(FnBufferDesc *buf);
extern Size FnStrategyShmemSize(void);
extern void FnStrategyInitialize(void);
extern int FnStrategyNumUsed(fn_page_kind kind, int64 ts_node, int64 seq, uint16 fid);
extern void FnStrategyNotifySender(int bgwprocno);
extern void FnStrategyWakeSender(int buf_id);

/*
 * Entry in shared memory HTAB: FnSndBufferHash.
 * 
 * This is designed for purpose of dispatching a received fn buffer to
 * specific fragment of a query. A receiver process put allocate a shared
 * page and link it into shared memory queue of this entry by the queryid
 * and fid the page have.
 *
 * Under normal circumstances, each process independently owns an
 * FnSndQueueEntry. However, there is a special case where if an initplan
 * has multiple outputs and it requires a param/msg, this channel will be
 * sent by multiple upstream fragments. The FnSndQueueEntry that these
 * fragments use to send param/msg will be shared. Therefore, it is
 * necessary to designate an owner, and only the owner can release the
 * corresponding resources.
 */
typedef struct FnSndQueueEntry
{
	FragmentKey key;    /* entry hash key */
	int			head;   /* head of sender buffers of this fragment */
	int			owner;	/* pid of owner */
} FnSndQueueEntry;

extern void fn_desc_enqueue(fn_desc_queue *q, int buf_id);
extern int  fn_desc_dequeue(fn_desc_queue *q);

#endif							/* FNBUFMGR_INTERNALS_H */
