/*-------------------------------------------------------------------------
 *
 * tqueueThread.h
 *	  Use shm_mq to send & receive tuples between threads in one process
 *
 * A TupleQueueAccess to reads and writes tuples with a shm_mq. Notice we
 * use local palloced memory as "shm_mq" since this tool is designed for
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
 *	  src/include/executor/tqueueThread.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TQUEUETHREAD_H
#define TQUEUETHREAD_H

#include "postgres.h"

#include "forward/fnbufmgr.h"
#include "pgxc/planner.h"
#include "storage/buffile.h"
#include "storage/local_mq.h"
#include "utils/resowner.h"

typedef struct BufFileAccess
{
	BufFile		*storage;

	int			write_file;		/* write temp file# */
	off_t		write_offset;	/* write byte offset in file */
	int			read_file;		/* read temp file# */
	off_t		read_offset;	/* read byte offset in file */
} BufFileAccess;

/* Opaque struct, only known inside tqueue.c. */
/*
 * TupleQueueReceiver object's private contents
 *
 * A wrapper of local_mq_handle
 * A threadid of sender
 */
typedef struct TupleQueueReceiver
{
	local_mq_handle **queue;    /* local_mq to read */
	int             nqueue;     /* number of queue */
	/* Notice: we may need more than one queue if merge-sort required */
	BufFileAccess	**disk;		/* sender disk storage, just for closing it... */

	pthread_t		threadid;		/* id of sender */
	bool			created;		/* is sender created */
	bool			disconnected;	/* is fragment disconnected */

	void	**last;	/* last item (array of different tape) we received,
 					 * should pfree it in next round */

	FnRcvQueueEntry	*entry;		/* entry point of this fragment */
	List			*wentry;	/* entry list of this fragment's virtual workers */
	MemoryContext	 context;
	slock_t 		*mutex;		/* protect palloc/pfree with TupleQueue context */

	ResourceOwner	 owner;
} TupleQueueReceiver;

/*
 * TupleQueueSender object's private contents
 *
 * A wrapper of local_mq_handle
 */
typedef struct TupleQueueSender
{
	local_mq_handle	**queue;	/* local_mq to write */
	slock_t 		*mutex;		/* protect palloc/pfree with TupleQueue context */
	int 			nqueue;		/* number of queue */
	/* Notice: we may need more than one queue if merge-sort required */

	BufFileAccess	**disk;		/* disk storage when queue is full */
} TupleQueueSender;

extern FnRcvQueueEntry *GetFnRcvQueueEntry(FNQueryId queryid, uint16 fid,
										   int workerid, int num_node,
										   int num_workers,
										   CombineType combine_type,
										   uint32 *queue_idx);
extern TupleQueueReceiver *TupleQueueThreadInit(int maxsize, FNQueryId queryid,
												uint16 fid, int num_node,
												int num_workers, int workerid,
												bool mul_tapes,
												CombineType combine_type);
extern void *TupleQueueReceiveMessage(TupleQueueReceiver *receiver, int tapenum, bool *block);
extern void DestroyTupleQueueReceiver(TupleQueueReceiver *access, ResourceOwner owner, bool cleanup);

#endif							/* TQUEUETHREAD_H */
