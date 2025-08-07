/*-------------------------------------------------------------------------
 *
 * local_mq.h
 *	  single-reader, single-writer local memory message queue
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/local_mq.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCAL_MQ_H
#define LOCAL_MQ_H

#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/proc.h"

/* The queue itself, in local memory. */
struct local_mq;
typedef struct local_mq local_mq;

/* Backend-private state. */
struct local_mq_handle;
typedef struct local_mq_handle local_mq_handle;

/* Possible results of a send or receive operation. */
typedef enum
{
	LOCAL_MQ_SUCCESS,				/* Sent or received a message. */
	LOCAL_MQ_WOULD_BLOCK,			/* Not completed; retry later. */
	LOCAL_MQ_DETACHED				/* Other process has detached queue. */
} local_mq_result;

/*
 * Primitives to create a queue and set the sender and receiver.
 *
 * Both the sender and the receiver must be set before any messages are read
 * or written, but they need not be set by the same process.  Each must be
 * set exactly once.
 */
extern local_mq *local_mq_create(uint32 size, int efd);

/* Set up backend-local queue state. */
extern local_mq_handle *local_mq_attach(local_mq *mq, int fid);

/* Associate worker handle with local_mq. */
extern void local_mq_set_handle(local_mq_handle *, BackgroundWorkerHandle *);

/* Break connection, release handle resources. */
extern void local_mq_detach(local_mq_handle *mqh, bool force);
extern bool	local_mq_detached(local_mq_handle *mqh);
extern bool local_mq_force_detached(local_mq_handle *mqh);

/* Get the local_mq from handle. */
extern local_mq *local_mq_get_queue(local_mq_handle *mqh);

/* Send or receive messages. */
extern local_mq_result local_mq_send(local_mq_handle *mqh, void *data, bool nowait);
extern bool local_mq_has_message(local_mq_handle *mqh);
extern local_mq_result local_mq_receive(local_mq_handle *mqh, void **datap, bool nowait);
extern bool local_mq_full(local_mq_handle *mqh, double ratio);
extern int local_mq_get_receive_pipe(struct local_mq_handle *mqh);
extern void local_mq_pipe_release(local_mq_handle *mq);
extern uint64 local_mq_get_remain(local_mq_handle *mqh);
extern void local_mq_set_wait(local_mq_handle *mqh, bool res);
extern void notify_write_num(local_mq *mq);
extern local_mq * mqh_get_mq(local_mq_handle *mqh);

#endif							/* LOCAL_MQ_H */
