/*-------------------------------------------------------------------------
 *
 * parallel.h
 *	  Infrastructure for launching parallel workers
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARALLEL_H
#define PARALLEL_H

#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"

typedef void (*parallel_worker_main_type) (dsm_segment *seg, shm_toc *toc);

typedef struct ParallelWorkerInfo
{
	BackgroundWorkerHandle *bgwhandle;
	shm_mq_handle *error_mqh;
	int32		pid;
} ParallelWorkerInfo;

typedef struct ParallelContext
{
	dlist_node	node;
	SubTransactionId subid;
	int			nworkers;
	int			nworkers_launched;
	bool		leader_participate;
	char	   *library_name;
	char	   *function_name;
	ErrorContextCallback *error_context_stack;
	shm_toc_estimator estimator;
	dsm_segment *seg;
	void	   *private_memory;
	shm_toc    *toc;
	ParallelWorkerInfo *worker;
	int			nknown_attached_workers;
	bool	   *known_attached_workers;
} ParallelContext;

#ifdef __OPENTENBASE__
/* 
 * total number of parallel workers launched in current parallel context. 
 * This is set by the session in shm after all workers launched.
 */
#define MAX_PARALLEL_WORKERS  32

typedef struct ParallelWorkerStatus
{
	volatile int  numExpectedWorkers;	/* total number of expected parallel workers */
	volatile int  numLaunchedWorkers;	/* total number of launched parallel workers */
	volatile bool cacheSend;			/* should I cache my send data? */
	volatile bool workerSendStatus[MAX_PARALLEL_WORKERS];
} ParallelWorkerStatus;
#endif

typedef struct ParallelWorkerContext
{
	dsm_segment *seg;
	shm_toc    *toc;
} ParallelWorkerContext;

extern volatile bool ParallelMessagePending;
extern int	ParallelWorkerNumber;
extern bool InitializingParallelWorker;

#define		IsParallelWorker()		(ParallelWorkerNumber >= 0)

extern ParallelContext *CreateParallelContext(const char *library_name, const char *function_name, int nworkers);
extern void InitializeParallelDSM(ParallelContext *pcxt);
extern void ReinitializeParallelDSM(ParallelContext *pcxt);
extern void LaunchParallelWorkers(ParallelContext *pcxt, bool has_remote);
extern void WaitForParallelWorkersToAttach(ParallelContext *pcxt);
extern void WaitForParallelWorkersToFinish(ParallelContext *pcxt);
extern void CheckFailedParallelWorkers(const ParallelContext *pcxt);
extern void DestroyParallelContext(ParallelContext *pcxt);
extern bool ParallelContextActive(void);

extern void HandleParallelMessageInterrupt(void);
extern void HandleParallelMessages(void);
extern void AtEOXact_Parallel(bool isCommit);
extern void AtEOSubXact_Parallel(bool isCommit, SubTransactionId mySubId);
extern void ParallelWorkerReportLastRecEnd(XLogRecPtr last_xlog_end);

extern void ParallelWorkerMain(Datum main_arg);

#endif							/* PARALLEL_H */
