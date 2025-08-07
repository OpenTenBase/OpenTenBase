/*-------------------------------------------------------------------------
 *
 * procsignal.h
 *	  Routines for interprocess signalling
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procsignal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCSIGNAL_H
#define PROCSIGNAL_H

#include "storage/backendid.h"

#define NUM_CUSTOM_PROCSIGNALS 64
/*
 * Reasons for signalling a Postgres child process (a backend or an auxiliary
 * process, like checkpointer).  We can cope with concurrent signals for different
 * reasons.  However, if the same reason is signaled multiple times in quick
 * succession, the process is likely to observe only one notification of it.
 * This is okay for the present uses.
 *
 * Also, because of race conditions, it's important that all the signals be
 * defined so that no harm is done if a process mistakenly receives one.
 */
#ifdef PGXC
/*
 * In the case of Postgres-XC, it may be possible that this backend is
 * signaled during a pool manager reload process. In this case it means that
 * remote node connection has been changed inside pooler, so backend has to
 * abort its current transaction, reconnect to pooler and update its session
 * information regarding remote node handles.
 */
#endif
typedef enum
{
	INVALID_PROCSIGNAL,             /* Must be first */
	PROCSIG_CATCHUP_INTERRUPT,	/* sinval catchup interrupt */
	PROCSIG_NOTIFY_INTERRUPT,	/* listen/notify interrupt */
#ifdef PGXC
	PROCSIG_PGXCPOOL_RELOAD,	/* abort current transaction and reconnect to pooler */
	PROCSIG_PGXCPOOL_REFRESH,	/* refresh local view of connection handles */
#endif
	PROCSIG_PARALLEL_MESSAGE,	/* message from cooperating parallel backend */
	PROCSIG_WALSND_INIT_STOPPING,	/* ask walsenders to prepare for shutdown  */
	PROCSIG_LOG_MEMORY_CONTEXT, /* ask backend to log the memory contexts */
	PROCSIG_GET_MEMORY_DETAIL,
	PROCSIG_GET_MEMORY_CONTEXT_DETAIL,

	/* Recovery conflict reasons */
	PROCSIG_RECOVERY_CONFLICT_FIRST,
	PROCSIG_RECOVERY_CONFLICT_DATABASE = PROCSIG_RECOVERY_CONFLICT_FIRST,
	PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
	PROCSIG_RECOVERY_CONFLICT_LOCK,
	PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
	PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
	PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
	PROCSIG_RECOVERY_CONFLICT_LAST = PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
	PROCSIG_REFRESH_MEMORY_TRACKINFO, /* Tell the backend process to archive the latest memory usage statistics. */

	FINISH_READ_ONLY_PROCESS,

	PROCSIG_CUSTOM_1,
	/*
	 * PROCSIG_CUSTOM_2,
	 * ...,
	 * PROCSIG_CUSTOM_N-1,
	 */
	PROCSIG_CUSTOM_N = PROCSIG_CUSTOM_1 + NUM_CUSTOM_PROCSIGNALS - 1,

	NUM_PROCSIGNALS				/* Must be last! */
} ProcSignalReason;

typedef void (*ProcSignalHandler_type) (void);

/*
 * prototypes for functions in procsignal.c
 */
extern Size ProcSignalShmemSize(void);
extern void ProcSignalShmemInit(void);

extern void ProcSignalInit(int pss_idx);
extern ProcSignalReason
	RegisterCustomProcSignalHandler(ProcSignalHandler_type handler);
extern int SendProcSignal(pid_t pid, ProcSignalReason reason,
			   BackendId backendId);

extern void CheckAndHandleCustomSignals(void);
extern void procsignal_sigusr1_handler(SIGNAL_ARGS);

#endif							/* PROCSIGNAL_H */
