/*-------------------------------------------------------------------------
 *
 * procsignal.h
 *      Routines for interprocess signalling
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/storage/procsignal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCSIGNAL_H
#define PROCSIGNAL_H

#include "storage/backendid.h"


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
    PROCSIG_CATCHUP_INTERRUPT,    /* sinval catchup interrupt */
    PROCSIG_NOTIFY_INTERRUPT,    /* listen/notify interrupt */
#ifdef PGXC
    PROCSIG_PGXCPOOL_RELOAD,    /* abort current transaction and reconnect to pooler */
    PROCSIG_PGXCPOOL_REFRESH,    /* refresh local view of connection handles */
#endif
    PROCSIG_PARALLEL_MESSAGE,    /* message from cooperating parallel backend */
#ifdef __OPENTENBASE__
    PROCSIG_PARALLEL_EXIT,        /* message from exited parallel backend */
#endif
    PROCSIG_WALSND_INIT_STOPPING,    /* ask walsenders to prepare for shutdown  */

    /* Recovery conflict reasons */
    PROCSIG_RECOVERY_CONFLICT_DATABASE,
    PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
    PROCSIG_RECOVERY_CONFLICT_LOCK,
    PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
    PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,

    NUM_PROCSIGNALS                /* Must be last! */
} ProcSignalReason;

/*
 * prototypes for functions in procsignal.c
 */
extern Size ProcSignalShmemSize(void);
extern void ProcSignalShmemInit(void);

extern void ProcSignalInit(int pss_idx);
extern int SendProcSignal(pid_t pid, ProcSignalReason reason,
               BackendId backendId);

extern void procsignal_sigusr1_handler(SIGNAL_ARGS);

#endif                            /* PROCSIGNAL_H */
