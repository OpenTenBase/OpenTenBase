/*-------------------------------------------------------------------------
 *
 * logicallauncher.h
 *      Exports for logical replication launcher.
 *
 * Portions Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/replication/logicallauncher.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALLAUNCHER_H
#define LOGICALLAUNCHER_H

extern int    max_logical_replication_workers;
extern int    max_sync_workers_per_subscription;
#ifdef __OPENTENBASE__
extern int  max_network_bandwidth_per_subscription;
#endif

extern void ApplyLauncherRegister(void);
extern void ApplyLauncherMain(Datum main_arg);

extern Size ApplyLauncherShmemSize(void);
extern void ApplyLauncherShmemInit(void);

extern void ApplyLauncherWakeupAtCommit(void);
extern bool XactManipulatesLogicalReplicationWorkers(void);
extern void AtEOXact_ApplyLauncher(bool isCommit);

extern bool IsLogicalLauncher(void);

#endif                            /* LOGICALLAUNCHER_H */
