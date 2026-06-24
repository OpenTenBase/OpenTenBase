/*-------------------------------------------------------------------------
 *
 * execDispatchFragment.h
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/execDispatchFragment.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECDISPATCHFRAGMENT_H
#define EXECDISPATCHFRAGMENT_H

#include "executor/execFragment.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxcnode.h"
#include "nodes/pg_list.h"

extern volatile sig_atomic_t end_query_requested;

typedef enum RemoteControllerRet
{
	RemoteControllerEventFN,
	RemoteControllerEventConnState,
	RemoteControllerEventFinish,
	RemoteControllerEventTimeout,
} RemoteControllerRet;

extern void ExecDispatchRemoteFragment(FragmentTable *ftable, EState *estate);

extern void ExecRunRemoteFragment(EState *estate);

extern void RewindRemoteController(RemoteFragmentController *control);
extern void
RemoteControlMessageProcess(PGXCNodeHandle *conn, RemoteFragmentController *control, bool ignore_error);

extern void WaitRemoteFragmentDone(EState *estate);
extern RemoteControllerRet RunRemoteController(RemoteFragmentController *control,
											   bool event_return, bool ignore_error, int timeout);

extern int RunRemoteControllerWithFN(TupleQueueReceiver *receiver, int tapenum, void* controller);
extern int RunRemoteControllerWithFD(int fd, void* controller, int timeout);

extern void CloseRemoteFragment(FragmentTable *ftable);
extern void RemoteFragmentResetConnection(EState *estate);
extern void RemoteFragmentSigusr2Handler(SIGNAL_ARGS);

extern SubPlan *lookup_subplan_for_param(List *initPlan, int paramno);
extern bool CheckParallelDone(ParallelWorkerStatus *parallel_status, ParallelContext *pcxt);

extern void redirect_remote_subplan_recv(List *subplans, Fragment *receiver);
extern void redirect_remote_subplan_send(PlannerGlobal *glob, Plan *receiver);
#endif							/* EXECDISPATCHFRAGMENT_H  */
