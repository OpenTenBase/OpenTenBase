/*-------------------------------------------------------------------------
 *
 * forward.h
 *	  Exports from postmaster/forwardsend.c and postmaster/forwardrecv.c.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/include/postmaster/forward.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _FORWARD_H
#define _FORWARD_H

extern void ForwardSenderMain(void) pg_attribute_noreturn();
extern void ForwardReceiverMain(void) pg_attribute_noreturn();
extern bool IsForwardSenderProcess(void);
extern void ForwardSenderProcessIam(void);
extern bool IsForwardRecverProcess(void);
extern void ForwardRecverProcessIam(void);
#define IsForwardPorcess() (IsForwardSenderProcess() || IsForwardRecverProcess())

typedef enum
{
	ForwardMgrSigReason_PgxcPoolReload = 0,

	ForwardMgrSigReason_Number				/* Must be last! */
} ForwardMgrSigReason;

extern Size ForwardMgrSignalShmemSize(void);
extern void ForwardMgrSignalShmemInit(void);
extern int  ForwardMgrSendSignal(ForwardMgrSigReason reason);
extern void fnmgr_process_sigusr1(void);
extern bool forward_is_shutdown_requested(void);
extern bool forward_is_pgxc_pool_reloaded(void);
extern void ReqReconnect(void);
extern bool forward_is_got_sigusr1(void);

#endif							/* _FORWARD_H */
