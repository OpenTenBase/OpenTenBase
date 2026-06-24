/*-------------------------------------------------------------------------
 *
 * forwardsend.c
 *
 * The forward node sender (FN sender)
 * 
 * This is background process forked by postmaster during system start up.
 * It's pretty much like a bgwriter, that continuously writes a share-memory
 * structure FnBufferBlocks (like BufferBlocks in bgwriter) into socket and
 * send to remote (a fn receiver process).
 * 
 * IDENTIFICATION
 *	  src/backend/postmaster/forwardsend.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "miscadmin.h"
#include "pgstat.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "forward/fnconn.h"
#include "forward/fnbufmgr.h"
#include "libpq/pqsignal.h"
#include "postmaster/forward.h"
#include "storage/ipc.h"
#include "utils/memutils.h"

#define		FNMGR_SIGREASON_MICROSEC		500000L		/* 500ms */

typedef struct
{
	pid_t				fss_pid;
	sig_atomic_t 		fss_signalFlags[ForwardMgrSigReason_Number];
} FnMgrSignalSlot;

/*
 * GUC parameters
 */
/* FN sender sleep time between rounds in millisecond */
int			FnSenderDelay;

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t fnmgr_gotSIGUSR1 = false;			/* ForwardMgrSigReason */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;
static volatile sig_atomic_t reconnect_requested = false;
static volatile sig_atomic_t fnmgr_gotPgxcReload = false;    	/* need to handle the pgxc_pool_reload signal */

static FnMgrSignalSlot *	fnmgr_signalSlots = NULL;

/* Signal handlers */

static void bg_quickdie(SIGNAL_ARGS);
static void BgSigHupHandler(SIGNAL_ARGS);
static void ReqShutdownHandler(SIGNAL_ARGS);
static void forward_sigusr1_handler(SIGNAL_ARGS);

static bool am_forward_sender = false;

static bool
fnmgr_check_signal(ForwardMgrSigReason reason)
{
	if (reason < ForwardMgrSigReason_Number)
	{
		if (fnmgr_signalSlots->fss_signalFlags[reason] == true)
		{
			fnmgr_signalSlots->fss_signalFlags[reason] = false;
			return true;
		}
	}

	return false;
}

Size
ForwardMgrSignalShmemSize(void)
{
	return sizeof(FnMgrSignalSlot);
}

void
ForwardMgrSignalShmemInit(void)
{
	Size	size = ForwardMgrSignalShmemSize();
	bool	found = false;

	fnmgr_signalSlots = (FnMgrSignalSlot *)
		ShmemInitStruct("ForwardMgrSignalSlots", size, &found);

	/* If we're first, set everything to zeroes */
	if (!found)
		MemSet(fnmgr_signalSlots, 0, size);
}

int
ForwardMgrSendSignal(ForwardMgrSigReason reason)
{
	/* waiting for forward process startup */
	while (fnmgr_signalSlots->fss_pid == 0)
	{
		pg_usleep(FNMGR_SIGREASON_MICROSEC);
	}

	Assert(fnmgr_signalSlots->fss_pid != 0);
	if (fnmgr_signalSlots->fss_pid &&
		reason < ForwardMgrSigReason_Number)
	{
		fnmgr_signalSlots->fss_signalFlags[reason] = true;

		elog(LOG, "ForwardMgrSendSignal %d send SIGUSR1 to forward process %d ",
			 MyProcPid, fnmgr_signalSlots->fss_pid);

		Assert(fnmgr_signalSlots->fss_pid != 0);
		return kill(fnmgr_signalSlots->fss_pid, SIGUSR1);
	}

	return 0;
}

static void
fnmgr_signal_startup(void)
{
	Size size = ForwardMgrSignalShmemSize();
	MemSet(fnmgr_signalSlots, 0, size);
	fnmgr_signalSlots->fss_pid = MyProcPid;
}

void
fnmgr_process_sigusr1(void)
{
	/* sigusr1: */
	if (fnmgr_gotSIGUSR1)
	{
		if (fnmgr_check_signal(ForwardMgrSigReason_PgxcPoolReload))
		{
			fnmgr_gotPgxcReload = true;
		}

		fnmgr_gotSIGUSR1 = false;
	}
}

bool
forward_is_got_sigusr1(void)
{
	return fnmgr_gotSIGUSR1;
}

/*
 * Main entry point for forward sender process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
ForwardSenderMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext fn_context;
	bool		prev_hibernate;

	MyLocalThreadId = pthread_self();

	SetProcessingMode(InitProcessing);

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, BgSigHupHandler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, ReqShutdownHandler);	/* shutdown */
	pqsignal(SIGQUIT, bg_quickdie); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, forward_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * We need syscache access to setup node definition to know where to
	 * connect, this is kinda heavy for this backend, should get rid of
	 * it someday.
	 */
	InitPostgres(NULL, TemplateDbOid, NULL, BOOTSTRAP_SUPERUSERID, NULL);

	SetProcessingMode(NormalProcessing);


	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	fn_context = AllocSetContextCreate(TopMemoryContext,
									   "Forward Node Sender",
									   ALLOCSET_DEFAULT_SIZES);

	/* avoid hang when dn swap */
	TermFidBackendProc("before start sender proc");

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in here, but we do have LWLocks, buffers.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(fn_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(fn_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Reset hibernation state after any error.
	 */
	prev_hibernate = false;

	fnmgr_signal_startup();

reconnect:
	fnmgr_gotPgxcReload = false;
	PgxcNodeListAndCountWrapTransaction(false, false);
	if ((!shutdown_requested) && (!InitForwardConns()))
		goto reconnect;

	MemoryContextSwitchTo(fn_context);

	/*
	 * Loop forever
	 */
	for (;;)
	{
		bool		can_hibernate;
		int			rc = 0;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (shutdown_requested)
		{
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit from the bgwriter is here */
			proc_exit(0);		/* done */
		}

		if (reconnect_requested)
		{
			reconnect_requested = false;
			TermFidBackendProc("got reconnect request");
			goto reconnect;
		}

		fnmgr_process_sigusr1();
		if (fnmgr_gotPgxcReload)
		{
			TermFidBackendProc("got pgxc pool reload");
			goto reconnect;
		}

		/*
		 * Do one cycle of dirty-buffer writing.
		 */
		can_hibernate = FnBufferSync();

		/*
		 * If no latch event and FnBufferSync says nothing's happening, extend
		 * the sleep in "hibernation" mode, where we sleep forever. Fewer
		 * wakeups save electricity.  When a backend starts using buffers
		 * again, it will wake us up by setting our latch.  Because the extra
		 * sleep will persist only as long as no buffer allocations happen,
		 * this should not distort the behavior of FnBufferSync's control loop
		 * too badly; essentially, it will think that the system-wide idle
		 * interval didn't exist.
		 *
		 * There is a race condition here, in that a backend might allocate a
		 * buffer between the time FnBufferSync saw the alloc count as zero
		 * and the time we call StrategyNotifyBgWriter.  While it's not
		 * critical that we not hibernate anyway, we try to reduce the odds of
		 * that by only hibernating when BgBufferSync says nothing's happening
		 * for two consecutive cycles.  Also, we mitigate any possible
		 * consequences of a missed wakeup by not hibernating forever.
		 */
		if (can_hibernate && prev_hibernate)
		{
			/* Ask for notification at next buffer allocation */
			FnStrategyNotifySender(MyProc->pgprocno);
			/* Sleep ... */
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   FnSenderDelay,
						   WAIT_EVENT_FNSENDER_HIBERNATE);
			/* Reset the notification request in case we timed out */
			FnStrategyNotifySender(-1);
		}

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		prev_hibernate = can_hibernate;
	}
}

bool
IsForwardSenderProcess(void)
{
	return am_forward_sender;
}

void
ForwardSenderProcessIam(void)
{
	am_forward_sender = true;
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * bg_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void
bg_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).  This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
BgSigHupHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void
ReqShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void
forward_sigusr1_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	fnmgr_gotSIGUSR1 = true;
	latch_sigusr1_handler();

	errno = save_errno;
}

bool
forward_is_shutdown_requested(void)
{
	return shutdown_requested;
}

void
ReqReconnect(void)
{
	reconnect_requested = true;
	SetLatch(MyLatch);
}

bool
forward_is_pgxc_pool_reloaded(void)
{
	return fnmgr_gotPgxcReload;
}
