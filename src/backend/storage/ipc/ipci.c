/*-------------------------------------------------------------------------
 *
 * ipci.c
 *	  POSTGRES inter-process communication initialization code.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipci.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/atxact.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "access/gtm.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/subtrans.h"
#include "access/twophase.h"
#include "commands/async.h"
#include "forward/fnbufmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "postmaster/clustermon.h"
#endif
#include "postmaster/autovacuum.h"
#include "postmaster/job_scheduler.h"
#include "postmaster/clean2pc.h"
#include "postmaster/clustermon.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/postmaster.h"
#include "replication/logicallauncher.h"
#include "replication/slot.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/spin.h"
#ifdef XCP
#include "pgxc/pgxc.h"
#include "pgxc/squeue.h"
#include "pgxc/pause.h"
#endif
#include "utils/backend_random.h"
#ifdef _MLS_
#include "utils/mls.h"
#endif
#include "utils/snapmgr.h"

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif

#ifdef _MIGRATE_
#include "pgxc/shardmap.h"
#include "pgxc/groupmgr.h"
#endif
#ifdef __OPENTENBASE__
#include "storage/nodelock.h"
#endif

#ifdef __LICENSE__
#include "license/license.h"
#endif

#ifdef __AUDIT__
#include "postmaster/auditlogger.h"
#endif

#ifdef __STORAGE_SCALABLE__
#include "replication/logical_statistic.h"
#endif
#include "utils/backend_cancel.h"

#ifdef __OPENTENBASE_C__
#include "access/result_cache.h"
#include "executor/execDispatchFragment.h"
#include "executor/nodeCtescan.h"
#include "postmaster/forward.h"
#endif

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif
#include "utils/resgroup.h"

#include "optimizer/spm_cache.h"

shmem_startup_hook_type shmem_startup_hook = NULL;

static Size total_addin_request = 0;
static bool addin_request_allowed = true;

/*
 * RequestAddinShmemSpace
 *		Request that extra shmem space be allocated for use by
 *		a loadable module.
 *
 * This is only useful if called from the _PG_init hook of a library that
 * is loaded into the postmaster via shared_preload_libraries.  Once
 * shared memory has been allocated, calls will be ignored.  (We could
 * raise an error, but it seems better to make it a no-op, so that
 * libraries containing such calls can be reloaded if needed.)
 */
void
RequestAddinShmemSpace(Size size)
{
	if (IsUnderPostmaster || !addin_request_allowed)
		return;					/* too late */
	total_addin_request = add_size(total_addin_request, size);
}


/*
 * CreateSharedMemoryAndSemaphores
 *		Creates and initializes shared memory and semaphores.
 *
 * This is called by the postmaster or by a standalone backend.
 * It is also called by a backend forked from the postmaster in the
 * EXEC_BACKEND case.  In the latter case, the shared memory segment
 * already exists and has been physically attached to, but we have to
 * initialize pointers in local memory that reference the shared structures,
 * because we didn't inherit the correct pointer values from the postmaster
 * as we do in the fork() scenario.  The easiest way to do that is to run
 * through the same code as before.  (Note that the called routines mostly
 * check IsUnderPostmaster, rather than EXEC_BACKEND, to detect this case.
 * This is a bit code-wasteful and could be cleaned up.)
 *
 * If "makePrivate" is true then we only need private memory, not shared
 * memory.  This is true for a standalone backend, false for a postmaster.
 */
void
CreateSharedMemoryAndSemaphores(bool makePrivate, int port)
{
	PGShmemHeader *shim = NULL;

	if (!IsUnderPostmaster)
	{
		PGShmemHeader *seghdr;
		Size		size;
		int			numSemas;

		/* Compute number of semaphores we'll need */
		numSemas = ProcGlobalSemas();
		numSemas += SpinlockSemas();

		/*
		 * Size of the Postgres shared-memory block is estimated via
		 * moderately-accurate estimates for the big hogs, plus 100K for the
		 * stuff that's too small to bother with estimating.
		 *
		 * We take some care during this phase to ensure that the total size
		 * request doesn't overflow size_t.  If this gets through, we don't
		 * need to be so careful during the actual allocation phase.
		 */
		size = 100000;
		size = add_size(size, PGSemaphoreShmemSize(numSemas));
		size = add_size(size, SpinlockSemaSize());
		size = add_size(size, hash_estimate_size(SHMEM_INDEX_SIZE,
												 sizeof(ShmemIndexEnt)));
		size = add_size(size, BufferShmemSize());
		size = add_size(size, FnBufferShmemSize());
		size = add_size(size, LockShmemSize());
		size = add_size(size, PredicateLockShmemSize());
		size = add_size(size, ProcGlobalShmemSize());
		size = add_size(size, XLOGShmemSize());
		size = add_size(size, CommitTsShmemSize());
        size = add_size(size, CSNLOGShmemSize());
#ifdef __OPENTENBASE_C__
		size = add_size(size, ForwardMgrSignalShmemSize());
#endif
#ifdef __OPENTENBASE__
		size = add_size(size, GTSTrackSize());
        if (IS_PGXC_COORDINATOR)
        {
            size = add_size(size, ResultCacheShmemSize());
        }
		size = add_size(size, RecoveryGTMHostSize());
		size = add_size(size, SHMGTMPrimaryInfoSize());
#endif
#ifdef __OPENTENBASE_DEBUG__
		size = add_size(size, SnapTableShmemSize());
#endif
		size = add_size(size, TwoPhaseShmemSize());
		size = add_size(size, BackgroundWorkerShmemSize());
		size = add_size(size, MultiXactShmemSize());
		size = add_size(size, LWLockShmemSize());
		size = add_size(size, ProcArrayShmemSize());
		size = add_size(size, BackendStatusShmemSize());
		size = add_size(size, PgxcQueryIdShmemSize());
		size = add_size(size, SInvalShmemSize());
		size = add_size(size, PMSignalShmemSize());
		size = add_size(size, ProcSignalShmemSize());
		size = add_size(size, CheckpointerShmemSize());
		size = add_size(size, AutoVacuumShmemSize());
		size = add_size(size, ReplicationSlotsShmemSize());
		size = add_size(size, ReplicationOriginShmemSize());
		size = add_size(size, WalSndShmemSize());
		size = add_size(size, WalRcvShmemSize());
		size = add_size(size, Clean2pcShmemSize());
#ifdef XCP
		if (IS_PGXC_COORDINATOR)
			size = add_size(size, ClusterLockShmemSize());
		size = add_size(size, ClusterMonitorShmemSize());
#endif
		size = add_size(size, ApplyLauncherShmemSize());
		size = add_size(size, SnapMgrShmemSize());
		size = add_size(size, BTreeShmemSize());
		size = add_size(size, SyncScanShmemSize());
		size = add_size(size, AsyncShmemSize());
#ifdef PGXC
		size = add_size(size, NodeTablesShmemSize());
		size = add_size(size, NodeGroupShmemSize());

#ifdef __OPENTENBASE__
		size = add_size(size, NodeHashTableShmemSize());
#endif
#endif
		size = add_size(size, BackendRandomShmemSize());
#ifdef EXEC_BACKEND
		size = add_size(size, ShmemBackendArraySize());
#endif

#ifdef USE_MODULE_MSGIDS
		size = add_size(size, MsgModuleShmemSize());
#endif

#ifdef __AUDIT_FGA__
        size = add_size(size, AuditFgaShmemSize());
#endif

		/* freeze the addin request size and include it */
		addin_request_allowed = false;
		size = add_size(size, total_addin_request);

		/* might as well round it off to a multiple of a typical page size */
		size = add_size(size, 8192 - (size % 8192));

#ifdef _MIGRATE_
		size = add_size(size, ShardMapShmemSize());
#endif

#ifdef _SHARDING_
		size = add_size(size, ShardBarrierShmemSize());
#endif
#ifdef __OPENTENBASE__
		size = add_size(size, NodeLockShmemSize());
		size = add_size(size, ShardStatisticShmemSize());
#endif
#ifdef __LICENSE__
		size = add_size(size, LicenseShmemSize());
#endif
#ifdef __AUDIT__
		size = add_size(size, AuditLoggerShmemSize());
#endif
#ifdef __STORAGE_SCALABLE__
		size = add_size(size, PubStatDataShmemSize(g_PubStatHashSize, g_PubTableStatHashSize));
		size = add_size(size, SubStatDataShmemSize(g_SubStatHashSize, g_SubTableStatHashSize));
#endif
#ifdef __TWO_PHASE_TRANS__
		size = add_size(size, Record2pcCacheSize());
#endif

#ifdef __RESOURCE_QUEUE__
		size = add_size(size, ResQUsageShmemSize());
#endif
		if (enable_resource_group)
			size = add_size(size, ResGroupShmemSize());

        size = add_size(size, CancelBackendMsgShmemSize());

		elog(DEBUG3, "invoking IpcMemoryCreate(size=%zu)", size);

		/* unlink rel hash table size */
		size = add_size(size, UnlinkRelHTABShmemSize());

		/* workfile entry array and workfile usage hash table */
		size = add_size(size, WorkFileShmemSize());

		size = add_size(size, SPMPlanShmemSize());
		size = add_size(size, SPMHistoryShmemSize());

		/*
		 * Create the shmem segment
		 */
		seghdr = PGSharedMemoryCreate(size, makePrivate, port, &shim);

		ereport(LOG, (errmsg("Initializing shared memory and semaphores. Shared memory size: [%zu]", size)));

		InitShmemAccess(seghdr);

		/*
		 * Create semaphores
		 */
		PGReserveSemaphores(numSemas, port);

		/*
		 * If spinlocks are disabled, initialize emulation layer (which
		 * depends on semaphores, so the order is important here).
		 */
#ifndef HAVE_SPINLOCKS
		SpinlockSemaInit();
#endif
	}
	else
	{
		/*
		 * We are reattaching to an existing shared memory segment. This
		 * should only be reached in the EXEC_BACKEND case, and even then only
		 * with makePrivate == false.
		 */
#ifdef EXEC_BACKEND
		Assert(!makePrivate);
#else
		elog(PANIC, "should be attached to shared memory already");
#endif
	}

	/*
	 * Set up shared memory allocation mechanism
	 */
	if (!IsUnderPostmaster)
		InitShmemAllocation();

	/*
	 * Now initialize LWLocks, which do shared memory allocation and are
	 * needed for InitShmemIndex.
	 */
	CreateLWLocks();

	/*
	 * Set up shmem.c index hashtable
	 */
	InitShmemIndex();

	/*
	 * Set up xlog, clog, and buffers
	 */
	XLOGShmemInit();
	CommitTsShmemInit();
    CSNLOGShmemInit();

#ifdef __OPENTENBASE_C__
	ForwardMgrSignalShmemInit();
	if (IS_PGXC_COORDINATOR)
	{
		GlobalXidShmemInit();
		InitResultCache();
	}
#endif
#ifdef __OPENTENBASE__
	GTSTrackInit();
	RecoveryGTMHostInit();
	SHMGTMPrimaryInfoInit();
#endif

#ifdef __OPENTENBASE_DEBUG__
	InitSnapBufTable();
#endif
	MultiXactShmemInit();
	InitBufferPool();
	InitUnlinkRelHTAB();
	InitFnBufferPool();
	WorkFileShmemInit();

	/*
	 * Set up lock manager
	 */
	InitLocks();

	/*
	 * Set up predicate lock manager
	 */
	InitPredicateLocks();

	/*
	 * Set up process table
	 */
	if (!IsUnderPostmaster)
		InitProcGlobal();

	CreateSharedProcArray();
	CreateSharedBackendStatus();
	CreateSharedPgxcQueryId();
	TwoPhaseShmemInit();
	BackgroundWorkerShmemInit();

	/*
	 * Set up shared-inval messaging
	 */
	CreateSharedInvalidationState();

	/*
	 * Set up interprocess signaling mechanisms
	 */
	PMSignalShmemInit();
	ProcSignalShmemInit();
	CheckpointerShmemInit();
	AutoVacuumShmemInit();
	ReplicationSlotsShmemInit();
	ReplicationOriginShmemInit();
	WalSndShmemInit();
	WalRcvShmemInit();
	ApplyLauncherShmemInit();

	Clean2pcShmemInit();

#ifdef XCP
	if (IS_PGXC_COORDINATOR)
		ClusterLockShmemInit();
	ClusterMonitorShmemInit();
#endif

	/*
	 * Set up other modules that need some shared memory space
	 */
	SnapMgrInit();
	BTreeShmemInit();
	SyncScanShmemInit();
	AsyncShmemInit();
	McxtDumpShmemInit();
	BackendRandomShmemInit();

#ifdef __AUDIT_FGA__
    AuditFgaShmemInit();
#endif    
#ifdef PGXC
	NodeTablesShmemInit();
	NodeGroupShmemInit();
#endif

#ifdef _MIGRATE_
	if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
	{
		ShardMapShmemInit_CN();
	}
	if (IS_PGXC_DATANODE)
	{
		ShardMapShmemInit_DN();
	}	
#endif

#ifdef _SHARDING_
	ShardBarrierShmemInit();
#endif

#ifdef __OPENTENBASE__
	NodeLockShmemInit();
	ShardStatisticShmemInit();
#endif

#ifdef __LICENSE__
	LicenseShmemInit();
#endif

#ifdef __AUDIT__
	AuditLoggerShmemInit();
#endif

#ifdef __RESOURCE_QUEUE__
	ResQUsageShmemInit();
#endif
	if (enable_resource_group && !IsUnderPostmaster)
		ResGroupShmemInit();

#ifdef __STORAGE_SCALABLE__
	InitPubStatData(g_PubStatHashSize, g_PubTableStatHashSize);
	InitSubStatData(g_SubStatHashSize, g_SubTableStatHashSize);
#endif

#ifdef __TWO_PHASE_TRANS__
	Record2pcCacheInit();
#endif

#ifdef __OPENTENBASE_C__
	if (IsPostmasterEnvironment &&
		!IsUnderPostmaster &&
		port != 0 &&
		ForwardPortNumber > 0
		)
	{
		if (IsProcCrash)
		{
			elog(LOG, "raise SIGQUIT because the subprocess crashed");
			raise(SIGQUIT);
		}
	}
#endif

    BackendCancelShmemInit();

	SPMPlanShmemInit();
	SPMHistoryShmemInit();
#ifdef EXEC_BACKEND

	/*
	 * Alloc the win32 shared backend array
	 */
	if (!IsUnderPostmaster)
		ShmemBackendArrayAllocation();
#endif

#ifdef USE_MODULE_MSGIDS
	MsgModuleShmemInit();
#endif

	/* Initialize dynamic shared memory facilities. */
	if (!IsUnderPostmaster)
		dsm_postmaster_startup(shim);

	/*
	 * Now give loadable modules a chance to set up their shmem allocations
	 */
	if (shmem_startup_hook)
		shmem_startup_hook();
}
