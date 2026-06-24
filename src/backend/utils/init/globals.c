/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"


ProtocolVersion FrontendProtocol;

volatile bool InterruptPending = false;
volatile bool QueryCancelPending = false;
volatile bool ProcDiePending = false;
volatile bool SubThreadErrorPending = false;
volatile bool ClientConnectionLost = false;
volatile bool IdleInTransactionSessionTimeoutPending = false;
volatile bool IdleSessionTimeoutPending = false;
volatile bool PersistentConnectionsTimeoutPending = false;
volatile sig_atomic_t ConfigReloadPending = false;
volatile sig_atomic_t AchieveMemTrackInfoPending = false;
__thread volatile uint32 InterruptHoldoffCount = 0;
__thread volatile uint32 QueryCancelHoldoffCount = 0;
__thread volatile uint32 CritSectionCount = 0;
volatile sig_atomic_t LogMemoryContextPending = false;
volatile sig_atomic_t GetMemoryDetailPending = false;
volatile sig_atomic_t GetMemoryContextDetailPending = false;

/* SyncRepCanceled use to mark whether synchronous replication is canceled */
volatile bool SyncRepCanceled = false;

#ifdef __OPENTENBASE__
__thread volatile int PoolerReloadHoldoffCount = 0;
volatile int PoolerReloadPending = 0;
#endif

int			MyProcPid;
pg_time_t	MyStartTime;
struct Port *MyProcPort;
int32		MyCancelKey;
int			MyPMChildSlot;
pthread_t	MyMainThreadId = 0;
__thread pthread_t MyLocalThreadId = 0;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
struct Latch *MyLatch;

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
char	   *DataDir = NULL;

char		OutputFileName[MAXPGPATH];	/* debugging output file */

char		my_exec_path[MAXPGPATH];	/* full path to my executable */
char		pkglib_path[MAXPGPATH]; /* full path to lib directory */

#ifdef EXEC_BACKEND
char		postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

#ifdef XCP
Oid			MyCoordId = InvalidOid;
char		MyCoordName[NAMEDATALEN];
int 		MyCoordPid = 0;
uint64		MyCoordTimestamp = 0;

BackendId	MyFirstBackendId = InvalidBackendId;
BackendId   MyCoorBackendId = InvalidBackendId;
int32		MyFirstProcPid = InvalidBackendId;
uint64		MyFirstProcStartTime = 0;
#endif

BackendId	MyBackendId = InvalidBackendId;

BackendId	ParallelMasterBackendId = InvalidBackendId;

Oid			MyDatabaseId = InvalidOid;
NameData	MyDatabaseName = {{0}};

Oid			MyDatabaseTableSpace = InvalidOid;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
char	   *DatabasePath = NULL;

pid_t		PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
bool		IsPostmasterEnvironment = false;
bool		IsUnderPostmaster = false;
bool		IsBinaryUpgrade = false;
bool		IsBackgroundWorker = false;

/*
 * Inplace upgrade, reserve version number:
 *     3.16.3 version begin from 100
 *     3.16.4 version begin from 200
 *     5.21.6 version begin from 500
 *     5.21.7.1 version begin from 506
 *     5.21.8.0 version begin from 508
 */
const uint32 GRAND_VERSION_NUM = 550;
volatile uint32 WorkingGrandVersionNum = 550;

bool IsInplaceUpgrade = false;
bool GetSystemOid = true;
int upgrade_mode = NON_UPGRADE;
char *inplace_upgrade_next_system_object_oids = NULL;


#ifdef __AUDIT__
bool 		IsBackendPostgres = false;
#endif

bool		ExitOnAnyError = false;

int			DateStyle = USE_ISO_DATES;
int			DateOrder = DATEORDER_MDY;
int			IntervalStyle = INTSTYLE_POSTGRES;

bool		enableFsync = true;
bool		allowSystemTableMods = false;
int			work_mem = 1024;
int 		work_mem_limit_multiplier = 10;
double		hash_mem_multiplier = 1.0;
int			maintenance_work_mem = 16384;
int			fn_recv_work_mem = 8192;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
int			NBuffers = 1000;
int			MaxConnections = 90;
int			max_worker_processes = 8;
int			max_parallel_workers = 8;
int			MaxBackends = 0;

int			VacuumCostPageHit = 1;	/* GUC parameters for vacuum */
int			VacuumCostPageMiss = 10;
int			VacuumCostPageDirty = 20;
int			VacuumCostLimit = 200;
int			VacuumCostDelay = 0;

int			VacuumPageHit = 0;
int			VacuumPageMiss = 0;
int			VacuumPageDirty = 0;

int			VacuumCostBalance = 0;	/* working state for vacuum */
bool		VacuumCostActive = false;

/* Share-memory structures in forward node */
int         FnNBuffers = 1000;

int 	SMCostBalance = 0;
bool 	SMCostDelayActive = false;
int		SMNumPageHit = 0;
int 	SMNumPageMiss = 0;
int 	SMNumSiloGroupWrite = 0;

#ifdef PGXC
bool useLocalXid = false;
bool tempOid = false;
#endif

#ifdef __OPENTENBASE_C__
int			ColNBuffers = 1000;		/* To be removed, for compatibility. */
bool		IsProcCrash = false;
bool 		IsBackendProc = false;
bool		enable_eager_free = true;
#endif

/* number of shard clusters used to split data in storage */
int shard_cluster_num = 8;

int RowLockTimerIdx = -1;
