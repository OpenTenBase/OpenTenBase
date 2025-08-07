/*-------------------------------------------------------------------------
 *
 * xact.c
 *	  top level transaction system support routines
 *
 * See src/backend/access/transam/README for more information.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/xact.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>
#include <unistd.h>

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "access/gtm.h"
#include "access/mvccvars.h"
/* PGXC_COORD */
#include "gtm/gtm_c.h"
#include "gtm/gtm_gxid.h"
#include "pgxc/execRemote.h"
#include "pgxc/pause.h"
#include "pgxc/nodemgr.h"

/* PGXC_DATANODE */
#include "postmaster/autovacuum.h"
#include "postmaster/clean2pc.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#endif
#include "access/csnlog.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/index.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "libpq/be-fsstubs.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/logical.h"
#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#ifdef XCP
#include "tcop/tcopprot.h"
#endif
#include "utils/bitmap.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/workfile_mgr.h"
#include "pg_trace.h"
#include "pgxc/execRemote.h"
#ifdef __STORAGE_SCALABLE__
#include "replication/logicalworker.h"
#endif

#ifdef XCP
#define implicit2PC_head "_$XC$"
#endif

#ifdef __LICENSE__
#include "license/license.h"
#endif
#ifdef __OPENTENBASE__
#include "access/xlog_internal.h"
#include "pgxc/squeue.h"
#include "commands/extension.h"
#include "utils/xact_whitebox.h"

#endif

#ifdef __OPENTENBASE_C__
#include "executor/execFragment.h"
#include "executor/execDispatchFragment.h"
#include "executor/nodeCtescan.h"
#include "postmaster/bgwriter.h"

#endif

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif
#include "utils/resgroup.h"

#ifdef __TWO_PHASE_TESTS__
#define TWO_PHASE_TEST_NOT_STOP    1
#define TWO_PHASE_TEST_STOP_DN     2
#define TWO_PHASE_TEST_STOP_ALL    3
#endif

#ifdef _PG_ORCL_
#include "access/atxact.h"
#endif

#include "optimizer/spm_cache.h"
#include "storage/ipc.h"

/*
 *	User-tweakable parameters
 */
int			DefaultXactIsoLevel = XACT_READ_COMMITTED;
int			XactIsoLevel;

bool		DefaultXactReadOnly = false;
bool		XactReadOnly;
bool        ReadWithLocalTs = false;

#ifdef __OPENTENBASE__
/* GTM Readonly flag which makes cluster read only */
bool        GTM_ReadOnly = false;
#endif

bool		DefaultXactDeferrable = false;
bool		XactDeferrable;
bool        enable_2pc_optimization = false;
bool		MaybeWriteXact = false;

int			synchronous_commit = SYNCHRONOUS_COMMIT_ON;

bool 		in_plpgsql_analysis = false;

int g_in_plpy_exec_fun = 0;
int g_plsql_excep_sub_level = 0;
extern bool savepoint_define_cmd;
extern bool g_in_rollback_to;
extern bool g_in_release_savepoint;
extern TransactionId pgxc_coordinator_proc_vxid;
/*
 * When running as a parallel worker, we place only a single
 * TransactionStateData on the parallel worker's state stack, and the XID
 * reflected there will be that of the *innermost* currently-active
 * subtransaction in the backend that initiated parallelism.  However,
 * GetTopTransactionId() and TransactionIdIsCurrentTransactionId()
 * need to return the same answers in the parallel worker as they would have
 * in the user backend, so we need some additional bookkeeping.
 *
 * XactTopTransactionId stores the XID of our toplevel transaction, which
 * will be the same as TopTransactionState.transactionId in an ordinary
 * backend; but in a parallel backend, which does not have the entire
 * transaction state, it will instead be copied from the backend that started
 * the parallel operation.
 *
 * nParallelCurrentXids will be 0 and ParallelCurrentXids NULL in an ordinary
 * backend, but in a parallel backend, nParallelCurrentXids will contain the
 * number of XIDs that need to be considered current, and ParallelCurrentXids
 * will contain the XIDs themselves.  This includes all XIDs that were current
 * or sub-committed in the parent at the time the parallel operation began.
 * The XIDs are stored sorted in numerical order (not logical order) to make
 * lookups as fast as possible.
 */
TransactionId XactTopTransactionId = InvalidTransactionId;
int			nParallelCurrentXids = 0;
TransactionId *ParallelCurrentXids;

/*
 * Miscellaneous flag bits to record events which occur on the top level
 * transaction. These flags are only persisted in MyXactFlags and are intended
 * so we remember to do certain things later on in the transaction. This is
 * globally accessible, so can be set from anywhere in the code that requires
 * recording flags.
 */
int			MyXactFlags;

#ifdef _SHARDING_
bool		g_allow_dml_on_datanode = false;
#endif

int	debug_thread_count = 0;
/*
 * If a backend is requesting XID for write. It is same as XactLastRecEnd, but
 * in distributed mode, XactLastRecEnd is not easy to get from datanode. Then
 * we use nRequestedXid to see if it is requesting XID, consider as it's doing
 * writing.
 */
uint64	nRequestedXid = 0;

/*
 * opentenbase_ora Autonomous Transaction Compatibility - CaptureSPMPlan is not allowed to affect the
 * autonomous transaction count
 */
uint64  nRequestedXidSPMUsed = 0;

#ifdef __OPENTENBASE__
extern PGDLLIMPORT int g_in_plpgsql_exec_fun;
extern bool PlpgsqlDebugPrint;
#endif


/*
 *	transaction states - transaction state from server perspective
 */
typedef enum TransState
{
	TRANS_DEFAULT,				/* idle */
	TRANS_START,				/* transaction starting */
	TRANS_INPROGRESS,			/* inside a valid transaction */
	TRANS_COMMIT,				/* commit in progress */
	TRANS_ABORT,				/* abort in progress */
	TRANS_PREPARE				/* prepare in progress */
} TransState;

/*
 *	transaction block states - transaction state of client queries
 *
 * Note: the subtransaction states are used only for non-topmost
 * transactions; the others appear only in the topmost transaction.
 */
typedef enum TBlockState
{
	/* not-in-transaction-block states */
	TBLOCK_DEFAULT,				/* idle */
	TBLOCK_STARTED,				/* running single-query transaction */

	/* transaction block states */
	TBLOCK_BEGIN,				/* starting transaction block */
	TBLOCK_INPROGRESS,			/* live transaction */
	TBLOCK_PARALLEL_INPROGRESS, /* live transaction inside parallel worker */
	TBLOCK_END,					/* COMMIT received */
	TBLOCK_ABORT,				/* failed xact, awaiting ROLLBACK */
	TBLOCK_ABORT_END,			/* failed xact, ROLLBACK received */
	TBLOCK_ABORT_PENDING,		/* live xact, ROLLBACK received */
	TBLOCK_PREPARE,				/* live xact, PREPARE received */

	/* subtransaction states */
	TBLOCK_SUBBEGIN,			/* starting a subtransaction */
	TBLOCK_SUBINPROGRESS,		/* live subtransaction */
	TBLOCK_SUBRELEASE,			/* RELEASE received */
	TBLOCK_SUBCOMMIT,			/* COMMIT received while TBLOCK_SUBINPROGRESS */
	TBLOCK_SUBABORT,			/* failed subxact, awaiting ROLLBACK */
	TBLOCK_SUBABORT_END,		/* failed subxact, ROLLBACK received */
	TBLOCK_SUBABORT_PENDING,	/* live subxact, ROLLBACK received */
	TBLOCK_SUBRESTART,			/* live subxact, ROLLBACK TO received */
	TBLOCK_SUBABORT_RESTART		/* failed subxact, ROLLBACK TO received */
} TBlockState;

typedef enum ImplicitSubState
{
	IMPLICIT_SUB_NON,
	IMPLICIT_SUB,
	IMPLICIT_PLSQL_SUB
} ImplicitSubState;

/*
 *	transaction state structure
 */
typedef struct TransactionStateData
{
#ifdef PGXC  /* PGXC_COORD */
	/* my GXID, or Invalid if none */
	GlobalTransactionId transactionId;
	GlobalTransactionId	topGlobalTransansactionId;
	GlobalTransactionId	auxilliaryTransactionId;
#ifdef __OPENTENBASE__
	bool				isLocalParameterUsed;		/* Check if a local parameter is active
													 * in transaction block (SET LOCAL, DEFERRED) */
#endif
#else
	TransactionId transactionId;	/* my XID, or Invalid if none */
#endif
	SubTransactionId subTransactionId;	/* my subxact ID */
	char	   *name;			/* savepoint name, if any */
	int			savepointLevel; /* savepoint level */
	TransState	state;			/* low-level state */
	TBlockState blockState;		/* high-level state */
	int			nestingLevel;	/* transaction nesting depth */
	int			gucNestLevel;	/* GUC context nesting depth */
	MemoryContext curTransactionContext;	/* my xact-lifetime context */
	ResourceOwner curTransactionOwner;	/* my query resources */
	TransactionId *childXids;	/* subcommitted child XIDs, in XID order */
	int			nChildXids;		/* # of subcommitted child XIDs */
	int			maxChildXids;	/* allocated size of childXids[] */
	Oid			prevUser;		/* previous CurrentUserId setting */
	int			prevSecContext; /* previous SecurityRestrictionContext */
	bool		prevXactReadOnly;	/* entry-time xact r/o state */
	bool		startedInRecovery;	/* did we start in recovery? */
	bool		didLogXid;		/* has xid been included in WAL record? */
	int			parallelModeLevel;	/* Enter/ExitParallelMode counter */
	struct TransactionStateData *parent;	/* back link to parent */
	struct TransactionStateData *child;	/* link to child, so the stack can be traversed from the bottom */
#ifdef XCP
	int				waitedForXidsCount;	/* count of xids we waited to finish */
	TransactionId	*waitedForXids;		/* xids we waited to finish */
#endif
#ifdef __OPENTENBASE__	
	List		*guc_param_list;
	uint64		 max_guc_cid;
#endif
    bool        can_local_commit;     /* distributed mode, current transaction can local commit or not */
#ifdef __OPENTENBASE_C__
	/*
	 * Only allowed to release sub-transaction created in plpgsql, if still in
	 * sub-transaction, any commit/rollback in plpgsql will be failed.
	 */
	int		plpgsql_subtrans;
#endif
	ImplicitSubState implicit_state;
	int 		spi_level;
} TransactionStateData;

typedef TransactionStateData *TransactionState;

/*
 * CurrentTransactionState always points to the current transaction state
 * block.  It will point to TopTransactionStateData when not in a
 * transaction at all, or when in a top-level transaction.
 */
static TransactionStateData TopTransactionStateData = {
	0,							/* global transaction id */
	0,							/* prepared global transaction id */
	0,							/* commit prepared global transaction id */
	0,							/* Check if a local parameter is active
									* in transaction block (SET LOCAL, DEFERRED) */

	0,							/* subtransaction id */
	NULL,						/* savepoint name */
	0,							/* savepoint level */
	TRANS_DEFAULT,				/* transaction state */
	TBLOCK_DEFAULT,				/* transaction block state from the client
								 * perspective */
	0,							/* transaction nesting depth */
	0,							/* GUC context nesting depth */
	NULL,						/* cur transaction context */
	NULL,						/* cur transaction resource owner */
	NULL,						/* subcommitted child Xids */
	0,							/* # of subcommitted child Xids */
	0,							/* allocated size of childXids[] */
	InvalidOid,					/* previous CurrentUserId setting */
	0,							/* previous SecurityRestrictionContext */
	false,						/* entry-time xact r/o state */
	false,						/* startedInRecovery */
	false,						/* didLogXid */
	0,							/* parallelMode */
	NULL,						/* link to parent state block */
	0,
	0,
	NULL,
	NIL,
	false,
	false,
	false
};

/*
 * unreportedXids holds XIDs of all subtransactions that have not yet been
 * reported in an XLOG_XACT_ASSIGNMENT record.
 */
static int	nUnreportedXids;
static TransactionId unreportedXids[PGPROC_MAX_CACHED_SUBXIDS];

static TransactionState CurrentTransactionState = &TopTransactionStateData;

/*
 * The subtransaction ID and command ID assignment counters are global
 * to a whole transaction, so we do not keep them in the state stack.
 */
static SubTransactionId currentSubTransactionId;
static CommandId currentCommandId;
static bool currentCommandIdUsed;
CommandId preCommandId = InvalidCommandId;  /* In the centralized deployment mode,
											 * when the execution plan is pushed down,
											 * ensure that different subplans of a SQL execution plan are bound to a DN node.
											 * This variable is used to identify whether it is the same SQL,
											 * if preCommandId and currentCommandId are equal in a transaction, it is a sql.
											 */

#ifdef PGXC
/*
 * Parameters for communication control of Command ID between Postgres-XC nodes.
 * isCommandIdReceived is used to determine of a command ID has been received by a remote
 * node from a Coordinator.
 * sendCommandId is used to determine if a Postgres-XC node needs to communicate its command ID.
 * This is possible for both remote nodes and Coordinators connected to applications.
 * receivedCommandId is the command ID received on Coordinator from remote node or on remote node
 * from Coordinator.
 */
static bool isCommandIdReceived;
static bool sendCommandId;
static CommandId receivedCommandId;
#endif

/*
 * xactStartTimestamp is the value of transaction_timestamp().
 * stmtStartTimestamp is the value of statement_timestamp().
 * xactStopTimestamp is the time at which we log a commit or abort WAL record.
 * These do not change as we enter and exit subtransactions, so we don't
 * keep them inside the TransactionState stack.
 */
static TimestampTz xactStartTimestamp;
static TimestampTz stmtStartTimestamp;
static TimestampTz xactStopTimestamp;

/*
 * PGXC receives from GTM a timestamp value at the same time as a GXID
 * This one is set as GTMxactStartTimestamp and is a return value of now(), current_transaction().
 * GTMxactStartTimestamp is also sent to each node with gxid and snapshot and delta is calculated locally.
 * GTMdeltaTimestamp is used to calculate current_statement as its value can change
 * during a transaction. Delta can have a different value through the nodes of the cluster
 * but its uniqueness in the cluster is maintained thanks to the global value GTMxactStartTimestamp.
 */
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
static GlobalTimestamp XactGlobalCommitTimestamp = 0;
static GlobalTimestamp XactGlobalPrepareTimestamp = 0;
static GlobalTimestamp XactLocalCommitTimestamp = 0;
static GlobalTimestamp XactLocalPrepareTimestamp = 0;

#endif

#ifdef PGXC
static TimestampTz GTMxactStartTimestamp = 0;
static TimestampTz GTMdeltaTimestamp = 0;
#endif


/*
 * GID to be used for preparing the current transaction.  This is also
 * global to a whole transaction, so we don't keep it in the state stack.
 */
static char *prepareGID;
static char *savePrepareGID;
#ifndef __USE_GLOBAL_SNAPSHOT__
static char *globalXid = NULL;   /* consisting of the local node name and local xid on CN */
static uint64 globalXidVersion = 0;
static pg_atomic_uint32 *globalxidcounter = NULL;
#endif

#ifdef XCP
static char *saveNodeString = NULL;
#endif
static bool XactLocalNodePrepared;
static bool  XactReadLocalNode;
static bool  XactWriteLocalNode;

#ifdef __TWO_PHASE_TRANS__
bool enable_2pc_error_stop = false;
#endif

/*
 * Some commands want to force synchronous commit.
 */
static bool forceSyncCommit = false;

/*
 * Private context for transaction-abort work --- we reserve space for this
 * at startup to ensure that AbortTransaction and AbortSubTransaction can work
 * when we've run out of memory.
 */
static MemoryContext TransactionAbortContext = NULL;

/*
 * List of add-on start- and end-of-xact callbacks
 */
typedef struct XactCallbackItem
{
	struct XactCallbackItem *next;
	XactCallback callback;
	void	   *arg;
} XactCallbackItem;

static XactCallbackItem *Xact_callbacks = NULL;
static XactCallbackItem *Xact_callbacks_once = NULL;

/*
 * List of add-on start- and end-of-subxact callbacks
 */
typedef struct SubXactCallbackItem
{
	struct SubXactCallbackItem *next;
	SubXactCallback callback;
	void	   *arg;
} SubXactCallbackItem;

static SubXactCallbackItem *SubXact_callbacks = NULL;

#ifdef PGXC
/*
 * List of callback items for GTM.
 * Those are called at transaction commit/abort to perform actions
 * on GTM in order to maintain data consistency on GTM with other cluster nodes.
 */
typedef struct GTMCallbackItem
{
	struct GTMCallbackItem *next;
	GTMCallback callback;
	void	   *arg;
} GTMCallbackItem;

static GTMCallbackItem *GTM_callbacks = NULL;
#endif

#ifdef __TWO_PHASE_TRANS__
static bool print_twophase_state(StringInfo errormsg, bool isprint);
#endif

/* local function prototypes */
static void AssignTransactionId(TransactionState s);
static void AbortTransaction(void);
static void AtAbort_Memory(void);
static void AtCleanup_Memory(void);
static void AtAbort_ResourceOwner(void);
static void AtCCI_LocalCache(void);
static void AtCommit_Memory(void);
static void AtStart_Cache(void);
static void AtStart_Memory(void);
static void AtStart_ResourceOwner(void);
static void CallXactCallbacks(XactEvent event);
static void CallXactCallbacksOnce(XactEvent event);
static void CallSubXactCallbacks(SubXactEvent event,
					 SubTransactionId mySubid,
					 SubTransactionId parentSubid);
#ifdef PGXC
static void CleanGTMCallbacks(void);
static void CallGTMCallbacks(GTMEvent event);
#endif
static void CleanupTransaction(void);
static void CheckTransactionChain(bool isTopLevel, bool throwError,
					  const char *stmtType);
static void CommitTransaction(void);
static TransactionId RecordTransactionAbort(bool isSubXact);
static void StartTransaction(void);

static void StartSubTransaction(void);
static void CommitSubTransaction(void);
static void CommitSubTransactionInternal(TransactionState s);
static void AbortSubTransaction(void);
static void AbortSubTransactionInternal(TransactionState s);
static void CleanupSubTransaction(void);
static void PushTransaction(bool savepoint);
static void PopTransaction(void);

static void AtSubAbort_Memory(void);
static void AtSubCleanup_Memory(void);
static void AtSubAbort_ResourceOwner(void);
static void AtSubCommit_Memory(void);
static void AtSubStart_Memory(void);
static void AtSubStart_ResourceOwner(void);

#ifdef XCP
static void AtSubCommit_WaitedXids(void);
static void AtSubAbort_WaitedXids(void);
static void AtEOXact_WaitedXids(void);
static void TransactionRecordXidWait_Internal(TransactionState s,
		TransactionId xid);
#endif

static void ShowTransactionState(const char *str);
static void ShowTransactionStateRec(const char *str, TransactionState state);
static const char *BlockStateAsString(TBlockState blockState);
static const char *TransStateAsString(TransState state);
static void PrepareTransaction(void);
static void AtEOXact_GlobalTxn(bool commit);

LocalTwoPhaseState g_twophase_state;
#ifdef __TWO_PHASE_TESTS__
bool                    complish = false;
int                     twophase_exception_case = 0;
int                     run_pg_clean = 0;
TwophaseTransAt         twophase_in = 0;
EnsureCapacityStack     capacity_stack = 0;
int                     exception_count = 0;
#endif

/* ----------------------------------------------------------------
 *	transaction state accessors
 * ----------------------------------------------------------------
 */

/*
 *	IsTransactionState
 *
 *	This returns true if we are inside a valid transaction; that is,
 *	it is safe to initiate database access, take heavyweight locks, etc.
 */
bool
IsTransactionState(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * TRANS_DEFAULT and TRANS_ABORT are obviously unsafe states.  However, we
	 * also reject the startup/shutdown states TRANS_START, TRANS_COMMIT,
	 * TRANS_PREPARE since it might be too soon or too late within those
	 * transition states to do anything interesting.  Hence, the only "valid"
	 * state is TRANS_INPROGRESS.
	 */
	return (s->state == TRANS_INPROGRESS);
}

bool
IsTransactionAbortState(void)
{
	TransactionState s = CurrentTransactionState;

	return (s->state == TRANS_ABORT);
}

/*
 *	IsTransactionCommit
 *
 *	This returns true if transaction state is TRANS_COMMIT
 */
bool
IsTransactionCommit(void)
{
	return (CurrentTransactionState->state == TRANS_COMMIT);
}

/*
 *	IsAbortedTransactionBlockState
 *
 *	This returns true if we are within an aborted transaction block.
 */
bool
IsAbortedTransactionBlockState(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_ABORT ||
		s->blockState == TBLOCK_SUBABORT)
		return true;

	return false;
}


/*
 *	GetTopTransactionId
 *
 * This will return the XID of the main transaction, assigning one if
 * it's not yet set.  Be careful to call this only inside a valid xact.
 */
TransactionId
GetTopTransactionId(void)
{
	nRequestedXid++;
	if (!TransactionIdIsValid(XactTopTransactionId))
		AssignTransactionId(&TopTransactionStateData);
	elog(DEBUG8, "get transaction xid %d.", XactTopTransactionId);
	return XactTopTransactionId;
}

/*
 *	GetTopTransactionIdIfAny
 *
 * This will return the XID of the main transaction, if one is assigned.
 * It will return InvalidTransactionId if we are not currently inside a
 * transaction, or inside a transaction that hasn't yet been assigned an XID.
 */
TransactionId
GetTopTransactionIdIfAny(void)
{
	return XactTopTransactionId;
}

/*
 *	GetCurrentTransactionId
 *
 * This will return the XID of the current transaction (main or sub
 * transaction), assigning one if it's not yet set.  Be careful to call this
 * only inside a valid xact.
 */
TransactionId
GetCurrentTransactionId(void)
{
	TransactionState s = CurrentTransactionState;

	nRequestedXid++;
#ifdef __USE_GLOBAL_SNAPSHOT__
	/*
	 * Never assign xid to the secondary session, that causes conflicts when
	 * writing to the clog at the transaction end.
	 */
	if (IsConnFromDatanode())
		return GetNextTransactionId();
#endif

	if (!TransactionIdIsValid(s->transactionId))
		AssignTransactionId(s);
	elog(DEBUG8, "get current transaction xid %d.", s->transactionId);
	return s->transactionId;
}

/*
 *	GetCurrentTransactionIdIfAny
 *
 * This will return the XID of the current sub xact, if one is assigned.
 * It will return InvalidTransactionId if we are not currently inside a
 * transaction, or inside a transaction that hasn't been assigned an XID yet.
 */
TransactionId
GetCurrentTransactionIdIfAny(void)
{
	/*
	 * Return XID if its available from the remote node, without assigning to
	 * the transaction state
	 */
	if (IsConnFromDatanode())
		return GetNextTransactionId();

	return CurrentTransactionState->transactionId;
}

/*
 *	MarkCurrentTransactionIdLoggedIfAny
 *
 * Remember that the current xid - if it is assigned - now has been wal logged.
 */
void
MarkCurrentTransactionIdLoggedIfAny(void)
{
	if (TransactionIdIsValid(CurrentTransactionState->transactionId))
		CurrentTransactionState->didLogXid = true;
}


/*
 *	GetStableLatestTransactionId
 *
 * Get the transaction's XID if it has one, else read the next-to-be-assigned
 * XID.  Once we have a value, return that same value for the remainder of the
 * current transaction.  This is meant to provide the reference point for the
 * age(xid) function, but might be useful for other maintenance tasks as well.
 */
TransactionId
GetStableLatestTransactionId(void)
{
	static LocalTransactionId lxid = InvalidLocalTransactionId;
	static TransactionId stablexid = InvalidTransactionId;

	if (lxid != MyProc->lxid)
	{
		lxid = MyProc->lxid;
		stablexid = GetTopTransactionIdIfAny();
		if (!TransactionIdIsValid(stablexid))
			stablexid = ReadNewTransactionId();
	}

	Assert(TransactionIdIsValid(stablexid));

	return stablexid;
}

bool
isXactWriteLocalNode(void)
{
    return XactWriteLocalNode;
}

#ifdef __OPENTENBASE__
/*
 *	GetCurrentLocalParamStatus
 *
 * This will return if current sub xact is using local parameters
 * that may involve pooler session related parameters (SET LOCAL).
 */
bool
GetCurrentLocalParamStatus(void)
{
	return CurrentTransactionState->isLocalParameterUsed;
}
/*
 *	SetCurrentLocalParamStatus
 *
 * This sets local parameter usage for current sub xact.
 */
void
SetCurrentLocalParamStatus(bool status)
{
	CurrentTransactionState->isLocalParameterUsed = status;
}
#endif


#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__

void
GlobalXidShmemInit(void)
{
	bool foundkey = false;

	globalxidcounter = (pg_atomic_uint32 *) ShmemInitStruct("counter used to compose global xid string",
											sizeof(pg_atomic_uint32),
											&foundkey);

	if (!foundkey)
	{
		(void)pg_atomic_exchange_u32(globalxidcounter, 0);
	}
}

static void FreeGlobalXid(void);

static uint64 seq = 0;
static char *
AssignGlobalXidInternal(void)
{

	MemoryContext oldContext = CurrentMemoryContext;
	StringInfoData str;
	int32 node_id;
	uint32 counter;

	oldContext = MemoryContextSwitchTo(TopMemoryContext);
	initStringInfo(&str);


	/*
	 * IMPORTANT:
	 *
	 * In get_node_id normally we check syscache to make sure that our
	 * input is unique universally. However the order of operation
	 * from OSS does not comply with our rules in here, resulting
	 * their operation always failing. To accomendate that we are
	 * asked to change this check part to false, so that OSS can 
	 * work in their way.
	 * But this introduce a obvious risk that the gxid generated are no
	 * longer unique as the node_id they use are not guaranteed to be
	 * unique. That could lead to severe data inconsistency.
	 * Comment out on this so ppl will know.
	 */
	node_id = (int32) get_node_id(PGXCNodeName, false);
	counter = (uint32) pg_atomic_add_fetch_u32(globalxidcounter, 1);
	if (unlikely(counter == UINT32_MAX))
	{
		/*
		 * We don't check for overflow of globalxidcounter, and let
		 * it overflow freely, as normal workflow should not consume
		 * counter faster than them expiring.
		 */
		elog(LOG, "Global xid counter reached UINT32_MAX, rotating back to 0");
	}

	seq++;
	if (seq == PG_UINT64_MAX)
	{
		seq = 0;
	}
	
	appendStringInfo(&str, "%d:%d:%u", node_id, MyProc->pid, counter);

	MemoryContextSwitchTo(oldContext);
	globalXidVersion++;
	if(enable_distri_print)
	{
		elog(LOG, "Assign global xid %s node_id %d, pid %d globalxidcounter %u gxid version "UINT64_FORMAT,
			str.data, node_id, MyProc->pid, counter, globalXidVersion);
	}

	if (strlen(str.data) + 1 > NAMEDATALEN)
	{
		elog(ERROR, "Assigned global xid is too long! Global xid %s node_id %d "
			"pid %d globalxidcounter %u gxid version "UINT64_FORMAT,
			str.data, node_id, MyProc->pid, counter, globalXidVersion);
	}
	
	return str.data;
}



void AssignGlobalXid(void)
{
	if (NULL == globalXid)
	{
		globalXid = AssignGlobalXidInternal();
		StoreLocalGlobalXid(globalXid);
	}
}


char *
GetGlobalXid(void)
{
	if(globalXid == NULL)
		ereport(PANIC,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("global xid is null xid %d version." UINT64_FORMAT,
						GetTopTransactionIdIfAny(), globalXidVersion)));
	return globalXid;
}

char *
GetGlobalXidNoCheck(void)
{
	return globalXid;
}


uint64
GetGlobalXidVersion(void)
{
	return globalXidVersion;
}

const char *
CurrentTransactionBlockStateAsString(void)
{
	TransactionState s = CurrentTransactionState;
	return BlockStateAsString(s->blockState);
}

const char *
CurrentTransactionTransStateAsString(void)
{
	TransactionState s = CurrentTransactionState;
	return TransStateAsString(s->state);
}

void 
SetGlobalXid(const char *globalXidString)
{
	if(globalXid != NULL)
	{
		pfree(globalXid);
		globalXid = NULL;
		globalXidVersion = 0;
		if(enable_distri_print)
		{
			elog(LOG, "local global xid exists %s new %s", globalXid, globalXidString);
		}
	}

	globalXid = MemoryContextStrdup(TopMemoryContext, globalXidString);
	globalXidVersion++;

	if(enable_distri_print)
	{
		elog(LOG, "set global xid %s version " UINT64_FORMAT, globalXidString, globalXidVersion);
	}
}

static void
FreeGlobalXid(void)
{
	if(globalXid)
	{
		if(enable_distri_print)
		{
			elog(LOG, "free global xid %s version "UINT64_FORMAT, globalXid, globalXidVersion);
		}
		
		pfree(globalXid);
		globalXid = NULL;
		globalXidVersion = 0;
	}
}
#endif

/*
 * AssignTransactionId
 *
 * Assigns a new permanent XID to the given TransactionState.
 * We do not assign XIDs to transactions until/unless this is called.
 * Also, any parent TransactionStates that don't yet have XIDs are assigned
 * one; this maintains the invariant that a child transaction has an XID
 * following its parent's.
 */
static void
AssignTransactionId(TransactionState s)
{
	bool		isSubXact = (s->parent != NULL);
	ResourceOwner currentOwner;
	bool		log_unknown_top = false;

	/* Assert that caller didn't screw up */
	Assert(!TransactionIdIsValid(s->transactionId));
	Assert(s->state == TRANS_INPROGRESS);

	if (IS_READONLY_DATANODE && !IS_CENTRALIZED_DATANODE && !proc_exit_inprogress)
		elog(ERROR, "should not execute write on readonly datanode process");

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for new XIDs at this point.
	 */
	if (IsInParallelMode() || IsParallelWorker())
		elog(ERROR, "cannot assign XIDs during a parallel operation");

	/*
	 * Ensure parent(s) have XIDs, so that a child always has an XID later
	 * than its parent.  Musn't recurse here, or we might get a stack overflow
	 * if we're at the bottom of a huge stack of subtransactions none of which
	 * have XIDs yet.
	 */
	if (isSubXact && !TransactionIdIsValid(s->parent->transactionId))
	{
		TransactionState p = s->parent;
		TransactionState *parents;
		size_t		parentOffset = 0;

		parents = palloc(sizeof(TransactionState) * s->nestingLevel);
		while (p != NULL && !TransactionIdIsValid(p->transactionId))
		{
			parents[parentOffset++] = p;
			p = p->parent;
		}

		/*
		 * This is technically a recursive call, but the recursion will never
		 * be more than one layer deep.
		 */
		while (parentOffset != 0)
			AssignTransactionId(parents[--parentOffset]);

		pfree(parents);
	}

	/*
	 * When wal_level=logical, guarantee that a subtransaction's xid can only
	 * be seen in the WAL stream if its toplevel xid has been logged before.
	 * If necessary we log an xact_assignment record with fewer than
	 * PGPROC_MAX_CACHED_SUBXIDS. Note that it is fine if didLogXid isn't set
	 * for a transaction even though it appears in a WAL record, we just might
	 * superfluously log something. That can happen when an xid is included
	 * somewhere inside a wal record, but not in XLogRecord->xl_xid, like in
	 * xl_standby_locks.
	 */
	if (isSubXact && XLogLogicalInfoActive() &&
		!TopTransactionStateData.didLogXid)
		log_unknown_top = true;

	/*
	 * Generate a new Xid and record it in PG_PROC and pg_subtrans.
	 *
	 * NB: we must make the subtrans entry BEFORE the Xid appears anywhere in
	 * shared storage other than PG_PROC; because if there's no room for it in
	 * PG_PROC, the subtrans entry is needed to ensure that other backends see
	 * the Xid as "running".  See GetNewTransactionId.
	 */
#ifdef __USE_GLOBAL_SNAPSHOT__  /* PGXC_COORD */
	{
		GTM_Timestamp	gtm_timestamp;
		bool			received_tp;

		s->transactionId = GetNewTransactionId(isSubXact, &received_tp, &gtm_timestamp);
		if (received_tp)
		{
			if (GTMxactStartTimestamp == 0)
				GTMxactStartTimestamp = (TimestampTz) gtm_timestamp;
			GTMdeltaTimestamp = GTMxactStartTimestamp - stmtStartTimestamp;
		}
	}
#else
	s->transactionId = GetNewTransactionId(isSubXact);
	if (enable_distri_print)
	{
		elog(LOG, "assign xid %d", s->transactionId);
	}
#endif /* PGXC */
	if (!isSubXact){
		XactTopTransactionId = s->transactionId;
	}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if(IS_PGXC_LOCAL_COORDINATOR)
	{
		AssignGlobalXid();
	}
#endif

	if (isSubXact)
        CSNSubTransSetParent(s->transactionId, s->parent->transactionId);

	/*
	 * If it's a top-level transaction, the predicate locking system needs to
	 * be told about it too.
	 */
	if (!isSubXact)
		RegisterPredicateLockingXid(s->transactionId);

	/*
	 * Acquire lock on the transaction XID.  (We assume this cannot block.) We
	 * have to ensure that the lock is assigned to the transaction's own
	 * ResourceOwner.
	 */
	currentOwner = CurrentResourceOwner;
	PG_TRY();
	{
		CurrentResourceOwner = s->curTransactionOwner;
		XactLockTableInsert(s->transactionId);
	}
	PG_CATCH();
	{
		/* Ensure CurrentResourceOwner is restored on error */
		CurrentResourceOwner = currentOwner;
		PG_RE_THROW();
	}
	PG_END_TRY();
	CurrentResourceOwner = currentOwner;

	/*
	 * Every PGPROC_MAX_CACHED_SUBXIDS assigned transaction ids within each
	 * top-level transaction we issue a WAL record for the assignment. We
	 * include the top-level xid and all the subxids that have not yet been
	 * reported using XLOG_XACT_ASSIGNMENT records.
	 *
	 * This is required to limit the amount of shared memory required in a hot
	 * standby server to keep track of in-progress XIDs. See notes for
	 * RecordKnownAssignedTransactionIds().
	 *
	 * We don't keep track of the immediate parent of each subxid, only the
	 * top-level transaction that each subxact belongs to. This is correct in
	 * recovery only because aborted subtransactions are separately WAL
	 * logged.
	 *
	 * This is correct even for the case where several levels above us didn't
	 * have an xid assigned as we recursed up to them beforehand.
	 */
	if (isSubXact && XLogStandbyInfoActive())
	{
		unreportedXids[nUnreportedXids] = s->transactionId;
		nUnreportedXids++;

		/*
		 * ensure this test matches similar one in
		 * RecoverPreparedTransactions()
		 */
		if (nUnreportedXids >= PGPROC_MAX_CACHED_SUBXIDS ||
			log_unknown_top)
		{
			xl_xact_assignment xlrec;

			/*
			 * xtop is always set by now because we recurse up transaction
			 * stack to the highest unassigned xid and then come back down
			 */
			xlrec.xtop = GetTopTransactionId();
			Assert(TransactionIdIsValid(xlrec.xtop));
			xlrec.nsubxacts = nUnreportedXids;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, MinSizeOfXactAssignment);
			XLogRegisterData((char *) unreportedXids,
							 nUnreportedXids * sizeof(TransactionId));

			(void) XLogInsert(RM_XACT_ID, XLOG_XACT_ASSIGNMENT);

			nUnreportedXids = 0;
			/* mark top, not current xact as having been logged */
			TopTransactionStateData.didLogXid = true;
		}
	}
}

GlobalTransactionId
GetTopGlobalTransactionId()
{
	TransactionState s = CurrentTransactionState;
	return s->topGlobalTransansactionId;
}

GlobalTransactionId
GetAuxilliaryTransactionId()
{
	TransactionState s = CurrentTransactionState;
#ifdef __USE_GLOBAL_SNAPSHOT__
	if (!GlobalTransactionIdIsValid(s->auxilliaryTransactionId))
		s->auxilliaryTransactionId = BeginTranGTM(NULL, NULL);
#endif
	return s->auxilliaryTransactionId;
}

void
SetTopGlobalTransactionId(GlobalTransactionId gxid)
{
	TransactionState s = CurrentTransactionState;
	s->topGlobalTransansactionId = gxid;
}

void
SetAuxilliaryTransactionId(GlobalTransactionId gxid)
{
	TransactionState s = CurrentTransactionState;
	s->auxilliaryTransactionId = gxid;
}

/*
 *	GetCurrentSubTransactionId
 */
SubTransactionId
GetCurrentSubTransactionId(void)
{
	TransactionState s = CurrentTransactionState;

	return s->subTransactionId;
}

/*
 *	SubTransactionIsActive
 *
 * Test if the specified subxact ID is still active.  Note caller is
 * responsible for checking whether this ID is relevant to the current xact.
 */
bool
SubTransactionIsActive(SubTransactionId subxid)
{
	TransactionState s;

	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		if (s->state == TRANS_ABORT)
			continue;
		if (s->subTransactionId == subxid)
			return true;
	}
	return false;
}

/*
 *	FindTransactionParent
 *
 * Find xid's transaction parent. if two xid's parent are the same, then
 * they must commit or abort together in either case we can prune this
 * tuple.
 */
TransactionId
FindTransactionParent(TransactionId xid)
{
	TransactionState s;

	/*
	 * readonly backend/parallel workers does not have full transaction
	 * stack which wll lead to misjudge.
	 */
	if (g_twophase_state.is_readonly || IsParallelWorker() || IsInParallelMode())
		return InvalidTransactionId;

	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		int			low,
				high;

		if (s->state == TRANS_ABORT)
			continue;
		if (!TransactionIdIsValid(s->transactionId))
			continue;			/* it can't have any child XIDs either */

		/* active xid can't be optimized for now. we have to consider cmax */
		if (TransactionIdEquals(xid, s->transactionId))
			return InvalidTransactionId;

		/* As the childXids array is ordered, we can use binary search */
		low = 0;
		high = s->nChildXids - 1;
		while (low <= high)
		{
			int			middle;
			TransactionId probe;

			middle = low + (high - low) / 2;
			probe = s->childXids[middle];
			if (TransactionIdEquals(probe, xid))
				return s->transactionId;
			else if (TransactionIdPrecedes(probe, xid))
				low = middle + 1;
			else
				high = middle - 1;
		}
	}

	/* transaction might have aborted or not exist */
	return InvalidTransactionId;
}

/*
 *	GetCurrentCommandId
 *
 * "used" must be TRUE if the caller intends to use the command ID to mark
 * inserted/updated/deleted tuples.  FALSE means the ID is being fetched
 * for read-only purposes (ie, as a snapshot validity cutoff).  See
 * CommandCounterIncrement() for discussion.
 */
CommandId
GetCurrentCommandId(bool used)
{
#ifdef PGXC
	/* If coordinator has sent a command id, remote node should use it */
	if (isCommandIdReceived)
	{
		/*
		 * Indicate to successive calls of this function that the sent command id has
		 * already been used.
		 */
		isCommandIdReceived = false;
		currentCommandId = GetReceivedCommandId();
	}
	else if (IS_PGXC_LOCAL_COORDINATOR)
	{
		/*
		 * If command id reported by remote node is greater that the current
		 * command id, the coordinator needs to use it. This is required because
		 * a remote node can increase the command id sent by the coordinator
		 * e.g. in case a trigger fires at the remote node and inserts some rows
		 * The coordinator should now send the next command id knowing
		 * the largest command id either current or received from remote node.
		 */
		if (GetReceivedCommandId() > currentCommandId)
			currentCommandId = GetReceivedCommandId();
	}
#endif

	/* this is global to a transaction, not subtransaction-local */
	if (used)
	{
		/*
		 * Forbid setting currentCommandIdUsed in a parallel worker, because
		 * we have no provision for communicating this back to the master.  We
		 * could relax this restriction when currentCommandIdUsed was already
		 * true at the start of the parallel operation.
		 */
		// Assert(!IsParallelWorker());
		currentCommandIdUsed = true;
	}
	return currentCommandId;
}

/*
 *	SetCurrentCommandIdUsedForWorker
 *
 * For a parallel worker, record that the currentCommandId has been used.
 * This must only be called at the start of a parallel operation.
 */
void
SetCurrentCommandIdUsedForWorker(void)
{
	Assert(IsParallelWorker() && !currentCommandIdUsed && currentCommandId != InvalidCommandId);
	
	currentCommandIdUsed = true;
}

/*
 *	SetParallelStartTimestamps
 *
 * In a parallel worker, we should inherit the parent transaction's
 * timestamps rather than setting our own.  The parallel worker
 * infrastructure must call this to provide those values before
 * calling StartTransaction() or SetCurrentStatementStartTimestamp().
 */
void
SetParallelStartTimestamps(TimestampTz xact_ts, TimestampTz stmt_ts)
{
	Assert(IsParallelWorker());
	xactStartTimestamp = xact_ts;
	stmtStartTimestamp = stmt_ts;
}

/*
 *	GetCurrentTransactionStartTimestamp
 */
TimestampTz
GetCurrentTransactionStartTimestamp(void)
{
	/*
	 * In Postgres-XC, Transaction start timestamp is the value received
	 * from GTM along with GXID.
	 */
	if (GTMxactStartTimestamp == 0)
		GTMxactStartTimestamp = xactStartTimestamp;
	return GTMxactStartTimestamp;
}

/*
 *	GetCurrentStatementStartTimestamp
 */
TimestampTz
GetCurrentStatementStartTimestamp(void)
{
	/*
	 * For Postgres-XC, Statement start timestamp is adjusted at each node
	 * (Coordinator and Datanode) with a difference value that is calculated
	 * based on the global timestamp value received from GTM and the local
	 * clock. This permits to follow the GTM timeline in the cluster.
	 */

#ifdef __USE_GLOBAL_SNAPSHOT__
	return stmtStartTimestamp + GTMdeltaTimestamp;
#else
	return stmtStartTimestamp;
#endif
}

#ifdef XCP
/*
 *	GetCurrentLocalStatementStartTimestamp
 */
TimestampTz
GetCurrentLocalStatementStartTimestamp(void)
{
	return stmtStartTimestamp;
}
#endif
/*
 *	GetCurrentTransactionStopTimestamp
 *
 * We return current time if the transaction stop time hasn't been set
 * (which can happen if we decide we don't need to log an XLOG record).
 */
TimestampTz
GetCurrentTransactionStopTimestamp(void)
{
	/*
	 * As for Statement start timestamp, stop timestamp has to
	 * be adjusted with the delta value calculated with the
	 * timestamp received from GTM and the local node clock.
	 */
#ifdef __USE_GLOBAL_SNAPSHOT__
	TimestampTz	timestamp;

	if (xactStopTimestamp != 0)
		return xactStopTimestamp + GTMdeltaTimestamp;

	timestamp = GetCurrentTimestamp() + GTMdeltaTimestamp;

	return timestamp;
#else
	if (xactStopTimestamp != 0)
		return xactStopTimestamp;
	return GetCurrentTimestamp();
#endif
}

#ifdef PGXC
TimestampTz
GetCurrentGTMStartTimestamp(void)
{
	if (GTMxactStartTimestamp == 0)
		GTMxactStartTimestamp = xactStartTimestamp;
	return GTMxactStartTimestamp;
}
#endif

/*
 *	SetCurrentStatementStartTimestamp
 *
 * In a parallel worker, this should already have been provided by a call
 * to SetParallelStartTimestamps().
 */
void
SetCurrentStatementStartTimestamp(void)
{
	if (!IsParallelWorker())
		stmtStartTimestamp = GetCurrentTimestamp();
	else
		Assert(stmtStartTimestamp != 0);
}

/*
 *	SetCurrentTransactionStopTimestamp
 */
static inline void
SetCurrentTransactionStopTimestamp(void)
{
	xactStopTimestamp = GetCurrentTimestamp();
}

#ifdef PGXC
/*
 *  SetCurrentGTMDeltaTimestamp
 *
 *  Note: Sets local timestamp delta with the value received from GTM
 */
void
SetCurrentGTMDeltaTimestamp(TimestampTz timestamp)
{
	if (GTMxactStartTimestamp == 0)
		GTMxactStartTimestamp = timestamp;
	GTMdeltaTimestamp = GTMxactStartTimestamp - xactStartTimestamp;
}
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
void 
SetGlobalCommitTimestamp(GlobalTimestamp timestamp)
{
	if(timestamp < XactGlobalPrepareTimestamp)
		elog(ERROR, "prepare timestamp should not lag behind commit timestamp: "
					"prepare "INT64_FORMAT "commit " INT64_FORMAT, XactGlobalPrepareTimestamp, timestamp);
	
	XactGlobalCommitTimestamp = timestamp;
    SetAllSharedCommitTs(timestamp);
}
GlobalTimestamp 
GetGlobalCommitTimestamp(void)
{
	if (IS_CENTRALIZED_MODE)
	{
		return LocalCommitTimestamp;
	}

	return XactGlobalCommitTimestamp;
}

void 
SetGlobalPrepareTimestamp(GlobalTimestamp timestamp)
{
	XactGlobalPrepareTimestamp = timestamp;
    if (!IS_CENTRALIZED_MODE)
        SetSharedLatestCommitTS(timestamp);
}
GlobalTimestamp 
GetGlobalPrepareTimestamp(void)
{
	return XactGlobalPrepareTimestamp;

}

void 
SetLocalCommitTimestamp(GlobalTimestamp timestamp)
{

	XactLocalCommitTimestamp = timestamp;

}
GlobalTimestamp 
GetLocalCommitTimestamp(void)
{
	return XactLocalCommitTimestamp;

}

void 
SetLocalPrepareTimestamp(GlobalTimestamp timestamp)
{

	XactLocalPrepareTimestamp = timestamp;

}
GlobalTimestamp 
GetLocalPrepareTimestamp(void)
{
	return XactLocalPrepareTimestamp;

}

#endif


/*
 *	GetCurrentTransactionNestLevel
 *
 * Note: this will return zero when not inside any transaction, one when
 * inside a top-level transaction, etc.
 */
int
GetCurrentTransactionNestLevel(void)
{
	TransactionState s = CurrentTransactionState;

	return s->nestingLevel;
}


/*
 *	TransactionIdIsCurrentTransactionId
 */
bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	TransactionState s;

	/*
	 * We always say that BootstrapTransactionId is "not my transaction ID"
	 * even when it is (ie, during bootstrap).  Along with the fact that
	 * transam.c always treats BootstrapTransactionId as already committed,
	 * this causes the tqual.c routines to see all tuples as committed, which
	 * is what we need during bootstrap.  (Bootstrap mode only inserts tuples,
	 * it never updates or deletes them, so all tuples can be presumed good
	 * immediately.)
	 *
	 * Likewise, InvalidTransactionId and FrozenTransactionId are certainly
	 * not my transaction ID, so we can just return "false" immediately for
	 * any non-normal XID.
	 */
	if (!TransactionIdIsNormal(xid))
		return false;


	/*
	 * The current TransactionId of secondary datanode session is never
	 * associated with the current transaction, so if it is a secondary
	 * Datanode session look into xid sent from the parent.
	 */
	if (TransactionIdIsCurrentGlobalTransactionId(xid))
		return true;


	/*
	 * In parallel workers, the XIDs we must consider as current are stored in
	 * ParallelCurrentXids rather than the transaction-state stack.  Note that
	 * the XIDs in this array are sorted numerically rather than according to
	 * transactionIdPrecedes order.
	 */
	if (nParallelCurrentXids > 0)
	{
		int			low,
					high;

		low = 0;
		high = nParallelCurrentXids - 1;
		while (low <= high)
		{
			int			middle;
			TransactionId probe;

			middle = low + (high - low) / 2;
			probe = ParallelCurrentXids[middle];
			if (probe == xid)
				return true;
			else if (probe < xid)
				low = middle + 1;
			else
				high = middle - 1;
		}
		return false;
	}

	/*
	 * We will return true for the Xid of the current subtransaction, any of
	 * its subcommitted children, any of its parents, or any of their
	 * previously subcommitted children.  However, a transaction being aborted
	 * is no longer "current", even though it may still have an entry on the
	 * state stack.
	 */
	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		int			low,
					high;

		if (s->state == TRANS_ABORT)
			continue;
		if (!TransactionIdIsValid(s->transactionId))
			continue;			/* it can't have any child XIDs either */
		if (TransactionIdEquals(xid, s->transactionId))
			return true;
		/* As the childXids array is ordered, we can use binary search */
		low = 0;
		high = s->nChildXids - 1;
		while (low <= high)
		{
			int			middle;
			TransactionId probe;

			middle = low + (high - low) / 2;
			probe = s->childXids[middle];
			if (TransactionIdEquals(probe, xid))
				return true;
			else if (TransactionIdPrecedes(probe, xid))
				low = middle + 1;
			else
				high = middle - 1;
		}
	}

	return false;
}

/*
 *	TransactionStartedDuringRecovery
 *
 * Returns true if the current transaction started while recovery was still
 * in progress. Recovery might have ended since so RecoveryInProgress() might
 * return false already.
 */
bool
TransactionStartedDuringRecovery(void)
{
	return CurrentTransactionState->startedInRecovery;
}

/*
 *	EnterParallelMode
 */
void
EnterParallelMode(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parallelModeLevel >= 0);

	++s->parallelModeLevel;
}

/*
 *	ExitParallelMode
 */
void
ExitParallelMode(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parallelModeLevel > 0);
	Assert(s->parallelModeLevel > 1 || !ParallelContextActive());

	--s->parallelModeLevel;
}

/*
 *	IsInParallelMode
 *
 * Are we in a parallel operation, as either the master or a worker?  Check
 * this to prohibit operations that change backend-local state expected to
 * match across all workers.  Mere caches usually don't require such a
 * restriction.  State modified in a strict push/pop fashion, such as the
 * active snapshot stack, is often fine.
 */
bool
IsInParallelMode(void)
{
	return CurrentTransactionState->parallelModeLevel != 0;
}

/*
 *	PrepareParallelModePlanExec
 *
 * Prepare for entering parallel mode plan execution, based on command-type.
 */
void
PrepareParallelModePlanExec(CmdType commandType)
{
	if (IsParallelWorker())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					errmsg("must not in parallel worker")));
	
	if (IsModifySupportedInParallelMode(commandType))
	{
		Assert(!IsInParallelMode());

		/*
		 * Prepare for entering parallel mode by assigning a TransactionId.
		 * Failure to do this now would result in heap_insert() subsequently
		 * attempting to assign a TransactionId whilst in parallel-mode, which
		 * is not allowed.
		 *
		 * This approach has a disadvantage in that if the underlying SELECT
		 * does not return any rows, then the TransactionId is not used,
		 * however that shouldn't happen in practice in many cases.
		 */
		(void) GetCurrentTransactionId();
		(void) GetCurrentCommandId(true);
	}
}

/*
 *	CommandCounterIncrementInternal
 *
 * Increment cid, flush_immediately = true means flush cid to coordinator
 * immediately, otherwise, cid will be flushed to coordinator along with other
 * commands subsequentyly.
 */
static void
CommandCounterIncrementInternal(bool flush_immediately)
{
	/*
	 * If the current value of the command counter hasn't been "used" to mark
	 * tuples, we need not increment it, since there's no need to distinguish
	 * a read-only command from others.  This helps postpone command counter
	 * overflow, and keeps no-op CommandCounterIncrement operations cheap.
	 */
	if (currentCommandIdUsed)
	{
		/*
		 * Workers synchronize transaction state at the beginning of each
		 * parallel operation, so we can't account for new commands after that
		 * point.
		 */
		if (IsInParallelMode() || IsParallelWorker())
			elog(ERROR, "cannot start commands during a parallel operation");

		currentCommandId += 1;
		if (currentCommandId == InvalidCommandId)
		{
			currentCommandId -= 1;
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("cannot have more than 2^32-2 commands in a transaction")));
		}
		currentCommandIdUsed = false;

		/* Propagate new command ID into static snapshots */
		SnapshotSetCommandId(currentCommandId);

#ifdef PGXC
		/*
		 * Remote node should report local command id changes only if
		 * required by the Coordinator. The requirement of the
		 * Coordinator is inferred from the fact that Coordinator
		 * has itself sent the command id to the remote nodes.
		 */
		if (IsConnFromCoord() && IsSendCommandId())
			ReportCommandIdChange(currentCommandId, flush_immediately);
#endif

		/*
		 * Make any catalog changes done by the just-completed command visible
		 * in the local syscache.  We obviously don't need to do this after a
		 * read-only command.  (But see hacks in inval.c to make real sure we
		 * don't think a command that queued inval messages was read-only.)
		 */
		AtCCI_LocalCache();
	}
}

/*
 *	CommandCounterIncrement
 *
 *  Invoke CommandCounterIncrementInternal, flush cid immediately.
 */
void
CommandCounterIncrement(void)
{
	CommandCounterIncrementInternal(true);
}

/*
 *	CommandCounterIncrementNotFlushCid
 *
 *  Invoke CommandCounterIncrementInternal, not flush cid immediately.
 */
void
CommandCounterIncrementNotFlushCid(void)
{
	CommandCounterIncrementInternal(false);
}

/*
 * ForceSyncCommit
 *
 * Interface routine to allow commands to force a synchronous commit of the
 * current top-level transaction
 */
void
ForceSyncCommit(void)
{
	forceSyncCommit = true;
}


/* ----------------------------------------------------------------
 *						StartTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	AtStart_Cache
 */
static void
AtStart_Cache(void)
{
	AcceptInvalidationMessages();
}

/*
 *	AtStart_Memory
 */
static void
AtStart_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * If this is the first time through, create a private context for
	 * AbortTransaction to work in.  By reserving some space now, we can
	 * insulate AbortTransaction from out-of-memory scenarios.  Like
	 * ErrorContext, we set it up with slow growth rate and a nonzero minimum
	 * size, so that space will be reserved immediately.
	 */
	if (TransactionAbortContext == NULL)
		TransactionAbortContext =
			AllocSetContextCreateExtended(TopMemoryContext,
										  "TransactionAbortContext",
										  32 * 1024,
										  32 * 1024,
										  32 * 1024);

	/*
	 * We shouldn't have a transaction context already.
	 */
	Assert(TopTransactionContext == NULL);

	/*
	 * Create a toplevel context for the transaction.
	 */
	TopTransactionContext =
		AllocSetContextCreate(TopMemoryContext,
							  "TopTransactionContext",
							  ALLOCSET_DEFAULT_SIZES);
	InitTransInvalInfo();

	/*
	 * In a top-level transaction, CurTransactionContext is the same as
	 * TopTransactionContext.
	 */
	CurTransactionContext = TopTransactionContext;
	s->curTransactionContext = CurTransactionContext;

	/* Make the CurTransactionContext active. */
	MemoryContextSwitchTo(CurTransactionContext);
}

/*
 *	AtStart_ResourceOwner
 */
static void
AtStart_ResourceOwner(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * We shouldn't have a transaction resource owner already.
	 */
	Assert(TopTransactionResourceOwner == NULL);

	/*
	 * Create a toplevel resource owner for the transaction.
	 */
	s->curTransactionOwner = ResourceOwnerCreate(NULL, "TopTransaction");

	TopTransactionResourceOwner = s->curTransactionOwner;
	CurTransactionResourceOwner = s->curTransactionOwner;
	CurrentResourceOwner = s->curTransactionOwner;
}

/* ----------------------------------------------------------------
 *						StartSubTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 * AtSubStart_Memory
 */
static void
AtSubStart_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(CurTransactionContext != NULL);

	/*
	 * Create a CurTransactionContext, which will be used to hold data that
	 * survives subtransaction commit but disappears on subtransaction abort.
	 * We make it a child of the immediate parent's CurTransactionContext.
	 */
	CurTransactionContext = AllocSetContextCreate(CurTransactionContext,
												  "CurTransactionContext",
												  ALLOCSET_DEFAULT_SIZES);
	s->curTransactionContext = CurTransactionContext;

	/* Make the CurTransactionContext active. */
	MemoryContextSwitchTo(CurTransactionContext);
}

/*
 * AtSubStart_ResourceOwner
 */
static void
AtSubStart_ResourceOwner(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parent != NULL);

	/*
	 * Create a resource owner for the subtransaction.  We make it a child of
	 * the immediate parent's resource owner.
	 */
	s->curTransactionOwner =
		ResourceOwnerCreate(s->parent->curTransactionOwner,
							"SubTransaction");

	CurTransactionResourceOwner = s->curTransactionOwner;
	CurrentResourceOwner = s->curTransactionOwner;
}

/* ----------------------------------------------------------------
 *						CommitTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	RecordTransactionCommit
 *
 * Returns latest XID among xact and its children, or InvalidTransactionId
 * if the xact has no XID.  (We compute that here just because it's easier.)
 *
 * If you change this function, see RecordTransactionCommitPrepared also.
 */
static TransactionId
RecordTransactionCommit(void)
{
	TransactionId xid = GetTopTransactionIdIfAny();
	bool		markXidCommitted = TransactionIdIsValid(xid);
	TransactionId latestXid = InvalidTransactionId;
	int			nrels;
	RelFileNodeNFork *rels;
	int			nchildren;
	TransactionId *children;
	int			nmsgs = 0;
	SharedInvalidationMessage *invalMessages = NULL;
	bool		RelcacheInitFileInval = false;
	bool		wrote_xlog;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	CommitSeqNo global_committs = InvalidCommitSeqNo;
#endif
	if(enable_distri_print)
	{
		elog(LOG, "record commit transaction xid %d", xid);
	}
	/* Get data needed for commit record */
	nrels = smgrGetPendingDeletes(true, &rels);
	nchildren = xactGetCommittedChildren(&children);
	if (XLogStandbyInfoActive())
		nmsgs = xactGetCommittedInvalidationMessages(&invalMessages,
													 &RelcacheInitFileInval);
	wrote_xlog = (XactLastRecEnd != 0);

	/*
	 * If we haven't been assigned an XID yet, we neither can, nor do we want
	 * to write a COMMIT record.
	 */
	if (!markXidCommitted)
	{
		/*
		 * We expect that every smgrscheduleunlink is followed by a catalog
		 * update, and hence XID assignment, so we shouldn't get here with any
		 * pending deletes.  Use a real test not just an Assert to check this,
		 * since it's a bit fragile.
		 */
		if (nrels != 0)
			elog(ERROR, "cannot commit a transaction that deleted files but has no xid");

		/* Can't have child XIDs either; AssignTransactionId enforces this */
		Assert(nchildren == 0);

		/*
		 * Transactions without an assigned xid can contain invalidation
		 * messages (e.g. explicit relcache invalidations or catcache
		 * invalidations for inplace updates); standbys need to process those.
		 * We can't emit a commit record without an xid, and we don't want to
		 * force assigning an xid, because that'd be problematic for e.g.
		 * vacuum.  Hence we emit a bespoke record for the invalidations. We
		 * don't want to use that in case a commit record is emitted, so they
		 * happen synchronously with commits (besides not wanting to emit more
		 * WAL recoreds).
		 */
		if (nmsgs != 0)
		{
			LogStandbyInvalidations(nmsgs, invalMessages,
									RelcacheInitFileInval);
			wrote_xlog = true;	/* not strictly necessary */
		}

		/*
		 * If we didn't create XLOG entries, we're done here; otherwise we
		 * should trigger flushing those entries the same as a commit record
		 * would.  This will primarily happen for HOT pruning and the like; we
		 * want these to be flushed to disk in due time.
		 */
		if (!wrote_xlog)
			goto cleanup;
	}
	else
	{
		bool		replorigin;

		/*
		 * Are we using the replication origins feature?  Or, in other words,
		 * are we replaying remote actions?
		 */
		replorigin = (replorigin_session_origin != InvalidRepOriginId &&
					  replorigin_session_origin != DoNotReplicateId);

		/*
		 * Begin commit critical section and insert the commit XLOG record.
		 */
		/* Tell bufmgr and smgr to prepare for commit */
		BufmgrCommit();

		/*
		 * Mark ourselves as within our "commit critical section".  This
		 * forces any concurrent checkpoint to wait until we've updated
		 * pg_xact.  Without this, it is possible for the checkpoint to set
		 * REDO after the XLOG record but fail to flush the pg_xact update to
		 * disk, leading to loss of the transaction commit if the system
		 * crashes a little later.
		 *
		 * Note: we could, but don't bother to, set this flag in
		 * RecordTransactionAbort.  That's because loss of a transaction abort
		 * is noncritical; the presumption would be that it aborted, anyway.
		 *
		 * It's safe to change the delayChkpt flag of our own backend without
		 * holding the ProcArrayLock, since we're the only one modifying it.
		 * This makes checkpoint's determination of which xacts are delayChkpt
		 * a bit fuzzy, but it doesn't matter.
		 */

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
		if(IsPostmasterEnvironment && IS_PGXC_DATANODE && !IsLogicalWorker() && !AmOpenTenBaseSubscriptionApplyWorker() 
		    && !IsCurrentTransactionCanLocalCommit()) 
		{
            CommitSeqNo prepareTs;

            prepareTs = GetSharedMaxCommitTs();
            CSNLogSetCSN(xid, 0, NULL, InvalidXLogRecPtr, false, MASK_PREPARE_BIT(prepareTs));
			if(delay_before_acquire_committs)
			{
				pg_usleep(delay_before_acquire_committs);
			}
            global_committs = CSNLogAssignCSN(xid, nchildren, children, false);
	
			if(!GlobalTimestampIsValid(global_committs)){
				elog(ERROR, "failed to get global timestamp for commit command");
			}
			if(enable_distri_print)
			{
				elog(LOG, "commit procno %d xid %d, csn " INT64_FORMAT, MyProc->pgprocno, xid, global_committs);
			}	

			if(delay_after_acquire_committs)
			{
				pg_usleep(delay_after_acquire_committs);
			}
			
		}
		else
		{
			
			global_committs = LocalCommitTimestamp;
			if(enable_distri_print)
			{
                elog(LOG, "local commit procno %d xid %d, csn " INT64_FORMAT, MyProc->pgprocno, xid, global_committs);
			}
		}
		
		Assert((MyPgXact->delayChkpt & DELAY_CHKPT_START) == 0);
		START_CRIT_SECTION();
		MyPgXact->delayChkpt |= DELAY_CHKPT_START;

		SetCurrentTransactionStopTimestamp();

        if (!IS_CENTRALIZED_MODE)
            SetSharedMaxCommitTs(global_committs);
        
		MyProc->commitTs = global_committs;
		XactLogCommitRecord(global_committs,
						xactStopTimestamp + GTMdeltaTimestamp,
						nchildren, children, nrels, rels,
						nmsgs, invalMessages,
						RelcacheInitFileInval, forceSyncCommit,
						MyXactFlags,
						InvalidTransactionId /* plain commit */ );
#else
        Assert((MyPgXact->delayChkpt & DELAY_CHKPT_START) == 0);
		START_CRIT_SECTION();
		MyPgXact->delayChkpt |= DELAY_CHKPT_START;

		SetCurrentTransactionStopTimestamp();
		
		XactLogCommitRecord(InvalidGlobalTimestamp,
					xactStopTimestamp + GTMdeltaTimestamp,
					nchildren, children, nrels, rels,
					nmsgs, invalMessages,
					RelcacheInitFileInval, forceSyncCommit,
					MyXactFlags,
					InvalidTransactionId /* plain commit */ );
#endif

		


		if (replorigin)
			/* Move LSNs forward for this replication origin */
			replorigin_session_advance(replorigin_session_origin_lsn,
									   XactLastRecEnd);

		/*
		 * Record commit timestamp.  The value comes from plain commit
		 * timestamp if there's no replication origin; otherwise, the
		 * timestamp was already set in replorigin_session_origin_timestamp by
		 * replication.
		 *
		 * We don't need to WAL-log anything here, as the commit record
		 * written above already contains the data.
		 */

		if (!replorigin || replorigin_session_origin_timestamp == 0)
			replorigin_session_origin_timestamp = xactStopTimestamp;

        TransactionTreeSetCommitTsData(xid, nchildren, children,
                                       global_committs,
                                       replorigin_session_origin_timestamp,
                                       replorigin_session_origin, false, InvalidXLogRecPtr);
#ifndef __SUPPORT_DISTRIBUTED_TRANSACTION__
		TransactionTreeSetCommitTsData(xid, nchildren, children,
									   InvalidGlobalTimestamp,
									   replorigin_session_origin_timestamp,
									   replorigin_session_origin, false, InvalidXLogRecPtr);
#endif
	}

	/*
	 * Check if we want to commit asynchronously.  We can allow the XLOG flush
	 * to happen asynchronously if synchronous_commit=off, or if the current
	 * transaction has not performed any WAL-logged operation or didn't assign
	 * an xid.  The transaction can end up not writing any WAL, even if it has
	 * an xid, if it only wrote to temporary and/or unlogged tables.  It can
	 * end up having written WAL without an xid if it did HOT pruning.  In
	 * case of a crash, the loss of such a transaction will be irrelevant;
	 * temp tables will be lost anyway, unlogged tables will be truncated and
	 * HOT pruning will be done again later. (Given the foregoing, you might
	 * think that it would be unnecessary to emit the XLOG record at all in
	 * this case, but we don't currently try to do that.  It would certainly
	 * cause problems at least in Hot Standby mode, where the
	 * KnownAssignedXids machinery requires tracking every XID assignment.  It
	 * might be OK to skip it only when wal_level < replica, but for now we
	 * don't.)
	 *
	 * However, if we're doing cleanup of any non-temp rels or committing any
	 * command that wanted to force sync commit, then we must flush XLOG
	 * immediately.  (We must not allow asynchronous commit if there are any
	 * non-temp tables to be deleted, because we might delete the files before
	 * the COMMIT record is flushed to disk.  We do allow asynchronous commit
	 * if all to-be-deleted tables are temporary though, since they are lost
	 * anyway if we crash.)
	 */
	if ((wrote_xlog && markXidCommitted &&
		 synchronous_commit > SYNCHRONOUS_COMMIT_OFF) ||
		forceSyncCommit || nrels > 0)
	{
		XLogFlush(XactLastRecEnd);

        /*
         * Wait for synchronous replication, if required. Similar to the decision
         * above about using committing asynchronously we only want to wait if
         * this backend assigned an xid and wrote WAL.  No need to wait if an xid
         * was assigned due to temporary/unlogged tables or due to HOT pruning.
         *
         * Note that at this stage we should not mark clog and tlog (keep the 
         * transaction invisible), and the xid is running in the procarray and continue 
         * to hold locks.
         */
        if (wrote_xlog && markXidCommitted)
        {
            SyncRepWaitForLSN(XactLastRecEnd, true);
            if (SyncRepCanceled)
            {
                SyncRepCanceled = false;
                elog(WARNING, "The waiting for synchronous replication is canceled. "
                              "The commit xlog has already committed locally, "
                              "but might not have been replicated to the standby.");
            }
        }
        
		/*
		 * Now we may update the CLOG, if we wrote a COMMIT record above
		 */
		if (markXidCommitted)
		{

			TransactionIdCommitTree(xid, nchildren, children, global_committs);
            if (enable_distri_print)
                elog(LOG, "commit record xid %d csn " UINT64_FORMAT, xid, global_committs);
		}
	}
	else
	{
		/*
		 * Asynchronous commit case:
		 *
		 * This enables possible committed transaction loss in the case of a
		 * postmaster crash because WAL buffers are left unwritten. Ideally we
		 * could issue the WAL write without the fsync, but some
		 * wal_sync_methods do not allow separate write/fsync.
		 *
		 * Report the latest async commit LSN, so that the WAL writer knows to
		 * flush this commit.
		 */
		XLogSetAsyncXactLSN(XactLastRecEnd);

		/*
		 * We must not immediately update the CLOG, since we didn't flush the
		 * XLOG. Instead, we store the LSN up to which the XLOG must be
		 * flushed before the CLOG may be updated.
		 */
		if (markXidCommitted)
		{
			TransactionIdAsyncCommitTree(xid, nchildren, children, XactLastRecEnd, global_committs);
            
            if (enable_distri_print)
                elog(LOG, "commit record xid %d csn %lu lsn" UINT64_FORMAT, xid, global_committs, XactLastRecEnd);
		}
	}

	if(enable_distri_debug)
	{
		SetLocalCommitTimestamp(GetCurrentTimestamp());
	}

	/*
	 * If we entered a commit critical section, leave it now, and let
	 * checkpoints proceed.
	 */
	if (markXidCommitted)
	{
		MyPgXact->delayChkpt &= ~DELAY_CHKPT_START;
		END_CRIT_SECTION();
	}

	/* Compute latestXid while we have the child XIDs handy */
	latestXid = TransactionIdLatest(xid, nchildren, children);
    
	/* remember end of last commit record */
	XactLastCommitEnd = XactLastRecEnd;

	/* Reset XactLastRecEnd until the next transaction writes something */
	XactLastRecEnd = 0;

cleanup:
	/* Clean up local data */
	if (rels)
		pfree(rels);

	return latestXid;
}


/*
 *	AtCCI_LocalCache
 */
static void
AtCCI_LocalCache(void)
{
	/*
	 * Make any pending relation map changes visible.  We must do this before
	 * processing local sinval messages, so that the map changes will get
	 * reflected into the relcache when relcache invals are processed.
	 */
	AtCCI_RelationMap();

	/*
	 * Make catalog changes visible to me for the next command.
	 */
	CommandEndInvalidationMessages();
}

/*
 *	AtCommit_Memory
 */
static void
AtCommit_Memory(void)
{
	/*
	 * Now that we're "out" of a transaction, have the system allocate things
	 * in the top memory context instead of per-transaction contexts.
	 */
	MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Release all transaction-local memory.
	 */
	Assert(TopTransactionContext != NULL);
	MemoryContextDelete(TopTransactionContext);
	TopTransactionContext = NULL;
	CurTransactionContext = NULL;
	CurrentTransactionState->curTransactionContext = NULL;
}

#ifdef PGXC
/*
 *	CleanGTMCallbacks
 */
static void
CleanGTMCallbacks(void)
{
	/*
	 * The transaction is done, TopTransactionContext as well as the GTM callback items
	 * are already cleaned, so we need here only to reset the GTM callback pointer properly.
	 */
	GTM_callbacks = NULL;
}
#endif

/* ----------------------------------------------------------------
 *						CommitSubTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 * AtSubCommit_Memory
 */
static void
AtSubCommit_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parent != NULL);

	/* Return to parent transaction level's memory context. */
	CurTransactionContext = s->parent->curTransactionContext;
	MemoryContextSwitchTo(CurTransactionContext);

	/*
	 * Ordinarily we cannot throw away the child's CurTransactionContext,
	 * since the data it contains will be needed at upper commit.  However, if
	 * there isn't actually anything in it, we can throw it away.  This avoids
	 * a small memory leak in the common case of "trivial" subxacts.
	 */
	if (MemoryContextIsEmpty(s->curTransactionContext))
	{
		MemoryContextDelete(s->curTransactionContext);
		s->curTransactionContext = NULL;
	}
}

/*
 * AtSubCommit_childXids
 *
 * Pass my own XID and my child XIDs up to my parent as committed children.
 */
static void
AtSubCommit_childXids(void)
{
	TransactionState s = CurrentTransactionState;
	int			new_nChildXids;

	Assert(s->parent != NULL);

	/*
	 * The parent childXids array will need to hold my XID and all my
	 * childXids, in addition to the XIDs already there.
	 */
	new_nChildXids = s->parent->nChildXids + s->nChildXids + 1;

	/* Allocate or enlarge the parent array if necessary */
	if (s->parent->maxChildXids < new_nChildXids)
	{
		int			new_maxChildXids;
		TransactionId *new_childXids;

		/*
		 * Make it 2x what's needed right now, to avoid having to enlarge it
		 * repeatedly. But we can't go above MaxAllocSize.  (The latter limit
		 * is what ensures that we don't need to worry about integer overflow
		 * here or in the calculation of new_nChildXids.)
		 */
		new_maxChildXids = Min(new_nChildXids * 2,
							   (int) (MaxAllocSize / sizeof(TransactionId)));

		if (new_maxChildXids < new_nChildXids)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("maximum number of committed subtransactions (%d) exceeded",
							(int) (MaxAllocSize / sizeof(TransactionId)))));

		/*
		 * We keep the child-XID arrays in TopTransactionContext; this avoids
		 * setting up child-transaction contexts for what might be just a few
		 * bytes of grandchild XIDs.
		 */
		if (s->parent->childXids == NULL)
			new_childXids =
				MemoryContextAlloc(TopTransactionContext,
								   new_maxChildXids * sizeof(TransactionId));
		else
			new_childXids = repalloc(s->parent->childXids,
									 new_maxChildXids * sizeof(TransactionId));

		s->parent->childXids = new_childXids;
		s->parent->maxChildXids = new_maxChildXids;
	}

	/*
	 * Copy all my XIDs to parent's array.
	 *
	 * Note: We rely on the fact that the XID of a child always follows that
	 * of its parent.  By copying the XID of this subtransaction before the
	 * XIDs of its children, we ensure that the array stays ordered. Likewise,
	 * all XIDs already in the array belong to subtransactions started and
	 * subcommitted before us, so their XIDs must precede ours.
	 */
	s->parent->childXids[s->parent->nChildXids] = s->transactionId;

	if (s->nChildXids > 0)
		memcpy(&s->parent->childXids[s->parent->nChildXids + 1],
			   s->childXids,
			   s->nChildXids * sizeof(TransactionId));

	s->parent->nChildXids = new_nChildXids;

	/* Release child's array to avoid leakage */
	if (s->childXids != NULL)
		pfree(s->childXids);
	/* We must reset these to avoid double-free if fail later in commit */
	s->childXids = NULL;
	s->nChildXids = 0;
	s->maxChildXids = 0;
}

/* ----------------------------------------------------------------
 *						AbortTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	RecordTransactionAbort
 *
 * Returns latest XID among xact and its children, or InvalidTransactionId
 * if the xact has no XID.  (We compute that here just because it's easier.)
 */
static TransactionId
RecordTransactionAbort(bool isSubXact)
{
	TransactionId xid = GetCurrentTransactionIdIfAny();
	TransactionId latestXid;
	int			nrels;
	RelFileNodeNFork *rels;
	int			nchildren;
	TransactionId *children;
	TimestampTz xact_time;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	GlobalTimestamp global_timestamp;
#endif


	/*
	 * The recording of sub-transaction state can only be done by the top
	 * process;  and any other process attempting to do so may cause data
	 * consistency issues.
	 */
	if (isSubXact && IsConnFromDatanode())
		return InvalidTransactionId;

	/*
	 * If we haven't been assigned an XID, nobody will care whether we aborted
	 * or not.  Hence, we're done in that case.  It does not matter if we have
	 * rels to delete (note that this routine is not responsible for actually
	 * deleting 'em).  We cannot have any child XIDs, either.
	 */
	if (!TransactionIdIsValid(xid))
	{
		/* Reset XactLastRecEnd until the next transaction writes something */
		if (!isSubXact)
			XactLastRecEnd = 0;
		return InvalidTransactionId;
	}

	/*
	 * We have a valid XID, so we should write an ABORT record for it.
	 *
	 * We do not flush XLOG to disk here, since the default assumption after a
	 * crash would be that we aborted, anyway.  For the same reason, we don't
	 * need to worry about interlocking against checkpoint start.
	 */

	/*
	 * Check that we haven't aborted halfway through RecordTransactionCommit.
	 */
	if (TransactionIdDidCommit(xid))
		elog(PANIC, "cannot abort transaction %u, it was already committed",
			 xid);

	/* Fetch the data we need for the abort record */
	nrels = smgrGetPendingDeletes(false, &rels);
	nchildren = xactGetCommittedChildren(&children);

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	global_timestamp = GetGlobalTimestampLocal();
	
	if(!GlobalTimestampIsValid(global_timestamp))
	{
		elog(WARNING, "failed to get global timestamp for abort command");
	}
	MyProc->commitTs = global_timestamp;
#endif

	/* XXX do we really need a critical section here? */
	START_CRIT_SECTION();

	/* Write the ABORT record */
	if (isSubXact)
		xact_time = GetCurrentTimestamp();
	else
	{
		SetCurrentTransactionStopTimestamp();
		xact_time = xactStopTimestamp + GTMdeltaTimestamp;
	}
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	XactLogAbortRecord(global_timestamp,
						xact_time,
					   nchildren, children,
					   nrels, rels,
					   MyXactFlags, InvalidTransactionId);

#else
	XactLogAbortRecord(InvalidGlobalTimestamp,
						xact_time,
					   nchildren, children,
					   nrels, rels,
					   MyXactFlags, InvalidTransactionId);
#endif

	/*
	 * Report the latest async abort LSN, so that the WAL writer knows to
	 * flush this abort. There's nothing to be gained by delaying this, since
	 * WALWriter may as well do this when it can. This is important with
	 * streaming replication because if we don't flush WAL regularly we will
	 * find that large aborts leave us with a long backlog for when commits
	 * occur after the abort, increasing our window of data loss should
	 * problems occur at that point.
	 */
	if (!isSubXact)
		XLogSetAsyncXactLSN(XactLastRecEnd);

    if (nrels > 0)
    {
        XLogFlush(XactLastRecEnd);
    }
	/*
	 * Mark the transaction aborted in clog.  This is not absolutely necessary
	 * but we may as well do it while we are here; also, in the subxact case
	 * it is helpful because XactLockTableWait makes use of it to avoid
	 * waiting for already-aborted subtransactions.  It is OK to do it without
	 * having flushed the ABORT record to disk, because in event of a crash
	 * we'd be assumed to have aborted anyway.
	 */
	TransactionIdAbortTree(xid, nchildren, children);

	END_CRIT_SECTION();

	/* Compute latestXid while we have the child XIDs handy */
	latestXid = TransactionIdLatest(xid, nchildren, children);

	/*
	 * If we're aborting a subtransaction, we can immediately remove failed
	 * XIDs from PGPROC's cache of running child XIDs.  We do that here for
	 * subxacts, because we already have the child XID array at hand.  For
	 * main xacts, the equivalent happens just after this function returns.
	 */
	if (isSubXact)
		XidCacheRemoveRunningXids(xid, nchildren, children, latestXid);

	/* Reset XactLastRecEnd until the next transaction writes something */
	if (!isSubXact)
		XactLastRecEnd = 0;

	/* And clean up local data */
	if (rels)
		pfree(rels);

	return latestXid;
}

/*
 *	AtAbort_Memory
 */
static void
AtAbort_Memory(void)
{
	/*
	 * Switch into TransactionAbortContext, which should have some free space
	 * even if nothing else does.  We'll work in this context until we've
	 * finished cleaning up.
	 *
	 * It is barely possible to get here when we've not been able to create
	 * TransactionAbortContext yet; if so use TopMemoryContext.
	 */
	if (TransactionAbortContext != NULL)
		MemoryContextSwitchTo(TransactionAbortContext);
	else
		MemoryContextSwitchTo(TopMemoryContext);
}

/*
 * AtSubAbort_Memory
 */
static void
AtSubAbort_Memory(void)
{
	Assert(TransactionAbortContext != NULL);

	MemoryContextSwitchTo(TransactionAbortContext);
}


/*
 *	AtAbort_ResourceOwner
 */
static void
AtAbort_ResourceOwner(void)
{
	/*
	 * Make sure we have a valid ResourceOwner, if possible (else it will be
	 * NULL, which is OK)
	 */
	CurrentResourceOwner = TopTransactionResourceOwner;
}

/*
 * AtSubAbort_ResourceOwner
 */
static void
AtSubAbort_ResourceOwner(void)
{
	TransactionState s = CurrentTransactionState;

	/* Make sure we have a valid ResourceOwner */
	CurrentResourceOwner = s->curTransactionOwner;
}


/*
 * AtSubAbort_childXids
 */
static void
AtSubAbort_childXids(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * We keep the child-XID arrays in TopTransactionContext (see
	 * AtSubCommit_childXids).  This means we'd better free the array
	 * explicitly at abort to avoid leakage.
	 */
	if (s->childXids != NULL)
		pfree(s->childXids);
	s->childXids = NULL;
	s->nChildXids = 0;
	s->maxChildXids = 0;

	/*
	 * We could prune the unreportedXids array here. But we don't bother. That
	 * would potentially reduce number of XLOG_XACT_ASSIGNMENT records but it
	 * would likely introduce more CPU time into the more common paths, so we
	 * choose not to do that.
	 */
}

/* ----------------------------------------------------------------
 *						CleanupTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 *	AtCleanup_Memory
 */
static void
AtCleanup_Memory(void)
{
	Assert(CurrentTransactionState->parent == NULL);

	/*
	 * Now that we're "out" of a transaction, have the system allocate things
	 * in the top memory context instead of per-transaction contexts.
	 */
	MemoryContextSwitchTo(TopMemoryContext);

	/*
	 * Clear the special abort context for next time.
	 */
	if (TransactionAbortContext != NULL)
		MemoryContextResetAndDeleteChildren(TransactionAbortContext);

	/*
	 * Release all transaction-local memory.
	 */
	if (TopTransactionContext != NULL)
		MemoryContextDelete(TopTransactionContext);
	TopTransactionContext = NULL;
	CurTransactionContext = NULL;
	CurrentTransactionState->curTransactionContext = NULL;
}


/* ----------------------------------------------------------------
 *						CleanupSubTransaction stuff
 * ----------------------------------------------------------------
 */

/*
 * AtSubCleanup_Memory
 */
static void
AtSubCleanup_Memory(void)
{
	TransactionState s = CurrentTransactionState;

	Assert(s->parent != NULL);

	/* Make sure we're not in an about-to-be-deleted context */
	MemoryContextSwitchTo(s->parent->curTransactionContext);
	CurTransactionContext = s->parent->curTransactionContext;

	/*
	 * Clear the special abort context for next time.
	 */
	if (TransactionAbortContext != NULL)
		MemoryContextResetAndDeleteChildren(TransactionAbortContext);

	/*
	 * Delete the subxact local memory contexts. Its CurTransactionContext can
	 * go too (note this also kills CurTransactionContexts from any children
	 * of the subxact).
	 */
	if (s->curTransactionContext)
		MemoryContextDelete(s->curTransactionContext);
	s->curTransactionContext = NULL;
}

/* ----------------------------------------------------------------
 *						interface routines
 * ----------------------------------------------------------------
 */

/*
 *	StartTransaction
 */
static void
StartTransaction(void)
{
	TransactionState s;
	VirtualTransactionId vxid;

	/*
	 * Let's just make sure the state stack is empty
	 */
	s = &TopTransactionStateData;
	CurrentTransactionState = s;

	Assert(XactTopTransactionId == InvalidTransactionId);

	/*
	 * Start a new transaction in PLpgSQL, will not reset SecContextIsSet. It
	 * assumes that we can not start a global transaction at down-stream node.
	 */
	if (ORA_MODE && !IsConnFromApp())
		SecContextIsSet = false;

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_DEFAULT)
		elog(WARNING, "StartTransaction while in %s state",
			 TransStateAsString(s->state));

	/*
	 * set the current transaction state information appropriately during
	 * start processing
	 */
	s->state = TRANS_START;
	s->transactionId = InvalidTransactionId;	/* until assigned */
	/*
	 * Make sure we've reset xact state variables
	 *
	 * If recovery is still in progress, mark this transaction as read-only.
	 * We have lower level defences in XLogInsert and elsewhere to stop us
	 * from modifying data during recovery, but this gives the normal
	 * indication to the user that the transaction is read-only.
	 */
	if (RecoveryInProgress())
	{
		s->startedInRecovery = true;
		XactReadOnly = true;
	}
	else
	{
		s->startedInRecovery = false;
		XactReadOnly = DefaultXactReadOnly && !IsInplaceUpgrade;
#ifdef __LICENSE__
#ifndef NOLIC
		if(IS_PGXC_COORDINATOR)
		{
			if(!IsLicenseValid() || IsLicenseExpired())
			{
				elog(WARNING, "license expired or did not exist, cluster can only be read now.");
				XactReadOnly |= true;
			}
		}
#endif
#endif
#ifdef PGXC
		/* Save Postgres-XC session as read-only if necessary */
		XactReadOnly |= IsPGXCNodeXactReadOnly();
#endif
        XactReadOnly |= ReadWithLocalTs;
	}
	XactDeferrable = DefaultXactDeferrable;
#ifdef PGXC
	/* PGXCTODO - PGXC doesn't support 9.1 serializable transactions. They are
	 * silently turned into repeatable-reads which is same as pre 9.1
	 * serializable isolation level
	 */
	if (DefaultXactIsoLevel == XACT_SERIALIZABLE)
		DefaultXactIsoLevel = XACT_REPEATABLE_READ;
#endif
	XactIsoLevel = DefaultXactIsoLevel;
	forceSyncCommit = false;
	XactLocalNodePrepared = false;
	MyXactFlags = 0;

	/*
	 * reinitialize within-transaction counters
	 */
	s->subTransactionId = TopSubTransactionId;
	currentSubTransactionId = TopSubTransactionId;
	currentCommandIdUsed = false;
	currentCommandId = FirstCommandId;
#ifdef PGXC
	if (!IS_READONLY_DATANODE)
	{
		/*
		 * Parameters related to global command ID control for transaction.
		 * Send the 1st command ID.
		 */
		isCommandIdReceived = false;
		if (IsConnFromCoord())
		{
			SetReceivedCommandId(FirstCommandId);
			SetSendCommandId(false);
		}
	}
#endif
	/*
	 * initialize reported xid accounting
	 */
	nUnreportedXids = 0;
	s->didLogXid = false;

	/*
	 * must initialize resource-management stuff first
	 */
	AtStart_Memory();
	AtStart_ResourceOwner();

	/*
	 * Assign a new LocalTransactionId, and combine it with the backendId to
	 * form a virtual transaction id.
	 */
	vxid.backendId = MyBackendId;
	vxid.localTransactionId = GetNextLocalTransactionId();

	/*
	 * Lock the virtual transaction id before we announce it in the proc array
	 */
	VirtualXactLockTableInsert(vxid);
	/*
	 * Advertise it in the proc array.  We assume assignment of
	 * LocalTransactionID is atomic, and the backendId should be set already.
	 */
	Assert(MyProc->backendId == vxid.backendId);
	MyProc->lxid = vxid.localTransactionId;

	TRACE_POSTGRESQL_TRANSACTION_START(vxid.localTransactionId);

	/*
     * set transaction_timestamp() (a/k/a now()).  Normally, we want this to
     * be the same as the first command's statement_timestamp(), so don't do a
     * fresh GetCurrentTimestamp() call (which'd be expensive anyway).  But
     * for transactions started inside procedures (i.e., nonatomic SPI
     * contexts), we do need to advance the timestamp.  Also, in a parallel
     * worker, the timestamp should already have been provided by a call to
     * SetParallelStartTimestamps().
	 */
    if (!IsParallelWorker())
    {
        if (!SPI_inside_nonatomic_context())
            xactStartTimestamp = stmtStartTimestamp;
        else
            xactStartTimestamp = GetCurrentTimestamp();
    }
    else
        Assert(xactStartTimestamp != 0);
#ifdef PGXC
	/* For Postgres-XC, transaction start timestamp has to follow the GTM timeline */
	pgstat_report_xact_timestamp(GTMxactStartTimestamp ?
			GTMxactStartTimestamp :
			xactStartTimestamp);
#else
	pgstat_report_xact_timestamp(xactStartTimestamp);
#endif
    /* Mark xactStopTimestamp as unset. */
    xactStopTimestamp = 0;

	/*
	 * initialize current transaction state fields
	 *
	 * note: prevXactReadOnly is not used at the outermost level
	 */
	s->nestingLevel = 1;
	s->gucNestLevel = 1;
	s->childXids = NULL;
	s->nChildXids = 0;
	s->maxChildXids = 0;
	s->can_local_commit = false;
	GetUserIdAndSecContext(&s->prevUser, &s->prevSecContext);
	/* SecurityRestrictionContext should never be set outside a transaction */
	Assert((ORA_MODE && !IsConnFromApp()) || s->prevSecContext == 0 || enable_secfunc_xact);
	/*
	 * initialize other subsystems for new transaction
	 */
	AtStart_GUC();
	AtStart_Cache();
	AfterTriggerBeginXact();

	AtStart_Remote();
	/*
	 * done with start processing, set current transaction state to "in
	 * progress"
	 */
	s->state = TRANS_INPROGRESS;

#ifdef _PG_ORCL_
	s->plpgsql_subtrans = 0;
	s->implicit_state = IMPLICIT_SUB_NON;
#endif

	ShowTransactionState("StartTransaction");
	if (IS_PGXC_COORDINATOR && IsConnFromApp())
		RefreshQueryId();
}


/*
 *	CommitTransaction
 *
 * NB: if you change this routine, better look at PrepareTransaction too!
 */
static void
CommitTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	TransactionId latestXid;
	bool		is_parallel_worker;
	bool		is_read_only = g_twophase_state.is_readonly;

	if (!SPI_get_internal_xact()
	)
		stmt_level_rollback = false;

	if(enable_distri_print)
	{
		elog(LOG, "Commit Transaction.");
	}

#ifdef __OPENTENBASE__
    disable_timeout_safely();
#endif

	xact_update_relstat_attstat();
	is_parallel_worker = (s->blockState == TBLOCK_PARALLEL_INPROGRESS);

	/* Enforce parallel mode restrictions during parallel worker commit. */
	if (is_parallel_worker)
		EnterParallelMode();

	ShowTransactionState("CommitTransaction");

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "CommitTransaction while in %s state",
			 TransStateAsString(s->state));
	Assert(s->parent == NULL);
	/*
	 * Do pre-commit processing that involves calling user-defined code, such
	 * as triggers.  Since closing cursors could queue trigger actions,
	 * triggers could open cursors, etc, we have to keep looping until there's
	 * nothing left to do.
	 */
	for (;;)
	{
		/*
		 * Fire all currently pending deferred triggers.
		 */
		AfterTriggerFireDeferred();

		/*
		 * Close open portals (converting holdable ones into static portals).
		 * If there weren't any, we are done ... otherwise loop back to check
		 * if they queued deferred triggers.  Lather, rinse, repeat.
		 */
		if (!PreCommit_Portals(false))
			break;
	}

	AtEOXact_SetUser(true);
	/*
	 * Insert notifications sent by NOTIFY commands into the queue.  This
	 * should be late in the pre-commit sequence to minimize time spent
	 * holding the notify-insertion lock.
	 *
	 * XXX XL: Since PreCommit_Notify may assign transaction ID for a
	 * transaction which till now doesn't have one, we want to process this
	 * before doing any XL specific transaction handling
	 */
	PreCommit_Notify();

#ifdef PGXC
	/*
	 * If we are a Coordinator and currently serving the client,
	 * we must run a 2PC if more than one nodes are involved in this
	 * transaction. We first prepare on the remote nodes and if everything goes
	 * right, we commit locally and then commit on the remote nodes. We must
	 * also be careful to prepare locally on this Coordinator only if the
	 * local Coordinator has done some write activity.
	 *
	 * If there are any errors, they will be reported via ereport and the
	 * transaction will be aborted.
	 *
	 * First save the current top transaction ID before it may get overwritten
	 * by PrepareTransaction below. We must not reset topGlobalTransansactionId
	 * until we are done with finishing the transaction
	 */
	s->topGlobalTransansactionId = s->transactionId;
	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		XactLocalNodePrepared = false;
		if (savePrepareGID)
		{
			pfree(savePrepareGID);
			savePrepareGID = NULL;
		}

#ifdef XCP
		if (saveNodeString)
		{
			pfree(saveNodeString);
			saveNodeString = NULL;
		}
#endif
		/*
		 * If the local node has done some write activity, prepare the local node
		 * first. If that fails, the transaction is aborted on all the remote
		 * nodes
		 */
		/*
		 * Fired OnCommit actions would fail 2PC process
		 */
		if (IsTwoPhaseCommitRequired(XactWriteLocalNode))
		{

			prepareGID = GetImplicit2PCGID(implicit2PC_head, XactWriteLocalNode);
			savePrepareGID = MemoryContextStrdup(TopMemoryContext, prepareGID);

			if (XactWriteLocalNode)
			{
				/*
				 * OK, local node is involved in the transaction. Prepare the
				 * local transaction now. Errors will be reported via ereport
				 * and that will lead to transaction abortion.
				 */
				Assert(GlobalTransactionIdIsValid(s->topGlobalTransansactionId));
				if(enable_distri_print)
				{
					elog(LOG, "implicit prepare xid %d.", GetTopTransactionIdIfAny());
				}
				PrepareTransaction();

				DBUG_EXECUTE_IF("resgroup_dn_down_2pc", {
					int i = 0;
					for (i = 0; i < 10; i++)
					{
						pg_usleep(1000000);
					}
				});

				s->blockState = TBLOCK_DEFAULT;

				/*
				 * PrepareTransaction would have ended the current transaction.
				 * Start a new transaction. We can also use the GXID of this
				 * new transaction to run the COMMIT/ROLLBACK PREPARED
				 * commands. Note that information as part of the
				 * auxilliaryTransactionId
				 */
				StartTransaction();
				XactLocalNodePrepared = true;
				s->auxilliaryTransactionId = GetTopTransactionId();
				if (SyncRepCanceled)
				{
					SyncRepCanceled = false;
					elog(ERROR, "The waiting for synchronous replication is canceled. "
						"The prepare transaction xlog has already committed locally, "
						"but might not have been replicated to the standby.");
				}
			}
			else
			{
				s->auxilliaryTransactionId = InvalidGlobalTransactionId;
				PrePrepare_Remote(prepareGID, false, true);
				remove_2pc_records(prepareGID, true);
			}
		}
	}
#endif

	/*
	 * The remaining actions cannot call any user-defined code, so it's safe
	 * to start shutting down within-transaction services.  But note that most
	 * of this stuff could still throw an error, which would switch us into
	 * the transaction-abort path.
	 */

	CallXactCallbacks(is_parallel_worker ? XACT_EVENT_PARALLEL_PRE_COMMIT
					  : XACT_EVENT_PRE_COMMIT);

	/* If we might have parallel workers, clean them up now. */
	if (IsInParallelMode())
		AtEOXact_Parallel(true);

	/* Shut down the deferred-trigger manager */
	AfterTriggerEndXact(true);

	/*
	 * Let ON COMMIT management do its thing (must happen after closing
	 * cursors, to avoid dangling-reference problems)
	 */
	PreCommit_on_commit_actions();

	/* close large objects before lower-level cleanup */
	AtEOXact_LargeObject(true);

	/*
	 * Mark serializable transaction as complete for predicate locking
	 * purposes.  This should be done as late as we can put it and still allow
	 * errors to be raised for failure patterns found at commit.
	 */
	PreCommit_CheckForSerializationFailure();

	pgxc_node_clean_all_handles_distribute_msg();
#ifdef PGXC
	if (IS_PGXC_DATANODE || !IsConnFromCoord())
	{
		/*
		 * Now run 2PC on the remote nodes. Any errors will be reported via
		 * ereport and we will run error recovery as part of AbortTransaction
		 */
		PreCommit_Remote(savePrepareGID, saveNodeString, XactLocalNodePrepared);
		/*
		 * Now that all the remote nodes have successfully prepared and
		 * commited, commit the local transaction as well. Remember, any errors
		 * before this point would have been reported via ereport. The fact
		 * that we are here shows that the transaction has been committed
		 * successfully on the remote nodes
		 */
		if (XactLocalNodePrepared)
		{
			XactLocalNodePrepared = false;
			PreventTransactionChain(true, "COMMIT IMPLICIT PREPARED");
			FinishPreparedTransaction(savePrepareGID, true);
		}

		/*
		 * The current transaction may have been ended and we might have
		 * started a new transaction. Re-initialize with
		 * CurrentTransactionState
		 */
		s = CurrentTransactionState;

		/*
		 * Callback on GTM if necessary, this needs to be done before HOLD_INTERRUPTS
		 * as this is not a part of the end of transaction processing involving clean up.
		 */
		CallGTMCallbacks(GTM_EVENT_COMMIT);

		/*
		 * Let the normal commit processing now handle the main transaction if
		 * the local node was not involved. Otherwise, we are in an
		 * auxilliary transaction and that will be closed along with the main
		 * transaction
		 */
	}

#endif

#ifdef USE_WHITEBOX_INJECTION
	if (TransactionIdIsValid(GetTopTransactionIdIfAny()))
		(void)whitebox_trigger_generic(AFTER_REMOTE_COMMIT_FAILED, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, NULL, NULL);
#endif

	/* do not allow to handle signal/interrupt */
	HOLD_INTERRUPTS();

#ifdef XCP
	AtEOXact_WaitedXids();
#endif

	/* Commit updates to the relation map --- do this as late as possible */
	AtEOXact_RelationMap(true);

#ifdef _MIGRATE_
	/*shard map will be refreshed at user sql command*/
	InvalidateShmemShardMap(true);
#endif

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	/*
	 * check gtm connection before set the current transaction state to commit,
	 * to avoid heap_open after transaction commit.
	 */
	if (!is_parallel_worker && !IS_CENTRALIZED_MODE)
	{
		if (!IsConnFromDatanode())
		{
			TransactionId xid = GetTopTransactionIdIfAny();

			if (TransactionIdIsValid(xid))
			{
				if(IsPostmasterEnvironment && IS_DISTRIBUTED_DATANODE && !IsLogicalWorker())
				{
					CheckGTMConnection();
				}
			}
		}
	}
#endif

	/*
	 * set the current transaction state information appropriately during
	 * commit processing
	 */
	s->state = TRANS_COMMIT;
	s->parallelModeLevel = 0;

	if (!is_parallel_worker)
	{
		/*
		 * We need to mark our XIDs as committed in pg_xact.  This is where we
		 * durably commit.
		 */
#ifdef XCP
		latestXid = InvalidTransactionId;
		if (!IsConnFromDatanode())
#endif
			latestXid = RecordTransactionCommit();

#ifdef __OPENTENBASE__
		FinishSeqOp(true);
#endif
	}
	else
	{
		/*
		 * We must not mark our XID committed; the parallel master is
		 * responsible for that.
		 */
		latestXid = InvalidTransactionId;

		/*
		 * Make sure the master will know about any WAL we wrote before it
		 * commits.
		 */
		ParallelWorkerReportLastRecEnd(XactLastRecEnd);
	}

	CommitSPMPendingList();

#ifdef USE_WHITEBOX_INJECTION
	if (TransactionIdIsValid(GetTopTransactionIdIfAny()))
		(void)whitebox_trigger_generic(AFTER_ALL_COMMIT_FAILED, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, NULL, NULL);
#endif

	TRACE_POSTGRESQL_TRANSACTION_COMMIT(MyProc->lxid);

	/*
	 * Let others know about no transaction in progress by me. Note that this
	 * must be done _before_ releasing locks we hold and _after_
	 * RecordTransactionCommit.
	 */
	ProcArrayEndTransaction(MyProc, latestXid);

	/*
	 * This is all post-commit cleanup.  Note that if an error is raised here,
	 * it's too late to abort the transaction.  This should be just
	 * noncritical resource releasing.
	 *
	 * The ordering of operations is not entirely random.  The idea is:
	 * release resources visible to other backends (eg, files, buffer pins);
	 * then release locks; then release backend-local resources. We want to
	 * release locks at the point where any backend waiting for us will see
	 * our transaction as being fully cleaned up.
	 *
	 * Resources that can be associated with individual queries are handled by
	 * the ResourceOwner mechanism.  The other calls here are for backend-wide
	 * state.
	 */

	CallXactCallbacks(is_parallel_worker ? XACT_EVENT_PARALLEL_COMMIT
					  : XACT_EVENT_COMMIT);
	CallXactCallbacksOnce(XACT_EVENT_COMMIT);

#ifdef PGXC
	/*
	 * Call any callback functions initialized for post-commit cleaning up
	 * of database/tablespace operations. Mostly this should involve resetting
	 * the abort callback functions registered during the db/tbspc operations.
	 */
	AtEOXact_DBCleanup(true);
#endif

	AtEOXact_SHMGTMPrimaryInfo(true);
	
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_PORTAL,
						 true, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, true);

	/* Check we've released all buffer pins */
	AtEOXact_Buffers(true);

	/* Clean up the relation cache */
	AtEOXact_RelationCache(true);

	/*
	 * Make catalog changes visible to all backends.  This has to happen after
	 * relcache references are dropped (see comments for
	 * AtEOXact_RelationCache), but before locks are released (if anyone is
	 * waiting for lock on a relation we've modified, we want them to know
	 * about the catalog change before they start using the relation).
	 */
	AtEOXact_Inval(true);

	AtEOXact_MultiXact();

#ifdef XCP
	/* If the cluster lock was held at commit time, keep it locked! */
	if (cluster_ex_lock_held)
	{
		elog(DEBUG2, "PAUSE CLUSTER still held at commit");
		/*if (IS_PGXC_LOCAL_COORDINATOR)
			RequestClusterPause(false, NULL);*/
	}
#endif

	/*
	 * End the transaction on the GTM before releasing the locks. This would
	 * ensure that any other backend which might be waiting for our locks,
	 * would see end of the global transaction before acquiring the locks.
	 *
	 * XXX Earlier we used to do this at the very end of this function. But
	 * that leads to various issues (such as "tuple concurrently updated"
	 * errors in concurrent ANALYZE). It seems like a good idea to end the
	 * global transaction before releasing the locks from correctness
	 * perspective. But we are now doing network calls while holding
	 * interrupts. That can lead to some unpleasant hangs. So lets be wary
	 * about this possibility
	 */

	AtEOXact_GlobalTxn(true);

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, true);

	/*
	 * Likewise, dropping of files deleted during the transaction is best done
	 * after releasing relcache and buffer pins.  (This is not strictly
	 * necessary during commit, since such pins should have been released
	 * already, but this ordering is definitely critical during abort.)  Since
	 * this may take many seconds, also delay until after releasing locks.
	 * Other backends will observe the attendant catalog changes and not
	 * attempt to access affected files.
	 */
	smgrDoPendingDeletes(true);

	AtCommit_Notify();
	AtEOXact_DisGuc(true, 1);
	AtEOXact_GUC(true, 1);
	AtEOXact_SPI(true);
	AtEOXact_on_commit_actions(true);
	AtEOXact_Namespace(true, is_parallel_worker);
	AtEOXact_SMgr();
	AtEOXact_Files();
	AtEOXact_HashTables(true);
	AtEOXact_PgStat(true);
	AtEOXact_Snapshot(true, false);
	AtEOXact_ApplyLauncher(true);
	AtEOXact_WorkFile();
	pgstat_report_xact_timestamp(0);

	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(TopTransactionResourceOwner);
	s->curTransactionOwner = NULL;
	CurTransactionResourceOwner = NULL;
	TopTransactionResourceOwner = NULL;	

	AtCommit_Memory();
#ifdef PGXC
	/* Clean up GTM callbacks at the end of transaction */
	CleanGTMCallbacks();
#endif

	s->transactionId = InvalidTransactionId;
	s->subTransactionId = InvalidSubTransactionId;
	s->nestingLevel = 0;
	s->gucNestLevel = 0;
	s->childXids = NULL;
	s->nChildXids = 0;
	s->maxChildXids = 0;

#ifdef PGXC
	ForgetTransactionLocalNode();

	/*
	 * Set the command ID of Coordinator to be sent to the remote nodes
	 * as the 1st one.
	 * For remote nodes, enforce the command ID sending flag to false to avoid
	 * sending any command ID by default as now transaction is done.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
		SetReceivedCommandId(FirstCommandId);
	else
		SetSendCommandId(false);
#endif
	XactTopTransactionId = InvalidTransactionId;
	nParallelCurrentXids = 0;

	nRequestedXid = 0;
	nRequestedXidSPMUsed = 0;
	g_plsql_excep_sub_level = 0;

	/*
	 * done with commit processing, set current transaction state back to
	 * default
	 */
	s->state = TRANS_DEFAULT;
	s->can_local_commit = false;

	RESUME_INTERRUPTS();
    /* clean timeout status */
    if (IsQueryCancelPending())
    {
        QueryCancelPending =  false;
        InterruptPending  = false;
    }  

#ifdef PGXC
	AtEOXact_Remote();
	GTMxactStartTimestamp = 0;
#endif
#ifdef __OPENTENBASE__
	AtEOXact_Global();
	ClearLocalTwoPhaseState();
	g_twophase_state.is_readonly = is_read_only;
#endif

	if (xactRelStatHash != NULL)
	{
		hash_destroy(xactRelStatHash);
		xactRelStatHash = NULL;
		clean_relstat_list();
	}

	if (analyzeRelHash != NULL)
	{
		hash_destroy(analyzeRelHash);
		analyzeRelHash = NULL;
	}

	if (IS_PGXC_DATANODE && !is_read_only)
	{
		uint32	TotalProcs = TOTALPROCNUMS;
		LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
		MemSet(MyProc->read_proc_map, 0, bitmap_size(TotalProcs));
		LWLockRelease(&MyProc->globalxidLock);
	}

	if (debug_thread_count != 0
	 )
		elog(PANIC, "there is %d unfinished tuple queue thread",
			 debug_thread_count);
#ifdef _PG_ORCL_
	s->plpgsql_subtrans = 0;
	s->implicit_state = IMPLICIT_SUB_NON;
#endif
}

/*
 * Mark the end of global transaction. This is called at the end of the commit
 * or abort processing when the local and remote transactions have been either
 * committed or aborted and we just need to close the transaction on the GTM.
 * Obviously, we don't call this at the PREPARE time because the GXIDs must not
 * be closed at the GTM until the transaction finishes
 */
static void
AtEOXact_GlobalTxn(bool commit)
{
	TransactionState s = CurrentTransactionState;


	s->topGlobalTransansactionId = InvalidGlobalTransactionId;
	s->auxilliaryTransactionId = InvalidGlobalTransactionId;
	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		if (s->waitedForXids)
			pfree(s->waitedForXids);
	}
	s->waitedForXids = NULL;
	s->waitedForXidsCount = 0;
#ifndef __SUPPORT_DISTRIBUTED_TRANSACTION__
	SetNextTransactionId(InvalidTransactionId);
#endif
	FreeGlobalXid();
	return;

}
void
AtEOXact_Global(void)
{
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	XactGlobalCommitTimestamp = InvalidGlobalTimestamp;
	XactGlobalPrepareTimestamp = InvalidGlobalTimestamp;
	XactLocalCommitTimestamp = InvalidGlobalTimestamp;
	XactLocalPrepareTimestamp = InvalidGlobalTimestamp;
	is_distri_report = false;
#endif

}


/*
 *	PrepareTransaction
 *
 * NB: if you change this routine, better look at CommitTransaction too!
 */
/*
 * Only a Postgres-XC Coordinator that received a PREPARE Command from
 * an application can use this special prepare.
 * If PrepareTransaction is called during an implicit 2PC, do not release ressources,
 * this is made by CommitTransaction when transaction has been committed on Nodes.
 */
static void
PrepareTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	TransactionId xid = GetCurrentTransactionId();
	GlobalTransaction gxact;
	TimestampTz prepared_at;
#ifdef PGXC
	bool		isImplicit = !(s->blockState == TBLOCK_PREPARE);
#endif

	Assert(!IsInParallelMode());
	if (!SPI_get_internal_xact()
	 )
		stmt_level_rollback = false;

	ShowTransactionState("PrepareTransaction");
	if(enable_distri_print)
	{
		elog(LOG, "prepare transaction xid %d.", xid);
	}

	/*
	 * check the current transaction state
	 */
	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "PrepareTransaction while in %s state",
			 TransStateAsString(s->state));
	Assert(s->parent == NULL);

	/*
	 * Do pre-commit processing that involves calling user-defined code, such
	 * as triggers.  Since closing cursors could queue trigger actions,
	 * triggers could open cursors, etc, we have to keep looping until there's
	 * nothing left to do.
	 */
	for (;;)
	{
		/*
		 * Fire all currently pending deferred triggers.
		 */
		AfterTriggerFireDeferred();

		/*
		 * Close open portals (converting holdable ones into static portals).
		 * If there weren't any, we are done ... otherwise loop back to check
		 * if they queued deferred triggers.  Lather, rinse, repeat.
		 */
		
		if (!PreCommit_Portals(true))
			break;
	}

	AtEOXact_SetUser(true);
	/* Verify there aren't locks of both xact and session level */
	CheckForSessionAndXactLocks();
#ifdef XCP
	/*
	 * Remote nodes must be done AFTER portals. If portal is still active it may
	 * need to send down a message to close remote objects on Datanode, but
	 * PrePrepare_Remote releases connections to remote nodes.
	 */
	if (IS_PGXC_DATANODE || !IsConnFromCoord())
	{
		char		*nodestring;
		if (saveNodeString)
		{
			pfree(saveNodeString);
			saveNodeString = NULL;
		}

		/* Needed in PrePrepare_Remote to submit nodes to GTM */
		s->topGlobalTransansactionId = s->transactionId;
		if (savePrepareGID)
			pfree(savePrepareGID);
		savePrepareGID = MemoryContextStrdup(TopMemoryContext, prepareGID);
#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(LOCAL_PREPARED_FAILED_A, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, g_twophase_state.gid, NULL);
#endif

		nodestring = PrePrepare_Remote(savePrepareGID, XactWriteLocalNode, isImplicit);
		if (nodestring)
		{
			saveNodeString = MemoryContextStrdup(TopMemoryContext, nodestring);
			if(enable_distri_print)
			{
				elog(LOG, "saveNodeString %s preparegid %s", saveNodeString, savePrepareGID);
			}
		}

#ifdef USE_WHITEBOX_INJECTION
		(void)whitebox_trigger_generic(LOCAL_PREPARED_FAILED_B, WHITEBOX_TRIGGER_DEFAULT,
			INJECTION_LOCATION, g_twophase_state.gid, NULL);
#endif
		/*
		 * Callback on GTM if necessary, this needs to be done before HOLD_INTERRUPTS
		 * as this is not a part of the end of transaction processing involving clean up.
		 */
		CallGTMCallbacks(GTM_EVENT_PREPARE);
	}
#endif
	CallXactCallbacks(XACT_EVENT_PRE_PREPARE);

	/*
	 * The remaining actions cannot call any user-defined code, so it's safe
	 * to start shutting down within-transaction services.  But note that most
	 * of this stuff could still throw an error, which would switch us into
	 * the transaction-abort path.
	 */

	/* Shut down the deferred-trigger manager */
	AfterTriggerEndXact(true);

	/*
	 * Let ON COMMIT management do its thing (must happen after closing
	 * cursors, to avoid dangling-reference problems)
	 */
	PreCommit_on_commit_actions();

	/* close large objects before lower-level cleanup */
	AtEOXact_LargeObject(true);

	/*
	 * Mark serializable transaction as complete for predicate locking
	 * purposes.  This should be done as late as we can put it and still allow
	 * errors to be raised for failure patterns found at commit.
	 */
	PreCommit_CheckForSerializationFailure();

	/* NOTIFY will be handled below */

	/*
	 * Don't allow PREPARE TRANSACTION if we've accessed a temporary table in
	 * this transaction.  Having the prepared xact hold locks on another
	 * backend's temp table seems a bad idea --- for instance it would prevent
	 * the backend from exiting.  There are other problems too, such as how to
	 * clean up the source backend's local buffers and ON COMMIT state if the
	 * prepared xact includes a DROP of a temp table.
	 *
	 * We must check this after executing any ON COMMIT actions, because they
	 * might still access a temp relation.
	 *
	 * XXX In principle this could be relaxed to allow some useful special
	 * cases, such as a temp table created and dropped all within the
	 * transaction.  That seems to require much more bookkeeping though.
	 */
	if ((MyXactFlags & XACT_FLAGS_ACCESSEDTEMPREL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has operated on temporary tables")));

	/*
	 * Likewise, don't allow PREPARE after pg_export_snapshot.  This could be
	 * supported if we added cleanup logic to twophase.c, but for now it
	 * doesn't seem worth the trouble.
	 */
	if (XactHasExportedSnapshots())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has exported snapshots")));

	/*
	 * Don't allow PREPARE but for transaction that has/might kill logical
	 * replication workers.
	 */
	if (XactManipulatesLogicalReplicationWorkers())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot PREPARE a transaction that has manipulated logical replication workers")));

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();

	/*
	 * set the current transaction state information appropriately during
	 * prepare processing
	 */
	s->state = TRANS_PREPARE;

	prepared_at = GetCurrentTimestamp();

	/* Tell bufmgr and smgr to prepare for commit */
	BufmgrCommit();


	/*
	 * Reserve the GID for this transaction. This could fail if the requested
	 * GID is invalid or already in use.
	 */
	gxact = MarkAsPreparing(xid, prepareGID, prepared_at,
							GetUserId(), MyDatabaseId);
	prepareGID = NULL;

	/*
	 * Collect data for the 2PC state file.  Note that in general, no actual
	 * state change should happen in the called modules during this step,
	 * since it's still possible to fail before commit, and in that case we
	 * want transaction abort to be able to clean up.  (In particular, the
	 * AtPrepare routines may error out if they find cases they cannot
	 * handle.)  State cleanup should happen in the PostPrepare routines
	 * below.  However, some modules can go ahead and clear state here because
	 * they wouldn't do anything with it during abort anyway.
	 *
	 * Note: because the 2PC state file records will be replayed in the same
	 * order they are made, the order of these calls has to match the order in
	 * which we want things to happen during COMMIT PREPARED or ROLLBACK
	 * PREPARED; in particular, pay attention to whether things should happen
	 * before or after releasing the transaction's locks.
	 */
	StartPrepare(gxact);

	AtPrepare_Notify();
	AtPrepare_Locks();
	AtPrepare_PredicateLocks();
	AtPrepare_PgStat();
	AtPrepare_MultiXact();
	AtPrepare_RelationMap();

#ifdef XCP
	AtEOXact_WaitedXids();
#endif

	/*
	 * Here is where we really truly prepare.
	 *
	 * We have to record transaction prepares even if we didn't make any
	 * updates, because the transaction manager might get confused if we lose
	 * a global transaction.
	 */
	EndPrepare(gxact);
#ifdef __TWO_PHASE_TRANS__
    /*
     * clear g_twophase_state for explicit trans, 
     * since after the explicit "PREPARE TRANSACTION"
     * we can start other twophase transactions 
     */
    g_twophase_state.state = TWO_PHASE_PREPARED;
    if (!IsXidImplicit(g_twophase_state.gid))
    {
        ClearLocalTwoPhaseState();
    }
#endif

    /*
 * Stamp this XID (and sub-XIDs) with the CSN we only mark prepared state
 * for connection from coordinator.
 */
    if (enable_distri_print)
        elog(LOG, "prepare record xid %d", xid);

    if (XactGlobalPrepareTimestamp)
    {
        GlobalTimestamp prepareTs;

        /*
         * We must first set committing status in CTS in order to ensure
         * consistency: guaranteeing that if start timestamp is larger than
         * prepare ts, then it can see the committing status to wait for
         * completion.
         *
         * Written by Junbin Kang, 2020.06.08
         */

        prepareTs = GetGlobalPrepareTimestamp();

        if (isImplicit && !CSN_IS_NORMAL(prepareTs))
            elog(ERROR, "invalid prepare ts " UINT64_FORMAT, prepareTs);

        CSNLogSetCSN(xid, 0, NULL, InvalidXLogRecPtr, false, MASK_PREPARE_BIT(prepareTs));
        
        if (enable_distri_print)
            elog(LOG, "prepare record xid %d prepare timestamp " UINT64_FORMAT,
             xid, MASK_PREPARE_BIT(prepareTs));
    }
	/*
	 * Now we clean up backend-internal state and release internal resources.
	 */

	/* Reset XactLastRecEnd until the next transaction writes something */
	XactLastRecEnd = 0;

	
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	EndGlobalPrepare(gxact, isImplicit);
#endif

	/*
	 * Let others know about no transaction in progress by me.  This has to be
	 * done *after* the prepared transaction has been marked valid, else
	 * someone may think it is unlocked and recyclable.
	 */
	ProcArrayClearTransaction(MyProc);

	/*
	 * In normal commit-processing, this is all non-critical post-transaction
	 * cleanup.  When the transaction is prepared, however, it's important
	 * that the locks and other per-backend resources are transferred to the
	 * prepared transaction's PGPROC entry.  Note that if an error is raised
	 * here, it's too late to abort the transaction. XXX: This probably should
	 * be in a critical section, to force a PANIC if any of this fails, but
	 * that cure could be worse than the disease.
	 */

	CallXactCallbacks(XACT_EVENT_PREPARE);
	CallXactCallbacksOnce(XACT_EVENT_PREPARE);

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_PORTAL,
						 true, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, true);

	/* Check we've released all buffer pins */
	AtEOXact_Buffers(true);

	/* Clean up the relation cache */
	AtEOXact_RelationCache(true);

	/* notify doesn't need a postprepare call */

	PostPrepare_PgStat();

	PostPrepare_Inval();

	PostPrepare_smgr();
	PostPrepare_seq();

	PostPrepare_MultiXact(xid);

	PostPrepare_Locks(xid);
	PostPrepare_PredicateLocks(xid);

	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, true);
	ResourceOwnerRelease(TopTransactionResourceOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, true);

	/*
	 * Allow another backend to finish the transaction.  After
	 * PostPrepare_Twophase(), the transaction is completely detached from our
	 * backend.  The rest is just non-critical cleanup of backend-local state.
	 */
	PostPrepare_Twophase();

	/* PREPARE acts the same as COMMIT as far as GUC is concerned */
	AtEOXact_DisGuc(true, 1);
	AtEOXact_GUC(true, 1);
	AtEOXact_SPI(true);
	AtEOXact_on_commit_actions(true);
	AtEOXact_Namespace(true, false);
	AtEOXact_SMgr();
	AtEOXact_Files();
	AtEOXact_HashTables(true);
	/* don't call AtEOXact_PgStat here; we fixed pgstat state above */
	AtEOXact_Snapshot(true, true);
	AtEOXact_WorkFile();
	pgstat_report_xact_timestamp(0);

	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(TopTransactionResourceOwner);
	s->curTransactionOwner = NULL;
	CurTransactionResourceOwner = NULL;
	TopTransactionResourceOwner = NULL;

	AtCommit_Memory();
#ifdef PGXC
	/* Clean up GTM callbacks */
	CleanGTMCallbacks();
#ifdef XCP
	if (!isImplicit)
		AtEOXact_Remote();
	GTMxactStartTimestamp = 0;
#endif	
#endif

	s->transactionId = InvalidTransactionId;
	s->subTransactionId = InvalidSubTransactionId;
	s->nestingLevel = 0;
	s->gucNestLevel = 0;
	s->childXids = NULL;
	s->nChildXids = 0;
	s->maxChildXids = 0;

	XactTopTransactionId = InvalidTransactionId;
	nParallelCurrentXids = 0;

	nRequestedXid = 0;
	nRequestedXidSPMUsed = 0;
	g_plsql_excep_sub_level = 0;

	/*
	 * done with 1st phase commit processing, set current transaction state
	 * back to default
	 */
	s->state = TRANS_DEFAULT;
	s->can_local_commit = false;

	RESUME_INTERRUPTS();

#ifdef PGXC /* PGXC_DATANODE */
	/*
	 * Now also prepare the remote nodes involved in this transaction. We do
	 * this irrespective of whether we are doing an implicit or an explicit
	 * prepare.
	 *
	 * XXX Like CommitTransaction and AbortTransaction, we do this after
	 * resuming interrupts because we are going to access the communication
	 * channels. So we want to keep receiving signals to avoid infinite
	 * blocking. But this must be checked for correctness
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		if (!isImplicit)
			s->topGlobalTransansactionId = InvalidGlobalTransactionId;
		ForgetTransactionLocalNode();
	}
#ifdef XCP
	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		if (s->waitedForXids)
			pfree(s->waitedForXids);
	}
	s->waitedForXids = NULL;
	s->waitedForXidsCount = 0;
#endif

	SetNextTransactionId(InvalidTransactionId);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	SetLocalTransactionId(InvalidTransactionId);
	FreeGlobalXid();
	ResetLocalFidFlevel();
#endif
	/*
	 * Set the command ID of Coordinator to be sent to the remote nodes
	 * as the 1st one.
	 * For remote nodes, enforce the command ID sending flag to false to avoid
	 * sending any command ID by default as now transaction is done.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
		SetReceivedCommandId(FirstCommandId);
	else
		SetSendCommandId(false);
#endif
#ifdef __OPENTENBASE__

	if (enable_distri_debug)
	{
		SetLocalPrepareTimestamp(GetCurrentTimestamp());
	}
#endif
#ifdef _PG_ORCL_
	s->plpgsql_subtrans = 0;
	s->implicit_state = IMPLICIT_SUB_NON;
#endif
}

#ifdef __TWO_PHASE_TRANS__
/* record and print errormsg in AbortTransaction, only print when isprint==true */
bool print_twophase_state(StringInfo errormsg, bool isprint)
{
    int i;
    int index;
    
    initStringInfo(errormsg);
    /* make sure not print repeated msg */
    if (g_twophase_state.isprinted)
    {
        return false;
    }
    else
    {
        g_twophase_state.isprinted = true;
    }
    appendStringInfo(errormsg, "Twophase Transaction '%s', started on node '%s', partnodes '%s', failed in Global State %s\n", 
            g_twophase_state.gid, g_twophase_state.start_node_name, g_twophase_state.participants, GetTransStateString(g_twophase_state.state));
    if (0 < (g_twophase_state.datanode_index + g_twophase_state.coord_index))
    {
        appendStringInfo(errormsg, "\t state of 2pc transaction '%s' on each node:\n", g_twophase_state.gid);
    }
    for (i = 0; i < g_twophase_state.datanode_index; i++)
    {
        if (g_twophase_state.datanode_state[i].is_participant == false)
            continue;
        index = g_twophase_state.datanode_state[i].handle_idx;
        appendStringInfo(errormsg, "\t\t 2pc twophase state on datanode: %s, gid: %s, trans state: %s, conn state: %s\n", 
            g_twophase_state.handles->datanode_handles[index]->nodename, 
            g_twophase_state.gid, 
            GetTransStateString(g_twophase_state.datanode_state[i].state), 
            GetConnStateString(g_twophase_state.datanode_state[i].conn_state));
    }

    for (i = 0; i < g_twophase_state.coord_index; i++)
    {
        if (g_twophase_state.coord_state[i].is_participant == false)
            continue;
        index = g_twophase_state.coord_state[i].handle_idx;
        appendStringInfo(errormsg, "\t\t 2pc twophase state on coordnode: %s, gid: %s, state: %s, conn state: %s\n", 
            g_twophase_state.handles->coord_handles[index]->nodename, 
            g_twophase_state.gid, 
            GetTransStateString(g_twophase_state.coord_state[i].state), 
            GetConnStateString(g_twophase_state.coord_state[i].conn_state));
    }
    if (0 < (g_twophase_state.datanode_index + g_twophase_state.coord_index))
    {
        appendStringInfo(errormsg, "\t response msg of 2pc transaction '%s' on each node:\n", g_twophase_state.gid);
    }
    for (i = 0; i < g_twophase_state.datanode_index; i++)
    {
        if (g_twophase_state.datanode_state[i].is_participant == false)
            continue;
        index = g_twophase_state.datanode_state[i].handle_idx;
        if (strlen(g_twophase_state.handles->datanode_handles[index]->error))
            appendStringInfo(errormsg, "\t\t 2pc twophase state on datanode: %s, gid: %s, errmsg: %s\n", 
                g_twophase_state.handles->datanode_handles[index]->nodename, 
                g_twophase_state.gid, g_twophase_state.handles->datanode_handles[index]->error);
    }

    for (i = 0; i < g_twophase_state.coord_index; i++)
    {
        if (g_twophase_state.coord_state[i].is_participant == false)
            continue;
        index = g_twophase_state.coord_state[i].handle_idx;
        if (strlen(g_twophase_state.handles->coord_handles[index]->error))
            appendStringInfo(errormsg, "\t\t 2pc twophase state on coordnode: %s, gid: %s, errmsg: %s\n", 
                g_twophase_state.handles->coord_handles[index]->nodename, 
                g_twophase_state.gid, g_twophase_state.handles->coord_handles[index]->error);
    }
    if (isprint)
    {
        elog(LOG, "%s", errormsg->data);
        resetStringInfo(errormsg);
    }
    return true;
}
#endif

/*
 *	AbortTransaction
 */
static void
AbortTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	TransactionId latestXid;
	bool		is_parallel_worker;
	bool		can_abort = true;
	bool       is_read_only = g_twophase_state.is_readonly;

#ifdef __TWO_PHASE_TRANS__
    StringInfoData errormsg;
	
#ifdef __TWO_PHASE_TESTS__
    bool test_stop = (complish && run_pg_clean);
#endif

	if (!SPI_get_internal_xact()
	 )
		stmt_level_rollback = false;
    
    can_abort = !(
#ifdef __TWO_PHASE_TESTS__
      (complish && run_pg_clean) ||
#endif
      TWO_PHASE_COMMITTING == g_twophase_state.state ||
      TWO_PHASE_COMMIT_END == g_twophase_state.state ||
      TWO_PHASE_COMMIT_ERROR == g_twophase_state.state ||
      TWO_PHASE_ABORT_END == g_twophase_state.state ||
      TWO_PHASE_UNKNOW_STATUS == g_twophase_state.state ||
        (TWO_PHASE_PREPARED == g_twophase_state.state &&
        false == g_twophase_state.is_start_node));

    if (!can_abort)
    {
        if (false == g_twophase_state.isprinted)
        {
            print_twophase_state(&errormsg, false);	

            if (enable_2pc_error_stop)
            {
               elog(STOP, "errormsg in AbortTransaction:\n %s", errormsg.data);
            }
#ifdef __TWO_PHASE_TESTS__
            else if (test_stop)
            {
                switch (run_pg_clean)
                {
                case TWO_PHASE_TEST_NOT_STOP:
                    break;
                case TWO_PHASE_TEST_STOP_DN:
                    if (IS_PGXC_LOCAL_COORDINATOR)
                    {
                        break;
                    }
                case TWO_PHASE_TEST_STOP_ALL:
                    elog(STOP, "in test, in AbortTransaction:\n %s", errormsg.data);
                    break;
                default:
                    break;
                }
                elog(WARNING, "in test, in AbortTransaction:\n %s", errormsg.data);
            }
#endif
            else
            {
                elog(WARNING, "2PC is interrupted, it will be committed or "
                    "rollbacked automatically later\n %s", errormsg.data);
            }
        }
        else
        {
            if (enable_2pc_error_stop)
            {
                elog(STOP, "STOP postmaster in AbortTransaction");
            }
#ifdef __TWO_PHASE_TESTS__
            else if (test_stop)
            {
                switch (run_pg_clean)
                {
                case TWO_PHASE_TEST_NOT_STOP:
                    break;
                case TWO_PHASE_TEST_STOP_DN:
                    if (IS_PGXC_LOCAL_COORDINATOR)
                    {
                        break;
                    }
                case TWO_PHASE_TEST_STOP_ALL:
                    elog(STOP, "in test, postmaster in AbortTransaction");
                    break;
                default:
                    break;
                }
                elog(WARNING, "in test, postmaster in AbortTransaction");
            }
#endif
            else
            {
                elog(WARNING, "WARNING postmaster in AbortTransaction");
            }
        }
	}

    /* print prepare err in pgxc_node_remote_prepare */
    if (!g_twophase_state.isprinted && TWO_PHASE_PREPARING == g_twophase_state.state)
    {
        print_twophase_state(&errormsg, true);
        /* since the transaction may fail in AbortTransaction again, it still need to print further errmsg */
        g_twophase_state.isprinted = false;
    }
#endif

#ifdef PGXC
	/*
	 * Cleanup the files created during database/tablespace operations.
	 * This must happen before we release locks, because we want to hold the
	 * locks acquired initially while we cleanup the files.
	 * If can_abort is false, needn't do DBCleanup, Createdb, movedb, createtablespace e.g.
	 */
	if (can_abort)
	{
		AtEOXact_DBCleanup(false);
	}

	AtEOXact_SHMGTMPrimaryInfo(false);
	/*
	 * Save the current top transaction ID. We need this to close the
	 * transaction at the GTM at thr end
	 */
	s->topGlobalTransansactionId = s->transactionId;

#ifdef __TWO_PHASE_TRANS__
	if (IS_PGXC_LOCAL_COORDINATOR && g_twophase_state.state != TWO_PHASE_INITIALTRANS)
	{
		elog(LOG, "send signal to clean 2pc launcher, gid: %s", g_twophase_state.gid);
		SendPostmasterSignal(PMSIGNAL_WAKEN_CLEAN_2PC_TRIGGER);
	}
#endif

	/*
	 * Shutdown all control threads before using the connection handle
	 */
	AtEOXact_SetUser(false);

	if (can_abort)
	{
		/*
		 * Handle remote abort first.
		 */
#ifdef __TWO_PHASE_TRANS__
		if (TWO_PHASE_ABORTTING != g_twophase_state.state)
		{
			g_twophase_state.state = TWO_PHASE_ABORTTING;
#endif
			if (IS_PGXC_COORDINATOR)
				PreAbort_Remote(TXN_TYPE_RollbackTxn, true);
#ifdef __TWO_PHASE_TRANS__
		}
#endif

		if (XactLocalNodePrepared)
		{
#ifdef __TWO_PHASE_TRANS__
			g_twophase_state.state = TWO_PHASE_ABORT_END;
#endif
			PreventTransactionChain(true, "ROLLBACK IMPLICIT PREPARED");
			FinishPreparedTransaction(savePrepareGID, false);
			XactLocalNodePrepared = false;
		}
	}
	else
	{
		/*
		 * if can't abort, also check for release handles,
		 * avoid state residues
		 */
		check_for_release_handles(false);
	}

	if (IS_PGXC_DATANODE && !is_read_only)
	{
		uint32		TotalProcs = TOTALPROCNUMS;

		WaitAllReadOnlyProcessFinish();

		LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
		MemSet(MyProc->read_proc_map, 0, bitmap_size(TotalProcs));
		LWLockRelease(&MyProc->globalxidLock);
	}

#ifdef __TWO_PHASE_TRANS__
	ClearLocalTwoPhaseState();
	g_twophase_state.is_readonly = is_read_only;
#endif

	if (enable_distri_debug && is_distri_report && IS_PGXC_COORDINATOR)
	{
		if(savePrepareGID)
		{
			LogCommitTranGTM(GetTopGlobalTransactionId(), 
							 savePrepareGID, 
							 NULL, 
							 0,
							 true,
							 false, 
							 InvalidGlobalTimestamp, 
							 InvalidGlobalTimestamp);
		}
		else if(GetTopGlobalTransactionId() && GetGlobalXidNoCheck())
		{
			LogCommitTranGTM(GetTopGlobalTransactionId(), 
							 GetGlobalXid(), 
							 NULL, 
							 0,
							 true,
		                     false, 
		                     InvalidGlobalTimestamp, 
		                     InvalidGlobalTimestamp);
		}
	}

	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		/*
		 * Callback on GTM if necessary, this needs to be done before HOLD_INTERRUPTS
		 * as this is not a part of the end of transaction procesing involving clean up.
		 */
		CallGTMCallbacks(GTM_EVENT_ABORT);
	}
#endif

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();

	/* Make sure we have a valid memory context and resource owner */
	AtAbort_Memory();
	AtAbort_ResourceOwner();

#ifdef PGXC
	/* Clean up GTM callbacks */
	CleanGTMCallbacks();
#endif

#ifdef _SHARDING_
	ATEOXact_CleanUpShardBarrier();
#endif

	/*
	 * Release any LW locks we might be holding as quickly as possible.
	 * (Regular locks, however, must be held till we finish aborting.)
	 * Releasing LW locks is critical since we might try to grab them again
	 * while cleaning up!
	 */
	LWLockReleaseAll();
	/* Clear wait information and command progress indicator */
	pgstat_report_wait_end();
	pgstat_progress_end_command();

	/* Clean up buffer I/O and buffer context locks, too */
	AbortBufferIO();
	UnlockBuffers();

	/* Reset WAL record construction state */
	XLogResetInsertion();

	/* Cancel condition variable sleep */
	ConditionVariableCancelSleep();

	/*
	 * Also clean up any open wait for lock, since the lock manager will choke
	 * if we try to wait for another lock before doing this.
	 */
	LockErrorCleanup();

	/*
	 * If any timeout events are still active, make sure the timeout interrupt
	 * is scheduled.  This covers possible loss of a timeout interrupt due to
	 * longjmp'ing out of the SIGINT handler (see notes in handle_sig_alarm).
	 * We delay this till after LockErrorCleanup so that we don't uselessly
	 * reschedule lock or deadlock check timeouts.
	 */
	reschedule_timeouts();

	/*
	 * Re-enable signals, in case we got here by longjmp'ing out of a signal
	 * handler.  We do this fairly early in the sequence so that the timeout
	 * infrastructure will be functional if needed while aborting.
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * check the current transaction state
	 */
	is_parallel_worker = (s->blockState == TBLOCK_PARALLEL_INPROGRESS);
	if (s->state != TRANS_INPROGRESS && s->state != TRANS_PREPARE)
		elog(WARNING, "AbortTransaction while in %s state",
			 TransStateAsString(s->state));
	Assert(s->parent == NULL);

#ifdef _MIGRATE_
	InvalidateShmemShardMap(false);
#endif

#ifdef __RESOURCE_QUEUE__
	if ((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) &&
		IsResQueueEnabled())
	{
		ResQUsageRemoveLocalResourceInfo();

		if (IS_PGXC_COORDINATOR)
			ReleaseResourceBackToResQueue();
	}
#endif

	/*
	 * set the current transaction state information appropriately during the
	 * abort processing
	 */
	s->state = TRANS_ABORT;

	/*
	 * Reset user ID which might have been changed transiently.  We need this
	 * to clean up in case control escaped out of a SECURITY DEFINER function
	 * or other local change of CurrentUserId; therefore, the prior value of
	 * SecurityRestrictionContext also needs to be restored.
	 *
	 * (Note: it is not necessary to restore session authorization or role
	 * settings here because those can only be changed via GUC, and GUC will
	 * take care of rolling them back if need be.)
	 */
	SetUserIdAndSecContext(s->prevUser, s->prevSecContext);

	/* Forget about any active REINDEX. */
	ResetReindexState(s->nestingLevel);

	/* Reset snapshot export state. */
	SnapBuildResetExportedSnapshotState();

	/* If in parallel mode, clean up workers and exit parallel mode. */
	if (IsInParallelMode())
	{
		AtEOXact_Parallel(false);
		s->parallelModeLevel = 0;
	}

	/*
	 * do abort processing
	 */
	AfterTriggerEndXact(false); /* 'false' means it's abort */

	AtAbort_Portals();
	AtEOXact_LargeObject(false);
	AtAbort_Notify();
	AtEOXact_RelationMap(false);
	AtAbort_Twophase();

	/* release portal related resources after AtAbort_Portals */
	if (TopTransactionResourceOwner != NULL)
		ResourceOwnerRelease(TopTransactionResourceOwner,
							 RESOURCE_RELEASE_PORTAL,
							 false, true);

	/*
	 * Advertise the fact that we aborted in pg_xact (assuming that we got as
	 * far as assigning an XID to advertise).  But if we're inside a parallel
	 * worker, skip this; the user backend must be the one to write the abort
	 * record.
	 */
	if (!is_parallel_worker)
	{

#ifdef XCP
		if (IsConnFromDatanode())
			latestXid = InvalidTransactionId;
		else
#endif
			latestXid = RecordTransactionAbort(false);
#ifdef __OPENTENBASE__
		if (can_abort)
		{
		FinishSeqOp(false);
		}
#endif
	}
	else
	{
		latestXid = InvalidTransactionId;

		/*
		 * Since the parallel master won't get our value of XactLastRecEnd in
		 * this case, we nudge WAL-writer ourselves in this case.  See related
		 * comments in RecordTransactionAbort for why this matters.
		 */
		XLogSetAsyncXactLSN(XactLastRecEnd);
	}

	AbortSPMPendingList();

	TRACE_POSTGRESQL_TRANSACTION_ABORT(MyProc->lxid);

	/*
	 * Read-only process exits due to an error, it removes itself
	 * from the write process's bitmap.
	 */
	if (is_read_only && MyProc->write_proc != NULL)
	{
		LWLockAcquire(&MyProc->write_proc->globalxidLock, LW_SHARED);
		/* clear my procno to write proc */
		bitmap_clear_atomic(MyProc->write_proc->read_proc_map, MyProc->pgprocno);
		LWLockRelease(&MyProc->write_proc->globalxidLock);

		/* reset read only process global xid */
		LWLockAcquire(&MyProc->globalxidLock, LW_EXCLUSIVE);
		MyProc->globalXid[0] = '\0';
		LWLockRelease(&MyProc->globalxidLock);

		MyProc->write_proc = NULL;
	}

	/*
	 * Let others know about no transaction in progress by me. Note that this
	 * must be done _before_ releasing locks we hold and _after_
	 * RecordTransactionAbort.
	 */
	ProcArrayEndTransaction(MyProc, latestXid);

	/*
	 * Post-abort cleanup.  See notes in CommitTransaction() concerning
	 * ordering.  We can skip all of it if the transaction failed before
	 * creating a resource owner.
	 */
	if (TopTransactionResourceOwner != NULL)
	{
		if (is_parallel_worker)
			CallXactCallbacks(XACT_EVENT_PARALLEL_ABORT);
		else
			CallXactCallbacks(XACT_EVENT_ABORT);
		CallXactCallbacksOnce(XACT_EVENT_ABORT);

		ResourceOwnerRelease(TopTransactionResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, true);
		AtEOXact_Buffers(false);
		AtEOXact_RelationCache(false);
		AtEOXact_Inval(false);
		AtEOXact_MultiXact();

		/* See comments in CommitTransaction */
#ifdef XCP
		if (can_abort)
		{
			AtEOXact_GlobalTxn(false);
		}
#endif

		ResourceOwnerRelease(TopTransactionResourceOwner,
							 RESOURCE_RELEASE_LOCKS,
							 false, true);
		ResourceOwnerRelease(TopTransactionResourceOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS,
							 false, true);
		smgrDoPendingDeletes(false);

		AtEOXact_DisGuc(false, 1);
		AtEOXact_GUC(false, 1);
		AtEOXact_SPI(false);
		AtEOXact_on_commit_actions(false);
		AtEOXact_Namespace(false, is_parallel_worker);
		AtEOXact_SMgr();
		AtEOXact_Files();
		AtEOXact_HashTables(false);
		AtEOXact_PgStat(false);
		AtEOXact_ApplyLauncher(false);
		AtEOXact_WorkFile();
		pgstat_report_xact_timestamp(0);
	}

#ifdef PGXC
	ForgetTransactionLocalNode();
#endif
	/*
	 * State remains TRANS_ABORT until CleanupTransaction().
	 */
	RESUME_INTERRUPTS();

	if (xactRelStatHash != NULL)
	{
		hash_destroy(xactRelStatHash);
		xactRelStatHash = NULL;
		clean_relstat_list();
	}

	if (analyzeRelHash != NULL)
	{
		hash_destroy(analyzeRelHash);
		analyzeRelHash = NULL;
	}
#ifdef PGXC
	AtEOXact_Remote();
	GTMxactStartTimestamp = 0;
#endif

#ifdef __OPENTENBASE__
	//SetExitPlpgsqlFunc();
	SetExitCreateExtension();
	AtEOXact_Global();
#endif
#ifdef __TWO_PHASE_TESTS__
    ClearTwophaseException();
#endif

	/* Release resource group slot */
	if (ShouldUnassignResGroup())
		UnassignResGroup();

	if (debug_thread_count != 0
	 )
		elog(PANIC, "there is %d unfinished tuple queue thread",
			 debug_thread_count);
}

/*
 *	CleanupTransaction
 */
static void
CleanupTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	bool is_read_only = g_twophase_state.is_readonly;
	/*
	 * State should still be TRANS_ABORT from AbortTransaction().
	 */
	if (s->state != TRANS_ABORT)
		elog(FATAL, "CleanupTransaction: unexpected state %s",
			 TransStateAsString(s->state));

	/*
	 * do abort cleanup processing
	 */
	AtCleanup_Portals();		/* now safe to release portal memory */
	AtEOXact_Snapshot(false, true); /* and release the transaction's snapshots */

	CurrentResourceOwner = NULL;	/* and resource owner */
	if (TopTransactionResourceOwner)
		ResourceOwnerDelete(TopTransactionResourceOwner);
	s->curTransactionOwner = NULL;
	CurTransactionResourceOwner = NULL;
	TopTransactionResourceOwner = NULL;

	AtCleanup_Memory();			/* and transaction memory */

	s->transactionId = InvalidTransactionId;
	s->subTransactionId = InvalidSubTransactionId;
	s->nestingLevel = 0;
	s->gucNestLevel = 0;
	s->childXids = NULL;
	s->nChildXids = 0;
	s->maxChildXids = 0;
	s->parallelModeLevel = 0;

	XactTopTransactionId = InvalidTransactionId;
	nParallelCurrentXids = 0;

	nRequestedXid = 0;
	nRequestedXidSPMUsed = 0;
	g_plsql_excep_sub_level = 0;

#ifdef XCP
	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		if (s->waitedForXids)
			pfree(s->waitedForXids);
	}
	s->waitedForXids = NULL;
	s->waitedForXidsCount = 0;
#endif

	/*
	 * done with abort processing, set current transaction state back to
	 * default
	 */
	s->state = TRANS_DEFAULT;
	s->can_local_commit = false;

#ifdef PGXC
	/*
	 * Set the command ID of Coordinator to be sent to the remote nodes
	 * as the 1st one.
	 * For remote nodes, enforce the command ID sending flag to false to avoid
	 * sending any command ID by default as now transaction is done.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
		SetReceivedCommandId(FirstCommandId);
	else
		SetSendCommandId(false);
#endif

#ifdef __TWO_PHASE_TRANS__
	ClearLocalTwoPhaseState();
	g_twophase_state.is_readonly = is_read_only;
#endif

	/* Release resource group slot */
	if (ShouldUnassignResGroup())
		UnassignResGroup();
#ifdef _PG_ORCL_
	s->plpgsql_subtrans = 0;
	s->implicit_state = IMPLICIT_SUB_NON;
#endif
}

/*
 *	StartTransactionCommand
 */
void
StartTransactionCommand(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
			/*
			 * if we aren't in a transaction block, we just do our usual start
			 * transaction.
			 */
		case TBLOCK_DEFAULT:
			StartTransaction();
			s->blockState = TBLOCK_STARTED;
			break;

			/*
			 * We are somewhere in a transaction block or subtransaction and
			 * about to start a new command.  For now we do nothing, but
			 * someday we may do command-local resource initialization. (Note
			 * that any needed CommandCounterIncrement was done by the
			 * previous CommitTransactionCommand.)
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
			break;

			/*
			 * Here we are in a failed transaction block (one of the commands
			 * caused an abort) so we do nothing but remain in the abort
			 * state.  Eventually we will get a ROLLBACK command which will
			 * get us out of this state.  (It is up to other code to ensure
			 * that no commands other than ROLLBACK will be processed in these
			 * states.)
			 */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			break;

			/* These cases are invalid. */
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(ERROR, "StartTransactionCommand: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	/*
	 * We must switch to CurTransactionContext before returning. This is
	 * already done if we called StartTransaction, otherwise not.
	 */
	Assert(CurTransactionContext != NULL);
	MemoryContextSwitchTo(CurTransactionContext);
}

/*
 *	CommitTransactionCommand
 */
void
CommitTransactionCommand(void)
{
	TransactionState s = CurrentTransactionState;

	savepoint_define_cmd = false;
	switch (s->blockState)
	{
			/*
			 * These shouldn't happen.  TBLOCK_DEFAULT means the previous
			 * StartTransactionCommand didn't set the STARTED state
			 * appropriately, while TBLOCK_PARALLEL_INPROGRESS should be ended
			 * by EndParallelWorkerTransaction(), not this function.
			 */
		case TBLOCK_DEFAULT:
		case TBLOCK_PARALLEL_INPROGRESS:
			elog(FATAL, "CommitTransactionCommand: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;

			/*
			 * If we aren't in a transaction block, just do our usual
			 * transaction commit, and return to the idle state.
			 */
		case TBLOCK_STARTED:
			CommitTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * We are completing a "BEGIN TRANSACTION" command, so we change
			 * to the "transaction block in progress" state and return.  (We
			 * assume the BEGIN did nothing to the database, so we need no
			 * CommandCounterIncrement.)
			 */
		case TBLOCK_BEGIN:
			s->blockState = TBLOCK_INPROGRESS;
			break;

			/*
			 * This is the case when we have finished executing a command
			 * someplace within a transaction block.  We increment the command
			 * counter and return.
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
			CommandCounterIncrement();
			break;

			/*
			 * We are completing a "COMMIT" command.  Do it and return to the
			 * idle state.
			 */
		case TBLOCK_END:
			CommitTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * Here we are in the middle of a transaction block but one of the
			 * commands caused an abort so we do nothing but remain in the
			 * abort state.  Eventually we will get a ROLLBACK command.
			 */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			break;

			/*
			 * Here we were in an aborted transaction block and we just got
			 * the ROLLBACK command from the user, so clean up the
			 * already-aborted transaction and return to the idle state.
			 */
		case TBLOCK_ABORT_END:
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * Here we were in a perfectly good transaction block but the user
			 * told us to ROLLBACK anyway.  We have to abort the transaction
			 * and then clean up.
			 */
		case TBLOCK_ABORT_PENDING:
			AbortTransaction();
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * We are completing a "PREPARE TRANSACTION" command.  Do it and
			 * return to the idle state.
			 */
		case TBLOCK_PREPARE:
			PrepareTransaction();
			s->blockState = TBLOCK_DEFAULT;

			if (SyncRepCanceled)
			{
				SyncRepCanceled = false;
				elog(ERROR, "The waiting for synchronous replication is canceled. "
					"The prepare transaction xlog has already committed locally, "
					"but might not have been replicated to the standby.");
			}
			break;

			/*
			 * We were just issued a SAVEPOINT inside a transaction block.
			 * Start a subtransaction.  (DefineSavepoint already did
			 * PushTransaction, so as to have someplace to put the SUBBEGIN
			 * state.)
			 */
		case TBLOCK_SUBBEGIN:
			StartSubTransaction();
			s->blockState = TBLOCK_SUBINPROGRESS;
			break;

			/*
			 * We were issued a RELEASE command, so we end the current
			 * subtransaction and return to the parent transaction. The parent
			 * might be ended too, so repeat till we find an INPROGRESS
			 * transaction or subtransaction.
			 */
		case TBLOCK_SUBRELEASE:
			do
			{
				CommitSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
			} while (s->blockState == TBLOCK_SUBRELEASE);

			Assert(s->blockState == TBLOCK_INPROGRESS ||
				   s->blockState == TBLOCK_SUBINPROGRESS);
			break;

			/*
			 * We were issued a COMMIT, so we end the current subtransaction
			 * hierarchy and perform final commit. We do this by rolling up
			 * any subtransactions into their parent, which leads to O(N^2)
			 * operations with respect to resource owners - this isn't that
			 * bad until we approach a thousands of savepoints but is
			 * necessary for correctness should after triggers create new
			 * resource owners.
			 */
		case TBLOCK_SUBCOMMIT:
			do
			{
				CommitSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
			} while (s->blockState == TBLOCK_SUBCOMMIT);
			/* If we had a COMMIT command, finish off the main xact too */
			if (s->blockState == TBLOCK_END)
			{
				Assert(s->parent == NULL);
				CommitTransaction();
				s->blockState = TBLOCK_DEFAULT;
			}
			else if (s->blockState == TBLOCK_PREPARE)
			{
				Assert(s->parent == NULL);
				PrepareTransaction();
				s->blockState = TBLOCK_DEFAULT;
			}
			else
				elog(ERROR, "CommitTransactionCommand: unexpected state %s",
					 BlockStateAsString(s->blockState));
			break;

			/*
			 * The current already-failed subtransaction is ending due to a
			 * ROLLBACK or ROLLBACK TO command, so pop it and recursively
			 * examine the parent (which could be in any of several states).
			 */
		case TBLOCK_SUBABORT_END:
			CleanupSubTransaction();
			CommitTransactionCommand();
			break;

			/*
			 * As above, but it's not dead yet, so abort first.
			 */
		case TBLOCK_SUBABORT_PENDING:
			AbortSubTransaction();
			CleanupSubTransaction();
			CommitTransactionCommand();
			break;

			/*
			 * The current subtransaction is the target of a ROLLBACK TO
			 * command.  Abort and pop it, then start a new subtransaction
			 * with the same name.
			 */
		case TBLOCK_SUBRESTART:
			{
				char	   *name;
				int			savepointLevel;

				/* save name and keep Cleanup from freeing it */
				name = s->name;
				s->name = NULL;
				savepointLevel = s->savepointLevel;

				AbortSubTransaction();
				CleanupSubTransaction();

				DefineSavepoint(NULL, false);
				s = CurrentTransactionState;	/* changed by push */
				s->name = name;
				s->savepointLevel = savepointLevel;

				/* This is the same as TBLOCK_SUBBEGIN case */
				AssertState(s->blockState == TBLOCK_SUBBEGIN);
				StartSubTransaction();
				s->blockState = TBLOCK_SUBINPROGRESS;
				bump_sub_transaction_id_current_txn_handles();
			}
			break;

			/*
			 * Same as above, but the subtransaction had already failed, so we
			 * don't need AbortSubTransaction.
			 */
		case TBLOCK_SUBABORT_RESTART:
			{
				char	   *name;
				int			savepointLevel;

				/* save name and keep Cleanup from freeing it */
				name = s->name;
				s->name = NULL;
				savepointLevel = s->savepointLevel;

				CleanupSubTransaction();

				DefineSavepoint(NULL, false);
				s = CurrentTransactionState;	/* changed by push */
				s->name = name;
				s->savepointLevel = savepointLevel;

				/* This is the same as TBLOCK_SUBBEGIN case */
				AssertState(s->blockState == TBLOCK_SUBBEGIN);
				StartSubTransaction();
				s->blockState = TBLOCK_SUBINPROGRESS;
				bump_sub_transaction_id_current_txn_handles();
			}
			break;
	}
}

/*
 *	AbortCurrentTransaction
 */
void
AbortCurrentTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		case TBLOCK_DEFAULT:
			if (s->state == TRANS_DEFAULT)
			{
				/* we are idle, so nothing to do */
			}
			else
			{
				/*
				 * We can get here after an error during transaction start
				 * (state will be TRANS_START).  Need to clean up the
				 * incompletely started transaction.  First, adjust the
				 * low-level state to suppress warning message from
				 * AbortTransaction.
				 */
				if (s->state == TRANS_START)
					s->state = TRANS_INPROGRESS;
				AbortTransaction();
				CleanupTransaction();
			}
			break;

			/*
			 * if we aren't in a transaction block, we just do the basic abort
			 * & cleanup transaction.
			 */
		case TBLOCK_STARTED:
			AbortTransaction();
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * If we are in TBLOCK_BEGIN it means something screwed up right
			 * after reading "BEGIN TRANSACTION".  We assume that the user
			 * will interpret the error as meaning the BEGIN failed to get him
			 * into a transaction block, so we should abort and return to idle
			 * state.
			 */
		case TBLOCK_BEGIN:
			AbortTransaction();
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * We are somewhere in a transaction block and we've gotten a
			 * failure, so we abort the transaction and set up the persistent
			 * ABORT state.  We will stay in ABORT until we get a ROLLBACK.
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_PARALLEL_INPROGRESS:
			AbortTransaction();
			s->blockState = TBLOCK_ABORT;
			/* CleanupTransaction happens when we exit TBLOCK_ABORT_END */

			/* Do not popup a transaction, it will do after TBLOCK_ABORT_END */
			break;

			/*
			 * Here, we failed while trying to COMMIT.  Clean up the
			 * transaction and return to idle state (we do not want to stay in
			 * the transaction).
			 */
		case TBLOCK_END:
			AbortTransaction();
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * Here, we are already in an aborted transaction state and are
			 * waiting for a ROLLBACK, but for some reason we failed again! So
			 * we just remain in the abort state.
			 */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			break;

			/*
			 * We are in a failed transaction and we got the ROLLBACK command.
			 * We have already aborted, we just need to cleanup and go to idle
			 * state.
			 */
		case TBLOCK_ABORT_END:
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * We are in a live transaction and we got a ROLLBACK command.
			 * Abort, cleanup, go to idle state.
			 */
		case TBLOCK_ABORT_PENDING:
			AbortTransaction();
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * Here, we failed while trying to PREPARE.  Clean up the
			 * transaction and return to idle state (we do not want to stay in
			 * the transaction).
			 */
		case TBLOCK_PREPARE:
			AbortTransaction();
			CleanupTransaction();
			s->blockState = TBLOCK_DEFAULT;
			break;

			/*
			 * We got an error inside a subtransaction.  Abort just the
			 * subtransaction, and go to the persistent SUBABORT state until
			 * we get ROLLBACK.
			 */
		case TBLOCK_SUBINPROGRESS:
			AbortSubTransaction();
			s->blockState = TBLOCK_SUBABORT;
			break;

			/*
			 * If we failed while trying to create a subtransaction, clean up
			 * the broken subtransaction and abort the parent.  The same
			 * applies if we get a failure while ending a subtransaction.
			 */
		case TBLOCK_SUBBEGIN:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
			AbortSubTransaction();
			CleanupSubTransaction();
			AbortCurrentTransaction();
			break;

			/*
			 * Same as above, except the Abort() was already done.
			 */
		case TBLOCK_SUBABORT_END:
		case TBLOCK_SUBABORT_RESTART:
			CleanupSubTransaction();
			AbortCurrentTransaction();
			break;
	}
}

/*
 *	PreventTransactionChain
 *
 *	This routine is to be called by statements that must not run inside
 *	a transaction block, typically because they have non-rollback-able
 *	side effects or do internal commits.
 *
 *	If we have already started a transaction block, issue an error; also issue
 *	an error if we appear to be running inside a user-defined function (which
 *	could issue more commands and possibly cause a failure after the statement
 *	completes).  Subtransactions are verboten too.
 *
 *	isTopLevel: passed down from ProcessUtility to determine whether we are
 *	inside a function or multi-query querystring.  (We will always fail if
 *	this is false, but it's convenient to centralize the check here instead of
 *	making callers do it.)
 *	stmtType: statement type name, for error messages.
 */
void
PreventTransactionChain(bool isTopLevel, const char *stmtType)
{
	/*
	 * xact block already started?
	 */
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
		/* translator: %s represents an SQL statement name */
				 errmsg("%s cannot run inside a transaction block",
						stmtType)));

	/*
	 * subtransaction?
	 */
	if (IsSubTransaction())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
		/* translator: %s represents an SQL statement name */
				 errmsg("%s cannot run inside a subtransaction",
						stmtType)));

	/*
	 * inside a function call?
	 */
	if (!isTopLevel)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
		/* translator: %s represents an SQL statement name */
				 errmsg("%s cannot be executed from a function or multi-command string",
						stmtType)));

	/* If we got past IsTransactionBlock test, should be in default state */
	if (CurrentTransactionState->blockState != TBLOCK_DEFAULT &&
		CurrentTransactionState->blockState != TBLOCK_STARTED)
		elog(FATAL, "cannot prevent transaction chain");
	/* all okay */
}

/*
 *	These two functions allow for warnings or errors if a command is
 *	executed outside of a transaction block.
 *
 *	While top-level transaction control commands (BEGIN/COMMIT/ABORT) and
 *	SET that have no effect issue warnings, all other no-effect commands
 *	generate errors.
 */
void
WarnNoTransactionChain(bool isTopLevel, const char *stmtType)
{
	CheckTransactionChain(isTopLevel, false, stmtType);
}

void
RequireTransactionChain(bool isTopLevel, const char *stmtType)
{
	CheckTransactionChain(isTopLevel, true, stmtType);
}

/*
 *	RequireTransactionChain
 *
 *	This routine is to be called by statements that must run inside
 *	a transaction block, because they have no effects that persist past
 *	transaction end (and so calling them outside a transaction block
 *	is presumably an error).  DECLARE CURSOR is an example.
 *
 *	If we appear to be running inside a user-defined function, we do not
 *	issue anything, since the function could issue more commands that make
 *	use of the current statement's results.  Likewise subtransactions.
 *	Thus this is an inverse for PreventTransactionChain.
 *
 *	isTopLevel: passed down from ProcessUtility to determine whether we are
 *	inside a function.
 *	stmtType: statement type name, for warning or error messages.
 */
static void
CheckTransactionChain(bool isTopLevel, bool throwError, const char *stmtType)
{
	/*
	 * internal connections?
	 */
	if (!throwError && !IsConnFromApp())
		return ;

	/*
	 * xact block already started?
	 */
	if (IsTransactionBlock())
		return;

	/*
	 * subtransaction?
	 */
	if (IsSubTransaction())
		return;

	/*
	 * inside a function call?
	 */
	if (!isTopLevel)
		return;

	ereport(throwError ? ERROR : WARNING,
			(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
	/* translator: %s represents an SQL statement name */
			 errmsg("%s can only be used in transaction blocks",
					stmtType)));
	return;
}

/*
 *	IsInTransactionChain
 *
 *	This routine is for statements that need to behave differently inside
 *	a transaction block than when running as single commands.  ANALYZE is
 *	currently the only example.
 *
 *	isTopLevel: passed down from ProcessUtility to determine whether we are
 *	inside a function.
 */
bool
IsInTransactionChain(bool isTopLevel)
{
	/*
	 * Return true on same conditions that would make PreventTransactionChain
	 * error out
	 */
	if (IsTransactionBlock())
		return true;

	if (IsSubTransaction())
		return true;

	if (!isTopLevel)
		return true;

	if (CurrentTransactionState->blockState != TBLOCK_DEFAULT &&
		CurrentTransactionState->blockState != TBLOCK_STARTED)
		return true;

	return false;
}


/*
 * Register or deregister callback functions for start- and end-of-xact
 * operations.
 *
 * These functions are intended for use by dynamically loaded modules.
 * For built-in modules we generally just hardwire the appropriate calls
 * (mainly because it's easier to control the order that way, where needed).
 *
 * At transaction end, the callback occurs post-commit or post-abort, so the
 * callback functions can only do noncritical cleanup.
 */
void
RegisterXactCallback(XactCallback callback, void *arg)
{
	XactCallbackItem *item;

	item = (XactCallbackItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(XactCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = Xact_callbacks;
	Xact_callbacks = item;
}

void
UnregisterXactCallback(XactCallback callback, void *arg)
{
	XactCallbackItem *item;
	XactCallbackItem *prev;

	prev = NULL;
	for (item = Xact_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				Xact_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallXactCallbacks(XactEvent event)
{
	XactCallbackItem *item;

	for (item = Xact_callbacks; item; item = item->next)
		item->callback(event, item->arg);
}

/* Register or deregister callback functions for start/end Xact.  Call only once. */
void
RegisterXactCallbackOnce(XactCallback callback, void *arg)
{
	XactCallbackItem *item;

	item = (XactCallbackItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(XactCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = Xact_callbacks_once;
	Xact_callbacks_once = item;
}

void
UnregisterXactCallbackOnce(XactCallback callback, void *arg)
{
	XactCallbackItem *item;
	XactCallbackItem *prev;

	prev = NULL;
	for (item = Xact_callbacks_once; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				Xact_callbacks_once = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallXactCallbacksOnce(XactEvent event)
{
	/* currently callback once should ignore prepare. */
	if (event == XACT_EVENT_PREPARE)
		return;

	while(Xact_callbacks_once)
	{
		XactCallbackItem *next = Xact_callbacks_once->next;
		XactCallback callback=Xact_callbacks_once->callback;
		void*arg=Xact_callbacks_once->arg;
		pfree(Xact_callbacks_once);
		Xact_callbacks_once = next;
		callback(event,arg);
	}
}

/*
 * Register or deregister callback functions for start- and end-of-subxact
 * operations.
 *
 * Pretty much same as above, but for subtransaction events.
 *
 * At subtransaction end, the callback occurs post-subcommit or post-subabort,
 * so the callback functions can only do noncritical cleanup.  At
 * subtransaction start, the callback is called when the subtransaction has
 * finished initializing.
 */
void
RegisterSubXactCallback(SubXactCallback callback, void *arg)
{
	SubXactCallbackItem *item;

	item = (SubXactCallbackItem *)
		MemoryContextAlloc(TopMemoryContext, sizeof(SubXactCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = SubXact_callbacks;
	SubXact_callbacks = item;
}

void
UnregisterSubXactCallback(SubXactCallback callback, void *arg)
{
	SubXactCallbackItem *item;
	SubXactCallbackItem *prev;

	prev = NULL;
	for (item = SubXact_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				SubXact_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallSubXactCallbacks(SubXactEvent event,
					 SubTransactionId mySubid,
					 SubTransactionId parentSubid)
{
	SubXactCallbackItem *item;

	for (item = SubXact_callbacks; item; item = item->next)
		item->callback(event, mySubid, parentSubid, item->arg);
}


#ifdef PGXC
/*
 * Register or deregister callback functions for GTM at xact start or stop.
 * Those operations are more or less the xact callbacks but we need to perform
 * them before HOLD_INTERRUPTS as it is a part of transaction management and
 * is not included in xact cleaning.
 *
 * The callback is called when xact finishes and may be initialized by events
 * related to GTM that need to be taken care of at the end of a transaction block.
 */
void
RegisterGTMCallback(GTMCallback callback, void *arg)
{
	GTMCallbackItem *item;

	item = (GTMCallbackItem *)
		MemoryContextAlloc(TopTransactionContext, sizeof(GTMCallbackItem));
	item->callback = callback;
	item->arg = arg;
	item->next = GTM_callbacks;
	GTM_callbacks = item;
}

void
UnregisterGTMCallback(GTMCallback callback, void *arg)
{
	GTMCallbackItem *item;
	GTMCallbackItem *prev;

	prev = NULL;
	for (item = GTM_callbacks; item; prev = item, item = item->next)
	{
		if (item->callback == callback && item->arg == arg)
		{
			if (prev)
				prev->next = item->next;
			else
				GTM_callbacks = item->next;
			pfree(item);
			break;
		}
	}
}

static void
CallGTMCallbacks(GTMEvent event)
{
	GTMCallbackItem *item;

	for (item = GTM_callbacks; item; item = item->next)
		(*item->callback) (event, item->arg);
}
#endif

/* ----------------------------------------------------------------
 *					   transaction block support
 * ----------------------------------------------------------------
 */

/*
 *	BeginTransactionBlock
 *		This executes a BEGIN command.
 */
void
BeginTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
			/*
			 * We are not inside a transaction block, so allow one to begin.
			 */
		case TBLOCK_STARTED:
			s->blockState = TBLOCK_BEGIN;
			break;

			/*
			 * Already a transaction block in progress.
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
			ereport(WARNING,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("there is already a transaction in progress")));
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "BeginTransactionBlock: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

#ifdef PGXC
	/*
	 * Set command Id sending flag only for a local Coordinator when transaction begins,
	 * For a remote node this flag is set to true only if a command ID has been received
	 * from a Coordinator. This may not be always the case depending on the queries being
	 * run and how command Ids are generated on remote nodes.
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
		SetSendCommandId(true);
#endif
}

/*
 *	PrepareTransactionBlock
 *		This executes a PREPARE command.
 *
 * Since PREPARE may actually do a ROLLBACK, the result indicates what
 * happened: TRUE for PREPARE, FALSE for ROLLBACK.
 *
 * Note that we don't actually do anything here except change blockState.
 * The real work will be done in the upcoming PrepareTransaction().
 * We do it this way because it's not convenient to change memory context,
 * resource owner, etc while executing inside a Portal.
 */
bool
PrepareTransactionBlock(const char *gid)
{
	TransactionState s;
	bool		result;

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(LOCAL_PREPARE_TRANSACTION_BLOCK, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, gid, NULL);
#endif

	/* Set up to commit the current transaction */
	result = EndTransactionBlock();

	/* If successful, change outer tblock state to PREPARE */
	if (result)
	{
		s = CurrentTransactionState;

		while (s->parent != NULL)
			s = s->parent;

		if (s->blockState == TBLOCK_END)
		{
			/* Save GID where PrepareTransaction can find it again */
			prepareGID = MemoryContextStrdup(TopTransactionContext, gid);

			s->blockState = TBLOCK_PREPARE;
		}
		else
		{
			/*
			 * ignore case where we are not in a transaction;
			 * EndTransactionBlock already issued a warning.
			 */
			Assert(s->blockState == TBLOCK_STARTED);
			/* Don't send back a PREPARE result tag... */
			result = false;
		}
	}

#ifdef PGXC
	/* Reset command ID sending flag */
	SetSendCommandId(false);
#endif

	return result;
}

/*
 *	EndTransactionBlock
 *		This executes a COMMIT command.
 *
 * Since COMMIT may actually do a ROLLBACK, the result indicates what
 * happened: TRUE for COMMIT, FALSE for ROLLBACK.
 *
 * Note that we don't actually do anything here except change blockState.
 * The real work will be done in the upcoming CommitTransactionCommand().
 * We do it this way because it's not convenient to change memory context,
 * resource owner, etc while executing inside a Portal.
 */
bool
EndTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;
	bool		result = false;

	switch (s->blockState)
	{
			/*
			 * We are in a transaction block, so tell CommitTransactionCommand
			 * to COMMIT.
			 */
		case TBLOCK_INPROGRESS:
			s->blockState = TBLOCK_END;
			result = true;
			break;

			/*
			 * We are in a failed transaction block.  Tell
			 * CommitTransactionCommand it's time to exit the block.
			 */
		case TBLOCK_ABORT:
			s->blockState = TBLOCK_ABORT_END;
			break;

			/*
			 * We are in a live subtransaction block.  Set up to subcommit all
			 * open subtransactions and then commit the main transaction.
			 */
		case TBLOCK_SUBINPROGRESS:
			while (s->parent != NULL)
			{
				if (s->blockState == TBLOCK_SUBINPROGRESS)
					s->blockState = TBLOCK_SUBCOMMIT;
				else
					elog(FATAL, "EndTransactionBlock: unexpected state %s",
						 BlockStateAsString(s->blockState));
				s = s->parent;
			}
			if (s->blockState == TBLOCK_INPROGRESS)
				s->blockState = TBLOCK_END;
			else
				elog(FATAL, "EndTransactionBlock: unexpected state %s",
					 BlockStateAsString(s->blockState));
			result = true;
			break;

			/*
			 * Here we are inside an aborted subtransaction.  Treat the COMMIT
			 * as ROLLBACK: set up to abort everything and exit the main
			 * transaction.
			 */
		case TBLOCK_SUBABORT:
			while (s->parent != NULL)
			{
				if (s->blockState == TBLOCK_SUBINPROGRESS)
					s->blockState = TBLOCK_SUBABORT_PENDING;
				else if (s->blockState == TBLOCK_SUBABORT)
					s->blockState = TBLOCK_SUBABORT_END;
				else
					elog(FATAL, "EndTransactionBlock: unexpected state %s",
						 BlockStateAsString(s->blockState));
				s = s->parent;
			}
			if (s->blockState == TBLOCK_INPROGRESS)
				s->blockState = TBLOCK_ABORT_PENDING;
			else if (s->blockState == TBLOCK_ABORT)
				s->blockState = TBLOCK_ABORT_END;
			else
				elog(FATAL, "EndTransactionBlock: unexpected state %s",
					 BlockStateAsString(s->blockState));
			break;

			/*
			 * The user issued COMMIT when not inside a transaction.  Issue a
			 * WARNING, staying in TBLOCK_STARTED state.  The upcoming call to
			 * CommitTransactionCommand() will then close the transaction and
			 * put us back into the default state.
			 */
		case TBLOCK_STARTED:
			ereport(WARNING,
					(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
					 errmsg("there is no transaction in progress")));
			result = true;
			break;

			/*
			 * The user issued a COMMIT that somehow ran inside a parallel
			 * worker.  We can't cope with that.
			 */
		case TBLOCK_PARALLEL_INPROGRESS:
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot commit during a parallel operation")));
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "EndTransactionBlock: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	return result;
}

/*
 *	UserAbortTransactionBlock
 *		This executes a ROLLBACK command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void
UserAbortTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
			/*
			 * We are inside a transaction block and we got a ROLLBACK command
			 * from the user, so tell CommitTransactionCommand to abort and
			 * exit the transaction block.
			 */
		case TBLOCK_INPROGRESS:
			s->blockState = TBLOCK_ABORT_PENDING;
			break;

			/*
			 * We are inside a failed transaction block and we got a ROLLBACK
			 * command from the user.  Abort processing is already done, so
			 * CommitTransactionCommand just has to cleanup and go back to
			 * idle state.
			 */
		case TBLOCK_ABORT:
			s->blockState = TBLOCK_ABORT_END;
			break;

			/*
			 * We are inside a subtransaction.  Mark everything up to top
			 * level as exitable.
			 */
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_SUBABORT:
			while (s->parent != NULL)
			{
				if (s->blockState == TBLOCK_SUBINPROGRESS)
					s->blockState = TBLOCK_SUBABORT_PENDING;
				else if (s->blockState == TBLOCK_SUBABORT)
					s->blockState = TBLOCK_SUBABORT_END;
				else
					elog(FATAL, "UserAbortTransactionBlock: unexpected state %s",
						 BlockStateAsString(s->blockState));
				s = s->parent;
			}
			if (s->blockState == TBLOCK_INPROGRESS)
				s->blockState = TBLOCK_ABORT_PENDING;
			else if (s->blockState == TBLOCK_ABORT)
				s->blockState = TBLOCK_ABORT_END;
			else
				elog(FATAL, "UserAbortTransactionBlock: unexpected state %s",
					 BlockStateAsString(s->blockState));
			break;

			/*
			 * The user issued ABORT when not inside a transaction. Issue a
			 * WARNING and go to abort state.  The upcoming call to
			 * CommitTransactionCommand() will then put us back into the
			 * default state.
			 */
		case TBLOCK_STARTED:
			ereport(WARNING,
					(errcode(ERRCODE_NO_ACTIVE_SQL_TRANSACTION),
					 errmsg("there is no transaction in progress")));
			s->blockState = TBLOCK_ABORT_PENDING;
			break;

			/*
			 * The user issued an ABORT that somehow ran inside a parallel
			 * worker.  We can't cope with that.
			 */
		case TBLOCK_PARALLEL_INPROGRESS:
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot abort during a parallel operation")));
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "UserAbortTransactionBlock: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

#ifdef PGXC
	/* Reset Command Id sending flag */
	SetSendCommandId(false);
#endif
}

/*
 * DefineSavepoint
 *		This executes a SAVEPOINT command.
 */
void
DefineSavepoint(char *name, bool internal)
{
	TransactionState s = CurrentTransactionState;

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(SAVEPOINT_DEFINE_FAILED, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, NULL, NULL);
#endif

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for new subtransactions after that
	 * point.  (Note that this check will certainly error out if s->blockState
	 * is TBLOCK_PARALLEL_INPROGRESS, so we can treat that as an invalid case
	 * below.)
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot define savepoints during a parallel operation")));

	switch (s->blockState)
	{
		case TBLOCK_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
			/* Normal subtransaction start */
			PushTransaction(internal);
			s = CurrentTransactionState;	/* changed by push */

			/*
			 * Savepoint names, like the TransactionState block itself, live
			 * in TopTransactionContext.
			 */
			if (name)
				s->name = MemoryContextStrdup(TopTransactionContext, name);
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "DefineSavepoint: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}
}

/*
 * ReleaseSavepoint
 *		This executes a RELEASE command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void
ReleaseSavepoint(List *options)
{
	TransactionState s = CurrentTransactionState;
	TransactionState target,
				xact;
	ListCell   *cell;
	char	   *name = NULL;

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(SAVEPOINT_RELEASE_FAILED, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, NULL, NULL);
#endif

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for transaction state change after that
	 * point.  (Note that this check will certainly error out if s->blockState
	 * is TBLOCK_PARALLEL_INPROGRESS, so we can treat that as an invalid case
	 * below.)
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot release savepoints during a parallel operation")));

	switch (s->blockState)
	{
			/*
			 * We can't rollback to a savepoint if there is no savepoint
			 * defined.
			 */
		case TBLOCK_INPROGRESS:
			ereport(ERROR,
					(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
					 errmsg("no such savepoint")));
			break;

			/*
			 * We are in a non-aborted subtransaction.  This is the only valid
			 * case.
			 */
		case TBLOCK_SUBINPROGRESS:
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "ReleaseSavepoint: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	foreach(cell, options)
	{
		DefElem    *elem = lfirst(cell);

		if (strcmp(elem->defname, "savepoint_name") == 0)
			name = strVal(elem->arg);
	}

	elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv release %s. cur_level: %d", MyProcPid, name, GetCurrentTransactionNestLevel());

	Assert(PointerIsValid(name));

	for (target = s; PointerIsValid(target); target = target->parent)
	{
		if (PointerIsValid(target->name) && strcmp(target->name, name) == 0)
			break;
	}

	if (!PointerIsValid(target))
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/* disallow crossing savepoint level boundaries */
	if (target->savepointLevel != s->savepointLevel)
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/*
	 * Mark "commit pending" all subtransactions up to the target
	 * subtransaction.  The actual commits will happen when control gets to
	 * CommitTransactionCommand.
	 */
	xact = CurrentTransactionState;
	for (;;)
	{
		Assert(xact->blockState == TBLOCK_SUBINPROGRESS);
		xact->blockState = TBLOCK_SUBRELEASE;
		if (xact == target)
			break;
		xact = xact->parent;
		Assert(PointerIsValid(xact));
	}

	g_in_release_savepoint = true;
}

/*
 * RollbackToSavepoint
 *		This executes a ROLLBACK TO <savepoint> command.
 *
 * As above, we don't actually do anything here except change blockState.
 */
void
RollbackToSavepoint(List *options)
{
	TransactionState s = CurrentTransactionState;
	TransactionState target,
				xact;
	ListCell   *cell;
	char	   *name = NULL;

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(SAVEPOINT_ROLLBACK_FAILED, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, NULL, NULL);
#endif

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for transaction state change after that
	 * point.  (Note that this check will certainly error out if s->blockState
	 * is TBLOCK_PARALLEL_INPROGRESS, so we can treat that as an invalid case
	 * below.)
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot rollback to savepoints during a parallel operation")));

	switch (s->blockState)
	{
			/*
			 * We can't rollback to a savepoint if there is no savepoint
			 * defined.
			 */
		case TBLOCK_INPROGRESS:
		case TBLOCK_ABORT:
			ereport(ERROR,
					(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
					 errmsg("no such savepoint")));
			break;

			/*
			 * There is at least one savepoint, so proceed.
			 */
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_SUBABORT:
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "RollbackToSavepoint: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	foreach(cell, options)
	{
		DefElem    *elem = lfirst(cell);

		if (strcmp(elem->defname, "savepoint_name") == 0)
			name = strVal(elem->arg);
	}

	elog(DEBUG1, "[NEST_LEVEL_CHECK:%d] recv rollback_to %s. cur_level: %d", MyProcPid, name, GetCurrentTransactionNestLevel());

	Assert(PointerIsValid(name));

	if (strcmp(name, IMPLICIT_SAVEPOINT_NAME) == 0)
		elog(ERROR, "cannot use this name for savepoint, please use others");

	for (target = s; PointerIsValid(target); target = target->parent)
	{
		if (PointerIsValid(target->name) && strcmp(target->name, name) == 0)
			break;
	}

	if (!PointerIsValid(target))
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/* disallow crossing savepoint level boundaries */
	if (target->savepointLevel != s->savepointLevel)
		ereport(ERROR,
				(errcode(ERRCODE_S_E_INVALID_SPECIFICATION),
				 errmsg("no such savepoint")));

	/*
	 * Mark "abort pending" all subtransactions up to the target
	 * subtransaction.  The actual aborts will happen when control gets to
	 * CommitTransactionCommand.
	 */
	xact = CurrentTransactionState;
	for (;;)
	{
		if (xact == target)
			break;
		if (xact->blockState == TBLOCK_SUBINPROGRESS)
			xact->blockState = TBLOCK_SUBABORT_PENDING;
		else if (xact->blockState == TBLOCK_SUBABORT)
			xact->blockState = TBLOCK_SUBABORT_END;
		else
			elog(FATAL, "RollbackToSavepoint: unexpected state %s",
				 BlockStateAsString(xact->blockState));
		xact = xact->parent;
		Assert(PointerIsValid(xact));
	}

	/* And mark the target as "restart pending" */
	if (xact->blockState == TBLOCK_SUBINPROGRESS)
		xact->blockState = TBLOCK_SUBRESTART;
	else if (xact->blockState == TBLOCK_SUBABORT)
		xact->blockState = TBLOCK_SUBABORT_RESTART;
	else
		elog(FATAL, "RollbackToSavepoint: unexpected state %s",
			 BlockStateAsString(xact->blockState));
	g_in_rollback_to = true;
}

/*
 * BeginInternalSubTransaction
 *		This is the same as DefineSavepoint except it allows TBLOCK_STARTED,
 *		TBLOCK_END, and TBLOCK_PREPARE states, and therefore it can safely be
 *		used in functions that might be called when not inside a BEGIN block
 *		or when running deferred triggers at COMMIT/PREPARE time.  Also, it
 *		automatically does CommitTransactionCommand/StartTransactionCommand
 *		instead of expecting the caller to do it.
 */
void
BeginInternalSubTransactionInternal(char *name, const char *filename, int lineno)
{
	TransactionState s = CurrentTransactionState;

	if(enable_distri_print)
		elog(LOG, "BeginInternalSubTransactionInternal %s %d", filename, lineno);

#ifdef __OPENTENBASE__
#else
	elog(ERROR, "Internal subtransactions not supported in Postgres-XL");
#endif
	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for new subtransactions after that
	 * point. We might be able to make an exception for the type of
	 * subtransaction established by this function, which is typically used in
	 * contexts where we're going to release or roll back the subtransaction
	 * before proceeding further, so that no enduring change to the
	 * transaction state occurs. For now, however, we prohibit this case along
	 * with all the others.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot start subtransactions during a parallel operation")));

	switch (s->blockState)
	{
		case TBLOCK_STARTED:
		case TBLOCK_INPROGRESS:
		case TBLOCK_END:
		case TBLOCK_PREPARE:
		case TBLOCK_SUBINPROGRESS:
			/* Normal subtransaction start */
			PushTransaction(true);
			s = CurrentTransactionState;	/* changed by push */

			/*
			 * Savepoint names, like the TransactionState block itself, live
			 * in TopTransactionContext.
			 */
			if (name)
				s->name = MemoryContextStrdup(TopTransactionContext, name);
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_BEGIN:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
			elog(FATAL, "BeginInternalSubTransaction: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	CommitTransactionCommand();
	StartTransactionCommand();
}

/*
 * ReleaseCurrentSubTransaction
 *
 * RELEASE (ie, commit) the innermost subtransaction, regardless of its
 * savepoint name (if any).
 * NB: do NOT use CommitTransactionCommand/StartTransactionCommand with this.
 */
void
ReleaseCurrentSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * Workers synchronize transaction state at the beginning of each parallel
	 * operation, so we can't account for commit of subtransactions after that
	 * point.  This should not happen anyway.  Code calling this would
	 * typically have called BeginInternalSubTransaction() first, failing
	 * there.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot commit subtransactions during a parallel operation")));

	if (s->blockState != TBLOCK_SUBINPROGRESS)
		elog(ERROR, "ReleaseCurrentSubTransaction: unexpected state %s",
			 BlockStateAsString(s->blockState));
	Assert(s->state == TRANS_INPROGRESS);
	MemoryContextSwitchTo(CurTransactionContext);
	CommitSubTransaction();
	s = CurrentTransactionState;	/* changed by pop */
	Assert(s->state == TRANS_INPROGRESS);
}

/*
 * RollbackAndReleaseCurrentSubTransaction
 *
 * ROLLBACK and RELEASE (ie, abort) the innermost subtransaction, regardless
 * of its savepoint name (if any).
 * NB: do NOT use CommitTransactionCommand/StartTransactionCommand with this.
 */
void
RollbackAndReleaseCurrentSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	/*
	 * Unlike ReleaseCurrentSubTransaction(), this is nominally permitted
	 * during parallel operations.  That's because we may be in the master,
	 * recovering from an error thrown while we were in parallel mode.  We
	 * won't reach here in a worker, because BeginInternalSubTransaction()
	 * will have failed.
	 */

	switch (s->blockState)
	{
			/* Must be in a subtransaction */
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_SUBABORT:
			break;

			/* These cases are invalid. */
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
		case TBLOCK_BEGIN:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_INPROGRESS:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_ABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
		case TBLOCK_PREPARE:
			elog(FATAL, "RollbackAndReleaseCurrentSubTransaction: unexpected state %s",
				 BlockStateAsString(s->blockState));
			break;
	}

	/*
	 * Abort the current subtransaction, if needed.
	 */
	if (s->blockState == TBLOCK_SUBINPROGRESS)
		AbortSubTransaction();

	/* And clean it up, too */
	CleanupSubTransaction();

	s = CurrentTransactionState;	/* changed by pop */
	AssertState(s->blockState == TBLOCK_SUBINPROGRESS ||
				s->blockState == TBLOCK_INPROGRESS ||
				s->blockState == TBLOCK_STARTED);
}

/*
 *	AbortOutOfAnyTransaction
 *
 *	This routine is provided for error recovery purposes.  It aborts any
 *	active transaction or transaction block, leaving the system in a known
 *	idle state.
 */
void
AbortOutOfAnyTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	/* Ensure we're not running in a doomed memory context */
	AtAbort_Memory();

	/*
	 * Get out of any transaction or nested transaction
	 */
	do
	{
		switch (s->blockState)
		{
			case TBLOCK_DEFAULT:
				if (s->state == TRANS_DEFAULT)
				{
					/* Not in a transaction, do nothing */
				}
				else
				{
					/*
					 * We can get here after an error during transaction start
					 * (state will be TRANS_START).  Need to clean up the
					 * incompletely started transaction.  First, adjust the
					 * low-level state to suppress warning message from
					 * AbortTransaction.
					 */
					if (s->state == TRANS_START)
						s->state = TRANS_INPROGRESS;
					AbortTransaction();
					CleanupTransaction();
				}
				break;
			case TBLOCK_STARTED:
			case TBLOCK_BEGIN:
			case TBLOCK_INPROGRESS:
			case TBLOCK_PARALLEL_INPROGRESS:
			case TBLOCK_END:
			case TBLOCK_ABORT_PENDING:
			case TBLOCK_PREPARE:
				/* In a transaction, so clean up */
				AbortTransaction();
				CleanupTransaction();
				s->blockState = TBLOCK_DEFAULT;
				break;
			case TBLOCK_ABORT:
			case TBLOCK_ABORT_END:

				/*
				 * AbortTransaction is already done, still need Cleanup.
				 * However, if we failed partway through running ROLLBACK,
				 * there will be an active portal running that command, which
				 * we need to shut down before doing CleanupTransaction.
				 */
				AtAbort_Portals();
				CleanupTransaction();
				s->blockState = TBLOCK_DEFAULT;
				break;

				/*
				 * In a subtransaction, so clean it up and abort parent too
				 */
			case TBLOCK_SUBBEGIN:
			case TBLOCK_SUBINPROGRESS:
			case TBLOCK_SUBRELEASE:
			case TBLOCK_SUBCOMMIT:
			case TBLOCK_SUBABORT_PENDING:
			case TBLOCK_SUBRESTART:
				AbortSubTransaction();
				CleanupSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
				break;

			case TBLOCK_SUBABORT:
			case TBLOCK_SUBABORT_END:
			case TBLOCK_SUBABORT_RESTART:
				/* As above, but AbortSubTransaction already done */
				if (s->curTransactionOwner)
				{
					/* As in TBLOCK_ABORT, might have a live portal to zap */
					AtSubAbort_Portals(s->subTransactionId,
									   s->parent->subTransactionId,
									   s->curTransactionOwner,
									   s->parent->curTransactionOwner);
				}
				CleanupSubTransaction();
				s = CurrentTransactionState;	/* changed by pop */
				break;
		}
	} while (s->blockState != TBLOCK_DEFAULT
		);

	/* Should be out of all subxacts now */
	Assert(s->parent == NULL);

	/* If we didn't actually have anything to do, revert to TopMemoryContext */
	AtCleanup_Memory();
}

/*
 * IsTransactionBlock --- are we within a transaction block?
 */
bool
IsTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_DEFAULT || s->blockState == TBLOCK_STARTED)
		return false;

	return true;
}

/*
 * If a valid running transaction block.
 */
bool
IsRunningTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_BEGIN || s->blockState == TBLOCK_INPROGRESS ||
					s->blockState == TBLOCK_SUBINPROGRESS)
		return true;

	return false;
}

/*
 * IsPLpgSQLSubTransaction
 */
bool
IsPLpgSQLSubTransaction(void)
{
	return (CurrentTransactionState->plpgsql_subtrans > 0) && IsSubTransaction();
}

/*
 * MarkAsPLpgSQLSubTransaction
 *    Mark current transaction as plpgsql created.
 */
void
MarkAsPLpgSQLSubTransaction(void)
{
	CurrentTransactionState->plpgsql_subtrans = ++g_plsql_excep_sub_level;
}

int
GetPlsqlExcepSubLevel(void)
{
	return CurrentTransactionState->plpgsql_subtrans;
}

/*
 * GetPLpgSQLSubTransactionNum
 */
int *
GetPLpgSQLSubTransactionNum(int *plsub_count)
{
	TransactionState s;
	int n = 0;
	int i = 0;
	int *spi_levels;
	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		if (s->plpgsql_subtrans > 0 && s->nestingLevel >= 2)
			n++;
	}

	i = n;
	*plsub_count = n;
	spi_levels = palloc0(sizeof(int) * n);

	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		if (s->plpgsql_subtrans > 0 && s->nestingLevel >= 2)
			spi_levels[--i] = s->spi_level;
	}
	Assert(i == 0);
	return spi_levels;
}

/*
 * IsTransactionOrTransactionBlock --- are we within either a transaction
 * or a transaction block?	(The backend is only really "idle" when this
 * returns false.)
 *
 * This should match up with IsTransactionBlock and IsTransactionState.
 */
bool
IsTransactionOrTransactionBlock(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->blockState == TBLOCK_DEFAULT)
		return false;

	return true;
}

/*
 * TransactionBlockStatusCode - return status code to send in ReadyForQuery
 */
char
TransactionBlockStatusCode(void)
{
	TransactionState s = CurrentTransactionState;

	switch (s->blockState)
	{
		case TBLOCK_DEFAULT:
		case TBLOCK_STARTED:
			return 'I';			/* idle --- not in transaction */
		case TBLOCK_BEGIN:
		case TBLOCK_SUBBEGIN:
		case TBLOCK_INPROGRESS:
		case TBLOCK_PARALLEL_INPROGRESS:
		case TBLOCK_SUBINPROGRESS:
		case TBLOCK_END:
		case TBLOCK_SUBRELEASE:
		case TBLOCK_SUBCOMMIT:
		case TBLOCK_PREPARE:
			return 'T';			/* in transaction */
		case TBLOCK_ABORT:
		case TBLOCK_SUBABORT:
		case TBLOCK_ABORT_END:
		case TBLOCK_SUBABORT_END:
		case TBLOCK_ABORT_PENDING:
		case TBLOCK_SUBABORT_PENDING:
		case TBLOCK_SUBRESTART:
		case TBLOCK_SUBABORT_RESTART:
			return 'E';			/* in failed transaction */
	}

	/* should never get here */
	elog(FATAL, "invalid transaction block state: %s",
		 BlockStateAsString(s->blockState));
	return 0;					/* keep compiler quiet */
}

/*
 * IsSubTransaction
 */
bool
IsSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->nestingLevel >= 2)
		return true;

	return false;
}

/*
 * StartSubTransaction
 *
 * If you're wondering why this is separate from PushTransaction: it's because
 * we can't conveniently do this stuff right inside DefineSavepoint.  The
 * SAVEPOINT utility command will be executed inside a Portal, and if we
 * muck with CurrentMemoryContext or CurrentResourceOwner then exit from
 * the Portal will undo those settings.  So we make DefineSavepoint just
 * push a dummy transaction block, and when control returns to the main
 * idle loop, CommitTransactionCommand will be called, and we'll come here
 * to finish starting the subtransaction.
 */
static void
StartSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->state != TRANS_DEFAULT)
		elog(WARNING, "StartSubTransaction while in %s state",
			 TransStateAsString(s->state));

	s->state = TRANS_START;

	/*
	 * Initialize subsystems for new subtransaction
	 *
	 * must initialize resource-management stuff first
	 */
	AtSubStart_Memory();
	AtSubStart_ResourceOwner();
	AtSubStart_Notify();
	AfterTriggerBeginSubXact();

	s->state = TRANS_INPROGRESS;

	/*
	 * Call start-of-subxact callbacks
	 */
	CallSubXactCallbacks(SUBXACT_EVENT_START_SUB, s->subTransactionId,
						 s->parent->subTransactionId);

	ShowTransactionState("StartSubTransaction");
}

/*
 * CommitSubTransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
CommitSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	MemoryContext oldcontext = CurrentMemoryContext;

	ShowTransactionState("CommitSubTransaction");

	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "CommitSubTransaction while in %s state",
			 TransStateAsString(s->state));

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(LOCAL_COMMIT_SUBTRANSACTION_FAILED, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, NULL, NULL);
#endif
#ifdef __OPENTENBASE__
	AtEOXact_SetUser(true);
#endif
	PG_TRY();
	{
		CommitSubTransactionInternal(s);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		elog(FATAL, "error report when commit subtransaction, origin error: %s", edata ? edata->message : "");
	}
	PG_END_TRY();
}

static void
CommitSubTransactionInternal(TransactionState s)
{
#ifdef __OPENTENBASE__
	if (IS_PGXC_COORDINATOR)
		SubTranscation_PreCommit_Remote();
#endif
	/* Pre-commit processing goes here */

	CallSubXactCallbacks(SUBXACT_EVENT_PRE_COMMIT_SUB, s->subTransactionId,
						 s->parent->subTransactionId);

	/* If in parallel mode, clean up workers and exit parallel mode. */
	if (IsInParallelMode())
	{
		AtEOSubXact_Parallel(true, s->subTransactionId);
		s->parallelModeLevel = 0;
	}

#ifdef __OPENTENBASE__
	if (s->curTransactionOwner)
	{
		TransactionId xid = GetCurrentTransactionIdIfAny();

		if (TransactionIdIsValid(xid) && !IS_CENTRALIZED_MODE)
		{
			CheckGTMConnection();
		}
	}
#endif

	/* Do the actual "commit", such as it is */
	s->state = TRANS_COMMIT;

	/* Must CCI to ensure commands of subtransaction are seen as done */
	CommandCounterIncrement();

	/*
	 * Prior to 8.4 we marked subcommit in clog at this point.  We now only
	 * perform that step, if required, as part of the atomic update of the
	 * whole transaction tree at top level commit or abort.
	 */

	/* Post-commit cleanup */
	if (TransactionIdIsValid(s->transactionId))
		AtSubCommit_childXids();
	AfterTriggerEndSubXact(true);
	AtSubCommit_Portals(s->subTransactionId,
						s->parent->subTransactionId,
						s->parent->curTransactionOwner);
	AtEOSubXact_LargeObject(true, s->subTransactionId,
							s->parent->subTransactionId);
	AtSubCommit_Notify();

	CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, s->subTransactionId,
						 s->parent->subTransactionId);

	ResourceOwnerRelease(s->curTransactionOwner,
						 RESOURCE_RELEASE_BEFORE_LOCKS,
						 true, false);
	AtEOSubXact_RelationCache(true, s->subTransactionId,
							  s->parent->subTransactionId);
	AtEOSubXact_Inval(true);
	AtSubCommit_smgr();
	AtSubCommit_seq();

	/*
	 * The only lock we actually release here is the subtransaction XID lock.
	 */
	CurrentResourceOwner = s->curTransactionOwner;
	if (TransactionIdIsValid(s->transactionId))
		XactLockTableDelete(s->transactionId);

	/*
	 * Other locks should get transferred to their parent resource owner.
	 */
	ResourceOwnerRelease(s->curTransactionOwner,
						 RESOURCE_RELEASE_LOCKS,
						 true, false);
	ResourceOwnerRelease(s->curTransactionOwner,
						 RESOURCE_RELEASE_AFTER_LOCKS,
						 true, false);

	AtEOXact_DisGuc(true, s->nestingLevel);
	AtEOXact_GUC(true, s->gucNestLevel);
	AtEOSubXact_SPI(true, s->subTransactionId);
	AtEOSubXact_on_commit_actions(true, s->subTransactionId,
								  s->parent->subTransactionId);
	AtEOSubXact_Namespace(true, s->subTransactionId,
						  s->parent->subTransactionId);
	AtEOSubXact_Files(true, s->subTransactionId,
					  s->parent->subTransactionId);
	AtEOSubXact_HashTables(true, s->nestingLevel);
	AtEOSubXact_PgStat(true, s->nestingLevel);
	AtSubCommit_Snapshot(s->nestingLevel);
#ifdef XCP
	AtSubCommit_WaitedXids();
#endif

	/*
	 * We need to restore the upper transaction's read-only state, in case the
	 * upper is read-write while the child is read-only; GUC will incorrectly
	 * think it should leave the child state in place.
	 */
	XactReadOnly = s->prevXactReadOnly;

	CurrentResourceOwner = s->parent->curTransactionOwner;
	CurTransactionResourceOwner = s->parent->curTransactionOwner;
	ResourceOwnerDelete(s->curTransactionOwner);
	s->curTransactionOwner = NULL;

	AtSubCommit_Memory();

	s->state = TRANS_DEFAULT;

	PopTransaction();
}

/*
 * AbortSubTransaction
 */
static void
AbortSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	MemoryContext oldcontext = CurrentMemoryContext;
#ifdef __OPENTENBASE__
	AtEOXact_SetUser(false);
#endif
	PG_TRY();
	{
		AbortSubTransactionInternal(s);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		elog(FATAL, "error report when abort subtransaction, origin error: %s", edata ? edata->message : "");
	}
	PG_END_TRY();
}

static void
AbortSubTransactionInternal(TransactionState s)
{
#ifdef __OPENTENBASE__
	SubTranscation_PreAbort_Remote();
#endif

	/* Prevent cancel/die interrupt while cleaning up */
	HOLD_INTERRUPTS();

	/* Make sure we have a valid memory context and resource owner */
	AtSubAbort_Memory();
	AtSubAbort_ResourceOwner();

	/*
	 * Release any LW locks we might be holding as quickly as possible.
	 * (Regular locks, however, must be held till we finish aborting.)
	 * Releasing LW locks is critical since we might try to grab them again
	 * while cleaning up!
	 *
	 * FIXME This may be incorrect --- Are there some locks we should keep?
	 * Buffer locks, for example?  I don't think so but I'm not sure.
	 */
	LWLockReleaseAll();

	pgstat_report_wait_end();
	pgstat_progress_end_command();
	AbortBufferIO();
	UnlockBuffers();

	/* Reset WAL record construction state */
	XLogResetInsertion();

	/* Cancel condition variable sleep */
	ConditionVariableCancelSleep();

	/*
	 * Also clean up any open wait for lock, since the lock manager will choke
	 * if we try to wait for another lock before doing this.
	 */
	LockErrorCleanup();

	/*
	 * If any timeout events are still active, make sure the timeout interrupt
	 * is scheduled.  This covers possible loss of a timeout interrupt due to
	 * longjmp'ing out of the SIGINT handler (see notes in handle_sig_alarm).
	 * We delay this till after LockErrorCleanup so that we don't uselessly
	 * reschedule lock or deadlock check timeouts.
	 */
	reschedule_timeouts();

	/*
	 * Re-enable signals, in case we got here by longjmp'ing out of a signal
	 * handler.  We do this fairly early in the sequence so that the timeout
	 * infrastructure will be functional if needed while aborting.
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * check the current transaction state
	 */
	ShowTransactionState("AbortSubTransaction");

	if (s->state != TRANS_INPROGRESS)
		elog(WARNING, "AbortSubTransaction while in %s state",
			 TransStateAsString(s->state));

#ifdef __OPENTENBASE__
	if (s->curTransactionOwner && !IS_CENTRALIZED_MODE)
	{
		FinishSeqOp(false);
	}
#endif

	s->state = TRANS_ABORT;

	AbortSPMPendingList();

	/*
	 * Reset user ID which might have been changed transiently.  (See notes in
	 * AbortTransaction.)
	 */
	SetUserIdAndSecContext(s->prevUser, s->prevSecContext);

	/* Forget about any active REINDEX. */
	ResetReindexState(s->nestingLevel);

	/*
	 * No need for SnapBuildResetExportedSnapshotState() here, snapshot
	 * exports are not supported in subtransactions.
	 */

	/* Exit from parallel mode, if necessary. */
	if (IsInParallelMode())
	{
		AtEOSubXact_Parallel(false, s->subTransactionId);
		s->parallelModeLevel = 0;
	}

	/*
	 * We can skip all this stuff if the subxact failed before creating a
	 * ResourceOwner...
	 */
	if (s->curTransactionOwner)
	{
		AfterTriggerEndSubXact(false);

		AtSubAbort_Portals(s->subTransactionId,
						   s->parent->subTransactionId,
						   s->curTransactionOwner,
						   s->parent->curTransactionOwner);

		AtEOSubXact_LargeObject(false, s->subTransactionId,
								s->parent->subTransactionId);
		AtSubAbort_Notify();

		/* Advertise the fact that we aborted in pg_xact. */
		(void) RecordTransactionAbort(true);

		/* Post-abort cleanup */
		if (TransactionIdIsValid(s->transactionId))
			AtSubAbort_childXids();

		CallSubXactCallbacks(SUBXACT_EVENT_ABORT_SUB, s->subTransactionId,
							 s->parent->subTransactionId);

		ResourceOwnerRelease(s->curTransactionOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, false);
		AtEOSubXact_RelationCache(false, s->subTransactionId,
								  s->parent->subTransactionId);
		AtEOSubXact_Inval(false);
		ResourceOwnerRelease(s->curTransactionOwner,
							 RESOURCE_RELEASE_LOCKS,
							 false, false);
		ResourceOwnerRelease(s->curTransactionOwner,
							 RESOURCE_RELEASE_AFTER_LOCKS,
							 false, false);
		AtSubAbort_smgr();

		AtEOXact_DisGuc(false, s->nestingLevel);
		AtEOXact_GUC(false, s->gucNestLevel);
		AtEOSubXact_SPI(false, s->subTransactionId);
		AtEOSubXact_on_commit_actions(false, s->subTransactionId,
									  s->parent->subTransactionId);
		AtEOSubXact_Namespace(false, s->subTransactionId,
							  s->parent->subTransactionId);
		AtEOSubXact_Files(false, s->subTransactionId,
						  s->parent->subTransactionId);
		AtEOSubXact_HashTables(false, s->nestingLevel);
		AtEOSubXact_PgStat(false, s->nestingLevel);
		AtSubAbort_Snapshot(s->nestingLevel);
#ifdef XCP
		AtSubAbort_WaitedXids();
#endif
	}

	/*
	 * Restore the upper transaction's read-only state, too.  This should be
	 * redundant with GUC's cleanup but we may as well do it for consistency
	 * with the commit case.
	 */
	XactReadOnly = s->prevXactReadOnly;

	RESUME_INTERRUPTS();
}

/*
 * CleanupSubTransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
CleanupSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	ShowTransactionState("CleanupSubTransaction");

	if (s->state != TRANS_ABORT)
		elog(WARNING, "CleanupSubTransaction while in %s state",
			 TransStateAsString(s->state));

#ifdef __OPENTENBASE__
	if (IS_LOCAL_ACCESS_NODE)
#endif
	{
		AtSubCleanup_Portals(s->subTransactionId);
	}

	CurrentResourceOwner = s->parent->curTransactionOwner;
	CurTransactionResourceOwner = s->parent->curTransactionOwner;
	if (s->curTransactionOwner)
		ResourceOwnerDelete(s->curTransactionOwner);
	s->curTransactionOwner = NULL;

	AtSubCleanup_Memory();

	s->state = TRANS_DEFAULT;

	PopTransaction();
}

/*
 * PushTransaction
 *		Create transaction state stack entry for a subtransaction
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
PushTransaction(bool pl_excep_sub)
{
	TransactionState p = CurrentTransactionState;
	TransactionState s;

	/*
	 * We keep subtransaction state nodes in TopTransactionContext.
	 */
	s = (TransactionState)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(TransactionStateData));

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(PUSH_TRANSACTION_BEFORE_PUSH_FAILED, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, NULL, NULL);
#endif

	/*
	 * Assign a subtransaction ID, watching out for counter wraparound.
	 */
	currentSubTransactionId += 1;
	if (currentSubTransactionId == InvalidSubTransactionId)
	{
		currentSubTransactionId -= 1;
		pfree(s);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("cannot have more than 2^32-1 subtransactions in a transaction")));
	}

	
	/*
	 * We can now stack a minimally valid subtransaction without fear of
	 * failure.
	 */
	s->transactionId = InvalidTransactionId;	/* until assigned */
	s->subTransactionId = currentSubTransactionId;
	s->parent = p;
	s->nestingLevel = p->nestingLevel + 1;
	s->gucNestLevel = NewGUCNestLevel();
	s->savepointLevel = p->savepointLevel;
	s->state = TRANS_DEFAULT;
	s->blockState = TBLOCK_SUBBEGIN;
	GetUserIdAndSecContext(&s->prevUser, &s->prevSecContext);
	s->prevXactReadOnly = XactReadOnly;
	s->parallelModeLevel = 0;
	s->curTransactionContext = NULL;

	p->child = s;
	CurrentTransactionState = s;
	
	
	/*
	 * AbortSubTransaction and CleanupSubTransaction have to be able to cope
	 * with the subtransaction from here on out; in particular they should not
	 * assume that it necessarily has a transaction context, resource owner,
	 * or XID.
	 */

#ifdef USE_WHITEBOX_INJECTION
	(void)whitebox_trigger_generic(PUSH_TRANSACTION_AFTER_PUSH_FAILED, WHITEBOX_TRIGGER_DEFAULT,
		INJECTION_LOCATION, NULL, NULL);
#endif

#ifdef _PG_ORCL_
	s->plpgsql_subtrans = 0;
	s->implicit_state = IMPLICIT_SUB_NON;
	s->spi_level = SPI_connect_level();
#endif
}

/*
 * PopTransaction
 *		Pop back to parent transaction state
 *
 *	The caller has to make sure to always reassign CurrentTransactionState
 *	if it has a local pointer to it after calling this function.
 */
static void
PopTransaction(void)
{
	TransactionState s = CurrentTransactionState;

	if (s->state != TRANS_DEFAULT)
		elog(WARNING, "PopTransaction while in %s state",
			 TransStateAsString(s->state));

	if (s->parent == NULL)
		elog(FATAL, "PopTransaction with no parent");
	
	if (s->plpgsql_subtrans > 0)
	{
		s->plpgsql_subtrans = 0;
		g_plsql_excep_sub_level--;
	}

	s->parent->child = NULL;
	CurrentTransactionState = s->parent;

	/* Let's just make sure CurTransactionContext is good */
	CurTransactionContext = s->parent->curTransactionContext;
	MemoryContextSwitchTo(CurTransactionContext);

	/* Ditto for ResourceOwner links */
	CurTransactionResourceOwner = s->parent->curTransactionOwner;
	CurrentResourceOwner = s->parent->curTransactionOwner;

	/* Free the old child structure */
	if (s->name)
		pfree(s->name);
	pfree(s);
}

/*
 * EstimateTransactionStateSpace
 *		Estimate the amount of space that will be needed by
 *		SerializeTransactionState.  It would be OK to overestimate slightly,
 *		but it's simple for us to work out the precise value, so we do.
 */
Size
EstimateTransactionStateSpace(void)
{
	TransactionState s;
	Size		nxids = 6;		/* iso level, deferrable, top & current XID,
								 * command counter, XID count */

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	int			nsub = GetNumSubTransactions();
	nxids++; /* local xid */
	if (nsub > 0)
	{
		nxids = add_size(nxids, nsub); /* local subxids */
	}
	else /* only do for loop below */
	{
#endif
	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		if (TransactionIdIsValid(s->transactionId))
			nxids = add_size(nxids, 1);
		nxids = add_size(nxids, s->nChildXids);
	}
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	}
#endif

	nxids = add_size(nxids, nParallelCurrentXids);

	return mul_size(nxids, sizeof(TransactionId));
}

/*
 * SerializeTransactionState
 *		Write out relevant details of our transaction state that will be
 *		needed by a parallel worker.
 *
 * We need to save and restore XactDeferrable, XactIsoLevel, and the XIDs
 * associated with this transaction.  The first eight bytes of the result
 * contain XactDeferrable and XactIsoLevel; the next twelve bytes contain the
 * XID of the top-level transaction, the XID of the current transaction
 * (or, in each case, InvalidTransactionId if none), and the current command
 * counter.  After that, the next 4 bytes contain a count of how many
 * additional XIDs follow; this is followed by all of those XIDs one after
 * another.  We emit the XIDs in sorted order for the convenience of the
 * receiving process.
 */
void
SerializeTransactionState(Size maxsize, char *start_address)
{
	TransactionState s;
	Size		nxids = 0;
	Size		i = 0;
	Size		c = 0;
	TransactionId *workspace;
	TransactionId *result = (TransactionId *) start_address;
	int			nsub = 0;

	result[c++] = (TransactionId) XactIsoLevel;
	result[c++] = (TransactionId) XactDeferrable;
	result[c++] = XactTopTransactionId;
	result[c++] = CurrentTransactionState->transactionId;
	result[c++] = (TransactionId) currentCommandId;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	result[c++] = (TransactionId) GetNextTransactionId();
#endif
	Assert(maxsize >= c * sizeof(TransactionId));

	/*
	 * If we're running in a parallel worker and launching a parallel worker
	 * of our own, we can just pass along the information that was passed to
	 * us.
	 */
	if (nParallelCurrentXids > 0)
	{
		result[c++] = nParallelCurrentXids;
		Assert(maxsize >= (nParallelCurrentXids + c) * sizeof(TransactionId));
		memcpy(&result[c], ParallelCurrentXids,
			   nParallelCurrentXids * sizeof(TransactionId));
		return;
	}

	/*
	 * OK, we need to generate a sorted list of XIDs that our workers should
	 * view as current.  First, figure out how many there are.
	 */
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	nsub = GetNumSubTransactions();
	if (nsub > 0)
	{
		nxids = add_size(nxids, nsub);
	}
	else
	{
#endif
	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		if (TransactionIdIsValid(s->transactionId))
			nxids = add_size(nxids, 1);
		nxids = add_size(nxids, s->nChildXids);
	}
	Assert((c + 1 + nxids) * sizeof(TransactionId) <= maxsize);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	}
#endif

	/* Copy them to our scratch space. */
	workspace = palloc(nxids * sizeof(TransactionId));
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	if (nsub > 0)
	{
		TransactionId *subxids = GetSubTransactions();
		memcpy(&workspace[i], subxids, nsub * sizeof(TransactionId));
		i += nsub;
	}
	else
	{
#endif
	for (s = CurrentTransactionState; s != NULL; s = s->parent)
	{
		if (TransactionIdIsValid(s->transactionId))
			workspace[i++] = s->transactionId;
		memcpy(&workspace[i], s->childXids,
			   s->nChildXids * sizeof(TransactionId));
		i += s->nChildXids;
	}
	Assert(i == nxids);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	}
#endif

	/* Sort them. */
	qsort(workspace, nxids, sizeof(TransactionId), xidComparator);

	/* Copy data into output area. */
	result[c++] = (TransactionId) nxids;
	memcpy(&result[c], workspace, nxids * sizeof(TransactionId));
}

/*
 * StartParallelWorkerTransaction
 *		Start a parallel worker transaction, restoring the relevant
 *		transaction state serialized by SerializeTransactionState.
 */
void
StartParallelWorkerTransaction(char *tstatespace)
{
	TransactionId *tstate = (TransactionId *) tstatespace;

	Assert(CurrentTransactionState->blockState == TBLOCK_DEFAULT);
	StartTransaction();

	XactIsoLevel = (int) tstate[0];
	XactDeferrable = (bool) tstate[1];
	XactTopTransactionId = tstate[2];
	CurrentTransactionState->transactionId = tstate[3];
	currentCommandId = tstate[4];
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	SetLocalTransactionId(tstate[5]);
	elog(LOG, "deserialize next xid %u", tstate[5]);
#endif
	nParallelCurrentXids = (int) tstate[6];
	ParallelCurrentXids = &tstate[7];

	CurrentTransactionState->blockState = TBLOCK_PARALLEL_INPROGRESS;
}

#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
Size
EstimateGlobalXidSpace(void)
{
	return add_size(NAMEDATALEN, sizeof(int));
}

void
SerializeGlobalXid(Size maxsize, char *start_address)
{
	if (NULL == globalXid)
	{
		*(int *) start_address = 0;
		if (enable_distri_print)
		{
			elog(LOG, "serialize null global xid"); 
		}
	}
	else
	{
		int len = strlen(globalXid) + 1;

		*(int *) start_address = len;
		memcpy(start_address + sizeof(int), globalXid, len);
		
		if(enable_distri_print)
		{
			elog(LOG, "serialize global xid %s", globalXid); 
		}
	}
}

void 
StartParallelWorkerGlobalXid(char *address)
{
	int len;
	char *gxidspace = address + sizeof(int);

	len = *(int *) address;
	
	if(len == 0)
	{
		Assert(globalXid == NULL);
		globalXid = NULL;
		if(enable_distri_print)
		{
			elog(LOG, "start worker null global xid"); 
		}
	}
	else
	{
		if(strlen(gxidspace) + 1 != len)
		{
			elog(ERROR, "global xid is corrupted given len %d gxid len" INT64_FORMAT, len, strlen(gxidspace) + 1);
		}
		
		SetGlobalXid(gxidspace); 
		if(enable_distri_print)
		{
			elog(LOG, "start worker global xid len %d", len); 
		}
	}
}
#endif
/*
 * EndParallelWorkerTransaction
 *		End a parallel worker transaction.
 */
void
EndParallelWorkerTransaction(void)
{
	Assert(CurrentTransactionState->blockState == TBLOCK_PARALLEL_INPROGRESS);
	CommitTransaction();
	CurrentTransactionState->blockState = TBLOCK_DEFAULT;
}

/*
 * ShowTransactionState
 *		Debug support
 */
static void
ShowTransactionState(const char *str)
{
	/* skip work if message will definitely not be printed */
	if (log_min_messages <= DEBUG5 || client_min_messages <= DEBUG5)
		ShowTransactionStateRec(str, CurrentTransactionState);
}

/*
 * ShowTransactionStateRec
 *		Recursive subroutine for ShowTransactionState
 */
static void
ShowTransactionStateRec(const char *str, TransactionState s)
{
	StringInfoData buf;

	initStringInfo(&buf);

	if (s->nChildXids > 0)
	{
		int			i;

		appendStringInfo(&buf, ", children: %u", s->childXids[0]);
		for (i = 1; i < s->nChildXids; i++)
			appendStringInfo(&buf, " %u", s->childXids[i]);
	}

	if (s->parent)
		ShowTransactionStateRec(str, s->parent);

	/* use ereport to suppress computation if msg will not be printed */
	ereport(DEBUG5,
			(errmsg_internal("%s(%d) name: %s; blockState: %s; state: %s, xid/subid/cid: %u/%u/%u%s%s",
							 str, s->nestingLevel,
							 PointerIsValid(s->name) ? s->name : "unnamed",
							 BlockStateAsString(s->blockState),
							 TransStateAsString(s->state),
							 (unsigned int) s->transactionId,
							 (unsigned int) s->subTransactionId,
							 (unsigned int) currentCommandId,
							 currentCommandIdUsed ? " (used)" : "",
							 buf.data)));

	pfree(buf.data);
}

/*
 * BlockStateAsString
 *		Debug support
 */
static const char *
BlockStateAsString(TBlockState blockState)
{
	switch (blockState)
	{
		case TBLOCK_DEFAULT:
			return "DEFAULT";
		case TBLOCK_STARTED:
			return "STARTED";
		case TBLOCK_BEGIN:
			return "BEGIN";
		case TBLOCK_INPROGRESS:
			return "INPROGRESS";
		case TBLOCK_PARALLEL_INPROGRESS:
			return "PARALLEL_INPROGRESS";
		case TBLOCK_END:
			return "END";
		case TBLOCK_ABORT:
			return "ABORT";
		case TBLOCK_ABORT_END:
			return "ABORT END";
		case TBLOCK_ABORT_PENDING:
			return "ABORT PEND";
		case TBLOCK_PREPARE:
			return "PREPARE";
		case TBLOCK_SUBBEGIN:
			return "SUB BEGIN";
		case TBLOCK_SUBINPROGRESS:
			return "SUB INPROGRS";
		case TBLOCK_SUBRELEASE:
			return "SUB RELEASE";
		case TBLOCK_SUBCOMMIT:
			return "SUB COMMIT";
		case TBLOCK_SUBABORT:
			return "SUB ABORT";
		case TBLOCK_SUBABORT_END:
			return "SUB ABORT END";
		case TBLOCK_SUBABORT_PENDING:
			return "SUB ABRT PEND";
		case TBLOCK_SUBRESTART:
			return "SUB RESTART";
		case TBLOCK_SUBABORT_RESTART:
			return "SUB AB RESTRT";
	}
	return "UNRECOGNIZED";
}

/*
 * TransStateAsString
 *		Debug support
 */
static const char *
TransStateAsString(TransState state)
{
	switch (state)
	{
		case TRANS_DEFAULT:
			return "DEFAULT";
		case TRANS_START:
			return "START";
		case TRANS_INPROGRESS:
			return "INPROGR";
		case TRANS_COMMIT:
			return "COMMIT";
		case TRANS_ABORT:
			return "ABORT";
		case TRANS_PREPARE:
			return "PREPARE";
	}
	return "UNRECOGNIZED";
}

/*
 * xactGetCommittedChildren
 *
 * Gets the list of committed children of the current transaction.  The return
 * value is the number of child transactions.  *ptr is set to point to an
 * array of TransactionIds.  The array is allocated in TopTransactionContext;
 * the caller should *not* pfree() it (this is a change from pre-8.4 code!).
 * If there are no subxacts, *ptr is set to NULL.
 */
int
xactGetCommittedChildren(TransactionId **ptr)
{
	TransactionState s = CurrentTransactionState;

	if (s->nChildXids == 0)
		*ptr = NULL;
	else
		*ptr = s->childXids;

	return s->nChildXids;
}

/*
 *	XLOG support routines
 */


/*
 * Log the commit record for a plain or twophase transaction commit.
 *
 * A 2pc commit will be emitted when twophase_xid is valid, a plain one
 * otherwise.
 */
XLogRecPtr
XactLogCommitRecord(CommitSeqNo gts,
					TimestampTz	 commit_time,
					int nsubxacts, TransactionId *subxacts,
					int nrels, RelFileNodeNFork *rels,
					int nmsgs, SharedInvalidationMessage *msgs,
					bool relcacheInval, bool forceSync,
					int xactflags, TransactionId twophase_xid)
{
	xl_xact_commit xlrec;
	xl_xact_xinfo xl_xinfo;
	xl_xact_dbinfo xl_dbinfo;
	xl_xact_subxacts xl_subxacts;
	xl_xact_relfilenodes xl_relfilenodes;
	xl_xact_invals xl_invals;
	xl_xact_twophase xl_twophase;
	xl_xact_origin xl_origin;
#ifdef __OPENTENBASE__
	XLogRecPtr    end_lsn;
#endif
    TransactionId oldest_active_xid;
	uint8		info;

	Assert(CritSectionCount > 0);

	xl_xinfo.xinfo = 0;

	/* decide between a plain and 2pc commit */
	if (!TransactionIdIsValid(twophase_xid))
		info = XLOG_XACT_COMMIT;
	else
		info = XLOG_XACT_COMMIT_PREPARED;

	/* First figure out and collect all the information needed */

	xlrec.xact_time = commit_time;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	xlrec.gts = gts;
#endif

#ifdef __OPENTENBASE__
    segmentTrackGTS = gts;
#endif

	if (relcacheInval)
		xl_xinfo.xinfo |= XACT_COMPLETION_UPDATE_RELCACHE_FILE;
	if (forceSyncCommit)
		xl_xinfo.xinfo |= XACT_COMPLETION_FORCE_SYNC_COMMIT;
	if ((xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK))
		xl_xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;

	/*
	 * Check if the caller would like to ask standbys for immediate feedback
	 * once this commit is applied.
	 */
	if (synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY)
		xl_xinfo.xinfo |= XACT_COMPLETION_APPLY_FEEDBACK;

	/*
	 * Relcache invalidations requires information about the current database
	 * and so does logical decoding.
	 */
	if (nmsgs > 0 || XLogLogicalInfoActive())
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_DBINFO;
		xl_dbinfo.dbId = MyDatabaseId;
		xl_dbinfo.tsId = MyDatabaseTableSpace;
	}

	if (nsubxacts > 0)
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
		xl_subxacts.nsubxacts = nsubxacts;
	}

	if (nrels > 0)
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_RELFILENODES;
		xl_relfilenodes.nrels = nrels;
	}

	if (nmsgs > 0)
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_INVALS;
		xl_invals.nmsgs = nmsgs;
	}

	if (TransactionIdIsValid(twophase_xid))
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_TWOPHASE;
		xl_twophase.xid = twophase_xid;
	}

	/* dump transaction origin information */
	if (replorigin_session_origin != InvalidRepOriginId)
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_ORIGIN;

		xl_origin.origin_lsn = replorigin_session_origin_lsn;
		xl_origin.origin_timestamp = replorigin_session_origin_timestamp;
	}

    oldest_active_xid = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);
    if (TransactionIdIsValid(oldest_active_xid))
    {
	    xl_xinfo.xinfo |= XACT_XINFO_HAS_OLDEST_ACTIVE_XID;
    }

	if (xl_xinfo.xinfo != 0)
		info |= XLOG_XACT_HAS_INFO;

	/* Then include all the collected data into the commit record. */

	XLogBeginInsert();

	XLogRegisterData((char *) (&xlrec), sizeof(xl_xact_commit));

	if (xl_xinfo.xinfo != 0)
		XLogRegisterData((char *) (&xl_xinfo.xinfo), sizeof(xl_xinfo.xinfo));

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_DBINFO)
		XLogRegisterData((char *) (&xl_dbinfo), sizeof(xl_dbinfo));

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_SUBXACTS)
	{
		XLogRegisterData((char *) (&xl_subxacts),
						 MinSizeOfXactSubxacts);
		XLogRegisterData((char *) subxacts,
						 nsubxacts * sizeof(TransactionId));
	}

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_RELFILENODES)
	{
		XLogRegisterData((char *) (&xl_relfilenodes),
						 MinSizeOfXactRelfilenodes);
		XLogRegisterData((char *) rels,
						 nrels * sizeof(RelFileNodeNFork));
	}

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_INVALS)
	{
		XLogRegisterData((char *) (&xl_invals), MinSizeOfXactInvals);
		XLogRegisterData((char *) msgs,
						 nmsgs * sizeof(SharedInvalidationMessage));
	}

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_TWOPHASE)
		XLogRegisterData((char *) (&xl_twophase), sizeof(xl_xact_twophase));

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_ORIGIN)
		XLogRegisterData((char *) (&xl_origin), sizeof(xl_xact_origin));
	
    if (xl_xinfo.xinfo & XACT_XINFO_HAS_OLDEST_ACTIVE_XID)
    {
        XLogRegisterData((char *)(&oldest_active_xid), sizeof(TransactionId));
    }
	/* we allow filtering by xacts */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);
	end_lsn = XLogInsert(RM_XACT_ID, info);
	
#ifdef __OPENTENBASE__
	{
		uint32		id,
					off;
		/* Decode ID and offset */
		id = (uint32) (end_lsn >> 32);
		off = (uint32) end_lsn;

		elog(DEBUG5, "XactLogCommitRecord %s, xid=%d, lsn=%X/%X, gts=" UINT64_FORMAT ", abort_time=%s, twophase_xid=%d",
				(info & XLOG_XACT_COMMIT) ? "COMMIT" : "COMMIT_PREPARED",
				GetCurrentTransactionIdIfAny(), id, off,
				gts,
				timestamptz_to_str(commit_time),
				twophase_xid);
	}
#endif

	return end_lsn;
}

/*
 * Log the commit record for a plain or twophase transaction abort.
 *
 * A 2pc abort will be emitted when twophase_xid is valid, a plain one
 * otherwise.
 */
XLogRecPtr
XactLogAbortRecord(CommitSeqNo global_timestamp,
					TimestampTz	 abort_time,
				   int nsubxacts, TransactionId *subxacts,
				   int nrels, RelFileNodeNFork *rels,
				   int xactflags, TransactionId twophase_xid)
{
	xl_xact_abort xlrec;
	xl_xact_xinfo xl_xinfo;
	xl_xact_subxacts xl_subxacts;
	xl_xact_relfilenodes xl_relfilenodes;
	xl_xact_twophase xl_twophase;
#ifdef __OPENTENBASE__
	XLogRecPtr	  end_lsn;
#endif
    TransactionId oldest_active_xid;
	uint8		info;
    
	Assert(CritSectionCount > 0);

	xl_xinfo.xinfo = 0;

	/* decide between a plain and 2pc abort */
	if (!TransactionIdIsValid(twophase_xid))
		info = XLOG_XACT_ABORT;
	else
		info = XLOG_XACT_ABORT_PREPARED;


	/* First figure out and collect all the information needed */

	xlrec.xact_time = abort_time;
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	xlrec.gts = global_timestamp;
#endif
#ifdef __OPENTENBASE__
    segmentTrackGTS = global_timestamp > segmentTrackGTS ? global_timestamp : segmentTrackGTS;
#endif

	if ((xactflags & XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK))
		xl_xinfo.xinfo |= XACT_XINFO_HAS_AE_LOCKS;

	if (nsubxacts > 0)
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_SUBXACTS;
		xl_subxacts.nsubxacts = nsubxacts;
	}

	if (nrels > 0)
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_RELFILENODES;
		xl_relfilenodes.nrels = nrels;
	}

	if (TransactionIdIsValid(twophase_xid))
	{
		xl_xinfo.xinfo |= XACT_XINFO_HAS_TWOPHASE;
		xl_twophase.xid = twophase_xid;
	}

	oldest_active_xid = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);
    if (TransactionIdIsValid(oldest_active_xid))
    {
	    xl_xinfo.xinfo |= XACT_XINFO_HAS_OLDEST_ACTIVE_XID;
    }

    if (xl_xinfo.xinfo != 0)
		info |= XLOG_XACT_HAS_INFO;
	/* Then include all the collected data into the abort record. */

	XLogBeginInsert();

	XLogRegisterData((char *) (&xlrec), MinSizeOfXactAbort);

	if (xl_xinfo.xinfo != 0)
		XLogRegisterData((char *) (&xl_xinfo), sizeof(xl_xinfo));

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_SUBXACTS)
	{
		XLogRegisterData((char *) (&xl_subxacts),
						 MinSizeOfXactSubxacts);
		XLogRegisterData((char *) subxacts,
						 nsubxacts * sizeof(TransactionId));
	}

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_RELFILENODES)
	{
		XLogRegisterData((char *) (&xl_relfilenodes),
						 MinSizeOfXactRelfilenodes);
		XLogRegisterData((char *) rels,
						 nrels * sizeof(RelFileNodeNFork));
	}

	if (xl_xinfo.xinfo & XACT_XINFO_HAS_TWOPHASE)
		XLogRegisterData((char *) (&xl_twophase), sizeof(xl_xact_twophase));

    if (xl_xinfo.xinfo & XACT_XINFO_HAS_OLDEST_ACTIVE_XID)
    {
        XLogRegisterData((char *)(&oldest_active_xid), sizeof(TransactionId));
    }
	end_lsn = XLogInsert(RM_XACT_ID, info);

#ifdef __OPENTENBASE__
	{
		uint32		id,
					off;
		/* Decode ID and offset */
		id = (uint32) (end_lsn >> 32);
		off = (uint32) end_lsn;

		elog(DEBUG5, "XactLogAbortRecord %s, xid=%d, lsn=%X/%X, global_timestamp=%lu, abort_time=%s, twophase_xid=%d",
				(info & XLOG_XACT_ABORT) ? "ABORT" : "ABORT_PREPARED",
				GetCurrentTransactionIdIfAny(), id, off,
				global_timestamp,
				timestamptz_to_str(abort_time),
				twophase_xid);
	}
#endif

	return end_lsn;
}

/* This function cannot be used to process segment tables. */
void
PushUnlinkRelToHTAB(RelFileNodeNFork *xnodes, int nrels)
{
	HTAB *relfilenode_hashtbl = UnlinkRelInfo->unlink_rel_hashtbl;
	int del_rel_num = 0;
	int i;

	if (nrels == 0)
		return;

	LWLockAcquire(&UnlinkRelInfo->rel_hashtbl_lock, LW_EXCLUSIVE);
	for (i = 0; i < nrels; i++)
	{
		RelFileNodeNFork *colFileNodeRel = &xnodes[i];
		bool found = false;
		RelFileNode *entry = NULL;

		entry = (RelFileNode*)hash_search(relfilenode_hashtbl,
										&(colFileNodeRel->rnode), HASH_ENTER, &found);
		if (!found)
		{
			entry->spcNode = colFileNodeRel->rnode.spcNode;
			entry->dbNode = colFileNodeRel->rnode.dbNode;
			entry->relNode = colFileNodeRel->rnode.relNode;
			del_rel_num++;
		}
	}
	LWLockRelease(&UnlinkRelInfo->rel_hashtbl_lock);

	if (del_rel_num > 0 && UnlinkRelInfo->invalid_buf_proc_latch != NULL)
	{
		SetLatch(UnlinkRelInfo->invalid_buf_proc_latch);
	}
	return;
}

/*
 * Only called during the replay process on the standby, and on the standby, 
 * there will be only one process performing updates, so no need for locking.
 */
static void
xact_redo_update_oldexst_active_xid(TransactionId xid)
{
    TransactionId old_vaule = pg_atomic_read_u32(&ShmemVariableCache->oldestActiveXid);
    Assert(TransactionIdIsValid(xid));
    if (TransactionIdFollows(xid, old_vaule))
    {
        pg_atomic_write_u32(&ShmemVariableCache->oldestActiveXid, xid);
    }
}

/*
 * Before 9.0 this was a fairly short function, but now it performs many
 * actions for which the order of execution is critical.
 */
static void
xact_redo_commit(xl_xact_parsed_commit *parsed,
				 TransactionId xid,
				 XLogRecPtr lsn,
				 RepOriginId origin_id)
{
	TransactionId max_xid;
	int			i;
	TimestampTz commit_time;
	CommitSeqNo global_timestamp;

	Assert(TransactionIdIsValid(xid));

	max_xid = TransactionIdLatest(xid, parsed->nsubxacts, parsed->subxacts);

	/*
	 * Make sure nextXid is beyond any XID mentioned in the record.
	 *
	 * We don't expect anyone else to modify nextXid, hence we don't need to
	 * hold a lock while checking this. We still acquire the lock to modify
	 * it, though.
	 */
	if (TransactionIdFollowsOrEquals(max_xid,
									 ShmemVariableCache->nextXid))
	{
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		ShmemVariableCache->nextXid = max_xid;
		TransactionIdAdvance(ShmemVariableCache->nextXid);
		LWLockRelease(XidGenLock);
	}

	Assert(((parsed->xinfo & XACT_XINFO_HAS_ORIGIN) == 0) ==
		   (origin_id == InvalidRepOriginId));

	if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
		commit_time = parsed->origin_timestamp;
	else
		commit_time = parsed->xact_time;
	
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	global_timestamp = parsed->gts;
	if (!CSN_IS_COMMITTED(global_timestamp) || !TransactionIdIsValid(xid))
	{
		elog(ERROR, "commit gts or xid is invalid for xid %d gts " UINT64_FORMAT, xid, global_timestamp);
	}
    
    SetAllSharedCommitTs(global_timestamp);
    
	/* Set the transaction commit timestamp and metadata */
	
	elog(DEBUG10, "xact redo commit ts xid %d committs "INT64_FORMAT, xid, commit_time);
	TransactionTreeSetCommitTsData(xid, parsed->nsubxacts, parsed->subxacts,
								   global_timestamp, commit_time, origin_id, false, InvalidXLogRecPtr);
#else


	/* Set the transaction commit timestamp and metadata */
	TransactionTreeSetCommitTsData(xid, parsed->nsubxacts, parsed->subxacts,
								   InvalidGlobalTimestamp, commit_time, origin_id, false, InvalidXLogRecPtr);
#endif
	if (standbyState == STANDBY_DISABLED)
	{
		/*
		 * Mark the transaction committed in pg_xact.
		 */
		TransactionIdCommitTree(xid, parsed->nsubxacts, parsed->subxacts, global_timestamp);
	}
	else
	{
		/*
		 * If a transaction completion record arrives that has as-yet
		 * unobserved subtransactions then this will not have been fully
		 * handled by the call to RecordKnownAssignedTransactionIds() in the
		 * main recovery loop in xlog.c. So we need to do bookkeeping again to
		 * cover that case. This is confusing and it is easy to think this
		 * call is irrelevant, which has happened three times in development
		 * already. Leave it in.
		 */
		RecordKnownAssignedTransactionIds(max_xid);

		/*
		 * Mark the transaction committed in pg_xact. We use async commit
		 * protocol during recovery to provide information on database
		 * consistency for when users try to set hint bits. It is important
		 * that we do not set hint bits until the minRecoveryPoint is past
		 * this commit record. This ensures that if we crash we don't see hint
		 * bits set on changes made by transactions that haven't yet
		 * recovered. It's unlikely but it's good to be safe.
		 */
		TransactionIdAsyncCommitTree(xid, parsed->nsubxacts, parsed->subxacts, lsn, 
                                     global_timestamp);

        /*
		 * We must mark csnlog before we update the ProcArray.
		 */
        ExpireTreeKnownAssignedTransactionIds(
                xid, parsed->nsubxacts, parsed->subxacts, max_xid);
		/*
		 * Send any cache invalidations attached to the commit. We must
		 * maintain the same order of invalidation then release locks as
		 * occurs in CommitTransaction().
		 */
		ProcessCommittedInvalidationMessages(
											 parsed->msgs, parsed->nmsgs,
											 XactCompletionRelcacheInitFileInval(parsed->xinfo),
											 parsed->dbId, parsed->tsId);

		/*
		 * Release locks, if any. We do this for both two phase and normal one
		 * phase transactions. In effect we are ignoring the prepare phase and
		 * just going straight to lock release. At commit we release all locks
		 * via their top-level xid only, so no need to provide subxact list,
		 * which will save time when replaying commits.
		 */
		if (parsed->xinfo & XACT_XINFO_HAS_AE_LOCKS)
			StandbyReleaseLockTree(xid, 0, NULL);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_ORIGIN)
	{
		/* recover apply progress */
		replorigin_advance(origin_id, parsed->origin_lsn, lsn,
						   false /* backward */ , false /* WAL */ );
	}

	if (parsed->xinfo & XACT_XINFO_HAS_OLDEST_ACTIVE_XID)
	    xact_redo_update_oldexst_active_xid(parsed->oldest_active_xid);

	/* Make sure files supposed to be dropped are dropped */
	if (parsed->nrels > 0)
	{
		/*
		 * First update minimum recovery point to cover this WAL record. Once
		 * a relation is deleted, there's no going back. The buffer manager
		 * enforces the WAL-first rule for normal updates to relation files,
		 * so that the minimum recovery point is always updated before the
		 * corresponding change in the data file is flushed to disk, but we
		 * have to do the same here since we're bypassing the buffer manager.
		 *
		 * Doing this before deleting the files means that if a deletion fails
		 * for some reason, you cannot start up the system even after restart,
		 * until you fix the underlying situation so that the deletion will
		 * succeed. Alternatively, we could update the minimum recovery point
		 * after deletion, but that would leave a small window where the
		 * WAL-first rule would be violated.
		 */
		XLogFlush(lsn);

		PushUnlinkRelToHTAB(parsed->xnodes, parsed->nrels);
		for (i = 0; i < parsed->nrels; i++)
		{
			SMgrRelation srel = smgropen(parsed->xnodes[i].rnode, InvalidBackendId);
			ForkNumber	fork;

			for (fork = 0; fork <= MAX_FORKNUM; fork++)
			{
				XLogDropRelation(parsed->xnodes[i].rnode, fork);
			}
			smgrdounlink(srel, true);
			smgrclose(srel);
		}
	}

	/*
	 * We issue an XLogFlush() for the same reason we emit ForceSyncCommit()
	 * in normal operation. For example, in CREATE DATABASE, we copy all files
	 * from the template database, and then commit the transaction. If we
	 * crash after all the files have been copied but before the commit, you
	 * have files in the data directory without an entry in pg_database. To
	 * minimize the window for that, we use ForceSyncCommit() to rush the
	 * commit record to disk as quick as possible. We have the same window
	 * during recovery, and forcing an XLogFlush() (which updates
	 * minRecoveryPoint during recovery) helps to reduce that problem window,
	 * for any user that requested ForceSyncCommit().
	 */
	if (XactCompletionForceSyncCommit(parsed->xinfo))
		XLogFlush(lsn);

	/*
	 * If asked by the primary (because someone is waiting for a synchronous
	 * commit = remote_apply), we will need to ask walreceiver to send a reply
	 * immediately.
	 */
	if (XactCompletionApplyFeedback(parsed->xinfo))
		XLogRequestWalReceiverReply();
}

/*
 * Be careful with the order of execution, as with xact_redo_commit().
 * The two functions are similar but differ in key places.
 *
 * Note also that an abort can be for a subtransaction and its children,
 * not just for a top level abort. That means we have to consider
 * topxid != xid, whereas in commit we would find topxid == xid always
 * because subtransaction commit is never WAL logged.
 */
static void
xact_redo_abort(xl_xact_parsed_abort *parsed, TransactionId xid)
{
	int			i;
	TransactionId max_xid;

	Assert(TransactionIdIsValid(xid));
    
    SetAllSharedCommitTs(parsed->gts);

	/*
	 * Make sure nextXid is beyond any XID mentioned in the record.
	 *
	 * We don't expect anyone else to modify nextXid, hence we don't need to
	 * hold a lock while checking this. We still acquire the lock to modify
	 * it, though.
	 */
	max_xid = TransactionIdLatest(xid,
								  parsed->nsubxacts,
								  parsed->subxacts);

	if (TransactionIdFollowsOrEquals(max_xid,
									 ShmemVariableCache->nextXid))
	{
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		ShmemVariableCache->nextXid = max_xid;
		TransactionIdAdvance(ShmemVariableCache->nextXid);
		LWLockRelease(XidGenLock);
	}

	if (standbyState == STANDBY_DISABLED)
	{
		/* Mark the transaction aborted in pg_xact, no need for async stuff */
		TransactionIdAbortTree(xid, parsed->nsubxacts, parsed->subxacts);
	}
	else
	{
		/*
		 * If a transaction completion record arrives that has as-yet
		 * unobserved subtransactions then this will not have been fully
		 * handled by the call to RecordKnownAssignedTransactionIds() in the
		 * main recovery loop in xlog.c. So we need to do bookkeeping again to
		 * cover that case. This is confusing and it is easy to think this
		 * call is irrelevant, which has happened three times in development
		 * already. Leave it in.
		 */
		RecordKnownAssignedTransactionIds(max_xid);

		/* Mark the transaction aborted in pg_xact, no need for async stuff */
		TransactionIdAbortTree(xid, parsed->nsubxacts, parsed->subxacts);

		/*
		 * We must update the ProcArray after we have marked csnlog.
		 */
		ExpireTreeKnownAssignedTransactionIds(
											  xid, parsed->nsubxacts, parsed->subxacts, max_xid);

		/*
		 * There are no flat files that need updating, nor invalidation
		 * messages to send or undo.
		 */

		/*
		 * Release locks, if any. There are no invalidations to send.
		 */
		if (parsed->xinfo & XACT_XINFO_HAS_AE_LOCKS)
			StandbyReleaseLockTree(xid, parsed->nsubxacts, parsed->subxacts);
	}

	if (parsed->xinfo & XACT_XINFO_HAS_OLDEST_ACTIVE_XID)
	    xact_redo_update_oldexst_active_xid(parsed->oldest_active_xid);

	// TRS: Deal with Estore

	PushUnlinkRelToHTAB(parsed->xnodes, parsed->nrels);
	/* Make sure files supposed to be dropped are dropped */
	for (i = 0; i < parsed->nrels; i++)
	{
		SMgrRelation srel = smgropen(parsed->xnodes[i].rnode, InvalidBackendId);
		ForkNumber	fork;
		for (fork = 0; fork <= MAX_FORKNUM; fork++)
		{
			XLogDropRelation(parsed->xnodes[i].rnode, fork);
		}
		smgrdounlink(srel, true);
		smgrclose(srel);
}
}

void
xact_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

	/* Backup blocks are not used in xact records */
	Assert(!XLogRecHasAnyBlockRefs(record));

	if (info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
		xl_xact_parsed_commit parsed;

		ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xact_redo_commit(&parsed, XLogRecGetXid(record),
						 record->EndRecPtr, XLogRecGetOrigin(record));
	}
	else if (info == XLOG_XACT_COMMIT_PREPARED)
	{
		xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
		xl_xact_parsed_commit parsed;

		ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xact_redo_commit(&parsed, parsed.twophase_xid,
						 record->EndRecPtr, XLogRecGetOrigin(record));

		/* Delete TwoPhaseState gxact entry and/or 2PC file. */
		LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
		record_2pc_redo_remove_gid_xid(parsed.twophase_xid);
        PrepareRedoRemove(parsed.twophase_xid, false);
		LWLockRelease(TwoPhaseStateLock);
	}
	else if (info == XLOG_XACT_ABORT)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		ParseAbortRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xact_redo_abort(&parsed, XLogRecGetXid(record));
	}
	else if (info == XLOG_XACT_ABORT_PREPARED)
	{
		xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		ParseAbortRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xact_redo_abort(&parsed, parsed.twophase_xid);

		/* Delete TwoPhaseState gxact entry and/or 2PC file. */
		LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
		record_2pc_redo_remove_gid_xid(parsed.twophase_xid);
		PrepareRedoRemove(parsed.twophase_xid, false);
		LWLockRelease(TwoPhaseStateLock);
	}
	else if (info == XLOG_XACT_PREPARE)
	{
		/*
		 * Store xid and start/end pointers of the WAL record in TwoPhaseState
		 * gxact entry.
		 */
		LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
		PrepareRedoAdd(XLogRecGetData(record),
					   record->ReadRecPtr,
					   record->EndRecPtr);
		LWLockRelease(TwoPhaseStateLock);
	}
	else if (info == XLOG_XACT_ASSIGNMENT)
	{
		xl_xact_assignment *xlrec = (xl_xact_assignment *) XLogRecGetData(record);

		if (standbyState >= STANDBY_INITIALIZED)
			ProcArrayApplyXidAssignment(xlrec->xtop,
										xlrec->nsubxacts, xlrec->xsub);
	}
#ifdef __OPENTENBASE__
	else if (info == XLOG_XACT_ACQUIRE_GTS)
	{
		xl_xact_acquire_gts *xlrec = (xl_xact_acquire_gts *) XLogRecGetData(record);

        SetSharedLatestCommitTS(xlrec->global_timestamp);
	}
#endif
	else
		elog(PANIC, "xact_redo: unknown op code %u", info);
}

#ifdef __TWO_PHASE_TESTS__
void ClearTwophaseException(void)
{
    twophase_in = 0;
    capacity_stack = 0;
    exception_count = 0;
    complish = false;
}
#endif


#ifdef PGXC
/*
 * Remember that the local node has done some write activity
 */
void
RegisterTransactionLocalNode(bool write)
{
	if (write)
	{
		XactWriteLocalNode = true;
		XactReadLocalNode = false;
	}
	else
		XactReadLocalNode = true;
}

/*
 * Forget about the local node's involvement in the transaction
 */
void
ForgetTransactionLocalNode(void)
{
	XactReadLocalNode = XactWriteLocalNode = false;
}

/*
 * Check if the local node is involved in the transaction
 */
bool
IsTransactionLocalNode(bool write)
{
	if (write && XactWriteLocalNode)
		return true;
	else if (!write && XactReadLocalNode)
		return true;
	else
		return false;
}

/*
 * Check if the given xid is form implicit 2PC
 */
bool
IsXidImplicit(const char *xid)
{
#define implicit2PC_head "_$XC$"
	const size_t implicit2PC_head_len = strlen(implicit2PC_head);

	if (strncmp(xid, implicit2PC_head, implicit2PC_head_len))
		return false;
	return true;
}

/*
 * SaveReceivedCommandId
 * Save a received command ID from another node for future use.
 */
void
SaveReceivedCommandId(CommandId cid)
{
	/* Set the new command ID */
	SetReceivedCommandId(cid);

	/*
	 * Change command ID information status to report any changes in remote ID
	 * for a remote node. A new command ID has also been received.
	 */
	{
		SetSendCommandId(true);
		isCommandIdReceived = true;
	}
}

/*
 * SetReceivedCommandId
 * Set the command Id received from other nodes
 */
void
SetReceivedCommandId(CommandId cid)
{
	receivedCommandId = cid;
	currentCommandId = cid;
}

/*
 * GetReceivedCommandId
 * Get the command id received from other nodes
 */
CommandId
GetReceivedCommandId(void)
{
	return receivedCommandId;
}


/*
 * ReportCommandIdChange
 * ReportCommandIdChange reports a change in current command id at remote node
 * to the Coordinator. This is required because a remote node can increment command
 * Id in case of triggers or constraints.
 */
void
ReportCommandIdChange(CommandId cid, bool flush_immediately)
{
	StringInfoData buf;

	/* Send command Id change to Coordinator */
	pq_beginmessage(&buf, 'M');
	pq_sendint32(&buf, cid);
	pq_endmessage(&buf);
	if (flush_immediately)
		pq_flush();
}

/*
 * IsSendCommandId
 * Get status of command ID sending. If set at true, command ID needs to be communicated
 * to other nodes.
 */
bool
IsSendCommandId(void)
{
	return sendCommandId;
}

/*
 * SetSendCommandId
 * Change status of command ID sending.
 */
void
SetSendCommandId(bool status)
{
	sendCommandId = status;
}

/*
 * IsPGXCNodeXactReadOnly
 * Determine if a Postgres-XC node session
 * is read-only or not.
 */
bool
IsPGXCNodeXactReadOnly(void)
{
	/*
	 * For the time being a Postgres-XC session is read-only
	 * under very specific conditions.
	 * This is the case of an application accessing directly
	 * a Datanode provided the server was not started in restore mode.
	 */
	return IsPGXCNodeXactDatanodeDirect() && !isRestoreMode;
}

/*
 * IsPGXCNodeXactDatanodeDirect
 * Determine if a Postgres-XC node session
 * is being accessed directly by an application.
 */
bool
IsPGXCNodeXactDatanodeDirect(void)
{
	/*
	 * For the time being a Postgres-XC session is considered
	 * as being connected directly under very specific conditions.
	 *
	 * IsPostmasterEnvironment || !useLocalXid
	 *     All standalone backends except initdb are considered to be
	 *     "directly connected" by application, which implies that for xid
	 *     consistency, the backend should use global xids. initdb is the only
	 *     one where local xids are used. So any standalone backend except
	 *     initdb is supposed to use global xids.
	 * IsNormalProcessingMode() - checks for new connections
	 * IsAutoVacuumLauncherProcess - checks for autovacuum launcher process
	 */
#ifdef __OPENTENBASE_C__
	return IS_PGXC_DATANODE &&
	#ifdef _SHARDING_
			!g_allow_dml_on_datanode &&
	#endif
		   (IsPostmasterEnvironment || !useLocalXid) &&
		   IsNormalProcessingMode() &&
		   !IsAutoVacuumLauncherProcess() &&
	#ifdef XCP
		   !IsConnFromDatanode() &&
	#endif
		   !IsConnFromCoord();
#else
		return IS_PGXC_DATANODE &&
	#ifdef _SHARDING_
			!g_allow_dml_on_datanode &&
	#endif
			   (IsPostmasterEnvironment || !useLocalXid) &&
			   IsNormalProcessingMode() &&
			   !IsAutoVacuumLauncherProcess() &&
		   !IsClean2pcLauncher() &&
	#ifdef XCP
			   !IsConnFromDatanode() &&
	#endif
			   !IsConnFromCoord();
#endif
}

#ifdef XCP
static void
TransactionRecordXidWait_Internal(TransactionState s, TransactionId xid)
{
	int i;

#ifndef __USE_GLOBAL_SNAPSHOT__
	return;
#endif
	
	if (s->waitedForXids == NULL)
	{
		/*
		 * XIDs recorded on the local coordinator, which are mostly collected
		 * from various remote nodes, must survive across transaction
		 * boundaries since they are sent to the GTM after local transaction is
		 * committed. So we track them in the TopMemoryContext and make extra
		 * efforts to free that memory later. Note that we do this only when we
		 * are running in the top-level transaction. For subtranctions, they
		 * will be copied to the parent transaction at the commit time. So at
		 * subtranction level, they can be tracked in a transaction-local
		 * memory without any problem
		 */
		if (IS_PGXC_LOCAL_COORDINATOR && (s->parent == NULL))
			s->waitedForXids = (TransactionId *)
				MemoryContextAlloc(TopMemoryContext, sizeof (TransactionId) *
						MaxConnections);
		else
			s->waitedForXids = (TransactionId *)
				MemoryContextAlloc(CurTransactionContext, sizeof (TransactionId) *
						MaxConnections);

		s->waitedForXidsCount = 0;
	}

	elog(DEBUG2, "TransactionRecordXidWait_Internal - recording %d", xid);

	for (i = 0; i < s->waitedForXidsCount; i++)
	{
		if (TransactionIdEquals(xid, s->waitedForXids[i]))
		{
			elog(DEBUG2, "TransactionRecordXidWait_Internal - xid %d already recorded", xid);
			return;
		}
	}

	/*
	 * We track maximum MaxConnections xids. In case of overflow, we forget the
	 * earliest recorded xid. That should be enough for all practical purposes
	 * since what we are guarding against is a very small window where a
	 * transaction which is already committed on a datanode has not yet got an
	 * opportunity to send its status to the GTM. Such transactions can only be
	 * running on a different coordinator session. So tracking MaxConnections
	 * worth xids seems like more than enough
	 */
	if (s->waitedForXidsCount == MaxConnections)
	{
		memmove(&s->waitedForXids[0],
				&s->waitedForXids[1],
				(s->waitedForXidsCount - 1) * sizeof (TransactionId));
		s->waitedForXids[s->waitedForXidsCount - 1] = xid;
	}
	else
		s->waitedForXids[s->waitedForXidsCount++] = xid;

}

void
TransactionRecordXidWait(TransactionId xid)
{
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
	return;
#endif
	TransactionRecordXidWait_Internal(CurrentTransactionState, xid);
}

static void
AtSubCommit_WaitedXids()
{
	TransactionState s = CurrentTransactionState;
	int i;

	Assert(s->parent != NULL);

	/*
	 * Move the recorded XIDs to the parent structure
	 */
	for (i = 0; i < s->waitedForXidsCount; i++)
		TransactionRecordXidWait_Internal(s->parent, s->waitedForXids[i]);

	/* And we can safely free them now */
	if (s->waitedForXids != NULL)
		pfree(s->waitedForXids);
	s->waitedForXids = NULL;
	s->waitedForXidsCount = 0;
}

static void
AtSubAbort_WaitedXids()
{
	TransactionState s = CurrentTransactionState;

	if (s->waitedForXids != NULL)
		pfree(s->waitedForXids);
	s->waitedForXids = NULL;
	s->waitedForXidsCount = 0;

}

static void
AtEOXact_WaitedXids(void)
{
	TransactionState s = CurrentTransactionState;
	TransactionId *sendXids;

#ifndef __USE_GLOBAL_SNAPSHOT__
	return;
#endif

	/*
	 * Report the set of XIDs this transaction (and its committed
	 * subtransactions) had waited-for to the coordinator. The coordinator will
	 * then forward the list to the GTM who ensures that the logical ordering
	 * between these transactions and this transaction is correctly followed.
	 */
	if (whereToSendOutput == DestRemote && !IS_PGXC_LOCAL_COORDINATOR)
	{
		if (s->waitedForXidsCount > 0)
		{
			int i;

			/*
			 * Convert the XIDs in network order and send to the client
			 */
			sendXids = (TransactionId *) palloc (sizeof (TransactionId) *
					s->waitedForXidsCount);
			for (i = 0; i < s->waitedForXidsCount; i++)
				sendXids[i] = pg_hton32(s->waitedForXids[i]);

			pq_putmessage('W',
					(const char *)sendXids,
					s->waitedForXidsCount * sizeof (TransactionId));
			pfree(sendXids);
		}
	}

}

/*
 * Remember the XID assigned to the top transaction. Even if multiple datanodes
 * report XIDs, they should always report the same XID given that they are tied
 * by an unique global session identifier
 */ 
void
SetTopTransactionId(GlobalTransactionId xid)
{
	TransactionState s = CurrentTransactionState;
	Assert(!GlobalTransactionIdIsValid(s->transactionId) ||
			GlobalTransactionIdEquals(s->transactionId, xid));

	/*
	 * Also extend the CLOG, SubtransLog and CommitTsLog to ensure that this
	 * XID can later be referenced correctly
	 *
	 * Normally this happens in the GetNextLocalTransactionId() path, but that
	 * may not be ever called when XID is received from the remote node
	 */
	ExtendLogs(xid);

	if (!IsConnFromDatanode())
	{
		XactTopTransactionId = s->transactionId = xid;
		elog(DEBUG2, "Assigning XID received from the remote node - %d", xid);
	}
	else if (!TransactionIdIsValid(GetNextTransactionId()))
	{
		SetNextTransactionId(xid);
		if (whereToSendOutput == DestRemote)
			pq_putmessage('x', (const char *) &xid, sizeof (GlobalTransactionId));
	}
}
#endif

#ifdef __OPENTENBASE__
bool 
InSubTransaction(void)
{
	TransactionState s = CurrentTransactionState;
	if (s->parent != NULL && s->nestingLevel > 1)
	{
		return true;
	}

	return false;	
}

bool 
InPlpgsqlFunc(void)
{
	return g_in_plpgsql_exec_fun > 0;
}

bool
InPlpyFunc(void)
{
	return g_in_plpy_exec_fun > 0;
}

void
SetEnterPlpyFunc(void)
{
	g_in_plpy_exec_fun++;
}

void SetExitPlpyFunc(void)
{
	g_in_plpy_exec_fun--;
}

int
SetEnterPlpgsqlFunc(void)
{
	return g_in_plpgsql_exec_fun++;
}

void SetExitPlpgsqlFunc(int plpgsql_level)
{
	g_in_plpgsql_exec_fun = plpgsql_level;
}


bool SavepointDefined(void)
{
	TransactionState s = CurrentTransactionState;
	if (s->name && TBLOCK_SUBBEGIN == s->blockState)
	{
		return true;
	}

	return false;
}

/* so far, DDL such as (savepoint,rollback to,release savepoint) should not acquire xid */
bool ExecDDLWithoutAcquireXid(Node* parsetree)
{
	TransactionStmt *stmt = (TransactionStmt *) parsetree;
	bool 			 ret  =  false;

	if (parsetree && T_TransactionStmt == nodeTag(parsetree))
	{
		switch (stmt->kind)
		{
			case TRANS_STMT_SAVEPOINT:
			case TRANS_STMT_RELEASE:
			case TRANS_STMT_ROLLBACK_TO:
				ret = true;
				break;
			default:
				break;
		}
	}
	
	return ret;
}

MemoryContext 
GetCurrentTransactionContext(void)
{
	return CurrentTransactionState->curTransactionContext;
}

ResourceOwner
GetCurrentTransactionResourceOwner(void)
{
	return CurrentTransactionState->curTransactionOwner;
}

MemoryContext 
GetValidTransactionContext(void)
{
	TransactionState s = CurrentTransactionState;
	while (s && s->curTransactionContext == NULL)
		s = s->parent;
	if (s && s->curTransactionContext)
		return s->curTransactionContext;
	return ErrorContext;	
}

bool
IsTransactionIdle(void)
{
	TransactionState s = CurrentTransactionState;

	if (TBLOCK_DEFAULT == s->blockState)
	{
        if (TRANS_DEFAULT == s->state)
        {
            return true;
        }
    }
	
    return false;
}

const char * GetPrepareGID(void)
{
    return prepareGID;
}

void ClearPrepareGID(void)
{
    prepareGID = NULL;
    return;
}

#endif

#endif

/*
 * Set transaction isolation level, it is only invoked when handling the 
 * internal transaction mssage from other CN.
 */
void
SetTransIsolationLevel(int xactisolevel)
{
	if (xactisolevel != XACT_READ_COMMITTED && !enable_experiment_isolation)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("transaction mode is restricted to READ COMMITTED.")));
		return;
	}

	if (xactisolevel != XactIsoLevel && IsTransactionState())
	{
		if (FirstSnapshotSet)
		{
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("SET TRANSACTION ISOLATION LEVEL must be called before any query.")));
			return;	
		}
		/* We ignore a subtransaction setting it to the existing value. */
		if (IsSubTransaction())
		{
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("SET TRANSACTION ISOLATION LEVEL must not be called in a subtransaction.")));
			return;	
		}
		/* Can't go to serializable mode while recovery is still active */
		if (xactisolevel == XACT_SERIALIZABLE && RecoveryInProgress())
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot use serializable mode in a hot standby.")));
			return;	
		}
	}
	XactIsoLevel = xactisolevel;
}

void SetCurrentTransactionCanLocalCommit(void)
{
    TransactionState s = CurrentTransactionState;
    s->can_local_commit = true;
    elog(DEBUG1, "Current Transaction can local commit");
}

bool IsCurrentTransactionCanLocalCommit(void)
{
    TransactionState s = CurrentTransactionState;
    return s->can_local_commit;
}

void
CheckTxnStateForSavepointAndRollbackTo()
{
	TransactionState s = CurrentTransactionState;
	while (s->plpgsql_subtrans > 0 && s->nestingLevel >= 2)
	{
		s = s->parent;
	}
	if (s->blockState != TBLOCK_INPROGRESS && s->blockState != TBLOCK_SUBINPROGRESS)
		elog(ERROR, "savepoint/rollback to should be in the transaction block, but state is %s", BlockStateAsString(s->blockState));
}

bool
IsBeginTransactionBlock(void)
{
	return CurrentTransactionState->blockState == TBLOCK_BEGIN;
}


bool
IsPlsqlImplicitSavepoint(void)
{
	TransactionState s = CurrentTransactionState;
	return s->blockState == TBLOCK_SUBINPROGRESS && s->implicit_state == IMPLICIT_PLSQL_SUB;
}

int
GetTransactionSpiLevel(void)
{
	return CurrentTransactionState->spi_level;
}

void
SetTransactionSpiLevel(int spi_level)
{
	CurrentTransactionState->spi_level = spi_level;
}


char *
GetTransactionString(void)
{
	TransactionState s = &TopTransactionStateData;
	StringInfo command;
	char *begin_subtxn_cmd = "BEGIN_SUBTXN %d;";
	char *save_point_cmd = "savepoint %s;";
	char *begin_cmd = "begin;";
	
	command = makeStringInfo();

	appendStringInfoString(command, begin_cmd);
	
	s = s->child;
	while (s)
	{
		if (!s->name)
		{
			appendStringInfo(command, begin_subtxn_cmd, s->nestingLevel);
		}
		else
		{
			appendStringInfo(command, save_point_cmd, s->name);
		}

		s = s->child;
	}

	return command->len == 0 ? NULL : command->data;
}

char *
GetHandleTransactionAndGucString(PGXCNodeHandle *handle, uint64 *guc_cid)
{
	TransactionState s = &TopTransactionStateData;
	StringInfo 		 command;
	char 			*begin_subtxn_cmd = "BEGIN_SUBTXN %d;";
	char 			*save_point_cmd = "savepoint \"%s\";";
	uint64			 target_guc_cid = 0;
	
	command = makeStringInfo();
	
	if (s->max_guc_cid > GetCurrentGucCid(handle))
	{
		get_set_command(s->guc_param_list, command, NULL, GetCurrentGucCid(handle));
		target_guc_cid = s->max_guc_cid;
	}
		

	s = s->child;
	while (s)
	{
		if (s->subTransactionId > GetSubTransactionId(handle) && !InPlpyFunc())
		{
			if (!s->name)
			{
				appendStringInfo(command, begin_subtxn_cmd, s->nestingLevel);
			}
			else if (!savepoint_define_cmd || s->child)
			{
				appendStringInfo(command, save_point_cmd, s->name);
			}
		}

		if (s->max_guc_cid > GetCurrentGucCid(handle))
		{
			get_set_command(s->guc_param_list, command, NULL, GetCurrentGucCid(handle));
			target_guc_cid = s->max_guc_cid;
		}
			
		
		s = s->child;
	}
	
	if (guc_cid)
		*guc_cid = target_guc_cid;
	
	return command->len == 0 ? NULL : command->data;
}


char *
GetAllTransactionAndGucString(bool with_begin)
{
	TransactionState s = &TopTransactionStateData;
	StringInfo command;
	char *begin_subtxn_cmd = "BEGIN_SUBTXN %d;";
	char *save_point_cmd = "savepoint %s;";
	char *begin_cmd = "begin;";

	command = makeStringInfo();

	if (with_begin)
		appendStringInfoString(command, begin_cmd);
	
	get_set_command(s->guc_param_list, command, NULL, 0);

	s = s->child;
	while (s)
	{
		if (!s->name)
		{
			appendStringInfo(command, begin_subtxn_cmd, s->nestingLevel);
		}
		else
		{
			appendStringInfo(command, save_point_cmd, s->name);
		}
		
		get_set_command(s->guc_param_list, command, NULL, 0);
		s = s->child;
	}
	
	return command->len == 0 ? NULL : command->data;
}

void 
TransactionSetGuc(bool local, const char *name, const char *value, int flags, uint64 guc_cid)
{
	MemoryContext		oldcontext;
	TransactionState	s = CurrentTransactionState;
	List 			   *param_list = s->guc_param_list;
	ParamEntry 		   *entry;

	/* do not add 'max_handles_per_node' to transaction guc list */
	if (strcmp(name, "max_handles_per_node") == 0)
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	param_list = paramlist_delete_param(param_list, name, local);

	/*
	 * Special case for
	 * 	RESET SESSION AUTHORIZATION
	 * 	SET SESSION AUTHORIZATION TO DEFAULT
	 *
	 * We must also forget any SET ROLE commands since RESET SESSION
	 * AUTHORIZATION also resets current role to session default
	 */
	if ((strcmp(name, "session_authorization") == 0) && (value == NULL))
		param_list = paramlist_delete_param(param_list, "role", false);
	
	entry = (ParamEntry *) palloc0(sizeof (ParamEntry));
	strlcpy((char *) (&entry->name), name, NAMEDATALEN);
	entry->flags = flags;
	entry->local = local;
	entry->guc_cid = guc_cid;
	if (value)
	{
		if (strlen(value) > PARAMDATALEN)
			elog(ERROR, "The size of guc %s value is larger than %d", name, PARAMDATALEN);

		strlcpy((char *) (&entry->value), value, PARAMDATALEN);
		entry->reset = false;
	}
	else
	{
		NameStr(entry->value)[0] = '\0';
		entry->reset = true;
	}
	param_list = lappend(param_list, entry);
	
	s->guc_param_list = param_list;
	
	if (guc_cid > s->max_guc_cid)
		s->max_guc_cid = guc_cid;
	
	MemoryContextSwitchTo(oldcontext);
}

void
TransactionResetAllGuc(uint64 guc_cid)
{
	TransactionState	s = CurrentTransactionState;
	List 			   *param_list = s->guc_param_list;
	List               *temp_list = NIL;
	ParamEntry 		   *entry;
	ParamEntry 		   *new_entry;
	ListCell		   *lc;

	/* consider gucs changed in this session */
	temp_list = list_copy(session_param_list);

	foreach(lc, temp_list)
	{
		const char *val;
		entry = (ParamEntry *)lfirst(lc);
		if (strcmp(NameStr(entry->name), "global_session") == 0)
			continue;

		val = GetConfigOptionResetString(NameStr(entry->name));

		/* same as the reset value, no need to reset */
		if (val && (strncmp(val, NameStr(entry->value), PARAMDATALEN) == 0))
			continue;

		TransactionSetGuc(entry->local, NameStr(entry->name), NULL, entry->flags, guc_cid);
	}

	while (s)
	{
		temp_list = NIL;

		foreach(lc, param_list)
		{
			entry = (ParamEntry *) lfirst(lc);
			new_entry = palloc0(sizeof(ParamEntry));

			*new_entry = *entry;
			temp_list = lappend(temp_list, new_entry);
		}

		foreach(lc, temp_list)
		{
			entry = (ParamEntry *) lfirst(lc);
			if (strcmp(NameStr(entry->name), "global_session") == 0)
				continue;
			TransactionSetGuc(entry->local, NameStr(entry->name), NULL, entry->flags, guc_cid);
		}

		list_free_deep(temp_list);

		s = s->parent;
	}
}

void
TransactionDelGuc(bool local, const char *name)
{
	TransactionState s = CurrentTransactionState;
	List *param_list = s->guc_param_list;
	
	param_list = paramlist_delete_param(param_list, name, local);

	s->guc_param_list = param_list;
}

List *
GetTopTransactionGucList(void)
{
	return TopTransactionStateData.guc_param_list;
}

List *
GetCurrentTransactionGucList(void)
{
	return CurrentTransactionState->guc_param_list;
}

void 
ResetTopTransactionGucList(void)
{
	list_free_deep(TopTransactionStateData.guc_param_list);
	TopTransactionStateData.guc_param_list = NULL;
	TopTransactionStateData.max_guc_cid = 0;
	return;
}

void
ResetCurrentTransactionGucList(void)
{
	list_free_deep(CurrentTransactionState->guc_param_list);
	CurrentTransactionState->guc_param_list = NULL;
	CurrentTransactionState->max_guc_cid = 0;
	return;
}

void
SubTransactionTransferGucListToParent(void)
{
	TransactionState s = CurrentTransactionState;
	
	s->parent->guc_param_list = list_concat(s->parent->guc_param_list, s->guc_param_list);
	s->guc_param_list = NULL;
	
	if (s->max_guc_cid > s->parent->max_guc_cid)
		s->parent->max_guc_cid = s->max_guc_cid;
	return;
}

void
TopTxnTransferNoneLocalGucList(void)
{
	TransactionState  s = &TopTransactionStateData;
	ListCell   		 *lc;
	
	foreach(lc, s->guc_param_list)
	{
		
		ParamEntry *entry = (ParamEntry *) lfirst(lc);

		if(enable_distri_print)
			elog(LOG, "transfer %s %s local %d", NameStr(entry->name), NameStr(entry->value), entry->local);
		if (!entry->local)
		{
			char *val;
			if (entry->reset)
				val = NULL;
			else
				val = NameStr(entry->value);
			session_param_list_set(entry->local, NameStr(entry->name), val, entry->flags, entry->guc_cid);
		}
	}
	return;
}

void 
SetEnterPlpgsqlAnalysis(void)
{
	in_plpgsql_analysis = true;
}

void 
SetExitPlpgsqlAnalysis(void)
{
	in_plpgsql_analysis = false;
}

bool 
IsInPlpgsqlAnalysis(void)
{
	return in_plpgsql_analysis;
}

void
bump_sub_transaction_id_current_txn_handles(void)
{
	int i = 0;
	
	for (i = 0; i < current_transaction_handles->dn_conn_count; i++)
	{
		current_transaction_handles->datanode_handles[i]->state.subtrans_id = currentSubTransactionId;
	}

	for (i = 0; i < current_transaction_handles->co_conn_count; i++)
	{
		current_transaction_handles->coord_handles[i]->state.subtrans_id = currentSubTransactionId;
	}
	
	return;
}

MemoryContext
SwitchToTransactionAbortContext(void)
{
	MemoryContext old_context = MemoryContextSwitchTo(TransactionAbortContext);
	return old_context;
}
