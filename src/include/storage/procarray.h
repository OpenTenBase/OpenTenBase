/*-------------------------------------------------------------------------
 *
 * procarray.h
 *	  POSTGRES process array definitions.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/storage/procarray.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCARRAY_H
#define PROCARRAY_H

#include "storage/lock.h"
#include "storage/standby.h"
#include "storage/proc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

#ifdef XCP
extern int GlobalSnapshotSource;

typedef enum GlobalSnapshotSourceType
{
	GLOBAL_SNAPSHOT_SOURCE_GTM,
	GLOBAL_SNAPSHOT_SOURCE_COORDINATOR
} GlobalSnapshotSourceType;
#endif

typedef struct FirstBackendInfo
{
	int		backendId;
	int		procPid;
	uint64	startTime;
} FirstBackendInfo;

/*
 * These are to implement PROCARRAY_FLAGS_XXX
 *
 * Note: These flags are cloned from PROC_XXX flags in src/include/storage/proc.h
 * to avoid forcing to include proc.h when including procarray.h. So if you modify
 * PROC_XXX flags, you need to modify these flags.
 */
#define		PROCARRAY_VACUUM_FLAG			0x02	/* currently running lazy
													 * vacuum */
#define		PROCARRAY_ANALYZE_FLAG			0x04	/* currently running
													 * analyze */
#define		PROCARRAY_LOGICAL_DECODING_FLAG 0x10	/* currently doing logical
													 * decoding outside xact */

#define		PROCARRAY_SLOTS_XMIN			0x20	/* replication slot xmin,
													 * catalog_xmin */
/*
 * Only flags in PROCARRAY_PROC_FLAGS_MASK are considered when matching
 * PGXACT->vacuumFlags. Other flags are used for different purposes and
 * have no corresponding PROC flag equivalent.
 */
#define		PROCARRAY_PROC_FLAGS_MASK	(PROCARRAY_VACUUM_FLAG | \
										 PROCARRAY_ANALYZE_FLAG | \
										 PROCARRAY_LOGICAL_DECODING_FLAG)

/* Use the following flags as an input "flags" to GetOldestXmin function */
/* Consider all backends except for logical decoding ones which manage xmin separately */
#define		PROCARRAY_FLAGS_DEFAULT			PROCARRAY_LOGICAL_DECODING_FLAG
/* Ignore vacuum backends */
#define		PROCARRAY_FLAGS_VACUUM			PROCARRAY_FLAGS_DEFAULT | PROCARRAY_VACUUM_FLAG
/* Ignore analyze backends */
#define		PROCARRAY_FLAGS_ANALYZE			PROCARRAY_FLAGS_DEFAULT | PROCARRAY_ANALYZE_FLAG
/* Ignore both vacuum and analyze backends */
#define		PROCARRAY_FLAGS_VACUUM_ANALYZE	PROCARRAY_FLAGS_DEFAULT | PROCARRAY_VACUUM_FLAG | PROCARRAY_ANALYZE_FLAG

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC *proc, bool hold_lock, bool is_atxact);
extern void ProcArrayRemove(PGPROC *proc, TransactionId latestXid);

extern void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid);
extern void ProcArrayClearTransaction(PGPROC *proc);

#ifdef PGXC  /* PGXC_DATANODE */
typedef enum
{
	SNAPSHOT_UNDEFINED,   /* Coordinator has not sent snapshot or not yet connected */
	SNAPSHOT_LOCAL,       /* Coordinator has instructed Datanode to build up snapshot from the local procarray */
	SNAPSHOT_COORDINATOR, /* Coordinator has sent snapshot data */
	SNAPSHOT_DIRECT       /* Datanode obtained directly from GTM */
} SnapshotSource;

extern void SetGlobalTimestamp(GlobalTimestamp gts, SnapshotSource source);
extern GlobalTimestamp GetGlobalTimestamp(void);
#if 0
extern void SetGlobalSnapshotData(TransactionId xmin, TransactionId xmax, int xcnt,
		TransactionId *xip,
		SnapshotSource source);
#endif
extern void UnsetGlobalSnapshotData(void);
extern void ReloadConnInfoOnBackends(bool refresh_only);
#endif /* PGXC */
extern void ProcArrayInitRecovery(TransactionId initializedUptoXID);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);
extern void ProcArrayApplyXidAssignment(TransactionId topxid,
							int nsubxids, TransactionId *subxids);

extern void RecordKnownAssignedTransactionIds(TransactionId xid);
extern void ExpireTreeKnownAssignedTransactionIds(TransactionId xid,
									  int nsubxids, TransactionId *subxids,
									  TransactionId max_xid);
extern void ExpireAllKnownAssignedTransactionIds(void);
extern void ExpireOldKnownAssignedTransactionIds(TransactionId xid);

extern int	GetMaxSnapshotXidCount(void);
extern int	GetMaxSnapshotSubxidCount(void);

#define GetSnapshotData(snapshot, latest) GetSnapshotData_shard(snapshot, latest, true)
extern Snapshot GetSnapshotData_shard(Snapshot snapshot, bool latest, bool need_shardmap);
extern TransactionId RecentGlobalXmin;
extern TransactionId GetRecentGlobalDataXmin(void);
extern TransactionId GetRecentXmin(void);

extern bool ProcArrayInstallImportedXmin(TransactionId xmin,
							 VirtualTransactionId *sourcevxid);
extern bool ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc);
extern void ProcArrayCheckXminConsistency(TransactionId global_xmin);
extern void SetLatestCompletedXid(TransactionId latestCompletedXid);

extern RunningTransactions GetRunningTransactionData(void);

extern bool TransactionIdIsInProgress(TransactionId xid);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern bool TransactionIdIsPrepared(TransactionId xid, Snapshot snapshot, GlobalTimestamp *prepare_ts);
#endif
#ifdef __OPENTENBASE__
extern TransactionId GetLocalTransactionId(const char *globalXid, TransactionId *subxids, int *nsub, bool *overflowed);
#endif
extern char *GetGlobalTransactionId(const int pid);
extern bool TransactionIdIsActive(TransactionId xid);
extern bool TransactionIdIsActiveUntilFinishPrepare(TransactionId xid);
extern TransactionId GetOldestXmin(Relation rel, int flags);
extern TransactionId GetOldestXminCurrentDB(int flags);
extern TransactionId GetOldestXminInternal(Relation rel, int flags,
		bool computeLocal, TransactionId lastGlobalXmin);
extern GlobalTimestamp GetOldestTmin(Relation rel, int flags);
extern GlobalTimestamp GetOldestTminOfOthers(Relation rel, int flags);
extern GlobalTimestamp GetOldestTminOfOthersCurrentDB(int flags);

extern TransactionId GetOldestActiveTransactionId(void);
extern TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly);

extern VirtualTransactionId *GetVirtualXIDsDelayingChkpt(int *nvxids, int type);
extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids,
										 int nvxids, int type);

extern PGPROC *BackendPidGetProc(int pid);
extern PGPROC *BackendPidGetProcWithLock(int pid);
extern int	BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(int pid);

extern VirtualTransactionId *GetCurrentVirtualXIDs(TransactionId limitXmin,
					  bool excludeXmin0, bool allDbs, int excludeVacuum,
					  int *nvxids);
extern VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
extern pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);

extern bool MinimumActiveBackends(int min);
extern int	CountDBBackends(Oid databaseid);
extern int	CountDBConnections(Oid databaseid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern int	CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId,
								 int *nbackends, int *nprepared);
extern void TerminateOtherDBBackends(Oid databaseId);

extern void XidCacheRemoveRunningXids(TransactionId xid,
						  int nxids, const TransactionId *xids,
						  TransactionId latestXid);
#ifdef XCP
extern void GetGlobalSessionInfo(int pid, Oid *coordId, int *coordPid);
extern void	GetFirstBackendId(int *numBackends, int *backends, FirstBackendInfo* backendInfo);
#endif /* XCP */
extern bool HasSessionEnded(int coordid, int coordpid, uint64 timestamp);
extern HTAB *BuildSessionCheckHash(void);

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin,
								TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
								TransactionId *catalog_xmin);
#ifdef __OPENTENBASE__
extern RunningTransactions GetCurrentRunningTransaction(void);
extern void AdvanceOldestActiveXid(TransactionId myXid);
extern TransactionId GetRecentGlobalXmin(void);
extern CommitSeqNo GetRecentRecentDataTs(void);
extern TransactionId XidGenGroupAcquire(PGPROC *proc, bool isSubXact);
extern TransactionId GetNewTransactionIdInternal(bool isSubXact, bool wait);
extern void NewTransactionIdAssign(TransactionId xid, volatile PGPROC *proc,
								   volatile PGXACT *xact, bool isSubXact);
extern bool TransactionIdCheckLimit(TransactionId xid);
#endif

extern void autokill_old_transaction(void);
extern void WaitAllReadOnlyProcessFinish(void);
#endif							/* PROCARRAY_H */
