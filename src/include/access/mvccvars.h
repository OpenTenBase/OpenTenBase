/*-------------------------------------------------------------------------
 *
 * mvccvars.h
 *	  Shared memory variables for XID assignment and snapshots
 *
 *
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/mvccvars.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MVCCVARS_H
#define MVCCVARS_H

#include "port/atomics.h"
#include "storage/spin.h"

/*
 * VariableCache is a data structure in shared memory that is used to track
 * OID and XID assignment state.  For largely historical reasons, there is
 * just one struct with different fields that are protected by different
 * LWLocks.
 *
 * Note: xidWrapLimit and oldestXidDB are not "active" values, but are
 * used just to generate useful messages when xidWarnLimit or xidStopLimit
 * are exceeded.
 */
typedef struct VariableCacheData
{
	/*
	 * These fields are protected by OidGenLock.
	 */
	Oid			nextOid;		/* next OID to assign */
	uint32		oidCount;		/* OIDs available before must do XLOG work */

	/*
	 * These fields are protected by XidGenLock.
	 */
	TransactionId nextXid;		/* next XID to assign */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	TransactionId xidVacLimit;	/* start forcing autovacuums here */
	TransactionId xidWarnLimit; /* start complaining here */
	TransactionId xidStopLimit; /* refuse to advance nextXid beyond here */
	TransactionId xidWrapLimit; /* where the world ends */
	TransactionId xidPhaseOneStopLimit;
	TransactionId xidPhaseTwoStopLimit;
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */


	/*
	 * Fields related to MVCC snapshots.
	 *
	 * lastCommitSeqNo is the CSN assigned to last committed transaction. It
	 * is protected by CommitSeqNoLock.
	 *
	 * latestCompletedXid is the highest XID that has committed. Anything >
	 * this is seen by still in-progress by everyone. Use atomic ops to
	 * update.
	 *
	 * oldestActiveXid is the XID of the oldest transaction that's still
	 * in-progress. (Or rather, the oldest XID among all still in-progress
	 * transactions; it's not necessarily the one that started first). Must
	 * hold ProcArrayLock in shared mode, and use atomic ops, to update.
	 */
	
    /*
	 * These fields are protected by CommitTsLock
	 */
	TransactionId oldestCommitTsXid;
	TransactionId newestCommitTsXid;

	/*
	 * These fields are protected by ProcArrayLock.
	 */
    pg_atomic_uint32 latestCompletedXid;	/* newest XID that has committed or
										 * aborted */
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
    pg_atomic_uint64 latestGTS;			/* latest gts of current node. */
    pg_atomic_uint64 maxCommitTs;	/* max commit ts */
	slock_t		ts_lock;		/* protect lock */
#endif
    pg_atomic_uint32 oldestActiveXid;

	/*
	 * These fields are protected by CLogTruncationLock
	 */
	TransactionId oldestClogXid;	/* oldest it's safe to look up in clog */

	/*
	 * the locally cached gts and the time it was obtained.
	 * localCacheGts.u64[0] is gts and localCacheGts.u64[1]
	 * is the time it was obtained
	 */
	uint128_u localCacheGts;
} VariableCacheData;

typedef VariableCacheData *VariableCache;

/* in transam/varsup.c */
extern PGDLLIMPORT VariableCache ShmemVariableCache;


extern GlobalTimestamp GetSharedLatestCommitTS(void);
extern void SetLatestCommitTSInternal(GlobalTimestamp gts, const char *file, const char *func, int line);
#define SetSharedLatestCommitTS(a) SetLatestCommitTSInternal(a, __FILE__, __FUNCTION__, __LINE__)

GlobalTimestamp AdvanceSharedMaxCommitTs(void);
extern GlobalTimestamp GetSharedMaxCommitTs(void);
extern void SetMaxCommitTsInternal(GlobalTimestamp gts, const char *file, const char *func, int line);
#define SetSharedMaxCommitTs(a) SetMaxCommitTsInternal(a, __FILE__, __FUNCTION__, __LINE__)

extern void SetAllCommitTsInternal(GlobalTimestamp gts, const char *file, const char *func, int line);
#define SetAllSharedCommitTs(a) SetAllCommitTsInternal(a, __FILE__, __FUNCTION__, __LINE__)

extern TransactionId GetLatestCompletedXid(void);

#endif							/* MVCCVARS_H */
