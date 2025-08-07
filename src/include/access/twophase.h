/*-------------------------------------------------------------------------
 *
 * twophase.h
 *	  Two-phase-commit related declarations.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_H
#define TWOPHASE_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "storage/lock.h"
#include "storage/relfilenode.h"

#include "gtm/gtm_c.h"

#define GIDSIZE (200 + 24)

/*
 * GlobalTransactionData is defined in twophase.c; other places have no
 * business knowing the internal definition.
 */
typedef struct GlobalTransactionData *GlobalTransaction;

/* GUC variable */
extern PGDLLIMPORT int max_prepared_xacts;

#ifdef __OPENTENBASE__
extern bool enable_2pc_recovery_info;
#endif

#ifdef __TWO_PHASE_TRANS__
extern bool enable_2pc_file_cache;
extern bool enable_2pc_file_check;
extern bool enable_2pc_entry_key_check;
extern bool enable_2pc_entry_trace;

extern int record_2pc_cache_size;
extern int record_2pc_entry_size;
#endif

extern Size TwoPhaseShmemSize(void);
extern void TwoPhaseShmemInit(void);

extern void AtAbort_Twophase(void);
extern void PostPrepare_Twophase(void);

extern bool CheckXidInTwoPhase(TransactionId xid);
extern PGPROC *TwoPhaseGetDummyProc(TransactionId xid);
extern BackendId TwoPhaseGetDummyBackendId(TransactionId xid);

extern GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
				TimestampTz prepared_at,
				Oid owner, Oid databaseid);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern void EndGlobalPrepare(GlobalTransaction gxact, bool isImplicit);
extern void EndExplicitGlobalPrepare(char *gid);
extern GlobalTransaction LockGXact(const char *gid, Oid user, bool is_check);

#endif
extern void EndPrepare(GlobalTransaction gxact);
extern void StartPrepare(GlobalTransaction gxact);
extern bool StandbyTransactionIdIsPrepared(TransactionId xid);

extern TransactionId PrescanPreparedTransactions(TransactionId **xids_p,
							int *nxids_p);
extern void StandbyRecoverPreparedTransactions(void);
extern void RecoverPreparedTransactions(void);

extern void CheckPointTwoPhase(XLogRecPtr redo_horizon);

extern void FinishPreparedTransaction(const char *gid, bool isCommit);

extern void CheckPreparedTransactionLock(const char *gid);

extern void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn,
			   XLogRecPtr end_lsn);
extern void PrepareRedoRemove(TransactionId xid, bool giveWarning);
extern void restoreTwoPhaseData(void);

#ifdef __TWO_PHASE_TRANS__
extern void record_2pc_redo_remove_gid_xid(TransactionId xid);
extern void record_2pc_involved_nodes_xid(const char * tid, 
                                        char * startnode, 
                                        GlobalTransactionId startxid, 
                                        char * nodestring, 
                                        GlobalTransactionId xid,
                                        const char *traceid,
                                        const char *database,
                                        const char *user);
extern void record_2pc_commit_timestamp(const char *tid, GlobalTimestamp commit_timestamp);
extern void remove_2pc_records(const char *tid, bool record_in_xlog);
extern void rename_2pc_records(const char *tid, TimestampTz timestamp);
extern void record_2pc_readonly(const char *gid);

extern char *get_2pc_info_from_cache(const char *tid);
extern char *get_2pc_list_from_cache(int *count);

extern void Record2pcCacheInit(void);
extern Size Record2pcCacheSize(void);
#endif

#endif							/* TWOPHASE_H */
