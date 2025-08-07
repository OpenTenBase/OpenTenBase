/*-------------------------------------------------------------------------
 *
 * xact.h
 *	  postgres transaction system definitions
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/access/xact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XACT_H
#define XACT_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/relfilenode.h"
#ifdef PGXC  /* PGXC_COORD */
#include "gtm/gtm_c.h"
#endif
#include "storage/sinval.h"
#ifdef __OPENTENBASE__
#include "utils/resowner.h"
#endif
#include "pgxc/pgxcnode.h"

#define IMPLICIT_SAVEPOINT_NAME "__$IMPLICIT_SAVEPOINT_NAME$__"

/*
 * Xact isolation levels
 */
#define XACT_READ_UNCOMMITTED	0
#define XACT_READ_COMMITTED		1
#define XACT_REPEATABLE_READ	2
#define XACT_SERIALIZABLE		3
#define XACT_ISOLATION_INVALID  4

extern int	DefaultXactIsoLevel;
extern PGDLLIMPORT int XactIsoLevel;

/*
 * We implement three isolation levels internally.
 * The two stronger ones use one snapshot per database transaction;
 * the others use one snapshot per statement.
 * Serializable uses predicate locks in addition to snapshots.
 * These macros should be used to check which isolation level is selected.
 */
#define IsolationUsesXactSnapshot() (XactIsoLevel >= XACT_REPEATABLE_READ)
#define IsolationIsSerializable() (XactIsoLevel == XACT_SERIALIZABLE)

/* Xact read-only state */
extern bool DefaultXactReadOnly;
extern bool XactReadOnly;
extern bool ReadWithLocalTs;

#ifdef __OPENTENBASE__
extern bool GTM_ReadOnly;
#endif

#ifdef __TWO_PHASE_TRANS__
extern bool enable_2pc_error_stop;
#endif

/*
 * Xact is deferrable -- only meaningful (currently) for read only
 * SERIALIZABLE transactions
 */
extern bool DefaultXactDeferrable;
extern bool XactDeferrable;

typedef enum
{
	SYNCHRONOUS_COMMIT_OFF,		/* asynchronous commit */
	SYNCHRONOUS_COMMIT_LOCAL_FLUSH, /* wait for local flush only */
	SYNCHRONOUS_COMMIT_REMOTE_WRITE,	/* wait for local flush and remote
										 * write */
	SYNCHRONOUS_COMMIT_REMOTE_FLUSH,	/* wait for local and remote flush */
	SYNCHRONOUS_COMMIT_REMOTE_APPLY /* wait for local flush and remote apply */
}			SyncCommitLevel;

/* Define the default setting for synchronous_commit */
#define SYNCHRONOUS_COMMIT_ON	SYNCHRONOUS_COMMIT_REMOTE_FLUSH

/* Synchronous commit level */
extern int	synchronous_commit;

/*
 * Miscellaneous flag bits to record events which occur on the top level
 * transaction. These flags are only persisted in MyXactFlags and are intended
 * so we remember to do certain things later in the transaction. This is
 * globally accessible, so can be set from anywhere in the code which requires
 * recording flags.
 */
extern int	MyXactFlags;

/*
 * XACT_FLAGS_ACCESSEDTEMPREL - set when a temporary relation is accessed. We
 * don't allow PREPARE TRANSACTION in that case.
 */
#define XACT_FLAGS_ACCESSEDTEMPREL				(1U << 0)

/*
 * XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK - records whether the top level xact
 * logged any Access Exclusive Locks.
 */
#define XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK	(1U << 1)


/*
 *	start- and end-of-transaction callbacks for dynamically loaded modules
 */
typedef enum
{
	XACT_EVENT_COMMIT,
	XACT_EVENT_PARALLEL_COMMIT,
	XACT_EVENT_ABORT,
	XACT_EVENT_PARALLEL_ABORT,
	XACT_EVENT_PREPARE,
	XACT_EVENT_PRE_COMMIT,
	XACT_EVENT_PARALLEL_PRE_COMMIT,
	XACT_EVENT_PRE_PREPARE
} XactEvent;

typedef void (*XactCallback) (XactEvent event, void *arg);

typedef enum
{
	SUBXACT_EVENT_START_SUB,
	SUBXACT_EVENT_COMMIT_SUB,
	SUBXACT_EVENT_ABORT_SUB,
	SUBXACT_EVENT_PRE_COMMIT_SUB
} SubXactEvent;

typedef void (*SubXactCallback) (SubXactEvent event, SubTransactionId mySubid,
								 SubTransactionId parentSubid, void *arg);

#ifdef PGXC
/*
 * GTM callback events
 */
typedef enum
{
	GTM_EVENT_COMMIT,
	GTM_EVENT_ABORT,
	GTM_EVENT_PREPARE
} GTMEvent;

typedef void (*GTMCallback) (GTMEvent event, void *arg);
#endif

/* ----------------
 *		transaction-related XLOG entries
 * ----------------
 */

/*
 * XLOG allows to store some information in high 4 bits of log record xl_info
 * field. We use 3 for the opcode, and one about an optional flag variable.
 */
#define XLOG_XACT_COMMIT			0x00
#define XLOG_XACT_PREPARE			0x10
#define XLOG_XACT_ABORT				0x20
#define XLOG_XACT_COMMIT_PREPARED	0x30
#define XLOG_XACT_ABORT_PREPARED	0x40
#define XLOG_XACT_ASSIGNMENT		0x50
#ifdef __OPENTENBASE__
/* free opcode 0x60 */
#define XLOG_XACT_ACQUIRE_GTS		0x60
#endif

/* free opcode 0x70 */

/* mask for filtering opcodes out of xl_info */
#define XLOG_XACT_OPMASK			0x70

/* does this record have a 'xinfo' field or not */
#define XLOG_XACT_HAS_INFO			0x80

/* record 2plc file for readonly explicit transaction */
#define XLOG_XACT_RECORD_READONLY 0x90
/*
 * The following flags, stored in xinfo, determine which information is
 * contained in commit/abort records.
 */
#define XACT_XINFO_HAS_DBINFO              (1U << 0)
#define XACT_XINFO_HAS_SUBXACTS            (1U << 1)
#define XACT_XINFO_HAS_RELFILENODES        (1U << 2)
#define XACT_XINFO_HAS_INVALS              (1U << 3)
#define XACT_XINFO_HAS_TWOPHASE            (1U << 4)
#define XACT_XINFO_HAS_ORIGIN              (1U << 5)
#define XACT_XINFO_HAS_AE_LOCKS            (1U << 6)
#define XACT_XINFO_HAS_OLDEST_ACTIVE_XID   (1U << 7)

/*
 * Also stored in xinfo, these indicating a variety of additional actions that
 * need to occur when emulating transaction effects during recovery.
 *
 * They are named XactCompletion... to differentiate them from
 * EOXact... routines which run at the end of the original transaction
 * completion.
 */
#define XACT_COMPLETION_APPLY_FEEDBACK			(1U << 29)
#define XACT_COMPLETION_UPDATE_RELCACHE_FILE	(1U << 30)
#define XACT_COMPLETION_FORCE_SYNC_COMMIT		(1U << 31)

/* Access macros for above flags */
#define XactCompletionApplyFeedback(xinfo) \
	((xinfo & XACT_COMPLETION_APPLY_FEEDBACK) != 0)
#define XactCompletionRelcacheInitFileInval(xinfo) \
	((xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE) != 0)
#define XactCompletionForceSyncCommit(xinfo) \
	((xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT) != 0)

typedef struct xl_xact_assignment
{
	TransactionId xtop;			/* assigned XID's top-level XID */
	int			nsubxacts;		/* number of subtransaction XIDs */
	TransactionId xsub[FLEXIBLE_ARRAY_MEMBER];	/* assigned subxids */
} xl_xact_assignment;

#define MinSizeOfXactAssignment offsetof(xl_xact_assignment, xsub)

/*
 * Commit and abort records can contain a lot of information. But a large
 * portion of the records won't need all possible pieces of information. So we
 * only include what's needed.
 *
 * A minimal commit/abort record only consists of a xl_xact_commit/abort
 * struct. The presence of additional information is indicated by bits set in
 * 'xl_xact_xinfo->xinfo'. The presence of the xinfo field itself is signalled
 * by a set XLOG_XACT_HAS_INFO bit in the xl_info field.
 *
 * NB: All the individual data chunks should be sized to multiples of
 * sizeof(int) and only require int32 alignment. If they require bigger
 * alignment, they need to be copied upon reading.
 */

/* sub-records for commit/abort */

typedef struct xl_xact_xinfo
{
	/*
	 * Even though we right now only require 1 byte of space in xinfo we use
	 * four so following records don't have to care about alignment. Commit
	 * records can be large, so copying large portions isn't attractive.
	 */
	uint32		xinfo;
} xl_xact_xinfo;

typedef struct xl_xact_dbinfo
{
	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */
} xl_xact_dbinfo;

typedef struct xl_xact_subxacts
{
	int			nsubxacts;		/* number of subtransaction XIDs */
	TransactionId subxacts[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_subxacts;
#define MinSizeOfXactSubxacts offsetof(xl_xact_subxacts, subxacts)

typedef struct xl_xact_relfilenodes
{
	int			nrels;			/* number of subtransaction XIDs */
	RelFileNodeNFork xnodes[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_relfilenodes;
#define MinSizeOfXactRelfilenodes offsetof(xl_xact_relfilenodes, xnodes)

typedef struct xl_xact_invals
{
	int			nmsgs;			/* number of shared inval msgs */
	SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_invals;
#define MinSizeOfXactInvals offsetof(xl_xact_invals, msgs)

typedef struct xl_xact_twophase
{
	TransactionId xid;
} xl_xact_twophase;

typedef struct xl_xact_origin
{
	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_origin;

typedef struct xl_xact_commit
{
	CommitSeqNo gts;			/* commit seq number */
	TimestampTz xact_time;		/* time of commit */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* xl_xact_dbinfo follows if XINFO_HAS_DBINFO */
	/* xl_xact_subxacts follows if XINFO_HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if XINFO_HAS_RELFILENODES */
	/* xl_xact_invals follows if XINFO_HAS_INVALS */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
	/* xl_xact_origin follows if XINFO_HAS_ORIGIN, stored unaligned! */
} xl_xact_commit;
#define MinSizeOfXactCommit (offsetof(xl_xact_commit, xact_time) + sizeof(TimestampTz))

typedef struct xl_xact_abort
{
    CommitSeqNo gts;   /* logical global timestamp */
	TimestampTz xact_time;		/* time of abort */

	/* xl_xact_xinfo follows if XLOG_XACT_HAS_INFO */
	/* No db_info required */
	/* xl_xact_subxacts follows if HAS_SUBXACT */
	/* xl_xact_relfilenodes follows if HAS_RELFILENODES */
	/* No invalidation messages needed. */
	/* xl_xact_twophase follows if XINFO_HAS_TWOPHASE */
} xl_xact_abort;
#define MinSizeOfXactAbort sizeof(xl_xact_abort)

/*
 * Commit/Abort records in the above form are a bit verbose to parse, so
 * there's a deconstructed versions generated by ParseCommit/AbortRecord() for
 * easier consumption.
 */
typedef struct xl_xact_parsed_commit
{
	CommitSeqNo gts;
	TimestampTz xact_time;

	uint32		xinfo;

	Oid			dbId;			/* MyDatabaseId */
	Oid			tsId;			/* MyDatabaseTableSpace */

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNodeNFork *xnodes;

	int			nmsgs;
	SharedInvalidationMessage *msgs;

	TransactionId twophase_xid; /* only for 2PC */
    TransactionId oldest_active_xid;
	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_parsed_commit;

typedef struct xl_xact_parsed_abort
{
    CommitSeqNo gts;   /* logical global timestamp */
	TimestampTz xact_time;
	uint32		xinfo;

	int			nsubxacts;
	TransactionId *subxacts;

	int			nrels;
	RelFileNodeNFork *xnodes;

	TransactionId twophase_xid; /* only for 2PC */
	TransactionId oldest_active_xid;
} xl_xact_parsed_abort;

#ifdef __OPENTENBASE__
typedef struct xl_xact_acquire_gts
{
	TimestampTz global_timestamp;   /* logical global timestamp */
}xl_xact_acquire_gts;
#endif

#ifdef __TWO_PHASE_TESTS__
extern int twophase_exception_case;
extern int run_pg_clean;
typedef enum
{
    /* twophase exception case */
    PART_PREPARE_GET_TIMESTAMP = 1,     
    PART_PREPARE_SEND_TIMESTAMP = 2,    
    PART_PREPARE_SEND_STARTER = 3,      
    PART_PREPARE_SEND_STARTXID= 4,      
    PART_PREPARE_SEND_PARTNODES = 5,
    PART_PREPARE_SEND_QUERY = 6,        
    PART_PREPARE_RESPONSE_ERROR = 7,    
    PART_PREPARE_ENDPREPARE = 8,
    PREPARE_ERROR_SEND_QUERY = 9,       
    PREPARE_ERROR_RESPONSE_ERROR = 10,
    PART_ABORT_SEND_ROLLBACK = 11,      
    PART_ABORT_REMOTE_FINISH = 12,
    
    ALL_PREPARE_REMOTE_FINISH = 13,
    PART_PREPARE_PREPARE_GTM = 14,    /* explicit-StartPreparedTranGTM fail*/
    ALL_PREPARE_REMOTE_PREFINISH = 15, /* explicit-GetGlobalTimestampGTM */
    ALL_PREPARE_FINISH_REMOTE_PREPARED = 16, /* explicit-GetGIDDataGTM */
    PART_COMMIT_SEND_TIMESTAMP = 17,
    PART_COMMIT_SEND_QUERY = 18,        
    PART_COMMIT_RESPONSE_ERROR = 19,    
    PART_COMMIT_FINISH_PREPARED = 20,
    /* 2pc file error */
    FILE_KERNEL_EXSISTED = 21,          
    FILE_PGCLEAN_EXISTED_CONSISTENT = 22,
    FILE_PGCLEAN_EXISTED_NCONSISTENT = 23,
    FILE_XLOG_EXISTED = 24,
    /* pg_clean error */
    PG_CLEAN_SEND_CLEAN = 25,
    PG_CLEAN_SEND_READONLY = 26,
    PG_CLEAN_SEND_AFTER_PREPARE = 27,
    PG_CLEAN_SEND_TIMESTAMP = 28,
    PG_CLEAN_SEND_STARTER = 29,
    PG_CLEAN_SEND_STARTXID = 30,
    PG_CLEAN_SEND_PARTNODES = 31,
    PG_CLEAN_SEND_QUERY = 32,
    PG_CLEAN_ELOG_ERROR = 33,
    /* other error */
    PART_PREPARE_AFTER_RECORD_2PC = 34 /* check whether nextXid update during xlog_redo in CREATE_2PC_FILE */
}TwophaseTestCase;
    
typedef enum
{
    IN_OTHER = 0,
    IN_REMOTE_PREPARE = 1,
    IN_PREPARE_ERROR,
    IN_FINISH_REMOTE_PREPARED,
    IN_REMOTE_PREFINISH,
    IN_REMOTE_ABORT,
    IN_REMOTE_FINISH,
    IN_PG_CLEAN
}TwophaseTransAt;
    
typedef enum
{
    SEND_OTHER = 0,
    SEND_PREPARE_TIMESTAMP = 1,
    SEND_STARTER,
    SEND_STARTXID,
    SEND_PARTNODES,
    SEND_QUERY,
    SEND_ROLLBACK,
    SEND_COMMIT_TIMESTAMP,
    SEND_PGCLEAN,
    SEND_READONLY,
    SEND_AFTER_PREPARE,
    SEND_DATABASE,
    SEND_USER
}EnsureCapacityStack;
extern bool                 complish;
extern TwophaseTransAt      twophase_in;
extern EnsureCapacityStack  capacity_stack;
extern int                  exception_count;
extern void ClearTwophaseException(void);
#endif

#ifdef __TWO_PHASE_TRANS__
typedef enum
{
    TWO_PHASE_HEALTHY = 0,                    /* send cmd succeed */
    TWO_PHASE_SEND_GXID_ERROR = -1,            /* send gxid failed */
    TWO_PHASE_SEND_TIMESTAMP_ERROR = -2,       /* send timestamp fail */
    TWO_PHASE_SEND_STARTER_ERROR = -3,         /* send startnode fail */
    TWO_PHASE_SEND_STARTXID_ERROR = -4,        /* send xid in startnode fail */
    TWO_PHASE_SEND_PARTICIPANTS_ERROR = -5,    /* send participants fail */
    TWO_PHASE_SEND_QUERY_ERROR = -6            /* send cmd fail */
}ConnState;
typedef enum 
{
    TWO_PHASE_INITIALTRANS = 0,   /* initial state */
    TWO_PHASE_PREPARING,          /* start to prepare */
    TWO_PHASE_PREPARE_END,        /* remote node complete prepare */
    TWO_PHASE_PREPARED,           /* finish prepare */
    TWO_PHASE_PREPARE_ERROR,      /* fail to prepare */
    TWO_PHASE_COMMITTING,         /* start to commit */
    TWO_PHASE_COMMIT_END,         /* remote node complete commit */
    TWO_PHASE_COMMITTED,          /* finish commit */
    TWO_PHASE_COMMIT_ERROR,       /* send fail or response fail during 'commit prepared' */
    TWO_PHASE_ABORTTING,          /* start to commit */
    TWO_PHASE_ABORT_END,          /* remote node complete abort*/
    TWO_PHASE_ABORTTED,            /* finish abort */
    TWO_PHASE_ABORT_ERROR,        /* send fail or response fail during 'rollback prepared'*/
    TWO_PHASE_UNKNOW_STATUS       /* explicit twophase trans can not GetGTMGID */
}TwoPhaseTransState;

typedef enum
{
    OTHER_OPERATIONS = 0,       /* we do not update g_twophase_state in receive_response for  OTHER_OPERATIONS*/
    REMOTE_PREPARE,             /* from pgxc_node_remote_prepare */
    REMOTE_PREPARE_ERROR,       /* from prepare_err in pgxc_node_remote_prepare */
    REMOTE_PREPARE_ABORT,       /* from abort in prepare_err */
    REMOTE_FINISH_COMMIT,       /* from pgxc_node_remote_finish(commit) */
    REMOTE_FINISH_ABORT,        /* from pgxc_node_remote_finish(abort) */
    REMOTE_ABORT                /* from pgxc_node_remote_abort */
}CurrentOperation;              /* record twophase trans operation before receive responses */

typedef struct ConnTransState	/* record twophase trasaction state of each connection*/
{
    bool                is_participant;
    ConnState           conn_state;     /* record state of each connection in twophase trans */
	TwoPhaseTransState	state;	        /* state of twophase trans in each connection */
	int			        handle_idx;     /* index of dn_handles or cn_handles */
}ConnTransState;

typedef struct AllConnNodeInfo
{
    char    node_type;                  /* 'C' or 'D'*/
    int     conn_trans_state_index;     /*index in g_twophase_state.coord_state or g_twophase_state.datanode_state*/
}AllConnNodeInfo;

typedef struct LocalTwoPhaseState
{
    bool                in_pg_clean;    /* execute in pg_clean */
    bool                is_start_node;
    bool                is_readonly;   /* since explicit transaction can be readonly, need to record readonly in 2pc file */
    bool                is_after_prepare; /* record whether the transaction pass the whole prepare phase */
	char 		        *gid;	        /* gid of twophase transaction*/
	TwoPhaseTransState	state;			    /* global twophase state */		
	ConnTransState 	    *coord_state;       /* each coord participants state */
    int                 coord_index;          /* index of coord_state */
	ConnTransState 	    *datanode_state;
    int                 datanode_index;       /* index of datanode_state */
    bool                isprinted;          /* is printed in AbortTransaction */
	char		        start_node_name[NAMEDATALEN];   /* twophase trans startnode */
    TransactionId       start_xid;
    char                *participants;
    PGXCNodeAllHandles  *handles;   /* handles in each phase in twophase trans */
	PGXCNodeAllHandles	*origin_handles_ptr;
    AllConnNodeInfo     *connections;   /* map to coord_state or datanode_state in pgxc_node_receive_response */
    int                 connections_num;
    CurrentOperation    response_operation;
    char                *database; /* twophase database */
    char                *user;     /* twophase user */
} LocalTwoPhaseState;
extern LocalTwoPhaseState g_twophase_state;

#define IS_XACT_IN_2PC	(g_twophase_state.state != TWO_PHASE_INITIALTRANS) /* whether the transaction is in 2pc */
#define IS_READONLY_DATANODE (g_twophase_state.is_readonly && IS_PGXC_DATANODE)

#endif

extern uint64 GucSubtxnId;
/* ----------------
 *		extern definitions
 * ----------------
 */
extern bool IsTransactionState(void);
extern bool IsTransactionCommit(void);
extern bool IsAbortedTransactionBlockState(void);
extern bool IsTransactionAbortState(void);
extern TransactionId GetTopTransactionId(void);
extern TransactionId GetTopTransactionIdIfAny(void);
extern TransactionId GetCurrentTransactionId(void);
#ifndef __USE_GLOBAL_SNAPSHOT__
extern char *GetGlobalXid(void);
extern char *GetGlobalXidNoCheck(void);
extern void SetGlobalXid(const char *globalXidString);
extern uint64 GetGlobalXidVersion(void);

extern const char *CurrentTransactionBlockStateAsString(void);
extern const char *CurrentTransactionTransStateAsString(void);

#endif

#ifdef __OPENTENBASE__
extern void AtEOXact_Global(void);
#endif
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern void SetGlobalCommitTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetGlobalCommitTimestamp(void);
extern void SetGlobalPrepareTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetGlobalPrepareTimestamp(void);

extern void SetLocalCommitTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetLocalCommitTimestamp(void);
extern void SetLocalPrepareTimestamp(GlobalTimestamp timestamp);
extern GlobalTimestamp GetLocalPrepareTimestamp(void);

extern void AtEOXact_SetUser(bool error_out);
#endif


extern TransactionId GetCurrentTransactionIdIfAny(void);
#ifdef __OPENTENBASE__
extern bool GetCurrentLocalParamStatus(void);
extern void SetCurrentLocalParamStatus(bool status);
#endif
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern void AssignGlobalXid(void);
#endif
#ifdef PGXC  /* PGXC_COORD */
extern GlobalTransactionId GetAuxilliaryTransactionId(void);
extern GlobalTransactionId GetTopGlobalTransactionId(void);
extern void SetAuxilliaryTransactionId(GlobalTransactionId gxid);
extern void SetTopGlobalTransactionId(GlobalTransactionId gxid);
extern void SetTopTransactionId(GlobalTransactionId xid);
#endif
#ifdef __OPENTENBASE__
extern bool InSubTransaction(void);
extern void CheckTxnStateForSavepointAndRollbackTo(void);
extern bool InPlpgsqlFunc(void);
extern int SetEnterPlpgsqlFunc(void);
extern void SetExitPlpgsqlFunc(int plpgsql_level);
extern void SetEnterPlpgsqlAnalysis(void);
extern void SetExitPlpgsqlAnalysis(void);
extern bool IsInPlpgsqlAnalysis(void);
extern bool SavepointDefined(void);
extern void bump_sub_transaction_id_current_txn_handles(void);
extern bool ExecDDLWithoutAcquireXid(Node* parsetree);
extern bool IsTransactionIdle(void);
extern const char * GetPrepareGID(void);
extern void ClearPrepareGID(void);
extern MemoryContext GetCurrentTransactionContext(void);
extern ResourceOwner GetCurrentTransactionResourceOwner(void);
extern MemoryContext GetValidTransactionContext(void);
#endif
extern TransactionId GetStableLatestTransactionId(void);
extern bool isXactWriteLocalNode(void);
extern SubTransactionId GetCurrentSubTransactionId(void);
extern void SetCurrentCommandIdUsedForWorker(void);
extern void MarkCurrentTransactionIdLoggedIfAny(void);
extern bool SubTransactionIsActive(SubTransactionId subxid);
extern TransactionId FindTransactionParent(TransactionId xid);
extern CommandId GetCurrentCommandId(bool used);
extern void SetParallelStartTimestamps(TimestampTz xact_ts, TimestampTz stmt_ts);
extern TimestampTz GetCurrentTransactionStartTimestamp(void);
extern TimestampTz GetCurrentStatementStartTimestamp(void);
#ifdef XCP
extern TimestampTz GetCurrentLocalStatementStartTimestamp(void);
#endif
extern TimestampTz GetCurrentTransactionStopTimestamp(void);
extern void SetCurrentStatementStartTimestamp(void);
#ifdef PGXC
extern TimestampTz GetCurrentGTMStartTimestamp(void);
extern void SetCurrentGTMDeltaTimestamp(TimestampTz timestamp);
#endif
extern int	GetCurrentTransactionNestLevel(void);
extern bool TransactionIdIsCurrentTransactionId(TransactionId xid);
extern void CommandCounterIncrement(void);
extern void CommandCounterIncrementNotFlushCid(void);
extern void ForceSyncCommit(void);
extern void StartTransactionCommand(void);
extern void CommitTransactionCommand(void);
#ifdef PGXC
extern void AbortCurrentTransactionOnce(void);
#endif
extern void AbortCurrentTransaction(void);
extern void BeginTransactionBlock(void);
extern bool EndTransactionBlock(void);
extern bool PrepareTransactionBlock(const char *gid);
extern void UserAbortTransactionBlock(void);
extern void ReleaseSavepoint(List *options);
extern void DefineSavepoint(char *name, bool internal);
extern void RollbackToSavepoint(List *options);
extern void BeginInternalSubTransactionInternal(char *name, const char *filename, int lineno);
#define BeginInternalSubTransaction(name) BeginInternalSubTransactionInternal(name, __FILE__, __LINE__)
extern void ReleaseCurrentSubTransaction(void);
extern void RollbackAndReleaseCurrentSubTransaction(void);
extern bool IsSubTransaction(void);
extern Size EstimateTransactionStateSpace(void);
extern void SerializeTransactionState(Size maxsize, char *start_address);
#ifdef __SUPPORT_DISTRIBUTED_TRANSACTION__
extern void GlobalXidShmemInit(void);
extern void SerializeGlobalXid(Size maxsize, char *start_address);
extern void StartParallelWorkerGlobalXid(char *address);
extern Size
EstimateGlobalXidSpace(void);
#endif
#ifdef _PG_ORCL_
extern bool IsPLpgSQLSubTransaction(void);
extern void MarkAsPLpgSQLSubTransaction(void);
extern int GetPlsqlExcepSubLevel(void);
extern int *GetPLpgSQLSubTransactionNum(int *plsub_count);
#endif
extern void StartParallelWorkerTransaction(char *tstatespace);
extern void EndParallelWorkerTransaction(void);
extern bool IsTransactionBlock(void);
extern bool IsBeginTransactionBlock(void);
extern bool IsRunningTransactionBlock(void);
extern bool IsTransactionOrTransactionBlock(void);
extern char TransactionBlockStatusCode(void);
extern void AbortOutOfAnyTransaction(void);
extern void PreventTransactionChain(bool isTopLevel, const char *stmtType);
extern void RequireTransactionChain(bool isTopLevel, const char *stmtType);
extern void WarnNoTransactionChain(bool isTopLevel, const char *stmtType);
extern bool IsInTransactionChain(bool isTopLevel);
extern void RegisterXactCallback(XactCallback callback, void *arg);
extern void UnregisterXactCallback(XactCallback callback, void *arg);
extern void RegisterXactCallbackOnce(XactCallback callback, void *arg);
extern void UnregisterXactCallbackOnce(XactCallback callback, void *arg);
extern void RegisterSubXactCallback(SubXactCallback callback, void *arg);
extern void UnregisterSubXactCallback(SubXactCallback callback, void *arg);

#ifdef PGXC
extern void RegisterGTMCallback(GTMCallback callback, void *arg);
extern void UnregisterGTMCallback(GTMCallback callback, void *arg);
extern void RegisterTransactionNodes(int count, void **connections, bool write);
extern void ForgetTransactionNodes(void);
extern void RegisterTransactionLocalNode(bool write);
extern bool IsTransactionLocalNode(bool write);
extern void ForgetTransactionLocalNode(void);
extern bool IsXidImplicit(const char *xid);
extern void SaveReceivedCommandId(CommandId cid);
extern void SetReceivedCommandId(CommandId cid);
extern CommandId GetReceivedCommandId(void);
extern void ReportCommandIdChange(CommandId cid, bool flush_immediately);
extern bool IsSendCommandId(void);
extern void SetSendCommandId(bool status);
extern bool IsPGXCNodeXactReadOnly(void);
extern bool IsPGXCNodeXactDatanodeDirect(void);
extern void TransactionRecordXidWait(TransactionId xid);
#endif

extern int	xactGetCommittedChildren(TransactionId **ptr);

extern XLogRecPtr XactLogCommitRecord(CommitSeqNo gts,
					TimestampTz	 commit_time,
					int nsubxacts, TransactionId *subxacts,
					int nrels, RelFileNodeNFork *rels,
					int nmsgs, SharedInvalidationMessage *msgs,
					bool relcacheInval, bool forceSync,
					int xactflags,
					TransactionId twophase_xid);

extern XLogRecPtr XactLogAbortRecord(CommitSeqNo gts,
					TimestampTz abort_time,
				   int nsubxacts, TransactionId *subxacts,
				   int nrels, RelFileNodeNFork *rels,
				   int xactflags, TransactionId twophase_xid);
extern void xact_redo(XLogReaderState *record);
extern void PushUnlinkRelToHTAB(RelFileNodeNFork *xnodes, int nrels);

/* xactdesc.c */
extern void xact_desc(StringInfo buf, XLogReaderState *record);
extern const char *xact_identify(uint8 info);

/* also in xactdesc.c, so they can be shared between front/backend code */
extern void ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, xl_xact_parsed_commit *parsed);
extern void ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, xl_xact_parsed_abort *parsed);

extern void EnterParallelMode(void);
extern void ExitParallelMode(void);
extern bool IsInParallelMode(void);
extern void PrepareParallelModePlanExec(CmdType commandType);
extern void SetTransIsolationLevel(int xactisolevel);

extern char *GetTransactionString(void);
extern char *GetHandleTransactionAndGucString(PGXCNodeHandle *handle, uint64 *guc_cid);
extern char *GetAllTransactionAndGucString(bool with_begin);
extern void TransactionSetGuc(bool local, const char *name, const char *value, int flags, uint64 guc_cid);
extern void TransactionResetAllGuc(uint64 guc_cid);
extern void TransactionDelGuc(bool local, const char *name);
extern List *GetTopTransactionGucList(void);
extern List *GetCurrentTransactionGucList(void);
extern void ResetTopTransactionGucList(void);
extern void ResetCurrentTransactionGucList(void);
extern void SubTransactionTransferGucListToParent(void);
extern void TopTxnTransferNoneLocalGucList(void);
extern uint64	nRequestedXid;
extern uint64	nRequestedXidSPMUsed;
extern void SetCurrentTransactionCanLocalCommit(void);
extern bool IsCurrentTransactionCanLocalCommit(void);

extern bool IsPlsqlImplicitSavepoint(void);
extern int  GetTransactionSpiLevel(void);
extern void SetTransactionSpiLevel(int spi_level);

/* opentenbase_ora: The moment for dbms_session.reset_package */
typedef enum
{
	RESET_PKG_NONE,
	RESET_PKG_BEGIN,
	RESET_PKG_END
} ResetPkgMoment;

extern ResetPkgMoment reset_package_moment;

/*
 * IsModifySupportedInParallelMode
 *
 * Indicates whether execution of the specified table-modification command
 * (INSERT/UPDATE/DELETE) in parallel-mode is supported, subject to certain
 * parallel-safety conditions.
 */
static inline bool
IsModifySupportedInParallelMode(CmdType commandType)
{
	/* Currently INSERT and UPDATE are supported */
	return (commandType == CMD_INSERT) || (commandType == CMD_UPDATE);
}

extern bool InPlpyFunc(void);
extern void SetEnterPlpyFunc(void);
extern void SetExitPlpyFunc(void);

extern MemoryContext SwitchToTransactionAbortContext(void);
#endif							/* XACT_H */
