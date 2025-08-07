#ifndef SPM_CACHE_H
#define SPM_CACHE_H

#include "c.h"
#include "optimizer/spm.h"


#define INVALID_SPM_CACHE_PLAN_ID -1
#define INVALID_SPM_CACHE_SQL_ID -1

#define SPM_CACHE_COUNTS_COL 6
#define SPM_CACHE_PLAN_COUNTS 1
#define SPM_CACHE_PLAN_HINT_COUNTS 2
#define SPM_CACHE_PLAN_HINT_RATIO 3
#define SPM_CACHE_HISTORY_COUNTS 4
#define SPM_CACHE_HISTORY_HINT_COUNTS 5
#define SPM_CACHE_HISTORY_HINT_RATIO 6

#define SPM_PLAN_CACHE_COLS 9
#define SPM_PLAN_CACHE_DBOID 1
#define SPM_PLAN_CACHE_SQL_ID 2
#define SPM_PLAN_CACHE_PLAN_ID 3
#define SPM_PLAN_CACHE_HINT 4
#define SPM_PLAN_CACHE_PRIORITY 5
#define SPM_PLAN_CACHE_ENABLED 6
#define SPM_PLAN_CACHE_ARRAY_ID 7
#define SPM_PLAN_CACHE_ITEM_ID 8
#define SPM_PLAN_CACHE_USECOUNT 9

#define SPM_HISTORY_CACHE_COLS 6
#define SPM_HISTORY_CACHE_DBOID 1
#define SPM_HISTORY_CACHE_SQL_ID 2
#define SPM_HISTORY_CACHE_PLAN_ID 3
#define SPM_HISTORY_CACHE_ARRAY_ID 4
#define SPM_HISTORY_CACHE_ITEM_ID 5
#define SPM_HISTORY_CACHE_USECOUNT 6

typedef enum
{
	SPM_PLAN_CACHE_ITEM_PLANID = 1,    /* Fixed, Highest priority. */
	SPM_PLAN_CACHE_ITEM_HINT,
	SPM_PLAN_CACHE_ITEM_PRIORITY,
	SPM_PLAN_CACHE_ITEM_ENABLED,
	SPM_PLAN_CACHE_ITEM_EXECUTIONS,
	SPM_PLAN_CACHE_ITEM_INVALID
} SPMPlanCachedItemIdx;

typedef enum
{
	SPM_PLAN_CACHE_FIXED = 1,    /* Fixed, Highest priority. */
	SPM_PLAN_CACHE_ACCEPT,
	SPM_PLAN_CACHE_MATCHED,
	SPM_PLAN_CACHE_NO_MATCHED,
	SPM_PLAN_CACHE_NO_VALID,
	SPM_PLAN_CACHE_NO_ENTRY
} SPMPlanCacheRetNo;

typedef enum
{
	SPM_PLAN_CACHE_PLAN = 1,    /* Fixed, Highest priority. */
	SPM_PLAN_CACHE_INVALID_PLAN,
	SPM_PLAN_CACHE_HISTORY,
	SPM_PLAN_CACHE_INVALID_HISTORY
} SPMPlanCacheType;

typedef struct SPMPlanCachedItem
{
	int64			plan_id;
	char			hint[SPM_PLAN_HINT_MAX_LEN];
	SPMPlanPriority	priority;
	bool			enabled;
	int64			executions;
	struct SPMPlanCachedItem *next;
} SPMPlanCachedItem;

typedef struct SPMCachedUniqueKey
{
	Oid		dboid;
	int64	sql_id;
	int64	plan_id;
} SPMCachedUniqueKey;

typedef struct SPMCachedIdEntry
{
	SPMCachedUniqueKey	key;
	int					id;
} SPMCachedIdEntry;

typedef struct SPMCacheNode
{
	SPMCachedUniqueKey	tag;
	bool				valid;
	pg_atomic_uint32	refcount;
	pg_atomic_uint32	usecount;
} SPMCacheNode;

typedef struct SPMCacheNodeDesc
{
	pg_atomic_uint32 next_victim;
	pg_atomic_uint32 next_invalid_victim;
} SPMCacheNodeDesc;

typedef struct SPMCachedKey
{
	Oid			dboid;
	int64		sql_id;
} SPMCachedKey;

typedef struct SPMPlanCachedEntry
{
	SPMCachedKey		key;
	bool				valid;
	uint32				id;
	LWLock				lock;
	SPMPlanCachedItem	*value;
} SPMPlanCachedEntry;

typedef struct SPMHistoryCachedItemForTblSync
{
	int64			plan_id;
	int64			executions;
	int64			elapsed_time;
} SPMHistoryCachedItemForTblSync;

typedef struct SPMHistoryCachedItem
{
	int64						plan_id;
	pg_atomic_uint64			executions;
	pg_atomic_uint64			elapsed_time;
	struct SPMHistoryCachedItem *next;
} SPMHistoryCachedItem;

typedef struct SPMHistoryCachedEntry
{
	SPMCachedKey			key;
	bool					valid;
	uint32					id;
	LWLock					lock;
	SPMHistoryCachedItem	*value;
} SPMHistoryCachedEntry;

typedef struct SPMCacheCount
{
	uint64 plan_total_count;
	uint64 plan_hint_count;
	uint64 history_total_count;
	uint64 history_hint_count;
	TimestampTz		plan_resettime;
	TimestampTz		history_resettime;
	slock_t 		slock;
} SPMCacheCount;

extern Size SPMPlanShmemSize(void);
extern void SPMPlanShmemInit(void);
extern Size SPMHistoryShmemSize(void);
extern void SPMHistoryShmemInit(void);
extern SPMPlanCacheRetNo GetSPMBindHint(int64 sql_id, int64 *plan_id, 
										char **hint);
extern void SPMPlanHashTblDelete(Oid dboid, int64 sqlid, int64 plan_id);
extern void SPMPlanHashTblDeleteEntry(Oid dboid, int64 sql_id);
extern void SPMPlanHashTabInsert(int64 sqlid, int64 plan_id, char *hint, 
									int32 priority, bool enabled, 
									int64 executions);
extern SPMPlanCacheRetNo SPMPlanCacheSearchByPlanId(Oid dboid, int64 sql_id, 
													int64 plan_id, Datum *values, 
													bool *nulls);
extern void SPMCacheHashTabClear(void);
extern void CommitSPMPendingList(void);
extern void AbortSPMPendingList(void);

extern SPMPlanCacheRetNo SPMHistoryCacheSearchByPlanId(Oid dboid, int64 sql_id, int64 plan_id,
                                                       Datum *values, bool *nulls,
                                                       int64 execute_time);
extern void SPMHistoryHashTblDelete(Oid dboid, int64 sql_id, int64 plan_id);
extern void SPMHistoryHashTblDeleteEntry(Oid dboid, int64 sql_id);
extern void SPMHistoryHashTabInsert(int64 sql_id, int64 plan_id, int64 executions, int64 elapsed_time);
extern void SPMHistoryHashTabClear(void);
extern List *SPMHistoryCacheSearchBySqlid(int64 sql_id);

extern void SPMHistoryHashTabPrint(void);
extern void SPMHistoryIdHashTabPrint(void);

extern void SPMPlanSearchAll(TupleDesc tupdesc, Tuplestorestate *tupstore);
extern void SPMPlanSearchAllBySqlId(Oid dboid, int64 sql_id, 
									TupleDesc tupdesc, 
									Tuplestorestate *tupstore);
extern void SPMPlanRelaseBySqlId(Oid dboid, int64 sql_id, int64 plan_id);
extern int SPMPlanRelaseCrushed(int trycount);
extern void SPMHistorySearchAll(TupleDesc tupdesc, Tuplestorestate *tupstore);
extern void SPMHistorySearchAllBySqlId(Oid dboid, int64 sql_id, 
									TupleDesc tupdesc, 
									Tuplestorestate *tupstore);
extern void SPMHistoryRelaseBySqlId(Oid dboid, int64 sql_id, int64 plan_id);
extern int SPMHistoryRelaseCrushed(int trycount);
extern void SPMCacheShowHintRatio(TupleDesc tupdesc, Tuplestorestate *tupstore);


#endif /* SPM_CACHE_H */