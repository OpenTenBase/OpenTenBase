#include "tenbase/pg.h"
#include "tenbase/fmgr.h"
#include "tenbase/executor.h"
#include "tenbase/utils/timestamp.h"
#include "tenbase/utils/guc.h"
#include "tenbase/catalog/pg_type.h"
#include "tenbase/pgstat.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

typedef struct {
    TimestampTz start_time;
    TimestampTz end_time;
    int expected_time; // 预期时间（微秒）
    bool exceeded;     // 是否超出预期时间
} QueryTiming;

HTAB *query_timings = NULL;

// 初始化哈希表
static void init_query_timings(void) {
    HASHCTL ctl;
    MemSet(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(uint32);
    ctl.entrysize = sizeof(QueryTiming);
    query_timings = hash_create("QueryTimings", 256, &ctl, HASH_ELEM | HASH_BLOBS);
}

// ExecutorStart钩子函数
static void pg_stat_statements_ExecutorStart(QueryDesc *queryDesc, int eflags) {
    uint32 query_id = queryDesc->plannedstmt->queryId;

    // 创建查询时间记录
    QueryTiming *timing = hash_search(query_timings, &query_id, HASH_ENTER, NULL);
    timing->start_time = GetCurrentTimestamp();
    timing->end_time = 0;
    timing->exceeded = false;
    timing->expected_time = GetConfigOptionIntByName("etime.min_value", 0, false);

    // 调用之前的ExecutorStart_hook函数
    if (prev_ExecutorStart_hook) {
        prev_ExecutorStart_hook(queryDesc, eflags);
    } else {
        standard_ExecutorStart(queryDesc, eflags);
    }
}

// ExecutorEnd钩子函数
static void pg_stat_statements_ExecutorEnd(QueryDesc *queryDesc) {
    uint32 query_id = queryDesc->plannedstmt->queryId;

    // 查找查询的时间记录
    QueryTiming *timing = hash_search(query_timings, &query_id, HASH_FIND, NULL);
    if (timing != NULL) {
        // 记录SQL结束时间
        timing->end_time = GetCurrentTimestamp();

        // 计算SQL执行时间
        long elapsed = timing->end_time - timing->start_time;

        // 如果预期时间大于0且实际执行时间超过预期时间，则记录超时查询
        if (timing->expected_time > 0 && elapsed > timing->expected_time) {
            timing->exceeded = true;
            ereport(LOG,
                    (errmsg("Query (ID: %u) exceeded expected time: %d microseconds (actual: %ld microseconds)", 
                            query_id, timing->expected_time, elapsed)));
        }

        // 删除已经记录的时间信息
        hash_search(query_timings, &query_id, HASH_REMOVE, NULL);
    }

    // 调用之前的ExecutorEnd_hook函数
    if (prev_ExecutorEnd_hook) {
        prev_ExecutorEnd_hook(queryDesc);
    } else {
        standard_ExecutorEnd(queryDesc);
    }
}

// 设置预期时间（微秒）
Datum set_expected_time(PG_FUNCTION_ARGS) {
    int expected_time = PG_GETARG_INT32(0);

    // 将预期时间设置为指定值
    if (expected_time < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Expected time cannot be negative")));
    }
    SetConfigOption("etime.min_value", DatumGetCString(DirectFunctionCall1(int4out, Int32GetDatum(expected_time))), PGC_USERSET, PGC_S_SESSION);
    PG_RETURN_NULL();
}

// 获取预期时间（微秒）
Datum get_expected_time(PG_FUNCTION_ARGS) {
    int expected_time = GetConfigOptionIntByName("etime.min_value", 0, false);
    PG_RETURN_INT32(expected_time);
}

// 初始化函数
void _PG_init(void) {
    init_query_timings();

    // 保存之前的ExecutorStart_hook函数，并注册自己的钩子函数
    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = pg_stat_statements_ExecutorStart;

    // 保存之前的ExecutorEnd_hook函数，并注册自己的钩子函数
    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = pg_stat_statements_ExecutorEnd;

    // 注册自定义函数：set_expected_time，用于设置预期时间
    DefineCustomFunction("set_expected_time",
                         set_expected_time,
                         NULL,
                         NULL);

    // 注册自定义函数：get_expected_time，用于获取预期时间
    DefineCustomFunction("get_expected_time",
                         get_expected_time,
                         NULL,
                         NULL);
}

// 卸载函数
void _PG_fini(void) {
    // 恢复原始的ExecutorStart_hook和ExecutorEnd_hook函数
    ExecutorStart_hook = prev_ExecutorStart_hook;
    ExecutorEnd_hook = prev_ExecutorEnd_hook;

    // 清理哈希表
    if (query_timings != NULL) {
        hash_destroy(query_timings);
    }
}
