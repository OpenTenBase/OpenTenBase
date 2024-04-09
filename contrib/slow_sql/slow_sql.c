#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

// 定义GUC变量：慢查询的阈值（单位：毫秒）
static int slow_sql_min_duration;
static TimestampTz startTime = 0;
static TimestampTz endTime = 0;

// 钩子函数的原始指针
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

// 我们自定义的钩子函数
static void slow_sql_executor_start(QueryDesc *queryDesc, int eflags);
static void slow_sql_executor_end(QueryDesc *queryDesc);

// 模块加载与卸载
void _PG_init(void);
void _PG_fini(void);

// 自定义钩子函数实现
static void slow_sql_executor_start(QueryDesc *queryDesc, int eflags) {
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    // 记录开始时间
    startTime = GetCurrentTimestamp();
}

static void slow_sql_executor_end(QueryDesc *queryDesc) {
    endTime = GetCurrentTimestamp();
    int duration = endTime-startTime;

    // 检查是否超过慢查询阈值
    if (duration > slow_sql_min_duration) {
        ereport(LOG, (errmsg("慢查询: %s, 执行时间: %d 毫秒.", 
                            queryDesc->sourceText, duration)));
    }

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

// GUC配置函数
static void assign_slow_sql_min_duration(int newval, void *extra) {
    slow_sql_min_duration = newval;
}

void _PG_init(void) {
    // 注册GUC变量
    DefineCustomIntVariable("slow_sql.min_duration",
                            "慢查询阈值（毫秒）.",
                            NULL,
                            &slow_sql_min_duration,
                            10000, // 默认10秒
                            0, INT_MAX,
                            PGC_SUSET,
                            GUC_UNIT_MS,
                            assign_slow_sql_min_duration,
                            NULL,
                            NULL);

    // 保存并注册钩子
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = slow_sql_executor_start;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = slow_sql_executor_end;
}

void _PG_fini(void) {
    // 恢复原有钩子
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
}
