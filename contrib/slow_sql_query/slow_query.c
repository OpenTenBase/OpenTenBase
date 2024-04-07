#include "postgres.h"
#include "fmgr.h"
#include 'slow_query.conf'
#include "access/htup_details.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

/* 插件名称 */
PG_FUNCTION_INFO_V1(slow_query);

/* 全局变量：用于存储慢查询的阈值 */
static int slow_query_threshold = 2000; // 默认阈值为2000毫秒

/* 执行SQL查询的钩子函数 */
static ExecutorRun_hook_type prev_ExecutorRun = NULL;

/* 慢查询钩子函数 */
static void slow_query_hook(QueryDesc *queryDesc, int eflags);

/* 设置慢查询阈值的GUC变量 */
static void assign_slow_query_threshold(int newval, void *extra);

/* 模块加载时的初始化函数 */
void _PG_init(void);

/* 模块卸载时的清理函数 */
void _PG_fini(void);

/* 慢查询钩子函数 */
static void
slow_query_hook(QueryDesc *queryDesc, int eflags)
{
    /* 获取当前查询的开始时间 */
    TimestampTz start_time = GetCurrentTimestamp();
    int query_time_ms; // 查询执行时间（毫秒）

    /* 调用之前注册的执行钩子函数 */
    if (prev_ExecutorRun)
        prev_ExecutorRun(queryDesc, eflags);

    /* 计算查询执行时间 */
    query_time_ms = DatumGetInt32(DirectFunctionCall1(timestamptz_mi,
                                                      TimestampTzGetDatum(start_time)));
    query_time_ms = query_time_ms / 1000;

    /* 检查查询执行时间是否超过阈值 */
    if (query_time_ms > slow_query_threshold)
    {
        /* 记录慢查询信息到日志 */
        elog(LOG, "Slow query detected: query='%s', duration='%d milliseconds'",
             queryDesc->sourceText ? queryDesc->sourceText : "<unknown>",
             query_time_ms);
    }
}

/* 设置慢查询阈值的GUC变量 */
static void
assign_slow_query_threshold(int newval, void *extra)
{
    /* 检查阈值是否为非负值 */
    if (newval < 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("slow_query_threshold must be non-negative")));
    }

    /* 更新全局变量 */
    slow_query_threshold = newval;
}

/* 模块加载时的初始化函数 */
void
_PG_init(void)
{
    /* 注册慢查询钩子函数 */
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = slow_query_hook;

    /* 注册GUC变量 */
    DefineCustomIntVariable("slow_query_threshold",
                            "Sets the threshold for slow queries in milliseconds",
                            NULL,
                            &slow_query_threshold,
                            1000, // 默认值为1000毫秒
                            0, INT_MAX,
                            PGC_SIGHUP,
                            GUC_UNIT_MS,
                            assign_slow_query_threshold,
                            NULL);
}

/* 模块卸载时的清理函数 */
void
_PG_fini(void)
{
    /* 恢复原始的执行钩子函数 */
    ExecutorRun_hook = prev_ExecutorRun;
}