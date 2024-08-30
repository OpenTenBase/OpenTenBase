/*-------------------------------------------------------------------------
 *
 * auto_explain.c
 *
 *
 * Copyright (c) 2008-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * IDENTIFICATION
 *      contrib/auto_explain/auto_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "commands/explain.h"
#include "executor/instrument.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* GUC variables */
/* �Զ�ִ�мƻ���־��¼�����ִ��ʱ�䣨���룩��-1��ʾ���� */
static int    auto_explain_log_min_duration = -1; /* msec or -1 */
/* �Ƿ�ʹ��EXPLAIN ANALYZE��¼ִ�мƻ� */
static bool auto_explain_log_analyze = false;
/* �Ƿ��¼��ϸ��Ϣ */
static bool auto_explain_log_verbose = false;
/* �Ƿ��¼��������Ϣ */
static bool auto_explain_log_buffers = false;
/* �Ƿ��¼��������Ϣ */
static bool auto_explain_log_triggers = false;
/* �Ƿ��¼��ʱ��Ϣ */
static bool auto_explain_log_timing = true;
/* ��¼��ʽ */
static int    auto_explain_log_format = EXPLAIN_FORMAT_TEXT;
/* �Ƿ��¼Ƕ����� */
static bool auto_explain_log_nested_statements = false;
/* ִ�мƻ������� */
static double auto_explain_sample_rate = 1;

/* �ƻ���ʽѡ�� */
static const struct config_enum_entry format_options[] = {
    {"text", EXPLAIN_FORMAT_TEXT, false},
    {"xml", EXPLAIN_FORMAT_XML, false},
    {"json", EXPLAIN_FORMAT_JSON, false},
    {"yaml", EXPLAIN_FORMAT_YAML, false},
    {NULL, 0, false}
};

/* Current nesting depth of ExecutorRun calls */
/* ExecutorRun���õĵ�ǰǶ����� */
static int    nesting_level = 0;

/* Saved hook values in case of unload */
/* ж��ʱ����Ĺ���ֵ */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/* Is the current query sampled, per backend */
/* ��ǰ��ѯ�Ƿ���� */
static bool current_query_sampled = true;

/* ����Զ�ִ�мƻ��Ƿ����� */
#define auto_explain_enabled() \
    (auto_explain_log_min_duration >= 0 && \
     (nesting_level == 0 || auto_explain_log_nested_statements))

void        _PG_init(void);
void        _PG_fini(void);

/* ִ�мƻ����Ӻ��� */
static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
                    ScanDirection direction,
                    uint64 count, bool execute_once);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);


/*
 * Module load callback
 */
 /*
  * ģ����ػص�
  */
void
_PG_init(void)
{
    /* Define custom GUC variables. */
    DefineCustomIntVariable("auto_explain.log_min_duration",
                            "Sets the minimum execution time above which plans will be logged.",
                            "Zero prints all plans. -1 turns this feature off.",
                            &auto_explain_log_min_duration,
                            -1,
                            -1, INT_MAX / 1000,
                            PGC_SUSET,
                            GUC_UNIT_MS,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomBoolVariable("auto_explain.log_analyze",
                             "Use EXPLAIN ANALYZE for plan logging.",
                             NULL,
                             &auto_explain_log_analyze,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("auto_explain.log_verbose",
                             "Use EXPLAIN VERBOSE for plan logging.",
                             NULL,
                             &auto_explain_log_verbose,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("auto_explain.log_buffers",
                             "Log buffers usage.",
                             NULL,
                             &auto_explain_log_buffers,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("auto_explain.log_triggers",
                             "Include trigger statistics in plans.",
                             "This has no effect unless log_analyze is also set.",
                             &auto_explain_log_triggers,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomEnumVariable("auto_explain.log_format",
                             "EXPLAIN format to be used for plan logging.",
                             NULL,
                             &auto_explain_log_format,
                             EXPLAIN_FORMAT_TEXT,
                             format_options,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("auto_explain.log_nested_statements",
                             "Log nested statements.",
                             NULL,
                             &auto_explain_log_nested_statements,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("auto_explain.log_timing",
                             "Collect timing data, not just row counts.",
                             NULL,
                             &auto_explain_log_timing,
                             true,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomRealVariable("auto_explain.sample_rate",
                             "Fraction of queries to process.",
                             NULL,
                             &auto_explain_sample_rate,
                             1.0,
                             0.0,
                             1.0,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    EmitWarningsOnPlaceholders("auto_explain");

    /* Install hooks. */
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = explain_ExecutorStart;
    prev_ExecutorRun = ExecutorRun_hook;
    ExecutorRun_hook = explain_ExecutorRun;
    prev_ExecutorFinish = ExecutorFinish_hook;
    ExecutorFinish_hook = explain_ExecutorFinish;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
    /* Uninstall hooks. */
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorRun_hook = prev_ExecutorRun;
    ExecutorFinish_hook = prev_ExecutorFinish;
    ExecutorEnd_hook = prev_ExecutorEnd;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    /*
     * For rate sampling, randomly choose top-level statement. Either all
     * nested statements will be explained or none will.
     */
    if (auto_explain_log_min_duration >= 0 && nesting_level == 0)
        current_query_sampled = (random() < auto_explain_sample_rate *
                                 MAX_RANDOM_VALUE);

    if (auto_explain_enabled() && current_query_sampled)
    {
        /* Enable per-node instrumentation iff log_analyze is required. */
        if (auto_explain_log_analyze && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
        {
            if (auto_explain_log_timing)
                queryDesc->instrument_options |= INSTRUMENT_TIMER;
            else
                queryDesc->instrument_options |= INSTRUMENT_ROWS;
            if (auto_explain_log_buffers)
                queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
        }
    }

    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    if (auto_explain_enabled() && current_query_sampled)
    {
        /*
         * Set up to track total elapsed time in ExecutorRun.  Make sure the
         * space is allocated in the per-query context so it will go away at
         * ExecutorEnd.
         */
        if (queryDesc->totaltime == NULL)
        {
            MemoryContext oldcxt;

            oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
            queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
            MemoryContextSwitchTo(oldcxt);
        }
    }
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
explain_ExecutorRun(QueryDesc* queryDesc, ScanDirection direction,
    uint64 count, bool execute_once)
{
    nesting_level++; // ����Ƕ�����
    PG_TRY();
    {
        if (prev_ExecutorRun)
            prev_ExecutorRun(queryDesc, direction, count, execute_once); // ����ԭʼ ExecutorRun ����
        else
            standard_ExecutorRun(queryDesc, direction, count, execute_once); // ʹ�ñ�׼ ExecutorRun ����
        nesting_level--; // ����Ƕ�����
    }
    PG_CATCH();
    {
        nesting_level--; // ����Ƕ�����
        PG_RE_THROW(); // �����׳��쳣
    }
    PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc* queryDesc)
{
    nesting_level++; // ����Ƕ�����
    PG_TRY();
    {
        if (prev_ExecutorFinish)
            prev_ExecutorFinish(queryDesc); // ����ԭʼ ExecutorFinish ����
        else
            standard_ExecutorFinish(queryDesc); // ʹ�ñ�׼ ExecutorFinish ����
        nesting_level--; // ����Ƕ�����
    }
    PG_CATCH();
    {
        nesting_level--; // ����Ƕ�����
        PG_RE_THROW(); // �����׳��쳣
    }
    PG_END_TRY();
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc* queryDesc)
{
    if (queryDesc->totaltime && auto_explain_enabled() && current_query_sampled)
    {
        double        msec;

        /*
         * Make sure stats accumulation is done.  (Note: it's okay if several
         * levels of hook all do this.)
         */
        InstrEndLoop(queryDesc->totaltime); // ����ѭ����ͳ��ִ��ʱ��

        /* Log plan if duration is exceeded. */
        msec = queryDesc->totaltime->total * 1000.0; // ��ִ��ʱ��ת��Ϊ����
        if (msec >= auto_explain_log_min_duration) // ���ִ��ʱ�䳬���趨����С��¼ʱ��
        {
            ExplainState* es = NewExplainState(); // ���� ExplainState �ṹ��

            es->analyze = (queryDesc->instrument_options && auto_explain_log_analyze); // �Ƿ����ִ�мƻ�
            es->verbose = auto_explain_log_verbose; // �Ƿ��¼��ϸ��Ϣ
            es->buffers = (es->analyze && auto_explain_log_buffers); // �Ƿ��¼��������Ϣ
            es->timing = (es->analyze && auto_explain_log_timing); // �Ƿ��¼��ʱ��Ϣ
            es->summary = es->analyze; // �Ƿ��ܽ�ִ�мƻ�
            es->format = auto_explain_log_format; // ��¼��ʽ

            ExplainBeginOutput(es); // ��ʼ��¼���
            ExplainQueryText(es, queryDesc); // ��¼��ѯ�ı�
            ExplainPrintPlan(es, queryDesc); // ��¼ִ�мƻ�
            if (es->analyze && auto_explain_log_triggers)
                ExplainPrintTriggers(es, queryDesc); // ��¼��������Ϣ
            ExplainEndOutput(es); // ������¼���

            /* Remove last line break */
            if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
                es->str->data[--es->str->len] = '\0'; // �Ƴ����һ�����з�

            /* Fix JSON to output an object */
            if (auto_explain_log_format == EXPLAIN_FORMAT_JSON)
            {
                es->str->data[0] = '{';
                es->str->data[es->str->len - 1] = '}';
            }

            /*
             * Note: we rely on the existing logging of context or
             * debug_query_string to identify just which statement is being
             * reported.  This isn't ideal but trying to do it here would
             * often result in duplication.
             */
            ereport(LOG,
                (errmsg("duration: %.3f ms  plan:\n%s",
                    msec, es->str->data),
                    errhidestmt(true))); // ��¼ִ�мƻ�����־��

            pfree(es->str->data); // �ͷ� ExplainState �ṹ���е��ַ�������
        }
    }

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc); // ����ԭʼ ExecutorEnd ����
    else
        standard_ExecutorEnd(queryDesc); // ʹ�ñ�׼ ExecutorEnd ����
}
