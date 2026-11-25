#include "postgres.h"

#include <limits.h>

#include "commands/explain.h"
#include "executor/instrument.h"
#include "utils/guc.h"


PG_MODULE_MAGIC;

void
_PG_init(void);


void _PG_fini(void);

static void time_monitor_ExecutorStart(QueryDesc *queryDesc, int eflags);

static void time_monitor_ExecutorEnd(QueryDesc *queryDesc);


static ExecutorStart_hook_type prev_ExecutorStart = NULL;

static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static int   scope_of_inquiry = 0;






void
_PG_init(void)
{
    DefineCustomIntVariable("time_monitor.scope_of_inquiry",
                                "The time frame of the monitoring",
                                NULL,
                                &scope_of_inquiry,
                                -1,
                                -1, INT_MAX / 1000,
                                PGC_SUSET,
                                GUC_UNIT_MS,
                                NULL,
                                NULL,
                                NULL);
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = time_monitor_ExecutorStart;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = time_monitor_ExecutorEnd;
}





static void
time_monitor_ExecutorStart(QueryDesc *queryDesc, int eflags)
{

    if (prev_ExecutorStart){
        prev_ExecutorStart(queryDesc, eflags);
    }
    else
        standard_ExecutorStart(queryDesc, eflags);

    if (queryDesc->totaltime == NULL) {
        queryDesc->totaltime = (Instrumentation *) palloc0(sizeof(Instrumentation));
    }


}



static void
time_monitor_ExecutorEnd(QueryDesc *queryDesc)
{
    if( queryDesc->totaltime-> total * 1000.0 > scope_of_inquiry && scope_of_inquiry != -1 ){

       if (queryDesc->totaltime != NULL && queryDesc->sourceText != NULL)
       {
           ereport(LOG,
                   (errmsg("time: %.3f ms  sourceText: %s ",
                           queryDesc->totaltime->total * 1000.0, queryDesc->sourceText),
                   errhidestmt(true)));
       }
       else
       {
           ereport(LOG,
                   (errmsg("time_monitor: queryDesc->totaltime 或 queryDesc->sourceText 是 NULL")));
       }
    }

    if (prev_ExecutorEnd){
        prev_ExecutorEnd(queryDesc);
    }
    else
        standard_ExecutorEnd(queryDesc);
}

void
_PG_fini(void)
{
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorEnd_hook = prev_ExecutorEnd;
}
