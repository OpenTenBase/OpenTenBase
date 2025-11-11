#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include <time.h>
#include <stdio.h>

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static long long start_time = 0;
static long long end_time = 0;
static long long total_time = 0;

void _PG_fini(void);
void _PG_init(void);
static void check_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void check_ExecutorEnd(QueryDesc *queryDesc);
void
_PG_init(void)
{
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = check_ExecutorStart;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = check_ExecutorEnd;

}

static void
check_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    start_time = time(NULL);

    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

}



static void
check_ExecutorEnd(QueryDesc *queryDesc)
{
   end_time = time(NULL);
   total_time =  end_time - start_time;
   if( total_time > 10){
       FILE *fptr;
       fptr = fopen("log.txt", "a");
       time_t timep;
       time (&timep);
       fprintf(fptr, "The current time：%s,execution time: %lld, Sql: %s\n", asctime( gmtime(&timep)), total_time, queryDesc->sourceText);
       fclose(fptr);
   }

   if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
   else
           standard_ExecutorEnd(queryDesc);
}


void
_PG_fini(void)
{
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorEnd_hook = prev_ExecutorEnd;
}
