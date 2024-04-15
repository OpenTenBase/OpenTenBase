#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* Prevents name mangling of the module functions */
PG_MODULE_MAGIC;

/* Define the minimum execution time in microseconds to be considered a slow query */
#define SLOW_QUERY_MIN_TIME 0

/* Global variable to store the minimum slow query threshold */
int slowquery_min_time = SLOW_QUERY_MIN_TIME;

/* Function pointers for hooks into the query execution lifecycle */
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/* A structure to hold information about a query and its execution time */
typedef struct {
    char* query;                            /* The SQL query text */
    TimestampTz startTimestamp;               /* The start timestamp of the query execution */
} SlowQueryRecord;

/* A structure to manage a stack of queries that are currently being executed */
typedef struct {
    struct SlowQueryStackItem* next;        /* Pointer to the next item in the stack */
    SlowQueryRecord record;                  /* The record of the query and its start time */
    MemoryContext context;                   /* Memory context in which this record is stored */
} SlowQueryStackItem;

/* Global stack of currently executing queries */
static SlowQueryStackItem* slowQueryStack = NULL;

/* Function prototypes for local helper functions */
static void slowquery_stack_free(SlowQueryStackItem* item);
static SlowQueryStackItem* slowquery_stack_push(void);
static SlowQueryStackItem* slowquery_stack_find(MemoryContext context);

/* GUC variable setup function */
void _PG_init(void) {
    /* Define the GUC variable for the minimum slow query threshold */
    DefineCustomIntVariable(
        "slowquery.min_time",
        "Minimum query execution time in microseconds to be considered slow.",
        NULL,
        &slowquery_min_time,
        0,
        0,
        (1 << 30) - 1,
        PGC_SUSET,
        GUC_NOT_IN_SAMPLE,
        NULL, NULL, NULL);

    /* Set up the hooks for the query execution lifecycle */
    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = slowquery_ExecutorStart_hook;

    prev_ExecutorEnd_hook = ExecutorEnd_hook;
    ExecutorEnd_hook = slowquery_ExecutorEnd_hook;
}

/* GUC variable cleanup function */
void _PG_fini(void) {
    /* Clean up the hooks when the module is unloaded */
    ExecutorStart_hook = prev_ExecutorStart_hook;
    ExecutorEnd_hook = prev_ExecutorEnd_hook;
}

/* ExecutorStart hook function to record the start of a query */
static void
slowquery_ExecutorStart_hook(QueryDesc* queryDesc, int eflags) {
    MemoryContext oldContext = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
    SlowQueryStackItem* item = slowquery_stack_push();
    item->record.query = pstrdup(queryDesc->sourceText); /* Duplicate the query text */
    item->record.startTimestamp = GetCurrentTimestamp(); /* Record the start time */
    MemoryContextSwitchTo(oldContext); /* Restore the previous memory context */
}

/* ExecutorEnd hook function to check if a query was slow and log it */
static void
slowquery_ExecutorEnd_hook(QueryDesc* queryDesc) {
    SlowQueryStackItem* item = slowquery_stack_find(queryDesc->estate->es_query_cxt);
    if (item != NULL) {
        TimestampTz endTimestamp = GetCurrentTimestamp(); /* Get the current timestamp */
        int64 msecsElapsed = DatumGetIntervalP(
            TimestampDifference(item->record.startTimestamp, endTimestamp)
        )->miksec; /* Calculate the elapsed time in milliseconds */
        if (msecsElapsed > slowquery_min_time) {
            ereport(LOG,
                (errmsg("Slow Query: %s executed in %ld ms",
                    item->record.query, msecsElapsed)));
        }
        pfree(item->record.query); /* Free the duplicated query text */
        slowquery_stack_free(item); /* Remove the item from the stack and clean up */
    }
}

/* Helper function to free a slow query stack item */
static void
slowquery_stack_free(SlowQueryStackItem* item) {
    /* Remove the item from the stack and clean up the memory context */
    SlowQueryStackItem* current = slowQueryStack;
    SlowQueryStackItem* prev = NULL;
    while (current != NULL) {
        if (current == item) {
            if (prev != NULL) {
                prev->next = current->next;
            }
            else {
                slowQueryStack = current->next;
            }
            break;
        }
        prev = current;
        current = current->next;
    }
    MemoryContextDelete(item->context); /* Delete the memory context */
    free(item); /* Free the stack item itself */
}

/* Helper function to push a new slow query record onto the stack */
static SlowQueryStackItem*
slowquery_stack_push(void) {
    SlowQueryStackItem* item = (SlowQueryStackItem*)palloc0(sizeof(SlowQueryStackItem));
    item->context = AllocSetContextCreate(CurrentMemoryContext, "slowQueryContext", ALLOCSET_DEFAULT_SIZES);
    item->next = slowQueryStack;
    slowQueryStack = item;
    return item;
}

/* Helper function to find a slow query record on the stack by its memory context */
static SlowQueryStackItem*
slowquery_stack_find(MemoryContext context) {
    SlowQueryStackItem* item = slowQueryStack;
    while (item != NULL) {
        if (item->context == context) {
            return item;
        }
        item = item->next;
    }
    return NULL;
}