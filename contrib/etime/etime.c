#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;
void _PG_init(void);
void _PG_fini(void);

/**
 * GUC variable for etime.min_value
 *
 * Administrators can choose the minimun's threshold of the query
 * execution time(us) that the etime plugin records;
 *
 */
int min_value = 0;

static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

/*
 * An ETimeEvent records the content and execution time of a query
 */
typedef struct
{
	char *queryText;						/* sql text */
	TimestampTz startTime;			/* start time the query executing */
	MemoryContext queryContext; /* Context for query tracking rows */
} ETimeEvent;

/*
 * A simple FIFO queue to keep track of the current stack of etime events.
 */
typedef struct ETimeEventStackItem
{
	struct ETimeEventStackItem *next;

	ETimeEvent eTimeEvent;

	MemoryContext context;
	MemoryContextCallback contextCallback;
} ETimeEventStackItem;

ETimeEventStackItem *eTimeEventStack = NULL;

/*
 * Respond to callbacks registered with MemoryContextRegisterResetCallback().
 * Removes the event(s) off the stack that have become obsolete once the
 * MemoryContext has been freed.  The callback should always be freeing the top
 * of the stack, but the code is tolerant of out-of-order callbacks.
 */
static void
stack_free(void *stackFree)
{
	ETimeEventStackItem *nextItem = eTimeEventStack;

	/* Only process if the stack contains items */
	while (nextItem != NULL)
	{
		/* Check if this item matches the item to be freed */
		if (nextItem == (ETimeEventStackItem *)stackFree)
		{
			/* Move top of stack to the item after the freed item */
			eTimeEventStack = nextItem->next;
			return;
		}

		nextItem = nextItem->next;
	}
}

/*
 * Push a new etime event onto the stack and create a new memory context to
 * store it.
 */
static ETimeEventStackItem *
stack_push()
{
	MemoryContext context;
	MemoryContext contextOld;
	ETimeEventStackItem *stackItem;

	/*
	 * Create a new memory context to contain the stack item.  This will be
	 * free'd on stack_pop, or by our callback when the parent context is
	 * destroyed.
	 */
	context = AllocSetContextCreate(CurrentMemoryContext,
																	"etime stack context",
																	ALLOCSET_DEFAULT_SIZES);

	/* Save the old context to switch back to at the end */
	contextOld = MemoryContextSwitchTo(context);

	/* Create our new stack item in our context */
	stackItem = palloc0(sizeof(ETimeEventStackItem));
	stackItem->context = context;

	/*
	 * Setup a callback in case an error happens.  stack_free() will truncate
	 * the stack at this item.
	 */
	stackItem->contextCallback.func = stack_free;
	stackItem->contextCallback.arg = (void *)stackItem;
	MemoryContextRegisterResetCallback(context,
																		 &stackItem->contextCallback);

	/* Push new item onto the stack */
	if (eTimeEventStack != NULL)
		stackItem->next = eTimeEventStack;
	else
		stackItem->next = NULL;

	eTimeEventStack = stackItem;

	MemoryContextSwitchTo(contextOld);

	return stackItem;
}

/**
 * Hook ExecutorStart to get the query text and its execution time
 */
static void
my_ExecutorStart_hook(QueryDesc *queryDesc, int eflags)
{
	ETimeEventStackItem *stackItem = NULL;

	/* Push the etime event onto the stack */
	stackItem = stack_push();

	stackItem->eTimeEvent.startTime = GetCurrentTimestamp();

	/* Call the previous hook or standard function */
	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	MemoryContextSetParent(stackItem->context,
												 queryDesc->estate->es_query_cxt);
	stackItem->eTimeEvent.queryContext = queryDesc->estate->es_query_cxt;
}

/*
 * Find an item on the stack by the specified query memory context.
 */
static ETimeEventStackItem *
stack_find_context(MemoryContext findContext)
{
	ETimeEventStackItem *nextItem = eTimeEventStack;

	/* Look through the stack for the stack entry by query memory context */
	while (nextItem != NULL)
	{
		if (nextItem->eTimeEvent.queryContext == findContext)
			break;

		nextItem = nextItem->next;
	}

	return nextItem;
}

/*
 * Hook ExecutorEnd to get rows processed by the current statement.
 */
static void
my_ExecutorEnd_hook(QueryDesc *queryDesc)
{
	ETimeEventStackItem *stackItem = NULL;
	TimestampTz endTime = GetCurrentTimestamp();
	long secs;
	int microsecs;

	/* Find an item from the stack by the query memory context */
	stackItem = stack_find_context(queryDesc->estate->es_query_cxt);
	if (stackItem != NULL)
	{
		TimestampDifference(stackItem->eTimeEvent.startTime, endTime, &secs, &microsecs);
		if (secs * USECS_PER_SEC + microsecs > min_value)
			elog(LOG, "SQL: (%s) Execution Time: %lds, %dus",
					 queryDesc->sourceText, secs, microsecs);
	}

	/* Call the previous hook or standard function */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

void _PG_init(void)
{
	/* Define etime.min_value */
	DefineCustomIntVariable(
			"etime.min_value",

			"Specifies, in us, the minimum query execution time of the etime "
			"plugin record.",

			NULL,
			&min_value,
			0,
			0,
			(1 << 30) - 1,
			PGC_SUSET,
			GUC_NOT_IN_SAMPLE,
			NULL, NULL, NULL);

	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = my_ExecutorStart_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = my_ExecutorEnd_hook;
}

void _PG_fini(void)
{
	ExecutorStart_hook = prev_ExecutorStart_hook;
	ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
/*
psql -h localhost -p 30004 -d testdb -U opentenbase

create default node group default_group  with (dn001,dn002);
create sharding group to group default_group;
create database testdb;

create extension etime;
load '$libdir/etime';

create table foo(id bigint, str text) distribute by shard(id);
insert into foo values(1, 'tencent'), (2, 'shenzhen');
select * from foo;
*/
