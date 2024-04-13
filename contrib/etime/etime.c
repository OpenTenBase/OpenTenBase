#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "executor/spi.h"
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

/*
 * table'name where info will be written.
 * The table must be have a schema like this:
 * ================================================
 * | create table etime_t(sql text, time bigint); |
 * ================================================
 */
static char *tablename = NULL;

/* max bytes of query writen including other info. */
static int maxSqlSize = 1024;

static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

static int internalStatement = 0;
static int64 stackTotal = 0;

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

	int64 stackId;

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
	while (nextItem != NULL) {
		/* Check if this item matches the item to be freed */
		if (nextItem == (ETimeEventStackItem *)stackFree) {
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
  stackItem->stackId = ++stackTotal;

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

static void
stack_pop(int64 stackId) {
	/* Make sure what we want to delete is at the top of the stack */
	if (eTimeEventStack != NULL && eTimeEventStack->stackId == stackId) {
		MemoryContextDelete(eTimeEventStack->context);
	} else {
		elog(WARNING,
				"etime stack item " INT64_FORMAT " not found on top - cannot pop",
				stackId);
	}
}

/*
 * repeat twice in str for ch and store it in new_str.
 */
char *
repeatChar2(const char *str, char *new_str, char ch) {
	while (*str != 0) {
		*new_str = *str;
		if(*str == ch) {
			*++new_str = ch;
		}
		++str;
		++new_str;
	}
	return new_str;
}

/**
 * Hook ExecutorStart to get the query text and its execution time
 */
static void
my_ExecutorStart_hook(QueryDesc *queryDesc, int eflags)
{
	// dont record query time due to no target table
	if(tablename == NULL) {
		goto hook;
	}

	ETimeEventStackItem *stackItem = NULL;

	if (internalStatement == 0) {
		/* Push the etime event onto the stack */
		stackItem = stack_push();

		stackItem->eTimeEvent.startTime = GetCurrentTimestamp();
	}

	/* Call the previous hook or standard function */
hook:
	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (stackItem) {
		MemoryContextSetParent(stackItem->context,
													 queryDesc->estate->es_query_cxt);
		stackItem->eTimeEvent.queryContext = queryDesc->estate->es_query_cxt;
	}
}

/*
 * Find an item on the stack by the specified query memory context.
 */
static ETimeEventStackItem *
stack_find_context(MemoryContext findContext)
{
	ETimeEventStackItem *nextItem = eTimeEventStack;

	/* Look through the stack for the stack entry by query memory context */
	while (nextItem != NULL) {
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
	MemoryContext contextOld;

	/* Find an item from the stack by the query memory context */
	stackItem = stack_find_context(queryDesc->estate->es_query_cxt);
	if (stackItem != NULL) {
		internalStatement = 1;
		contextOld = MemoryContextSwitchTo(stackItem->context);
		TimestampDifference(stackItem->eTimeEvent.startTime, endTime, &secs,
												&microsecs);
		if (secs * USECS_PER_SEC + microsecs > min_value) {
			PG_TRY();
			{
				if (SPI_connect() != SPI_OK_CONNECT) {
					elog(WARNING, "etime plugin SPI_connect failed");
					goto clear_stack_item;
				}

				// construct a sql to insert record in etime_t table
				char sql[maxSqlSize];
				int sz = snprintf(sql, maxSqlSize, "insert into %s values(", tablename);
				int st_sz = repeatChar2(queryDesc->sourceText, sql + sz + 1, '\'') -
										(sql + sz + 1);
				sql[sz] = '\'';
				if (sz + 1 + st_sz >= maxSqlSize) {
					elog(WARNING, "etime plugin try to execute [%s], but it is too long",
							 sql);
					goto clear_conn;
				}
				int post_sz = snprintf(sql + sz + 1 + st_sz, maxSqlSize - sz - st_sz,
															 "\',%ld)", secs * USECS_PER_SEC + microsecs);
				if (sz + 1 + st_sz + post_sz >= maxSqlSize) {
					elog(WARNING, "etime plugin try to execute [%s], but it is too long",
							 sql);
					goto clear_conn;
				}
				sql[sz + 1 + st_sz + post_sz] = 0;

				// int ret = SPI_execute("insert into etime_t values(123)", false, 0);
				int ret = SPI_execute(sql, false, 0);
				if (ret != SPI_OK_INSERT) {
					elog(WARNING, "etime plugin SPI_execute [%s] failed", sql);
					goto clear_conn;
				}
			}
			PG_CATCH();
			{
				elog(WARNING, "etime plugin come across some wrong");
				goto clear_conn;
			}
			PG_END_TRY();

			// clear connection
		clear_conn:
			if (SPI_finish() != SPI_OK_FINISH) {
				elog(WARNING, "etime plugin SPI_finish failed");
			}
		}
		// clear stack item
	clear_stack_item:
		MemoryContextSwitchTo(contextOld);
		stack_pop(stackItem->stackId);
		internalStatement = 0;
	}

	/* Call the previous hook or standard function */
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

void _PG_init(void)
{
	/* Be sure we do initialization only once */
	static bool inited = false;

	if (inited)
		return;

	/* Define etime.min_value */
	DefineCustomIntVariable(
			"etime.min_value",

			"Specifies, in us, the minimum query execution time of the etime "
			"plugin record.",

			NULL,
			&min_value,
			0,
			0,
			(1 << 31) - 1,
			PGC_SUSET,
			GUC_NOT_IN_SAMPLE,
			NULL, NULL, NULL);

	/* Define etime.tablename */
	DefineCustomStringVariable(
			"etime.tablename",
			"Specifies, is us, the table'name which store record recorded.",
			NULL,
			&tablename,
			NULL,
			PGC_SUSET,
			0,
			NULL, NULL, NULL);

	/* Define etime.maxSqlSize */
	DefineCustomIntVariable(
			"etime.max_sql_size",

			"Specifies, in us, max bytes of query writen including other info.",

			NULL,
			&maxSqlSize,
			1024,
			0,
			(1 << 31) - 1,
			PGC_SUSET,
			GUC_NOT_IN_SAMPLE,
			NULL, NULL, NULL);

	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = my_ExecutorStart_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = my_ExecutorEnd_hook;

	inited = true;
}

void _PG_fini(void)
{
	ExecutorStart_hook = prev_ExecutorStart_hook;
	ExecutorEnd_hook = prev_ExecutorEnd_hook;
}
