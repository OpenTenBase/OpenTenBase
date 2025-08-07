/*-------------------------------------------------------------------------
 *
 * memtrack.c
 * Used to implement context memory inflation tracking function,
 * supports tracking at three levels: node/session/context.
 * Used in conjunction with parameters track_memory_mode and track_context_name.
 * 
 * Currently implemented system functions include:
 *     pg_stat_node_memory_detail
 *     pg_stat_session_memory_detail
 *     pg_stat_context_memory_detail
 *     pg_log_backend_memory_contexts.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "storage/proc.h"
#include "storage/lock.h"
#include "optimizer/planner.h"
#include "pgxc/execRemote.h"
#include "pgstat.h"
#include "foreign/foreign.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"
#include "access/hash.h"

#define MAX_TRACK_CXT_NUM 16

/* used for track memory alloc info */
MemoryContext        track_context;
HTAB *               track_cxtlist_hash;
HTAB *               mem_cxt_stats_hash;
int                  track_memory_mode = TRACK_MEM_OFF;
char *               track_context_name = NULL;
bool                 no_track_meminfos = false;
__thread bool        track_memory_inited = false;
bool                 track_all_memcxt = true;

extern MemoryContext CurrentDynaHashCxt;
extern HTAB *        CreateTrackMemAllocDetailHash(void);
extern HTAB *        CreateTrackMemCxtDetailHash(void);
static void          CreateTrackCxtlistHash(void);
static void          CreateMemcxtStatsHash(void);
static void          CreateMemAllocEleHash(TrackMemCxtEle *track_cxt_info);
static void          CopyContextName(char *source);
static void          ResetTrackMemoryStructure(void);

/* Hash for managing the status of memory context dump. */
static HTAB *mcxtdumpHash = NULL;

bool
TrackMemInit(void)
{
	MemoryContext oldContext;

	track_memory_inited = false;

	if (track_context == NULL)
	{
		/* init memory context for tracking memory alloc info */
		track_context =
			AllocSetContextCreate(TopMemoryContext, "TrackMemoryContext", ALLOCSET_DEFAULT_SIZES);
	}
	else
		ResetTrackMemoryStructure();

	oldContext = MemoryContextSwitchTo(track_context);

	CreateTrackCxtlistHash();
	CreateMemcxtStatsHash();

	track_memory_inited = true;
	(void) MemoryContextSwitchTo(oldContext);
	return true;
}

static uint32
TrackMemCxtEleHashFunc(const void *key, Size keysize)
{
	uint32 hashval;

	hashval = DatumGetUInt32(hash_any(key, keysize));

	return hashval;
}

static void
CreateMemcxtStatsHash(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));

	hctl.keysize = sizeof(MemoryContext);
	hctl.entrysize = sizeof(TrackMemCxtEle);
	hctl.hash = TrackMemCxtEleHashFunc;
	hctl.hcxt = track_context;

	mem_cxt_stats_hash =
		hash_create("Track memcxt hash", 1000, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
}

#ifdef MEMORY_ALLOC_TRACKING
static uint32
MemAllocEleKeyHashFunc(const void *key, Size keysize)
{
	MemAllocEleKey *val = (MemAllocEleKey *) key;
	uint32          val2 = string_hash(val->file, NAMEDATALEN);
	uint32          val3 = uint32_hash(&val->line, sizeof(int));
	return (val2 + val3);
}

static int
MemAllocEleKeyHashMatch(const void *key1, const void *key2, Size keysize)
{
	MemAllocEleKey *val1 = (MemAllocEleKey *) key1;
	MemAllocEleKey *val2 = (MemAllocEleKey *) key2;

	if (strcmp(val1->file, val2->file) != 0)
	{
		return 1;
	}
	if (val1->line != val2->line)
	{
		return 1;
	}

	return 0;
}
#endif

static void
CreateMemAllocEleHash(TrackMemCxtEle *track_cxt_info)
{
#ifdef MEMORY_ALLOC_TRACKING
	/* create the hash table for tracking memory alloc info */
	HASHCTL hctl;

	NO_TRACK_MEMINFOS();

	memset(&hctl, 0, sizeof(hctl));

	hctl.keysize = sizeof(MemAllocEleKey);
	hctl.entrysize = sizeof(MemAllocEle);
	hctl.hash = MemAllocEleKeyHashFunc;
	hctl.match = MemAllocEleKeyHashMatch;
	hctl.hcxt = track_context;

	track_cxt_info->mem_alloc_stats_hash =
		(void *) hash_create("Track memalloc hash",
	                         1000,
	                         &hctl,
	                         HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	RESUME_TRACK_MEMINFOS();
#else
	return;
#endif
}

static void
CreateTrackCxtlistHash(void)
{
	HASHCTL hctl;

	if (track_cxtlist_hash != NULL)
		return;

	memset(&hctl, 0, sizeof(hctl));

	hctl.keysize = NAMEDATALEN;
	hctl.entrysize = NAMEDATALEN;
	hctl.hcxt = TopMemoryContext;

	track_cxtlist_hash =
		hash_create("Track MemoryContext hash", MAX_TRACK_CXT_NUM, &hctl, HASH_ELEM | HASH_CONTEXT);
}

void
CopyContextName(char *source)
{
	char *tmp;
	char  name[NAMEDATALEN] = {0};

	/* ignore space before and end */
	tmp = TrimStr(source);

	if (strcmp(tmp, "ALL") == 0)
	{
		track_all_memcxt = true;
		return;
	}

	strcpy(name, tmp);
	if (strlen(tmp) > NAMEDATALEN - 1)
	{
		ereport(WARNING, (errmsg("the context_name(%s) will be cut off to %s", tmp, name)));
	}
	(void) hash_search(track_cxtlist_hash, name, HASH_ENTER, NULL);
}

bool
ParseTrackCxtlist(const char *cxt_name)
{
	int   len = strlen(cxt_name) + 1;
	char *origin_name = (char *) palloc(len);
	int   count = 0;
	char *tmp = NULL;

	ResetTrackMemoryStructure();

	if (!cxt_name || cxt_name[0] == '\0')
		return false;

	strcpy(origin_name, cxt_name);
	while (origin_name != NULL)
	{
		if (track_all_memcxt)
			return true;

		if (count >= MAX_TRACK_CXT_NUM)
		{
			ereport(WARNING, (errmsg("The number of memoryContext could not exceed 16.")));
			return false;
		}
		tmp = strrchr(origin_name, ',');
		if (tmp != NULL)
		{
			tmp[0] = '\0';
			CopyContextName(tmp + 1);
		}
		else
		{
			CopyContextName(origin_name);
			origin_name = NULL;
		}
		count++;
	}

	return true;
}

static void
CleanTrackCxtlistHash(void)
{
	HASH_SEQ_STATUS hash_seq;
	char *          entry = NULL;

	CreateTrackCxtlistHash();

	/* get the sum of same alloc location */
	hash_seq_init(&hash_seq, track_cxtlist_hash);
	while ((entry = (char *) hash_seq_search(&hash_seq)) != NULL)
	{
		if (hash_search(track_cxtlist_hash, entry, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted HASH_REMOVE");
	}
}

static void
CleanMemCxtStatsHash()
{
	HASH_SEQ_STATUS hash_seq;
	TrackMemCxtEle *entry = NULL;
	uint64          key;

	hash_seq_init(&hash_seq, mem_cxt_stats_hash);
	while ((entry = (TrackMemCxtEle *) hash_seq_search(&hash_seq)) != NULL)
	{
#ifdef MEMORY_ALLOC_TRACKING
		HTAB * mem_alloc_stats_hash = (HTAB *) entry->mem_alloc_stats_hash;
#endif
		key = (uint64) entry->key;

		if (hash_search(mem_cxt_stats_hash, &key, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted HASH_REMOVE");

#ifdef MEMORY_ALLOC_TRACKING
		hash_destroy(mem_alloc_stats_hash);
#endif
	}
}

static void
ResetTrackMemoryStructure(void)
{
	if (!track_memory_inited)
		return;

	track_all_memcxt = false;
	CleanTrackCxtlistHash();
	CleanMemCxtStatsHash();
	RESUME_TRACK_MEMINFOS();
}

bool
MemCxtShouldTrack(const MemoryContext context)
{
	bool        found = false;
	const char *name = context->ident ? context->ident : context->name;

	if (!(track_memory_inited && track_memory_mode == TRACK_MEM_CONTEXT &&
	      (unlikely(strncmp(name, "Track memalloc hash", NAMEDATALEN) != 0)) &&
	      (unlikely(strncmp(name, "Track memcxt hash", NAMEDATALEN) != 0)) &&
	      (unlikely(strncmp(name, "TrackMemoryContext", NAMEDATALEN) != 0))))
		return false;

	if (track_all_memcxt)
		return true;

	/* Check is memory context in track hash table */
	(void) hash_search(track_cxtlist_hash, name, HASH_FIND, &found);
	if (found)
		return true;

	return false;
}

void
InsertTrackMemAllocInfo(const void *pointer, const MemoryContext context, const char *file,
                        int line, Size size)
{
	TrackMemCxtEle *entry = NULL;
	uint64          key = (uint64) context;
	bool            found = false;
	MemoryContext   old_dyncontext;

	if (no_track_meminfos)
	{
		return;
	}

	old_dyncontext = get_current_dynahashcxt();

	entry = (TrackMemCxtEle *) hash_search(mem_cxt_stats_hash, &key, HASH_ENTER, &found);
	if (entry)
	{
#ifdef MEMORY_ALLOC_TRACKING
		MemAllocEle *entry1 = NULL;
		MemAllocEleKey  key1 = {file, line};
#endif

		if (!found)
		{
			CreateMemAllocEleHash(entry);
		}

#ifdef MEMORY_ALLOC_TRACKING
		entry1 = (MemAllocEle *)
			hash_search((HTAB *) entry->mem_alloc_stats_hash, &key1, HASH_ENTER, &found);
		if (entry1)
		{
			if (found)
			{
				entry1->total_size += size;
			}
			else
			{
				entry1->total_size = size;
			}
		}
		else
		{
			elog(ERROR, "InsertTrackMemAllocInfo: HASH_ENTER failed");
		}
#endif
	}
	else
	{
		elog(ERROR, "InsertTrackMemAllocInfo: HASH_ENTER failed");
	}

	set_current_dynahashcxt(old_dyncontext);
}

void
RemoveTrackMemAllocInfo(const void *pointer, const MemoryContext context, const char *file,
                        int line, Size size)
{
	uint64          key1 = (uint64) context;
	TrackMemCxtEle *entry = NULL;
	bool            found = false;

	entry = (TrackMemCxtEle *) hash_search(mem_cxt_stats_hash, &key1, HASH_FIND, &found);
	if (found && entry != NULL)
	{
#ifdef MEMORY_ALLOC_TRACKING
		MemAllocEle *entry1 = NULL;
		MemAllocEleKey  key = {file, line};
#endif

#ifdef MEMORY_ALLOC_TRACKING
		entry1 = (MemAllocEle *)
			hash_search((HTAB *) entry->mem_alloc_stats_hash, &key, HASH_FIND, NULL);
		if (found && entry1 != NULL)
		{
			Size oldsize = entry1->total_size;

			if (oldsize > size)
			{
				entry1->total_size -= size;
			}
			else
			{
				if (hash_search((HTAB *) entry->mem_alloc_stats_hash, &key, HASH_REMOVE, NULL) ==
				    NULL)
					elog(ERROR, "hash table corrupted HASH_REMOVE");
			}
		}
#endif
	}
}

void
RemoveTrackMemCxt(const MemoryContext context)
{
	uint64          key = (uint64) context;
	TrackMemCxtEle *entry = NULL;
	bool            found = false;

	entry = (TrackMemCxtEle *) hash_search(mem_cxt_stats_hash, &key, HASH_FIND, &found);
	if (found && entry != NULL)
	{
#ifdef MEMORY_ALLOC_TRACKING
		HTAB *mem_alloc_stats_hash = (HTAB *) entry->mem_alloc_stats_hash;
#endif

		if (hash_search(mem_cxt_stats_hash, &key, HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "hash table corrupted HASH_REMOVE");

#ifdef MEMORY_ALLOC_TRACKING
		hash_destroy(mem_alloc_stats_hash);
#endif
	}
}

static uint32
MemAllocDetailKeyHashFunc(const void *key, Size keysize)
{
	MemAllocDetailKey *val = (MemAllocDetailKey *) key;
	uint32             val1 = string_hash(val->cxt_name, NAMEDATALEN);
	uint32             val2 = string_hash(val->file, NAMEDATALEN);
	uint32             val3 = uint32_hash(&val->line, sizeof(int));
	return (val1 & (val2 + val3));
}

static int
MemAllocDetailKeyHashMatch(const void *key1, const void *key2, Size keysize)
{
	MemAllocDetailKey *val1 = (MemAllocDetailKey *) key1;
	MemAllocDetailKey *val2 = (MemAllocDetailKey *) key2;

	if (strcmp(val1->cxt_name, val2->cxt_name) != 0)
	{
		return 1;
	}
	if (strcmp(val1->file, val2->file) != 0)
	{
		return 1;
	}
	if (val1->line != val2->line)
	{
		return 1;
	}

	return 0;
}

HTAB *
CreateTrackMemAllocDetailHash(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));

	hctl.keysize = sizeof(MemAllocDetailKey);
	hctl.entrysize = sizeof(MemAllocDetail);
	hctl.hash = MemAllocDetailKeyHashFunc;
	hctl.match = MemAllocDetailKeyHashMatch;

	return hash_create("Track MemoryAlloc Detail hash",
	                   10000,
	                   &hctl,
	                   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

HTAB *
CreateTrackMemCxtDetailHash(void)
{
	HASHCTL hctl;

	memset(&hctl, 0, sizeof(hctl));

	hctl.keysize = NAMEDATALEN;
	hctl.entrysize = sizeof(MemCxtDetail);

	return hash_create("Track MemoryCxt Detail hash", 1000, &hctl, HASH_ELEM);
}

static void
AssignMemCxtDetail(MemCxtDetail *dst, MemCxtDetail *src)
{
	strncpy(dst->cxt_name, src->cxt_name, NAMEDATALEN - 1);
	dst->total_size = src->total_size;
}

static int
CmpMemCxtInfo(const void *a, const void *b)
{
	const MemCxtDetail *tmpa = (MemCxtDetail *) a;
	const MemCxtDetail *tmpb = (MemCxtDetail *) b;

	if (tmpa->total_size < tmpb->total_size)
		return -1;
	else if (tmpa->total_size > tmpb->total_size)
		return 1;
	else
		return 0;
}

void
GetTopnMemcxtEles(MemCxtDetail *elem, MemCxtDetail *arr, int *arr_len)
{
	int n = *arr_len;

	if (n < TOP50_MEMDETAIL_LEN)
	{
		AssignMemCxtDetail(&arr[n++], elem);

		if (n == TOP50_MEMDETAIL_LEN)
			qsort(arr, TOP50_MEMDETAIL_LEN, sizeof(MemCxtDetail), CmpMemCxtInfo);
	}
	else if (elem->total_size > arr[0].total_size)
	{
		AssignMemCxtDetail(&arr[0], elem);
		qsort(arr, TOP50_MEMDETAIL_LEN, sizeof(MemCxtDetail), CmpMemCxtInfo);
	}

	*arr_len = n;
}

/*
 * Algorithm to obtain top 50 data based on size ranking.
 */
static void
AssignMemAllocDetail(MemAllocDetail *dst, MemAllocDetail *src)
{
	dst->key.file = src->key.file;
	dst->key.line = src->key.line;
	dst->total_size = src->total_size;
	strncpy(dst->key.cxt_name, src->key.cxt_name, NAMEDATALEN - 1);
}

static int
CmpMemAllocInfo(const void *a, const void *b)
{
	MemAllocDetail *tmpa = (MemAllocDetail *) a;
	MemAllocDetail *tmpb = (MemAllocDetail *) b;

	if (tmpa->total_size < tmpb->total_size)
		return -1;
	else if (tmpa->total_size > tmpb->total_size)
		return 1;
	else
		return 0;
}

void
GetTopnMemAllocEles(MemAllocDetail *elem, MemAllocDetail *arr, int *arr_len)
{
	int n = *arr_len;

	if (n < TOP50_MEMDETAIL_LEN)
	{
		AssignMemAllocDetail(&arr[n++], elem);

		if (n == TOP50_MEMDETAIL_LEN)
			qsort(arr, TOP50_MEMDETAIL_LEN, sizeof(MemAllocDetail), CmpMemAllocInfo);
	}
	else if (elem->total_size > arr[0].total_size)
	{
		AssignMemAllocDetail(&arr[0], elem);
		qsort(arr, TOP50_MEMDETAIL_LEN, sizeof(MemAllocDetail), CmpMemAllocInfo);
	}

	*arr_len = n;
}

static Size
GetPssMemUsage(int pid)
{
#define BUFFER_SIZE 1024
	char  command[256];
	char  buffer[BUFFER_SIZE];
	FILE *fp;
	Size  pss;

	sprintf(command, "grep Pss /proc/%d/smaps | awk '{total+=$2}; END {print total}'", pid);
	fp = popen(command, "r");
	if (fp == NULL)
	{
		return -1;
	}

	fgets(buffer, BUFFER_SIZE, fp);
	pclose(fp);
	pss = (Size) atol(buffer);
	return pss;
}

Datum
pg_stat_pss_memory_usage(PG_FUNCTION_ARGS)
{
	int  pid = PG_ARGISNULL(0) ? -1 : PG_GETARG_INT32(0);
	Size result = GetPssMemUsage(pid);

	PG_RETURN_INT64(result);
}

Datum
pg_stat_session_memory_detail(PG_FUNCTION_ARGS)
{
#define STAT_SESSION_MEMORY_ELEMENT_NUMBER 4
	FuncCallContext *        funcctx = NULL;
	Datum                    result;
	MemoryContext            oldcontext;
	TupleDesc                tupledesc;
	HeapTuple                tuple;
	int                      i = 1;
	SessionMemCxtDetailList *node = NULL;

	if (track_memory_mode < TRACK_MEM_SESSION)
	{
		PG_RETURN_NULL();
	}

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(STAT_SESSION_MEMORY_ELEMENT_NUMBER, false);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "sessionid", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "context_name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i, "totalsize", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupledesc);

		funcctx->user_fctx = GetSessionMemCxtDetail();

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	node = (SessionMemCxtDetailList *) funcctx->user_fctx;
	if (node != NULL)
	{
		Datum                values[STAT_SESSION_MEMORY_ELEMENT_NUMBER];
		bool                 nulls[STAT_SESSION_MEMORY_ELEMENT_NUMBER] = {false};
		SessionMemCxtDetail *entry = &node->entry;

		i = 0;
		values[i++] = Int32GetDatum(entry->pid);
		values[i++] = CStringGetTextDatum(entry->sessionid);
		values[i++] = CStringGetTextDatum(entry->cxt_name);
		values[i] = Int64GetDatum(entry->total_size);

		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		funcctx->user_fctx = (void *) node->next;

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

Datum
pg_stat_context_memory_detail(PG_FUNCTION_ARGS)
{
#define STAT_CONTEXT_MEMORY_ELEMENT_NUMBER 6
	FuncCallContext *          funcctx = NULL;
	Datum                      result;
	MemoryContext              oldcontext;
	TupleDesc                  tupledesc;
	HeapTuple                  tuple;
	int                        i = 1;
	SessionMemAllocDetailList *node = NULL;

	if (track_memory_mode < TRACK_MEM_CONTEXT)
	{
		PG_RETURN_NULL();
	}

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(STAT_CONTEXT_MEMORY_ELEMENT_NUMBER, false);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "sessonid", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "context_name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "file", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "line", INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i, "size", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupledesc);

		funcctx->user_fctx = (void *) GetSessionMemAllocDetail();

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	node = (SessionMemAllocDetailList *) funcctx->user_fctx;
	if (node != NULL)
	{
		Datum                  values[STAT_CONTEXT_MEMORY_ELEMENT_NUMBER];
		bool                   nulls[STAT_CONTEXT_MEMORY_ELEMENT_NUMBER] = {false};
		SessionMemAllocDetail *entry = &node->entry;

		i = 0;
		values[i++] = Int32GetDatum(entry->pid);
		values[i++] = CStringGetTextDatum(entry->sessionid);
		values[i++] = CStringGetTextDatum(entry->entry.key.cxt_name);
		values[i++] = CStringGetTextDatum(entry->entry.key.file);
		values[i++] = Int32GetDatum(entry->entry.key.line);
		values[i] = Int64GetDatum(entry->entry.total_size);

		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		funcctx->user_fctx = (void *) node->next;

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

static void
PGStatClusterMemDetailInternal(FunctionCallInfo fcinfo, const char *query)
{
	ReturnSetInfo *  rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc        tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext    per_query_ctx;
	MemoryContext    oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("materialize mode required, but it is not "
		                "allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* switch to query's memory context to save results during execution */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* dispatch query to remote if needed */
	pg_stat_get_global_stat(query, true, rsinfo);
	pg_stat_get_global_stat(query, false, rsinfo);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
}

Datum
pg_stat_cluster_node_memory_detail(PG_FUNCTION_ARGS)
{
	const char *query = "select (select reset_val from pg_settings where name ='pgxc_node_name'), "
						"sum(pg_stat_pss_memory_usage(pid)) / 1024 size from pg_stat_activity";

	if (!IS_PGXC_COORDINATOR)
		elog(ERROR, "pg_stat_cluster_node_memory_detail only support on cn");

	PGStatClusterMemDetailInternal(fcinfo, query);
	return (Datum) 0;
}

Datum
pg_stat_cluster_session_memory_detail(PG_FUNCTION_ARGS)
{
	const char *query = "select (select reset_val from pg_settings where name ='pgxc_node_name'),* "
						"from pg_stat_session_memory_detail();";

	if (!IS_PGXC_COORDINATOR)
		elog(ERROR, "pg_stat_cluster_session_memory_detail only support on cn");

	if (track_memory_mode < TRACK_MEM_SESSION)
	{
		PG_RETURN_NULL();
	}

	PGStatClusterMemDetailInternal(fcinfo, query);
	return (Datum) 0;
}

Datum
pg_stat_cluster_context_memory_detail(PG_FUNCTION_ARGS)
{
	const char *query = "select (select reset_val from pg_settings where name ='pgxc_node_name'),* "
						"from pg_stat_context_memory_detail();";

	if (track_memory_mode < TRACK_MEM_CONTEXT)
	{
		PG_RETURN_NULL();
	}

	PGStatClusterMemDetailInternal(fcinfo, query);
	return (Datum) 0;
}

Datum
pg_get_backend_memory_detail(PG_FUNCTION_ARGS)
{
#define BACKEND_MEMORY_ELEMENT_NUMBER 3
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	int i;
	PgBackendMemStat *      memstat;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	CancelDBBackends(InvalidOid, PROCSIG_GET_MEMORY_DETAIL, false);
	pg_usleep(100000L); /* wait for 100 msec */

	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;

		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		
		if (!local_beentry)
			continue;

		beentry = &local_beentry->backendStatus;
		memstat = &beentry->memstat;
		
		for (i = 0; i < memstat->memory_detail_len; i++) {
			Datum		values[BACKEND_MEMORY_ELEMENT_NUMBER];
			bool		nulls[BACKEND_MEMORY_ELEMENT_NUMBER];
			
			MemSet(values, 0, sizeof(values));
			MemSet(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(beentry->st_procpid);
			values[1] = CStringGetTextDatum(beentry->memstat.memory_detail[i].cxt_name);
			values[2] = Int32GetDatum(beentry->memstat.memory_detail[i].total_size);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}
	
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

void
McxtDumpShmemInit(void)
{
	HASHCTL		info;
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(pid_t);
	info.entrysize = sizeof(mcxtdumpEntry);

	mcxtdumpHash = ShmemInitHash("mcxtdump hash",
									SHMEM_MEMCONTEXT_SIZE,
									SHMEM_MEMCONTEXT_SIZE,
									&info,
									HASH_ELEM | HASH_BLOBS);
}

static void
CheckContextNameValid(const char* ctx_name)
{
    if (ctx_name == NULL || strlen(ctx_name) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name of memory context: NULL or \"\"")));
    }

    if (strlen(ctx_name) >= NAMEDATALEN) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME),
            errmsg("The name of memory context is too long(>=%dbytes)", NAMEDATALEN)));
    }
}

Datum
pg_get_backend_memctx_detail(PG_FUNCTION_ARGS)
{
#define BACKEND_MEMORY_CONTEXT_ALLOC_DETAIL_ELEMENT_NUMBER 3
	int			num_backends = pgstat_fetch_stat_numbackends();
	int			curr_backend;
	PgBackendMemStat *      	memstat;
	FuncCallContext *          funcctx = NULL;
	Datum                      result;
	MemoryContext              oldcontext;
	TupleDesc                  tupledesc;
	HeapTuple                  tuple;
	int                        i = 1;
	char* 		ctx_name = TextDatumGetCString(PG_GETARG_TEXT_PP(1));
	CheckContextNameValid(ctx_name);

	if (SRF_IS_FIRSTCALL())
	{
		int			pid = PG_GETARG_INT32(0);
		PGPROC	   *proc;
		bool		found;
		mcxtdumpEntry  *entry;

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(BACKEND_MEMORY_CONTEXT_ALLOC_DETAIL_ELEMENT_NUMBER, false);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "file", TEXTOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i++, "line", INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) i, "size", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupledesc);
		
		proc = BackendPidGetProc(pid);
		if (proc == NULL)
			proc = AuxiliaryPidGetProc(pid);
		
		if (proc == NULL)
		{
			ereport(WARNING,
					(errmsg("PID %d is not a PostgreSQL server process. Returning empty set.", pid)));
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		LWLockAcquire(McxtDumpHashLock, LW_EXCLUSIVE);
		entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &pid, HASH_ENTER, &found);
		entry->src_pid = MyProcPid;
		entry->dst_pid = pid;
		strcpy(entry->cxt_name, ctx_name);
		LWLockRelease(McxtDumpHashLock);
		
		if (SendProcSignal(pid, PROCSIG_GET_MEMORY_CONTEXT_DETAIL, proc->backendId) < 0)
		{
			ereport(WARNING,
					(errmsg("could not send signal to process %d: %m. Returning empty set", pid)));
			MemoryContextSwitchTo(oldcontext);
			SRF_RETURN_DONE(funcctx);
		}

		/* 1-based index */
		for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
		{
			LocalPgBackendStatus *local_beentry;
			PgBackendStatus *beentry;
			
			local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
			
			if (!local_beentry)
				continue;
			
			beentry = &local_beentry->backendStatus;
			if (beentry->st_procpid != pid)
				continue;
			else {
				memstat = &beentry->memstat;
				funcctx->user_fctx = (void *) memstat;
				funcctx->max_calls = memstat->memctx_detail_len;
				break;
			}
		}

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum                  values[BACKEND_MEMORY_CONTEXT_ALLOC_DETAIL_ELEMENT_NUMBER];
		bool                   nulls[BACKEND_MEMORY_CONTEXT_ALLOC_DETAIL_ELEMENT_NUMBER] = {false};
		MemCtxDetail *entry;
		memstat = (PgBackendMemStat*)(funcctx->user_fctx);
		entry = &memstat->memctx_detail[funcctx->call_cntr];
		i = 0;
		values[i++] = CStringGetTextDatum(entry->file);
		values[i++] = Int32GetDatum(entry->line);
		values[i] = Int64GetDatum(entry->total_size);

		/* Build and return the tuple. */
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

void
ProcessGetMemoryContextDetailInterrupt(void)
{
	mcxtdumpEntry	*entry;
	GetMemoryContextDetailPending = false;
	LWLockAcquire(McxtDumpHashLock, LW_SHARED);
	entry = (mcxtdumpEntry *) hash_search(mcxtdumpHash, &MyProcPid, HASH_FIND, NULL);
	LWLockRelease(McxtDumpHashLock);

	DoMemCtxDetailBooking(entry->cxt_name);
}
