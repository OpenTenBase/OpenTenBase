/*-------------------------------------------------------------------------
 *
 * explain_dist.c
 *    This code provides support for distributed explain analyze.
 *
 * Portions Copyright (c) 2020, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *        src/backend/commands/explain_dist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain_dist.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/resultCache.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "nodes/bitmapset.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "pgxc/execRemote.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "storage/shmem.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "float.h"
/* Read instrument field */
#define INSTR_READ_FIELD(fldname)                \
do {                                             \
	instr->fldname = strtod(tmp_head, &tmp_pos); \
	tmp_head = tmp_pos + 1;                      \
} while(0)

#define INSTR_READ_STR(dst, len)		\
do {									\
	StrNCpy(dst, tmp_head, len);		\
	dst[len] = '\0';					\
	tmp_pos = tmp_head + len;			\
	tmp_head = tmp_pos + 1;				\
} while(0)

#define INSTR_MOVE_HEAD(len)			\
do {									\
	tmp_head = tmp_head + len;			\
} while(0)

/* Tools for max/min */
#define SET_MIN_MAX(min, max, tmp) \
do {                               \
	if (min > tmp)                 \
		min = tmp;                 \
	if (max < tmp)                 \
		max = tmp;                 \
} while(0)

#define SET_MIN_MAX_AVG(val, tmp)                               \
do {                                                            \
	SET_MIN_MAX(val[MIN_VAL], val[MAX_VAL], tmp);               \
	if (tmp > val[AVG_VAL])										\
		val[AVG_VAL] = val[AVG_VAL] + (tmp - val[AVG_VAL]) / count; \
	else														\
		val[AVG_VAL] = val[AVG_VAL] - (val[AVG_VAL] - tmp) / count; \
} while(0)

/* Values we show in explain analyze without verbose */
typedef enum
{
	MIN_VAL,
	MAX_VAL,
	AVG_VAL,
	EXPLAIN_VAL_NUM /* must be the last one */
} ExplainValues;

/* According to RemoteHashState and HashInstrumentation */
typedef enum
{
	SILO_SCANNED,
	SILO_FILTERED,
	SCAN_MEM_PEAK,
	IO_TIME,
	UNPACK_TIME,
	FILL_TIME,
	PROJ_TIME,
	FILTER_TIME,
	ESTORESEQSCANINSTR_VAL_NUM /* must be the last one */
} EstoreSeqScansInstrValues;

/* According to RemoteHashState and HashInstrumentation */
typedef enum
{
	NBUCKETS,
	NBUCKETS_ORG,
	NBATCH,
	NBATCH_ORG,
	SPACEPEAK,
	ROWSFILTERED,
	HASHINSTR_VAL_NUM /* must be the last one */
} HashInstrValues;

/* According to RemoteAggState and AggregateInstrumentation */
typedef enum
{
	HASH_MEM_PEAK,
	HASH_DISK_USED,
	HASH_BATCHES_USED,
	AGGINSTR_VAL_NUM /* must be the last one */
} AggInstrValues;

typedef enum
{
	FRAGMENT_MEM,
	EXECUTOR_MEM,
	TOTAL_PAGE,
	DISK_PAGE,
	FRAGINSTR_VAL_NUM /* must be the last one */
} FragInstrValues;

/* According to RemoteSipFilterState (HashInstrumentation later) */
typedef enum
{
	ROWS_FILTERED,
	SIPINSTR_VAL_NUM /* must be the last one */
} SipInstrValues;

/* Serialize state */
typedef struct
{
	/* ids of plan nodes we've handled */
	Bitmapset  *printed_nodes;
	/* send str buf */
	StringInfoData buf;
} SerializeState;

/* Hash table entry */
typedef struct
{
	/* fid of current fragment */
	int fid;
	/* an instrument str array of current fragment on datanodes */
	StringInfoData strs[FLEXIBLE_ARRAY_MEMBER];
} RemoteInstrStr;

/*
 * The hash table in which remote instruments are stored.
 * It is per-backend. The key for this hash table is fid;
 * the entry is an array of StringInfo received from remote nodes.
 */
static HTAB *remote_instrs_htab = NULL;
static char **dn_names = NULL;

static void ResultCacheInstrIn(StringInfo str, ResultCacheState *instr);

/*
 * InitRemoteInstr
 *
 * Initialize the hash table and dn names (if verbose) for remote instruments.
 * Current memory context is PortalHeapMemory.
 */
void
InitRemoteInstr(void)
{
	HASHCTL hash_ctl;
	int i;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	/* key is fid */
	hash_ctl.keysize = sizeof(int);
	/* entry is an array of StringInfoData for all datanodes */
	hash_ctl.entrysize = offsetof(RemoteInstrStr, strs) +
						 sizeof(StringInfoData) * NumDataNodes;
	hash_ctl.hcxt = CurrentMemoryContext;
	remote_instrs_htab = hash_create("Remote Instruments", 8, &hash_ctl,
									 HASH_ELEM | HASH_CONTEXT);

	/* init dn names for verbose or auto_explain's verbose */
	dn_names = (char **) palloc0(sizeof(char *) * NumDataNodes);
	for (i = 0; i < NumDataNodes; i++)
	{
		dn_names[i] = get_pgxc_nodename(
						PGXCNodeGetNodeOid(i, PGXC_NODE_DATANODE));
	}
}

void
ResetRemoteInstr()
{
	/*
	 * It is ok to just set RemoteInstr NULL, for the memory has already
	 * been clean up with memory context.
	 */
	remote_instrs_htab = NULL;
	dn_names = NULL;
}

/*
 * HasRemoteInstr
 *
 * If the hash table for remote instruments exists.
 */
bool
HasRemoteInstr()
{
	return remote_instrs_htab != NULL;
}

/*
 * InstrOut
 *
 * Serialize Instrumentation structure with the format
 * "nodetype-plan_node_id{val,val,...,val}".
 *
 * NOTE: The function should be modified if the structure of Instrumentation
 * or its relevant members has been changed.
 */
static void
InstrOut(StringInfo buf, Plan *plan, Instrumentation *instr)
{
	/* nodeTag for varify */
	appendStringInfo(buf, "%hd-%d{", nodeTag(plan), plan->plan_node_id);

	/* bool */
	/* running should be false after InstrEndLoop */
	appendStringInfo(buf, "%hd,", instr->need_timer);
	appendStringInfo(buf, "%hd,", instr->need_bufusage);
	appendStringInfo(buf, "%hd,", instr->running);
	/* instr_time */
	/* starttime and counter should be 0 after InstrEndLoop */
	appendStringInfo(buf, "%ld,", instr->starttime.tv_sec);
	appendStringInfo(buf, "%ld,", instr->starttime.tv_nsec);
	appendStringInfo(buf, "%ld,", instr->counter.tv_sec);
	appendStringInfo(buf, "%ld,", instr->counter.tv_nsec);
	/* double */
	/* firsttuple and tuplecount should be 0 after InstrEndLoop */
	appendStringInfo(buf, "%.0f,", instr->firsttuple);
	appendStringInfo(buf, "%.0f,", instr->tuplecount);
	appendStringInfo(buf, "%.0f,", instr->vectorcount);
	/* BufferUsage */
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.shared_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.local_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.temp_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.temp_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_read_time.tv_sec);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_read_time.tv_nsec);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_write_time.tv_sec);
	appendStringInfo(buf, "%ld,", instr->bufusage_start.blk_write_time.tv_nsec);
	/* double */
	appendStringInfo(buf, "%.10f,", instr->startup);
	appendStringInfo(buf, "%.10f,", instr->total);
	appendStringInfo(buf, "%.0f,", instr->ntuples);
	appendStringInfo(buf, "%.0f,", instr->nvectors);
	appendStringInfo(buf, "%.0f,", instr->nloops);
	appendStringInfo(buf, "%.0f,", instr->nfiltered1);
	appendStringInfo(buf, "%.0f,", instr->nfiltered2);
	appendStringInfo(buf, "%.10f,", instr->exec_firsttuple);
	appendStringInfo(buf, "%.10f,", instr->exec_total);
	appendStringInfo(buf, "%.10f,", instr->exec_enter);
	/* BufferUsage */
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage.shared_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_hit);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_dirtied);
	appendStringInfo(buf, "%ld,", instr->bufusage.local_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage.temp_blks_read);
	appendStringInfo(buf, "%ld,", instr->bufusage.temp_blks_written);
	appendStringInfo(buf, "%ld,", instr->bufusage.blk_read_time.tv_sec);
	appendStringInfo(buf, "%ld,", instr->bufusage.blk_read_time.tv_nsec);
	appendStringInfo(buf, "%ld,", instr->bufusage.blk_write_time.tv_sec);
	appendStringInfo(buf, "%ld}", instr->bufusage.blk_write_time.tv_nsec);
}

/*
 * WorkerInstrOut
 *
 * Serialize worker instrumentation with the format
 * "n|val,val,..,val|...|val,val,..,val|". n indicates the worker num,
 * and | separates each worker instrumentation.
 */
static void
WorkerInstrOut(StringInfo buf, WorkerInstrumentation *worker_instr)
{
	int n;

	if (worker_instr == NULL)
	{
		appendStringInfo(buf, "0|");
		return;
	}

	appendStringInfo(buf, "%d|", worker_instr->num_workers);
	for (n = 0; n < worker_instr->num_workers; n++)
	{
		Instrumentation *instr = &worker_instr->instrument[n];

		if (instr->nloops <= 0)
			appendStringInfo(buf, "0|");
		else
			/* send startup, total, ntuples, loops for now */
			appendStringInfo(buf, "%.0f,%.10f,%.10f,%.0f,%.0f,%.10f,%.10f,%.10f|",
							 instr->nloops, instr->startup, instr->total,
							 instr->ntuples, instr->nvectors, instr->exec_enter,
							 instr->exec_firsttuple, instr->exec_total);
	}
}

/*
 * SpecInstrOut
 *
 * Serialize specific information in planstate with the format
 * "1/0<val,val,...,val>", and 1/0 indicates if values are valid or not.
 *
 * NOTE: The function should be modified if the corresponding data structure
 * has been changed.
 * The function is VERY related to show_sort_info, show_hash_info,
 * show_hashagg_info and so on.
 */
static void
SpecInstrOut(StringInfo buf, PlanState *planstate)
{
	switch(nodeTag(planstate->plan))
	{
		case T_Gather:
			{
				appendStringInfo(buf, "1<%d>",
					((GatherState *) planstate)->nworkers_launched);
			}
			break;

		case T_GatherMerge:
			{
				appendStringInfo(buf, "1<%d>",
					((GatherMergeState *) planstate)->nworkers_launched);
			}
			break;

		case T_RemoteSubplan:
			{
				Size cxt_mem;
				RemoteFragmentState *fstate = castNode(RemoteFragmentState, planstate);

				if (fstate->shared_info)
				{
					int n;
					appendStringInfo(buf, "0>");
					appendStringInfo(buf, "%d>", fstate->shared_info->num_workers);
					for (n = 0; n < fstate->shared_info->num_workers; n++)
					{
						FragmentInstrumentation *w_stats;
						w_stats = &fstate->shared_info->sinstrument[n];
						appendStringInfo(buf, "%ld,%ld,%d,%d>",
							w_stats->fragment_mem, w_stats->executor_mem, w_stats->total_pages, w_stats->disk_pages);
						/* no sort info for parallel fragment */
					}
					break;
				}

				cxt_mem = MemoryContextMemAllocated(fstate->cxt, true);
				appendStringInfo(buf, "1<%ld,", (cxt_mem + 1023) / 1024);

				if (fstate->sendFragment)
				{
					cxt_mem = MemoryContextMemAllocated(planstate->state->es_query_cxt, true);
					/* no total_pages and disk_pages info for send fragment */
					appendStringInfo(buf, "%ld,0,0>", (cxt_mem + 1023) / 1024);
				}
				else
				{
					/* no executor mem info for recv fragment */
					appendStringInfo(buf, "0,%d,%d>", fstate->total_pages, fstate->disk_pages);
				}
			}
			break;

		case T_Sort:
			{
				/* according to RemoteSortState and show_sort_info */
				SortState *sortstate = castNode(SortState, planstate);

				if (sortstate->sort_Done && sortstate->tuplesortstate)
				{
					Tuplesortstate *state = (Tuplesortstate *) sortstate->tuplesortstate;
					TuplesortInstrumentation stats;
					tuplesort_get_stats(state, &stats);
					appendStringInfo(buf, "1<%hd,%hd,%ld>",
						stats.sortMethod, stats.spaceType, stats.spaceUsed);
				}
				else
					appendStringInfo(buf, "0>");

				if (sortstate->shared_info)
				{
					int n;
					appendStringInfo(buf, "%d>", sortstate->shared_info->num_workers);
					for (n = 0; n < sortstate->shared_info->num_workers; n++)
					{
						TuplesortInstrumentation *w_stats;
						w_stats = &sortstate->shared_info->sinstrument[n];
						if (w_stats->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
							appendStringInfo(buf, "0>");
						else
							appendStringInfo(buf, "%hd,%hd,%ld>",
								w_stats->sortMethod,
								w_stats->spaceType, w_stats->spaceUsed);
					}
				}
				else
					appendStringInfo(buf, "0>");
			}
			break;

		case T_Hash:
			{
				/* according to RemoteHashState and show_hash_info */
				HashState *hashstate = castNode(HashState, planstate);
				HashInstrumentation hinstrument = {0};

				if (hashstate->hinstrument)
					memcpy(&hinstrument, hashstate->hinstrument,
						sizeof(HashInstrumentation));

				if (hashstate->shared_info)
				{
					SharedHashInfo *shared_info = hashstate->shared_info;
					int			i;

					for (i = 0; i < shared_info->num_workers; ++i)
					{
						HashInstrumentation *worker_hi = &shared_info->hinstrument[i];

						hinstrument.nbuckets = Max(hinstrument.nbuckets,
												worker_hi->nbuckets);
						hinstrument.nbuckets_original = Max(hinstrument.nbuckets_original,
															worker_hi->nbuckets_original);
						hinstrument.nbatch = Max(hinstrument.nbatch,
												worker_hi->nbatch);
						hinstrument.nbatch_original = Max(hinstrument.nbatch_original,
														worker_hi->nbatch_original);
						hinstrument.space_peak = Max(hinstrument.space_peak,
													worker_hi->space_peak);
						hinstrument.rowsFiltered = Max(hinstrument.rowsFiltered,
													worker_hi->rowsFiltered);
					}
				}

				if (hinstrument.nbatch > 0)
					appendStringInfo(buf, "1<%d,%d,%d,%d,%ld,%ld>",
						hinstrument.nbuckets, hinstrument.nbuckets_original,
						hinstrument.nbatch, hinstrument.nbatch_original,
						(hinstrument.space_peak + 1023) / 1024,
						hinstrument.rowsFiltered);
				else
					appendStringInfo(buf, "0>");
			}
			break;

		case T_Agg:
			{
				/* according to show_hashagg_info */
				Agg *agg = (Agg *) planstate->plan;
				AggState *aggstate = (AggState *)planstate;
				if (agg->aggstrategy != AGG_HASHED &&
					agg->aggstrategy != AGG_MIXED)
					break;

				if (aggstate->hash_mem_peak > 0)
				{
					appendStringInfo(buf, "1<%ld,%ld,%d>",
						(aggstate->hash_mem_peak + 1023) / 1024,
						aggstate->hash_disk_used,
						aggstate->hash_batches_used);
				}
				else
					appendStringInfo(buf, "0>");

				if (aggstate->shared_info)
				{
					int n;
					appendStringInfo(buf, "%d>", aggstate->shared_info->num_workers);
					for (n = 0; n < aggstate->shared_info->num_workers; n++)
					{
						AggregateInstrumentation *w_stats;
						w_stats =  &aggstate->shared_info->sinstrument[n];
						if (w_stats->hash_mem_peak == 0)
							appendStringInfo(buf, "0>");
						else
							appendStringInfo(buf, "%ld,%ld,%d>",
								(w_stats->hash_mem_peak + 1023) / 1024,
								w_stats->hash_disk_used,
								w_stats->hash_batches_used);
					}
				}
				else
					appendStringInfo(buf, "0>");
			}
			break;
		default:
			break;
	}
}

/*
 * SerializeLocalInstr
 *
 * Serialize local instruments in the planstate tree for sending.
 */
static bool
SerializeLocalInstr(PlanState *planstate, SerializeState *ss)
{
	/*
	 * We should handle InitPlan/SubPlan the same as in ExplainSubPlans.
	 * But we do not want another planstate_tree_walker,
	 * it is ok to use plan_node_id in place of plan_id.
	 */
	int plan_node_id = planstate->plan->plan_node_id;
	if (bms_is_member(plan_node_id, ss->printed_nodes))
		return false;
	else
		ss->printed_nodes = bms_add_member(ss->printed_nodes, plan_node_id);

	if (planstate->instrument)
	{
		/* clean up the instrumentation state as in ExplainNode */
		InstrEndLoop(planstate->instrument);
		ResultCacheInstrOut(&ss->buf, planstate->state->es_rc_hash_table,
							&planstate->state->es_rc_explained);
		InstrOut(&ss->buf, planstate->plan, planstate->instrument);
		WorkerInstrOut(&ss->buf, planstate->worker_instrument);
		SpecInstrOut(&ss->buf, planstate);
	}
	else
	{
		/* should not be NULL */
		elog(ERROR, "SerializeLocalInstr: instrument is NULL, %d",
			 nodeTag(planstate));
	}

	/* skip CteScan for it is running on its own fragment */
	if (planstate->initPlan)
	{
		ListCell *lc, *prev = NULL;

		/*
		 * Current memory context is MessageContext, which will be reset after
		 * process current message.
		 * We can switch to another context or do not switch context, instead of
		 * generate a new list we delete those unsatisfied listcells from the
		 * original list.
		 */
		lc = list_head(planstate->initPlan);
		while (lc != NULL)
		{
			SubPlanState *sps = (SubPlanState *) lfirst(lc);

			if (!list_member(planstate->plan->exec_subplan, sps->subplan) ||
				sps->subplan->subLinkType == CTE_SUBLINK)
			{
				planstate->initPlan = list_delete_cell(planstate->initPlan, lc, prev);
				if (prev == NULL)
					lc = list_head(planstate->initPlan);
				else
					lc = lnext(prev);
			}
			else
			{
				prev = lc;
				lc = lnext(lc);
			}
		}
	}

	return planstate_tree_walker(planstate, SerializeLocalInstr, ss);
}

/*
 * SendLocalInstr
 *
 * Serialize local instrument of the given portal and send it to CN.
 */
void
SendLocalInstr(QueryDesc *queryDesc)
{
	PlanState *planstate;
	SerializeState ss;

	if (queryDesc == NULL || queryDesc->planstate == NULL)
	{
		elog(DEBUG1, "Invalid query desc when sending local instrument");
		pq_putemptymessage('i');
		return;
	}

	planstate = queryDesc->planstate;

	/* Construct str with the same logic in ExplainNode */
	ss.printed_nodes = NULL;
	pq_beginmessage(&ss.buf, 'i');
	/* special for shared CteScan producer */
	if (IsA(planstate, CteScanState))
	{
		Size cxt_mem;
		Plan *plan = planstate->plan;
		cxt_mem = MemoryContextMemAllocated(planstate->state->es_query_cxt, true);
		appendStringInfo(&ss.buf, "%hd-%d<%ld>",
			nodeTag(plan), plan->plan_node_id, (cxt_mem + 1023) / 1024);
		planstate = ((CteScanState *)planstate)->cteplanstate;
	}
	SerializeLocalInstr(planstate, &ss);
	pq_endmessage(&ss.buf);
	bms_free(ss.printed_nodes);

	pq_flush();
}

/*
 * HandleRemoteInstr
 *
 * Handle remote instrument message and save it in hash table.
 */
void
HandleRemoteInstr(char *msg_body, size_t len, int nodeoid, int fid)
{
	/* for now handle with datanode only */
	char nodetype = PGXC_NODE_DATANODE;
	int nodeid;
	RemoteInstrStr *entry;
	bool found;
	StringInfo instr_str;

	if (remote_instrs_htab == NULL)
		return;

	nodeid = PGXCNodeGetNodeId(nodeoid, &nodetype);
	entry = (RemoteInstrStr *) hash_search(remote_instrs_htab, &fid, HASH_ENTER, &found);
	/* init if new entry */
	if (!found)
	{
		MemSet(entry->strs, 0, sizeof(StringInfoData) * NumDataNodes);
	}

	instr_str = &(entry->strs[nodeid]);
	/* Can not use normal StringInfo functions for we are in CtrlThread */
	instr_str->data = MemoryContextAlloc(hash_get_memcxt(remote_instrs_htab), len + 1);
	memcpy(instr_str->data, msg_body, len);
	instr_str->data[len] = '\0';
	instr_str->len = len;
	instr_str->maxlen = len + 1;
}

/*
 * InstrIn
 *
 * DeSerialize of one Instrumentation.
 */
static void
InstrIn(StringInfo str, Plan *plan, Instrumentation *instr)
{
	char *tmp_pos;
	char *tmp_head = &str->data[str->cursor];

	/* verify nodetype and plan_node_id */
	if (strtol(tmp_head, &tmp_pos, 0) != nodeTag(plan))
	{
		elog(ERROR, "Mismatch node type in InstrIn: %s %d",
			&str->data[str->cursor], nodeTag(plan));
	}
	tmp_head = tmp_pos + 1;
	if (strtol(tmp_head, &tmp_pos, 0) != plan->plan_node_id)
	{
		elog(ERROR, "Mismatch node id in InstrIn: %s %d",
			&str->data[str->cursor], plan->plan_node_id);
	}
	tmp_head = tmp_pos + 1;

	/* read values */
	INSTR_READ_FIELD(need_timer);
	INSTR_READ_FIELD(need_bufusage);
	INSTR_READ_FIELD(running);

	INSTR_READ_FIELD(starttime.tv_sec);
	INSTR_READ_FIELD(starttime.tv_nsec);
	INSTR_READ_FIELD(counter.tv_sec);
	INSTR_READ_FIELD(counter.tv_nsec);

	INSTR_READ_FIELD(firsttuple);
	INSTR_READ_FIELD(tuplecount);
	INSTR_READ_FIELD(vectorcount);

	INSTR_READ_FIELD(bufusage_start.shared_blks_hit);
	INSTR_READ_FIELD(bufusage_start.shared_blks_read);
	INSTR_READ_FIELD(bufusage_start.shared_blks_dirtied);
	INSTR_READ_FIELD(bufusage_start.shared_blks_written);
	INSTR_READ_FIELD(bufusage_start.local_blks_hit);
	INSTR_READ_FIELD(bufusage_start.local_blks_read);
	INSTR_READ_FIELD(bufusage_start.local_blks_dirtied);
	INSTR_READ_FIELD(bufusage_start.local_blks_written);
	INSTR_READ_FIELD(bufusage_start.temp_blks_read);
	INSTR_READ_FIELD(bufusage_start.temp_blks_written);
	INSTR_READ_FIELD(bufusage_start.blk_read_time.tv_sec);
	INSTR_READ_FIELD(bufusage_start.blk_read_time.tv_nsec);
	INSTR_READ_FIELD(bufusage_start.blk_write_time.tv_sec);
	INSTR_READ_FIELD(bufusage_start.blk_write_time.tv_nsec);

	INSTR_READ_FIELD(startup);
	INSTR_READ_FIELD(total);
	INSTR_READ_FIELD(ntuples);
	INSTR_READ_FIELD(nvectors);
	INSTR_READ_FIELD(nloops);
	INSTR_READ_FIELD(nfiltered1);
	INSTR_READ_FIELD(nfiltered2);
	INSTR_READ_FIELD(exec_firsttuple);
	INSTR_READ_FIELD(exec_total);
	INSTR_READ_FIELD(exec_enter);

	INSTR_READ_FIELD(bufusage.shared_blks_hit);
	INSTR_READ_FIELD(bufusage.shared_blks_read);
	INSTR_READ_FIELD(bufusage.shared_blks_dirtied);
	INSTR_READ_FIELD(bufusage.shared_blks_written);
	INSTR_READ_FIELD(bufusage.local_blks_hit);
	INSTR_READ_FIELD(bufusage.local_blks_read);
	INSTR_READ_FIELD(bufusage.local_blks_dirtied);
	INSTR_READ_FIELD(bufusage.local_blks_written);
	INSTR_READ_FIELD(bufusage.temp_blks_read);
	INSTR_READ_FIELD(bufusage.temp_blks_written);
	INSTR_READ_FIELD(bufusage.blk_read_time.tv_sec);
	INSTR_READ_FIELD(bufusage.blk_read_time.tv_nsec);
	INSTR_READ_FIELD(bufusage.blk_write_time.tv_sec);
	INSTR_READ_FIELD(bufusage.blk_write_time.tv_nsec);

	/* tmp_head points to next instrument's nodetype or '\0' already */
	str->cursor = tmp_head - &str->data[0];
}

/*
 * WorkerInstrIn
 *
 * DeSerialize of worker instrument info of current node.
 */
static void
WorkerInstrIn(StringInfo str, RemoteInstrumentation *remote_instr)
{
	char *tmp_pos;
	char *tmp_head = &str->data[str->cursor];
	int num_workers;

	num_workers = strtol(tmp_head, &tmp_pos, 0);
	tmp_head = tmp_pos + 1;
	if (num_workers > 0)
	{
		int n;
		Size size;
		Instrumentation *instr;

		size = mul_size(num_workers, sizeof(Instrumentation));
		remote_instr->w_instrs = (WorkerInstrumentation *) palloc0(size +
								offsetof(WorkerInstrumentation, instrument));
		remote_instr->w_instrs->num_workers = num_workers;

		for (n = 0; n < num_workers; n++)
		{
			instr = &(remote_instr->w_instrs->instrument[n]);
			INSTR_READ_FIELD(nloops);
			if (instr->nloops > 0)
			{
				INSTR_READ_FIELD(startup);
				INSTR_READ_FIELD(total);
				INSTR_READ_FIELD(ntuples);
				INSTR_READ_FIELD(nvectors);
				INSTR_READ_FIELD(exec_enter);
				INSTR_READ_FIELD(exec_firsttuple);
				INSTR_READ_FIELD(exec_total);
			}
		}
	}

	str->cursor = tmp_head - &str->data[0];
}

/*
 * DeSerialize of result cache info of current node.
 */
static void
ResultCacheInstrIn(StringInfo str, ResultCacheState *instr)
{
	char *tmp_pos;
	char *tmp_head = &str->data[str->cursor];

	INSTR_READ_FIELD(rcisvalid);
	if (instr->rcisvalid)
	{
		int n = 0;
		INSTR_READ_FIELD(num_rc_objs);
		instr->rc_stats = (ResultCacheInfo *) palloc0(instr->num_rc_objs * sizeof(ResultCacheInfo));
		for (n = 0; n < instr->num_rc_objs; n++)
		{
			INSTR_READ_FIELD(rc_stats[n].rc_name_len);
			instr->rc_stats[n].rc_name = (char *) palloc0(instr->rc_stats[n].rc_name_len + 1);
			INSTR_READ_STR(instr->rc_stats[n].rc_name, instr->rc_stats[n].rc_name_len + 1);
			INSTR_READ_FIELD(rc_stats[n].rc_memory);
			INSTR_READ_FIELD(rc_stats[n].rc_lru_count);
			INSTR_READ_FIELD(rc_stats[n].rc_total_search);
			INSTR_READ_FIELD(rc_stats[n].rc_hits);
		}
	}
	INSTR_MOVE_HEAD(1);
	str->cursor = tmp_head - &str->data[0];
}

/*
 * SpecInstrIn
 *
 * DeSerialize of specific instrument info of current node.
 */
static void
SpecInstrIn(StringInfo str, Plan *plan, RemoteInstrumentation *remote_instr)
{
	char *tmp_pos;
	char *tmp_head = &str->data[str->cursor];

	switch(nodeTag(plan))
	{
		case T_Gather:
		case T_GatherMerge:
			{
				RemoteState *instr = (RemoteState *)palloc0(sizeof(RemoteState));
				/* always valid */
				INSTR_READ_FIELD(isvalid);
				INSTR_READ_FIELD(num_workers);
				remote_instr->state = instr;
			}
			break;

		case T_RemoteSubplan:
			{
				RemoteFragState *instr = (RemoteFragState *)palloc0(
													sizeof(RemoteFragState));
				INSTR_READ_FIELD(rs.isvalid);
				if (instr->rs.isvalid)
				{
					INSTR_READ_FIELD(stat.fragment_mem);
					INSTR_READ_FIELD(stat.executor_mem);
					INSTR_READ_FIELD(stat.total_pages);
					INSTR_READ_FIELD(stat.disk_pages);
				}
				else
				{
					int n;
					Size size;

					INSTR_READ_FIELD(rs.num_workers);
					size = mul_size(sizeof(FragmentInstrumentation),
									instr->rs.num_workers);
					instr->w_stats = (FragmentInstrumentation *)palloc0(size);
					for (n = 0; n < instr->rs.num_workers; n++)
					{
						INSTR_READ_FIELD(w_stats[n].fragment_mem);
						INSTR_READ_FIELD(w_stats[n].executor_mem);
						INSTR_READ_FIELD(w_stats[n].total_pages);
						INSTR_READ_FIELD(w_stats[n].disk_pages);
					}
				}

				remote_instr->state = (RemoteState *) instr;
			}
			break;

		case T_Sort:
			{
				RemoteSortState *instr = (RemoteSortState *)palloc0(
													sizeof(RemoteSortState));
				/* either stat or w_stat is valid */
				INSTR_READ_FIELD(rs.isvalid);
				if (instr->rs.isvalid)
				{
					INSTR_READ_FIELD(stat.sortMethod);
					INSTR_READ_FIELD(stat.spaceType);
					INSTR_READ_FIELD(stat.spaceUsed);
				}

				INSTR_READ_FIELD(rs.num_workers);
				if (instr->rs.num_workers > 0)
				{
					int n;
					Size size;

					size = mul_size(sizeof(TuplesortInstrumentation),
									instr->rs.num_workers);
					instr->w_stats = (TuplesortInstrumentation *)palloc0(size);

					for (n = 0; n < instr->rs.num_workers; n++)
					{
						INSTR_READ_FIELD(w_stats[n].sortMethod);
						if (instr->w_stats[n].sortMethod != SORT_TYPE_STILL_IN_PROGRESS)
						{
							INSTR_READ_FIELD(w_stats[n].spaceType);
							INSTR_READ_FIELD(w_stats[n].spaceUsed);
						}
					}
				}
				remote_instr->state = (RemoteState *) instr;
			}
			break;

		case T_Hash:
			{
				RemoteHashState *instr = (RemoteHashState *)palloc0(
													sizeof(RemoteHashState));
				INSTR_READ_FIELD(rs.isvalid);
				if (instr->rs.isvalid)
				{
					INSTR_READ_FIELD(stat.nbuckets);
					INSTR_READ_FIELD(stat.nbuckets_original);
					INSTR_READ_FIELD(stat.nbatch);
					INSTR_READ_FIELD(stat.nbatch_original);
					INSTR_READ_FIELD(stat.space_peak);
					INSTR_READ_FIELD(stat.rowsFiltered);
				}
				remote_instr->state = (RemoteState *) instr;
			}
			break;

		case T_Agg:
			{
				RemoteAggState *instr;
				if (((Agg *)plan)->aggstrategy != AGG_HASHED &&
					((Agg *)plan)->aggstrategy != AGG_MIXED)
					break;

				instr = (RemoteAggState *)palloc0(sizeof(RemoteAggState));
				INSTR_READ_FIELD(rs.isvalid);
				if (instr->rs.isvalid)
				{
					INSTR_READ_FIELD(stat.hash_mem_peak);
					INSTR_READ_FIELD(stat.hash_disk_used);
					INSTR_READ_FIELD(stat.hash_batches_used);
				}

				INSTR_READ_FIELD(rs.num_workers);
				if (instr->rs.num_workers > 0)
				{
					int n;
					Size size;

					size = mul_size(sizeof(AggregateInstrumentation),
									instr->rs.num_workers);
					instr->w_stats = (AggregateInstrumentation *)palloc0(size);

					for (n = 0; n < instr->rs.num_workers; n++)
					{
						INSTR_READ_FIELD(w_stats[n].hash_mem_peak);
						if (instr->w_stats[n].hash_mem_peak > 0)
						{
							INSTR_READ_FIELD(w_stats[n].hash_disk_used);
							INSTR_READ_FIELD(w_stats[n].hash_batches_used);
						}
					}
				}
				remote_instr->state = (RemoteState *) instr;
			}
			break;
		default:
			break;
	}

	str->cursor = tmp_head - &str->data[0];
}

/*
 * ConstructRemoteInstr
 *
 * Construct all remote instruments for current node.
 */
RemoteInstrumentation *
ConstructRemoteInstr(Plan *plan, int fid)
{
	int i;
	RemoteInstrStr *entry;
	StringInfo instr_str;
	Size size;
	RemoteInstrumentation *instrs = NULL;

	if (remote_instrs_htab == NULL)
	{
		elog(ERROR, "Explain remote instrument: hash table is NULL");
	}
	entry = (RemoteInstrStr *) hash_search(remote_instrs_htab, &fid, HASH_FIND, NULL);
	if (entry == NULL)
	{
		/* fragment may be not executed */
		elog(DEBUG1, "Explain remote instrument: can not find fid %d", fid);
		return NULL;
	}

	size = mul_size(sizeof(RemoteInstrumentation), NumDataNodes);
	instrs = (RemoteInstrumentation *) palloc0(size);
	for (i = 0; i < NumDataNodes; i++)
	{
		instr_str = &(entry->strs[i]);
		if (instr_str->data)
		{
			/* maybe reparse sometimes */
			if (instr_str->cursor == instr_str->len)
				instr_str->cursor = 0;

			ResultCacheInstrIn(instr_str, &instrs[i].rcstate);
			InstrIn(instr_str, plan, &instrs[i].instr);
			WorkerInstrIn(instr_str, &instrs[i]);
			SpecInstrIn(instr_str, plan, &instrs[i]);
		}
	}
	return instrs;
}

/*
 * DestroyRemoteInstr
 *
 * Free the space of all remote instruments for current node.
 */
void
DestroyRemoteInstr(RemoteInstrumentation *remote_instrs)
{
	int i;

	for (i = 0; i < NumDataNodes; i++)
	{
		pfree_ext_not_set_null(remote_instrs[i].state);
		pfree_ext_not_set_null(remote_instrs[i].w_instrs);
	}

	pfree(remote_instrs);
}

/*
 * ExplainCommonRemoteInstr
 *
 * Explain remote instruments for common info of current node.
 */
void
ExplainCommonRemoteInstr(RemoteInstrumentation *instrs, ExplainState *es,
						const char *prefix)
{
	int i;
	int n;
	/* for min/max display */
	double nloops_min, nloops_max, nloops_tmp;
	double startup_sec_min, startup_sec_max, startup_sec_tmp, startup_sec_worker;
	double total_sec_min, total_sec_max, total_sec_tmp, total_sec_worker;
	double rows_min, rows_max, rows_tmp;
	double vecs_min, vecs_max, vecs_tmp;
	/* for verbose */
	StringInfoData buf;
	const char *spec = prefix ? prefix : " ";
	/*for fix dn info by worker data*/
	WorkerInstrumentation *w_instrs;
	Instrumentation *w_instr;

	appendStringInfoSpaces(es->str, es->indent * 2);
	for (i = 0; i < NumDataNodes; i++)
	{
		if (instrs[i].instr.nloops > 0)
			break;
	}
	if (i == NumDataNodes)
	{
		if (es->format == EXPLAIN_FORMAT_TEXT)
			appendStringInfo(es->str, "DN%s: never executed\n", spec);
		else
			ExplainPropertyText("DN", "never executed\n", es);
		return;
	}

	nloops_min = nloops_max = instrs[i].instr.nloops;
	rows_min = rows_max = instrs[i].instr.ntuples / nloops_min;
	vecs_min = vecs_max = instrs[i].instr.nvectors / nloops_min;
	startup_sec_min = DBL_MAX;
	startup_sec_max = 0;
	total_sec_min = DBL_MAX;
	total_sec_max = 0;

	if (es->verbose)
		initStringInfo(&buf);

	for (; i < NumDataNodes; i++)
	{
		if (instrs[i].instr.nloops == 0)
			continue;

		w_instrs = instrs[i].w_instrs;
		nloops_tmp = instrs[i].instr.nloops;
		rows_tmp = instrs[i].instr.ntuples / nloops_tmp;
		vecs_tmp = instrs[i].instr.nvectors / nloops_tmp;
		startup_sec_tmp = 1000.0 * instrs[i].instr.startup / nloops_tmp;
		total_sec_tmp = 1000.0 * instrs[i].instr.total / nloops_tmp;
		if (w_instrs != NULL)
		{
			for (n = 0; n < w_instrs->num_workers; n++)
			{
				w_instr = &(w_instrs->instrument[n]);
				if (w_instr->nloops == 0)
					continue;

				startup_sec_worker = 1000.0 * w_instr->startup / w_instr->nloops;
				startup_sec_tmp = startup_sec_worker < startup_sec_tmp ? startup_sec_worker : startup_sec_tmp;
				total_sec_worker = 1000.0 * w_instr->total / w_instr->nloops;
				total_sec_tmp = total_sec_worker > total_sec_tmp ? total_sec_worker : total_sec_tmp;
			}
		}
		SET_MIN_MAX(nloops_min, nloops_max, nloops_tmp);
		SET_MIN_MAX(startup_sec_min, startup_sec_max, startup_sec_tmp);
		SET_MIN_MAX(total_sec_min, total_sec_max, total_sec_tmp);
		SET_MIN_MAX(rows_min, rows_max, rows_tmp);
		SET_MIN_MAX(vecs_min, vecs_max, vecs_tmp);
		/* one line for each dn if verbose */
		if (es->verbose)
		{
            appendStringInfoSpaces(&buf, es->indent * 2);
            if (es->timing)
				appendStringInfo(&buf,
					"- %s (actual time=%.3f..%.3f rows=%.0f loops=%.0f)\n",
					dn_names[i], startup_sec_tmp, total_sec_tmp,
					rows_tmp, nloops_tmp);
			else
				appendStringInfo(&buf, "- %s (actual rows=%.0f loops=%.0f)\n",
					dn_names[i], rows_tmp, nloops_tmp);
        }
    }


    if (es->timing)
    {
		appendStringInfo(es->str,
			"DN%s(actual startup time=%.3f..%.3f total time=%.3f..%.3f rows=%.0f..%.0f "
			"loops=%.0f..%.0f)\n",
			spec, startup_sec_min, startup_sec_max,
			total_sec_min, total_sec_max, rows_min, rows_max,
			nloops_min, nloops_max);
	}
	else
	{
		appendStringInfo(es->str,
			"DN%s(actual rows=%.0f..%.0f loops=%.0f..%.0f)\n",
			spec, rows_min, rows_max, nloops_min, nloops_max);
    }
    if (es->verbose)
    {
        appendStringInfo(es->str, "%s", buf.data);
        pfree(buf.data);
	}
}



/*
 * ExplainCommonRemoteInstrJson
 *
 * Explain remote instruments for common info of current node(JSON FORMAT).
 */
void ExplainCommonRemoteInstrJson(RemoteInstrumentation *instrs, ExplainState *es,
								  const char *prefix)
{
	int i;
	int n;
	/* for min/max display */
	double nloops_tmp;
	double startup_sec_tmp, startup_sec_worker;
	double total_sec_tmp, total_sec_worker;
	double rows_tmp;
	const char *spec = prefix ? prefix : "DN";
	/*for fix dn info by worker data*/
	WorkerInstrumentation *w_instrs;
	Instrumentation *w_instr;

	for (i = 0; i < NumDataNodes; i++)
	{
		if (instrs[i].instr.nloops > 0)
			break;
	}
	if (i == NumDataNodes)
	{
		ExplainPropertyText("DN", "never executed\n", es);
		return;
	}

	ExplainOpenGroup(spec, spec, true, es);

	for (; i < NumDataNodes; i++)
	{
		if (instrs[i].instr.nloops == 0)
			continue;

		w_instrs = instrs[i].w_instrs;
		nloops_tmp = instrs[i].instr.nloops;
		rows_tmp = instrs[i].instr.ntuples / nloops_tmp;
		startup_sec_tmp = 1000.0 * instrs[i].instr.startup / nloops_tmp;
		total_sec_tmp = 1000.0 * instrs[i].instr.total / nloops_tmp;
		if (w_instrs != NULL)
		{
			for (n = 0; n < w_instrs->num_workers; n++)
			{
				w_instr = &(w_instrs->instrument[n]);
				if (w_instr->nloops == 0)
					continue;

				startup_sec_worker = 1000.0 * w_instr->startup / w_instr->nloops;
				startup_sec_tmp = startup_sec_worker < startup_sec_tmp ? startup_sec_worker : startup_sec_tmp;
				total_sec_worker = 1000.0 * w_instr->total / w_instr->nloops;
				total_sec_tmp = total_sec_worker > total_sec_tmp ? total_sec_worker : total_sec_tmp;
			}
		}
		if (es->verbose)
		{
			if (es->timing)
			{
				/* Consider vectorize */
				ExplainOpenGroup(dn_names[i], dn_names[i], true, es);
				ExplainPropertyFloat("exec_enter", NULL, instrs[i].instr.exec_enter * 1000.0, 3, es);
				ExplainPropertyFloat("exec_startup", NULL, instrs[i].instr.exec_firsttuple * 1000.0, 3, es);
				ExplainPropertyFloat("exec_total", NULL, instrs[i].instr.exec_total * 1000.0, 3, es);
				ExplainPropertyFloat("startup", NULL, startup_sec_tmp, 3, es);
				ExplainPropertyFloat("total", NULL, total_sec_tmp, 3, es);
				ExplainPropertyFloat("rows", NULL, rows_tmp, 0, es);
				ExplainPropertyFloat("loops", NULL, nloops_tmp, 0, es);
				ExplainCloseGroup(dn_names[i], dn_names[i], true, es);
			}
		}
	}
	ExplainCloseGroup(spec, spec, true, es);
}

/*
 * ExplainRemoteSortState
 *
 * Display instrument info in SortState of datanodes.
 */
static void
ExplainRemoteSortState(RemoteInstrumentation *instrs, ExplainState *es)
{
	int i;
	TuplesortInstrumentation *stat;
	char *spaceType[NUM_TUPLESORTSPACETYPE] = {"Disk", "Memory"};

	for (i = 0; i < NumDataNodes; i++)
	{
		if (instrs[i].state == NULL)
			continue;
		if (instrs[i].state->isvalid || instrs[i].state->num_workers > 0)
			break;
	}

	if (i == NumDataNodes)
		return;

	/* display min/max/avg values if not verbose */
	if (!es->verbose)
	{
		int count;
		/* Disk/Memory vs. min max avg */
		long spaceUsed[NUM_TUPLESORTSPACETYPE][EXPLAIN_VAL_NUM];
		/* count for Disk/Memory */
		int counts[NUM_TUPLESORTSPACETYPE] = {0};
		int n;

		/* init */
		MemSet(spaceUsed, 0, sizeof(spaceUsed));
		spaceUsed[SORT_SPACE_TYPE_DISK][MIN_VAL] = LONG_MAX;
		spaceUsed[SORT_SPACE_TYPE_MEMORY][MIN_VAL] = LONG_MAX;

		for (; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL ||
				(!instrs[i].state->isvalid && instrs[i].state->num_workers == 0))
				continue;

			if (instrs[i].state->isvalid)
			{
				stat = &((RemoteSortState *) instrs[i].state)->stat;
				count = ++counts[stat->spaceType];
				SET_MIN_MAX_AVG(spaceUsed[stat->spaceType], stat->spaceUsed);
			}
			for (n = 0; n < instrs[i].state->num_workers; n++)
			{
				stat = &((RemoteSortState *) instrs[i].state)->w_stats[n];
				if (stat->sortMethod == SORT_TYPE_STILL_IN_PROGRESS)
					continue;

				count = ++counts[stat->spaceType];
				SET_MIN_MAX_AVG(spaceUsed[stat->spaceType], stat->spaceUsed);
			}
		}

		/* TODO: display sort method if needed */
		appendStringInfoSpaces(es->str, es->indent * 2);
		if (counts[SORT_SPACE_TYPE_DISK] > 0)
		{
			appendStringInfo(es->str, "Sort %s: min %ldkB max %ldkB avg %ldkB%s",
				spaceType[SORT_SPACE_TYPE_DISK],
				spaceUsed[SORT_SPACE_TYPE_DISK][MIN_VAL],
				spaceUsed[SORT_SPACE_TYPE_DISK][MAX_VAL],
				spaceUsed[SORT_SPACE_TYPE_DISK][AVG_VAL],
				counts[SORT_SPACE_TYPE_MEMORY] > 0 ? "  " : "\n");
		}
		if (counts[SORT_SPACE_TYPE_MEMORY] > 0)
		{
			appendStringInfo(es->str, "Sort %s: min %ldkB max %ldkB avg %ldkB\n",
				spaceType[SORT_SPACE_TYPE_MEMORY],
				spaceUsed[SORT_SPACE_TYPE_MEMORY][MIN_VAL],
				spaceUsed[SORT_SPACE_TYPE_MEMORY][MAX_VAL],
				spaceUsed[SORT_SPACE_TYPE_MEMORY][AVG_VAL]);
		}
	}
	else
	{
		const char *sortMethod[NUM_TUPLESORTMETHODS + 1] = {0};
		StringInfoData buf;

		/* init */
		initStringInfo(&buf);

		for (; i < NumDataNodes; i++)
		{
			/* parallel details are displayed by ExplainRemoteWorkerInstr */
			if (instrs[i].state == NULL || !instrs[i].state->isvalid)
				continue;

			stat = &((RemoteSortState *) instrs[i].state)->stat;

			if (sortMethod[stat->sortMethod] == NULL)
				sortMethod[stat->sortMethod] = tuplesort_method_name(stat->sortMethod);

			appendStringInfoSpaces(&buf, es->indent * 2);
			appendStringInfo(&buf, "- %s Sort Method: %s  %s: %ldkB\n",
				dn_names[i], sortMethod[stat->sortMethod],
				spaceType[stat->spaceType], stat->spaceUsed);
		}

		if (buf.len > 0)
			appendStringInfo(es->str, "%s", buf.data);
		pfree(buf.data);
	}
}

/*
 * ExplainRemoteHashState
 *
 * Display instrument info in HashState of datanodes.
 */
static void
ExplainRemoteHashState(RemoteInstrumentation *instrs, ExplainState *es)
{
	int i;
	HashInstrumentation *stat;

	for (i = 0; i < NumDataNodes; i++)
	{
		if (instrs[i].state == NULL)
			continue;
		if (instrs[i].state->isvalid)
			break;
	}

	if (i == NumDataNodes)
		return;

	if (!es->verbose)
	{
		/* RemoteHashState values vs. min max avg */
		long values[HASHINSTR_VAL_NUM][EXPLAIN_VAL_NUM];
		int count = 0;

		/* init */
		MemSet(values, 0, sizeof(values));
		values[NBUCKETS][MIN_VAL] = values[NBUCKETS_ORG][MIN_VAL] = LONG_MAX;
		values[NBATCH][MIN_VAL] = values[NBATCH_ORG][MIN_VAL] = LONG_MAX;
		values[SPACEPEAK][MIN_VAL] = values[ROWSFILTERED][MIN_VAL] = LONG_MAX;

		for (; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL || !instrs[i].state->isvalid)
				continue;

			stat = &((RemoteHashState *) instrs[i].state)->stat;
			count++;
			SET_MIN_MAX_AVG(values[NBUCKETS], stat->nbuckets);
			SET_MIN_MAX_AVG(values[NBUCKETS_ORG], stat->nbuckets_original);
			SET_MIN_MAX_AVG(values[NBATCH], stat->nbatch);
			SET_MIN_MAX_AVG(values[NBATCH_ORG], stat->nbatch_original);
			SET_MIN_MAX_AVG(values[SPACEPEAK], stat->space_peak);
			SET_MIN_MAX_AVG(values[ROWSFILTERED], stat->rowsFiltered);
			if (instrs[i].w_instrs && instrs[i].w_instrs->num_workers > 0)
				es->query_mem += stat->space_peak/instrs[i].w_instrs->num_workers;
			else
				es->query_mem += stat->space_peak;
		}

		/* TODO: display min/max/avg of original nbuckets/nbatch if needed */
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str,
			"Buckets: min %ld max %ld avg %ld  "
			"Batches: min %ld max %ld avg %ld  "
			"Memory Usage: min %ldkB max %ldkB avg %ldkB  "
			"Rows Filtered: min %ld max %ld avg %ld\n",
			values[NBUCKETS][0], values[NBUCKETS][1], values[NBUCKETS][2],
			values[NBATCH][0], values[NBATCH][1], values[NBATCH][2],
			values[SPACEPEAK][0], values[SPACEPEAK][1], values[SPACEPEAK][2],
			values[ROWSFILTERED][0], values[ROWSFILTERED][1], values[ROWSFILTERED][2]);
	}
	else
	{
		StringInfoData buf;
		initStringInfo(&buf);

		for (; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL || !instrs[i].state->isvalid)
				continue;

			stat = &((RemoteHashState *) instrs[i].state)->stat;
			if (instrs[i].w_instrs && instrs[i].w_instrs->num_workers > 0)
				es->query_mem += stat->space_peak/instrs[i].w_instrs->num_workers;
			else
				es->query_mem += stat->space_peak;

			appendStringInfoSpaces(&buf, es->indent * 2);
			if (stat->nbatch_original != stat->nbatch ||
				stat->nbuckets_original != stat->nbuckets)
				appendStringInfo(&buf,
					"- %s Buckets: %d (originally %d)  Batches: %d (originally %d)  "
					"Memory Usage: %ldkB  Rows Filtered: %ld\n",
					dn_names[i], stat->nbuckets, stat->nbuckets_original,
					stat->nbatch, stat->nbatch_original, stat->space_peak,
					stat->rowsFiltered);
			else
				appendStringInfo(&buf,
					"- %s Buckets: %d  Batches: %d  "
					"Memory Usage: %ldkB  Rows Filtered: %ld\n",
					dn_names[i], stat->nbuckets, stat->nbatch,
					stat->space_peak, stat->rowsFiltered);
		}

		if (buf.len > 0)
			appendStringInfo(es->str, "%s", buf.data);
		pfree(buf.data);
	}
}

/*
 * ExplainRemoteAggState
 *
 * Display instrument info in AggState of datanodes.
 */
static void
ExplainRemoteAggState(RemoteInstrumentation *instrs, ExplainState *es)
{
	int i;
	AggregateInstrumentation *stat;

	for (i = 0; i < NumDataNodes; i++)
	{
		if (instrs[i].state == NULL)
			continue;
		if (instrs[i].state->isvalid || instrs[i].state->num_workers > 0)
			break;
	}

	if (i == NumDataNodes)
		return;

	/* display min/max/avg values if not verbose */
	if (!es->verbose)
	{
		int count = 0;
		long values[AGGINSTR_VAL_NUM][EXPLAIN_VAL_NUM];
		int n;

		/* init */
		MemSet(values, 0, sizeof(values));
		values[HASH_MEM_PEAK][MIN_VAL] = LONG_MAX;
		values[HASH_DISK_USED][MIN_VAL] = LONG_MAX;
		values[HASH_BATCHES_USED][MIN_VAL] = LONG_MAX;

		for (; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL ||
				(!instrs[i].state->isvalid && instrs[i].state->num_workers == 0))
				continue;

			if (instrs[i].state->isvalid)
			{
				stat = &((RemoteAggState *) instrs[i].state)->stat;
				count++;
				SET_MIN_MAX_AVG(values[HASH_MEM_PEAK], stat->hash_mem_peak);
				SET_MIN_MAX_AVG(values[HASH_DISK_USED], stat->hash_disk_used);
				SET_MIN_MAX_AVG(values[HASH_BATCHES_USED], stat->hash_batches_used);
				es->query_mem += stat->hash_mem_peak;
			}
			for (n = 0; n < instrs[i].state->num_workers; n++)
			{
				stat = &((RemoteAggState *) instrs[i].state)->w_stats[n];
				if (stat->hash_mem_peak == 0)
					continue;

				count++;
				SET_MIN_MAX_AVG(values[HASH_MEM_PEAK], stat->hash_mem_peak);
				SET_MIN_MAX_AVG(values[HASH_DISK_USED], stat->hash_disk_used);
				SET_MIN_MAX_AVG(values[HASH_BATCHES_USED], stat->hash_batches_used);
				es->query_mem += stat->hash_mem_peak;
			}
		}

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str,
			"Batches: min %ld max %ld avg %ld  "
			"Memory Usage: min %ldkB max %ldkB avg %ldkB",
			values[HASH_BATCHES_USED][0], values[HASH_BATCHES_USED][1], values[HASH_BATCHES_USED][2],
			values[HASH_MEM_PEAK][0], values[HASH_MEM_PEAK][1], values[HASH_MEM_PEAK][2]);
		if (values[HASH_DISK_USED][MAX_VAL] > 0)
			appendStringInfo(es->str,
				"  Disk Usage: min %ldkB max %ldkB avg %ldkB",
				values[HASH_DISK_USED][0], values[HASH_DISK_USED][1], values[HASH_DISK_USED][2]);
		appendStringInfoChar(es->str, '\n');
	}
	else
	{
		StringInfoData buf;
		initStringInfo(&buf);

		for (; i < NumDataNodes; i++)
		{
			/* parallel details are displayed by ExplainRemoteWorkerInstr */
			if (instrs[i].state == NULL || !instrs[i].state->isvalid)
				continue;

			stat = &((RemoteAggState *) instrs[i].state)->stat;
			es->query_mem += stat->hash_mem_peak;

			appendStringInfoSpaces(&buf, es->indent * 2);
			appendStringInfo(&buf,
				"- %s Batches: %d  Memory Usage: " INT64_FORMAT "kB",
				dn_names[i], stat->hash_batches_used, stat->hash_mem_peak);
			if (stat->hash_batches_used > 1)
				appendStringInfo(&buf, "  Disk Usage: " UINT64_FORMAT "kB",
					stat->hash_disk_used);
			appendStringInfoChar(&buf, '\n');
		}

		if (buf.len > 0)
			appendStringInfo(es->str, "%s", buf.data);
		pfree(buf.data);
	}
}

void
ExplainResultCacheInstr(RemoteInstrumentation *instrs, ExplainState *es)
{
	int i = 0;
	int j = 0;

	for (i = 0; i < NumDataNodes; i++)
	{
		ResultCacheState	*rcs = &instrs[i].rcstate;
		if (rcs == NULL)
			continue;
		if (rcs->rcisvalid)
		{
			char *dn_name = PGXCNodeGetNodename(i + 1, PGXC_NODE_DATANODE);

			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Result_Cache - %s (", dn_name);

			for (j = 0; j < rcs->num_rc_objs; j++)
			{
				ResultCacheInfo	*rci = &(rcs->rc_stats[j]);
				appendStringInfo(es->str, "rc_name: %s, ", rci->rc_name);
				appendStringInfo(es->str, "rc_memory: %ld, ", rci->rc_memory);
				appendStringInfo(es->str, "rc_lru_count: %ld, ", rci->rc_lru_count);
				appendStringInfo(es->str, "rc_total_search: %ld, ", rci->rc_total_search);
				appendStringInfo(es->str, "rc_hit_ratio: %.2f; ", rci->rc_hits);
			}
			appendStringInfo(es->str, ")\n");
		}
	}
}

/*
 * ExplainRemoteFragState
 *
 * Display instrument info in RemoteFragmentState of datanodes.
 */
static void
ExplainRemoteFragState(RemoteInstrumentation *instrs, ExplainState *es)
{
	int i;
	FragmentInstrumentation *stat;

	if (!es->summary)
		return;

	for (i = 0; i < NumDataNodes; i++)
	{
		if (instrs[i].state == NULL)
			continue;
		if (instrs[i].state->isvalid || instrs[i].state->num_workers > 0)
			break;
	}

	if (i == NumDataNodes)
		return;
	if (es->format != EXPLAIN_FORMAT_TEXT)
		return;
	if (!es->verbose)
	{
		int count;
		long values[FRAGINSTR_VAL_NUM][EXPLAIN_VAL_NUM];
		int num_count = 0;
		int n;

		/* init */
		MemSet(values, 0, sizeof(values));
		values[FRAGMENT_MEM][MIN_VAL] = values[EXECUTOR_MEM][MIN_VAL] = LONG_MAX;

		for (; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL ||
				(!instrs[i].state->isvalid && instrs[i].state->num_workers == 0))
				continue;

			if (instrs[i].state->isvalid)
			{
				stat = &((RemoteFragState *) instrs[i].state)->stat;
				count = ++num_count;
				SET_MIN_MAX_AVG(values[FRAGMENT_MEM], stat->fragment_mem);
				SET_MIN_MAX_AVG(values[TOTAL_PAGE], stat->total_pages);
				SET_MIN_MAX_AVG(values[DISK_PAGE], stat->disk_pages);
				if (stat->executor_mem > 0)
				{
					SET_MIN_MAX_AVG(values[EXECUTOR_MEM], stat->executor_mem);
					es->query_mem += stat->executor_mem;
				}
			}
			for (n = 0; n < instrs[i].state->num_workers; n++)
			{
				stat = &((RemoteFragState *) instrs[i].state)->w_stats[n];
				count = ++num_count;
				SET_MIN_MAX_AVG(values[FRAGMENT_MEM], stat->fragment_mem);
				SET_MIN_MAX_AVG(values[TOTAL_PAGE], stat->total_pages);
				SET_MIN_MAX_AVG(values[DISK_PAGE], stat->disk_pages);
				if (stat->executor_mem > 0)
				{
					SET_MIN_MAX_AVG(values[EXECUTOR_MEM], stat->executor_mem);
					es->query_mem += stat->executor_mem;
				}
			}
		}

		appendStringInfoSpaces(es->str, es->indent * 2);
		if (values[EXECUTOR_MEM][MAX_VAL] > 0)
			appendStringInfo(es->str,
				"Executor Memory: min %ldkB max %ldkB avg %ldkB  Send ",
				values[EXECUTOR_MEM][MIN_VAL],
				values[EXECUTOR_MEM][MAX_VAL],
				values[EXECUTOR_MEM][AVG_VAL]);
		else
			appendStringInfo(es->str, "Recv ");

		appendStringInfo(es->str,
			"Memory Usage: min %ldkB max %ldkB avg %ldkB%s",
			values[FRAGMENT_MEM][MIN_VAL],
			values[FRAGMENT_MEM][MAX_VAL],
			values[FRAGMENT_MEM][AVG_VAL],
			values[TOTAL_PAGE][MAX_VAL] > 0 || values[DISK_PAGE][MAX_VAL] > 0 ? "  " : "\n");

		if (values[TOTAL_PAGE][MAX_VAL] > 0)
			appendStringInfo(es->str,
				"Total Pages: min %ld max %ld avg %ld%s",
				values[TOTAL_PAGE][MIN_VAL],
				values[TOTAL_PAGE][MAX_VAL],
				values[TOTAL_PAGE][AVG_VAL],
				values[DISK_PAGE][MAX_VAL] > 0 ? "  " : "\n");

		if (values[DISK_PAGE][MAX_VAL] > 0)
			appendStringInfo(es->str,
				"Spilled Pages: min %ld max %ld avg %ld\n",
				values[DISK_PAGE][MIN_VAL],
				values[DISK_PAGE][MAX_VAL],
				values[DISK_PAGE][AVG_VAL]);
	}
	else
	{
		StringInfoData buf;
		initStringInfo(&buf);

		for (; i < NumDataNodes; i++)
		{
			/* parallel details are displayed by ExplainRemoteWorkerInstr */
			if (instrs[i].state == NULL || !instrs[i].state->isvalid)
				continue;

			stat = &((RemoteFragState *) instrs[i].state)->stat;
			appendStringInfoSpaces(&buf, es->indent * 2);
			appendStringInfo(&buf, "- %s ", dn_names[i]);
			if (stat->executor_mem > 0)
			{
				appendStringInfo(&buf, "Executor Memory: %ldkB  Send ",
					stat->executor_mem);
				es->query_mem += stat->executor_mem;
			}
			else
				appendStringInfo(&buf, "Recv ");
			appendStringInfo(&buf, "Memory Usage: %ldkB", stat->fragment_mem);

			if (stat->total_pages > 0)
				appendStringInfo(&buf, " Total Pages: %d", stat->total_pages);
			if (stat->disk_pages > 0)
				appendStringInfo(&buf, " Spilled Pages: %d", stat->disk_pages);

			appendStringInfoChar(&buf, '\n');
		}

		if (buf.len > 0)
			appendStringInfo(es->str, "%s", buf.data);
		pfree(buf.data);
	}
}

/*
 * ExplainGatherState
 *
 * Display instrument infor in Gather and GatherMerge of datanodes.
 */
static void
ExplainGatherState(RemoteInstrumentation *instrs, ExplainState *es)
{
	int i;

	if (!es->verbose)
	{
		int nums[EXPLAIN_VAL_NUM] = {0};
		int count = 0;
		nums[MIN_VAL] = INT_MAX;

		for (i = 0; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL)
				continue;

			count++;
			SET_MIN_MAX_AVG(nums, instrs[i].state->num_workers);
		}

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "Workers Launched: min %d max %d avg %d\n",
			nums[MIN_VAL], nums[MAX_VAL], nums[AVG_VAL]);
	}
	else
	{
		StringInfoData buf;
		initStringInfo(&buf);

		for (i = 0; i < NumDataNodes; i++)
		{
			if (instrs[i].state == NULL)
				continue;

			appendStringInfoSpaces(&buf, es->indent * 2);
			appendStringInfo(&buf, "- %s Workers Launched: %d\n",
				dn_names[i], instrs[i].state->num_workers);
		}

		if (buf.len > 0)
			appendStringInfo(es->str, "%s", buf.data);
		pfree(buf.data);
	}
}

/*
 * ExplainSpecialRemoteInstr
 *
 * Explain remote instruments for specific info of current node.
 * 1.display min/max/avg if no verbose
 * 2.display all datanodes if verbose
 */
void
ExplainSpecialRemoteInstr(RemoteInstrumentation *instrs, ExplainState *es,
						  NodeTag plantag)
{
	/*
	 * TODO:
	 * 1.display nfiltered1 and nfiltered2 as in show_instrumentation_count.
	 * 2.display bufusage as in show_buffer_usage
	 * 3.display more info in ModifyTableState as show_modifytable_info
	 * 4. ...
	 *
	 * ModifyTableState need to be collected from datanodes too.
	 */
	switch(plantag)
	{
		case T_Gather:
		case T_GatherMerge:
			ExplainGatherState(instrs, es);
			break;
		case T_RemoteSubplan:
			ExplainRemoteFragState(instrs, es);
			break;
		case T_Sort:
			/* display more info in SortState as show_sort_info */
			ExplainRemoteSortState(instrs, es);
			break;
		case T_Hash:
			/* display more info in HashState as show_hash_info */
			ExplainRemoteHashState(instrs, es);
			break;
		case T_Agg:
			/* display more info in AggState as show_hashagg_info */
			ExplainRemoteAggState(instrs, es);
			break;
			break;
		default:
			break;
	}
}

/*
 * ExplainCommonRemoteInstrRecv
 *
 * Explain recieve instruments of current remote subplan node.
 */
void
ExplainCommonRemoteInstrRecv(Plan *plan, ExplainState *es)
{
	RemoteInstrumentation *recv_instrs = NULL;
	int recv_fid = list_nth_int(es->fids, list_length(es->fids) - 2);

	recv_instrs = ConstructRemoteInstr(plan, recv_fid);
	if (recv_instrs)
	{
		ExplainResultCacheInstr(recv_instrs, es);
		ExplainCommonRemoteInstr(recv_instrs, es, " Recv ");
		ExplainSpecialRemoteInstr(recv_instrs, es, nodeTag(plan));
		if (es->verbose)
			ExplainRemoteWorkerInstr(recv_instrs, es, nodeTag(plan));

		DestroyRemoteInstr(recv_instrs);
	}
}


/*
 * ExplainCommonRemoteInstrRecvJson
 *
 * Explain recieve instruments of current remote subplan node (JSON FORMAT).
 */
void
ExplainCommonRemoteInstrRecvJson(Plan *plan, ExplainState *es)
{
	RemoteInstrumentation *recv_instrs = NULL;
	int recv_fid = list_nth_int(es->fids, list_length(es->fids) - 2);

	recv_instrs = ConstructRemoteInstr(plan, recv_fid);
	if (recv_instrs)
	{

		ExplainCommonRemoteInstrJson(recv_instrs, es, "Recv");
		if (es->verbose)
			ExplainRemoteWorkerInstrJson(recv_instrs, es, nodeTag(plan), true);

		DestroyRemoteInstr(recv_instrs);
	}
}
/*
 * ExplainRemoteWorkerInstr
 *
 * Explain remote worker's instrument.
 */
void
ExplainRemoteWorkerInstr(RemoteInstrumentation *instrs, ExplainState *es,
						 NodeTag plantag)
{
	int i;
	int n;
	WorkerInstrumentation *w_instrs;
	Instrumentation *instr;
	double nloops, startup_sec, total_sec, rows;

	for (i = 0; i < NumDataNodes; i++)
	{
		w_instrs = instrs[i].w_instrs;
		if (w_instrs == NULL)
			continue;

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "- %s\n", dn_names[i]);
		es->indent++;
		for (n = 0; n < w_instrs->num_workers; n++)
		{
			instr = &(w_instrs->instrument[n]);
			if (instr->nloops == 0)
				continue;

			nloops = instr->nloops;
			startup_sec = 1000.0 * instr->startup / nloops;
			total_sec = 1000.0 * instr->total / nloops;
			rows = instr->ntuples / nloops;

			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Worker %d: ", n);
			if (es->timing)
			{
					appendStringInfo(es->str,
									"actual time=%.3f..%.3f rows=%.0f loops=%.0f",
									startup_sec, total_sec, rows, nloops);
			}
			else
			{
					appendStringInfo(es->str,
									"actual rows=%.0f loops=%.0f",
									rows, nloops);
			}

			/* specific info */
			switch(plantag)
			{
				case T_Sort:
					if (instrs[i].state && instrs[i].state->num_workers > 0)
					{
						TuplesortInstrumentation *stat =
							&((RemoteSortState *)instrs[i].state)->w_stats[n];
						if (stat->sortMethod != SORT_TYPE_STILL_IN_PROGRESS)
						{
							appendStringInfo(es->str,
									"  Sort Method: %s  %s: %ldkB",
									tuplesort_method_name(stat->sortMethod),
									tuplesort_space_type_name(stat->spaceType),
									stat->spaceUsed);
						}
					}
					appendStringInfoChar(es->str, '\n');
					break;
				case T_Agg:
					if (instrs[i].state && instrs[i].state->num_workers > 0)
					{
						AggregateInstrumentation *stat =
							&((RemoteAggState *) instrs[i].state)->w_stats[n];
						if (stat->hash_mem_peak > 0)
						{
							es->query_mem += stat->hash_mem_peak;
							appendStringInfo(es->str,
								"  Batches: %d  Memory Usage: " INT64_FORMAT "kB",
								stat->hash_batches_used, stat->hash_mem_peak);
							if (stat->hash_batches_used > 1)
								appendStringInfo(es->str,
									"  Disk Usage: " UINT64_FORMAT "kB",
									stat->hash_disk_used);
						}
					}
					appendStringInfoChar(es->str, '\n');
					break;
				case T_RemoteSubplan:
					if (instrs[i].state && instrs[i].state->num_workers > 0)
					{
						FragmentInstrumentation *stat =
							&((RemoteFragState *) instrs[i].state)->w_stats[n];
						if (stat->executor_mem > 0 && es->summary)
						{
							appendStringInfo(es->str,
								"  Executor Memory: %ldkB  Send ",
								stat->executor_mem);
							es->query_mem += stat->executor_mem;
						}
						else
							appendStringInfo(es->str, "  Recv ");
						appendStringInfo(es->str,
							"Memory Usage: %ldkB", stat->fragment_mem);

						if (stat->total_pages > 0)
							appendStringInfo(es->str,
								"  Total Pages: %d", stat->total_pages);
						if (stat->disk_pages > 0)
							appendStringInfo(es->str,
								"  Spilled Pages: %d", stat->disk_pages);
					}
					appendStringInfo(es->str, "\n");
					break;
				default:
					appendStringInfoChar(es->str, '\n');
					break;
			}
		}
		es->indent--;
	}
}

/*
 * ExplainRemoteWorkerInstrJson
 *
 * Explain remote worker's instrument(JSON FORMAT).
 */
void
ExplainRemoteWorkerInstrJson(RemoteInstrumentation *instrs, ExplainState *es,
						 NodeTag plantag, bool isRecv)
{
	int i;
	int n;
	WorkerInstrumentation *w_instrs;
	Instrumentation *instr;
	double nloops, startup_sec, total_sec, rows, exec_enter, exec_start, exec_total;
	char tmp_name[20] = {0};
	if (isRecv)
		ExplainOpenGroup("Recv_worker_info", "Recv_worker_info", true, es);
	else
		ExplainOpenGroup("worker_info", "worker_info", true, es);
	for (i = 0; i < NumDataNodes; i++)
	{
		w_instrs = instrs[i].w_instrs;
		if (w_instrs == NULL)
			continue;

		ExplainOpenGroup(dn_names[i], dn_names[i], true, es);

		for (n = 0; n < w_instrs->num_workers; n++)
		{
			instr = &(w_instrs->instrument[n]);
			if (instr->nloops == 0)
				continue;

			nloops = instr->nloops;
			startup_sec = 1000.0 * instr->startup / nloops;
			total_sec = 1000.0 * instr->total / nloops;
			rows = instr->ntuples / nloops;
			exec_enter = 1000.0 * instr->exec_enter;
			exec_start = 1000.0 * instr->exec_firsttuple;
			exec_total = 1000.0 * instr->exec_total;
			if (es->timing)
			{
				pg_ltoa(n, tmp_name);
				ExplainOpenGroup(tmp_name, tmp_name, true, es);
				ExplainPropertyFloat("startup", NULL, startup_sec, 3, es);
				ExplainPropertyFloat("total", NULL, total_sec, 3, es);
				ExplainPropertyFloat("rows", NULL, rows, 0, es);
				ExplainPropertyFloat("exec_enter", NULL, exec_enter, 3, es);
				ExplainPropertyFloat("exec_startup", NULL, exec_start, 3, es);
				ExplainPropertyFloat("exec_total", NULL, exec_total, 3, es);
				ExplainPropertyFloat("loops", NULL, nloops, 0, es);
				ExplainCloseGroup(tmp_name, tmp_name, true, es);
			}
		}
		ExplainCloseGroup(dn_names[i], dn_names[i], true, es);
	}
	if (isRecv)
		ExplainCloseGroup("Recv_worker_info", "Recv_worker_info", true, es);
	else
		ExplainCloseGroup("worker_info", "worker_info", true, es);
}
