/*-------------------------------------------------------------------------
 * worker.c
 *	   PostgreSQL logical replication worker (apply)
 *
 * Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/worker.c
 *
 * NOTES
 *	  This file contains the worker which applies logical changes as they come
 *	  from remote logical replication stream.
 *
 *	  The main worker (apply) is started by logical replication worker
 *	  launcher for every enabled subscription in a database. It uses
 *	  walsender protocol to communicate with publisher.
 *
 *	  This module includes server facing code and shares libpqwalreceiver
 *	  module with walreceiver for providing the libpq specific functionality.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/xact.h"
#include "access/xlog_internal.h"

#include "catalog/namespace.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"

#include "commands/trigger.h"

#include "executor/executor.h"
#include "executor/nodeModifyTable.h"

#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_relation.h"

#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "postmaster/walwriter.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "replication/logicalworker.h"
#include "replication/reorderbuffer.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"

#include "rewrite/rewriteHandler.h"

#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"

#include "tcop/tcopprot.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timeout.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#ifdef __STORAGE_SCALABLE__
#include "replication/logical_statistic.h"
#endif
#ifdef __SUBSCRIPTION__
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#endif
#ifdef __OPENTENBASE_C__
#endif
#include "access/sysattr.h"
#include "access/hash.h"

#include "nodes/parsenodes.h"
#include "foreign/foreign.h"
#include "access/external.h"
#include "pgxc/pgxc.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "commands/defrem.h"
#include "executor/spi.h"

#define NAPTIME_PER_CYCLE 1000	/* max sleep time between cycles (1s) */

typedef struct FlushPosition
{
	dlist_node	node;
	XLogRecPtr	local_end;
	XLogRecPtr	remote_end;
} FlushPosition;

static dlist_head lsn_mapping = DLIST_STATIC_INIT(lsn_mapping);

typedef struct SlotErrCallbackArg
{
	LogicalRepRelation *rel;
	int			attnum;
} SlotErrCallbackArg;

static MemoryContext ApplyMessageContext = NULL;
static MemoryContext ChangesCacheContext = NULL;
MemoryContext ApplyContext = NULL;

WalReceiverConn *wrconn = NULL;

Subscription *MySubscription = NULL;
bool		MySubscriptionValid = false;

bool		in_remote_transaction = false;

static XLogRecPtr remote_final_lsn = InvalidXLogRecPtr;
static TimestampTz batch_remote_final_committime = 0;
static TimestampTz last_merge_time = 0;
static int logical_apply_batch_size = 0;
static bool in_batch_process_mode = false;
static bool cache_single_txn_ing = false;

static void send_feedback(XLogRecPtr recvpos, bool force, bool requestReply);

static void store_flush_position(XLogRecPtr remote_lsn);

static void maybe_reread_subscription(void);

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;

/*
 * internal (INSERT/DELETE) format for one change on CN.
 */
typedef struct ChangeEntry
{
	uint64 keys_hash;   /* hash key*/
	Oid nodeoid;        /* the execution node */
	int repeat;         /* We store multiple records from publisher on the same tuple here.*/
	StringInfo change_str;  /* tuple string in remote format from stream. */
} ChangeEntry;

/*
 * An inner format to represent operations to a table,
 * Which contains information about table,
 * batch insert operation and batch delete operation.
 */
typedef struct LogicalRepChangesCacheEntry
{
	LogicalRepRelId remoterelid;    /* hash key. */
	HTAB *MergedInsert;             /* key - tuplekey. value - LogicalRepTupleData */
	HTAB *MergedDelete;
} LogicalRepChangesCacheEntry;

/*
 * The hash table used to store batch changes.
 * The key is remote relid read from message stream,
 * the entry is "Change".
 */
static HTAB *LogicalRepChangesCache = NULL;

/*
 * should send all changes in HTAB to datanode now?
 */
static bool
should_send_cached_changes()
{
	return ((logical_apply_time_delay &&
	         TimestampDifferenceExceeds(last_merge_time, GetCurrentTimestamp(), logical_apply_time_delay * 1000))
	        || (logical_apply_cache_capacity && (logical_apply_batch_size >= logical_apply_cache_capacity)));
}
/*
 * Initialize LogicalRepChangesCache.
 */
static void
logicalrep_changes_cache_init(void)
{
	HASHCTL ctl;
	MemoryContext oldctx = NULL;
	
	if (LogicalRepChangesCache)
		return;
	
	if (!ChangesCacheContext)
		ChangesCacheContext = AllocSetContextCreate(ApplyContext,
		                                            "ChangesCacheContext",
		                                            ALLOCSET_DEFAULT_SIZES);
	oldctx = MemoryContextSwitchTo(ChangesCacheContext);
	
	/* Initialize the cache hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(LogicalRepRelId);
	ctl.entrysize = sizeof(LogicalRepChangesCacheEntry);
	ctl.hcxt = ChangesCacheContext;
	
	LogicalRepChangesCache = hash_create("logicalrep batch changes cache", 32, &ctl,
	                                          HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	MemoryContextSwitchTo(oldctx);
}

/*
 * Initialize MergedInsert or MergedDelete to store
 * "Merged" insert or delete changes.
 */
static HTAB *
logicalrep_merged_op_hash_init(void)
{
	HASHCTL ctl;
	HTAB *MergedOpHash = NULL;
	MemoryContext oldctx = NULL;
	
	Assert(ChangesCacheContext);
	oldctx = MemoryContextSwitchTo(ChangesCacheContext);
	
	/* Initialize the cache hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(ChangeEntry);
	ctl.hcxt = ChangesCacheContext;
	
	MergedOpHash = hash_create("logicalrep merged I/D changes cache for one rel", 32, &ctl,
	                           HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	MemoryContextSwitchTo(oldctx);
	return MergedOpHash;
}

/*
 * Should this worker apply changes for given relation.
 *
 * This is mainly needed for initial relation data sync as that runs in
 * separate worker process running in parallel and we need some way to skip
 * changes coming to the main apply worker during the sync of a table.
 *
 * Note we need to do smaller or equals comparison for SYNCDONE state because
 * it might hold position of end of initial slot consistent point WAL
 * record + 1 (ie start of next record) and next record can be COMMIT of
 * transaction we are now processing (which is what we set remote_final_lsn
 * to in apply_handle_begin).
 */
static bool
should_apply_changes_for_rel(LogicalRepRelMapEntry *rel)
{
#ifdef __SUBSCRIPTION__
	if (NULL == rel)
	{
		return false;
	}

	if (IS_PGXC_COORDINATOR)
	{
		if (rel->localrel != NULL)
		{
			/*
			 * OpenTenBase Subscripton on COORDINATOR currently only supports subscribing into the SHARD table
			 */
			if (false == RelationIsSharded(rel->localrel))
			{
				return false;
			}
		}
	}
#endif

	if (am_tablesync_worker())
#ifdef __STORAGE_SCALABLE__
	{
		Oid localreloid = rel->localreloid;
		if (!OidIsValid(localreloid))
		{
			if (rel->localrel)
			{
				localreloid = RelationGetRelid(rel->localrel);
			}

			if (!OidIsValid(localreloid))
			{
				elog(ERROR, "should_apply_changes_for_rel: invalid localreloid %u", localreloid);
			}
		}
		return MyLogicalRepWorker->relid == localreloid;
	}
#else
		return MyLogicalRepWorker->relid == rel->localreloid;
#endif
	else
		return (rel->state == SUBREL_STATE_READY ||
				(rel->state == SUBREL_STATE_SYNCDONE &&
				 rel->statelsn <= remote_final_lsn));
}

/*
 * Make sure that we started local transaction.
 *
 * Also switches to ApplyMessageContext as necessary.
 */
static bool
ensure_transaction(void)
{
	if (IsTransactionState())
	{
		SetCurrentStatementStartTimestamp();

		if (CurrentMemoryContext != ApplyMessageContext)
			MemoryContextSwitchTo(ApplyMessageContext);

		return false;
	}

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();

	maybe_reread_subscription();

	MemoryContextSwitchTo(ApplyMessageContext);
	return true;
}


/*
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *
create_estate_for_relation(LogicalRepRelMapEntry *rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel->localrel);
	rte->relkind = rel->localrel->rd_rel->relkind;
	estate->es_range_table = list_make1(rte);

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel->localrel, 1, NULL, 0, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	/* Triggers might need a slot */
	if (resultRelInfo->ri_TrigDesc)
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}

/*
 * Executes default values for columns for which we can't map to remote
 * relation columns.
 *
 * This allows us to support tables which have more columns on the downstream
 * than on the upstream.
 */
static void
slot_fill_defaults(LogicalRepRelMapEntry *rel, EState *estate,
				   TupleTableSlot *slot)
{
	TupleDesc	desc = RelationGetDescr(rel->localrel);
	int			num_phys_attrs = desc->natts;
	int			i;
	int			attnum,
				num_defaults = 0;
	int		   *defmap;
	ExprState **defexprs;
	ExprContext *econtext;

	econtext = GetPerTupleExprContext(estate);

	/* We got all the data via replication, no need to evaluate anything. */
	if (num_phys_attrs == rel->remoterel.natts)
		return;

	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 0; attnum < num_phys_attrs; attnum++)
	{
		Expr	   *defexpr;

		if (TupleDescAttr(desc, attnum)->attisdropped)
			continue;

		if (rel->attrmap[attnum] >= 0)
			continue;

		defexpr = (Expr *) build_column_default(rel->localrel, attnum + 1);

		if (defexpr != NULL)
		{
			/* Run the expression through planner */
			defexpr = expression_planner(defexpr);

			/* Initialize executable expression in copycontext */
			defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
			defmap[num_defaults] = attnum;
			num_defaults++;
		}

	}

	for (i = 0; i < num_defaults; i++)
		slot->tts_values[defmap[i]] =
			ExecEvalExpr(defexprs[i], econtext, &slot->tts_isnull[defmap[i]]);
}

/*
 * Error callback to give more context info about type conversion failure.
 */
static void
slot_store_error_callback(void *arg)
{
	SlotErrCallbackArg *errarg = (SlotErrCallbackArg *) arg;
	Oid			remotetypoid,
				localtypoid;

	if (errarg->attnum < 0)
		return;

	remotetypoid = errarg->rel->atttyps[errarg->attnum];
	localtypoid = logicalrep_typmap_getid(remotetypoid);
	errcontext("processing remote data for replication target relation \"%s.%s\" column \"%s\", "
			   "remote type %s, local type %s",
			   errarg->rel->nspname, errarg->rel->relname,
			   errarg->rel->attnames[errarg->attnum],
			   format_type_be(remotetypoid),
			   format_type_be(localtypoid));
}

/*
 * Store data in C string form into slot.
 * This is similar to BuildTupleFromCStrings but TupleTableSlot fits our
 * use better.
 */
static void
slot_store_cstrings(TupleTableSlot *slot, LogicalRepRelMapEntry *rel,
					char **values)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;
	SlotErrCallbackArg errarg;
	ErrorContextCallback errcallback;

	ExecClearTuple(slot);

	/* Push callback + info on the error context stack */
	errarg.rel = &rel->remoterel;
	errarg.attnum = -1;
	errcallback.callback = slot_store_error_callback;
	errcallback.arg = (void *) &errarg;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Call the "in" function for each non-dropped attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		int			remoteattnum = rel->attrmap[i];

		if (!att->attisdropped && remoteattnum >= 0 &&
			values[remoteattnum] != NULL)
		{
			Oid			typinput;
			Oid			typioparam;

			errarg.attnum = remoteattnum;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput,
													   values[remoteattnum],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			/*
			 * We assign NULL to dropped attributes, NULL values, and missing
			 * values (missing values should be later filled using
			 * slot_fill_defaults).
			 */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	ExecStoreVirtualTuple(slot);
}

/*
 * Modify slot with user data provided as C strings.
 * This is somewhat similar to heap_modify_tuple but also calls the type
 * input function on the user data as the input is the text representation
 * of the types.
 */
static void
slot_modify_cstrings(TupleTableSlot *slot, LogicalRepRelMapEntry *rel,
					 char **values, bool *replaces)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;
	SlotErrCallbackArg errarg;
	ErrorContextCallback errcallback;

	slot_getallattrs(slot);
	ExecClearTuple(slot);

	/* Push callback + info on the error context stack */
	errarg.rel = &rel->remoterel;
	errarg.attnum = -1;
	errcallback.callback = slot_store_error_callback;
	errcallback.arg = (void *) &errarg;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Call the "in" function for each replaced attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		int			remoteattnum = rel->attrmap[i];

		if (remoteattnum >= 0 && !replaces[remoteattnum])
			continue;

		if (remoteattnum >= 0 && values[remoteattnum] != NULL)
		{
			Oid			typinput;
			Oid			typioparam;

			errarg.attnum = remoteattnum;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput,
													   values[remoteattnum],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	ExecStoreVirtualTuple(slot);
}

#ifdef __SUBSCRIPTION__
/*
 * After obtaining the execution node information, 
 * we can obtain the connection of these execution nodes as follows:
 *
 * get_handles(exec_nodes->nodelist, NULL, false, true);
 */
static ExecNodes * 
apply_get_exec_nodes(LogicalRepRelMapEntry * rel,
						LogicalRepTupleData * tuple,
						RelationAccessType accessType)
{
	TupleDesc			desc = NULL;
	RelationLocInfo * 	rel_loc = NULL;
	Datum       *disValues = NULL;
	bool        *disIsNulls = NULL;
	ExecNodes * exec_nodes = NULL;

	desc = RelationGetDescr(rel->localrel);
	rel_loc = RelationGetLocInfo(rel->localrel);

	if (rel_loc->nDisAttrs > 0 && AttributeNumberIsValid(rel_loc->disAttrNums[0]))
	{
		int i = 0;
		disValues = (Datum *) palloc(sizeof(Datum) * rel_loc->nDisAttrs);
		disIsNulls = (bool *) palloc(sizeof(bool) * rel_loc->nDisAttrs);
		for (i = 0; i < rel_loc->nDisAttrs; i++)
		{
			AttrNumber attrNum = rel_loc->disAttrNums[i];
			Form_pg_attribute att = TupleDescAttr(desc, attrNum - 1);
			int remoteattnum = rel->attrmap[attrNum - 1];
			
			if (!att->attisdropped && remoteattnum >= 0 &&
			    tuple->values[remoteattnum] != NULL)
			{
				Oid typinput = InvalidOid;
				Oid typioparam = InvalidOid;
				
				getTypeInputInfo(att->atttypid, &typinput, &typioparam);
				disValues[i] = OidInputFunctionCall(typinput,
				                                    tuple->values[remoteattnum],
				                                    typioparam,
				                                    att->atttypmod);
				disIsNulls[i] = false;
			}
			else
			{
				disValues[i] = (Datum) 0;
				disIsNulls[i] = true;
			}
		}
	}

	exec_nodes = GetRelationNodes(rel_loc,
								  disValues, disIsNulls, rel_loc->nDisAttrs,
								  accessType);
	if (disValues)
		pfree(disValues);
	if (disIsNulls)
		pfree(disIsNulls);
	return exec_nodes;
}

/*
 * send apply message to datanode and wait response
 */
static void
apply_exec_on_nodes(StringInfo s, ExecNodes * exec_nodes)
{
	PGXCNodeAllHandles * all_handles = NULL;
	int i = 0, result = 0;
	bool ignore_pk_conflict = false;
	ResponseCombiner combiner;

	if (exec_nodes == NULL)
		return;

	/* send apply message to DN and wait response */
	all_handles = get_handles(exec_nodes->nodeList, NIL, false, true, FIRST_LEVEL, InvalidFid, true, false);
	ignore_pk_conflict = MySubscription->ignore_pk_conflict;

	for (i = 0; i < all_handles->dn_conn_count; i++)
	{
		Assert(all_handles->datanode_handles[i]->sock != PGINVALID_SOCKET);
		if (!(all_handles->datanode_handles[i]->sock != PGINVALID_SOCKET)) abort();
		
		if (pgxc_node_send_apply(all_handles->datanode_handles[i], s->data, s->len, ignore_pk_conflict))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("apply_exec_on_nodes sending apply fails")));
		}
	}

	InitResponseCombiner(&combiner, all_handles->dn_conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	result = pgxc_node_receive_responses(all_handles->dn_conn_count, all_handles->datanode_handles, NULL, &combiner);
	if (result)
	{
		if (combiner.errorMessage)
		{
			pgxc_node_report_error(&combiner);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Failed to APPLY the tuple on one or more datanodes")));
		}
		result = EOF;
	}
	else if (ignore_pk_conflict &&
				combiner.errorMessage &&
				MAKE_SQLSTATE(combiner.errorCode[0], combiner.errorCode[1],
							  combiner.errorCode[2], combiner.errorCode[3],
							  combiner.errorCode[4]) == ERRCODE_UNIQUE_VIOLATION)
	{
		if (combiner.errorDetail && combiner.errorHint)
        {
			elog(LOG, "logical apply found that %s, %s, %s",
						combiner.errorMessage,
						combiner.errorDetail,
						combiner.errorHint);
        }
        else if (combiner.errorDetail)
        {
            elog(LOG, "logical apply found that %s, %s",
						combiner.errorMessage,
						combiner.errorDetail);
        }
        else
        {
            elog(LOG, "logical apply found that %s",
						combiner.errorMessage);
        }
	}
	else if (!validate_combiner(&combiner))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("apply_exec_on_nodes validate_combiner responese of APPLY failed")));
		
	}

	CloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);
}

static bool
check_if_ican_apply(int32 tuple_hash)
{
	if (MySubscription->is_all_actived)
	{
		if (tuple_hash != 0)
		{
			Assert(MySubscription->parallel_number > 1);
			if (!(MySubscription->parallel_number > 1)) abort();
		}
		else
		{
			Assert(MySubscription->parallel_index == 0);
			if (!(MySubscription->parallel_index == 0)) abort();
		}

		if (MySubscription->active_lsn < MyLogicalRepWorker->last_lsn)
		{
			if (MySubscription->parallel_index == tuple_hash)
			{
				return true;
			}
		}
	}
	else
	{
		Assert(MySubscription->parallel_index == 0);
		if (!(MySubscription->parallel_index == 0)) abort();

		return true;
	}

	return false;
}

#endif

/*
 * Handle BEGIN message.
 */
static void
apply_handle_begin(StringInfo s)
{
	LogicalRepBeginData begin_data;

	logicalrep_read_begin(s, &begin_data);

	remote_final_lsn = begin_data.final_lsn;

	in_remote_transaction = true;

	pgstat_report_activity(STATE_RUNNING, NULL);
}

/*
 * Handle COMMIT message.
 *
 * TODO, support tracking of multiple origins
 */
static void
apply_handle_commit(StringInfo s)
{
	LogicalRepCommitData commit_data;

	logicalrep_read_commit(s, &commit_data);

	Assert(commit_data.commit_lsn == remote_final_lsn);

	/* The synchronization worker runs in single transaction. */
	if (IsTransactionState() && !am_tablesync_worker())
	{
#ifdef __SUBSCRIPTION__
		if (am_opentenbase_subscription_dispatch_worker())
		{
			TransactionId xid = GetTopTransactionIdIfAny();
			bool markXidCommitted = TransactionIdIsValid(xid);

			if (!markXidCommitted)
			{
				/* Force to request a xid to write a COMMIT log */
				GetCurrentTransactionId();
			}
		}
#endif
		/*
		 * Update origin state so we can restart streaming from correct
		 * position in case of crash.
		 */
		replorigin_session_origin_lsn = commit_data.end_lsn;
		replorigin_session_origin_timestamp = commit_data.committime;

		CommitTransactionCommand();
		pgstat_report_stat(false);

		store_flush_position(commit_data.end_lsn);
	}
	else
	{
		/* Process any invalidation messages that might have accumulated. */
		AcceptInvalidationMessages();
		maybe_reread_subscription();
	}

	in_remote_transaction = false;

	/* Process any tables that are being synchronized in parallel. */
	process_syncing_tables(commit_data.end_lsn);

	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Handle ORIGIN message.
 *
 * TODO, support tracking of multiple origins
 */
static void
apply_handle_origin(StringInfo s)
{
	/*
	 * ORIGIN message can only come inside remote transaction and before any
	 * actual writes.
	 */
	if (!in_remote_transaction ||
		(IsTransactionState() && !am_tablesync_worker()))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("ORIGIN message sent out of order")));
}

/*
 * Handle RELATION message.
 *
 * Note we don't do validation against local schema here. The validation
 * against local schema is postponed until first change for given relation
 * comes as we only care about it when applying changes for it anyway and we
 * do less locking this way.
 */
static void
apply_handle_relation(StringInfo s)
{
	LogicalRepRelation *rel = NULL;

	rel = logicalrep_read_rel(s);

#ifdef __SUBSCRIPTION__
	/*
	 * Perform a check on the table structure of the local table and the remote table.
	 * Only the table structure is identical, the logical replication can be performed.
	 */
	do
	{
		Relation 	localrel = NULL;
		TupleDesc	desc = NULL;
		int			natts = 0, nliveatts = 0;
		int			i = 0;
		RangeVar * 	local_relname = NULL;

		ensure_transaction();

		local_relname = makeRangeVar(rel->nspname, rel->relname, -1);

		if (am_opentenbase_subscription_dispatch_worker())
		{
			Oid	local_relid = InvalidOid;

			local_relid = RangeVarGetRelid(local_relname, NoLock, true);
			if (!OidIsValid(local_relid))
			{
				elog(LOG, "The subscriber cannot find the table name received from the publisher locally, ignoring the subscription for %s.%s.",
					rel->nspname, rel->relname);

				pfree(local_relname);
				logicalrep_relation_free(rel);

				return;
			}
		}

		localrel = relation_openrv(makeRangeVar(rel->nspname, rel->relname, -1), NoLock);
		desc = RelationGetDescr(localrel);
		natts = desc->natts;

		/* Check for supported relkind. */
		CheckSubscriptionRelkind(localrel->rd_rel->relkind, rel->nspname, rel->relname);

		for (i = 0; i < desc->natts; i++)
		{
			if (TupleDescAttr(desc, i)->attisdropped)
			{
				continue;
			}
			nliveatts++;
		}

		if (nliveatts != rel->natts)
		{
			heap_close(localrel, NoLock);
			ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("The number of attributes between the remote table \"%s.%s\" and the local table \"%s.%s\" is different",
				 		rel->nspname, rel->relname, rel->nspname, rel->relname)));
			return;
		}

		nliveatts = -1;
		for (i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (att->attisdropped)
				continue;
			
			nliveatts++;
			
			if (pg_strcasecmp(rel->attnames[nliveatts], NameStr(att->attname)) != 0)
			{
				heap_close(localrel, NoLock);
				ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("The %d th attribute name between the remote table \"%s.%s\" and the local table \"%s.%s\" is different",
					 		nliveatts, rel->nspname, rel->relname, rel->nspname, rel->relname)));
				return;
			}

			if (rel->atttyps[nliveatts] != att->atttypid)
			{
				heap_close(localrel, NoLock);
				ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("The %d th attribute type between the remote table \"%s.%s\" and the local table \"%s.%s\" is different",
					 		nliveatts, rel->nspname, rel->relname, rel->nspname, rel->relname)));
				return;
			}
		}

		heap_close(localrel, NoLock);
		pfree(local_relname);
	} while (0);
#endif

	logicalrep_relmap_update(rel);
}

/*
 * Handle TYPE message.
 *
 * Note we don't do local mapping here, that's done when the type is
 * actually used.
 */
static void
apply_handle_type(StringInfo s)
{
	LogicalRepTyp typ;

	logicalrep_read_typ(s, &typ);
	logicalrep_typmap_update(&typ);
}

/*
 * Get replica identity index or if it is not defined a primary key.
 *
 * If neither is defined, returns InvalidOid
 */
Oid
GetRelationIdentityOrPK(Relation rel)
{
	Oid			idxoid;

	idxoid = RelationGetReplicaIndex(rel);

	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(rel);

	return idxoid;
}

/*
 * Handle INSERT message.
 */
static void
apply_handle_insert(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepTupleData newtup;
	LogicalRepRelId relid;
	EState	   *estate;
	TupleTableSlot *remoteslot;
	MemoryContext oldctx;

	ensure_transaction();

	relid = logicalrep_read_insert(s, NULL, NULL, NULL, &newtup, NULL);
	rel = logicalrep_rel_open(relid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(rel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(rel, RowExclusiveLock);
		return;
	}

#ifdef __SUBSCRIPTION__
	if (IS_PGXC_COORDINATOR &&
		rel->localrel != NULL &&
		rel->localrel->rd_locator_info != NULL &&
		IsRelationColumnDistributed(rel->localrel->rd_locator_info))
	{
		if (check_if_ican_apply(newtup.tuple_hash))
		{
			ExecNodes * exec_nodes = NULL;

			/* send insert to DN and wait exec finish */
			exec_nodes = apply_get_exec_nodes(rel, &newtup, RELATION_ACCESS_INSERT);
			apply_exec_on_nodes(s, exec_nodes);
			FreeExecNodes(&exec_nodes);
		}

		logicalrep_rel_close(rel, NoLock);
		CommandCounterIncrement();
		return;
	}
#endif

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel->localrel));

	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_cstrings(remoteslot, rel, newtup.values);
	slot_fill_defaults(rel, estate, remoteslot);
	MemoryContextSwitchTo(oldctx);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Do the insert. */
	ExecSimpleRelationInsert(estate, remoteslot);

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);

	logicalrep_rel_close(rel, NoLock);

	CommandCounterIncrement();

#ifdef __STORAGE_SCALABLE__
	rel->ntups_insert++;

	GetSubTableEntry(MySubscription->oid, rel->localreloid, &rel->ent, CMD_INSERT);
#endif
}

/*
 * Check if the logical replication relation is updatable and throw
 * appropriate error if it isn't.
 */
static void
check_relation_updatable(LogicalRepRelMapEntry *rel)
{
	/* Updatable, no error. */
	if (rel->updatable)
		return;

	/*
	 * We are in error mode so it's fine this is somewhat slow. It's better to
	 * give user correct error.
	 */
	if (OidIsValid(GetRelationIdentityOrPK(rel->localrel)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("publisher does not send replica identity column "
						"expected by the logical replication target relation \"%s.%s\"",
						rel->remoterel.nspname, rel->remoterel.relname)));
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("logical replication target relation \"%s.%s\" has "
					"neither REPLICA IDENTITY index nor PRIMARY "
					"KEY and published relation does not have "
					"REPLICA IDENTITY FULL",
					rel->remoterel.nspname, rel->remoterel.relname)));
}

/*
 * Handle UPDATE message.
 *
 * TODO: FDW support
 */
static void
apply_handle_update(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepRelId relid;
	Oid			idxoid;
	EState	   *estate;
	EPQState	epqstate;
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool		has_oldtup;
	TupleTableSlot *localslot;
	TupleTableSlot *remoteslot;
	bool		found;
	MemoryContext oldctx;

	ensure_transaction();

	relid = logicalrep_read_update(s, NULL, NULL, NULL,
								   &has_oldtup, &oldtup,
								   &newtup, NULL, NULL);
	rel = logicalrep_rel_open(relid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(rel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(rel, RowExclusiveLock);
		return;
	}

	/* Check if we can do the update. */
	check_relation_updatable(rel);

#ifdef __SUBSCRIPTION__
	if (IS_PGXC_COORDINATOR &&
		rel->localrel != NULL &&
		rel->localrel->rd_locator_info != NULL &&
		IsRelationColumnDistributed(rel->localrel->rd_locator_info))
	{
		if (check_if_ican_apply(has_oldtup ? oldtup.tuple_hash : newtup.tuple_hash))
		{
			ExecNodes * exec_nodes = NULL;

			/* send update to DN and wait exec finish */
			exec_nodes = apply_get_exec_nodes(rel, has_oldtup ? &oldtup : &newtup,
												RELATION_ACCESS_UPDATE);
			apply_exec_on_nodes(s, exec_nodes);
			FreeExecNodes(&exec_nodes);
		}

		logicalrep_rel_close(rel, NoLock);

		CommandCounterIncrement();
		return;
	}
#endif

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel->localrel));
	localslot = ExecInitExtraTupleSlot(estate,
									   RelationGetDescr(rel->localrel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Build the search tuple. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_cstrings(remoteslot, rel,
						has_oldtup ? oldtup.values : newtup.values);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel->localrel);
	Assert(OidIsValid(idxoid) ||
		   (rel->remoterel.replident == REPLICA_IDENTITY_FULL && has_oldtup));

	if (OidIsValid(idxoid))
	{
		found = RelationFindReplTupleByIndex(rel->localrel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel->localrel, LockTupleExclusive,
										 remoteslot, localslot);
	}

	ExecClearTuple(remoteslot);

	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		ExecStoreHeapTuple(localslot->tts_tuple, remoteslot, false);
		slot_modify_cstrings(remoteslot, rel, newtup.values, newtup.changed);
		MemoryContextSwitchTo(oldctx);

		EvalPlanQualSetSlot(&epqstate, remoteslot);

		/* Do the actual update. */
		ExecSimpleRelationUpdate(estate, &epqstate, localslot, remoteslot);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * TODO what to do here, change the log level to LOG perhaps?
		 */
		elog(DEBUG1,
			 "logical replication did not find row for update "
			 "in replication target relation \"%s\"",
			 RelationGetRelationName(rel->localrel));
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);

	logicalrep_rel_close(rel, NoLock);

	CommandCounterIncrement();

#ifdef __STORAGE_SCALABLE__
	if (found)
	{
		rel->ntups_insert++;
		rel->ntups_delete++;

		GetSubTableEntry(MySubscription->oid, rel->localreloid, &rel->ent, CMD_UPDATE);
	}
#endif
}

/*
 * Handle DELETE message.
 *
 * TODO: FDW support
 */
static void
apply_handle_delete(StringInfo s)
{
	LogicalRepRelMapEntry *rel;
	LogicalRepTupleData oldtup;
	LogicalRepRelId relid;
	Oid			idxoid;
	EState	   *estate;
	EPQState	epqstate;
	TupleTableSlot *remoteslot;
	TupleTableSlot *localslot;
	bool		found;
	MemoryContext oldctx;

	ensure_transaction();

	relid = logicalrep_read_delete(s, NULL, NULL, NULL, &oldtup, NULL);
	rel = logicalrep_rel_open(relid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(rel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(rel, RowExclusiveLock);
		return;
	}

	/* Check if we can do the delete. */
	check_relation_updatable(rel);

#ifdef __SUBSCRIPTION__
	if (IS_PGXC_COORDINATOR &&
		rel->localrel != NULL &&
		rel->localrel->rd_locator_info != NULL &&
		IsRelationColumnDistributed(rel->localrel->rd_locator_info))
	{
		if (check_if_ican_apply(oldtup.tuple_hash))
		{
			ExecNodes * exec_nodes = NULL;

			/* send delete to DN and wait exec finish */
			exec_nodes = apply_get_exec_nodes(rel, &oldtup, RELATION_ACCESS_UPDATE);
			apply_exec_on_nodes(s, exec_nodes);
			FreeExecNodes(&exec_nodes);
		}

		logicalrep_rel_close(rel, NoLock);

		CommandCounterIncrement();
		return;
	}
#endif

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel);
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel->localrel));
	localslot = ExecInitExtraTupleSlot(estate,
									   RelationGetDescr(rel->localrel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Find the tuple using the replica identity index. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_cstrings(remoteslot, rel, oldtup.values);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel->localrel);
	Assert(OidIsValid(idxoid) ||
		   (rel->remoterel.replident == REPLICA_IDENTITY_FULL));

	if (OidIsValid(idxoid))
	{
		found = RelationFindReplTupleByIndex(rel->localrel, idxoid,
											 LockTupleExclusive,
											 remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel->localrel, LockTupleExclusive,
										 remoteslot, localslot);
	}
	/* If found delete it. */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);

		/* Do the actual delete. */
		ExecSimpleRelationDelete(estate, &epqstate, localslot);
	}
	else
	{
		/* The tuple to be deleted could not be found. */
		ereport(DEBUG1,
				(errmsg("logical replication could not find row for delete "
						"in replication target %s",
						RelationGetRelationName(rel->localrel))));
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);

	logicalrep_rel_close(rel, NoLock);

	CommandCounterIncrement();

#ifdef __STORAGE_SCALABLE__
	if (found)
	{
		rel->ntups_delete++;

		GetSubTableEntry(MySubscription->oid, rel->localreloid, &rel->ent, CMD_DELETE);
	}
#endif
}


/*
 * Logical replication protocol message dispatcher.
 */
static void
apply_dispatch(StringInfo s)
{
	char		action = pq_getmsgbyte(s);

#ifdef _PUB_SUB_RELIABLE_
	wal_set_internal_stream();
#endif

	switch (action)
	{
			/* BEGIN */
		case 'B':
			apply_handle_begin(s);
			break;
			/* COMMIT */
		case 'C':
			apply_handle_commit(s);
			break;
			/* INSERT */
		case 'I':
			apply_handle_insert(s);
			break;
			/* UPDATE */
		case 'U':
			apply_handle_update(s);
			break;
			/* DELETE */
		case 'D':
			apply_handle_delete(s);
			break;
			/* RELATION */
		case 'R':
			apply_handle_relation(s);
			break;
			/* TYPE */
		case 'Y':
			apply_handle_type(s);
			break;
			/* ORIGIN */
		case 'O':
			apply_handle_origin(s);
			break;
		default:
			{
				#ifdef _PUB_SUB_RELIABLE_
				wal_reset_stream();
				#endif

				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid logical replication message type %c", action)));
			}
	}

#ifdef _PUB_SUB_RELIABLE_
	wal_reset_stream();
#endif
}

/* Batch Process Begin. */
/*
 * Calculate hash value of this tuple as key of MergedInsert/MergedDelete.
 * Columns with default value may be null here.
*/
static uint64
logicalrep_calc_keys_hash(char **values, Relation rel)
{
	Bitmapset  *idattrs = NULL;
	bool		replidentfull = false;
	TupleDesc	desc = RelationGetDescr(rel);
	int			attnum = 0;
	uint64		sum = 0;
	
	/* fetch bitmap of REPLICATION IDENTITY attributes */
	replidentfull = (rel->rd_rel->relreplident == REPLICA_IDENTITY_FULL);
	if (!replidentfull)
		idattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
	
	/* travers the attributes */
	for (attnum = 0; attnum < desc->natts; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, attnum);
		
		if (att->attisdropped == true || values[attnum] == NULL)
			continue;
		
		/* REPLICA IDENTITY FULL means all columns are sent as part of key. */
		if (replidentfull ||
		    bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber,
		                  idattrs))
		{
			Datum	attr_hash_datum = hash_any((const unsigned char *) values[attnum], strlen(values[attnum]));
			uint32	attr_hash_uint32 = DatumGetUInt32(attr_hash_datum);
			sum += attr_hash_uint32;
		}
	}
	
	return sum;
}

/*
 * Cache INSERT message.
 */
static void
apply_cache_insert(StringInfo s)
{
	LogicalRepRelMapEntry *reprel;
	LogicalRepTupleData newtup;
	LogicalRepRelId remoterelid;
	MemoryContext oldctx = NULL;
	ChangeEntry *change_entry = NULL;
	bool found = false;
	uint64 keys_hash = (uint64) 0;
	int nodeidx = -1;
	int new_tuple_begin_cursor = -1;
	LogicalRepChangesCacheEntry *changes_cache_entry = NULL;
	
	ensure_transaction();
	
	/* 1. read remoterelid and newtuple from stream. */
	remoterelid = logicalrep_read_insert(s, NULL, NULL, NULL, &newtup, &new_tuple_begin_cursor);
	reprel = logicalrep_rel_open(remoterelid, NoLock);
	
	if (!should_apply_changes_for_rel(reprel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(reprel, NoLock);
		return;
	}
	
	if (!check_if_ican_apply(newtup.tuple_hash))
	{
		return;
	}
	
	/* 2. compute where this change should be sent to. */
	if (reprel->localrel != NULL &&
	    reprel->localrel->rd_locator_info != NULL &&
	    IsRelationColumnDistributed(reprel->localrel->rd_locator_info))
	{
		ExecNodes *exec_nodes = NULL;
		
		exec_nodes = apply_get_exec_nodes(reprel, &newtup, RELATION_ACCESS_INSERT);
		Assert(list_length(exec_nodes->nodeList) == 1);
		nodeidx = lfirst_int(list_head(exec_nodes->nodeList));
		FreeExecNodes(&exec_nodes);
	}
	else
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
				        errmsg("something wrong. \"%s.%s\"",
				               reprel->remoterel.nspname, reprel->remoterel.relname)));
	}
	
	/* 3. Find batch changes cache entry for this rel. */
	oldctx = MemoryContextSwitchTo(ChangesCacheContext);
	
	changes_cache_entry = (LogicalRepChangesCacheEntry *) hash_search(LogicalRepChangesCache,
	                                                                  (const void *) &remoterelid,
	                                                                  HASH_ENTER,
	                                                                  &found);
	if (!found)
	{
		/* rel changes cache wasn't found. Initialize the new buffer as empty. */
		changes_cache_entry->MergedInsert = logicalrep_merged_op_hash_init();
		changes_cache_entry->MergedDelete = logicalrep_merged_op_hash_init();
	}
	
	/* 4. Add this insert-change into MergedInsert. */
	keys_hash = logicalrep_calc_keys_hash(newtup.values, reprel->localrel);
	
	change_entry = (ChangeEntry *) hash_search(changes_cache_entry->MergedInsert,
	                                           (const void *) &keys_hash,
	                                           HASH_ENTER,
	                                           &found);
	if (found)
	{
		Assert(change_entry->repeat >= 1);
		Assert(change_entry->nodeoid == nodeidx);
		change_entry->repeat ++;
	}
	else
	{
		Assert(new_tuple_begin_cursor != -1);
		change_entry->change_str = makeStringInfo();
		change_entry->repeat = 1;
		change_entry->nodeoid = PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE);
		appendBinaryStringInfo(change_entry->change_str, s->data + new_tuple_begin_cursor, s->cursor - new_tuple_begin_cursor);
		ereport(DEBUG5,
		        (errmsg("add a new insert change into cache, values(%ld)", change_entry->keys_hash)));
	}
	MemoryContextSwitchTo(oldctx);
	
	logicalrep_rel_close(reprel, NoLock);
}

/*
 * Cache UPDATE message.
 */
static void
apply_cache_update(StringInfo s)
{
	LogicalRepRelMapEntry *reprel;
	LogicalRepRelId remoterelid;
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool		has_oldtup;
	bool		found;
	MemoryContext oldctx;
	LogicalRepChangesCacheEntry *changes_cache_entry = NULL;
	ChangeEntry *change_entry = NULL;
	int old_tuple_begin_cursor = -1;
	int new_tuple_begin_cursor = -1;
	int old_nodeidx = -1;
	int new_nodeidx = -1;
	uint64 new_keys_hash = (uint64) 0;
	uint64 old_keys_hash = (uint64) 0;
	
	ensure_transaction();
	
	/* 1. read rel and tuple from string stream. */
	remoterelid = logicalrep_read_update(s, NULL, NULL, NULL,
	                                     &has_oldtup, &oldtup,
	                                     &newtup, &old_tuple_begin_cursor, &new_tuple_begin_cursor);
	
	reprel = logicalrep_rel_open(remoterelid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(reprel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(reprel, RowExclusiveLock);
		return;
	}
	
	if (!check_if_ican_apply(has_oldtup ? oldtup.tuple_hash : newtup.tuple_hash))
	{
		logicalrep_rel_close(reprel, RowExclusiveLock);
		return;
	}
	
	/* Check if we can do the update. */
	check_relation_updatable(reprel);
	
	/* 2. obtain the execution node information. */
	if (reprel->localrel != NULL &&
	    reprel->localrel->rd_locator_info != NULL &&
	    IsRelationColumnDistributed(reprel->localrel->rd_locator_info))
	{
		ExecNodes *exec_nodes = NULL;
		
		/* old_tuple */
		exec_nodes = apply_get_exec_nodes(reprel, has_oldtup ? &oldtup : &newtup,
		                                  RELATION_ACCESS_UPDATE);
		Assert(list_length(exec_nodes->nodeList) == 1);
		old_nodeidx = lfirst_int(list_head(exec_nodes->nodeList));
		FreeExecNodes(&exec_nodes);
		
		/* new_tuple */
		exec_nodes = apply_get_exec_nodes(reprel, &newtup,
		                                  RELATION_ACCESS_UPDATE);
		Assert(list_length(exec_nodes->nodeList) == 1);
		new_nodeidx = lfirst_int(list_head(exec_nodes->nodeList));
		FreeExecNodes(&exec_nodes);
	}
	else
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
				        errmsg("something wrong. \"%s.%s\"",
				               reprel->remoterel.nspname, reprel->remoterel.relname)));
	}
	
	/* 3. Add the "update" change into LogicalRepChangesCache. */
	oldctx = MemoryContextSwitchTo(ChangesCacheContext);
	
	/* 3.1 Find batch changes cache entry for this rel. */
	changes_cache_entry = (LogicalRepChangesCacheEntry *) hash_search(LogicalRepChangesCache,
	                                                                  (const void*) &remoterelid,
	                                                                  HASH_ENTER,
	                                                                  &found);
	if (!found)
	{
		ereport(DEBUG5,
		        (errmsg("init LogicalRepChangesCache %s, remoterelid %d",
		                RelationGetRelationName(reprel->localrel), remoterelid)));
		changes_cache_entry->MergedInsert = logicalrep_merged_op_hash_init();
		changes_cache_entry->MergedDelete = logicalrep_merged_op_hash_init();
	}
	
	/* 3.2
	 * Check if there is already an insert-change on this oldtuple. If so, remove it or num - 1.
	 * If not, add a delete-change.
	 */
	if (has_oldtup)
	{
		Assert(old_tuple_begin_cursor != -1);
		old_keys_hash = logicalrep_calc_keys_hash(oldtup.values, reprel->localrel);
		change_entry = (ChangeEntry *) hash_search(changes_cache_entry->MergedInsert,
		                                           (void *) &old_keys_hash,
		                                           HASH_FIND,
		                                           &found);
		
		if (found)
		{
			/* 3.2.1 delete insert change. */
			if (change_entry->repeat == 1)
			{
				hash_search(changes_cache_entry->MergedInsert,
				            (void *) &old_keys_hash,
				            HASH_REMOVE,
				            NULL);
				ereport(DEBUG5,
				        (errmsg("remove an insert change into cache, values(%ld)",
				                change_entry->keys_hash)));
			}
			else if (change_entry->repeat > 1)
			{
				/* There is already more than one insert-changes on this rep-key, */
				change_entry->repeat--;
			}
			else
			{
				ereport(ERROR,
				        (errcode(ERRCODE_INTERNAL_ERROR),
						        errmsg("get wrong num of insert changes.")));
			}
		}
		else
		{
			/* 3.2.2 or add delete change. */
			change_entry = (ChangeEntry *) hash_search(changes_cache_entry->MergedDelete,
			                                           (void *) &old_keys_hash,
			                                           HASH_ENTER,
			                                           &found);
			
			Assert(old_nodeidx != -1);
			if (found)
			{
				Assert(change_entry->repeat >= 1);
				change_entry->repeat ++;
			}
			else
			{
				change_entry->change_str = makeStringInfo();
				change_entry->nodeoid = PGXCNodeGetNodeOid(old_nodeidx, PGXC_NODE_DATANODE);
				change_entry->repeat = 1;
				appendBinaryStringInfo(change_entry->change_str, s->data + old_tuple_begin_cursor, new_tuple_begin_cursor - old_tuple_begin_cursor - 1); /* char 'N'. */
				ereport(DEBUG5,
				        (errmsg("add a new delete change into cache, values(%ld)", change_entry->keys_hash)));
			}
		}
	}
	
	/* 3.3 add an insert-change on new_tuple into htab. */
	new_keys_hash = logicalrep_calc_keys_hash(newtup.values, reprel->localrel);
	
	change_entry = (ChangeEntry *) hash_search(changes_cache_entry->MergedInsert,
	                                           (const void*) &new_keys_hash,
	                                           HASH_ENTER,
	                                           &found);
	
	/* HINT: there may be more than one insert-changes on the same new_tuple. */
	if (found)
	{
		Assert(change_entry->repeat >= 1);
		change_entry->repeat ++;
	}
	else
	{
		Assert(new_tuple_begin_cursor != -1);
		change_entry->change_str = makeStringInfo();
		change_entry->nodeoid = PGXCNodeGetNodeOid(new_nodeidx, PGXC_NODE_DATANODE);
		change_entry->repeat = 1;
		appendBinaryStringInfo(change_entry->change_str, s->data + new_tuple_begin_cursor, s->cursor - new_tuple_begin_cursor);
		ereport(DEBUG5,
		        (errmsg("add a new insert change into cache, values(%ld)", change_entry->keys_hash)));
	}
	MemoryContextSwitchTo(oldctx);
	
	logicalrep_rel_close(reprel, RowExclusiveLock);
}

/*
 * Cache DELETE message.
 */
static void
apply_cache_delete(StringInfo s)
{
	LogicalRepRelMapEntry *reprel;
	LogicalRepTupleData oldtup;
	LogicalRepRelId remoterelid;
	bool found = false;
	MemoryContext oldctx = NULL;
	LogicalRepChangesCacheEntry *changes_cache_entry = NULL;
	ChangeEntry *change_entry = NULL;
	int old_tuple_begin_cursor = -1;
	int nodeidx = -1;
	uint64 keys_hash = (uint64) 0;
	
	ensure_transaction();
	
	/* 1. read rel and tuple from string stream. */
	remoterelid = logicalrep_read_delete(s, NULL, NULL, NULL, &oldtup, &old_tuple_begin_cursor);
	reprel = logicalrep_rel_open(remoterelid, RowExclusiveLock);
	if (!should_apply_changes_for_rel(reprel))
	{
		/*
		 * The relation can't become interesting in the middle of the
		 * transaction so it's safe to unlock it.
		 */
		logicalrep_rel_close(reprel, RowExclusiveLock);
		return;
	}
	
	if (!check_if_ican_apply(oldtup.tuple_hash))
	{
		logicalrep_rel_close(reprel, NoLock);
		return;
	}
	
	/* Check if we can do the delete. */
	check_relation_updatable(reprel);
	
	/* 2. compute where this change should be sent to. */
	if (reprel->localrel != NULL &&
	    reprel->localrel->rd_locator_info != NULL &&
	    IsRelationColumnDistributed(reprel->localrel->rd_locator_info))
	{
		ExecNodes *exec_nodes = NULL;
		
		exec_nodes = apply_get_exec_nodes(reprel, &oldtup, RELATION_ACCESS_UPDATE);
		Assert(list_length(exec_nodes->nodeList) == 1);
		nodeidx = lfirst_int(list_head(exec_nodes->nodeList));
		FreeExecNodes(&exec_nodes);
	}
	else
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
				        errmsg("something wrong. \"%s.%s\"",
				               reprel->remoterel.nspname, reprel->remoterel.relname)));
	}
	
	/* 3. Find changescache entry for the relation */
	oldctx = MemoryContextSwitchTo(ChangesCacheContext);
	
	changes_cache_entry = (LogicalRepChangesCacheEntry *) hash_search(LogicalRepChangesCache,
	                                                                  (const void *) &remoterelid,
	                                                                  HASH_ENTER,
	                                                                  &found);
	if (!found)
	{
		/* rel changes cache wasn't found. Initialize the new buffer as empty. */
		changes_cache_entry->MergedInsert = logicalrep_merged_op_hash_init();
		changes_cache_entry->MergedDelete = logicalrep_merged_op_hash_init();
	}
	
	/* 3.1 check if there is already an insert change with same keys_hash in MergedInsert. */
	keys_hash = logicalrep_calc_keys_hash(oldtup.values, reprel->localrel);
	
	change_entry = (ChangeEntry *) hash_search(changes_cache_entry->MergedInsert,
	                                           (const void *) &keys_hash,
	                                           HASH_FIND,
	                                           &found);
	if (found)
	{
		/* 3.1.1 remove the insert-change from MergedInsert. */
		Assert(change_entry->repeat >= 1);
		if (change_entry->repeat == 1)
		{
			(void) hash_search(changes_cache_entry->MergedInsert,
			                   (const void *) &keys_hash,
			                   HASH_REMOVE,
			                   &found);
			Assert(found);
		}
		else if (change_entry->repeat > 1)
		{
			change_entry->repeat--;
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR),
					        errmsg("get wrong num of insert changes.")));
		}
	}
	else
	{
		/* 3.1.2
		 * if there is no insert changes on same keys_hash in MergedInsert,
		 * add the delete-change into MergedDelete.
		 */
		Assert(old_tuple_begin_cursor != -1);
		change_entry = hash_search(changes_cache_entry->MergedDelete,
		                           (const void *) &keys_hash,
		                           HASH_ENTER,
		                           &found);
		Assert(nodeidx != -1);
		if (found)
		{
			Assert(change_entry->repeat >= 1);
			change_entry->repeat++;
		}
		else
		{
			change_entry->change_str = makeStringInfo();
			change_entry->nodeoid = PGXCNodeGetNodeOid(nodeidx, PGXC_NODE_DATANODE);
			change_entry->repeat = 1;
			appendBinaryStringInfo(change_entry->change_str,
			                       s->data + old_tuple_begin_cursor,
			                       s->cursor - old_tuple_begin_cursor);
		}
	}
	
	MemoryContextSwitchTo(oldctx);
	
	logicalrep_rel_close(reprel, NoLock);
}

/*
 * Handle BEGIN message
 */
static void
apply_cache_begin(StringInfo s)
{
	LogicalRepBeginData begin_data;
	
	/* init changes context and cache. */
	logicalrep_changes_cache_init();
	
	logicalrep_read_begin(s, &begin_data);
	
	remote_final_lsn = begin_data.final_lsn;
	
	batch_remote_final_committime = begin_data.committime;
	
	in_remote_transaction = true;
	cache_single_txn_ing = true;
	pgstat_report_activity(STATE_RUNNING, NULL);
}

/*
 * Handle COMMIT message on CN after execute batch changed to DN.
*/
static void
exec_batch_commit_on_cn()
{
	/* The synchronization worker runs in single transaction. */
	if (IsTransactionState() && !am_tablesync_worker())
	{
		if (am_opentenbase_subscription_dispatch_worker())
		{
			TransactionId xid = GetTopTransactionIdIfAny();
			bool markXidCommitted = TransactionIdIsValid(xid);
			
			if (!markXidCommitted)
			{
				/* Force to request a xid to write a COMMIT log */
				GetCurrentTransactionId();
			}
		}
		/*
		 * Update origin state so we can restart streaming from correct
		 * position in case of crash.
		 */
		replorigin_session_origin_lsn = remote_final_lsn;
		replorigin_session_origin_timestamp = batch_remote_final_committime;
		
		CommitTransactionCommand();
		pgstat_report_stat(false);
		
		store_flush_position(remote_final_lsn);
	}
	else
	{
		/* Process any invalidation messages that might have accumulated. */
		AcceptInvalidationMessages();
		maybe_reread_subscription();
	}
	
	in_remote_transaction = false;
	
	/* Process any tables that are being synchronized in parallel. */
	process_syncing_tables(remote_final_lsn);
	
	pgstat_report_activity(STATE_IDLE, NULL);
}

/*
 * Send changes to handle->outBuffer iff handle->nodeoid equals to change_entry->nodeoid.
 */
static int
pgxc_node_send_batch_apply(PGXCNodeHandle *handle, HTAB *changes,
						   LogicalRepRelation remoterel, bool ignore_pk_conflict,
						   char op)
{
	int msgLen = 0;
	int changes_entry_num = 0;
	HASH_SEQ_STATUS seq_status;
	ChangeEntry *change_entry;
	int tuple_total_len = 0;
	uint32 remoteid = (uint32) remoterel.remoteid;
	int nspname_len = strlen(remoterel.nspname);
	int relname_len = strlen(remoterel.relname);
	
	/* Get the string length first here. */
	hash_seq_init(&seq_status, changes);
	while ((change_entry = (ChangeEntry *) hash_seq_search(&seq_status)) != NULL)
	{
		/* pick the changes which will be sent through this conn. */
		if (change_entry->nodeoid == handle->nodeoid)
		{
			Assert(change_entry->repeat >= 1);
			changes_entry_num++;
			tuple_total_len += (4 + change_entry->change_str->len);
		}
	}

	/*
	 * size + ignore_pk_conflict + 'B' + op('I'/'D') + reloid + nspname_len + 1('\0') + relname_len + 1('\0') +
	 * repl_ident + changes_entry_num + string_len
	 */
	msgLen = 4 + 1 + 1 + 1 + 4 + nspname_len + 1 + relname_len + 1
	         + 1 + 4 + tuple_total_len;
	
	/* msgType + msgLen */
	if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0)
	{
		add_error_message(handle, "out of memory");
		return EOF;
	}
	
	handle->outBuffer[handle->outEnd++] = 'a';        /* logical apply */
	
	msgLen = pg_hton32(msgLen);
	memcpy(handle->outBuffer + handle->outEnd, &msgLen, 4);
	handle->outEnd += 4;
	
	if (ignore_pk_conflict)
		handle->outBuffer[handle->outEnd++] = 'Y';
	else
		handle->outBuffer[handle->outEnd++] = 'N';
	
	/* batch process. */
	handle->outBuffer[handle->outEnd++] = 'B';
	
	/* DELETE or ISNERT */
	handle->outBuffer[handle->outEnd++] = op;
	
	/* use Oid as relation identifier */
	remoteid = pg_hton32(remoteid);
	memcpy(handle->outBuffer + handle->outEnd, &remoteid, 4);
	handle->outEnd += 4;
	
	do
	{
		/* nspname + relname + replident */
		memcpy(handle->outBuffer + handle->outEnd, remoterel.nspname, nspname_len);
		handle->outEnd += nspname_len;
		handle->outBuffer[handle->outEnd++] = '\0';
		memcpy(handle->outBuffer + handle->outEnd, remoterel.relname, relname_len + 1);
		handle->outEnd += relname_len;
		handle->outBuffer[handle->outEnd++] = '\0';
		handle->outBuffer[handle->outEnd++] = remoterel.replident;
	} while (0);
	
	/* num of change entries. */
	changes_entry_num = pg_hton32(changes_entry_num);
	memcpy(handle->outBuffer + handle->outEnd, &changes_entry_num, 4);
	handle->outEnd += 4;
	
	hash_seq_init(&seq_status, changes);
	while ((change_entry = (ChangeEntry *) hash_seq_search(&seq_status)) != NULL)
	{
		if (change_entry->nodeoid == handle->nodeoid)
		{
			int repeat = change_entry->repeat;
			repeat = pg_hton32(repeat);
			memcpy(handle->outBuffer + handle->outEnd, &repeat, 4);
			handle->outEnd += 4;
			
			/* LogicalRepTupleData */
			memcpy(handle->outBuffer + handle->outEnd, change_entry->change_str->data, change_entry->change_str->len);
			handle->outEnd += change_entry->change_str->len;
		}
	}
	
	SetTransitionInExtendedQuery(handle, false);
	return SendRequest(handle);
}

/*
 * Send changes to datanodes and handle the response.
 */
static void
apply_exec_on_nodes_batch(LogicalRepRelId remoterelid, HTAB *changes, char op)
{
	LogicalRepRelMapEntry *reprel;
	PGXCNodeAllHandles *all_handles = NULL;
	int i = 0;
	bool ignore_pk_conflict;
	int result;
	ResponseCombiner combiner;

	reprel = logicalrep_rel_open(remoterelid, NoLock);
	
	ignore_pk_conflict = MySubscription->ignore_pk_conflict;
	
	/* send apply message to DN and wait for response */
	all_handles = get_handles(reprel->localrel->rd_locator_info->rl_nodeList,
	                          NIL, false, true, FIRST_LEVEL, InvalidFid, true, false);
	
	
	for (i = 0; i < all_handles->dn_conn_count; i++)
	{
		Assert(all_handles->datanode_handles[i]->sock != PGINVALID_SOCKET);
		if (!(all_handles->datanode_handles[i]->sock != PGINVALID_SOCKET))
			abort();
		
		if (pgxc_node_send_batch_apply(all_handles->datanode_handles[i],
		                               changes, reprel->remoterel,
		                               ignore_pk_conflict, op))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR),
					        errmsg("pgxc_node_send_batch_apply sending insert apply fails")));
		}
	}
	
	InitResponseCombiner(&combiner, all_handles->dn_conn_count, COMBINE_TYPE_NONE);

	/* Receive responses */
	result = pgxc_node_receive_responses(all_handles->dn_conn_count, all_handles->datanode_handles, NULL, &combiner);
	if (result)
	{
		if (combiner.errorMessage)
		{
			pgxc_node_report_error(&combiner);
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR),
					        errmsg("Failed to APPLY the tuple on one or more datanodes")));
		}
		result = EOF;
	}
	else if (ignore_pk_conflict &&
	         combiner.errorMessage &&
	         MAKE_SQLSTATE(combiner.errorCode[0], combiner.errorCode[1],
	                       combiner.errorCode[2], combiner.errorCode[3],
	                       combiner.errorCode[4]) == ERRCODE_UNIQUE_VIOLATION)
	{
		if (combiner.errorDetail && combiner.errorHint)
		{
			elog(LOG, "logical apply found that %s, %s, %s",
			     combiner.errorMessage,
			     combiner.errorDetail,
			     combiner.errorHint);
		}
		else if (combiner.errorDetail)
		{
			elog(LOG, "logical apply found that %s, %s",
			     combiner.errorMessage,
			     combiner.errorDetail);
		}
		else
		{
			elog(LOG, "logical apply found that %s",
			     combiner.errorMessage);
		}
	}
	else if (!validate_combiner(&combiner))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR),
				        errmsg("apply_exec_on_nodes validate_combiner responese of APPLY failed")));
		
	}
	
	CloseCombiner(&combiner);
	pfree_pgxc_all_handles(all_handles);
	
	logicalrep_rel_close(reprel, NoLock);
	
	/* End */
	hash_destroy(changes);
	changes = NULL;
}

/*
 * Apply all merged changes in LogicalRepChangesCache.
 */
static void
apply_cached_changes()
{
	MemoryContext oldctx = NULL;
	HASH_SEQ_STATUS seq_status;
	LogicalRepChangesCacheEntry *rel_changes_entry = NULL;
	
	if (!LogicalRepChangesCache)
		return;
	
	if (CurrentMemoryContext != ChangesCacheContext)
		oldctx = MemoryContextSwitchTo(ChangesCacheContext);
	
	/* 1. transaction has already begun before. */
	/* 2. send merged changes to datanode and wait for response. */
	hash_seq_init(&seq_status, LogicalRepChangesCache);
	
	while ((rel_changes_entry = (LogicalRepChangesCacheEntry *) hash_seq_search(&seq_status)) != NULL)
	{
		/* send and execute all delete-changes and insert-changes for a rel sequentially */
		if (hash_get_num_entries(rel_changes_entry->MergedDelete) > 0)
			apply_exec_on_nodes_batch(rel_changes_entry->remoterelid, rel_changes_entry->MergedDelete, 'D');
		if (hash_get_num_entries(rel_changes_entry->MergedInsert) > 0)
			apply_exec_on_nodes_batch(rel_changes_entry->remoterelid, rel_changes_entry->MergedInsert, 'I');
		CommandCounterIncrement();
	}
	
	/* 3. commit transaction. */
	exec_batch_commit_on_cn();
	
	/* 4. reset count. */
	last_merge_time = GetCurrentTimestamp();
	logical_apply_batch_size = 0;
	
	/* 5. destroy hash table */
	if (LogicalRepChangesCache)
	{
		hash_destroy(LogicalRepChangesCache);
		LogicalRepChangesCache = NULL;
	}
	
	MemoryContextReset(ChangesCacheContext);
	
	if (oldctx)
		MemoryContextSwitchTo(oldctx);
}

/*
 * Cache COMMIT message
 */
static void
apply_cache_commit(StringInfo s)
{
	LogicalRepCommitData commit_data;
	
	logicalrep_read_commit(s, &commit_data);
	
	Assert(commit_data.commit_lsn == remote_final_lsn);
	Assert(commit_data.committime == batch_remote_final_committime);
	
	/* send merged changes to datanode if reach the threshold. Otherwise, ignore the commit msg. */
	if (should_send_cached_changes())
	{
		apply_cached_changes();
	}
	
	cache_single_txn_ing = false;
}

/*
 * Logical replication protocol message cacher.
 */
static void
apply_dispatch_batch(StringInfo s)
{
	char		action = pq_getmsgbyte(s);

	wal_set_internal_stream();
	
	switch (action)
	{
		/* BEGIN */
		case 'B':
			apply_cache_begin(s);
			break;
			/* COMMIT */
		case 'C':
			apply_cache_commit(s);
			break;
			/* INSERT */
		case 'I':
			apply_cache_insert(s);
			logical_apply_batch_size++;
			break;
			/* UPDATE */
		case 'U':
			apply_cache_update(s);
			logical_apply_batch_size++;
			break;
			/* DELETE */
		case 'D':
			apply_cache_delete(s);
			logical_apply_batch_size++;
			break;
			/* RELATION */
		case 'R':
			apply_handle_relation(s);
			break;
			/* TYPE */
		case 'Y':
			apply_handle_type(s);
			break;
			/* ORIGIN */
		case 'O':
			apply_handle_origin(s);
			break;
		default:
		{
			wal_reset_stream();
			
			ereport(ERROR,
			        (errcode(ERRCODE_PROTOCOL_VIOLATION),
					        errmsg("invalid logical replication message type %c", action)));
		}
	}
	wal_reset_stream();
}

/* Batch Process end. */

/*
 * Figure out which write/flush positions to report to the walsender process.
 *
 * We can't simply report back the last LSN the walsender sent us because the
 * local transaction might not yet be flushed to disk locally. Instead we
 * build a list that associates local with remote LSNs for every commit. When
 * reporting back the flush position to the sender we iterate that list and
 * check which entries on it are already locally flushed. Those we can report
 * as having been flushed.
 *
 * The have_pending_txes is true if there are outstanding transactions that
 * need to be flushed.
 */
static void
get_flush_position(XLogRecPtr *write, XLogRecPtr *flush,
				   bool *have_pending_txes)
{
	dlist_mutable_iter iter;
	XLogRecPtr	local_flush = GetFlushRecPtr();

	*write = InvalidXLogRecPtr;
	*flush = InvalidXLogRecPtr;

	dlist_foreach_modify(iter, &lsn_mapping)
	{
		FlushPosition *pos =
		dlist_container(FlushPosition, node, iter.cur);

		*write = pos->remote_end;

		if (pos->local_end <= local_flush)
		{
			*flush = pos->remote_end;
			dlist_delete(iter.cur);
			pfree(pos);
		}
		else
		{
			/*
			 * Don't want to uselessly iterate over the rest of the list which
			 * could potentially be long. Instead get the last element and
			 * grab the write position from there.
			 */
			pos = dlist_tail_element(FlushPosition, node,
									 &lsn_mapping);
			*write = pos->remote_end;
			*have_pending_txes = true;
			return;
		}
	}

	*have_pending_txes = !dlist_is_empty(&lsn_mapping);
}

/*
 * Store current remote/local lsn pair in the tracking list.
 */
static void
store_flush_position(XLogRecPtr remote_lsn)
{
	FlushPosition *flushpos;

	/* Need to do this in permanent context */
	MemoryContextSwitchTo(ApplyContext);

	/* Track commit lsn  */
	flushpos = (FlushPosition *) palloc(sizeof(FlushPosition));
	flushpos->local_end = XactLastCommitEnd;
	flushpos->remote_end = remote_lsn;

	dlist_push_tail(&lsn_mapping, &flushpos->node);
	MemoryContextSwitchTo(ApplyMessageContext);
}


/* Update statistics of the worker. */
static void
UpdateWorkerStats(XLogRecPtr last_lsn, TimestampTz send_time, bool reply)
{
	MyLogicalRepWorker->last_lsn = last_lsn;
	MyLogicalRepWorker->last_send_time = send_time;
	MyLogicalRepWorker->last_recv_time = GetCurrentTimestamp();
	if (reply)
	{
		MyLogicalRepWorker->reply_lsn = last_lsn;
		MyLogicalRepWorker->reply_time = send_time;
	}
}

/*
 * Apply main loop.
 */
static void
LogicalRepApplyLoop(XLogRecPtr last_received)
{
	/*
	 * Init the ApplyMessageContext which we clean up after each replication
	 * protocol message.
	 */
	ApplyMessageContext = AllocSetContextCreate(ApplyContext,
												"ApplyMessageContext",
												ALLOCSET_DEFAULT_SIZES);

	/* mark as idle, before starting to loop */
	pgstat_report_activity(STATE_IDLE, NULL);

	last_merge_time = GetCurrentTimestamp();
	logical_apply_batch_size = 0;
	for (;;)
	{
		pgsocket	fd = PGINVALID_SOCKET;
		int			rc;
		int			len;
		char	   *buf = NULL;
		bool		endofstream = false;
		TimestampTz last_recv_timestamp = GetCurrentTimestamp();
		bool		ping_sent = false;
		long		wait_time;

		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(ApplyMessageContext);

		len = walrcv_receive(wrconn, &buf, &fd);

		if (len != 0)
		{
			/* Process the data */
			for (;;)
			{
				CHECK_FOR_INTERRUPTS();

				if (len == 0)
				{
					break;
				}
				else if (len < 0)
				{
					ereport(LOG,
							(errmsg("data stream from publisher has ended")));
					endofstream = true;
					break;
				}
				else
				{
					int			c;
					StringInfoData s;

					/* Reset timeout. */
					last_recv_timestamp = GetCurrentTimestamp();
					ping_sent = false;

					/* Ensure we are reading the data into our memory context. */
					MemoryContextSwitchTo(ApplyMessageContext);

					s.data = buf;
					s.len = len;
					s.cursor = 0;
					s.maxlen = -1;

					c = pq_getmsgbyte(&s);

					if (c == 'w')	/* WalSndPrepareWrite */
					{
						XLogRecPtr	start_lsn;
						XLogRecPtr	end_lsn;
						TimestampTz send_time;

						start_lsn = pq_getmsgint64(&s);
						end_lsn = pq_getmsgint64(&s);
						send_time = pq_getmsgint64(&s);

						if (last_received < start_lsn)
							last_received = start_lsn;

						if (last_received < end_lsn)
							last_received = end_lsn;

						UpdateWorkerStats(last_received, send_time, false);

						if (IS_PGXC_COORDINATOR && !am_tablesync_worker() && (logical_apply_time_delay || logical_apply_cache_capacity))
						{
							in_batch_process_mode = true;
							apply_dispatch_batch(&s);
						}
						else
						{
							apply_dispatch(&s);
						}
					}
					else if (c == 'k')	/* WalSndKeepalive */
					{
						XLogRecPtr	end_lsn;
						TimestampTz timestamp;
						bool		reply_requested;

						end_lsn = pq_getmsgint64(&s);
						timestamp = pq_getmsgint64(&s);
						reply_requested = pq_getmsgbyte(&s);

						if (last_received < end_lsn)
							last_received = end_lsn;

						/* send cached changes to datanode and wait for the responses. */
						if (in_batch_process_mode && !cache_single_txn_ing && should_send_cached_changes())
						{
							apply_cached_changes();
						}
						send_feedback(last_received, reply_requested, false);
						UpdateWorkerStats(last_received, timestamp, true);
					}
					/* other message types are purposefully ignored */

					MemoryContextReset(ApplyMessageContext);
				}

				len = walrcv_receive(wrconn, &buf, &fd);
			}
		}

#ifdef __STORAGE_SCALABLE__
		/* update all tables' statistics */
		if (!am_tablesync_worker())
		{
			logicalrep_statistic_update_for_apply(MySubscription->oid, MySubscription->name);
		}
#endif

		/* confirm all writes so far */
		send_feedback(last_received, false, false);
		
		if (!in_remote_transaction)
		{
			/*
			 * If we didn't get any transactions for a while there might be
			 * unconsumed invalidation messages in the queue, consume them
			 * now.
			 */
			AcceptInvalidationMessages();
			maybe_reread_subscription();

			/* Process any table synchronization changes. */
			process_syncing_tables(last_received);
		}

		/* Cleanup the memory. */
		MemoryContextResetAndDeleteChildren(ApplyMessageContext);
		MemoryContextSwitchTo(TopMemoryContext);

		/* Check if we need to exit the streaming loop. */
		if (endofstream)
		{
			TimeLineID	tli;

			walrcv_endstreaming(wrconn, &tli);
			break;
		}

		/*
		 * Wait for more data or latch.  If we have unflushed transactions,
		 * wake up after WalWriterDelay to see if they've been flushed yet (in
		 * which case we should send a feedback message).  Otherwise, there's
		 * no particular urgency about waking up unless we get data or a
		 * signal.
		 */
		if (!dlist_is_empty(&lsn_mapping))
			wait_time = WalWriterDelay;
		else
			wait_time = NAPTIME_PER_CYCLE;

		rc = WaitLatchOrSocket(MyLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   fd, wait_time,
							   WAIT_EVENT_LOGICAL_APPLY_MAIN);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (rc & WL_TIMEOUT)
		{
			/*
			 * We didn't receive anything new. If we haven't heard anything
			 * from the server for more than wal_receiver_timeout / 2, ping
			 * the server. Also, if it's been longer than
			 * wal_receiver_status_interval since the last update we sent,
			 * send a status update to the master anyway, to report any
			 * progress in applying WAL.
			 */
			bool		requestReply = false;

			/*
			 * Check if time since last receive from standby has reached the
			 * configured limit.
			 */
			if (wal_receiver_timeout > 0)
			{
				TimestampTz now = GetCurrentTimestamp();
				TimestampTz timeout;

				timeout =
					TimestampTzPlusMilliseconds(last_recv_timestamp,
												wal_receiver_timeout);

				if (now >= timeout)
					ereport(ERROR,
							(errmsg("terminating logical replication worker due to timeout")));

				/*
				 * We didn't receive anything new, for half of receiver
				 * replication timeout. Ping the server.
				 */
				if (!ping_sent)
				{
					timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
														  (wal_receiver_timeout / 2));
					if (now >= timeout)
					{
						requestReply = true;
						ping_sent = true;
					}
				}
			}

			send_feedback(last_received, requestReply, requestReply);
		}
	}
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
static void
send_feedback(XLogRecPtr recvpos, bool force, bool requestReply)
{
	static StringInfo reply_message = NULL;
	static TimestampTz send_time = 0;

	static XLogRecPtr last_recvpos = InvalidXLogRecPtr;
	static XLogRecPtr last_writepos = InvalidXLogRecPtr;
	static XLogRecPtr last_flushpos = InvalidXLogRecPtr;

	XLogRecPtr	writepos;
	XLogRecPtr	flushpos;
	TimestampTz now;
	bool		have_pending_txes;

	/*
	 * If the user doesn't want status to be reported to the publisher, be
	 * sure to exit before doing anything at all.
	 */
	if (!force && wal_receiver_status_interval <= 0)
		return;

	/* It's legal to not pass a recvpos */
	if (recvpos < last_recvpos)
		recvpos = last_recvpos;

	get_flush_position(&writepos, &flushpos, &have_pending_txes);

	/*
	 * No outstanding transactions to flush, we can report the latest received
	 * position. This is important for synchronous replication.
	 */
	if (!have_pending_txes)
		flushpos = writepos = recvpos;

	if (writepos < last_writepos)
		writepos = last_writepos;

	if (flushpos < last_flushpos)
		flushpos = last_flushpos;

	now = GetCurrentTimestamp();

	/* if we've already reported everything we're good */
	if (!force &&
		writepos == last_writepos &&
		flushpos == last_flushpos &&
		!TimestampDifferenceExceeds(send_time, now,
									wal_receiver_status_interval * 1000))
		return;
	send_time = now;

	if (!reply_message)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(ApplyContext);

		reply_message = makeStringInfo();
		MemoryContextSwitchTo(oldctx);
	}
	else
		resetStringInfo(reply_message);

	pq_sendbyte(reply_message, 'r');
	pq_sendint64(reply_message, recvpos);	/* write */
	pq_sendint64(reply_message, flushpos);	/* flush */
	pq_sendint64(reply_message, writepos);	/* apply */
	pq_sendint64(reply_message, now);	/* sendTime */
	pq_sendbyte(reply_message, requestReply);	/* replyRequested */
#ifdef __SUBSCRIPTION__
	pq_sendbyte(reply_message, MySubscription->is_all_actived ? 1 : 0);	/* is_all_actived */
#endif

	elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
		 force,
		 (uint32) (recvpos >> 32), (uint32) recvpos,
		 (uint32) (writepos >> 32), (uint32) writepos,
		 (uint32) (flushpos >> 32), (uint32) flushpos
		);

	walrcv_send(wrconn, reply_message->data, reply_message->len);

	if (recvpos > last_recvpos)
		last_recvpos = recvpos;
	if (writepos > last_writepos)
		last_writepos = writepos;
	if (flushpos > last_flushpos)
		last_flushpos = flushpos;
}

/*
 * Reread subscription info if needed. Most changes will be exit.
 */
static void
maybe_reread_subscription(void)
{
	MemoryContext oldctx;
	Subscription *newsub;
	bool		started_tx = false;

	/* When cache state is valid there is nothing to do here. */
	if (MySubscriptionValid)
		return;

	/* This function might be called inside or outside of transaction. */
	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_tx = true;
	}

	/* Ensure allocations in permanent context. */
	oldctx = MemoryContextSwitchTo(ApplyContext);

	newsub = GetSubscription(MyLogicalRepWorker->subid, true);

	/*
	 * Exit if the subscription was removed. This normally should not happen
	 * as the worker gets killed during DROP SUBSCRIPTION.
	 */
	if (!newsub)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"stop because the subscription was removed",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if the subscription was disabled. This normally should not happen
	 * as the worker gets killed during ALTER SUBSCRIPTION ... DISABLE.
	 */
	if (!newsub->enabled)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"stop because the subscription was disabled",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if connection string was changed. The launcher will start new
	 * worker.
	 */
	if (strcmp(newsub->conninfo, MySubscription->conninfo) != 0)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because the connection information was changed",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if subscription name was changed (it's used for
	 * fallback_application_name). The launcher will start new worker.
	 */
	if (strcmp(newsub->name, MySubscription->name) != 0)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because subscription was renamed",
						MySubscription->name)));

		proc_exit(0);
	}

	/* !slotname should never happen when enabled is true. */
	Assert(newsub->slotname);

	/*
	 * We need to make new connection to new slot if slot name has changed so
	 * exit here as well if that's the case.
	 */
	if (strcmp(newsub->slotname, MySubscription->slotname) != 0)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because the replication slot name was changed",
						MySubscription->name)));

		proc_exit(0);
	}

	/*
	 * Exit if publication list was changed. The launcher will start new
	 * worker.
	 */
	if (!equal(newsub->publications, MySubscription->publications))
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will "
						"restart because subscription's publications were changed",
						MySubscription->name)));

		proc_exit(0);
	}

	/* Check for other changes that should never happen too. */
	if (newsub->dbid != MySubscription->dbid)
	{
		elog(ERROR, "subscription %u changed unexpectedly",
			 MyLogicalRepWorker->subid);
	}

	/* Clean old subscription info and switch to new one. */
	FreeSubscription(MySubscription);
	MySubscription = newsub;

	MemoryContextSwitchTo(oldctx);

	/* Change synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit", MySubscription->synccommit,
					PGC_BACKEND, PGC_S_OVERRIDE);

	if (started_tx)
		CommitTransactionCommand();

	MySubscriptionValid = true;
}

/*
 * Callback from subscription syscache invalidation.
 */
static void
subscription_change_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	MySubscriptionValid = false;
}

/* SIGHUP: set flag to reload configuration at next convenient time */
static void
logicalrep_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;

	/* Waken anything waiting on the process latch */
	SetLatch(MyLatch);

	errno = save_errno;
}

/* Logical Replication Apply worker entry point */
void
ApplyWorkerMain(Datum main_arg)
{
	int			worker_slot = DatumGetInt32(main_arg);
	MemoryContext oldctx;
	char		originname[NAMEDATALEN];
	XLogRecPtr	origin_startpos;
	char	   *myslotname;
	WalRcvStreamOptions options;

	if (IsInplaceUpgrade && WorkingGrandVersionNum < 5)
	{
		ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("during 3.16 upgrade, this operation not allowed until upgrade finish.")));
		proc_exit(0);
	}
	/* Attach to slot */
	logicalrep_worker_attach(worker_slot);

	/* Setup signal handling */
	pqsignal(SIGHUP, logicalrep_worker_sighup);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Initialise stats to a sanish value */
	MyLogicalRepWorker->last_send_time = MyLogicalRepWorker->last_recv_time =
		MyLogicalRepWorker->reply_time = GetCurrentTimestamp();

	/* Load the libpq-specific functions */
	load_file("libpqwalreceiver", false);

	/* Run as replica session replication role. */
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_OVERRIDE);

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyLogicalRepWorker->dbid,
											  MyLogicalRepWorker->userid);

	/*
	 * Set always-secure search path, so malicious users can't redirect user
	 * code (e.g. pg_index.indexprs).
	 */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/* Load the subscription into persistent memory context. */
	ApplyContext = AllocSetContextCreate(TopMemoryContext,
										 "ApplyContext",
										 ALLOCSET_DEFAULT_SIZES);
	StartTransactionCommand();
	oldctx = MemoryContextSwitchTo(ApplyContext);
	MySubscription = GetSubscription(MyLogicalRepWorker->subid, true);
	if (!MySubscription)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription %u will not "
						"start because the subscription was removed during startup",
						MyLogicalRepWorker->subid)));
		proc_exit(0);
	}

	MySubscriptionValid = true;
	MemoryContextSwitchTo(oldctx);

	/* Setup synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit", MySubscription->synccommit,
					PGC_BACKEND, PGC_S_OVERRIDE);

#ifdef __SUBSCRIPTION__
	if (am_opentenbase_subscription_dispatch_worker())
	{
		SetConfigOption("persistent_connections_timeout", "-1",
					PGC_BACKEND, PGC_S_OVERRIDE);
	}
#endif

	if (!MySubscription->enabled)
	{
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" will not "
						"start because the subscription was disabled during startup",
						MySubscription->name)));

		proc_exit(0);
	}

	/* Keep us informed about subscription changes. */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONOID,
								  subscription_change_cb,
								  (Datum) 0);

	if (am_tablesync_worker())
		ereport(LOG,
				(errmsg("logical replication table synchronization worker for subscription \"%s\", table \"%s\" has started",
						MySubscription->name, get_rel_name(MyLogicalRepWorker->relid))));
	else
		ereport(LOG,
				(errmsg("logical replication apply worker for subscription \"%s\" has started",
						MySubscription->name)));

#ifdef __SUBSCRIPTION__
	/* Initialize XL executor. This must be done inside a transaction block. */
	InitMultinodeExecutor(false);
#endif

	CommitTransactionCommand();

	/* Connect to the origin and start the replication. */
	elog(DEBUG1, "connecting to publisher using connection string \"%s\"",
		 MySubscription->conninfo);

	if (am_tablesync_worker())
	{
		char	   *syncslotname;

		/* This is table synchroniation worker, call initial sync. */
		syncslotname = LogicalRepSyncTableStart(&origin_startpos);

		/* The slot name needs to be allocated in permanent memory context. */
		oldctx = MemoryContextSwitchTo(ApplyContext);
		myslotname = pstrdup(syncslotname);
		MemoryContextSwitchTo(oldctx);

		pfree(syncslotname);
	}
	else
	{
		/* This is main apply worker */
		RepOriginId originid;
		TimeLineID	startpointTLI;
		char	   *err;
		int			server_version;

		myslotname = MySubscription->slotname;

		/*
		 * This shouldn't happen if the subscription is enabled, but guard
		 * against DDL bugs or manual catalog changes.  (libpqwalreceiver will
		 * crash if slot is NULL.)
		 */
		if (!myslotname)
			ereport(ERROR,
					(errmsg("subscription has no replication slot set")));

		/* Setup replication origin tracking. */
		StartTransactionCommand();
		snprintf(originname, sizeof(originname), "pg_%u", MySubscription->oid);
		originid = replorigin_by_name(originname, true);
		if (!OidIsValid(originid))
			originid = replorigin_create(originname);
		replorigin_session_setup(originid);
		replorigin_session_origin = originid;
		origin_startpos = replorigin_session_get_progress(false);
		CommitTransactionCommand();

		wrconn = walrcv_connect(MySubscription->conninfo, true, MySubscription->name,
								&err);
		if (wrconn == NULL)
			ereport(ERROR,
					(errmsg("could not connect to the publisher: %s", err)));

		/*
		 * We don't really use the output identify_system for anything but it
		 * does some initializations on the upstream so let's still call it.
		 */
		(void) walrcv_identify_system(wrconn, &startpointTLI,
									  &server_version);
	}

	/*
	 * Setup callback for syscache so that we know when something changes in
	 * the subscription relation state.
	 */
	CacheRegisterSyscacheCallback(SUBSCRIPTIONRELMAP,
								  invalidate_syncing_table_states,
								  (Datum) 0);

	/* Build logical replication streaming options. */
	options.logical = true;
	options.startpoint = origin_startpos;
	options.slotname = myslotname;
	options.proto.logical.proto_version = LOGICALREP_PROTO_VERSION_NUM;
	options.proto.logical.publication_names = MySubscription->publications;

	/* Start normal logical streaming replication. */
	walrcv_startstreaming(wrconn, &options);

	/* Run the main loop. */
	LogicalRepApplyLoop(origin_startpos);

	proc_exit(0);
}

/*
 * Is current process a logical replication worker?
 */
bool
IsLogicalWorker(void)
{
	return MyLogicalRepWorker != NULL;
}

/*
 * A wrapper around 'SPI_execute()' which fails with 'ereport(ERROR)' if an
 * error happens.
 *
 * @note Unexpected errors may happen if 'read_only' is set. In addition,
 * 'tcount' imposes a hard limit on the number of tuples processed. Use these
 * parameters cautiously.
 */
static inline void
execute_spi_or_error(const char *query, bool read_only, int64 tcount)
{
	int r;
	const char *old_debug_query_string = debug_query_string;
	
	debug_query_string = query;
	PG_TRY();
			{
				r = SPI_execute(query, read_only, tcount);
			}
		PG_CATCH();
			{
				debug_query_string = old_debug_query_string;
				PG_RE_THROW();
			}
	PG_END_TRY();
	debug_query_string = old_debug_query_string;
	
	if (r < 0)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
				errmsg("TDX: Failed to execute '%s' via SPI: %s [%d]", query, SPI_result_code_string(r), r)));
}

List *
load_partition_offset_pairs_asc(Oid ftoid, List *partitions)
{
	List *volatile result = NIL;
	MemoryContext allocation_mcxt = NULL;
	StringInfoData query;
	initStringInfo(&query);
	
	/* Build query in multiple steps */
	appendStringInfo(&query, "SELECT prt, off FROM exttable_fdw.offsets WHERE ftoid = %d", ftoid);
	if (partitions != NIL)
	{
		ListCell *it;
		appendStringInfo(&query, " AND prt IN (");
		
		foreach(it, partitions)
			appendStringInfo(&query, it != list_tail(partitions) ? "%d, " : "%d)", it->data.int_value);
	}
	appendStringInfo(&query, "order by prt;");
	
	/* Between SPI_connect() and SPI_finish(), a temporary mcxt is used. */
	allocation_mcxt = CurrentMemoryContext;
	
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
				errmsg("TDX: Failed to connect to SPI")));
	PG_TRY();
			{
				uint64 row_i;
				execute_spi_or_error(query.data, true, 0);
				if (SPI_tuptable == NULL)
					ereport(ERROR,
					        (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("Failed to retrieve results of '%s'",
					                                                        query.data)));
				
				for (row_i = 0; row_i < SPI_processed; row_i++)
				{
					bool is_null_partition;
					bool is_null_offset;
					MemoryContext oldcontext = MemoryContextSwitchTo(allocation_mcxt);
					PartitionOffsetPair *pop = (PartitionOffsetPair *) palloc0(sizeof(PartitionOffsetPair));
					
					result = lappend(result, pop);
					MemoryContextSwitchTo(oldcontext);
					
					pop->partition = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[row_i],
					                                             SPI_tuptable->tupdesc,
					                                             1,
					                                             &is_null_partition));
					Assert(!is_null_partition);
					
					pop->offset = DatumGetUInt64(SPI_getbinval(SPI_tuptable->vals[row_i],
					                                           SPI_tuptable->tupdesc,
					                                           2,
					                                           &is_null_offset));
					Assert(!is_null_offset);
				}
			}
		PG_CATCH();
			{
				SPI_finish();
				PG_RE_THROW();
			}
	PG_END_TRY();
	SPI_finish();
	
	pfree(query.data);
	
	return result;
}

static void
add_partition_offset_pairs(Oid ftoid, List *partition_offset_pairs, char *ftname)
{
	if (list_length(partition_offset_pairs) == 0)
		return;
	
	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
				errmsg("TDX: Failed to connect to SPI")));
	PG_TRY();
			{
				StringInfoData query_insert_template;
				StringInfoData query_insert;
				ListCell *it;
				
				initStringInfo(&query_insert_template);
				appendStringInfo(&query_insert_template,
				                 "INSERT INTO exttable_fdw.offsets(ftoid, prt, off, ftname, updatetime) VALUES ");
				
				initStringInfo(&query_insert);
				
				foreach(it, partition_offset_pairs)
				{
					PartitionOffsetPair *pop = (PartitionOffsetPair *) lfirst(it);
					
					resetStringInfo(&query_insert);
					appendStringInfoString(&query_insert, query_insert_template.data);
					appendStringInfo(&query_insert, "(%u, %d, %ld, '%s', now()) ON CONFLICT (ftoid, prt) DO UPDATE "
					                                "SET off = %ld, updatetime = now();",
					                 ftoid, pop->partition, pop->offset, ftname, pop->offset);
					
					execute_spi_or_error(query_insert.data, false, 0);
				}
			}
		PG_CATCH();
			{
				SPI_finish();
				PG_RE_THROW();
			}
	PG_END_TRY();
	SPI_finish();
	
	elog(DEBUG1, "TDX: UPDATE %d partition-offset pairs", list_length(partition_offset_pairs));
}

void ApplyMsgFromStreamOnDN(LoadFromStmt *stmt)
{
	FileScanDesc currentScanDesc;
	MemoryContext oldcontext;
	TdxSyncDesc tdxDesc = NULL;
	List *options = NULL;
	CopyState cstate = NULL;
	bool still_has_data = false;
	StringInfo line_buf = NULL;
	Bitmapset *idkey = NULL;
	int i = -1;
	
	tdxDesc = (TdxSyncDesc) palloc0(sizeof(TdxSyncDescData));
	tdxDesc->syncContext = AllocSetContextCreate(CurrentMemoryContext,
	                                             "TDX-SYNC",
	                                             ALLOCSET_DEFAULT_SIZES);
	
	oldcontext = MemoryContextSwitchTo(tdxDesc->syncContext);
	
	tdxDesc->ext_relid = RangeVarGetRelid(stmt->ext_rel, AccessShareLock, true);
	/* If the relation has gone, throw error. */
	if (!OidIsValid(tdxDesc->ext_relid))
		ereport(ERROR,
		        (errcode(ERRCODE_UNDEFINED_TABLE),
				        errmsg("could not find relation %s",
				               RangeVarGetName(stmt->ext_rel))));
	
	tdxDesc->rel = heap_openrv(stmt->rel, RowExclusiveLock);
	
	tdxDesc->relname = pstrdup(stmt->rel->relname);
	tdxDesc->nspname = get_namespace_name(tdxDesc->rel->rd_rel->relnamespace);
	
	if (tdxDesc->rel->rd_rel->relkind == RELKIND_RELATION && tdxDesc->rel->trigdesc)
		ereport(ERROR,
		        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				        errmsg("cannot import data into table with triggers via tdx")));
	
	/* allocate and initialize scan descriptor */
	currentScanDesc = (FileScanDesc) palloc0(sizeof(FileScanDescData));
	currentScanDesc->fs_noop = false;
	
	/* shard locator_info should is needed by tdx. */
	if (tdxDesc->rel->rd_locator_info && tdxDesc->rel->rd_locator_info->locatorType == 'S')
		currentScanDesc->locator_info = CopyRelationLocInfo(tdxDesc->rel->rd_locator_info);
	else
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				        errmsg("tdx dml-sync only supported shard table.")));
	
	exttableGetOptions(tdxDesc->ext_relid, currentScanDesc, &options, NULL);
	
	/* allocate and init our structure that keeps track of data parsing state */
	cstate = BeginCopyFrom(NULL, tdxDesc->rel, NULL, false,
	                       external_getdata_callback, (void *) currentScanDesc,
	                       NIL, NULL, options, false);
	
	cstate->is_logical_woker = true;
	cstate->eol_type = EOL_NL;
	cstate->errMode = ALL_OR_NOTHING;
	cstate->srehandler = NULL;
	currentScanDesc->fs_pstate = cstate;
	
	/* Get the existing partition and offset corresponding to the external table from exttable_fdw.offsets. */
	cstate->l_partition_offset_pairs = load_partition_offset_pairs_asc(tdxDesc->ext_relid, NIL);
	cstate->n_partition_offset_pairs = NIL;
	
	/* open the external source. */
	if (!currentScanDesc->fs_file)
		open_external_readable_source(currentScanDesc, NULL);
	
	/* Check for supported relkind. */
	CheckSubscriptionRelkind(tdxDesc->rel->rd_rel->relkind,
	                         tdxDesc->nspname,
	                         tdxDesc->relname);
	
	/* Check if the local relation has replica identity. */
	tdxDesc->updatable = true;
	idkey = RelationGetIndexAttrBitmap(tdxDesc->rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
	/* fallback to PK if no replica identity */
	if (idkey == NULL)
	{
		idkey = RelationGetIndexAttrBitmap(tdxDesc->rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);
		
		/*
		 * If no replica identity index and no PK, the published table
		 * must have replica identity FULL(not supported).
		 */
		if (idkey == NULL)
			tdxDesc->updatable = false;
	}
	
	while ((i = bms_next_member(idkey, i)) >= 0)
	{
		int attnum = i + FirstLowInvalidHeapAttributeNumber;
		
		if (!AttrNumberIsForUserDefinedAttr(attnum))
			ereport(ERROR,
			        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					        errmsg("TDX target relation \"%s.%s\" uses "
					               "system columns in REPLICA IDENTITY index",
					               tdxDesc->nspname, tdxDesc->relname)));
	}
	
	line_buf = makeStringInfo();
	do
	{
		resetStringInfo(line_buf);
		
		still_has_data = CopyLoadRawBuf(cstate);
		if (!still_has_data && cstate->raw_buf_len == 0)
			break;
		
		appendBinaryStringInfo(line_buf, cstate->raw_buf, cstate->raw_buf_len);
		cstate->raw_buf_index = cstate->raw_buf_len;
		
		while (line_buf->cursor < line_buf->len)
		{
			int record_cursor = line_buf->cursor;
			/* dispatch */
			PG_TRY();
					{
						tdx_apply_dispatch(line_buf, tdxDesc);
					}
				PG_CATCH();
					{
						if (line_buf->cursor >= line_buf->len)
						{
							cstate->raw_buf_index = record_cursor;
							HOLD_INTERRUPTS();
							EmitErrorReport();
							FlushErrorState();
							RESUME_INTERRUPTS();
							break; /* need more data */
						}
						else
						{
							PG_RE_THROW();
						}
					}
			PG_END_TRY();
		}
	} while (still_has_data);
	
	
	heap_close(tdxDesc->rel, RowExclusiveLock);
	tdxDesc->rel = NULL;
	
	/* update exttable_fdw.offsets */
	add_partition_offset_pairs(tdxDesc->ext_relid, cstate->n_partition_offset_pairs, stmt->ext_rel->relname);
	
	EndCopyFrom(cstate);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tdxDesc->syncContext);
	pfree(tdxDesc);
}
