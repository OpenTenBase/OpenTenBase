/*-------------------------------------------------------------------------
 *
 * Gp_resgroup_helper.c
 *	  Helper functions for resource group.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "catalog/pg_resgroup.h"

#include "commands/resgroupcmds.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/resgroup.h"
#include "utils/resgroup-ops.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/planner.h"
#include "nodes/nodes.h"
#include "catalog/pg_type.h"

typedef struct ResGroupStat
{
	NameData groupName;

	StringInfo num_running;
	StringInfo num_queueing;
	StringInfo num_queued;
	StringInfo num_executed;
	StringInfo total_queue_duration;

	StringInfo cpuUsage;

	bool cleaned;
} ResGroupStat;

typedef struct ResGroupStatCtx
{
	int nGroups;
	ResGroupStat groups[1];
} ResGroupStatCtx;

static void calcCpuUsage(StringInfoData *str,
						 int64 usageBegin, TimestampTz timestampBegin,
						 int64 usageEnd, TimestampTz timestampEnd);
static void getResUsage(ResGroupStatCtx *ctx, char *inGroupName);
static void getResGroupInfo(StringInfo str, char *cmd);

static void
calcCpuUsage(StringInfoData *str,
			 int64 usageBegin, TimestampTz timestampBegin,
			 int64 usageEnd, TimestampTz timestampEnd)
{
	int64 duration;
	long secs;
	int usecs;
	int64 usage;

	usage = usageEnd - usageBegin;

	TimestampDifference(timestampBegin, timestampEnd, &secs, &usecs);

	duration = secs * 1000000 + usecs;

	appendStringInfo(str, "\"%s\":%.2f",
					 PGXCNodeName,
					 ResGroupOps_ConvertCpuUsageToPercent(usage, duration));
}

/*
 * Get resource usage.
 *
 * On CN this function dispatch the request to all DNs, collecting both
 * DNs' and CN's resource usage.
 *
 * On DN this function only collect the resource usage on itself.
 *
 * Memory & cpu usage are returned in JSON format.
 */
static void
getResUsage(ResGroupStatCtx *ctx, char *inGroupName)
{
	int64 *usages;
	TimestampTz *timestamps;
	int *groupidx;
	int i, j;
	int count, count_finished;
	List *dn_list = GetAllDataNodes();
	List *cn_list = GetAllCoordNodes(false);
	PGXCNodeAllHandles * all_handles;
	PGXCNodeHandle *handle;
	ResponseCombiner combiner;
	TupleTableSlot *tupleslot = MakeTupleTableSlot(NULL);

	usages = palloc(sizeof(*usages) * ctx->nGroups);
	timestamps = palloc(sizeof(*timestamps) * ctx->nGroups);

	for (j = 0; j < ctx->nGroups; j++)
	{
		ResGroupStat *row = &ctx->groups[j];
		char *groupName = NameStr(row->groupName);

		usages[j] = ResGroupOps_GetCpuUsage(groupName);
		timestamps[j] = GetCurrentTimestamp();
	}

	groupidx = palloc0(sizeof(int) * (NumDataNodes + NumCoords -1));

	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		StringInfoData buffer;

		initStringInfo(&buffer);
		if (inGroupName == NULL)
			appendStringInfo(&buffer,
							 "SELECT groupname, num_running, num_queueing, num_queued, num_executed, total_queue_duration, cpu_usage "
							 "FROM pg_resgroup_get_status(null)");
		else
			appendStringInfo(&buffer,
							 "SELECT groupname, num_running, num_queueing, num_queued, num_executed, total_queue_duration, cpu_usage "
							 "FROM pg_resgroup_get_status('%s')",
							 inGroupName);

		InitResponseCombiner(&combiner, 0, COMBINE_TYPE_NONE);
		all_handles = get_handles(dn_list, cn_list, false, true, FIRST_LEVEL, InvalidFid, true, false);
		count = all_handles->dn_conn_count + all_handles->co_conn_count;
		combiner.recv_node_count = count;
		combiner.recv_tuples	  = 0;
		combiner.recv_total_time = -1;
		combiner.recv_datarows = 0;
		combiner.ss.ps.ps_ResultTupleSlot = tupleslot;

		for (i = 0; i < count; i++)
		{
			if (i < all_handles->co_conn_count)
				handle = all_handles->coord_handles[i];
			else
				handle = all_handles->datanode_handles[i - all_handles->co_conn_count];

			if (pgxc_node_send_query(handle, buffer.data))
				elog(ERROR, "Failed to send cmd to node %s", handle->nodename);
		}

		count_finished = 0;
		while (count_finished < count)
		{
			i = 0;
			while (i < count)
			{
				int res;

				if (i < all_handles->co_conn_count)
					handle = all_handles->coord_handles[i];
				else
					handle = all_handles->datanode_handles[i - all_handles->co_conn_count];

				if (pgxc_node_receive(1, &handle, NULL))
					elog(ERROR, "Failed to receive response from node %s", handle->nodename);

				res = handle_response(handle, &combiner);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_ERROR)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_READY)
				{
					i++;
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ExecSetSlotDescriptor(combiner.ss.ps.ps_ResultTupleSlot,
										  combiner.tuple_desc);
					/* Now slot is responsible for free the descriptor */
					combiner.tuple_desc = NULL;
				}
				else if (res == RESPONSE_DATAROW)
				{
					bool isnull;
					char *groupName;
					ResGroupStat *row;
					tupleslot = combiner.ss.ps.ps_ResultTupleSlot;
					CopyDataRowTupleToSlot(&combiner, tupleslot);
					combiner.current_conn_rows_consumed++;
					groupName = NameStr(*DatumGetName(slot_getattr(tupleslot, 1, &isnull)));

					j = groupidx[i];
					groupidx[i]++;
					row = &ctx->groups[j];
					if (row->cpuUsage->len == 0)
					{
						Datum d;

						strcpy(NameStr(row->groupName), groupName);

						appendStringInfo(row->cpuUsage, "[");
						appendStringInfo(row->cpuUsage, "{");
						calcCpuUsage(row->cpuUsage, usages[j], timestamps[j],
									 ResGroupOps_GetCpuUsage(groupName),
									 GetCurrentTimestamp());
						appendStringInfoChar(row->cpuUsage, '}');

						d = ResGroupGetStat(groupName, RES_GROUP_STAT_NRUNNING);
						appendStringInfoChar(row->num_running, '[');
						appendStringInfoChar(row->num_running, '{');
						appendStringInfo(row->num_running, "\"%s\":%d",
										 PGXCNodeName, DatumGetInt32(d));
						appendStringInfoChar(row->num_running, '}');

						d = ResGroupGetStat(groupName, RES_GROUP_STAT_NQUEUEING);
						appendStringInfoChar(row->num_queueing, '[');
						appendStringInfoChar(row->num_queueing, '{');
						appendStringInfo(row->num_queueing, "\"%s\":%d",
										 PGXCNodeName, DatumGetInt32(d));
						appendStringInfoChar(row->num_queueing, '}');

						d = ResGroupGetStat(groupName, RES_GROUP_STAT_TOTAL_QUEUED);
						appendStringInfoChar(row->num_queued, '[');
						appendStringInfoChar(row->num_queued, '{');
						appendStringInfo(row->num_queued, "\"%s\":%d",
										 PGXCNodeName, DatumGetInt32(d));
						appendStringInfoChar(row->num_queued, '}');

						d = ResGroupGetStat(groupName, RES_GROUP_STAT_TOTAL_EXECUTED);
						appendStringInfoChar(row->num_executed, '[');
						appendStringInfoChar(row->num_executed, '{');
						appendStringInfo(row->num_executed, "\"%s\":%d",
										 PGXCNodeName, DatumGetInt32(d));
						appendStringInfoChar(row->num_executed, '}');

						d = ResGroupGetStat(groupName, RES_GROUP_STAT_TOTAL_QUEUE_TIME);
						appendStringInfoChar(row->total_queue_duration, '[');
						appendStringInfoChar(row->total_queue_duration, '{');
						appendStringInfo(row->total_queue_duration, "\"%s\":%ld",
										 PGXCNodeName, DatumGetInt64(d));
						appendStringInfoChar(row->total_queue_duration, '}');
					}

					appendStringInfo(row->num_running, ", %s", TextDatumGetCString(slot_getattr(tupleslot, 2, &isnull)));

					appendStringInfo(row->num_queueing, ", %s", TextDatumGetCString(slot_getattr(tupleslot, 3, &isnull)));

					appendStringInfo(row->num_queued, ", %s", TextDatumGetCString(slot_getattr(tupleslot, 4, &isnull)));

					appendStringInfo(row->num_executed, ", %s", TextDatumGetCString(slot_getattr(tupleslot, 5, &isnull)));

					appendStringInfo(row->total_queue_duration, ", %s", TextDatumGetCString(slot_getattr(tupleslot, 6, &isnull)));

					appendStringInfo(row->cpuUsage, ", %s", TextDatumGetCString(slot_getattr(tupleslot, 7, &isnull)));

					if (groupidx[i] == ctx->nGroups)
						count_finished++;
				}
			}
		}
		for (j = 0; j < ctx->nGroups; j++)
		{
			ResGroupStat *row = &ctx->groups[j];

			appendStringInfoChar(row->num_running, ']');

			appendStringInfoChar(row->num_queueing, ']');

			appendStringInfoChar(row->num_queued, ']');

			appendStringInfoChar(row->num_executed, ']');

			appendStringInfoChar(row->total_queue_duration, ']');

			appendStringInfoChar(row->cpuUsage, ']');
		}

		ValidateAndCloseCombiner(&combiner);
		pfree_pgxc_all_handles(all_handles);
	}
	else
	{
		pg_usleep(300000);

		for (j = 0; j < ctx->nGroups; j++)
		{
			ResGroupStat *row = &ctx->groups[j];
			char *groupName = NameStr(row->groupName);
			Datum d;

			appendStringInfo(row->cpuUsage, "{");
			calcCpuUsage(row->cpuUsage, usages[j], timestamps[j],
						 ResGroupOps_GetCpuUsage(groupName),
						 GetCurrentTimestamp());
			appendStringInfoChar(row->cpuUsage, '}');

			d = ResGroupGetStat(groupName, RES_GROUP_STAT_NRUNNING);
			appendStringInfoChar(row->num_running, '{');
			appendStringInfo(row->num_running, "\"%s\":%d",
							 PGXCNodeName, IS_ACCESS_NODE ? DatumGetInt32(d) : 0);
			appendStringInfoChar(row->num_running, '}');

			d = ResGroupGetStat(groupName, RES_GROUP_STAT_NQUEUEING);
			appendStringInfoChar(row->num_queueing, '{');
			appendStringInfo(row->num_queueing, "\"%s\":%d",
							 PGXCNodeName, IS_ACCESS_NODE ? DatumGetInt32(d) : 0);
			appendStringInfoChar(row->num_queueing, '}');

			d = ResGroupGetStat(groupName, RES_GROUP_STAT_TOTAL_QUEUED);
			appendStringInfoChar(row->num_queued, '{');
			appendStringInfo(row->num_queued, "\"%s\":%d",
							 PGXCNodeName, IS_ACCESS_NODE ? DatumGetInt32(d) : 0);
			appendStringInfoChar(row->num_queued, '}');

			d = ResGroupGetStat(groupName, RES_GROUP_STAT_TOTAL_EXECUTED);
			appendStringInfoChar(row->num_executed, '{');
			appendStringInfo(row->num_executed, "\"%s\":%d",
							 PGXCNodeName, IS_ACCESS_NODE ? DatumGetInt32(d) : 0);
			appendStringInfoChar(row->num_executed, '}');

			d = ResGroupGetStat(groupName, RES_GROUP_STAT_TOTAL_QUEUE_TIME);
			appendStringInfoChar(row->total_queue_duration, '{');
			appendStringInfo(row->total_queue_duration, "\"%s\":%ld",
							 PGXCNodeName, IS_ACCESS_NODE ? DatumGetInt64(d) : 0);
			appendStringInfoChar(row->total_queue_duration, '}');
		}
	}
}

static int
compareRow(const void *ptr1, const void *ptr2)
{
	const ResGroupStat *row1 = (const ResGroupStat *) ptr1;
	const ResGroupStat *row2 = (const ResGroupStat *) ptr2;

	return strcmp(NameStr(row1->groupName), NameStr(row2->groupName));
}

/*
 * Get status of resource groups
 */
Datum
pg_resgroup_get_status(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ResGroupStatCtx *ctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			nattr = 7;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(nattr, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "groupname", NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "num_running", JSONOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "num_queueing", JSONOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "num_queued", JSONOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "num_executed", JSONOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "total_queue_duration", JSONOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "cpu_usage", JSONOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		if (IsResGroupActivated())
		{
			Relation	pg_resgroup_rel;
			SysScanDesc	sscan;
			HeapTuple	tuple;
			char		*inGroupName = PG_GETARG_CSTRING(0);

			int ctxsize = sizeof(ResGroupStatCtx) +
				sizeof(ResGroupStat) * (MaxResourceGroups - 1);

			(void) inGroupName;

			funcctx->user_fctx = palloc(ctxsize);
			ctx = (ResGroupStatCtx *) funcctx->user_fctx;

			/*
			 * others may be creating/dropping resource group concurrently,
			 * block until creating/dropping finish to avoid inconsistent
			 * resource group metadata
			 */
			pg_resgroup_rel = relation_open(ResGroupRelationId, ExclusiveLock);

			sscan = systable_beginscan(pg_resgroup_rel, InvalidOid, false,
									   NULL, 0, NULL);
			while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
			{
				bool		isnull;
				char		*groupName = NameStr(*DatumGetName(heap_getattr(tuple,
																			Anum_pg_resgroup_rsgname,
																			pg_resgroup_rel->rd_att,
																			&isnull)));

				if (inGroupName == NULL || strcmp(inGroupName, groupName) == 0)
				{
					Assert(funcctx->max_calls < MaxResourceGroups);
					ctx->groups[funcctx->max_calls].num_running = makeStringInfo();
					ctx->groups[funcctx->max_calls].num_queueing = makeStringInfo();
					ctx->groups[funcctx->max_calls].num_queued = makeStringInfo();
					ctx->groups[funcctx->max_calls].num_executed = makeStringInfo();
					ctx->groups[funcctx->max_calls].total_queue_duration = makeStringInfo();
					ctx->groups[funcctx->max_calls].cpuUsage = makeStringInfo();
					strcpy(NameStr(ctx->groups[funcctx->max_calls++].groupName), groupName);

					if (inGroupName != NULL)
						break;
				}
			}
			systable_endscan(sscan);

			ctx->nGroups = funcctx->max_calls;
			qsort(ctx->groups, ctx->nGroups, sizeof(ctx->groups[0]), compareRow);

			if (ctx->nGroups > 0)
				getResUsage(ctx, inGroupName);

			relation_close(pg_resgroup_rel, ExclusiveLock);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	ctx = (ResGroupStatCtx *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[7];
		bool		nulls[7];
		HeapTuple	tuple;
		char		statVal[MAXDATELEN + 1];
		ResGroupStat *row = &ctx->groups[funcctx->call_cntr];

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		MemSet(statVal, 0, sizeof(statVal));

		values[0] = NameGetDatum(&row->groupName);
		values[1] = CStringGetTextDatum(row->num_running->data);
		values[2] = CStringGetTextDatum(row->num_queueing->data);
		values[3] = CStringGetTextDatum(row->num_queued->data);
		values[4] = CStringGetTextDatum(row->num_executed->data);
		values[5] = CStringGetTextDatum(row->total_queue_duration->data);
		values[6] = CStringGetTextDatum(row->cpuUsage->data);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * Get status of resource groups in key-value style
 */
Datum
pg_resgroup_get_status_kv(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	StringInfoData   str;
	bool             do_dump, do_check;

	do_dump = (!PG_ARGISNULL(0) && strncmp(text_to_cstring(PG_GETARG_TEXT_P(0)), "dump", 4) == 0);
	do_check = (!PG_ARGISNULL(0) && strncmp(text_to_cstring(PG_GETARG_TEXT_P(0)), "check", 5) == 0);
	
	if (do_dump)
	{
		/* Only super user can call this function with para=dump. */
		if (!superuser())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("only superusers can call this function")));
		}
		
		initStringInfo(&str);
		/* dump info in CN and collect info from DNs to form str.*/
		getResGroupInfo(&str, "dump");
	}
	else if (do_check)
	{
		/* Only super user can call this function with para=check. */
		if (!superuser())
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("only superusers can call this function")));
		}

		initStringInfo(&str);
		/* dump info in CN and collect info from DNs to form str.*/
		getResGroupInfo(&str, "check");
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			nattr = 3;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(nattr, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "groupname", NAMEOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "prop", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "value", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->max_calls = do_dump || do_check ? 1 : 0;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		if (do_dump || do_check)
		{
			Datum		values[3];
			bool		nulls[3];
			HeapTuple	tuple;

			MemSet(values, 0, sizeof(values));
			MemSet(nulls, 0, sizeof(nulls));

			nulls[0] = nulls[1] = true;
			values[2] = CStringGetTextDatum(str.data);
			
			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
		}
		else
		{
			SRF_RETURN_DONE(funcctx);
		}
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

static void
getResGroupInfo(StringInfo str, char *cmd)
{
	bool do_dump = (strcmp(cmd, "dump") == 0);
	bool do_check = (strcmp(cmd, "check") == 0);

	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		int               i;
		StringInfoData    str_qd;
		StringInfoData    buffer;
		int count;
		List *cn_list = GetAllCoordNodes(false);
		List *dn_list = GetAllDataNodes();
		PGXCNodeAllHandles * all_handles;
		PGXCNodeHandle *handle;
		ResponseCombiner combiner;
		TupleTableSlot *tupleslot = MakeTupleTableSlot(NULL);

		initStringInfo(&str_qd);
		initStringInfo(&buffer);
		if (do_dump)
			appendStringInfo(&buffer,
							 "select * from pg_resgroup_get_status_kv('dump');");
		else if (do_check)
			appendStringInfo(&buffer,
							 "select * from pg_resgroup_get_status_kv('check');");

		InitResponseCombiner(&combiner, 0, COMBINE_TYPE_NONE);
		all_handles = get_handles(NIL, cn_list, false, true, FIRST_LEVEL, InvalidFid, true, false);
		count = all_handles->co_conn_count;
		combiner.recv_node_count = count;
		combiner.recv_tuples	  = 0;
		combiner.recv_total_time = -1;
		combiner.recv_datarows = 0;
		combiner.ss.ps.ps_ResultTupleSlot = tupleslot;

		LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
		if (do_dump)
			ResGroupDumpInfo(&str_qd);
		else if (do_check)
			ResGroupCheckFreeSlots(&str_qd);
		LWLockRelease(ResGroupLock);

		/* append all qes and qd together to form str */
		appendStringInfo(str, "{\"info\":[%s", str_qd.data);

		for (i = 0; i < count; i++)
		{
			handle = all_handles->coord_handles[i];
			if (pgxc_node_send_query(handle, buffer.data))
				elog(ERROR, "Failed to send cmd to datanodes");
		}

		while (count > 0)
		{
			i = 0;

			if (pgxc_node_receive(count, all_handles->coord_handles, NULL))
				elog(ERROR, "Failed to receive response from datanodes");

			while (i < count)
			{
				int res = handle_response(all_handles->coord_handles[i], &combiner);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_ERROR)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_READY)
				{
					if (i < --count)
						all_handles->coord_handles[i] =
							all_handles->coord_handles[count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ExecSetSlotDescriptor(combiner.ss.ps.ps_ResultTupleSlot,
										  combiner.tuple_desc);
					/* Now slot is responsible for free the descriptor */
					combiner.tuple_desc = NULL;
				}
				else if (res == RESPONSE_DATAROW)
				{
					bool isnull;
					tupleslot = combiner.ss.ps.ps_ResultTupleSlot;
					CopyDataRowTupleToSlot(&combiner, tupleslot);
					combiner.current_conn_rows_consumed++;
					appendStringInfo(str, ",%s",
									 TextDatumGetCString(slot_getattr(tupleslot, 3, &isnull)));
				}
			}
		}

		InitResponseCombiner(&combiner, 0, COMBINE_TYPE_NONE);
		all_handles = get_handles(dn_list, NIL, false, true, FIRST_LEVEL, InvalidFid, true, false);
		count = all_handles->dn_conn_count;
		combiner.recv_node_count = count;
		combiner.recv_tuples	  = 0;
		combiner.recv_total_time = -1;
		combiner.recv_datarows = 0;
		combiner.ss.ps.ps_ResultTupleSlot = tupleslot;

		for (i = 0; i < count; i++)
		{
			handle = all_handles->datanode_handles[i];
			if (pgxc_node_send_query(handle, buffer.data))
				elog(ERROR, "Failed to send cmd to datanodes");
		}

		while (count > 0)
		{
			i = 0;

			if (pgxc_node_receive(count, all_handles->datanode_handles, NULL))
				elog(ERROR, "Failed to receive response from datanodes");

			while (i < count)
			{
				int res = handle_response(all_handles->datanode_handles[i], &combiner);
				if (res == RESPONSE_EOF)
				{
					i++;
				}
				else if (res == RESPONSE_COMPLETE)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_ERROR)
				{
					/* Ignore, wait for ReadyForQuery */
				}
				else if (res == RESPONSE_READY)
				{
					if (i < --count)
						all_handles->datanode_handles[i] =
							all_handles->datanode_handles[count];
				}
				else if (res == RESPONSE_TUPDESC)
				{
					ExecSetSlotDescriptor(combiner.ss.ps.ps_ResultTupleSlot,
										  combiner.tuple_desc);
					/* Now slot is responsible for free the descriptor */
					combiner.tuple_desc = NULL;
				}
				else if (res == RESPONSE_DATAROW)
				{
					bool isnull;
					tupleslot = combiner.ss.ps.ps_ResultTupleSlot;
					CopyDataRowTupleToSlot(&combiner, tupleslot);
					combiner.current_conn_rows_consumed++;
					appendStringInfo(str, ",%s",
									 TextDatumGetCString(slot_getattr(tupleslot, 3, &isnull)));
				}
			}
		}
		appendStringInfo(str, "]}");
	}
	else
	{
		LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
		if (do_dump)
			ResGroupDumpInfo(str);
		else if (do_check)
			ResGroupCheckFreeSlots(str);
		LWLockRelease(ResGroupLock);
	}
}
