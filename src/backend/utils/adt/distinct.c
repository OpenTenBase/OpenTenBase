/*-------------------------------------------------------------------------
 *
 * distinct.c
 *	  Functions for distinct.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/distinct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "libpq/pqformat.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "miscadmin.h"

typedef struct TupleSortTable
{
	Tuplesortstate *sortstate;
	bool			sort_done;
	uint32			dist_members;
} TupleSortTable;


/*-------------------------------------------------------------------------
 *                             basic functions
 *-------------------------------------------------------------------------
 */
static TupleHashTable
create_distinct_hashtable(AggState *aggstate)
{
	TupleHashTable hashtable;
	AggStatePerTrans pertrans;
	MemoryContext oldcxt;
	AttrNumber *keyColIdx;
	Oid *eqfuncoids;

	pertrans = aggstate->curpertrans;
	oldcxt = MemoryContextSwitchTo(aggstate->dist_optcxt);

	keyColIdx = (AttrNumber *)palloc(sizeof(AttrNumber) * pertrans->numDistinctCols);
	eqfuncoids = (Oid *)palloc(sizeof(Oid) * pertrans->numDistinctCols);
	keyColIdx[0] = 1;
	eqfuncoids[0] = pertrans->equalfnOne.fn_oid;

	hashtable = BuildTupleHashTableExt(&aggstate->ss.ps,
										pertrans->sortdesc,
										pertrans->numDistinctCols,
										keyColIdx,
										eqfuncoids,
										&pertrans->distinct_hashfn,
										pertrans->aggref->distinct_num,
										0,
										aggstate->dist_optcxt,
										aggstate->dist_optcxt,
										aggstate->tmpcontext->ecxt_per_tuple_memory,
										false);

	MemoryContextSwitchTo(oldcxt);

	return hashtable;
}

static TupleSortTable *
create_distinct_sortstate(AggState *aggstate)
{
	TupleSortTable *sorttable;
	AggStatePerTrans pertrans;
	Form_pg_attribute attr;
	SortGroupClause *sortcl;
	TargetEntry *tle;
	MemoryContext oldcxt;

	pertrans = aggstate->curpertrans;
	attr = TupleDescAttr(pertrans->sortdesc, 0);
	sortcl = (SortGroupClause *) linitial(pertrans->aggref->aggdistinct);
	tle = (TargetEntry *) linitial(pertrans->aggref->distinct_args);

	oldcxt = MemoryContextSwitchTo(aggstate->dist_optcxt);

	sorttable = (TupleSortTable *)palloc0(sizeof(TupleSortTable));

	sorttable->sortstate = tuplesort_begin_datum(attr->atttypid,
												 sortcl->sortop,
												 exprCollation((Node *) tle->expr),
												 sortcl->nulls_first,
												 work_mem, false);

	MemoryContextSwitchTo(oldcxt);

	return sorttable;
}

static inline void
serialize_datum(StringInfo buf, Form_pg_attribute attr, Datum value)
{
	char   *pstring;
	int		len;

	if (attr->attbyval)
	{
		Assert(attr->attlen > 0);
		pstring = (char *)&value;
		/* no need to send len */
		pq_sendbytes(buf, pstring, attr->attlen);
	}
	else if (attr->attlen == -1)
	{
		pstring = DatumGetPointer(value);
		len = VARSIZE_ANY(value);
		pq_sendint32(buf, len);
		pq_sendbytes(buf, pstring, len);
	}
	else if (attr->attlen == -2)
	{
		pstring = DatumGetPointer(value);
		len = strlen(pstring) + 1;
		pq_sendint32(buf, len);
		pq_sendbytes(buf, pstring, len);
	}
	else
	{
		pstring = DatumGetPointer(value);
		len = attr->attlen;
		/* no need to send len */
		Assert(len > 0);
		pq_sendbytes(buf, pstring, len);
	}
}

static void
combine_hash_internal(TupleHashTable hashtable, TupleTableSlot *sortslot,
					  StringInfo buf)
{
	Form_pg_attribute attr;
	Datum		value;
	int			len;
	uint32		members, i;
	bool		isnew;

	attr = TupleDescAttr(sortslot->tts_tupleDescriptor, 0);
	len = attr->attlen;

	members = pq_getmsgint(buf, 4);
	for (i = 0; i < members; i++)
	{
		if (attr->attbyval)
		{
			Assert(attr->attlen > 0);
			memcpy(&value, pq_getmsgbytes(buf, len), len);
		}
		else if (attr->attlen == -1 || attr->attlen == -2)
		{
			len = pq_getmsgint(buf, 4);
			value = PointerGetDatum(pq_getmsgbytes(buf, len));
		}
		else
		{
			value = PointerGetDatum(pq_getmsgbytes(buf, len));
		}

		ExecClearTuple(sortslot);
		sortslot->tts_values[0] = value;
		ExecStoreVirtualTuple(sortslot);

		(void) LookupTupleHashEntry(hashtable, sortslot, &isnew, NULL);
	}
	pq_getmsgend(buf);
}

static void
combine_sort_internal(Tuplesortstate *sortstate, TupleTableSlot *sortslot,
					  StringInfo buf)
{
	Form_pg_attribute attr;
	Datum		value;
	int			len;
	uint32		members, i;

	attr = TupleDescAttr(sortslot->tts_tupleDescriptor, 0);
	len = attr->attlen;

	members = pq_getmsgint(buf, 4);
	for (i = 0; i < members; i++)
	{
		if (attr->attbyval)
		{
			Assert(attr->attlen > 0);
			memcpy(&value, pq_getmsgbytes(buf, len), len);
		}
		else if (attr->attlen == -1 || attr->attlen == -2)
		{
			len = pq_getmsgint(buf, 4);
			value = PointerGetDatum(pq_getmsgbytes(buf, len));
		}
		else
		{
			value = PointerGetDatum(pq_getmsgbytes(buf, len));
		}

		tuplesort_putdatum(sortstate, value, false);
	}
	pq_getmsgend(buf);
}

/*-------------------------------------------------------------------------
 *                            accum functions
 *-------------------------------------------------------------------------
 */
static Datum
distinct_accum_hash(FunctionCallInfo fcinfo)
{
	TupleHashTable hashtable;
	AggState *aggstate = ((AggState *) fcinfo->context);

	if (PG_ARGISNULL(0))
		hashtable = create_distinct_hashtable(aggstate);
	else
		hashtable = (TupleHashTable) PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
	{
		bool		isnew;
		TupleTableSlot *sortslot = aggstate->curpertrans->sortslot;

		ExecClearTuple(sortslot);
		/* make sure we have detoast value */
		if (TupleDescAttr(sortslot->tts_tupleDescriptor, 0)->attlen == -1)
			sortslot->tts_values[0] = PointerGetDatum(PG_GETARG_VARLENA_P(1));
		else
			sortslot->tts_values[0] = PG_GETARG_DATUM(1);
		ExecStoreVirtualTuple(sortslot);

		(void) LookupTupleHashEntry(hashtable, sortslot, &isnew, NULL);
	}

	PG_RETURN_POINTER(hashtable);
}

static Datum
distinct_accum_sort(FunctionCallInfo fcinfo)
{
	TupleSortTable *sorttable;
	AggState *aggstate = ((AggState *) fcinfo->context);

	if (PG_ARGISNULL(0))
		sorttable = create_distinct_sortstate(aggstate);
	else
		sorttable = (TupleSortTable *) PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
	{
		TupleTableSlot *sortslot = aggstate->curpertrans->sortslot;

		/* make sure we have detoast value */
		if (TupleDescAttr(sortslot->tts_tupleDescriptor, 0)->attlen == -1)
			tuplesort_putdatum(sorttable->sortstate, PointerGetDatum(PG_GETARG_VARLENA_P(1)), false);
		else
			tuplesort_putdatum(sorttable->sortstate, PG_GETARG_DATUM(1), false);
	}

	PG_RETURN_POINTER(sorttable);
}

Datum
distinct_accum(PG_FUNCTION_ARGS)
{
	AggStatePerTrans curpertrans;

	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "distinct aggregate called in non-aggregate context");

	curpertrans = ((AggState *) fcinfo->context)->curpertrans;
	/* set if the first time through */
	if (curpertrans->aggref->distinct_num > 0)
	{
		fcinfo->flinfo->fn_addr = distinct_accum_hash;
		return distinct_accum_hash(fcinfo);
	}
	else
	{
		fcinfo->flinfo->fn_addr = distinct_accum_sort;
		return distinct_accum_sort(fcinfo);
	}
}

/*-------------------------------------------------------------------------
 *                            combine functions
 *-------------------------------------------------------------------------
 */
static Datum
distinct_combine_hash(FunctionCallInfo fcinfo)
{
	TupleHashTable hashtable;
	bytea	   *values;
	StringInfoData buf;
	AggState *aggstate = ((AggState *) fcinfo->context);

	initStringInfo(&buf);

	if (PG_ARGISNULL(0))
		hashtable = create_distinct_hashtable(aggstate);
	else
		hashtable = (TupleHashTable) PG_GETARG_POINTER(0);

	values = PG_GETARG_BYTEA_PP(1);
	appendBinaryStringInfo(&buf, VARDATA_ANY(values), VARSIZE_ANY_EXHDR(values));

	combine_hash_internal(hashtable, aggstate->curpertrans->sortslot, &buf);

	pfree(buf.data);

	PG_RETURN_POINTER(hashtable);
}

static Datum
distinct_combine_sort(FunctionCallInfo fcinfo)
{
	TupleSortTable *sorttable;
	bytea	   *values;
	StringInfoData buf;
	AggState *aggstate = ((AggState *) fcinfo->context);

	initStringInfo(&buf);

	if (PG_ARGISNULL(0))
		sorttable = create_distinct_sortstate(aggstate);
	else
		sorttable = (TupleSortTable *) PG_GETARG_POINTER(0);

	values = PG_GETARG_BYTEA_PP(1);
	appendBinaryStringInfo(&buf, VARDATA_ANY(values), VARSIZE_ANY_EXHDR(values));

	combine_sort_internal(sorttable->sortstate, aggstate->curpertrans->sortslot, &buf);

	pfree(buf.data);

	PG_RETURN_POINTER(sorttable);
}

Datum
distinct_combine(PG_FUNCTION_ARGS)
{
	AggStatePerTrans curpertrans;

	if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
		elog(ERROR, "distinct aggregate called in non-aggregate context");

	curpertrans = ((AggState *) fcinfo->context)->curpertrans;
	/* set if the first time through */
	if (curpertrans->aggref->distinct_num > 0)
	{
		fcinfo->flinfo->fn_addr = distinct_combine_hash;
		return distinct_combine_hash(fcinfo);
	}
	else
	{
		fcinfo->flinfo->fn_addr = distinct_combine_sort;
		return distinct_combine_sort(fcinfo);
	}
}

/*-------------------------------------------------------------------------
 *                            serialize functions
 *-------------------------------------------------------------------------
 */
static Datum
distinct_serialize_hash(FunctionCallInfo fcinfo)
{
	TupleHashTable hashtable;
	StringInfoData buf;
	bytea	   *result;
	TupleHashIterator hashiter;
	TupleHashEntry entry;
	Form_pg_attribute attr;
	Datum		value;
	bool		isnull;

	hashtable = (TupleHashTable) PG_GETARG_POINTER(0);
	attr = TupleDescAttr(hashtable->tableslot->tts_tupleDescriptor, 0);

	pq_begintypsend(&buf);
	pq_sendint32(&buf, hashtable->hashtab->members);

	InitTupleHashIterator(hashtable, &hashiter);
	while ((entry = ScanTupleHashTable(hashtable, &hashiter)) != NULL)
	{
		ExecStoreMinimalTuple(entry->firstTuple, hashtable->tableslot, false);
		value = slot_getattr(hashtable->tableslot, 1, &isnull);

		serialize_datum(&buf, attr, value);
	}

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

static Datum
distinct_serialize_sort(FunctionCallInfo fcinfo)
{
	TupleSortTable *sorttable;
	AggStatePerTrans pertrans;
	Form_pg_attribute attr;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;
	StringInfoData buf;
	bytea	   *result;
	uint32		members = 0;
	int			members_pos;
	Datum		oldVal = (Datum) 0;
	bool		haveOldVal = false;
	Datum		newAbbrev = (Datum) 0;
	Datum		oldAbbrev = (Datum) 0;
	Datum		newVal;
	bool		isNull;
	AggState *aggstate = ((AggState *) fcinfo->context);

	sorttable = (TupleSortTable *) PG_GETARG_POINTER(0);
	pertrans = &aggstate->pertrans[aggstate->curperagg->transno];
	attr = TupleDescAttr(pertrans->sortdesc, 0);
	tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory;
	oldcxt = CurrentMemoryContext;

	pq_begintypsend(&buf);
	/* get buf pos of members */
	members_pos = buf.len;
	pq_sendint32(&buf, members);

	/* tuplesort_getdatum will palloc datum if not attbyval */
	if (!attr->attbyval)
		MemoryContextSwitchTo(tmpcxt);

	if (!sorttable->sort_done)
	{
		tuplesort_performsort(sorttable->sortstate);
		sorttable->sort_done = true;
	}
	else
		elog(ERROR, "partial agg should not be rescan without param");

	while (tuplesort_getdatum(sorttable->sortstate, true, &newVal, &isNull, &newAbbrev))
	{
		if (haveOldVal && oldAbbrev == newAbbrev &&
			DatumGetBool(FunctionCall2(&pertrans->equalfnOne,
										oldVal, newVal)))
			continue;

		if (attr->attbyval)
		{
			serialize_datum(&buf, attr, newVal);
		}
		else
		{
			/* buf need to remain in old memory context */
			MemoryContextSwitchTo(oldcxt);
			serialize_datum(&buf, attr, newVal);
			MemoryContextSwitchTo(tmpcxt);
		}

		oldVal = newVal;
		oldAbbrev = newAbbrev;
		haveOldVal = true;
		members++;
	}
	tuplesort_end(sorttable->sortstate);

	if (!attr->attbyval)
	{
		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(tmpcxt);
	}

	/* update members */
	members = pg_hton32(members);
	memcpy(buf.data + members_pos, &members, sizeof(int32));
	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

Datum
distinct_serialize(PG_FUNCTION_ARGS)
{
	AggStatePerAgg curperagg;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "distinct aggregate called in non-aggregate context");

	curperagg = ((AggState *) fcinfo->context)->curperagg;
	/* set if the first time through */
	if (curperagg->aggref->distinct_num > 0)
	{
		fcinfo->flinfo->fn_addr = distinct_serialize_hash;
		return distinct_serialize_hash(fcinfo);
	}
	else
	{
		fcinfo->flinfo->fn_addr = distinct_serialize_sort;
		return distinct_serialize_sort(fcinfo);
	}
}

/*-------------------------------------------------------------------------
 *                           deserialize functions
 *-------------------------------------------------------------------------
 */
Datum
distinct_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *values;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "distinct aggregate called in non-aggregate context");

	/* handle in combine and do nothing here */
	values = PG_GETARG_BYTEA_PP(0);

	PG_RETURN_BYTEA_P(values);
}

/*-------------------------------------------------------------------------
 *                             final functions
 *-------------------------------------------------------------------------
 */
static Datum
distinct_final_hash(FunctionCallInfo fcinfo)
{
	TupleHashTable hashtable;

	if (PG_ARGISNULL(0))
	{
		if (ORA_MODE)
			return DirectFunctionCall1(int4_numeric,Int64GetDatum(0));
		else
			PG_RETURN_UINT32(0);
	}

	hashtable = (TupleHashTable) PG_GETARG_POINTER(0);
	if (ORA_MODE)
		return DirectFunctionCall1(int4_numeric,Int64GetDatum((int64)(hashtable->hashtab->members)));
	else
		PG_RETURN_UINT32(hashtable->hashtab->members);
}

static Datum
distinct_final_sort(FunctionCallInfo fcinfo)
{
	TupleSortTable *sorttable;
	AggStatePerTrans pertrans;
	Form_pg_attribute attr;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;
	Datum		oldVal = (Datum) 0;
	bool		haveOldVal = false;
	Datum		newAbbrev = (Datum) 0;
	Datum		oldAbbrev = (Datum) 0;
	Datum		newVal;
	bool		isNull;
	uint32		members = 0;
	AggState *aggstate = ((AggState *) fcinfo->context);

	if (PG_ARGISNULL(0))
	{
		if (ORA_MODE)
			return DirectFunctionCall1(int4_numeric,Int64GetDatum(0));
		else
			PG_RETURN_UINT32(0);
	}

	sorttable = (TupleSortTable *) PG_GETARG_POINTER(0);
	pertrans = &aggstate->pertrans[aggstate->curperagg->transno];
	attr = TupleDescAttr(pertrans->sortdesc, 0);
	tmpcxt = aggstate->tmpcontext->ecxt_per_tuple_memory;
	oldcxt = CurrentMemoryContext;

	if (!sorttable->sort_done)
	{
		/* tuplesort_getdatum will palloc if not attbyval */
		if (!attr->attbyval)
			MemoryContextSwitchTo(tmpcxt);

		tuplesort_performsort(sorttable->sortstate);
		while (tuplesort_getdatum(sorttable->sortstate, true, &newVal, &isNull, &newAbbrev))
		{
			if (haveOldVal && oldAbbrev == newAbbrev &&
				DatumGetBool(FunctionCall2(&pertrans->equalfnOne,
											oldVal, newVal)))
				continue;

			oldVal = newVal;
			oldAbbrev = newAbbrev;
			haveOldVal = true;
			members++;
		}
		tuplesort_end(sorttable->sortstate);

		if (!attr->attbyval)
		{
			MemoryContextSwitchTo(oldcxt);
			MemoryContextReset(tmpcxt);
		}

		sorttable->sort_done = true;
		sorttable->dist_members = members;
	}
	if (ORA_MODE)
		return DirectFunctionCall1(int4_numeric,Int64GetDatum((int64)(sorttable->dist_members)));
	else
		PG_RETURN_UINT32(sorttable->dist_members);
}

Datum
distinct_final(PG_FUNCTION_ARGS)
{
	AggStatePerAgg curperagg;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "distinct aggregate called in non-aggregate context");

	curperagg = ((AggState *) fcinfo->context)->curperagg;
	/* set if the first time through */
	if (curperagg->aggref->distinct_num > 0)
	{
		fcinfo->flinfo->fn_addr = distinct_final_hash;
		return distinct_final_hash(fcinfo);
	}
	else
	{
		fcinfo->flinfo->fn_addr = distinct_final_sort;
		return distinct_final_sort(fcinfo);
	}
}
