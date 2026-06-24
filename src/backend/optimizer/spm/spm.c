/*------------------------------------------------------------------------
 *
 * spm.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/spm/spm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <float.h>
#include <limits.h>
#include <math.h>
#include "catalog/pg_depend.h"
#include "optimizer/spm.h"
#include "optimizer/spm_cache.h"
#include "access/sysattr.h"

PGDLLIMPORT SPMTrackSqlId_hook_type SPMTrackSqlId_hook;

MemoryContext     spm_context = NULL;
LocalSPMPlanInfo *pre_spmplan = NULL;
__thread bool     spmplan_inited = false;

int32             save_spmplan_state = 0;
bool              debug_print_spm = false;
bool              enable_spm = false;
bool              enable_spm_alert = false;
bool              enable_experiment_spm = false;

void
SPMPlanInit(void)
{
	MemoryContext old;

	if (!IS_ACCESS_NODE)
		return;

	spm_context = AllocSetContextCreate(TopMemoryContext, "SPM", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(spm_context);
	pre_spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));
	InitSPMPlanInfo(pre_spmplan, spm_context);
	MemoryContextSwitchTo(old);

	spmplan_inited = true;
	compute_query_id = true;
}

/*
 * remove all spmplan item according to the spmplan_oid
 * this function will be used in cascade drop
 */
void
RemoveSPMPlanById(Oid spmplan_oid, bool history)
{
	Oid         relid;
	int         indexid;
	Relation    rel;
	HeapTuple   oldtup;
	SysScanDesc scandesc;
	const int   scan_keys = 1;
	ScanKeyData entry[scan_keys];

	if (history)
	{
		relid = SPMPlanHistoryRelationId;
		indexid = SPMPlanHistoryOidIndexId;
	}
	else
	{
		relid = SPMPlanRelationId;
		indexid = SPMPlanOidIndexId;
	}

	Assert(OidIsValid(spmplan_oid));

	ScanKeyInit(&entry[0],
	            ObjectIdAttributeNumber,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(spmplan_oid));

	rel = heap_open(relid, RowExclusiveLock);

	scandesc = systable_beginscan(rel, indexid, true, NULL, scan_keys, entry);
	if ((oldtup = systable_getnext(scandesc)) != NULL)
	{
		LocalSPMPlanInfo *deformed_spmplan;

		deformed_spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));
		InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
		DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
		if (relid == SPMPlanRelationId)
		{
			SPMPlanHashTblDelete(MyDatabaseId, deformed_spmplan->sql_id, 
									deformed_spmplan->plan_id);
		}
		else if (relid == SPMPlanHistoryRelationId)
		{
			SPMHistoryHashTblDelete(MyDatabaseId, deformed_spmplan->sql_id, 
									deformed_spmplan->plan_id);
		}
		DeepFreeSPMPlan(deformed_spmplan, false);
		pfree(deformed_spmplan);

		CatalogTupleDelete(rel, &oldtup->t_self);
	}

	systable_endscan(scandesc);
	heap_close(rel, RowExclusiveLock);	

	CommandCounterIncrement();
}

/*
 * disable all spmplan item according to the spmplan_oid
 * this function will be used in cascade drop
 */
void
DisableSPMPlanById(Oid spmplan_oid, bool history)
{
	Oid         relid;
	int         indexid;
	Relation    rel;
	HeapTuple   tup;
	SysScanDesc scandesc;
	const int   scan_keys = 1;
	ScanKeyData entry[scan_keys];
	LocalSPMPlanInfo *deformed_spmplan = NULL;

	if (history)
	{
		relid = SPMPlanHistoryRelationId;
		indexid = SPMPlanHistoryOidIndexId;
	}
	else
	{
		relid = SPMPlanRelationId;
		indexid = SPMPlanOidIndexId;
	}

	Assert(OidIsValid(spmplan_oid));

	deformed_spmplan = (LocalSPMPlanInfo *) 
								palloc0(sizeof(LocalSPMPlanInfo));
	InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);

	ScanKeyInit(&entry[0],
	            ObjectIdAttributeNumber,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(spmplan_oid));

	rel = heap_open(relid, RowExclusiveLock);

	scandesc = systable_beginscan(rel, indexid, true, NULL, scan_keys, entry);
	if ((tup = systable_getnext(scandesc)) != NULL)
	{
		bool  nulls[Natts_pg_spm_plan];
		bool  replaces[Natts_pg_spm_plan];
		Datum values[Natts_pg_spm_plan];

		if (relid == SPMPlanRelationId)
		{
			DeformSPMPlanTupe(rel, tup, deformed_spmplan);
			/* Delete cache in case of transaction rollback */
			SPMPlanHashTblDelete(MyDatabaseId, deformed_spmplan->sql_id, deformed_spmplan->plan_id);
		}

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		values[Anum_pg_spm_plan_enabled - 1] = BoolGetDatum(false);
		replaces[Anum_pg_spm_plan_enabled - 1] = true;

		tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

		CatalogTupleUpdate(rel, &tup->t_self, tup);

		if (relid == SPMPlanRelationId && IS_PGXC_LOCAL_COORDINATOR)
		{
			ExecSendSPMPlan(deformed_spmplan, SPM_PLAN_SEND_PLAN_UPDATE, 
							"enabled", "no");
		}
	}

	systable_endscan(scandesc);
	heap_close(rel, RowExclusiveLock);

	DeepFreeSPMPlan(deformed_spmplan, false);
	pfree(deformed_spmplan);

	CommandCounterIncrement();
}

char *
FormatSpmplan(Oid oid)
{
	char     *result;
	HeapTuple tup;

	tup = SearchSysCache1(SPMPLANOID, ObjectIdGetDatum(oid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_spm_plan spmplan_form = (Form_pg_spm_plan) GETSTRUCT(tup);
		char            *name = NameStr(spmplan_form->spmname);
		char            *nspname;
		StringInfoData   buf;

		/* XXX no support here for bootstrap mode */
		Assert(!IsBootstrapProcessingMode());
		initStringInfo(&buf);

		nspname = get_namespace_name(spmplan_form->spmnsp);

		appendStringInfo(&buf, "%s", quote_qualified_identifier(nspname, name));
		result = buf.data;
		ReleaseSysCache(tup);
	}
	else
	{
		/* If OID doesn't match any pg_spm_plan entry, return it numerically */
		result = (char *) palloc0(NAMEDATALEN);
		snprintf(result, NAMEDATALEN, "%u", oid);
	}

	return result;
}

char *
FormatSpmplanHistory(Oid oid)
{
	char       *result;
	HeapTuple   tup;
	Relation    rel;
	SysScanDesc scandesc;
	const int   scan_keys = 1;
	ScanKeyData entry[scan_keys];

	rel = heap_open(SPMPlanHistoryRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
	            ObjectIdAttributeNumber,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(oid));

	scandesc = systable_beginscan(rel, SPMPlanHistoryOidIndexId, true, NULL, scan_keys, entry);
	tup = systable_getnext(scandesc);

	if (HeapTupleIsValid(tup))
	{
		Form_pg_spm_plan spmplan_form = (Form_pg_spm_plan) GETSTRUCT(tup);
		char            *name = NameStr(spmplan_form->spmname);
		char            *nspname;
		StringInfoData   buf;

		/* XXX no support here for bootstrap mode */
		Assert(!IsBootstrapProcessingMode());
		initStringInfo(&buf);

		nspname = get_namespace_name(spmplan_form->spmnsp);

		appendStringInfo(&buf, "%s", quote_qualified_identifier(nspname, name));
		result = buf.data;
	}
	else
	{
		/* If OID doesn't match any pg_spm_plan_history entry, return it numerically */
		result = (char *) palloc0(NAMEDATALEN);
		snprintf(result, NAMEDATALEN, "%u", oid);
	}

	systable_endscan(scandesc);
	heap_close(rel, AccessShareLock);

	return result;
}

void
RemoveSPMPlanByRelId(Oid relid)
{
	Relation    depRel;
	ScanKeyData key[3];
	SysScanDesc scan;
	HeapTuple   tup;

	depRel = heap_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
	            Anum_pg_depend_refclassid,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&key[1],
	            Anum_pg_depend_refobjid,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(relid));
	ScanKeyInit(&key[2],
	            Anum_pg_depend_refobjsubid,
	            BTEqualStrategyNumber,
	            F_OIDEQ,
	            ObjectIdGetDatum(InvalidOid));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 3, key);

	while (HeapTupleIsValid((tup = systable_getnext(scan))))
	{
		Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

		if (depform->classid == SPMPlanRelationId || depform->classid == SPMPlanHistoryRelationId)
		{
			RemoveSPMPlanById(depform->objid, depform->classid == SPMPlanHistoryRelationId);
		}
	}

	systable_endscan(scan);
	heap_close(depRel, AccessShareLock);
}

void
ResetSPMPlan(LocalSPMPlanInfo *spmplan)
{
	if (!SPM_ENABLED)
		return;

	if (spmplan->reloids)
	{
		pfree(spmplan->reloids);
		spmplan->reloids = NULL;
	}

	if (spmplan->indexoids)
	{
		pfree(spmplan->indexoids);
		spmplan->indexoids = NULL;
	}

	if (spmplan->relnames)
	{
		pfree(spmplan->relnames);
		spmplan->relnames = NULL;
	}

	if (spmplan->indexnames)
	{
		pfree(spmplan->indexnames);
		spmplan->indexnames = NULL;
	}

	spmplan->spmnsp = 0;
	spmplan->ownerid = 0;
	spmplan->sql_id = 0;
	spmplan->plan_id = 0;
	spmplan->hint_id = 0;
	spmplan->priority = 0;
	spmplan->enabled = false;
	spmplan->autopurge = false;
	spmplan->executions = 0;
	spmplan->created = 0;
	spmplan->last_modified = 0;
	spmplan->execute_time = 0;
	spmplan->last_verified = 0;
	spmplan->version = 0;
	spmplan->elapsed_time = 0;
	spmplan->task_id = 0;
	spmplan->sql_normalize_rule = 0;
	spmplan->cost = 0;
	memset(spmplan->spmname.data, 0, NAMEDATALEN);
	memset(spmplan->origin.data, 0, NAMEDATALEN);
	memset(spmplan->hint, 0, spmplan->hint_len);
	memset(spmplan->hint_detail, 0, spmplan->hint_detail_len);
	memset(spmplan->guc_list, 0, spmplan->guc_list_len);
	memset(spmplan->sql_normalize, 0, spmplan->sql_normalize_len);
	memset(spmplan->sql, 0, spmplan->sql_len);
	memset(spmplan->plan, 0, spmplan->plan_len);
	memset(spmplan->param_list, 0, spmplan->param_list_len);
	memset(spmplan->description, 0, spmplan->description_len);
}

void
DeepFreeSPMPlan(LocalSPMPlanInfo *spmplan, bool force)
{
	if (!SPM_ENABLED)
		return;

	if (force)
	{
		if (spmplan->reloids)
		{
			pfree(spmplan->reloids);
			spmplan->reloids = NULL;
		}

		if (spmplan->indexoids)
		{
			pfree(spmplan->indexoids);
			spmplan->indexoids = NULL;
		}

		if (spmplan->relnames)
		{
			pfree(spmplan->relnames);
			spmplan->relnames = NULL;
		}

		if (spmplan->indexnames)
		{
			pfree(spmplan->indexnames);
			spmplan->indexnames = NULL;
		}
	}

	if (spmplan->hint)
		pfree(spmplan->hint);
	
	if (spmplan->hint_detail)
		pfree(spmplan->hint_detail);
	
	if (spmplan->guc_list)
		pfree(spmplan->guc_list);
	
	if (spmplan->sql_normalize)
		pfree(spmplan->sql_normalize);

	if (spmplan->sql)
		pfree(spmplan->sql);
	
	if (spmplan->plan)
		pfree(spmplan->plan);
	
	if (spmplan->param_list)
		pfree(spmplan->param_list);
	
	if (spmplan->description)
		pfree(spmplan->description);
}

void
InitSPMPlanInfo(LocalSPMPlanInfo *spmplan, MemoryContext cxt)
{
	MemoryContext old;

	Assert(spmplan != NULL);
	old = MemoryContextSwitchTo(cxt);

	spmplan->hint = (char *) palloc0(SPM_PLAN_HINT_DEFAULT_LEN);
	spmplan->hint_len = SPM_PLAN_HINT_DEFAULT_LEN;

	spmplan->hint_detail = (char *) palloc0(SPM_PLAN_HINT_DETAIL_DEFAULT_LEN);
	spmplan->hint_detail_len = SPM_PLAN_HINT_DETAIL_DEFAULT_LEN;

	spmplan->guc_list = (char *) palloc0(SPM_PLAN_GUC_LIST_DEFAULT_LEN);
	spmplan->guc_list_len = SPM_PLAN_GUC_LIST_DEFAULT_LEN;

	spmplan->sql_normalize = (char *) palloc0(SPM_PLAN_SQL_DEFAULT_LEN);
	spmplan->sql_normalize_len = SPM_PLAN_SQL_DEFAULT_LEN;

	spmplan->sql = (char *) palloc0(SPM_PLAN_SQL_DEFAULT_LEN);
	spmplan->sql_len = SPM_PLAN_SQL_DEFAULT_LEN;

	spmplan->plan = (char *) palloc0(SPM_PLAN_PLAN_DEFAULT_LEN);
	spmplan->plan_len = SPM_PLAN_PLAN_DEFAULT_LEN;

	spmplan->param_list = (char *) palloc0(SPM_PLAN_PARAM_DEFAULT_LEN);
	spmplan->param_list_len = SPM_PLAN_PARAM_DEFAULT_LEN;

	spmplan->description = (char *) palloc0(SPM_PLAN_DESC_DEFAULT_LEN);
	spmplan->description_len = SPM_PLAN_DESC_DEFAULT_LEN;

	MemoryContextSwitchTo(old);
}

void
EnlargeSPMPlanInfoMember(char **member, int *member_len, int needed)
{
	int newlen;

	if (*member_len > needed)
		return;

	/* should not happen */
	if (needed < 0)
		elog(ERROR, "[SPM] invalid string enlargement request size: %d", needed);

	needed += 1; /* total space required now */
	if (((Size) needed) >= MaxAllocSize)
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
		         errmsg("out of memory"),
		         errdetail("Cannot enlarge string buffer by %d more bytes.", needed)));

	Assert(*member_len > 0);

	/*
	 * We don't want to allocate just a little more space with each enlarge;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * (*member_len);
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	*member = (char *) repalloc(*member, newlen);
	*member_len = newlen;
}

void
ShrinkSPMPlanInfo(LocalSPMPlanInfo *spmplan)
{
	Assert(spmplan != NULL);

	if (spmplan->hint_len > SPM_PLAN_HINT_MAX_LEN)
	{
		spmplan->hint = (char *) repalloc(spmplan->hint, SPM_PLAN_HINT_DEFAULT_LEN);
		spmplan->hint_len = SPM_PLAN_HINT_DEFAULT_LEN;
	}

	if (spmplan->hint_detail_len > SPM_PLAN_HINT_DETAIL_MAX_LEN)
	{
		spmplan->hint_detail = (char *) repalloc(spmplan->hint_detail, SPM_PLAN_HINT_DETAIL_DEFAULT_LEN);
		spmplan->hint_detail_len = SPM_PLAN_HINT_DETAIL_DEFAULT_LEN;
	}

	if (spmplan->guc_list_len > SPM_PLAN_GUC_LIST_MAX_LEN)
	{
		spmplan->guc_list = (char *) repalloc(spmplan->guc_list, SPM_PLAN_GUC_LIST_DEFAULT_LEN);
		spmplan->guc_list_len = SPM_PLAN_GUC_LIST_DEFAULT_LEN;
	}

	if (spmplan->sql_normalize_len > SPM_PLAN_SQL_MAX_LEN)
	{
		spmplan->sql_normalize = (char *) repalloc(spmplan->sql_normalize, SPM_PLAN_SQL_DEFAULT_LEN);
		spmplan->sql_normalize_len = SPM_PLAN_SQL_DEFAULT_LEN;
	}

	if (spmplan->sql_len > SPM_PLAN_SQL_MAX_LEN)
	{
		spmplan->sql = (char *) repalloc(spmplan->sql, SPM_PLAN_SQL_DEFAULT_LEN);
		spmplan->sql_len = SPM_PLAN_SQL_DEFAULT_LEN;
	}

	if (spmplan->plan_len > SPM_PLAN_PLAN_MAX_LEN)
	{
		spmplan->plan = (char *) repalloc(spmplan->plan, SPM_PLAN_PLAN_DEFAULT_LEN);
		spmplan->plan_len = SPM_PLAN_PLAN_DEFAULT_LEN;
	}

	if (spmplan->param_list_len > SPM_PLAN_PARAM_MAX_LEN)
	{
		spmplan->param_list = (char *) repalloc(spmplan->param_list, SPM_PLAN_PARAM_DEFAULT_LEN);
		spmplan->param_list_len = SPM_PLAN_PARAM_DEFAULT_LEN;
	}

	if (spmplan->description_len > SPM_PLAN_DESC_MAX_LEN)
	{
		spmplan->description = (char *) repalloc(spmplan->description, SPM_PLAN_DESC_DEFAULT_LEN);
		spmplan->description_len = SPM_PLAN_DESC_DEFAULT_LEN;
	}
}