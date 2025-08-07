/*------------------------------------------------------------------------
 *
 * spm_apply.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/spm/spm_apply.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <float.h>
#include <limits.h>
#include <math.h>
#include "optimizer/spm.h"
#include "optimizer/spm_cache.h"
#include "utils/catcache.h"

int optimizer_use_sql_plan_baselines = SPM_APPLY_ON;

bool
SearchSPMPlan(int64 sql_id, int64 *plan_id)
{
	bool      ret = false;
	HeapTuple oldtup = NULL;
	CatCList *memlist;
	int       i;
	SPMPlanCacheRetNo cacheret;

	if (!SPM_ENABLED || sql_id <= 1 || (plan_id != NULL && *plan_id <= 1))
	{
		return ret;
	}

	cacheret = SPMPlanCacheSearchByPlanId(MyDatabaseId, sql_id, *plan_id, NULL, NULL);
	if (cacheret == SPM_PLAN_CACHE_MATCHED)
	{
		return true;
	}
	else if (cacheret == SPM_PLAN_CACHE_NO_VALID)
	{
		return false;
	}

	memlist = SearchSysCacheList1(SPMPLANSQLID, ObjectIdGetDatum(sql_id));
	for (i = 0; i < memlist->n_members; i++)
	{
		Assert(memlist->n_members == 1);
		oldtup = &memlist->members[i]->tuple;
	}

	if (HeapTupleIsValid(oldtup))
	{
		if (plan_id == NULL)
			ret = true;
		else
		{
			Relation          rel = heap_open(SPMPlanRelationId, AccessShareLock);
			LocalSPMPlanInfo *deformed_spmplan =
				(LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));
			InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);

			DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
			heap_close(rel, AccessShareLock);

			if (cacheret == SPM_PLAN_CACHE_NO_ENTRY)
			{
				SPMPlanHashTabInsert(deformed_spmplan->sql_id,
									deformed_spmplan->plan_id,
									deformed_spmplan->hint,
									deformed_spmplan->priority,
									deformed_spmplan->enabled,
									deformed_spmplan->executions);
			}

			if (deformed_spmplan->plan_id == *plan_id)
			{
				ret = true;
				if (cacheret == SPM_PLAN_CACHE_NO_MATCHED)
				{
					SPMPlanHashTabInsert(deformed_spmplan->sql_id,
										deformed_spmplan->plan_id,
										deformed_spmplan->hint,
										deformed_spmplan->priority,
										deformed_spmplan->enabled,
										deformed_spmplan->executions);
				}
			}
			DeepFreeSPMPlan(deformed_spmplan, false);
			pfree(deformed_spmplan);
		}
	}
	else
	{
		ret = false;

		/* Insert an invalid spm cache entry */
		SPMPlanHashTabInsert(sql_id, INVALID_SPM_CACHE_PLAN_ID, NULL, 0, 
								false, 0);
	}

	ReleaseSysCacheList(memlist);
	return ret;
}

bool
ChooseSPMPlan(int64 sql_id, bool output_detail)
{
	return ChooseSPMPlanInternal(sql_id, output_detail, NULL);
}

bool
ChooseSPMPlanInternal(int64 sql_id, bool output_detail, char **hint)
{
	bool              ret = false;
	HeapTuple         oldtup = NULL;
	int               level = (debug_print_spm) ? LOG : DEBUG5;
	char             *cache_hint = NULL;
	int64             cache_plan_id = 0;
	SPMPlanCacheRetNo cacheret;
	CatCList         *memlist;
	int               i;

	if (!SPM_ENABLED || !optimizer_use_sql_plan_baselines || sql_id <= 1 || NOSPM_DURING_UPGRADE ||
	    CAP_SPM_MANUAL)
	{
		return ret;
	}

	cacheret = GetSPMBindHint(sql_id, &cache_plan_id, &cache_hint);
	if (cacheret == SPM_PLAN_CACHE_FIXED)
	{
		if (hint)
			*hint = cache_hint;

		if (output_detail)
			elog(level, "[SPM] choose spmplan sql_id[%ld], plan_id[%ld]", sql_id, cache_plan_id);

		return true;
	}
	if (cacheret != SPM_PLAN_CACHE_NO_ENTRY)
		return false;
	
	memlist = SearchSysCacheList1(SPMPLANSQLID, ObjectIdGetDatum(sql_id));
	for (i = 0; i < memlist->n_members; i++)
	{
		Assert(memlist->n_members == 1);
		oldtup = &memlist->members[i]->tuple;
	}

	if (HeapTupleIsValid(oldtup))
	{
		Relation          rel = heap_open(SPMPlanRelationId, AccessShareLock);
		LocalSPMPlanInfo *deformed_spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));

		InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
		DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
		heap_close(rel, AccessShareLock);

		if (cacheret == SPM_PLAN_CACHE_NO_ENTRY)
		{
			SPMPlanHashTabInsert(deformed_spmplan->sql_id,
								deformed_spmplan->plan_id,
								deformed_spmplan->hint,
								deformed_spmplan->priority,
								deformed_spmplan->enabled,
								deformed_spmplan->executions);
		}
		if (deformed_spmplan->enabled && deformed_spmplan->priority == SPM_PLAN_FIXED)
		{
			ret = true;

			if (hint)
				*hint = pstrdup(deformed_spmplan->hint);

			if (output_detail)
				elog(level,
				     "[SPM] choose spmplan sql_id[%ld], plan_id[%ld]",
				     deformed_spmplan->sql_id,
				     deformed_spmplan->plan_id);

			if (cacheret == SPM_PLAN_CACHE_NO_MATCHED)
			{
				SPMPlanHashTabInsert(deformed_spmplan->sql_id,
									deformed_spmplan->plan_id,
									deformed_spmplan->hint,
									deformed_spmplan->priority,
									deformed_spmplan->enabled,
									deformed_spmplan->executions);
			}
		}
		else
		{
			ret = false;
		}

		DeepFreeSPMPlan(deformed_spmplan, false);
		pfree(deformed_spmplan);
	}
	else
	{
		ret = false;

		/* Insert an invalid spm cache entry */
		SPMPlanHashTabInsert(sql_id, INVALID_SPM_CACHE_PLAN_ID, NULL, 0, 
								false, 0);
	}

	ReleaseSysCacheList(memlist);

	return ret;
}

static void
SPMPrintDataDetail(void)
{
	int level = (debug_print_spm) ? LOG: DEBUG5;

	if (!SPM_ENABLED)
		return;

	elog(level,
	     "[SPM] SPMPrintDataDetail: sql_id: %ld, plan_id: %ld, use spmplan: %s, param_list: %s",
	     pre_spmplan->sql_id,
	     pre_spmplan->plan_id,
	     (save_spmplan_state & SAVE_SPMPLAN_APPLYED) ? "true" : "false",
	     (strlen(pre_spmplan->param_list) > 0) ? pre_spmplan->param_list : "null");
}

bool
ValidateSPMPlan(int64 sql_id, int64 plan_id)
{
	HeapTuple oldtup = NULL;
	bool      ret = true;
	int       level = LOG;
	int64     cache_planid;
	CatCList *memlist;
	int       i;
	SPMPlanCacheRetNo cacheret;

	/* Output some spm positional information. */
	SPMPrintDataDetail();

	if (!SPM_ENABLED || !enable_spm_alert || CAP_SPMPLAN_ABNORMAL || !CAP_SPM_MANUAL ||
	    sql_id <= 1 || plan_id <= 1 || !optimizer_use_sql_plan_baselines)
	{
		return ret;
	}

	cacheret = GetSPMBindHint(sql_id, &cache_planid, NULL);
	if ((cacheret == SPM_PLAN_CACHE_FIXED && cache_planid == plan_id) || 
		cacheret == SPM_PLAN_CACHE_NO_MATCHED || 
		cacheret == SPM_PLAN_CACHE_NO_VALID)
	{
		return ret;
	}

	memlist = SearchSysCacheList1(SPMPLANSQLID, ObjectIdGetDatum(sql_id));
	for (i = 0; i < memlist->n_members; i++)
	{
		Assert(memlist->n_members == 1);
		oldtup = &memlist->members[i]->tuple;
	}

	if (HeapTupleIsValid(oldtup))
	{
		Relation          rel = heap_open(SPMPlanRelationId, AccessShareLock);
		LocalSPMPlanInfo *deformed_spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));

		InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
		DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
		heap_close(rel, AccessShareLock);
		if (cacheret == SPM_PLAN_CACHE_NO_ENTRY)
		{
			SPMPlanHashTabInsert(deformed_spmplan->sql_id,
							deformed_spmplan->plan_id,
							deformed_spmplan->hint,
							deformed_spmplan->priority,
							deformed_spmplan->enabled,
							deformed_spmplan->executions);
		}

		if (deformed_spmplan->enabled && deformed_spmplan->priority == SPM_PLAN_FIXED &&
		    deformed_spmplan->plan_id != plan_id)
		{
			elog(level,
			     "[SPM] Alert: Details are as follows:\n"
			     "Alert begin "
			     "-------------------------------------------------------------------\n"
			     "Failed to apply SPM plan. Unexpected plan generated. Details are as follows:\n"
			     "sql_id: %ld\n"
			     "sql_normalize: %s\n"
			     "Expect Hint:\n%s\n"
			     "Expect Hint Detail:\n%s\n"
			     "Expect Plan:\n%s\n"
			     "Expect Plan_id: %ld\n"
			     "Actual Hint:\n%s\n"
			     "Actual Hint Detail:\n%s\n"
			     "Actual Plan:\n%s\n"
			     "Actual Plan_id: %ld\n"
			     "Alert end "
			     "----------------------------------------------------------------------\n",
			     sql_id,
			     deformed_spmplan->sql_normalize,
			     deformed_spmplan->hint,
			     deformed_spmplan->hint_detail,
			     deformed_spmplan->plan,
			     deformed_spmplan->plan_id,
			     pre_spmplan->hint,
			     pre_spmplan->hint_detail,
			     pre_spmplan->plan,
			     pre_spmplan->plan_id);

			ret = false;
		}
		DeepFreeSPMPlan(deformed_spmplan, false);
		pfree(deformed_spmplan);
	}

	ReleaseSysCacheList(memlist);
	return ret;
}

bool
CheckNewSPMPlanGenerated(void)
{
	Relation    rel;
	HeapTuple   tup = NULL;
	Oid         relid = SPMPlanHistoryRelationId;
	SysScanDesc scan;
	ScanKeyData entry[1];
	int         plans = 0;
	bool        find = false;
	bool        nulls[Natts_pg_spm_plan];
	Datum       values[Natts_pg_spm_plan];
	bool        check = (!(save_spmplan_state & (SAVE_SPMPLAN_CAPTURE | SAVE_SPMPLAN_APPLY | SAVE_SPMPLAN_APPLYED)));
	SPMPlanCacheRetNo cacheret;

	if (!SPM_ENABLED || CAP_SPM_MANUAL || !enable_spm_alert || CAP_SPMPLAN_ABNORMAL ||
	    !ValidPreSPMPlan(false) || ChooseSPMPlan(pre_spmplan->sql_id, false))
	{
		return false;
	}

	cacheret = SPMHistoryCacheSearchByPlanId(MyDatabaseId,
	                                         pre_spmplan->sql_id,
	                                         pre_spmplan->plan_id,
	                                         NULL,
	                                         NULL,
	                                         pre_spmplan->execute_time);
	if (cacheret == SPM_PLAN_CACHE_MATCHED || cacheret == SPM_PLAN_CACHE_NO_VALID)
	{
		if (cacheret == SPM_PLAN_CACHE_MATCHED)
			return true;
		return false;
	}

	rel = heap_open(relid, AccessShareLock);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            pre_spmplan->sql_id);

	scan = systable_beginscan(rel, SPMPlanHistorySqlidPlanidIndexId, true, NULL, 1, entry);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		heap_deform_tuple(tup, RelationGetDescr(rel), values, nulls);
		if (cacheret == SPM_PLAN_CACHE_NO_ENTRY)
		{
			SPMHistoryHashTabInsert(DatumGetInt64(values[Anum_pg_spm_plan_history_sql_id - 1]), 
									DatumGetInt64(values[Anum_pg_spm_plan_history_plan_id - 1]),
									DatumGetInt64(values[Anum_pg_spm_plan_history_executions - 1]),
									DatumGetInt64(values[Anum_pg_spm_plan_history_elapsed_time - 1]));
		}

		if (DatumGetInt64(values[Anum_pg_spm_plan_history_plan_id - 1]) == pre_spmplan->plan_id)
		{
			find = true;
			if (cacheret == SPM_PLAN_CACHE_NO_MATCHED)
			{
				SPMHistoryHashTabInsert(DatumGetInt64(values[Anum_pg_spm_plan_history_sql_id - 1]), 
										DatumGetInt64(values[Anum_pg_spm_plan_history_plan_id - 1]),
										DatumGetInt64(values[Anum_pg_spm_plan_history_executions - 1]),
										DatumGetInt64(values[Anum_pg_spm_plan_history_elapsed_time - 1]));
			}
			break;
		}
		plans++;
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	if (!find && plans == 0)
	{
		SPMHistoryHashTabInsert(pre_spmplan->sql_id, INVALID_SPM_CACHE_PLAN_ID, 0, 0);
	}

	if (!find && plans > 0 && check)
	{
		int level = LOG;
	
		elog(level,
		     "[SPM] Alert: Details are as follows:\n"
		     "Alert begin "
		     "-------------------------------------------------------------------\n"
		     "A new plan has been generated. Details are as follows:\n"
		     "sql_id: %ld\n"
		     "plan_id: %ld\n"
		     "sql_normalize: %s\n"
		     "Hint:\n%s\n"
		     "Plan:\n%s\n"
		     "Alert end "
		     "----------------------------------------------------------------------\n",
		     pre_spmplan->sql_id,
		     pre_spmplan->plan_id,
		     pre_spmplan->sql_normalize,
		     pre_spmplan->hint,
		     pre_spmplan->plan);
	}

	return false;
}

char *
ApplySPMPlan(char *hint_str, MemoryContext cxt, int64 sql_id)
{
	MemoryContext old;
	char *spm_hint = NULL;

	if (ChooseSPMPlanInternal(sql_id, false, &spm_hint))
	{
		int   spm_hint_len = (spm_hint != NULL) ? strlen(spm_hint) : 0;
		int   cur_hint_len = (hint_str != NULL) ? strlen(hint_str) : 0;

		old = MemoryContextSwitchTo(cxt);

		if (spm_hint_len > 0)
		{
			if (hint_str == NULL)
				hint_str = (char *) palloc0(spm_hint_len + 2);
			else
				hint_str = (char *) repalloc((void *) hint_str, cur_hint_len + spm_hint_len + 2);

			strncpy(hint_str + cur_hint_len, spm_hint, spm_hint_len);
			hint_str[cur_hint_len + spm_hint_len] = ' ';
			hint_str[cur_hint_len + spm_hint_len + 1] = '\0';
			pfree(spm_hint);
			save_spmplan_state |= SAVE_SPMPLAN_APPLYED;
		}

		MemoryContextSwitchTo(old);
	}
	return hint_str;
}

void
DeformSPMPlanTupe(Relation rel, HeapTuple tup, LocalSPMPlanInfo *spmplan)
{
	bool          nulls[Natts_pg_spm_plan];
	Datum         values[Natts_pg_spm_plan];
	TupleDesc     desc = RelationGetDescr(rel);
	MemoryContext old;
	char		 *sql = NULL;
	char		 *sql_normalize = NULL;
	char		 *hint = NULL;
	char		 *hint_detail = NULL;
	char		 *plan = NULL;
	char		 *guc_list = NULL;
	char		 *param_list = NULL;
	char		 *description = NULL;

	heap_deform_tuple(tup, desc, values, nulls);
	old = MemoryContextSwitchTo(spm_context);

	strcpy(NameStr(spmplan->spmname), (*(DatumGetName(values[Anum_pg_spm_plan_spmname - 1]))).data);
	spmplan->spmnsp = DatumGetObjectId(values[Anum_pg_spm_plan_spmnsp - 1]);
	spmplan->ownerid = DatumGetObjectId(values[Anum_pg_spm_plan_ownerid - 1]);
	spmplan->created = DatumGetTimestampTz(values[Anum_pg_spm_plan_created - 1]);
	spmplan->last_modified = DatumGetTimestampTz(values[Anum_pg_spm_plan_last_modified - 1]);
	spmplan->last_verified = DatumGetTimestampTz(values[Anum_pg_spm_plan_last_verified - 1]);
	spmplan->version = DatumGetInt64(values[Anum_pg_spm_plan_version - 1]);
	spmplan->elapsed_time = DatumGetInt64(values[Anum_pg_spm_plan_elapsed_time - 1]);
	spmplan->executions = DatumGetInt64(values[Anum_pg_spm_plan_executions - 1]);
	spmplan->task_id = DatumGetInt64(values[Anum_pg_spm_plan_task_id - 1]);
	spmplan->sql_normalize_rule = DatumGetInt32(values[Anum_pg_spm_plan_sql_normalize_rule - 1]);
	spmplan->priority = DatumGetInt32(values[Anum_pg_spm_plan_priority - 1]);
	spmplan->cost = DatumGetFloat4(values[Anum_pg_spm_plan_cost - 1]);
	spmplan->enabled = DatumGetBool(values[Anum_pg_spm_plan_enabled - 1]);
	spmplan->autopurge = DatumGetBool(values[Anum_pg_spm_plan_autopurge - 1]);

	if (!nulls[Anum_pg_spm_plan_reloids - 1])
		spmplan->reloids = (oidvector *) (values[Anum_pg_spm_plan_reloids - 1]);
	else
		spmplan->reloids = NULL;

	if (!nulls[Anum_pg_spm_plan_indexoids - 1])
		spmplan->indexoids = (oidvector *) (values[Anum_pg_spm_plan_indexoids - 1]);
	else
		spmplan->indexoids = NULL;

	spmplan->sql_id = DatumGetInt64(values[Anum_pg_spm_plan_sql_id - 1]);
	spmplan->plan_id = DatumGetInt64(values[Anum_pg_spm_plan_plan_id - 1]);

	sql = TextDatumGetCString(values[Anum_pg_spm_plan_sql - 1]);
	EnlargeSPMPlanInfoMember(&spmplan->sql, &spmplan->sql_len, strlen(sql));
	strcpy(spmplan->sql, sql);

	sql_normalize = TextDatumGetCString(values[Anum_pg_spm_plan_sql_normalize - 1]);
	EnlargeSPMPlanInfoMember(&spmplan->sql_normalize, &spmplan->sql_normalize_len, strlen(sql_normalize));
	strcpy(spmplan->sql_normalize, sql_normalize);

	hint = TextDatumGetCString(values[Anum_pg_spm_plan_hint - 1]);
	EnlargeSPMPlanInfoMember(&spmplan->hint, &spmplan->hint_len, strlen(hint));
	strcpy(spmplan->hint, hint);

	hint_detail = TextDatumGetCString(values[Anum_pg_spm_plan_hint_detail - 1]);
	EnlargeSPMPlanInfoMember(&spmplan->hint_detail, &spmplan->hint_detail_len, strlen(hint_detail));
	strcpy(spmplan->hint_detail, hint_detail);

	plan = TextDatumGetCString(values[Anum_pg_spm_plan_plan - 1]);
	EnlargeSPMPlanInfoMember(&spmplan->plan, &spmplan->plan_len, strlen(plan));
	strcpy(spmplan->plan, plan);

	guc_list = TextDatumGetCString(values[Anum_pg_spm_plan_guc_list - 1]);
	EnlargeSPMPlanInfoMember(&spmplan->guc_list, &spmplan->guc_list_len, strlen(guc_list));
	strcpy(spmplan->guc_list, guc_list);

	if (!nulls[Anum_pg_spm_plan_param_list - 1])
	{
		param_list = TextDatumGetCString(values[Anum_pg_spm_plan_param_list - 1]);
		EnlargeSPMPlanInfoMember(&spmplan->param_list, &spmplan->param_list_len, strlen(param_list));
		strcpy(spmplan->param_list, param_list);
	}

	if (!nulls[Anum_pg_spm_plan_relnames - 1])
		spmplan->relnames = DatumGetArrayTypeP(values[Anum_pg_spm_plan_relnames - 1]);
	else
		spmplan->relnames = NULL;

	if (!nulls[Anum_pg_spm_plan_indexnames - 1])
		spmplan->indexnames = DatumGetArrayTypeP(values[Anum_pg_spm_plan_indexnames - 1]);
	else
		spmplan->indexnames = NULL;

	if (!nulls[Anum_pg_spm_plan_description - 1])
	{
		description = TextDatumGetCString(values[Anum_pg_spm_plan_description - 1]);
		EnlargeSPMPlanInfoMember(&spmplan->description, &spmplan->description_len, strlen(description));
		strcpy(spmplan->description, description);
	}

	strcpy(spmplan->origin.data, TextDatumGetCString(values[Anum_pg_spm_plan_origin - 1]));

	MemoryContextSwitchTo(old);
}
