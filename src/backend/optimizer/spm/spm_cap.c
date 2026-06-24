/*------------------------------------------------------------------------
 *
 * spm_cap.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/optimizer/spm/spm_cap.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <float.h>
#include <limits.h>
#include <math.h>
#include "optimizer/spm.h"
#include "optimizer/spm_cache.h"
#include "storage/bufmgr.h"
#include "commands/tablecmds.h"
#include "commands/prepare.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "access/xact.h"
#include "utils/plancache.h"
#include "executor/nodeModifyTable.h"
#include "access/hash.h"

typedef enum
{
	PARAM_ENABLED,
	PARAM_FIXED,
	PARAM_AUTOPURGE,
	PARAM_PLAN_NAME,
	PARAM_DESCRIPTION
} AlterSPMPlanParamType;

extern List        *session_param_list;
int                 optimizer_capture_sql_plan_baselines = SPM_CAPTURE_OFF;
char               *optimizer_capture_sql_plan_baselines_rules = NULL;
int                 space_budget_limit = 0;
int                 space_budget_percent = 0;

SPMPlanCaptureRules spm_capture_rules = 
#ifdef _PG_REGRESS_
	{-1, -1};
#else
	{10 * USECS_PER_SEC, -1};
#endif

extern void start_xact_command(void);
extern void finish_xact_command(void);
static Oid AddSPMPlanInternal(bool isreplace, bool apply, LocalSPMPlanInfo *spmplan);
static Oid AddSPMPlanHistoryInternal(bool isreplace, LocalSPMPlanInfo *spmplan);
static bool DuringExplainExecute(void);

static void
ResetSPMPlanState(bool force)
{
	if (force)
	{
		save_spmplan_state = 0;
		return;
	}

	save_spmplan_state =
		save_spmplan_state & (SAVE_SPMPLAN_APPLY | SAVE_SPMPLAN_CAPTURE | SAVE_SPMPLAN_EXPLAIN |
	                          SAVE_SPMPLAN_FILL_PARAMLIST_DONE);
}

void
ResetPreSPMPlan(bool force)
{
	if (!SPM_ENABLED)
		return;

	ShrinkSPMPlanInfo(pre_spmplan);
	ResetSPMPlan(pre_spmplan);

	ResetSPMPlanState(force && (!DuringExplainExecute()));
}

void
ResetPreSPMPlanExtended(bool force)
{
	if (!SPM_ENABLED)
		return;

	ResetSPMPlanState(force);
}

/*
 * Marked as not supporting SPM plan capture.
 */
void
SetSPMUnsupported(void)
{
	/* Only keep the ABNORMAL flag for unsupported scenarios. */
	save_spmplan_state = SAVE_SPMPLAN_ABNORMAL;
}

/*
 * Extend the protocol scenario to skip further processing in SPM if gplan is used.
 */
void
SetSkipSPMProcess(void)
{
	save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
}

void
SetEnterExplainExecute(void)
{
	if (!SPM_ENABLED)
		return;

	save_spmplan_state |= SAVE_SPMPLAN_EXPLAN_EXECUTE;
}

void
SetExitExplainExecute(void)
{
	if (!SPM_ENABLED)
		return;

	save_spmplan_state &= ~SAVE_SPMPLAN_EXPLAN_EXECUTE;
}

static bool
DuringExplainExecute()
{
	return (save_spmplan_state & SAVE_SPMPLAN_EXPLAN_EXECUTE) > 0;
}


#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

/**
 * Parse the SPM plan capture rules from a string.
 * The rules string should be in the format: "frequency: 5, time: 5s"
 * The parsed values are filled into the SPMPlanCaptureRules structure.
 *
 * @param rules The rules string to parse
 * @param capture The SPMPlanCaptureRules structure to fill
 * @return true if the parsing is successful, false otherwise
 */
bool
ParseSPMPlanCaptureRules(const char *rules)
{
	/* Parse the rules string */
	const char          *token = NULL;
	char                *rules_copy = pstrdup(rules);
	char                *saveptr;
	char                *saveptr2 = NULL;
	SPMPlanCaptureRules *capture = &spm_capture_rules;
	SPMPlanCaptureRules  spm_capture_rules_old = spm_capture_rules;
	char                *key = NULL;
	char                *value;
	char                *trimmed_key;
	char                *trimmed_value;
	char                *trimmed_key_cur;
	char                *trimmed_value_cur;
	char                *end;
	bool                 null_runtime = true;
	bool                 ret = true;

	token = strtok_r(rules_copy, ",", &saveptr);
	while (token != NULL)
	{
		char *delimiter = strpbrk(token, "=:");
		/* Format and replace the delimiter with '='." */
		if (delimiter == NULL)
		{
			ret = false;
			break;
		}
		else
			*delimiter = '=';

		/* Parse key-value pairs from the rules */
		key = strtok_r((char *) token, "=", &saveptr2);
		if (key == NULL)
		{
			ret = false;
			break;
		}

		value = strtok_r(NULL, "=", &saveptr2);
		if (value == NULL)
		{
			ret = false;
			break;
		}

		/* Trim leading and trailing spaces from the key and value */
		trimmed_key = pstrdup(key);
		trimmed_value = pstrdup(value);
		trimmed_key_cur = trimmed_key;
		trimmed_value_cur = trimmed_value;

		/* Remove leading spaces from the key and value */
		while (*trimmed_key_cur == ' ' || *trimmed_key_cur == '"')
		{
			trimmed_key_cur++;
		}
		while (*trimmed_value_cur == ' ' || *trimmed_key_cur == '"')
		{
			trimmed_value_cur++;
		}

		/* Set the corresponding variable based on the key name */
		if (strncmp(trimmed_key_cur, "time", strlen("time")) == 0)
		{
			long time_value = strtol(trimmed_value_cur, &end, 10);

			if (end == trimmed_value_cur)
				elog(ERROR,
				     "Incorrect input format, a correct example is 'set spm_plan_capture_rules = "
				     "'time: 10s''. The unit 'time' can be configured in milliseconds (ms), "
				     "seconds (s), minutes (min), or hours (h). If no unit is specified, it "
				     "defaults to seconds (s).");

			if (time_value < 0)
				elog(ERROR,
				     "Invalid input for time value. The valid range is [0, %lu]",
				     INT64_MAX / USECS_PER_MSEC);

			/* Skip whitespace */
			while (*end != '\0' && *end == ' ')
			{
				end++;
			}

			trimmed_value_cur = end;

			/* Parse time unit, default unit is 's' */
			if (*trimmed_value_cur == '\0' || *trimmed_value_cur == '"' ||
			    strncmp(trimmed_value_cur, "s", 1) == 0)
			{
				if (time_value <= INT64_MAX / USECS_PER_SEC)
					capture->execute_time = time_value * USECS_PER_SEC;
				else
				{
					elog(ERROR,
					     "current time values exceed the allowed range, When the unit is in "
					     "seconds, the allowed time range is [0, %lu]",
					     INT64_MAX / USECS_PER_SEC);
					break;
				}

				if (strncmp(trimmed_value_cur, "s", 1) == 0)
					trimmed_value_cur += 1;
			}
			else if (strncmp(trimmed_value_cur, "ms", strlen("ms")) == 0)
			{
				if (time_value <= INT64_MAX / USECS_PER_MSEC)
					capture->execute_time = time_value * USECS_PER_MSEC;
				else
				{
					elog(ERROR,
					     "current time values exceed the allowed range, When the unit is in "
					     "milliseconds, the allowed time range is [0, %lu]",
					     INT64_MAX / USECS_PER_MSEC);
					break;
				}
				trimmed_value_cur += 2;
			}
			else if (strncmp(trimmed_value_cur, "min", 3) == 0)
			{
				if (time_value <= INT64_MAX / USECS_PER_MINUTE)
					capture->execute_time = time_value * USECS_PER_MINUTE;
				else
				{
					elog(ERROR,
					     "current time values exceed the allowed range, When the unit is in "
					     "minutes, the allowed time range is [0, %lu]",
					     INT64_MAX / USECS_PER_MINUTE);
					break;
				}
				trimmed_value_cur += 3;
			}
			else if (strncmp(trimmed_value_cur, "h", 1) == 0)
			{
				if (time_value <= INT64_MAX / USECS_PER_HOUR)
					capture->execute_time = time_value * USECS_PER_HOUR;
				else
				{
					elog(ERROR,
					     "current time values exceed the allowed range, When the unit is in hours, "
					     "the allowed time range is [0, %lu]",
					     INT64_MAX / USECS_PER_HOUR);
					break;
				}
				trimmed_value_cur += 1;
			}

			/* Skip whitespace */
			while (*trimmed_value_cur != '\0' && *trimmed_value_cur == ' ')
			{
				trimmed_value_cur++;
			}

			/* Check if the end has been reached, and throw an error if there are any remaining
			 * characters */
			if (*trimmed_value_cur != '\0' && *trimmed_value_cur != ',' &&
			    *trimmed_value_cur != '"')
			{
				elog(ERROR,
				     "Incorrect input format, a correct example is 'set spm_plan_capture_rules = "
				     "'time: 10s''. The unit 'time' can be configured in milliseconds (ms), "
				     "seconds (s), minutes (min), or hours (h). If no unit is specified, it "
				     "defaults to seconds (s).");
			}

			null_runtime = false;
		}
		else
		{
			/* Error handling, unable to parse as integer */
			pfree(trimmed_key);
			pfree(trimmed_value);
			ret = false;
			break;
		}

		pfree(trimmed_key);
		pfree(trimmed_value);

		token = strtok_r(NULL, ",", &saveptr);
	}

	if (ret)
	{
		if (null_runtime)
			capture->execute_time = -1;
	}
	else
		spm_capture_rules = spm_capture_rules_old;

	pfree(rules_copy);
	return ret;
}
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

static bool
ContainsMultipleSQL(const char *query_string)
{
	int   count = 0;
	char *copy = NULL;
	char *token = NULL;

	if (query_string == NULL || query_string[0] == '\0')
	{
		return false;
	}

	copy = pstrdup(query_string);
	token = strtok(copy, ";");

	while (token != NULL && (count < 2))
	{
		count++;
		token = strtok(NULL, ";");
	}

	pfree(copy);
	return count > 1;
}

#define NSP_NAME(oid) isTempNamespace(oid) ? "pg_temp" : get_namespace_name(oid)

static int
param_compare(const void *v1,const void *v2)
{
	Param	*p1 = (Param *) lfirst(*(ListCell **) v1);
	Param   *p2 = (Param *) lfirst(*(ListCell **) v2);

	if(p1->paramid > p2->paramid)
		return 1;
	else if (p1->paramid == p2->paramid)
		return 0;
	return -1;
}

void
SPMFillParamList(JumbleState *jstate)
{
	ListCell  *lc;
	StringInfo pstr;
	int        len = 0;

	if (!SPM_ENABLED || jstate->param_list == NULL)
	{
		save_spmplan_state |= SAVE_SPMPLAN_FILL_PARAMLIST_DONE;
		return;
	}

	pstr = makeStringInfo();
	jstate->param_list = list_qsort(jstate->param_list, param_compare);
	foreach (lc, jstate->param_list)
	{
		Param *p = (Param *) lfirst(lc);
		appendStringInfo(pstr, "%d:", p->paramid);
		appendStringInfo(pstr,
		                 "%s.%s",
		                 NSP_NAME(get_typ_namespace(p->paramtype)),
		                 get_typ_name(p->paramtype));
		if (lc->next)
			appendStringInfo(pstr, ",");
	}

	len = strlen(pstr->data);
	EnlargeSPMPlanInfoMember(&pre_spmplan->param_list, &pre_spmplan->param_list_len, len);

	strncpy(pre_spmplan->param_list, pstr->data, len);
	pre_spmplan->param_list[len] = '\0';

	save_spmplan_state |= SAVE_SPMPLAN_FILL_PARAMLIST_DONE;
}

void
SPMFillStatement(Query *query, JumbleState *jstate, const char *query_string)
{
	int          normalized_query_len;
	char        *normalized_query = NULL;
	JumbleState *new_jstate = NULL;
	int          level = (debug_print_spm) ? LOG: DEBUG5;
	Query       *new_jump_query = query;
	const char  *jump_query_str = query_string;
	const int    multy_sql_minlen_check = 2048;

	if (!SPM_ENABLED)
		return;

	if (!IS_ACCESS_NODE || creating_extension)
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		return;
	}

	/* reset pre_spmplan */
	ResetPreSPMPlan(true);

	/* strip out the top-level query for further processing */
	if (new_jump_query->commandType == CMD_UTILITY && new_jump_query->utilityStmt)
	{
		new_jump_query = (Query *) new_jump_query->utilityStmt;
		if (IsA(new_jump_query, ExplainStmt))
		{
			ExplainStmt *stmt = (ExplainStmt *) new_jump_query;

			Assert(IsA(stmt->query, Query));
			new_jump_query = (Query *) stmt->query;

			/* strip out the top-level query for further processing */
			if (new_jump_query->commandType == CMD_UTILITY && new_jump_query->utilityStmt != NULL)
				new_jump_query = (Query *) new_jump_query->utilityStmt;
		}

		if (IsA(new_jump_query, CreateTableAsStmt))
		{
			CreateTableAsStmt *stmt = (CreateTableAsStmt *) new_jump_query;

			Assert(IsA(stmt->query, Query));
			new_jump_query = (Query *) stmt->query;

			/* strip out the top-level query for further processing */
			if (new_jump_query->commandType == CMD_UTILITY && new_jump_query->utilityStmt != NULL)
				new_jump_query = (Query *) new_jump_query->utilityStmt;
		}

		if (IsA(new_jump_query, ExecuteStmt))
		{
			/*
			 * Use the prepared query for EXECUTE. The Query for jumble
			 * also replaced with the corresponding one.
			 */
			ExecuteStmt       *stmt = (ExecuteStmt *) new_jump_query;
			PreparedStatement *entry;

			/*
			 * Silently ignore nonexistent prepared statements.  This may
			 * happen for EXECUTE within a function definition.  Otherwise the
			 * execution will fail anyway.
			 */
			entry = FetchPreparedStatement(stmt->name, false);

			if (entry && entry->plansource->is_valid)
			{
				jump_query_str = (char *) entry->plansource->query_string;
				new_jump_query = (Query *) linitial(entry->plansource->query_list);
			}
			else
			{
				new_jump_query = NULL;
			}
		}

		/* JumbleQuery accespts only a non-utility Query */
		if (new_jump_query && (!IsA(new_jump_query, Query) || new_jump_query->utilityStmt != NULL))
		{
			new_jump_query = NULL;
		}
	}

	if (new_jump_query != NULL && new_jump_query != query)
	{
		new_jstate = JumbleQuery(new_jump_query, jump_query_str);
		jstate = new_jstate;
	}

	if (jstate == NULL || (query->utilityStmt && (!(IsA(query->utilityStmt, ExplainStmt)) &&
	                                              !(IsA(query->utilityStmt, ExecuteStmt)))))
	{
		pre_spmplan->sql_id = (int64) -1;
		save_spmplan_state |= SAVE_SPMPLAN_INVALID_SQLID;
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		return;
	}

	pre_spmplan->sql_id = (int64) new_jump_query->queryId;

	normalized_query_len = strlen(query_string) + 1;

	/* When there are multiple SQL statements, we do not keep records. */
	if (normalized_query_len > multy_sql_minlen_check && ContainsMultipleSQL(query_string))
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		return;
	}

	normalized_query = generate_normalized_query(jstate, query_string, 0, &normalized_query_len, false);

	SPMFillParamList(jstate);

	if (normalized_query == NULL)
		elog(level, "[SPM] SPMFillStatement terminated, get no generate_normalized_query ");

	EnlargeSPMPlanInfoMember(&pre_spmplan->sql, &pre_spmplan->sql_len, strlen(query_string));
	EnlargeSPMPlanInfoMember(&pre_spmplan->sql_normalize, &pre_spmplan->sql_normalize_len, normalized_query_len);

	if (*query_string != '\0')
		strcpy(pre_spmplan->sql, query_string);

	if (normalized_query && *normalized_query != '\0')
	{
		strcpy(pre_spmplan->sql_normalize, normalized_query);
		pfree(normalized_query);
	}

	save_spmplan_state |= SAVE_SPMPLAN_FILL_SQL_DONE;

	if (debug_print_spm)
		(void) ChooseSPMPlan(pre_spmplan->sql_id, true);
}

void
SPMFillStatementExtended(const char *query_string, const char *normalized_query, int64 sql_id)
{
	size_t query_len = 0;
	size_t normalized_query_len = 0;

	if (!IS_ACCESS_NODE)
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		return;
	}

	if (!SPM_ENABLED || query_string == NULL || normalized_query == NULL || sql_id <= 1)
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		save_spmplan_state |= SAVE_SPMPLAN_FILL_SQL_DONE;
		return;
	}

	query_len = strlen(query_string) + 1;
	normalized_query_len = strlen(normalized_query) + 1;

	EnlargeSPMPlanInfoMember(&pre_spmplan->sql, &pre_spmplan->sql_len, query_len);
	EnlargeSPMPlanInfoMember(&pre_spmplan->sql_normalize, &pre_spmplan->sql_normalize_len, normalized_query_len);

	pre_spmplan->sql_id = sql_id;

	if (query_string && *query_string != '\0')
	{
		strcpy(pre_spmplan->sql, query_string);
	}

	if (normalized_query && *normalized_query != '\0')
	{
		strcpy(pre_spmplan->sql_normalize, normalized_query);
	}

	save_spmplan_state |= SAVE_SPMPLAN_FILL_SQL_DONE;

	if (debug_print_spm)
		(void) ChooseSPMPlan(pre_spmplan->sql_id, true);
}

/*
 * Modify the 'explain execute' to print the binding information of the spmplan.
 * Here, record the current plan information being used.
 */
void
SPMFillApplyedInfoExtended(int64 sql_id, int64 plan_id, bool spm_applyed)
{
	if (!SPM_ENABLED || !IS_ACCESS_NODE || !spm_applyed)
		return;

	pre_spmplan->sql_id = sql_id;
	pre_spmplan->plan_id = plan_id;
	save_spmplan_state |= SAVE_SPMPLAN_APPLYED;
}

void
SPMFillGUC(void)
{
	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL)
		return;

	SPMSetParamStr(pre_spmplan->guc_list, sizeof(pre_spmplan->guc_list));
	save_spmplan_state |= SAVE_SPMPLAN_FILL_GUC_DONE;
}

static char *
FmtName(const char *name, bool need_quotes)
{
	StringInfoData buf;
	const char    *cp;

	initStringInfo(&buf);

	if (!need_quotes)
	{
		/* no quoting needed */
		appendStringInfo(&buf, "%s", name);
	}
	else
	{
		appendStringInfo(&buf, "%c", '"');
		for (cp = name; *cp; cp++)
		{
			/*
			 * Did we find a double-quote in the string? Then make this a
			 * double double-quote per SQL99. Before, we put in a
			 * backslash/double-quote pair. - thomas 2000-08-05
			 */
			if (*cp == '"')
				appendStringInfo(&buf, "%c", '"');
			appendStringInfo(&buf, "%c", *cp);
		}
		appendStringInfo(&buf, "%c", '"');
	}

	return buf.data;
}

Datum
TextListToArray(List *relnms)
{
	ArrayBuildState *astate = NULL;
	ListCell        *cell;

	foreach (cell, relnms)
	{
		text *value;
		Size  len;
		char *str;
		text *t;

		value = lfirst(cell);
		str = text_to_cstring(value);
		len = VARHDRSZ + strlen(str);

		t = (text *) palloc0(len);
		SET_VARSIZE(t, len);
		memcpy(VARDATA(t), str, strlen(str));

		astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
		pfree(str);
	}

	if (astate)
		return makeArrayResult(astate, CurrentMemoryContext);

	return PointerGetDatum(NULL);
}

char **
ArrayToList(ArrayType *array, int *nvalues, int *totallen, bool needres)
{
	char		**result = NULL;
	int 		i;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *values;
	char		*elem;

	*nvalues = 0;
	*totallen = 0;

	/* We could cache this data, but not clear it's worth it */
	get_typlenbyvalalign(ARR_ELEMTYPE(array),
							&elmlen, &elmbyval, &elmalign);
	/* Deconstruct array into Datum elements; NULLs not expected */
	deconstruct_array(array,
						ARR_ELEMTYPE(array),
						elmlen, elmbyval, elmalign,
						&values, NULL, nvalues);
	
	if (needres)
		result = palloc0(sizeof(char *) * (*nvalues));
	for (i = 0; i < *nvalues; i++)
	{
		text *value = DatumGetTextP(values[i]);
		elem = text_to_cstring(value);
		*totallen += strlen(elem) + 1;
		if (needres)
			result[i] = elem;
		else
			pfree(elem);
	}
	return result;
}

static bool
SPMFillRelsPreCheck(PlannedStmt *plan)
{
#define MIN_SUPPORTED_RELS 1
	int       valid_rel_cnt = 0;
	int       rt_index;
	ListCell *lc;

	if (plan->commandType != CMD_INSERT)
		return true;

	rt_index = 0;
	foreach (lc, plan->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		rt_index++;
		if (rte->rtekind != RTE_RELATION)
			continue;

		/*
		 * Skip resultRelations if there are still valid relations present, considering the current
		 * INSERT allows recording SPM plans.
		 */
		if (!list_member_int(plan->resultRelations, rt_index))
			valid_rel_cnt++;
	}

	return valid_rel_cnt >= MIN_SUPPORTED_RELS;
}

static bool
IsExtensionSchema(char *schema)
{
#define EXTENSION_SCHEMA_NUMS 10
	int i;
	char lower_schema[64] = {0};
	const char *extensionSchemas[] = {
		"pg_toast",
		"pg_slice",
		"pg_dbms_job",
		"pg_catalog",
		"information_schema",
		"hint_plan",
		"plan_repo",
		"squeeze",
		"dbms_spm"};

	for (i = 0; schema[i] != '\0'; i++)
	{
		lower_schema[i] = tolower(schema[i]);
	}

	for (i = 0; i < EXTENSION_SCHEMA_NUMS; i++)
	{
		if (strcmp(lower_schema, extensionSchemas[i]) == 0)
		{
			return true;
		}
	}

	return false;
}

static void
SPMFillRels(PlannedStmt *plan)
{
#define MIN_SUPPORTED_RELS 1
#define LOCAL_BUFSIZE (NAMEDATALEN * 2 + 5)
	List          *relids = NIL;
	List          *rtable = plan->rtable;
	List          *relnms = NIL;
	ListCell      *lc = NULL;
	RangeTblEntry *rte = NULL;
	Oid            rid;
	int            i = 0;
	char           buf[LOCAL_BUFSIZE] = {0};
	int            valid_rel_cnt = 0;
	int            tmp_rel_cnt = 0;
	char          *relschema;
	int            oidsvec_len = 0;
	int            level = (debug_print_spm) ? LOG : DEBUG5;

	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL ||
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_RELS_DONE))
		return;

	if (!SPMFillRelsPreCheck(plan))
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		save_spmplan_state |= SAVE_SPMPLAN_FILL_RELS_DONE;
		return;
	}

	foreach (lc, rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);
		rid = rte->relid;

		/*
		 * Skipping invalid rels
		 * skipping system tables as they do not change. Skipping duplicate relations.
		 */
		if (rte->rtekind != RTE_RELATION || OidIsValid(rid) == false || rid < FirstNormalObjectId || list_member_oid(relids, rid))
			continue;

		if (IsTempTable(rid) || IsGlobalTempRelId(rid))
		{
			if (enable_experiment_spm)
			{
				/* No need to add 'depend' dependency for temporary tables. */
				tmp_rel_cnt++;
				continue;
			}
			else
			{
				save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
				if (relids != NIL)
				{
					list_free(relids);
					relids = NIL;
				}

				tmp_rel_cnt = 0;
				valid_rel_cnt = 0;

				elog(level,
				     "[SPM] SPMFillRels terminated: due to contain temp rels, sql_id: %ld",
				     pre_spmplan->sql_id);
				break;
			}

			save_spmplan_state |= SAVE_SPMPLAN_TEMP_TABLE;
		}

		relschema = get_namespace_name(get_rel_namespace(rid));
		if (IsExtensionSchema(relschema))
			continue;

		if (rte->relkind == RELKIND_RELATION || rte->relkind == RELKIND_FOREIGN_TABLE ||
		    rte->relkind == RELKIND_PARTITIONED_TABLE)
			valid_rel_cnt++;

		memset(buf, 0, LOCAL_BUFSIZE);
		sprintf(buf, "%s.%s ", FmtName(relschema, true), FmtName(get_rel_name(rid), true));

		relids = lcons_oid(rid, relids);
		relnms = lcons((void *) cstring_to_text(buf), relnms);
	}

	if (relids != NIL && valid_rel_cnt >= MIN_SUPPORTED_RELS)
	{
		MemoryContext old;
		Oid          *o_relids = palloc0(sizeof(Oid) * list_length(relids));

		foreach (lc, relids)
		{
			o_relids[i++] = (Oid) lfirst_oid(lc);
		}

		old = MemoryContextSwitchTo(spm_context);
		pre_spmplan->reloids = buildoidvector(o_relids, list_length(relids));
		pre_spmplan->relnames = DatumGetArrayTypeP(TextListToArray(relnms));
		(void) MemoryContextSwitchTo(old);
		Assert(i == list_length(relids));
	}
	else if (!tmp_rel_cnt)
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		save_spmplan_state |= SAVE_SPMPLAN_INVALID_SQLID;
	}

	if (tmp_rel_cnt > 0)
	{
		elog(level,
		     "[SPM] SPMFillRels: contain %d temp rels, sql_id: %ld",
		     tmp_rel_cnt,
		     pre_spmplan->sql_id);
	}

	
	if (pre_spmplan->indexoids != NULL)
		oidsvec_len = att_addlength_pointer(oidsvec_len, -1, pre_spmplan->indexoids);

	if (pre_spmplan->reloids != NULL)
		oidsvec_len = att_addlength_pointer(oidsvec_len, -1, pre_spmplan->reloids);

	if (oidsvec_len < SPM_PLAN_MAX_OIDSVLENGTH)
		save_spmplan_state |= SAVE_SPMPLAN_FILL_RELS_DONE;
	else
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		elog(level,
		     "[SPM] SPMFillIndex: reloids and indexoids for spm tuple are too long [%d] to "
		     "store, "
		     "sql_id: %ld",
		     oidsvec_len, pre_spmplan->sql_id);
	}
}

void
SPMFillIndex(List *itable)
{
#define LOCAL_BUFSIZE (NAMEDATALEN * 2 + 5)
	List          *relids = NIL;
	Oid            iid = -1;
	ListCell      *lc = NULL;
	List          *relnms = NIL;
	char           buf[LOCAL_BUFSIZE] = {0};
	char          *idxschema;
	int            oidsvec_len = 0;
	int            level = (debug_print_spm) ? LOG : DEBUG5;

	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL || itable == NULL || pre_spmplan->indexoids != NULL)
	{
		save_spmplan_state |= SAVE_SPMPLAN_FILL_INDEX_DONE;
		return;
	}

	foreach (lc, itable)
	{
		iid = (Oid) lfirst_oid(lc);
		idxschema = get_namespace_name(get_rel_namespace(iid));
		if (IsExtensionSchema(idxschema))
			continue;

		if (IsIndexUsingTempTable(iid) || IsIndexUsingGlobalTempTable(iid))
		{
			save_spmplan_state |= SAVE_SPMPLAN_TEMP_TABLE;

			/* No need to add 'depend' dependency for temporary tables. */
			continue;
		}

		memset(buf, 0, LOCAL_BUFSIZE);
		sprintf(buf, "%s.%s ", FmtName(idxschema, true), FmtName(get_rel_name(iid), true));

		relids = lcons_oid((Oid) iid, relids);
		relnms = lcons((void *) cstring_to_text(buf), relnms);
	}

	if (relids != NIL)
	{
		MemoryContext old;
		Oid          *o_relids = palloc0(sizeof(Oid) * list_length(relids));
		int           i = 0;

		foreach (lc, relids)
		{
			o_relids[i++] = (Oid) lfirst_oid(lc);
		}

		old = MemoryContextSwitchTo(spm_context);
		pre_spmplan->indexoids = buildoidvector(o_relids, list_length(relids));
		pre_spmplan->indexnames = DatumGetArrayTypeP(TextListToArray(relnms));
		(void) MemoryContextSwitchTo(old);
		Assert(i == list_length(relids));
	}

	if (pre_spmplan->reloids != NULL)
		oidsvec_len = att_addlength_pointer(oidsvec_len, -1, pre_spmplan->reloids);

	if (pre_spmplan->indexoids != NULL)
		oidsvec_len = att_addlength_pointer(oidsvec_len, -1, pre_spmplan->indexoids);

	if (oidsvec_len < SPM_PLAN_MAX_OIDSVLENGTH)
		save_spmplan_state |= SAVE_SPMPLAN_FILL_INDEX_DONE;
	else
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		elog(level,
		     "[SPM] SPMFillIndex: reloids and indexoids for spm tuple are too long [%d] to "
		     "store, sql_id: %ld",
		     oidsvec_len, pre_spmplan->sql_id);
	}
}

void
SPMFillHint(char *hint, int hint_len, char *detail, int detail_len)
{
	int level = (debug_print_spm) ? LOG: DEBUG5;

	if (!IS_ACCESS_NODE)
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		return;
	}

	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL)
		return;

	if (hint_len + 1 < SPM_PLAN_HINT_MAX_LEN)
	{
		EnlargeSPMPlanInfoMember(&pre_spmplan->hint, &pre_spmplan->hint_len, hint_len);
		strncpy(pre_spmplan->hint, hint, hint_len);
		pre_spmplan->hint[hint_len] = '\0';
		pre_spmplan->hint_id = (int64) hash_any((const unsigned char *) hint, hint_len);
	}
	else
	{
		elog(level,
		     "[SPM] SPMFillHint terminated: hintlen = %d, buffer_size = %d",
		     hint_len + 1,
		     SPM_PLAN_HINT_MAX_LEN);
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
	}

	EnlargeSPMPlanInfoMember(&pre_spmplan->hint_detail, &pre_spmplan->hint_detail_len, detail_len);
	strncpy(pre_spmplan->hint_detail, detail, detail_len);
	pre_spmplan->hint_detail[detail_len] = '\0';

	save_spmplan_state |= SAVE_SPMPLAN_FILL_HINT_DONE;
}

void
SPMGenHintPlan(QueryDesc *query_desc)
{
	ExplainState *es;
	Snapshot		snapshot = ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot();

	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL)
		return;

	if (enable_gts_optimize && snapshot != NULL && CSN_IS_USE_LATEST(snapshot->start_ts))
	{
		save_spmplan_state |= SAVE_SPMPLAN_ABNORMAL;
		return;
	}

	/* Create a QueryDesc for the query */
	es = NewExplainState();

	ExplainSpmHints(es, query_desc);

	SPMFillIndex(query_desc->estate->es_itable);
}

void
SPMFillPlan(char *s, int len)
{
	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL)
		return;

	EnlargeSPMPlanInfoMember(&pre_spmplan->plan, &pre_spmplan->plan_len, len);
	strncpy(pre_spmplan->plan, s, len);
	pre_spmplan->plan[len] = '\0';

	save_spmplan_state |= SAVE_SPMPLAN_FILL_PLAN_DONE;
}

void
SPMFillExtraData(PlannedStmt *plan)
{
	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL)
		return;

	sprintf(NameStr(pre_spmplan->spmname), "%ld", pre_spmplan->sql_id);
	if (CAP_SPM_MANUAL)
		sprintf(NameStr(pre_spmplan->origin), "%s", "manual capture");
	else
		sprintf(NameStr(pre_spmplan->origin), "%s", "auto capture");
	pre_spmplan->created = GetCurrentStatementStartTimestamp();
	pre_spmplan->last_modified = GetCurrentStatementStartTimestamp();
	pre_spmplan->last_verified = GetCurrentStatementStartTimestamp();
	pre_spmplan->version = -1;
	pre_spmplan->elapsed_time = 0;
	pre_spmplan->executions = 1;
	pre_spmplan->task_id = -1;
	pre_spmplan->sql_normalize_rule = -1;
	pre_spmplan->priority =
		(save_spmplan_state & SAVE_SPMPLAN_APPLY) ? SPM_PLAN_FIXED : SPM_APPLY_ACCEPT;
	pre_spmplan->cost = -1;
	pre_spmplan->enabled = true;
	pre_spmplan->autopurge = false;
	pre_spmplan->ownerid = GetUserId();
	pre_spmplan->spmnsp = PG_PUBLIC_NAMESPACE;

	SPMFillRels(plan);

	save_spmplan_state |= SAVE_SPMPLAN_FILL_EXTRA_DONE;
}

void
SPMFillRunTime(void)
{
	if (!SPM_ENABLED || CAP_SPMPLAN_ABNORMAL)
		return;

	pre_spmplan->execute_time = GetCurrentTimestamp() - pre_spmplan->created;

	save_spmplan_state |= SAVE_SPMPLAN_FILL_RUNTIME_DONE;
}

bool
ValidPreSPMPlan(bool output_log)
{
	/*
	 * When automatic capture is disabled, except for explain capture/apply, capturing SPM plans is
	 * not allowed.
	 */
	if (CAP_SPMPLAN_ABNORMAL || (!(save_spmplan_state & SAVE_SPMPLAN_CAPTURE) &&
	                             !(save_spmplan_state & SAVE_SPMPLAN_APPLY) &&
	                             optimizer_capture_sql_plan_baselines == SPM_CAPTURE_OFF))
		return false;

	if ((save_spmplan_state & SAVE_SPMPLAN_FILL_SQL_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_GUC_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_RELS_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_INDEX_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_HINT_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_PLAN_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_EXTRA_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_RUNTIME_DONE) &&
	    (save_spmplan_state & SAVE_SPMPLAN_FILL_PARAMLIST_DONE))
	{
		return true;
	}
	else
	{
		if (output_log)
		{
			int level = (debug_print_spm) ? LOG: DEBUG5;
			elog(level,
			     "[SPM] capture interrupted due to incomplete captured content:"
			     "SQL[%s], GUC[%s], Rels[%s], Index[%s], Hint[%s], Plan[%s], Paramlist[%s], Extra "
			     "data[%s], , Runtime[%s]",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_SQL_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_GUC_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_RELS_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_INDEX_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_HINT_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_PLAN_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_PARAMLIST_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_EXTRA_DONE) ? "Done" : "Not Done",
			     (save_spmplan_state & SAVE_SPMPLAN_FILL_RUNTIME_DONE) ? "Done" : "Not Done");
		}

		return false;
	}
}

bool
MatchRuleSPMPlan(void)
{
	int  level = (debug_print_spm) ? LOG: DEBUG5;
	bool ret = true;

	if (CAP_SPM_MANUAL)
		return true;

	if (spm_capture_rules.execute_time > -1 &&
	    pre_spmplan->execute_time < spm_capture_rules.execute_time)
	{
		ret = false;
		elog(
			level,
			"[SPM] MatchRuleSPMPlan: no need to save current spm plan. actual_time: %ld, rules_time: %ld",
			pre_spmplan->execute_time,
			spm_capture_rules.execute_time);
	}

	return ret;
}

static void
AddSPMPlanHistory(LocalSPMPlanInfo *spmplan)
{
	(void) AddSPMPlanHistoryInternal(CAP_SPM_MANUAL, spmplan);
	return;
}

static bool
AddSPMPlan(LocalSPMPlanInfo *spmplan)
{
	(void) AddSPMPlanInternal(false, save_spmplan_state & SAVE_SPMPLAN_APPLY, spmplan);
	return true;
}

void
CaptureSPMPlan(void)
{
	uint64 old_nRequestedXid = 0;

	if (ORA_MODE)
		old_nRequestedXid = nRequestedXid;

	if (!SPM_ENABLED || (save_spmplan_state & SAVE_SPMPLAN_APPLYED) || !ValidPreSPMPlan(true) || !MatchRuleSPMPlan())
		return;

	AddSPMPlanHistory(pre_spmplan);

	if (pre_spmplan->priority == SPM_PLAN_FIXED)
		AddSPMPlan(pre_spmplan);

	if (ORA_MODE)
		nRequestedXidSPMUsed += nRequestedXid - old_nRequestedXid;
}

static HeapTuple
FormSPMPlanTupe(Relation rel, LocalSPMPlanInfo *spmplan)
{
	int      i = 0;
	bool     nulls[Natts_pg_spm_plan];
	Datum    values[Natts_pg_spm_plan];

	for (i = 0; i < Natts_pg_spm_plan; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) NULL;
	}

	values[Anum_pg_spm_plan_spmname - 1] = NameGetDatum(&spmplan->spmname);
	values[Anum_pg_spm_plan_spmnsp - 1] = ObjectIdGetDatum(spmplan->spmnsp);
	values[Anum_pg_spm_plan_ownerid - 1] = ObjectIdGetDatum(spmplan->ownerid);

	values[Anum_pg_spm_plan_created - 1] = TimestampTzGetDatum(spmplan->created);
	values[Anum_pg_spm_plan_last_modified - 1] = TimestampTzGetDatum(spmplan->last_modified);
	values[Anum_pg_spm_plan_last_verified - 1] = TimestampTzGetDatum(spmplan->last_verified);
	values[Anum_pg_spm_plan_version - 1] = Int64GetDatum(-1);
	values[Anum_pg_spm_plan_elapsed_time - 1] = Int64GetDatum(spmplan->execute_time);
	values[Anum_pg_spm_plan_executions - 1] = Int64GetDatum(spmplan->executions);
	values[Anum_pg_spm_plan_task_id - 1] = Int64GetDatum(-1);
	values[Anum_pg_spm_plan_sql_normalize_rule - 1] = Int32GetDatum(spmplan->sql_normalize_rule);
	values[Anum_pg_spm_plan_priority - 1] = Int32GetDatum(spmplan->priority);
	values[Anum_pg_spm_plan_cost - 1] = Float4GetDatum(spmplan->cost);
	values[Anum_pg_spm_plan_enabled - 1] = BoolGetDatum(spmplan->enabled);
	values[Anum_pg_spm_plan_autopurge - 1] = BoolGetDatum(spmplan->autopurge);

	if (spmplan->reloids)
		values[Anum_pg_spm_plan_reloids - 1] = PointerGetDatum(spmplan->reloids);
	else
		nulls[Anum_pg_spm_plan_reloids - 1] = true;

	if (spmplan->indexoids)
		values[Anum_pg_spm_plan_indexoids - 1] = PointerGetDatum(spmplan->indexoids);
	else
		nulls[Anum_pg_spm_plan_indexoids - 1] = true;

	values[Anum_pg_spm_plan_sql_id - 1] = Int64GetDatum(spmplan->sql_id);
	values[Anum_pg_spm_plan_plan_id - 1] = Int64GetDatum(spmplan->plan_id);

	values[Anum_pg_spm_plan_sql_normalize - 1] = CStringGetTextDatum(spmplan->sql_normalize);
	values[Anum_pg_spm_plan_sql - 1] = CStringGetTextDatum(spmplan->sql);
	values[Anum_pg_spm_plan_hint - 1] = CStringGetTextDatum(spmplan->hint);
	values[Anum_pg_spm_plan_hint_detail - 1] = CStringGetTextDatum(spmplan->hint_detail);
	values[Anum_pg_spm_plan_plan - 1] = CStringGetTextDatum(spmplan->plan);
	values[Anum_pg_spm_plan_guc_list - 1] = CStringGetTextDatum(spmplan->guc_list);
	if (strlen(spmplan->param_list) > 0)
	{
		values[Anum_pg_spm_plan_param_list - 1] = CStringGetTextDatum(spmplan->param_list);
	}
	else
		nulls[Anum_pg_spm_plan_param_list - 1] = true;

	if (spmplan->relnames)
		values[Anum_pg_spm_plan_relnames - 1] = (Datum) spmplan->relnames;
	else
		nulls[Anum_pg_spm_plan_relnames - 1] = true;

	if (spmplan->indexnames)
		values[Anum_pg_spm_plan_indexnames - 1] = (Datum) spmplan->indexnames;
	else
		nulls[Anum_pg_spm_plan_indexnames - 1] = true;

	if (strlen(spmplan->description) > 0)
		values[Anum_pg_spm_plan_description - 1] = CStringGetTextDatum(spmplan->description);
	else
		nulls[Anum_pg_spm_plan_description - 1] = true;

	values[Anum_pg_spm_plan_origin - 1] = CStringGetTextDatum(NameStr(spmplan->origin));

	return heap_form_tuple(RelationGetDescr(rel), values, nulls);
}

static bool
SPMTupleDelete(Relation relation, ItemPointer tid)
{
	HTSU_Result           result;
	HeapUpdateFailureData hufd;
	bool                  ret = true;

	result = heap_delete(relation,
	                     tid,
	                     GetCurrentCommandId(true),
	                     InvalidSnapshot,
	                     true /* wait for commit */,
	                     &hufd,
	                     false /* changingPart */);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			break;

		case HeapTupleUpdated:
			ret = false;
			break;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			break;
	}

	return ret;
}

static void
SyncHistoryCacheToTab(Relation rel, LocalSPMPlanInfo *spmplan)
{
	Oid			relid = RelationGetRelid(rel);
	List		*plan_list = NIL;
	ListCell	*lc;

	if (relid != SPMPlanHistoryRelationId)
	{
		return;
	}

	plan_list = SPMHistoryCacheSearchBySqlid(spmplan->sql_id);
	foreach(lc, plan_list)
	{
		HeapTuple   heaptup = NULL;
		HeapTuple	newTuple = NULL;
		SysScanDesc scan;
		ScanKeyData entry[2];
		SPMHistoryCachedItemForTblSync *item = (SPMHistoryCachedItemForTblSync *) lfirst(lc);
		bool		nulls[Natts_pg_spm_plan_history];
		Datum		values[Natts_pg_spm_plan_history];
		bool		replace[Natts_pg_spm_plan_history];

		ScanKeyInit(&entry[0],
			Anum_pg_spm_plan_history_sql_id,
			BTEqualStrategyNumber,
			F_INT8EQ,
			spmplan->sql_id);
		ScanKeyInit(&entry[1], Anum_pg_spm_plan_history_plan_id, BTEqualStrategyNumber, F_INT8EQ,
					item->plan_id);
		scan = systable_beginscan(rel, SPMPlanHistorySqlidPlanidIndexId, true, NULL, 2, entry);
		heaptup = systable_getnext(scan);

		if (HeapTupleIsValid(heaptup))
		{
			memset(nulls, false, sizeof(nulls));
			memset(values, 0, sizeof(values));
			memset(replace, 0, sizeof(replace));
			nulls[Anum_pg_spm_plan_executions - 1] = false;
			values[Anum_pg_spm_plan_executions - 1] = Int64GetDatum(item->executions);
			replace[Anum_pg_spm_plan_executions - 1] = true;
			nulls[Anum_pg_spm_plan_elapsed_time - 1] = false;
			values[Anum_pg_spm_plan_elapsed_time - 1] = Int64GetDatum(item->elapsed_time);
			replace[Anum_pg_spm_plan_elapsed_time - 1] = true;
			newTuple = heap_modify_tuple(heaptup, RelationGetDescr(rel), values, nulls, replace);
			CatalogTupleUpdateNoError(rel, &newTuple->t_self, newTuple);
		}

		systable_endscan(scan);
	}

	if (plan_list)
		list_free_deep(plan_list);
}

static Oid
InsertOrUpdateSPMPlan(Relation rel, HeapTuple oldtup, bool isreplace, LocalSPMPlanInfo *spmplan)
{
	HeapTuple     tup = NULL;
	Oid           spmplan_id = 0;
	Oid           relid = RelationGetRelid(rel);
	ObjectAddress myself;
	ObjectAddress referenced;
	bool          doinsert = true;

	if (HeapTupleIsValid(oldtup))
	{
		spmplan_id = HeapTupleGetOid(oldtup);

		if (isreplace)
		{
			LocalSPMPlanInfo *deformed_spmplan = NULL;
			doinsert = true;

			deformed_spmplan = (LocalSPMPlanInfo *) 
								palloc0(sizeof(LocalSPMPlanInfo));
			InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
			DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
			if (relid == SPMPlanRelationId)
			{
				SPMPlanHashTblDelete(MyDatabaseId, deformed_spmplan->sql_id, 
										deformed_spmplan->plan_id);
			}
			else
			{
				Assert(relid == SPMPlanHistoryRelationId);
				SPMHistoryHashTblDelete(MyDatabaseId, deformed_spmplan->sql_id, 
											deformed_spmplan->plan_id);
			}
			DeepFreeSPMPlan(deformed_spmplan, false);
			pfree(deformed_spmplan);

			if (!SPMTupleDelete(rel, &oldtup->t_self))
			{
				if (debug_print_spm)
					elog(LOG, "[SPM] SPMTupleDelete tuple concurrently updated");

				return InvalidOid;
			}
		}
		else
			doinsert = false;
	}

	if (doinsert)
	{
		int nreloids = 0;
		int nindexoids = 0;

		SyncHistoryCacheToTab(rel, spmplan);
		tup = FormSPMPlanTupe(rel, spmplan);

		spmplan_id = ExecInsertCheckConflict(rel, tup);
		if (!OidIsValid(spmplan_id))
		{
			heap_freetuple(tup);
			return spmplan_id;
		}

		/* Depend on schema */
		myself.classId = relid;
		myself.objectId = spmplan_id;
		myself.objectSubId = 0;

		referenced.classId = NamespaceRelationId;
		referenced.objectId = spmplan->spmnsp;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

		nreloids = (spmplan->reloids) ? spmplan->reloids->dim1 : 0;
		while (nreloids)
		{
			nreloids--;
			referenced.classId = RelationRelationId;
			referenced.objectId = spmplan->reloids->values[nreloids];
			referenced.objectSubId = 0;
			recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
		}

		nindexoids = (spmplan->indexoids != NULL) ? spmplan->indexoids->dim1 : 0;
		while (nindexoids)
		{
			nindexoids--;
			referenced.classId = RelationRelationId;
			referenced.objectId = spmplan->indexoids->values[nindexoids];
			referenced.objectSubId = 0;
			recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
		}

		heap_freetuple(tup);

		if (relid == SPMPlanRelationId)
		{
			SPMPlanHashTblDeleteEntry(MyDatabaseId, spmplan->sql_id);
		}
		else
		{
			Assert(relid == SPMPlanHistoryRelationId);
			SPMHistoryHashTblDeleteEntry(MyDatabaseId, spmplan->sql_id);
		}

		CommandCounterIncrement();
	}

	return spmplan_id;
}

static Oid
AddSPMPlanHistoryInternal(bool isreplace, LocalSPMPlanInfo *spmplan)
{
	Relation          rel;
	HeapTuple         tup = NULL;
	HeapTuple         oldtup = NULL;
	Oid               spmplan_id = InvalidOid;
	Oid               relid = SPMPlanHistoryRelationId;
	SysScanDesc       scan;
	ScanKeyData       entry[2];
	SnapshotData      SnapshotDirty;
	SPMPlanCacheRetNo cacheret;
	int               level = (debug_print_spm) ? LOG : DEBUG5;

	cacheret = SPMHistoryCacheSearchByPlanId(MyDatabaseId,
	                                         spmplan->sql_id,
	                                         spmplan->plan_id,
	                                         NULL,
	                                         NULL,
	                                         spmplan->execute_time);
	if (cacheret == SPM_PLAN_CACHE_MATCHED && !isreplace)
	{
		return InvalidOid;
	}

	rel = heap_open(relid, RowExclusiveLock);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            spmplan->sql_id);
	ScanKeyInit(&entry[1], Anum_pg_spm_plan_history_plan_id, BTEqualStrategyNumber, F_INT8EQ,
	            spmplan->plan_id);
	
	InitDirtySnapshot(SnapshotDirty);

	/*
	 * In the current implementation, if the same spm plan exists with open transactions,
	 * no new plan record will be inserted
	 */
	scan = systable_beginscan(rel, SPMPlanHistorySqlidPlanidIndexId, true, &SnapshotDirty, 2, entry);
	tup = systable_getnext(scan);
	oldtup = heap_copytuple(tup);

	if (HeapTupleIsValid(oldtup))
	{
		Buffer buffer;
		LocalSPMPlanInfo *deformed_spmplan;

		buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&(oldtup->t_self)));
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		if (!HeapTupleSatisfiesVisibility(oldtup,
		                                  ActiveSnapshotSet() ? GetActiveSnapshot()
		                                                      : GetTransactionSnapshot(),
		                                  buffer))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return InvalidOid;
		}

		deformed_spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));
		InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
		DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
		SPMHistoryHashTabInsert(deformed_spmplan->sql_id, deformed_spmplan->plan_id, deformed_spmplan->executions, deformed_spmplan->elapsed_time);
		DeepFreeSPMPlan(deformed_spmplan, false);
		pfree(deformed_spmplan);

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
	}

	systable_endscan(scan);

	if (IS_LOCAL_ACCESS_NODE)
	{
		spmplan_id = InsertOrUpdateSPMPlan(rel, oldtup, isreplace, spmplan);
	}

	heap_close(rel, RowExclusiveLock);

	if (OidIsValid(spmplan_id))
		elog(level,
			     "[SPM] AddSPMPlanHistoryInternal success. dboid: %u, userid: %u, sql_id: %ld, plan_id: %ld",
			     MyDatabaseId,
				 spmplan->ownerid,
			     spmplan->sql_id,
			     spmplan->plan_id);

	return spmplan_id;
}

/*
 * CreateSPMPlan
 */
static Oid
AddSPMPlanInternal(bool isreplace, bool apply, LocalSPMPlanInfo *spmplan)
{
	Relation    rel;
	HeapTuple   oldtup = NULL;
	Oid         spmplan_id;
	Oid         relid = SPMPlanRelationId;
	SysScanDesc scan;
	ScanKeyData entry[1];
	SnapshotData SnapshotDirty;

	rel = heap_open(relid, RowExclusiveLock);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            spmplan->sql_id);

	InitDirtySnapshot(SnapshotDirty);
	scan = systable_beginscan(rel, SPMPlanSqlidPlanidIndexId, true, &SnapshotDirty, 1, entry);
	oldtup = systable_getnext(scan);
	if (apply && HeapTupleIsValid(oldtup))
	{
		Buffer buffer;
		LocalSPMPlanInfo *deformed_spmplan = NULL;

		buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&(oldtup->t_self)));
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		if (!HeapTupleSatisfiesVisibility(oldtup, ActiveSnapshotSet() ? GetActiveSnapshot() : GetTransactionSnapshot(),
		                                  buffer))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return InvalidOid;
		}

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);

		deformed_spmplan = (LocalSPMPlanInfo *) 
						palloc0(sizeof(LocalSPMPlanInfo));
		InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
		DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
		SPMPlanHashTblDelete(MyDatabaseId, deformed_spmplan->sql_id, 
									deformed_spmplan->plan_id);
		DeepFreeSPMPlan(deformed_spmplan, false);
		pfree(deformed_spmplan);

		if (!SPMTupleDelete(rel, &oldtup->t_self))
		{
			if (debug_print_spm)
				elog(LOG, "[SPM] SPMTupleDelete tuple concurrently updated");

			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return InvalidOid;
		}

		oldtup = NULL;
	}

	spmplan_id = InsertOrUpdateSPMPlan(rel, oldtup, isreplace, spmplan);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	if (IS_PGXC_LOCAL_COORDINATOR)
	{
		ExecSendSPMPlan(spmplan, SPM_PLAN_SEND_PLAN, NULL, NULL);
	}

	return spmplan_id;
}

static Oid
SPMPlanApply(int64 sql_id, int64 plan_id, bool fixed, bool enabled)
{
	Relation    rel;
	HeapTuple   oldtup = NULL;
	Oid         spmplan_id = InvalidOid;
	Oid         relid = SPMPlanHistoryRelationId;
	SysScanDesc scan;
	ScanKeyData entry[2];

	rel = heap_open(relid, AccessShareLock);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            sql_id);
	ScanKeyInit(&entry[1],
	            Anum_pg_spm_plan_history_plan_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            plan_id);

	scan = systable_beginscan(rel, SPMPlanHistorySqlidPlanidIndexId, true, NULL, 2, entry);
	oldtup = systable_getnext(scan);
	if (HeapTupleIsValid(oldtup))
	{
		LocalSPMPlanInfo *deformed_spmplan = (LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));

		InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
		DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
		if (fixed)
			deformed_spmplan->priority = SPM_PLAN_FIXED;
		else
			deformed_spmplan->priority = SPM_APPLY_ACCEPT;

		deformed_spmplan->enabled = enabled;
		deformed_spmplan->last_modified = GetCurrentStatementStartTimestamp();
		deformed_spmplan->last_verified = GetCurrentStatementStartTimestamp();

		spmplan_id = AddSPMPlanInternal(true, true, deformed_spmplan);
		DeepFreeSPMPlan(deformed_spmplan, false);
		pfree(deformed_spmplan);
	}
	else
	{
		systable_endscan(scan);
		heap_close(rel, AccessShareLock);
		elog(ERROR, "[SPM] This record does not exist sql_id: %ld, plan_id: %ld", sql_id, plan_id);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return spmplan_id;
}

Datum
pg_spm_load_plans_from_history(PG_FUNCTION_ARGS)
{
	int64 sql_id = PG_GETARG_INT64_0_IF_EXISTS(0);
	int64 plan_id = PG_GETARG_INT64_0_IF_EXISTS(1);
	bool  fixed = PG_GETARG_BOOL(2);
	bool  enabled = PG_GETARG_BOOL(3);
	int   ret = 0;

	/* 
	 * This function requires synchronizing the modifications of pg_spm_plan 
	 * to other CN nodes, which requires a writable connection to those nodes. 
	 * However, for the "execute direct on" operation, the connection is 
	 * read-only, so calling this function through a non-direct CN connection 
	 * is not possible.
	 */
	if (!IS_ACCESS_NODE)
		PG_RETURN_INT32(0);

	if (sql_id <= 1 || plan_id <= 1)
	{
		elog(ERROR, "[SPM] invalid input params, sql_id: %ld, plan_id: %ld", sql_id, plan_id);
	}

	if (SPMPlanApply(sql_id, plan_id, fixed, enabled) != InvalidOid)
	{
		ret++;
	}

	PG_RETURN_INT32(ret);
}

int
DropSPMPlan(Oid relid, Oid indexid, int64 sql_id, int64 plan_id)
{
	Relation    rel;
	HeapTuple   oldtup = NULL;
	SysScanDesc scan;
	ScanKeyData entry[1];
	int         cnt = 0;
	bool        nulls[Natts_pg_spm_plan];
	Datum       values[Natts_pg_spm_plan];
	TupleDesc   desc;
	SnapshotData SnapshotDirty;

	rel = heap_open(relid, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            sql_id);

	InitDirtySnapshot(SnapshotDirty);
	scan = systable_beginscan(rel, indexid, true, &SnapshotDirty, 1, entry);
	while ((oldtup = systable_getnext(scan)) != NULL)
	{
		Buffer buffer = ReadBuffer(rel, ItemPointerGetBlockNumber(&(oldtup->t_self)));
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		if (!HeapTupleSatisfiesVisibility(oldtup,
		                                  ActiveSnapshotSet() ? GetActiveSnapshot()
		                                                      : GetTransactionSnapshot(),
		                                  buffer))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
			continue;
		}

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);

		if (plan_id > 1)
		{
			heap_deform_tuple(oldtup, desc, values, nulls);
			if (DatumGetInt64(values[Anum_pg_spm_plan_plan_id - 1]) != plan_id)
			{
				continue;
			}

			if (relid == SPMPlanRelationId)
			{
				SPMPlanHashTblDelete(MyDatabaseId, sql_id, plan_id);
			}
			else if (relid == SPMPlanHistoryRelationId)
			{
				SPMHistoryHashTblDelete(MyDatabaseId, sql_id, plan_id);
			}
		}
		else
		{
			if (relid == SPMPlanRelationId)
			{
				SPMPlanHashTblDeleteEntry(MyDatabaseId, sql_id);
			}
			else if (relid == SPMPlanHistoryRelationId)
			{
				SPMHistoryHashTblDeleteEntry(MyDatabaseId, sql_id);
			}
		}
		

		if (!SPMTupleDelete(rel, &oldtup->t_self))
		{
			if (debug_print_spm)
				elog(LOG, "[SPM] SPMTupleDelete tuple concurrently updated");

			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return cnt;
		}

		if (relid == SPMPlanRelationId && IS_PGXC_LOCAL_COORDINATOR)
		{
			LocalSPMPlanInfo *deformed_spmplan = (LocalSPMPlanInfo *) 
													palloc0(sizeof(LocalSPMPlanInfo));
			InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
			deformed_spmplan->sql_id = sql_id;
			deformed_spmplan->plan_id = plan_id;
			ExecSendSPMPlan(deformed_spmplan, SPM_PLAN_SEND_PLAN_DELETE, NULL, NULL);
			DeepFreeSPMPlan(deformed_spmplan, false);
			pfree(deformed_spmplan);
		}

		cnt++;
	}
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return cnt;
}

Datum
pg_spm_drop_sql_plan_baseline(PG_FUNCTION_ARGS)
{
	int64 sql_id = PG_GETARG_INT64_0_IF_EXISTS(0);
	int64 plan_id = PG_GETARG_INT64_0_IF_EXISTS(1);
	int   level = (debug_print_spm) ? LOG : DEBUG5;
	int   ret = 0;
	int   ret2 = 0;

	if (!IS_ACCESS_NODE)
		PG_RETURN_INT32(0);

	if (sql_id <= 1 || (!PG_ARGISNULL(1) && plan_id <= 1))
	{
		elog(ERROR, "[SPM] invalid input params");
	}

	ret = DropSPMPlan(SPMPlanRelationId, SPMPlanSqlidPlanidIndexId, sql_id, plan_id);
	elog(level, "[SPM] pg_spm_drop_sql_plan_baseline, Delete %d records from the table pg_spm_plan.", ret);

	ret2 = DropSPMPlan(SPMPlanHistoryRelationId, SPMPlanHistorySqlidPlanidIndexId, sql_id, plan_id);
	elog(level,
	     "[SPM] pg_spm_drop_sql_plan_baseline, Delete %d records from the table pg_spm_plan_history.",
	     ret2);

	PG_RETURN_INT32(ret + ret2);
}

Datum
pg_spm_reload_sql_plan_cache(PG_FUNCTION_ARGS)
{
	Relation	rel;
	SysScanDesc sscan;
	HeapTuple	tuple;
	LocalSPMPlanInfo *deformed_spmplan = NULL;

	SPMCacheHashTabClear();

	deformed_spmplan = (LocalSPMPlanInfo *) 
						palloc0(sizeof(LocalSPMPlanInfo));
	InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);

	rel = heap_open(SPMPlanRelationId, AccessShareLock);
	sscan = systable_beginscan(rel, InvalidOid, false,
							   NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		DeformSPMPlanTupe(rel, tuple, deformed_spmplan);
		SPMPlanHashTabInsert(deformed_spmplan->sql_id, 
						deformed_spmplan->plan_id, 
						deformed_spmplan->hint, 
						deformed_spmplan->priority, 
						deformed_spmplan->enabled, 
						deformed_spmplan->executions);
	}
	systable_endscan(sscan);
	heap_close(rel, AccessShareLock);

	rel = heap_open(SPMPlanHistoryRelationId, AccessShareLock);
	sscan = systable_beginscan(rel, InvalidOid, false,
							   NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		DeformSPMPlanTupe(rel, tuple, deformed_spmplan);
		SPMHistoryHashTabInsert(deformed_spmplan->sql_id, 
							deformed_spmplan->plan_id,
							deformed_spmplan->executions,
							deformed_spmplan->elapsed_time);
	}
	systable_endscan(sscan);
	heap_close(rel, AccessShareLock);

	DeepFreeSPMPlan(deformed_spmplan, false);
	pfree(deformed_spmplan);

	PG_RETURN_BOOL(true);
}

static SPMPlanCachedItemIdx
GetSPMUpdatedParameters(char *attribute_name_str, char *attribute_value_str, Datum *values,
                        bool *replaces, bool *fixed)
{
	AlterSPMPlanParamType param_type = -1;
	int                   i;
	bool                  attribute_value = true;
	SPMPlanCachedItemIdx	idx = SPM_PLAN_CACHE_ITEM_INVALID;

	if (attribute_name_str == NULL || attribute_value_str == NULL)
		elog(ERROR, "[SPM] Invalid params");

	for (i = 0; attribute_name_str[i]; i++)
		attribute_name_str[i] = tolower(attribute_name_str[i]);

	if (strcmp(attribute_name_str, "enabled") == 0)
		param_type = PARAM_ENABLED;
	else if (strcmp(attribute_name_str, "fixed") == 0)
		param_type = PARAM_FIXED;
	else if (strcmp(attribute_name_str, "autopurge") == 0)
		param_type = PARAM_AUTOPURGE;
	else if (strcmp(attribute_name_str, "plan_name") == 0)
		param_type = PARAM_PLAN_NAME;
	else if (strcmp(attribute_name_str, "description") == 0)
		param_type = PARAM_DESCRIPTION;
	else
	{
		elog(ERROR,
		     "[SPM] Invalid attribute name: %s. Currently, only the following are supported: enabled, "
		     "fixed, autopurge, plan_name, and description. ",
		     attribute_name_str);
		return idx;
	}

	switch (param_type)
	{
		case PARAM_ENABLED:
		case PARAM_FIXED:
		case PARAM_AUTOPURGE:
			for (i = 0; attribute_value_str[i]; i++)
				attribute_value_str[i] = tolower(attribute_value_str[i]);

			if (strcmp(attribute_value_str, "yes") == 0)
				attribute_value = true;
			else if (strcmp(attribute_value_str, "no") == 0)
				attribute_value = false;
			else
				elog(ERROR,
				     "[SPM] Invalid attribute value: %s. Currently, only the following are supported: "
				     "yes and no",
				     attribute_value_str);

			if (param_type == PARAM_ENABLED)
			{
				values[Anum_pg_spm_plan_enabled - 1] = BoolGetDatum(attribute_value);
				replaces[Anum_pg_spm_plan_enabled - 1] = true;
				idx = SPM_PLAN_CACHE_ITEM_ENABLED;
			}
			else if (param_type == PARAM_FIXED)
			{
				if (attribute_value)
				{
					values[Anum_pg_spm_plan_priority - 1] = Int32GetDatum(SPM_PLAN_FIXED);
					replaces[Anum_pg_spm_plan_priority - 1] = true;
				}
				else
				{
					values[Anum_pg_spm_plan_priority - 1] = Int32GetDatum(SPM_APPLY_ACCEPT);
					replaces[Anum_pg_spm_plan_priority - 1] = true;
				}

				if (fixed != NULL && attribute_value)
					*fixed = true;
				idx = SPM_PLAN_CACHE_ITEM_PRIORITY;
			}
			else if (param_type == PARAM_AUTOPURGE)
			{
				values[Anum_pg_spm_plan_autopurge - 1] = BoolGetDatum(attribute_value);
				replaces[Anum_pg_spm_plan_autopurge - 1] = true;
			}

			break;
		case PARAM_PLAN_NAME:
			if (strlen(attribute_value_str) > 30)
			{
				elog(ERROR, "[SPM] Plan name exceeds maximum length of 30 characters");
			}
			else
			{
				NameData n_name;

				namestrcpy(&n_name, attribute_value_str);
				values[Anum_pg_spm_plan_spmname - 1] = NameGetDatum(&n_name);
				replaces[Anum_pg_spm_plan_spmname - 1] = true;
			}
			break;
		case PARAM_DESCRIPTION:
			if (strlen(attribute_value_str) > 500)
			{
				elog(ERROR, "[SPM] Description exceeds maximum length of 500 bytes");
			}
			else
			{
				values[Anum_pg_spm_plan_description - 1] = CStringGetTextDatum(attribute_value_str);
				replaces[Anum_pg_spm_plan_description - 1] = true;
			}
			break;
		default:
			break;
	}
	return idx;
}

int
AlterSPMPlan(int64 sql_id, int64 plan_id, char *attribute_name, char *attribute_value)
{
	Relation    rel;
	HeapTuple   oldtup = NULL;
	Oid         relid = SPMPlanRelationId;
	SysScanDesc scan;
	ScanKeyData entry[1];
	int         cnt = 0;

	if (!SearchSPMPlan(sql_id, &plan_id))
	{
		return 0;
	}

	rel = heap_open(relid, RowExclusiveLock);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            sql_id);

	scan = systable_beginscan(rel, SPMPlanSqlidPlanidIndexId, true, NULL, 1, entry);
	oldtup = systable_getnext(scan);
	if (HeapTupleIsValid(oldtup))
	{
		bool  nulls[Natts_pg_spm_plan];
		bool  replaces[Natts_pg_spm_plan];
		Datum values[Natts_pg_spm_plan];

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		heap_deform_tuple(oldtup, RelationGetDescr(rel), values, nulls);

		if (DatumGetInt64(values[Anum_pg_spm_plan_plan_id - 1]) == plan_id)
		{
			SPMPlanCachedItemIdx	idx;
			memset(values, 0, sizeof(values));
			memset(nulls, false, sizeof(nulls));

			idx = GetSPMUpdatedParameters(attribute_name, attribute_value, 
											values, replaces, NULL);
			if (idx != SPM_PLAN_CACHE_ITEM_INVALID)
			{
				/* Delete cache in case of transaction rollback */
				SPMPlanHashTblDelete(MyDatabaseId, sql_id, plan_id);
			}
			oldtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);
			CatalogTupleUpdate(rel, &oldtup->t_self, oldtup);

			if (IS_PGXC_LOCAL_COORDINATOR)
			{
				LocalSPMPlanInfo *deformed_spmplan = (LocalSPMPlanInfo *) 
														palloc0(sizeof(LocalSPMPlanInfo));
				InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
				DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);
				deformed_spmplan->sql_id = sql_id;
				deformed_spmplan->plan_id = plan_id;
				ExecSendSPMPlan(deformed_spmplan, SPM_PLAN_SEND_PLAN_UPDATE, 
								attribute_name, attribute_value);
				DeepFreeSPMPlan(deformed_spmplan, false);
				pfree(deformed_spmplan);
			}

			cnt++;
		}
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return cnt;
}

static int
AlterSPMPlanHistory(int64 sql_id, int64 plan_id, char *attribute_name, char *attribute_value)
{
	Relation    rel;
	HeapTuple   oldtup = NULL;
	Oid         relid = SPMPlanHistoryRelationId;
	SysScanDesc scan;
	ScanKeyData entry[2];
	bool        is_fixed = false;
	int         cnt = 0;

	rel = heap_open(relid, RowExclusiveLock);

	ScanKeyInit(&entry[0],
	            Anum_pg_spm_plan_history_sql_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            sql_id);
	ScanKeyInit(&entry[1],
	            Anum_pg_spm_plan_history_plan_id,
	            BTEqualStrategyNumber,
	            F_INT8EQ,
	            plan_id);

	scan = systable_beginscan(rel, SPMPlanHistorySqlidPlanidIndexId, true, NULL, 2, entry);
	oldtup = systable_getnext(scan);

	if (HeapTupleIsValid(oldtup))
	{
		bool  nulls[Natts_pg_spm_plan];
		bool  replaces[Natts_pg_spm_plan];
		Datum values[Natts_pg_spm_plan];

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));
		memset(replaces, false, sizeof(replaces));

		GetSPMUpdatedParameters(attribute_name, attribute_value, values, replaces, &is_fixed);

		oldtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);

		CatalogTupleUpdate(rel, &oldtup->t_self, oldtup);
		cnt++;

		if (is_fixed)
		{
			LocalSPMPlanInfo *deformed_spmplan =
				(LocalSPMPlanInfo *) palloc0(sizeof(LocalSPMPlanInfo));

			InitSPMPlanInfo(deformed_spmplan, CurrentMemoryContext);
			DeformSPMPlanTupe(rel, oldtup, deformed_spmplan);

			deformed_spmplan->last_modified = GetCurrentStatementStartTimestamp();
			deformed_spmplan->last_verified = GetCurrentStatementStartTimestamp();

			(void) AddSPMPlanInternal(true, true, deformed_spmplan);
			DeepFreeSPMPlan(deformed_spmplan, false);
			pfree(deformed_spmplan);
		}
	}
	else
	{
		systable_endscan(scan);
		heap_close(rel, RowExclusiveLock);
		elog(ERROR, "[SPM] This record does not exist sql_id: %ld, plan_id: %ld", sql_id, plan_id);
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
	return cnt;
}

Datum
pg_spm_alter_sql_plan_baseline(PG_FUNCTION_ARGS)
{
	int64 sql_id = PG_GETARG_INT64_1_IF_EXISTS(0);
	int64 plan_id = PG_GETARG_INT64_0_IF_EXISTS(1);
	Datum attribute_name = PG_GETARG_DATUM(2);
	Datum attribute_value = PG_GETARG_DATUM(3);
	char *attribute_name_str = TextDatumGetCString(attribute_name);
	char *attribute_value_str = TextDatumGetCString(attribute_value);
	int		level = (debug_print_spm) ? LOG : DEBUG5;
	int		ret1;
	int		ret2;

	/* 
	 * This function requires synchronizing the modifications of pg_spm_plan 
	 * to other CN nodes, which requires a writable connection to those nodes. 
	 * However, for the "execute direct on" operation, the connection is 
	 * read-only, so calling this function through a non-direct CN connection 
	 * is not possible.
	 */
	if (!IS_ACCESS_NODE)
		PG_RETURN_INT32(0);

	if (sql_id <= 1 || plan_id <= 1)
	{
		elog(ERROR, "[SPM] invalid input params, sql_id: %ld, plan_id: %ld", sql_id, plan_id);
	}

	ret1 = AlterSPMPlan(sql_id, plan_id, attribute_name_str, attribute_value_str);
	elog(level, "[SPM] pg_spm_alter_sql_plan_baseline, Alter %d records in the table pg_spm_plan.", ret1);

	ret2 = AlterSPMPlanHistory(sql_id, plan_id, attribute_name_str, attribute_value_str);
	elog(level,
	     "[SPM] pg_spm_alter_sql_plan_baseline, Alter %d records in the table pg_spm_plan_history.",
	     ret2);

	PG_RETURN_INT32(ret1 + ret2);
}

void
SyncSPMPlan(LocalSPMPlanInfo *spmplan)
{
	AddSPMPlanInternal(false, true, spmplan);
}
