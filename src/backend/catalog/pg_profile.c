/*-------------------------------------------------------------------------
 *
 * pg_profile.c
 *	routines to support manipulation of the pg_profile relation
 *
 *-------------------------------------------------------------------------
 */
 
#include "postgres.h"

#include <stdlib.h>
#include <math.h>
#include "catalog/pg_profile.h"
#include "catalog/pg_profile_cost_weight.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "utils/acl.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_authid.h"
#include "commands/defrem.h"
#include "common/opentenbase_ora.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "access/sysattr.h"
#include "utils/memutils.h"
#include "optimizer/clauses.h"
#include "utils/numeric.h"

/* OPENTENBASE_ORA PASSWORD_VERIFY_FUNCTION have 3 arguments: (username, new_password, old_password)*/
#define PASSWD_VERIFY_FUNC_ARG_NUM 3
static void reset_authid(char *pname, Oid profile, DropBehavior behavior);
static bool parse_unit(const char *value, int *result);
static float4 get_profile_param_value(Value *param_value, char* param_name);

bool profile_resource_limit = false;
bool profile_password_limit = false;
ProfileResourceLimit *MyProfile = NULL;
ProfileUserPasswd *MyProfUserPasswd = NULL;
ProfileSessionInfo *MyProfSessionInfo = NULL;
ProfileCostWeight *profcostweight = NULL;

void
PgProfileCreate(ProfileResourceLimit *profile)
{
	Relation	rel;
	HeapTuple	htup;
	bool		nulls[Natts_pg_profile];
	Datum		values[Natts_pg_profile];

	memset(nulls, 0, sizeof(bool) * Natts_pg_profile);
	memset(values, 0, sizeof(Datum) * Natts_pg_profile);

	values[Anum_pg_profile_name - 1]              = DirectFunctionCall1(namein, CStringGetDatum(profile->name));
	values[Anum_pg_profile_sessions - 1]          = Float4GetDatum(profile->sessions);
	values[Anum_pg_profile_session_cpu - 1]       = Float4GetDatum(profile->cpu_per_session);
	values[Anum_pg_profile_query_cpu - 1]         = Float4GetDatum(profile->cpu_per_query);
	values[Anum_pg_profile_connect_time - 1]      = Float4GetDatum(profile->connection_time);
	values[Anum_pg_profile_idle_time - 1]         = Float4GetDatum(profile->idle_time);
	values[Anum_pg_profile_session_blks - 1]      = Float4GetDatum(profile->blks_per_session);
	values[Anum_pg_profile_query_blks - 1]        = Float4GetDatum(profile->blks_per_query);
	values[Anum_pg_profile_private_sga - 1]       = Float4GetDatum(profile->private_sga);
	values[Anum_pg_profile_composite_limit - 1]   = Float4GetDatum(profile->composite_limit);
	values[Anum_pg_profile_failed_log_times - 1]  = Float4GetDatum(profile->failed_login_times);
	values[Anum_pg_profile_passwd_lock_time - 1]  = Float4GetDatum(profile->passwd_lock_time);
	values[Anum_pg_profile_passwd_life_time - 1]  = Float4GetDatum(profile->passwd_life_time);
	values[Anum_pg_profile_passwd_grace_time - 1] = Float4GetDatum(profile->passwd_grace_time);
	values[Anum_pg_profile_passwd_reuse_time - 1] = Float4GetDatum(profile->passwd_reuse_time);
	values[Anum_pg_profile_passwd_reuse_max - 1]  = Float4GetDatum(profile->passwd_reuse_max);
	if (profile->passwd_verify_func)
		values[Anum_pg_profile_verify_function - 1]   = CStringGetTextDatum(profile->passwd_verify_func);
	else
		nulls[Anum_pg_profile_verify_function - 1] = true;
	values[Anum_pg_profile_container - 1]         = Float4GetDatum(profile->container);

	/* Open the relation for insertion */
	rel = heap_open(PgProfileRelationId, RowExclusiveLock);

	htup = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, htup);

	heap_close(rel, NoLock);
}

void
PgProfileRemove(char *pname, DropBehavior behavior)
{
	Relation	rel;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;

	rel = heap_open(PgProfileRelationId, RowExclusiveLock);

	ScanKeyInit(&skey[0],
				Anum_pg_profile_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(pname));

	scan = systable_beginscan(rel, PgProfileNameIndexId, true,
							  NULL, 1, skey);

	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "profile %s does not exist", pname);

    reset_authid(pname, HeapTupleGetOid(tuple), behavior);

	CatalogTupleDelete(rel, &tuple->t_self);

	systable_endscan(scan);
	heap_close(rel, NoLock);
}

static Oid
GetProfileVerifyFunction(char *funcname)
{
	Oid funcid = InvalidOid;
	HeapTuple	tup;
	ScanKeyData scankey;
	SysScanDesc scan;
	Relation	rel;

	rel = heap_open(ProcedureRelationId, AccessShareLock);
	if (ORA_MODE)
		funcname = asc_toupper(funcname, strlen(funcname));
	ScanKeyInit(&scankey,
		Anum_pg_proc_proname,
		BTEqualStrategyNumber, F_NAMEEQ,
		CStringGetDatum(funcname));
	scan = systable_beginscan(rel, ProcedureNameArgsNspIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		bool satisfy = true;
    	Form_pg_proc proc_form = ((Form_pg_proc) GETSTRUCT(tup));

		if (proc_form->pronargs == PASSWD_VERIFY_FUNC_ARG_NUM &&
			proc_form->proargtypes.dim1 == PASSWD_VERIFY_FUNC_ARG_NUM)
		{
			int i = 0;

			for (i = 0; i < proc_form->proargtypes.dim1; i++)
			{
				if (proc_form->proargtypes.values[i] != TEXTOID &&
					proc_form->proargtypes.values[i] != VARCHAR2OID &&
					proc_form->proargtypes.values[i] != NVARCHAR2OID &&
					proc_form->proargtypes.values[i] != VARCHAROID)
				{
					satisfy = false;
					break;
				}
			}

			if (satisfy)
			{
				funcid = HeapTupleGetOid(tup);
				break;
			}
		}
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return funcid;
}
void
CreateProfile(CreateProfileStmt *stmt)
{
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData scankey;
	SysScanDesc scan;
	ListCell   *option;
	ProfileResourceLimit *profile = (ProfileResourceLimit *)palloc0(sizeof(ProfileResourceLimit));

	profile->name = stmt->name;
	if (strcmp(profile->name, "unlimited") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("profile name \"%s\" not permitted to use", stmt->name)));
	}
	/* Check some permissions first */
	if (!has_createprofile_privilege(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create profile")));
	/* check if exists */
	rel = heap_open(PgProfileRelationId, AccessShareLock);
	ScanKeyInit(&scankey,
				Anum_pg_profile_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));
	scan = systable_beginscan(rel, PgProfileNameIndexId, true,
							  NULL, 1, &scankey);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("profile \"%s\" already exists", stmt->name)));

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	/* init profile with default value */
    profile->sessions = INFINITY;
    profile->cpu_per_session = INFINITY;
    profile->cpu_per_query = INFINITY;
    profile->connection_time = INFINITY;
    profile->idle_time = INFINITY;
    profile->blks_per_session = INFINITY;
    profile->blks_per_query = INFINITY;
    profile->private_sga = INFINITY;
    profile->composite_limit = INFINITY;
    profile->failed_login_times = 10;
    profile->passwd_lock_time = 1;
    profile->passwd_life_time = 180;
    profile->passwd_grace_time = 7;
    profile->passwd_reuse_time = INFINITY;
    profile->passwd_reuse_max = INFINITY;
    profile->container = INFINITY;
    profile->passwd_verify_func = NULL;

	/* Extract options from the statement node tree */
	foreach(option, stmt->profile_args)
	{
		ProfileParameter  *defel = (ProfileParameter *) lfirst(option);

		if (strcmp(defel->name, "sessions_per_user") == 0)
		{
			if (!isinf(profile->sessions))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->sessions = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "cpu_per_session") == 0)
		{
			if (!isinf(profile->cpu_per_session))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->cpu_per_session = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "cpu_per_call") == 0)
		{
			if (!isinf(profile->cpu_per_query))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->cpu_per_query = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "connect_time") == 0)
		{
			if (!isinf(profile->connection_time))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->connection_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "idle_time") == 0)
		{
			if (!isinf(profile->idle_time))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->idle_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "logical_reads_per_session") == 0)
		{
			if (!isinf(profile->blks_per_session))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->blks_per_session = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "logical_reads_per_call") == 0)
		{
			if (!isinf(profile->blks_per_query))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->blks_per_query = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "private_sga") == 0)
		{
			if (!isinf(profile->private_sga))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->private_sga = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "composite_limit") == 0)
		{
			if (!isinf(profile->composite_limit))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->composite_limit = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "failed_login_attempts") == 0)
		{
			if (profile->failed_login_times != 10)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->failed_login_times = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "password_lock_time") == 0)
		{
			if (profile->passwd_lock_time != 1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_lock_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "password_life_time") == 0)
		{
			if (profile->passwd_life_time != 180)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer || nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_life_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "password_grace_time") == 0)
		{
			if (profile->passwd_grace_time != 7)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_grace_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "password_reuse_time") == 0)
		{
			if (!isinf(profile->passwd_reuse_time))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_reuse_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "password_reuse_max") == 0)
		{
			if (!isinf(profile->passwd_reuse_max))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->passwd_reuse_max = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
		}
		else if (strcmp(defel->name, "password_verify_function") == 0)
		{
			Oid			oid;
			if (profile->passwd_verify_func)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
			if (nodeTag(defel->value) != T_String)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input options %s", defel->name)
						 ));
            if (strcmp(strVal(defel->value), "unlimited") == 0 ||
                    strcmp(strVal(defel->value), "default") == 0 ||
                    strcmp(strVal(defel->value), "null") == 0)
                profile->passwd_verify_func = NULL;
			else
			{
				oid = GetProfileVerifyFunction(strVal(defel->value));
				if (!OidIsValid(oid))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("function \"%s\" does not exist", defel->value->val.str)));

				profile->passwd_verify_func = strVal(defel->value);
			}
		}
		else if (strcmp(defel->name, "container") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unsupported options %s", defel->name)
					 ));

		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->name);
	}

	PgProfileCreate(profile);
}
void
AlterProfile(AlterProfileStmt *stmt)
{
	Relation	rel;
	HeapTuple	tuple,
				newtuple;
	ScanKeyData scankey;
	SysScanDesc scan;
	ListCell   *option;
	Datum		new_record[Natts_pg_profile];
	bool		new_record_nulls[Natts_pg_profile];
	bool		new_record_repl[Natts_pg_profile];
	ProfileResourceLimit *profile = (ProfileResourceLimit *)palloc0(sizeof(ProfileResourceLimit));

	/* Check some permissions first */
	if (!has_createprofile_privilege(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to alter profile")));

 	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/* Extract options from the statement node tree */
	foreach(option, stmt->profile_args)
	{
		ProfileParameter  *defel = (ProfileParameter *) lfirst(option);

		if (strcmp(defel->name, "sessions_per_user") == 0)
		{
            if (profile->sessions)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("conflicting or redundant options %s", defel->name)
                        ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->sessions = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
            new_record[Anum_pg_profile_sessions - 1] = Float4GetDatum(profile->sessions);
			new_record_repl[Anum_pg_profile_sessions - 1] = true;
		}
		else if (strcmp(defel->name, "cpu_per_session") == 0)
		{
			if (profile->cpu_per_session)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->cpu_per_session = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_session_cpu - 1] = Float4GetDatum(profile->cpu_per_session);
			new_record_repl[Anum_pg_profile_session_cpu - 1] = true;
		}
		else if (strcmp(defel->name, "cpu_per_call") == 0)
		{
			if (profile->cpu_per_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->cpu_per_query = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_query_cpu - 1] = Float4GetDatum(profile->cpu_per_query);
			new_record_repl[Anum_pg_profile_query_cpu - 1] = true;
		}
		else if (strcmp(defel->name, "connect_time") == 0)
		{
			if (profile->connection_time)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->connection_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_connect_time - 1] = Float4GetDatum(profile->connection_time);
			new_record_repl[Anum_pg_profile_connect_time - 1] = true;
		}
		else if (strcmp(defel->name, "idle_time") == 0)
		{
			if (profile->idle_time)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->idle_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_idle_time - 1] = Float4GetDatum(profile->idle_time);
			new_record_repl[Anum_pg_profile_idle_time - 1] = true;
		}
		else if (strcmp(defel->name, "logical_reads_per_session") == 0)
		{
			if (profile->blks_per_session)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->blks_per_session = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_session_blks - 1] = Float4GetDatum(profile->blks_per_session);
			new_record_repl[Anum_pg_profile_session_blks - 1] = true;
		}
		else if (strcmp(defel->name, "logical_reads_per_call") == 0)
		{
			if (profile->blks_per_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->blks_per_query = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_query_blks - 1] = Float4GetDatum(profile->blks_per_query);
			new_record_repl[Anum_pg_profile_query_blks - 1] = true;
		}
		else if (strcmp(defel->name, "private_sga") == 0)
		{
			if (profile->private_sga)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->private_sga = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_private_sga - 1] = Float4GetDatum(profile->private_sga);
			new_record_repl[Anum_pg_profile_private_sga - 1] = true;
		}
		else if (strcmp(defel->name, "composite_limit") == 0)
		{
			if (profile->composite_limit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->composite_limit = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_composite_limit - 1] = Float4GetDatum(profile->composite_limit);
			new_record_repl[Anum_pg_profile_composite_limit - 1] = true;
		}
		else if (strcmp(defel->name, "failed_login_attempts") == 0)
		{
			if (profile->failed_login_times)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->failed_login_times = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_failed_log_times - 1] = Float4GetDatum(profile->failed_login_times);
			new_record_repl[Anum_pg_profile_failed_log_times - 1] = true;
		}
		else if (strcmp(defel->name, "password_lock_time") == 0)
		{
			if (profile->passwd_lock_time)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_lock_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_passwd_lock_time - 1] = Float4GetDatum(profile->passwd_lock_time);
			new_record_repl[Anum_pg_profile_passwd_lock_time - 1] = true;
		}
		else if (strcmp(defel->name, "password_life_time") == 0)
		{
			if (profile->passwd_life_time)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_life_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_passwd_life_time - 1] = Float4GetDatum(profile->passwd_life_time);
			new_record_repl[Anum_pg_profile_passwd_life_time - 1] = true;
		}
		else if (strcmp(defel->name, "password_grace_time") == 0)
		{
			if (profile->passwd_grace_time)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_grace_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_passwd_grace_time - 1] = Float4GetDatum(profile->passwd_grace_time);
			new_record_repl[Anum_pg_profile_passwd_grace_time - 1] = true;
		}
		else if (strcmp(defel->name, "password_reuse_time") == 0)
		{
			if (profile->passwd_reuse_time)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String ||
                    nodeTag(defel->value) == T_Integer ||
                    nodeTag(defel->value) == T_Float ||
                    nodeTag(defel->value) == T_OpExpr)
                profile->passwd_reuse_time = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_passwd_reuse_time - 1] = Float4GetDatum(profile->passwd_reuse_time);
			new_record_repl[Anum_pg_profile_passwd_reuse_time - 1] = true;
		}
		else if (strcmp(defel->name, "password_reuse_max") == 0)
		{
			if (profile->passwd_reuse_max)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) == T_String || nodeTag(defel->value) == T_Integer)
                profile->passwd_reuse_max = get_profile_param_value(defel->value, defel->name);
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", defel->name)
                        ));
			new_record[Anum_pg_profile_passwd_reuse_max - 1] = Float4GetDatum(profile->passwd_reuse_max);
			new_record_repl[Anum_pg_profile_passwd_reuse_max - 1] = true;
		}
		else if (strcmp(defel->name, "password_verify_function") == 0)
		{
			Oid			oid;
			if (profile->passwd_verify_func)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));

			if (nodeTag(defel->value) != T_String)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input options %s", defel->name)
						 ));
			}
			if (strcmp(strVal(defel->value), "unlimited") == 0 ||
                    strcmp(strVal(defel->value), "default") == 0 ||
                    strcmp(strVal(defel->value), "null") == 0 )
			{
				new_record_repl[Anum_pg_profile_verify_function - 1] = true;
				new_record_nulls[Anum_pg_profile_verify_function - 1] = true;
				new_record[Anum_pg_profile_verify_function - 1] = 0;
			}
			else
			{
				oid = GetProfileVerifyFunction(strVal(defel->value));
				if (!OidIsValid(oid))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("function \"%s\" does not exist", strVal(defel->value))));

				profile->passwd_verify_func = strVal(defel->value);

				new_record[Anum_pg_profile_verify_function - 1] = CStringGetTextDatum(profile->passwd_verify_func);
				new_record_repl[Anum_pg_profile_verify_function - 1] = true;
			}
		}
		else if (strcmp(defel->name, "container") == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unsupported options %s", defel->name)
					 ));

		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->name);
	}

	rel = heap_open(PgProfileRelationId, RowExclusiveLock);
	ScanKeyInit(&scankey,
				Anum_pg_profile_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));
	scan = systable_beginscan(rel, PgProfileNameIndexId, true,
							  NULL, 1, &scankey);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("profile \"%s\" does not exist", stmt->name)));
	newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), new_record,
								 new_record_nulls, new_record_repl);
	CatalogTupleUpdate(rel, &tuple->t_self, newtuple);

	systable_endscan(scan);

	/* Close pg_profile, but keep lock till commit */
	heap_close(rel, NoLock);
}
void
DropProfile(DropProfileStmt *stmt)
{
	/* Check some permissions first */
	if (!has_createprofile_privilege(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to drop profile")));
	PgProfileRemove(stmt->name, stmt->behavior);
}
void
AlterResourceCost(AlterResourceCostStmt *stmt)
{
	ListCell   *option;
	bool		replace[Natts_pg_profile_cost_weight];
	Datum		values[Natts_pg_profile_cost_weight];
	bool        nulls[Natts_pg_profile_cost_weight];
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	tuple;

	/* Check some permissions first */
	if (!has_createprofile_privilege(GetUserId()))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("permission denied to alter resource cost")));

 	MemSet(values, 0, sizeof(values));
	MemSet(replace, false, sizeof(replace));
	MemSet(nulls, false, sizeof(nulls));

	foreach(option, stmt->resource_args)
	{
		ProfileParameter  *defel = (ProfileParameter *) lfirst(option);

		if (strcmp(defel->name, "cpu_per_session") == 0)
		{
			if (values[Anum_pg_profile_weight_cpu - 1])
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));

            if (nodeTag(defel->value) != T_Integer ||
                    intVal(defel->value) < 0 ||
                    intVal(defel->value) > INT_MAX)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input options %s", defel->name)
						 ));
			}
			values[Anum_pg_profile_weight_cpu - 1] = intVal(defel->value);
			replace[Anum_pg_profile_weight_cpu - 1] = true;
		}
		else if (strcmp(defel->name, "connect_time") == 0)
		{
			if (values[Anum_pg_profile_weight_connect - 1])
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) != T_Integer ||
                    intVal(defel->value) < 0 ||
                    intVal(defel->value) > INT_MAX)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input options %s", defel->name)
						 ));
			}
			values[Anum_pg_profile_weight_connect - 1] = intVal(defel->value);
			replace[Anum_pg_profile_weight_connect - 1] = true;
		}
		else if (strcmp(defel->name, "logical_reads_per_session") == 0)
		{
			if (values[Anum_pg_profile_weight_blks - 1])
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) != T_Integer ||
                    intVal(defel->value) < 0 ||
                    intVal(defel->value) > INT_MAX)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input options %s", defel->name)
						 ));
			}
			values[Anum_pg_profile_weight_blks - 1] = intVal(defel->value);
			replace[Anum_pg_profile_weight_blks - 1] = true;
		}
		else if (strcmp(defel->name, "private_sga") == 0)
		{
			if (values[Anum_pg_profile_weight_sga - 1])
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options %s", defel->name)
						 ));
            if (nodeTag(defel->value) != T_Integer ||
                    intVal(defel->value) < 0 ||
                    intVal(defel->value) > INT_MAX)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input options %s", defel->name)
						 ));
			}
			values[Anum_pg_profile_weight_sga - 1] = intVal(defel->value);
			replace[Anum_pg_profile_weight_sga - 1] = true;
		}
		else
		{
			elog(ERROR, "option \"%s\" not recognized",
						 defel->name);
		}
	}

	rel = heap_open(PgProfileCostWeightRelationId, RowExclusiveLock);

	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 0, NULL);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_profile_cost_weight costForm = (Form_pg_profile_cost_weight) GETSTRUCT(tuple);
		if (replace[Anum_pg_profile_weight_cpu - 1])
			costForm->cpuweight = DatumGetInt32(values[Anum_pg_profile_weight_cpu - 1]);
		if (replace[Anum_pg_profile_weight_connect - 1])
			costForm->connectweight = DatumGetInt32(values[Anum_pg_profile_weight_connect - 1]);
		if (replace[Anum_pg_profile_weight_blks - 1])
			costForm->blksweight = DatumGetInt32(values[Anum_pg_profile_weight_blks - 1]);
		if (replace[Anum_pg_profile_weight_sga - 1])
			costForm->sgaweight = DatumGetInt32(values[Anum_pg_profile_weight_sga - 1]);
		heap_inplace_update(rel, tuple);
	}
	else
	{
		tuple = heap_form_tuple(rel->rd_att, values, nulls);

		CatalogTupleInsert(rel, tuple);
	}
	systable_endscan(scan);
	heap_close(rel, NoLock);
}
Oid
GetProfileIdByName(char *pname)
{
	Relation	rel;
	ScanKeyData skey[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid         profid = InvalidOid;

	rel = heap_open(PgProfileRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_profile_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(pname));

	scan = systable_beginscan(rel, PgProfileNameIndexId, true,
							  NULL, 1, skey);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		profid = HeapTupleGetOid(tuple);
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return profid;
}
void
profile_passwd_comlexity_check(char *pname, char *user, char *password)
{
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData scankey;
	SysScanDesc scan;
	char        *funcname = NULL;
	char        *profname = pname;
	Oid          profid = InvalidOid;

	if (!profname)
	{
		if (user)
		{
			Form_pg_authid authform;

			tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(user));
			if (!HeapTupleIsValid(tuple))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("role \"%s\" does not exist", user)));

			authform = (Form_pg_authid) GETSTRUCT(tuple);
			profid = authform->rolprofile;
			ReleaseSysCache(tuple);

			if (OidIsValid(profid))
			{
				Datum verify_func;
				bool  func_is_null;
				rel = heap_open(PgProfileRelationId, AccessShareLock);
				ScanKeyInit(&scankey,
							ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(profid));
				scan = systable_beginscan(rel, PgProfileObjectIndexId, true,
										  NULL, 1, &scankey);
				tuple = systable_getnext(scan);
				if (!HeapTupleIsValid(tuple))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_DATABASE),
							 errmsg("profile \"%d\" does not exist", profid)));
		        verify_func = heap_getattr(tuple, Anum_pg_profile_verify_function,
					 RelationGetDescr(rel), &func_is_null);
				if (!func_is_null)
				{
					funcname = TextDatumGetCString(verify_func);
				}
				systable_endscan(scan);
				heap_close(rel, AccessShareLock);
			}
		}
	}


	if (profname)
	{
		Datum verify_func;
		bool  func_is_null;
		rel = heap_open(PgProfileRelationId, AccessShareLock);
		ScanKeyInit(&scankey,
					Anum_pg_profile_name,
					BTEqualStrategyNumber, F_NAMEEQ,
					CStringGetDatum(profname));
		scan = systable_beginscan(rel, PgProfileNameIndexId, true,
								  NULL, 1, &scankey);
		tuple = systable_getnext(scan);
		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_DATABASE),
					 errmsg("profile \"%s\" does not exist", profname)));
        verify_func = heap_getattr(tuple, Anum_pg_profile_verify_function,
			 RelationGetDescr(rel), &func_is_null);
		if (!func_is_null)
		{
			funcname = TextDatumGetCString(verify_func);
		}
		systable_endscan(scan);
		heap_close(rel, AccessShareLock);
	}

	if (funcname)
	{
		Oid funcid = GetProfileVerifyFunction(funcname);
		if (!OidIsValid(funcid))
		{
			elog(ERROR, "verify function %s does not exist", funcname);
		}
		/*
		 * opentenbase_ora password verfity function must has three arguments, however we only have username and password.
		 * The old_password was encrypted, so we can't get origin old password. To be compatible with opentenbase_ora, use
		 * "" represent old_password temporarily.
		 */
		OidFunctionCall3(funcid, CStringGetTextDatum(user), CStringGetTextDatum(password), CStringGetTextDatum(""));
	}
}

void
InsertProfileUserPasswd(char *user, char *passwd, bool update)
{
	Relation	rel;
	HeapTuple	htup;
	bool		nulls[Natts_pg_profile_user_passwd];
	Datum		values[Natts_pg_profile_user_passwd];
	bool        replaces[Natts_pg_profile_user_passwd];
	bool        need_insert = true;

	if (update)
	{
		ScanKeyData scankey;
		SysScanDesc scan;
		HeapTuple	tup;
		HeapTuple   new_tup;

		rel = heap_open(PgProfileUserPasswdRelationId, RowExclusiveLock);
		ScanKeyInit(&scankey,
			Anum_pg_profile_user_passwd_name,
			BTEqualStrategyNumber, F_NAMEEQ,
			CStringGetDatum(user));
		scan = systable_beginscan(rel, PgProfileUserIndexId, true,
								  NULL, 1, &scankey);
		tup = systable_getnext(scan);

		while(HeapTupleIsValid(tup))
	    {
			Datum dpassword = 0;
			bool  passwd_is_null = false;
			char  *password = NULL;
	    	bool iscurrent = ((Form_pg_profile_user_password) GETSTRUCT(tup))->iscurrent;
			int32 passwdchangetimes = ((Form_pg_profile_user_password) GETSTRUCT(tup))->passwdchangetimes;
	        MemSet(values, 0, sizeof(values));
	    	MemSet(nulls, false, sizeof(nulls));
	    	MemSet(replaces, false, sizeof(replaces));

			if (iscurrent)
			{
		        replaces[Anum_pg_profile_user_passwd_curruser - 1] = true;
		        values[Anum_pg_profile_user_passwd_curruser - 1] = BoolGetDatum(false);

		        replaces[Anum_pg_profile_user_passwd_discard_time - 1] = true;
		        values[Anum_pg_profile_user_passwd_discard_time - 1] = TimestampGetDatum(GetCurrentTimestamp());
			}

	        replaces[Anum_pg_profile_user_passwd_change_times - 1] = true;
	        values[Anum_pg_profile_user_passwd_change_times - 1] = Int32GetDatum(passwdchangetimes + 1);

	        dpassword = heap_getattr(tup, Anum_pg_profile_user_passwd_password,
					 RelationGetDescr(rel), &passwd_is_null);
			if (!passwd_is_null && passwd)
			{
				password = TextDatumGetCString(dpassword);
				if (strcmp(password, passwd) == 0)
				{
					need_insert = false;
			        replaces[Anum_pg_profile_user_passwd_curruser - 1] = true;
			        values[Anum_pg_profile_user_passwd_curruser - 1] = BoolGetDatum(true);
			        replaces[Anum_pg_profile_user_passwd_change_times - 1] = true;
			        values[Anum_pg_profile_user_passwd_change_times - 1] = Int32GetDatum(0);
			        replaces[Anum_pg_profile_user_passwd_discard_time - 1] = true;
			        values[Anum_pg_profile_user_passwd_discard_time - 1] = TimestampGetDatum(0);
			        replaces[Anum_pg_profile_user_passwd_start_time - 1] = true;
			        values[Anum_pg_profile_user_passwd_start_time - 1] = TimestampGetDatum(GetCurrentTimestamp());
				}
				pfree(password);
			}
			else if (passwd_is_null && !passwd)
			{
				need_insert = false;
		        replaces[Anum_pg_profile_user_passwd_curruser - 1] = true;
		        values[Anum_pg_profile_user_passwd_curruser - 1] = BoolGetDatum(true);
		        replaces[Anum_pg_profile_user_passwd_change_times - 1] = true;
		        values[Anum_pg_profile_user_passwd_change_times - 1] = Int32GetDatum(0);
		        replaces[Anum_pg_profile_user_passwd_discard_time - 1] = true;
		        values[Anum_pg_profile_user_passwd_discard_time - 1] = TimestampGetDatum(0);
		        replaces[Anum_pg_profile_user_passwd_start_time - 1] = true;
		        values[Anum_pg_profile_user_passwd_start_time - 1] = TimestampGetDatum(GetCurrentTimestamp());
			}

	        new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
	        							 nulls, replaces);

	        CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);

	        tup = systable_getnext(scan);
	    }

		systable_endscan(scan);
		heap_close(rel, NoLock);
	}

	if (!need_insert)
	{
		return;
	}

    MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));

	values[Anum_pg_profile_user_passwd_name - 1]            = DirectFunctionCall1(namein, CStringGetDatum(user));
	values[Anum_pg_profile_user_passwd_curruser - 1]        = BoolGetDatum(true);
	values[Anum_pg_profile_user_passwd_failed_times - 1]    = Int32GetDatum(0);
	values[Anum_pg_profile_user_passwd_failed_time - 1]     = TimestampGetDatum(0);
	values[Anum_pg_profile_user_passwd_start_time - 1]      = TimestampGetDatum(GetCurrentTimestamp());
	values[Anum_pg_profile_user_passwd_discard_time - 1]    = TimestampGetDatum(0);
	values[Anum_pg_profile_user_passwd_change_times - 1]    = Int32GetDatum(0);
	if (passwd)
		values[Anum_pg_profile_user_passwd_password - 1]    = CStringGetTextDatum(passwd);
	else
		nulls[Anum_pg_profile_user_passwd_password - 1] = true;

	/* Open the relation for insertion */
	rel = heap_open(PgProfileUserPasswdRelationId, RowExclusiveLock);

	htup = heap_form_tuple(rel->rd_att, values, nulls);

	CatalogTupleInsert(rel, htup);

	heap_close(rel, NoLock);
}

void
GetMyProfile(char *user)
{
	Relation	rel;
	HeapTuple	tuple;
	ScanKeyData scankey;
	SysScanDesc scan;
	Oid          profid = InvalidOid;
	Form_pg_authid authform;
	Form_pg_profile profileform;

	tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(user));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", user)));

	authform = (Form_pg_authid) GETSTRUCT(tuple);
	profid = authform->rolprofile;
	ReleaseSysCache(tuple);

	if (OidIsValid(profid))
	{
		MemoryContext old;
		Datum verify_func = 0;
		bool  func_is_null = false;
		rel = heap_open(PgProfileRelationId, AccessShareLock);
		ScanKeyInit(&scankey,
					ObjectIdAttributeNumber,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(profid));
		scan = systable_beginscan(rel, PgProfileObjectIndexId, true,
								  NULL, 1, &scankey);
		tuple = systable_getnext(scan);
		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("profile \"%d\" does not exist", profid)));
		profileform = ((Form_pg_profile) GETSTRUCT(tuple));
		old = MemoryContextSwitchTo(TopMemoryContext);
		if (!MyProfile)
			MyProfile = (ProfileResourceLimit *)palloc0(sizeof(ProfileResourceLimit));
		MyProfile->name = NameStr(profileform->profname);
		MyProfile->sessions = profileform->profsessions;
		MyProfile->cpu_per_session = profileform->profsessioncpu;
		MyProfile->cpu_per_query = profileform->profquerycpu;
		MyProfile->connection_time = profileform->profcontime;
		MyProfile->idle_time = profileform->profidletime;
		MyProfile->blks_per_session = profileform->profsessionblks;
		MyProfile->blks_per_query = profileform->profqueryblks;
		MyProfile->private_sga = profileform->profprivatesga;
		MyProfile->composite_limit = profileform->profcompositelimit;
		MyProfile->failed_login_times = profileform->proffailedlogtimes;
		MyProfile->passwd_lock_time = profileform->profpasslocktime;
		MyProfile->passwd_life_time = profileform->profpasslifetime;
		MyProfile->passwd_grace_time = profileform->profpassgracetime;
		MyProfile->passwd_reuse_time = profileform->profpassreusetime;
		MyProfile->passwd_reuse_max = profileform->profpassreusemax;
		MyProfile->container = profileform->profcontainer;
        verify_func = heap_getattr(tuple, Anum_pg_profile_verify_function,
					 RelationGetDescr(rel), &func_is_null);
		if (!func_is_null)
		{
			MyProfile->passwd_verify_func = TextDatumGetCString(verify_func);
		}
		MemoryContextSwitchTo(old);
		systable_endscan(scan);
		heap_close(rel, AccessShareLock);
	}
	else if (MyProfile)
	{
		pfree(MyProfile);
		MyProfile = NULL;
	}
}
ProfileUserPasswd *
GetMyProfUserPassword(char *user)
{
	Relation	pg_profile;
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tup;
	ProfileUserPasswd *result = NULL;

	ScanKeyInit(&skey[0],
				Anum_pg_profile_user_passwd_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(user));
	ScanKeyInit(&skey[1],
				Anum_pg_profile_user_passwd_curruser,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));

	pg_profile = heap_open(PgProfileUserPasswdRelationId, AccessShareLock);

	scan = systable_beginscan(pg_profile, PgProfileUserPasswdIndexId, true,
							  NULL, 2, skey);
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		Datum passwd = 0;
		bool  passwd_is_null = false;
		Form_pg_profile_user_password profileuserpasswd = ((Form_pg_profile_user_password) GETSTRUCT(tup));
		result = (ProfileUserPasswd *)palloc0(sizeof(ProfileUserPasswd));
		result->username = user;
		result->iscurrent = true;
		result->failedlogattempts = profileuserpasswd->failedlogattempts;
		result->failedlogtime = profileuserpasswd->failedlogtime;
		result->passwdstarttime = profileuserpasswd->passwdstarttime;
		result->Passwddiscardtime = profileuserpasswd->passwddiscardtime;
		result->passwdchangetimes = profileuserpasswd->passwdchangetimes;
        passwd = heap_getattr(tup, Anum_pg_profile_user_passwd_password,
					 RelationGetDescr(pg_profile), &passwd_is_null);
		if (!passwd_is_null)
		{
			result->password = TextDatumGetCString(passwd);
		}
	}
	systable_endscan(scan);
	heap_close(pg_profile, AccessShareLock);

	return result;
}
void
ProfileAuthentication(int status, char *user)
{
	bool        dirty = false;
	bool		nulls[Natts_pg_profile_user_passwd];
	Datum		values[Natts_pg_profile_user_passwd];
	bool        replaces[Natts_pg_profile_user_passwd];
	ProfileUserPasswd *profuserpasswd = GetMyProfUserPassword(user);
	if (MyProfile && profuserpasswd)
	{
        MemSet(values, 0, sizeof(values));
    	MemSet(nulls, false, sizeof(nulls));
    	MemSet(replaces, false, sizeof(replaces));
		/* account is locked */
		if (MyProfile->failed_login_times && profuserpasswd->failedlogattempts >= MyProfile->failed_login_times)
		{
			TimestampTz time = GetCurrentTimestamp();
			if ((time - profuserpasswd->failedlogtime) / USECS_PER_SEC < MyProfile->passwd_lock_time * 86400)
			{
				ereport(FATAL,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					 errmsg("the account is locked")));
			}
			else
			{
				profuserpasswd->failedlogattempts = 0;
				profuserpasswd->failedlogtime = GetCurrentTimestamp();
				replaces[Anum_pg_profile_user_passwd_failed_times - 1] = true;
				values[Anum_pg_profile_user_passwd_failed_times - 1] = Int32GetDatum(0);
				replaces[Anum_pg_profile_user_passwd_failed_time - 1] = true;
				values[Anum_pg_profile_user_passwd_failed_time - 1] = TimestampGetDatum(profuserpasswd->failedlogtime);
				dirty = true;
			}
		}
		if (status == STATUS_OK)
		{
			if (MyProfile->passwd_life_time)
			{
				TimestampTz time = GetCurrentTimestamp();
				/* check password lift time */
				double life_time = MyProfile->passwd_life_time * 86400;
				double life_plus_grace_time = (MyProfile->passwd_life_time + MyProfile->passwd_grace_time) * 86400;
				double current_time = (time - profuserpasswd->passwdstarttime) / USECS_PER_SEC;
				if (current_time >= life_time && current_time < life_plus_grace_time)
				{
					int32 expire_days = (int32)((life_plus_grace_time - current_time) / 86400);
					ereport(WARNING,
						(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
						 errmsg("the password will expire within %d days, please change the password",
						        expire_days)));
				}
				else if (current_time >= life_plus_grace_time)
				{
					ereport(FATAL,
						(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
						 errmsg("the password has expired, please change the password")));
				}
			}
		}
		else if (status != STATUS_EOF)
		{
			/* failed */
			profuserpasswd->failedlogattempts++;
			profuserpasswd->failedlogtime = GetCurrentTimestamp();
			replaces[Anum_pg_profile_user_passwd_failed_times - 1] = true;
			values[Anum_pg_profile_user_passwd_failed_times - 1] = Int32GetDatum(profuserpasswd->failedlogattempts);
			replaces[Anum_pg_profile_user_passwd_failed_time - 1] = true;
			values[Anum_pg_profile_user_passwd_failed_time - 1] = TimestampGetDatum(profuserpasswd->failedlogtime);
			dirty = true;
		}
	}

	if (dirty)
	{
		Relation	pg_profile;
		ScanKeyData skey[2];
		SysScanDesc scan;
		HeapTuple	tup;
		Form_pg_profile_user_password user_passwd_Form;

		ScanKeyInit(&skey[0],
					Anum_pg_profile_user_passwd_name,
					BTEqualStrategyNumber, F_NAMEEQ,
					CStringGetDatum(user));
		ScanKeyInit(&skey[1],
					Anum_pg_profile_user_passwd_curruser,
					BTEqualStrategyNumber, F_BOOLEQ,
					BoolGetDatum(true));

		pg_profile = heap_open(PgProfileUserPasswdRelationId, RowExclusiveLock);

		scan = systable_beginscan(pg_profile, PgProfileUserPasswdIndexId, true,
								  NULL, 2, skey);
		tup = systable_getnext(scan);
		if (!HeapTupleIsValid(tup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("user \"%s\" does not exist in relation pg_profile_user_password", user)));

		user_passwd_Form = (Form_pg_profile_user_password) GETSTRUCT(tup);
		if (replaces[Anum_pg_profile_user_passwd_failed_times - 1])
			user_passwd_Form->failedlogattempts = DatumGetInt32(values[Anum_pg_profile_user_passwd_failed_times - 1]);
		if (replaces[Anum_pg_profile_user_passwd_failed_time - 1])
			user_passwd_Form->failedlogtime = DatumGetTimestamp(values[Anum_pg_profile_user_passwd_failed_time - 1]);

		heap_inplace_update(pg_profile, tup);

		systable_endscan(scan);
		heap_close(pg_profile, NoLock);
	}

	if (profuserpasswd)
	{
		pfree(profuserpasswd);
	}
}
void
ProfileReusePassword(char *user, char *passwd)
{
    Relation        pg_profile;
    ScanKeyData     skey[2];
    SysScanDesc     scan;
    ScanKeyData     scankey;
    float4          passwd_reuse_time = 0;
    int32           passwd_reuse_max  = 0;
    HeapTuple       tuple             = NULL;
    Oid             profid            = 0;
    Form_pg_profile profileform       = NULL;
    Form_pg_authid  authform          = NULL;

    tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(user));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("role \"%s\" does not exist", user)));

    authform = (Form_pg_authid) GETSTRUCT(tuple);
    profid = authform->rolprofile;
    ReleaseSysCache(tuple);

    if (OidIsValid(profid))
    {
        pg_profile = heap_open(PgProfileRelationId, AccessShareLock);
        ScanKeyInit(&scankey,
                    ObjectIdAttributeNumber,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(profid));
        scan = systable_beginscan(pg_profile, PgProfileObjectIndexId, true,
                                  NULL, 1, &scankey);
        tuple = systable_getnext(scan);
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_DATABASE),
                            errmsg("profile \"%d\" does not exist", profid)));
        profileform = ((Form_pg_profile) GETSTRUCT(tuple));
        passwd_reuse_time = profileform->profpassreusetime;
        passwd_reuse_max = profileform->profpassreusemax;
        systable_endscan(scan);
        heap_close(pg_profile, AccessShareLock);
    }

    if (!passwd_reuse_time || !passwd_reuse_max)
    {
        return;
    }

    ScanKeyInit(&skey[0],
                Anum_pg_profile_user_passwd_name,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(user));
    ScanKeyInit(&skey[1],
                Anum_pg_profile_user_passwd_password,
                BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(passwd));

    pg_profile = heap_open(PgProfileUserPasswdRelationId, AccessShareLock);

    scan = systable_beginscan(pg_profile, PgProfileUserPasswdIndexId, true,
                              NULL, 2, skey);
    tuple = systable_getnext(scan);
    while(HeapTupleIsValid(tuple))
    {
        TimestampTz time = GetCurrentTimestamp();
        Form_pg_profile_user_password profileuserpasswd = ((Form_pg_profile_user_password) GETSTRUCT(tuple));
        double time_expired = (time - profileuserpasswd->passwdstarttime) / USECS_PER_SEC;
        if (time_expired < passwd_reuse_time * 86400 ||
                profileuserpasswd->passwdchangetimes < passwd_reuse_max)
        {
            elog(ERROR, "the password cannot be reused");
        }
        tuple = systable_getnext(scan);
    }
    systable_endscan(scan);
    heap_close(pg_profile, AccessShareLock);
}
void
ProfileAccountUnlock(char *user)
{
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tup;
	HeapTuple   new_tup;
	Relation	rel;
	bool		nulls[Natts_pg_profile_user_passwd];
	Datum		values[Natts_pg_profile_user_passwd];
	bool        replaces[Natts_pg_profile_user_passwd];


    MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));

	ScanKeyInit(&skey[0],
				Anum_pg_profile_user_passwd_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(user));
	ScanKeyInit(&skey[1],
				Anum_pg_profile_user_passwd_curruser,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));

	rel = heap_open(PgProfileUserPasswdRelationId, RowExclusiveLock);

	scan = systable_beginscan(rel, PgProfileUserPasswdIndexId, true,
							  NULL, 2, skey);
	tup = systable_getnext(scan);

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("user \"%s\" does not exist in relation pg_profile_user_password", user)));
	values[Anum_pg_profile_user_passwd_failed_times - 1] = Int32GetDatum(0);
	replaces[Anum_pg_profile_user_passwd_failed_times - 1] = true;
	values[Anum_pg_profile_user_passwd_failed_time - 1] = TimestampGetDatum(GetCurrentTimestamp());
	replaces[Anum_pg_profile_user_passwd_failed_time - 1] = true;
    new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
        							 nulls, replaces);

    CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);

	systable_endscan(scan);
	heap_close(rel, NoLock);
}
void
DropProfileUserPasswd(char *user)
{
	HeapTuple	tuple;
	ScanKeyData scankey;
	SysScanDesc scan;
	Relation	rel;

	rel = heap_open(PgProfileUserPasswdRelationId, RowExclusiveLock);
	ScanKeyInit(&scankey,
		Anum_pg_profile_user_passwd_name,
		BTEqualStrategyNumber, F_NAMEEQ,
		CStringGetDatum(user));
	scan = systable_beginscan(rel, PgProfileUserIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		CatalogTupleDelete(rel, &tuple->t_self);
	}

	systable_endscan(scan);
	heap_close(rel, NoLock);
}
void
RenameProfileUserPasswd(const char *olduser, const char *newuser)
{
	HeapTuple	tup;
	HeapTuple	new_tup;
	ScanKeyData scankey;
	SysScanDesc scan;
	Relation	rel;
	bool		nulls[Natts_pg_profile_user_passwd];
	Datum		values[Natts_pg_profile_user_passwd];
	bool        replaces[Natts_pg_profile_user_passwd];

	rel = heap_open(PgProfileUserPasswdRelationId, RowExclusiveLock);
	ScanKeyInit(&scankey,
		Anum_pg_profile_user_passwd_name,
		BTEqualStrategyNumber, F_NAMEEQ,
		CStringGetDatum(olduser));
	scan = systable_beginscan(rel, PgProfileUserIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
    	bool iscurrent = ((Form_pg_profile_user_password) GETSTRUCT(tup))->iscurrent;

        MemSet(values, 0, sizeof(values));
    	MemSet(nulls, false, sizeof(nulls));
    	MemSet(replaces, false, sizeof(replaces));

		if (iscurrent)
		{
	        replaces[Anum_pg_profile_user_passwd_password - 1] = true;
	        nulls[Anum_pg_profile_user_passwd_password - 1] = true;
		}

        replaces[Anum_pg_profile_user_passwd_name - 1] = true;
        values[Anum_pg_profile_user_passwd_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(newuser));

        new_tup = heap_modify_tuple(tup, RelationGetDescr(rel), values,
        							 nulls, replaces);

        CatalogTupleUpdate(rel, &new_tup->t_self, new_tup);
	}

	systable_endscan(scan);
	heap_close(rel, NoLock);
}

void
GetMyProfCostWeight(void)
{
    Relation	pg_profile;
    SysScanDesc scan;
    HeapTuple	tup;
    MemoryContext old;
    Form_pg_profile_cost_weight profilecostweight;

    if (profcostweight)
        return;

    pg_profile = heap_open(PgProfileCostWeightRelationId, AccessShareLock);
    scan = systable_beginscan(pg_profile, InvalidOid, false,
                              NULL, 0, NULL);

    tup = systable_getnext(scan);
    if (HeapTupleIsValid(tup))
    {
        old = MemoryContextSwitchTo(TopMemoryContext);
        profilecostweight = ((Form_pg_profile_cost_weight) GETSTRUCT(tup));
        profcostweight = (ProfileCostWeight *)palloc0(sizeof(ProfileCostWeight));
        profcostweight->blksweight = profilecostweight->blksweight;
        profcostweight->connectweight = profilecostweight->connectweight;
        profcostweight->cpuweight = profilecostweight->cpuweight;
        profcostweight->sgaweight = profilecostweight->sgaweight;
        MemoryContextSwitchTo(old);
    }
    else
    {
        /* set default resource cost weight */
        old = MemoryContextSwitchTo(TopMemoryContext);
        profcostweight = (ProfileCostWeight *)palloc0(sizeof(ProfileCostWeight));
        profcostweight->blksweight = 1;
        profcostweight->connectweight = 1;
        profcostweight->cpuweight = 1;
        profcostweight->sgaweight = 1;
        MemoryContextSwitchTo(old);
    }
    systable_endscan(scan);
    heap_close(pg_profile, AccessShareLock);
}

static void
InitProfileSessionInfo()
{
    MyProfSessionInfo->cpu_now = 0;
    MyProfSessionInfo->blks_now = 0;
    MyProfSessionInfo->private_sga_now = 0;

    MyProfSessionInfo->idle_time_now = 0;
    MyProfSessionInfo->conn_time_now = 0;
}

void
ProfileSessionResourceLimitAuthDN(int64 query_cpu, int64 query_blks, int64 shared_blks)
{
	int64	composite;
	MemoryContext	old;

	if (!MyProfile)
		return;

	old = MemoryContextSwitchTo(TopMemoryContext);
	if (MyProfSessionInfo == NULL)
	{
		MyProfSessionInfo = (ProfileSessionInfo *)palloc0(sizeof(ProfileSessionInfo));
		InitProfileSessionInfo();
	}

	Assert(MyProfSessionInfo && MyProfile);

	MyProfSessionInfo->cpu_now += query_cpu;
	MyProfSessionInfo->blks_now += query_blks;
	MyProfSessionInfo->private_sga_now += shared_blks;
	MemoryContextSwitchTo(old);

	/* for each query */
    if (MyProfile->cpu_per_query != 0 && query_cpu > MyProfile->cpu_per_query)
        ereport(ERROR,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("exceeded call limit on CPU usage")));

    if (MyProfile->blks_per_query != 0 && query_blks > MyProfile->blks_per_query)
        ereport(ERROR,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("exceeded call limit on IO usage")));

    /* for each session */
    if (MyProfile->cpu_per_session != 0 && MyProfSessionInfo->cpu_now > MyProfile->cpu_per_session)
    {
        InitProfileSessionInfo();
        ereport(ERROR,
                (errcode(SESSION_PROFILE_LIMIT_EXCEEDED),
                        errmsg("exceeded session limit on CPU usage, you are being logged off")));
    }

    if (MyProfile->blks_per_session != 0 && MyProfSessionInfo->blks_now > MyProfile->blks_per_session)
    {
        InitProfileSessionInfo();
        ereport(ERROR,
                (errcode(SESSION_PROFILE_LIMIT_EXCEEDED),
                        errmsg("exceeded session limit on IO usage, you are being logged off")));
    }

    if (MyProfile->private_sga != 0 && MyProfSessionInfo->private_sga_now > MyProfile->private_sga)
    {
        InitProfileSessionInfo();
        ereport(ERROR,
                (errcode(SESSION_PROFILE_LIMIT_EXCEEDED),
                        errmsg("exceeded private_sga, you are being logged off")));
    }

    if(profcostweight)
    {
        composite = profcostweight->cpuweight * MyProfSessionInfo->cpu_now +
                profcostweight->blksweight * MyProfSessionInfo->blks_now +
                profcostweight->sgaweight * MyProfSessionInfo->private_sga_now +
                profcostweight->connectweight * MyProfSessionInfo->conn_time_now;

        if(MyProfile->composite_limit != 0 && composite > MyProfile->composite_limit)
        {
            InitProfileSessionInfo();
            ereport(ERROR,
                    (errcode(SESSION_PROFILE_LIMIT_EXCEEDED),
                            errmsg("exceeded COMPOSITE_LIMIT, you are being logged off")));
        }
    }

}

void
ProfileSessionResourceLimitAuthCN(int64 new_conn_time, int64 last_idle_time)
{
    MemoryContext old;

    old = MemoryContextSwitchTo(TopMemoryContext);
    if(MyProfSessionInfo == NULL)
    {
        MyProfSessionInfo = (ProfileSessionInfo *)palloc0(sizeof(ProfileSessionInfo));
        InitProfileSessionInfo();
    }

    Assert(MyProfSessionInfo && MyProfile);

    MyProfSessionInfo->conn_time_now = new_conn_time;
    MyProfSessionInfo->idle_time_now += last_idle_time;
    MemoryContextSwitchTo(old);

    /* for each session */
    if (MyProfile->connection_time != 0 && MyProfSessionInfo->conn_time_now > MyProfile->connection_time)
        ereport(FATAL,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("exceeded maximum connect time, you are being logged off")));

    if (MyProfile->idle_time != 0 && MyProfSessionInfo->idle_time_now > MyProfile->idle_time)
        ereport(FATAL,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                        errmsg("exceeded maximum idle time, please connect again")));
}

void reset_authid(char *pname, Oid profile, DropBehavior behavior)
{
    SysScanDesc scan;
    Relation    rel;
    HeapTuple   htuple;
    HeapTuple   newtuple;
    Datum		new_record[Natts_pg_profile];
    bool		new_record_nulls[Natts_pg_profile];
    bool		new_record_repl[Natts_pg_profile];

    MemSet(new_record, 0, sizeof(new_record));
    MemSet(new_record_nulls, false, sizeof(new_record_nulls));
    MemSet(new_record_repl, false, sizeof(new_record_repl));

    rel = heap_open(AuthIdRelationId, AccessShareLock);
    scan = systable_beginscan(rel, InvalidOid, false,
                              NULL, 0, NULL);

    while (HeapTupleIsValid(htuple = systable_getnext(scan)))
    {
        Form_pg_authid pg_authid_form = ((Form_pg_authid) GETSTRUCT(htuple));
        if (OidIsValid(pg_authid_form->rolprofile) && (profile == pg_authid_form->rolprofile))
        {
            if (behavior == DROP_CASCADE)
            {
                new_record[Anum_pg_authid_rolprofile - 1] = 0;
                new_record_repl[Anum_pg_authid_rolprofile - 1] = true;
                newtuple = heap_modify_tuple(htuple, RelationGetDescr(rel), new_record,
                                             new_record_nulls, new_record_repl);
                CatalogTupleUpdate(rel, &htuple->t_self, newtuple);
            }
            else
                ereport(ERROR,
                        (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                                errmsg("cannot drop profile \"%s\" because it was assigned to some role",
                                       pname),
                                errhint("Use DROP ... CASCADE to reset the roles too.")));
        }
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);
}

bool parse_unit(const char *value, int *result)
{
#define MAX_UNIT_LEN        1
    int32 val;
    char  *endptr;

    /* To suppress compiler warnings, always set output params */
    if (result)
        *result  = 0;

    errno = 0;
    val = strtol(value, &endptr, 0);

    if (endptr == value)
        return false;            /* no HINT for integer syntax error */

    if (errno == ERANGE || val != (int64)((int32)val))
        return false;

    /* allow whitespace between integer and unit */
    while (isspace((unsigned char)*endptr))
        endptr++;

    /* Handle possible unit */
    if (*endptr != '\0')
    {
        char unit[MAX_UNIT_LEN + 1];//length of longest recognized unit string
        int  unitlen;

        unitlen = 0;
        while (*endptr != '\0' && !isspace((unsigned char)*endptr) &&
                unitlen < MAX_UNIT_LEN)
            unit[unitlen++] = *(endptr++);
        unit[unitlen]       = '\0';
        /* allow whitespace after unit */
        while (isspace((unsigned char)*endptr))
            endptr++;

        if (*endptr == '\0')
        {
            switch (*unit)
            {
                case 'k':
                    val = val * 1024;
                    break;
                case 'm':
                    val = val * 1024 * 1024;
                    break;
                case 'g':
                    val = val * 1024 * 1024 * 1024;
                    break;
                default:
                    elog(ERROR, "unrecognized unit: %s", unit);
            }
        }
        else
            return false;
    }
    if (result)
        *result = (int)val;
    return true;
}

float4 get_profile_param_value(Value *param_value, char* param_name)
{
    float4 ret = (float4) 0;
    Node *result = NULL;
    int unit_result = 0;

    switch (nodeTag(param_value))
    {
        case T_Integer:
            if (intVal(param_value) < 0 || intVal(param_value) > INT_MAX)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", param_name)
                        ));
            ret = intVal(param_value);
            break;
        case T_Float:
            if (floatVal(param_value) < 0 || floatVal(param_value) > INT_MAX)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", param_name)
                        ));
            ret = floatVal(param_value);
            break;
        case T_String:
            if (strcmp(strVal(param_value), "unlimited") == 0)
                ret = INFINITY;
            else if (strcmp(strVal(param_value), "default") == 0)
            {
                if (strcmp(param_name, "failed_login_attempts") == 0)
                    ret = 10;
                else if (strcmp(param_name, "password_lock_time") == 0)
                    ret = 1;
                else if (strcmp(param_name, "password_life_time") == 0)
                    ret = 180;
                else if (strcmp(param_name, "password_grace_time") == 0)
                    ret = 7;
                else
                    ret = INFINITY;
            }
            else if (strcmp(param_name, "private_sga") == 0 && parse_unit(strVal(param_value), &unit_result))
                ret = unit_result;
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", param_name)
                        ));
            break;
        case T_OpExpr:
            result = eval_const_expressions(NULL, (Node *)param_value);
            if (result && IsA(result, Const))
            {
                float4 val = DatumGetFloat4(
                        DirectFunctionCall1(numeric_float4, ((Const *)result)->constvalue));

                if (val < 0 || val > INT_MAX)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("invalid input options %s", param_name)
                            ));
                ret = val;
            }
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("invalid input options %s", param_name)
                        ));
            break;
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("invalid input options %s", param_name)
                    ));
    }
    return ret;
}
