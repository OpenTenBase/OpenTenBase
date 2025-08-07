/*-------------------------------------------------------------------------
 *
 * user.c
 *	  Commands for manipulating roles (formerly called users).
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/commands/user.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_auth_history.h"
#include "catalog/pg_dbms_job.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/seclabel.h"
#include "commands/user.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#ifdef _MLS_
#include "utils/mls.h"
#include "utils/datamask.h"
#endif
#ifdef __OPENTENBASE__
#include "pgstat.h"
#include "pgxc/poolutils.h"
#include "pgxc/poolmgr.h"
#endif
#ifdef _PG_ORCL_
#include "catalog/pg_profile.h"
#endif
#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif
#include "catalog/pg_resgroup.h"
#include "utils/resgroup.h"
#include "commands/resgroupcmds.h"

#define CREATE_PG_AUTH_ROLE 1
#define ALTER_PG_AUTH_ROLE 2

/* Potentially set by pg_upgrade_support functions */
Oid			binary_upgrade_next_pg_authid_oid = InvalidOid;


/* GUC parameter */
int			Password_encryption = PASSWORD_TYPE_MD5;
#ifdef _MLS_
extern bool g_CheckPassword;
#endif
/* Hook to check passwords in CreateRole() and AlterRole() */
check_password_hook_type check_password_hook = NULL;

static void AddRoleMems(const char *rolename, Oid roleid,
			List *memberSpecs, List *memberIds,
			Oid grantorId, bool admin_opt);
static void DelRoleMems(const char *rolename, Oid roleid,
			List *memberSpecs, List *memberIds,
			bool admin_opt);
#ifdef _MLS_
static void mls_passwd_comlexity_enforcement(const char *username, const char *password, int password_type,
			   Datum validuntil_time, bool validuntil_null);
#endif
#ifdef __OPENTENBASE_C__
static void AddAuthHistory(Oid roleID, const char* rolename, const char* shadow_pass, int operatType);
static void DropAuthHistory(Oid roleID);
#endif

/* Check if current user has createrole privileges */
static bool
have_createrole_privilege(void)
{
	return has_createrole_privilege(GetUserId());
}


/*
 * CREATE ROLE
 */
Oid
CreateRole(ParseState *pstate, CreateRoleStmt *stmt)
{
	Relation	pg_authid_rel;
	TupleDesc	pg_authid_dsc;
	HeapTuple	tuple;
	Datum		new_record[Natts_pg_authid];
	bool		new_record_nulls[Natts_pg_authid];
	Oid			roleid;
	ListCell   *item;
	ListCell   *option;
	char	   *password = NULL;	/* user password */
	bool		issuper = false;	/* Make the user a superuser? */
	bool		inherit = true; /* Auto inherit privileges? */
	bool		createrole = false; /* Can this user create roles? */
	bool		createdb = false;	/* Can the user create databases? */
	bool		canlogin = false;	/* Can this user login? */
	bool		isreplication = false;	/* Is this a replication role? */
	bool		bypassrls = false;	/* Is this a row security enabled role? */
	int			connlimit = -1; /* maximum connections allowed */
	List	   *addroleto = NIL;	/* roles to make this a member of */
	List	   *rolemembers = NIL;	/* roles to be members of this role */
	List	   *adminmembers = NIL; /* roles to be admins of this role */
	char	   *validUntil = NULL;	/* time the login is valid until */
	Datum		validUntil_datum;	/* same, as timestamptz Datum */
	bool		validUntil_null;
	char	   *resgroup = NULL;		/* resource group for this role */
	DefElem    *dpassword = NULL;
	DefElem    *dresgroup = NULL;
	DefElem    *dissuper = NULL;
	DefElem    *dinherit = NULL;
	DefElem    *dcreaterole = NULL;
	DefElem    *dcreatedb = NULL;
	DefElem    *dcanlogin = NULL;
	DefElem    *disreplication = NULL;
	DefElem    *dconnlimit = NULL;
	DefElem    *daddroleto = NULL;
	DefElem    *drolemembers = NULL;
	DefElem    *dadminmembers = NULL;
	DefElem    *dvalidUntil = NULL;
	DefElem    *dbypassRLS = NULL;
#ifdef _PG_ORCL_
	bool       createprofile = false;
	DefElem    *dcreateprofile = NULL;
	DefElem    *profile = NULL;
	char       *profname = NULL;
	Oid        profid = InvalidOid;
#endif
#ifdef __RESOURCE_QUEUE__
	char	   *resqueue = NULL;		/* resource queue for this role */
	DefElem    *dresqueue = NULL;
#endif
	char	   *priority = NULL;
	DefElem    *dpriority = NULL;

	/* The defaults can vary depending on the original statement type */
	switch (stmt->stmt_type)
	{
		case ROLESTMT_ROLE:
			break;
		case ROLESTMT_USER:
			canlogin = true;
			/* may eventually want inherit to default to false here */
			break;
		case ROLESTMT_GROUP:
			break;
	}

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "password") == 0)
		{
			if (dpassword)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dpassword = defel;
		}
		else if (strcmp(defel->defname, "sysid") == 0)
		{
			ereport(NOTICE,
					(errmsg("SYSID can no longer be specified")));
		}
		else if (strcmp(defel->defname, "superuser") == 0)
		{
			if (dissuper)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dissuper = defel;
		}
		else if (strcmp(defel->defname, "inherit") == 0)
		{
			if (dinherit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dinherit = defel;
		}
		else if (strcmp(defel->defname, "createrole") == 0)
		{
			if (dcreaterole)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dcreaterole = defel;
		}
		else if (strcmp(defel->defname, "createdb") == 0)
		{
			if (dcreatedb)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dcreatedb = defel;
		}
		else if (strcmp(defel->defname, "canlogin") == 0)
		{
			if (dcanlogin)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dcanlogin = defel;
		}
		else if (strcmp(defel->defname, "isreplication") == 0)
		{
			if (disreplication)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			disreplication = defel;
		}
		else if (strcmp(defel->defname, "connectionlimit") == 0)
		{
			if (dconnlimit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dconnlimit = defel;
		}
		else if (strcmp(defel->defname, "addroleto") == 0)
		{
			if (daddroleto)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			daddroleto = defel;
		}
		else if (strcmp(defel->defname, "rolemembers") == 0)
		{
			if (drolemembers)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			drolemembers = defel;
		}
		else if (strcmp(defel->defname, "adminmembers") == 0)
		{
			if (dadminmembers)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dadminmembers = defel;
		}
		else if (strcmp(defel->defname, "validUntil") == 0)
		{
			if (dvalidUntil)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dvalidUntil = defel;
		}
		else if (strcmp(defel->defname, "resourceGroup") == 0)
		{
			if (dresgroup)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dresgroup = defel;
		}
		else if (strcmp(defel->defname, "bypassrls") == 0)
		{
			if (dbypassRLS)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			dbypassRLS = defel;
		}
#ifdef _PG_ORCL_
		else if (strcmp(defel->defname, "profile") == 0)
		{
				if (profile)
						ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("conflicting or redundant options")));
				profile = defel;
		}
		else if (strcmp(defel->defname, "createprofile") == 0)
		{
				if (dcreateprofile)
						ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("conflicting or redundant options")));
				dcreateprofile = defel;
		}
#endif
#ifdef __RESOURCE_QUEUE__
		else if (strcmp(defel->defname, "resourceQueue") == 0)
		{
			if (dresqueue)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dresqueue = defel;
		}
#endif
		else if (strcmp(defel->defname, "priority") == 0)
		{
			if (dpriority)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("conflicting or redundant options")));
			dpriority = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (dpassword && dpassword->arg)
		password = strVal(dpassword->arg);
	if (dissuper)
		issuper = intVal(dissuper->arg) != 0;
	if (dinherit)
		inherit = intVal(dinherit->arg) != 0;
	if (dcreaterole)
		createrole = intVal(dcreaterole->arg) != 0;
	if (dcreatedb)
		createdb = intVal(dcreatedb->arg) != 0;
	if (dcanlogin)
		canlogin = intVal(dcanlogin->arg) != 0;
	if (disreplication)
		isreplication = intVal(disreplication->arg) != 0;
	if (dconnlimit)
	{
		connlimit = intVal(dconnlimit->arg);
		if (connlimit < -1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid connection limit: %d", connlimit)));
	}
	if (daddroleto)
		addroleto = (List *) daddroleto->arg;
	if (drolemembers)
		rolemembers = (List *) drolemembers->arg;
	if (dadminmembers)
		adminmembers = (List *) dadminmembers->arg;
	if (dvalidUntil)
		validUntil = strVal(dvalidUntil->arg);
	if (dresgroup)
		resgroup = strVal(linitial((List *) dresgroup->arg));
	if (dbypassRLS)
		bypassrls = intVal(dbypassRLS->arg) != 0;
#ifdef _PG_ORCL_
	if (profile)
	{
		profname = strVal(profile->arg);
		if (profname)
		{
			if (strcmp(profname, "unlimited") == 0)
			{
				profid = InvalidOid;
			}
			else
			{
				profid = GetProfileIdByName(profname);
				if (!OidIsValid(profid))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("profile \"%s\" does not exist", profname)));
				}
			}
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("no profile provided")));
		}
	}
	if (dcreateprofile)
	{
		createprofile = intVal(dcreateprofile->arg);
	}
#endif

	/* Check some permissions first */
	if (issuper)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to create superusers")));
	}
	else if (isreplication)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to create replication users")));
	}
	else if (bypassrls)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to change bypassrls attribute")));
	}
	else
	{
		if (!have_createrole_privilege())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to create role")));
	}

#ifdef __RESOURCE_QUEUE__
	if (dresqueue)
		resqueue = strVal(dresqueue->arg);
#endif
	if (dpriority)
	{
		char *str = strVal(dpriority->arg);
		if (strcmp(str, "high") == 0)
			priority = str;
		else if (strcmp(str, "medium") == 0)
			priority = str;
		else if (strcmp(str, "low") == 0)
			priority = str;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid priority value, choose one from \"high\", \"medium\" and \"low\"(default)")));
	}
	/*
	 * Check that the user is not trying to create a role in the reserved
	 * "pg_" namespace.
	 */
	if (IsReservedName(stmt->role) && !IsInplaceUpgrade)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("role name \"%s\" is reserved",
						stmt->role),
				 errdetail("Role names starting with \"pg_\" are reserved.")));

	/*
	 * Check the pg_authid relation to be certain the role doesn't already
	 * exist.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	pg_authid_dsc = RelationGetDescr(pg_authid_rel);

	if (OidIsValid(get_role_oid(stmt->role, true)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("role \"%s\" already exists",
						stmt->role)));

	/* Convert validuntil to internal form */
	if (validUntil)
	{
		validUntil_datum = DirectFunctionCall3(timestamptz_in,
											   CStringGetDatum(validUntil),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
		validUntil_null = false;
	}
	else
	{
		validUntil_datum = (Datum) 0;
		validUntil_null = true;
	}
    
#ifdef _MLS_
    if (g_CheckPassword && password)
    {
        mls_passwd_comlexity_enforcement(stmt->role,
                        password,
                        get_password_type(password),
                        validUntil_datum,
                        validUntil_null);
    }
#endif

	/*
	 * Call the password checking hook if there is one defined
	 */
	if (check_password_hook && password)
		(*check_password_hook) (stmt->role,
								password,
								get_password_type(password),
								validUntil_datum,
								validUntil_null);

	/*
	 * Build a tuple to insert
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));

	new_record[Anum_pg_authid_rolname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->role));

	new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper);
	new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit);
	new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole);
	new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb);
	new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin);
	new_record[Anum_pg_authid_rolreplication - 1] = BoolGetDatum(isreplication);
	new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);
#ifdef _PG_ORCL_
	new_record[Anum_pg_authid_rolcreateprof - 1] = BoolGetDatum(createprofile);
	new_record[Anum_pg_authid_rolprofile - 1] = ObjectIdGetDatum(profid);
#endif
	if (password)
	{
		char	   *shadow_pass;
		char	   *logdetail;

		/*
		 * Don't allow an empty password. Libpq treats an empty password the
		 * same as no password at all, and won't even try to authenticate. But
		 * other clients might, so allowing it would be confusing. By clearing
		 * the password when an empty string is specified, the account is
		 * consistently locked for all clients.
		 *
		 * Note that this only covers passwords stored in the database itself.
		 * There are also checks in the authentication code, to forbid an
		 * empty password from being used with authentication methods that
		 * fetch the password from an external system, like LDAP or PAM.
		 */
		if (password[0] == '\0' ||
			plain_crypt_verify(stmt->role, password, "", &logdetail) == STATUS_OK)
		{
			ereport(NOTICE,
					(errmsg("empty string is not a valid password, clearing password")));
			new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
		}
		else
		{
			/* Encrypt the password to the requested format. */
			shadow_pass = encrypt_password(Password_encryption, stmt->role,
										   password);

			new_record[Anum_pg_authid_rolpassword - 1] =
				CStringGetTextDatum(shadow_pass);
#ifdef _PG_ORCL_
			if (profile_password_limit && IS_LOCAL_ACCESS_NODE)
			{
				if (profname && OidIsValid(profid))
				{
					profile_passwd_comlexity_check(profname, stmt->role, password);
				}
			}
#endif
		}
	}
	else
		new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
#ifdef _PG_ORCL_
	if (new_record_nulls[Anum_pg_authid_rolpassword - 1] == false)
	{
		char *passwd = TextDatumGetCString(new_record[Anum_pg_authid_rolpassword - 1]);
		InsertProfileUserPasswd(stmt->role, passwd, false);
	}
	else
	{
		InsertProfileUserPasswd(stmt->role, NULL, false);
	}
#endif
	new_record[Anum_pg_authid_rolvaliduntil - 1] = validUntil_datum;
	new_record_nulls[Anum_pg_authid_rolvaliduntil - 1] = validUntil_null;
	new_record[Anum_pg_authid_rolbypassrls - 1] = BoolGetDatum(bypassrls);

#ifdef __RESOURCE_QUEUE__
	if (resqueue)
	{
		if (pg_strcasecmp(resqueue, "none") == 0 ||
			pg_strcasecmp(resqueue, DEFAULT_RESQUEUE_NAME) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_RESERVED_NAME),
					 errmsg("resource queue name \"%s\" is reserved",
							resqueue)));

		/* check if queue exists */
		if (!IsResQueueExists(resqueue))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("resource queue \"%s\" does not exist",
							resqueue)));

		new_record[Anum_pg_authid_rolresqueue - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(resqueue));
	}
	else
	{
		/*
		 * Resource queue required -- use default queue
		 * Don't complain if you CREATE a superuser, who doesn't use the queue
		 */
		new_record[Anum_pg_authid_rolresqueue - 1] = 
			DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_RESQUEUE_NAME));
	}
#endif

	if (resgroup)
	{
		Oid			rsgid;

		rsgid = get_resgroup_oid(resgroup, false);
		if (rsgid == ADMINRESGROUP_OID && !issuper)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("only superuser can be assigned to admin resgroup")));

		new_record[Anum_pg_authid_rolresgroup - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(resgroup));
		if (!IsResGroupActivated() && IS_LOCAL_ACCESS_NODE)
			ereport(WARNING,
					(errmsg("resource group is disabled"),
					 errhint("To enable set enable_resource_group=true")));
	}
	else if (issuper)
	{
		if (IsResGroupActivated() && IS_LOCAL_ACCESS_NODE)
		{
			ereport(NOTICE,
					(errmsg("resource group not specified, using admin resource group \"admin_group\"")));
		}

		new_record[Anum_pg_authid_rolresgroup - 1] =
			DirectFunctionCall1(namein, CStringGetDatum("admin_group"));
	}
	else
	{
		if (IsResGroupActivated() && IS_LOCAL_ACCESS_NODE)
		{
			ereport(NOTICE,
					(errmsg("resource group not specified, using default resource group \"default_group\"")));
		}

		new_record[Anum_pg_authid_rolresgroup - 1] =
			DirectFunctionCall1(namein, CStringGetDatum("default_group"));
	}

	if (priority)
	{
		new_record[Anum_pg_authid_rolpriority - 1] = CStringGetTextDatum(priority);
	}
	else if (issuper)
	{
		new_record[Anum_pg_authid_rolpriority - 1] = CStringGetTextDatum("high");
	}
	else
	{
		new_record[Anum_pg_authid_rolpriority - 1] = CStringGetTextDatum("low");
	}

	tuple = heap_form_tuple(pg_authid_dsc, new_record, new_record_nulls);

	/*
	 * pg_largeobject_metadata contains pg_authid.oid's, so we use the
	 * binary-upgrade override.
	 */
	if (IsBinaryUpgrade)
	{
		if (!OidIsValid(binary_upgrade_next_pg_authid_oid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_authid OID value not set when in binary upgrade mode")));

		HeapTupleSetOid(tuple, binary_upgrade_next_pg_authid_oid);
		binary_upgrade_next_pg_authid_oid = InvalidOid;
	}

	/*
	 * Insert new record in the pg_authid table
	 */
	roleid = CatalogTupleInsert(pg_authid_rel, tuple);

	/* whether the create role satisfied the reuse conditions */
	if (g_CheckPassword && password != NULL)
		AddAuthHistory(roleid, stmt->role, password, CREATE_PG_AUTH_ROLE);

	/*
	 * Advance command counter so we can see new record; else tests in
	 * AddRoleMems may fail.
	 */
	if (addroleto || adminmembers || rolemembers)
		CommandCounterIncrement();

	/*
	 * Add the new role to the specified existing roles.
	 */
	foreach(item, addroleto)
	{
		RoleSpec   *oldrole = lfirst(item);
		HeapTuple	oldroletup = get_rolespec_tuple(oldrole);
		Oid			oldroleid = HeapTupleGetOid(oldroletup);
		char	   *oldrolename = NameStr(((Form_pg_authid) GETSTRUCT(oldroletup))->rolname);

		AddRoleMems(oldrolename, oldroleid,
					list_make1(makeString(stmt->role)),
					list_make1_oid(roleid),
					GetUserId(), false);

		ReleaseSysCache(oldroletup);
	}

	/*
	 * Add the specified members to this new role. adminmembers get the admin
	 * option, rolemembers don't.
	 */
	AddRoleMems(stmt->role, roleid,
				adminmembers, roleSpecsToIds(adminmembers),
				GetUserId(), true);
	AddRoleMems(stmt->role, roleid,
				rolemembers, roleSpecsToIds(rolemembers),
				GetUserId(), false);

	/* Post creation hook for new role */
	InvokeObjectPostCreateHook(AuthIdRelationId, roleid, 0);

	/*
	 * Close pg_authid, but keep lock till commit.
	 */
	heap_close(pg_authid_rel, NoLock);

	return roleid;
}


/*
 * ALTER ROLE
 *
 * Note: the rolemembers option accepted here is intended to support the
 * backwards-compatible ALTER GROUP syntax.  Although it will work to say
 * "ALTER ROLE role ROLE rolenames", we don't document it.
 */
Oid
AlterRole(AlterRoleStmt *stmt)
{
	Datum		new_record[Natts_pg_authid];
	bool		new_record_nulls[Natts_pg_authid];
	bool		new_record_repl[Natts_pg_authid];
	Relation	pg_authid_rel;
	TupleDesc	pg_authid_dsc;
	HeapTuple	tuple,
				new_tuple;
	Form_pg_authid authform;
	ListCell   *option;
	char	   *rolename = NULL;
	char	   *password = NULL;	/* user password */
	int			issuper = -1;	/* Make the user a superuser? */
	int			inherit = -1;	/* Auto inherit privileges? */
	int			createrole = -1;	/* Can this user create roles? */
	int			createdb = -1;	/* Can the user create databases? */
	int			canlogin = -1;	/* Can this user login? */
	int			isreplication = -1; /* Is this a replication role? */
	int			connlimit = -1; /* maximum connections allowed */
	List	   *rolemembers = NIL;	/* roles to be added/removed */
	char	   *validUntil = NULL;	/* time the login is valid until */
	Datum		validUntil_datum;	/* same, as timestamptz Datum */
	bool		validUntil_null;
	int			bypassrls = -1;
	int			accountlock = -1;
	DefElem    *dpassword = NULL;
	DefElem    *dissuper = NULL;
	DefElem    *dinherit = NULL;
	DefElem    *dcreaterole = NULL;
	DefElem    *dcreatedb = NULL;
	DefElem    *dcanlogin = NULL;
	DefElem    *disreplication = NULL;
	DefElem    *dconnlimit = NULL;
	DefElem    *drolemembers = NULL;
	DefElem    *dvalidUntil = NULL;
	DefElem    *dbypassRLS = NULL;
	DefElem    *daccountlock = NULL;
	bool		bWas_super = false;	/* Was the user a superuser? */
#ifdef _PG_ORCL_
    DefElem    *dcreateprofile = NULL;
    int         createprofile = -1;
    DefElem    *profile = NULL;
    char       *profname = NULL;
    Oid        profid = InvalidOid;
    bool       unlock_account = false;
#endif
	Oid			roleid;

#ifdef __RESOURCE_QUEUE__
	char	   *resqueue = NULL;		/* resource queue for this role */
	DefElem    *dresqueue = NULL;
#endif
	char	   *resgroup = NULL;		/* resource queue for this role */
	DefElem    *dresgroup = NULL;
	char	   *priority = NULL;		/* priority for this role */
	DefElem    *dpriority = NULL;

	check_rolespec_name(stmt->role,
						"Cannot alter reserved roles.");

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "password") == 0)
		{
			if (dpassword)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dpassword = defel;
		}
		else if (strcmp(defel->defname, "superuser") == 0)
		{
			if (dissuper)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dissuper = defel;
		}
		else if (strcmp(defel->defname, "inherit") == 0)
		{
			if (dinherit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dinherit = defel;
		}
		else if (strcmp(defel->defname, "createrole") == 0)
		{
			if (dcreaterole)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dcreaterole = defel;
		}
		else if (strcmp(defel->defname, "createdb") == 0)
		{
			if (dcreatedb)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dcreatedb = defel;
		}
		else if (strcmp(defel->defname, "canlogin") == 0)
		{
			if (dcanlogin)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dcanlogin = defel;
		}
		else if (strcmp(defel->defname, "isreplication") == 0)
		{
			if (disreplication)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			disreplication = defel;
		}
		else if (strcmp(defel->defname, "connectionlimit") == 0)
		{
			if (dconnlimit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dconnlimit = defel;
		}
		else if (strcmp(defel->defname, "rolemembers") == 0 &&
				 stmt->action != 0)
		{
			if (drolemembers)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			drolemembers = defel;
		}
		else if (strcmp(defel->defname, "validUntil") == 0)
		{
			if (dvalidUntil)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dvalidUntil = defel;
		}
		else if (strcmp(defel->defname, "bypassrls") == 0)
		{
			if (dbypassRLS)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dbypassRLS = defel;
		}
		else if (strcmp(defel->defname, "accountlock") == 0)
		{
			if (daccountlock)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			daccountlock = defel;
		}
#ifdef _PG_ORCL_
		else if (strcmp(defel->defname, "profile") == 0)
		{
				if (profile)
						ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("conflicting or redundant options")));
				profile = defel;
		}
		else if (strcmp(defel->defname, "createprofile") == 0)
		{
				if (dcreateprofile)
						ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("conflicting or redundant options")));
				dcreateprofile = defel;
		}
		else if (strcmp(defel->defname, "unlock") == 0)
		{
				if (unlock_account)
						ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										errmsg("conflicting or redundant options")));
				unlock_account = true;
		}
#endif	
#ifdef __RESOURCE_QUEUE__
		else if (strcmp(defel->defname, "resourceQueue") == 0)
		{
			if (dresqueue)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dresqueue = defel;
		}
#endif
		else if (strcmp(defel->defname, "resourceGroup") == 0)
		{
			if (dresgroup)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dresgroup = defel;
		}
		else if (strcmp(defel->defname, "priority") == 0)
		{
			if (dpriority)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dpriority = defel;
		}
		else
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
	}

	if (dpassword && dpassword->arg)
		password = strVal(dpassword->arg);
	if (dissuper)
		issuper = intVal(dissuper->arg);
	if (dinherit)
		inherit = intVal(dinherit->arg);
	if (dcreaterole)
		createrole = intVal(dcreaterole->arg);
	if (dcreatedb)
		createdb = intVal(dcreatedb->arg);
	if (dcanlogin)
		canlogin = intVal(dcanlogin->arg);
	if (disreplication)
		isreplication = intVal(disreplication->arg);
	if (dconnlimit)
	{
		connlimit = intVal(dconnlimit->arg);
		if (connlimit < -1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid connection limit: %d", connlimit)));
	}
	if (drolemembers)
		rolemembers = (List *) drolemembers->arg;
	if (dvalidUntil)
		validUntil = strVal(dvalidUntil->arg);
	if (dbypassRLS)
		bypassrls = intVal(dbypassRLS->arg);
	if (daccountlock)
		accountlock = intVal(daccountlock->arg);
#ifdef _PG_ORCL_
	if (profile)
	{
			profname = strVal(profile->arg);
			if (profname)
			{
					if (strcmp(profname, "unlimited") == 0)
					{
							profid = InvalidOid;
					}
					else
					{
							profid = GetProfileIdByName(profname);
							if (!OidIsValid(profid))
							{
									ereport(ERROR,
													(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
													errmsg("profile \"%s\" does not exist", profname)));
							}
					}
			}
			else
			{
					ereport(ERROR,
									(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("no profile provided")));
			}
	}
	if (dcreateprofile)
	{
			createprofile = intVal(dcreateprofile->arg);
	}
#endif
#ifdef __RESOURCE_QUEUE__
	if (dresqueue)
		resqueue = strVal(dresqueue->arg);
#endif
	if (dresgroup)
		resgroup = strVal(linitial((List *) dresgroup->arg));
	if (dpriority && dpriority->arg)
		priority = strVal(dpriority->arg);
	/*
	 * Scan the pg_authid relation to be certain the user exists.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	pg_authid_dsc = RelationGetDescr(pg_authid_rel);

	tuple = get_rolespec_tuple(stmt->role);
	authform = (Form_pg_authid) GETSTRUCT(tuple);
	rolename = pstrdup(NameStr(authform->rolname));
	roleid = HeapTupleGetOid(tuple);

#ifdef _MLS_
    /*
     * the information of cls user and audit user in pg_authid could only be modidied by themselves, 
     * and only password could be changed.
     */
    if (DEFAULT_ROLE_MLS_SYS_USERID == roleid)
    {
        if (DEFAULT_ROLE_MLS_SYS_USERID != GetAuthenticatedUserId())
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("permission denied, the information of mls user would be changed IFF the authenticated user is mls user")));
        }
        if (NULL == dpassword||dissuper||dinherit||dcreaterole||dcreatedb||dcanlogin||disreplication||dconnlimit||dbypassRLS||daccountlock||drolemembers||dvalidUntil)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("permission denied, only passwd of mls user can be altered")));
        }
    }
    else if (DEFAULT_ROLE_AUDIT_SYS_USERID == roleid)
    {
        if (DEFAULT_ROLE_AUDIT_SYS_USERID != GetAuthenticatedUserId())
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("permission denied, the information of audit user should be changed IFF the authenticated user is audit user")));
        }  
        if (NULL == dpassword||dissuper||dinherit||dcreaterole||dcreatedb||dcanlogin||disreplication||dconnlimit||dbypassRLS||daccountlock||drolemembers||dvalidUntil)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("permission denied, only passwd of audit user can be altered")));
        }
    }
    else if (dpassword && datamask_check_user_in_white_list(roleid))
    {
        if (roleid != GetAuthenticatedUserId())
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("permission denied, password of users in white list can be altered only by themselves")));
        } 
    }
#endif

	/*
	 * To mess with a superuser you gotta be superuser; else you need
	 * createrole, or just want to change your own password
	 */

	bWas_super = ((Form_pg_authid) GETSTRUCT(tuple))->rolsuper;

	if (authform->rolsuper || issuper >= 0)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers")));
	}
	else if (authform->rolreplication || isreplication >= 0)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter replication users")));
	}
	else if (authform->rolbypassrls || bypassrls >= 0)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to change bypassrls attribute")));
	}
	else if (!have_createrole_privilege())
	{
		if (!(inherit < 0 &&
			  createrole < 0 &&
			  createdb < 0 &&
			  canlogin < 0 &&
			  isreplication < 0 &&
#ifdef _PG_ORCL_
			  createprofile < 0 && !unlock_account &&
#endif
			  !dconnlimit &&
			  !rolemembers &&
			  !validUntil &&
			  !daccountlock &&
			  dpassword &&
			  roleid == GetUserId()))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied")));
	}

	/* Convert validuntil to internal form */
	if (validUntil)
	{
		validUntil_datum = DirectFunctionCall3(timestamptz_in,
											   CStringGetDatum(validUntil),
											   ObjectIdGetDatum(InvalidOid),
											   Int32GetDatum(-1));
		validUntil_null = false;
	}
	else
	{
		/* fetch existing setting in case hook needs it */
		validUntil_datum = SysCacheGetAttr(AUTHNAME, tuple,
										   Anum_pg_authid_rolvaliduntil,
										   &validUntil_null);
	}

#ifdef _MLS_
    if (g_CheckPassword && password)
    {
        mls_passwd_comlexity_enforcement(rolename,
                        password,
                        get_password_type(password),
                        validUntil_datum,
                        validUntil_null);
    }
#endif


	/*
	 * Call the password checking hook if there is one defined
	 */
	if (check_password_hook && password)
		(*check_password_hook) (rolename,
								password,
								get_password_type(password),
								validUntil_datum,
								validUntil_null);

	/*
	 * Build an updated tuple, perusing the information just obtained
	 */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/*
	 * issuper/createrole/etc
	 */
	if (issuper >= 0)
	{
		bool isNull;
		char *roleResgroup;

		new_record[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(issuper > 0);
		new_record_repl[Anum_pg_authid_rolsuper - 1] = true;

		roleResgroup = NameStr(*DatumGetName(heap_getattr(tuple,
													Anum_pg_authid_rolresgroup,
													pg_authid_dsc, &isNull)));
		if (!isNull)
		{
			/*
			 * change the default resource group accordingly: admin_group
			 * for superuser and default_group for non-superuser
			 */
			if (issuper == 0 && strcmp(roleResgroup, "admin_group") == 0)
			{
				new_record[Anum_pg_authid_rolresgroup - 1] =
					DirectFunctionCall1(namein, CStringGetDatum("default_group"));
				new_record_repl[Anum_pg_authid_rolresgroup - 1] = true;
			}
			else if (issuper > 0 && strcmp(roleResgroup, "default_group") == 0)
			{
				new_record[Anum_pg_authid_rolresgroup - 1] =
					DirectFunctionCall1(namein, CStringGetDatum("admin_group"));
				new_record_repl[Anum_pg_authid_rolresgroup - 1] = true;
			}
		}

		/* get current superuser status */
		bWas_super = (issuper > 0);
	}

	if (inherit >= 0)
	{
		new_record[Anum_pg_authid_rolinherit - 1] = BoolGetDatum(inherit > 0);
		new_record_repl[Anum_pg_authid_rolinherit - 1] = true;
	}

	if (createrole >= 0)
	{
		new_record[Anum_pg_authid_rolcreaterole - 1] = BoolGetDatum(createrole > 0);
		new_record_repl[Anum_pg_authid_rolcreaterole - 1] = true;
	}

	if (createdb >= 0)
	{
		new_record[Anum_pg_authid_rolcreatedb - 1] = BoolGetDatum(createdb > 0);
		new_record_repl[Anum_pg_authid_rolcreatedb - 1] = true;
	}

	if (canlogin >= 0)
	{
		new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(canlogin > 0);
		new_record_repl[Anum_pg_authid_rolcanlogin - 1] = true;
	}

	if (isreplication >= 0)
	{
		new_record[Anum_pg_authid_rolreplication - 1] = BoolGetDatum(isreplication > 0);
		new_record_repl[Anum_pg_authid_rolreplication - 1] = true;
	}

	if (dconnlimit)
	{
		new_record[Anum_pg_authid_rolconnlimit - 1] = Int32GetDatum(connlimit);
		new_record_repl[Anum_pg_authid_rolconnlimit - 1] = true;
	}

	/* password */
	if (password)
	{
		char	   *shadow_pass;
		char	   *logdetail;

		/* Like in CREATE USER, don't allow an empty password. */
		if (password[0] == '\0' ||
			plain_crypt_verify(rolename, password, "", &logdetail) == STATUS_OK)
		{
			ereport(NOTICE,
					(errmsg("empty string is not a valid password, clearing password")));
			new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
		}
		else
		{
			/* Encrypt the password to the requested format. */
			shadow_pass = encrypt_password(Password_encryption, rolename,
										   password);

			new_record[Anum_pg_authid_rolpassword - 1] =
				CStringGetTextDatum(shadow_pass);
#ifdef _PG_ORCL_
			if (profile_password_limit && IS_LOCAL_ACCESS_NODE)
			{
				if (profname && OidIsValid(profid))
				{
					/* use specified profile */
					profile_passwd_comlexity_check(profname, rolename, password);
				}
				else
				{
					/* use previous profile if exists */
					profile_passwd_comlexity_check(NULL, rolename, password);
				}
			}
#endif
		}

		/* whether the create role satisfied the reuse conditions */
		if (g_CheckPassword)
			AddAuthHistory(roleid, rolename, password, ALTER_PG_AUTH_ROLE);

		new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
	}

	/* unset password */
	if (dpassword && dpassword->arg == NULL)
	{
		new_record_repl[Anum_pg_authid_rolpassword - 1] = true;
		new_record_nulls[Anum_pg_authid_rolpassword - 1] = true;
	}
#ifdef _PG_ORCL_
	if (new_record_repl[Anum_pg_authid_rolpassword - 1])
	{
		if (new_record_nulls[Anum_pg_authid_rolpassword - 1] == false)
		{
			char *passwd = TextDatumGetCString(new_record[Anum_pg_authid_rolpassword - 1]);
			if (IS_PGXC_LOCAL_COORDINATOR && profile_password_limit)
			{
				ProfileReusePassword(rolename, passwd);
			}
			InsertProfileUserPasswd(rolename, passwd, true);
		}
		else
		{
			InsertProfileUserPasswd(rolename, NULL, true);
		}
	}
#endif

	/* valid until */
	new_record[Anum_pg_authid_rolvaliduntil - 1] = validUntil_datum;
	new_record_nulls[Anum_pg_authid_rolvaliduntil - 1] = validUntil_null;
	new_record_repl[Anum_pg_authid_rolvaliduntil - 1] = true;

	if (bypassrls >= 0)
	{
		new_record[Anum_pg_authid_rolbypassrls - 1] = BoolGetDatum(bypassrls > 0);
		new_record_repl[Anum_pg_authid_rolbypassrls - 1] = true;
	}
#ifdef _PG_ORCL_
	if (profname)
	{
			new_record[Anum_pg_authid_rolprofile - 1] = ObjectIdGetDatum(profid);
			new_record_repl[Anum_pg_authid_rolprofile - 1] = true;
	}
	if (createprofile >= 0)
	{
			new_record[Anum_pg_authid_rolcreateprof - 1] = BoolGetDatum(createprofile > 0);
			new_record_repl[Anum_pg_authid_rolcreateprof - 1] = true;
	}
	if (unlock_account)
	{
			bool replace = false;
			int i = 0;
			ProfileAccountUnlock(rolename);
			/* only unlock account */
			for (i = 0; i < Natts_pg_authid; i++)
			{
					if (new_record_repl[i])
					{
							replace = true;
							break;
					}
			}
			if (!replace)
			{
					ReleaseSysCache(tuple);
					heap_close(pg_authid_rel, RowExclusiveLock);
					return InvalidOid;
			}
	}
#endif
#ifdef __RESOURCE_QUEUE__
	/* resource queue */
	if (resqueue)
	{
		/* NONE not supported -- use default queue  */
		if (0 == pg_strcasecmp(resqueue, "none") ||
			0 == pg_strcasecmp(resqueue, DEFAULT_RESQUEUE_NAME))
		{
			new_record[Anum_pg_authid_rolresqueue - 1] =
				DirectFunctionCall1(namein, CStringGetDatum(DEFAULT_RESQUEUE_NAME));
		}
		else
		{
			/* check resqueue if exists */
			if (!IsResQueueExists(resqueue))
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("resource queue \"%s\" does not exist",
							resqueue)));

			new_record[Anum_pg_authid_rolresqueue - 1] =
				DirectFunctionCall1(namein, CStringGetDatum(resqueue));
		}

		new_record_repl[Anum_pg_authid_rolresqueue - 1] = true;
	}
#endif
	/* resource group */
	if (resgroup)
	{
		Oid			rsgid;

		if (strcmp(resgroup, "none") == 0)
		{
			if (bWas_super)
				resgroup = pstrdup("admin_group");
			else
				resgroup = pstrdup("default_group");

			if (IsResGroupActivated() && IS_LOCAL_ACCESS_NODE)
				ereport(NOTICE,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("resource group required -- "
								"using default resource group \"%s\"",
								resgroup)));
		}

		rsgid = get_resgroup_oid(resgroup, false);
		if (rsgid == ADMINRESGROUP_OID && !bWas_super)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("only superuser can be assigned to admin resgroup")));
		new_record[Anum_pg_authid_rolresgroup - 1] =
			DirectFunctionCall1(namein, CStringGetDatum(resgroup));
		new_record_repl[Anum_pg_authid_rolresgroup - 1] = true;

		if (!IsResGroupActivated() && IS_LOCAL_ACCESS_NODE)
		{
			ereport(WARNING,
					(errmsg("resource group is disabled"),
					 errhint("To enable set enable_resource_group=true")));
		}
	}

	if (priority)
	{
			/* Only allow "high", "medium" and "low" */
			if (pg_strcasecmp(priority, "high") == 0)
			{
				new_record[Anum_pg_authid_rolpriority - 1] =
					CStringGetTextDatum("high");
			}
			else if (pg_strcasecmp(priority, "medium") == 0)
			{
				new_record[Anum_pg_authid_rolpriority - 1] =
					CStringGetTextDatum("medium");
			}
			else if (pg_strcasecmp(priority, "low") == 0)
			{
				new_record[Anum_pg_authid_rolpriority - 1] =
					CStringGetTextDatum("low");
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unexpected priority %s, shoule be one of \"high\", \"medium\" and \"low\"", priority)));
			}

			new_record_repl[Anum_pg_authid_rolpriority - 1] = true;
	}

	/* use rolcanlogin field to handle account lock */
	if (accountlock >= 0)
	{
		new_record[Anum_pg_authid_rolcanlogin - 1] = BoolGetDatum(accountlock == 0);
		new_record_repl[Anum_pg_authid_rolcanlogin - 1] = true;
	}

	new_tuple = heap_modify_tuple(tuple, pg_authid_dsc, new_record,
								  new_record_nulls, new_record_repl);
#ifdef _MLS_
    if (password)
    {
        /*
         * in heap_update procedure, it can not distinguish direct updating or alter stmt,
         * so, make a switch here.
         */
        mls_enable_update_rolpassword();
    }
#endif    
    CatalogTupleUpdate(pg_authid_rel, &tuple->t_self, new_tuple);
#ifdef _MLS_
    mls_disable_update_rolpassword();
#endif
	InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0);

	ReleaseSysCache(tuple);
	heap_freetuple(new_tuple);

	/*
	 * Advance command counter so we can see new record; else tests in
	 * AddRoleMems may fail.
	 */
	if (rolemembers)
		CommandCounterIncrement();

	if (stmt->action == +1)		/* add members to role */
		AddRoleMems(rolename, roleid,
					rolemembers, roleSpecsToIds(rolemembers),
					GetUserId(), false);
	else if (stmt->action == -1)	/* drop members from role */
		DelRoleMems(rolename, roleid,
					rolemembers, roleSpecsToIds(rolemembers),
					false);

	/*
	 * Close pg_authid, but keep lock till commit.
	 */
	heap_close(pg_authid_rel, NoLock);

	return roleid;
}


/*
 * ALTER ROLE ... SET
 */
Oid
AlterRoleSet(AlterRoleSetStmt *stmt)
{
	HeapTuple	roletuple;
	Oid			databaseid = InvalidOid;
	Oid			roleid = InvalidOid;

	if (stmt->role)
	{
		check_rolespec_name(stmt->role,
							"Cannot alter reserved roles.");

		roletuple = get_rolespec_tuple(stmt->role);
		roleid = HeapTupleGetOid(roletuple);

		/*
		 * Obtain a lock on the role and make sure it didn't go away in the
		 * meantime.
		 */
		shdepLockAndCheckObject(AuthIdRelationId, HeapTupleGetOid(roletuple));

		/*
		 * To mess with a superuser you gotta be superuser; else you need
		 * createrole, or just want to change your own settings
		 */
		if (((Form_pg_authid) GETSTRUCT(roletuple))->rolsuper)
		{
			if (!superuser())
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser to alter superusers")));
		}
		else
		{
			if (!have_createrole_privilege() &&
				HeapTupleGetOid(roletuple) != GetUserId())
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied")));
		}

		ReleaseSysCache(roletuple);
	}

	/* look up and lock the database, if specified */
	if (stmt->database != NULL)
	{
		databaseid = get_database_oid(stmt->database, false);
		shdepLockAndCheckObject(DatabaseRelationId, databaseid);

		if (!stmt->role)
		{
			/*
			 * If no role is specified, then this is effectively the same as
			 * ALTER DATABASE ... SET, so use the same permission check.
			 */
			if (!pg_database_ownercheck(databaseid, GetUserId()))
				aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
							   stmt->database);
		}
	}

	if (!stmt->role && !stmt->database)
	{
		/* Must be superuser to alter settings globally. */
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter settings globally")));
	}

	AlterSetting(databaseid, roleid, stmt->setstmt);

	return roleid;
}


/*
 * DROP ROLE
 */
void
DropRole(DropRoleStmt *stmt)
{
	Relation	pg_authid_rel,
				pg_auth_members_rel;
	ListCell   *item;

	if (!have_createrole_privilege())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to drop role")));

	/*
	 * Scan the pg_authid relation to find the Oid of the role(s) to be
	 * deleted.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	pg_auth_members_rel = heap_open(AuthMemRelationId, RowExclusiveLock);

	foreach(item, stmt->roles)
	{
		RoleSpec   *rolspec = lfirst(item);
		char	   *role;
		HeapTuple	tuple,
					tmp_tuple;
		ScanKeyData scankey;
		char	   *detail;
		char	   *detail_log;
		SysScanDesc sscan;
		Oid			roleid;

		if (rolspec->roletype != ROLESPEC_CSTRING)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot use special role specifier in DROP ROLE")));
		role = rolspec->rolename;

		tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(role));
		if (!HeapTupleIsValid(tuple))
		{
			if (!stmt->missing_ok)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("role \"%s\" does not exist", role)));
			}
			else
			{
				ereport(NOTICE,
						(errmsg("role \"%s\" does not exist, skipping",
								role)));
			}

			continue;
		}

		roleid = HeapTupleGetOid(tuple);

		if (roleid == GetUserId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("current user cannot be dropped")));
		if (roleid == GetOuterUserId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("current user cannot be dropped")));
		if (roleid == GetSessionUserId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_IN_USE),
					 errmsg("session user cannot be dropped")));

		/*
		 * For safety's sake, we allow createrole holders to drop ordinary
		 * roles but not superuser roles.  This is mainly to avoid the
		 * scenario where you accidentally drop the last superuser.
		 */
		if (((Form_pg_authid) GETSTRUCT(tuple))->rolsuper &&
			!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to drop superusers")));

		/* DROP hook for the role being removed */
		InvokeObjectDropHook(AuthIdRelationId, roleid, 0);

		/*
		 * Lock the role, so nobody can add dependencies to her while we drop
		 * her.  We keep the lock until the end of transaction.
		 */
		LockSharedObject(AuthIdRelationId, roleid, 0, AccessExclusiveLock);

		/* Check for pg_shdepend entries depending on this role */
		if (checkSharedDependencies(AuthIdRelationId, roleid,
									&detail, &detail_log))
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("role \"%s\" cannot be dropped because some objects depend on it",
							role),
					 errdetail_internal("%s", detail),
					 errdetail_log("%s", detail_log)));
#ifdef _MLS_
        if (true == mls_check_role_permission(roleid) ||
                true == cls_check_user_has_policy(roleid))
        {
            elog(ERROR, "could not drop role:%s, cause this role has mls poilcy bound", 
                            role);
        }
#endif
		/*
		 * Remove the role from the pg_authid table
		 */
		CatalogTupleDelete(pg_authid_rel, &tuple->t_self);

		ReleaseSysCache(tuple);

		/*
		 * Remove role from the pg_auth_members table.  We have to remove all
		 * tuples that show it as either a role or a member.
		 *
		 * XXX what about grantor entries?	Maybe we should do one heap scan.
		 */
		ScanKeyInit(&scankey,
					Anum_pg_auth_members_roleid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(roleid));

		sscan = systable_beginscan(pg_auth_members_rel, AuthMemRoleMemIndexId,
								   true, NULL, 1, &scankey);

		while (HeapTupleIsValid(tmp_tuple = systable_getnext(sscan)))
		{
			CatalogTupleDelete(pg_auth_members_rel, &tmp_tuple->t_self);
		}

		systable_endscan(sscan);

		ScanKeyInit(&scankey,
					Anum_pg_auth_members_member,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(roleid));

		sscan = systable_beginscan(pg_auth_members_rel, AuthMemMemRoleIndexId,
								   true, NULL, 1, &scankey);

		while (HeapTupleIsValid(tmp_tuple = systable_getnext(sscan)))
		{
			CatalogTupleDelete(pg_auth_members_rel, &tmp_tuple->t_self);
		}

		systable_endscan(sscan);

		/* delete the records of roleid in pg_auth_history */
		DropAuthHistory(roleid);

		/*
		 * Remove any comments or security labels on this role.
		 */
		DeleteSharedComments(roleid, AuthIdRelationId);
		DeleteSharedSecurityLabel(roleid, AuthIdRelationId);

		/*
		 * Remove settings for this role.
		 */
		DropSetting(InvalidOid, roleid);

		/*
		 * Advance command counter so that later iterations of this loop will
		 * see the changes already made.  This is essential if, for example,
		 * we are trying to drop both a role and one of its direct members ---
		 * we'll get an error if we try to delete the linking pg_auth_members
		 * tuple twice.  (We do not need a CCI between the two delete loops
		 * above, because it's not allowed for a role to directly contain
		 * itself.)
		 */
		CommandCounterIncrement();

		if (POOL_CONN_RELEASE_SUCCESS != PoolManagerClosePooledConnections(NULL, role))
		{
			elog(ERROR, "failed to close pooled connection for role:%s", role);
		}
#ifdef _PG_ORCL_
		DropProfileUserPasswd(role);
#endif
	}

	/*
	 * Now we can clean up; but keep locks until commit.
	 */
	heap_close(pg_auth_members_rel, NoLock);
	heap_close(pg_authid_rel, NoLock);
}

/*
 * Rename role
 */
ObjectAddress
RenameRole(const char *oldname, const char *newname)
{
	HeapTuple	oldtuple,
				newtuple;
	TupleDesc	dsc;
	Relation	rel;
	Datum		datum;
	bool		isnull;
	Datum		repl_val[Natts_pg_authid];
	bool		repl_null[Natts_pg_authid];
	bool		repl_repl[Natts_pg_authid];
	int			i;
	Oid			roleid;
	ObjectAddress address;
	Form_pg_authid authform;

	rel = heap_open(AuthIdRelationId, RowExclusiveLock);
	dsc = RelationGetDescr(rel);

	oldtuple = SearchSysCache1(AUTHNAME, CStringGetDatum(oldname));
	if (!HeapTupleIsValid(oldtuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", oldname)));

	/*
	 * XXX Client applications probably store the session user somewhere, so
	 * renaming it could cause confusion.  On the other hand, there may not be
	 * an actual problem besides a little confusion, so think about this and
	 * decide.  Same for SET ROLE ... we don't restrict renaming the current
	 * effective userid, though.
	 */

	roleid = HeapTupleGetOid(oldtuple);
	authform = (Form_pg_authid) GETSTRUCT(oldtuple);

	if (roleid == GetSessionUserId())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("session user cannot be renamed")));
	if (roleid == GetOuterUserId())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("current user cannot be renamed")));

	/*
	 * Check that the user is not trying to rename a system role and not
	 * trying to rename a role into the reserved "pg_" namespace.
	 */
	if (IsReservedName(NameStr(authform->rolname)))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("role name \"%s\" is reserved",
						NameStr(authform->rolname)),
				 errdetail("Role names starting with \"pg_\" are reserved.")));

	if (IsReservedName(newname))
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("role name \"%s\" is reserved",
						newname),
				 errdetail("Role names starting with \"pg_\" are reserved.")));

	/* make sure the new name doesn't exist */
	if (SearchSysCacheExists1(AUTHNAME, CStringGetDatum(newname)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("role \"%s\" already exists", newname)));

#ifdef _MLS_
    if (DEFAULT_ROLE_MLS_SYS_USERID == roleid || DEFAULT_ROLE_AUDIT_SYS_USERID == roleid)
    {
        ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("\"%s\" cannot be renamed",
						NameStr(authform->rolname))));
    }
#endif


	/*
	 * createrole is enough privilege unless you want to mess with a superuser
	 */
	if (((Form_pg_authid) GETSTRUCT(oldtuple))->rolsuper)
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to rename superusers")));
	}
	else
	{
		if (!have_createrole_privilege())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to rename role")));
	}
#ifdef _MLS_
    if (true == mls_check_role_permission(roleid))
    {
        elog(ERROR, "could not rename role:%s, cause this role has mls poilcy bound", 
                        oldname);
    }
#endif
    

	/* OK, construct the modified tuple */
	for (i = 0; i < Natts_pg_authid; i++)
		repl_repl[i] = false;

	repl_repl[Anum_pg_authid_rolname - 1] = true;
	repl_val[Anum_pg_authid_rolname - 1] = DirectFunctionCall1(namein,
															   CStringGetDatum(newname));
	repl_null[Anum_pg_authid_rolname - 1] = false;

	datum = heap_getattr(oldtuple, Anum_pg_authid_rolpassword, dsc, &isnull);

	if (!isnull && get_password_type(TextDatumGetCString(datum)) == PASSWORD_TYPE_MD5)
	{
		/* MD5 uses the username as salt, so just clear it on a rename */
		repl_repl[Anum_pg_authid_rolpassword - 1] = true;
		repl_null[Anum_pg_authid_rolpassword - 1] = true;

		ereport(NOTICE,
				(errmsg("MD5 password cleared because of role rename")));
	}

	newtuple = heap_modify_tuple(oldtuple, dsc, repl_val, repl_null, repl_repl);
#ifdef _MLS_
    if (!isnull 
        && !datamask_check_user_in_white_list(roleid))
    {
        mls_enable_update_rolpassword();
    }
#endif
	CatalogTupleUpdate(rel, &oldtuple->t_self, newtuple);
#ifdef _MLS_
    mls_disable_update_rolpassword();
#endif
#ifdef _PG_ORCL_
	RenameProfileUserPasswd(oldname, newname);
#endif
	InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0);

	ObjectAddressSet(address, AuthIdRelationId, roleid);

	ReleaseSysCache(oldtuple);

	/*
	 * Close pg_authid, but keep lock till commit.
	 */
	heap_close(rel, NoLock);

	return address;
}

/*
 * GrantRoleStmt
 *
 * Grant/Revoke roles to/from roles
 */
void
GrantRole(GrantRoleStmt *stmt)
{
	Relation	pg_authid_rel;
	Oid			grantor;
	List	   *grantee_ids;
	ListCell   *item;

	if (stmt->grantor)
		grantor = get_rolespec_oid(stmt->grantor, false);
	else
		grantor = GetUserId();

	grantee_ids = roleSpecsToIds(stmt->grantee_roles);

	/* AccessShareLock is enough since we aren't modifying pg_authid */
	pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);

	/*
	 * Step through all of the granted roles and add/remove entries for the
	 * grantees, or, if admin_opt is set, then just add/remove the admin
	 * option.
	 *
	 * Note: Permissions checking is done by AddRoleMems/DelRoleMems
	 */
	foreach(item, stmt->granted_roles)
	{
		AccessPriv *priv = (AccessPriv *) lfirst(item);
		char	   *rolename = priv->priv_name;
		Oid			roleid;

		/* Must reject priv(columns) and ALL PRIVILEGES(columns) */
		if (rolename == NULL || priv->cols != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_GRANT_OPERATION),
					 errmsg("column names cannot be included in GRANT/REVOKE ROLE")));

		roleid = get_role_oid(rolename, false);
		if (stmt->is_grant)
			AddRoleMems(rolename, roleid,
						stmt->grantee_roles, grantee_ids,
						grantor, stmt->admin_opt);
		else
			DelRoleMems(rolename, roleid,
						stmt->grantee_roles, grantee_ids,
						stmt->admin_opt);
	}

	/*
	 * Close pg_authid, but keep lock till commit.
	 */
	heap_close(pg_authid_rel, NoLock);
}

/*
 * DropOwnedObjects
 *
 * Drop the objects owned by a given list of roles.
 */
void
DropOwnedObjects(DropOwnedStmt *stmt)
{
	List	   *role_ids = roleSpecsToIds(stmt->roles);
	ListCell   *cell;

	/* Check privileges */
	foreach(cell, role_ids)
	{
		Oid			roleid = lfirst_oid(cell);

		if (!has_privs_of_role(GetUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to drop objects")));
	}

	/* Ok, do it */
	shdepDropOwned(role_ids, stmt->behavior);
}

/*
 * ReassignOwnedObjects
 *
 * Give the objects owned by a given list of roles away to another user.
 */
void
ReassignOwnedObjects(ReassignOwnedStmt *stmt)
{
	List	   *role_ids = roleSpecsToIds(stmt->roles);
	ListCell   *cell;
	Oid			newrole;

	/* Check privileges */
	foreach(cell, role_ids)
	{
		Oid			roleid = lfirst_oid(cell);

		if (!has_privs_of_role(GetUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to reassign objects")));
	}

	/* Must have privileges on the receiving side too */
	newrole = get_rolespec_oid(stmt->newrole, false);

	if (!has_privs_of_role(GetUserId(), newrole))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to reassign objects")));

	/* Ok, do it */
	shdepReassignOwned(role_ids, newrole);
}

/*
 * roleSpecsToIds
 *
 * Given a list of RoleSpecs, generate a list of role OIDs in the same order.
 *
 * ROLESPEC_PUBLIC is not allowed.
 */
List *
roleSpecsToIds(List *memberNames)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, memberNames)
	{
		RoleSpec   *rolespec = lfirst_node(RoleSpec, l);
		Oid			roleid;

		roleid = get_rolespec_oid(rolespec, false);
		result = lappend_oid(result, roleid);
	}
	return result;
}

/*
 * AddRoleMems -- Add given members to the specified role
 *
 * rolename: name of role to add to (used only for error messages)
 * roleid: OID of role to add to
 * memberSpecs: list of RoleSpec of roles to add (used only for error messages)
 * memberIds: OIDs of roles to add
 * grantorId: who is granting the membership
 * admin_opt: granting admin option?
 *
 * Note: caller is responsible for calling auth_file_update_needed().
 */
static void
AddRoleMems(const char *rolename, Oid roleid,
			List *memberSpecs, List *memberIds,
			Oid grantorId, bool admin_opt)
{
	Relation	pg_authmem_rel;
	TupleDesc	pg_authmem_dsc;
	ListCell   *specitem;
	ListCell   *iditem;

	Assert(list_length(memberSpecs) == list_length(memberIds));

	/* Skip permission check if nothing to do */
	if (!memberIds)
		return;

	/*
	 * Check permissions: must have createrole or admin option on the role to
	 * be changed.  To mess with a superuser role, you gotta be superuser.
	 */
	if (superuser_arg(roleid))
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers")));
	}
	else
	{
		if (!have_createrole_privilege() &&
			!is_admin_of_role(grantorId, roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must have admin option on role \"%s\"",
							rolename)));
	}

	/*
	 * The role membership grantor of record has little significance at
	 * present.  Nonetheless, inasmuch as users might look to it for a crude
	 * audit trail, let only superusers impute the grant to a third party.
	 *
	 * Before lifting this restriction, give the member == role case of
	 * is_admin_of_role() a fresh look.  Ensure that the current role cannot
	 * use an explicit grantor specification to take advantage of the session
	 * user's self-admin right.
	 */
	if (grantorId != GetUserId() && !superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set grantor")));

	pg_authmem_rel = heap_open(AuthMemRelationId, RowExclusiveLock);
	pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

	forboth(specitem, memberSpecs, iditem, memberIds)
	{
		RoleSpec   *memberRole = lfirst(specitem);
		Oid			memberid = lfirst_oid(iditem);
		HeapTuple	authmem_tuple;
		HeapTuple	tuple;
		Datum		new_record[Natts_pg_auth_members];
		bool		new_record_nulls[Natts_pg_auth_members];
		bool		new_record_repl[Natts_pg_auth_members];

		/*
		 * Refuse creation of membership loops, including the trivial case
		 * where a role is made a member of itself.  We do this by checking to
		 * see if the target role is already a member of the proposed member
		 * role.  We have to ignore possible superuserness, however, else we
		 * could never grant membership in a superuser-privileged role.
		 */
		if (is_member_of_role_nosuper(roleid, memberid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_GRANT_OPERATION),
					 (errmsg("role \"%s\" is a member of role \"%s\"",
							 rolename, get_rolespec_name(memberRole)))));

		/*
		 * Check if entry for this role/member already exists; if so, give
		 * warning unless we are adding admin option.
		 */
		authmem_tuple = SearchSysCache2(AUTHMEMROLEMEM,
										ObjectIdGetDatum(roleid),
										ObjectIdGetDatum(memberid));
		if (HeapTupleIsValid(authmem_tuple) &&
			(!admin_opt ||
			 ((Form_pg_auth_members) GETSTRUCT(authmem_tuple))->admin_option))
		{
			ereport(NOTICE,
					(errmsg("role \"%s\" is already a member of role \"%s\"",
							get_rolespec_name(memberRole), rolename)));
			ReleaseSysCache(authmem_tuple);
			continue;
		}

		/* Build a tuple to insert or update */
		MemSet(new_record, 0, sizeof(new_record));
		MemSet(new_record_nulls, false, sizeof(new_record_nulls));
		MemSet(new_record_repl, false, sizeof(new_record_repl));

		new_record[Anum_pg_auth_members_roleid - 1] = ObjectIdGetDatum(roleid);
		new_record[Anum_pg_auth_members_member - 1] = ObjectIdGetDatum(memberid);
		new_record[Anum_pg_auth_members_grantor - 1] = ObjectIdGetDatum(grantorId);
		new_record[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(admin_opt);

		if (HeapTupleIsValid(authmem_tuple))
		{
			new_record_repl[Anum_pg_auth_members_grantor - 1] = true;
			new_record_repl[Anum_pg_auth_members_admin_option - 1] = true;
			tuple = heap_modify_tuple(authmem_tuple, pg_authmem_dsc,
									  new_record,
									  new_record_nulls, new_record_repl);
			CatalogTupleUpdate(pg_authmem_rel, &tuple->t_self, tuple);
			ReleaseSysCache(authmem_tuple);
		}
		else
		{
			tuple = heap_form_tuple(pg_authmem_dsc,
									new_record, new_record_nulls);
			CatalogTupleInsert(pg_authmem_rel, tuple);
		}

		/* CCI after each change, in case there are duplicates in list */
		CommandCounterIncrement();
	}

	/*
	 * Close pg_authmem, but keep lock till commit.
	 */
	heap_close(pg_authmem_rel, NoLock);
}

/*
 * DelRoleMems -- Remove given members from the specified role
 *
 * rolename: name of role to del from (used only for error messages)
 * roleid: OID of role to del from
 * memberSpecs: list of RoleSpec of roles to del (used only for error messages)
 * memberIds: OIDs of roles to del
 * admin_opt: remove admin option only?
 *
 * Note: caller is responsible for calling auth_file_update_needed().
 */
static void
DelRoleMems(const char *rolename, Oid roleid,
			List *memberSpecs, List *memberIds,
			bool admin_opt)
{
	Relation	pg_authmem_rel;
	TupleDesc	pg_authmem_dsc;
	ListCell   *specitem;
	ListCell   *iditem;

	Assert(list_length(memberSpecs) == list_length(memberIds));

	/* Skip permission check if nothing to do */
	if (!memberIds)
		return;

	/*
	 * Check permissions: must have createrole or admin option on the role to
	 * be changed.  To mess with a superuser role, you gotta be superuser.
	 */
	if (superuser_arg(roleid))
	{
		if (!superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to alter superusers")));
	}
	else
	{
		if (!have_createrole_privilege() &&
			!is_admin_of_role(GetUserId(), roleid))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must have admin option on role \"%s\"",
							rolename)));
	}

	pg_authmem_rel = heap_open(AuthMemRelationId, RowExclusiveLock);
	pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

	forboth(specitem, memberSpecs, iditem, memberIds)
	{
		RoleSpec   *memberRole = lfirst(specitem);
		Oid			memberid = lfirst_oid(iditem);
		HeapTuple	authmem_tuple;

		/*
		 * Find entry for this role/member
		 */
		authmem_tuple = SearchSysCache2(AUTHMEMROLEMEM,
										ObjectIdGetDatum(roleid),
										ObjectIdGetDatum(memberid));
		if (!HeapTupleIsValid(authmem_tuple))
		{
			ereport(WARNING,
					(errmsg("role \"%s\" is not a member of role \"%s\"",
							get_rolespec_name(memberRole), rolename)));
			continue;
		}

		if (!admin_opt)
		{
			/* Remove the entry altogether */
			CatalogTupleDelete(pg_authmem_rel, &authmem_tuple->t_self);
		}
		else
		{
			/* Just turn off the admin option */
			HeapTuple	tuple;
			Datum		new_record[Natts_pg_auth_members];
			bool		new_record_nulls[Natts_pg_auth_members];
			bool		new_record_repl[Natts_pg_auth_members];

			/* Build a tuple to update with */
			MemSet(new_record, 0, sizeof(new_record));
			MemSet(new_record_nulls, false, sizeof(new_record_nulls));
			MemSet(new_record_repl, false, sizeof(new_record_repl));

			new_record[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(false);
			new_record_repl[Anum_pg_auth_members_admin_option - 1] = true;

			tuple = heap_modify_tuple(authmem_tuple, pg_authmem_dsc,
									  new_record,
									  new_record_nulls, new_record_repl);
			CatalogTupleUpdate(pg_authmem_rel, &tuple->t_self, tuple);
		}

		ReleaseSysCache(authmem_tuple);

		/* CCI after each change, in case there are duplicates in list */
		CommandCounterIncrement();
	}

	/*
	 * Close pg_authmem, but keep lock till commit.
	 */
	heap_close(pg_authmem_rel, NoLock);
}

#ifdef _MLS_
/*
 * password complexity enforcement.
 * 1. length cannot be less than 8 characters
 * 2. must contain '1' lower letter/upper letter/digital number/specail letter at the same time.
 * 3. password cannot contain username
 * NOTE: if alter user XX encrypted password 'md5xxxxxx', we cannot decrypt to the original context, 
 * so, we just get a encrypted password and make a comparsion, to prove the md5 password was not the username.
 *
 * we do this just as checkpassword
 */
static void mls_passwd_comlexity_enforcement(const char *username,
			   const char *password,
			   int password_type,
			   Datum validuntil_time,
			   bool validuntil_null)
{
/* passwords shorter than this will be rejected */
#define MIN_PWD_LENGTH 8
	int			pwdlen = strlen(password);
	int			i;
	bool		pwd_has_upper_letter	= false;
	bool		pwd_has_lower_letter	= false;
	bool		pwd_has_sepcial_letter	= false;
	bool		pwd_has_digit			= false;

	if (PASSWORD_TYPE_PLAINTEXT != password_type)
	{
		/*
		 * Unfortunately we cannot perform exhaustive checks on encrypted
		 * passwords - we are restricted to guessing. (Alternatively, we
		 * could insist on the password being presented non-encrypted, but
		 * that has its own security disadvantages.)
		 *
		 * so, we only check for wether username equals encryted password.
		 */
        char	   *logdetail;

		if (plain_crypt_verify(username, password, username, &logdetail) == STATUS_OK)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("password must not contain user name")));
		}
        
        (void)logdetail;
	}
    else
    {
		/*
		 * For unencrypted passwords we can perform better checks
		 */

		/* enforce minimum length */
		if (pwdlen < MIN_PWD_LENGTH)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("password is too short")));
		}

		/* check if the password contains the username */
		if (strstr(password, username))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("password must not contain user name")));
		}

		/* check if the password contains upper letters, lower letters, specail letters, numbers */
		for (i = 0; i < pwdlen; i++)
		{
			if (isupper((unsigned char) password[i]))
			{
				pwd_has_upper_letter = true;
			}
            else if (islower((unsigned char) password[i]))
            {
				pwd_has_lower_letter = true;
            }
			else if (isdigit((unsigned char) password[i]))
			{
				pwd_has_digit		 = true;
			}
			else if (ispunct((unsigned char) password[i]))
			{
				pwd_has_sepcial_letter = true;
			}
		}
		if (!pwd_has_upper_letter || !pwd_has_lower_letter || !pwd_has_digit || !pwd_has_sepcial_letter)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			        errmsg("password must contain uppercase-letters, lowercase-letters, specail-letters and digits.")));
		}
	}
    
    return;
}
#endif

#ifdef __OPENTENBASE_C__
/*
 * when rolpassword is changed, add the change record in pg_auth_history
 * roleID, Oid of role; passwd, input password; operatType, whether the operation is create role or alter role
 */
static void AddAuthHistory(Oid roleID, const char* rolename, const char* password, int operatType)
{
	Relation pg_auth_history_rel;

	if (password == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PASSWORD),
				 errmsg("the parameter shadow_pass of AddAuthHistory is null")));

	pg_auth_history_rel = RelationIdGetRelation(AuthHistoryRelationId);
	if (RelationIsValid(pg_auth_history_rel)) {
		TupleDesc pg_auth_history_dsc = NULL;
		HeapTuple password_tuple = NULL;
		ScanKeyData key[1];
		SysScanDesc scan = NULL;
		Datum rolpasswordDatum;
		char *rolpassword = NULL; /* get rolpassword from pg_auth_history */
		bool rolpasswordIsNull = false;
		Datum password_new_record[Natts_pg_auth_history];
		char password_new_record_nulls[Natts_pg_auth_history];
		char *shadow_pass = NULL;

		LockRelationOid(AuthHistoryRelationId, RowExclusiveLock);
		pgstat_initstats(pg_auth_history_rel);
		pg_auth_history_dsc = RelationGetDescr(pg_auth_history_rel);

		shadow_pass = encrypt_password(Password_encryption, rolename, password);

		/* if the operation is alter role, we will check the reuse conditons */
		if (operatType == ALTER_PG_AUTH_ROLE)
		{
			/* get all the records from pg_auth_history of the roleID */
			ScanKeyInit(&key[0],
						Anum_pg_auth_history_roloid,
						BTEqualStrategyNumber,
						F_OIDEQ,
						ObjectIdGetDatum(roleID));

			scan = systable_beginscan(pg_auth_history_rel, AuthHistoryIndexId, true, NULL, 1, key);

			/* get the tuple according to reverse order of the index */
			while (HeapTupleIsValid(password_tuple = systable_getnext_ordered(scan, BackwardScanDirection)))
			{
				rolpasswordDatum = heap_getattr(password_tuple,
												Anum_pg_auth_history_rolpassword,
												pg_auth_history_dsc,
												&rolpasswordIsNull);

				/* get the password of the tuple */
				rolpassword = TextDatumGetCString(rolpasswordDatum);

				/* whether the tuple's rolepassword is same to passwd */
				if (0 == strcmp(shadow_pass, rolpassword))
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PASSWORD),
							 errmsg("Password has been already used. Please choose another one.")));
				}
			}

			systable_endscan(scan);
		}

		/* insert the current operation record into the table */
		memset(password_new_record, 0, sizeof(password_new_record));
		memset(password_new_record_nulls, false, sizeof(password_new_record_nulls));

		password_new_record[Anum_pg_auth_history_roloid - 1] = ObjectIdGetDatum(roleID);
		password_new_record[Anum_pg_auth_history_rolpassword - 1] = CStringGetTextDatum(shadow_pass);

		password_tuple = heap_form_tuple(pg_auth_history_dsc, password_new_record, password_new_record_nulls);

		CatalogTupleInsert(pg_auth_history_rel, password_tuple);
	}
	else
	{
		ereport(WARNING,
				(errmsg("the relation pg_auth_history is invalid")));
		return;
	}

	heap_close(pg_auth_history_rel, NoLock);
}

/*
 * delete all the records of roleID in pg_auth_history
 */
static void DropAuthHistory(Oid roleID)
{
	Relation pg_auth_history_rel = NULL;
	SysScanDesc sscan = NULL;
	HeapTuple tmp_tuple = NULL;
	ScanKeyData scankey;

	pg_auth_history_rel = RelationIdGetRelation(AuthHistoryRelationId);

	/* if the relation is valid, then delete the records of the role */
	if (RelationIsValid(pg_auth_history_rel))
	{
		LockRelationOid(AuthHistoryRelationId, RowExclusiveLock);
		pgstat_initstats(pg_auth_history_rel);

		ScanKeyInit(&scankey,
					Anum_pg_auth_history_roloid,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(roleID));

		sscan = systable_beginscan(pg_auth_history_rel, AuthHistoryIndexId, true, 0, 1, &scankey);

		while (HeapTupleIsValid(tmp_tuple = systable_getnext(sscan)))
		{
			simple_heap_delete(pg_auth_history_rel, &tmp_tuple->t_self);
		}

		systable_endscan(sscan);
		heap_close(pg_auth_history_rel, NoLock);
	}
	else
	{
		ereport(WARNING,
				(errmsg("the relation pg_auth_history is invalid")));
		return;
	}
}
#endif

void
ProhibitRoleLogin(const char *rolename)
{
	HeapTuple	tuple;
	Relation	pg_authid_rel;

	pg_authid_rel = heap_open(AuthIdRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(AUTHNAME, CStringGetDatum(rolename));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", rolename)));

	Assert(((Form_pg_authid) GETSTRUCT(tuple))->rolcanlogin);

	((Form_pg_authid) GETSTRUCT(tuple))->rolcanlogin = BoolGetDatum(false);

	heap_inplace_update(pg_authid_rel, tuple);

	ReleaseSysCache(tuple);
	heap_close(pg_authid_rel, RowExclusiveLock);

	/*
	 * Make changes visible
	 */
	CommandCounterIncrement();

	return;
}
