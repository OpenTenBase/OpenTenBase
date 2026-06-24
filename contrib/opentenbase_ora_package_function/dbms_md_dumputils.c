/*-------------------------------------------------------------------------
 *
 * Utility routines for SQL dumping
 *
 * Basically this is stuff that is useful in both pg_dump and pg_dumpall.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pg_dump/dumputils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "lib/stringinfo.h"
#include "common/keywords.h"
#include "dbms_md_string_utils.h"
#include "dbms_md_dumputils.h"

static bool parseAclItem(const char *item, const char *type,
			 const char *name, const char *subname, int remoteVersion,
			 StringInfo grantee, StringInfo grantor,
			 StringInfo privs, StringInfo privswgo);
static char *copyAclUserName(StringInfo output, char *input);
static void AddAcl(StringInfo aclbuf, const char *keyword,
	   const char *subname);

char *dbms_md_strdup(const char *in)
{
	char	   *tmp;

	if (!in)
	{
		/*elog(ERROR,	_("cannot duplicate null pointer (internal error)\n"));*/
		return NULL;
	}
	
	tmp = strdup(in);
	if (!tmp)
	{
		elog(ERROR, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	
	return tmp;
}

void *
dbms_md_malloc0(MemoryContext mem_ctx, size_t len)
{
	return MemoryContextAllocZero(mem_ctx, len);
}

void *
dbms_md_palloc(size_t len)
{
	return palloc(len);
}


void *
dbms_md_realloc(void *ptr, size_t len)
{
	void	   *tmp;

	/* Avoid unportable behavior of realloc(NULL, 0) */
	if (ptr == NULL && len == 0)
		len = 1;
	tmp = repalloc(ptr, len);
	if (!tmp)
	{
		elog(ERROR, _("out of memory\n"));
		exit(EXIT_FAILURE);
	}
	return tmp;
}

void 
dbms_md_free(void *ptr)
{
	if (ptr)
		pfree(ptr);
}


/*
 * Build GRANT/REVOKE command(s) for an object.
 *
 *	name: the object name, in the form to use in the commands (already quoted)
 *	subname: the sub-object name, if any (already quoted); NULL if none
 *	type: the object type (as seen in GRANT command: must be one of
 *		TABLE, SEQUENCE, FUNCTION, PROCEDURE, LANGUAGE, SCHEMA, DATABASE, TABLESPACE,
 *		FOREIGN DATA WRAPPER, SERVER, or LARGE OBJECT)
 *	acls: the ACL string fetched from the database
 *	racls: the ACL string of any initial-but-now-revoked privileges
 *	owner: username of object owner (will be passed through fmtId); can be
 *		NULL or empty string to indicate "no owner known"
 *	prefix: string to prefix to each generated command; typically empty
 *	remoteVersion: version of database
 *
 * Returns TRUE if okay, FALSE if could not parse the acl string.
 * The resulting commands (if any) are appended to the contents of 'sql'.
 *
 * Note: when processing a default ACL, prefix is "ALTER DEFAULT PRIVILEGES "
 * or something similar, and name is an empty string.
 *
 * Note: beware of passing a fmtId() result directly as 'name' or 'subname',
 * since this routine uses fmtId() internally.
 */
bool
buildACLCommands(const char *name, const char *subname,
				 const char *type, const char *acls, const char *racls,
				 const char *owner, const char *prefix, int remoteVersion,
				 StringInfo sql)
{
	bool		ok = true;
	char	  **aclitems = NULL;
	char	  **raclitems = NULL;
	int			naclitems = 0;
	int			nraclitems = 0;
	int			i;
	StringInfo grantee,
				grantor,
				privs,
				privswgo;
	StringInfo firstsql,
				secondsql;
	bool		found_owner_privs = false;

	if (strlen(acls) == 0 && strlen(racls) == 0)
		return true;			/* object has default permissions */

	/* treat empty-string owner same as NULL */
	if (owner && *owner == '\0')
		owner = NULL;

	if (strlen(acls) != 0)
	{
		if (!dbms_md_parse_array(acls, &aclitems, &naclitems))
		{
			if (aclitems)
				pfree(aclitems);
			return false;
		}
	}

	if (strlen(racls) != 0)
	{
		if (!dbms_md_parse_array(racls, &raclitems, &nraclitems))
		{
			if (raclitems)
				pfree(raclitems);
			return false;
		}
	}

	grantee = createStringInfo();
	grantor = createStringInfo();
	privs = createStringInfo();
	privswgo = createStringInfo();

	/*
	 * At the end, these two will be pasted together to form the result.
	 *
	 * For older systems we use these to ensure that the owner privileges go
	 * before the other ones, as a GRANT could create the default entry for
	 * the object, which generally includes all rights for the owner. In more
	 * recent versions we normally handle this because the owner rights come
	 * first in the ACLs, but older versions might have them after the PUBLIC
	 * privileges.
	 *
	 * For 9.6 and later systems, much of this changes.  With 9.6, we check
	 * the default privileges for the objects at dump time and create two sets
	 * of ACLs- "racls" which are the ACLs to REVOKE from the object (as the
	 * object may have initial privileges on it, along with any default ACLs
	 * which are not part of the current set of privileges), and regular
	 * "acls", which are the ACLs to GRANT to the object.  We handle the
	 * REVOKEs first, followed by the GRANTs.
	 */
	firstsql = createStringInfo();
	secondsql = createStringInfo();

	/*
	 * For pre-9.6 systems, we always start with REVOKE ALL FROM PUBLIC, as we
	 * don't wish to make any assumptions about what the default ACLs are, and
	 * we do not collect them during the dump phase (and racls will always be
	 * the empty set, see above).
	 *
	 * For 9.6 and later, if any revoke ACLs have been provided, then include
	 * them in 'firstsql'.
	 *
	 * Revoke ACLs happen when an object starts out life with a set of
	 * privileges (eg: GRANT SELECT ON pg_class TO PUBLIC;) and the user has
	 * decided to revoke those rights.  Since those objects come into being
	 * with those default privileges, we have to revoke them to match what the
	 * current state of affairs is.  Note that we only started explicitly
	 * tracking such initial rights in 9.6, and prior to that all initial
	 * rights are actually handled by the simple 'REVOKE ALL .. FROM PUBLIC'
	 * case, for initdb-created objects.  Prior to 9.6, we didn't handle
	 * extensions correctly, but we do now by tracking their initial
	 * privileges, in the same way we track initdb initial privileges, see
	 * pg_init_privs.
	 */
	if (remoteVersion < 90600)
	{
		Assert(nraclitems == 0);

		appendStringInfo(firstsql, "%sREVOKE ALL", prefix);
		if (subname)
			appendStringInfo(firstsql, "(%s)", subname);
		appendStringInfo(firstsql, " ON %s %s FROM PUBLIC;\n", type, name);
	}
	else
	{
		/* Scan individual REVOKE ACL items */
		for (i = 0; i < nraclitems; i++)
		{
			if (!parseAclItem(raclitems[i], type, name, subname, remoteVersion,
							  grantee, grantor, privs, privswgo))
			{
				ok = false;
				break;
			}

			if (privs->len > 0 || privswgo->len > 0)
			{
				if (privs->len > 0)
				{
					appendStringInfo(firstsql, "%sREVOKE %s ON %s %s FROM ",
									  prefix, privs->data, type, name);
					if (grantee->len == 0)
						appendStringInfoString(firstsql, "PUBLIC;\n");
					else if (strncmp(grantee->data, "group ",	strlen("group ")) == 0)
						appendStringInfo(firstsql, "GROUP %s;\n",
										  dbms_md_fmtId(grantee->data + strlen("group ")));
					else
						appendStringInfo(firstsql, "%s;\n", dbms_md_fmtId(grantee->data));
				}
				if (privswgo->len > 0)
				{
					appendStringInfo(firstsql,
									  "%sREVOKE GRANT OPTION FOR %s ON %s %s FROM ",
									  prefix, privswgo->data, type, name);
					if (grantee->len == 0)
						appendStringInfoString(firstsql, "PUBLIC");
					else if (strncmp(grantee->data, "group ", strlen("group ")) == 0)
						appendStringInfo(firstsql, "GROUP %s", 
								dbms_md_fmtId(grantee->data + strlen("group ")));
					else
						appendStringInfoString(firstsql, dbms_md_fmtId(grantee->data));
				}
			}
		}
	}

	/*
	 * We still need some hacking though to cover the case where new default
	 * public privileges are added in new versions: the REVOKE ALL will revoke
	 * them, leading to behavior different from what the old version had,
	 * which is generally not what's wanted.  So add back default privs if the
	 * source database is too old to have had that particular priv.
	 */
	if (remoteVersion < 80200 && strcmp(type, "DATABASE") == 0)
	{
		/* database CONNECT priv didn't exist before 8.2 */
		appendStringInfo(firstsql, "%sGRANT CONNECT ON %s %s TO PUBLIC;\n",
						  prefix, type, name);
	}

	/* Scan individual ACL items */
	for (i = 0; i < naclitems; i++)
	{
		if (!parseAclItem(aclitems[i], type, name, subname, remoteVersion,
						  grantee, grantor, privs, privswgo))
		{
			ok = false;
			break;
		}

		if (grantor->len == 0 && owner)
			appendStringInfo(grantor, "%s", owner);

		if (privs->len > 0 || privswgo->len > 0)
		{
			/*
			 * Prior to 9.6, we had to handle owner privileges in a special
			 * manner by first REVOKE'ing the rights and then GRANT'ing them
			 * after.  With 9.6 and above, what we need to REVOKE and what we
			 * need to GRANT is figured out when we dump and stashed into
			 * "racls" and "acls", respectively.  See above.
			 */
			if (remoteVersion < 90600 && owner
				&& strcmp(grantee->data, owner) == 0
				&& strcmp(grantor->data, owner) == 0)
			{
				found_owner_privs = true;

				/*
				 * For the owner, the default privilege level is ALL WITH
				 * GRANT OPTION.
				 */
				if (strcmp(privswgo->data, "ALL") != 0)
				{
					appendStringInfo(firstsql, "%sREVOKE ALL", prefix);
					if (subname)
						appendStringInfo(firstsql, "(%s)", subname);
					appendStringInfo(firstsql, " ON %s %s FROM %s;\n",
									  type, name, dbms_md_fmtId(grantee->data));
					if (privs->len > 0)
						appendStringInfo(firstsql,
										  "%sGRANT %s ON %s %s TO %s;\n",
										  prefix, privs->data, type, name,
										  dbms_md_fmtId(grantee->data));
					if (privswgo->len > 0)
						appendStringInfo(firstsql,
										  "%sGRANT %s ON %s %s TO %s WITH GRANT OPTION;\n",
										  prefix, privswgo->data, type, name,
										  dbms_md_fmtId(grantee->data));
				}
			}
			else
			{
				/*
				 * For systems prior to 9.6, we can assume we are starting
				 * from no privs at this point.
				 *
				 * For 9.6 and above, at this point we have issued REVOKE
				 * statements for all initial and default privileges which are
				 * no longer present on the object (as they were passed in as
				 * 'racls') and we can simply GRANT the rights which are in
				 * 'acls'.
				 */
				if (grantor->len > 0
					&& (!owner || strcmp(owner, grantor->data) != 0))
					appendStringInfo(secondsql, "SET SESSION AUTHORIZATION %s;\n",
									  dbms_md_fmtId(grantor->data));

				if (privs->len > 0)
				{
					appendStringInfo(secondsql, "%sGRANT %s ON %s %s TO ",
									  prefix, privs->data, type, name);
					if (grantee->len == 0)
						appendStringInfo(secondsql, "PUBLIC;\n");
					else if (strncmp(grantee->data, "group ",	 strlen("group ")) == 0)
						appendStringInfo(secondsql, "GROUP %s;\n",
										  dbms_md_fmtId(grantee->data + strlen("group ")));
					else
						appendStringInfo(secondsql, "%s;\n", dbms_md_fmtId(grantee->data));
				}
				if (privswgo->len > 0)
				{
					appendStringInfo(secondsql, "%sGRANT %s ON %s %s TO ",
									  prefix, privswgo->data, type, name);
					if (grantee->len == 0)
						appendStringInfoString(secondsql, "PUBLIC");
					else if (strncmp(grantee->data, "group ", strlen("group ")) == 0)
						appendStringInfo(secondsql, "GROUP %s",
										  dbms_md_fmtId(grantee->data + strlen("group ")));
					else
						appendStringInfoString(secondsql, dbms_md_fmtId(grantee->data));
					appendStringInfoString(secondsql, " WITH GRANT OPTION;\n");
				}

				if ((grantor->len > 0)
					&& (!owner || strcmp(owner, grantor->data) != 0))
					appendStringInfoString(secondsql, "RESET SESSION AUTHORIZATION;\n");
			}
		}
	}

	/*
	 * For systems prior to 9.6, if we didn't find any owner privs, the owner
	 * must have revoked 'em all.
	 *
	 * For 9.6 and above, we handle this through the 'racls'.  See above.
	 */
	if (remoteVersion < 90600 && !found_owner_privs && owner)
	{
		appendStringInfo(firstsql, "%sREVOKE ALL", prefix);
		if (subname)
			appendStringInfo(firstsql, "(%s)", subname);
		appendStringInfo(firstsql, " ON %s %s FROM %s;\n",
						  type, name, dbms_md_fmtId(owner));
	}

	destroyStringInfo(grantee);
	destroyStringInfo(grantor);
	destroyStringInfo(privs);
	destroyStringInfo(privswgo);
	pfree(grantee);
	pfree(grantor);
	pfree(privs);
	pfree(privswgo);

	appendStringInfo(sql, "%s%s", firstsql->data, secondsql->data);
	destroyStringInfo(firstsql);
	destroyStringInfo(secondsql);
	pfree(firstsql);
	pfree(secondsql);

	if (aclitems)
		pfree(aclitems);

	if (raclitems)
		pfree(raclitems);

	return ok;
}

/*
 * Build ALTER DEFAULT PRIVILEGES command(s) for single pg_default_acl entry.
 *
 *	type: the object type (TABLES, FUNCTIONS, etc)
 *	nspname: schema name, or NULL for global default privileges
 *	acls: the ACL string fetched from the database
 *	owner: username of privileges owner (will be passed through fmtId)
 *	remoteVersion: version of database
 *
 * Returns TRUE if okay, FALSE if could not parse the acl string.
 * The resulting commands (if any) are appended to the contents of 'sql'.
 */
bool
buildDefaultACLCommands(const char *type, const char *nspname,
						const char *acls, const char *racls,
						const char *initacls, const char *initracls,
						const char *owner,
						int remoteVersion,
						StringInfo sql)
{
	StringInfo prefix;

	prefix = createStringInfo();

	/*
	 * We incorporate the target role directly into the command, rather than
	 * playing around with SET ROLE or anything like that.  This is so that a
	 * permissions error leads to nothing happening, rather than changing
	 * default privileges for the wrong user.
	 */
	appendStringInfo(prefix, "ALTER DEFAULT PRIVILEGES FOR ROLE %s ",
					  dbms_md_fmtId(owner));
	if (nspname)
		appendStringInfo(prefix, "IN SCHEMA %s ", dbms_md_fmtId(nspname));

	if (strlen(initacls) != 0 || strlen(initracls) != 0)
	{
		appendStringInfo(sql, "SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\n");
		if (!buildACLCommands("", NULL, type, initacls, initracls, owner,
							  prefix->data, remoteVersion, sql))
		{
			destroyStringInfo(prefix);
			pfree(prefix);
			return false;
		}
		appendStringInfo(sql, "SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\n");
	}

	if (!buildACLCommands("", NULL, type, acls, racls, owner,
						  prefix->data, remoteVersion, sql))
	{
		destroyStringInfo(prefix);
		pfree(prefix);
		return false;
	}

	destroyStringInfo(prefix);
	pfree(prefix);

	return true;
}

/*
 * This will parse an aclitem string, having the general form
 *		username=privilegecodes/grantor
 * or
 *		group groupname=privilegecodes/grantor
 * (the "group" case occurs only with servers before 8.1).
 *
 * Returns true on success, false on parse error.  On success, the components
 * of the string are returned in the StringInfo parameters.
 *
 * The returned grantee string will be the dequoted username or groupname
 * (preceded with "group " in the latter case).  Note that a grant to PUBLIC
 * is represented by an empty grantee string.  The returned grantor is the
 * dequoted grantor name.  Privilege characters are decoded and split between
 * privileges with grant option (privswgo) and without (privs).
 *
 * Note: for cross-version compatibility, it's important to use ALL to
 * represent the privilege sets whenever appropriate.
 */
static bool
parseAclItem(const char *item, const char *type,
			 const char *name, const char *subname, int remoteVersion,
			 StringInfo grantee, StringInfo grantor,
			 StringInfo privs, StringInfo privswgo)
{
	char	   *buf;
	bool		all_with_go = true;
	bool		all_without_go = true;
	char	   *eqpos;
	char	   *slpos;
	char	   *pos;

	buf = strdup(item);
	if (!buf)
		return false;

	/* user or group name is string up to = */
	eqpos = copyAclUserName(grantee, buf);
	if (*eqpos != '=')
	{
		free(buf);
		return false;
	}

	/* grantor should appear after / */
	slpos = strchr(eqpos + 1, '/');
	if (slpos)
	{
		*slpos++ = '\0';
		slpos = copyAclUserName(grantor, slpos);
		if (*slpos != '\0')
		{
			free(buf);
			return false;
		}
	}
	else
	{
		free(buf);
		return false;
	}

	/* privilege codes */
#define CONVERT_PRIV(code, keywd) \
do { \
	if ((pos = strchr(eqpos + 1, code))) \
	{ \
		if (*(pos + 1) == '*') \
		{ \
			AddAcl(privswgo, keywd, subname); \
			all_without_go = false; \
		} \
		else \
		{ \
			AddAcl(privs, keywd, subname); \
			all_with_go = false; \
		} \
	} \
	else \
		all_with_go = all_without_go = false; \
} while (0)

	resetStringInfo(privs);
	resetStringInfo(privswgo);

	if (strcmp(type, "TABLE") == 0 || strcmp(type, "SEQUENCE") == 0 ||
		strcmp(type, "TABLES") == 0 || strcmp(type, "SEQUENCES") == 0)
	{
		CONVERT_PRIV('r', "SELECT");

		if (strcmp(type, "SEQUENCE") == 0 ||
			strcmp(type, "SEQUENCES") == 0)
			/* sequence only */
			CONVERT_PRIV('U', "USAGE");
		else
		{
			/* table only */
			CONVERT_PRIV('a', "INSERT");
			CONVERT_PRIV('x', "REFERENCES");
			/* rest are not applicable to columns */
			if (subname == NULL)
			{
				CONVERT_PRIV('d', "DELETE");
				CONVERT_PRIV('t', "TRIGGER");
				if (remoteVersion >= 80400)
					CONVERT_PRIV('D', "TRUNCATE");
			}
		}

		/* UPDATE */
		CONVERT_PRIV('w', "UPDATE");
	}
	else if (strcmp(type, "FUNCTION") == 0 ||
			 strcmp(type, "FUNCTIONS") == 0)
		CONVERT_PRIV('X', "EXECUTE");
	else if (strcmp(type, "PROCEDURE") == 0 ||
			 strcmp(type, "PROCEDURES") == 0)
		CONVERT_PRIV('X', "EXECUTE");
	else if (strcmp(type, "LANGUAGE") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "SCHEMA") == 0 ||
			 strcmp(type, "SCHEMAS") == 0)
	{
		CONVERT_PRIV('C', "CREATE");
		CONVERT_PRIV('U', "USAGE");
	}
	else if (strcmp(type, "DATABASE") == 0)
	{
		CONVERT_PRIV('C', "CREATE");
		CONVERT_PRIV('c', "CONNECT");
		CONVERT_PRIV('T', "TEMPORARY");
	}
	else if (strcmp(type, "TABLESPACE") == 0)
		CONVERT_PRIV('C', "CREATE");
	else if (strcmp(type, "TYPE") == 0 ||
			 strcmp(type, "TYPES") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "FOREIGN DATA WRAPPER") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "FOREIGN SERVER") == 0)
		CONVERT_PRIV('U', "USAGE");
	else if (strcmp(type, "FOREIGN TABLE") == 0)
		CONVERT_PRIV('r', "SELECT");
	else if (strcmp(type, "LARGE OBJECT") == 0)
	{
		CONVERT_PRIV('r', "SELECT");
		CONVERT_PRIV('w', "UPDATE");
	}
	else
		abort();

#undef CONVERT_PRIV

	if (all_with_go)
	{
		resetStringInfo(privs);
		printfStringInfo(privswgo, "ALL");
		if (subname)
			appendStringInfo(privswgo, "(%s)", subname);
	}
	else if (all_without_go)
	{
		resetStringInfo(privswgo);
		printfStringInfo(privs, "ALL");
		if (subname)
			appendStringInfo(privs, "(%s)", subname);
	}

	free(buf);

	return true;
}

/*
 * Transfer a user or group name starting at *input into the output buffer,
 * dequoting if needed.  Returns a pointer to just past the input name.
 * The name is taken to end at an unquoted '=' or end of string.
 */
static char *
copyAclUserName(StringInfo output, char *input)
{
	resetStringInfo(output);

	while (*input && *input != '=')
	{
		/*
		 * If user name isn't quoted, then just add it to the output buffer
		 */
		if (*input != '"')
			appendStringInfoChar(output, *input++);
		else
		{
			/* Otherwise, it's a quoted username */
			input++;
			/* Loop until we come across an unescaped quote */
			while (!(*input == '"' && *(input + 1) != '"'))
			{
				if (*input == '\0')
					return input;	/* really a syntax error... */

				/*
				 * Quoting convention is to escape " as "".  Keep this code in
				 * sync with putid() in backend's acl.c.
				 */
				if (*input == '"' && *(input + 1) == '"')
					input++;
				appendStringInfoChar(output, *input++);
			}
			input++;
		}
	}
	return input;
}

/*
 * Append a privilege keyword to a keyword list, inserting comma if needed.
 */
static void
AddAcl(StringInfo aclbuf, const char *keyword, const char *subname)
{
	if (aclbuf->len > 0)
		appendStringInfoChar(aclbuf, ',');
	appendStringInfoString(aclbuf, keyword);
	if (subname)
		appendStringInfo(aclbuf, "(%s)", subname);
}


/*
 * buildShSecLabelQuery
 *
 * Build a query to retrieve security labels for a shared object.
 */
#if 0 
void
buildShSecLabelQuery(PGconn *conn, const char *catalog_name, uint32 objectId,
					 StringInfo sql)
{
	appendStringInfo(sql,
					  "SELECT provider, label FROM pg_catalog.pg_shseclabel "
					  "WHERE classoid = '%s'::pg_catalog.regclass AND "
					  "objoid = %u", catalog_name, objectId);
}

/*
 * emitShSecLabels
 *
 * Format security label data retrieved by the query generated in
 * buildShSecLabelQuery.
 */

void
emitShSecLabels(PGconn *conn, PGresult *res, StringInfo buffer,
				const char *target, const char *objname)
{
	int			i;

	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *provider = PQgetvalue(res, i, 0);
		char	   *label = PQgetvalue(res, i, 1);

		/* must use fmtId result before calling it again */
		appendStringInfo(buffer,
						  "SECURITY LABEL FOR %s ON %s",
						  fmtId_internal(provider, true), target);

		if (pg_strcasecmp(target, "LANGUAGE") == 0)
			appendStringInfo(buffer,
						  " %s IS ",
						  fmtId_internal(objname, true));
		else
			appendStringInfo(buffer,
						  " %s IS ",
						  fmtId(objname));
		appendStringLiteralConn(buffer, label, conn);
		appendStringInfo(buffer, ";\n");
	}
}
#endif

/*
 * buildACLQueries
 *
 * Build the subqueries to extract out the correct set of ACLs to be
 * GRANT'd and REVOKE'd for the specific kind of object, accounting for any
 * initial privileges (from pg_init_privs) and based on if we are in binary
 * upgrade mode or not.
 *
 * Also builds subqueries to extract out the set of ACLs to go from the object
 * default privileges to the privileges in pg_init_privs, if we are in binary
 * upgrade mode, so that those privileges can be set up and recorded in the new
 * cluster before the regular privileges are added on top of those.
 */
void
buildACLQueries(StringInfo acl_subquery, StringInfo racl_subquery,
				StringInfo init_acl_subquery, StringInfo init_racl_subquery,
				const char *acl_column, const char *acl_owner,
				const char *obj_kind, bool binary_upgrade)
{
	/*
	 * To get the delta from what the permissions were at creation time
	 * (either initdb or CREATE EXTENSION) vs. what they are now, we have to
	 * look at two things:
	 *
	 * What privileges have been added, which we calculate by extracting all
	 * the current privileges (using the set of default privileges for the
	 * object type if current privileges are NULL) and then removing those
	 * which existed at creation time (again, using the set of default
	 * privileges for the object type if there were no creation time
	 * privileges).
	 *
	 * What privileges have been removed, which we calculate by extracting the
	 * privileges as they were at creation time (or the default privileges, as
	 * above), and then removing the current privileges (or the default
	 * privileges, if current privileges are NULL).
	 *
	 * As a good cross-check, both directions of these checks should result in
	 * the empty set if both the current ACL and the initial privs are NULL
	 * (meaning, in practice, that the default ACLs were there at init time
	 * and is what the current privileges are).
	 *
	 * We always perform this delta on all ACLs and expect that by the time
	 * these are run the initial privileges will be in place, even in a binary
	 * upgrade situation (see below).
	 */
	printfStringInfo(acl_subquery, "(SELECT pg_catalog.array_agg(acl) FROM "
					  "(SELECT pg_catalog.unnest(coalesce(%s,pg_catalog.acldefault(%s,%s))) AS acl "
					  "EXCEPT "
					  "SELECT pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault(%s,%s)))) as foo)",
					  acl_column,
					  obj_kind,
					  acl_owner,
					  obj_kind,
					  acl_owner);

	printfStringInfo(racl_subquery, "(SELECT pg_catalog.array_agg(acl) FROM "
					  "(SELECT pg_catalog.unnest(coalesce(pip.initprivs,pg_catalog.acldefault(%s,%s))) AS acl "
					  "EXCEPT "
					  "SELECT pg_catalog.unnest(coalesce(%s,pg_catalog.acldefault(%s,%s)))) as foo)",
					  obj_kind,
					  acl_owner,
					  acl_column,
					  obj_kind,
					  acl_owner);

	/*
	 * In binary upgrade mode we don't run the extension script but instead
	 * dump out the objects independently and then recreate them.  To preserve
	 * the initial privileges which were set on extension objects, we need to
	 * grab the set of GRANT and REVOKE commands necessary to get from the
	 * default privileges of an object to the initial privileges as recorded
	 * in pg_init_privs.
	 *
	 * These will then be run ahead of the regular ACL commands, which were
	 * calculated using the queries above, inside of a block which sets a flag
	 * to indicate that the backend should record the results of these GRANT
	 * and REVOKE statements into pg_init_privs.  This is how we preserve the
	 * contents of that catalog across binary upgrades.
	 */
	if (binary_upgrade)
	{
		printfStringInfo(init_acl_subquery,
						  "CASE WHEN privtype = 'e' THEN "
						  "(SELECT pg_catalog.array_agg(acl) FROM "
						  "(SELECT pg_catalog.unnest(pip.initprivs) AS acl "
						  "EXCEPT "
						  "SELECT pg_catalog.unnest(pg_catalog.acldefault(%s,%s))) as foo) END",
						  obj_kind,
						  acl_owner);

		printfStringInfo(init_racl_subquery,
						  "CASE WHEN privtype = 'e' THEN "
						  "(SELECT pg_catalog.array_agg(acl) FROM "
						  "(SELECT pg_catalog.unnest(pg_catalog.acldefault(%s,%s)) AS acl "
						  "EXCEPT "
						  "SELECT pg_catalog.unnest(pip.initprivs)) as foo) END",
						  obj_kind,
						  acl_owner);
	}
	else
	{
		printfStringInfo(init_acl_subquery, "NULL");
		printfStringInfo(init_racl_subquery, "NULL");
	}
}

/*
 * Parse a --section=foo command line argument.
 *
 * Set or update the bitmask in *dumpSections according to arg.
 * dumpSections is initialised as DUMP_UNSECTIONED by pg_dump and
 * pg_restore so they can know if this has even been called.
 */
void
set_dump_section(const char *arg, int *dumpSections)
{
	/* if this is the first call, clear all the bits */
	if (*dumpSections == DUMP_UNSECTIONED)
		*dumpSections = 0;

	if (strcmp(arg, "pre-data") == 0)
		*dumpSections |= DUMP_PRE_DATA;
	else if (strcmp(arg, "data") == 0)
		*dumpSections |= DUMP_DATA;
	else if (strcmp(arg, "post-data") == 0)
		*dumpSections |= DUMP_POST_DATA;
	else
	{
		elog(ERROR, _("unrecognized section name: \"%s\"\n"), arg);		
	}
}


/*
 * Write a printf-style message to stderr.
 *
 * The program name is prepended, if "progname" has been set.
 * Also, if modulename isn't NULL, that's included too.
 * Note that we'll try to translate the modulename and the fmt string.
 */
void
write_msg(const char *modulename, const char *fmt,...)
{
	va_list		ap;
	
	if (NULL == modulename)
		return ;
	va_start(ap, fmt);
	vwrite_msg(modulename, fmt, ap);
	va_end(ap);
}

/*
 * As write_msg, but pass a va_list not variable arguments.
 */
void
vwrite_msg(const char *modulename, const char *fmt, va_list ap)
{	
	elog(DEBUG3, _(fmt), ap);
}

/*
 * Append an OID to the list.
 */
void
dbms_md_oid_list_append(MemoryContext mem_ctx, DbmsMdOidList *list, Oid val)
{
	DbmsMdOidListCell *cell;

	cell = (DbmsMdOidListCell *) dbms_md_malloc0(mem_ctx, sizeof(DbmsMdOidListCell));
	cell->next = NULL;
	cell->val = val;

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}

void
dbms_md_oid_list_clear(DbmsMdOidList *list)
{
	DbmsMdOidListCell *cell = NULL;
	DbmsMdOidListCell *head = NULL;

	if (NULL == list)
		return ;

	if (NULL ==list->head)
		return ;
	
	head = list->head;
	while (NULL != head)
	{
		cell = head->next;
		dbms_md_free(head);
		head = cell;
	}

	list->head = list->tail = NULL;
}

/*
 * Is OID present in the list?
 */
bool
dbms_md_oid_list_member(DbmsMdOidList *list, Oid val)
{
	DbmsMdOidListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (cell->val == val)
			return true;
	}
	return false;
}

/*
 * Append a string to the list.
 *
 * The given string is copied, so it need not survive past the call.
 */
void
dbms_md_string_list_append(MemoryContext mem_ctx, DbmsMdStringList *list, const char *val)
{
	DbmsMdStringListCell *cell;

	cell = (DbmsMdStringListCell *)
		dbms_md_malloc0(mem_ctx, offsetof(DbmsMdStringListCell, val) + strlen(val) + 1);

	cell->next = NULL;
	cell->touched = false;
	strcpy(cell->val, val);

	if (list->tail)
		list->tail->next = cell;
	else
		list->head = cell;
	list->tail = cell;
}

void
dbms_md_string_list_clear(DbmsMdStringList *list)
{
	DbmsMdStringListCell *cell = NULL;
	DbmsMdStringListCell *head = NULL;

	if (NULL == list)
		return ;

	if (NULL == list->head)
		return ;

	head = list->head;
	while (NULL != head)
	{
		cell = head->next;
		dbms_md_free(head);
		head = cell;
	}	

	list->head = list->tail = NULL;
}

/*
 * Is string present in the list?
 *
 * If found, the "touched" field of the first match is set true.
 */
bool
dbms_md_string_list_member(DbmsMdStringList *list, const char *val)
{
	DbmsMdStringListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (strcmp(cell->val, val) == 0)
		{
			cell->touched = true;
			return true;
		}
	}
	return false;
}

/*
 * Find first not-touched list entry, if there is one.
 */
const char *
dbms_md_string_list_not_touched(DbmsMdStringList *list)
{
	DbmsMdStringListCell *cell;

	for (cell = list->head; cell; cell = cell->next)
	{
		if (!cell->touched)
			return cell->val;
	}
	return NULL;
}

