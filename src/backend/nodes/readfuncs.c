/*-------------------------------------------------------------------------
 *
 * readfuncs.c
 *	  Reader functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/readfuncs.c
 *
 * NOTES
 *	  Path nodes do not have any readfuncs support, because we never
 *	  have occasion to read them in.  (There was once code here that
 *	  claimed to read them, but it was broken as well as unused.)  We
 *	  never read executor state trees, either.
 *
 *	  Parse location fields are written out by outfuncs.c, but only for
 *	  possible debugging use.  When reading a location field, we discard
 *	  the stored value and set the location field to -1 (ie, "unknown").
 *	  This is because nodes coming from a stored rule should not be thought
 *	  to have a known location in the current query's text.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#ifdef PGXC
#include "access/htup.h"
#endif
#ifdef XCP
#include "fmgr.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "nodes/plannodes.h"
#include "pgxc/execRemote.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif


#ifdef __AUDIT__
#include "audit/audit.h"
#endif

#ifdef __OPENTENBASE__
#include "catalog/pg_constraint_fn.h"
#include "commands/defrem.h"
#include "catalog/pg_am.h"
#endif
#include "foreign/foreign.h"

/*
 * When we sending query plans between nodes we need to send OIDs of various
 * objects - relations, data types, functions, etc.
 * On different nodes OIDs of these objects may differ, so we need to send an
 * identifier, depending on object type, allowing to lookup OID on target node.
 * On the other hand we want to save space when storing rules, or in other cases
 * when we need to encode and decode nodes on the same node.
 * For now default format is not portable, as it is in original Postgres code.
 * Later we may want to add extra parameter in stringToNode() function
 */
static bool portable_input = false;
bool
set_portable_input(bool value)
{
	bool old_portable_input = portable_input;
	portable_input = value;
	return old_portable_input;
}
#endif /* XCP */

bool skip_read_extern_fields = false;

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */

/* A few guys need only local_node */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	nodeTypeName *local_node = makeNode(nodeTypeName)

/* And a few guys need only the pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	char	   *token;		\
	int			length

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)			\
	READ_LOCALS_NO_FIELDS(nodeTypeName);	\
	READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoui(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_LONG_UINT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atouli(token)


#ifdef XCP
/* Read a long integer field (anything written as ":fldname %ld") */
#define READ_LONG_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atol(token)
#endif

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#ifdef XCP
#define READ_OID_FIELD(fldname) \
	(AssertMacro(!portable_input), 	/* only allow to read OIDs within a node */ \
	 token = pg_strtok(&length),	/* skip :fldname */ \
	 token = pg_strtok(&length),	/* get field value */ \
	 local_node->fldname = atooid(token))
#else
#define READ_OID_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atooid(token)
#endif

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	/* avoid overhead of calling debackslash() for one char */ \
	local_node->fldname = (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtobool(token)

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = -1	/* set field to "unknown" */

/* Read a Node field */
#define READ_NODE_FIELD(fldname) \
	do { \
		token = pg_strtok(&length);		/* skip :fldname */ \
		(void) token;				/* in case not used elsewhere */ \
		local_node->fldname = nodeRead(NULL, 0); \
	} while (0)

/* Read a Node field */
#define READ_NODE_FIELD_WITH_NODE(param_node, fldname) \
	do { \
		token = pg_strtok(&length);		/* skip :fldname */ \
		(void) token;				/* in case not used elsewhere */ \
		(param_node)->fldname = nodeRead(NULL, 0); \
	} while (0)

/* Read a bitmapset field */
#define READ_BITMAPSET_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = _readBitmapset()

#ifdef XCP
/*
 * Macros to read an identifier and lookup the OID
 * The identifier depends on object type.
 */
#define NSP_OID(nspname) LookupNamespaceNoError(nspname)

/* Read relation identifier and lookup the OID */
#define READ_RELID_INTERNAL(relid, warn) \
	do { \
		char	   *nspname; /* namespace name */ \
		char	   *relname; /* relation name */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get relname */ \
		relname = nullable_string(token, length); \
		if (relname) \
		{ \
			relid = get_relname_relid(relname, \
													NSP_OID(nspname)); \
			if ((!OidIsValid(relid)) && (warn)) \
				elog(WARNING, "could not find OID for relation %s.%s", nspname,\
						relname); \
		} \
		else \
			relid = InvalidOid; \
	} while (0)

#define READ_RELID_FIELD_NOWARN(fldname) \
	do { \
		Oid relid; \
		token = pg_strtok(&length);		/* skip :fldname */ \
		READ_RELID_INTERNAL(relid, false); \
		local_node->fldname = relid; \
	} while (0)

#define READ_RELID_FIELD(fldname) \
	do { \
		Oid relid; \
		token = pg_strtok(&length);		/* skip :fldname */ \
		READ_RELID_INTERNAL(relid, true); \
		local_node->fldname = relid; \
	} while (0)

#define READ_RELID_LIST_FIELD(fldname) \
	do { \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); 	/* skip '(' */ \
		if (length > 0 ) \
		{ \
			Assert(token[0] == '('); \
			for (;;) \
			{ \
				Oid relid; \
				READ_RELID_INTERNAL(relid, true); \
				local_node->fldname = lappend_oid(local_node->fldname, relid); \
				token = pg_strtok(&length); \
				if (token[0] == ')') \
				break; \
			} \
		} \
		else \
			local_node->fldname = NIL; \
	} while (0)

/* Read foreign server identifier and lookup the OID */
#define READ_SERVERID_INTERNAL(serverid, warn)                                                     \
	do                                                                                             \
	{                                                                                              \
		char *servername = NULL; /* servername name */                                             \
		token = pg_strtok(&length);                                                                \
		servername = nullable_string(token, length);                                               \
		if (servername)                                                                            \
		{                                                                                          \
			serverid = get_foreign_server_oid(servername, warn);                                   \
			if ((!OidIsValid(serverid)) && (warn))                                                 \
				elog(WARNING, "could not find OID for server %s", servername);                     \
		}                                                                                          \
		else                                                                                       \
			serverid = InvalidOid;                                                                 \
	} while (0)

#define READ_SERVERID_FIELD(fldname)                                                               \
	do                                                                                             \
	{                                                                                              \
		Oid serverid;                                                                              \
		token = pg_strtok(&length); /* skip :fldname */                                            \
		READ_SERVERID_INTERNAL(serverid, true);                                                    \
		local_node->fldname = serverid;                                                            \
	} while (0)

/* Read data type identifier and lookup the OID */
#define READ_TYPID_INTERNAL(typid) \
	do { \
		char	   *nspname; /* namespace name */ \
		char	   *typname; /* data type name */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get typname */ \
		typname = nullable_string(token, length); \
		if (typname) \
		{ \
			typid = get_typname_typid(typname, \
										NSP_OID(nspname)); \
			if (!OidIsValid((typid))) \
				elog(WARNING, "could not find OID for type %s.%s", nspname,\
						typname); \
		} \
		else \
			typid = InvalidOid; \
	} while (0)

#define READ_TYPID_FIELD(fldname) \
	do { \
		Oid typid; \
		token = pg_strtok(&length);		/* skip :fldname */ \
		READ_TYPID_INTERNAL(typid); \
		local_node->fldname = typid; \
	} while (0)

#define READ_TYPID_LIST_FIELD(fldname) \
	do { \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); 	/* skip '(' */ \
		if (length > 0 ) \
		{ \
			Assert(token[0] == '('); \
			for (;;) \
			{ \
				Oid typid; \
				READ_TYPID_INTERNAL(typid); \
				local_node->fldname = lappend_oid(local_node->fldname, typid); \
				token = pg_strtok(&length); \
				if (token[0] == ')') \
				break; \
			} \
		} \
		else \
			local_node->fldname = NIL; \
	} while (0)

/* Read function identifier and lookup the OID */
#define READ_FUNCID_FIELD(fldname) \
	do { \
		char       *nspname; /* namespace name */ \
		char       *funcname; /* function name */ \
		int 		nargs; /* number of arguments */ \
		Oid		   *argtypes; /* argument types */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get funcname */ \
		funcname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get nargs */ \
		nargs = atoi(token); \
		if (funcname) \
		{ \
			int	i; \
			argtypes = palloc(nargs * sizeof(Oid)); \
			for (i = 0; i < nargs; i++) \
			{ \
				char *typnspname; /* argument type namespace */ \
				char *typname; /* argument type name */ \
				token = pg_strtok(&length); /* get type nspname */ \
				typnspname = nullable_string(token, length); \
				token = pg_strtok(&length); /* get type name */ \
				typname = nullable_string(token, length); \
				argtypes[i] = get_typname_typid(typname, \
												NSP_OID(typnspname)); \
			} \
			local_node->fldname = get_funcid(funcname, \
											 buildoidvector(argtypes, nargs), \
											 NSP_OID(nspname)); \
		} \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

/* Read operator identifier and lookup the OID */
#define READ_OPERID_FIELD(fldname) \
	do { \
		char       *nspname; /* namespace name */ \
		char       *oprname; /* operator name */ \
		char	   *leftnspname; /* left type namespace */ \
		char	   *leftname; /* left type name */ \
		Oid			oprleft; /* left type */ \
		char	   *rightnspname; /* right type namespace */ \
		char	   *rightname; /* right type name */ \
		Oid			oprright; /* right type */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get operator name */ \
		oprname = nullable_string(token, length); \
		token = pg_strtok(&length); /* left type namespace */ \
		leftnspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* left type name */ \
		leftname = nullable_string(token, length); \
		token = pg_strtok(&length); /* right type namespace */ \
		rightnspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* right type name */ \
		rightname = nullable_string(token, length); \
		if (oprname) \
		{ \
			if (leftname) \
				oprleft = get_typname_typid(leftname, \
											NSP_OID(leftnspname)); \
			else \
				oprleft = InvalidOid; \
			if (rightname) \
				oprright = get_typname_typid(rightname, \
											 NSP_OID(rightnspname)); \
			else \
				oprright = InvalidOid; \
			local_node->fldname = get_operid(oprname, \
											 oprleft, \
											 oprright, \
											 NSP_OID(nspname)); \
		} \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

/* Read collation identifier and lookup the OID */
#define READ_COLLID_FIELD(fldname) \
	do { \
		char       *nspname; /* namespace name */ \
		char       *collname; /* collation name */ \
		int 		collencoding; /* collation encoding */ \
		token = pg_strtok(&length);		/* skip :fldname */ \
		token = pg_strtok(&length); /* get nspname */ \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get collname */ \
		collname = nullable_string(token, length); \
		token = pg_strtok(&length); /* get collencoding */ \
		collencoding = atoi(token); \
		if (collname) \
			local_node->fldname = get_collid(collname, \
											 collencoding, \
											 NSP_OID(nspname)); \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

#define READ_OPCLASS_FIELD(fldname) \
	do { \
		char *nspname; \
		char *name; \
		token = pg_strtok(&length);		 \
		token = pg_strtok(&length); \
		nspname = nullable_string(token, length); \
		token = pg_strtok(&length);  \
		name = nullable_string(token, length); \
		if (nspname) \
		{\
			List *opclassname = list_make1(makeString(nspname)); \
			lappend(opclassname, makeString(name)); \
			local_node->fldname = get_opclass_oid(BTREE_AM_OID, \
												   opclassname, false); \
		} \
		else \
			local_node->fldname = InvalidOid; \
	} while (0)

#define READ_CONSTRAINT_FIELD(fldname) \
	do { \
		char *cons_name; \
		char *rel_namespace; \
		char *rel_name; \
		token = pg_strtok(&length);		 \
		token = pg_strtok(&length); \
		cons_name = nullable_string(token, length); \
		token = pg_strtok(&length); \
		rel_namespace = nullable_string(token, length); \
		token = pg_strtok(&length); \
		rel_name = nullable_string(token, length); \
		if (cons_name && rel_name) \
		{\
			Oid nsp_oid = get_namespaceid(rel_namespace); \
			Oid relid =  get_relname_relid(rel_name, nsp_oid); \
			local_node->fldname = get_relation_constraint_oid(relid, cons_name, false); \
		}\
		else \
			local_node->fldname = InvalidOid; \
	} while (0) 
#endif

/* Read an attribute number array */
#define READ_ATTRNUMBER_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readAttrNumberCols(len);

/* Read an oid array */
#define READ_OID_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readOidCols(len);

/* Read an int array */
#define READ_INT_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readIntCols(len);

/* Read a bool array */
#define READ_BOOL_ARRAY(fldname, len) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	local_node->fldname = readBoolCols(len);

/* Routine exit */
#define READ_DONE() \
	return local_node


/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))

#define atouli(x) ((uint64) strtoul((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))

#ifdef XCP
static Datum scanDatum(Oid typid, int typmod);
#endif

#define IF_EXIST(fieldname)                              \
	if ((NULL != (token = pg_strtok_change(&length, false))) && \
		(0 == strncmp(token, ":" CppAsString(fieldname), strlen(":" CppAsString(fieldname)))))

/* Skip a field with fldname and value */
#define SKIP_SCALAR_FIELD(fieldname)	\
	do {								\
		IF_EXIST(fieldname)				\
		{								\
			token = pg_strtok(&length);	\
			token = pg_strtok(&length);	\
		}								\
	} while (0)

#define SKIP_NODE_FIELD(fieldname)		\
	do {								\
		IF_EXIST(fieldname)				\
		{								\
			token = pg_strtok(&length);	\
			(void) token;				\
			(void) nodeRead(NULL, 0);	\
		}								\
	} while (0)

#define SKIP_BITMAPSET_FIELD(fieldname)	\
	do {								\
		IF_EXIST(fieldname)				\
		{								\
			token = pg_strtok(&length);	\
			(void) token;				\
			(void) _readBitmapset();	\
		}								\
	} while (0)

/*
 * _readBitmapset
 */
static Bitmapset *
_readBitmapset(void)
{
	Bitmapset  *result = NULL;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != '(')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	token = pg_strtok(&length);
	if (token == NULL)
		elog(ERROR, "incomplete Bitmapset structure");
	if (length != 1 || token[0] != 'b')
		elog(ERROR, "unrecognized token: \"%.*s\"", length, token);

	for (;;)
	{
		int			val;
		char	   *endptr;

		token = pg_strtok(&length);
		if (token == NULL)
			elog(ERROR, "unterminated Bitmapset structure");
		if (length == 1 && token[0] == ')')
			break;
		val = (int) strtol(token, &endptr, 10);
		if (endptr != token + length)
			elog(ERROR, "unrecognized integer: \"%.*s\"", length, token);
		result = bms_add_member(result, val);
	}

	return result;
}

/*
 * for use by extensions which define extensible nodes
 */
Bitmapset *
readBitmapset(void)
{
	return _readBitmapset();
}

/*
 * _readQuery
 */
static Query *
_readQuery(void)
{
	READ_LOCALS(Query);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(querySource, QuerySource);
	local_node->queryId = 0;	/* not saved in output format */
	READ_BOOL_FIELD(canSetTag);
	READ_NODE_FIELD(utilityStmt);
	READ_INT_FIELD(resultRelation);
	READ_BOOL_FIELD(hasAggs);
	READ_BOOL_FIELD(hasWindowFuncs);
	READ_BOOL_FIELD(hasTargetSRFs);
	READ_BOOL_FIELD(hasSubLinks);
	READ_BOOL_FIELD(hasDistinctOn);
	READ_BOOL_FIELD(hasRecursive);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(hasForUpdate);
	READ_BOOL_FIELD(hasRowSecurity);
	READ_NODE_FIELD(cteList);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(jointree);
	READ_NODE_FIELD(targetList);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_NODE_FIELD(onConflict);
	READ_NODE_FIELD(returningList);
	READ_NODE_FIELD(groupClause);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(havingQual);
	READ_NODE_FIELD(windowClause);
	READ_NODE_FIELD(distinctClause);
	READ_NODE_FIELD(sortClause);
	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	IF_EXIST(limitOption)
	{
		READ_ENUM_FIELD(limitOption, LimitOption);
	}
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(setOperations);
	READ_NODE_FIELD(constraintDeps);
	IF_EXIST(connectByExpr)
	{
		READ_NODE_FIELD(connectByExpr);
	}
	IF_EXIST(hasRowNumExpr)
	{
		READ_BOOL_FIELD(hasRowNumExpr);
	}
	/* withCheckOptions intentionally omitted, see comment in parsenodes.h */
	READ_LOCATION_FIELD(stmt_location);
	READ_LOCATION_FIELD(stmt_len);
	/* begin ora_compatible */
	IF_EXIST(isall)
	{
		READ_BOOL_FIELD(isall);
	}
	IF_EXIST(multi_inserts)
	{
		READ_NODE_FIELD(multi_inserts);
	}
	/* end ora_compatible */

#ifdef _PG_ORCL_
	IF_EXIST(with_funcs)
	{
		READ_NODE_FIELD(with_funcs);
	}
#endif

	READ_DONE();
}

/*
 * _readNotifyStmt
 */
static NotifyStmt *
_readNotifyStmt(void)
{
	READ_LOCALS(NotifyStmt);

	READ_STRING_FIELD(conditionname);
	READ_STRING_FIELD(payload);

	READ_DONE();
}

/*
 * _readDeclareCursorStmt
 */
static DeclareCursorStmt *
_readDeclareCursorStmt(void)
{
	READ_LOCALS(DeclareCursorStmt);

	READ_STRING_FIELD(portalname);
	READ_INT_FIELD(options);
	READ_NODE_FIELD(query);

	READ_DONE();
}

/*
 * _readWithCheckOption
 */
static WithCheckOption *
_readWithCheckOption(void)
{
	READ_LOCALS(WithCheckOption);

	READ_ENUM_FIELD(kind, WCOKind);
	READ_STRING_FIELD(relname);
	READ_STRING_FIELD(polname);
	READ_NODE_FIELD(qual);
	READ_BOOL_FIELD(cascaded);

	READ_DONE();
}

/*
 * _readSortGroupClause
 */
static SortGroupClause *
_readSortGroupClause(void)
{
	READ_LOCALS(SortGroupClause);

	READ_UINT_FIELD(tleSortGroupRef);
#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(eqop);
	else
#endif
	READ_OID_FIELD(eqop);
#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(sortop);
	else
#endif
	READ_OID_FIELD(sortop);
	READ_BOOL_FIELD(nulls_first);
	READ_BOOL_FIELD(hashable);

	READ_DONE();
}

/*
 * _readGroupingSet
 */
static GroupingSet *
_readGroupingSet(void)
{
	READ_LOCALS(GroupingSet);

	READ_ENUM_FIELD(kind, GroupingSetKind);
	READ_NODE_FIELD(content);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readWindowClause
 */
static WindowClause *
_readWindowClause(void)
{
	READ_LOCALS(WindowClause);

	READ_STRING_FIELD(name);
	READ_STRING_FIELD(refname);
	READ_NODE_FIELD(partitionClause);
	READ_NODE_FIELD(orderClause);
	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	IF_EXIST(startInRangeFunc)
	{
		if (portable_input)
		{
			READ_FUNCID_FIELD(startInRangeFunc);
			READ_FUNCID_FIELD(endInRangeFunc);
			READ_COLLID_FIELD(inRangeColl);
		}
		else
		{
			READ_OID_FIELD(startInRangeFunc);
			READ_OID_FIELD(endInRangeFunc);
			READ_OID_FIELD(inRangeColl);
		}
		READ_BOOL_FIELD(inRangeAsc);
		READ_BOOL_FIELD(inRangeNullsFirst);
	}
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(copiedOrder);
	IF_EXIST(start_contain_vars)
	{
		READ_BOOL_FIELD(start_contain_vars);
		READ_BOOL_FIELD(end_contain_vars);
	}

	READ_DONE();
}

/*
 * _readRowMarkClause
 */
static RowMarkClause *
_readRowMarkClause(void)
{
	READ_LOCALS(RowMarkClause);

	READ_UINT_FIELD(rti);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	IF_EXIST(waitTimeout)
	{
		READ_INT_FIELD(waitTimeout);
	}
	READ_BOOL_FIELD(pushedDown);

	READ_DONE();
}

/*
 * _readCommonTableExpr
 */
static CommonTableExpr *
_readCommonTableExpr(void)
{
	READ_LOCALS(CommonTableExpr);
	READ_STRING_FIELD(ctename);
	READ_NODE_FIELD(aliascolnames);
	IF_EXIST(ctematerialized)
	{
		READ_ENUM_FIELD(ctematerialized, CTEMaterialize);
	}
	READ_NODE_FIELD(ctequery);
	READ_LOCATION_FIELD(location);
	READ_BOOL_FIELD(cterecursive);
	READ_INT_FIELD(cterefcount);
	READ_NODE_FIELD(ctecolnames);
	READ_NODE_FIELD(ctecoltypes);
	READ_NODE_FIELD(ctecoltypmods);
	READ_NODE_FIELD(ctecolcollations);

	READ_DONE();
}

/*
 * _readSetOperationStmt
 */
static SetOperationStmt *
_readSetOperationStmt(void)
{
	READ_LOCALS(SetOperationStmt);

	READ_ENUM_FIELD(op, SetOperation);
	READ_BOOL_FIELD(all);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(colTypes);
	READ_NODE_FIELD(colTypmods);
	READ_NODE_FIELD(colCollations);
	READ_NODE_FIELD(groupClauses);

	READ_DONE();
}


/*
 *	Stuff from primnodes.h.
 */

static Alias *
_readAlias(void)
{
	READ_LOCALS(Alias);

	READ_STRING_FIELD(aliasname);
	READ_NODE_FIELD(colnames);

	READ_DONE();
}

static RangeVar *
_readRangeVar(void)
{
	READ_LOCALS(RangeVar);

	local_node->catalogname = NULL; /* not currently saved in output format */

	READ_STRING_FIELD(schemaname);
	READ_STRING_FIELD(relname);
	READ_BOOL_FIELD(inh);
	READ_CHAR_FIELD(relpersistence);
	READ_NODE_FIELD(alias);
	READ_LOCATION_FIELD(location);
#ifdef __STORAGE_SCALABLE__
	READ_STRING_FIELD(pubname);
#endif

	IF_EXIST(childtablename)
	{
		READ_STRING_FIELD(childtablename);
	}

	READ_DONE();
}

/*
 * _readTableFunc
 */
static TableFunc *
_readTableFunc(void)
{
	READ_LOCALS(TableFunc);

	READ_NODE_FIELD(ns_uris);
	READ_NODE_FIELD(ns_names);
	READ_NODE_FIELD(docexpr);
	READ_NODE_FIELD(rowexpr);
	READ_NODE_FIELD(colnames);
	READ_NODE_FIELD(coltypes);
	READ_NODE_FIELD(coltypmods);
	READ_NODE_FIELD(colcollations);
	READ_NODE_FIELD(colexprs);
	READ_NODE_FIELD(coldefexprs);
	READ_BITMAPSET_FIELD(notnulls);
	READ_INT_FIELD(ordinalitycol);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static IntoClause *
_readIntoClause(void)
{
	READ_LOCALS(IntoClause);

	READ_NODE_FIELD(rel);
	READ_NODE_FIELD(colNames);
	READ_NODE_FIELD(options);
	READ_ENUM_FIELD(onCommit, OnCommitAction);
	READ_STRING_FIELD(tableSpaceName);
	READ_NODE_FIELD(viewQuery);
	READ_BOOL_FIELD(skipData);

	READ_DONE();
}

/*
 * _readVar
 */
static Var *
_readVar(void)
{
	READ_LOCALS(Var);

	READ_INT_FIELD(varno);
	READ_INT_FIELD(varattno);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(vartype);
	else
#endif
	READ_OID_FIELD(vartype);
	READ_INT_FIELD(vartypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(varcollid);
	else
#endif
	READ_OID_FIELD(varcollid);
	READ_UINT_FIELD(varlevelsup);
	READ_UINT_FIELD(varnoold);
	READ_INT_FIELD(varoattno);
	READ_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	READ_UINT_FIELD(varrelid);
	READ_INT_FIELD(varrelcolid);
	READ_UINT_FIELD(varseq);
#endif
	READ_DONE();
}

/*
 * _readConst
 */
static Const *
_readConst(void)
{
	READ_LOCALS(Const);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(consttype);
	else
#endif
	READ_OID_FIELD(consttype);
	READ_INT_FIELD(consttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(constcollid);
	else
#endif
	READ_OID_FIELD(constcollid);
	READ_INT_FIELD(constlen);
	READ_BOOL_FIELD(constbyval);
	READ_BOOL_FIELD(constisnull);
	READ_LOCATION_FIELD(location);

	token = pg_strtok(&length); /* skip :constvalue */
	if (local_node->constisnull)
		token = pg_strtok(&length); /* skip "<>" */
	else
#ifdef XCP
		if (portable_input)
			local_node->constvalue = scanDatum(local_node->consttype,
											   local_node->consttypmod);
		else
#endif
		local_node->constvalue = readDatum(local_node->constbyval);

	READ_DONE();
}

/*
 * _readParam
 */
static Param *
_readParam(void)
{
	READ_LOCALS(Param);

	READ_ENUM_FIELD(paramkind, ParamKind);
	READ_INT_FIELD(paramid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(paramtype);
	else
#endif
	READ_OID_FIELD(paramtype);
	READ_INT_FIELD(paramtypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(paramcollid);
	else
#endif
	READ_OID_FIELD(paramcollid);
	READ_LOCATION_FIELD(location);

	IF_EXIST(paramindextype)
	{
		READ_NODE_FIELD(paramindextype);
	}

	READ_DONE();
}

/*
 * _readAggref
 */
static Aggref *
_readAggref(void)
{
	READ_LOCALS(Aggref);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(aggfnoid);
	else
#endif
	READ_OID_FIELD(aggfnoid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(aggtype);
	else
#endif
	READ_OID_FIELD(aggtype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(aggcollid);
	else
#endif
	READ_OID_FIELD(aggcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(aggtranstype);
	else
#endif
	READ_OID_FIELD(aggtranstype);
#ifdef XCP
	if (portable_input)
		READ_TYPID_LIST_FIELD(aggargtypes);
	else
#endif
	READ_NODE_FIELD(aggargtypes);
	READ_NODE_FIELD(aggdirectargs);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggorder);
	READ_NODE_FIELD(aggdistinct);
	READ_NODE_FIELD(aggfilter);
	READ_BOOL_FIELD(aggstar);
	READ_BOOL_FIELD(aggvariadic);
	READ_CHAR_FIELD(aggkind);
	READ_UINT_FIELD(agglevelsup);
	READ_ENUM_FIELD(aggsplit, AggSplit);
	READ_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	IF_EXIST(distinct_args)
	{
		READ_NODE_FIELD(distinct_args);
	}
	IF_EXIST(distinct_num)
	{
		READ_LONG_FIELD(distinct_num);
	}
#endif
	IF_EXIST(aggkeep)
	{
		READ_BOOL_FIELD(aggkeep);
	}
	READ_DONE();
}

/*
 * _readGroupingFunc
 */
static GroupingFunc *
_readGroupingFunc(void)
{
	READ_LOCALS(GroupingFunc);

	READ_NODE_FIELD(args);
	READ_NODE_FIELD(refs);
	READ_NODE_FIELD(cols);
	READ_UINT_FIELD(agglevelsup);
	READ_LOCATION_FIELD(location);
#ifdef __OPENTENBASE_C__
	READ_BOOL_FIELD(groupingSets);
#endif
	IF_EXIST(kind)
	{
		READ_ENUM_FIELD(kind, GroupingFuncKind);
	}
	READ_DONE();
}

/*
 * _readWindowFunc
 */
static WindowFunc *
_readWindowFunc(void)
{
	READ_LOCALS(WindowFunc);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(winfnoid);
	else
#endif
	READ_OID_FIELD(winfnoid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(wintype);
	else
#endif
	READ_OID_FIELD(wintype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(wincollid);
	else
#endif
	READ_OID_FIELD(wincollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(aggfilter);
	READ_UINT_FIELD(winref);
	READ_BOOL_FIELD(winstar);
	READ_BOOL_FIELD(winagg);
	IF_EXIST(windistinct)
	{
		READ_BOOL_FIELD(windistinct);
	}
	IF_EXIST(winaggargtype)
	{
#ifdef XCP
		if (portable_input)
			READ_TYPID_FIELD(winaggargtype);
		else
#endif
			READ_OID_FIELD(winaggargtype);
	}
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readArrayRef
 */
static ArrayRef *
_readArrayRef(void)
{
	READ_LOCALS(ArrayRef);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(refarraytype);
	else
#endif
	READ_OID_FIELD(refarraytype);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(refelemtype);
	else
#endif
	READ_OID_FIELD(refelemtype);
	READ_INT_FIELD(reftypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(refcollid);
	else
#endif
	READ_OID_FIELD(refcollid);
	READ_NODE_FIELD(refupperindexpr);
	READ_NODE_FIELD(reflowerindexpr);
	READ_NODE_FIELD(refexpr);
	READ_NODE_FIELD(refassgnexpr);

	READ_DONE();
}

/*
 * _readFuncExpr
 */
static FuncExpr *
_readFuncExpr(void)
{
	READ_LOCALS(FuncExpr);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(funcid);
	else
#endif
	READ_OID_FIELD(funcid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(funcresulttype);
	else
#endif
	READ_OID_FIELD(funcresulttype);
	READ_BOOL_FIELD(funcretset);
	READ_BOOL_FIELD(funcvariadic);
	READ_ENUM_FIELD(funcformat, CoercionForm);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(funccollid);
	else
#endif
	READ_OID_FIELD(funccollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
#ifdef _PG_ORCL_
	IF_EXIST(withfuncnsp)
	{
		READ_NODE_FIELD(withfuncnsp);
	}
	IF_EXIST(withfuncid)
	{
		READ_INT_FIELD(withfuncid);
	}
#endif
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNamedArgExpr
 */
static NamedArgExpr *
_readNamedArgExpr(void)
{
	READ_LOCALS(NamedArgExpr);

	READ_NODE_FIELD(arg);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(argnumber);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readOpExpr
 */
static OpExpr *
_readOpExpr(void)
{
	READ_LOCALS(OpExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(opresulttype);
	else
#endif
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(opcollid);
	else
#endif
	READ_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readDistinctExpr
 */
static DistinctExpr *
_readDistinctExpr(void)
{
	READ_LOCALS(DistinctExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(opresulttype);
	else
#endif
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(opcollid);
	else
#endif
	READ_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullIfExpr
 */
static NullIfExpr *
_readNullIfExpr(void)
{
	READ_LOCALS(NullIfExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(opresulttype);
	else
#endif
	READ_OID_FIELD(opresulttype);
	READ_BOOL_FIELD(opretset);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(opcollid);
	else
#endif
	READ_OID_FIELD(opcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readScalarArrayOpExpr
 */
static ScalarArrayOpExpr *
_readScalarArrayOpExpr(void)
{
	READ_LOCALS(ScalarArrayOpExpr);

#ifdef XCP
	if (portable_input)
		READ_OPERID_FIELD(opno);
	else
#endif
	READ_OID_FIELD(opno);
#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(opfuncid);
	else
#endif
	READ_OID_FIELD(opfuncid);
	READ_BOOL_FIELD(useOr);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readBoolExpr
 */
static BoolExpr *
_readBoolExpr(void)
{
	READ_LOCALS(BoolExpr);

	/* do-it-yourself enum representation */
	token = pg_strtok(&length); /* skip :boolop */
	token = pg_strtok(&length); /* get field value */
	if (strncmp(token, "and", 3) == 0)
		local_node->boolop = AND_EXPR;
	else if (strncmp(token, "or", 2) == 0)
		local_node->boolop = OR_EXPR;
	else if (strncmp(token, "not", 3) == 0)
		local_node->boolop = NOT_EXPR;
	else
		elog(ERROR, "unrecognized boolop \"%.*s\"", length, token);

	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSubLink
 */
static SubLink *
_readSubLink(void)
{
	READ_LOCALS(SubLink);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_INT_FIELD(subLinkId);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(operName);
	READ_NODE_FIELD(subselect);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readFieldSelect
 */
static FieldSelect *
_readFieldSelect(void)
{
	READ_LOCALS(FieldSelect);

	READ_NODE_FIELD(arg);
	READ_INT_FIELD(fieldnum);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);

	READ_DONE();
}

/*
 * _readFieldStore
 */
static FieldStore *
_readFieldStore(void)
{
	READ_LOCALS(FieldStore);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(fieldnums);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);

	READ_DONE();
}

/*
 * _readRelabelType
 */
static RelabelType *
_readRelabelType(void)
{
	READ_LOCALS(RelabelType);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(relabelformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceViaIO
 */
static CoerceViaIO *
_readCoerceViaIO(void)
{
	READ_LOCALS(CoerceViaIO);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
    IF_EXIST(resulttypmod)
    {
        READ_INT_FIELD(resulttypmod);
    }
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readArrayCoerceExpr
 */
static ArrayCoerceExpr *
_readArrayCoerceExpr(void)
{
	READ_LOCALS(ArrayCoerceExpr);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(elemexpr);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coerceformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readConvertRowtypeExpr
 */
static ConvertRowtypeExpr *
_readConvertRowtypeExpr(void)
{
	READ_LOCALS(ConvertRowtypeExpr);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_ENUM_FIELD(convertformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCollateExpr
 */
static CollateExpr *
_readCollateExpr(void)
{
	READ_LOCALS(CollateExpr);

	READ_NODE_FIELD(arg);
#ifdef __OPENTENBASE__
	if (portable_input)
	{
		READ_COLLID_FIELD(collOid);
	}
	else
#endif
	READ_OID_FIELD(collOid);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readConstraint
 */
static Constraint *
_readConstraint(void)
{
	READ_LOCALS(Constraint);

	READ_STRING_FIELD(conname);
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_LOCATION_FIELD(location);

	token = pg_strtok(&length); /* skip :contype */
	token = pg_strtok(&length); /* get field value */
	if (length == 4 && strncmp(token, "NULL", 4) == 0)
		local_node->contype = CONSTR_NULL;
	else if (length == 8 && strncmp(token, "NOT_NULL", 8) == 0)
		local_node->contype = CONSTR_NOTNULL;
	else if (length == 7 && strncmp(token, "DEFAULT", 7) == 0)
		local_node->contype = CONSTR_DEFAULT;
	else if (length == 8 && strncmp(token, "IDENTITY", 8) == 0)
		local_node->contype = CONSTR_IDENTITY;
	else if (length == 5 && strncmp(token, "CHECK", 5) == 0)
		local_node->contype = CONSTR_CHECK;
	else if (length == 11 && strncmp(token, "PRIMARY_KEY", 11) == 0)
		local_node->contype = CONSTR_PRIMARY;
	else if (length == 6 && strncmp(token, "UNIQUE", 6) == 0)
		local_node->contype = CONSTR_UNIQUE;
	else if (length == 9 && strncmp(token, "EXCLUSION", 9) == 0)
		local_node->contype = CONSTR_EXCLUSION;
	else if (length == 11 && strncmp(token, "FOREIGN_KEY", 11) == 0)
		local_node->contype = CONSTR_FOREIGN;
	else if (length == 15 && strncmp(token, "ATTR_DEFERRABLE", 15) == 0)
		local_node->contype = CONSTR_ATTR_DEFERRABLE;
	else if (length == 19 && strncmp(token, "ATTR_NOT_DEFERRABLE", 19) == 0)
		local_node->contype = CONSTR_ATTR_NOT_DEFERRABLE;
	else if (length == 13 && strncmp(token, "ATTR_DEFERRED", 13) == 0)
		local_node->contype = CONSTR_ATTR_DEFERRED;
	else if (length == 14 && strncmp(token, "ATTR_IMMEDIATE", 14) == 0)
		local_node->contype = CONSTR_ATTR_IMMEDIATE;

	switch (local_node->contype)
	{
		case CONSTR_NULL:
		case CONSTR_NOTNULL:
			/* no extra fields */
			break;

		case CONSTR_DEFAULT:
			READ_NODE_FIELD(raw_expr);
			READ_STRING_FIELD(cooked_expr);
			break;

		case CONSTR_IDENTITY:
			READ_NODE_FIELD(options);
			READ_CHAR_FIELD(generated_when);
			break;

		case CONSTR_CHECK:
			READ_BOOL_FIELD(is_no_inherit);
			READ_NODE_FIELD(raw_expr);
			READ_STRING_FIELD(cooked_expr);
			READ_BOOL_FIELD(skip_validation);
			READ_BOOL_FIELD(initially_valid);
			break;

		case CONSTR_PRIMARY:
			READ_NODE_FIELD(keys);
			READ_NODE_FIELD(options);
			READ_STRING_FIELD(indexname);
			READ_STRING_FIELD(indexspace);
			READ_BOOL_FIELD(reset_default_tblspc);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_UNIQUE:
			READ_NODE_FIELD(keys);
			READ_NODE_FIELD(options);
			READ_STRING_FIELD(indexname);
			READ_STRING_FIELD(indexspace);
			READ_BOOL_FIELD(reset_default_tblspc);
			/* access_method and where_clause not currently used */
			break;

		case CONSTR_EXCLUSION:
			READ_NODE_FIELD(exclusions);
			READ_NODE_FIELD(options);
			READ_STRING_FIELD(indexname);
			READ_STRING_FIELD(indexspace);
			READ_BOOL_FIELD(reset_default_tblspc);
			READ_STRING_FIELD(access_method);
			READ_NODE_FIELD(where_clause);
			break;

		case CONSTR_FOREIGN:
			READ_NODE_FIELD(pktable);
			READ_NODE_FIELD(fk_attrs);
			READ_NODE_FIELD(pk_attrs);
			READ_CHAR_FIELD(fk_matchtype);
			READ_CHAR_FIELD(fk_upd_action);
			READ_CHAR_FIELD(fk_del_action);
			READ_NODE_FIELD(old_conpfeqop);
			READ_OID_FIELD(old_pktable_oid);
			READ_BOOL_FIELD(skip_validation);
			READ_BOOL_FIELD(initially_valid);
			break;

		case CONSTR_ATTR_DEFERRABLE:
		case CONSTR_ATTR_NOT_DEFERRABLE:
		case CONSTR_ATTR_DEFERRED:
		case CONSTR_ATTR_IMMEDIATE:
			/* no extra fields */
			break;

		default:
			elog(ERROR, "unrecognized ConstrType: %d", (int) local_node->contype);
			break;
	}

	READ_DONE();
}


/*
 * _readCaseExpr
 */
static CaseExpr *
_readCaseExpr(void)
{
	READ_LOCALS(CaseExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(casetype);
	else
#endif
	READ_OID_FIELD(casetype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(casecollid);
	else
#endif
	READ_OID_FIELD(casecollid);
	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(defresult);
	IF_EXIST(isdecode)
	{
		READ_BOOL_FIELD(isdecode);
	}
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseWhen
 */
static CaseWhen *
_readCaseWhen(void)
{
	READ_LOCALS(CaseWhen);

	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(result);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCaseTestExpr
 */
static CaseTestExpr *
_readCaseTestExpr(void)
{
	READ_LOCALS(CaseTestExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(typeId);
	else
#endif
	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(collation);
	else
#endif
	READ_OID_FIELD(collation);

	READ_DONE();
}

/*
 * _readArrayExpr
 */
static ArrayExpr *
_readArrayExpr(void)
{
	READ_LOCALS(ArrayExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(array_typeid);
	else
#endif
	READ_OID_FIELD(array_typeid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(array_collid);
	else
#endif
	READ_OID_FIELD(array_collid);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(element_typeid);
	else
#endif
	READ_OID_FIELD(element_typeid);
	READ_NODE_FIELD(elements);
	READ_BOOL_FIELD(multidims);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowExpr
 */
static RowExpr *
_readRowExpr(void)
{
	READ_LOCALS(RowExpr);

	READ_NODE_FIELD(args);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(row_typeid);
	else
#endif
	READ_OID_FIELD(row_typeid);
	READ_ENUM_FIELD(row_format, CoercionForm);
	READ_NODE_FIELD(colnames);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readRowCompareExpr
 */
static RowCompareExpr *
_readRowCompareExpr(void)
{
	READ_LOCALS(RowCompareExpr);

	READ_ENUM_FIELD(rctype, RowCompareType);
	READ_NODE_FIELD(opnos);
	READ_NODE_FIELD(opfamilies);
	READ_NODE_FIELD(inputcollids);
	READ_NODE_FIELD(largs);
	READ_NODE_FIELD(rargs);

	READ_DONE();
}

/*
 * _readCoalesceExpr
 */
static CoalesceExpr *
_readCoalesceExpr(void)
{
	READ_LOCALS(CoalesceExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(coalescetype);
	else
#endif
	READ_OID_FIELD(coalescetype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(coalescecollid);
	else
#endif
	READ_OID_FIELD(coalescecollid);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readMinMaxExpr
 */
static MinMaxExpr *
_readMinMaxExpr(void)
{
	READ_LOCALS(MinMaxExpr);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(minmaxtype);
	else
#endif
	READ_OID_FIELD(minmaxtype);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(minmaxcollid);
	else
#endif
	READ_OID_FIELD(minmaxcollid);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(inputcollid);
	else
#endif
	READ_OID_FIELD(inputcollid);
	READ_ENUM_FIELD(op, MinMaxOp);
	READ_NODE_FIELD(args);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSQLValueFunction
 */
static SQLValueFunction *
_readSQLValueFunction(void)
{
	READ_LOCALS(SQLValueFunction);

	READ_ENUM_FIELD(op, SQLValueFunctionOp);
	if (portable_input)
		READ_TYPID_FIELD(type);
	else
		READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNextValueExpr
 */
static NextValueExpr *
_readNextValueExpr(void)
{
	READ_LOCALS(NextValueExpr);

	if (portable_input)
	{
		READ_RELID_FIELD(seqid);
		READ_TYPID_FIELD(typeId);
	}
	else
	{
		READ_OID_FIELD(seqid);
		READ_OID_FIELD(typeId);
	}
	IF_EXIST(typmod)
	{
		READ_INT_FIELD(typmod);
	}
	READ_DONE();
}

/*
 * _readXmlExpr
 */
static XmlExpr *
_readXmlExpr(void)
{
	READ_LOCALS(XmlExpr);

	READ_ENUM_FIELD(op, XmlExprOp);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(named_args);
	READ_NODE_FIELD(arg_names);
	READ_NODE_FIELD(args);
	READ_ENUM_FIELD(xmloption, XmlOptionType);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(type);
	else
#endif
	READ_OID_FIELD(type);
	READ_INT_FIELD(typmod);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readNullTest
 */
static NullTest *
_readNullTest(void)
{
	READ_LOCALS(NullTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(nulltesttype, NullTestType);
	READ_BOOL_FIELD(argisrow);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readBooleanTest
 */
static BooleanTest *
_readBooleanTest(void)
{
	READ_LOCALS(BooleanTest);

	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(booltesttype, BoolTestType);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomain
 */
static CoerceToDomain *
_readCoerceToDomain(void)
{
	READ_LOCALS(CoerceToDomain);

	READ_NODE_FIELD(arg);
#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(resulttype);
	else
#endif
	READ_OID_FIELD(resulttype);
	READ_INT_FIELD(resulttypmod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(resultcollid);
	else
#endif
	READ_OID_FIELD(resultcollid);
	READ_ENUM_FIELD(coercionformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCoerceToDomainValue
 */
static CoerceToDomainValue *
_readCoerceToDomainValue(void)
{
	READ_LOCALS(CoerceToDomainValue);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(typeId);
	else
#endif
	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(collation);
	else
#endif
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readSetToDefault
 */
static SetToDefault *
_readSetToDefault(void)
{
	READ_LOCALS(SetToDefault);

#ifdef XCP
	if (portable_input)
		READ_TYPID_FIELD(typeId);
	else
#endif
	READ_OID_FIELD(typeId);
	READ_INT_FIELD(typeMod);
#ifdef XCP
	if (portable_input)
		READ_COLLID_FIELD(collation);
	else
#endif
	READ_OID_FIELD(collation);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCurrentOfExpr
 */
static CurrentOfExpr *
_readCurrentOfExpr(void)
{
	READ_LOCALS(CurrentOfExpr);

	READ_UINT_FIELD(cvarno);
	READ_STRING_FIELD(cursor_name);
	READ_INT_FIELD(cursor_param);

	READ_DONE();
}

/*
 * _readInferenceElem
 */
static InferenceElem *
_readInferenceElem(void)
{
	READ_LOCALS(InferenceElem);

	READ_NODE_FIELD(expr);
#ifdef __OPENTENBASE__
	if (portable_input)
	{
		READ_COLLID_FIELD(infercollid);
		READ_OPCLASS_FIELD(inferopclass);
	}
	else
	{
#endif
	READ_OID_FIELD(infercollid);
	READ_OID_FIELD(inferopclass);
#ifdef __OPENTENBASE__
	}
#endif
	READ_DONE();
}

/*
 * _readTargetEntry
 */
static TargetEntry *
_readTargetEntry(void)
{
	READ_LOCALS(TargetEntry);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(resno);
	READ_STRING_FIELD(resname);
	READ_UINT_FIELD(ressortgroupref);
#ifdef XCP
	if (portable_input)
		READ_RELID_FIELD_NOWARN(resorigtbl);
	else
#endif
	READ_OID_FIELD(resorigtbl);
	READ_INT_FIELD(resorigcol);
	READ_BOOL_FIELD(resjunk);
	READ_DONE();
}

/*
 * _readRangeTblRef
 */
static RangeTblRef *
_readRangeTblRef(void)
{
	READ_LOCALS(RangeTblRef);

	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readJoinExpr
 */
static JoinExpr *
_readJoinExpr(void)
{
	READ_LOCALS(JoinExpr);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(isNatural);
	READ_NODE_FIELD(larg);
	READ_NODE_FIELD(rarg);
	READ_NODE_FIELD(usingClause);
	READ_NODE_FIELD(quals);
	READ_NODE_FIELD(alias);
	READ_INT_FIELD(rtindex);

	READ_DONE();
}

/*
 * _readFromExpr
 */
static FromExpr *
_readFromExpr(void)
{
	READ_LOCALS(FromExpr);

	READ_NODE_FIELD(fromlist);
	READ_NODE_FIELD(quals);

	READ_DONE();
}

/*
 * _readMergeAction
 */
static MergeAction*
_readMergeAction(void)
{
	READ_LOCALS(MergeAction);

	READ_BOOL_FIELD(matched);
	READ_ENUM_FIELD(commandType, CmdType);
	READ_ENUM_FIELD(override, OverridingKind);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(targetList);
	READ_NODE_FIELD(updateColnos);
    READ_BOOL_FIELD(actionOnly);
	READ_NODE_FIELD(deleteAction);

	READ_DONE();
}

/*
 * _readOnConflictExpr
 */
static OnConflictExpr *
_readOnConflictExpr(void)
{
	READ_LOCALS(OnConflictExpr);

	READ_ENUM_FIELD(action, OnConflictAction);
	READ_NODE_FIELD(arbiterElems);
	READ_NODE_FIELD(arbiterWhere);
#ifdef __OPENTENBASE__
	if (portable_input)
	{
		READ_CONSTRAINT_FIELD(constraint);
	}
	else
#endif
	READ_OID_FIELD(constraint);
	READ_NODE_FIELD(onConflictSet);
	READ_NODE_FIELD(onConflictWhere);
	READ_INT_FIELD(exclRelIndex);
	READ_NODE_FIELD(exclRelTlist);

	READ_DONE();
}

/*
 * _readConnectByExpr
 */
static ConnectByExpr *
_readConnectByExpr(void)
{
	READ_LOCALS(ConnectByExpr);

	READ_BOOL_FIELD(nocycle);
	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(start);
	READ_NODE_FIELD(sort);

	READ_DONE();
}

/*
 *	Stuff from parsenodes.h.
 */

/*
 * _readRangeTblEntry
 */
static RangeTblEntry *
_readRangeTblEntry(void)
{
	READ_LOCALS(RangeTblEntry);

	/* put alias + eref first to make dump more legible */
	READ_NODE_FIELD(alias);
	READ_NODE_FIELD(eref);
	READ_ENUM_FIELD(rtekind, RTEKind);

	switch (local_node->rtekind)
	{
		case RTE_RELATION:
			READ_CHAR_FIELD(relkind);
#ifdef XCP
			if (portable_input)
			{
				if ((local_node->relkind != RELKIND_MATVIEW) &&
						(local_node->relkind != RELKIND_VIEW))
					READ_RELID_FIELD(relid);
				else
					READ_RELID_FIELD_NOWARN(relid);
			}
			else
#endif
			READ_OID_FIELD(relid);
			READ_NODE_FIELD(tablesample);
			break;
		case RTE_SUBQUERY:
			READ_NODE_FIELD(subquery);
			READ_BOOL_FIELD(security_barrier);
			break;
		case RTE_JOIN:
			READ_ENUM_FIELD(jointype, JoinType);
			READ_NODE_FIELD(joinaliasvars);
			break;
		case RTE_FUNCTION:
			READ_NODE_FIELD(functions);
			READ_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			READ_NODE_FIELD(tablefunc);
			break;
		case RTE_VALUES:
			READ_NODE_FIELD(values_lists);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			READ_STRING_FIELD(ctename);
			READ_UINT_FIELD(ctelevelsup);
			READ_BOOL_FIELD(self_reference);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			READ_STRING_FIELD(enrname);
			READ_FLOAT_FIELD(enrtuples);
#ifdef __OPENTENBASE__
			if (portable_input)
			{
				READ_RELID_FIELD(relid);
			}
			else
#endif
			READ_OID_FIELD(relid);
			READ_NODE_FIELD(coltypes);
			READ_NODE_FIELD(coltypmods);
			READ_NODE_FIELD(colcollations);
			break;
#ifdef PGXC
		case RTE_REMOTE_DUMMY:
			/* Nothing to do */
			break;
#endif /* PGXC */
		default:
			elog(ERROR, "unrecognized RTE kind: %d",
				 (int) local_node->rtekind);
			break;
	}

	READ_BOOL_FIELD(lateral);
	READ_BOOL_FIELD(inh);
	READ_BOOL_FIELD(inFromCl);
	READ_UINT_FIELD(requiredPerms);
#ifdef XCP
	if (portable_input)
	{
		local_node->requiredPerms = 0; /* no permission checks on data node */
		token = pg_strtok(&length);	/* skip :fldname */ \
		token = pg_strtok(&length);	/* skip field value */ \
		local_node->checkAsUser = InvalidOid;
	}
	else
#endif
	READ_OID_FIELD(checkAsUser);
	READ_BITMAPSET_FIELD(selectedCols);
	READ_BITMAPSET_FIELD(insertedCols);
	READ_BITMAPSET_FIELD(updatedCols);
	READ_NODE_FIELD(securityQuals);

#ifdef __OPENTENBASE__
	SKIP_SCALAR_FIELD(intervalparent);
	SKIP_SCALAR_FIELD(isdefault);
	SKIP_NODE_FIELD(partvalue);
#endif

	SKIP_SCALAR_FIELD(relpartOid);

	READ_DONE();
}

/*
 * _readRangeTblFunction
 */
static RangeTblFunction *
_readRangeTblFunction(void)
{
	READ_LOCALS(RangeTblFunction);

	READ_NODE_FIELD(funcexpr);
	READ_INT_FIELD(funccolcount);
	READ_NODE_FIELD(funccolnames);
    if (portable_input)
    {
        READ_TYPID_LIST_FIELD(funccoltypes);
    }
    else
    {
	READ_NODE_FIELD(funccoltypes);
    }
	READ_NODE_FIELD(funccoltypmods);
	READ_NODE_FIELD(funccolcollations);
	READ_BITMAPSET_FIELD(funcparams);

	READ_DONE();
}

/*
 * _readTableSampleClause
 */
static TableSampleClause *
_readTableSampleClause(void)
{
	READ_LOCALS(TableSampleClause);

#ifdef XCP
	if (portable_input)
		READ_FUNCID_FIELD(tsmhandler);
	else
#endif
	READ_OID_FIELD(tsmhandler);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(repeatable);

	READ_DONE();
}


/*
 * ReadCommonPlan
 *	Assign the basic stuff of all nodes that inherit from Plan
 */
static void
ReadCommonPlan(Plan *local_node)
{
	READ_TEMP_LOCALS();

	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(total_cost);
	READ_FLOAT_FIELD(plan_rows);
	READ_INT_FIELD(plan_width);
	READ_BOOL_FIELD(parallel_aware);
	READ_BOOL_FIELD(parallel_safe);
	READ_INT_FIELD(plan_node_id);
	READ_NODE_FIELD(targetlist);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(lefttree);
	READ_NODE_FIELD(righttree);
	READ_NODE_FIELD(initPlan);
	READ_NODE_FIELD(exec_subplan);
	READ_BITMAPSET_FIELD(extParam);
	READ_BITMAPSET_FIELD(allParam);
#ifdef __OPENTENBASE_C__
	READ_INT_FIELD(parallel_num);
	READ_BOOL_FIELD(is_subplan);
	READ_INT_FIELD(operator_mem);
#endif
#ifdef __OPENTENBASE__
	READ_INT_FIELD(remote_flag);
#endif
#ifdef __AUDIT_FGA__
	READ_NODE_FIELD(audit_fga_quals);
#endif
}

/* begin ora_compatible */
/*
 * _readMultiInsertInto
 */
static MultiModifyTable *
_readMultiModifyTable(void)
{
	READ_LOCALS(MultiModifyTable);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(operation, CmdType);
	READ_NODE_FIELD(sub_modifytable);
	READ_INT_FIELD(startno);
	READ_INT_FIELD(endno);
	READ_BOOL_FIELD(has_else);

	READ_DONE();
}

static MultiInsertInto *
_readMultiInsertInto(void)
{
	READ_LOCALS(MultiInsertInto);

	READ_NODE_FIELD(when_cond);
	READ_NODE_FIELD(sub_intos);

	READ_DONE();
}
/* end ora_compatible */

/*
 * _readDefElem
 */
static DefElem *
_readDefElem(void)
{
	READ_LOCALS(DefElem);

	READ_STRING_FIELD(defnamespace);
	READ_STRING_FIELD(defname);
	READ_NODE_FIELD(arg);
	READ_ENUM_FIELD(defaction, DefElemAction);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCopyFormatter
 */
static CopyFormatter *
_readCopyFormatter(void)
{
	READ_LOCALS(CopyFormatter);

    READ_NODE_FIELD(fixed_attrs);
    READ_BOOL_FIELD(with_cut);

	READ_DONE();
}

/*
 * _readCopyAttrFixed
 */
static CopyAttrFixed *
_readCopyAttrFixed(void)
{
	READ_LOCALS(CopyAttrFixed);

	READ_STRING_FIELD(attname);
	READ_INT_FIELD(fix_len);
	READ_INT_FIELD(attnum);
	READ_ENUM_FIELD(align, ColAlign);
	READ_BOOL_FIELD(with_cut);

	READ_DONE();
}

/*
 * _readPlan
 */
static Plan *
_readPlan(void)
{
	READ_LOCALS_NO_FIELDS(Plan);

	ReadCommonPlan(local_node);

	READ_DONE();
}

/*
 *	Stuff from plannodes.h.
 */

/*
 * _readPlannedStmt
 */
static PlannedStmt *
_readPlannedStmt(void)
{
	READ_LOCALS(PlannedStmt);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_UINT_FIELD(queryId);
	READ_BOOL_FIELD(hasReturning);
	READ_BOOL_FIELD(hasModifyingCTE);
	READ_BOOL_FIELD(canSetTag);
	READ_BOOL_FIELD(transientPlan);
	READ_BOOL_FIELD(dependsOnRole);
	READ_BOOL_FIELD(parallelModeNeeded);
    READ_BOOL_FIELD(canNextvalParallel);
	READ_BOOL_FIELD(remote);
	READ_INT_FIELD(jitFlags);
	READ_NODE_FIELD(planTree);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(nonleafResultRelations);
	READ_NODE_FIELD(rootResultRelations);
	READ_NODE_FIELD(subplans);
	READ_BITMAPSET_FIELD(rewindPlanIDs);
	READ_NODE_FIELD(rowMarks);
	READ_NODE_FIELD(relationOids);
	READ_NODE_FIELD(invalItems);
	READ_NODE_FIELD(paramExecTypes);
	READ_NODE_FIELD(utilityStmt);
	READ_LOCATION_FIELD(stmt_location);
	READ_LOCATION_FIELD(stmt_len);

#ifdef __AUDIT__
	READ_STRING_FIELD(queryString);
	READ_NODE_FIELD(parseTree);
#endif

#ifdef __OPENTENBASE_C__
	READ_NODE_FIELD(sharedCtePlans);
	READ_BITMAPSET_FIELD(sharedCtePlanIds);
	READ_INT_FIELD(planned_query_mem);
	READ_INT_FIELD(planned_work_mem);
	READ_INT_FIELD(fragmentNum);
#endif

	/* begin ora_compatible */
	IF_EXIST(multi_dist)
	{
		READ_NODE_FIELD(multi_dist);
	}
	IF_EXIST(p_startno)
	{
		READ_INT_FIELD(p_startno);
	}
	IF_EXIST(p_endno)
	{
		READ_INT_FIELD(p_endno);
	}
	IF_EXIST(p_has_else)
	{
		READ_BOOL_FIELD(p_has_else);
	}
	IF_EXIST(p_has_val_func)
	{
		READ_BOOL_FIELD(p_has_val_func);
	}
	/* end ora_compatible */
	IF_EXIST(queryid.timestamp_nodeid)
	{
		READ_LONG_FIELD(queryid.timestamp_nodeid);
	}
	IF_EXIST(queryid.sequence)
	{
		READ_LONG_FIELD(queryid.sequence);
	}

	IF_EXIST(guc_str->data)
	{
		READ_STRING_FIELD(guc_str->data);
	}
	IF_EXIST(guc_str->len)
	{
		READ_INT_FIELD(guc_str->len);
	}
	IF_EXIST(guc_str->maxlen)
	{
		READ_INT_FIELD(guc_str->maxlen);
	}
	IF_EXIST(guc_str->cursor)
	{
		READ_INT_FIELD(guc_str->cursor);
	}

	READ_DONE();
}

/*
 * _readResult
 */
static Result *
_readResult(void)
{
	READ_LOCALS(Result);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(resconstantqual);
	READ_BOOL_FIELD(fail_return);

	READ_DONE();
}

/*
 * _readProjectSet
 */
static ProjectSet *
_readProjectSet(void)
{
	READ_LOCALS_NO_FIELDS(ProjectSet);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readModifyTable
 */
static ModifyTable *
_readModifyTable(void)
{
	READ_LOCALS(ModifyTable);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(operation, CmdType);
	READ_BOOL_FIELD(canSetTag);
	READ_UINT_FIELD(nominalRelation);
	READ_NODE_FIELD(partitioned_rels);
	READ_BOOL_FIELD(partColsUpdated);
	READ_NODE_FIELD(resultRelations);
	READ_INT_FIELD(resultRelIndex);
#ifdef _PG_ORCL_
	READ_INT_FIELD(rid_col_idx);
#endif
	READ_INT_FIELD(rootResultRelIndex);
	READ_NODE_FIELD(updateColnosLists);
	READ_NODE_FIELD(withCheckOptionLists);
	READ_NODE_FIELD(returningLists);
	READ_NODE_FIELD(fdwPrivLists);
	READ_BITMAPSET_FIELD(fdwDirectModifyPlans);
	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);
	READ_ENUM_FIELD(onConflictAction, OnConflictAction);
	if (portable_input)
		READ_RELID_LIST_FIELD(arbiterIndexes);
	else
		READ_NODE_FIELD(arbiterIndexes);
	READ_NODE_FIELD(onConflictSet);
	READ_NODE_FIELD(onConflictWhere);
	READ_UINT_FIELD(exclRelRTI);
	READ_NODE_FIELD(exclRelTlist);
#ifdef __OPENTENBASE__
	READ_NODE_FIELD(remote_plans);
    READ_INT_FIELD(mergeTargetRelation);
    READ_NODE_FIELD(mergeActionLists);
	READ_BOOL_FIELD(global_index);
	READ_BOOL_FIELD(update_gindex_key);
#endif
	IF_EXIST(is_submod)
	{
		READ_BOOL_FIELD(is_submod); /* ora_compatible */
	}
	READ_DONE();
}

/*
 * _readPartIterator
 */
static PartIterator *
_readPartIterator(void)
{
	READ_LOCALS(PartIterator);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(leafNum);
	READ_ENUM_FIELD(direction, ScanDirection);
	READ_INT_FIELD(partParamno);
	IF_EXIST(part_prune_info)
	{
		READ_NODE_FIELD(part_prune_info);
	}
	READ_DONE();
}

/*
 * _readAppend
 */
static Append *
_readAppend(void)
{
	READ_LOCALS(Append);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(appendplans);
	READ_INT_FIELD(first_partial_plan);
	READ_NODE_FIELD(partitioned_rels);
	READ_NODE_FIELD(part_prune_info);

	READ_DONE();
}

/*
 * _readMergeAppend
 */
static MergeAppend *
_readMergeAppend(void)
{
	int i;
	READ_LOCALS(MergeAppend);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(partitioned_rels);
	READ_NODE_FIELD(mergeplans);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);

	token = pg_strtok(&length);		/* skip :sortOperators */
	local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->sortOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
		local_node->sortOperators[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :collations */
	local_node->collations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->collations[i] = get_collid(collname,
													   collencoding,
													   NSP_OID(nspname));
			else
				local_node->collations[i] = InvalidOid;
		}
		else
		local_node->collations[i] = atooid(token);
	}

	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);

	READ_DONE();
}

/*
 * _readMergeStmt
 */
static MergeStmt*
_readMergeStmt(void)
{
    READ_LOCALS(MergeStmt);

    READ_NODE_FIELD(target);
    READ_NODE_FIELD(sourceRelation);
    READ_NODE_FIELD(joinCondition);
	READ_NODE_FIELD(mergeWhenClauses);
	READ_NODE_FIELD(withClause);

    READ_DONE();
}

/*
 * _readRecursiveUnion
 */
static RecursiveUnion *
_readRecursiveUnion(void)
{
	int i;
	READ_LOCALS(RecursiveUnion);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(wtParam);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(dupColIdx, local_node->numCols);

#ifdef PGXC
	token = pg_strtok(&length); 	/* skip :grpOperators */
	local_node->dupOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char	   *nspname; /* namespace name */
			char	   *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid 		oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid 		oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->dupOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->dupOperators[i] = atooid(token);
	}
#else
	READ_OID_ARRAY(dupOperators, local_node->numCols);
#endif

	READ_LONG_FIELD(numGroups);

	READ_DONE();
}

/*
 * _readBitmapAnd
 */
static BitmapAnd *
_readBitmapAnd(void)
{
	READ_LOCALS(BitmapAnd);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(bitmapplans);
	READ_BOOL_FIELD(isPartTbl);

	READ_DONE();
}

/*
 * _readBitmapOr
 */
static BitmapOr *
_readBitmapOr(void)
{
	READ_LOCALS(BitmapOr);

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(isshared);
	READ_NODE_FIELD(bitmapplans);
	READ_BOOL_FIELD(isPartTbl);

	READ_DONE();
}

/*
 * ReadCommonScan
 *	Assign the basic stuff of all nodes that inherit from Scan
 */
static void
ReadCommonScan(Scan *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_UINT_FIELD(scanrelid);
	READ_INT_FIELD(leafNum);
	READ_ENUM_FIELD(partScanDirection, ScanDirection);

#ifdef XCP
	if (portable_input)
		READ_RELID_LIST_FIELD(partition_leaf_rels);
	else
	{
#endif
		READ_NODE_FIELD(partition_leaf_rels);
#ifdef XCP
	}
#endif
	READ_NODE_FIELD(partition_leaf_rels_idx);
	READ_INT_FIELD(partIterParamno);
	READ_BOOL_FIELD(isPartTbl);
}

/*
 * _readScan
 */
static Scan *
_readScan(void)
{
	READ_LOCALS_NO_FIELDS(Scan);

	ReadCommonScan(local_node);

	READ_DONE();
}

/*
 * _readSeqScan
 */
static SeqScan *
_readSeqScan(void)
{
	READ_LOCALS_NO_FIELDS(SeqScan);

	ReadCommonScan(local_node);

	READ_DONE();
}

/*
 * _readExternalScanInfo
 */
static ExternalScanInfo *
_readExternalScanInfo(void)
{
	READ_LOCALS(ExternalScanInfo);

	READ_NODE_FIELD(uriList);
	READ_CHAR_FIELD(fmtType);
	READ_BOOL_FIELD(isMasterOnly);
	READ_INT_FIELD(rejLimit);
	READ_BOOL_FIELD(rejLimitInRows);
	READ_CHAR_FIELD(logErrors);
	READ_INT_FIELD(encoding);
	READ_INT_FIELD(scancounter);
	READ_NODE_FIELD(extOptions);

	READ_DONE();
}

/*
 * _readSampleScan
 */
static SampleScan *
_readSampleScan(void)
{
	READ_LOCALS(SampleScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tablesample);

	READ_DONE();
}

/*
 * _readIndexScan
 */
static IndexScan *
_readIndexScan(void)
{
	READ_LOCALS(IndexScan);

	ReadCommonScan(&local_node->scan);

	if (portable_input)
		READ_RELID_FIELD(indexid);
	else
		READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indexorderbyorig);
	READ_NODE_FIELD(indexorderbyops);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);

	READ_DONE();
}

/*
 * _readIndexOnlyScan
 */
static IndexOnlyScan *
_readIndexOnlyScan(void)
{
	READ_LOCALS(IndexOnlyScan);

	ReadCommonScan(&local_node->scan);

	if (portable_input)
		READ_RELID_FIELD(indexid);
	else
		READ_OID_FIELD(indexid);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexorderby);
	READ_NODE_FIELD(indextlist);
	READ_ENUM_FIELD(indexorderdir, ScanDirection);

	READ_DONE();
}

/*
 * _readBitmapIndexScan
 */
static BitmapIndexScan *
_readBitmapIndexScan(void)
{
	READ_LOCALS(BitmapIndexScan);

	ReadCommonScan(&local_node->scan);

	if (portable_input)
		READ_RELID_FIELD(indexid);
	else
		READ_OID_FIELD(indexid);
	READ_BOOL_FIELD(isshared);
	READ_NODE_FIELD(indexqual);
	READ_NODE_FIELD(indexqualorig);

	READ_DONE();
}

/*
 * _readBitmapHeapScan
 */
static BitmapHeapScan *
_readBitmapHeapScan(void)
{
	READ_LOCALS(BitmapHeapScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(bitmapqualorig);

	READ_DONE();
}

/*
 * _readTidScan
 */
static TidScan *
_readTidScan(void)
{
	READ_LOCALS(TidScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tidquals);

	READ_BOOL_FIELD(remote);

	READ_NODE_FIELD(remote_targets);
	
	READ_STRING_FIELD(remote_sql);
	
	READ_NODE_FIELD(remote_indexqual);
	
    READ_NODE_FIELD(remote_qual);

	READ_DONE();
}

/*
 * _readSubqueryScan
 */
static SubqueryScan *
_readSubqueryScan(void)
{
	READ_LOCALS(SubqueryScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(subplan);

	READ_DONE();
}

/*
 * _readFunctionScan
 */
static FunctionScan *
_readFunctionScan(void)
{
	READ_LOCALS(FunctionScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(functions);
	READ_BOOL_FIELD(funcordinality);

	READ_DONE();
}

/*
 * _readValuesScan
 */
static ValuesScan *
_readValuesScan(void)
{
	READ_LOCALS(ValuesScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(values_lists);

	READ_DONE();
}

/*
 * _readTableFuncScan
 */
static TableFuncScan *
_readTableFuncScan(void)
{
	READ_LOCALS(TableFuncScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tablefunc);

	READ_DONE();
}

/*
 * _readCteScan
 */
static CteScan *
_readCteScan(void)
{
	READ_LOCALS(CteScan);

	ReadCommonScan(&local_node->scan);

	READ_INT_FIELD(ctePlanId);
	READ_INT_FIELD(cteParam);

	READ_DONE();
}

/*
 * _readWorkTableScan
 */
static WorkTableScan *
_readWorkTableScan(void)
{
	READ_LOCALS(WorkTableScan);

	ReadCommonScan(&local_node->scan);

	READ_INT_FIELD(wtParam);

	READ_DONE();
}

/*
 * _readForeignScan
 */
static ForeignScan *
_readForeignScan(void)
{
	READ_LOCALS(ForeignScan);

	ReadCommonScan(&local_node->scan);

	READ_ENUM_FIELD(operation, CmdType);
#ifdef XCP
	if (portable_input)
	{
		READ_SERVERID_FIELD(fs_server);
	}
	else
#endif
		READ_OID_FIELD(fs_server);
	READ_NODE_FIELD(fdw_exprs);
	READ_NODE_FIELD(fdw_private);
	READ_NODE_FIELD(fdw_scan_tlist);
	READ_NODE_FIELD(fdw_recheck_quals);
	READ_BITMAPSET_FIELD(fs_relids);
	READ_BOOL_FIELD(fsSystemCol);

	READ_DONE();
}

/*
 * _readCustomScan
 */
static CustomScan *
_readCustomScan(void)
{
	READ_LOCALS(CustomScan);
	char	   *custom_name;
	const CustomScanMethods *methods;

	ReadCommonScan(&local_node->scan);

	READ_UINT_FIELD(flags);
	READ_NODE_FIELD(custom_plans);
	READ_NODE_FIELD(custom_exprs);
	READ_NODE_FIELD(custom_private);
	READ_NODE_FIELD(custom_scan_tlist);
	READ_BITMAPSET_FIELD(custom_relids);

	/* Lookup CustomScanMethods by CustomName */
	token = pg_strtok(&length); /* skip methods: */
	token = pg_strtok(&length); /* CustomName */
	custom_name = nullable_string(token, length);
	methods = GetCustomScanMethods(custom_name, false);
	local_node->methods = methods;

	READ_DONE();
}

/*
 * ReadCommonJoin
 *	Assign the basic stuff of all nodes that inherit from Join
 */
static void
ReadCommonJoin(Join *local_node)
{
	READ_TEMP_LOCALS();

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(jointype, JoinType);
	READ_BOOL_FIELD(inner_unique);
	READ_NODE_FIELD(joinqual);

	IF_EXIST(plannerinfo_id)
	{
		READ_INT_FIELD(plannerinfo_id);
	}
}

/*
 * _readJoin
 */
static Join *
_readJoin(void)
{
	READ_LOCALS_NO_FIELDS(Join);

	ReadCommonJoin(local_node);

	READ_DONE();
}

/*
 * _readNestLoop
 */
static NestLoop *
_readNestLoop(void)
{
	READ_LOCALS(NestLoop);

	ReadCommonJoin(&local_node->join);

	READ_NODE_FIELD(nestParams);

	READ_DONE();
}

/*
 * _readMergeJoin
 */
static MergeJoin *
_readMergeJoin(void)
{
	int			i;
	int			numCols;

	READ_LOCALS(MergeJoin);

	ReadCommonJoin(&local_node->join);

	READ_BOOL_FIELD(skip_mark_restore);
	READ_NODE_FIELD(mergeclauses);

	numCols = list_length(local_node->mergeclauses);

	READ_OID_ARRAY(mergeFamilies, numCols);

	token = pg_strtok(&length);		/* skip :mergeCollations */
	local_node->mergeCollations = (Oid *) palloc(numCols * sizeof(Oid));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->mergeCollations[i] = get_collid(collname,
															collencoding,
															NSP_OID(nspname));
			else
				local_node->mergeCollations[i] = InvalidOid;
		}
		else
		local_node->mergeCollations[i] = atooid(token);
	}

	READ_INT_ARRAY(mergeStrategies, numCols);
	READ_BOOL_ARRAY(mergeNullsFirst, numCols);

	IF_EXIST(is_not_full)
	{
		READ_BOOL_FIELD(is_not_full);
	}

	READ_DONE();
}

/*
 * _readHashJoin
 */
static HashJoin *
_readHashJoin(void)
{
	READ_LOCALS(HashJoin);

	ReadCommonJoin(&local_node->join);

	READ_NODE_FIELD(hashclauses);
	IF_EXIST(hashoperators)
	{
		READ_NODE_FIELD(hashoperators);
	}

	IF_EXIST(hashcollations)
	{
		READ_NODE_FIELD(hashcollations);
	}

	IF_EXIST(hashkeys)
	{
		READ_NODE_FIELD(hashkeys);
	}

	READ_BOOL_FIELD(nonequijoin);

	READ_INT_FIELD(num_batches);

	READ_INT_FIELD(num_buckets);

	READ_FLOAT_FIELD(innerbucketsize);

	READ_FLOAT_FIELD(hashjointuples);

	READ_DONE();
}

/*
 * _readMaterial
 */
static Material *
_readMaterial(void)
{
	READ_LOCALS_NO_FIELDS(Material);

	ReadCommonPlan(&local_node->plan);

	READ_DONE();
}

/*
 * _readSort
 */
static Sort *
_readSort(void)
{
	int i;
	READ_LOCALS(Sort);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :sortColIdx */
	local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->sortColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :sortOperators */
	local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->sortOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
		local_node->sortOperators[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :collations */
	local_node->collations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->collations[i] = get_collid(collname,
													   collencoding,
													   NSP_OID(nspname));
			else
				local_node->collations[i] = InvalidOid;
		}
		else
		local_node->collations[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :nullsFirst */
	local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->nullsFirst[i] = strtobool(token);
	}

	READ_DONE();
}

/*
 * _readGroup
 */
static Group *
_readGroup(void)
{
	int	i;
	READ_LOCALS(Group);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :grpColIdx */
	local_node->grpColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->grpColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :grpOperators */
	local_node->grpOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->grpOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->grpOperators[i] = atooid(token);
	}

	IF_EXIST(plannerinfo_seq_no)
	{
		READ_INT_FIELD(plannerinfo_seq_no);
	}

	IF_EXIST(agg_type)
	{
		READ_ENUM_FIELD(agg_type, AggType);
	}

	READ_DONE();
}

/*
 * _readAgg
 */
static Agg *
_readAgg(void)
{
	int i;
	READ_LOCALS(Agg);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(aggstrategy, AggStrategy);
	READ_ENUM_FIELD(aggsplit, AggSplit);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);

#ifdef PGXC
	token = pg_strtok(&length);		/* skip :grpOperators */
	local_node->grpOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->grpOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->grpOperators[i] = atooid(token);
	}
#else
	READ_OID_ARRAY(grpOperators, local_node->numCols);
#endif

	READ_LONG_FIELD(numGroups);
	READ_BITMAPSET_FIELD(aggParams);
	READ_NODE_FIELD(groupingSets);
	READ_NODE_FIELD(chain);
#ifdef __OPENTENBASE_C__
	READ_BOOL_FIELD(groupingFunc);
#endif

	READ_INT_FIELD(plannerinfo_seq_no);
	READ_ENUM_FIELD(agg_type, AggType);

	READ_DONE();
}

/*
 * _readWindowAgg
 */
static WindowAgg *
_readWindowAgg(void)
{
	int i;

	READ_LOCALS(WindowAgg);

	ReadCommonPlan(&local_node->plan);

	READ_UINT_FIELD(winref);
	READ_INT_FIELD(partNumCols);

	token = pg_strtok(&length);		/* skip :partColIdx */
	local_node->partColIdx = (AttrNumber *) palloc(local_node->partNumCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->partNumCols; i++)
	{
		token = pg_strtok(&length);
		local_node->partColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :partOperators */
	local_node->partOperators = (Oid *) palloc(local_node->partNumCols * sizeof(Oid));
	for (i = 0; i < local_node->partNumCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->partOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
			local_node->partOperators[i] = atooid(token);
	}

	READ_INT_FIELD(ordNumCols);

	token = pg_strtok(&length);		/* skip :ordColIdx */
	local_node->ordColIdx = (AttrNumber *) palloc(local_node->ordNumCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->ordNumCols; i++)
	{
		token = pg_strtok(&length);
		local_node->ordColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :ordOperators */
	local_node->ordOperators = (Oid *) palloc(local_node->ordNumCols * sizeof(Oid));
	for (i = 0; i < local_node->ordNumCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->ordOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
			local_node->ordOperators[i] = atooid(token);
	}

	READ_INT_FIELD(frameOptions);
	READ_NODE_FIELD(startOffset);
	READ_NODE_FIELD(endOffset);
	IF_EXIST(startInRangeFunc)
	{
		if (portable_input)
		{
			READ_FUNCID_FIELD(startInRangeFunc);
			READ_FUNCID_FIELD(endInRangeFunc);
			READ_FUNCID_FIELD(inRangeColl);
		}
		else
		{
			READ_OID_FIELD(startInRangeFunc);
			READ_OID_FIELD(endInRangeFunc);
			READ_OID_FIELD(inRangeColl);
		}
		READ_BOOL_FIELD(inRangeAsc);
		READ_BOOL_FIELD(inRangeNullsFirst);
		READ_BOOL_FIELD(start_contain_vars);
		READ_BOOL_FIELD(end_contain_vars);
	}

	READ_DONE();
}

/*
 * _readUnique
 */
static Unique *
_readUnique(void)
{
	int i;
	READ_LOCALS(Unique);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->numCols);

	token = pg_strtok(&length);		/* skip :uniqOperators */
	local_node->uniqOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->uniqOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
			local_node->uniqOperators[i] = atooid(token);
	}

	IF_EXIST(plannerinfo_seq_no)
	{
		READ_INT_FIELD(plannerinfo_seq_no);
	}

	IF_EXIST(agg_type)
	{
		READ_ENUM_FIELD(agg_type, AggType);
	}

	READ_DONE();
}

/*
 * _readGather
 */
static Gather *
_readGather(void)
{
	READ_LOCALS(Gather);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(rescan_param);
	READ_BOOL_FIELD(single_copy);
	READ_BOOL_FIELD(invisible);
	READ_BITMAPSET_FIELD(initParam);

	READ_DONE();
}

/*
 * _readGatherMerge
 */
static GatherMerge *
_readGatherMerge(void)
{
	READ_LOCALS(GatherMerge);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(rescan_param);
	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(sortColIdx, local_node->numCols);
	READ_OID_ARRAY(sortOperators, local_node->numCols);
	READ_OID_ARRAY(collations, local_node->numCols);
	READ_BOOL_ARRAY(nullsFirst, local_node->numCols);
	READ_BITMAPSET_FIELD(initParam);

	READ_DONE();
}

/*
 * _readHash
 */
static Hash *
_readHash(void)
{
	READ_LOCALS(Hash);

	ReadCommonPlan(&local_node->plan);

	IF_EXIST(hashkeys)
	{
		READ_NODE_FIELD(hashkeys);
	}

	if (portable_input)
		READ_RELID_FIELD(skewTable);
	else
		READ_OID_FIELD(skewTable);
	READ_INT_FIELD(skewColumn);
	READ_BOOL_FIELD(skewInherit);
	READ_FLOAT_FIELD(rows_total);

	READ_DONE();
}

/*
 * _readSetOp
 */
static SetOp *
_readSetOp(void)
{
	int i;
	READ_LOCALS(SetOp);

	ReadCommonPlan(&local_node->plan);

	READ_ENUM_FIELD(cmd, SetOpCmd);
	READ_ENUM_FIELD(strategy, SetOpStrategy);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :dupColIdx */
	local_node->dupColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->dupColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :dupOperators */
	local_node->dupOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */

			token = pg_strtok(&length);	/* get operator namespace */
			nspname = nullable_string(token, length);

			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);

			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);

			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);

			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);

			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);

			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;

			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;

			local_node->dupOperators[i] = get_operid(oprname,
													 oprleft,
													 oprright,
													 NSP_OID(nspname));
		}
		else
		{
			token = pg_strtok(&length);
			local_node->dupOperators[i] = atooid(token);
		}
	}

	READ_INT_FIELD(flagColIdx);
	READ_INT_FIELD(firstFlag);
	READ_LONG_FIELD(numGroups);

	READ_DONE();
}

/*
 * _readLockRows
 */
static LockRows *
_readLockRows(void)
{
	READ_LOCALS(LockRows);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(rowMarks);
	READ_INT_FIELD(epqParam);

	READ_DONE();
}

/*
 * _readLimit
 */
static Limit *
_readLimit(void)
{
	READ_LOCALS(Limit);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(limitOffset);
	READ_NODE_FIELD(limitCount);
	IF_EXIST(limitOption)
	{
		READ_ENUM_FIELD(limitOption, LimitOption);
	}
	IF_EXIST(uniqNumCols)
	{
		READ_INT_FIELD(uniqNumCols);
	}
	IF_EXIST(uniqColIdx)
	{
		READ_ATTRNUMBER_ARRAY(uniqColIdx, local_node->uniqNumCols);
	}
	IF_EXIST(uniqOperators)
	{
		READ_OID_ARRAY(uniqOperators, local_node->uniqNumCols);
	}
	IF_EXIST(uniqCollations)
	{
		READ_OID_ARRAY(uniqCollations, local_node->uniqNumCols);
	}

	READ_DONE();
}

static ConnectBy *
_readConnectBy(void)
{
	READ_LOCALS(ConnectBy);

	ReadCommonPlan(&local_node->plan);

	READ_BOOL_FIELD(nocycle);
	READ_BOOL_FIELD(reset_cache);
	READ_BOOL_FIELD(qual_again);

	READ_INT_FIELD(nparams);
	if (local_node->nparams > 0)
	{
		READ_ATTRNUMBER_ARRAY(priorIdx, local_node->nparams);
		READ_INT_ARRAY(paramIdx, local_node->nparams);
	}

	READ_INT_FIELD(nconnect);
	if (local_node->nconnect > 0)
	{
		READ_ATTRNUMBER_ARRAY(connectIdx, local_node->nconnect);
		READ_OID_ARRAY(connectOperators, local_node->nconnect);
	}

	READ_NODE_FIELD(sort);
	IF_EXIST(cb_flags)
	{
		READ_CHAR_FIELD(cb_flags);
	}

	READ_DONE();
}

/*
 * _readNestLoopParam
 */
static NestLoopParam *
_readNestLoopParam(void)
{
	READ_LOCALS(NestLoopParam);

	READ_INT_FIELD(paramno);
	READ_NODE_FIELD(paramval);

	READ_DONE();
}

/*
 * _readPlanRowMark
 */
static PlanRowMark *
_readPlanRowMark(void)
{
	READ_LOCALS(PlanRowMark);

	READ_UINT_FIELD(rti);
	READ_UINT_FIELD(prti);
	READ_UINT_FIELD(rowmarkId);
	READ_ENUM_FIELD(markType, RowMarkType);
	READ_INT_FIELD(allMarkTypes);
	READ_ENUM_FIELD(strength, LockClauseStrength);
	READ_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	IF_EXIST(waitTimeout)
	{
		READ_INT_FIELD(waitTimeout);
	}
	READ_BOOL_FIELD(isParent);

	READ_DONE();
}

static PartitionPruneStepOp *
_readPartitionPruneStepOp(void)
{
   READ_LOCALS(PartitionPruneStepOp);

   READ_INT_FIELD(step.step_id);
   READ_INT_FIELD(opstrategy);
   READ_NODE_FIELD(exprs);
   READ_NODE_FIELD(cmpfns);
   READ_BITMAPSET_FIELD(nullkeys);

   READ_DONE();
}

static PartitionPruneStepCombine *
_readPartitionPruneStepCombine(void)
{
   READ_LOCALS(PartitionPruneStepCombine);

   READ_INT_FIELD(step.step_id);
   READ_ENUM_FIELD(combineOp, PartitionPruneCombineOp);
   READ_NODE_FIELD(source_stepids);

   READ_DONE();
}

static PartitionPruneInfo *
_readPartitionPruneInfo(void)
{
	READ_LOCALS(PartitionPruneInfo);

	READ_NODE_FIELD(prune_infos);
	READ_BITMAPSET_FIELD(other_subplans);

	READ_DONE();
}

static PartitionedRelPruneInfo *
_readPartitionedRelPruneInfo(void)
{
	READ_LOCALS(PartitionedRelPruneInfo);

#ifdef __OPENTENBASE__
	if (portable_input)
	{
		READ_RELID_FIELD(reloid);
	}
	else
#endif
	READ_OID_FIELD(reloid);
	READ_NODE_FIELD(pruning_steps);
	READ_BITMAPSET_FIELD(present_parts);
	READ_INT_FIELD(nparts);
	READ_INT_FIELD(nexprs);
	READ_INT_ARRAY(subplan_map, local_node->nparts);
	READ_INT_ARRAY(subpart_map, local_node->nparts);
	READ_BOOL_ARRAY(hasexecparam, local_node->nexprs);
	READ_BOOL_FIELD(do_initial_prune);
	READ_BOOL_FIELD(do_exec_prune);
	READ_BITMAPSET_FIELD(execparamids);

	READ_DONE();
}

/*
 * _readPlanInvalItem
 */
static PlanInvalItem *
_readPlanInvalItem(void)
{
	READ_LOCALS(PlanInvalItem);

	READ_INT_FIELD(cacheId);
	READ_UINT_FIELD(hashValue);

	READ_DONE();
}

/*
 * _readSubPlan
 */
static SubPlan *
_readSubPlan(void)
{
	READ_LOCALS(SubPlan);

	READ_ENUM_FIELD(subLinkType, SubLinkType);
	READ_NODE_FIELD(testexpr);
	READ_NODE_FIELD(paramIds);
	READ_INT_FIELD(plan_id);
	READ_STRING_FIELD(plan_name);
	if (portable_input)
		READ_TYPID_FIELD(firstColType);
	else
		READ_OID_FIELD(firstColType);
	READ_INT_FIELD(firstColTypmod);
	if (portable_input)
		READ_COLLID_FIELD(firstColCollation);
	else
		READ_OID_FIELD(firstColCollation);
	READ_BOOL_FIELD(useHashTable);
	READ_BOOL_FIELD(unknownEqFalse);
	READ_BOOL_FIELD(parallel_safe);
	READ_NODE_FIELD(setParam);
	READ_NODE_FIELD(parParam);
	READ_NODE_FIELD(args);
	READ_FLOAT_FIELD(startup_cost);
	READ_FLOAT_FIELD(per_call_cost);
#ifdef __OPENTENBASE_C__
	READ_BOOL_FIELD(isInitPlan);
#endif

	READ_DONE();
}

/*
 * _readAlternativeSubPlan
 */
static AlternativeSubPlan *
_readAlternativeSubPlan(void)
{
	READ_LOCALS(AlternativeSubPlan);

	READ_NODE_FIELD(subplans);

	READ_DONE();
}

/*
 * _readExtensibleNode
 */
static ExtensibleNode *
_readExtensibleNode(void)
{
	const ExtensibleNodeMethods *methods;
	ExtensibleNode *local_node;
	const char *extnodename;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length); /* skip :extnodename */
	token = pg_strtok(&length); /* get extnodename */

	extnodename = nullable_string(token, length);
	if (!extnodename)
		elog(ERROR, "extnodename has to be supplied");
	methods = GetExtensibleNodeMethods(extnodename, false);

	local_node = (ExtensibleNode *) newNode(methods->node_size,
											T_ExtensibleNode);
	local_node->extnodename = extnodename;

	/* deserialize the private fields */
	methods->nodeRead(local_node);

	READ_DONE();
}

/*
 * _readRemoteSubplan
 */
static RemoteSubplan *
_readRemoteSubplan(void)
{
	READ_LOCALS(RemoteSubplan);
	ReadCommonScan(&local_node->scan);
#ifdef __OPENTENBASE_C__
	READ_INT_FIELD(fid);
	READ_INT_FIELD(param_fid);
	READ_INT_FIELD(flevel);
	READ_NODE_FIELD(targetNodes);
	READ_INT_FIELD(targetNodeType);
	READ_INT_FIELD(protocol);
	READ_BOOL_FIELD(localSend);
	READ_BOOL_FIELD(under_subplan);

	READ_INT_FIELD(nparams);
	if (local_node->nparams > 0)
	{
		int i = 0;
		local_node->remoteparams = (RemoteParam *) palloc(
				local_node->nparams * sizeof(RemoteParam));
		for (i = 0; i < local_node->nparams; i++)
		{
			RemoteParam *remoteparams = (RemoteParam *)local_node->remoteparams;
			RemoteParam *rparam = &(remoteparams[i]);
			token = pg_strtok(&length); /* skip  :paramkind */
			token = pg_strtok(&length);
			rparam->paramkind = (ParamKind) atoi(token);

			token = pg_strtok(&length); /* skip  :paramid */
			token = pg_strtok(&length);
			rparam->paramid = atoi(token);

			token = pg_strtok(&length); /* skip  :paramused */
			token = pg_strtok(&length);
			rparam->paramused = atoi(token);

			token = pg_strtok(&length); /* skip  :paramtype */
			if (portable_input)
			{
				char	   *nspname; /* namespace name */
				char	   *typname; /* data type name */
				token = pg_strtok(&length); /* get nspname */
				nspname = nullable_string(token, length);
				token = pg_strtok(&length); /* get typname */
				typname = nullable_string(token, length);
				if (typname)
					rparam->paramtype = get_typname_typid(typname,
														  NSP_OID(nspname));
				else
					rparam->paramtype = InvalidOid;
			}
			else
			{
				token = pg_strtok(&length);
				rparam->paramtype = atooid(token);
			}
		}
	}
	else
		local_node->remoteparams = NULL;
#endif
	READ_CHAR_FIELD(distributionType);
#ifdef __OPENTENBASE_C__
	READ_INT_FIELD(ndiskeys);
	if (local_node->ndiskeys > 0)
	{
		int i = 0;
		local_node->diskeys = (AttrNumber *)palloc(sizeof(AttrNumber) * local_node->ndiskeys);
		for (i = 0; i < local_node->ndiskeys; i++)
		{
			token = pg_strtok(&length); /* skip  :diskey */
			token = pg_strtok(&length);
			local_node->diskeys[i] = (AttrNumber) atoi(token);
		}
	}
	else 
		local_node->diskeys = NULL;
#endif
	READ_NODE_FIELD(distributionNodes);
	READ_NODE_FIELD(distributionRestrict);
	READ_NODE_FIELD(nodeList);
	READ_BOOL_FIELD(execOnAll);
	READ_NODE_FIELD(sort);
	READ_STRING_FIELD(cursor);
	READ_BOOL_FIELD(roundrobin_distributed);
	READ_BOOL_FIELD(roundrobin_replicate);
	READ_NODE_FIELD(retain_list);
	READ_NODE_FIELD(replicated_list);
	READ_INT_FIELD(num_workers);
	READ_INT_FIELD(num_virtualdop);
	READ_BOOL_FIELD(cacheSend);
	READ_BOOL_FIELD(transfer_datarow);
	READ_BITMAPSET_FIELD(initParam);
	READ_NODE_FIELD(dests);
	/* begin ora_compatible */
	IF_EXIST(multi_dist)
	{
		READ_NODE_FIELD(multi_dist);
	}
	IF_EXIST(startno)
	{
		READ_INT_FIELD(startno);
	}
	IF_EXIST(endno)
	{
		READ_INT_FIELD(endno);
	}
	IF_EXIST(has_else)
	{
		READ_BOOL_FIELD(has_else);
	}
	IF_EXIST(only_one_send_tuple)
	{
		READ_BOOL_FIELD(only_one_send_tuple);
	}
	/* end ora_compatible */
	READ_DONE();
}

#ifdef __OPENTENBASE__
/*
 * _readRemoteQuery
 */
static RemoteQuery *
_readRemoteQuery(void)
{
	int i;
	READ_LOCALS(RemoteQuery);
	ReadCommonScan(&local_node->scan);
	READ_ENUM_FIELD(exec_direct_type, ExecDirectType);
	READ_BOOL_FIELD(exec_direct_dn_allow);
	READ_STRING_FIELD(sql_statement);
	READ_NODE_FIELD(exec_nodes);
	READ_ENUM_FIELD(combine_type, CombineType);
	READ_NODE_FIELD(sort);
	READ_BOOL_FIELD(read_only);
	READ_BOOL_FIELD(force_autocommit);
	READ_STRING_FIELD(statement);
	READ_STRING_FIELD(cursor);
	READ_INT_FIELD(rq_num_params);
	if (local_node->rq_num_params > 0)
	{
		local_node->rq_param_types = (Oid *) palloc(local_node->rq_num_params * sizeof(Oid));
		token = pg_strtok(&length); /* skip :rq_param_types */
		for (i = 0; i < local_node->rq_num_params; i++)
		{
			token = pg_strtok(&length); 
			local_node->rq_param_types[i] = atooid(token);
		}
	}
	READ_ENUM_FIELD(exec_type, RemoteQueryExecType);
	READ_INT_FIELD(reduce_level);
	READ_STRING_FIELD(outer_alias);
	READ_STRING_FIELD(inner_alias);
	READ_INT_FIELD(outer_reduce_level);
	READ_INT_FIELD(inner_reduce_level);
	READ_BITMAPSET_FIELD(outer_relids);
	READ_BITMAPSET_FIELD(inner_relids);
	READ_STRING_FIELD(inner_statement);
	READ_STRING_FIELD(outer_statement);
	READ_STRING_FIELD(join_condition);
	READ_BOOL_FIELD(has_row_marks);
	READ_BOOL_FIELD(has_ins_child_sel_parent);

	READ_BOOL_FIELD(rq_finalise_aggs);
	READ_BOOL_FIELD(rq_sortgroup_colno);
	READ_NODE_FIELD(remote_query);
	READ_NODE_FIELD(base_tlist);
	READ_NODE_FIELD(coord_var_tlist);
	READ_NODE_FIELD(query_var_tlist);
	READ_BOOL_FIELD(is_temp);
	READ_ENUM_FIELD(rcmdtype, RCmdType);

	READ_DONE();
}

static ExecNodes *
_readExecNodes(void)
{
	READ_LOCALS(ExecNodes);

	READ_NODE_FIELD(primarynodelist);
	READ_NODE_FIELD(nodeList);
	READ_CHAR_FIELD(baselocatortype);
#ifdef __OPENTENBASE__
	if (portable_input)
	{
		READ_RELID_FIELD(en_relid);
	}
	else
#endif
	READ_OID_FIELD(en_relid);
	READ_ENUM_FIELD(accesstype, RelationAccessType);
	READ_STRING_FIELD(g_index_table_name);
	READ_DONE();
}
#endif


#ifdef __AUDIT_FGA__
static AuditFgaPolicy *
_readAuditFgaStmt(void)
    {
        READ_LOCALS(AuditFgaPolicy);
    
        READ_STRING_FIELD(policy_name);
        READ_NODE_FIELD(qual);
        READ_STRING_FIELD(query_string);

        READ_DONE();
    }

#endif


/*
 * _readRemoteStmt
 */
static RemoteStmt *
_readRemoteStmt(void)
{
	int i;
	READ_LOCALS(RemoteStmt);

	READ_ENUM_FIELD(commandType, CmdType);
	READ_BOOL_FIELD(hasReturning);
	READ_INT_FIELD(jitFlags);
	READ_NODE_FIELD(planTree);
	READ_NODE_FIELD(rtable);
	READ_NODE_FIELD(resultRelations);
	READ_NODE_FIELD(nonleafResultRelations);
	READ_NODE_FIELD(rootResultRelations);
	READ_NODE_FIELD(subplans);
	READ_BITMAPSET_FIELD(rewindPlanIDs);
	READ_NODE_FIELD(paramExecTypes);
	READ_INT_FIELD(nParamRemote);
	if (local_node->nParamRemote > 0)
	{
		local_node->remoteparams = (RemoteParam *) palloc(
				local_node->nParamRemote * sizeof(RemoteParam));
		for (i = 0; i < local_node->nParamRemote; i++)
		{
			RemoteParam *rparam = &(local_node->remoteparams[i]);
			token = pg_strtok(&length); /* skip  :paramkind */
			token = pg_strtok(&length);
			rparam->paramkind = (ParamKind) atoi(token);

			token = pg_strtok(&length); /* skip  :paramid */
			token = pg_strtok(&length);
			rparam->paramid = atoi(token);

			token = pg_strtok(&length); /* skip  :paramused */
			token = pg_strtok(&length);
			rparam->paramused = atoi(token);

			token = pg_strtok(&length); /* skip  :paramtype */
			if (portable_input)
			{
				char	   *nspname; /* namespace name */
				char	   *typname; /* data type name */
				token = pg_strtok(&length); /* get nspname */
				nspname = nullable_string(token, length);
				token = pg_strtok(&length); /* get typname */
				typname = nullable_string(token, length);
				if (typname)
					rparam->paramtype = get_typname_typid(typname,
														  NSP_OID(nspname));
				else
					rparam->paramtype = InvalidOid;
			}
			else
			{
				token = pg_strtok(&length);
				rparam->paramtype = atooid(token);
			}
		}
	}
	else
		local_node->remoteparams = NULL;

	READ_NODE_FIELD(rowMarks);
#ifdef __OPENTENBASE_C__
	READ_INT_FIELD(ndiskeys);
	if (local_node->ndiskeys > 0)
	{
		int i = 0;
		local_node->diskeys = (AttrNumber *)palloc0(sizeof(AttrNumber) * local_node->ndiskeys);
		for (i = 0; i < local_node->ndiskeys; i++)
		{
			token = pg_strtok(&length); /* skip  :diskey */
			token = pg_strtok(&length);
			local_node->diskeys[i] = (AttrNumber) atoi(token);
		}
	}
	else 
		local_node->diskeys = NULL;
#endif
#ifdef __OPENTENBASE__
	READ_BOOL_FIELD(parallelModeNeeded);
#endif
#ifdef __OPENTENBASE_C__
	READ_INT_FIELD(fragmentType);
	READ_BITMAPSET_FIELD(sharedCtePlanIds);
	READ_INT_FIELD(fragment_work_mem);
	READ_INT_FIELD(fragment_num);
#endif
#ifdef __AUDIT__
	READ_STRING_FIELD(queryString);
	READ_NODE_FIELD(parseTree);
#endif

	/* begin ora_compatible */
	IF_EXIST(multi_dist)
	{
		READ_NODE_FIELD(multi_dist);
	}
	IF_EXIST(startno)
	{
		READ_INT_FIELD(startno);
	}
	IF_EXIST(endno)
	{
		READ_INT_FIELD(endno);
	}
	IF_EXIST(has_else)
	{
		READ_BOOL_FIELD(has_else);
	}
	/* end ora_compatible */
	IF_EXIST(queryid.timestamp_nodeid)
	{
		READ_LONG_FIELD(queryid.timestamp_nodeid);
	}
	IF_EXIST(queryid.sequence)
	{
		READ_LONG_FIELD(queryid.sequence);
	}
	READ_DONE();
}


/*
 * _readSimpleSort
 */
static SimpleSort *
_readSimpleSort(void)
{
	int i;
	READ_LOCALS(SimpleSort);

	READ_INT_FIELD(numCols);

	token = pg_strtok(&length);		/* skip :sortColIdx */
	local_node->sortColIdx = (AttrNumber *) palloc(local_node->numCols * sizeof(AttrNumber));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->sortColIdx[i] = atoi(token);
	}

	token = pg_strtok(&length);		/* skip :sortOperators */
	local_node->sortOperators = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *oprname; /* operator name */
			char	   *leftnspname; /* left type namespace */
			char	   *leftname; /* left type name */
			Oid			oprleft; /* left type */
			char	   *rightnspname; /* right type namespace */
			char	   *rightname; /* right type name */
			Oid			oprright; /* right type */
			/* token is already set to nspname */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get operator name */
			oprname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type namespace */
			leftnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* left type name */
			leftname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type namespace */
			rightnspname = nullable_string(token, length);
			token = pg_strtok(&length); /* right type name */
			rightname = nullable_string(token, length);
			if (leftname)
				oprleft = get_typname_typid(leftname,
											NSP_OID(leftnspname));
			else
				oprleft = InvalidOid;
			if (rightname)
				oprright = get_typname_typid(rightname,
											 NSP_OID(rightnspname));
			else
				oprright = InvalidOid;
			local_node->sortOperators[i] = get_operid(oprname,
													  oprleft,
													  oprright,
													  NSP_OID(nspname));
		}
		else
			local_node->sortOperators[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :sortCollations */
	local_node->sortCollations = (Oid *) palloc(local_node->numCols * sizeof(Oid));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		if (portable_input)
		{
			char       *nspname; /* namespace name */
			char       *collname; /* collation name */
			int 		collencoding; /* collation encoding */
			/* the token is already read */
			nspname = nullable_string(token, length);
			token = pg_strtok(&length); /* get collname */
			collname = nullable_string(token, length);
			token = pg_strtok(&length); /* get nargs */
			collencoding = atoi(token);
			if (collname)
				local_node->sortCollations[i] = get_collid(collname,
													   collencoding,
													   NSP_OID(nspname));
			else
				local_node->sortCollations[i] = InvalidOid;
		}
		else
			local_node->sortCollations[i] = atooid(token);
	}

	token = pg_strtok(&length);		/* skip :nullsFirst */
	local_node->nullsFirst = (bool *) palloc(local_node->numCols * sizeof(bool));
	for (i = 0; i < local_node->numCols; i++)
	{
		token = pg_strtok(&length);
		local_node->nullsFirst[i] = strtobool(token);
	}

	READ_DONE();
}


/*
 * _readPartitionBoundSpec
 */
static PartitionBoundSpec *
_readPartitionBoundSpec(void)
{
	READ_LOCALS(PartitionBoundSpec);

	READ_CHAR_FIELD(strategy);
	READ_BOOL_FIELD(is_default);
	READ_INT_FIELD(modulus);
	READ_INT_FIELD(remainder);
	READ_NODE_FIELD(listdatums);
	READ_NODE_FIELD(lowerdatums);
	READ_NODE_FIELD(upperdatums);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readPartitionRangeDatum
 */
static PartitionRangeDatum *
_readPartitionRangeDatum(void)
{
	READ_LOCALS(PartitionRangeDatum);

	READ_ENUM_FIELD(kind, PartitionRangeDatumKind);
	READ_NODE_FIELD(value);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#ifdef __AUDIT__

static AuditStmt *
_readAuditStmt(void)
{
	READ_LOCALS(AuditStmt);

	READ_BOOL_FIELD(audit_ison);
	READ_ENUM_FIELD(audit_type, AuditType);
	READ_ENUM_FIELD(audit_mode, AuditMode);
	READ_NODE_FIELD(action_list);
	READ_NODE_FIELD(user_list);
	READ_NODE_FIELD(object_name);
	READ_ENUM_FIELD(object_type, ObjectType);

	READ_DONE();
}

static CleanAuditStmt *
_readCleanAuditStmt(void)
{
	READ_LOCALS(CleanAuditStmt);

	READ_ENUM_FIELD(clean_type, CleanAuditType);
	READ_NODE_FIELD(user_list);
	READ_NODE_FIELD(object_name);
	READ_ENUM_FIELD(object_type, ObjectType);

	READ_DONE();
}
#endif

#ifdef __RESOURCE_QUEUE__
static CreateResourceQueueStmt *
_readCreateResourceQueueStmt(void)
{
	READ_LOCALS(CreateResourceQueueStmt);

	READ_STRING_FIELD(queue);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterResourceQueueStmt *
_readAlterResourceQueueStmt(void)
{
	READ_LOCALS(AlterResourceQueueStmt);

	READ_STRING_FIELD(queue);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static DropResourceQueueStmt *
_readDropResourceQueueStmt(void)
{
	READ_LOCALS(DropResourceQueueStmt);

	READ_STRING_FIELD(queue);

	READ_DONE();
}
#endif

#ifdef __OPENTENBASE__
static PlaceHolderVar *
_readPlaceHolderVar(void)
{
	READ_LOCALS(PlaceHolderVar);

	READ_NODE_FIELD(phexpr);
	READ_BITMAPSET_FIELD(phrels);
	READ_UINT_FIELD(phid);
	READ_UINT_FIELD(phlevelsup);

	READ_DONE();
}

static PlaceHolderInfo *
_readPlaceHolderInfo(void)
{
	READ_LOCALS(PlaceHolderInfo);

	READ_UINT_FIELD(phid);
	READ_NODE_FIELD(ph_var);
	READ_BITMAPSET_FIELD(ph_eval_at);
	READ_BITMAPSET_FIELD(ph_lateral);
	READ_BITMAPSET_FIELD(ph_needed);
	READ_INT_FIELD(ph_width);

	READ_DONE();
}
#endif

#ifdef _PG_ORCL_
static PartitionInto *
_readPartitionInto(void)
{
	READ_LOCALS(PartitionInto);

	READ_NODE_FIELD(part_name);
	READ_STRING_FIELD(part_tablespace);

	READ_DONE();
}

static PartitionMergeSplit *
_readPartitionMergeSplit(void)
{
	READ_LOCALS(PartitionMergeSplit);

	READ_NODE_FIELD(part_ext_names);
	READ_NODE_FIELD(into_parts);
	READ_ENUM_FIELD(stp, MergeSplitStep);
	READ_NODE_FIELD(from_rel);
	READ_BOOL_FIELD(is_merge);
	READ_BOOL_FIELD(is_partition);
	READ_NODE_FIELD(bound);

	READ_DONE();
}

static PartitionMergeSpec *
_readPartitionMergeSpec(void)
{
	READ_LOCALS(PartitionMergeSpec);

	READ_CHAR_FIELD(strategy);
	READ_INT_FIELD(partnatts);

	if (local_node->partnatts > 0)
	{
		READ_OID_ARRAY(partopclass, local_node->partnatts);
		READ_OID_ARRAY(partcollation, local_node->partnatts);
		READ_ATTRNUMBER_ARRAY(partattrs, local_node->partnatts);
	}

	READ_NODE_FIELD(partexprs);

	READ_DONE();
}

static RownumExpr *
_readRownumExpr(void)
{
    READ_LOCALS(RownumExpr);

    READ_INT_FIELD(location);
    READ_DONE();
}
#endif

static MergeQualProj *
_readMergeQualProj(void)
{
	READ_LOCALS(MergeQualProj);

	ReadCommonPlan(&local_node->plan);

	READ_NODE_FIELD(mergeActionList);
	READ_DONE();
}


static FdwDataNodeTask* 
_readFdwDataNodeTask(void)
{
    READ_LOCALS(FdwDataNodeTask);

    READ_STRING_FIELD(dnName);
	READ_STRING_FIELD(instant_time);
    READ_NODE_FIELD(task);

    READ_DONE();
}

static FdwFileSegment* 
_readFdwFileSegment(void)
{
    READ_LOCALS(FdwFileSegment);

    READ_STRING_FIELD(filename);
    READ_LONG_FIELD(begin);
	READ_LONG_FIELD(end);
    READ_LONG_FIELD(fdwfileSize);
	READ_NODE_FIELD(hudi_delta_log);

    READ_DONE();
}

static FragmentDest *
_readFragmentDest(void)
{
	READ_LOCALS(FragmentDest);

	READ_INT_FIELD(fid);
	READ_INT_FIELD(recvfid);
	READ_INT_FIELD(targetNodeType);
	READ_NODE_FIELD(targetNodes);

	READ_DONE();
}

static PriorExpr *
_readPriorExpr(void)
{
	READ_LOCALS(PriorExpr);

	READ_NODE_FIELD(expr);
	IF_EXIST(for_path)
	{
		READ_BOOL_FIELD(for_path);
	}
	IF_EXIST(location)
	{
		READ_INT_FIELD(location);
	}
	READ_DONE();
}

static LevelExpr *
_readLevelExpr(void)
{
	READ_LOCALS(LevelExpr);

	READ_INT_FIELD(location);
	READ_DONE();
}

static CBIsCycleExpr *
_readCBIsCycleExpr(void)
{
	READ_LOCALS(CBIsCycleExpr);

	READ_INT_FIELD(location);
	READ_DONE();
}

static CBIsLeafExpr *
_readCBIsLeafExpr(void)
{
	READ_LOCALS(CBIsLeafExpr);

	READ_INT_FIELD(location);
	READ_DONE();
}

static CBRootExpr *
_readCBRootExpr(void)
{
	READ_LOCALS(CBRootExpr);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(location);
	READ_DONE();
}

static CBPathExpr *
_readCBPathExpr(void)
{
	READ_LOCALS(CBPathExpr);

	READ_NODE_FIELD(expr);
	READ_INT_FIELD(location);
	READ_DONE();
}

static MultiSetExpr *
_readMultiSetExpr(void)
{
	READ_LOCALS(MultiSetExpr);

	READ_NODE_FIELD(lexpr);
	READ_NODE_FIELD(rexpr);
	READ_ENUM_FIELD(setop, SetOperation);
	READ_BOOL_FIELD(all);
	READ_INT_FIELD(location);
	READ_DONE();
}

/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
Node *
parseNodeString(void)
{
	void	   *return_value;

	READ_TEMP_LOCALS();

	token = pg_strtok(&length);

#define MATCH(tokname, namelen) \
	(length == namelen && memcmp(token, tokname, namelen) == 0)

	if (MATCH("QUERY", 5))
		return_value = _readQuery();
	else if (MATCH("WITHCHECKOPTION", 15))
		return_value = _readWithCheckOption();
	else if (MATCH("SORTGROUPCLAUSE", 15))
		return_value = _readSortGroupClause();
	else if (MATCH("GROUPINGSET", 11))
		return_value = _readGroupingSet();
	else if (MATCH("WINDOWCLAUSE", 12))
		return_value = _readWindowClause();
	else if (MATCH("ROWMARKCLAUSE", 13))
		return_value = _readRowMarkClause();
	else if (MATCH("COMMONTABLEEXPR", 15))
		return_value = _readCommonTableExpr();
	else if (MATCH("SETOPERATIONSTMT", 16))
		return_value = _readSetOperationStmt();
	else if (MATCH("ALIAS", 5))
		return_value = _readAlias();
	else if (MATCH("RANGEVAR", 8))
		return_value = _readRangeVar();
	else if (MATCH("INTOCLAUSE", 10))
		return_value = _readIntoClause();
	else if (MATCH("TABLEFUNC", 9))
		return_value = _readTableFunc();
	else if (MATCH("VAR", 3))
		return_value = _readVar();
	else if (MATCH("CONST", 5))
		return_value = _readConst();
	else if (MATCH("CONSTRAINT", 10))
		return_value = _readConstraint();
	else if (MATCH("PARAM", 5))
		return_value = _readParam();
	else if (MATCH("AGGREF", 6))
		return_value = _readAggref();
	else if (MATCH("GROUPINGFUNC", 12))
		return_value = _readGroupingFunc();
	else if (MATCH("WINDOWFUNC", 10))
		return_value = _readWindowFunc();
	else if (MATCH("ARRAYREF", 8))
		return_value = _readArrayRef();
	else if (MATCH("FUNCEXPR", 8))
		return_value = _readFuncExpr();
	else if (MATCH("NAMEDARGEXPR", 12))
		return_value = _readNamedArgExpr();
	else if (MATCH("OPEXPR", 6))
		return_value = _readOpExpr();
	else if (MATCH("DISTINCTEXPR", 12))
		return_value = _readDistinctExpr();
	else if (MATCH("NULLIFEXPR", 10))
		return_value = _readNullIfExpr();
	else if (MATCH("SCALARARRAYOPEXPR", 17))
		return_value = _readScalarArrayOpExpr();
	else if (MATCH("BOOLEXPR", 8))
		return_value = _readBoolExpr();
	else if (MATCH("SUBLINK", 7))
		return_value = _readSubLink();
	else if (MATCH("FIELDSELECT", 11))
		return_value = _readFieldSelect();
	else if (MATCH("FIELDSTORE", 10))
		return_value = _readFieldStore();
	else if (MATCH("RELABELTYPE", 11))
		return_value = _readRelabelType();
	else if (MATCH("COERCEVIAIO", 11))
		return_value = _readCoerceViaIO();
	else if (MATCH("ARRAYCOERCEEXPR", 15))
		return_value = _readArrayCoerceExpr();
	else if (MATCH("CONVERTROWTYPEEXPR", 18))
		return_value = _readConvertRowtypeExpr();
	else if (MATCH("COLLATE", 7))
		return_value = _readCollateExpr();
	else if (MATCH("CASE", 4))
		return_value = _readCaseExpr();
	else if (MATCH("WHEN", 4))
		return_value = _readCaseWhen();
	else if (MATCH("CASETESTEXPR", 12))
		return_value = _readCaseTestExpr();
	else if (MATCH("ARRAY", 5))
		return_value = _readArrayExpr();
	else if (MATCH("ROW", 3))
		return_value = _readRowExpr();
	else if (MATCH("ROWCOMPARE", 10))
		return_value = _readRowCompareExpr();
	else if (MATCH("COALESCE", 8))
		return_value = _readCoalesceExpr();
	else if (MATCH("MINMAX", 6))
		return_value = _readMinMaxExpr();
	else if (MATCH("SQLVALUEFUNCTION", 16))
		return_value = _readSQLValueFunction();
	else if (MATCH("NEXTVALUEEXPR", 13))
		return_value = _readNextValueExpr();
	else if (MATCH("XMLEXPR", 7))
		return_value = _readXmlExpr();
	else if (MATCH("NULLTEST", 8))
		return_value = _readNullTest();
	else if (MATCH("BOOLEANTEST", 11))
		return_value = _readBooleanTest();
	else if (MATCH("COERCETODOMAIN", 14))
		return_value = _readCoerceToDomain();
	else if (MATCH("COERCETODOMAINVALUE", 19))
		return_value = _readCoerceToDomainValue();
	else if (MATCH("SETTODEFAULT", 12))
		return_value = _readSetToDefault();
	else if (MATCH("CURRENTOFEXPR", 13))
		return_value = _readCurrentOfExpr();
	else if (MATCH("NEXTVALUEEXPR", 13))
		return_value = _readNextValueExpr();
	else if (MATCH("INFERENCEELEM", 13))
		return_value = _readInferenceElem();
	else if (MATCH("TARGETENTRY", 11))
		return_value = _readTargetEntry();
	else if (MATCH("RANGETBLREF", 11))
		return_value = _readRangeTblRef();
	else if (MATCH("JOINEXPR", 8))
		return_value = _readJoinExpr();
	else if (MATCH("FROMEXPR", 8))
		return_value = _readFromExpr();
    else if (MATCH("MERGEACTION", 11))
        return_value = _readMergeAction();
	else if (MATCH("ONCONFLICTEXPR", 14))
		return_value = _readOnConflictExpr();
	else if (MATCH("CONNECTBYEXPR", 13))
		return_value = _readConnectByExpr();
	else if (MATCH("RTE", 3))
		return_value = _readRangeTblEntry();
	else if (MATCH("RANGETBLFUNCTION", 16))
		return_value = _readRangeTblFunction();
	else if (MATCH("TABLESAMPLECLAUSE", 17))
		return_value = _readTableSampleClause();
	else if (MATCH("NOTIFY", 6))
		return_value = _readNotifyStmt();
	/* begin ora_compatible */
	else if (MATCH("MultiInsertInto", 15))
		return_value = _readMultiInsertInto();
	else if (MATCH("MULTIMODIFYTABLE", 16))
		return_value = _readMultiModifyTable();
	/* end ora_compatible */
	else if (MATCH("DEFELEM", 7))
		return_value = _readDefElem();
	else if (MATCH("DECLARECURSOR", 13))
		return_value = _readDeclareCursorStmt();
	else if (MATCH("PLANNEDSTMT", 11))
		return_value = _readPlannedStmt();
	else if (MATCH("PLAN", 4))
		return_value = _readPlan();
	else if (MATCH("RESULT", 6))
		return_value = _readResult();
	else if (MATCH("PROJECTSET", 10))
		return_value = _readProjectSet();
	else if (MATCH("MODIFYTABLE", 11))
		return_value = _readModifyTable();
	else if (MATCH("MERGEQUALPROJ", 13))
		return_value = _readMergeQualProj();
	else if (MATCH("PARTITERATOR", 12))
		return_value = _readPartIterator();
	else if (MATCH("APPEND", 6))
		return_value = _readAppend();
	else if (MATCH("MERGEAPPEND", 11))
		return_value = _readMergeAppend();
    else if (MATCH("MERGEINTO", 9))
        return_value = _readMergeStmt();
	else if (MATCH("RECURSIVEUNION", 14))
		return_value = _readRecursiveUnion();
	else if (MATCH("BITMAPAND", 9))
		return_value = _readBitmapAnd();
	else if (MATCH("BITMAPOR", 8))
		return_value = _readBitmapOr();
	else if (MATCH("SCAN", 4))
		return_value = _readScan();
	else if (MATCH("SEQSCAN", 7))
		return_value = _readSeqScan();
	else if (MATCH("EXTERNALSCAN", 12))
		return_value = _readExternalScanInfo();
	else if (MATCH("SAMPLESCAN", 10))
		return_value = _readSampleScan();
	else if (MATCH("INDEXSCAN", 9))
		return_value = _readIndexScan();
	else if (MATCH("INDEXONLYSCAN", 13))
		return_value = _readIndexOnlyScan();
	else if (MATCH("BITMAPINDEXSCAN", 15))
		return_value = _readBitmapIndexScan();
	else if (MATCH("BITMAPHEAPSCAN", 14))
		return_value = _readBitmapHeapScan();
	else if (MATCH("TIDSCAN", 7))
		return_value = _readTidScan();
	else if (MATCH("SUBQUERYSCAN", 12))
		return_value = _readSubqueryScan();
	else if (MATCH("FUNCTIONSCAN", 12))
		return_value = _readFunctionScan();
	else if (MATCH("VALUESSCAN", 10))
		return_value = _readValuesScan();
	else if (MATCH("TABLEFUNCSCAN", 13))
		return_value = _readTableFuncScan();
	else if (MATCH("CTESCAN", 7))
		return_value = _readCteScan();
	else if (MATCH("WORKTABLESCAN", 13))
		return_value = _readWorkTableScan();
	else if (MATCH("FOREIGNSCAN", 11))
		return_value = _readForeignScan();
	else if (MATCH("CUSTOMSCAN", 10))
		return_value = _readCustomScan();
	else if (MATCH("JOIN", 4))
		return_value = _readJoin();
	else if (MATCH("NESTLOOP", 8))
		return_value = _readNestLoop();
	else if (MATCH("MERGEJOIN", 9))
		return_value = _readMergeJoin();
	else if (MATCH("HASHJOIN", 8))
		return_value = _readHashJoin();
	else if (MATCH("MATERIAL", 8))
		return_value = _readMaterial();
	else if (MATCH("SORT", 4))
		return_value = _readSort();
	else if (MATCH("GROUP", 5))
		return_value = _readGroup();
	else if (MATCH("AGG", 3))
		return_value = _readAgg();
	else if (MATCH("WINDOWAGG", 9))
		return_value = _readWindowAgg();
	else if (MATCH("UNIQUE", 6))
		return_value = _readUnique();
	else if (MATCH("GATHER", 6))
		return_value = _readGather();
	else if (MATCH("GATHERMERGE", 11))
		return_value = _readGatherMerge();
	else if (MATCH("HASH", 4))
		return_value = _readHash();
	else if (MATCH("SETOP", 5))
		return_value = _readSetOp();
	else if (MATCH("LOCKROWS", 8))
		return_value = _readLockRows();
	else if (MATCH("LIMIT", 5))
		return_value = _readLimit();
	else if (MATCH("CONNECTBY", 9))
		return_value = _readConnectBy();
	else if (MATCH("NESTLOOPPARAM", 13))
		return_value = _readNestLoopParam();
	else if (MATCH("PLANROWMARK", 11))
		return_value = _readPlanRowMark();
	else if (MATCH("PARTITIONPRUNESTEPOP", 20))
		return_value = _readPartitionPruneStepOp();
	else if (MATCH("PARTITIONPRUNESTEPCOMBINE", 25))
		return_value = _readPartitionPruneStepCombine();
	else if (MATCH("PARTITIONPRUNEINFO", 18))
		return_value = _readPartitionPruneInfo();
	else if (MATCH("PARTITIONEDRELPRUNEINFO", 23))
		return_value = _readPartitionedRelPruneInfo();
	else if (MATCH("PLANINVALITEM", 13))
		return_value = _readPlanInvalItem();
	else if (MATCH("SUBPLAN", 7))
		return_value = _readSubPlan();
	else if (MATCH("ALTERNATIVESUBPLAN", 18))
		return_value = _readAlternativeSubPlan();
	else if (MATCH("EXTENSIBLENODE", 14))
		return_value = _readExtensibleNode();
	else if (MATCH("REMOTESUBPLAN", 13))
		return_value = _readRemoteSubplan();
	else if (MATCH("REMOTESTMT", 10))
		return_value = _readRemoteStmt();
	else if (MATCH("SIMPLESORT", 10))
		return_value = _readSimpleSort();
	else if (MATCH("PARTITIONBOUNDSPEC", 18))
		return_value = _readPartitionBoundSpec();
	else if (MATCH("PARTITIONRANGEDATUM", 19))
		return_value = _readPartitionRangeDatum();
#ifdef _PG_ORCL_
	else if (MATCH("PARTITIONINTO", 13))
		return_value = _readPartitionInto();
	else if (MATCH("PARTITIONMERGESPLIT", 19))
		return_value = _readPartitionMergeSplit();
	else if (MATCH("PARTITIONMERGESPEC", 18))
		return_value = _readPartitionMergeSpec();
    else if (MATCH("ROWNUMEXPR", 10))
        return_value = _readRownumExpr();
#endif
#ifdef __AUDIT_FGA__
    else if (MATCH("AUDITFGAPOLICY", 14))
		return_value = _readAuditFgaStmt();
#endif
#ifdef __OPENTENBASE__
	else if (MATCH("REMOTEQUERY", 11))
		return_value = _readRemoteQuery();	
	else if (MATCH("EXEC_NODES", 10))
		return_value = _readExecNodes();
	else if (MATCH("PLACEHOLDERVAR", 14))
		return_value = _readPlaceHolderVar();
	else if (MATCH("PLACEHOLDERINFO", 15))
		return_value = _readPlaceHolderInfo();
#endif
#ifdef __AUDIT__
	else if (MATCH("AUDIT", 5))
		return_value = _readAuditStmt();
	else if (MATCH("CLEAN_AUDIT", 11))
		return_value = _readCleanAuditStmt();
#endif
#ifdef __RESOURCE_QUEUE__
	else if (MATCH("CREATERESQUEUESTMT", 18))
		return_value = _readCreateResourceQueueStmt();
	else if (MATCH("ALTERRESQUEUESTMT", 17))
		return_value = _readAlterResourceQueueStmt();
	else if (MATCH("DROPRESQUEUESTMT", 16))
		return_value = _readDropResourceQueueStmt();
#endif
    else if (MATCH("FDWDATANODETASK", 15))
        return_value = _readFdwDataNodeTask();
    else if (MATCH("FDWFILESEGMENT", 14))
        return_value = _readFdwFileSegment();
	else if (MATCH("FRAGMENTDEST", 12))
		return_value = _readFragmentDest();
	else if (MATCH("COPYFORMATTER", 13))
		return_value = _readCopyFormatter();
	else if (MATCH("COPYATTRFIXED", 13))
		return_value = _readCopyAttrFixed();
	else if (MATCH("PRIOREXPR", 9))
		return_value = _readPriorExpr();
	else if (MATCH("LEVELEXPR", 9))
		return_value = _readLevelExpr();
	else if (MATCH("CBISCYCLEEXPR", 13))
		return_value = _readCBIsCycleExpr();
	else if (MATCH("CBISLEAFEXPR", 12))
		return_value = _readCBIsLeafExpr();
	else if (MATCH("CBROOTEXPR", 10))
		return_value = _readCBRootExpr();
	else if (MATCH("CBPATHEXPR", 10))
		return_value = _readCBPathExpr();
	else if (MATCH("MULTISETEXPR", 12))
		return_value = _readMultiSetExpr();
	else
	{
		elog(ERROR, "badly formatted node string \"%.32s\"...", token);
		return_value = NULL;	/* keep compiler quiet */
	}

	return (Node *) return_value;
}


/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
Datum
readDatum(bool typbyval)
{
	Size		length,
				i;
	int			tokenLength;
	char	   *token;
	Datum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	token = pg_strtok(&tokenLength);
	length = atoui(token);

	token = pg_strtok(&tokenLength);	/* read the '[' */
	if (token == NULL || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
			 token ? (const char *) token : "[NULL]", length);

	if (typbyval)
	{
		if (length > (Size) sizeof(Datum))
			elog(ERROR, "byval datum but length = %zu", length);
		res = (Datum) 0;
		s = (char *) (&res);
		for (i = 0; i < (Size) sizeof(Datum); i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (Datum) NULL;
	else
	{
		s = (char *) palloc(length);
		for (i = 0; i < length; i++)
		{
			token = pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
		res = PointerGetDatum(s);
	}

	token = pg_strtok(&tokenLength);	/* read the ']' */
	if (token == NULL || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
			 token ? (const char *) token : "[NULL]", length);

	return res;
}

#ifdef XCP
/*
 * scanDatum
 *
 * Recreate Datum from the text format understandable by the input function
 * of the specified data type.
 */
static Datum
scanDatum(Oid typid, int typmod)
{
	Oid			typInput;
	Oid			typioparam;
	FmgrInfo	finfo;
	FunctionCallInfoData fcinfo;
	char	   *value;
	Datum		res;
	READ_TEMP_LOCALS();

	/* Get input function for the type */
	getTypeInputInfo(typid, &typInput, &typioparam);
	fmgr_info(typInput, &finfo);

	/* Read the value */
	token = pg_strtok(&length);
	value = nullable_string(token, length);

	/* The value can not be NULL, so we actually received empty string */
	if (value == NULL)
		value = "";

	/* Invoke input function */
	InitFunctionCallInfoData(fcinfo, &finfo, 3, InvalidOid, NULL, NULL);

	fcinfo.arg[0] = CStringGetDatum(value);
	fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
	fcinfo.arg[2] = Int32GetDatum(typmod);
	fcinfo.argnull[0] = false;
	fcinfo.argnull[1] = false;
	fcinfo.argnull[2] = false;

	res = FunctionCallInvoke(&fcinfo);

	return res;
}
#endif

/*
 * readAttrNumberCols
 */
AttrNumber *
readAttrNumberCols(int numCols)
{
	int			tokenLength,
				i;
	char	   *token;
	AttrNumber *attr_vals;

	if (numCols <= 0)
		return NULL;

	attr_vals = (AttrNumber *) palloc(numCols * sizeof(AttrNumber));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		attr_vals[i] = atoi(token);
	}

	return attr_vals;
}

/*
 * readOidCols
 */
Oid *
readOidCols(int numCols)
{
	int			tokenLength,
				i;
	char	   *token;
	Oid		   *oid_vals;

	if (numCols <= 0)
		return NULL;

	oid_vals = (Oid *) palloc(numCols * sizeof(Oid));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		oid_vals[i] = atooid(token);
	}

	return oid_vals;
}

/*
 * readIntCols
 */
int *
readIntCols(int numCols)
{
	int			tokenLength,
				i;
	char	   *token;
	int		   *int_vals;

	if (numCols <= 0)
		return NULL;

	int_vals = (int *) palloc(numCols * sizeof(int));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		int_vals[i] = atoi(token);
	}

	return int_vals;
}

/*
 * readBoolCols
 */
bool *
readBoolCols(int numCols)
{
	int			tokenLength,
				i;
	char	   *token;
	bool	   *bool_vals;

	if (numCols <= 0)
		return NULL;

	bool_vals = (bool *) palloc(numCols * sizeof(bool));
	for (i = 0; i < numCols; i++)
	{
		token = pg_strtok(&tokenLength);
		bool_vals[i] = strtobool(token);
	}

	return bool_vals;
}

#ifdef PGXC
/*
 * An object's oid is different between different nodes, so we send the object 
 * name and namespace to remote node, and re-parse the object name to local OID.
 * But sometimes no need to do this. For example, when we send a SQL statement
 * "create table t (a int default func())" to other node, and the func's LOCAL 
 * OID will be recorded in pg_attrdef's adbin, so no need to re-parse the 
 * object name to LOCAL OID.
 */
void * 
stringToNode_skip_extern_fields(char *str)
{
	void *retval = NULL;

	PG_TRY();
	{
		skip_read_extern_fields = true;
		retval = stringToNode(str);
		skip_read_extern_fields = false;
	}
	PG_CATCH();
	{
		skip_read_extern_fields = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return retval;
}
#endif /* PGXC */
