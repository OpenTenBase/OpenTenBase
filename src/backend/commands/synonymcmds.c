/*-------------------------------------------------------------------------
 *
 * synonymcmds.c
 *	  synonym creation/manipulation commands
 *
 * Portions Copyright (c) 2020-2030, Tencent OpenTenBase Develop Team.
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/synonym.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_synonym.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/objectaccess.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * CreateSynonym
 *   Create a synonym under a schema.
 */
ObjectAddress
CreateSynonymCommand(CreateSynonymStmt *stmt)
{
	AclResult	aclresult;
	Oid		namespaceId;
	char	*objschm,
			*objname;
	Oid		synId;
	ObjectAddress addr;

	/*
	 * For PUBLIC synonym, error out if a synonym schema presents.
	 */
	if (stmt->ispublic && stmt->synname->schemaname != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("can not create PUBLIC synonym under a schema")));
	else if (stmt->ispublic)
		namespaceId = PG_PUBLIC_NAMESPACE;

	else
		namespaceId = RangeVarGetCreationNamespace(stmt->synname);
	if (namespaceId == PG_PUBLIC_NAMESPACE)
	{
		if (!stmt->ispublic)
		{
			elog(NOTICE, "create synonym as PUBLIC");
			stmt->ispublic = true;
		}
	}

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));

	/* Deconstruct the object name */
	DeconstructQualifiedName(stmt->objname, &objschm, &objname);

	synId = CreateSynonym(stmt->synname->relname, namespaceId, objschm, objname,
							stmt->dblink, GetUserId(), stmt->replace);
	CommandCounterIncrement();

	InvokeObjectPostCreateHook(SynonymRelationId, synId, 0);

	addr.classId = SynonymRelationId;
	addr.objectId = synId;
	addr.objectSubId = 0;
	return addr;
}

/*
 * Guts of schema deletion.
 */
void
RemoveSynonymById(Oid synOid)
{
	Relation	relation;
	HeapTuple	tup;

	relation = heap_open(SynonymRelationId, RowExclusiveLock);

	tup = SearchSysCache1(SYNONYMOID,
						  ObjectIdGetDatum(synOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for synonym %u", synOid);

	CatalogTupleDelete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}

char *
get_synonym_name(Oid synspcId)
{
	HeapTuple	tup;
	char	*name;
	Form_pg_synonym syntup;

	tup = SearchSysCache1(SYNONYMOID,
							 ObjectIdGetDatum(synspcId));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "failed to lookup synonym name for Oid: %u", synspcId);

	syntup = (Form_pg_synonym) GETSTRUCT(tup);
	name = pstrdup(NameStr(syntup->synname));

	ReleaseSysCache(tup);
	return name;
}

Oid
get_synonym_oid(List *name, bool missok)
{
	Oid		namespaceId,
			synid = InvalidOid;
	HeapTuple	tup;
	char	*syname;
	bool	ispublic;
	RangeVar	*rname = lsecond_node(RangeVar, name);
	List	*nspcIds;
	ListCell	*l;

	syname = rname->relname;
	ispublic = (intVal(linitial(name)) == 1);
	if (ispublic)
	{
		nspcIds = list_make1_oid(PG_PUBLIC_NAMESPACE);
		if (rname->schemaname != NULL)
		{
			List	*input_sch = get_synonym_lookup_schema(rname->schemaname);
			if (input_sch == NULL || linitial_oid(input_sch) != PG_PUBLIC_NAMESPACE)
				ereport(ERROR,
				 (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("PUBLIC synonym \"%s\" should not have schema",
                               syname)));
		}
	}
	else
		nspcIds = get_synonym_lookup_schema(rname->schemaname);

	foreach(l, nspcIds)
	{
		namespaceId = lfirst_oid(l);
		tup = SearchSysCache2(SYNONYMNAME,
								 CStringGetDatum(syname),
								 ObjectIdGetDatum(namespaceId));

		if (HeapTupleIsValid(tup))
		{
			synid = HeapTupleGetOid(tup);
			ReleaseSysCache(tup);
			break;
		}
	}


	if (!missok && synid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("synonym \"%s\" does not exist", syname)));
	return synid;
}

typedef struct
{
	List	*syn_oid;
	char	**objname;
	char	**objspcname;
	char	**objdblink;
} resolve_synonym_context;

/*
 * get_synonym_object
 *   Get the object namespace and name from synonym.
 */
static void
get_synonym_object(resolve_synonym_context *ctxt, const char *synspc, const char *synname)
{
	List	*synnsp_Ids;
	ListCell	*l;
	Relation	rel;

	synnsp_Ids = get_synonym_lookup_schema(synspc);
	if (synnsp_Ids == NULL)
		return;

	rel = heap_open(SynonymRelationId, AccessShareLock);
	foreach(l, synnsp_Ids)
	{
		Oid		synnspId = lfirst_oid(l),
				synId;
		HeapTuple	tup;
		Datum	value;
		bool	isnull;

		tup = SearchSysCache2(SYNONYMNAME,
							CStringGetDatum(synname), ObjectIdGetDatum(synnspId));
		if (!HeapTupleIsValid(tup))
			continue;

		/* Record the last found */
		value = heap_getattr(tup, Anum_pg_synonym_objnamespace,
									RelationGetDescr(rel), &isnull);
		if (isnull)
			*ctxt->objspcname = NULL;
		else
			*ctxt->objspcname = pstrdup(NameStr(*DatumGetName(value)));

		value = heap_getattr(tup, Anum_pg_synonym_objname,
									RelationGetDescr(rel), &isnull);
		Assert(!isnull);
		*ctxt->objname = pstrdup(NameStr(*DatumGetName(value)));

		value = heap_getattr(tup, Anum_pg_synonym_objdblink,
									RelationGetDescr(rel), &isnull);
		if (isnull)
			*ctxt->objdblink = NULL;
		else
			*ctxt->objdblink = pstrdup(NameStr(*DatumGetName(value)));

		synId = HeapTupleGetOid(tup);
		if (list_member_oid(ctxt->syn_oid, synId))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("synonyms defined as a loop")));
		ctxt->syn_oid = lappend_oid(ctxt->syn_oid, synId);

		ReleaseSysCache(tup);
		break;
	}

	heap_close(rel, AccessShareLock);

	if (*ctxt->objdblink != NULL || l == NULL)
		return;

	/* To see if treated as a synonym like opentenbase_ora */
	get_synonym_object(ctxt, *ctxt->objspcname, *ctxt->objname);
}

Oid
resolve_synonym(const char *synspc, const char *synname, char **objspc, char **objname, char **dblink)
{
	Oid						syn_oid = InvalidOid;
	resolve_synonym_context	ctxt;

	ctxt.objname = objname;
	ctxt.objspcname = objspc;
	ctxt.objdblink = dblink;
	ctxt.syn_oid = NIL;
	*ctxt.objname = *ctxt.objspcname = *ctxt.objdblink = NULL;

	get_synonym_object(&ctxt, synspc, synname);

	if (list_length(ctxt.syn_oid) == 1)
		syn_oid = linitial_oid(ctxt.syn_oid);

	list_free(ctxt.syn_oid);
	return syn_oid;
}
