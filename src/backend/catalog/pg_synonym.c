/*-------------------------------------------------------------------------
 *
 * pg_synonym.c
 *	  routines to support manipulation of the pg_synonym relation
 *
 * Portions Copyright (c) 2020-2030, Tencent.com.
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_synonym.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_namespace.h"
#include "catalog/objectaccess.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * CreateSynonym
 */
Oid
CreateSynonym(char *synname, Oid synspcId, char *objspc, char *objname,
				char *dblink, Oid ownerId, bool isreplace)
{
	bool		nulls[Natts_pg_synonym];
	Datum		values[Natts_pg_synonym];
	bool		replaces[Natts_pg_synonym];
	int		i = 0;
	NameData	n_sname,
				n_ospc,
				n_oname,
				n_dblink;
	Relation	synRel;
	HeapTuple	oldtup;
	HeapTuple	tup;
	Oid		synId;
	ObjectAddress	myself;
	ObjectAddress	referenced;

	for (i = 0; i < Natts_pg_synonym; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) NULL;
		replaces[i] = false;
	}

	namestrcpy(&n_sname, synname);
	values[Anum_pg_synonym_synname - 1] = NameGetDatum(&n_sname);
	values[Anum_pg_synonym_synnamespace - 1] = ObjectIdGetDatum(synspcId);

	if (objspc)
	{
		namestrcpy(&n_ospc, objspc);
		values[Anum_pg_synonym_objnamespace - 1] = NameGetDatum(&n_ospc);
	}
	else
		nulls[Anum_pg_synonym_objnamespace - 1] = true;

	namestrcpy(&n_oname, objname);
	values[Anum_pg_synonym_objname - 1] = NameGetDatum(&n_oname);

	if (dblink)
	{
		namestrcpy(&n_dblink, dblink);
		values[Anum_pg_synonym_objdblink - 1] = NameGetDatum(&n_dblink);
	}
	else
		nulls[Anum_pg_synonym_objdblink - 1] = true;

	values[Anum_pg_synonym_synowner - 1] = ObjectIdGetDatum(ownerId);

	synRel = heap_open(SynonymRelationId, RowExclusiveLock);
	oldtup = SearchSysCache2(SYNONYMNAME,
							 CStringGetDatum(synname),
							 ObjectIdGetDatum(synspcId));
	if (HeapTupleIsValid(oldtup))
	{
		if (!isreplace)
			elog(ERROR, "synonym \"%s\" already exists", synname);

		replaces[Anum_pg_synonym_objname - 1] = true;
		replaces[Anum_pg_synonym_objnamespace - 1] = true;
		replaces[Anum_pg_synonym_objdblink - 1] = true;

		tup = heap_modify_tuple(oldtup, RelationGetDescr(synRel), values, nulls, replaces);
		CatalogTupleUpdate(synRel, &tup->t_self, tup);

		ReleaseSysCache(oldtup);

		synId = HeapTupleGetOid(tup);

		/* No any dependency changes. */
	}
	else
	{
		tup = heap_form_tuple(RelationGetDescr(synRel), values, nulls);

		synId = CatalogTupleInsert(synRel, tup);
		Assert(OidIsValid(synId));

		/* Depend on creator */
		recordDependencyOnOwner(SynonymRelationId, synId, ownerId);

		/* Depend on schema */
		myself.classId = SynonymRelationId;
		myself.objectId = synId;
		myself.objectSubId = 0;
		referenced.classId = NamespaceRelationId;
		referenced.objectId = synspcId;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}
	heap_close(synRel, RowExclusiveLock);

	heap_freetuple(tup);

	return synId;
}
