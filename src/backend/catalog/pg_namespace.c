/*-------------------------------------------------------------------------
 *
 * pg_namespace.c
 *	  routines to support manipulation of the pg_namespace relation
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_namespace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/formatting.h"
#include "utils/guc.h"


/* inplace upgrade */
Oid			inplace_upgrade_next_pg_namespace_oid = InvalidOid;


/* ----------------
 * NamespaceCreate
 *
 * Create a namespace (schema) with the given name and owner OID.
 *
 * If isTemp is true, this schema is a per-backend schema for holding
 * temporary tables.  Currently, it is used to prevent it from being
 * linked as a member of any active extension.  (If someone does CREATE
 * TEMP TABLE in an extension script, we don't want the temp schema to
 * become part of the extension). And to avoid checking for default ACL
 * for temp namespace (as it is not necessary).
 * ---------------
 */
Oid
NamespaceCreate(const char *nspName, Oid ownerId, bool isTemp)
{
	Relation	nspdesc;
	HeapTuple	tup;
	Oid			nspoid;
	bool		nulls[Natts_pg_namespace];
	Datum		values[Natts_pg_namespace];
	NameData	nname;
	TupleDesc	tupDesc;
	ObjectAddress myself;
	int			i;
	Acl		   *nspacl;

	/* sanity checks */
	if (!nspName)
		elog(ERROR, "no namespace name supplied");

	if (ORA_MODE && IsSystemNamespaceName(nspName))
		nspName = asc_tolower(nspName, strlen(nspName));

	/* make sure there is no existing namespace of same name */
	if (SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(nspName)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_SCHEMA),
				 errmsg("schema \"%s\" already exists", nspName)));

	if (!isTemp)
		nspacl = get_user_default_acl(ACL_OBJECT_NAMESPACE, ownerId,
									  InvalidOid);
	else
		nspacl = NULL;

	/* initialize nulls and values */
	for (i = 0; i < Natts_pg_namespace; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) NULL;
	}
	namestrcpy(&nname, nspName);
	values[Anum_pg_namespace_nspname - 1] = NameGetDatum(&nname);
	values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(ownerId);
	if (nspacl != NULL)
		values[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(nspacl);
	else
		nulls[Anum_pg_namespace_nspacl - 1] = true;

	nspdesc = heap_open(NamespaceRelationId, RowExclusiveLock);
	tupDesc = nspdesc->rd_att;

	tup = heap_form_tuple(tupDesc, values, nulls);

	if (IsInplaceUpgrade && inplace_upgrade_next_pg_namespace_oid != InvalidOid)
	{
		HeapTupleSetOid(tup, inplace_upgrade_next_pg_namespace_oid);
		inplace_upgrade_next_pg_namespace_oid = InvalidOid;
	}

	nspoid = CatalogTupleInsert(nspdesc, tup);
	Assert(OidIsValid(nspoid));

	heap_close(nspdesc, RowExclusiveLock);

	/* Record dependencies */
	myself.classId = NamespaceRelationId;
	myself.objectId = nspoid;
	myself.objectSubId = 0;

	if (IsInplaceUpgrade && myself.objectId < FirstBootstrapObjectId)
	{
		recordPinnedDependency(&myself);
	}
	else
	{
		/* dependency on owner */
		recordDependencyOnOwner(NamespaceRelationId, nspoid, ownerId);

		/* dependences on roles mentioned in default ACL */
		recordDependencyOnNewAcl(NamespaceRelationId, nspoid, 0, ownerId, nspacl);

		/* dependency on extension ... but not for magic temp schemas */
		if (!isTemp)
			recordDependencyOnCurrentExtension(&myself, false);
	}
	/* Post creation hook for new schema */
	InvokeObjectPostCreateHook(NamespaceRelationId, nspoid, 0);

	return nspoid;
}


bool
IsSystemNamespaceName(const char *nspname)
{
	return strcmp(nspname, "PG_CATALOG") == 0 ||
			strcmp(nspname, "PG_TOTAST") == 0 ||
			strcmp(nspname, "PUBLIC") == 0 ||
			strcmp(nspname, "OPENTENBASE_ORA") == 0 ||
			strcmp(nspname, "PG_SLICE") == 0 ||
			strcmp(nspname, "ESTORE") == 0;
}
