/*-------------------------------------------------------------------------
 *
 * extprotocolcmds.c
 *
 *	  Routines for external protocol-manipulation commands
 *
 * src/backend/commands/extprotocolcmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extprotocolcmds.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"


/*
 *	DefineExtprotocol
 */
void
DefineExtProtocol(List *name, List *parameters, bool trusted)
{
	List	   *readfuncName = NIL;
	List	   *writefuncName = NIL;
	ListCell   *pl;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create an external protocol")));

	foreach(pl, parameters)
	{
		DefElem    *defel = (DefElem *) lfirst(pl);

		if (pg_strcasecmp(defel->defname, "readfunc") == 0)
			readfuncName = defGetQualifiedName(defel);
		else if (pg_strcasecmp(defel->defname, "writefunc") == 0)
			writefuncName = defGetQualifiedName(defel);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("protocol attribute \"%s\" not recognized",
							defel->defname)));
	}

	/*
	 * make sure we have our required definitions
	 */
	if (readfuncName == NULL && writefuncName == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("protocol must be specify at least a readfunc or a writefunc")));
}

/*
 * Drop PROTOCOL by OID. This is the guts of deletion.
 * This is called to clean up dependencies.
 */
void
RemoveExtProtocolById(Oid protOid)
{
	Relation	rel;
	ScanKeyData skey;
	SysScanDesc scan;
	HeapTuple	tup;
	bool		found = false;

	/*
	 * Search pg_extprotocol.
	 */
	rel = relation_open(ExtprotocolRelationId, RowExclusiveLock);

	ScanKeyInit(&skey,
				Anum_pg_extprotocol_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(protOid));
	scan = systable_beginscan(rel, ExtprotocolOidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		CatalogTupleDelete(rel, &tup->t_self);
		found = true;
	}
	systable_endscan(scan);

	if (!found)
		elog(ERROR, "protocol %u could not be found", protOid);

	relation_close(rel, NoLock);
}
