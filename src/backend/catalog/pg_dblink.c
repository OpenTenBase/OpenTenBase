/*-------------------------------------------------------------------------
 *
 * pg_dblink.c
 *	support opentenbase_ora database link
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <stdlib.h>
#include "tsearch/ts_locale.h"
#include "catalog/pg_dblink.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "catalog/pg_collation.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "access/xact.h"
#include "nodes/primnodes.h"

#include "tcop/utility.h"
#include "catalog/namespace.h"

/*
 * get server name from system table pg_dblink
 */
char *
GetForeignServerFromDblinkName(const char *dblink_name)
{
    Relation	rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple	tuple;
    Datum       server_datum;
    bool        server_is_null;
    StringInfoData buf;

    initStringInfo(&buf);

    rel = heap_open(DblinkRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0],
                Anum_pg_dblink_name,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dblink_name));

    scan = systable_beginscan(rel, PgDblinkNameIndexId, true, NULL, 1, skey);
    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
    {
		ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("dblink %s does not exist", dblink_name)));
    }

    server_datum = heap_getattr(tuple, Anum_pg_foreign_server,
                                        RelationGetDescr(rel), &server_is_null);
    if (!server_is_null)
    {
        if (ORA_MODE)
            appendStringInfoString(&buf, TextDatumGetCString(server_datum));
        else
            appendStringInfoString(&buf, lowerstr(TextDatumGetCString(server_datum)));
    }
    else
    {
    	appendStringInfoString(&buf, "public");
    }

    systable_endscan(scan);
    heap_close(rel, NoLock);

    return buf.data;
}

/*
 * get foreign default user name from system table pg_dblink
 */
char *
GetForeignUserFromDblinkName(const char *dblink_name)
{
    Relation	rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple	tuple;
    Datum       datum;
    bool        is_null;
    StringInfoData buf;

    initStringInfo(&buf);

    rel = heap_open(DblinkRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0],
                Anum_pg_dblink_name,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dblink_name));

    scan = systable_beginscan(rel, PgDblinkNameIndexId, true, NULL, 1, skey);
    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
    {
		ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("dblink %s does not exist", dblink_name)));
    }

    datum = heap_getattr(tuple, Anum_pg_dblink_user_name,
                                        RelationGetDescr(rel), &is_null);
    if (!is_null)
    {
        appendStringInfoString(&buf, NameStr(*(DatumGetName(datum))));
    }
    else
    {
    	elog(ERROR, "dblink:%s no foreign user", dblink_name);
    }

    systable_endscan(scan);
    heap_close(rel, NoLock);

    return buf.data;
}

/*
 * Create a database link for system table
 */
void
DblinkCreate(const char *dblink_name, 
						int dblink_kind, 
						const char *user_name,
						const char *host,
						int port,
						const char *foreign_server)
{
    Relation	rel;
    Datum		values[Natts_pg_dblink];
    bool		nulls[Natts_pg_dblink];
    HeapTuple	htup;

    /*
     * Insert tuple into pg_dblink.
     */
    rel = heap_open(DblinkRelationId, RowExclusiveLock);

    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_dblink_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(dblink_name));
    values[Anum_pg_dblink_owner - 1] = DirectFunctionCall1(namein, CStringGetDatum(GetUserNameFromId(GetUserId(), false)));
    values[Anum_pg_dblink_user_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(user_name));
    values[Anum_pg_dblink_created - 1] = TimestampGetDatum(GetCurrentTimestamp());
    values[Anum_pg_dblink_host - 1] = CStringGetTextDatum(host);
    values[Anum_pg_dblink_port - 1] = Int32GetDatum(port);
    values[Anum_pg_dblink_kind - 1] = Int32GetDatum(dblink_kind);
    values[Anum_pg_foreign_server - 1] = CStringGetTextDatum(foreign_server);

    htup = heap_form_tuple(rel->rd_att, values, nulls);
    CatalogTupleInsert(rel, htup);
    heap_close(rel, RowExclusiveLock);
}

/*
 * Drop a database link from system table
 */
void
DblinkDrop(const char* dblink_name)
{
    Relation	rel;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple	tuple;

    rel = heap_open(DblinkRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0],
                Anum_pg_dblink_name,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dblink_name));

    scan = systable_beginscan(rel, PgDblinkNameIndexId, true,
                              NULL, 1, skey);

    tuple = systable_getnext(scan);

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "dblink %s does not exist", dblink_name);

    CatalogTupleDelete(rel, &tuple->t_self);

    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);
}
