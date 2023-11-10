/*-------------------------------------------------------------------------
 *
 * pg_partition_interval.c
 *    routines to support manipulation of the interval partition relation
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/indexing.h"
#include "access/attnum.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/heapam.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"
#include "catalog/pg_partition_interval.h"


/* create interval partition table's metadata in pg_partition_interval */
void CreateIntervalPartition(Oid relid, 
                                AttrNumber partkey,
                                int16 intervaltype,
                                Oid partdatatype,
                                int nparts, 
                                int64 startval, 
                                int32 interval)
{
    Relation    rel;
    HeapTuple     tup;
    int64       startval_int;
    Timestamp   startval_ts;
    Datum           values[Natts_pg_partition_interval];
    bool        isnull[Natts_pg_partition_interval];

    switch(intervaltype)
    {
        case IntervalType_Int2:
        case IntervalType_Int4:
        case IntervalType_Int8:
            startval_int = startval;
            startval_ts  = 0;
            break;
        case IntervalType_Day:
        case IntervalType_Month:
            startval_int = 0;
            startval_ts  = startval;
            break;
        default:
            /* should not happen... */
            elog(ERROR, "unrecognized interval type: %d.", intervaltype);
            break;
    }

    values[Anum_pg_partition_interval_partrelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_partition_interval_partpartkey - 1] = Int16GetDatum(partkey);
    values[Anum_pg_partition_interval_partinterval_type - 1] = Int16GetDatum(intervaltype);
    values[Anum_pg_partition_interval_partdatatype - 1] = ObjectIdGetDatum(partdatatype);
    values[Anum_pg_partition_interval_partnparts - 1] = Int32GetDatum(nparts);
    values[Anum_pg_partition_interval_partstartvalue_int - 1] = Int64GetDatum(startval_int);
    values[Anum_pg_partition_interval_partstartvalue_ts - 1] = TimestampGetDatum(startval_ts);
    values[Anum_pg_partition_interval_partinterval_int - 1] = Int32GetDatum(interval);

    MemSet(isnull, 0, sizeof(isnull));

    rel = heap_open(PgPartitionIntervalRelationId, RowExclusiveLock);
    
    tup = heap_form_tuple(RelationGetDescr(rel), values, isnull);

    CatalogTupleInsert(rel, tup);

    heap_close(rel, RowExclusiveLock);
}

/* remove one interval partition from pg_partition_interval */
void RemoveIntervalPartition(Oid relid)
{
    Relation rel;
    HeapTuple tup;

    rel = heap_open(PgPartitionIntervalRelationId, RowExclusiveLock);
    tup = SearchSysCache1(PGPARTITIONINTERVALREL, relid);

    if(!HeapTupleIsValid(tup))
        elog(ERROR,"RemoveIntervalPartition: relid[%d] is not exist", relid);

    simple_heap_delete(rel, &tup->t_self);
    ReleaseSysCache(tup);
    heap_close(rel, RowExclusiveLock);
}

void AddPartitions(Oid relid, int num)
{
    Relation rel;
    HeapTuple tup;
    HeapTuple newtup;
    Form_pg_partition_interval pg_partition = NULL;

    Datum values[Natts_pg_partition_interval];
    bool  isnull[Natts_pg_partition_interval];
    bool  replace[Natts_pg_partition_interval];

    MemSet(values, 0, sizeof(values));
    MemSet(isnull, false, sizeof(isnull));
    MemSet(replace, false, sizeof(replace));

    replace[Anum_pg_partition_interval_partnparts - 1] = true;

    rel = heap_open(PgPartitionIntervalRelationId, RowExclusiveLock);
    tup = SearchSysCache1(PGPARTITIONINTERVALREL, relid);

    if(!HeapTupleIsValid(tup))
        elog(ERROR,"RemoveIntervalPartition: relid[%d] is not exist", relid);

    pg_partition = (Form_pg_partition_interval)GETSTRUCT(tup);

    values[Anum_pg_partition_interval_partnparts -1] = Int32GetDatum(pg_partition->partnparts + num);
    newtup = heap_modify_tuple(tup, 
                            RelationGetDescr(rel), 
                            values, isnull,
                            replace);

    CatalogTupleUpdate(rel, &tup->t_self, newtup);
    
    ReleaseSysCache(tup);
    heap_close(rel,RowExclusiveLock);
}

/* is given relation a interval partition */
bool
IsIntervalPartition(Oid relid)
{
    bool result = false;

    Relation rel;
    HeapTuple tup;

    rel = heap_open(PgPartitionIntervalRelationId, AccessShareLock);
    tup = SearchSysCache1(PGPARTITIONINTERVALREL, relid);

    if(!HeapTupleIsValid(tup))
        result = false;
    else
    {
        result = true;
        ReleaseSysCache(tup);
    }
        
    heap_close(rel, AccessShareLock);

    return result;
}

void
ModifyPartitionStartValue(Oid relid, int64 startval)
{
    Relation rel;
    HeapTuple tup;
    HeapTuple newtup;
    Form_pg_partition_interval pg_partition = NULL;

    Datum values[Natts_pg_partition_interval];
    bool  isnull[Natts_pg_partition_interval];
    bool  replace[Natts_pg_partition_interval];

    MemSet(values, 0, sizeof(values));
    MemSet(isnull, false, sizeof(isnull));
    MemSet(replace, false, sizeof(replace));

    rel = heap_open(PgPartitionIntervalRelationId, RowExclusiveLock);
    tup = SearchSysCache1(PGPARTITIONINTERVALREL, relid);

    if(!HeapTupleIsValid(tup))
        elog(ERROR,"RemoveIntervalPartition: relid[%d] is not exist", relid);

    pg_partition = (Form_pg_partition_interval)GETSTRUCT(tup);

    switch(pg_partition->partinterval_type)
    {
        case IntervalType_Int2:
        case IntervalType_Int4:
        case IntervalType_Int8:
            replace[Anum_pg_partition_interval_partstartvalue_int - 1] = true;
            values[Anum_pg_partition_interval_partstartvalue_int - 1] = Int64GetDatum(startval);
            break;
        case IntervalType_Day:
        case IntervalType_Month:
            replace[Anum_pg_partition_interval_partstartvalue_ts - 1] = true;
            values[Anum_pg_partition_interval_partstartvalue_ts - 1] = TimestampGetDatum(startval);
            break;
        default:
            /* should not happen... */
            elog(ERROR, "unrecognized interval type: %d.", pg_partition->partinterval_type);
            break;
    }

    newtup = heap_modify_tuple(tup, 
                            RelationGetDescr(rel), 
                            values, isnull,
                            replace);

    CatalogTupleUpdate(rel, &tup->t_self, newtup);
    
    ReleaseSysCache(tup);
    heap_close(rel,RowExclusiveLock);
}

