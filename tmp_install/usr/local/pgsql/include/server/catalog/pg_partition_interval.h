/*-------------------------------------------------------------------------
 *
 * pg_partition_interval.h
 *      definition of the system "interval partitioned table" relation
 *      along with the relation's initial contents.
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * src/include/catalog/pg_partition_interval.h
 *
 * NOTES
 *      the genbki.sh script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PARTITION_INTERVAL_H
#define PG_PARTITION_INTERVAL_H

#include "catalog/genbki.h"

#include "datatype/timestamp.h"

#define timestamptz int64

#define PgPartitionIntervalRelationId 8100

CATALOG(pg_partition_interval,8100) BKI_WITHOUT_OIDS
{
    Oid            partrelid;
    int16       partpartkey;
    int16        partinterval_type;
    Oid            partdatatype;
    int32        partnparts;
    int64        partstartvalue_int;
    timestamptz    partstartvalue_ts;
    int32        partinterval_int;
}FormData_pg_partition_interval;

#undef timestamptz

typedef FormData_pg_partition_interval *Form_pg_partition_interval;


#define Natts_pg_partition_interval                        8

#define Anum_pg_partition_interval_partrelid            1
#define Anum_pg_partition_interval_partpartkey            2
#define Anum_pg_partition_interval_partinterval_type    3
#define Anum_pg_partition_interval_partdatatype            4
#define Anum_pg_partition_interval_partnparts            5
#define Anum_pg_partition_interval_partstartvalue_int    6
#define Anum_pg_partition_interval_partstartvalue_ts    7
#define Anum_pg_partition_interval_partinterval_int        8


typedef enum PartitionIntervalType
{
    IntervalType_Int2,
    IntervalType_Int4,
    IntervalType_Int8,
    IntervalType_Hour,
    IntervalType_Day,
    IntervalType_Month,
    IntervalType_Year,
    IntervalType_Reserve
} PartitionIntervalType;


extern void CreateIntervalPartition(Oid relid, 
                                AttrNumber partkey,
                                int16 intervaltype,
                                Oid partdatatype,
                                int nparts, 
                                int64 startval, 
                                int32 interval);

extern void RemoveIntervalPartition(Oid relid);

extern void AddPartitions(Oid relid, int num);

extern bool IsIntervalPartition(Oid relid);

extern void ModifyPartitionStartValue(Oid relid, int64 startval);
#endif   /* PG_PARTITION_INTERVAL_H */
