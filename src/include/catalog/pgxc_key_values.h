/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PGXC_SECONDARY_DISTRIBUTE_H
#define PGXC_SECONDARY_DISTRIBUTE_H

#include "catalog/genbki.h"

#define PgxcKeyValueRelationId  9177

CATALOG(pgxc_key_value,9177) BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
    Oid         dboid;            /* database of table */
    Oid         reloid;            /* distribute table */    
    
    NameData       keyvalue;        /* special value of primary distribute key */    
    Oid         nodegroup;        /* hot group */
    Oid         coldnodegroup;  /* cold group */
}FormData_key_value;

typedef FormData_key_value *Form_pgxc_key_value;

#define Natts_pgxc_key_value                5

#define Anum_pgxc_key_valuew_db                1
#define Anum_pgxc_key_values_rel            2
#define Anum_pgxc_key_value_value            3
#define Anum_pgxc_key_value_group            4
#define Anum_pgxc_key_value_cold_group       5
#endif   /* PGXC_SECONDARY_DISTRIBUTE_H */

extern void CreateKeyValues(Oid db, Oid rel, int32 nValues, char **keyvalues, Oid nodeGroup, Oid coldGroup);

extern void CheckKeyValueGroupValid(Oid hot, Oid cold, bool create_table);

extern void CheckPgxcGroupValid(Oid tablehot);

extern bool GatherRelationKeyValueGroup(Oid relation, int32 *hot, Oid **group, int32 *cold, Oid **coldgroup);

extern void GetRelationSecondGroup(Oid rel, Oid **groups, int32 *nGroup);

extern char *BuildKeyValueCheckoverlapsStr(Oid hotgroup, Oid coldgroup);

extern char *BuildRelationCheckoverlapsStr(DistributeBy *distributeby, PGXCSubCluster *subcluster);

extern Oid GetKeyValuesGroup(Oid db, Oid rel, char *value, Oid *coldgroup);

extern bool IsKeyValues(Oid db, Oid rel, char *value);