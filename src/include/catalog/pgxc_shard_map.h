/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#ifndef PGXC_SHARD_MAP_H
#define PGXC_SHARD_MAP_H

#include "catalog/genbki.h"
/* oid */
#define PgxcShardMapRelationId  9023

CATALOG(pgxc_shard_map,9023) BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
    Oid     disgroup;
    int32    shardgroupid;
    int16    ncopy;    
    Oid        primarycopy;    /* data node OID */
    Oid        copy1;
    Oid        copy2;
    char    primarystatus;    
    char    status1;
    char    status2;    
    int16   extended;      
    int32   ntuples;
} FormData_pgxc_shard_map;

typedef FormData_pgxc_shard_map *Form_pgxc_shard_map;
/* the numbers of attribute  */
#define Natts_pgxc_shard_map                    11
/* the number of attribute  */
#define Anum_pgxc_shard_map_nodegroup           1
#define Anum_pgxc_shard_map_shardgroupid        2
#define Anum_pgxc_shard_map_ncopy                3
#define Anum_pgxc_shard_map_primarycopy            4
#define Anum_pgxc_shard_map_copy1                5
#define Anum_pgxc_shard_map_copy2                6
#define Anum_pgxc_shard_map_primarystatus        7
#define Anum_pgxc_shard_map_status1                8
#define Anum_pgxc_shard_map_status2                9
#define Anum_pgxc_shard_map_extend                10
#define Anum_pgxc_shard_map_ntuples                11


#define SHARD_MAP_STATUS_ERROR        'E'
#define SHARD_MAP_STATUS_UNDEFINED    'N'
#define SHARD_MAP_STATUS_USING        'U'
#define SHARD_MAP_STATUS_MIGRATING    'M'
#define SHARD_MAP_STATUS_INVALID    'I'
/* related function declaration */
extern bool is_group_sharding_inited(Oid group);
extern void InitShardMap(int32 nShardGroup, int32 nNodes, Oid *nodes, int16 extend, Oid nodeGroup);
extern bool is_already_inited(void);
extern bool NodeHasShard(Oid node);
extern void UpdateRelationShardMap(Oid group, Oid from_node, Oid to_node,
                                int shardgroup_num,    int* shardgroups);

extern void DropShardMap_Node(Oid group);
#endif  /* PGXC_SHARD_MAP_H */
