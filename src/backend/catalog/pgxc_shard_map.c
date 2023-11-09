/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#include "postgres.h"
#include "catalog/indexing.h"
#include "access/skey.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "catalog/pgxc_shard_map.h"


bool is_group_sharding_inited(Oid group)
{
    SysScanDesc scan;
    HeapTuple   oldtup;
    Relation    sharrel;
    bool         result;
    ScanKeyData skey;

    sharrel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    ScanKeyInit(&skey,
                Anum_pgxc_shard_map_nodegroup,
                BTEqualStrategyNumber, 
                F_OIDEQ,
                ObjectIdGetDatum(group));    
    
    scan    = systable_beginscan(sharrel,
                                  PgxcShardMapGroupIndexId, 
                                  true,
                                  NULL, 
                                  1, 
                                  &skey);                              

    oldtup  = systable_getnext(scan);

    result  = HeapTupleIsValid(oldtup);

    systable_endscan(scan);
    heap_close(sharrel, AccessShareLock);

    return result;
}

/* called when the first table created */
void InitShardMap(int32 nShardGroup, int32 nNodes, Oid *nodes, int16 extend, Oid nodeGroup)
{
    Relation    shardmaprel;
    HeapTuple    htup;
    bool        nulls[Natts_pgxc_shard_map];
    Datum        values[Natts_pgxc_shard_map];
    int        iShard;
    int        iAttr;
    //int        shards_of_dn;

    if(is_group_sharding_inited(nodeGroup))
    {
        return;
    }
    

    if(nNodes <= 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("Cannot initiate shard map when cluster has not any datanode")));
    }

    //shards_of_dn = (nShardGroup + nNodes - 1) / nNodes;
    
    /*make and insert shard map record*/
    for(iAttr = 0; iAttr < Natts_pgxc_shard_map; iAttr++)
    {
        nulls[iAttr]    =    false;
        values[iAttr]    =    (Datum)0;
    }

    values[Anum_pgxc_shard_map_nodegroup-1]            = ObjectIdGetDatum(nodeGroup);
    values[Anum_pgxc_shard_map_ncopy-1]             = UInt16GetDatum(1);
    values[Anum_pgxc_shard_map_copy1-1]                = ObjectIdGetDatum(InvalidOid);
    values[Anum_pgxc_shard_map_copy2-1]                = ObjectIdGetDatum(InvalidOid);
    values[Anum_pgxc_shard_map_primarystatus-1]        = CharGetDatum(SHARD_MAP_STATUS_USING);
    values[Anum_pgxc_shard_map_status1-1]            = CharGetDatum(SHARD_MAP_STATUS_UNDEFINED);
    values[Anum_pgxc_shard_map_status2-1]            = CharGetDatum(SHARD_MAP_STATUS_UNDEFINED);
    values[Anum_pgxc_shard_map_extend-1]             = UInt16GetDatum(extend);

    shardmaprel = heap_open(PgxcShardMapRelationId, AccessExclusiveLock);
    
    for(iShard = 0; iShard < nShardGroup; iShard++)
    {        
        values[Anum_pgxc_shard_map_shardgroupid - 1]     = UInt32GetDatum(iShard);        
        //values[Anum_pgxc_shard_map_primarycopy  - 1]    = ObjectIdGetDatum(nodes[iShard % nNodes]);
        values[Anum_pgxc_shard_map_primarycopy  - 1]    = ObjectIdGetDatum(nodes[iShard % nNodes]);

        htup = heap_form_tuple(shardmaprel->rd_att, values, nulls);

        CatalogTupleInsert(shardmaprel, htup);
    }    

    CommandCounterIncrement();

    heap_close(shardmaprel, AccessExclusiveLock);

    RegisterInvalidShmemShardMap(nodeGroup, ShardOpType_create);
}

bool 
is_already_inited()
{
    SysScanDesc scan;
    HeapTuple  oldtup;
    Relation sharrel;
    bool result;

    sharrel = heap_open(PgxcShardMapRelationId, AccessShareLock);

    scan    = systable_beginscan(sharrel,
                              InvalidOid, 
                              false,
                              NULL, 
                              0, 
                              NULL);

    oldtup  = systable_getnext(scan);

    result  = HeapTupleIsValid(oldtup);

    systable_endscan(scan);
    heap_close(sharrel, AccessShareLock);

    return result;
}

bool NodeHasShard(Oid node)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    ScanKeyData skey;
    SysScanDesc scan;
    bool result;

    result = false;
    
    shardmapRel = heap_open(PgxcShardMapRelationId, AccessShareLock);
    ScanKeyInit(&skey,
                    Anum_pgxc_shard_map_primarycopy,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(node));
    
    scan = systable_beginscan(shardmapRel,
                                PgxcShardMapNodeIndexId,true,
                                NULL,
                                1,&skey);

    oldtup = systable_getnext(scan);

    if(HeapTupleIsValid(oldtup))
    {
        result = true;
    }
    
    systable_endscan(scan);

    heap_close(shardmapRel,AccessShareLock);
    return result;
}


void UpdateRelationShardMap(Oid group, Oid from_node, Oid to_node,
                                int shardgroup_num,    int* shardgroups)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    HeapTuple   newtup;
    ScanKeyData skey[2];
    SysScanDesc scan;
    int i;
    
    Datum        new_record[Natts_pgxc_shard_map];
    bool        new_record_nulls[Natts_pgxc_shard_map];
    bool        new_record_repl[Natts_pgxc_shard_map];

    shardmapRel = heap_open(PgxcShardMapRelationId, AccessExclusiveLock);

    MemSet(new_record, 0, sizeof(new_record));
    MemSet(new_record_nulls, false, sizeof(new_record_nulls));
    MemSet(new_record_repl, false, sizeof(new_record_repl));

    new_record_repl[Anum_pgxc_shard_map_primarycopy - 1] = true;

    for(i = 0; i < shardgroup_num; i++)
    {
        if(i > 0 && shardgroups[i] <= shardgroups[i - 1])
        {
            elog(ERROR,"MOVE DATA: sharding group array is not sorted");
        }

        ScanKeyInit(&skey[0],
                    Anum_pgxc_shard_map_nodegroup,
                    BTEqualStrategyNumber, F_INT4EQ,
                    ObjectIdGetDatum(group));
        
        ScanKeyInit(&skey[1],
                    Anum_pgxc_shard_map_shardgroupid,
                    BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum(shardgroups[i]));

        scan = systable_beginscan(shardmapRel,
                                    PgxcShardMapShardIndexId,true,
                                    NULL,
                                    2, skey);
    
        oldtup = systable_getnext(scan);

        if(!HeapTupleIsValid(oldtup))
        {
            elog(ERROR,"update shard map failed: shardgroup id[%d] is not exist.", shardgroups[i]);
        }
        
        /* just need to update primarycopy*/
        new_record[Anum_pgxc_shard_map_primarycopy-1]    = ObjectIdGetDatum(to_node);

        newtup = heap_modify_tuple(oldtup, RelationGetDescr(shardmapRel),
                                       new_record,
                                       new_record_nulls, new_record_repl);

        CatalogTupleUpdate(shardmapRel, &newtup->t_self, newtup);

        systable_endscan(scan);
        
        pfree(newtup);
    }

    CommandCounterIncrement();

    heap_close(shardmapRel, AccessExclusiveLock);
    
    //RegisterInvalidShmemShardMap(group);
}

void DropShardMap_Node(Oid group)
{
    Relation    shardmapRel;
    HeapTuple   oldtup;
    SysScanDesc scan;
    Form_pgxc_shard_map shard_map;

    shardmapRel = heap_open(PgxcShardMapRelationId, AccessExclusiveLock);

        
    scan = systable_beginscan(shardmapRel,
                                InvalidOid,
                                false,
                                NULL,
                                0,
                                NULL);

    oldtup = systable_getnext(scan);    
    while(HeapTupleIsValid(oldtup))
    {
        shard_map = (Form_pgxc_shard_map)GETSTRUCT(oldtup);

        if(shard_map->disgroup == group)
        {
            simple_heap_delete(shardmapRel,&oldtup->t_self);
        }
        oldtup = systable_getnext(scan);
    }    

    systable_endscan(scan);

    CommandCounterIncrement();
    
    heap_close(shardmapRel,AccessExclusiveLock);

    RegisterInvalidShmemShardMap(group, ShardOpType_drop);
}

