#ifndef PGXC_SHARD_MAP_H
#define PGXC_SHARD_MAP_H

#include "catalog/genbki.h"

#define PgxcShardMapRelationId  9023

CATALOG(pgxc_shard_map,9023) BKI_WITHOUT_OIDS BKI_SHARED_RELATION
{
	Oid     disgroup;
	int32	shardgroupid;
	int16	ncopy;	
	Oid		primarycopy;	/* data node OID */
	Oid		copy1;
	Oid		copy2;
#ifdef __OPENTENBASE_C__
	int16   shardclusterid;
#endif
	char	primarystatus;	
	char	status1;
	char	status2;	
	int16   extended;      
	int32   ntuples;
} FormData_pgxc_shard_map;

typedef FormData_pgxc_shard_map *Form_pgxc_shard_map;

#define Natts_pgxc_shard_map					12

#define Anum_pgxc_shard_map_nodegroup   		1
#define Anum_pgxc_shard_map_shardgroupid		2
#define Anum_pgxc_shard_map_ncopy				3
#define Anum_pgxc_shard_map_primarycopy			4
#define Anum_pgxc_shard_map_copy1				5
#define Anum_pgxc_shard_map_copy2				6
#ifdef __OPENTENBASE_C__
#define Anum_pgxc_shard_map_shardclusterid		7
#endif
#define Anum_pgxc_shard_map_primarystatus		8
#define Anum_pgxc_shard_map_status1				9
#define Anum_pgxc_shard_map_status2				10
#define Anum_pgxc_shard_map_extend				11
#define Anum_pgxc_shard_map_ntuples				12


#define SHARD_MAP_STATUS_ERROR		'E'
#define SHARD_MAP_STATUS_UNDEFINED	'N'
#define SHARD_MAP_STATUS_USING		'U'
#define SHARD_MAP_STATUS_MIGRATING	'M'
#define SHARD_MAP_STATUS_INVALID	'I'

extern bool is_group_sharding_inited(Oid group);
extern void InitShardMap(int32 nShardGroup, int32 nNodes, Oid *nodes, int16 extend, Oid nodeGroup);
extern bool is_already_inited(void);
extern bool NodeHasShard(Oid node);
#ifdef __OPENTENBASE_C__
extern void UpdateRelationShardMap(Oid group, Oid from_node, Oid to_node,
								int shardgroup_num,	int* shardgroups,
								int16 scid);
#else
extern void UpdateRelationShardMap(Oid group, Oid from_node, Oid to_node,
								int shardgroup_num,	int* shardgroups);
#endif


extern void DropShardMap_Node(Oid group);
#endif
