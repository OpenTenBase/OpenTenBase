#ifndef _SHARDMAP_H_
#define _SHARDMAP_H_

#include "postgres.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "utils/relcache.h"
#include "pgxc/locator.h"
#include "storage/relfilenode.h"
#include "nodes/plannodes.h"
#include "datatype/timestamp.h"

/* the following macro have same define */
extern int shard_cluster_num;

//#define SHARD_CLUSTER_NUM 8			/* Column store shared cluster number */
#define MAX_SHARD_CLUSTER_NUM	(128)


#define SHARD_MAP_GROUP_NUM   4096		/*2^12  MUST be 2^n, cannot be greater than HASH_SIZE*/
#define SHARD_MAP_SHARD_NUM	  4096		/*2^12  size:1*/

#define EXTENSION_SHARD_MAP_GROUP_NUM   4096	/*2^12  MUST be 2^n, cannot be greater than HASH_SIZE*/
#define EXTENSION_SHARD_MAP_SHARD_NUM	4096	/*2^12  size:1*/

#define MAX_GROUP_NODE_NUMBER 	1024      	  /* max node number per group */
#define MAX_SHARDING_NODE_GROUP 256            /* max sharding node group number */

#define MAJOR_SHARD_NODE_GROUP  0
#define FIRST_EXTENSION_GROUP   1
#define LAST_EXTENSION_GROUP    MAX_SHARDING_NODE_GROUP
#define INVALID_SHARDMAP_ID     -1
#define PGXC_INVALID_NODE_IDX   -1

#define SHMEM_SHRADMAP_STATUS_UNINITED 	0     /* never to be writed */
#define SHMEM_SHRADMAP_STATUS_UNDEFINED	1     /* pgxc_shard_map is empty */
#define SHMEM_SHRADMAP_STATUS_LOADING	2     /* pgxc_shard_map is empty */
#define SHMEM_SHRADMAP_STATUS_USING		3     /* could be used */
#define SHMEM_SHRADMAP_STATUS_INVALID	4     /* pgxc_shard_map has be updated, but shmem has not be sync */

#define STRINGLENGTH 1024   /* string buffer length */

/* The shard cluster id of non-shard table is 0 */
typedef int8 ShardClusterId;
#define InvalidShardClusterId ((int8)0xff)

extern int32       GetNodeIndexByHashValue(Oid group, uint32 hashvalue);
#ifdef __OPENTENBASE_C__
int16  GetNodeShardClusterByHashValue(uint32 hashvalue);
int16 GetShardClusterID(uint32 shardid);
#endif

extern Bitmapset  *g_DatanodeShardgroupBitmap;

extern bool g_StatShardInfo;
extern int  g_MaxSessionsPerPool;
extern int  g_ShardInfoFlushInterval;

#define SHARD_TABLE_BITMAP_SIZE \
	(BITMAPSET_SIZE(WORDNUM(SHARD_MAP_GROUP_NUM) + 1))

#define SHARD_CLUSTER_BITMAP_SIZE \
	(BITMAPSET_SIZE(WORDNUM(MAX_SHARD_CLUSTER_NUM) + 1))

typedef struct ShardMapItemDef
{
	int32	shardgroupid;
	Oid		primarycopy;
	int32   nodeindex;	/* node index, set when load sharding map */
	int16   shardclusterid;
}ShardMapItemDef;

typedef struct
{
	int32  shard;
	int64  count;
	int64  size;
}ShardStat;

typedef enum
{
	ShardOpType_create = 0,
	ShardOpType_drop   = 1,
	ShardOpType_butty
}ShardOpType;

extern bool  show_all_shard_stat;

extern void ShardMapShmemInit_CN(void);
extern void ShardMapShmemInit_DN(void);
extern bool MajorShardAlreadyExist(void);
extern void SyncShardMapList(bool force);
extern void InvalidateShmemShardMap(bool iscommit);
extern void RegisterInvalidShmemShardMap(Oid group, int32 op);
extern Size ShardMapShmemSize(void);
extern void GetShardNodes(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension);
extern void PrepareMoveData(MoveDataStmt* stmt);
extern void PgxcMoveData_Node(MoveDataStmt* stmt);
extern void PgxcMoveData_DN(MoveDataStmt* stmt);
extern void UpdateReplicaRelNodes(Oid newnodeid);
extern void ForceRefreshShardMap(Oid groupoid);
extern int  GetGroupSize(void);
extern Bitmapset* CopyShardGroups_DN(Bitmapset * shard, Bitmapset * shardcluster);

extern int EvaluateSCIdWithValuesNulls(Relation relation,
													TupleDesc tupledesc,
													Datum *values,
													bool *nulls);
extern ShardID EvaluateShardIdWithValuesNulls(Relation relation,
															TupleDesc tupledesc,
															Datum *values,
															bool *nulls);
extern int32 EvaluateShardId(Oid *type, bool *isNull, Datum *dvalue, int nAttr);

extern int TruncateShard(Oid reloid, ShardID sid, int pausetime);

/* shard barrier */
extern void ShardBarrierShmemInit(void);
extern Size ShardBarrierShmemSize(void);
extern void AddShardBarrier(RelFileNode rel, ShardID sid, BackendId pid);
extern void RemoveOneShardBarrier(RelFileNode rel, ShardID sid);
extern void RemoveShardBarrier(void);
extern bool IsShardBarriered(RelFileNode rel, ShardID sid);
extern bool LocalHasShardBarriered(RelFileNode rel, ShardID sid);
extern void ATEOXact_CleanUpShardBarrier(void);

extern void   StatShardRelation(Oid relid, ShardStat *shardstat, int32 shardnumber);
extern void   SampleRowRelationScStatistic(Relation rel, ShardStat *shardstat, int32 shardnumber, float *ratio);
extern void   SampleShardStatistic(ShardStat *shardstat, int32 shardnumber, float ratio);

extern void   StatShardAllRelations(ShardStat *shardstat, int32 shardnumber);
extern void   GetGroupNodeIndexMap(Oid group, int32 *map);

extern Size ShardStatisticShmemSize(void);

extern void ShardStatisticShmemInit(void);

extern void UpdateShardStatistic(CmdType cmd, ShardID sid, int64 new_size, int64 old_size);

extern void FlushShardStatistic(void);

extern void RecoverShardStatistic(void);

extern void ResetShardStatistic(void);

extern Datum opentenbase_shard_statistic(PG_FUNCTION_ARGS);

extern void GetShardIdsByShardClusterId(Bitmapset *sc_bms, ShardID *shard_ids, int *nShards);

extern int32 GetNodeIndexByShardid(Oid group, int shardid);
extern Oid GetNodeOidByShardid(int shardid);

#endif /*_SHARDMAP_H_*/

extern int GetNodeid(int shardid);
