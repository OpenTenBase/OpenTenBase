#include "postgres.h"
#include "storage/extentmapping.h"
#include "storage/s_lock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/lwlock.h"
#include "storage/lockdefs.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "pgxc/shardmap.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "nodes/bitmapset.h"
#include "access/htup.h"
#include "access/hash.h"
#include "access/xact.h"
#include "access/skey.h"
#include "access/heapam.h"
#include "access/sdir.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "access/hash.h"
#include "catalog/pgxc_shard_map.h"
#include "catalog/pgxc_group.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_class.h"
#include "catalog/heap.h"
#include "catalog/storage_xlog.h"
#include "nodes/makefuncs.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "postmaster/bgwriter.h"
#include "miscadmin.h"
#include "pgxc/groupmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "catalog/namespace.h"
#include <sys/stat.h>
#ifdef __OPENTENBASE__
#include "replication/worker_internal.h"
#include "utils/memutils.h"
#include "postmaster/postmaster.h"
#include "utils/ruleutils.h"
#endif
#include "utils/rel.h"
#include "utils/guc.h"

extern bool trace_extent;

typedef struct
{
	Oid 	group;					/* group sharding into */
	int32   shardMaxGlblNdIdx;		/* max global node index */
	int32   shardMapStatus;         /* shard map status */
	int32   shmemNumShardGroups;    /* number of shard groups */
	int32   shmemNumShards;			/* number of shards */
	int32   shmemNumShardNodes;		/* number of nodes in the sharding group*/

	int32   shmemshardnodes[MAX_GROUP_NODE_NUMBER];/* node index of this group, ordered by node name */
	int32   shmemNodeMap[OPENTENBASE_MAX_DATANODE_NUMBER];	/* map for global node index to group native node index */
	ShardMapItemDef shmemshardmap[1];	/* shard map info */
}GroupShardInfo;

typedef struct
{
	uint64     	   group;
}GroupLookupTag;

typedef struct
{	
	GroupLookupTag tag;
	int64	       shardIndex;				/* Associated into  ShardNodeGroupInfo index */
} GroupLookupEnt;

typedef struct 
{
	bool           inited;
	bool           needLock;					  /* whether we need lock */
	slock_t		   lock[MAX_SHARDING_NODE_GROUP]; /* locks to protect used fields */
	bool 		   used[MAX_SHARDING_NODE_GROUP];

	GroupShardInfo *members[MAX_SHARDING_NODE_GROUP];
}ShardNodeGroupInfo;

/* As DN, only one group has shard info. */
typedef struct 
{
	bool           inited;
	bool           needLock;					  /* whether we need lock */
	slock_t		   lock; /* locks to protect used fields */
	bool 		   used;

	GroupShardInfo *members;
}ShardNodeGroupInfo_DN;

/*For CN*/
static ShardNodeGroupInfo *g_GroupShardingMgr = NULL;
static HTAB               *g_GroupHashTab     = NULL;

/*For DN*/
static ShardNodeGroupInfo_DN *g_GroupShardingMgr_DN = NULL;

/* used for datanodes */
Bitmapset  		  		  *g_DatanodeShardgroupBitmap  = NULL;
Bitmapset  		  		  *g_DatanodeShardClusterBitmap  = NULL;

typedef struct
{
	int32 nGroups;
	Oid   group[MAX_SHARDING_NODE_GROUP];
	int32 optype[MAX_SHARDING_NODE_GROUP];
}ShardingGroupInfo;


/* Working status for pg_stat_table_shard */
typedef struct
{
	int		  currIdx;
	ShardStat shardstat[SHARD_MAP_GROUP_NUM];	
} ShardStat_State;
/* used to record sharding group info */
static ShardingGroupInfo g_UpdateShardingGroupInfo = {0, {InvalidOid}};

/*        shard statistic management 
 *
 *  stat all shards' info including:
 *    ntuples_select: number of tuples scanned in each shard
 *    ntuples_insert: number of tuples inserted in each shard
 *    ntuples_update: number of tuples updated in each shard
 *    ntuples_delete: number of tuples deleted in each shard
 *    ntuples: number of tuples in each shard
 *    size: size of all tuples in each shard
 */

typedef struct 
{
	pg_atomic_uint64 ntuples_select;
	pg_atomic_uint64 ntuples_insert;
	pg_atomic_uint64 ntuples_update;
	pg_atomic_uint64 ntuples_delete;
	pg_atomic_uint64 ntuples;
	pg_atomic_uint64 size;
} ShardStatistic;

typedef struct
{
	ShardStatistic stat;
	pg_crc32c      crc;
} ShardRecord;

ShardStatistic *shardStatInfo = NULL;

#define SHARD_STATISTIC_FILE_PATH "pg_stat/shard.stat"

/* GUC used for shard statistic */
bool   g_StatShardInfo = true;
int    g_MaxSessionsPerPool = 250;
int    g_ShardInfoFlushInterval = 60;

typedef struct
{
	int	currIdx;
    ShardStatistic *rec;
} ShmMgr_State;

bool  show_all_shard_stat = false;

/* function declaration */
static void   SyncShardMapListForAllGroup(bool force);
static void   SyncShardMapListForCurrGroup(bool force);
static void   SyncShardMapListForAllGroupInternal(void);
static bool   SyncShardMapListForCurrGroupInternal(void);
static void   InsertShardMap_CN(int32 map, Form_pgxc_shard_map record);
static void   InsertShardMap_DN(Form_pgxc_shard_map record);
static void   ShardMapInitDone_CN(int32 map, Oid group, bool lock);
static void   ShardMapInitDone_DN(Oid group, bool need_lock);
static int    cmp_int32(const void *p1, const void *p2);
static bool   GroupShardingTabExist(Oid group, bool lock, int *group_index);
static int32  AllocShardMap(bool extend, Oid groupoid);
static Size   ShardMapShmemSize_Node_CN(void);
static Size   ShardMapShmemSize_Node_DN(void);
static int*   decide_shardgroups_to_move(Oid group, Oid fromnode, int *shardgroup_num);
static void   RemoveShardMapEntry(Oid group);
static void   FreshGroupShardMap(Oid group);
static void   BuildDatanodeVisibilityMap(Form_pgxc_shard_map tuple, Oid self_oid);
static void GetShardNodes_CN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension);
static void GetShardNodes_DN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension);

extern Datum  pg_stat_table_shard(PG_FUNCTION_ARGS);
extern Datum  pg_stat_all_shard(PG_FUNCTION_ARGS);
static void RefreshShardStatistic(ShardStat *shardstat);

int GetNodeid(int shardid);


/*----------------------------------------------------------
 *
 *		Shard Map In Share Memory
 *
 -----------------------------------------------------------*/

void InvalidateShmemShardMap(bool iscommit)
{
	int32 i = 0;
	
	if (g_UpdateShardingGroupInfo.nGroups)
	{
		if(iscommit)
		{
			if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
			{
				/* tell others use lock to access shard map */
				g_GroupShardingMgr->needLock = true;
				
				LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
				for (i = 0; i < g_UpdateShardingGroupInfo.nGroups; i++)
				{
					if (ShardOpType_create == g_UpdateShardingGroupInfo.optype[i])
					{
						/* refresh shard map of given group. */
						FreshGroupShardMap(g_UpdateShardingGroupInfo.group[i]);
					}
					else if (ShardOpType_drop == g_UpdateShardingGroupInfo.optype[i])
					{
						RemoveShardMapEntry(g_UpdateShardingGroupInfo.group[i]);
					}					
				}
				LWLockRelease(ShardMapLock);
				/* reset flag */
				g_GroupShardingMgr->needLock = false;
			}
			if (IS_PGXC_DATANODE)
			{
				/* tell others use lock to access shard map */
				g_GroupShardingMgr_DN->needLock = true;				
				for (i = 0; i < g_UpdateShardingGroupInfo.nGroups; i++)
				{
					if (ShardOpType_create == g_UpdateShardingGroupInfo.optype[i])
					{
						/* force fresh shard map of DN. SyncShardMapList will take care of lock itself, no need to take lock here. */
						SyncShardMapList(true);
					}
					else if (ShardOpType_drop == g_UpdateShardingGroupInfo.optype[i])
					{
						LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
						RemoveShardMapEntry(g_UpdateShardingGroupInfo.group[i]);
						LWLockRelease(ShardMapLock);
					}
				}				
				/* reset flag */
				g_GroupShardingMgr_DN->needLock = false;
			}
		
			g_UpdateShardingGroupInfo.nGroups = 0;
		}
		else
		{
			g_UpdateShardingGroupInfo.nGroups = 0;
		}
	}
}

void RegisterInvalidShmemShardMap(Oid group, int32 op)
{
	if (g_UpdateShardingGroupInfo.nGroups < MAX_SHARDING_NODE_GROUP)
	{
		g_UpdateShardingGroupInfo.group[g_UpdateShardingGroupInfo.nGroups]  = group;
		g_UpdateShardingGroupInfo.optype[g_UpdateShardingGroupInfo.nGroups] = op;
		g_UpdateShardingGroupInfo.nGroups++;		
	}
	else
	{
		elog(ERROR, "too many groups created");
	}

	// TODO:
	//hold_select_shard();
}

/*
 * ShardMapShmem In DN will only has one GroupShardInfo info.
 * Since DN only store its own group shardmap info. 
 */
void ShardMapShmemInit_CN(void)
{
	bool found;
	int  i = 0;
	GroupShardInfo *groupshard = NULL;
	char name[MAXPGPATH];
	HASHCTL		info;

	/* init hash table */
	info.keysize   = sizeof(GroupLookupTag);
	info.entrysize = sizeof(GroupLookupEnt);
	info.hash = tag_hash;

	g_GroupHashTab = ShmemInitHash("Group Sharding info look up",
								  MAX_SHARDING_NODE_GROUP, 
								  MAX_SHARDING_NODE_GROUP,
								  &info,
								  HASH_ELEM | HASH_FUNCTION);	

	if (!g_GroupHashTab)
	{
		elog(FATAL, "invalid shmem status when creating sharding hash ");
	}
	
	g_GroupShardingMgr = (ShardNodeGroupInfo *)ShmemInitStruct("Group shard mgr",
																sizeof(ShardNodeGroupInfo),
																&found);
	if (found)
	{
		elog(FATAL, "invalid shmem status when creating Group shard mgr ");
	}
	g_GroupShardingMgr->inited   = false;
	g_GroupShardingMgr->needLock = false;
	
	groupshard = (GroupShardInfo *)ShmemInitStruct("Group shard major",
														MAXALIGN64(sizeof(GroupShardInfo)) + MAXALIGN64(sizeof(ShardMapItemDef)) * (SHARD_MAP_GROUP_NUM - 1),
														&found);

	if (found)
	{
		elog(FATAL, "invalid shmem status when creating Group shard major ");
	}
	
	/* init first major group */
	groupshard->group 		        = InvalidOid;
	groupshard->shardMapStatus      = SHMEM_SHRADMAP_STATUS_UNINITED;
	groupshard->shmemNumShardGroups = SHARD_MAP_GROUP_NUM;
	groupshard->shmemNumShards      = SHARD_MAP_SHARD_NUM;
	groupshard->shmemNumShardNodes  = 0;
	memset(groupshard->shmemshardnodes, 0X00, sizeof(groupshard->shmemshardnodes));
	memset(groupshard->shmemshardmap, 0X00, sizeof(ShardMapItemDef) * SHARD_MAP_GROUP_NUM);

	SpinLockInit(&g_GroupShardingMgr->lock[0]);
	g_GroupShardingMgr->used[0]     = false;
	g_GroupShardingMgr->members[0]  = groupshard;

	/* init extension group */
	for (i = FIRST_EXTENSION_GROUP; i < LAST_EXTENSION_GROUP; i++)
	{
		snprintf(name, MAXPGPATH, "Extension group %d", i);

		groupshard = (GroupShardInfo *)ShmemInitStruct(name,
														MAXALIGN64(sizeof(GroupShardInfo)) + MAXALIGN64(sizeof(ShardMapItemDef)) * (EXTENSION_SHARD_MAP_GROUP_NUM - 1),
														&found);
		if (found)
		{
			elog(FATAL, "invalid shmem status when creating %s ", name);
		}
		
		/* init first major group */
		groupshard->group 		   = InvalidOid;
		groupshard->shardMapStatus = SHMEM_SHRADMAP_STATUS_UNINITED;
		groupshard->shmemNumShardGroups = EXTENSION_SHARD_MAP_GROUP_NUM;
		groupshard->shmemNumShards      = EXTENSION_SHARD_MAP_SHARD_NUM;
		groupshard->shmemNumShardNodes  = 0;
		memset(groupshard->shmemshardnodes, 0X00, sizeof(groupshard->shmemshardnodes));
		memset(groupshard->shmemshardmap, 0X00, sizeof(ShardMapItemDef) * EXTENSION_SHARD_MAP_GROUP_NUM);

		SpinLockInit(&g_GroupShardingMgr->lock[i]);
		g_GroupShardingMgr->used[i]     = false;
		g_GroupShardingMgr->members[i]  = groupshard;
	}
}


void ShardMapShmemInit_DN(void)
{
	bool found;
	GroupShardInfo *groupshard = NULL;

	g_GroupShardingMgr_DN = (ShardNodeGroupInfo_DN *)ShmemInitStruct("DN Group shard mgr",
																sizeof(ShardNodeGroupInfo_DN),
																&found);
	if (found)
	{
		elog(FATAL, "invalid shmem status when creating Group shard mgr ");
	}
	g_GroupShardingMgr_DN->inited   = false;
	g_GroupShardingMgr_DN->needLock = false;
	
	groupshard = (GroupShardInfo *)ShmemInitStruct("DN Group shard major",
														MAXALIGN64(sizeof(GroupShardInfo)) + MAXALIGN64(sizeof(ShardMapItemDef)) * (SHARD_MAP_GROUP_NUM - 1),
														&found);

	if (found)
	{
		elog(FATAL, "invalid shmem status when creating Group shard major ");
	}
	
	/* init first major group */
	groupshard->group 		        = InvalidOid;
	groupshard->shardMapStatus      = SHMEM_SHRADMAP_STATUS_UNINITED;
	groupshard->shmemNumShardGroups = SHARD_MAP_GROUP_NUM;
	groupshard->shmemNumShards      = SHARD_MAP_SHARD_NUM;
	groupshard->shmemNumShardNodes  = 0;
	memset(groupshard->shmemshardnodes, 0X00, sizeof(groupshard->shmemshardnodes));
	memset(groupshard->shmemshardmap, 0X00, sizeof(ShardMapItemDef) * SHARD_MAP_GROUP_NUM);

	SpinLockInit(&g_GroupShardingMgr_DN->lock);
	g_GroupShardingMgr_DN->used     = false;
	g_GroupShardingMgr_DN->members  = groupshard;

	/* DN need to construct g_DatanodeShardgroupBitmap */
	g_DatanodeShardgroupBitmap = (Bitmapset *) ShmemInitStruct("Data node bitmap",
	   												  MAXALIGN64(SHARD_TABLE_BITMAP_SIZE),
													  &found);

	if(!found)
	{
		g_DatanodeShardgroupBitmap = bms_make((char *)g_DatanodeShardgroupBitmap, SHARD_MAP_GROUP_NUM);
	}	
	
	/* DN need to construct g_DatanodeShardClusterBitmap */
	g_DatanodeShardClusterBitmap = (Bitmapset *) ShmemInitStruct("Data node shardcluster bitmap",
	   												  MAXALIGN64(SHARD_CLUSTER_BITMAP_SIZE),
													  &found);

	if(!found)
	{
		g_DatanodeShardClusterBitmap = bms_make((char *)g_DatanodeShardClusterBitmap, MAX_SHARD_CLUSTER_NUM);
	}
	
}


bool   MajorShardAlreadyExist(void)
{
	SyncShardMapList(false);	
	if (IS_PGXC_COORDINATOR)
	{
		return g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP];
	}
	else if (IS_PGXC_DATANODE)
	{
		return g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP] || g_GroupShardingMgr_DN->used;
	}

	return false;/* should not reach here */
}

void SyncShardMapList(bool force)
{
	if(isRestoreMode)
	{
		return;
	}

	/* All shardmap info should be maintained for DNs besides CNs in order to
	 * support cross group query. The current shardmap info for local DNs
	 * is still maintained alone for other purposes.
	 */
	if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
	{
		SyncShardMapListForAllGroup(force);
	}
	if (IS_PGXC_DATANODE)
	{
		SyncShardMapListForCurrGroup(force);
	}
}

static void SyncShardMapListForAllGroup(bool force)
{
#ifdef __OPENTENBASE_C__
	bool lwlock_held_by_me = false;
#endif

	if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr->inited)
	{
		return;
	}
	/* tell others use lock to access shard map */
	g_GroupShardingMgr->needLock = true;

#ifdef __OPENTENBASE_C__
	lwlock_held_by_me = LWLockHeldByMeInMode(ShardMapLock, LW_EXCLUSIVE);

	if (false == lwlock_held_by_me)
	{
		LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
	}
#else
	LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
#endif

	/* in case of race conditions */
	if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr->inited)
	{
		LWLockRelease(ShardMapLock);
		g_GroupShardingMgr->needLock = false;
		return;
	}

	g_GroupShardingMgr->inited = false;
	SyncShardMapListForAllGroupInternal();
	g_GroupShardingMgr->inited = true;

#ifdef __OPENTENBASE_C__
	if (false == lwlock_held_by_me)
	{
		LWLockRelease(ShardMapLock);
	}
#else
	LWLockRelease(ShardMapLock);
#endif
	
	/*reset flag*/
	g_GroupShardingMgr->needLock = false;
}

static void SyncShardMapListForCurrGroup(bool force)
{
#ifdef __OPENTENBASE_C__
	bool lwlock_held_by_me = false;
#endif

	if (!IsPostmasterEnvironment)
	{
		return;
	}
	
	if (!IS_PGXC_DATANODE)
	{
		elog(ERROR, "SyncShardMapListForCurrGroup should only be called in datanode");
	}

	if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr_DN->inited)
	{
		if (!g_GroupShardingMgr_DN->used)
		{
			elog(LOG, "SyncShardMapListForCurrGroup GroupShardingMgr is not inited yet.");
		}
		return;
	}
	/* tell others use lock to access shard map */
	g_GroupShardingMgr_DN->needLock = true;

#ifdef __OPENTENBASE_C__
	lwlock_held_by_me = LWLockHeldByMeInMode(ShardMapLock, LW_EXCLUSIVE);

	if (false == lwlock_held_by_me)
	{
		LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
	}
#else
	LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
#endif

	/* in case of race conditions */
	if (!force && g_UpdateShardingGroupInfo.nGroups == 0 && g_GroupShardingMgr_DN->inited)
	{
		LWLockRelease(ShardMapLock);
		g_GroupShardingMgr_DN->needLock = false;
		return;
	}
	g_GroupShardingMgr_DN->inited = false;
	g_GroupShardingMgr_DN->inited = SyncShardMapListForCurrGroupInternal();
		
#ifdef __OPENTENBASE_C__
	if (false == lwlock_held_by_me)
	{
		LWLockRelease(ShardMapLock);
	}
#else
	LWLockRelease(ShardMapLock);
#endif

	
	/*reset flag*/
	g_GroupShardingMgr_DN->needLock = false;
}


/* read shard map from system table and fill in the share memory */
/*
 * On datanode, if pgxc_shard_map tuple's primarycopy is exactly the datanode itself,
 * then this shard is visible, and should be a member of g_DatanodeShardgroupBitmap.
 */

static void SyncShardMapListForAllGroupInternal(void)
{
	int32 shard_mgr = INVALID_SHARDMAP_ID;		
	HeapTuple  oldtup;
	Relation shardrel;
	Form_pgxc_shard_map  pgxc_shard;
	Form_pgxc_shard_map  firsttup = NULL;	/* flag for invalid null shardmap. */
	ScanKeyData          skey;
	SysScanDesc  		 sysscan;
	Relation 		     rel;

	HeapScanDesc 		 scan;	
	HeapTuple			 tuple;
	Oid                  groupoid;

	InitMultinodeExecutor(false);

	/* cross check to ensure one node can only be in one node group */	
	rel = heap_open(PgxcGroupRelationId, AccessShareLock);

	scan = heap_beginscan_catalog(rel, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{		
		groupoid = HeapTupleGetOid(tuple);

		if (is_group_sharding_inited(groupoid))
		{
			/* If the group sharding has not been created. */
			if (!GroupShardingTabExist(groupoid, false, &shard_mgr))
			{
				/* Now to create a new shard mapping mgr */
				shard_mgr = AllocShardMap(true, groupoid);
				if (INVALID_SHARDMAP_ID == shard_mgr)
				{
					elog(ERROR, "too many sharding group defined");
				}
			}			
			
			/* build sharding table */
			firsttup  = NULL;
			shardrel  = heap_open(PgxcShardMapRelationId, AccessShareLock);
			ScanKeyInit(&skey,
				Anum_pgxc_shard_map_nodegroup,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupoid));
			
			sysscan = systable_beginscan(shardrel,
									  PgxcShardMapGroupIndexId, 
									  true,
									  NULL, 1, &skey);			
			
			while(HeapTupleIsValid(oldtup = systable_getnext(sysscan)))
			{
				if(NULL == firsttup)
				{
					if (NULL == oldtup)
					{
						elog(ERROR, "invalid status, group:%u has no sharding info", groupoid);
					}
					
					firsttup = (Form_pgxc_shard_map)GETSTRUCT(oldtup);	
				}
				
				pgxc_shard = (Form_pgxc_shard_map)GETSTRUCT(oldtup);
				InsertShardMap_CN(shard_mgr, pgxc_shard);				
			}
			systable_endscan(sysscan);
			heap_close(shardrel, AccessShareLock);	
			ShardMapInitDone_CN(shard_mgr, groupoid, false);			
		}
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
}

static bool SyncShardMapListForCurrGroupInternal(void)
{
	HeapTuple  oldtup;
	Relation shardrel;
	Form_pgxc_shard_map  pgxc_shard;
	ScanKeyData          skey;
	SysScanDesc  		 sysscan;
	Oid					 curr_groupoid = InvalidOid;
	Oid 			     self_node_oid = InvalidOid;

	if (!IS_PGXC_DATANODE)
	{
		elog(ERROR, "SyncShardMapListForCurrGroupInternal should only be called in datanode");
		return false;
	}

	InitMultinodeExecutor(false);

	self_node_oid = get_pgxc_nodeoid_extend(PGXCNodeName, pgxc_plane_name(PGXCMainPlaneNameID));
	if (InvalidOid == self_node_oid)
	{
		elog(LOG, "SyncShardMapListForCurrGroupInternal failed to get nodeoid, node:%s", PGXCNodeName);
		return false;
	}
	curr_groupoid = GetGroupOidByNode(self_node_oid);
	if (InvalidOid == curr_groupoid)
	{
		elog(LOG, "SyncShardMapListForCurrGroupInternal failed to get groupoid, node:%s, nodeoid:%d", PGXCNodeName, self_node_oid);
		return false;
	}

	if (is_group_sharding_inited(curr_groupoid))
	{
		bms_clear(g_DatanodeShardgroupBitmap);
		bms_clear(g_DatanodeShardClusterBitmap);

		/*
		 * If sharding of the group has not been inited, or this sharding map is in use but
		 * store overdue information, possibly caused by group syncing backend crashing right
		 * before the shmem sync.
		 */
		if (!g_GroupShardingMgr_DN->used || curr_groupoid != g_GroupShardingMgr_DN->members->group)
		{
			/*
			 * Datanodes can only be in one node group, so we save the effort of
			 * removing entry and skip right into resetting the mgr.
			 */
			g_GroupShardingMgr_DN->members->shardMapStatus = SHMEM_SHRADMAP_STATUS_LOADING;
			SpinLockAcquire(&g_GroupShardingMgr_DN->lock);
			g_GroupShardingMgr_DN->members->group = curr_groupoid;
			g_GroupShardingMgr_DN->used = true;
			SpinLockRelease(&g_GroupShardingMgr_DN->lock);
		}

		shardrel = heap_open(PgxcShardMapRelationId, AccessShareLock);
		ScanKeyInit(&skey,
			Anum_pgxc_shard_map_nodegroup,
			BTEqualStrategyNumber, F_OIDEQ,
			ObjectIdGetDatum(curr_groupoid));

		sysscan = systable_beginscan(shardrel,
								  PgxcShardMapGroupIndexId,
								  true,
								  NULL, 1, &skey);

		while(HeapTupleIsValid(oldtup = systable_getnext(sysscan)))
		{
			pgxc_shard = (Form_pgxc_shard_map)GETSTRUCT(oldtup);
			InsertShardMap_DN(pgxc_shard);

			/* 
			 * If node is DN AND pgxc_shard_map tuple's primary copy is itself,
			 * Add this shardid to bitmap.
			 * Add this shardclusterid to bitmap.
			 */
			BuildDatanodeVisibilityMap(pgxc_shard, self_node_oid);
		}
		systable_endscan(sysscan);
		heap_close(shardrel, AccessShareLock);	
		ShardMapInitDone_DN(curr_groupoid, false);
	}
	else
	{
		elog(LOG, "SyncShardMapListForCurrGroupInternal group %d is not inited.", curr_groupoid);
		return false;
	}

	return true;
}


bool GroupShardingTabExist(Oid group, bool lock, int *group_index)
{
	bool           found;
	GroupLookupTag tag;
	GroupLookupEnt *ent;
	tag.group =  group;

	if (lock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}
	ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);	
	if (lock)
	{
		LWLockRelease(ShardMapLock);
	}

	if(found && group_index)
	{
		*group_index = (int32)ent->shardIndex;
	}
	
	return found;
}

static void InsertShardMap_CN(int32 map, Form_pgxc_shard_map record)
{
	if (map != INVALID_SHARDMAP_ID)
	{
		int32 nodeindex;		
		if (record->shardgroupid <= g_GroupShardingMgr->members[map]->shmemNumShardGroups && record->shardgroupid >= 0)
		{
			char node_type = get_pgxc_nodetype(record->primarycopy);
			nodeindex = PGXCNodeGetNodeId(record->primarycopy, &node_type);
			if (nodeindex < 0)
			{
				elog(PANIC, " get node:%u for index failed", record->primarycopy);
			}
			
			g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].primarycopy  = record->primarycopy;
			g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].shardgroupid = record->shardgroupid;
			g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].nodeindex	   = nodeindex;
			
#ifdef __OPENTENBASE_C__
			g_GroupShardingMgr->members[map]->shmemshardmap[record->shardgroupid].shardclusterid = record->shardclusterid;
#endif
		}
		else
		{
			elog(ERROR, " invalid pgxc_shard_map record with shardgroupid:%d", record->shardgroupid);
		}		
	}
}


static void InsertShardMap_DN(Form_pgxc_shard_map record)
{
	int32 nodeindex;		
	if (!IS_PGXC_DATANODE)
	{
		elog(ERROR, "InsertShardMap_DN should only be called in datanode");
	}
	
	if (record->shardgroupid <= g_GroupShardingMgr_DN->members->shmemNumShardGroups && record->shardgroupid >= 0)
	{
		char node_type = get_pgxc_nodetype(record->primarycopy);
		nodeindex = PGXCNodeGetNodeId(record->primarycopy, &node_type);
		if (nodeindex < 0)
		{
			elog(PANIC, " get node:%u for index failed", record->primarycopy);
		}
		
		g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].primarycopy  = record->primarycopy;
		g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].shardgroupid = record->shardgroupid;
		g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].nodeindex	 = nodeindex;
#ifdef __OPENTENBASE_C__
		g_GroupShardingMgr_DN->members->shmemshardmap[record->shardgroupid].shardclusterid = record->shardclusterid;
#endif
	}
	else
	{
		elog(ERROR, "[InsertShardMap_DN]invalid pgxc_shard_map record with shardgroupid:%d", record->shardgroupid);
	}
}

static void ShardMapInitDone_CN(int32 map, Oid group, bool need_lock)
{
	bool		   found = false;
	bool           dup = false;
	int32          i;
	int32          j;
	int32          nodeindex = 0;
	int32          nodeCnt = 0;
	int32		   maxNodeIndex = 0;
	ShardMapItemDef item;
	GroupLookupEnt *ent	   = NULL;
	GroupLookupTag tag;
	tag.group =  group;

	if(map < 0 || map >= MAX_SHARDING_NODE_GROUP)
	{
		elog(ERROR, "group index %d is invalid.", map);
		return;
	}
	
	if(group != g_GroupShardingMgr->members[map]->group)
	{
		elog(ERROR, "groupoid in slot %d oid:%u is not group %d", map, g_GroupShardingMgr->members[map]->group, group);
		return;
	}

	if (need_lock)
	{
		LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
	}

	/* Add group node to hashmap. */
	g_GroupShardingMgr->members[map]->shardMapStatus = SHMEM_SHRADMAP_STATUS_USING;		
	ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_ENTER, &found);
	if (!ent)
	{
		/* Failed to add the hash table entry. */		
		/* Release the shard map entry. */
		SpinLockAcquire(&g_GroupShardingMgr->lock[map]);
		g_GroupShardingMgr->used[map] = false;
		SpinLockRelease(&g_GroupShardingMgr->lock[map]);

		if (need_lock)
		{
			LWLockRelease(ShardMapLock);
		}
		elog(ERROR, "ShardMapInitDone_CN corrupted shared hash table");
	}
    ent->shardIndex = map;	

	{
			
		/* init shard group nodes of the shard map */
		nodeCnt = 0;
		memset(g_GroupShardingMgr->members[map]->shmemshardnodes, 0Xff, sizeof(g_GroupShardingMgr->members[map]->shmemshardnodes));

		for (i = 0; i < g_GroupShardingMgr->members[map]->shmemNumShardGroups; i++)
		{
			dup = false;
			item = g_GroupShardingMgr->members[map]->shmemshardmap[i];
			for (j = 0; j < nodeCnt; j++)
			{
				if (g_GroupShardingMgr->members[map]->shmemshardnodes[j] == item.nodeindex)
				{
					dup = true;
					break;
				}
			}
			
			/* all node index are in shmemNumShardNodes */
			if (!dup)
			{
				/* store the max global node index. */
				maxNodeIndex = maxNodeIndex < item.nodeindex ? item.nodeindex :  maxNodeIndex;
				g_GroupShardingMgr->members[map]->shmemshardnodes[nodeCnt] = item.nodeindex;
				nodeCnt++;
			}
		}

		g_GroupShardingMgr->members[map]->shmemNumShardNodes = nodeCnt;
		g_GroupShardingMgr->members[map]->shardMaxGlblNdIdx  = maxNodeIndex + 2; /* 1 is enough, just in case*/

		qsort(g_GroupShardingMgr->members[map]->shmemshardnodes, 
				nodeCnt, 
				sizeof(int32), 
				cmp_int32);
		
		/*init all element to invalid big. */
		memset(g_GroupShardingMgr->members[map]->shmemNodeMap, 0XFF, sizeof(int32) * OPENTENBASE_MAX_DATANODE_NUMBER);
		for (i = 0; i < nodeCnt; i++)
		{
			nodeindex = g_GroupShardingMgr->members[map]->shmemshardnodes[i];
			/* Store the group native index of the node into the node global map. */
			g_GroupShardingMgr->members[map]->shmemNodeMap[nodeindex] = i;
		}
	}
	g_GroupShardingMgr->members[map]->shardMapStatus = SHMEM_SHRADMAP_STATUS_USING;
	
	if (need_lock)
	{
		LWLockRelease(ShardMapLock);
	}
}


static void ShardMapInitDone_DN(Oid group, bool need_lock)
{
	bool           dup = false;
	int32		   maxNodeIndex = 0;
	int32          i;
	int32          j;
	int32          nodeindex = 0;
	int32          nodeCnt = 0;
	ShardMapItemDef item;

	if(!IS_PGXC_DATANODE)
	{
		elog(ERROR, "ShardMapInitDone_DN should only be called in datanode");
		return;
	}

	if(group != g_GroupShardingMgr_DN->members->group)
	{
		/* PANIC here is to reset shmem, although a more elegant way should be provided by ShardMapShmem AM */
		elog(PANIC, "groupoid %d in mgr is not group %d", g_GroupShardingMgr_DN->members->group, group);
		return;
	}

	if (need_lock)
	{
		LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
	}
	
	/* init shard group nodes of the shard map */
	nodeCnt = 0;
	memset(g_GroupShardingMgr_DN->members->shmemshardnodes, 0Xff, sizeof(g_GroupShardingMgr_DN->members->shmemshardnodes));

	for (i = 0; i < g_GroupShardingMgr_DN->members->shmemNumShardGroups; i++)
	{
		dup = false;
		item = g_GroupShardingMgr_DN->members->shmemshardmap[i];
		for (j = 0; j < nodeCnt; j++)
		{
			if (g_GroupShardingMgr_DN->members->shmemshardnodes[j] == item.nodeindex)
			{
				dup = true;
				break;
			}
		}
		
		/* all node index are in shmemNumShardNodes */
		if (!dup)
		{
			g_GroupShardingMgr_DN->members->shmemshardnodes[nodeCnt] = item.nodeindex;
			/* store the max global node index. */
			maxNodeIndex = maxNodeIndex < item.nodeindex ? item.nodeindex :  maxNodeIndex;
			nodeCnt++;
		}
	}
	
	g_GroupShardingMgr_DN->members->shmemNumShardNodes = nodeCnt;
	g_GroupShardingMgr_DN->members->shardMaxGlblNdIdx  = maxNodeIndex + 2; /* 1 is enough, just in case*/
	qsort(g_GroupShardingMgr_DN->members->shmemshardnodes, 
			nodeCnt, 
			sizeof(int32), 
			cmp_int32);
	
	/*init all element to invalid big. */
	memset(g_GroupShardingMgr_DN->members->shmemNodeMap, 0XFF, sizeof(int32) * OPENTENBASE_MAX_DATANODE_NUMBER);
	for (i = 0; i < nodeCnt; i++)
	{
		nodeindex = g_GroupShardingMgr_DN->members->shmemshardnodes[i];
		/* Store the group native index of the node into the node global map. */
		g_GroupShardingMgr_DN->members->shmemNodeMap[nodeindex] = i;
	}
	g_GroupShardingMgr_DN->members->shardMapStatus = SHMEM_SHRADMAP_STATUS_USING;
	
	if (need_lock)
	{
		LWLockRelease(ShardMapLock);
	}
}


static int cmp_int32(const void *p1, const void *p2)
{
	int i1;
	int i2;
	
	if(!p1 || !p2)
		elog(ERROR,"cmp_int32:args cannot be null");

	i1 = *(int32 *)p1;
	i2 = *(int32 *)p2;

	if(i1 == i2) return 0;
	if(i1 < i2) return -1;

	return 1;
}
/* Alloc a shard map entry in shared memory. */
static int32 AllocShardMap(bool extend, Oid groupoid)
{
	int32 i;
	int32 index = INVALID_SHARDMAP_ID;

	if (!extend)
	{
		SpinLockAcquire(&g_GroupShardingMgr->lock[MAJOR_SHARD_NODE_GROUP]);
		if (!g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP])
		{
			index = MAJOR_SHARD_NODE_GROUP;
			g_GroupShardingMgr->used[MAJOR_SHARD_NODE_GROUP] = true;
			g_GroupShardingMgr->members[MAJOR_SHARD_NODE_GROUP]->group = groupoid;
		}	
		SpinLockRelease(&g_GroupShardingMgr->lock[MAJOR_SHARD_NODE_GROUP]);
	}
	else
	{
		for (i = FIRST_EXTENSION_GROUP; i < LAST_EXTENSION_GROUP; i++)
		{
			SpinLockAcquire(&g_GroupShardingMgr->lock[i]);
			if (!g_GroupShardingMgr->used[i])
			{
				g_GroupShardingMgr->used[i] = true;
				g_GroupShardingMgr->members[i]->group = groupoid;
				index = i;
				SpinLockRelease(&g_GroupShardingMgr->lock[i]);
				break;
			}
			SpinLockRelease(&g_GroupShardingMgr->lock[i]);
		}
	}
	return index;
}

Size ShardMapShmemSize(void)
{
	if(IS_PGXC_COORDINATOR)
	{
		return ShardMapShmemSize_Node_CN();
	}
	else if (IS_PGXC_DATANODE)
	{
		return ShardMapShmemSize_Node_CN() + ShardMapShmemSize_Node_DN();
	}
	else
	{
		return 0;
	}
}

static Size ShardMapShmemSize_Node_CN(void)
{
	Size size;
	
	size = 0;	
	/* hash table size */
	// hash_estimate_size(MAX_SHARDING_NODE_GROUP, sizeof(GroupLookupEnt));
	
	/* management info */
	size = add_size(size, MAXALIGN64(sizeof(ShardNodeGroupInfo)));

	/* struct self */
	size = add_size(size, MAXALIGN64(sizeof(GroupShardInfo)) * MAX_SHARDING_NODE_GROUP);

	/* non extension router info */
	size = add_size(size, mul_size(MAXALIGN64(sizeof(ShardMapItemDef)), SHARD_MAP_SHARD_NUM));

	/* extension router info */
	size = add_size(size, mul_size(mul_size(MAXALIGN64(sizeof(ShardMapItemDef)), EXTENSION_SHARD_MAP_GROUP_NUM), (LAST_EXTENSION_GROUP - FIRST_EXTENSION_GROUP)));

	/* shardmap bitmap info. Only used in datanode */
	size = add_size(size, MAXALIGN64(SHARD_TABLE_BITMAP_SIZE));	

	/* hash table, here just double the element size, in case of memory corruption */
	size = add_size(size, mul_size(MAX_SHARDING_NODE_GROUP * 2 , MAXALIGN64(sizeof(GroupLookupEnt))));
	return size;
}

static Size ShardMapShmemSize_Node_DN(void)
{
	Size size;
	
	size = 0;	
	/* hash table size */
	// hash_estimate_size(MAX_SHARDING_NODE_GROUP, sizeof(GroupLookupEnt));
	
	/* management info */
	size = add_size(size, MAXALIGN64(sizeof(ShardNodeGroupInfo_DN)));

	/* struct self */
	size = add_size(size, MAXALIGN64(sizeof(GroupShardInfo)));

	/* non extension router info */
	size = add_size(size, mul_size(MAXALIGN64(sizeof(ShardMapItemDef)), SHARD_MAP_SHARD_NUM));

	/* shardmap bitmap info. Only used in datanode */
	size = add_size(size, MAXALIGN64(SHARD_TABLE_BITMAP_SIZE));	

	return size;
}

int32 GetNodeIndexByHashValue(Oid group, uint32 hashvalue)
{	
	int            shardIdx;
	int 		   nodeIdx = -1;
	ShardMapItemDef *shardgroup;
	
	/* fast path */
	if (likely(IS_PGXC_DATANODE && !g_GroupShardingMgr_DN->needLock &&
			   g_GroupShardingMgr_DN->members->group == group))
	{
		shardIdx     = abs((int)hashvalue) % (g_GroupShardingMgr_DN->members->shmemNumShards);
		shardgroup   = &g_GroupShardingMgr_DN->members->shmemshardmap[shardIdx];
		nodeIdx      = shardgroup->nodeindex;
	}
	else
	{
		GroupLookupTag tag;
		GroupLookupEnt *ent;
		int slot;
		bool found;
		bool needLock = g_GroupShardingMgr->needLock;

		if (!OidIsValid(group))
			elog(PANIC, "[GetNodeIndexByHashValue]group oid can not be invalid.");

		if (needLock)
			LWLockAcquire(ShardMapLock, LW_SHARED);

		tag.group =  group;
		ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);			
		if (!found)
			elog(ERROR , "no shard group of %u found", group);

		slot = ent->shardIndex;

		shardIdx     = abs((int)hashvalue) % (g_GroupShardingMgr->members[slot]->shmemNumShards);
		shardgroup   = &g_GroupShardingMgr->members[slot]->shmemshardmap[shardIdx];
		nodeIdx      = shardgroup->nodeindex;

		if (needLock)
			LWLockRelease(ShardMapLock);
	}

	return nodeIdx;
}

#ifdef __OPENTENBASE_C__
int16 GetShardClusterID(uint32 shardid)
{
	int16 shardClusterId;
	bool needLock = false;
	ShardMapItemDef *shardgroup;

	Assert(IS_PGXC_DATANODE);
	
	/* assign shardclusterid 0 to all data in non-sharded tables.*/
	if (!ShardIDIsValid(shardid))
		return 0;

	needLock = g_GroupShardingMgr_DN->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}

	shardgroup = &g_GroupShardingMgr_DN->members->shmemshardmap[shardid];
	shardClusterId = shardgroup->shardclusterid;

	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
	return shardClusterId;
}

/*
 * Acquire node index according to the shardid
 */
int32 GetNodeIndexByShardid(Oid group, int shardid)
{
	int 		   nodeIdx = -1;
	bool           needLock = false;
	ShardMapItemDef *shardgroup;

	needLock = g_GroupShardingMgr_DN->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}

	shardgroup = &g_GroupShardingMgr_DN->members->shmemshardmap[shardid];
	nodeIdx = shardgroup->nodeindex;

	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
	return nodeIdx;
}

int16 GetNodeShardClusterByHashValue(uint32 hashvalue)
{	
	int            shardIdx;
	int 		   shardClusterIdx= -1;
	bool           needLock = false;
	ShardMapItemDef *shardgroup;

	Assert(IS_PGXC_DATANODE);
	
	needLock  = g_GroupShardingMgr_DN->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}

	shardIdx        = abs((int)hashvalue) % (g_GroupShardingMgr_DN->members->shmemNumShards);
	shardgroup      = &g_GroupShardingMgr_DN->members->shmemshardmap[shardIdx];
	shardClusterIdx = shardgroup->shardclusterid;

	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
	return shardClusterIdx;
}
#endif

/* Get node index map of group. */
void  GetGroupNodeIndexMap(Oid group, int32 *map)
{
	bool           needLock = false;
	bool           found;
	GroupLookupTag tag;
	GroupLookupEnt *ent;
	int slot = 0;
	
	if(!OidIsValid(group))
	{
		elog(ERROR, "GetGroupNodeIndexMap group oid invalid.");
	}

	if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
	{
		needLock  = g_GroupShardingMgr->needLock;
		if (needLock)
		{
			LWLockAcquire(ShardMapLock, LW_SHARED);
		}

		tag.group =  group;
		ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);			
		if (!found)
		{
			elog(ERROR , "no shard group of %u found", group);
		}

		slot = ent->shardIndex;
		memcpy(map, g_GroupShardingMgr->members[slot]->shmemNodeMap, sizeof(int32) * g_GroupShardingMgr->members[slot]->shardMaxGlblNdIdx);
	}
//	else if (IS_PGXC_DATANODE)
//	{
//		if (group != g_GroupShardingMgr_DN->members->group)
//		{
//			elog(ERROR, "GetGroupNodeIndexMap group oid:%u is not the stored group:%u.", group, g_GroupShardingMgr_DN->members->group);
//		}
//
//		needLock  = g_GroupShardingMgr_DN->needLock;
//		if (needLock)
//		{
//			LWLockAcquire(ShardMapLock, LW_SHARED);
//		}
//		memcpy(map, g_GroupShardingMgr_DN->members->shmemNodeMap, sizeof(int32) * g_GroupShardingMgr_DN->members->shardMaxGlblNdIdx);
//	}

	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
}



void GetShardNodes(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension)
{
	if (IS_PGXC_COORDINATOR)
	{
		GetShardNodes_CN(group, nodes, num_nodes, isextension);
	}
	else if (IS_PGXC_DATANODE)
	{
		GetShardNodes_DN(group, nodes, num_nodes, isextension);
	}
}



static void GetShardNodes_CN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension)
{
	bool           found;
	bool		   needLock;
	int 			nNodes = 0;
	GroupLookupTag tag;
	GroupLookupEnt *ent;
	tag.group =  group;		

	if (!IS_PGXC_COORDINATOR)
	{
		elog(ERROR, "GetShardNodes should only be called in coordinator");
	}
	

	SyncShardMapList(false);

	needLock  = g_GroupShardingMgr->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}
	
	ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);			
	if (!found)
	{
		elog(ERROR , "corrupted catalog, no shard group of %u found", group);
	}

	nNodes = g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShardNodes;
	
	if(num_nodes)
		*num_nodes = nNodes;	
	
	if(nodes)
	{
		*nodes = (int32 *)palloc0((nNodes) * sizeof(int32));
		memcpy((char*)*nodes, (char*)&(g_GroupShardingMgr->members[ent->shardIndex]->shmemshardnodes), (nNodes) * sizeof(int32));
	}
	
	if(isextension)
	{
		if (g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShards == SHARD_MAP_SHARD_NUM)
			*isextension = false;
		else if(g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShards == EXTENSION_SHARD_MAP_SHARD_NUM)
			*isextension= true;
		else
			elog(ERROR, "shards(%d) of group is invalid ", g_GroupShardingMgr->members[ent->shardIndex]->shmemNumShards);
	}
	
	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
}

static void GetShardNodes_DN(Oid group, int32 ** nodes, int32 *num_nodes, bool *isextension)
{
	bool		   needLock;
	int 			nNodes = 0;	


	if (!IS_PGXC_DATANODE)
	{
		elog(ERROR, "InsertShardMap_DN should only be called in datanode");
	}
	

	SyncShardMapList(false);

	needLock  = g_GroupShardingMgr_DN->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}

	if (!g_GroupShardingMgr_DN->used || g_GroupShardingMgr_DN->members->group != group)
	{
		elog(ERROR , "[GetShardNodes_DN]corrupted catalog, no shard group of %u found", group);
	}
	
	nNodes = g_GroupShardingMgr_DN->members->shmemNumShardNodes;
	
	if(num_nodes)
		*num_nodes = nNodes;	
	
	if(nodes)
	{
		*nodes = (int32 *)palloc0((nNodes) * sizeof(int32));
		memcpy((char*)*nodes, (char*)&(g_GroupShardingMgr_DN->members->shmemshardnodes), (nNodes) * sizeof(int32));
	}
	
	if(isextension)
	{
		if (g_GroupShardingMgr_DN->members->shmemNumShards == SHARD_MAP_SHARD_NUM)
			*isextension = false;
		else if(g_GroupShardingMgr_DN->members->shmemNumShards == EXTENSION_SHARD_MAP_SHARD_NUM)
			*isextension= true;
		else
			elog(ERROR, "shards(%d) of group is invalid ", g_GroupShardingMgr_DN->members->shmemNumShards);
	}
	
	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
}


void PrepareMoveData(MoveDataStmt* stmt)
{
	Oid	   group_oid;
	Oid		*dnoids = NULL;
	int 	ndns = 0;
	Value *fromvalue;
	Value *tovalue;	
	int i;
	bool found;

	fromvalue = &(stmt->from_node->val);
	tovalue = &(stmt->to_node->val);

	if (IsA(fromvalue,String))
	{
		stmt->fromid = get_pgxc_nodeoid(strVal(fromvalue));
		if(!OidIsValid(stmt->fromid))
		{
			elog(ERROR,"datanode[%s] is not exist",strVal(fromvalue));
		}
	}
	else if(IsA(fromvalue,Integer))
	{
		stmt->fromid = intVal(fromvalue);
	}
	else
	{
		elog(ERROR, "PgxcMoveData error");
	}

	
	if (IsA(tovalue,String))
	{
		stmt->toid = get_pgxc_nodeoid(strVal(tovalue));
		if(!OidIsValid(stmt->toid))
		{
			elog(ERROR,"datanode[%s] is not exist",strVal(tovalue));
		}
	}
	else if(IsA(tovalue,Integer))
	{
		stmt->toid = intVal(tovalue);
	}
	else
	{
		elog(ERROR, "PgxcMoveData error");
	}

	
	group_oid = get_pgxc_groupoid(strVal(stmt->group));
	if (!OidIsValid(group_oid))
	{
		elog(ERROR, "group with name:%s not found", strVal(stmt->group));
	}

	/* check if the group contains this two datanodes */
	ndns = get_pgxc_groupmembers(group_oid, &dnoids);

	if(ndns <= 0)
	{
		elog(ERROR, "group %s dose not contain datanode %s or datanode %s", strVal(stmt->group), strVal(fromvalue), strVal(tovalue));
	}

	found = false;
	for(i = 0; i < ndns; i++)
	{
		if(stmt->fromid == dnoids[i])
			found = true;
	}
	if(!found)
	{
		elog(ERROR, "group %s dose not contain datanode %s", strVal(stmt->group), strVal(fromvalue));
	}

	found = false;
	for(i = 0; i < ndns; i++)
	{
		if(stmt->toid== dnoids[i])
			found = true;
	}
	if(!found)
	{
		elog(ERROR, "group %s dose not contain datanode %s", strVal(stmt->group), strVal(tovalue));
	}

	//GetShardNodes(group_oid, NULL, NULL, &stmt->isextension);
	SyncShardMapList(false);
	
	switch (stmt->strategy)
	{
		case MOVE_DATA_STRATEGY_SHARD:
			{
				//elog(ERROR,"shard-based data moving is not supported in coordinator.");
				ListCell 	*lc;
				A_Const 	*con;
				Value		*val;
				int			shard;
				int			shard_idx = 0;

				stmt->num_shard = list_length(stmt->shards);
				if(list_length(stmt->shards) == 0)
					elog(ERROR, "shard list must be assigned");
				stmt->arr_shard = palloc0(stmt->num_shard * sizeof(int32));
				foreach(lc,stmt->shards)
				{
					con = (A_Const *)lfirst(lc);
					val = &(con->val);
					shard = intVal(val);
					if(shard < 0)
						elog(ERROR, "%d shard is invalid, shard to move must be greater than or equal with zero", shard);
					stmt->arr_shard[shard_idx++] = shard;
				}

				qsort(stmt->arr_shard, 
						stmt->num_shard, 
						sizeof(int32), 
						cmp_int32);
				stmt->split_point = -1;
			}
			break;
		case MOVE_DATA_STRATEGY_NODE:
			{
				stmt->arr_shard = decide_shardgroups_to_move(group_oid, stmt->fromid, &stmt->num_shard);
				stmt->split_point = stmt->arr_shard[stmt->num_shard - 1];
				if(NodeHasShard(stmt->toid))
				{
					elog(ERROR, "shard must be moved to a new node which is ready for data but no shard");
				}
			}
			break;
		case  MOVE_DATA_STRATEGY_AT:
			elog(ERROR, "Move Strategy MOVE_DATA_STRATEGY_AT is not supported in coordinator");
			break;
		default:
			elog(ERROR, "Invalid Move Stratety %d", stmt->strategy);
	}

	pfree(dnoids);
}

static int* 
decide_shardgroups_to_move(Oid group, Oid fromnode, int *shardgroup_num)
{
	Relation    shardmapRel;
	ScanKeyData skey[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	Form_pgxc_shard_map pgxc_shard;
	List * tuplist;
	int *shardgroups;
	int i;

	tuplist = NULL;
	
	shardmapRel = heap_open(PgxcShardMapRelationId, AccessShareLock);
	
	ScanKeyInit(&skey[0],
					Anum_pgxc_shard_map_nodegroup,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(group));
	
	ScanKeyInit(&skey[1],
					Anum_pgxc_shard_map_primarycopy,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(fromnode));	
	
	scan = systable_beginscan(shardmapRel,
								PgxcShardMapGroupIndexId,true,
								NULL,
								2,
								skey);

	tuple = systable_getnext(scan);

	while(HeapTupleIsValid(tuple))
	{
		pgxc_shard = (Form_pgxc_shard_map) GETSTRUCT(tuple);
		tuplist = lappend_int(tuplist,pgxc_shard->shardgroupid);
		tuple = systable_getnext(scan);
	}

	if(list_length(tuplist) == 0)
	{
		elog(ERROR, "datanode[%d] has no shardgroup to be moved",fromnode);
	}
	else if(list_length(tuplist) == 1)
	{
		elog(ERROR, "datanode[%d] has only one shardgroup, it cannot be splited",fromnode);
	}

	*shardgroup_num = list_length(tuplist)/2;
	shardgroups = (int*)palloc0((*shardgroup_num) * sizeof(int));
	
	for (i = 0; i < *shardgroup_num; i++)
	{
		int shardgroupid;
		shardgroupid = list_nth_int(tuplist,i);
		shardgroups[i] = shardgroupid;
	}

	systable_endscan(scan);
	heap_close(shardmapRel,AccessShareLock);

	list_free(tuplist);

	return shardgroups;
}

void PgxcMoveData_Node(MoveDataStmt* stmt)
{	
	Oid group_oid;
	group_oid = get_pgxc_groupoid(strVal(stmt->group));
	if (!OidIsValid(group_oid))
	{
		elog(ERROR, "group with name:%s not found", strVal(stmt->group));
	}

	UpdateRelationShardMap(group_oid, stmt->fromid,stmt->toid,stmt->num_shard,stmt->arr_shard, stmt->scid);
}

/*
 * Diff from CN, DN should truncate all non-shard tables in to-node when split one dn to two.
 */
void PgxcMoveData_DN(MoveDataStmt* stmt)
{	
	//Value *fromvalue;
	Value *tovalue;
	bool	isFrom;

	//fromvalue = &(stmt->from_node->val);
	tovalue = &(stmt->to_node->val);
	isFrom = true;

	/* Only tonode and strategy equals to MOVE_DATA_STRATEGY_AT should truncate all non-shard tables */
	if(IsA(tovalue,String) && strncmp(strVal(tovalue), PGXCNodeName, NAMEDATALEN)==0)
	{
		isFrom = false;
	}
	else
	{
		elog(LOG, "[PgxcMoveData_DN]Nove data tonode:%s not datanode itself:%s, "
				  "no need to truncate all non-shard tables.", strVal(tovalue), PGXCNodeName);
		return;
	}


	switch(stmt->strategy)
	{
		case MOVE_DATA_STRATEGY_SHARD:
			{
				elog(LOG,"[PgxcMoveData_DN]When using strategy MOVE_DATA_STRATEGY_SHARD, no need to"
					     " truncate all non-shard tables.");
			}
			break;
	 	case MOVE_DATA_STRATEGY_AT:
			{
				if(!isFrom)
				{
					List * relnames = NULL;
					List * nsnames = NULL;
					List * relrvs = NULL;
					/* get rel list except shard table */
					GetNotShardRelations(false, &relnames, &nsnames);
					
					if(list_length(relnames) != 0)
					{
						ListCell *relcell;
						ListCell *nscell;
						RangeVar * rv;
						char 	*relname;
						char	*nsname;
						TruncateStmt * stmt = (TruncateStmt*)makeNode(TruncateStmt);

						stmt->paretablename = NULL;
						stmt->behavior = DROP_CASCADE;
						stmt->restart_seqs = false;

						forboth(relcell, relnames, nscell, nsnames)
						{
							relname = (char *)lfirst(relcell);
							nsname = (char *)lfirst(nscell);
							rv = makeRangeVar(nsname, relname, -1);
							
							elog(LOG, "truncate relation %s because of moving data in new datanode",relname);
							relrvs = lappend(relrvs, rv);
						}

						stmt->relations = relrvs;		
						ExecuteTruncate(stmt);
						list_free_deep(relnames);
						list_free_deep(nsnames);
						list_free_deep(relrvs);
					}
				}
			}
			break;
		case MOVE_DATA_STRATEGY_NODE:
			//elog(ERROR, "Move Strategy MOVE_DATA_STRATEGY_NODE is not supported in data node");
			{
				if(!isFrom)
				{
					List * relnames = NULL;
					List * nsnames = NULL;
					List * relrvs = NULL;
					/* get rel list except shard table */
					GetNotShardRelations(false, &relnames, &nsnames);
					
					if(list_length(relnames) != 0)
					{
						ListCell *relcell;
						ListCell *nscell;
						RangeVar * rv;
						char 	*relname;
						char	*nsname;
						TruncateStmt * stmt = (TruncateStmt*)makeNode(TruncateStmt);

						stmt->paretablename = NULL;
						stmt->behavior = DROP_CASCADE;
						stmt->restart_seqs = false;

						forboth(relcell, relnames, nscell, nsnames)
						{
							relname = (char *)lfirst(relcell);
							nsname = (char *)lfirst(nscell);
							rv = makeRangeVar(nsname, relname, -1);
							
							elog(LOG, "truncate relation %s because of moving data in new datanode",relname);
							relrvs = lappend(relrvs, rv);
						}

						stmt->relations = relrvs;		
						ExecuteTruncate(stmt);
						list_free_deep(relnames);
						list_free_deep(nsnames);
						list_free_deep(relrvs);
					}
				}
			}
			break;
		default:
			elog(ERROR, "Invalid Move Strategy %d", stmt->strategy);
			break;
	}
}


void UpdateReplicaRelNodes(Oid newnodeid)
{
	Relation	rel;
	HeapTuple	oldtup;
	HeapScanDesc scan;
	Form_pgxc_class pgxc_class;
	Oid *old_oid_array;
	Oid *new_oid_array;
	int new_num;
	int old_num;

	if(!OidIsValid(newnodeid))
	{
		elog(ERROR,"add invalid node oid to relation nodes");
	}

	rel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	scan = heap_beginscan_catalog(rel,0,NULL);
	
	oldtup = heap_getnext(scan,ForwardScanDirection);
	
	while(HeapTupleIsValid(oldtup))
	{
		pgxc_class = (Form_pgxc_class)GETSTRUCT(oldtup);

		if(pgxc_class->pclocatortype != LOCATOR_TYPE_REPLICATED)
		{
			oldtup = heap_getnext(scan,ForwardScanDirection);
			continue;
		}

		old_num = get_pgxc_classnodes(pgxc_class->pcrelid, &old_oid_array);

		if(oidarray_contian_oid(old_oid_array, old_num, newnodeid))
		{
			oldtup = heap_getnext(scan,ForwardScanDirection);
			continue;
		}

		new_oid_array = add_node_list(old_oid_array, old_num, &newnodeid, 1, &new_num);
		
		/* Sort once again the newly-created array of node Oids to maintain consistency */
		new_oid_array = SortRelationDistributionNodes(new_oid_array, new_num);

		/* Update pgxc_class entry */
		PgxcClassAlter(pgxc_class->pcrelid,
				   '\0',
				   0,
				   NULL,
				   0,
				   0,
				   new_num,
				   new_oid_array,
				   PGXC_CLASS_ALTER_NODES);

		oldtup = heap_getnext(scan,ForwardScanDirection);
	}

	
	heap_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}

/*
 * fresh shard map in share memory.
 * groupoid can be InvalidOid when this function called in datanode
*/
void ForceRefreshShardMap(Oid groupoid)
{
	LWLockAcquire(ShardMapLock, LW_EXCLUSIVE);
	if(!OidIsValid(groupoid))
	{	
		if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
		{
			g_GroupShardingMgr->inited = false;
			SyncShardMapListForAllGroupInternal();
			g_GroupShardingMgr->inited = true;

		}
		if (IS_PGXC_DATANODE)
		{	
			g_GroupShardingMgr_DN->inited = false;
			g_GroupShardingMgr_DN->inited = SyncShardMapListForCurrGroupInternal();
		}
	}
	LWLockRelease(ShardMapLock);
        
        /*
         * Invalidate the relcache after refresh shard map in shmem,
         * because Relation->rd_locator_info changed.
         */
        CacheInvalidateRelcacheAll();
}

/*
 * upper level has taken care of the locks
 */
static void RemoveShardMapEntry(Oid group)
{
	bool           found;
	int32          map = 0;
	GroupLookupTag tag;
	GroupLookupEnt *ent;
	tag.group =  group;	

	if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
	{
		ent = (GroupLookupEnt*)hash_search(g_GroupHashTab, (void *) &tag, HASH_FIND, &found);
		if (!ent)
		{			
			return;
		}
	    map = ent->shardIndex;	

		if (!hash_search(g_GroupHashTab, (void *) &tag, HASH_REMOVE, NULL))
		{
			elog(ERROR, "hash table corrupted");
		}

		SpinLockAcquire(&g_GroupShardingMgr->lock[map]);
		g_GroupShardingMgr->used[map]          =  false;
		g_GroupShardingMgr->members[map]->group =  InvalidOid;
		SpinLockRelease(&g_GroupShardingMgr->lock[map]);	
	}
	if (IS_PGXC_DATANODE)
	{
		SpinLockAcquire(&g_GroupShardingMgr_DN->lock);
		if (g_GroupShardingMgr_DN->members->group == group)
		{
			g_GroupShardingMgr_DN->used = false;
			g_GroupShardingMgr_DN->members->group = InvalidOid;
			bms_clear(g_DatanodeShardgroupBitmap);
			bms_clear(g_DatanodeShardClusterBitmap);
		}
		SpinLockRelease(&g_GroupShardingMgr_DN->lock);
	}
}

/* sync shard map list of a group */
static void FreshGroupShardMap(Oid group)
{
	SysScanDesc scan;
	HeapTuple  oldtup;
	Relation shardrel;
	Form_pgxc_shard_map  pgxc_shard;
	int32 shard_mgr = INVALID_SHARDMAP_ID;		
	ScanKeyData skey;
	Oid   self_node_oid = InvalidOid;

	/* 
	 * In datanode, one can only see the shardmap belongs to single group,
	 * so it's ok to clear the g_DatanodeShardgroupBitmap here.
	 */
	if (IS_PGXC_DATANODE)
	{
		bms_clear(g_DatanodeShardgroupBitmap);
		bms_clear(g_DatanodeShardClusterBitmap);
	}

	self_node_oid = get_pgxc_nodeoid_extend(PGXCNodeName, pgxc_plane_name(PGXCMainPlaneNameID));
	/* build sharding table */
	shardrel = heap_open(PgxcShardMapRelationId, AccessShareLock);
	ScanKeyInit(&skey,
		Anum_pgxc_shard_map_nodegroup,
		BTEqualStrategyNumber, F_OIDEQ,
		ObjectIdGetDatum(group));

	scan = systable_beginscan(shardrel,
							  PgxcShardMapGroupIndexId, 
							  true,
							  SnapshotSelf, 1, &skey);

	/* no such tuples exist, so remove the existing map entry */
	oldtup = systable_getnext(scan);
	if (!HeapTupleIsValid(oldtup))
	{
		RemoveShardMapEntry(group);
	}

	while(HeapTupleIsValid(oldtup))
	{
		pgxc_shard = (Form_pgxc_shard_map)GETSTRUCT(oldtup);

		if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
		{
			if (INVALID_SHARDMAP_ID == shard_mgr)
			{
				/* If the group sharding has not been created */
				if (!GroupShardingTabExist(group, false, &shard_mgr))
				{
					/* here, we try to get a new shard mapping mgr */
					shard_mgr = AllocShardMap(pgxc_shard->extended, group);
					if (INVALID_SHARDMAP_ID == shard_mgr)
					{
						elog(ERROR, "too many sharding group defined");
					}
				}
			}
			InsertShardMap_CN(shard_mgr, pgxc_shard);	
		}
		if (IS_PGXC_DATANODE)
		{
			/*
			 * If sharding of the group has not been inited, or this sharding map is in use but
			 * store overdue information, possibly caused by group syncing backend crashing right
			 * before the shmem sync.
			 */
			if (!g_GroupShardingMgr_DN->used || group != g_GroupShardingMgr_DN->members->group)
			{
				/*
				 * Datanodes can only be in one node group, so we save the effort of
				 * removing entry and skip right into resetting the mgr.
				 */
				g_GroupShardingMgr_DN->members->shardMapStatus = SHMEM_SHRADMAP_STATUS_LOADING;
				SpinLockAcquire(&g_GroupShardingMgr_DN->lock);
				g_GroupShardingMgr_DN->members->group = group;
				g_GroupShardingMgr_DN->used = true;
				SpinLockRelease(&g_GroupShardingMgr_DN->lock);
			}

			InsertShardMap_DN(pgxc_shard);
		}

		oldtup = systable_getnext(scan);
		if (IS_PGXC_DATANODE)
		{
			BuildDatanodeVisibilityMap(pgxc_shard, self_node_oid);
		}
	}

	systable_endscan(scan);
	heap_close(shardrel, AccessShareLock);	

	if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE)
	{
		if(INVALID_SHARDMAP_ID != shard_mgr)
		{
			ShardMapInitDone_CN(shard_mgr, group, false);	
		}
	}
	if (IS_PGXC_DATANODE)
	{
		if(INVALID_SHARDMAP_ID != shard_mgr)
		{
			ShardMapInitDone_DN(group, false);	
		}
	}
}

/*
 * It's caller's responsibilty to ensure g_DatanodeShardgroupBitmap and g_DatanodeShardClusterBitmap is cleared.
 */
static void BuildDatanodeVisibilityMap(Form_pgxc_shard_map tuple, Oid self_oid)
{
	Bitmapset *tmp = NULL;
	
	if (tuple->primarycopy == self_oid)
	{
		tmp = bms_add_member(g_DatanodeShardgroupBitmap, tuple->shardgroupid);
		if(tmp != g_DatanodeShardgroupBitmap)
		{
			elog(PANIC, "build shard group bitmap failed. new bitmap maybe not in share memory");
		}
		elog(DEBUG1, "shardid %d belongs to datanode:%s", tuple->shardgroupid, PGXCNodeName);
		
		/* add sc_id into g_DatanodeShardClusterBitmap if needed*/
		if (!bms_is_member(tuple->shardclusterid, g_DatanodeShardClusterBitmap))
		{
			tmp = bms_add_member(g_DatanodeShardClusterBitmap, tuple->shardclusterid);
			if(tmp != g_DatanodeShardClusterBitmap)
			{
				elog(PANIC, "build shard cluster bitmap failed. new bitmap maybe not in share memory");
			}
			elog(DEBUG1, "shard cluster %d belongs to datanode:%s", tuple->shardclusterid, PGXCNodeName);
		}
	}
	else
	{
		elog(DEBUG1, "shardid %d shardclusterid %d belongs to datanode whose oid:%d, NOT datanode itself:%s",
				 tuple->shardgroupid, tuple->shardclusterid, tuple->primarycopy, PGXCNodeName);
	}
}


Bitmapset * 
CopyShardGroups_DN(Bitmapset * shard, Bitmapset * shardcluster)
{
	SyncShardMapList(false);	

	/* Only one group exists in  */
	if(g_GroupShardingMgr_DN->members->shardMapStatus != SHMEM_SHRADMAP_STATUS_USING)
	{
		return NULL;
	}

	LWLockAcquire(ShardMapLock, LW_SHARED);	
	memcpy(shard, g_DatanodeShardgroupBitmap, SHARD_TABLE_BITMAP_SIZE);
	memcpy(shardcluster, g_DatanodeShardClusterBitmap, SHARD_CLUSTER_BITMAP_SIZE);
	LWLockRelease(ShardMapLock);
	
	return shard;
}


int GetGroupSize()
{
	return 1;
}

/*
 * Acquire node oid according to the shardid
 */
Oid
GetNodeOidByShardid(int shardid)
{
	Oid 		   nodeOid = 0;
	bool           needLock = false;
	ShardMapItemDef *shardgroup;

	needLock = g_GroupShardingMgr_DN->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}

	shardgroup = &g_GroupShardingMgr_DN->members->shmemshardmap[shardid];
	nodeOid = shardgroup->primarycopy;

	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
	return nodeOid;
}

int
EvaluateSCIdWithValuesNulls(Relation relation,
											TupleDesc tupledesc,
											Datum *values,
											bool *nulls)
{
	ShardID shardId = InvalidShardID;
	int sc_id = InvalidShardClusterId;

	shardId = EvaluateShardIdWithValuesNulls(relation, tupledesc, values, nulls);

	if (advance_verification_action != VERIFY_ACTION_NONE && RelationIsSharded(relation))
	{
		Oid nodeOid = 0;
		if (!ShardIDIsValid(shardId))
		{
			int saved_stacktrace_level = error_stacktrace_level;
			error_stacktrace_level = STACKTRACE_LOG;

			elog(advance_verification_action, "invalid shardId %d, relation is %s",
					shardId, RelationGetRelationName(relation));

			error_stacktrace_level = saved_stacktrace_level;
		}
		else if (IS_PGXC_DATANODE && MyLogicalRepWorker == NULL)
		{	
			/* LogicalRepWorker need skip this verify, rel shard map not modify to correct */
			nodeOid = GetNodeOidByShardid(shardId);
			if (nodeOid != get_pgxc_nodeoid(PGXCNodeName))
			{
				int saved_stacktrace_level = error_stacktrace_level;
				error_stacktrace_level = STACKTRACE_LOG;

				elog(advance_verification_action,
					"shardId %d, should not be inserted to this node, correct nodeoid is %u, relation is %s",
						shardId, nodeOid, RelationGetRelationName(relation));

				error_stacktrace_level = saved_stacktrace_level;
			}
		}
	}
	sc_id = GetShardClusterID(shardId);

	return sc_id;
}

/*
 * Computer ShardID for given values and nulls.
 * Return InvalidShardID for non-sharded relation.
 */
ShardID
EvaluateShardIdWithValuesNulls(Relation relation,
											TupleDesc tupledesc,
											Datum *values,
											bool *nulls)
{
	ShardID shardId = InvalidShardID;

	if (RelationIsSharded(relation))
	{
		Datum *distvalues = NULL;
		bool *distnulls = NULL;
		Oid *colTypes = NULL;
		int num_dist_cols = 0;
		AttrNumber *other_dist_keys = NULL;
		int i = 0;

		other_dist_keys = RelationGetDisKeys(relation);
		num_dist_cols = RelationGetNumDisKeys(relation);
		distvalues = (Datum *)palloc(sizeof(Datum) * num_dist_cols);
		distnulls = (bool *)palloc(sizeof(bool) * num_dist_cols);
		colTypes = (Oid *)palloc(sizeof(Oid) * num_dist_cols);
		
		if (num_dist_cols > 0)
		{
			for (i = 0; i < num_dist_cols; i++)
			{
				colTypes[i] = tupledesc->attrs[other_dist_keys[i] - 1].atttypid;
				distvalues[i] = values[other_dist_keys[i] - 1];
				distnulls[i] = nulls[other_dist_keys[i] - 1];
			}
		}

		shardId = EvaluateShardId(colTypes, distnulls, distvalues, num_dist_cols);

		pfree(distvalues);
		pfree(distnulls);
		pfree(colTypes);
	}

	return shardId;
}


/*
 * Get ShardId in datanode
 */
int32 EvaluateShardId(Oid *type, bool *isNull, Datum *dvalue, int nAttr)
{
	int i = 0;
	uint32 hashkey = 0;

	if (nAttr <= 0)
	{
		return NullShardId;
	}

	for (i = 0; i < nAttr; i++)
	{
		uint32	hkey;
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);
		if (!isNull[i])
		{
			hkey = DatumGetUInt32(compute_hash(type[i], dvalue[i], LOCATOR_TYPE_SHARD)); 
			hashkey ^= hkey;
		}
	}

	return abs((int)hashkey) % MAX_SHARDS;
}

int
TruncateShard(Oid reloid, ShardID sid, int pausetime)
{
	ExtentID eid = InvalidExtentID;
	int	tuples = 0;
	Relation rel = NULL;
	Oid		toastoid = InvalidOid;

	StartTransactionCommand();
	rel = heap_open(reloid, AccessShareLock);
	toastoid = rel->rd_rel->reltoastrelid;

	if(!RelationHasExtent(rel))
	{
		elog(ERROR, "only sharded table can be truncated by shard.");
	}

	/*
	 * step 1: add shard barrier
	 */
	AddShardBarrier(rel->rd_node, sid, MyProcPid);
	heap_close(rel,AccessShareLock);
	CommitTransactionCommand();

	/*
	 * step 2: do checkpoint, make sure dirty page of this shard flushed to storage. 
	 */
	RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

	/*
	 * step 3: start remove index items and recycle storage space
	 */
	StartTransactionCommand();
	eid = RelOidGetShardScanHead(reloid, sid);
	
	while(ExtentIdIsValid(eid))
	{
		int deleted_tuples = 0;

		rel = heap_open(reloid, RowExclusiveLock);
		/*
		 * delete this extent's tuples and their index entries
		 */
		truncate_extent_tuples(rel, 
								eid * PAGES_PER_EXTENTS, 
								(eid+1) * PAGES_PER_EXTENTS, 
								false, 
								&deleted_tuples);
		tuples += deleted_tuples;

		/*
		 * end transaction 
		 */
		heap_close(rel, RowExclusiveLock);
		rel = NULL;
		CommitTransactionCommand();

		/*
		 * start another transaction
		 */
		StartTransactionCommand();

		rel = heap_open(reloid, AccessExclusiveLock);

		RelationOpenSmgr(rel);
#ifndef DISABLE_FALLOCATE
		log_smgrdealloc(&rel->rd_node, eid, SMGR_DEALLOC_FREESTORAGE, RelationHasChecksum(rel));
		smgrdealloc(rel->rd_smgr, MAIN_FORKNUM, eid * PAGES_PER_EXTENTS);
		if(trace_extent)
		{
			ereport(LOG,
				(errmsg("[trace extent]Dealloc:[rel:%d/%d/%d]"
						"[eid:%d, flags=FREESTORAGE]",
						rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
						eid)));
		}
#else
		log_smgrdealloc(&rel->rd_node, eid, SMGR_DEALLOC_REINIT, RelationHasChecksum(rel));
		reinit_extent_pages(rel, eid);

		if(trace_extent)
		{
			ereport(LOG,
				(errmsg("[trace extent]Dealloc:[rel:%d/%d/%d]"
						"[eid:%d, flags=REINIT_PAGE]",
						rel->rd_node.dbNode, rel->rd_node.spcNode, rel->rd_node.relNode,
						eid)));
		}
#endif		
		/* 
		 * detach extent
		 */
		FreeExtent(rel, eid);
		heap_close(rel, AccessExclusiveLock);
		rel = NULL;
		
		eid = RelOidGetShardScanHead(reloid, sid);

		if(ExtentIdIsValid(eid) && pausetime > 0)
			pg_usleep(pausetime);
	}
	CommitTransactionCommand();

	StartTransactionCommand();

	/*
	 * step 4: invalidate buf page.
	 */
#ifndef DISABLE_FALLOCATE
	rel = heap_open(reloid, AccessShareLock);	
	DropRelfileNodeShardBuffers(rel->rd_node, sid);
	heap_close(rel, AccessShareLock);
#endif

	/*
	 * step 5: release barrier
	 */
	RemoveShardBarrier();
	CommitTransactionCommand();
	
	if(OidIsValid(toastoid))
	{
		TruncateShard(toastoid, sid, pausetime);
	}
	
	return tuples;
}
void StatShardRelation(Oid relid, ShardStat *shardstat, int32 shardnumber)
{
	int32        shardid;
	Relation     rel;
	HeapScanDesc scan;
	HeapTuple    tuple;
	BufferAccessStrategy bstrategy;
	Snapshot	snapshot;
	
	rel = heap_open(relid, AccessShareLock);
	if (rel->rd_locator_info)
	{
		if (LOCATOR_TYPE_SHARD == rel->rd_locator_info->locatorType)
		{
			snapshot = GetTransactionSnapshot();
			scan  = heap_beginscan(rel,snapshot,0,NULL);		
			
			/* set proper strategy */
			bstrategy = GetAccessStrategy(BAS_BULKREAD_STAT_SHARD);
			if (scan->rs_strategy)
			{
				FreeAccessStrategy(scan->rs_strategy);
			}
			
			scan->rs_strategy = bstrategy;			
			tuple = heap_getnext(scan,ForwardScanDirection);			
			while(HeapTupleIsValid(tuple))
			{
				shardid = HeapTupleGetShardId(tuple);
				if (shardid >= shardnumber)
				{
					elog(ERROR, "invalid shard id:%d of shard tuple in relation:%u", shardid, relid);	
				}
				else
				{
					shardstat[shardid].shard = shardid;
					shardstat[shardid].count ++;
					shardstat[shardid].size += tuple->t_len;
				}
				tuple   = heap_getnext(scan,ForwardScanDirection);
			}

			heap_endscan(scan);
		}
	}
	heap_close(rel, AccessShareLock);
}

void
SampleRowRelationScStatistic(Relation rel, ShardStat *shardstat, int32 shardnumber, float *ratio)
{
	int32 shardid;
	HeapScanDesc scan;
	HeapTuple tuple;
	BufferAccessStrategy bstrategy;
	Snapshot snapshot;
	int i;
	if (rel->rd_locator_info)
	{

#define  PG_CNT_16_SEG (16 * RELSEG_SIZE)

#define  BASE_DELTA (RELSEG_SIZE / 8)

#define  MIN_SEQ_SCAN_PGS 128

#define  RATIO_MAX 1.0
#define  RATIO_TO_MAX 0.9
		
		BlockNumber total_blk;
		BlockNumber cur_start_blk = 0;
		BlockNumber delta, base_delta;
		int32 loop;
		
		total_blk = RelationGetNumberOfBlocks(rel);
		if (total_blk < 8 * MIN_SEQ_SCAN_PGS)
		{
			*ratio = RATIO_MAX;
			loop = 1;
			base_delta = total_blk;
			delta = total_blk;
		}
		else if (total_blk < BASE_DELTA * 8)
		{
			base_delta = 8 * MIN_SEQ_SCAN_PGS;
			
			if (base_delta * (*ratio) < MIN_SEQ_SCAN_PGS)
			{
				*ratio = MIN_SEQ_SCAN_PGS / (float) base_delta;
			}
			
			delta = (int32) (base_delta * *ratio);
			loop = total_blk / base_delta + 1;
		}
		else
		{
			base_delta = BASE_DELTA;
			
			if (base_delta * *ratio < MIN_SEQ_SCAN_PGS)
			{
				*ratio = MIN_SEQ_SCAN_PGS / (float) base_delta;
			}
			
			delta = (int32) (base_delta * *ratio);
			loop = total_blk / base_delta + 1;
		}
		
		if (*ratio > RATIO_TO_MAX)
		{
			*ratio = RATIO_MAX;
			loop = 1;
			base_delta = total_blk;
			delta = total_blk;
		}
		
		for (i = 0; i < loop; i++)
		{
			if (cur_start_blk >= total_blk - 1)
			{
				break;
			}
			if (cur_start_blk + delta > total_blk - 1)
			{
				delta = total_blk - cur_start_blk;
			}
			if (LOCATOR_TYPE_SHARD == rel->rd_locator_info->locatorType)
			{
				elog(DEBUG1, "loop %d cur_start_blk %d delta %d", i, cur_start_blk, delta);
				snapshot = GetTransactionSnapshot();
				scan = heap_beginscan(rel, snapshot, 0, NULL);
				heap_setscanlimits(scan, cur_start_blk, delta);
				/* set proper strategy */
				bstrategy = GetAccessStrategy(BAS_BULKREAD_STAT_SHARD);
				if (scan->rs_strategy)
				{
					FreeAccessStrategy(scan->rs_strategy);
				}
				
				scan->rs_strategy = bstrategy;
				tuple = heap_getnext(scan, ForwardScanDirection);
				while (HeapTupleIsValid(tuple))
				{
					/* For v3, the statistic unit is shardcluster. */
					shardid = GetShardClusterID(HeapTupleGetShardId(tuple));
					if (shardid >= shardnumber)
					{
						elog(ERROR,
						     "invalid shardcluster id:%d of shard tuple in relation:%s",
						     shardid,
						     NameStr(rel->rd_rel->relname));
					}
					else
					{
						shardstat[shardid].shard = shardid;
						shardstat[shardid].count++;
						shardstat[shardid].size += tuple->t_len;
					}
					tuple = heap_getnext(scan, ForwardScanDirection);
				}
				
				heap_endscan(scan);
			}
			cur_start_blk = cur_start_blk + base_delta;
		}
		
	}
}

void
SampleShardStatistic(ShardStat *shardstat, int32 shardnumber, float ratio)
{
	Oid relid;
	Relation classRel;
	HeapTuple tuple;
	HeapScanDesc relScan;
	ShardStat_State *tempStatus;
	int i;
	float tableRatio;
	Form_pg_class classForm = NULL;
	Relation rel = NULL;
	MemoryContext ctx;

	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	/*
	 * Here we only process main tables, skip other kind relations
	 */
	tempStatus = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		tempStatus->currIdx = 0;
		while (tempStatus->currIdx < shard_cluster_num)
		{
			tempStatus->shardstat[tempStatus->currIdx].shard = tempStatus->currIdx;
			tempStatus->shardstat[tempStatus->currIdx].count = 0;
			tempStatus->shardstat[tempStatus->currIdx].size = 0;
			tempStatus->currIdx++;
		}
		tempStatus->currIdx = 0;
		
		classForm = (Form_pg_class) GETSTRUCT(tuple);
        relid = HeapTupleGetOid(tuple);
        
		if ((classForm->relkind != RELKIND_RELATION &&
		    classForm->relkind != RELKIND_MATVIEW) ||
			classForm->relpersistence == RELPERSISTENCE_GLOBAL_TEMP ||
			classForm->relpersistence == RELPERSISTENCE_TEMP ||
            relid < FirstNormalObjectId)
			continue;
        
		tableRatio = ratio;

		ctx = CurrentMemoryContext;
        PG_TRY();
        {
            rel = heap_open(relid, AccessShareLock);
            if (RelationIsRowStore(rel))
            {
                SampleRowRelationScStatistic(rel, tempStatus->shardstat, shardnumber, &tableRatio);
                for (i = 0; i < shard_cluster_num; i++)
                {
                    shardstat[i].count += (int64)((double) tempStatus->shardstat[i].count / tableRatio);
                    shardstat[i].size += (int64)((double) tempStatus->shardstat[i].size / tableRatio);
                }
            }
            heap_close(rel, AccessShareLock);
        }
        PG_CATCH();
        {
			MemoryContextSwitchTo(ctx);
			FlushErrorState();

            elog(LOG, "could not sample shard of relation %u %s", relid, NameStr(classForm->relname));
        }
        PG_END_TRY();
	}
	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);
	pfree(tempStatus);
}

void StatShardAllRelations(ShardStat *shardstat, int32 shardnumber)
{
	Oid 			 relid;
	Relation		 classRel;
	HeapTuple		 tuple;
	HeapScanDesc	 relScan;
	
	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);
	/*
	 * Here we only process main tables, skip other kind relations
	 */
	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;

		relid = HeapTupleGetOid(tuple);
		StatShardRelation(relid, shardstat, shardnumber);
	}
	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);
}

Datum
opentenbase_shard_stat_internal(PG_FUNCTION_ARGS)
{
#define  COLUMN_NUM_3 3
	
	FuncCallContext *funcctx;
	ShardStat_State *status;
	float ratio = 1.0;
	
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupdesc;
		MemoryContext oldcontext;
		
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(COLUMN_NUM_3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shard_id",
		                   INT4OID, -1, 0);
		
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ntups",
		                   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "size",
		                   INT8OID, -1, 0);
		
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		status = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
		funcctx->user_fctx = (void *) status;
		status->currIdx = 0;
		while (status->currIdx < shard_cluster_num)
		{
			status->shardstat[status->currIdx].shard = status->currIdx;
			status->currIdx++;
		}
		status->currIdx = 0;
		
		/* get sampling statistic ratio. */
		if (!PG_ARGISNULL(0))
		{
			ratio = PG_GETARG_FLOAT4(0);
			if (ratio < 1e-6)
			{
				ratio = 1e-6;
			}
			else if (ratio > 1.0)
			{
				ratio = 1.0;
			}
		}
		SampleShardStatistic(status->shardstat, shard_cluster_num, ratio);
		
		// RefreshShardStatistic(status->shardstat);
		
		MemoryContextSwitchTo(oldcontext);
	}
	
	funcctx = SRF_PERCALL_SETUP();
	status = (ShardStat_State *) funcctx->user_fctx;
	
	while (status->currIdx < shard_cluster_num)
	{
		Datum values[COLUMN_NUM_3];
		bool nulls[COLUMN_NUM_3];
		HeapTuple tuple;
		Datum result;
		
		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		
		values[0] = Int32GetDatum(status->shardstat[status->currIdx].shard);
		values[1] = Int64GetDatum(status->shardstat[status->currIdx].count);
		values[2] = Int64GetDatum(status->shardstat[status->currIdx].size);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		status->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}
	pfree(status);
	SRF_RETURN_DONE(funcctx);
}

Datum
opentenbase_table_shard_stat_internal(PG_FUNCTION_ARGS)
{
#define  COLUMN_NUM_3 3
	
	FuncCallContext *funcctx;
	ShardStat_State *status;
	ListCell *lc = NULL;
	
	if (SRF_IS_FIRSTCALL())
	{
		Oid nsp;
		Oid relid = InvalidOid;
		List *relInfoList;
		List *search_path;
		char *table;
		text *tbl;
		float ratio = 1.0;
		int i;
		Relation rel = NULL;
		TupleDesc tupdesc;
		MemoryContext oldcontext;
		
		/* get table oid */
		if (PG_ARGISNULL(0))
		{
			elog(ERROR, "no table specified");
		}
		
		tbl = (text *) PG_GETARG_CSTRING(0);
		if (!tbl)
		{
			elog(ERROR, "no table specified");
		}
		
		table = text_to_cstring(tbl);
		relInfoList = stringToQualifiedNameList(table);
		switch (list_length(relInfoList))
		{
			case 1:
			{
				/* only relation */
				search_path = fetch_search_path(true);
				foreach(lc, search_path)
				{
					nsp = lfirst_oid(lc);
					if (OidIsValid(nsp))
					{
						relid = get_relname_relid(strVal(linitial(relInfoList)), nsp);
						if (OidIsValid(relid))
						{
							break;
						}
					}
				}
				list_free(search_path);
				break;
			}
			case 2:
			{
				/* schema.relation */
				nsp = get_namespace_oid(strVal(linitial(relInfoList)), false);
				relid = get_relname_relid(strVal(lsecond(relInfoList)), nsp);
				break;
			}
			case 3:
			{
				/* database.schema.relation */
				nsp = get_namespace_oid(strVal(lsecond(relInfoList)), false);
				relid = get_relname_relid(strVal(lthird(relInfoList)), nsp);
				break;
			}
			default:
			{
				ereport(ERROR,
				        (errcode(ERRCODE_UNDEFINED_OBJECT),
						        errmsg("table %s not exist", table)));
			}
			
		}
		/* free name list */
		pfree(table);
		list_free_deep(relInfoList);
		
		if (!OidIsValid(relid))
		{
			ereport(ERROR,
			        (errcode(ERRCODE_UNDEFINED_OBJECT),
					        errmsg("table %s not exist", table)));
		}
		
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		
		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(COLUMN_NUM_3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shard_id",
		                   INT4OID, -1, 0);
		
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ntups",
		                   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "size",
		                   INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		status = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
		funcctx->user_fctx = (void *) status;
		status->currIdx = 0;
		while (status->currIdx < shard_cluster_num)
		{
			status->shardstat[status->currIdx].shard = status->currIdx;
			status->currIdx++;
		}
		status->currIdx = 0;
		rel = heap_open(relid, AccessShareLock);
		if (RelationIsRowStore(rel))
		{
			/* Row storage sampling statistics. */
			if (!PG_ARGISNULL(1))
			{
				/* get sampling statistic ratio. */
				ratio = PG_GETARG_FLOAT4(1);
				if (ratio <= 1e-6)
				{
					ratio = 1e-6;
				}
				else if (ratio > 1.0)
				{
					ratio = 1.0;
				}
			}
			
			SampleRowRelationScStatistic(rel, status->shardstat, shard_cluster_num, &ratio);
			for (i = 0; i < shard_cluster_num; i++)
			{
				status->shardstat[i].count = (int64)((double) status->shardstat[i].count / ratio);
				status->shardstat[i].size = (int64)((double) status->shardstat[i].size / ratio);
			}
		}
		heap_close(rel, AccessShareLock);
		MemoryContextSwitchTo(oldcontext);
	}
	
	funcctx = SRF_PERCALL_SETUP();
	status = (ShardStat_State *) funcctx->user_fctx;
	
	while (status->currIdx < shard_cluster_num)
	{
		Datum values[COLUMN_NUM_3];
		bool nulls[COLUMN_NUM_3];
		HeapTuple tuple;
		Datum result;
		
		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		
		values[0] = Int32GetDatum(status->shardstat[status->currIdx].shard);
		values[1] = Int64GetDatum(status->shardstat[status->currIdx].count);
		values[2] = Int64GetDatum(status->shardstat[status->currIdx].size);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		status->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}
	pfree(status);
	SRF_RETURN_DONE(funcctx);
}

Datum
pg_stat_table_shard(PG_FUNCTION_ARGS)
{
#define  COLUMN_NUM 2

	FuncCallContext *funcctx;
	ShardStat_State *status;
	ListCell *lc   		 = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		Oid    nsp;
		Oid    rel = InvalidOid;
		List   *relInfoList;
		List   *search_path;
		char   *table;
		text   *tbl;
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		
		/* get table oid */
		tbl = (text*)PG_GETARG_CSTRING(0);
		if (!tbl)
		{
			elog(ERROR, "no table specified");
		}

		table = text_to_cstring(tbl);
		relInfoList = stringToQualifiedNameList(table);
		switch (list_length(relInfoList))
		{
			case 1:
			{
				/* only relation */
				search_path = fetch_search_path(true);			
				foreach(lc, search_path)
				{
					nsp = lfirst_oid(lc);
					if (OidIsValid(nsp))			
					{
						rel = get_relname_relid(strVal(linitial(relInfoList)), nsp);
						if (OidIsValid(rel))
						{
							break;
						}
					}
				}						
				list_free(search_path);
				break;
			}
			case 2:
			{
				/* schema.relation */					
				nsp = get_namespace_oid(strVal(linitial(relInfoList)), false);	
				rel = get_relname_relid(strVal(lsecond(relInfoList)), nsp);
				break;
			}
			case 3:
			{
				/* database.schema.relation */							
				nsp = get_namespace_oid(strVal(lsecond(relInfoList)), false);
				rel = get_relname_relid(strVal(lthird(relInfoList)), nsp);
				break;
			}

			default:
			{
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("table %s not exist", table)));
			}

		}
		/* free name list */
		pfree(table);
		list_free_deep(relInfoList);

		if (!OidIsValid(rel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("table %s not exist", table)));
		}
		
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(COLUMN_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shardid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tuplecount",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		status = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
		funcctx->user_fctx = (void *) status;
		status->currIdx = 0;	
		
		StatShardRelation(rel, status->shardstat, SHARD_MAP_GROUP_NUM);		
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status  = (ShardStat_State *) funcctx->user_fctx;
	
	while (status->currIdx < SHARD_MAP_SHARD_NUM)
	{
	    Datum		values[COLUMN_NUM];
		bool		nulls[COLUMN_NUM];
		HeapTuple	tuple;
		Datum		result;
		
		if (!status->shardstat[status->currIdx].count)
		{
			status->currIdx++;
			continue;
		}
		
		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls,  0, sizeof(nulls));

		values[0] = Int32GetDatum(status->shardstat[status->currIdx].shard);
		values[1] = Int64GetDatum(status->shardstat[status->currIdx].count);
		
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		status->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
pg_stat_all_shard(PG_FUNCTION_ARGS)
{
#define  COLUMN_NUM 2

	FuncCallContext *funcctx;
	ShardStat_State *status;

	if (SRF_IS_FIRSTCALL())
	{		
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(COLUMN_NUM, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "shardid",
						   INT4OID, -1, 0);

		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "tuplecount",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		status = (ShardStat_State *) palloc0(sizeof(ShardStat_State));
		funcctx->user_fctx = (void *) status;
		status->currIdx = 0;	
		
		StatShardAllRelations(status->shardstat, SHARD_MAP_GROUP_NUM);	

		RefreshShardStatistic(status->shardstat);
			
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	status  = (ShardStat_State *) funcctx->user_fctx;
	
	while (status->currIdx < SHARD_MAP_SHARD_NUM)
	{
		Datum		values[COLUMN_NUM];
		bool		nulls[COLUMN_NUM];
		HeapTuple	tuple;
		Datum		result;
		
		if (!status->shardstat[status->currIdx].count)
		{
			status->currIdx++;
			continue;
		}
		
		/*
		 * Form tuple with appropriate data.
		 */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls,  0, sizeof(nulls));

		values[0] = Int32GetDatum(status->shardstat[status->currIdx].shard);
		values[1] = Int64GetDatum(status->shardstat[status->currIdx].count);
		
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		status->currIdx++;
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Estimate space needed for shard statistic hashtable 
 */
Size
ShardStatisticShmemSize(void)
{
	Size space = 0;
	int nelems = MAX_SHARDS;
	int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
	Size pool_size = mul_size(nelems, sizeof(ShardStatistic));

	space = mul_size(npools, pool_size);
	
	return space;
}

/*
 * Initialize shard statistic info in shared memory
 */
void
ShardStatisticShmemInit(void)
{
	bool found;
	int nelems = MAX_SHARDS;
	int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
	Size pool_size = mul_size(nelems, sizeof(ShardStatistic));

	
	shardStatInfo = (ShardStatistic *)
		ShmemInitStruct("Shard Statistic Info",
						mul_size(npools, pool_size),
						&found);


	if (!found)
	{
		int i = 0;
		int max_elems = nelems * npools;

		for (i = 0; i < max_elems; i++)
		{
			pg_atomic_init_u64(&shardStatInfo[i].ntuples_select, 0);
			pg_atomic_init_u64(&shardStatInfo[i].ntuples_insert, 0);
			pg_atomic_init_u64(&shardStatInfo[i].ntuples_update, 0);
			pg_atomic_init_u64(&shardStatInfo[i].ntuples_delete, 0);
			pg_atomic_init_u64(&shardStatInfo[i].ntuples, 0);
			pg_atomic_init_u64(&shardStatInfo[i].size, 0);
		}
	}
}


/*
 * update shard statistic info by each backend which do select/insert/update/delete.
 */
void
UpdateShardStatistic(CmdType cmd, ShardID sid, int64 new_size, int64 old_size)
{
	int index = 0;
	int nelems = MAX_SHARDS;
	int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;

	if (!ShardIDIsValid(sid))
		return;

	index = (MyProc->pgprocno % npools) * nelems + sid;
	
	switch(cmd)
	{
		case CMD_SELECT:
			{
				pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_select, 1);
			}
			break;
		case CMD_UPDATE:
			{
				pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_update, 1);

				if (new_size > old_size)
				{
					int64 size = new_size - old_size;

					pg_atomic_fetch_add_u64(&shardStatInfo[index].size, size);
				}
				else if (new_size < old_size)
				{
					int64 size = old_size - new_size;

					pg_atomic_fetch_sub_u64(&shardStatInfo[index].size, size);
				}
			}
			break;
		case CMD_INSERT:
			{
				pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_insert, 1);

				pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples, 1);

				pg_atomic_fetch_add_u64(&shardStatInfo[index].size, new_size);
			}
			break;
		case CMD_DELETE:
			{
				pg_atomic_fetch_add_u64(&shardStatInfo[index].ntuples_delete, 1);

				pg_atomic_fetch_sub_u64(&shardStatInfo[index].ntuples, 1);

				pg_atomic_fetch_sub_u64(&shardStatInfo[index].size, old_size);
			}
			break;
		default:
			elog(LOG, "Unsupported CmdType %d in UpdateShardStatistic", cmd);
			break;
	}
}

static void
InitShardStatistic(ShardStatistic *stat)
{
	pg_atomic_init_u64(&stat->ntuples, 0);
	pg_atomic_init_u64(&stat->ntuples_delete, 0);
	pg_atomic_init_u64(&stat->ntuples_update, 0);
	pg_atomic_init_u64(&stat->ntuples_insert, 0);
	pg_atomic_init_u64(&stat->ntuples_select, 0);
	pg_atomic_init_u64(&stat->size, 0);
}

/* fetch from shard statistic from source, add to dest */
static void
FetchAddShardStatistic(ShardStatistic *dest, ShardStatistic *src)
{
	uint64 result = 0;

	result = pg_atomic_read_u64(&src->ntuples);
	pg_atomic_fetch_add_u64(&dest->ntuples, result);

	result = pg_atomic_read_u64(&src->ntuples_delete);
	pg_atomic_fetch_add_u64(&dest->ntuples_delete, result);

	result = pg_atomic_read_u64(&src->ntuples_update);
	pg_atomic_fetch_add_u64(&dest->ntuples_update, result);

	result = pg_atomic_read_u64(&src->ntuples_insert);
	pg_atomic_fetch_add_u64(&dest->ntuples_insert, result);

	result = pg_atomic_read_u64(&src->ntuples_select);
	pg_atomic_fetch_add_u64(&dest->ntuples_select, result);

	result = pg_atomic_read_u64(&src->size);
	pg_atomic_fetch_add_u64(&dest->size, result);
}
/* write shard statistic info into file */
void
FlushShardStatistic(void)
{
	int i      = 0;
	int j      = 0;
	int fd     = 0;
	int ret    = 0;
	int nelems = MAX_SHARDS;
	int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
	int size   = sizeof(ShardRecord) * nelems;
	static ShardRecord rec[MAX_SHARDS];

	/* init all shard records */
	for (i = 0; i < nelems; i++)
	{
		InitShardStatistic(&rec[i].stat);
		INIT_CRC32C(rec[i].crc);
	}

	/* merge shard statistic */
	for (i = 0; i < npools; i++)
	{
		for (j = 0; j < nelems; j++)
		{
			int index = i * nelems + j;

			FetchAddShardStatistic(&rec[j].stat, &shardStatInfo[index]);
		}
	}

	/* calculate crc for each record */
	for (i = 0; i < nelems; i++)
	{
		COMP_CRC32C(rec[i].crc, (char *)&rec[i].stat, sizeof(ShardStatistic));
		FIN_CRC32C(rec[i].crc);
	}

	/* write record into file */
	fd = open(SHARD_STATISTIC_FILE_PATH, O_RDWR | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);

	if (fd == -1)
	{
		return;
	}

	ret = write(fd, rec, size);
	if (ret != size)
	{
		close(fd);
		return;
	}

	close(fd);
}

void
RecoverShardStatistic(void)
{
	int i      = 0;
	int fd     = 0;
	int ret    = 0;
	int nelems = MAX_SHARDS;
	int size   = sizeof(ShardRecord) * nelems;
	ShardRecord *rec = (ShardRecord *)malloc(size);

	if(access(SHARD_STATISTIC_FILE_PATH, F_OK) == 0)
	{
		fd = open(SHARD_STATISTIC_FILE_PATH, O_RDONLY, S_IRUSR | S_IWUSR);
		if (fd == -1)
		{
			elog(LOG, "could not open file \"%s\"", SHARD_STATISTIC_FILE_PATH);
			goto end;
		}

		ret = read(fd, rec, size);
		if (ret != size)
		{
			elog(LOG, "failed to read file \"%s\"", SHARD_STATISTIC_FILE_PATH);
			close(fd);
			goto end;
		}

		close(fd);

		for (i = 0; i < nelems; i++)
		{
			pg_crc32c crc;

			INIT_CRC32C(crc);

			COMP_CRC32C(crc, (char *)&rec[i].stat, sizeof(ShardStatistic));
			FIN_CRC32C(crc);

			if (crc == rec[i].crc)
			{
				FetchAddShardStatistic(&shardStatInfo[i], &rec[i].stat);
			}
			else
			{
				elog(LOG, "shard %d statistic info CRC-ERROR", i);
			}
		}
	}

end:
	free(rec);
}

static void
RefreshShardStatistic(ShardStat *shardstat)
{
	int i = 0;
	int nelems = MAX_SHARDS;

	if (!shardstat)
	{
		return;
	}

	for (i = 0; i < nelems; i++)
	{
		if (shardstat[i].count)
		{
			pg_atomic_fetch_add_u64(&shardStatInfo[i].ntuples, shardstat[i].count);

			pg_atomic_fetch_add_u64(&shardStatInfo[i].size, shardstat[i].size);
		}
	}
}
void
ResetShardStatistic(void)
{
	int i = 0;
	int j = 0;
	int nelems = MAX_SHARDS;
	int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;

	for (i = 0; i < nelems; i++)
	{
		for (j = 0; j < npools; j++)
		{
			int index = j * nelems + i;

			pg_atomic_init_u64(&shardStatInfo[index].ntuples, 0);
			pg_atomic_init_u64(&shardStatInfo[index].size, 0);
		}
	}
}

/* display shard statistic info */
Datum  
opentenbase_shard_statistic(PG_FUNCTION_ARGS)
{
#define NCOLUMNS 9
	FuncCallContext 	*funcctx;
	ShmMgr_State	*status  = NULL;
	ShardStatistic *rec;
	int nelems = MAX_SHARDS;
	int npools = (MaxBackends / g_MaxSessionsPerPool) + 1;
	int size   = sizeof(ShardStatistic) * nelems;
	
	if (SRF_IS_FIRSTCALL())
	{		
		MemoryContext oldcontext;
		TupleDesc	  tupdesc;
		int i = 0;
		int j = 0;
		
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		tupdesc = CreateTemplateTupleDesc(NCOLUMNS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "group_name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "node_name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "shard_id",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "ntups_select",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "ntups_insert",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "ntups_update",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "ntups_delete",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "size",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "ntups",
						   INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

	    status = (ShmMgr_State *) palloc(sizeof(ShmMgr_State));
        funcctx->user_fctx = (void *) status;

        status->currIdx = 0;
		rec = (ShardStatistic *)palloc(size);

		for (i = 0; i < nelems; i++)
		{
			InitShardStatistic(&rec[i]);
		}

		LWLockAcquire(ShardMapLock, LW_SHARED);	
		for (i = 0; i < npools; i++)
		{
			for (j = 0; j < nelems; j++)
			{
	            if (show_all_shard_stat || IS_PGXC_COORDINATOR ||
	               !g_DatanodeShardgroupBitmap ||
	               g_GroupShardingMgr_DN->members->shardMapStatus != SHMEM_SHRADMAP_STATUS_USING ||
	               bms_is_member(j, g_DatanodeShardgroupBitmap))
            	{
                                   
					int index = i * nelems + j;

					FetchAddShardStatistic(&rec[j], &shardStatInfo[index]);
            	}
			}
		}
		LWLockRelease(ShardMapLock);

		status->rec = rec;
		MemoryContextSwitchTo(oldcontext);
	}


	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();
	status  = (ShmMgr_State *) funcctx->user_fctx;
	rec = status->rec;
	
	while (status->currIdx < nelems)
	{
        Datum		values[NCOLUMNS];
        bool		nulls[NCOLUMNS];
        HeapTuple	tuple;
        Datum		result;
		char        group_name[NAMEDATALEN] = {0};

        MemSet(values, 0, sizeof(values));
        MemSet(nulls,  0, sizeof(nulls));

		GetMyGroupName(group_name);

		if (group_name[0])
		{
			values[0] = CStringGetTextDatum(group_name);
		}
		else
		{
			nulls[0] = true;
		}
		values[1] = CStringGetTextDatum(PGXCNodeName);
		values[2] = Int32GetDatum(status->currIdx);
		values[3] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_select));
		values[4] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_insert));
		values[5] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_update));
		values[6] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples_delete));
		values[7] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].size));
		values[8] = Int64GetDatum(pg_atomic_read_u64(&rec[status->currIdx].ntuples));

		status->currIdx++;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}
	
	SRF_RETURN_DONE(funcctx);
}

/* find all involved shardids by given shardclusterid */
void
GetShardIdsByShardClusterId(Bitmapset *sc_bms, ShardID *shard_ids, int *nShards)
{
	ShardClusterId shardClusterId;
	Oid self_node_oid = InvalidOid;
	GroupShardInfo *members = NULL;
	bool needLock = false;
	int shard_num = 0;
	int idx = 0;
	
	Assert(IS_PGXC_DATANODE);
	
	self_node_oid = get_pgxc_nodeoid_extend(PGXCNodeName,
	                                        pgxc_plane_name(PGXCMainPlaneNameID));
	
	if (InvalidOid == self_node_oid)
	{
		elog(ERROR, "GetShardIdsByShardClusterId failed to get nodeoid, node:%s", PGXCNodeName);
	}
	
	/* Acquire lock if needed */
	needLock = g_GroupShardingMgr_DN->needLock;
	if (needLock)
	{
		LWLockAcquire(ShardMapLock, LW_SHARED);
	}
	
	members = g_GroupShardingMgr_DN->members;
	
	for (idx = 0; idx < members->shmemNumShards; idx++)
	{
		if (members->shmemshardmap[idx].primarycopy != self_node_oid)
			continue;
		
		shardClusterId = members->shmemshardmap[idx].shardclusterid;
		Assert(0 <= shardClusterId && shardClusterId < shard_cluster_num);
		
		if (bms_is_member(shardClusterId, sc_bms))
		{
			shard_ids[shard_num++] = members->shmemshardmap[idx].shardgroupid;
		}
	}
	
	if (needLock)
	{
		LWLockRelease(ShardMapLock);
	}
	
	*nShards = shard_num;
}

int GetNodeid(int shardid)
{
	return g_GroupShardingMgr_DN->members->shmemshardmap[shardid].nodeindex + 1;
}
