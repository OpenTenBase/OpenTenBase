/*-------------------------------------------------------------------------
 *
 * resgroup.h
 *	  resource group definitions.
 *
 * IDENTIFICATION
 *	    src/include/utils/resgroup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RES_GROUP_H
#define RES_GROUP_H

#include "catalog/pg_resgroup.h"

#define BITS_IN_KB 10

extern bool enable_resource_group;
extern bool ResGroupActivated;

#define IsResGroupActivated() \
	(ResGroupActivated)

/*
 * The max number of resource groups.
 */
#define MaxResourceGroups 100

/*
 * The max length of cpuset
 */
#define MaxCpuSetLength 1024

/*
 * Default value of cpuset
 */
#define DefaultCpuset "-1"

/*
 * When setting memory_spill_ratio to 0 the statement_mem will be used to
 * decide the operator memory, this is called the fallback mode, the benefit is
 * statement_mem can be set in absolute values such as "128 MB" which is easier
 * to understand.
 */
#define RESGROUP_FALLBACK_MEMORY_SPILL_RATIO		(0)

/*
 * Resource group capability.
 */
typedef int32 ResGroupCap;

/*
 * Resource group capabilities.
 *
 * These are usually a snapshot of the pg_resgroupcapability table
 * for a resource group.
 *
 * The properties must be in the same order as ResGroupLimitType.
 *
 * This struct can also be converted to an array of ResGroupCap so the fields
 * can be accessed via index and iterated with loop.
 *
 *     ResGroupCaps caps;
 *     ResGroupCap *array = (ResGroupCap *) &caps;
 *     caps.concurrency.value = 1;
 *     array[RESGROUP_LIMIT_TYPE_CONCURRENCY] = 2;
 *     Assert(caps.concurrency.value == 2);
 */
typedef struct ResGroupCaps
{
	ResGroupCap		__unknown;			/* placeholder, do not use it */
	ResGroupCap		concurrency;
	ResGroupCap		cpuRateLimit;
	ResGroupCap		memLimit;
	char			cpuset[MaxCpuSetLength];
} ResGroupCaps;

/* Set 'cpuset' to an empty string, and reset all other fields to zero */
#define ClearResGroupCaps(caps) \
	MemSet((caps), 0, offsetof(ResGroupCaps, cpuset) + 1)


/*
 * GUC variables.
 */
extern bool						resource_group_debug_wait_queue;

extern int resource_group_cpu_priority;
extern double resource_group_cpu_limit;
extern bool resource_group_cpu_ceiling_enforcement;
extern bool resource_group_bypass;
extern int resource_group_queuing_timeout;
extern char *resource_group_name;
extern char *resource_group_bypass_sql;
extern bool resource_group_local;
extern int resgroup_memory_query_fixed_mem;
extern char *resource_group_leader_coordinator;

/*
 * Non-GUC global variables.
 */
extern bool resource_group_enable_cgroup_cpuset;

/* Type of statistic information */
typedef enum
{
	RES_GROUP_STAT_UNKNOWN = -1,

	RES_GROUP_STAT_NRUNNING = 0,
	RES_GROUP_STAT_NQUEUEING,
	RES_GROUP_STAT_TOTAL_EXECUTED,
	RES_GROUP_STAT_TOTAL_QUEUED,
	RES_GROUP_STAT_TOTAL_QUEUE_TIME,
	RES_GROUP_STAT_CPU_USAGE,
} ResGroupStatType;

/*
 * The context to pass to callback in CREATE/ALTER/DROP resource group
 */
typedef struct
{
	Oid				groupid;
	NameData		groupname;
	ResGroupLimitType	limittype;
	ResGroupCaps	caps;
	ResGroupCaps	oldCaps;	/* last config value, alter operation need to
								 * check last config for recycling */
} ResourceGroupCallbackContext;

extern bool IsResGroupEnabled(void);

/* Shared memory and semaphores */
extern Size ResGroupShmemSize(void);
extern void ResGroupShmemInit(void);

/* Load resource group information from catalog */
extern void	InitResGroups(void);
extern void InitResGroupsIfEnabled(void);

extern void AllocResGroupEntry(Oid groupId, const ResGroupCaps *caps);

extern void SerializeResGroupInfo(StringInfo str);
extern void DeserializeResGroupInfo(struct ResGroupCaps *capsOut,
									char *groupName,
									const char *buf,
									int len);
extern void DNReceiveLocalResGroupInfo(StringInfo input_message);
extern void CNReceiveResGroupReqAndReply(StringInfo input_message);
extern void CNReceiveResGroupReply(void);
extern void AssignResGroupForNormalQuery(void);
extern bool ShouldAssignResGroup(void);
extern bool ShouldUnassignResGroup(void);
extern void AssignResGroup(void);
extern bool AssignResGroupForBypassQueryOrNot(void);
extern void UnassignResGroup(void);
extern void SwitchResGroupOnDN(const char *buf, int len);

extern bool ResGroupIsAssigned(void);

/* Retrieve statistic information of type from resource group */
extern Datum ResGroupGetStat(char *groupName, ResGroupStatType type);

extern void ResGroupDumpMemoryInfo(void);

extern void ResGroupDropFinish(const ResourceGroupCallbackContext *callbackCtx,
							   bool isCommit);
extern void ResGroupCreateOnAbort(const ResourceGroupCallbackContext *callbackCtx);
extern void ResGroupAlterOnCommit(const ResourceGroupCallbackContext *callbackCtx);
extern void ResGroupCheckForDrop(Oid groupId, char *name);

/*
 * Get resource group id of my proc.
 *
 * This function is not dead code although there is no consumer in the
 * code tree.  Some extensions require this to get the internal resource group
 * information.
 */
extern Oid GetMyResGroupId(void);
extern bool GetEnableResGroupOnParallelWorkerOnDN(void);
extern bool GetNonLeaderCNHasSetResgroup(void);

extern void ResGroupDumpInfo(StringInfo str);
extern void ResGroupCheckFreeSlots(StringInfo str);


extern Bitmapset *CpusetToBitset(const char *cpuset,
								 int len);
extern void BitsetToCpuset(const Bitmapset *bms,
							char *cpuset,
							int cpusetSize);
extern void CpusetUnion(char *cpuset1, const char *cpuset2, int len);
extern void CpusetDifference(char *cpuset1, const char *cpuset2, int len);
extern bool CpusetIsEmpty(const char *cpuset);
extern void SetCpusetEmpty(char *cpuset, int cpusetSize);
extern bool EnsureCpusetIsAvailable(int elevel);

/*
 * Calculate the amount of memory reserved for the query
 */
extern uint64 ResourceGroupGetQueryMemoryLimit(void);

extern void AssignResGroupOnParallelWorker(void);
extern bool resgroupCleanSlot(char *groupName);


#endif   /* RES_GROUP_H */
