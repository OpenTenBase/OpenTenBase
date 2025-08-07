/*-------------------------------------------------------------------------
 *
 * resgroup.c
 *	  resource group management code.
 *
 *
 * TERMS:
 *
 * - SLOT POOL: the global slot pool shared by all the resource groups.
 *   A resource group must acquire a free slot in this pool for a new
 *   transaction to run in it.
 *
 * IDENTIFICATION
 *	    src/backend/utils/resgroup/resgroup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "access/genam.h"
#include "tcop/tcopprot.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_resgroupcapability.h"
#include "commands/resgroupcmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resgroup-ops.h"
#include "utils/resgroup.h"
#include "pgxc/nodemgr.h"
#include "pgxc/execRemote.h"
#include "replication/walsender.h"
#include "regex/regex.h"
#include "optimizer/memctl.h"

bool enable_resource_group = false;
bool ResGroupActivated = false;
int resource_group_cpu_priority = 10;
double resource_group_cpu_limit = 0.9;
bool resource_group_cpu_ceiling_enforcement = false;
bool resource_group_bypass = false;
int resource_group_queuing_timeout = 0;
char *resource_group_name = "opentenbase";
char *resource_group_bypass_sql = NULL;
bool resource_group_local = false;
int resgroup_memory_query_fixed_mem = 0;
char *resource_group_leader_coordinator = NULL;

#define InvalidSlotId	(-1)
#define RESGROUP_MAX_SLOTS	(MaxConnections)

/*
 * GUC variables.
 */
bool						resource_group_debug_wait_queue = true;

/*
 * Data structures
 */

typedef struct ResGroupInfo				ResGroupInfo;
typedef struct ResGroupHashEntry		ResGroupHashEntry;
typedef struct ResGroupProcData			ResGroupProcData;
typedef struct ResGroupSlotData			ResGroupSlotData;
typedef struct ResGroupData				ResGroupData;
typedef struct ResGroupControl			ResGroupControl;

/*
 * Resource group info.
 *
 * This records the group and groupId for a transaction.
 * When group->groupId != groupId, it means the group
 * has been dropped.
 */
struct ResGroupInfo
{
	ResGroupData	*group;
	Oid				groupId;
	NameData		groupName;
};

struct ResGroupHashEntry
{
	Oid		groupId;
	int		index;
};

/*
 * Per proc resource group information.
 *
 * Config snapshot and runtime accounting information in current proc.
 */
struct ResGroupProcData
{
	Oid		groupId;

	ResGroupData		*group;
	ResGroupSlotData	*slot;

	ResGroupCaps	caps;
};

/*
 * Per slot resource group information.
 *
 * Resource group have 'concurrency' number of slots.
 * Each transaction acquires a slot on CN before running.
 * The information shared by processes on each DN are stored
 * in this structure.
 */
#define MaxResGroupProcsPerSlot DEFAULT_MAX_HANDLES
struct ResGroupSlotData
{
	Oid				groupId;
	NameData		groupName;	/* only used for bypass mode */
	ResGroupData	*group;		/* pointer to the group */

	int				nProcs;		/* number of procs in this slot */

	ResGroupSlotData	*next;

	ResGroupCaps	caps;

	int				procs[MaxResGroupProcsPerSlot];
};

/*
 * Resource group information.
 */
struct ResGroupData
{
	Oid			groupId;		/* Id for this group */

	volatile int			nRunning;		/* number of running trans */
	volatile int	nRunningBypassed;		/* number of running trans in bypass mode */
	int			totalExecuted;	/* total number of executed trans */
	int			totalQueued;	/* total number of queued trans	*/
	int64	totalQueuedTimeMs;	/* total queue time, in milliseconds */
	PROC_QUEUE	waitProcsH;		/* list of PGPROC objects waiting on this group with high priority */
	PROC_QUEUE	waitProcsM;		/* list of PGPROC objects waiting on this group with medium priority */
	PROC_QUEUE	waitProcsL;		/* list of PGPROC objects waiting on this group with low priority */

	bool		lockedForDrop;  /* true if resource group is dropped but not committed yet */

	ResGroupCaps	caps;		/* capabilities of this group */
};

struct ResGroupControl
{
	ResGroupSlotData	*slots;		/* slot pool shared by all resource groups */
	ResGroupSlotData	*freeSlot;	/* head of the free list */

	HTAB			*htbl;

	/*
	 * The hash table for resource groups in shared memory should only be populated
	 * once, so we add a flag here to implement this requirement.
	 */
	bool			loaded;

	int				nGroups;
	ResGroupData	groups[1];
};

bool resource_group_enable_cgroup_cpuset = false;

/* static variables */

static ResGroupControl *pResGroupControl = NULL;

static ResGroupProcData __self =
{
	InvalidOid,
};
static ResGroupProcData *self = &__self;
static bool enable_resgroup_on_parallel_workers_on_dn = false;
static bool non_leader_cn_has_set_resgroup = false;

/* If we are waiting on a group, this points to the associated group */
static ResGroupData *groupAwaited = NULL;
static PROC_QUEUE *groupWaitQueue = NULL;
static TimestampTz groupWaitStart;
static TimestampTz groupWaitEnd;

/* the resource group self is running in bypass mode */
static ResGroupData *bypassedGroup = NULL;
/* a fake slot used in bypass mode */
static ResGroupSlotData bypassedSlot;

/* static functions */
static void wakeupSlotsForWaitQueue(ResGroupData *group, bool grant, PROC_QUEUE *waitQueue);
static void wakeupSlots(ResGroupData *group, bool grant);
static ResGroupData *groupHashNew(Oid groupId);
static ResGroupData *groupHashFind(Oid groupId, bool raise);
static ResGroupData *groupHashRemove(Oid groupId);
static void waitOnGroup(ResGroupData *group, PROC_QUEUE *waitQueue);
static ResGroupData *createGroup(Oid groupId, const ResGroupCaps *caps);
static void removeGroup(Oid groupId);
static void AtProcExit_ResGroup(int code, Datum arg);
static void groupWaitCancel(void);
static void initSlot(ResGroupSlotData *slot, ResGroupData *group);
static void selfAttachResGroup(ResGroupData *group, ResGroupSlotData *slot);
static void selfDetachResGroup(ResGroupData *group, ResGroupSlotData *slot);
static bool slotpoolInit(void);
static ResGroupSlotData *slotpoolAllocSlot(void);
static void slotpoolFreeSlot(ResGroupSlotData *slot);
static ResGroupSlotData *groupGetSlot(ResGroupData *group);
static void groupPutSlot(ResGroupData *group, ResGroupSlotData *slot);
static char *decideResGroupName(void);
static void decideResGroup(ResGroupInfo *pGroupInfo);
static bool groupIncBypassedRef(ResGroupInfo *pGroupInfo);
static void groupDecBypassedRef(ResGroupData *group);
static PROC_QUEUE *chooseWaitQueue(ResGroupData *group, char *priority);
static ResGroupSlotData *groupAcquireSlot(ResGroupInfo *pGroupInfo);
static void groupReleaseSlot(ResGroupData *group, ResGroupSlotData *slot);
static void addTotalQueueDuration(ResGroupData *group);
static void selfValidateResGroupInfo(void);
static bool selfIsAssigned(void);
static void selfSetGroup(ResGroupData *group);
static void selfUnsetGroup(void);
static void selfSetSlot(ResGroupSlotData *slot);
static void selfUnsetSlot(void);
static bool procIsWaiting(const PGPROC *proc);
static void procWakeup(PGPROC *proc);
static int slotGetId(const ResGroupSlotData *slot);
static void groupWaitQueueValidate(const PROC_QUEUE *waitQueue);
static void groupWaitProcValidate(PGPROC *proc, PROC_QUEUE *head);
static void groupWaitQueuePush(PROC_QUEUE *waitQueue, PGPROC *proc);
static PGPROC *groupWaitQueuePop(PROC_QUEUE *waitQueue);
static void groupWaitQueueErase(PROC_QUEUE *waitQueue, PGPROC *proc);
static bool groupWaitQueueIsEmpty(const PROC_QUEUE *waitQueue);
static bool checkSQLBypass(const char *sql);
static bool shouldBypassQuery(const char *query_string);
static void lockResGroupForDrop(ResGroupData *group);
static void unlockResGroupForDrop(ResGroupData *group);
static bool groupIsDropped(ResGroupInfo *pGroupInfo);

static void resgroupDumpGroup(StringInfo str, ResGroupData *group);
static void resgroupDumpWaitQueue(StringInfo str, PROC_QUEUE *queue);
static void resgroupDumpCaps(StringInfo str, ResGroupCap *caps);
static void resgroupDumpSlots(StringInfo str);
static void resgroupDumpFreeSlots(StringInfo str);

static void cpusetOperation(char *cpuset1,
							const char *cpuset2,
							int len,
							bool sub);

#ifdef USE_ASSERT_CHECKING
static bool selfHasGroup(void);
static bool selfHasSlot(void);
static void slotValidate(const ResGroupSlotData *slot);
static bool slotIsInFreelist(const ResGroupSlotData *slot);
static bool slotIsInUse(const ResGroupSlotData *slot);
static bool groupIsNotDropped(const ResGroupData *group);
static bool groupWaitQueueFind(PROC_QUEUE *waitQueue, const PGPROC *proc);
#endif /* USE_ASSERT_CHECKING */

bool IsResGroupEnabled(void)
{
	if (enable_resource_group &&
		(IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) &&
//		IsNormalProcessingMode() &&
		IsPostmasterEnvironment &&
		IsUnderPostmaster &&
		IsBackendPostgres &&
		!am_walsender &&
		!am_db_walsender)
	{
		return true;
	}

	return false;
}

/*
 * Estimate size the resource group structures will need in
 * shared memory.
 */
Size
ResGroupShmemSize(void)
{
	Size		size = 0;

	/* The hash of groups. */
	size = hash_estimate_size(MaxResourceGroups, sizeof(ResGroupHashEntry));

	/* The control structure. */
	size = add_size(size, sizeof(ResGroupControl) - sizeof(ResGroupData));

	/* The control structure. */
	size = add_size(size, mul_size(MaxResourceGroups, sizeof(ResGroupData)));

	/* The slot pool. */
	size = add_size(size, mul_size(RESGROUP_MAX_SLOTS, sizeof(ResGroupSlotData)));

	/* Add a safety margin */
	size = add_size(size, size / 10);

	return size;
}

/*
 * Initialize the global ResGroupControl struct of resource groups.
 */
void
ResGroupShmemInit(void)
{
	int			i;
	bool		found;
	HASHCTL		info;
	int			hash_flags;
	int			size;

	size = sizeof(*pResGroupControl) - sizeof(ResGroupData);
	size += mul_size(MaxResourceGroups, sizeof(ResGroupData));

	pResGroupControl = ShmemInitStruct("global resource group control",
									   size, &found);
	if (found)
		return;
	if (pResGroupControl == NULL)
		goto error_out;

	/* Set key and entry sizes of hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ResGroupHashEntry);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	pResGroupControl->htbl = ShmemInitHash("Resource Group Hash Table",
										   MaxResourceGroups,
										   MaxResourceGroups,
										   &info, hash_flags);

	if (!pResGroupControl->htbl)
		goto error_out;

	/*
	 * No need to acquire LWLock here, since this is expected to be called by
	 * postmaster only
	 */
	pResGroupControl->loaded = false;
	pResGroupControl->nGroups = MaxResourceGroups;

	for (i = 0; i < MaxResourceGroups; i++)
		pResGroupControl->groups[i].groupId = InvalidOid;

	if (!slotpoolInit())
		goto error_out;

	return;

error_out:
	ereport(FATAL,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("not enough shared memory for resource group control")));
}

/*
 * Allocate a resource group entry from a hash table
 */
void
AllocResGroupEntry(Oid groupId, const ResGroupCaps *caps)
{
	ResGroupData	*group;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = createGroup(groupId, caps);
	Assert(group != NULL);
	if (group == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("failed to alloc resource group entry from a hash table")));
	}

	LWLockRelease(ResGroupLock);
}

/*
 * Load the resource groups in shared memory. Note this
 * can only be done after enough setup has been done. This uses
 * heap_open etc which in turn requires shared memory to be set up.
 */
void
InitResGroups(void)
{
	HeapTuple	tuple;
	SysScanDesc	sscan;
	int			numGroups;
	ResGroupCaps		caps;
	Relation			relResGroup;
	Relation			relResGroupCapability;
	char		cpuset[MaxCpuSetLength] = {0};
	int			defaultCore = -1;
	Bitmapset	*bmsUnused = NULL;
	ResGroupCap cpu_quota_sum = 0;

	on_shmem_exit(AtProcExit_ResGroup, 0);

	/*
	 * On CN and DN, the first backend does the initialization.
	 */
	if (pResGroupControl->loaded)
		return;

	/*
	 * The resgroup shared mem initialization must be serialized. Only the first session
	 * should do the init.
	 * Serialization is done by LW_EXCLUSIVE ResGroupLock. However, we must obtain all DB
	 * locks before obtaining LWlock to prevent deadlock.
	 */
	relResGroup = relation_open(ResGroupRelationId, AccessShareLock);
	relResGroupCapability = relation_open(ResGroupCapabilityRelationId, AccessShareLock);
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	if (pResGroupControl->loaded)
		goto exit;

	ResGroupOps_Init();

	if (resource_group_enable_cgroup_cpuset)
	{
		/* Get cpuset from cpuset/opentenbasedb, and transform it into bitset */
		ResGroupOps_GetCpuSet(RESGROUP_ROOT_PATH, cpuset, MaxCpuSetLength);
		bmsUnused = CpusetToBitset(cpuset, MaxCpuSetLength);
		/* get the minimum core number, in case of the zero core is not exist */
		defaultCore = bms_next_member(bmsUnused, -1);
		Assert(defaultCore >= 0);
	}

	numGroups = 0;
	sscan = systable_beginscan(relResGroup, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		Oid			groupId = HeapTupleGetOid(tuple);
		ResGroupData	*group;
		char			*groupName;
		bool			isnull;

		GetResGroupCapabilities(relResGroupCapability, groupId, &caps);

		group = createGroup(groupId, &caps);
		Assert(group != NULL);
		if (group == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("failed to create group during initialization of resource group")));
		}

		groupName = NameStr(*DatumGetName(heap_getattr(tuple,
													   Anum_pg_resgroup_rsgname,
													   relResGroup->rd_att,
													   &isnull)));
		ResGroupOps_CreateGroup(groupName);

		if (caps.cpuRateLimit != CPU_RATE_LIMIT_DISABLED)
		{
			cpu_quota_sum += caps.cpuRateLimit;
			ResGroupOps_SetCpuRateLimit(groupName, caps.cpuRateLimit);
		}
		else
		{
			Bitmapset *bmsCurrent = CpusetToBitset(caps.cpuset,
												   MaxCpuSetLength);
			Bitmapset *bmsCommon = bms_intersect(bmsCurrent, bmsUnused);
			Bitmapset *bmsMissing = bms_difference(bmsCurrent, bmsCommon);

			/*
			 * Do not call EnsureCpusetIsAvailable() here as resource group is
			 * not activated yet
			 */
			if (!resource_group_enable_cgroup_cpuset)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cgroup is not properly configured to use the cpuset feature"),
						 errhint("Extra cgroup configurations are required to enable this feature, "
								 "please refer to the Greenplum Documentations for details")));
			}

			Assert(caps.cpuRateLimit == CPU_RATE_LIMIT_DISABLED);

			if (bms_is_empty(bmsMissing))
			{
				/*
				 * write cpus to corresponding file
				 * if all the cores are available
				 */
				ResGroupOps_SetCpuSet(groupName, caps.cpuset);
				bmsUnused = bms_del_members(bmsUnused, bmsCurrent);
			}
			else
			{
				char		cpusetMissing[MaxCpuSetLength] = {0};

				/*
				 * if some of the cores are unavailable, just set defaultCore
				 * to this group and send a warning message, so the system
				 * can startup, then DBA can fix it
				 */
				snprintf(cpuset, MaxCpuSetLength, "%d", defaultCore);
				ResGroupOps_SetCpuSet(groupName, cpuset);
				BitsetToCpuset(bmsMissing, cpusetMissing, MaxCpuSetLength);
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cpu cores %s are unavailable on the system "
								"in resource group %s",
								cpusetMissing, groupName),
						 errhint("using core %d for this resource group, "
								 "please adjust the settings and restart",
								 defaultCore)));
			}
		}

		numGroups++;
		Assert(numGroups <= MaxResourceGroups);
	}
	systable_endscan(sscan);

	if (resource_group_enable_cgroup_cpuset)
	{
		/*
		 * set default cpuset
		 */

		if (bms_is_empty(bmsUnused))
		{
			/* no unused core, assign default core to default group */
			snprintf(cpuset, MaxCpuSetLength, "%d", defaultCore);
		}
		else
		{
			/* assign all unused cores to default group */
			BitsetToCpuset(bmsUnused, cpuset, MaxCpuSetLength);
		}

		Assert(cpuset[0]);
		Assert(!CpusetIsEmpty(cpuset));

		ResGroupOps_SetCpuSet(DEFAULT_CPUSET_GROUP_PATH, cpuset);
	}

	if (resource_group_cpu_ceiling_enforcement)
	{
		if (cpu_quota_sum > resource_group_cpu_limit * 100) {
			float require = cpu_quota_sum / 100.0;
			elog(ERROR, "resource_group_cpu_limit is too small. It should not be smaller than %.2f. "
						"Because the existing resource groups require this much.", require);
		}
	}

	pResGroupControl->loaded = true;

exit:
	LWLockRelease(ResGroupLock);

	/*
	 * release lock here to guarantee we have no lock held when acquiring
	 * resource group slot
	 */
	relation_close(relResGroup, AccessShareLock);
	relation_close(relResGroupCapability, AccessShareLock);
}

void
InitResGroupsIfEnabled()
{
	if (IsResGroupEnabled())
	{
		InitResGroups();
		ResGroupActivated = true;
	}
}

/*
 * Check resource group status when DROP RESOURCE GROUP
 *
 * Errors out if there're running transactions, otherwise lock the resource group.
 * New transactions will be queued if the resource group is locked.
 */
void
ResGroupCheckForDrop(Oid groupId, char *name)
{
	ResGroupData	*group;

	if (!IS_PGXC_COORDINATOR)
		return;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	group = groupHashFind(groupId, true);

	if (group->nRunning + group->nRunningBypassed > 0)
	{
		int nQuery = group->nRunning + group->nRunningBypassed + group->waitProcsH.size + group->waitProcsM.size + group->waitProcsL.size;

		Assert(name != NULL);
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("cannot drop resource group \"%s\"", name),
				 errhint(" The resource group is currently managing %d query(ies) and cannot be dropped.\n"
						 "\tTerminate the queries first or try dropping the group later.\n"
						 "\tThe view pg_stat_activity tracks the queries managed by resource groups.", nQuery)));
	}

	lockResGroupForDrop(group);

	LWLockRelease(ResGroupLock);
}

/*
 * Drop resource group call back function
 *
 * Wake up the backends in the wait queue when DROP RESOURCE GROUP finishes.
 * Unlock the resource group if the transaction is aborted.
 * Remove the resource group entry in shared memory if the transaction is committed.
 *
 * This function is called in the callback function of DROP RESOURCE GROUP.
 */
void
ResGroupDropFinish(const ResourceGroupCallbackContext *callbackCtx,
				   bool isCommit)
{
	ResGroupData	*group;
	volatile int	savedInterruptHoldoffCount;
	MemoryContext   ctx = CurrentMemoryContext;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		savedInterruptHoldoffCount = InterruptHoldoffCount;

		group = groupHashFind(callbackCtx->groupid, true);

		if (IS_PGXC_COORDINATOR)
		{
			wakeupSlots(group, false);
			unlockResGroupForDrop(group);
		}

		if (isCommit)
		{
			removeGroup(callbackCtx->groupid);
			if (!CpusetIsEmpty(group->caps.cpuset))
			{
				if (resource_group_enable_cgroup_cpuset)
				{
					/* reset default group, add cpu cores to it */
					char cpuset[MaxCpuSetLength];
					ResGroupOps_GetCpuSet(DEFAULT_CPUSET_GROUP_PATH,
										  cpuset, MaxCpuSetLength);
					CpusetUnion(cpuset, group->caps.cpuset, MaxCpuSetLength);
					ResGroupOps_SetCpuSet(DEFAULT_CPUSET_GROUP_PATH, cpuset);
				}
			}

			ResGroupOps_DestroyGroup(NameStr(callbackCtx->groupname), true);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ctx);
		FlushErrorState();

		InterruptHoldoffCount = savedInterruptHoldoffCount;
//		if (elog_demote(WARNING))
//		{
//			EmitErrorReport();
//			FlushErrorState();
//		}
//		else
//		{
//			elog(LOG, "unable to demote error");
//		}
	}
	PG_END_TRY();

	LWLockRelease(ResGroupLock);
}


/*
 * Remove the resource group entry in shared memory if the transaction is aborted.
 *
 * This function is called in the callback function of CREATE RESOURCE GROUP.
 */
void
ResGroupCreateOnAbort(const ResourceGroupCallbackContext *callbackCtx)
{
	volatile int savedInterruptHoldoffCount;
	MemoryContext ctx = CurrentMemoryContext;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		savedInterruptHoldoffCount = InterruptHoldoffCount;
		removeGroup(callbackCtx->groupid);
		/* remove the os dependent part for this resource group */
		ResGroupOps_DestroyGroup(NameStr(callbackCtx->groupname), true);

		if (!CpusetIsEmpty(callbackCtx->caps.cpuset) &&
			resource_group_enable_cgroup_cpuset)
		{
			/* return cpu cores to default group */
			char defaultGroupCpuset[MaxCpuSetLength];
			ResGroupOps_GetCpuSet(DEFAULT_CPUSET_GROUP_PATH,
								  defaultGroupCpuset,
								  MaxCpuSetLength);
			CpusetUnion(defaultGroupCpuset,
						callbackCtx->caps.cpuset,
						MaxCpuSetLength);
			ResGroupOps_SetCpuSet(DEFAULT_CPUSET_GROUP_PATH, defaultGroupCpuset);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ctx);
		FlushErrorState();

		InterruptHoldoffCount = savedInterruptHoldoffCount;
//		if (elog_demote(WARNING))
//		{
//			EmitErrorReport();
//			FlushErrorState();
//		}
//		else
//		{
//			elog(LOG, "unable to demote error");
//		}
	}
	PG_END_TRY();
	LWLockRelease(ResGroupLock);
}

/*
 * Apply the new resgroup caps.
 */
void
ResGroupAlterOnCommit(const ResourceGroupCallbackContext *callbackCtx)
{
	ResGroupData	*group;
	volatile int	savedInterruptHoldoffCount;
	MemoryContext   ctx = CurrentMemoryContext;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	PG_TRY();
	{
		savedInterruptHoldoffCount = InterruptHoldoffCount;
		group = groupHashFind(callbackCtx->groupid, true);

		group->caps = callbackCtx->caps;

		if (callbackCtx->limittype == RESGROUP_LIMIT_TYPE_CPU)
		{
			ResGroupOps_SetCpuRateLimit(NameStr(callbackCtx->groupname),
										callbackCtx->caps.cpuRateLimit);
		}
		else if (callbackCtx->limittype == RESGROUP_LIMIT_TYPE_CPUSET)
		{
			if (resource_group_enable_cgroup_cpuset)
				ResGroupOps_SetCpuSet(NameStr(callbackCtx->groupname),
									  callbackCtx->caps.cpuset);
		}
		else if (callbackCtx->limittype == RESGROUP_LIMIT_TYPE_CONCURRENCY)
		{
			wakeupSlots(group, true);
		}

		/* reset default group if cpuset has changed */
		if (strcmp(callbackCtx->oldCaps.cpuset, callbackCtx->caps.cpuset) &&
			resource_group_enable_cgroup_cpuset)
		{
			char defaultCpusetGroup[MaxCpuSetLength];
			/* get current default group value */
			ResGroupOps_GetCpuSet(DEFAULT_CPUSET_GROUP_PATH,
								  defaultCpusetGroup,
								  MaxCpuSetLength);
			/* Add old value to default group
			 * sub new value from default group */
			CpusetUnion(defaultCpusetGroup,
							callbackCtx->oldCaps.cpuset,
							MaxCpuSetLength);
			CpusetDifference(defaultCpusetGroup,
							callbackCtx->caps.cpuset,
							MaxCpuSetLength);
			ResGroupOps_SetCpuSet(DEFAULT_CPUSET_GROUP_PATH, defaultCpusetGroup);
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(ctx);
		FlushErrorState();

		InterruptHoldoffCount = savedInterruptHoldoffCount;
//		if (elog_demote(WARNING))
//		{
//			EmitErrorReport();
//			FlushErrorState();
//		}
//		else
//		{
//			elog(LOG, "unable to demote error");
//		}
	}
	PG_END_TRY();

	LWLockRelease(ResGroupLock);
}

bool
ResGroupIsAssigned(void)
{
	return selfIsAssigned();
}

/*
 * Get resource group id of my proc.
 *
 * Returns InvalidOid in any of below cases:
 * - resource group is not enabled;
 * - resource group is not activated (initialized);
 * - my proc is not running inside a transaction;
 * - my proc is not assigned a resource group yet;
 *
 * Otherwise a valid resource group id is returned.
 *
 * This function is not dead code although there is no consumer in the
 * code tree.  Some extensions require this to get the internal resource group
 * information.
 */
Oid
GetMyResGroupId(void)
{
	return self->groupId;
}

bool
GetEnableResGroupOnParallelWorkerOnDN(void) {
	return enable_resgroup_on_parallel_workers_on_dn;
}

bool
GetNonLeaderCNHasSetResgroup(void)
{
	return non_leader_cn_has_set_resgroup;
}

/*
 *  Retrieve statistic information of type from resource group
 */
Datum
ResGroupGetStat(char *groupName, ResGroupStatType type)
{
	Oid			groupId;
	ResGroupData *group;
	Datum		result;

	Assert(IsResGroupActivated());

	LWLockAcquire(ResGroupLock, LW_SHARED);

	groupId = get_resgroup_oid(groupName, false);
	group = groupHashFind(groupId, true);

	switch (type)
	{
		case RES_GROUP_STAT_NRUNNING:
			result = Int32GetDatum(group->nRunning + group->nRunningBypassed);
			break;
		case RES_GROUP_STAT_NQUEUEING:
			result = Int32GetDatum(group->waitProcsH.size) + Int32GetDatum(group->waitProcsM.size) + Int32GetDatum(group->waitProcsL.size);
			break;
		case RES_GROUP_STAT_TOTAL_EXECUTED:
			result = Int32GetDatum(group->totalExecuted);
			break;
		case RES_GROUP_STAT_TOTAL_QUEUED:
			result = Int32GetDatum(group->totalQueued);
			break;
		case RES_GROUP_STAT_TOTAL_QUEUE_TIME:
			result = Int64GetDatum(group->totalQueuedTimeMs);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid stat type %d", type)));
	}

	LWLockRelease(ResGroupLock);

	return result;
}

/*
 * removeGroup -- remove resource group from share memory and
 * reclaim the group's memory back to MEM POOL.
 */
static void
removeGroup(Oid groupId)
{
	ResGroupData *group;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(OidIsValid(groupId));

	group = groupHashRemove(groupId);

	group->groupId = InvalidOid;
}

/*
 * createGroup -- initialize the elements for a resource group.
 *
 * Notes:
 *	It is expected that the appropriate lightweight lock is held before
 *	calling this - unless we are the startup process.
 */
static ResGroupData *
createGroup(Oid groupId, const ResGroupCaps *caps)
{
	ResGroupData	*group;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(OidIsValid(groupId));

	group = groupHashNew(groupId);
	Assert(group != NULL);

	group->groupId = groupId;
	group->caps = *caps;
	group->nRunning = 0;
	group->nRunningBypassed = 0;
	ProcQueueInit(&group->waitProcsH);
	ProcQueueInit(&group->waitProcsM);
	ProcQueueInit(&group->waitProcsL);
	group->totalExecuted = 0;
	group->totalQueued = 0;
	group->totalQueuedTimeMs = 0;
	group->lockedForDrop = false;

	return group;
}

/*
 * Attach a process to a slot.
 */
static void
selfAttachResGroup(ResGroupData *group, ResGroupSlotData *slot)
{
	selfSetGroup(group);
	selfSetSlot(slot);

	if (slot->nProcs < MaxResGroupProcsPerSlot)
		slot->procs[slot->nProcs] = MyProcPid;
	pg_atomic_add_fetch_u32((pg_atomic_uint32*) &slot->nProcs, 1);
}


/*
 * Detach a process from a slot.
 */
static void
selfDetachResGroup(ResGroupData *group, ResGroupSlotData *slot)
{
	pg_atomic_sub_fetch_u32((pg_atomic_uint32*) &slot->nProcs, 1);
	if (slot->nProcs < MaxResGroupProcsPerSlot)
		slot->procs[slot->nProcs] = 0;
	selfUnsetSlot();
	selfUnsetGroup();
}

/*
 * Initialize the members of a slot
 */
static void
initSlot(ResGroupSlotData *slot, ResGroupData *group)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(!slotIsInUse(slot));
	Assert(group->groupId != InvalidOid);

	slot->group = group;
	slot->groupId = group->groupId;
	slot->caps = group->caps;
}

/*
 * Alloc and initialize slot pool
 */
static bool
slotpoolInit(void)
{
	ResGroupSlotData *slot;
	ResGroupSlotData *next;
	int numSlots;
	int memSize;
	int i;

	numSlots = RESGROUP_MAX_SLOTS;
	memSize = mul_size(numSlots, sizeof(ResGroupSlotData));

	pResGroupControl->slots = ShmemAlloc(memSize);
	if (!pResGroupControl->slots)
		return false;

	MemSet(pResGroupControl->slots, 0, memSize);

	/* push all the slots into the list */
	next = NULL;
	for (i = numSlots - 1; i >= 0; i--)
	{
		slot = &pResGroupControl->slots[i];

		slot->group = NULL;
		slot->groupId = InvalidOid;
		memset(slot->procs, 0, MaxResGroupProcsPerSlot * sizeof(int));

		slot->next = next;
		next = slot;
	}
	pResGroupControl->freeSlot = next;

	return true;
}

/*
 * Alloc a slot from shared slot pool
 */
static ResGroupSlotData *
slotpoolAllocSlot(void)
{
	ResGroupSlotData *slot;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(pResGroupControl->freeSlot != NULL);

	if (pResGroupControl->freeSlot == NULL)
		elog(ERROR, "No free slots available");

	slot = pResGroupControl->freeSlot;
	pResGroupControl->freeSlot = slot->next;

	elog(DEBUG5, "alloc slot %d, next free slot %d",
			(int) (slot - pResGroupControl->slots),
			(int) (slot->next - pResGroupControl->slots));
	return slot;
}

/*
 * Free a slot back to shared slot pool
 */
static void
slotpoolFreeSlot(ResGroupSlotData *slot)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(slotIsInUse(slot));
	Assert(slot->nProcs == 0);

	slot->group = NULL;
	slot->groupId = InvalidOid;
	memset(slot->procs, 0, MaxResGroupProcsPerSlot * sizeof(int));

	slot->next = pResGroupControl->freeSlot;
	pResGroupControl->freeSlot = slot;

	elog(DEBUG5, "free slot %d, next free slot %d",
			(int) (slot - pResGroupControl->slots),
			(int) (slot->next - pResGroupControl->slots));
}

/*
 * Get a slot with memory quota granted.
 *
 * A slot can be got with this function if there is enough memory quota
 * available and the concurrency limit is not reached.
 *
 * On success the memory quota is marked as granted, nRunning is increased
 * and the slot's groupId is also set accordingly, the slot id is returned.
 *
 * On failure nothing is changed and InvalidSlotId is returned.
 */
static ResGroupSlotData *
groupGetSlot(ResGroupData *group)
{
	ResGroupSlotData	*slot;
	ResGroupCaps		*caps;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(IS_PGXC_COORDINATOR);
	Assert(groupIsNotDropped(group));

	caps = &group->caps;

	/* First check if the concurrency limit is reached */
	if (group->nRunning >= caps->concurrency)
		return NULL;

	/* Now actually get a free slot */
	slot = slotpoolAllocSlot();
	Assert(!slotIsInUse(slot));

	initSlot(slot, group);

	group->nRunning++;

	return slot;
}

/*
 * Put back the slot assigned to self.
 *
 * This will release a slot, its memory used will be freed and
 * nRunning will be decreased.
 */
static void
groupPutSlot(ResGroupData *group, ResGroupSlotData *slot)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(slotIsInUse(slot));

	/* Return the slot back to free list */
	slotpoolFreeSlot(slot);
	group->nRunning--;

	/*
	 * Once we have waken up other groups then the slot we just released
	 * might be reused, so we should not touch it anymore since now.
	 */
}

/*
 * Pick a resource group for the current transaction.
 */
static char *
decideResGroupName(void)
{
	char *groupName = NULL;

	if (groupName == NULL)
		groupName = GetResGroupNameForRole(GetUserId());

	return groupName;
}

/*
 * Decide the proper resource group for current role.
 *
 * An exception is thrown if current role is invalid.
 */
static void
decideResGroup(ResGroupInfo *pGroupInfo)
{
	ResGroupData	*group;
	char			*groupName;
	Oid				groupId;

	Assert(pResGroupControl != NULL);

	/* always find out the up-to-date resgroup id */
	groupName = decideResGroupName(); 
	groupId = get_resgroup_oid(groupName, false);

	LWLockAcquire(ResGroupLock, LW_SHARED);
	group = groupHashFind(groupId, false);

	if (!group)
	{
		groupId = superuser() ? ADMINRESGROUP_OID : DEFAULTRESGROUP_OID;
		group = groupHashFind(groupId, false);
	}

	Assert(group != NULL);

	LWLockRelease(ResGroupLock);

	pGroupInfo->group = group;
	strcpy(NameStr(pGroupInfo->groupName), groupName);
	pGroupInfo->groupId = groupId;
}

/*
 * Increase the bypassed ref count
 *
 * Return true if the operation is done, or false if the group is dropped.
 */
static bool
groupIncBypassedRef(ResGroupInfo *pGroupInfo)
{
	ResGroupData	*group = pGroupInfo->group;
	bool			result = false;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	/* Has the group been dropped? */
	if (groupIsDropped(pGroupInfo))
		goto end;

	/* Is the group locked for drop? */
	if (group->lockedForDrop)
		goto end;

	result = true;
	pg_atomic_add_fetch_u32((pg_atomic_uint32 *) &group->nRunningBypassed, 1);

end:
	LWLockRelease(ResGroupLock);
	return result;
}

/*
 * Decrease the bypassed ref count
 */
static void
groupDecBypassedRef(ResGroupData *group)
{
	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *) &group->nRunningBypassed, 1);
}

/*
 * Choose the wait queue according to priority
 */
static PROC_QUEUE *
chooseWaitQueue(ResGroupData *group, char *priority)
{
	if (strcmp(priority, "high") == 0)
		return &group->waitProcsH;
	else if (strcmp(priority, "medium") == 0)
		return &group->waitProcsM;
	else if (strcmp(priority, "low") == 0)
		return &group->waitProcsL;
	else
		elog(ERROR, "unexpected role priority %s, should be one of \"high\", \"medium\" and \"low\"", priority);
}

/*
 * Acquire a resource group slot
 *
 * Call this function at the start of the transaction.
 * This function set current resource group in MyResGroupSharedInfo,
 * and current slot in MyProc->resSlot.
 */
static ResGroupSlotData *
groupAcquireSlot(ResGroupInfo *pGroupInfo)
{
	ResGroupSlotData *slot;
	ResGroupData	 *group;
	char			 *priority;
	PROC_QUEUE		 *waitQueue;

	group = pGroupInfo->group;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	/* Has the group been dropped? */
	if (groupIsDropped(pGroupInfo))
	{
		LWLockRelease(ResGroupLock);
		return NULL;
	}

	/* acquire a slot */
	if (!group->lockedForDrop)
	{
		/* try to get a slot directly */
		slot = groupGetSlot(group);

		if (slot != NULL)
		{
			/* got one, lucky */
			group->totalExecuted++;
			LWLockRelease(ResGroupLock);
			pgstat_report_resgroup(group->groupId);
			return slot;
		}
	}

	/*
	 * Add into group wait queue (if not waiting yet).
	 */
	Assert(!proc_exit_inprogress);
	priority = GetPriorityForRole(GetUserId());
	waitQueue = chooseWaitQueue(group, priority);
	groupWaitQueuePush(waitQueue, MyProc);

	if (!group->lockedForDrop)
		group->totalQueued++;
	LWLockRelease(ResGroupLock);

	/*
	 * To avoid excessive connections, release idle connections
	 * while the resource group is waiting
	 */
	if (!temp_object_included && PersistentConnectionsTimeout > 0)
		pgxc_node_cleanup_and_release_handles(false, true);

	/*
	 * wait on the queue
	 * slot will be assigned by the proc wakes me up
	 * if i am waken up by DROP RESOURCE GROUP statement, the
	 * resSlot will be NULL.
	 */
	waitOnGroup(group, waitQueue);

	if (MyProc->resSlot == NULL)
		return NULL;

	/*
	 * The waking process has granted us a valid slot.
	 * Update the statistic information of the resource group.
	 */
	slot = (ResGroupSlotData *) MyProc->resSlot;
	MyProc->resSlot = NULL;
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
	addTotalQueueDuration(group);
	group->totalExecuted++;
	LWLockRelease(ResGroupLock);

	pgstat_report_resgroup(group->groupId);
	return slot;
}

/*
 * Attempt to wake up pending slots in the specific priority queue.
 */
static void
wakeupSlotsForWaitQueue(ResGroupData *group, bool grant, PROC_QUEUE *waitQueue)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	while (!groupWaitQueueIsEmpty(waitQueue))
	{
		PGPROC *waitProc;
		ResGroupSlotData *slot = NULL;

		if (grant)
		{
			/* first try to get a slot for that proc with memory req zero */
			slot = groupGetSlot(group);
			if (slot == NULL)
				/* if can't get one then give up */
				break;
		}

		/* wake up one process in the wait queue */
		waitProc = groupWaitQueuePop(waitQueue);

		waitProc->resSlot = slot;

		procWakeup(waitProc);
	}
}

/*
 * Attempt to wake up pending slots in the group.
 *
 * - grant indicates whether to grant the proc a slot;
 * - release indicates whether to wake up the proc with the LWLock
 *   temporarily released;
 *
 * When grant is true we'll give up once no slot can be get,
 * e.g. due to lack of free slot or enough memory quota.
 *
 * When grant is false all the pending procs will be woken up.
 */
static void
wakeupSlots(ResGroupData *group, bool grant)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	wakeupSlotsForWaitQueue(group, grant, &group->waitProcsH);
	wakeupSlotsForWaitQueue(group, grant, &group->waitProcsM);
	wakeupSlotsForWaitQueue(group, grant, &group->waitProcsL);
}

/* Update the total queued time of this group */
static void
addTotalQueueDuration(ResGroupData *group)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	if (group == NULL)
		return;

	group->totalQueuedTimeMs += (groupWaitEnd - groupWaitStart);
}

/*
 * Release the resource group slot
 *
 * Call this function at the end of the transaction.
 */
static void
groupReleaseSlot(ResGroupData *group, ResGroupSlotData *slot)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	groupPutSlot(group, slot);

	/*
	 * We should wake up other pending queries on CN nodes.
	 */
	if (IS_ACCESS_NODE)
		/*
		 * My slot is put back, then how many queuing queries should I wake up?
		 * Maybe zero, maybe one, maybe more, depends on how the resgroup's
		 * configuration were changed during our execution.
		 */
		wakeupSlots(group, true);
}

/*
 * Serialize the resource group information that need to dispatch to segment.
 */
void
SerializeResGroupInfo(StringInfo str)
{
	Oid groupId = InvalidOid;
	char		*groupname;
	unsigned int groupname_len;
	unsigned int cpuset_len;
	int32		itmp;
	ResGroupCaps empty_caps;
	ResGroupInfo groupInfo;
	ResGroupCaps *caps;

	if (selfIsAssigned())
	{
		groupId = self->groupId;
		caps = &self->caps;
	}
	else if (non_leader_cn_has_set_resgroup)
	{
		decideResGroup(&groupInfo);
		groupId = groupInfo.groupId;
		caps = &groupInfo.group->caps;
	}
	else
	{
		ClearResGroupCaps(&empty_caps);
		caps = &empty_caps;
	}

	groupname = GetResGroupNameForId(groupId);
	groupname_len = strlen(groupname);
	itmp = htonl(groupname_len);
	appendBinaryStringInfo(str, (char *) &itmp, sizeof(int32));
	appendBinaryStringInfo(str, groupname, groupname_len);

	itmp = htonl(caps->concurrency);
	appendBinaryStringInfo(str, (char *) &itmp, sizeof(int32));
	itmp = htonl(caps->cpuRateLimit);
	appendBinaryStringInfo(str, (char *) &itmp, sizeof(int32));
	itmp = htonl(caps->memLimit);
	appendBinaryStringInfo(str, (char *) &itmp, sizeof(int32));

	cpuset_len = strlen(caps->cpuset);
	itmp = htonl(cpuset_len);
	appendBinaryStringInfo(str, (char *) &itmp, sizeof(int32));
	appendBinaryStringInfo(str, caps->cpuset, cpuset_len);

	groupname = NameStr(bypassedSlot.groupName);
	groupname_len = strlen(groupname);
	itmp = htonl(groupname_len);
	appendBinaryStringInfo(str, (char *) &itmp, sizeof(int32));
	appendBinaryStringInfo(str, groupname, groupname_len);
}

/*
 * Deserialize the resource group information dispatched by CN.
 */
void
DeserializeResGroupInfo(struct ResGroupCaps *capsOut,
						char *groupName,
						const char *buf,
						int len)
{
	int32		itmp;
	unsigned int groupname_len;
	unsigned int cpuset_len;
	unsigned int bypassgroupname_len;
	const char	*ptr = buf;

	Assert(len > 0);

	ClearResGroupCaps(capsOut);

	memcpy(&itmp, ptr, sizeof(int32)); ptr += sizeof(int32);
	groupname_len = ntohl(itmp);
	if (groupname_len >= NAMEDATALEN)
		elog(ERROR, "malformed serialized resource group info");
	memcpy(groupName, ptr, groupname_len); ptr += groupname_len;
	groupName[groupname_len] = '\0';

	memcpy(&itmp, ptr, sizeof(int32)); ptr += sizeof(int32);
	capsOut->concurrency = ntohl(itmp);
	memcpy(&itmp, ptr, sizeof(int32)); ptr += sizeof(int32);
	capsOut->cpuRateLimit = ntohl(itmp);
	memcpy(&itmp, ptr, sizeof(int32)); ptr += sizeof(int32);
	capsOut->memLimit = ntohl(itmp);

	memcpy(&itmp, ptr, sizeof(int32)); ptr += sizeof(int32);
	cpuset_len = ntohl(itmp);
	if (cpuset_len >= sizeof(capsOut->cpuset))
		elog(ERROR, "malformed serialized resource group info");
	memcpy(capsOut->cpuset, ptr, len); ptr += cpuset_len;
	capsOut->cpuset[cpuset_len] = '\0';

	memcpy(&itmp, ptr, sizeof(int32)); ptr += sizeof(int32);
	bypassgroupname_len = ntohl(itmp);
	if (bypassgroupname_len >= NAMEDATALEN)
		elog(ERROR, "malformed serialized resource group info");
	memcpy(NameStr(bypassedSlot.groupName), ptr, bypassgroupname_len); ptr += bypassgroupname_len;
	NameStr(bypassedSlot.groupName)[bypassgroupname_len] = '\0';

	Assert(len == ptr - buf);
}

/*
 * Handle resource group infomation from CN, and perform resource control at DN.
 */
void
DNReceiveLocalResGroupInfo(StringInfo input_message)
{
	const char	*buf = NULL;
	int			len = input_message->len;

	buf = pq_getmsgbytes(input_message, len);

	SwitchResGroupOnDN(buf, len);

	pq_getmsgend(input_message);
}

/*
 * Current is leader CN. Handle resource group request from non-leader CN, and perform resource control at leader CN.
 */
void
CNReceiveResGroupReqAndReply(StringInfo input_message)
{
	StringInfoData str;

	/* leader cn never set this */
	Assert (enable_resgroup_on_parallel_workers_on_dn == false);
	Assert (non_leader_cn_has_set_resgroup == false);
	/*
	 * If resgroup assigned on remote leader coordinator, just unassign it,
	 * implying end of resource control. Else, acquire a resource group slot.
	 */
	if (ResGroupIsAssigned())
		UnassignResGroup();
	else
		AssignResGroup();

	/* Reply to local coordinator */
	pq_beginmessage(&str, 'J');
	pq_endmessage(&str);

	pq_flush();
}

/*
 * Current is the client connected CN. Handle resource group info from leader CN, and perform resource control at current CN.
 */
void
CNReceiveResGroupReply()
{
	Assert(!ResGroupIsAssigned());
	if (non_leader_cn_has_set_resgroup)
		UnassignResGroup();
	else
	{
		AssignResGroupForNormalQuery();
		non_leader_cn_has_set_resgroup = true;
	}
}

/*
 * Check whether resource group should be assigned on CN.
 */
bool
ShouldAssignResGroup(void)
{
	/*
	 * Bypass resource group when it's waiting for a resource group slot. e.g.
	 * MyProc was interrupted by SIGTERM while waiting for resource group slot.
	 * Some callbacks - RemoveTempRelationsCallback for example - open new
	 * transactions on proc exit. It can cause a double add of MyProc to the
	 * waiting queue (and its corruption).
	 *
	 * Also bypass resource group when it's exiting.
	 */
	return IsResGroupActivated() &&
		IsNormalProcessingMode() &&
		!RecoveryInProgress() &&
		!proc_exit_inprogress &&
		!procIsWaiting(MyProc);
}

/*
 * Check whether resource group should be un-assigned.
 * This will be called on both CN and DNs.
 */
bool
ShouldUnassignResGroup(void)
{
	return IsResGroupActivated() &&
		IsNormalProcessingMode();
}

/*
 * On leader CN, backend is assigned to a resource group.
 * It will first acquire a slot from the resource group, and then, it will get the
 * current capability snapshot, update the memory usage information, and add to
 * the corresponding cgroup.
 */
void
AssignResGroup(void)
{
	ResGroupSlotData	*slot;
	ResGroupInfo		groupInfo;

	/*
	 * if query should be bypassed, do not assign a
	 * resource group, leave self unassigned
	 */
	if (shouldBypassQuery(debug_query_string))
	{
		/*
		 * Increase a bypassed ref count to prevent the group
		 * being dropped concurrently.
		 */
		do {
			decideResGroup(&groupInfo);
		} while (!groupIncBypassedRef(&groupInfo));

		/* Record which resgroup we are running in */
		bypassedGroup = groupInfo.group;

		/* Update pg_stat_activity statistics */
		bypassedGroup->totalExecuted++;
		pgstat_report_resgroup(bypassedGroup->groupId);

		/* Initialize the fake slot */
		bypassedSlot.group = groupInfo.group;
		bypassedSlot.groupId = groupInfo.groupId;
		strcpy(NameStr(bypassedSlot.groupName), NameStr(groupInfo.groupName));

		/* Add into cgroup */
		ResGroupOps_AssignGroup(NameStr(bypassedSlot.groupName),
								&bypassedGroup->caps,
								MyProcPid);

		return;
	}

	PG_TRY();
	{
		do {
			decideResGroup(&groupInfo);

			/* Acquire slot */
			slot = groupAcquireSlot(&groupInfo);
		} while (slot == NULL);

		/*
		 * Need not set resource group slot for current session
		 * because we have no global session on CNs as DNs yet.
		 */
		//sessionSetSlot(slot);

		/* Add proc memory accounting info into group and slot */
		selfAttachResGroup(groupInfo.group, slot);

		/* Init self */
		self->caps = slot->caps;

		DBUG_EXECUTE_IF("resgroup_assigned_failed", {
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("hit stub on resgroup_assigned_failed")));
		});

		/* Add into cgroup */
		ResGroupOps_AssignGroup(GetResGroupNameForId(self->groupId), &(self->caps), MyProcPid);
	}
	PG_CATCH();
	{
		UnassignResGroup();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * if query should be bypassed, do not assign a
 * resource group, leave self unassigned
 */
bool
AssignResGroupForBypassQueryOrNot(void)
{
	ResGroupInfo		groupInfo;

	if (shouldBypassQuery(debug_query_string))
	{
		/*
		 * Increase a bypassed ref count to prevent the group
		 * being dropped concurrently.
		 */
		do {
			decideResGroup(&groupInfo);
		} while (!groupIncBypassedRef(&groupInfo));

		/* Record which resgroup we are running in */
		bypassedGroup = groupInfo.group;

		/* Update pg_stat_activity statistics */
		bypassedGroup->totalExecuted++;
		pgstat_report_resgroup(bypassedGroup->groupId);

		/* Initialize the fake slot */
		bypassedSlot.group = groupInfo.group;
		bypassedSlot.groupId = groupInfo.groupId;
		strcpy(NameStr(bypassedSlot.groupName), NameStr(groupInfo.groupName));

		/* Add into cgroup */
		ResGroupOps_AssignGroup(NameStr(bypassedSlot.groupName),
								&bypassedGroup->caps,
								MyProcPid);

		return true;
	}

	return false;
}

/*
 * On client-connected non-leader CN, backend can be waken up after assignment
 * of resource group on leader CN.
 */
void
AssignResGroupForNormalQuery()
{
	ResGroupInfo		groupInfo;
	Assert(!selfIsAssigned() && !enable_resgroup_on_parallel_workers_on_dn && !non_leader_cn_has_set_resgroup);

	PG_TRY();
	{
		decideResGroup(&groupInfo);

		/* Add into cgroup */
		ResGroupOps_AssignGroup(NameStr(groupInfo.groupName), &(groupInfo.group->caps), MyProcPid);
	}
	PG_CATCH();
	{
		UnassignResGroup();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Detach from a resource group at the end of the transaction.
 */
void
UnassignResGroup(void)
{
	ResGroupData		*group = self->group;
	ResGroupSlotData	*slot = self->slot;

	enable_resgroup_on_parallel_workers_on_dn = false;
	non_leader_cn_has_set_resgroup = false;

	if (bypassedGroup)
	{
		/* bypass mode ref count is only maintained on qd */
		if (IS_PGXC_LOCAL_COORDINATOR)
			groupDecBypassedRef(bypassedGroup);

		/* Reset the fake slot */
		bypassedSlot.group = NULL;
		bypassedSlot.groupId = InvalidOid;
		memset(NameStr(bypassedSlot.groupName), 0, NAMEDATALEN);
		bypassedGroup = NULL;

		/* Update pg_stat_activity statistics */
		pgstat_report_resgroup(InvalidOid);
		return;
	}

	if (!selfIsAssigned())
	{
		/* non leader cn will reach here */
		pgstat_report_resgroup(InvalidOid);
		return;
	}

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	/* Sub proc memory accounting info from group and slot */
	selfDetachResGroup(group, slot);

	/* Release the slot if no reference. */
	if (slot->nProcs == 0)
	{
		groupReleaseSlot(group, slot);
	}

	LWLockRelease(ResGroupLock);

	pgstat_report_resgroup(InvalidOid);
}

/*
 * DNs are not assigned/unassigned to a resource group on segments for each
 * transaction, instead, they switch resource group when a new resource group
 * id or slot id is dispatched.
 */
void
SwitchResGroupOnDN(const char *buf, int len)
{
	char	newGroupName[NAMEDATALEN] = "";
	Oid		newGroupId;
	ResGroupCaps		caps;

	Assert(!selfIsAssigned() && !enable_resgroup_on_parallel_workers_on_dn && !non_leader_cn_has_set_resgroup);
	DeserializeResGroupInfo(&caps, newGroupName, buf, len);

	/*
	 * CN will dispatch the resgroup name via bypassedSlot.groupName
	 * in bypass mode.
	 */
	if (NameStr(bypassedSlot.groupName)[0] != '\0')
	{
		Oid bypassedGroupId = get_resgroup_oid(NameStr(bypassedSlot.groupName), true);

		/* Are we already running in bypass mode? */
		if (bypassedGroup != NULL)
		{
			Assert(bypassedGroup->groupId == bypassedGroupId);
			return;
		}

		/* Find out the resgroup by id */
		LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
		bypassedGroup = groupHashFind(bypassedGroupId, true);
		LWLockRelease(ResGroupLock);

		Assert(bypassedGroup != NULL);

		return;
	}

	newGroupId = get_resgroup_oid(newGroupName, true);
	if (newGroupId == InvalidOid)
	{
		return;
	}

	enable_resgroup_on_parallel_workers_on_dn = true;

	/* Add into cgroup */
	ResGroupOps_AssignGroup(newGroupName, &caps, MyProcPid);
}

/*
 * Wait on the queue of resource group
 */
static void
waitOnGroup(ResGroupData *group, PROC_QUEUE *waitQueue)
{
	int64 timeout = -1;
	int64 curTime;
	PGPROC *proc = MyProc;

	Assert(!LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	/* adjust the process title to indicate that it's waiting */
	set_ps_display_suffix("waiting");

	/*
	 * The low bits of 'wait_event_info' argument to WaitLatch are
	 * not enough to store a full Oid, so we set groupId out-of-band,
	 * via the backend entry.
	 */
	pgstat_report_resgroup(group->groupId);

	/*
	 * Mark that we are waiting on resource group
	 *
	 * This is used for interrupt cleanup, similar to lockAwaited in ProcSleep
	 */
	groupAwaited = group;
	groupWaitQueue = waitQueue;
	groupWaitStart = GetCurrentTimestamp();

	/*
	 * Make sure we have released all locks before going to sleep, to eliminate
	 * deadlock situations
	 */
	PG_TRY();
	{
		for (;;)
		{
			ResetLatch(&proc->procLatch);

			CHECK_FOR_INTERRUPTS();

			if (!procIsWaiting(proc))
				break;

			if (resource_group_queuing_timeout > 0)
			{
				curTime = GetCurrentTimestamp();
				timeout = resource_group_queuing_timeout - (curTime - groupWaitStart) / 1000;
				if (timeout < 0)
					ereport(ERROR,
							(errcode(ERRCODE_QUERY_CANCELED),
							 errmsg("canceling statement due to resource group waiting timeout")));

				WaitLatch(&proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						  (long) timeout, PG_WAIT_RESOURCE_GROUP);
			}
			else
			{
				WaitLatch(&proc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
						  PG_WAIT_RESOURCE_GROUP);
			}
		}
	}
	PG_CATCH();
	{
		/* reset ps display to remove the suffix */
		set_ps_display_remove_suffix();

		groupWaitCancel();
		PG_RE_THROW();
	}
	PG_END_TRY();

	groupAwaited = NULL;
	groupWaitQueue = NULL;
	groupWaitEnd = GetCurrentTimestamp();

	/* reset ps display to remove the suffix */
	set_ps_display_remove_suffix();
}

/*
 * groupHashNew -- return a new (empty) group object to initialize.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroupData *
groupHashNew(Oid groupId)
{
	int			i;
	bool		found;
	ResGroupHashEntry *entry;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(groupId != InvalidOid);

	for (i = 0; i < pResGroupControl->nGroups; i++)
	{
		if (pResGroupControl->groups[i].groupId == InvalidOid)
			break;
	}
	Assert(i < pResGroupControl->nGroups);

	entry = (ResGroupHashEntry *)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_ENTER, &found);
	/* caller should test that the group does not exist already */
	Assert(!found);
	entry->index = i;

	return &pResGroupControl->groups[i];
}

/*
 * groupHashFind -- return the group for a given oid.
 *
 * If the group cannot be found, then NULL is returned if 'raise' is false,
 * otherwise an exception is thrown.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroupData *
groupHashFind(Oid groupId, bool raise)
{
	bool				found;
	ResGroupHashEntry	*entry;

	Assert(LWLockHeldByMe(ResGroupLock));

	entry = (ResGroupHashEntry *)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_FIND, &found);

	if (!found)
	{
		ereport(raise ? ERROR : LOG,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot find resource group with Oid %d in shared memory",
						groupId)));
		return NULL;
	}

	Assert(entry->index < pResGroupControl->nGroups);
	return &pResGroupControl->groups[entry->index];
}


/*
 * groupHashRemove -- remove the group for a given oid.
 *
 * If the group cannot be found then an exception is thrown.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroupData *
groupHashRemove(Oid groupId)
{
	bool		found;
	ResGroupHashEntry	*entry;
	ResGroupData		*group;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	entry = (ResGroupHashEntry*)hash_search(pResGroupControl->htbl,
											(void *) &groupId,
											HASH_REMOVE,
											&found);
	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot find resource group with Oid %d in shared memory to remove",
						groupId)));

	group = &pResGroupControl->groups[entry->index];

	return group;
}

/* Process exit without waiting for slot or received SIGTERM */
static void
AtProcExit_ResGroup(int code, Datum arg)
{
	groupWaitCancel();
}

/*
 * Handle the interrupt cases when waiting on the queue
 *
 * The proc may wait on the queue for a slot, or wait for the
 * DROP transaction to finish. In the first case, at the same time
 * we get interrupted (SIGINT or SIGTERM), we could have been
 * granted a slot or not. In the second case, there's no running
 * transaction in the group. If the DROP transaction is finished
 * (commit or abort) at the same time as we get interrupted,
 * MyProc should have been removed from the wait queue, and the
 * ResGroupData entry may have been removed if the DROP is committed.
 */
static void
groupWaitCancel(void)
{
	ResGroupData		*group;
	PROC_QUEUE			*waitQueue;
	ResGroupSlotData	*slot;

	/* Release resource group slot at the end for safety.*/
	if (IS_PGXC_COORDINATOR && ShouldUnassignResGroup())
		UnassignResGroup();

	/* Nothing to do if we weren't waiting on a group */
	if (groupAwaited == NULL)
		return;

	pgstat_report_wait_end();
	groupWaitEnd = GetCurrentTimestamp();

	group = groupAwaited;
	waitQueue = groupWaitQueue;

	/* We are sure to be interrupted in the for loop of waitOnGroup now */
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	AssertImply(procIsWaiting(MyProc),
				groupWaitQueueFind(waitQueue, MyProc));

	if (procIsWaiting(MyProc))
	{
		/*
		 * Still waiting on the queue when get interrupted, remove
		 * myself from the queue
		 */

		Assert(!groupWaitQueueIsEmpty(waitQueue));

		groupWaitQueueErase(waitQueue, MyProc);

		addTotalQueueDuration(group);
	}
	else if (MyProc->resSlot != NULL)
	{
		/* Woken up by a slot holder */

		Assert(!procIsWaiting(MyProc));

		/* First complete the slot's transfer from MyProc to self */
		slot = MyProc->resSlot;
		MyProc->resSlot = NULL;

		/*
		 * Similar as groupReleaseSlot(), how many pending queries to
		 * wake up depends on how many slots we can get.
		 */
		groupReleaseSlot(group, slot);

		group->totalExecuted++;

		addTotalQueueDuration(group);
	}
	else
	{
		/*
		 * The transaction of DROP RESOURCE GROUP is finished,
		 * groupAcquireSlot will do the retry.
		 *
		 * The resource group pointed by self->group may have
		 * already been removed by here.
		 */

		Assert(!procIsWaiting(MyProc));
	}

	LWLockRelease(ResGroupLock);

	groupAwaited = NULL;
}

/*
 * Validate the consistency of the resgroup information in self.
 *
 * This function checks the consistency of (group & groupId).
 */
static void
selfValidateResGroupInfo(void)
{
	AssertImply(self->groupId != InvalidOid,
				self->group != NULL);
}

/*
 * Check whether self is assigned.
 *
 * This is mostly equal to (selfHasSlot() && selfHasGroup()),
 * however this function requires the slot and group to be in
 * a consistent status, they must both be set or unset,
 * so calling this function during the assign/unassign/switch process
 * might cause an error, use with caution.
 *
 * Even selfIsAssigned() is true it doesn't mean the assign/switch
 * process is completely done, for example the memory accounting
 * information might not been updated yet.
 *
 * This function doesn't check whether the assigned resgroup
 * is valid or dropped.
 */
static bool
selfIsAssigned(void)
{
	selfValidateResGroupInfo();
	AssertImply(self->group == NULL,
			self->slot == NULL);
	AssertImply(self->group != NULL,
			self->slot != NULL);

	return self->groupId != InvalidOid;
}

#ifdef USE_ASSERT_CHECKING
/*
 * Check whether self has been set a slot.
 *
 * We don't check whether a resgroup is set or not.
 */
static bool
selfHasSlot(void)
{
	return self->slot != NULL;
}

/*
 * Check whether self has been set a resgroup.
 *
 * Consistency will be checked on the groupId and group pointer.
 *
 * We don't check whether the resgroup is valid or dropped.
 *
 * We don't check whether a slot is set or not.
 */
static bool
selfHasGroup(void)
{
	AssertImply(self->groupId != InvalidOid,
				self->group != NULL);

	return self->groupId != InvalidOid;
}
#endif /* USE_ASSERT_CHECKING */

/*
 * Set both the groupId and the group pointer in self.
 *
 * The group must not be dropped.
 *
 * Some over limitations are put to force the caller understand
 * what it's doing and what it wants:
 * - self must has not been set a resgroup;
 */
static void
selfSetGroup(ResGroupData *group)
{
	Assert(!selfIsAssigned());
	Assert(groupIsNotDropped(group));

	self->group = group;
	self->groupId = group->groupId;
}

/*
 * Unset both the groupId and the resgroup pointer in self.
 *
 * Some over limitations are put to force the caller understand
 * what it's doing and what it wants:
 * - self must has been set a resgroup;
 */
static void
selfUnsetGroup(void)
{
	Assert(selfHasGroup());
	Assert(!selfHasSlot());

	self->groupId = InvalidOid;
	self->group = NULL;
}

/*
 * Set the slot pointer in self.
 *
 * Some over limitations are put to force the caller understand
 * what it's doing and what it wants:
 * - self must has been set a resgroup;
 * - self must has not been set a slot before set;
 */
static void
selfSetSlot(ResGroupSlotData *slot)
{
	Assert(selfHasGroup());
	Assert(!selfHasSlot());
	Assert(slotIsInUse(slot));

	self->slot = slot;
}

/*
 * Unset the slot pointer in self.
 *
 * Some over limitations are put to force the caller understand
 * what it's doing and what it wants:
 * - self must has been set a resgroup;
 * - self must has been set a slot before unset;
 */
static void
selfUnsetSlot(void)
{
	Assert(selfHasGroup());
	Assert(selfHasSlot());

	self->slot = NULL;
}

/*
 * Check whether proc is in some resgroup's wait queue.
 *
 * The LWLock is not required.
 *
 * This function does not check whether proc is in a specific resgroup's
 * wait queue. To make this check use groupWaitQueueFind().
 */
static bool
procIsWaiting(const PGPROC *proc)
{
	/*------
	 * The typical asm instructions fow below C operation can be like this:
	 * ( gcc 4.8.5-11, x86_64-redhat-linux, -O0 )
	 *
     *     mov    -0x8(%rbp),%rax           ; load proc
     *     mov    0x8(%rax),%rax            ; load proc->links.next
     *     cmp    $0,%rax                   ; compare with NULL
     *     setne  %al                       ; store the result
	 *
	 * The operation is atomic, so a lock is not required here.
	 *------
	 */
	return proc->links.next != NULL;
}

/*
 * Notify a proc it's woken up.
 */
static void
procWakeup(PGPROC *proc)
{
	Assert(!procIsWaiting(proc));

	SetLatch(&proc->procLatch);
}

#ifdef USE_ASSERT_CHECKING
/*
 * Validate a slot's attributes.
 */
static void
slotValidate(const ResGroupSlotData *slot)
{
	Assert(slot != NULL);

	/* further checks whether the slot is freed or idle */
	if (slot->groupId == InvalidOid)
	{
		Assert(slot->nProcs == 0);
	}
	else
	{
		Assert(!slotIsInFreelist(slot));
	}
}

/*
 * A slot is in use if it has a valid groupId.
 */
static bool
slotIsInUse(const ResGroupSlotData *slot)
{
	slotValidate(slot);

	return slot->groupId != InvalidOid;
}

static bool
slotIsInFreelist(const ResGroupSlotData *slot)
{
	ResGroupSlotData *current;

	current = pResGroupControl->freeSlot;

	for ( ; current != NULL; current = current->next)
	{
		if (current == slot)
			return true;
	}

	return false;
}
#endif /* USE_ASSERT_CHECKING */

/*
 * Get the slot id of the given slot.
 *
 * Return InvalidSlotId if slot is NULL.
 */
static int
slotGetId(const ResGroupSlotData *slot)
{
	int			slotId;

	if (slot == NULL)
		return InvalidSlotId;

	slotId = slot - pResGroupControl->slots;

	Assert(slotId >= 0);
	Assert(slotId < RESGROUP_MAX_SLOTS);

	return slotId;
}

static void
lockResGroupForDrop(ResGroupData *group)
{
	if (group->lockedForDrop)
		return;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(IS_PGXC_COORDINATOR);
	Assert(group->nRunning == 0);
	Assert(group->nRunningBypassed == 0);
	group->lockedForDrop = true;
}

static void
unlockResGroupForDrop(ResGroupData *group)
{
	if (!group->lockedForDrop)
		return;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(IS_PGXC_COORDINATOR);
	Assert(group->nRunning == 0);
	Assert(group->nRunningBypassed == 0);
	group->lockedForDrop = false;
}

#ifdef USE_ASSERT_CHECKING
/*
 * Check whether a resgroup is dropped.
 *
 * A dropped resgroup has groupId == InvalidOid,
 * however there is also the case that the resgroup is first dropped
 * then the shm struct is reused by another newly created resgroup,
 * in such a case the groupId is not InvalidOid but the original
 * resgroup does is dropped.
 *
 * So this function is not always reliable, use with caution.
 */
static bool
groupIsNotDropped(const ResGroupData *group)
{
	return group
		&& group->groupId != InvalidOid;
}
#endif /* USE_ASSERT_CHECKING */

/*
 * Validate the consistency of the resgroup wait queue.
 */
static void
groupWaitQueueValidate(const PROC_QUEUE *waitQueue)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	if (resource_group_debug_wait_queue)
	{
		if (waitQueue->size == 0)
		{
			if (waitQueue->links.next != &waitQueue->links ||
				waitQueue->links.prev != &waitQueue->links)
				elog(PANIC, "resource group wait queue is corrupted");
		}
		else
		{
			PGPROC *nextProc = (PGPROC *)waitQueue->links.next;
			PGPROC *prevProc = (PGPROC *)waitQueue->links.prev;

			if (nextProc->links.prev != &waitQueue->links ||
				prevProc->links.next != &waitQueue->links)
				elog(PANIC, "resource group wait queue is corrupted");
		}

		return;
	}

	AssertImply(waitQueue->size == 0,
				waitQueue->links.next == &waitQueue->links &&
				waitQueue->links.prev == &waitQueue->links);
}

static void
groupWaitProcValidate(PGPROC *proc, PROC_QUEUE *head)
{
	PGPROC *nextProc = (PGPROC *)proc->links.next;
	PGPROC *prevProc = (PGPROC *)proc->links.prev;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	if (!resource_group_debug_wait_queue)
		return;

	if (nextProc->links.prev != &proc->links ||
		prevProc->links.next != &proc->links)
		elog(PANIC, "resource group wait queue is corrupted");

	return;
}

/*
 * Push a proc to the resgroup wait queue.
 */
static void
groupWaitQueuePush(PROC_QUEUE *waitQueue, PGPROC *proc)
{
	PGPROC				*headProc;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(!procIsWaiting(proc));
	Assert(proc->resSlot == NULL);

	groupWaitQueueValidate(waitQueue);

	headProc = (PGPROC *) &waitQueue->links;

	SHMQueueInsertBefore(&headProc->links, &proc->links);
	groupWaitProcValidate(proc, waitQueue);

	waitQueue->size++;

	Assert(groupWaitQueueFind(waitQueue, proc));
}

/*
 * Pop the top proc from the resgroup wait queue and return it.
 */
static PGPROC *
groupWaitQueuePop(PROC_QUEUE *waitQueue)
{
	PGPROC				*proc;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(!groupWaitQueueIsEmpty(waitQueue));

	groupWaitQueueValidate(waitQueue);

	proc = (PGPROC *) waitQueue->links.next;
	groupWaitProcValidate(proc, waitQueue);
	Assert(groupWaitQueueFind(waitQueue, proc));
	Assert(proc->resSlot == NULL);

	SHMQueueDelete(&proc->links);

	waitQueue->size--;

	return proc;
}

/*
 * Erase proc from the resgroup wait queue.
 */
static void
groupWaitQueueErase(PROC_QUEUE *waitQueue, PGPROC *proc)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));
	Assert(!groupWaitQueueIsEmpty(waitQueue));
	Assert(groupWaitQueueFind(waitQueue, proc));
	Assert(proc->resSlot == NULL);

	groupWaitQueueValidate(waitQueue);

	groupWaitProcValidate(proc, waitQueue);
	SHMQueueDelete(&proc->links);

	waitQueue->size--;
}

/*
 * Check whether the resgroup wait queue is empty.
 */
static bool
groupWaitQueueIsEmpty(const PROC_QUEUE *waitQueue)
{
	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	groupWaitQueueValidate(waitQueue);

	return waitQueue->size == 0;
}

#ifdef USE_ASSERT_CHECKING
/*
 * Find proc in group's wait queue.
 *
 * Return true if found or false if not found.
 *
 * This functions is expensive so should only be used in debugging logic,
 * in most cases procIsWaiting() shall be used.
 */
static bool
groupWaitQueueFind(PROC_QUEUE *waitQueue, const PGPROC *proc)
{
	SHM_QUEUE			*head;
	PGPROC				*iter;
	Size				offset;

	Assert(LWLockHeldByMeInMode(ResGroupLock, LW_EXCLUSIVE));

	groupWaitQueueValidate(waitQueue);

	head = &waitQueue->links;
	offset = offsetof(PGPROC, links);

	for (iter = (PGPROC *) SHMQueueNext(head, head, offset); iter;
		 iter = (PGPROC *) SHMQueueNext(head, &iter->links, offset))
	{
		if (iter == proc)
		{
			Assert(procIsWaiting(proc));
			return true;
		}
	}

	return false;
}
#endif/* USE_ASSERT_CHECKING */

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

static bool
checkSQLBypass(const char *sql)
{
	char *sql_copy = pstrdup(sql);
	char *token, *saveptr;
	bool result = false;

	token = strtok_r(resource_group_bypass_sql, ";", &saveptr);
	while (token != NULL)
	{
		if (RE_compile_and_execute(cstring_to_text(token),
								   sql_copy,
								   strlen(sql_copy),
								   REG_ADVANCED,
								   DEFAULT_COLLATION_OID,
								   0, NULL))
		{
			result = true;
			break;
		}

		token = strtok_r(NULL, ",", &saveptr);
	}

	pfree(sql_copy);

	return result;
}
#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

/*
 * Parse the query and check if this query should
 * bypass the management of resource group.
 *
 * Currently, only SET/RESET/SHOW command can be bypassed
 */
static bool
shouldBypassQuery(const char *query_string)
{
	MemoryContext oldcontext = NULL;
	MemoryContext tmpcontext = NULL;
	List *parsetree_list; 
	ListCell *parsetree_item;
	Node *parsetree;
	bool		bypass;

	if (resource_group_bypass)
		return true;

	if (!query_string)
		return false;

	if (resource_group_bypass_sql != NULL)
		if (checkSQLBypass(query_string))
			return true;

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 *
	 * It is possible that MessageContext is NULL, for example in a bgworker:
	 *
	 *     debug_query_string = "select 1";
	 *     StartTransactionCommand();
	 *
	 * This is not the recommended order of setting debug_query_string, but we
	 * should not put a constraint on the order by resource group anyway.
	 */
	if (MessageContext)
		oldcontext = MemoryContextSwitchTo(MessageContext);
	else
	{
		/* Create a temp memory context to prevent memory leaks */
		tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
										   "resgroup temporary context",
										   ALLOCSET_DEFAULT_SIZES);
		oldcontext = MemoryContextSwitchTo(tmpcontext);
	}

	parsetree_list = pg_parse_query(query_string);

	MemoryContextSwitchTo(oldcontext);

	if (parsetree_list == NULL)
		return false;

	/* Only bypass SET/RESET/SHOW command for now */
	bypass = true;
	foreach(parsetree_item, parsetree_list)
	{
		parsetree = (Node *) lfirst(parsetree_item);

		if (nodeTag(parsetree) == T_RawStmt)
			parsetree = ((RawStmt *)parsetree)->stmt;

		if (nodeTag(parsetree) != T_VariableSetStmt &&
			nodeTag(parsetree) != T_VariableShowStmt)
		{
			bypass = false;
			break;
		}
	}

	list_free_deep(parsetree_list);

	if (tmpcontext)
		MemoryContextDelete(tmpcontext);

	return bypass;
}

/*
 * Check whether the resource group has been dropped.
 */
static bool
groupIsDropped(ResGroupInfo *pGroupInfo)
{
	Assert(pGroupInfo != NULL);
	Assert(pGroupInfo->group != NULL);

	return pGroupInfo->group->groupId != pGroupInfo->groupId;
}

/*
 * Debug helper functions
 */
void
ResGroupDumpInfo(StringInfo str)
{
	int				i;

	if (!enable_resource_group)
		return;

	appendStringInfo(str, "{\"nodename\":\"%s\",", PGXCNodeName);
	/* dump fields in pResGroupControl. */
	appendStringInfo(str, "\"loaded\":%s,", pResGroupControl->loaded ? "true" : "false");

	/* dump each group */
	appendStringInfo(str, "\"groups\":[");
	for (i = 0; i < pResGroupControl->nGroups; i++)
	{
		resgroupDumpGroup(str, &pResGroupControl->groups[i]);
		if (i < pResGroupControl->nGroups - 1)
			appendStringInfo(str, ","); 
	}
	appendStringInfo(str, "],"); 
	/* dump slots */
	resgroupDumpSlots(str);

	appendStringInfo(str, ",");

	/* dump freeslot links */
	resgroupDumpFreeSlots(str);

	appendStringInfo(str, "}"); 
}

static void
resgroupDumpGroup(StringInfo str, ResGroupData *group)
{
	appendStringInfo(str, "{");
	appendStringInfo(str, "\"group_id\":%u,", group->groupId);
	appendStringInfo(str, "\"nRunning\":%d,", group->nRunning);
	appendStringInfo(str, "\"nRunningBypassed\":%d,", group->nRunningBypassed);
	appendStringInfo(str, "\"locked_for_drop\":%d,", group->lockedForDrop);

	resgroupDumpWaitQueue(str, &group->waitProcsH);
	resgroupDumpWaitQueue(str, &group->waitProcsM);
	resgroupDumpWaitQueue(str, &group->waitProcsL);
	resgroupDumpCaps(str, (ResGroupCap*)(&group->caps));
	
	appendStringInfo(str, "}");
}

static void
resgroupDumpWaitQueue(StringInfo str, PROC_QUEUE *queue)
{
	PGPROC *proc;

	appendStringInfo(str, "\"wait_queue\":{");
	appendStringInfo(str, "\"wait_queue_size\":%d,", queue->size);
	appendStringInfo(str, "\"wait_queue_content\":[");

	proc = (PGPROC *)SHMQueueNext(&queue->links,
								  &queue->links, 
								  offsetof(PGPROC, links));

	if (!ShmemAddrIsValid(&proc->links))
	{
		appendStringInfo(str, "]},");
		return;
	}

	while (proc)
	{
		appendStringInfo(str, "{");
		appendStringInfo(str, "\"pid\":%d,", proc->pid);
		appendStringInfo(str, "\"resWaiting\":%s,",
						 procIsWaiting(proc) ? "true" : "false");
		appendStringInfo(str, "\"resSlot\":%d", slotGetId(proc->resSlot));
		appendStringInfo(str, "}");
		proc = (PGPROC *)SHMQueueNext(&queue->links,
							&proc->links, 
							offsetof(PGPROC, links));
		if (proc)
			appendStringInfo(str, ",");
	}
	appendStringInfo(str, "]},");
}

static void
resgroupDumpCaps(StringInfo str, ResGroupCap *caps)
{
	int i;
	appendStringInfo(str, "\"caps\":[");
	for (i = 1; i < RESGROUP_LIMIT_TYPE_COUNT; i++)
	{
		appendStringInfo(str, "{\"%d\":%d}", i, caps[i]);
		if (i < RESGROUP_LIMIT_TYPE_COUNT - 1)
			appendStringInfo(str, ",");
	}
	appendStringInfo(str, "]");
}

static void
resgroupDumpSlots(StringInfo str)
{
	int               i;
	ResGroupSlotData* slot;

	appendStringInfo(str, "\"slots\":[");

	for (i = 0; i < RESGROUP_MAX_SLOTS; i++)
	{
		slot = &(pResGroupControl->slots[i]);

		appendStringInfo(str, "{");
		appendStringInfo(str, "\"slotId\":%d,", i);
		appendStringInfo(str, "\"groupId\":%u,", slot->groupId);
		appendStringInfo(str, "\"nProcs\":%d,", slot->nProcs);
		appendStringInfo(str, "\"next\":%d,", slotGetId(slot->next));
		resgroupDumpCaps(str, (ResGroupCap*)(&slot->caps));
		appendStringInfo(str, "}");
		if (i < RESGROUP_MAX_SLOTS - 1)
			appendStringInfo(str, ",");
	}
	
	appendStringInfo(str, "]");
}

static void
resgroupDumpFreeSlots(StringInfo str)
{
	ResGroupSlotData* slot;

	appendStringInfo(str, "\"free_slots\":[");
	slot = pResGroupControl->freeSlot;
	while (slot != NULL)
	{
		appendStringInfo(str, "{");
		appendStringInfo(str, "\"slotId\":%d", slotGetId(slot));
		appendStringInfo(str, "}");
		if (slot->next != NULL)
			appendStringInfo(str, ",");

		slot = slot->next;
	}
	appendStringInfo(str, "]");
}

void
ResGroupCheckFreeSlots(StringInfo str)
{
	int count;
	ResGroupSlotData* slot;

	if (!enable_resource_group)
		return;

	appendStringInfo(str, "{\"nodename\":\"%s\",", PGXCNodeName);

	count = 0;
	slot = pResGroupControl->freeSlot;
	if (slot != NULL)
	{
		count++;
		while (slot->next != NULL)
		{
			slot = slot->next;
			count++;
		}
	}
	appendStringInfo(str, "\"length\":%d,\"head\":%d,\"tail\":%d}",
					 count, slotGetId(pResGroupControl->freeSlot), slotGetId(slot));
}

/*
 * Parse cpuset to bitset
 * If cpuset is "1,3-5", Bitmapset 1,3,4,5 are set.
 */
Bitmapset *
CpusetToBitset(const char *cpuset, int len)
{
	int	pos = 0, num1 = 0, num2 = 0;
	int i;
	enum Status
	{
		Initial,
		Begin,
		Number,
		Interval,
		Number2
	};
	enum Status	s = Initial;

	Bitmapset	*bms = NULL;
	if (cpuset == NULL || len <= 0)
		return bms;
	while (pos < len && cpuset[pos])
	{
		char c = cpuset[pos++];
		if (c == ',')
		{
			if (s == Initial || s == Begin)
			{
				continue;
			}
			else if (s == Interval)
			{
				goto error_logic;
			}
			else if (s == Number)
			{
				bms = bms_union(bms, bms_make_singleton(num1));
				num1 = 0;
				s = Begin;
			}
			else if (s == Number2)
			{
				if (num1 > num2)
				{
					goto error_logic;
				}
				for (i = num1; i <= num2; ++i)
				{
					bms = bms_union(bms, bms_make_singleton(i));
				}
				num1 = num2 = 0;
				s = Begin;
			}
		}
		else if (c == '-')
		{
			if (s != Number)
			{
				goto error_logic;
			}
			s = Interval;
		}
		else if (isdigit(c))
		{
			if (s == Initial || s == Begin)
			{
				s = Number;
			}
			else if (s == Interval)
			{
				s = Number2;
			}
			if (s == Number)
			{
				num1 = num1 * 10 + (c - '0');
			}
			else if (s == Number2)
			{
				num2 = num2 * 10 + (c - '0');
			}
		}
		else if (c == '\n')
		{
			break;
		}
		else
		{
			goto error_logic;
		}
	}
	if (s == Number)
	{
		bms = bms_union(bms, bms_make_singleton(num1));
	}
	else if (s == Number2)
	{
		if (num1 > num2)
		{
			goto error_logic;
		}
		for (i = num1; i <= num2; ++i)
		{
			bms = bms_union(bms, bms_make_singleton(i));
		}
	}
	else if (s == Initial || s == Interval)
	{
		goto error_logic;
	}
	return bms;
error_logic:
	return NULL;
}

/*
 * Check the value of cpuset is empty or not
 */
bool CpusetIsEmpty(const char *cpuset)
{
	return strcmp(cpuset, DefaultCpuset) == 0;
}

/*
 * Set cpuset value to default value -1.
 */
void SetCpusetEmpty(char *cpuset, int cpusetSize)
{
	StrNCpy(cpuset, DefaultCpuset, cpusetSize);
}

/*
 * Transform non-empty bitset to cpuset.
 *
 * This function does not check the cpu cores are available or not.
 */
void
BitsetToCpuset(const Bitmapset *bms,
			   char *cpuset,
			   int cpusetSize)
{
	int len = 0;
	int lastContinuousBit = -1;
	int	intervalStart = -1;
	int num;
	char buffer[32] = {0};

	Assert(!bms_is_empty(bms));

	cpuset[0] = '\0';

	num = -1;
	while ((num = bms_next_member(bms, num)) >= 0)
	{
		if (lastContinuousBit == -1)
		{
			intervalStart = lastContinuousBit = num;
		}
		else
		{
			if (num != lastContinuousBit + 1)
			{
				if (intervalStart == lastContinuousBit)
				{
					snprintf(buffer, sizeof(buffer), "%d,", intervalStart);
				}
				else
				{
					snprintf(buffer, sizeof(buffer), "%d-%d,", intervalStart, lastContinuousBit);
				}
				if (len + strlen(buffer) >= cpusetSize)
				{
					Assert(cpuset[0]);
					return ;
				}
				strcpy(cpuset + len, buffer);
				len += strlen(buffer);
				intervalStart = lastContinuousBit = num;
			}
			else
			{
				lastContinuousBit = num;
			}
		}
	}
	if (intervalStart != -1)
	{
		if (intervalStart == lastContinuousBit)
		{
			snprintf(buffer, sizeof(buffer), "%d", intervalStart);
		}
		else
		{
			snprintf(buffer, sizeof(buffer), "%d-%d", intervalStart, lastContinuousBit);
		}
		if (len + strlen(buffer) >= cpusetSize)
		{
			Assert(cpuset[0]);
			return ;
		}
		strcpy(cpuset + len, buffer);
		len += strlen(buffer);
	}
	else
	{
		/* bms is non-empty, so it should never reach here */
		pg_unreachable();
	}
}

/*
 * calculate the result of cpuset1 plus/minus cpuset2 and save in place
 * if sub is true, the operation is minus
 * if sub is false, the operation is plus
 */
void
cpusetOperation(char *cpuset1, const char *cpuset2,
							int len, bool sub)
{
	char cpuset[MaxCpuSetLength] = {0};
	int defaultCore = -1;
	Bitmapset *bms1 = CpusetToBitset(cpuset1, len);
	Bitmapset *bms2 = CpusetToBitset(cpuset2, len);
	if (sub)
	{
		bms1 = bms_del_members(bms1, bms2);
	}
	else
	{
		bms1 = bms_add_members(bms1, bms2);
	}
	if (!bms_is_empty(bms1))
	{
		BitsetToCpuset(bms1, cpuset1, len);
	}
	else
	{
		Bitmapset *bmsDefault;
		/* Get cpuset from cpuset/opentenbasedb, and transform it into bitset */
		ResGroupOps_GetCpuSet(RESGROUP_ROOT_PATH, cpuset, MaxCpuSetLength);
		bmsDefault = CpusetToBitset(cpuset, MaxCpuSetLength);
		/* get the minimum core number, in case of the zero core is not exist */
		defaultCore = bms_next_member(bmsDefault, -1);
		Assert(defaultCore >= 0);
		snprintf(cpuset1, MaxCpuSetLength, "%d", defaultCore);
	}
}

/*
 * union cpuset2 to cpuset1
 */
void
CpusetUnion(char *cpuset1, const char *cpuset2, int len)
{
	cpusetOperation(cpuset1, cpuset2, len, false);
}

/*
 * subtract cpuset2 from cpuset1
 */
void
CpusetDifference(char *cpuset1, const char *cpuset2, int len)
{
	cpusetOperation(cpuset1, cpuset2, len, true);
}

/*
 * ensure that cpuset is available.
 */
bool
EnsureCpusetIsAvailable(int elevel)
{
	if (!IsResGroupActivated())
	{
		ereport(elevel,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("resource group must be enabled to use cpuset feature")));

		return false;
	}

	if (!resource_group_enable_cgroup_cpuset)
	{
		ereport(elevel,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cgroup is not properly configured to use the cpuset feature"),
				 errhint("Extra cgroup configurations are required to enable this feature, "
						 "please refer to the Greenplum Documentations for details")));

		return false;
	}

	return true;
}

/*
 * In resource group mode, how much memory should a query take in bytes.
 */
uint64
ResourceGroupGetQueryMemoryLimit(void)
{
	ResGroupCaps		*caps;
	int64	resgLimit = -1;
	uint64	queryMem = -1;
	ResGroupInfo		groupInfo;

	Assert(IS_PGXC_COORDINATOR || IS_PGXC_DATANODE);

	if (resgroup_memory_query_fixed_mem)
		return (uint64) resgroup_memory_query_fixed_mem * 1024L;

	decideResGroup(&groupInfo);
	LWLockAcquire(ResGroupLock, LW_SHARED);

	caps = &groupInfo.group->caps;
	resgLimit = caps->memLimit;

	AssertImply(resgLimit < 0, resgLimit == -1);
	if (resgLimit == -1)
	{
		LWLockRelease(ResGroupLock);
		return (uint64) 0;
	}

	queryMem = (uint64)(resgLimit *1024L / caps->concurrency);
	LWLockRelease(ResGroupLock);

	return queryMem;
}

void
AssignResGroupOnParallelWorker(void)
{
	AssignResGroupForNormalQuery();
}

bool
resgroupCleanSlot(char *groupName)
{
	Oid groupId = get_resgroup_oid(groupName, true);
	ResGroupData *groupData;
	ResGroupSlotData *groupSlot;
	int i, j, nLiveProcs;
	bool cleaned;

	if (groupId == InvalidOid)
	{
		return false;
	}

	cleaned = false;
	groupData = groupHashFind(groupId, true);
	for (i = 0; i < RESGROUP_MAX_SLOTS; i++)
	{
		groupSlot = &(pResGroupControl->slots[i]);

		if (groupSlot->nProcs == 0)
			continue;

		nLiveProcs = 0;
		for (j = 0; j < groupSlot->nProcs; j++)
		{
			nLiveProcs += kill(groupSlot->procs[j], 0) == 0;
		}

		if (nLiveProcs == 0)
		{
			elog(WARNING, "resgroup slot leak: groupid %u, nProcs %d, procs[0] %d",
				 groupSlot->groupId, groupSlot->nProcs, groupSlot->procs[0]);
			LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
			groupSlot->nProcs = 0;
			groupReleaseSlot(groupData, groupSlot);
			LWLockRelease(ResGroupLock);
			cleaned = true;
		}
	}

	return cleaned;
}

Datum
pg_resgroup_clean_slot(PG_FUNCTION_ARGS)
{
#define PG_RESGROUP_CLEAN_SLOT_COLS	3
	char		*groupname = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HeapTuple	tuple;
	SysScanDesc sscan;
	Relation	relResGroup;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (IsResGroupActivated())
	{
		relResGroup = relation_open(ResGroupRelationId, ExclusiveLock);

		sscan = systable_beginscan(relResGroup, InvalidOid, false, NULL, 0, NULL);
		while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
		{
			/* for each row */
			Datum		values[PG_RESGROUP_CLEAN_SLOT_COLS] = {0};
			bool		nulls[PG_RESGROUP_CLEAN_SLOT_COLS] = {0};
			bool		isnull;
			char		*tupleGroupName = NameStr(*DatumGetName(heap_getattr(tuple,
																		Anum_pg_resgroup_rsgname,
																		relResGroup->rd_att,
																		&isnull)));

			if (groupname != NULL && strcmp(groupname, tupleGroupName) == 0)
			{
				values[0] = CStringGetTextDatum(tupleGroupName);
				values[1] = CStringGetTextDatum(PGXCNodeName);
				values[2] = BoolGetDatum(resgroupCleanSlot(tupleGroupName));

				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				break;
			}

			values[0] = CStringGetTextDatum(tupleGroupName);
			values[1] = CStringGetTextDatum(PGXCNodeName);
			values[2] = BoolGetDatum(resgroupCleanSlot(tupleGroupName));

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}

		systable_endscan(sscan);

		relation_close(relResGroup, ExclusiveLock);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	/* Returns the record as Datum */
	return (Datum) 0;
}

static void
pg_resgroup_get_global_clean_slot(const char *groupName, bool coord, ReturnSetInfo *rsinfo)
{
#define QUERY_LEN 1024
	char		query[QUERY_LEN];

	if (groupName == NULL)
		sprintf(query, "select rsgname, nodename, cleaned from pg_resgroup_clean_slot(NULL) order by rsgname, nodename;");
	else
		sprintf(query, "select rsgname, nodename, cleaned from pg_resgroup_clean_slot('%s') order by rsgname, nodename;", groupName);

	pg_stat_get_global_stat(query, coord, rsinfo);
}

Datum
pg_resgroup_cluster_clean_slot(PG_FUNCTION_ARGS)
{
	const char		*groupname = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
	bool			coordonly = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;

	if (!IS_PGXC_COORDINATOR)
		elog(ERROR, "pg_resgroup_cluster_clean_slot only support on cn");

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* switch to query's memory context to save results during execution */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* dispatch query to remote if needed */
	pg_resgroup_get_global_clean_slot(groupname, true, rsinfo);
	if (!coordonly)
		pg_resgroup_get_global_clean_slot(groupname, false, rsinfo);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
