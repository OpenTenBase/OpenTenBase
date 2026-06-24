/*-------------------------------------------------------------------------
 *
 * resgroup-ops.h
 *	  opentenbase resource group definitions.
 *
 * IDENTIFICATION
 *	    src/include/utils/resgroup-ops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RES_GROUP_OPS_H
#define RES_GROUP_OPS_H

/*
 * Resource Group underlying component types.
 */
typedef enum
{
	RESGROUP_COMP_TYPE_FIRST			= 0,
	RESGROUP_COMP_TYPE_UNKNOWN			= -1,

	RESGROUP_COMP_TYPE_CPU,
	RESGROUP_COMP_TYPE_CPUACCT,
	RESGROUP_COMP_TYPE_CPUSET,

	RESGROUP_COMP_TYPE_COUNT,
} ResGroupCompType;

#define RESGROUP_ROOT_PATH NULL

/*
 * Default cpuset group is a group manages the cpu cores which not belong to
 * any other cpuset group. All the processes which not belong to any cpuset
 * group will be run on cores in default cpuset group. It is a virtual group,
 * can't be seen in opentenbase.
 */
#define DEFAULT_CPUSET_GROUP_PATH "1"
/*
 * If cpu_rate_limit is set to this value, it means this feature is disabled
 */
#define CPU_RATE_LIMIT_DISABLED (-1)

/*
 * Interfaces for OS dependent operations
 */

extern const char *ResGroupOps_Name(void);
extern bool ResGroupOps_Probe(void);
extern void ResGroupOps_Bless(void);
extern void ResGroupOps_Init(void);
//extern void ResGroupOps_AdjustGUCs(void);
extern void ResGroupOps_CreateGroup(char *group);
extern void ResGroupOps_DestroyGroup(const char * group, bool migrate);
extern void ResGroupOps_AssignGroup(char *group, ResGroupCaps *caps, int pid);
extern int ResGroupOps_LockGroup(char *group, ResGroupCompType comp, bool block);
extern void ResGroupOps_UnLockGroup(char *group, int fd);
extern void ResGroupOps_SetCpuRateLimit(const char *group, int cpu_rate_limit);
extern int64 ResGroupOps_GetCpuUsage(char *group);
//extern int32 ResGroupOps_GetMemoryUsage(Oid group);
//extern int32 ResGroupOps_GetMemoryLimit(Oid group);
extern int ResGroupOps_GetCpuCores(void);
extern void ResGroupOps_SetCpuSet(const char *group, const char *cpuset);
extern void ResGroupOps_GetCpuSet(char *group, char *cpuset, int len);
float ResGroupOps_ConvertCpuUsageToPercent(int64 usage, int64 duration);

#endif   /* RES_GROUP_OPS_H */
