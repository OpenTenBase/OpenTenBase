/*-------------------------------------------------------------------------
 *
 * resgroup-ops-dummy.c
 *	  OS dependent resource group operations - dummy implementation
 *
 * IDENTIFICATION
 *	    src/backend/utils/resgroup/resgroup-ops-dummy.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/resgroup.h"
#include "utils/resgroup-ops.h"

/*
 * Interfaces for OS dependent operations.
 *
 * Resource group relies on OS dependent group implementation to manage
 * resources like cpu usage, such as cgroup on Linux system.
 * We call it OS group in below function description.
 *
 * So far these operations are mainly for CPU rate limitation and accounting.
 */

#define unsupported_system() \
	elog(WARNING, "resource group is not supported on this system")

/* Return the name for the OS group implementation */
const char *
ResGroupOps_Name(void)
{
	return "unsupported";
}

/*
 * Probe the configuration for the OS group implementation.
 *
 * Return true if everything is OK, or false is some requirements are not
 * satisfied.  Will not fail in either case.
 */
bool
ResGroupOps_Probe(void)
{
	return false;
}

/* Check whether the OS group implementation is available and useable */
void
ResGroupOps_Bless(void)
{
	unsupported_system();
}

/* Initialize the OS group */
void
ResGroupOps_Init(void)
{
	unsupported_system();
}

/* Adjust GUCs for this OS group implementation */
//void
//ResGroupOps_AdjustGUCs(void)
//{
//	unsupported_system();
//}

/*
 * Create the OS group for group.
 */
void
ResGroupOps_CreateGroup(char *group)
{
	unsupported_system();
}

/*
 * Destroy the OS group for group.
 *
 * One OS group can not be dropped if there are processes running under it,
 * if migrate is true these processes will be moved out automatically.
 */
void
ResGroupOps_DestroyGroup(char *group, bool migrate)
{
	unsupported_system();
}

/*
 * Assign a process to the OS group. A process can only be assigned to one
 * OS group, if it's already running under other OS group then it'll be moved
 * out that OS group.
 *
 * pid is the process id.
 */
void
ResGroupOps_AssignGroup(char *group, ResGroupCaps *caps, int pid)
{
	unsupported_system();
}

/*
 * Lock the OS group. While the group is locked it won't be removed by other
 * processes.
 *
 * This function would block if block is true, otherwise it return with -1
 * immediately.
 *
 * On success it return a fd to the OS group, pass it to
 * ResGroupOps_UnLockGroup() to unlock it.
 */
int
ResGroupOps_LockGroup(char *group, ResGroupCompType comp, bool block)
{
	unsupported_system();
	return -1;
}

/*
 * Unblock a OS group.
 *
 * fd is the value returned by ResGroupOps_LockGroup().
 */
void
ResGroupOps_UnLockGroup(char *group, int fd)
{
	unsupported_system();
}

/*
 * Set the cpu rate limit for the OS group.
 *
 * cpu_rate_limit should be within [0, 100].
 */
void
ResGroupOps_SetCpuRateLimit(char *group, int cpu_rate_limit)
{
	unsupported_system();
}

/*
 * Get the cpu usage of the OS group, that is the total cpu time obtained
 * by this OS group, in nano seconds.
 */
int64
ResGroupOps_GetCpuUsage(char *group)
{
	unsupported_system();
	return 0;
}

/*
 * Get the count of cpu cores on the system.
 */
int
ResGroupOps_GetCpuCores(void)
{
	unsupported_system();
	return 1;
}

/*
 * Get the total memory on the system.
 * (total RAM * overcommit_ratio + total Swap)
 */
int
ResGroupOps_GetTotalMemory(void)
{
	unsupported_system();
	return 0;
}

/*
 * Set the cpuset for the OS group.
 * @param group: the destination group
 * @param cpuset: the value to be set
 * The syntax of CPUSET is a combination of the tuples, each tuple represents
 * one core number or the core numbers interval, separated by comma.
 * E.g. 0,1,2-3.
 */
void
ResGroupOps_SetCpuSet(char *group, const char *cpuset)
{
	unsupported_system();
}

/*
 * Get the cpuset of the OS group.
 * @param group: the destination group
 * @param cpuset: the str to be set
 * @param len: the upper limit of the str
 */
void
ResGroupOps_GetCpuSet(char *group, char *cpuset, int len)
{
	unsupported_system();
}

/*
 * Convert the cpu usage to percentage within the duration.
 *
 * usage is the delta of GetCpuUsage() of a duration,
 * duration is in micro seconds.
 *
 * When fully consuming one cpu core the return value will be 100.0 .
 */
float
ResGroupOps_ConvertCpuUsageToPercent(int64 usage, int64 duration)
{
	unsupported_system();
	return 0.0;
}
