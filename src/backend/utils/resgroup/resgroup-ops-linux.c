/*-------------------------------------------------------------------------
 *
 * resgroup-ops-linux.c
 *	  OS dependent resource group operations - cgroup implementation
 *
 * IDENTIFICATION
 *	    src/backend/utils/resgroup/resgroup-ops-linux.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

//#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/resgroup.h"
#include "utils/resgroup-ops.h"
//#include "utils/vmem_tracker.h"

#ifndef __linux__
#error  cgroup is only available on linux
#endif

#include <fcntl.h>
#include <unistd.h>
#include <sched.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <stdio.h>
#include <mntent.h>

/*
 * Interfaces for OS dependent operations.
 *
 * Resource group relies on OS dependent group implementation to manage
 * resources like cpu usage, such as cgroup on Linux system.
 * We call it OS group in below function description.
 *
 * So far these operations are mainly for CPU rate limitation and accounting.
 */

#define CGROUP_ERROR(...) elog(ERROR, __VA_ARGS__)
#define CGROUP_CONFIG_ERROR(...) \
	CGROUP_ERROR("cgroup is not properly configured: " __VA_ARGS__)

#define FALLBACK_COMP_DIR ""
#define PROC_MOUNTS "/proc/self/mounts"
#define MAX_INT_STRING_LEN 20
#define MAX_RETRY 10

typedef enum BaseType BaseType;
typedef struct PermItem PermItem;
typedef struct PermList PermList;

enum BaseType
{
	BASETYPE_OPENTENBASE,		/* translate to "/opentenbase" */
	BASETYPE_PARENT,	/* translate to "" */
};

struct PermItem
{
	ResGroupCompType comp;
	const char	*prop;
	int			perm;
};

struct PermList
{
	const PermItem	*items;
	bool			optional;
	bool			*presult;
};

#define foreach_perm_list(i, lists) \
	for ((i) = 0; (lists)[(i)].items; (i)++)

#define foreach_perm_item(i, items) \
	for ((i) = 0; (items)[(i)].comp != RESGROUP_COMP_TYPE_UNKNOWN; (i)++)

#define foreach_comp_type(comp) \
	for ((comp) = RESGROUP_COMP_TYPE_FIRST; \
		 (comp) < RESGROUP_COMP_TYPE_COUNT; \
		 (comp)++)

static const char *compGetName(ResGroupCompType comp);
static ResGroupCompType compByName(const char *name);
static const char *compGetDir(ResGroupCompType comp);
static void compSetDir(ResGroupCompType comp, const char *dir);
static void detectCompDirs(void);
static bool validateCompDir(ResGroupCompType comp);
static void dumpCompDirs(void);

static char *buildPath(const char *group, BaseType base, ResGroupCompType comp, const char *prop, char *path, size_t pathsize);
static char *buildPathSafe(const char *group, BaseType base, ResGroupCompType comp, const char *prop, char *path, size_t pathsize);
static int lockDir(const char *path, bool block);
static void unassignGroup(const char *group, ResGroupCompType comp, int fddir);
static bool createDir(const char *group, ResGroupCompType comp);
static bool removeDir(const char *group, ResGroupCompType comp, const char *prop, bool unassign);
static int getCpuCores(void);
static size_t readData(const char *path, char *data, size_t datasize);
static void writeData(const char *path, const char *data, size_t datasize);
static int64 readInt64(const char *group, BaseType base, ResGroupCompType comp, const char *prop);
static void writeInt64(const char *group, BaseType base, ResGroupCompType comp, const char *prop, int64 x);
static void readStr(const char *group, BaseType base, ResGroupCompType comp, const char *prop, char *str, int len);
static void writeStr(const char *group, BaseType base, ResGroupCompType comp, const char *prop, const char *strValue);
static bool permListCheck(const PermList *permlist, const char *group, bool report);
static bool checkPermission(const char *group, bool report);
static bool checkCpuSetPermission(const char *group, bool report);
static void checkCompHierarchy();
static bool detectCgroupMountPoint(void);
static void initCpu(void);
static void initCpuSet(void);
static void createDefaultCpuSetGroup(void);
static int64 getCfsPeriodUs(ResGroupCompType);

/*
 * currentGroupIdInCGroup & oldCaps are used for reducing redundant
 * file operations
 */
static char currentGroupNameInCGroup[NAMEDATALEN] = "";
static ResGroupCaps oldCaps;

static char cgdir[MAXPATHLEN];

static int64 system_cfs_quota_us = -1LL;
static int64 parent_cfs_quota_us = -1LL;
static int ncores;

static const PermItem perm_items_cpu[] =
{
	{ RESGROUP_COMP_TYPE_CPU, "", R_OK | W_OK | X_OK },
	{ RESGROUP_COMP_TYPE_CPU, "cgroup.procs", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_CPU, "cpu.cfs_period_us", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_CPU, "cpu.cfs_quota_us", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_CPU, "cpu.shares", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_UNKNOWN, NULL, 0 }
};
static const PermItem perm_items_cpu_acct[] =
{
	{ RESGROUP_COMP_TYPE_CPUACCT, "", R_OK | W_OK | X_OK },
	{ RESGROUP_COMP_TYPE_CPUACCT, "cgroup.procs", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_CPUACCT, "cpuacct.usage", R_OK },
	{ RESGROUP_COMP_TYPE_CPUACCT, "cpuacct.stat", R_OK },
	{ RESGROUP_COMP_TYPE_UNKNOWN, NULL, 0 }
};
static const PermItem perm_items_cpuset[] =
{
	{ RESGROUP_COMP_TYPE_CPUSET, "", R_OK | W_OK | X_OK },
	{ RESGROUP_COMP_TYPE_CPUSET, "cgroup.procs", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_CPUSET, "cpuset.cpus", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_CPUSET, "cpuset.mems", R_OK | W_OK },
	{ RESGROUP_COMP_TYPE_UNKNOWN, NULL, 0 }
};

/*
 * just for cpuset check, same as the cpuset Permlist in permlists
 */
static const PermList cpusetPermList =
{
	perm_items_cpuset,
	false,
	&resource_group_enable_cgroup_cpuset,
};

/*
 * Permission groups.
 */
static const PermList permlists[] =
{
	/* cpu/cpuacct permissions are mandatory */
	{ perm_items_cpu, false, NULL },
	{ perm_items_cpu_acct, false, NULL },

	/*
	 * cpuset permissions are mandatory
	 */
	{ perm_items_cpuset, false, &resource_group_enable_cgroup_cpuset},

	{ NULL, false, NULL }
};

/*
 * Comp names.
 */
const char *compnames[RESGROUP_COMP_TYPE_COUNT] =
{
	"cpu", "cpuacct", "cpuset"
};

/*
 * Comp dirs.
 */
char compdirs[RESGROUP_COMP_TYPE_COUNT][MAXPATHLEN] =
{
	FALLBACK_COMP_DIR, FALLBACK_COMP_DIR, FALLBACK_COMP_DIR
};

/*
 * Get the name of comp.
 */
static const char *
compGetName(ResGroupCompType comp)
{
	Assert(comp > RESGROUP_COMP_TYPE_UNKNOWN);
	Assert(comp < RESGROUP_COMP_TYPE_COUNT);

	return compnames[comp];
}

/*
 * Get the comp type from name.
 */
static ResGroupCompType
compByName(const char *name)
{
	ResGroupCompType comp;

	for (comp = 0; comp < RESGROUP_COMP_TYPE_COUNT; comp++)
		if (strcmp(name, compGetName(comp)) == 0)
			return comp;

	return RESGROUP_COMP_TYPE_UNKNOWN;
}

/*
 * Get the comp dir of comp.
 */
static const char *
compGetDir(ResGroupCompType comp)
{
	Assert(comp > RESGROUP_COMP_TYPE_UNKNOWN);
	Assert(comp < RESGROUP_COMP_TYPE_COUNT);

	return compdirs[comp];
}

/*
 * Set the comp dir of comp.
 */
static void
compSetDir(ResGroupCompType comp, const char *dir)
{
	Assert(comp > RESGROUP_COMP_TYPE_UNKNOWN);
	Assert(comp < RESGROUP_COMP_TYPE_COUNT);
	Assert(strlen(dir) < MAXPATHLEN);

	strcpy(compdirs[comp], dir);
}

/*
 * Detect opentenbase cgroup component dirs.
 *
 * Take cpu for example, by default we expect opentenbase dir to locate at
 * cgroup/cpu/opentenbase.  But we'll also check for the cgroup dirs of init process
 * (pid 1), e.g. cgroup/cpu/custom, then we'll look for opentenbase dir at
 * cgroup/cpu/custom/opentenbase, if it's found and has good permissions, it can be
 * used instead of the default one.
 *
 * If any of the opentenbase cgroup component dir can not be found under init process'
 * cgroup dirs or has bad permissions we'll fallback all the opentenbase cgroup
 * component dirs to the default ones.
 *
 * NOTE: This auto detection will look for memory & cpuset opentenbase dirs even on
 * 5X.
 */
static void
detectCompDirs(void)
{
	ResGroupCompType comp;
	FILE	   *f;
	char		buf[MAXPATHLEN * 2];
	int			maskAll = (1 << RESGROUP_COMP_TYPE_COUNT) - 1;
	int			maskDetected = 0;

	f = fopen("/proc/1/cgroup", "r");
	if (!f)
		goto fallback;

	/*
	 * format: id:comps:path, e.g.:
	 *
	 *     10:cpuset:/
	 *     4:cpu,cpuacct:/
	 *     1:name=systemd:/init.scope
	 *     0::/init.scope
	 */
	while (fscanf(f, "%*d:%s", buf) != EOF)
	{
		ResGroupCompType comps[RESGROUP_COMP_TYPE_COUNT];
		int			ncomps = 0;
		char	   *ptr;
		char	   *tmp;
		char		sep = '\0';
		int			i;

		/* buf is stored with "comps:path" */

		if (buf[0] == ':')
			continue; /* ignore empty comp */

		/* split comps */
		for (ptr = buf; sep != ':'; ptr = tmp)
		{
			tmp = strpbrk(ptr, ":,=");

			sep = *tmp;
			*tmp++ = 0;

			/* for name=comp case there is nothing to do with the name */
			if (sep == '=')
				continue;

			comp = compByName(ptr);

			if (comp == RESGROUP_COMP_TYPE_UNKNOWN)
				continue; /* not used by us */

			/*
			 * push the comp to the comps stack, but if the stack is already
			 * full (which is unlikely to happen in real world), simply ignore
			 * it.
			 */
			if (ncomps < RESGROUP_COMP_TYPE_COUNT)
				comps[ncomps++] = comp;
		}

		/* now ptr point to the path */
		Assert(strlen(ptr) < MAXPATHLEN);

		/* if the path is "/" then use empty string "" instead of it */
		if (strcmp(ptr, "/") == 0)
			ptr[0] = '\0';

		/* validate and set path for the comps */
		for (i = 0; i < ncomps; i++)
		{
			comp = comps[i];
			compSetDir(comp, ptr);

			if (!validateCompDir(comp))
				goto fallback; /* dir missing or bad permissions */

			if (maskDetected & (1 << comp))
				goto fallback; /* comp are detected more than once */

			maskDetected |= 1 << comp;
		}
	}

	if (maskDetected != maskAll)
		goto fallback; /* not all the comps are detected */

	/*
	 * Dump the comp dirs for debugging?  No!
	 * This function is executed before timezone initialization, logs are
	 * forbidden.
	 */

	fclose(f);
	return;

fallback:
	/* set the fallback dirs for all the comps */
	foreach_comp_type(comp)
	{
		compSetDir(comp, FALLBACK_COMP_DIR);
	}

	fclose(f);
}

/*
 * Validate a comp dir.
 *
 * Return True if it exists and has good permissions,
 * return False otherwise.
 */
static bool
validateCompDir(ResGroupCompType comp)
{
	char		path[MAXPATHLEN];
	size_t		pathsize = sizeof(path);

	if (!buildPathSafe(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "",
					   path, pathsize))
		return false;

	return access(path, R_OK | W_OK | X_OK) == 0;
}

/*
 * Dump comp dirs.
 */
static void
dumpCompDirs(void)
{
	ResGroupCompType comp;
	char		path[MAXPATHLEN];
	size_t		pathsize = sizeof(path);

	foreach_comp_type(comp)
	{
		buildPath(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "", path, pathsize);

		elog(LOG, "opentenbase dir for cgroup component \"%s\": %s",
			 compGetName(comp), path);
	}
}

/*
 * Build path string with parameters.
 *
 * Will raise an exception if the path buffer is not large enough.
 *
 * Refer to buildPathSafe() for details.
 */
static char *
buildPath(const char *group,
		  BaseType base,
		  ResGroupCompType comp,
		  const char *prop,
		  char *path,
		  size_t pathsize)
{
	char	   *result = buildPathSafe(group, base, comp, prop, path, pathsize);

	if (!result)
	{
		CGROUP_CONFIG_ERROR("invalid %s name '%s': %m",
							prop[0] ? "file" : "directory",
							path);
	}

	return result;
}

/*
 * Build path string with parameters.
 *
 * Return NULL if the path buffer is not large enough, errno will also be set.
 *
 * Examples (path and pathsize are omitted):
 * - buildPath(ROOT, PARENT, CPU, ""     ): /sys/fs/cgroup/cpu
 * - buildPath(ROOT, PARENT, CPU, "tasks"): /sys/fs/cgroup/cpu/tasks
 * - buildPath(ROOT, OPENTENBASE, CPU, "tasks"): /sys/fs/cgroup/cpu/opentenbase/tasks
 * - buildPath(6437, OPENTENBASE, CPU, "tasks"): /sys/fs/cgroup/cpu/opentenbase/6437/tasks
 */
static char *
buildPathSafe(const char *group,
			  BaseType base,
			  ResGroupCompType comp,
			  const char *prop,
			  char *path,
			  size_t pathsize)
{
	const char *compname = compGetName(comp);
	const char *compdir = compGetDir(comp);
	char		basedir[MAXPATHLEN] = "";
	char		groupdir[MAXPATHLEN] = "";
	int			len;

	Assert(cgdir[0] != 0);
	Assert(base == BASETYPE_OPENTENBASE ||
		   base == BASETYPE_PARENT);

	if (base == BASETYPE_OPENTENBASE)
	{
		len = snprintf(basedir, sizeof(basedir), "/%s", resource_group_name);
		/* We are sure basedir is large enough */
		Assert(len > 0 &&
			   len < sizeof(basedir));
	}

	if (group != RESGROUP_ROOT_PATH)
	{
		len = snprintf(groupdir, sizeof(groupdir), "/%s", group);
		/* We are sure groupdir is large enough */
		Assert(len > 0 &&
			   len < sizeof(groupdir));
	}

	len = snprintf(path, pathsize, "%s/%s%s%s%s/%s",
				   cgdir, compname, compdir, basedir, groupdir, prop);
	if (len >= pathsize || len < 0)
	{
		errno = ENAMETOOLONG;
		return NULL;
	}

	return path;
}

/*
 * Unassign all the processes from group.
 *
 * These processes will be moved to the opentenbase toplevel cgroup.
 *
 * This function must be called with the opentenbase toplevel dir locked,
 * fddir is the fd for this lock, on any failure fddir will be closed
 * (and unlocked implicitly) then an error is raised.
 */
static void
unassignGroup(const char *group, ResGroupCompType comp, int fddir)
{
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);
	char *buf;
	size_t bufsize;
	const size_t bufdeltasize = 512;
	size_t buflen = -1;
	int fdr = -1;
	int fdw = -1;
	char *ptr = NULL;
	char *end = NULL;
	long pid;
	char str[22];
	int n;

	/*
	 * Check an operation result on path.
	 *
	 * Operation can be open(), close(), read(), write(), etc., which must
	 * set the errno on error.
	 *
	 * - condition describes the expected result of the operation;
	 * - action is the cleanup action on failure, such as closing the fd,
	 *   multiple actions can be specified by putting them in brackets,
	 *   such as (op1, op2);
	 * - message describes what's failed;
	 */
#define __CHECK(condition, action, message) do { \
	if (!(condition)) \
	{ \
		/* save errno in case it's changed in actions */ \
		int err = errno; \
		action; \
		CGROUP_ERROR(message ": %s: %s", path, strerror(err)); \
	} \
} while (0)

	buildPath(group, BASETYPE_OPENTENBASE, comp, "cgroup.procs", path, pathsize);

	fdr = open(path, O_RDONLY);
	__CHECK(fdr >= 0, ( close(fddir) ), "can't open file for read");

	buflen = 0;
	bufsize = bufdeltasize;
	buf = palloc(bufsize);

	while (1)
	{
		int n = read(fdr, buf + buflen, bufdeltasize);
		__CHECK(n >= 0, ( close(fdr), close(fddir) ), "can't read from file");

		buflen += n;

		if (n < bufdeltasize)
			break;

		bufsize += bufdeltasize;
		buf = repalloc(buf, bufsize);
	}

	close(fdr);
	if (buflen == 0)
		return;

	buildPath(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cgroup.procs",
			  path, pathsize);

	fdw = open(path, O_WRONLY);
	__CHECK(fdw >= 0, ( close(fddir) ), "can't open file for write");

	ptr = buf;

	/*
	 * as required by cgroup, only one pid can be migrated in each single
	 * write() call, so we have to parse the pids from the buffer first,
	 * then write them one by one.
	 */
	while (1)
	{
		pid = strtol(ptr, &end, 10);
		__CHECK(pid != LONG_MIN && pid != LONG_MAX,
				( close(fdw), close(fddir) ),
				"can't parse pid");

		if (ptr == end)
			break;

		sprintf(str, "%ld", pid);
		n = write(fdw, str, strlen(str));
		if (n < 0)
		{
			elog(LOG, "failed to migrate pid to opentenbase root cgroup: pid=%ld: %m",
				 pid);
		}
		else
		{
			__CHECK(n == strlen(str),
					( close(fdw), close(fddir) ),
					"can't write to file");
		}

		ptr = end;
	}

	close(fdw);

#undef __CHECK
}

/*
 * Lock the dir specified by path.
 *
 * - path must be a dir path;
 * - if block is true then lock in block mode, otherwise will give up if
 *   the dir is already locked;
 */
static int
lockDir(const char *path, bool block)
{
	int fddir;
	int flags;
	int err;

	fddir = open(path, O_RDONLY);
	if (fddir < 0)
	{
		if (errno == ENOENT)
		{
			/* the dir doesn't exist, nothing to do */
			return -1;
		}

		CGROUP_ERROR("can't open dir to lock: %s: %m", path);
	}

	flags = LOCK_EX;
	if (!block)
		flags |= LOCK_NB;

	while (flock(fddir, flags))
	{
		/*
		 * EAGAIN is not described in flock(2),
		 * however it does appear in practice.
		 */
		if (errno == EAGAIN)
			continue;

		err = errno;
		close(fddir);

		/*
		 * In block mode all errors should be reported;
		 * In non block mode only report errors != EWOULDBLOCK.
		 */
		if (block || err != EWOULDBLOCK)
			CGROUP_ERROR("can't lock dir: %s: %s", path, strerror(err));
		return -1;
	}

	/*
	 * Even if we accquired the lock the dir may still been removed by other
	 * processes, e.g.:
	 *
	 * 1: open()
	 * 1: flock() -- process 1 accquired the lock
	 *
	 * 2: open()
	 * 2: flock() -- blocked by process 1
	 *
	 * 1: rmdir()
	 * 1: close() -- process 1 released the lock
	 *
	 * 2:flock() will now return w/o error as process 2 still has a valid
	 * fd (reference) on the target dir, and process 2 does accquired the lock
	 * successfully. However as the dir is already removed so process 2
	 * shouldn't make any further operation (rmdir(), etc.) on the dir.
	 *
	 * So we check for the existence of the dir again and give up if it's
	 * already removed.
	 */
	if (access(path, F_OK))
	{
		/* the dir is already removed by other process, nothing to do */
		close(fddir);
		return -1;
	}

	return fddir;
}

/*
 * Create the cgroup dir for group.
 */
static bool
createDir(const char *group, ResGroupCompType comp)
{
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);

	buildPath(group, BASETYPE_OPENTENBASE, comp, "", path, pathsize);

	if (mkdir(path, 0755) && errno != EEXIST)
		return false;

	return true;
}

/*
 * Remove the cgroup dir for group.
 *
 * - if unassign is true then unassign all the processes first before removal;
 */
static bool
removeDir(const char *group, ResGroupCompType comp, const char *prop, bool unassign)
{
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);
	int retry = unassign ? 0 : MAX_RETRY - 1;
	int fddir;

	buildPath(group, BASETYPE_OPENTENBASE, comp, "", path, pathsize);

	/*
	 * To prevent race condition between multiple processes we require a dir
	 * to be removed with the lock accquired first.
	 */
	fddir = lockDir(path, true);
	if (fddir < 0)
	{
		/* the dir is already removed */
		return true;
	}

	/*
	 * Reset the corresponding control file to zero
	 */
	if (prop)
		writeInt64(group, BASETYPE_OPENTENBASE, comp, prop, 0);

	while (++retry <= MAX_RETRY)
	{
		if (unassign)
			unassignGroup(group, comp, fddir);

		if (rmdir(path))
		{
			int err = errno;

			if (err == EBUSY && unassign && retry < MAX_RETRY)
			{
				elog(DEBUG1, "can't remove dir, will retry: %s: %s",
					 path, strerror(err));
				pg_usleep(1000);
				continue;
			}

			/*
			 * we don't check for ENOENT again as we already accquired the lock
			 * on this dir and the dir still exist at that time, so if then
			 * it's removed by other processes then it's a bug.
			 */
			elog(DEBUG1, "can't remove dir, ignore the error: %s: %s",
				 path, strerror(err));
		}
		break;
	}

	if (retry <= MAX_RETRY)
		elog(DEBUG1, "cgroup dir '%s' removed", path);

	/* close() also releases the lock */
	close(fddir);

	return true;
}

/*
 * Get the cpu cores assigned for current system or container.
 *
 * Suppose a physical machine has 8 cpu cores, 2 of them assigned to
 * a container, then the return value is:
 * - 8 if running directly on the machine;
 * - 2 if running in the container;
 */
static int
getCpuCores(void)
{
	int cpucores = 0;

	/*
	 * cpuset ops requires _GNU_SOURCE to be defined,
	 * and _GNU_SOURCE is forced on in src/template/linux,
	 * so we assume these ops are always available on linux.
	 */
	cpu_set_t cpuset;
	int i;

	if (sched_getaffinity(0, sizeof(cpuset), &cpuset) < 0)
		CGROUP_ERROR("can't get cpu cores: %m");

	for (i = 0; i < CPU_SETSIZE; i++)
	{
		if (CPU_ISSET(i, &cpuset))
			cpucores++;
	}

	if (cpucores == 0)
		CGROUP_ERROR("can't get cpu cores");

	return cpucores;
}

/*
 * Read at most datasize bytes from a file.
 */
static size_t
readData(const char *path, char *data, size_t datasize)
{
	ssize_t ret;
	int err;
	int fd = open(path, O_RDONLY);
	if (fd < 0)
		elog(ERROR, "can't open file '%s': %m", path);

	ret = read(fd, data, datasize);

	/* save errno before close() */
	err = errno;
	close(fd);

	if (ret < 0)
		elog(ERROR, "can't read data from file '%s': %s", path, strerror(err));

	return ret;
}

/*
 * Write datasize bytes to a file.
 */
static void
writeData(const char *path, const char *data, size_t datasize)
{
	ssize_t ret;
	int err;
	int fd = open(path, O_WRONLY);
	if (fd < 0)
		elog(ERROR, "can't open file '%s': %m", path);

	ret = write(fd, data, datasize);

	/* save errno before close */
	err = errno;
	close(fd);

	if (ret < 0)
		elog(ERROR, "can't write data to file '%s': %s", path, strerror(err));
	if (ret != datasize)
		elog(ERROR, "can't write all data to file '%s'", path);
}

/*
 * Read an int64 value from a cgroup interface file.
 */
static int64
readInt64(const char *group, BaseType base, ResGroupCompType comp, const char *prop)
{
	int64 x;
	char data[MAX_INT_STRING_LEN];
	size_t datasize = sizeof(data);
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);

	buildPath(group, base, comp, prop, path, pathsize);

	readData(path, data, datasize);

	if (sscanf(data, "%lld", (long long *) &x) != 1)
		CGROUP_ERROR("invalid number '%s' when reading from '%s'", data, path);

	return x;
}

/*
 * Write an int64 value to a cgroup interface file.
 */
static void
writeInt64(const char *group, BaseType base,
		   ResGroupCompType comp, const char *prop, int64 x)
{
	char data[MAX_INT_STRING_LEN];
	size_t datasize = sizeof(data);
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);

	buildPath(group, base, comp, prop, path, pathsize);
	snprintf(data, datasize, "%lld", (long long) x);

	writeData(path, data, strlen(data));
}

/*
 * Read a string value from a cgroup interface file.
 */
static void
readStr(const char *group, BaseType base,
		ResGroupCompType comp, const char *prop, char *str, int len)
{
	char data[MAX_INT_STRING_LEN];
	size_t datasize = sizeof(data);
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);

	buildPath(group, base, comp, prop, path, pathsize);

	readData(path, data, datasize);

	StrNCpy(str, data, len > MAX_INT_STRING_LEN ? MAX_INT_STRING_LEN : len);
}

/*
 * Write an string value to a cgroup interface file.
 */
static void
writeStr(const char *group, BaseType base,
		 ResGroupCompType comp, const char *prop, const char *strValue)
{
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);

	buildPath(group, base, comp, prop, path, pathsize);
	writeData(path, strValue, strlen(strValue));
}

/*
 * Check a list of permissions on group.
 *
 * - if all the permissions are met then return true;
 * - otherwise:
 *   - raise an error if report is true and permlist is not optional;
 *   - or return false;
 */
static bool
permListCheck(const PermList *permlist, const char *group, bool report)
{
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);
	int i;

	if (group == RESGROUP_ROOT_PATH && permlist->presult)
		*permlist->presult = false;

	foreach_perm_item(i, permlist->items)
	{
		ResGroupCompType comp = permlist->items[i].comp;
		const char	*prop = permlist->items[i].prop;
		int			perm = permlist->items[i].perm;

		if (!buildPathSafe(group, BASETYPE_OPENTENBASE, comp, prop, path, pathsize))
		{
			/* Buffer is not large enough for the path */

			if (report && !permlist->optional)
			{
				CGROUP_CONFIG_ERROR("invalid %s name '%s': %m",
									prop[0] ? "file" : "directory",
									path);
			}
			return false;
		}

		if (access(path, perm))
		{
			/* No such file or directory / Permission denied */

			if (report && !permlist->optional)
			{
				CGROUP_CONFIG_ERROR("can't access %s '%s': %m",
									prop[0] ? "file" : "directory",
									path);
			}
			return false;
		}
	}

	if (group == RESGROUP_ROOT_PATH && permlist->presult)
		*permlist->presult = true;

	return true;
}

/*
 * Check permissions on group's cgroup dir & interface files.
 *
 * - if report is true then raise an error if any mandatory permission
 *   is not met;
 * - otherwise only return false;
 */
static bool
checkPermission(const char *group, bool report)
{
	int i;

	foreach_perm_list(i, permlists)
	{
		const PermList *permlist = &permlists[i];

		if (!permListCheck(permlist, group, report) && !permlist->optional)
			return false;
	}

	return true;
}

/*
 * Same as checkPermission, just check cpuset dir & interface files
 *
 */
static bool
checkCpuSetPermission(const char *group, bool report)
{
	if (!resource_group_enable_cgroup_cpuset)
		return true;

	if (!permListCheck(&cpusetPermList, group, report) &&
		!cpusetPermList.optional)
		return false;

	return true;
}

/*
 * Check the mount hierarchy of cpu and cpuset subsystem.
 *
 * Raise an error if cpu and cpuset are mounted on the same hierarchy.
 */
static void
checkCompHierarchy()
{
	ResGroupCompType comp;
	FILE       *f;
	char        buf[MAXPATHLEN * 2];
	
	f = fopen("/proc/1/cgroup", "r");
	if (!f)
	{
		CGROUP_CONFIG_ERROR("can't check component mount hierarchy \
					file '/proc/1/cgroup' doesn't exist");
		return;
	}

	/*
	 * format: id:comps:path, e.g.:
	 *
	 * 10:cpuset:/
	 * 4:cpu,cpuacct:/
	 * 1:name=systemd:/init.scope
	 * 0::/init.scope
	 */
	while (fscanf(f, "%*d:%s", buf) != EOF)
	{
		char       *ptr;
		char       *tmp;
		char        sep = '\0';
		/* mark if the line has alread contained cpu or cpuset comp */
		int        markComp = RESGROUP_COMP_TYPE_UNKNOWN;

		/* buf is stored with "comps:path" */
		if (buf[0] == ':')
			continue; /* ignore empty comp */

		/* split comps */
		for (ptr = buf; sep != ':'; ptr = tmp)
		{
			tmp = strpbrk(ptr, ":,=");
			
			sep = *tmp;
			*tmp++ = 0;

			/* for name=comp case there is nothing to do with the name */
			if (sep == '=')
				continue;
			
			comp = compByName(ptr);

			if (comp == RESGROUP_COMP_TYPE_UNKNOWN)
				continue; /* not used by us */
			
			if (comp == RESGROUP_COMP_TYPE_CPU || comp == RESGROUP_COMP_TYPE_CPUSET)
			{
				if (markComp == RESGROUP_COMP_TYPE_UNKNOWN)
					markComp = comp;
				else
				{
					Assert(markComp != comp);
					fclose(f);
					CGROUP_CONFIG_ERROR("can't mount 'cpu' and 'cpuset' on the same hierarchy");
					return;
				}
			}
		}
	}

	fclose(f);
}

/* detect cgroup mount point */
static bool
detectCgroupMountPoint(void)
{
	struct mntent *me;
	FILE *fp;

	if (cgdir[0])
		return true;

	fp = setmntent(PROC_MOUNTS, "r");
	if (fp == NULL)
		CGROUP_CONFIG_ERROR("can not open '%s' for read", PROC_MOUNTS);


	while ((me = getmntent(fp)))
	{
		char * p;

		if (strcmp(me->mnt_type, "cgroup"))
			continue;

		strncpy(cgdir, me->mnt_dir, sizeof(cgdir) - 1);

		p = strrchr(cgdir, '/');
		if (p == NULL)
			CGROUP_CONFIG_ERROR("cgroup mount point parse error: %s", cgdir);
		else
			*p = 0;
		break;
	}

	endmntent(fp);

	return !!cgdir[0];
}

/*
 * Init opentenbase cpu settings.
 *
 * Must be called after Probe() and Bless().
 */
static void
initCpu(void)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPU;
	int64		cfs_quota_us;
	int64		shares;

	/*
	 * CGroup promises that cfs_quota_us will never be 0, however on centos6
	 * we ever noticed that it has the value 0.
	 */
	if (parent_cfs_quota_us <= 0LL)
	{
		/*
		 * parent cgroup is unlimited, calculate opentenbase's limitation based on
		 * system hardware configuration.
		 *
		 * cfs_quota_us := parent.cfs_period_us * ncores * resource_group_cpu_limit
		 */
		cfs_quota_us = system_cfs_quota_us * resource_group_cpu_limit;
	}
	else
	{
		/*
		 * parent cgroup is also limited, then calculate opentenbase's limitation
		 * based on it.
		 *
		 * cfs_quota_us := parent.cfs_quota_us * resource_group_cpu_limit
		 */
		cfs_quota_us = parent_cfs_quota_us * resource_group_cpu_limit;
	}

	writeInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE,
			   comp, "cpu.cfs_quota_us", cfs_quota_us);

	/*
	 * shares := parent.shares * resource_group_cpu_priority
	 *
	 * We used to set a large shares (like 1024 * 256, the maximum possible
	 * value), it has very bad effect on overall system performance,
	 * especially on 1-core or 2-core low-end systems.
	 * Processes in a cold cgroup get launched and scheduled with large
	 * latency (a simple `cat a.txt` may executes for more than 100s).
	 * Here a cold cgroup is a cgroup that doesn't have active running
	 * processes, this includes not only the toplevel system cgroup,
	 * but also the inactive opentenbase resgroups.
	 */
	shares = readInt64(RESGROUP_ROOT_PATH, BASETYPE_PARENT, comp, "cpu.shares");
	shares = shares * resource_group_cpu_priority;

	writeInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE,
			   comp, "cpu.shares", shares);
}

/*
 * Init opentenbase cpuset settings.
 *
 * Must be called after Probe() and Bless().
 */
static void
initCpuSet(void)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPUSET;
	char		buffer[MaxCpuSetLength];

	if (!resource_group_enable_cgroup_cpuset)
		return;

	/*
	 * Get cpuset.mems and cpuset.cpus values from cgroup cpuset root path,
	 * and set them to cpuset/opentenbase/cpuset.mems and cpuset/opentenbase/cpuset.cpus
	 * to make sure that opentenbase directory configuration is same as its
	 * parent directory
	 */

	readStr(RESGROUP_ROOT_PATH, BASETYPE_PARENT, comp, "cpuset.mems",
			buffer, sizeof(buffer));
	writeStr(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.mems", buffer);

	readStr(RESGROUP_ROOT_PATH, BASETYPE_PARENT, comp, "cpuset.cpus",
			buffer, sizeof(buffer));
	writeStr(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.cpus", buffer);

	createDefaultCpuSetGroup();
}

static int64
getCfsPeriodUs(ResGroupCompType comp)
{
	int64		cfs_period_us;

	/*
	 * calculate cpu rate limit of system.
	 *
	 * Ideally the cpu quota is calculated from parent information:
	 *
	 * system_cfs_quota_us := parent.cfs_period_us * ncores.
	 *
	 * However on centos6 we found parent.cfs_period_us can be 0 and is not
	 * writable.  In the other side, opentenbase.cfs_period_us should be equal to
	 * parent.cfs_period_us because sub dirs inherit parent properties by
	 * default, so we read it instead.
	 */
	cfs_period_us = readInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE,
							  comp, "cpu.cfs_period_us");
	if (cfs_period_us == 0LL)
	{
		/*
		 * if opentenbase.cfs_period_us is also 0 try to correct it by setting the
		 * default value 100000 (100ms).
		 */
		writeInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE,
				   comp, "cpu.cfs_period_us", 100000LL);

		/* read again to verify the effect */
		cfs_period_us = readInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE,
								  comp, "cpu.cfs_period_us");
		if (cfs_period_us <= 0LL)
			CGROUP_CONFIG_ERROR("invalid cpu.cfs_period_us value: "
								INT64_FORMAT,
								cfs_period_us);
	}
	return cfs_period_us;
}

/* Return the name for the OS group implementation */
const char *
ResGroupOps_Name(void)
{
	return "cgroup";
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
	/*
	 * We only have to do these checks and initialization once on each host,
	 * so only let postmaster do the job.
	 */
	if (IsUnderPostmaster)
		return true;

	/*
	 * Ignore the error even if cgroup mount point can not be successfully
	 * probed, the error will be reported in Bless() later.
	 */
	if (!detectCgroupMountPoint())
		return false;

	detectCompDirs();

	/*
	 * Probe for optional features like the 'cgroup' memory auditor,
	 * do not raise any errors.
	 */
	if (!checkPermission(RESGROUP_ROOT_PATH, false))
		return false;

	return true;
}

/* Check whether the OS group implementation is available and useable */
void
ResGroupOps_Bless(void)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPU;
	int64		cfs_period_us;

	/*
	 * We only have to do these checks and initialization once on each host,
	 * so only let postmaster do the job.
	 */
	if (IsUnderPostmaster)
		return;

	/*
	 * We should have already detected for cgroup mount point in Probe(),
	 * it was not an error if the detection failed at that step.  But once
	 * we call Bless() we know we want to make use of cgroup then we must
	 * know the mount point, otherwise it's a critical error.
	 */
	if (!cgdir[0])
		CGROUP_CONFIG_ERROR("can not find cgroup mount point");

	/*
	 * Check again, this time we will fail on unmet requirements.
	 */
	checkPermission(RESGROUP_ROOT_PATH, true);

	/*
 	 * Check if cpu and cpuset subsystems are mounted on the same hierarchy.
 	 * We do not allow they mount on the same hierarchy, because writting pid
 	 * to DEFAULT_CPUSET_GROUP_ID in ResGroupOps_AssignGroup will cause the
 	 * removal of the pid in group BASETYPE_OPENTENBASE, which will make cpu usage
 	 * out of control.
	 */
	// if (!CGROUP_CPUSET_IS_OPTIONAL)
		checkCompHierarchy();

	/*
	 * Dump the cgroup comp dirs to logs.
	 * Check detectCompDirs() to know why this is not done in that function.
	 */
	dumpCompDirs();

	/*
	 * Get some necessary system information.
	 * We can not do them in Probe() as failure is not allowed in that one.
	 */

	/* get system cpu cores */
	ncores = getCpuCores();

	cfs_period_us = getCfsPeriodUs(comp);
	system_cfs_quota_us = cfs_period_us * ncores;

	/* read cpu rate limit of parent cgroup */
	parent_cfs_quota_us = readInt64(RESGROUP_ROOT_PATH, BASETYPE_PARENT,
									comp, "cpu.cfs_quota_us");
}

/* Initialize the OS group */
void
ResGroupOps_Init(void)
{
	initCpu();
	initCpuSet();

	/*
	 * Put postmaster and all the children processes into the opentenbase cgroup,
	 * otherwise auxiliary processes might get too low priority when
	 * resource_group_cpu_priority is set to a large value
	 */
	ResGroupOps_AssignGroup(RESGROUP_ROOT_PATH, NULL, PostmasterPid);
}

///* Adjust GUCs for this OS group implementation */
//void
//ResGroupOps_AdjustGUCs(void)
//{
//	/*
//	 * cgroup cpu limitation works best when all processes have equal
//	 * priorities, so we force all the segments and postmaster to
//	 * work with nice=0.
//	 *
//	 * this function should be called before GUCs are dispatched to segments.
//	 */
//	segworker_relative_priority = 0;
//}

/*
 * Create the OS group for group.
 */
void
ResGroupOps_CreateGroup(char *group)
{
	int retry = 0;

	if (!createDir(group, RESGROUP_COMP_TYPE_CPU) ||
		!createDir(group, RESGROUP_COMP_TYPE_CPUACCT) ||
		(resource_group_enable_cgroup_cpuset &&
		 !createDir(group, RESGROUP_COMP_TYPE_CPUSET)))
	{
		CGROUP_ERROR("can't create cgroup for resgroup '%s': %m", group);
	}

	/*
	 * although the group dir is created the interface files may not be
	 * created yet, so we check them repeatedly until everything is ready.
	 */
	while (++retry <= MAX_RETRY && !checkPermission(group, false))
		pg_usleep(1000);

	if (retry > MAX_RETRY)
	{
		/*
		 * still not ready after MAX_RETRY retries, might be a real error,
		 * raise the error.
		 */
		checkPermission(group, true);
	}

	if (resource_group_enable_cgroup_cpuset)
	{
		/*
		 * Initialize cpuset.mems and cpuset.cpus values as its parent directory
		 */
		ResGroupCompType comp = RESGROUP_COMP_TYPE_CPUSET;
		char buffer[MaxCpuSetLength];

		readStr(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.mems",
				buffer, sizeof(buffer));
		writeStr(group, BASETYPE_OPENTENBASE, comp, "cpuset.mems", buffer);

		readStr(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.cpus",
				buffer, sizeof(buffer));
		writeStr(group, BASETYPE_OPENTENBASE, comp, "cpuset.cpus", buffer);
	}
}

/*
 * Create the OS group for default cpuset group.
 * default cpuset group is a special group, only take effect in cpuset
 */
static void
createDefaultCpuSetGroup(void)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPUSET;
	int retry = 0;
	char buffer[MaxCpuSetLength];

	if (!createDir(DEFAULT_CPUSET_GROUP_PATH, comp))
	{
		CGROUP_ERROR("can't create cpuset cgroup for resgroup '%s': %m",
					 DEFAULT_CPUSET_GROUP_PATH);
	}

	/*
	 * although the group dir is created the interface files may not be
	 * created yet, so we check them repeatedly until everything is ready.
	 */
	while (++retry <= MAX_RETRY &&
		   !checkCpuSetPermission(DEFAULT_CPUSET_GROUP_PATH, false))
		pg_usleep(1000);

	if (retry > MAX_RETRY)
	{
		/*
		 * still not ready after MAX_RETRY retries, might be a real error,
		 * raise the error.
		 */
		checkCpuSetPermission(DEFAULT_CPUSET_GROUP_PATH, true);
	}

	/*
	 * Initialize cpuset.mems and cpuset.cpus in default group as its
	 * parent directory
	 */
	readStr(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.mems",
			buffer, sizeof(buffer));
	writeStr(DEFAULT_CPUSET_GROUP_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.mems", buffer);

	readStr(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.cpus",
			buffer, sizeof(buffer));
	writeStr(DEFAULT_CPUSET_GROUP_PATH, BASETYPE_OPENTENBASE, comp, "cpuset.cpus", buffer);
}

/*
 * Destroy the OS group for group.
 *
 * One OS group can not be dropped if there are processes running under it,
 * if migrate is true these processes will be moved out automatically.
 */
void
ResGroupOps_DestroyGroup(const char *group, bool migrate)
{
	if (!removeDir(group, RESGROUP_COMP_TYPE_CPU, "cpu.shares", migrate) ||
		!removeDir(group, RESGROUP_COMP_TYPE_CPUACCT, NULL, migrate) ||
		(resource_group_enable_cgroup_cpuset &&
		 !removeDir(group, RESGROUP_COMP_TYPE_CPUSET, NULL, migrate)))
	{
		CGROUP_ERROR("can't remove cgroup for resgroup '%s': %m", group);
	}
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
	bool oldViaCpuset = oldCaps.cpuRateLimit == CPU_RATE_LIMIT_DISABLED;
	bool curViaCpuset = caps ? caps->cpuRateLimit == CPU_RATE_LIMIT_DISABLED : false;

	/* needn't write to file if the pid has already been written in.
	 * Unless it has not been writtien or the group has changed or
	 * cpu control mechanism has changed */
	if (IsUnderPostmaster &&
		(group == NULL || strcmp(group, currentGroupNameInCGroup) == 0) &&
		caps != NULL &&
		oldViaCpuset == curViaCpuset)
		return;

	writeInt64(group, BASETYPE_OPENTENBASE, RESGROUP_COMP_TYPE_CPU,
			   "cgroup.procs", pid);
	writeInt64(group, BASETYPE_OPENTENBASE, RESGROUP_COMP_TYPE_CPUACCT,
			   "cgroup.procs", pid);

	if (resource_group_enable_cgroup_cpuset)
	{
		if (caps == NULL || !curViaCpuset)
		{
			/* add pid to default group */
			writeInt64(DEFAULT_CPUSET_GROUP_PATH, BASETYPE_OPENTENBASE,
					   RESGROUP_COMP_TYPE_CPUSET, "cgroup.procs", pid);
		}
		else
		{
			writeInt64(group, BASETYPE_OPENTENBASE,
					   RESGROUP_COMP_TYPE_CPUSET, "cgroup.procs", pid);
		}
	}

	/*
	 * Do not assign the process to cgroup/memory for now.
	 */

	if (group != NULL)
		strcpy(currentGroupNameInCGroup, group);
	if (caps != NULL)
	{
		oldCaps.cpuRateLimit = caps->cpuRateLimit;
		StrNCpy(oldCaps.cpuset, caps->cpuset, sizeof(oldCaps.cpuset));
	}
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
	char path[MAXPATHLEN];
	size_t pathsize = sizeof(path);

	buildPath(group, BASETYPE_OPENTENBASE, comp, "", path, pathsize);

	return lockDir(path, block);
}

/*
 * Unblock a OS group.
 *
 * fd is the value returned by ResGroupOps_LockGroup().
 */
void
ResGroupOps_UnLockGroup(char *group, int fd)
{
	if (fd >= 0)
		close(fd);
}

/*
 * Set the cpu rate limit for the OS group.
 *
 * cpu_rate_limit should be within [0, 100].
 */
void
ResGroupOps_SetCpuRateLimit(const char *group, int cpu_rate_limit)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPU;

	/* group.shares := opentenbase.shares * cpu_rate_limit */

	int64 shares = readInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp,
							 "cpu.shares");
	writeInt64(group, BASETYPE_OPENTENBASE, comp,
			   "cpu.shares", shares * cpu_rate_limit / 100);

	/* set cpu.cfs_quota_us if hard CPU enforment is enabled */
	if (resource_group_cpu_ceiling_enforcement)
	{
		/*
		 * We have already prevend possible wrong situation during create/alter operations
		 * by validateCapabilities. But there exists some circumstance that cpu.cfs_quota_us
		 * will be not properly setup, e.g., restart when resource_group_cpu_ceiling_enforcement
		 * is swithed on. Need check again, and provide clear prompting message.
		 */
		int64 periods = getCfsPeriodUs(comp);
		int64 cfs_quota_us = periods * ResGroupOps_GetCpuCores() * cpu_rate_limit / 100;
		int64 root_cfs_quota_us = readInt64(RESGROUP_ROOT_PATH, BASETYPE_OPENTENBASE, comp,
											"cpu.cfs_quota_us");
		if (cfs_quota_us > root_cfs_quota_us)
			elog(ERROR, "Setting cpu.cfs_quota_us to %ld for group %s, which is too high. "
						"The value should not execeed that of the root one (%ld). "
						"Please consider swithing off resource_group_cpu_ceiling_enforcement "
						"or reducing the cpu_rate_limit(%d) to solve the problem. Need restart. ",
						cfs_quota_us, group, root_cfs_quota_us, cpu_rate_limit);
		else
			writeInt64(group, BASETYPE_OPENTENBASE, comp, "cpu.cfs_quota_us", cfs_quota_us);
	}
	else
	{
		writeInt64(group, BASETYPE_OPENTENBASE, comp, "cpu.cfs_quota_us", -1);
	}
}

/*
 * Get the cpu usage of the OS group, that is the total cpu time obtained
 * by this OS group, in nano seconds.
 */
int64
ResGroupOps_GetCpuUsage(char *group)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPUACCT;

	return readInt64(group, BASETYPE_OPENTENBASE, comp, "cpuacct.usage");
}

/*
 * Get the count of cpu cores on the system.
 */
int
ResGroupOps_GetCpuCores(void)
{
	return ncores;
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
ResGroupOps_SetCpuSet(const char *group, const char *cpuset)
{
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPUSET;

	if (!resource_group_enable_cgroup_cpuset)
		return ;

	writeStr(group, BASETYPE_OPENTENBASE, comp, "cpuset.cpus", cpuset);
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
	ResGroupCompType comp = RESGROUP_COMP_TYPE_CPUSET;

	if (!resource_group_enable_cgroup_cpuset)
		return ;

	readStr(group, BASETYPE_OPENTENBASE, comp, "cpuset.cpus", cpuset, len);
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
	float		percent;

	Assert(usage >= 0LL);
	Assert(duration > 0LL);

	/* There should always be at least one core on the system */
	Assert(ncores > 0);

	/*
	 * Usage is the cpu time (nano seconds) obtained by this group in the time
	 * duration (micro seconds), so cpu time on one core can be calculated as:
	 *
	 *     usage / 1000 / duration / ncores
	 *
	 * To convert it to percentage we should multiple 100%:
	 *
	 *     usage / 1000 / duration / ncores * 100%
	 *   = usage / 10 / duration / ncores
	 */
	percent = usage / 10.0 / duration / ncores;

	/*
	 * Now we have the system level percentage, however when running in a
	 * container with limited cpu quota we need to further scale it with
	 * parent.  Suppose parent has 50% cpu quota and opentenbase is consuming all of
	 * it, then we want opentenbase to report the cpu usage as 100% instead of 50%.
	 */

	if (parent_cfs_quota_us > 0LL)
	{
		/*
		 * Parent cgroup is also limited, scale the percentage to the one in
		 * parent cgroup.  Do not change the expression to `percent *= ...`,
		 * that will lose the precision.
		 */
		percent = percent * system_cfs_quota_us / parent_cfs_quota_us;
	}

	return percent;
}
