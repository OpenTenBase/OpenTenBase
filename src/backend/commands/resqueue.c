/*-------------------------------------------------------------------------
 *
 * resqueue.c
 *	  Commands for manipulating resource queues.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    src/backend/commands/resqueue.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gtm.h"
#include "utils/fmgrprotos.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_tablespace.h"
#include "commands/defrem.h"
#include "commands/resqueue.h"
#include "libpq/auth.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#ifdef XCP
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "port/atomics.h"
#include "postmaster/clustermon.h"
#endif
#include "postmaster/autovacuum.h"
#include "postmaster/clustermon.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/tqual.h"

#define ResQueueOption_Group			"group"
#define ResQueueOption_MemoryLimit		"memory_limit"
#define ResQueueOption_NetworkLimit		"network_limit"
#define ResQueueOption_ActiveStmts		"active_stmts"
#define ResQueueOption_WaitOverload		"wait_overload"
#define ResQueueOption_Priority			"priority"

/**
 * Establish a lower bound on what memory limit may be set on a queue.
 */
#define MIN_RESOURCEQUEUE_MEMORY_LIMIT_KB 		(10 * 1024L)	/* Minimum memory 10 MB */
#define MIN_RESOURCEQUEUE_NETWORK_LIMIT_KB 		(10L)			/* Minimum network bandwidth 10KB/s */
#define DEFAULT_RESOURCEQUEUE_ACTIVE_STMTS		(20)			/* Default active_stmts value */
#define NON_MEM_INTENSIVE_PLAN_MEMORY_LIMIT_KB	(100)			/* 100KB for each NonMemIntensiveNodes */

/**
 * Difference of two timevals in microsecs
 */
#define TIMEVAL_DIFF_USEC(b, a) ((double) (b.tv_sec - a.tv_sec) * 1000000.0 + (b.tv_usec - a.tv_usec))

/**
 * Resource queue should not backoff in scenarios when process is in a critical section 
 * or do not accept interrupt at all. It also make no sense to backoff when we are
 * emitting log or error messages.
 */
#define RESQ_ACTIVE() ((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) &&		\
		IsResQueueEnabled() &&											\
		true == Node_local_resqueue_info.b_addto_shm)
#define RESQ_NO_BACKOFF() (resource_queue_no_backoff					\
		|| (InterruptHoldoffCount != 0 || CritSectionCount != 0))

/* In ms */
#define MIN_SLEEP_THRESHOLD  5000

/* In ms */
#define DEFAULT_SLEEP_TIME 100.0

/* In ms */
#define MAX_SLEEP_TIME 5.0 * 60 * 1000

#ifndef EPSILON
#define EPSILON        1.0E-06
#endif

typedef struct PriorityMap
{
	PriorityVal		priorityVal;
	int				weight;
	char			*sets;
} PriorityMap;

typedef struct MemIntensiveContext
{
	int64 			numNonMemIntensivePlans; 	/* number of non-blocking plans */
	int64 			numMemIntensivePlans; 		/* number of blocking plans */
} MemIntensiveContext;

typedef struct ResQueueUsageShmInfo
{
	pg_atomic_uint64	total_nodes_on_same_host;		/* The number of nodes on the same machine with me */
	pg_atomic_uint64	total_fn_send_buff_bytes;		/* bytes of memory used by FN for send data, see fn_send_shared_buffer_size */
	pg_atomic_uint64	total_dprocess_priorities;		/* The sum of the PriorityVals of all DProcess on this node */
	pg_atomic_uint64	total_dprocess_network_bytes;	/* The sum of the NetworkBytes of all DProcess on this node */
} ResQueueUsageShmInfo;

typedef struct CNAcquiredResourceInfo					/* resource info acquired from GTM by CN */
{
	ResQueueInfo	resqueue_info;						/* ResQueueInfo acquired from GTM */
	int64			memory_bytes;						/* memory Bytes acquired from ResQueueInfo above */
} CNAcquiredResourceInfo;

typedef struct NodeLocalResourceInfo					/* Node local resource info received from CN */
{
	ResQueueInfo	resqueue_info;						/* ResQueueInfo acquired by CN from GTM, and send to DN in pgxc_node_send_resqinfo */
	int64			network_bytes;						/* network bytes limit estimated by CN and send to DN in pgxc_node_send_resqinfo */
	bool			b_addto_shm;						/* Has it been added to ResQueueUsageShmInfo? */
} NodeLocalResourceInfo;

/**
 * This is information that only the current backend ever needs to see.
 */
typedef struct PriorityBackoffInfo
{
	struct rusage 	startUsage;			/* Usage when current statement began. To account for caching of backends. */
	struct rusage 	lastUsage;			/* Usage statistics when backend process performed local backoff action */
	double			lastSleepTime;		/* Last sleep time when local backing-off action was performed */

	struct timeval 	lastCheckTime;		/* Last time the backend process
										 * performed local back-off action.
										 * Used to determine inactive
										 * backends. */
	double			targetUsage;		/* Current target CPU usage */
} PriorityBackoffInfo;

typedef struct NetworkBackoffInfo
{
	struct timeval 	lastCheckTime;		/* Last time the backend process
										 * performed local back-off action.
										 * Used to determine inactive
										 * backends. */
	int64			targetBandwidth;	/* Current target network bandwidth in bytes per sencond */
	int64			allocatedBytes;		/* FnSendPages already allocated */
	int64			releasedBytes;		/* FnSendPages already released */
} NetworkBackoffInfo;

static struct PriorityMap priority_map[] = {
	{PriorityVal_Invalid,	-1,			"Invalid"},
	{PriorityVal_Max, 		1000000,	"MAX"},
	{PriorityVal_High, 		1000,		"HIGH"},
	{PriorityVal_Medium, 	500,		"MEDIUM"},
	{PriorityVal_Low, 		200,		"LOW"},
	{PriorityVal_Min, 		100,		"MIN"},
	{PriorityVal_Butt,		-1,			NULL},
};

static CNAcquiredResourceInfo CN_acquired_resqueue_info	= 
{
	.resqueue_info =
	{ 
		.resq_name.data = { 0 },
	 	.resq_group.data = { 0 },
	 	.resq_memory_limit = 0,
	 	.resq_network_limit = 0,
	 	.resq_active_stmts = 0,
	 	.resq_wait_overload = 0,
	 	.resq_priority = 0
	},
	.memory_bytes = -1
};

static NodeLocalResourceInfo Node_local_resqueue_info = 
{
	.resqueue_info =
	{ 
		.resq_name.data = { 0 },
	 	.resq_group.data = { 0 },
	 	.resq_memory_limit = 0,
	 	.resq_network_limit = 0,
	 	.resq_active_stmts = 0,
	 	.resq_wait_overload = 0,
	 	.resq_priority = 0
	},
	.network_bytes = -1,
	.b_addto_shm = false
};

static PriorityBackoffInfo 	Local_priority_backoff_info;
static NetworkBackoffInfo	Local_network_backoff_info;

static ResQueueUsageShmInfo * g_resqueue_usage_info = NULL;	/* shared memory of ResQueueUsageShmInfo */

bool enable_resource_queue = false;
bool enable_resource_queue_debug = false;

__thread volatile bool resource_queue_no_backoff = false;

static
bool ValidName(char * pResName)
{
	if (pg_strcasecmp(pResName, "none") == 0 ||
		pg_strcasecmp(pResName, DEFAULT_RESQUEUE_NAME) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("resource queue name \"%s\" is reserved",
						pResName)));
	}
	else if (strlen(pResName) > NAMEDATALEN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("The name of the resource queue is invalid. The maximum length of the name is 63 bytes.")));
	}

	return true;
}

char * ResQPriorityGetSetting(int16 pval)
{
	if (pval > PriorityVal_Invalid && pval < PriorityVal_Butt)
	{
		return priority_map[pval].sets;
	}

	return "Invalid";
}

static 
bool ValidPriority(char	* pResSetting, PriorityVal * priorityVal)
{
	int ii	= 0;

	char * pval = priority_map[ii].sets; 

	while (pval != NULL)
	{
		if (0 == pg_strcasecmp(pval, pResSetting))
		{
			if (priorityVal != NULL)
			{
				*priorityVal = priority_map[ii].priorityVal;
			}

			if (priority_map[ii].priorityVal > PriorityVal_Invalid &&
				priority_map[ii].priorityVal < PriorityVal_Butt)
			{
				return true;
			}
		}

		ii++;
		pval = priority_map[ii].sets; 
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("Invalid parameter value \"%s\" for "
					"resource type \"priority\". ",
					pResSetting)));
	return false;
}

/**
 * Validate memory limit setting.
 */
static 
bool ValidMemoryLimit(char * pResSetting, int * memoryLimitKB)
{
	int valueKB = 0;
	const char *restyp = ResQueueOption_MemoryLimit;

	bool result = parse_int(pResSetting, &valueKB, GUC_UNIT_KB, NULL);

	if (!result)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid parameter value \"%s\" for "
						"resource type \"%s\". "
						"Value must be in kB, MB or GB.",
						pResSetting, restyp)));
	}
	else if (valueKB < MIN_RESOURCEQUEUE_MEMORY_LIMIT_KB)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid parameter value \"%s\" for "
						"resource type \"%s\". "
						"Value must be at least %dkB",
						pResSetting, restyp, (int) MIN_RESOURCEQUEUE_MEMORY_LIMIT_KB)));
	}

	if (memoryLimitKB != NULL)
	{
		*memoryLimitKB = valueKB;
	}

	return true;
}

/**
 * Validate network limit setting.
 */
static 
bool ValidNetworkLimit(char * pResSetting, int * networkLimitKB)
{
	int valueKB = 0;
	const char *restyp = ResQueueOption_NetworkLimit;

	bool result = parse_int(pResSetting, &valueKB, GUC_UNIT_KB, NULL);

	if (!result)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid parameter value \"%s\" for "
						"resource type \"%s\". "
						"Value must be in kB, MB or GB.",
						pResSetting, restyp)));
	}
	else if (valueKB < MIN_RESOURCEQUEUE_NETWORK_LIMIT_KB)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Invalid parameter value \"%s\" for "
						"resource type \"%s\". "
						"Value must be at least %dkB",
						pResSetting, restyp, (int) MIN_RESOURCEQUEUE_NETWORK_LIMIT_KB)));
	}

	if (networkLimitKB != NULL)
	{
		*networkLimitKB = valueKB;
	}

	return true;
}

/*
 * CREATE RESOURCE QUEUE
 */
void
CreateResourceQueue(CreateResourceQueueStmt *stmt)
{
	ListCell    *option = NULL;

	DefElem		*def_resq_group = NULL;
	DefElem		*def_resq_memory_limit = NULL;
	DefElem		*def_resq_network_limit = NULL;
	DefElem		*def_resq_active_stmts = NULL;
	DefElem		*def_resq_wait_overload = NULL;
	DefElem		*def_resq_priority = NULL;

	char 		*resq_group = NULL;

	ResQueueInfo	resq_info;
	MemSet(&resq_info, 0, sizeof(resq_info));

	if (!IS_PGXC_LOCAL_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be coordinator to create resource queues")));

	/* Permission check - only superuser can create queues. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create resource queues")));

	/*
	 * Check for an illegal name ('none' is used to signify no queue in ALTER
	 * ROLE).
	 */
	ValidName(stmt->queue);
	snprintf(NameStr(resq_info.resq_name), NAMEDATALEN, "%s", stmt->queue);

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (pg_strcasecmp(defel->defname, ResQueueOption_Group) == 0)
		{
			if (def_resq_group)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_group = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_MemoryLimit) == 0)
		{
			if (def_resq_memory_limit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_memory_limit = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_NetworkLimit) == 0)
		{
			if (def_resq_network_limit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_network_limit = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_ActiveStmts) == 0)
		{
			if (def_resq_active_stmts)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_active_stmts = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_WaitOverload) == 0)
		{
			if (def_resq_wait_overload)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_wait_overload = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_Priority) == 0)
		{
			if (def_resq_priority)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_priority = defel;
		}
		else
		{
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
		}
	}

	if (def_resq_group)
	{
		resq_group = defGetString(def_resq_group);
		if (!OidIsValid(get_pgxc_groupoid(resq_group)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("PGXC Group %s: group is not defined yet",
							resq_group)));
	}
	else
	{
		resq_group = "NoneGroup";
	}

	snprintf(NameStr(resq_info.resq_group), NAMEDATALEN, "%s", resq_group);

	if (def_resq_memory_limit)
	{
		char * pResSetting = defGetString(def_resq_memory_limit);
		int memoryLimitKB = 0;

		ValidMemoryLimit(pResSetting, &memoryLimitKB);
		resq_info.resq_memory_limit = (int64) (((int64)memoryLimitKB) * 1024);	/* convert KB to Byte */
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("memory_limit threshold must be specified")));
	}

	if (def_resq_network_limit)
	{
		char * pResSetting = defGetString(def_resq_network_limit);
		int networkLimitKB = 0;

		ValidNetworkLimit(pResSetting, &networkLimitKB);
		resq_info.resq_network_limit = (int64) (((int64)networkLimitKB) * 1024);	/* convert KB to Byte */
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("network_limit threshold must be specified")));
	}

	if (def_resq_active_stmts)
	{
		int32 resq_active_stmts = 0;
		resq_active_stmts = defGetInt32(def_resq_active_stmts);

		if (resq_active_stmts <= 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("active_stmts threshold cannot be less than or equal to 0")));
		}
		else
		{
			resq_info.resq_active_stmts = resq_active_stmts;
		}
	}
	else
	{
		resq_info.resq_active_stmts = DEFAULT_RESOURCEQUEUE_ACTIVE_STMTS;
	}

	if (def_resq_wait_overload)
	{
		if (true == defGetBoolean(def_resq_wait_overload))
		{
			resq_info.resq_wait_overload = 1;
		}
		else
		{
			resq_info.resq_wait_overload = 0;
		}
	}
	else
	{
		resq_info.resq_wait_overload = 1;
	}

	if (def_resq_priority)
	{
		char * pResSetting = defGetString(def_resq_priority);
		PriorityVal priorityVal = PriorityVal_Butt;

		ValidPriority(pResSetting, &priorityVal);
		resq_info.resq_priority = (int16) priorityVal;
	}
	else
	{
		resq_info.resq_priority = (int16) PriorityVal_Medium;
	}

	/* create it on the GTM */
	if (CreateResQueueGTM(&resq_info) < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("GTM error, could not create resource queue")));
	}
}

/*
 * ALTER RESOURCE QUEUE
 */
void
AlterResourceQueue(AlterResourceQueueStmt *stmt)
{
	ListCell    *option = NULL;

	DefElem		*def_resq_group = NULL;
	DefElem		*def_resq_memory_limit = NULL;
	DefElem		*def_resq_network_limit = NULL;
	DefElem		*def_resq_active_stmts = NULL;
	DefElem		*def_resq_wait_overload = NULL;
	DefElem		*def_resq_priority = NULL;

	char 		*resq_group = NULL;

	char alter_name = 0;
	char alter_group = 0;
	char alter_memory_limit = 0;
	char alter_network_limit = 0;
	char alter_active_stmts = 0;
	char alter_wait_overload = 0;
	char alter_priority = 0;

	ResQueueInfo	resq_info;
	MemSet(&resq_info, 0, sizeof(resq_info));

	if (!IS_PGXC_LOCAL_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be coordinator to alter resource queues")));

	/* Permission check - only superuser can alter queues. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to alter resource queues")));
	/*
	 * Check for an illegal name ('none' is used to signify no queue in ALTER
	 * ROLE).
	 */
	ValidName(stmt->queue);
	snprintf(NameStr(resq_info.resq_name), NAMEDATALEN, "%s", stmt->queue);

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (pg_strcasecmp(defel->defname, ResQueueOption_Group) == 0)
		{
			if (def_resq_group)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_group = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_MemoryLimit) == 0)
		{
			if (def_resq_memory_limit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_memory_limit = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_NetworkLimit) == 0)
		{
			if (def_resq_network_limit)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_network_limit = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_ActiveStmts) == 0)
		{
			if (def_resq_active_stmts)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_active_stmts = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_WaitOverload) == 0)
		{
			if (def_resq_wait_overload)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_wait_overload = defel;
		}
		else if (pg_strcasecmp(defel->defname, ResQueueOption_Priority) == 0)
		{
			if (def_resq_priority)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			def_resq_priority = defel;
		}
		else
		{
			elog(ERROR, "option \"%s\" not recognized",
				 defel->defname);
		}
	}

	if (def_resq_group)
	{
		resq_group = defGetString(def_resq_group);
		if (!OidIsValid(get_pgxc_groupoid(resq_group)))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("PGXC Group %s: group is not defined yet",
							resq_group)));
	}
	else
	{
		resq_group = "NoneGroup";
	}

	snprintf(NameStr(resq_info.resq_group), NAMEDATALEN, "%s", resq_group);

	if (def_resq_memory_limit)
	{
		char * pResSetting = defGetString(def_resq_memory_limit);
		int memoryLimitKB = 0;

		ValidMemoryLimit(pResSetting, &memoryLimitKB);
		resq_info.resq_memory_limit = (int64) (((int64)memoryLimitKB) * 1024);	/* convert KB to Byte */
		alter_memory_limit = 1;
	}

	if (def_resq_network_limit)
	{
		char * pResSetting = defGetString(def_resq_network_limit);
		int networkLimitKB = 0;

		ValidNetworkLimit(pResSetting, &networkLimitKB);
		resq_info.resq_network_limit = (int64) (((int64)networkLimitKB) * 1024);	/* convert KB to Byte */
		alter_network_limit = 1;
	}

	if (def_resq_active_stmts)
	{
		int32 resq_active_stmts = 0;
		resq_active_stmts = defGetInt32(def_resq_active_stmts);

		if (resq_active_stmts <= 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("active_stmts threshold cannot be less than or equal to 0")));
		}
		else
		{
			resq_info.resq_active_stmts = resq_active_stmts;
			alter_active_stmts = 1;
		}
	}

	if (def_resq_wait_overload)
	{
		if (true == defGetBoolean(def_resq_wait_overload))
		{
			resq_info.resq_wait_overload = 1;
		}
		else
		{
			resq_info.resq_wait_overload = 0;
		}

		alter_wait_overload = 1;
	}

	if (def_resq_priority)
	{
		char * pResSetting = defGetString(def_resq_priority);
		PriorityVal priorityVal = PriorityVal_Butt;

		ValidPriority(pResSetting, &priorityVal);
		resq_info.resq_priority = (int16) priorityVal;
		alter_priority = 1;
	}

	alter_name = 0;
	alter_group = 0;

	if (alter_memory_limit ||
		alter_network_limit ||
		alter_active_stmts ||
		alter_wait_overload ||
		alter_priority)
	{
		if (AlterResQueueGTM(&resq_info,
							alter_name,
							alter_group,
							alter_memory_limit,
							alter_network_limit,
							alter_active_stmts,
							alter_wait_overload,
							alter_priority) < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("GTM error, could not alter resource queue")));
		}
	}
}

/*
 * DROP RESOURCE QUEUE
 */
void
DropResourceQueue(DropResourceQueueStmt *stmt)
{
	NameData resq_name;
	MemSet(&resq_name, 0, sizeof(resq_name));

	if (!IS_PGXC_LOCAL_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be coordinator to drop resource queues")));

	/* Permission check - only superuser can drop queues. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop resource queues")));

	ValidName(stmt->queue);
	snprintf(NameStr(resq_name), NAMEDATALEN, "%s", stmt->queue);

	do
	{
		Relation	pg_authid_rel = NULL;
		HeapScanDesc scan = NULL;
		HeapTuple	tuple = NULL;
		TupleDesc 	tup_desc = NULL;
		bool		in_using = false;

		pg_authid_rel = heap_open(AuthIdRelationId, AccessShareLock);
		tup_desc = RelationGetDescr(pg_authid_rel);

		scan = heap_beginscan_catalog(pg_authid_rel, 0, NULL);
		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			bool tuple_isnull = false;
			Name tuple_resqname = NULL;

			tuple_resqname = DatumGetName(fastgetattr(tuple, Anum_pg_authid_rolresqueue, tup_desc, &tuple_isnull));
			if (tuple_isnull == false && tuple_resqname != NULL && 
				pg_strncasecmp(tuple_resqname->data, NameStr(resq_name), NAMEDATALEN) == 0)
			{
				in_using = true;
				break;
			}
		}

		heap_endscan(scan);
		heap_close(pg_authid_rel, AccessShareLock);

		if (in_using)
		{
			ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("Cannot perform Drop operations on the resource queue %s who is being used", NameStr(resq_name))));
		}
	} while (0);

	/* create it on the GTM */
	if (DropResQueueGTM(&resq_name) < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("GTM error, could not drop resource queue")));
	}
}

bool IsResQueueEnabled(void)
{
	if (enable_resource_queue &&
		IsNormalProcessingMode() &&
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

bool IsResQueueExists(char * resqueue)
{
	bool exists = false;
	NameData resq_name;
	MemSet(&resq_name, 0, sizeof(resq_name));

	ValidName(resqueue);
	snprintf(NameStr(resq_name), NAMEDATALEN, "%s", resqueue);

	/* check it on the GTM */
	if (CheckIfExistsResQueueGTM(&resq_name, &exists) < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("GTM error, could not check resource queue")));
	}

	return exists;
}

Datum opentenbase_resource_queue_acquire(PG_FUNCTION_ARGS)
{
	NameData * resq_name = NULL;
	int64	memory_Bytes = 0;
	int32	errcode = -1;
	ResQueueInfo resResult;

	resq_name =  PG_GETARG_NAME(0);
	memory_Bytes = PG_GETARG_INT64(1) * 1024;

	MemSet(&resResult, 0, sizeof(resResult));

	PG_RETURN_NULL();

	if (AcquireResQueueGTM(resq_name, memory_Bytes, &resResult, &errcode) < 0 ||
		errcode != 0)
	{
		elog(WARNING, "Failed to acqurie %ld Bytes from resqueue %s, errcode = %d",
				memory_Bytes, resq_name->data, errcode);
	}
	else
	{
		elog(WARNING, "Successed to acqurie %ld Bytes from resqueue %s, errcode = %d, "
				"resq_name = %s, resq_group = %s, resq_memory_limit = %ld, "
				"resq_network_limit = %ld, resq_active_stmts = %d, "
				"resq_wait_overload = %d, resq_priority = %d",
				memory_Bytes, resq_name->data, errcode,
				resResult.resq_name.data,
				resResult.resq_group.data,
				resResult.resq_memory_limit,
				resResult.resq_network_limit,
				resResult.resq_active_stmts,
				resResult.resq_wait_overload,
				resResult.resq_priority);
	}

	PG_RETURN_NULL();
}

Datum opentenbase_resource_queue_release(PG_FUNCTION_ARGS)
{
	NameData * resq_name = NULL;
	int64 memory_Bytes = 0;
	int32	errcode = -1;

	resq_name =  PG_GETARG_NAME(0);
	memory_Bytes = PG_GETARG_INT64(1) * 1024;

	PG_RETURN_NULL();

	if (ReleaseResQueueGTM(resq_name, memory_Bytes, &errcode) < 0 ||
		errcode != 0)
	{
		elog(WARNING, "Failed to release %ld Bytes back to resqueue %s, errcode = %d",
				 memory_Bytes, resq_name->data, errcode);
	}
	else
	{
	}

	PG_RETURN_NULL();
}

/**
 * Is memory intensive plan node?
 */
static bool
IsMemoryIntensivePlan(Plan *node)
{
	switch(nodeTag(node))
	{
		case T_Agg:
		case T_BitmapIndexScan:
		case T_BitmapOr:
		case T_CteScan:
		case T_Distribution:
		case T_FunctionScan:
		case T_GatherMerge:
		case T_Hash:
		case T_Material:
		case T_MergeAppend:
		case T_RecursiveUnion:
		case T_RemoteSubplan:
		case T_RemoteQuery:
		case T_Sort:
		case T_TableFuncScan:
		case T_Unique:
		case T_WindowAgg:
		case T_WindowFunc:
		case T_WorkTableScan:
		{
			return true;
			break;
		}
		default:
		{
			return false;
			break;
		}
	}
}

/**
 * This walker counts the number of memory intensive and non-memory intensive plans
 * in a plan tree.
 */
static bool MemIntensiveWalker(Plan * planTree, MemIntensiveContext *context)
{
	if (planTree == NULL)
	{
		return false;
	}

	Assert(planTree);
	Assert(context);

	if (IsMemoryIntensivePlan(planTree))
	{
		context->numMemIntensivePlans++;
	}
	else
	{
		context->numNonMemIntensivePlans++;
	}

	return plan_tree_walker(planTree, MemIntensiveWalker, context);
}

int64 EstimateQueryMemoryBytes(PlannedStmt *stmt)
{
	MemIntensiveContext context = { 0, 0 };
	int64 requiredQueryMemKB = 0;

	MemIntensiveWalker(stmt->planTree, &context);

	requiredQueryMemKB = context.numMemIntensivePlans * work_mem + 
								context.numNonMemIntensivePlans * NON_MEM_INTENSIVE_PLAN_MEMORY_LIMIT_KB;

	if (enable_resource_queue_debug)
	{
		elog(LOG, "The estimated query requires "INT64_FORMAT" bytes of memory, "
				""INT64_FORMAT" blocked plans require "INT64_FORMAT" bytes of memory, "
				""INT64_FORMAT" non-blocking plans require "INT64_FORMAT" bytes of memory, "
				"and the work_mem value is "INT64_FORMAT" bytes.",
				requiredQueryMemKB * 1024,
				context.numMemIntensivePlans,
				context.numMemIntensivePlans * work_mem * 1024,
				context.numNonMemIntensivePlans,
				context.numNonMemIntensivePlans * NON_MEM_INTENSIVE_PLAN_MEMORY_LIMIT_KB * 1024,
				((int64)work_mem) * 1024);

		elog(WARNING, "The estimated query requires "INT64_FORMAT" bytes of memory, "
				""INT64_FORMAT" blocked plans require "INT64_FORMAT" bytes of memory, "
				""INT64_FORMAT" non-blocking plans require "INT64_FORMAT" bytes of memory, "
				"and the work_mem value is "INT64_FORMAT" bytes.",
				requiredQueryMemKB * 1024,
				context.numMemIntensivePlans,
				context.numMemIntensivePlans * work_mem * 1024,
				context.numNonMemIntensivePlans,
				context.numNonMemIntensivePlans * NON_MEM_INTENSIVE_PLAN_MEMORY_LIMIT_KB * 1024,
				((int64)work_mem) * 1024);
	}

	return requiredQueryMemKB * 1024L;
}

void GetUserResQueueName(Oid roleid, NameData * resq)
{
	HeapTuple tuple = NULL;

	tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("invalid role OID: %u", roleid)));
	}
	else
	{
		if (resq != NULL)
		{
			snprintf(resq->data, NAMEDATALEN, "%s", 
				NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolresqueue));
		}
		ReleaseSysCache(tuple);
	}
}

static void CleanAcquiredResourceInfo(void)
{
	MemSet(&CN_acquired_resqueue_info, 0, sizeof(CN_acquired_resqueue_info));
	CN_acquired_resqueue_info.memory_bytes = -1;
}

void AcquireResourceFromResQueue(int64 acquiredMemBytes)
{
	NameData 		resq_name;
	ResQueueInfo	resq_info;
	int32			err_result = -1;

	if (!IS_PGXC_COORDINATOR)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Only CN is allowed to acquire resources from GTM")));
	}

	if (CN_acquired_resqueue_info.memory_bytes >= 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("There is still %lu Bytes of memory not released, "
						"Cannot acquire new resource.",
				 		CN_acquired_resqueue_info.memory_bytes)));
	}

	MemSet(&resq_name, 0, sizeof(resq_name));
	MemSet(&resq_info, 0, sizeof(resq_info));

	CleanAcquiredResourceInfo();

	GetUserResQueueName(GetUserId(), &resq_name);
	if (AcquireResQueueGTM(&resq_name, acquiredMemBytes, &resq_info, &err_result) < 0 ||
		err_result != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to acquire %ld Bytes from resqueue %s, errcode = %d",
						acquiredMemBytes, resq_name.data, err_result)));
	}
	else
	{
		if (enable_resource_queue_debug)
			elog(LOG,
				 "Successed to acquire %ld Bytes from resqueue %s, errcode = %d, "
				 "resq_name = %s, resq_group = %s, resq_memory_limit = %ld, "
				 "resq_network_limit = %ld, resq_active_stmts = %d, "
				 "resq_wait_overload = %d, resq_priority = %d",
				 acquiredMemBytes, resq_name.data, err_result,
				 resq_info.resq_name.data,
				 resq_info.resq_group.data,
				 resq_info.resq_memory_limit,
				 resq_info.resq_network_limit,
				 resq_info.resq_active_stmts,
				 resq_info.resq_wait_overload,
				 resq_info.resq_priority);
	}

	CN_acquired_resqueue_info.resqueue_info = resq_info;
	CN_acquired_resqueue_info.memory_bytes = acquiredMemBytes;
}

void ReleaseResourceBackToResQueue(void)
{
	if (!IS_PGXC_COORDINATOR)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Only CN is allowed to release resources back to GTM")));
	}

	if (CN_acquired_resqueue_info.memory_bytes >= 0)
	{
		int32 err_result = -1;

		if (ReleaseResQueueGTM(&CN_acquired_resqueue_info.resqueue_info.resq_name, CN_acquired_resqueue_info.memory_bytes, &err_result) < 0 ||
			err_result != 0)
		{
			if (enable_resource_queue_debug)
			{
				elog(LOG, "Failed to release %ld Bytes back to resqueue %s, errcode = %d, "
						"resq_name = %s, resq_group = %s, resq_memory_limit = %ld, "
						"resq_network_limit = %ld, resq_active_stmts = %d, "
						"resq_wait_overload = %d, resq_priority = %d",
						CN_acquired_resqueue_info.memory_bytes, CN_acquired_resqueue_info.resqueue_info.resq_name.data, err_result,
						CN_acquired_resqueue_info.resqueue_info.resq_name.data,
						CN_acquired_resqueue_info.resqueue_info.resq_group.data,
						CN_acquired_resqueue_info.resqueue_info.resq_memory_limit,
						CN_acquired_resqueue_info.resqueue_info.resq_network_limit,
						CN_acquired_resqueue_info.resqueue_info.resq_active_stmts,
						CN_acquired_resqueue_info.resqueue_info.resq_wait_overload,
						CN_acquired_resqueue_info.resqueue_info.resq_priority);
			}
		}
		else
		{
			if (enable_resource_queue_debug)
			{
				elog(LOG, "Successed to release %ld Bytes back to resqueue %s, errcode = %d, "
						"resq_name = %s, resq_group = %s, resq_memory_limit = %ld, "
						"resq_network_limit = %ld, resq_active_stmts = %d, "
						"resq_wait_overload = %d, resq_priority = %d",
						CN_acquired_resqueue_info.memory_bytes, CN_acquired_resqueue_info.resqueue_info.resq_name.data, err_result,
						CN_acquired_resqueue_info.resqueue_info.resq_name.data,
						CN_acquired_resqueue_info.resqueue_info.resq_group.data,
						CN_acquired_resqueue_info.resqueue_info.resq_memory_limit,
						CN_acquired_resqueue_info.resqueue_info.resq_network_limit,
						CN_acquired_resqueue_info.resqueue_info.resq_active_stmts,
						CN_acquired_resqueue_info.resqueue_info.resq_wait_overload,
						CN_acquired_resqueue_info.resqueue_info.resq_priority);
			}
		}
	}

	CleanAcquiredResourceInfo();
}

int64 EstimateDProcessNetworkBytes(int16 dnDprocessNumber)
{
	int64 perDProcessNetworkBytesLimit = -1;

	if (CN_acquired_resqueue_info.resqueue_info.resq_network_limit > 0 && 
		dnDprocessNumber > 0)
	{
		int64 perQueryNetworkBytesLimit = 0;

		perQueryNetworkBytesLimit = CN_acquired_resqueue_info.resqueue_info.resq_network_limit / CN_acquired_resqueue_info.resqueue_info.resq_active_stmts;
		perDProcessNetworkBytesLimit = perQueryNetworkBytesLimit / dnDprocessNumber;

		if (perDProcessNetworkBytesLimit < 1000L)
			perDProcessNetworkBytesLimit = 1000L;		/* Min 1000 Bytes/s */	
	}

	return perDProcessNetworkBytesLimit;
}

void SendResQueueInfoToDProcess(PGXCNodeHandle *handle,
								int64 network_bytes_limit)
{
	if (pgxc_node_send_resqinfo(handle, &CN_acquired_resqueue_info.resqueue_info, network_bytes_limit))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to send resqinfo to data node:%s", handle->nodename)));
	}
}

void CleanLocalResQueueInfo(void)
{
	MemSet(&Node_local_resqueue_info, 0, sizeof(Node_local_resqueue_info));
	Node_local_resqueue_info.network_bytes = -1;
	Node_local_resqueue_info.b_addto_shm = false;
}

void CNAssignLocalResQueueInfo(void)
{
	int64 perQueryNetworkBytesLimit = -1;

	if (CN_acquired_resqueue_info.resqueue_info.resq_network_limit > 0)
	{
		perQueryNetworkBytesLimit = CN_acquired_resqueue_info.resqueue_info.resq_network_limit / CN_acquired_resqueue_info.resqueue_info.resq_active_stmts;

		if (perQueryNetworkBytesLimit < 1000L)
			perQueryNetworkBytesLimit = 1000L;		/* Min 1000 Bytes/s */	
	}

	CleanLocalResQueueInfo();

	Node_local_resqueue_info.resqueue_info = CN_acquired_resqueue_info.resqueue_info;
	Node_local_resqueue_info.network_bytes = perQueryNetworkBytesLimit;
	Node_local_resqueue_info.b_addto_shm = false;
}

void DNReceiveLocalResQueueInfo(StringInfo input_message)
{
	ResQueueInfo 	resq_info;
	int64			network_bytes_limit = -1;
	
	if (!IS_PGXC_DATANODE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Only DN is allowed to receive local resource info from CN")));
	}

	MemSet(&resq_info, 0, sizeof(resq_info));
	CleanLocalResQueueInfo();

	pq_copymsgbytes(input_message, resq_info.resq_name.data, NAMEDATALEN);
	pq_copymsgbytes(input_message, resq_info.resq_group.data, NAMEDATALEN);
	resq_info.resq_memory_limit = pq_getmsgint64(input_message);
	resq_info.resq_network_limit = pq_getmsgint64(input_message);
	resq_info.resq_active_stmts = (int32)pq_getmsgint(input_message, 4);
	resq_info.resq_wait_overload = (int16)pq_getmsgint(input_message, 2);
	resq_info.resq_priority = (int16)pq_getmsgint(input_message, 2);
	network_bytes_limit = pq_getmsgint64(input_message);
	pq_getmsgend(input_message);

	Node_local_resqueue_info.resqueue_info = resq_info;
	Node_local_resqueue_info.network_bytes = network_bytes_limit;
	Node_local_resqueue_info.b_addto_shm = false;
}

Size
ResQUsageShmemSize(void)
{
    Size		size = 0;

	size = sizeof(ResQueueUsageShmInfo);

	return size;
}

void
ResQUsageShmemInit(void)
{
    
    Size		size = 0;
    bool		found = false;

    /* Create or attach to the shared array */
    size = ResQUsageShmemSize();
    g_resqueue_usage_info = (ResQueueUsageShmInfo *) ShmemInitStruct("ResQueue Shm", size, &found);

    if (!found)
    {
		/*
		* We're the first - initialize.
		*/
        MemSet(g_resqueue_usage_info, 0, size);

		pg_atomic_init_u64(&g_resqueue_usage_info->total_fn_send_buff_bytes, 0);
		pg_atomic_init_u64(&g_resqueue_usage_info->total_dprocess_priorities, 0);
		pg_atomic_init_u64(&g_resqueue_usage_info->total_dprocess_network_bytes, 0);
    }
}

void ResQUsageSetTotalNodesOnSameHost(int64 node_number)
{
	pg_atomic_write_u64(&g_resqueue_usage_info->total_nodes_on_same_host, node_number);
}

int64 ResQUsageGetTotalNodesOnSameHost(void)
{
	return (int64) pg_atomic_read_u64(&g_resqueue_usage_info->total_nodes_on_same_host);
}

void ResQUsageSetFnSendBuffBytes(int64 fn_send_buff_bytes)
{
	pg_atomic_write_u64(&g_resqueue_usage_info->total_fn_send_buff_bytes, fn_send_buff_bytes);
}

int64 ResQUsageGetFnSendBuffBytes(void)
{
	return (int64) pg_atomic_read_u64(&g_resqueue_usage_info->total_fn_send_buff_bytes);
}

void ResQUsageAddPriorityVal(int16 priorityVal)
{
	if (priorityVal > PriorityVal_Invalid && priorityVal < PriorityVal_Butt)
	{
		int64 weight = priority_map[priorityVal].weight;
		pg_atomic_add_fetch_u64(&g_resqueue_usage_info->total_dprocess_priorities, weight);
	}
}

void ResQUsageSubPriorityVal(int16 priorityVal)
{
	if (priorityVal > PriorityVal_Invalid && priorityVal < PriorityVal_Butt)
	{
		int64 weight = priority_map[priorityVal].weight;
		pg_atomic_sub_fetch_u64(&g_resqueue_usage_info->total_dprocess_priorities, weight);
	}
}

void ResQUsageSetTotalPriorities(int64 totalPriorities)
{
	pg_atomic_write_u64(&g_resqueue_usage_info->total_dprocess_priorities, totalPriorities);
}

int64 ResQUsageGetTotalPriorities(void)
{
	return (int64) pg_atomic_read_u64(&g_resqueue_usage_info->total_dprocess_priorities);
}

void ResQUsageAddNetworkBytesLimit(int64 network_bytes)
{
	pg_atomic_add_fetch_u64(&g_resqueue_usage_info->total_dprocess_network_bytes, network_bytes);
}

void ResQUsageSubNetworkBytesLimit(int64 network_bytes)
{
	pg_atomic_sub_fetch_u64(&g_resqueue_usage_info->total_dprocess_network_bytes, network_bytes);
}

void ResQUsageSetTotalNetworkBytesLimit(int64 totalNetworkBytes)
{
	pg_atomic_write_u64(&g_resqueue_usage_info->total_dprocess_network_bytes, totalNetworkBytes);
}

int64 ResQUsageGetTotalNetworkBytesLimit(void)
{
	return (int64) pg_atomic_read_u64(&g_resqueue_usage_info->total_dprocess_network_bytes);
}

void ResQUsageAddLocalResourceInfo(void)
{
	if (false == Node_local_resqueue_info.b_addto_shm)
	{
		Node_local_resqueue_info.b_addto_shm = true;

		if (Node_local_resqueue_info.network_bytes >= 0)
		{
			ResQUsageAddNetworkBytesLimit(Node_local_resqueue_info.network_bytes);
		}

		if (Node_local_resqueue_info.resqueue_info.resq_priority > PriorityVal_Invalid &&
			Node_local_resqueue_info.resqueue_info.resq_priority < PriorityVal_Butt)
		{
			ResQUsageAddPriorityVal(Node_local_resqueue_info.resqueue_info.resq_priority);
		}

		MemSet(&Local_priority_backoff_info, 0, sizeof(Local_priority_backoff_info));
		ResetUsageCommon(&Local_priority_backoff_info.lastUsage, 
							&Local_priority_backoff_info.lastCheckTime);
		memcpy(&Local_priority_backoff_info.startUsage, 
				&Local_priority_backoff_info.lastUsage,
				sizeof(Local_priority_backoff_info.lastUsage));
		Local_priority_backoff_info.lastSleepTime = DEFAULT_SLEEP_TIME;

		MemSet(&Local_network_backoff_info, 0, sizeof(Local_network_backoff_info));
		memcpy(&Local_network_backoff_info.lastCheckTime, 
				&Local_priority_backoff_info.lastCheckTime,
				sizeof(Local_priority_backoff_info.lastCheckTime));
	}
}

void ResQUsageRemoveLocalResourceInfo(void)
{
	if (true == Node_local_resqueue_info.b_addto_shm)
	{
		Node_local_resqueue_info.b_addto_shm = false;

		if (Node_local_resqueue_info.network_bytes >= 0)
		{
			ResQUsageSubNetworkBytesLimit(Node_local_resqueue_info.network_bytes);
			Node_local_resqueue_info.network_bytes = -1;
		}

		if (Node_local_resqueue_info.resqueue_info.resq_priority > PriorityVal_Invalid &&
			Node_local_resqueue_info.resqueue_info.resq_priority < PriorityVal_Butt)
		{
			ResQUsageSubPriorityVal(Node_local_resqueue_info.resqueue_info.resq_priority);
			Node_local_resqueue_info.resqueue_info.resq_priority = PriorityVal_Invalid;
		}

		MemSet(&Local_priority_backoff_info, 0, sizeof(Local_priority_backoff_info));
		MemSet(&Local_network_backoff_info, 0, sizeof(Local_network_backoff_info));
	}

	CleanLocalResQueueInfo();
}

void ResQUsageBackoffPriority(void)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeclaration-after-statement"

	struct timeval currentTime;
	struct rusage currentUsage;
	double		  totalPriorities = 0;
	int	j = 0;

	if (likely(!RESQ_ACTIVE() || RESQ_NO_BACKOFF()))
	{
		return;
	}

	MemSet(&currentTime, 0, sizeof(currentTime));
	MemSet(&currentUsage, 0, sizeof(currentUsage));
	ResetUsageCommon(&currentUsage, &currentTime);

	totalPriorities = (double) ResQUsageGetTotalPriorities();

	/* If backoff can be performed by this process */
	if (Node_local_resqueue_info.resqueue_info.resq_priority > PriorityVal_Invalid &&
		Node_local_resqueue_info.resqueue_info.resq_priority < PriorityVal_Butt &&
		totalPriorities > 0)
	{
		double		thisProcessTime = 0.0;
		double		totalTime = 0.0;
		double		cpuRatio = 0.0;
		double		changeFactor = 1.0;
		double		myWeight = 0.0;

		myWeight = (double) priority_map[Node_local_resqueue_info.resqueue_info.resq_priority].weight;
		Local_priority_backoff_info.targetUsage = myWeight / totalPriorities;

			/* How much did the cpu work on behalf of this process - incl user and sys time */
		thisProcessTime = TIMEVAL_DIFF_USEC(currentUsage.ru_utime, Local_priority_backoff_info.lastUsage.ru_utime)
			+ TIMEVAL_DIFF_USEC(currentUsage.ru_stime, Local_priority_backoff_info.lastUsage.ru_stime);

		/* Absolute cpu time since the last check. */
		totalTime = TIMEVAL_DIFF_USEC(currentTime, Local_priority_backoff_info.lastCheckTime);

		if (unlikely(totalTime < EPSILON))
		{
			/* 
			 * There are cases where we enter in CHECK_INTERRUPT again without time eclipses, namely gather_getNext, etc.
			 * TODO: The priority setting should be an array via GUC that can be adjusted.
			 */
			return;
		}

		cpuRatio = thisProcessTime / totalTime;

		cpuRatio = Min(cpuRatio, 1.0);

		changeFactor = cpuRatio / Local_priority_backoff_info.targetUsage;

		Local_priority_backoff_info.lastSleepTime *= changeFactor;

		if (Local_priority_backoff_info.lastSleepTime < DEFAULT_SLEEP_TIME)
			Local_priority_backoff_info.lastSleepTime = DEFAULT_SLEEP_TIME;

		if (Local_priority_backoff_info.lastSleepTime > MAX_SLEEP_TIME)
			Local_priority_backoff_info.lastSleepTime = MAX_SLEEP_TIME;

		memcpy(&Local_priority_backoff_info.lastUsage, &currentUsage, sizeof(currentUsage));
		memcpy(&Local_priority_backoff_info.lastCheckTime, &currentTime, sizeof(currentTime));

		if (Local_priority_backoff_info.lastSleepTime > MIN_SLEEP_THRESHOLD)
		{
			if (enable_resource_queue_debug)
			{
				elog(LOG, "Current resource queue is %s, with priority of %d and calculated weight of %f "
							"out of global total priority of %f. "
							"This resource queue has used %f out of total time %f (ratio %f), "
							"and therefore this process will sleep for another %f (ms).",
							Node_local_resqueue_info.resqueue_info.resq_name.data,
							Node_local_resqueue_info.resqueue_info.resq_priority,
							myWeight, totalPriorities,
							thisProcessTime, totalTime, cpuRatio,
							Local_priority_backoff_info.lastSleepTime);
			}
			/*
			 * Sleeping happens in chunks so that the backend may exit early
			 * from its sleep if the sweeper requests it to.
			 */

			int			numIterations = (int) (Local_priority_backoff_info.lastSleepTime / 1000L);
			double		leftOver = (double) ((long) Local_priority_backoff_info.lastSleepTime % 1000L);

			for (j = 0; j < numIterations; j++)
			{
				/* Sleep a chunk */
				pg_usleep(1000L);
			}

			if (j == numIterations)
			{
				pg_usleep(leftOver);
			}

			Local_priority_backoff_info.lastSleepTime = DEFAULT_SLEEP_TIME;
		}
	}
	else
	{
		/*
		 * Even if this backend did not backoff, it should record current
		 * usage and current time so that subsequent calculations are
		 * accurate.
		 */
		memcpy(&Local_priority_backoff_info.lastUsage, &currentUsage, sizeof(currentUsage));
		memcpy(&Local_priority_backoff_info.lastCheckTime, &currentTime, sizeof(currentTime));
	}
#pragma GCC diagnostic pop
}

void ResQUsageBackoffNetwork(int64 fn_alloc_bytes, int64 fn_released_bytes)
{
	if (!((IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) && IsResQueueEnabled() && !superuser()))
	{
		return;
	}

	if (!(true == Node_local_resqueue_info.b_addto_shm && 
		  Node_local_resqueue_info.network_bytes > 0 && 
		  Node_local_resqueue_info.resqueue_info.resq_network_limit > 0))
	{
		return;
	}

	if (fn_alloc_bytes == 0 && fn_released_bytes > 0)
	{
		Local_network_backoff_info.releasedBytes += fn_released_bytes;
	}
	else if (fn_alloc_bytes > 0 && fn_released_bytes == 0)
	{
		struct timeval 	currentTime;
		double			totalTime = 0.0;

		MemSet(&currentTime, 0, sizeof(currentTime));
		gettimeofday(&currentTime, NULL);

		Local_network_backoff_info.allocatedBytes += fn_alloc_bytes;

		/* Absolute time since the last check. */
		totalTime = TIMEVAL_DIFF_USEC(currentTime, Local_network_backoff_info.lastCheckTime);

		if (Local_network_backoff_info.allocatedBytes * 1000000L > Node_local_resqueue_info.network_bytes * totalTime)
		{
			int64  totalNetworkBytesLimit = ResQUsageGetTotalNetworkBytesLimit();
			int64  totalFnSendBuffBytes = ResQUsageGetFnSendBuffBytes();
			int64  totalNodesOnSameHost = ResQUsageGetTotalNodesOnSameHost();

			int64 v1 = (Local_network_backoff_info.allocatedBytes - Local_network_backoff_info.releasedBytes) * totalNetworkBytesLimit * totalNodesOnSameHost;
			int64 v2 = Node_local_resqueue_info.network_bytes * totalFnSendBuffBytes;

			double sleepval = ((Local_network_backoff_info.allocatedBytes * 1000000L) / (Node_local_resqueue_info.network_bytes)) - totalTime;

			/*
			 *	(Local_network_backoff_info.allocatedBytes - Local_network_backoff_info.releasedBytes)              Node_local_resqueue_info.network_bytes      
			 *	________________________________________________________________________________________    >    ____________________________________________
			 *	                           totalFnSendBuffBytes                                                        totalNetworkBytesLimit
			 *	                           ____________________
			 *		                       totalNodesOnSameHost
			 */
			if (v1 > v2 && sleepval > MIN_SLEEP_THRESHOLD)
			{
				/*
				 * Sleeping happens in chunks so that the backend may exit early
				 * from its sleep if the sweeper requests it to.
				 */
				int			j = 0;
				int			numIterations = (int) (sleepval / 1000L);
				double		leftOver = (double) ((long) sleepval % 1000L);

				for (j = 0; j < numIterations; j++)
				{
					/* Sleep a chunk */
					pg_usleep(1000L);
				}

				if (j == numIterations)
				{
					pg_usleep(leftOver);
				}
			}
		}
	}
}
