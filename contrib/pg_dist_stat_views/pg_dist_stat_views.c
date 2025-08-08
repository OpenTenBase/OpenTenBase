/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
#include "postgres.h"
#include "stddef.h"

#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "common/ip.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/squeue.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/predicate_internals.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


PG_MODULE_MAGIC;

#define PG_DIST_STAT_ACTIVITY_COLS 27	// 列数定义

/* ----------
 * Total number of backends including auxiliary
 *
 * We reserve a slot for each possible BackendId, plus one for each
 * possible auxiliary process type.  (This scheme assumes there is not
 * more than one of any auxiliary process type at a time.) MaxBackends
 * includes autovacuum workers and background workers as well.
 * ----------
 */
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)

#define UINT32_ACCESS_ONCE(var)		 ((uint32)(*((volatile uint32 *)&(var))))

/*
 * PgDistStatStatus is something like PgBackendStatus (see pgstat.c) but it
 * contains information that a query executed in a cluster database system.
 * Each PgDistStatStatus stands for a backend process forked by postmaster,
 * the same way PgBackendStatus does, like extended fields of PgBackendStatus.
 * We show it in view pg_stat_cluster_activity, still, one tuple for an entry.
 */
typedef struct PgDistStatStatus	// 上面的说明记得改
{
	/*
	 * To avoid locking overhead, we use the following protocol: a backend
	 * increments changecount before modifying its entry, and again after
	 * finishing a modification.  A would-be reader should note the value of
	 * changecount, copy the entry into private memory, then check
	 * changecount again.  If the value hasn't changed, and if it's even,
	 * the copy is valid; otherwise start over.  This makes updates cheap
	 * while reads are potentially expensive, but that's the tradeoff we want.
	 *
	 * The above protocol needs the memory barriers to ensure that the
	 * apparent order of execution is as it desires. Otherwise, for example,
	 * the CPU might rearrange the code so that changecount is incremented
	 * twice before the modification on a machine with weak memory ordering.
	 * This surprising result can lead to bugs.
	 */
    // 持久化状态
	int changecount;
	
	bool valid;                     /* don't show this entry if false */
	char nodename[NAMEDATALEN];     /* nodename, determined after process started */
	char role[NAMEDATALEN];         /* coord, datanode, producer or consumer */

    // 临时状态
	char sessionid[NAMEDATALEN];    /* global session id in a cluster, one for a session */
	
	/* portal_name or portal_name_unique */
	char sqname[NAMEDATALEN];
	/* true if sharequeue end, but currently change when query ends in this backend */
	bool sqdone;
	/* part of plantree this backend is processing, OR last processed if backend is idle */
	char planstate[4096];
	
	/*
	 * portal name: the name of current portal, given by upper node of processing query 
	 * cursor name: contained in planstate this backend is querying, which would be
	 *              portal name of next layer of nodes bellow this backend
	 *              
	 * Note: with these two fields plus nodename, we can build a backend tree of executing query
	 *       in whole distributed system.
	 */
	char portal[NAMEDATALEN];
	char cursors[NAMEDATALEN * 64];

	// 新增字段
    // 存储GID的哈希值，用于在C代码中进行快速、高效的比较和过滤
    uint64 global_query_id_hash; 
    // 存储GID的完整字符串，用于最终在视图中显示
    char   global_query_id[256]; // 预留足够长的空间
    char application_name[NAMEDATALEN];
    TransactionId backend_xid;
    TransactionId backend_xmin;
    char backend_type[NAMEDATALEN];

} PgDistStatStatus;

static PgDistStatStatus *DistStatArray = NULL;
static PgDistStatStatus *MyDistStatEntry = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static pgstat_report_hook_type prev_pgstat_report_hook = NULL;
static PortalStart_hook_type prev_PortalStart = NULL;
static PortalDrop_hook_type prev_PortalDrop = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static bool pgds_enable_planstate; /* whether to show planstate in result sets */
static int pgds_nesting_level = 0;
static char *pgds_gid_guc_string = NULL;

/*
 * Macros to load and store st_changecount with the memory barriers.
 *
 * increment_changecount_before() and
 * increment_changecount_after() need to be called before and after
 * entries are modified, respectively. This makes sure that st_changecount
 * is incremented around the modification.
 *
 * Also save_changecount_before() and save_changecount_after()
 * need to be called before and after entries are copied into private memory
 * respectively.
 */
#define increment_changecount_before(status)	\
	do {	\
		status->changecount++;	\
		pg_write_barrier(); \
	} while (0)

#define increment_changecount_after(status) \
	do {	\
		pg_write_barrier(); \
		status->changecount++;	\
		Assert((status->changecount & 1) == 0); \
	} while (0)

#define save_changecount_before(status, save_changecount)	\
	do {	\
		save_changecount = status->changecount; \
		pg_read_barrier();	\
	} while (0)

#define save_changecount_after(status, save_changecount)	\
	do {	\
		pg_read_barrier();	\
		save_changecount = status->changecount; \
	} while (0)

Datum dist_pg_stat_get_activity(PG_FUNCTION_ARGS);

void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(dist_pg_stat_get_activity);
PG_FUNCTION_INFO_V1(get_dist_pg_locks);

static ParamListInfo
EvaluateSessionIDParam(const char *sessionid)
{
	int num_params = 1;
	ParamListInfo paramLI = (ParamListInfo)
		palloc0(offsetof(ParamListInfoData, params) +
		        num_params * sizeof(ParamExternData));
	
	ParamExternData *prm;
	
	/* we have static list of params, so no hooks needed */
	paramLI->paramFetch = NULL;
	paramLI->paramFetchArg = NULL;
	paramLI->parserSetup = NULL;
	paramLI->parserSetupArg = NULL;
	paramLI->numParams = num_params;
	paramLI->paramMask = NULL;
	
	prm = &paramLI->params[0];
	prm->ptype = TEXTOID;
	prm->pflags = PARAM_FLAG_CONST;
	if (sessionid != NULL)
	{
		prm->value = CStringGetTextDatum(sessionid);
		prm->isnull = false;
	}
	else
	{
		prm->isnull = true;
	}
	
	return paramLI;
}

/*
 * walk through planstate tree and gets cursors it contains in
 * RemoteSubplan node, formed as a single string delimited each
 * cursor by a space (one cursor stands for a RemoteSubplan node).
 */
static bool
cursorCollectWalker(PlanState *planstate, StringInfo str)
{
	if (IsA(planstate, RemoteSubplanState))
	{
		RemoteSubplan *plan = (RemoteSubplan *) planstate->plan;
		if (plan->cursor != NULL)
		{
			appendStringInfoString(str, plan->cursor);
			if (plan->unique)
				appendStringInfo(str, "_"INT64_FORMAT, plan->unique);
			/* add a space as delimiter */
			appendStringInfoString(str, " ");
		}
	}
	
	return planstate_tree_walker(planstate, cursorCollectWalker, str);
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
static void
CreateSharedDistStatus(void)
{
	Size		size;
	bool        found;
	
	/* Create or attach to the shared array */
	size = mul_size(sizeof(PgDistStatStatus), NumBackendStatSlots);
	DistStatArray = (PgDistStatStatus *)
		ShmemInitStruct("Distributed Status Array", size, &found);
	
	if (!found)
	{
		/*
		 * We're the first - initialize.
		 */
		MemSet(DistStatArray, 0, size);
	}
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 */
static void
pgds_shutdown_hook(int code, Datum arg)
{
	volatile PgDistStatStatus *entry = MyDistStatEntry;
	
	/*
	 * Clear my status entry, following the protocol of bumping st_changecount
	 * before and after.  We use a volatile pointer here to ensure the
	 * compiler doesn't try to get cute.
	 */
	increment_changecount_before(entry);
	
	entry->valid = false;	/* mark invalid to hide this entry */
	
	entry->global_query_id_hash = 0; // 新增
    entry->global_query_id[0] = '\0'; // 新增

	increment_changecount_after(entry);
}

/* ----------
 * pgds_entry_initialize() -
 *
 *	Initialize my cluster status entry, and set up our on-proc-exit hook.
 *	as an extension but we don't have hook during process startup, so called
 *	each time the backend try to report something.
 * ----------
 */
static void
pgds_entry_initialize(void)
{
	/* already initialized */
	if (MyDistStatEntry != NULL)
		return;

	if (DistStatArray == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("shared memory for pg_dist_stat_view is not prepared"),
			        errhint("maybe you need to set shared_preload_libraries in postgresql.conf file")));
		return;
	}
	
	/* Initialize MyCSEntry */
	if (MyBackendId != InvalidBackendId)
	{
		Assert(MyBackendId >= 1 && MyBackendId <= MaxBackends);
		MyDistStatEntry = &DistStatArray[MyBackendId - 1];
	}
	else
	{
		/* Must be an auxiliary process */
		Assert(MyAuxProcType != NotAnAuxProcess);
		
		/*
		 * Assign the MyDistStatEntry for an auxiliary process. Since it doesn't
		 * have a BackendId, the slot is statically allocated based on the
		 * auxiliary process type (MyAuxProcType).  Backends use slots indexed
		 * in the range from 1 to MaxBackends (inclusive), so we use
		 * MaxBackends + AuxBackendType + 1 as the index of the slot for an
		 * auxiliary process.
		 */
		MyDistStatEntry = &DistStatArray[MaxBackends + MyAuxProcType];
	}
	
	// 在初始化时，清空GID信息
    MyDistStatEntry->global_query_id_hash = 0;
    MyDistStatEntry->global_query_id[0] = '\0';

	/* also set nodename here, it won't change anyway */
	memcpy(MyDistStatEntry->nodename, PGXCNodeName, strlen(PGXCNodeName) + 1);
	
	/* Set up a process-exit hook to clean up */
	on_shmem_exit(pgds_shutdown_hook, 0);
}

/* ----------
 * pgds_report_common
 * 
 *  Report common fileds of cluster backend status activity,
 *  called by pgds_report_query_activity and pgds_report_activity.
 * ----------
 */
static void
pgds_report_common(PgDistStatStatus *entry)
{
	strncpy((char *) entry->sessionid, PGXCSessionId, NAMEDATALEN);
	
	entry->sqdone = false;
	entry->valid = true;
}

/* ----------
 * pgds_report_role
 * 
 *  Report role, sqname, also if this backend become consumer, remove
 *  previous planstate and cursor.
 * ----------
 */
static void
pgds_report_role(PgDistStatStatus *entry, QueryDesc *desc)
{
	/* fields need queryDesc */
	if (IS_PGXC_DATANODE)
	{
		if (desc != NULL && desc->squeue)
		{
			strncpy((char *) entry->sqname, SqueueName(desc->squeue), NAMEDATALEN);
			if (IsSqueueProducer())
			{
				strncpy((char *) entry->role, "producer", NAMEDATALEN);
			}
			else if (IsSqueueConsumer())
			{
				strncpy((char *) entry->role, "consumer", NAMEDATALEN);
				/* consumer does not know of planstate */
				entry->planstate[0] = '\0';
				entry->cursors[0] = '\0';
			}
			else
			{
				/* do not support */
				entry->role[0] = '\0';
			}
		}
		else if (IsParallelWorker())
		{
			strncpy((char *) entry->role, "parallel worker", NAMEDATALEN);
		}
		else
		{
			strncpy((char *) entry->role, "datanode", NAMEDATALEN);
		}
	}
	else if (IS_PGXC_COORDINATOR)
	{
		strncpy((char *) entry->role, "coordinator", NAMEDATALEN);
	}
	else
	{
		/* do not support */
		entry->role[0] = '\0';
	}
}

/* ----------
 * pgds_report_query_activity
 *
 *  Do nothing but set common field, just enable this cluster entry
 *  to make it visible in the same time as pg_stat_activity. Hooked
 *  in pgstat_report_activity, args are redundant.
 * 	
 */
static void
pgds_report_query_activity(BackendState state, const char *cmd_str)
{
	volatile PgDistStatStatus *entry;
	LocalPgBackendStatus *local_beentry;
	PgBackendStatus *beentry;
	
	// 只调用前任钩子
	if (prev_pgstat_report_hook)
		prev_pgstat_report_hook(state, cmd_str);

    if (MyDistStatEntry && MyDistStatEntry->global_query_id_hash != 0)
    {
        entry = MyDistStatEntry;
        
        // 获取实时的 beentry 来更新动态信息
        local_beentry = pgstat_fetch_stat_local_beentry(MyBackendId);
        if (local_beentry)
        {
             // 更新 backend_type
             beentry = &local_beentry->backendStatus;
             increment_changecount_before(entry);
             snprintf((char *)entry->backend_type, NAMEDATALEN, "%s", pgstat_get_backend_desc(beentry->st_backendType));
             increment_changecount_after(entry);
        }
    }
}

/* ----------
 * pgds_report_executor_activity
 * 
 *  Report fileds of per-query referred, hooked as ExecutorStart_hook
 *  report planstate, cursors and common fields.
 * 
 * 唯一负责写入所有与一次“顶层查询”活动相关的瞬时状态
 * ----------
 */
static void
pgds_report_executor_activity(QueryDesc *desc, int eflags)
{
	volatile PgDistStatStatus *entry;	// volatile 告诉编译器，entry 指向的那块内存随时可能被外部改变，请不要做任何优化
	StringInfo planstate_str = NULL;
	StringInfo cursors = NULL;
	MemoryContext oldcxt;
	char gid_string[256];
	char *gid_to_write;
	LocalPgBackendStatus *local_beentry;
	PgBackendStatus *beentry;
	
	if (prev_ExecutorStart)
		prev_ExecutorStart(desc, eflags);
	else
		standard_ExecutorStart(desc, eflags);
	
	pgds_nesting_level++;	// 嵌套层级就加一

	if (!desc)
		return;
	// 2. 在CN端，为顶层查询生成并设置 GID
    if (pgds_nesting_level == 1)	// 判断一个查询是否为顶层查询，即子查询不再生成新gid
    {
        if (IS_PGXC_COORDINATOR && (pgds_gid_guc_string == NULL || pgds_gid_guc_string[0] == '\0'))
        {
            // 生成 GID
            snprintf(gid_string, sizeof(gid_string), "%s-%d-%lu",
                     PGXCNodeName, MyProcPid, (unsigned long)GetCurrentTimestamp());

            // 设置GUC变量，以便传播到DN
            (void)set_config_option("pg_dist_stat_views.global_query_id", gid_string,
                                    PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SET, true, 0, false);
        }
		/* 统一写入所有瞬时状态 */
		pgds_entry_initialize();
		entry = MyDistStatEntry;
		gid_to_write = (char *)pgds_gid_guc_string;
		// 无论CN还是DN，都在这里，把所有信息一次性写入共享内存
		increment_changecount_before(entry);
		// 写入通用信息
		pgds_report_common((PgDistStatStatus *) entry);

		// --- 在数据节点 (DN) 上，或者作为“中间人”的CN上：读取GUC并存储 --
		// 检查绑定的C变量是否有值 (即GUC是否被传播过来)
		if (gid_to_write && gid_to_write[0] != '\0')
		{	
			snprintf((char *)entry->global_query_id, 
					sizeof(entry->global_query_id), 
					"%s", 
					gid_to_write);	// entry->global_query_id是volatile char *,就是
			entry->global_query_id_hash = string_hash(gid_to_write, strlen(gid_to_write));

		}
		// 写入角色
		pgds_report_role((PgDistStatStatus *)entry, desc);

		// 新增的采集逻辑，连接名称和后台进程相关信息
		local_beentry = pgstat_fetch_stat_local_beentry(MyBackendId);
		if (local_beentry)
        {
            beentry = &local_beentry->backendStatus;
            if (beentry->st_appname)
                snprintf((char *)entry->application_name, NAMEDATALEN, "%s", beentry->st_appname);
            snprintf((char *)entry->backend_type, NAMEDATALEN, "%s", pgstat_get_backend_desc(beentry->st_backendType));
            entry->backend_xid = local_beentry->backend_xid;
            entry->backend_xmin = local_beentry->backend_xmin;
        }

		 /* -- 采集并写入 planstate 和 cursors -- */
        if (desc->already_executed)
		{
			entry->sqdone = true;
		}
		else if (desc->planstate != NULL)
		{
			/* 保留原始的内存管理策略 */
			oldcxt = MemoryContextSwitchTo(desc->estate->es_query_cxt);
			
			/* 采集 Cursors */
			cursors = makeStringInfo();
			cursorCollectWalker(desc->planstate, cursors);
			if (cursors->len > 0)
				snprintf((char *)entry->cursors, sizeof(entry->cursors), "%s", cursors->data);

			/* 采集 Planstate */
			if (pgds_enable_planstate)
			{
				ExplainState es;
				planstate_str = makeStringInfo();
				
				memset(&es, 0, sizeof(es));
				es.str = planstate_str;
				es.costs = false;
				es.skip_remote_query = true;
				
				ExplainBeginOutput(&es);
				ExplainPrintPlan(&es, desc);	// planstate_str被填充
				ExplainEndOutput(&es);
				
				if (planstate_str->len > 0)
					snprintf((char *)entry->planstate, sizeof(entry->planstate), "%s", planstate_str->data);
			}
			else
			{
				snprintf((char *)entry->planstate, sizeof(entry->planstate), "disabled");
			}

			/* 在使用完毕后，立刻释放内存 */
			pfree(cursors->data);
			pfree(cursors);
			if (planstate_str) // 因为 planstate_str 可能不被创建
			{
				pfree(planstate_str->data);
				pfree(planstate_str);
			}

			/* 切换回原来的内存上下文 */
			MemoryContextSwitchTo(oldcxt);
		}
        
		increment_changecount_after(entry);
    }
}

/* ----------
 * pgds_executor_end_hook
 * 用于在查询结束时对进程瞬时信息的清空（除了进程的基本标识信息）
 *  
 *  
 * ----------
 */
static void
pgds_executor_end_hook(QueryDesc *queryDesc)
{
	volatile PgDistStatStatus *v_entry;
	PgDistStatStatus *entry;
	pgds_nesting_level--;	// 嵌套层级减一

    if (pgds_nesting_level == 0)
    {
        /* --- A. 在CN节点上，清理GUC传播信道 --- */
        if (IS_PGXC_COORDINATOR)
        {
            if (pgds_gid_guc_string && pgds_gid_guc_string[0] != '\0')
            {
                (void)set_config_option("pg_dist_stat_views.global_query_id", "",
                                        PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SET, true, 0, false);
            }
        }

        /* --- B. 在所有节点上，清理自己共享内存里的GID --- */
        // MyDistStatEntry 在进程生命周期中，只要初始化过就不会是 NULL
        if (MyDistStatEntry)
        {
            v_entry = MyDistStatEntry;
			entry = (PgDistStatStatus *)v_entry;

            increment_changecount_before(v_entry);
            // v_entry->nodename 和 v_entry->role 被刻意保留，不作清理！
            MemSet(&entry->sessionid, 0, 
                   sizeof(PgDistStatStatus) - offsetof(PgDistStatStatus, sessionid));
            
            increment_changecount_after(v_entry);

        }
    }
		// 调用原始的钩子链
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);

}

/* ----------
 * pgds_report_activity
 * 
 *  Report fileds of per-portal referred, hooked as PortalStart_hook
 *  report portal name and common fields.
 * 
 * 职责：只负责更新与 `Portal` 严格绑定的信息，比如 `portal` 名字。在 Portal 销毁时，清理掉这个名字。
 * ----------
 */
static void
pgds_report_activity(Portal portal, bool is_drop)	// 新增一个参数区分是Portal Start 还是 Drop
{
	volatile PgDistStatStatus *entry;
	
	if (is_drop)
	{
		if (prev_PortalDrop)
			prev_PortalDrop(portal);
	}
	else
	{
		if (prev_PortalStart)
			prev_PortalStart(portal);
	}

	pgds_entry_initialize();
	entry = MyDistStatEntry;
	
	/* if query already done, just report sqdone and return */
	// if (desc != NULL && desc->already_executed)
	// {
	// 	increment_changecount_before(entry);
	// 	entry->sqdone = true;
	// 	increment_changecount_after(entry);
	// 	return;
	// }
	
	increment_changecount_before(entry);

    if (is_drop)
    {
        // 如果是 PortalDrop，就清空 portal 名字
        entry->portal[0] = '\0';
    }
    else
    {
        // 如果是 PortalStart，就写入 portal 名字
        strncpy((char *)entry->portal, portal->name, NAMEDATALEN);
    }
    
    increment_changecount_after(entry);
}

// 为了适配钩子函数的不同签名，我们需要两个“包装”函数
static void
pgds_portal_start_hook(Portal portal)
{
    pgds_report_activity(portal, false);
}

static void
pgds_portal_drop_hook(Portal portal)
{
    pgds_report_activity(portal, true);
}

/* ----------
 * pgstat_fetch_stat_local_csentry
 * 
 *  Given a backend id, find particular cluster status entry, copy valid
 *  entry into local memory, loop around changecount to ensure concurrency.
 * ----------
 */
static PgDistStatStatus *
pgstat_fetch_stat_local_dsentry(int beid)
{
	PgDistStatStatus *dsentry;
	PgDistStatStatus *local = palloc(sizeof(PgDistStatStatus));
	local->valid = false;
	
	if (DistStatArray == NULL)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			        errmsg("shared memory for pg_stat_cluster_activity is not prepared"),
			        errhint("maybe you need to set shared_preload_libraries in postgresql.conf")));
		return NULL;
	}
	
	if (beid < 1)
		return NULL;
	
	dsentry = &DistStatArray[beid - 1];
	
	for (;;)
	{
		int			before_changecount;
		int			after_changecount;
		
		save_changecount_before(dsentry, before_changecount);
		if (dsentry->valid)
		{
			memcpy(local, dsentry, sizeof(PgDistStatStatus));
		}
		save_changecount_after(dsentry, after_changecount);
		if (before_changecount == after_changecount &&
		    (before_changecount & 1) == 0)
			break;
		
		/* Make sure we can break out of loop if stuck... */
		CHECK_FOR_INTERRUPTS();
	}
	
	return local;
}

/* ----------
 * dist_pg_get_remote_activity
 * 
 *  Execute dist_pg_stat_get_activity query remotely and save
 *  results in the given tuplestore.
 */
static void
dist_pg_get_remote_activity(const char *sessionid, bool coordonly, Tuplestorestate *tupstore, TupleDesc tupdesc)
{
#define QUERY_LEN 1024
	char    query[QUERY_LEN];
	EState              *estate;
	MemoryContext		oldcontext;
	RemoteQuery 		*plan;
	RemoteQueryState    *pstate;
	TupleTableSlot		*result = NULL;
	
	/*
	* Here we call dist_pg_stat_get_activity remotely with args:
	* coordonly = false, localonly = true, to prevent recursive calls on remote nodes.
	*/
	snprintf(query, QUERY_LEN, "select * from dist_pg_stat_get_activity($1, false, true)");
	
	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	/*
	 * set exec_nodes to NULL makes ExecRemoteQuery send query to all nodes
	 * (local CN nodes won't recieved query again).
	 */
	plan->exec_nodes = NULL;
	plan->exec_type = EXEC_ON_ALL_NODES;
	plan->sql_statement = (char *) query;
	plan->force_autocommit = false;
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_nodes->missing_ok = true;
	
	if (coordonly)
	{
		plan->exec_nodes->nodeList = GetAllCoordNodes();
		plan->exec_type = EXEC_ON_COORDS;
	}
	
	/* prepare to execute */
	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = EvaluateSessionIDParam(sessionid);
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	ExecAssignResultType((PlanState *) pstate, tupdesc);
	MemoryContextSwitchTo(oldcontext);
	
	result = ExecRemoteQuery((PlanState *) pstate);
	
	while (result != NULL && !TupIsNull(result))
	{
		slot_getallattrs(result);
		
		tuplestore_puttupleslot(tupstore, result);
		result = ExecRemoteQuery((PlanState *) pstate);
	}
	
	ExecEndRemoteQuery(pstate);
	FreeExecutorState(estate);
}

/* ----------
 * dist_pg_stat_get_activity
 * 
 *  Internal SRF function of this extension. It accesses shared memory to find
 *  every live backend, collects their local and distributed status,
 *  and presents the information. It combines fields from both PgBackendStatus
 *  and our custom PgDistStatStatus.
 *
 *  arguments:  sessionid -- global unique id for a session, generated by CN
 *              coordonly -- only dispatch to other CNs if true.
 *              localonly -- only collect local entries' status if true.
 *              
 *  Note: since we also collect PGBackendStatus, get them first and use
 *  backend id to access a particular distributed status entry to narrow down
 *  the loop search range from all backend slots to localNumBackends (see pgstat.c)
 * ----------
 */
Datum
dist_pg_stat_get_activity(PG_FUNCTION_ARGS)
{
	int              num_backends = pgstat_fetch_stat_numbackends();
	int			     curr_backend;
	bool             with_sessionid = !PG_ARGISNULL(0);
	bool             coordonly = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	bool             localonly = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	const char      *sessionid = with_sessionid ? text_to_cstring(PG_GETARG_TEXT_P(0)) : NULL;
	ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	     tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext    per_query_ctx;
	MemoryContext    oldcontext;
	
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
	if (!localonly && IS_PGXC_COORDINATOR)
		dist_pg_get_remote_activity(sessionid, coordonly, tupstore, tupdesc);
	
	/* 1-based index */
	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
		/* for each row */
		Datum		values[PG_DIST_STAT_ACTIVITY_COLS];
		bool		nulls[PG_DIST_STAT_ACTIVITY_COLS];
		
		/* same as pg_stat_get_activity */
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		PGPROC	   *proc;
		const char *wait_event_type = NULL;
		const char *wait_event = NULL;
		
		/* cluster information */
		PgDistStatStatus *local_dsentry;
		
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		
		/* Get the next one in the list */	// ????
		local_beentry = pgstat_fetch_stat_local_beentry(curr_backend);
		local_dsentry = pgstat_fetch_stat_local_dsentry(local_beentry->backend_id);
		if (!local_beentry || !local_dsentry)
		{
			int			i;
			
			/* Ignore missing entries if looking for specific sessionid */
			if (with_sessionid)
				continue;
			
			for (i = 0; i < lengthof(nulls); i++)
				nulls[i] = true;
			
			nulls[13] = false;
			values[13] = CStringGetTextDatum("<backend information not available>");
			
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			continue;
		}
		
		if (!local_dsentry->valid)
			continue;
		
		beentry = &local_beentry->backendStatus;
		/* If looking for specific sessionid, ignore all the others */
		if (with_sessionid && strcmp(sessionid, local_dsentry->sessionid) != 0)
			continue;
		
		/* Values available to all callers */
		values[0] = CStringGetTextDatum(local_dsentry->sessionid);
		values[1] = Int32GetDatum(beentry->st_procpid);
		
		if (beentry->st_databaseid != InvalidOid)
		{
			char *dbname = get_database_name(beentry->st_databaseid);
			if (dbname != NULL)
				values[7] = CStringGetTextDatum(dbname);
			else
				nulls[7] = true;
		}
		else
			nulls[7] = true;
		
		if (beentry->st_userid != InvalidOid)
		{
			char *usename = GetUserNameFromId(beentry->st_userid, true);
			if (usename != NULL)
				values[8] = CStringGetTextDatum(usename);
			else
				nulls[8] = true;
		}
		else
			nulls[8] = true;
		
		/* Values only available to owner or superuser or pg_read_all_stats */
		if (has_privs_of_role(GetUserId(), beentry->st_userid) ||
		    is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS))
		{
			SockAddr	zero_clientaddr;
			
			/* A zeroed client addr means we don't know */
			memset(&zero_clientaddr, 0, sizeof(zero_clientaddr));
			if (memcmp(&(beentry->st_clientaddr), &zero_clientaddr,
			           sizeof(zero_clientaddr)) == 0)
			{
				nulls[2] = true;
				nulls[3] = true;
				nulls[4] = true;
			}
			else
			{
				if (beentry->st_clientaddr.addr.ss_family == AF_INET
#ifdef HAVE_IPV6
				    || beentry->st_clientaddr.addr.ss_family == AF_INET6
#endif
					)
				{
					char		remote_host[NI_MAXHOST];
					char		remote_port[NI_MAXSERV];
					int			ret;
					
					remote_host[0] = '\0';
					remote_port[0] = '\0';
					ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr,
					                         beentry->st_clientaddr.salen,
					                         remote_host, sizeof(remote_host),
					                         remote_port, sizeof(remote_port),
					                         NI_NUMERICHOST | NI_NUMERICSERV);
					if (ret == 0)
					{
						clean_ipv6_addr(beentry->st_clientaddr.addr.ss_family, remote_host);
						values[2] = DirectFunctionCall1(inet_in,
						                                CStringGetDatum(remote_host));
						if (beentry->st_clienthostname &&
						    beentry->st_clienthostname[0])
							values[3] = CStringGetTextDatum(beentry->st_clienthostname);
						else
							nulls[3] = true;
						values[4] = Int32GetDatum(atoi(remote_port));
					}
					else
					{
						nulls[2] = true;
						nulls[3] = true;
						nulls[4] = true;
					}
				}
				else if (beentry->st_clientaddr.addr.ss_family == AF_UNIX)
				{
					/*
					 * Unix sockets always reports NULL for host and -1 for
					 * port, so it's possible to tell the difference to
					 * connections we have no permissions to view, or with
					 * errors.
					 */
					nulls[2] = true;
					nulls[3] = true;
					values[4] = DatumGetInt32(-1);
				}
				else
				{
					/* Unknown address type, should never happen */
					nulls[2] = true;
					nulls[3] = true;
					nulls[4] = true;
				}
			}
			
			values[5] = CStringGetTextDatum(local_dsentry->nodename);
			values[6] = CStringGetTextDatum(local_dsentry->role);
			
			proc = BackendPidGetProc(beentry->st_procpid);
			if (proc != NULL)
			{
				uint32		raw_wait_event;
				
				raw_wait_event = UINT32_ACCESS_ONCE(proc->wait_event_info);
				wait_event_type = pgstat_get_wait_event_type(raw_wait_event);
				wait_event = pgstat_get_wait_event(raw_wait_event);
			}
			else if (beentry->st_backendType != B_BACKEND)
			{
				/*
				 * For an auxiliary process, retrieve process info from
				 * AuxiliaryProcs stored in shared-memory.
				 */
				proc = AuxiliaryPidGetProc(beentry->st_procpid);
				
				if (proc != NULL)
				{
					uint32		raw_wait_event;
					
					raw_wait_event =
						UINT32_ACCESS_ONCE(proc->wait_event_info);
					wait_event_type =
						pgstat_get_wait_event_type(raw_wait_event);
					wait_event = pgstat_get_wait_event(raw_wait_event);
				}
			}
			
			if (wait_event_type)
				values[9] = CStringGetTextDatum(wait_event_type);
			else
				nulls[9] = true;
			
			if (wait_event)
				values[10] = CStringGetTextDatum(wait_event);
			else
				nulls[10] = true;
			
			switch (beentry->st_state)
			{
				case STATE_IDLE:
					values[11] = CStringGetTextDatum("idle");
					break;
				case STATE_RUNNING:
					values[11] = CStringGetTextDatum("active");
					break;
				case STATE_IDLEINTRANSACTION:
					values[11] = CStringGetTextDatum("idle in transaction");
					break;
				case STATE_FASTPATH:
					values[11] = CStringGetTextDatum("fastpath function call");
					break;
				case STATE_IDLEINTRANSACTION_ABORTED:
					values[11] = CStringGetTextDatum("idle in transaction (aborted)");
					break;
				case STATE_DISABLED:
					values[11] = CStringGetTextDatum("disabled");
					break;
				case STATE_UNDEFINED:
					nulls[11] = true;
					break;
			}
			
			values[12] = CStringGetTextDatum(local_dsentry->sqname);
			values[13] = BoolGetDatum(local_dsentry->sqdone);
			values[14] = CStringGetTextDatum(beentry->st_activity);
			values[15] = CStringGetTextDatum(local_dsentry->planstate);
			values[16] = CStringGetTextDatum(local_dsentry->portal);
			values[17] = CStringGetTextDatum(local_dsentry->cursors);
			
			if (beentry->st_proc_start_timestamp != 0)
				values[18] = TimestampTzGetDatum(beentry->st_proc_start_timestamp);
			else
				nulls[18] = true;
			
			if (beentry->st_xact_start_timestamp != 0)
				values[19] = TimestampTzGetDatum(beentry->st_xact_start_timestamp);
			else
				nulls[19] = true;
			
			if (beentry->st_activity_start_timestamp != 0)
				values[20] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
			else
				nulls[20] = true;
			
			if (beentry->st_state_start_timestamp != 0)
				values[21] = TimestampTzGetDatum(beentry->st_state_start_timestamp);
			else
				nulls[21] = true;

			if (local_dsentry->application_name[0] != '\0')
				values[22] = CStringGetTextDatum(local_dsentry->application_name);
			else
				nulls[22] = true;
			
			if (TransactionIdIsValid(local_dsentry->backend_xid))
				values[23] = TransactionIdGetDatum(local_dsentry->backend_xid);
			else
				nulls[23] = true;

			if (TransactionIdIsValid(local_dsentry->backend_xmin))
				values[24] = TransactionIdGetDatum(local_dsentry->backend_xmin);
			else
				nulls[24] = true;

			if (local_dsentry->backend_type[0] != '\0')
				values[25] = CStringGetTextDatum(local_dsentry->backend_type);
			else
				nulls[25] = true;

			if (local_dsentry->global_query_id[0] != '\0')
				values[26] = CStringGetTextDatum(local_dsentry->global_query_id);
			else
				nulls[26] = true;
		}
		else
		{
			values[14] = CStringGetTextDatum("<insufficient privilege>");
			nulls[2] = true;
			nulls[3] = true;
			nulls[4] = true;
			nulls[5] = true;
			nulls[6] = true;
			nulls[9] = true;
			nulls[10] = true;
			nulls[11] = true;
			nulls[12] = true;
			nulls[13] = true;
			nulls[15] = true;
			nulls[16] = true;
			nulls[17] = true;
			nulls[18] = true;
			nulls[19] = true;
			nulls[20] = true;
			nulls[21] = true;
			nulls[22] = true;
			nulls[23] = true;
			nulls[24] = true;
			nulls[25] = true;
			nulls[26] = true;
			nulls[27] = true;
		}
		
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	
	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	
	return (Datum) 0;
}

/*
 * ===================================================================
 *                    LOCKS INFORMATION FUNCTIONS
 * ===================================================================
 */

static Datum
VXIDGetDatum(BackendId bid, LocalTransactionId lxid)
{
    char vxidstr[32];
    snprintf(vxidstr, sizeof(vxidstr), "%d/%u", bid, lxid);
    return CStringGetTextDatum(vxidstr);
}

/*
 * dist_pg_get_local_locks - 安全的本地数据采集器
 *
 * 它的职责是安全地采集本地锁信息，并将其放入一个传入的 Tuplestore。
 * 它不是SRF，只是一个普通的辅助函数。
 */
static void
dist_pg_get_local_locks(Tuplestorestate *tupstore, TupleDesc tupdesc)
{
    LockData   *lockData;
    PredicateLockData *predLockData;
    int         i;
	static const char *const PredicateLockTagTypeNames[] = {"relation", "page", "tuple"};

#define NUM_DIST_LOCKS_RAW_COLS 17
    Datum       values[NUM_DIST_LOCKS_RAW_COLS];
    bool        nulls[NUM_DIST_LOCKS_RAW_COLS];

    const char *const LockTagTypeNames[] = {
        "relation", "extend", "page", "tuple", "transactionid",
        "virtualxid", "speculative token", "object",
#ifdef _SHARDING_
        "shard",
#endif
        "userlock", "advisory"
    };

    // --- 1. 处理常规锁 (Regular Locks) ---
    lockData = GetLockStatusData();
    
    for (i = 0; i < lockData->nelements; i++)
    {
        LockInstanceData *instance = &(lockData->locks[i]);
        PGPROC     *proc = BackendPidGetProc(instance->pid);
		// 处理持有的锁
        uint32      holdMask = instance->holdMask;
        LOCKMODE    mode;
        
        if (!proc) 
            continue;

        for (mode = 0; mode < MAX_LOCKMODES; mode++)
        {
            if (holdMask & LOCKBIT_ON(mode))
            {
                MemSet(values, 0, sizeof(values));
                MemSet(nulls, false, sizeof(nulls));
                
                values[0] = CStringGetTextDatum(PGXCNodeName);
                if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
                    values[1] = CStringGetTextDatum(LockTagTypeNames[instance->locktag.locktag_type]);
                else
                    values[1] = CStringGetTextDatum("unknown");
                switch ((LockTagType) instance->locktag.locktag_type)
                {
                    case LOCKTAG_RELATION:
                    case LOCKTAG_RELATION_EXTEND:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        nulls[4]=nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_PAGE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TUPLE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        values[5] = UInt16GetDatum(instance->locktag.locktag_field4);
                        nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TRANSACTION:
                        values[7] = TransactionIdGetDatum(instance->locktag.locktag_field1);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_VIRTUALTRANSACTION:
                        values[6] = VXIDGetDatum(instance->locktag.locktag_field1, instance->locktag.locktag_field2);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_OBJECT:
                    case LOCKTAG_USERLOCK:
                    case LOCKTAG_ADVISORY:
                    default:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[8] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[9] = ObjectIdGetDatum(instance->locktag.locktag_field3);
                        values[10] = Int16GetDatum(instance->locktag.locktag_field4);
                        nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[7]=true;
                        break;
                }
                values[11] = VXIDGetDatum(instance->backend, instance->lxid);
                values[12] = Int32GetDatum(instance->pid);
                values[13] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, mode));
                values[14] = BoolGetDatum(true);
                values[15] = BoolGetDatum(instance->fastpath);
                if (proc->hasGlobalXid && proc->globalXid[0] != '\0')
                    values[16] = CStringGetTextDatum(proc->globalXid);
                else
                    nulls[16] = true;
                tuplestore_putvalues(tupstore, tupdesc, values, nulls);
            }
        }
        
        // 处理等待的锁
        if (instance->waitLockMode != NoLock)
        {
            MemSet(values, 0, sizeof(values));
            MemSet(nulls, false, sizeof(nulls));
            
            values[0] = CStringGetTextDatum(PGXCNodeName);
            if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
                values[1] = CStringGetTextDatum(LockTagTypeNames[instance->locktag.locktag_type]);
            else
                values[1] = CStringGetTextDatum("unknown");
            switch ((LockTagType) instance->locktag.locktag_type)
            {
                    case LOCKTAG_RELATION:
                    case LOCKTAG_RELATION_EXTEND:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        nulls[4]=nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_PAGE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        nulls[5]=nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TUPLE:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[3] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[4] = UInt32GetDatum(instance->locktag.locktag_field3);
                        values[5] = UInt16GetDatum(instance->locktag.locktag_field4);
                        nulls[6]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_TRANSACTION:
                        values[7] = TransactionIdGetDatum(instance->locktag.locktag_field1);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_VIRTUALTRANSACTION:
                        values[6] = VXIDGetDatum(instance->locktag.locktag_field1, instance->locktag.locktag_field2);
                        nulls[2]=nulls[3]=nulls[4]=nulls[5]=nulls[7]=nulls[8]=nulls[9]=nulls[10]=true;
                        break;
                    case LOCKTAG_OBJECT:
                    case LOCKTAG_USERLOCK:
                    case LOCKTAG_ADVISORY:
                    default:
                        values[2] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                        values[8] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                        values[9] = ObjectIdGetDatum(instance->locktag.locktag_field3);
                        values[10] = Int16GetDatum(instance->locktag.locktag_field4);
                        nulls[3]=nulls[4]=nulls[5]=nulls[6]=nulls[7]=true;
                        break;
            }
            values[11] = VXIDGetDatum(instance->backend, instance->lxid);
            values[12] = Int32GetDatum(instance->pid);
            values[13] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, instance->waitLockMode));
            values[14] = BoolGetDatum(false);
            values[15] = BoolGetDatum(instance->fastpath);
            if (proc->hasGlobalXid && proc->globalXid[0] != '\0')
                values[16] = CStringGetTextDatum(proc->globalXid);
            else
                nulls[16] = true;
            tuplestore_putvalues(tupstore, tupdesc, values, nulls);
        }
    }

    // --- 2. 处理谓词锁 (Predicate Locks) ---
    predLockData = GetPredicateLockStatusData();

    for (i = 0; i < predLockData->nelements; i++)
    {
        PredicateLockTargetType lockType;
        PREDICATELOCKTARGETTAG *predTag = &(predLockData->locktags[i]);
        SERIALIZABLEXACT *xact = &(predLockData->xacts[i]);
        PGPROC *proc;

        proc = BackendPidGetProc(xact->pid);

        MemSet(values, 0, sizeof(values));
        MemSet(nulls, false, sizeof(nulls));

        // 第1列: node_name
        values[0] = CStringGetTextDatum(PGXCNodeName);
        
        // --- 第2-16列: 填充谓词锁信息 ---
        lockType = GET_PREDICATELOCKTARGETTAG_TYPE(*predTag);
        values[1] = CStringGetTextDatum(PredicateLockTagTypeNames[lockType]); // locktype

        values[2] = GET_PREDICATELOCKTARGETTAG_DB(*predTag);      // database
        values[3] = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag); // relation

        if (lockType == PREDLOCKTAG_TUPLE)
            values[5] = GET_PREDICATELOCKTARGETTAG_OFFSET(*predTag); // tuple
        else
            nulls[5] = true;

        if (lockType == PREDLOCKTAG_TUPLE || lockType == PREDLOCKTAG_PAGE)
            values[4] = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);   // page
        else
            nulls[4] = true;
            
        nulls[6] = true;   // virtualxid
        nulls[7] = true;   // transactionid
        nulls[8] = true;   // classid
        nulls[9] = true;   // objid
        nulls[10] = true;  // objsubid

        values[11] = VXIDGetDatum(xact->vxid.backendId, xact->vxid.localTransactionId); // virtualtransaction
        if (xact->pid != 0)
            values[12] = Int32GetDatum(xact->pid); // pid
        else
            nulls[12] = true;

        values[13] = CStringGetTextDatum("SIReadLock"); // mode
        values[14] = BoolGetDatum(true);             // granted
        values[15] = BoolGetDatum(false);            // fastpath
        
        // 第17列: gxid
        if (proc && proc->hasGlobalXid && proc->globalXid[0] != '\0')
            values[16] = CStringGetTextDatum(proc->globalXid);
        else
            nulls[16] = true;

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }
}

/*
 * dist_pg_get_remote_locks - 远程执行 get_dist_pg_locks(true)
 */
static void
dist_pg_get_remote_locks(Tuplestorestate *tupstore, TupleDesc tupdesc)
{
	char    query[256];
	EState *estate;
	RemoteQuery *plan;
	RemoteQueryState *pstate;
	TupleTableSlot *result = NULL;
	MemoryContext oldcontext;

	snprintf(query, sizeof(query), "SELECT * FROM @extschema@.get_dist_pg_locks(true)");

	plan = makeNode(RemoteQuery);
	plan->combine_type = COMBINE_TYPE_NONE;
	
	plan->exec_nodes = makeNode(ExecNodes);
	plan->exec_nodes->nodeList = list_concat(GetAllCoordNodes(), GetAllDataNodes());
	plan->exec_nodes->missing_ok = true;
	plan->exec_type = EXEC_ON_ALL_NODES;

	plan->sql_statement = query;
	plan->force_autocommit = false;

	estate = CreateExecutorState();
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
	estate->es_snapshot = GetActiveSnapshot();
	estate->es_param_list_info = NULL;
	pstate = ExecInitRemoteQuery(plan, estate, 0);
	ExecAssignResultType((PlanState *) pstate, tupdesc);
	MemoryContextSwitchTo(oldcontext);

	while ((result = ExecRemoteQuery((PlanState *) pstate)) != NULL && !TupIsNull(result))
	{
		tuplestore_puttupleslot(tupstore, result);
	}

	ExecEndRemoteQuery(pstate);
	FreeExecutorState(estate);
}

/*
 * get_dist_pg_locks - 最终SQL入口
 * 采用物化模式，协调远程和本地的数据采集。
 */
Datum
get_dist_pg_locks(PG_FUNCTION_ARGS)
{
    bool localonly = PG_GETARG_BOOL(0);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx, oldcontext;
    
    // 检查调用上下文是否支持返回集合
    if (!IsA(rsinfo, ReturnSetInfo) || !(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));

    // 获取期望返回的元组描述 (TupleDesc)
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // 切换到查询级别的内存上下文，以确保Tuplestore的生命周期正确
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    // 创建 Tuplestore
    tupstore = tuplestore_begin_heap(true, false, work_mem);

    // 告诉PostgreSQL执行器，我们将使用物化模式返回结果
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore; // 将 tupstore 挂在返回结果上
    rsinfo->setDesc = tupdesc;      // 将 tupdesc 挂在返回结果上

	// 1. 如果是CN，且非localonly，则进行远程数据采集
    if (!localonly && IS_PGXC_COORDINATOR)
        dist_pg_get_remote_locks(tupstore, tupdesc);

    // 2. 然后采集本地节点的数据
    dist_pg_get_local_locks(tupstore, tupdesc);
    
    tuplestore_donestoring(tupstore);
    MemoryContextSwitchTo(oldcontext);

    return (Datum) 0;
}

/*
 * Hooked as shmem_startup_hook
 */
static void
pgds_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	
	CreateSharedDistStatus();
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgds_memsize(void)
{
	return mul_size(sizeof(PgDistStatStatus), NumBackendStatSlots);
}

/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;
	
	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomBoolVariable("pg_dist_stat_views.enable_planstate",
	                         "whether to show planstate in result sets.",
	                         NULL,
	                         &pgds_enable_planstate,
	                         true,
	                         PGC_SUSET,
	                         0,
	                         NULL,
	                         NULL,
	                         NULL);

	// 定义自己用于调试的GUC变量
	DefineCustomStringVariable(
							"pg_dist_stat_views.global_query_id",
							"Internal GUC to propagate Global Query ID.",
							NULL,
							&pgds_gid_guc_string,
							"",
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL
	);
	
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgds_shmem_startup().
	 */
	RequestAddinShmemSpace(pgds_memsize());
	
	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgds_shmem_startup;
	prev_pgstat_report_hook = pgstat_report_hook;
	pgstat_report_hook = pgds_report_query_activity;
	prev_PortalStart = PortalStart_hook;
	PortalStart_hook = pgds_portal_start_hook;
	prev_PortalDrop = PortalDrop_hook;
	PortalDrop_hook = pgds_portal_drop_hook;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgds_report_executor_activity;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgds_executor_end_hook;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	pgstat_report_hook = prev_pgstat_report_hook;
	PortalStart_hook = pgds_portal_start_hook;
	PortalDrop_hook = pgds_portal_drop_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorEnd_hook = prev_ExecutorEnd;
}
