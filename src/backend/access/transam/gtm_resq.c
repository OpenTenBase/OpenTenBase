/*-------------------------------------------------------------------------
 *
 * gtm_resq.c
 *
 *	  Module interfacing with GTM
 *
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"
#include "gtm/libpq-fe.h"
#include "gtm/gtm_client.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "access/xact.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "gtm/gtm_c.h"
#include "postmaster/autovacuum.h"
#include "postmaster/clustermon.h"
#include "storage/backendid.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"
#ifdef __OPENTENBASE__
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pgxc_node.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "pgxc/nodemgr.h"
#endif
#ifdef __OPENTENBASE_C__
#include "gtm/gtm_fid.h"
#include "pgxc/pgxcnode.h"
#endif

#ifdef __RESOURCE_QUEUE__
#include "commands/resqueue.h"
#endif

extern bool	   GTMDebugPrint;

static int GtmConnectTimeout = 60;
static GTM_Conn *resq_conn = NULL;

static void ResQueue_CheckConnection(void);
static void ResQueue_InitGTM(void);

static void
ResQueue_CheckConnection(void)
{
#ifdef __OPENTENBASE__
	/* First time try connect to gtm, get gtm info from syscache first */
	if (NULL == GtmHost && 0 == GtmPort)
	{
		GetMasterGtmInfo();
	}

	/* If NewGtmHost and NewGtmPort were set, we are in create/alter gtm node command */
	if (NewGtmHost && NewGtmPort != 0)
	{
		ResetGtmInfo();
		
		GtmHost = strdup(NewGtmHost);
		GtmPort = NewGtmPort;

		free(NewGtmHost);
		NewGtmHost = NULL;
		NewGtmPort = 0;

		/* Close old gtm connection */
		ResQueue_CloseGTM();
	}
#endif

	/* Be sure that a backend does not use a postmaster connection */
	if (IsUnderPostmaster && GTMPQispostmaster(resq_conn) == 1)
	{
		ResQueue_InitGTM();
		return;
	}

	if (GTMPQstatus(resq_conn) != CONNECTION_OK)
		ResQueue_InitGTM();
}

static void
ResQueue_InitGTM(void)
{
#define  CONNECT_STR_LEN   256 /* 256 bytes should be enough */
	char conn_str[CONNECT_STR_LEN] = { 0 };
#ifdef __OPENTENBASE__
	int  try_cnt = 0;
	const int max_try_cnt = 2;
#endif

try_connect_gtm:
	/* If this thread is postmaster itself, it contacts gtm identifying itself */
	if (!IsUnderPostmaster)
	{
		GTM_PGXCNodeType remote_type = GTM_NODE_DEFAULT;

		if (IS_PGXC_COORDINATOR)
			remote_type = GTM_NODE_COORDINATOR;
		else if (IS_PGXC_DATANODE)
			remote_type = GTM_NODE_DATANODE;
		/* Use 60s as connection timeout */
		snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s remote_type=%d postmaster=1 connect_timeout=%d",
								GtmHost, GtmPort, PGXCNodeName, remote_type,
								GtmConnectTimeout);

		/* Log activity of GTM connections */
		if(GTMDebugPrint)
			elog(LOG, "Postmaster: connection established to GTM for Resource Queue with string %s", conn_str);
	}
	else
	{
		/* Use 60s as connection timeout */
		snprintf(conn_str, CONNECT_STR_LEN, "host=%s port=%d node_name=%s connect_timeout=%d",
				GtmHost, GtmPort, PGXCNodeName, GtmConnectTimeout);

		/* Log activity of GTM connections */
		if(GTMDebugPrint)
			elog(LOG, "Postmaster child: connection established to GTM for Resource Queue with string %s", conn_str);
	}

	resq_conn = PQconnectGTM(conn_str);
	if (GTMPQstatus(resq_conn) != CONNECTION_OK)
	{
		int save_errno = errno;
		
#ifdef __OPENTENBASE__	
		if (try_cnt < max_try_cnt)
		{
			/* If connect gtm failed, get gtm info from syscache, and try again */
			GetMasterGtmInfo();
			if (GtmHost != NULL && GtmPort)
			{
				elog(DEBUG1, "[ResQueue_InitGTM] Get GtmHost:%s  GtmPort:%d try_cnt:%d max_try_cnt:%d", 
							 GtmHost, GtmPort, try_cnt, max_try_cnt);
			}
			ResQueue_CloseGTM();
			try_cnt++;
			goto try_connect_gtm;
		}
		else
#endif		
		{
			ResetGtmInfo();

			/* Use LOG instead of ERROR to avoid error stack overflow. */
			if(resq_conn)
			{
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("can not connect to GTM: %s %m",
								GTMPQerrorMessage(resq_conn))));
			}
			else
			{
				ereport(LOG,
						(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
						 errmsg("connection is null: %m")));
			}

			errno = save_errno;

			ResQueue_CloseGTM();
		}
		
	}
	else
	{
		int32	move_success = -2;

		/* Connect Success, dispatch resq_conn to the ResourceQueueManager thread.*/
		if (resqueue_move_conn(resq_conn, PGXCNodeName, MyProcPid, MyBackendId, &move_success) < 0 ||
			move_success != 0)
		{
			int save_errno = errno;
		
			if (try_cnt < max_try_cnt)
			{
				/* If connect gtm failed, get gtm info from syscache, and try again */
				GetMasterGtmInfo();
				if (GtmHost != NULL && GtmPort)
				{
					elog(DEBUG1, "[ResQueue_InitGTM] Get GtmHost:%s  GtmPort:%d try_cnt:%d max_try_cnt:%d", 
								 GtmHost, GtmPort, try_cnt, max_try_cnt);
				}
				ResQueue_CloseGTM();
				try_cnt++;
				goto try_connect_gtm;
			}
			else
			{
				ResetGtmInfo();

				/* Use LOG instead of ERROR to avoid error stack overflow. */
				if(resq_conn)
				{
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
							 errmsg("can not connect to GTM: %s %m",
									GTMPQerrorMessage(resq_conn))));
				}
				else
				{
					ereport(LOG,
							(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
							 errmsg("connection is null: %m")));
				}

				errno = save_errno;

				ResQueue_CloseGTM();
			}
		}
	}
}

void
ResQueue_CloseGTM(void)
{
	if (resq_conn)
	{
		GTMPQfinish(resq_conn);
		resq_conn = NULL;
	}

	elog(DEBUG1, "Postmaster child: connection to GTM for Resource Queue closed");
}

int
AcquireResQueueGTM(NameData * resq_name, int64 memory_Bytes, ResQueueInfo * result, int32 * errcode)
{
	ResQueue_CheckConnection();

	if (resq_conn)
	{
		return resqueue_acquire(resq_conn, 
									PGXCNodeName,
									MyProcPid,
									MyBackendId,
									resq_name,
									memory_Bytes, 
									&result->resq_name,
									&result->resq_group,
									&result->resq_memory_limit,
									&result->resq_network_limit,
									&result->resq_active_stmts,
									&result->resq_wait_overload,
									&result->resq_priority,
									errcode);
	}
	else
	{
		return -1;
	}
}

int
ReleaseResQueueGTM(NameData * resq_name, int64	memory_Bytes, int32 * errcode)
{
	ResQueue_CheckConnection();

	if (resq_conn)
	{
		return resqueue_release(resq_conn,
									PGXCNodeName,
									MyProcPid,
									MyBackendId,
									resq_name,
									memory_Bytes,
									errcode);
	}
	else
	{
		return -1;
	}
}
