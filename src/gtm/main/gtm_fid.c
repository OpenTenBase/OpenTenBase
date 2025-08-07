/*-------------------------------------------------------------------------
 *
 * gtm_fid.c
 *	Fragment Id handling
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_txn.h"

#include <unistd.h>
#include <gtm/gtm_xlog.h>
#include <gtm/gtm_xlog_internal.h>
#include "gtm/assert.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_fid.h"
#include "gtm/gtm_serialize.h"
#include "gtm/gtm_standby.h"
#include "gtm/standby_utils.h"
#include "gtm/libpq.h"
#include "gtm/libpq-int.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_backup.h"
#include "gtm/gtm_store.h"
#include "gtm/gtm_time.h"

#define STR_FIDS_LEN (GTM_MAX_FRAGMENTS * 6)

GTM_FidManager GTMFidManager;

void
GTM_InitFidManager(void)
{
	int ii;
	MemoryContext oldContext;
	
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTMFidManager.Freelist = gtm_NIL;
	for (ii = 0; ii < GTM_MAX_FRAGMENTS; ii++)
	{
		GTMFidManager.Fids[ii].nodeId = INVALID_NODE_ID;
		GTMFidManager.Fids[ii].fidnum = ii;
		GTMFidManager.Freelist = gtm_lappend(GTMFidManager.Freelist, &GTMFidManager.Fids[ii]);
	}
	for (ii = 0; ii < MAX_NODES; ii++)
	{
		GTMFidManager.FidNodes[ii].fids = NULL;
		GTMFidManager.FidNodes[ii].node_start_ts = 0;
		GTMFidManager.FidNodes[ii].time = GTM_TimestampGetMonotonicRaw();
	}
	GTM_MutexLockInit(&GTMFidManager.FidLock);
	MemoryContextSwitchTo(oldContext);
}

/* forcibly release the fid which acquired by the abnormal exit CN */
void static ForceReleaseFidByNode(int nodeId)
{
	int i, ret;

	XLogBeginInsert();

	for (i = 0; i < GTM_MAX_FRAGMENTS; i++)
	{
		if (GTMFidManager.FidNodes[nodeId].fids[i] != 0)
		{
			GTM_Fid *fid = &GTMFidManager.Fids[i];
			fid->nodeId = INVALID_NODE_ID;
			elog(LOG, "Fid %d on %lld timeout release from node %d.",
				fid->fidnum, (long long int)GTMFidManager.FidNodes[nodeId].time, nodeId);
			GTMFidManager.FidNodes[nodeId].fids[i] = 0;
			GTMFidManager.Freelist = gtm_lappend(GTMFidManager.Freelist, fid);
			ret = GTM_StoreSyncFid(fid->fidnum, INVALID_NODE_ID);
			if (ret < 0)
			{
				elog(LOG, "Release sync fid %d error for %s", fid->fidnum, strerror(errno));
			}
		}
	}

	BeforeReplyToClientXLogTrigger();	
}

void
ProcessFidAcquireCommand(Port *myport, StringInfo message)
{
	int count = 0;
	int i;
	int ret;
	StringInfoData buf;
	gtm_ListCell *cell;
	MemoryContext oldContext;
	int nodeId = pq_getmsgint(message, sizeof(int));
	int size = pq_getmsgint(message, sizeof(int));
	int node_start_ts = pq_getmsgint64(message);
	int fidarray[size];
	int maxlen = STR_FIDS_LEN;
	int len;
	char fidbuffer[STR_FIDS_LEN] = {0};
	char *strfid = fidbuffer;

	pq_getmsgend(message);
	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM_CTL &&
			myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't provide fid.");
		}
	}

	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);
	if (GTMFidManager.FidNodes[nodeId].node_start_ts > 0 &&
		GTMFidManager.FidNodes[nodeId].node_start_ts != node_start_ts &&
		GTMFidManager.FidNodes[nodeId].fids != NULL)
	{
		ForceReleaseFidByNode(nodeId);
	}
	GTMFidManager.FidNodes[nodeId].node_start_ts = node_start_ts;
	if (gtm_list_length(GTMFidManager.Freelist) < size)
	{
		GTM_MutexLockRelease(&GTMFidManager.FidLock);
		MemoryContextSwitchTo(oldContext);
		pq_beginmessage(&buf, 'S');
		pq_sendint(&buf, MSG_ACQUIRE_FID_RESULT, 4);
		pq_sendint(&buf, 0, sizeof(size));
		pq_endmessage(myport, &buf);
		pq_flush(myport);
		return;
	}

	if (NULL == GTMFidManager.FidNodes[nodeId].fids)
	{
		GTMFidManager.FidNodes[nodeId].fids = palloc0(sizeof(int8) * GTM_MAX_FRAGMENTS);
	}
	
	while (count < size)
	{
		GTM_Fid *fid;
		cell = gtm_list_head(GTMFidManager.Freelist);
		fid = gtm_lfirst(cell);
		fid->nodeId = nodeId;
		fidarray[count] = fid->fidnum;
		len = snprintf(strfid, maxlen, "%d ", fid->fidnum);
		strfid += len;
		maxlen -= len;
		GTMFidManager.Freelist = gtm_list_delete_first(GTMFidManager.Freelist);

		GTMFidManager.FidNodes[nodeId].fids[fid->fidnum] = 1;
		GTMFidManager.FidNodes[nodeId].time = GTM_TimestampGetMonotonicRaw();
		ret = GTM_StoreSyncFid(fid->fidnum, nodeId);
		if (ret < 0)
		{
			elog(LOG, "Acquire sync fid %d error for %s",
				fid->fidnum, strerror(errno));
		}
		count++;
	}

	GTM_MutexLockRelease(&GTMFidManager.FidLock);

	elog(LOG, "Node %d acquire fid %s count %d.", nodeId, fidbuffer, count);

    BeforeReplyToClientXLogTrigger();
	MemoryContextSwitchTo(oldContext);

	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_ACQUIRE_FID_RESULT, 4);
	pq_sendint(&buf, size, sizeof(size));
	for (i = 0 ; i < size ; i++)
	{	
		pq_sendint(&buf, fidarray[i], sizeof(int));
	}
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}

void ProcessFidReleaseCommand(Port *myport, StringInfo message)
{
	int i;
	int ret;
	StringInfoData buf;	
	MemoryContext oldContext;
	int nodeId = pq_getmsgint(message, sizeof (int));
	int size = pq_getmsgint(message, sizeof (int));
	int fidarray[size];
	int maxlen = STR_FIDS_LEN;
	int len;
	char fidbuffer[STR_FIDS_LEN] = {0};
	char *strfid = fidbuffer;

	for (i = 0; i < size; i++)
	{
		fidarray[i] = pq_getmsgint(message, sizeof(int));
	}

	pq_getmsgend(message);
	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM_CTL && myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't release fid.");
		}
	}
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);
	if (NULL == GTMFidManager.FidNodes[nodeId].fids)
	{
		GTMFidManager.FidNodes[nodeId].fids = palloc0(sizeof(int8) * GTM_MAX_FRAGMENTS);
	}

	for (i = 0; i < size; i++)
	{
		if (fidarray[i] < 0 || fidarray[i] >= GTM_MAX_FRAGMENTS)
		{
			continue;
		}
		if (GTMFidManager.Fids[fidarray[i]].nodeId != nodeId)
		{
			elog(LOG, "Node %d release fid %d fail because it belongs to %d", 
				nodeId, fidarray[i], GTMFidManager.Fids[fidarray[i]].nodeId);
			continue;
		}
		GTMFidManager.Fids[fidarray[i]].nodeId = INVALID_NODE_ID;
		len = snprintf(strfid, maxlen, "%d ", fidarray[i]);
		strfid += len;
		maxlen -= len;
		GTMFidManager.FidNodes[nodeId].fids[fidarray[i]] = 0;
		GTMFidManager.Freelist = gtm_lappend(GTMFidManager.Freelist, &GTMFidManager.Fids[fidarray[i]]);
		ret = GTM_StoreSyncFid(fidarray[i], INVALID_NODE_ID);
		if (ret < 0)
		{
			elog(LOG, "Release sync fid %d error for %s", fidarray[i], strerror(errno));
		}
	}
	GTM_MutexLockRelease(&GTMFidManager.FidLock);

	elog(LOG, "Node %d release fid %s.", nodeId, fidbuffer);

    BeforeReplyToClientXLogTrigger();
	MemoryContextSwitchTo(oldContext);
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_RELEASE_FID_RESULT, 4);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}

void ProcessFidReleaseNodeCommand(Port *myport, StringInfo message)
{
	int ret;
	StringInfoData buf;	
	MemoryContext oldContext;
	int nodeId = pq_getmsgint(message, sizeof (int));

	pq_getmsgend(message);
	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM_CTL && myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't release fid.");
		}
	}
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);
	elog(LOG, "Node %d release all fid.", nodeId);

	if (NULL != GTMFidManager.FidNodes[nodeId].fids)
	{
		int i;
		for (i = 0; i < GTM_MAX_FRAGMENTS; i++)
		{
			GTM_Fid *fid;

			if (0 == GTMFidManager.FidNodes[nodeId].fids[i])
			{
				continue;
			}
			
			fid = &GTMFidManager.Fids[i];
			fid->nodeId = INVALID_NODE_ID;
			elog(LOG, "Node %d release fid %d.", nodeId, fid->fidnum);
			GTMFidManager.FidNodes[nodeId].fids[i] = 0;
			GTMFidManager.Freelist = gtm_lappend(GTMFidManager.Freelist, fid);
			ret = GTM_StoreSyncFid(fid->fidnum, INVALID_NODE_ID);
			if (ret < 0)
			{
				elog(LOG, "Release sync fid %d error for %s",fid->fidnum, strerror(errno));
			}
		}
	}

	GTM_MutexLockRelease(&GTMFidManager.FidLock);
    BeforeReplyToClientXLogTrigger();

	MemoryContextSwitchTo(oldContext);
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_RELEASE_FID_RESULT, 4);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}


void ProcessFidKeepAliveCommand(Port *myport, StringInfo message)
{
	StringInfoData buf;	
	MemoryContext oldContext;
	int nodeId = pq_getmsgint(message, sizeof (int));
	time_t node_start_ts = pq_getmsgint64(message);

	pq_getmsgend(message);
	if (Recovery_IsStandby())
	{
		if (myport->remote_type != GTM_NODE_GTM_CTL && myport->remote_type != GTM_NODE_GTM)
		{
			elog(ERROR, "gtm standby can't keep alive fid.");
		}
	}
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);
	if (GTMFidManager.FidNodes[nodeId].node_start_ts > 0 &&
		GTMFidManager.FidNodes[nodeId].node_start_ts != node_start_ts &&
		GTMFidManager.FidNodes[nodeId].fids != NULL)
	{
		ForceReleaseFidByNode(nodeId);
	}
	GTMFidManager.FidNodes[nodeId].node_start_ts = node_start_ts;
	GTMFidManager.FidNodes[nodeId].time = GTM_TimestampGetMonotonicRaw();
	GTM_MutexLockRelease(&GTMFidManager.FidLock);
	elog(LOG, "keep alive fid for node %d.", nodeId);
	MemoryContextSwitchTo(oldContext);
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_KEEPALIVE_FID_RESULT, 4);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}


void ProcessFidListCommand(Port *myport, StringInfo message)
{
	int nodeId = INVALID_NODE_ID;
	int size = 0;
	int i;
	StringInfoData buf;
	MemoryContext oldContext;
	int fidarray[GTM_MAX_FRAGMENTS];
	nodeId = pq_getmsgint(message, sizeof (int));
	pq_getmsgend(message);
	
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);
	elog(LOG, "Node %d list fid.", nodeId);

	if (NULL != GTMFidManager.FidNodes[nodeId].fids)
	{
		int i;
		for (i = 0; i < GTM_MAX_FRAGMENTS; i++)
		{
			if (0 != GTMFidManager.FidNodes[nodeId].fids[i])
			{
				fidarray[size] = i;
				size++;
			}
		}
	}

	GTM_MutexLockRelease(&GTMFidManager.FidLock);
	MemoryContextSwitchTo(oldContext);
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_LIST_FID_RESULT, 4);
	pq_sendint(&buf, size, sizeof(size));
	for (i = 0 ; i < size ; i++)
	{		
		pq_sendint(&buf, fidarray[i], sizeof(int));
	}
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}

void ProcessFidListAliveCommand(Port * myport, StringInfo message)
{
	int size = 0;
	int i;
	StringInfoData buf;
	MemoryContext oldContext;
	GTM_Fid fidarray[GTM_MAX_FRAGMENTS];
	pq_getmsgend(message);
	
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);
	for (i = 0; i < GTM_MAX_FRAGMENTS; i++)
	{
		GTM_Fid fid = GTMFidManager.Fids[i];
		if (fid.nodeId != INVALID_NODE_ID)
		{
			fidarray[size] = fid;
			size++;
		}
	}
	
	GTM_MutexLockRelease(&GTMFidManager.FidLock);
	MemoryContextSwitchTo(oldContext);
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_LIST_ALIVE_FID_RESULT, 4);
	pq_sendint(&buf, size, sizeof(size));
	for (i = 0 ; i < size ; i++)
	{		
		pq_sendbytes(&buf, (char*)&fidarray[i], sizeof(GTM_Fid));
	}
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}

void ProcessFidListAllCommand(Port * myport, StringInfo message)
{
	int i;
	StringInfoData buf;
	MemoryContext oldContext;
	pq_getmsgend(message);
	oldContext = MemoryContextSwitchTo(TopMostMemoryContext);
	GTM_MutexLockAcquire(&GTMFidManager.FidLock);	
	pq_beginmessage(&buf, 'S');
	pq_sendint(&buf, MSG_LIST_ALL_FID_RESULT, 4);
	pq_sendint(&buf, GTM_MAX_FRAGMENTS, sizeof(GTM_MAX_FRAGMENTS));
	for (i = 0 ; i < GTM_MAX_FRAGMENTS ; i++)
	{		
		pq_sendbytes(&buf, (char*)&(GTMFidManager.Fids[i]), sizeof(GTM_Fid));
	}
	GTM_MutexLockRelease(&GTMFidManager.FidLock);
	MemoryContextSwitchTo(oldContext);
	pq_endmessage(myport, &buf);
	pq_flush(myport);
	return;
}


