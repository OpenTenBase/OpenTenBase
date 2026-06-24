/*-------------------------------------------------------------------------
 *
 * gtm_fid.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _GTM_FID_H
#define _GTM_FID_H

#include "gtm/libpq-be.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_lock.h"
#include "gtm/gtm_list.h"
#include "gtm/stringinfo.h"
#include "port/atomics.h"
#include "gtm/register.h"

typedef struct GTM_Fid
{
	int nodeId;			/* INVALID_NODE_ID means null, otherwise, nodeid */
	int fidnum;			/* fid number */
}GTM_Fid;

typedef struct GTM_FidNode
{
	GlobalTimestamp time;
	time_t			node_start_ts;
	int8			*fids;
}GTM_FidNode;

typedef struct GTM_FidManager
{
	GTM_Fid			Fids[GTM_MAX_FRAGMENTS];			
	gtm_List		*Freelist;
	GTM_FidNode		FidNodes[MAX_NODES]; 			/* contains every node fid list head */
	GTM_MutexLock	FidLock;
}GTM_FidManager;

extern GTM_FidManager GTMFidManager;

void GTM_InitFidManager(void);
void ProcessFidAcquireCommand(Port *myport, StringInfo message);
void ProcessFidReleaseCommand(Port *myport, StringInfo message);
void ProcessFidReleaseNodeCommand(Port *myport, StringInfo message);
void ProcessFidKeepAliveCommand(Port *myport, StringInfo message);
void ProcessFidListCommand(Port *myport, StringInfo message);
void ProcessFidListAliveCommand(Port *myport, StringInfo message);
void ProcessFidListAllCommand(Port *myport, StringInfo message);


#endif
