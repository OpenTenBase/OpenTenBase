/*-------------------------------------------------------------------------
 *
 * gtm_resqueue.h
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_RESQUEUE_H
#define GTM_RESQUEUE_H

#include "gtm/stringinfo.h"
#include "gtm/gtm_lock.h"
#include "gtm/libpq-be.h"

typedef struct GTM_ResQueueInfo
{
	NameData						resq_name;
	NameData						resq_group;
	int64							resq_memory_limit;
	int64							resq_network_limit;
	int32							resq_active_stmts;
	int16							resq_wait_overload;
	int16							resq_priority;

	int32							resq_ref_count;
	int32							resq_state;
	GTM_RWLock						resq_lock;
	GTMStorageHandle 				resq_store_handle;

	int64							resq_used_memory;
	gtm_List						*resq_users;
	gtm_List						*resq_waiters;
} GTM_ResQueueInfo;

typedef struct GTM_ResQAlterInfo
{
	char 							alter_name;
	char 							alter_group;
	char 							alter_memory_limit;
	char 							alter_network_limit;
	char 							alter_active_stmts;
	char 							alter_wait_overload;
	char 							alter_priority;
} GTM_ResQAlterInfo;

/*
 * Resource Queue request data from client
 * 
 * MSG_RESQUEUE_ACQUIRE/MSG_RESQUEUE_RELEASE
 */
typedef struct GTM_ResQRequestData
{
	NameData 						nodeName;
	int32							nodeProcPid;
	int32							nodeBackendId;
	NameData						resqName;
	int64							memoryBytes;
} GTM_ResQRequestData;

typedef struct GTM_ResQWaiter
{
	Port 							*myport;
	GTM_ResQRequestData				request;
} GTM_ResQWaiter;

typedef GTM_ResQWaiter GTM_ResQUser;

typedef struct GTM_ResQUsageInfo
{
	GTM_ResQRequestData				usage;
	bool							isuser;
} GTM_ResQUsageInfo;

#define RESQUEUE_STATE_ACTIVE	1
#define RESQUEUE_STATE_DELETED	2

#define RESQUEUE_MAX_REFCOUNT	1024

extern void GTM_InitResourceQueueManager(void);

extern int GTM_ResourceQueueOpenDefault(void);
extern int GTM_ResourceQueueOpen(GTM_ResQueueInfo * resq);
extern int GTM_ResourceQueueAlter(GTM_ResQueueInfo * resq, GTM_ResQAlterInfo * resq_alter);
extern int GTM_ResourceQueueClose(GTM_ResQueueKey resqkey);
extern int GTM_ResourceQueueIfExists(GTM_ResQueueKey resqkey);
extern int GTM_ResourceQueueAcqurie(Port *myport, GTM_ResQRequestData * request);
extern int GTM_ResourceQueueRelease(Port *myport, GTM_ResQRequestData * request);
extern int GTM_ResourceQueueClean(Port *myport);

extern void ProcessResourceQueueInitCommand(Port *myport, StringInfo message, bool is_backup);
extern void ProcessResourceQueueAlterCommand(Port *myport, StringInfo message, bool is_backup);
extern void ProcessResourceQueueListCommand(Port *myport, StringInfo message);
extern void ProcessResourceQueueCloseCommand(Port *myport, StringInfo message, bool is_backup);
extern void ProcessResourceQueueMoveConnCommand(Port *myport, StringInfo message);
extern void ProcessResourceQueueAcquireCommand(Port *myport, StringInfo message);
extern void ProcessResourceQueueReleaseCommand(Port *myport, StringInfo message);
extern void ProcessResourceQueueCheckIfExistsCommand(Port *myport, StringInfo message);
extern void ProcessResourceQueueListUsageCommand(Port *myport, StringInfo message);

extern GTM_ResQueueInfo * GTM_FormResQueueOfStore(GTM_ResQueueKey resqkey);

#endif
