/*-------------------------------------------------------------------------
 *
 * resqueue.h
 *	  Commands for manipulating resource queues.
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	    src/include/commands/resqueue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __OPENTENBASE_RESQUEUE_H__
#define __OPENTENBASE_RESQUEUE_H__

#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "pgxc/pgxcnode.h"

typedef struct ResQueueInfo
{
	NameData						resq_name;
	NameData						resq_group;
	int64							resq_memory_limit;
	int64							resq_network_limit;
	int32							resq_active_stmts;
	int16							resq_wait_overload;
	int16							resq_priority;
} ResQueueInfo;

typedef enum PriorityVal
{
	PriorityVal_Invalid	= 0,
	PriorityVal_Max 	= 1,
	PriorityVal_High	= 2,
	PriorityVal_Medium	= 3,
	PriorityVal_Low		= 4,
	PriorityVal_Min		= 5,

	/* End of list marker */
	PriorityVal_Butt,
} PriorityVal;

extern bool enable_resource_queue;
extern bool enable_resource_queue_debug;

extern char * ResQPriorityGetSetting(int16 pval);

extern void CreateResourceQueue(CreateResourceQueueStmt *stmt);
extern void AlterResourceQueue(AlterResourceQueueStmt *stmt);
extern void DropResourceQueue(DropResourceQueueStmt *stmt);
extern bool IsResQueueEnabled(void);
extern bool IsResQueueExists(char *resqueue);

extern int64 EstimateQueryMemoryBytes(PlannedStmt *stmt);
extern void GetUserResQueueName(Oid roleid, NameData * resq);
extern void AcquireResourceFromResQueue(int64 acquiredMemBytes);
extern void ReleaseResourceBackToResQueue(void);

extern int64 EstimateDProcessNetworkBytes(int16 dnDprocessNumber);
extern void SendResQueueInfoToDProcess(PGXCNodeHandle *handle,
										int64 network_bytes_limit);

extern void CleanLocalResQueueInfo(void);

extern void CNAssignLocalResQueueInfo(void);
extern void DNReceiveLocalResQueueInfo(StringInfo input_message);

extern Size ResQUsageShmemSize(void);
extern void ResQUsageShmemInit(void);

extern void ResQUsageSetTotalNodesOnSameHost(int64 node_number);
extern int64 ResQUsageGetTotalNodesOnSameHost(void);

extern void ResQUsageSetFnSendBuffBytes(int64 fn_send_buff_bytes);
extern int64 ResQUsageGetFnSendBuffBytes(void);

extern void ResQUsageAddPriorityVal(int16 priorityVal);
extern void ResQUsageSubPriorityVal(int16 priorityVal);
extern void ResQUsageSetTotalPriorities(int64 totalPriorities);
extern int64 ResQUsageGetTotalPriorities(void);

extern void ResQUsageAddNetworkBytesLimit(int64 network_bytes);
extern void ResQUsageSubNetworkBytesLimit(int64 network_bytes);
extern void ResQUsageSetTotalNetworkBytesLimit(int64 totalNetworkBytes);
extern int64 ResQUsageGetTotalNetworkBytesLimit(void);

extern void ResQUsageAddLocalResourceInfo(void);
extern void ResQUsageRemoveLocalResourceInfo(void);

extern void ResQUsageBackoffPriority(void);
extern void ResQUsageBackoffNetwork(int64 fn_alloc_bytes, int64 fn_released_bytes);

#endif   /* __OPENTENBASE_RESQUEUE_H__ */
