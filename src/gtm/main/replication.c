/*-------------------------------------------------------------------------
 *
 * replication.c
 *  Controlling the initialization and end of replication process of GTM data
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *      src/gtm/recovery/replication.c
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/replication.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "gtm/gtm_c.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/standby_utils.h"
#include "gtm/gtm_standby.h"
#include "gtm/register.h"
#include "gtm/assert.h"
#include <stdio.h>
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"
#include "gtm/gtm_ip.h"

/*
 * Process MSG_NODE_BEGIN_REPLICATION_INIT
 */
void
ProcessBeginReplicaInitSyncRequest(Port *myport, StringInfo message)
{
    StringInfoData buf;
    MemoryContext oldContext;

    pq_getmsgend(message);

    if (Recovery_IsStandby())
        ereport(ERROR,
            (EPERM,
             errmsg("Operation not permitted under the standby mode.")));

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /* Acquire global locks to copy resource data to the standby. */
    GTM_RWLockAcquire(&GTMTransactions.gt_XidGenLock, GTM_LOCKMODE_WRITE);
    GTM_RWLockAcquire(&GTMTransactions.gt_TransArrayLock, GTM_LOCKMODE_WRITE);
    elog(LOG, "Prepared for copying data with holding XidGenLock and TransArrayLock.");

    MemoryContextSwitchTo(oldContext);

    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, NODE_BEGIN_REPLICATION_INIT_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_endmessage(myport, &buf);

    /*
     * Beause this command comes from the standby, we don't have to flush
     * messages to the standby here.
     */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);

    elog(LOG, "ProcessBeginReplicationInitialSyncRequest() ok.");

    return;
}

/*
 * Process MSG_NODE_END_REPLICATION_INIT
 */
void
ProcessEndReplicaInitSyncRequest(Port *myport, StringInfo message)
{
    StringInfoData buf;
    MemoryContext oldContext;

    pq_getmsgend(message);

    if (Recovery_IsStandby())
        ereport(ERROR,
            (EPERM,
             errmsg("Operation not permitted under the standby mode.")));

    oldContext = MemoryContextSwitchTo(TopMemoryContext);

    /*
     * Release global locks after copying resource data to the standby.
     */
    GTM_RWLockRelease(&GTMTransactions.gt_TransArrayLock);
    GTM_RWLockRelease(&GTMTransactions.gt_XidGenLock);
    elog(LOG, "XidGenLock and TransArrayLock released.");

    MemoryContextSwitchTo(oldContext);

    BeforeReplyToClientXLogTrigger();
    
    pq_beginmessage(&buf, 'S');
    pq_sendint(&buf, NODE_END_REPLICATION_INIT_RESULT, 4);
    if (myport->remote_type == GTM_NODE_GTM_PROXY)
    {
        GTM_ProxyMsgHeader proxyhdr;
        proxyhdr.ph_conid = myport->conn_id;
        pq_sendbytes(&buf, (char *)&proxyhdr, sizeof (GTM_ProxyMsgHeader));
    }
    pq_endmessage(myport, &buf);

    /*
     * Beause this command comes from the standby, we don't have to flush
     * messages to the standby here.
     */
    if (myport->remote_type != GTM_NODE_GTM_PROXY)
        pq_flush(myport);

    elog(LOG, "ProcessEndReplicationInitialSyncRequest() ok.");

    return;
}
