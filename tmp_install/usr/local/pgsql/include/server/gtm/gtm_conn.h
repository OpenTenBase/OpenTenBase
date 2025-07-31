/*-------------------------------------------------------------------------
 *
 * gtm_conn.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_CONN_H
#define GTM_CONN_H

#include "gtm/libpq-be.h"
#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"

struct GTM_ThreadInfo;

typedef struct GTM_ConnectionInfo
{
    /* Port contains all the vital information about this connection */
    Port                    *con_port;
    struct GTM_ThreadInfo    *con_thrinfo;
    bool                    con_authenticated;
    bool                    con_init;
    uint32                    con_client_id;
    uint32                    con_idx;

#ifndef __XLOG__
    /* a connection object to the standby */
    GTM_Conn                *standby;
#endif
} GTM_ConnectionInfo;

typedef struct GTM_Connections
{
    uint32                gc_conn_count;
    uint32                gc_array_size;
    GTM_ConnectionInfo    *gc_connections;
    GTM_RWLock            gc_lock;
} GTM_Connections;


#endif
