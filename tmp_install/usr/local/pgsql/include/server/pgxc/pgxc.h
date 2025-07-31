/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *        Postgres-XC flags and connection control information
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/pgxc/pgxc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_H
#define PGXC_H

#include "postgres.h"
#include "nodes/pg_list.h"

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;
extern bool isRestoreMode;
extern char *parentPGXCNode;
extern int parentPGXCPid;
extern int    parentPGXCNodeId;
extern char    parentPGXCNodeType;

typedef enum
{
    REMOTE_CONN_APP,
    REMOTE_CONN_COORD,
    REMOTE_CONN_DATANODE,
    REMOTE_CONN_GTM,
    REMOTE_CONN_GTM_PROXY
} RemoteConnTypes;

/* Determine remote connection type for a PGXC backend */
extern int		remoteConnType;

/* Local node name and numer */
extern char    *PGXCNodeName;
extern int    PGXCNodeId;
extern bool IsPGXCMainCluster;
extern uint32    PGXCNodeIdentifier;
extern char *PGXCClusterName;
extern char *PGXCMainClusterName;
extern char *PGXCDefaultClusterName;
#ifdef __OPENTENBASE__
extern char PGXCSessionId[NAMEDATALEN];
extern int PGXCLevelId;
extern List *PGXCGroupNodeList;
#endif


extern Datum xc_lockForBackupKey1;
extern Datum xc_lockForBackupKey2;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode

#define IS_PGXC_LOCAL_COORDINATOR    \
    (IS_PGXC_COORDINATOR && !IsConnFromCoord())
#define IS_PGXC_REMOTE_COORDINATOR    \
    (IS_PGXC_COORDINATOR && IsConnFromCoord())
#define IS_PGXC_MAINCLUSTER_SLAVENODE \
    (IsPGXCMainCluster && RecoveryInProgress())

#define PGXC_PARENT_NODE parentPGXCNode
#define PGXC_PARENT_NODE_ID    parentPGXCNodeId
#define PGXC_PARENT_NODE_TYPE    parentPGXCNodeType
#define REMOTE_CONN_TYPE remoteConnType

#define IsConnFromApp() (remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord() (remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode() (remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromGtm() (remoteConnType == REMOTE_CONN_GTM)
#define IsConnFromGtmProxy() (remoteConnType == REMOTE_CONN_GTM_PROXY)

/* key pair to be used as object id while using advisory lock for backup */
#define XC_LOCK_FOR_BACKUP_KEY_1      0xFFFF
#define XC_LOCK_FOR_BACKUP_KEY_2      0xFFFF

#endif   /* PGXC */
