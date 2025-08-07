/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *		Postgres-XC flags and connection control information
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_H
#define PGXC_H

#include "postgres.h"
#include "port/atomics.h"

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;
extern bool isRestoreMode;
extern char *parentPGXCNode;
extern int parentPGXCPid;

typedef enum
{
	REMOTE_CONN_APP,
	REMOTE_CONN_COORD,
	REMOTE_CONN_DATANODE,
#ifdef __OPENTENBASE_C__
	REMOTE_CONN_FORWARDNODE,
#endif
	REMOTE_CONN_GTM,
	REMOTE_CONN_GTM_PROXY
} RemoteConnTypes;

#ifdef __OPENTENBASE_C__
typedef enum
{
	OpenTenBase_Cluster = 0,
	OpenTenBase_Cluster_01,
	OpenTenBase_Cluster_02,
	OpenTenBase_Cluster_03,
	OpenTenBase_Cluster_04,
	OpenTenBase_Cluster_05,
	OpenTenBase_Cluster_06,
	OpenTenBase_Cluster_07,
	OpenTenBase_Cluster_08,
	OpenTenBase_Cluster_09,
	OpenTenBase_Cluster_10,
	OpenTenBase_Cluster_11,
	OpenTenBase_Cluster_12,
	OpenTenBase_Cluster_13,
	OpenTenBase_Cluster_14,
	OpenTenBase_Cluster_15
} PgxcPlaneNameId;

extern int PGXCPlaneNameID;
extern int PGXCMainPlaneNameID;
extern int PGXCDefaultPlaneNameID;
extern char * pgxc_plane_name(PgxcPlaneNameId name_id);
#endif

/* Determine remote connection type for a PGXC backend */
extern int		remoteConnType;

/* Local node name and numer */
extern char	*PGXCNodeName;
extern int	PGXCNodeId;
extern Oid	PGXCNodeOid;
extern bool IsPGXCMainPlane;
extern uint32	PGXCNodeIdentifier;
extern bool PGXCNodeTypeCoordinator;

extern char GlobalTraceIdHistory[];
extern char GlobalTraceId[];
extern char *GlobalTraceIdHistoryPointer;
extern char LocalTraceId[];
extern TransactionId GlobalTraceVxid;

/* Is centralized mode? */
extern bool is_centralized_mode;

extern char		PGXCQueryId[NAMEDATALEN];
extern uint64	PGXCDigitalQueryId;
extern volatile pg_atomic_uint64 *query_id;

extern Datum xc_lockForBackupKey1;
extern Datum xc_lockForBackupKey2;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode

#define IS_PGXC_LOCAL_COORDINATOR	\
	(IS_PGXC_COORDINATOR && !IsConnFromCoord())
#define IS_PGXC_REMOTE_COORDINATOR	\
	(IS_PGXC_COORDINATOR && IsConnFromCoord())
#define IS_PGXC_MAINCLUSTER_SLAVENODE \
    (IsPGXCMainPlane && RecoveryInProgress())

#define IsConnFromApp() (remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord() (remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode() (remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromGtm() (remoteConnType == REMOTE_CONN_GTM)
#define IsConnFromGtmProxy() (remoteConnType == REMOTE_CONN_GTM_PROXY)

#define IsConnFromProxy() (am_conn_from_proxy)

#define IS_CENTRALIZED_MODE (is_centralized_mode)

#define IS_DISTRIBUTED_DATANODE (IS_PGXC_DATANODE && !IS_CENTRALIZED_MODE)
#define IS_CENTRALIZED_DATANODE (IS_PGXC_DATANODE && IS_CENTRALIZED_MODE)

#define IS_ACCESS_NODE (IS_PGXC_COORDINATOR || IS_CENTRALIZED_DATANODE)
#define IS_LOCAL_ACCESS_NODE (IS_PGXC_LOCAL_COORDINATOR || IS_CENTRALIZED_DATANODE)

#define IS_STMT_LEVEL_ROLLBACK \
	((IS_PGXC_LOCAL_COORDINATOR || IS_CENTRALIZED_DATANODE) && stmt_level_rollback)

#define IS_PLSQL_STMT_LEVEL_ROLLBACK \
	(IS_STMT_LEVEL_ROLLBACK && plsql_stmt_level_rollback)

/* key pair to be used as object id while using advisory lock for backup */
#define XC_LOCK_FOR_BACKUP_KEY_1      0xFFFF
#define XC_LOCK_FOR_BACKUP_KEY_2      0xFFFF

#endif   /* PGXC */
