/*-------------------------------------------------------------------------
 *
 * nodemgr.h
 *  Routines for node management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/nodemgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEMGR_H
#define NODEMGR_H

#include "storage/s_lock.h"
#include "utils/hsearch.h"
#include "nodes/parsenodes.h"

#define 	PGXC_NODENAME_LENGTH				64
#define		OPENTENBASE_MAX_COORDINATOR_NUMBER_LOG2	11
#define		OPENTENBASE_MAX_COORDINATOR_NUMBER 		(1 << OPENTENBASE_MAX_COORDINATOR_NUMBER_LOG2)
#define		OPENTENBASE_MAX_DATANODE_NUMBER    		2048

/* Global number of nodes */
extern int 	NumDataNodes;
extern int 	NumCoords;
#ifdef __OPENTENBASE__
extern char *PGXCNodeHost;
#endif

/* Node definition */
typedef struct
{
	Oid 		nodeoid;
	NameData	nodename;
	NameData	nodehost;
	int			nodeport;
#ifdef __OPENTENBASE_C__
	int			nodeforwardport;
#endif
	bool		nodeisprimary;
	bool 		nodeispreferred;
	bool		nodeishealthy;
#ifdef __OPENTENBASE_C__
	int32		nodeplaneid;
#endif
} NodeDefinition;

typedef struct
{
	int				port;
	NameData	    host;
	bool			valid;
	slock_t			mutex;
} SHMGTMPrimaryInfo;

extern SHMGTMPrimaryInfo *shm_gtm_primary;

typedef struct
{
	int				port;
	NameData	    host;
	int				status;
} GTMPrimaryInfo;

typedef enum GTMPrimaryInfoStatus
{
	GTM_INFO_INVAID,
	GTM_INFO_SET,
	GTM_INFO_RESET
} GTMPrimaryInfoStatus;

extern GTMPrimaryInfo	new_gtm_primary;

extern void NodeTablesShmemInit(void);
#ifdef __OPENTENBASE__
extern void NodeDefHashTabShmemInit(void);
extern Size NodeHashTableShmemSize(void);
#endif
extern Size NodeTablesShmemSize(void);

extern void PgxcNodeListAndCountWrapTransaction(bool is_force, bool pool_reload);
extern void PgxcNodeGetOids(Oid **coOids, Oid **dnOids, int *num_coords, int *num_dns,
							bool update_preferred);
extern void PgxcNodeGetHealthMap(Oid *coOids, Oid *dnOids, int *num_coords, int *num_dns,
								 bool *coHealthMap, bool *dnHealthMap);
extern NodeDefinition *PgxcNodeGetDefinition(Oid node);
extern int PGXCNodeGetNodeIdByNodeOID(Oid node);
extern int PGXCNodeGetNodeIdByMasterNodeOID(Oid node);
extern void PgxcNodeAlter(AlterNodeStmt *stmt);
extern void PgxcNodeCreate(CreateNodeStmt *stmt);
extern void PgxcNodeRemove(DropNodeStmt *stmt);
extern void PgxcNodeDnListHealth(List *nodeList, bool *dnhealth);
extern bool PgxcNodeUpdateHealth(Oid node, bool status);

#ifdef __LICENSE__
extern void PgxcNodeGetNum(int *num_of_dn, int *num_of_cn);
#endif
extern bool PrimaryNodeNumberChanged(void);
#ifdef __OPENTENBASE_C__
extern uint32 get_node_id(const char* node_name, bool check_syscache);
#endif
extern void PgxcNodeGetAllDefinitions(NodeDefinition **cn_defs, int *cn_count,
                                      NodeDefinition **dn_defs, int *dn_count,
									  NodeDefinition **self_defs, int *index);
/* GUC parameter */
extern bool enable_multi_plane;
extern bool enable_multi_plane_print;
/* for resource group */
extern List *PgxcNodeGetAllDataNodeNames(void);
extern List *PgxcNodeGetDataNodeNames(List* nodeList);
extern size_t SHMGTMPrimaryInfoSize(void);
extern void SHMGTMPrimaryInfoInit(void);
extern void AtEOXact_SHMGTMPrimaryInfo(bool isCommit);
#endif	/* NODEMGR_H */
