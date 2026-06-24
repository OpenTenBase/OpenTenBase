/*-------------------------------------------------------------------------
 *
 * groupmgr.h
 *	  Routines for PGXC node group management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/groupmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GROUPMGR_H
#define GROUPMGR_H

#include "nodes/parsenodes.h"
#include "pgxc/nodemgr.h"
#define OPENTENBASE_MAX_NODEGROUP_NUMBER 16

/* Node Group definition */
typedef struct
{
	Oid 		group_oid;
	NameData	group_name;
	int			numidx;								   /* node_idx array real size */
	int 		node_idx[OPENTENBASE_MAX_DATANODE_NUMBER];   /* node index info in dnDefs */
} NodeGroupDefinition;

typedef struct 
{
	bool    needLock;						/* whether we need lock */
	bool	inited;							/* whether finished initialization */
	int	    shmemNumNodeGroups;			    /* NodeGroupDefs array real size */
	NodeGroupDefinition NodeGroupDefs[OPENTENBASE_MAX_NODEGROUP_NUMBER];
}NodeGroupInfo;

extern NodeGroupInfo *g_NodeGroupMgr;
extern Size NodeGroupShmemSize(void);
extern void NodeGroupShmemInit(void);
extern void SortPgxcNodeGroup(Relation	rel, bool is_force);
extern void PgxcGroupCreate(CreateGroupStmt *stmt);
extern void PgxcGroupRemove(DropGroupStmt *stmt);
extern void PgxcGroupAlter(AlterGroupStmt *stmt);
#ifdef _MIGRATE_
extern Oid  GetDefaultGroup(void);
extern void AddNodeToGroup(Oid nodeoid, Oid groupoid);
extern Oid RemoveNodeFromGroup(Oid nodeoid);
extern Oid GetGroupOidByNode(Oid nodeoid);
extern List *GetGroupNodeList(Oid group);
extern char *GetMyGroupName(char *groupname);
extern char *GetGroupNameByNode(Oid nodeoid);
extern int GetGroupNodeListByIndex(int idx, int *nodes);
#endif

#endif   /* GROUPMGR_H */
