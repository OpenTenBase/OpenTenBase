/*-------------------------------------------------------------------------
 *
 * groupmgr.h
 *      Routines for PGXC node group management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/pgxc/groupmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GROUPMGR_H
#define GROUPMGR_H

#include "nodes/parsenodes.h"

extern void PgxcGroupCreate(CreateGroupStmt *stmt);
extern void PgxcGroupRemove(DropGroupStmt *stmt);
extern void PgxcGroupAlter(AlterGroupStmt *stmt);
#ifdef _MIGRATE_
extern Oid  GetDefaultGroup(void);
extern void AddNodeToGroup(Oid nodeoid, Oid groupoid);
extern Oid RemoveNodeFromGroup(Oid nodeoid);
extern Oid GetGroupOidByNode(Oid nodeoid);
extern List *GetGroupNodeList(Oid group);
extern char *GetMyGroupName(void);
extern char* GetGroupNameByNode(Oid nodeoid);
#endif

#endif   /* GROUPMGR_H */
