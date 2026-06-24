/*-------------------------------------------------------------------------
 *
 * resgroupcmds.h
 *	  Commands for manipulating resource group.
 *
 * IDENTIFICATION
 * 		src/include/commands/resgroupcmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESGROUPCMDS_H
#define RESGROUPCMDS_H

#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/resgroup.h"
#include "utils/relcache.h"

extern void CreateResourceGroup(CreateResourceGroupStmt *stmt);
extern void DropResourceGroup(DropResourceGroupStmt *stmt);
extern void AlterResourceGroup(AlterResourceGroupStmt *stmt);

/* catalog access function */
extern Oid get_resgroup_oid(const char *name, bool missing_ok);
extern char *GetResGroupNameForId(Oid oid);
extern char *GetResGroupNameForRole(Oid roleid);
extern char *GetPriorityForRole(Oid roleid);
extern void GetResGroupCapabilities(Relation rel,
									Oid groupId,
									ResGroupCaps *resgroupCaps);
//extern void ResGroupCheckForRole(Oid groupId);

#endif   /* RESGROUPCMDS_H */
