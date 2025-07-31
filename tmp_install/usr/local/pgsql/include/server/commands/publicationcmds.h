/*-------------------------------------------------------------------------
 *
 * publicationcmds.h
 *      prototypes for publicationcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/commands/publicationcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PUBLICATIONCMDS_H
#define PUBLICATIONCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress CreatePublication(CreatePublicationStmt *stmt);
extern void AlterPublication(AlterPublicationStmt *stmt);
extern void RemovePublicationById(Oid pubid);
extern void RemovePublicationRelById(Oid proid);

extern ObjectAddress AlterPublicationOwner(const char *name, Oid newOwnerId);
extern void AlterPublicationOwner_oid(Oid pubid, Oid newOwnerId);

#ifdef __STORAGE_SCALABLE__
extern void RemovePublicationShardById(Oid proid);

extern char *GetPublicationShardDesc(Oid proid);
#endif

#endif                            /* PUBLICATIONCMDS_H */
