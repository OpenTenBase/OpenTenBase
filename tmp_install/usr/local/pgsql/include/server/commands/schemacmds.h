/*-------------------------------------------------------------------------
 *
 * schemacmds.h
 *      prototypes for schemacmds.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/commands/schemacmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SCHEMACMDS_H
#define SCHEMACMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern Oid CreateSchemaCommand(CreateSchemaStmt *parsetree,
                    const char *queryString,
                    bool is_top_level,
                    int stmt_location, int stmt_len);

extern void RemoveSchemaById(Oid schemaOid);

extern ObjectAddress RenameSchema(const char *oldname, const char *newname);
extern ObjectAddress AlterSchemaOwner(const char *name, Oid newOwnerId);
extern void AlterSchemaOwner_oid(Oid schemaOid, Oid newOwnerId);
extern char * GetSchemaNameByOid(Oid schemaOid);

#endif                            /* SCHEMACMDS_H */
