/*-------------------------------------------------------------------------
 *
 * dbcommands.h
 *		Database management commands (create/drop database).
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/commands/dbcommands.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBCOMMANDS_H
#define DBCOMMANDS_H

#include "access/xlogreader.h"
#include "catalog/objectaddress.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

extern Oid	createdb(ParseState *pstate, const CreatedbStmt *stmt);
extern bool dropdb(const char *dbname, bool missing_ok);
extern void dropdb_prepare(const char *dbname, bool missing_ok);
extern ObjectAddress RenameDatabase(const char *oldname, const char *newname);
extern Oid	AlterDatabase(ParseState *pstate, AlterDatabaseStmt *stmt, bool isTopLevel);
extern Oid	AlterDatabaseSet(AlterDatabaseSetStmt *stmt);
extern ObjectAddress AlterDatabaseOwner(const char *dbname, Oid newOwnerId);

extern Oid	get_database_oid(const char *dbname, bool missingok);
extern char *get_database_name(Oid dbid);

extern void check_encoding_locale_matches(int encoding, const char *collate, const char *ctype);

#ifdef PGXC
extern bool IsSetTableSpace(AlterDatabaseStmt *stmt);
#endif

#endif							/* DBCOMMANDS_H */
