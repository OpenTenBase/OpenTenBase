/*-------------------------------------------------------------------------
 *
 * dbcommands.h
 *		Database management commands (create/drop database).
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
extern void dropdb_prepare(const char *dbname, bool missing_ok, bool force);
extern void dropdb(const char *dbname, bool missing_ok, bool force);
#if 0
extern void DropDatabase(ParseState *pstate, DropdbStmt *stmt);
#endif
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

extern char pg_char_to_sql_mode(char* smode);
extern const char* pg_sql_mode_to_char(char smode);

extern void CreateDirAlias(const CreateDirStmt *stmt);
extern void CheckDirExists(const CreateDirStmt *stmt);
extern void DropDirAlias(const DropDirStmt *stmt);

#endif							/* DBCOMMANDS_H */
