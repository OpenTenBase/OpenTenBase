/*-------------------------------------------------------------------------
 *
 * pg_dblink.h
 *	  definition of the system dblink relation like opentenbase_ora (pg_dblink)
 *	  along with the relation's initial contents.
 *
 *
 * src/include/catalog/pg_dblink.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DBLINK_H
#define PG_DBLINK_H

#include "catalog/genbki.h"
#include "catalog/pg_profile_user_password.h"
#include "nodes/parsenodes.h"

#define timestamptz int64
#define INET6_ADDRSTRLEN 46

#define DblinkRelationId  9046

CATALOG(pg_dblink,9046)
{
    NameData  dblinkname;            /* Name of the database link */
    NameData  dblinkowner;           /* Owner of the database link */
    NameData  username;              /* Name of the user when logging in */
    timestamptz  created;            /* Creation time of the database link */
#ifdef CATALOG_VARLEN
    text      host;                  /* opentenbase_ora style Net connect string */
#endif
    int32      port;                  /* opentenbase_ora style Net connect string */
    int32      dblinkkind;            /* dblink type(public | shared) */
#ifdef CATALOG_VARLEN
    text       dblink_foreign_server;
#endif
} FormData_pg_dblink;

typedef FormData_pg_dblink *Form_pg_dblink;

#define Natts_pg_dblink					8

#define Anum_pg_dblink_name				1
#define Anum_pg_dblink_owner			2
#define Anum_pg_dblink_user_name		3
#define Anum_pg_dblink_created			4
#define Anum_pg_dblink_host				5
#define Anum_pg_dblink_port				6
#define Anum_pg_dblink_kind				7
#define Anum_pg_foreign_server			8

extern void DblinkDrop(const char* dblink_name);
extern void DblinkCreate(const char *dblink_name, 
						 int dblink_kind,
						 const char *user_name,
						 const char *host,
						 int port,
						 const char *foreign_server);
extern char* GetForeignServerFromDblinkName(const char *dblink_name);
extern char *GetForeignUserFromDblinkName(const char *dblink_name);

#endif
/* PG_DBLINK_H */
