/*
 * pg_auth_history.c
 *	  definition of the system "authorization history" relation
 *	  (pg_auth_history) along with the relation's initial contents.
 *
 * Copyright (c) 1996-2018, OpenTenBase-C Development Group
 *
 * IDENTIFICATION
 *	  src/include/catalog/pg_auth_history.c
 */

#ifndef PG_AUTH_HISTORY_H
#define PG_AUTH_HISTORY_H

/* define the OID of the table pg_auth_history*/
#define AuthHistoryRelationId	9108
#define AuthHistoryRelation_Rowtype_Id 9109

CATALOG(pg_auth_history,9108) BKI_SHARED_RELATION BKI_SCHEMA_MACRO
{
	Oid roloid;		    			/* role OID*/
#ifdef CATALOG_VARLEN
	text rolpassword;				/* role password, md5-encryption,sha256-encryption, or none-encryption */
#endif
} FormData_pg_auth_history;

/*-------------------------------------------------------------------------
 *		Form_pg_auth_history corresponds to a pointer to a tuple with
 *		the format of pg_auth_history relation.
 *-------------------------------------------------------------------------
 */
typedef FormData_pg_auth_history *Form_pg_auth_history;

/*-------------------------------------------------------------------------
 *		compiler constants for pg_auth_history
 *-------------------------------------------------------------------------
 */
#define Natts_pg_auth_history					2
#define Anum_pg_auth_history_roloid				1
#define Anum_pg_auth_history_rolpassword		2

#endif   /* PG_AUTH_HISTORY_H */
