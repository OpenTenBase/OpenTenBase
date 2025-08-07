/*-------------------------------------------------------------------------
 *
 * pg_resgroupcapability.h
 *	  definition of the system "resource group capability" relation (pg_resgroupcapability).
 *
 *
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_RESGROUPCAPABILITY_H
#define PG_RESGROUPCAPABILITY_H

#include "catalog/genbki.h"

#define ResGroupCapabilityRelationId 9018

CATALOG(pg_resgroupcapability,9018) BKI_SHARED_RELATION
{
	Oid			resgroupid;	/* OID of the group with this capability  */

	int16		reslimittype;	/* resource limit type id (RESGROUP_LIMIT_TYPE_XXX) */

#ifdef CATALOG_VARLEN
	text		value;		/* resource limit (opaque type)  */
#endif
} FormData_pg_resgroupcapability;

/* ----------------
 *	Form_pg_resgroupcapability corresponds to a pointer to a tuple with
 *	the format of pg_resgroupcapability relation.
 * ----------------
 */
typedef FormData_pg_resgroupcapability *Form_pg_resgroupcapability;

/* ----------------
 *		compiler constants for pg_resgroupcapability
 * ----------------
 */
#define Natts_pg_resgroupcapability					3
#define Anum_pg_resgroupcapability_resgroupid		1
#define Anum_pg_resgroupcapability_reslimittype		2
#define Anum_pg_resgroupcapability_value			3

PGDATA(insert   ( 9019 1 20));
PGDATA(insert   ( 9019 2 30));
PGDATA(insert   ( 9019 3 -1));
PGDATA(insert   ( 9019 4 -1));
PGDATA(insert   ( 9020 1 10));
PGDATA(insert   ( 9020 2 10));
PGDATA(insert   ( 9020 3 -1));
PGDATA(insert   ( 9020 4 -1));

#endif   /* PG_RESGROUPCAPABILITY_H */
