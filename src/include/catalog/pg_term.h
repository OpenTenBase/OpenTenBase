/*-------------------------------------------------------------------------
 *
 * pg_term.h
 *
 * src/include/catalog/pg_term.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TERM_H
#define PG_TERM_H

#include "catalog/genbki.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"

/* ----------------
 *		pg_term definition.  cpp turns this into
 *		typedef struct FormData_pg_term
 * ----------------
 */
#define TermRelationId	5200
#define TermRelation_Rowtype_Id	5203

CATALOG(pg_term,5200) BKI_SHARED_RELATION BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(5203) BKI_SCHEMA_MACRO
{
	Oid id;
	Oid term;		/* real term */
} FormData_pg_term;
/* ----------------
 *		Form_pg_term corresponds to a pointer to a tuple with
 *		the format of pg_term relation.
 * ----------------
 */
typedef FormData_pg_term *Form_pg_term;

/* ----------------
 *		compiler constants for pg_term
 * ----------------
 */
#define Natts_pg_term					2
#define Anum_pg_term_id					1
#define Anum_pg_term_term				2
/* ----------------
 *		initial contents of pg_term
 * ----------------
 */
PGDATA(insert (3, 1));

extern void UpdateTerm(UpdateTermStmt *stmt);
extern void ShowCurrentTerm(QueryTermStmt *stmt, DestReceiver *dest);

#endif							/* PG_TERM_H */
