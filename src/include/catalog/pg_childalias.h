/*-------------------------------------------------------------------------
 *
 * pg_childalias.h
 *	  definition of the system "package" relation (pg_package)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 2020-2030, Tencent.com.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_childalias.h
 *
 * NOTES
 *	  The script catalog/genbki.pl reads this file and generates .bki
 *	  information from the DATA() statements.  utils/Gen_fmgrtab.pl
 *	  generates fmgroids.h and fmgrtab.c the same way.
 *
 *	  XXX do NOT break up DATA() statements into multiple lines!
 *		  the scripts are not as smart as you might think...
 *	  XXX (eg. #if 0 #endif won't do what you think)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CHILDALIAS_H
#define PG_CHILDALIAS_H

#include "catalog/genbki.h"

#define ChildAliasRelationId  9050

CATALOG(pg_childalias,9050)
{
    Oid	        parentoid;	/* Oid of child table */
    NameData	aname;	    /* alias name */
	Oid			childoid;	/* OID of child table */
} FormData_pg_childalias;

/* ----------------
 *		FormData_pg_childalias corresponds to a pointer to a tuple with
 *		the format of pg_childalias relation.
 * ----------------
 */
typedef FormData_pg_childalias *Form_pg_childalias;

/* ----------------
 *		compiler constants for pg_package
 * ----------------
 */
#define Natts_pg_childas					3
#define Anum_pg_childas_poid			    1
#define Anum_pg_childas_aname			    2
#define Anum_pg_childas_coid			    3



#endif							/* PG_CHILDALIAS_H */
