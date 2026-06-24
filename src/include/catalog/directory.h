/*-------------------------------------------------------------------------
 *
 * pg_directory.h
 *	  definition of the system "directory" relation (pg_directory)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_directory.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DIRECTORY_H
#define PG_DIRECTORY_H

#include "catalog/genbki.h"

#define DirectoryId  4808

CATALOG(pg_directory,4808) BKI_WITHOUT_OIDS
{
	NameData	directory_name;
	NameData	nodename;
	NameData	directory_owner;
#ifdef CATALOG_VARLEN
	text		directory_path;
#endif
} FormData_pg_directory;

/* ----------------
 *		Form_pg_index corresponds to a pointer to a tuple with
 *		the format of pg_index relation.
 * ----------------
 */
typedef FormData_pg_directory *Form_pg_directory;

/* ----------------
 *		compiler constants for pg_index
 * ----------------
 */
#define Natts_pg_directory			4
#define Anum_directory_name			1
#define Anum_nodename				2
#define Anum_directory_owner		3
#define Anum_directory_path			4


#endif							/* PG_DIRECTORY_H */
