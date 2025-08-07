/*-------------------------------------------------------------------------
 *
 * pg_synonym.h
 *	  definition of the synonym relation (pg_synonym)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 2020-2030, Tencent.com.
 *
 * src/include/catalog/pg_synonym.h
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

#ifndef PG_SYNONYM_H
#define PG_SYNONYM_H

#include "catalog/genbki.h"

#define  SynonymRelationId 9085

CATALOG(pg_synonym,9085)
{
	NameData	synname;
	Oid			synnamespace;
	NameData	objnamespace; /* if null, will search object under search_path */
	NameData	objname;
	NameData	objdblink;
	Oid		synowner;
} FormData_pg_synonym;

typedef FormData_pg_synonym *Form_pg_synonym;

#define Natts_pg_synonym				6
#define Anum_pg_synonym_synname			1
#define Anum_pg_synonym_synnamespace	2
#define Anum_pg_synonym_objnamespace	3
#define Anum_pg_synonym_objname			4
#define Anum_pg_synonym_objdblink		5
#define Anum_pg_synonym_synowner		6

extern Oid CreateSynonym(char *synname, Oid synspcId, char *objspc, char *objname,
				char *dblink, Oid ownerId, bool isreplace);
/* synonymcmds.c */
extern Oid resolve_synonym(const char *synspc, const char *synname, char **objspc,
						char **objname, char **dblink);
#endif
