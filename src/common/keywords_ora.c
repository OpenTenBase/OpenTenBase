/*-------------------------------------------------------------------------
 *
 * keywords_ora.c
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/keywords_ora.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#include "catalog/pg_database.h"
#else
#include "postgres_fe.h"
#endif

#ifndef FRONTEND

#include "nodes/parsenodes.h"
#include "parser/scanner.h"
#include "opentenbase_ora/gram_ora.h"

#define ORA_KEYWORD(a,b,c) {a,b,c},

#else

#include "common/keywords.h"

/*
 * We don't need the token number for frontend uses, so leave it out to avoid
 * requiring backend headers that won't compile cleanly here.
 */
#define ORA_KEYWORD(a,b,c) {a,0,c},

#endif							/* FRONTEND */

const ScanKeyword OraScanKeywords[] = {
#include "opentenbase_ora/kwlist_ora.h"
};
const int	OraNumScanKeywords = lengthof(OraScanKeywords);

/*
 * Use opentenbase_ora keyword as default.
 *
 * In postgres backends, key word automatically switch according to
 * the connection while other tools don't.
 * opentenbase_ora contains all the postgres keywords right now and pg doesn't.
 * while adding special keyword for pg that opentenbase_ora doesn't, this has to
 * be refactored according to the database tool connects.
 */
const ScanKeyword *ScanKeywords = OraScanKeywords;
int   NumScanKeywords  = lengthof(OraScanKeywords);
