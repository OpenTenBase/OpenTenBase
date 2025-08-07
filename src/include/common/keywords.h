/*-------------------------------------------------------------------------
 *
 * keywords.h
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/keywords.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KEYWORDS_H
#define KEYWORDS_H

/* Keyword categories --- should match lists in gram.y */
#define UNRESERVED_KEYWORD		0
#define COL_NAME_KEYWORD		1
#define TYPE_FUNC_NAME_KEYWORD	2
#define RESERVED_KEYWORD		3
#define PKG_RESERVED_KEYWORD	4


typedef struct ScanKeyword
{
	const char *name;			/* in lower case */
	int16		value;			/* grammar's token code */
	int16		category;		/* see codes above */
} ScanKeyword;

#ifndef FRONTEND
extern PGDLLIMPORT const ScanKeyword *ScanKeywords;
extern PGDLLIMPORT int NumScanKeywords;

#else
extern const ScanKeyword *ScanKeywords;
extern int NumScanKeywords;
#endif

extern const ScanKeyword *ScanKeywordLookup(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords);
extern const ScanKeyword *ScanKeywordLookupInternal(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords,
				  int *move_len);
extern void PgKeyWordSwitch(void);
extern void OraKeyWordSwitch(void);
extern const ScanKeyword *ScanKeywordLookupByVal(int tok);

#endif							/* KEYWORDS_H */
