/*-------------------------------------------------------------------------
 *
 * keywords.c
 *	  lexical token lookup for key words in PostgreSQL
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/keywords.c
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
#include "common/keywords.h"
#include "parser/gram.h"

#define PG_KEYWORD(a,b,c) {a,b,c},

#else

#include "common/keywords.h"

/*
 * We don't need the token number for frontend uses, so leave it out to avoid
 * requiring backend headers that won't compile cleanly here.
 */
#define PG_KEYWORD(a,b,c) {a,0,c},

#endif							/* FRONTEND */

const ScanKeyword PgScanKeywords[] = {
#include "parser/kwlist.h"
};
const int	PgNumScanKeywords = lengthof(PgScanKeywords);

extern ScanKeyword OraScanKeywords[];
extern const int	OraNumScanKeywords;

/* begin opentenbase-ora-compatible */
typedef struct
{
	const char *name;
	int			len;
} OpKeyWord;

static OpKeyWord g_op_list[] = {
	{"like", 4},
	{"between", 7},
	{"and", 3}
};

typedef enum {
	OP_LIKE,
	OP_BETWEEN,
	OP_AND
} OP_INDEX;

/* the result is 94 */
#define KeyWord_INDEX_SIZE ('~' - ' ')

static int g_op_quickmatch[KeyWord_INDEX_SIZE] = {
	/*
	 * ---- first 0..31 chars are skipped ----
	 * 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
	 */
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, // 0
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, OP_AND, OP_BETWEEN, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, OP_LIKE, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, OP_AND, OP_BETWEEN, -1, -1, -1, // 5
	-1, -1, -1, -1, -1, -1, OP_LIKE, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1
	/* ---- chars over 126 are skipped ---- */
};

static bool
ora_op_prefix_match(const char *str, int len)
{
	int kwlen, idx = g_op_quickmatch[*str - ' '];
	bool res = false;

	/* no match */
	if (idx < 0)
		return res;

	kwlen = g_op_list[idx].len;

	/* eg: prepare fun1(text, int) as select * from tb1 where b like$1 a=$2; */
	if (strncasecmp(str, g_op_list[idx].name, g_op_list[idx].len) == 0 &&
		str[kwlen] == '$' &&
		(str[kwlen + 1] >= '0' && str[kwlen + 1] <= '9'))
		res = true;
	return res;
}

const ScanKeyword *
ScanKeywordLookup(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords)
{
	return ScanKeywordLookupInternal(text, keywords, num_keywords, NULL);
}
/* end opentenbase-ora-compatible */

/*
 * ScanKeywordLookup - see if a given word is a keyword
 *
 * The table to be searched is passed explicitly, so that this can be used
 * to search keyword lists other than the standard list appearing above.
 *
 * Returns a pointer to the ScanKeyword table entry, or NULL if no match.
 *
 * The match is done case-insensitively.  Note that we deliberately use a
 * dumbed-down case conversion that will only translate 'A'-'Z' into 'a'-'z',
 * even if we are in a locale where tolower() would produce more or different
 * translations.  This is to conform to the SQL99 spec, which says that
 * keywords are to be matched in this way even though non-keyword identifiers
 * receive a different case-normalization mapping.
 */
const ScanKeyword *
ScanKeywordLookupInternal(const char *text,
				  const ScanKeyword *keywords,
				  int num_keywords,
				  int *move_len)
{
	int			len,
				i;
	char		word[NAMEDATALEN];
	const ScanKeyword *low;
	const ScanKeyword *high;

	len = strlen(text);
	/* We assume all keywords are shorter than NAMEDATALEN. */
	if (len >= NAMEDATALEN)
		return NULL;

	/*
	 * Apply an ASCII-only downcasing.  We must not use tolower() since it may
	 * produce the wrong translation in some locales (eg, Turkish).
	 */
	for (i = 0; i < len; i++)
	{
		char		ch = text[i];

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		word[i] = ch;
	}
	word[len] = '\0';

	/*
	 * begin opentenbase-ora-compatible
	 * opentenbase_ora operator prefix matching support.
	 */
	if (move_len != NULL &&
		(*text - ' ') >= 0 &&
		(*text - ' ') < KeyWord_INDEX_SIZE &&
		g_op_quickmatch[*text - ' '] >= 0 &&
		len > g_op_list[g_op_quickmatch[*text - ' ']].len &&
		ora_op_prefix_match(text, len))
	{
		int oplen = g_op_list[g_op_quickmatch[*text - ' ']].len;

		word[oplen] = '\0';
		*move_len = oplen;
	}
	/* end opentenbase-ora-compatible */

	/*
	 * Now do a binary search using plain strcmp() comparison.
	 */
	low = keywords;
	high = keywords + (num_keywords - 1);
	while (low <= high)
	{
		const ScanKeyword *middle;
		int			difference;

		middle = low + (high - low) / 2;
		difference = strcmp(middle->name, word);
		if (difference == 0)
			return middle;
		else if (difference < 0)
			low = middle + 1;
		else
			high = middle - 1;
	}

	return NULL;
}


/*
 * PgKeyWordSwitch - switch to postgres key word
 */
void
PgKeyWordSwitch(void)
{
    NumScanKeywords = PgNumScanKeywords;
    ScanKeywords = PgScanKeywords;
}

/*
 * OraKeyWordSwitch - switch to opentenbase_ora key word
 */
void
OraKeyWordSwitch(void)
{
    NumScanKeywords = OraNumScanKeywords;
    ScanKeywords = OraScanKeywords;
}

const ScanKeyword *
ScanKeywordLookupByVal(int tok)
{
	int	idx;

	for (idx = 0; idx < OraNumScanKeywords; idx++)
	{
		if (OraScanKeywords[idx].value == tok)
			return &OraScanKeywords[idx];
	}

	return NULL;
}
