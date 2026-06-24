#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "regex/regex.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/varlena.h"

PG_FUNCTION_INFO_V1(orcl_regexp_instr_2);
PG_FUNCTION_INFO_V1(orcl_regexp_instr_3);
PG_FUNCTION_INFO_V1(orcl_regexp_instr_4);
PG_FUNCTION_INFO_V1(orcl_regexp_instr_5);
PG_FUNCTION_INFO_V1(orcl_regexp_instr_6);
PG_FUNCTION_INFO_V1(orcl_regexp_instr_7);

#define PG_GETARG_TEXT_PP_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_TEXT_PP(_n) : NULL)

Datum regexp_instr(PG_FUNCTION_ARGS);

/* cross-call state for regexp_match and regexp_split functions */
typedef struct regexp_matches_ctx
{
	text	   *orig_str;		/* data string in original TEXT form */
	int			nmatches;		/* number of places where pattern matched */
	int			npatterns;		/* number of capturing subpatterns */
	/* We store start char index and end+1 char index for each match */
	/* so the number of entries in match_locs is nmatches * npatterns * 2 */
	int		   *match_locs;		/* 0-based character indexes */
	int			next_match;		/* 0-based index of next match to process */
	/* workspace for build_regexp_match_result() */
	Datum	   *elems;			/* has npatterns elements */
	bool	   *nulls;			/* has npatterns elements */
} regexp_matches_ctx;

/*
 * We cache precompiled regular expressions using a "self organizing list"
 * structure, in which recently-used items tend to be near the front.
 * Whenever we use an entry, it's moved up to the front of the list.
 * Over time, an item's average position corresponds to its frequency of use.
 *
 * When we first create an entry, it's inserted at the front of
 * the array, dropping the entry at the end of the array if necessary to
 * make room.  (This might seem to be weighting the new entry too heavily,
 * but if we insert new entries further back, we'll be unable to adjust to
 * a sudden shift in the query mix where we are presented with MAX_CACHED_RES
 * never-before-seen items used circularly.  We ought to be able to handle
 * that case, so we have to insert at the front.)
 *
 * Knuth mentions a variant strategy in which a used item is moved up just
 * one place in the list.  Although he says this uses fewer comparisons on
 * average, it seems not to adapt very well to the situation where you have
 * both some reusable patterns and a steady stream of non-reusable patterns.
 * A reusable pattern that isn't used at least as often as non-reusable
 * patterns are seen will "fail to keep up" and will drop off the end of the
 * cache.  With move-to-front, a reusable pattern is guaranteed to stay in
 * the cache as long as it's used at least once in every MAX_CACHED_RES uses.
 */

/* this is the maximum number of cached regular expressions */
#ifndef MAX_CACHED_RES
#define MAX_CACHED_RES	32
#endif

/* this structure describes one cached regular expression */
typedef struct cached_re_str
{
	char	   *cre_pat;		/* original RE (not null terminated!) */
	int			cre_pat_len;	/* length of original RE, in bytes */
	int			cre_flags;		/* compile flags: extended,icase etc */
	Oid			cre_collation;	/* collation to use */
	regex_t		cre_re;			/* the compiled regular expression */
} cached_re_str;

/* RE flags parse functions */
static void orcl_parse_re_flags(pg_re_flags *flags, text *opts);
typedef void (*re_flags_hook)(pg_re_flags *flags, text *opts);

static regexp_matches_ctx *setup_regexp_matches_opentenbase_ora(text *orig_str,
					 int start_position, text *pattern,
					 text *flags, re_flags_hook flags_hook,
					 Oid collation,
					 bool force_glob,
					 bool use_subpatterns,
					 bool ignore_degenerate,
					 bool sava_all_match_locs);
static Datum build_orcl_regexp_instr_result(regexp_matches_ctx *instrctx,
							  int return_opt, int subexpr);

/*
 * RE_wchar_execute - execute a RE on pg_wchar data
 *
 * Returns TRUE on match, FALSE on no match
 *
 *	re --- the compiled pattern as returned by RE_compile_and_cache
 *	data --- the data to match against (need not be null-terminated)
 *	data_len --- the length of the data string
 *	start_search -- the offset in the data to start searching
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Data is given as array of pg_wchar which is what Spencer's regex package
 * wants.
 */
static bool
RE_wchar_execute(regex_t *re, pg_wchar *data, int data_len,
				 int start_search, int nmatch, regmatch_t *pmatch)
{
	int			regexec_result;
	char		errMsg[100];

	/* Perform RE match and return result */
	regexec_result = pg_regexec(re,
								data,
								data_len,
								start_search,
								NULL,	/* no details */
								nmatch,
								pmatch,
								0);

	if (regexec_result != REG_OKAY && regexec_result != REG_NOMATCH)
	{
		/* re failed??? */
		CHECK_FOR_INTERRUPTS();
		pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("regular expression failed: %s", errMsg)));
	}

	return (regexec_result == REG_OKAY);
}

/*
 * setup_regexp_matches --- do the initial matching for regexp_matches()
 *		or regexp_split()
 *
 * To avoid having to re-find the compiled pattern on each call, we do
 * all the matching in one swoop.  The returned regexp_matches_ctx contains
 * the locations of all the substrings matching the pattern.
 *
 * The three bool parameters have only two patterns (one for each caller)
 * but it seems clearer to distinguish the functionality this way than to
 * key it all off one "is_split" flag.
 */
static regexp_matches_ctx *
setup_regexp_matches_opentenbase_ora(text *orig_str, int start_position, text *pattern,
					 text *flags, re_flags_hook re_flags_hook,
					 Oid collation,
					 bool force_glob, bool use_subpatterns,
					 bool ignore_degenerate, bool sava_all_match_locs)

{
	regexp_matches_ctx *matchctx = palloc0(sizeof(regexp_matches_ctx));
	int			orig_len;
	pg_wchar   *wide_str;
	int			wide_len;
	pg_re_flags re_flags;
	regex_t    *cpattern;
	regmatch_t *pmatch;
	int			pmatch_len;
	int			array_len;
	int			array_idx;
	int			prev_match_end;
	int			start_search;
	Assert(re_flags_hook != NULL);
	Assert(start_position >= 0);
	/* save original string --- we'll extract result substrings from it */
	matchctx->orig_str = orig_str;
	/* convert string to pg_wchar form for matching */
	orig_len = VARSIZE_ANY_EXHDR(orig_str);
	wide_str = (pg_wchar *) palloc(sizeof(pg_wchar) * (orig_len + 1));
	wide_len = pg_mb2wchar_with_len(VARDATA_ANY(orig_str), wide_str, orig_len);
	/* determine options */
	(*re_flags_hook)(&re_flags, flags);

	if (force_glob)
	{
		/* user mustn't specify 'g' for regexp_split */
		if (re_flags.glob)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("regexp_split does not support the global option")));
		/* but we find all the matches anyway */
		re_flags.glob = true;
	}

	/* set up the compiled pattern */
	cpattern = RE_compile_and_cache(pattern, re_flags.cflags, collation);

	/* do we want to remember subpatterns? */
	if (use_subpatterns && cpattern->re_nsub > 0)
	{
		matchctx->npatterns = cpattern->re_nsub;
		pmatch_len = cpattern->re_nsub + 1;
	}
	else
	{
		/* no subpattern, npatterns = 0 indicate the whole pattern */
		use_subpatterns = false;
		matchctx->npatterns = 0;
		pmatch_len = 1;
	}

	/* temporary output space for RE package */
	pmatch = palloc(sizeof(regmatch_t) * pmatch_len);

	/* the real output space (grown dynamically if needed) */
	array_len = re_flags.glob ? 256 : 32;
	matchctx->match_locs = (int *) palloc(sizeof(int) * array_len);
	array_idx = 0;

	/* search for the pattern, perhaps repeatedly */
	prev_match_end = 0;
	if (start_position >= wide_len)
	{
		matchctx->nmatches = 0;
		return matchctx;
	}
	start_search = start_position;

	while (RE_wchar_execute(cpattern, wide_str, wide_len, start_search,
							pmatch_len, pmatch))
	{
		int			i;
		/*
		 * If requested, ignore degenerate matches, which are zero-length
		 * matches occurring at the start or end of a string or just after a
		 * previous match.
		 */
		if (!ignore_degenerate ||
			(pmatch[0].rm_so < wide_len &&
			 pmatch[0].rm_eo > prev_match_end))
		{
			/* enlarge output space if needed */
			while (array_idx + (matchctx->npatterns + 1) * 2 > array_len)
			{
				array_len *= 2;
				matchctx->match_locs = (int *) repalloc(matchctx->match_locs,
													sizeof(int) * array_len);
			}
			/*
			 * When compatible with opentenbase_ora grammar, we need save all match's
			 * locations. so that, 0 indicate the whole pattern, and 1 to 9
			 * indicate each sub pattern.
			 */
			if (sava_all_match_locs)
			{
				if (use_subpatterns)
				{
					for (i = 0; i <= matchctx->npatterns; i++)
					{
						matchctx->match_locs[array_idx++] = pmatch[i].rm_so;
						matchctx->match_locs[array_idx++] = pmatch[i].rm_eo;
					}
				}
				else
				{
					/* npatterns and pmatch_len is 1, no need for loop */
					matchctx->match_locs[array_idx++] = pmatch[0].rm_so;
					matchctx->match_locs[array_idx++] = pmatch[0].rm_eo;
				}
			}
			else
			{
				/* save this match's locations */
				if (use_subpatterns)
				{
					int			i;
					for (i = 1; i <= matchctx->npatterns; i++)
					{
						matchctx->match_locs[array_idx++] = pmatch[i].rm_so;
						matchctx->match_locs[array_idx++] = pmatch[i].rm_eo;
					}
				}
				else
				{
					matchctx->match_locs[array_idx++] = pmatch[0].rm_so;
					matchctx->match_locs[array_idx++] = pmatch[0].rm_eo;
				}
			}
			matchctx->nmatches++;
		}
		prev_match_end = pmatch[0].rm_eo;

		/* if not glob, stop after one match */
		if (!re_flags.glob)
			break;

		/*
		 * Advance search position.  Normally we start the next search at the
		 * end of the previous match; but if the match was of zero length, we
		 * have to advance by one character, or we'd just find the same match
		 * again.
		 */
		start_search = prev_match_end;
		if (pmatch[0].rm_so == pmatch[0].rm_eo)
			start_search++;
		if (start_search > wide_len)
			break;
	}

	/* Clean up temp storage */
	pfree(wide_str);
	pfree(pmatch);

	return matchctx;
}

static void
orcl_parse_re_flags(pg_re_flags *flags, text *opts)
{
	flags->cflags = REG_ADVANCED | REG_NLSTOP;
	flags->glob = false;

	if (opts)
	{
		char	   *opt_p = VARDATA_ANY(opts);
		int 		opt_len = VARSIZE_ANY_EXHDR(opts);
		int 		i;

		for (i = 0; i < opt_len; i++)
		{
			switch (opt_p[i])
			{
				case 'c':		/* case sensitive */
					flags->cflags &= ~REG_ICASE;
					break;
				case 'i':		/* case insensitive */
					flags->cflags |= REG_ICASE;
					break;
				case 'n':
					flags->cflags &= ~REG_NEWLINE;
					break;
				case 'm':
					flags->cflags |= REG_NEWLINE;
					break;
				case 'x':
					flags->cflags |= REG_EXPANDED;
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid opentenbase_ora regexp option: \"%c\"",
									opt_p[i])));
					break;
			}
		}
	}
}

static Datum
build_orcl_regexp_instr_result(regexp_matches_ctx *instrctx,
							  int return_opt, int subexpr)
{
	int loc;
	int so, eo;

	Assert(instrctx);
	Assert(return_opt >= 0);
	Assert(subexpr >= 0 && subexpr <= instrctx->npatterns);

	loc = instrctx->next_match * (instrctx->npatterns + 1) * 2;
	loc += 2 * subexpr;

	so = instrctx->match_locs[loc];
	eo = instrctx->match_locs[loc + 1];

	/*
	 * Convert source_char offset to "position" by adding 1.
	 */
	if (return_opt == 0)
		PG_RETURN_INT32(so + 1);

	PG_RETURN_INT32(eo + 1);
}

/*
 * REGEXP_INSTR (source_char, pattern
 *				 [, position
 *					[, occurrence
 *					   [, return_opt
 *						  [, match_param
 *							 [, subexpr]
 *						  ]
 *					   ]
 *					]
 *				 ]
 * Note: position is a positive integer indicating the character of source_char
 * where we should begin the search. The default is 1, meaning that we begin the
 * search at the first character of source_char.
 *
 * Note: occurrence is a positive integer indicating which occurrence of pattern
 * in source_char we should search for. The default is 1, meaning that we search
 * for the first occurrence of pattern. If occurrence is greater than 1, then
 * the database searches for the second occurrence beginning with the first
 * character following the first occurrence of pattern, and so forth.
 *
 * Note: return_option lets you specify what we should return in relation to
 * the occurrence:
 *	   If you specify 0, then we return the position of the first character of
 *	   the occurrence. This is the default.
 *	   If you specify greater than 1, then we return the position of the
 *	   character following the occurrence.
 *
 * Note: For a pattern with subexpressions, subexpr is an non-negative integer
 * indicating which subexpression in pattern is the target of the function. The
 * subexpr is a fragment of pattern enclosed in parentheses. Subexpressions can
 * be nested. Subexpressions are numbered in order in which their left
 * parentheses appear in pattern.
 */
Datum
regexp_instr(PG_FUNCTION_ARGS)
{
	text	*s = PG_GETARG_TEXT_PP_IF_NULL(0);
	text	*p = PG_GETARG_TEXT_PP_IF_NULL(1);
	int 	 position = PG_GETARG_INT32_1_IF_NULL(2);
	int 	 occurence = PG_GETARG_INT32_1_IF_NULL(3);
	int 	 return_opt = PG_GETARG_INT32_0_IF_NULL(4);
	text	*flags = PG_GETARG_TEXT_PP_IF_NULL(5);
	int 	 subexpr = PG_GETARG_INT32_0_IF_NULL(6);
	regexp_matches_ctx *instrctx;

	/*
	 * return null if source_char is null or empty
	 */
	if (s == NULL || text_to_cstring(s)[0] == 0x00)
		PG_RETURN_NULL();

	/*
	 * return null if pattern is null or empty
	 */
	if (p == NULL || text_to_cstring(p)[0] == 0x00)
		PG_RETURN_NULL();

	/*
	 * similar to OpenTenBase_Ora, pattern size limits to 512 bytes.
	 */
	if (VARSIZE_ANY_EXHDR(p) > 512)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Regular expression too long"),
				errhint("\"pattern\" must be less than or equal to 512 bytes")));

	/*
	 * convert "position" to "source_char" offset
	 */
	if (position <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"position\" value (%d) for \"regexp_instr\"",
					position),
				errhint("\"position\" must be a positive integer")));
	position = position - 1;

	/*
	 * make sure "occurence" be a positive integer
	 */
	if (occurence <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"occurence\" value (%d) for \"regexp_instr\"",
					occurence),
				errhint("\"occurence\" must be a positive integer")));

	/*
	 * make sure "return_opt" be a nonnegative integer
	 */
	if (return_opt < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"return_opt\" value (%d) for \"regexp_instr\"",
					occurence),
				errhint("\"return_opt\" must be a nonnegative integer")));

	/*
	 * make sure "subexpr" be a nonnegative integer
	 */
	if (subexpr < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"subexpr\" value (%d) for \"regexp_instr\"",
					subexpr),
				errhint("\"subexpr\" must be a nonnegative integer")));

	instrctx = setup_regexp_matches_opentenbase_ora(s, position, p,
									flags, orcl_parse_re_flags,
									PG_GET_COLLATION(),
									true, true, true, true);

	if (occurence > instrctx->nmatches)
		PG_RETURN_INT32(0);
	if (subexpr > instrctx->npatterns)
		PG_RETURN_INT32(0);

	instrctx->next_match = occurence - 1;

	return build_orcl_regexp_instr_result(instrctx, return_opt, subexpr);
}

/*
 * REGEXP_INSTR (source_char, pattern)
 */
Datum
orcl_regexp_instr_2(PG_FUNCTION_ARGS)
{
	return regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position)
 */
Datum
orcl_regexp_instr_3(PG_FUNCTION_ARGS)
{
	return regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence)
 */
Datum
orcl_regexp_instr_4(PG_FUNCTION_ARGS)
{
	return regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence, return_opt)
 */
Datum
orcl_regexp_instr_5(PG_FUNCTION_ARGS)
{
	return regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence, return_opt, match_param)
 */
Datum
orcl_regexp_instr_6(PG_FUNCTION_ARGS)
{
	return regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence, return_opt, match_param, subexpr)
 */
Datum
orcl_regexp_instr_7(PG_FUNCTION_ARGS)
{
	return regexp_instr(fcinfo);
}
