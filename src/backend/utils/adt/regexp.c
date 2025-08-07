/*-------------------------------------------------------------------------
 *
 * regexp.c
 *	  Postgres' interface to the regular expression package.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/regexp.c
 *
 *		Alistair Crooks added the code for the regex caching
 *		agc - cached the regular expressions used - there's a good chance
 *		that we'll get a hit, so this saves a compile step for every
 *		attempted match. I haven't actually measured the speed improvement,
 *		but it `looks' a lot quicker visually when watching regression
 *		test output.
 *
 *		agc - incorporated Keith Bostic's Berkeley regex code into
 *		the tree for all ports. To distinguish this regex code from any that
 *		is existent on a platform, I've prepended the string "pg_" to
 *		the functions regcomp, regerror, regexec and regfree.
 *		Fixed a bug that was originally a typo by me, where `i' was used
 *		instead of `oldest' when compiling regular expressions - benign
 *		results mostly, although occasionally it bit you...
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "regex/regex.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/varlena.h"

#define PG_GETARG_TEXT_PP_IF_EXISTS(_n) \
	(PG_NARGS() > (_n) ? PG_GETARG_TEXT_PP(_n) : NULL)


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
	pg_wchar   *wide_str;		/* wide-char version of original string */
	char	   *conv_buf;		/* conversion buffer, if needed */
	int			conv_bufsiz;	/* size thereof */
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

static int	num_res = 0;		/* # of cached re's */
static cached_re_str re_array[MAX_CACHED_RES];	/* cached re's */

#ifdef _PG_ORCL_
/* RE flags parse functions */
static void orcl_parse_re_flags(pg_re_flags *flags, text *opts);
typedef void (*re_flags_hook)(pg_re_flags *flags, text *opts);
#endif

/* Local functions */
static regexp_matches_ctx *setup_regexp_matches(text *orig_str, text *pattern,
					 pg_re_flags *flags,
					 int start_search,
					 Oid collation,
					 bool use_subpatterns,
					 bool ignore_degenerate,
					 bool fetching_unmatched);

static regexp_matches_ctx *setup_regexp_matches_opentenbase_ora(text *orig_str,
					 int start_position, text *pattern,
					 text *flags, re_flags_hook flags_hook,
					 Oid collation,
					 bool force_glob,
					 bool use_subpatterns,
					 bool ignore_degenerate,
					 bool sava_all_match_locs);
static ArrayType *build_regexp_match_result(regexp_matches_ctx *matchctx);
static Datum build_regexp_split_result(regexp_matches_ctx *splitctx);
#ifdef _PG_ORCL_
static Datum build_orcl_regexp_substr_result(regexp_matches_ctx *substrctx,
											int subexpr);
static Datum build_orcl_regexp_instr_result(regexp_matches_ctx *instrctx,
							  int return_opt, int subexpr);
#endif

/*
 * RE_compile_and_cache - compile a RE, caching if possible
 *
 * Returns regex_t *
 *
 *	text_re --- the pattern, expressed as a TEXT object
 *	cflags --- compile options for the pattern
 *	collation --- collation to use for LC_CTYPE-dependent behavior
 *
 * Pattern is given in the database encoding.  We internally convert to
 * an array of pg_wchar, which is what Spencer's regex package wants.
 */
regex_t *
RE_compile_and_cache(text *text_re, int cflags, Oid collation)
{
	int			text_re_len = VARSIZE_ANY_EXHDR(text_re);
	char	   *text_re_val = VARDATA_ANY(text_re);
	pg_wchar   *pattern;
	int			pattern_len;
	int			i;
	int			regcomp_result;
	cached_re_str re_temp;
	char		errMsg[100];

	/*
	 * Look for a match among previously compiled REs.  Since the data
	 * structure is self-organizing with most-used entries at the front, our
	 * search strategy can just be to scan from the front.
	 */
	for (i = 0; i < num_res; i++)
	{
		if (re_array[i].cre_pat_len == text_re_len &&
			re_array[i].cre_flags == cflags &&
			re_array[i].cre_collation == collation &&
			memcmp(re_array[i].cre_pat, text_re_val, text_re_len) == 0)
		{
			/*
			 * Found a match; move it to front if not there already.
			 */
			if (i > 0)
			{
				re_temp = re_array[i];
				memmove(&re_array[1], &re_array[0], i * sizeof(cached_re_str));
				re_array[0] = re_temp;
			}

			return &re_array[0].cre_re;
		}
	}

	/*
	 * Couldn't find it, so try to compile the new RE.  To avoid leaking
	 * resources on failure, we build into the re_temp local.
	 */

	/* Convert pattern string to wide characters */
	pattern = (pg_wchar *) palloc((text_re_len + 1) * sizeof(pg_wchar));
	pattern_len = pg_mb2wchar_with_len(text_re_val,
									   pattern,
									   text_re_len);

	regcomp_result = pg_regcomp(&re_temp.cre_re,
								pattern,
								pattern_len,
								cflags,
								collation);

	pfree(pattern);

	if (regcomp_result != REG_OKAY)
	{
		/* re didn't compile (no need for pg_regfree, if so) */

		/*
		 * Here and in other places in this file, do CHECK_FOR_INTERRUPTS
		 * before reporting a regex error.  This is so that if the regex
		 * library aborts and returns REG_CANCEL, we don't print an error
		 * message that implies the regex was invalid.
		 */
		CHECK_FOR_INTERRUPTS();

		pg_regerror(regcomp_result, &re_temp.cre_re, errMsg, sizeof(errMsg));
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
				 errmsg("invalid regular expression: %s", errMsg)));
	}

	/*
	 * We use malloc/free for the cre_pat field because the storage has to
	 * persist across transactions, and because we want to get control back on
	 * out-of-memory.  The Max() is because some malloc implementations return
	 * NULL for malloc(0).
	 */
	re_temp.cre_pat = malloc(Max(text_re_len, 1));
	if (re_temp.cre_pat == NULL)
	{
		pg_regfree(&re_temp.cre_re);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
	}
	memcpy(re_temp.cre_pat, text_re_val, text_re_len);
	re_temp.cre_pat_len = text_re_len;
	re_temp.cre_flags = cflags;
	re_temp.cre_collation = collation;

	/*
	 * Okay, we have a valid new item in re_temp; insert it into the storage
	 * array.  Discard last entry if needed.
	 */
	if (num_res >= MAX_CACHED_RES)
	{
		--num_res;
		Assert(num_res < MAX_CACHED_RES);
		pg_regfree(&re_array[num_res].cre_re);
		free(re_array[num_res].cre_pat);
	}

	if (num_res > 0)
		memmove(&re_array[1], &re_array[0], num_res * sizeof(cached_re_str));

	re_array[0] = re_temp;
	num_res++;

	return &re_array[0].cre_re;
}

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
 * RE_execute - execute a RE
 *
 * Returns TRUE on match, FALSE on no match
 *
 *	re --- the compiled pattern as returned by RE_compile_and_cache
 *	dat --- the data to match against (need not be null-terminated)
 *	dat_len --- the length of the data string
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Data is given in the database encoding.  We internally
 * convert to array of pg_wchar which is what Spencer's regex package wants.
 */
static bool
RE_execute(regex_t *re, char *dat, int dat_len,
		   int nmatch, regmatch_t *pmatch)
{
	pg_wchar   *data;
	int			data_len;
	bool		match;

	/* Convert data string to wide characters */
	data = (pg_wchar *) palloc((dat_len + 1) * sizeof(pg_wchar));
	data_len = pg_mb2wchar_with_len(dat, data, dat_len);

	/* Perform RE match and return result */
	match = RE_wchar_execute(re, data, data_len, 0, nmatch, pmatch);

	pfree(data);
	return match;
}

/*
 * RE_compile_and_execute - compile and execute a RE
 *
 * Returns TRUE on match, FALSE on no match
 *
 *	text_re --- the pattern, expressed as a TEXT object
 *	dat --- the data to match against (need not be null-terminated)
 *	dat_len --- the length of the data string
 *	cflags --- compile options for the pattern
 *	collation --- collation to use for LC_CTYPE-dependent behavior
 *	nmatch, pmatch	--- optional return area for match details
 *
 * Both pattern and data are given in the database encoding.  We internally
 * convert to array of pg_wchar which is what Spencer's regex package wants.
 */
bool
RE_compile_and_execute(text *text_re, char *dat, int dat_len,
					   int cflags, Oid collation,
					   int nmatch, regmatch_t *pmatch)
{
	regex_t    *re;

	/* Compile RE */
	re = RE_compile_and_cache(text_re, cflags, collation);

	return RE_execute(re, dat, dat_len, nmatch, pmatch);
}


/*
 * parse_re_flags - parse the options argument of regexp_match and friends
 *
 *	flags --- output argument, filled with desired options
 *	opts --- TEXT object, or NULL for defaults
 *
 * This accepts all the options allowed by any of the callers; callers that
 * don't want some have to reject them after the fact.
 */
void
parse_re_flags(pg_re_flags *flags, text *opts)
{
	/* regex flavor is always folded into the compile flags */
	flags->cflags = REG_ADVANCED;
	flags->glob = false;

	if (opts)
	{
		char	   *opt_p = VARDATA_ANY(opts);
		int			opt_len = VARSIZE_ANY_EXHDR(opts);
		int			i;

		for (i = 0; i < opt_len; i++)
		{
			switch (opt_p[i])
			{
				case 'g':
					flags->glob = true;
					break;
				case 'b':		/* BREs (but why???) */
					flags->cflags &= ~(REG_ADVANCED | REG_EXTENDED | REG_QUOTE);
					break;
				case 'c':		/* case sensitive */
					flags->cflags &= ~REG_ICASE;
					break;
				case 'e':		/* plain EREs */
					flags->cflags |= REG_EXTENDED;
					flags->cflags &= ~(REG_ADVANCED | REG_QUOTE);
					break;
				case 'i':		/* case insensitive */
					flags->cflags |= REG_ICASE;
					break;
				case 'm':		/* Perloid synonym for n */
				case 'n':		/* \n affects ^ $ . [^ */
					flags->cflags |= REG_NEWLINE;
					break;
				case 'p':		/* ~Perl, \n affects . [^ */
					flags->cflags |= REG_NLSTOP;
					flags->cflags &= ~REG_NLANCH;
					break;
				case 'q':		/* literal string */
					flags->cflags |= REG_QUOTE;
					flags->cflags &= ~(REG_ADVANCED | REG_EXTENDED);
					break;
				case 's':		/* single line, \n ordinary */
					flags->cflags &= ~REG_NEWLINE;
					break;
				case 't':		/* tight syntax */
					flags->cflags &= ~REG_EXPANDED;
					break;
				case 'w':		/* weird, \n affects ^ $ only */
					flags->cflags &= ~REG_NLSTOP;
					flags->cflags |= REG_NLANCH;
					break;
				case 'x':		/* expanded syntax */
					flags->cflags |= REG_EXPANDED;
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid regular expression option: \"%c\"",
									opt_p[i])));
					break;
			}
		}
	}
}


/*
 *	interface routines called by the function manager
 */

Datum
nameregexeq(PG_FUNCTION_ARGS)
{
	Name		n = PG_GETARG_NAME(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(RE_compile_and_execute(p,
										  NameStr(*n),
										  strlen(NameStr(*n)),
										  REG_ADVANCED,
										  PG_GET_COLLATION(),
										  0, NULL));
}

Datum
nameregexne(PG_FUNCTION_ARGS)
{
	Name		n = PG_GETARG_NAME(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(!RE_compile_and_execute(p,
										   NameStr(*n),
										   strlen(NameStr(*n)),
										   REG_ADVANCED,
										   PG_GET_COLLATION(),
										   0, NULL));
}

Datum
textregexeq(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(RE_compile_and_execute(p,
										  VARDATA_ANY(s),
										  VARSIZE_ANY_EXHDR(s),
										  REG_ADVANCED,
										  PG_GET_COLLATION(),
										  0, NULL));
}

Datum
textregexne(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(!RE_compile_and_execute(p,
										   VARDATA_ANY(s),
										   VARSIZE_ANY_EXHDR(s),
										   REG_ADVANCED,
										   PG_GET_COLLATION(),
										   0, NULL));
}


/*
 *	routines that use the regexp stuff, but ignore the case.
 *	for this, we use the REG_ICASE flag to pg_regcomp
 */


Datum
nameicregexeq(PG_FUNCTION_ARGS)
{
	Name		n = PG_GETARG_NAME(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(RE_compile_and_execute(p,
										  NameStr(*n),
										  strlen(NameStr(*n)),
										  REG_ADVANCED | REG_ICASE,
										  PG_GET_COLLATION(),
										  0, NULL));
}

Datum
nameicregexne(PG_FUNCTION_ARGS)
{
	Name		n = PG_GETARG_NAME(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(!RE_compile_and_execute(p,
										   NameStr(*n),
										   strlen(NameStr(*n)),
										   REG_ADVANCED | REG_ICASE,
										   PG_GET_COLLATION(),
										   0, NULL));
}

Datum
texticregexeq(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(RE_compile_and_execute(p,
										  VARDATA_ANY(s),
										  VARSIZE_ANY_EXHDR(s),
										  REG_ADVANCED | REG_ICASE,
										  PG_GET_COLLATION(),
										  0, NULL));
}

Datum
texticregexne(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);

	PG_RETURN_BOOL(!RE_compile_and_execute(p,
										   VARDATA_ANY(s),
										   VARSIZE_ANY_EXHDR(s),
										   REG_ADVANCED | REG_ICASE,
										   PG_GET_COLLATION(),
										   0, NULL));
}


/*
 * textregexsubstr()
 *		Return a substring matched by a regular expression.
 */
Datum
textregexsubstr(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);
	regex_t    *re;
	regmatch_t	pmatch[2];
	int			so,
				eo;

	/* Compile RE */
	re = RE_compile_and_cache(p, REG_ADVANCED, PG_GET_COLLATION());

	/*
	 * We pass two regmatch_t structs to get info about the overall match and
	 * the match for the first parenthesized subexpression (if any). If there
	 * is a parenthesized subexpression, we return what it matched; else
	 * return what the whole regexp matched.
	 */
	if (!RE_execute(re,
					VARDATA_ANY(s), VARSIZE_ANY_EXHDR(s),
					2, pmatch))
		PG_RETURN_NULL();		/* definitely no match */

	if (re->re_nsub > 0)
	{
		/* has parenthesized subexpressions, use the first one */
		so = pmatch[1].rm_so;
		eo = pmatch[1].rm_eo;
	}
	else
	{
		/* no parenthesized subexpression, use whole match */
		so = pmatch[0].rm_so;
		eo = pmatch[0].rm_eo;
	}

	/*
	 * It is possible to have a match to the whole pattern but no match for a
	 * subexpression; for example 'foo(bar)?' is considered to match 'foo' but
	 * there is no subexpression match.  So this extra test for match failure
	 * is not redundant.
	 */
	if (so < 0 || eo < 0)
		PG_RETURN_NULL();

	return DirectFunctionCall3(text_substr,
							   PointerGetDatum(s),
							   Int32GetDatum(so + 1),
							   Int32GetDatum(eo - so));
}

/*
 * textregexreplace_noopt()
 *		Return a string matched by a regular expression, with replacement.
 *
 * This version doesn't have an option argument: we default to case
 * sensitive match, replace the first instance only.
 */
Datum
textregexreplace_noopt(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);
	text	   *r = PG_GETARG_TEXT_PP(2);
	regex_t    *re;
    text* result = NULL;

	re = RE_compile_and_cache(p, REG_ADVANCED, PG_GET_COLLATION());

	result = replace_text_regexp(s, (void *) re, r, false);
	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(result), VARSIZE_ANY_EXHDR(result)) == 0)
	{
		pfree(result);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(result);
}

/*
 * textregexreplace()
 *		Return a string matched by a regular expression, with replacement.
 */
Datum
textregexreplace(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP(0);
	text	   *p = PG_GETARG_TEXT_PP(1);
	text	   *r = PG_GETARG_TEXT_PP(2);
	text	   *opt = PG_GETARG_TEXT_PP(3);
	regex_t    *re;
	pg_re_flags flags;
    text* result = NULL;

	parse_re_flags(&flags, opt);

	re = RE_compile_and_cache(p, flags.cflags, PG_GET_COLLATION());

	result = replace_text_regexp(s, (void *) re, r, flags.glob);
	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(result), VARSIZE_ANY_EXHDR(result)) == 0)
	{
		pfree(result);
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_TEXT_P(result);
}

/*
 * similar_escape()
 * Convert a SQL:2008 regexp pattern to POSIX style, so it can be used by
 * our regexp engine.
 */
Datum
similar_escape(PG_FUNCTION_ARGS)
{
	text	   *pat_text;
	text	   *esc_text;
	text	   *result;
	char	   *p,
			   *e,
			   *r;
	int			plen,
				elen;
	bool		afterescape = false;
	bool		incharclass = false;
	int			nquotes = 0;

	/* This function is not strict, so must test explicitly */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	pat_text = PG_GETARG_TEXT_PP(0);
	p = VARDATA_ANY(pat_text);
	plen = VARSIZE_ANY_EXHDR(pat_text);
	if (PG_ARGISNULL(1))
	{
		/* No ESCAPE clause provided; default to backslash as escape */
		e = "\\";
		elen = 1;
	}
	else
	{
		esc_text = PG_GETARG_TEXT_PP(1);
		e = VARDATA_ANY(esc_text);
		elen = VARSIZE_ANY_EXHDR(esc_text);
		if (elen == 0)
			e = NULL;			/* no escape character */
		else
		{
			int			escape_mblen = pg_mbstrlen_with_len(e, elen);

			if (escape_mblen > 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
						 errmsg("invalid escape string"),
						 errhint("Escape string must be empty or one character.")));
		}
	}

	/*----------
	 * We surround the transformed input string with
	 *			^(?: ... )$
	 * which requires some explanation.  We need "^" and "$" to force
	 * the pattern to match the entire input string as per SQL99 spec.
	 * The "(?:" and ")" are a non-capturing set of parens; we have to have
	 * parens in case the string contains "|", else the "^" and "$" will
	 * be bound into the first and last alternatives which is not what we
	 * want, and the parens must be non capturing because we don't want them
	 * to count when selecting output for SUBSTRING.
	 *----------
	 */

	/*
	 * We need room for the prefix/postfix plus as many as 3 output bytes per
	 * input byte; since the input is at most 1GB this can't overflow
	 */
	result = (text *) palloc(VARHDRSZ + 6 + 3 * plen);
	r = VARDATA(result);

	*r++ = '^';
	*r++ = '(';
	*r++ = '?';
	*r++ = ':';

	while (plen > 0)
	{
		char		pchar = *p;

		/*
		 * If both the escape character and the current character from the
		 * pattern are multi-byte, we need to take the slow path.
		 *
		 * But if one of them is single-byte, we can process the pattern one
		 * byte at a time, ignoring multi-byte characters.  (This works
		 * because all server-encodings have the property that a valid
		 * multi-byte character representation cannot contain the
		 * representation of a valid single-byte character.)
		 */

		if (elen > 1)
		{
			int			mblen = pg_mblen(p);

			if (mblen > 1)
			{
				/* slow, multi-byte path */
				if (afterescape)
				{
					*r++ = '\\';
					memcpy(r, p, mblen);
					r += mblen;
					afterescape = false;
				}
				else if (e && elen == mblen && memcmp(e, p, mblen) == 0)
				{
					/* SQL99 escape character; do not send to output */
					afterescape = true;
				}
				else
				{
					/*
					 * We know it's a multi-byte character, so we don't need
					 * to do all the comparisons to single-byte characters
					 * that we do below.
					 */
					memcpy(r, p, mblen);
					r += mblen;
				}

				p += mblen;
				plen -= mblen;

				continue;
			}
		}

		/* fast path */
		if (afterescape)
		{
			if (pchar == '"' && !incharclass)	/* for SUBSTRING patterns */
				*r++ = ((nquotes++ % 2) == 0) ? '(' : ')';
			else
			{
				*r++ = '\\';
				*r++ = pchar;
			}
			afterescape = false;
		}
		else if (e && pchar == *e)
		{
			/* SQL99 escape character; do not send to output */
			afterescape = true;
		}
		else if (incharclass)
		{
			if (pchar == '\\')
				*r++ = '\\';
			*r++ = pchar;
			if (pchar == ']')
				incharclass = false;
		}
		else if (pchar == '[')
		{
			*r++ = pchar;
			incharclass = true;
		}
		else if (pchar == '%')
		{
			*r++ = '.';
			*r++ = '*';
		}
		else if (pchar == '_')
			*r++ = '.';
		else if (pchar == '(')
		{
			/* convert to non-capturing parenthesis */
			*r++ = '(';
			*r++ = '?';
			*r++ = ':';
		}
		else if (pchar == '\\' || pchar == '.' ||
				 pchar == '^' || pchar == '$')
		{
			*r++ = '\\';
			*r++ = pchar;
		}
		else
			*r++ = pchar;
		p++, plen--;
	}

	*r++ = ')';
	*r++ = '$';

	SET_VARSIZE(result, r - ((char *) result));

	PG_RETURN_TEXT_P(result);
}

/*
 * regexp_match()
 *		Return the first substring(s) matching a pattern within a string.
 */
Datum
regexp_match(PG_FUNCTION_ARGS)
{
	text	   *orig_str = PG_GETARG_TEXT_PP(0);
	text	   *pattern = PG_GETARG_TEXT_PP(1);
	text	   *flags = PG_GETARG_TEXT_PP_IF_EXISTS(2);
	pg_re_flags re_flags;
	regexp_matches_ctx *matchctx;

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		/* translator: %s is a SQL function name */
				 errmsg("%s does not support the \"global\" option",
						"regexp_match()"),
				 errhint("Use the regexp_matches function instead.")));

	matchctx = setup_regexp_matches(orig_str, pattern, &re_flags, 0,
									PG_GET_COLLATION(), true, false, false);

	if (matchctx->nmatches == 0)
		PG_RETURN_NULL();

	Assert(matchctx->nmatches == 1);

	/* Create workspace that build_regexp_match_result needs */
	matchctx->elems = (Datum *) palloc(sizeof(Datum) * matchctx->npatterns);
	matchctx->nulls = (bool *) palloc(sizeof(bool) * matchctx->npatterns);

	PG_RETURN_DATUM(PointerGetDatum(build_regexp_match_result(matchctx)));
}

/* This is separate to keep the opr_sanity regression test from complaining */
Datum
regexp_match_no_flags(PG_FUNCTION_ARGS)
{
	return regexp_match(fcinfo);
}

/*
 * regexp_matches()
 *		Return a table of all matches of a pattern within a string.
 */
Datum
regexp_matches(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	regexp_matches_ctx *matchctx;

	if (SRF_IS_FIRSTCALL())
	{
		text	   *pattern = PG_GETARG_TEXT_PP(1);
		text	   *flags = PG_GETARG_TEXT_PP_IF_EXISTS(2);
		pg_re_flags re_flags;
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Determine options */
		parse_re_flags(&re_flags, flags);

		/* be sure to copy the input string into the multi-call ctx */
		matchctx = setup_regexp_matches(PG_GETARG_TEXT_P_COPY(0), pattern,
										&re_flags, 0,
										PG_GET_COLLATION(),
										true, false, false);

		/* Pre-create workspace that build_regexp_match_result needs */
		matchctx->elems = (Datum *) palloc(sizeof(Datum) * matchctx->npatterns);
		matchctx->nulls = (bool *) palloc(sizeof(bool) * matchctx->npatterns);

		MemoryContextSwitchTo(oldcontext);
		funcctx->user_fctx = (void *) matchctx;
	}

	funcctx = SRF_PERCALL_SETUP();
	matchctx = (regexp_matches_ctx *) funcctx->user_fctx;

	if (matchctx->next_match < matchctx->nmatches)
	{
		ArrayType  *result_ary;

		result_ary = build_regexp_match_result(matchctx);
		matchctx->next_match++;
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(result_ary));
	}

	SRF_RETURN_DONE(funcctx);
}

/* This is separate to keep the opr_sanity regression test from complaining */
Datum
regexp_matches_no_flags(PG_FUNCTION_ARGS)
{
	return regexp_matches(fcinfo);
}

/*
 * setup_regexp_matches --- do the initial matching for regexp_match,
 *		regexp_split, and related functions
 *
 * To avoid having to re-find the compiled pattern on each call, we do
 * all the matching in one swoop.  The returned regexp_matches_ctx contains
 * the locations of all the substrings matching the pattern.
 *
 * The three bool parameters have only two patterns (one for matching, one for
 * splitting) but it seems clearer to distinguish the functionality this way
 * than to key it all off one "is_split" flag. We don't currently assume that
 * fetching_unmatched is exclusive of fetching the matched text too; if it's
 * set, the conversion buffer is large enough to fetch any single matched or
 * unmatched string, but not any larger substring. (In practice, when splitting
 * the matches are usually small anyway, and it didn't seem worth complicating
 * the code further.)
 * start_search: the character (not byte) offset in orig_str at which to
 * begin the search.  Returned positions are relative to orig_str anyway.
 * use_subpatterns: collect data about matches to parenthesized subexpressions.
 * ignore_degenerate: ignore zero-length matches.
 * fetching_unmatched: caller wants to fetch unmatched substrings.
 *
 * We don't currently assume that fetching_unmatched is exclusive of fetching
 * the matched text too; if it's set, the conversion buffer is large enough to
 * fetch any single matched or unmatched string, but not any larger
 * substring.  (In practice, when splitting the matches are usually small
 * anyway, and it didn't seem worth complicating the code further.)
 */
static regexp_matches_ctx *
setup_regexp_matches(text *orig_str, text *pattern, pg_re_flags *re_flags,
					 int start_search,
					 Oid collation,
					 bool use_subpatterns,
					 bool ignore_degenerate,
					 bool fetching_unmatched)
{
	regexp_matches_ctx *matchctx = palloc0(sizeof(regexp_matches_ctx));
	int			eml = pg_database_encoding_max_length();
	int			orig_len;
	pg_wchar   *wide_str;
	int			wide_len;
	regex_t    *cpattern;
	regmatch_t *pmatch;
	int			pmatch_len;
	int			array_len;
	int			array_idx;
	int			prev_match_end;
	int			prev_valid_match_end;
	int			maxlen = 0;		/* largest fetch length in characters */

	/* save original string --- we'll extract result substrings from it */
	matchctx->orig_str = orig_str;

	/* convert string to pg_wchar form for matching */
	orig_len = VARSIZE_ANY_EXHDR(orig_str);
	wide_str = (pg_wchar *) palloc(sizeof(pg_wchar) * (orig_len + 1));
	wide_len = pg_mb2wchar_with_len(VARDATA_ANY(orig_str), wide_str, orig_len);

	/* set up the compiled pattern */
	cpattern = RE_compile_and_cache(pattern, re_flags->cflags, collation);

	/* do we want to remember subpatterns? */
	if (use_subpatterns && cpattern->re_nsub > 0)
	{
		matchctx->npatterns = cpattern->re_nsub;
		pmatch_len = cpattern->re_nsub + 1;
	}
	else
	{
		use_subpatterns = false;
		matchctx->npatterns = 1;
		pmatch_len = 1;
	}

	/* temporary output space for RE package */
	pmatch = palloc(sizeof(regmatch_t) * pmatch_len);

	/*
	 * the real output space (grown dynamically if needed)
	 *
	 * use values 2^n-1, not 2^n, so that we hit the limit at 2^28-1 rather
	 * than at 2^27
	 */
	array_len = re_flags->glob ? 255 : 31;
	matchctx->match_locs = (int *) palloc(sizeof(int) * array_len);
	array_idx = 0;

	/* search for the pattern, perhaps repeatedly */
	prev_match_end = 0;
	prev_valid_match_end = 0;
	while (RE_wchar_execute(cpattern, wide_str, wide_len, start_search,
							pmatch_len, pmatch))
	{
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
			while (array_idx + matchctx->npatterns * 2 + 1 > array_len)
			{
				array_len += array_len + 1;		/* 2^n-1 => 2^(n+1)-1 */
				if (array_len > MaxAllocSize/sizeof(int))
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("too many regular expression matches")));
				matchctx->match_locs = (int *) repalloc(matchctx->match_locs,
														sizeof(int) * array_len);
			}

			/* save this match's locations */
			if (use_subpatterns)
			{
				int			i;

				for (i = 1; i <= matchctx->npatterns; i++)
				{
					int		so = pmatch[i].rm_so;
					int		eo = pmatch[i].rm_eo;
					matchctx->match_locs[array_idx++] = so;
					matchctx->match_locs[array_idx++] = eo;
					if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
						maxlen = (eo - so);
				}
			}
			else
			{
				int		so = pmatch[0].rm_so;
				int		eo = pmatch[0].rm_eo;
				matchctx->match_locs[array_idx++] = so;
				matchctx->match_locs[array_idx++] = eo;
				if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
					maxlen = (eo - so);
			}
			matchctx->nmatches++;
			
			/*
			 * check length of unmatched portion between end of previous valid
			 * (nondegenerate, or degenerate but not ignored) match and start
			 * of current one
			 */
			if (fetching_unmatched &&
				pmatch[0].rm_so >= 0 &&
				(pmatch[0].rm_so - prev_valid_match_end) > maxlen)
				maxlen = (pmatch[0].rm_so - prev_valid_match_end);
			prev_valid_match_end = pmatch[0].rm_eo;
		}
		prev_match_end = pmatch[0].rm_eo;

		/* if not glob, stop after one match */
		if (!re_flags->glob)
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

	/*
	 * check length of unmatched portion between end of last match and end of
	 * input string
	 */
	if (fetching_unmatched &&
		(wide_len - prev_valid_match_end) > maxlen)
		maxlen = (wide_len - prev_valid_match_end);

	/*
	 * Keep a note of the end position of the string for the benefit of
	 * splitting code.
	 */
	matchctx->match_locs[array_idx] = wide_len;

	if (eml > 1)
	{
		int64		maxsiz = eml * (int64) maxlen;
		int			conv_bufsiz;

		/*
		 * Make the conversion buffer large enough for any substring of
		 * interest.
		 *
		 * Worst case: assume we need the maximum size (maxlen*eml), but take
		 * advantage of the fact that the original string length in bytes is an
		 * upper bound on the byte length of any fetched substring (and we know
		 * that len+1 is safe to allocate because the varlena header is longer
		 * than 1 byte).
		 */
		if (maxsiz > orig_len)
			conv_bufsiz = orig_len + 1;
		else
			conv_bufsiz = maxsiz + 1;	/* safe since maxsiz < 2^30 */

		matchctx->conv_buf = palloc(conv_bufsiz);
		matchctx->conv_bufsiz = conv_bufsiz;
		matchctx->wide_str = wide_str;
	}
	else
	{
		/* No need to keep the wide string if we're in a single-byte charset. */
		pfree(wide_str);
		matchctx->wide_str = NULL;
		matchctx->conv_buf = NULL;
		matchctx->conv_bufsiz = 0;
	}

	/* Clean up temp storage */
	pfree(pmatch);

	return matchctx;
}

#ifdef _PG_ORCL_
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
#endif




/*
 * build_regexp_match_result - build output array for current match
 */
static ArrayType *
build_regexp_match_result(regexp_matches_ctx *matchctx)
{
	char	   *buf = matchctx->conv_buf;
	Datum	   *elems = matchctx->elems;
	bool	   *nulls = matchctx->nulls;
	int			dims[1];
	int			lbs[1];
	int			loc;
	int			i;

	/* Extract matching substrings from the original string */
	loc = matchctx->next_match * matchctx->npatterns * 2;
	for (i = 0; i < matchctx->npatterns; i++)
	{
		int			so = matchctx->match_locs[loc++];
		int			eo = matchctx->match_locs[loc++];

		if (so < 0 || eo < 0)
		{
			elems[i] = (Datum) 0;
			nulls[i] = true;
		}
		else if (buf)
		{
			int		len = pg_wchar2mb_with_len(matchctx->wide_str + so,
											   buf,
											   eo - so);
			Assert(len < matchctx->conv_bufsiz);
			elems[i] = PointerGetDatum(cstring_to_text_with_len(buf, len));
			nulls[i] = false;
		}
		else
		{
			elems[i] = DirectFunctionCall3(text_substr,
										   PointerGetDatum(matchctx->orig_str),
										   Int32GetDatum(so + 1),
										   Int32GetDatum(eo - so));
			nulls[i] = false;
		}
	}

	/* And form an array */
	dims[0] = matchctx->npatterns;
	lbs[0] = 1;
	/* XXX: this hardcodes assumptions about the text type */
	return construct_md_array(elems, nulls, 1, dims, lbs,
							  TEXTOID, -1, false, 'i');
}

/*
 * regexp_split_to_table()
 *		Split the string at matches of the pattern, returning the
 *		split-out substrings as a table.
 */
Datum
regexp_split_to_table(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	regexp_matches_ctx *splitctx;

	if (SRF_IS_FIRSTCALL())
	{
		text	   *pattern = PG_GETARG_TEXT_PP(1);
		text	   *flags = PG_GETARG_TEXT_PP_IF_EXISTS(2);
		pg_re_flags re_flags;
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Determine options */
		parse_re_flags(&re_flags, flags);
		/* User mustn't specify 'g' */
		if (re_flags.glob)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			/* translator: %s is a SQL function name */
					 errmsg("%s does not support the \"global\" option",
							"regexp_split_to_table()")));
		/* But we find all the matches anyway */
		re_flags.glob = true;

		/* be sure to copy the input string into the multi-call ctx */
		splitctx = setup_regexp_matches(PG_GETARG_TEXT_P_COPY(0), pattern,
										&re_flags, 0,
										PG_GET_COLLATION(),
										false, true, true);

		MemoryContextSwitchTo(oldcontext);
		funcctx->user_fctx = (void *) splitctx;
	}

	funcctx = SRF_PERCALL_SETUP();
	splitctx = (regexp_matches_ctx *) funcctx->user_fctx;

	if (splitctx->next_match <= splitctx->nmatches)
	{
		Datum		result = build_regexp_split_result(splitctx);

		splitctx->next_match++;
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/* This is separate to keep the opr_sanity regression test from complaining */
Datum
regexp_split_to_table_no_flags(PG_FUNCTION_ARGS)
{
	return regexp_split_to_table(fcinfo);
}

/*
 * regexp_split_to_array()
 *		Split the string at matches of the pattern, returning the
 *		split-out substrings as an array.
 */
Datum
regexp_split_to_array(PG_FUNCTION_ARGS)
{
	ArrayBuildState *astate = NULL;
	pg_re_flags re_flags;
	regexp_matches_ctx *splitctx;

	/* Determine options */
	parse_re_flags(&re_flags, PG_GETARG_TEXT_PP_IF_EXISTS(2));
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		/* translator: %s is a SQL function name */
				 errmsg("%s does not support the \"global\" option",
						"regexp_split_to_array()")));
	/* But we find all the matches anyway */
	re_flags.glob = true;

	splitctx = setup_regexp_matches(PG_GETARG_TEXT_PP(0),
									PG_GETARG_TEXT_PP(1),
									&re_flags, 0,
									PG_GET_COLLATION(),
									false, true, true);

	while (splitctx->next_match <= splitctx->nmatches)
	{
		astate = accumArrayResult(astate,
								  build_regexp_split_result(splitctx),
								  false,
								  TEXTOID,
								  CurrentMemoryContext);
		splitctx->next_match++;
	}

	PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
}

/* This is separate to keep the opr_sanity regression test from complaining */
Datum
regexp_split_to_array_no_flags(PG_FUNCTION_ARGS)
{
	return regexp_split_to_array(fcinfo);
}

/*
 * build_regexp_split_result - build output string for current match
 *
 * We return the string between the current match and the previous one,
 * or the string after the last match when next_match == nmatches.
 */
static Datum
build_regexp_split_result(regexp_matches_ctx *splitctx)
{
	char	   *buf = splitctx->conv_buf;
	int			startpos;
	int			endpos;

	if (splitctx->next_match > 0)
		startpos = splitctx->match_locs[splitctx->next_match * 2 - 1];
	else
		startpos = 0;
	if (startpos < 0)
		elog(ERROR, "invalid match ending position");

	endpos = splitctx->match_locs[splitctx->next_match * 2];
	if (endpos < startpos)
		elog(ERROR, "invalid match starting position");

	if (buf)
	{
		int		len;

		len = pg_wchar2mb_with_len(splitctx->wide_str + startpos,
								   buf,
								   endpos-startpos);
		Assert(len < splitctx->conv_bufsiz);
		return PointerGetDatum(cstring_to_text_with_len(buf, len));
	}
	else
	{
		return DirectFunctionCall3(text_substr,
								   PointerGetDatum(splitctx->orig_str),
								   Int32GetDatum(startpos + 1),
								   Int32GetDatum(endpos - startpos));
	}
}

/*
 * regexp_substr()
 *		Return the substring that matches a regular expression pattern
 */
Datum
regexp_substr(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);
	text	   *pattern = PG_GETARG_TEXT_PP(1);
	int			start = 1;
	int			n = 1;
	text	   *flags = PG_GETARG_TEXT_PP_IF_EXISTS(4);
	int			subexpr = 0;
	int			so,
				eo,
				pos;
	pg_re_flags re_flags;
	regexp_matches_ctx *matchctx;

	/* Collect optional parameters */
	if (PG_NARGS() > 2)
	{
		start = PG_GETARG_INT32(2);
		if (start <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"start", start)));
	}
	if (PG_NARGS() > 3)
	{
		n = PG_GETARG_INT32(3);
		if (n <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"n", n)));
	}
	if (PG_NARGS() > 5)
	{
		subexpr = PG_GETARG_INT32(5);
		if (subexpr < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value for parameter \"%s\": %d",
							"subexpr", subexpr)));
	}

	/* Determine options */
	parse_re_flags(&re_flags, flags);
	/* User mustn't specify 'g' */
	if (re_flags.glob)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		/* translator: %s is a SQL function name */
				 errmsg("%s does not support the \"global\" option",
						"regexp_substr()")));
	/* But we find all the matches anyway */
	re_flags.glob = true;

	/* Do the matching */
	matchctx = setup_regexp_matches(str, pattern, &re_flags, start - 1,
									PG_GET_COLLATION(),
									(subexpr > 0),	/* need submatches? */
									false, false);

	/* When n exceeds matches return NULL (includes case of no matches) */
	if (n > matchctx->nmatches)
		PG_RETURN_NULL();

	/* When subexpr exceeds number of subexpressions return NULL */
	if (subexpr > matchctx->npatterns)
		PG_RETURN_NULL();

	/* Select the appropriate match position to return */
	pos = (n - 1) * matchctx->npatterns;
	if (subexpr > 0)
		pos += subexpr - 1;
	pos *= 2;
	so = matchctx->match_locs[pos];
	eo = matchctx->match_locs[pos + 1];

	if (so < 0 || eo < 0)
		PG_RETURN_NULL();		/* unidentifiable location */

	PG_RETURN_DATUM(DirectFunctionCall3(text_substr,
										PointerGetDatum(matchctx->orig_str),
										Int32GetDatum(so + 1),
										Int32GetDatum(eo - so)));
}

/*
 * regexp_fixed_prefix - extract fixed prefix, if any, for a regexp
 *
 * The result is NULL if there is no fixed prefix, else a palloc'd string.
 * If it is an exact match, not just a prefix, *exact is returned as TRUE.
 */
char *
regexp_fixed_prefix(text *text_re, bool case_insensitive, Oid collation,
					bool *exact)
{
	char	   *result;
	regex_t    *re;
	int			cflags;
	int			re_result;
	pg_wchar   *str;
	size_t		slen;
	size_t		maxlen;
	char		errMsg[100];

	*exact = false;				/* default result */

	/* Compile RE */
	cflags = REG_ADVANCED;
	if (case_insensitive)
		cflags |= REG_ICASE;

	re = RE_compile_and_cache(text_re, cflags, collation);

	/* Examine it to see if there's a fixed prefix */
	re_result = pg_regprefix(re, &str, &slen);

	switch (re_result)
	{
		case REG_NOMATCH:
			return NULL;

		case REG_PREFIX:
			/* continue with wchar conversion */
			break;

		case REG_EXACT:
			*exact = true;
			/* continue with wchar conversion */
			break;

		default:
			/* re failed??? */
			CHECK_FOR_INTERRUPTS();
			pg_regerror(re_result, re, errMsg, sizeof(errMsg));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("regular expression failed: %s", errMsg)));
			break;
	}

	/* Convert pg_wchar result back to database encoding */
	maxlen = pg_database_encoding_max_length() * slen + 1;
	result = (char *) palloc(maxlen);
	slen = pg_wchar2mb_with_len(str, result, slen);
	Assert(slen < maxlen);

	free(str);

	return result;
}

#ifdef _PG_ORCL_
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

/*
 * REGEXP_COUNT (source_char, pattern [, position [, match_param]])
 */
Datum
orcl_regexp_count(PG_FUNCTION_ARGS)
{
	regexp_matches_ctx *countctx;
	int position = PG_GETARG_INT32_1_IF_NULL(2);
	text *s = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *p = PG_GETARG_TEXT_PP_IF_NULL(1);
	Oid collid = DEFAULT_COLLATION_OID;
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
	 * convert "position" to "source_char" offset
	 */
	if (position <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"position\" value (%d) for \"regexp_count\"",
					position),
				errhint("\"position\" must be a positive integer")));
	position = position - 1;

	/* get collation oid */
	collid = PG_GET_COLLATION();
	if (!OidIsValid(collid))
	{
		elog(DEBUG2, "get collation oid is %d", collid);

		/* use default collation oid */
		collid = DEFAULT_COLLATION_OID;
	}

	countctx = setup_regexp_matches_opentenbase_ora(s, position,
									p,
									PG_GETARG_TEXT_PP_IF_NULL(3),
									orcl_parse_re_flags,
									collid,
									true, false, true, true);

	PG_RETURN_INT32(countctx->nmatches);
}

/*
 * REGEXP_COUNT (source_char, pattern)
 */
Datum
orcl_regexp_count_2(PG_FUNCTION_ARGS)
{
	return orcl_regexp_count(fcinfo);
}

/*
 * REGEXP_COUNT (source_char, pattern, position)
 */
Datum
orcl_regexp_count_3(PG_FUNCTION_ARGS)
{
	return orcl_regexp_count(fcinfo);
}

/*
 * REGEXP_REPLACE(source_char, pattern
 *				  [, replace_string
 *					 [, position
 *						[, occurrence
 *						   [, match_param ]
 *						]
 *					 ]
 *				  ]
 *				 )
 * if you omit "replace_string", then the default value is empty string.
 * if you omit "position", then the default value is 0, means begin the match
 * at the first character of "source_char".
 * if you omit "occurrence", then the default value is 0, means replace all
 * occurrences of the match.
 */
Datum
orcl_regexp_replace(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP_IF_NULL(0);
	text	   *p = PG_GETARG_TEXT_PP_IF_NULL(1);
	text	   *r = PG_GETARG_TEXT_PP_IF_NULL(2);
	int 		position = PG_GETARG_INT32_1_IF_NULL(3);
	int 		occurence = PG_GETARG_INT32_0_IF_NULL(4);
	text	   *opt = PG_GETARG_TEXT_PP_IF_NULL(5);
	Oid 		collid = DEFAULT_COLLATION_OID;
	regex_t    *re;
	pg_re_flags flags;
    text* result = NULL;

	/*
	 * return null if source_char is null or empty
	 */
	if (s == NULL || text_to_cstring(s)[0] == 0x00)
		PG_RETURN_NULL();

	if (PG_NARGS() >= 5)
	{
		/* return null if position or occurence is null */
		if (PG_ARGISNULL(3) || PG_ARGISNULL(4))
			PG_RETURN_NULL();
	}

	/*
	 * return source_char if pattern is null or empty
	 */
	if (p == NULL || text_to_cstring(p)[0] == 0x00)
		PG_RETURN_TEXT_P(s);

	/*
	 * replace_string can be empty
	 */
	if (r == NULL)
		r = cstring_to_text("");

	/*
	 * convert "position" to "source_char" offset
	 */
	if (position <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"position\" value (%d) for \"regexp_replace\"",
					position),
				errhint("\"position\" must be a positive integer")));
	position = position - 1;

	/*
	 * make sure occurence be a nonnegative integer
	 */
	if (occurence < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"occurence\" value (%d) for \"regexp_replace\"",
					occurence),
				errhint("\"occurence\" must be a nonnegative integer")));

	orcl_parse_re_flags(&flags, opt);

	/* must be true */
	flags.glob = true;

	/* get collation oid */
	collid = PG_GET_COLLATION();
	if (!OidIsValid(collid))
	{
		elog(DEBUG2, "get collation oid is %d", collid);

		/* use default collation oid */
		collid = DEFAULT_COLLATION_OID;
	}

	re = RE_compile_and_cache(p, flags.cflags, collid);

	result = replace_text_regexp_opentenbase_ora(s, (void *) re, r, position, occurence, flags.glob);

	if ((ORA_MODE || enable_lightweight_ora_syntax) &&
	    pg_mbstrlen_with_len(VARDATA_ANY(result), VARSIZE_ANY_EXHDR(result)) == 0)
		PG_RETURN_NULL();
	else
		PG_RETURN_TEXT_P(result);
}

/*
 * REGEXP_REPLACE(source_char, pattern)
 */
Datum
orcl_regexp_replace_2(PG_FUNCTION_ARGS)
{
	return orcl_regexp_replace(fcinfo);
}

/*
 * REGEXP_REPLACE(source_char, pattern, replace_string)
 */
Datum
orcl_regexp_replace_3(PG_FUNCTION_ARGS)
{
	return orcl_regexp_replace(fcinfo);
}

/*
 * REGEXP_REPLACE(source_char, pattern, replace_string, position)
 */
Datum
orcl_regexp_replace_4(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(3))
		PG_RETURN_NULL();

	return orcl_regexp_replace(fcinfo);
}

/*
 * REGEXP_REPLACE(source_char, pattern, replace_string, position, occurrence)
 */
Datum
orcl_regexp_replace_5(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(3) || PG_ARGISNULL(4))
		PG_RETURN_NULL();

	return orcl_regexp_replace(fcinfo);
}

static Datum
build_orcl_regexp_substr_result(regexp_matches_ctx *substrctx, int subexpr)
{
	int loc;
	int so, eo;

	Assert(substrctx);
	Assert(subexpr >= 0 && subexpr <= substrctx->npatterns);

	loc = substrctx->next_match * (substrctx->npatterns + 1) * 2;
	loc += 2 * subexpr;

	so = substrctx->match_locs[loc];
	eo = substrctx->match_locs[loc + 1];

	return DirectFunctionCall3(text_substr,
							   PointerGetDatum(substrctx->orig_str),
							   Int32GetDatum(so + 1),
							   Int32GetDatum(eo - so));
}

/*
 * REGEXP_SUBSTR(source_char, pattern
 *				 [, position
 *					[, occurrence
 *					   [, match_param
 *						  [, subexpr
 *						  ]
 *					   ]
 *					]
 *				 ]
 *				)
 *
 * Note: position is a positive integer indicating the character of source_char
 * where opentenbase_ora should begin the search. The default is 0, meaning that we
 * begin the search at the first character of source_char.
 *
 * Note: occurrence is a positive integer indicating which occurrence of pattern
 * in source_char we should search for. The default is 1, meaning that we search
 * for the first occurrence of pattern.
 *
 * Note: For a pattern with subexpressions, subexpr is a nonnegative integer
 * from 0 to 9 indicating which subexpression in pattern is to be returned by
 * the function.
 */
Datum
orcl_regexp_substr(PG_FUNCTION_ARGS)
{
	text	*s = PG_GETARG_TEXT_PP_IF_NULL(0);
	text	*p = PG_GETARG_TEXT_PP_IF_NULL(1);
	int64 	 position;
	int64 	 occurence;
	text	*flags = PG_GETARG_TEXT_PP_IF_NULL(4);
	int64 	 subexpr;
	Oid 	 collid = DEFAULT_COLLATION_OID;
	regexp_matches_ctx *substrctx;

	if (PG_MODE && enable_lightweight_ora_syntax)
		return regexp_substr(fcinfo);
	/*
	 * return null if sourch_char is null or empty
	 */
	if (s == NULL || text_to_cstring(s)[0] == 0x00)
		PG_RETURN_NULL();

	/*
	 * return null if pattern is null or empty
	 */
	if (p == NULL || text_to_cstring(p)[0] == 0x00)
		PG_RETURN_NULL();

	/*
	 * The arg2, arg3 and arg5 type is int64 in opentenbase_ora mode while
	 * it is int32 in postgresql mode.
	 *
	 * Notice: we can't use INT64 for opentenbase_ora mode and postgresql mode
	 * because it will cause core while the arguments are negative.
	 */
	if (ORA_MODE)
	{
		position = PG_GETARG_INT64_1_IF_NULL(2);
		occurence = PG_GETARG_INT64_1_IF_NULL(3);
		subexpr = PG_GETARG_INT64_0_IF_NULL(5);
	}
	else
	{
		position = PG_GETARG_INT32_1_IF_NULL(2);
		occurence = PG_GETARG_INT32_1_IF_NULL(3);
		subexpr = PG_GETARG_INT32_0_IF_NULL(5);
	}

	/*
	 * convert "position" to "source_char" offset
	 *
	 * Notice: we support bigint position in orcl_regexp_substr function
	 * declaration in opentenbase_ora mode but we can't support the position
	 * larger than INT_MAX in opentenbase_ora mode which will cause a core,
	 * so just error report it.
	 */
	if (position <= 0 || position > INT_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"position\" value (%ld) for \"regexp_substr\"",
					position),
				errhint("\"position\" must be a positive integer")));
	position = position - 1;

	/*
	 * make sure "occurence" be a positive integer
	 */
	if (occurence <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"occurence\" value (%ld) for \"regexp_substr\"",
					occurence),
				errhint("\"occurence\" must be a positive integer")));

	/*
	 * make sure "subexpr" be a nonnegative integer from 0 to 9
	 */
	if (subexpr < 0 || subexpr > 9)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"subexpr\" value (%ld) for \"regexp_substr\"",
					subexpr),
				errhint("\"subexpr\" must be a nonnegative integer from 0 to 9")));

	/* get collation oid */
	collid = PG_GET_COLLATION();
	if (!OidIsValid(collid))
	{
		elog(DEBUG2, "get collation oid is %d", collid);

		/* use default collation oid */
		collid = DEFAULT_COLLATION_OID;
	}

	substrctx = setup_regexp_matches_opentenbase_ora(s, position, p,
									 flags, orcl_parse_re_flags,
									 collid,
									 true, true, true, true);

	/*
	 * return null if occurence is greater than substrctx->nmatches
	 */
	if (occurence > substrctx->nmatches)
	{
		if (ORA_MODE)
			PG_RETURN_NULL();
		else
			PG_RETURN_TEXT_P(cstring_to_text(""));
	}

	if (subexpr > substrctx->npatterns)
	{
		if (ORA_MODE)
			PG_RETURN_NULL();
		else
			PG_RETURN_TEXT_P(cstring_to_text(""));
	}

	substrctx->next_match = occurence - 1;

	return build_orcl_regexp_substr_result(substrctx, subexpr);
}

/*
 * REGEXP_SUBSTR(source_char, pattern)
 */
Datum
orcl_regexp_substr_2(PG_FUNCTION_ARGS)
{
	if (PG_MODE && enable_lightweight_ora_syntax)
		return regexp_substr(fcinfo);
	return orcl_regexp_substr(fcinfo);
}

/*
 * REGEXP_SUBSTR(source_char, pattern, position)
 */
Datum
orcl_regexp_substr_3(PG_FUNCTION_ARGS)
{
	if (PG_MODE && enable_lightweight_ora_syntax)
		return regexp_substr(fcinfo);
	return orcl_regexp_substr(fcinfo);
}

/*
 * REGEXP_SUBSTR(source_char, pattern, position, occurrence)
 */
Datum
orcl_regexp_substr_4(PG_FUNCTION_ARGS)
{
	if (PG_MODE && enable_lightweight_ora_syntax)
		return regexp_substr(fcinfo);
	return orcl_regexp_substr(fcinfo);
}

/*
 * REGEXP_SUBSTR(source_char, pattern, position, occurrence, match_param)
 */
Datum
orcl_regexp_substr_5(PG_FUNCTION_ARGS)
{
	if (PG_MODE && enable_lightweight_ora_syntax)
		return regexp_substr(fcinfo);
	return orcl_regexp_substr(fcinfo);
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
 * Note: For a pattern with subexpressions, subexpr is an integer from 0 to 9
 * indicating which subexpression in pattern is the target of the function. The
 * subexpr is a fragment of pattern enclosed in parentheses. Subexpressions can
 * be nested. Subexpressions are numbered in order in which their left
 * parentheses appear in pattern.
 */
Datum
orcl_regexp_instr(PG_FUNCTION_ARGS)
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
	 * make sure "subexpr" be a nonnegative integer from 0 to 9
	 */
	if (subexpr < 0 || subexpr > 9)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("Invalid \"subexpr\" value (%d) for \"regexp_instr\"",
					subexpr),
				errhint("\"subexpr\" must be a nonnegative integer from 0 to 9")));

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
	return orcl_regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position)
 */
Datum
orcl_regexp_instr_3(PG_FUNCTION_ARGS)
{
	return orcl_regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence)
 */
Datum
orcl_regexp_instr_4(PG_FUNCTION_ARGS)
{
	return orcl_regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence, return_opt)
 */
Datum
orcl_regexp_instr_5(PG_FUNCTION_ARGS)
{
	return orcl_regexp_instr(fcinfo);
}

/*
 * REGEXP_INSTR (source_char, pattern, position, occurrence, return_opt, match_param)
 */
Datum
orcl_regexp_instr_6(PG_FUNCTION_ARGS)
{
	return orcl_regexp_instr(fcinfo);
}

/*
 * REGEXP_LIKE(source_char, pattern
 *			   [, match_param]
 *			  )
 */
Datum
orcl_regexp_like(PG_FUNCTION_ARGS)
{
	text	   *s = PG_GETARG_TEXT_PP_IF_NULL(0);
	text	   *p = PG_GETARG_TEXT_PP_IF_NULL(1);
	text	   *f = PG_GETARG_TEXT_PP_IF_NULL(2);
	pg_re_flags flags;

	if (s == NULL || text_to_cstring(s)[0] == 0x00)
		PG_RETURN_BOOL(false);

	if (p == NULL || text_to_cstring(p)[0] == 0x00)
		PG_RETURN_BOOL(false);

	orcl_parse_re_flags(&flags, f);

	PG_RETURN_BOOL(RE_compile_and_execute(p,
										  VARDATA_ANY(s),
										  VARSIZE_ANY_EXHDR(s),
										  flags.cflags,
										  PG_GET_COLLATION(),
										  0, NULL));
}

/*
 * REGEXP_LIKE (source_char, pattern)
 */
Datum
orcl_regexp_like_2(PG_FUNCTION_ARGS)
{
	return orcl_regexp_like(fcinfo);
}

#endif /* _PG_ORCL_ */
