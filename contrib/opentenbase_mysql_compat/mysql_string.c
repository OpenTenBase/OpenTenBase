/*-------------------------------------------------------------------------
 *
 * mysql_string.c
 *		MySQL-compatible string functions for opentenbase_mysql_compat.
 *
 * Implements: CONCAT, CONCAT_WS, FIND_IN_SET, ELT, FIELD, INSERT,
 * SUBSTRING_INDEX, LOCATE, INSTR, REVERSE, SPACE and UNHEX with MySQL
 * NULL semantics and implicit string conversion of non-text arguments.
 *
 * Comparisons follow MySQL's default case-insensitive collation for the
 * ASCII range; multibyte characters are compared byte-exactly.
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/mysql_string.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(mysql_concat);
PG_FUNCTION_INFO_V1(mysql_concat_ws);
PG_FUNCTION_INFO_V1(mysql_find_in_set);
PG_FUNCTION_INFO_V1(mysql_elt);
PG_FUNCTION_INFO_V1(mysql_field);
PG_FUNCTION_INFO_V1(mysql_insert);
PG_FUNCTION_INFO_V1(mysql_substring_index);
PG_FUNCTION_INFO_V1(mysql_locate2);
PG_FUNCTION_INFO_V1(mysql_locate3);
PG_FUNCTION_INFO_V1(mysql_instr);
PG_FUNCTION_INFO_V1(mysql_reverse);
PG_FUNCTION_INFO_V1(mysql_space);
PG_FUNCTION_INFO_V1(mysql_unhex);

extern char *mysql_any_to_cstring(FunctionCallInfo fcinfo, int argno);

static char *mysql_ascii_strdown(char *s);
static int	mysql_hexdigit(char c);

/*
 * Lower-case ASCII letters in place.  pg_tolower() only maps ASCII, so
 * multibyte characters stay byte-identical.
 */
static char *
mysql_ascii_strdown(char *s)
{
	char	   *p;

	for (p = s; *p; p++)
		*p = pg_tolower((unsigned char) *p);
	return s;
}

static int
mysql_hexdigit(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	return -1;
}

/*
 * mysql_concat(variadic "any") -> text
 *
 * MySQL CONCAT() returns NULL if any argument is NULL, and implicitly
 * converts every argument to its string form.
 */
Datum
mysql_concat(PG_FUNCTION_ARGS)
{
	StringInfoData str;
	int			i;

	initStringInfo(&str);

	for (i = 0; i < PG_NARGS(); i++)
	{
		if (PG_ARGISNULL(i))
			PG_RETURN_NULL();

		appendStringInfoString(&str, mysql_any_to_cstring(fcinfo, i));
	}

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/*
 * mysql_concat_ws(sep text, variadic "any") -> text
 *
 * CONCAT WITH SEPARATOR.  A NULL separator makes the whole result NULL,
 * while NULL arguments are skipped.  An empty argument list yields the
 * empty string.
 */
Datum
mysql_concat_ws(PG_FUNCTION_ARGS)
{
	StringInfoData str;
	char	   *sepstr;
	bool		first = true;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	sepstr = text_to_cstring(PG_GETARG_TEXT_PP(0));

	initStringInfo(&str);

	for (i = 1; i < PG_NARGS(); i++)
	{
		if (PG_ARGISNULL(i))
			continue;

		if (!first)
			appendStringInfoString(&str, sepstr);
		first = false;

		appendStringInfoString(&str, mysql_any_to_cstring(fcinfo, i));
	}

	PG_RETURN_TEXT_P(cstring_to_text(str.data));
}

/*
 * mysql_find_in_set(str text, strlist text) -> int
 *
 * Returns the 1-based index of str in the comma-separated strlist,
 * 0 when not found, NULL when either argument is NULL.
 */
Datum
mysql_find_in_set(PG_FUNCTION_ARGS)
{
	char	   *needle,
			   *haystack,
			   *member;
	size_t		needlelen;
	int			idx = 1;
	char	   *scan,
			   *comma;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	needle = text_to_cstring(PG_GETARG_TEXT_PP(0));
	haystack = text_to_cstring(PG_GETARG_TEXT_PP(1));

	/* MySQL: empty needle matches only an empty (sole) list member. */
	if (needle[0] == '\0')
		PG_RETURN_INT32(haystack[0] == '\0' ? 1 : 0);

	/* A needle containing a comma can never equal a list member. */
	needlelen = strlen(needle);
	if (strchr(needle, ',') != NULL)
		PG_RETURN_INT32(0);

	scan = haystack;
	for (;;)
	{
		comma = strchr(scan, ',');
		if (comma != NULL)
			*comma = '\0';
		member = scan;

		if (strlen(member) == needlelen &&
			strncmp(mysql_ascii_strdown(pstrdup(member)),
					mysql_ascii_strdown(needle), needlelen) == 0)
			PG_RETURN_INT32(idx);

		if (comma == NULL)
			break;

		idx++;
		scan = comma + 1;
	}

	PG_RETURN_INT32(0);
}

/*
 * mysql_elt(n int, variadic text) -> text
 *
 * Returns the n-th element of the list, NULL when n is out of range or
 * the selected element is NULL.
 */
Datum
mysql_elt(PG_FUNCTION_ARGS)
{
	int32		n;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	n = PG_GETARG_INT32(0);

	if (n < 1 || n >= PG_NARGS() || PG_ARGISNULL(n))
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(text_to_cstring(PG_GETARG_TEXT_PP(n))));
}

/*
 * mysql_field(needle text, variadic text) -> int
 *
 * Returns the 1-based index of needle in the argument list, 0 when it
 * is absent.  A NULL needle never matches, like in MySQL.
 */
Datum
mysql_field(PG_FUNCTION_ARGS)
{
	char	   *needle;
	char	   *lneedle;
	size_t		needlelen;
	int			i;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT32(0);

	needle = text_to_cstring(PG_GETARG_TEXT_PP(0));
	lneedle = mysql_ascii_strdown(needle);
	needlelen = strlen(needle);

	for (i = 1; i < PG_NARGS(); i++)
	{
		if (!PG_ARGISNULL(i))
		{
			char	   *cand = mysql_ascii_strdown(text_to_cstring(PG_GETARG_TEXT_PP(i)));

			if (strlen(cand) == needlelen &&
				memcmp(cand, lneedle, needlelen) == 0)
				PG_RETURN_INT32(i);
		}
	}

	PG_RETURN_INT32(0);
}

/*
 * mysql_insert(str text, pos int, len int, newstr text) -> text
 *
 * MySQL INSERT(): replace the substring of str that starts at the
 * 1-based character position pos and spans len characters with newstr.
 * When pos is outside the string the original string is returned; a
 * negative len replaces everything from pos to the end.
 */
Datum
mysql_insert(PG_FUNCTION_ARGS)
{
	char	   *orig,
			   *repl;
	int			pos,
				len;
	int			origbytes,
				replbytes;
	int			origchars;
	size_t		prefix_bytes,
				suffix_bytes;
	StringInfoData res;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3))
		PG_RETURN_NULL();

	orig = text_to_cstring(PG_GETARG_TEXT_PP(0));
	pos = PG_GETARG_INT32(1);
	len = PG_GETARG_INT32(2);
	repl = text_to_cstring(PG_GETARG_TEXT_PP(3));
	origbytes = (int) strlen(orig);
	replbytes = (int) strlen(repl);
	origchars = (int) pg_mbstrlen_with_len(orig, origbytes);

	if (pos < 1 || pos > origchars)
		PG_RETURN_TEXT_P(cstring_to_text(orig));

	prefix_bytes = (size_t) pg_mbcharcliplen(orig, origbytes, pos - 1);

	if (len < 0 || len > origchars - pos + 1)
		suffix_bytes = 0;
	else
	{
		int			kept = origchars - pos + 1 - len;

		suffix_bytes = (size_t) pg_mbcharcliplen(orig, origbytes,
												 origchars - kept);
	}

	initStringInfo(&res);
	appendBinaryStringInfo(&res, orig, (int) prefix_bytes);
	appendBinaryStringInfo(&res, repl, replbytes);
	appendBinaryStringInfo(&res, orig + origbytes - (int) suffix_bytes,
						   (int) suffix_bytes);

	PG_RETURN_TEXT_P(cstring_to_text(res.data));
}

/*
 * mysql_substring_index(str text, delim text, count int) -> text
 *
 * Returns the substring of str before (count > 0) or after (count < 0)
 * the count-th occurrence of delim.
 */
Datum
mysql_substring_index(PG_FUNCTION_ARGS)
{
	char	   *s,
			   *d;
	int			count;
	size_t		dlen;
	char	   *p;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	s = text_to_cstring(PG_GETARG_TEXT_PP(0));
	d = text_to_cstring(PG_GETARG_TEXT_PP(1));
	count = PG_GETARG_INT32(2);
	dlen = strlen(d);

	if (count == 0 || dlen == 0)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	if (count > 0)
	{
		int			found = 0;

		p = s;
		while ((p = strstr(p, d)) != NULL)
		{
			if (++found == count)
			{
				*p = '\0';
				break;
			}
			p += dlen;
		}
	}
	else
	{
		/*
		 * Negative count: keep everything after the |count|-th occurrence
		 * counted from the right.
		 */
		int			target = -count;
		int			total = 0;
		int			found = 0;

		for (p = s; (p = strstr(p, d)) != NULL; p += dlen)
			total++;

		if (total < target)
		{
			p = NULL;
			PG_RETURN_TEXT_P(cstring_to_text(s));
		}

		for (p = s; (p = strstr(p, d)) != NULL; p += dlen)
		{
			if (++found == total - target + 1)
			{
				p += dlen;
				break;
			}
		}
		PG_RETURN_TEXT_P(cstring_to_text(p));
	}

	PG_RETURN_TEXT_P(cstring_to_text(s));
}

/*
 * Shared LOCATE implementation: 1-based character position of substr
 * in str starting the scan at the 1-based position from,
 * case-insensitively for ASCII.
 */
static int32
mysql_locate_internal(text *substr, text *str, int from)
{
	char	   *sub,
			   *s;
	size_t		sublen;
	char	   *hit;

	if (from < 1)
		return 0;

	sub = mysql_ascii_strdown(text_to_cstring(substr));
	s = mysql_ascii_strdown(text_to_cstring(str));
	sublen = strlen(sub);

	if (sublen == 0)
		return from;

	if ((size_t) from > strlen(s))
		return 0;

	hit = strstr(s + from - 1, sub);
	if (hit == NULL)
		return 0;

	/* Convert the byte offset into a character offset. */
	return (int32) pg_mbstrlen_with_len(s, (int) (hit - s)) + 1;
}

Datum
mysql_locate2(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	PG_RETURN_INT32(mysql_locate_internal(PG_GETARG_TEXT_PP(0),
										  PG_GETARG_TEXT_PP(1), 1));
}

Datum
mysql_locate3(PG_FUNCTION_ARGS)
{
	int			from;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	from = PG_ARGISNULL(2) ? 1 : PG_GETARG_INT32(2);

	PG_RETURN_INT32(mysql_locate_internal(PG_GETARG_TEXT_PP(0),
										  PG_GETARG_TEXT_PP(1), from));
}

Datum
mysql_instr(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	PG_RETURN_INT32(mysql_locate_internal(PG_GETARG_TEXT_PP(1),
										  PG_GETARG_TEXT_PP(0), 1));
}

/*
 * mysql_reverse(str text) -> text, multibyte aware.
 */
Datum
mysql_reverse(PG_FUNCTION_ARGS)
{
	char	   *s;
	size_t		buflen;
	StringInfoData res;
	char	   *p;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	s = text_to_cstring(PG_GETARG_TEXT_PP(0));
	buflen = strlen(s);

	initStringInfo(&res);
	enlargeStringInfo(&res, (int) buflen);

	p = s + buflen;
	while (p > s)
	{
		char	   *q = p - 1;

		while (q > s && (*q & 0xC0) == 0x80)
			q--;
		appendBinaryStringInfo(&res, q, (int) (p - q));
		p = q;
	}

	PG_RETURN_TEXT_P(cstring_to_text(res.data));
}

/*
 * mysql_space(n int) -> text of n spaces.
 */
Datum
mysql_space(PG_FUNCTION_ARGS)
{
	int32		n;
	char	   *res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	n = PG_GETARG_INT32(0);

	if (n <= 0)
		PG_RETURN_TEXT_P(cstring_to_text(""));

	if (n > 1048576)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("SPACE() length %d exceeds the maximum of 1048576",
						n)));

	res = (char *) palloc(n + 1);
	memset(res, ' ', n);
	res[n] = '\0';

	PG_RETURN_TEXT_P(cstring_to_text(res));
}

/*
 * mysql_unhex(str text) -> bytea
 *
 * Interpret each pair of hex digits as a byte.  Returns NULL for odd
 * input length or invalid digits, following MySQL.
 */
Datum
mysql_unhex(PG_FUNCTION_ARGS)
{
	char	   *s;
	size_t		len,
				i;
	bytea	   *result;
	char	   *rp;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	s = text_to_cstring(PG_GETARG_TEXT_PP(0));
	len = strlen(s);

	if (len % 2 != 0)
		PG_RETURN_NULL();

	result = (bytea *) palloc(VARHDRSZ + len / 2);
	SET_VARSIZE(result, VARHDRSZ + len / 2);
	rp = VARDATA(result);

	for (i = 0; i < len; i += 2)
	{
		int			hi = mysql_hexdigit(s[i]);
		int			lo = mysql_hexdigit(s[i + 1]);

		if (hi < 0 || lo < 0)
			PG_RETURN_NULL();
		*rp++ = (char) ((hi << 4) | lo);
	}

	PG_RETURN_BYTEA_P(result);
}
