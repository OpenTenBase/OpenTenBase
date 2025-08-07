/*-------------------------------------------------------------------------
 *
 * String-processing utility routines for dbms_metadata code
 *
 * Assorted utility functions that are useful in constructing SQL queries
 * and interpreting backend output.
 *
 *dbms_md_string_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include "utils/builtins.h"
#include "mb/pg_wchar.h"
#include "common/keywords.h"
#include "dbms_md_string_utils.h"
#include "dbms_md_dumputils.h"

#define DBMS_MD_VERSION_LEN (32)
#define ISFIRSTOCTDIGIT(CH) ((CH) >= '0' && (CH) <= '3')
#define ISOCTDIGIT(CH) ((CH) >= '0' && (CH) <= '7')
#define OCTVAL(CH) ((CH) - '0')

static StringInfo defaultGetLocalStringBuffer(void);
StringInfo (*getLocalStringBuffer) (void) = defaultGetLocalStringBuffer;

static const int8 dbms_md_hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static inline char
dbms_md_get_hex(char c)
{
	int			res = -1;

	if (c > 0 && c < 127)
		res = dbms_md_hexlookup[(unsigned char) c];

	return (char) res;
}

/*
 * Returns a temporary StringBuffer, valid until the next call to the function.
 * This is used by fmtId and fmtQualifiedId.
 *
 * Non-reentrant and non-thread-safe but reduces memory leakage. You can
 * replace this with a custom version by setting the getLocalStringInfo
 * function pointer.
 */
static StringInfo g_dbms_md_id_return = NULL;
static StringInfo
defaultGetLocalStringBuffer(void)
{
	StringInfo id_return = g_dbms_md_id_return;

	if (id_return)				/* first time through? */
	{
		/* same buffer, just wipe contents */
		resetStringInfo(id_return);
	}
	else
	{
		/* new buffer */
		id_return = createStringInfo();
	}

	return id_return;
}

void SetLocalStringBuffer(StringInfo buf)
{
	g_dbms_md_id_return = buf;
}

Oid dbms_md_str_to_oid(const char* str)
{
	if (NULL == str)
		return InvalidOid;

	return atooid(str);
}

/*
 *	Quotes input string if it's not a legitimate SQL identifier as-is.
 *
 *	Note that the returned string must be used before calling fmtId again,
 *	since we re-use the same return buffer each time.
 */

const char *
dbms_md_fmtId_internal(const char *rawid, bool force_pg_mode)
{
	StringInfo id_return = NULL;	
	const char *cp;
	bool		need_quotes = false;
	char startc = (ORA_MODE && !force_pg_mode) ? 'A' : 'a';
	char endc = (ORA_MODE && !force_pg_mode) ? 'Z' : 'z';

	id_return = getLocalStringBuffer();

	/*
	 * These checks need to match the identifier production in scan.l. Don't
	 * use islower() etc.
	 */
	if (quote_all_identifiers)
		need_quotes = true;
	/* slightly different rules for first character */
	else if (!((rawid[0] >= startc && rawid[0] <= endc) || rawid[0] == '_'))
		need_quotes = true;
	else
	{
		/* otherwise check the entire string */
		for (cp = rawid; *cp; cp++)
		{
			if (!((*cp >= startc && *cp <= endc)
				  || (*cp >= '0' && *cp <= '9')
				  || (*cp == '_')))
			{
				need_quotes = true;
				break;
			}
		}
	}

	if (!need_quotes)
	{
		/*
		 * Check for keyword.  We quote keywords except for unreserved ones.
		 * (In some cases we could avoid quoting a col_name or type_func_name
		 * keyword, but it seems much harder than it's worth to tell that.)
		 *
		 * Note: ScanKeywordLookup() does case-insensitive comparison, but
		 * that's fine, since we already know we have all-lower-case.
		 */
		const ScanKeyword *keyword = ScanKeywordLookup(rawid,
													   ScanKeywords,
													   NumScanKeywords);

		if (keyword != NULL && keyword->category != UNRESERVED_KEYWORD)
			need_quotes = true;
	}

	//id_return = getLocalStringBuffer();
	resetStringInfo(id_return);		

	if (!need_quotes)
	{
		/* no quoting needed */
		appendStringInfoString(id_return, rawid);
	}
	else
	{
		appendStringInfoChar(id_return, '"');
		for (cp = rawid; *cp; cp++)
		{
			/*
			 * Did we find a double-quote in the string? Then make this a
			 * double double-quote per SQL99. Before, we put in a
			 * backslash/double-quote pair. - thomas 2000-08-05
			 */
			if (*cp == '"')
				appendStringInfoChar(id_return, '"');
			appendStringInfoChar(id_return, *cp);
		}
		appendStringInfoChar(id_return, '"');
	}

	return id_return->data;
}
/*
 * fmtQualifiedId - convert a qualified name to the proper format for
 * the source database.
 *
 * Like fmtId, use the result before calling again.
 *
 * Since we call fmtId and it also uses getLocalStringInfo() we cannot
 * use that buffer until we're finished with calling fmtId().
 */	
const char *
dbms_md_fmt_qualified_id(int remoteVersion, const char *schema, const char *id)
{
	StringInfo id_return = NULL;
	StringInfo lcl_pqexp = createStringInfo();

	/* Suppress schema name if fetching from pre-7.3 DB */
	if (remoteVersion >= 70300 && schema && *schema)
	{
		appendStringInfo(lcl_pqexp, "%s.", dbms_md_fmtId(schema));
	}
	appendStringInfoString(lcl_pqexp, dbms_md_fmtId(id));

	id_return = getLocalStringBuffer();

	appendStringInfoString(id_return, lcl_pqexp->data);
	destroyStringInfo(lcl_pqexp);

	return id_return->data;
}

/*
 * Convert a string value to an SQL string literal and append it to
 * the given buffer.  We assume the specified client_encoding and
 * standard_conforming_strings settings.
 *
 * This is essentially equivalent to libpq's PQescapeStringInternal,
 * except for the output buffer structure.  We need it in situations
 * where we do not have a PGconn available.  Where we do,
 * appendStringLiteralConn is a better choice.
 */
void
dbms_md_append_string_literal(StringInfo buf, const char *str,
					int encoding, bool std_strings)
{
	size_t		length = strlen(str);
	const char *source = str;
	char	   *target;

	enlargeStringInfo(buf, 2 * length + 2);

	target = buf->data + buf->len;
	*target++ = '\'';

	while (*source != '\0')
	{
		char		c = *source;
		int			len;
		int			i;

		/* Fast path for plain ASCII */
		if (!IS_HIGHBIT_SET(c))
		{
			/* Apply quoting if needed */
			if (SQL_STR_DOUBLE(c, !std_strings))
				*target++ = c;
			/* Copy the character */
			*target++ = c;
			source++;
			continue;
		}

		/* Slow path for possible multibyte characters */
		len = pg_encoding_mblen(encoding, source);

		/* Copy the character */
		for (i = 0; i < len; i++)
		{
			if (*source == '\0')
				break;
			*target++ = *source++;
		}

		/*
		 * If we hit premature end of string (ie, incomplete multibyte
		 * character), try to pad out to the correct length with spaces. We
		 * may not be able to pad completely, but we will always be able to
		 * insert at least one pad space (since we'd not have quoted a
		 * multibyte character).  This should be enough to make a string that
		 * the server will error out on.
		 */
		if (i < len)
		{
			char	   *stop = buf->data + buf->maxlen - 2;

			for (; i < len; i++)
			{
				if (target >= stop)
					break;
				*target++ = ' ';
			}
			break;
		}
	}

	/* Write the terminating quote and NUL character. */
	*target++ = '\'';
	*target = '\0';

	buf->len = target - buf->data;
}


/*
 * Convert a string value to an SQL string literal and append it to
 * the given buffer.  Encoding and string syntax rules are as indicated
 * by current settings of the PGconn.
 */
void
dbms_md_append_string_literal_conn(StringInfo buf, 
												const char *str,  
												int server_version,
												int encoding, 
												bool std_strings)
{
	size_t		length = strlen(str);

	/*
	 * XXX This is a kluge to silence escape_string_warning in our utility
	 * programs.  It should go away someday.
	 */
	if (strchr(str, '\\') != NULL && server_version >= 80100)
	{
		/* ensure we are not adjacent to an identifier */
		if (buf->len > 0 && buf->data[buf->len - 1] != ' ')
			appendStringInfoChar(buf, ' ');
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
		dbms_md_append_string_literal(buf, str, encoding, false);
		return;
	}
	/* XXX end kluge */

	enlargeStringInfo(buf, 2 * length + 2);
	appendStringInfoChar(buf, '\'');
	buf->len += dbms_md_escape_string_conn(encoding, std_strings, buf->data + buf->len,
								   str, length, NULL);
	appendStringInfoChar(buf, '\'');
}


/*
 * Convert a string value to a dollar quoted literal and append it to
 * the given buffer. If the dqprefix parameter is not NULL then the
 * dollar quote delimiter will begin with that (after the opening $).
 *
 * No escaping is done at all on str, in compliance with the rules
 * for parsing dollar quoted strings.  Also, we need not worry about
 * encoding issues.
 */
void
dbms_md_append_string_literal_dq(StringInfo buf, const char *str, const char *dqprefix)
{
	static const char suffixes[] = "_XXXXXXX";
	int			nextchar = 0;
	StringInfo delimBuf = createStringInfo();

	/* start with $ + dqprefix if not NULL */
	appendStringInfoChar(delimBuf, '$');
	if (dqprefix)
		appendStringInfoString(delimBuf, dqprefix);

	/*
	 * Make sure we choose a delimiter which (without the trailing $) is not
	 * present in the string being quoted. We don't check with the trailing $
	 * because a string ending in $foo must not be quoted with $foo$.
	 */
	while (strstr(str, delimBuf->data) != NULL)
	{
		appendStringInfoChar(delimBuf, suffixes[nextchar++]);
		nextchar %= sizeof(suffixes) - 1;
	}

	/* add trailing $ */
	appendStringInfoChar(delimBuf, '$');

	/* quote it and we are all done */
	appendStringInfoString(buf, delimBuf->data);
	appendStringInfoString(buf, str);
	appendStringInfoString(buf, delimBuf->data);

	destroyStringInfo(delimBuf);
	pfree(delimBuf);
}


/*
 * Convert a bytea value (presented as raw bytes) to an SQL string literal
 * and append it to the given buffer.  We assume the specified
 * standard_conforming_strings setting.
 *
 * This is needed in situations where we do not have a PGconn available.
 * Where we do, PQescapeByteaConn is a better choice.
 */
void
dbms_md_append_bytea_literal(StringInfo buf, const unsigned char *str, size_t length,
				   bool std_strings)
{
	const unsigned char *source = str;
	char	   *target;

	static const char hextbl[] = "0123456789abcdef";

	/*
	 * This implementation is hard-wired to produce hex-format output. We do
	 * not know the server version the output will be loaded into, so making
	 * an intelligent format choice is impossible.  It might be better to
	 * always use the old escaped format.
	 */
	enlargeStringInfo(buf, 2 * length + 5);

	target = buf->data + buf->len;
	*target++ = '\'';
	if (!std_strings)
		*target++ = '\\';
	*target++ = '\\';
	*target++ = 'x';

	while (length-- > 0)
	{
		unsigned char c = *source++;

		*target++ = hextbl[(c >> 4) & 0xF];
		*target++ = hextbl[c & 0xF];
	}

	/* Write the terminating quote and NUL character. */
	*target++ = '\'';
	*target = '\0';

	buf->len = target - buf->data;
}

#if 0
/*
 * Append the given string to the shell command being built in the buffer,
 * with shell-style quoting as needed to create exactly one argument.
 *
 * Forbid LF or CR characters, which have scant practical use beyond designing
 * security breaches.  The Windows command shell is unusable as a conduit for
 * arguments containing LF or CR characters.  A future major release should
 * reject those characters in CREATE ROLE and CREATE DATABASE, because use
 * there eventually leads to errors here.
 *
 * appendShellString() simply prints an error and dies if LF or CR appears.
 * appendShellStringNoError() omits those characters from the result, and
 * returns false if there were any.
 */
void
appendShellString(StringInfo buf, const char *str)
{
	if (!appendShellStringNoError(buf, str))
	{
		fprintf(stderr,
				_("shell command argument contains a newline or carriage return: \"%s\"\n"),
				str);
		exit(EXIT_FAILURE);
	}
}

bool
appendShellStringNoError(StringInfo buf, const char *str)
{
#ifdef WIN32
	int			backslash_run_length = 0;
#endif
	bool		ok = true;
	const char *p;

	/*
	 * Don't bother with adding quotes if the string is nonempty and clearly
	 * contains only safe characters.
	 */
	if (*str != '\0' &&
		strspn(str, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./:") == strlen(str))
	{
		appendStringInfoStr(buf, str);
		return ok;
	}

#ifndef WIN32
	appendStringInfoChar(buf, '\'');
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			ok = false;
			continue;
		}

		if (*p == '\'')
			appendStringInfoStr(buf, "'\"'\"'");
		else
			appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
#else							/* WIN32 */

	/*
	 * A Windows system() argument experiences two layers of interpretation.
	 * First, cmd.exe interprets the string.  Its behavior is undocumented,
	 * but a caret escapes any byte except LF or CR that would otherwise have
	 * special meaning.  Handling of a caret before LF or CR differs between
	 * "cmd.exe /c" and other modes, and it is unusable here.
	 *
	 * Second, the new process parses its command line to construct argv (see
	 * https://msdn.microsoft.com/en-us/library/17w5ykft.aspx).  This treats
	 * backslash-double quote sequences specially.
	 */
	appendStringInfoStr(buf, "^\"");
	for (p = str; *p; p++)
	{
		if (*p == '\n' || *p == '\r')
		{
			ok = false;
			continue;
		}

		/* Change N backslashes before a double quote to 2N+1 backslashes. */
		if (*p == '"')
		{
			while (backslash_run_length)
			{
				appendStringInfoStr(buf, "^\\");
				backslash_run_length--;
			}
			appendStringInfoStr(buf, "^\\");
		}
		else if (*p == '\\')
			backslash_run_length++;
		else
			backslash_run_length = 0;

		/*
		 * Decline to caret-escape the most mundane characters, to ease
		 * debugging and lest we approach the command length limit.
		 */
		if (!((*p >= 'a' && *p <= 'z') ||
			  (*p >= 'A' && *p <= 'Z') ||
			  (*p >= '0' && *p <= '9')))
			appendStringInfoChar(buf, '^');
		appendStringInfoChar(buf, *p);
	}

	/*
	 * Change N backslashes at end of argument to 2N backslashes, because they
	 * precede the double quote that terminates the argument.
	 */
	while (backslash_run_length)
	{
		appendStringInfoStr(buf, "^\\");
		backslash_run_length--;
	}
	appendStringInfoStr(buf, "^\"");
#endif							/* WIN32 */

	return ok;
}
#endif

/*
 * Append the given string to the buffer, with suitable quoting for passing
 * the string as a value, in a keyword/pair value in a 
 * string
 */
void
dbms_md_append_conn_str_val(StringInfo buf, const char *str)
{
	const char *s;
	bool		needquotes;

	/*
	 * If the string is one or more plain ASCII characters, no need to quote
	 * it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = true;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
		needquotes = false;
	}

	if (needquotes)
	{
		appendStringInfoChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendStringInfoChar(buf, '\\');

			appendStringInfoChar(buf, *str);
			str++;
		}
		appendStringInfoChar(buf, '\'');
	}
	else
		appendStringInfoString(buf, str);
}


/*
 * Append a psql meta-command that connects to the given database with the
 * then-current connection's user, host and port.
 */
void
dbms_md_append_psql_meta_connect(StringInfo buf, const char *dbname)
{
	const char *s;
	bool complex;

	/*
	 * If the name is plain ASCII characters, emit a trivial "\connect "foo"".
	 * For other names, even many not technically requiring it, skip to the
	 * general case.  No database has a zero-length name.
	 */
	complex = false;

	for (s = dbname; *s; s++)
	{
		if (*s == '\n' || *s == '\r')
		{
			elog(ERROR,	_("database name contains a newline or carriage return: \"%s\"\n"),
					dbname);
		}

		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			complex = true;
		}
	}

	appendStringInfoString(buf, "\\connect ");
	if (complex)
	{
		StringInfoData connstr;

		initStringInfo(&connstr);
		appendStringInfo(&connstr, "dbname=");
		dbms_md_append_conn_str_val(&connstr, dbname);

		appendStringInfo(buf, "-reuse-previous=on ");

		/*
		 * As long as the name does not contain a newline, SQL identifier
		 * quoting satisfies the psql meta-command parser.  Prefer not to
		 * involve psql-interpreted single quotes, which behaved differently
		 * before PostgreSQL 9.2.
		 */
		appendStringInfoString(buf, dbms_md_fmtId(connstr.data));

		destroyStringInfo(&connstr);
	}
	else
		appendStringInfoString(buf, dbms_md_fmtId(dbname));
	appendStringInfoChar(buf, '\n');
}


/*
 * Deconstruct the text representation of a 1-dimensional Postgres array
 * into individual items.
 *
 * On success, returns true and sets *itemarray and *nitems to describe
 * an array of individual strings.  On parse failure, returns false;
 * *itemarray may exist or be NULL.
 *
 * NOTE: free'ing itemarray is sufficient to deallocate the working storage.
 */
bool dbms_md_parse_array(const char *atext, char ***itemarray, int *nitems)
{
	int 		inputlen;
	char	  **items;
	char	   *strings;
	int 		curitem;
	
	/*
	 * We expect input in the form of "{item,item,item}" where any item is
	 * either raw data, or surrounded by double quotes (in which case embedded
	 * characters including backslashes and quotes are backslashed).
	 *
	 * We build the result as an array of pointers followed by the actual
	 * string data, all in one palloc block for convenience of deallocation.
	 * The worst-case storage need is not more than one pointer and one
	 * character for each input character (consider "{,,,,,,,,,,}").
	 */
	*itemarray = NULL;
	*nitems = 0;
	
	if (NULL == atext)
		return false;

	inputlen = strlen(atext);
	if (inputlen < 2 || atext[0] != '{' || atext[inputlen - 1] != '}')
		return false;			/* bad input */
	items = (char **) palloc(inputlen * (sizeof(char *) + sizeof(char)));
	if (items == NULL)
		return false;			/* out of memory */
	*itemarray = items;
	strings = (char *) (items + inputlen);

	atext++;					/* advance over initial '{' */
	curitem = 0;
	while (*atext != '}')
	{
		if (*atext == '\0')
			return false;		/* premature end of string */
		items[curitem] = strings;
		while (*atext != '}' && *atext != ',')
		{
			if (*atext == '\0')
				return false;	/* premature end of string */
			if (*atext != '"')
				*strings++ = *atext++;	/* copy unquoted data */
			else
			{
				/* process quoted substring */
				atext++;
				while (*atext != '"')
				{
					if (*atext == '\0')
						return false;	/* premature end of string */
					if (*atext == '\\')
					{
						atext++;
						if (*atext == '\0')
							return false;	/* premature end of string */
					}
					*strings++ = *atext++;	/* copy quoted data */
				}
				atext++;
			}
		}
		*strings++ = '\0';
		if (*atext == ',')
			atext++;
		curitem++;
	}
	if (atext[1] != '\0')
		return false;			/* bogus syntax (embedded '}') */
	*nitems = curitem;
	return true;
}	


size_t
dbms_md_escape_string_internal(char *to, const char *from, size_t length,
					   int *error,
					   int encoding, bool std_strings)
{
	const char *source = from;
	char	   *target = to;
	size_t		remaining = length;

	if (error)
		*error = 0;

	while (remaining > 0 && *source != '\0')
	{
		char		c = *source;
		int			len;
		int			i;

		/* Fast path for plain ASCII */
		if (!IS_HIGHBIT_SET(c))
		{
			/* Apply quoting if needed */
			if (SQL_STR_DOUBLE(c, !std_strings))
				*target++ = c;
			/* Copy the character */
			*target++ = c;
			source++;
			remaining--;
			continue;
		}

		/* Slow path for possible multibyte characters */
		len = pg_encoding_mblen(encoding, source);

		/* Copy the character */
		for (i = 0; i < len; i++)
		{
			if (remaining == 0 || *source == '\0')
				break;
			*target++ = *source++;
			remaining--;
		}

		/*
		 * If we hit premature end of string (ie, incomplete multibyte
		 * character), try to pad out to the correct length with spaces. We
		 * may not be able to pad completely, but we will always be able to
		 * insert at least one pad space (since we'd not have quoted a
		 * multibyte character).  This should be enough to make a string that
		 * the server will error out on.
		 */
		if (i < len)
		{
			if (error)
				*error = 1;			
			for (; i < len; i++)
			{
				if (((size_t) (target - to)) / 2 >= length)
					break;
				*target++ = ' ';
			}
			break;
		}
	}

	/* Write the terminating NUL character. */
	*target = '\0';

	return target - to;
}

size_t
dbms_md_escape_string_conn(int encoding, bool std_strings,
				   char *to, const char *from, size_t length,
				   int *error)
{	
	return dbms_md_escape_string_internal(to, from, length, error,
								  encoding,
								  std_strings);
}

/*
 *		PQunescapeBytea - converts the null terminated string representation
 *		of a bytea, strtext, into binary, filling a buffer. It returns a
 *		pointer to the buffer (or NULL on error), and the size of the
 *		buffer in retbuflen. The pointer may subsequently be used as an
 *		argument to the function PQfreemem.
 *
 *		The following transformations are made:
 *		\\	 == ASCII 92 == \
 *		\ooo == a byte whose value = ooo (ooo is an octal number)
 *		\x	 == x (x is any character not matched by the above transformations)
 */
unsigned char *
dbms_md_escape_bytea(const unsigned char *strtext, size_t *retbuflen)
{
	size_t		strtextlen,
				buflen;
	unsigned char *buffer,
			   *tmpbuf;
	size_t		i,
				j;

	if (strtext == NULL)
		return NULL;

	strtextlen = strlen((const char *) strtext);

	if (strtext[0] == '\\' && strtext[1] == 'x')
	{
		const unsigned char *s;
		unsigned char *p;

		buflen = (strtextlen - 2) / 2;
		/* Avoid unportable palloc(0) */
		buffer = (unsigned char *) palloc(buflen > 0 ? buflen : 1);
		if (buffer == NULL)
			return NULL;

		s = strtext + 2;
		p = buffer;
		while (*s)
		{
			char		v1,
						v2;

			/*
			 * Bad input is silently ignored.  Note that this includes
			 * whitespace between hex pairs, which is allowed by byteain.
			 */
			v1 = dbms_md_get_hex(*s++);
			if (!*s || v1 == (char) -1)
				continue;
			v2 = dbms_md_get_hex(*s++);
			if (v2 != (char) -1)
				*p++ = (v1 << 4) | v2;
		}

		buflen = p - buffer;
	}
	else
	{
		/*
		 * Length of input is max length of output, but add one to avoid
		 * unportable palloc(0) if input is zero-length.
		 */
		buffer = (unsigned char *) palloc(strtextlen + 1);
		if (buffer == NULL)
			return NULL;

		for (i = j = 0; i < strtextlen;)
		{
			switch (strtext[i])
			{
				case '\\':
					i++;
					if (strtext[i] == '\\')
						buffer[j++] = strtext[i++];
					else
					{
						if ((ISFIRSTOCTDIGIT(strtext[i])) &&
							(ISOCTDIGIT(strtext[i + 1])) &&
							(ISOCTDIGIT(strtext[i + 2])))
						{
							int			byte;

							byte = OCTVAL(strtext[i++]);
							byte = (byte << 3) + OCTVAL(strtext[i++]);
							byte = (byte << 3) + OCTVAL(strtext[i++]);
							buffer[j++] = byte;
						}
					}

					/*
					 * Note: if we see '\' followed by something that isn't a
					 * recognized escape sequence, we loop around having done
					 * nothing except advance i.  Therefore the something will
					 * be emitted as ordinary data on the next cycle. Corner
					 * case: '\' at end of string will just be discarded.
					 */
					break;

				default:
					buffer[j++] = strtext[i++];
					break;
			}
		}
		buflen = j;				/* buflen is the length of the dequoted data */
	}

	/* Shrink the buffer to be no larger than necessary */
	/* +1 avoids unportable behavior when buflen==0 */
	tmpbuf = repalloc(buffer, buflen + 1);

	/* It would only be a very brain-dead repalloc that could fail, but... */
	if (!tmpbuf)
	{
		pfree(buffer);
		return NULL;
	}

	*retbuflen = buflen;
	return tmpbuf;
}

#if 0
/*
 * Escape arbitrary strings.  If as_ident is true, we escape the result
 * as an identifier; if false, as a literal.  The result is returned in
 * a newly allocated buffer.  If we fail due to an encoding violation or out
 * of memory condition, we return NULL, storing an error message into conn.
 */
static char *
dbms_md_escape_internal(int encoding, bool std_strings, const char *str, size_t len, bool as_ident)
{
	const char *s;
	char	   *result;
	char	   *rp;
	int			num_quotes = 0; /* single or double, depending on as_ident */
	int			num_backslashes = 0;
	int			input_len;
	int			result_size;
	char		quote_char = as_ident ? '"' : '\'';

	/* Scan the string for characters that must be escaped. */
	for (s = str; (s - str) < len && *s != '\0'; ++s)
	{
		if (*s == quote_char)
			++num_quotes;
		else if (*s == '\\')
			++num_backslashes;
		else if (IS_HIGHBIT_SET(*s))
		{
			int			charlen;

			/* Slow path for possible multibyte characters */
			charlen = pg_encoding_mblen(encoding, s);

			/* Multibyte character overruns allowable length. */
			if ((s - str) + charlen > len || memchr(s, 0, charlen) != NULL)
			{
				elog(ERROR, "incomplete multibyte character");
				return NULL;
			}

			/* Adjust s, bearing in mind that for loop will increment it. */
			s += charlen - 1;
		}
	}

	/* Allocate output buffer. */
	input_len = s - str;
	result_size = input_len + num_quotes + 3;	/* two quotes, plus a NUL */
	if (!as_ident && num_backslashes > 0)
		result_size += num_backslashes + 2;
	result = rp = (char *) malloc(result_size);
	if (rp == NULL)
	{
		elog(ERROR, "out of memory");
		return NULL;
	}

	/*
	 * If we are escaping a literal that contains backslashes, we use the
	 * escape string syntax so that the result is correct under either value
	 * of standard_conforming_strings.  We also emit a leading space in this
	 * case, to guard against the possibility that the result might be
	 * interpolated immediately following an identifier.
	 */
	if (!as_ident && num_backslashes > 0)
	{
		*rp++ = ' ';
		*rp++ = 'E';
	}

	/* Opening quote. */
	*rp++ = quote_char;

	/*
	 * Use fast path if possible.
	 *
	 * We've already verified that the input string is well-formed in the
	 * current encoding.  If it contains no quotes and, in the case of
	 * literal-escaping, no backslashes, then we can just copy it directly to
	 * the output buffer, adding the necessary quotes.
	 *
	 * If not, we must rescan the input and process each character
	 * individually.
	 */
	if (num_quotes == 0 && (num_backslashes == 0 || as_ident))
	{
		memcpy(rp, str, input_len);
		rp += input_len;
	}
	else
	{
		for (s = str; s - str < input_len; ++s)
		{
			if (*s == quote_char || (!as_ident && *s == '\\'))
			{
				*rp++ = *s;
				*rp++ = *s;
			}
			else if (!IS_HIGHBIT_SET(*s))
				*rp++ = *s;
			else
			{
				int	i = pg_encoding_mblen(encoding, s);

				while (1)
				{
					*rp++ = *s;
					if (--i == 0)
						break;
					++s;		/* for loop will provide the final increment */
				}
			}
		}
	}

	/* Closing quote and terminating NUL. */
	*rp++ = quote_char;
	*rp = '\0';

	return result;
}

#endif

/*
 * Format a reloptions array and append it to the given buffer.
 *
 * "prefix" is prepended to the option names; typically it's "" or "toast.".
 *
 * Returns false if the reloptions array could not be parsed (in which case
 * nothing will have been appended to the buffer), or true on success.
 *
 * Note: this logic should generally match the backend's flatten_reloptions()
 * (in adt/ruleutils.c).
 */
bool
dbms_append_reloptions_array(StringInfo buffer, const char *reloptions,
					  const char *prefix, int encoding, bool std_strings)
{
	char	  **options;
	int			noptions;
	int			i;

	if (!dbms_md_parse_array(reloptions, &options, &noptions))
	{
		if (options)
			pfree(options);
		return false;
	}

	for (i = 0; i < noptions; i++)
	{
		char	   *option = options[i];
		char	   *name;
		char	   *separator;
		char	   *value;

		/*
		 * Each array element should have the form name=value.  If the "=" is
		 * missing for some reason, treat it like an empty value.
		 */
		name = option;
		separator = strchr(option, '=');
		if (separator)
		{
			*separator = '\0';
			value = separator + 1;
		}
		else
			value = "";

		if (i > 0)
			appendStringInfoString(buffer, ", ");
		appendStringInfo(buffer, "%s%s=", prefix, dbms_md_fmtId_internal(name, true));

		/*
		 * In general we need to quote the value; but to avoid unnecessary
		 * clutter, do not quote if it is an identifier that would not need
		 * quoting.  (We could also allow numbers, but that is a bit trickier
		 * than it looks --- for example, are leading zeroes significant?  We
		 * don't want to assume very much here about what custom reloptions
		 * might mean.)
		 */
		if (strcmp(dbms_md_fmtId(value), value) == 0)
			appendStringInfoString(buffer, value);
		else
			dbms_md_append_string_literal(buffer, value, encoding, std_strings);
	}

	if (options)
		pfree(options);

	return true;
}


/*
 * processSQLNamePattern
 *
 * Scan a wildcard-pattern string and generate appropriate WHERE clauses
 * to limit the set of objects returned.  The WHERE clauses are appended
 * to the already-partially-constructed query in buf.  Returns whether
 * any clause was added.
 *
 * conn: connection query will be sent to (consulted for escaping rules).
 * buf: output parameter.
 * pattern: user-specified pattern option, or NULL if none ("*" is implied).
 * have_where: true if caller already emitted "WHERE" (clauses will be ANDed
 * onto the existing WHERE clause).
 * force_escape: always quote regexp special characters, even outside
 * double quotes (else they are quoted only between double quotes).
 * schemavar: name of query variable to match against a schema-name pattern.
 * Can be NULL if no schema.
 * namevar: name of query variable to match against an object-name pattern.
 * altnamevar: NULL, or name of an alternative variable to match against name.
 * visibilityrule: clause to use if we want to restrict to visible objects
 * (for example, "pg_catalog.pg_table_is_visible(p.oid)").  Can be NULL.
 *
 * Formatting note: the text already present in buf should end with a newline.
 * The appended text, if any, will end with one too.
 */
bool
dbms_process_sql_name_pattern(int encoding, int server_version, bool std_string,
					StringInfo buf, const char *pattern,
					  bool have_where, bool force_escape,
					  const char *schemavar, const char *namevar,
					  const char *altnamevar, const char *visibilityrule)
{
	StringInfoData schemabuf;
	StringInfoData namebuf;	
	bool		inquotes;
	const char *cp;
	int			i;
	bool		added_clause = false;

#define WHEREAND() \
	(appendStringInfoString(buf, have_where ? "  AND " : "WHERE "), \
	 have_where = true, added_clause = true)

	if (pattern == NULL)
	{
		/* Default: select all visible objects */
		if (visibilityrule)
		{
			WHEREAND();
			appendStringInfo(buf, "%s", visibilityrule);
		}
		return added_clause;
	}

	initStringInfo(&schemabuf);
	initStringInfo(&namebuf);

	/*
	 * Parse the pattern, converting quotes and lower-casing unquoted letters.
	 * Also, adjust shell-style wildcard characters into regexp notation.
	 *
	 * We surround the pattern with "^(...)$" to force it to match the whole
	 * string, as per SQL practice.  We have to have parens in case the string
	 * contains "|", else the "^" and "$" will be bound into the first and
	 * last alternatives which is not what we want.
	 *
	 * Note: the result of this pass is the actual regexp pattern(s) we want
	 * to execute.  Quoting/escaping into SQL literal format will be done
	 * below using appendStringLiteralConn().
	 */
	appendStringInfoString(&namebuf, "^(");

	inquotes = false;
	cp = pattern;

	while (*cp)
	{
		char		ch = *cp;

		if (ch == '"')
		{
			if (inquotes && cp[1] == '"')
			{
				/* emit one quote, stay in inquotes mode */
				appendStringInfoChar(&namebuf, '"');
				cp++;
			}
			else
				inquotes = !inquotes;
			cp++;
		}
		else if (!inquotes && isupper((unsigned char) ch))
		{
			appendStringInfoChar(&namebuf,
								  (unsigned char) ch);
			cp++;
		}
		else if (!inquotes && ch == '*')
		{
			appendStringInfoString(&namebuf, ".*");
			cp++;
		}
		else if (!inquotes && ch == '?')
		{
			appendStringInfoChar(&namebuf, '.');
			cp++;
		}
		else if (!inquotes && ch == '.')
		{
			/* Found schema/name separator, move current pattern to schema */
			resetStringInfo(&schemabuf);
			appendStringInfoString(&schemabuf, namebuf.data);
			resetStringInfo(&namebuf);
			appendStringInfoString(&namebuf, "^(");
			cp++;
		}
		else if (ch == '$')
		{
			/*
			 * Dollar is always quoted, whether inside quotes or not. The
			 * reason is that it's allowed in SQL identifiers, so there's a
			 * significant use-case for treating it literally, while because
			 * we anchor the pattern automatically there is no use-case for
			 * having it possess its regexp meaning.
			 */
			appendStringInfoString(&namebuf, "\\$");
			cp++;
		}
		else
		{
			/*
			 * Ordinary data character, transfer to pattern
			 *
			 * Inside double quotes, or at all times if force_escape is true,
			 * quote regexp special characters with a backslash to avoid
			 * regexp errors.  Outside quotes, however, let them pass through
			 * as-is; this lets knowledgeable users build regexp expressions
			 * that are more powerful than shell-style patterns.
			 */
			if ((inquotes || force_escape) &&
				strchr("|*+?()[]{}.^$\\", ch))
				appendStringInfoChar(&namebuf, '\\');
			i = pg_encoding_mblen(encoding, cp);
			while (i-- && *cp)
			{
				appendStringInfoChar(&namebuf, *cp);
				cp++;
			}
		}
	}

	/*
	 * Now decide what we need to emit.  Note there will be a leading "^(" in
	 * the patterns in any case.
	 */
	if (namebuf.len > 2)
	{
		/* We have a name pattern, so constrain the namevar(s) */

		appendStringInfoString(&namebuf, ")$");
		/* Optimize away a "*" pattern */
		if (strcmp(namebuf.data, "^(.*)$") != 0)
		{
			WHEREAND();
			if (altnamevar)
			{
				appendStringInfo(buf, "(%s ~ ", namevar);
				dbms_md_append_string_literal_conn(buf, namebuf.data, server_version, encoding, std_string);
				appendStringInfo(buf, "        OR %s ~ ", altnamevar);
				dbms_md_append_string_literal_conn(buf, namebuf.data, server_version, encoding, std_string);
				appendStringInfoString(buf, ")");
			}
			else
			{
				appendStringInfo(buf, "%s ~ ", namevar);
				dbms_md_append_string_literal_conn(buf, namebuf.data, server_version, encoding, std_string);
				/*appendStringInfoChar(buf, '\n');*/
			}
		}
	}

	if (schemabuf.len > 2)
	{
		/* We have a schema pattern, so constrain the schemavar */

		appendStringInfoString(&schemabuf, ")$");
		/* Optimize away a "*" pattern */
		if (strcmp(schemabuf.data, "^(.*)$") != 0 && schemavar)
		{
			WHEREAND();
			appendStringInfo(buf, "%s ~ ", schemavar);
			dbms_md_append_string_literal_conn(buf, schemabuf.data, server_version, encoding, std_string);
			/*appendStringInfoChar(buf, '\n');*/
		}
	}
	else
	{
		/* No schema pattern given, so select only visible objects */
		if (visibilityrule)
		{
			WHEREAND();
			appendStringInfo(buf, "%s", visibilityrule);
		}
	}

	destroyStringInfo(&schemabuf);
	destroyStringInfo(&namebuf);

	return added_clause;
#undef WHEREAND
}


void destroyStringInfo(StringInfo output)
{
	if (output)
		pfree(output->data);
}

/*StringInfo createStringInfo(MemoryContext mem_ctx)
{	
	StringInfo	res;
	MemoryContext old_ctx = MemoryContextSwitchTo(mem_ctx);
	res = (StringInfo) palloc0(sizeof(StringInfoData));
	initStringInfo(res);

	MemoryContextSwitchTo(old_ctx);
	return res;
}*/

StringInfo createStringInfo(void)
{	
	StringInfo	res;
	res = (StringInfo) palloc(sizeof(StringInfoData));
	initStringInfo(res);

	return res;
}

void printfStringInfo(StringInfo str, const char *fmt,...)
{
	va_list		args;
	bool		needed;

	resetStringInfo(str);

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		/* Try to format the data. */
		va_start(args, fmt);
		needed = appendStringInfoVA(str, fmt, args);
		va_end(args);

		if (needed == 0)
			break;				/* success */

		/* Increase the buffer size and try again. */
		enlargeStringInfo(str, needed);		
	} while (!needed);
}

/*
 * Format a Postgres version number (in the PG_VERSION_NUM integer format
 * returned by PQserverVersion()) as a string.  This exists mainly to
 * encapsulate knowledge about two-part vs. three-part version numbers.
 *
 * For reentrancy, caller must supply the buffer the string is put in.
 * Recommended size of the buffer is 32 bytes.
 *
 * Returns address of 'buf', as a notational convenience.
 */
char *
dbms_md_fmt_version_number(MemoryContext mem_ctx, int version_number, bool include_minor)
{
	size_t buflen = DBMS_MD_VERSION_LEN;
	char *buf = (char*)dbms_md_malloc0(mem_ctx, DBMS_MD_VERSION_LEN);
	if (version_number >= 100000)
	{
		/* New two-part style */
		if (include_minor)
			snprintf(buf, buflen, "%d.%d", version_number / 10000,
					 version_number % 10000);
		else
			snprintf(buf, buflen, "%d", version_number / 10000);
	}
	else
	{
		/* Old three-part style */
		if (include_minor)
			snprintf(buf, buflen, "%d.%d.%d", version_number / 10000,
					 (version_number / 100) % 100,
					 version_number % 100);
		else
			snprintf(buf, buflen, "%d.%d", version_number / 10000,
					 (version_number / 100) % 100);
	}
	return buf;
}


