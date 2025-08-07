/*-------------------------------------------------------------------------
 *
 * scansup.c
 *	  support routines for the lex/flex scanner, used by both the normal
 * backend as well as the bootstrap backend
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/scansup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "parser/scansup.h"
#include "mb/pg_wchar.h"
#include "parser/scanner.h"
#include "utils/guc.h"

static char *upcase_identifier_internal(const char *ident, int len, bool warn, bool truncate);
/* ----------------
 *		scanstr
 *
 * if the string passed in has escaped codes, map the escape codes to actual
 * chars
 *
 * the string returned is palloc'd and should eventually be pfree'd by the
 * caller!
 * ----------------
 */

char *
scanstr(const char *s)
{
	char	   *newStr;
	int			len,
				i,
				j;

	if (s == NULL || s[0] == '\0')
		return pstrdup("");

	len = strlen(s);

	newStr = palloc(len + 1);	/* string cannot get longer */

	for (i = 0, j = 0; i < len; i++)
	{
		if (s[i] == '\'')
		{
			/*
			 * Note: if scanner is working right, unescaped quotes can only
			 * appear in pairs, so there should be another character.
			 */
			i++;
			/* The bootstrap parser is not as smart, so check here. */
			Assert(s[i] == '\'');
			newStr[j] = s[i];
		}
		else if (s[i] == '\\')
		{
			i++;
			switch (s[i])
			{
				case 'b':
					newStr[j] = '\b';
					break;
				case 'f':
					newStr[j] = '\f';
					break;
				case 'n':
					newStr[j] = '\n';
					break;
				case 'r':
					newStr[j] = '\r';
					break;
				case 't':
					newStr[j] = '\t';
					break;
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
					{
						int			k;
						long		octVal = 0;

						for (k = 0;
							 s[i + k] >= '0' && s[i + k] <= '7' && k < 3;
							 k++)
							octVal = (octVal << 3) + (s[i + k] - '0');
						i += k - 1;
						newStr[j] = ((char) octVal);
					}
					break;
				default:
					newStr[j] = s[i];
					break;
			}					/* switch */
		}						/* s[i] == '\\' */
		else
			newStr[j] = s[i];
		j++;
	}
	newStr[j] = '\0';
	return newStr;
}


/*
 * downcase_truncate_identifier() --- do appropriate downcasing and
 * truncation of an unquoted identifier.  Optionally warn of truncation.
 *
 * Returns a palloc'd string containing the adjusted identifier.
 *
 * Note: in some usages the passed string is not null-terminated.
 *
 * Note: the API of this function is designed to allow for downcasing
 * transformations that increase the string length, but we don't yet
 * support that.  If you want to implement it, you'll need to fix
 * SplitIdentifierString() in utils/adt/varlena.c.
 */
char *
downcase_truncate_identifier(const char *ident, int len, bool warn)
{
	return downcase_identifier(ident, len, warn, true);
}

/*
 * a workhorse for downcase_truncate_identifier
 */
char *
downcase_identifier(const char *ident, int len, bool warn, bool truncate)
{
	char	   *result;
	int			i;
	bool		enc_is_single_byte;

	result = palloc(len + 1);
	enc_is_single_byte = pg_database_encoding_max_length() == 1;

	/*
	 * SQL99 specifies Unicode-aware case normalization, which we don't yet
	 * have the infrastructure for.  Instead we use tolower() to provide a
	 * locale-aware translation.  However, there are some locales where this
	 * is not right either (eg, Turkish may do strange things with 'i' and
	 * 'I').  Our current compromise is to use tolower() for characters with
	 * the high bit set, as long as they aren't part of a multi-byte
	 * character, and use an ASCII-only downcasing for 7-bit characters.
	 */
	for (i = 0; i < len; i++)
	{
		unsigned char ch = (unsigned char) ident[i];

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		else if (enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch))
			ch = tolower(ch);
		result[i] = (char) ch;
	}
	result[i] = '\0';

	if (i >= NAMEDATALEN && truncate)
		truncate_identifier(result, i, warn);

	return result;
}


/*
 * truncate_identifier() --- truncate an identifier to NAMEDATALEN-1 bytes.
 *
 * The given string is modified in-place, if necessary.  A warning is
 * issued if requested.
 *
 * We require the caller to pass in the string length since this saves a
 * strlen() call in some common usages.
 */
void
truncate_identifier(char *ident, int len, bool warn)
{
	if (len >= NAMEDATALEN)
	{
		len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);
		if (warn)
		{
			/*
			 * We avoid using %.*s here because it can misbehave if the data
			 * is not valid in what libc thinks is the prevailing encoding.
			 */
			char		buf[NAMEDATALEN];

			memcpy(buf, ident, len);
			buf[len] = '\0';
			ereport(NOTICE,
					(errcode(ERRCODE_NAME_TOO_LONG),
					 errmsg("identifier \"%s\" will be truncated to \"%s\"",
							ident, buf)));
		}
		ident[len] = '\0';
	}
}

/*
 * scanner_isspace() --- return TRUE if flex scanner considers char whitespace
 *
 * This should be used instead of the potentially locale-dependent isspace()
 * function when it's important to match the lexer's behavior.
 *
 * In principle we might need similar functions for isalnum etc, but for the
 * moment only isspace seems needed.
 */
bool
scanner_isspace(char ch)
{
	/* This must match scan.l's list of {space} characters */
	if (ch == ' ' ||
		ch == '\t' ||
		ch == '\n' ||
		ch == '\r' ||
		ch == '\f')
		return true;
	return false;
}

core_yyscan_t
scanner_init(const char *str,
			 core_yy_extra_type *yyext,
			 const ScanKeyword *keywords,
			 int num_keywords)
{
	if (ORA_MODE)
		return ora_scanner_init(str, yyext, keywords, num_keywords);
	return pg_scanner_init(str, yyext, keywords, num_keywords);
}

void
scanner_finish(core_yyscan_t yyscanner)
{
	if (ORA_MODE)
		return ora_scanner_finish(yyscanner);
	return pg_scanner_finish(yyscanner);
}

int
core_yylex(core_YYSTYPE *lvalp, YYLTYPE *llocp,
					  core_yyscan_t yyscanner)
{
	if (ORA_MODE)
		return ora_core_yylex(lvalp, llocp, yyscanner);
	return pg_core_yylex(lvalp, llocp, yyscanner);
}

int
scanner_errposition(int location, core_yyscan_t yyscanner)
{
	if (ORA_MODE)
		return ora_scanner_errposition(location, yyscanner);
	return pg_scanner_errposition(location, yyscanner);
}

void
scanner_yyerror(const char *message, core_yyscan_t yyscanner)
{
	if (ORA_MODE)
		ora_scanner_yyerror(message, yyscanner);
    else
        pg_scanner_yyerror(message, yyscanner);
}

char *
upcase_truncate_identifier(const char *ident, int len, bool warn)
{
	return upcase_identifier_internal(ident, len, warn, true);
}

/*
 * Upper the identifier and truncate it to NAMEDATALEN without warning.
 *
 * NOTICE: The function will truncate the ident to NAMEDATALEN in default.
 */
char *
upcase_identifier(const char *ident, int len)
{
	return upcase_identifier_internal(ident, len, false, true);
}

/*
 * Upper the identifier in opentenbase_ora mode.
 */
static char *
upcase_identifier_internal(const char *ident, int len, bool warn, bool truncate)
{
	char	   *result;
	int			i;
	bool		enc_is_single_byte;

	Assert(ORA_MODE);
	result = palloc(len + 1);
	enc_is_single_byte = pg_database_encoding_max_length() == 1;

	/*
	 * SQL99 specifies Unicode-aware case normalization, which we don't yet
	 * have the infrastructure for.  Instead we use toupper() to provide a
	 * locale-aware translation.  However, there are some locales where this
	 * is not right either (eg, Turkish may do strange things with 'i' and
	 * 'I').  Our current compromise is to use toupper() for characters with
	 * the high bit set, as long as they aren't part of a multi-byte
	 * character, and use an ASCII-only upcasing for 7-bit characters.
	 */
	for (i = 0; i < len; i++)
	{
		unsigned char ch = (unsigned char) ident[i];

		if (ch >= 'a' && ch <= 'z')
			ch -= ('a' - 'A');
		else if (enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch))
			ch = toupper(ch);
		result[i] = (char) ch;
	}
	result[i] = '\0';

	if (i >= NAMEDATALEN && truncate)
		truncate_identifier(result, i, warn);

	return result;
}

/*
 * Identifier in oralce will be converted to uppercase, but we want to
 * get lowercase identifier in somecases.
 *
 * Note: Return a copy string while it contains uppercase and lowercase
 * characters.
 */
char *
get_lowercase_ora_ident_str(char *s)
{
	bool enc_is_single_byte = pg_database_encoding_max_length() == 1;
	char *d = pstrdup(s);
	int	i;

	Assert(ORA_MODE);
	for (i = 0; i < strlen(s); i++)
	{
		unsigned char ch = (unsigned char) (s[i]);

		if (ch >= 'A' && ch <= 'Z')
			d[i] = (char) (ch + 'a' - 'A');
		else if (enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch))
			d[i] = (char) tolower(ch);
		/* It contains uppercase characters and lowercase characters, keep the origin string  */
		else if ((ch >= 'a' && ch <= 'z') ||
				 (enc_is_single_byte && IS_HIGHBIT_SET(ch) && islower(ch)))
		{
			pfree(d);
			d = pstrdup(s);
			break;
		}
	}

	return d;
}

/*
 * The identifier in opentenbase_ora is uppercase, but sometimes we want to compare
 * with lowercase string. We can't use pg_strcasecmp, because it will return 0
 * while s1 is "AbC" and s2 is "abc". So we need a new function to convert
 * the opentenbase_ora identifier to lowercase if its characters are all uppercase and
 * compare with the constant string.
 * It will return 0 while ora_ident like ABC and const_str like abc or
 * ora_ident is same with const_str.
 */
int
downcase_ora_ident_strcmp(const char *ora_ident, const char *const_str)
{
	char *lowercase_ident;
	int result;

	if (!ORA_MODE)
		return strcmp(ora_ident, const_str);

	lowercase_ident = get_lowercase_ora_ident_str((char *) ora_ident);
	result = strcmp(lowercase_ident, const_str);
	pfree(lowercase_ident);
	return result;
}
