#include "postgres.h"
#include "funcapi.h"
#include "assert.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/catcache.h"
#include "catalog/namespace.h"

#if PG_VERSION_NUM >= 120000

#include "catalog/pg_namespace_d.h"

#endif

#include "ctype.h"
#include "string.h"
#include "opentenbase_ora.h"
#include "builtins.h"

#if PG_VERSION_NUM >=  100000

#include "utils/regproc.h"

#endif


PG_FUNCTION_INFO_V1(dbms_assert_enquote_literal);
PG_FUNCTION_INFO_V1(dbms_assert_enquote_name);
PG_FUNCTION_INFO_V1(dbms_assert_noop);
PG_FUNCTION_INFO_V1(dbms_assert_qualified_sql_name);
PG_FUNCTION_INFO_V1(dbms_assert_schema_name);
PG_FUNCTION_INFO_V1(dbms_assert_simple_sql_name);
PG_FUNCTION_INFO_V1(dbms_assert_object_name);


#define CUSTOM_EXCEPTION(code, msg) \
	ereport(ERROR, \
		(errcode(ERRCODE_ORA_PACKAGES_##code), \
		 errmsg(msg)))

#define INVALID_QUOTE_LITERAL_EXCEPTION()	\
	CUSTOM_EXCEPTION(INVALID_QUOTE_LITERAL, "numeric or value error")

#define INVALID_SCHEMA_NAME_EXCEPTION()	\
	CUSTOM_EXCEPTION(INVALID_SCHEMA_NAME, "invalid schema name")

#define INVALID_OBJECT_NAME_EXCEPTION() \
	CUSTOM_EXCEPTION(INVALID_OBJECT_NAME, "invalid object name")

#define ISNOT_SIMPLE_SQL_NAME_EXCEPTION() \
	CUSTOM_EXCEPTION(ISNOT_SIMPLE_SQL_NAME, "string is not simple SQL name")

#define ISNOT_QUALIFIED_SQL_NAME_EXCEPTION() \
	CUSTOM_EXCEPTION(ISNOT_QUALIFIED_SQL_NAME, "string is not qualified SQL name")

#define EMPTY_STR(str)		((VARSIZE(str) - VARHDRSZ) == 0)
#define LOCAL_SINGLE_QUOTE ('\'')
#define LOCAL_DOUBLE_QUOTE ('"')

static bool ora_enquote_literal(char *dst, const char *src, size_t len);
static bool ora_check_sql_name(char *cp, int len);
static bool ora_identifier_string(char *rawstring);
static char *ora_enquote_sql_name(char *ident, bool is_to_upper);

/*
 * enquote_literal
 * if the first character is '\'', it only checks the string including valid single quote
 * if the first character is not '\'', it adds single quote both on head and tail, 
 * and then checks the single quote valid
 */
static bool
ora_enquote_literal(char *dst, const char *src, size_t len)
{
	int sum_quotes = 0;
	int nquotes = 0;
	char	   *d = dst;
	bool not_quote = false;

	/* if the first is not single quote, it should add a single quote both on head and tail.*/
	if (*src != LOCAL_SINGLE_QUOTE)
	{
		*d++ = LOCAL_SINGLE_QUOTE;
		memcpy(d, src, len);
		d += len;
		*d = LOCAL_SINGLE_QUOTE;
	}
	else
	{
		memcpy(d, src, len);
	}

	d = dst;
	while (*d != 0 )
	{
		if (*d ==LOCAL_SINGLE_QUOTE)
		{
			if (not_quote)
				nquotes--;
			else
				nquotes++;

			not_quote = false;
			sum_quotes++;			
			d++;
			continue;
		}
		else
		{
			d++;
			not_quote = true;
			if (0 == nquotes % 2) 
				return false;
		}
	}

	if (0 != nquotes % 2)
		return false;

	if (0 != sum_quotes % 2)
		return false;

	return true;
}

/*
 * check sql is valid name
 */
static bool
ora_check_sql_name(char *cp, int len)
{
	if (*cp == LOCAL_DOUBLE_QUOTE)
	{
		for (cp++, len -= 2; len-- > 0; cp++)
		{
			/* all double quotes have to be paired */
			if (*cp == LOCAL_DOUBLE_QUOTE)
			{
				if (len-- == 0)
					return false;
				/* next char has to be quote */
				if (*cp != LOCAL_DOUBLE_QUOTE)
					return false;
			}

		}
		if (*cp != LOCAL_DOUBLE_QUOTE)
			return false;
	}
	else
	{
		/* 
		 * OpenTenBase_Ora's Rules:
		 * The name must begin with an alphabetic character. 
		 * It may contain alphanumeric characters as well as the characters _, $, and # 
		 * in the second and subsequent character positions.
		 */
		if (!isalpha(*cp))
			return false;
		
		for (; len-- > 0; cp++)
			if ((*cp >0) && !isalnum(*cp) && *cp != '_' && *cp != '$' && *cp != '#')
				return false;
	}

	return true;
}

/*
 * enquote a ident
 * if is_to_upper = true, it will translate lower charater to upper charater.
 */
static char * 
ora_enquote_sql_name(char *ident, bool is_to_upper)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter or underscore
	 * and contains only lowercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int 		nquotes = 0;
	char		ch;
	char *ptr;
	char	   *result;
	char	   *optr;
	
	for (ptr = ident; *ptr; ptr++)
	{
		ch = *ptr;
		if (ch == LOCAL_DOUBLE_QUOTE)
			nquotes++;
	}

	result = (char *) palloc(strlen(ident) + nquotes + 2 + 1);

	optr = result;
	*optr++ = LOCAL_DOUBLE_QUOTE;
	for (ptr = ident; *ptr; ptr++)
	{
		ch = *ptr;
		if (ch == LOCAL_DOUBLE_QUOTE)
			*optr++ = LOCAL_DOUBLE_QUOTE;
		else if (is_to_upper && (ch >= 'a') && (ch <= 'z'))
			ch = toupper(ch);

		*optr++ = ch;
	}

	*optr++ = LOCAL_DOUBLE_QUOTE;
	*optr = '\0';

	return result;
}

/*
 * Procedure ora_identifier_string is based on SplitIdentifierString
 * from varlena.c. We need different behave of quote symbol evaluation.
 */
static bool
ora_identifier_string(char *rawstring)
{
	char	   *nextp = rawstring;
	bool		done = false;

	while (isspace((unsigned char) *nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return false;			/* not allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;

		if (*nextp == LOCAL_DOUBLE_QUOTE)
		{
			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, LOCAL_DOUBLE_QUOTE);
				if (endp == NULL)
					return false;		/* mismatched quotes */
				if (endp[1] != LOCAL_DOUBLE_QUOTE)
					break;		/* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			curname = nextp;
			if (!isalpha(*curname))
				return false;
			
			while (*nextp && *nextp != '.' &&
				   !isspace((unsigned char) *nextp))
			{
				if ((*nextp > 0) && !isalnum(*nextp) && *nextp != '_' && 
					*nextp != '$' && *nextp != '#')
				{
					return false;
				}
				nextp++;
			}
			endp = nextp;
			if (curname == nextp)
				return false;	/* empty unquoted name not allowed */
		}

		while (isspace((unsigned char) *nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == '.')
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}

static void 
ora_object_name_tolower(char * obj_name)
{
	char *str = obj_name;
	while (*str)
	{
		if ((*str >= 'A') && (*str <= 'Z'))
		{
			*str = pg_tolower(*str);
		}
		str++;
	}
}

/****************************************************************
 * DBMS_ASSERT.ENQUOTE_LITERAL
 *
 * Syntax:
 *   FUNCTION ENQUOTE_LITERAL(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Add leading and trailing quotes, verify that all single quotes
 *   are paired with adjacent single quotes.
 *
 ****************************************************************/
Datum
dbms_assert_enquote_literal(PG_FUNCTION_ARGS)
{
	text	   *t = PG_GETARG_TEXT_PP(0);	
	char *ptr = NULL;
	char	 *optr = NULL;
	text *result = NULL;
	int len = 0;
	
	optr = text_to_cstring(t);
	len = strlen(optr);

	/*the source string lenth + head and tail single quote + last terminator*/
	ptr = palloc0(sizeof(char) * len + 2 + 1);

	if (!ora_enquote_literal(ptr, optr, len))
		INVALID_QUOTE_LITERAL_EXCEPTION();

	result = cstring_to_text(ptr);
	pfree(optr);
	pfree(ptr);

	PG_RETURN_TEXT_P(result);
	//PG_RETURN_DATUM(DirectFunctionCall1(quote_literal, PG_GETARG_DATUM(0)));
}


/****************************************************************
 * DBMS_ASSERT.ENQUOTE_NAME
 *
 * Syntax:
 *   FUNCTION ENQUOTE_NAME(str varchar) RETURNS varchar;
 *   FUNCTION ENQUOTE_NAME(str varchar, loweralize boolean := true)
 *      RETURNS varchar;
 * Purpouse:
 *   Enclose name in double quotes.
 * Atention!:
 *   On OpenTenBase_Ora is second parameter capitalize!
 *
 ****************************************************************/
Datum
dbms_assert_enquote_name(PG_FUNCTION_ARGS)
{
	bool loweralize = PG_GETARG_BOOL(1);
	Oid collation = PG_GET_COLLATION();	
	text	   *t = PG_GETARG_TEXT_PP(0);	
	char *ptr = NULL;
	char	 *optr = NULL;
	text *result = NULL;

	optr = text_to_cstring(t);
	optr = ora_enquote_sql_name(optr, true);

	if (!PG_ARGISNULL(1))
	{
		if (loweralize)
		{
			ptr = optr;
			optr = str_tolower(optr, strlen(optr), collation);
			pfree(ptr);
		}
	}

	result = cstring_to_text(optr);
	pfree(optr);

	PG_RETURN_TEXT_P(result);
}


/****************************************************************
 * DBMS_ASSERT.NOOP
 *
 * Syntax:
 *   FUNCTION NOOP(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Returns value without any checking.
 *
 ****************************************************************/

Datum
dbms_assert_noop(PG_FUNCTION_ARGS)
{
	text *str = PG_GETARG_TEXT_P(0);

	PG_RETURN_TEXT_P(TextPCopy(str));
}


/****************************************************************
 * DBMS_ASSERT.QUALIFIED_SQL_NAME
 *
 * Syntax:
 *   FUNCTION QUALIFIED_SQL_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   This function verifies that the input string is qualified SQL
 *   name.
 * Exception: 44004 string is not a qualified SQL name
 *
 ****************************************************************/

Datum
dbms_assert_qualified_sql_name(PG_FUNCTION_ARGS)
{
	text *qname;

	if (PG_ARGISNULL(0))
		ISNOT_QUALIFIED_SQL_NAME_EXCEPTION();

	qname = PG_GETARG_TEXT_P(0);
	if (EMPTY_STR(qname))
		ISNOT_QUALIFIED_SQL_NAME_EXCEPTION();

	if (!ora_identifier_string(text_to_cstring(qname)))
		ISNOT_QUALIFIED_SQL_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(qname);
}


/****************************************************************
 * DBMS_ASSERT.SCHEMA_NAME
 *
 * Syntax:
 *   FUNCTION SCHEMA_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Function verifies that input string is an existing schema
 *   name.
 * Exception: 44001 Invalid schema name
 *
 ****************************************************************/

Datum
dbms_assert_schema_name(PG_FUNCTION_ARGS)
{
	Oid			namespaceId;
	AclResult	aclresult;
	text *sname;
	char *nspname;
	List	*names;

	if (PG_ARGISNULL(0))
		INVALID_SCHEMA_NAME_EXCEPTION();

	sname = PG_GETARG_TEXT_P(0);
	if (EMPTY_STR(sname))
		INVALID_SCHEMA_NAME_EXCEPTION();

	nspname = text_to_cstring(sname);
	names = stringToQualifiedNameList(nspname);
	if (list_length(names) != 1)
		INVALID_SCHEMA_NAME_EXCEPTION();

#if PG_VERSION_NUM >= 120000

	namespaceId = GetSysCacheOid(NAMESPACENAME, Anum_pg_namespace_oid,
							CStringGetDatum(strVal(linitial(names))),
							0, 0, 0);

#else

	namespaceId = GetSysCacheOid(NAMESPACENAME,
							CStringGetDatum(strVal(linitial(names))),
							0, 0, 0);

#endif

	if (!OidIsValid(namespaceId))
		INVALID_SCHEMA_NAME_EXCEPTION();

	aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		INVALID_SCHEMA_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(sname);
}


/****************************************************************
 * DBMS_ASSERT.SIMPLE_SQL_NAME
 *
 * Syntax:
 *   FUNCTION SIMPLE_SQL_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   This function verifies that the input string is simple SQL
 *   name.
 * Exception: 44003 String is not a simple SQL name
 *
 ****************************************************************/
Datum
dbms_assert_simple_sql_name(PG_FUNCTION_ARGS)
{
	text  *sname;
	int		len;
	char *cp;

	if (PG_ARGISNULL(0))
		ISNOT_SIMPLE_SQL_NAME_EXCEPTION();

	sname = PG_GETARG_TEXT_P(0);
	if (EMPTY_STR(sname))
		ISNOT_SIMPLE_SQL_NAME_EXCEPTION();

	len = VARSIZE(sname) - VARHDRSZ;
	cp = VARDATA(sname);

	if (!ora_check_sql_name(cp, len))
		ISNOT_SIMPLE_SQL_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(sname);
}


/****************************************************************
 * DBMS_ASSERT.SQL_OBJECT_NAME
 *
 * Syntax:
 *   FUNCTION SQL_OBJECT_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Verifies that input string is qualified SQL identifier of
 *   an existing SQL object.
 * Exception: 44002 Invalid object name
 *
 ****************************************************************/

Datum
dbms_assert_object_name(PG_FUNCTION_ARGS)
{
	List	*names;
	text	*str;
	char	*object_name;
	Oid 		obj_oid;
	CatCList   *catlist;
	int n_member = 0;

	if (PG_ARGISNULL(0))
		INVALID_OBJECT_NAME_EXCEPTION();

	str = PG_GETARG_TEXT_P(0);
	if (EMPTY_STR(str))
		INVALID_OBJECT_NAME_EXCEPTION();

	object_name = text_to_cstring(str);
	ora_object_name_tolower(object_name);

	names = stringToQualifiedNameList(object_name);

	/*is this a relation object*/
	obj_oid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true);
	if (OidIsValid(obj_oid))
		PG_RETURN_TEXT_P(str);

	/**is this a namespace*/
	obj_oid = LookupNamespaceNoError(object_name);
	if (OidIsValid(obj_oid))
		PG_RETURN_TEXT_P(str);

	/*is this a function or procedure*/
	catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(object_name));
	n_member = catlist->n_members;
	ReleaseSysCacheList(catlist);
	
	if (n_member > 0)
		PG_RETURN_TEXT_P(str);
	
	INVALID_OBJECT_NAME_EXCEPTION();
}
