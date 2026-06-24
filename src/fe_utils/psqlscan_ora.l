%top{
/*-------------------------------------------------------------------------
 *
 * psqlscan_ora.l
 *	  lexical scanner for SQL commands
 *
 * This lexer used to be part of psql, and that heritage is reflected in
 * the file name as well as function and typedef names, though it can now
 * be used by other frontend programs as well.  It's also possible to extend
 * this lexer with a compatible add-on lexer to handle program-specific
 * backslash commands.
 *
 * This code is mainly concerned with determining where the end of a SQL
 * statement is: we are looking for semicolons that are not within quotes,
 * comments, or parentheses.  The most reliable way to handle this is to
 * borrow the backend's flex lexer rules, lock, stock, and barrel.  The rules
 * below are (except for a few) the same as the backend's, but their actions
 * are just ECHO whereas the backend's actions generally do other things.
 *
 * XXX The rules in this file must be kept in sync with the backend lexer!!!
 *
 * XXX Avoid creating backtracking cases --- see the backend lexer for info.
 *
 * See psqlscan_int.h for additional commentary.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/fe_utils/psqlscan_ora.l
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "fe_utils/psqlscan.h"

#include "libpq-fe.h"

#include <ctype.h>

#include <string.h>
}

%{
#include "fe_utils/psqlscan_int.h"

/*
 * We must have a typedef YYSTYPE for yylex's first argument, but this lexer
 * doesn't presently make use of that argument, so just declare it as int.
 */
typedef int YYSTYPE;

/*
 * Set the type of yyextra; we use it as a pointer back to the containing
 * PsqlScanState.
 */
#define YY_EXTRA_TYPE PsqlScanState


/* Return values from yylex() */
#define LEXRES_EOL			0	/* end of input */
#define LEXRES_SEMI			1	/* command-terminating semicolon found */
#define LEXRES_BACKSLASH	2	/* backslash command start */


/*
 * Work around a bug in flex 2.5.35: it emits a couple of functions that
 * it forgets to emit declarations for.  Since we use -Wmissing-prototypes,
 * this would cause warnings.  Providing our own declarations should be
 * harmless even when the bug gets fixed.
 */
extern int	psql_yyget_column(yyscan_t yyscanner);
extern void psql_yyset_column(int column_no, yyscan_t yyscanner);
static void ora_psqlscan_emit(PsqlScanState state, const char *txt, int len, int yystart);
static bool check_if_opentenbase_ora_plsql_clause(PsqlScanState state);
static void ora_psqlscan_escape_variable(PsqlScanState state, const char *txt, int len,
						 PsqlScanQuoteType quote);

#define ECHO ora_psqlscan_emit(cur_state, yytext, yyleng, YY_START)

%}

%option reentrant
%option bison-bridge
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="ora_psql_yy"

/*
 * All of the following definitions and rules should exactly match
 * src/backend/parser/scan.l so far as the flex patterns are concerned.
 * The rule bodies are just ECHO as opposed to what the backend does,
 * however.  (But be sure to duplicate code that affects the lexing process,
 * such as BEGIN() and yyless().)  Also, psqlscan uses a single <<EOF>> rule
 * whereas scan.l has a separate one for each exclusive state.
 */

/*
 * OK, here is a short description of lex/flex rules behavior.
 * The longest pattern which matches an input string is always chosen.
 * For equal-length patterns, the first occurring in the rules list is chosen.
 * INITIAL is the starting state, to which all non-conditional rules apply.
 * Exclusive states change parsing rules while the state is active.  When in
 * an exclusive state, only those rules defined for that state apply.
 *
 * We use exclusive states for quoted strings, extended comments,
 * and to eliminate parsing troubles for numeric strings.
 * Exclusive states:
 *  <xb> bit string literal
 *  <xc> extended C-style comments
 *  <xd> delimited identifiers (double-quoted identifiers)
 *  <xh> hexadecimal numeric string
 *  <xq> standard quoted strings
 *  <xe> extended quoted strings (support backslash escape sequences)
 *  <xdolq> $foo$ quoted strings
 *  <xui> quoted identifier with Unicode escapes
 *  <xuiend> end of a quoted identifier with Unicode escapes, UESCAPE can follow
 *  <xus> quoted string with Unicode escapes
 *  <xusend> end of a quoted string with Unicode escapes, UESCAPE can follow
 *  <xoraq> opentenbase_ora compatible: q'[ ]' quoted strings
 *
 * Note: we intentionally don't mimic the backend's <xeu> state; we have
 * no need to distinguish it from <xe> state, and no good way to get out
 * of it in error cases.  The backend just throws yyerror() in those
 * cases, but that's not an option here.
 */

%x xb
%x xc
%x xd
%x xh
%x xe
%x xq
%x xdolq
%x xui
%x xuiend
%x xus
%x xusend
%x xoraq

/*
 * In order to make the world safe for Windows and Mac clients as well as
 * Unix ones, we accept either \n or \r as a newline.  A DOS-style \r\n
 * sequence will be seen as two successive newlines, but that doesn't cause
 * any problems.  Comments that start with -- and extend to the next
 * newline are treated as equivalent to a single whitespace character.
 *
 * NOTE a fine point: if there is no newline following --, we will absorb
 * everything to the end of the input as a comment.  This is correct.  Older
 * versions of Postgres failed to recognize -- as a comment if the input
 * did not end with a newline.
 *
 * XXX perhaps \f (formfeed) should be treated as a newline as well?
 *
 * XXX if you change the set of whitespace characters, fix scanner_isspace()
 * to agree, and see also the plpgsql lexer.
 */

space			[ \t\n\r\f]
horiz_space		[ \t\f]
newline			[\n\r]
non_newline		[^\n\r]

comment			("--"{non_newline}*)

whitespace		({space}+|{comment})

/*
 * SQL requires at least one newline in the whitespace separating
 * string literals that are to be concatenated.  Silly, but who are we
 * to argue?  Note that {whitespace_with_newline} should not have * after
 * it, whereas {whitespace} should generally have a * after it...
 */

special_whitespace		({space}+|{comment}{newline})
horiz_whitespace		({horiz_space}|{comment})
whitespace_with_newline	({horiz_whitespace}*{newline}{special_whitespace}*)

/*
 * To ensure that {quotecontinue} can be scanned without having to back up
 * if the full pattern isn't matched, we include trailing whitespace in
 * {quotestop}.  This matches all cases where {quotecontinue} fails to match,
 * except for {quote} followed by whitespace and just one "-" (not two,
 * which would start a {comment}).  To cover that we have {quotefail}.
 * The actions for {quotestop} and {quotefail} must throw back characters
 * beyond the quote proper.
 */
quote			'
quotestop		{quote}{whitespace}*
quotecontinue	{quote}{whitespace_with_newline}{quote}
quotefail		{quote}{whitespace}*"-"

/* Bit string
 * It is tempting to scan the string for only those characters
 * which are allowed. However, this leads to silently swallowed
 * characters if illegal characters are included in the string.
 * For example, if xbinside is [01] then B'ABCD' is interpreted
 * as a zero-length string, and the ABCD' is lost!
 * Better to pass the string forward and let the input routines
 * validate the contents.
 */
xbstart			[bB]{quote}
xbinside		[^']*

/* Hexadecimal number */
xhstart			[xX]{quote}
xhinside		[^']*

/* National character */
xnstart			[nN]{quote}

/* Quoted string that allows backslash escapes */
xestart			[eE]{quote}
xeinside		[^\\']+
xeescape		[\\][^0-7]
xeoctesc		[\\][0-7]{1,3}
xehexesc		[\\]x[0-9A-Fa-f]{1,2}
xeunicode		[\\](u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})
xeunicodefail	[\\](u[0-9A-Fa-f]{0,3}|U[0-9A-Fa-f]{0,7})

/* Extended quote
 * xqdouble implements embedded quote, ''''
 */
xqstart			{quote}
xqdouble		{quote}{quote}
xqinside		[^']+

/* $foo$ style quotes ("dollar quoting")
 * The quoted string starts with $foo$ where "foo" is an optional string
 * in the form of an identifier, except that it may not contain "$",
 * and extends to the first occurrence of an identical string.
 * There is *no* processing of the quoted text.
 *
 * {dolqfailed} is an error rule to avoid scanner backup when {dolqdelim}
 * fails to match its trailing "$".
 */
dolq_start		[A-Za-z\200-\377_]
dolq_cont		[A-Za-z\200-\377_0-9]
dolqdelim		\$({dolq_start}{dolq_cont}*)?\$
dolqfailed		\${dolq_start}{dolq_cont}*
dolqinside		[^$]+

/*
 * begin ora_compatible
 * q'[...]' style quotes:
 * opentenbase_ora Database offers the ability, in both SQL and PL/SQL, to specify
 * user-defined delimiters for string literals.
 *
 * q quoted string follow this general format: q'[your string here]' where
 * "[" represents the starting delimiter, and "]" represents the ending delimiter.
 * opentenbase_ora automatically recognizes "paired" delimiters, such as [], {}, (), and <>.
 * If the starting delimiter is another character , the ending delimiter should be the same one.
 * eg. select q'!what's your name?!' from dual;
 *
 * The q quoted style string is similar to dollar quoted style in PostgreSQL.It is valid to
 * implement the q quoted style string according to the dollar quoted string's mechanism.
 */
oraq_delim	[A-Za-z0-9\~\`\@\#\$\%\^\&\*\-\_\+\=\|\\\"\:\;\?\/\,\.\'\!\<\(\[\{\>\)\]\}]
oraqstart		[qQ]{quote}{oraq_delim}
oraqfailed		[qQ]{quote}[^A-Za-z0-9\~\`\@\#\$\%\^\&\*\-\_\+\=\|\\\"\:\;\?\/\,\.\'\!\<\(\[\{\>\)\]\}]*
oraqend         {oraq_delim}{quote}
oraqinside	    [^']+
/* end ora_compatible */

/* Double quote
 * Allows embedded spaces and other special characters into identifiers.
 */
dquote			\"
xdstart			{dquote}
xdstop			{dquote}
xddouble		{dquote}{dquote}
xdinside		[^"]+

/* Unicode escapes */
uescape			[uU][eE][sS][cC][aA][pP][eE]{whitespace}*{quote}[^']{quote}
/* error rule to avoid backup */
uescapefail		[uU][eE][sS][cC][aA][pP][eE]{whitespace}*"-"|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*{quote}[^']|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*{quote}|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*|[uU][eE][sS][cC][aA][pP]|[uU][eE][sS][cC][aA]|[uU][eE][sS][cC]|[uU][eE][sS]|[uU][eE]|[uU]

/* Quoted identifier with Unicode escapes */
xuistart		[uU]&{dquote}

/* Quoted string with Unicode escapes */
xusstart		[uU]&{quote}

/* Optional UESCAPE after a quoted string or identifier with Unicode escapes. */
xustop1		{uescapefail}?
xustop2		{uescape}

/* error rule to avoid backup */
xufailed		[uU]&


/* C-style comments
 *
 * The "extended comment" syntax closely resembles allowable operator syntax.
 * The tricky part here is to get lex to recognize a string starting with
 * slash-star as a comment, when interpreting it as an operator would produce
 * a longer match --- remember lex will prefer a longer match!  Also, if we
 * have something like plus-slash-star, lex will think this is a 3-character
 * operator whereas we want to see it as a + operator and a comment start.
 * The solution is two-fold:
 * 1. append {op_chars}* to xcstart so that it matches as much text as
 *    {operator} would. Then the tie-breaker (first matching rule of same
 *    length) ensures xcstart wins.  We put back the extra stuff with yyless()
 *    in case it contains a star-slash that should terminate the comment.
 * 2. In the operator rule, check for slash-star within the operator, and
 *    if found throw it back with yyless().  This handles the plus-slash-star
 *    problem.
 * Dash-dash comments have similar interactions with the operator rule.
 */
xcstart			\/\*{op_chars}*
xcstop			\*+\/
xcinside		[^*/]+

digit			[0-9]
ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$]

identifier		{ident_start}{ident_cont}*

/* Assorted special-case operators and operator-like tokens */
typecast		"::"
dot_dot			\.\.
colon_equals	":="
equals_greater	"=>"
less_equals		"<="
greater_equals	">="
less_greater	"<>"
not_equals		"!="
concatenation	"||"

/*
 * "self" is the set of chars that should be returned as single-character
 * tokens.  "op_chars" is the set of chars that can make up "Op" tokens,
 * which can be one or more characters long (but if a single-char token
 * appears in the "self" set, it is not to be returned as an Op).  Note
 * that the sets overlap, but each has some chars that are not in the other.
 *
 * If you change either set, adjust the character lists appearing in the
 * rule for "operator"!
 */
self			[,()\[\].;\:\+\-\*\/\%\^\<\>\=]
op_chars		[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]
operator		{op_chars}+

/* we no longer allow unary minus in numbers.
 * instead we pass it separately to parser. there it gets
 * coerced via doNegate() -- Leon aug 20 1999
 *
 * {decimalfail} is used because we would like "1..10" to lex as 1, dot_dot, 10.
 *
 * {realfail1} and {realfail2} are added to prevent the need for scanner
 * backup when the {real} rule fails to match completely.
 */

integer			{digit}+
decimal			(({digit}*\.{digit}+)|({digit}+\.{digit}*))
decimalfail		{digit}+\.\.
real			({integer}|{decimal})[Ee][-+]?{digit}+
realfail1		({integer}|{decimal})[Ee]
realfail2		({integer}|{decimal})[Ee][-+]

param			\${integer}

/* psql-specific: characters allowed in variable names */
variable_char	[A-Za-z\200-\377_0-9]

other			.

/*
 * Dollar quoted strings are totally opaque, and no escaping is done on them.
 * Other quoted strings must allow some special characters such as single-quote
 *  and newline.
 * Embedded single-quotes are implemented both in the SQL standard
 *  style of two adjacent single quotes "''" and in the Postgres/Java style
 *  of escaped-quote "\'".
 * Other embedded escaped characters are matched explicitly and the leading
 *  backslash is dropped from the string.
 * Note that xcstart must appear before operator, as explained above!
 *  Also whitespace (comment) must appear before operator.
 */

%%

%{
		/* Declare some local variables inside yylex(), for convenience */
		PsqlScanState cur_state = yyextra;
		PQExpBuffer output_buf = cur_state->output_buf;

		/*
		 * Force flex into the state indicated by start_state.  This has a
		 * couple of purposes: it lets some of the functions below set a new
		 * starting state without ugly direct access to flex variables, and it
		 * allows us to transition from one flex lexer to another so that we
		 * can lex different parts of the source string using separate lexers.
		 */
		BEGIN(cur_state->start_state);
%}

{whitespace}	{
					/*
					 * Note that the whitespace rule includes both true
					 * whitespace and single-line ("--" style) comments.
					 * We suppress whitespace at the start of the query
					 * buffer.  We also suppress all single-line comments,
					 * which is pretty dubious but is the historical
					 * behavior.
					 */
					if (output_buf->len != 0 &&
						(yytext[0] != '-' || cur_state->send_extra_lines))
						ECHO;
				}

{xcstart}		{
					cur_state->xcdepth = 0;
					BEGIN(xc);
					/* Put back any characters past slash-star; see above */
					yyless(2);
					ECHO;
				}

<xc>{xcstart}	{
					cur_state->xcdepth++;
					/* Put back any characters past slash-star; see above */
					yyless(2);
					ECHO;
				}

<xc>{xcstop}	{
					BEGIN(INITIAL);
					ECHO;
				}

<xc>{xcinside}	{
					ECHO;
				}

<xc>{op_chars}	{
					ECHO;
				}

<xc>\*+			{
					ECHO;
				}

{xbstart}		{
					BEGIN(xb);
					ECHO;
				}
<xb>{quotestop}	|
<xb>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					ECHO;
				}
<xh>{xhinside}	|
<xb>{xbinside}	{
					ECHO;
				}
<xh>{quotecontinue}	|
<xb>{quotecontinue}	{
					ECHO;
				}

{xhstart}		{
					/* Hexadecimal bit type.
					 * At some point we should simply pass the string
					 * forward to the parser and label it there.
					 * In the meantime, place a leading "x" on the string
					 * to mark it for the input routine as a hex string.
					 */
					BEGIN(xh);
					ECHO;
				}
<xh>{quotestop}	|
<xh>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					ECHO;
				}

{xnstart}		{
					yyless(1);	/* eat only 'n' this time */
					ECHO;
				}

{xqstart}		{
					if (cur_state->std_strings)
						BEGIN(xq);
					else
						BEGIN(xe);
					ECHO;
				}
{xestart}		{
					BEGIN(xe);
					ECHO;
				}
{xusstart}		{
					BEGIN(xus);
					ECHO;
				}
<xq,xe>{quotestop}	|
<xq,xe>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					ECHO;
				}
<xus>{quotestop} |
<xus>{quotefail} {
					/* throw back all but the quote */
					yyless(1);
					BEGIN(xusend);
					ECHO;
				}
<xusend>{whitespace} {
					ECHO;
				}
<xusend>{other} |
<xusend>{xustop1} {
					yyless(0);
					BEGIN(INITIAL);
					ECHO;
				}
<xusend>{xustop2} {
					BEGIN(INITIAL);
					ECHO;
				}
<xq,xe,xus>{xqdouble} {
					ECHO;
				}
<xq,xus>{xqinside}  {
					ECHO;
				}
<xe>{xeinside}  {
					ECHO;
				}
<xe>{xeunicode} {
					ECHO;
				}
<xe>{xeunicodefail}	{
					ECHO;
				}
<xe>{xeescape}  {
					ECHO;
				}
<xe>{xeoctesc}  {
					ECHO;
				}
<xe>{xehexesc}  {
					ECHO;
				}
<xq,xe,xus>{quotecontinue} {
					ECHO;
				}
<xe>.			{
					/* This is only needed for \ just before EOF */
					ECHO;
				}

{dolqdelim}		{
					cur_state->dolqstart = pg_strdup(yytext);
					BEGIN(xdolq);
					ECHO;
				}
{dolqfailed}	{
					/* throw back all but the initial "$" */
					yyless(1);
					ECHO;
				}
<xdolq>{dolqdelim} {
					if (strcmp(yytext, cur_state->dolqstart) == 0)
					{
						free(cur_state->dolqstart);
						cur_state->dolqstart = NULL;
						BEGIN(INITIAL);
					}
					else
					{
						/*
						 * When we fail to match $...$ to dolqstart, transfer
						 * the $... part to the output, but put back the final
						 * $ for rescanning.  Consider $delim$...$junk$delim$
						 */
						yyless(yyleng - 1);
					}
					ECHO;
				}
<xdolq>{dolqinside} {
					ECHO;
				}
<xdolq>{dolqfailed} {
					ECHO;
				}
<xdolq>.		{
					/* This is only needed for $ inside the quoted text */
					ECHO;
				}
	/* begin ora_compatible: q quoted string rules */
{oraqstart}		{
					cur_state->oraqstart = pg_strdup(yytext);
					BEGIN(xoraq);
					ECHO;
				}
{oraqfailed}	{
					/* throw back all but the initial "q/Q" */
					yyless(1);
					ECHO;
				}
<xoraq>{oraqend} {
					bool match = false;

					switch (cur_state->oraqstart[OPENTENBASE_ORA_QQUOTE_DELIM_POS])
					{
						case '(':
							if (yytext[0] == ')')
								match = true;
							break;
						case '{':
							if (yytext[0] == '}')
								match = true;
							break;
						case '<':
							if (yytext[0] == '>')
								match = true;
							break;
						case '[':
							if (yytext[0] == ']')
								match = true;
							break;
						default:
							if (yytext[0] == cur_state -> oraqstart[OPENTENBASE_ORA_QQUOTE_DELIM_POS])
								match = true;

					}
					if (match)
					{
						free(cur_state->oraqstart);
						cur_state->oraqstart = NULL;
						BEGIN(INITIAL);
					}
					else
					{
						/*
						 * When we fail to match ...' to oraqstart, transfer
						 * the character to the output, but put back the final
						 * ' for rescanning.  Consider q''...F''
						 */
						yyless(yyleng - 1);
					}
					ECHO;
				}
<xoraq>{oraqinside} {
					if (yyleng > 1)
						yyless(yyleng - 1);
					ECHO;
				}
<xoraq>.		{
					/* This is only needed for ' inside the quoted text */
					ECHO;
				}
	/* end ora_compatible */
{xdstart}		{
					BEGIN(xd);
					ECHO;
				}
{xuistart}		{
					BEGIN(xui);
					ECHO;
				}
<xd>{xdstop}	{
					BEGIN(INITIAL);
					ECHO;
				}
<xui>{dquote} {
					yyless(1);
					BEGIN(xuiend);
					ECHO;
				}
<xuiend>{whitespace} {
					ECHO;
				}
<xuiend>{other} |
<xuiend>{xustop1} {
					yyless(0);
					BEGIN(INITIAL);
					ECHO;
				}
<xuiend>{xustop2}	{
					BEGIN(INITIAL);
					ECHO;
				}
<xd,xui>{xddouble}	{
					ECHO;
				}
<xd,xui>{xdinside}	{
					ECHO;
				}

{xufailed}	{
					/* throw back all but the initial u/U */
					yyless(1);
					ECHO;
				}

{typecast}		{
					ECHO;
				}

{dot_dot}		{
					ECHO;
				}

{colon_equals}	{
					ECHO;
				}

{equals_greater} {
					ECHO;
				}

{less_equals}	{
					ECHO;
				}

{greater_equals} {
					ECHO;
				}

{less_greater}	{
					ECHO;
				}

{not_equals}	{
					ECHO;
				}

{concatenation}	{
					ECHO;
				}

	/*
	 * These rules are specific to psql --- they implement parenthesis
	 * counting and detection of command-ending semicolon.  These must
	 * appear before the {self} rule so that they take precedence over it.
	 */

"("				{
					cur_state->paren_depth++;
					ECHO;
				}

")"				{
					if (cur_state->paren_depth > 0)
						cur_state->paren_depth--;
					ECHO;
				}

";"				{
					ECHO;
					if (cur_state->paren_depth == 0)
					{
						/* Terminate lexing temporarily */
						cur_state->start_state = YY_START;

						/* Countine to next toke for WITH FUNCTION */
						if (!check_if_opentenbase_ora_plsql_clause(cur_state))
							return LEXRES_SEMI;
					}
				}

	/*
	 * psql-specific rules to handle backslash commands and variable
	 * substitution.  We want these before {self}, also.
	 */

"\\"[;:]		{
					/* Force a semicolon or colon into the query buffer */
					ora_psqlscan_emit(cur_state, yytext + 1, 1, -1);
				}

"\\"			{
					/* Terminate lexing temporarily */
					cur_state->start_state = YY_START;
					return LEXRES_BACKSLASH;
				}

:{variable_char}+	{
					/* Possible psql variable substitution */
					char	   *varname;
					char	   *value;

					varname = psqlscan_extract_substring(cur_state,
														 yytext + 1,
														 yyleng - 1);
					if (cur_state->callbacks->get_variable)
						value = cur_state->callbacks->get_variable(varname,
																   PQUOTE_PLAIN,
																   cur_state->cb_passthrough);
					else
						value = NULL;

					if (value)
					{
						/* It is a variable, check for recursion */
						if (psqlscan_var_is_current_source(cur_state, varname))
						{
							/* Recursive expansion --- don't go there */
							cur_state->callbacks->write_error("skipping recursive expansion of variable \"%s\"\n",
															  varname);
							/* Instead copy the string as is */
							ECHO;
						}
						else
						{
							/* OK, perform substitution */
							psqlscan_push_new_buffer(cur_state, value, varname);
							/* yy_scan_string already made buffer active */
						}
						free(value);
					}
					else
					{
						/*
						 * if the variable doesn't exist we'll copy the string
						 * as is
						 */
						ECHO;
					}

					free(varname);
				}

:'{variable_char}+'	{
					ora_psqlscan_escape_variable(cur_state, yytext, yyleng,
											 PQUOTE_SQL_LITERAL);
				}

:\"{variable_char}+\"	{
					ora_psqlscan_escape_variable(cur_state, yytext, yyleng,
											 PQUOTE_SQL_IDENT);
				}

	/*
	 * These rules just avoid the need for scanner backup if one of the
	 * two rules above fails to match completely.
	 */

:'{variable_char}*	{
					/* Throw back everything but the colon */
					yyless(1);
					ECHO;
				}

:\"{variable_char}*	{
					/* Throw back everything but the colon */
					yyless(1);
					ECHO;
				}

	/*
	 * Back to backend-compatible rules.
	 */

{self}			{
					ECHO;
				}

{operator}		{
					/*
					 * Check for embedded slash-star or dash-dash; those
					 * are comment starts, so operator must stop there.
					 * Note that slash-star or dash-dash at the first
					 * character will match a prior rule, not this one.
					 */
					int			nchars = yyleng;
					char	   *slashstar = strstr(yytext, "/*");
					char	   *dashdash = strstr(yytext, "--");

					if (slashstar && dashdash)
					{
						/* if both appear, take the first one */
						if (slashstar > dashdash)
							slashstar = dashdash;
					}
					else if (!slashstar)
						slashstar = dashdash;
					if (slashstar)
						nchars = slashstar - yytext;

					/*
					 * For SQL compatibility, '+' and '-' cannot be the
					 * last char of a multi-char operator unless the operator
					 * contains chars that are not in SQL operators.
					 * The idea is to lex '=-' as two operators, but not
					 * to forbid operator names like '?-' that could not be
					 * sequences of SQL operators.
					 */
					while (nchars > 1 &&
						   (yytext[nchars - 1] == '+' ||
							yytext[nchars - 1] == '-'))
					{
						int			ic;

						for (ic = nchars - 2; ic >= 0; ic--)
						{
							if (strchr("~!@#^&|`?%", yytext[ic]))
								break;
						}
						if (ic >= 0)
							break; /* found a char that makes it OK */
						nchars--; /* else remove the +/-, and check again */
					}

					if (nchars < yyleng)
					{
						/* Strip the unwanted chars from the token */
						yyless(nchars);
					}
					ECHO;
				}

{param}			{
					ECHO;
				}

{integer}		{
					ECHO;
				}
{decimal}		{
					ECHO;
				}
{decimalfail}	{
					/* throw back the .., and treat as integer */
					yyless(yyleng - 2);
					ECHO;
				}
{real}			{
					ECHO;
				}
{realfail1}		{
					/*
					 * throw back the [Ee], and treat as {decimal}.  Note
					 * that it is possible the input is actually {integer},
					 * but since this case will almost certainly lead to a
					 * syntax error anyway, we don't bother to distinguish.
					 */
					yyless(yyleng - 1);
					ECHO;
				}
{realfail2}		{
					/* throw back the [Ee][+-], and proceed as above */
					yyless(yyleng - 2);
					ECHO;
				}


{identifier}	{
					ECHO;
				}

{other}			{
					ECHO;
				}

<<EOF>>			{
					if (cur_state->buffer_stack == NULL)
					{
						cur_state->start_state = YY_START;
						return LEXRES_EOL;		/* end of input reached */
					}

					/*
					 * We were expanding a variable, so pop the inclusion
					 * stack and keep lexing
					 */
					psqlscan_pop_buffer_stack(cur_state);
					psqlscan_select_top_buffer(cur_state);
				}

%%

/*
 * check_if_opentenbase_ora_plsql_clause
 *    Check if it is a plsql block, and the result is cached in slash_end.
 */
static bool
check_if_opentenbase_ora_plsql_clause(PsqlScanState state)
{
	if (state->non_quote_pos == 0)
		return false;

	if (state->is_plpgsql)
		return false;

	if (state->slash_end)
	{
		/* but, if we found an "execute" then it's still plpgsql as always */
		if (state->is_trigger_proc)
		{
			int j = 0;

			for (; j < state->non_quote_pos; j++)
			{
				if (state->non_quote_tokens[j] == BEGIN_P)
					return true; /* no chages. ie. not a TRIGGER..EXECUTE proc_trig.. */

				if (state->non_quote_tokens[j] == EXECUTE_P)
				{
					state->is_trigger_proc = false;
					state->slash_end = false;
					state->is_plpgsql = true;
					return false;
				}
			}
		}
		/* Force to a slash end */
		return true;
	}

	if (state->non_quote_tokens[0] != CREATE_P &&
			state->non_quote_tokens[0] != EXPLAIN_P &&
			state->non_quote_tokens[0] != WITH_P)
		return false;

	if (state->non_quote_tokens[0] == CREATE_P)
	{
		/* Search for AS_P */
		int	i = 0;

#define IS_PL_SQL(t) ((t) == FUNCTION_P || (t) == PACKAGE_P || (t) == PROCEDURE_P ||\
						(t) == EDITIONABLE_P || (t) == NONEDITIONABLE_P || (t) == OBJECT_P)

		if (state->non_quote_pos >= 2 && state->non_quote_tokens[0] == CREATE_P &&
				(IS_PL_SQL(state->non_quote_tokens[1]) ||
				(state->non_quote_pos >= 4 && state->non_quote_tokens[1] == OR_P &&
				 state->non_quote_tokens[2] == REPLACE_P &&
				 IS_PL_SQL(state->non_quote_tokens[3]))))
			state->is_func_proc = true;

		/* we may dealing with a plsql trigger ... */
		if (state->non_quote_pos >= 2 && state->non_quote_tokens[0] == CREATE_P &&
				(state->non_quote_tokens[1] == TRIGGER_P ||
				(state->non_quote_pos >= 4 && state->non_quote_tokens[1] == OR_P &&
				 state->non_quote_tokens[2] == REPLACE_P &&
				 state->non_quote_tokens[3] == TRIGGER_P)))
			state->is_trigger_proc = true;

		for (; i < state->non_quote_pos; i++)
		{
			if (i < 1)
				continue;
			/* for type object body */
			if (state->non_quote_tokens[i] == BODY_P)
			{
				/* not "create type body xxx as" or "create or replace type body xxx as " */
				if (state->non_quote_tokens[i - 1] != TYPE_P)
				{
					return false;
				}
				else
				{
					state->slash_end = true;
					return true;
				}
			}
		}
		i = 0;

		for (; i < state->non_quote_pos; i++)
		{
			if (state->non_quote_tokens[i] == AS_P)
				break;
		}

		if (i == state->non_quote_pos || state->non_quote_pos - i < 3)
			return false;

		if (state->non_quote_tokens[i + 1] == WITH_P &&
					state->non_quote_tokens[i + 2] == FUNCTION_P)
		{
			state->slash_end = true;
			return true;
		}
	}
	else if (state->non_quote_tokens[0] == EXPLAIN_P)
	{
		/* Search for WITH */
		int	i = 0;

		for (; i < state->non_quote_pos; i++)
		{
			if (state->non_quote_tokens[i] == WITH_P)
				break;
		}

		if (i == state->non_quote_pos || state->non_quote_pos - 1 < 2)
			return false;

		if (state->non_quote_tokens[i] == WITH_P &&
					state->non_quote_tokens[i + 1] == FUNCTION_P)
		{
			state->slash_end = true;
			return true;
		}
	}
	else if (state->non_quote_tokens[0] == WITH_P &&
					state->non_quote_tokens[1] == FUNCTION_P)
	{
		state->slash_end = true;
		return true;
	}

	return false;
}
void
psql_scan_slash_reset(PsqlScanState state)
{
	state->non_quote_pos = 0;
	state->is_func_proc = false;
	state->is_trigger_proc = false;
	state->is_plpgsql = false;
	state->slash_end = false;
}

static bool
ch_is_space(char ch)
{
	if (ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r' || ch == '\f')
		return true;
	else
		return false;
}

/*
 * Only the case where the comment is first is handled
 */
char*
preprocess_comment(const char *query_string, bool *is_find)
{
	const char *begin = "/*";
	const char *end = "*/";
	const int len = 2;
	char *pos = NULL;

	if (strncmp(query_string, begin, len) != 0)
	{
		*is_find = false;
		return (char *)query_string;
	}

	pos = strstr(query_string, end);
	if (pos == NULL)
	{
		*is_find = false;
		return (char *)query_string;
	}

	pos += len;
	while (pos && *pos != '\0')
	{
		if (ch_is_space(*pos))
		{
			pos++;
		}
		else
		{
			break;
		}
	}

	*is_find = true;
	return pos;
}

/*
 * check if opentenbase_ora user function or procedure need to execute switch (state->start_state)
 * need_start_state is true : need to execute switch (state->start_state); false: do not need to execute switch (state->start_state);
 */
static bool
check_if_opentenbase_ora_func_proc(PQExpBuffer query_buf)
{
	bool need_start_state = false;
	char *tmp_char = NULL;
	size_t query_len = query_buf->len;
	int i = 0;
	bool func_go_on = false;
	bool as_go_on = false;
	bool is_find = false;

	tmp_char = preprocess_comment(query_buf->data, &is_find);
	if (is_find)
		query_len = query_buf->len - (tmp_char - query_buf->data);

	if (query_len > 6)
	{
		if (pg_strncasecmp(tmp_char + i, "create", 6) == 0)
		{
			i = i + 6;
			while (i < query_len)
			{
				if (ch_is_space(tmp_char[i]))
				{
					i++;
					continue;
				}

				if (!func_go_on && !as_go_on)
				{
					if ((i + 4 < query_len) &&
						pg_strncasecmp(tmp_char + i, "cast", 4) == 0)
					{
						if (ch_is_space(tmp_char[i - 1]) &&
							ch_is_space(tmp_char[i + 4]))
						{
							/* case: create cast */
							need_start_state = false;
							break;
						}
					}

					/* ignore package case: later will refactor sql-broken logic */
					if ((i + 7 < query_len) && pg_strncasecmp(tmp_char + i, "package", 7) == 0)
						return false;

					if ((i + 8 < query_len) &&
						pg_strncasecmp(tmp_char + i, "function", 8) == 0)
					{
						if (ch_is_space(tmp_char[i - 1]) &&
							ch_is_space(tmp_char[i + 8]))
						{
							i = i + 9;
							func_go_on = true;
							continue;
						}
					}
					else if ((i + 9 < query_len) &&
							  pg_strncasecmp(tmp_char + i, "procedure", 9) == 0)
					{
						if (ch_is_space(tmp_char[i - 1]) &&
							ch_is_space(tmp_char[i + 9]))
						{
							i = i + 10;
							func_go_on = true;
							continue;
						}
					}
				}

				if (func_go_on && !as_go_on)
				{
					if ((i + 2 < query_len) &&
						(pg_strncasecmp(tmp_char + i, "is", 2) == 0 ||
						 pg_strncasecmp(tmp_char + i, "as", 2) == 0))
					{
						if (ch_is_space(tmp_char[i - 1]) &&
							ch_is_space(tmp_char[i + 2]))
						{
							i = i + 3;
							need_start_state = true;
							as_go_on = true;
							continue;
						}
					}
				}

				if (func_go_on && as_go_on)
				{
					if (tmp_char[i] == '\'' || tmp_char[i] == '$')
						need_start_state = false;

					break;
				}

				i++;
			}
		}
	}

	return need_start_state;
}

/*
 * String comparisons ignore case and ignore extra spaces.
 */
static bool
sentence_compare_ignore_case_extrspace(const char *str, const char *base, int words)
{
#define KEYWORLD_MAX_LEN 1024
	char tmp[KEYWORLD_MAX_LEN + 1] = {0};
	const char *head = str;
	int base_len,
		idx = 0,
		cur_words = 0;
	bool new_word = true;

	if (str == NULL || base == NULL)
		return false;

	base_len = strlen(base);
	if (base_len > KEYWORLD_MAX_LEN)
		return false;

	/* step1: Ignore extra spaces.. */
	while (*head != '\0' && ch_is_space(*head))
		head++;

	/* step2: Remove excess space and turn to lowercase. */
	for (; *head != '\0'; head++)
	{
		if (ch_is_space(*head))
		{
			if (cur_words == words)
				break;

			new_word = true;
			tmp[idx++] = *head;

			while (*head != '\0' && ch_is_space(*head))
				head++;
		}

		if (*head == ';')
			break;

		if (idx > KEYWORLD_MAX_LEN)
			return false;

		if (new_word)
		{
			new_word = false;
			cur_words++;
		}
		tmp[idx++] = tolower(*head);
	}

	if (idx == base_len &&
		(strncmp(tmp, base, base_len) == 0) &&
		((ch_is_space(*head)) || *head == ';' || *head == '\0'))
		return true;
	else
		return false;
}

char*
skip_label(char *query, bool *is_find)
{
	const char *begin = "<<";
	const char *end = ">>";
	const int len = 2;
	char *pos = NULL;

   	if (query == NULL)
	{
		*is_find = false;
		return NULL;
	}

	if (strncmp(query, begin, len) != 0)
	{
		*is_find = false;
		return (char *)query;
	}

	pos = strstr(query, end);
	if (pos == NULL)
	{
		*is_find = false;
		return (char *)query;
	}

	pos += len;
	while (pos && *pos != '\0')
	{
		if (ch_is_space(*pos))
			pos++;
		else
			break;
	}

	*is_find = true;
	return pos;
}

char *
skip_comments(char *query_string, int query_len)
{
	bool is_find = false;
	char *new_query = query_string;
	int new_query_len  = query_len;

	while(new_query && new_query_len > 2)
	{
		is_find = false;
		new_query = preprocess_comment(new_query, &is_find);
		if (new_query && is_find)
			new_query_len = strlen(new_query);
		else
			break;
	}

	return new_query;
}
/*
 * check if opentenbase_ora anonymous code block need to execute switch (state->start_state)
 * need_start_state is true : need to execute switch (state->start_state); false: do not need to execute switch (state->start_state);
 */

static bool
check_if_opentenbase_ora_anonymous_block(PQExpBuffer query_buf)
{
	int i = 0;
	char *tmp_char = NULL,
		 *new_query = query_buf->data;
	char sql_start_cmd[12] = "";
	bool need_start_state = false;
	const int one_word = 1;
	const int two_words = 2;
	bool is_find = false;
	int query_len = query_buf->len;

	new_query = skip_comments(new_query, query_len);
	if (new_query)
		query_len = strlen(new_query);

	is_find = false;
	new_query = skip_label(new_query, &is_find);
	if (new_query && is_find)
		query_len = strlen(new_query);

	is_find = false;

	new_query = skip_comments(new_query, query_len);
	if (new_query)
		query_len = strlen(new_query);

	memset(sql_start_cmd, 0, 12);

	if (query_len >= 8)
	{
		strncpy(sql_start_cmd, new_query, 7);
		for (i = 0; i < 7; i++)
			sql_start_cmd[i] = tolower(sql_start_cmd[i]);

		if (strncmp(sql_start_cmd,"declare", 7) == 0)
		{
			/* check sql start: declare */
			tmp_char = new_query + 7;
		}
		else if (strncmp(sql_start_cmd, "begin", 5) == 0)
		{
			/* check sql start: begin */
			tmp_char = new_query + 5;
		}
	}
	else if (query_len >= 6)
	{
		strncpy(sql_start_cmd, new_query, 5);
		for (i = 0; i < 5; i++)
			sql_start_cmd[i] = tolower(sql_start_cmd[i]);

		if (strncmp(sql_start_cmd, "begin", 5) == 0)
		{
			/* check sql start: begin */
			tmp_char = new_query + 5;
		}
	}

	if (tmp_char)
	{
		if (ch_is_space(*tmp_char) || (*tmp_char == ';'))
		{
			while (*tmp_char != '\0')
			{
				if (ch_is_space(*tmp_char))
					tmp_char++;
				else if (*tmp_char == ';')
					break;
				else if (strncmp(sql_start_cmd, "declare", 7) == 0)
				{
					/* DECLARE _psql_cursor NO SCROLL CURSOR FOR select ... ;  --need_start_state=false
					 *
					 * OR
					 *
					 * --need_start_state=true
					 * DECLARE
					 *     ...;
					 * BEGIN
					 *     ...;
					 * END;
					 */
					while (tmp_char !=  query_buf->data + 6)
					{
						if (*tmp_char == '\n')
						{
							need_start_state = true;
							break;
						}
						tmp_char--;
					}
					break;
				}
				else if (strncmp(sql_start_cmd, "begin", 5) == 0)
				{
					int tmp_char_len = strlen(tmp_char);
					char firstchar = tolower(*tmp_char);

					/* check: BEGIN TRANSACTION ... ;*/
					if ((firstchar == 't' &&
								tmp_char_len >= 11 &&
								sentence_compare_ignore_case_extrspace(tmp_char, "transaction", one_word)) ||

							/* check: BEGIN WORK ... ; */
							(firstchar == 'w' &&
							 tmp_char_len >= 4 &&
							 sentence_compare_ignore_case_extrspace(tmp_char, "work", one_word)) ||

							/* check: BEGIN isolation level ... ; */
							(firstchar == 'i' &&
							 tmp_char_len >= 9 &&
							 sentence_compare_ignore_case_extrspace(tmp_char, "isolation", one_word)) ||

							/* check: BEGIN read/write only ... ; */
							(firstchar == 'r' &&
							 tmp_char_len >= 9 &&
							 (sentence_compare_ignore_case_extrspace(tmp_char, "read only", two_words) ||
							  sentence_compare_ignore_case_extrspace(tmp_char, "read write", two_words))) ||

							/* check: BEGIN DEFERRABLE ... ; */
							(firstchar == 'd' &&
							 tmp_char_len >= 10 &&
							 sentence_compare_ignore_case_extrspace(tmp_char, "deferrable", one_word)) ||

							/* check: BEGIN DEFERRABLE ... ; */
							(firstchar == 'n' &&
							 tmp_char_len >= 14 &&
							 sentence_compare_ignore_case_extrspace(tmp_char, "not deferrable", two_words)))
							 {
								 break;
							 }

					need_start_state = true;
					break;
				}
				else
				{
					need_start_state = true;
					break;
				}
			}
		}

	}

	return need_start_state;
}


/*
 * Do lexical analysis of SQL command text.
 *
 * The text previously passed to psql_scan_setup is scanned, and appended
 * (possibly with transformation) to query_buf.
 *
 * The return value indicates the condition that stopped scanning:
 *
 * PSCAN_SEMICOLON: found a command-ending semicolon.  (The semicolon is
 * transferred to query_buf.)  The command accumulated in query_buf should
 * be executed, then clear query_buf and call again to scan the remainder
 * of the line.
 *
 * PSCAN_BACKSLASH: found a backslash that starts a special command.
 * Any previous data on the line has been transferred to query_buf.
 * The caller will typically next apply a separate flex lexer to scan
 * the special command.
 *
 * PSCAN_INCOMPLETE: the end of the line was reached, but we have an
 * incomplete SQL command.  *prompt is set to the appropriate prompt type.
 *
 * PSCAN_EOL: the end of the line was reached, and there is no lexical
 * reason to consider the command incomplete.  The caller may or may not
 * choose to send it.  *prompt is set to the appropriate prompt type if
 * the caller chooses to collect more input.
 *
 * In the PSCAN_INCOMPLETE and PSCAN_EOL cases, psql_scan_finish() should
 * be called next, then the cycle may be repeated with a fresh input line.
 *
 * In all cases, *prompt is set to an appropriate prompt type code for the
 * next line-input operation.
 */
PsqlScanResult
psql_scan_ora(PsqlScanState state,
		  PQExpBuffer query_buf,
		  promptStatus_t *prompt)
{
	PsqlScanResult result;
	int			lexresult;
	/* begin opentenbase-ora-compatible */
	int skip_len = 0;
	bool need_end = false;
	char *tmp_char = NULL;
	bool need_start_state = false;
	/* end opentenbase-ora-compatible */

	/* Must be scanning already */
	Assert(state->scanbufhandle != NULL);

	/* Set current output target */
	state->output_buf = query_buf;

	/* Set input source */
	if (state->buffer_stack != NULL)
		ora_psql_yy_switch_to_buffer(state->buffer_stack->buf, state->scanner);
	else
		ora_psql_yy_switch_to_buffer(state->scanbufhandle, state->scanner);

	/* And lex. */
	lexresult = ora_psql_yylex(NULL, state->scanner);

	/*
	 * Check termination state and return appropriate result info.
	 */
	switch (lexresult)
	{
		case LEXRES_EOL:		/* end of input */
			switch (state->start_state)
			{
				case INITIAL:
				case xuiend:	/* we treat these like INITIAL */
				case xusend:
					if (state->paren_depth > 0)
					{
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_PAREN;
					}
					else if (query_buf->len > 0)
					{
						/* begin opentenbase-ora-compatible */
						int	slash_cnt = 0;

						skip_len = 0;
						need_end = false;
						tmp_char = query_buf->data + query_buf->len - 1;

						while (tmp_char != query_buf->data && *tmp_char != '\n')
						{
							if (ch_is_space(*tmp_char))
							{
								tmp_char--;
								skip_len++;
								continue;
							}

							if (*tmp_char == '/')
							{
								/* Move next */
								tmp_char--;
								skip_len++;

								slash_cnt++;
								if (slash_cnt == 2)
									break;
								continue;
							}

							break;
						}

						if (slash_cnt == 1 && *tmp_char == '\n')
						{
							need_end = true;
							*(tmp_char + 1) = '\0';
							query_buf->len = query_buf->len - skip_len;

							Assert(query_buf->len == strlen(query_buf->data));
						}
						else if (*tmp_char == '/' && query_buf->len == 1)
						{
							/*
							 * The first line of input has only one char '/'. We
							 * complete this command and empty the query_buf, empty
							 * command.
							 */
							need_end = true;
							*tmp_char = '\0';
							query_buf->len = 0;
 						}

						if (need_end)
						{
							result = PSCAN_SEMICOLON;
							*prompt = PROMPT_READY;
						}
						else
						{
							result = PSCAN_EOL;
							*prompt = PROMPT_CONTINUE;
						}
						/* end opentenbase-ora-compatible */
					}
					else
					{
						/* never bother to send an empty buffer */
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_READY;
					}
					break;
				case xb:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_SINGLEQUOTE;
					break;
				case xc:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_COMMENT;
					break;
				case xd:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_DOUBLEQUOTE;
					break;
				case xh:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_SINGLEQUOTE;
					break;
				case xe:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_SINGLEQUOTE;
					break;
				case xq:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_SINGLEQUOTE;
					break;
				case xdolq:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_DOLLARQUOTE;
					break;
				/* begin opentenbase-ora-compatible */
				case xoraq:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_QQUOTE;
					break;
				/* end opentenbase-ora-compatible */
				case xui:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_DOUBLEQUOTE;
					break;
				case xus:
					result = PSCAN_INCOMPLETE;
					*prompt = PROMPT_SINGLEQUOTE;
					break;
				default:
					/* can't get here */
					fprintf(stderr, "invalid YY_START\n");
					exit(1);
			}
			break;
		case LEXRES_SEMI:		/* semicolon */
			/* begin opentenbase-ora-compatible */

			need_start_state = check_if_opentenbase_ora_plsql_clause(state);

			if (!need_start_state)
				need_start_state = check_if_opentenbase_ora_func_proc(query_buf);

			if (!need_start_state)
				need_start_state = check_if_opentenbase_ora_anonymous_block(query_buf);

			if (need_start_state)
			{
				switch (state->start_state)
				{
					case INITIAL:
					case xuiend:	/* we treat these like INITIAL */
					case xusend:
						if (state->paren_depth > 0)
						{
							result = PSCAN_INCOMPLETE;
							*prompt = PROMPT_PAREN;
						}
						else if (query_buf->len > 0)
						{
							result = PSCAN_EOL;
							*prompt = PROMPT_CONTINUE;
						}
						else
						{
							/* never bother to send an empty buffer */
							result = PSCAN_INCOMPLETE;
							*prompt = PROMPT_READY;
						}
						break;
					case xb:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_SINGLEQUOTE;
						break;
					case xc:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_COMMENT;
						break;
					case xd:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_DOUBLEQUOTE;
						break;
					case xh:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_SINGLEQUOTE;
						break;
					case xe:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_SINGLEQUOTE;
						break;
					case xq:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_SINGLEQUOTE;
						break;
					case xdolq:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_DOLLARQUOTE;
						break;
					case xui:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_DOUBLEQUOTE;
						break;
					case xus:
						result = PSCAN_INCOMPLETE;
						*prompt = PROMPT_SINGLEQUOTE;
						break;
					default:
						/* can't get here */
						fprintf(stderr, "invalid YY_START\n");
						exit(1);
				}
			}
			else
			{
				result = PSCAN_SEMICOLON;
				*prompt = PROMPT_READY;
			}
			/* end opentenbase-ora-compatible */
			break;
		case LEXRES_BACKSLASH:	/* backslash */
			result = PSCAN_BACKSLASH;
			*prompt = PROMPT_READY;
			break;
		default:
			/* can't get here */
			fprintf(stderr, "invalid psql_yylex result\n");
			exit(1);
	}

	return result;
}


/*
 * ora_psqlscan_emit() --- body for ECHO macro for opentenbase_ora syntax 
 *
 * NB: this must be used for ALL and ONLY the text copied from the flex
 * input data.  If you pass it something that is not part of the yytext
 * string, you are making a mistake.  Internally generated text can be
 * appended directly to state->output_buf.
 */
void
ora_psqlscan_emit(PsqlScanState state, const char *txt, int len, int yystart)
{
	PQExpBuffer output_buf = state->output_buf;

	if (state->safe_encoding)
		appendBinaryPQExpBuffer(output_buf, txt, len);
	else
	{
		/* Gotta do it the hard way */
		const char *reference = state->refline;
		int			i;

		reference += (txt - state->curline);

		for (i = 0; i < len; i++)
		{
			char		ch = txt[i];

			if (ch == (char) 0xFF)
				ch = reference[i];
			appendPQExpBufferChar(output_buf, ch);
		}
	}

	/* ignore string and identifier */
	switch (yystart)
	{
		case INITIAL:
		case xuiend:	/* we treat these like INITIAL */
		case xusend:
			if (state->paren_depth > 0)
				return;
			break;
		case xq:
			break;
		default:
			return;
	}

	if (ch_is_space(txt[0]) || (txt[0] == '-' && txt[1] == '-' && txt[2] != 0) ||
			txt[0] == '.' || txt[0] == '"' || (txt[0] == '/' && txt[1] == '*') ||
			(txt[0] == '*' && txt[1] == '/'))
		return;

	/*
	 * Check if the first unquoted tokens are WITH FUNCTION or other kind of
	 * pl/sql structure.
	 * - WITH FUNCTION in subquery will not send to server until the subquery
	 *   is completed. So we don't do this only from the frist two tokens in
	 *   SQL command.
	 */
	if (state->non_quote_pos < MAX_UNQ_CNT)
	{
		/* Calling more expensive strcmp() */
#define ADD_TOK(c1, c2, c3, c4)	\
		if ((txt[0] == c1 || txt[0] == c2) && strcasecmp(txt, c3) == 0)	\
		{		\
			state->non_quote_tokens[state->non_quote_pos] = c4;	\
			state->non_quote_pos++;	\
		}

		ADD_TOK('c', 'C', "create", CREATE_P)
		else ADD_TOK('o', 'O', "or", OR_P)
		else ADD_TOK('r', 'R', "replace", REPLACE_P)
		else ADD_TOK('v', 'V', "view", VIEW_P)
		else ADD_TOK('e', 'E', "explain", EXPLAIN_P)
		else ADD_TOK('a', 'A', "analyze", ANALYZE_P)
		else ADD_TOK('v', 'V', "verbose", VERBOSE_P)
		else ADD_TOK('w', 'W', "with", WITH_P)
		else ADD_TOK('f', 'F', "function", FUNCTION_P)
		else ADD_TOK('a', 'A', "as", AS_P)
		else ADD_TOK('t', 'T', "table", TABLE_P)
		else ADD_TOK('p', 'P', "procedure", PROCEDURE_P)
		else ADD_TOK('p', 'P', "package", PACKAGE_P)
		else ADD_TOK('e', 'E', "editionable", EDITIONABLE_P)
		else ADD_TOK('n', 'N', "noneditionable", NONEDITIONABLE_P)
		else ADD_TOK('i', 'I', "is", IS_P)
		else ADD_TOK('t', 'T', "trigger", TRIGGER_P)
		else ADD_TOK('e', 'E', "execute", EXECUTE_P)
		else ADD_TOK('b', 'B', "begin", BEGIN_P)
		else ADD_TOK('d', 'D', "declare", DECLARE_P)
		else ADD_TOK('c', 'C', "cursor", CURSOR_P)
		else ADD_TOK('f', 'F', "for", FOR_P)
		else ADD_TOK(';', ';', ";", SEMICOLON_P)
		else ADD_TOK('o', 'O', "object", OBJECT_P)
		else ADD_TOK('t', 'T', "type", TYPE_P)
		else ADD_TOK('b', 'B', "body", BODY_P)
		else ADD_TOK('<', '<', "<<", LESS_LESS_P)
		else ADD_TOK('>', '>', ">>", GREATER_GREATER_P)
		else
		{
			state->non_quote_tokens[state->non_quote_pos] = 0;
			state->non_quote_pos++;
		}
	}

	check_if_opentenbase_ora_plsql_clause(state);
	if (state->non_quote_pos >= 2 && !state->slash_end && (txt[0] != '$' && txt[0] != '\'') && 
	        (state->is_trigger_proc || (state->is_func_proc &&
			(state->non_quote_tokens[state->non_quote_pos - 2] == AS_P ||
			 state->non_quote_tokens[state->non_quote_pos - 2] == IS_P))))
		state->slash_end = true;
	if (!state->slash_end && state->non_quote_pos > 0)
	{
		/*
		 * For: DECLARE name CURSOR FOR... and DECLARE .. BEGIN...
		 */
		int		i = 0;
		bool	found_decl = false;
		bool	has_label = false;

		/*
		 * DECLARE name [ BINARY ] [ INSENSITIVE ] [ [ NO ] SCROLL ]
		 *                  CURSOR [ { WITH | WITHOUT } HOLD ] FOR query
		 *
		 * The key word 'FOR' is at the 11th posistion. Then to look 12 tokens is enough.
		 */

#define MAX_TOKENS_LOOKUP 12

		for (; i < state->non_quote_pos && i < MAX_TOKENS_LOOKUP; i++)
		{
			if (state->non_quote_tokens[i] == SEMICOLON_P)
				goto _cs_end;
			if ((i == 0 && state->non_quote_tokens[i] == LESS_LESS_P) || 
				(i == 1 && state->non_quote_tokens[i-1] == LESS_LESS_P) ||
				(i == 2 && (state->non_quote_tokens[i-2] == LESS_LESS_P &&
							state->non_quote_tokens[i] == GREATER_GREATER_P)))
			{
				if (i == 2)
					has_label = true;
				continue;
				
			}
			if (state->non_quote_tokens[i] == DECLARE_P)
			{
				found_decl = true;

				/* Continue for CURSOR */
				for (; i < state->non_quote_pos && i < MAX_TOKENS_LOOKUP; i++)
				{
					if (state->non_quote_tokens[i] == SEMICOLON_P)
						goto _cs_end;

					if (state->non_quote_tokens[i] == CURSOR_P)
					{
						/* Continue for FOR */
						for (; i < state->non_quote_pos && i < MAX_TOKENS_LOOKUP; i++)
						{
							if (state->non_quote_tokens[i] == SEMICOLON_P)
								goto _cs_end;

							if (state->non_quote_tokens[i] == FOR_P)
							{
								state->slash_end = false;
								return;
							}
						}
					}
				}
			}
			else
				break; /* not a declare leading statement */
		}

_cs_end:
		/* The last token is a ';', consider it is a anonymous block */
		if (found_decl && (i >= MAX_TOKENS_LOOKUP ||
				(i < state->non_quote_pos && state->non_quote_tokens[i] == SEMICOLON_P) ||
				(has_label == true)))
			state->slash_end = true;
	}
}

/*
 * ora_psqlscan_escape_variable --- process :'VARIABLE' or :"VARIABLE"
 *
 * If the variable name is found, escape its value using the appropriate
 * quoting method and emit the value to output_buf.  (Since the result is
 * surely quoted, there is never any reason to rescan it.)	If we don't
 * find the variable or escaping fails, emit the token as-is.
 */
static void
ora_psqlscan_escape_variable(PsqlScanState state, const char *txt, int len,
						 PsqlScanQuoteType quote)
{
	char	   *varname;
	char	   *value;

	/* Variable lookup. */
	varname = psqlscan_extract_substring(state, txt + 2, len - 3);
	if (state->callbacks->get_variable)
		value = state->callbacks->get_variable(varname, quote,
											   state->cb_passthrough);
	else
		value = NULL;
	free(varname);

	if (value)
	{
		/* Emit the suitably-escaped value */
		appendPQExpBufferStr(state->output_buf, value);
		free(value);
	}
	else
	{
		/* Emit original token as-is */
		ora_psqlscan_emit(state, txt, len, -1);
	}
}
