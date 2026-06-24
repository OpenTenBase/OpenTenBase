/*-------------------------------------------------------------------------
 *
 * parser.c
 *		Main entry point/driver for PostgreSQL grammar
 *
 * Note that the grammar is not allowed to perform any table access
 * (since we need to be able to do basic parsing even while inside an
 * aborted transaction).  Therefore, the data structures returned by
 * the grammar are "raw" parsetrees that still need to be analyzed by
 * analyze.c and related files.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "parser/parser.h"
#include "parser/gram.h"
#include "parser/gramparse_extra.h"
#include "nodes/makefuncs.h"

#include "utils/guc.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/orcl_datetime_formatting.h"

/* It is a lower ident in const string */
bool lower_ident_as_const_str = false;

extern bool keywords_as_ident;
extern int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);
extern int ora_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);
extern int pg_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);


/*
 * raw_parser
 *		Given a query in string form, do lexical and grammatical analysis.
 *
 * Returns a list of raw (un-analyzed) parse trees.  The immediate elements
 * of the list are always RawStmt nodes.
 */
List *
raw_parser(const char *str)
{
	core_yyscan_t yyscanner;
	base_yy_extra_type yyextra;
	int			yyresult;
	int			i;

	yyextra.is_mult_insert = false; /* ora_compatible */

	/* initialize the flex scanner */
	yyscanner = scanner_init(str, &yyextra.core_yy_extra,
							 ScanKeywords, NumScanKeywords);

	/* base_yylex() only needs this much initialization */
	yyextra.have_lookahead = false;
	yyextra.check_dblink = false;
	yyextra.do_as_alias = false;
	yyextra.try_ora_plsql = false;
	yyextra.cur_his_tok = 0;
	yyextra.is_func_sp = false;
	yyextra.is_trigger_sp = false;
	yyextra.is_pkg_sp = false;
	yyextra.start_with_clause = false;
	yyextra.is_type_object_spec_sp = false;
	yyextra.is_type_object_body_sp = false;
	yyextra.has_type_token = false;
	yyextra.pl_def_name.data[0] = '\0';
	yyextra.scope_stack = NULL;
	/* init scope status */
	yyextra.keywords_as_identifier = keywords_as_ident;
	yyextra.scope_stack = lappend(yyextra.scope_stack, palloc(sizeof(bool)));
	yyextra.current_scope = NULL;
	yyextra.query_level = 0;
	memset(yyextra.global_scope_cnt, 0, ORA_UNKNOWN_SCOPE * sizeof(int32));
	for (i = 0; i < NUM_HIS_TOKEN; i++)
		yyextra.history_tokens[i].token = ORA_INVALID_TOKEN;
	for (i = 0; i < ORA_YY_MAX_LOOKAHEAD; i++)
		yyextra.lookahead[i].token = ORA_INVALID_TOKEN;

	/* initialize the bison parser */
	parser_init(&yyextra);

	/* Parse! */
	yyresult = base_yyparse(yyscanner);

	/* Clean up (release memory) */
	scanner_finish(yyscanner);

	if (yyresult)				/* error */
		return NIL;

	return yyextra.parsetree;
}

int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner)
{
	if (ORA_MODE)
		return ora_base_yylex(lvalp, llocp, yyscanner);
	return pg_base_yylex(lvalp, llocp, yyscanner);
}

/*
 * Intermediate filter between parser and core lexer (core_yylex in scan.l).
 *
 * This filter is needed because in some cases the standard SQL grammar
 * requires more than one token lookahead.  We reduce these cases to one-token
 * lookahead by replacing tokens here, in order to keep the grammar LALR(1).
 *
 * Using a filter is simpler than trying to recognize multiword tokens
 * directly in scan.l, because we'd have to allow for comments between the
 * words.  Furthermore it's not clear how to do that without re-introducing
 * scanner backtrack, which would cost more performance than this filter
 * layer does.
 *
 * The filter also provides a convenient place to translate between
 * the core_YYSTYPE and YYSTYPE representations (which are really the
 * same thing anyway, but notationally they're different).
 */
int
pg_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner)
{
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	int			cur_token;
	int			next_token;
	int			cur_token_length;
	YYLTYPE		cur_yylloc;

	/* Get next token --- we might already have it */
	if (yyextra->have_lookahead)
	{
		cur_token = yyextra->lookahead_token;
		lvalp->core_yystype = yyextra->lookahead_yylval;
		*llocp = yyextra->lookahead_yylloc;
		*(yyextra->lookahead_end) = yyextra->lookahead_hold_char;
		yyextra->have_lookahead = false;
	}
	else
		cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);

	/*
	 * If this token isn't one that requires lookahead, just return it.  If it
	 * does, determine the token length.  (We could get that via strlen(), but
	 * since we have such a small set of possibilities, hardwiring seems
	 * feasible and more efficient.)
	 */
	switch (cur_token)
	{
		case NOT:
			cur_token_length = 3;
			break;
		case NULLS_P:
			cur_token_length = 5;
			break;
		case WITH:
			cur_token_length = 4;
			break;
		default:
			return cur_token;
	}

	/*
	 * Identify end+1 of current token.  core_yylex() has temporarily stored a
	 * '\0' here, and will undo that when we call it again.  We need to redo
	 * it to fully revert the lookahead call for error reporting purposes.
	 */
	yyextra->lookahead_end = yyextra->core_yy_extra.scanbuf +
		*llocp + cur_token_length;
	Assert(*(yyextra->lookahead_end) == '\0');

	/*
	 * Save and restore *llocp around the call.  It might look like we could
	 * avoid this by just passing &lookahead_yylloc to core_yylex(), but that
	 * does not work because flex actually holds onto the last-passed pointer
	 * internally, and will use that for error reporting.  We need any error
	 * reports to point to the current token, not the next one.
	 */
	cur_yylloc = *llocp;

	/* Get next token, saving outputs into lookahead variables */
	next_token = core_yylex(&(yyextra->lookahead_yylval), llocp, yyscanner);
	yyextra->lookahead_token = next_token;
	yyextra->lookahead_yylloc = *llocp;

	*llocp = cur_yylloc;

	/* Now revert the un-truncation of the current token */
	yyextra->lookahead_hold_char = *(yyextra->lookahead_end);
	*(yyextra->lookahead_end) = '\0';

	yyextra->have_lookahead = true;

	/* Replace cur_token if needed, based on lookahead */
	switch (cur_token)
	{
		case NOT:
			/* Replace NOT by NOT_LA if it's followed by BETWEEN, IN, etc */
			switch (next_token)
			{
				case BETWEEN:
				case IN_P:
				case LIKE:
				case ILIKE:
				case SIMILAR:
					cur_token = NOT_LA;
					break;
			}
			break;

		case NULLS_P:
			/* Replace NULLS_P by NULLS_LA if it's followed by FIRST or LAST */
			switch (next_token)
			{
				case FIRST_P:
				case LAST_P:
					cur_token = NULLS_LA;
					break;
			}
			break;

		case WITH:
			/* Replace WITH by WITH_LA if it's followed by TIME or ORDINALITY */
			switch (next_token)
			{
				case TIME:
				case ORDINALITY:
					cur_token = WITH_LA;
					break;
			}
			break;

	}

	return cur_token;
}

void
parser_init(base_yy_extra_type *yyext)
{
	if (ORA_MODE)
	{
		ora_parser_init(yyext);
		return ;
	}
	pg_parser_init(yyext);
}

int
base_yyparse(core_yyscan_t yyscanner)
{
	if (ORA_MODE)
	{
		return ora_base_yyparse(yyscanner);
	}
	return pg_base_yyparse(yyscanner);
}

Node *
makeNullAConst(int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Null;
	n->location = location;

	return (Node *)n;
}

/* SystemTypeName()
 * Build a properly-qualified reference to a built-in type.
 *
 * typmod is defaulted, but may be changed afterwards by caller.
 * Likewise for the location.
 */
TypeName *
SystemTypeName(char *name)
{
	char *namespace = "pg_catalog";

	if (ORA_MODE)
	{

		/* the system typename in opentenbase_ora mode is in capitals, so uppercase it */
		name = asc_toupper(name, strlen(name));
	}

	return makeTypeNameFromNameList(list_make2(makeString(namespace),
                                               makeString(name)));
}

/* SystemFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
List *
SystemFuncName(char *name)
{
	/* The system typename in opentenbase_ora mode is capital, so lowercase it */
	if (ORA_MODE)
		name = asc_toupper(name, strlen(name));

	return list_make2(makeString("pg_catalog"), makeString(name));
}

/*
 * OpenTenBaseOraFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
List *
OpenTenBaseOraFuncName(char *name)
{
	/* The system typename in opentenbase_ora mode is capital, so lowercase it */
	if (ORA_MODE)
		name = asc_toupper(name, strlen(name));

	return list_make2(makeString("opentenbase_ora"), makeString(name));
}

Node *
makeTypeCast(Node *arg, TypeName *typename, int location)
{
	TypeCast *n = makeNode(TypeCast);
	n->arg = arg;
	n->typeName = typename;
	n->location = location;
	return (Node *) n;
}

/*
 * makeBoolAConst()
 * Create an A_Const string node and put it inside a boolean cast.
 */
Node *
makeBoolAConst(bool state, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = (state ? "t" : "f");
	n->location = location;

	return makeTypeCast((Node *)n, SystemTypeName("bool"), -1);
}

Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;

	return (Node *) n;
}

Node *
makeStringConst(const char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = pstrdup(str);
	n->location = location;
	/*
	 * We can't judge whether the guc value contains a
	 * ident name which is converted to lowercase in
	 * gram_ora.y or not, so just mark it in the node.
	 *
	 * Notice: it is just used to get guc value.
	 */
	n->lower_ident = lower_ident_as_const_str;
	lower_ident_as_const_str = false;

	return (Node *) n;
}

Node *
makeStringConstCast(char *str, int location, TypeName *typename)
{
	Node *s = makeStringConst(str, location);

	return makeTypeCast(s, typename, -1);
}

Node *
make_string_const_cast_orcl(char *str, int location, TypeName *typename)
{
		return makeStringConstCast(str, location, typename);
}



/*
 * check2pcGid()
 * Check wherher 2PC gid is valid
 */
void
check2pcGid(const char* gid)
{
	int i = 0;
	int len = 0;
	const char * blacklist = "*<>?|/, \\\"";

	if (gid == NULL)
		return;

	len = strlen(gid);
	for (i = 0; i < len; i++)
	{
		if (strchr(blacklist, gid[i]) != NULL)
			elog(ERROR, "gid is invalid, gid cannot contain character '%c', "
				"gid: '%s'", gid[i], gid);
	}
}
