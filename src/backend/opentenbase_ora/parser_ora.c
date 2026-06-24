/*-------------------------------------------------------------------------
 *
 * parser_ora.c
 *      Main entry point/driver for opentenbase_ora grammar
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
 *    src/backend/opentenbase_ora/parser_ora.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "parser/parser.h"
#include "opentenbase_ora/gram_ora.h"
#include "parser/gramparse_extra.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "catalog/heap.h" /* SystemAttributeByName */
#include "miscadmin.h"
#include "parser/parse_target.h"
#include "utils/guc.h"
#include "catalog/pg_database.h"
#include "parser/scansup.h"
#include "mb/pg_wchar.h"
#include "common/keywords.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "catalog/namespace.h"

#define CONNECT_BY_ISLEAF           ("connect_by_isleaf")
#define CONNECT_BY_LEVEL            ("level")
#define LEFT_ISLEAF_TABLE           ("_leaf1")
#define RIGHT_ISLEAF_TABLE          ("_leaf2")
#define CONNECT_BY_OUTER_TABLE      ("_cbout")

#define CONNECT_BY_ROOT_COLUMN_LEN  (128)
#define READ_HISTORY_TOKEN(index) (yyextra->history_tokens[(yyextra->cur_his_tok + (index)) % NUM_HIS_TOKEN].token)

extern int ora_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);
extern bool allow_limit_ident;
extern bool do_as_alias;
extern bool case_as_alias;
extern bool keywords_as_ident;
static bool token_is_operator(int token);
static bool token_is_mathop(int token);

void ORA_ENTER_SCOPE(enum ora_parse_scope scope, base_yy_extra_type *extra)
{
	bool *sc = NULL;

	if (!extra->keywords_as_identifier)
		return;
	extra->query_level++;
	extra->global_scope_cnt[scope]++;
	sc = (bool *)palloc0(ORA_UNKNOWN_SCOPE);
	if (extra->current_scope)
		memcpy(sc, extra->current_scope, ORA_UNKNOWN_SCOPE);
	sc[scope] = true;
	lappend_cell(extra->scope_stack, extra->scope_stack->head, sc);
	extra->current_scope = (bool *)lfirst(extra->scope_stack->head->next);
}

void ORA_LEAVE_SCOPE(enum ora_parse_scope scope, base_yy_extra_type *extra)
{
	if (!extra->keywords_as_identifier)
		return;
	extra->query_level--;
	extra->global_scope_cnt[scope]--;
	if (extra->query_level < 0 || extra->global_scope_cnt[scope] < 0)
	{
		return;
	}
	pfree(extra->current_scope);
	extra->scope_stack = list_delete_cell(extra->scope_stack,
					 extra->scope_stack->head->next, extra->scope_stack->head);
	if (extra->scope_stack->head->next)
		extra->current_scope = (bool *)lfirst(extra->scope_stack->head->next);
	else
		extra->current_scope = NULL;
}

/*
 * Reset status for each of parsing statement.
 */
void
reset_parser_statement(base_yy_extra_type *yyextra)
{
	/*
	 * Reset for next parsing
	 */
	yyextra->try_ora_plsql = false;
	yyextra->is_mult_insert = false;
	yyextra->cur_his_tok = 0;
	yyextra->is_func_sp = false;
	yyextra->is_trigger_sp = false;
	yyextra->is_pkg_sp= false;
	yyextra->start_with_clause = false;
	yyextra->pl_def_name.data[0] = '\0';
	yyextra->func_def_idx = -1;
	yyextra->initialize_idx = -1;
	yyextra->func_spec = NULL;
	yyextra->do_as_alias = false;
	yyextra->is_type_object_spec_sp = false;
	yyextra->is_type_object_body_sp = false;
	yyextra->has_type_token = false;
	/* reset scope status */
	yyextra->query_level = 0;
	list_free_deep(yyextra->scope_stack);
	yyextra->scope_stack = NULL;
	yyextra->scope_stack = lappend(yyextra->scope_stack, palloc0(sizeof(bool)));
	yyextra->current_scope = NULL;
	memset(yyextra->global_scope_cnt, 0, sizeof(int32) * ORA_UNKNOWN_SCOPE);


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

int ora_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner)
{
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	int			next2_token = -1;
	int 		cur_token = -1;
	int 		next_token = -1;
	int         prev_token = -1;
	int 		cur_token_length = 0;
	YYLTYPE 	cur_yylloc;
	bool		cmp_op = 0;
	bool		is_op_mark = false;

	/* 
	 * Chcek lookahead tokens first, we might already have one.
	 * If so then get the first lookahead token, move the rest tokens one slot forward.
	 */
	if (yyextra->lookahead[0].token != ORA_INVALID_TOKEN)	
	{
		cur_token = yyextra->lookahead[0].token;
		lvalp->core_yystype = yyextra->lookahead[0].lval;
		*llocp = yyextra->lookahead[0].loc;
		memcpy(yyextra->lookahead, &(yyextra->lookahead[1]),
			sizeof(yyextra->lookahead) - sizeof(yyextra->lookahead[0]));
		yyextra->lookahead[lengthof(yyextra->lookahead)-1].token = ORA_INVALID_TOKEN;
	}
	else
		cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);

	if (yyextra->have_lookahead)
	{
		*(yyextra->lookahead_end) = yyextra->lookahead_hold_char;
		yyextra->have_lookahead = false;
	}

	if (cur_token == TYPE_P)
		yyextra->has_type_token = true;
	if (cur_token == TYPECAST && keywords_as_ident)
		ORA_ENTER_SCOPE(ORA_TYPECAST_SCOPE, yyextra);

	/*
	 * Keep the history of parsed tokens up to 8.
	 */
	yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].token = cur_token;
	yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].lval = lvalp->core_yystype;
	yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].loc = *llocp;

	if (yyextra->cur_his_tok >= 1)
		prev_token = yyextra->history_tokens[(yyextra->cur_his_tok - 1) % NUM_HIS_TOKEN].token;

	/* THE FOLLOWING BLOCKS IS TO DETECT HARDWIRED PATTERNS IN HISTORY TOKENS. */
	
	/*
	 * Dblink syntax support, supports syntax such as tb1@t_link1.
	 */
	if (yyextra->cur_his_tok >= 1 &&
		cur_token == Op &&
		lvalp->core_yystype.str &&
		strcmp(lvalp->core_yystype.str, "@") == 0)
	{
		if (READ_HISTORY_TOKEN(-1) == IDENT || READ_HISTORY_TOKEN(-1) == IDENT_IN_D_QUOTES)
		{
			cur_token = '@';
			yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].token = cur_token;
			yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].lval =
				yyextra->history_tokens[(yyextra->cur_his_tok - 1) % NUM_HIS_TOKEN].lval;
		}
	}

	/*
	 * Detecting the beginning of a PL/SQL-like procedure/function,
	 * keep its name identifier into parser context.
	 */
	if (yyextra->cur_his_tok >= 2)
	{
		if (yyextra->is_func_sp &&
			yyextra->pl_def_name.data[0] == '\0' &&
			(	cur_token == '(' ||
				cur_token == IS ||
				cur_token == AS ||
				cur_token == RETURN ||
				cur_token == AUTHID ||
				cur_token == RETURNS
			))
		{
			base_yy_extra_type	*yyextra = pg_yyget_extra(yyscanner);

			StrNCpy(NameStr(yyextra->pl_def_name),
				yyextra->history_tokens[(yyextra->cur_his_tok - 1) % NUM_HIS_TOKEN].lval.str, NAMEDATALEN);
		}
	}

	if (yyextra->cur_his_tok == 1 &&
		READ_HISTORY_TOKEN(-1) == WITH &&
		(cur_token == FUNCTION || cur_token == PROCEDURE))
		yyextra->start_with_clause = true;

	/*
	 * FUNCTION | PROCEDURE -> is_func_sp
	 * TIGGER -> is_trigger_sp
	 * PACKAGE -> is_pkg_sp
	 * CREATE FUNCTION | CREATE PRCEDURE | WITH FUNCTION | WITH PROCEDURE |
	 * CREATE OR REPLACE FUNCTION | CREATE OR REPLACE PROCEDURE -> try_ora_plsql
	 */
	if (cur_token == FUNCTION ||
		cur_token == PROCEDURE ||
		cur_token == PACKAGE ||
		cur_token == TRIGGER ||
		cur_token == OBJECT_P ||
		cur_token == BODY)
	{
		/*
		 * To check if we are creating a function, a trigger or a package in PL/SQL.
		 */
		if (yyextra->cur_his_tok >= 1 &&
			(READ_HISTORY_TOKEN(-1) == CREATE || READ_HISTORY_TOKEN(-1) == WITH))
		{
			if (keywords_as_ident &&
				READ_HISTORY_TOKEN(-1) == WITH &&
				cur_token == BODY &&
			    (IS_SCOPE_SET(ORA_SELECT_SCOPE, yyextra) ||
			     IS_SCOPE_SET(ORA_INSERT_SCOPE, yyextra) ||
			     IS_SCOPE_SET(ORA_UPDATE_SCOPE, yyextra) ||
			     IS_SCOPE_SET(ORA_MERGE_SCOPE, yyextra) ||
			     IS_SCOPE_SET(ORA_SORT_SCOPE, yyextra)))
				yyextra->try_ora_plsql = false;
			else
				yyextra->try_ora_plsql = true;
			if (cur_token == FUNCTION || cur_token == PROCEDURE)
				yyextra->is_func_sp = true;
			if (cur_token == TRIGGER)
				yyextra->is_trigger_sp = true;
			if (cur_token == PACKAGE)
				yyextra->is_pkg_sp = true;
		}
		else if (yyextra->cur_his_tok >= 2 &&
				READ_HISTORY_TOKEN(-2) == CREATE &&
				(READ_HISTORY_TOKEN(-1) == EDITIONABLE || READ_HISTORY_TOKEN(-1) == NONEDITIONABLE))
		{
			yyextra->try_ora_plsql = true;
			if (cur_token == FUNCTION || cur_token == PROCEDURE)
				yyextra->is_func_sp = true;
			if (cur_token == TRIGGER)
				yyextra->is_trigger_sp = true;
			if (cur_token == PACKAGE)
				yyextra->is_pkg_sp = true;
		}
		else if (yyextra->cur_his_tok >= 3 &&
					READ_HISTORY_TOKEN(-3) == CREATE &&
					READ_HISTORY_TOKEN(-2) == OR &&
					READ_HISTORY_TOKEN(-1) == REPLACE)
		{
			yyextra->try_ora_plsql = true;
			if (cur_token == FUNCTION || cur_token == PROCEDURE)
				yyextra->is_func_sp = true;
			if (cur_token == TRIGGER)
				yyextra->is_trigger_sp = true;
			if (cur_token == PACKAGE)
				yyextra->is_pkg_sp = true;
		}
		else if (yyextra->cur_his_tok >= 4 &&
					READ_HISTORY_TOKEN(-4) == CREATE &&
					READ_HISTORY_TOKEN(-3) == OR &&
					READ_HISTORY_TOKEN(-2) == REPLACE &&
					(READ_HISTORY_TOKEN(-1) == EDITIONABLE || READ_HISTORY_TOKEN(-1) == NONEDITIONABLE))
		{
			yyextra->try_ora_plsql = true;
			if (cur_token == FUNCTION || cur_token == PROCEDURE)
				yyextra->is_func_sp = true;
			if (cur_token == TRIGGER)
				yyextra->is_trigger_sp = true;
			if (cur_token == PACKAGE)
				yyextra->is_pkg_sp = true;
		}
		/* a lot of possible combination,
		 * eg: 'create or replace type xxx as object, create type "sch"."xxx" as object
		 * we check three token here, 'type', 'as'/'is', 'object'
		 */
		else if (cur_token == OBJECT_P && yyextra->has_type_token && yyextra->cur_his_tok > 1 &&
					(READ_HISTORY_TOKEN(-1) == AS || READ_HISTORY_TOKEN(-1) == IS))
		{
			yyextra->try_ora_plsql = true;
			yyextra->is_type_object_spec_sp = true;
			yyextra->func_spec = NULL;
		}
		else if (yyextra->cur_his_tok >= 1 &&
					READ_HISTORY_TOKEN(-1) == TYPE_P)
		{
			if (cur_token == BODY)
			{
				yyextra->try_ora_plsql = true;
				yyextra->is_type_object_body_sp = true;
				yyextra->func_spec = NULL;
			}
		}
		else if (yyextra->cur_his_tok == 0 &&
					(cur_token == FUNCTION || cur_token == PROCEDURE))
		{
			/*
			 * PL/SQL allows such syntax to declare function or procedure within
			 * package body.
			 */
			yyextra->try_ora_plsql = true;
			yyextra->is_func_sp = true;
		}
		else if ((cur_token == FUNCTION || cur_token == PROCEDURE) &&
					(READ_HISTORY_TOKEN(-1) == MEMBER_P || READ_HISTORY_TOKEN(-1) == STATIC_P))
		{
			yyextra->try_ora_plsql = true;
			yyextra->is_func_sp = true;
		}
		else if (yyextra->cur_his_tok == 1 && (cur_token == FUNCTION || cur_token == PROCEDURE) &&
					READ_HISTORY_TOKEN(-1) == CONSTRUCTOR_P)
		{
			yyextra->try_ora_plsql = true;
			yyextra->is_func_sp = true;
		}
		else if (yyextra->start_with_clause)
		{
			yyextra->try_ora_plsql = true;
			if (cur_token == FUNCTION || cur_token == PROCEDURE)
				yyextra->is_func_sp = true;
		}

	}


	if (cur_token == LINK && yyextra->cur_his_tok >=1)
	{
		if (READ_HISTORY_TOKEN(-1) == DATABASE)
			cur_token = LINK_P;
	}

	if (cur_token == BINARY && yyextra->cur_his_tok >=1)
	{
		if (READ_HISTORY_TOKEN(-1) == COPY)
			cur_token = BINARY_P;
	}

	if ((cur_token == ASYMMETRIC || cur_token == SYMMETRIC) &&
		yyextra->cur_his_tok >=1)
	{
		if (READ_HISTORY_TOKEN(-1) != BETWEEN)
		{
			cur_token = IDENT;
			lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
		}
	}

	/*
	 * Each IS | AS SCONST {PL/pgSQL body} pattern ends opentenbase_ora PL/SQL-like function syntax matching.
	 *
	 * Note: CREATE PACKAGE needs to set try_ora_plsql flag to extract PL/SQL functions out of package.
	 */
	if (yyextra->try_ora_plsql && !yyextra->is_pkg_sp && !yyextra->is_type_object_body_sp &&
		yyextra->cur_his_tok >= 1 && cur_token == SCONST &&
		(READ_HISTORY_TOKEN(-1) == AS || READ_HISTORY_TOKEN(-1) == IS))
	{
		yyextra->func_def_idx = -1;
		yyextra->initialize_idx = -1;
		yyextra->func_spec = NULL;
		yyextra->try_ora_plsql = false;
	}

	/*
	 * In opentenbase_ora database SELECT UNIQUE and SELECT DISTINCT are synonymous.
	 */
	if (ORA_MODE && yyextra->cur_his_tok >= 1 && cur_token == UNIQUE)
	{
		if (READ_HISTORY_TOKEN(-1) == SELECT ||
			READ_HISTORY_TOKEN(-1) == '(')
		{
			cur_token = DISTINCT;
		}
	}

	/* Now advance history token cursor */
	yyextra->cur_his_tok++;

	/* END OF MATCHING HARDWIRED PATTERNS IN HISTORY TOKENS. */
	
	/* BEGIN opentenbase_ora keywords support */
	if (keywords_as_ident && 
		(IS_SCOPE_SET(ORA_SELECT_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_INSERT_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_UPDATE_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_MERGE_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_SORT_SCOPE, yyextra)))
	{
		switch(cur_token)
		{
			case DEFERRABLE:
			case INITIALLY:
			case PRIMARY:
			case REFERENCES:
			case CONSTRAINT:
			case FOREIGN:
			case ANALYZE:
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case RETURNING:
				if (!IS_IN_SCOPE(ORA_INSERT_SCOPE, yyextra) && !IS_IN_SCOPE(ORA_UPDATE_SCOPE, yyextra))
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			default:
				break;
		}
	}
	/* BEGIN opentenbase_ora keywords support */

	/*
	 * If this token isn't one that requires lookahead, just return it.  If it
	 * does, determine the token length.  (We could get that via strlen(), but
	 * since we have such a small set of possibilities, hardwiring seems
	 * feasible and more efficient.)
	 */
	switch (cur_token)
	{
		case VALUES:
			Assert(ORA_MODE);
			if (yyextra->is_mult_insert)
				return VALUES_LA;
			else
				return cur_token;
		case NOT:
			cur_token_length = 3;
			break;
		case DO:
			if (do_as_alias)
				cur_token_length = 2;
			else
				return cur_token;
			break;
		case CASE:
			if (case_as_alias)
				cur_token_length = 4;
			else
				return cur_token;
			break;
		case AS:
		case IS:
			{
				if (yyextra->try_ora_plsql)
					cur_token_length = 2;
				else
					return cur_token;
			}
			break;
		case NULLS_P:
			cur_token_length = 5;
			break;
		case WITH:
			cur_token_length = 4;
			break;
		/*
		 * In opentenbase_ora, blank space including comments can in the operator, <> <= != >=.
		 * Such as, < > is considered as <>.
		 */
		case '<':
		case '>': /* ! is returned as Op */
			cur_token_length = 1;
			break;
		case '(':
			cur_token_length = 1;
			break;
		case ')':
			/* CREATE TRIGGER ... on tbl WHEN () [plsql block] */
			cur_token_length = cur_token_length ? cur_token_length : 1;
			/* fallthrough */
		case ROW:
		case OLD:
		case NEW:
		case SCHEMA:
		case DATABASE:
		case IDENT_IN_D_QUOTES:
		case IDENT:
			{
				/* CREATE TRIGGER ... ON tbl [plsql block] */
				cur_token_length = cur_token_length ? cur_token_length : strlen(lvalp->str);
				if (cur_token == IDENT_IN_D_QUOTES)
					cur_token_length += 2;
				if (!(yyextra->try_ora_plsql && yyextra->is_trigger_sp))
					return cur_token;
			}
			break;
		case PARTITION:
			cur_token_length = 9;
			break;
		case SUBPARTITION:
			cur_token_length = 12;
			break;
		case LEFT:
			cur_token_length = 4;
			break;
		case LINK_P:
			cur_token_length = 4;
			break;
		case BINARY_P:
			cur_token_length = 6;
			break;
		case Op:
			Assert(strlen(lvalp->str) >= 1);
			/* Char by char comparison for performance */
			if (lvalp->str[0] == '|' && lvalp->str[1] == '|')
			{
					/* Still check if it is || operator */
					Assert(strlen(lvalp->str) >= 2);
					if (lvalp->str[2] == 0)
					{
						cur_token_length = 2;
						break;
					}
					else
				return cur_token;
			}
			else if (lvalp->str[0] == '!' && lvalp->str[1] == '\0')
			{
					cur_token_length = 1;
					is_op_mark = true;
					break; /* Go to lookahead, maybe ! = like opentenbase_ora */
			}
			else if (lvalp->str[0] == '=' && lvalp->str[1] == '>')
				return EQUALS_GREATER;

			return cur_token;
		case LIMIT:
			{
				if (allow_limit_ident)
				{
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
					return IDENT;
				}
				else
					return LIMIT;
			}
			break;
		case ORDER:
			cur_token_length = 5;
			break;
		case ',':
			/* like object(a int, constructor function) */
			if (yyextra->try_ora_plsql && yyextra->is_type_object_spec_sp)
				cur_token_length = 1;
			else
				return cur_token;
			break;
		case END_P:
		case ARRAY:
		case OFFSET:
		case PRIMARY:
		case SIMILAR:
		case PLACING:
		case NATURAL:
		case OVERLAPS:
		case TABLESAMPLE:
		case CHARACTER:
		case FULL:
		case SOME:
		case WHEN:
		case TIME:
		case BYTE_P:
		case RIGHT:
		case CROSS:
		case INNER_P:
		case FETCH:
		case WINDOW:
		case USING:
		case EXCEPT:
		case VARIADIC:
		case FOREIGN:
			if (keywords_as_ident)
			{
				cur_token_length = strlen(lvalp->str);
				break;
			}
			else
				return cur_token;
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
	Assert(*(yyextra->lookahead_end) == '\0' || yyextra->lookahead[0].token != ORA_INVALID_TOKEN);
	
	/*
	 * Save and restore *llocp around the call.  It might look like we could
	 * avoid this by just passing &lookahead_yylloc to core_yylex(), but that
	 * does not work because flex actually holds onto the last-passed pointer
	 * internally, and will use that for error reporting.  We need any error
	 * reports to point to the current token, not the next one.
	 */
	cur_yylloc = *llocp;

	if (yyextra->lookahead[0].token == ORA_INVALID_TOKEN)
	{
		yyextra->lookahead[0].token = core_yylex(&(yyextra->lookahead[0].lval) , 
											  llocp, 
											  yyscanner);
		next_token = yyextra->lookahead[0].token;
		yyextra->lookahead[0].loc = *llocp;

	}
	else
		next_token = yyextra->lookahead[0].token;
	
	if ( yyextra->lookahead[1].token == ORA_INVALID_TOKEN )
	{
		if ((cur_token == WITH && next_token == LOCAL) ||
                        (cur_token == ORDER && next_token == SIBLINGS) ||
                        (cur_token == LINK_P) || (cur_token == FULL) ||
                        (cur_token == RIGHT) || (cur_token == TABLESAMPLE) ||
                        (cur_token == WINDOW) || (cur_token == WHEN) ||
						cur_token == USING || cur_token == ARRAY || cur_token == OFFSET)
                {
                                yyextra->lookahead[1].token = core_yylex(&(yyextra->lookahead[1].lval),
                                                                                                                 llocp,
                                                                                                                 yyscanner);
                                next2_token = yyextra->lookahead[1].token;
                                yyextra->lookahead[1].loc = *llocp;
                }
	}
	else
		next2_token = yyextra->lookahead[1].token;



	if (yyextra->try_ora_plsql && yyextra->is_type_object_spec_sp)
	{
		if (next_token == CONSTRUCTOR_P || next_token == MEMBER_P || next_token == STATIC_P)
		{
			get_type_object_spec_func_decl(yyscanner, llocp);
			yyextra->lookahead[0].token = ';';
			yyextra->lookahead[0].lval.str = ";";
			yyextra->have_lookahead = true;
			/* make the type object attr declare rule finish */
			cur_token = ')';
			return cur_token;
		}
	}
	/* BEGIN OPENTENBASE_ORA keywords as identifier */
	if (keywords_as_ident &&
		(IS_SCOPE_SET(ORA_SELECT_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_INSERT_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_UPDATE_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_MERGE_SCOPE, yyextra) ||
		IS_SCOPE_SET(ORA_SORT_SCOPE, yyextra)))
	{
		switch(cur_token)
		{
			case CROSS:
				if (next_token != JOIN)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case FULL:
			case RIGHT:
				if (!((next_token == OUTER_P && next2_token == JOIN) ||
				      next_token == JOIN))
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case INNER_P:
				if (next_token != JOIN)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case SOME:
				break;
			case OVERLAPS:
				if (next_token != ROW)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case SIMILAR:
				if (next_token != TO)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case PLACING:
				if (IS_IN_SCOPE(ORA_OVERLAY_SCOPE, yyextra))
				{
					cur_token = PLACING;
				}
				else
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case TABLESAMPLE:
				if (next2_token != '(')
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case NATURAL:
				if (!(next_token == LEFT || next_token == JOIN ||
				      next_token == RIGHT || next_token == FULL ||
					  next_token == INNER_P))
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case WINDOW:
				if (next2_token != AS)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));

				}
				break;
			case FETCH:
				if (next_token != FIRST_P && next_token != NEXT)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case WHEN:
				if (IS_SCOPE_SET(ORA_MERGE_SCOPE, yyextra) &&
				    ((next_token == NOT && next2_token == MATCHED) || next_token == MATCHED))
					cur_token = WHEN;
				else if (IS_IN_SCOPE(ORA_CASE_WHEN_SCOPE, yyextra) ||
					    IS_IN_SCOPE(ORA_INSERT_SCOPE, yyextra))
						cur_token = WHEN;
				else
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case END_P:
				if (IS_IN_SCOPE(ORA_CASE_WHEN_SCOPE, yyextra))
					cur_token = END_P;
				else
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case BYTE_P:
			case CHARACTER:
				if (!IS_IN_SCOPE(ORA_TYPECAST_SCOPE, yyextra) && next_token != VARYING)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case USING:
				if (!IS_IN_SCOPE(ORA_SORT_SCOPE, yyextra) && IS_IN_SCOPE(ORA_SELECT_SCOPE, yyextra) &&
					next_token != '(' && next_token != NCHAR_CS)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				else if (IS_IN_SCOPE(ORA_SORT_SCOPE, yyextra))
				{
					if (!(((next_token == Op || token_is_mathop(next_token)) &&
					(next2_token == NULLS_P || next2_token == ';' || next2_token == ',')) ||
					next_token == OPERATOR))
					{
						cur_token = IDENT;
						lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
					}
				}
				break;
			case EXCEPT:
				if (IS_IN_SCOPE(ORA_SELECT_SCOPE, yyextra) &&
					(next_token == ALL || next_token == DISTINCT ||
					next_token == '(' || next_token == VALUES || next_token == TABLE ||
					next_token == SELECT))
					cur_token = EXCEPT;
				else
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case VARIADIC:
				if (next_token == ',' || next_token == ')' || next_token == '.'
					|| next_token == FROM || next_token == ';' || next_token == ORDER
					|| next_token == GROUP_P)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			case TIME:
				if (next_token != ZONE && next_token != '(' && next_token != WITH_LA && next_token != WITHOUT
					&& next_token != WITH)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			// TODO:optimieze if case
			case ARRAY:
			case OFFSET:
				if (next_token == ',' || next_token == FROM || next_token == ';' ||
					(next_token == ORDER && next2_token == BY) || (next_token == GROUP_P && next2_token == BY) ||
					  (next_token == NATURAL && next2_token == JOIN) || (next_token == INNER_P && next2_token == JOIN) ||
					  (next_token == RIGHT && next2_token == OUTER_P) || (next_token == LEFT_P && next2_token == OUTER_P) ||
					  (next_token == RIGHT && next2_token == JOIN) || (next_token == LEFT_P && next2_token == JOIN) ||
					  next_token == JOIN || next_token == WHERE || next_token == ON || next_token == Op ||
					  next_token == AND || next_token == OR || next_token == PRIOR ||
					  (READ_HISTORY_TOKEN(-2) == WITH && READ_HISTORY_TOKEN(-3) == START) ||
					  READ_HISTORY_TOKEN(-2) == AS ||
					  (READ_HISTORY_TOKEN(-2) == WITH_LA && READ_HISTORY_TOKEN(-3) == START) ||
					next_token == LIMIT || next_token == OFFSET || next_token == '.')
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			default:
				break;
		}
	}
	/* END OPENTENBASE_ORA keyword */
	switch(cur_token)
	{
		/* find "(+)" token */	
		case '(':
			{
				if (yyextra->lookahead[0].token != '+')
					break;

				if (yyextra->lookahead[1].token == ORA_INVALID_TOKEN)
				{
					*llocp = yyextra->lookahead[0].loc;
					cur_token_length = 1;
					yyextra->lookahead_end = yyextra->core_yy_extra.scanbuf +
						*llocp + cur_token_length;
					Assert(*(yyextra->lookahead_end) == '\0');

					cur_yylloc = *llocp;
					yyextra->lookahead[1].token = core_yylex(&(yyextra->lookahead[1].lval), 
														  llocp, 
														  yyscanner);
					yyextra->lookahead[1].loc = *llocp;
				}
				
				if (yyextra->lookahead[1].token != ')')
					break;	/* now we have "(+)" token */

				cur_token = OUTER_CONN_TBL;	 /*this token means (+) */	
				yyextra->lookahead[0].token = yyextra->lookahead[1].token = ORA_INVALID_TOKEN;			
			}
			break;
		case '<':
		case '>':
			{
				int	old_token = cur_token;

				if (yyextra->lookahead[0].token == '=')
				{
					if (cur_token == '<')
						cur_token = LESS_EQUALS;
					else if (cur_token == '>')
						cur_token = GREATER_EQUALS;
				}
				else if (yyextra->lookahead[0].token == '>')
				{
					if (cur_token == '<')
						cur_token = NOT_EQUALS;
				}

				Assert(ORA_YY_MAX_LOOKAHEAD == 2);
				if (old_token != cur_token)
				{
					/* Find a Operator, and replace the first lookahead with the 2nd */
					yyextra->lookahead[0].token = yyextra->lookahead[1].token;
					yyextra->lookahead[0].lval = yyextra->lookahead[1].lval;
					yyextra->lookahead[0].loc = yyextra->lookahead[1].loc;

					yyextra->lookahead[1].token = ORA_INVALID_TOKEN;
				}

				break;
			}
		case LEFT:
			if (next_token == OUTER_P || next_token == JOIN)
				cur_token = LEFT_P;
			break;
		case LINK_P:
			if ((next2_token > 0 && next2_token != CONNECT && next2_token != ';')
				|| next_token == ';')
			cur_token = LINK;
			break;
		case BINARY_P:
			if (next_token == '(' || next_token == FROM || next_token == TO || next_token == '.')
				cur_token = BINARY;
			break;
		case DO:
			{
				/* It's impractical to list all possible tokens here, just list some possible tokens extracted
				 * from select_clause to cover the most common case.
				 *
				 * As a rare used feature it should be effective in minimum scope.
				 *
				 * Minics a_expr in gram.y to list possible operators before or after the wannabe identifier.
				 *
				 * Same applies to CASE in below.
				 */
				if (next_token == ',' || next_token == FROM || next_token == '.' || next_token == ';' ||
					next_token == ')' || next_token == HAVING ||
					next_token == LIMIT || next_token == WHERE || next_token == GROUP_P || next_token == ORDER ||
					next_token == INTO || next_token == WINDOW || next_token == START || next_token == CONNECT ||
					next_token == ORDER_S || next_token == OVER || token_is_operator(next_token) ||
					prev_token == NOT || prev_token == NOT_LA)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			}
		case CASE:
			{
				if (next_token == ',' || next_token == FROM || next_token == '.' || next_token == ';' ||
					next_token == ')' || next_token == HAVING ||
					next_token == LIMIT || next_token == WHERE || next_token == GROUP_P || next_token == ORDER ||
					next_token == INTO || next_token == WINDOW || next_token == START || next_token == CONNECT ||
					next_token == ORDER_S || next_token == OVER || token_is_operator(next_token) ||
					prev_token == NOT || prev_token == NOT_LA)
				{
					cur_token = IDENT;
					lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
				}
				break;
			}
		case NOT:
			{
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
			}
			break;
		case NULLS_P:
			{
				/* Replace NULLS_P by NULLS_LA if it's followed by FIRST or LAST */
				switch (next_token)
				{
					case FIRST_P:
					case LAST_P:
						cur_token = NULLS_LA;
						break;
				}
			}
			break;
		case Op:
			{
				if (yyextra->lookahead[0].token == '=' && is_op_mark)
				{
					cur_token = NOT_EQUALS;
					yyextra->lookahead[0].token = yyextra->lookahead[1].token;
					yyextra->lookahead[0].lval = yyextra->lookahead[1].lval;
					yyextra->lookahead[0].loc = yyextra->lookahead[1].loc;

					yyextra->lookahead[1].token = ORA_INVALID_TOKEN;
					break;
				}

				/*
				 * Look ahead one toke for '||' to support: string || KeyWord.
				 */
				switch (next_token)
				{
					/*
					 * Currently only support EVENT, more keywords may be added here
					 * in the future.
					 */
					case EVENT:
					{
						/* Set next token type as IDENT for Col || key words. */
						if (yyextra->lookahead[0].token == next_token)
						{
							yyextra->lookahead[0].token = IDENT;
							/* It is treated as a identifier, convert it to upper case */
							yyextra->lookahead[0].lval.str = pstrdup("EVENT");
						}
						break;
					}
					case '=':
					{
						if (lvalp->str && lvalp->str[0] == '!')
						{
							cur_token = NOT_EQUALS;
							cmp_op = 1;
						}
						break;
					}
					case NAMES:
					{
						if (yyextra->lookahead[0].token == next_token)
						{
							yyextra->lookahead[0].token = IDENT;
							/* It is treated as a identifier, convert it to upper case */
							yyextra->lookahead[0].lval.str = pstrdup("NAMES");
						}
						break;
					}
					case OLD:
					{
						/* Set next token type as IDENT for Col || key words. */
						if (yyextra->lookahead[0].token == next_token)
						{
							yyextra->lookahead[0].token = IDENT;
							/* It is treated as a identifier, convert it to upper case */
							yyextra->lookahead[0].lval.str = pstrdup("OLD");
						}
						break;
					}
				}
			}
			break;
		case WITH:
			{
				/* Replace WITH by WITH_LA if it's followed by TIME or ORDINALITY */
				switch (next_token)
				{
					case LOCAL:
						{
							if (next2_token == TIME)
								cur_token = WITH_LA;
						}
						break;
					case TIME:
					case ORDINALITY:
						cur_token = WITH_LA;
						break;
				}
			}
			break;
		case AS:
		case IS:
			{
				int	la_token = yyextra->lookahead[0].token;
				bool     reset_flag = true;

				/* trigger deal with function body not here, just break */
				if (yyextra->is_trigger_sp)
					break;

				switch (la_token)
				{
					case SCONST:
					/*fall through, for CREATE CAST modifier */	
					case ASSIGNMENT:
					case IMPLICIT_P:
						{
							yyextra->func_def_idx = -1;
							yyextra->initialize_idx = -1;
							yyextra->func_spec = NULL;
							yyextra->try_ora_plsql = false;

							break;
						}
					default:
						{
							/* Check next_token as the function body. */
							char	*result;
							
							if (yyextra->try_ora_plsql)
							{
								/*
								* constructor function ff(arg) return self as result is/as func body, skip the first one
								* other case: function As result number; begin null; end; 'reuslt' treat as local var
								*/
								if (la_token == RESULT_P &&
									yyextra->cur_his_tok > 2 &&
									yyextra->history_tokens[(yyextra->cur_his_tok - 2) % NUM_HIS_TOKEN].token == SELF_P)
								{
									reset_flag = false;
									break;
								}
							}
							if (yyextra->try_ora_plsql && yyextra->is_type_object_body_sp)
							{
								YYLTYPE yylloc = yyextra->lookahead[0].loc;
								yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].token = next_token;
								yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].lval = lvalp->core_yystype;
								yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].loc = *llocp;
								result = get_type_object_body(yyscanner, &yylloc);
							}
							else
							{
								result = get_plpgsql_body(yyscanner,
														NameStr(yyextra->pl_def_name),
														&yyextra->lookahead[0],
														yyextra->is_func_sp,
														&yyextra->lookahead[1],
														cur_yylloc + 2);
								if (result == NULL)
									elog(ERROR, "unexpected end of input string");

							}
							/* Change next token as function/procedure/package body */
							yyextra->lookahead[0].token = SCONST;
							yyextra->lookahead[0].lval.str = result;

							/* Next token is not looked ahead? */
							Assert((!yyextra->is_func_sp &&
										yyextra->lookahead[1].token == ORA_INVALID_TOKEN) ||
											yyextra->is_func_sp);
						}
				}


				/*
				 * stick in opentenbase_ora function status if it's a WITH FUNCTION statement
				 * since it may have multiple functions.
				 */
				if (!yyextra->start_with_clause && reset_flag)
					yyextra->is_func_sp = false;
			}
			break;
		case PARTITION:
			/* Just select * from xxx partition(xxx) or select * from xxx partition for is PG_PARTITION */
			if ((prev_token == IDENT || prev_token == IDENT_IN_D_QUOTES ) &&
				(next_token == '(' || next_token == FOR))
				cur_token = PG_PARTITION;
			break;
		case SUBPARTITION:
			/* Just select * from xxx subpartition(xxx) or select * from xxx subpartition for is PG_SUBPARTITION */
			if ((prev_token == IDENT || prev_token == IDENT_IN_D_QUOTES ) &&
				(next_token == '(' || next_token == FOR))
				cur_token = PG_SUBPARTITION;
			break;
		case CONSTRUCTOR_P:
			break;
		case ORDER:
			switch (next_token)
			{
				case SIBLINGS:
					if (next2_token == BY)
						cur_token = ORDER_S;
					break;
			}
			break;
		/* skip directly*/
		case IDENT:
		case ARRAY:
		case OFFSET:
		case CROSS:
		case FULL:
		case RIGHT:
		case INNER_P:
		case SOME:
		case TIME:
		case OVERLAPS:
		case SIMILAR:
		case PLACING:
		case TABLESAMPLE:
		case NATURAL:
		case WINDOW:
		case FETCH:
		case WHEN:
		case END_P:
		case BYTE_P:
		case CHARACTER:
		case USING:
		case EXCEPT:
		case VARIADIC:
			break;
		case PRIMARY:
			if (next_token != KEY)
			{
				cur_token = IDENT;
				lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
			}
			break;
		case FOREIGN:
			if (next_token != TABLE && next_token != KEY && next_token != DATA_P &&
				next_token != SERVER && next_token != SCHEMA)
			{
				cur_token = IDENT;
				lvalp->str = upcase_identifier(lvalp->str, strlen(lvalp->str));
			}
			break;
		default:
			if (!yyextra->is_trigger_sp &&
				!(yyextra->try_ora_plsql && yyextra->is_type_object_spec_sp))
			{
					/*can't reach here in*/
					elog(ERROR, "Internal error. Invalid token.");
			}
			break;
	}

	if (cmp_op)
	{
		yyextra->lookahead[0] = yyextra->lookahead[1];
		yyextra->lookahead[1].token = ORA_INVALID_TOKEN;
	}
	/* Now revert the un-truncation of the current token */
	yyextra->lookahead_hold_char = *(yyextra->lookahead_end);
	*(yyextra->lookahead_end) = '\0';

	yyextra->have_lookahead = true;
	*llocp = cur_yylloc;
	return cur_token;
}

#define MaxHistToken	8

typedef struct
{
	int	tok;
	YYLTYPE		yylloc;
	core_YYSTYPE	yylval;
} HistToken;

/*
 * Cache a number of MaxHistToken tokens for parsing BEGIN ... END ... In fact, only
 * cache 4 tokens is enough. Since we only concern about:
 * - ; END;
 * - ; END IDENT;
 * - ; END IF/LOOP/CASE;
 * - ; END {unreserved word}
 */
typedef struct
{
	int		num_hist_token;
	bool	eof;
	HistToken	hist_tokens[MaxHistToken];
} TokenHistCache;

static void
reset_hist_token_cache(TokenHistCache *cache)
{
	cache->num_hist_token = 0;
	cache->eof = false;
}

/*
 * Read one statement until meet ';'. and return the last 4 tokens to handle the cases:
 * - ; END ;
 * - ; END ident/LOOP/CASE/IF ;
 * - <ident> .. END/or not;
 * We only concern the 1st and 2nd cases.
 */
static int
read_one_statement(core_yyscan_t yyscanner, core_YYSTYPE *yylval,
						YYLTYPE *yylloc, TokenHistCache *hist_cache,
						List **parse_stack)
{
	int		*idx;
	int		maxtok;
	HistToken	*tokens = hist_cache->hist_tokens;

	/* quick exit if EOF */
	if (hist_cache->eof)
		return 0;

	maxtok = MaxHistToken;
	idx = &hist_cache->num_hist_token;
	while (true)
	{
		HistToken	*htok = &tokens[(*idx) % maxtok];

		htok->tok = core_yylex(&htok->yylval, &htok->yylloc, yyscanner);

		if (htok->tok != 0)
		{
			*idx = *idx + 1;
			memcpy(yylval, &htok->yylval, sizeof(core_YYSTYPE));
			*yylloc = htok->yylloc;
		}
		else
			hist_cache->eof = true; /* set EOF for future calls */

		if (htok->tok == ';' || htok->tok == 0)
			return htok->tok; /* ie. always a ';' */

		/* Return BEGIN for caller */
		if (htok->tok == BEGIN_P)
			return htok->tok;
	}
}

/*
 * If the parsed tokens contain END.
 */
static bool
plpgsql_has_end_token(int *tok_idx, YYLTYPE *yylloc, TokenHistCache *hist_cache)
{
	HistToken	*cur_tok;
	int			num_his_tok = hist_cache->num_hist_token;
	int			cur_idx = num_his_tok - 1;
	HistToken	*histokens = hist_cache->hist_tokens;
	int			max_tokens = MaxHistToken;

	if (num_his_tok == 0 || num_his_tok < 3)
		return false;

	Assert(cur_idx >= 0);
	cur_tok = &histokens[cur_idx % max_tokens];
	if (cur_tok->tok != ';')
		return false;

	cur_idx--;
	Assert(cur_idx >= 0);
	cur_tok = &histokens[cur_idx % max_tokens];
	if (cur_tok->tok != END_P && cur_tok->tok != IDENT)
	{
		const ScanKeyword	*end_keyword = NULL;

		end_keyword = ScanKeywordLookupByVal(cur_tok->tok);
		if (!end_keyword || end_keyword->category != UNRESERVED_KEYWORD)
			return false;
	}

	if (cur_tok->tok == END_P)
	{
		*tok_idx = cur_idx; /* END loc */
		*yylloc = cur_tok->yylloc;
		cur_idx--;
		Assert(cur_idx >= 0);
		cur_tok = &histokens[cur_idx % max_tokens];

		if (cur_tok->tok != ';')
			return false;
		else
			return true; /* Found pattern: ...; END;.. */
	}
	else
	{
		cur_idx--;
		Assert(cur_idx >= 0);
		cur_tok = &histokens[cur_idx % max_tokens];
		if (cur_tok->tok != END_P || cur_idx == 0)
			return false;

		*tok_idx = cur_idx; /* END loc */
		*yylloc = cur_tok->yylloc;

		cur_idx--;
		Assert(cur_idx >= 0);
		cur_tok = &histokens[cur_idx % max_tokens];
		if (cur_tok->tok != ';')
			return false;
		
		/* Matched one of patterns:
		 * 1. ...; END IDENT; ...
		 * 2. ...; END {unreserved_keyword}; ...
		 */
		return true; /* Found pattern: ...; END IDENT; ... */
	}
}

/*
 * Read one token from history tokens or from input parsing string. If from the history
 * parsed tokens (read_stmt == true), tok_idx is pointing to index of END at the beginning
 * and move *tok_idx to next after each call.
 */
static void
read_one_token(int *tok, core_YYSTYPE *yylval, YYLTYPE *yylloc, core_yyscan_t yyscanner,
						bool read_stmt, int *tok_idx, TokenHistCache *hist_cache)
{
	HistToken	*cur_tok;

	if (!read_stmt)
	{
		*tok = core_yylex(yylval, yylloc, yyscanner);
		return;
	}

	*tok_idx = *tok_idx + 1;
	if (*tok_idx == hist_cache->num_hist_token)
	{
		*tok = 0;
		return;
	}

	cur_tok = &hist_cache->hist_tokens[(*tok_idx) % MaxHistToken];
	*tok = cur_tok->tok;
	memcpy(yylval, &cur_tok->yylval, sizeof(core_YYSTYPE));
	*yylloc = cur_tok->yylloc;
}

/*
 * Read a toke until meet a BEGIN of the out-most level.
 */
static int
read_outmost_begin(core_yyscan_t yyscanner, YYLTYPE *yylloc, int nested_level)
{
	core_YYSTYPE	yylval;
	List	*case_begin_stack = NIL;
	int		func_st = FUNC_EMPTY;
	int		tok;

	if (nested_level != 0)
		func_st = FUNC_DECL;

	while (true)
	{
		tok = core_yylex(&yylval, yylloc, yyscanner);

		if (tok == SCONST)
		{
			/*
			 * The function body is in the format "$proc$ ... $proc$;".
			 */
			base_yy_extra_type	*yyextra;

			yyextra = pg_yyget_extra(yyscanner);

			if (yyextra->core_yy_extra.scanbuf[*yylloc] == '$')
			{
				int i = 1;

				while (*yylloc + i < yyextra->core_yy_extra.scanbuflen &&
					   yyextra->core_yy_extra.scanbuf[*yylloc + i] != '$' &&
					   !isspace((unsigned char) yyextra->core_yy_extra.scanbuf[*yylloc + i]))
					i++;

				if (*yylloc + i < yyextra->core_yy_extra.scanbuflen &&
					yyextra->core_yy_extra.scanbuf[*yylloc + i] == '$')
					return tok;
			}
		}

		if ((tok == PROCEDURE || tok == FUNCTION) && func_st != FUNC_BEGIN)
		{
			nested_level++;
			func_st = FUNC_DECL;
		}
		else if ((tok == AS || tok == IS) && func_st == FUNC_DECL)
		{
			func_st = FUNC_DEF;
		}
		else if (tok == BEGIN_P && nested_level == 0 && list_length(case_begin_stack) == 0) /* outmost level BEGIN */
		{
			return tok;
		}
		else if (tok == BEGIN_P && func_st == FUNC_DEF)
		{
			func_st = FUNC_BEGIN;
		}
		else if (tok == BEGIN_P &&(func_st == FUNC_BEGIN || func_st == FUNC_EMPTY))
		{
			case_begin_stack = lcons_int(PS_TYPE_BEGIN, case_begin_stack);
			func_st = FUNC_BEGIN;
		}
		else if (tok == CASE)
		{
			case_begin_stack = lcons_int(PS_TYPE_CASE, case_begin_stack);
		}
		else if (tok == ';' && func_st == FUNC_DECL)
		{
			/* Check if end of function declaration */
			if (nested_level - 1 != 0)
				func_st = FUNC_DEF;
			else
				func_st = FUNC_EMPTY;
			nested_level--;
		}
		else if (tok == END_P)
		{
			int	end_num = 1;

			tok = core_yylex(&yylval, yylloc, yyscanner);
			if (tok == END_P)
				end_num++;

			if (tok == IF_P || tok == CASE ||
				(tok == IDENT && strcasecmp(yylval.str, "loop") == 0))
			{
				if (list_length(case_begin_stack) > 0 && tok == CASE)
					case_begin_stack = list_delete_first(case_begin_stack);
				continue;
			}

			if (tok != ';')
			{
				tok = core_yylex(&yylval, yylloc, yyscanner);

				if (tok == END_P)
					end_num++;
				else if (tok == CASE)
					case_begin_stack = lcons_int(PS_TYPE_CASE, case_begin_stack);

				if (tok != ';' &&
					!(list_length(case_begin_stack) > 0 &&
					linitial_int(case_begin_stack) == PS_TYPE_CASE))
					elog(ERROR, "unexpected end of PLPGSQL function");
			}

			if (list_length(case_begin_stack) > 0)
			{
				while (end_num != 0 && list_length(case_begin_stack) > 0)
				{
					case_begin_stack = list_delete_first(case_begin_stack);
					end_num--;
				}
				continue;
			}
			else
			{
				func_st = FUNC_DEF;
			}

			if (func_st == FUNC_DEF && nested_level - 1 == 0)
				func_st = FUNC_EMPTY;
			nested_level--;
		}
		else if (tok == 0)
			return 0;
	}

	return 0;
}

void
get_type_object_spec_func_decl(core_yyscan_t yyscanner, YYLTYPE *yylloc)
{
	core_YYSTYPE	yylval;
	TokenHistCache	hist_cache;
	List	*parse_stack = NIL;
	YYLTYPE init_loc = *yylloc;
	StringInfoData	func_decl;
	int 			token;
	HistToken		prev_token;
	int				offset;
	int				token_idx;

	initStringInfo(&func_decl);

	reset_hist_token_cache(&hist_cache);
	token = read_one_statement(yyscanner, &yylval, yylloc,
							   &hist_cache, &parse_stack);
	if (hist_cache.eof)
	{
		/* eof is not store in history, and the last one should be ')' */
		token_idx = ((hist_cache.num_hist_token - 1) % MaxHistToken);
		prev_token = hist_cache.hist_tokens[token_idx];
	}
	else if (token == ';')
	{
		token_idx = ((hist_cache.num_hist_token - 2) % MaxHistToken);
		prev_token = hist_cache.hist_tokens[token_idx];
	}
	else
		elog(ERROR, "type object spect declaration is illegal");

	if (prev_token.tok != ')')
		elog(ERROR, "type object specification illegal, check if end with ')'");
	
	offset = prev_token.yylloc;
	appendBinaryStringInfo(&func_decl,
						&pg_yyget_extra(yyscanner)->core_yy_extra.scanbuf[init_loc],
						offset - init_loc);

	pg_yyget_extra(yyscanner)->func_spec = func_decl.data;
}

char *
get_type_object_body(core_yyscan_t yyscanner, YYLTYPE *yylloc)
{
	core_YYSTYPE yylval;
	YYLTYPE init_loc = *yylloc;
	StringInfoData src_def = {0};
	int prev_token = 0, prev_prev_token = 0;

	YYLTYPE prev_loc = 0, prev_prev_loc = 0;
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	char *scan_buf = yyextra->core_yy_extra.scanbuf;
	int tmp_token;

	initStringInfo(&src_def);
	prev_token = yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].token;

	do
	{
		tmp_token = core_yylex(&yylval, yylloc, yyscanner);
		if (tmp_token != 0)
		{
			prev_prev_token = prev_token;
			prev_prev_loc = prev_loc;
			prev_token = tmp_token;
			prev_loc = *yylloc;
		}
	} while(tmp_token != 0);

	if (prev_token == ';' && prev_prev_token == END_P)
		appendBinaryStringInfo(&src_def,
								scan_buf + (int)init_loc,
								(int)prev_prev_loc - (int)init_loc);
	else
		elog(ERROR, "illegal type body definition, check if end with ';'"
					" or missing 'END' token");
	
	elog(DEBUG5, "type body: %s", src_def.data);
	return src_def.data;
}

/*
 * get_plpgsql_body
 *    Get the body of function, procedure or package, which starts at AS/IS and
 *  ends with the outer-most END or END <object name>.
 */
char *
get_plpgsql_body(core_yyscan_t yyscanner, char *objname,
						ora_yy_lookahead_type *la_tok, bool is_fun_sp,
						ora_yy_lookahead_type *la_next_tok, int start_body_loc)
{
	base_yy_extra_type	*yyextra = pg_yyget_extra(yyscanner);
	core_YYSTYPE	yylval;
	StringInfoData	func_decl;
	YYLTYPE		yylloc;
	YYLTYPE		tmp_def_yylloc = -1;
	YYLTYPE		tmp_init_yylloc = -1;
	bool	has_declare = false;
	int		func_st = FUNC_EMPTY;
	bool	end_decl_section = false;
	int		start_loc = la_tok->loc;
	bool	first = true;
	List	*parse_stack = NIL;
	bool	read_stmt = false;
	TokenHistCache	hist_cache;

	initStringInfo(&func_decl);

	yyextra->func_def_idx = -1;
	yyextra->initialize_idx = -1;
	yyextra->func_spec = NULL;

	if (start_body_loc <= 0 || start_body_loc > start_loc)
		start_body_loc = start_loc;

	/* Initialize history token cache */
	reset_hist_token_cache(&hist_cache);

	while (true)
	{
		int		tok;

		if (first)
		{
			/* first use the look ahead token */
			tok = la_tok->token;
			yylloc = la_tok->loc;
			memcpy(&yylval, &la_tok->lval, sizeof(yylval));

			first = false;

			if (tok == DECLARE)
				has_declare = true;

			/* Find function begin...end section */
			if (is_fun_sp && tok != BEGIN_P)
			{
				int nested_level = 0;

				if (tok == PROCEDURE || tok == FUNCTION)
					nested_level = 1;
				tok = read_outmost_begin(yyscanner, &yylloc, nested_level);
			}
		}
		else
		{
			if (!read_stmt)
				tok = core_yylex(&yylval, &yylloc, yyscanner);
			else
				tok = read_one_statement(yyscanner, &yylval, &yylloc, &hist_cache,
											&parse_stack);
		}

		if (tok == PROCEDURE || tok == FUNCTION)
		{
			tmp_def_yylloc = yylloc;
			func_st = FUNC_DECL;

			/*
			 * Reset previous BEGIN location for a new procedure or function.
			 * Only the BEGIN...END which is last and does not belong to any
			 * procedure/function, will be a initialization part.
			 */
			tmp_init_yylloc = -1;
		}

		/*
		 * Append literal
		 */
		if ((tok == AS || tok == IS) && func_st == FUNC_DECL)
		{
			/*
			 * Find a first function/procedure definition.
			 */
			if (yyextra->func_def_idx == -1 && tmp_def_yylloc != -1)
				yyextra->func_def_idx = tmp_def_yylloc - start_body_loc;

			func_st = FUNC_DEF;
			end_decl_section = true;

			/* Eat all declaration tokens until BEGIN at the out level, zero if error */
			tok = read_outmost_begin(yyscanner, &yylloc, 0);

			if (tok != BEGIN_P)
				func_st = FUNC_EMPTY;
		}

		if (tok == BEGIN_P)
		{
			tmp_init_yylloc = yylloc;
			parse_stack = lcons_int(PS_TYPE_BEGIN, parse_stack);
			read_stmt = true;
		}
		else if (tok == ';' && func_st == FUNC_DECL && !end_decl_section)
		{
			/* Check if end of function declaration */
			func_st = FUNC_EMPTY;
			appendBinaryStringInfo(&func_decl,
									&yyextra->core_yy_extra.scanbuf[tmp_def_yylloc],
									yylloc - tmp_def_yylloc + 1);
		}
		else if (tok == END_P || (tok != 0 && read_stmt))
		{
			/*
			 * Read an END or tokens other than AS/IS/BEGIN/;
			 */
			YYLTYPE	end_loc = -1;
			YYLTYPE	pre_loc = yylloc; /* END token location. */
			int		tok_idx = -1; /* Point to END token for read_stmt == true. */

			if (read_stmt)
			{
				/*
				 * Not a END LOOP/IF/CASE or END ident or END; If found, return the location
				 * of END from pre_loc.
				 */
				if (!plpgsql_has_end_token(&tok_idx, &pre_loc, &hist_cache))
					continue;
			}

			/*
			 * The END keyword pattern should be:
			 * - K_END opt_label for a named block
			 * - K_END K_IF for a IF block
			 * - K_END K_CASE for a CASE WHEN
			 * - K_END K_LOOP for a LOOP
			 */
			read_one_token(&tok, &yylval, &yylloc, yyscanner, read_stmt,
									&tok_idx, &hist_cache);
			if (tok == IF_P || tok == CASE ||
					(tok == IDENT && strcasecmp(yylval.str, "loop") == 0))
			{
				read_one_token(&tok, &yylval, &yylloc, yyscanner, read_stmt,
										&tok_idx, &hist_cache);

				/* END IF/CASE/LOOP to continue */
				continue;
			}

			if (list_length(parse_stack) > 0)
				parse_stack = list_delete_first(parse_stack);

			/* In inner 'BEGIN...END;' */
			if (list_length(parse_stack) != 0)
				continue;

			/*
			 * But we expected a 'END <object name>' or a 'END;' for a complete
			 * body.
			 */
			/* Eat END or END procname from the last core_yylex */
			end_loc = yylloc;

			if (!is_fun_sp) /* need not save END in package definition */
			{
				if (pre_loc > 0 && yyextra->core_yy_extra.scanbuf[pre_loc - 1] != ';')
					end_loc = pre_loc - 1;
				else
					end_loc = pre_loc;
			}
			else if (tok == ';')
				end_loc = yylloc;
			else if (tok == 0)
				end_loc = pre_loc + 3; /* End of input is fine. */
			else
			{
				if (strcasecmp(yylval.str, objname) != 0)
					elog(ERROR, "expected end of \"%s\", but meet %s",
									objname, yylval.str);

				read_one_token(&tok, &yylval, &yylloc, yyscanner, read_stmt,
										&tok_idx, &hist_cache);
				if (!is_fun_sp)
					end_loc = yylloc;
				if (tok != ';')
					elog(ERROR, "unexpected end of PLPGSQL body");
			}

			/* It's time to reset statement mode */
			read_stmt = false;
			reset_hist_token_cache(&hist_cache);

			/*
			 * At this point, we are done in function definition since the last
			 * END token.
			 */
			if (func_st != FUNC_DECL && func_st != FUNC_EMPTY)
			{
				func_st = FUNC_EMPTY;

				/*
				 * A complete function definition, continue to searching if
				 * there is a another BEGIN..END.
				 */
				tmp_init_yylloc = -1;

				continue;
			}

			if (tmp_init_yylloc != -1 && yyextra->initialize_idx == -1)
				yyextra->initialize_idx = tmp_init_yylloc - start_body_loc;

			/*
			 * If met a END after function declaration, we are done. And if the
			 * last block is a function definition, we will continue for the
			 * last END.
			 */
			if (end_loc != -1)
			{
				StringInfoData	strb;

				initStringInfo(&strb);

				if (is_fun_sp && !has_declare)
					appendStringInfo(&strb, "declare ");

				if (is_fun_sp && tok == ';' && la_next_tok != NULL)
				{
					la_next_tok->token = ';';
					la_next_tok->loc = yylloc;
				}

				if (end_loc - start_body_loc > 0)
					appendBinaryStringInfo(&strb, &yyextra->core_yy_extra.scanbuf[start_body_loc],
												end_loc - start_body_loc);
				/*
				 * Append END for initialization part.
				 */
				if (!is_fun_sp && yyextra->initialize_idx != -1)
					appendStringInfo(&strb, " \nend");

				/*
				 * Return the collected function specifications.
				 */
				func_decl.data[func_decl.len] = '\0';
				yyextra->func_spec = func_decl.data;

				return strb.data;
			}
			else
			{
				yyextra->func_spec = func_decl.data;
				return NULL;
			}
		}
		else if (tok == 0)
		{
			yyextra->func_spec = func_decl.data;
			return NULL;
		}
	}

	return NULL;
}

char *
get_type_object_body_obsolete(core_yyscan_t yyscanner, YYLTYPE *yylloc)
{
	core_YYSTYPE	yylval;
	YYLTYPE init_loc = *yylloc;
	StringInfoData	src_def, func_def;
	int prev_token = 0, prev_prev_token = 0;

	YYLTYPE prev_loc = 0, prev_prev_loc = 0;
	List *insert_list = NULL;
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	char *scan_buf = yyextra->core_yy_extra.scanbuf;
	int tmp_token;
	bool entercons = false;

	initStringInfo(&src_def);
	initStringInfo(&func_def);
	prev_token = yyextra->history_tokens[yyextra->cur_his_tok % NUM_HIS_TOKEN].token;

	do
	{
		tmp_token = core_yylex(&yylval, yylloc, yyscanner);
		if (tmp_token != 0)
		{
			if (tmp_token == FUNCTION && prev_token == CONSTRUCTOR_P)
				entercons = true;
			if (tmp_token == ';' && prev_token == END_P)
				entercons = false;
			/* constructor syntax is return stmt is "return ;", here record the position */
			if (tmp_token == ';' && prev_token == RETURN && entercons)
			{
				if (insert_list == NULL)
				{
					insert_list = list_make1_int(*yylloc);
				}
				else
				{
					insert_list = list_append_unique_int(insert_list, *yylloc);
				}
			}
			prev_prev_token = prev_token;
			prev_prev_loc = prev_loc;
			prev_token = tmp_token;
			prev_loc = *yylloc;
		}
	} while(tmp_token != 0);
	if (prev_token == ';' && prev_prev_token == END_P)
	{
		appendBinaryStringInfo(&src_def,
								scan_buf + (int)init_loc,
								(int)prev_prev_loc - (int)init_loc);
	}
	else
	{
		elog(ERROR, "illegal type body definition");
	}
	
	/*
		we have to insert 'self' after return of a constructor function 
		which will modify "return; " to "return self; "
	*/
	if (insert_list != NULL)
	{
		ListCell *lc;
		int pos;
		int offset = (int)init_loc;
		const char *self = " self";
		foreach(lc, insert_list)
		{
			pos = lc->data.int_value;
			appendBinaryStringInfo(&func_def, scan_buf + offset, pos - offset);
			appendBinaryStringInfo(&func_def, self, strlen(self));
			offset = pos;
		}
		if (offset < (int)prev_prev_loc)
		{
			appendBinaryStringInfo(&func_def, scan_buf + offset, prev_prev_loc - offset);
		}
	}
	else /* if without cnstructor, just copy it*/
	{
		appendBinaryStringInfo(&func_def, src_def.data, src_def.len);
	}
	yyextra->initialize_idx = src_def.len;
	appendBinaryStringInfo(&src_def, func_def.data, func_def.len);
	yyextra->func_spec = src_def.data;
	elog(DEBUG5, "type body: %s", src_def.data);
	return src_def.data;
}

Node *
replaceCastNode(TypeCast *src, Oid targetoid, int32 typmod)
{
    int location;
    TypeName *typename = NULL;
    Node *dst = NULL;
    Node *arg = NULL;

    if (src == NULL)
    {
        return NULL;
    }

    location = src->location;
    typename = makeTypeNameFromOid(targetoid, typmod);
    arg = src->arg;

    dst = makeTypeCast(arg, typename, location);
    return dst;
}

/*
 * We will lower all the identifier while ENABLE_LOWER_ORA_CI_IDENT
 * is true. However, there are speical identifier name of collate
 * "C" and "POSIX" which are inserted into pg_catalog by initdb.
 *
 * We can't lower them but we also can't judge whether the identifier
 * is COLLATE "C" and "POSIX" or not while it is in plpgsql,
 * so the only we can do is just lower all identifiers in plpgsql
 * and process this case in pl_gram.y.
 *
 * It will cause something weird in plpgsql:
 * 1. COLLATE c will be converted to COLLATE C and will be success
 * while it is failed before.
 * 2. The notice log is identifier "C" will be converted to lowercase
 * while enable_lower_ora_ci_ident is on, but we still lookup the
 * collation with "C".
 * The situation of POSIX is as same as "C". We think they are
 * harmless and can be ignored now.
 */
bool
processSpecialCollateIdent(char *collname, bool update_name)
{
	//if (ENABLE_LOWER_ORA_CI_IDENT)
	if (ORA_MODE)
	{
		if (strcmp(collname, "c") == 0)
		{
			if (update_name)
				strcpy(collname, "C");
			return true;
		}
		else if (strcmp(collname, "posix") == 0)
		{
			if (update_name)
				strcpy(collname, "POSIX");
			return true;
		}
	}

	return false;
}

/*
 * Whether token is operator likes '+', '-', '*', '/', etc.
 * This mimics operators listed in a_expr in gram.y.
 */
static bool
token_is_operator(int token)
{
	return (token == '+' || token == '-' || token == '*' ||
		token == '/' || token == '%' || token == '^' ||
		token == '<' || token == '>' || token == '=' ||
		token == LESS_EQUALS || token == GREATER_EQUALS ||
		token == NOT_EQUALS ||
		token == CONCATENATION || token == AND || token == OR ||
		token == LIKE || token == NOT_LA || token == ILIKE ||
		token == SIMILAR || token == IS || token == ISNULL ||
		token == NOTNULL || token == OVERLAPS || token == BETWEEN ||
		token == IN_P);
}

static bool
token_is_mathop(int token)
{
		return (token == '+' || token == '-' || token == '*' ||
		token == '/' || token == '%' || token == '^' ||
		token == '<' || token == '>' || token == '=' ||
		token == LESS_EQUALS || token == GREATER_EQUALS ||
		token == NOT_EQUALS);
}
