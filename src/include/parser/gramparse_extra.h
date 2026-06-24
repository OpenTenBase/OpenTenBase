/*-------------------------------------------------------------------------
 *
 * gramparse_extra.h
 *		Shared definitions for the "raw" parser (flex and bison phases only)
 *
 * NOTE: this file is only meant to be included in the core parsing files,
 * ie, parser.c, gram.y, scan.l, and src/common/keywords.c.
 * Definitions that are needed outside the core parser should be in parser.h.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/gramparse.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GRAMPARSE_EXTRA_H
#define GRAMPARSE_EXTRA_H

#include "nodes/parsenodes.h"
#include "parser/scanner.h"

enum ora_parse_scope {
	ORA_SELECT_SCOPE = 0,
	ORA_INSERT_SCOPE,
	ORA_UPDATE_SCOPE,
	ORA_MERGE_SCOPE,
	ORA_SORT_SCOPE,
	ORA_OVERLAY_SCOPE,
	ORA_CASE_WHEN_SCOPE,
	ORA_TYPECAST_SCOPE,
	ORA_UNKNOWN_SCOPE
};

/*
 * NB: include gram.h only AFTER including scanner.h, because scanner.h
 * is what #defines YYLTYPE.
 */
//#include "parser/gram.h"

typedef struct ora_yy_lookahead_type
{
	core_YYSTYPE 	lval;
	int 			token;
	YYLTYPE			loc;
}ora_yy_lookahead_type;

#define ORA_YY_MAX_LOOKAHEAD 2
#define ORA_INVALID_TOKEN -1

/*
 * For histry tokens to recognize which statement is parsing.
 */
#define NUM_HIS_TOKEN	8


/*
 * The YY_EXTRA data that a flex scanner allows us to pass around.  Private
 * state needed for raw parsing/lexing goes here.
 */
typedef struct base_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	core_yy_extra_type core_yy_extra;

	/*
	 * State variables for base_yylex().
	 */
	bool		have_lookahead; /* is lookahead info valid? */
	int			lookahead_token;	/* one-token lookahead */
	core_YYSTYPE lookahead_yylval;	/* yylval for lookahead token */
	YYLTYPE		lookahead_yylloc;	/* yylloc for lookahead token */
	char		*lookahead_end;	/* end of current token */
	char		lookahead_hold_char;	/* to be put back at *lookahead_end */

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */

	ora_yy_lookahead_type lookahead[ORA_YY_MAX_LOOKAHEAD];
	ora_yy_lookahead_type	history_tokens[NUM_HIS_TOKEN];
	int		cur_his_tok;
	bool	is_mult_insert;

	/* plpgsql related */
	bool	try_ora_plsql;
	bool	is_func_sp;
	bool	is_trigger_sp;
	bool	is_pkg_sp;
	bool	start_with_clause;
	NameData	pl_def_name;

	int		func_def_idx; /* -1 not function/process definition */
	int		initialize_idx; /* -1 not function/process definition */
	char	*func_spec;

	/* Dblink syntax support, supports syntax such as tb1@t_link1 */
	bool	check_dblink;

	/*
	 * Indexes of package body for private declaration, definition or
	 * initialization part.
	 */
	bool    has_type_token;
	bool    is_type_object_spec_sp;
	bool    is_type_object_body_sp;
	bool    do_as_alias;

	bool keywords_as_identifier;
	bool *current_scope;
	List *scope_stack;
	int64 query_level;
	int32 global_scope_cnt[ORA_UNKNOWN_SCOPE];
} base_yy_extra_type;

extern void ORA_ENTER_SCOPE(enum ora_parse_scope scope, base_yy_extra_type *extra);
extern void ORA_LEAVE_SCOPE(enum ora_parse_scope scope, base_yy_extra_type *extra);

#define IS_IN_SCOPE(scope, extra)(((extra)->keywords_as_identifier && (extra)->current_scope) \
									? (extra)->current_scope[scope] : false)
#define IS_SCOPE_SET(scope, extra)((extra)->keywords_as_identifier ? (extra)->global_scope_cnt[scope] > 0 : false)
/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define pg_yyget_extra(yyscanner) (*((base_yy_extra_type **) (yyscanner)))

/* from parser.c */
extern void parser_init(base_yy_extra_type *yyext);
extern int	base_yyparse(core_yyscan_t yyscanner);

/* from gram_pg.y */
extern void pg_parser_init(base_yy_extra_type *yyext);
extern int	pg_base_yyparse(core_yyscan_t yyscanner);

/* from gram_ora.y */
extern void ora_parser_init(base_yy_extra_type *yyext);
extern int	ora_base_yyparse(core_yyscan_t yyscanner);
extern void reset_parser_statement(base_yy_extra_type *yyextra);

extern void reset_parser_statement(base_yy_extra_type *yyextra);
extern void extract_package_body_defs(CreatePackageBodyStmt *n,
									  base_yy_extra_type *yyextra, char *body);
extern char *get_plpgsql_body(core_yyscan_t yyscanner, char *objname,
							  ora_yy_lookahead_type *la, bool is_func_sp,
							  ora_yy_lookahead_type *la_nex,
							  int start_body_loc);
extern void get_type_object_spec_func_decl(core_yyscan_t yyscanner, YYLTYPE *yylloc);
extern char *get_type_object_body(core_yyscan_t yyscanner, YYLTYPE *yylloc);
extern char *get_type_object_body_obsolete(core_yyscan_t yyscanner, YYLTYPE *yylloc);
//extern void getTypeBodySrcAndFunDef(CreateTypeObjectBodyStmt *stmt, char *src, int offset);
extern Node *replaceCastNode(TypeCast *src, Oid targetoid, int32 typmod);
extern bool processSpecialCollateIdent(char *collname, bool update_name); 
#endif							/* GRAMPARSE_H */
