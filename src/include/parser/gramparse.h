/*-------------------------------------------------------------------------
 *
 * gramparse.h
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

#ifndef GRAMPARSE_H
#define GRAMPARSE_H

#include "nodes/parsenodes.h"
#include "parser/scanner.h"

/*
 * NB: include gram.h only AFTER including scanner.h, because scanner.h
 * is what #defines YYLTYPE.
 */
#include "parser/gram.h"

#ifdef _PG_ORCL_
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
#endif


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
	char	   *lookahead_end;	/* end of current token */
	char		lookahead_hold_char;	/* to be put back at *lookahead_end */

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */
#ifdef _PG_ORCL_		
	ora_yy_lookahead_type lookahead[ORA_YY_MAX_LOOKAHEAD];
	bool	is_mult_insert;

	/* plpgsql related */
	bool	try_ora_plsql;
	bool	is_func_sp;
	bool    is_trigger_sp;
	NameData	pl_def_name;
	int		history_tokes[NUM_HIS_TOKEN];
	int		cur_his_tok;

	/*
	 * Indexes of package body for private declaration, definition or
	 * initialization part.
	 */
	int		func_def_idx; /* -1 not function/process definition */
	int		initialize_idx; /* -1 not function/process definition */
	char	*func_spec;
#endif
} base_yy_extra_type;

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define pg_yyget_extra(yyscanner) (*((base_yy_extra_type **) (yyscanner)))
#endif

#define PS_TYPE_BEGIN	1
#define PS_TYPE_CASE	2

#define FUNC_EMPTY 0
#define FUNC_DECL  1
#define FUNC_DEF   2
#define FUNC_BEGIN 3

/* from parser.c */
extern int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp,
					  core_yyscan_t yyscanner);

#if 0
/* from gram.y */
extern void parser_init(base_yy_extra_type *yyext);
extern int	base_yyparse(core_yyscan_t yyscanner);
extern void extract_package_body_defs(CreatePackageBodyStmt *n,
										base_yy_extra_type *yyextra, char *body);
extern void reset_parser_statement(base_yy_extra_type *yyextra);
extern char *get_plpgsql_body(core_yyscan_t yyscanner, char *objname,
								ora_yy_lookahead_type *la, bool is_func_sp,
								ora_yy_lookahead_type *la_nex, int start_body_loc);

#endif							/* GRAMPARSE_H */
