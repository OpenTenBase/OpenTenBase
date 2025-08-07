/*-------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#include "nodes/parsenodes.h"
#include "parser/scanner.h"

typedef enum
{
	BACKSLASH_QUOTE_OFF,
	BACKSLASH_QUOTE_ON,
	BACKSLASH_QUOTE_SAFE_ENCODING
}			BackslashQuoteType;

#define PS_TYPE_BEGIN     1
#define PS_TYPE_CASE      2

#define FUNC_EMPTY 0
#define FUNC_DECL  1
#define FUNC_DEF   2
#define FUNC_BEGIN 3

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern int	backslash_quote;
extern bool escape_string_warning;
extern PGDLLIMPORT bool standard_conforming_strings;
extern bool enable_lightweight_ora_syntax;

extern bool trig_on_table;

extern bool lower_ident_as_const_str;

/* Primary entry point for the raw parsing functions */
extern List *raw_parser(const char *str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List *SystemFuncName(char *name);
extern TypeName *SystemTypeName(char *name);

extern Node *makeBoolAConst(bool state, int location);
extern Node *makeTypeCast(Node *arg, TypeName *typename, int location);
extern Node *makeIntConst(int val, int location);
extern Node *makeStringConst(const char *str, int location);
extern Node *makeStringConstCast(char *str, int location, TypeName *typename);
extern Node *make_string_const_cast_orcl(char *str, int location,
										 TypeName *typename);

extern List *OpenTenBaseOraFuncName(char *name);

extern char *downcase_ident(char *ident);
extern List *downcase_any_name(List *any_name);
extern List *downcase_any_name_list(List *any_name_list);
extern List *downcase_name_list(List *name_list);
extern bool processSpecialCollateIdent(char *collname, bool update_name);
extern void check2pcGid(const char* gid);
extern Node *makeColumnRef_ext(char *colname);

#endif							/* PARSER_H */
