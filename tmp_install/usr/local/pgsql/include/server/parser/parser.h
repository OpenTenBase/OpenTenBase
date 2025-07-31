/*-------------------------------------------------------------------------
 *
 * parser.h
 *        Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#include "nodes/parsenodes.h"


typedef enum
{
    BACKSLASH_QUOTE_OFF,
    BACKSLASH_QUOTE_ON,
    BACKSLASH_QUOTE_SAFE_ENCODING
}            BackslashQuoteType;

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern int    backslash_quote;
extern bool escape_string_warning;
extern PGDLLIMPORT bool standard_conforming_strings;


/* Primary entry point for the raw parsing functions */
extern List *raw_parser(const char *str);

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List *SystemFuncName(char *name);
extern TypeName *SystemTypeName(char *name);

#ifdef _PG_ORCL_
extern List *OracleFuncName(char *name);
#endif

#endif                            /* PARSER_H */
