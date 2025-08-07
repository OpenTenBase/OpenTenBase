/*-------------------------------------------------------------------------
 *
 * parse_param.h
 *	  handle parameters in parser
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_param.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_PARAM_H
#define PARSE_PARAM_H

#include "parser/parse_node.h"

extern void parse_fixed_parameters(ParseState *pstate,
					   Oid *paramTypes, int numParams);
extern void parse_variable_parameters(ParseState *pstate,
						  Oid **paramTypes, int *numParams);
extern void check_variable_parameters(ParseState *pstate, Query *query);
extern bool query_contains_extern_params(Query *query);
extern bool contains_params(Node *node);
Bitmapset *pull_exec_paramids(Node *node);
List *nullify_prior_params(List *exprs, Bitmapset *prior_params);
extern bool contain_prior_params(Node *node, Bitmapset *priors);
#endif							/* PARSE_PARAM_H */
