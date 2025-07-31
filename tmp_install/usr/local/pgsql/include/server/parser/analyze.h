/*-------------------------------------------------------------------------
 *
 * analyze.h
 *        parse analysis for optimizable statements
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/parser/analyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANALYZE_H
#define ANALYZE_H

#include "parser/parse_node.h"

#ifdef __OPENTENBASE__
extern bool g_transform_insert_to_copy;
#endif

/* Hook for plugins to get control at end of parse analysis */
typedef void (*post_parse_analyze_hook_type) (ParseState *pstate,
                                              Query *query);
extern PGDLLIMPORT post_parse_analyze_hook_type post_parse_analyze_hook;


extern Query *parse_analyze(RawStmt *parseTree, const char *sourceText,
              Oid *paramTypes, int numParams, QueryEnvironment *queryEnv);
extern Query *parse_analyze_varparams(RawStmt *parseTree, const char *sourceText,
                        Oid **paramTypes, int *numParams);

extern Query *parse_sub_analyze(Node *parseTree, ParseState *parentParseState,
                  CommonTableExpr *parentCTE,
                  bool locked_from_parent,
                  bool resolve_unknowns);

extern Query *transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree);
extern Query *transformStmt(ParseState *pstate, Node *parseTree);

extern bool analyze_requires_snapshot(RawStmt *parseTree);

extern const char *LCS_asString(LockClauseStrength strength);
extern void CheckSelectLocking(Query *qry, LockClauseStrength strength);
extern void applyLockingClause(Query *qry, Index rtindex,
                   LockClauseStrength strength,
                   LockWaitPolicy waitPolicy, bool pushedDown);

#ifdef XCP
extern void ParseAnalyze_callback(ParseState *pstate, Query *query);
extern post_parse_analyze_hook_type prev_ParseAnalyze_callback;
#endif

#ifdef __OPENTENBASE__
extern List *transformInsertValuesIntoCopyFrom(List *plantree_list, InsertStmt *stmt, bool *success,
                                            char *transform_string, Query    *query);
#endif
#endif                            /* ANALYZE_H */
