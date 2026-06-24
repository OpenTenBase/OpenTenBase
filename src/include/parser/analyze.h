/*-------------------------------------------------------------------------
 *
 * analyze.h
 *		parse analysis for optimizable statements
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/analyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANALYZE_H
#define ANALYZE_H

#include "parser/parse_node.h"
#include "utils/queryjumble.h"

#ifdef __OPENTENBASE__
extern bool g_transform_insert_to_copy;
#endif

/* Hook for plugins to get control at end of parse analysis */
typedef void (*post_parse_analyze_hook_type) (ParseState *pstate,
											  Query *query,
											  JumbleState *jstate);
extern PGDLLIMPORT post_parse_analyze_hook_type post_parse_analyze_hook;


extern Query *parse_analyze(RawStmt *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams, QueryEnvironment *queryEnv, bool isDeparsedQuery);
extern Query *parse_analyze_varparams(RawStmt *parseTree, const char *sourceText,
									  Oid **paramTypes, int *numParams, bool isDeparsedQuery);

extern Query *parse_sub_analyze(Node *parseTree, ParseState *parentParseState,
				  CommonTableExpr *parentCTE,
				  bool locked_from_parent,
				  bool resolve_unknowns);

extern Query *transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree);
extern Query *transformStmt(ParseState *pstate, Node *parseTree);

extern bool analyze_requires_snapshot(RawStmt *parseTree);

extern const char *LCS_asString(LockClauseStrength strength);
extern void CheckSelectLocking(Query *qry, LockClauseStrength strength);
extern void applyLockingClause(Query *qry, Index rtindex, LockClauseStrength strength,
							   LockWaitPolicy waitPolicy, bool pushedDown, int waitTimeout);
extern List *transformUpdateTargetList(ParseState *pstate, List *origTlist, Node *targetRangeVar);
extern List *BuildOnConflictExcludedTargetlist(Relation targetrel, Index exclRelIndex);

#ifdef XCP
extern void ParseAnalyze_callback(ParseState *pstate, Query *query);
extern post_parse_analyze_hook_type prev_ParseAnalyze_callback;
#endif


#ifdef __OPENTENBASE__
extern List *transformInsertValuesIntoCopyFromPlan(List *plantree_list, InsertStmt *stmt, bool *success,
                                                   char *transform_string, Query *query);
extern List *transformInsertValuesIntoCopyFromQuery(InsertStmt *stmt, char *transform_string, Query *query);

extern List *transformInsertRow(ParseState *pstate, List *exprlist, List *stmtcols, List *icolumns, List *attrnos,
                                bool strip_indirection);

extern void fixResTargetNameWithTableNameRef(Relation rd, RangeVar* rel, ResTarget* res);
extern void fixResTargetListWithTableNameRef(Relation rd, RangeVar* rel, List* clause_list);

extern void checkUpdateColValid(TargetEntry *tle, AttrNumber updateAttrno, Relation targetRel,
								List *rtables);
#endif

#endif							/* ANALYZE_H */
