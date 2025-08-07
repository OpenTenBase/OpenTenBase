/*-------------------------------------------------------------------------
 *
 * parse_clause.h
 *	  handle clauses in parser
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_clause.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_CLAUSE_H
#define PARSE_CLAUSE_H

#include "parser/parse_node.h"

/* Convenience macro for the most common makeNamespaceItem() case */
#define makeDefaultNSItem(rte)	makeNamespaceItem(rte, true, true, false, true)

extern void transformFromClause(ParseState *pstate, List *frmList);
extern int setTargetTable(ParseState *pstate, RangeVar *relation,
			   bool inh, bool alsoSource, AclMode requiredPerms);
extern bool interpretOidsOption(List *defList, bool allowOids);
#ifdef _PG_ORCL_
extern bool interpretRowIdOption(List *defList, bool allowRowId);
#endif
#ifdef _SHARDING_
extern bool interpretExtentOption(List *defList, bool allowExtent);
#endif
#ifdef __OPENTENBASE_C__
extern char interpretStoreOption(CreateStmt* stmt_loc, char relkind);
#endif
extern Node *transformWhereClause(ParseState *pstate, Node *clause,
					 ParseExprKind exprKind, const char *constructName);
extern Node *transformLimitClause(ParseState *pstate, Node *clause,
					 ParseExprKind exprKind, const char *constructName, LimitOption limitOption);
extern List *transformGroupClause(ParseState *pstate, List *grouplist,
					 List **groupingSets,
					 List **targetlist, List *sortClause,
					 ParseExprKind exprKind, bool useSQL99);
extern List *transformSortClause(ParseState *pstate, List *orderlist,
					List **targetlist, ParseExprKind exprKind,
					bool useSQL99);

extern List *transformWindowDefinitions(ParseState *pstate,
						   List *windowdefs,
						   List **targetlist);

extern List *transformDistinctClause(ParseState *pstate,
						List **targetlist, List *sortClause, bool is_agg);
extern List *transformDistinctOnClause(ParseState *pstate, List *distinctlist,
						  List **targetlist, List *sortClause);
extern void transformOnConflictArbiter(ParseState *pstate,
						   OnConflictClause *onConflictClause,
						   List **arbiterExpr, Node **arbiterWhere,
						   Oid *constraint);

extern List *addTargetToSortList(ParseState *pstate, TargetEntry *tle,
					List *sortlist, List *targetlist, SortBy *sortby);
extern Index assignSortGroupRef(TargetEntry *tle, List *tlist);
extern bool targetIsInSortList(TargetEntry *tle, Oid sortop, List *sortList);

extern Node *transformFromClauseItem(ParseState *pstate, Node *n, RangeTblEntry **top_rte, int *top_rti,
                                     List **namespace, bool isMergeInto);
extern void parseDBlinkItem(ParseState *pstate, RangeVar *rv);
extern ParseNamespaceItem *makeNamespaceItem(RangeTblEntry *rte, bool rel_visible, bool cols_visible,
                                             bool lateral_only, bool lateral_ok);

#endif							/* PARSE_CLAUSE_H */
