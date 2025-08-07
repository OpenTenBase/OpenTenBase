/*-------------------------------------------------------------------------
 *
 * parse_relation.h
 *	  prototypes for parse_relation.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_relation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_RELATION_H
#define PARSE_RELATION_H

#include "parser/parse_node.h"

#define XMLTABLE_ALIAS (ORA_MODE ? ("XMLTABLE") : ("xmltable"))

/*
 * Support for fuzzily matching column.
 *
 * This is for building diagnostic messages, where non-exact matching
 * attributes are suggested to the user.  The struct's fields may be facets of
 * a particular RTE, or of an entire range table, depending on context.
 */
typedef struct
{
	int			distance;		/* Weighted distance (lowest so far) */
	RangeTblEntry *rfirst;		/* RTE of first */
	AttrNumber	first;			/* Closest attribute so far */
	RangeTblEntry *rsecond;		/* RTE of second */
	AttrNumber	second;			/* Second closest attribute so far */
} FuzzyAttrMatchState;


extern RangeTblEntry *refnameRangeTblEntry(ParseState *pstate,
					 const char *schemaname,
					 const char *refname,
					 int location,
					 int *sublevels_up);
extern CommonTableExpr *scanNameSpaceForCTE(ParseState *pstate,
					const char *refname,
					Index *ctelevelsup);
extern bool scanNameSpaceForENR(ParseState *pstate, const char *refname);
extern void checkNameSpaceConflicts(ParseState *pstate, List *namespace1,
						List *namespace2);
extern int RTERangeTablePosn(ParseState *pstate,
				  RangeTblEntry *rte,
				  int *sublevels_up);
extern RangeTblEntry *GetRTEByRangeTablePosn(ParseState *pstate,
					   int varno,
					   int sublevels_up);
extern CommonTableExpr *GetCTEForRTE(ParseState *pstate, RangeTblEntry *rte,
			 int rtelevelsup);
extern Node *scanRTEForColumn(ParseState *pstate, RangeTblEntry *rte,
				 char *colname, int location,
				 int fuzzy_rte_penalty, FuzzyAttrMatchState *fuzzystate);
extern Node *colNameToVar(ParseState *pstate, char *colname, bool localonly,
			 int location);
extern TargetEntry *scanNameSpaceForTle(ParseState *pstate, char *colname, 
				List *tlist, int location);
extern void markVarForSelectPriv(ParseState *pstate, Var *var,
					 RangeTblEntry *rte);
extern Relation parserOpenTable(ParseState *pstate, const RangeVar *relation,
				int lockmode);
extern List *refnameRangeTblEntries(ParseState *pstate, const char *refname, int location);
#ifdef __OPENTENBASE__
extern Relation parserOpenTableNewRel(ParseState *pstate, const RangeVar *relation, int lockmode, RangeVar **new_rel);
#endif
extern RangeTblEntry *addRangeTableEntry(ParseState *pstate,
				   RangeVar *relation,
				   Alias *alias,
				   bool inh,
				   bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForRelation(ParseState *pstate,
							  Relation rel,
							  Alias *alias,
							  bool inh,
							  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForSubquery(ParseState *pstate,
							  Query *subquery,
							  Alias *alias,
							  bool lateral,
							  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForFunction(ParseState *pstate,
							  List *funcnames,
							  List *funcexprs,
							  List *coldeflists,
							  RangeFunction *rangefunc,
							  bool lateral,
							  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForValues(ParseState *pstate,
							List *exprs,
							List *coltypes,
							List *coltypmods,
							List *colcollations,
							Alias *alias,
							bool lateral,
							bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForTableFunc(ParseState *pstate,
							   TableFunc *tf,
							   Alias *alias,
							   bool lateral,
							   bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForJoin(ParseState *pstate,
						  List *colnames,
						  JoinType jointype,
						  List *aliasvars,
						  Alias *alias,
						  bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForCTE(ParseState *pstate,
						 CommonTableExpr *cte,
						 Index levelsup,
						 RangeVar *rv,
						 bool inFromCl);
extern RangeTblEntry *addRangeTableEntryForENR(ParseState *pstate,
						 RangeVar *rv,
						 bool inFromCl);
extern bool isLockedRefname(ParseState *pstate, const char *refname);
extern void addRTEtoQuery(ParseState *pstate, RangeTblEntry *rte,
			  bool addToJoinList,
			  bool addToRelNameSpace, bool addToVarNameSpace);
extern void errorMissingRTE(ParseState *pstate, RangeVar *relation) pg_attribute_noreturn();
extern void errorMissingColumn(ParseState *pstate,
				   char *relname, char *colname, int location) pg_attribute_noreturn();
extern void expandRTE(RangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  List **colnames, List **colvars);
extern List *expandRelAttrs(ParseState *pstate, RangeTblEntry *rte,
			   int rtindex, int sublevels_up, int location);
extern int	attnameAttNum(Relation rd, const char *attname, bool sysColOK);
extern int  getAttNumByAttname(ParseState *pstate, ResTarget *target, RangeVar *targetRangeVar);
extern int  getAttNumByAttnameSubquery(RangeTblEntry *rangeTblEntry, ResTarget *target);
extern Name attnumAttName(Relation rd, int attid);
extern Oid	attnumTypeId(Relation rd, int attid);
extern Oid	attnumCollationId(Relation rd, int attid);
extern bool isQueryUsingTempRelation(Query *query);
extern bool MetaGlobalTempTblReplaceWalker(Node *node, void *context);

#ifdef PGXC
extern int	specialAttNum(const char *attname);
#endif


/* Parse the partition xxx in the select/delete/insert/update */
extern RangeVar *parse_partition_relname(ParseState *pstate, RangeVar *relation);
#endif							/* PARSE_RELATION_H */
