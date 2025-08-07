/* ---------------------------------------------------------------------------------------
 *
 * parse_merge.h
 *        handle merge-stmt in parser
 *
 *
 * IDENTIFICATION
 *        src/include/parser/parse_merge.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARSE_MERGE_H
#define PARSE_MERGE_H

#include "parser/parse_node.h"
#include "c.h"

extern Query* transformMergeStmt(ParseState* pstate, MergeStmt* stmt);
extern List  *expandTargetTL(List *te_list, RangeTblEntry *target_rte, Index rtindex);
extern void   checkMergeUpdateOnJoinKey(Query *parse, List *target, List *join_var_list);
extern void   CheckMergeTargetValid(Relation targetRel);
#endif
