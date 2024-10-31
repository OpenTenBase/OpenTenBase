/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *        parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/parser/parse_utilcmd.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "parser/parse_node.h"

#ifdef __COLD_HOT__
extern bool loose_unique_index;
#endif

#ifdef __OPENTENBASE__
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString,
					bool autodistribute, Oid *nspaceid, bool existsok);
extern List *transformChildPartBounds(CreateStmt *parent_stmt);
extern CreateStmt *transformPartitionCmd2CreateStmt(PartitionCmd *partcmd, AlterTableStmt *atstmt);
#elif XCP
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString,
                    bool autodistribute);
#else
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString);
#endif

#ifdef XCP
extern bool loose_constraints;
#endif

extern List *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
                        const char *queryString);
extern IndexStmt *transformIndexStmt(Oid relid, IndexStmt *stmt,
                   const char *queryString);
extern void transformRuleStmt(RuleStmt *stmt, const char *queryString,
                  List **actions, Node **whereClause);
extern List *transformCreateSchemaStmt(CreateSchemaStmt *stmt);
#ifdef PGXC
extern bool CheckLocalIndexColumn (char loctype, char *partcolname, char *indexcolname);
#endif
extern PartitionBoundSpec *transformPartitionBound(ParseState *pstate, Relation parent,
                        PartitionBoundSpec *spec);
extern IndexStmt *generateClonedIndexStmt(RangeVar *heapRel, Oid heapOid,
                                                 Relation source_idx,
						const AttrNumber *attmap, int attmap_length,
						Oid *constraintOid);

#endif                            /* PARSE_UTILCMD_H */
