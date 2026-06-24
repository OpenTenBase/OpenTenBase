/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *		parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/parser/parse_utilcmd.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "parser/parse_node.h"

#ifdef XCP
extern bool loose_constraints;
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString,
					bool is_local, bool sentToRemote);
#else
extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString);
#endif
extern List *transformCreateExternalStmt(CreateExternalStmt *stmt, const char *queryString);
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
extern PGXCSubCluster *makeShardSubCluster(Oid groupId);
#endif							/* PARSE_UTILCMD_H */
