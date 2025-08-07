/*-------------------------------------------------------------------------
 *
 * rewriteHandler.h
 *		External interface to query rewriter.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rewrite/rewriteHandler.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REWRITEHANDLER_H
#define REWRITEHANDLER_H

#include "utils/relcache.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "utils/queryenvironment.h"

#define ADD_MODIFIED_COLS(m_clos, tlist)                                                           \
	do                                                                                             \
	{                                                                                              \
		ListCell *lc;                                                                              \
		foreach (lc, tlist)                                                                        \
		{                                                                                          \
			TargetEntry *tle = (TargetEntry *) lfirst(lc);                                         \
                                                                                                   \
			if (!tle->resjunk)                                                                     \
				m_clos = bms_add_member(m_clos, tle->resno - FirstLowInvalidHeapAttributeNumber);  \
		}                                                                                          \
	} while (0)

extern List *QueryRewrite(Query *parsetree);
extern void AcquireRewriteLocks(Query *parsetree,
					bool forExecute,
					bool forUpdatePushedDown);

extern Node *build_column_default(Relation rel, int attrno);

extern Query *get_view_query(Relation view);
extern const char *view_query_is_auto_updatable(Query *viewquery, CmdType cmdType);
extern int relation_is_updatable(Oid reloid,
					  bool include_triggers,
					  Bitmapset *include_cols);
#ifdef PGXC
extern List *QueryRewriteCTAS(Query *parsetree,
                              ParserSetupHook parserSetup,
                              void *parserSetupArg,
                              QueryEnvironment *queryEnv,
                              Oid *paramTypes,
                              int numParams);
#endif
extern void HandleGlobalTempTable(CmdType commandType, Node *utilityStmt);
extern Oid ActivateGlobalTempTable(Relation metarel, Oid metaOid);
#endif	/* REWRITEHANDLER_H */
