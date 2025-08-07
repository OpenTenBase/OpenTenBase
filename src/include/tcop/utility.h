/*-------------------------------------------------------------------------
 *
 * utility.h
 *	  prototypes for utility.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/utility.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UTILITY_H
#define UTILITY_H

#include "tcop/tcopprot.h"
#ifdef __OPENTENBASE__
#include "lib/stringinfo.h"
#endif
typedef enum
{
	PROCESS_UTILITY_TOPLEVEL,	/* toplevel interactive command */
	PROCESS_UTILITY_QUERY,		/* a complete query, but not toplevel */
	PROCESS_UTILITY_QUERY_NONATOMIC, /* a complete query, nonatomic execution context */
	PROCESS_UTILITY_SUBCOMMAND	/* a portion of a query */
} ProcessUtilityContext;
extern bool allow_modify_index_table;
/* Hook for plugins to get control in ProcessUtility() */
typedef void (*ProcessUtility_hook_type) (PlannedStmt *pstmt,
										  const char *queryString,
										  bool readOnlyTree,
										  ProcessUtilityContext context,
										  ParamListInfo params,
										  QueryEnvironment *queryEnv,
										  DestReceiver *dest,
										  bool sentToRemote,
										  char *completionTag);
extern PGDLLIMPORT ProcessUtility_hook_type ProcessUtility_hook;

extern void ProcessUtility(PlannedStmt *pstmt, const char *queryString,bool readOnlyTree,
			   ProcessUtilityContext context, ParamListInfo params,
			   QueryEnvironment *queryEnv,
			   DestReceiver *dest,
			   bool sentToRemote,
			   char *completionTag);
extern void standard_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						bool readOnlyTree,
						ProcessUtilityContext context, ParamListInfo params,
						QueryEnvironment *queryEnv,
						DestReceiver *dest,
						bool sentToRemote,
						char *completionTag);

extern bool UtilityReturnsTuples(Node *parsetree);

extern TupleDesc UtilityTupleDescriptor(Node *parsetree);

extern Query *UtilityContainsQuery(Node *parsetree);

extern const char *CreateCommandTag(Node *parsetree);

extern LogStmtLevel GetCommandLogLevel(Node *parsetree);

extern bool CommandIsReadOnly(PlannedStmt *pstmt);

#ifdef PGXC
extern bool pgxc_lock_for_utility_stmt(Node *parsetree);
#endif
#ifdef __OPENTENBASE__
typedef void (*ErrcodeHookType) (ErrorData *edata, StringInfo buff);
extern PGDLLIMPORT __thread ErrcodeHookType g_pfErrcodeHook;
#endif
#endif							/* UTILITY_H */
