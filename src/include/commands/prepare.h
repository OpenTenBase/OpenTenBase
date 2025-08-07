/*-------------------------------------------------------------------------
 *
 * prepare.h
 *	  PREPARE, EXECUTE and DEALLOCATE commands, and prepared-stmt storage
 *
 *
 * Copyright (c) 2002-2017, PostgreSQL Global Development Group
 *
 * src/include/commands/prepare.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREPARE_H
#define PREPARE_H

#include "commands/explain.h"
#include "datatype/timestamp.h"
#include "utils/plancache.h"

/*
 * The data structure representing a prepared statement.  This is now just
 * a thin veneer over a plancache entry --- the main addition is that of
 * a name.
 *
 * Note: all subsidiary storage lives in the referenced plancache entry.
 */
typedef struct
{
	/* dynahash.c requires key to be first field */
	char		stmt_name[NAMEDATALEN];
	CachedPlanSource *plansource;	/* the actual cached plan */
	bool		from_sql;		/* prepared via SQL, not FE/BE protocol? */
#ifdef XCP	
	ResourceOwner owner;		/* does it use resowner for tracking? */
#endif
	TimestampTz prepare_time;	/* the time when the stmt was prepared */
} PreparedStatement;

#ifdef PGXC
typedef struct
{
	char	stmt_name[NAMEDATALEN];			/* dynahash.c requires key to be first field */
	int 	number_of_nodes;				/* number of nodes where statement is active */
	PGXCNodeHandle *handles[0];	/* node handles where statement is active */
} DatanodeStatement;
#endif

/* Utility statements PREPARE, EXECUTE, DEALLOCATE, EXPLAIN EXECUTE */
extern void PrepareQuery(PrepareStmt *stmt, const char *queryString,
			 int stmt_location, int stmt_len);
extern void ExecuteQuery(ExecuteStmt *stmt, IntoClause *intoClause,
			 const char *queryString, ParamListInfo params,
			 DestReceiver *dest, char *completionTag);
extern void DeallocateQuery(DeallocateStmt *stmt);
extern void ExplainExecuteQuery(ExecuteStmt *execstmt, IntoClause *into,
					ExplainState *es, const char *queryString,
					ParamListInfo params, QueryEnvironment *queryEnv);

/* Low-level access to stored prepared statements */
extern void StorePreparedStatement(const char *stmt_name,
					   CachedPlanSource *plansource,
					   bool from_sql,
					   bool use_resowner);
extern PreparedStatement *FetchPreparedStatement(const char *stmt_name,
					   bool throwError);
extern void DropPreparedStatement(const char *stmt_name, bool showError);
extern TupleDesc FetchPreparedStatementResultDesc(PreparedStatement *stmt);
extern List *FetchPreparedStatementTargetList(PreparedStatement *stmt);

extern void DropAllPreparedStatements(void);

#ifdef PGXC
extern DatanodeStatement *FetchDatanodeStatement(const char *stmt_name, bool throwError);
extern bool HaveActiveDatanodeStatements(void);
extern void DropDatanodeStatement(const char *stmt_name);
extern void DestroyDatanodeStatementsHtab(void);
extern int SetRemoteStatementName(Plan *plan, const char *stmt_name, int num_params,
						Oid *param_types, int n);
extern HTAB *GetActiveDatanodeStatementHandles(void);
#endif

#ifdef __OPENTENBASE__
extern void PrepareRemoteDMLStatement(bool upsert, char *stmt, 
                                    char *select_stmt, char *update_stmt);

extern void DropRemoteDMLStatement(char *stmt, char *update_stmt);
extern void RebuildDatanodeQueryHashTable(void);
extern PGXCNodeAllHandles *DatanodeStatementGetHandle(DatanodeStatement *entry, List **nodeList);
extern void DatanodeStatementStoreHandle(DatanodeStatement *entry, PGXCNodeAllHandles *handles);
extern DatanodeStatement *InitDatanodeStatementEntry(char *stmt_name);
#endif

#endif							/* PREPARE_H */
