/*-------------------------------------------------------------------------
 *
 * analyze.c
 *	  transform the raw parse tree into a query tree
 *
 * For optimizable statements, we are careful to obtain a suitable lock on
 * each referenced table, and other modules of the backend preserve or
 * re-obtain these locks before depending on the results.  It is therefore
 * okay to do significant semantic analysis of these statements.  For
 * utility commands, no locks are obtained here (and if they were, we could
 * not be sure we'd still have them at execution).  Hence the general rule
 * for utility commands is to just dump them into a Query node untransformed.
 * DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are exceptions because they
 * contain optimizable statements, which we should transform.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/parser/analyze.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#ifdef XCP
#include "catalog/index.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#endif
#ifdef PGXC
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/indexing.h"
#include "utils/tqual.h"
#endif
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/var.h"
#include "optimizer/clauses.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cte.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_param.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parsetree.h"
#include "parser/parse_target.h"
#ifdef _MLS_
#include "postmaster/postmaster.h"
#endif
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteHandler.h"
#ifdef PGXC
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "access/gtm.h"
#include "utils/lsyscache.h"
#include "pgxc/planner.h"
#include "tcop/tcopprot.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#include "pgxc/xc_maintenance_mode.h"
#include "access/xact.h"
#endif
#include "utils/rel.h"
#ifdef _MLS_
#include "utils/datamask.h"
#endif
#ifdef __OPENTENBASE__
#include "optimizer/pgxcship.h"
#include "audit/audit_fga.h"
#include "pgstat.h"
#include "optimizer/clauses.h"
#include "utils/memutils.h"
#include "pgxc/locator.h"
#include "foreign/foreign.h"
#endif

#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/heap.h"
#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/indexing.h"
#include "commands/defrem.h"
#include "utils/fmgroids.h"
#include "utils/tqual.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/lsyscache.h"
#include "optimizer/subselect.h"
#include "optimizer/spm.h"
#include "tcop/tcopprot.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#include "pgxc/xc_maintenance_mode.h"
#include "access/xact.h"
#include "optimizer/prep.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "parser/parse_merge.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/queryjumble.h"

#ifdef _PG_ORCL_
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/heap.h"
#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc_fn.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "utils/fmgroids.h"
#include "utils/tqual.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/lsyscache.h"
#include "optimizer/subselect.h"
#include "tcop/tcopprot.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#include "pgxc/xc_maintenance_mode.h"
#include "access/xact.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parser.h"
#include "catalog/pg_profile.h"
#include "commands/vacuum.h"
#include "catalog/pg_type_object.h"
#endif


typedef struct collect_agg_context
{
	bool has_agg;
	List *agglist;
} collect_agg_context;


#ifdef __OPENTENBASE__
/* GUC to enable transform insert into multi-values to copy from */
bool   g_transform_insert_to_copy;
#endif

/* Hook for plugins to get control at end of parse analysis */
post_parse_analyze_hook_type post_parse_analyze_hook = NULL;

static Query *transformOptionalSelectInto(ParseState *pstate, Node *parseTree);
static Query *transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt);
static Query *transformInsertStmt(ParseState *pstate, InsertStmt *stmt);
static OnConflictExpr *transformOnConflictClause(ParseState *pstate,
						  OnConflictClause *onConflictClause, Node *target);
static int	count_rowexpr_columns(ParseState *pstate, Node *expr);
static Query *transformSelectStmt(ParseState *pstate, SelectStmt *stmt);
static Query *transformValuesClause(ParseState *pstate, SelectStmt *stmt);
static Query *transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt);
static Node *transformSetOperationTree(ParseState *pstate, SelectStmt *stmt,
						  bool isTopLevel, List **targetlist);
static void determineRecursiveColTypes(ParseState *pstate,
						   Node *larg, List *nrtargetlist);
static Query *transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt);
static List *transformReturningList(ParseState *pstate, List *returningList);
static Query *transformDeclareCursorStmt(ParseState *pstate,
						   DeclareCursorStmt *stmt);
static Query *transformExplainStmt(ParseState *pstate,
					 ExplainStmt *stmt);
static Query *transformCreateTableAsStmt(ParseState *pstate,
						   CreateTableAsStmt *stmt);
static Query *transformCallStmt(ParseState *pstate,
                                        CallStmt *stmt);
#ifdef _PG_ORCL_
static Query *transformCreateProfileStmt(ParseState *pstate, CreateProfileStmt *stmt);
static Query *transformAlterProfileStmt(ParseState *pstate, AlterProfileStmt *stmt);
static Query *transformCopyStmt(ParseState *pstate, CopyStmt *stmt);
static Node *targetEntryGetOrigExpr(ParseState *pstate, Expr *expr, int levelsup);
static List *GetSetOperationTlist(ParseState *pstate, Node *node,
									RangeTblEntry **ent, int *refid);
#endif

#ifdef PGXC
static Query *transformExecDirectStmt(ParseState *pstate, ExecDirectStmt *stmt);
#endif

static void transformLockingClause(ParseState *pstate, Query *qry,
					   LockingClause *lc, bool pushedDown);
#ifdef RAW_EXPRESSION_COVERAGE_TEST
static bool test_raw_expression_coverage(Node *node, void *context);
#endif
/* begin ora_compatible */
static List *GetSetOperationTlist(ParseState *pstate, Node *node,
								  RangeTblEntry **ent, int *refid);
static Node *targetEntryGetOrigExpr(ParseState *pstate, Expr *expr, int levelsup);
static void setop_make_child_subquery(ParseState *pstate, Node **arg, List *tlist);
/*
 * parse_analyze
 *		Analyze a raw parse tree and transform it to Query form.
 *
 * Optionally, information about $n parameter types can be supplied.
 * References to $n indexes not defined by paramTypes[] are disallowed.
 *
 * The result is a Query node.  Optimizable statements require considerable
 * transformation, while utility-type statements are simply hung off
 * a dummy CMD_UTILITY Query node.
 * 
 * isDeparsedQuery: Indicates that sourceText is a deparsed string and parseTree
 *                 comes from a deparsed string.
 */
Query *
parse_analyze(RawStmt *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams,
			  QueryEnvironment *queryEnv,
			  bool isDeparsedQuery)
{
	ParseState *pstate = make_parsestate(NULL);
	Query	   *query;
	JumbleState *jstate = NULL;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;
	pstate->isDeparsedQuery = isDeparsedQuery;

	if (numParams > 0)
		parse_fixed_parameters(pstate, paramTypes, numParams);

	pstate->p_queryEnv = queryEnv;

	query = transformTopLevelStmt(pstate, parseTree);
	
	if ((compute_query_id || (SPM_ENABLED && optimizer_capture_sql_plan_baselines)) && !IsA(parseTree->stmt, ExecDirectStmt))
		jstate = JumbleQuery(query, sourceText);

	SPMFillStatement(query, jstate, sourceText);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

	free_parsestate(pstate);

	return query;
}

/*
 * parse_analyze_varparams
 *
 * This variant is used when it's okay to deduce information about $n
 * symbol datatypes from context.  The passed-in paramTypes[] array can
 * be modified or enlarged (via repalloc).
 */
Query *
parse_analyze_varparams(RawStmt *parseTree, const char *sourceText,
						Oid **paramTypes, int *numParams, bool isDeparsedQuery)
{
	ParseState *pstate = make_parsestate(NULL);
	Query	   *query;
	JumbleState *jstate = NULL;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;
	pstate->isDeparsedQuery = isDeparsedQuery;

	parse_variable_parameters(pstate, paramTypes, numParams);

	query = transformTopLevelStmt(pstate, parseTree);

	/* make sure all is well with parameter types */
	check_variable_parameters(pstate, query);

	if (compute_query_id || (SPM_ENABLED && optimizer_capture_sql_plan_baselines))
		jstate = JumbleQuery(query, sourceText);

	SPMFillStatement(query, jstate, sourceText);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

	free_parsestate(pstate);

	return query;
}

/*
 * parse_sub_analyze
 *		Entry point for recursively analyzing a sub-statement.
 */
Query *
parse_sub_analyze(Node *parseTree, ParseState *parentParseState,
				  CommonTableExpr *parentCTE,
				  bool locked_from_parent,
				  bool resolve_unknowns)
{
	ParseState *pstate = make_parsestate(parentParseState);
	Query	   *query;

	pstate->p_parent_cte = parentCTE;
	pstate->p_locked_from_parent = locked_from_parent;
	pstate->p_resolve_unknowns = resolve_unknowns;
	if (parentParseState->p_expr_kind == EXPR_KIND_VALUES_SINGLE)
		pstate->long_type_check = LT_INSERT;

	query = transformStmt(pstate, parseTree);

	free_parsestate(pstate);

	return query;
}

/*
 * transformTopLevelStmt -
 *	  transform a Parse tree into a Query tree.
 *
 * This function is just responsible for transferring statement location data
 * from the RawStmt into the finished Query.
 */
Query *
transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree)
{
	Query	   *result;

	/* We're at top level, so allow SELECT INTO */
	result = transformOptionalSelectInto(pstate, parseTree->stmt);

	result->stmt_location = parseTree->stmt_location;
	result->stmt_len = parseTree->stmt_len;

	return result;
}

/*
 * transformOptionalSelectInto -
 *	  If SELECT has INTO, convert it to CREATE TABLE AS.
 *
 * The only thing we do here that we don't do in transformStmt() is to
 * convert SELECT ... INTO into CREATE TABLE AS.  Since utility statements
 * aren't allowed within larger statements, this is only allowed at the top
 * of the parse tree, and so we only try it before entering the recursive
 * transformStmt() processing.
 */
static Query *
transformOptionalSelectInto(ParseState *pstate, Node *parseTree)
{
	if (IsA(parseTree, SelectStmt))
	{
		SelectStmt *stmt = (SelectStmt *) parseTree;

		/* If it's a set-operation tree, drill down to leftmost SelectStmt */
		while (stmt && stmt->op != SETOP_NONE)
			stmt = stmt->larg;
		Assert(stmt && IsA(stmt, SelectStmt) &&stmt->larg == NULL);

		if (stmt && stmt->intoClause)
		{
			CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);

			ctas->query = parseTree;
			ctas->into = stmt->intoClause;
			ctas->relkind = OBJECT_TABLE;
			ctas->is_select_into = true;

			/*
			 * Remove the intoClause from the SelectStmt.  This makes it safe
			 * for transformSelectStmt to complain if it finds intoClause set
			 * (implying that the INTO appeared in a disallowed place).
			 */
			stmt->intoClause = NULL;

			parseTree = (Node *) ctas;
		}
	}

	return transformStmt(pstate, parseTree);
}

/*
 * transformStmt -
 *	  recursively transform a Parse tree into a Query tree.
 */
Query *
transformStmt(ParseState *pstate, Node *parseTree)
{
	Query	   *result;

	/*
	 * We apply RAW_EXPRESSION_COVERAGE_TEST testing to basic DML statements;
	 * we can't just run it on everything because raw_expression_tree_walker()
	 * doesn't claim to handle utility statements.
	 */
#ifdef RAW_EXPRESSION_COVERAGE_TEST
	switch (nodeTag(parseTree))
	{
		case T_SelectStmt:
		case T_InsertStmt:
		case T_UpdateStmt:
		case T_DeleteStmt:
			(void) test_raw_expression_coverage(parseTree, NULL);
			break;
		default:
			break;
	}
#endif							/* RAW_EXPRESSION_COVERAGE_TEST */

	switch (nodeTag(parseTree))
	{
			/*
			 * Optimizable statements
			 */
		case T_InsertStmt:
			result = transformInsertStmt(pstate, (InsertStmt *) parseTree);
			break;


		case T_DeleteStmt:
			result = transformDeleteStmt(pstate, (DeleteStmt *) parseTree);
			break;

		case T_UpdateStmt:
			result = transformUpdateStmt(pstate, (UpdateStmt *) parseTree);
			break;

        case T_MergeStmt:
            result = transformMergeStmt(pstate, (MergeStmt*)parseTree);
            break;

		case T_SelectStmt:
			{
				SelectStmt *n = (SelectStmt *) parseTree;

				if (n->valuesLists)
					result = transformValuesClause(pstate, n);
				else if (n->op == SETOP_NONE)
					result = transformSelectStmt(pstate, n);
				else
					result = transformSetOperationStmt(pstate, n);
			}
			break;

			/*
			 * Special cases
			 */
		case T_DeclareCursorStmt:
			result = transformDeclareCursorStmt(pstate,
												(DeclareCursorStmt *) parseTree);
			break;

		case T_ExplainStmt:
			result = transformExplainStmt(pstate,
										  (ExplainStmt *) parseTree);
			break;

#ifdef PGXC
		case T_ExecDirectStmt:
			result = transformExecDirectStmt(pstate,
											 (ExecDirectStmt *) parseTree);
			break;
#endif

		case T_CreateTableAsStmt:
			result = transformCreateTableAsStmt(pstate,
												(CreateTableAsStmt *) parseTree);
			break;

		case T_CallStmt:
			result = transformCallStmt(pstate,
									   (CallStmt *) parseTree);
			break;

#ifdef _PG_ORCL_
        case T_CreateProfileStmt:
            result = transformCreateProfileStmt(pstate, (CreateProfileStmt *)parseTree);
            break;
        case T_AlterProfileStmt:
            result = transformAlterProfileStmt(pstate, (AlterProfileStmt *)parseTree);
            break;
		case T_CopyStmt:
			result = transformCopyStmt(pstate, (CopyStmt *) parseTree);
			break;
#endif
		default:

			/*
			 * other statements don't require any transformation; just return
			 * the original parsetree with a Query node plastered on top.
			 */
			result = makeNode(Query);
			result->commandType = CMD_UTILITY;
			result->utilityStmt = (Node *) parseTree;
			break;
	}

#ifdef _MLS_
    if (g_enable_data_mask)
    {
        if (CMD_UPDATE == result->commandType || CMD_DELETE == result->commandType )
        {
    	    (void) query_tree_walker(result, datamask_check_column_in_expr, (void*)pstate, 
                                (QTW_IGNORE_RANGE_TABLE|QTW_IGNORE_RETURNING_LIST));
        }
        else if (CMD_INSERT == result->commandType)
        {
            (void) query_tree_walker(result, datamask_check_column_in_expr, (void*)pstate, 
                                (QTW_IGNORE_RANGE_TABLE|QTW_IGNORE_RETURNING_LIST|QTW_IGNORE_TARGET_LIST));
        }
        else
        {
            (void) query_tree_walker(result, datamask_check_column_in_expr, (void*)pstate, 
                                (QTW_IGNORE_RANGE_TABLE|QTW_IGNORE_TARGET_LIST));
        }
    }
#endif


	/* Mark as original query until we learn differently */
	result->querySource = QSRC_ORIGINAL;
	result->canSetTag = true;

	return result;
}

/*
 * analyze_requires_snapshot
 *		Returns true if a snapshot must be set before doing parse analysis
 *		on the given raw parse tree.
 *
 * Classification here should match transformStmt().
 */
bool
analyze_requires_snapshot(RawStmt *parseTree)
{
	bool		result;

	switch (nodeTag(parseTree->stmt))
	{
			/*
			 * Optimizable statements
			 */
		case T_InsertStmt:
		case T_DeleteStmt:
		case T_UpdateStmt:
        case T_MergeStmt:
		case T_SelectStmt:
		case T_MultiInsertStmt:	/* ora_compatible */
			result = true;
			break;

			/*
			 * Special cases
			 */
		case T_DeclareCursorStmt:
		case T_ExplainStmt:
		case T_CreateTableAsStmt:
			/* yes, because we must analyze the contained statement */
			result = true;
			break;

#ifdef PGXC
		case T_ExecDirectStmt:

			/*
			 * We will parse/analyze/plan inner query, which probably will
			 * need a snapshot. Ensure it is set.
			 */
			result = true;
			break;
#endif

		default:
			/* other utility statements don't have any real parse analysis */
			result = false;
			break;
	}

	return result;
}

/*
 * transformDeleteStmt -
 *	  transforms a Delete Statement
 */
static Query *
transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	ParseNamespaceItem *nsitem;
	Node	   *qual;
	Node	   *whereClause = NULL;

	qry->commandType = CMD_DELETE;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* set up range table with just the result rel */
	qry->resultRelation = setTargetTable(pstate, stmt->relation,
										 stmt->relation->inh,
										 true,
										 ACL_DELETE);

	/* grab the namespace item made by setTargetTable */
	nsitem = (ParseNamespaceItem *) llast(pstate->p_namespace);

	/* there's no DISTINCT in DELETE */
	qry->distinctClause = NIL;

	/* subqueries in USING cannot access the result relation */
	nsitem->p_lateral_only = true;
	nsitem->p_lateral_ok = false;

	/*
	 * The USING clause is non-standard SQL syntax, and is equivalent in
	 * functionality to the FROM list that can be specified for UPDATE. The
	 * USING keyword is used rather than FROM because FROM is already a
	 * keyword in the DELETE syntax.
	 */
	transformFromClause(pstate, stmt->usingClause);

	/* remaining clauses can reference the result relation normally */
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	/* We should copy whereClause ahead because transform maybe change it */
	if (ORA_MODE &&	!IS_CENTRALIZED_DATANODE &&
		stmt->whereClause &&
		pstate->p_target_relation &&
		RelationIsReplication(pstate->p_target_relation))
	{
		MemoryContext old_ctx, orig_ctx;

		orig_ctx = GetMemoryChunkContext(stmt->whereClause);
		old_ctx = MemoryContextSwitchTo(orig_ctx);
		whereClause = copyObject(stmt->whereClause);
		MemoryContextSwitchTo(old_ctx);
	}

	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");
	qry->returningList = transformReturningList(pstate, stmt->returningList);
	/* done building the range table and jointree */
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;
	qry->hasRowNumExpr = pstate->p_hasRownum;

	/*
	 * For `DELETE FROM t WHERE rownum < N` for replication table, if there exist
	 * primary key or rowid on it, then we convert the statement like:
	 * `DELETE FROM t WHERE (rowid/pk) in (SLEECT rowid/pk FROM t WHERE rownum < N)`
	 * By this way we can make sure each datanode delete the same tuple.
	 * If primary key and rowid both exist, then rowid is preferred.
	 */
	if (ORA_MODE &&	!IS_CENTRALIZED_DATANODE &&
		pstate->p_target_relation &&
		RelationIsReplication(pstate->p_target_relation) &&
		qry->hasRowNumExpr &&
		stmt->usingClause == NULL &&
		stmt->returningList == NIL &&
		stmt->withClause == NULL)
	{
		Relation	 pkindex;
		char		*pkcolname;
		SubLink		*sublink;
		SelectStmt	*subselect = NULL;
		int			 i;
		Node		*testexpr;
		List		*tlist = NIL;
		IndexInfo	*pkinfo;
		AttrNumber	 KeyAttrNumbers[INDEX_MAX_KEYS];
		int			 numKey = 0;

		{
			RelationGetIndexList(pstate->p_target_relation);

			if (!OidIsValid(pstate->p_target_relation->rd_pkindex))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("DELETE on replication table with rownum as condition is not supportd."),
						 errhint("You can add primary key or rowid for the table.")));

			pkindex = index_open(pstate->p_target_relation->rd_pkindex, AccessShareLock);

			pkinfo = BuildIndexInfo(pkindex);
			numKey = pkinfo->ii_NumIndexAttrs;
			memcpy((char *) KeyAttrNumbers, (char *) pkinfo->ii_KeyAttrNumbers,
					pkinfo->ii_NumIndexAttrs * sizeof(AttrNumber));
			index_close(pkindex, AccessShareLock);
		}

		if (numKey == 1)
		{
			pkcolname = get_attname(RelationGetRelid(pstate->p_target_relation),
									KeyAttrNumbers[0]);
			testexpr = makeColumnRef_ext(pkcolname);
		}
		else
		{
			RowExpr *r = makeNode(RowExpr);
			List	*args = NIL;

			for (i = 0; i < numKey; i++)
			{
				char *attname = get_attname(RelationGetRelid(pstate->p_target_relation),
											KeyAttrNumbers[i]);
				args = lappend(args, makeColumnRef_ext(attname));
			}

			r->row_format = COERCE_IMPLICIT_CAST;
			r->args = args;
			testexpr = (Node *) r;
		}

		sublink = makeNode(SubLink);
		sublink->subLinkType = ANY_SUBLINK;
		sublink->subLinkId = 0;
		sublink->testexpr = testexpr;
		sublink->operName = NULL;

		for (i = 0; i < numKey; i++)
		{
			ResTarget	*rest = makeNode(ResTarget);
			char *attname = get_attname(RelationGetRelid(pstate->p_target_relation),
										KeyAttrNumbers[i]);

			rest->val = makeColumnRef_ext(attname);
			tlist = lappend(tlist, rest);
		}

		subselect = makeNode(SelectStmt);

		subselect->targetList = tlist;
		subselect->whereClause = whereClause;
		subselect->fromClause = list_make1(stmt->relation);

		sublink->subselect = (Node *) subselect;

		qry->jointree->quals = transformExpr(pstate,
											 (Node *) sublink,
											 EXPR_KIND_WHERE);
		qry->hasSubLinks = true;
		qry->hasRowNumExpr = false;
	}

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs)
		parseCheckAggregates(pstate, qry);

	return qry;
}

char ***
InitCopyDatalist(int ndatarows, int ncolumns)
{
	int     i;
	char ***data_list = (char ***) palloc0(sizeof(char **) * ndatarows);
	char  **data_ptr = (char **) palloc0(sizeof(char *) * ndatarows * ncolumns);

	for (i = 0; i < ndatarows; i++)
	{
		data_list[i] = &(data_ptr[i * ncolumns]);
	}
	return data_list;
}

void
DeepfreeCopyDatalist(char ***data_list, int nrows, int ncolumns)
{
	int i, j;

	if (!data_list)
		return;

	for (i = 0; i < nrows && data_list[i]; i++)
	{
		for (j = 0; j < ncolumns; j++)
		{
			if (data_list[i][j])
				pfree(data_list[i][j]);
		}
	}

	pfree(data_list[0]);
	pfree(data_list);
}

static bool
SetToCopyAConstValue(char ***data_list, int rawno, int colno, A_Const *aconst)
{
	/* A_Const */
	switch(aconst->val.type)
	{
		case T_Integer:
			{
				StringInfoData data;
				initStringInfo(&data);
				appendStringInfo(&data, "%d", aconst->val.val.ival);
				data_list[rawno][colno] = data.data;
				break;
			}
		case T_Float:
			{
				char *str = pstrdup(aconst->val.val.str);
				if (ORA_MODE)
				{
					suppres_trailing_zeroes(str);
					suppres_leading_zeroes(str);
				}
				data_list[rawno][colno] = str;
				break;
			}
		case T_String:
		case T_BitString:
			{
				data_list[rawno][colno] = pstrdup(aconst->val.val.str);
				break;
			}
		case T_Null:
			{
				data_list[rawno][colno] = NULL;
				break;
			}
		default:
			elog(ERROR, "unknown value type %d", aconst->val.type);
	}
	return true;
}

static bool
SetToCopyTypeCastValue(ParseState *pstate, char ***data_list, int rawno, int colno, Node *trans_node, InsertToCopyInfoData *infodata)
{
	Expr          *res;
	MemoryContext  cxt;
	ParamListInfo  params;

	if (infodata != NULL)
	{
		params = infodata->params;
		cxt = infodata->cxt;
	}
	else
	{
		params = NULL;
		cxt = CurrentMemoryContext;
	}

	res = (Expr *) estimate_const_expressions_with_params(params, trans_node);
	if (IsA(res, Const))
	{
		Const *const_node = (Const *) res;

		if (const_node->constisnull)
		{
			data_list[rawno][colno] = NULL;
		}
		else
		{
			Oid  typoutput;
			bool typIsVarlena;
			char *extval = NULL;

			getTypeOutputInfo(const_node->consttype, &typoutput, &typIsVarlena);
			extval = OidOutputFunctionCall(typoutput, const_node->constvalue);
			data_list[rawno][colno] = MemoryContextStrdup(cxt, extval);
		}
	}
	else
	{
		return false;
	}
	return true;
}

char ***
transformInsertValuesToCopy(ParseState *pstate, SelectStmt *selectStmt, bool partialinsert, int *ncolumns, bool *support_tocopy)
{
	int              ndatarows = 0;
	int              rawno = 0;
	int              colno = 0;
	bool             copy_from = true;
	char          ***data_list = NULL;
	ListCell        *lc = NULL;
	ListCell        *cell = NULL;
	List            *sublist = NIL;
	Node            *parse_node = NULL;
	TypeCast        *cast = NULL;
	int              cur_ncolumns = 0;
	int              old_ncolumns = *ncolumns;
	int              final_ncolumns = 0;
	bool             contain_params = false;

	/* transform all values into memory */
	ndatarows = list_length(selectStmt->valuesLists);
	data_list = InitCopyDatalist(ndatarows, *ncolumns);

	foreach(lc, selectStmt->valuesLists)
	{
		colno = 0;
		sublist = (List *) lfirst(lc);
		cur_ncolumns = list_length(sublist);

		if (!copy_from)
		{
			break;
		}

		if (final_ncolumns == 0)
		{
			final_ncolumns = cur_ncolumns;
		}

		/*
		 * Partial column insertion to copy is supported, but inconsistent values column quantity is not allowed,
		 * unexpected case do not copy from
		 */
		if ((!partialinsert && *ncolumns != cur_ncolumns) ||
		    (partialinsert && (*ncolumns < list_length(sublist) || cur_ncolumns != final_ncolumns)))
		{
			copy_from = false;
			break;
		}

		foreach(cell, sublist)
		{
			parse_node = (Node *) lfirst(cell);

			if (!copy_from)
			{
				break;
			}

			/* we can just handle simple case, the value must be const and param */
			switch(nodeTag(parse_node))
			{
				case T_A_Const:
				{
					copy_from = SetToCopyAConstValue(data_list, rawno, colno, (A_Const *) parse_node);
					break;
				}
				case T_ParamRef:
				{
					/* support partialcopy */
					contain_params = true;
					break;
				}
				case T_TypeCast:
				{
					TypeCast        *copy_cast;
					Node            *trans_node;

					cast = (TypeCast *) parse_node;
					if (IsA(cast->arg, ParamRef))
					{
						/* support partialcopy */
						contain_params = true;
						break;
					}
					else if (IsA(cast->arg, A_Const))
					{
						copy_cast = copyObject(cast);
						trans_node = transformExpr(pstate, (Node*) copy_cast, EXPR_KIND_VALUES);
						copy_from = SetToCopyTypeCastValue(pstate, data_list, rawno, colno, trans_node, NULL);
					}
					else
					{
						copy_from = false;
					}
					break;
				}
				default:
				{
					copy_from = false;
					break;
				}
			}
			colno++;
		}
		rawno++;
	}

	/* Refresh the final expected number of insert columns */
	if (copy_from && partialinsert)
		*ncolumns = final_ncolumns;

	*support_tocopy = copy_from;

	/*
	 * In scenarios involving variable carrying, it is necessary to call 'transformInsertValuesToCopyForBind'
	 * during the bind phase to perform value conversion.
	 */
	if (copy_from && !contain_params)
	{
		/* will never happened */
		if (unlikely(ndatarows != rawno))
			elog(ERROR, "datarow count mismatched, expected %d, result %d", ndatarows, rawno);

		elog(DEBUG5, "transform insert values to copy successfully, insert rows[%d]", rawno);
		return data_list;
	}
	else
	{
		/* deepfree data_list */
		DeepfreeCopyDatalist(data_list, ndatarows, old_ncolumns);
		return NULL;
	}
}

/*
 * To find the value list based on the Query structure
 */
static List *
GetInsertStmtValueList(List *query_list)
{
	ListCell   *lc;
	Query      *query;
	List       *values_lists = NIL;

	query = linitial_node(Query, query_list);
	Assert(query->commandType == CMD_INSERT);

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_VALUES)
		{
			values_lists =  rte->values_lists;
			break;
		}
	}

	if (values_lists == NIL)
	{
		/* never happened */
		Assert(0);
		elog(ERROR, "Internal error, GetValueList failed.");
	}

	return values_lists;
}

static Node *
SkipTypecastGetAconstWalker(Node *node)
{
	TypeCast *tc = NULL;

	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, A_Const) ||
	    !IsA(node, TypeCast))
	{
		return node;
	}

	tc = (TypeCast *) node;
	return SkipTypecastGetAconstWalker((Node *) tc->arg);
}

static bool
SetToCopyTypeCastValueForBind(ParseState *pstate, char ***data_list, int rawno, int colno, Node *trans_node, InsertToCopyInfoData *infodata, Node *parse_node)
{
	MemoryContext    oldcxt = NULL;
	bool             copy_from = true;
	bool             docast = false;
	char           **param_string_list = infodata->param_string_list;

	if (IsA(trans_node, Const))
	{
		if (IsA(parse_node, A_Const))
		{
			oldcxt = MemoryContextSwitchTo(infodata->cxt);
			copy_from = SetToCopyAConstValue(data_list, rawno, colno, (A_Const *) parse_node);
			MemoryContextSwitchTo(oldcxt);
		}
		else
		{
			docast = true;
		}
	}
	else if (IsA(trans_node, Param))
	{
		Param *param_v = (Param *) trans_node;
		int    paramno = param_v->paramid;

		/* The function exec_bind_message has already saved params as strings */
		if (param_string_list[paramno - 1])
			data_list[rawno][colno] = MemoryContextStrdup(infodata->cxt, param_string_list[paramno - 1]);
		else
			data_list[rawno][colno] = NULL;
	}
	else if (IsA(trans_node, RelabelType))
	{
		if (IsA(parse_node, ParamRef))
		{
			ParamRef *param_v = (ParamRef *) parse_node;
			int       paramno = param_v->number;

			/* The function exec_bind_message has already saved params as strings */
			if (param_string_list[paramno - 1])
				data_list[rawno][colno] = MemoryContextStrdup(infodata->cxt, param_string_list[paramno - 1]);
			else
				data_list[rawno][colno] = NULL;
		}
		else if (IsA(parse_node, A_Const))
		{
			oldcxt = MemoryContextSwitchTo(infodata->cxt);
			copy_from = SetToCopyAConstValue(data_list, rawno, colno, (A_Const *) parse_node);
			MemoryContextSwitchTo(oldcxt);
		}
		else
		{
			docast = true;
		}
	}
	else if (IsA(trans_node, FuncExpr) ||
	         IsA(trans_node, CoerceViaIO))
	{
		docast = true;
	}
	else
	{
		copy_from = false;
		docast = false;
	}

	if (docast)
	{
		copy_from = SetToCopyTypeCastValue(pstate, data_list, rawno, colno, trans_node, infodata);
	}
	return copy_from;
}

/*
 * For performance considerations, executing the transform operation for each "VALUES"
 * entry incurs significant overhead. To address this, the bind process has been modified
 * to populate the `data_list` based on the Query structure. This change reduces the
 * overhead associated with performing the transform operation for each individual "VALUES" entry.
 */
char ***
transformInsertValuesToCopyForBind(ParseState *pstate, List *query_list, SelectStmt *select_stmt, int ncolumns, InsertToCopyInfoData *infodata)
{
	int              ndatarows = 0;
	int              rawno = 0;
	bool             copy_from = true;
	char          ***data_list = NULL;
	ListCell        *lc1,
	                *lc2,
	                *cell1,
	                *cell2;
	char           **param_string_list = infodata->param_string_list;
	List            *values_lists = GetInsertStmtValueList(query_list);
	int              colno = 0;
	List            *sublist1,
	                *sublist2;
	Node            *parse_node = NULL;
	Node            *trans_node = NULL;
	MemoryContext    oldcxt = NULL;

	/* transform all values into memory */
	ndatarows = list_length(values_lists);

	oldcxt = MemoryContextSwitchTo(infodata->cxt);
	data_list = InitCopyDatalist(ndatarows, ncolumns);
	MemoryContextSwitchTo(oldcxt);

	forboth(lc1, select_stmt->valuesLists, lc2, values_lists)
	{
		colno = 0;
		sublist1 = (List *) lfirst(lc1);
		sublist2 = (List *) lfirst(lc2);

		if (!copy_from)
		{
			break;
		}

		forboth(cell1, sublist1, cell2, sublist2)
		{
			if (!copy_from)
			{
				break;
			}

			parse_node = (Node *) lfirst(cell1);
			switch(nodeTag(parse_node))
			{
				case T_A_Const:
				{
					oldcxt = MemoryContextSwitchTo(infodata->cxt);
					copy_from = SetToCopyAConstValue(data_list, rawno, colno, (A_Const *) parse_node);
					MemoryContextSwitchTo(oldcxt);
					break;
				}
				case T_ParamRef:
				{
					ParamRef *param_v = (ParamRef *) parse_node;
					int       paramno = param_v->number;

					/* The function exec_bind_message has already saved params as strings */
					if (param_string_list[paramno - 1])
						data_list[rawno][colno] = MemoryContextStrdup(infodata->cxt, param_string_list[paramno - 1]);
					else
						data_list[rawno][colno] = NULL;
					break;
				}
				case T_TypeCast:
				{
					trans_node = (Node *) lfirst(cell2);
					if (IsA(trans_node, Const) ||
					    IsA(trans_node, RelabelType))
					{
						parse_node = SkipTypecastGetAconstWalker(parse_node);
					}

					copy_from = SetToCopyTypeCastValueForBind(pstate, data_list, rawno, colno, trans_node, infodata, parse_node);
					break;
				}
				default:
				{
					copy_from = false;
					break;
				}
			}
			colno++;
		}
		rawno++;
	}

	if (copy_from)
	{
		elog(DEBUG5, "transform insert values to copy successfully, insert rows[%d]", rawno);
		return data_list;
	}
	else
	{
		/* deepfree data_list */
		DeepfreeCopyDatalist(data_list, ndatarows, ncolumns);
		return NULL;
	}
}
/*
 * check if insert cols has col.field
 * when col type is a typerel
 * such as insert into xxx(col1.field1) values xxx
 */
static bool InsertColsInd(List *cols)
{
	bool has_ind = false;
	ListCell *lc = NULL;

	foreach(lc, cols)
	{
		ResTarget *tgt = (ResTarget *) lfirst(lc);

		if (tgt->indirection)
		{
			has_ind = true;
			break;
		}
	}
	return has_ind;
}

/*
 * transformInsertStmt -
 *	  transform an Insert Statement
 */
static Query *
transformInsertStmt(ParseState *pstate, InsertStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	SelectStmt *selectStmt = (SelectStmt *) stmt->selectStmt;
	List	   *exprList = NIL;
	bool		isGeneralSelect;
	List	   *sub_rtable;
	List	   *sub_namespace;
	List	   *icolumns;
	List	   *attrnos;
	RangeTblEntry *rte;
	RangeTblRef *rtr;
	ListCell   *icols;
	ListCell   *attnos;
	ListCell   *lc;
	bool		isOnConflictUpdate;
	AclMode		targetPerms;
#ifdef __OPENTENBASE__
	int ncolumns = 0;
#endif

	/* There can't be any outer WITH to worry about */
	Assert(pstate->p_ctenamespace == NIL);

	qry->commandType = CMD_INSERT;
	pstate->p_is_insert = true;
#ifdef __OPENTENBASE__
	qry->isMultiValues = false;
	stmt->ninsert_columns = 0;
#endif

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	qry->override = stmt->override;

	isOnConflictUpdate = (stmt->onConflictClause &&
						  stmt->onConflictClause->action == ONCONFLICT_UPDATE);

	/*
	 * We have three cases to deal with: DEFAULT VALUES (selectStmt == NULL),
	 * VALUES list, or general SELECT input.  We special-case VALUES, both for
	 * efficiency and so we can handle DEFAULT specifications.
	 *
	 * The grammar allows attaching ORDER BY, LIMIT, FOR UPDATE, or WITH to a
	 * VALUES clause.  If we have any of those, treat it as a general SELECT;
	 * so it will work, but you can't use DEFAULT items together with those.
	 */
	isGeneralSelect = (selectStmt && (selectStmt->valuesLists == NIL ||
									  selectStmt->sortClause != NIL ||
									  selectStmt->limitOffset != NULL ||
									  selectStmt->limitCount != NULL ||
									  selectStmt->lockingClause != NIL ||
									  selectStmt->withClause != NULL));

	/*
	 * If a non-nil rangetable/namespace was passed in, and we are doing
	 * INSERT/SELECT, arrange to pass the rangetable/namespace down to the
	 * SELECT.  This can only happen if we are inside a CREATE RULE, and in
	 * that case we want the rule's OLD and NEW rtable entries to appear as
	 * part of the SELECT's rtable, not as outer references for it.  (Kluge!)
	 * The SELECT's joinlist is not affected however.  We must do this before
	 * adding the target table to the INSERT's rtable.
	 */
	if (isGeneralSelect)
	{
		sub_rtable = pstate->p_rtable;
		pstate->p_rtable = NIL;
		sub_namespace = pstate->p_namespace;
		pstate->p_namespace = NIL;
	}
	else
	{
		sub_rtable = NIL;		/* not used, but keep compiler quiet */
		sub_namespace = NIL;
	}

	/*
	 * Must get write lock on INSERT target table before scanning SELECT, else
	 * we will grab the wrong kind of initial lock if the target table is also
	 * mentioned in the SELECT part.  Note that the target table is not added
	 * to the joinlist or namespace.
	 */
	targetPerms = ACL_INSERT;
	if (isOnConflictUpdate)
		targetPerms |= ACL_UPDATE;
	qry->resultRelation = setTargetTable(pstate, stmt->relation,
										 false, false, targetPerms);

	/* Validate stmt->cols list, or build default list if no list given */
	icolumns = checkInsertTargets(pstate, stmt->cols, &attrnos);
	Assert(list_length(icolumns) == list_length(attrnos));

	/*
	 * Determine which variant of INSERT we have.
	 */
	if (selectStmt == NULL)
	{
		/*
		 * We have INSERT ... DEFAULT VALUES.  We can handle this case by
		 * emitting an empty targetlist --- all columns will be defaulted when
		 * the planner expands the targetlist.
		 */
		exprList = NIL;
	}
	else if (isGeneralSelect)
	{
		/*
		 * We make the sub-pstate a child of the outer pstate so that it can
		 * see any Param definitions supplied from above.  Since the outer
		 * pstate's rtable and namespace are presently empty, there are no
		 * side-effects of exposing names the sub-SELECT shouldn't be able to
		 * see.
		 */
		ParseState *sub_pstate = make_parsestate(pstate);
		Query	   *selectQuery;

		/*
		 * Process the source SELECT.
		 *
		 * It is important that this be handled just like a standalone SELECT;
		 * otherwise the behavior of SELECT within INSERT might be different
		 * from a stand-alone SELECT. (Indeed, Postgres up through 6.5 had
		 * bugs of just that nature...)
		 *
		 * The sole exception is that we prevent resolving unknown-type
		 * outputs as TEXT.  This does not change the semantics since if the
		 * column type matters semantically, it would have been resolved to
		 * something else anyway.  Doing this lets us resolve such outputs as
		 * the target column's type, which we handle below.
		 */
		sub_pstate->p_rtable = sub_rtable;
		sub_pstate->p_joinexprs = NIL;	/* sub_rtable has no joins */
		sub_pstate->p_namespace = sub_namespace;
		sub_pstate->p_resolve_unknowns = false;

		selectQuery = transformStmt(sub_pstate, stmt->selectStmt);

		if (ORA_MODE && IS_PGXC_COORDINATOR)
		{
			if (selectQuery && contain_plpgsql_functions((Node *) selectQuery))
			{
				elog(ERROR, "plpgsql's hook functions are not allowed as constraints.");
			}
		}

		free_parsestate(sub_pstate);

		/* The grammar should have produced a SELECT */
		if (!IsA(selectQuery, Query) ||
			selectQuery->commandType != CMD_SELECT)
			elog(ERROR, "unexpected non-SELECT command in INSERT ... SELECT");

		/*
		 * Make the source be a subquery in the INSERT's rangetable, and add
		 * it to the INSERT's joinlist.
		 */
		rte = addRangeTableEntryForSubquery(pstate,
											selectQuery,
											makeAlias("*SELECT*", NIL),
											false,
											false);
		rtr = makeNode(RangeTblRef);
		/* assume new rte is at end */
		rtr->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
		pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);

		/*----------
		 * Generate an expression list for the INSERT that selects all the
		 * non-resjunk columns from the subquery.  (INSERT's tlist must be
		 * separate from the subquery's tlist because we may add columns,
		 * insert datatype coercions, etc.)
		 *
		 * HACK: unknown-type constants and params in the SELECT's targetlist
		 * are copied up as-is rather than being referenced as subquery
		 * outputs.  This is to ensure that when we try to coerce them to
		 * the target column's datatype, the right things happen (see
		 * special cases in coerce_type).  Otherwise, this fails:
		 *		INSERT INTO foo SELECT 'bar', ... FROM baz
		 *----------
		 */
		exprList = NIL;
		foreach(lc, selectQuery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Expr	   *expr;

			if (tle->resjunk)
				continue;
			if (tle->expr &&
				(IsA(tle->expr, Const) ||IsA(tle->expr, Param)) &&
				exprType((Node *) tle->expr) == UNKNOWNOID)
				expr = tle->expr;
			else
			{
				Var		   *var = makeVarFromTargetEntry(rtr->rtindex, tle);

				if (ORA_MODE &&
					(var->vartype == LRAWOID ||
					 var->vartype == LONGOID ||
					 var->vartype == LONGARROID))
				{
					elog(ERROR, "\"long\" type can't in select list of create as/insert grammer");
				}

				var->location = exprLocation((Node *) tle->expr);
				expr = (Expr *) var;
			}
			exprList = lappend(exprList, expr);
		}

		/* Prepare row for assignment to target table */
		exprList = transformInsertRow(pstate, exprList,
									  stmt->cols,
									  icolumns, attrnos,
									  false);
	}
	else if (list_length(selectStmt->valuesLists) > 1)
	{
		/*
		 * Process INSERT ... VALUES with multiple VALUES sublists. We
		 * generate a VALUES RTE holding the transformed expression lists, and
		 * build up a targetlist containing Vars that reference the VALUES
		 * RTE.
		 */
		List	   *exprsLists = NIL;
		List	   *coltypes = NIL;
		List	   *coltypmods = NIL;
		List	   *colcollations = NIL;
		int			sublist_length = -1;
		bool		lateral = false;

		Assert(selectStmt->intoClause == NULL);

#ifdef _PG_ORCL_
		/* opentenbase_ora not support to insert multi-values at once, so dblink reports errror */
		if (stmt->relation->dblinkname != NULL)
		{
			elog(ERROR, "The sql of 'insert into' foreign table with dblink could not include multiple records.");
		}
#endif

#ifdef __OPENTENBASE__
		/*
		 * transform 'insert into values' into 'COPY FROM', only handle
		 * distributed relation(by hash/shard/replication) without
		 * on conflict/returning/with clause/triggers.
		 * PrepareStmt do not support transform insert to copy.
		 */
		if (g_transform_insert_to_copy && IS_PGXC_COORDINATOR &&
			!stmt->onConflictClause && !stmt->returningList &&
			!stmt->withClause && !explain_stmt && !stmt->is_preparestmt &&
			!InsertColsInd(stmt->cols))
		{
			if (pstate->p_target_relation)
			{
				RelationLocInfo *rel_loc_info = pstate->p_target_relation->rd_locator_info;

				if (rel_loc_info &&
					(rel_loc_info->locatorType == LOCATOR_TYPE_HASH ||
					rel_loc_info->locatorType == LOCATOR_TYPE_SHARD ||
					rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED))
				{
					int       i;
					TupleDesc tupDesc = RelationGetDescr(pstate->p_target_relation);

					qry->isMultiValues = true;
					for (i = 0; i < tupDesc->natts; ++i)
					{
						if (TupleDescAttr(tupDesc, i)->attidentity)
							qry->isMultiValues = false;
					}

					/* has triggers? */
					if (!pgxc_check_triggers_shippability(RelationGetRelid(pstate->p_target_relation),
													  qry->commandType))
					{
						qry->hasUnshippableDml = true;
						qry->has_trigger = true;
					}
				}
			}
		}

		/*
		 * put values into memory, then copy to remote datanode
		 */
		if (qry->isMultiValues && !qry->hasUnshippableDml)
		{
			int                     i;
			char                 ***data_list = NULL;
			bool                    partialinsert = false;
			bool                    support_tocopy = true;

			ncolumns = 0;
			
			if (stmt->cols)
			{
				ncolumns = list_length(stmt->cols);
			}
			else
			{
				/* Generate default column list */
				TupleDesc tupDesc = RelationGetDescr(pstate->p_target_relation);
				int       attr_count = tupDesc->natts;

				for (i = 0; i < attr_count; i++)
				{
					if (TupleDescAttr(tupDesc, i)->attisdropped)
						continue;
					ncolumns++;
				}
			}

			stmt->ninsert_columns = ncolumns;
			partialinsert = stmt->cols == NIL ? true : false;

			data_list = transformInsertValuesToCopy(pstate, selectStmt, partialinsert, &ncolumns, &support_tocopy);
			if (data_list)
			{
				qry->copy_filename = palloc(MAXPGPATH);
				snprintf(qry->copy_filename, MAXPGPATH, "%s", "Insert_into to Copy_from(Simple Protocl)");
				stmt->ndatarows = list_length(selectStmt->valuesLists);
				stmt->data_list = data_list;
			}

			/* To support partial column insertion, we need to construct cols ourselves */
			if (partialinsert && (stmt->ninsert_columns > ncolumns))
			{
				generateDefaultColums(pstate, &stmt->cols, NULL, ncolumns);
				stmt->ninsert_columns = ncolumns;

				/* Validate stmt->cols list, Previously requested memory is released with the context */
				icolumns = checkInsertTargets(pstate, stmt->cols, &attrnos);
				Assert(list_length(icolumns) == list_length(attrnos));
			}
			
			/*
			 * In the case of the extension protocol, if copy conversion is supported, allow the bind process to continue.
			 * In the case of the regular protocol, if the data_list is empty, it definitely does not support copy conversion.
			 */
			if (IsExtendedQuery() || !data_list)
			{
				/*
				 * Passing variables via transformInsertValuesToCopyForBind
				 * is not supported in PL/SQL.
				 */
				if (!support_tocopy || InPlpgsqlFunc())
				{
					qry->isMultiValues = false;
				}

				goto TRANSFORM_VALUELISTS;
			}
		}
		else
		{
#endif
TRANSFORM_VALUELISTS:
		foreach(lc, selectStmt->valuesLists)
		{
			List	   *sublist = (List *) lfirst(lc);

			/*
			 * Do basic expression transformation (same as a ROW() expr, but
			 * allow SetToDefault at top level)
			 */
			sublist = transformExpressionList(pstate, sublist,
											  EXPR_KIND_VALUES, true);

			/*
			 * All the sublists must be the same length, *after*
			 * transformation (which might expand '*' into multiple items).
			 * The VALUES RTE can't handle anything different.
			 */
			if (sublist_length < 0)
			{
				/* Remember post-transformation length of first sublist */
				sublist_length = list_length(sublist);
			}
			else if (sublist_length != list_length(sublist))
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("VALUES lists must all be the same length"),
						 parser_errposition(pstate,
											exprLocation((Node *) sublist))));
			}

#ifdef __OPENTENBASE__
			if (IsExtendedQuery() && qry->isMultiValues && !qry->hasUnshippableDml)
			{
				/*
				 * simple insert if all values are params
				 *
				 * if not simple insert, do not transform insert into to copy from
				 */
				ListCell *cell;
				foreach(cell, sublist)
				{
					Node *node = (Node *)lfirst(cell);
					if (!IsA(node, Param) &&
					    !pgxc_is_expr_shippable((Expr *) node, NULL))
					{
						qry->isMultiValues = false;
						break;
					}
				}
			}
#endif

			/*
			 * Prepare row for assignment to target table.  We process any
			 * indirection on the target column specs normally but then strip
			 * off the resulting field/array assignment nodes, since we don't
			 * want the parsed statement to contain copies of those in each
			 * VALUES row.  (It's annoying to have to transform the
			 * indirection specs over and over like this, but avoiding it
			 * would take some really messy refactoring of
			 * transformAssignmentIndirection.)
			 */
			sublist = transformInsertRow(pstate, sublist,
										 stmt->cols,
										 icolumns, attrnos,
										 true);

			/*
			 * We must assign collations now because assign_query_collations
			 * doesn't process rangetable entries.  We just assign all the
			 * collations independently in each row, and don't worry about
			 * whether they are consistent vertically.  The outer INSERT query
			 * isn't going to care about the collations of the VALUES columns,
			 * so it's not worth the effort to identify a common collation for
			 * each one here.  (But note this does have one user-visible
			 * consequence: INSERT ... VALUES won't complain about conflicting
			 * explicit COLLATEs in a column, whereas the same VALUES
			 * construct in another context would complain.)
			 */
			assign_list_collations(pstate, sublist);

			exprsLists = lappend(exprsLists, sublist);
		}

#ifdef __OPENTENBASE__
		/* number of insert columns must be same as valueslist */
		if (IsExtendedQuery() && qry->isMultiValues && !qry->hasUnshippableDml)
		{
			if (ncolumns != sublist_length)
			{
				qry->isMultiValues = false;
			}
		}
#endif

		/*
		 * Construct column type/typmod/collation lists for the VALUES RTE.
		 * Every expression in each column has been coerced to the type/typmod
		 * of the corresponding target column or subfield, so it's sufficient
		 * to look at the exprType/exprTypmod of the first row.  We don't care
		 * about the collation labeling, so just fill in InvalidOid for that.
		 */
		foreach(lc, (List *) linitial(exprsLists))
		{
			Node	   *val = (Node *) lfirst(lc);

			coltypes = lappend_oid(coltypes, exprType(val));
			coltypmods = lappend_int(coltypmods, exprTypmod(val));
			colcollations = lappend_oid(colcollations, InvalidOid);
		}

		/*
		 * Ordinarily there can't be any current-level Vars in the expression
		 * lists, because the namespace was empty ... but if we're inside
		 * CREATE RULE, then NEW/OLD references might appear.  In that case we
		 * have to mark the VALUES RTE as LATERAL.
		 */
		if (list_length(pstate->p_rtable) != 1 &&
			contain_vars_of_level((Node *) exprsLists, 0))
			lateral = true;

		/*
		 * Generate the VALUES RTE
		 */
		rte = addRangeTableEntryForValues(pstate, exprsLists,
										  coltypes, coltypmods, colcollations,
										  NULL, lateral, true);
		rtr = makeNode(RangeTblRef);
		/* assume new rte is at end */
		rtr->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
		pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);

		/*
		 * Generate list of Vars referencing the RTE
		 */
		expandRTE(rte, rtr->rtindex, 0, -1, false, NULL, &exprList);

		/*
		 * Re-apply any indirection on the target column specs to the Vars
		 */
		exprList = transformInsertRow(pstate, exprList,
									  stmt->cols,
									  icolumns, attrnos,
									  false);
#ifdef __OPENTENBASE__
		}
#endif
	}
	else
	{
		/*
		 * Process INSERT ... VALUES with a single VALUES sublist.  We treat
		 * this case separately for efficiency.  The sublist is just computed
		 * directly as the Query's targetlist, with no VALUES RTE.  So it
		 * works just like a SELECT without any FROM.
		 */
		List	   *valuesLists = selectStmt->valuesLists;

		Assert(list_length(valuesLists) == 1);
		Assert(selectStmt->intoClause == NULL);

		/*
		 * Do basic expression transformation (same as a ROW() expr, but allow
		 * SetToDefault at top level)
		 */
		exprList = transformExpressionList(pstate,
										   (List *) linitial(valuesLists),
										   EXPR_KIND_VALUES_SINGLE,
										   true);

		if (ORA_MODE && IS_PGXC_COORDINATOR && pstate->p_target_relation)
		{
			RelationLocInfo *target_rel_loc_info = pstate->p_target_relation->rd_locator_info;

			if (target_rel_loc_info && target_rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED &&
				exprList && contain_plpgsql_functions((Node *) exprList))
			{
				elog(ERROR, "plpgsql's hook functions are not allowed as constraints.");
			}
		}

		/* Prepare row for assignment to target table */
		exprList = transformInsertRow(pstate, exprList,
									  stmt->cols,
									  icolumns, attrnos,
									  false);
	}

	/*
	 * Generate query's target list using the computed list of expressions.
	 * Also, mark all the target columns as needing insert permissions.
	 */
	rte = pstate->p_target_rangetblentry;
	qry->targetList = NIL;
	icols = list_head(icolumns);
	attnos = list_head(attrnos);
	foreach(lc, exprList)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		ResTarget  *col;
		AttrNumber	attr_num;
		TargetEntry *tle;

		col = lfirst_node(ResTarget, icols);
		attr_num = (AttrNumber) lfirst_int(attnos);

		tle = makeTargetEntry(expr,
							  attr_num,
							  col->name,
							  false);
		qry->targetList = lappend(qry->targetList, tle);

		rte->insertedCols = bms_add_member(rte->insertedCols,
										   attr_num - FirstLowInvalidHeapAttributeNumber);

		icols = lnext(icols);
		attnos = lnext(attnos);
	}

	/* Process ON CONFLICT, if any. */
	if (stmt->onConflictClause)
		qry->onConflict = transformOnConflictClause(pstate,
													stmt->onConflictClause, (Node*) stmt->relation);

	/*
	 * If we have a RETURNING clause, we need to add the target relation to
	 * the query namespace before processing it, so that Var references in
	 * RETURNING will work.  Also, remove any namespace entries added in a
	 * sub-SELECT or VALUES list.
	 */
	if (stmt->returningList)
	{
		pstate->p_namespace = NIL;
		addRTEtoQuery(pstate, pstate->p_target_rangetblentry,
					  false, true, true);
		qry->returningList = transformReturningList(pstate,
													stmt->returningList);
	}

	/* done building the range table and jointree */
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasRowNumExpr = pstate->p_hasRownum;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * Prepare an INSERT row for assignment to the target table.
 *
 * exprlist: transformed expressions for source values; these might come from
 * a VALUES row, or be Vars referencing a sub-SELECT or VALUES RTE output.
 * stmtcols: original target-columns spec for INSERT (we just test for NIL)
 * icolumns: effective target-columns spec (list of ResTarget)
 * attrnos: integer column numbers (must be same length as icolumns)
 * strip_indirection: if true, remove any field/array assignment nodes
 */
List *
transformInsertRow(ParseState *pstate, List *exprlist,
				   List *stmtcols, List *icolumns, List *attrnos,
				   bool strip_indirection)
{
	List	   *result;
	ListCell   *lc;
	ListCell   *icols;
	ListCell   *attnos;

	/*
	 * Check length of expr list.  It must not have more expressions than
	 * there are target columns.  We allow fewer, but only if no explicit
	 * columns list was given (the remaining columns are implicitly
	 * defaulted).  Note we must check this *after* transformation because
	 * that could expand '*' into multiple items.
	 */
	if (list_length(exprlist) > list_length(icolumns))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("INSERT has more expressions than target columns"),
				 parser_errposition(pstate,
									exprLocation(list_nth(exprlist,
														  list_length(icolumns))))));
	if (stmtcols != NIL &&
		list_length(exprlist) < list_length(icolumns))
	{
		/*
		 * We can get here for cases like INSERT ... SELECT (a,b,c) FROM ...
		 * where the user accidentally created a RowExpr instead of separate
		 * columns.  Add a suitable hint if that seems to be the problem,
		 * because the main error message is quite misleading for this case.
		 * (If there's no stmtcols, you'll get something about data type
		 * mismatch, which is less misleading so we don't worry about giving a
		 * hint in that case.)
		 */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("INSERT has more target columns than expressions"),
				 ((list_length(exprlist) == 1 &&
				   count_rowexpr_columns(pstate, linitial(exprlist)) ==
				   list_length(icolumns)) ?
				  errhint("The insertion source is a row expression containing the same number of columns expected by the INSERT. Did you accidentally use extra parentheses?") : 0),
				 parser_errposition(pstate,
									exprLocation(list_nth(icolumns,
														  list_length(exprlist))))));
	}

	/* if insertion source is an Object in opentenbase_ora mode */
	if (ORA_MODE && list_length(exprlist) == 1)
	{
		Expr *expr = (Expr *) linitial(exprlist);
		Oid toid = exprType((Node *) expr);

		if (OidIsValid(toid) && (type_is_rowtype(toid)))
		{
			result = transformAssignedByObject(pstate, expr, EXPR_KIND_INSERT_TARGET, icolumns, attrnos);
			if (list_length(result) > 0)
				return result;
		}
	}

	/*
	 * Prepare columns for assignment to target table.
	 */
	result = NIL;
	icols = list_head(icolumns);
	attnos = list_head(attrnos);

	foreach(lc, exprlist)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		ResTarget  *col;

		col = lfirst_node(ResTarget, icols);

		expr = transformAssignedExpr(pstate, expr,
									 EXPR_KIND_INSERT_TARGET,
									 col->name,
									 lfirst_int(attnos),
									 col->indirection,
									 col->location);

		if (strip_indirection)
		{
			while (expr)
			{
				if (IsA(expr, FieldStore))
				{
					FieldStore *fstore = (FieldStore *) expr;

					expr = (Expr *) linitial(fstore->newvals);
				}
				else if (IsA(expr, ArrayRef))
				{
					ArrayRef   *aref = (ArrayRef *) expr;

					if (aref->refassgnexpr == NULL)
						break;
					expr = aref->refassgnexpr;
				}
				else
					break;
			}
		}

		result = lappend(result, expr);

		icols = lnext(icols);
		attnos = lnext(attnos);
	}

	return result;
}

/*
 * transformOnConflictClause -
 *	  transforms an OnConflictClause in an INSERT
 */
static OnConflictExpr *
transformOnConflictClause(ParseState *pstate, OnConflictClause *onConflictClause, Node *target)
{
	List	   *arbiterElems;
	Node	   *arbiterWhere;
	Oid			arbiterConstraint;
	List	   *onConflictSet = NIL;
	Node	   *onConflictWhere = NULL;
	RangeTblEntry *exclRte = NULL;
	int			exclRelIndex = 0;
	List	   *exclRelTlist = NIL;
	OnConflictExpr *result;

	/* Process the arbiter clause, ON CONFLICT ON (...) */
	transformOnConflictArbiter(pstate, onConflictClause, &arbiterElems,
							   &arbiterWhere, &arbiterConstraint);

	/* Process DO UPDATE */
	if (onConflictClause->action == ONCONFLICT_UPDATE)
	{
		Relation	targetrel = pstate->p_target_relation;

		/*
		 * All INSERT expressions have been parsed, get ready for potentially
		 * existing SET statements that need to be processed like an UPDATE.
		 */
		pstate->p_is_insert = false;

		/*
		 * Add range table entry for the EXCLUDED pseudo relation.  relkind is
		 * set to composite to signal that we're not dealing with an actual
		 * relation, and no permission checks are required on it.  (We'll
		 * check the actual target relation, instead.)
		 */
		exclRte = addRangeTableEntryForRelation(pstate,
												targetrel,
												makeAlias((ORA_MODE) ? "EXCLUDED" : "excluded", NIL),
												false, false);
		exclRte->relkind = RELKIND_COMPOSITE_TYPE;
		exclRte->requiredPerms = 0;
		/* other permissions fields in exclRte are already empty */

		exclRelIndex = list_length(pstate->p_rtable);

		/* Create EXCLUDED rel's targetlist for use by EXPLAIN */
		exclRelTlist = BuildOnConflictExcludedTargetlist(targetrel,
														 exclRelIndex);

		/*
		 * Add EXCLUDED and the target RTE to the namespace, so that they can
		 * be used in the UPDATE subexpressions.
		 */
		addRTEtoQuery(pstate, exclRte, false, true, true);
		addRTEtoQuery(pstate, pstate->p_target_rangetblentry,
					  false, true, true);

		/*
		 * Now transform the UPDATE subexpressions.
		 */
		onConflictSet =
			transformUpdateTargetList(pstate, onConflictClause->targetList, target);

		onConflictWhere = transformWhereClause(pstate,
											   onConflictClause->whereClause,
											   EXPR_KIND_WHERE, "WHERE");
	}

	/* Finally, build ON CONFLICT DO [NOTHING | UPDATE] expression */
	result = makeNode(OnConflictExpr);

	result->action = onConflictClause->action;
	result->arbiterElems = arbiterElems;
	result->arbiterWhere = arbiterWhere;
	result->constraint = arbiterConstraint;
	result->onConflictSet = onConflictSet;
	result->onConflictWhere = onConflictWhere;
	result->exclRelIndex = exclRelIndex;
	result->exclRelTlist = exclRelTlist;

	return result;
}


/*
 * BuildOnConflictExcludedTargetlist
 *		Create target list for the EXCLUDED pseudo-relation of ON CONFLICT,
 *		representing the columns of targetrel with varno exclRelIndex.
 *
 * Note: Exported for use in the rewriter.
 */
List *
BuildOnConflictExcludedTargetlist(Relation targetrel,
								  Index exclRelIndex)
{
	List	   *result = NIL;
	int			attno;
	Var		   *var;
	TargetEntry *te;

	/*
	 * Note that resnos of the tlist must correspond to attnos of the
	 * underlying relation, hence we need entries for dropped columns too.
	 */
	for (attno = 0; attno < RelationGetNumberOfAttributes(targetrel); attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(targetrel->rd_att, attno);
		char	   *name;

		if (attr->attisdropped)
		{
			/*
			 * can't use atttypid here, but it doesn't really matter what type
			 * the Const claims to be.
			 */
			var = (Var *) makeNullConst(INT4OID, -1, InvalidOid);
			name = "";
		}
		else
		{
			var = makeVar(exclRelIndex, attno + 1,
						  attr->atttypid, attr->atttypmod,
						  attr->attcollation,
						  0);
			name = pstrdup(NameStr(attr->attname));
		}

		te = makeTargetEntry((Expr *) var,
							 attno + 1,
							 name,
							 false);

		result = lappend(result, te);
	}

	/*
	 * Add a whole-row-Var entry to support references to "EXCLUDED.*".  Like
	 * the other entries in the EXCLUDED tlist, its resno must match the Var's
	 * varattno, else the wrong things happen while resolving references in
	 * setrefs.c.  This is against normal conventions for targetlists, but
	 * it's okay since we don't use this as a real tlist.
	 */
	var = makeVar(exclRelIndex, InvalidAttrNumber,
				  targetrel->rd_rel->reltype,
				  -1, InvalidOid, 0);
	te = makeTargetEntry((Expr *) var, InvalidAttrNumber, NULL, true);
	result = lappend(result, te);

	return result;
}


/*
 * count_rowexpr_columns -
 *	  get number of columns contained in a ROW() expression;
 *	  return -1 if expression isn't a RowExpr or a Var referencing one.
 *
 * This is currently used only for hint purposes, so we aren't terribly
 * tense about recognizing all possible cases.  The Var case is interesting
 * because that's what we'll get in the INSERT ... SELECT (...) case.
 */
static int
count_rowexpr_columns(ParseState *pstate, Node *expr)
{
	if (expr == NULL)
		return -1;
	if (IsA(expr, RowExpr))
		return list_length(((RowExpr *) expr)->args);
	if (IsA(expr, Var))
	{
		Var		   *var = (Var *) expr;
		AttrNumber	attnum = var->varattno;

		if (attnum > 0 && var->vartype == RECORDOID)
		{
			RangeTblEntry *rte;

			rte = GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
			if (rte->rtekind == RTE_SUBQUERY)
			{
				/* Subselect-in-FROM: examine sub-select's output expr */
				TargetEntry *ste = get_tle_by_resno(rte->subquery->targetList,
													attnum);

				if (ste == NULL || ste->resjunk)
					return -1;
				expr = (Node *) ste->expr;
				if (IsA(expr, RowExpr))
					return list_length(((RowExpr *) expr)->args);
			}
		}
	}
	return -1;
}


/*
 * transformSelectStmt -
 *	  transforms a Select Statement
 *
 * Note: this covers only cases with no set operations and no VALUES lists;
 * see below for the other cases.
 */
static Query *
transformSelectStmt(ParseState *pstate, SelectStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	Node	   *qual;
	ListCell   *l;
	qry->commandType = CMD_SELECT;
	pstate->cteList = NIL;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
		pstate->cteList = qry->cteList;
	}


	/* Complain if we get called from someplace where INTO is not allowed */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT ... INTO is not allowed here"),
				 parser_errposition(pstate,
									exprLocation((Node *) stmt->intoClause))));

	/* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
	pstate->p_locking_clause = stmt->lockingClause;

	/* make WINDOW info available for window functions, too */
	pstate->p_windowdefs = stmt->windowClause;

	/* process the FROM clause */
	transformFromClause(pstate, stmt->fromClause);


#ifdef _PG_ORCL_
	/* transform WHERE */
	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");
#endif	

	/* transform targetlist */
	qry->targetList = transformTargetList(pstate, stmt->targetList,
										  EXPR_KIND_SELECT_TARGET);

	/* Only resolve nested aggregate functions in targetlist. */
	pstate->p_resolve_nestagg = false;

	/* mark column origins */
	markTargetListOrigins(pstate, qry->targetList);

#ifndef _PG_ORCL_
	/* transform WHERE */
	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");
#endif

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = transformWhereClause(pstate, stmt->havingClause,
										   EXPR_KIND_HAVING, "HAVING");

	pstate->p_hasDistinct = (stmt->distinctClause != NULL);

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
	qry->sortClause = transformSortClause(pstate,
										  stmt->sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );
	if (stmt->connectClause && stmt->connectClause->sort)
	{
		Assert(qry->connectByExpr != NULL);

		if (stmt->sortClause != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("ORDER BY and ORDER SIBLINGS BY clauses cannot coexist"),
						parser_errposition(pstate,
										   exprLocation((Node *) stmt->sortClause))));

		qry->connectByExpr->sort = transformSortClause(pstate,
										   stmt->connectClause->sort,
										   &qry->targetList,
										   EXPR_KIND_ORDER_BY,
										   true /* force SQL99 rules */);
	}

	qry->groupClause = transformGroupClause(pstate,
											stmt->groupClause,
											&qry->groupingSets,
											&qry->targetList,
											qry->sortClause,
											EXPR_KIND_GROUP_BY,
											false /* allow SQL92 rules */ );

	if (stmt->distinctClause == NIL)
	{
		qry->distinctClause = NIL;
		qry->hasDistinctOn = false;
	}
	else if (linitial(stmt->distinctClause) == NULL)
	{
		/* We had SELECT DISTINCT */
		qry->distinctClause = transformDistinctClause(pstate,
													  &qry->targetList,
													  qry->sortClause,
													  false);
		qry->hasDistinctOn = false;
	}
	else
	{
		/* We had SELECT DISTINCT ON */
		qry->distinctClause = transformDistinctOnClause(pstate,
														stmt->distinctClause,
														&qry->targetList,
														qry->sortClause);
		qry->hasDistinctOn = true;
	}

	/* transform LIMIT */
	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											stmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   stmt->limitOption);
	qry->limitOption = stmt->limitOption;

	/* transform window clauses after we have seen all window functions */
	qry->windowClause = transformWindowDefinitions(pstate,
												   pstate->p_windowdefs,
												   &qry->targetList);

	/* resolve any still-unresolved output columns as being type text */
	if (pstate->p_resolve_unknowns)
		resolveTargetListUnknowns(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;
	qry->hasRowNumExpr = pstate->p_hasRownum;

	foreach(l, stmt->lockingClause)
	{
		transformLockingClause(pstate, qry,
							   (LockingClause *) lfirst(l), false);
	}

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
	{
		parseCheckAggregates(pstate, qry);
	}
	return qry;
}

/*
 * transformValuesClause -
 *	  transforms a VALUES clause that's being used as a standalone SELECT
 *
 * We build a Query containing a VALUES RTE, rather as if one had written
 *			SELECT * FROM (VALUES ...) AS "*VALUES*"
 */
static Query *
transformValuesClause(ParseState *pstate, SelectStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	List	   *exprsLists;
	List	   *coltypes = NIL;
	List	   *coltypmods = NIL;
	List	   *colcollations = NIL;
	List	  **colexprs = NULL;
	int			sublist_length = -1;
	bool		lateral = false;
#ifdef _PG_ORCL_
	List	  **nonnullexprs = NULL;
#endif
	RangeTblEntry *rte;
	int			rtindex;
	ListCell   *lc;
	ListCell   *lc2;
	int			i;

	qry->commandType = CMD_SELECT;

	/* Most SELECT stuff doesn't apply in a VALUES clause */
	Assert(stmt->distinctClause == NIL);
	Assert(stmt->intoClause == NULL);
	Assert(stmt->targetList == NIL);
	Assert(stmt->fromClause == NIL);
	Assert(stmt->whereClause == NULL);
	Assert(stmt->groupClause == NIL);
	Assert(stmt->havingClause == NULL);
	Assert(stmt->windowClause == NIL);
	Assert(stmt->op == SETOP_NONE);

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * For each row of VALUES, transform the raw expressions.
	 *
	 * Note that the intermediate representation we build is column-organized
	 * not row-organized.  That simplifies the type and collation processing
	 * below.
	 */
	foreach(lc, stmt->valuesLists)
	{
		List	   *sublist = (List *) lfirst(lc);

		/*
		 * Do basic expression transformation (same as a ROW() expr, but here
		 * we disallow SetToDefault)
		 */
		sublist = transformExpressionList(pstate, sublist,
										  EXPR_KIND_VALUES, false);

		/*
		 * All the sublists must be the same length, *after* transformation
		 * (which might expand '*' into multiple items).  The VALUES RTE can't
		 * handle anything different.
		 */
		if (sublist_length < 0)
		{
			/* Remember post-transformation length of first sublist */
			sublist_length = list_length(sublist);
			/* and allocate array for per-column lists */
			colexprs = (List **) palloc0(sublist_length * sizeof(List *));
#ifdef _PG_ORCL_
			if (ORA_MODE)
				nonnullexprs = (List **) palloc0(sublist_length * sizeof(List *));
#endif
		}
		else if (sublist_length != list_length(sublist))
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("VALUES lists must all be the same length"),
					 parser_errposition(pstate,
										exprLocation((Node *) sublist))));
		}

		/* Build per-column expression lists */
		i = 0;
		foreach(lc2, sublist)
		{
			Node	   *col = (Node *) lfirst(lc2);
#ifdef _PG_ORCL_
			if (ORA_MODE)
			{
				Node	*n_node = col;

				if (pstate->parentParseState != NULL)
					n_node = targetEntryGetOrigExpr(pstate, (Expr *) col, 0);

				/* Collect non-null expression for type selection */
				if (!IsA(n_node, Const) || !((Const *) n_node)->constisnull ||
						((Const *) n_node)->consttype != TEXTOID)
					nonnullexprs[i] = lappend(nonnullexprs[i], col);
			}
#endif

			colexprs[i] = lappend(colexprs[i], col);
			i++;
		}

		/* Release sub-list's cells to save memory */
		list_free(sublist);
	}

	/*
	 * Now resolve the common types of the columns, and coerce everything to
	 * those types.  Then identify the common typmod and common collation, if
	 * any, of each column.
	 *
	 * We must do collation processing now because (1) assign_query_collations
	 * doesn't process rangetable entries, and (2) we need to label the VALUES
	 * RTE with column collations for use in the outer query.  We don't
	 * consider conflict of implicit collations to be an error here; instead
	 * the column will just show InvalidOid as its collation, and you'll get a
	 * failure later if that results in failure to resolve a collation.
	 *
	 * Note we modify the per-column expression lists in-place.
	 */
	for (i = 0; i < sublist_length; i++)
	{
		Oid			coltype;
		int32		coltypmod = -1;
		Oid			colcoll;
		bool		first = true;

#ifdef _PG_ORCL_
		bool	explicit_cast = false;

		if (ORA_MODE && nonnullexprs[i] != NULL &&
			list_length(nonnullexprs[i]) != list_length(colexprs[i]))
		{
			coltype = select_common_type(pstate, nonnullexprs[i], "VALUES", NULL);
			explicit_cast = true;
		}
		else
		{
			coltype = select_common_type(pstate, colexprs[i], "VALUES", NULL);
		}
#else
		coltype = select_common_type(pstate, colexprs[i], "VALUES", NULL);
#endif

		foreach(lc, colexprs[i])
		{
			Node	   *col = (Node *) lfirst(lc);

#ifdef _PG_ORCL_
			if (ORA_MODE && explicit_cast)

				col = coerce_type(pstate, col, exprType(col), coltype, -1,
								COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
			else
#endif
				col = coerce_to_common_type(pstate, col, coltype, "VALUES");
			lfirst(lc) = (void *) col;
			if (first)
			{
				coltypmod = exprTypmod(col);
				first = false;
			}
			else
			{
				/* As soon as we see a non-matching typmod, fall back to -1 */
				if (coltypmod >= 0 && coltypmod != exprTypmod(col))
					coltypmod = -1;
			}
		}

		colcoll = select_common_collation(pstate, colexprs[i], true);

		coltypes = lappend_oid(coltypes, coltype);
		coltypmods = lappend_int(coltypmods, coltypmod);
		colcollations = lappend_oid(colcollations, colcoll);
	}

	/*
	 * Finally, rearrange the coerced expressions into row-organized lists.
	 */
	exprsLists = NIL;
	foreach(lc, colexprs[0])
	{
		Node	   *col = (Node *) lfirst(lc);
		List	   *sublist;

		sublist = list_make1(col);
		exprsLists = lappend(exprsLists, sublist);
	}
	list_free(colexprs[0]);
	for (i = 1; i < sublist_length; i++)
	{
		forboth(lc, colexprs[i], lc2, exprsLists)
		{
			Node	   *col = (Node *) lfirst(lc);
			List	   *sublist = lfirst(lc2);

			/* sublist pointer in exprsLists won't need adjustment */
			(void) lappend(sublist, col);
		}
		list_free(colexprs[i]);
	}

	/*
	 * Ordinarily there can't be any current-level Vars in the expression
	 * lists, because the namespace was empty ... but if we're inside CREATE
	 * RULE, then NEW/OLD references might appear.  In that case we have to
	 * mark the VALUES RTE as LATERAL.
	 */
	if (pstate->p_rtable != NIL &&
		contain_vars_of_level((Node *) exprsLists, 0))
		lateral = true;

	/*
	 * Generate the VALUES RTE
	 */
	rte = addRangeTableEntryForValues(pstate, exprsLists,
									  coltypes, coltypmods, colcollations,
									  NULL, lateral, true);
	addRTEtoQuery(pstate, rte, true, true, true);

	/* assume new rte is at end */
	rtindex = list_length(pstate->p_rtable);
	Assert(rte == rt_fetch(rtindex, pstate->p_rtable));

	/*
	 * Generate a targetlist as though expanding "*"
	 */
	Assert(pstate->p_next_resno == 1);
	qry->targetList = expandRelAttrs(pstate, rte, rtindex, 0, -1);

	/*
	 * The grammar allows attaching ORDER BY, LIMIT, and FOR UPDATE to a
	 * VALUES, so cope.
	 */
	qry->sortClause = transformSortClause(pstate,
										  stmt->sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											stmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   stmt->limitOption);
	qry->limitOption = stmt->limitOption;

	if (stmt->lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s cannot be applied to VALUES",
						LCS_asString(((LockingClause *)
									  linitial(stmt->lockingClause))->strength))));

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformSetOperationStmt -
 *	  transforms a set-operations tree
 *
 * A set-operation tree is just a SELECT, but with UNION/INTERSECT/EXCEPT
 * structure to it.  We must transform each leaf SELECT and build up a top-
 * level Query that contains the leaf SELECTs as subqueries in its rangetable.
 * The tree of set operations is converted into the setOperations field of
 * the top-level Query.
 */
static Query *
transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	SelectStmt *leftmostSelect;
	int			leftmostRTI;
	Query	   *leftmostQuery;
	SetOperationStmt *sostmt;
	List	   *sortClause;
	Node	   *limitOffset;
	Node	   *limitCount;
	List	   *lockingClause;
	WithClause *withClause;
	Node	   *node;
	ListCell   *left_tlist,
			   *lct,
			   *lcm,
			   *lcc,
			   *l;
	List	   *targetvars,
			   *targetnames,
			   *sv_namespace;
	int			sv_rtable_length;
	RangeTblEntry *jrte;
	int			tllen;

	qry->commandType = CMD_SELECT;

	/*
	 * Find leftmost leaf SelectStmt.  We currently only need to do this in
	 * order to deliver a suitable error message if there's an INTO clause
	 * there, implying the set-op tree is in a context that doesn't allow
	 * INTO.  (transformSetOperationTree would throw error anyway, but it
	 * seems worth the trouble to throw a different error for non-leftmost
	 * INTO, so we produce that error in transformSetOperationTree.)
	 */
	leftmostSelect = stmt->larg;
	while (leftmostSelect && leftmostSelect->op != SETOP_NONE)
		leftmostSelect = leftmostSelect->larg;
	Assert(leftmostSelect && IsA(leftmostSelect, SelectStmt) &&
		   leftmostSelect->larg == NULL);
	if (leftmostSelect->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT ... INTO is not allowed here"),
				 parser_errposition(pstate,
									exprLocation((Node *) leftmostSelect->intoClause))));

	/*
	 * We need to extract ORDER BY and other top-level clauses here and not
	 * let transformSetOperationTree() see them --- else it'll just recurse
	 * right back here!
	 */
	sortClause = stmt->sortClause;
	limitOffset = stmt->limitOffset;
	limitCount = stmt->limitCount;
	lockingClause = stmt->lockingClause;
	withClause = stmt->withClause;

	stmt->sortClause = NIL;
	stmt->limitOffset = NULL;
	stmt->limitCount = NULL;
	stmt->lockingClause = NIL;
	stmt->withClause = NULL;

	/* We don't support FOR UPDATE/SHARE with set ops at the moment. */
	if (lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with UNION/INTERSECT/%s",
						LCS_asString(((LockingClause *)
									  linitial(lockingClause))->strength),
						ORA_MODE ? "MINUS" : "EXCEPT")));

	/* Process the WITH clause independently of all else */
	if (withClause)
	{
		qry->hasRecursive = withClause->recursive;
		qry->cteList = transformWithClause(pstate, withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * Recursively transform the components of the tree.
	 */
	sostmt = castNode(SetOperationStmt,
					  transformSetOperationTree(pstate, stmt, true, NULL));
	Assert(sostmt);
	qry->setOperations = (Node *) sostmt;

	/*
	 * Re-find leftmost SELECT (now it's a sub-query in rangetable)
	 */
	node = sostmt->larg;
	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));
	leftmostRTI = ((RangeTblRef *) node)->rtindex;
	leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;
	Assert(leftmostQuery != NULL);

	/*
	 * Generate dummy targetlist for outer query using column names of
	 * leftmost select and common datatypes/collations of topmost set
	 * operation.  Also make lists of the dummy vars and their names for use
	 * in parsing ORDER BY.
	 *
	 * Note: we use leftmostRTI as the varno of the dummy variables. It
	 * shouldn't matter too much which RT index they have, as long as they
	 * have one that corresponds to a real RT entry; else funny things may
	 * happen when the tree is mashed by rule rewriting.
	 */
	qry->targetList = NIL;
	targetvars = NIL;
	targetnames = NIL;
	left_tlist = list_head(leftmostQuery->targetList);

	forthree(lct, sostmt->colTypes,
			 lcm, sostmt->colTypmods,
			 lcc, sostmt->colCollations)
	{
		Oid			colType = lfirst_oid(lct);
		int32		colTypmod = lfirst_int(lcm);
		Oid			colCollation = lfirst_oid(lcc);
		TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
		char	   *colName;
		TargetEntry *tle;
		Var		   *var;

		Assert(!lefttle->resjunk);
		colName = pstrdup(lefttle->resname);
		var = makeVar(leftmostRTI,
					  lefttle->resno,
					  colType,
					  colTypmod,
					  colCollation,
					  0);
		var->location = exprLocation((Node *) lefttle->expr);
		tle = makeTargetEntry((Expr *) var,
							  (AttrNumber) pstate->p_next_resno++,
							  colName,
							  false);
		qry->targetList = lappend(qry->targetList, tle);
		targetvars = lappend(targetvars, var);
		targetnames = lappend(targetnames, makeString(colName));
		left_tlist = lnext(left_tlist);
	}

	/*
	 * As a first step towards supporting sort clauses that are expressions
	 * using the output columns, generate a namespace entry that makes the
	 * output columns visible.  A Join RTE node is handy for this, since we
	 * can easily control the Vars generated upon matches.
	 *
	 * Note: we don't yet do anything useful with such cases, but at least
	 * "ORDER BY upper(foo)" will draw the right error message rather than
	 * "foo not found".
	 */
	sv_rtable_length = list_length(pstate->p_rtable);

	jrte = addRangeTableEntryForJoin(pstate,
									 targetnames,
									 JOIN_INNER,
									 targetvars,
									 NULL,
									 false);

	sv_namespace = pstate->p_namespace;
	pstate->p_namespace = NIL;

	/* add jrte to column namespace only */
	addRTEtoQuery(pstate, jrte, false, false, true);

	/*
	 * For now, we don't support resjunk sort clauses on the output of a
	 * setOperation tree --- you can only use the SQL92-spec options of
	 * selecting an output column by name or number.  Enforce by checking that
	 * transformSortClause doesn't add any items to tlist.
	 */
	tllen = list_length(qry->targetList);

	qry->sortClause = transformSortClause(pstate,
										  sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	/* restore namespace, remove jrte from rtable */
	pstate->p_namespace = sv_namespace;
	pstate->p_rtable = list_truncate(pstate->p_rtable, sv_rtable_length);

	if (tllen != list_length(qry->targetList))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid UNION/INTERSECT/EXCEPT ORDER BY clause"),
				 errdetail("Only result column names can be used, not expressions or functions."),
				 errhint("Add the expression/function to every SELECT, or move the UNION into a FROM clause."),
				 parser_errposition(pstate,
									exprLocation(list_nth(qry->targetList, tllen)))));

	qry->limitOffset = transformLimitClause(pstate, limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											stmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   stmt->limitOption);
	qry->limitOption = stmt->limitOption;

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;

	foreach(l, lockingClause)
	{
		transformLockingClause(pstate, qry,
							   (LockingClause *) lfirst(l), false);
	}

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	return qry;
}

#ifdef _PG_ORCL_
static void
setop_make_child_subquery(ParseState *pstate, Node **arg, List *tlist)
{
	if (IsA(*arg, RangeTblRef))
	{
		Query	*res_q = makeNode(Query);
		RangeTblEntry	*rte,
						*n_rte;
		RangeTblRef	*rtr;

		res_q->commandType = CMD_SELECT;
		res_q->targetList = tlist;

		rtr = castNode(RangeTblRef, *arg);

		rte = rt_fetch(rtr->rtindex, pstate->p_rtable);
		IncrementVarSublevelsUp((Node *) rte->subquery, 1, 1);

		n_rte = copyObject(rte);
		res_q->rtable = lappend(res_q->rtable, n_rte);
		rte->subquery = res_q;
		n_rte->inFromCl = true;

		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 1;
		res_q->jointree = makeFromExpr(list_make1(rtr), NULL);

		markTargetListOrigins(pstate, res_q->targetList);
	}
	else if (IsA(*arg, SetOperationStmt))
	{
		/* Make a Query to wrap SetOpStmt */
		Query	*res_q = makeNode(Query);
		char		selectName[32];
		RangeTblRef	*rtr;
		SetOperationStmt *sostmt = (SetOperationStmt *) (*arg);
		ListCell	*left_tlist,
					*lct,
					*lcm,
					*lcc;
		List		*left_qry_tlist;
		RangeTblEntry	*rentry;
		int		refidx;
		int		resno = 1;

		res_q->commandType = CMD_SELECT;
		res_q->setOperations = *arg;
		res_q->rtable = list_copy(pstate->p_rtable);
		res_q->targetList = NIL;

		left_qry_tlist = GetSetOperationTlist(pstate, *arg, &rentry, &refidx);
		left_tlist = list_head(left_qry_tlist);
		forthree(lct, sostmt->colTypes,
				 lcm, sostmt->colTypmods,
				 lcc, sostmt->colCollations)
		{
			Oid			colType = lfirst_oid(lct);
			int32		colTypmod = lfirst_int(lcm);
			Oid			colCollation = lfirst_oid(lcc);
			TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
			char	   *colName;
			TargetEntry *tle;
			Var		   *var;

			Assert(!lefttle->resjunk);
			colName = pstrdup(lefttle->resname);
			var = makeVar(refidx,
						  lefttle->resno,
						  colType,
						  colTypmod,
						  colCollation,
						  0);
			var->location = exprLocation((Node *) lefttle->expr);
			tle = makeTargetEntry((Expr *) var,
								  (AttrNumber) resno,
								  colName,
								  false);
			res_q->targetList = lappend(res_q->targetList, tle);
			left_tlist = lnext(left_tlist);
			resno++;
		}

		IncrementVarSublevelsUp(*arg, 1, 1);

		snprintf(selectName, sizeof(selectName), "*SELECT* %d",
							list_length(pstate->p_rtable) + 1);
		addRangeTableEntryForSubquery(pstate, res_q,
											makeAlias(selectName, NIL),
											false,
											false);
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = list_length(pstate->p_rtable);
		res_q->jointree = makeFromExpr(NULL, NULL);

		/* Push down current SetOpStmt */
		*arg = (Node *) rtr;

		setop_make_child_subquery(pstate, arg, tlist);
	}
	else
		elog(ERROR, "unrecognized node type: %d", (*arg)->type);
}

/*
 * targetEntryGetOrigExpr
 *   Walk down query tree, include CTE, to find the original expression of a
 *  target list entry.
 */
static Node *
targetEntryGetOrigExpr(ParseState *pstate, Expr *expr, int levelsup)
{
	Var		*var;
	RangeTblEntry	*rte = NULL;
	int		netlevelsup;
	AttrNumber	attnum;

	if (expr == NULL || !IsA(expr, Var))
		return (Node *) expr;

	/* Get the original expression of a Var */
	var = (Var *) expr;
	attnum = var->varattno;
	if (attnum == InvalidAttrNumber)
		return (Node *) var;

	netlevelsup = var->varlevelsup + levelsup;
	rte = GetRTEByRangeTablePosn(pstate, var->varno, netlevelsup);

	switch (rte->rtekind)
	{
		case RTE_RELATION:
		case RTE_REMOTE_DUMMY:
		case RTE_NAMEDTUPLESTORE:
		case RTE_FUNCTION:
		case RTE_TABLEFUNC:
			return (Node *) var; /* need not recursive down */
		case RTE_VALUES:
			return (Node *) list_nth((List *) linitial(rte->values_lists),
									 attnum - 1);
		case RTE_JOIN:
			{
				Expr *j_var = (Expr *)list_nth(rte->joinaliasvars, attnum - 1);

				return targetEntryGetOrigExpr(pstate, j_var, netlevelsup);
			}
		case RTE_CTE:
			/* recursive to CTE subquery */
			{
				CommonTableExpr *cte;
				TargetEntry *ste;

				cte = GetCTEForRTE(pstate, rte, netlevelsup);
				if (((Query *) cte->ctequery)->commandType != CMD_SELECT)
					return (Node *) var;

				ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
				if (ste == NULL)
					return (Node *) var;
				else if (rte->self_reference)
					return (Node *) ste->expr;
				else
				{
					ParseState	mypstate;
					Index		levelsup2;

					MemSet(&mypstate, 0, sizeof(mypstate));
					for (levelsup2 = 0;
						 levelsup2 < rte->ctelevelsup + netlevelsup;
						 levelsup2++)
						pstate = pstate->parentParseState;
					mypstate.parentParseState = pstate;
					mypstate.p_rtable = ((Query *) cte->ctequery)->rtable;
					mypstate.p_ctenamespace = ((Query *) cte->ctequery)->cteList;

					return targetEntryGetOrigExpr(&mypstate, ste->expr, 0);
				}
			}
		case RTE_SUBQUERY:
			{
				TargetEntry *ste;

				if (rte->subquery->commandType != CMD_SELECT)
					return (Node *) var;

				ste = get_tle_by_resno(rte->subquery->targetList, attnum);
				if (ste == NULL)
					return (Node *) var;
				else
				{
					ParseState	mypstate;
					Index		levelsup2;

					MemSet(&mypstate, 0, sizeof(mypstate));
					for (levelsup2 = 0;
						 levelsup2 < netlevelsup;
						 levelsup2++)
						pstate = pstate->parentParseState;
					mypstate.parentParseState = pstate;
					mypstate.p_rtable = rte->subquery->rtable;
					mypstate.p_ctenamespace = rte->subquery->cteList;

					return targetEntryGetOrigExpr(&mypstate, ste->expr, 0);
				}
			}
		default:
			elog(ERROR, "unrecognized RTE kind: %d", rte->relkind);
	}

	return (Node *) var;
}

static List *
GetSetOperationTlist(ParseState *pstate, Node *node, RangeTblEntry **ent, int *refid)
{
	int		rti;
	Query	*lquery;

	Assert(IsA(node, SetOperationStmt));

	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));

	*refid = rti = ((RangeTblRef *) node)->rtindex;
	*ent = rt_fetch(rti, pstate->p_rtable);
	lquery = (*ent)->subquery;

	return lquery->targetList;
}
#endif

/*
 * transformSetOperationTree
 *		Recursively transform leaves and internal nodes of a set-op tree
 *
 * In addition to returning the transformed node, if targetlist isn't NULL
 * then we return a list of its non-resjunk TargetEntry nodes.  For a leaf
 * set-op node these are the actual targetlist entries; otherwise they are
 * dummy entries created to carry the type, typmod, collation, and location
 * (for error messages) of each output column of the set-op node.  This info
 * is needed only during the internal recursion of this function, so outside
 * callers pass NULL for targetlist.  Note: the reason for passing the
 * actual targetlist entries of a leaf node is so that upper levels can
 * replace UNKNOWN Consts with properly-coerced constants.
 */
static Node *
transformSetOperationTree(ParseState *pstate, SelectStmt *stmt,
						  bool isTopLevel, List **targetlist)
{
	bool		isLeaf;

	Assert(stmt && IsA(stmt, SelectStmt));

	/* Guard against stack overflow due to overly complex set-expressions */
	check_stack_depth();

	/*
	 * Validity-check both leaf and internal SELECTs for disallowed ops.
	 */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("INTO is only allowed on first SELECT of UNION/INTERSECT/%s",
						   ORA_MODE ? "MINUS" : "EXCEPT"),
					parser_errposition(pstate,
									   exprLocation((Node *) stmt->intoClause))));

	/* We don't support FOR UPDATE/SHARE with set ops at the moment. */
	if (stmt->lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with UNION/INTERSECT/%s",
						LCS_asString(((LockingClause *)
									  linitial(stmt->lockingClause))->strength),
						ORA_MODE ? "MINUS" : "EXCEPT")));

	/*
	 * If an internal node of a set-op tree has ORDER BY, LIMIT, FOR UPDATE,
	 * or WITH clauses attached, we need to treat it like a leaf node to
	 * generate an independent sub-Query tree.  Otherwise, it can be
	 * represented by a SetOperationStmt node underneath the parent Query.
	 */
	if (stmt->op == SETOP_NONE)
	{
		Assert(stmt->larg == NULL && stmt->rarg == NULL);
		isLeaf = true;
	}
	else
	{
		Assert(stmt->larg != NULL && stmt->rarg != NULL);
		if (stmt->sortClause || stmt->limitOffset || stmt->limitCount ||
			stmt->lockingClause || stmt->withClause)
			isLeaf = true;
		else
			isLeaf = false;
	}

	if (isLeaf)
	{
		/* Process leaf SELECT */
		Query	   *selectQuery;
		char		selectName[32];
		RangeTblEntry *rte PG_USED_FOR_ASSERTS_ONLY;
		RangeTblRef *rtr;
		ListCell   *tl;

		/*
		 * Transform SelectStmt into a Query.
		 *
		 * This works the same as SELECT transformation normally would, except
		 * that we prevent resolving unknown-type outputs as TEXT.  This does
		 * not change the subquery's semantics since if the column type
		 * matters semantically, it would have been resolved to something else
		 * anyway.  Doing this lets us resolve such outputs using
		 * select_common_type(), below.
		 *
		 * Note: previously transformed sub-queries don't affect the parsing
		 * of this sub-query, because they are not in the toplevel pstate's
		 * namespace list.
		 */
		selectQuery = parse_sub_analyze((Node *) stmt, pstate,
										NULL, false, false);

		/*
		 * Check for bogus references to Vars on the current query level (but
		 * upper-level references are okay). Normally this can't happen
		 * because the namespace will be empty, but it could happen if we are
		 * inside a rule.
		 */
		if (pstate->p_namespace)
		{
			if (contain_vars_of_level((Node *) selectQuery, 1))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of same query level"),
						 parser_errposition(pstate,
											locate_var_of_level((Node *) selectQuery, 1))));
		}

		/*
		 * Extract a list of the non-junk TLEs for upper-level processing.
		 */
		if (targetlist)
		{
			*targetlist = NIL;
			foreach(tl, selectQuery->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tl);

				if (!tle->resjunk)
					*targetlist = lappend(*targetlist, tle);
			}
		}

		/*
		 * Make the leaf query be a subquery in the top-level rangetable.
		 */
		snprintf(selectName, sizeof(selectName), "*SELECT* %d",
				 list_length(pstate->p_rtable) + 1);
		rte = addRangeTableEntryForSubquery(pstate,
											selectQuery,
											makeAlias(selectName, NIL),
											false,
											false);

		/*
		 * Return a RangeTblRef to replace the SelectStmt in the set-op tree.
		 */
		rtr = makeNode(RangeTblRef);
		/* assume new rte is at end */
		rtr->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
		return (Node *) rtr;
	}
	else
	{
		/* Process an internal node (set operation node) */
		SetOperationStmt *op = makeNode(SetOperationStmt);
		List	   *ltargetlist;
		List	   *rtargetlist;
		ListCell   *ltl;
		ListCell   *rtl;
		const char *context;

		context = (stmt->op == SETOP_UNION ? "UNION" :
				   (stmt->op == SETOP_INTERSECT ? "INTERSECT" :
				   (stmt->op == SETOP_MINUS ? "MINUS" :
				    "EXCEPT")));

		op->op = stmt->op;
		op->all = stmt->all;

		/*
		 * Recursively transform the left child node.
		 */
		op->larg = transformSetOperationTree(pstate, stmt->larg,
											 false,
											 &ltargetlist);

		/*
		 * If we are processing a recursive union query, now is the time to
		 * examine the non-recursive term's output columns and mark the
		 * containing CTE as having those result columns.  We should do this
		 * only at the topmost setop of the CTE, of course.
		 */
		if (isTopLevel &&
			pstate->p_parent_cte &&
			pstate->p_parent_cte->cterecursive)
			determineRecursiveColTypes(pstate, op->larg, ltargetlist);

		/*
		 * Recursively transform the right child node.
		 */
		op->rarg = transformSetOperationTree(pstate, stmt->rarg,
											 false,
											 &rtargetlist);

		/*
		 * Verify that the two children have the same number of non-junk
		 * columns, and determine the types of the merged output columns.
		 */
		if (list_length(ltargetlist) != list_length(rtargetlist))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("each %s query must have the same number of columns",
							context),
					 parser_errposition(pstate,
										exprLocation((Node *) rtargetlist))));

#ifdef _PG_ORCL_
		if (ORA_MODE)
		{
			List	*n_ltlist = NIL;
			List	*n_rtlist = NIL;
			int		resno = 1;
			bool	lcoerced = false;
			bool	lneed_sub = false;
			bool	rneed_sub = false;
			List	*n_ltargetlist = ltargetlist;
			List	*n_rtargetlist = rtargetlist;
			RangeTblEntry	*lent;
			RangeTblEntry	*rent;
			int		lrefidx;
			int		rrefidx;
			ParseState	mypstate;

			MemSet(&mypstate, 0, sizeof(mypstate));
			mypstate.parentParseState = pstate;
			/*
			 * If larg or rarg is a SetOperationStmt, use leftmost query's
			 * targetlist as comparison one.
			 */
			if (!IsA(op->larg, RangeTblRef))
				n_ltargetlist = GetSetOperationTlist(pstate, op->larg, &lent, &lrefidx);
			else
				lent = rt_fetch(((RangeTblRef *) op->larg)->rtindex,
												pstate->p_rtable);

			if (!IsA(op->rarg, RangeTblRef))
				n_rtargetlist = GetSetOperationTlist(pstate, op->rarg, &rent, &rrefidx);
			else
				rent = rt_fetch(((RangeTblRef *) op->rarg)->rtindex,
															pstate->p_rtable);

			forboth(ltl, n_ltargetlist, rtl, n_rtargetlist)
			{
				TargetEntry *ltle = (TargetEntry *) lfirst(ltl);
				TargetEntry *rtle = (TargetEntry *) lfirst(rtl);
				Node	   *lcolnode = (Node *) ltle->expr;
				Node	   *rcolnode = (Node *) rtle->expr;
				Oid			lcoltype = exprType(lcolnode);
				Oid			rcoltype = exprType(rcolnode);
				int32		lcoltypmod = exprTypmod(lcolnode);
				int32		rcoltypmod = exprTypmod(rcolnode);
				Node		*var;
				TargetEntry	*te;
				bool		coerced = false;

				Node		*lorig_node;
				Node		*rorig_node;
				Oid			targetcoltype;

				mypstate.p_rtable = lent->subquery->rtable;
				mypstate.p_ctenamespace = lent->subquery->cteList;
				lorig_node = targetEntryGetOrigExpr(&mypstate, ltle->expr, 0);

				mypstate.p_rtable = rent->subquery->rtable;
				mypstate.p_ctenamespace = rent->subquery->cteList;
				rorig_node = targetEntryGetOrigExpr(&mypstate, rtle->expr, 0);

				var = (Node *) makeVar(1, resno, lcoltype,
									lcoltypmod, exprCollation(lcolnode), 0);
				if (!setOpTypeCanImplicitCast(pstate, lorig_node,
														lcoltype, rcoltype))
				{
					var = coerce_type(pstate, (Node *) var,
										exprType((Node *) var), rcoltype, -1,
						COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
					lneed_sub = true;
					coerced = true;

					/*
					 * If rcoltype is unknown, it will cast to text later,
					 * but lcoltype has been cast to rcoltype, then will cause
					 * error. So add check betwwen targettype and lcoltype.
					 */
					if (rcoltype == UNKNOWNOID)
					{
						targetcoltype = select_common_type(pstate,
										list_make2(var, rcolnode),
										context,
										NULL);
						if (targetcoltype == lcoltype)
						{
							var = (Node *) makeVar(1, resno, lcoltype,
									lcoltypmod, exprCollation(lcolnode), 0);
							lneed_sub = false;
							coerced = false;
						}
					}
				}
				/*
				 * If var is coerced, next target maybe overwrite coerced,
				 * so keep it in outer var to rewrite targetlist later:
				 *		select null, null from dual union all
				 *		select null, null from dual union all
				 *		select '1'::number, null from dual;
				 */
				if (!lcoerced && coerced)
					lcoerced = true;

				te = makeTargetEntry((Expr *) var, resno, ltle->resname, false);
				n_ltlist = lappend(n_ltlist, te);
				var = (Node *) makeVar(1, resno, rcoltype, rcoltypmod,
												exprCollation(rcolnode), 0);
				if (!coerced && !setOpTypeCanImplicitCast(pstate, rorig_node,
														rcoltype, lcoltype))
				{
					var = coerce_type(pstate, (Node *) var,
										exprType((Node *) var), lcoltype, -1,
								COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
					rneed_sub = true;
				}
				te = makeTargetEntry((Expr *) var, resno, rtle->resname, false);
				n_rtlist = lappend(n_rtlist, te);

				resno++;
			}

			/*
			 * Make a subquery to make a type cast. If inplace update the
			 * targetlist of child query with type-cast, this will change the
			 * child query semantics. This change is to fix a narrow case to
			 * reduce the chance to introduce SubqueryScan.
			 */
			if (lneed_sub || lcoerced)
			{
				setop_make_child_subquery(pstate, &op->larg, n_ltlist);
				ltargetlist = n_ltlist;
			}

			if (rneed_sub)
			{
				setop_make_child_subquery(pstate, &op->rarg, n_rtlist);
				rtargetlist = n_rtlist;
			}
		}
#endif
		if (targetlist)
			*targetlist = NIL;
		op->colTypes = NIL;
		op->colTypmods = NIL;
		op->colCollations = NIL;
		op->groupClauses = NIL;
		forboth(ltl, ltargetlist, rtl, rtargetlist)
		{
			TargetEntry *ltle = (TargetEntry *) lfirst(ltl);
			TargetEntry *rtle = (TargetEntry *) lfirst(rtl);
			Node	   *lcolnode = (Node *) ltle->expr;
			Node	   *rcolnode = (Node *) rtle->expr;
			Oid			lcoltype = exprType(lcolnode);
			Oid			rcoltype = exprType(rcolnode);
			int32		lcoltypmod = exprTypmod(lcolnode);
			int32		rcoltypmod = exprTypmod(rcolnode);
			Node	   *bestexpr;
			int			bestlocation;
			Oid			rescoltype;
			int32		rescoltypmod;
			Oid			rescolcoll;

			/* select common type, same as CASE et al */
			rescoltype = select_common_type(pstate,
											list_make2(lcolnode, rcolnode),
											context,
											&bestexpr);
			bestlocation = exprLocation(bestexpr);
			/* if same type and same typmod, use typmod; else default */
			if (lcoltype == rcoltype && lcoltypmod == rcoltypmod)
				rescoltypmod = lcoltypmod;
			else
				rescoltypmod = -1;

			/*
			 * Verify the coercions are actually possible.  If not, we'd fail
			 * later anyway, but we want to fail now while we have sufficient
			 * context to produce an error cursor position.
			 *
			 * For all non-UNKNOWN-type cases, we verify coercibility but we
			 * don't modify the child's expression, for fear of changing the
			 * child query's semantics.
			 *
			 * If a child expression is an UNKNOWN-type Const or Param, we
			 * want to replace it with the coerced expression.  This can only
			 * happen when the child is a leaf set-op node.  It's safe to
			 * replace the expression because if the child query's semantics
			 * depended on the type of this output column, it'd have already
			 * coerced the UNKNOWN to something else.  We want to do this
			 * because (a) we want to verify that a Const is valid for the
			 * target type, or resolve the actual type of an UNKNOWN Param,
			 * and (b) we want to avoid unnecessary discrepancies between the
			 * output type of the child query and the resolved target type.
			 * Such a discrepancy would disable optimization in the planner.
			 *
			 * If it's some other UNKNOWN-type node, eg a Var, we do nothing
			 * (knowing that coerce_to_common_type would fail).  The planner
			 * is sometimes able to fold an UNKNOWN Var to a constant before
			 * it has to coerce the type, so failing now would just break
			 * cases that might work.
			 */
			if (lcoltype != UNKNOWNOID)
				lcolnode = coerce_to_common_type(pstate, lcolnode,
												 rescoltype, context);
			else if (IsA(lcolnode, Const) ||
					 IsA(lcolnode, Param))
			{
				lcolnode = coerce_to_common_type(pstate, lcolnode,
												 rescoltype, context);
				ltle->expr = (Expr *) lcolnode;
			}

			if (rcoltype != UNKNOWNOID)
				rcolnode = coerce_to_common_type(pstate, rcolnode,
												 rescoltype, context);
			else if (IsA(rcolnode, Const) ||
					 IsA(rcolnode, Param))
			{
				rcolnode = coerce_to_common_type(pstate, rcolnode,
												 rescoltype, context);
				rtle->expr = (Expr *) rcolnode;
			}

			/*
			 * Select common collation.  A common collation is required for
			 * all set operators except UNION ALL; see SQL:2008 7.13 <query
			 * expression> Syntax Rule 15c.  (If we fail to identify a common
			 * collation for a UNION ALL column, the curCollations element
			 * will be set to InvalidOid, which may result in a runtime error
			 * if something at a higher query level wants to use the column's
			 * collation.)
			 */
			rescolcoll = select_common_collation(pstate,
												 list_make2(lcolnode, rcolnode),
												 (op->op == SETOP_UNION && op->all));

			/* emit results */
			op->colTypes = lappend_oid(op->colTypes, rescoltype);
			op->colTypmods = lappend_int(op->colTypmods, rescoltypmod);
			op->colCollations = lappend_oid(op->colCollations, rescolcoll);

			/*
			 * For all cases except UNION ALL, identify the grouping operators
			 * (and, if available, sorting operators) that will be used to
			 * eliminate duplicates.
			 */
			if (op->op != SETOP_UNION || !op->all)
			{
				SortGroupClause *grpcl = makeNode(SortGroupClause);
				Oid			sortop;
				Oid			eqop;
				bool		hashable;
				ParseCallbackState pcbstate;

				setup_parser_errposition_callback(&pcbstate, pstate,
												  bestlocation);

				/* determine the eqop and optional sortop */
				get_sort_group_operators(rescoltype,
										 false, true, false,
										 &sortop, &eqop, NULL,
										 &hashable);

				cancel_parser_errposition_callback(&pcbstate);

				/* we don't have a tlist yet, so can't assign sortgrouprefs */
				grpcl->tleSortGroupRef = 0;
				grpcl->eqop = eqop;
				grpcl->sortop = sortop;
				grpcl->nulls_first = false; /* OK with or without sortop */
				grpcl->hashable = hashable;

				op->groupClauses = lappend(op->groupClauses, grpcl);
			}

			/*
			 * Construct a dummy tlist entry to return.  We use a SetToDefault
			 * node for the expression, since it carries exactly the fields
			 * needed, but any other expression node type would do as well.
			 */
			if (targetlist)
			{
				SetToDefault *rescolnode = makeNode(SetToDefault);
				TargetEntry *restle;

				rescolnode->typeId = rescoltype;
				rescolnode->typeMod = rescoltypmod;
				rescolnode->collation = rescolcoll;
				rescolnode->location = bestlocation;
				restle = makeTargetEntry((Expr *) rescolnode,
										 0, /* no need to set resno */
										 NULL,
										 false);
				*targetlist = lappend(*targetlist, restle);
			}
		}

		return (Node *) op;
	}
}

/*
 * Process the outputs of the non-recursive term of a recursive union
 * to set up the parent CTE's columns
 */
static void
determineRecursiveColTypes(ParseState *pstate, Node *larg, List *nrtargetlist)
{
	Node	   *node;
	int			leftmostRTI;
	Query	   *leftmostQuery;
	List	   *targetList;
	ListCell   *left_tlist;
	ListCell   *nrtl;
	int			next_resno;

	/*
	 * Find leftmost leaf SELECT
	 */
	node = larg;
	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));
	leftmostRTI = ((RangeTblRef *) node)->rtindex;
	leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;
	Assert(leftmostQuery != NULL);

	/*
	 * Generate dummy targetlist using column names of leftmost select and
	 * dummy result expressions of the non-recursive term.
	 */
	targetList = NIL;
	left_tlist = list_head(leftmostQuery->targetList);
	next_resno = 1;

	foreach(nrtl, nrtargetlist)
	{
		TargetEntry *nrtle = (TargetEntry *) lfirst(nrtl);
		TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
		char	   *colName;
		TargetEntry *tle;

		Assert(!lefttle->resjunk);
		colName = pstrdup(lefttle->resname);
		tle = makeTargetEntry(nrtle->expr,
							  next_resno++,
							  colName,
							  false);
		targetList = lappend(targetList, tle);
		left_tlist = lnext(left_tlist);
	}

	/* Now build CTE's output column info using dummy targetlist */
	analyzeCTETargetList(pstate, pstate->p_parent_cte, targetList);
}


/*
 * transformUpdateStmt -
 *	  transforms an update statement
 */
static Query *
transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	ParseNamespaceItem *nsitem;
	Node	   *qual;

	qry->commandType = CMD_UPDATE;
	pstate->p_is_insert = false;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	qry->resultRelation = setTargetTable(pstate, stmt->relation,
										 stmt->relation->inh,
										 true,
										 ACL_UPDATE);

	/* grab the namespace item made by setTargetTable */
	nsitem = (ParseNamespaceItem *) llast(pstate->p_namespace);

	/* subqueries in FROM cannot access the result relation */
	nsitem->p_lateral_only = true;
	nsitem->p_lateral_ok = false;

	/*
	 * the FROM clause is non-standard SQL syntax. We used to be able to do
	 * this with REPLACE in POSTQUEL so we keep the feature.
	 */
	transformFromClause(pstate, stmt->fromClause);

	/* remaining clauses can reference the result relation normally */
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");
	qry->returningList = transformReturningList(pstate, stmt->returningList);

	/*
	 * Now we are done with SELECT-like processing, and can get on with
	 * transforming the target list to match the UPDATE target columns.
	 */
	qry->targetList = transformUpdateTargetList(pstate, stmt->targetList, (Node*) stmt->relation);

	if (ORA_MODE && IS_PGXC_COORDINATOR)
	{
		if (qry->targetList && contain_plpgsql_functions((Node *) qry->targetList))
		{
			elog(ERROR, "plpgsql's hook functions are not allowed as constraints.");
		}
	}

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasRowNumExpr = pstate->p_hasRownum;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformUpdateTargetList -
 *	handle SET clause in UPDATE/INSERT ... ON CONFLICT UPDATE
 *  handle SET clause in MERGE INTO ... MATCHED THEN UPDATE 
 */
List *
transformUpdateTargetList(ParseState *pstate, List *origTlist, Node *targetRangeVar)
{
	List          *tlist = NIL;
	RangeTblEntry *target_rte;
	ListCell      *orig_tl;
	ListCell      *tl;
	int16          attrNum = 0;

	tlist = transformTargetList(pstate, origTlist,
								EXPR_KIND_UPDATE_SOURCE);

	if (pstate->p_target_rangetblentry->rtekind == RTE_SUBQUERY)
		attrNum = list_length(pstate->p_target_rangetblentry->subquery->targetList);
	else
		attrNum = RelationGetNumberOfAttributes(pstate->p_target_relation);

	/* Prepare to assign non-conflicting resnos to resjunk attributes */
	if (pstate->p_next_resno <= attrNum)
		pstate->p_next_resno = attrNum + 1;

	/* Prepare non-junk columns for assignment to target table */
	target_rte = pstate->p_target_rangetblentry;
	orig_tl = list_head(origTlist);

	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		ResTarget  *origTarget;
		int			attrno;

		if (tle->resjunk)
		{
			/*
			 * Resjunk nodes need no additional processing, but be sure they
			 * have resnos that do not match any target columns; else rewriter
			 * or planner might get confused.  They don't need a resname
			 * either.
			 */
			tle->resno = (AttrNumber) pstate->p_next_resno++;
			tle->resname = NULL;
			continue;
		}
		if (orig_tl == NULL)
			elog(ERROR, "UPDATE target count mismatch --- internal error");

		origTarget = lfirst_node(ResTarget, orig_tl);

		if (pstate->p_target_rangetblentry->rtekind == RTE_SUBQUERY)
		{
			attrno = getAttNumByAttnameSubquery(pstate->p_target_rangetblentry, origTarget);

			if (attrno == InvalidAttrNumber)
				ereport(ERROR,
				        (errcode(ERRCODE_UNDEFINED_COLUMN),
				         errmsg("column \"%s\" of subquery does not exist", origTarget->name),
				         parser_errposition(pstate, origTarget->location)));
		}
		else
		{
			Assert(IsA(targetRangeVar, RangeVar));
			attrno = getAttNumByAttname(pstate, origTarget, (RangeVar *) targetRangeVar);
			if (attrno == InvalidAttrNumber)
			{
				if (!ORA_MODE)
				{
				ereport(ERROR,
				        (errcode(ERRCODE_UNDEFINED_COLUMN),
				         errmsg("column \"%s\" of relation \"%s\" does not exist",
				                origTarget->name,
				                RelationGetRelationName(pstate->p_target_relation)),
				         parser_errposition(pstate, origTarget->location)));
				}
				else
				{
					StringInfoData str;

					initStringInfo(&str);
					appendStringInfo(&str, "\"%s\"", origTarget->name);
					if (origTarget->indirection)
					{
						ListCell *lc;

						foreach (lc, origTarget->indirection)
						{
							Node *n = (Node *) lfirst(lc);

							if (IsA(n, String))
								appendStringInfo(&str, ".\"%s\"", strVal((Value *)n));
						}
					}

					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column %s of relation \"%s\" does not exist",
								 str.data,
								 RelationGetRelationName(pstate->p_target_relation)),
							 parser_errposition(pstate, origTarget->location)));
				}
			}
		}

#ifdef __OPENTENBASE__
		/* could not update distributed columns */
		if (IS_PGXC_COORDINATOR && pstate->p_target_rangetblentry->rtekind != RTE_SUBQUERY)
		{
			checkUpdateColValid(tle, attrno, pstate->p_target_relation, pstate->p_rtable);
		}
#endif

		updateTargetListEntry(pstate, tle, origTarget->name,
							  attrno,
							  origTarget->indirection,
							  origTarget->location);

		/* Mark the target column as requiring update permissions */
		target_rte->updatedCols = bms_add_member(target_rte->updatedCols,
												 attrno - FirstLowInvalidHeapAttributeNumber);

		orig_tl = lnext(orig_tl);
	}

	if (orig_tl != NULL)
		elog(ERROR, "UPDATE target count mismatch --- internal error");

	return tlist;
}

/*
 * transformReturningList -
 *	handle a RETURNING clause in INSERT/UPDATE/DELETE
 */
static List *
transformReturningList(ParseState *pstate, List *returningList)
{
	List	   *rlist;
	int			save_next_resno;

	if (returningList == NIL)
		return NIL;				/* nothing to do */

	/*
	 * We need to assign resnos starting at one in the RETURNING list. Save
	 * and restore the main tlist's value of p_next_resno, just in case
	 * someone looks at it later (probably won't happen).
	 */
	save_next_resno = pstate->p_next_resno;
	pstate->p_next_resno = 1;

	/* transform RETURNING identically to a SELECT targetlist */
	rlist = transformTargetList(pstate, returningList, EXPR_KIND_RETURNING);

	/*
	 * Complain if the nonempty tlist expanded to nothing (which is possible
	 * if it contains only a star-expansion of a zero-column table).  If we
	 * allow this, the parsed Query will look like it didn't have RETURNING,
	 * with results that would probably surprise the user.
	 */
	if (rlist == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RETURNING must have at least one column"),
				 parser_errposition(pstate,
									exprLocation(linitial(returningList)))));

	/* mark column origins */
	markTargetListOrigins(pstate, rlist);

	/* resolve any still-unresolved output columns as being type text */
	if (pstate->p_resolve_unknowns)
		resolveTargetListUnknowns(pstate, rlist);

	/* restore state */
	pstate->p_next_resno = save_next_resno;

	return rlist;
}


/*
 * transformDeclareCursorStmt -
 *	transform a DECLARE CURSOR Statement
 *
 * DECLARE CURSOR is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.  We used to postpone that until execution, but it's really necessary
 * to do it during the normal parse analysis phase to ensure that side effects
 * of parser hooks happen at the expected time.
 */
static Query *
transformDeclareCursorStmt(ParseState *pstate, DeclareCursorStmt *stmt)
{
	Query	   *result;
	Query	   *query;

	/*
	 * Don't allow both SCROLL and NO SCROLL to be specified
	 */
	if ((stmt->options & CURSOR_OPT_SCROLL) &&
		(stmt->options & CURSOR_OPT_NO_SCROLL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
				 errmsg("cannot specify both SCROLL and NO SCROLL")));

	/* Transform contained query, not allowing SELECT INTO */
	query = transformStmt(pstate, stmt->query);
	stmt->query = (Node *) query;

	/* Grammar should not have allowed anything but SELECT */
	if (!IsA(query, Query) ||
		query->commandType != CMD_SELECT)
		elog(ERROR, "unexpected non-SELECT command in DECLARE CURSOR");

	/*
	 * We also disallow data-modifying WITH in a cursor.  (This could be
	 * allowed, but the semantics of when the updates occur might be
	 * surprising.)
	 */
	if (query->hasModifyingCTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("DECLARE CURSOR must not contain data-modifying statements in WITH")));

	/* FOR UPDATE and WITH HOLD are not compatible */
	if (query->rowMarks != NIL && (stmt->options & CURSOR_OPT_HOLD))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("DECLARE CURSOR WITH HOLD ... %s is not supported",
						LCS_asString(((RowMarkClause *)
									  linitial(query->rowMarks))->strength)),
				 errdetail("Holdable cursors must be READ ONLY.")));

	/* FOR UPDATE and SCROLL are not compatible */
	if (query->rowMarks != NIL && (stmt->options & CURSOR_OPT_SCROLL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("DECLARE SCROLL CURSOR ... %s is not supported",
						LCS_asString(((RowMarkClause *)
									  linitial(query->rowMarks))->strength)),
				 errdetail("Scrollable cursors must be READ ONLY.")));

	/* FOR UPDATE and INSENSITIVE are not compatible */
	if (query->rowMarks != NIL && (stmt->options & CURSOR_OPT_INSENSITIVE))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("DECLARE INSENSITIVE CURSOR ... %s is not supported",
						LCS_asString(((RowMarkClause *)
									  linitial(query->rowMarks))->strength)),
				 errdetail("Insensitive cursors must be READ ONLY.")));

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}


/*
 * transformExplainStmt -
 *	transform an EXPLAIN Statement
 *
 * EXPLAIN is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.  We used to postpone that until execution, but it's really necessary
 * to do it during the normal parse analysis phase to ensure that side effects
 * of parser hooks happen at the expected time.
 */
static Query *
transformExplainStmt(ParseState *pstate, ExplainStmt *stmt)
{
	Query	   *result;

	/* transform contained query, allowing SELECT INTO */
	stmt->query = (Node *) transformOptionalSelectInto(pstate, stmt->query);

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}


/*
 * transformCreateTableAsStmt -
 *	transform a CREATE TABLE AS, SELECT ... INTO, or CREATE MATERIALIZED VIEW
 *	Statement
 *
 * As with DECLARE CURSOR and EXPLAIN, transform the contained statement now.
 */
static Query *
transformCreateTableAsStmt(ParseState *pstate, CreateTableAsStmt *stmt)
{
	Query	   *result;
	Query	   *query;

	pstate->long_type_check = (stmt->relkind == OBJECT_MATVIEW) ? LT_MAVIEW : LT_NONE;

	/* transform contained query, not allowing SELECT INTO */
	query = transformStmt(pstate, stmt->query);
	stmt->query = (Node *) query;

	/* additional work needed for CREATE MATERIALIZED VIEW */
	if (stmt->relkind == OBJECT_MATVIEW)
	{
		/*
		 * Prohibit a data-modifying CTE in the query used to create a
		 * materialized view. It's not sufficiently clear what the user would
		 * want to happen if the MV is refreshed or incrementally maintained.
		 */
		if (query->hasModifyingCTE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views must not use data-modifying statements in WITH")));

		/*
		 * Check whether any temporary database objects are used in the
		 * creation query. It would be hard to refresh data or incrementally
		 * maintain it if a source disappeared.
		 */
		if (isQueryUsingTempRelation(query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views must not use temporary tables or views")));

		/*
		 * A materialized view would either need to save parameters for use in
		 * maintaining/loading the data or prohibit them entirely.  The latter
		 * seems safer and more sane.
		 */
		if (query_contains_extern_params(query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views may not be defined using bound parameters")));

		/*
		 * For now, we disallow unlogged materialized views, because it seems
		 * like a bad idea for them to just go to empty after a crash. (If we
		 * could mark them as unpopulated, that would be better, but that
		 * requires catalog changes which crash recovery can't presently
		 * handle.)
		 */
		if (stmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views cannot be unlogged")));

		/*
		 * At runtime, we'll need a copy of the parsed-but-not-rewritten Query
		 * for purposes of creating the view's ON SELECT rule.  We stash that
		 * in the IntoClause because that's where intorel_startup() can
		 * conveniently get it from.
		 */
		stmt->into->viewQuery = (Node *) copyObject(query);
	}

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

/*
 * Supports the syntax for call function, refer to LIGHTWEIGHT_ORA
 * eg. call fun1();
 */
static Query *
TryTransformFunc(ParseState *pstate, CallStmt *stmt)
{
	SelectStmt   *new_stmt = NULL;
	FuncCall     *fn = NULL;
	ResTarget    *rt = NULL;
	List         *args;
	List         *new_args = NIL;
	MemoryContext old_cxt = CurrentMemoryContext;
	bool          isfunc = false;
	Query        *result = NULL;
	ParseExprKind sv_expr_kind = pstate->p_expr_kind;

	/* Similar to LIGHTWEIGHT_ORA, stored procedures do not support calling functions. Skipping the current processing logic. */
	if (InPlpgsqlFunc() || !enable_lightweight_ora_syntax)
		return NULL;

	PG_TRY();
	{
		ListCell *lc;

		pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

		args = copyObject(stmt->funccall->args);
		foreach (lc, args)
		{
			new_args = lappend(new_args, transformExpr(pstate,
													   (Node *) lfirst(lc),
													   EXPR_KIND_SELECT_TARGET));
		}

		if (ParseFuncOrColumn(pstate,
		                      stmt->funccall->funcname,
		                      new_args,
		                      pstate->p_last_srf,
		                      stmt->funccall,
		                      false,
		                      stmt->funccall->location) != NULL)
		{
			isfunc = true;
		}
	}
	PG_CATCH();
	{
		/* Flush any strings created in ErrorContext */
		FlushErrorState();
	}
	PG_END_TRY();

	if (isfunc)
	{
		fn = makeFuncCall(stmt->funccall->funcname, stmt->funccall->args, stmt->funccall->location);
		rt = makeNode(ResTarget);
		rt->location = stmt->funccall->location;
		rt->val = (Node *) fn;
		new_stmt = makeNode(SelectStmt);
		new_stmt->targetList = list_make1(rt);
		new_stmt->op = SETOP_NONE;
		result = transformStmt(pstate, (Node *) new_stmt);
	}
	else
	{
		pstate->p_expr_kind = sv_expr_kind;
	}

	MemoryContextSwitchTo(old_cxt);
	return result;
}

/*
 * transform a CallStmt
 *
 * We need to do parse analysis on the procedure call and its arguments.
 */
static Query *
transformCallStmt(ParseState *pstate, CallStmt *stmt)
{
	List     *targs;
	ListCell *lc;
	Node     *node;
	Query    *result = NULL;
	ParseExprItem	old_item = pstate->p_expr_item;

	pstate->p_expr_item = EXPR_ITEM_FN_ARGS;

	/*
	 * Attempt to parse as a select function beforehand. If successful, it is actually a function call.
	 * Adjust the stmt accordingly.
	 */
	result = TryTransformFunc(pstate, stmt);
	if (result != NULL)
		return result;

	targs = NIL;
	foreach (lc, stmt->funccall->args)
	{
		targs = lappend(targs, transformExpr(pstate,
											 (Node *) lfirst(lc),
											 EXPR_KIND_CALL_ARGUMENT));
	}

	pstate->p_expr_item = old_item;

	set_with_local_funcs(pstate);

	node = ParseFuncOrColumn(pstate,
	                         stmt->funccall->funcname,
	                         targs,
	                         pstate->p_last_srf,
	                         stmt->funccall,
	                         true,
	                         stmt->funccall->location);

	assign_expr_collations(pstate, node);

	stmt->funcexpr = castNode(FuncExpr, node);

	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

#ifdef _PG_ORCL_
/*
 * transform a CreateProfileStmt
 */
static Query *
transformCreateProfileStmt(ParseState *pstate, CreateProfileStmt *stmt)
{
    ListCell *lc;
    Query    *result;
    Node     *param_value;

    foreach(lc, stmt->profile_args)
    {
        param_value = (Node *)((ProfileParameter *)lfirst(lc))->value;
        if (nodeTag(param_value) == T_A_Expr)
            ((ProfileParameter *)lfirst(lc))->value = (Value *)transformExpr(pstate,
                                                                             param_value,
                                                                             pstate->p_expr_kind);
    }
    result = makeNode(Query);
    result->commandType = CMD_UTILITY;
    result->utilityStmt = (Node *)stmt;

    return result;
}

static Query *
transformAlterProfileStmt(ParseState *pstate, AlterProfileStmt *stmt)
{
    ListCell *lc;
    Query    *result;
    Node     *param_value;

    foreach(lc, stmt->profile_args)
    {
        param_value = (Node *)((ProfileParameter *)lfirst(lc))->value;
        if (nodeTag(param_value) == T_A_Expr)
            ((ProfileParameter *)lfirst(lc))->value = (Value *)transformExpr(pstate,
                                                                             param_value,
                                                                             pstate->p_expr_kind);
    }
    result = makeNode(Query);
    result->commandType = CMD_UTILITY;
    result->utilityStmt = (Node *)stmt;

    return result;
}

static Query *
transformCopyStmt(ParseState *pstate, CopyStmt *stmt)
{
	Query *result = NULL;
	List  *attinfolist = stmt->attinfolist;

	if (attinfolist)
	{
		int       i, numParams;
		Oid      *paramTypes = NULL;
		ListCell *l = NULL;
		bool      has_custom_expr = false;

		numParams = attinfolist->length;
		paramTypes = (Oid *) palloc(numParams * sizeof(Oid));

		for (i = 0; i < numParams; i++)
		{
			paramTypes[i] = TEXTOID;
		}

		/* transform the user-supplied expr in attinfolist. */
		foreach (l, attinfolist)
		{
			AttInfo *attr_info_s = (AttInfo *) (lfirst(l));

			if (attr_info_s->custom_expr)
			{
				/*  pre reserve the same number of params as the fields. */
				parse_fixed_parameters(pstate, paramTypes, numParams);
				attr_info_s->custom_expr =
					transformExpr(pstate, attr_info_s->custom_expr, EXPR_KIND_SELECT_TARGET);
				has_custom_expr = true;
			}
		}

		/* Clean up resources */
		if (!has_custom_expr)
		{
			pfree(paramTypes);
		}
	}

	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

#endif

#ifdef PGXC
/*
 * transformExecDirectStmt -
 *	transform an EXECUTE DIRECT Statement
 *
 * Handling is depends if we should execute on nodes or on Coordinator.
 * To execute on nodes we return CMD_UTILITY query having one T_RemoteQuery node
 * with the inner statement as a sql_command.
 * If statement is to run on Coordinator we should parse inner statement and
 * analyze resulting query tree.
 */
static Query *
transformExecDirectStmt(ParseState *pstate, ExecDirectStmt *stmt)
{
	Query		*result = makeNode(Query);
	char		*query = stmt->query;
	List		*nodelist = stmt->node_names;
	RemoteQuery	*step = makeNode(RemoteQuery);
	bool		is_local = false;
	List		*raw_parsetree_list;
	ListCell	*raw_parsetree_item;
	char		*nodename;
	int			nodeIndex;
	char		nodetype;

#ifdef __OPENTENBASE_C__
	/* Support only available on Coordinator */
	if (!IS_ACCESS_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT can only be executed on a Coordinator")));
#else
	/* Support not available on Datanodes */
	if (IS_DISTRIBUTED_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot be executed on a Datanode")));
#endif

	if (list_length(nodelist) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Support for EXECUTE DIRECT on multiple nodes is not available yet")));

	Assert(list_length(nodelist) == 1);
	Assert(IS_ACCESS_NODE);

	/* There is a single element here */
	nodename = strVal(linitial(nodelist));
#ifdef XCP
	nodetype = PGXC_NODE_NONE;
	nodeIndex = PGXCNodeGetNodeIdFromName(nodename, &nodetype);
	if (nodetype == PGXC_NODE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						nodename)));
#else
	nodeoid = get_pgxc_nodeoid(nodename);

	if (!OidIsValid(nodeoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						nodename)));

	/* Get node type and index */
	nodetype = get_pgxc_nodetype(nodeoid);
	nodeIndex = PGXCNodeGetNodeId(nodeoid, get_pgxc_nodetype(nodeoid));
#endif

	/* Check if node is requested is the self-node or not */
	if ((nodetype == PGXC_NODE_COORDINATOR ||
		(nodetype == PGXC_NODE_DATANODE && IS_CENTRALIZED_MODE)) &&
		nodeIndex == PGXCNodeId - 1)
		is_local = true;

	/* Transform the query into a raw parse list */
	raw_parsetree_list = pg_parse_query(query);

	/* EXECUTE DIRECT can just be executed with a single query */
	if (list_length(raw_parsetree_list) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot execute multiple queries")));

	/*
	 * if the query is a empty query, such as execute direct on(cn002) '' or execute direct on(cn002) '    ',
	 * raw_parsetree_list is null, set commandType to CMD_UTILITY
	 */
	if (raw_parsetree_list == NULL)
	{
		result->commandType = CMD_UTILITY;
		result->utilityStmt = (Node *) step;
	}

	/*
	 * Analyze the Raw parse tree
	 * EXECUTE DIRECT is restricted to one-step usage
	 */
	foreach(raw_parsetree_item, raw_parsetree_list)
	{
		RawStmt   *parsetree = lfirst_node(RawStmt, raw_parsetree_item);
		List *result_list = pg_analyze_and_rewrite(parsetree, query, NULL, 0, NULL);
		result = linitial_node(Query, result_list);
	}

	/* Default list of parameters to set */
	step->sql_statement = NULL;
	step->exec_nodes = makeNode(ExecNodes);
	step->combine_type = COMBINE_TYPE_NONE;
	step->sort = NULL;
	step->read_only = true;
	step->force_autocommit = false;
	step->cursor = NULL;

	/* This is needed by executor */
	step->sql_statement = pstrdup(query);
	if (nodetype == PGXC_NODE_COORDINATOR)
		step->exec_type = EXEC_ON_COORDS;
	else
		step->exec_type = EXEC_ON_DATANODES;

	step->reduce_level = 0;
	step->base_tlist = NIL;
	step->outer_alias = NULL;
	step->inner_alias = NULL;
	step->outer_reduce_level = 0;
	step->inner_reduce_level = 0;
	step->outer_relids = NULL;
	step->inner_relids = NULL;
	step->inner_statement = NULL;
	step->outer_statement = NULL;
	step->join_condition = NULL;

	/* Change the list of nodes that will be executed for the query and others */
	step->force_autocommit = false;
	step->combine_type = COMBINE_TYPE_SAME;
	step->exec_direct_type = EXEC_DIRECT_NONE;
	step->exec_direct_dn_allow = false;

	if (result->commandType == CMD_MERGE)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EXECUTE DIRECT cannot execute MERGE INTO query")));
	}

	/* Set up EXECUTE DIRECT flag */
	if (is_local)
	{
		if (result->commandType == CMD_UTILITY)
			step->exec_direct_type = (raw_parsetree_list == NULL) ? EXEC_DIRECT_LOCAL_NOTHING :
						EXEC_DIRECT_LOCAL_UTILITY;
		else
			step->exec_direct_type = EXEC_DIRECT_LOCAL;
	}
	else
	{
		switch(result->commandType)
		{
			case CMD_UTILITY:
				{
					step->exec_direct_type = EXEC_DIRECT_UTILITY;
					switch (nodeTag(result->utilityStmt))
					{
						case T_AlterNodeStmt:
						case T_CreateNodeStmt:
						case T_DropNodeStmt:
							step->exec_direct_dn_allow = true;
							step->is_node_stmt = true;
							break;
						default:
							break;
					}
				}
				break;
			case CMD_SELECT:
				step->exec_direct_type = EXEC_DIRECT_SELECT;
				step->exec_direct_dn_allow = true;
				break;
			case CMD_INSERT:
				step->exec_direct_type = EXEC_DIRECT_INSERT;
				break;
			case CMD_UPDATE:
				step->exec_direct_type = EXEC_DIRECT_UPDATE;
				break;
			case CMD_DELETE:
				step->exec_direct_type = EXEC_DIRECT_DELETE;
				break;
			default:
			    /* CMD_MERGE and CMD_NOTHING are handled before set up EXECUTE DIRECT flag */
				Assert(0);
		}
	}

	/* Build Execute Node list, there is a unique node for the time being */
	step->exec_nodes->nodeList = lappend_int(step->exec_nodes->nodeList, nodeIndex);

	if (result->commandType == CMD_SELECT && !result->hasForUpdate)
	{
		step->read_only = true;
		/* connection for RELATION_ACCESS_READ_FQS will be marked read_only */
		step->exec_nodes->accesstype = RELATION_ACCESS_READ_FQS;
	}

	if (!is_local)
	{
		result->utilityStmt = (Node *) step;
		result->exec_direct_local = false;	/* Execute direct ON(nodename) xxx sql is not local */
	}
	else
	{
		result->exec_direct_local = true;	/* Execute direct ON(nodename) xxx sql is local */
	}

	/*
	 * Reset the queryId since the caller would do that anyways.
	 */
	result->queryId = 0;

	return result;
}

#endif

/*
 * Produce a string representation of a LockClauseStrength value.
 * This should only be applied to valid values (not LCS_NONE).
 */
const char *
LCS_asString(LockClauseStrength strength)
{
	switch (strength)
	{
		case LCS_NONE:
			Assert(false);
			break;
		case LCS_FORKEYSHARE:
			return "FOR KEY SHARE";
		case LCS_FORSHARE:
			return "FOR SHARE";
		case LCS_FORNOKEYUPDATE:
			return "FOR NO KEY UPDATE";
		case LCS_FORUPDATE:
			return "FOR UPDATE";
	}
	return "FOR some";			/* shouldn't happen */
}

/*
 * Check for features that are not supported with FOR [KEY] UPDATE/SHARE.
 *
 * exported so planner can check again after rewriting, query pullup, etc
 */
void
CheckSelectLocking(Query *qry, LockClauseStrength strength)
{
	Assert(strength != LCS_NONE);	/* else caller error */

	if (qry->setOperations)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with UNION/INTERSECT/%s",
						LCS_asString(strength),
						ORA_MODE ? "MINUS" : "EXCEPT")));
	if (qry->distinctClause != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with DISTINCT clause",
						LCS_asString(strength))));
	if (qry->groupClause != NIL || qry->groupingSets != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with GROUP BY clause",
						LCS_asString(strength))));
	if (qry->havingQual != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with HAVING clause",
						LCS_asString(strength))));
	if (qry->hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with aggregate functions",
						LCS_asString(strength))));
	if (qry->hasWindowFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with window functions",
						LCS_asString(strength))));
	if (qry->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with set-returning functions in the target list",
						LCS_asString(strength))));
}

/*
 * Transform a FOR [KEY] UPDATE/SHARE clause
 *
 * This basically involves replacing names by integer relids.
 *
 * NB: if you need to change this, see also markQueryForLocking()
 * in rewriteHandler.c, and isLockedRefname() in parse_relation.c.
 */
static void
transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc,
					   bool pushedDown)
{
	List	   *lockedRels = lc->lockedRels;
	ListCell   *l;
	ListCell   *rt;
	Index		i;
	LockingClause *allrels;

	CheckSelectLocking(qry, lc->strength);

	/* make a clause we can pass down to subqueries to select all rels */
	allrels = makeNode(LockingClause);
	allrels->lockedRels = NIL;	/* indicates all rels */
	allrels->strength = lc->strength;
	allrels->waitPolicy = lc->waitPolicy;
	allrels->waitTimeout = lc->waitTimeout;

	if (lockedRels == NIL)
	{
		/*
		 * Lock all regular tables used in query and its subqueries.  We
		 * examine inFromCl to exclude auto-added RTEs, particularly NEW/OLD
		 * in rules.  This is a bit of an abuse of a mostly-obsolete flag, but
		 * it's convenient.  We can't rely on the namespace mechanism that has
		 * largely replaced inFromCl, since for example we need to lock
		 * base-relation RTEs even if they are masked by upper joins.
		 */
		i = 0;
		foreach(rt, qry->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

			++i;
			if (!rte->inFromCl)
				continue;
			switch (rte->rtekind)
			{
				case RTE_RELATION:
					if (rel_is_external_table(rte->relid) || rel_is_hdfsfdw_table(rte->relid))
						ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("SELECT FOR UPDATE/SHARE cannot be applied to external tables")));
					applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
									   pushedDown, lc->waitTimeout);
					rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
					break;
				case RTE_SUBQUERY:
					applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
									   pushedDown, lc->waitTimeout);

					/*
					 * FOR UPDATE/SHARE of subquery is propagated to all of
					 * subquery's rels, too.  We could do this later (based on
					 * the marking of the subquery RTE) but it is convenient
					 * to have local knowledge in each query level about which
					 * rels need to be opened with RowShareLock.
					 */
					transformLockingClause(pstate, rte->subquery,
										   allrels, true);
					break;
				default:
					/* ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs */
					break;
			}
		}
	}
	else
	{
		/*
		 * Lock just the named tables.  As above, we allow locking any base
		 * relation regardless of alias-visibility rules, so we need to
		 * examine inFromCl to exclude OLD/NEW.
		 */
		foreach(l, lockedRels)
		{
			RangeVar   *thisrel = (RangeVar *) lfirst(l);

			/* For simplicity we insist on unqualified alias names here */
			if (thisrel->catalogname || thisrel->schemaname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				/*------
				  translator: %s is a SQL row locking clause such as FOR UPDATE */
						 errmsg("%s must specify unqualified relation names",
								LCS_asString(lc->strength)),
						 parser_errposition(pstate, thisrel->location)));

			i = 0;
			foreach(rt, qry->rtable)
			{
				RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

				++i;
				if (!rte->inFromCl)
					continue;
				if (strcmp(rte->eref->aliasname, thisrel->relname) == 0)
				{
					switch (rte->rtekind)
					{
						case RTE_RELATION:
							if (rel_is_external_table(rte->relid) || rel_is_hdfsfdw_table(rte->relid))
								ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 	errmsg("SELECT FOR UPDATE/SHARE cannot be applied to external tables")));
							applyLockingClause(qry, i, lc->strength,
											   lc->waitPolicy, pushedDown, lc->waitTimeout);
							rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
							break;
						case RTE_SUBQUERY:
							applyLockingClause(qry, i, lc->strength,
											   lc->waitPolicy, pushedDown, lc->waitTimeout);
							/* see comment above */
							transformLockingClause(pstate, rte->subquery,
												   allrels, true);
							break;
						case RTE_JOIN:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a join",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_FUNCTION:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a function",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_TABLEFUNC:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a table function",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_VALUES:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to VALUES",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_CTE:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a WITH query",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_NAMEDTUPLESTORE:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a named tuplestore",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						default:
							elog(ERROR, "unrecognized RTE type: %d",
								 (int) rte->rtekind);
							break;
					}
					break;		/* out of foreach loop */
				}
			}
			if (rt == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_TABLE),
				/*------
				  translator: %s is a SQL row locking clause such as FOR UPDATE */
						 errmsg("relation \"%s\" in %s clause not found in FROM clause",
								thisrel->relname,
								LCS_asString(lc->strength)),
						 parser_errposition(pstate, thisrel->location)));
		}
	}
}

/*
 * Record locking info for a single rangetable item
 */
void
applyLockingClause(Query *qry, Index rtindex,
				   LockClauseStrength strength, LockWaitPolicy waitPolicy,
				   bool pushedDown, int waitTimeout)
{
	RowMarkClause *rc;

	Assert(strength != LCS_NONE);	/* else caller error */

	/* If it's an explicit clause, make sure hasForUpdate gets set */
	if (!pushedDown)
		qry->hasForUpdate = true;

	/* Check for pre-existing entry for same rtindex */
	if ((rc = get_parse_rowmark(qry, rtindex)) != NULL)
	{
		/*
		 * If the same RTE is specified with more than one locking strength,
		 * use the strongest.  (Reasonable, since you can't take both a shared
		 * and exclusive lock at the same time; it'll end up being exclusive
		 * anyway.)
		 *
		 * Similarly, if the same RTE is specified with more than one lock
		 * wait policy, consider that NOWAIT wins over SKIP LOCKED, which in
		 * turn wins over waiting for the lock (the default).  This is a bit
		 * more debatable but raising an error doesn't seem helpful. (Consider
		 * for instance SELECT FOR UPDATE NOWAIT from a view that internally
		 * contains a plain FOR UPDATE spec.)  Having NOWAIT win over SKIP
		 * LOCKED is reasonable since the former throws an error in case of
		 * coming across a locked tuple, which may be undesirable in some
		 * cases but it seems better than silently returning inconsistent
		 * results.
		 *
		 * And of course pushedDown becomes false if any clause is explicit.
		 */
		rc->strength = Max(rc->strength, strength);
		rc->waitPolicy = Max(rc->waitPolicy, waitPolicy);
		/* The values of (waitPolicy, waitTimeout) are as follows:
		 * waitPolicy, waitTimeout
		 * LockWaitBlock, 0 (plain FOR UPDATE)
		 * LockWaitBlock, n (n>0, WAIT n)
		 * LockWaitSkip, 0
		 * LockWaitError, 0 (NOWAIT, WAIT 0)
		 *
		 * When waitPolicy LockWaitBlock encouters LockWaitBlock, waitTimeout's value behaves like follows:
		 * waitTimeout1, waitTimeout2 => final waitTimeout  0,0=>0  0,1=>1 1,0 =>1 1,2=>1.
		 * In other cases, final waitTimeout always be the smaller one.
		 */
		if(rc->waitPolicy == LockWaitBlock && waitPolicy == LockWaitBlock)
			rc->waitTimeout = (rc->waitTimeout == 0 || waitTimeout == 0)? Max(rc->waitTimeout, waitTimeout) : Min(waitTimeout, rc->waitTimeout);
		else
			rc->waitTimeout = Min(waitTimeout, rc->waitTimeout);
		rc->pushedDown &= pushedDown;
		return;
	}

	/* Make a new RowMarkClause */
	rc = makeNode(RowMarkClause);
	rc->rti = rtindex;
	rc->strength = strength;
	rc->waitPolicy = waitPolicy;
	rc->waitTimeout = waitTimeout;
	rc->pushedDown = pushedDown;
	qry->rowMarks = lappend(qry->rowMarks, rc);
}

/*
 * Coverage testing for raw_expression_tree_walker().
 *
 * When enabled, we run raw_expression_tree_walker() over every DML statement
 * submitted to parse analysis.  Without this provision, that function is only
 * applied in limited cases involving CTEs, and we don't really want to have
 * to test everything inside as well as outside a CTE.
 */
#ifdef RAW_EXPRESSION_COVERAGE_TEST

static bool
test_raw_expression_coverage(Node *node, void *context)
{
	if (node == NULL)
		return false;
	return raw_expression_tree_walker(node,
									  test_raw_expression_coverage,
									  context);
}

#endif							/* RAW_EXPRESSION_COVERAGE_TEST */

/*
 * checkIndirectionValid
 *
 * This routine recurses for multiple levels of indirection --- but note that 
 * several adjacent A_Indices nodes in the indirection list are treated as a
 * single multidimensional subscript operation we have ignored these nodes.
 *
 */
static bool
checkIndirectionValid(ParseState *pstate, Oid targetTypeId, ListCell *indirection, int location)
{
	ListCell *indirectionCell;
	for_each_cell(indirectionCell, indirection)
	{
		Node      *n = lfirst(indirectionCell);
		Oid        typrelid;
		AttrNumber attnum;
		Oid        fieldTypeId;

		Assert(IsA(n, String));

		typrelid = typeidTypeRelid(targetTypeId);
		/* Not a composite type, so the indirection is illegal. */
		if (!OidIsValid(typrelid))
			return false;

		attnum = get_attnum(typrelid, strVal(n));
		/* no field specified by indirection */
		if (attnum == InvalidAttrNumber)
			return false;

		if (attnum < 0)
			ereport(ERROR,
			        (errcode(ERRCODE_UNDEFINED_COLUMN),
			         errmsg("cannot assign to system column \"%s\"", strVal(n)),
			         parser_errposition(pstate, location)));

		fieldTypeId = get_atttype(typrelid, attnum);
		if (!OidIsValid(fieldTypeId))
			return false;

		if (lnext(indirectionCell) != NULL)
			return checkIndirectionValid(pstate, fieldTypeId, lnext(indirectionCell), location);
	}
	return true;
}

/*
 * attnameIndirectionAttNum
 *
 * given relation, att name and indirection, return attnum of variable
 *
 * Returns InvalidAttrNumber if the attr or field specified by indirection doesn't exist (or is dropped).
 */
static AttrNumber
attnameIndirectionAttNum(ParseState *pstate, char* col_name, ListCell *indirection, int location)
{
	AttrNumber attrno = InvalidAttrNumber;
	Oid        targetTypeId = InvalidOid;

	attrno = attnameAttNum(pstate->p_target_relation, col_name, true);
	if (!AttributeNumberIsValid(attrno))
		return InvalidAttrNumber;

	targetTypeId = attnumTypeId(pstate->p_target_relation, attrno);
	if (OidIsValid(targetTypeId) &&
	    checkIndirectionValid(pstate, targetTypeId, indirection, location))
		return attrno;

	return InvalidAttrNumber;
}

/*
 * getAttNumByAttnameSubquery
 *
 * get target idx num in subquery targetlist, return InvalidAttrNumber if the target can not be found.
 */
int
getAttNumByAttnameSubquery(RangeTblEntry *rangeTblEntry, ResTarget *target)
{
	ListCell    *targetCell;
	TargetEntry *targetTle;
	List        *tlist;
	int          attrno = 0;
	bool         attrnoValid = false;

	Assert(rangeTblEntry->rtekind == RTE_SUBQUERY);
	tlist = rangeTblEntry->subquery->targetList;

	foreach (targetCell, tlist)
	{
		targetTle = lfirst_node(TargetEntry, targetCell);
		attrno++;
		if (pg_strcasecmp(targetTle->resname, target->name) == 0)
		{
			attrnoValid = true;
			break;
		}
	}

	return attrnoValid ? attrno : InvalidAttrNumber;
}
/*
 * getAttNumByAttname
 *
 * get the target attribute not in update stmt
 *
 * PG/OpenTenBase Primeval syntax can't handle these case: 
 * update table_name alias set table_name.col_name = value;
 * update table_name alias set alias.col_name = value;
 *
 * IN target ResTarget:
 * 		target->name store the first element, 
 *			it may be a column name, table anme, table alias name,  or schema name
 *		target->indirection which is a list, 
 *			it stores some name(T_String) or subscirbe(A_Indices, 
 *				by updating array column) 
 * It will fetch column name to 'target_name', 
 *	and returning the attribution number in target table
 *
 * need consider column name and indirection for composite type.
 */
int
getAttNumByAttname(ParseState *pstate, ResTarget *target, RangeVar *targetRangeVar)
{
	Relation  rel = NULL;
	char     *table_name = NULL;
	List     *name_list = NULL;
	ListCell *lc = NULL;
	int       attrno_ret = InvalidAttrNumber;
	bool      matched = false;
	char     *col_name_ret = NULL;
	int       colIdx = 0;
	char     *target_table_name = NULL;
	Value    *col_node = NULL;

	Assert(pstate);
	Assert(target);

	/* must set ORA_MODE to on for target with tableref indirection */
	if (PG_MODE || NULL == target->indirection)
	{
		return attnameAttNum(pstate->p_target_relation, target->name, true);
	}

	rel = pstate->p_target_relation;
	Assert(rel);

	/*
	 * filter the T_String node which store table name, alias name or column name
	 */
	foreach (lc, target->indirection)
	{
		Node *node = (Node *) lfirst(lc);
		if (IsA(node, String))
			name_list = lappend(name_list, node);
	}

	/*
	 * No names, but having A_Indices nodes, it menas "target->name" storing column name
	 */
	if (0 == list_length(name_list))
		return attnameAttNum(pstate->p_target_relation, target->name, true);

	/* Prefer to use alias */
	if (targetRangeVar->alias)
		target_table_name = targetRangeVar->alias->aliasname;
	else
		target_table_name = NameStr(rel->rd_rel->relname);

	if (list_length(name_list) >= 2)
	{
		/*
		 * the target->name is schema name or table name/alias. Here's the rule,
		 * first we compare the target->name and schema name when target with specified schema name,
		 * if matched then continue to check table and column names; we convert 'target' to PG
		 * format when all matched successfully; otherwise, we compare the target->name and table
		 * name/alias, check whether we could find the right column specified.
		 */
		Value *table_node = NULL;

		if (targetRangeVar->schemaname && strcmp(target->name, targetRangeVar->schemaname) == 0)
		{
			table_node = (Value *) linitial(name_list);
			table_name = strVal(table_node);

			if (strcmp(table_name, target_table_name) == 0)
			{
				col_node = (Value *) lsecond(name_list);
				attrno_ret = attnameIndirectionAttNum(pstate,
				                                      strVal(col_node),
				                                      name_list->head->next->next,
				                                      target->location);
				if (AttributeNumberIsValid(attrno_ret))
				{
					matched = true;
					colIdx = 2;
					col_name_ret = strVal(col_node);
				}
			}
		}

		if (strcmp(target->name, target_table_name) == 0)
		{
			int attrno = InvalidAttrNumber;
			col_node = (Value *) linitial(name_list);
			attrno = attnameIndirectionAttNum(pstate,
			                                  strVal(col_node),
			                                  name_list->head->next,
			                                  target->location);
			if (AttributeNumberIsValid(attrno))
			{
				if (matched)
				{
					ereport(ERROR,
					        (errcode(ERRCODE_AMBIGUOUS_COLUMN),
					         errmsg("column reference \"%s\" is ambiguous",
					                NameListToString(name_list)),
					         parser_errposition(pstate, target->location)));
				}
				matched = true;
				colIdx = 1;
				attrno_ret = attrno;
				col_name_ret = strVal(col_node);
			}
		}
	}
	else if (list_length(name_list) == 1)
	{
		attrno_ret =
			attnameIndirectionAttNum(pstate, target->name, name_list->head, target->location);
		if (AttributeNumberIsValid(attrno_ret))
		{
			matched = true;
			colIdx = 0;
			col_name_ret = target->name;
		}
		if (!matched && strcmp(target->name, target_table_name) == 0)
		{
			col_node = (Value *) linitial(name_list);
			attrno_ret = attnameAttNum(pstate->p_target_relation, strVal(col_node), true);
			if (AttributeNumberIsValid(attrno_ret))
			{
				matched = true;
				colIdx = 1;
				col_name_ret = strVal(col_node);
			}
		}
	}

	list_free(name_list);

	if (matched)
	{
		if (colIdx > 0)
		{
			pfree(target->name);
			target->name = pstrdup(col_name_ret);
		}
		while (colIdx)
		{
			Node *node = (Node *) linitial(target->indirection);
			pfree(node);
			target->indirection = list_delete_first(target->indirection);
			colIdx--;
		}
		return attrno_ret;
	}

	return InvalidAttrNumber;
}

#ifdef __OPENTENBASE__
static CopyStmt *
transformInsertValuesIntoCopyFrom(InsertStmt *stmt, char *transform_string)
{
	CopyStmt   *copy = makeNode(CopyStmt);
	
	/* copy from */
	copy->is_from = true;
	
	copy->relation = stmt->relation;
	
	copy->filename = transform_string;
	
	copy->data_list = stmt->data_list;
	
	copy->ncolumns = stmt->ninsert_columns;
	
	copy->ndatarows = stmt->ndatarows;
	
	/* insert into xxx(c2,c4,c6) to copy xxx(c2,c4,c6) */
	copy->attlist = NULL;
	/*
	 * INSERT will expand the targetlist according to the target relation's
	 * physical targetlist.
	 * insert into xxx(c3,c2,c4)----> insert into xxx(c1,c2,c3,c4)
	 * we transform any form of 'insert into' to copy xxx from ...
	 *
	 * if we write the values of insert_into into file, we also need this form
	 */
	if (stmt->cols)
	{
		ListCell *cell;
		
		foreach(cell, stmt->cols)
		{
			ResTarget *target = (ResTarget *)lfirst(cell);
			
			Value *v = makeString(target->name);
			
			copy->attlist = lappend(copy->attlist, v);
		}
	}
	
	copy->insert_into = true;
	
	return copy;
}
	
List *
transformInsertValuesIntoCopyFromPlan(List *plantree_list, InsertStmt *stmt, bool *success,
                                            char *transform_string, Query	*query)
{
	PlannedStmt *planstmt = makeNode(PlannedStmt);
	CopyStmt   *copy = transformInsertValuesIntoCopyFrom(stmt, transform_string);
	List	   *stmt_list = NULL;
	
	planstmt->commandType = CMD_UTILITY;
	planstmt->canSetTag = query->canSetTag;
	planstmt->utilityStmt = (Node *)copy;
	planstmt->stmt_location = query->stmt_location;
	planstmt->stmt_len = query->stmt_len;
	planstmt->guc_str = NULL;
	
	stmt_list = lappend(stmt_list, planstmt);

	if (success)
	{
		*success = true;
	}

	return stmt_list;
}

List *
transformInsertValuesIntoCopyFromQuery(InsertStmt *stmt, char *transform_string, Query *query)
{
	CopyStmt    *copy = transformInsertValuesIntoCopyFrom(stmt, transform_string);
	List        *query_list = NULL;
	
	query->commandType = CMD_UTILITY;
	query->utilityStmt = (Node *) copy;
	
	query_list = lappend(query_list, query);
	
	return query_list;
}

/*
 *  fixResTargetNameWithTableNameRef
 *  The goal of this function try to handle set_clause in updatestmt, and update
 *  with table name or alias prefix. Do this for compatibility ora.
 */
void
fixResTargetNameWithTableNameRef(Relation rd, RangeVar *rel, ResTarget *res)
{
    char *resname = NULL;
    AttrNumber attrnoIndName = InvalidAttrNumber;
    AttrNumber attrnoResName = InvalidAttrNumber;
    Oid attrtypeid = InvalidOid;
    Oid typrelid = InvalidOid;

    /* the length of indireciton can't less than 1 and the first item type must not be A_Indices */
    if ((list_length(res->indirection) < 1) || IsA(linitial(res->indirection), A_Indices))
    {
        return;
    }

    /* name must relname or aliasname */
    if ((strcmp(rel->relname, res->name) != 0) &&
        ((rel->alias == NULL) || (strcmp((rel->alias)->aliasname, res->name) != 0)))
    {
        return;
    }

    /*
     * if meet set_clause like a.b.c = 1 then the indirection will exceed one,
     * so treat first indirection as tablename/alisename like a in this ex.
     */
    resname = pstrdup(strVal(linitial(res->indirection)));
    if (list_length(res->indirection) > 1)
    {
        res->name = resname;
        res->indirection = list_delete_nth(res->indirection, 1);
        return;
    }

    /*
     * Here only meet set_clause which has two elements like a.b or test.b.
     * So first keep forward-ompatibility behavior of old version, that results
     * if a is a composite type and also relname/alisename then we first handle it as
     * composite type.
     * Otherwise we handle it like a.b like relname/alisename, then a is a relname just
     * update set_clause over.
     */
    attrnoResName = attnameAttNum(rd, res->name, true);
    attrnoIndName = attnameAttNum(rd, resname, true);

    if (attrnoResName > 0)
    {
        attrtypeid = attnumTypeId(rd, attrnoResName);
        if (attrtypeid)
        {
            typrelid = typeidTypeRelid(attrtypeid);
            if (typrelid && ((get_attnum(typrelid, resname) != InvalidAttrNumber) || (attrnoIndName <= 0)))
            {
                ereport(
                        NOTICE,
                        (errmsg("update field '%s' of column '%s', though it's ambiguous.", resname, res->name)));
                return;
            }
        }
    }

    res->name = resname;
    res->indirection = list_delete_nth(res->indirection, 1);
}

/*
 * fixResTargetListWithTableNameRef
 * 	  transforms an update set_clause_list with tablename or alias prefix
 */
void
fixResTargetListWithTableNameRef(Relation rd, RangeVar *rel, List *clause_list)
{
    ListCell *cell = NULL;

    foreach (cell, clause_list)
    {
        if (IsA(lfirst(cell), ResTarget))
        {
            fixResTargetNameWithTableNameRef(rd, rel, (ResTarget *) lfirst(cell));
        }
    }
}

/*
 * checkUpdateColValid
 *
 * Check whether the column can be legally updated.
 * We can not update the distribution key.
 */
void
checkUpdateColValid(TargetEntry *tle, AttrNumber updateAttrno, Relation targetRel, List *rtables)
{
	RelationLocInfo *rd_locator_info = targetRel->rd_locator_info;

	if (tle->resjunk || !IsDistributedColumn(updateAttrno, rd_locator_info))
		return;

	if (IsA(tle->expr, Var))
	{
		Oid            resultrel;
		Oid            varrel;
		Var           *var = (Var *) tle->expr;
		RangeTblEntry *rte = rt_fetch(var->varno, rtables);
		char          *aliasname = NULL;

		if (rte->eref)
			aliasname = rte->eref->aliasname;

		resultrel = RelationGetRelid(targetRel);
		varrel = rte->relid;

		if (resultrel == varrel && var->varattno == updateAttrno)
		{
			if (!aliasname || strcmp(aliasname, "excluded") != 0)
				return;
		}
	}

	ereport(ERROR,
		(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
	     (errmsg("Distributed column \"%s\" can't be updated in current version",
		 		 tle->resname))));
}

#endif
