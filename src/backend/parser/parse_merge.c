/* -------------------------------------------------------------------------
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * parse_merge.c
 *
 *
 * IDENTIFICATION
 *    src/backend/parser/parse_merge.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "pgstat.h"

#include "access/sysattr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parsetree.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_cte.h"
#include "parser/parse_merge.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "rewrite/rewriteHandler.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "nodes/execnodes.h"
#include "catalog/catalog.h"
#include "catalog/index.h"

static void  setNamespaceForMergeWhen(ParseState *pstate, MergeWhenClause *mergeWhenClause,
                                      Index targetRTI, Index sourceRTI);
static void  setNamespaceVisibilityForRTE(List *namespace, RangeTblEntry *rte, bool rel_visible,
                                          bool cols_visible);
static void checkUpdateOnJoinKey(ParseState *pstate, MergeWhenClause *clause, List *join_var_list, Node *target);
static bool checkTargetTableReplicated(Relation rel);
static void checkTargetTableSystemCatalog(Relation targetRel);
static void fixResTargetNameWithAlias(List *clause_list, const char *aliasname);
static void  check_system_column_varlist(List *varlist);
static void  check_system_column_node(Node *node);
static void  check_system_column_reference(List *joinVarList, List *mergeActionList);

/*
 * Make appropriate changes to the namespace visibility while transforming
 * individual action's quals and targetlist expressions. In particular, for
 * INSERT actions we must only see the source relation (since INSERT action is
 * invoked for NOT MATCHED tuples and hence there is no target tuple to deal
 * with). On the other hand, UPDATE and DELETE actions can see both source and
 * target relations.
 *
 * Also, since the internal join node can hide the source and target
 * relations, we must explicitly make the respective relation as visible so
 * that columns can be referenced unqualified from these relations.
 */
static void
setNamespaceForMergeWhen(ParseState *pstate, MergeWhenClause *mergeWhenClause, Index targetRTI,
                         Index sourceRTI)
{
    RangeTblEntry *targetRelRTE, *sourceRelRTE;

    targetRelRTE = rt_fetch(targetRTI, pstate->p_rtable);
    sourceRelRTE = rt_fetch(sourceRTI, pstate->p_rtable);

    if (mergeWhenClause->matched || ORA_MODE)
    {
        Assert(mergeWhenClause->commandType == CMD_UPDATE ||
               mergeWhenClause->commandType == CMD_DELETE ||
               mergeWhenClause->commandType == CMD_NOTHING || ORA_MODE);

        /* in opentenbase_ora compatible mode or MATCHED actions can see both target and source relations. */
        setNamespaceVisibilityForRTE(pstate->p_namespace, targetRelRTE, true, true);
        setNamespaceVisibilityForRTE(pstate->p_namespace, sourceRelRTE, true, true);
    }
    else
    {
        /*
         * NOT MATCHED actions can't see target relation, but they can see
         * source relation.
         */
        Assert(mergeWhenClause->commandType == CMD_INSERT ||
               mergeWhenClause->commandType == CMD_NOTHING);
        setNamespaceVisibilityForRTE(pstate->p_namespace, targetRelRTE, false, false);
        setNamespaceVisibilityForRTE(pstate->p_namespace, sourceRelRTE, true, true);
    }
}

static void
setNamespaceVisibilityForRTE(List *namespace, RangeTblEntry *rte, bool rel_visible,
                             bool cols_visible)
{
    ListCell *lc;

    foreach (lc, namespace)
    {
        ParseNamespaceItem *nsitem = (ParseNamespaceItem *)lfirst(lc);

        if (nsitem->p_rte == rte)
        {
            nsitem->p_rel_visible  = rel_visible;
            nsitem->p_cols_visible = cols_visible;
            break;
        }
    }
}

/* traverse set_clause_list and judge if the ColId is equal to aliasname of table */
static void
fixResTargetNameWithAlias(List *clause_list, const char *aliasname)
{
    ListCell *cell = NULL;
    foreach (cell, clause_list)
    {
        if (IsA(lfirst(cell), ResTarget))
        {
            ResTarget *res = (ResTarget *) lfirst(cell);
            if (!strcmp(res->name, aliasname) && res->indirection)
            {
                char *resname = pstrdup((char *) (((Value *) lfirst(list_head(res->indirection)))->val.str));
                ((ResTarget *) lfirst(cell))->name = resname;
                ((ResTarget *) lfirst(cell))->indirection = list_delete_nth(res->indirection, 1);
            }
        }
    }
}

/*
 * transformMergeStmt -
 *	  transforms a MERGE statement
 */
Query *
transformMergeStmt(ParseState *pstate, MergeStmt *stmt)
{
	Query         *qry = makeNode(Query);
	Query         *target_subqry;
	RangeTblEntry *target_rte;
	ListCell      *l = NULL;
	AclMode        targetPerms = ACL_NO_RIGHTS;
	Node          *joinExpr = NULL;
	FromExpr      *fromExpr = NULL;
	List          *mergeActionList = NIL;
	List          *join_var_list = NIL;
	bool           is_terminal[2];
	Index          sourceRTI;
	RangeTblEntry *sourceRTE;

	/* There can't be any outer WITH to worry about */
    Assert(pstate->p_ctenamespace == NIL);

    qry->commandType = CMD_MERGE;
    qry->hasRecursive = false;
	pstate->isMergeStmt = true;

	if (stmt->withClause)
	{
		if (stmt->withClause->recursive)
			ereport(ERROR,
			        (errcode(ERRCODE_SYNTAX_ERROR),
			         errmsg("WITH RECURSIVE is not supported for MERGE statement")));
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* Check WHEN clauses for permissions and sanity */
    is_terminal[0] = false;
    is_terminal[1] = false;

    foreach (l, stmt->mergeWhenClauses)
    {
        MergeWhenClause *mergeWhenClause = (MergeWhenClause *) lfirst(l);
        int when_type = (mergeWhenClause->matched ? 0 : 1);

        /* Collect action types so we can check Target permissions */
        switch (mergeWhenClause->commandType)
        {
            case CMD_INSERT:
                targetPerms |= ACL_INSERT;
                break;
            case CMD_UPDATE:
                targetPerms |= ACL_UPDATE;
                if (NULL != mergeWhenClause->deleteClause)
                    targetPerms |= ACL_DELETE;
                break;
            case CMD_DELETE:
                targetPerms |= ACL_DELETE;
                break;
            case CMD_NOTHING:
                break;
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                         errmsg("unknown action in MERGE WHEN clause")));
        }

        /* Check for unreachable WHEN clauses */
		if (is_terminal[when_type])
        {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("unreachable WHEN clause specified after unconditional WHEN clause")));
		}
		
        if (mergeWhenClause->condition == NULL)
        {
            is_terminal[when_type] = true;
        }
    }

	/*
     * Construct a query of the form
     * 	SELECT relation.ctid	--junk attribute
     *		  ,relation.tableoid	--junk attribute
     * 		  ,source_relation.<somecols>
     * 		  ,relation.<somecols>
     *  FROM relation RIGHT JOIN source_relation
     *  ON  join_condition; -- no WHERE clause - all conditions are applied in
     * executor
     *
     * stmt->relation is the target relation, given as a RangeVar
     * stmt->source_relation is a RangeVar or subquery
     *
     * We specify the join as a RIGHT JOIN as a simple way of forcing the
     * first (larg) RTE to refer to the target table.
     *
     * The MERGE query's join can be tuned in some cases, see below for these
     * special case tweaks.
     */
	if (IsA(stmt->target, RangeSubselect))
	{
		ParseState *sub_pstate = make_parsestate(pstate);
		target_subqry = transformStmt(sub_pstate, ((RangeSubselect *) stmt->target)->subquery);
		/* The grammar should have produced a SELECT */
		if (!IsA(target_subqry, Query) || target_subqry->commandType != CMD_SELECT)
			elog(ERROR, "unexpected non-SELECT command in MERGE INTO (SELECT ...)");

		target_rte = addRangeTableEntryForSubquery(pstate,
		                                           target_subqry,
		                                           ((RangeSubselect *) stmt->target)->alias,
		                                           false,
		                                           false);
		pstate->p_target_rangetblentry = target_rte;
		qry->resultRelation = list_length(pstate->p_rtable);
	}
	else
	{
		Assert(IsA(stmt->target, RangeVar));
		qry->resultRelation =
			setTargetTable(pstate, (RangeVar *) stmt->target, ((RangeVar *) stmt->target)->inh, false, targetPerms);
		CheckMergeTargetValid(pstate->p_target_relation);
	}

	/* Now transform the source relation to produce the source RTE. */
	transformFromClause(pstate, list_make1(stmt->sourceRelation));
	sourceRTI = list_length(pstate->p_rtable);
	sourceRTE = GetRTEByRangeTablePosn(pstate, sourceRTI, 0);

	/*
	 * Check that the target table doesn't conflict with the source table.
	 * This would typically be a checkNameSpaceConflicts call, but we want a
	 * more specific error message.
	 */
	if (strcmp(pstate->p_target_rangetblentry->eref->aliasname, sourceRTE->eref->aliasname) == 0)
		ereport(ERROR,
		        (errcode(ERRCODE_DUPLICATE_ALIAS),
		         errmsg("name \"%s\" specified more than once",
		                pstate->p_target_rangetblentry->eref->aliasname),
		         errdetail("The name is used both as MERGE target table and "
		                   "data source.")));
    /*
     * There's no need for a targetlist here; it'll be set up by
     * preprocess_targetlist later.
     */
    qry->targetList = NIL;
    qry->rtable     = pstate->p_rtable;
    /*
     * Transform the join condition.  This includes references to the target
     * side, so add that to the namespace.
     */
    addRTEtoQuery(pstate, pstate->p_target_rangetblentry, false, true, true);
	joinExpr = transformExpr(pstate, stmt->joinCondition, EXPR_KIND_JOIN_ON);
    /*
     * Create the temporary query's jointree using the joinlist we built using
     * just the source relation; the target relation is not included.  The
     * quals we use are the join conditions to the merge target.  The join
     * will be constructed fully by transform_MERGE_to_join.
     */
	fromExpr = makeFromExpr(pstate->p_joinlist, joinExpr);
	qry->jointree = makeFromExpr(list_make1(fromExpr), NULL);
    /* get var that used in ON clause, then use it to check some restriction */
    join_var_list = pull_var_clause(joinExpr, PVC_RECURSE_AGGREGATES | PVC_RECURSE_PLACEHOLDERS);

    /* Check unsupported merge when clauses */
    if (ORA_MODE)
	{
		if (list_length(stmt->mergeWhenClauses) > 2)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			         errmsg("More than two MERGE WHEN clauses are specified")));
		}

		else if (list_length(stmt->mergeWhenClauses) == 2)
		{
			MergeWhenClause *mergeWhenClause1 =
				(MergeWhenClause *) linitial(stmt->mergeWhenClauses);
			MergeWhenClause *mergeWhenClause2 = (MergeWhenClause *) lsecond(stmt->mergeWhenClauses);
			if (mergeWhenClause1->commandType == CMD_DELETE ||
			    mergeWhenClause2->commandType == CMD_DELETE)
				ereport(ERROR,
				        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				         errmsg("DELETE must be in MATCHED clause with UPDATE")));

			if (mergeWhenClause1->matched == true && mergeWhenClause2->matched == true)
			{
				ereport(ERROR,
				        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				         errmsg("Two WHEN MATCHED clauses are not supported for "
				                "MERGE INTO")));
			}
			if (mergeWhenClause1->matched == false && mergeWhenClause2->matched == false)
			{
				ereport(ERROR,
				        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				         errmsg("Two WHEN NOT MATCHED clauses are not supported "
				                "for MERGE INTO")));
			}
		}
	}
    
    /*
     * We now have a good query shape, so now look at the when conditions and
     * action targetlists.
     *
     * Overall, the MERGE Query's targetlist is NIL.
     *
     * Each individual action has its own targetlist that needs separate
     * transformation. These transforms don't do anything to the overall
     * targetlist, since that is only used for resjunk columns.
     *
     * We can reference any column in Target or Source, which is OK because
     * both of those already have RTEs. There is nothing like the EXCLUDED
     * pseudo-relation for INSERT ON CONFLICT.
     */
    mergeActionList = NIL;
    foreach (l, stmt->mergeWhenClauses)
    {
        MergeWhenClause *mergeWhenClause = (MergeWhenClause *)lfirst(l);
        MergeAction     *action          = makeNode(MergeAction);

        action->commandType              = mergeWhenClause->commandType;
        action->matched                  = mergeWhenClause->matched;

        /*
         * Set namespace for the specific action. This must be done before
         * analyzing the WHEN quals and the action targetlist.
         */
        setNamespaceForMergeWhen(pstate, mergeWhenClause, qry->resultRelation, sourceRTI);

        /*
         * Transform the WHEN condition.
         *
         * Note that these quals are NOT added to the join quals; instead they
         * are evaluated separately during execution to decide which of the
         * WHEN MATCHED or WHEN NOT MATCHED actions to execute.
         */
        action->qual =
            transformWhereClause(pstate, mergeWhenClause->condition, EXPR_KIND_MERGE_WHEN, "WHEN");

        /*
         * Transform target lists for each INSERT and UPDATE action stmt
         */
        switch (action->commandType)
        {
            case CMD_INSERT:
            {
				List          *exprList = NIL;
				ListCell      *lc = NULL;
				RangeTblEntry *rte = NULL;
				ListCell      *icols = NULL;
				ListCell      *attnos = NULL;
				List          *icolumns = NIL;
				List          *attrnos = NIL;
                List          *set_clause_list_copy = NIL;
                Alias         *alias = NULL;

				pstate->p_is_insert = true;

				set_clause_list_copy = mergeWhenClause->targetList;
				if (IsA(stmt->target, RangeVar))
					alias = ((RangeVar *) stmt->target)->alias;
				else
					alias = ((RangeSubselect *) stmt->target)->alias;
				if (alias != NULL)
				{
					char *aliasname = (char *) (alias->aliasname);
					fixResTargetNameWithAlias(set_clause_list_copy, aliasname);
				}
				mergeWhenClause->targetList = set_clause_list_copy;

				if (pstate->p_target_rangetblentry->rtekind == RTE_SUBQUERY)
					icolumns = checkInsertTargetsSubquery(pstate, mergeWhenClause->targetList, &attrnos);
				else
				{
					icolumns = checkInsertTargets(pstate, mergeWhenClause->targetList, &attrnos);
				}

                Assert(list_length(icolumns) == list_length(attrnos));

                action->override = mergeWhenClause->override;
                /*
                 * Handle INSERT much like in transformInsertStmt
                 */
                if (mergeWhenClause->values == NIL)
                {
                    /*
                     * We have INSERT ... DEFAULT VALUES.  We can handle
                     * this case by emitting an empty targetlist --- all
                     * columns will be defaulted when the planner expands
                     * the targetlist.
                     */
                    exprList = NIL;
                }
                else
                {
                    /*
                     * Process INSERT ... VALUES with a single VALUES
                     * sublist.  We treat this case separately for
                     * efficiency.  The sublist is just computed directly
                     * as the Query's targetlist, with no VALUES RTE.  So
                     * it works just like a SELECT without any FROM.
                     */
                    /*
                     * Do basic expression transformation (same as a ROW()
                     * expr, but allow SetToDefault at top level)
                     */
                    exprList = transformExpressionList(pstate, mergeWhenClause->values, EXPR_KIND_INSERT_TARGET, true);

                    /* Prepare row for assignment to target table */
                    exprList = transformInsertRow(pstate, exprList, mergeWhenClause->targetList, icolumns, attrnos, false);
                }

                /*
                 * Generate action's target list using the computed list
                 * of expressions. Also, mark all the target columns as
                 * needing insert permissions.
                 */
                rte = pstate->p_target_rangetblentry;
                forthree(lc, exprList, icols, icolumns, attnos, attrnos)
                {
					Expr        *expr = (Expr *) lfirst(lc);
					ResTarget   *col = lfirst_node(ResTarget, icols);
					AttrNumber   attr_num = (AttrNumber) lfirst_int(attnos);
					TargetEntry *tle;

                    tle = makeTargetEntry(expr, attr_num, col->name, false);
                    action->targetList = lappend(action->targetList, tle);

					rte->insertedCols =
						bms_add_member(rte->insertedCols,
					                   attr_num - FirstLowInvalidHeapAttributeNumber);
				}
            }
            break;
            case CMD_UPDATE:
            {
                List *set_clause_list_copy = mergeWhenClause->targetList;

				if (IsA(stmt->target, RangeVar))
					fixResTargetListWithTableNameRef(pstate->p_target_relation,
					                                 (RangeVar *) stmt->target,
					                                 set_clause_list_copy);
				else
				{
					Alias *alias = ((RangeSubselect *) stmt->target)->alias;
					char  *aliasname = (char *) (alias->aliasname);
					fixResTargetNameWithAlias(set_clause_list_copy, aliasname);
				}
				mergeWhenClause->targetList = set_clause_list_copy;
				
				pstate->p_is_insert = false;
				
				action->targetList = transformUpdateTargetList(pstate,
				                                               mergeWhenClause->targetList, stmt->target);

                /* check if update cols are leagal */
                checkUpdateOnJoinKey(pstate, mergeWhenClause, join_var_list, stmt->target);

                if (ORA_MODE && NULL != mergeWhenClause->deleteClause)
                {
                    MergeWhenClause *deleteClause = mergeWhenClause->deleteClause;
                    MergeAction     *deleteAction = makeNode(MergeAction);

                    deleteAction->commandType = deleteClause->commandType;
                    deleteAction->matched = deleteClause->matched;

					deleteAction->qual = transformWhereClause(pstate, deleteClause->condition, EXPR_KIND_MERGE_WHEN,
																								"WHEN");
					action->deleteAction = deleteAction;
				}
			}
            break;
            case CMD_DELETE:
                break;
            case CMD_NOTHING:
                action->targetList = NIL;
                break;
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                         errmsg("unknown action in MERGE WHEN clause")));
        }

        mergeActionList = lappend(mergeActionList, action);
    }

    /* cannot reference system column */
	check_system_column_reference(join_var_list, mergeActionList);
	
    qry->mergeActionList = mergeActionList;
    qry->returningList = NULL;
    qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasRowNumExpr = pstate->p_hasRownum;

	assign_query_collations(pstate, qry);

    return qry;
}

List *
expandTargetTL(List *te_list, RangeTblEntry *target_rte, Index rtindex)
{
    List *names = NIL;
    List *vars = NIL;
    ListCell *name = NULL;
    ListCell *var = NULL;
    AttrNumber resno = 0;

    expandRTE(target_rte, rtindex, 0, -1, true, &names, &vars);

    /* Require read access to the table. */
    target_rte->requiredPerms |= ACL_SELECT;
    resno = list_length(te_list) + 1;

    forboth(name, names, var, vars)
    {
        char *label = strVal(lfirst(name));
        Var *varnode = (Var *) lfirst(var);
        TargetEntry *te = NULL;

        te = makeTargetEntry((Expr *) varnode, resno++, label, false);
        te_list = lappend(te_list, te);
    }

    Assert(name == NULL && var == NULL); /* lists not the same length? */

    return te_list;
}

static void
checkUpdateOnJoinKey(ParseState *pstate, MergeWhenClause *clause, List *join_var_list, Node *target)
{
    ListCell *update_target_cell = NULL;
    ListCell *join_var_cell = NULL;

    foreach (update_target_cell, clause->targetList)
    {
        ResTarget *origTarget = (ResTarget *) lfirst(update_target_cell);
        int attrno = InvalidAttrNumber;

        Assert(IsA(origTarget, ResTarget));

		if (pstate->p_target_rangetblentry->rtekind == RTE_SUBQUERY)
			attrno = getAttNumByAttnameSubquery(pstate->p_target_rangetblentry, origTarget);
		else
		{
            Assert(IsA(target, RangeVar));
			attrno = getAttNumByAttname(pstate, origTarget, (RangeVar*)target);
		}

        /* we checked already, should not happend */
        if (attrno == InvalidAttrNumber)
        {
			if (pstate->p_target_rangetblentry->rtekind != RTE_SUBQUERY)
				ereport(ERROR,
				        (errcode(ERRCODE_UNDEFINED_COLUMN),
				         errmsg("column \"%s\" of relation \"%s\" does not exist",
				                origTarget->name,
				                RelationGetRelationName(pstate->p_target_relation)),
				         parser_errposition(pstate, origTarget->location)));
			else
				ereport(ERROR,
				        (errcode(ERRCODE_UNDEFINED_COLUMN),
				         errmsg("column \"%s\" of subquery does not exist", origTarget->name),
				         parser_errposition(pstate, origTarget->location)));
        }
        /* can't update columns that referenced in the ON clause */
        foreach (join_var_cell, join_var_list)
        {
            Var *join_var = (Var *) lfirst(join_var_cell);
            if (join_var->varno == 1 && join_var->varattno == attrno)
            {
				if (pstate->p_target_rangetblentry->rtekind != RTE_SUBQUERY)
					ereport(ERROR,
					        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					         errmsg("Columns referenced in the ON Clause "
					                "cannot be updated: \"%s\".\"%s\"",
					                RelationGetRelationName(pstate->p_target_relation),
					                origTarget->name)));

				else
					ereport(ERROR,
					        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					         errmsg("Columns referenced in the ON Clause "
					                "cannot be updated: \"%s\"",
					                origTarget->name)));
            }
        }
    }
}

void
checkMergeUpdateOnJoinKey(Query *parse, List *target, List *join_var_list)
{
	ListCell      *update_target_cell = NULL;
	ListCell      *join_var_cell = NULL;
	RangeTblEntry *targetRTE;
    Relation targetRel;
	int            result_relation = parse->resultRelation;
	List          *range_table = parse->rtable;

	targetRTE = rt_fetch(result_relation, range_table);
    targetRel = heap_open(targetRTE->relid, NoLock);
	foreach (update_target_cell, target)
	{
		TargetEntry *origTarget = (TargetEntry *) lfirst(update_target_cell);
		int          attrno = InvalidAttrNumber;

		attrno = origTarget->resno;

		/* we checked already, should not happend */
		if (attrno == InvalidAttrNumber)
		{
			ereport(ERROR,
			        (errcode(ERRCODE_UNDEFINED_COLUMN),
			         errmsg("column \"%s\" of relation \"%s\" does not exist",
			                origTarget->resname,
			                targetRTE->eref->aliasname)));
		}
		/* can't update columns that referenced in the ON clause */
		foreach (join_var_cell, join_var_list)
		{
			Var           *join_var = (Var *) lfirst(join_var_cell);
			RangeTblEntry *sourceRTE ;

            if (!IsA(join_var, Var) || join_var->varno < 1)
                continue;
            
            sourceRTE = list_nth_node(RangeTblEntry, range_table, join_var->varno - 1);
			if (sourceRTE->rtekind != RTE_RELATION)
				continue;
			/*
			 * case 1: The source table and target table are the same
			 * case 2: The source table is a child partition of the target table
			 * case 3: The target table is a child partition of the source table
			 */
			if (join_var->varno != result_relation && targetRTE->relid != sourceRTE->relid)
			{
				Relation sourceRel;
				sourceRel = heap_open(sourceRTE->relid, NoLock);

				if (!is_partition_descendant_of(targetRel, sourceRel) &&
				    !is_partition_descendant_of(sourceRel, targetRel))
				{
					heap_close(sourceRel, NoLock);
					continue;
				}

				heap_close(sourceRel, NoLock);
			}
			if (join_var->varno != result_relation)
			{
				targetRTE = sourceRTE;
			}
			if (join_var->varattno == attrno)
			{
				ereport(ERROR,
				        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				         errmsg("Columns referenced in the ON Clause "
				                "cannot be updated: \"%s\".\"%s\"",
				                targetRTE->eref->aliasname,
				                origTarget->resname)));
			}
		}
	}
    heap_close(targetRel, NoLock);
}

static bool
checkTargetTableReplicated(Relation rel)
{
    if (rel == NULL)
    {
        return false;
    }
    
    if (rel->rd_rel->relkind != RELKIND_RELATION &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
    {
        /* Bad relation type */
        return false;
    }

    /* Any column updation on local relations is fine */
    if (rel->rd_locator_info == NULL)
    {
        return false;
    }
    /* Only relations distributed by value can be checked */
    if (IsRelationReplicated(rel->rd_locator_info))
    {
        return true;
    }
    return false;
}

static void
checkTargetTableSystemCatalog(Relation targetRel)
{
    if (IsUnderPostmaster && !allowSystemTableMods && IsSystemRelation(targetRel))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("permission denied: \"%s\" is a system catalog", RelationGetRelationName(targetRel))));
    }
}

/*
 * cannot reference system column in
 *   1. on clause
 *   2. action's qual
 * report error if we found any
 */
static void
check_system_column_reference(List *joinVarList, List *mergeActionList)
{
    ListCell *lc = NULL;

    /* check action's qual and target list */
    foreach (lc, mergeActionList)
    {
        MergeAction *action = (MergeAction *)lfirst(lc);

        check_system_column_node((Node *)action->qual);
        check_system_column_node((Node *)action->targetList);
    }

    /* check join on clause */
    check_system_column_varlist(joinVarList);
}

static void
check_system_column_node(Node *node)
{
    List *vars = pull_var_clause(node, PVC_RECURSE_AGGREGATES | PVC_RECURSE_PLACEHOLDERS);

    check_system_column_varlist(vars);
}

static void
check_system_column_varlist(List *varlist)
{
    ListCell *lc = NULL;

    foreach (lc, varlist)
    {
        Var *var = (Var *)lfirst(lc);

        if (var->varattno < 0)
        {
            ereport(
                ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("System Column reference are not yet supported for MERGE INTO")));
        }
    }
}

/*
 * CheckMergeTargetValid
 *
 * Check that a proposed result relation is a legal target for MERGE
 */
void
CheckMergeTargetValid(Relation targetRel)
{
	if (targetRel->rd_rel->relkind != RELKIND_RELATION &&
	    targetRel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
	    targetRel->rd_rel->relkind != RELKIND_VIEW)
		ereport(
			ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		     errmsg("cannot execute MERGE on relation \"%s\"", RelationGetRelationName(targetRel)),
		     errdetail_relkind_not_supported(targetRel->rd_rel->relkind)));
	if (targetRel->rd_rel->relhasrules && targetRel->rd_rel->relkind != RELKIND_VIEW)
		ereport(
			ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		     errmsg("cannot execute MERGE on relation \"%s\"", RelationGetRelationName(targetRel)),
		     errdetail("MERGE is not supported for relations with rules.")));
	if (targetRel->trigdesc && !IS_CENTRALIZED_DATANODE)
		ereport(
			ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		     errmsg("cannot execute MERGE on relation \"%s\"", RelationGetRelationName(targetRel)),
		     errhint("MERGE is not supported for relations with trigger or foreign key.")));

	/* replicate table is not yet supported */
	if (checkTargetTableReplicated(targetRel) && !IS_CENTRALIZED_MODE)
		ereport(
			ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		     errmsg("cannot execute MERGE on relation \"%s\"", RelationGetRelationName(targetRel)),
		     errhint("MERGE is not supported for replicate table.")));
	
	/* system catalog is not yet supported */
	checkTargetTableSystemCatalog(targetRel);

	/* Global index does not support partition table */
	if (relation_has_cross_node_index(targetRel))
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("Merge into relation with global index is not supported"),
		         errhint("Please drop global index")));
	}
}
