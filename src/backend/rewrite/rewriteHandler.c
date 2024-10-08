/*-------------------------------------------------------------------------
 *
 * rewriteHandler.c
 *        Primary module of query rewriter.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *      src/backend/rewrite/rewriteHandler.c
 *
 * NOTES
 *      Some of the terms used in this file are of historic nature: "retrieve"
 *      was the PostQUEL keyword for what today is SELECT. "RIR" stands for
 *      "Retrieve-Instead-Retrieve", that is an ON SELECT DO INSTEAD SELECT rule
 *      (which has to be unconditional and where only one rule can exist on each
 *      relation).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "foreign/fdwapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteDefine.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rowsecurity.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"

#ifdef PGXC
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#endif


/* We use a list of these to detect recursion in RewriteQuery */
typedef struct rewrite_event
{
    Oid            relation;        /* OID of relation having rules */
    CmdType        event;            /* type of rule being fired */
} rewrite_event;

typedef struct acquireLocksOnSubLinks_context
{
    bool        for_execute;    /* AcquireRewriteLocks' forExecute param */
} acquireLocksOnSubLinks_context;

static bool acquireLocksOnSubLinks(Node *node,
                       acquireLocksOnSubLinks_context *context);
static Query *rewriteRuleAction(Query *parsetree,
                  Query *rule_action,
                  Node *rule_qual,
                  int rt_index,
                  CmdType event,
                  bool *returning_flag);
static List *adjustJoinTreeList(Query *parsetree, bool removert, int rt_index);
static List *rewriteTargetListIU(List *targetList,
                    CmdType commandType,
                    OverridingKind override,
                    Relation target_relation,
                    int result_rti,
                    List **attrno_list);
static TargetEntry *process_matched_tle(TargetEntry *src_tle,
                    TargetEntry *prior_tle,
                    const char *attrName);
static Node *get_assignment_input(Node *node);
static void rewriteValuesRTE(RangeTblEntry *rte, Relation target_relation,
                 List *attrnos);
static void rewriteTargetListUD(Query *parsetree, RangeTblEntry *target_rte,
                    Relation target_relation);
static void markQueryForLocking(Query *qry, Node *jtnode,
                    LockClauseStrength strength, LockWaitPolicy waitPolicy,
                    bool pushedDown);
static List *matchLocks(CmdType event, RuleLock *rulelocks,
           int varno, Query *parsetree, bool *hasUpdate);
static Query *fireRIRrules(Query *parsetree, List *activeRIRs,
             bool forUpdatePushedDown);
static bool view_has_instead_trigger(Relation view, CmdType event);
static Bitmapset *adjust_view_column_set(Bitmapset *cols, List *targetlist);

/*
 * AcquireRewriteLocks -
 *      Acquire suitable locks on all the relations mentioned in the Query.
 *      These locks will ensure that the relation schemas don't change under us
 *      while we are rewriting and planning the query.
 *
 * forExecute indicates that the query is about to be executed.
 * If so, we'll acquire RowExclusiveLock on the query's resultRelation,
 * RowShareLock on any relation accessed FOR [KEY] UPDATE/SHARE, and
 * AccessShareLock on all other relations mentioned.
 *
 * If forExecute is false, AccessShareLock is acquired on all relations.
 * This case is suitable for ruleutils.c, for example, where we only need
 * schema stability and we don't intend to actually modify any relations.
 *
 * forUpdatePushedDown indicates that a pushed-down FOR [KEY] UPDATE/SHARE
 * applies to the current subquery, requiring all rels to be opened with at
 * least RowShareLock.  This should always be false at the top of the
 * recursion.  This flag is ignored if forExecute is false.
 *
 * A secondary purpose of this routine is to fix up JOIN RTE references to
 * dropped columns (see details below).  Because the RTEs are modified in
 * place, it is generally appropriate for the caller of this routine to have
 * first done a copyObject() to make a writable copy of the querytree in the
 * current memory context.
 *
 * This processing can, and for efficiency's sake should, be skipped when the
 * querytree has just been built by the parser: parse analysis already got
 * all the same locks we'd get here, and the parser will have omitted dropped
 * columns from JOINs to begin with.  But we must do this whenever we are
 * dealing with a querytree produced earlier than the current command.
 *
 * About JOINs and dropped columns: although the parser never includes an
 * already-dropped column in a JOIN RTE's alias var list, it is possible for
 * such a list in a stored rule to include references to dropped columns.
 * (If the column is not explicitly referenced anywhere else in the query,
 * the dependency mechanism won't consider it used by the rule and so won't
 * prevent the column drop.)  To support get_rte_attribute_is_dropped(), we
 * replace join alias vars that reference dropped columns with null pointers.
 *
 * (In PostgreSQL 8.0, we did not do this processing but instead had
 * get_rte_attribute_is_dropped() recurse to detect dropped columns in joins.
 * That approach had horrible performance unfortunately; in particular
 * construction of a nested join was O(N^2) in the nesting depth.)
 */
void
AcquireRewriteLocks(Query *parsetree,
                    bool forExecute,
                    bool forUpdatePushedDown)
{// #lizard forgives
    ListCell   *l;
    int            rt_index;
    acquireLocksOnSubLinks_context context;

    context.for_execute = forExecute;

    /*
     * First, process RTEs of the current query level.
     */
    rt_index = 0;
    foreach(l, parsetree->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
        Relation    rel;
        LOCKMODE    lockmode;
        List       *newaliasvars;
        Index        curinputvarno;
        RangeTblEntry *curinputrte;
        ListCell   *ll;

        ++rt_index;
        switch (rte->rtekind)
        {
            case RTE_RELATION:

                /*
                 * Grab the appropriate lock type for the relation, and do not
                 * release it until end of transaction. This protects the
                 * rewriter and planner against schema changes mid-query.
                 *
                 * Assuming forExecute is true, this logic must match what the
                 * executor will do, else we risk lock-upgrade deadlocks.
                 */
                if (!forExecute)
                    lockmode = AccessShareLock;
                else if (rt_index == parsetree->resultRelation)
                    lockmode = RowExclusiveLock;
                else if (forUpdatePushedDown ||
                         get_parse_rowmark(parsetree, rt_index) != NULL)
                    lockmode = RowShareLock;
                else
                    lockmode = AccessShareLock;

                rel = heap_open(rte->relid, lockmode);

                /*
                 * While we have the relation open, update the RTE's relkind,
                 * just in case it changed since this rule was made.
                 */
                rte->relkind = rel->rd_rel->relkind;

                heap_close(rel, NoLock);
                break;

            case RTE_JOIN:

                /*
                 * Scan the join's alias var list to see if any columns have
                 * been dropped, and if so replace those Vars with null
                 * pointers.
                 *
                 * Since a join has only two inputs, we can expect to see
                 * multiple references to the same input RTE; optimize away
                 * multiple fetches.
                 */
                newaliasvars = NIL;
                curinputvarno = 0;
                curinputrte = NULL;
                foreach(ll, rte->joinaliasvars)
                {
                    Var           *aliasitem = (Var *) lfirst(ll);
                    Var           *aliasvar = aliasitem;

                    /* Look through any implicit coercion */
                    aliasvar = (Var *) strip_implicit_coercions((Node *) aliasvar);

                    /*
                     * If the list item isn't a simple Var, then it must
                     * represent a merged column, ie a USING column, and so it
                     * couldn't possibly be dropped, since it's referenced in
                     * the join clause.  (Conceivably it could also be a null
                     * pointer already?  But that's OK too.)
                     */
                    if (aliasvar && IsA(aliasvar, Var))
                    {
                        /*
                         * The elements of an alias list have to refer to
                         * earlier RTEs of the same rtable, because that's the
                         * order the planner builds things in.  So we already
                         * processed the referenced RTE, and so it's safe to
                         * use get_rte_attribute_is_dropped on it. (This might
                         * not hold after rewriting or planning, but it's OK
                         * to assume here.)
                         */
                        Assert(aliasvar->varlevelsup == 0);
                        if (aliasvar->varno != curinputvarno)
                        {
                            curinputvarno = aliasvar->varno;
                            if (curinputvarno >= rt_index)
                                elog(ERROR, "unexpected varno %d in JOIN RTE %d",
                                     curinputvarno, rt_index);
                            curinputrte = rt_fetch(curinputvarno,
                                                   parsetree->rtable);
                        }
                        if (get_rte_attribute_is_dropped(curinputrte,
                                                         aliasvar->varattno))
                        {
                            /* Replace the join alias item with a NULL */
                            aliasitem = NULL;
                        }
                    }
                    newaliasvars = lappend(newaliasvars, aliasitem);
                }
                rte->joinaliasvars = newaliasvars;
                break;

            case RTE_SUBQUERY:

                /*
                 * The subquery RTE itself is all right, but we have to
                 * recurse to process the represented subquery.
                 */
                AcquireRewriteLocks(rte->subquery,
                                    forExecute,
                                    (forUpdatePushedDown ||
                                     get_parse_rowmark(parsetree, rt_index) != NULL));
                break;

            default:
                /* ignore other types of RTEs */
                break;
        }
    }

    /* Recurse into subqueries in WITH */
    foreach(l, parsetree->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);

        AcquireRewriteLocks((Query *) cte->ctequery, forExecute, false);
    }

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (parsetree->hasSubLinks)
        query_tree_walker(parsetree, acquireLocksOnSubLinks, &context,
                          QTW_IGNORE_RC_SUBQUERIES);
}

/*
 * Walker to find sublink subqueries for AcquireRewriteLocks
 */
static bool
acquireLocksOnSubLinks(Node *node, acquireLocksOnSubLinks_context *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, SubLink))
    {
        SubLink    *sub = (SubLink *) node;

        /* Do what we came for */
        AcquireRewriteLocks((Query *) sub->subselect,
                            context->for_execute,
                            false);
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because AcquireRewriteLocks already
     * processed subselects of subselects for us.
     */
    return expression_tree_walker(node, acquireLocksOnSubLinks, context);
}


/*
 * rewriteRuleAction -
 *      Rewrite the rule action with appropriate qualifiers (taken from
 *      the triggering query).
 *
 * Input arguments:
 *    parsetree - original query
 *    rule_action - one action (query) of a rule
 *    rule_qual - WHERE condition of rule, or NULL if unconditional
 *    rt_index - RT index of result relation in original query
 *    event - type of rule event
 * Output arguments:
 *    *returning_flag - set TRUE if we rewrite RETURNING clause in rule_action
 *                    (must be initialized to FALSE)
 * Return value:
 *    rewritten form of rule_action
 */
static Query *
rewriteRuleAction(Query *parsetree,
                  Query *rule_action,
                  Node *rule_qual,
                  int rt_index,
                  CmdType event,
                  bool *returning_flag)
{// #lizard forgives
    int            current_varno,
                new_varno;
    int            rt_length;
    Query       *sub_action;
    Query      **sub_action_ptr;
    acquireLocksOnSubLinks_context context;

    context.for_execute = true;

    /*
     * Make modifiable copies of rule action and qual (what we're passed are
     * the stored versions in the relcache; don't touch 'em!).
     */
    rule_action = copyObject(rule_action);
    rule_qual = copyObject(rule_qual);

    /*
     * Acquire necessary locks and fix any deleted JOIN RTE entries.
     */
    AcquireRewriteLocks(rule_action, true, false);
    (void) acquireLocksOnSubLinks(rule_qual, &context);

    current_varno = rt_index;
    rt_length = list_length(parsetree->rtable);
    new_varno = PRS2_NEW_VARNO + rt_length;

    /*
     * Adjust rule action and qual to offset its varnos, so that we can merge
     * its rtable with the main parsetree's rtable.
     *
     * If the rule action is an INSERT...SELECT, the OLD/NEW rtable entries
     * will be in the SELECT part, and we have to modify that rather than the
     * top-level INSERT (kluge!).
     */
    sub_action = getInsertSelectQuery(rule_action, &sub_action_ptr);

    OffsetVarNodes((Node *) sub_action, rt_length, 0);
    OffsetVarNodes(rule_qual, rt_length, 0);
    /* but references to OLD should point at original rt_index */
    ChangeVarNodes((Node *) sub_action,
                   PRS2_OLD_VARNO + rt_length, rt_index, 0);
    ChangeVarNodes(rule_qual,
                   PRS2_OLD_VARNO + rt_length, rt_index, 0);

    /*
     * Generate expanded rtable consisting of main parsetree's rtable plus
     * rule action's rtable; this becomes the complete rtable for the rule
     * action.  Some of the entries may be unused after we finish rewriting,
     * but we leave them all in place for two reasons:
     *
     * We'd have a much harder job to adjust the query's varnos if we
     * selectively removed RT entries.
     *
     * If the rule is INSTEAD, then the original query won't be executed at
     * all, and so its rtable must be preserved so that the executor will do
     * the correct permissions checks on it.
     *
     * RT entries that are not referenced in the completed jointree will be
     * ignored by the planner, so they do not affect query semantics.  But any
     * permissions checks specified in them will be applied during executor
     * startup (see ExecCheckRTEPerms()).  This allows us to check that the
     * caller has, say, insert-permission on a view, when the view is not
     * semantically referenced at all in the resulting query.
     *
     * When a rule is not INSTEAD, the permissions checks done on its copied
     * RT entries will be redundant with those done during execution of the
     * original query, but we don't bother to treat that case differently.
     *
     * NOTE: because planner will destructively alter rtable, we must ensure
     * that rule action's rtable is separate and shares no substructure with
     * the main rtable.  Hence do a deep copy here.
     */
    sub_action->rtable = list_concat(copyObject(parsetree->rtable),
                                     sub_action->rtable);

    /*
     * There could have been some SubLinks in parsetree's rtable, in which
     * case we'd better mark the sub_action correctly.
     */
    if (parsetree->hasSubLinks && !sub_action->hasSubLinks)
    {
        ListCell   *lc;

        foreach(lc, parsetree->rtable)
        {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

            switch (rte->rtekind)
            {
                case RTE_RELATION:
                    sub_action->hasSubLinks =
                        checkExprHasSubLink((Node *) rte->tablesample);
                    break;
                case RTE_FUNCTION:
                    sub_action->hasSubLinks =
                        checkExprHasSubLink((Node *) rte->functions);
                    break;
                case RTE_TABLEFUNC:
                    sub_action->hasSubLinks =
                        checkExprHasSubLink((Node *) rte->tablefunc);
                    break;
                case RTE_VALUES:
                    sub_action->hasSubLinks =
                        checkExprHasSubLink((Node *) rte->values_lists);
                    break;
                default:
                    /* other RTE types don't contain bare expressions */
                    break;
            }
            if (sub_action->hasSubLinks)
                break;            /* no need to keep scanning rtable */
        }
    }

    /*
     * Also, we might have absorbed some RTEs with RLS conditions into the
     * sub_action.  If so, mark it as hasRowSecurity, whether or not those
     * RTEs will be referenced after we finish rewriting.  (Note: currently
     * this is a no-op because RLS conditions aren't added till later, but it
     * seems like good future-proofing to do this anyway.)
     */
    sub_action->hasRowSecurity |= parsetree->hasRowSecurity;

    /*
     * Each rule action's jointree should be the main parsetree's jointree
     * plus that rule's jointree, but usually *without* the original rtindex
     * that we're replacing (if present, which it won't be for INSERT). Note
     * that if the rule action refers to OLD, its jointree will add a
     * reference to rt_index.  If the rule action doesn't refer to OLD, but
     * either the rule_qual or the user query quals do, then we need to keep
     * the original rtindex in the jointree to provide data for the quals.  We
     * don't want the original rtindex to be joined twice, however, so avoid
     * keeping it if the rule action mentions it.
     *
     * As above, the action's jointree must not share substructure with the
     * main parsetree's.
     */
    if (sub_action->commandType != CMD_UTILITY)
    {
        bool        keeporig;
        List       *newjointree;

        Assert(sub_action->jointree != NULL);
        keeporig = (!rangeTableEntry_used((Node *) sub_action->jointree,
                                          rt_index, 0)) &&
            (rangeTableEntry_used(rule_qual, rt_index, 0) ||
             rangeTableEntry_used(parsetree->jointree->quals, rt_index, 0));
        newjointree = adjustJoinTreeList(parsetree, !keeporig, rt_index);
        if (newjointree != NIL)
        {
            /*
             * If sub_action is a setop, manipulating its jointree will do no
             * good at all, because the jointree is dummy.  (Perhaps someday
             * we could push the joining and quals down to the member
             * statements of the setop?)
             */
            if (sub_action->setOperations != NULL)
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));

            sub_action->jointree->fromlist =
                list_concat(newjointree, sub_action->jointree->fromlist);

            /*
             * There could have been some SubLinks in newjointree, in which
             * case we'd better mark the sub_action correctly.
             */
            if (parsetree->hasSubLinks && !sub_action->hasSubLinks)
                sub_action->hasSubLinks =
                    checkExprHasSubLink((Node *) newjointree);
        }
    }

    /*
     * If the original query has any CTEs, copy them into the rule action. But
     * we don't need them for a utility action.
     */
    if (parsetree->cteList != NIL && sub_action->commandType != CMD_UTILITY)
    {
        ListCell   *lc;

        /*
         * Annoying implementation restriction: because CTEs are identified by
         * name within a cteList, we can't merge a CTE from the original query
         * if it has the same name as any CTE in the rule action.
         *
         * This could possibly be fixed by using some sort of internally
         * generated ID, instead of names, to link CTE RTEs to their CTEs.
         */
        foreach(lc, parsetree->cteList)
        {
            CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
            ListCell   *lc2;

            foreach(lc2, sub_action->cteList)
            {
                CommonTableExpr *cte2 = (CommonTableExpr *) lfirst(lc2);

                if (strcmp(cte->ctename, cte2->ctename) == 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("WITH query name \"%s\" appears in both a rule action and the query being rewritten",
                                    cte->ctename)));
            }
        }

        /* OK, it's safe to combine the CTE lists */
        sub_action->cteList = list_concat(sub_action->cteList,
                                          copyObject(parsetree->cteList));
    }

    /*
     * Event Qualification forces copying of parsetree and splitting into two
     * queries one w/rule_qual, one w/NOT rule_qual. Also add user query qual
     * onto rule action
     */
    AddQual(sub_action, rule_qual);

    AddQual(sub_action, parsetree->jointree->quals);

    /*
     * Rewrite new.attribute with right hand side of target-list entry for
     * appropriate field name in insert/update.
     *
     * KLUGE ALERT: since ReplaceVarsFromTargetList returns a mutated copy, we
     * can't just apply it to sub_action; we have to remember to update the
     * sublink inside rule_action, too.
     */
    if ((event == CMD_INSERT || event == CMD_UPDATE) &&
        sub_action->commandType != CMD_UTILITY)
    {
        sub_action = (Query *)
            ReplaceVarsFromTargetList((Node *) sub_action,
                                      new_varno,
                                      0,
                                      rt_fetch(new_varno, sub_action->rtable),
                                      parsetree->targetList,
                                      (event == CMD_UPDATE) ?
                                      REPLACEVARS_CHANGE_VARNO :
                                      REPLACEVARS_SUBSTITUTE_NULL,
                                      current_varno,
                                      NULL);
        if (sub_action_ptr)
            *sub_action_ptr = sub_action;
        else
            rule_action = sub_action;
    }

    /*
     * If rule_action has a RETURNING clause, then either throw it away if the
     * triggering query has no RETURNING clause, or rewrite it to emit what
     * the triggering query's RETURNING clause asks for.  Throw an error if
     * more than one rule has a RETURNING clause.
     */
    if (!parsetree->returningList)
        rule_action->returningList = NIL;
    else if (rule_action->returningList)
    {
        if (*returning_flag)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("cannot have RETURNING lists in multiple rules")));
        *returning_flag = true;
        rule_action->returningList = (List *)
            ReplaceVarsFromTargetList((Node *) parsetree->returningList,
                                      parsetree->resultRelation,
                                      0,
                                      rt_fetch(parsetree->resultRelation,
                                               parsetree->rtable),
                                      rule_action->returningList,
                                      REPLACEVARS_REPORT_ERROR,
                                      0,
                                      &rule_action->hasSubLinks);

        /*
         * There could have been some SubLinks in parsetree's returningList,
         * in which case we'd better mark the rule_action correctly.
         */
        if (parsetree->hasSubLinks && !rule_action->hasSubLinks)
            rule_action->hasSubLinks =
                checkExprHasSubLink((Node *) rule_action->returningList);
    }

    return rule_action;
}

/*
 * Copy the query's jointree list, and optionally attempt to remove any
 * occurrence of the given rt_index as a top-level join item (we do not look
 * for it within join items; this is OK because we are only expecting to find
 * it as an UPDATE or DELETE target relation, which will be at the top level
 * of the join).  Returns modified jointree list --- this is a separate copy
 * sharing no nodes with the original.
 */
static List *
adjustJoinTreeList(Query *parsetree, bool removert, int rt_index)
{
    List       *newjointree = copyObject(parsetree->jointree->fromlist);
    ListCell   *l;

    if (removert)
    {
        foreach(l, newjointree)
        {
            RangeTblRef *rtr = lfirst(l);

            if (IsA(rtr, RangeTblRef) &&
                rtr->rtindex == rt_index)
            {
                newjointree = list_delete_ptr(newjointree, rtr);

                /*
                 * foreach is safe because we exit loop after list_delete...
                 */
                break;
            }
        }
    }
    return newjointree;
}


/*
 * rewriteTargetListIU - rewrite INSERT/UPDATE targetlist into standard form
 *
 * This has the following responsibilities:
 *
 * 1. For an INSERT, add tlist entries to compute default values for any
 * attributes that have defaults and are not assigned to in the given tlist.
 * (We do not insert anything for default-less attributes, however.  The
 * planner will later insert NULLs for them, but there's no reason to slow
 * down rewriter processing with extra tlist nodes.)  Also, for both INSERT
 * and UPDATE, replace explicit DEFAULT specifications with column default
 * expressions.
 *
 * 2. For an UPDATE on a trigger-updatable view, add tlist entries for any
 * unassigned-to attributes, assigning them their old values.  These will
 * later get expanded to the output values of the view.  (This is equivalent
 * to what the planner's expand_targetlist() will do for UPDATE on a regular
 * table, but it's more convenient to do it here while we still have easy
 * access to the view's original RT index.)  This is only necessary for
 * trigger-updatable views, for which the view remains the result relation of
 * the query.  For auto-updatable views we must not do this, since it might
 * add assignments to non-updatable view columns.  For rule-updatable views it
 * is unnecessary extra work, since the query will be rewritten with a
 * different result relation which will be processed when we recurse via
 * RewriteQuery.
 *
 * 3. Merge multiple entries for the same target attribute, or declare error
 * if we can't.  Multiple entries are only allowed for INSERT/UPDATE of
 * portions of an array or record field, for example
 *            UPDATE table SET foo[2] = 42, foo[4] = 43;
 * We can merge such operations into a single assignment op.  Essentially,
 * the expression we want to produce in this case is like
 *        foo = array_set_element(array_set_element(foo, 2, 42), 4, 43)
 *
 * 4. Sort the tlist into standard order: non-junk fields in order by resno,
 * then junk fields (these in no particular order).
 *
 * We must do items 1,2,3 before firing rewrite rules, else rewritten
 * references to NEW.foo will produce wrong or incomplete results.  Item 4
 * is not needed for rewriting, but will be needed by the planner, and we
 * can do it essentially for free while handling the other items.
 *
 * If attrno_list isn't NULL, we return an additional output besides the
 * rewritten targetlist: an integer list of the assigned-to attnums, in
 * order of the original tlist's non-junk entries.  This is needed for
 * processing VALUES RTEs.
 */
static List *
rewriteTargetListIU(List *targetList,
                    CmdType commandType,
                    OverridingKind override,
                    Relation target_relation,
                    int result_rti,
                    List **attrno_list)
{// #lizard forgives
    TargetEntry **new_tles;
    List       *new_tlist = NIL;
    List       *junk_tlist = NIL;
    Form_pg_attribute att_tup;
    int            attrno,
                next_junk_attrno,
                numattrs;
    ListCell   *temp;

    if (attrno_list)            /* initialize optional result list */
        *attrno_list = NIL;

    /*
     * We process the normal (non-junk) attributes by scanning the input tlist
     * once and transferring TLEs into an array, then scanning the array to
     * build an output tlist.  This avoids O(N^2) behavior for large numbers
     * of attributes.
     *
     * Junk attributes are tossed into a separate list during the same tlist
     * scan, then appended to the reconstructed tlist.
     */
    numattrs = RelationGetNumberOfAttributes(target_relation);
    new_tles = (TargetEntry **) palloc0(numattrs * sizeof(TargetEntry *));
    next_junk_attrno = numattrs + 1;

    foreach(temp, targetList)
    {
        TargetEntry *old_tle = (TargetEntry *) lfirst(temp);

        if (!old_tle->resjunk)
        {
            /* Normal attr: stash it into new_tles[] */
            attrno = old_tle->resno;
            if (attrno < 1 || attrno > numattrs)
                elog(ERROR, "bogus resno %d in targetlist", attrno);
            att_tup = target_relation->rd_att->attrs[attrno - 1];

            /* put attrno into attrno_list even if it's dropped */
            if (attrno_list)
                *attrno_list = lappend_int(*attrno_list, attrno);

            /* We can (and must) ignore deleted attributes */
            if (att_tup->attisdropped)
                continue;

            /* Merge with any prior assignment to same attribute */
            new_tles[attrno - 1] =
                process_matched_tle(old_tle,
                                    new_tles[attrno - 1],
                                    NameStr(att_tup->attname));
        }
        else
        {
            /*
             * Copy all resjunk tlist entries to junk_tlist, and assign them
             * resnos above the last real resno.
             *
             * Typical junk entries include ORDER BY or GROUP BY expressions
             * (are these actually possible in an INSERT or UPDATE?), system
             * attribute references, etc.
             */

            /* Get the resno right, but don't copy unnecessarily */
            if (old_tle->resno != next_junk_attrno)
            {
                old_tle = flatCopyTargetEntry(old_tle);
                old_tle->resno = next_junk_attrno;
            }
            junk_tlist = lappend(junk_tlist, old_tle);
            next_junk_attrno++;
        }
    }

    for (attrno = 1; attrno <= numattrs; attrno++)
    {
        TargetEntry *new_tle = new_tles[attrno - 1];
        bool        apply_default;

        att_tup = target_relation->rd_att->attrs[attrno - 1];

        /* We can (and must) ignore deleted attributes */
        if (att_tup->attisdropped)
            continue;

        /*
         * Handle the two cases where we need to insert a default expression:
         * it's an INSERT and there's no tlist entry for the column, or the
         * tlist entry is a DEFAULT placeholder node.
         */
        apply_default = ((new_tle == NULL && commandType == CMD_INSERT) ||
                         (new_tle && new_tle->expr && IsA(new_tle->expr, SetToDefault)));

        if (commandType == CMD_INSERT)
        {
            if (att_tup->attidentity == ATTRIBUTE_IDENTITY_ALWAYS && !apply_default)
            {
                if (override != OVERRIDING_SYSTEM_VALUE)
                    ereport(ERROR,
                            (errcode(ERRCODE_GENERATED_ALWAYS),
                             errmsg("cannot insert into column \"%s\"", NameStr(att_tup->attname)),
                             errdetail("Column \"%s\" is an identity column defined as GENERATED ALWAYS.",
                                       NameStr(att_tup->attname)),
                             errhint("Use OVERRIDING SYSTEM VALUE to override.")));
            }

            if (att_tup->attidentity == ATTRIBUTE_IDENTITY_BY_DEFAULT && override == OVERRIDING_USER_VALUE)
                apply_default = true;
        }

        if (commandType == CMD_UPDATE)
        {
            if (att_tup->attidentity == ATTRIBUTE_IDENTITY_ALWAYS && new_tle && !apply_default)
                ereport(ERROR,
                        (errcode(ERRCODE_GENERATED_ALWAYS),
                         errmsg("column \"%s\" can only be updated to DEFAULT", NameStr(att_tup->attname)),
                         errdetail("Column \"%s\" is an identity column defined as GENERATED ALWAYS.",
                                   NameStr(att_tup->attname))));
        }

        if (apply_default)
        {
            Node       *new_expr;

            if (att_tup->attidentity)
            {
                NextValueExpr *nve = makeNode(NextValueExpr);

                nve->seqid = getOwnedSequence(RelationGetRelid(target_relation), attrno);
                nve->typeId = att_tup->atttypid;

                new_expr = (Node *) nve;
            }
            else
                new_expr = build_column_default(target_relation, attrno);

            /*
             * If there is no default (ie, default is effectively NULL), we
             * can omit the tlist entry in the INSERT case, since the planner
             * can insert a NULL for itself, and there's no point in spending
             * any more rewriter cycles on the entry.  But in the UPDATE case
             * we've got to explicitly set the column to NULL.
             */
            if (!new_expr)
            {
                if (commandType == CMD_INSERT)
                    new_tle = NULL;
                else
                {
                    new_expr = (Node *) makeConst(att_tup->atttypid,
                                                  -1,
                                                  att_tup->attcollation,
                                                  att_tup->attlen,
                                                  (Datum) 0,
                                                  true, /* isnull */
                                                  att_tup->attbyval);
                    /* this is to catch a NOT NULL domain constraint */
                    new_expr = coerce_to_domain(new_expr,
                                                InvalidOid, -1,
                                                att_tup->atttypid,
                                                COERCE_IMPLICIT_CAST,
                                                -1,
                                                false,
                                                false);
                }
            }

            if (new_expr)
                new_tle = makeTargetEntry((Expr *) new_expr,
                                          attrno,
                                          pstrdup(NameStr(att_tup->attname)),
                                          false);
        }

        /*
         * For an UPDATE on a trigger-updatable view, provide a dummy entry
         * whenever there is no explicit assignment.
         */
        if (new_tle == NULL && commandType == CMD_UPDATE &&
            target_relation->rd_rel->relkind == RELKIND_VIEW &&
            view_has_instead_trigger(target_relation, CMD_UPDATE))
        {
            Node       *new_expr;

            new_expr = (Node *) makeVar(result_rti,
                                        attrno,
                                        att_tup->atttypid,
                                        att_tup->atttypmod,
                                        att_tup->attcollation,
                                        0);

            new_tle = makeTargetEntry((Expr *) new_expr,
                                      attrno,
                                      pstrdup(NameStr(att_tup->attname)),
                                      false);
        }

        if (new_tle)
            new_tlist = lappend(new_tlist, new_tle);
    }

    pfree(new_tles);

    return list_concat(new_tlist, junk_tlist);
}


/*
 * Convert a matched TLE from the original tlist into a correct new TLE.
 *
 * This routine detects and handles multiple assignments to the same target
 * attribute.  (The attribute name is needed only for error messages.)
 */
static TargetEntry *
process_matched_tle(TargetEntry *src_tle,
                    TargetEntry *prior_tle,
                    const char *attrName)
{// #lizard forgives
    TargetEntry *result;
    CoerceToDomain *coerce_expr = NULL;
    Node       *src_expr;
    Node       *prior_expr;
    Node       *src_input;
    Node       *prior_input;
    Node       *priorbottom;
    Node       *newexpr;

    if (prior_tle == NULL)
    {
        /*
         * Normal case where this is the first assignment to the attribute.
         */
        return src_tle;
    }

    /*----------
     * Multiple assignments to same attribute.  Allow only if all are
     * FieldStore or ArrayRef assignment operations.  This is a bit
     * tricky because what we may actually be looking at is a nest of
     * such nodes; consider
     *        UPDATE tab SET col.fld1.subfld1 = x, col.fld2.subfld2 = y
     * The two expressions produced by the parser will look like
     *        FieldStore(col, fld1, FieldStore(placeholder, subfld1, x))
     *        FieldStore(col, fld2, FieldStore(placeholder, subfld2, y))
     * However, we can ignore the substructure and just consider the top
     * FieldStore or ArrayRef from each assignment, because it works to
     * combine these as
     *        FieldStore(FieldStore(col, fld1,
     *                              FieldStore(placeholder, subfld1, x)),
     *                   fld2, FieldStore(placeholder, subfld2, y))
     * Note the leftmost expression goes on the inside so that the
     * assignments appear to occur left-to-right.
     *
     * For FieldStore, instead of nesting we can generate a single
     * FieldStore with multiple target fields.  We must nest when
     * ArrayRefs are involved though.
     *
     * As a further complication, the destination column might be a domain,
     * resulting in each assignment containing a CoerceToDomain node over a
     * FieldStore or ArrayRef.  These should have matching target domains,
     * so we strip them and reconstitute a single CoerceToDomain over the
     * combined FieldStore/ArrayRef nodes.  (Notice that this has the result
     * that the domain's checks are applied only after we do all the field or
     * element updates, not after each one.  This is arguably desirable.)
     *----------
     */
    src_expr = (Node *) src_tle->expr;
    prior_expr = (Node *) prior_tle->expr;

    if (src_expr && IsA(src_expr, CoerceToDomain) &&
        prior_expr && IsA(prior_expr, CoerceToDomain) &&
        ((CoerceToDomain *) src_expr)->resulttype ==
        ((CoerceToDomain *) prior_expr)->resulttype)
    {
        /* we assume without checking that resulttypmod/resultcollid match */
        coerce_expr = (CoerceToDomain *) src_expr;
        src_expr = (Node *) ((CoerceToDomain *) src_expr)->arg;
        prior_expr = (Node *) ((CoerceToDomain *) prior_expr)->arg;
    }

    src_input = get_assignment_input(src_expr);
    prior_input = get_assignment_input(prior_expr);
    if (src_input == NULL ||
        prior_input == NULL ||
        exprType(src_expr) != exprType(prior_expr))
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("multiple assignments to same column \"%s\"",
                        attrName)));

    /*
     * Prior TLE could be a nest of assignments if we do this more than once.
     */
    priorbottom = prior_input;
    for (;;)
    {
        Node       *newbottom = get_assignment_input(priorbottom);

        if (newbottom == NULL)
            break;                /* found the original Var reference */
        priorbottom = newbottom;
    }
    if (!equal(priorbottom, src_input))
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("multiple assignments to same column \"%s\"",
                        attrName)));

    /*
     * Looks OK to nest 'em.
     */
    if (IsA(src_expr, FieldStore))
    {
        FieldStore *fstore = makeNode(FieldStore);

        if (IsA(prior_expr, FieldStore))
        {
            /* combine the two */
            memcpy(fstore, prior_expr, sizeof(FieldStore));
            fstore->newvals =
                list_concat(list_copy(((FieldStore *) prior_expr)->newvals),
                            list_copy(((FieldStore *) src_expr)->newvals));
            fstore->fieldnums =
                list_concat(list_copy(((FieldStore *) prior_expr)->fieldnums),
                            list_copy(((FieldStore *) src_expr)->fieldnums));
        }
        else
        {
            /* general case, just nest 'em */
            memcpy(fstore, src_expr, sizeof(FieldStore));
            fstore->arg = (Expr *) prior_expr;
        }
        newexpr = (Node *) fstore;
    }
    else if (IsA(src_expr, ArrayRef))
    {
        ArrayRef   *aref = makeNode(ArrayRef);

        memcpy(aref, src_expr, sizeof(ArrayRef));
        aref->refexpr = (Expr *) prior_expr;
        newexpr = (Node *) aref;
    }
    else
    {
        elog(ERROR, "cannot happen");
        newexpr = NULL;
    }

    if (coerce_expr)
    {
        /* put back the CoerceToDomain */
        CoerceToDomain *newcoerce = makeNode(CoerceToDomain);

        memcpy(newcoerce, coerce_expr, sizeof(CoerceToDomain));
        newcoerce->arg = (Expr *) newexpr;
        newexpr = (Node *) newcoerce;
    }

    result = flatCopyTargetEntry(src_tle);
    result->expr = (Expr *) newexpr;
    return result;
}

/*
 * If node is an assignment node, return its input; else return NULL
 */
static Node *
get_assignment_input(Node *node)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, FieldStore))
    {
        FieldStore *fstore = (FieldStore *) node;

        return (Node *) fstore->arg;
    }
    else if (IsA(node, ArrayRef))
    {
        ArrayRef   *aref = (ArrayRef *) node;

        if (aref->refassgnexpr == NULL)
            return NULL;
        return (Node *) aref->refexpr;
    }
    return NULL;
}

/*
 * Make an expression tree for the default value for a column.
 *
 * If there is no default, return a NULL instead.
 */
Node *
build_column_default(Relation rel, int attrno)
{// #lizard forgives
    TupleDesc    rd_att = rel->rd_att;
    Form_pg_attribute att_tup = rd_att->attrs[attrno - 1];
    Oid            atttype = att_tup->atttypid;
    int32        atttypmod = att_tup->atttypmod;
    Node       *expr = NULL;
    Oid            exprtype;

    /*
     * Scan to see if relation has a default for this column.
     */
#ifdef _MLS_
    if (att_tup->atthasdef && rd_att->constr &&
        rd_att->constr->num_defval > 0)
#endif        
    {
        AttrDefault *defval = rd_att->constr->defval;
        int            ndef = rd_att->constr->num_defval;

        while (--ndef >= 0)
        {
            if (attrno == defval[ndef].adnum)
            {
                /*
                 * Found it, convert string representation to node tree.
                 */
                expr = stringToNode(defval[ndef].adbin);
                break;
            }
        }
    }

    if (expr == NULL)
    {
        /*
         * No per-column default, so look for a default for the type itself.
         */
        expr = get_typdefault(atttype);
    }

    if (expr == NULL)
        return NULL;            /* No default anywhere */

    /*
     * Make sure the value is coerced to the target column type; this will
     * generally be true already, but there seem to be some corner cases
     * involving domain defaults where it might not be true. This should match
     * the parser's processing of non-defaulted expressions --- see
     * transformAssignedExpr().
     */
    exprtype = exprType(expr);

    expr = coerce_to_target_type(NULL,    /* no UNKNOWN params here */
                                 expr, exprtype,
                                 atttype, atttypmod,
                                 COERCION_ASSIGNMENT,
                                 COERCE_IMPLICIT_CAST,
                                 -1);
    if (expr == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("column \"%s\" is of type %s"
                        " but default expression is of type %s",
                        NameStr(att_tup->attname),
                        format_type_be(atttype),
                        format_type_be(exprtype)),
                 errhint("You will need to rewrite or cast the expression.")));

    return expr;
}


/* Does VALUES RTE contain any SetToDefault items? */
static bool
searchForDefault(RangeTblEntry *rte)
{
    ListCell   *lc;

    foreach(lc, rte->values_lists)
    {
        List       *sublist = (List *) lfirst(lc);
        ListCell   *lc2;

        foreach(lc2, sublist)
        {
            Node       *col = (Node *) lfirst(lc2);

            if (IsA(col, SetToDefault))
                return true;
        }
    }
    return false;
}

/*
 * When processing INSERT ... VALUES with a VALUES RTE (ie, multiple VALUES
 * lists), we have to replace any DEFAULT items in the VALUES lists with
 * the appropriate default expressions.  The other aspects of targetlist
 * rewriting need be applied only to the query's targetlist proper.
 *
 * Note that we currently can't support subscripted or field assignment
 * in the multi-VALUES case.  The targetlist will contain simple Vars
 * referencing the VALUES RTE, and therefore process_matched_tle() will
 * reject any such attempt with "multiple assignments to same column".
 */
static void
rewriteValuesRTE(RangeTblEntry *rte, Relation target_relation, List *attrnos)
{
    List       *newValues;
    ListCell   *lc;

    /*
     * Rebuilding all the lists is a pretty expensive proposition in a big
     * VALUES list, and it's a waste of time if there aren't any DEFAULT
     * placeholders.  So first scan to see if there are any.
     */
    if (!searchForDefault(rte))
        return;                    /* nothing to do */

    /* Check list lengths (we can assume all the VALUES sublists are alike) */
    Assert(list_length(attrnos) == list_length(linitial(rte->values_lists)));

    newValues = NIL;
    foreach(lc, rte->values_lists)
    {
        List       *sublist = (List *) lfirst(lc);
        List       *newList = NIL;
        ListCell   *lc2;
        ListCell   *lc3;

        forboth(lc2, sublist, lc3, attrnos)
        {
            Node       *col = (Node *) lfirst(lc2);
            int            attrno = lfirst_int(lc3);

            if (IsA(col, SetToDefault))
            {
                Form_pg_attribute att_tup;
                Node       *new_expr;

                att_tup = target_relation->rd_att->attrs[attrno - 1];

                if (!att_tup->attisdropped)
                    new_expr = build_column_default(target_relation, attrno);
                else
                    new_expr = NULL;    /* force a NULL if dropped */

                /*
                 * If there is no default (ie, default is effectively NULL),
                 * we've got to explicitly set the column to NULL.
                 */
                if (!new_expr)
                {
                    new_expr = (Node *) makeConst(att_tup->atttypid,
                                                  -1,
                                                  att_tup->attcollation,
                                                  att_tup->attlen,
                                                  (Datum) 0,
                                                  true, /* isnull */
                                                  att_tup->attbyval);
                    /* this is to catch a NOT NULL domain constraint */
                    new_expr = coerce_to_domain(new_expr,
                                                InvalidOid, -1,
                                                att_tup->atttypid,
                                                COERCE_IMPLICIT_CAST,
                                                -1,
                                                false,
                                                false);
                }
                newList = lappend(newList, new_expr);
            }
            else
                newList = lappend(newList, col);
        }
        newValues = lappend(newValues, newList);
    }
    rte->values_lists = newValues;
}

/*
 * rewriteTargetListUD - rewrite UPDATE/DELETE targetlist as needed
 *
 * This function adds a "junk" TLE that is needed to allow the executor to
 * find the original row for the update or delete.  When the target relation
 * is a regular table, the junk TLE emits the ctid attribute of the original
 * row.  When the target relation is a view, there is no ctid, so we instead
 * emit a whole-row Var that will contain the "old" values of the view row.
 * If it's a foreign table, we let the FDW decide what to add.
 *
 * For UPDATE queries, this is applied after rewriteTargetListIU.  The
 * ordering isn't actually critical at the moment.
 */
static void
rewriteTargetListUD(Query *parsetree, RangeTblEntry *target_rte,
                    Relation target_relation)
{// #lizard forgives
    Var           *var = NULL;
    const char *attrname;
    TargetEntry *tle;

#ifdef PGXC
    /*
     * If relation is non-replicated, we need also to identify the Datanode
     * from where tuple is fetched.
     */
    if (IS_PGXC_COORDINATOR &&
        !IsConnFromCoord() &&
        !IsLocatorReplicated(GetRelationLocType(RelationGetRelid(target_relation))) &&
        (target_relation->rd_rel->relkind == RELKIND_RELATION ||
         target_relation->rd_rel->relkind == RELKIND_MATVIEW))
    {
        var = makeVar(parsetree->resultRelation,
                      XC_NodeIdAttributeNumber,
                      INT4OID,
                      -1,
                      InvalidOid,
                      0);

        attrname = "xc_node_id";

        tle = makeTargetEntry((Expr *) var,
                              list_length(parsetree->targetList) + 1,
                              pstrdup(attrname),
                              true);

        parsetree->targetList = lappend(parsetree->targetList, tle);
    }
#endif

    if (target_relation->rd_rel->relkind == RELKIND_RELATION ||
        target_relation->rd_rel->relkind == RELKIND_MATVIEW ||
        target_relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
    {
        /*
         * Emit CTID so that executor can find the row to update or delete.
         */
        var = makeVar(parsetree->resultRelation,
                      SelfItemPointerAttributeNumber,
                      TIDOID,
                      -1,
                      InvalidOid,
                      0);

        attrname = "ctid";
    }
    else if (target_relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
    {
        /*
         * Let the foreign table's FDW add whatever junk TLEs it wants.
         */
        FdwRoutine *fdwroutine;

        fdwroutine = GetFdwRoutineForRelation(target_relation, false);

        if (fdwroutine->AddForeignUpdateTargets != NULL)
            fdwroutine->AddForeignUpdateTargets(parsetree, target_rte,
                                                target_relation);

        /*
         * If we have a row-level trigger corresponding to the operation, emit
         * a whole-row Var so that executor will have the "old" row to pass to
         * the trigger.  Alas, this misses system columns.
         */
        if (target_relation->trigdesc &&
            ((parsetree->commandType == CMD_UPDATE &&
              (target_relation->trigdesc->trig_update_after_row ||
               target_relation->trigdesc->trig_update_before_row)) ||
             (parsetree->commandType == CMD_DELETE &&
              (target_relation->trigdesc->trig_delete_after_row ||
               target_relation->trigdesc->trig_delete_before_row))))
        {
            var = makeWholeRowVar(target_rte,
                                  parsetree->resultRelation,
                                  0,
                                  false);

            attrname = "wholerow";
        }
    }
    else
    {
        /*
         * Emit whole-row Var so that executor will have the "old" view row to
         * pass to the INSTEAD OF trigger.
         */
        var = makeWholeRowVar(target_rte,
                              parsetree->resultRelation,
                              0,
                              false);

        attrname = "wholerow";
    }

    if (var != NULL)
    {
        tle = makeTargetEntry((Expr *) var,
                              list_length(parsetree->targetList) + 1,
                              pstrdup(attrname),
                              true);
        parsetree->targetList = lappend(parsetree->targetList, tle);
    }

#ifdef _MIGRATE_
    if(target_relation->rd_rel->relkind == RELKIND_RELATION)
    {
        var = makeVar(parsetree->resultRelation,
                ShardIdAttributeNumber,
                INT4OID,
                -1,
                InvalidOid,
                0);
        tle = makeTargetEntry((Expr *) var,
                              list_length(parsetree->targetList) + 1,
                              pstrdup("shardid"),
                              true);
        parsetree->targetList = lappend(parsetree->targetList, tle);
    }
#endif

#ifdef __OPENTENBASE__
    if(target_relation->rd_rel->relkind == RELKIND_RELATION &&
        IS_PGXC_COORDINATOR)
    {
        /*
         * If we have a row-level trigger corresponding to the operation, emit
         * a whole-row Var so that executor will have the "old" row to pass to
         * the trigger. 
         */
        if (target_relation->trigdesc &&
            ((parsetree->commandType == CMD_UPDATE &&
              (target_relation->trigdesc->trig_update_after_row ||
               target_relation->trigdesc->trig_update_before_row ||
               target_relation->trigdesc->trig_update_after_statement ||
               target_relation->trigdesc->trig_update_before_statement)) ||
             (parsetree->commandType == CMD_DELETE &&
              (target_relation->trigdesc->trig_delete_after_row ||
               target_relation->trigdesc->trig_delete_before_row ||
               target_relation->trigdesc->trig_delete_after_statement ||
               target_relation->trigdesc->trig_delete_before_statement))))
        {
            var = makeWholeRowVar(target_rte,
                                  parsetree->resultRelation,
                                  0,
                                  false);

            attrname = "wholerow";

            tle = makeTargetEntry((Expr *) var,
                          list_length(parsetree->targetList) + 1,
                          pstrdup(attrname),
                          true);
            parsetree->targetList = lappend(parsetree->targetList, tle);
        }
    }
#endif
}


/*
 * matchLocks -
 *      match the list of locks and returns the matching rules
 */
static List *
matchLocks(CmdType event,
           RuleLock *rulelocks,
           int varno,
           Query *parsetree,
           bool *hasUpdate)
{// #lizard forgives
    List       *matching_locks = NIL;
    int            nlocks;
    int            i;

    if (rulelocks == NULL)
        return NIL;

    if (parsetree->commandType != CMD_SELECT)
    {
        if (parsetree->resultRelation != varno)
            return NIL;
    }

    nlocks = rulelocks->numLocks;

    for (i = 0; i < nlocks; i++)
    {
        RewriteRule *oneLock = rulelocks->rules[i];

        if (oneLock->event == CMD_UPDATE)
            *hasUpdate = true;

        /*
         * Suppress ON INSERT/UPDATE/DELETE rules that are disabled or
         * configured to not fire during the current sessions replication
         * role. ON SELECT rules will always be applied in order to keep views
         * working even in LOCAL or REPLICA role.
         */
        if (oneLock->event != CMD_SELECT)
        {
            if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA)
            {
                if (oneLock->enabled == RULE_FIRES_ON_ORIGIN ||
                    oneLock->enabled == RULE_DISABLED)
                    continue;
            }
            else                /* ORIGIN or LOCAL ROLE */
            {
                if (oneLock->enabled == RULE_FIRES_ON_REPLICA ||
                    oneLock->enabled == RULE_DISABLED)
                    continue;
            }
        }

        if (oneLock->event == event)
        {
            if (parsetree->commandType != CMD_SELECT ||
                rangeTableEntry_used((Node *) parsetree, varno, 0))
                matching_locks = lappend(matching_locks, oneLock);
        }
    }

    return matching_locks;
}


/*
 * ApplyRetrieveRule - expand an ON SELECT rule
 */
static Query *
ApplyRetrieveRule(Query *parsetree,
                  RewriteRule *rule,
                  int rt_index,
                  Relation relation,
                  List *activeRIRs,
                  bool forUpdatePushedDown)
{// #lizard forgives
    Query       *rule_action;
    RangeTblEntry *rte,
               *subrte;
    RowMarkClause *rc;

    if (list_length(rule->actions) != 1)
        elog(ERROR, "expected just one rule action");
    if (rule->qual != NULL)
        elog(ERROR, "cannot handle qualified ON SELECT rule");

    if (rt_index == parsetree->resultRelation)
    {
        /*
         * We have a view as the result relation of the query, and it wasn't
         * rewritten by any rule.  This case is supported if there is an
         * INSTEAD OF trigger that will trap attempts to insert/update/delete
         * view rows.  The executor will check that; for the moment just plow
         * ahead.  We have two cases:
         *
         * For INSERT, we needn't do anything.  The unmodified RTE will serve
         * fine as the result relation.
         *
         * For UPDATE/DELETE, we need to expand the view so as to have source
         * data for the operation.  But we also need an unmodified RTE to
         * serve as the target.  So, copy the RTE and add the copy to the
         * rangetable.  Note that the copy does not get added to the jointree.
         * Also note that there's a hack in fireRIRrules to avoid calling this
         * function again when it arrives at the copied RTE.
         */
        if (parsetree->commandType == CMD_INSERT)
            return parsetree;
        else if (parsetree->commandType == CMD_UPDATE ||
                 parsetree->commandType == CMD_DELETE)
        {
            RangeTblEntry *newrte;

            rte = rt_fetch(rt_index, parsetree->rtable);
            newrte = copyObject(rte);
            parsetree->rtable = lappend(parsetree->rtable, newrte);
            parsetree->resultRelation = list_length(parsetree->rtable);

            /*
             * There's no need to do permissions checks twice, so wipe out the
             * permissions info for the original RTE (we prefer to keep the
             * bits set on the result RTE).
             */
            rte->requiredPerms = 0;
            rte->checkAsUser = InvalidOid;
            rte->selectedCols = NULL;
            rte->insertedCols = NULL;
            rte->updatedCols = NULL;

            /*
             * For the most part, Vars referencing the view should remain as
             * they are, meaning that they implicitly represent OLD values.
             * But in the RETURNING list if any, we want such Vars to
             * represent NEW values, so change them to reference the new RTE.
             *
             * Since ChangeVarNodes scribbles on the tree in-place, copy the
             * RETURNING list first for safety.
             */
            parsetree->returningList = copyObject(parsetree->returningList);
            ChangeVarNodes((Node *) parsetree->returningList, rt_index,
                           parsetree->resultRelation, 0);

            /* Now, continue with expanding the original view RTE */
        }
        else
            elog(ERROR, "unrecognized commandType: %d",
                 (int) parsetree->commandType);
    }

    /*
     * If FOR [KEY] UPDATE/SHARE of view, be sure we get right initial lock on
     * the relations it references.
     */
    rc = get_parse_rowmark(parsetree, rt_index);
    forUpdatePushedDown |= (rc != NULL);

    /*
     * Make a modifiable copy of the view query, and acquire needed locks on
     * the relations it mentions.
     */
    rule_action = copyObject(linitial(rule->actions));

    AcquireRewriteLocks(rule_action, true, forUpdatePushedDown);

    /*
     * Recursively expand any view references inside the view.
     */
    rule_action = fireRIRrules(rule_action, activeRIRs, forUpdatePushedDown);

    /*
     * Now, plug the view query in as a subselect, replacing the relation's
     * original RTE.
     */
    rte = rt_fetch(rt_index, parsetree->rtable);

    rte->rtekind = RTE_SUBQUERY;
    rte->relid = InvalidOid;
    rte->security_barrier = RelationIsSecurityView(relation);
    rte->subquery = rule_action;
    rte->inh = false;            /* must not be set for a subquery */

    /*
     * We move the view's permission check data down to its rangetable. The
     * checks will actually be done against the OLD entry therein.
     */
    subrte = rt_fetch(PRS2_OLD_VARNO, rule_action->rtable);
    Assert(subrte->relid == relation->rd_id);
    subrte->requiredPerms = rte->requiredPerms;
    subrte->checkAsUser = rte->checkAsUser;
    subrte->selectedCols = rte->selectedCols;
    subrte->insertedCols = rte->insertedCols;
    subrte->updatedCols = rte->updatedCols;

    rte->requiredPerms = 0;        /* no permission check on subquery itself */
    rte->checkAsUser = InvalidOid;
    rte->selectedCols = NULL;
    rte->insertedCols = NULL;
    rte->updatedCols = NULL;

    /*
     * If FOR [KEY] UPDATE/SHARE of view, mark all the contained tables as
     * implicit FOR [KEY] UPDATE/SHARE, the same as the parser would have done
     * if the view's subquery had been written out explicitly.
     *
     * Note: we don't consider forUpdatePushedDown here; such marks will be
     * made by recursing from the upper level in markQueryForLocking.
     */
    if (rc != NULL)
        markQueryForLocking(rule_action, (Node *) rule_action->jointree,
                            rc->strength, rc->waitPolicy, true);

    return parsetree;
}

/*
 * Recursively mark all relations used by a view as FOR [KEY] UPDATE/SHARE.
 *
 * This may generate an invalid query, eg if some sub-query uses an
 * aggregate.  We leave it to the planner to detect that.
 *
 * NB: this must agree with the parser's transformLockingClause() routine.
 * However, unlike the parser we have to be careful not to mark a view's
 * OLD and NEW rels for updating.  The best way to handle that seems to be
 * to scan the jointree to determine which rels are used.
 */
static void
markQueryForLocking(Query *qry, Node *jtnode,
                    LockClauseStrength strength, LockWaitPolicy waitPolicy,
                    bool pushedDown)
{
    if (jtnode == NULL)
        return;
    if (IsA(jtnode, RangeTblRef))
    {
        int            rti = ((RangeTblRef *) jtnode)->rtindex;
        RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

        if (rte->rtekind == RTE_RELATION)
        {
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown);
            rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
        }
        else if (rte->rtekind == RTE_SUBQUERY)
        {
            applyLockingClause(qry, rti, strength, waitPolicy, pushedDown);
            /* FOR UPDATE/SHARE of subquery is propagated to subquery's rels */
            markQueryForLocking(rte->subquery, (Node *) rte->subquery->jointree,
                                strength, waitPolicy, true);
        }
        /* other RTE types are unaffected by FOR UPDATE */
    }
    else if (IsA(jtnode, FromExpr))
    {
        FromExpr   *f = (FromExpr *) jtnode;
        ListCell   *l;

        foreach(l, f->fromlist)
            markQueryForLocking(qry, lfirst(l), strength, waitPolicy, pushedDown);
    }
    else if (IsA(jtnode, JoinExpr))
    {
        JoinExpr   *j = (JoinExpr *) jtnode;

        markQueryForLocking(qry, j->larg, strength, waitPolicy, pushedDown);
        markQueryForLocking(qry, j->rarg, strength, waitPolicy, pushedDown);
    }
    else
        elog(ERROR, "unrecognized node type: %d",
             (int) nodeTag(jtnode));
}


/*
 * fireRIRonSubLink -
 *    Apply fireRIRrules() to each SubLink (subselect in expression) found
 *    in the given tree.
 *
 * NOTE: although this has the form of a walker, we cheat and modify the
 * SubLink nodes in-place.  It is caller's responsibility to ensure that
 * no unwanted side-effects occur!
 *
 * This is unlike most of the other routines that recurse into subselects,
 * because we must take control at the SubLink node in order to replace
 * the SubLink's subselect link with the possibly-rewritten subquery.
 */
static bool
fireRIRonSubLink(Node *node, List *activeRIRs)
{
    if (node == NULL)
        return false;
    if (IsA(node, SubLink))
    {
        SubLink    *sub = (SubLink *) node;

        /* Do what we came for */
        sub->subselect = (Node *) fireRIRrules((Query *) sub->subselect,
                                               activeRIRs, false);
        /* Fall through to process lefthand args of SubLink */
    }

    /*
     * Do NOT recurse into Query nodes, because fireRIRrules already processed
     * subselects of subselects for us.
     */
    return expression_tree_walker(node, fireRIRonSubLink,
                                  (void *) activeRIRs);
}


/*
 * fireRIRrules -
 *    Apply all RIR rules on each rangetable entry in a query
 */
static Query *
fireRIRrules(Query *parsetree, List *activeRIRs, bool forUpdatePushedDown)
{// #lizard forgives
    int            origResultRelation = parsetree->resultRelation;
    int            rt_index;
    ListCell   *lc;

    /*
     * don't try to convert this into a foreach loop, because rtable list can
     * get changed each time through...
     */
    rt_index = 0;
    while (rt_index < list_length(parsetree->rtable))
    {
        RangeTblEntry *rte;
        Relation    rel;
        List       *locks;
        RuleLock   *rules;
        RewriteRule *rule;
        int            i;

        ++rt_index;

        rte = rt_fetch(rt_index, parsetree->rtable);

        /*
         * A subquery RTE can't have associated rules, so there's nothing to
         * do to this level of the query, but we must recurse into the
         * subquery to expand any rule references in it.
         */
        if (rte->rtekind == RTE_SUBQUERY)
        {
            rte->subquery = fireRIRrules(rte->subquery, activeRIRs,
                                         (forUpdatePushedDown ||
                                          get_parse_rowmark(parsetree, rt_index) != NULL));
            continue;
        }

        /*
         * Joins and other non-relation RTEs can be ignored completely.
         */
        if (rte->rtekind != RTE_RELATION)
            continue;

        /*
         * Always ignore RIR rules for materialized views referenced in
         * queries.  (This does not prevent refreshing MVs, since they aren't
         * referenced in their own query definitions.)
         *
         * Note: in the future we might want to allow MVs to be conditionally
         * expanded as if they were regular views, if they are not scannable.
         * In that case this test would need to be postponed till after we've
         * opened the rel, so that we could check its state.
         */
        if (rte->relkind == RELKIND_MATVIEW)
            continue;

        /*
         * If the table is not referenced in the query, then we ignore it.
         * This prevents infinite expansion loop due to new rtable entries
         * inserted by expansion of a rule. A table is referenced if it is
         * part of the join set (a source table), or is referenced by any Var
         * nodes, or is the result table.
         */
        if (rt_index != parsetree->resultRelation &&
            !rangeTableEntry_used((Node *) parsetree, rt_index, 0))
            continue;

        /*
         * Also, if this is a new result relation introduced by
         * ApplyRetrieveRule, we don't want to do anything more with it.
         */
        if (rt_index == parsetree->resultRelation &&
            rt_index != origResultRelation)
            continue;

        /*
         * We can use NoLock here since either the parser or
         * AcquireRewriteLocks should have locked the rel already.
         */
        rel = heap_open(rte->relid, NoLock);

        /*
         * Collect the RIR rules that we must apply
         */
        rules = rel->rd_rules;
        if (rules != NULL)
        {
            locks = NIL;
            for (i = 0; i < rules->numLocks; i++)
            {
                rule = rules->rules[i];
                if (rule->event != CMD_SELECT)
                    continue;

                locks = lappend(locks, rule);
            }

            /*
             * If we found any, apply them --- but first check for recursion!
             */
            if (locks != NIL)
            {
                ListCell   *l;

                if (list_member_oid(activeRIRs, RelationGetRelid(rel)))
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                             errmsg("infinite recursion detected in rules for relation \"%s\"",
                                    RelationGetRelationName(rel))));
                activeRIRs = lcons_oid(RelationGetRelid(rel), activeRIRs);

                foreach(l, locks)
                {
                    rule = lfirst(l);

                    parsetree = ApplyRetrieveRule(parsetree,
                                                  rule,
                                                  rt_index,
                                                  rel,
                                                  activeRIRs,
                                                  forUpdatePushedDown);
                }

                activeRIRs = list_delete_first(activeRIRs);
            }
        }

#ifdef _MLS_
        /* attach cls_func call node to rte if exists */
        if (rel->rd_cls_struct)
        {
            Node *funcexpr;

            funcexpr = copyObject(rel->rd_cls_struct->rd_cls_expr);
            
            rte->cls_expr = funcexpr;
        }
        else
        {
            rte->cls_expr = NULL;
        }
#endif

        heap_close(rel, NoLock);
    }

    /* Recurse into subqueries in WITH */
    foreach(lc, parsetree->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

        cte->ctequery = (Node *)
            fireRIRrules((Query *) cte->ctequery, activeRIRs, false);
    }

    /*
     * Recurse into sublink subqueries, too.  But we already did the ones in
     * the rtable and cteList.
     */
    if (parsetree->hasSubLinks)
        query_tree_walker(parsetree, fireRIRonSubLink, (void *) activeRIRs,
                          QTW_IGNORE_RC_SUBQUERIES);

    /*
     * Apply any row level security policies.  We do this last because it
     * requires special recursion detection if the new quals have sublink
     * subqueries, and if we did it in the loop above query_tree_walker would
     * then recurse into those quals a second time.
     */
    rt_index = 0;
    foreach(lc, parsetree->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        Relation    rel;
        List       *securityQuals;
        List       *withCheckOptions;
        bool        hasRowSecurity;
        bool        hasSubLinks;

        ++rt_index;

        /* Only normal relations can have RLS policies */
        if (rte->rtekind != RTE_RELATION ||
            (rte->relkind != RELKIND_RELATION &&
             rte->relkind != RELKIND_PARTITIONED_TABLE))
            continue;

        rel = heap_open(rte->relid, NoLock);

        /*
         * Fetch any new security quals that must be applied to this RTE.
         */
        get_row_security_policies(parsetree, rte, rt_index,
                                  &securityQuals, &withCheckOptions,
                                  &hasRowSecurity, &hasSubLinks);

        if (securityQuals != NIL || withCheckOptions != NIL)
        {
            if (hasSubLinks)
            {
                acquireLocksOnSubLinks_context context;

                /*
                 * Recursively process the new quals, checking for infinite
                 * recursion.
                 */
                if (list_member_oid(activeRIRs, RelationGetRelid(rel)))
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                             errmsg("infinite recursion detected in policy for relation \"%s\"",
                                    RelationGetRelationName(rel))));

                activeRIRs = lcons_oid(RelationGetRelid(rel), activeRIRs);

                /*
                 * get_row_security_policies just passed back securityQuals
                 * and/or withCheckOptions, and there were SubLinks, make sure
                 * we lock any relations which are referenced.
                 *
                 * These locks would normally be acquired by the parser, but
                 * securityQuals and withCheckOptions are added post-parsing.
                 */
                context.for_execute = true;
                (void) acquireLocksOnSubLinks((Node *) securityQuals, &context);
                (void) acquireLocksOnSubLinks((Node *) withCheckOptions,
                                              &context);

                /*
                 * Now that we have the locks on anything added by
                 * get_row_security_policies, fire any RIR rules for them.
                 */
                expression_tree_walker((Node *) securityQuals,
                                       fireRIRonSubLink, (void *) activeRIRs);

                expression_tree_walker((Node *) withCheckOptions,
                                       fireRIRonSubLink, (void *) activeRIRs);

                activeRIRs = list_delete_first(activeRIRs);
            }

            /*
             * Add the new security barrier quals to the start of the RTE's
             * list so that they get applied before any existing barrier quals
             * (which would have come from a security-barrier view, and should
             * get lower priority than RLS conditions on the table itself).
             */
            rte->securityQuals = list_concat(securityQuals,
                                             rte->securityQuals);

            parsetree->withCheckOptions = list_concat(withCheckOptions,
                                                      parsetree->withCheckOptions);
        }

        /*
         * Make sure the query is marked correctly if row level security
         * applies, or if the new quals had sublinks.
         */
        if (hasRowSecurity)
            parsetree->hasRowSecurity = true;
        if (hasSubLinks)
            parsetree->hasSubLinks = true;

        heap_close(rel, NoLock);
    }

    return parsetree;
}


/*
 * Modify the given query by adding 'AND rule_qual IS NOT TRUE' to its
 * qualification.  This is used to generate suitable "else clauses" for
 * conditional INSTEAD rules.  (Unfortunately we must use "x IS NOT TRUE",
 * not just "NOT x" which the planner is much smarter about, else we will
 * do the wrong thing when the qual evaluates to NULL.)
 *
 * The rule_qual may contain references to OLD or NEW.  OLD references are
 * replaced by references to the specified rt_index (the relation that the
 * rule applies to).  NEW references are only possible for INSERT and UPDATE
 * queries on the relation itself, and so they should be replaced by copies
 * of the related entries in the query's own targetlist.
 */
static Query *
CopyAndAddInvertedQual(Query *parsetree,
                       Node *rule_qual,
                       int rt_index,
                       CmdType event)
{
    /* Don't scribble on the passed qual (it's in the relcache!) */
    Node       *new_qual = copyObject(rule_qual);
    acquireLocksOnSubLinks_context context;

    context.for_execute = true;

    /*
     * In case there are subqueries in the qual, acquire necessary locks and
     * fix any deleted JOIN RTE entries.  (This is somewhat redundant with
     * rewriteRuleAction, but not entirely ... consider restructuring so that
     * we only need to process the qual this way once.)
     */
    (void) acquireLocksOnSubLinks(new_qual, &context);

    /* Fix references to OLD */
    ChangeVarNodes(new_qual, PRS2_OLD_VARNO, rt_index, 0);
    /* Fix references to NEW */
    if (event == CMD_INSERT || event == CMD_UPDATE)
        new_qual = ReplaceVarsFromTargetList(new_qual,
                                             PRS2_NEW_VARNO,
                                             0,
                                             rt_fetch(rt_index,
                                                      parsetree->rtable),
                                             parsetree->targetList,
                                             (event == CMD_UPDATE) ?
                                             REPLACEVARS_CHANGE_VARNO :
                                             REPLACEVARS_SUBSTITUTE_NULL,
                                             rt_index,
                                             &parsetree->hasSubLinks);
    /* And attach the fixed qual */
    AddInvertedQual(parsetree, new_qual);

    return parsetree;
}


/*
 *    fireRules -
 *       Iterate through rule locks applying rules.
 *
 * Input arguments:
 *    parsetree - original query
 *    rt_index - RT index of result relation in original query
 *    event - type of rule event
 *    locks - list of rules to fire
 * Output arguments:
 *    *instead_flag - set TRUE if any unqualified INSTEAD rule is found
 *                    (must be initialized to FALSE)
 *    *returning_flag - set TRUE if we rewrite RETURNING clause in any rule
 *                    (must be initialized to FALSE)
 *    *qual_product - filled with modified original query if any qualified
 *                    INSTEAD rule is found (must be initialized to NULL)
 * Return value:
 *    list of rule actions adjusted for use with this query
 *
 * Qualified INSTEAD rules generate their action with the qualification
 * condition added.  They also generate a modified version of the original
 * query with the negated qualification added, so that it will run only for
 * rows that the qualified action doesn't act on.  (If there are multiple
 * qualified INSTEAD rules, we AND all the negated quals onto a single
 * modified original query.)  We won't execute the original, unmodified
 * query if we find either qualified or unqualified INSTEAD rules.  If
 * we find both, the modified original query is discarded too.
 */
static List *
fireRules(Query *parsetree,
          int rt_index,
          CmdType event,
          List *locks,
          bool *instead_flag,
          bool *returning_flag,
          Query **qual_product)
{
    List       *results = NIL;
    ListCell   *l;

    foreach(l, locks)
    {
        RewriteRule *rule_lock = (RewriteRule *) lfirst(l);
        Node       *event_qual = rule_lock->qual;
        List       *actions = rule_lock->actions;
        QuerySource qsrc;
        ListCell   *r;

        /* Determine correct QuerySource value for actions */
        if (rule_lock->isInstead)
        {
            if (event_qual != NULL)
                qsrc = QSRC_QUAL_INSTEAD_RULE;
            else
            {
                qsrc = QSRC_INSTEAD_RULE;
                *instead_flag = true;    /* report unqualified INSTEAD */
            }
        }
        else
            qsrc = QSRC_NON_INSTEAD_RULE;

        if (qsrc == QSRC_QUAL_INSTEAD_RULE)
        {
            /*
             * If there are INSTEAD rules with qualifications, the original
             * query is still performed. But all the negated rule
             * qualifications of the INSTEAD rules are added so it does its
             * actions only in cases where the rule quals of all INSTEAD rules
             * are false. Think of it as the default action in a case. We save
             * this in *qual_product so RewriteQuery() can add it to the query
             * list after we mangled it up enough.
             *
             * If we have already found an unqualified INSTEAD rule, then
             * *qual_product won't be used, so don't bother building it.
             */
            if (!*instead_flag)
            {
                if (*qual_product == NULL)
                    *qual_product = copyObject(parsetree);
                *qual_product = CopyAndAddInvertedQual(*qual_product,
                                                       event_qual,
                                                       rt_index,
                                                       event);
            }
        }

        /* Now process the rule's actions and add them to the result list */
        foreach(r, actions)
        {
            Query       *rule_action = lfirst(r);

            if (rule_action->commandType == CMD_NOTHING)
                continue;

            rule_action = rewriteRuleAction(parsetree, rule_action,
                                            event_qual, rt_index, event,
                                            returning_flag);

            rule_action->querySource = qsrc;
            rule_action->canSetTag = false; /* might change later */

            results = lappend(results, rule_action);
        }
    }

    return results;
}


/*
 * get_view_query - get the Query from a view's _RETURN rule.
 *
 * Caller should have verified that the relation is a view, and therefore
 * we should find an ON SELECT action.
 *
 * Note that the pointer returned is into the relcache and therefore must
 * be treated as read-only to the caller and not modified or scribbled on.
 */
Query *
get_view_query(Relation view)
{
    int            i;

    Assert(view->rd_rel->relkind == RELKIND_VIEW);

    for (i = 0; i < view->rd_rules->numLocks; i++)
    {
        RewriteRule *rule = view->rd_rules->rules[i];

        if (rule->event == CMD_SELECT)
        {
            /* A _RETURN rule should have only one action */
            if (list_length(rule->actions) != 1)
                elog(ERROR, "invalid _RETURN rule action specification");

            return (Query *) linitial(rule->actions);
        }
    }

    elog(ERROR, "failed to find _RETURN rule for view");
    return NULL;                /* keep compiler quiet */
}


/*
 * view_has_instead_trigger - does view have an INSTEAD OF trigger for event?
 *
 * If it does, we don't want to treat it as auto-updatable.  This test can't
 * be folded into view_query_is_auto_updatable because it's not an error
 * condition.
 */
static bool
view_has_instead_trigger(Relation view, CmdType event)
{// #lizard forgives
    TriggerDesc *trigDesc = view->trigdesc;

    switch (event)
    {
        case CMD_INSERT:
            if (trigDesc && trigDesc->trig_insert_instead_row)
                return true;
            break;
        case CMD_UPDATE:
            if (trigDesc && trigDesc->trig_update_instead_row)
                return true;
            break;
        case CMD_DELETE:
            if (trigDesc && trigDesc->trig_delete_instead_row)
                return true;
            break;
        default:
            elog(ERROR, "unrecognized CmdType: %d", (int) event);
            break;
    }
    return false;
}


/*
 * view_col_is_auto_updatable - test whether the specified column of a view
 * is auto-updatable. Returns NULL (if the column can be updated) or a message
 * string giving the reason that it cannot be.
 *
 * Note that the checks performed here are local to this view. We do not check
 * whether the referenced column of the underlying base relation is updatable.
 */
static const char *
view_col_is_auto_updatable(RangeTblRef *rtr, TargetEntry *tle)
{
    Var           *var = (Var *) tle->expr;

    /*
     * For now, the only updatable columns we support are those that are Vars
     * referring to user columns of the underlying base relation.
     *
     * The view targetlist may contain resjunk columns (e.g., a view defined
     * like "SELECT * FROM t ORDER BY a+b" is auto-updatable) but such columns
     * are not auto-updatable, and in fact should never appear in the outer
     * query's targetlist.
     */
    if (tle->resjunk)
        return gettext_noop("Junk view columns are not updatable.");

    if (!IsA(var, Var) ||
        var->varno != rtr->rtindex ||
        var->varlevelsup != 0)
        return gettext_noop("View columns that are not columns of their base relation are not updatable.");

    if (var->varattno < 0)
        return gettext_noop("View columns that refer to system columns are not updatable.");

    if (var->varattno == 0)
        return gettext_noop("View columns that return whole-row references are not updatable.");

    return NULL;                /* the view column is updatable */
}


/*
 * view_query_is_auto_updatable - test whether the specified view definition
 * represents an auto-updatable view. Returns NULL (if the view can be updated)
 * or a message string giving the reason that it cannot be.
 *
 * If check_cols is true, the view is required to have at least one updatable
 * column (necessary for INSERT/UPDATE). Otherwise the view's columns are not
 * checked for updatability. See also view_cols_are_auto_updatable.
 *
 * Note that the checks performed here are only based on the view definition.
 * We do not check whether any base relations referred to by the view are
 * updatable.
 */
const char *
view_query_is_auto_updatable(Query *viewquery, bool check_cols)
{// #lizard forgives
    RangeTblRef *rtr;
    RangeTblEntry *base_rte;

    /*----------
     * Check if the view is simply updatable.  According to SQL-92 this means:
     *    - No DISTINCT clause.
     *    - Each TLE is a column reference, and each column appears at most once.
     *    - FROM contains exactly one base relation.
     *    - No GROUP BY or HAVING clauses.
     *    - No set operations (UNION, INTERSECT or EXCEPT).
     *    - No sub-queries in the WHERE clause that reference the target table.
     *
     * We ignore that last restriction since it would be complex to enforce
     * and there isn't any actual benefit to disallowing sub-queries.  (The
     * semantic issues that the standard is presumably concerned about don't
     * arise in Postgres, since any such sub-query will not see any updates
     * executed by the outer query anyway, thanks to MVCC snapshotting.)
     *
     * We also relax the second restriction by supporting part of SQL:1999
     * feature T111, which allows for a mix of updatable and non-updatable
     * columns, provided that an INSERT or UPDATE doesn't attempt to assign to
     * a non-updatable column.
     *
     * In addition we impose these constraints, involving features that are
     * not part of SQL-92:
     *    - No CTEs (WITH clauses).
     *    - No OFFSET or LIMIT clauses (this matches a SQL:2008 restriction).
     *    - No system columns (including whole-row references) in the tlist.
     *    - No window functions in the tlist.
     *    - No set-returning functions in the tlist.
     *
     * Note that we do these checks without recursively expanding the view.
     * If the base relation is a view, we'll recursively deal with it later.
     *----------
     */
    if (viewquery->distinctClause != NIL)
        return gettext_noop("Views containing DISTINCT are not automatically updatable.");

    if (viewquery->groupClause != NIL || viewquery->groupingSets)
        return gettext_noop("Views containing GROUP BY are not automatically updatable.");

    if (viewquery->havingQual != NULL)
        return gettext_noop("Views containing HAVING are not automatically updatable.");

    if (viewquery->setOperations != NULL)
        return gettext_noop("Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.");

    if (viewquery->cteList != NIL)
        return gettext_noop("Views containing WITH are not automatically updatable.");

    if (viewquery->limitOffset != NULL || viewquery->limitCount != NULL)
        return gettext_noop("Views containing LIMIT or OFFSET are not automatically updatable.");

    /*
     * We must not allow window functions or set returning functions in the
     * targetlist. Otherwise we might end up inserting them into the quals of
     * the main query. We must also check for aggregates in the targetlist in
     * case they appear without a GROUP BY.
     *
     * These restrictions ensure that each row of the view corresponds to a
     * unique row in the underlying base relation.
     */
    if (viewquery->hasAggs)
        return gettext_noop("Views that return aggregate functions are not automatically updatable.");

    if (viewquery->hasWindowFuncs)
        return gettext_noop("Views that return window functions are not automatically updatable.");

    if (viewquery->hasTargetSRFs)
        return gettext_noop("Views that return set-returning functions are not automatically updatable.");

    /*
     * The view query should select from a single base relation, which must be
     * a table or another view.
     */
    if (list_length(viewquery->jointree->fromlist) != 1)
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    rtr = (RangeTblRef *) linitial(viewquery->jointree->fromlist);
    if (!IsA(rtr, RangeTblRef))
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);
    if (base_rte->rtekind != RTE_RELATION ||
        (base_rte->relkind != RELKIND_RELATION &&
         base_rte->relkind != RELKIND_FOREIGN_TABLE &&
         base_rte->relkind != RELKIND_VIEW &&
         base_rte->relkind != RELKIND_PARTITIONED_TABLE))
        return gettext_noop("Views that do not select from a single table or view are not automatically updatable.");

    if (base_rte->tablesample)
        return gettext_noop("Views containing TABLESAMPLE are not automatically updatable.");

    /*
     * Check that the view has at least one updatable column. This is required
     * for INSERT/UPDATE but not for DELETE.
     */
    if (check_cols)
    {
        ListCell   *cell;
        bool        found;

        found = false;
        foreach(cell, viewquery->targetList)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(cell);

            if (view_col_is_auto_updatable(rtr, tle) == NULL)
            {
                found = true;
                break;
            }
        }

        if (!found)
            return gettext_noop("Views that have no updatable columns are not automatically updatable.");
    }

    return NULL;                /* the view is updatable */
}


/*
 * view_cols_are_auto_updatable - test whether all of the required columns of
 * an auto-updatable view are actually updatable. Returns NULL (if all the
 * required columns can be updated) or a message string giving the reason that
 * they cannot be.
 *
 * This should be used for INSERT/UPDATE to ensure that we don't attempt to
 * assign to any non-updatable columns.
 *
 * Additionally it may be used to retrieve the set of updatable columns in the
 * view, or if one or more of the required columns is not updatable, the name
 * of the first offending non-updatable column.
 *
 * The caller must have already verified that this is an auto-updatable view
 * using view_query_is_auto_updatable.
 *
 * Note that the checks performed here are only based on the view definition.
 * We do not check whether the referenced columns of the base relation are
 * updatable.
 */
static const char *
view_cols_are_auto_updatable(Query *viewquery,
                             Bitmapset *required_cols,
                             Bitmapset **updatable_cols,
                             char **non_updatable_col)
{
    RangeTblRef *rtr;
    AttrNumber    col;
    ListCell   *cell;

    /*
     * The caller should have verified that this view is auto-updatable and so
     * there should be a single base relation.
     */
    Assert(list_length(viewquery->jointree->fromlist) == 1);
    rtr = linitial_node(RangeTblRef, viewquery->jointree->fromlist);

    /* Initialize the optional return values */
    if (updatable_cols != NULL)
        *updatable_cols = NULL;
    if (non_updatable_col != NULL)
        *non_updatable_col = NULL;

    /* Test each view column for updatability */
    col = -FirstLowInvalidHeapAttributeNumber;
    foreach(cell, viewquery->targetList)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(cell);
        const char *col_update_detail;

        col++;
        col_update_detail = view_col_is_auto_updatable(rtr, tle);

        if (col_update_detail == NULL)
        {
            /* The column is updatable */
            if (updatable_cols != NULL)
                *updatable_cols = bms_add_member(*updatable_cols, col);
        }
        else if (bms_is_member(col, required_cols))
        {
            /* The required column is not updatable */
            if (non_updatable_col != NULL)
                *non_updatable_col = tle->resname;
            return col_update_detail;
        }
    }

    return NULL;                /* all the required view columns are updatable */
}


/*
 * relation_is_updatable - determine which update events the specified
 * relation supports.
 *
 * Note that views may contain a mix of updatable and non-updatable columns.
 * For a view to support INSERT/UPDATE it must have at least one updatable
 * column, but there is no such restriction for DELETE. If include_cols is
 * non-NULL, then only the specified columns are considered when testing for
 * updatability.
 *
 * This is used for the information_schema views, which have separate concepts
 * of "updatable" and "trigger updatable".  A relation is "updatable" if it
 * can be updated without the need for triggers (either because it has a
 * suitable RULE, or because it is simple enough to be automatically updated).
 * A relation is "trigger updatable" if it has a suitable INSTEAD OF trigger.
 * The SQL standard regards this as not necessarily updatable, presumably
 * because there is no way of knowing what the trigger will actually do.
 * The information_schema views therefore call this function with
 * include_triggers = false.  However, other callers might only care whether
 * data-modifying SQL will work, so they can pass include_triggers = true
 * to have trigger updatability included in the result.
 *
 * The return value is a bitmask of rule event numbers indicating which of
 * the INSERT, UPDATE and DELETE operations are supported.  (We do it this way
 * so that we can test for UPDATE plus DELETE support in a single call.)
 */
int
relation_is_updatable(Oid reloid,
                      bool include_triggers,
                      Bitmapset *include_cols)
{// #lizard forgives
    int            events = 0;
    Relation    rel;
    RuleLock   *rulelocks;

#define ALL_EVENTS ((1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE))

    rel = try_relation_open(reloid, AccessShareLock);

    /*
     * If the relation doesn't exist, return zero rather than throwing an
     * error.  This is helpful since scanning an information_schema view under
     * MVCC rules can result in referencing rels that have actually been
     * deleted already.
     */
    if (rel == NULL)
        return 0;

    /* If the relation is a table, it is always updatable */
    if (rel->rd_rel->relkind == RELKIND_RELATION ||
        rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
    {
        relation_close(rel, AccessShareLock);
        return ALL_EVENTS;
    }

    /* Look for unconditional DO INSTEAD rules, and note supported events */
    rulelocks = rel->rd_rules;
    if (rulelocks != NULL)
    {
        int            i;

        for (i = 0; i < rulelocks->numLocks; i++)
        {
            if (rulelocks->rules[i]->isInstead &&
                rulelocks->rules[i]->qual == NULL)
            {
                events |= ((1 << rulelocks->rules[i]->event) & ALL_EVENTS);
            }
        }

        /* If we have rules for all events, we're done */
        if (events == ALL_EVENTS)
        {
            relation_close(rel, AccessShareLock);
            return events;
        }
    }

    /* Similarly look for INSTEAD OF triggers, if they are to be included */
    if (include_triggers)
    {
        TriggerDesc *trigDesc = rel->trigdesc;

        if (trigDesc)
        {
            if (trigDesc->trig_insert_instead_row)
                events |= (1 << CMD_INSERT);
            if (trigDesc->trig_update_instead_row)
                events |= (1 << CMD_UPDATE);
            if (trigDesc->trig_delete_instead_row)
                events |= (1 << CMD_DELETE);

            /* If we have triggers for all events, we're done */
            if (events == ALL_EVENTS)
            {
                relation_close(rel, AccessShareLock);
                return events;
            }
        }
    }

    /* If this is a foreign table, check which update events it supports */
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
    {
        FdwRoutine *fdwroutine = GetFdwRoutineForRelation(rel, false);

        if (fdwroutine->IsForeignRelUpdatable != NULL)
            events |= fdwroutine->IsForeignRelUpdatable(rel);
        else
        {
            /* Assume presence of executor functions is sufficient */
            if (fdwroutine->ExecForeignInsert != NULL)
                events |= (1 << CMD_INSERT);
            if (fdwroutine->ExecForeignUpdate != NULL)
                events |= (1 << CMD_UPDATE);
            if (fdwroutine->ExecForeignDelete != NULL)
                events |= (1 << CMD_DELETE);
        }

        relation_close(rel, AccessShareLock);
        return events;
    }

    /* Check if this is an automatically updatable view */
    if (rel->rd_rel->relkind == RELKIND_VIEW)
    {
        Query       *viewquery = get_view_query(rel);

        if (view_query_is_auto_updatable(viewquery, false) == NULL)
        {
            Bitmapset  *updatable_cols;
            int            auto_events;
            RangeTblRef *rtr;
            RangeTblEntry *base_rte;
            Oid            baseoid;

            /*
             * Determine which of the view's columns are updatable. If there
             * are none within the set of columns we are looking at, then the
             * view doesn't support INSERT/UPDATE, but it may still support
             * DELETE.
             */
            view_cols_are_auto_updatable(viewquery, NULL,
                                         &updatable_cols, NULL);

            if (include_cols != NULL)
                updatable_cols = bms_int_members(updatable_cols, include_cols);

            if (bms_is_empty(updatable_cols))
                auto_events = (1 << CMD_DELETE);    /* May support DELETE */
            else
                auto_events = ALL_EVENTS;    /* May support all events */

            /*
             * The base relation must also support these update commands.
             * Tables are always updatable, but for any other kind of base
             * relation we must do a recursive check limited to the columns
             * referenced by the locally updatable columns in this view.
             */
            rtr = (RangeTblRef *) linitial(viewquery->jointree->fromlist);
            base_rte = rt_fetch(rtr->rtindex, viewquery->rtable);
            Assert(base_rte->rtekind == RTE_RELATION);

            if (base_rte->relkind != RELKIND_RELATION &&
                base_rte->relkind != RELKIND_PARTITIONED_TABLE)
            {
                baseoid = base_rte->relid;
                include_cols = adjust_view_column_set(updatable_cols,
                                                      viewquery->targetList);
                auto_events &= relation_is_updatable(baseoid,
                                                     include_triggers,
                                                     include_cols);
            }
            events |= auto_events;
        }
    }

    /* If we reach here, the relation may support some update commands */
    relation_close(rel, AccessShareLock);
    return events;
}


/*
 * adjust_view_column_set - map a set of column numbers according to targetlist
 *
 * This is used with simply-updatable views to map column-permissions sets for
 * the view columns onto the matching columns in the underlying base relation.
 * The targetlist is expected to be a list of plain Vars of the underlying
 * relation (as per the checks above in view_query_is_auto_updatable).
 */
static Bitmapset *
adjust_view_column_set(Bitmapset *cols, List *targetlist)
{
    Bitmapset  *result = NULL;
    int            col;

    col = -1;
    while ((col = bms_next_member(cols, col)) >= 0)
    {
        /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
        AttrNumber    attno = col + FirstLowInvalidHeapAttributeNumber;

        if (attno == InvalidAttrNumber)
        {
            /*
             * There's a whole-row reference to the view.  For permissions
             * purposes, treat it as a reference to each column available from
             * the view.  (We should *not* convert this to a whole-row
             * reference to the base relation, since the view may not touch
             * all columns of the base relation.)
             */
            ListCell   *lc;

            foreach(lc, targetlist)
            {
                TargetEntry *tle = lfirst_node(TargetEntry, lc);
                Var           *var;

                if (tle->resjunk)
                    continue;
                var = castNode(Var, tle->expr);
                result = bms_add_member(result,
                                        var->varattno - FirstLowInvalidHeapAttributeNumber);
            }
        }
        else
        {
            /*
             * Views do not have system columns, so we do not expect to see
             * any other system attnos here.  If we do find one, the error
             * case will apply.
             */
            TargetEntry *tle = get_tle_by_resno(targetlist, attno);

            if (tle != NULL && !tle->resjunk && IsA(tle->expr, Var))
            {
                Var           *var = (Var *) tle->expr;

                result = bms_add_member(result,
                                        var->varattno - FirstLowInvalidHeapAttributeNumber);
            }
            else
                elog(ERROR, "attribute number %d not found in view targetlist",
                     attno);
        }
    }

    return result;
}


/*
 * rewriteTargetView -
 *      Attempt to rewrite a query where the target relation is a view, so that
 *      the view's base relation becomes the target relation.
 *
 * Note that the base relation here may itself be a view, which may or may not
 * have INSTEAD OF triggers or rules to handle the update.  That is handled by
 * the recursion in RewriteQuery.
 */
static Query *
rewriteTargetView(Query *parsetree, Relation view)
{// #lizard forgives
    Query       *viewquery;
    const char *auto_update_detail;
    RangeTblRef *rtr;
    int            base_rt_index;
    int            new_rt_index;
    RangeTblEntry *base_rte;
    RangeTblEntry *view_rte;
    RangeTblEntry *new_rte;
    Relation    base_rel;
    List       *view_targetlist;
    ListCell   *lc;

    /*
     * Get the Query from the view's ON SELECT rule.  We're going to munge the
     * Query to change the view's base relation into the target relation,
     * along with various other changes along the way, so we need to make a
     * copy of it (get_view_query() returns a pointer into the relcache, so we
     * have to treat it as read-only).
     */
    viewquery = copyObject(get_view_query(view));

    /* The view must be updatable, else fail */
    auto_update_detail =
        view_query_is_auto_updatable(viewquery,
                                     parsetree->commandType != CMD_DELETE);

    if (auto_update_detail)
    {
        /* messages here should match execMain.c's CheckValidResultRel */
        switch (parsetree->commandType)
        {
            case CMD_INSERT:
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("cannot insert into view \"%s\"",
                                RelationGetRelationName(view)),
                         errdetail_internal("%s", _(auto_update_detail)),
                         errhint("To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule.")));
                break;
            case CMD_UPDATE:
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("cannot update view \"%s\"",
                                RelationGetRelationName(view)),
                         errdetail_internal("%s", _(auto_update_detail)),
                         errhint("To enable updating the view, provide an INSTEAD OF UPDATE trigger or an unconditional ON UPDATE DO INSTEAD rule.")));
                break;
            case CMD_DELETE:
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("cannot delete from view \"%s\"",
                                RelationGetRelationName(view)),
                         errdetail_internal("%s", _(auto_update_detail)),
                         errhint("To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an unconditional ON DELETE DO INSTEAD rule.")));
                break;
            default:
                elog(ERROR, "unrecognized CmdType: %d",
                     (int) parsetree->commandType);
                break;
        }
    }

    /*
     * For INSERT/UPDATE the modified columns must all be updatable. Note that
     * we get the modified columns from the query's targetlist, not from the
     * result RTE's insertedCols and/or updatedCols set, since
     * rewriteTargetListIU may have added additional targetlist entries for
     * view defaults, and these must also be updatable.
     */
    if (parsetree->commandType != CMD_DELETE)
    {
        Bitmapset  *modified_cols = NULL;
        char       *non_updatable_col;

        foreach(lc, parsetree->targetList)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(lc);

            if (!tle->resjunk)
                modified_cols = bms_add_member(modified_cols,
                                               tle->resno - FirstLowInvalidHeapAttributeNumber);
        }

        if (parsetree->onConflict)
        {
            foreach(lc, parsetree->onConflict->onConflictSet)
            {
                TargetEntry *tle = (TargetEntry *) lfirst(lc);

                if (!tle->resjunk)
                    modified_cols = bms_add_member(modified_cols,
                                                   tle->resno - FirstLowInvalidHeapAttributeNumber);
            }
        }

        auto_update_detail = view_cols_are_auto_updatable(viewquery,
                                                          modified_cols,
                                                          NULL,
                                                          &non_updatable_col);
        if (auto_update_detail)
        {
            /*
             * This is a different error, caused by an attempt to update a
             * non-updatable column in an otherwise updatable view.
             */
            switch (parsetree->commandType)
            {
                case CMD_INSERT:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot insert into column \"%s\" of view \"%s\"",
                                    non_updatable_col,
                                    RelationGetRelationName(view)),
                             errdetail_internal("%s", _(auto_update_detail))));
                    break;
                case CMD_UPDATE:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot update column \"%s\" of view \"%s\"",
                                    non_updatable_col,
                                    RelationGetRelationName(view)),
                             errdetail_internal("%s", _(auto_update_detail))));
                    break;
                default:
                    elog(ERROR, "unrecognized CmdType: %d",
                         (int) parsetree->commandType);
                    break;
            }
        }
    }

    /* Locate RTE describing the view in the outer query */
    view_rte = rt_fetch(parsetree->resultRelation, parsetree->rtable);

    /*
     * If we get here, view_query_is_auto_updatable() has verified that the
     * view contains a single base relation.
     */
    Assert(list_length(viewquery->jointree->fromlist) == 1);
    rtr = linitial_node(RangeTblRef, viewquery->jointree->fromlist);

    base_rt_index = rtr->rtindex;
    base_rte = rt_fetch(base_rt_index, viewquery->rtable);
    Assert(base_rte->rtekind == RTE_RELATION);

    /*
     * Up to now, the base relation hasn't been touched at all in our query.
     * We need to acquire lock on it before we try to do anything with it.
     * (The subsequent recursive call of RewriteQuery will suppose that we
     * already have the right lock!)  Since it will become the query target
     * relation, RowExclusiveLock is always the right thing.
     */
    base_rel = heap_open(base_rte->relid, RowExclusiveLock);

    /*
     * While we have the relation open, update the RTE's relkind, just in case
     * it changed since this view was made (cf. AcquireRewriteLocks).
     */
    base_rte->relkind = base_rel->rd_rel->relkind;

    heap_close(base_rel, NoLock);

    /*
     * If the view query contains any sublink subqueries then we need to also
     * acquire locks on any relations they refer to.  We know that there won't
     * be any subqueries in the range table or CTEs, so we can skip those, as
     * in AcquireRewriteLocks.
     */
    if (viewquery->hasSubLinks)
    {
        acquireLocksOnSubLinks_context context;

        context.for_execute = true;
        query_tree_walker(viewquery, acquireLocksOnSubLinks, &context,
                          QTW_IGNORE_RC_SUBQUERIES);
    }

    /*
     * Create a new target RTE describing the base relation, and add it to the
     * outer query's rangetable.  (What's happening in the next few steps is
     * very much like what the planner would do to "pull up" the view into the
     * outer query.  Perhaps someday we should refactor things enough so that
     * we can share code with the planner.)
     */
    new_rte = (RangeTblEntry *) base_rte;
    parsetree->rtable = lappend(parsetree->rtable, new_rte);
    new_rt_index = list_length(parsetree->rtable);

    /*
     * INSERTs never inherit.  For UPDATE/DELETE, we use the view query's
     * inheritance flag for the base relation.
     */
    if (parsetree->commandType == CMD_INSERT)
        new_rte->inh = false;

    /*
     * Adjust the view's targetlist Vars to reference the new target RTE, ie
     * make their varnos be new_rt_index instead of base_rt_index.  There can
     * be no Vars for other rels in the tlist, so this is sufficient to pull
     * up the tlist expressions for use in the outer query.  The tlist will
     * provide the replacement expressions used by ReplaceVarsFromTargetList
     * below.
     */
    view_targetlist = viewquery->targetList;

    ChangeVarNodes((Node *) view_targetlist,
                   base_rt_index,
                   new_rt_index,
                   0);

    /*
     * Mark the new target RTE for the permissions checks that we want to
     * enforce against the view owner, as distinct from the query caller.  At
     * the relation level, require the same INSERT/UPDATE/DELETE permissions
     * that the query caller needs against the view.  We drop the ACL_SELECT
     * bit that is presumably in new_rte->requiredPerms initially.
     *
     * Note: the original view RTE remains in the query's rangetable list.
     * Although it will be unused in the query plan, we need it there so that
     * the executor still performs appropriate permissions checks for the
     * query caller's use of the view.
     */
    new_rte->checkAsUser = view->rd_rel->relowner;
    new_rte->requiredPerms = view_rte->requiredPerms;

    /*
     * Now for the per-column permissions bits.
     *
     * Initially, new_rte contains selectedCols permission check bits for all
     * base-rel columns referenced by the view, but since the view is a SELECT
     * query its insertedCols/updatedCols is empty.  We set insertedCols and
     * updatedCols to include all the columns the outer query is trying to
     * modify, adjusting the column numbers as needed.  But we leave
     * selectedCols as-is, so the view owner must have read permission for all
     * columns used in the view definition, even if some of them are not read
     * by the outer query.  We could try to limit selectedCols to only columns
     * used in the transformed query, but that does not correspond to what
     * happens in ordinary SELECT usage of a view: all referenced columns must
     * have read permission, even if optimization finds that some of them can
     * be discarded during query transformation.  The flattening we're doing
     * here is an optional optimization, too.  (If you are unpersuaded and
     * want to change this, note that applying adjust_view_column_set to
     * view_rte->selectedCols is clearly *not* the right answer, since that
     * neglects base-rel columns used in the view's WHERE quals.)
     *
     * This step needs the modified view targetlist, so we have to do things
     * in this order.
     */
    Assert(bms_is_empty(new_rte->insertedCols) &&
           bms_is_empty(new_rte->updatedCols));

    new_rte->insertedCols = adjust_view_column_set(view_rte->insertedCols,
                                                   view_targetlist);

    new_rte->updatedCols = adjust_view_column_set(view_rte->updatedCols,
                                                  view_targetlist);

    /*
     * Move any security barrier quals from the view RTE onto the new target
     * RTE.  Any such quals should now apply to the new target RTE and will
     * not reference the original view RTE in the rewritten query.
     */
    new_rte->securityQuals = view_rte->securityQuals;
    view_rte->securityQuals = NIL;

    /*
     * For UPDATE/DELETE, rewriteTargetListUD will have added a wholerow junk
     * TLE for the view to the end of the targetlist, which we no longer need.
     * Remove it to avoid unnecessary work when we process the targetlist.
     * Note that when we recurse through rewriteQuery a new junk TLE will be
     * added to allow the executor to find the proper row in the new target
     * relation.  (So, if we failed to do this, we might have multiple junk
     * TLEs with the same name, which would be disastrous.)
     */
    if (parsetree->commandType != CMD_INSERT)
    {
        TargetEntry *tle = (TargetEntry *) llast(parsetree->targetList);

        Assert(tle->resjunk);
        Assert(IsA(tle->expr, Var) &&
               ((Var *) tle->expr)->varno == parsetree->resultRelation &&
               ((Var *) tle->expr)->varattno == 0);
        parsetree->targetList = list_delete_ptr(parsetree->targetList, tle);
    }

    /*
     * Now update all Vars in the outer query that reference the view to
     * reference the appropriate column of the base relation instead.
     */
    parsetree = (Query *)
        ReplaceVarsFromTargetList((Node *) parsetree,
                                  parsetree->resultRelation,
                                  0,
                                  view_rte,
                                  view_targetlist,
                                  REPLACEVARS_REPORT_ERROR,
                                  0,
                                  &parsetree->hasSubLinks);

    /*
     * Update all other RTI references in the query that point to the view
     * (for example, parsetree->resultRelation itself) to point to the new
     * base relation instead.  Vars will not be affected since none of them
     * reference parsetree->resultRelation any longer.
     */
    ChangeVarNodes((Node *) parsetree,
                   parsetree->resultRelation,
                   new_rt_index,
                   0);
    Assert(parsetree->resultRelation == new_rt_index);

    /*
     * For INSERT/UPDATE we must also update resnos in the targetlist to refer
     * to columns of the base relation, since those indicate the target
     * columns to be affected.
     *
     * Note that this destroys the resno ordering of the targetlist, but that
     * will be fixed when we recurse through rewriteQuery, which will invoke
     * rewriteTargetListIU again on the updated targetlist.
     */
    if (parsetree->commandType != CMD_DELETE)
    {
        foreach(lc, parsetree->targetList)
        {
            TargetEntry *tle = (TargetEntry *) lfirst(lc);
            TargetEntry *view_tle;

            if (tle->resjunk)
                continue;

            view_tle = get_tle_by_resno(view_targetlist, tle->resno);
            if (view_tle != NULL && !view_tle->resjunk && IsA(view_tle->expr, Var))
                tle->resno = ((Var *) view_tle->expr)->varattno;
            else
                elog(ERROR, "attribute number %d not found in view targetlist",
                     tle->resno);
        }
    }

    /*
     * For UPDATE/DELETE, pull up any WHERE quals from the view.  We know that
     * any Vars in the quals must reference the one base relation, so we need
     * only adjust their varnos to reference the new target (just the same as
     * we did with the view targetlist).
     *
     * If it's a security-barrier view, its WHERE quals must be applied before
     * quals from the outer query, so we attach them to the RTE as security
     * barrier quals rather than adding them to the main WHERE clause.
     *
     * For INSERT, the view's quals can be ignored in the main query.
     */
    if (parsetree->commandType != CMD_INSERT &&
        viewquery->jointree->quals != NULL)
    {
        Node       *viewqual = (Node *) viewquery->jointree->quals;

        /*
         * Even though we copied viewquery already at the top of this
         * function, we must duplicate the viewqual again here, because we may
         * need to use the quals again below for a WithCheckOption clause.
         */
        viewqual = copyObject(viewqual);

        ChangeVarNodes(viewqual, base_rt_index, new_rt_index, 0);

        if (RelationIsSecurityView(view))
        {
            /*
             * The view's quals go in front of existing barrier quals: those
             * would have come from an outer level of security-barrier view,
             * and so must get evaluated later.
             *
             * Note: the parsetree has been mutated, so the new_rte pointer is
             * stale and needs to be re-computed.
             */
            new_rte = rt_fetch(new_rt_index, parsetree->rtable);
            new_rte->securityQuals = lcons(viewqual, new_rte->securityQuals);

            /*
             * Do not set parsetree->hasRowSecurity, because these aren't RLS
             * conditions (they aren't affected by enabling/disabling RLS).
             */

            /*
             * Make sure that the query is marked correctly if the added qual
             * has sublinks.
             */
            if (!parsetree->hasSubLinks)
                parsetree->hasSubLinks = checkExprHasSubLink(viewqual);
        }
        else
            AddQual(parsetree, (Node *) viewqual);
    }

    /*
     * For INSERT/UPDATE, if the view has the WITH CHECK OPTION, or any parent
     * view specified WITH CASCADED CHECK OPTION, add the quals from the view
     * to the query's withCheckOptions list.
     */
    if (parsetree->commandType != CMD_DELETE)
    {
        bool        has_wco = RelationHasCheckOption(view);
        bool        cascaded = RelationHasCascadedCheckOption(view);

        /*
         * If the parent view has a cascaded check option, treat this view as
         * if it also had a cascaded check option.
         *
         * New WithCheckOptions are added to the start of the list, so if
         * there is a cascaded check option, it will be the first item in the
         * list.
         */
        if (parsetree->withCheckOptions != NIL)
        {
            WithCheckOption *parent_wco =
            (WithCheckOption *) linitial(parsetree->withCheckOptions);

            if (parent_wco->cascaded)
            {
                has_wco = true;
                cascaded = true;
            }
        }

        /*
         * Add the new WithCheckOption to the start of the list, so that
         * checks on inner views are run before checks on outer views, as
         * required by the SQL standard.
         *
         * If the new check is CASCADED, we need to add it even if this view
         * has no quals, since there may be quals on child views.  A LOCAL
         * check can be omitted if this view has no quals.
         */
        if (has_wco && (cascaded || viewquery->jointree->quals != NULL))
        {
            WithCheckOption *wco;

            wco = makeNode(WithCheckOption);
            wco->kind = WCO_VIEW_CHECK;
            wco->relname = pstrdup(RelationGetRelationName(view));
            wco->polname = NULL;
            wco->qual = NULL;
            wco->cascaded = cascaded;

            parsetree->withCheckOptions = lcons(wco,
                                                parsetree->withCheckOptions);

            if (viewquery->jointree->quals != NULL)
            {
                wco->qual = (Node *) viewquery->jointree->quals;
                ChangeVarNodes(wco->qual, base_rt_index, new_rt_index, 0);

                /*
                 * Make sure that the query is marked correctly if the added
                 * qual has sublinks.  We can skip this check if the query is
                 * already marked, or if the command is an UPDATE, in which
                 * case the same qual will have already been added, and this
                 * check will already have been done.
                 */
                if (!parsetree->hasSubLinks &&
                    parsetree->commandType != CMD_UPDATE)
                    parsetree->hasSubLinks = checkExprHasSubLink(wco->qual);
            }
        }
    }

    return parsetree;
}


/*
 * RewriteQuery -
 *      rewrites the query and apply the rules again on the queries rewritten
 *
 * rewrite_events is a list of open query-rewrite actions, so we can detect
 * infinite recursion.
 */
static List *
RewriteQuery(Query *parsetree, List *rewrite_events)
{// #lizard forgives
    CmdType        event = parsetree->commandType;
    bool        instead = false;
    bool        returning = false;
    bool        updatableview = false;
    Query       *qual_product = NULL;
    List       *rewritten = NIL;
    ListCell   *lc1;

#ifdef PGXC
    List    *parsetree_list = NIL;
    List *qual_product_list = NIL;
    ListCell *pt_cell = NULL;
#endif

    /*
     * First, recursively process any insert/update/delete statements in WITH
     * clauses.  (We have to do this first because the WITH clauses may get
     * copied into rule actions below.)
     */
    foreach(lc1, parsetree->cteList)
    {
        CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc1);
        Query       *ctequery = castNode(Query, cte->ctequery);
        List       *newstuff;

        if (ctequery->commandType == CMD_SELECT)
            continue;

        newstuff = RewriteQuery(ctequery, rewrite_events);

        /*
         * Currently we can only handle unconditional, single-statement DO
         * INSTEAD rules correctly; we have to get exactly one Query out of
         * the rewrite operation to stuff back into the CTE node.
         */
        if (list_length(newstuff) == 1)
        {
            /* Push the single Query back into the CTE node */
            ctequery = linitial_node(Query, newstuff);
            /* WITH queries should never be canSetTag */
            Assert(!ctequery->canSetTag);
            cte->ctequery = (Node *) ctequery;
        }
        else if (newstuff == NIL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("DO INSTEAD NOTHING rules are not supported for data-modifying statements in WITH")));
        }
        else
        {
            ListCell   *lc2;

            /* examine queries to determine which error message to issue */
            foreach(lc2, newstuff)
            {
                Query       *q = (Query *) lfirst(lc2);

                if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("conditional DO INSTEAD rules are not supported for data-modifying statements in WITH")));
                if (q->querySource == QSRC_NON_INSTEAD_RULE)
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("DO ALSO rules are not supported for data-modifying statements in WITH")));
            }

            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("multi-statement DO INSTEAD rules are not supported for data-modifying statements in WITH")));
        }
    }

    /*
     * If the statement is an insert, update, or delete, adjust its targetlist
     * as needed, and then fire INSERT/UPDATE/DELETE rules on it.
     *
     * SELECT rules are handled later when we have all the queries that should
     * get executed.  Also, utilities aren't rewritten at all (do we still
     * need that check?)
     */
    if (event != CMD_SELECT && event != CMD_UTILITY)
    {
        int            result_relation;
        RangeTblEntry *rt_entry;
        Relation    rt_entry_relation;
        List       *locks;
        List       *product_queries;
        bool        hasUpdate = false;

        result_relation = parsetree->resultRelation;
        Assert(result_relation != 0);
        rt_entry = rt_fetch(result_relation, parsetree->rtable);
        Assert(rt_entry->rtekind == RTE_RELATION);

        /*
         * We can use NoLock here since either the parser or
         * AcquireRewriteLocks should have locked the rel already.
         */
        rt_entry_relation = heap_open(rt_entry->relid, NoLock);

        /*
         * Rewrite the targetlist as needed for the command type.
         */
        if (event == CMD_INSERT)
        {
            RangeTblEntry *values_rte = NULL;

            /*
             * If it's an INSERT ... VALUES (...), (...), ... there will be a
             * single RTE for the VALUES targetlists.
             */
            if (list_length(parsetree->jointree->fromlist) == 1)
            {
                RangeTblRef *rtr = (RangeTblRef *) linitial(parsetree->jointree->fromlist);

                if (IsA(rtr, RangeTblRef))
                {
                    RangeTblEntry *rte = rt_fetch(rtr->rtindex,
                                                  parsetree->rtable);

                    if (rte->rtekind == RTE_VALUES)
                        values_rte = rte;
                }
            }

            if (values_rte)
            {
                List       *attrnos;

                /* Process the main targetlist ... */
                parsetree->targetList = rewriteTargetListIU(parsetree->targetList,
                                                            parsetree->commandType,
                                                            parsetree->override,
                                                            rt_entry_relation,
                                                            parsetree->resultRelation,
                                                            &attrnos);
                /* ... and the VALUES expression lists */
                rewriteValuesRTE(values_rte, rt_entry_relation, attrnos);
            }
            else
            {
                /* Process just the main targetlist */
                parsetree->targetList =
                    rewriteTargetListIU(parsetree->targetList,
                                        parsetree->commandType,
                                        parsetree->override,
                                        rt_entry_relation,
                                        parsetree->resultRelation, NULL);
            }

            if (parsetree->onConflict &&
                parsetree->onConflict->action == ONCONFLICT_UPDATE)
            {
                parsetree->onConflict->onConflictSet =
                    rewriteTargetListIU(parsetree->onConflict->onConflictSet,
                                        CMD_UPDATE,
                                        parsetree->override,
                                        rt_entry_relation,
                                        parsetree->resultRelation,
                                        NULL);
            }
        }
        else if (event == CMD_UPDATE)
        {
            parsetree->targetList =
                rewriteTargetListIU(parsetree->targetList,
                                    parsetree->commandType,
                                    parsetree->override,
                                    rt_entry_relation,
                                    parsetree->resultRelation, NULL);
            rewriteTargetListUD(parsetree, rt_entry, rt_entry_relation);
        }
        else if (event == CMD_DELETE)
        {
            rewriteTargetListUD(parsetree, rt_entry, rt_entry_relation);
        }
        else
            elog(ERROR, "unrecognized commandType: %d", (int) event);

#ifdef PGXC
        if (parsetree_list == NIL)
        {
#endif
        /*
         * Collect and apply the appropriate rules.
         */
        locks = matchLocks(event, rt_entry_relation->rd_rules,
                           result_relation, parsetree, &hasUpdate);

#ifdef PGXC
            product_queries = NIL;
            if (IS_PGXC_COORDINATOR)
#endif                

        product_queries = fireRules(parsetree,
                                    result_relation,
                                    event,
                                    locks,
                                    &instead,
                                    &returning,
                                    &qual_product);

        /*
         * If there were no INSTEAD rules, and the target relation is a view
         * without any INSTEAD OF triggers, see if the view can be
         * automatically updated.  If so, we perform the necessary query
         * transformation here and add the resulting query to the
         * product_queries list, so that it gets recursively rewritten if
         * necessary.
         */
        if (!instead && qual_product == NULL &&
            rt_entry_relation->rd_rel->relkind == RELKIND_VIEW &&
            !view_has_instead_trigger(rt_entry_relation, event))
        {
            /*
             * This throws an error if the view can't be automatically
             * updated, but that's OK since the query would fail at runtime
             * anyway.
             */
            parsetree = rewriteTargetView(parsetree, rt_entry_relation);

            /*
             * At this point product_queries contains any DO ALSO rule
             * actions. Add the rewritten query before or after those.  This
             * must match the handling the original query would have gotten
             * below, if we allowed it to be included again.
             */
            if (parsetree->commandType == CMD_INSERT)
                product_queries = lcons(parsetree, product_queries);
            else
                product_queries = lappend(product_queries, parsetree);

            /*
             * Set the "instead" flag, as if there had been an unqualified
             * INSTEAD, to prevent the original query from being included a
             * second time below.  The transformation will have rewritten any
             * RETURNING list, so we can also set "returning" to forestall
             * throwing an error below.
             */
            instead = true;
            returning = true;
            updatableview = true;
        }

        /*
         * If we got any product queries, recursively rewrite them --- but
         * first check for recursion!
         */
        if (product_queries != NIL)
        {
            ListCell   *n;
            rewrite_event *rev;

            foreach(n, rewrite_events)
            {
                rev = (rewrite_event *) lfirst(n);
                if (rev->relation == RelationGetRelid(rt_entry_relation) &&
                    rev->event == event)
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                             errmsg("infinite recursion detected in rules for relation \"%s\"",
                                    RelationGetRelationName(rt_entry_relation))));
            }

            rev = (rewrite_event *) palloc(sizeof(rewrite_event));
            rev->relation = RelationGetRelid(rt_entry_relation);
            rev->event = event;
            rewrite_events = lcons(rev, rewrite_events);

            foreach(n, product_queries)
            {
                Query       *pt = (Query *) lfirst(n);
                List       *newstuff;

                newstuff = RewriteQuery(pt, rewrite_events);
                rewritten = list_concat(rewritten, newstuff);
            }

            rewrite_events = list_delete_first(rewrite_events);
        }

        /*
         * If there is an INSTEAD, and the original query has a RETURNING, we
         * have to have found a RETURNING in the rule(s), else fail. (Because
         * DefineQueryRewrite only allows RETURNING in unconditional INSTEAD
         * rules, there's no need to worry whether the substituted RETURNING
         * will actually be executed --- it must be.)
         */
        if ((instead || qual_product != NULL) &&
            parsetree->returningList &&
            !returning)
        {
            switch (event)
            {
                case CMD_INSERT:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot perform INSERT RETURNING on relation \"%s\"",
                                    RelationGetRelationName(rt_entry_relation)),
                             errhint("You need an unconditional ON INSERT DO INSTEAD rule with a RETURNING clause.")));
                    break;
                case CMD_UPDATE:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot perform UPDATE RETURNING on relation \"%s\"",
                                    RelationGetRelationName(rt_entry_relation)),
                             errhint("You need an unconditional ON UPDATE DO INSTEAD rule with a RETURNING clause.")));
                    break;
                case CMD_DELETE:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("cannot perform DELETE RETURNING on relation \"%s\"",
                                    RelationGetRelationName(rt_entry_relation)),
                             errhint("You need an unconditional ON DELETE DO INSTEAD rule with a RETURNING clause.")));
                    break;
                default:
                    elog(ERROR, "unrecognized commandType: %d",
                         (int) event);
                    break;
            }
        }

        /*
         * Updatable views are supported by ON CONFLICT, so don't prevent that
         * case from proceeding
         */
        if (parsetree->onConflict &&
            (product_queries != NIL || hasUpdate) &&
            !updatableview)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("INSERT with ON CONFLICT clause cannot be used with table that has INSERT or UPDATE rules")));

        heap_close(rt_entry_relation, NoLock);
#ifdef PGXC
        }
        else
        {
            foreach(pt_cell, parsetree_list)
            {
                Query  *query;

                query = (Query *)lfirst(pt_cell);

                /*
                 * Collect and apply the appropriate rules.
                 */
                locks = matchLocks(event, rt_entry_relation->rd_rules,
                               result_relation, query, &hasUpdate);

                if (locks != NIL)
                {
                    List       *product_queries = NIL;

                    if (IS_PGXC_COORDINATOR)
                        product_queries = fireRules(query,
                                        result_relation,
                                        event,
                                        locks,
                                        &instead,
                                        &returning,
                                        &qual_product);

                    qual_product_list = lappend(qual_product_list,  qual_product);

                    /*
                     * If we got any product queries, recursively rewrite them --- but
                     * first check for recursion!
                     */
                    if (product_queries != NIL)
                    {
                        ListCell   *n;
                        rewrite_event *rev;

                        foreach(n, rewrite_events)
                        {
                            rev = (rewrite_event *) lfirst(n);
                            if (rev->relation == RelationGetRelid(rt_entry_relation) &&
                                rev->event == event)
                                ereport(ERROR,
                                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                         errmsg("infinite recursion detected in rules for relation \"%s\"",
                                       RelationGetRelationName(rt_entry_relation))));
                        }

                        rev = (rewrite_event *) palloc(sizeof(rewrite_event));
                        rev->relation = RelationGetRelid(rt_entry_relation);
                        rev->event = event;
                        rewrite_events = lcons(rev, rewrite_events);

                        foreach(n, product_queries)
                        {
                            Query       *pt = (Query *) lfirst(n);
                            List       *newstuff;

                            newstuff = RewriteQuery(pt, rewrite_events);
                            rewritten = list_concat(rewritten, newstuff);
                        }

                        rewrite_events = list_delete_first(rewrite_events);
                    }
                }

                /*
                 * If there is an INSTEAD, and the original query has a RETURNING, we
                 * have to have found a RETURNING in the rule(s), else fail. (Because
                 * DefineQueryRewrite only allows RETURNING in unconditional INSTEAD
                 * rules, there's no need to worry whether the substituted RETURNING
                 * will actually be executed --- it must be.)
                 */
                if ((instead || qual_product != NULL) &&
                    query->returningList &&
                    !returning)
                {
                    switch (event)
                    {
                        case CMD_INSERT:
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("cannot perform INSERT RETURNING on relation \"%s\"",
                                         RelationGetRelationName(rt_entry_relation)),
                                     errhint("You need an unconditional ON INSERT DO INSTEAD rule with a RETURNING clause.")));
                            break;
                        case CMD_UPDATE:
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("cannot perform UPDATE RETURNING on relation \"%s\"",
                                         RelationGetRelationName(rt_entry_relation)),
                                     errhint("You need an unconditional ON UPDATE DO INSTEAD rule with a RETURNING clause.")));
                            break;
                        case CMD_DELETE:
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("cannot perform DELETE RETURNING on relation \"%s\"",
                                         RelationGetRelationName(rt_entry_relation)),
                                     errhint("You need an unconditional ON DELETE DO INSTEAD rule with a RETURNING clause.")));
                            break;
                        default:
                            elog(ERROR, "unrecognized commandType: %d",
                                 (int) event);
                            break;
                    }
                }
            }

            heap_close(rt_entry_relation, NoLock);
        }
    }

    if (parsetree_list == NIL)
    {
#endif
    /*
     * For INSERTs, the original query is done first; for UPDATE/DELETE, it is
     * done last.  This is needed because update and delete rule actions might
     * not do anything if they are invoked after the update or delete is
     * performed. The command counter increment between the query executions
     * makes the deleted (and maybe the updated) tuples disappear so the scans
     * for them in the rule actions cannot find them.
     *
     * If we found any unqualified INSTEAD, the original query is not done at
     * all, in any form.  Otherwise, we add the modified form if qualified
     * INSTEADs were found, else the unmodified form.
     */
    if (!instead)
    {
        if (parsetree->commandType == CMD_INSERT)
        {
            if (qual_product != NULL)
                rewritten = lcons(qual_product, rewritten);
            else
                rewritten = lcons(parsetree, rewritten);
        }
        else
        {
            if (qual_product != NULL)
                rewritten = lappend(rewritten, qual_product);
            else
                rewritten = lappend(rewritten, parsetree);
        }
    }
#ifdef PGXC
    }
    else
    {
        int query_no = 0;

        foreach(pt_cell, parsetree_list)
        {

            Query    *query = NULL;
            Query    *qual = NULL;

            query = (Query *)lfirst(pt_cell);
            if (!instead)
            {
                 if (qual_product_list)
                    qual = (Query *)list_nth(qual_product_list,
                                         query_no);

                if (query->commandType == CMD_INSERT)
                {
                    if (qual != NULL)
                        rewritten = lcons(qual, rewritten);
                    else
                        rewritten = lcons(query, rewritten);
                }
                else
                {
                    if (qual != NULL)
                        rewritten = lappend(rewritten, qual);
                    else
                        rewritten = lappend(rewritten, query);
                }
            }
            query_no++;
        }
    }
#endif

    /*
     * If the original query has a CTE list, and we generated more than one
     * non-utility result query, we have to fail because we'll have copied the
     * CTE list into each result query.  That would break the expectation of
     * single evaluation of CTEs.  This could possibly be fixed by
     * restructuring so that a CTE list can be shared across multiple Query
     * and PlannableStatement nodes.
     */
    if (parsetree->cteList != NIL)
    {
        int            qcount = 0;

        foreach(lc1, rewritten)
        {
            Query       *q = (Query *) lfirst(lc1);

            if (q->commandType != CMD_UTILITY)
                qcount++;
        }
        if (qcount > 1)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("WITH cannot be used in a query that is rewritten by rules into multiple queries")));
    }

    return rewritten;
}


/*
 * QueryRewrite -
 *      Primary entry point to the query rewriter.
 *      Rewrite one query via query rewrite system, possibly returning 0
 *      or many queries.
 *
 * NOTE: the parsetree must either have come straight from the parser,
 * or have been scanned by AcquireRewriteLocks to acquire suitable locks.
 */
List *
QueryRewrite(Query *parsetree)
{
    uint32        input_query_id = parsetree->queryId;
    List       *querylist;
    List       *results;
    ListCell   *l;
    CmdType        origCmdType;
    bool        foundOriginalQuery;
    Query       *lastInstead;

    /*
     * This function is only applied to top-level original queries
     */
    Assert(parsetree->querySource == QSRC_ORIGINAL);
    Assert(parsetree->canSetTag);

    /*
     * Step 1
     *
     * Apply all non-SELECT rules possibly getting 0 or many queries
     */
    querylist = RewriteQuery(parsetree, NIL);

    /*
     * Step 2
     *
     * Apply all the RIR rules on each query
     *
     * This is also a handy place to mark each query with the original queryId
     */
    results = NIL;
    foreach(l, querylist)
    {
        Query       *query = (Query *) lfirst(l);

        query = fireRIRrules(query, NIL, false);

        query->queryId = input_query_id;

        results = lappend(results, query);
    }

    /*
     * Step 3
     *
     * Determine which, if any, of the resulting queries is supposed to set
     * the command-result tag; and update the canSetTag fields accordingly.
     *
     * If the original query is still in the list, it sets the command tag.
     * Otherwise, the last INSTEAD query of the same kind as the original is
     * allowed to set the tag.  (Note these rules can leave us with no query
     * setting the tag.  The tcop code has to cope with this by setting up a
     * default tag based on the original un-rewritten query.)
     *
     * The Asserts verify that at most one query in the result list is marked
     * canSetTag.  If we aren't checking asserts, we can fall out of the loop
     * as soon as we find the original query.
     */
    origCmdType = parsetree->commandType;
    foundOriginalQuery = false;
    lastInstead = NULL;

    foreach(l, results)
    {
        Query       *query = (Query *) lfirst(l);

        if (query->querySource == QSRC_ORIGINAL)
        {
            Assert(query->canSetTag);
#ifndef PGXC
            Assert(!foundOriginalQuery);
#endif
            foundOriginalQuery = true;
#ifndef USE_ASSERT_CHECKING
            break;
#endif
        }
        else
        {
            Assert(!query->canSetTag);
            if (query->commandType == origCmdType &&
                (query->querySource == QSRC_INSTEAD_RULE ||
                 query->querySource == QSRC_QUAL_INSTEAD_RULE))
                lastInstead = query;
        }
    }

    if (!foundOriginalQuery && lastInstead != NULL)
        lastInstead->canSetTag = true;

    return results;
}

#ifdef PGXC
/*
 * Rewrite the CREATE TABLE AS and SELECT INTO queries as a
 * INSERT INTO .. SELECT query. The target table must be created first using
 * utility command processing. This takes care of creating the target table on
 * all the Coordinators and the Datanodes.
 */
List *
QueryRewriteCTAS(Query *parsetree,
				 ParserSetupHook parserSetup,
				 void *parserSetupArg,
				 QueryEnvironment *queryEnv,
				 Oid *paramTypes,
				 int numParams)
{
	RangeVar *relation;
	CreateStmt *create_stmt;
	PlannedStmt *wrapper;
	List *tableElts = NIL;
	StringInfoData cquery;
	ListCell *col;
	Query *cparsetree;
	List *raw_parsetree_list, *tlist;
	char *selectstr;
	CreateTableAsStmt *stmt;
	IntoClause *into;
	ListCell *lc;
	const int InvalidLevel = -1;
	int old_level = InvalidLevel;

    if (parsetree->commandType != CMD_UTILITY ||
        !IsA(parsetree->utilityStmt, CreateTableAsStmt))
        elog(ERROR, "Unexpected commandType or intoClause is not set properly");

    /* Get the target table */
    stmt = (CreateTableAsStmt *) parsetree->utilityStmt;

    if (stmt->relkind == OBJECT_MATVIEW)
        return list_make1(parsetree);

    relation = stmt->into->rel;

    if (stmt->if_not_exists)
    {
        Oid            nspid;

        nspid = RangeVarGetCreationNamespace(stmt->into->rel);

        if (get_relname_relid(stmt->into->rel->relname, nspid))
        {
            ereport(NOTICE,
                    (errcode(ERRCODE_DUPLICATE_TABLE),
                     errmsg("relation \"%s\" already exists, skipping",
                            stmt->into->rel->relname)));
            return NIL;
        }
    }

    /* Start building a CreateStmt for creating the target table */
    create_stmt = makeNode(CreateStmt);
    create_stmt->relation = relation;
    create_stmt->islocal = stmt->islocal;
    create_stmt->if_not_exists = stmt->if_not_exists;
    into = stmt->into;

    /* Obtain the target list of new table */
    Assert(IsA(stmt->query, Query));
    cparsetree = (Query *) stmt->query;
    tlist = cparsetree->targetList;

    /*
     * Based on the targetList, populate the column information for the target
     * table. If a column name list was specified in CREATE TABLE AS, override
     * the column names derived from the query. (Too few column names are OK, too
     * many are not.).
     */
    lc = list_head(into->colNames);
    foreach(col, tlist)
    {
        TargetEntry *tle = (TargetEntry *)lfirst(col);
        ColumnDef   *coldef;
        TypeName    *typename;

        /* Ignore junk columns from the targetlist */
        if (tle->resjunk)
            continue;

        coldef = makeNode(ColumnDef);
        typename = makeNode(TypeName);

        /* Take the column name specified if any */
        if (lc)
        {
            coldef->colname = strVal(lfirst(lc));
            lc = lnext(lc);
        }
        else
            coldef->colname = pstrdup(tle->resname);

        coldef->inhcount = 0;
        coldef->is_local = true;
        coldef->is_not_null = false;
        coldef->raw_default = NULL;
        coldef->cooked_default = NULL;
        coldef->constraints = NIL;

        /*
         * Set typeOid and typemod. The name of the type is derived while
         * generating query
         */
        typename->typeOid = exprType((Node *)tle->expr);
        typename->typemod = exprTypmod((Node *)tle->expr);

        coldef->typeName = typename;

        tableElts = lappend(tableElts, coldef);
    }

    if (lc != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("CREATE TABLE AS specifies too many column names")));

    /*
     * Set column information and the distribution mechanism (which will be
     * NULL for SELECT INTO and the default mechanism will be picked)
     */
    create_stmt->tableElts = tableElts;
    create_stmt->distributeby = stmt->into->distributeby;
    create_stmt->subcluster = stmt->into->subcluster;

    create_stmt->tablespacename = stmt->into->tableSpaceName;
    create_stmt->oncommit = stmt->into->onCommit;
    create_stmt->options = stmt->into->options;

    /*
     * Check consistency of arguments
     */
    if (create_stmt->oncommit != ONCOMMIT_NOOP
            && create_stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                 errmsg("ON COMMIT can only be used on temporary tables")));

    /* Get a copy of the parsetree which we can freely modify  */
    cparsetree = copyObject(parsetree);

    /*
     * Now build a utility statement in order to run the CREATE TABLE DDL on
     * the local and remote nodes. We keep others fields as it is since they
     * are ignored anyways by deparse_query.
     */
    cparsetree->commandType = CMD_UTILITY;
    cparsetree->utilityStmt = (Node *) create_stmt;

    initStringInfo(&cquery);
    deparse_query(cparsetree, &cquery, NIL, false, false);


    /* finally, wrap it in a dummy PlannedStmt */
    wrapper = makeNode(PlannedStmt);
    wrapper->commandType = CMD_UTILITY;
    wrapper->canSetTag = false;
    wrapper->utilityStmt = (Node *) create_stmt;
    wrapper->stmt_location = -1;
    wrapper->stmt_len = -1;

    /* Finally, fire off the query to run the DDL */
    ProcessUtility(wrapper, cquery.data, PROCESS_UTILITY_QUERY,
            NULL, NULL, NULL, false, NULL);

	/* Use new snapshot for insert and update the snapshot status. */
	if (ActiveSnapshotSet())
	{
		old_level = GetActiveSnapshotLevel();
        PopActiveSnapshot();
	}
	
    PushActiveSnapshot(GetTransactionSnapshot());
	UpdateActiveSnapshotStatus(S_FOR_CTAS);


    /*
	 * Only snapshot replacement is performed to prevent abnormal snapshot clearing caused by sub-transactions.
	 * Active snapshots set by this subtransaction will be cleared.
	 */
	if (old_level != InvalidLevel)
	{
		SetActiveSnapshotLevel(old_level);
	}

	/*
     * Now fold the CTAS statement into an INSERT INTO statement. The
     * utility is no more required.
     */
    parsetree->utilityStmt = NULL;

    /* Get the SELECT query string */
    initStringInfo(&cquery);
    deparse_query((Query *)stmt->query, &cquery, NIL, false, false);
    selectstr = pstrdup(cquery.data);

    /* Now, finally build the INSERT INTO statement */
    initStringInfo(&cquery);

    appendStringInfo(&cquery, "INSERT INTO %s.%s",
                quote_identifier(get_namespace_name(RangeVarGetCreationNamespace(relation))),
                quote_identifier(relation->relname));

    appendStringInfo(&cquery, " %s %s", selectstr,
            into->skipData ? "LIMIT 0" : "");

	raw_parsetree_list = pg_parse_query(cquery.data);
	
	if (parserSetup != NULL)
		return pg_analyze_and_rewrite_params(linitial(raw_parsetree_list), cquery.data,
		                                     parserSetup, parserSetupArg, queryEnv);
	else
		return pg_analyze_and_rewrite(linitial(raw_parsetree_list), cquery.data,
		                              paramTypes, numParams, queryEnv);

}
#endif
