/*-------------------------------------------------------------------------
 *
 * preptlist.c
 *	  Routines to preprocess the parse tree target list
 *
 * For an INSERT, the targetlist must contain an entry for each attribute of
 * the target relation in the correct order.
 *
 * For an UPDATE, the targetlist just contains the expressions for the new
 * column values.
 *
 * For UPDATE and DELETE queries, the targetlist must also contain "junk"
 * tlist entries needed to allow the executor to identify the rows to be
 * updated or deleted; for example, the ctid of a heap row.  (The planner
 * adds these; they're not in what we receive from the planner/rewriter.)
 *
 * For all query types, there can be additional junk tlist entries, such as
 * sort keys, Vars needed for a RETURNING list, and row ID information needed
 * for SELECT FOR UPDATE locking and/or EvalPlanQual checking.
 *
 * The query rewrite phase also does preprocessing of the targetlist (see
 * rewriteTargetListIU).  The division of labor between here and there is
 * partially historical, but it's not entirely arbitrary.  The stuff done
 * here is closely connected to physical access to tables, whereas the
 * rewriter's work is more concerned with SQL semantics.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/prep/preptlist.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#ifdef XCP
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#endif
#ifdef __OPENTENBASE__
#include "catalog/pgxc_class.h"
#include "optimizer/planmain.h"
#include "parser/parse_merge.h"
#include "rewrite/rewriteHandler.h"
#endif
#ifdef _PG_ORCL_
#include "catalog/heap.h"
#include "commands/sequence.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "utils/lsyscache.h"
#endif
#ifdef __OPENTENBASE_C__
#include "catalog/pg_inherits_fn.h"
#include "executor/execFragment.h"
#endif

static List *extract_update_colnos(List *tlist);
static List *expand_targetlist(List *tlist, int command_type,
#ifdef _PG_ORCL_
							   Index result_relation, Relation rel, PlannerInfo *root);
#else
							   Index result_relation, Relation rel);
#endif

static void rewriteTargetListMerge(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte,
                                   Relation target_relation);

static void add_merge_source_targets(PlannerInfo *root, Index rtindex, Node *exprs);
/*
 * preprocess_targetlist
 *	  Driver for preprocessing the parse tree targetlist.
 *
 * The preprocessed targetlist is returned in root->processed_tlist.
 * Also, if this is an UPDATE, we return a list of target column numbers
 * in root->update_colnos.  (Resnos in processed_tlist will be consecutive,
 * so do not look at that to find out which columns are targets!)
 *
 * As a side effect, if there's an ON CONFLICT UPDATE clause, its targetlist
 * is also preprocessed (and updated in-place).
 */
void
preprocess_targetlist(PlannerInfo *root)
{
	Query	   *parse = root->parse;
	int			result_relation = parse->resultRelation;
	List	   *range_table = parse->rtable;
	CmdType		command_type = parse->commandType;
	RangeTblEntry *target_rte = NULL;
	Relation	target_relation = NULL;
	List	   *tlist;
	ListCell      *lc;
	/* 1 for update estore, -1 for update row, 0 for not update */
	int16		update_info = 0;

	/*
	 * If there is a result relation, open it so we can look for missing
	 * columns and so on.  We assume that previous code already acquired at
	 * least AccessShareLock on the relation, so we need no lock here.
	 */
	if (result_relation)
	{
		target_rte = rt_fetch(result_relation, range_table);

		/*
		 * Sanity check: it'd better be a real relation not, say, a subquery.
		 * Else parser or rewriter messed up.
		 */
		if (target_rte->rtekind != RTE_RELATION)
			elog(ERROR, "result relation must be a regular relation");

		target_relation = heap_open(target_rte->relid, NoLock);
	}
	else
	{
		/* begin ora_compatible */
		if (PG_MODE || parse->multi_inserts == NULL)
			Assert(command_type == CMD_SELECT);
		/* end ora_compatible */
	}

	/*
	 * In an INSERT, the executor expects the targetlist to match the exact
	 * order of the target table's attributes, including entries for
	 * attributes not mentioned in the source query.
	 *
	 * In an UPDATE, we don't rearrange the tlist order, but we need to make a
	 * separate list of the target attribute numbers, in tlist order, and then
	 * renumber the processed_tlist entries to be consecutive.
	 *
	 * OPENTENBASE: Unfortunately, UPDATE on Estore still need full targetlist for now.
	 * It can be optimized in the future if necessary.
	 */
	tlist = parse->targetList;
	if (command_type == CMD_INSERT &&
			(PG_MODE || parse->multi_inserts == NULL))
		tlist = expand_targetlist(tlist, command_type,
#ifdef _PG_ORCL_
								  result_relation, target_relation, root);
#else
								  result_relation, target_relation);
#endif
	else if (command_type == CMD_UPDATE)
	{
#ifdef __OPENTENBASE_C__
		update_info = check_rel_for_uppdate(target_relation);
		if (update_info > 0)
			tlist = expand_targetlist(tlist, command_type,
#ifdef _PG_ORCL_
									  result_relation, target_relation, root);
#else
									  result_relation, target_relation);
#endif
		else
#endif
		root->update_colnos = extract_update_colnos(tlist);
	}

	/*
	 * For non-inherited UPDATE/DELETE, register any junk column(s) needed to
	 * allow the executor to identify the rows to be updated or deleted.  In
	 * the inheritance case, we do nothing now, leaving this to be dealt with
	 * when expand_inherited_rtentry() makes the leaf target relations.  (But
	 * there might not be any leaf target relations, in which case we must do
	 * this in distribute_row_identity_vars().)
	 */
	if ((command_type == CMD_UPDATE || command_type == CMD_DELETE) &&
		!target_rte->inh)
	{
		/* row-identity logic expects to add stuff to processed_tlist */
		root->processed_tlist = tlist;
		add_row_identity_columns(root, result_relation,
								 target_rte, target_relation);
		tlist = root->processed_tlist;
	}

	/*
	 * For MERGE we also need to handle the target list for each INSERT and
	 * UPDATE action separately.  In addition, we examine the qual of each
	 * action and add any Vars there (other than those of the target rel) to
	 * the subplan targetlist.
	 */
	if (command_type == CMD_MERGE)
    {
        ListCell *l = NULL;
        JoinExpr *joinExpr;
		List     *join_var_list = NIL;

		/*
		 * For MERGE, add any junk column(s) needed to allow the executor to
		 * identify the rows to be updated or deleted, with different
		 * handling for partitioned tables.
		 */
		root->processed_tlist = tlist;
		rewriteTargetListMerge(root, result_relation, target_rte, target_relation);

		/*
		 * We only add VARs in JOIN CONDITION from data source.
		 * If data source is a subquery with empty FROM list, it's be deleted
		 * when pull up subquery. Just skip.
		 */
		if(IsA(linitial(parse->jointree->fromlist), JoinExpr))
		{
			joinExpr = linitial_node(JoinExpr, parse->jointree->fromlist);
			add_merge_source_targets(root, result_relation, joinExpr->quals);
			join_var_list = pull_var_clause(joinExpr->quals, PVC_INCLUDE_PLACEHOLDERS |
															 PVC_INCLUDE_CNEXPR);
		}

		update_info = check_rel_for_uppdate(target_relation);

		/*
         * For MERGE command, handle targetlist of each MergeAction separately.
         * Give the same treatment to MergeAction->targetList as we would have
         * given to a regular INSERT/UPDATE/DELETE.
         */
        foreach (l, parse->mergeActionList)
        {
            MergeAction *action = (MergeAction *) lfirst(l);
			List        *exprList = NIL;

			if (action->commandType == CMD_UPDATE) {
				checkMergeUpdateOnJoinKey(parse, action->targetList, join_var_list);
			}

			if (action->commandType == CMD_INSERT ||
			    (update_info > 0 && action->commandType == CMD_UPDATE))
			{
				action->targetList = expand_targetlist(action->targetList, action->commandType,
#ifdef _PG_ORCL_
				                                       result_relation, target_relation, root);
#else
				                                       result_relation, target_relation);

#endif
			if (IS_PGXC_LOCAL_COORDINATOR)
				action->rmt_targetList = expand_targetlist(action->rmt_targetList, action->commandType,
#ifdef _PG_ORCL_
					                                       result_relation, target_relation, root);
#else
					                                       result_relation, target_relation);

#endif

				if (action->commandType == CMD_UPDATE)
				{
					int          junkIdx = list_length(action->targetList);
					ListCell    *junkResCell;
					TargetEntry *junkTle;
					junkResCell = list_nth_cell(root->processed_tlist, junkIdx);
					while (junkResCell)
					{
						junkTle = (TargetEntry *) lfirst(junkResCell);
						if (!junkTle->resjunk)
							break;
						action->targetList = lappend(action->targetList, copyObject(junkTle));
						junkResCell = lnext(junkResCell);
					}
				}
			}
			else if (action->commandType == CMD_UPDATE)
			{
				action->updateColnos = extract_update_colnos(action->targetList);
			}
			/*
			 * Add resjunk entries for any Vars and PlaceHolderVars used in
			 * each action's targetlist and WHEN condition that belong to
			 * relations other than the target.  We don't expect to see any
			 * aggregates or window functions here.
			 */
			exprList = lappend(exprList, action->targetList);
			exprList = lappend(exprList, action->qual);
			if (action->commandType == CMD_UPDATE && action->deleteAction)
			{
				exprList = lappend(exprList, action->deleteAction->qual);
			}
			add_merge_source_targets(
				root,
				result_relation,
				(Node *) exprList);
		}
		tlist = root->processed_tlist;
    }

#ifdef XCP
	/*
	 * If target relation is specified set distribution of the plan
	 */
	if (IS_PGXC_COORDINATOR && result_relation)
	{
		Relation rel = heap_open(getrelid(result_relation, range_table), NoLock);
		RelationLocInfo *rel_loc_info = rel->rd_locator_info;

		/* Is target table distributed ? */
		if (rel_loc_info)
		{
			Distribution *distribution = makeNode(Distribution);
			ListCell *lc;

			distribution->distributionType = rel_loc_info->locatorType;
			foreach(lc, rel_loc_info->rl_nodeList)
			{
				distribution->nodes = bms_add_member(distribution->nodes,
													 lfirst_int(lc));
			}
			distribution->restrictNodes = NULL;

			if (rel_loc_info->nDisAttrs > 0)
			{
				/*
				 * For INSERT and UPDATE plan tlist is matching the target table
				 * layout
				 */
				if (command_type == CMD_INSERT || update_info > 0 || command_type == CMD_MERGE)
				{
					TargetEntry *keyTle;
					int i = 0;
					
					distribution->disExprs = (Node **) palloc(sizeof(Node *) * rel_loc_info->nDisAttrs);
					distribution->nExprs = rel_loc_info->nDisAttrs;
					for (i = 0; i < rel_loc_info->nDisAttrs; i++)
					{
						keyTle = (TargetEntry *) list_nth(tlist,
						                                  rel_loc_info->disAttrNums[i] - 1);
						distribution->disExprs[i] = (Node *) keyTle->expr;
					}

					/*
					 * We can restrict the distribution if the expression
					 * is evaluated to a constant
					 */
					if (command_type == CMD_INSERT)
					{
						Oid 	keytype;
						Const  *constExpr = NULL;
						Datum   *disvalues = NULL;
						bool    *disisnulls = NULL;
						int     ndiscols = 0;
						/* for multi distributed columns */
						List *nodeList = NIL;
						Bitmapset *tmpset = bms_copy(distribution->nodes);
						Bitmapset *restrictinfo = NULL;
						Locator *locator;
						int *nodenums;
						int count;
						
						while ((i = bms_first_member(tmpset)) >= 0)
						{
							nodeList = lappend_int(nodeList, i);
						}
						bms_free(tmpset);
						
						ndiscols = rel_loc_info->nDisAttrs;
						disvalues = (Datum *) palloc(sizeof(Datum) * ndiscols);
						disisnulls = (bool *) palloc(sizeof(bool) * ndiscols);
						for (i = 0; i < ndiscols; i++)
						{
							keytype = exprType(distribution->disExprs[i]);
							constExpr = (Const *) eval_const_expressions(root,
							                                             distribution->disExprs[i]);
							if (!IsA(constExpr, Const) ||
							    constExpr->consttype != keytype)
							{
								list_free(nodeList);
								pfree(disvalues);
								pfree(disisnulls);
								goto END_restrict;
							}
							disisnulls[i] = constExpr->constisnull;
							disvalues[i] = constExpr->constvalue;
						}
#ifdef _MIGRATE_
						locator = createLocator(distribution->distributionType,
												RELATION_ACCESS_INSERT,
												LOCATOR_LIST_LIST,
												0,
												(void *) nodeList,
												(void **) &nodenums,
												false,
												rel_loc_info->groupId,
												rel_loc_info->disAttrTypes,
												rel_loc_info->disAttrNums,
												rel_loc_info->nDisAttrs, NULL);
#else
						locator = createLocator(distribution->distributionType,
												RELATION_ACCESS_INSERT,
												keytype,
												LOCATOR_LIST_LIST,
												0,
												(void *) nodeList,
												(void **) &nodenums,
												false, NULL);
#endif
						count = GET_NODES(locator,
										  disvalues, disisnulls, ndiscols,
										  NULL);
						for (i = 0; i < count; i++)
							restrictinfo = bms_add_member(restrictinfo, nodenums[i]);
						distribution->restrictNodes = restrictinfo;

						list_free(nodeList);
						pfree(disvalues);
						pfree(disisnulls);
						freeLocator(locator);
					}
				}
#ifdef __OPENTENBASE_C__
END_restrict:
#endif
				/*
				 * For UPDATE and DELETE we need to add the distribution key of
				 * the target table to the tlist, so distribution can be handled
				 * correctly through all the planning process.
				 *
				 * Specially, add distribution keys in front of original tlist
				 * as in expand_targetlist for CMD_INSERT.
				 */
				if (update_info < 0 || command_type == CMD_DELETE)
				{
					int		i;
					Form_pg_attribute att_tup;
					Var	   *var;

					root->processed_tlist = tlist;
					distribution->disExprs = (Node **)palloc(sizeof(Node *) * rel_loc_info->nDisAttrs);
					distribution->nExprs = rel_loc_info->nDisAttrs;
					for (i = 0; i < rel_loc_info->nDisAttrs; i++)
					{
						att_tup = TupleDescAttr(rel->rd_att, rel_loc_info->disAttrNums[i] - 1);

						var = makeVar(result_relation,
									  rel_loc_info->disAttrNums[i],
						              att_tup->atttypid,
									  att_tup->atttypmod,
						              att_tup->attcollation,
									  0);
						add_row_identity_var(root, var, result_relation, pstrdup(NameStr(att_tup->attname)));
						distribution->disExprs[i] = (Node *) var;
					}
					tlist = root->processed_tlist;
				}
			}
			else
			{
				distribution->disExprs = NULL;
				distribution->nExprs = 0;
			}

			root->distribution = distribution;
		}
		else
			root->distribution = NULL;

		heap_close(rel, NoLock);
	}
#endif

	/*
	 * Add necessary junk columns for rowmarked rels.  These values are needed
	 * for locking of rels selected FOR UPDATE/SHARE, and to do EvalPlanQual
	 * rechecking.  See comments for PlanRowMark in plannodes.h.  If you
	 * change this stanza, see also expand_inherited_rtentry(), which has to
	 * be able to add on junk columns equivalent to these.
	 *
	 * (Someday it might be useful to fold these resjunk columns into the
	 * row-identity-column management used for UPDATE/DELETE.  Today is not
	 * that day, however.  One notable issue is that it seems important that
	 * the whole-row Vars made here use the real table rowtype, not RECORD, so
	 * that conversion to/from child relations' rowtypes will happen.  Also,
	 * since these entries don't potentially bloat with more and more child
	 * relations, there's not really much need for column sharing.)
	 */
	foreach(lc, root->rowMarks)
	{
		PlanRowMark *rc = (PlanRowMark *) lfirst(lc);
		Var		   *var;
		char		resname[32];
		TargetEntry *tle;

		/* child rels use the same junk attrs as their parents */
		if (rc->rti != rc->prti)
			continue;

		if (rc->allMarkTypes & ~(1 << ROW_MARK_COPY))
		{
			/* Need to fetch TID */
			var = makeVar(rc->rti,
						  SelfItemPointerAttributeNumber,
						  TIDOID,
						  -1,
						  InvalidOid,
						  0);
			snprintf(resname, sizeof(resname), "ctid%u", rc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(tlist) + 1,
								  pstrdup(resname),
								  true);
			tlist = lappend(tlist, tle);

			/* Need to fetch another xc_node_id */
			var = makeVar(rc->rti,
						  XC_NodeIdAttributeNumber,
						  INT4OID,
						  -1,
						  InvalidOid,
						  0);
			snprintf(resname, sizeof(resname), "xc_node_id%u", rc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(tlist) + 1,
								  pstrdup(resname),
								  true);
			tlist = lappend(tlist, tle);
		}
		if (rc->allMarkTypes & (1 << ROW_MARK_COPY))
		{
			/* Need the whole row as a junk var */
			var = makeWholeRowVar(rt_fetch(rc->rti, range_table),
								  rc->rti,
								  0,
								  false);
			snprintf(resname, sizeof(resname), "wholerow%u", rc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(tlist) + 1,
								  pstrdup(resname),
								  true);
			tlist = lappend(tlist, tle);
		}

		/* If parent of inheritance tree, always fetch the tableoid too. */
		if (rc->isParent)
		{
			var = makeVar(rc->rti,
						  TableOidAttributeNumber,
						  OIDOID,
						  -1,
						  InvalidOid,
						  0);
			snprintf(resname, sizeof(resname), "tableoid%u", rc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(tlist) + 1,
								  pstrdup(resname),
								  true);
			tlist = lappend(tlist, tle);
		}
	}

	/*
	 * If the query has a RETURNING list, add resjunk entries for any Vars
	 * used in RETURNING that belong to other relations.  We need to do this
	 * to make these Vars available for the RETURNING calculation.  Vars that
	 * belong to the result rel don't need to be added, because they will be
	 * made to refer to the actual heap tuple.
	 */
	if (parse->returningList && list_length(parse->rtable) > 1)
	{
		List	   *vars;
		ListCell   *l;

		vars = pull_var_clause((Node *) parse->returningList,
							   PVC_RECURSE_AGGREGATES |
							   PVC_RECURSE_WINDOWFUNCS |
							   PVC_INCLUDE_PLACEHOLDERS);
		foreach(l, vars)
		{
			Var		   *var = (Var *) lfirst(l);
			TargetEntry *tle;

			if (IsA(var, Var) &&
				var->varno == result_relation)
				continue;		/* don't need it */

			if (tlist_member((Expr *) var, tlist))
				continue;		/* already got it */

			tle = makeTargetEntry((Expr *) var,
								  list_length(tlist) + 1,
								  NULL,
								  true);

			tlist = lappend(tlist, tle);
		}
		list_free(vars);
	}

	root->processed_tlist = tlist;

	/*
	 * If there's an ON CONFLICT UPDATE clause, preprocess its targetlist too
	 * while we have the relation open.
	 */
	if (parse->onConflict)
		parse->onConflict->onConflictSet =
			expand_targetlist(parse->onConflict->onConflictSet,
							  CMD_UPDATE,
							  result_relation,
#ifdef _PG_ORCL_
							  target_relation, NULL);
#else
							  target_relation);
#endif

	if (target_relation)
		heap_close(target_relation, NoLock);
}

/*
 * extract_update_colnos
 * 		Extract a list of the target-table column numbers that
 * 		an UPDATE's targetlist wants to assign to, then renumber.
 *
 * The convention in the parser and rewriter is that the resnos in an
 * UPDATE's non-resjunk TLE entries are the target column numbers
 * to assign to.  Here, we extract that info into a separate list, and
 * then convert the tlist to the sequential-numbering convention that's
 * used by all other query types.
 */
static List *
extract_update_colnos(List *tlist)
{
	List	   *update_colnos = NIL;
	AttrNumber	nextresno = 1;
	ListCell   *lc;

	foreach(lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (!tle->resjunk)
			update_colnos = lappend_int(update_colnos, tle->resno);
		tle->resno = nextresno++;
	}
	return update_colnos;
}


/*****************************************************************************
 *
 *		TARGETLIST EXPANSION
 *
 *****************************************************************************/

extern int	NumDataNodes;


/*
 * expand_targetlist
 *	  Given a target list as generated by the parser and a result relation,
 *	  add targetlist entries for any missing attributes, and ensure the
 *	  non-junk attributes appear in proper field order.
 *
 * command_type is a bit of an archaism now: it's CMD_INSERT when we're
 * processing an INSERT, all right, but the only other use of this function
 * is for ON CONFLICT UPDATE tlists, for which command_type is CMD_UPDATE.
 */
static List *
expand_targetlist(List *tlist, int command_type,
#ifdef _PG_ORCL_
				  Index result_relation, Relation rel, PlannerInfo *root)
#else
				  Index result_relation, Relation rel)
#endif
{
	List	   *new_tlist = NIL;
	ListCell   *tlist_item;
	int			attrno,
				numattrs;

	tlist_item = list_head(tlist);

	/*
	 * The rewriter should have already ensured that the TLEs are in correct
	 * order; but we have to insert TLEs for any missing attributes.
	 *
	 * Scan the tuple description in the relation's relcache entry to make
	 * sure we have all the user attributes in the right order.
	 */
	numattrs = RelationGetNumberOfAttributes(rel);

	for (attrno = 1; attrno <= numattrs; attrno++)
	{
		Form_pg_attribute att_tup = TupleDescAttr(rel->rd_att, attrno - 1);
		TargetEntry *new_tle = NULL;

		if (tlist_item != NULL)
		{
			TargetEntry *old_tle = (TargetEntry *) lfirst(tlist_item);

			if (!old_tle->resjunk && old_tle->resno == attrno)
			{
				new_tle = old_tle;
				tlist_item = lnext(tlist_item);
			}
		}

		if (new_tle == NULL)
		{
			/*
			 * Didn't find a matching tlist entry, so make one.
			 *
			 * For INSERT, generate a NULL constant.  (We assume the rewriter
			 * would have inserted any available default value.) Also, if the
			 * column isn't dropped, apply any domain constraints that might
			 * exist --- this is to catch domain NOT NULL.
			 *
			 * For UPDATE, generate a Var reference to the existing value of
			 * the attribute, so that it gets copied to the new tuple. But
			 * generate a NULL for dropped columns (we want to drop any old
			 * values).
			 *
			 * When generating a NULL constant for a dropped column, we label
			 * it INT4 (any other guaranteed-to-exist datatype would do as
			 * well). We can't label it with the dropped column's datatype
			 * since that might not exist anymore.  It does not really matter
			 * what we claim the type is, since NULL is NULL --- its
			 * representation is datatype-independent.  This could perhaps
			 * confuse code comparing the finished plan to the target
			 * relation, however.
			 */
			Oid			atttype = att_tup->atttypid;
			int32		atttypmod = att_tup->atttypmod;
			Oid			attcollation = att_tup->attcollation;
			Node	   *new_expr;

			switch (command_type)
			{
				case CMD_INSERT:
					if (!att_tup->attisdropped)
					{
						new_expr = (Node *) makeConst(atttype,
													  #ifdef __OPENTENBASE_C__
													  atttypmod,
													  #else
													  -1,
													  #endif
													  attcollation,
													  att_tup->attlen,
													  (Datum) 0,
													  true, /* isnull */
													  att_tup->attbyval);

						new_expr = coerce_to_domain(new_expr,
													InvalidOid, -1,
													atttype,
													COERCION_IMPLICIT,
													COERCE_IMPLICIT_CAST,
													-1,
													false);
					}
					else
					{
						/* Insert NULL for dropped column */
						new_expr = (Node *) makeConst(INT4OID,
													  #ifdef __OPENTENBASE_C__
													  atttypmod,
													  #else
													  -1,
													  #endif
													  InvalidOid,
													  sizeof(int32),
													  (Datum) 0,
													  true, /* isnull */
													  true /* byval */ );
					}
					break;
				case CMD_UPDATE:
					if (!att_tup->attisdropped)
					{
						new_expr = (Node *) makeVar(result_relation,
													attrno,
													atttype,
													atttypmod,
													attcollation,
													0);
					}
					else
					{
						/* Insert NULL for dropped column */
						new_expr = (Node *) makeConst(INT4OID,
													  #ifdef __OPENTENBASE_C__
													  atttypmod,
													  #else
													  -1,
													  #endif
													  InvalidOid,
													  sizeof(int32),
													  (Datum) 0,
													  true, /* isnull */
													  true /* byval */ );
					}
					break;
				default:
					elog(ERROR, "unrecognized command_type: %d",
						 (int) command_type);
					new_expr = NULL;	/* keep compiler quiet */
					break;
			}

			new_tle = makeTargetEntry((Expr *) new_expr,
									  attrno,
									  pstrdup(NameStr(att_tup->attname)),
									  false);
		}

		new_tlist = lappend(new_tlist, new_tle);
	}

	/*
	 * The remaining tlist entries should be resjunk; append them all to the
	 * end of the new tlist, making sure they have resnos higher than the last
	 * real attribute.  (Note: although the rewriter already did such
	 * renumbering, we have to do it again here in case we are doing an UPDATE
	 * in a table with dropped columns, or an inheritance child table with
	 * extra columns.)
	 */
	while (tlist_item)
	{
		TargetEntry *old_tle = (TargetEntry *) lfirst(tlist_item);

		if (!old_tle->resjunk)
			elog(ERROR, "targetlist is not sorted correctly");
		/* Get the resno right, but don't copy unnecessarily */
		if (old_tle->resno != attrno)
		{
			old_tle = flatCopyTargetEntry(old_tle);
			if (OidIsValid(root->target_rel_gSeqId) &&
				root->tlist_rid_indx == old_tle->resno &&
				is_multi_insert(root))
				root->tlist_rid_indx = attrno;
			old_tle->resno = attrno;
		}
		new_tlist = lappend(new_tlist, old_tle);
		attrno++;
		tlist_item = lnext(tlist_item);
	}


	return new_tlist;
}


/*
 * Locate PlanRowMark for given RT index, or return NULL if none
 *
 * This probably ought to be elsewhere, but there's no very good place
 */
PlanRowMark *
get_plan_rowmark(List *rowmarks, Index rtindex)
{
	ListCell   *l;

	foreach(l, rowmarks)
	{
		PlanRowMark *rc = (PlanRowMark *) lfirst(l);

		if (rc->rti == rtindex)
			return rc;
	}
	return NULL;
}

static void
rewriteTargetListMerge(PlannerInfo *root, Index rtindex,
						 RangeTblEntry *target_rte,
						 Relation target_relation)
{
    /*
     * The rewriter should have already ensured that the TLEs are in correct
     * order; but we have to insert TLEs for any missing attributes.
     *
     * Scan the tuple description in the relation's relcache entry to make
     * sure we have all the user attributes in the right order.  We assume
     * that the rewriter already acquired at least AccessShareLock on the
     * relation, so we need no lock here.
     */

	root->processed_tlist = expandTargetTL(root->processed_tlist, target_rte, rtindex);

	if (target_rte->inh)
	{
		Var *rrvar;
		rrvar = makeVar(rtindex, TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0);
		add_row_identity_var(root, rrvar, rtindex, "tableoid");
	}
	add_row_identity_columns(root, rtindex, target_rte, target_relation);
}

/*
 * add_merge_source_targets
 *
 * add VARs from the data source to targetList and mergeSourceTargetList for MERGE
 */
static void
add_merge_source_targets(PlannerInfo *root, Index rtindex, Node *exprs)
{
    List     *vars;
    ListCell *lc;

	vars = pull_var_clause(exprs, PVC_INCLUDE_PLACEHOLDERS |
								  PVC_INCLUDE_CNEXPR);
    foreach (lc, vars)
    {
        Var         *var = (Var *)lfirst(lc);
        TargetEntry *tle;

        if (IsA(var, Var) && var->varno == rtindex)
            continue; /* aleady add to targetlist, don't need it in mergeSourceTargetList */

        if (tlist_member((Expr *)var, root->processed_tlist))
            continue; /* already got it */

		tle = makeTargetEntry((Expr *)var, list_length(root->processed_tlist) + 1, NULL, true);
        root->processed_tlist = lappend(root->processed_tlist, tle);
    }
    list_free(vars);
}


PlannerInfo *
get_top_root(PlannerInfo *root)
{
	PlannerInfo *top_root = root;

	while (top_root->parent_root)
		top_root = top_root->parent_root;

	return top_root;
}

bool
is_multi_insert(PlannerInfo *root)
{
	PlannerInfo *top_root = get_top_root(root);

	if (top_root->parse && top_root->parse->multi_inserts != NULL)
		return true;

	return false;
}
