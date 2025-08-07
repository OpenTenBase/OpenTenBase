/*-------------------------------------------------------------------------
 *
 * pgxcship.c
 *		Routines to evaluate expression shippability to remote nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/pgxcship.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#ifdef PGXC
#include "catalog/pg_trigger.h"
#include "catalog/pgxc_class.h"
#endif
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/proclang.h"
#include "commands/trigger.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pgxcship.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "pgxc/locator.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "miscadmin.h"
#ifdef __OPENTENBASE__
#include "catalog/pg_constraint.h"
#include "commands/proclang.h"
#include "access/htup_details.h"
#include "optimizer/pathnode.h"
#endif
/*
 * Shippability_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct
{
	bool		sc_for_expr;		/* if false, the we are checking shippability
									 * of the Query, otherwise, we are checking
									 * shippability of a stand-alone expression.
									 */
	Bitmapset	*sc_shippability;	/* The conditions for (un)shippability of the
									 * query.
									 */
	Query		*sc_query;			/* the query being analysed for FQS */
	int			sc_query_level;		/* level of the query */
	int			sc_max_varlevelsup;	/* maximum upper level referred to by any
									 * variable reference in the query. If this
									 * value is greater than 0, the query is not
									 * shippable, if shipped alone.
									 */
	ExecNodes	*sc_exec_nodes;		/* nodes where the query should be executed */
	ExecNodes	*sc_subquery_en;	/* ExecNodes produced by merging the ExecNodes
									 * for individual subqueries. This gets
									 * ultimately merged with sc_exec_nodes.
									 */
	bool		sc_groupby_has_distcol;	/* GROUP BY clause has distribution column */

	ParamListInfo boundParams;
	int			cached_param_num;
} Shippability_context;

/*
 * ShippabilityStat
 * List of reasons why a query/expression is not shippable to remote nodes.
 */
typedef enum
{
	SS_UNSHIPPABLE_EXPR = 0,	/* it has unshippable expression */
	SS_NEED_SINGLENODE,			/* Has expressions which can be evaluated when
								 * there is only a single node involved.
								 * Athought aggregates too fit in this class, we
								 * have a separate status to report aggregates,
								 * see below.
								 */
	SS_NEEDS_COORD,				/* the query needs Coordinator */
	SS_VARLEVEL,				/* one of its subqueries has a VAR
								 * referencing an upper level query
								 * relation
								 */
	SS_NO_NODES,				/* no suitable nodes can be found to ship
								 * the query
								 */
	SS_UNSUPPORTED_EXPR,		/* it has expressions currently unsupported
								 * by FQS, but such expressions might be
								 * supported by FQS in future
								 */
	SS_HAS_AGG_EXPR,			/* it has aggregate expressions */
	SS_UNSHIPPABLE_TYPE,		/* the type of expression is unshippable */
	SS_UNSHIPPABLE_TRIGGER,		/* the type of trigger is unshippable */
	SS_UPDATES_DISTRIBUTION_COLUMN,	/* query updates the distribution column */
	SS_NEED_FUNC_CONVERT_TO_PARAM,		/* exist func expression of distribution column,
								 		 * we should convert the the expr to param
								 		 */
	SS_HAS_UNSHIPPABLE_SUBQUERY,
	SS_NEED_PROCESS_IN_CN,		/* functions should be called in CN */
	SS_REPLICATED_ONLY			/* expressions that only allowed in replicated distribution */
} ShippabilityStat;

extern void PoolPingNodes(void);

#ifdef _PG_ORCL_
static bool pgxc_is_withfunc_shippable(List *funcnsp, int funcid);
#endif

/* Check equijoin conditions on given relations */
static Expr *pgxc_find_dist_equijoin_qual(Relids varnos_1, Relids varnos_2,
								Oid distcol_type, Node *quals, List *rtable);
/* Merge given execution nodes based on join shippability conditions */
static ExecNodes *pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2);
/* Check if given Query includes distribution column */
static bool pgxc_query_has_distcolgrouping(Query *query);

/* Manipulation of shippability reason */
static bool pgxc_test_shippability_reason(Shippability_context *context,
										  ShippabilityStat reason);
static void pgxc_set_shippability_reason(Shippability_context *context,
										 ShippabilityStat reason);
static void pgxc_reset_shippability_reason(Shippability_context *context,
										   ShippabilityStat reason);

/* Evaluation of shippability */
static bool pgxc_shippability_walker(Node *node, Shippability_context *sc_context);
static void pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context *sc_context);

static ExecNodes *pgxc_is_join_shippable(ExecNodes *inner_en, ExecNodes *outer_en,
						Relids in_relids, Relids out_relids, JoinType jointype,
						List *join_quals, Query *query, List *rtables, ParamListInfo boundParams);

/* Fast-query shipping (FQS) functions */
static ExecNodes *pgxc_FQS_get_relation_nodes(RangeTblEntry *rte, Index varno, Query *query,
                                              RelationLocInfo *rel_loc_info,
                                              RelationAccessType rel_access,
                                              Shippability_context *sc_context);
static ExecNodes *pgxc_FQS_find_datanodes(Query *query, Shippability_context *sc_context);
static bool pgxc_query_needs_coord(Query *query);
static bool pgxc_query_contains_only_pg_catalog(Query *query);
static bool pgxc_is_var_distrib_column(Var *var, List *rtable);
static bool pgxc_distinct_has_distcol(Query *query);
static bool pgxc_targetlist_has_distcol(Query *query);
static ExecNodes *pgxc_FQS_find_datanodes_recurse(Node *node, Query *query,
											Bitmapset **relids, Shippability_context *sc_context);
static ExecNodes *pgxc_FQS_datanodes_for_rtr(Index varno, Query *query, int leftmostRTI,
                                             Shippability_context *sc_context);
static bool pgxc_merge_has_distcol(Query *query);
static ExecNodes *pgxc_FQS_find_set_op_datanodes_recurse(Node *node, Query *query,
                                                         Bitmapset **relids, int leftmostRTI,
                                                         Shippability_context *sc_context);
#ifdef __OPENTENBASE__
static ExecNodes* pgxc_is_group_subquery_shippable(Query *query, Shippability_context *sc_context);
static bool pgxc_is_simple_subquery(Query *subquery);
static bool pgxc_FQS_check_subquery_const(Query *query);
static ExecNodes* merge_sublink_exec_nodes(const Query *query,
	Shippability_context *sc_context, ExecNodes *exec_nodes);
#endif
/*
 * Set the given reason in Shippability_context indicating why the query can not be
 * shipped directly to remote nodes.
 */
static void
pgxc_set_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
	context->sc_shippability = bms_add_member(context->sc_shippability, reason);
}

/*
 * pgxc_reset_shippability_reason
 * Reset reason why the query cannot be shipped to remote nodes
 */
static void
pgxc_reset_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
	context->sc_shippability = bms_del_member(context->sc_shippability, reason);
	return;
}


/*
 * See if a given reason is why the query can not be shipped directly
 * to the remote nodes.
 */
static bool
pgxc_test_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
	return bms_is_member(reason, context->sc_shippability);
}


/*
 * pgxc_set_exprtype_shippability
 * Set the expression type shippability. For now composite types
 * derived from view definitions are not shippable.
 */
static void
pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context *sc_context)
{
	char	typerelkind;

	typerelkind = get_rel_relkind(typeidTypeRelid(exprtype));

	if (typerelkind == RELKIND_SEQUENCE ||
		typerelkind == RELKIND_VIEW		||
		typerelkind == RELKIND_FOREIGN_TABLE)
		pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_TYPE);
}

#ifdef __OPENTENBASE__
/*
 * pgxc_FQS_check_const_recurse
 * Recursively check the query node to see if it only contains constant values.
 * We only support all constant values in same leaf nodes, correlated cases are
 * not supported.
 */
static bool
pgxc_FQS_check_const_recurse(Node *node, Query *query)
{
	if (!node)
		return false;

	switch(nodeTag(node))
	{
		case T_FromExpr:
		{
			FromExpr	*from_expr = (FromExpr *)node;
			ListCell	*lcell;
			bool		 result = true;

			/*
			 * Only support SELECT for now
			 */
			if (query->commandType != CMD_SELECT)
				return false;

			/*
			 * Check the SetOperation to cover the case of
			 * '(const subquery) UNION (const subquery)...'
			 */
			if (!from_expr->fromlist)
			{
				if (query->setOperations &&
					IsA(query->setOperations, SetOperationStmt))
				{
					return pgxc_FQS_check_const_recurse(query->setOperations, query);
				}
				return false;
			}

			/* Check if all RTEs contains only constant values */
			foreach (lcell, from_expr->fromlist)
			{
				Node	*fromlist_entry = lfirst(lcell);

				if (!pgxc_FQS_check_const_recurse(fromlist_entry, query))
				{
					result = false;
				}
			}
			return result;
		}
		case T_RangeTblRef:
		{
			RangeTblRef *rtr = (RangeTblRef *)node;
			RangeTblEntry *rte = rt_fetch(rtr->rtindex, query->rtable);

			if (rte->rtekind == RTE_SUBQUERY)
			{
				return pgxc_FQS_check_subquery_const(rte->subquery);
			}
			return false;
		}
		case T_JoinExpr:
		{
			/* TODO: Not supported yet */
			return false;
		}
		case T_SetOperationStmt:
		{
			SetOperationStmt *setOp = (SetOperationStmt *)node;

			/* Only handle UNION cases */
			if (setOp->op == SETOP_UNION &&
				pgxc_FQS_check_const_recurse(setOp->larg, query) &&
				pgxc_FQS_check_const_recurse(setOp->rarg, query))
			{
				return true;
			}
			return false;
		}
		default:
			return false;
	}
	/* Keep compiler happy */
	return false;
}

/*
 * pgxc_FQS_check_subquery_const
 * Check the query node to see if it only contains constant values, we could
 * provide more shipping optimizations based on this hint.
 */
static bool
pgxc_FQS_check_subquery_const(Query *query)
{
	ListCell *lc;
	bool result = true;

	/* If all target list entries are T_Const, then we are done. */
	foreach(lc, query->targetList)
	{
		TargetEntry *tle = lfirst(lc);
		if (!IsA(tle->expr, Const))
		{
			result = false;
		}
	}

	if (result && list_length(query->jointree->fromlist) == 0 &&
	    !query->hasSubLinks && pgxc_is_simple_subquery(query))
		return true;

	/* Otherwise, check if all RTEs are const */
	return pgxc_FQS_check_const_recurse((Node *)query->jointree, query);
}
#endif

static DisKeyAttr *
CreateDisKeyAttr(int nDisAttrs)
{
	DisKeyAttr *res = makeNode(DisKeyAttr);

	Assert(nDisAttrs);

	res->nDisAttrs = nDisAttrs;
	res->disAttrNums = (AttrNumber *) palloc0(sizeof(AttrNumber) * nDisAttrs);
	res->disAttrTypes = (Oid *) palloc0(sizeof(Oid) * nDisAttrs);
	res->disAttrTypMods = (int32 *) palloc0(sizeof(int32) * nDisAttrs);
	return res;
}

static void
FreeDisKeyAttr(DisKeyAttr *attr)
{
	if (attr == NULL)
		return;

	pfree(attr->disAttrNums);
	pfree(attr->disAttrTypes);
	pfree(attr->disAttrTypMods);
	pfree(attr);
}

static DisKeyAttr *
GetNewDisAttribute(List *targetList, RelationLocInfo *rel_loc_info, 
					Index varno, RelationAccessType rel_access)
{
	int          i;
	int          nattrs = 0;
	DisKeyAttr  *res = NULL;
	AttrNumber   distkey;
	ListCell    *lc;
	TargetEntry *tle;

	if (rel_loc_info == NULL || !IsLocatorDistributedByValue(rel_loc_info->locatorType))
		return NULL;

	res = CreateDisKeyAttr(rel_loc_info->nDisAttrs);
	for (i = 0; i < rel_loc_info->nDisAttrs; i++)
	{
		distkey = rel_loc_info->disAttrNums[i];
		foreach (lc, targetList)
		{
			tle = lfirst_node(TargetEntry, lc);

			if ((IsA(tle->expr, Var) && castNode(Var, tle->expr)->varno == varno &&
			    castNode(Var, tle->expr)->varattno == distkey) || 
				(rel_access == RELATION_ACCESS_INSERT && tle->resno == distkey))
			{
				res->disAttrNums[nattrs] = tle->resno;
				res->disAttrTypes[nattrs] = rel_loc_info->disAttrTypes[i];
				res->disAttrTypMods[nattrs] = rel_loc_info->disAttrTypMods[i];
				nattrs++;
				break;
			}
		}
	}

	res->nDisAttrs = nattrs;

	if (rel_loc_info->nDisAttrs != nattrs)
	{
		FreeDisKeyAttr(res);
		res = NULL;
	}
	return res;
}

/*
 * pgxc_FQS_datanodes_for_rtr
 * For a given RangeTblRef find the datanodes where corresponding data is
 * located.
 */
static ExecNodes *
pgxc_FQS_datanodes_for_rtr(Index varno, Query *query, int leftmostRTI, Shippability_context *sc_context)
{
	RangeTblEntry     *rte = rt_fetch(varno, query->rtable);
	RelationLocInfo   *rel_loc_info;
	RelationAccessType rel_access = RELATION_ACCESS_READ;

	switch (query->commandType)
	{
		case CMD_SELECT:
			if (query->rowMarks)
				rel_access = RELATION_ACCESS_READ_FOR_UPDATE;
			else
				rel_access = RELATION_ACCESS_READ_FQS;
			break;
		case CMD_UPDATE:
		case CMD_DELETE:
		case CMD_MERGE:
			rel_access = RELATION_ACCESS_UPDATE;
			break;
		case CMD_INSERT:
			rel_access = RELATION_ACCESS_INSERT;
			break;
		default:
			/* should not happen, but */
			elog(ERROR, "Unrecognised command type %d", query->commandType);
			break;
	}

	switch (rte->rtekind)
	{
		case RTE_RELATION:
		{
			/* For anything, other than a table, we can't find the datanodes */
#ifdef __FDW__
			if (rte->relkind != RELKIND_RELATION && rte->relkind != RELKIND_PARTITIONED_TABLE &&
				!(rte->relkind == RELKIND_FOREIGN_TABLE && rel_is_external_table(rte->relid)))
			{
				RelationLocInfo *rel_loc_info;

				if (rte->relkind != RELKIND_FOREIGN_TABLE)
					return NULL;

				rel_loc_info = GetRelationLocInfo(rte->relid);
				if (NULL == rel_loc_info || rel_loc_info->locatorType != LOCATOR_TYPE_FOREIGN)
					return NULL;
            }
#else
			if (rte->relkind != RELKIND_RELATION)
				return NULL;
#endif				
			/*
			 * In case of inheritance, child tables can have completely different
			 * Datanode distribution than parent. To handle inheritance we need
			 * to merge the Datanodes of the children table as well. The inheritance
			 * is resolved during planning, so we may not have the RTEs of the
			 * children here. Also, the exact method of merging Datanodes of the
			 * children is not known yet. So, when inheritance is requested, query
			 * can not be shipped.
			 * See prologue of has_subclass, we might miss on the optimization
			 * because has_subclass can return true even if there aren't any
			 * subclasses, but it's ok.
			 */
#ifdef __OPENTENBASE__
			/* 
			 * all partitioned tables should have the same distribution, try to 
			 * get execution datanodes
			 */
			if (rte->inh && has_subclass(rte->relid) &&
				rte->relkind != RELKIND_PARTITIONED_TABLE)
			{
				return NULL;
			}
#else
			if (rte->inh && has_subclass(rte->relid))
				return NULL;
#endif

			rel_loc_info = GetRelationLocInfo(rte->relid);
			/* If we don't know about the distribution of relation, bail out */
			if (!rel_loc_info)
			{
				pgxc_set_shippability_reason(sc_context, SS_NEEDS_COORD);
				return NULL;
			}

			return pgxc_FQS_get_relation_nodes(rte,
			                                   varno,
			                                   query,
			                                   rel_loc_info,
			                                   rel_access,
			                                   sc_context);
		}
		break;
		case RTE_CTE:
		case RTE_SUBQUERY:
#ifdef __OPENTENBASE__
		{
			Query 		*subquery = rte->subquery;
			ExecNodes 	*exec_nodes = NULL;

			if (rte->rtekind == RTE_CTE && subquery == NULL)
				return NULL;

			/* Try to process exec_nodes for simple Subquery */
			if (enable_subquery_shipping)
			{
				/* Recurse into the subquery to find executable datanodes. */
				exec_nodes = pgxc_is_query_shippable(subquery,
				                                     sc_context->sc_query_level + 1,
				                                     sc_context->boundParams,
													 sc_context->cached_param_num);
				if (!subquery->canShip)
				{
					query->canShip = false;
					if (sc_context->sc_query)
						sc_context->sc_query->canShip = false;
				}

				/*
				 * Currently we only support Subquery push down to single DN.
				 * Multiple DN pushdown will have cross-phase issues between
				 * main query and subquery, it needs more complicate
				 * calculation. So we just skip the case by now.
				 */
				if (exec_nodes && exec_nodes->nodeList &&
					(list_length(exec_nodes->nodeList) == 1))
					return exec_nodes;
				else if (exec_nodes && exec_nodes->nodeList)
				{
					List       *tmp = list_copy(exec_nodes->dist_attno);
					ListCell   *lc;
					DisKeyAttr *diskeyattr;

					foreach (lc, tmp)
					{
						diskeyattr = (DisKeyAttr *) lfirst(lc);

						if (!(IsLocatorDistributedByValue(exec_nodes->baselocatortype) &&
							diskeyattr->disAttrNums != NULL) || 
							IsLocatorReplicated(exec_nodes->baselocatortype))
						{
							return NULL;
						}

						rel_loc_info = (RelationLocInfo *) palloc0(sizeof(RelationLocInfo));

						rel_loc_info->relid = InvalidOid;
						rel_loc_info->locatorType = exec_nodes->baselocatortype;
						rel_loc_info->nDisAttrs = diskeyattr->nDisAttrs;
						rel_loc_info->disAttrNums = diskeyattr->disAttrNums;
						rel_loc_info->disAttrTypes = diskeyattr->disAttrTypes;
						rel_loc_info->disAttrTypMods = diskeyattr->disAttrTypMods;
						rel_loc_info->groupId = exec_nodes->groupid;
						rel_loc_info->rl_nodeList = list_copy(exec_nodes->nodeList);
						FreeExecNodes(&exec_nodes);

						varno = leftmostRTI > 0 ? leftmostRTI : varno;

						exec_nodes = GetSimpleNodesByQuals(rel_loc_info,
						                                     varno,
						                                     query->jointree->quals,
						                                     query->rtable,
						                                     rel_access,
						                                     sc_context->boundParams);

						if (exec_nodes && diskeyattr->disAttrNums > 0)
						{
							DisKeyAttr *new_diskeyattr =
								GetNewDisAttribute(query->targetList, rel_loc_info, varno, rel_access);

							if (new_diskeyattr != NULL)
							{
								exec_nodes->dist_attno =
									lappend(exec_nodes->dist_attno, new_diskeyattr);
								exec_nodes->groupid = rel_loc_info->groupId;
							}

							break;
						}

						FreeRelationLocInfo(rel_loc_info);
					}

					return exec_nodes;
				}
				else
					return NULL;
			}

			return NULL;
		}
#endif
		/* For any other type of RTE, we return NULL for now */
		case RTE_JOIN:
		case RTE_FUNCTION:
		case RTE_VALUES:
		default:
			return NULL;
	}
}

/*
 * pgxc_FQS_find_datanodes_recurse
 * Recursively find whether the sub-tree of From Expr rooted under given node is
 * pushable and if yes where.
 */
static ExecNodes *
pgxc_FQS_find_datanodes_recurse(Node *node, Query *query, Bitmapset **relids,
	Shippability_context *sc_context)
{
	List		*query_rtable = query->rtable;

	if (!node)
		return NULL;

	switch(nodeTag(node))
	{
		case T_FromExpr:
		{
			FromExpr	*from_expr = (FromExpr *)node;
			ListCell	*lcell;
			bool		first;
			Bitmapset	*from_relids;
			ExecNodes	*result_en;

			/*
			 * For INSERT commands, we won't have any entries in the from list.
			 * Get the datanodes using the resultRelation index.
			 */
			if (query->commandType != CMD_SELECT && !from_expr->fromlist)
			{
				*relids = bms_make_singleton(query->resultRelation);
				return pgxc_FQS_datanodes_for_rtr(query->resultRelation,
														query, 0, sc_context);
			}

			/*
			 * All the entries in the From list are considered to be INNER
			 * joined with the quals as the JOIN condition. Get the datanodes
			 * for the first entry in the From list. For every subsequent entry
			 * determine whether the join between the relation in that entry and
			 * the cumulative JOIN of previous entries can be pushed down to the
			 * datanodes and the corresponding set of datanodes where the join
			 * can be pushed down.
			 */
			first = true;
			result_en = NULL;
			from_relids = NULL;
			foreach (lcell, from_expr->fromlist)
			{
				Node *fromlist_entry = lfirst(lcell);
				Bitmapset *fle_relids = NULL;
				ExecNodes *tmp_en;
				ExecNodes *en = pgxc_FQS_find_datanodes_recurse(fromlist_entry,
																query, &fle_relids, sc_context);
				/*
				 * If any entry in fromlist is not shippable, jointree is not
				 * shippable
				 */
				if (!en)
				{
					FreeExecNodes(&result_en);
					return NULL;
				}

				/* FQS does't ship a DML with more than one relation involved */
				if (!first && query->commandType != CMD_SELECT)
				{
					FreeExecNodes(&result_en);
					return NULL;
				}

				if (first)
				{
					first = false;
					result_en = en;
					from_relids = fle_relids;
					continue;
				}

				tmp_en = result_en;
				/*
				 * Check whether the JOIN is pushable to the datanodes and
				 * find the datanodes where the JOIN can be pushed to
				 */
				result_en = pgxc_is_join_shippable(result_en, en, from_relids,
										fle_relids, JOIN_INNER,
										make_ands_implicit((Expr *)from_expr->quals),
#ifdef __OPENTENBASE__
										query,
#endif
										query_rtable, sc_context->boundParams);
				from_relids = bms_join(from_relids, fle_relids);
				FreeExecNodes(&tmp_en);
			}

			*relids = from_relids;
			return result_en;
		}
			break;

		case T_RangeTblRef:
		{
			RangeTblRef *rtr = (RangeTblRef *)node;
			*relids = bms_make_singleton(rtr->rtindex);
			return pgxc_FQS_datanodes_for_rtr(rtr->rtindex, query, 0, sc_context);
		}
			break;

		case T_JoinExpr:
		{
			JoinExpr *join_expr = (JoinExpr *)node;
			Bitmapset *l_relids = NULL;
			Bitmapset *r_relids = NULL;
			ExecNodes *len;
			ExecNodes *ren;
			ExecNodes *result_en;

			/* FQS does't ship a DML with more than one relation involved */
			if (query->commandType != CMD_SELECT)
				return NULL;

			len = pgxc_FQS_find_datanodes_recurse(join_expr->larg, query,
																&l_relids, sc_context);
			ren = pgxc_FQS_find_datanodes_recurse(join_expr->rarg, query,
																&r_relids, sc_context);
			/* If either side of JOIN is unshippable, JOIN is unshippable */
			if (!len || !ren)
			{
				FreeExecNodes(&len);
				FreeExecNodes(&ren);
				return NULL;
			}
			/*
			 * Check whether the JOIN is pushable or not, and find the datanodes
			 * where the JOIN can be pushed to.
			 */
			result_en = pgxc_is_join_shippable(ren, len, r_relids, l_relids,
												join_expr->jointype,
												make_ands_implicit((Expr *)join_expr->quals),
#ifdef __OPENTENBASE__
												query,
#endif
												query_rtable, sc_context->boundParams);
			FreeExecNodes(&len);
			FreeExecNodes(&ren);
			*relids = bms_join(l_relids, r_relids);
			return result_en;
		}
			break;

		default:
			*relids = NULL;
			return NULL;
			break;
	}
	/* Keep compiler happy */
	return NULL;
}

static ExecNodes *
pgxc_FQS_find_set_op_datanodes_recurse(Node *node, Query *query, Bitmapset **relids, int leftmostRTI,
                                       Shippability_context *sc_context)
{
	if (!node)
		return NULL;

	switch (nodeTag(node))
	{
		case T_SetOperationStmt:
		{
			SetOperationStmt *set_op = (SetOperationStmt *) node;
			Bitmapset        *l_relids = NULL;
			Bitmapset        *r_relids = NULL;
			ExecNodes        *len;
			ExecNodes        *ren;
			ExecNodes        *result_en = NULL;
			
			bool try_merge = true;

			len = pgxc_FQS_find_set_op_datanodes_recurse(set_op->larg, query, &l_relids, leftmostRTI, sc_context);
			ren = pgxc_FQS_find_set_op_datanodes_recurse(set_op->rarg, query, &r_relids, leftmostRTI, sc_context);

			/*
			 * unshippable cases:
			 * 1. If either side of set op is unshippable,
			 * 2. it's not an UNION ALL op and distributed in multiple DN
			 * 3. UNION ALL op for multi node distribution and replicated distribution
			 */
			if (!len || !ren)
				try_merge = false;
			else if (list_length(len->nodeList) > 1 || list_length(ren->nodeList) > 1)
			{
				try_merge = false;

				if (IsExecNodesReplicated(len) && IsExecNodesReplicated(ren))
					try_merge = true;
				else if (set_op->op == SETOP_UNION && set_op->all &&
				         IsExecNodesDistributedByValue(len) && IsExecNodesDistributedByValue(ren))
					try_merge = true;
			}

			if (!try_merge)
			{
				FreeExecNodes(&len);
				FreeExecNodes(&ren);
				return NULL;
			}

			result_en = pgxc_merge_exec_nodes(len, ren);

			if (result_en &&
			    equal(len->dist_attno, ren->dist_attno) &&
			    len->groupid == ren->groupid)
			{
				result_en->dist_attno = (List *) copyObject(len->dist_attno);
				result_en->groupid = len->groupid;
			}

			FreeExecNodes(&len);
			FreeExecNodes(&ren);

			return result_en;
		}
		break;

		case T_RangeTblRef:
		{
			RangeTblRef *rtr = (RangeTblRef *) node;
			*relids = bms_make_singleton(rtr->rtindex);
			return pgxc_FQS_datanodes_for_rtr(rtr->rtindex, query, leftmostRTI, sc_context);
		}
		break;
		default:
			*relids = NULL;
			return NULL;
			break;
	}
	/* Keep compiler happy */
	return NULL;
}

/*
 * pgxc_FQS_find_datanodes
 * Find the list of nodes where to ship query.
 */
static ExecNodes *
pgxc_FQS_find_datanodes(Query *query, Shippability_context *sc_context)
{
	Bitmapset	*relids = NULL;
	ExecNodes	*exec_nodes = NULL;

	if (query->setOperations)
	{
		Node *node = query->setOperations;
		int   leftmostRTI;

		/*
		 * Re-find leftmost SELECT (now it's a sub-query in rangetable)
		 */
		while (node && IsA(node, SetOperationStmt))
			node = ((SetOperationStmt *) node)->larg;
		Assert(node && IsA(node, RangeTblRef));
		leftmostRTI = ((RangeTblRef *) node)->rtindex;

		exec_nodes = pgxc_FQS_find_set_op_datanodes_recurse(query->setOperations,
		                                                    query,
		                                                    &relids,
		                                                    leftmostRTI,
		                                                    sc_context);

		return exec_nodes;
	}

	/*
	 * For SELECT, the datanodes required to execute the query is obtained from
	 * the join tree of the query
	 */
	exec_nodes = pgxc_FQS_find_datanodes_recurse((Node *)query->jointree,
														query, &relids, sc_context);
	bms_free(relids);
	relids = NULL;
	/*
	 * If we found the expression which can decide which can be used to decide
	 * where to ship the query, or already figure out where to execute, use it.
	 */
	if (exec_nodes &&
	    ((exec_nodes->nExprs > 0 && exec_nodes->dis_exprs[0]) ||
	     exec_nodes->nodeList))
		return exec_nodes;
	/* No way to figure out datanodes to ship the query to */
	return NULL;
}

static bool
is_allowed_fqs_clipping(Node *expr)
{
	FuncExpr *func = NULL;
	Node     *arg1 = NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	if (!expr)
		return false;

	/* case1, Param supported */
	if (IsA(expr, Param))
		return true;

	/*
	 * case2, RelabelType with param is supported
	 * cast the short shard field into the long field.
	 */
	if (IS_RELABELFORMAT_CAST(expr))
	{
		RelabelType *tmp = castNode(RelabelType, expr);

		if (IsA(tmp->arg, Param))
			return true;
		else
			return is_allowed_fqs_clipping((Node *) tmp->arg);
	}

	/*
	 * case3, cast function with param is supported
	 * cast the long shard field into the short field.
	 */
	if (!IsA(expr, FuncExpr) ||
		!IS_FUNCFORMAT_CAST(expr))
		return false;

	/*
	 * The first parameter of cast conversion is the original variable.
	 * eg. varchar2(source, typmod, isExplicit)
	 */
	func = castNode(FuncExpr, expr);
	arg1 = linitial((List *) func->args);

	if (IsA(arg1, Param))
		return true;
	else
		return is_allowed_fqs_clipping(arg1);
}


static void
safe_join_walker(int relid, Node *jtnode, bool *result)
{
	if (IsA(jtnode, RangeTblRef))
	{
		RangeTblRef *r = (RangeTblRef *) jtnode;

		if (r->rtindex == relid)
			*result = true;
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *lc;

		foreach(lc, f->fromlist)
			safe_join_walker(relid, (Node *) lfirst(lc), result);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		/* Nope, but inspect children */
		safe_join_walker(relid, j->larg, result);
		safe_join_walker(relid, j->rarg, result);

		if (result && j->jointype != JOIN_INNER)
			*result = false;
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
	}
}

static bool
is_rel_safe_join(int relid, FromExpr *from)
{
	bool result = false;

	safe_join_walker(relid, (Node *) from, &result);

	return result;
}

static Expr *
create_dis_col_eval(Node *quals, AttrNumber discol)
{
	Expr *result = NULL;

	if(!quals)
		return result;
	
	if(IsA(quals, OpExpr))
	{
		Expr *lexpr;
		Expr *rexpr;
		OpExpr * op = (OpExpr *)quals;

		/* could not be '=' */
		if (list_length(op->args) != 2)
			return result;
				
		lexpr = linitial(op->args);
		rexpr = lsecond(op->args);

		if (IsA(lexpr, RelabelType))
			lexpr = ((RelabelType*)lexpr)->arg;
		if (IsA(rexpr, RelabelType))
			rexpr = ((RelabelType*)rexpr)->arg;

		if (IsA(lexpr, Var) && is_allowed_fqs_clipping((Node*)rexpr))
		{
			if(discol == ((Var *)lexpr)->varattno)
			{
				/* must be '=' */
				if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
					!op_hashjoinable(op->opno, exprType((Node *)lexpr)))
					return result;
				else
					result = (Expr *)copyObject(rexpr);
			}
		}
		else if (IsA(rexpr, Var) && is_allowed_fqs_clipping((Node*)lexpr))
		{
			if(discol == ((Var *)rexpr)->varattno)
			{
				/* must be '=' */
				if (!op_mergejoinable(op->opno, exprType((Node *)lexpr)) &&
					!op_hashjoinable(op->opno, exprType((Node *)lexpr)))
					return result;
				else
					result = (Expr *)copyObject(lexpr);
			}
		}
	}
	else if(IsA(quals, BoolExpr) && ((BoolExpr *)quals)->boolop == AND_EXPR)
	{
		ListCell *lc;
		BoolExpr * bexpr = (BoolExpr *)quals;

		foreach(lc, bexpr->args)
		{
			Expr *lexpr = lfirst(lc);

			result = create_dis_col_eval((Node *)lexpr, discol);

			if(result)
				break;
		}
	}


	return result;
}

/*
 * pgxc_FQS_get_relation_nodes
 * Return ExecNodes structure so as to decide which node the query should
 * execute on. If it is possible to set the node list directly, set it.
 * Otherwise set the appropriate distribution column expression or relid in
 * ExecNodes structure.
 */
static ExecNodes *
pgxc_FQS_get_relation_nodes(RangeTblEntry *rte, Index varno, Query *query,
                            RelationLocInfo *rel_loc_info, RelationAccessType rel_access,
                            Shippability_context *sc_context)
{
	CmdType command_type = query->commandType;
	ExecNodes	*rel_exec_nodes;
	bool retry = true;
	Node *dis_qual = NULL;

	Assert(rte == rt_fetch(varno, (query->rtable)));

	/*
	 * Find out the datanodes to execute this query on.
	 * But first if it's a replicated table, identify and remove
	 * unhealthy nodes from the rel_loc_info. Only for SELECTs!
	 *
	 * PGXC_FQS_TODO: for now, we apply node reduction only when there is only
	 * one relation involved in the query. If there are multiple distributed
	 * tables in the query and we apply node reduction here, we may fail to ship
	 * the entire join. We should apply node reduction transitively.
	 */
retry_pools:
	if (command_type == CMD_SELECT &&
			rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
	{
		int i;
		List *newlist = NIL;
		ListCell *lc;
		bool *healthmap = NULL;
		healthmap = (bool*)palloc(sizeof(bool) * OPENTENBASE_MAX_DATANODE_NUMBER);
	    if (healthmap == NULL)
	    {
	        ereport(ERROR,
	                (errcode(ERRCODE_OUT_OF_MEMORY),
	                 errmsg("out of memory for healthmap")));
	    }
		
		PgxcNodeDnListHealth(rel_loc_info->rl_nodeList, healthmap);

		i = 0;
		foreach(lc, rel_loc_info->rl_nodeList)
		{
			if (!healthmap[i++])
				newlist = lappend_int(newlist, lfirst_int(lc));
		}

		if (healthmap)
		{
			pfree(healthmap);
			healthmap = NULL;
		}

		if (newlist != NIL)
			rel_loc_info->rl_nodeList = list_difference_int(rel_loc_info->rl_nodeList,
													 newlist);
		/*
		 * If all nodes are down, cannot do much, just return NULL here
		 */
		if (rel_loc_info->rl_nodeList == NIL)
		{
			/*
			 * Try an on-demand pool maintenance just to see if some nodes
			 * have come back.
			 *
			 * Try once and error out if datanodes are still down
			 */
			if (retry)
			{
				rel_loc_info->rl_nodeList = newlist;
				newlist = NIL;
				PoolPingNodes();
				retry = false;
				goto retry_pools;
			}
			else
				elog(ERROR,
				 "Could not find healthy datanodes for replicated table. Exiting!");
			return NULL;
		}
	}
	
	rel_exec_nodes = GetRelationNodesByQuals(rte->relid, rel_loc_info,
											 varno, query->jointree->quals,
											 query->rtable, rel_access,
											 &dis_qual, sc_context->boundParams);
	
	if (!rel_exec_nodes)
		return NULL;

	if (IsRelationDistributedByValue(rel_loc_info))
	{

		DisKeyAttr *new_diskeyattr = GetNewDisAttribute(query->targetList, 
														rel_loc_info, 
														varno, rel_access);

		if (new_diskeyattr != NULL)
		{
			rel_exec_nodes->dist_attno = lappend(rel_exec_nodes->dist_attno, new_diskeyattr);
			rel_exec_nodes->groupid = rel_loc_info->groupId;
		}

		/*
		 * check if we can reduce the Datanodes using global index
		 */
		RestrictNodesByGlobalIndex(rte->relid,
		                           varno,
								   query->rtable,
		                           query->jointree->quals,
								   rel_access,
								   &dis_qual,
		                           &rel_exec_nodes,
		                           sc_context->boundParams);
	}

	if ((rel_access == RELATION_ACCESS_INSERT || list_length(query->rtable) == 1)&&
			 IsRelationDistributedByValue(rel_loc_info))
	{
		ListCell *lc;
		TargetEntry *tle = NULL;
		/*
		 * If the INSERT is happening on a table distributed by value of a
		 * column, find out the
		 * expression for distribution column in the targetlist, and stick in
		 * in ExecNodes, and clear the nodelist. Execution will find
		 * out where to insert the row.
		 */
		/* It is a partitioned table, get value by looking in targetList */
		if (rel_access == RELATION_ACCESS_INSERT)
		{
			int     paramid;

			if (sc_context->boundParams)
				paramid = sc_context->boundParams->numParams + 1;
			else
				paramid = sc_context->cached_param_num + 1;

			foreach(lc, query->targetList)
			{
				tle = (TargetEntry *) lfirst(lc);

				if (tle->resjunk)
					continue;
				
				if (rel_loc_info->nDisAttrs > 0)
				{
					int i = 0;
					if (!rel_exec_nodes->dis_exprs)
					{
						rel_exec_nodes->nExprs = rel_loc_info->nDisAttrs;
						rel_exec_nodes->dis_exprs = (Expr **) palloc0(sizeof(Expr *) * rel_loc_info->nDisAttrs);
						rel_exec_nodes->en_param_expr = (Expr **) palloc0(sizeof(Expr *) * rel_loc_info->nDisAttrs);
					}
					for (i = 0; i < rel_loc_info->nDisAttrs; i++)
					{
						char *colname = get_attname(rel_loc_info->relid, rel_loc_info->disAttrNums[i]);
						if (strcmp(tle->resname, colname) == 0 && !rel_exec_nodes->dis_exprs[i])
						{
							if (expression_returns_set((Node *) tle->expr))
							{
								rel_exec_nodes->dis_exprs[i] = NULL;
								rel_exec_nodes->en_param_expr[i] = NULL;
							}
							else if (!pgxc_is_expr_shippable(tle->expr, NULL) ||
									 contain_mutable_functions((Node *)tle->expr))
							{
								/* case: INSERT INTO t(distkey) VALUES(foo()) */
								
								Param  *param;

								/* create a Param, preserve it, if we can FQS, replace it */
								param = pgxc_make_param(paramid, exprType((Node *) tle->expr));
								rel_exec_nodes->dis_exprs[i] = (Expr *) param;
								rel_exec_nodes->en_param_expr[i] = tle->expr;
								paramid++;
							}
							else
							{
								rel_exec_nodes->dis_exprs[i] = tle->expr;
								rel_exec_nodes->en_param_expr[i] = NULL;
							}
						}
					}
				}
			}
			/*
			 * query->targetList is empty when:
			 * 1. insert into t default values; But there is no column on t has default values.
			 * 2. when transform_insert_to_copy is enabled
			 */
        }
		else
		{
			if (rel_loc_info->locatorType == LOCATOR_TYPE_HASH ||
				rel_loc_info->locatorType == LOCATOR_TYPE_SHARD)
			{
				Node *quals = query->jointree->quals;
				int i = 0;
				
				rel_exec_nodes->nExprs = rel_loc_info->nDisAttrs;
				rel_exec_nodes->dis_exprs = (Expr **) palloc0(sizeof(Expr *) * rel_loc_info->nDisAttrs);
				if (!dis_qual)
				{
					for (i = 0; i < rel_loc_info->nDisAttrs; i++)
					{
						Expr	*dis_expr;
						Oid		expr_typ;

						dis_expr = create_dis_col_eval(quals, rel_loc_info->disAttrNums[i]);

						if (dis_expr)
						{
							expr_typ = exprType((Node *) dis_expr);

							if (expr_typ != rel_loc_info->disAttrTypes[i])
							{
								/*
								 * The dis_expr may be NULL if there is no coercion path,
								 * we think it is right. Because we can't use the distributed
								 * key type hash function in this case.
								 */
								dis_expr = (Expr *) coerce_to_target_type(
												NULL,
												(Node *) dis_expr,
												expr_typ,
												rel_loc_info->disAttrTypes[i],
												rel_loc_info->disAttrTypMods[i],
												COERCION_ASSIGNMENT,
												COERCE_IMPLICIT_CAST,
												-1);
							}
						}

						rel_exec_nodes->dis_exprs[i] = dis_expr;
					}
				}
			}
		}
		/* Not found, bail out */
		if (rel_exec_nodes->nExprs == 0 ||
			(rel_exec_nodes->nExprs > 0 && !rel_exec_nodes->dis_exprs[0]))
		{
			if(rel_access != RELATION_ACCESS_INSERT)				
				return rel_exec_nodes;		
			else				
				return NULL;
		}

		/* We found the TargetEntry for the partition column */
		list_free(rel_exec_nodes->primarynodelist);
		rel_exec_nodes->primarynodelist = NULL;
		list_free(rel_exec_nodes->nodeList);
		rel_exec_nodes->nodeList = NULL;
		rel_exec_nodes->en_relid = rel_loc_info->relid;
	}
	else if (rel_access == RELATION_ACCESS_INSERT &&
	         IsRelationReplicated(rel_loc_info) &&
	         bms_is_member(SS_NEED_FUNC_CONVERT_TO_PARAM, 
			 				sc_context->sc_shippability))
	{
		return NULL;
	}

	return rel_exec_nodes;
}

static bool
pgxc_query_has_distcolgrouping(Query *query)
{
	ListCell	*lcell;
	foreach (lcell, query->groupClause)
	{
		SortGroupClause 	*sgc = lfirst(lcell);
		Node				*sgc_expr;
		if (!IsA(sgc, SortGroupClause))
			continue;
		sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var))
		{
			Var *var = castNode(Var, sgc_expr);
			if (var->varlevelsup == 0 &&
				pgxc_is_var_distrib_column(var, query->rtable) &&
				is_rel_safe_join(var->varno, query->jointree))
				return true;
		}
	}
	return false;
}

static bool
pgxc_distinct_has_distcol(Query *query)
{
	ListCell	*lcell;
	foreach (lcell, query->distinctClause)
	{
		SortGroupClause 	*sgc = lfirst(lcell);
		Node				*sgc_expr;
		if (!IsA(sgc, SortGroupClause))
			continue;
		sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var))
		{
			Var *var = castNode(Var, sgc_expr);
			if (var->varlevelsup == 0 &&
				pgxc_is_var_distrib_column(var, query->rtable) &&
				is_rel_safe_join(var->varno, query->jointree))
				return true;
		}
	}
	return false;
}

/*
 * pgxc_shippability_walker
 * walks the query/expression tree routed at the node passed in, gathering
 * information which will help decide whether the query to which this node
 * belongs is shippable to the Datanodes.
 *
 * The function should try to walk the entire tree analysing each subquery for
 * shippability. If a subquery is shippable but not the whole query, we would be
 * able to create a RemoteQuery node for that subquery, shipping it to the
 * Datanode.
 *
 * Return value of this function is governed by the same rules as
 * expression_tree_walker(), see prologue of that function for details.
 */
static bool
pgxc_shippability_walker(Node *node, Shippability_context *sc_context)
{
	if (node == NULL)
		return false;

	/* Below is the list of nodes that can appear in a query, examine each
	 * kind of node and find out under what conditions query with this node can
	 * be shippable. For each node, update the context (add fields if
	 * necessary) so that decision whether to FQS the query or not can be made.
	 * Every node which has a result is checked to see if the result type of that
	 * expression is shippable.
	 */
	switch(nodeTag(node))
	{
		/* Constants are always shippable */
		case T_Const:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

			/*
			 * For placeholder nodes the shippability of the node, depends upon the
			 * expression which they refer to. It will be checked separately, when
			 * that expression is encountered.
			 */
		case T_CaseTestExpr:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

			/*
			 * record_in() function throws error, thus requesting a result in the
			 * form of anonymous record from datanode gets into error. Hence, if the
			 * top expression of a target entry is ROW(), it's not shippable.
			 */
		case T_TargetEntry:
		{
			TargetEntry *tle = (TargetEntry *)node;
			if (tle->expr)
			{
				char typtype = get_typtype(exprType((Node *)tle->expr));
				if (!typtype || typtype == TYPTYPE_PSEUDO)
					pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			}
		}
		break;

		case T_SortGroupClause:
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			break;

		case T_CoerceViaIO:
		{
			CoerceViaIO		*cvio = (CoerceViaIO *)node;
			Oid				input_type = exprType((Node *)cvio->arg);
			Oid				output_type = cvio->resulttype;
			CoercionContext	cc;

			cc = cvio->coerceformat == COERCE_IMPLICIT_CAST ? COERCION_IMPLICIT :
				COERCION_EXPLICIT;
			/*
			 * Internally we use IO coercion for types which do not have casting
			 * defined for them e.g. cstring::date. If such casts are sent to
			 * the datanode, those won't be accepted. Hence such casts are
			 * unshippable. Since it will be shown as an explicit cast.
			 *
			 * If single node with no column table, it can be shipped
			 * because cn and dn should do the same casting.
			 */
			if (IsA(cvio->arg, Const))
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
			else if (!can_coerce_type(1, &input_type, &output_type, cc, false))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;
		/*
		 * Nodes, which are shippable if the tree rooted under these nodes is
		 * shippable
		 */
		case T_CoerceToDomainValue:
			/*
			 * PGXCTODO: mostly, CoerceToDomainValue node appears in DDLs,
			 * do we handle DDLs here?
			 */
		case T_FieldSelect:
		case T_NamedArgExpr:
		case T_RelabelType:
		case T_BoolExpr:
			/*
			 * PGXCTODO: we might need to take into account the kind of boolean
			 * operator we have in the quals and see if the corresponding
			 * function is immutable.
			 */
		case T_ArrayCoerceExpr:
		case T_ConvertRowtypeExpr:
		case T_CaseExpr:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_CollateExpr:
		case T_CoalesceExpr:
		case T_XmlExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_CoerceToDomain:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

		case T_List:
			break;
			
		case T_RangeTblRef:
			break;

		case T_ArrayRef:
			/*
			 * When multiple values of of an array are updated at once
			 * FQS planner cannot yet handle SQL representation correctly.
			 * So disable FQS in this case and let standard planner manage it.
			 */
		case T_FieldStore:
			/*
			 * PostgreSQL deparsing logic does not handle the FieldStore
			 * for more than one fields (see processIndirection()). So, let's
			 * handle it through standard planner, where whole row will be
			 * constructed.
			 */
		case T_SetToDefault:
			/*
			 * PGXCTODO: we should actually check whether the default value to
			 * be substituted is shippable to the Datanode. Some cases like
			 * nextval() of a sequence can not be shipped to the Datanode, hence
			 * for now default values can not be shipped to the Datanodes
			 */
			pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

		case T_Var:
		{
			Var	*var = (Var *)node;
			/*
			 * if a subquery references an upper level variable, that query is
			 * not shippable, if shipped alone.
			 */
			if (var->varlevelsup > sc_context->sc_max_varlevelsup)
				sc_context->sc_max_varlevelsup = var->varlevelsup;
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_Param:
		{
			Param *param = (Param *)node;
			/* PGXCTODO: Can we handle internally generated parameters? */
			if (param->paramkind != PARAM_EXTERN &&
			    param->paramkind != PARAM_SUBLINK)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_CurrentOfExpr:
		{
			/*
			 * Ideally we should not see CurrentOf expression here, it
			 * should have been replaced by the CTID = ? expression. But
			 * still, no harm in shipping it as is.
			 */
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_SQLValueFunction:
			/*
			 * XXX PG10MERGE: Do we really need to do any checks here?
			 * Shouldn't all SQLValueFunctions be shippable?
			 */
			if (sc_context->sc_query)
			{
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			}
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

		case T_NextValueExpr:
			/*
			 * We must not FQS NextValueExpr since it could be used for
			 * distribution key and it should get mapped to the correct
			 * datanode.
			 */
			pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			break;

		case T_Aggref:
		{
			Aggref *aggref = (Aggref *)node;
			/*
			 * An aggregate is completely shippable to the Datanode, if the
			 * whole group resides on that Datanode. This will be clear when
			 * we see the GROUP BY clause.
			 * agglevelsup is minimum of variable's varlevelsup, so we will
			 * set the sc_max_varlevelsup when we reach the appropriate
			 * VARs in the tree.
			 */
			pgxc_set_shippability_reason(sc_context, SS_HAS_AGG_EXPR);
			/*
			 * If a stand-alone expression to be shipped, is an
			 * 1. aggregate with ORDER BY, DISTINCT directives, it needs all
			 * the qualifying rows
			 * 2. aggregate without collection function
			 * 3. (PGXCTODO:)aggregate with polymorphic transition type, the
			 *    the transition type needs to be resolved to correctly interpret
			 *    the transition results from Datanodes.
			 * Hence, such an expression can not be shipped to the datanodes.
			 */
			if (aggref->aggorder ||
				aggref->aggdistinct ||
				aggref->agglevelsup ||
				!aggref->agghas_collectfn ||
				IsPolymorphicType(aggref->aggtrantype))
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_FuncExpr:
		{
			FuncExpr	*funcexpr = (FuncExpr *)node;
			/*
			 * PGXC_FQS_TODO: it's too restrictive not to ship non-immutable
			 * functions to the Datanode. We need a better way to see what
			 * can be shipped to the Datanode and what can not be.
			 */
#ifdef _PG_ORCL_
			if (funcexpr->withfuncnsp != NULL)
			{
				if (!pgxc_is_withfunc_shippable(funcexpr->withfuncnsp,
												funcexpr->withfuncid))
					pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			}
			else /* Else normal case */
#endif
			if (!pgxc_is_func_shippable(funcexpr->funcid))
			{
				/* Ship insert if function doesn't return set */
				if (sc_context->sc_query &&
					sc_context->sc_query->commandType == CMD_INSERT)
				{
					pgxc_set_shippability_reason(sc_context, SS_NEED_FUNC_CONVERT_TO_PARAM);
				}
				else
					pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

				/* after that, the flag hasUnshippableDml is set and the function call is made in CN */
				if (ORA_MODE && contain_plpgsql_functions((Node *) funcexpr))
					pgxc_set_shippability_reason(sc_context, SS_NEED_PROCESS_IN_CN);
			}

			if (funcexpr->funcretset && sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
		{
			/*
			 * All of these three are structurally equivalent to OpExpr, so
			 * cast the node to OpExpr and check if the operator function is
			 * immutable. See PGXC_FQS_TODO item for FuncExpr.
			 */
			OpExpr *op_expr = (OpExpr *)node;
			Oid		opfuncid = OidIsValid(op_expr->opfuncid) ?
				op_expr->opfuncid : get_opcode(op_expr->opno);
			if (!OidIsValid(opfuncid) ||
				!pgxc_is_func_shippable(opfuncid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_ScalarArrayOpExpr:
		{
			/*
			 * Check if the operator function is shippable to the Datanode
			 * PGXC_FQS_TODO: see immutability note for FuncExpr above
			 */
			ScalarArrayOpExpr *sao_expr = (ScalarArrayOpExpr *)node;
			Oid		opfuncid = OidIsValid(sao_expr->opfuncid) ?
				sao_expr->opfuncid : get_opcode(sao_expr->opno);
			if (!OidIsValid(opfuncid) ||
				!pgxc_is_func_shippable(opfuncid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_RowCompareExpr:
		case T_MinMaxExpr:
		{
			/*
			 * PGXCTODO should we be checking the comparision operator
			 * functions as well, as we did for OpExpr OR that check is
			 * unnecessary. Operator functions are always shippable?
			 * Otherwise this node should be treated similar to other
			 * "shell" nodes.
			 */
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_Query:
		{
			bool   tree_examine_ret = false;
			Query *query = (Query *)node;
			ExecNodes *exec_nodes = NULL;

			/* A stand-alone expression containing Query is not shippable */
			if (sc_context->sc_for_expr)
			{
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
				break;
			}
			/*
			 * We are checking shippability of whole query, go ahead. The query
			 * in the context should be same as the query being checked
			 */
			Assert(query == sc_context->sc_query);

			/* CREATE TABLE AS is not supported in FQS */
			if (query->commandType == CMD_UTILITY &&
				IsA(query->utilityStmt, CreateTableAsStmt))
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			if (query->hasRecursive)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			if (query->connectByExpr)
				pgxc_set_shippability_reason(sc_context, SS_REPLICATED_ONLY);

			/* Queries with FOR UPDATE/SHARE can't be shipped */
		//	if (query->hasForUpdate || query->rowMarks)
			//	pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/*
			 * If the query needs Coordinator for evaluation or the query can be
			 * completed on Coordinator itself, we don't ship it to the Datanode
			 */
			if (pgxc_query_needs_coord(query))
				pgxc_set_shippability_reason(sc_context, SS_NEEDS_COORD);

			/*
			 * PGXCTODO: It should be possible to look at the Query and find out
			 * whether it can be completely evaluated on the Datanode just like SELECT
			 * queries. But we need to be careful while finding out the Datanodes to
			 * execute the query on, esp. for the result relations. If one happens to
			 * remove/change this restriction, make sure you change
			 * pgxc_FQS_get_relation_nodes appropriately.
			 * For now DMLs with single rtable entry are candidates for FQS
			 * However for upsert, there are something special:
			 */
			if (list_length(query->rtable) > 1 &&
			    query->commandType != CMD_SELECT &&
			    (!query->onConflict || query->onConflict->action == ONCONFLICT_NOTHING))
			{
				/*
				 * For DML without onConflict, or conflict do nothing, we only
				 * allow FQS for query with single rtable.
				 */
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			}
			else if (list_length(query->rtable) > 2 &&
			         query->onConflict &&
			         query->onConflict->action == ONCONFLICT_UPDATE)
			{
				/* only INSERT may has onConflict */
				Assert(query->commandType == CMD_INSERT);
				/*
				 * But for INSERT ON CONFLICT DO UPDATE, since there are always
				 * an extra rtable for update result, we disallow FQS with
				 * more than two rtables instead of just one.
				 */
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			}

			/*
			 * In following conditions query is shippable when there is only one
			 * Datanode involved
			 * 1. the query has groupset clause
			 * 2. the query has window functions
			 * 3. the query has ORDER BY clause
			 * 4. the query has limit and offset clause
			 */
			if (query->groupingSets || query->hasWindowFuncs || query->sortClause ||
				query->limitOffset || query->limitCount)
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * Presence of aggregates or having clause, implies grouping. In
			 * such cases, the query won't be shippable unless 1. there is only
			 * a single node involved 2. GROUP BY clause has distribution column
			 * in it. In the later case aggregates for a given group are entirely
			 * computable on a single datanode, because all the rows
			 * participating in particular group reside on that datanode.
			 * The distribution column can be of any relation
			 * participating in the query. All the rows of that relation with
			 * the same value of distribution column reside on same node.
			 */
			if ((query->hasAggs || query->havingQual || query->groupClause))
			{	
				/* Check whether the sub query is shippable */
				exec_nodes = pgxc_is_group_subquery_shippable(query, sc_context);
				if (!exec_nodes && !pgxc_query_has_distcolgrouping(query))
					pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
			}

			/*
			 * If distribution column of any relation is present in the distinct
			 * clause, values for that column across nodes will differ, thus two
			 * nodes won't be able to produce same result row. Hence in such
			 * case, we can execute the queries on many nodes managing to have
			 * distinct result.
			 */
			if (query->distinctClause && !pgxc_distinct_has_distcol(query))
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			if ((query->commandType == CMD_UPDATE) &&
					pgxc_targetlist_has_distcol(query))
				pgxc_set_shippability_reason(sc_context, SS_UPDATES_DISTRIBUTION_COLUMN);

			if (query->commandType == CMD_MERGE && pgxc_merge_has_distcol(query))
			{
				pgxc_set_shippability_reason(sc_context, SS_UPDATES_DISTRIBUTION_COLUMN);
			}
#ifdef __OPENTENBASE__
			/*
			 * Check shippability of triggers on this query. Don't consider
			 * TRUNCATE triggers; it's a utility statement and triggers are
			 * handled explicitly in ExecuteTruncate()
			 */
			if (query->commandType == CMD_UPDATE ||
				query->commandType == CMD_INSERT ||
				query->commandType == CMD_DELETE)
			{
				RangeTblEntry *rte = (RangeTblEntry *)
					list_nth(query->rtable, query->resultRelation - 1);

				if (!pgxc_check_triggers_shippability(rte->relid,
													  query->commandType))
				{
					query->hasUnshippableDml = true;
					query->has_trigger = true;
					pgxc_set_shippability_reason(sc_context,
												 SS_UNSHIPPABLE_TRIGGER);
				}

				/* We have to check update triggers if insert...on conflict do update... */
				if (query->onConflict && query->onConflict->action == ONCONFLICT_UPDATE)
				{
					if (!pgxc_check_triggers_shippability(rte->relid,
									  CMD_UPDATE))
					{
						query->hasUnshippableDml = true;
						query->has_trigger = true;
						pgxc_set_shippability_reason(sc_context,
													 SS_UNSHIPPABLE_TRIGGER);
					}
				}

				/*
				 * PGXCTODO: For the time being Postgres-XC does not support
				 * global constraints, but once it does it will be necessary
				 * to add here evaluation of the shippability of indexes and
				 * constraints of the relation used for INSERT/UPDATE/DELETE.
				 */
			}
#endif

			/*
			 * walk the entire query tree to analyse the query. We will walk the
			 * range table, when examining the FROM clause. No need to do it
			 * here
			 */
			tree_examine_ret = query_tree_walker(query, pgxc_shippability_walker,
									sc_context, QTW_IGNORE_RANGE_TABLE );
			if (tree_examine_ret)
			{
				return true;
			}
			else
			{
			    if (bms_is_member(SS_HAS_UNSHIPPABLE_SUBQUERY, sc_context->sc_shippability))
                {
                    return false;
                }
				exec_nodes = sc_context->sc_exec_nodes;
			}

			/*
			 * Walk the join tree of the query and find the
			 * Datanodes needed for evaluating this query
			 */
			sc_context->sc_exec_nodes = pgxc_FQS_find_datanodes(query, sc_context);

			/*
			 * PGXC_FQS_TODO:
			 * There is a subquery in this query, which references Vars in the upper
			 * query. For now stop shipping such queries. We should get rid of this
			 * condition.
			 * except the distribution is only on one node or it is a replicated distribution.
			 */
			if (sc_context->sc_max_varlevelsup != 0 &&
			    (!sc_context->sc_exec_nodes ||
			     (list_length(sc_context->sc_exec_nodes->nodeList) > 1 &&
			      !IsExecNodesReplicated(sc_context->sc_exec_nodes))))
			{
				pgxc_set_shippability_reason(sc_context, SS_VARLEVEL);
				FreeExecNodes(&sc_context->sc_exec_nodes);
				sc_context->sc_exec_nodes = NULL;
			}
		}
		break;

		case T_FromExpr:
		{
			/* We don't expect FromExpr in a stand-alone expression */
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/*
			 * We will examine the jointree of query separately to determine the
			 * set of datanodes where to execute the query.
			 * If this is an INSERT query with quals, resulting from say
			 * conditional rule, we can not handle those in FQS, since there is
			 * not SQL representation for such quals.
			 */
			if (sc_context->sc_query->commandType == CMD_INSERT &&
				((FromExpr *)node)->quals)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

		}
		break;

		case T_WindowFunc:
		{
			WindowFunc *winf = (WindowFunc *)node;
			/*
			 * A window function can be evaluated on a Datanode if there is
			 * only one Datanode involved.
			 */
			pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * A window function is not shippable as part of a stand-alone
			 * expression. If the window function is non-immutable, it can not
			 * be shipped to the datanodes.
			 */
			if (sc_context->sc_for_expr ||
				!pgxc_is_func_shippable(winf->winfnoid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_WindowClause:
		{
			/*
			 * A window function can be evaluated on a Datanode if there is
			 * only one Datanode involved.
			 */
			pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * A window function is not shippable as part of a stand-alone
			 * expression
			 */
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_JoinExpr:
			/* We don't expect JoinExpr in a stand-alone expression */
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/*
			 * The shippability of join will be deduced while
			 * examining the jointree of the query. Nothing to do here
			 */
			break;

		case T_SubLink:
		{
			/*
			 * We need to walk the tree in sublink to check for its
			 * shippability. We need to call pgxc_is_query_shippable() on Query
			 * instead of this function so that every subquery gets a different
			 * context for itself. We should avoid the default expression walker
			 * getting called on the subquery. At the same time we don't want to
			 * miss any other member (current or future) of this structure, from
			 * being scanned. So, copy the SubLink structure with subselect
			 * being NULL and call expression_tree_walker on the copied
			 * structure.
			 */
			SubLink		sublink = *(SubLink *)node;
			ExecNodes	*sublink_en;
			/*
			 * Walk the query and find the nodes where the query should be
			 * executed and node distribution. Merge this with the existing
			 * node list obtained for other subqueries. If merging fails, we
			 * can not ship the whole query.
			 */
			if (IsA(sublink.subselect, Query))
            {
				sublink_en = pgxc_is_query_shippable((Query *)(sublink.subselect),
													 sc_context->sc_query_level + 1,
													 sc_context->boundParams,
													 sc_context->cached_param_num);
				if (!((Query *)(sublink.subselect))->canShip && sc_context->sc_query)
					sc_context->sc_query->canShip = false;
            }
			else
				sublink_en = NULL;

			/* PGXCTODO free the old sc_subquery_en. */
			/* If we already know that this query does not have a set of nodes
			 * to evaluate on, don't bother to merge again.
			 */
			if (!pgxc_test_shippability_reason(sc_context, SS_NO_NODES))
			{
				/*
				 * If this is the first time we are finding out the nodes for
				 * SubLink, we don't have anything to merge, just assign.
				 */
				if (!sc_context->sc_subquery_en)
					sc_context->sc_subquery_en = sublink_en;
				/*
				 * Merge if only the accumulated SubLink ExecNodes and the
				 * ExecNodes for this subquery are both replicated.
				 */
				else if (sublink_en && IsExecNodesReplicated(sublink_en) &&
							IsExecNodesReplicated(sc_context->sc_subquery_en))
				{
					sc_context->sc_subquery_en = pgxc_merge_exec_nodes(sublink_en,
																   sc_context->sc_subquery_en);
				}
				else
					sc_context->sc_subquery_en = NULL;

				/*
				 * If we didn't find a cumulative ExecNodes, set shippability
				 * reason, so that we don't bother merging future sublinks.
				 */
				if (!sc_context->sc_subquery_en)
					pgxc_set_shippability_reason(sc_context, SS_NO_NODES);
			}
			else
				Assert(!sc_context->sc_subquery_en);

			/* Check if the type of sublink result is shippable */
			pgxc_set_exprtype_shippability(exprType(node), sc_context);

			/* Wipe out subselect as explained above and walk the copied tree */
			sublink.subselect = NULL;
			return expression_tree_walker((Node *)&sublink, pgxc_shippability_walker,
											sc_context);
		}
		break;

		case T_OnConflictExpr:
			return false;
		case T_SetOperationStmt:
			break;
		case T_ConnectByExpr:
		break;
		case T_SubPlan:
		case T_AlternativeSubPlan:
		case T_PlaceHolderVar:
		case T_AppendRelInfo:
		case T_PlaceHolderInfo:
		case T_WithCheckOption:
		{
			/* PGXCTODO: till we exhaust this list */
			pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			/*
			 * These expressions are not supported for shippability entirely, so
			 * there is no need to walk trees underneath those. If we do so, we
			 * might walk the trees with wrong context there.
			 */
			return false;
		}
		break;
		case T_CommonTableExpr:
		{
			CommonTableExpr	*cte = (CommonTableExpr *) node;
			ExecNodes       *sublink_en;

			pgxc_set_shippability_reason(sc_context, SS_REPLICATED_ONLY);

			/*
			 * We need to walk the tree in sublink to check for its
			 * shippability. We need to call pgxc_is_query_shippable() on Query
			 * instead of this function so that every subquery gets a different
			 * context for itself. We should avoid the default expression walker
			 * getting called on the subquery. At the same time we don't want to
			 * miss any other member (current or future) of this structure, from
			 * being scanned. So, copy the SubLink structure with subselect
			 * being NULL and call expression_tree_walker on the copied
			 * structure.
			 */
			/*
			 * Walk the query and find the nodes where the query should be
			 * executed and node distribution. Merge this with the existing
			 * node list obtained for other subqueries. If merging fails, we
			 * can not ship the whole query.
			 */
			if (IsA(cte->ctequery, Query))
			{
				sublink_en = pgxc_is_query_shippable((Query *) (cte->ctequery),
													 sc_context->sc_query_level + 1,
													 sc_context->boundParams,
													 sc_context->cached_param_num);
				if (!((Query *)(cte->ctequery))->canShip && sc_context->sc_query)
					sc_context->sc_query->canShip = false;
			}
			else
				sublink_en = NULL;

			/* PGXCTODO free the old sc_subquery_en. */
			/* If we already know that this query does not have a set of nodes
			 * to evaluate on, don't bother to merge again.
			 */
			if (!pgxc_test_shippability_reason(sc_context, SS_NO_NODES))
			{
				/*
				 * If this is the first time we are finding out the nodes for
				 * SubLink, we don't have anything to merge, just assign.
				 */
				if (!sc_context->sc_subquery_en)
					sc_context->sc_subquery_en = sublink_en;
					/*
					 * Merge if only the accumulated SubLink ExecNodes and the
					 * ExecNodes for this subquery are both replicated.
					 */
				else if (sublink_en && IsExecNodesReplicated(sublink_en) &&
						 IsExecNodesReplicated(sc_context->sc_subquery_en))
				{
					sc_context->sc_subquery_en = pgxc_merge_exec_nodes(sublink_en,
																	   sc_context->sc_subquery_en);
				}
				else
					sc_context->sc_subquery_en = NULL;

				/*
				 * If we didn't find a cumulative ExecNodes, set shippability
				 * reason, so that we don't bother merging future sublinks.
				 */
				if (!sc_context->sc_subquery_en)
					pgxc_set_shippability_reason(sc_context, SS_NO_NODES);
			}
			else
				Assert(!sc_context->sc_subquery_en);

			return false;
		}
		break;
		case T_GroupingFunc:
			/*
			 * Let expression tree walker inspect the arguments. Not sure if
			 * that's necessary, as those are just references to grouping
			 * expressions of the query (and thus likely examined as part
			 * of another node).
			 */
			return expression_tree_walker(node, pgxc_shippability_walker,
										  sc_context);
		case T_MergeAction:
			return expression_tree_walker(node, pgxc_shippability_walker,
			                              sc_context);
		case T_RownumExpr:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);
			break;
		case T_LevelExpr:
		case T_CBIsLeafExpr:
		case T_CBIsCycleExpr:
			break;
		case T_PriorExpr:
		case T_CBRootExpr:
		case T_CBPathExpr:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}

	return expression_tree_walker(node, pgxc_shippability_walker, (void *)sc_context);
}


/*
 * pgxc_query_needs_coord
 * Check if the query needs Coordinator for evaluation or it can be completely
 * evaluated on Coordinator. Return true if so, otherwise return false.
 */
static bool
pgxc_query_needs_coord(Query *query)
{
	/*
	 * If the query involves just the catalog tables, and is not an EXEC DIRECT
	 * statement, it can be evaluated completely on the Coordinator. No need to
	 * involve Datanodes.
	 */
	if (pgxc_query_contains_only_pg_catalog(query))
		return true;

	return false;
}


/*
 * pgxc_is_var_distrib_column
 * Check if given var is a distribution key.
 */
static bool
pgxc_is_var_distrib_column(Var *var, List *rtable)
{
	RangeTblEntry   *rte = rt_fetch(var->varno, rtable);
	RelationLocInfo	*rel_loc_info;

	/* distribution column only applies to the relations */
	if (rte->rtekind != RTE_RELATION ||
		(rte->relkind != RELKIND_RELATION &&
		 rte->relkind != RELKIND_PARTITIONED_TABLE))
		return false;

	rel_loc_info = GetRelationLocInfo(rte->relid);
	/* return true if var is the only distrib column. */
	if (!rel_loc_info || rel_loc_info->nDisAttrs != 1)
		return false;

	/* XXX: support multi-keys？*/
	if (var->varattno != rel_loc_info->disAttrNums[0])
		return false;

	return true;
}

#ifdef __OPENTENBASE__
static bool
pgxc_is_shard_in_same_group(Var *var1, Var *var2, List *rtable)
{
	bool result = true;
	RangeTblEntry   *rte1 = rt_fetch(var1->varno, rtable);
	RelationLocInfo	*rel_loc_info1 = GetRelationLocInfo(rte1->relid);
	RangeTblEntry   *rte2 = rt_fetch(var2->varno, rtable);
	RelationLocInfo	*rel_loc_info2 = GetRelationLocInfo(rte2->relid);

	if (rel_loc_info1->locatorType == LOCATOR_TYPE_SHARD &&
		rel_loc_info2->locatorType == LOCATOR_TYPE_SHARD)
	{
		if (rel_loc_info1->groupId != rel_loc_info2->groupId)
		{
			result = false;
		}
	}
	else if (rel_loc_info1->locatorType == LOCATOR_TYPE_SHARD &&
		     rel_loc_info2->locatorType != LOCATOR_TYPE_SHARD)
	{
		result = false;
	}
	else if (rel_loc_info1->locatorType != LOCATOR_TYPE_SHARD &&
		     rel_loc_info2->locatorType == LOCATOR_TYPE_SHARD)
	{
		result = false;
	}

	return result;
}

/*
 * Check is the subquery is simple enough to pushdown to DN
 */
static bool
pgxc_is_simple_subquery(Query *query)
{
	/*
	 * Let's just make sure it's a valid select ...
	 */
	if (!IsA(query, Query) || query->commandType != CMD_SELECT)
		return false;

	/*
	 * Can't currently pushdown a query with setops (unless it's simple UNION
	 * ALL, which is handled by a different code path).
	 */
	if (query->setOperations)
		return false;

	/*
	 * Can't pushdown a subquery involving grouping, aggregation, SRFs,
	 * sorting, limiting, or WITH.
	 */
	if (query->hasAggs ||
		query->hasWindowFuncs ||
		query->hasTargetSRFs ||
		query->groupClause ||
		query->groupingSets ||
		query->havingQual ||
		query->sortClause ||
		query->distinctClause ||
		query->limitOffset ||
		query->limitCount ||
		query->hasForUpdate ||
		query->cteList)
		return false;

	/*
	 * Don't pushdown a subquery that has any volatile functions in its
	 * targetlist.  Otherwise we might introduce multiple evaluations of these
	 * functions, if they get copied to multiple places in the upper query,
	 * leading to surprising results.  (Note: the PlaceHolderVar mechanism
	 * doesn't quite guarantee single evaluation; else we could pull up anyway
	 * and just wrap such items in PlaceHolderVars ...)
	 */
	if (contain_volatile_functions((Node *) query->targetList))
		return false;

	return true;
}
#endif

/*
 * Returns whether or not the rtable (and its subqueries)
 * only contain pg_catalog entries.
 */
static bool
pgxc_query_contains_only_pg_catalog(Query *query)
{
	ListCell	*item;
	List		*rtable = query->rtable;

	/* May be complicated. Before giving up, just check for pg_catalog usage */
	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (get_rel_namespace(rte->relid) != PG_CATALOG_NAMESPACE)
				return false;
		}
		else if (rte->rtekind == RTE_CTE)
		{
			if (rte->subquery == NULL)
				continue;

			if (!pgxc_query_contains_only_pg_catalog((Query *)rte->subquery))
				return false;
		}
		else if (rte->rtekind == RTE_SUBQUERY &&
				 !pgxc_query_contains_only_pg_catalog(rte->subquery))
			return false;
	}
	return true;
}

static ExecNodes *
make_FQS_single_node()
{
	ExecNodes       *exec_nodes;
	exec_nodes = makeNode(ExecNodes);
	exec_nodes->accesstype = RELATION_ACCESS_READ_FQS;
	exec_nodes->baselocatortype = LOCATOR_TYPE_SHARD;
	exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, 0);
	return exec_nodes;
}

static bool
has_all_dis_exprs(ExecNodes *exec_nodes)
{
	int i = 0;

	if (exec_nodes == NULL)
		return false;

	for (i = 0; i < exec_nodes->nExprs; i++)
	{
		if (exec_nodes->dis_exprs[i] == NULL)
			return false;
	}
	return true;
}
/*
 * pgxc_is_query_shippable
 * This function calls the query walker to analyse the query to gather
 * information like  Constraints under which the query can be shippable, nodes
 * on which the query is going to be executed etc.
 * Based on the information gathered, it decides whether the query can be
 * executed on Datanodes directly without involving Coordinator.
 * If the query is shippable this routine also returns the nodes where the query
 * should be shipped. If the query is not shippable, it returns NULL.
 */
ExecNodes *
pgxc_is_query_shippable(Query *query, int query_level, 
						ParamListInfo boundParams, int cached_param_num)
{
	Shippability_context sc_context;
	ExecNodes	*exec_nodes;
	bool		canShip = true;
	Bitmapset	*shippability;

	memset(&sc_context, 0, sizeof(sc_context));
	/* let's assume that by default query is shippable */
	sc_context.sc_query = query;
	sc_context.sc_query_level = query_level;
	sc_context.sc_for_expr = false;
	sc_context.boundParams = boundParams;
	sc_context.cached_param_num = cached_param_num;

	/* multi insert can not ship */
	if (ORA_MODE && query->commandType == CMD_INSERT && query->multi_inserts != NULL)
	{
		ListCell    *info_cell;
		ExecNodes *mr_ens = NULL;

		query->canShip = true;
		foreach(info_cell, query->multi_inserts)
		{
			MultiInsertInto *info = (MultiInsertInto *) lfirst(info_cell);
			ListCell    *sub_query_lc;
			foreach(sub_query_lc, info->sub_intos)
			{
				Query *sub_query = (Query *) lfirst(sub_query_lc);

				/* call this walker to ensure triggers work */
				ExecNodes *ens = pgxc_is_query_shippable(sub_query, 0, boundParams, cached_param_num);
				if (!sub_query->canShip)
					query->canShip = false;

				if (sub_query->hasUnshippableDml)
				{
					ListCell *again;

					/*
					 * once we found a resultRelation with triggers, set all the
					 * other relations force to execute trigger process:
					 * 1. set flag
					 * 2. create remote dml query inside it's own plan
					 * 3. execute remote dml on CN
					 */
					foreach(again, info->sub_intos)
					{
						sub_query = (Query *) lfirst(again);
						sub_query->hasUnshippableDml = true;
					}
					/*
					 * also set parant query has triggers, make sure there is no
					 * remote subplan on top of plan, then break
					 */
					query->hasUnshippableDml = true;
				}

				/* only when all the sub insert can ship to the same single node, the whole multi insert can ship  */
				if (ens == NULL || list_length(ens->nodeList) != 1)
				{
					mr_ens = NULL;
					query->canShip = false;
				}

				mr_ens = mr_ens == NULL ? ens : pgxc_merge_exec_nodes(mr_ens, ens);
				if (mr_ens == NULL || list_length(mr_ens->nodeList) != 1)
				{
					mr_ens = NULL;
					query->canShip = false;
				}
			}
		}

		/* anyway, disable fqs for multi insert here */
		mr_ens = NULL;
		query->canShip = false;
		return mr_ens;
	}

	/*
	 * We might have already decided not to ship the query to the Datanodes, but
	 * still walk it anyway to find out if there are any subqueries which can be
	 * shipped.
	 */
	query->canShip = true;
	pgxc_shippability_walker((Node *)query, &sc_context);

	exec_nodes = sc_context.sc_exec_nodes;

	/*
	 * For single datanode and select command, if we don't need coord
	 * and exec_nodes exists, return it directly. But if exec_nodes is
	 * NULL we make exec_nodes for FQS; SetOperations are excluded
	 * because dn planner may append targetlists which coordinator do not know.
	 */
	if (!bms_is_member(SS_NEEDS_COORD, sc_context.sc_shippability) &&
		query->commandType == CMD_SELECT && query->setOperations == NULL)
	{
		int	dn_nodes_num = NumDataNodes;
#ifdef __SUPPORT_CENTRALIZED_MODE__
		/*
		 * We get dn master number by involved PGXCGetAllMasterDnOid
		 * instead of NumDataNodes, because in centralized mode,
		 * NumDataNodes include dn master and dn slave.
		 */
		if (cluster_mode == CENTRALIZED_MODE)
		{
			Oid	*dn_node_list = (Oid *) palloc0(dn_nodes_num * sizeof(Oid));
			dn_nodes_num = PGXCGetAllMasterDnOid(dn_node_list);
			Assert(dn_nodes_num > 0);
			pfree(dn_node_list);
		}
#endif
		if (dn_nodes_num == 1)
		{
			if (exec_nodes)
				return exec_nodes;
			return make_FQS_single_node();
		}
	}

	/*
	 * The shippability context contains two ExecNodes, one for the subLinks
	 * involved in the Query and other for the relation involved in FromClause.
	 * They are computed at different times while scanning the query. Merge both
	 * of them if they are both replicated. If query doesn't have SubLinks, we
	 * don't need to consider corresponding ExecNodes.
	 * PGXC_FQS_TODO:
	 * Merge the subquery ExecNodes if both of them are replicated.
	 * The logic to merge node lists with other distribution
	 * strategy is not clear yet.
	 */
	if (query->hasSubLinks)
	{
        exec_nodes = merge_sublink_exec_nodes(query, &sc_context, exec_nodes);
    }

	/*
	 * Copy the shippability reasons. We modify the copy for easier handling.
	 * The original can be saved away
	 */
	shippability = bms_copy(sc_context.sc_shippability);

	if (bms_is_member(SS_UNSHIPPABLE_EXPR, shippability))
		query->canShip = false;

	/* If we found the datanodes to ship, use them */
	if (exec_nodes && exec_nodes->nodeList)
	{
		/*
		 * If relations involved in the query are such that ultimate JOIN is
		 * replicated JOIN, choose only one of them. If one of them is a
		 * preferred node choose that one, otherwise choose the first one.
		 */
		if ((IsLocatorReplicated(exec_nodes->baselocatortype) || LOCATOR_TYPE_FOREIGN == exec_nodes->baselocatortype) &&
		    (exec_nodes->accesstype == RELATION_ACCESS_READ ||
		     exec_nodes->accesstype == RELATION_ACCESS_READ_FQS ||
		     exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE))
		{
			/*
			 * Replication table execute "SELECT FOR UPDATE", it is necessary to
			 * send the command to all DNs to lock the rows.
			 */
			if (query_level == 0 && exec_nodes->accesstype != RELATION_ACCESS_READ_FOR_UPDATE)
			{
				List *tmp_list = exec_nodes->nodeList;
				exec_nodes->nodeList = GetPreferredReplicationNode(exec_nodes->nodeList);
				list_free(tmp_list);
			}

			shippability = bms_del_member(shippability, SS_REPLICATED_ONLY);
		}
	}

	if (query_level != 0 && pgxc_FQS_check_subquery_const(query))
	{
		int i;

		exec_nodes = makeNode(ExecNodes);
		exec_nodes->baselocatortype = LOCATOR_TYPE_REPLICATED;
		/*
		 * No locate info stored for such subquery RTEs, we use this
		 * flag to force using the other hand locate info.
		 */
		exec_nodes->const_subquery = true;
		for (i = 0; i < NumDataNodes; ++i)
			exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, i);
		return exec_nodes;
	}

	/*
	 * Look at the information gathered by the walker in Shippability_context and that
	 * in the Query structure to decide whether we should ship this query
	 * directly to the Datanode or not
	 */

	/* SS_NEED_PROCESS_IN_CN: functions should be called in CN. */
	if (ORA_MODE && (query->commandType == CMD_UPDATE ||
		query->commandType == CMD_INSERT ||
		query->commandType == CMD_DELETE) &&
		bms_is_member(SS_NEED_PROCESS_IN_CN, shippability))
		query->hasUnshippableDml = true;

	/*
	 * If the planner was not able to find the Datanodes to the execute the
	 * query, the query is not completely shippable. So, return NULL
	 */
	if (!exec_nodes)
		return NULL;
	/*
	 * If the query has an expression which renders the shippability to single
	 * node, and query needs to be shipped to more than one node, it can not be
	 * shipped
	 */
	if (bms_is_member(SS_NEED_SINGLENODE, shippability))
	{
		/*
		 * if nodeList has no nodes, it ExecNodes will have other means to know
		 * the nodes where to execute like distribution column expression. We
		 * can't tell how many nodes the query will be executed on, hence treat
		 * that as multiple nodes.
		 * Additionally, replication can be pushed down.
		 */
		if (exec_nodes->baselocatortype == LOCATOR_TYPE_REPLICATED)
			canShip = true;
		else if (list_length(exec_nodes->nodeList) > 1)
			canShip = false;
		else if (list_length(exec_nodes->nodeList) == 1)
			canShip = true;
		else if (!((exec_nodes->baselocatortype == LOCATOR_TYPE_HASH ||
		            exec_nodes->baselocatortype == LOCATOR_TYPE_SHARD ||
		            exec_nodes->baselocatortype == LOCATOR_TYPE_DISTRIBUTED) &&
		           (has_all_dis_exprs(exec_nodes) ||
		            list_length(exec_nodes->nodeList) == 1)))
			canShip = false;

		/* We handled the reason here, reset it */
		shippability = bms_del_member(shippability, SS_NEED_SINGLENODE);
	}

	if (query_level != 0 && exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE)
		canShip = false;

	/*
	 * If HAS_AGG_EXPR is set but NEED_SINGLENODE is not set, it means the
	 * aggregates are entirely shippable, so don't worry about it.
	 */
	shippability = bms_del_member(shippability, SS_HAS_AGG_EXPR);

	/*
	 * If an insert sql command whose distribute key's value is a
	 * function, we allow it to be shipped to datanode. But we must
	 * must know the function's result before real execute. So set
	 * the flag to identify rewrite in ExecutePlan.
	 */
	if (bms_is_member(SS_NEED_FUNC_CONVERT_TO_PARAM, shippability) &&
		(IsLocatorColumnDistributed(exec_nodes->baselocatortype) ||
		IsLocatorDistributedByValue(exec_nodes->baselocatortype)))
	{
		shippability = bms_del_member(shippability, SS_NEED_FUNC_CONVERT_TO_PARAM);
	}

	/* Can not ship the query for some reason */
	if (!bms_is_empty(shippability))
		canShip = false;

	/* Always keep this at the end before checking canShip and return */
	if (!canShip && exec_nodes)
		FreeExecNodes(&exec_nodes);
	/* If query is to be shipped, we should know where to execute the query */
	Assert (!canShip || exec_nodes);

	bms_free(shippability);
	shippability = NULL;

	return exec_nodes;
}

static ExecNodes*
merge_sublink_exec_nodes(const Query *query, Shippability_context *sc_context, ExecNodes *exec_nodes)
{
    int num_fromclause_nodes = 0;
    int num_sublink_nodes = 0;

    /* Get number of DN nodes for Main Query result */
    if (exec_nodes && exec_nodes->nodeList)
    {
        num_fromclause_nodes = list_length(exec_nodes->nodeList);
    }

    /* Get number of DN nodes for Sublink result */
    if (sc_context->sc_subquery_en && sc_context->sc_subquery_en->nodeList)
    {
        num_sublink_nodes = list_length(sc_context->sc_subquery_en->nodeList);
    }

    /*
    * Try to merge sublink nodelist only if:
    * XXX Only cover CMD_SELECT
    * XXX Both main query and sublink results got single DN node
    * XXX With same column distributed type
    */
    if (enable_subquery_shipping &&
        exec_nodes && sc_context->sc_subquery_en &&
        query->commandType == CMD_SELECT &&
        IsExecNodesColumnDistributed(exec_nodes) &&
        IsExecNodesColumnDistributed(sc_context->sc_subquery_en) &&
        exec_nodes->baselocatortype == sc_context->sc_subquery_en->baselocatortype &&
        (num_fromclause_nodes == 1) && (num_sublink_nodes == 1))
    {
        exec_nodes = pgxc_merge_exec_nodes(exec_nodes, sc_context->sc_subquery_en);
    }
    /* Fall back to PGXC logic that only try with replicated type */
    else if (sc_context->sc_subquery_en &&
             IsExecNodesReplicated(sc_context->sc_subquery_en))
    {
        exec_nodes = pgxc_merge_exec_nodes(exec_nodes, sc_context->sc_subquery_en);
    }
    else
    {
        exec_nodes = NULL;
    }
    return exec_nodes;
}


/*
 * pgxc_is_expr_shippable
 * Check whether the given expression can be shipped to datanodes.
 *
 * Note on has_aggs
 * The aggregate expressions are not shippable if they can not be completely
 * evaluated on a single datanode. But this function does not have enough
 * context to determine the set of datanodes where the expression will be
 * evaluated. Hence, the caller of this function can handle aggregate
 * expressions, it passes a non-NULL value for has_aggs. This function returns
 * whether the expression has any aggregates or not through this argument. If a
 * caller passes NULL value for has_aggs, this function assumes that the caller
 * can not handle the aggregates and deems the expression has unshippable.
 */
bool
pgxc_is_expr_shippable(Expr *node, bool *has_aggs)
{
	Shippability_context sc_context;

	/* Create the FQS context */
	memset(&sc_context, 0, sizeof(sc_context));
	sc_context.sc_query = NULL;
	sc_context.sc_query_level = 0;
	sc_context.sc_for_expr = true;

	/* Walk the expression to check its shippability */
	pgxc_shippability_walker((Node *)node, &sc_context);

	/*
	 * If caller is interested in knowing, whether the expression has aggregates
	 * let the caller know about it. The caller is capable of handling such
	 * expressions. Otherwise assume such an expression as not shippable.
	 */
	if (has_aggs)
		*has_aggs = pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);
	else if (pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR))
		return false;
	/* Done with aggregate expression shippability. Delete the status */
	pgxc_reset_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);

	/* If there are reasons why the expression is unshippable, return false */
	if (!bms_is_empty(sc_context.sc_shippability))
		return false;

	/* If nothing wrong found, the expression is shippable */
	return true;
}

#ifdef _PG_ORCL_
static bool
pgxc_is_withfunc_shippable(List *funcnsp, int funcid)
{
	return (func_volatile_withfuncs(funcnsp, funcid) == PROVOLATILE_IMMUTABLE);
}
#endif

/*
 * pgxc_is_func_shippable
 * Determine if a function is shippable
 */
bool
pgxc_is_func_shippable(Oid funcid)
{	
#ifdef __OPENTENBASE__
	bool result = false;
	
	switch(funcid)
	{
		case 2026 : /* pg_backend_pid */
		case 2196 : /* inet_client_addr */
		case 2197 : /* inet_client_port */
		case 2198 : /* inet_server_addr */
		case 2199 : /* inet_server_port */
		case 2560 : /* pg_postmaster_start_time */
		{
			result = false;
			break;
		}
		case 4217: /* opentenbase_ora_collect_func */
		case 4216: /* opentenbase_ora_collect_exists */
		case 4215: /* opentenbase_ora_collect_proc */
		case 4219: /* opentenbase_ora_collect_func_text */
		case 4220: /* opentenbase_ora_collect_exists_text */
		case 4221: /* opentenbase_ora_collect_proc_text */
		case 4222: /* opentenbase_ora_collect_get_subscript */
		case 4223: /* opentenbase_ora_collect_get_subscript */
		case 4235: /* opentenbase_ora_collect_get_subscript */
		case 4236: /* opentenbase_ora_collect_func */
		case 4237: /* opentenbase_ora_collect_exists */
		case 4238: /* opentenbase_ora_collect_proc */
		case 4239: /* opentenbase_ora_collect_get_subscript */
		case 4240: /* opentenbase_ora_collect_func_text */
		case 4241: /* opentenbase_ora_collect_exists_text */
		case 4242: /* opentenbase_ora_collect_proc_text */
		case 4243: /* opentenbase_ora_collect_get_subscript */
		{
			result = false;
			break;
		}
		default:
		{
			bool	is_plsql_func;
			Oid		pl_lang = InvalidOid;

			if (ORA_MODE)
				pl_lang = get_language_oid("oraplsql", true);
			else
				pl_lang = get_language_oid("plpgsql", true);

			is_plsql_func = (get_func_lang(funcid) == pl_lang);

			if (funcid >= FirstNormalObjectId && is_plsql_func)
			{
				/*
				 * If it's a plsql user defined function, we should check
				 * it's cost to see whether it's forced as "pushdown".
				 * Immutable function is considered inside it.
				 *
				 * The key point here is different from system function is
				 * that we will not ship a STABLE UDF if it was defined as
				 * "pullup".
				 *
				 * For historical reason, it's name still is
				 * "contain_user_defined_functions_checker", we should rename
				 * it in the future.
				 */
				result = !contain_user_defined_functions_checker(funcid, NULL, NULL);
			}
			else
			{
				/* if not, not shipping VOLATILE function only */
				result = (func_volatile(funcid) != PROVOLATILE_VOLATILE);
			}
			break;
		}
	}
	return result;
#else
	/*
     * For the time being a function is thought as shippable
     * only if it is immutable.
     */
	return (func_volatile(funcid) == PROVOLATILE_IMMUTABLE);	
#endif	
}


/*
 * pgxc_find_dist_equijoin_qual
 * Check equijoin conditions on given relations
 */
static Expr *
pgxc_find_dist_equijoin_qual(Relids varnos_1,
		Relids varnos_2, Oid distcol_type, Node *quals, List *rtable)
{
	List		*lquals;
	ListCell	*qcell;

	/* If no quals, no equijoin */
	if (!quals)
		return NULL;
	/*
	 * Make a copy of the argument bitmaps, it will be modified by
	 * bms_first_member().
	 */
	varnos_1 = bms_copy(varnos_1);
	varnos_2 = bms_copy(varnos_2);

	if (!IsA(quals, List))
		lquals = make_ands_implicit((Expr *)quals);
	else
		lquals = (List *)quals;

	foreach(qcell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qcell);
		OpExpr *op;
		Var *lvar;
		Var *rvar;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;

		/*
		 * Check if both operands are Vars, if not check next expression */
		if (IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Var))
		{
			lvar = (Var *)linitial(op->args);
			rvar = (Var *)lsecond(op->args);
		}
		else
#ifndef _PG_REGRESS_
		/*
		 * handle IMPLICIT_CAST case here
         */
		{
			Node *left_arg = (Node *)linitial(op->args);
			Node *right_arg = (Node *)lsecond(op->args);
			
			lvar = (Var *)get_var_from_arg(left_arg);
			rvar = (Var *)get_var_from_arg(right_arg);

			if (!lvar || !rvar)
			{
				continue;
			}
		}
#else
			continue;
#endif

		/*
		 * If the data types of both the columns are not same, continue. Hash
		 * and Modulo of a the same bytes will be same if the data types are
		 * same. So, only when the data types of the columns are same, we can
		 * ship a distributed JOIN to the Datanodes
		 */
		if (exprType((Node *)lvar) != exprType((Node *)rvar))
			continue;

		/* if the vars do not correspond to the required varnos, continue. */
		if ((bms_is_member(lvar->varno, varnos_1) && bms_is_member(rvar->varno, varnos_2)) ||
			(bms_is_member(lvar->varno, varnos_2) && bms_is_member(rvar->varno, varnos_1)))
		{
			if (!pgxc_is_var_distrib_column(lvar, rtable) ||
				!pgxc_is_var_distrib_column(rvar, rtable))
				continue;
#ifdef __OPENTENBASE__
			/* join shard tables should in same group */
			if (!pgxc_is_shard_in_same_group(lvar, rvar, rtable))
			{
				continue;
			}
#endif
		}
		else
			continue;
		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)lvar)) &&
			!op_hashjoinable(op->opno, exprType((Node *)lvar)))
			continue;
		/* Found equi-join condition on distribution columns */
		return qual_expr;
	}
	return NULL;
}

#ifdef __OPENTENBASE__
static List*
pgxc_find_dist_equi_nodes(Relids varnos_1, Relids varnos_2, Oid distcol_type,
                          Node *quals, List *rtable, ParamListInfo boundParams)
{
	List		*lquals;
	ListCell	*qcell;

	/* If no quals, no equijoin */
	if (!quals)
		return NULL;
	/*
	 * Make a copy of the argument bitmaps, it will be modified by
	 * bms_first_member().
	 */
	varnos_1 = bms_copy(varnos_1);
	varnos_2 = bms_copy(varnos_2);

	if (!IsA(quals, List))
		lquals = make_ands_implicit((Expr *)quals);
	else
		lquals = (List *)quals;

	foreach(qcell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qcell);
		OpExpr *op;
		Var *var;
		Expr *const_expr;
		Node *left_arg;
		Node *right_arg;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;
		
		left_arg = (Node *)linitial(op->args);
		right_arg = (Node *)lsecond(op->args);
		left_arg = strip_implicit_coercions(left_arg);
		right_arg = strip_implicit_coercions(right_arg);
		left_arg = eval_const_expressions_with_params(boundParams, left_arg);
		right_arg = eval_const_expressions_with_params(boundParams, right_arg);

		if (IsA(left_arg, Var))
		{
			var = (Var *) left_arg;
			const_expr = (Expr *) right_arg;
		}
		else if (IsA(right_arg, Var))
		{
			var = (Var *) right_arg;
			const_expr = (Expr *) left_arg;
		}
		else
		{
			continue;
		}

		/* outer reference variable, skip */
		if (var->varlevelsup != 0)
			continue;

		/* if the vars do not correspond to the required varnos, continue. */
		if ((bms_is_member(var->varno, varnos_1) || bms_is_member(var->varno, varnos_2)))
		{
			if (!pgxc_is_var_distrib_column(var, rtable))
				continue;
		}
        else
		{
			RangeTblEntry *rte = rt_fetch(var->varno, rtable);

			if (rte->rtekind == RTE_JOIN)
			{
				var = (Var *) list_nth(rte->joinaliasvars, var->varattno - 1);

				if ((bms_is_member(var->varno, varnos_1) || bms_is_member(var->varno, varnos_2)))
				{
					if (!pgxc_is_var_distrib_column(var, rtable))
						continue;
				}
				else
					continue;
			}
			else
				continue;
		}

		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)var)) &&
			!op_hashjoinable(op->opno, exprType((Node *)var)))
			continue;
		/* Found equi-qual condition on distribution columns, get executed nodelist */
		if (const_expr)
		{
			RangeTblEntry   *rte = rt_fetch(var->varno, rtable);
			RelationLocInfo	*rel_loc_info = GetRelationLocInfo(rte->relid);
			AttrNumber      partAttrNum = rel_loc_info->nDisAttrs > 0 ? rel_loc_info->disAttrNums[0] : InvalidAttrNumber;
			Oid		        disttype = get_atttype(rte->relid, partAttrNum);
			int32	        disttypmod = get_atttypmod(rte->relid, partAttrNum);

			const_expr = (Expr *)coerce_to_target_type(NULL,
													(Node *)const_expr,
													exprType((Node *)const_expr),
													disttype, disttypmod,
													COERCION_ASSIGNMENT,
													COERCE_IMPLICIT_CAST, -1);

			const_expr = (Expr *)eval_const_expressions_with_params(boundParams,(Node *)const_expr);

			if (const_expr && IsA(const_expr, Const))
			{
				Const *con = (Const *)const_expr;
				ExecNodes *exec_nodes = NULL;
				int     ndiscols = 1;
				Datum   *disvalues = (Datum *) palloc0(sizeof(Datum) * ndiscols);
				bool    *disisnulls = (bool *) palloc0(sizeof(bool) * ndiscols);
				disvalues[0] = con->constvalue;
				disisnulls[0] = con->constisnull;
				
				exec_nodes = GetRelationNodes(rel_loc_info,
											  disvalues, disisnulls, ndiscols,
											  RELATION_ACCESS_READ);
				
				pfree_ext(disvalues);
				pfree_ext(disisnulls);
				return exec_nodes->nodeList;
			}
		}
	}
	return NULL;
}

#endif

/*
 * pgxc_merge_exec_nodes
 * The routine combines the two exec_nodes passed such that the resultant
 * exec_node corresponds to the JOIN of respective relations.
 * If both exec_nodes can not be merged, it returns NULL.
 */
static ExecNodes *
pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2)
{
	ExecNodes	*merged_en = makeNode(ExecNodes);
	/*
	 * If either of exec_nodes are NULL, return NULL to disable FQS.
	 *
	 * Notice: if either side of exec nodes's g_index_table_name is valid
	 * we know it was generated by a global index and nodelist was constrained
	 * by it too, so they can't merge.
	 */
	if (!en1 || !en2||
	    en1->g_index_table_name != NULL ||
	    en2->g_index_table_name != NULL)
	{
		return NULL;
	}


	/* Following cases are not handled in this routine */
	/* PGXC_FQS_TODO how should we handle table usage type? */
	if (en1->primarynodelist || en2->primarynodelist ||
		(en1->nExprs > 0 && en1->dis_exprs[0]) || (en2->nExprs > 0 && en2->dis_exprs[0]) ||
		OidIsValid(en1->en_relid) || OidIsValid(en2->en_relid) ||
		(en1->accesstype != RELATION_ACCESS_READ && en1->accesstype != RELATION_ACCESS_READ_FQS) ||
		(en2->accesstype != RELATION_ACCESS_READ && en2->accesstype != RELATION_ACCESS_READ_FQS))
    {
        if (!((en1->nExprs > 0 && en1->dis_exprs[0] && IsA(en1->dis_exprs[0], Param) && ((Param*)en1->dis_exprs[0])->paramkind == PARAM_EXTERN) ||
        (en2->nExprs > 0 && en2->dis_exprs[0] && IsA(en2->dis_exprs[0], Param) && ((Param*)en2->dis_exprs[0])->paramkind == PARAM_EXTERN)))
		return NULL;
    }
		

	if (IsExecNodesReplicated(en1) &&
		IsExecNodesReplicated(en2))
	{
		/*
		 * Replicated/replicated join case
		 * Check that replicated relation is not disjoint
		 * with initial relation which is also replicated.
		 * If there is a common portion of the node list between
		 * the two relations, other rtables have to be checked on
		 * this restricted list.
		 */
		merged_en->nodeList = list_intersection_int(en1->nodeList,
													en2->nodeList);
		merged_en->baselocatortype = LOCATOR_TYPE_REPLICATED;
		if (!merged_en->nodeList)
			FreeExecNodes(&merged_en);
		return merged_en;
	}

	if (IsExecNodesReplicated(en1) &&
		IsExecNodesColumnDistributed(en2))
	{
		List	*diff_nodelist = NULL;
		/*
		 * Replicated/distributed join case.
		 * Node list of distributed table has to be included
		 * in node list of replicated table.
		 */
		diff_nodelist = list_difference_int(en2->nodeList, en1->nodeList);
		/*
		 * If the difference list is not empty, this means that node list of
		 * distributed table is not completely mapped by node list of replicated
		 * table, so go through standard planner.
		 */
		if (diff_nodelist)
			FreeExecNodes(&merged_en);
		else
			merged_en = copyObject(en2);

		return merged_en;
	}

	if (IsExecNodesColumnDistributed(en1) &&
		IsExecNodesReplicated(en2))
	{
		List *diff_nodelist = NULL;
		/*
		 * Distributed/replicated join case.
		 * Node list of distributed table has to be included
		 * in node list of replicated table.
		 */
		diff_nodelist = list_difference_int(en1->nodeList, en2->nodeList);

		/*
		 * If the difference list is not empty, this means that node list of
		 * distributed table is not completely mapped by node list of replicated
			 * table, so go through standard planner.
		 */
		if (diff_nodelist)
			FreeExecNodes(&merged_en);
		else
			merged_en = copyObject(en1);

		return merged_en;
	}

	if (IsExecNodesColumnDistributed(en1) &&
		IsExecNodesColumnDistributed(en2))
	{
		/*
		 * Distributed/distributed case
		 * If the caller has suggested that this is an equi-join between two
		 * distributed results, check that they have the same nodes in the distribution
		 * node list. The caller is expected to fully decide whether to merge
		 * the nodes or not.
		 */
		if (!list_difference_int(en1->nodeList, en2->nodeList) &&
			!list_difference_int(en2->nodeList, en1->nodeList))
		{
			merged_en->nodeList = list_copy(en1->nodeList);
			if (en1->baselocatortype == en2->baselocatortype)
				merged_en->baselocatortype = en1->baselocatortype;
			else
				merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
		}
		else
			FreeExecNodes(&merged_en);
		return merged_en;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Postgres-XC does not support this distribution type yet"),
			 errdetail("The feature is not currently supported")));

	/* Keep compiler happy */
	return NULL;
}


/*
 * pgxc_is_join_reducible
 * The shippability of JOIN is decided in following steps
 * 1. Are the JOIN conditions shippable?
 * 	For INNER JOIN it's possible to apply some of the conditions at the
 * 	Datanodes and others at coordinator. But for other JOINs, JOIN conditions
 * 	decide which tuples on the OUTER side are appended with NULL columns from
 * 	INNER side, we need all the join conditions to be shippable for the join to
 * 	be shippable.
 * 2. Do the JOIN conditions have quals that will make it shippable?
 * 	When both sides of JOIN are replicated, irrespective of the quals the JOIN
 * 	is shippable.
 * 	INNER joins between replicated and distributed relation are shippable
 * 	irrespective of the quals. OUTER join between replicated and distributed
 * 	relation is shippable if distributed relation is the outer relation.
 * 	All joins between hash/modulo distributed relations are shippable if they
 * 	have equi-join on the distributed column, such that distribution columns
 * 	have same datatype and same distribution strategy.
 * 3. Are datanodes where the joining relations exist, compatible?
 * 	Joins between replicated relations are shippable if both relations share a
 * 	datanode. Joins between distributed relations are shippable if both
 * 	relations are distributed on same set of Datanodes. Join between replicated
 * 	and distributed relations is shippable is replicated relation is replicated
 * 	on all nodes where distributed relation is distributed.
 *
 * The first step is to be applied by the caller of this function.
 */
static ExecNodes *
pgxc_is_join_shippable(ExecNodes *inner_en, ExecNodes *outer_en, Relids in_relids,
						Relids out_relids, JoinType jointype, List *join_quals,
#ifdef __OPENTENBASE__
						Query *query,
#endif
						List *rtables, ParamListInfo boundParams)
{
	bool       merge_nodes = false;
	ExecNodes *merged_en;

	/*
	 * If either of inner_en or outer_en is NULL, return NULL. We can't ship the
	 * join when either of the sides do not have datanodes to ship to.
	 */
	if (!outer_en || !inner_en)
		return NULL;
	/*
	 * We only support reduction of INNER, LEFT [OUTER] and FULL [OUTER] joins.
	 * RIGHT [OUTER] join is converted to LEFT [OUTER] join during join tree
	 * deconstruction.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT && jointype != JOIN_FULL)
		return NULL;

	merged_en = makeNode(ExecNodes);

	/*
	 * If both sides are replicated or have single node each, we ship any kind
	 * of JOIN
	 */
	if ((IsExecNodesReplicated(inner_en) && IsExecNodesReplicated(outer_en) &&
		 !inner_en->const_subquery && !outer_en->const_subquery) ||
		(list_length(inner_en->nodeList) == 1 &&
		 list_length(outer_en->nodeList) == 1))
		merge_nodes = true;

	/* If both sides are distributed, ... */
	else if (IsExecNodesColumnDistributed(inner_en) &&
			 IsExecNodesColumnDistributed(outer_en))
	{
		/*
		 * If two sides are distributed in the same manner by a value, with an
		 * equi-join on the distribution column and that condition
		 * is shippable, ship the join if node lists from both sides can be
		 * merged.
		 */
		if (inner_en->baselocatortype == outer_en->baselocatortype &&
			IsExecNodesDistributedByValue(inner_en) && jointype != JOIN_FULL)
		{
			Expr *equi_join_expr = pgxc_find_dist_equijoin_qual(in_relids,
													out_relids, InvalidOid,
													(Node *)join_quals, rtables);
			if (equi_join_expr && pgxc_is_expr_shippable(equi_join_expr, NULL))
				merge_nodes = true;
#ifdef __OPENTENBASE__
			if (merge_nodes && query->commandType == CMD_SELECT)
			{
				if (outer_en->restrict_shippable || inner_en->restrict_shippable)
				{
					switch (jointype)
					{
						case JOIN_INNER:
						{
							if (outer_en->restrict_shippable)
								merged_en->nodeList = list_copy(outer_en->nodeList);
							if (inner_en->restrict_shippable)
								merged_en->nodeList = list_copy(inner_en->nodeList);
							merged_en->restrict_shippable = true;
							break;
						}
						case JOIN_LEFT:
						{
							merged_en->nodeList = list_copy(outer_en->nodeList);
							if (outer_en->restrict_shippable)
							{
								merged_en->restrict_shippable = true;
							}
							break;
						}
						default:
							break;
					}
					merged_en->baselocatortype = inner_en->baselocatortype;
					return merged_en;
				}
				else
				{
					List *nodelist = NULL;

					switch (jointype)
					{
						case JOIN_INNER:
						{
							nodelist = pgxc_find_dist_equi_nodes(in_relids,
							                                     out_relids,
							                                     InvalidOid,
							                                     (Node *) join_quals,
							                                     rtables,
							                                     boundParams);
							if (nodelist)
							{
								merged_en->nodeList = nodelist;
								merged_en->baselocatortype = inner_en->baselocatortype;
								merged_en->restrict_shippable = true;
								break;
							}

							nodelist = pgxc_find_dist_equi_nodes(
								in_relids,
								out_relids,
								InvalidOid,
								(Node *) make_ands_implicit((Expr *) query->jointree->quals),
								rtables,
								boundParams);
							if (nodelist)
							{
								merged_en->nodeList = nodelist;
								merged_en->baselocatortype = inner_en->baselocatortype;
								merged_en->restrict_shippable = true;
								break;
							}

							break;
						}
						case JOIN_LEFT:
						{
							List *nodelist = NULL;

							nodelist = pgxc_find_dist_equi_nodes(
								in_relids,
								out_relids,
								InvalidOid,
								(Node *) make_ands_implicit((Expr *) query->jointree->quals),
								rtables,
								boundParams);
							if (nodelist)
							{
								merged_en->nodeList = nodelist;
								merged_en->baselocatortype = inner_en->baselocatortype;
								merged_en->restrict_shippable = true;
								break;
							}

							break;
						}
						default:
							break;
					}
				}
			}
#endif
		}
	}
	/*
	 * If outer side is distributed and inner side is replicated, we can ship
	 * LEFT OUTER and INNER join.
	 */
	else if (IsExecNodesColumnDistributed(outer_en) &&
			 IsExecNodesReplicated(inner_en) &&
			 (jointype == JOIN_INNER || jointype == JOIN_LEFT))
	{
		merge_nodes = true;
#ifdef __OPENTENBASE__
		/*
		 * Push down to restrict datanodes based if join is on distributed
		 * column or related qual
		 */
		if (query->commandType == CMD_SELECT && !outer_en->restrict_shippable)
		{
			List *nodelist = NULL;

			switch (jointype)
			{
				case JOIN_INNER:
				{
					nodelist = pgxc_find_dist_equi_nodes(in_relids,
					                                     out_relids,
					                                     InvalidOid,
					                                     (Node *) join_quals,
					                                     rtables,
					                                     boundParams);

					if (nodelist && !list_difference_int(nodelist, inner_en->nodeList))
					{
						merged_en->nodeList = nodelist;
						merged_en->baselocatortype = outer_en->baselocatortype;
						merged_en->restrict_shippable = true;
						break;
					}
					/* else fallthrough */
				}

				case JOIN_LEFT:
				{
					nodelist = pgxc_find_dist_equi_nodes(
						in_relids,
						out_relids,
						InvalidOid,
						(Node *) make_ands_implicit((Expr *) query->jointree->quals),
						rtables,
						boundParams);

					if (nodelist && !list_difference_int(nodelist, inner_en->nodeList))
					{
						merged_en->nodeList = nodelist;
						merged_en->baselocatortype = outer_en->baselocatortype;
						merged_en->restrict_shippable = true;
						break;
					}
				}
				break;
				default:
					break;
			}
		}

		/* Inner side is constant subquery */
		if (enable_subquery_shipping && inner_en->const_subquery && !merged_en->nodeList)
		{
			merged_en->nodeList = list_copy(outer_en->nodeList);
			merged_en->baselocatortype = outer_en->baselocatortype;
		}
#endif
	}
	/*
	 * If outer side is replicated and inner side is distributed, we can ship
	 * only for INNER join.
	 */
	else if (IsExecNodesReplicated(outer_en) &&
			 IsExecNodesColumnDistributed(inner_en) &&
			 jointype == JOIN_INNER)
	{
		merge_nodes = true;
#ifdef __OPENTENBASE__
		/*
		 * Push down to restrict datanodes based if join is on distributed
		 * column or related qual
		 */
		if (query->commandType == CMD_SELECT && !inner_en->restrict_shippable)
		{
			List *nodelist = NULL;
			
			nodelist = pgxc_find_dist_equi_nodes(in_relids, out_relids,
			                                     InvalidOid, (Node *)join_quals,
			                                     rtables, boundParams);
			if (nodelist && !list_difference_int(nodelist, outer_en->nodeList))
			{
				merged_en->nodeList = nodelist;
				merged_en->baselocatortype = inner_en->baselocatortype;
				merged_en->restrict_shippable = true;
			}
			
			nodelist = pgxc_find_dist_equi_nodes(in_relids, out_relids,
			                                     InvalidOid,
			                                     (Node *)make_ands_implicit((Expr *)query->jointree->quals),
			                                     rtables, boundParams);
			if (nodelist && !list_difference_int(nodelist, outer_en->nodeList))
			{
				merged_en->nodeList = nodelist;
				merged_en->baselocatortype = inner_en->baselocatortype;
				merged_en->restrict_shippable = true;
			}
		}

		/* Outer side is constant subquery */
		if (enable_subquery_shipping && outer_en->const_subquery && !merged_en->nodeList)
		{
			merged_en->nodeList = list_copy(inner_en->nodeList);
			merged_en->baselocatortype = inner_en->baselocatortype;
		}
#endif
	}

	/*
	 * If the ExecNodes of inner and outer nodes can be merged, the JOIN is
	 * shippable
	 */
	if (!merge_nodes)
	{
		FreeExecNodes(&merged_en);
		return NULL;
	}

	if (!merged_en->nodeList)
	{
		FreeExecNodes(&merged_en);
		merged_en = pgxc_merge_exec_nodes(inner_en, outer_en);
		if (NULL == merged_en)
			return NULL;
	}

	if (inner_en->groupid == 0)
		merged_en->groupid = outer_en->groupid;
	else if (outer_en->groupid == 0)
		merged_en->groupid = inner_en->groupid;
	else if (outer_en->groupid == inner_en->groupid)
		merged_en->groupid = inner_en->groupid;
	else
		merged_en->groupid = 0;

	if (merged_en->groupid > 0)
		merged_en->dist_attno = list_union(inner_en->dist_attno, outer_en->dist_attno);

	return merged_en;
}

static
bool pgxc_targetlist_has_distcol(Query *query)
{
	RangeTblEntry   *rte = rt_fetch(query->resultRelation, query->rtable);
	RelationLocInfo	*rel_loc_info;
	ListCell   *lc;

	/* distribution column only applies to the relations */
	if (rte->rtekind != RTE_RELATION ||
		(rte->relkind != RELKIND_RELATION &&
		 rte->relkind != RELKIND_PARTITIONED_TABLE))
		return false;
	rel_loc_info = GetRelationLocInfo(rte->relid);
	if (!rel_loc_info)
		return false;

	foreach(lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;
		if (IsRelationDistribColumn(rel_loc_info, tle->resname))
			return true;
	}
	return false;
}

static bool
pgxc_merge_has_distcol(Query *query)
{
	RangeTblEntry	*rte = rt_fetch(query->resultRelation, query->rtable);
	RelationLocInfo *rel_loc_info;
	ListCell		*lc;
	ListCell		*aclc;

	/* distribution column only applies to the relations */
	if (rte->rtekind != RTE_RELATION ||
		(rte->relkind != RELKIND_RELATION &&
		 rte->relkind != RELKIND_PARTITIONED_TABLE))
		return false;
	rel_loc_info = GetRelationLocInfo(rte->relid);
	if (!rel_loc_info)
		return false;

	/* No distribution column if relation is not distributed with a key */
	if (!IsRelationDistributedByValue(rel_loc_info))
		return false;

	foreach (lc, query->mergeActionList)
	{
		MergeAction *ma = lfirst_node(MergeAction, lc);

		foreach (aclc, ma->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, aclc);
			int          i = 0;

			if (tle->resjunk)
				continue;
			for (i = 0; i < rel_loc_info->nDisAttrs; i++)
			{
				if (strcmp(tle->resname,
				           get_attname(rel_loc_info->relid, rel_loc_info->disAttrNums[i])) == 0)
					return true;
			}
		}
	}
	return false;
}
#ifdef __OPENTENBASE__
/*
 * pgxc_check_triggers_shippability:
 * Return true if none of the triggers prevents the query from being FQSed.
 */
bool
pgxc_check_triggers_shippability(Oid relid, int commandType)
{
	bool  has_unshippable_trigger;
	TriggerDesc *trigdesc = NULL;
	int16 trigevent = pgxc_get_trigevent(commandType);
	Relation	rel = relation_open(relid, AccessShareLock);
	RelationLocInfo *rd_locator_info = rel->rd_locator_info;

	/* only process hash and shard table */
	if (rd_locator_info && 
	   (rd_locator_info->locatorType != LOCATOR_TYPE_SHARD))
	{
		relation_close(rel, AccessShareLock);

		return true;
	}

	/*
	 * the relecahe of the table may be rebuilt during the
	 * execution of pgxc_find_unshippable_triggers,
	 * make a copy so as not to depend on relcache info
	 */
	trigdesc = CopyTriggerDesc(rel->trigdesc);
	relation_close(rel, AccessShareLock);

	/*
	 * If we don't find unshippable row trigger, then the statement is
	 * shippable as far as triggers are concerned. For FQSed query, statement
	 * triggers are separately invoked on coordinator.
	 */
	has_unshippable_trigger = pgxc_find_unshippable_triggers(trigdesc, trigevent, 0, true);
	FreeTriggerDesc(trigdesc);
	
	return !has_unshippable_trigger;
}

/*
  * find unshippable triggers if any 
  *
  * If ignore_timing is true, just the trig_event is used to find a match, so
  * once the event matches, the search returns true regardless of whether it is a
  * before or after row trigger.
  *
  * If ignore_timing is false, return true if we find one or more unshippable
  * triggers that match the exact combination of event and timing.
  */
bool
pgxc_find_unshippable_triggers(TriggerDesc *trigdesc, int16 trig_event, 
                                       int16 trig_timing, bool ignore_timing)
{
	int i = 0;

	/* no triggers */
	if (!trigdesc)
		return false;

	/*
	 * Quick check by just scanning the trigger descriptor, before
	 * actually peeking into each of the individual triggers.
	 */
	if (!pgxc_has_trigger_for_event(trig_event, trigdesc))
		return false;

	for (i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trigger = &trigdesc->triggers[i];
		int16		tgtype = trigger->tgtype;

		/*
		 * If we are asked to find triggers of *any* level or timing, just match
		 * the event type to determine whether we should ignore this trigger.
		 */
		if (ignore_timing)
		{
			if ((TRIGGER_FOR_INSERT(trig_event) && !TRIGGER_FOR_INSERT(tgtype)) ||
				(TRIGGER_FOR_UPDATE(trig_event) && !TRIGGER_FOR_UPDATE(tgtype)) ||
				(TRIGGER_FOR_DELETE(trig_event) && !TRIGGER_FOR_DELETE(tgtype)))
				continue;
		}
		else
		{
			/*
			 * Otherwise, do an exact match with the given combination of event
			 * and timing.
			 */
			if (!((tgtype & TRIGGER_TYPE_TIMING_MASK) == trig_timing &&
				(tgtype & trig_event)))
				continue;
		}

		/*
		 * We now know that we cannot ignore this trigger, so check its
		 * shippability.
		 */
		if (!pgxc_is_trigger_shippable(trigger))
			return true;
	}

	return false;
}

static bool
constraint_is_foreign_key(Oid constroid)
{
	Relation	constrRel;
	HeapTuple	constrTup;
	Form_pg_constraint constrForm;
	bool result = false;

	constrRel = heap_open(ConstraintRelationId, AccessShareLock);
	constrTup = get_catalog_object_by_oid(constrRel, constroid);
	if (!HeapTupleIsValid(constrTup))
		elog(ERROR, "cache lookup failed for constraint %u", constroid);

	constrForm = (Form_pg_constraint) GETSTRUCT(constrTup);

	if (constrForm->contype == CONSTRAINT_FOREIGN)
	{
		result = true;
	}
	else
	{
		result = false;
	}

	heap_close(constrRel, AccessShareLock);

	return result;
}

/*
 * pgxc_is_trigger_shippable:
 * Check if trigger is shippable to a remote node. This function would be
 * called both on coordinator as well as datanode. We want this function
 * to be workable on datanode because we want to skip non-shippable triggers
 * on datanode.
 */
bool
pgxc_is_trigger_shippable(Trigger *trigger)
{
	/*
	 * If trigger is based on a constraint or is internal, enforce its launch
	 * whatever the node type where we are for the time being.
	 * PGXCTODO: we need to remove this condition once constraints are better
	 * implemented within Postgres-XC as a constraint can be locally
	 * evaluated on remote nodes depending on the distribution type of the table
	 * on which it is defined or on its parent/child distribution types.
	 */
	if (trigger->tgisinternal)
	{
		if (!OidIsValid(trigger->tgconstraint) || 
			!constraint_is_foreign_key(trigger->tgconstraint) ||
			(GetRelLocatorType(trigger->tgconstrrelid) != LOCATOR_TYPE_SHARD))
			return true;
		if (!pgxc_is_func_shippable(trigger->tgfoid))
			return false;
	}

	/*
	 * INSTEAD OF triggers can only be defined on views, which are defined
	 * only on Coordinators, so they cannot be shipped.
	 */
	if (TRIGGER_FOR_INSTEAD(trigger->tgtype))
		return false;

	if (g_trig_exec_mode == TRIG_EXECMODE_PUSHDOWN)
		return true;
	else if (g_trig_exec_mode == TRIG_EXECMODE_PULLUP)
		return false;
	else /* else check if this function could be pushdown */
		return !func_is_pullup(trigger->tgfoid, NULL);
}

static ExecNodes*
pgxc_is_group_subquery_shippable(Query *query, Shippability_context *sc_context)
{
	ListCell	*lcell;
	ExecNodes   *exec_nodes = NULL;  	
	
	foreach (lcell, query->groupClause)
	{
		SortGroupClause 	*sgc = lfirst(lcell);
		Node				*sgc_expr;
		if (!IsA(sgc, SortGroupClause))
		{
			continue;
		}
		
		sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var))
		{
			Var 			*var       = NULL;
			RangeTblEntry   *rte	   = NULL;			
			var             = (Var *)sgc_expr;
			
			if (var->varlevelsup != 0)
			{
				continue;
			}
			
			rte = rt_fetch(var->varno, query->rtable);	
			if (RTE_SUBQUERY == rte->rtekind)
			{
				Shippability_context local_sc;  
				ExecNodes   *local_exec_nodes_0 = NULL;  
				ExecNodes   *local_exec_nodes_1 = NULL;  

				/* just assume we are a standalone query. */
				memset((char*)&local_sc, 0X00, sizeof(Shippability_context));

				local_sc.sc_query = rte->subquery;
				local_sc.sc_query_level = 0;
				local_sc.sc_for_expr = false;

				local_sc.sc_exec_nodes = pgxc_is_query_shippable(rte->subquery,
				                                                 sc_context->sc_query_level + 1,
				                                                 sc_context->boundParams,
																 sc_context->cached_param_num);
				if (!rte->subquery->canShip)
				{
					query->canShip = false;
					if (sc_context->sc_query)
						sc_context->sc_query->canShip = false;
				}
				pgxc_shippability_walker((Node *) rte->subquery, &local_sc);

				if (local_sc.sc_exec_nodes == NULL)
				{
					pgxc_set_shippability_reason(sc_context, SS_HAS_UNSHIPPABLE_SUBQUERY);
					return NULL;
				}

				if (local_sc.sc_exec_nodes && 1 == list_length(local_sc.sc_exec_nodes->nodeList))
				{
					/* try to merge the exec node to check whether the subquery has the same exec
					 * node as the local one. */
					local_exec_nodes_0 = pgxc_merge_exec_nodes(local_sc.sc_exec_nodes, sc_context->sc_exec_nodes);
					local_exec_nodes_1 = exec_nodes;
					exec_nodes = pgxc_merge_exec_nodes(local_exec_nodes_0, local_exec_nodes_1);

					if (local_exec_nodes_0)
					{
						FreeExecNodes(&local_exec_nodes_0);
					}

					if (local_exec_nodes_1)
					{
						FreeExecNodes(&local_exec_nodes_1);
					}
					
					if (!exec_nodes)
					{	
						/* Can't be push down. */
						pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

						/* Free the structs if needed. */
						if (local_sc.sc_exec_nodes)
						{
							FreeExecNodes(&local_sc.sc_exec_nodes);
						}

						if (local_sc.sc_subquery_en)
						{
							FreeExecNodes(&local_sc.sc_subquery_en);
						}
						return NULL;
					}	

					/* Free the structs if needed. */
					if (local_sc.sc_exec_nodes)
					{
						FreeExecNodes(&local_sc.sc_exec_nodes);
					}

					if (local_sc.sc_subquery_en)
					{
						FreeExecNodes(&local_sc.sc_subquery_en);
					}
				}
				else
				{		
					if (exec_nodes)
					{
						FreeExecNodes(&exec_nodes);
					}
					return NULL;
				}
				
			}
			continue;
		}
	}
	return exec_nodes;
}

Node *
get_var_from_arg(Node *arg)
{
	Var *var = NULL;

	if (!arg)
	{
		return NULL;
	}
	
	switch(nodeTag(arg))
	{
		case T_Var:
			var = (Var *)arg;
			break;
		case T_RelabelType:
			{
				RelabelType *rt = (RelabelType *)arg;

				if (rt->relabelformat == COERCE_IMPLICIT_CAST && 
					IsA(rt->arg, Var))
				{
					var = (Var *)rt->arg;
				}
				break;
			}
		default:
			var = NULL;
			break;
	}

	return (Node *)var;
}
#endif
