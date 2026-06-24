/*-------------------------------------------------------------------------
 *
 * distribution.c
 *	  Routines related to adjust path distribution
 *
 * Copyright (c) 2020-Present OpenTenBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/distribution.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "optimizer/distribution.h"
#include "optimizer/paths.h"
#include "optimizer/pgxcship.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"

/*
 * Allow insert into select statement and the data type is Varchar to avoid
 * generating distribution plan.
 */
bool
extend_equal_disExpr(PlannerInfo *root, Path *path, Node *disExprs1, Node *disExprs2)
{
	if (disExprs1 == NULL || disExprs2 == NULL)
		return false;

	if (root->parse->commandType == CMD_INSERT &&
		IsA(disExprs2, Var) && ((Var*) disExprs2)->vartype == VARCHAROID)
	{
		Node *disVar1 = disExprs1;

		/*
		 * Case for insert varchar(2) select varchar(4)
		 * The node type is FuncExpr with relabelformat as COERCE_IMPLICIT_CAST.
		 */
		if (IsA(disVar1, FuncExpr))
		{
			FuncExpr   *f = (FuncExpr *) disVar1;

			if (f->funcformat == COERCE_IMPLICIT_CAST)
				disVar1 = (Node *)linitial(f->args);
		}
		/*
		 * Case for insert varchar(4) select varchar(2)
		 * The node type is RelabelType with relabelformat as COERCE_EXPLICIT_CAST
		 */
		else if (IsA(disVar1, RelabelType))
		{
			RelabelType *r = (RelabelType *) disVar1;

			if (r->relabelformat == COERCE_EXPLICIT_CAST ||
				r->relabelformat == COERCE_IMPLICIT_CAST)
				disVar1 = (Node *) r->arg;
		}

		if (disVar1 != NULL &&
			IsA(disVar1, Var) && ((Var*) disVar1)->vartype == VARCHAROID)
		{
			ListCell *lc1;
			ListCell *lc2;

			forboth (lc1, root->processed_tlist, lc2, path->pathtarget->exprs)
			{
				TargetEntry *tle1 = (TargetEntry *) lfirst(lc1);

				/* should be the same position in targetlist */
				if (disExprs1 == (Node *) tle1->expr)
				{
					Node *disVar2 = (Node *) lfirst(lc2);

					if (IsA(disVar2, FuncExpr))
					{
						FuncExpr   *f = (FuncExpr *) disVar2;

						if (f->funcformat == COERCE_IMPLICIT_CAST)
							disVar2 = (Node *)linitial(f->args);
					}
					else if (IsA(disVar2, RelabelType))
					{
						RelabelType *r = (RelabelType *) disVar2;

						if (r->relabelformat == COERCE_EXPLICIT_CAST ||
							r->relabelformat == COERCE_IMPLICIT_CAST)
							disVar2 = (Node *) r->arg;
					}

					if (equal(disExprs2, disVar2))
						return true;
					else
						return false;
				}
			}
		}
	}

	/*
	 * Allow checking DELETE and UPDATE statement distribution keys
	 * under function nodes.
	 */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE)
	{
		Expr *expr1 = (Expr *)get_var_from_arg((Node *)disExprs1);
		Expr *expr2 = (Expr *)get_var_from_arg((Node *)disExprs2);

		if (equal(expr1, expr2))
			return true;
	}

	return false;
}

/*
 * equal_distributions
 * 	Check that two distributions are equal.
 *
 * Distributions are considered equal if they are of the same type, on the
 * same set of nodes, and if the distribution expressions are known to be equal
 * (either the same expressions or members of the same equivalence class).
 */
bool
equal_distributions(PlannerInfo *root, Path *path)
{
	int i;
	Distribution *dst1 = root->distribution;
	Distribution *dst2 = path->distribution;

	/* fast path */
	if (dst1 == dst2)
		return true;

	if (dst1 == NULL || dst2 == NULL)
		return false;

	/* conditions easier to check go first */
	if (dst1->distributionType != dst2->distributionType)
		return false;

	if (!bms_equal(dst1->nodes, dst2->nodes))
		return false;

	if (dst1->nExprs != dst2->nExprs)
		return false;

	for (i = 0; i < dst1->nExprs; i++)
	{
		if (equal(dst1->disExprs[i], dst2->disExprs[i]))
			continue;

		if (!extend_equal_disExpr(root, path, dst1->disExprs[i], dst2->disExprs[i]))
			return false;
	}

	/* The restrictNodes field does not matter for distribution equality */
	return true;
}

/*
 * Get the location of DML result relation if it appears in either subpath
 */
ResultRelLocation
getResultRelLocation(int resultRel, Relids inner, Relids outer)
{
	ResultRelLocation location = RESULT_REL_NONE;

	if (bms_is_member(resultRel, inner))
	{
		location = RESULT_REL_INNER;
	}
	else if (bms_is_member(resultRel, outer))
	{
		location = RESULT_REL_OUTER;
	}

	return location;
}

/*
 * Check if the path distribution satisfy the result relation distribution.
 */
bool
SatisfyResultRelDist(PlannerInfo *root, Path *path)
{
	PlannerInfo    *top_root = root;
	bool			equal = false;

	/* Get top root */
	while(top_root->parent_root)
	{
		top_root = top_root->parent_root;
	}

	/*
	 * Check the UPDATE/DELETE command, make sure the path distribution equals the
	 * result relation distribution.
	 * We only invalidate the check if the result relation appears in one of
	 * the left/right subpath.
	 */
	if ((top_root->parse->commandType == CMD_UPDATE ||
		 top_root->parse->commandType == CMD_DELETE) &&
		path->parent->resultRelLoc != RESULT_REL_NONE)
	{
		equal = equal_distributions(top_root, path);

		if (!equal)
			return false;
	}

	return true;
}

/*
 * get_baserel_num_data_nodes
 *     get the number of data nodes from a baserel
 */
static int
get_baserel_num_data_nodes(Oid relid)
{
	RelationLocInfo *rel_loc_info = GetRelationLocInfo(relid);
	int num_nodes = 1;

	if (IS_PGXC_COORDINATOR && rel_loc_info)
	{
		if (rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
		{
			num_nodes = 1;
		}
		else
		{
			num_nodes = list_length(rel_loc_info->rl_nodeList);
		}
	}

	return num_nodes;
}

/*
 * get_rel_num_data_nodes
 *     get the number of data nodes from a RelOptInfo
 */
int
get_rel_num_data_nodes(PlannerInfo *root, RelOptInfo *rel)
{
	int num_data_nodes = NumDataNodes;
	RangeTblEntry *rte;
	Path *path;

	switch (rel->rtekind)
	{
		case RTE_RELATION:
			/* RELKIND_FOREIGN_TABLE or not */
			rte = root->simple_rte_array[rel->relid];
			num_data_nodes = get_baserel_num_data_nodes(rte->relid);
			break;
		case RTE_JOIN:
			path = rel->cheapest_total_path;
			num_data_nodes = path_count_datanodes(path);
			break;
		case RTE_SUBQUERY:
		case RTE_FUNCTION:
		case RTE_VALUES:
		case RTE_CTE:
		case RTE_TABLEFUNC:
			num_data_nodes = NumDataNodes;
			break;
		case RTE_NAMEDTUPLESTORE:
			num_data_nodes = 1;
			break;
		default:
			elog(ERROR, "Unexpected range table entry type.");
			break;
	}

	if (num_data_nodes == 0)
	{
		elog(DEBUG1, "[ng_get_dest_num_data_nodes] num of data nodes is 0");
		num_data_nodes = 1;
	}
	return num_data_nodes;
}


/*
 * The same type of length conversion does not change the result, so there is no need to redistribute.
 *
 * eg.
 *  create table test_1(f1 varchar(4),f2 varchar(32),f3 int);
 *  create table test_2(f1 varchar(8),f2 varchar(32),f3 int);
 *
 * Expected
 * postgres=# explain insert into test_2 select * from test_1;
 *                                            QUERY PLAN
 *  --------------------------------------------------------------------------------
 *   Remote Subquery Scan on all (dn001,dn002)  (cost=0.00..8.95 rows=395 width=50)
 *     ->    Insert on test_2  (cost=0.00..8.95 rows=395 width=50)
 *           ->  Seq Scan on test_1  (cost=0.00..8.95 rows=395 width=50)
 * (3 rows)
 *
 * No expected
 *  postgres=# explain insert into test_2 select * from test_1;
 *                                            QUERY PLAN
 * ------------------------------------------------------------------------------------------------
 *  Remote Subquery Scan on all (dn001,dn002)  (cost=100.00..131.66 rows=395 width=46)
 *    ->  Insert on test_2  (cost=100.00..131.66 rows=395 width=46)
 *          ->  Remote Subquery Scan on all (dn001,dn002)  (cost=100.00..131.66 rows=395 width=46)
 *                Distribute results by S: f1
 *                ->  Seq Scan on test_1  (cost=0.00..9.94 rows=395 width=46)
 * (5 rows)
 */
static bool
check_var_typemod_cast_equal(Node *dst, Node *src)
{
	Oid var_type;
	FuncExpr *func = NULL;
	Var *var = NULL;
	Node *tmp = NULL;

	if (src == NULL || dst == NULL)
		return false;

	/*
	 * case1, insert the short shard field into the long shard field.
	 * eg.insert into test_2 select * from test_1;
	 */
	if (IsA(dst, RelabelType) &&
		IsA(src, Var) &&
		((RelabelType*)dst)->relabelformat == COERCE_EXPLICIT_CAST)
	{
		RelabelType *tmp_dst = castNode(RelabelType, dst);

		if (IsA(tmp_dst->arg, Var) && equal(tmp_dst->arg, src))
			return true;
		else
			return false;
	}

	if (!IsA(dst, FuncExpr) || !IsA(src, Var))
		return false;

	/*
	 * case 2, insert the long shard field into the short shard field.
	 * eg.insert into test_1 select * from test_2;
	 */
	var = castNode(Var, src);
	func = castNode(FuncExpr, dst);
	var_type = var->vartype;

	/* Only cast conversion modes are handled */
	if (func->funcformat != COERCE_IMPLICIT_CAST ||
		func->funcresulttype != var_type)
		return false;

	/*
	 * The first parameter of cast conversion is the original variable.
	 * eg. varchar2(source, typmod, isExplicit)
	 */
	tmp = linitial((List *)func->args);

	/*
	 * Here we can confirm that the current is a different length
	 * conversion function of the same type
	 */
	if (IsA(tmp, Var) && equal(tmp, var))
		return true;

	return false;
}

/*
 * equal_distributions
 * 	Check that two distributions are equal.
 *
 * Distributions are considered equal if they are of the same type, on the
 * same set of nodes, and if the distribution expressions are known to be equal
 * (either the same expressions or members of the same equivalence class).
 */
bool
equal_distributions_3(PlannerInfo *root, Distribution *dst1, Distribution *dst2)
{
	int	i;

	/* fast path */
	if (dst1 == dst2)
		return true;

	if (dst1 == NULL || dst2 == NULL)
		return false;

	/* conditions easier to check go first */
	if (dst1->distributionType != dst2->distributionType)
		return false;

	if (dst1->nExprs != dst2->nExprs)
		return false;

	if (!bms_equal(dst1->nodes, dst2->nodes))
		return false;

	/*
	 * The same type of length conversion does not change the result, so there
	 * is no need to redistribute
	 */
	for (i = 0; i < dst1->nExprs; i++)
	{
		if (!check_var_typemod_cast_equal(dst1->disExprs[i], dst2->disExprs[i]))
			return false;
	}

	/*
	 * More thorough check, but allows some important cases, like if
	 * distribution column is not updated (implicit set distcol=distcol) or
	 * set distcol = CONST, ... WHERE distcol = CONST - pattern used by many
	 * applications.
	 */
	for (i = 0; i < dst1->nExprs; i++)
	{
		if (!exprs_known_equal(root, dst1->disExprs[i], dst2->disExprs[i]))
			return false;
	}

	return true;
}
