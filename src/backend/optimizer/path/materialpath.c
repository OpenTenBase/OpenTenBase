/*-------------------------------------------------------------------------
 *
 * materialpath.c
 *	  Routines to build path with different materialization strategy.
 *
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/materialpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "nodes/nodes.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/materialinfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

/*
 * compare_material_status
 *	  Compare the materialize status to see if either can be said to
 *	  dominate the other.
 */
MaterialComparison
compare_material_status(RelOptInfo *parent_rel, Path *path1, Path *path2)
{
	MaterialComparison 		cmp;
	bool	better1 = true;
	bool	better2 = true;
	bool	equal = true;
	int 	relid = -1;

	AttrMask *mask1 = path1->attr_masks;
	AttrMask *mask2 = path2->attr_masks;

	if (!mask1 && !mask2)
		elog(ERROR, "Both paths are not late materialized path!");

	/*
	 * compare each relid mask
	 */
	while ((relid = bms_next_member(parent_rel->relids, relid)) >= 0)
	{
		BMS_Comparison bmsCmp;

		if (!better1 && !better2 && !equal)
			break;

		if (!mask1[relid] && mask2[relid])
		{
			better2 = false;
			equal = false;
			continue;
		}

		if (mask1[relid] && !mask2[relid])
		{
			better1 = false;
			equal = false;
			continue;
		}

		if (!mask1[relid] && !mask2[relid])
		{
			continue;
		}

		/* Compare the bitmap */
		bmsCmp = bms_subset_compare(mask1[relid], mask2[relid]);
		switch(bmsCmp)
		{
			case BMS_EQUAL:
				break;
			case BMS_SUBSET1:
				better1 = false;
				equal = false;
				break;
			case BMS_SUBSET2:
				better2 = false;
				equal = false;
				break;
			case BMS_DIFFERENT:
				equal = false;
				break;
		}
	}

	if (equal)
		cmp = MATERIAL_EQUAL;
	else if (better1)
		cmp = MATERIAL_BETTER1;
	else if (better2)
		cmp = MATERIAL_BETTER2;
	else
		cmp = MATERIAL_DIFFERENT;

	return cmp;
}

/*
 * get_path_lm_width
 *
 * 		Get the real width of materialized column based on
 * 		the attr_masks
 */
int
get_path_lm_width(PlannerInfo *root,
				  RelOptInfo *rel,
				  AttrMask mask)
{
	int 		tuple_width = 0;
	ListCell   *lc;

	/*
	 * loop through all target expression to get attr needed
	 */
	foreach(lc, rel->reltarget->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);

		/* We only set var to un-material status */
		if (IsA(node, Var))
		{
			Var		   *var = (Var *) node;
			int32		item_width;

			/* We should not see any upper-level Vars here */
			Assert(var->varlevelsup == 0);

			/* Skip unmaterial columns */
			if (rel->reloptkind == RELOPT_BASEREL)
			{
				/* For baserel, it should be passed in a single mask */
				if (mask != NULL && !bms_is_member(var->varattno, mask))
					continue;
			}
			else
				elog(ERROR, "get_path_lm_width only support BaseRel & JoinRel type.");

			/* Try to get data from RelOptInfo cache */
			if (var->varno < root->simple_rel_array_size)
			{
				RelOptInfo *rel = root->simple_rel_array[var->varno];

				if (rel != NULL &&
					var->varattno >= rel->min_attr &&
					var->varattno <= rel->max_attr)
				{
					int			ndx = var->varattno - rel->min_attr;

					if (rel->attr_widths[ndx] > 0)
					{
						tuple_width += rel->attr_widths[ndx];
						continue;
					}
				}
			}

			/*
			 * No cached data available, so estimate using just the type info.
			 */
			item_width = get_typavgwidth(var->vartype, var->vartypmod);
			Assert(item_width > 0);
			tuple_width += item_width;
		}
	}

	return tuple_width;
}

/*
 * Create list of paths that satisfy hashjoin restrictions.
 *
 * 1. all paths should satisfy the hashjoin restrictions
 * 2. add LM+GtidScan path if not satisfied
 * 3. TODO(LM): Skip PATH_PARAM_BY_REL
 */
List *
generate_joinrestrict_satisfied_lm_paths(PlannerInfo *root, List *pathlist,
										 JoinPathExtraData *extra)
{
	ListCell	*lc;
	List		*result = NULL;

	foreach(lc, pathlist)
	{
		Path *path = (Path *) lfirst(lc);

		if (path->attr_masks == NULL)
		{
			elog(ERROR, "This is not a late materialized path!");
		}
	}

	return result;
}

/*
 * create_hashjoin_attr_masks
 *
 * 		Build hashjoin attr masks based on input paths. Also check if the path is
 * 		already fully materialized. */
AttrMask *
create_hashjoin_attr_masks(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   Path *inner_path,
						   Path *outer_path)
{
	AttrMask 	*joinrel_mask;
	List 		*vars;
	int 	  	relid;
	ListCell	*lc;
	bool		hasUnmaterial = false;

	if (!inner_path->attr_masks && !outer_path->attr_masks)
	{
		return NULL;
	}

	joinrel_mask = (AttrMask *)
			palloc0(root->simple_rel_array_size * sizeof(AttrMask));

	/* Reuse all inner path attr_masks */
	relid = -1;
	while((relid = bms_next_member(inner_path->parent->relids, relid)) >= 0)
	{
		if (inner_path->attr_masks && inner_path->attr_masks[relid])
			joinrel_mask[relid] = inner_path->attr_masks[relid];
		else
			joinrel_mask[relid] = NULL;
	}

	/* Reuse all outer path attr_masks */
	relid = -1;
	while((relid = bms_next_member(outer_path->parent->relids, relid)) >= 0)
	{
		if (outer_path->attr_masks && outer_path->attr_masks[relid])
			joinrel_mask[relid] = outer_path->attr_masks[relid];
		else
			joinrel_mask[relid] = NULL;
	}

	/* Check if all vars have been materialized */
	vars = pull_var_clause((Node *)joinrel->reltarget->exprs,
						   PVC_RECURSE_AGGREGATES |
						   PVC_RECURSE_WINDOWFUNCS |
						   PVC_INCLUDE_PLACEHOLDERS);

	foreach(lc, vars)
	{
		Node *node = (Node *) lfirst(lc);

		if (IsA(node, Var))
		{
			Var		   *var = (Var *) node;
			Index 		varno = var->varno;
			AttrNumber	attno = var->varattno;

			if (joinrel_mask[varno] && !bms_is_member(attno, joinrel_mask[varno]))
			{
				hasUnmaterial = true;
				break;
			}
		}
	}

	/* If all reltarget expr have been materialiezed, then set to NULL */
	if (!hasUnmaterial)
	{
		pfree(joinrel_mask);
		return NULL;
	}

	return joinrel_mask;
}
