/*-------------------------------------------------------------------------
 *
 * materialinfo.c
 *	  support functions to material path generation
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

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "optimizer/materialinfo.h"

/*
 * get_path_status
 *
 * 		return the path material status in string format
 */
StringInfo
get_path_status(Path *path)
{
	ListCell 		*lc;
	StringInfo 		info = makeStringInfo();

	if (path->parallel_aware)
		appendStringInfo(info, "[PARALLEL %d] ", path->parallel_workers);

	foreach(lc, path->pathtarget->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);
		if (IsA(node, Var))
		{
			bool isMaterialed = true;
			int varno = ((Var*)node)->varno;
			int varattno = ((Var*)node)->varattno;

			if (path->attr_masks &&
				path->attr_masks[varno] &&
				!bms_is_member(varattno, path->attr_masks[varno]))
			{
				isMaterialed = false;
			}

			appendStringInfo(info, "Var(varno=%d, varoattno=%d, isMaterialed=%d) ",
					varno, varattno, isMaterialed);
		}
	}

	return info;
}

/*
 * get_attr_masks_status
 *
 * 		return the material masks(array of baserel masks) in string format
 */
StringInfo
get_attr_masks_status(PlannerInfo *root, AttrMask *mask)
{
	StringInfo 		info = makeStringInfo();
	int				attrno;
	int				i;

	appendStringInfo(info, "Masks[");

	if (mask)
	{
		for(i = 1; i < root->simple_rel_array_size; i++)
		{
			if (mask[i])
			{
				attrno = -1;
				appendStringInfo(info, "relid_%d:{", root->simple_rte_array[i]->relid);

				while((attrno = bms_next_member(mask[i], attrno)) >= 0)
				{
					appendStringInfo(info, "%d,", attrno);
				}

				appendStringInfo(info, "}");
			}
		}
	}
	else
	{
		appendStringInfo(info, "NULL");
	}

	appendStringInfo(info, "]");

	return info;
}

/*
 * get_attr_mask_status
 *
 * 		return the material mask(single mask) in string format
 */
StringInfo
get_attr_mask_status(AttrMask mask)
{
	StringInfo 		info = makeStringInfo();
	int				attrno = -1;

	appendStringInfo(info, "AttrMask(");

	while((attrno = bms_next_member(mask, attrno)) >= 0)
	{
		appendStringInfo(info, "%d,", attrno);
	}
	appendStringInfo(info, ")");

	return info;
}

/*
 * get_attr_mask_status
 *
 * 		return the material mask(single mask) in string format
 */
StringInfo
get_relids_status(Relids relids)
{
	StringInfo 		info = makeStringInfo();
	int				attrno = -1;

	appendStringInfo(info, "Relids(");

	while((attrno = bms_next_member(relids, attrno)) >= 0)
	{
		appendStringInfo(info, "%d,", attrno);
	}
	appendStringInfo(info, ")");

	return info;
}

/*
 * get_joinrel_status
 *
 * 		return the material status of joinrel in string format
 */
StringInfo
get_joinrel_status(PlannerInfo *root, RelOptInfo *joinrel)
{
	StringInfo 		info = makeStringInfo();
	int				relid = -1;

	appendStringInfo(info, "JoinRel[");

	while((relid = bms_next_member(joinrel->relids, relid)) >= 0)
	{
		appendStringInfo(info, "%d,", root->simple_rte_array[relid]->relid);
	}

	appendStringInfo(info, "]");

	return info;
}
