/*-------------------------------------------------------------------------
 *
 * materialinfo.h
 *	  support functions to material path generation
 *
 *
 * * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/joininfo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATERIALINFO_H
#define MATERIALINFO_H

#include "nodes/relation.h"

extern StringInfo get_path_status(Path *path);
extern StringInfo get_attr_mask_status(AttrMask mask);
extern StringInfo get_relids_status(Relids relids);
extern StringInfo get_attr_masks_status(PlannerInfo *root, AttrMask *mask);
extern StringInfo get_joinrel_status(PlannerInfo *root, RelOptInfo *joinrel);


#endif							/* MATERIALINFO_H */
