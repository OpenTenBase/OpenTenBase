/*-------------------------------------------------------------------------
 *
 * distribution.h
 *	  Routines related to adjust distribution
 *
 * Copyright (c) 2020-Present OpenTenBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/include/optimizer/distribution.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DISTRIBUTION_H
#define DISTRIBUTION_H

#include "postgres.h"

#include "nodes/relation.h"

/* TODO(OpenTenBase): Move all plan/path distribution routines to this file */

#define IsValidDistribution(distribution) \
    ((distribution) != NULL && (distribution)->nExprs > 0 && \
		(distribution)->disExprs != NULL && (distribution)->disExprs[0] != NULL)

#define IsSingleColDistribution(distribution) \
    (IsValidDistribution(distribution) && (distribution)->nExprs == 1)

#define IsMultiColsDistribution(distribution) \
    (IsValidDistribution(distribution) && \
        (distribution)->nExprs > 1 && (distribution)->disExprs[1])

extern bool equal_distributions(PlannerInfo *root, Path *path);
extern bool equal_distributions_3(PlannerInfo *root, Distribution *dst1,
								  Distribution *dst2);
extern bool extend_equal_disExpr(PlannerInfo *root, Path *path, 
                                Node *disExprs1, Node *disExprs2);
extern ResultRelLocation getResultRelLocation(int resultRel, Relids inner,
					Relids outer);
extern bool SatisfyResultRelDist(PlannerInfo *root, Path *path);
extern int get_rel_num_data_nodes(PlannerInfo *root, RelOptInfo *rel);
#endif  /* DISTRIBUTION_H */
