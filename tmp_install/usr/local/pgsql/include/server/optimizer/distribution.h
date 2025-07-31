/*-------------------------------------------------------------------------
 *
 * distribution.h
 *	  Routines related to adjust distribution
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
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

extern bool equal_distributions(PlannerInfo *root, Distribution *dst1,
					Distribution *dst2);
extern ResultRelLocation getResultRelLocation(int resultRel, Relids inner,
					Relids outer);
extern bool SatisfyResultRelDist(PlannerInfo *root, Path *path);
#endif  /* DISTRIBUTION_H */
