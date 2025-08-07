/*-------------------------------------------------------------------------
 *
 * memctl.h
 *	  Routines to calculate and adjust memory amount used by query.
 *
 * Copyright (c) 2022-Present OpenTenBase development team, Tencent
 *
 *
 * IDENTIFICATION
 *	  src/include/optimizer/memctl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MEMCTL_H
#define MEMCTL_H

#include "commands/explain.h"
#include "nodes/plannodes.h"

/* min threshold for queyr_mem/max_query_mem (32MB) */
#define SIMPLE_QUERY_THRESHOLD (32 * 1024.0)
/* minvalue of work_mem (64kB) */
#define MIN_WORK_MEM 64

extern int query_mem;
extern int max_query_mem;

extern void set_plan_querymem(PlannedStmt *stmt, int curr_query_mem);
extern void ExplainQueryMem(ExplainState *es, QueryDesc *queryDesc);

#endif  /* MEMCTL_H */
