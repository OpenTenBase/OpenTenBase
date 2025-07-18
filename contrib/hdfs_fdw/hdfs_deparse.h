#ifndef HDFS_DEPARSE_H
#define HDFS_DEPARSE_H

#include "hdfs_fdw.h"
#include "optimizer/planner.h"

/* Deparse functions */
extern char *hdfs_deparse_select_stmt(StringInfo buf, PlannerInfo *root, RelOptInfo *baserel,
                                     List *remote_conds, List **retrieved_attrs);

#endif /* HDFS_DEPARSE_H */
