/*-------------------------------------------------------------------------
 *
 * var.h
 *      prototypes for optimizer/util/var.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/optimizer/var.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VAR_H
#define VAR_H

#include "nodes/relation.h"

/* Bits that can be OR'd into the flags argument of pull_var_clause() */
#define PVC_INCLUDE_AGGREGATES    0x0001    /* include Aggrefs in output list */
#define PVC_RECURSE_AGGREGATES    0x0002    /* recurse into Aggref arguments */
#define PVC_INCLUDE_WINDOWFUNCS 0x0004    /* include WindowFuncs in output list */
#define PVC_RECURSE_WINDOWFUNCS 0x0008    /* recurse into WindowFunc arguments */
#define PVC_INCLUDE_PLACEHOLDERS    0x0010    /* include PlaceHolderVars in
                                             * output list */
#define PVC_RECURSE_PLACEHOLDERS    0x0020    /* recurse into PlaceHolderVar
                                             * arguments */


extern Relids pull_varnos(Node *node);
extern Relids pull_varnos_of_level(Node *node, int levelsup);
extern void pull_varattnos(Node *node, Index varno, Bitmapset **varattnos);
extern List *pull_vars_of_level(Node *node, int levelsup);
extern bool contain_var_clause(Node *node);
extern bool contain_vars_of_level(Node *node, int levelsup);
extern int    locate_var_of_level(Node *node, int levelsup);
extern List *pull_var_clause(Node *node, int flags);
extern Node *flatten_join_alias_vars(PlannerInfo *root, Node *node);
#ifdef __OPENTENBASE__
extern bool contain_vars_upper_level(Node *node, int levelsup);
#endif

#endif                            /* VAR_H */
