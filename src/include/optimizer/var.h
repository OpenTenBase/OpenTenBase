/*-------------------------------------------------------------------------
 *
 * var.h
 *	  prototypes for optimizer/util/var.c.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/var.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VAR_H
#define VAR_H

#include "nodes/relation.h"

/* Bits that can be OR'd into the flags argument of pull_var_clause() */
#define PVC_INCLUDE_AGGREGATES	0x0001	/* include Aggrefs in output list */
#define PVC_RECURSE_AGGREGATES	0x0002	/* recurse into Aggref arguments */
#define PVC_INCLUDE_WINDOWFUNCS 0x0004	/* include WindowFuncs in output list */
#define PVC_RECURSE_WINDOWFUNCS 0x0008	/* recurse into WindowFunc arguments */
#define PVC_INCLUDE_PLACEHOLDERS	0x0010	/* include PlaceHolderVars in
											 * output list */
#define PVC_RECURSE_PLACEHOLDERS	0x0020	/* recurse into PlaceHolderVar
											 * arguments */
#ifdef __OPENTENBASE__
#define PVC_INCLUDE_UPPERLEVELVAR	0x0040	/* include upper level var */
#define PVC_INCLUDE_CNEXPR     0x0080  /* include RownumExpr or CONNECT BY
                                        * special expr in output list */
#endif

#ifdef __OPENTENBASE__
#define RNC_RECURSE_AGGREF		0x0001	/* go into Aggref */
#define RNC_COPY_NON_LEAF_NODES	0x0002	/* copy non-leaf nodes */
#define RNC_REPLACE_FIRST_ONLY	0x0004	/* RNC_REPLACE_FIRST_ONLY */
#endif

extern Relids pull_varnos(Node *node);
extern Relids pull_varnos_of_level(Node *node, int levelsup);
extern void pull_varattnos(Node *node, Index varno, Bitmapset **varattnos);
extern List *pull_vars_of_level(Node *node, int levelsup);
extern bool contain_var_clause(Node *node);
extern bool contain_vars_of_level(Node *node, int levelsup);
extern int	locate_var_of_level(Node *node, int levelsup);
extern List *pull_var_clause(Node *node, int flags);
extern Node *flatten_join_alias_vars(PlannerInfo *root, Node *node);
#ifdef __OPENTENBASE__
extern bool contain_vars_upper_level(Node *node, int levelsup);
#endif

#ifdef __OPENTENBASE_C__
extern List *replace_var_clause(Node **node, List *vars, bool transform_grouping);
extern Node *replace_node_clause(Node* clause, Node* src_list, Node* dest_list,
					uint32 rncbehavior);
extern bool contain_vars_of_level_or_above(Node* node, int levelsup);
extern bool contain_booling_vars_upper_level(Node *node, int levelsup);
#endif

#ifdef __OPENTENBASE__
extern bool check_varno(Node *qual, int varno, int varlevelsup);
extern bool equal_var(Var *var1, Var *var2);
#endif

#endif							/* VAR_H */
