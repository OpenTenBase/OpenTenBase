/*-------------------------------------------------------------------------
 *
 * nodeFuncs.h
 *		Various general-purpose manipulations of Node trees
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/nodeFuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEFUNCS_H
#define NODEFUNCS_H

#include "nodes/parsenodes.h"
#include "nodes/relation.h"


/* flags bits for query_tree_walker and query_tree_mutator */
#define QTW_IGNORE_RT_SUBQUERIES	0x01	/* subqueries in rtable */
#define QTW_IGNORE_CTE_SUBQUERIES	0x02	/* subqueries in cteList */
#define QTW_IGNORE_RC_SUBQUERIES	0x03	/* both of above */
#define QTW_IGNORE_JOINALIASES		0x04	/* JOIN alias var lists */
#define QTW_IGNORE_RANGE_TABLE		0x08	/* skip rangetable entirely */
#define QTW_EXAMINE_RTES_BEFORE		0x10	/* examine RTE nodes before their
											 * contents */
#define QTW_EXAMINE_RTES_AFTER		0x20	/* examine RTE nodes after their
											 * contents */
#define QTW_DONT_COPY_QUERY			0x40	/* do not copy top Query */
#ifdef _MLS_
#define QTW_IGNORE_TARGET_LIST      0x0100	/* skip target list */
#define QTW_IGNORE_RETURNING_LIST   0x0200	/* skip returning list */
#endif
#define QTW_IGNORE_DUMMY_RTE		0x0800	/* skip multi-insert nodes */

#define IS_PLAN_NODE(node) (nodeTag(node) > T_Plan && nodeTag(node) <= T_ConnectBy)

/* callback function for check_functions_in_node */
typedef bool (*check_function_callback) (Oid func_id, void *context
#ifdef _PG_ORCL_
										, Node *node
#endif
										);


extern Oid	exprType(const Node *expr);
extern int32 exprTypmod(const Node *expr);
extern bool exprIsLengthCoercion(const Node *expr, int32 *coercedTypmod);
extern Node *relabel_to_typmod(Node *expr, int32 typmod);
extern Node *strip_implicit_coercions(Node *node);
extern bool expression_returns_set(Node *clause);

extern Oid	exprCollation(const Node *expr);
extern Oid	exprInputCollation(const Node *expr);
extern void exprSetCollation(Node *expr, Oid collation);
extern void exprSetInputCollation(Node *expr, Oid inputcollation);

extern int	exprLocation(const Node *expr);

extern void find_nextval_seqoid_walker(Node *node, Oid *seqoid);
extern void fix_opfuncids(Node *node);
extern void set_opfuncid(OpExpr *opexpr);
extern void set_sa_opfuncid(ScalarArrayOpExpr *opexpr);

extern bool check_functions_in_node(Node *node, check_function_callback checker,
						void *context);

extern bool expression_tree_walker(Node *node, bool (*walker) (),
								   void *context);
extern Node *expression_tree_mutator(Node *node, Node *(*mutator) (),
									 void *context);
extern Node *expression_tree_mutator_internal(Node *node, Node *(*mutator) (),
											void *context, bool isCopy);

extern bool query_tree_walker(Query *query, bool (*walker) (),
							  void *context, int flags);
extern Query *query_tree_mutator(Query *query, Node *(*mutator) (),
								 void *context, int flags);

extern bool range_table_walker(List *rtable, bool (*walker) (),
							   void *context, int flags);
extern List *range_table_mutator(List *rtable, Node *(*mutator) (),
								 void *context, int flags);

extern bool range_table_entry_walker(RangeTblEntry *rte, bool (*walker) (),
									 void *context, int flags);

extern bool query_or_expression_tree_walker(Node *node, bool (*walker) (),
											void *context, int flags);
extern Node *query_or_expression_tree_mutator(Node *node, Node *(*mutator) (),
											  void *context, int flags);

extern bool raw_expression_tree_walker(Node *node, bool (*walker) (),
									   void *context);

struct PlanState;
extern bool planstate_tree_walker(struct PlanState *planstate, bool (*walker) (),
								  void *context);


extern bool planstate_tree_walker_get_rels(struct PlanState *planstate, bool (*walker) (),
								  void *context);

struct Plan;
extern bool plan_tree_walker(struct Plan *plan, bool (*walker) (),
							 void *context);
extern bool plan_or_expression_tree_walker(Node *node, bool (*walker) (),
										   void *context);

extern bool path_tree_walker(Path *path, bool (*walker)(), void *context);
extern Plan* ReplaceSqlValueFuncInPlan(Plan * plan, bool need_copy);
#endif							/* NODEFUNCS_H */
