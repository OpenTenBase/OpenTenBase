/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/cluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"


typedef enum {
	OP_CLUSTER,
    OP_VACUUM    
} ClusterOp;

extern void cluster(ClusterStmt *stmt, bool isTopLevel);
extern void cluster_rel(Oid tableOid, Oid indexOid, bool recheck,
			bool verbose, ClusterOp op);
extern void check_index_is_clusterable(Relation OldHeap, Oid indexOid,
						   bool recheck, LOCKMODE lockmode);
extern void mark_index_clustered(Relation rel, Oid indexOid, bool is_internal);

extern Oid make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, char relpersistence,
			  LOCKMODE lockmode);
extern void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
				 bool is_system_catalog,
				 bool swap_toast_by_content,
				 bool check_constraints,
				 bool is_internal,
				 TransactionId frozenXid,
				 MultiXactId minMulti,
				 char newrelpersistence);
extern void rebuild_relation(Relation OldHeap, Oid indexOid, bool verbose);
#ifdef _SHARDING_
extern AttrNumber *get_newheap_diskeys(Relation oldHeap, Relation newHeap, int *ndiscols);
#endif
extern void swap_relation_names(Oid r1, Oid r2);

#endif							/* CLUSTER_H */
