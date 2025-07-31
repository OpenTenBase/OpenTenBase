/*-------------------------------------------------------------------------
 *
 * cluster.h
 *      header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
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


extern void cluster(ClusterStmt *stmt, bool isTopLevel);
extern void cluster_rel(Oid tableOid, Oid indexOid, bool recheck,
            bool verbose);
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
#ifdef _SHARDING_
extern AttrNumber get_newheap_diskey(Relation oldHeap, Relation newHeap);

extern AttrNumber get_newheap_secdiskey(Relation oldHeap, Relation newHeap);
#endif

#endif                            /* CLUSTER_H */
