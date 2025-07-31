/*-------------------------------------------------------------------------
 *
 * partition.h
 *		Header file for structures and utility functions related to
 *		partitioning
 *
 * Copyright (c) 2007-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/catalog/partition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTITION_H
#define PARTITION_H

#include "fmgr.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "parser/parse_node.h"
#include "utils/rel.h"

/* Seed for the extended hash function */
#define HASH_PARTITION_SEED UINT64CONST(0x7A5B22367996DCFD)

/*
 * PartitionBoundInfo encapsulates a set of partition bounds.  It is usually
 * associated with partitioned tables as part of its partition descriptor.
 *
 * The internal structure appears in partbounds.h.
 */
typedef struct PartitionBoundInfoData *PartitionBoundInfo;

/*
 * Information about partitions of a partitioned table.
 */
typedef struct PartitionDescData
{
	int			nparts;			/* Number of partitions */
	Oid		   *oids;			/* OIDs of partitions */
	PartitionBoundInfo boundinfo;	/* collection of partition bounds */
} PartitionDescData;

typedef struct PartitionDescData *PartitionDesc;

extern void RelationBuildPartitionDesc(Relation relation);
extern bool partition_bounds_equal(int partnatts, int16 *parttyplen,
					   bool *parttypbyval, PartitionBoundInfo b1,
					   PartitionBoundInfo b2);
extern PartitionBoundInfo partition_bounds_copy(PartitionBoundInfo src,
					  PartitionKey key);

extern void check_new_partition_bound(char *relname, Relation parent,
						  PartitionBoundSpec *spec);
extern Oid	get_partition_parent(Oid relid);
extern List *get_partition_ancestors(Oid relid);
extern List *get_qual_from_partbound(Relation rel, Relation parent,
						PartitionBoundSpec *spec);
extern List *map_partition_varattnos(List *expr, int fromrel_varno,
						Relation to_rel, Relation from_rel,
						bool *found_whole_row);
extern List *RelationGetPartitionQual(Relation rel);
extern Expr *get_partition_qual_relid(Oid relid);
extern bool has_partition_attrs(Relation rel, Bitmapset *attnums,
					bool *used_in_expr);

extern Oid	get_default_oid_from_partdesc(PartitionDesc partdesc);
extern Oid	get_default_partition_oid(Oid parentId);
extern void update_default_partition_oid(Oid parentId, Oid defaultPartId);
extern void check_default_allows_bound(Relation parent, Relation defaultRel,
						   PartitionBoundSpec *new_spec);
extern List *get_proposed_default_constraint(List *new_part_constaints);

extern int get_partition_for_tuple(Relation relation, Datum *values,
							bool *isnull);

#endif							/* PARTITION_H */
