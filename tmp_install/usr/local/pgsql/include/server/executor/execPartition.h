/*--------------------------------------------------------------------
 * execPartition.h
 *		POSTGRES partitioning executor interface
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/executor/execPartition.h
 *--------------------------------------------------------------------
 */

#ifndef EXECPARTITION_H
#define EXECPARTITION_H

#include "catalog/partition.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

/* See execPartition.c for the definition. */
typedef struct PartitionDispatchData *PartitionDispatch;

/*-----------------------
 * PartitionTupleRouting - Encapsulates all information required to execute
 * tuple-routing between partitions.
 *
 * partition_dispatch_info		Array of PartitionDispatch objects with one
 *								entry for every partitioned table in the
 *								partition tree.
 * num_dispatch					number of partitioned tables in the partition
 *								tree (= length of partition_dispatch_info[])
 * partition_oids				Array of leaf partitions OIDs with one entry
 *								for every leaf partition in the partition tree,
 *								initialized in full by
 *								ExecSetupPartitionTupleRouting.
 * partitions					Array of ResultRelInfo* objects with one entry
 *								for every leaf partition in the partition tree,
 *								initialized lazily by ExecInitPartitionInfo.
 * num_partitions				Number of leaf partitions in the partition tree
 *								(= 'partitions_oid'/'partitions' array length)
 * parent_child_tupconv_maps	Array of TupleConversionMap objects with one
 *								entry for every leaf partition (required to
 *								convert tuple from the root table's rowtype to
 *								a leaf partition's rowtype after tuple routing
 *								is done)
 * child_parent_tupconv_maps	Array of TupleConversionMap objects with one
 *								entry for every leaf partition (required to
 *								convert an updated tuple from the leaf
 *								partition's rowtype to the root table's rowtype
 *								so that tuple routing can be done)
 * child_parent_map_not_required  Array of bool. True value means that a map is
 *								determined to be not required for the given
 *								partition. False means either we haven't yet
 *								checked if a map is required, or it was
 *								determined to be required.
 * subplan_partition_offsets	Integer array ordered by UPDATE subplans. Each
 *								element of this array has the index into the
 *								corresponding partition in partitions array.
 * num_subplan_partition_offsets  Length of 'subplan_partition_offsets' array
 * partition_tuple_slot			TupleTableSlot to be used to manipulate any
 *								given leaf partition's rowtype after that
 *								partition is chosen for insertion by
 *								tuple-routing.
 *-----------------------
 */
typedef struct PartitionTupleRouting
{
	PartitionDispatch *partition_dispatch_info;
	int			num_dispatch;
	Oid		   *partition_oids;
	ResultRelInfo **partitions;
	int			num_partitions;
	TupleConversionMap **parent_child_tupconv_maps;
	TupleConversionMap **child_parent_tupconv_maps;
	bool	   *child_parent_map_not_required;
	int		   *subplan_partition_offsets;
	int			num_subplan_partition_offsets;
	TupleTableSlot *partition_tuple_slot;
	TupleTableSlot *root_tuple_slot;
} PartitionTupleRouting;

extern PartitionTupleRouting *ExecSetupPartitionTupleRouting(ModifyTableState *mtstate,
							   Relation rel);
extern int ExecFindPartition(ResultRelInfo *resultRelInfo,
				  PartitionDispatch *pd,
				  TupleTableSlot *slot,
				  EState *estate);
extern ResultRelInfo *ExecInitPartitionInfo(ModifyTableState *mtstate,
					ResultRelInfo *resultRelInfo,
					PartitionTupleRouting *proute,
					EState *estate, int partidx);
extern void ExecSetupChildParentMapForLeaf(PartitionTupleRouting *proute);
extern TupleConversionMap *TupConvMapForLeaf(PartitionTupleRouting *proute,
				  ResultRelInfo *rootRelInfo, int leaf_index);
extern HeapTuple ConvertPartitionTupleSlot(Relation partrel, TupleConversionMap *map,
						  HeapTuple tuple,
						  TupleTableSlot *new_slot,
						  TupleTableSlot **p_my_slot);
extern void ExecCleanupTupleRouting(PartitionTupleRouting *proute);

#endif							/* EXECPARTITION_H */
