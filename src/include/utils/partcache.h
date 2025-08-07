/*-------------------------------------------------------------------------
 *
 * partcache.h
 *
 * Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * src/include/utils/partcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARTCACHE_H
#define PARTCACHE_H

#include "access/attnum.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "partitioning/partdefs.h"
#include "utils/relcache.h"

/*
 * Information about the partition key of a relation
 */
typedef struct PartitionKeyData
{
	char		strategy;		/* partitioning strategy */
	int16		partnatts;		/* number of columns in the partition key */
	AttrNumber *partattrs;		/* attribute numbers of columns in the
								 * partition key */
	List	   *partexprs;		/* list of expressions in the partitioning
								 * key, or NIL */

	Oid		   *partopfamily;	/* OIDs of operator families */
	Oid		   *partopcintype;	/* OIDs of opclass declared input data types */
	FmgrInfo   *partsupfunc;	/* lookup info for support funcs */

	/* Partitioning collation per attribute */
	Oid		   *partcollation;

	/* Type information per attribute */
	Oid		   *parttypid;
	int32	   *parttypmod;
	int16	   *parttyplen;
	bool	   *parttypbyval;
	char	   *parttypalign;
	Oid		   *parttypcoll;
} PartitionKeyData;

extern PartitionKey RelationGetPartitionKey(Relation rel);
extern PartitionDesc RelationGetPartitionDesc(Relation rel);
extern List *RelationGetPartitionQual(Relation rel);
extern Expr *get_partition_qual_relid(Oid relid);
extern Datum get_relation_bound_datum(Oid relid);

#ifdef _PG_ORCL_
extern PartitionBoundSpec *get_relation_bound(Oid relid);
extern List *get_partitions_bound(List *relids, bool only_one_def, List **copied_relids);
extern List *get_overlap_partition_bound(List *bounds, PartitionKey key);
extern PartitionBoundSpec *merge_partition_bound(List *bounds, PartitionKey key);
extern Oid get_relid_from_partbound(Relation parent, List *bounds, LOCKMODE mode,
						bool ispartition);
extern List *split_partition_bound(Relation rel, Relation parentrel, Oid partrel,
                                                 PartitionBoundSpec *split_bound);
extern bool is_partition_descendant_of(Relation prel, Relation crel);
#endif


/*
 * PartitionKey inquiry functions
 */
static inline int
get_partition_strategy(PartitionKey key)
{
	return key->strategy;
}

static inline int
get_partition_natts(PartitionKey key)
{
	return key->partnatts;
}

static inline List *
get_partition_exprs(PartitionKey key)
{
	return key->partexprs;
}

/*
 * PartitionKey inquiry functions - one column
 */
static inline int16
get_partition_col_attnum(PartitionKey key, int col)
{
	return key->partattrs[col];
}

static inline Oid
get_partition_col_typid(PartitionKey key, int col)
{
	return key->parttypid[col];
}

static inline int32
get_partition_col_typmod(PartitionKey key, int col)
{
	return key->parttypmod[col];
}

#endif							/* PARTCACHE_H */
