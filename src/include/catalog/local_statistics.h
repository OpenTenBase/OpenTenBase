/*-------------------------------------------------------------------------
 *
 * local_statistics.h
 *	  prototypes for functions in backend/catalog/local_statistics.c
 *
 * src/include/catalog/local_statistics.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCAL_STATISTICS_H
#define LOCAL_STATISTICS_H

#include "access/htup.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"
#include "utils/relcache.h"
#include "nodes/execnodes.h"

#define ENABLE_AUTO_ANALYZE_IN_XACT() \
	(xact_autoanalyze && IsTransactionBlock())

extern HTAB *local_statistic_hash;
typedef struct
{
	Oid			relid;
	int32		relpages;
	int32		relallvisible;
	float4		livetuples;
	float4		deadtuples;
	int			natts;
	int			*attnum;
	HeapTuple       *att_stat_tups; /* stainherit is false */
	HeapTuple       *att_stat_tups_inh; /* stainherit is true */
} local_statistic_hash_entry;

extern void LocalStatisticRemoveByRelid(Oid relid);
extern void LocalStatisticRemoveAttrStats(Oid reloid, int attnum);
extern void LocalStatisticRemoveAll(void);
extern void LocalStatisticSetRelStats(Relation relation, BlockNumber relpages, BlockNumber relallvisible, double livetuples, double deadtuples, bool enter);
extern bool LocalStatisticGetRelStats(Oid relid, BlockNumber *relpages, BlockNumber *relallvisible, double *livetuples, double *deadtuples);
extern void LocalStatisticUpdateAttrStats(Oid relid, int attnum, int natts, TupleDesc tupleDescriptor, bool inh, Datum *values, bool *isnull);
extern HeapTuple LocalStatisticGetAttrStats(Oid relid, int attnum, bool inh);
extern void LocalStatisticReleaseCache(HeapTuple tup);

#endif							/* STORAGE_H */
