/*
 * result_cache.h
 *
 * OpenTenBase-C result cache managment
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/result_cache.h
 */
#ifndef RESULT_CACHE_H
#define RESULT_CACHE_H

#include "utils/relcache.h"
#include "utils/builtins.h"
#include "storage/lwlock.h"

extern bool enable_result_cache;
extern int  max_size_per_cached_result;
extern int	result_cache_partition_number;
extern int	result_cache_number_per_partition;


#define MAX_QUERY_LENGTH				1024								/* Encoded query length */
#define	MAX_RELATION_PER_QUERY			20									/* Max relation related to a result cache */
#define	MAX_RESULT_CACHE_LENGTH			max_size_per_cached_result * 1024	/* Max size of a cached result */
#define	MAX_RESULT_CACHE_PER_PARTITION	result_cache_number_per_partition	/* Max result cache entry per partition */
#define CACHE_PARTITION_NUMBER 			result_cache_partition_number		/* Partition number */
#define	INIT_RESULT_CACHE_SIZE 	(CACHE_PARTITION_NUMBER*MAX_RESULT_CACHE_PER_PARTITION)	/* Max result cache entry number */

typedef struct
{
	uint32 					queryid;							/* Hashcode of encoded query */
	GlobalTimestamp			gts;								/* Global Timestamp of this result cache entry generation */
	int16					prev;								/* Index of previous result cache entry */
	int16					next;								/* Index of next result cache entry */
	Oid						relids[MAX_RELATION_PER_QUERY];		/* Relation id related or InvalidOid */
	char					*result;							/* Cached Result */
}ResultCacheEntryData;

typedef ResultCacheEntryData * ResultCacheEntry;


typedef	struct
{
	int16					partition_head;						/* Lru head */
	int16					partition_tail;						/* Lru tail */
	int16					partition_size;						/* Partition size */
	LWLock					partition_lock;						/* Partition lock */
	ResultCacheEntry		*partition_queue;					/* Array of result cache entry */
}ResultCachePartitionData;

typedef ResultCachePartitionData * ResultCachePartition;

extern Size ResultCacheShmemSize(void);
extern void InitResultCache(void);
extern void RemoveResultCacheForRelation(Relation rel);
extern uint32 QueryTreeGetEncodedQueryId(Query *query);
extern bool CheckQuerySatisfyCache(List *querytree);
extern Oid *QueryTreeGetRelids(Query *query);
extern char *SearchQueryResultCache(uint32 queryid);
extern void InsertQueryResultCache(uint32 queryid, char *result, int size, Oid *relids);
#endif
