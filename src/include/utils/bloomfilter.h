/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * src/include/utils/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <stdint.h>

#include "postgres.h"
#include "utils/dsa.h"

typedef uint32 BucketWord;
typedef pg_atomic_uint32 BucketParallelWord;

/* The BloomFilter is divided up into Buckets */
#define BUCKET_WORDS  			8
/* log2(number of bits in a BucketWord) */
#define	LOG_BUCKET_WORD_BITS	5
/* log2(number of bytes in a bucket) */
#define LOG_BUCKET_BYTE_SIZE	5

/*
 * A BloomFilter stores sets of items and offers a query operation indicating whether or
 * not that item is in the set.  BloomFilters use much less space than other compact data
 * structures, but they are less accurate: for a small percentage of elements, the query
 * operation incorrectly returns true even when the item is not in the set.
 *
 * When talking about Bloom filter size, rather than talking about 'size', which might be
 * ambiguous, we distinguish two different quantities:
 *
 * 1. Space: the amount of buffer pool memory used
 *
 * 2. NDV: the number of unique items that have been inserted
 *
 * BloomFilter is implemented using block Bloom filters from Putze et al.'s "Cache-,
 * Hash- and Space-Efficient Bloom Filters". The basic idea is to hash the item to a tiny
 * Bloom filter the size of a single cache line or smaller. This implementation sets 8
 * bits in each tiny Bloom filter. This provides a false positive rate near optimal for
 * between 5 and 15 bits per distinct value, which corresponds to false positive
 * probabilities between 0.1% (for 15 bits) and 10% (for 5 bits).
 */
typedef struct BlockBloomFilterData
{
	int			allocSize;			/* total alloc size, for remote filter transfer */
	int 		logNumBuckets;		/* log(base 2) of the number of buckets in the directory. */
	int32 		directoryMask;		/* (1 << log_num_buckets_) - 1 */
	uint64		ninsert;			/* number of inserted value */
	uint64		nlookups;			/* number of lookups */
	uint64		nmatches;			/* number of matches */
	BucketWord	directory[1];		/* directory */
}BlockBloomFilterData;

typedef struct SharedBloomFilterData
{
	int 					logNumBuckets;		/* log(base 2) of the number of buckets in the directory. */
	int32 					directoryMask;		/* (1 << log_num_buckets_) - 1 */
	pg_atomic_uint64		ninsert;			/* number of inserted value */
	pg_atomic_uint64		nlookups;			/* number of lookups */
	pg_atomic_uint64		nmatches;			/* number of matches */
	BucketParallelWord		directory[1];		/* directory */
} SharedBloomFilterData;

typedef BlockBloomFilterData *BlockBloomFilter;
typedef SharedBloomFilterData *SharedBloomFilter;


extern BlockBloomFilter BlockBloomFilterInit(double nrows, double fpp);
extern dsa_pointer SharedBloomFilterInit(double nrows, double fpp, dsa_area *area);

extern void BlockBloomFilterInsert(BlockBloomFilter filter, const uint32 hash);

extern bool BlockBloomFilterFind(BlockBloomFilter filter, const uint32 hash,
						uint64 *pLookups, uint64 *pMatches);

extern bool BlockBloomFilterIsEfficient(BlockBloomFilter filter);

extern void MergeParallelBloomFilter(BlockBloomFilter mergeFilter, BlockBloomFilter localFilter);

#endif // BLOOMFILTER_H
