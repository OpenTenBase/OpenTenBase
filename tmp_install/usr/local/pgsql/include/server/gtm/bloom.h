/*-------------------------------------------------------------------------
 *
 * bloom.h
 *
 *
 *	  a bloom filter, using murmurhash
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * IDENTIFICATION
 *	  src/include/gtm/bloom.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BLOOM_H
#define _BLOOM_H

#include "gtm/gtm_lock.h"

typedef unsigned int (*hashfunc_t)(const void *, int);

typedef struct
{
    int            bitmap_size;   /* bitmap size of bloom filter */
    unsigned char* bitmap;        /* bloom filter bitmap */
    int            nfuncs;        /* hash functions num */
    uint32*        seeds;         /* hash functions seeds */
} BLOOM;

BLOOM *BloomCreate(int bitmap_size, int nfuncs, ...);
int BloomDestroy(BLOOM *bloom);
void BloomReset(BLOOM *bloom);
void BloomAdd(BLOOM *bloom, const char *s, int len);
bool BloomCheck(BLOOM *bloom, const char *s, int len);
bool BloomCheckAndAdd(BLOOM *bloom, const char *s, int len);
uint32_t MurmurHash2(const void * key, int len, uint32_t seed);

#endif
