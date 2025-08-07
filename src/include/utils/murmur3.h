/*-------------------------------------------------------------------------
 *
 * murmurhash3.h
 *
 * MurmurHash3 was written by Austin Appleby, and is placed in the
 * public domain. The author hereby disclaims copyright to this source
 * code.
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase-C Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * src/include/utils/murmurhash3.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MURMURHASH3_H
#define MURMURHASH3_H

#include <stdint.h>

#include "postgres.h"

void MurmurHash3_x86_32(const void *key, int len, uint32 seed, void *out);

void MurmurHash3_x86_128(const void *key, int len, uint32 seed, void *out);

void MurmurHash3_x64_128(const void *key, int len, uint32 seed, void *out);

#endif // MURMURHASH3_H
