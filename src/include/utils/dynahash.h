/*-------------------------------------------------------------------------
 *
 * dynahash
 *	  POSTGRES dynahash.h file definitions
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dynahash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DYNAHASH_H
#define DYNAHASH_H
#include "storage/buf_internals.h"
#include "utils/hsearch.h"

extern int	my_log2(long num);

extern void* buf_hash_operate(HTAB* hashp, const BufferTag* keyPtr, uint32 hashvalue, bool* foundPtr, HASHACTION action);

#endif							/* DYNAHASH_H */
