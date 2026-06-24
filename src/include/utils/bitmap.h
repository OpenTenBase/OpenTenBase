/*-------------------------------------------------------------------------
 *
 * bitmap.h
 *	  bitmap typedefs, shortcuts, and related macros.
 *
 * 
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/bitmap.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BITMAP_H
#define BITMAP_H

#define bitmap_size(x) (((x) + 7) / 8)

static inline bool bitmap_query(char* bitmap, int which)
{
    return (bitmap[which / 8] & (1 << (which % 8)));
}

static inline void bitmap_set(char* bitmap, int which)
{
    bitmap[which / 8] |= (1 << (which % 8));
}

static inline void bitmap_clear(char* bitmap, int which)
{
    bitmap[which / 8] &= ~(1 << (which % 8));
}

static inline void bitmap_set_atomic(char* bitmap, int which)
{
	int byteIndex = which / 8;
	int offset = which % 8;
	unsigned char mask = 1 << offset;

	__sync_fetch_and_or(&bitmap[byteIndex], mask);
}

static inline void bitmap_clear_atomic(char* bitmap, int which)
{
	int byteIndex = which / 8;
	int offset = which % 8;
	unsigned char mask = ~(1 << offset);

	__sync_fetch_and_and(&bitmap[byteIndex], mask);
}

#endif
