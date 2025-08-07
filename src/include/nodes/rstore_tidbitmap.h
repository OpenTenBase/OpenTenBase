/*-------------------------------------------------------------------------
 *
 * rstore_tidbitmap.h
 *	  PostgreSQL tuple-id (TID) bitmap package
 *
 * This module provides bitmap data structures that are spiritually
 * similar to Bitmapsets, but are specially adapted to store sets of
 * tuple identifiers (TIDs), or ItemPointers.  In particular, the division
 * of an ItemPointer into BlockNumber and OffsetNumber is catered for.
 * Also, since we wish to be able to store very large tuple sets in
 * memory with this data structure, we support "lossy" storage, in which
 * we no longer remember individual tuple offsets on a page but only the
 * fact that a particular page needs to be visited.
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development Group
 *
 * src/include/nodes/rstore_tidbitmap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RSTORE_TIDBITMAP_H
#define RSTORE_TIDBITMAP_H

#include "storage/itemptr.h"
#include "utils/dsa.h"
#include "nodes/tidbitmap.h"

extern TIDBitmap *r_tbm_create(long maxbytes, dsa_area *dsa, int flags);
extern void r_tbm_free(TIDBitmap *tbm);
extern void r_tbm_free_shared_area(dsa_area *dsa, dsa_pointer dp);

extern void r_tbm_add_tuples(TIDBitmap *tbm,
			   const ItemPointer tids, int ntids,
			   bool recheck);
extern void r_tbm_add_page(TIDBitmap *tbm, BlockNumber pageno);

extern void r_tbm_union(TIDBitmap *a, const TIDBitmap *b);
extern void r_tbm_intersect(TIDBitmap *a, const TIDBitmap *b);

extern bool r_tbm_is_empty(const TIDBitmap *tbm);

extern TBMIterator *r_tbm_begin_iterate(TIDBitmap *tbm);
extern dsa_pointer r_tbm_prepare_shared_iterate(TIDBitmap *tbm);
extern TBMIterateResult *r_tbm_iterate(TBMIterator *iterator);
extern TBMIterateResult *r_tbm_shared_iterate(TBMSharedIterator *iterator);
extern TBMSharedIterator *r_tbm_attach_shared_iterate(dsa_area *dsa,
						  dsa_pointer dp);

#endif							/* RSTORE_TIDBITMAP_H */
