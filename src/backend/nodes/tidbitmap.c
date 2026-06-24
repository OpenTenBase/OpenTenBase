/*-------------------------------------------------------------------------
 *
 * tidbitmap.c
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
 * The "lossy" storage uses one bit per disk page, so at the standard 8K
 * BLCKSZ, we can represent all pages in 64Gb of disk space in about 1Mb
 * of memory.  People pushing around tables of that size should have a
 * couple of Mb to spare, so we don't worry about providing a second level
 * of lossiness.  In theory we could fall back to page ranges at some
 * point, but for now that seems useless complexity.
 *
 * We also support the notion of candidate matches, or rechecking.  This
 * means we know that a search need visit only some tuples on a page,
 * but we are not certain that all of those tuples are real matches.
 * So the eventual heap scan must recheck the quals for these tuples only,
 * rather than rechecking the quals for all tuples on the page as in the
 * lossy-bitmap case.  Rechecking can be specified when TIDs are inserted
 * into a bitmap, and it can also happen internally when we AND a lossy
 * and a non-lossy page.
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/nodes/tidbitmap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include "access/htup_details.h"
#include "nodes/bitmapset.h"
#include "nodes/rstore_tidbitmap.h"
#include "nodes/tidbitmap.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/hashutils.h"


/*
 * Header of the TIDBitMap:
 */
struct TIDBitmap
{
	NodeTag		type;			/* to make it a valid Node */
	int			flags;			/* flags for rowstore or estore */
};

/* Header of the TBMSharedIteratorState */
typedef struct TBMSharedIteratorState
{
	int			flags;
} TBMSharedIteratorState;

/* Header of the TBMIterator */
struct TBMIterator
{
	TIDBitmap  *tbm;			/* TIDBitmap we're iterating over */
};

/* Header of the TBMSharedIterator */
struct TBMSharedIterator
{
	TBMSharedIteratorState *state;	/* shared state */
};

/*
 * wrapper function for tbm_create
 */
TIDBitmap *
tbm_create(long maxbytes, dsa_area *dsa, int flags)
{
	return r_tbm_create(maxbytes, dsa, flags);
}

/*
 * wrapper function for tbm_free
 */
void
tbm_free(TIDBitmap *tbm)
{
	if (likely(tbm->flags == TBM_FLAG_RSTORE))
	{
		r_tbm_free(tbm);
	}
	return;
}

/*
 * wrapper function for tbm_free_shared_area
 */
void
tbm_free_shared_area(dsa_area *dsa, dsa_pointer dp)
{
	TBMSharedIteratorState *istate = dsa_get_address(dsa, dp);
	if (likely(istate->flags == TBM_FLAG_RSTORE))
	{
		r_tbm_free_shared_area(dsa, dp);
	}
}

/*
 * wrapper function for tbm_add_tuples
 */
void
tbm_add_tuples(TIDBitmap *tbm, const ItemPointer tids, int ntids,
			   bool recheck)
{
	if (likely(tbm->flags == TBM_FLAG_RSTORE))
	{
		r_tbm_add_tuples(tbm, tids, ntids, recheck);
	}
}

/*
 * wrapper function for tbm_add_pages
 */
void
tbm_add_page(TIDBitmap *tbm, BlockNumber pageno)
{
	if (likely(tbm->flags == TBM_FLAG_RSTORE))
	{
		r_tbm_add_page(tbm, pageno);
	}
}

/*
 * wrapper function for tbm_union
 */
void
tbm_union(TIDBitmap *a, const TIDBitmap *b)
{
	if (likely(a->flags == TBM_FLAG_RSTORE))
	{
		r_tbm_union(a, b);
	}
}

/*
 * wrapper function for tbm_intersect
 */
void
tbm_intersect(TIDBitmap *a, const TIDBitmap *b)
{
	if (likely(a->flags == TBM_FLAG_RSTORE))
	{
		r_tbm_intersect(a, b);
	}
}

/*
 * wrapper function for tbm_is_empty
 */
bool
tbm_is_empty(const TIDBitmap *tbm)
{
	return r_tbm_is_empty(tbm);
}


/*
 * wrapper function for tbm_begin_iterate
 */
TBMIterator *
tbm_begin_iterate(TIDBitmap *tbm)
{
	return r_tbm_begin_iterate(tbm);
}

/*
 * wrapper function for tbm_prepare_shared_iterate
 */
dsa_pointer
tbm_prepare_shared_iterate(TIDBitmap *tbm)
{
	return r_tbm_prepare_shared_iterate(tbm);
}

/*
 * wrapper function for tbm_iterate
 */
TBMIterateResult *
tbm_iterate(TBMIterator *iterator)
{
	return r_tbm_iterate(iterator);
}

/*
 * wrapper function for tbm_shared_iterate
 */
TBMIterateResult *
tbm_shared_iterate(TBMSharedIterator *iterator)
{
	return r_tbm_shared_iterate(iterator);
}

/*
 * tbm_end_iterate - finish an iteration over a TIDBitmap
 *
 * Currently this is just a pfree, but it might do more someday.  (For
 * instance, it could be useful to count open iterators and allow the
 * bitmap to return to read/write status when there are no more iterators.)
 * only free the iterator, we have no RSTORE or ESTORE flags now,
 * so free it directly.
 */
void
tbm_end_iterate(TBMIterator *iterator)
{
	pfree(iterator);
}

/*
 * tbm_end_shared_iterate - finish a shared iteration over a TIDBitmap
 *
 * This doesn't free any of the shared state associated with the iterator,
 * just our backend-private state.
 * only free the iterator, we have no RSTORE or ESTORE flags now,
 * so free it directly.
 */
void
tbm_end_shared_iterate(TBMSharedIterator *iterator)
{
	pfree(iterator);
}

/*
 * wrapper function for tbm_attach_shared_iterate
 */
TBMSharedIterator *
tbm_attach_shared_iterate(dsa_area *dsa, dsa_pointer dp)
{
	return r_tbm_attach_shared_iterate(dsa, dp);
}
