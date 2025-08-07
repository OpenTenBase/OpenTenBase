/*-------------------------------------------------------------------------
 *
 * fnbufpage.c
 *	  OpenTenBase forwar node buffer page code.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * IDENTIFICATION
 *	  src/backend/forward/storage/fnbufpage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "forward/fnbufpage.h"
#include "pgxc/pgxc.h"

/* ----------------------------------------------------------------
 *						Page support functions
 * ----------------------------------------------------------------
 */

/*
 * FnPageInit
 *		Initializes the contents of a page.
 */
void
FnPageInit(FnPage page, FNQueryId queryid, uint16 fid,
		   uint16 nodeid, uint16 workerid)
{
	FnPageHeader p = (FnPageHeader) page;

	p->queryid = queryid;
	p->fid = fid;
	p->nodeid = nodeid;
	p->workerid = workerid;
	p->flag = 0;
	p->lower = SizeOfFnPageHeaderData;
}

/*
 *	FnPageAddItem
 *
 *	Add an item to a page.  Return value is the offset at end of this page
 *	or InvalidOffsetNumber if the item is not inserted for any reason.
 *
 *	!!! EREPORT(ERROR) IS DISALLOWED HERE !!!
 */
void
FnPageAddItem(FnPage page, Item item, Size size, bool align)
{
	FnPageHeader phdr = (FnPageHeader) page;
	uint32		lower;
	uint32      offset;

	/*
	 * Select offsetNumber to place the new item at
	 */
	offset = FnPageGetLower(page);

	/*
	 * Compute new lower pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > upper.
	 */
	if (align)
		size = MAXALIGN(size);

	lower = offset + size;

	if (unlikely(lower > BLCKSZ))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("try to add oversized datum into fnpage: "
						   "size = %zu, curoff = %u", size, phdr->lower)));

	memcpy((char *) page + offset, (char *) item, size);

	/* adjust page header */
	phdr->lower = lower;
}
