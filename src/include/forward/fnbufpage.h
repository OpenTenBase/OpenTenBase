/*-------------------------------------------------------------------------
 *
 * fnbufpage.h
 *	  OPENTENBASE forward node's buffer page definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/forward/fnbufpage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FNBUFPAGE_H
#define FNBUFPAGE_H

#include "storage/item.h"

/*
 * A forward node page is a piece of memory to organize the data.
 *
 * The FN page is always a slotted page of the form:
 *
 * +------------------+-------------------------------+
 * | FnPageHeaderData | tuple1, tuple2 ...            |
 * +------------------+-------------------------------+
 * |                                                  |
 * |                                                  |
 * |                                                  |
 * |                                                  |
 * |                                v lower           |
 * +--------------------------------------------------+
 * |                     ... tupleN |                 |
 * +--------------------------------------------------+
 * |                                                  |
 * +--------------------------------------------------+
 *
 * the page is always in the memory.
 */
#define	FNPAGE_HUGE		(1U)
#define	FNPAGE_HUGE_END	(1U << 1)
#define FNPAGE_VECTOR	(1U << 2)
#define	FNPAGE_END		(1U << 3)
#define	FNPAGE_PARAMS	(1U << 4)

typedef Pointer FnPage;

typedef struct FNQueryId
{
	int64	timestamp_nodeid;
	int64	sequence;
} FNQueryId;

typedef struct FnPageHeaderData
{
	uint32		lower;      /* offset to start of free space */
	uint16		fid;        /* fragment id */
	uint16		nodeid;     /* src node id */
	FNQueryId	queryid;    /* query id */
	uint32		flag;		/* extra information */
	uint16		workerid;   /* src parallel worker id */
	uint8		virtualid;	/* dest virtual dn id */
	uint8		padding;
	char		data[FLEXIBLE_ARRAY_MEMBER]; /* flexible data array */
} FnPageHeaderData;

typedef FnPageHeaderData *FnPageHeader;

typedef struct FnPageIterator
{
	uint16      offset;
} FnPageIterator;

/* ----------------------------------------------------------------
 *						page support macros
 * ----------------------------------------------------------------
 */

/*
 * FnPageIsValid
 *		True iff page is valid.
 */
#define FnPageIsValid(page) PointerIsValid(page)

/*
 * actual data pointer do not count as part of header
 */
#define SizeOfFnPageHeaderData (offsetof(FnPageHeaderData, data))

/*
 * FnPageIsEmpty
 *		returns true if no tuple has been allocated on the page
 * FnPageSetEmpty
 *      set lower to SizeOfFnPageHeaderData
 */
#define FnPageIsEmpty(page) \
	(((FnPageHeader) (page))->lower <= SizeOfFnPageHeaderData)

/*
 * Iterate facility of forward page, unlike disk page, we don't use item
 * pointer in fn page due to different page structure. the data inside fn
 * page is always in order of (length, data, length, data, ...). So we
 * iterate fn buffer with a iterator, current pos is preserved inside.
 * The code should be format like:
 *
 * InitFnPageIterator(iter)
 * while (!FnPageIterateDone(page, iter))
 * {
 *      uint32  size;
 *      Item    data;
 *      FnPageIterateNext(page, iter, size, data);
 *      ...
 * }
 *
 * Note:
 *		This does not change the status of any of the resources passed.
 *		The semantics may change in the future.
 */
#define InitFnPageIterator(iter) ((iter)->offset = SizeOfFnPageHeaderData)
#define FnPageIterateNext(page, iter, size, data) \
	do { \
	    size = *(uint32 *)((Pointer)(page) + (iter)->offset);		\
	    data = (((Pointer)(page)) + (iter)->offset);				\
	    if (size != MAX_UINT32)										\
            (iter)->offset += MAXALIGN(size);						\
	} while (0)
#define FnPageIterateDone(page, iter) ((iter)->offset >= ((FnPageHeader)(page))->lower)

/*
 * FnPageGetLower
 *		Returns the current offset number used by the given page.
 *
 *		NOTE: if the page is not initialized (lower == 0), we must
 *		return zero to ensure sane behavior.  Accept double evaluation
 *		of the argument so that we can ensure this.
 */
#define FnPageGetLower(page) \
	(((FnPageHeader) (page))->lower <= SizeOfFnPageHeaderData ? \
	SizeOfFnPageHeaderData : ((FnPageHeader) (page))->lower)

#define FnPageReset(page) \
	do { \
        ((FnPageHeader) (page))->lower = SizeOfFnPageHeaderData; \
        ((FnPageHeader) (page))->flag = 0;                       \
	} while (0)
#define FnPageSetFlag(page, _flag_) (((FnPageHeader) (page))->flag |= (_flag_))

extern void FnPageAddItem(FnPage page, Item item, Size size, bool align);
extern void FnPageInit(FnPage page, FNQueryId queryid, uint16 fid,
					   uint16 nodeid, uint16 workerid);

#endif							/* FNBUFPAGE_H */
