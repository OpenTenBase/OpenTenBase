/*-------------------------------------------------------------------------
 *
 * fnbufmgr.h
 *	  OpenTenBase forward node buffer manager definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * src/include/forward/fnbufmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FNBUFMGR_H
#define FNBUFMGR_H

#include "forward/fnbuf.h"
#include "forward/fnbuf_internals.h"
#include "storage/spin.h"

/* in globals.c ... this duplicates miscadmin.h */
extern PGDLLIMPORT int FnNBuffers;

/* in fnbuf_init.c */
extern PGDLLIMPORT char **FnBufferBlocks;
extern PGDLLIMPORT HTAB *FnRcvBufferHash;
extern PGDLLIMPORT HTAB *FnSndBufferHash;
extern fn_desc_queue *ForwardSenderQueue;

extern void InitFnBufferPool(void);
extern Size FnBufferShmemSize(void);

/* in fnbufmgr.c */
extern void PinFnBuffer(FnBufferDesc *buf);
extern FnBufferDesc *FnBufferAlloc(fn_page_kind kind, FNQueryId queryid);
extern void FnBufferFree(FnBufferDesc *buf);
extern bool FnBufferSync(void);
extern void MarkFnBufferDirty(FnBufferDesc *buf, uint32 flag);
extern void PrintFnBufferLeakWarning(FnBuffer buffer);
extern void ClearFnSndQueueEntry(FnSndQueueEntry *entry);
extern void ClearFnRcvQueueEntry(FnRcvQueueEntry *entry);

static inline uint32
FnRcvQueueGetIndex(FnRcvQueueEntry *entry, uint32 origin)
{
	uint32 current = origin;

	while (!entry->valid[current])
	{
		SpinLockRelease(&entry->mutex[current]);
		current = (current + 1) % entry->nqueue;
		SpinLockAcquireNoPanic(&entry->mutex[current]);

		if (current == origin) /* loop ? return ! */
			return origin;
	}

	return current;
}

#endif							/* FNBUFMGR_H */
