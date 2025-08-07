/*-------------------------------------------------------------------------
 *
 * bgwriter.h
 *	  Exports from postmaster/bgwriter.c and postmaster/checkpointer.c.
 *
 * The bgwriter process used to handle checkpointing duties too.  Now
 * there is a separate process, but we did not bother to split this header.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/include/postmaster/bgwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BGWRITER_H
#define _BGWRITER_H

#include "storage/block.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"

typedef struct HTAB HTAB;
typedef struct UnlinkRelCxt
{
    HTAB *unlink_rel_hashtbl;
    LWLock rel_hashtbl_lock;
    HTAB *unlink_rel_fork_hashtbl;     /* delete one fork relation file */
    LWLock rel_one_fork_hashtbl_lock;
    Latch *invalid_buf_proc_latch;
} UnlinkRelCxt;

extern UnlinkRelCxt* UnlinkRelInfo;

extern Size UnlinkRelHTABShmemSize(void);
extern void InitUnlinkRelHTAB(void);
extern void DropRelFileNodeAllForksBuffers(void);
extern void DropRelFileNodeOneForkBuffers(void);
extern bool IsExistInUnlinkRelHTAB(RelFileNode *rnode);

/* GUC options */
extern int	BgWriterDelay;
extern int	CheckPointTimeout;
extern int	CheckPointWarning;
extern double CheckPointCompletionTarget;

extern void BackgroundWriterMain(void) pg_attribute_noreturn();
extern void CheckpointerMain(void) pg_attribute_noreturn();
extern void InvalidateBufferBgWriterMain(void) pg_attribute_noreturn();

extern void RequestCheckpoint(int flags);
extern void CheckpointWriteDelay(int flags, double progress);

extern bool ForwardFsyncRequest(RelFileNode rnode, ForkNumber forknum,
					BlockNumber segno);
extern void AbsorbFsyncRequests(void);

extern Size CheckpointerShmemSize(void);
extern void CheckpointerShmemInit(void);

extern bool FirstCallSinceLastCheckpoint(void);

#ifdef _MLS_
extern void RequestFlushRelcryptMap(void);
extern bool NeedFlushRelcryptMap(void);
extern void ClearRequestFlushRelcryptMap(void);

extern void RequestFlushCryptkeyMap(void);
extern bool NeedFlushCryptkeyMap(void);
extern void ClearRequestFlushCryptkeyMap(void);
#endif

#ifdef __OPENTENBASE_C__
extern bool CheckpointerIsRunning(void);
#endif
extern void CheckpointerProcessSIGHUP(void);
#endif							/* _BGWRITER_H */
