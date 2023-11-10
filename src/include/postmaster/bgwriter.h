/*-------------------------------------------------------------------------
 *
 * bgwriter.h
 *      Exports from postmaster/bgwriter.c and postmaster/checkpointer.c.
 *
 * The bgwriter process used to handle checkpointing duties too.  Now
 * there is a separate process, but we did not bother to split this header.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/postmaster/bgwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BGWRITER_H
#define _BGWRITER_H

#include "storage/block.h"
#include "storage/relfilenode.h"


/* GUC options */
extern int    BgWriterDelay;
extern int    CheckPointTimeout;
extern int    CheckPointWarning;
extern double CheckPointCompletionTarget;

extern void BackgroundWriterMain(void) pg_attribute_noreturn();
extern void CheckpointerMain(void) pg_attribute_noreturn();

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

#endif                            /* _BGWRITER_H */
