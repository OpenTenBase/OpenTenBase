/*-------------------------------------------------------------------------
 *
 * buffile.h
 *      Management of large buffered files, primarily temporary files.
 *
 * The BufFile routines provide a partial replacement for stdio atop
 * virtual file descriptors managed by fd.c.  Currently they only support
 * buffered access to a virtual file, without any of stdio's formatting
 * features.  That's enough for immediate needs, but the set of facilities
 * could be expanded if necessary.
 *
 * BufFile also supports working with temporary files that exceed the OS
 * file size limit and/or the largest offset representable in an int.
 * It might be better to split that out as a separately accessible module,
 * but currently we have no need for oversize temp files without buffered
 * access.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/storage/buffile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BUFFILE_H
#define BUFFILE_H

#ifdef __OPENTENBASE__
#include "utils/dsa.h"
#endif

/* BufFile is an opaque type whose details are not known outside buffile.c. */

typedef struct BufFile BufFile;

/*
 * prototypes for functions in buffile.c
 */

extern BufFile *BufFileCreateTemp(bool interXact);
extern void BufFileClose(BufFile *file);
extern size_t BufFileRead(BufFile *file, void *ptr, size_t size);
extern size_t BufFileWrite(BufFile *file, void *ptr, size_t size);
extern int    BufFileSeek(BufFile *file, int fileno, off_t offset, int whence);
extern void BufFileTell(BufFile *file, int *fileno, off_t *offset);
extern int    BufFileSeekBlock(BufFile *file, long blknum);

#ifdef __OPENTENBASE__
extern int FlushBufFile(BufFile *file);
extern char *getBufFileName(BufFile *file, int fileIndex);
extern void CreateBufFile(dsa_area *dsa, int fileNum, dsa_pointer *fileName, BufFile **fileptr);
extern int NumFilesBufFile(BufFile *file);
extern bool BufFileReadDone(BufFile *file);
extern void ReSetBufFile(BufFile *file);
#endif
#ifdef _MLS_
extern BufFile * BufFileOpen(char* fileName, int fileFlags, int fileMode, bool interXact, int log_level);
#endif
#endif                            /* BUFFILE_H */
