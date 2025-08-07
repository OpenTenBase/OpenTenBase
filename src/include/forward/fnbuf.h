/*-------------------------------------------------------------------------
 *
 * fnbuf.h
 *	  Basic forward node buffer manager data types.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2022, Tencent OpenTenBase Group
 *
 * src/include/forward/fnbuf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FNBUF_H
#define FNBUF_H

/*
 * FnBuffer identifiers.
 *
 * Zero is invalid, positive is the index of a shared buffer (1..NBuffers),
 * negative is impossible.
 */
typedef int FnBuffer;

#define InvalidFnBuffer	0

/*
 * FnBufferIsInvalid
 *		True if the buffer is invalid.
 */
#define FnBufferIsInvalid(buffer) ((buffer) == InvalidFnBuffer)

#endif							/* FNBUF_H */
