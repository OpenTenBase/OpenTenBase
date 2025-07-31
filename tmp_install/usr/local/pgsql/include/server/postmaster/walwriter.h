/*-------------------------------------------------------------------------
 *
 * walwriter.h
 *      Exports from postmaster/walwriter.c.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * src/include/postmaster/walwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _WALWRITER_H
#define _WALWRITER_H

/* GUC options */
extern int    WalWriterDelay;
extern int    WalWriterFlushAfter;
#ifdef __OPENTENBASE__
extern int    WalGTSAcquireDelay;
#endif
extern void WalWriterMain(void) pg_attribute_noreturn();

#endif                            /* _WALWRITER_H */
