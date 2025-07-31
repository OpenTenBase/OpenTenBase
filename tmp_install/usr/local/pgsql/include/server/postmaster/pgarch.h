/*-------------------------------------------------------------------------
 *
 * pgarch.h
 *      Exports from postmaster/pgarch.c.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 * 
 * src/include/postmaster/pgarch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PGARCH_H
#define _PGARCH_H

/* ----------
 * Archiver control info.
 *
 * We expect that archivable files within pg_wal will have names between
 * MIN_XFN_CHARS and MAX_XFN_CHARS in length, consisting only of characters
 * appearing in VALID_XFN_CHARS.  The status files in archive_status have
 * corresponding names with ".ready" or ".done" appended.
 * ----------
 */
#define MIN_XFN_CHARS    16
#define MAX_XFN_CHARS    40
#define VALID_XFN_CHARS "0123456789ABCDEF.history.backup.partial"


#ifdef __OPENTENBASE__

typedef enum
{
    ARCHSTATUS_CONTINUE,        /* continue archive xlog */
    ARCHSTATUS_BREAK            /* break archive xlog */
} ArchStatusControl;

/* break off or continue archive xlog procedual */
extern int archive_status_control;

/* How often to force a poll of the archive status directory; in seconds. */
extern int archive_autowake_interval;

#endif

/* ----------
 * Functions called from postmaster
 * ----------
 */
extern int    pgarch_start(void);

#ifdef EXEC_BACKEND
extern void PgArchiverMain(int argc, char *argv[]) pg_attribute_noreturn();
#endif

#endif                            /* _PGARCH_H */
