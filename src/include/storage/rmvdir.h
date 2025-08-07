/*-------------------------------------------------------------------------
 *
 * rmvdir.h
 *	  Remove a directory.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/rmvdir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RMVDIR_H
#define RMVDIR_H

extern void rmvdir(char *dirname, bool force);

#endif							/* RMVDIR_H */

