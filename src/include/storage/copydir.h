/*-------------------------------------------------------------------------
 *
 * copydir.h
 *	  Copy a directory.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/copydir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYDIR_H
#define COPYDIR_H

extern void copydir(char *fromdir, char *todir, bool recurse);
extern bool copy_file(char *fromfile, char *tofile, int elevel);
bool is_directory_exists(const char *dir);

#endif							/* COPYDIR_H */
