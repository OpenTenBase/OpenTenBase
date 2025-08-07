/*-------------------------------------------------------------------------
 *
 * rmvdir.c
 *	  remove a directory
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	While "xcopy /e /i /q" works fine for copying directories, on Windows XP
 *	it requires a Window handle which prevents it from working when invoked
 *	as a service.
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/rmvdir.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
 
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
 
#include "storage/rmvdir.h"
#include "storage/fd.h"
#include "miscadmin.h"
#include "pgstat.h"

 /*
  * copydir: copy a directory
  *
  * If recurse is false, subdirectories are ignored.  Anything that's not
  * a directory or a regular file is ignored.
  */
 void
 rmvdir(char *dirname, bool force)
 {
	 DIR		*xldir;
	 struct dirent *xlde;
	 char		 fromfile[MAXPGPATH * 2];

	if (dirname == NULL)
	{
		return;
	}

  	if (!force)
 	{
 		if (unlink(dirname) != 0)
 		{
			 ereport(LOG,
					 (errcode_for_file_access(),
					  errmsg("could not remove directory \"%s\": %m", dirname))); 			 
 		}
 	}

	/* no such dir maybe, don't print */
	xldir = AllocateDir(dirname);
	if (NULL == xldir)
 	{
		 ereport(DEBUG1,
				 (errcode_for_file_access(),
				  errmsg("could not open directory \"%s\": %m", dirname)));
		 return;
 	}
 
	while ((xlde = ReadDir(xldir, dirname)) != NULL)
	{
		 struct stat fst;
 
		 /* If we got a cancel signal during the copy of the directory, quit */
		 CHECK_FOR_INTERRUPTS();
 
		 if (strcmp(xlde->d_name, ".") == 0 ||
			 strcmp(xlde->d_name, "..") == 0)
	 	 {
			 continue;
	 	}

		snprintf(fromfile, sizeof(fromfile), "%s/%s", dirname, xlde->d_name);
 
		if (lstat(fromfile, &fst) < 0)
		{
			 ereport(LOG,
					 (errcode_for_file_access(),
					  errmsg("could not stat file \"%s\": %m", fromfile)));
		}
 
		 if (S_ISDIR(fst.st_mode))
		 {
			/* recurse to handle subdirectories */
			if (force)
			{
				 rmvdir(fromfile, force);
			}
		}
		else if (S_ISREG(fst.st_mode))
		{
			if (unlink(fromfile) != 0)
			{
		 		ereport(LOG,
					(errcode_for_file_access(),
				 	  errmsg("could not unlink file \"%s\": %m", fromfile)));			
			}
		}
	 }
	 FreeDir(xldir);

	 if (rmdir(dirname) != 0)
	 {
		 ereport(LOG,
				 (errcode_for_file_access(),
				  errmsg("could not unlink file \"%s\": %m", dirname)));
	 }
 }

