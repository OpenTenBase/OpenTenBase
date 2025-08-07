/*-------------------------------------------------------------------------
 *
 * fd.h
 *	  Virtual file descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/fd.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * calls:
 *
 *	File {Close, Read, Write, Seek, Tell, Sync}
 *	{Path Name Open, Allocate, Free} File
 *
 * These are NOT JUST RENAMINGS OF THE UNIX ROUTINES.
 * Use them for all file activity...
 *
 *	File fd;
 *	fd = PathNameOpenFile("foo", O_RDONLY);
 *
 *	AllocateFile();
 *	FreeFile();
 *
 * Use AllocateFile, not fopen, if you need a stdio file (FILE*); then
 * use FreeFile, not fclose, to close it.  AVOID using stdio for files
 * that you intend to hold open for any length of time, since there is
 * no way for them to share kernel file descriptors with other files.
 *
 * Likewise, use AllocateDir/FreeDir, not opendir/closedir, to allocate
 * open directories (DIR*), and OpenTransientFile/CloseTransient File for an
 * unbuffered file descriptor.
 */
#ifndef FD_H
#define FD_H

#include <dirent.h>
#ifdef __OPENTENBASE_C__
#include "utils/resowner.h"
#endif


typedef enum RecoveryInitSyncMethod {
	RECOVERY_INIT_SYNC_METHOD_FSYNC,
	RECOVERY_INIT_SYNC_METHOD_SYNCFS
} RecoveryInitSyncMethod;

/*
 * FileSeek uses the standard UNIX lseek(2) flags.
 */

typedef int File;


/* GUC parameter */
extern PGDLLIMPORT int max_files_per_process;
extern PGDLLIMPORT bool data_sync_retry;
/*
 * This is private to fd.c, but exported for save/restore_backend_variables()
 */
extern int	max_safe_fds;

extern int recovery_init_sync_method;

/*
 * prototypes for functions in fd.c
 */

/* Operations on virtual Files --- equivalent to Unix kernel file ops */
extern File PathNameOpenFile(const char *fileName, int fileFlags);
extern File PathNameOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode);
extern File OpenTemporaryFile(bool interXact);
extern bool FileValid(File file);
extern void FileClose(File file);
extern int	FilePrefetch(File file, off_t offset, int amount, uint32 wait_event_info);
extern int	FileRead(File file, char *buffer, int amount, uint32 wait_event_info);
extern int FilePRead(File file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
extern int	FileWrite(File file, char *buffer, int amount, uint32 wait_event_info);
extern int FilePWrite(File file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
extern int	FileSync(File file, uint32 wait_event_info);
extern off_t FileSeek(File file, off_t offset, int whence);
extern int	FileTruncate(File file, off_t offset, uint32 wait_event_info);
extern int FileExtend(File file, off_t offset, uint32 len, bool keep_size, uint32 wait_event_info);
#ifdef _SHARDING_
extern int	FileDealloc(File file, off_t offset, uint32 len,  uint32 wait_event_info);
#endif
extern void FileWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info);
extern char *FilePathName(File file);
extern int	FileGetRawDesc(File file);
extern int	FileGetRawFlags(File file);
extern mode_t FileGetRawMode(File file);

/* Operations used for sharing named temporary files */
extern File PathNameCreateTemporaryFile(const char *name, bool error_on_failure);
extern File PathNameOpenTemporaryFile(const char *name);
extern bool PathNameDeleteTemporaryFile(const char *name, bool error_on_failure);
extern void PathNameCreateTemporaryDir(const char *base, const char *name);
extern void PathNameDeleteTemporaryDir(const char *name);
extern void TempTablespacePath(char *path, Oid tablespace);

/* Operations that allow use of regular stdio --- USE WITH CAUTION */
extern FILE *AllocateFile(const char *name, const char *mode);
extern int	FreeFile(FILE *file);

/* Operations that allow use of pipe streams (popen/pclose) */
extern FILE *OpenPipeStream(const char *command, const char *mode);
extern int	ClosePipeStream(FILE *file);

/* Operations to allow use of the <dirent.h> library routines */
extern DIR *AllocateDir(const char *dirname);
extern struct dirent *ReadDir(DIR *dir, const char *dirname);
extern struct dirent *ReadDirExtended(DIR *dir, const char *dirname,
				int elevel);
extern int	FreeDir(DIR *dir);

/* Operations to allow use of a plain kernel FD, with automatic cleanup */
extern int	OpenTransientFile(const char *fileName, int fileFlags);
extern int	OpenTransientFilePerm(const char *fileName, int fileFlags, mode_t fileMode);
extern int	CloseTransientFile(int fd);

/* If you've really really gotta have a plain kernel FD, use this */
extern int	BasicOpenFile(const char *fileName, int fileFlags);
extern int	BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode);

/* Miscellaneous support routines */
extern void InitFileAccess(void);
extern void set_max_safe_fds(void);
extern void closeAllVfds(void);
extern void SetTempTablespaces(Oid *tableSpaces, int numSpaces);
extern bool TempTablespacesAreSet(void);
extern int	GetTempTablespaces(Oid *tableSpaces, int numSpaces);
extern Oid	GetNextTempTableSpace(void);
extern void AtEOXact_Files(void);
extern void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid,
				  SubTransactionId parentSubid);
extern void CleanupTempFiles(bool isProcExit);
extern void RemovePgTempFiles(void);
extern int	pg_fsync(int fd);
extern int	pg_fsync_no_writethrough(int fd);
extern int	pg_fsync_writethrough(int fd);
extern int	pg_fdatasync(int fd);
extern void pg_flush_data(int fd, off_t offset, off_t amount);
extern void fsync_fname(const char *fname, bool isdir);
extern int	durable_rename(const char *oldfile, const char *newfile, int loglevel);
extern int	durable_unlink(const char *fname, int loglevel);
extern int	durable_link_or_rename(const char *oldfile, const char *newfile, int loglevel);
extern void SyncDataDirectory(void);
extern int data_sync_elevel(int elevel);

/* Filename components */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"
#define PG_CORE_FILE_PREFIX "core."
#define PG_DETECTED_CORE_FILE_PREFIX "detected_core."

#ifdef __OPENTENBASE_C__
extern void SetFileResourceOwner(File file, ResourceOwner owner);
#endif

extern void FileSetIsWorkfile(File file);
extern void FreeFileAccess(void);
#endif							/* FD_H */
