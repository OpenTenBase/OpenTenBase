/*-------------------------------------------------------------------------
 *
 * disk_verify.h
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_rewind/disk_verify.h
  *-------------------------------------------------------------------------
 */

#ifndef DISK_VERIFY_H_
#define DISK_VERIFY_H_

#include <pthread.h>
#include "pg_config.h"
#include "pg_config_manual.h"
#include "storage/block.h"
#include "storage/bufpage.h"
#include "filemap.h"

#define CHECKER_BUFSZ 256
#define PageHasCheckSum(page) \
         (((PageHeader) (page))->pd_flags & PD_CRC_CHECK)

typedef struct 
{
    char data[BLCKSZ];
    char path[MAXPGPATH];
    BlockNumber blkno;
} block_t;

typedef struct 
{
    block_t *buffer;
    int head;
    int tail;
    int size;
    pthread_mutex_t lck;
    pthread_cond_t not_empty;
} checker_task_t;

typedef struct fe_t
{
    char path[MAXPGPATH];
    struct fe_t *next;
} fe_t;


typedef struct 
{
    fe_t *head;
    fe_t *last;
    size_t nlist;
} loader_task_t;

//typedef void (*FuncHandler)(const char *path, uint32 offset, uint32 size);
typedef void (*FuncHandler)(FILE *resultfp, const char *file, int64 offset, 
                            uint32 size, bool checksum_enabled, 
                            bool checksum_verified, bool needlock);
extern void verifyDisk(const char *data_path, int nt, const char *checksum_verify_file);
extern bool pageIsVerified(Page page, BlockNumber blkno);
extern void copy_crackedpages(const char *checksum_verify_file, bool uselibpq);
extern void verifyFiles(const char *datadir, const char *path);
extern FILE *RemoveAndOpenFile(const char *filename);
extern void closeFile(FILE *fp);

#endif
