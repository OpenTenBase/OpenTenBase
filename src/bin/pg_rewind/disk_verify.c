/*-------------------------------------------------------------------------
 *
 * disk_verify.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_rewind/disk_verify.c
 *-------------------------------------------------------------------------
 */

#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <string.h>

#include "postgres_fe.h"
#include "common/string.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "fetch.h"
#include "disk_verify.h"
#include "logging.h"
#include "access/xlog.h"
#include "storage/bufpage.h"
#include "filemap.h"

extern char *datadir_target;

static void initTasks(int nt, const char *checksum_verify_file);
static void destroyTasks(void);
static void *loaderTask(void *arg);
static void *checkerTask(void *arg);
static void scanCallback(const char *parentpath, const char *path, file_type_t type, size_t oldsize, const char *link_target);
static void verifyScanCallback(const char *parentpath, const char *datapath, file_type_t type,
                                size_t oldsize, const char *link_target);
static void writeChecksumResultFile(FILE *resultfp, const char *file, 
                                    int64 offset, uint32 size, 
                                    bool checksum_enabled, 
                                    bool checksum_verified,
                                    bool needlock);

extern char *connstr_source;
checker_task_t *checker_tasks;
loader_task_t *loader_task;
pthread_mutex_t filemap_lck;
FILE* checksum_err_fp = NULL;
FILE* checksum_result_fp = NULL;
int nthread = 0;
bool noMoreTask = false;
int total_cracked_pages = 0;
pthread_mutex_t mutex_total;
char full_path[MAXPGPATH];
FuncHandler checksumFailureHandler = writeChecksumResultFile;



/*
 * Initialize the structures used by loader and checker. 
 */
static void
initTasks(int nt, const char *checksum_verify_file)
{
    int i;
    block_t *pb;
    loader_task = (loader_task_t *)pg_malloc(sizeof(loader_task_t));
    loader_task->head = NULL;
    loader_task->last = NULL;
    loader_task->nlist = 0;

    checker_tasks = (checker_task_t *)pg_malloc(nt * sizeof(checker_task_t));
    if (!checker_tasks)
    {
        fprintf(stderr, "Out of memory when allocates checker_tasks\n");
        exit(1);
    }
    nthread = nt;

    for (i = 0; i < nthread; i++)
    {
        pb = (block_t *)pg_malloc(CHECKER_BUFSZ * sizeof(block_t));
        if (!pb)
        {
            fprintf(stderr, "Out of memory when allocates buffers\n");
            exit(1);
        }
        checker_tasks[i].buffer = pb;
        checker_tasks[i].head = 0;
        checker_tasks[i].tail = 0;
        checker_tasks[i].size = 0;
        pthread_mutex_init(&checker_tasks[i].lck, NULL);
        pthread_cond_init(&checker_tasks[i].not_empty, NULL);
    }
    pthread_mutex_init(&filemap_lck, NULL);
    pthread_mutex_init(&mutex_total, NULL);

    checksum_err_fp = RemoveAndOpenFile(checksum_verify_file);
}

/*
 * Free data structures. 
 */

static void
destroyTasks(void)
{
    int idx;
    fe_t *entry = loader_task->head;
    fe_t *temp = NULL;

    while (entry)
    {
        temp = entry->next;
        pg_free(entry);
        entry = temp;
    }

    pg_free(loader_task);

    for (idx = 0; idx < nthread; idx++)
    {
        pg_free(checker_tasks[idx].buffer);
    }
    pg_free(checker_tasks);

    closeFile(checksum_err_fp);
}

/*
 * Loader thread: Load pages from disk and send them to 
 * checker threads. 
 */
static void *
loaderTask(void *arg)
{
    int idx = 0;
    int retcode;
    char *read_buffer = (char *)pg_malloc(BLCKSZ);
    ssize_t nbytes;
    int blkno = 0;
    int total_block = 0;
    fe_t *entry = loader_task->head;

    if (!read_buffer)
    {
        fprintf(stderr, "Out of memroy\n");
        exit(1);
    }

    while (entry)
    {
        char    fullpath[MAXPGPATH];
        int     fd = -1;

        snprintf(fullpath, MAXPGPATH, "%s/%s", datadir_target, entry->path);

        fd = open(fullpath, O_RDONLY | PG_BINARY, 0);
        if (fd == -1)
        {
            pg_fatal("File open failed: \"%s\", errno: %d, %s\n", entry->path, errno, strerror(errno));
            exit(1);
        }
        blkno = 0;
        do
        {
            /* load a page */
            nbytes = read(fd, read_buffer, BLCKSZ);
            Assert((nbytes % BLCKSZ) == 0);

            /* find a lucky checker thread */
            while (nbytes)
            {
                checker_task_t *task = &checker_tasks[idx];
                idx++;
                idx = idx % nthread;
                retcode = pthread_mutex_trylock(&(task->lck));
                if (!retcode)
                {
                    if (task->size < CHECKER_BUFSZ)
                    {
                        memcpy(task->buffer[task->tail].data, read_buffer, BLCKSZ);
                        memcpy(task->buffer[task->tail].path, entry->path, strlen(entry->path) + 1);
                        task->buffer[task->tail].blkno = blkno;
                        task->tail++;
                        task->tail = task->tail % CHECKER_BUFSZ;
                        task->size++;

                        /* notify the customer */
                        pthread_cond_signal(&(task->not_empty));
                        pthread_mutex_unlock(&(task->lck));
                        break;
                    }
                    pthread_mutex_unlock(&(task->lck));
                } // end if
            }     // end while

            /* update block number after sending the block */
            if (nbytes)
            {
                blkno++;
                total_block++;
            }
        } while (nbytes);

        close(fd);

        /* handle next file */
        entry = entry->next;
    }

    /*  work done, notify checker threads */
    noMoreTask = true;
    for (idx = 0; idx < nthread; idx++)
    {
        pthread_mutex_lock(&(checker_tasks[idx].lck));
        pthread_cond_signal(&(checker_tasks[idx].not_empty));
        pthread_mutex_unlock(&(checker_tasks[idx].lck));
    }
    pg_free(read_buffer);
    return NULL;
}

/*
 * Checker thread: verify the checksum of pages fetched from a page buffer, 
 * the buffer is with size 256. If chekcsum verification of a page is failed, 
 * mark it on filemap.
 */
static void *
checkerTask(void *arg)
{
    checker_task_t *ct = (checker_task_t *)arg;
    block_t *phead;
    int max_buf_size = 0;
    int total_block = 0;

    do
    {
        bool isVerified = true;

        pthread_mutex_lock(&(ct->lck));
        if (ct->size > max_buf_size)
        {
            max_buf_size = ct->size;
        }

        while (ct->size == 0)
        {
            if (noMoreTask)
            {
                pthread_mutex_unlock(&(ct->lck));
                return NULL;
            }
            pthread_cond_wait(&(ct->not_empty), &(ct->lck));
        }

        phead = &(ct->buffer[ct->head]);

        total_block++;
        isVerified = pageIsVerified(phead->data, phead->blkno);

        ct->head++;
        ct->head = ct->head % CHECKER_BUFSZ;
        ct->size--;

        pthread_mutex_unlock(&(ct->lck));

        if (!isVerified)
        {
            checksumFailureHandler(checksum_err_fp, phead->path, phead->blkno * BLCKSZ, BLCKSZ, true, false, true);
        }

    } while (true);

    return NULL;
}

/*
 * Copy checksum failed pages from new master
 */
void 
copy_crackedpages(const char *checksum_verify_file, bool uselibpq)
{
	char *content = NULL;

	FILE* file = fopen(checksum_verify_file, "r");

    if (file == NULL)
	{
        printf("Failed to open checksum error file: %s.\n", checksum_verify_file);
        exit(1);
    }

    if (uselibpq)
        libpq_executePrepare();

    content = palloc0(MAXPGPATH);
    while (fgets(content, MAXPGPATH, file) != NULL)
	{
		char    path[MAXPGPATH];
        char    fullpath[MAXPGPATH];
        char    *filename;
        int     offset;
        int     size;
        int     enable_checksum = 0;
        int     checksum_verified = 0;

        sscanf(content, "%[^:]:%d:%d:%d:%d", path, &offset, &size, &enable_checksum, &checksum_verified);
        if (checksum_verified)
            continue;

        filename = last_dir_separator(path);
        /*
         * For estore file, verify result file only record file's path 
         * without segnum, so add segnum to get fullpath.
         */
        if (strstr(filename, "_") != 0)
        {
	        snprintf(fullpath, MAXPGPATH, "%s.%ld", path, SILO_FILE_ID(offset));
            offset = SILO_FILE_OFFSET(offset);
        }
        else
        {
            snprintf(fullpath, MAXPGPATH, "%s", path);
        }

        if (uselibpq)
            libpq_copy_crackedpage(fullpath, offset, size);
        else
		    copy_crackedpage(fullpath, offset, size);
    }

    pfree(content);
    fclose(file);

    if (uselibpq)
        libpq_receiveChuncks();
}

/*
 * If file exists, remove and create a new file.
 */
FILE *
RemoveAndOpenFile(const char *filename)
{
    FILE *fp = NULL;

    if (access(filename, F_OK) == 0)
    {
        printf("File exists. Deleting...\n");
        if (remove(filename) == 0)
        {
            printf("File %s deleted successfully\n", filename);
        }
        else
        {
            printf("File %s deleted failed\n", filename);
            exit(1);
        }
    }

    fp = fopen(filename, "a");
    if (fp == NULL) 
    {
        fprintf(stderr, "Failed to open file: %s\n, errno: %d, %s", filename, errno, strerror(errno));
        exit(1);
    }

    return fp;
}

void
closeFile(FILE *fp)
{
    if (fp == NULL) 
        return;

    fclose(fp);
}

/*
 * Write checksum result to 'checksum_verify_file'
 */
static void
writeChecksumResultFile(FILE *resultfp, const char *file, int64 offset, 
                        uint32 size, bool checksum_enabled, 
                        bool checksum_verified, bool needlock)
{
    char *content = NULL;

    content = pg_malloc0(MAXPGPATH);

    snprintf(content, MAXPGPATH, "%s:%ld:%d:%d:%d", file, offset, size, checksum_enabled, checksum_verified);
    
    if (needlock)
        pthread_mutex_lock(&mutex_total);

    fprintf(resultfp, "%s\n", content);

    if (!checksum_verified)
        total_cracked_pages++;

    if (needlock)
        pthread_mutex_unlock(&mutex_total);

    pg_free(content);
}

/*
 * verify files callback function. Do the real verify.
 */
static void
verifyScanCallback(const char *datadir, const char *datapath, file_type_t type,
                    size_t oldsize, const char *link_target)
{
    int         fd = -1;
    int         blkno = 0;
    int         nbytes = 0;
    char        *read_buffer;
    char        fullpath[MAXPGPATH];
    
    if (type != FILE_TYPE_REGULAR)
    {
        return;
    }

    if (datadir)
    {
        snprintf(fullpath, MAXPGPATH, "%s/%s", datadir, datapath);
    }
    else
    {
        snprintf(fullpath, MAXPGPATH, "%s", datapath);
    }

    fd = open(fullpath, O_RDONLY | PG_BINARY, 0);
    if (fd == -1)
    {
        printf("File open failed: \"%s\", errno: %d, %s\n", datapath, 
                errno, strerror(errno));
        exit(1);
    }

    read_buffer = (char *)pg_malloc(BLCKSZ);
    do
    {
        Page    page;
        bool    checksum_enabled = false;
        bool    checksum_verified = true;

        memset(read_buffer, 0, BLCKSZ);
        /* load a page */
        nbytes = read(fd, read_buffer, BLCKSZ);

        /* update block number after sending the block */
        if (nbytes)
        {            
            page = read_buffer;
            checksum_enabled = PageHasCheckSum(page);
            if (checksum_enabled)
            {
                checksum_verified = pageIsVerified(page, blkno);
            }
            writeChecksumResultFile(checksum_result_fp, datapath, 
                                    blkno * BLCKSZ, BLCKSZ, checksum_enabled, 
                                    checksum_verified, false);
            blkno++;
        }

    } while (nbytes);

    close(fd);
}

/*
 * Check files in path according checksum.
 * Write the result in 'checksum_verify_file' with format of
 * filename:offset:size:checksum_enabled:checksum_verified
 */
void
verifyFiles(const char *datadir, const char *path)
{
    struct stat path_stat;
    char fullpath[MAXPGPATH * 2];

    snprintf(fullpath, MAXPGPATH * 2, "%s/%s", datadir, path);

    if (stat(fullpath, &path_stat) != 0) 
    {
        printf("Failed to get path information for %s\n", fullpath);
        exit(1);
    }

    checksum_result_fp = RemoveAndOpenFile("checksum_verify_file");

    if (S_ISREG(path_stat.st_mode))
    {
        verifyScanCallback(datadir, path, FILE_TYPE_REGULAR, 0, NULL);
    }
    else if (S_ISDIR(path_stat.st_mode))
    {
        recurse_dir(datadir, path, &verifyScanCallback);
    }
    else
    {
        printf("%s is not a file or a dir, can't verify\n", path);
        exit(1);
    }

    closeFile(checksum_result_fp);

    fprintf(stderr, "Total found cracked page: %d\n", total_cracked_pages);
}

/* 
 * Callback function for traverse_datadir, fiter out relfiles and 
 * add  them to a list. 
 */

static void
scanCallback(const char *parentpath, const char *path, file_type_t type,
             size_t oldsize, const char *link_target)
{
    fe_t *entry;
    bool is_relation_file = false;

    if (type != FILE_TYPE_REGULAR)
        return;

    is_relation_file = isRelDataFile(path);

    if (!is_relation_file)
        return;

    entry = (fe_t *)pg_malloc(sizeof(fe_t));
    memcpy(entry->path, path, strlen(path) + 1);
    entry->next = NULL;

    if (loader_task->head == NULL)
    {
        loader_task->head = entry;
        loader_task->last = entry;
        loader_task->nlist = 1;
    }
    else
    {
        loader_task->last->next = entry;
        loader_task->last = entry;
        loader_task->nlist += 1;
    }
}

/*
 * Interface provided for pg_rewind for disk verification. 
 * it uses multi-threads to scan the disk to find the cracked 
 * pages. 
 */
void 
verifyDisk(const char *data_path, int nt, const char *checksum_verify_file)
{
    int i;
    pthread_t loader_thread;
    pthread_t *checker_threads;

    /* Initialize loader and checker structure */
    initTasks(nt - 1, checksum_verify_file);

    printf("Disk verification is enabled, number of threads: %d\n", nt - 1);

    /* record relation files in base directory */
    traverse_datadir(data_path, &scanCallback);

    /* At least one checker thread */
    Assert(nthread >= 1);

    /* start mul-thread page scanning */
    checker_threads = (pthread_t *)pg_malloc((nthread) * sizeof(pthread_t));

    for (i = 0; i < nthread; i++)
    {
        pthread_create(&checker_threads[i], NULL, checkerTask, (void *)&checker_tasks[i]);
    }

    pthread_create(&loader_thread, NULL, loaderTask, NULL);

    /* All threads waiting here */
    pthread_join(loader_thread, NULL);

    for (i = 0; i < nthread; i++)
    {
        pthread_join(checker_threads[i], NULL);
    }
    fprintf(stderr, "Total found cracked page: %d\n", total_cracked_pages);
    pg_log(PG_PROGRESS, "Disk verification found %d cracked pages!", total_cracked_pages);
    destroyTasks();
}

/*
 * Calculate a checksum of a page, if the checksum equals to 
 * the value stored in the page header, return true, 
 * otherwise, return false
*/
bool 
pageIsVerified(Page page, BlockNumber blkno)
{
    bool header_sane = false;
    PageHeader p = (PageHeader)page;
    uint16 checksum = 0;
    bool isVerified = true;

    /* 
     * If checksum is not enabled, check whether the value of checksum
     * equls to 0
     */
    if (!PageHasCheckSum(page))
    {
        isVerified = (p->pd_checksum == 0);
        return isVerified;
    }

    if ((p->pd_flags & ~PD_VALID_FLAG_BITS) == 0 &&
        p->pd_lower <= p->pd_upper &&
        p->pd_upper <= p->pd_special &&
        p->pd_special <= BLCKSZ &&
        p->pd_special == MAXALIGN(p->pd_special))
        header_sane = true;

    if (!header_sane)
    {
        return false;
    }

    /* Calculate checksum of a page */
    checksum = pg_checksum_page(page, blkno);

    isVerified = checksum == p->pd_checksum;

    return isVerified;
}
