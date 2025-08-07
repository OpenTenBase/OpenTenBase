/*-------------------------------------------------------------------------
 *
 * mt_checksum.cpp
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_rewind/unittest/mt_checksum.cpp
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "postgres.h"
#include "c.h"

#include <sys/stat.h>
#include <unistd.h>

#include "logging.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "storage/checksum.h"
#include "storage/bufpage.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"

#include "disk_verify.h"
}

#undef Max
#undef StrNCpy

#include "gtest/gtest.h"

#define MAX_THREAD 80

const char *basePath = "./test12213";
char temp_path[1024];
int numCrackedPage;
int numCrackedPageFound;
pthread_mutex_t mutex_pp;
extern FuncHandler checksumFailureHandler;

/* 
 * Do not remove the following declarations!
 */
char *datadir_target = NULL;
char *datadir_source = NULL;
bool debug = true;
bool showprogress = false;
bool dry_run = false;
bool test = false;

void 
makeRandomPage(Page page)
{
    int i;
    int num_bytes_to_changes = (rand() % 1000) + 1;
    int offset;
    PageHeader p = (PageHeader)page;

    for (i = 0; i < num_bytes_to_changes; i++)
    {
        offset = rand() % BLCKSZ;
        page[offset] = rand() % 256;
    }

    /* make a fake header */
    p->pd_flags = PD_VALID_FLAG_BITS;
    p->pd_lower = 0;
    p->pd_special = 10;
    p->pd_special = MAXALIGN(p->pd_special);
    p->pd_upper = p->pd_special;
}

void 
countCrackedPages(const char *page, BlockNumber blk)
{
    pthread_mutex_lock(&mutex_pp);
    numCrackedPageFound++;
    pthread_mutex_unlock(&mutex_pp);
}

void 
CreateFakeFiles(const char *ppath)
{
#define MAX_NUM_PAGES 10000
#define MAX_PRB_CRACK_PAGE 30
    int fd;
    int blkno = 0;
    char page_buffer[BLCKSZ];
    int num_pages = (rand() % MAX_NUM_PAGES) + 1;
    int i;
    int temp_rand;
    /* probablity is 5% to (MAX_PRB_CRACK_PAGE+5)% */
    int current_prob = (rand() % MAX_PRB_CRACK_PAGE) + 5;
    PageHeader p;
    numCrackedPage = 0;
    numCrackedPageFound = 0;
    uint16 checksum;
    char base_dir[1024];
    int mode;

    sprintf(base_dir, "%s/%s", ppath, "base");
    sprintf(temp_path, "%s/%s/%s", ppath, "base", "12355");

    mkdir(ppath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(base_dir, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    mode = O_RDWR | O_CREAT | PG_BINARY;

    fd = open(temp_path, mode, 0600);

    for (i = 0; i < num_pages; i++)
    {
        makeRandomPage(page_buffer);
        PageSetCheckSum(page_buffer);
        checksum = pg_checksum_page(page_buffer, blkno);
        p = (PageHeader)page_buffer;
        p->pd_checksum = checksum;

        /* 
        * Break some checksum with a probability defined by current_prob
        */
        temp_rand = random() % 100;

        if (temp_rand <= current_prob)
        {
            p->pd_checksum = ~(p->pd_checksum);
            numCrackedPage++;
        }
        write(fd, page_buffer, BLCKSZ);
        blkno++;
    }
    printf("Number of cracked / total pages: %d / %d\n", numCrackedPage, num_pages);

    close(fd);
}

TEST(DiskChecksumTest, PageChecksumNFailure)
{
    char path[1024];
    int threads = rand() % MAX_THREAD;
    checksumFailureHandler = countCrackedPages;
    CreateFakeFiles(basePath);
    verifyDisk(basePath, threads);
    remove(temp_path);
    sprintf(path, "%s/%s", basePath, "base");
    remove(path);
    remove(basePath);
    printf("Number of cracked pages found: %d\n", numCrackedPageFound);
    ASSERT_EQ(numCrackedPage, numCrackedPageFound);
}
