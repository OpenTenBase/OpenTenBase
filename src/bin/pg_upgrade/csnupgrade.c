#define CSN_UPGRADE

#include "postgres_fe.h"
#include "pg_upgrade.h"
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/csnlog.h"
#include "access/lru.h"
#include "access/slru.h"
#include "access/subtrans.h"
#include "access/transam.h"
#include "datatype/timestamp.h"
#include "catalog/pg_control.h"
#include "common/controldata_utils.h"
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include <string.h>

#undef CSN_UPGRADE

#define NUM_THREADS 8

bool csn_upgrade = false;

char csn_dir[MAXPGPATH];
char clog_dir[MAXPGPATH];
char cts_dir[MAXPGPATH];
char subtrans_dir[MAXPGPATH];

typedef struct {
    TransactionId   start_xid;
    TransactionId   end_xid;

    TransactionId   step;
    TransactionId   next_part_xid;

    pthread_mutex_t	part_lock;
} XidParts;

typedef struct {
    int      thread_index;
    XidParts *parts;

    FILE *clog_fp;
    char *clog_lastfile;
    char *clog_buffer;
    int  clog_curpageno;

    FILE *cts_fp;
    char *cts_lastfile;
    char *cts_buffer;
    int  cts_curpageno;

    FILE *csn_fp;
    char *csn_buffer;
    char *csn_lastfile;
    bool csn_buffer_dirty;
    int  csn_buffer_offset;
    
} WorkerInfo;

void init_worker(WorkerInfo *worker, int thread_index, XidParts *parts);
bool xid_precedes(TransactionId id1, TransactionId id2);

XidStatus xid_get_status(TransactionId xid, WorkerInfo *worker);
GlobalTimestamp xid_get_cts(TransactionId xid, WorkerInfo *worker);
void xid_set_csn(TransactionId xid, CommitSeqNo csn, WorkerInfo *worker);
void xid_set_csn_finish(WorkerInfo *worker);
bool get_next_part(XidParts *allparts, TransactionId *next_start, TransactionId *next_end, WorkerInfo *worker);
void *thread_func(void *arg);

void
init_worker(WorkerInfo *worker, int thread_index, XidParts *parts)
{
    worker->thread_index = thread_index;
    worker->parts = parts;

    worker->clog_fp = NULL;
    worker->clog_lastfile = (char*)calloc(MAXPGPATH, 1);
    worker->clog_curpageno = -1;
    worker->clog_buffer = NULL;

    worker->cts_fp = NULL;
    worker->cts_lastfile = (char*)calloc(MAXPGPATH, 1);
    worker->cts_curpageno = -1;
    worker->cts_buffer = NULL;

    worker->csn_fp = NULL;
    worker->csn_lastfile = (char*)calloc(MAXPGPATH, 1);
    worker->csn_buffer = NULL;
    worker->csn_buffer_offset = -1;
    worker->csn_buffer_dirty = false;

}

XidStatus 
xid_get_status(TransactionId xid, WorkerInfo *worker)
{
    uint32      pageno = TransactionIdToPage(xid);
    uint32      segno = pageno / SLRU_PAGES_PER_SEGMENT;
    uint32	    rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int	        offset = rpageno * BLCKSZ;
    char        tgfile[MAXPGPATH];
    int			byteno = TransactionIdToByte(xid);
    int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
    char	    *byteptr;
    XidStatus	status;
    
    if(worker->clog_buffer == NULL)
    {
        worker->clog_buffer = (char*) malloc(BLCKSZ);
    }

    snprintf(tgfile, MAXPGPATH, "%s/%04X", clog_dir, segno);

    if (strncmp(worker->clog_lastfile, tgfile, MAXPGPATH) != 0)
    {
        if (worker->clog_fp != NULL)
        {
            fclose(worker->clog_fp);
            worker->clog_fp = NULL;
        }

        worker->clog_fp = fopen(tgfile, "r");
        if (worker->clog_fp == NULL)
        {
            pg_fatal("file \"%s\" fopen error.\n", tgfile);
        }
        
        snprintf(worker->clog_lastfile, MAXPGPATH, "%s", tgfile);
    }
    
    if(pageno != worker->clog_curpageno)
    {
        if (fseek(worker->clog_fp, (off_t) offset, SEEK_SET) < 0)
        {
            fclose(worker->clog_fp);
            pg_fatal("file \"%s\" seek failed.\n", tgfile);
        }

        if (fread(worker->clog_buffer, 1, BLCKSZ, worker->clog_fp) != BLCKSZ)
        {
            fclose(worker->clog_fp);
            pg_fatal("file \"%s\" fread failed.\n", tgfile);
        }

        worker->clog_curpageno = pageno;
    }

    byteptr = worker->clog_buffer + byteno;
    status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

    return status;
}

GlobalTimestamp 
xid_get_cts(TransactionId xid, WorkerInfo *worker)
{
    uint32          pageno = TransactionIdToCTsPage(xid);
    uint32          segno = pageno / LRU_PAGES_PER_SEGMENT;
    uint32	        rpageno = pageno % LRU_PAGES_PER_SEGMENT;
    int	            offset = rpageno * BLCKSZ;
    int			    entryno = TransactionIdToCTsEntry(xid);
    char            tgfile[MAXPGPATH];
    GlobalTimestamp cts;
    CommitTimestampEntry entry;
    
    if(worker->cts_buffer == NULL)
    {
        worker->cts_buffer = (char*) malloc(BLCKSZ);
    }

    snprintf(tgfile, MAXPGPATH, "%s/%04X", cts_dir, segno);

    if (strncmp(worker->cts_lastfile, tgfile, MAXPGPATH) != 0)
    {
        if (worker->cts_fp != NULL)
        {
            fclose(worker->cts_fp);
            worker->cts_fp = NULL;
        }

        worker->cts_fp = fopen(tgfile, "r");
        if (worker->cts_fp == NULL)
        {
            pg_fatal("file \"%s\" fopen error.\n", tgfile);
        }

        snprintf(worker->cts_lastfile, MAXPGPATH, "%s", tgfile);
    }

    if(pageno != worker->cts_curpageno)
    {
        if (fseek(worker->cts_fp, (off_t) offset, SEEK_SET) < 0)
        {
            fclose(worker->cts_fp);
            pg_fatal("file \"%s\" seek failed.\n", tgfile);
        }

        if (fread(worker->cts_buffer, 1, BLCKSZ, worker->cts_fp) != BLCKSZ)
        {
            fclose(worker->cts_fp);
            pg_fatal("file \"%s\" fread failed.\n", tgfile);
        }

        worker->cts_curpageno = pageno;
    }

    memcpy(&entry, worker->cts_buffer + SizeOfCommitTimestampEntry * entryno, SizeOfCommitTimestampEntry);
    cts = entry.global_timestamp;
    
    return cts;
}

void 
xid_set_csn(TransactionId xid, CommitSeqNo csn, WorkerInfo *worker)
{
    uint32      pageno = TransactionIdToCSNPage(xid);
    uint32	    segno = pageno / LRU_PAGES_PER_SEGMENT;
    int	        rpageno = pageno % LRU_PAGES_PER_SEGMENT;
    int	        offset = rpageno * BLCKSZ;
    int			entryno = TransactionIdToCSNPgIndex(xid);

    char        tgfile[MAXPGPATH];
    
    CommitSeqNo *ptr;

    if(worker->csn_buffer == NULL)
    {
        worker->csn_buffer = (char*) malloc(BLCKSZ);
        memset(worker->csn_buffer, 0, BLCKSZ);
    }
    
    snprintf(tgfile, MAXPGPATH, "%s/%04X", csn_dir, segno);
    
    if (strncmp(worker->csn_lastfile, tgfile, MAXPGPATH) != 0)
    {
        if (worker->csn_fp != NULL)
        {
            if (fflush(worker->csn_fp) != 0)
            {
                pg_fatal("file \"%s\" fflush error.\n", worker->csn_lastfile);
            }
            if (fsync(fileno(worker->csn_fp)) != 0)
            {
                pg_fatal("file \"%s\" fsync error.\n", worker->csn_lastfile);
            }
            
            if (fclose(worker->csn_fp) != 0)
            {
                pg_fatal("file \"%s\" fclose error.\n", worker->csn_lastfile);
            }
            worker->csn_fp = NULL;
        }

        worker->csn_fp = fopen(tgfile, "w");
        if (worker->csn_fp == NULL)
        {
            pg_fatal("could not open file for write \"%s\".\n", tgfile);
        }

        snprintf(worker->csn_lastfile, MAXPGPATH, "%s", tgfile);
    }

    ptr = (CommitSeqNo *)worker->csn_buffer + entryno;
    *ptr = csn;
    
    if(!worker->csn_buffer_dirty)
    {
        worker->csn_buffer_dirty = true;
        worker->csn_buffer_offset = offset;
    }
    
    if(TransactionIdToCSNPgIndex(xid + 1) == 0)
    {
        if (fseek(worker->csn_fp, (off_t) offset, SEEK_SET) < 0)
        {
            if (ferror (worker->csn_fp))
                printf ("Error Writing to myfile.txt %d.\n", ferror (worker->csn_fp));
            perror ("The following error occurred.");
            
            fclose(worker->csn_fp);
            pg_fatal("file \"%s\" seek failed.\n", tgfile);
        }
        if (fwrite(worker->csn_buffer, 1, BLCKSZ, worker->csn_fp) != BLCKSZ)
        {
            fclose(worker->csn_fp);
            pg_fatal("file \"%s\" fwrite failed.\n", tgfile);
        }
        memset(worker->csn_buffer, 0, BLCKSZ);
        worker->csn_buffer_dirty = false;
        worker->csn_buffer_offset = -1;
    }
}

void 
xid_set_csn_finish(WorkerInfo *worker)
{
    if (worker->csn_fp != NULL)
    {
        if(worker->csn_buffer_dirty)
        {
            if (fseek(worker->csn_fp, (off_t) worker->csn_buffer_offset, SEEK_SET) < 0)
            {
                fclose(worker->csn_fp);
                pg_fatal("file \"%s\" seek failed.\n", worker->csn_lastfile);
            }
            if (fwrite(worker->csn_buffer, 1, BLCKSZ, worker->csn_fp) != BLCKSZ)
            {
                fclose(worker->csn_fp);
                pg_fatal("file \"%s\" fwrite failed.\n", worker->csn_lastfile);
            }
            memset(worker->csn_buffer, 0, BLCKSZ);

            worker->csn_buffer_dirty = false;
            worker->csn_buffer_offset = -1;
        }

        if (fflush(worker->csn_fp) != 0)
        {
            pg_fatal("file \"%s\" fflush error.\n", worker->csn_lastfile);
        }
        if (fsync(fileno(worker->csn_fp)) != 0)
        {
            pg_fatal("file \"%s\" fsync error.\n", worker->csn_lastfile);
        }

        if (fclose(worker->csn_fp) != 0)
        {
            pg_fatal("file \"%s\" fclose error.\n", worker->csn_lastfile);
        }
        worker->csn_fp = NULL;
    }
}

bool
xid_precedes(TransactionId id1, TransactionId id2)
{
    /*
     * If either ID is a permanent XID then we can just do unsigned
     * comparison.  If both are normal, do a modulo-2^32 comparison.
     */
    int32		diff;

    if (!TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2))
        return (id1 < id2);

    diff = (int32) (id1 - id2);
    return (diff < 0);
}

int	upgrade_to_csn(ClusterInfo *cluster)
{
    bool crc_ok     = false;
    ControlFileData *ControlFile;
    TransactionId   oxid;
    TransactionId   nextXid;
    pthread_t       *threads;
    WorkerInfo      *workers;
    XidParts        allparts;
    int             i;
    
    snprintf(csn_dir, sizeof(csn_dir), "%s/%s", new_cluster.pgdata, "pg_csnlog");
    snprintf(clog_dir, sizeof(clog_dir), "%s/%s", new_cluster.pgdata, "pg_xact");
    snprintf(cts_dir, sizeof(cts_dir), "%s/%s", new_cluster.pgdata, "pg_commit_ts");
    snprintf(subtrans_dir, sizeof(cts_dir), "%s/%s", new_cluster.pgdata, "pg_subtrans");

    if (mkdir(csn_dir, S_IRWXU) < 0)
    {
        if (errno != EEXIST)
            pg_fatal("FATAL: create dir %s faild.\n", csn_dir);
    }
    
    ControlFile = get_controlfile(cluster->pgdata, "pg_upgrade", &crc_ok);

    if (!crc_ok)
        pg_fatal(_("FATAL: Calculated pg_control CRC checksum does not match value stored in file.\n"
                 "Either the file is corrupt, or it has a different layout than this program.\n"
                 "is expecting.  The results below are untrustworthy.\n\n"));

    oxid = ControlFile->checkPointCopy.oldestXid;
    nextXid = ControlFile->checkPointCopy.nextXid;
    
    if (oxid > nextXid)
        pg_log(PG_REPORT, "xid wrap around xids: oldestXid %u, nextXid %u.\n", oxid, nextXid);
    
    allparts.start_xid = oxid;
    allparts.next_part_xid = oxid;
    allparts.end_xid = nextXid;
    allparts.step = CSNLOG_XACTS_PER_PAGE * LRU_PAGES_PER_SEGMENT * 8;
    
    if (pthread_mutex_init(&allparts.part_lock, NULL) != 0)
        pg_fatal("could not int mutex.\n");
    
    threads = (pthread_t *) malloc(sizeof(pthread_t) * NUM_THREADS);
    workers = (WorkerInfo *) malloc(sizeof(WorkerInfo) * NUM_THREADS);
    
    for(i = 0; i < NUM_THREADS; i++)
    {
        int err;
        
        init_worker(&workers[i], i, &allparts);
        err = pthread_create(&threads[i], NULL, thread_func, (void *)&workers[i]);
        if (err != 0)
        {
            pg_fatal("could not create thread: %s\n", strerror(err));
        }
    }

    for(i = 0; i < NUM_THREADS; i++)
    {
        int err = pthread_join(threads[i], NULL);

        if (err != 0)
        {
            pg_fatal("could not join thread: %s\n", strerror(err));
        }
    }
    
    return 0;
}



bool 
get_next_part(XidParts *allparts, TransactionId *next_start, TransactionId *next_end, WorkerInfo *worker)
{
    bool    res = false;
    uint32  pageno;
    uint32	segno;
    
    pthread_mutex_lock(&allparts->part_lock);

    if(xid_precedes(allparts->next_part_xid, allparts->end_xid))
    {
        *next_start = allparts->next_part_xid;

        pageno = TransactionIdToCSNPage(*next_start);
        segno = pageno / LRU_PAGES_PER_SEGMENT;
        allparts->next_part_xid = segno * LRU_PAGES_PER_SEGMENT * CSNLOG_XACTS_PER_PAGE;
        
        allparts->next_part_xid = allparts->next_part_xid + allparts->step;

        if(!xid_precedes(allparts->next_part_xid, allparts->end_xid))
        {
            allparts->next_part_xid = allparts->end_xid;
        }
        *next_end = allparts->next_part_xid;
        
        res = true;
        pg_log(PG_REPORT, "worker started %d, xid %u -> %u.\n",
               worker->thread_index, *next_start, *next_end);
    }
    
    pthread_mutex_unlock(&allparts->part_lock);
    
    return res;
}
void *thread_func(void *arg)
{
    WorkerInfo *worker = (WorkerInfo *)arg;
    TransactionId next_start;
    TransactionId next_end;
    TransactionId xid;

    pg_log(PG_REPORT, "worker started %d.\n", worker->thread_index);
    
    while(get_next_part(worker->parts, &next_start, &next_end, worker))
    {
        for(xid = next_start; xid_precedes(xid, next_end); xid++)
        {

            XidStatus       stat;
            GlobalTimestamp cts;
            CommitSeqNo     csn = CSN_INPROGRESS;

            if(!TransactionIdIsNormal(xid))
                continue;

            stat = xid_get_status(xid, worker);
            cts = xid_get_cts(xid, worker);

            if(stat == TRANSACTION_STATUS_IN_PROGRESS)
            {
                csn = CSN_INPROGRESS;
            }
            else if (stat == TRANSACTION_STATUS_COMMITTED)
            {
                csn = cts;
            }
            else if (stat == TRANSACTION_STATUS_ABORTED)
            {
                csn = CSN_ABORTED;
            }
            else if (stat == TRANSACTION_STATUS_SUB_COMMITTED)
            {
                /* we assume there is no transaction record generated by the previous binary version, 
                 * so  this intermediate should not happen when upgrade to csn version. 
                 */
                pg_fatal(_("FATAL: TRANSACTION_STATUS_SUB_COMMITTED int previous clog.\n"
                           "Need to do crash recovery with the previous binary version.\n\n"));
            }

            xid_set_csn(xid, csn, worker);
        }

        xid_set_csn_finish(worker);
    }
    
    return NULL;
}
