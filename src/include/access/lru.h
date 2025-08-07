/*-------------------------------------------------------------------------
 *
 * slru.h
 *		Simple LRU buffering for transaction status logfiles
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/slru.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LRU_H
#define LRU_H

#ifndef CSN_UPGRADE
#include "access/xlogdefs.h"
#include "storage/lwlock.h"
#endif

#define NUM_PARTITIONS 32
#define BufHashPartition(hashcode) \
	((hashcode) % NUM_PARTITIONS)

#define INIT_LRUBUFTAG(a, pageNum) \
( \
	(a).pageno = (pageNum)\
)


/*
 * Define SLRU segment size.  A page is the same BLCKSZ as is used everywhere
 * else in Postgres.  The segment size can be chosen somewhat arbitrarily;
 * we make it 32 pages by default, or 256Kb, i.e. 1M transactions for CLOG
 * or 64K transactions for SUBTRANS.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * page numbering also wraps around at 0xFFFFFFFF/xxxx_XACTS_PER_PAGE (where
 * xxxx is CLOG or SUBTRANS, respectively), and segment numbering at
 * 0xFFFFFFFF/xxxx_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need
 * take no explicit notice of that fact in slru.c, except when comparing
 * segment and page numbers in SimpleLruTruncate (see PagePrecedes()).
 */
#define LRU_PAGES_PER_SEGMENT	32

/* Maximum length of an SLRU name */
#define LRU_MAX_NAME_LENGTH	32

#ifndef CSN_UPGRADE
/*
 * Page status codes.  Note that these do not include the "dirty" bit.
 * page_dirty can be TRUE only in the VALID or WRITE_IN_PROGRESS states;
 * in the latter case it implies that the page has been re-dirtied since
 * the write started.
 */
typedef enum
{
	LRU_PAGE_EMPTY,			/* buffer is not in use */
	LRU_PAGE_READ_IN_PROGRESS, /* page is being read in */
	LRU_PAGE_VALID,			/* page is valid and not being written */
	LRU_PAGE_WRITE_IN_PROGRESS /* page is being written out */
} LruPageStatus;

typedef struct GlobalLruSharedData
{
	LWLock		*ControlLock;	
	/*
	 * latest_page_number is the page number of the current end of the log;
	 * this is not critical data, since we use it only to avoid swapping out
	 * the latest page.
	 */
	int			latest_page_number;
}GlobalLruSharedData;

typedef struct GlobalLruSharedData * GlobalLruShared;

/*
 * Shared-memory state
 */
typedef struct LruSharedData
{
	LWLock	   *ControlLock;

	/* Number of buffers managed by this SLRU structure */
	int			num_slots;

	/*
	 * Arrays holding info for each buffer slot.  Page number is undefined
	 * when status is EMPTY, as is page_lru_count.
	 */
	char	  **page_buffer;
	LruPageStatus *page_status;
	bool	   *page_dirty;
	int		   *page_number;
	int		   *page_lru_count;
	int			latest_page_number;

	/*
	 * Optional array of WAL flush LSNs associated with entries in the SLRU
	 * pages.  If not zero/NULL, we must flush WAL before writing pages (true
	 * for pg_xact, false for multixact, pg_subtrans, pg_notify).  group_lsn[]
	 * has lsn_groups_per_page entries per buffer slot, each containing the
	 * highest LSN known for a contiguous group of SLRU entries on that slot's
	 * page.
	 */
	XLogRecPtr *group_lsn;
	int			lsn_groups_per_page;

	/*----------
	 * We mark a page "most recently used" by setting
	 *		page_lru_count[slotno] = ++cur_lru_count;
	 * The oldest page is therefore the one with the highest value of
	 *		cur_lru_count - page_lru_count[slotno]
	 * The counts will eventually wrap around, but this calculation still
	 * works as long as no page's age exceeds INT_MAX counts.
	 *----------
	 */
	int			cur_lru_count;


	/* LWLocks */
	int			lwlock_tranche_id;
	char		lwlock_tranche_name[LRU_MAX_NAME_LENGTH];
	LWLockPadded *buffer_locks;
} LruSharedData;

typedef LruSharedData *LruShared;

/*
 * SlruCtlData is an unshared structure that points to the active information
 * in shared memory.
 */
typedef struct LruCtlData
{
	GlobalLruShared global_shared;
	LruShared	shared[NUM_PARTITIONS];

	/*
	 * This flag tells whether to fsync writes (true for pg_xact and multixact
	 * stuff, false for pg_subtrans and pg_notify).
	 */
	bool		do_fsync;

    /*
     * Decide whether a page is "older" for truncation and as a hint for
     * evicting pages in LRU order.  Return true if every entry of the first
     * argument is older than every entry of the second argument.  Note that
     * !PagePrecedes(a,b) && !PagePrecedes(b,a) need not imply a==b; it also
     * arises when some entries are older and some are not.  For SLRUs using
     * SimpleLruTruncate(), this must use modular arithmetic.  (For others,
     * the behavior of this callback has no functional implications.)  Use
     * SlruPagePrecedesUnitTests() in SLRUs meeting its criteria.
     */
	bool		(*PagePrecedes) (int, int);

	/*
	 * Dir is set during SimpleLruInit and does not change thereafter. Since
	 * it's always the same, it doesn't need to be in shared memory.
	 */
	char		Dir[64];
} LruCtlData;

typedef LruCtlData *LruCtl;


#define PARTITION_LOCK_IDX(shared) ((shared)->num_slots)

extern Size LruShmemSize(int nslots, int nlsns);
extern Size
LruBufTableShmemSize(int size);
extern void LruInit(LruCtl ctl, const char *name, int nslots, int nlsns, int nentries,
			  LWLock *ctllock, const char *subdir, int tranche_id);
extern int LruZeroPage(LruCtl ctl, int partitionno, int pageno);
extern int LruReadPage(LruCtl ctl, int partitionno, int pageno, bool write_ok,
				  TransactionId xid);
extern int LruReadPage_ReadOnly(LruCtl ctl, int partitionno, int pageno,
						   TransactionId xid);
extern int
LruReadPage_ReadOnly_Locked(LruCtl ctl, int partitionno,  int pageno, bool write_ok, TransactionId xid);
extern int
LruLookupSlotno_Locked(LruCtl ctl, int partitionno, int pageno);
extern int PagenoMappingPartitionno(LruCtl ctl, int pageno);
extern LWLock * GetPartitionLock(LruCtl ctl, int partitionno);
extern void LruWritePage(LruCtl ctl, int partitionno, int slotno);
extern void LruFlush(LruCtl ctl, bool allow_redirtied);
extern void LruTruncate(LruCtl ctl, int cutoffPage);
extern bool LruDoesPhysicalPageExist(LruCtl ctl, int pageno);

typedef bool (*LruScanCallback) (LruCtl ctl, char *filename, int segpage,
								  void *data);
extern bool LruScanDirectory(LruCtl ctl, LruScanCallback callback, void *data);

/* SlruScanDirectory public callbacks */
extern bool LruScanDirCbReportPresence(LruCtl ctl, char *filename,
							int segpage, void *data);
extern bool LruScanDirCbDeleteAll(LruCtl ctl, char *filename, int segpage,
					   void *data);
extern void LruDeleteFiles(LruCtl ctl, int cutoffPage);
#ifdef _PG_REGRESS_	
bool LruTestTruncateCompress(LruCtl ctl, int pageno, int segno);
bool LruTestTruncateRename(LruCtl ctl, int pageno, int segno);
bool CheckSegmentIsExist(LruCtl ctl, int segno);
int CompareFiles(const char *filename1, const char *filename2);
bool LruBackupLogFile(LruCtl ctl, int segno);
bool LruBackupLogFile(LruCtl ctl, int segno);
void LruRestoreBackupFile(LruCtl ctl, int segno);

#endif

#endif                          /* CSN_UPGRADE */
#endif							/* SLRU_H */
