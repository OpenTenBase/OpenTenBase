/*-------------------------------------------------------------------------
 *
 * extent_xlog.c
 *	  routines to search and manipulate one FSM page.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/freespace/fsmpage.c
 *
 */
#include "postgres.h"
#include "miscadmin.h"
#include "access/hio.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "nodes/bitmapset.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"
#include "storage/block.h"
#include "storage/extentmapping.h"
#include "storage/extent_xlog.h"
#include "storage/smgr.h"

static void extent_xlog_apply_record(XLogReaderState *record);
static void extent_xlog_apply_truncate(XLogReaderState *record);
static void extent_xlog_apply_common(XLogReaderState *record);
static void extent_redo_seteob(Page pg, char *record);
static void extent_redo_extendeob(Page pg, char *record);
static void extent_redo_seteme(Page pg, char *record);
static void extent_redo_extendeme(Page pg, char *record);
static void extent_redo_initeme(Page pg, char *record);
static void extent_redo_cleaneme(Page pg, char *record);
static void extent_redo_setesa(Page pg, char *record);
static void extent_redo_trunceob(Page pg, char *record);
static void extent_redo_cleaneob(Page pg, char *record);
static void extent_redo_truncema(Page pg, char *record);
static void extent_redo_cleanema(Page pg, char *record);

static void extend_heap(RelFileNode rnode, xl_extent_seteme *xlogrec);

typedef struct extent_xlog_frag_ele
{
	int		size;
	EmaPageType	page_type;
	void	(*redo_fn)(Page pg, char *record);
	void	(*desc_fn)(StringInfo buf, char *record);
}extent_xlog_frag_ele;

static const extent_xlog_frag_ele e_redomgr[] = {
	{0,EmaPageType_NONE,NULL,NULL},	/* 0 is not used */
	{SizeOfSetEOB, EmaPageType_EOB, extent_redo_seteob, NULL},
	{SizeOfExtendEOB, EmaPageType_EOB, extent_redo_extendeob, NULL},
	{SizeOfSetEME, EmaPageType_EMA, extent_redo_seteme, NULL},
	{SizeOfExtendEME, EmaPageType_EMA, extent_redo_extendeme, NULL},
	{SizeOfInitEME, EmaPageType_EMA, extent_redo_initeme, NULL},
	{SizeOfCleanEME, EmaPageType_EMA, extent_redo_cleaneme, NULL},
	{SizeOfSetESA, EmaPageType_ESA, extent_redo_setesa, NULL},
	{SizeOfTruncateExtentSeg, EmaPageType_NONE, NULL, NULL},
	{SizeOfTruncEOB, EmaPageType_NONE, extent_redo_trunceob, NULL},
	{SizeOfCleanEOB, EmaPageType_NONE, extent_redo_cleaneob, NULL},
	{SizeOfTruncEMA, EmaPageType_NONE, extent_redo_truncema, NULL},
	{SizeOfCleanEMA, EmaPageType_NONE, extent_redo_cleanema, NULL}
};

void
extent_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_EXTENT_OPMASK)
	{
		case XLOG_EXTENT_NEW_EXTENT:
		case XLOG_EXTENT_UPDATE_EME:
		case XLOG_EXTENT_APPEND_EXTENT:	
		case XLOG_EXTENT_ATTACH_EXTENT:	
		case XLOG_EXTENT_DETACH_EXTENT:	
		case XLOG_EXTENT_MAKE_FULL:		
		case XLOG_EXTENT_MAKE_AVAIL:
		case XLOG_EXTENT_FREE_DISK:
		case XLOG_EXTENT_EXTEND:
			extent_xlog_apply_record(record);
			break;
		case XLOG_EXTENT_TRUNCATE:
			extent_xlog_apply_truncate(record);
			break;
		case XLOG_EXTENT_COMMON:
			extent_xlog_apply_common(record);
		default:
			break;
	}
}

#define MAXLEN_XLOG_RECORD 128
static void
extent_xlog_apply_record(XLogReaderState *record)
{
	int block_idx = 0;
	Buffer	*bufs;
	RelFileNode rnode;
	char xlog_buf[MAXLEN_XLOG_RECORD];

	DecodedBkpBlock *blk = NULL;

	bufs = (Buffer *)palloc((record->max_block_id + 1) * sizeof(Buffer));
	/*
	 * lock buffer
	 */
	for(block_idx=0; block_idx <= record->max_block_id; block_idx++)
	{		
		blk = &record->blocks[block_idx];
		if(!blk->in_use || !blk->has_data)
		{
			bufs[block_idx] = InvalidBuffer;
			continue;
		}
		
		memcpy(&rnode, &(blk->rnode), sizeof(rnode));
		//XLogReadBufferForRedoExtended(record, block_idx, RBM_ZERO_AND_LOCK, false, &bufs[block_idx]);
		//bufs[block_idx] = XLogReadBufferExtended(rnode, EXTENT_FORKNUM, blk->blkno, RBM_NORMAL);
		bufs[block_idx] = extent_readbuffer_for_redo(blk->rnode, blk->blkno, true, record->checksum_enabled);
		
		if(BufferIsInvalid(bufs[block_idx]))
		{
			elog(WARNING, "read extent page error when redo extent xlog.rel:%d/%d/%d, blkno:%d",
							blk->rnode.dbNode, blk->rnode.spcNode, blk->rnode.relNode, blk->blkno);
		}
		
		LockBuffer(bufs[block_idx], BUFFER_LOCK_EXCLUSIVE);

		if (PageIsNew(BufferGetPage(bufs[block_idx])))
		{
			EmaPageType pagetype;
			int 		max_eles;

			if(blk->blkno < ESAPAGE_OFFSET - 1)
			{
				pagetype = EmaPageType_EOB;
				max_eles = EOBS_PER_PAGE;
			}
			else if(blk->blkno == ESAPAGE_OFFSET - 1)
			{
				pagetype = EmaPageType_EOB;
				max_eles = MAX_EXTENTS % EOBS_PER_PAGE;
			}
			else if(blk->blkno < EMAPAGE_OFFSET - 1)
			{
				pagetype = EmaPageType_ESA;
				max_eles = ESAS_PER_PAGE;
			}
			else if(blk->blkno == EMAPAGE_OFFSET - 1)
			{
				pagetype = EmaPageType_ESA;
				max_eles = MAX_EXTENTS % ESAS_PER_PAGE;
			}
			else if(blk->blkno < EMA_FORK_BLOCKS - 1)
			{
				pagetype = EmaPageType_EMA;
				max_eles = EMES_PER_PAGE;
			}
			else if(blk->blkno == EMA_FORK_BLOCKS - 1)
			{
				pagetype = EmaPageType_EMA;
				max_eles = MAX_EXTENTS % EMES_PER_PAGE;
			}	
			else /* (blkno >= EMA_FORK_BLOCKS) */
			{
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("block number too larger"),
						 errdetail("ema page number cannot be greater than %d",
								   EMA_FORK_BLOCKS - 1)));
			}
	
			PageInit_shard(BufferGetPage(bufs[block_idx]), BLCKSZ, 0, InvalidShardID, true, record->checksum_enabled);
			switch(pagetype)
			{
				case EmaPageType_EOB:
					eob_init_page(BufferGetPage(bufs[block_idx]), max_eles);
					break;
				case EmaPageType_ESA:
					esa_init_page(BufferGetPage(bufs[block_idx]), max_eles);
					break;
				case EmaPageType_EMA:
					ema_init_page(BufferGetPage(bufs[block_idx]), max_eles);
					break;
				default:
					elog(PANIC, "page type %d is not supported.", pagetype);
					break;
			}
		}
	}

	START_CRIT_SECTION();
	/*
	 * redo record
	 */
	for(block_idx=0; block_idx <= record->max_block_id; block_idx++)
	{		
		/* read record */
		DecodedBkpBlock *blk = &record->blocks[block_idx];
		char *xlog_cursor = NULL;
		Page pg = NULL;
		
		if(!blk->in_use)
			continue;
		if(!blk->has_data)
			continue;

		if(blk->forknum != EXTENT_FORKNUM)
		{
			elog(ERROR, "forknum %d is not extent fork.", blk->forknum);
		}
		
		pg = BufferGetPage(bufs[block_idx]);

		xlog_cursor = blk->data;

		while(xlog_cursor - blk->data < blk->data_len)
		{
			int8 	xlogtag;
			bool	pgtype_invalid = false;
			memcpy(&xlogtag, xlog_cursor, sizeof(int8));
			
			switch(xlogtag)
			{
				case FragTag_EXTENT_XLOG_SETEOB:
				case FragTag_EXTENT_XLOG_EXTENDEOB:
				case FragTag_EXTENT_XLOG_SETEME:
				case FragTag_EXTENT_XLOG_EXTENDEME:
				case FragTag_EXTENT_XLOG_INITEME:
				case FragTag_EXTENT_XLOG_CLEANEME:
				case FragTag_EXTENT_XLOG_SETESA:
					break;
				default:
					elog(ERROR, "unrecognized extent xlog:%d", xlogtag);
					break;
			}

			//xlog_buf = palloc(e_redomgr[xlogtag].size);
			MemSet(xlog_buf, 0, MAXLEN_XLOG_RECORD);
			memcpy(xlog_buf, xlog_cursor, e_redomgr[xlogtag].size);
			xlog_cursor += e_redomgr[xlogtag].size;

			if(xlog_cursor - blk->data > blk->data_len)
			{
				elog(ERROR, "extent xlog is invalid. %d bytes is expected, but only %d bytes remained.",
							e_redomgr[xlogtag].size, (int32)(xlog_cursor - blk->data - blk->data_len));
			}
			/*
			 * validate page type
			 */
			switch(e_redomgr[xlogtag].page_type)
			{
				case EmaPageType_EOB:
					if(blk->blkno >= ESAPAGE_OFFSET)
						pgtype_invalid = true;
					break;
				case EmaPageType_ESA:
					if(blk->blkno < ESAPAGE_OFFSET || blk->blkno >= EMAPAGE_OFFSET)
						pgtype_invalid = true;
					break;
				case EmaPageType_EMA:
					if(blk->blkno >= EMA_FORK_BLOCKS)
						pgtype_invalid = true;
					break;
				default:
					break;
			}
			if(pgtype_invalid)
			{
				elog(ERROR, "extent xlog is applied to a wrong extent page. "
							"extent xlogtype:%d, block number:%d",
							xlogtag, blk->blkno);
			}

			/* redo xlog */
			e_redomgr[xlogtag].redo_fn(pg, xlog_buf);
		}
	}

	/*
	 * release resource
	 */

	for(block_idx=0; block_idx <= record->max_block_id; block_idx++)
	{		
		if(BufferIsValid(bufs[block_idx]))
		{
			MarkBufferDirty(bufs[block_idx]);
			UnlockReleaseBuffer(bufs[block_idx]);
		}
	}

	END_CRIT_SECTION();
}

static void
extent_xlog_apply_truncate(XLogReaderState *record)
{
	xl_extent_truncate *xlrec = (xl_extent_truncate *) XLogRecGetData(record);
	SMgrRelation reln;
	reln = smgropen(xlrec->rnode, InvalidBackendId);
	smgrdounlinkfork(reln, EXTENT_FORKNUM, true);
	smgrclose(reln);
}

static void
extent_xlog_apply_common(XLogReaderState *record)
{
	uint8 xlogtag;
	char xlog_buf[MAXLEN_XLOG_RECORD];
	char *xlog_cursor = XLogRecGetData(record);
	int len = XLogRecGetDataLen(record);

	while(xlog_cursor - XLogRecGetData(record) < len)
	{
		memcpy(&xlogtag, xlog_cursor, sizeof(xlogtag));

		if(xlog_cursor + e_redomgr[xlogtag].size - XLogRecGetData(record) > len)
		{
			elog(ERROR, "invalid xlog record length for extent common: logtag: %d, wanted %d, get %ld.",
					xlogtag,
					e_redomgr[xlogtag].size,
					len - (xlog_cursor - XLogRecGetData(record)));
		}
		MemSet(xlog_buf, 0, sizeof(xlog_buf));
		memcpy(xlog_buf, xlog_cursor, e_redomgr[xlogtag].size);
		xlog_cursor += e_redomgr[xlogtag].size;

		e_redomgr[xlogtag].redo_fn(NULL, xlog_buf);
	}
}

static void extent_redo_seteob(Page pg, char *record)
{
	EOBPage eob_pg = (EOBPage)PageGetContents(pg);
	xl_extent_seteob *xlogrec = (xl_extent_seteob *)record;

	eob_page_mark_extent(eob_pg, xlogrec->slot, xlogrec->setfree);
}

static void extent_redo_extendeob(Page pg, char *record)
{
	int bits;
	EOBPage eob_pg = (EOBPage)PageGetContents(pg);
	xl_extent_extendeob *xlogrec = (xl_extent_extendeob *)record;

	if(xlogrec->flags & EXTEND_EOB_FLAGS_SETFREE)
	{
		for(bits = xlogrec->setfree_start; bits <= xlogrec->setfree_end; bits++)
			bms_add_member(&eob_pg->eob_bits, bits);	
	}
	eob_pg->n_bits = xlogrec->n_eobs;
}

static void extent_redo_seteme(Page pg, char *record)
{
	xl_extent_seteme *xlogrec = (xl_extent_seteme *)record;

	ema_page_set_eme(pg, (int32)xlogrec->slot, xlogrec->setflag, &xlogrec->eme);

	if(xlogrec->setflag & EMA_SETFLAG_EXTENDHEAP)
		extend_heap(xlogrec->rnode, xlogrec);
}

static void extent_redo_extendeme(Page pg, char *record)
{
	xl_extent_extendeme *xlogrec = (xl_extent_extendeme *)record;

	if(xlogrec->flags & EXTEND_EME_FLAGS_SETFREE)
		ema_page_extend_eme(pg, xlogrec->n_emes, 
							true, xlogrec->setfree_start, xlogrec->setfree_end);
	else
		ema_page_extend_eme(pg, xlogrec->n_emes, false, -1, -1);
}

static void extent_redo_initeme(Page pg, char *record)
{
	xl_extent_initeme *xlogrec = (xl_extent_initeme *)record;

	ema_page_init_eme(pg, (int32)xlogrec->slot,xlogrec->shardid, xlogrec->freespace);
}

static void extent_redo_cleaneme(Page pg, char *record)
{
	xl_extent_cleaneme *xlogrec = (xl_extent_cleaneme *)record;

	ema_page_free_eme(pg, (int32)xlogrec->slot);
}

static void extent_redo_setesa(Page pg, char *record)
{
	xl_extent_setesa *xlogrec = (xl_extent_setesa *)record;
	esa_page_set_anchor(pg, (int32)xlogrec->slot, xlogrec->setflag,
				xlogrec->anchor.scan_head, xlogrec->anchor.scan_tail,
				xlogrec->anchor.alloc_head, xlogrec->anchor.alloc_tail);
}

static void extent_redo_trunceob(Page pg, char *record)
{
	xl_extent_trunceob *xlogrec = (xl_extent_trunceob*)record;

	Buffer buf = XLogReadBufferExtended(xlogrec->rnode, 
							EXTENT_FORKNUM,
							xlogrec->pageno,
							RBM_NORMAL,
							false);

	if(BufferIsInvalid(buf))
	{
		elog(WARNING, "truncate eobpage: read pageno %d of extent in relation %d/%d failed.",
				xlogrec->pageno, xlogrec->rnode.dbNode, xlogrec->rnode.relNode);
	}
	
	if(BufferIsValid(buf))
		eob_truncate_page(BufferGetEOBPage(buf), xlogrec->offset);

	ReleaseBuffer(buf);
}

static void extent_redo_cleaneob(Page pg, char *record)
{
	xl_extent_cleaneob *xlogrec = (xl_extent_cleaneob*)record;

	Buffer buf = XLogReadBufferExtended(xlogrec->rnode, 
							EXTENT_FORKNUM,
							xlogrec->pageno,
							RBM_NORMAL,
							false);

	if(BufferIsInvalid(buf))
	{
		elog(WARNING, "clean eobpage: read pageno %d of extent in relation %d/%d failed.",
				xlogrec->pageno, xlogrec->rnode.dbNode, xlogrec->rnode.relNode);
	}
	
	if(BufferIsValid(buf))
		eob_clean_page(BufferGetEOBPage(buf));

	ReleaseBuffer(buf);
}

static void extent_redo_truncema(Page pg, char *record)
{
	xl_extent_truncema *xlogrec = (xl_extent_truncema*)record;

	Buffer buf = XLogReadBufferExtended(xlogrec->rnode, 
							EXTENT_FORKNUM,
							xlogrec->pageno,
							RBM_NORMAL,
							false);

	if(BufferIsInvalid(buf))
	{
		elog(WARNING, "truncate emapage: read pageno %d of extent in relation %d/%d failed.",
				xlogrec->pageno, xlogrec->rnode.dbNode, xlogrec->rnode.relNode);
	}
	
	if(BufferIsValid(buf))
		ema_truncate_page(BufferGetEMAPage(buf), xlogrec->offset);

	ReleaseBuffer(buf);
}

static void extent_redo_cleanema(Page pg, char *record)
{
	xl_extent_cleanema *xlogrec = (xl_extent_cleanema*)record;

	Buffer buf = XLogReadBufferExtended(xlogrec->rnode, 
							EXTENT_FORKNUM,
							xlogrec->pageno,
							RBM_NORMAL,
							false);

	if(BufferIsInvalid(buf))
	{
		elog(WARNING, "clean emapage: read pageno %d of extent in relation %d/%d failed.",
				xlogrec->pageno, xlogrec->rnode.dbNode, xlogrec->rnode.relNode);
	}
	
	if(BufferIsValid(buf))
		ema_clean_page(BufferGetEMAPage(buf));

	ReleaseBuffer(buf);
}

static void extend_heap(RelFileNode rnode, xl_extent_seteme *xlogrec)
{	
	if(!(xlogrec->setflag & EMA_SETFLAG_EXTENDHEAP))
		return;
	
	if(!ExtentIdIsValid(xlogrec->extentid))
	{
		elog(FATAL, 
			"xlog is invalid. [xl_extent_seteme] has flag to extend heap, but extentid %d is invalid.",
			xlogrec->extentid);
	}

	if(!(xlogrec->setflag & EMA_SETFLAG_SHARDID) || !ShardIDIsValid(xlogrec->eme.shardid))
	{
		elog(FATAL,
			"xlog is invalid. [xl_extent_seteme] has flag to extend heap, but shardid invalid.");
	}

	RelationExtendHeapForRedo(rnode, xlogrec->extentid, xlogrec->eme.shardid, xlogrec->checksum_enabled);
}


