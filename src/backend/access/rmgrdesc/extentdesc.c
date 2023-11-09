/*-------------------------------------------------------------------------
 *
 * heapdesc.c
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * IDENTIFICATION
 *      src/backend/access/rmgrdesc/heapdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/extentmapping.h"
#include "storage/extent_xlog.h"

static void extent_desc_seteob(StringInfo buf, char *record);
static void extent_desc_extendeob(StringInfo buf, char *record);
static void extent_desc_seteme(StringInfo buf, char *record);
static void extent_desc_extendeme(StringInfo buf, char *record);
static void extent_desc_initeme(StringInfo buf, char *record);
static void extent_desc_setesa(StringInfo buf, char *record);
static void extent_desc_cleaneme(StringInfo buf, char *record);
static void extent_desc_truncate(StringInfo buf, char *record);
static void extent_desc_trunceob(StringInfo buf, char *record);
static void extent_desc_cleaneob(StringInfo buf, char *record);
static void extent_desc_truncema(StringInfo buf, char *record);
static void extent_desc_cleanema(StringInfo buf, char *record);

typedef struct extent_xlog_frag_ele
{
    int        size;
    EmaPageType    page_type;
    void    (*redo_fn)(Page pg, char *record);
    void    (*desc_fn)(StringInfo buf, char *record);
}extent_xlog_frag_ele;

static const extent_xlog_frag_ele e_redomgr[] = {
    {0,EmaPageType_EOB, NULL, NULL}, /*0 is not used */
    {SizeOfSetEOB, EmaPageType_EOB, NULL, extent_desc_seteob},
    {SizeOfExtendEOB, EmaPageType_EOB, NULL, extent_desc_extendeob},
    {SizeOfSetEME, EmaPageType_EMA, NULL, extent_desc_seteme},
    {SizeOfExtendEME, EmaPageType_EMA, NULL, extent_desc_extendeme},
    {SizeOfInitEME, EmaPageType_EMA, NULL, extent_desc_initeme},
    {SizeOfCleanEME, EmaPageType_EMA, NULL, extent_desc_cleaneme},
    {SizeOfSetESA, EmaPageType_ESA, NULL, extent_desc_setesa},
    {SizeOfTruncateExtentSeg, EmaPageType_EOB, NULL, extent_desc_truncate},
    {SizeOfTruncEOB, EmaPageType_NONE, NULL, extent_desc_trunceob},
    {SizeOfCleanEOB, EmaPageType_NONE, NULL, extent_desc_cleaneob},
    {SizeOfTruncEMA, EmaPageType_NONE, NULL, extent_desc_truncema},
    {SizeOfCleanEMA, EmaPageType_NONE, NULL, extent_desc_cleanema}
};

void
extent_desc(StringInfo buf, XLogReaderState *record)
{// #lizard forgives
    int block_idx = 0;

    if(record->main_data_len > 0)
    {
        char *main_xlog_cursor = NULL;
        char *main_xlog_buf = NULL;

        main_xlog_cursor = record->main_data;
        while(main_xlog_cursor - record->main_data < record->main_data_len)
        {
            int8     xlogtag;
            memcpy(&xlogtag, main_xlog_cursor, sizeof(int8));
            //xlog_cursor += sizeof(int8);

            main_xlog_buf = palloc(e_redomgr[xlogtag].size);
            memcpy(main_xlog_buf, main_xlog_cursor, e_redomgr[xlogtag].size);
            main_xlog_cursor += e_redomgr[xlogtag].size;

            if(main_xlog_cursor - record->main_data > record->main_data_len)
            {
                appendStringInfo(buf, "\nextent xlog of main data is invalid. %d bytes is expected, but only %d bytes remained.",
                            e_redomgr[xlogtag].size, (int32)(record->main_data_len - (main_xlog_cursor - record->main_data)));
                break;
            }

            if(xlogtag > FragTag_EXTENT_XLOG_CLEANEMA || xlogtag <= 0)
            {
                appendStringInfo(buf, "extent xlog tag of main data is invalid:%d", xlogtag);
                break;
            }
            
            /* desc extent xlog record */
            appendStringInfo(buf, "\n\t\t");
            e_redomgr[xlogtag].desc_fn(buf, main_xlog_buf);
            pfree(main_xlog_buf);
            main_xlog_buf = NULL;
        }
    }
    
    for(block_idx = 0; block_idx <= record->max_block_id; block_idx++)
    {        
        /* read record */
        DecodedBkpBlock *blk = &record->blocks[block_idx];
        char *xlog_cursor = NULL;
        char *xlog_buf = NULL;
        
        if(!blk->in_use)
            continue;
        if(!blk->has_data)
            continue;
#if 0
        if(blk->forknum != EXTENT_FORKNUM)
        {
            elog(ERROR, "forknum %d is not extent fork.", blk->forknum);
        }
#endif
        appendStringInfo(buf, "\n\tblk %u", blk->blkno);
        xlog_cursor = blk->data;

        while(xlog_cursor - blk->data < blk->data_len)
        {
            int8     xlogtag;
            bool    pgtype_invalid = false;
            memcpy(&xlogtag, xlog_cursor, sizeof(int8));
            //xlog_cursor += sizeof(int8);

            xlog_buf = palloc(e_redomgr[xlogtag].size);
            memcpy(xlog_buf, xlog_cursor, e_redomgr[xlogtag].size);
            xlog_cursor += e_redomgr[xlogtag].size;

            if(xlog_cursor - blk->data > blk->data_len)
            {
                appendStringInfo(buf, "\nextent xlog is invalid. %d bytes is expected, but only %d bytes remained.",
                            e_redomgr[xlogtag].size, (int32)(blk->data_len - (xlog_cursor - blk->data) ));
                break;
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
                case EmaPageType_NONE:
                    break;
            }
            if(pgtype_invalid)
            {
                appendStringInfo(buf, "\n\textent xlog is applied to a wrong extent page. "
                            "extent xlogtype:%d, block number:%d",
                            xlogtag, blk->blkno);
                break;
            }

            /* desc extent xlog record */
            appendStringInfo(buf, "\n\t\t");
            e_redomgr[xlogtag].desc_fn(buf, xlog_buf);
            pfree(xlog_buf);
            xlog_buf = NULL;
        }
    }
}

static void extent_desc_seteob(StringInfo buf, char *record)
{
    xl_extent_seteob *xlogrec = (xl_extent_seteob *)record;

    appendStringInfo(buf, " [seteob]slot %u", xlogrec->slot);
}

static void extent_desc_extendeob(StringInfo buf, char *record)
{
    xl_extent_extendeob *xlogrec = (xl_extent_extendeob *)record;
    appendStringInfo(buf, " [extenteob]slot %u, n_bits %u", xlogrec->slot, xlogrec->n_eobs);
    
    if(xlogrec->flags & EXTEND_EOB_FLAGS_SETFREE)
    {
        appendStringInfo(buf, " [setfree]from %u, to %u", xlogrec->setfree_start, xlogrec->setfree_end);
    }
}

static void extent_desc_seteme(StringInfo buf, char *record)
{// #lizard forgives
    xl_extent_seteme *xlogrec = (xl_extent_seteme *)record;

    appendStringInfo(buf, " [seteme]slot %u", xlogrec->slot);

    if((xlogrec->setflag & EMA_SETFLAG_SHARDID) != 0)
        appendStringInfo(buf, ",sid %u", xlogrec->eme.shardid);
    if((xlogrec->setflag & EMA_SETFLAG_FREESPACE) != 0)
        appendStringInfo(buf, ",freespace %u", xlogrec->eme.max_freespace);
    if((xlogrec->setflag & EMA_SETFLAG_HWM) != 0)
        appendStringInfo(buf, ",hwm %u", xlogrec->eme.hwm);
    if((xlogrec->setflag & EMA_SETFLAG_OCCUPIED) != 0)
        appendStringInfo(buf, ",is_occupied %d", xlogrec->eme.is_occupied);
    if((xlogrec->setflag & EMA_SETFLAG_SCANPREV) != 0)
        appendStringInfo(buf, ",scanprev %u", xlogrec->eme.scan_prev);
    if((xlogrec->setflag & EMA_SETFLAG_SCANNEXT) != 0)
        appendStringInfo(buf, ",scannext %u", xlogrec->eme.scan_next);
    if((xlogrec->setflag & EMA_SETFLAG_ALLOCPREV) != 0)
        appendStringInfo(buf, ",allocprev %u", EMEGetAllocPrev(&xlogrec->eme));
    if((xlogrec->setflag & EMA_SETFLAG_ALLOCNEXT) != 0)
        appendStringInfo(buf, ",allocnext %u", xlogrec->eme.alloc_next);

    if(xlogrec->setflag == EMA_SETFLAG_INIT)
        appendStringInfo(buf, ",initeme sid %u", xlogrec->eme.shardid);

    if(xlogrec->setflag == EMA_SETFLAG_CLEAN)
        appendStringInfo(buf, ",clean");

    if(xlogrec->setflag & EMA_SETFLAG_EXTENDHEAP)
    {
        appendStringInfo(buf, ",extend heap:rnode %d/%d/%d eid=%d",
            xlogrec->rnode.dbNode, xlogrec->rnode.spcNode, xlogrec->rnode.relNode,
            xlogrec->extentid);
    }
}

static void extent_desc_extendeme(StringInfo buf, char *record)
{    
    xl_extent_extendeme *xlogrec = (xl_extent_extendeme *)record;
    appendStringInfo(buf, " [extendeme]n_emes: %d, flags: %x", xlogrec->n_emes, xlogrec->flags);
    if(xlogrec->flags & EXTEND_EME_FLAGS_SETFREE)
    {
        appendStringInfo(buf, " [cleanpointer]from %u, to %u", xlogrec->setfree_start, xlogrec->setfree_end);
    }
}

static void extent_desc_initeme(StringInfo buf, char *record)
{
    xl_extent_initeme *xlogrec = (xl_extent_initeme *)record;
    appendStringInfo(buf, " [initeme]slot %u", xlogrec->slot);
}

static void extent_desc_cleaneme(StringInfo buf, char *record)
{
    xl_extent_cleaneme *xlogrec = (xl_extent_cleaneme *)record;
    appendStringInfo(buf, " [cleaneme]slot %u", xlogrec->slot);
}

static void extent_desc_setesa(StringInfo buf, char *record)
{
    xl_extent_setesa *xlogrec = (xl_extent_setesa *)record;
    appendStringInfo(buf, " [setesa]slot %u", xlogrec->slot);

    if((xlogrec->setflag & ESA_SETFLAG_SCANHEAD) != 0)
        appendStringInfo(buf, ",scanhead %u", xlogrec->anchor.scan_head);
    if((xlogrec->setflag & ESA_SETFLAG_SCANTAIL) != 0)
        appendStringInfo(buf, ",scantail %u", xlogrec->anchor.scan_tail);
    if((xlogrec->setflag & ESA_SETFLAG_ALLOCHEAD) != 0)
        appendStringInfo(buf, ",allochead %u", xlogrec->anchor.alloc_head);
    if((xlogrec->setflag & ESA_SETFLAG_ALLOCTAIL) != 0)
        appendStringInfo(buf, ",alloctail %u", xlogrec->anchor.alloc_tail);
}

static void extent_desc_truncate(StringInfo buf, char *record)
{
    xl_extent_truncate *xlogrec = (xl_extent_truncate *)record;
    appendStringInfo(buf, " [truncate extent file]%d/%d/%d", 
                        xlogrec->rnode.dbNode, 
                        xlogrec->rnode.spcNode, 
                        xlogrec->rnode.relNode);
}

static void extent_desc_trunceob(StringInfo buf, char *record)
{
    xl_extent_trunceob *xlogrec = (xl_extent_trunceob *)record;
    appendStringInfo(buf, " [truncate eob page]%d/%d/%d, pageno:%d, offset:%d", 
                        xlogrec->rnode.dbNode, 
                        xlogrec->rnode.spcNode, 
                        xlogrec->rnode.relNode,
                        xlogrec->pageno,
                        xlogrec->offset);
}
static void extent_desc_cleaneob(StringInfo buf, char *record)
{
    xl_extent_cleaneob *xlogrec = (xl_extent_cleaneob *)record;
    appendStringInfo(buf, " [clean eob page]%d/%d/%d, pageno:%d", 
                        xlogrec->rnode.dbNode, 
                        xlogrec->rnode.spcNode, 
                        xlogrec->rnode.relNode,
                        xlogrec->pageno);
}

static void extent_desc_truncema(StringInfo buf, char *record)
{
    xl_extent_truncema *xlogrec = (xl_extent_truncema *)record;
    appendStringInfo(buf, " [truncate ema page]%d/%d/%d, pageno:%d, offset:%d", 
                        xlogrec->rnode.dbNode, 
                        xlogrec->rnode.spcNode, 
                        xlogrec->rnode.relNode,
                        xlogrec->pageno,
                        xlogrec->offset);
}
static void extent_desc_cleanema(StringInfo buf, char *record)
{
    xl_extent_cleanema *xlogrec = (xl_extent_cleanema *)record;
    appendStringInfo(buf, " [clean ema page]%d/%d/%d, pageno:%d", 
                        xlogrec->rnode.dbNode, 
                        xlogrec->rnode.spcNode, 
                        xlogrec->rnode.relNode,
                        xlogrec->pageno);
}


const char *
extent_identify(uint8 info)
{// #lizard forgives
    switch (info & XLOG_EXTENT_OPMASK)
    {
        case XLOG_EXTENT_NEW_EXTENT:
            return "New Extent";
        case XLOG_EXTENT_UPDATE_EME:
            return "Update EME";
        case XLOG_EXTENT_APPEND_EXTENT:
            return "Append Extent";
        case XLOG_EXTENT_ATTACH_EXTENT:    
            return "Attach Extent";
        case XLOG_EXTENT_DETACH_EXTENT:
            return "Detach Extent";
        case XLOG_EXTENT_MAKE_FULL:
            return "Make Extent Full";
        case XLOG_EXTENT_MAKE_AVAIL:
            return "Make Extent Available";
        case XLOG_EXTENT_FREE_DISK:
            return "Free Extent Data";
        case XLOG_EXTENT_TRUNCATE:
            return "Truncate Extent File Tile";
        case XLOG_EXTENT_COMMON:
            return "Common Extent Ops";
        case XLOG_EXTENT_EXTEND:
            return "Extend Extents";
        default:
            return "Extent ERROR";
    }
}


