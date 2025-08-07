/*-------------------------------------------------------------------------
 *
 * smgrdesc.c
 *	  rmgr descriptor routines for catalog/storage.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/smgrdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catalog.h"
#include "catalog/storage_xlog.h"


void
smgr_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) rec;
		char	   *path = relpathperm_client(xlrec->rnode, xlrec->forkNum, "");

		appendStringInfoString(buf, path);
		pfree(path);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) rec;
		char	   *path = relpathperm_client(xlrec->rnode, MAIN_FORKNUM, "");

		appendStringInfo(buf, "%s to %u blocks flags %d", path,
						 xlrec->blkno, xlrec->flags);
		pfree(path);
	}
#ifdef _SHARDING_
	else if (info == XLOG_SMGR_DEALLOC)
	{
		xl_smgr_dealloc *xlrec = (xl_smgr_dealloc *)rec;
		char	   *path = relpathperm_client(xlrec->rnode, MAIN_FORKNUM, "");

		appendStringInfo(buf, "%s flags:%d,eid:%d, from %u to %u blocks", 
						path, xlrec->flags, xlrec->eid, 
						xlrec->eid * PAGES_PER_EXTENTS, (xlrec->eid + 1) * PAGES_PER_EXTENTS -1);
		pfree(path);
	}
#endif
}

const char *
smgr_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SMGR_CREATE:
			id = "CREATE";
			break;
		case XLOG_SMGR_TRUNCATE:
			id = "TRUNCATE";
			break;
#ifdef _SHARDING_
		case XLOG_SMGR_DEALLOC:
			id = "DEALLOC";
			break;
#endif
	}

	return id;
}
