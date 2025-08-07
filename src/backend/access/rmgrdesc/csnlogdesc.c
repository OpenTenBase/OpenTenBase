/*-------------------------------------------------------------------------
 *
 * csnlogdesc.c
 *	  rmgr descriptor routines for access/transam/csnlog.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/csnlogdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csnlog.h"


void
csnlog_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == CSNLOG_ZEROPAGE)
	{
		int			pageno;

		memcpy(&pageno, rec, sizeof(int));
		appendStringInfo(buf, "page %d", pageno);
	}
	else if (info == CSNLOG_TRUNCATE)
	{
		xl_csnlog_truncate xlrec;

		memcpy(&xlrec, rec, sizeof(xl_csnlog_truncate));
		appendStringInfo(buf, "page %d; oldestXact %u",
						 xlrec.pageno, xlrec.oldestXact);
	}
}

const char *
csnlog_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case CSNLOG_ZEROPAGE:
			id = "ZEROPAGE";
			break;
		case CSNLOG_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
