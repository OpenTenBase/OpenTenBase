/*-------------------------------------------------------------------------
 *
 * replslotdesc.c
 *    rmgr descriptor routines for replication/slot.c
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * IDENTIFICATION
 *    src/backend/access/rmgrdesc/replslotdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/replslotdesc.h"

void replication_slot_desc(StringInfo buf, XLogReaderState *record)
{
    char       *rec = XLogRecGetData(record);
    uint8       info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info)
    {
        case XLOG_REPLORIGIN_SLOT_LSN_REPLICA:
        {
            xl_replication_slot_lsn_replica *xlrec;

            xlrec = (xl_replication_slot_lsn_replica *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, xmin:%u, catalog_xmin:%u, restart_lsn %X/%X, confirmed_flush %X/%X",
                             xlrec->slotid,
                             xlrec->xmin,
                             xlrec->catalog_xmin,
                             (uint32) (xlrec->restart_lsn >> 32),
                             (uint32) xlrec->restart_lsn,
                             (uint32) (xlrec->confirmed_flush >> 32),
                             (uint32) xlrec->confirmed_flush);
            break;
        }
        case XLOG_REPLORIGIN_SLOT_CREATE:
        {
            xl_replication_slot_create *xlrec;

            xlrec = (xl_replication_slot_create *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, name:%s, database:%u, persistency:%d, xmin:%u, catalog_xmin:%u, "
                            "restart_lsn:%X/%X, confirmed_flush:%X/%X, effective_xmin:%d, effective_catalog_xmin:%d, "
                            "pgoutput:%d, subid:%u, subname:%s, relid:%u", 
                            xlrec->slotid,
                            NameStr(xlrec->slotname),
                            xlrec->database,
                            xlrec->persistency, xlrec->xmin, xlrec->catalog_xmin,
                            (uint32) (xlrec->restart_lsn >> 32), (uint32) xlrec->restart_lsn,
                            (uint32) (xlrec->confirmed_flush >> 32), (uint32) xlrec->confirmed_flush,
                            xlrec->effective_xmin, xlrec->effective_catalog_xmin,
                            xlrec->pgoutput, xlrec->subid, NameStr(xlrec->subname), xlrec->relid);
            break;
        }
        case XLOG_REPLORIGIN_SLOT_DROP:
        {
            xl_replication_slot_drop *xlrec;

            xlrec = (xl_replication_slot_drop *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, name:%s", 
                            xlrec->slotid,
                            NameStr(xlrec->slotname));
            break;
        }
        case XLOG_REPLORIGIN_SLOT_RENAME:
        {
            xl_replication_slot_rename *xlrec;

            xlrec = (xl_replication_slot_rename *) rec;

            appendStringInfo(buf, "info in xlrec, id:%d, old_name:%s, new_name:%s",
                             xlrec->slotid,
                             NameStr(xlrec->old_slotname),
                             NameStr(xlrec->new_slotname));
            break;
        }
        default:
            break;
    }
    return;
}

const char * replication_slot_identify(uint8 info)
{
    switch (info)
    {
        case XLOG_REPLORIGIN_SLOT_LSN_REPLICA:
            return "REPLORIGIN_SLOT_LSN_REPLICA";
        case XLOG_REPLORIGIN_SLOT_CREATE:
            return "REPLORIGIN_SLOT_CREATE";
        case XLOG_REPLORIGIN_SLOT_DROP:
            return "REPLORIGIN_SLOT_DROP";
        case XLOG_REPLORIGIN_SLOT_RENAME:
            return "REPLORIGIN_SLOT_RENAME";
        default:
            break;
    }
    return NULL;
}
