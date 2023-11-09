/*-------------------------------------------------------------------------
 *
 * relcryptdesc.c
 *    rmgr descriptor routines for utils/cache/relcrypt.c
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * IDENTIFICATION
 *    src/backend/access/rmgrdesc/relcryptdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/relcrypt.h"
#include "utils/relcryptmap.h"
#include "access/xlogrecord.h"

void rel_crypt_desc(StringInfo buf, XLogReaderState *record)
{
    uint8                info;

    info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch(info & XLR_RMGR_INFO_MASK)
    {
        case XLOG_CRYPT_KEY_INSERT:
            {
                CryptKeyInfo xlrec;
                xlrec = (CryptKeyInfo ) XLogRecGetData(record);
                appendStringInfo(buf, "crypt key insert, algo_id:%d, option:%d, keysize:%d", 
                    xlrec->algo_id, xlrec->option, xlrec->keysize);
                break;
            }
        case XLOG_CRYPT_KEY_DELETE:
            appendStringInfo(buf, "xlog type is comming, info:%u", XLOG_CRYPT_KEY_DELETE);
            break;
        case XLOG_REL_CRYPT_INSERT:
            {
                xl_rel_crypt_insert *xlrec;
                xlrec = (xl_rel_crypt_insert *) XLogRecGetData(record);
                appendStringInfo(buf, "rel crypt insert, database:%u tablespace:%u relnode:%u, algo_id:%d",
                                 xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode, xlrec->algo_id);
                break;
            }
        case XLOG_REL_CRYPT_DELETE:
            {
            	xl_rel_crypt_delete *xlrec;
				xlrec = (xl_rel_crypt_delete *) XLogRecGetData(record);
				appendStringInfo(buf, "rel crypt delete, database:%u tablespace:%u relnode:%u, algo_id:%d",
						xlrec->rnode.dbNode, xlrec->rnode.spcNode, xlrec->rnode.relNode, xlrec->algo_id);
            break;
            }
        default:
            Assert(0);
            break;
    }
    
    return;
}

const char * rel_crypt_identify(uint8 info)
{
    const char *id = NULL;

    switch(info & XLR_RMGR_INFO_MASK)
    {
        case XLOG_CRYPT_KEY_INSERT:
            id = "CRYPT KEY INSERT";
            break;
        case XLOG_CRYPT_KEY_DELETE:
            id = "CRYPT KEY DELETE";
            break;
        case XLOG_REL_CRYPT_INSERT:
            id = "REL CRYPT INSERT";
            break;
        case XLOG_REL_CRYPT_DELETE:
            id = "REL CRYPT DELETE";
            break;
        default:
            Assert(0);
            break;
    }

    return id;
}

