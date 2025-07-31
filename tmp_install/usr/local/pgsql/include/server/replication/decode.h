/*-------------------------------------------------------------------------
 * decode.h
 *       PostgreSQL WAL to logical transformation
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 *-------------------------------------------------------------------------
 */
#ifndef DECODE_H
#define DECODE_H

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "replication/reorderbuffer.h"
#include "replication/logical.h"


#ifdef _PUB_SUB_RELIABLE_
bool LogicalDecoding_XLR_InfoMask_Test(uint8 infoMask);
void LogicalDecoding_XLR_InfoMask_Set(uint8 infoMask);
void LogicalDecoding_XLR_InfoMask_Clear(uint8 infoMask);
#endif

void LogicalDecodingProcessRecord(LogicalDecodingContext *ctx,
                             XLogReaderState *record);

#endif
