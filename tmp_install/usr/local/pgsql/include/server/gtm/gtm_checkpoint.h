/*-------------------------------------------------------------------------
 *
 * gtm_checkpoint.h
 *
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 *
 * src/include/gtm/gtm_checkpoint.h
 *
 *-------------------------------------------------------------------------
 */
 
#ifndef GTM_CHECKPOINT_H
#define GTM_CHECKPOINT_H

#include "gtm/gtm_c.h"
#include "access/xlogdefs.h"

typedef int64 pg_time_t;

typedef enum DBState
{
    DB_STARTUP = 0,
    DB_SHUTDOWNED,
    DB_SHUTDOWNED_IN_RECOVERY,
    DB_SHUTDOWNING,
    DB_IN_CRASH_RECOVERY,
    DB_IN_ARCHIVE_RECOVERY,
    DB_IN_PRODUCTION
} DBState;

typedef struct ControlFileData
{
    /*
     * Unique system identifier --- to ensure we match up xlog files with the
     * installation that produced them.
     */
    uint64        system_identifier;

    uint32        gtm_control_version;    /* GTM_CONTROL_VERSION */

    /*
     * System status data
     */
    DBState        state;            /* see enum above */
    GlobalTimestamp gts;
    pg_time_t    time;            /* time stamp of last gtm_control update */
    XLogRecPtr    checkPoint;        /* last check point record ptr */
    XLogRecPtr    prevCheckPoint; /* previous check point record ptr */

    uint64    CurrBytePos;
    uint64    PrevBytePos;

    TimeLineID    thisTimeLineID;

    /*
     * This data is used to make sure that configuration of this database is
     * compatible with the backend executable.
     */
    uint32        xlog_blcksz;    /* block size within WAL files */
    uint32        xlog_seg_size;    /* size of each WAL segment */

    pg_crc32c    crc;

    char padding[8112]; /* padding control file to 8k */
} ControlFileData;

#endif /* GTM_CHECKPOINT_H */
