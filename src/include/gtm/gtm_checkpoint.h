/*-------------------------------------------------------------------------
 *
 * gtm_checkpoint.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
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

typedef enum _GtmState
{
    GTM_STARTUP = 0,
    GTM_SHUTDOWNED,
    GTM_IN_RECOVERY,
    GTM_IN_OVERWRITING_STORE,       /* Overwrite local data with data from the host. */
    GTM_IN_OVERWRITE_DONE,
    GTM_IN_ARCHIVE_RECOVERY,        /* not used */
    GTM_IN_PRODUCTION,
    GTM_STATE_NUM,
} GtmState;

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
    GtmState        state;            /* see enum above */
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
