/*-------------------------------------------------------------------------
 *
 * gtm_time.c
 *            Timestamp handling on GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *            $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_time.h"
#include <time.h>
#include <sys/time.h>

GTM_Timestamp
GTM_TimestampGetCurrent(void)
{
    struct timeval    tp;
    GTM_Timestamp    result;

    gettimeofday(&tp, NULL);

    result = (GTM_Timestamp) tp.tv_sec -
    ((GTM_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
    result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
    result = result + (tp.tv_usec / 1000000.0);
#endif

    return result;
}

GlobalTimestamp
GTM_TimestampGetMonotonicRaw(void)
{
    struct timespec     tp;
    GlobalTimestamp  result;    

    clock_gettime(CLOCK_MONOTONIC_RAW, &tp);

    result = tp.tv_sec * USECS_PER_SEC + tp.tv_nsec / 1000;
    elog(DEBUG8, "clock gettime sec "INT64_FORMAT " nsec " INT64_FORMAT " res " INT64_FORMAT, tp.tv_sec, tp.tv_nsec, result);
    
    return result;
}


GlobalTimestamp
GTM_TimestampGetMonotonicRawPrecise(GlobalTimestamp *tv_sec, GlobalTimestamp *tv_nsec)
{
    struct timespec     tp;
    GlobalTimestamp  result;

    clock_gettime(CLOCK_MONOTONIC_RAW, &tp);

    result = tp.tv_sec * USECS_PER_SEC + tp.tv_nsec / 1000;
    elog(DEBUG8, "clock gettime sec "INT64_FORMAT " nsec " INT64_FORMAT " res " INT64_FORMAT, tp.tv_sec, tp.tv_nsec, result);
    *tv_sec = tp.tv_sec;
    *tv_nsec = tp.tv_nsec;
    
    return result;
}

/*
 * TimestampDifference -- convert the difference between two timestamps
 *        into integer seconds and microseconds
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GTM_TimestampGetCurrent()).
 *
 * We expect start_time <= stop_time.  If not, we return zeroes; for current
 * callers there is no need to be tense about which way division rounds on
 * negative inputs.
 */
void
GTM_TimestampDifference(GTM_Timestamp start_time, GTM_Timestamp stop_time,
                    long *secs, int *microsecs)
{
    GTM_Timestamp diff = stop_time - start_time;

    if (diff <= 0)
    {
        *secs = 0;
        *microsecs = 0;
    }
    else
    {
#ifdef HAVE_INT64_TIMESTAMP
        *secs = (long) (diff / USECS_PER_SEC);
        *microsecs = (int) (diff % USECS_PER_SEC);
#else
        *secs = (long) diff;
        *microsecs = (int) ((diff - *secs) * 1000000.0);
#endif
    }
}

/*
 * GTM_TimestampDifferenceExceeds -- report whether the difference between two
 *        timestamps is >= a threshold (expressed in milliseconds)
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GTM_TimestampDifferenceExceeds()).
 */
bool
GTM_TimestampDifferenceExceeds(GTM_Timestamp start_time,
                           GTM_Timestamp stop_time,
                           int msec)
{
    GTM_Timestamp diff = stop_time - start_time;

#ifdef HAVE_INT64_TIMESTAMP
    return (diff >= msec * INT64CONST(1000));
#else
    return (diff * 1000.0 >= msec);
#endif
}
