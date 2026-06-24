/*-------------------------------------------------------------------------
 *
 * timestamp.h
 *	  Definitions for the SQL "timestamp" and "interval" types.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/timestamp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIMESTAMP_H
#define TIMESTAMP_H

#include "datatype/timestamp.h"
#include "fmgr.h"
#include "pgtime.h"
#include "forward/fnbufpage.h"
#include "common/int128.h"

/*
 * Macros for fmgr-callable functions.
 *
 * For Timestamp, we make use of the same support routines as for int64.
 * Therefore Timestamp is pass-by-reference if and only if int64 is!
 */
#define DatumGetTimestamp(X)  ((Timestamp) DatumGetInt64(X))
#define DatumGetTimestampTz(X)	((TimestampTz) DatumGetInt64(X))
#define DatumGetIntervalP(X)  ((Interval *) DatumGetPointer(X))

#define TimestampGetDatum(X) Int64GetDatum(X)
#define TimestampTzGetDatum(X) Int64GetDatum(X)
#define IntervalPGetDatum(X) PointerGetDatum(X)

#define PG_GETARG_TIMESTAMP(n) DatumGetTimestamp(PG_GETARG_DATUM(n))
#define PG_GETARG_TIMESTAMPTZ(n) DatumGetTimestampTz(PG_GETARG_DATUM(n))
#define PG_GETARG_INTERVAL_P(n) DatumGetIntervalP(PG_GETARG_DATUM(n))

#define PG_RETURN_TIMESTAMP(x) return TimestampGetDatum(x)
#define PG_RETURN_TIMESTAMPTZ(x) return TimestampTzGetDatum(x)
#define PG_RETURN_INTERVAL_P(x) return IntervalPGetDatum(x)


#define TIMESTAMP_MASK(b) (1 << (b))
#define INTERVAL_MASK(b) (1 << (b))

/* Macros to handle packing and unpacking the typmod field for intervals */
#define INTERVAL_FULL_RANGE (0x7FFF)
#define INTERVAL_RANGE_MASK (0x7FFF)
#define INTERVAL_FULL_PRECISION (0xFFFF)
#define INTERVAL_PRECISION_MASK (0xFFFF)
#define INTERVAL_TYPMOD(p,r) ((((r) & INTERVAL_RANGE_MASK) << 16) | ((p) & INTERVAL_PRECISION_MASK))
#define INTERVAL_PRECISION(t) ((t) & INTERVAL_PRECISION_MASK)
#define INTERVAL_RANGE(t) (((t) >> 16) & INTERVAL_RANGE_MASK)

#define INTERVAL_DAY_PREC_MASK (0xFF00)
#define INTERVAL_HOUR_PREC_MASK (0xFF00)
#define INTERVAL_MINUTE_PREC_MASK (0xFF00)
#define INTERVAL_SEC_PREC_MASK (0x00FF)
#define INTERVAL_YEAR_PREC_MASK (0x00FF)
#define INTERVAL_MONTH_PREC_MASK (0x00FF)
#define INTERVAL_YEAR_PRECISION(t) ((t) & INTERVAL_YEAR_PREC_MASK)
#define INTERVAL_MONTH_PRECISION(t) ((t) & INTERVAL_MONTH_PREC_MASK)
#define INTERVAL_DAY_PRECISION(t) (((t) & INTERVAL_DAY_PREC_MASK) >> 8)
#define INTERVAL_HOUR_PRECISION(t) (((t) & INTERVAL_HOUR_PREC_MASK) >> 8)
#define INTERVAL_MINUTE_PRECISION(t) (((t) & INTERVAL_MINUTE_PREC_MASK) >> 8)
#define INTERVAL_SEC_PRECISION(t) ((t) & INTERVAL_SEC_PREC_MASK)
#define INTERVAL_MIX_RREC(d,s) ((d) << 8 | s)
#define INTERVAL_FULL_SEC_PRECISION (0xFF)
#define INTERVAL_FULL_DAY_PRECISION (0xFF)
#define INTERVAL_FULL_YEAR_PRECISION (0xFF)
#define INTERVAL_FULL_MONTH_PRECISION (0xFF)
#define INTERVAL_FULL_HOUR_PRECISION (0xFF)
#define INTERVAL_FULL_MINUTE_PRECISION (0xFF)
#define INTERVAL_DEFAULT_DAY_PRECISION (0x2)
#define INTERVAL_DEFAULT_YEAR_PRECISION (0x2)
#define INTERVAL_DEFAULT_MONTH_PRECISION (0x2)
#define INTERVAL_DEFAULT_SEC_PRECISION (0x6)

#define TimestampTzPlusMilliseconds(tz,ms) ((tz) + ((ms) * (int64) 1000))

typedef struct Node Node;
typedef struct SortSupportData *SortSupport;
/* Set at postmaster start */
extern TimestampTz PgStartTime;

/* Set at configuration reload */
extern TimestampTz PgReloadTime;


/* Internal routines (not fmgr-callable) */

extern int32 anytimestamp_typmod_check(bool istz, int32 typmod);

extern TimestampTz GetCurrentTimestamp(void);
extern TimestampTz GetSQLCurrentTimestamp(int32 typmod);
extern Timestamp GetSQLLocalTimestamp(int32 typmod);
extern void TimestampDifference(TimestampTz start_time, TimestampTz stop_time,
					long *secs, int *microsecs);
extern bool TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec);

extern TimestampTz time_t_to_timestamptz(pg_time_t tm);
extern pg_time_t timestamptz_to_time_t(TimestampTz t);

extern const char *timestamptz_to_str(TimestampTz t);

extern int	tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *dt);
extern int timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm,
			 fsec_t *fsec, const char **tzn, pg_tz *attimezone);
extern void dt2time(Timestamp dt, int *hour, int *min, int *sec, fsec_t *fsec);

extern int	interval2tm(Interval span, struct pg_tm *tm, fsec_t *fsec);
extern int	tm2interval(struct pg_tm *tm, fsec_t fsec, Interval *span);

extern Timestamp SetEpochTimestamp(void);
extern void GetEpochTime(struct pg_tm *tm);

extern int timestamp_cmp_internal(Timestamp dt1, Timestamp dt2);

/* timestamp comparison works for timestamptz also */
#define timestamptz_cmp_internal(dt1,dt2)	timestamp_cmp_internal(dt1, dt2)

extern int	isoweek2j(int year, int week);
extern void isoweek2date(int woy, int *year, int *mon, int *mday);
extern void isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday);
extern int	date2isoweek(int year, int mon, int mday);
extern int	date2isoyear(int year, int mon, int mday);
extern int	date2isoyearday(int year, int mon, int mday);
extern TimestampTz timestamp2timestamptz(Timestamp timestamp);
extern Timestamp timestamptz2timestamp(TimestampTz timestamp);
extern int interval_cmp_internal(Interval *interval1, Interval *interval2);
extern int timestamp_fastcmp(Datum x, Datum y, SortSupport ssup);
extern Node *TemporalTransform(int32 max_precis, Node *node);
extern void AdjustTimestampForTypmod(Timestamp *time, int32 typmod);
extern void AdjustIntervalForTypmod(Interval *interval, int32 typmod);
extern Timestamp make_timestamp_internal(int year, int month, int day, int hour, int min, double sec);
extern int parse_sane_timezone(struct pg_tm *tm, text *zone);
extern Timestamp dt2local(Timestamp dt, int tz);
extern TimestampTz GetCurrentTransactionStartTimestamp(void);
extern TimestampTz GetCurrentStatementStartTimestamp(void);
extern Datum hash_uint32_extended(uint32 k, uint64 seed);
extern void GenerateFNQueryId(FNQueryId *id);
extern void CheckIntervalTypmod(int32 exprTypMod, int32 targetTypMod);

extern INT128 interval_cmp_value(const Interval *interval);
extern Datum in_range_timestamptz_interval(PG_FUNCTION_ARGS);
extern Datum in_range_timestamp_interval(PG_FUNCTION_ARGS);
extern Datum in_range_interval_interval(PG_FUNCTION_ARGS);

#endif							/* TIMESTAMP_H */
