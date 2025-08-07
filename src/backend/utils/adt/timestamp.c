/*-------------------------------------------------------------------------
 *
 * timestamp.c
 *	  Functions for the built-in SQL types "timestamp" and "interval".
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/timestamp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <sys/time.h>

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "common/int128.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/scansup.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/orcl_datetime.h"
#ifdef PGXC
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#endif

#ifdef _PG_ORCL_
#include "utils/numeric.h"
#include "utils/guc.h"
#endif


/*
 * gcc's -ffast-math switch breaks routines that expect exact results from
 * expressions like timeval / SECS_PER_HOUR, where timeval is double.
 */
#ifdef __FAST_MATH__
#error -ffast-math is known to break this code
#endif

#define SAMESIGN(a,b)	(((a) < 0) == ((b) < 0))

/* Set at postmaster start */
TimestampTz PgStartTime;

/* Set at configuration reload */
TimestampTz PgReloadTime;

static TimestampTz sf_last_timestamp = 0;

typedef struct
{
	Timestamp	current;
	Timestamp	finish;
	Interval	step;
	int			step_sign;
} generate_series_timestamp_fctx;

typedef struct
{
	TimestampTz current;
	TimestampTz finish;
	Interval	step;
	int			step_sign;
	pg_tz	   *attimezone;
} generate_series_timestamptz_fctx;


static TimeOffset time2t(const int hour, const int min, const int sec, const fsec_t fsec);
static Datum pg_timestamp_mi_day_internal(Timestamp dt, float8 days);
/* common code for timestamptypmodin and timestamptztypmodin */
static int32
anytimestamp_typmodin(bool istz, ArrayType *ta)
{
	int32	   *tl;
	int			n;

	tl = ArrayGetIntegerTypmods(ta, &n);

	/*
	 * we're not too tense about good error message here because grammar
	 * shouldn't allow wrong number of modifiers for TIMESTAMP
	 */
	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type modifier")));

	return anytimestamp_typmod_check(istz, tl[0]);
}

/* exported so parse_expr.c can use it */
int32
anytimestamp_typmod_check(bool istz, int32 typmod)
{
	if (typmod < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("TIMESTAMP(%d)%s precision must not be negative",
						typmod, (istz ? " WITH TIME ZONE" : ""))));
	if (typmod > MAX_TIMESTAMP_PRECISION)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("TIMESTAMP(%d)%s precision reduced to maximum allowed, %d",
						typmod, (istz ? " WITH TIME ZONE" : ""),
						MAX_TIMESTAMP_PRECISION)));
		typmod = MAX_TIMESTAMP_PRECISION;
	}

	return typmod;
}

/* common code for timestamptypmodout and timestamptztypmodout */
static char *
anytimestamp_typmodout(bool istz, int32 typmod)
{
	const char *tz = istz ? " with time zone" : " without time zone";

	if (typmod >= 0)
		return psprintf("(%d)%s", (int) typmod, tz);
	else
		return psprintf("%s", tz);
}


/* timestamp_in()
 * Convert a string to internal form.
 */
Datum
timestamp_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Timestamp	result = 0;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz = 0;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];

	/* this case is used for timestamp format is specified. */
	if (4 == PG_NARGS())
	{
		char *timestamp_fmt = PG_GETARG_CSTRING(3);
		if (timestamp_fmt == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
							errmsg("specified timestamp format is null")));
		}

		/* the following logic shared from to_timestamp(). */
		general_do_to_timestamp(str, timestamp_fmt, tm, &fsec);

		if (tm2timestamp(tm, fsec, NULL, &result) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							errmsg("timestamp out of range")));
		}

		AdjustTimestampForTypmod(&result, typmod);
	}
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		PG_TRY();
		{
			dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
								  field, ftype, MAXDATEFIELDS, &nf);
			if (dterr == 0)
				dterr = DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tz);
			if (dterr != 0)
				DateTimeParseError(dterr, str, "timestamp");

			switch (dtype)
			{
				case DTK_DATE:
					if (tm2timestamp(tm, fsec, NULL, &result) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								errmsg("timestamp out of range: \"%s\"", str)));
					break;

				case DTK_EPOCH:
					result = SetEpochTimestamp();
					break;

				case DTK_LATE:
					TIMESTAMP_NOEND(result);
					break;

				case DTK_EARLY:
					TIMESTAMP_NOBEGIN(result);
					break;

				case DTK_INVALID:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("date/time value \"%s\" is no longer supported", str)));

					TIMESTAMP_NOEND(result);
					break;

				default:
					elog(ERROR, "unexpected dtype %d while parsing timestamp \"%s\"",
						dtype, str);
					TIMESTAMP_NOEND(result);
			}

			AdjustTimestampForTypmod(&result, typmod);
		}
		PG_CATCH();
		{
			ErrorData *edata = NULL;

			(void)MemoryContextSwitchTo(oldcontext);

			/* Forward compatibility, which returns the previous error by default */
			edata = CopyErrorData();
				ReThrowError(edata);

			/* Flush any strings created in ErrorContext */
			FlushErrorState();
		}
		PG_END_TRY();

		(void)MemoryContextSwitchTo(oldcontext);
	}

	PG_RETURN_TIMESTAMP(result);
}

/* timestamp_out()
 * Convert a timestamp to external form.
 */
Datum
timestamp_out(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char		buf[MAXDATELEN + 1];

	if (TIMESTAMP_NOT_FINITE(timestamp))
		EncodeSpecialTimestamp(timestamp, buf);
	else if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0)
		EncodeDateTime(tm, fsec, false, 0, NULL, DateStyle, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		timestamp_recv			- converts external binary format to timestamp
 */
Datum
timestamp_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Timestamp	timestamp;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	timestamp = (Timestamp) pq_getmsgint64(buf);

	/* range check: see if timestamp_out would like it */
	if (TIMESTAMP_NOT_FINITE(timestamp))
		 /* ok */ ;
	else if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0 ||
			 !IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	AdjustTimestampForTypmod(&timestamp, typmod);

	PG_RETURN_TIMESTAMP(timestamp);
}

/*
 *		timestamp_send			- converts timestamp to binary format
 */
Datum
timestamp_send(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, timestamp);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
timestamptypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

	PG_RETURN_INT32(anytimestamp_typmodin(false, ta));
}

Datum
timestamptypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);

	PG_RETURN_CSTRING(anytimestamp_typmodout(false, typmod));
}


/* timestamp_transform()
 * Flatten calls to timestamp_scale() and timestamptz_scale() that solely
 * represent increases in allowed precision.
 */
Datum
timestamp_transform(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(TemporalTransform(MAX_TIMESTAMP_PRECISION,
										(Node *) PG_GETARG_POINTER(0)));
}

/* timestamp_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
Datum
timestamp_scale(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	int32		typmod = PG_GETARG_INT32(1);
	Timestamp	result;

	result = timestamp;

	AdjustTimestampForTypmod(&result, typmod);

	PG_RETURN_TIMESTAMP(result);
}

/*
 * AdjustTimestampForTypmod --- round off a timestamp to suit given typmod
 * Works for either timestamp or timestamptz.
 */
void
AdjustTimestampForTypmod(Timestamp *time, int32 typmod)
{
	static const int64 TimestampScales[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 TimestampOffsets[MAX_TIMESTAMP_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	if (!TIMESTAMP_NOT_FINITE(*time)
		&& (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION))
	{
		if (typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("timestamp(%d) precision must be between %d and %d",
							typmod, 0, MAX_TIMESTAMP_PRECISION)));

		if (*time >= INT64CONST(0))
		{
			*time = ((*time + TimestampOffsets[typmod]) / TimestampScales[typmod]) *
				TimestampScales[typmod];
		}
		else
		{
			*time = -((((-*time) + TimestampOffsets[typmod]) / TimestampScales[typmod])
					  * TimestampScales[typmod]);
		}
	}
}


/* timestamptz_in()
 * Convert a string to internal form.
 */
Datum
timestamptz_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimestampTz result = 0;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];
	MemoryContext oldcontext = CurrentMemoryContext;

	PG_TRY();
	{
		dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
							field, ftype, MAXDATEFIELDS, &nf);
		if (dterr == 0)
			dterr = DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tz);
		if (dterr != 0)
			DateTimeParseError(dterr, str, "timestamp with time zone");

		switch (dtype)
		{
			case DTK_DATE:
				if (tm2timestamp(tm, fsec, &tz, &result) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							errmsg("timestamp out of range: \"%s\"", str)));
				break;

			case DTK_EPOCH:
				result = SetEpochTimestamp();
				break;

			case DTK_LATE:
				TIMESTAMP_NOEND(result);
				break;

			case DTK_EARLY:
				TIMESTAMP_NOBEGIN(result);
				break;

			case DTK_INVALID:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("date/time value \"%s\" is no longer supported", str)));

				TIMESTAMP_NOEND(result);
				break;

			default:
				elog(ERROR, "unexpected dtype %d while parsing timestamptz \"%s\"",
					dtype, str);
				TIMESTAMP_NOEND(result);
		}

		AdjustTimestampForTypmod(&result, typmod);
	}
	PG_CATCH();
	{
		ErrorData *edata = NULL;

		(void)MemoryContextSwitchTo(oldcontext);

		/* Forward compatibility, which returns the previous error by default */
		edata = CopyErrorData();

			ReThrowError(edata);

		/* Flush any strings created in ErrorContext */
		FlushErrorState();
	}
	PG_END_TRY();

	(void)MemoryContextSwitchTo(oldcontext);
	PG_RETURN_TIMESTAMPTZ(result);
}

/*
 * Try to parse a timezone specification, and return its timezone offset value
 * if it's acceptable.  Otherwise, an error is thrown.
 *
 * Note: some code paths update tm->tm_isdst, and some don't; current callers
 * don't care, so we don't bother being consistent.
 */
int
parse_sane_timezone(struct pg_tm *tm, text *zone)
{
	char		tzname[TZ_STRLEN_MAX + 1];
	int			rt;
	int			tz;

	text_to_cstring_buffer(zone, tzname, sizeof(tzname));

	/*
	 * Look up the requested timezone.  First we try to interpret it as a
	 * numeric timezone specification; if DecodeTimezone decides it doesn't
	 * like the format, we look in the timezone abbreviation table (to handle
	 * cases like "EST"), and if that also fails, we look in the timezone
	 * database (to handle cases like "America/New_York").  (This matches the
	 * order in which timestamp input checks the cases; it's important because
	 * the timezone database unwisely uses a few zone names that are identical
	 * to offset abbreviations.)
	 *
	 * Note pg_tzset happily parses numeric input that DecodeTimezone would
	 * reject.  To avoid having it accept input that would otherwise be seen
	 * as invalid, it's enough to disallow having a digit in the first
	 * position of our input string.
	 */
	if (isdigit((unsigned char) *tzname))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid input syntax for numeric time zone: \"%s\"",
						tzname),
				 errhint("Numeric time zones must have \"-\" or \"+\" as first character.")));

	rt = DecodeTimezone(tzname, &tz);
	if (rt != 0)
	{
		char	   *lowzone;
		int			type,
					val;
		pg_tz	   *tzp;

		if (rt == DTERR_TZDISP_OVERFLOW)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("numeric time zone \"%s\" out of range", tzname)));
		else if (rt != DTERR_BAD_FORMAT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));

		/* DecodeTimezoneAbbrev requires lowercase input */
		lowzone = downcase_truncate_identifier(tzname,
											   strlen(tzname),
											   false);
		type = DecodeTimezoneAbbrev(0, lowzone, &val, &tzp);

		if (type == TZ || type == DTZ)
		{
			/* fixed-offset abbreviation */
			tz = -val;
		}
		else if (type == DYNTZ)
		{
			/* dynamic-offset abbreviation, resolve using specified time */
			tz = DetermineTimeZoneAbbrevOffset(tm, tzname, tzp);
		}
		else
		{
			/* try it as a full zone name */
			tzp = pg_tzset(tzname, false);
			if (tzp)
				tz = DetermineTimeZoneOffset(tm, tzp);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("time zone \"%s\" not recognized", tzname)));
		}
	}

	return tz;
}

/*
 * Look up the requested timezone, returning a pg_tz struct.
 *
 * This is the same as DecodeTimezoneNameToTz, but starting with a text Datum.
 */
static pg_tz *
lookup_timezone(text *zone)
{
	char		tzname[TZ_STRLEN_MAX + 1];

	text_to_cstring_buffer(zone, tzname, sizeof(tzname));

	return DecodeTimezoneNameToTz(tzname);
}

/*
 * make_timestamp_internal
 *		workhorse for make_timestamp and make_timestamptz
 */
Timestamp
make_timestamp_internal(int year, int month, int day,
						int hour, int min, double sec)
{
	struct pg_tm tm;
	TimeOffset	date;
	TimeOffset	time;
	int			dterr;
	Timestamp	result;

	tm.tm_year = year;
	tm.tm_mon = month;
	tm.tm_mday = day;

	/*
	 * Note: we'll reject zero or negative year values.  Perhaps negatives
	 * should be allowed to represent BC years?
	 */
	dterr = ValidateDate(DTK_DATE_M, false, false, false, &tm);

	if (dterr != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("date field value out of range: %d-%02d-%02d",
						year, month, day)));

	if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range: %d-%02d-%02d",
						year, month, day)));

	date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

	/*
	 * This should match the checks in DecodeTimeOnly, except that since we're
	 * dealing with a float "sec" value, we also explicitly reject NaN.  (An
	 * infinity input should get rejected by the range comparisons, but we
	 * can't be sure how those will treat a NaN.)
	 */
	if (hour < 0 || min < 0 || min > MINS_PER_HOUR - 1 ||
		isnan(sec) ||
		sec < 0 || sec > SECS_PER_MINUTE ||
		hour > HOURS_PER_DAY ||
	/* test for > 24:00:00 */
		(hour == HOURS_PER_DAY && (min > 0 || sec > 0)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("time field value out of range: %d:%02d:%02g",
						hour, min, sec)));

	/* This should match tm2time */
	time = (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE)
			* USECS_PER_SEC) + rint(sec * USECS_PER_SEC);

	result = date * USECS_PER_DAY + time;
	/* check for major overflow */
	if ((result - time) / USECS_PER_DAY != date)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range: %d-%02d-%02d %d:%02d:%02g",
						year, month, day,
						hour, min, sec)));

	/* check for just-barely overflow (okay except time-of-day wraps) */
	/* caution: we want to allow 1999-12-31 24:00:00 */
	if ((result < 0 && date > 0) ||
		(result > 0 && date < -1))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range: %d-%02d-%02d %d:%02d:%02g",
						year, month, day,
						hour, min, sec)));

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range: %d-%02d-%02d %d:%02d:%02g",
						year, month, day,
						hour, min, sec)));

	return result;
}

/*
 * make_timestamp() - timestamp constructor
 */
Datum
make_timestamp(PG_FUNCTION_ARGS)
{
	int32		year = PG_GETARG_INT32(0);
	int32		month = PG_GETARG_INT32(1);
	int32		mday = PG_GETARG_INT32(2);
	int32		hour = PG_GETARG_INT32(3);
	int32		min = PG_GETARG_INT32(4);
	float8		sec = PG_GETARG_FLOAT8(5);
	Timestamp	result;

	result = make_timestamp_internal(year, month, mday,
									 hour, min, sec);

	PG_RETURN_TIMESTAMP(result);
}

/*
 * make_timestamptz() - timestamp with time zone constructor
 */
Datum
make_timestamptz(PG_FUNCTION_ARGS)
{
	int32		year = PG_GETARG_INT32(0);
	int32		month = PG_GETARG_INT32(1);
	int32		mday = PG_GETARG_INT32(2);
	int32		hour = PG_GETARG_INT32(3);
	int32		min = PG_GETARG_INT32(4);
	float8		sec = PG_GETARG_FLOAT8(5);
	Timestamp	result;

	result = make_timestamp_internal(year, month, mday,
									 hour, min, sec);

	PG_RETURN_TIMESTAMPTZ(timestamp2timestamptz(result));
}

/*
 * Construct a timestamp with time zone.
 *		As above, but the time zone is specified as seventh argument.
 */
Datum
make_timestamptz_at_timezone(PG_FUNCTION_ARGS)
{
	int32		year = PG_GETARG_INT32(0);
	int32		month = PG_GETARG_INT32(1);
	int32		mday = PG_GETARG_INT32(2);
	int32		hour = PG_GETARG_INT32(3);
	int32		min = PG_GETARG_INT32(4);
	float8		sec = PG_GETARG_FLOAT8(5);
	text	   *zone = PG_GETARG_TEXT_PP(6);
	TimestampTz result;
	Timestamp	timestamp;
	struct pg_tm tt;
	int			tz;
	fsec_t		fsec;

	timestamp = make_timestamp_internal(year, month, mday,
										hour, min, sec);

	if (timestamp2tm(timestamp, NULL, &tt, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	tz = parse_sane_timezone(&tt, zone);

	result = dt2local(timestamp, -tz);

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(result);
}

/*
 * to_timestamp(double precision)
 * Convert UNIX epoch to timestamptz.
 */
Datum
float8_timestamptz(PG_FUNCTION_ARGS)
{
	float8		seconds = PG_GETARG_FLOAT8(0);
	TimestampTz result;

	/* Deal with NaN and infinite inputs ... */
	if (isnan(seconds))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp cannot be NaN")));

	if (isinf(seconds))
	{
		if (seconds < 0)
			TIMESTAMP_NOBEGIN(result);
		else
			TIMESTAMP_NOEND(result);
	}
	else
	{
		/* Out of range? */
		if (seconds <
			(float8) SECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)
			|| seconds >=
			(float8) SECS_PER_DAY * (TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range: \"%g\"", seconds)));

		/* Convert UNIX epoch to Postgres epoch */
		seconds -= ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

		seconds = rint(seconds * USECS_PER_SEC);
		result = (int64) seconds;

		/* Recheck in case roundoff produces something just out of range */
		if (!IS_VALID_TIMESTAMP(result))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range: \"%g\"",
							PG_GETARG_FLOAT8(0))));
	}

	PG_RETURN_TIMESTAMP(result);
}

/* timestamptz_out()
 * Convert a timestamp to external form.
 */
Datum
timestamptz_out(PG_FUNCTION_ARGS)
{
	TimestampTz dt = PG_GETARG_TIMESTAMPTZ(0);
	char	   *result;
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	const char *tzn;
	char		buf[MAXDATELEN + 1];

	if (TIMESTAMP_NOT_FINITE(dt))
		EncodeSpecialTimestamp(dt, buf);
	else if (timestamp2tm(dt, &tz, tm, &fsec, &tzn, NULL) == 0)
		EncodeDateTime(tm, fsec, true, tz, tzn, DateStyle, buf);
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 *		timestamptz_recv			- converts external binary format to timestamptz
 */
Datum
timestamptz_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	TimestampTz timestamp;
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;

	timestamp = (TimestampTz) pq_getmsgint64(buf);

	/* range check: see if timestamptz_out would like it */
	if (TIMESTAMP_NOT_FINITE(timestamp))
		 /* ok */ ;
	else if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0 ||
			 !IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	AdjustTimestampForTypmod(&timestamp, typmod);

	PG_RETURN_TIMESTAMPTZ(timestamp);
}

/*
 *		timestamptz_send			- converts timestamptz to binary format
 */
Datum
timestamptz_send(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, timestamp);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
timestamptztypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

	PG_RETURN_INT32(anytimestamp_typmodin(true, ta));
}

Datum
timestamptztypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);

	PG_RETURN_CSTRING(anytimestamp_typmodout(true, typmod));
}


/* timestamptz_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
Datum
timestamptz_scale(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int32		typmod = PG_GETARG_INT32(1);
	TimestampTz result;

	result = timestamp;

	AdjustTimestampForTypmod(&result, typmod);

	PG_RETURN_TIMESTAMPTZ(result);
}


/* interval_in()
 * Convert a string to internal form.
 *
 * External format(s):
 *	Uses the generic date/time parsing and decoding routines.
 */
Datum
interval_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Interval   *result;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			dtype;
	int			nf;
	int			range;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[256];

	tm->tm_year = 0;
	tm->tm_mon = 0;
	tm->tm_mday = 0;
	tm->tm_hour = 0;
	tm->tm_min = 0;
	tm->tm_sec = 0;
	fsec = 0;

	if (typmod >= 0)
		range = INTERVAL_RANGE(typmod);
	else
		range = INTERVAL_FULL_RANGE;

	dterr = ParseDateTime(str, workbuf, sizeof(workbuf), field,
						  ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeInterval(field, ftype, nf, range,
							   &dtype, tm, &fsec);

	/* if those functions think it's a bad format, try ISO8601 style */
	if (dterr == DTERR_BAD_FORMAT)
		dterr = DecodeISO8601Interval(str,
									  &dtype, tm, &fsec);

	if (dterr != 0)
	{
		if (dterr == DTERR_FIELD_OVERFLOW)
			dterr = DTERR_INTERVAL_OVERFLOW;
		DateTimeParseError(dterr, str, "interval");
	}

	result = (Interval *) palloc(sizeof(Interval));

	switch (dtype)
	{
		case DTK_DELTA:
			if (tm2interval(tm, fsec, result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("interval out of range")));
			break;

		case DTK_INVALID:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("date/time value \"%s\" is no longer supported", str)));
			break;

		default:
			elog(ERROR, "unexpected dtype %d while parsing interval \"%s\"",
				 dtype, str);
	}

	AdjustIntervalForTypmod(result, typmod);

	if (ORA_MODE)
		IntervalForSQLStandard(result);

	PG_RETURN_INTERVAL_P(result);
}

/* interval_out()
 * Convert a time span to external form.
 */
Datum
interval_out(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	char	   *result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	char		buf[MAXDATELEN + 1];

	if (interval2tm(*span, tm, &fsec) != 0)
		elog(ERROR, "could not convert interval to tm");

	EncodeInterval(tm, fsec, IntervalStyle, buf);

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

/*
 * To reduce storage overhead of INTERVAL, add a new output function to
 * format interval output with a typemod.
 */
Datum
interval_out_withtypmod(PG_FUNCTION_ARGS)
{
	Interval	*span = PG_GETARG_INTERVAL_P(0);
	char	*result;
	struct pg_tm	tt,
					*tm = &tt;
	fsec_t	fsec;
	char	buf[MAXDATELEN + 1];
	int		typemod = PG_GETARG_INT32(1);

	if (ORA_MODE)
		IntervalForSQLStandard(span);

	if (interval2tm(*span, tm, &fsec) != 0)
		elog(ERROR, "could not convert interval to tm");

	EncodeInterval_WithTypmod(tm, fsec, IntervalStyle, buf, typemod);

	result = pstrdup(buf);
	PG_RETURN_CSTRING(result);
}

Datum
orcl_interval_typmode_tochar(PG_FUNCTION_ARGS)
{
	Interval	*span = PG_GETARG_INTERVAL_P(0);
	text		*result;
	struct pg_tm	tt,
					*tm = &tt;
	fsec_t	fsec;
	char	buf[MAXDATELEN + 1];
	int		typemod = PG_GETARG_INT32(1);

	if (interval2tm(*span, tm, &fsec) != 0)
		elog(ERROR, "could not convert interval to tm");

	EncodeInterval_WithTypmod(tm, fsec, IntervalStyle, buf, typemod);

	result = cstring_to_text(pstrdup(buf));

	PG_RETURN_TEXT_P(result);
}

/*
 *		interval_recv			- converts external binary format to interval
 */
Datum
interval_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Interval   *interval;

	interval = (Interval *) palloc(sizeof(Interval));

	interval->time = pq_getmsgint64(buf);
	interval->day = pq_getmsgint(buf, sizeof(interval->day));
	interval->month = pq_getmsgint(buf, sizeof(interval->month));

	AdjustIntervalForTypmod(interval, typmod);

	PG_RETURN_INTERVAL_P(interval);
}

/*
 *		interval_send			- converts interval to binary format
 */
Datum
interval_send(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, interval->time);
	pq_sendint32(&buf, interval->day);
	pq_sendint32(&buf, interval->month);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * Get typmode for interval which only has one precision,
 * including month(n)/hour(n)/minute(n)/second(n)/day to second(n)
 */
static int32
get_interval_typmod_one_precision(int32 *tl, bool *range_matched)
{
	int32 typmod = -1;
	*range_matched = false;

	if (INTERVAL_MASK(MONTH) == tl[0])
	{
		if (tl[1] > MAX_INTERVAL_MONTH_PRECISION)
		{
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL MONTH(%d) precision execeed %d",
					tl[1], MAX_INTERVAL_MONTH_PRECISION)));
		}
		typmod = INTERVAL_TYPMOD(tl[1], tl[0]);
		*range_matched = true;
	}
	else if (INTERVAL_MASK(HOUR) == tl[0])
	{
		int	hour_prec = INTERVAL_HOUR_PRECISION(tl[1]);
		if (hour_prec > MAX_INTERVAL_HOUR_PRECISION)
		{
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL HOUR(%d) precision execeed %d",
					hour_prec, MAX_INTERVAL_HOUR_PRECISION)));
		}

		typmod = INTERVAL_TYPMOD(tl[1],	tl[0]);
		*range_matched = true;
	}
	else if (INTERVAL_MASK(MINUTE) == tl[0])
	{
		int	min_prec = INTERVAL_MINUTE_PRECISION(tl[1]);
		if (min_prec > MAX_INTERVAL_MINUTE_PRECISION)
		{
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL MINUTE(%d) precision execeed %d",
					min_prec, MAX_INTERVAL_MINUTE_PRECISION)));
		}

		typmod = INTERVAL_TYPMOD(tl[1],	tl[0]);
		*range_matched = true;
	}
	else if (INTERVAL_MASK(SECOND) == tl[0])
	{
		if (tl[1] > MAX_INTERVAL_DAY_PRECISION)
		{
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL SECOND(%d) precision execeed %d",
					tl[1], MAX_INTERVAL_DAY_PRECISION)));
		}
		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(tl[1],
									MAX_INTERVAL_PRECISION),
									tl[0]);
		*range_matched = true;
	}
	else if ((INTERVAL_MASK(DAY) | INTERVAL_MASK(SECOND)) == tl[0])
	{
		/* second(m, n) */
		int	day_prec = INTERVAL_DAY_PRECISION(tl[1]);
		int	sec_prec = INTERVAL_SEC_PRECISION(tl[1]);

		if (day_prec > MAX_INTERVAL_DAY_PRECISION)
		{
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL SECOND(%d) precision execeed %d",
					tl[1], MAX_INTERVAL_DAY_PRECISION)));
		}
		if (sec_prec > MAX_INTERVAL_PRECISION)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INTERVAL SECOND(%d) precision reduced to maximum allowed, %d",
							sec_prec, MAX_INTERVAL_PRECISION)));
			sec_prec = MAX_INTERVAL_PRECISION;
		}

		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(day_prec, sec_prec),
								INTERVAL_MASK(SECOND));
		*range_matched = true;
	}
	else if ((INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
			 INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) == tl[0] ||
			/*
			 * Setup default precison of DAY, DAY(2). We disable this feature for old
			 * instances to reduce risk of incorrect results widely.
			 */
			(default_interval_day_prec && ORA_MODE &&
			((INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) == tl[0] ||
			(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) == tl[0])))
	{
		/* day to sencod(n) */
		int sec_prec = tl[1];
		if (tl[1] > MAX_INTERVAL_PRECISION)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("INTERVAL(%d) precision reduced to maximum allowed, %d",
							tl[1], MAX_INTERVAL_PRECISION)));
			sec_prec = MAX_INTERVAL_PRECISION;
		}
		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(
								INTERVAL_DEFAULT_DAY_PRECISION,
								sec_prec),
								tl[0]);
		*range_matched = true;
	}
	return typmod;
}

/*
 * Get typmode for interval which in format of xxx(n) to second(n)
 */
static int32
get_interval_typmod_two_precision(int32 *tl, bool *range_matched)
{
	int32 typmod = -1;
	*range_matched = false;

	if (INTERVAL_MASK(YEAR) & tl[2])
	{
		int	year_prec = INTERVAL_YEAR_PRECISION(tl[1]);

		if (year_prec < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("INTERVAL YEAR(%d) precision must not be negative",
							year_prec)));
		}

		if (year_prec > MAX_INTERVAL_YEAR_PRECISION)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INTERVAL YEAR(%d) precision execeed maximum allowed, %d",
							year_prec, MAX_INTERVAL_YEAR_PRECISION)));
			year_prec = MAX_INTERVAL_YEAR_PRECISION;
		}

		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(0, year_prec), tl[0]);
		*range_matched = true;
	}
	else if (INTERVAL_MASK(DAY) & tl[2])
	{
		int	day_prec = INTERVAL_DAY_PRECISION(tl[1]);
		int	sec_prec = INTERVAL_SEC_PRECISION(tl[1]);

		if (INTERVAL_MASK(SECOND) & tl[2])
		{
			if (sec_prec < 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("INTERVAL SECOND(%d) precision must not be negative",
							sec_prec)));
			}

			if (sec_prec > MAX_INTERVAL_PRECISION)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("INTERVAL SECOND(%d) precision reduced to maximum allowed, %d",
								sec_prec, MAX_INTERVAL_PRECISION)));
				sec_prec = MAX_INTERVAL_PRECISION;
			}
		}
		else
				sec_prec = INTERVAL_DEFAULT_SEC_PRECISION;

		if (day_prec < 0)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL DAY(%d) precision must not be negative", day_prec)));
		}

		if (day_prec > MAX_INTERVAL_DAY_PRECISION)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INTERVAL DAY(%d) precision execeed maximum allowed, %d",
							day_prec, MAX_INTERVAL_DAY_PRECISION)));
			day_prec = MAX_INTERVAL_DAY_PRECISION;
		}

		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(day_prec, sec_prec), tl[0]);
		*range_matched = true;
	}
	else if (INTERVAL_MASK(HOUR) & tl[2])
	{
		int	hour_prec = INTERVAL_HOUR_PRECISION(tl[1]);
		int	sec_prec = INTERVAL_SEC_PRECISION(tl[1]);

		/* hour(n) to second */
		if (INTERVAL_MASK(SECOND) & tl[2])
		{
			if (sec_prec < 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("INTERVAL SECOND(%d) precision must not be negative",
							sec_prec)));
			}

			if (sec_prec > MAX_INTERVAL_PRECISION)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("INTERVAL SECOND(%d) precision reduced to maximum allowed, %d",
								sec_prec, MAX_INTERVAL_PRECISION)));
				sec_prec = MAX_INTERVAL_PRECISION;
			}
		}
		else
				sec_prec = INTERVAL_DEFAULT_SEC_PRECISION;

		if (hour_prec < 0)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL HOUR(%d) precision must not be negative", hour_prec)));
		}

		if (hour_prec > MAX_INTERVAL_DAY_PRECISION)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INTERVAL HOUR(%d) precision execeed maximum allowed, %d",
							hour_prec, MAX_INTERVAL_DAY_PRECISION)));
			hour_prec = MAX_INTERVAL_DAY_PRECISION;
		}

		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(hour_prec, sec_prec), tl[0]);
		*range_matched = true;
	}
	else if (INTERVAL_MASK(MINUTE) & tl[2])
	{
		int	min_prec = INTERVAL_MINUTE_PRECISION(tl[1]);
		int	sec_prec = INTERVAL_SEC_PRECISION(tl[1]);

		/* minute(n) to second */
		if (INTERVAL_MASK(SECOND) & tl[2])
		{
			if (sec_prec < 0)
			{
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("INTERVAL SECOND(%d) precision must not be negative",
							sec_prec)));
			}

			if (sec_prec > MAX_INTERVAL_PRECISION)
			{
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("INTERVAL SECOND(%d) precision reduced to maximum allowed, %d",
								sec_prec, MAX_INTERVAL_PRECISION)));
				sec_prec = MAX_INTERVAL_PRECISION;
			}
		}
		else
				sec_prec = INTERVAL_DEFAULT_SEC_PRECISION;

		if (min_prec < 0)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("INTERVAL HOUR(%d) precision must not be negative", min_prec)));
		}

		if (min_prec > MAX_INTERVAL_DAY_PRECISION)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INTERVAL HOUR(%d) precision execeed maximum allowed, %d",
							min_prec, MAX_INTERVAL_DAY_PRECISION)));
			min_prec = MAX_INTERVAL_DAY_PRECISION;
		}

		typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(min_prec, sec_prec), tl[0]);
		*range_matched = true;
	}
	return typmod;
}

/*
 * The interval typmod stores a "range" in its high 16 bits and a "precision"
 * in its low 16 bits.  Both contribute to defining the resolution of the
 * type.  Range addresses resolution granules larger than one second, and
 * precision specifies resolution below one second.  This representation can
 * express all SQL standard resolutions, but we implement them all in terms of
 * truncating rightward from some position.  Range is a bitmap of permitted
 * fields, but only the temporally-smallest such field is significant to our
 * calculations.  Precision is a count of sub-second decimal places to retain.
 * Setting all bits (INTERVAL_FULL_PRECISION) gives the same truncation
 * semantics as choosing MAX_INTERVAL_PRECISION.
 */
Datum
intervaltypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod = -1;

	tl = ArrayGetIntegerTypmods(ta, &n);

	/*
	 * tl[0] - interval range (fields bitmask)	tl[1] - precision (optional)
	 *
	 * Note we must validate tl[0] even though it's normally guaranteed
	 * correct by the grammar --- consider SELECT 'foo'::"interval"(1000).
	 */
	if (n > 0)
	{
		switch (tl[0])
		{
			case INTERVAL_MASK(YEAR):
			case INTERVAL_MASK(MONTH):
			case INTERVAL_MASK(DAY):
			case INTERVAL_MASK(HOUR):
			case INTERVAL_MASK(MINUTE):
			case INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(DAY) | INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			case INTERVAL_FULL_RANGE:
				/* all OK */
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid INTERVAL type modifier")));
		}
	}

	if (n == 1)
	{
		if (tl[0] != INTERVAL_FULL_RANGE)
		{
			typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, tl[0]);
			if (ORA_MODE)
			{
				if ((INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
					INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) & tl[0])
				{
					typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(
												INTERVAL_DEFAULT_DAY_PRECISION,
												INTERVAL_DEFAULT_SEC_PRECISION),
											tl[0]);
				}
				else if ((INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)) & tl[0])
				{
					typmod = INTERVAL_TYPMOD(INTERVAL_MIX_RREC(0,
												INTERVAL_DEFAULT_YEAR_PRECISION),
											tl[0]);
				}
			}
		}
		else
			typmod = -1;
	}
	else if (n == 2)
	{
		if (tl[1] < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("INTERVAL(%d) precision must not be negative",
							tl[1])));
		if (ORA_MODE)
		{
			bool range_matched = false;
			typmod = get_interval_typmod_one_precision(tl, &range_matched);
			if (range_matched)
				PG_RETURN_INT32(typmod);
		}

		if (tl[1] > MAX_INTERVAL_PRECISION)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("INTERVAL(%d) precision reduced to maximum allowed, %d",
							tl[1], MAX_INTERVAL_PRECISION)));
			typmod = INTERVAL_TYPMOD(MAX_INTERVAL_PRECISION, tl[0]);
		}
		else
			typmod = INTERVAL_TYPMOD(tl[1], tl[0]);
	}
	else if (n == 3)
	{
		bool range_matched = false;
		/*
		 * Three elements found, it maybe a DAY(n) or YEAR(n).
		 */
		if (!ORA_MODE)
			elog(ERROR, "only opentenbase_ora mode support INTERVAL DAY/YEAR(n)");

		Assert((INTERVAL_MASK(YEAR) & tl[2]) ||
				(INTERVAL_MASK(DAY) & tl[2]) ||
				(INTERVAL_MASK(HOUR) & tl[2]) ||
				(INTERVAL_MASK(MINUTE) & tl[2]));

		typmod = get_interval_typmod_two_precision(tl, &range_matched);
		if (range_matched)
			PG_RETURN_INT32(typmod);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid INTERVAL type modifier")));
		typmod = 0;				/* keep compiler quiet */
	}

	PG_RETURN_INT32(typmod);
}

Datum
intervaltypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *res = (char *) palloc(64);
	int			fields;
	int			precision;
	const char *fieldstr;

#define INTERVAL_DAY_TYPMOD(fmt)		\
		do	\
		{	\
			if (ORA_MODE)	\
			{			\
				if (precision != INTERVAL_FULL_PRECISION)		\
				{	\
					int	day_prec;	\
					day_prec = INTERVAL_DAY_PRECISION(precision);	\
					if (day_prec != INTERVAL_FULL_DAY_PRECISION)	\
					{	\
						char	*field_typmod = (char *) palloc(64);	\
						snprintf(field_typmod, 64, fmt, day_prec);	\
						fieldstr = field_typmod;	\
					}	\
				}	\
			}	\
		} while (0)

#define INTERVAL_YEAR_TYPMOD(fmt)		\
		do	\
		{	\
			if (ORA_MODE)	\
			{			\
				if (precision != INTERVAL_FULL_PRECISION)		\
				{	\
					int	year_prec;	\
					year_prec = INTERVAL_YEAR_PRECISION(precision);	\
					if (year_prec != INTERVAL_FULL_YEAR_PRECISION)	\
					{	\
						char	*field_typmod = (char *) palloc(64);	\
						snprintf(field_typmod, 64, fmt, year_prec);	\
						fieldstr = field_typmod;	\
					}	\
				}	\
			}	\
		} while (0)

#define INTERVAL_MONTH_TYPMOD(fmt)		\
		do	\
		{	\
			if (ORA_MODE)	\
			{			\
				if (precision != INTERVAL_FULL_PRECISION)		\
				{	\
					int	month_prec;	\
					month_prec = INTERVAL_MONTH_PRECISION(precision);	\
					if (month_prec != INTERVAL_FULL_MONTH_PRECISION)	\
					{	\
						char	*field_typmod = (char *) palloc(64);	\
						snprintf(field_typmod, 64, fmt, month_prec);	\
						fieldstr = field_typmod;	\
					}	\
				}	\
			}	\
		} while (0)
#define INTERVAL_HOUR_TYPMOD(fmt)		\
		do	\
		{	\
			if (ORA_MODE)	\
			{			\
				if (precision != INTERVAL_FULL_PRECISION)		\
				{	\
					int	hour_prec;	\
					hour_prec = INTERVAL_HOUR_PRECISION(precision);	\
					if (hour_prec != INTERVAL_FULL_HOUR_PRECISION)	\
					{	\
						char	*field_typmod = (char *) palloc(64);	\
						snprintf(field_typmod, 64, fmt, hour_prec);	\
						fieldstr = field_typmod;	\
					}	\
				}	\
			}	\
		} while (0)

	if (typmod < 0)
	{
		*res = '\0';
		PG_RETURN_CSTRING(res);
	}

	fields = INTERVAL_RANGE(typmod);
	precision = INTERVAL_PRECISION(typmod);

	switch (fields)
	{
		case INTERVAL_MASK(YEAR):
			fieldstr = " year";
			INTERVAL_YEAR_TYPMOD(" year(%d)");
			break;
		case INTERVAL_MASK(MONTH):
			fieldstr = " month";
			INTERVAL_MONTH_TYPMOD( " month(%d)");
			break;
		case INTERVAL_MASK(DAY):
			fieldstr = " day";
			INTERVAL_DAY_TYPMOD(" day(%d)");
			break;
		case INTERVAL_MASK(HOUR):
			fieldstr = " hour";
			INTERVAL_HOUR_TYPMOD( " hour(%d)");
			break;
		case INTERVAL_MASK(MINUTE):
			fieldstr = " minute";
			break;
		case INTERVAL_MASK(SECOND):
			fieldstr = " second";
			break;
		case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
			fieldstr = " year to month";
			INTERVAL_YEAR_TYPMOD(" year(%d) to month");
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
			fieldstr = " day to hour";
			INTERVAL_DAY_TYPMOD(" day(%d) to hour");
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			fieldstr = " day to minute";
			INTERVAL_DAY_TYPMOD(" day(%d) to minute");
			break;
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			fieldstr = " day to second";
			INTERVAL_DAY_TYPMOD(" day(%d) to second");
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			fieldstr = " hour to minute";
			break;
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			fieldstr = " hour to second";
			break;
		case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			fieldstr = " minute to second";
			break;
		case INTERVAL_FULL_RANGE:
			fieldstr = "";
			break;
		default:
			elog(ERROR, "invalid INTERVAL typmod: 0x%x", typmod);
			fieldstr = "";
			break;
	}

	if (precision != INTERVAL_FULL_PRECISION)
	{
		if (ORA_MODE)
		{
			precision = INTERVAL_SEC_PRECISION(precision);
			if ((fields & INTERVAL_MASK(SECOND)) &&
					precision != INTERVAL_FULL_SEC_PRECISION)
				snprintf(res, 64, "%s(%d)", fieldstr, precision);
			else
				snprintf(res, 64, "%s", fieldstr);
		}
		else
			snprintf(res, 64, "%s(%d)", fieldstr, precision);
	}
	else
		snprintf(res, 64, "%s", fieldstr);

	PG_RETURN_CSTRING(res);
}

/*
 * Given an interval typmod value, return a code for the least-significant
 * field that the typmod allows to be nonzero, for instance given
 * INTERVAL DAY TO HOUR we want to identify "hour".
 *
 * The results should be ordered by field significance, which means
 * we can't use the dt.h macros YEAR etc, because for some odd reason
 * they aren't ordered that way.  Instead, arbitrarily represent
 * SECOND = 0, MINUTE = 1, HOUR = 2, DAY = 3, MONTH = 4, YEAR = 5.
 */
static int
intervaltypmodleastfield(int32 typmod)
{
	if (typmod < 0)
		return 0;				/* SECOND */

	switch (INTERVAL_RANGE(typmod))
	{
		case INTERVAL_MASK(YEAR):
			return 5;			/* YEAR */
		case INTERVAL_MASK(MONTH):
			return 4;			/* MONTH */
		case INTERVAL_MASK(DAY):
			return 3;			/* DAY */
		case INTERVAL_MASK(HOUR):
			return 2;			/* HOUR */
		case INTERVAL_MASK(MINUTE):
			return 1;			/* MINUTE */
		case INTERVAL_MASK(SECOND):
			return 0;			/* SECOND */
		case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
			return 4;			/* MONTH */
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
			return 2;			/* HOUR */
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			return 1;			/* MINUTE */
		case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			return 0;			/* SECOND */
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
			return 1;			/* MINUTE */
		case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			return 0;			/* SECOND */
		case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
			return 0;			/* SECOND */
		case INTERVAL_FULL_RANGE:
			return 0;			/* SECOND */
		default:
			elog(ERROR, "invalid INTERVAL typmod: 0x%x", typmod);
			break;
	}
	return 0;					/* can't get here, but keep compiler quiet */
}


/* interval_transform()
 * Flatten superfluous calls to interval_scale().  The interval typmod is
 * complex to permit accepting and regurgitating all SQL standard variations.
 * For truncation purposes, it boils down to a single, simple granularity.
 */
Datum
interval_transform(PG_FUNCTION_ARGS)
{
	FuncExpr   *expr = castNode(FuncExpr, PG_GETARG_POINTER(0));
	Node	   *ret = NULL;
	Node	   *typmod;

	Assert(list_length(expr->args) >= 2);

	typmod = (Node *) lsecond(expr->args);

	if (IsA(typmod, Const) &&!((Const *) typmod)->constisnull)
	{
		Node	   *source = (Node *) linitial(expr->args);
		int32		new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
		bool		noop;

		if (new_typmod < 0)
			noop = true;
		else
		{
			int32		old_typmod = exprTypmod(source);
			int			old_least_field;
			int			new_least_field;
			int			old_precis;
			int			new_precis;

			old_least_field = intervaltypmodleastfield(old_typmod);
			new_least_field = intervaltypmodleastfield(new_typmod);
			if (old_typmod < 0)
				old_precis = INTERVAL_FULL_PRECISION;
			else
				old_precis = INTERVAL_PRECISION(old_typmod);
			new_precis = INTERVAL_PRECISION(new_typmod);

			if (ORA_MODE)
			{
				/* Pickup true second precision */
				if (old_precis != INTERVAL_FULL_PRECISION)
					old_precis = INTERVAL_SEC_PRECISION(old_precis);
				else
					old_precis = INTERVAL_FULL_SEC_PRECISION;

				if (new_precis != INTERVAL_FULL_PRECISION)
					new_precis = INTERVAL_SEC_PRECISION(new_precis);
				else
					new_precis = INTERVAL_FULL_SEC_PRECISION;

				if (new_precis == INTERVAL_FULL_SEC_PRECISION)
					new_precis = MAX_INTERVAL_PRECISION;
				if (old_precis == INTERVAL_FULL_SEC_PRECISION)
					old_precis = MAX_INTERVAL_PRECISION;
			}

			/*
			 * Cast is a no-op if least field stays the same or decreases
			 * while precision stays the same or increases.  But precision,
			 * which is to say, sub-second precision, only affects ranges that
			 * include SECOND.
			 */
			noop = (new_least_field <= old_least_field) &&
				(old_least_field > 0 /* SECOND */ ||
				 new_precis >= MAX_INTERVAL_PRECISION ||
				 new_precis >= old_precis);

			if (ORA_MODE && noop)
			{
				if (INTERVAL_RANGE(new_typmod) & INTERVAL_MASK(DAY))
				{
					/* restore new_precis */
					new_precis = INTERVAL_PRECISION(new_typmod);
					new_precis = INTERVAL_DAY_PRECISION(new_precis);
					if (new_precis < MAX_INTERVAL_DAY_PRECISION)
						noop = false;
				}

				if (noop)
				{
					if (INTERVAL_RANGE(new_typmod) & INTERVAL_MASK(YEAR))
					{
						/* restore new_precis */
						new_precis = INTERVAL_PRECISION(new_typmod);
						new_precis = INTERVAL_YEAR_PRECISION(new_precis);
						if (new_precis < MAX_INTERVAL_YEAR_PRECISION)
							noop = false;
					}
				}
			}
			new_precis = INTERVAL_PRECISION(new_typmod);
		}
		if (noop)
			ret = relabel_to_typmod(source, new_typmod);
	}

	PG_RETURN_POINTER(ret);
}

/* interval_scale()
 * Adjust interval type for specified fields.
 * Used by PostgreSQL type system to stuff columns.
 */
Datum
interval_scale(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	int32		typmod = PG_GETARG_INT32(1);
	Interval   *result;

	result = palloc(sizeof(Interval));
	*result = *interval;

	AdjustIntervalForTypmod(result, typmod);

	PG_RETURN_INTERVAL_P(result);
}

static void
CheckIntervalPrecision(Interval *interval, int precision, int range)
{
	struct pg_tm tt,
				*tm = &tt;
	fsec_t	fsec;
	int	year = 0;
	int	mon = 0;
	int	mday = 0;
	int	hour = 0;
	int	min = 0;
	int	sec = 0;

	if (interval2tm(*interval, tm, &fsec) != 0)
		elog(ERROR, "could not convert interval to tm");

	year = tm->tm_year;
	mon = tm->tm_mon;
	mday = tm->tm_mday;
	hour = tm->tm_hour;
	min = tm->tm_min;
	sec = tm->tm_sec;

	if (sec >= SECS_PER_MINUTE || sec + SECS_PER_MINUTE <= 0)
	{
		min += (sec / SECS_PER_MINUTE);
		sec %= SECS_PER_MINUTE;
	}

	if (min >= MINS_PER_HOUR || min + MINS_PER_HOUR <= 0)
	{
		hour += (min / MINS_PER_HOUR);
		min %= MINS_PER_HOUR;
	}

	if (hour >= HOURS_PER_DAY || hour + HOURS_PER_DAY <= 0)
	{
		mday += (hour / HOURS_PER_DAY);
		hour %= HOURS_PER_DAY;
	}

	if (mon >= MONTHS_PER_YEAR || mon + MONTHS_PER_YEAR <= 0)
	{
		year += (mon / MONTHS_PER_YEAR);
		mon %= MONTHS_PER_YEAR;
	}

	if (range & (INTERVAL_MASK(YEAR)| INTERVAL_MASK(MONTH)))
	{
		int	year_prec = INTERVAL_YEAR_PRECISION(precision);
		if (year_prec != INTERVAL_FULL_YEAR_PRECISION)
		{
			int	ndigits = 1;
			int temp_year = year;
			if (year_prec < 0 || year_prec > MAX_INTERVAL_YEAR_PRECISION)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("interval YEAR(%d) precision must be between %d and %d",
								year_prec, 1, MAX_INTERVAL_YEAR_PRECISION)));

			while ((temp_year = temp_year / 10) != 0)
				ndigits++;
			if (year != 0 && year_prec < ndigits)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("interval YEAR(%d) precision is too small for input interval year %d",
								year_prec, year)));
		}
	}

	if (range & (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
		INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)))
	{
		int	day_prec = INTERVAL_DAY_PRECISION(precision);
		if (day_prec != INTERVAL_FULL_DAY_PRECISION)
		{
			int	ndigits = 1;
			int temp_day = mday;
			if (day_prec < 0 || day_prec > MAX_INTERVAL_DAY_PRECISION)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("interval DAY(%d) precision must be between %d and %d",
								day_prec, 1, MAX_INTERVAL_DAY_PRECISION)));

			while ((temp_day = temp_day / 10) != 0)
				ndigits++;
			if (mday != 0 && day_prec < ndigits)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("interval DAY(%d) precision is too small for input interval DAY %d",
								day_prec, mday)));
		}
	}
}

void
CheckIntervalTypmod(int32 exprTypMod, int32 targetTypMod)
{
	int		expr_range = INTERVAL_RANGE(exprTypMod);
	int		target_range = INTERVAL_RANGE(targetTypMod);
	int		year_mon = (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH));
	int		day_sec = (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) |
						INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND));
	if (((target_range & year_mon) && !(expr_range & year_mon)) ||
		((target_range & day_sec) && !(expr_range & day_sec)))
	{
		char *expr_type_str = format_type_with_typemod(INTERVALOID, exprTypMod);
		char *target_type_str = format_type_with_typemod(INTERVALOID, targetTypMod);
		elog(ERROR, "%s does not match %s", expr_type_str, target_type_str);
	}
}

/*
 *	Adjust interval for specified precision, in both YEAR to SECOND
 *	range and sub-second precision.
 */
void
AdjustIntervalForTypmod(Interval *interval, int32 typmod)
{
	static const int64 IntervalScales[MAX_INTERVAL_PRECISION + 1] = {
		INT64CONST(1000000),
		INT64CONST(100000),
		INT64CONST(10000),
		INT64CONST(1000),
		INT64CONST(100),
		INT64CONST(10),
		INT64CONST(1)
	};

	static const int64 IntervalOffsets[MAX_INTERVAL_PRECISION + 1] = {
		INT64CONST(500000),
		INT64CONST(50000),
		INT64CONST(5000),
		INT64CONST(500),
		INT64CONST(50),
		INT64CONST(5),
		INT64CONST(0)
	};

	/*
	 * Unspecified range and precision? Then not necessary to adjust. Setting
	 * typmod to -1 is the convention for all data types.
	 */
	if (typmod >= 0)
	{
		int			range = INTERVAL_RANGE(typmod);
		int			precision = INTERVAL_PRECISION(typmod);

		/*
		 * Our interpretation of intervals with a limited set of fields is
		 * that fields to the right of the last one specified are zeroed out,
		 * but those to the left of it remain valid.  Thus for example there
		 * is no operational difference between INTERVAL YEAR TO MONTH and
		 * INTERVAL MONTH.  In some cases we could meaningfully enforce that
		 * higher-order fields are zero; for example INTERVAL DAY could reject
		 * nonzero "month" field.  However that seems a bit pointless when we
		 * can't do it consistently.  (We cannot enforce a range limit on the
		 * highest expected field, since we do not have any equivalent of
		 * SQL's <interval leading field precision>.)  If we ever decide to
		 * revisit this, interval_transform will likely require adjusting.
		 *
		 * Note: before PG 8.4 we interpreted a limited set of fields as
		 * actually causing a "modulo" operation on a given value, potentially
		 * losing high-order as well as low-order information.  But there is
		 * no support for such behavior in the standard, and it seems fairly
		 * undesirable on data consistency grounds anyway.  Now we only
		 * perform truncation or rounding of low-order fields.
		 */
		if (range == INTERVAL_FULL_RANGE)
		{
			/* Do nothing... */
		}
		else if (range == INTERVAL_MASK(YEAR))
		{
			interval->month = (interval->month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
			interval->day = 0;
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(MONTH))
		{
			interval->day = 0;
			interval->time = 0;
		}
		/* YEAR TO MONTH */
		else if (range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)))
		{
			interval->day = 0;
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(DAY))
		{
			interval->time = 0;
		}
		else if (range == INTERVAL_MASK(HOUR))
		{
			interval->time = (interval->time / USECS_PER_HOUR) *
				USECS_PER_HOUR;
		}
		else if (range == INTERVAL_MASK(MINUTE))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		else if (range == INTERVAL_MASK(SECOND))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* DAY TO HOUR */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR)))
		{
			interval->time = (interval->time / USECS_PER_HOUR) *
				USECS_PER_HOUR;
		}
		/* DAY TO MINUTE */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE)))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		/* DAY TO SECOND */
		else if (range == (INTERVAL_MASK(DAY) |
						   INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* HOUR TO MINUTE */
		else if (range == (INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE)))
		{
			interval->time = (interval->time / USECS_PER_MINUTE) *
				USECS_PER_MINUTE;
		}
		/* HOUR TO SECOND */
		else if (range == (INTERVAL_MASK(HOUR) |
						   INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		/* MINUTE TO SECOND */
		else if (range == (INTERVAL_MASK(MINUTE) |
						   INTERVAL_MASK(SECOND)))
		{
			/* fractional-second rounding will be dealt with below */
		}
		else
			elog(ERROR, "unrecognized interval typmod: %d", typmod);

		/* Need to adjust sub-second precision? */
		if (precision != INTERVAL_FULL_PRECISION)
		{
			if (ORA_MODE)
			{
				CheckIntervalPrecision(interval, precision, range);

				if (!(range & INTERVAL_MASK(SECOND)))
				{
					/*
					 * Check DAY/YEAR precision is enough for current input
					 * interval.
					 */
					return;
				}

				/*
				 * Pickup the true precision of second.
				 */
				precision = INTERVAL_SEC_PRECISION(precision);
				if (precision == INTERVAL_FULL_SEC_PRECISION)
					return;
			}
			if (precision < 0 || precision > MAX_INTERVAL_PRECISION)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("interval(%d) precision must be between %d and %d",
								precision, 0, MAX_INTERVAL_PRECISION)));

			if (interval->time >= INT64CONST(0))
			{
				interval->time = ((interval->time +
								   IntervalOffsets[precision]) /
								  IntervalScales[precision]) *
					IntervalScales[precision];
			}
			else
			{
				interval->time = -(((-interval->time +
									 IntervalOffsets[precision]) /
									IntervalScales[precision]) *
								   IntervalScales[precision]);
			}
		}
	}
}

/*
 * make_interval - numeric Interval constructor
 */
Datum
make_interval(PG_FUNCTION_ARGS)
{
	int32		years = PG_GETARG_INT32(0);
	int32		months = PG_GETARG_INT32(1);
	int32		weeks = PG_GETARG_INT32(2);
	int32		days = PG_GETARG_INT32(3);
	int32		hours = PG_GETARG_INT32(4);
	int32		mins = PG_GETARG_INT32(5);
	double		secs = PG_GETARG_FLOAT8(6);
	Interval   *result;

	/*
	 * Reject out-of-range inputs.  We really ought to check the integer
	 * inputs as well, but it's not entirely clear what limits to apply.
	 */
	if (isinf(secs) || isnan(secs))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result = (Interval *) palloc(sizeof(Interval));
	result->month = years * MONTHS_PER_YEAR + months;
	result->day = weeks * 7 + days;

	secs = rint(secs * USECS_PER_SEC);
	result->time = hours * ((int64) SECS_PER_HOUR * USECS_PER_SEC) +
		mins * ((int64) SECS_PER_MINUTE * USECS_PER_SEC) +
		(int64) secs;

	PG_RETURN_INTERVAL_P(result);
}

/* EncodeSpecialTimestamp()
 * Convert reserved timestamp data type to string.
 */
void
EncodeSpecialTimestamp(Timestamp dt, char *str)
{
	if (TIMESTAMP_IS_NOBEGIN(dt))
		strcpy(str, EARLY);
	else if (TIMESTAMP_IS_NOEND(dt))
		strcpy(str, LATE);
	else						/* shouldn't happen */
		elog(ERROR, "invalid argument for EncodeSpecialTimestamp");
}

Datum
now(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(GetCurrentTransactionStartTimestamp());
}

/*
 * Description: Return the last day of the month
 * return: convert result.
 */
Datum
last_day(PG_FUNCTION_ARGS)
{
	Timestamp     timestamp = PG_GETARG_TIMESTAMP(0);
	Timestamp     result;
	struct pg_tm  tt;
	struct pg_tm *tm = &tt;
	fsec_t        fsec = 0;

	if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
		         errmsg("timestamp out of range.")));
	}

	/* Set day to this month's last day.*/
	tm->tm_mday = day_tab[isleap(tm->tm_year)][tm->tm_mon - 1];

	if (tm2timestamp(tm, fsec, NULL, &result) != 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
		         errmsg("timestamp out of range.")));
	}

	PG_RETURN_TIMESTAMP(result);
}

/*
 * Description: Return the last day of the month for timestamptz
 * return: convert result.
 */
Datum
last_day_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp   result;
	TimestampTz timestamptz = PG_GETARG_TIMESTAMPTZ(0);
	Datum       timestamp = timestamptz2timestamp(timestamptz);

	result = DatumGetTimestamp(DirectFunctionCall1(last_day, TimestampGetDatum(timestamp)));
	PG_RETURN_TIMESTAMPTZ(timestamp2timestamptz(result));
}

Datum
statement_timestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(GetCurrentStatementStartTimestamp());
}

Datum
clock_timestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(GetCurrentTimestamp());
}

Datum
pg_postmaster_start_time(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(PgStartTime);
}

Datum
pg_conf_load_time(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(PgReloadTime);
}

Datum
pg_systimestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(GetCurrentTimestamp());
}

/*
 * GetCurrentTimestamp -- get the current operating system time
 *
 * Result is in the form of a TimestampTz value, and is expressed to the
 * full precision of the gettimeofday() syscall
 */
TimestampTz
GetCurrentTimestamp(void)
{
	TimestampTz result;
	struct timeval tp;

	gettimeofday(&tp, NULL);

	result = (TimestampTz) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
	result = (result * USECS_PER_SEC) + tp.tv_usec;

	return result;
}

/*
 * GetSQLCurrentTimestamp -- implements CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(n)
 */
TimestampTz
GetSQLCurrentTimestamp(int32 typmod)
{
	TimestampTz ts;

	ts = GetCurrentTransactionStartTimestamp();
	if (typmod >= 0)
		AdjustTimestampForTypmod(&ts, typmod);
	return ts;
}

/*
 * GetSQLLocalTimestamp -- implements LOCALTIMESTAMP, LOCALTIMESTAMP(n)
 */
Timestamp
GetSQLLocalTimestamp(int32 typmod)
{
	Timestamp	ts;

	ts = timestamptz2timestamp(GetCurrentTransactionStartTimestamp());
	if (typmod >= 0)
		AdjustTimestampForTypmod(&ts, typmod);
	return ts;
}

#ifdef _PG_ORCL_
static Timestamp
GetSysdate(void)
{
	TimestampTz timestamptz = GetCurrentTransactionStartTimestamp();
	Timestamp timestamp;

	timestamp = timestamptz2timestamp(timestamptz);
	/* opentenbase_ora date type has no fractional seconds */
	timestamp = timestamp / MICROSEC_PER_SEC * MICROSEC_PER_SEC;

	return timestamp;
}
/*
 * opentenbase_ora constant/keyword sysdate return a datetime with no fractional seconds
 * according to the system timezone, which is probably different from that of
 * the database datetime, particulartly in China (PRC, +08:00).
 *
 * Starting with v5.21.2, the opentenbase_ora keyword SYSDATE corresponds to different
 * implementations in PG and opentenbase_ora mode. SYSDATE remains the same in PG mode
 * because of current user requirements, actually returning a PG timestamp,
 * while the one in opentenbase_ora mode returns a DateOrcl (see sysdate_orcl() in
 * orcl_datetime_func.c) to be consistent with opentenbase_ora.
 */
Datum
orcl_sysdate(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(GetSysdate());
}

#endif

/*
 * TimestampDifference -- convert the difference between two timestamps
 *		into integer seconds and microseconds
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentTimestamp()).
 *
 * We expect start_time <= stop_time.  If not, we return zeros; for current
 * callers there is no need to be tense about which way division rounds on
 * negative inputs.
 */
void
TimestampDifference(TimestampTz start_time, TimestampTz stop_time,
					long *secs, int *microsecs)
{
	TimestampTz diff = stop_time - start_time;

	if (diff <= 0)
	{
		*secs = 0;
		*microsecs = 0;
	}
	else
	{
		*secs = (long) (diff / USECS_PER_SEC);
		*microsecs = (int) (diff % USECS_PER_SEC);
	}
}

/*
 * TimestampDifferenceExceeds -- report whether the difference between two
 *		timestamps is >= a threshold (expressed in milliseconds)
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentTimestamp()).
 */
bool
TimestampDifferenceExceeds(TimestampTz start_time,
						   TimestampTz stop_time,
						   int msec)
{
	TimestampTz diff = stop_time - start_time;

	return (diff >= msec * INT64CONST(1000));
}

/*
 * Convert a time_t to TimestampTz.
 *
 * We do not use time_t internally in Postgres, but this is provided for use
 * by functions that need to interpret, say, a stat(2) result.
 *
 * To avoid having the function's ABI vary depending on the width of time_t,
 * we declare the argument as pg_time_t, which is cast-compatible with
 * time_t but always 64 bits wide (unless the platform has no 64-bit type).
 * This detail should be invisible to callers, at least at source code level.
 */
TimestampTz
time_t_to_timestamptz(pg_time_t tm)
{
	TimestampTz result;

	result = (TimestampTz) tm -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
	result *= USECS_PER_SEC;

	return result;
}

/*
 * Convert a TimestampTz to time_t.
 *
 * This too is just marginally useful, but some places need it.
 *
 * To avoid having the function's ABI vary depending on the width of time_t,
 * we declare the result as pg_time_t, which is cast-compatible with
 * time_t but always 64 bits wide (unless the platform has no 64-bit type).
 * This detail should be invisible to callers, at least at source code level.
 */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

	result = (pg_time_t) (t / USECS_PER_SEC +
						  ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));

	return result;
}

/*
 * Produce a C-string representation of a TimestampTz.
 *
 * This is mostly for use in emitting messages.  The primary difference
 * from timestamptz_out is that we force the output format to ISO.  Note
 * also that the result is in a static buffer, not pstrdup'd.
 */
const char *
timestamptz_to_str(TimestampTz t)
{
	static char buf[MAXDATELEN + 1];
	int			tz;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	const char *tzn;

	if (TIMESTAMP_NOT_FINITE(t))
		EncodeSpecialTimestamp(t, buf);
	else if (timestamp2tm(t, &tz, tm, &fsec, &tzn, NULL) == 0)
		EncodeDateTime(tm, fsec, true, tz, tzn, USE_ISO_DATES, buf);
	else
		strlcpy(buf, "(timestamp out of range)", sizeof(buf));

	return buf;
}


void
dt2time(Timestamp jd, int *hour, int *min, int *sec, fsec_t *fsec)
{
	TimeOffset	time;

	time = jd;

	*hour = time / USECS_PER_HOUR;
	time -= (*hour) * USECS_PER_HOUR;
	*min = time / USECS_PER_MINUTE;
	time -= (*min) * USECS_PER_MINUTE;
	*sec = time / USECS_PER_SEC;
	if (fsec)
		*fsec = time - (*sec * USECS_PER_SEC);
}								/* dt2time() */


/*
 * timestamp2tm() - Convert timestamp data type to POSIX time structure.
 *
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 * Returns:
 *	 0 on success
 *	-1 on out of range
 *
 * If attimezone is NULL, the global timezone setting will be used.
 */
int
timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec, const char **tzn, pg_tz *attimezone)
{
	Timestamp	date;
	Timestamp	time;
	pg_time_t	utime;

	/* Use session timezone if caller asks for default */
	if (attimezone == NULL)
		attimezone = session_timezone;

	time = dt;
	TMODULO(time, date, USECS_PER_DAY);

	if (time < INT64CONST(0))
	{
		time += USECS_PER_DAY;
		date -= 1;
	}

	/* add offset to go from J2000 back to standard Julian date */
	date += POSTGRES_EPOCH_JDATE;

	/* Julian day routine does not work for negative Julian days */
	if (date < 0 || date > (Timestamp) INT_MAX)
		return -1;

	j2date((int) date, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
	dt2time(time, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, fsec);

	/* Done if no TZ conversion wanted */
	if (tzp == NULL)
	{
		tm->tm_isdst = -1;
		tm->tm_gmtoff = 0;
		tm->tm_zone = NULL;
		if (tzn != NULL)
			*tzn = NULL;
		return 0;
	}

	/*
	 * If the time falls within the range of pg_time_t, use pg_localtime() to
	 * rotate to the local time zone.
	 *
	 * First, convert to an integral timestamp, avoiding possibly
	 * platform-specific roundoff-in-wrong-direction errors, and adjust to
	 * Unix epoch.  Then see if we can convert to pg_time_t without loss. This
	 * coding avoids hardwiring any assumptions about the width of pg_time_t,
	 * so it should behave sanely on machines without int64.
	 */
	dt = (dt - *fsec) / USECS_PER_SEC +
		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;
	utime = (pg_time_t) dt;
	if ((Timestamp) utime == dt)
	{
		struct pg_tm *tx = pg_localtime(&utime, attimezone);

		tm->tm_year = tx->tm_year + 1900;
		tm->tm_mon = tx->tm_mon + 1;
		tm->tm_mday = tx->tm_mday;
		tm->tm_hour = tx->tm_hour;
		tm->tm_min = tx->tm_min;
		tm->tm_sec = tx->tm_sec;
		tm->tm_isdst = tx->tm_isdst;
		tm->tm_gmtoff = tx->tm_gmtoff;
		tm->tm_zone = tx->tm_zone;
		*tzp = -tm->tm_gmtoff;
		if (tzn != NULL)
			*tzn = tm->tm_zone;
	}
	else
	{
		/*
		 * When out of range of pg_time_t, treat as GMT
		 */
		*tzp = 0;
		/* Mark this as *no* time zone available */
		tm->tm_isdst = -1;
		tm->tm_gmtoff = 0;
		tm->tm_zone = NULL;
		if (tzn != NULL)
			*tzn = NULL;
	}

	return 0;
}


/* tm2timestamp()
 * Convert a tm structure to a timestamp data type.
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 *
 * Returns -1 on failure (value out of range).
 */
int
tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result)
{
	TimeOffset	date;
	TimeOffset	time;

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;
	time = time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);

	*result = date * USECS_PER_DAY + time;
	/* check for major overflow */
	if ((*result - time) / USECS_PER_DAY != date)
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
	/* check for just-barely overflow (okay except time-of-day wraps) */
	/* caution: we want to allow 1999-12-31 24:00:00 */
	if ((*result < 0 && date > 0) ||
		(*result > 0 && date < -1))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
	if (tzp != NULL)
		*result = dt2local(*result, -(*tzp));

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(*result))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	return 0;
}


/* interval2tm()
 * Convert an interval data type to a tm structure.
 */
int
interval2tm(Interval span, struct pg_tm *tm, fsec_t *fsec)
{
	TimeOffset	time;
	TimeOffset	tfrac;

	tm->tm_year = span.month / MONTHS_PER_YEAR;
	tm->tm_mon = span.month % MONTHS_PER_YEAR;
	tm->tm_mday = span.day;
	time = span.time;

	tfrac = time / USECS_PER_HOUR;
	time -= tfrac * USECS_PER_HOUR;
	tm->tm_hour = tfrac;
	if (!SAMESIGN(tm->tm_hour, tfrac))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
	tfrac = time / USECS_PER_MINUTE;
	time -= tfrac * USECS_PER_MINUTE;
	tm->tm_min = tfrac;
	tfrac = time / USECS_PER_SEC;
	*fsec = time - (tfrac * USECS_PER_SEC);
	tm->tm_sec = tfrac;

	return 0;
}

int
tm2interval(struct pg_tm *tm, fsec_t fsec, Interval *span)
{
	double		total_months = (double) tm->tm_year * MONTHS_PER_YEAR + tm->tm_mon;

	if (total_months > INT_MAX || total_months < INT_MIN)
		return -1;
	span->month = total_months;
	span->day = tm->tm_mday;
	span->time = (((((tm->tm_hour * INT64CONST(60)) +
					 tm->tm_min) * INT64CONST(60)) +
				   tm->tm_sec) * USECS_PER_SEC) + fsec;

	return 0;
}

static TimeOffset
time2t(const int hour, const int min, const int sec, const fsec_t fsec)
{
	return (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) * USECS_PER_SEC) + fsec;
}

Timestamp
dt2local(Timestamp dt, int tz)
{
	dt -= (tz * USECS_PER_SEC);
	return dt;
}


/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/


Datum
timestamp_finite(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);

	PG_RETURN_BOOL(!TIMESTAMP_NOT_FINITE(timestamp));
}

Datum
interval_finite(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(true);
}


/*----------------------------------------------------------
 *	Relational operators for timestamp.
 *---------------------------------------------------------*/

void
GetEpochTime(struct pg_tm *tm)
{
	struct pg_tm *t0;
	pg_time_t	epoch = 0;

	t0 = pg_gmtime(&epoch);

	tm->tm_year = t0->tm_year;
	tm->tm_mon = t0->tm_mon;
	tm->tm_mday = t0->tm_mday;
	tm->tm_hour = t0->tm_hour;
	tm->tm_min = t0->tm_min;
	tm->tm_sec = t0->tm_sec;

	tm->tm_year += 1900;
	tm->tm_mon++;
}

Timestamp
SetEpochTimestamp(void)
{
	Timestamp	dt;
	struct pg_tm tt,
			   *tm = &tt;

	GetEpochTime(tm);
	/* we don't bother to test for failure ... */
    (void) tm2timestamp(tm, 0, NULL, &dt);

	return dt;
}								/* SetEpochTimestamp() */

/*
 * We are currently sharing some code between timestamp and timestamptz.
 * The comparison functions are among them. - thomas 2001-09-25
 *
 *		timestamp_relop - is timestamp1 relop timestamp2
 */
int
timestamp_cmp_internal(Timestamp dt1, Timestamp dt2)
{
	return (dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0);
}

Datum
timestamp_eq(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) == 0);
}

Datum
timestamp_ne(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) != 0);
}

Datum
timestamp_lt(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) < 0);
}

Datum
timestamp_gt(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) > 0);
}

Datum
timestamp_le(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) <= 0);
}

Datum
timestamp_ge(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) >= 0);
}

Datum
timestamp_cmp(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);

	PG_RETURN_INT32(timestamp_cmp_internal(dt1, dt2));
}

/* note: this is used for timestamptz also */
int
timestamp_fastcmp(Datum x, Datum y, SortSupport ssup)
{
	Timestamp	a = DatumGetTimestamp(x);
	Timestamp	b = DatumGetTimestamp(y);

	return timestamp_cmp_internal(a, b);
}

Datum
timestamp_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = timestamp_fastcmp;
	PG_RETURN_VOID();
}

Datum
timestamp_hash(PG_FUNCTION_ARGS)
{
	return hashint8(fcinfo);
}

Datum
timestamp_hash_new(PG_FUNCTION_ARGS)
{
	return hashint8new(fcinfo);
}

Datum
timestamp_hash_extended(PG_FUNCTION_ARGS)
{
	return hashint8extended(fcinfo);
}

/*
 * Cross-type comparison functions for timestamp vs timestamptz
 */

Datum
timestamp_eq_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) == 0);
}

Datum
timestamp_ne_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) != 0);
}

Datum
timestamp_lt_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) < 0);
}

Datum
timestamp_gt_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) > 0);
}

Datum
timestamp_le_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) <= 0);
}

Datum
timestamp_ge_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) >= 0);
}

Datum
timestamp_cmp_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz dt1;

	dt1 = timestamp2timestamptz(timestampVal);

	PG_RETURN_INT32(timestamp_cmp_internal(dt1, dt2));
}

Datum
timestamptz_eq_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) == 0);
}

Datum
timestamptz_ne_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) != 0);
}

Datum
timestamptz_lt_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) < 0);
}

Datum
timestamptz_gt_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) > 0);
}

Datum
timestamptz_le_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) <= 0);
}

Datum
timestamptz_ge_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_BOOL(timestamp_cmp_internal(dt1, dt2) >= 0);
}

Datum
timestamptz_cmp_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	Timestamp	timestampVal = PG_GETARG_TIMESTAMP(1);
	TimestampTz dt2;

	dt2 = timestamp2timestamptz(timestampVal);

	PG_RETURN_INT32(timestamp_cmp_internal(dt1, dt2));
}

/*
 * in_range support functions for timestamps and intervals.
 *
 * Per SQL spec, we support these with interval as the offset type.
 * The spec's restriction that the offset not be negative is a bit hard to
 * decipher for intervals, but we choose to interpret it the same as our
 * interval comparison operators would.
 */

Datum
in_range_timestamptz_interval(PG_FUNCTION_ARGS)
{
	TimestampTz val = PG_GETARG_TIMESTAMPTZ(0);
	TimestampTz base = PG_GETARG_TIMESTAMPTZ(1);
	Interval   *offset = PG_GETARG_INTERVAL_P(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	TimestampTz sum;

	if (int128_compare(interval_cmp_value(offset), int64_to_int128(0)) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	/* We don't currently bother to avoid overflow hazards here */
	if (sub)
		sum = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_mi_interval,
													  TimestampTzGetDatum(base),
													  IntervalPGetDatum(offset)));
	else
		sum = DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
													  TimestampTzGetDatum(base),
													  IntervalPGetDatum(offset)));

	if (less)
		PG_RETURN_BOOL(val <= sum);
	else
		PG_RETURN_BOOL(val >= sum);
}

Datum
in_range_timestamp_interval(PG_FUNCTION_ARGS)
{
	Timestamp	val = PG_GETARG_TIMESTAMP(0);
	Timestamp	base = PG_GETARG_TIMESTAMP(1);
	Interval   *offset = PG_GETARG_INTERVAL_P(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	Timestamp	sum;

	if (int128_compare(interval_cmp_value(offset), int64_to_int128(0)) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	/* We don't currently bother to avoid overflow hazards here */
	if (sub)
		sum = DatumGetTimestamp(DirectFunctionCall2(timestamp_mi_interval,
													TimestampGetDatum(base),
													IntervalPGetDatum(offset)));
	else
		sum = DatumGetTimestamp(DirectFunctionCall2(timestamp_pl_interval,
													TimestampGetDatum(base),
													IntervalPGetDatum(offset)));

	if (less)
		PG_RETURN_BOOL(val <= sum);
	else
		PG_RETURN_BOOL(val >= sum);
}

Datum
in_range_interval_interval(PG_FUNCTION_ARGS)
{
	Interval   *val = PG_GETARG_INTERVAL_P(0);
	Interval   *base = PG_GETARG_INTERVAL_P(1);
	Interval   *offset = PG_GETARG_INTERVAL_P(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	Interval   *sum;

	if (int128_compare(interval_cmp_value(offset), int64_to_int128(0)) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	/* We don't currently bother to avoid overflow hazards here */
	if (sub)
		sum = DatumGetIntervalP(DirectFunctionCall2(interval_mi,
													IntervalPGetDatum(base),
													IntervalPGetDatum(offset)));
	else
		sum = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
													IntervalPGetDatum(base),
													IntervalPGetDatum(offset)));

	if (less)
		PG_RETURN_BOOL(interval_cmp_internal(val, sum) <= 0);
	else
		PG_RETURN_BOOL(interval_cmp_internal(val, sum) >= 0);
}

/*
 *		interval_relop	- is interval1 relop interval2
 *
 * Interval comparison is based on converting interval values to a linear
 * representation expressed in the units of the time field (microseconds,
 * in the case of integer timestamps) with days assumed to be always 24 hours
 * and months assumed to be always 30 days.  To avoid overflow, we need a
 * wider-than-int64 datatype for the linear representation, so use INT128.
 */

inline INT128
interval_cmp_value(const Interval *interval)
{
	INT128		span;
	int64		dayfraction;
	int64		days;

	/*
	 * Separate time field into days and dayfraction, then add the month and
	 * day fields to the days part.  We cannot overflow int64 days here.
	 */
	dayfraction = interval->time % USECS_PER_DAY;
	days = interval->time / USECS_PER_DAY;
	days += interval->month * INT64CONST(30);
	days += interval->day;

	/* Widen dayfraction to 128 bits */
	span = int64_to_int128(dayfraction);

	/* Scale up days to microseconds, forming a 128-bit product */
	int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

	return span;
}

int
interval_cmp_internal(Interval *interval1, Interval *interval2)
{
	INT128		span1 = interval_cmp_value(interval1);
	INT128		span2 = interval_cmp_value(interval2);

	return int128_compare(span1, span2);
}

Datum
interval_eq(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) == 0);
}

Datum
interval_ne(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) != 0);
}

Datum
interval_lt(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) < 0);
}

Datum
interval_gt(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) > 0);
}

Datum
interval_le(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) <= 0);
}

Datum
interval_ge(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_BOOL(interval_cmp_internal(interval1, interval2) >= 0);
}

Datum
interval_cmp(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_INT32(interval_cmp_internal(interval1, interval2));
}

/*
 * Hashing for intervals
 *
 * We must produce equal hashvals for values that interval_cmp_internal()
 * considers equal.  So, compute the net span the same way it does,
 * and then hash that.
 */
Datum
interval_hash(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	INT128		span = interval_cmp_value(interval);
	int64		span64;

	/*
	 * Use only the least significant 64 bits for hashing.  The upper 64 bits
	 * seldom add any useful information, and besides we must do it like this
	 * for compatibility with hashes calculated before use of INT128 was
	 * introduced.
	 */
	span64 = int128_to_int64(span);

	return DirectFunctionCall1(hashint8, Int64GetDatumFast(span64));
}

/*
 * Hashing for intervals
 *
 * We must produce equal hashvals for values that interval_cmp_internal()
 * considers equal.  So, compute the net span the same way it does,
 * and then hash that.
 */
Datum
interval_hash_new(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	INT128		span = interval_cmp_value(interval);
	int64		span64;

	/*
	 * Use only the least significant 64 bits for hashing.  The upper 64 bits
	 * seldom add any useful information, and besides we must do it like this
	 * for compatibility with hashes calculated before use of INT128 was
	 * introduced.
	 */
	span64 = int128_to_int64(span);

	return DirectFunctionCall1(hashint8new, Int64GetDatumFast(span64));
}

Datum
interval_hash_extended(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	INT128		span = interval_cmp_value(interval);
	int64		span64;

	/* Same approach as interval_hash */
	span64 = int128_to_int64(span);

	return DirectFunctionCall2(hashint8extended, Int64GetDatumFast(span64),
							   PG_GETARG_DATUM(1));
}

/* overlaps_timestamp() --- implements the SQL OVERLAPS operator.
 *
 * Algorithm is per SQL spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
Datum
overlaps_timestamp(PG_FUNCTION_ARGS)
{
	/*
	 * The arguments are Timestamps, but we leave them as generic Datums to
	 * avoid unnecessary conversions between value and reference forms --- not
	 * to mention possible dereferences of null pointers.
	 */
	Datum		ts1 = PG_GETARG_DATUM(0);
	Datum		te1 = PG_GETARG_DATUM(1);
	Datum		ts2 = PG_GETARG_DATUM(2);
	Datum		te2 = PG_GETARG_DATUM(3);
	bool		ts1IsNull = PG_ARGISNULL(0);
	bool		te1IsNull = PG_ARGISNULL(1);
	bool		ts2IsNull = PG_ARGISNULL(2);
	bool		te2IsNull = PG_ARGISNULL(3);

#define TIMESTAMP_GT(t1,t2) \
	DatumGetBool(DirectFunctionCall2(timestamp_gt,t1,t2))
#define TIMESTAMP_LT(t1,t2) \
	DatumGetBool(DirectFunctionCall2(timestamp_lt,t1,t2))

	/*
	 * If both endpoints of interval 1 are null, the result is null (unknown).
	 * If just one endpoint is null, take ts1 as the non-null one. Otherwise,
	 * take ts1 as the lesser endpoint.
	 */
	if (ts1IsNull)
	{
		if (te1IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts1 = te1;
		te1IsNull = true;
	}
	else if (!te1IsNull)
	{
		if (TIMESTAMP_GT(ts1, te1))
		{
			Datum		tt = ts1;

			ts1 = te1;
			te1 = tt;
		}
	}

	/* Likewise for interval 2. */
	if (ts2IsNull)
	{
		if (te2IsNull)
			PG_RETURN_NULL();
		/* swap null for non-null */
		ts2 = te2;
		te2IsNull = true;
	}
	else if (!te2IsNull)
	{
		if (TIMESTAMP_GT(ts2, te2))
		{
			Datum		tt = ts2;

			ts2 = te2;
			te2 = tt;
		}
	}

	/*
	 * At this point neither ts1 nor ts2 is null, so we can consider three
	 * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
	 */
	if (TIMESTAMP_GT(ts1, ts2))
	{
		/*
		 * This case is ts1 < te2 OR te1 < te2, which may look redundant but
		 * in the presence of nulls it's not quite completely so.
		 */
		if (te2IsNull)
			PG_RETURN_NULL();
		if (TIMESTAMP_LT(ts1, te2))
			PG_RETURN_BOOL(true);
		if (te1IsNull)
			PG_RETURN_NULL();

		/*
		 * If te1 is not null then we had ts1 <= te1 above, and we just found
		 * ts1 >= te2, hence te1 >= te2.
		 */
		PG_RETURN_BOOL(false);
	}
	else if (TIMESTAMP_LT(ts1, ts2))
	{
		/* This case is ts2 < te1 OR te2 < te1 */
		if (te1IsNull)
			PG_RETURN_NULL();
		if (TIMESTAMP_LT(ts2, te1))
			PG_RETURN_BOOL(true);
		if (te2IsNull)
			PG_RETURN_NULL();

		/*
		 * If te2 is not null then we had ts2 <= te2 above, and we just found
		 * ts2 >= te1, hence te2 >= te1.
		 */
		PG_RETURN_BOOL(false);
	}
	else
	{
		/*
		 * For ts1 = ts2 the spec says te1 <> te2 OR te1 = te2, which is a
		 * rather silly way of saying "true if both are non-null, else null".
		 */
		if (te1IsNull || te2IsNull)
			PG_RETURN_NULL();
		PG_RETURN_BOOL(true);
	}

#undef TIMESTAMP_GT
#undef TIMESTAMP_LT
}


/*----------------------------------------------------------
 *	"Arithmetic" operators on date/times.
 *---------------------------------------------------------*/

Datum
timestamp_smaller(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;

	/* use timestamp_cmp_internal to be sure this agrees with comparisons */
	if (timestamp_cmp_internal(dt1, dt2) < 0)
		result = dt1;
	else
		result = dt2;
	PG_RETURN_TIMESTAMP(result);
}

Datum
timestamp_larger(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;

	if (timestamp_cmp_internal(dt1, dt2) > 0)
		result = dt1;
	else
		result = dt2;
	PG_RETURN_TIMESTAMP(result);
}


Datum
timestamp_mi(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	if (TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("cannot subtract infinite timestamps")));

	result->time = dt1 - dt2;

	result->month = 0;
	result->day = 0;

	/*----------
	 *	This is wrong, but removing it breaks a lot of regression tests.
	 *	For example:
	 *
	 *	test=> SET timezone = 'EST5EDT';
	 *	test=> SELECT
	 *	test-> ('2005-10-30 13:22:00-05'::timestamptz -
	 *	test(>	'2005-10-29 13:22:00-04'::timestamptz);
	 *	?column?
	 *	----------------
	 *	 1 day 01:00:00
	 *	 (1 row)
	 *
	 *	so adding that to the first timestamp gets:
	 *
	 *	 test=> SELECT
	 *	 test-> ('2005-10-29 13:22:00-04'::timestamptz +
	 *	 test(> ('2005-10-30 13:22:00-05'::timestamptz -
	 *	 test(>  '2005-10-29 13:22:00-04'::timestamptz)) at time zone 'EST';
	 *		timezone
	 *	--------------------
	 *	2005-10-30 14:22:00
	 *	(1 row)
	 *----------
	 */
	result = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
												   IntervalPGetDatum(result)));

	PG_RETURN_INTERVAL_P(result);
}

/*
 *	interval_justify_interval()
 *
 *	Adjust interval so 'month', 'day', and 'time' portions are within
 *	customary bounds.  Specifically:
 *
 *		0 <= abs(time) < 24 hours
 *		0 <= abs(day)  < 30 days
 *
 *	Also, the sign bit on all three fields is made equal, so either
 *	all three fields are negative or all are positive.
 */
Datum
interval_justify_interval(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	Interval   *result;
	TimeOffset	wholeday;
	int32		wholemonth;

	result = (Interval *) palloc(sizeof(Interval));
	result->month = span->month;
	result->day = span->day;
	result->time = span->time;

	TMODULO(result->time, wholeday, USECS_PER_DAY);
	result->day += wholeday;	/* could overflow... */

	wholemonth = result->day / DAYS_PER_MONTH;
	result->day -= wholemonth * DAYS_PER_MONTH;
	result->month += wholemonth;

	if (result->month > 0 &&
		(result->day < 0 || (result->day == 0 && result->time < 0)))
	{
		result->day += DAYS_PER_MONTH;
		result->month--;
	}
	else if (result->month < 0 &&
			 (result->day > 0 || (result->day == 0 && result->time > 0)))
	{
		result->day -= DAYS_PER_MONTH;
		result->month++;
	}

	if (result->day > 0 && result->time < 0)
	{
		result->time += USECS_PER_DAY;
		result->day--;
	}
	else if (result->day < 0 && result->time > 0)
	{
		result->time -= USECS_PER_DAY;
		result->day++;
	}

	PG_RETURN_INTERVAL_P(result);
}

/*
 *	interval_justify_hours()
 *
 *	Adjust interval so 'time' contains less than a whole day, adding
 *	the excess to 'day'.  This is useful for
 *	situations (such as non-TZ) where '1 day' = '24 hours' is valid,
 *	e.g. interval subtraction and division.
 */
Datum
interval_justify_hours(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	Interval   *result;
	TimeOffset	wholeday;

	result = (Interval *) palloc(sizeof(Interval));
	result->month = span->month;
	result->day = span->day;
	result->time = span->time;

	TMODULO(result->time, wholeday, USECS_PER_DAY);
	result->day += wholeday;	/* could overflow... */

	if (result->day > 0 && result->time < 0)
	{
		result->time += USECS_PER_DAY;
		result->day--;
	}
	else if (result->day < 0 && result->time > 0)
	{
		result->time -= USECS_PER_DAY;
		result->day++;
	}

	PG_RETURN_INTERVAL_P(result);
}

/*
 *	interval_justify_days()
 *
 *	Adjust interval so 'day' contains less than 30 days, adding
 *	the excess to 'month'.
 */
Datum
interval_justify_days(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	Interval   *result;
	int32		wholemonth;

	result = (Interval *) palloc(sizeof(Interval));
	result->month = span->month;
	result->day = span->day;
	result->time = span->time;

	wholemonth = result->day / DAYS_PER_MONTH;
	result->day -= wholemonth * DAYS_PER_MONTH;
	result->month += wholemonth;

	if (result->month > 0 && result->day < 0)
	{
		result->day += DAYS_PER_MONTH;
		result->month--;
	}
	else if (result->month < 0 && result->day > 0)
	{
		result->day -= DAYS_PER_MONTH;
		result->month++;
	}

	PG_RETURN_INTERVAL_P(result);
}

/* timestamp_pl_interval()
 * Add an interval to a timestamp data type.
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
Datum
timestamp_pl_interval(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Timestamp	result;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (span->month != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;

#ifdef _PG_ORCL_
			bool	last_day = false;
#endif
			if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
#ifdef _PG_ORCL_
			last_day = (tm->tm_mday == day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);
#endif

			tm->tm_mon += span->month;
			if (tm->tm_mon > MONTHS_PER_YEAR)
			{
				tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
				tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
			}
			else if (tm->tm_mon < 1)
			{
				tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
				tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
			}

			/* adjust for end of month boundary problems... */
			if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
				tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);

#ifdef _PG_ORCL_
			if (ORA_MODE && last_day)
			{
				tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);
			}
#endif
			if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (span->day != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;
			int			julian;

			if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			/* Add days by converting to and from Julian */
			julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) + span->day;
			j2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

			if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		timestamp += span->time;

		if (!IS_VALID_TIMESTAMP(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		result = timestamp;
	}

	PG_RETURN_TIMESTAMP(result);
}

Datum
timestamp_mi_interval(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	Interval	tspan;

	tspan.month = -span->month;
	tspan.day = -span->day;
	tspan.time = -span->time;

	return DirectFunctionCall2(timestamp_pl_interval,
							   TimestampGetDatum(timestamp),
							   PointerGetDatum(&tspan));
}


/* timestamptz_pl_interval_internal()
 * Add an interval to a timestamptz, in the given (or session) timezone.
 *
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
static TimestampTz
timestamptz_pl_interval_internal(TimestampTz timestamp,
								 Interval *span,
								 pg_tz *attimezone)
{
	TimestampTz result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		/* Use session timezone if caller asks for default */
		if (attimezone == NULL)
			attimezone = session_timezone;

		if (span->month != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;

#ifdef _PG_ORCL_
			bool	last_day = false;
#endif
			if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, attimezone) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
#ifdef _PG_ORCL_
			last_day = (tm->tm_mday == day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);
#endif
			tm->tm_mon += span->month;
			if (tm->tm_mon > MONTHS_PER_YEAR)
			{
				tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
				tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
			}
			else if (tm->tm_mon < 1)
			{
				tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
				tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
			}

			/* adjust for end of month boundary problems... */
			if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
				tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);

#ifdef _PG_ORCL_
			if (ORA_MODE && last_day)
			{
				tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);
			}
#endif
			tz = DetermineTimeZoneOffset(tm, attimezone);

			if (tm2timestamp(tm, fsec, &tz, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		if (span->day != 0)
		{
			struct pg_tm tt,
					   *tm = &tt;
			fsec_t		fsec;
			int			julian;

			if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, attimezone) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			/* Add days by converting to and from Julian */
			julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) + span->day;
			j2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

			tz = DetermineTimeZoneOffset(tm, attimezone);

			if (tm2timestamp(tm, fsec, &tz, &timestamp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}

		timestamp += span->time;

		if (!IS_VALID_TIMESTAMP(timestamp))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		result = timestamp;
	}

	return result;
}

/* timestamptz_mi_interval_internal()
 * As above, but subtract the interval.
 */
static TimestampTz
timestamptz_mi_interval_internal(TimestampTz timestamp,
								 Interval *span,
								 pg_tz *attimezone)
{
	Interval	tspan;

	tspan.month = -span->month;
	tspan.day = -span->day;
	tspan.time = -span->time;

	return timestamptz_pl_interval_internal(timestamp, &tspan, attimezone);
}

/* timestamptz_pl_interval()
 * Add an interval to a timestamptz, in the session timezone.
 */
Datum
timestamptz_pl_interval(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_TIMESTAMP(timestamptz_pl_interval_internal(timestamp, span, NULL));
}

Datum
timestamptz_mi_interval(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);

	PG_RETURN_TIMESTAMP(timestamptz_mi_interval_internal(timestamp, span, NULL));
}

/* timestamptz_pl_interval_at_zone()
 * Add an interval to a timestamptz, in the specified timezone.
 */
Datum
timestamptz_pl_interval_at_zone(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	text	   *zone = PG_GETARG_TEXT_PP(2);
	pg_tz	   *attimezone = lookup_timezone(zone);

	PG_RETURN_TIMESTAMP(timestamptz_pl_interval_internal(timestamp, span, attimezone));
}

Datum
timestamptz_mi_interval_at_zone(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	Interval   *span = PG_GETARG_INTERVAL_P(1);
	text	   *zone = PG_GETARG_TEXT_PP(2);
	pg_tz	   *attimezone = lookup_timezone(zone);

	PG_RETURN_TIMESTAMP(timestamptz_mi_interval_internal(timestamp, span, attimezone));
}

Datum
interval_um(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	result->time = -interval->time;
	/* overflow check copied from int4um */
	if (interval->time != 0 && SAMESIGN(result->time, interval->time))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
	result->day = -interval->day;
	if (interval->day != 0 && SAMESIGN(result->day, interval->day))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
	result->month = -interval->month;
	if (interval->month != 0 && SAMESIGN(result->month, interval->month))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	PG_RETURN_INTERVAL_P(result);
}


Datum
interval_smaller(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	/* use interval_cmp_internal to be sure this agrees with comparisons */
	if (interval_cmp_internal(interval1, interval2) < 0)
		result = interval1;
	else
		result = interval2;
	PG_RETURN_INTERVAL_P(result);
}

Datum
interval_larger(PG_FUNCTION_ARGS)
{
	Interval   *interval1 = PG_GETARG_INTERVAL_P(0);
	Interval   *interval2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	if (interval_cmp_internal(interval1, interval2) > 0)
		result = interval1;
	else
		result = interval2;
	PG_RETURN_INTERVAL_P(result);
}

Datum
interval_pl(PG_FUNCTION_ARGS)
{
	Interval   *span1 = PG_GETARG_INTERVAL_P(0);
	Interval   *span2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	result->month = span1->month + span2->month;
	/* overflow check copied from int4pl */
	if (SAMESIGN(span1->month, span2->month) &&
		!SAMESIGN(result->month, span1->month))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result->day = span1->day + span2->day;
	if (SAMESIGN(span1->day, span2->day) &&
		!SAMESIGN(result->day, span1->day))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result->time = span1->time + span2->time;
	if (SAMESIGN(span1->time, span2->time) &&
		!SAMESIGN(result->time, span1->time))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	PG_RETURN_INTERVAL_P(result);
}

Datum
interval_mi(PG_FUNCTION_ARGS)
{
	Interval   *span1 = PG_GETARG_INTERVAL_P(0);
	Interval   *span2 = PG_GETARG_INTERVAL_P(1);
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	result->month = span1->month - span2->month;
	/* overflow check copied from int4mi */
	if (!SAMESIGN(span1->month, span2->month) &&
		!SAMESIGN(result->month, span1->month))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result->day = span1->day - span2->day;
	if (!SAMESIGN(span1->day, span2->day) &&
		!SAMESIGN(result->day, span1->day))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	result->time = span1->time - span2->time;
	if (!SAMESIGN(span1->time, span2->time) &&
		!SAMESIGN(result->time, span1->time))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));

	PG_RETURN_INTERVAL_P(result);
}

/*
 *	There is no interval_abs():  it is unclear what value to return:
 *	  http://archives.postgresql.org/pgsql-general/2009-10/msg01031.php
 *	  http://archives.postgresql.org/pgsql-general/2009-11/msg00041.php
 */

Datum
interval_mul(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	float8		factor = PG_GETARG_FLOAT8(1);
	double		month_remainder_days,
				sec_remainder,
				result_double;
	int32		orig_month = span->month,
				orig_day = span->day;
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	result_double = span->month * factor;
	if (isnan(result_double) ||
		result_double > INT_MAX || result_double < INT_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
	result->month = (int32) result_double;

	result_double = span->day * factor;
	if (isnan(result_double) ||
		result_double > INT_MAX || result_double < INT_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
	result->day = (int32) result_double;

	/*
	 * The above correctly handles the whole-number part of the month and day
	 * products, but we have to do something with any fractional part
	 * resulting when the factor is non-integral.  We cascade the fractions
	 * down to lower units using the conversion factors DAYS_PER_MONTH and
	 * SECS_PER_DAY.  Note we do NOT cascade up, since we are not forced to do
	 * so by the representation.  The user can choose to cascade up later,
	 * using justify_hours and/or justify_days.
	 */

	/*
	 * Fractional months full days into days.
	 *
	 * Floating point calculation are inherently imprecise, so these
	 * calculations are crafted to produce the most reliable result possible.
	 * TSROUND() is needed to more accurately produce whole numbers where
	 * appropriate.
	 */
	month_remainder_days = (orig_month * factor - result->month) * DAYS_PER_MONTH;
	month_remainder_days = TSROUND(month_remainder_days);
	sec_remainder = (orig_day * factor - result->day +
					 month_remainder_days - (int) month_remainder_days) * SECS_PER_DAY;
	sec_remainder = TSROUND(sec_remainder);

	/*
	 * Might have 24:00:00 hours due to rounding, or >24 hours because of time
	 * cascade from months and days.  It might still be >24 if the combination
	 * of cascade and the seconds factor operation itself.
	 */
	if (Abs(sec_remainder) >= SECS_PER_DAY)
	{
		result->day += (int) (sec_remainder / SECS_PER_DAY);
		sec_remainder -= (int) (sec_remainder / SECS_PER_DAY) * SECS_PER_DAY;
	}

	/* cascade units down */
	result->day += (int32) month_remainder_days;
	result_double = rint(span->time * factor + sec_remainder * USECS_PER_SEC);
	if (result_double > PG_INT64_MAX || result_double < PG_INT64_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("interval out of range")));
	result->time = (int64) result_double;

	PG_RETURN_INTERVAL_P(result);
}

Datum
mul_d_interval(PG_FUNCTION_ARGS)
{
	/* Args are float8 and Interval *, but leave them as generic Datum */
	Datum		factor = PG_GETARG_DATUM(0);
	Datum		span = PG_GETARG_DATUM(1);

	return DirectFunctionCall2(interval_mul, span, factor);
}

Datum
interval_div(PG_FUNCTION_ARGS)
{
	Interval   *span = PG_GETARG_INTERVAL_P(0);
	float8		factor = PG_GETARG_FLOAT8(1);
	double		month_remainder_days,
				sec_remainder;
	int32		orig_month = span->month,
				orig_day = span->day;
	Interval   *result;

	result = (Interval *) palloc(sizeof(Interval));

	if (factor == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result->month = (int32) (span->month / factor);
	result->day = (int32) (span->day / factor);

	/*
	 * Fractional months full days into days.  See comment in interval_mul().
	 */
	month_remainder_days = (orig_month / factor - result->month) * DAYS_PER_MONTH;
	month_remainder_days = TSROUND(month_remainder_days);
	sec_remainder = (orig_day / factor - result->day +
					 month_remainder_days - (int) month_remainder_days) * SECS_PER_DAY;
	sec_remainder = TSROUND(sec_remainder);
	if (Abs(sec_remainder) >= SECS_PER_DAY)
	{
		result->day += (int) (sec_remainder / SECS_PER_DAY);
		sec_remainder -= (int) (sec_remainder / SECS_PER_DAY) * SECS_PER_DAY;
	}

	/* cascade units down */
	result->day += (int32) month_remainder_days;
	result->time = rint(span->time / factor + sec_remainder * USECS_PER_SEC);

	PG_RETURN_INTERVAL_P(result);
}


/*
 * interval_accum, interval_accum_inv, and interval_avg implement the
 * AVG(interval) aggregate.
#ifdef PGXC
 * as well as interval_collect
#endif
 *
 * The transition datatype for this aggregate is a 2-element array of
 * intervals, where the first is the running sum and the second contains
 * the number of values so far in its 'time' field.  This is a bit ugly
 * but it beats inventing a specialized datatype for the purpose.
 */

Datum
interval_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Interval   *newval = PG_GETARG_INTERVAL_P(1);
	Datum	   *transdatums;
	int			ndatums;
	Interval	sumX,
				N;
	Interval   *newsum;
	ArrayType  *result;

	deconstruct_array(transarray,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &transdatums, NULL, &ndatums);
	if (ndatums != 2)
		elog(ERROR, "expected 2-element interval array");

	sumX = *(DatumGetIntervalP(transdatums[0]));
	N = *(DatumGetIntervalP(transdatums[1]));

	newsum = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
												   IntervalPGetDatum(&sumX),
												   IntervalPGetDatum(newval)));
	N.time += 1;

	transdatums[0] = IntervalPGetDatum(newsum);
	transdatums[1] = IntervalPGetDatum(&N);

	result = construct_array(transdatums, 2,
							 INTERVALOID, sizeof(Interval), false, 'd');

	PG_RETURN_ARRAYTYPE_P(result);
}

Datum
interval_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray2 = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *transdatums1;
	Datum	   *transdatums2;
	int			ndatums1;
	int			ndatums2;
	Interval	sum1,
				N1;
	Interval	sum2,
				N2;

	Interval   *newsum;
	ArrayType  *result;

	deconstruct_array(transarray1,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &transdatums1, NULL, &ndatums1);
	if (ndatums1 != 2)
		elog(ERROR, "expected 2-element interval array");

	sum1 = *(DatumGetIntervalP(transdatums1[0]));
	N1 = *(DatumGetIntervalP(transdatums1[1]));

	deconstruct_array(transarray2,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &transdatums2, NULL, &ndatums2);
	if (ndatums2 != 2)
		elog(ERROR, "expected 2-element interval array");

	sum2 = *(DatumGetIntervalP(transdatums2[0]));
	N2 = *(DatumGetIntervalP(transdatums2[1]));

	newsum = DatumGetIntervalP(DirectFunctionCall2(interval_pl,
												   IntervalPGetDatum(&sum1),
												   IntervalPGetDatum(&sum2)));
	N1.time += N2.time;

	transdatums1[0] = IntervalPGetDatum(newsum);
	transdatums1[1] = IntervalPGetDatum(&N1);

	result = construct_array(transdatums1, 2,
							 INTERVALOID, sizeof(Interval), false, 'd');

	PG_RETURN_ARRAYTYPE_P(result);
}

Datum
interval_accum_inv(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Interval   *newval = PG_GETARG_INTERVAL_P(1);
	Datum	   *transdatums;
	int			ndatums;
	Interval	sumX,
				N;
	Interval   *newsum;
	ArrayType  *result;

	deconstruct_array(transarray,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &transdatums, NULL, &ndatums);
	if (ndatums != 2)
		elog(ERROR, "expected 2-element interval array");

	sumX = *(DatumGetIntervalP(transdatums[0]));
	N = *(DatumGetIntervalP(transdatums[1]));

	newsum = DatumGetIntervalP(DirectFunctionCall2(interval_mi,
												   IntervalPGetDatum(&sumX),
												   IntervalPGetDatum(newval)));
	N.time -= 1;

	transdatums[0] = IntervalPGetDatum(newsum);
	transdatums[1] = IntervalPGetDatum(&N);

	result = construct_array(transdatums, 2,
							 INTERVALOID, sizeof(Interval), false, 'd');

	PG_RETURN_ARRAYTYPE_P(result);
}

Datum
interval_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *transdatums;
	int			ndatums;
	Interval	sumX,
				N;

	deconstruct_array(transarray,
					  INTERVALOID, sizeof(Interval), false, 'd',
					  &transdatums, NULL, &ndatums);
	if (ndatums != 2)
		elog(ERROR, "expected 2-element interval array");

	sumX = *(DatumGetIntervalP(transdatums[0]));
	N = *(DatumGetIntervalP(transdatums[1]));

	/* SQL defines AVG of no values to be NULL */
	if (N.time == 0)
		PG_RETURN_NULL();

	return DirectFunctionCall2(interval_div,
							   IntervalPGetDatum(&sumX),
							   Float8GetDatum((double) N.time));
}


/* timestamp_age()
 * Calculate time difference while retaining year/month fields.
 * Note that this does not result in an accurate absolute time span
 *	since year and month are out of context once the arithmetic
 *	is done.
 */
Datum
timestamp_age(PG_FUNCTION_ARGS)
{
	Timestamp	dt1 = PG_GETARG_TIMESTAMP(0);
	Timestamp	dt2 = PG_GETARG_TIMESTAMP(1);
	Interval   *result;
	fsec_t		fsec,
				fsec1,
				fsec2;
	struct pg_tm tt,
			   *tm = &tt;
	struct pg_tm tt1,
			   *tm1 = &tt1;
	struct pg_tm tt2,
			   *tm2 = &tt2;

	result = (Interval *) palloc(sizeof(Interval));

	if (timestamp2tm(dt1, NULL, tm1, &fsec1, NULL, NULL) == 0 &&
		timestamp2tm(dt2, NULL, tm2, &fsec2, NULL, NULL) == 0)
	{
		/* form the symbolic difference */
		fsec = fsec1 - fsec2;
		tm->tm_sec = tm1->tm_sec - tm2->tm_sec;
		tm->tm_min = tm1->tm_min - tm2->tm_min;
		tm->tm_hour = tm1->tm_hour - tm2->tm_hour;
		tm->tm_mday = tm1->tm_mday - tm2->tm_mday;
		tm->tm_mon = tm1->tm_mon - tm2->tm_mon;
		tm->tm_year = tm1->tm_year - tm2->tm_year;

		/* flip sign if necessary... */
		if (dt1 < dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		/* propagate any negative fields into the next higher field */
		while (fsec < 0)
		{
			fsec += USECS_PER_SEC;
			tm->tm_sec--;
		}

		while (tm->tm_sec < 0)
		{
			tm->tm_sec += SECS_PER_MINUTE;
			tm->tm_min--;
		}

		while (tm->tm_min < 0)
		{
			tm->tm_min += MINS_PER_HOUR;
			tm->tm_hour--;
		}

		while (tm->tm_hour < 0)
		{
			tm->tm_hour += HOURS_PER_DAY;
			tm->tm_mday--;
		}

		while (tm->tm_mday < 0)
		{
			if (dt1 < dt2)
			{
				tm->tm_mday += day_tab[isleap(tm1->tm_year)][tm1->tm_mon - 1];
				tm->tm_mon--;
			}
			else
			{
				tm->tm_mday += day_tab[isleap(tm2->tm_year)][tm2->tm_mon - 1];
				tm->tm_mon--;
			}
		}

		while (tm->tm_mon < 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		/* recover sign if necessary... */
		if (dt1 < dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		if (tm2interval(tm, fsec, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_INTERVAL_P(result);
}


/* timestamptz_age()
 * Calculate time difference while retaining year/month fields.
 * Note that this does not result in an accurate absolute time span
 *	since year and month are out of context once the arithmetic
 *	is done.
 */
Datum
timestamptz_age(PG_FUNCTION_ARGS)
{
	TimestampTz dt1 = PG_GETARG_TIMESTAMPTZ(0);
	TimestampTz dt2 = PG_GETARG_TIMESTAMPTZ(1);
	Interval   *result;
	fsec_t		fsec,
				fsec1,
				fsec2;
	struct pg_tm tt,
			   *tm = &tt;
	struct pg_tm tt1,
			   *tm1 = &tt1;
	struct pg_tm tt2,
			   *tm2 = &tt2;
	int			tz1;
	int			tz2;

	result = (Interval *) palloc(sizeof(Interval));

	if (timestamp2tm(dt1, &tz1, tm1, &fsec1, NULL, NULL) == 0 &&
		timestamp2tm(dt2, &tz2, tm2, &fsec2, NULL, NULL) == 0)
	{
		/* form the symbolic difference */
		fsec = fsec1 - fsec2;
		tm->tm_sec = tm1->tm_sec - tm2->tm_sec;
		tm->tm_min = tm1->tm_min - tm2->tm_min;
		tm->tm_hour = tm1->tm_hour - tm2->tm_hour;
		tm->tm_mday = tm1->tm_mday - tm2->tm_mday;
		tm->tm_mon = tm1->tm_mon - tm2->tm_mon;
		tm->tm_year = tm1->tm_year - tm2->tm_year;

		/* flip sign if necessary... */
		if (dt1 < dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		/* propagate any negative fields into the next higher field */
		while (fsec < 0)
		{
			fsec += USECS_PER_SEC;
			tm->tm_sec--;
		}

		while (tm->tm_sec < 0)
		{
			tm->tm_sec += SECS_PER_MINUTE;
			tm->tm_min--;
		}

		while (tm->tm_min < 0)
		{
			tm->tm_min += MINS_PER_HOUR;
			tm->tm_hour--;
		}

		while (tm->tm_hour < 0)
		{
			tm->tm_hour += HOURS_PER_DAY;
			tm->tm_mday--;
		}

		while (tm->tm_mday < 0)
		{
			if (dt1 < dt2)
			{
				tm->tm_mday += day_tab[isleap(tm1->tm_year)][tm1->tm_mon - 1];
				tm->tm_mon--;
			}
			else
			{
				tm->tm_mday += day_tab[isleap(tm2->tm_year)][tm2->tm_mon - 1];
				tm->tm_mon--;
			}
		}

		while (tm->tm_mon < 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		/*
		 * Note: we deliberately ignore any difference between tz1 and tz2.
		 */

		/* recover sign if necessary... */
		if (dt1 < dt2)
		{
			fsec = -fsec;
			tm->tm_sec = -tm->tm_sec;
			tm->tm_min = -tm->tm_min;
			tm->tm_hour = -tm->tm_hour;
			tm->tm_mday = -tm->tm_mday;
			tm->tm_mon = -tm->tm_mon;
			tm->tm_year = -tm->tm_year;
		}

		if (tm2interval(tm, fsec, result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("interval out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_INTERVAL_P(result);
}


/*----------------------------------------------------------
 *	Conversion operators.
 *---------------------------------------------------------*/


/* timestamp_trunc()
 * Truncate timestamp to specified units.
 */
Datum
timestamp_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_WEEK:
				{
					int			woy;

					woy = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);

					/*
					 * If it is week 52/53 and the month is January, then the
					 * week must belong to the previous year. Also, some
					 * December dates belong to the next year.
					 */
					if (woy >= 52 && tm->tm_mon == 1)
						--tm->tm_year;
					if (woy <= 1 && tm->tm_mon == MONTHS_PER_YEAR)
						++tm->tm_year;
					isoweek2date(woy, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
					tm->tm_hour = 0;
					tm->tm_min = 0;
					tm->tm_sec = 0;
					fsec = 0;
					break;
				}
			case DTK_MILLENNIUM:
				/* see comments in timestamptz_trunc */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 999) / 1000) * 1000 - 999;
				else
					tm->tm_year = -((999 - (tm->tm_year - 1)) / 1000) * 1000 + 1;
			case DTK_CENTURY:
				/* see comments in timestamptz_trunc */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 99) / 100) * 100 - 99;
				else
					tm->tm_year = -((99 - (tm->tm_year - 1)) / 100) * 100 + 1;
			case DTK_DECADE:
				/* see comments in timestamptz_trunc */
				if (val != DTK_MILLENNIUM && val != DTK_CENTURY)
				{
					if (tm->tm_year > 0)
						tm->tm_year = (tm->tm_year / 10) * 10;
					else
						tm->tm_year = -((8 - (tm->tm_year - 1)) / 10) * 10;
				}
			case DTK_YEAR:
				tm->tm_mon = 1;
			case DTK_QUARTER:
				tm->tm_mon = (3 * ((tm->tm_mon - 1) / 3)) + 1;
			case DTK_MONTH:
				tm->tm_mday = 1;
			case DTK_DAY:
				tm->tm_hour = 0;
			case DTK_HOUR:
				tm->tm_min = 0;
			case DTK_MINUTE:
				tm->tm_sec = 0;
			case DTK_SECOND:
				fsec = 0;
				break;

			case DTK_MILLISEC:
				fsec = (fsec / 1000) * 1000;
				break;

			case DTK_MICROSEC:
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
				result = 0;
		}

		if (tm2timestamp(tm, fsec, NULL, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timestamp units \"%s\" not recognized",
						lowunits)));
		result = 0;
	}

	PG_RETURN_TIMESTAMP(result);
}

/* timestamptz_trunc()
 * Truncate timestamp to specified units.
 */
Datum
timestamptz_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz result;
	int			tz;
	int			type,
				val;
	bool		redotz = false;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_WEEK:
				{
					int			woy;

					woy = date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);

					/*
					 * If it is week 52/53 and the month is January, then the
					 * week must belong to the previous year. Also, some
					 * December dates belong to the next year.
					 */
					if (woy >= 52 && tm->tm_mon == 1)
						--tm->tm_year;
					if (woy <= 1 && tm->tm_mon == MONTHS_PER_YEAR)
						++tm->tm_year;
					isoweek2date(woy, &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
					tm->tm_hour = 0;
					tm->tm_min = 0;
					tm->tm_sec = 0;
					fsec = 0;
					redotz = true;
					break;
				}
				/* one may consider DTK_THOUSAND and DTK_HUNDRED... */
			case DTK_MILLENNIUM:

				/*
				 * truncating to the millennium? what is this supposed to
				 * mean? let us put the first year of the millennium... i.e.
				 * -1000, 1, 1001, 2001...
				 */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 999) / 1000) * 1000 - 999;
				else
					tm->tm_year = -((999 - (tm->tm_year - 1)) / 1000) * 1000 + 1;
				/* FALL THRU */
			case DTK_CENTURY:
				/* truncating to the century? as above: -100, 1, 101... */
				if (tm->tm_year > 0)
					tm->tm_year = ((tm->tm_year + 99) / 100) * 100 - 99;
				else
					tm->tm_year = -((99 - (tm->tm_year - 1)) / 100) * 100 + 1;
				/* FALL THRU */
			case DTK_DECADE:

				/*
				 * truncating to the decade? first year of the decade. must
				 * not be applied if year was truncated before!
				 */
				if (val != DTK_MILLENNIUM && val != DTK_CENTURY)
				{
					if (tm->tm_year > 0)
						tm->tm_year = (tm->tm_year / 10) * 10;
					else
						tm->tm_year = -((8 - (tm->tm_year - 1)) / 10) * 10;
				}
				/* FALL THRU */
			case DTK_YEAR:
				tm->tm_mon = 1;
				/* FALL THRU */
			case DTK_QUARTER:
				tm->tm_mon = (3 * ((tm->tm_mon - 1) / 3)) + 1;
				/* FALL THRU */
			case DTK_MONTH:
				tm->tm_mday = 1;
				/* FALL THRU */
			case DTK_DAY:
				tm->tm_hour = 0;
				redotz = true;	/* for all cases >= DAY */
				/* FALL THRU */
			case DTK_HOUR:
				tm->tm_min = 0;
				/* FALL THRU */
			case DTK_MINUTE:
				tm->tm_sec = 0;
				/* FALL THRU */
			case DTK_SECOND:
				fsec = 0;
				break;
			case DTK_MILLISEC:
				fsec = (fsec / 1000) * 1000;
				break;
			case DTK_MICROSEC:
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp with time zone units \"%s\" not "
								"supported", lowunits)));
				result = 0;
		}

		if (redotz)
			tz = DetermineTimeZoneOffset(tm, session_timezone);

		if (tm2timestamp(tm, fsec, &tz, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timestamp with time zone units \"%s\" not recognized",
						lowunits)));
		result = 0;
	}

	PG_RETURN_TIMESTAMPTZ(result);
}

/* interval_trunc()
 * Extract specified field from interval.
 */
Datum
interval_trunc(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Interval   *interval = PG_GETARG_INTERVAL_P(1);
	Interval   *result;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	result = (Interval *) palloc(sizeof(Interval));

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);

	if (type == UNITS)
	{
		if (interval2tm(*interval, tm, &fsec) == 0)
		{
			switch (val)
			{
					/* fall through */
				case DTK_MILLENNIUM:
					/* caution: C division may have negative remainder */
					tm->tm_year = (tm->tm_year / 1000) * 1000;
				case DTK_CENTURY:
					/* caution: C division may have negative remainder */
					tm->tm_year = (tm->tm_year / 100) * 100;
				case DTK_DECADE:
					/* caution: C division may have negative remainder */
					tm->tm_year = (tm->tm_year / 10) * 10;
				case DTK_YEAR:
					tm->tm_mon = 0;
				case DTK_QUARTER:
					tm->tm_mon = 3 * (tm->tm_mon / 3);
				case DTK_MONTH:
					tm->tm_mday = 0;
				case DTK_DAY:
					tm->tm_hour = 0;
				case DTK_HOUR:
					tm->tm_min = 0;
				case DTK_MINUTE:
					tm->tm_sec = 0;
				case DTK_SECOND:
					fsec = 0;
					break;
				case DTK_MILLISEC:
					fsec = (fsec / 1000) * 1000;
					break;
				case DTK_MICROSEC:
					break;

				default:
					if (val == DTK_WEEK)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("interval units \"%s\" not supported "
										"because months usually have fractional weeks",
										lowunits)));
					else
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("interval units \"%s\" not supported",
										lowunits)));
			}

			if (tm2interval(tm, fsec, result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("interval out of range")));
		}
		else
			elog(ERROR, "could not convert interval to tm");
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval units \"%s\" not recognized",
						lowunits)));
	}

	PG_RETURN_INTERVAL_P(result);
}

/* isoweek2j()
 *
 *	Return the Julian day which corresponds to the first day (Monday) of the given ISO 8601 year and week.
 *	Julian days are used to convert between ISO week dates and Gregorian dates.
 */
int
isoweek2j(int year, int week)
{
	int			day0,
				day4;

	/* fourth day of current year */
	day4 = date2j(year, 1, 4);

	/* day0 == offset to first day of week (Monday) */
	day0 = j2day(day4 - 1);

	return ((week - 1) * 7) + (day4 - day0);
}

/* isoweek2date()
 * Convert ISO week of year number to date.
 * The year field must be specified with the ISO year!
 * karel 2000/08/07
 */
void
isoweek2date(int woy, int *year, int *mon, int *mday)
{
	j2date(isoweek2j(*year, woy), year, mon, mday);
}

/* isoweekdate2date()
 *
 *	Convert an ISO 8601 week date (ISO year, ISO week) into a Gregorian date.
 *	Gregorian day of week sent so weekday strings can be supplied.
 *	Populates year, mon, and mday with the correct Gregorian values.
 *	year must be passed in as the ISO year.
 */
void
isoweekdate2date(int isoweek, int wday, int *year, int *mon, int *mday)
{
	int			jday;

	jday = isoweek2j(*year, isoweek);
	/* convert Gregorian week start (Sunday=1) to ISO week start (Monday=1) */
	if (wday > 1)
		jday += wday - 2;
	else
		jday += 6;
	j2date(jday, year, mon, mday);
}

/* date2isoweek()
 *
 *	Returns ISO week number of year.
 */
int
date2isoweek(int year, int mon, int mday)
{
	float8		result;
	int			day0,
				day4,
				dayn;

	/* current day */
	dayn = date2j(year, mon, mday);

	/* fourth day of current year */
	day4 = date2j(year, 1, 4);

	/* day0 == offset to first day of week (Monday) */
	day0 = j2day(day4 - 1);

	/*
	 * We need the first week containing a Thursday, otherwise this day falls
	 * into the previous year for purposes of counting weeks
	 */
	if (dayn < day4 - day0)
	{
		day4 = date2j(year - 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = j2day(day4 - 1);
	}

	result = (dayn - (day4 - day0)) / 7 + 1;

	/*
	 * Sometimes the last few days in a year will fall into the first week of
	 * the next year, so check for this.
	 */
	if (result >= 52)
	{
		day4 = date2j(year + 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = j2day(day4 - 1);

		if (dayn >= day4 - day0)
			result = (dayn - (day4 - day0)) / 7 + 1;
	}

	return (int) result;
}


/* date2isoyear()
 *
 *	Returns ISO 8601 year number.
 */
int
date2isoyear(int year, int mon, int mday)
{
	float8		result;
	int			day0,
				day4,
				dayn;

	/* current day */
	dayn = date2j(year, mon, mday);

	/* fourth day of current year */
	day4 = date2j(year, 1, 4);

	/* day0 == offset to first day of week (Monday) */
	day0 = j2day(day4 - 1);

	/*
	 * We need the first week containing a Thursday, otherwise this day falls
	 * into the previous year for purposes of counting weeks
	 */
	if (dayn < day4 - day0)
	{
		day4 = date2j(year - 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = j2day(day4 - 1);

		year--;
	}

	result = (dayn - (day4 - day0)) / 7 + 1;

	/*
	 * Sometimes the last few days in a year will fall into the first week of
	 * the next year, so check for this.
	 */
	if (result >= 52)
	{
		day4 = date2j(year + 1, 1, 4);

		/* day0 == offset to first day of week (Monday) */
		day0 = j2day(day4 - 1);

		if (dayn >= day4 - day0)
			year++;
	}

	return year;
}


/* date2isoyearday()
 *
 *	Returns the ISO 8601 day-of-year, given a Gregorian year, month and day.
 *	Possible return values are 1 through 371 (364 in non-leap years).
 */
int
date2isoyearday(int year, int mon, int mday)
{
	return date2j(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1;
}

/*
 * NonFiniteTimestampTzPart
 *
 *	Used by timestamp_part and timestamptz_part when extracting from infinite
 *	timestamp[tz].  Returns +/-Infinity if that is the appropriate result,
 *	otherwise returns zero (which should be taken as meaning to return NULL).
 *
 *	Errors thrown here for invalid units should exactly match those that
 *	would be thrown in the calling functions, else there will be unexpected
 *	discrepancies between finite- and infinite-input cases.
 */
static float8
NonFiniteTimestampTzPart(int type, int unit, char *lowunits,
						 bool isNegative, bool isTz)
{
	if ((type != UNITS) && (type != RESERV))
	{
		if (isTz)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("timestamp with time zone units \"%s\" not recognized",
							lowunits)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("timestamp units \"%s\" not recognized",
							lowunits)));
	}

	switch (unit)
	{
			/* Oscillating units */
		case DTK_MICROSEC:
		case DTK_MILLISEC:
		case DTK_SECOND:
		case DTK_MINUTE:
		case DTK_HOUR:
		case DTK_DAY:
		case DTK_MONTH:
		case DTK_QUARTER:
		case DTK_WEEK:
		case DTK_DOW:
		case DTK_ISODOW:
		case DTK_DOY:
		case DTK_TZ:
		case DTK_TZ_MINUTE:
		case DTK_TZ_HOUR:
			return 0.0;

			/* Monotonically-increasing units */
		case DTK_YEAR:
		case DTK_DECADE:
		case DTK_CENTURY:
		case DTK_MILLENNIUM:
		case DTK_JULIAN:
		case DTK_ISOYEAR:
		case DTK_EPOCH:
			if (isNegative)
				return -get_float8_infinity();
			else
				return get_float8_infinity();

		default:
			if (isTz)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp with time zone units \"%s\" not supported",
								lowunits)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
			return 0.0;			/* keep compiler quiet */
	}
}

/* timestamp_part()
 * Extract specified field from timestamp.
 */
Datum
timestamp_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	float8		result;
	Timestamp	epoch;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		result = NonFiniteTimestampTzPart(type, val, lowunits,
										  TIMESTAMP_IS_NOBEGIN(timestamp),
										  false);
		if (result)
			PG_RETURN_FLOAT8(result);
		else
			PG_RETURN_NULL();
	}

	if (type == UNITS)
	{
		if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_MICROSEC:
				result = tm->tm_sec * 1000000.0 + fsec;
				break;

			case DTK_MILLISEC:
				result = tm->tm_sec * 1000.0 + fsec / 1000.0;
				break;

			case DTK_SECOND:
				result = tm->tm_sec + fsec / 1000000.0;
				break;

			case DTK_MINUTE:
				result = tm->tm_min;
				break;

			case DTK_HOUR:
				result = tm->tm_hour;
				break;

			case DTK_DAY:
				result = tm->tm_mday;
				break;

			case DTK_MONTH:
				result = tm->tm_mon;
				break;

			case DTK_QUARTER:
				result = (tm->tm_mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				result = (float8) date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_YEAR:
				if (tm->tm_year > 0)
					result = tm->tm_year;
				else
					/* there is no year 0, just 1 BC and 1 AD */
					result = tm->tm_year - 1;
				break;

			case DTK_DECADE:

				/*
				 * what is a decade wrt dates? let us assume that decade 199
				 * is 1990 thru 1999... decade 0 starts on year 1 BC, and -1
				 * is 11 BC thru 2 BC...
				 */
				if (tm->tm_year >= 0)
					result = tm->tm_year / 10;
				else
					result = -((8 - (tm->tm_year - 1)) / 10);
				break;

			case DTK_CENTURY:

				/* ----
				 * centuries AD, c>0: year in [ (c-1)* 100 + 1 : c*100 ]
				 * centuries BC, c<0: year in [ c*100 : (c+1) * 100 - 1]
				 * there is no number 0 century.
				 * ----
				 */
				if (tm->tm_year > 0)
					result = (tm->tm_year + 99) / 100;
				else
					/* caution: C division may have negative remainder */
					result = -((99 - (tm->tm_year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				/* see comments above. */
				if (tm->tm_year > 0)
					result = (tm->tm_year + 999) / 1000;
				else
					result = -((999 - (tm->tm_year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				result = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
				result += ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
						   tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY;
				break;

			case DTK_ISOYEAR:
				result = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_DOW:
			case DTK_ISODOW:
				if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (val == DTK_ISODOW && result == 0)
					result = 7;
				break;

			case DTK_DOY:
				if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)
						  - date2j(tm->tm_year, 1, 1) + 1);
				break;

			case DTK_TZ:
			case DTK_TZ_MINUTE:
			case DTK_TZ_HOUR:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
				result = 0;
		}
	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
				epoch = SetEpochTimestamp();
				/* try to avoid precision loss in subtraction */
				if (timestamp < (PG_INT64_MAX + epoch))
					result = (timestamp - epoch) / 1000000.0;
				else
					result = ((float8) timestamp - epoch) / 1000000.0;
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp units \"%s\" not supported",
								lowunits)));
				result = 0;
		}

	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timestamp units \"%s\" not recognized", lowunits)));
		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}

/* timestamptz_part()
 * Extract specified field from timestamp with time zone.
 */
Datum
timestamptz_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	float8		result;
	Timestamp	epoch;
	int			tz;
	int			type,
				val;
	char	   *lowunits;
	double		dummy;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		result = NonFiniteTimestampTzPart(type, val, lowunits,
										  TIMESTAMP_IS_NOBEGIN(timestamp),
										  true);
		if (result)
			PG_RETURN_FLOAT8(result);
		else
			PG_RETURN_NULL();
	}

	if (type == UNITS)
	{
		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		switch (val)
		{
			case DTK_TZ:
				result = -tz;
				break;

			case DTK_TZ_MINUTE:
				result = -tz;
				result /= MINS_PER_HOUR;
				FMODULO(result, dummy, (double) MINS_PER_HOUR);
				break;

			case DTK_TZ_HOUR:
				dummy = -tz;
				FMODULO(dummy, result, (double) SECS_PER_HOUR);
				break;

			case DTK_MICROSEC:
				result = tm->tm_sec * 1000000.0 + fsec;
				break;

			case DTK_MILLISEC:
				result = tm->tm_sec * 1000.0 + fsec / 1000.0;
				break;

			case DTK_SECOND:
				result = tm->tm_sec + fsec / 1000000.0;
				break;

			case DTK_MINUTE:
				result = tm->tm_min;
				break;

			case DTK_HOUR:
				result = tm->tm_hour;
				break;

			case DTK_DAY:
				result = tm->tm_mday;
				break;

			case DTK_MONTH:
				result = tm->tm_mon;
				break;

			case DTK_QUARTER:
				result = (tm->tm_mon - 1) / 3 + 1;
				break;

			case DTK_WEEK:
				result = (float8) date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_YEAR:
				if (tm->tm_year > 0)
					result = tm->tm_year;
				else
					/* there is no year 0, just 1 BC and 1 AD */
					result = tm->tm_year - 1;
				break;

			case DTK_DECADE:
				/* see comments in timestamp_part */
				if (tm->tm_year > 0)
					result = tm->tm_year / 10;
				else
					result = -((8 - (tm->tm_year - 1)) / 10);
				break;

			case DTK_CENTURY:
				/* see comments in timestamp_part */
				if (tm->tm_year > 0)
					result = (tm->tm_year + 99) / 100;
				else
					result = -((99 - (tm->tm_year - 1)) / 100);
				break;

			case DTK_MILLENNIUM:
				/* see comments in timestamp_part */
				if (tm->tm_year > 0)
					result = (tm->tm_year + 999) / 1000;
				else
					result = -((999 - (tm->tm_year - 1)) / 1000);
				break;

			case DTK_JULIAN:
				result = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
				result += ((((tm->tm_hour * MINS_PER_HOUR) + tm->tm_min) * SECS_PER_MINUTE) +
						   tm->tm_sec + (fsec / 1000000.0)) / (double) SECS_PER_DAY;
				break;

			case DTK_ISOYEAR:
				result = date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday);
				break;

			case DTK_DOW:
			case DTK_ISODOW:
				if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = j2day(date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (val == DTK_ISODOW && result == 0)
					result = 7;
				break;

			case DTK_DOY:
				if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				result = (date2j(tm->tm_year, tm->tm_mon, tm->tm_mday)
						  - date2j(tm->tm_year, 1, 1) + 1);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp with time zone units \"%s\" not supported",
								lowunits)));
				result = 0;
		}

	}
	else if (type == RESERV)
	{
		switch (val)
		{
			case DTK_EPOCH:
				epoch = SetEpochTimestamp();
				/* try to avoid precision loss in subtraction */
				if (timestamp < (PG_INT64_MAX + epoch))
					result = (timestamp - epoch) / 1000000.0;
				else
					result = ((float8) timestamp - epoch) / 1000000.0;
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("timestamp with time zone units \"%s\" not supported",
								lowunits)));
				result = 0;
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("timestamp with time zone units \"%s\" not recognized",
						lowunits)));

		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}


/* interval_part()
 * Extract specified field from interval.
 */
Datum
interval_part(PG_FUNCTION_ARGS)
{
	text	   *units = PG_GETARG_TEXT_PP(0);
	Interval   *interval = PG_GETARG_INTERVAL_P(1);
	float8		result;
	int			type,
				val;
	char	   *lowunits;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;

	lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
											VARSIZE_ANY_EXHDR(units),
											false);

	type = DecodeUnits(0, lowunits, &val);
	if (type == UNKNOWN_FIELD)
		type = DecodeSpecial(0, lowunits, &val);

	if (type == UNITS)
	{
		if (interval2tm(*interval, tm, &fsec) == 0)
		{
			switch (val)
			{
				case DTK_MICROSEC:
					result = tm->tm_sec * 1000000.0 + fsec;
					break;

				case DTK_MILLISEC:
					result = tm->tm_sec * 1000.0 + fsec / 1000.0;
					break;

				case DTK_SECOND:
					result = tm->tm_sec + fsec / 1000000.0;
					break;

				case DTK_MINUTE:
					result = tm->tm_min;
					break;

				case DTK_HOUR:
					result = tm->tm_hour;
					break;

				case DTK_DAY:
					result = tm->tm_mday;
					break;

				case DTK_MONTH:
					result = tm->tm_mon;
					break;

				case DTK_QUARTER:
					result = (tm->tm_mon / 3) + 1;
					break;

				case DTK_YEAR:
					result = tm->tm_year;
					break;

				case DTK_DECADE:
					/* caution: C division may have negative remainder */
					result = tm->tm_year / 10;
					break;

				case DTK_CENTURY:
					/* caution: C division may have negative remainder */
					result = tm->tm_year / 100;
					break;

				case DTK_MILLENNIUM:
					/* caution: C division may have negative remainder */
					result = tm->tm_year / 1000;
					break;

				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("interval units \"%s\" not supported",
									lowunits)));
					result = 0;
			}

		}
		else
		{
			elog(ERROR, "could not convert interval to tm");
			result = 0;
		}
	}
	else if (type == RESERV && val == DTK_EPOCH)
	{
		result = interval->time / 1000000.0;
		result += ((double) DAYS_PER_YEAR * SECS_PER_DAY) * (interval->month / MONTHS_PER_YEAR);
		result += ((double) DAYS_PER_MONTH * SECS_PER_DAY) * (interval->month % MONTHS_PER_YEAR);
		result += ((double) SECS_PER_DAY) * interval->day;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval units \"%s\" not recognized",
						lowunits)));
		result = 0;
	}

	PG_RETURN_FLOAT8(result);
}


/* timestamp_zone_transform()
 * The original optimization here caused problems by relabeling Vars that
 * could be matched to index entries.  It might be possible to resurrect it
 * at some point by teaching the planner to be less cavalier with RelabelType
 * nodes, but that will take careful analysis.
 */
Datum
timestamp_zone_transform(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(NULL);
}

/*	timestamp_zone()
 *	Encode timestamp type with specified time zone.
 *	This function is just timestamp2timestamptz() except instead of
 *	shifting to the global timezone, we shift to the specified timezone.
 *	This is different from the other AT TIME ZONE cases because instead
 *	of shifting _to_ a new time zone, it sets the time to _be_ the
 *	specified timezone.
 */
Datum
timestamp_zone(PG_FUNCTION_ARGS)
{
	text	   *zone = PG_GETARG_TEXT_PP(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	TimestampTz result;
	int			tz;
	char		tzname[TZ_STRLEN_MAX + 1];
	char	   *lowzone;
	int			type,
				val;
	pg_tz	   *tzp;
	struct pg_tm tm;
	fsec_t		fsec;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	/*
	 * Look up the requested timezone.  First we look in the timezone
	 * abbreviation table (to handle cases like "EST"), and if that fails, we
	 * look in the timezone database (to handle cases like
	 * "America/New_York").  (This matches the order in which timestamp input
	 * checks the cases; it's important because the timezone database unwisely
	 * uses a few zone names that are identical to offset abbreviations.)
	 */
	text_to_cstring_buffer(zone, tzname, sizeof(tzname));

	/* DecodeTimezoneAbbrev requires lowercase input */
	lowzone = downcase_truncate_identifier(tzname,
										   strlen(tzname),
										   false);

	type = DecodeTimezoneAbbrev(0, lowzone, &val, &tzp);

	if (type == TZ || type == DTZ)
	{
		/* fixed-offset abbreviation */
		tz = val;
		result = dt2local(timestamp, tz);
	}
	else if (type == DYNTZ)
	{
		/* dynamic-offset abbreviation, resolve using specified time */
		if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, tzp) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		tz = -DetermineTimeZoneAbbrevOffset(&tm, tzname, tzp);
		result = dt2local(timestamp, tz);
	}
	else
	{
		/* try it as a full zone name */
		tzp = pg_tzset(tzname, false);
		if (tzp)
		{
			/* Apply the timezone change */
			if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, tzp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			tz = DetermineTimeZoneOffset(&tm, tzp);
			if (tm2timestamp(&tm, fsec, &tz, &result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));
			result = 0;			/* keep compiler quiet */
		}
	}

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(result);
}

/* timestamp_izone_transform()
 * The original optimization here caused problems by relabeling Vars that
 * could be matched to index entries.  It might be possible to resurrect it
 * at some point by teaching the planner to be less cavalier with RelabelType
 * nodes, but that will take careful analysis.
 */
Datum
timestamp_izone_transform(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(NULL);
}

/* timestamp_izone()
 * Encode timestamp type with specified time interval as time zone.
 */
Datum
timestamp_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	TimestampTz result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	if (zone->month != 0 || zone->day != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must not include months or days",
						DatumGetCString(DirectFunctionCall1(interval_out,
															PointerGetDatum(zone))))));

	tz = zone->time / USECS_PER_SEC;

	result = dt2local(timestamp, tz);

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(result);
}								/* timestamp_izone() */

/* timestamp_timestamptz()
 * Convert local timestamp to timestamp at GMT
 */
Datum
timestamp_timestamptz(PG_FUNCTION_ARGS)
{
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(0);

	PG_RETURN_TIMESTAMPTZ(timestamp2timestamptz(timestamp));
}

TimestampTz
timestamp2timestamptz(Timestamp timestamp)
{
	TimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		tz = DetermineTimeZoneOffset(tm, session_timezone);

		if (tm2timestamp(tm, fsec, &tz, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}

	return result;
}

/* timestamptz_timestamp()
 * Convert timestamp at GMT to local timestamp
 */
Datum
timestamptz_timestamp(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);

	PG_RETURN_TIMESTAMP(timestamptz2timestamp(timestamp));
}

Timestamp
timestamptz2timestamp(TimestampTz timestamp)
{
	Timestamp	result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		result = timestamp;
	else
	{
		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, NULL) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
		if (tm2timestamp(tm, fsec, NULL, &result) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	return result;
}

/* timestamptz_zone()
 * Evaluate timestamp with time zone type at the specified time zone.
 * Returns a timestamp without time zone.
 */
Datum
timestamptz_zone(PG_FUNCTION_ARGS)
{
	text	   *zone = PG_GETARG_TEXT_PP(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	Timestamp	result;
	int			tz;
	char		tzname[TZ_STRLEN_MAX + 1];
	char	   *lowzone;
	int			type,
				val;
	pg_tz	   *tzp;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	/*
	 * Look up the requested timezone.  First we look in the timezone
	 * abbreviation table (to handle cases like "EST"), and if that fails, we
	 * look in the timezone database (to handle cases like
	 * "America/New_York").  (This matches the order in which timestamp input
	 * checks the cases; it's important because the timezone database unwisely
	 * uses a few zone names that are identical to offset abbreviations.)
	 */
	text_to_cstring_buffer(zone, tzname, sizeof(tzname));

	/* DecodeTimezoneAbbrev requires lowercase input */
	lowzone = downcase_truncate_identifier(tzname,
										   strlen(tzname),
										   false);

	type = DecodeTimezoneAbbrev(0, lowzone, &val, &tzp);

	if (type == TZ || type == DTZ)
	{
		/* fixed-offset abbreviation */
		tz = -val;
		result = dt2local(timestamp, tz);
	}
	else if (type == DYNTZ)
	{
		/* dynamic-offset abbreviation, resolve using specified time */
		int			isdst;

		tz = DetermineTimeZoneAbbrevOffsetTS(timestamp, tzname, tzp, &isdst);
		result = dt2local(timestamp, tz);
	}
	else
	{
		/* try it as a full zone name */
		tzp = pg_tzset(tzname, false);
		if (tzp)
		{
			/* Apply the timezone change */
			struct pg_tm tm;
			fsec_t		fsec;

			if (timestamp2tm(timestamp, &tz, &tm, &fsec, NULL, tzp) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
			if (tm2timestamp(&tm, fsec, NULL, &result) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("time zone \"%s\" not recognized", tzname)));
			result = 0;			/* keep compiler quiet */
		}
	}

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}

/* timestamptz_izone()
 * Encode timestamp with time zone type with specified time interval as time zone.
 * Returns a timestamp without time zone.
 */
Datum
timestamptz_izone(PG_FUNCTION_ARGS)
{
	Interval   *zone = PG_GETARG_INTERVAL_P(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	Timestamp	result;
	int			tz;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	if (zone->month != 0 || zone->day != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval time zone \"%s\" must not include months or days",
						DatumGetCString(DirectFunctionCall1(interval_out,
															PointerGetDatum(zone))))));

	tz = -(zone->time / USECS_PER_SEC);

	result = dt2local(timestamp, tz);

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}

/* generate_series_timestamp()
 * Generate the set of timestamps from start to finish by step
 */
Datum
generate_series_timestamp(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	generate_series_timestamp_fctx *fctx;
	Timestamp	result;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		Timestamp	start = PG_GETARG_TIMESTAMP(0);
		Timestamp	finish = PG_GETARG_TIMESTAMP(1);
		Interval   *step = PG_GETARG_INTERVAL_P(2);
		MemoryContext oldcontext;
		Interval	interval_zero;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* allocate memory for user context */
		fctx = (generate_series_timestamp_fctx *)
			palloc(sizeof(generate_series_timestamp_fctx));

		/*
		 * Use fctx to keep state from call to call. Seed current with the
		 * original start value
		 */
		fctx->current = start;
		fctx->finish = finish;
		fctx->step = *step;

		/* Determine sign of the interval */
		MemSet(&interval_zero, 0, sizeof(Interval));
		fctx->step_sign = interval_cmp_internal(&fctx->step, &interval_zero);

		if (fctx->step_sign == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("step size cannot equal zero")));

		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	/*
	 * get the saved state and use current as the result for this iteration
	 */
	fctx = funcctx->user_fctx;
	result = fctx->current;

	if (fctx->step_sign > 0 ?
		timestamp_cmp_internal(result, fctx->finish) <= 0 :
		timestamp_cmp_internal(result, fctx->finish) >= 0)
	{
		/* increment current in preparation for next iteration */
		fctx->current = DatumGetTimestamp(
										  DirectFunctionCall2(timestamp_pl_interval,
															  TimestampGetDatum(fctx->current),
															  PointerGetDatum(&fctx->step)));

		/* do when there is more left to send */
		SRF_RETURN_NEXT(funcctx, TimestampGetDatum(result));
	}
	else
	{
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}
}

/* generate_series_timestamptz()
 * Generate the set of timestamps from start to finish by step,
 * doing arithmetic in the specified or session timezone.
 */
static Datum
generate_series_timestamptz_internal(FunctionCallInfo fcinfo)
{
	FuncCallContext *funcctx;
	generate_series_timestamptz_fctx *fctx;
	TimestampTz result;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		TimestampTz start = PG_GETARG_TIMESTAMPTZ(0);
		TimestampTz finish = PG_GETARG_TIMESTAMPTZ(1);
		Interval   *step = PG_GETARG_INTERVAL_P(2);
		text	   *zone = (PG_NARGS() == 4) ? PG_GETARG_TEXT_PP(3) : NULL;
		MemoryContext oldcontext;
		Interval	interval_zero;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* allocate memory for user context */
		fctx = (generate_series_timestamptz_fctx *)
			palloc(sizeof(generate_series_timestamptz_fctx));

		/*
		 * Use fctx to keep state from call to call. Seed current with the
		 * original start value
		 */
		fctx->current = start;
		fctx->finish = finish;
		fctx->step = *step;
		fctx->attimezone = zone ? lookup_timezone(zone) : session_timezone;

		/* Determine sign of the interval */
		MemSet(&interval_zero, 0, sizeof(Interval));
		fctx->step_sign = interval_cmp_internal(&fctx->step, &interval_zero);

		if (fctx->step_sign == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("step size cannot equal zero")));

		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	/*
	 * get the saved state and use current as the result for this iteration
	 */
	fctx = funcctx->user_fctx;
	result = fctx->current;

	if (fctx->step_sign > 0 ?
		timestamp_cmp_internal(result, fctx->finish) <= 0 :
		timestamp_cmp_internal(result, fctx->finish) >= 0)
	{
		/* increment current in preparation for next iteration */
		fctx->current = timestamptz_pl_interval_internal(fctx->current,
														 &fctx->step,
														 fctx->attimezone);

		/* do when there is more left to send */
		SRF_RETURN_NEXT(funcctx, TimestampTzGetDatum(result));
	}
	else
	{
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}
}

void
GenerateFNQueryId(FNQueryId *id)
{
	TimestampTz curtime;

	Assert(PGXCDigitalQueryId > 0);

	curtime = GetCurrentTimestamp();

	if (unlikely(curtime < sf_last_timestamp))
		elog(ERROR, "clock moved backwards. refusing to generate id");

	if (unlikely(curtime == sf_last_timestamp))
	{
		pg_usleep(1L);
		curtime = GetCurrentTimestamp();
	}

	sf_last_timestamp = curtime;

	id->timestamp_nodeid = (curtime - PgStartTime) << OPENTENBASE_MAX_COORDINATOR_NUMBER_LOG2 | (PGXCNodeId - 1);
	id->sequence = PGXCDigitalQueryId;

	elog(LOG, "GenerateFNQueryId %ld_%ld, PGXCQueryId %s",
		 id->timestamp_nodeid, id->sequence, PGXCQueryId);
}

static Datum
pg_timestamp_pl_day_internal(Timestamp dt, float8 days)
{
	Timestamp result;

	if (TIMESTAMP_NOT_FINITE(dt))
	{
		PG_RETURN_TIMESTAMP(dt);
	}

	days *= SECS_PER_DAY;
	result = dt + USECS_PER_SEC * DatumGetFloat8(DirectFunctionCall1(dtrunc, Float8GetDatumFast(days)));

	if (!IS_VALID_TIMESTAMP(result))
		ereport(ERROR,
		        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			          errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}

Datum
lightweight_ora_timestamp_pl_day_i2(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	int16		arg2 = PG_GETARG_INT16(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(i2tod, Int16GetDatum(arg2)));

	return pg_timestamp_pl_day_internal(arg1, days);
}

Datum
lightweight_ora_timestamp_pl_day_i4(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	int32		arg2 = PG_GETARG_INT32(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(i4tod, Int32GetDatum(arg2)));

	return pg_timestamp_pl_day_internal(arg1, days);
}

Datum
lightweight_ora_timestamp_pl_day_i8(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	int64		arg2 = PG_GETARG_INT64(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(i8tod, Int64GetDatum(arg2)));

	return pg_timestamp_pl_day_internal(arg1, days);
}

Datum
lightweight_ora_timestamp_pl_day_n(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	Numeric		arg2 = PG_GETARG_NUMERIC(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(arg2)));

	return pg_timestamp_pl_day_internal(arg1, days);
}

static Datum
pg_timestamp_mi_day_internal(Timestamp dt, float8 days)
{
	Timestamp		result;
	struct pg_tm 	tt, *tm = &tt;
	fsec_t			fsec = 0;
	struct pg_tm	mytt, *mytm = &mytt;
	float8			leave_day,
		  leave_hour,
		  leave_minute;
	float8			temp;

	if (TIMESTAMP_NOT_FINITE(dt))
	{
		PG_RETURN_TIMESTAMP(dt);
	}

	MemSet(tm, 0, sizeof(tt));
	MemSet(mytm, 0, sizeof(mytt));

	mytm->tm_mday = DatumGetInt32(DirectFunctionCall1(dtoi4, DirectFunctionCall1(dtrunc, Float8GetDatumFast(days))));

	leave_day = days - mytm->tm_mday;
	temp = leave_day * HOURS_PER_DAY;
	mytm->tm_hour = DatumGetInt32(DirectFunctionCall1(dtoi4, DirectFunctionCall1(dtrunc, Float8GetDatumFast(temp))));

	leave_hour = temp - mytm->tm_hour;
	temp = leave_hour * MINS_PER_HOUR;
	mytm->tm_min = DatumGetInt32(DirectFunctionCall1(dtoi4, DirectFunctionCall1(dtrunc, Float8GetDatumFast(temp))));

	leave_minute = temp - mytm->tm_min;
	temp = leave_minute * SECS_PER_MINUTE;
	mytm->tm_sec = DatumGetInt32(DirectFunctionCall1(dtoi4, DirectFunctionCall1(dtrunc, Float8GetDatumFast(temp))));

	/*
	 * ignore microsecond
	 *
	 * leave_second = temp - mytm->tm_sec;
	 * temp = leave_second * USECS_PER_SEC;
	 * mytm->fsec = DatumGetInt32(DirectFunctionCall1(dtoi4, DirectFunctionCall1(dtrunc, Float8GetDatumFast(temp))));
	 */

	if (timestamp2tm(dt, NULL, tm, &fsec, NULL, NULL) != 0)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			          errmsg("timestamp out of range")));
	}
	else
	{
		tm->tm_mday -= mytm->tm_mday;
		tm->tm_hour -= mytm->tm_hour;
		tm->tm_min  -= mytm->tm_min;
		tm->tm_sec  -= mytm->tm_sec;
	}

	if (tm2timestamp(tm, fsec, NULL, &result) != 0)
		ereport(ERROR,
		        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			          errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}

Datum
lightweight_ora_timestamp_mi_day_i2(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	int16		arg2 = PG_GETARG_INT16(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(i2tod, Int16GetDatum(arg2)));

	return pg_timestamp_mi_day_internal(arg1, days);
}

Datum
lightweight_ora_timestamp_mi_day_i4(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	int32		arg2 = PG_GETARG_INT32(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(i4tod, Int32GetDatum(arg2)));

	return pg_timestamp_mi_day_internal(arg1, days);
}

Datum
lightweight_ora_timestamp_mi_day_i8(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	int64		arg2 = PG_GETARG_INT64(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(i8tod, Int64GetDatum(arg2)));

	return pg_timestamp_mi_day_internal(arg1, days);
}

Datum
lightweight_ora_timestamp_mi_day_n(PG_FUNCTION_ARGS)
{
	Timestamp 	arg1 = PG_GETARG_TIMESTAMP(0);
	Numeric		arg2 = PG_GETARG_NUMERIC(1);
	float8 		days = DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(arg2)));

	return pg_timestamp_mi_day_internal(arg1, days);
}


Datum
generate_series_timestamptz(PG_FUNCTION_ARGS)
{
	return generate_series_timestamptz_internal(fcinfo);
}

Datum
generate_series_timestamptz_at_zone(PG_FUNCTION_ARGS)
{
	return generate_series_timestamptz_internal(fcinfo);
}
