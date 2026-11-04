/*-------------------------------------------------------------------------
 *
 * mysql_datetime.c
 *	  MySQL-compatible date/time functions.
 *
 * Functions: DATE_FORMAT, STR_TO_DATE, DATEDIFF, TIMESTAMPDIFF,
 *            TIMESTAMPADD, LAST_DAY
 *
 * Copyright (c) 2026, OpenTenBase Contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "utils/nabstime.h"
#include "catalog/pg_type.h"
#include <time.h>

/* Forward declarations */
Datum mysql_date_format(PG_FUNCTION_ARGS);
Datum mysql_str_to_date(PG_FUNCTION_ARGS);
Datum mysql_datediff(PG_FUNCTION_ARGS);
Datum mysql_timestampdiff(PG_FUNCTION_ARGS);
Datum mysql_timestampadd(PG_FUNCTION_ARGS);
Datum mysql_last_day(PG_FUNCTION_ARGS);

/*
 * Map a single MySQL date format specifier to PostgreSQL format.
 * Returns NULL if the specifier is not recognized.
 */
static const char *
mysql_fmt_to_pg(char spec)
{
	switch (spec)
	{
		case 'Y': return "YYYY";		/* 4-digit year */
		case 'y': return "YY";			/* 2-digit year */
		case 'm': return "MM";			/* Month (01-12) */
		case 'c': return "FMMM";		/* Month (1-12) */
		case 'M': return "FMMonth";		/* Full month name */
		case 'b': return "Mon";			/* Abbreviated month name */
		case 'd': return "DD";			/* Day (01-31) */
		case 'e': return "FMDD";		/* Day (1-31) */
		case 'D': return "FMDDth";		/* Day with suffix */
		case 'H': return "HH24";		/* Hour (00-23) */
		case 'h': return "HH12";		/* Hour (01-12) */
		case 'i': return "MI";			/* Minutes */
		case 's': return "SS";			/* Seconds */
		case 'p': return "AM";			/* AM/PM */
		case 'W': return "FMDay";		/* Full day name */
		case 'a': return "Dy";			/* Abbreviated day name */
		case 'w': return "D";			/* Day of week (0=Sunday) */
		case 'j': return "DDD";			/* Day of year */
		case 'U': return "WW";			/* Week number */
		case 'u': return "IW";			/* ISO week number */
		case 'T': return "HH24:MI:SS";	/* Time 24-hour */
		case 'r': return "HH12:MI:SS AM"; /* Time 12-hour */
		case 'f': return "US";			/* Microseconds */
		default:  return NULL;
	}
}


/*
 * DATE_FORMAT(date, format_string)
 * Formats a date/timestamp according to MySQL format specifiers.
 * Converts MySQL %specifiers to PostgreSQL format and delegates to to_char().
 */
PG_FUNCTION_INFO_V1(mysql_date_format);
Datum
mysql_date_format(PG_FUNCTION_ARGS)
{
	Timestamp	ts;
	text	   *fmt_text;
	char	   *fmt_str;
	StringInfoData pg_fmt;
	int			i;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	ts = PG_GETARG_TIMESTAMP(0);
	fmt_text = PG_GETARG_TEXT_P(1);
	fmt_str = text_to_cstring(fmt_text);

	initStringInfo(&pg_fmt);

	for (i = 0; fmt_str[i] != '\0'; i++)
	{
		if (fmt_str[i] == '%' && fmt_str[i + 1] != '\0')
		{
			const char *pg_spec;

			i++; /* consume '%' */
			pg_spec = mysql_fmt_to_pg(fmt_str[i]);
			if (pg_spec != NULL)
				appendStringInfoString(&pg_fmt, pg_spec);
			else
			{
				/* unrecognized, output literally */
				appendStringInfoChar(&pg_fmt, '%');
				appendStringInfoChar(&pg_fmt, fmt_str[i]);
			}
		}
		else
		{
			appendStringInfoChar(&pg_fmt, fmt_str[i]);
		}
	}

	/* Use PostgreSQL's to_char for the actual formatting */
	{
		text   *pg_fmt_text = cstring_to_text(pg_fmt.data);
		text   *result;

		result = DatumGetTextP(DirectFunctionCall2(timestamp_to_char,
												   TimestampGetDatum(ts),
												   PointerGetDatum(pg_fmt_text)));
		pfree(pg_fmt_text);
		pfree(fmt_str);
		pfree(pg_fmt.data);
		PG_RETURN_TEXT_P(result);
	}
}


/*
 * STR_TO_DATE(str, format)
 * Parses a string into a timestamp using MySQL format specifiers.
 * Converts MySQL %specifiers to PostgreSQL format and delegates to to_timestamp().
 */
PG_FUNCTION_INFO_V1(mysql_str_to_date);
Datum
mysql_str_to_date(PG_FUNCTION_ARGS)
{
	text	   *str_text;
	text	   *fmt_text;
	char	   *str;
	char	   *fmt_str;
	StringInfoData pg_fmt;
	int			i;
	TimestampTz result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	str_text = PG_GETARG_TEXT_P(0);
	fmt_text = PG_GETARG_TEXT_P(1);
	str = text_to_cstring(str_text);
	fmt_str = text_to_cstring(fmt_text);

	initStringInfo(&pg_fmt);

	for (i = 0; fmt_str[i] != '\0'; i++)
	{
		if (fmt_str[i] == '%' && fmt_str[i + 1] != '\0')
		{
			const char *pg_spec;

			i++;
			pg_spec = mysql_fmt_to_pg(fmt_str[i]);
			if (pg_spec != NULL)
				appendStringInfoString(&pg_fmt, pg_spec);
			else
			{
				appendStringInfoChar(&pg_fmt, '%');
				appendStringInfoChar(&pg_fmt, fmt_str[i]);
			}
		}
		else
		{
			appendStringInfoChar(&pg_fmt, fmt_str[i]);
		}
	}

	{
		text   *str_arg = cstring_to_text(str);
		text   *pg_fmt_text = cstring_to_text(pg_fmt.data);

		result = DatumGetTimestampTz(DirectFunctionCall2(to_timestamp,
														 PointerGetDatum(str_arg),
														 PointerGetDatum(pg_fmt_text)));
		pfree(str_arg);
		pfree(pg_fmt_text);
	}

	pfree(str);
	pfree(fmt_str);
	pfree(pg_fmt.data);
	PG_RETURN_TIMESTAMP(result);
}


/*
 * DATEDIFF(expr1, expr2)
 * Returns expr1 - expr2 in days.
 * Both arguments are treated as dates.
 */
PG_FUNCTION_INFO_V1(mysql_datediff);
Datum
mysql_datediff(PG_FUNCTION_ARGS)
{
	DateADT		d1, d2;
	int32		diff;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	d1 = PG_GETARG_DATEADT(0);
	d2 = PG_GETARG_DATEADT(1);

	diff = d1 - d2;
	PG_RETURN_INT32(diff);
}


/*
 * TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)
 * Returns datetime_expr2 - datetime_expr1 in the specified unit.
 * Units: MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR
 */
PG_FUNCTION_INFO_V1(mysql_timestampdiff);
Datum
mysql_timestampdiff(PG_FUNCTION_ARGS)
{
	text	   *unit;
	Timestamp	ts1, ts2;
	Interval   *interval;
	int64		diff;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	unit = PG_GETARG_TEXT_P(0);
	ts1 = PG_GETARG_TIMESTAMP(1);
	ts2 = PG_GETARG_TIMESTAMP(2);

	/* ts2 - ts1 */
	interval = DatumGetIntervalP(DirectFunctionCall2(timestamp_mi,
													 TimestampGetDatum(ts2),
													 TimestampGetDatum(ts1)));

	{
		char   *unit_str = text_to_cstring(unit);

		if (strcasecmp(unit_str, "MICROSECOND") == 0)
			diff = (interval->time + interval->day * USECS_PER_DAY + interval->month * 30 * USECS_PER_DAY);
		else if (strcasecmp(unit_str, "SECOND") == 0)
			diff = (interval->time / USECS_PER_SEC) + interval->day * SECS_PER_DAY + interval->month * 30 * SECS_PER_DAY;
		else if (strcasecmp(unit_str, "MINUTE") == 0)
			diff = (interval->time / USECS_PER_MINUTE) + interval->day * (SECS_PER_DAY / 60) + interval->month * 30 * (SECS_PER_DAY / 60);
		else if (strcasecmp(unit_str, "HOUR") == 0)
			diff = (interval->time / USECS_PER_HOUR) + interval->day * 24 + interval->month * 30 * 24;
		else if (strcasecmp(unit_str, "DAY") == 0)
			diff = interval->time / USECS_PER_DAY + interval->day + interval->month * 30;
		else if (strcasecmp(unit_str, "WEEK") == 0)
			diff = (interval->time / USECS_PER_DAY + interval->day + interval->month * 30) / 7;
		else if (strcasecmp(unit_str, "MONTH") == 0)
			diff = interval->month + interval->day / 30 + interval->time / (30 * USECS_PER_DAY);
		else if (strcasecmp(unit_str, "QUARTER") == 0)
			diff = (interval->month + interval->day / 30) / 3;
		else if (strcasecmp(unit_str, "YEAR") == 0)
			diff = (interval->month + interval->day / 30) / 12;
		else
		{
			pfree(unit_str);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid unit: %s", unit_str)));
		}
		pfree(unit_str);
	}

	PG_RETURN_INT64(diff);
}


/*
 * TIMESTAMPADD(unit, interval, datetime_expr)
 * Returns datetime_expr + interval in the specified unit.
 * Units: same as TIMESTAMPDIFF.
 */
PG_FUNCTION_INFO_V1(mysql_timestampadd);
Datum
mysql_timestampadd(PG_FUNCTION_ARGS)
{
	text	   *unit;
	int32		value;
	Timestamp	ts;
	Interval   *pg_interval;
	Timestamp	result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	unit = PG_GETARG_TEXT_P(0);
	value = PG_GETARG_INT32(1);
	ts = PG_GETARG_TIMESTAMP(2);

	pg_interval = (Interval *) palloc(sizeof(Interval));
	pg_interval->month = 0;
	pg_interval->day = 0;
	pg_interval->time = 0;

	{
		char   *unit_str = text_to_cstring(unit);

		if (strcasecmp(unit_str, "MICROSECOND") == 0)
			pg_interval->time = value;
		else if (strcasecmp(unit_str, "SECOND") == 0)
			pg_interval->time = value * USECS_PER_SEC;
		else if (strcasecmp(unit_str, "MINUTE") == 0)
			pg_interval->time = value * USECS_PER_MINUTE;
		else if (strcasecmp(unit_str, "HOUR") == 0)
			pg_interval->time = value * USECS_PER_HOUR;
		else if (strcasecmp(unit_str, "DAY") == 0)
			pg_interval->day = value;
		else if (strcasecmp(unit_str, "WEEK") == 0)
			pg_interval->day = value * 7;
		else if (strcasecmp(unit_str, "MONTH") == 0)
			pg_interval->month = value;
		else if (strcasecmp(unit_str, "QUARTER") == 0)
			pg_interval->month = value * 3;
		else if (strcasecmp(unit_str, "YEAR") == 0)
			pg_interval->month = value * 12;
		else
		{
			pfree(unit_str);
			pfree(pg_interval);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid unit: %s", unit_str)));
		}
		pfree(unit_str);
	}

	result = DatumGetTimestamp(DirectFunctionCall2(timestamp_pl_interval,
												   TimestampGetDatum(ts),
												   IntervalPGetDatum(pg_interval)));
	pfree(pg_interval);
	PG_RETURN_TIMESTAMP(result);
}


/*
 * LAST_DAY(date)
 * Returns the last day of the month for the given date.
 */
PG_FUNCTION_INFO_V1(mysql_last_day);
Datum
mysql_last_day(PG_FUNCTION_ARGS)
{
	DateADT		d;
	int32		year, month, day;
	int			last_day;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	d = PG_GETARG_DATEADT(0);

	/* Convert to year/month/day */
	{
		int			date = d + POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
		int			y, m;

		/* Julian date to year/month/day */
		j2date(date, &year, &month, &day);
	}

	/* Compute last day of that month */
	switch (month)
	{
		case 1: case 3: case 5: case 7: case 8: case 10: case 12:
			last_day = 31;
			break;
		case 4: case 6: case 9: case 11:
			last_day = 30;
			break;
		case 2:
			/* Check leap year */
			if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))
				last_day = 29;
			else
				last_day = 28;
			break;
		default:
			last_day = 31;
			break;
	}

	{
		DateADT result = date2j(year, month, last_day) - POSTGRES_EPOCH_JDATE;
		PG_RETURN_DATEADT(result);
	}
}
