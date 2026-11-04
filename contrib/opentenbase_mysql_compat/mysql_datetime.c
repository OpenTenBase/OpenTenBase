/*-------------------------------------------------------------------------
 *
 * mysql_datetime.c
 *		MySQL-compatible date and time functions for
 *		opentenbase_mysql_compat.
 *
 * Implements: DATE_FORMAT, STR_TO_DATE, DATEDIFF, TIMESTAMPDIFF,
 * TIMESTAMPADD, LAST_DAY and QUARTER with MySQL semantics.
 *
 * DATE_FORMAT/STR_TO_DATE share one format-specification scanner that
 * understands the MySQL % specifiers (English lc_time_names, which is
 * the MySQL default).
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/mysql_datetime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

PG_FUNCTION_INFO_V1(mysql_date_format);
PG_FUNCTION_INFO_V1(mysql_str_to_date);
PG_FUNCTION_INFO_V1(mysql_datediff);
PG_FUNCTION_INFO_V1(mysql_timestampdiff);
PG_FUNCTION_INFO_V1(mysql_timestampadd);
PG_FUNCTION_INFO_V1(mysql_last_day);

static const char *const month_names[] = {
	"January", "February", "March", "April", "May", "June", "July",
	"August", "September", "October", "November", "December", NULL
};

static const char *const month_names_abbr[] = {
	"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
	"Oct", "Nov", "Dec", NULL
};

static const char *const day_names[] = {
	"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
	"Sunday", NULL
};

static const char *const day_names_abbr[] = {
	"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", NULL
};

static const int month_days[2][13] = {
	{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

#define MYSQL_IS_LEAP_YEAR(y) \
	(((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

#define MONTH_DAYS(y, m) \
	(month_days[((y) > 0 && MYSQL_IS_LEAP_YEAR(y)) ? 1 : 0][m])

/* Upper-case ASCII letters in place, for unit-name matching. */
static char *
mysql_strupper(char *s)
{
	char	   *p;

	for (p = s; *p; p++)
		*p = pg_toupper((unsigned char) *p);
	return s;
}

/* Day of week with Sunday = 0, computed from the Julian date. */
static int
jday_dow_sunday(long jday)
{
	return (int) ((jday + 1L) % 7L);
}

static void
append_ordinal_day(StringInfo buf, int day)
{
	int			rem11 = day % 100;
	int			rem10 = day % 10;

	appendStringInfo(buf, "%d", day);
	if (rem11 >= 11 && rem11 <= 13)
		appendStringInfoString(buf, "th");
	else if (rem10 == 1)
		appendStringInfoString(buf, "st");
	else if (rem10 == 2)
		appendStringInfoString(buf, "nd");
	else if (rem10 == 3)
		appendStringInfoString(buf, "rd");
	else
		appendStringInfoString(buf, "th");
}

/*
 * Week number of the Sunday that starts the given date's week,
 * counted within that Sunday's own year (1-based, 1..53).  This is
 * the MySQL %V value; the year of that Sunday is stored in *wkyear
 * for %X.
 */
static int
sunday_week(int y, int m, int d, int *wkyear)
{
	long		jday = date2j(y, m, d);
	int			dow = jday_dow_sunday(jday);		/* 0 = Sunday */
	long		sun_jday = jday - dow;
	int			sy,
				sm,
				sd;
	long		jan1;
	int			jan1_dow;
	long		first_sun;

	j2date(sun_jday, &sy, &sm, &sd);
	jan1 = date2j(sy, 1, 1);
	jan1_dow = jday_dow_sunday(jan1);
	first_sun = jan1 + ((7 - jan1_dow) % 7);

	if (wkyear)
		*wkyear = sy;

	/*
	 * The week-Sunday is the first day of a week by definition, so it
	 * can never fall before the year's first Sunday within its own
	 * year.
	 */
	return 1 + (int) ((sun_jday - first_sun) / 7);
}

/*
 * MySQL %U: week number with Sunday as first day; days before the
 * year's first Sunday belong to week 0.
 */
static int
sunday_week_u(int y, int m, int d)
{
	long		jday = date2j(y, m, d);
	int			dow = jday_dow_sunday(jday);
	long		sun_jday = jday - dow;
	int			sy,
				sm,
				sd;

	j2date(sun_jday, &sy, &sm, &sd);
	if (sy < y)
		return 0;

	return sunday_week(y, m, d, NULL);
}

/*
 * MySQL %u: week number with Monday as first day; days before the
 * year's first Monday belong to week 0.
 */
static int
monday_week(int y, int m, int d)
{
	long		jday = date2j(y, m, d);
	int			dow = jday_dow_sunday(jday);
	int			isomon = (dow == 0) ? 6 : dow - 1;	/* 0 = Monday */
	long		mon_jday = jday - isomon;
	int			my_,
				mm,
				md;
	long		jan1;
	int			jan1_dow;
	int			jan1_isomon;
	long		first_mon;

	j2date(mon_jday, &my_, &mm, &md);
	if (my_ < y)
		return 0;

	jan1 = date2j(y, 1, 1);
	jan1_dow = jday_dow_sunday(jan1);
	jan1_isomon = (jan1_dow == 0) ? 6 : jan1_dow - 1;
	first_mon = jan1 + ((7 - jan1_isomon) % 7);

	return 1 + (int) ((mon_jday - first_mon) / 7);
}

static int
iso_weeks_in_year(int y)
{
	long		jan1 = date2j(y, 1, 1);
	int			dow = jday_dow_sunday(jan1);	/* 0 = Sunday */
	int			isodow = (dow == 0) ? 7 : dow;

	if (isodow == 4)
		return 53;
	if (isodow == 3 && MYSQL_IS_LEAP_YEAR(y))
		return 53;
	return 52;
}

/*
 * ISO-8601 week number and year, used for MySQL %v/%x.
 */
static void
iso_week(int y, int m, int d, int *week, int *iyear)
{
	long		jday = date2j(y, m, d);
	int			dow = jday_dow_sunday(jday);
	int			isodow = (dow == 0) ? 7 : dow;		/* 1 = Monday */
	int			doy = (int) (jday - date2j(y, 1, 1)) + 1;
	int			w = (doy - isodow + 10) / 7;

	if (w < 1)
	{
		*iyear = y - 1;
		*week = iso_weeks_in_year(y - 1);
	}
	else if (w > iso_weeks_in_year(y))
	{
		*iyear = y + 1;
		*week = 1;
	}
	else
	{
		*iyear = y;
		*week = w;
	}
}

/*
 * mysql_date_format(timestamp, format text) -> text
 */
Datum
mysql_date_format(PG_FUNCTION_ARGS)
{
	Timestamp	ts;
	char	   *fmt;
	struct pg_tm tm;
	fsec_t		fsec;
	char		spec;
	StringInfoData res;
	const char *f;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	ts = PG_GETARG_TIMESTAMP(0);
	fmt = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	initStringInfo(&res);

	for (f = fmt; *f; f++)
	{
		if (*f != '%')
		{
			appendStringInfoChar(&res, *f);
			continue;
		}

		spec = *(++f);
		switch (spec)
		{
			case 'a':			/* abbreviated weekday name */
				appendStringInfoString(&res,
									   day_names_abbr[(jday_dow_sunday(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday)) + 6) % 7]);
				break;
			case 'b':			/* abbreviated month name */
				appendStringInfoString(&res, month_names_abbr[tm.tm_mon - 1]);
				break;
			case 'c':			/* month, numeric */
				appendStringInfo(&res, "%d", tm.tm_mon);
				break;
			case 'D':			/* day of month with English suffix */
				append_ordinal_day(&res, tm.tm_mday);
				break;
			case 'd':			/* day of month, zero padded */
				appendStringInfo(&res, "%02d", tm.tm_mday);
				break;
			case 'e':			/* day of month */
				appendStringInfo(&res, "%d", tm.tm_mday);
				break;
			case 'f':			/* microseconds */
				appendStringInfo(&res, "%06d", (int) fsec);
				break;
			case 'H':			/* hour, 24h, zero padded */
				appendStringInfo(&res, "%02d", tm.tm_hour);
				break;
			case 'h':			/* hour, 12h, zero padded */
			case 'I':
				appendStringInfo(&res, "%02d",
								 tm.tm_hour % 12 == 0 ? 12 : tm.tm_hour % 12);
				break;
			case 'i':			/* minutes */
				appendStringInfo(&res, "%02d", tm.tm_min);
				break;
			case 'j':			/* day of year */
				appendStringInfo(&res, "%03d",
								 (int) (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) -
										date2j(tm.tm_year, 1, 1)) + 1);
				break;
			case 'k':			/* hour, 24h */
				appendStringInfo(&res, "%d", tm.tm_hour);
				break;
			case 'l':			/* hour, 12h */
				appendStringInfo(&res, "%d",
								 tm.tm_hour % 12 == 0 ? 12 : tm.tm_hour % 12);
				break;
			case 'M':			/* month name */
				appendStringInfoString(&res, month_names[tm.tm_mon - 1]);
				break;
			case 'm':			/* month, zero padded */
				appendStringInfo(&res, "%02d", tm.tm_mon);
				break;
			case 'p':			/* AM or PM */
				appendStringInfoString(&res, tm.tm_hour < 12 ? "AM" : "PM");
				break;
			case 'r':			/* hh:mm:ss followed by AM or PM */
				appendStringInfo(&res, "%02d:%02d:%02d %s",
								 tm.tm_hour % 12 == 0 ? 12 : tm.tm_hour % 12,
								 tm.tm_min, tm.tm_sec,
								 tm.tm_hour < 12 ? "AM" : "PM");
				break;
			case 'S':			/* seconds */
			case 's':
				appendStringInfo(&res, "%02d", tm.tm_sec);
				break;
			case 'T':			/* hh:mm:ss, 24h */
				appendStringInfo(&res, "%02d:%02d:%02d",
								 tm.tm_hour, tm.tm_min, tm.tm_sec);
				break;
			case 'U':			/* week, Sunday first day */
				appendStringInfo(&res, "%02d",
								 sunday_week_u(tm.tm_year, tm.tm_mon, tm.tm_mday));
				break;
			case 'u':			/* week, Monday first day */
				appendStringInfo(&res, "%02d",
								 monday_week(tm.tm_year, tm.tm_mon, tm.tm_mday));
				break;
			case 'V':			/* week, Sunday first, 1..53 as %X */
				{
					int			wkyear;

					appendStringInfo(&res, "%02d",
									 sunday_week(tm.tm_year, tm.tm_mon, tm.tm_mday,
												 &wkyear));
				}
				break;
			case 'v':			/* ISO week 1..53, as %x */
				{
					int			w,
								iy;

					iso_week(tm.tm_year, tm.tm_mon, tm.tm_mday, &w, &iy);
					appendStringInfo(&res, "%02d", w);
				}
				break;
			case 'W':			/* weekday name */
				appendStringInfoString(&res,
									   day_names[(jday_dow_sunday(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday)) + 6) % 7]);
				break;
			case 'w':			/* day of week, 0 = Sunday */
				appendStringInfo(&res, "%d",
								 jday_dow_sunday(date2j(tm.tm_year, tm.tm_mon, tm.tm_mday)));
				break;
			case 'X':			/* year for the Sunday-based week */
				{
					int			wkyear;

					(void) sunday_week(tm.tm_year, tm.tm_mon, tm.tm_mday,
									   &wkyear);
					appendStringInfo(&res, "%04d", wkyear);
				}
				break;
			case 'x':			/* ISO year */
				{
					int			w,
								iy;

					iso_week(tm.tm_year, tm.tm_mon, tm.tm_mday, &w, &iy);
					appendStringInfo(&res, "%04d", iy);
				}
				break;
			case 'Y':			/* four-digit year */
				appendStringInfo(&res, "%04d", tm.tm_year);
				break;
			case 'y':			/* two-digit year */
				appendStringInfo(&res, "%02d", tm.tm_year % 100);
				break;
			case '%':			/* literal % */
				appendStringInfoChar(&res, '%');
				break;
			case '\0':			/* stray % at end of format */
				appendStringInfoChar(&res, '%');
				goto done;
			default:			/* literal for unknown specifiers */
				appendStringInfoChar(&res, spec);
				break;
		}
	}
done:

	PG_RETURN_TEXT_P(cstring_to_text(res.data));
}

/* ---------------------- STR_TO_DATE support ---------------------- */

typedef struct StrToDateCtx
{
	/* input */
	const char *in;
	/* collected fields; -1 marks "not seen" for numeric fields */
	int			year;
	int			mon;
	int			mday;
	int			hour;
	int			min;
	int			sec;
	int			usec;
	int			pm;			/* 1 = PM seen, 0 = AM seen, -1 = no %p */
	int			hour12;		/* hour was set through a 12h specifier */
} StrToDateCtx;

static const char *
skip_input_space(const char *p)
{
	while (*p && isspace((unsigned char) *p))
		p++;
	return p;
}

/*
 * Read between 1 and maxdigits decimal digits; returns NULL when the
 * input does not start with a digit.
 */
static const char *
read_digits(const char *p, int maxdigits, int *value)
{
	int			v = 0;
	int			n = 0;

	p = skip_input_space(p);
	while (n < maxdigits && isdigit((unsigned char) *p))
	{
		v = v * 10 + (*p - '0');
		p++;
		n++;
	}
	if (n == 0)
		return NULL;
	*value = v;
	return p;
}

/*
 * Match one of the names in the NULL-terminated list case-insensitively;
 * returns the index of the match (0-based) or -1.
 */
static int
match_name(const char **p, const char *const *names)
{
	const char *start = *p;
	int			i;

	for (i = 0; names[i] != NULL; i++)
	{
		size_t		len = strlen(names[i]);

		if (pg_strncasecmp(start, names[i], len) == 0)
		{
			*p = start + len;
			return i;
		}
	}
	return -1;
}

/*
 * Parse str according to the MySQL format string.  Returns true on
 * success; on failure returns false or raises an error through
 * report_bad_input() for clearly malformed values.
 */
static bool
str_to_date_internal(const char *str, const char *fmt, StrToDateCtx *ctx)
{
	const char *p = str;
	const char *f = fmt;
	char		spec;

	memset(ctx, 0, sizeof(StrToDateCtx));
	ctx->year = ctx->mon = ctx->mday = -1;
	ctx->hour = ctx->min = ctx->sec = ctx->usec = -1;
	ctx->pm = -1;
	ctx->hour12 = 0;

	while (*f)
	{
		if (*f != '%')
		{
			if (isspace((unsigned char) *f))
			{
				f = skip_input_space(f);
				p = skip_input_space(p);
			}
			else if (ispunct((unsigned char) *f))
			{
				/*
				 * MySQL is lenient about punctuation: one punctuation
				 * character in the format consumes one from the input.
				 */
				p = skip_input_space(p);
				if (!ispunct((unsigned char) *p))
					return false;
				p++;
				f++;
			}
			else
			{
				p = skip_input_space(p);
				if (*p++ != *f)
					return false;
				f++;
			}
			continue;
		}

		spec = *(++f);
		if (spec == '\0')
			return false;
		f++;

		switch (spec)
		{
			case 'Y':
				if ((p = read_digits(p, 4, &ctx->year)) == NULL)
					return false;
				break;
			case 'y':
				{
					int			v;

					if ((p = read_digits(p, 2, &v)) == NULL)
						return false;
					ctx->year = (v >= 0 && v <= 69) ? 2000 + v : 1900 + v;
				}
				break;
			case 'm':
			case 'c':
				if ((p = read_digits(p, 2, &ctx->mon)) == NULL)
					return false;
				break;
			case 'd':
			case 'e':
				if ((p = read_digits(p, 2, &ctx->mday)) == NULL)
					return false;
				break;
			case 'D':
				{
					int			v;
					const char *q = read_digits(p, 2, &v);

					if (q == NULL)
						return false;
					/* consume the st/nd/rd/th suffix */
					while (isalpha((unsigned char) *q))
						q++;
					p = q;
					ctx->mday = v;
				}
				break;
			case 'H':
			case 'k':
				if ((p = read_digits(p, 2, &ctx->hour)) == NULL)
					return false;
				break;
			case 'h':
			case 'I':
			case 'l':
				if ((p = read_digits(p, 2, &ctx->hour)) == NULL)
					return false;
				ctx->hour12 = 1;
				break;
			case 'i':
				if ((p = read_digits(p, 2, &ctx->min)) == NULL)
					return false;
				break;
			case 'S':
			case 's':
				if ((p = read_digits(p, 2, &ctx->sec)) == NULL)
					return false;
				break;
			case 'f':
				{
					int			v = 0;
					int			n = 0;

					p = skip_input_space(p);
					while (n < 6 && isdigit((unsigned char) *p))
					{
						v = v * 10 + (*p - '0');
						p++;
						n++;
					}
					if (n == 0)
						return false;
					/* right-pad to microseconds */
					while (n < 6)
					{
						v *= 10;
						n++;
					}
					ctx->usec = v;
				}
				break;
			case 'j':
				{
					int			doy;

					if ((p = read_digits(p, 3, &doy)) == NULL)
						return false;
					/* stored; converted once the year is known */
					ctx->mday = -doy;	/* negative marks day-of-year */
				}
				break;
			case 'M':
				{
					int			idx = match_name(&p, month_names);

					if (idx < 0)
						return false;
					ctx->mon = idx + 1;
				}
				break;
			case 'b':
				{
					int			idx = match_name(&p, month_names_abbr);

					if (idx < 0)
						return false;
					ctx->mon = idx + 1;
				}
				break;
			case 'W':
				{
					int			idx = match_name(&p, day_names);

					if (idx < 0)
						return false;
					/* day name carries no data for building the date */
				}
				break;
			case 'a':
				{
					int			idx = match_name(&p, day_names_abbr);

					if (idx < 0)
						return false;
				}
				break;
			case 'p':
				p = skip_input_space(p);
				if (pg_strncasecmp(p, "AM", 2) == 0)
				{
					ctx->pm = 0;
					p += 2;
				}
				else if (pg_strncasecmp(p, "PM", 2) == 0)
				{
					ctx->pm = 1;
					p += 2;
				}
				else
					return false;
				break;
			case 'r':
				{
					int			h,
								m,
								s;
					const char *q;

					q = read_digits(p, 2, &h);
					if (q == NULL || *q != ':')
						return false;
					q = read_digits(q + 1, 2, &m);
					if (q == NULL || *q != ':')
						return false;
					q = read_digits(q + 1, 2, &s);
					if (q == NULL)
						return false;
					q = skip_input_space(q);
					if (pg_strncasecmp(q, "AM", 2) == 0)
					{
						ctx->pm = 0;
						q += 2;
					}
					else if (pg_strncasecmp(q, "PM", 2) == 0)
					{
						ctx->pm = 1;
						q += 2;
					}
					else
						return false;
					ctx->hour = h;
					ctx->min = m;
					ctx->sec = s;
					ctx->hour12 = 1;
					p = q;
				}
				break;
			case 'T':
				{
					int			h,
								m,
								s;
					const char *q;

					q = read_digits(p, 2, &h);
					if (q == NULL || *q != ':')
						return false;
					q = read_digits(q + 1, 2, &m);
					if (q == NULL || *q != ':')
						return false;
					q = read_digits(q + 1, 2, &s);
					if (q == NULL)
						return false;
					ctx->hour = h;
					ctx->min = m;
					ctx->sec = s;
					p = q;
				}
				break;
			case '%':
				p = skip_input_space(p);
				if (*p++ != '%')
					return false;
				break;
			default:
				/* Unknown specifier: require the literal character. */
				p = skip_input_space(p);
				if (*p++ != spec)
					return false;
				break;
		}
	}

	/* MySQL ignores input left over after the format is exhausted. */

	return true;
}

/*
 * Validate the collected fields and produce the timestamp.
 */
static Timestamp
str_to_date_finish(StrToDateCtx *ctx)
{
	struct pg_tm tm;
	int			doy;
	fsec_t		fsec = 0;
	Timestamp	result;

	/* Fields not present in the format default like MySQL's zero date. */
	tm.tm_year = (ctx->year >= 0) ? ctx->year : 1;
	tm.tm_mon = (ctx->mon > 0) ? ctx->mon : 1;
	tm.tm_mday = (ctx->mday > 0) ? ctx->mday : 1;
	tm.tm_hour = (ctx->hour >= 0) ? ctx->hour : 0;
	tm.tm_min = (ctx->min >= 0) ? ctx->min : 0;
	tm.tm_sec = (ctx->sec >= 0) ? ctx->sec : 0;

	/* Resolve day-of-year input once year and month state are known. */
	doy = -ctx->mday;
	if (doy > 0)
	{
		long		j = date2j(tm.tm_year, 1, 1) + doy - 1;
		int			y,
					m,
					d;

		j2date(j, &y, &m, &d);
		tm.tm_year = y;
		tm.tm_mon = m;
		tm.tm_mday = d;
	}

	/* Apply AM/PM to a 12-hour clock hour. */
	if (ctx->hour12 && ctx->pm >= 0)
	{
		if (ctx->pm == 1 && tm.tm_hour < 12)
			tm.tm_hour += 12;
		else if (ctx->pm == 0 && tm.tm_hour == 12)
			tm.tm_hour = 0;
	}

	if (tm.tm_mon < 1 || tm.tm_mon > 12)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("month value %d is out of range", tm.tm_mon)));
	if (tm.tm_mday < 1 || tm.tm_mday > MONTH_DAYS(tm.tm_year, tm.tm_mon))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("day value %d is out of range", tm.tm_mday)));
	if (tm.tm_hour < 0 || tm.tm_hour > 23)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("hour value %d is out of range", tm.tm_hour)));
	if (tm.tm_min < 0 || tm.tm_min > 59)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("minute value %d is out of range", tm.tm_min)));
	if (tm.tm_sec < 0 || tm.tm_sec > 59)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
				 errmsg("second value %d is out of range", tm.tm_sec)));

	if (ctx->usec >= 0)
		fsec = (fsec_t) ctx->usec;

	if (tm2timestamp(&tm, fsec, NULL, &result) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	return result;
}

/*
 * mysql_str_to_date(str text, format text) -> timestamp
 */
Datum
mysql_str_to_date(PG_FUNCTION_ARGS)
{
	char	   *str;
	char	   *fmt;
	StrToDateCtx ctx;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	str = text_to_cstring(PG_GETARG_TEXT_PP(0));
	fmt = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (!str_to_date_internal(str, fmt, &ctx))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("invalid value '%s' for format '%s'", str, fmt)));

	PG_RETURN_TIMESTAMP(str_to_date_finish(&ctx));
}

/* -------------------- DATEDIFF / TIMESTAMPDIFF -------------------- */

static void
timestamp_to_tm(Timestamp ts, struct pg_tm *tm)
{
	fsec_t		fsec;

	if (timestamp2tm(ts, NULL, tm, &fsec, NULL, NULL) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));
}

/*
 * mysql_datediff(end, start) -> int
 *
 * Difference in days between the date parts; time-of-day is ignored.
 */
Datum
mysql_datediff(PG_FUNCTION_ARGS)
{
	struct pg_tm tm1,
				tm2;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	timestamp_to_tm(PG_GETARG_TIMESTAMP(0), &tm1);
	timestamp_to_tm(PG_GETARG_TIMESTAMP(1), &tm2);

	PG_RETURN_INT32((int32) (date2j(tm1.tm_year, tm1.tm_mon, tm1.tm_mday) -
							 date2j(tm2.tm_year, tm2.tm_mon, tm2.tm_mday)));
}

/*
 * Compare the time-of-day parts of two broken-down times; returns
 * strcmp-style value.
 */
static int
cmp_time_of_day(const struct pg_tm *a, const struct pg_tm *b)
{
	int64		ta = ((int64) a->tm_hour * 3600 + a->tm_min * 60 + a->tm_sec);
	int64		tb = ((int64) b->tm_hour * 3600 + b->tm_min * 60 + b->tm_sec);

	return (ta < tb) ? -1 : (ta > tb) ? 1 : 0;
}

/*
 * mysql_timestampdiff(unit text, ts1, ts2) -> bigint
 *
 * ts2 - ts1 expressed in complete units of the given kind.
 */
Datum
mysql_timestampdiff(PG_FUNCTION_ARGS)
{
	char	   *unitstr;
	struct pg_tm tm1,
				tm2;
	Timestamp	ts1,
				ts2;
	int64		diff_us;
	int64		result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	unitstr = mysql_strupper(text_to_cstring(PG_GETARG_TEXT_PP(0)));
	ts1 = PG_GETARG_TIMESTAMP(1);
	ts2 = PG_GETARG_TIMESTAMP(2);

	diff_us = (int64) ts2 - (int64) ts1;

	timestamp_to_tm(ts1, &tm1);
	timestamp_to_tm(ts2, &tm2);

	if (strcmp(unitstr, "MICROSECOND") == 0)
		result = diff_us;
	else if (strcmp(unitstr, "SECOND") == 0)
		result = diff_us / INT64CONST(1000000);
	else if (strcmp(unitstr, "MINUTE") == 0)
		result = diff_us / INT64CONST(60000000);
	else if (strcmp(unitstr, "HOUR") == 0)
		result = diff_us / INT64CONST(3600000000);
	else if (strcmp(unitstr, "DAY") == 0)
		result = diff_us / INT64CONST(86400000000);
	else if (strcmp(unitstr, "WEEK") == 0)
		result = diff_us / INT64CONST(604800000000);
	else if (strcmp(unitstr, "MONTH") == 0 ||
			 strcmp(unitstr, "QUARTER") == 0 ||
			 strcmp(unitstr, "YEAR") == 0)
	{
		int64		months = (tm2.tm_year - tm1.tm_year) * 12 +
			(tm2.tm_mon - tm1.tm_mon);

		if (months > 0 &&
			(tm2.tm_mday < tm1.tm_mday ||
			 (tm2.tm_mday == tm1.tm_mday && cmp_time_of_day(&tm2, &tm1) < 0)))
			months--;
		else if (months < 0 &&
				 (tm2.tm_mday > tm1.tm_mday ||
				  (tm2.tm_mday == tm1.tm_mday && cmp_time_of_day(&tm2, &tm1) > 0)))
			months++;

		if (strcmp(unitstr, "MONTH") == 0)
			result = months;
		else if (strcmp(unitstr, "QUARTER") == 0)
			result = months / 3;
		else
			result = months / 12;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown timestamp unit \"%s\"", unitstr)));

	PG_RETURN_INT64(result);
}

/* ------------------------- TIMESTAMPADD ------------------------- */

/*
 * mysql_timestampadd(unit text, count bigint, ts) -> timestamp
 */
Datum
mysql_timestampadd(PG_FUNCTION_ARGS)
{
	char	   *unitstr;
	int64		count;
	Timestamp	ts;
	Timestamp	result;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	unitstr = mysql_strupper(text_to_cstring(PG_GETARG_TEXT_PP(0)));
	count = PG_GETARG_INT64(1);
	ts = PG_GETARG_TIMESTAMP(2);

	if (strcmp(unitstr, "MICROSECOND") == 0)
		result = ts + count;
	else if (strcmp(unitstr, "SECOND") == 0)
		result = ts + count * INT64CONST(1000000);
	else if (strcmp(unitstr, "MINUTE") == 0)
		result = ts + count * INT64CONST(60000000);
	else if (strcmp(unitstr, "HOUR") == 0)
		result = ts + count * INT64CONST(3600000000);
	else if (strcmp(unitstr, "DAY") == 0)
		result = ts + count * INT64CONST(86400000000);
	else if (strcmp(unitstr, "WEEK") == 0)
		result = ts + count * INT64CONST(604800000000);
	else if (strcmp(unitstr, "MONTH") == 0 ||
			 strcmp(unitstr, "QUARTER") == 0 ||
			 strcmp(unitstr, "YEAR") == 0)
	{
		struct pg_tm tm;
		fsec_t		fsec;
		int64		months;

		if (strcmp(unitstr, "MONTH") == 0)
			months = count;
		else if (strcmp(unitstr, "QUARTER") == 0)
			months = count * 3;
		else
			months = count * 12;

		if (timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));

		{
			int64		absmon = (int64) tm.tm_year * 12 + (tm.tm_mon - 1) + months;
			int			y = (int) (absmon / 12);
			int			m = (int) (absmon % 12) + 1;

			if (m <= 0)
			{
				m += 12;
				y -= 1;
			}
			tm.tm_year = y;
			tm.tm_mon = m;
			/* Clamp the day like MySQL does, e.g. Jan 31 + 1 month = Feb 28. */
			if (tm.tm_mday > MONTH_DAYS(y, m))
				tm.tm_mday = MONTH_DAYS(y, m);
		}

		if (tm2timestamp(&tm, fsec, NULL, &result) < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown timestamp unit \"%s\"", unitstr)));

	PG_RETURN_TIMESTAMP(result);
}

/* --------------------------- LAST_DAY --------------------------- */

/*
 * mysql_last_day(timestamp) -> date
 *
 * Last day of the month containing the given timestamp.
 */
Datum
mysql_last_day(PG_FUNCTION_ARGS)
{
	struct pg_tm tm;
	DateADT		result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	timestamp_to_tm(PG_GETARG_TIMESTAMP(0), &tm);

	result = (DateADT) (date2j(tm.tm_year, tm.tm_mon,
								MONTH_DAYS(tm.tm_year, tm.tm_mon)) -
						POSTGRES_EPOCH_JDATE);

	PG_RETURN_DATEADT(result);
}
