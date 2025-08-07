/*-------------------------------------------------------------------------
 *
 * compat.c
 *		Reimplementations of various backend functions.
 *
 * Portions Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/bin/pg_waldump/compat.c
 *
 * This file contains client-side implementations for various backend
 * functions that the rm_desc functions in *desc.c files rely on.
 *
 *-------------------------------------------------------------------------
 */

/* ugly hack, same as in e.g pg_controldata */
#define FRONTEND 1
#include "postgres.h"

#include <time.h>

#include "utils/datetime.h"
#include "lib/stringinfo.h"

/* copied from timestamp.c */
pg_time_t
timestamptz_to_time_t(TimestampTz t)
{
	pg_time_t	result;

	result = (pg_time_t) (t / USECS_PER_SEC +
						  ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
	return result;
}

/*
 * Stopgap implementation of timestamptz_to_str that doesn't depend on backend
 * infrastructure.  This will work for timestamps that are within the range
 * of the platform time_t type.  (pg_time_t is compatible except for possibly
 * being wider.)
 *
 * XXX the return value points to a static buffer, so beware of using more
 * than one result value concurrently.
 *
 * XXX: The backend timestamp infrastructure should instead be split out and
 * moved into src/common.  That's a large project though.
 */
const char *
timestamptz_to_str(TimestampTz dt)
{
	static char buf[MAXDATELEN * 2 + 12]; /* avoid compiler warning */
	char		ts[MAXDATELEN + 1];
	char		zone[MAXDATELEN + 1];
	time_t		result = (time_t) timestamptz_to_time_t(dt);
	struct tm  *ltime = localtime(&result);

	strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
	strftime(zone, sizeof(zone), "%Z", ltime);

	sprintf(buf, "%s.%06d %s", ts, (int) (dt % USECS_PER_SEC), zone);

	return buf;
}

/*
 * Provide a hacked up compat layer for StringInfos so xlog desc functions can
 * be linked/called.
 */
void
appendStringInfo(StringInfo str, const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	vprintf(fmt, args);
	va_end(args);
}

void
appendStringInfoString(StringInfo str, const char *string)
{
	appendStringInfo(str, "%s", string);
}

void
appendStringInfoChar(StringInfo str, char ch)
{
	appendStringInfo(str, "%c", ch);
}
