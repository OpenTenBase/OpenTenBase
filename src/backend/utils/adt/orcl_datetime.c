/*-------------------------------------------------------------------------
 *
 * orcl_datetime.c
 *     functions for datetime types in opentenbase_ora 19c
 *
 * src/backend/utils/adt/orcl_datetime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>
#include <time.h>
#include <string.h>

#include "access/hash.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "utils/orcl_datetime.h"
#include "utils/orcl_datetime_formatting.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
#include "common/int128.h"
/*
 * The interval literal "-1 1:00:00" is ambiguous in setting IntervalStyle to
 * postgres or sql_standard. When setting sql_standard, the leading sign is
 * applied to all fields behind if there is no other explicit sign. And it is
 * the only input and output style of interval in opentenbase_ora. Moreover, the literal
 * "1 -1:00:00" is rejected in opentenbase_ora but accepted in PG when IntervalStyle is
 * postgres. Do some equivalent conversions on the members of an Interval to
 * make sure they are all none-negative or all none_positive so that it could
 * be encoded correctly in sql_standard style.
 *
 * For example, input "-1 1:00:00" or "1 -1:00:00" in postgres intervalstyle:
 * 		origin span = {time = 3600000000, day = -1, month = 0},
 * 		final span  = {time = -82800000000, day = 0, month = 0};
 *
 * 		origin span = {time = -3600000000, day = 1, month = 0},
 * 		final span  = {time = 82800000000, day = 0, month = 0}.
 */
void
IntervalForSQLStandard(Interval *span)
{
	int sign_flag = 0; /* mark if the interval is positive, negative or zero by
						* 1, -1 and 0 */

	/* 
	 * Non-opentenbase_ora standard syntax requires conversion
	 * Standard opentenbase_ora needs to carry if the signs are different
	 */
	if (span->month != 0 && (span->day != 0 || span->time != 0))
	{
		/* Convert seconds to days */
		if (labs(span->time) >= USECS_PER_DAY)
		{
			span->day += span->time / USECS_PER_DAY;
			span->time %= USECS_PER_DAY;
		}
        
		/* Convert days to months */
		if (abs(span->day) >= DAYS_PER_MONTH)
		{
			span->month += span->day / DAYS_PER_MONTH;
			span->day %= DAYS_PER_MONTH;
		}
	}
	else if ((span->month == 0 && span->day > 0 && span->time < 0) ||
			(span->month == 0 && span->day < 0 && span->time > 0)) 
	{
		/* Convert seconds to days */
		if (labs(span->time) >= USECS_PER_DAY)
		{
			span->day += span->time / USECS_PER_DAY;
			span->time %= USECS_PER_DAY;
		}
	}

	/*
	 * suppossing span->day is in the range of [-DAYS_PER_MONTH, DAYS_PER_MONTH]
	 * and span->time is in the range of [-USECS_PER_DAY, USECS_PER_DAY], check
	 * from the higher place to set sign_flag
	 */
	if (span->month > 0)
		sign_flag = 1;
	else if (span->month < 0)
		sign_flag = -1;
	else if (span->day > 0)
		sign_flag = 1;
	else if (span->day < 0)
		sign_flag = -1;
	else if (span->time > 0)
		sign_flag = 1;
	else if (span->time < 0)
		sign_flag = -1;

	/* the conversion below must make sure span is equivalent to the origin */
	if (sign_flag == 1)
	{
		/* select '2 year 3 day -13 second'::interval */
		if (span->time < 0 && (span->day > 0 || span->month > 0))
		{
			span->time += USECS_PER_DAY;
			span->day -= 1;
		}
		if (span->day < 0)
		{
			span->day += DAYS_PER_MONTH;
			span->month -= 1;
		}
		Assert(!(span->month < 0 || span->day < 0 || span->time < 0));
	}
	else if (sign_flag == -1)
	{
		/* select '-2 year -3 day +13 second'::interval */
		if (span->time > 0 && (span->day < 0 || span->month < 0))
		{
			span->time -= USECS_PER_DAY;
			span->day += 1;
		}
		if (span->day > 0)
		{
			span->day -= DAYS_PER_MONTH;
			span->month += 1;
		}
		Assert(!(span->month > 0 || span->day > 0 || span->time > 0));
	}
}
