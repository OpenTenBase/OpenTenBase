#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "catalog/pg_type.h"
#include "common/int.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#ifdef _PG_ORCL_
#include "utils/guc.h"
#include "utils/date.h"
#include "datatype/timestamp.h"
#include "utils/numeric.h"
#include "utils/orcl_datetime.h"
#include "utils/errcodes.h"
#endif

PG_FUNCTION_INFO_V1(in_range_date_interval_ex);
PG_FUNCTION_INFO_V1(in_range_time_interval_ex);
PG_FUNCTION_INFO_V1(in_range_timetz_interval_ex);
PG_FUNCTION_INFO_V1(in_range_int4_int4_ex);
PG_FUNCTION_INFO_V1(in_range_int4_int2_ex);
PG_FUNCTION_INFO_V1(in_range_int4_int8_ex);
PG_FUNCTION_INFO_V1(in_range_int2_int4_ex);
PG_FUNCTION_INFO_V1(in_range_int2_int2_ex);
PG_FUNCTION_INFO_V1(in_range_int2_int8_ex);
PG_FUNCTION_INFO_V1(in_range_int8_int8_ex);
PG_FUNCTION_INFO_V1(in_range_timestamptz_interval_ex);
PG_FUNCTION_INFO_V1(in_range_timestamp_interval_ex);
PG_FUNCTION_INFO_V1(in_range_interval_interval_ex);
PG_FUNCTION_INFO_V1(in_range_numeric_numeric_ex);
PG_FUNCTION_INFO_V1(in_range_float8_float8_ex);
PG_FUNCTION_INFO_V1(in_range_float4_float8_ex);
/*
 * in_range support function for date.
 *
 * We implement this by promoting the dates to timestamp (without time zone)
 * and then using the timestamp-and-interval in_range function.
 */
Datum
in_range_date_interval_ex(PG_FUNCTION_ARGS)
{
	return in_range_date_interval(fcinfo);
}

/*
 * in_range support function for time.
 */
Datum
in_range_time_interval_ex(PG_FUNCTION_ARGS)
{
	return in_range_time_interval(fcinfo);
}

/*
 * in_range support function for timetz.
 */
Datum
in_range_timetz_interval_ex(PG_FUNCTION_ARGS)
{
	return in_range_timetz_interval(fcinfo);
}

/*----------------------------------------------------------
 *	in_range functions for int4 and int2,
 *	including cross-data-type comparisons.
 *
 *	Note: we provide separate intN_int8 functions for performance
 *	reasons.  This forces also providing intN_int2, else cases with a
 *	smallint offset value would fail to resolve which function to use.
 *	But that's an unlikely situation, so don't duplicate code for it.
 *---------------------------------------------------------*/

Datum
in_range_int4_int4_ex(PG_FUNCTION_ARGS)
{
	return in_range_int4_int4(fcinfo);
}

Datum
in_range_int4_int2_ex(PG_FUNCTION_ARGS)
{
	return in_range_int4_int2(fcinfo);
}

Datum
in_range_int4_int8_ex(PG_FUNCTION_ARGS)
{
	return in_range_int4_int8(fcinfo);
}

Datum
in_range_int2_int4_ex(PG_FUNCTION_ARGS)
{
	return in_range_int2_int4(fcinfo);
}

Datum
in_range_int2_int2_ex(PG_FUNCTION_ARGS)
{
	return in_range_int2_int2(fcinfo);
}

Datum
in_range_int2_int8_ex(PG_FUNCTION_ARGS)
{
	return in_range_int2_int8(fcinfo);
}

/*
 * in_range support function for int8.
 *
 * Note: we needn't supply int8_int4 or int8_int2 variants, as implicit
 * coercion of the offset value takes care of those scenarios just as well.
 */
Datum
in_range_int8_int8_ex(PG_FUNCTION_ARGS)
{
	return in_range_int8_int8(fcinfo);
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
in_range_timestamptz_interval_ex(PG_FUNCTION_ARGS)
{
	return in_range_timestamptz_interval(fcinfo);
}

Datum
in_range_timestamp_interval_ex(PG_FUNCTION_ARGS)
{
	return in_range_timestamp_interval(fcinfo);
}

Datum
in_range_interval_interval_ex(PG_FUNCTION_ARGS)
{
	return in_range_interval_interval(fcinfo);
}

/*
 * in_range support function for numeric.
 */
Datum
in_range_numeric_numeric_ex(PG_FUNCTION_ARGS)
{
	return in_range_numeric_numeric(fcinfo);
}

/*
 * in_range support function for float8.
 *
 * Note: we needn't supply a float8_float4 variant, as implicit coercion
 * of the offset value takes care of that scenario just as well.
 */
Datum
in_range_float8_float8_ex(PG_FUNCTION_ARGS)
{
	return in_range_float8_float8(fcinfo);
}

/*
 * in_range support function for float4.
 *
 * We would need a float4_float8 variant in any case, so we supply that and
 * let implicit coercion take care of the float4_float4 case.
 */
Datum
in_range_float4_float8_ex(PG_FUNCTION_ARGS)
{
	return in_range_float4_float8(fcinfo);
}
