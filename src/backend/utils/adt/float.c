/*-------------------------------------------------------------------------
 *
 * float.c
 *	  Functions for the built-in floating-point types.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/float.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>

#include "catalog/pg_type.h"
#include "common/int.h"
#include "common/shortest_dec.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/sortsupport.h"
#include "utils/guc.h"
#include "utils/numeric.h"
#include "pgxc/pgxc.h"


#ifndef M_PI
/* from my RH5.2 gcc math.h file - thomas 2000-04-03 */
#define M_PI 3.14159265358979323846
#endif

/* Radians per degree, a.k.a. PI / 180 */
#define RADIANS_PER_DEGREE 0.0174532925199432957692

/* Visual C++ etc lacks NAN, and won't accept 0.0/0.0.  NAN definition from
 * http://msdn.microsoft.com/library/default.asp?url=/library/en-us/vclang/html/vclrfNotNumberNANItems.asp
 */
#if defined(WIN32) && !defined(NAN)
static const uint32 nan[2] = {0xffffffff, 0x7fffffff};

#define NAN (*(const double *) nan)
#endif

/* not sure what the following should be, but better to make it over-sufficient */
#define MAXFLOATWIDTH	64
#define MAXDOUBLEWIDTH	128

/*
 * check to see if a float4/8 val has underflowed or overflowed
 */
#define CHECKFLOATVAL(val, inf_is_valid, zero_is_valid)			\
do {															\
	if (isinf(val) && !(inf_is_valid))							\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		  errmsg("value out of range: overflow")));				\
																\
	if ((val) == 0.0 && !(zero_is_valid))						\
		ereport(ERROR,											\
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),	\
		 errmsg("value out of range: underflow")));				\
} while(0)


/*
 * Configurable GUC parameter
 *
 * If >0, use shortest-decimal format for output; this is both the default and
 * allows for compatibility with clients that explicitly set a value here to
 * get round-trip-accurate results. If 0 or less, then use the old, slow,
 * decimal rounding method.
 */
int			extra_float_digits = 1;

/* Cached constants for degree-based trig functions */
static bool degree_consts_set = false;
static float8 sin_30 = 0;
static float8 one_minus_cos_60 = 0;
static float8 asin_0_5 = 0;
static float8 acos_0_5 = 0;
static float8 atan_1_0 = 0;
static float8 tan_45 = 0;
static float8 cot_45 = 0;

/*
 * These are intentionally not static; don't "fix" them.  They will never
 * be referenced by other files, much less changed; but we don't want the
 * compiler to know that, else it might try to precompute expressions
 * involving them.  See comments for init_degree_constants().
 */
static float8		degree_c_thirty = 30.0;
static float8		degree_c_forty_five = 45.0;
static float8		degree_c_sixty = 60.0;
static float8		degree_c_one_half = 0.5;
static float8		degree_c_one = 1.0;

/* Local function prototypes */
static double sind_q1(double x);
static double cosd_q1(double x);
static void init_degree_constants(void);

#ifndef HAVE_CBRT
/*
 * Some machines (in particular, some versions of AIX) have an extern
 * declaration for cbrt() in <math.h> but fail to provide the actual
 * function, which causes configure to not set HAVE_CBRT.  Furthermore,
 * their compilers spit up at the mismatch between extern declaration
 * and static definition.  We work around that here by the expedient
 * of a #define to make the actual name of the static function different.
 */
#define cbrt my_cbrt
static double cbrt(double x);
#endif							/* HAVE_CBRT */


/*
 * Routines to provide reasonably platform-independent handling of
 * infinity and NaN.  We assume that isinf() and isnan() are available
 * and work per spec.  (On some platforms, we have to supply our own;
 * see src/port.)  However, generating an Infinity or NaN in the first
 * place is less well standardized; pre-C99 systems tend not to have C99's
 * INFINITY and NAN macros.  We centralize our workarounds for this here.
 */

double
get_float8_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (double) INFINITY;
#else

	/*
	 * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
	 * largest normal double.  We assume forcing an overflow will get us a
	 * true infinity.
	 */
	return (double) (HUGE_VAL * HUGE_VAL);
#endif
}

/*
* The funny placements of the two #pragmas is necessary because of a
* long lived bug in the Microsoft compilers.
* See http://support.microsoft.com/kb/120968/en-us for details
*/
#if (_MSC_VER >= 1800)
#pragma warning(disable:4756)
#endif
float
get_float4_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (float) INFINITY;
#else
#if (_MSC_VER >= 1800)
#pragma warning(default:4756)
#endif

	/*
	 * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
	 * largest normal double.  We assume forcing an overflow will get us a
	 * true infinity.
	 */
	return (float) (HUGE_VAL * HUGE_VAL);
#endif
}

double
get_float8_nan(void)
{
	/* (double) NAN doesn't work on some NetBSD/MIPS releases */
#if defined(NAN) && !(defined(__NetBSD__) && defined(__mips__))
	/* C99 standard way */
	return (double) NAN;
#else
	/* Assume we can get a NAN via zero divide */
	return (double) (0.0 / 0.0);
#endif
}

float
get_float4_nan(void)
{
#ifdef NAN
	/* C99 standard way */
	return (float) NAN;
#else
	/* Assume we can get a NAN via zero divide */
	return (float) (0.0 / 0.0);
#endif
}


/*
 * Returns -1 if 'val' represents negative infinity, 1 if 'val'
 * represents (positive) infinity, and 0 otherwise. On some platforms,
 * this is equivalent to the isinf() macro, but not everywhere: C99
 * does not specify that isinf() needs to distinguish between positive
 * and negative infinity.
 */
int
is_infinite(double val)
{
	int			inf = isinf(val);

	if (inf == 0)
		return 0;
	else if (val > 0)
		return 1;
	else
		return -1;
}


/* ========== USER I/O ROUTINES ========== */


/*
 *		float4in		- converts "num" to float4
 */
Datum
float4in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);
	char	   *orig_num;
	double		val;
	char	   *endptr;

	/*
	 * endptr points to the first character _after_ the sequence we recognized
	 * as a valid floating point number. orig_num points to the original input
	 * string.
	 */
	orig_num = num;

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtod() on different platforms.
	 */
	if (*num == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"real", orig_num)));

	errno = 0;
	val = strtod(num, &endptr);

	/* did we not see anything that looks like a double? */
	if (endptr == num || errno != 0)
	{
		int			save_errno = errno;

		/*
		 * C99 requires that strtod() accept NaN, [+-]Infinity, and [+-]Inf,
		 * but not all platforms support all of these (and some accept them
		 * but set ERANGE anyway...)  Therefore, we check for these inputs
		 * ourselves if strtod() fails.
		 *
		 * Note: C99 also requires hexadecimal input as well as some extended
		 * forms of NaN, but we consider these forms unportable and don't try
		 * to support them.  You can use 'em if your strtod() takes 'em.
		 */
		if (pg_strncasecmp(num, "NaN", 3) == 0)
		{
			val = get_float4_nan();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "Infinity", 8) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 8;
		}
		else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
		{
			val = -get_float4_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "inf", 3) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "+inf", 4) == 0)
		{
			val = get_float4_infinity();
			endptr = num + 4;
		}
		else if (pg_strncasecmp(num, "-inf", 4) == 0)
		{
			val = -get_float4_infinity();
			endptr = num + 4;
		}
		else if (save_errno == ERANGE)
		{
			/*
			 * Some platforms return ERANGE for denormalized numbers (those
			 * that are not zero, but are too close to zero to have full
			 * precision).  We'd prefer not to throw error for that, so try to
			 * detect whether it's a "real" out-of-range condition by checking
			 * to see if the result is zero or huge.
			 */
			if (val == 0.0 || val >= HUGE_VAL || val <= -HUGE_VAL)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("\"%s\" is out of range for type real",
								orig_num)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							"real", orig_num)));
	}
#ifdef HAVE_BUGGY_SOLARIS_STRTOD
	else
	{
		/*
		 * Many versions of Solaris have a bug wherein strtod sets endptr to
		 * point one byte beyond the end of the string when given "inf" or
		 * "infinity".
		 */
		if (endptr != num && endptr[-1] == '\0')
			endptr--;
	}
#endif							/* HAVE_BUGGY_SOLARIS_STRTOD */

	/* skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* if there is any junk left at the end of the string, bail out */
	if (*endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"real", orig_num)));

	/*
	 * if we get here, we have a legal double, still need to check to see if
	 * it's a legal float4
	 */
	CHECKFLOATVAL((float4) val, isinf(val), val == 0);

	PG_RETURN_FLOAT4((float4) val);
}

/*
 *		float4out		- converts a float4 number to a string
 *						  using a standard output format
 */
Datum
float4out(PG_FUNCTION_ARGS)
{
	float4		num = PG_GETARG_FLOAT4(0);
	char	   *ascii = (char *) palloc(MAXFLOATWIDTH + 1);


	if (extra_float_digits > 0)
	{
		float_to_shortest_decimal_buf(num, ascii);
		PG_RETURN_CSTRING(ascii);
	}

	if (isnan(num))
		PG_RETURN_CSTRING(strcpy(ascii, "NaN"));

	switch (is_infinite(num))
	{
		case 1:
			strcpy(ascii, "Infinity");
			break;
		case -1:
			strcpy(ascii, "-Infinity");
			break;
		default:
			{
				int			ndig;
				int extra_float_digits_old = extra_float_digits;

				/*
				 * For internal data transmission and deparse,  output
			     * float with high precision
			     */
				if (portable_output)
					extra_float_digits = MAX_EXTRA_FLOAT_DIGITS;

				ndig = FLT_DIG + extra_float_digits;
				if (ndig < 1)
					ndig = 1;

				snprintf(ascii, MAXFLOATWIDTH + 1, "%.*g", ndig, num);

				if (extra_float_digits_old != extra_float_digits)
					extra_float_digits = extra_float_digits_old;
			}
	}

	PG_RETURN_CSTRING(ascii);
}

/*
 *		float4recv			- converts external binary format to float4
 */
Datum
float4recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_FLOAT4(pq_getmsgfloat4(buf));
}

/*
 *		float4send			- converts float4 to binary format
 */
Datum
float4send(PG_FUNCTION_ARGS)
{
	float4		num = PG_GETARG_FLOAT4(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendfloat4(&buf, num);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 *		float8in		- converts "num" to float8
 */
Datum
float8in(PG_FUNCTION_ARGS)
{
	char	   *num = PG_GETARG_CSTRING(0);

	PG_RETURN_FLOAT8(float8in_internal(num, NULL, "double precision", num));
}

/* Convenience macro: set *have_error flag (if provided) or throw error */
#define RETURN_ERROR(throw_error) \
do { \
	if (have_error) { \
		*have_error = true; \
		return 0.0; \
	} else { \
		throw_error; \
	} \
} while (0)

/*
 * float8in_internal_opt_error - guts of float8in()
 *
 * This is exposed for use by functions that want a reasonably
 * platform-independent way of inputting doubles.  The behavior is
 * essentially like strtod + ereport on error, but note the following
 * differences:
 * 1. Both leading and trailing whitespace are skipped.
 * 2. If endptr_p is NULL, we throw error if there's trailing junk.
 * Otherwise, it's up to the caller to complain about trailing junk.
 * 3. In event of a syntax error, the report mentions the given type_name
 * and prints orig_string as the input; this is meant to support use of
 * this function with types such as "box" and "point", where what we are
 * parsing here is just a substring of orig_string.
 *
 * "num" could validly be declared "const char *", but that results in an
 * unreasonable amount of extra casting both here and in callers, so we don't.
 *
 * When "*have_error" flag is provided, it's set instead of throwing an
 * error.  This is helpful when caller need to handle errors by itself.
 */
double
float8in_internal_opt_error(char *num, char **endptr_p,
							const char *type_name, const char *orig_string,
							bool *have_error)
{
	double		val;
	char	   *endptr;

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtod() on different platforms.
	 */
	if (*num == '\0')
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							  errmsg("invalid input syntax for type %s: \"%s\"",
									 type_name, orig_string))));

	errno = 0;
	val = strtod(num, &endptr);

	/* did we not see anything that looks like a double? */
	if (endptr == num || errno != 0)
	{
		int			save_errno = errno;

		/*
		 * C99 requires that strtod() accept NaN, [+-]Infinity, and [+-]Inf,
		 * but not all platforms support all of these (and some accept them
		 * but set ERANGE anyway...)  Therefore, we check for these inputs
		 * ourselves if strtod() fails.
		 *
		 * Note: C99 also requires hexadecimal input as well as some extended
		 * forms of NaN, but we consider these forms unportable and don't try
		 * to support them.  You can use 'em if your strtod() takes 'em.
		 */
		if (pg_strncasecmp(num, "NaN", 3) == 0)
		{
			val = get_float8_nan();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "Infinity", 8) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 8;
		}
		else if (pg_strncasecmp(num, "+Infinity", 9) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "-Infinity", 9) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 9;
		}
		else if (pg_strncasecmp(num, "inf", 3) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 3;
		}
		else if (pg_strncasecmp(num, "+inf", 4) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 4;
		}
		else if (pg_strncasecmp(num, "-inf", 4) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 4;
		}
		else if (save_errno == ERANGE)
		{
			/*
			 * Some platforms return ERANGE for denormalized numbers (those
			 * that are not zero, but are too close to zero to have full
			 * precision).  We'd prefer not to throw error for that, so try to
			 * detect whether it's a "real" out-of-range condition by checking
			 * to see if the result is zero or huge.
			 *
			 * On error, we intentionally complain about double precision not
			 * the given type name, and we print only the part of the string
			 * that is the current number.
			 */
			if (val == 0.0 || val >= HUGE_VAL || val <= -HUGE_VAL)
			{
				char	   *errnumber = pstrdup(num);

				errnumber[endptr - num] = '\0';
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
									  errmsg("\"%s\" is out of range for "
											 "type double precision",
											 errnumber))));
			}
		}
		else
			RETURN_ERROR(ereport(ERROR,
								 (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								  errmsg("invalid input syntax for type "
										 "%s: \"%s\"",
										 type_name, orig_string))));
	}
#ifdef HAVE_BUGGY_SOLARIS_STRTOD
	else
	{
		/*
		 * Many versions of Solaris have a bug wherein strtod sets endptr to
		 * point one byte beyond the end of the string when given "inf" or
		 * "infinity".
		 */
		if (endptr != num && endptr[-1] == '\0')
			endptr--;
	}
#endif							/* HAVE_BUGGY_SOLARIS_STRTOD */

	/* skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = endptr;
	else if (*endptr != '\0')
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
							  errmsg("invalid input syntax for type "
									 "%s: \"%s\"",
									 type_name, orig_string))));

	return val;
}

/*
 * Interface to float8in_internal_opt_error() without "have_error" argument.
 */
double
float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string)
{
	return float8in_internal_opt_error(num, endptr_p, type_name,
									   orig_string, NULL);
}


/*
 *		float8out		- converts float8 number to a string
 *						  using a standard output format
 */
Datum
float8out(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);

	PG_RETURN_CSTRING(float8out_internal(num));
}

/*
 * float8out_internal - guts of float8out()
 *
 * This is exposed for use by functions that want a reasonably
 * platform-independent way of outputting doubles.
 * The result is always palloc'd.
 */
char *
float8out_internal(double num)
{
	char	   *ascii = (char *) palloc(MAXDOUBLEWIDTH + 1);

	if (extra_float_digits > 0)
	{
		double_to_shortest_decimal_buf(num, ascii);
		return ascii;
	}

	if (isnan(num))
		return strcpy(ascii, "NaN");

	switch (is_infinite(num))
	{
		case 1:
			strcpy(ascii, "Infinity");
			break;
		case -1:
			strcpy(ascii, "-Infinity");
			break;
		default:
			{
				int			ndig;
				int extra_float_digits_old = extra_float_digits;

				/*
				 * For internal data transmission and deparse,  output
			     * float with high precision
			     */
				if (portable_output)
					extra_float_digits = MAX_EXTRA_FLOAT_DIGITS;

				ndig = DBL_DIG + extra_float_digits;
				if (ndig < 1)
					ndig = 1;

				snprintf(ascii, MAXDOUBLEWIDTH + 1, "%.*g", ndig, num);

				if (extra_float_digits_old != extra_float_digits)
					extra_float_digits = extra_float_digits_old;
			}
	}

	return ascii;
}

/*
 *		float8recv			- converts external binary format to float8
 */
Datum
float8recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_FLOAT8(pq_getmsgfloat8(buf));
}

/*
 *		float8send			- converts float8 to binary format
 */
Datum
float8send(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendfloat8(&buf, num);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/* ========== PUBLIC ROUTINES ========== */


/*
 *		======================
 *		FLOAT4 BASE OPERATIONS
 *		======================
 */

/*
 *		float4abs		- returns |arg1| (absolute value)
 */
Datum
float4abs(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);

	PG_RETURN_FLOAT4((float4) fabs(arg1));
}

/*
 *		float4um		- returns -arg1 (unary minus)
 */
Datum
float4um(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		result;

	result = -arg1;
	PG_RETURN_FLOAT4(result);
}

Datum
float4up(PG_FUNCTION_ARGS)
{
	float4		arg = PG_GETARG_FLOAT4(0);

	PG_RETURN_FLOAT4(arg);
}

Datum
float4larger(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	if (float4_cmp_internal(arg1, arg2) > 0)
		result = arg1;
	else
		result = arg2;
	PG_RETURN_FLOAT4(result);
}

Datum
float4smaller(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	if (float4_cmp_internal(arg1, arg2) < 0)
		result = arg1;
	else
		result = arg2;
	PG_RETURN_FLOAT4(result);
}

/*
 *		======================
 *		FLOAT8 BASE OPERATIONS
 *		======================
 */

/*
 *		float8abs		- returns |arg1| (absolute value)
 */
Datum
float8abs(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(fabs(arg1));
}


/*
 *		float8um		- returns -arg1 (unary minus)
 */
Datum
float8um(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	result = -arg1;
	PG_RETURN_FLOAT8(result);
}

Datum
float8up(PG_FUNCTION_ARGS)
{
	float8		arg = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(arg);
}

Datum
float8larger(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	if (float8_cmp_internal(arg1, arg2) > 0)
		result = arg1;
	else
		result = arg2;
	PG_RETURN_FLOAT8(result);
}

Datum
float8smaller(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	if (float8_cmp_internal(arg1, arg2) < 0)
		result = arg1;
	else
		result = arg2;
	PG_RETURN_FLOAT8(result);
}


/*
 *		====================
 *		ARITHMETIC OPERATORS
 *		====================
 */

/*
 *		float4pl		- returns arg1 + arg2
 *		float4mi		- returns arg1 - arg2
 *		float4mul		- returns arg1 * arg2
 *		float4div		- returns arg1 / arg2
 */
Datum
float4pl(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	result = arg1 + arg2;

	/*
	 * There isn't any way to check for underflow of addition/subtraction
	 * because numbers near the underflow value have already been rounded to
	 * the point where we can't detect that the two values were originally
	 * different, e.g. on x86, '1e-45'::float4 == '2e-45'::float4 ==
	 * 1.4013e-45.
	 */
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT4(result);
}

Datum
float4mi(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	result = arg1 - arg2;
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT4(result);
}

Datum
float4mul(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	result = arg1 * arg2;
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2),
				  arg1 == 0 || arg2 == 0);
	PG_RETURN_FLOAT4(result);
}

Datum
float4div(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	if (arg2 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result = arg1 / arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);
	PG_RETURN_FLOAT4(result);
}

Datum
float4remainder(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float4		result;

	if (arg2 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result = arg1 / arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);

	result = arg1 - pg_banker_round(result) * arg2;

	PG_RETURN_FLOAT4(result);
}

/*
 *		float8pl		- returns arg1 + arg2
 *		float8mi		- returns arg1 - arg2
 *		float8mul		- returns arg1 * arg2
 *		float8div		- returns arg1 / arg2
 */
Datum
float8pl(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	result = arg1 + arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT8(result);
}

Datum
float8mi(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	result = arg1 - arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT8(result);
}

Datum
float8mul(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	result = arg1 * arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2),
				  arg1 == 0 || arg2 == 0);
	PG_RETURN_FLOAT8(result);
}

Datum
float8div(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	if (arg2 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result = arg1 / arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}

Datum
float8remainder(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	if (arg2 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result = arg1 / arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);

	result = arg1 - pg_banker_round(result) * arg2;

	PG_RETURN_FLOAT8(result);
}

/*
 *		====================
 *		COMPARISON OPERATORS
 *		====================
 */

/*
 *		float4{eq,ne,lt,le,gt,ge}		- float4/float4 comparison operations
 */
int
float4_cmp_internal(float4 a, float4 b)
{
	/*
	 * We consider all NANs to be equal and larger than any non-NAN. This is
	 * somewhat arbitrary; the important thing is to have a consistent sort
	 * order.
	 */
	if (isnan(a))
	{
		if (isnan(b))
			return 0;			/* NAN = NAN */
		else
			return 1;			/* NAN > non-NAN */
	}
	else if (isnan(b))
	{
		return -1;				/* non-NAN < NAN */
	}
	else
	{
		if (a > b)
			return 1;
		else if (a < b)
			return -1;
		else
			return 0;
	}
}

Datum
float4eq(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float4_cmp_internal(arg1, arg2) == 0);
}

Datum
float4ne(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float4_cmp_internal(arg1, arg2) != 0);
}

Datum
float4lt(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float4_cmp_internal(arg1, arg2) < 0);
}

Datum
float4le(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float4_cmp_internal(arg1, arg2) <= 0);
}

Datum
float4gt(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float4_cmp_internal(arg1, arg2) > 0);
}

Datum
float4ge(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float4_cmp_internal(arg1, arg2) >= 0);
}

Datum
btfloat4cmp(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_INT32(float4_cmp_internal(arg1, arg2));
}

static int
btfloat4fastcmp(Datum x, Datum y, SortSupport ssup)
{
	float4		arg1 = DatumGetFloat4(x);
	float4		arg2 = DatumGetFloat4(y);

	return float4_cmp_internal(arg1, arg2);
}

Datum
btfloat4sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = btfloat4fastcmp;
	PG_RETURN_VOID();
}

/*
 *		float8{eq,ne,lt,le,gt,ge}		- float8/float8 comparison operations
 */
int
float8_cmp_internal(float8 a, float8 b)
{
	/*
	 * We consider all NANs to be equal and larger than any non-NAN. This is
	 * somewhat arbitrary; the important thing is to have a consistent sort
	 * order.
	 */
	if (isnan(a))
	{
		if (isnan(b))
			return 0;			/* NAN = NAN */
		else
			return 1;			/* NAN > non-NAN */
	}
	else if (isnan(b))
	{
		return -1;				/* non-NAN < NAN */
	}
	else
	{
		if (a > b)
			return 1;
		else if (a < b)
			return -1;
		else
			return 0;
	}
}

Datum
float8eq(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) == 0);
}

Datum
float8ne(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) != 0);
}

Datum
float8lt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) < 0);
}

Datum
float8le(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) <= 0);
}

Datum
float8gt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) > 0);
}

Datum
float8ge(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) >= 0);
}

Datum
btfloat8cmp(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_INT32(float8_cmp_internal(arg1, arg2));
}

static int
btfloat8fastcmp(Datum x, Datum y, SortSupport ssup)
{
	float8		arg1 = DatumGetFloat8(x);
	float8		arg2 = DatumGetFloat8(y);

	return float8_cmp_internal(arg1, arg2);
}

Datum
btfloat8sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = btfloat8fastcmp;
	PG_RETURN_VOID();
}

Datum
btfloat48cmp(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	/* widen float4 to float8 and then compare */
	PG_RETURN_INT32(float8_cmp_internal(arg1, arg2));
}

Datum
btfloat84cmp(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	/* widen float4 to float8 and then compare */
	PG_RETURN_INT32(float8_cmp_internal(arg1, arg2));
}

/*
 * in_range support function for float8.
 *
 * Note: we needn't supply a float8_float4 variant, as implicit coercion
 * of the offset value takes care of that scenario just as well.
 */
Datum
in_range_float8_float8(PG_FUNCTION_ARGS)
{
	float8		val = PG_GETARG_FLOAT8(0);
	float8		base = PG_GETARG_FLOAT8(1);
	float8		offset = PG_GETARG_FLOAT8(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	float8		sum;

	/*
	 * Reject negative or NaN offset.  Negative is per spec, and NaN is
	 * because appropriate semantics for that seem non-obvious.
	 */
	if (isnan(offset) || offset < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	/*
	 * Deal with cases where val and/or base is NaN, following the rule that
	 * NaN sorts after non-NaN (cf float8_cmp_internal).  The offset cannot
	 * affect the conclusion.
	 */
	if (isnan(val))
	{
		if (isnan(base))
			PG_RETURN_BOOL(true);	/* NAN = NAN */
		else
			PG_RETURN_BOOL(!less);	/* NAN > non-NAN */
	}
	else if (isnan(base))
	{
		PG_RETURN_BOOL(less);	/* non-NAN < NAN */
	}

	/*
	 * Deal with infinite offset (necessarily +inf, at this point).  We must
	 * special-case this because if base happens to be -inf, their sum would
	 * be NaN, which is an overflow-ish condition we should avoid.
	 */
	if (isinf(offset))
	{
		PG_RETURN_BOOL(sub ? !less : less);
	}

	/*
	 * Otherwise it should be safe to compute base +/- offset.  We trust the
	 * FPU to cope if base is +/-inf or the true sum would overflow, and
	 * produce a suitably signed infinity, which will compare properly against
	 * val whether or not that's infinity.
	 */
	if (sub)
		sum = base - offset;
	else
		sum = base + offset;

	if (less)
		PG_RETURN_BOOL(val <= sum);
	else
		PG_RETURN_BOOL(val >= sum);
}

/*
 * in_range support function for float4.
 *
 * We would need a float4_float8 variant in any case, so we supply that and
 * let implicit coercion take care of the float4_float4 case.
 */
Datum
in_range_float4_float8(PG_FUNCTION_ARGS)
{
	/* Doesn't seem worth duplicating code for, so just invoke float8_float8 */
	return DirectFunctionCall5(in_range_float8_float8,
							   Float8GetDatumFast((float8) PG_GETARG_FLOAT4(0)),
							   Float8GetDatumFast((float8) PG_GETARG_FLOAT4(1)),
							   PG_GETARG_DATUM(2),
							   PG_GETARG_DATUM(3),
							   PG_GETARG_DATUM(4));
}

/*
 *		===================
 *		CONVERSION ROUTINES
 *		===================
 */

/*
 *		ftod			- converts a float4 number to a float8 number
 */
Datum
ftod(PG_FUNCTION_ARGS)
{
	float4		num = PG_GETARG_FLOAT4(0);

	PG_RETURN_FLOAT8((float8) num);
}


/*
 *		dtof			- converts a float8 number to a float4 number
 */
Datum
dtof(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);

	CHECKFLOATVAL((float4) num, isinf(num), num == 0);

	PG_RETURN_FLOAT4((float4) num);
}


/*
 *		dtoi4			- converts a float8 number to an int4 number
 */
Datum
dtoi4(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);
	int32		result;

	/* 'Inf' is handled by INT_MAX */
	if (num < INT_MIN || num > INT_MAX || isnan(num))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	result = (int32) rint(num);
	PG_RETURN_INT32(result);
}


/*
 *		dtoi2			- converts a float8 number to an int2 number
 */
Datum
dtoi2(PG_FUNCTION_ARGS)
{
	float8		num = PG_GETARG_FLOAT8(0);

	if (num < SHRT_MIN || num > SHRT_MAX || isnan(num))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16((int16) rint(num));
}


/*
 *		i4tod			- converts an int4 number to a float8 number
 */
Datum
i4tod(PG_FUNCTION_ARGS)
{
	int32		num = PG_GETARG_INT32(0);

	PG_RETURN_FLOAT8((float8) num);
}


/*
 *		i2tod			- converts an int2 number to a float8 number
 */
Datum
i2tod(PG_FUNCTION_ARGS)
{
	int16		num = PG_GETARG_INT16(0);

	PG_RETURN_FLOAT8((float8) num);
}


/*
 *		ftoi4			- converts a float4 number to an int4 number
 */
Datum
ftoi4(PG_FUNCTION_ARGS)
{
	float4		num = PG_GETARG_FLOAT4(0);

	if (num < INT_MIN || num > INT_MAX || isnan(num))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	PG_RETURN_INT32((int32) rint(num));
}


/*
 *		ftoi2			- converts a float4 number to an int2 number
 */
Datum
ftoi2(PG_FUNCTION_ARGS)
{
	float4		num = PG_GETARG_FLOAT4(0);

	if (num < SHRT_MIN || num > SHRT_MAX || isnan(num))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16((int16) rint(num));
}


/*
 *		i4tof			- converts an int4 number to a float4 number
 */
Datum
i4tof(PG_FUNCTION_ARGS)
{
	int32		num = PG_GETARG_INT32(0);

	PG_RETURN_FLOAT4((float4) num);
}


/*
 *		i2tof			- converts an int2 number to a float4 number
 */
Datum
i2tof(PG_FUNCTION_ARGS)
{
	int16		num = PG_GETARG_INT16(0);

	PG_RETURN_FLOAT4((float4) num);
}


/*
 *		=======================
 *		RANDOM FLOAT8 OPERATORS
 *		=======================
 */

/*
 *		dround			- returns	ROUND(arg1)
 */
Datum
dround(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(rint(arg1));
}

/*
 *		dceil			- returns the smallest integer greater than or
 *						  equal to the specified float
 */
Datum
dceil(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(ceil(arg1));
}

/*
 *		dfloor			- returns the largest integer lesser than or
 *						  equal to the specified float
 */
Datum
dfloor(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);

	PG_RETURN_FLOAT8(floor(arg1));
}

/*
 *		dsign			- returns -1 if the argument is less than 0, 0
 *						  if the argument is equal to 0, and 1 if the
 *						  argument is greater than zero.
 */
Datum
dsign(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* opentenbase_ora function bug: return 1 when the value is nan  */
	if (ORA_MODE && isnan(arg1))
	{
		result = 1.0;
		PG_RETURN_FLOAT8(result);
	}

	if (arg1 > 0)
		result = 1.0;
	else if (arg1 < 0)
		result = -1.0;
	else
		result = 0.0;

	PG_RETURN_FLOAT8(result);
}

/*
 *		dtrunc			- returns truncation-towards-zero of arg1,
 *						  arg1 >= 0 ... the greatest integer less
 *										than or equal to arg1
 *						  arg1 < 0	... the least integer greater
 *										than or equal to arg1
 */
Datum
dtrunc(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	if (arg1 >= 0)
		result = floor(arg1);
	else
		result = -floor(-arg1);

	PG_RETURN_FLOAT8(result);
}


/*
 *		dsqrt			- returns square root of arg1
 */
Datum
dsqrt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	if (arg1 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("cannot take square root of a negative number")));

	result = sqrt(arg1);

	CHECKFLOATVAL(result, isinf(arg1), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dcbrt			- returns cube root of arg1
 */
Datum
dcbrt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	result = cbrt(arg1);
	CHECKFLOATVAL(result, isinf(arg1), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dpow			- returns pow(arg1,arg2)
 */
Datum
dpow(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	/*
	 * The SQL spec requires that we emit a particular SQLSTATE error code for
	 * certain error conditions.  Specifically, we don't return a
	 * divide-by-zero error code for 0 ^ -1.
	 */
	if (arg1 == 0 && arg2 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("zero raised to a negative power is undefined")));
	if (arg1 < 0 && floor(arg2) != arg2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("a negative number raised to a non-integer power yields a complex result")));

	/*
	 * pow() sets errno only on some platforms, depending on whether it
	 * follows _IEEE_, _POSIX_, _XOPEN_, or _SVID_, so we try to avoid using
	 * errno.  However, some platform/CPU combinations return errno == EDOM
	 * and result == Nan for negative arg1 and very large arg2 (they must be
	 * using something different from our floor() test to decide it's
	 * invalid).  Other platforms (HPPA) return errno == ERANGE and a large
	 * (HUGE_VAL) but finite result to signal overflow.
	 */
	errno = 0;
	result = pow(arg1, arg2);
	if (errno == EDOM && isnan(result))
	{
		if ((fabs(arg1) > 1 && arg2 >= 0) || (fabs(arg1) < 1 && arg2 < 0))
			/* The sign of Inf is not significant in this case. */
			result = get_float8_infinity();
		else if (fabs(arg1) != 1)
			result = 0;
		else
			result = 1;
	}
	else if (errno == ERANGE && result != 0 && !isinf(result))
		result = get_float8_infinity();

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dexp			- returns the exponential function of arg1
 */
Datum
dexp(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	errno = 0;
	result = exp(arg1);
	if (errno == ERANGE && result != 0 && !isinf(result))
		result = get_float8_infinity();

	CHECKFLOATVAL(result, isinf(arg1), false);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dlog1			- returns the natural logarithm of arg1
 */
Datum
dlog1(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Emit particular SQLSTATE error codes for ln(). This is required by the
	 * SQL standard.
	 */
	if (arg1 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	if (arg1 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	result = log(arg1);

	CHECKFLOATVAL(result, isinf(arg1), arg1 == 1);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dlog10			- returns the base 10 logarithm of arg1
 */
Datum
dlog10(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/*
	 * Emit particular SQLSTATE error codes for log(). The SQL spec doesn't
	 * define log(), but it does define ln(), so it makes sense to emit the
	 * same error code for an analogous error condition.
	 */
	if (arg1 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	if (arg1 < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	result = log10(arg1);

	CHECKFLOATVAL(result, isinf(arg1), arg1 == 1);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dacos			- returns the arccos of arg1 (radians)
 */
Datum
dacos(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * The principal branch of the inverse cosine function maps values in the
	 * range [-1, 1] to values in the range [0, Pi], so we should reject any
	 * inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = acos(arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dasin			- returns the arcsin of arg1 (radians)
 */
Datum
dasin(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * The principal branch of the inverse sine function maps values in the
	 * range [-1, 1] to values in the range [-Pi/2, Pi/2], so we should reject
	 * any inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = asin(arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		datan			- returns the arctan of arg1 (radians)
 */
Datum
datan(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * The principal branch of the inverse tangent function maps all inputs to
	 * values in the range [-Pi/2, Pi/2], so the result should always be
	 * finite, even if the input is infinite.
	 */
	result = atan(arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		atan2			- returns the arctan of arg1/arg2 (radians)
 */
Datum
datan2(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	/* Per the POSIX spec, return NaN if either input is NaN */
	if (isnan(arg1) || isnan(arg2))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* opentenbase_ora function bug: value '0' need throw */
	if (ORA_MODE && fabs(arg1 - 0.0) < 1e-9 && fabs(arg2 - 0.0) < 1e-9)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("numeric overflow")));

	/*
	 * atan2 maps all inputs to values in the range [-Pi, Pi], so the result
	 * should always be finite, even if the inputs are infinite.
	 */
	result = atan2(arg1, arg2);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dcos			- returns the cosine of arg1 (radians)
 */
Datum
dcos(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * cos() is periodic and so theoretically can work for all finite inputs,
	 * but some implementations may choose to throw error if the input is so
	 * large that there are no significant digits in the result.  So we should
	 * check for errors.  POSIX allows an error to be reported either via
	 * errno or via fetestexcept(), but currently we only support checking
	 * errno.  (fetestexcept() is rumored to report underflow unreasonably
	 * early on some platforms, so it's not clear that believing it would be a
	 * net improvement anyway.)
	 *
	 * For infinite inputs, POSIX specifies that the trigonometric functions
	 * should return a domain error; but we won't notice that unless the
	 * platform reports via errno, so also explicitly test for infinite
	 * inputs.
	 */
	errno = 0;
	result = cos(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dcot			- returns the cotangent of arg1 (radians)
 */
Datum
dcot(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* Be sure to throw an error if the input is infinite --- see dcos() */
	errno = 0;
	result = tan(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	result = 1.0 / result;
	CHECKFLOATVAL(result, true /* cot(0) == Inf */ , true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dsin			- returns the sine of arg1 (radians)
 */
Datum
dsin(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* Be sure to throw an error if the input is infinite --- see dcos() */
	errno = 0;
	result = sin(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dtan			- returns the tangent of arg1 (radians)
 */
Datum
dtan(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	/* Be sure to throw an error if the input is infinite --- see dcos() */
	errno = 0;
	result = tan(arg1);
	if (errno != 0 || isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	CHECKFLOATVAL(result, true /* tan(pi/2) == Inf */ , true);
	PG_RETURN_FLOAT8(result);
}


/* ========== DEGREE-BASED TRIGONOMETRIC FUNCTIONS ========== */


/*
 * Initialize the cached constants declared at the head of this file
 * (sin_30 etc).  The fact that we need those at all, let alone need this
 * Rube-Goldberg-worthy method of initializing them, is because there are
 * compilers out there that will precompute expressions such as sin(constant)
 * using a sin() function different from what will be used at runtime.  If we
 * want exact results, we must ensure that none of the scaling constants used
 * in the degree-based trig functions are computed that way.  To do so, we
 * compute them from the variables degree_c_thirty etc, which are also really
 * constants, but the compiler cannot assume that.
 *
 * Other hazards we are trying to forestall with this kluge include the
 * possibility that compilers will rearrange the expressions, or compute
 * some intermediate results in registers wider than a standard double.
 *
 * In the places where we use these constants, the typical pattern is like
 *		volatile float8 sin_x = sin(x * RADIANS_PER_DEGREE);
 *		return (sin_x / sin_30);
 * where we hope to get a value of exactly 1.0 from the division when x = 30.
 * The volatile temporary variable is needed on machines with wide float
 * registers, to ensure that the result of sin(x) is rounded to double width
 * the same as the value of sin_30 has been.  Experimentation with gcc shows
 * that marking the temp variable volatile is necessary to make the store and
 * reload actually happen; hopefully the same trick works for other compilers.
 * (gcc's documentation suggests using the -ffloat-store compiler switch to
 * ensure this, but that is compiler-specific and it also pessimizes code in
 * many places where we don't care about this.)
 */
static void
init_degree_constants(void)
{
	sin_30 = sin(degree_c_thirty * RADIANS_PER_DEGREE);
	one_minus_cos_60 = 1.0 - cos(degree_c_sixty * RADIANS_PER_DEGREE);
	asin_0_5 = asin(degree_c_one_half);
	acos_0_5 = acos(degree_c_one_half);
	atan_1_0 = atan(degree_c_one);
	tan_45 = sind_q1(degree_c_forty_five) / cosd_q1(degree_c_forty_five);
	cot_45 = cosd_q1(degree_c_forty_five) / sind_q1(degree_c_forty_five);
	degree_consts_set = true;
}

#define INIT_DEGREE_CONSTANTS() \
do { \
	if (!degree_consts_set) \
		init_degree_constants(); \
} while(0)


/*
 *		asind_q1		- returns the inverse sine of x in degrees, for x in
 *						  the range [0, 1].  The result is an angle in the
 *						  first quadrant --- [0, 90] degrees.
 *
 *						  For the 3 special case inputs (0, 0.5 and 1), this
 *						  function will return exact values (0, 30 and 90
 *						  degrees respectively).
 */
static double
asind_q1(double x)
{
	/*
	 * Stitch together inverse sine and cosine functions for the ranges [0,
	 * 0.5] and (0.5, 1].  Each expression below is guaranteed to return
	 * exactly 30 for x=0.5, so the result is a continuous monotonic function
	 * over the full range.
	 */
	if (x <= 0.5)
	{
		volatile float8 asin_x = asin(x);

		return (asin_x / asin_0_5) * 30.0;
	}
	else
	{
		volatile float8 acos_x = acos(x);

		return 90.0 - (acos_x / acos_0_5) * 60.0;
	}
}


/*
 *		acosd_q1		- returns the inverse cosine of x in degrees, for x in
 *						  the range [0, 1].  The result is an angle in the
 *						  first quadrant --- [0, 90] degrees.
 *
 *						  For the 3 special case inputs (0, 0.5 and 1), this
 *						  function will return exact values (0, 60 and 90
 *						  degrees respectively).
 */
static double
acosd_q1(double x)
{
	/*
	 * Stitch together inverse sine and cosine functions for the ranges [0,
	 * 0.5] and (0.5, 1].  Each expression below is guaranteed to return
	 * exactly 60 for x=0.5, so the result is a continuous monotonic function
	 * over the full range.
	 */
	if (x <= 0.5)
	{
		volatile float8 asin_x = asin(x);

		return 90.0 - (asin_x / asin_0_5) * 30.0;
	}
	else
	{
		volatile float8 acos_x = acos(x);

		return (acos_x / acos_0_5) * 60.0;
	}
}


/*
 *		dacosd			- returns the arccos of arg1 (degrees)
 */
Datum
dacosd(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * The principal branch of the inverse cosine function maps values in the
	 * range [-1, 1] to values in the range [0, 180], so we should reject any
	 * inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	if (arg1 >= 0.0)
		result = acosd_q1(arg1);
	else
		result = 90.0 + asind_q1(-arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dasind			- returns the arcsin of arg1 (degrees)
 */
Datum
dasind(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * The principal branch of the inverse sine function maps values in the
	 * range [-1, 1] to values in the range [-90, 90], so we should reject any
	 * inputs outside that range and the result will always be finite.
	 */
	if (arg1 < -1.0 || arg1 > 1.0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	if (arg1 >= 0.0)
		result = asind_q1(arg1);
	else
		result = -asind_q1(-arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		datand			- returns the arctan of arg1 (degrees)
 */
Datum
datand(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	volatile float8 atan_arg1;

	/* Per the POSIX spec, return NaN if the input is NaN */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * The principal branch of the inverse tangent function maps all inputs to
	 * values in the range [-90, 90], so the result should always be finite,
	 * even if the input is infinite.  Additionally, we take care to ensure
	 * than when arg1 is 1, the result is exactly 45.
	 */
	atan_arg1 = atan(arg1);
	result = (atan_arg1 / atan_1_0) * 45.0;

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		atan2d			- returns the arctan of arg1/arg2 (degrees)
 */
Datum
datan2d(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;
	volatile float8 atan2_arg1_arg2;

	/* Per the POSIX spec, return NaN if either input is NaN */
	if (isnan(arg1) || isnan(arg2))
		PG_RETURN_FLOAT8(get_float8_nan());

	INIT_DEGREE_CONSTANTS();

	/*
	 * atan2d maps all inputs to values in the range [-180, 180], so the
	 * result should always be finite, even if the inputs are infinite.
	 *
	 * Note: this coding assumes that atan(1.0) is a suitable scaling constant
	 * to get an exact result from atan2().  This might well fail on us at
	 * some point, requiring us to decide exactly what inputs we think we're
	 * going to guarantee an exact result for.
	 */
	atan2_arg1_arg2 = atan2(arg1, arg2);
	result = (atan2_arg1_arg2 / atan_1_0) * 45.0;

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		sind_0_to_30	- returns the sine of an angle that lies between 0 and
 *						  30 degrees.  This will return exactly 0 when x is 0,
 *						  and exactly 0.5 when x is 30 degrees.
 */
static double
sind_0_to_30(double x)
{
	volatile float8 sin_x = sin(x * RADIANS_PER_DEGREE);

	return (sin_x / sin_30) / 2.0;
}


/*
 *		cosd_0_to_60	- returns the cosine of an angle that lies between 0
 *						  and 60 degrees.  This will return exactly 1 when x
 *						  is 0, and exactly 0.5 when x is 60 degrees.
 */
static double
cosd_0_to_60(double x)
{
	volatile float8 one_minus_cos_x = 1.0 - cos(x * RADIANS_PER_DEGREE);

	return 1.0 - (one_minus_cos_x / one_minus_cos_60) / 2.0;
}


/*
 *		sind_q1			- returns the sine of an angle in the first quadrant
 *						  (0 to 90 degrees).
 */
static double
sind_q1(double x)
{
	/*
	 * Stitch together the sine and cosine functions for the ranges [0, 30]
	 * and (30, 90].  These guarantee to return exact answers at their
	 * endpoints, so the overall result is a continuous monotonic function
	 * that gives exact results when x = 0, 30 and 90 degrees.
	 */
	if (x <= 30.0)
		return sind_0_to_30(x);
	else
		return cosd_0_to_60(90.0 - x);
}


/*
 *		cosd_q1			- returns the cosine of an angle in the first quadrant
 *						  (0 to 90 degrees).
 */
static double
cosd_q1(double x)
{
	/*
	 * Stitch together the sine and cosine functions for the ranges [0, 60]
	 * and (60, 90].  These guarantee to return exact answers at their
	 * endpoints, so the overall result is a continuous monotonic function
	 * that gives exact results when x = 0, 60 and 90 degrees.
	 */
	if (x <= 60.0)
		return cosd_0_to_60(x);
	else
		return sind_0_to_30(90.0 - x);
}


/*
 *		dcosd			- returns the cosine of arg1 (degrees)
 */
Datum
dcosd(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* cosd(-x) = cosd(x) */
		arg1 = -arg1;
	}

	if (arg1 > 180.0)
	{
		/* cosd(360-x) = cosd(x) */
		arg1 = 360.0 - arg1;
	}

	if (arg1 > 90.0)
	{
		/* cosd(180-x) = -cosd(x) */
		arg1 = 180.0 - arg1;
		sign = -sign;
	}

	result = sign * cosd_q1(arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dcotd			- returns the cotangent of arg1 (degrees)
 */
Datum
dcotd(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	volatile float8 cot_arg1;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* cotd(-x) = -cotd(x) */
		arg1 = -arg1;
		sign = -sign;
	}

	if (arg1 > 180.0)
	{
		/* cotd(360-x) = -cotd(x) */
		arg1 = 360.0 - arg1;
		sign = -sign;
	}

	if (arg1 > 90.0)
	{
		/* cotd(180-x) = -cotd(x) */
		arg1 = 180.0 - arg1;
		sign = -sign;
	}

	cot_arg1 = cosd_q1(arg1) / sind_q1(arg1);
	result = sign * (cot_arg1 / cot_45);

	/*
	 * On some machines we get cotd(270) = minus zero, but this isn't always
	 * true.  For portability, and because the user constituency for this
	 * function probably doesn't want minus zero, force it to plain zero.
	 */
	if (result == 0.0)
		result = 0.0;

	CHECKFLOATVAL(result, true /* cotd(0) == Inf */ , true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dsind			- returns the sine of arg1 (degrees)
 */
Datum
dsind(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* sind(-x) = -sind(x) */
		arg1 = -arg1;
		sign = -sign;
	}

	if (arg1 > 180.0)
	{
		/* sind(360-x) = -sind(x) */
		arg1 = 360.0 - arg1;
		sign = -sign;
	}

	if (arg1 > 90.0)
	{
		/* sind(180-x) = sind(x) */
		arg1 = 180.0 - arg1;
	}

	result = sign * sind_q1(arg1);

	CHECKFLOATVAL(result, false, true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dtand			- returns the tangent of arg1 (degrees)
 */
Datum
dtand(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;
	volatile float8 tan_arg1;
	int			sign = 1;

	/*
	 * Per the POSIX spec, return NaN if the input is NaN and throw an error
	 * if the input is infinite.
	 */
	if (isnan(arg1))
		PG_RETURN_FLOAT8(get_float8_nan());

	if (isinf(arg1))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("input is out of range")));

	INIT_DEGREE_CONSTANTS();

	/* Reduce the range of the input to [0,90] degrees */
	arg1 = fmod(arg1, 360.0);

	if (arg1 < 0.0)
	{
		/* tand(-x) = -tand(x) */
		arg1 = -arg1;
		sign = -sign;
	}

	if (arg1 > 180.0)
	{
		/* tand(360-x) = -tand(x) */
		arg1 = 360.0 - arg1;
		sign = -sign;
	}

	if (arg1 > 90.0)
	{
		/* tand(180-x) = -tand(x) */
		arg1 = 180.0 - arg1;
		sign = -sign;
	}

	tan_arg1 = sind_q1(arg1) / cosd_q1(arg1);
	result = sign * (tan_arg1 / tan_45);

	/*
	 * On some machines we get tand(180) = minus zero, but this isn't always
	 * true.  For portability, and because the user constituency for this
	 * function probably doesn't want minus zero, force it to plain zero.
	 */
	if (result == 0.0)
		result = 0.0;

	CHECKFLOATVAL(result, true /* tand(90) == Inf */ , true);
	PG_RETURN_FLOAT8(result);
}


/*
 *		degrees		- returns degrees converted from radians
 */
Datum
degrees(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	result = arg1 / RADIANS_PER_DEGREE;

	CHECKFLOATVAL(result, isinf(arg1), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}


/*
 *		dpi				- returns the constant PI
 */
Datum
dpi(PG_FUNCTION_ARGS)
{
	PG_RETURN_FLOAT8(M_PI);
}


/*
 *		radians		- returns radians converted from degrees
 */
Datum
radians(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float8		result;

	result = arg1 * RADIANS_PER_DEGREE;

	CHECKFLOATVAL(result, isinf(arg1), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}


/*
 *		drandom		- returns a random number
 */
Datum
drandom(PG_FUNCTION_ARGS)
{
	float8		result;

	/* result [0.0 - 1.0) */
	result = (double) random() / ((double) MAX_RANDOM_VALUE + 1);

	PG_RETURN_FLOAT8(result);
}


/*
 *		setseed		- set seed for the random number generator
 */
Datum
setseed(PG_FUNCTION_ARGS)
{
	float8		seed = PG_GETARG_FLOAT8(0);
	int			iseed;

	if (seed < -1 || seed > 1)
		elog(ERROR, "setseed parameter %f out of range [-1,1]", seed);

	iseed = (int) (seed * MAX_RANDOM_VALUE);
	srandom((unsigned int) iseed);

	PG_RETURN_VOID();
}



/*
 *		=========================
 *		FLOAT AGGREGATE OPERATORS
 *		=========================
 *
 *		float8_accum		- accumulate for AVG(), variance aggregates, etc.
 *		float4_accum		- same, but input data is float4
 *		float8_avg			- produce final result for float AVG()
 *		float8_var_samp		- produce final result for float VAR_SAMP()
 *		float8_var_pop		- produce final result for float VAR_POP()
 *		float8_stddev_samp	- produce final result for float STDDEV_SAMP()
 *		float8_stddev_pop	- produce final result for float STDDEV_POP()
 *
 * The transition datatype for all these aggregates is a 3-element array
 * of float8, holding the values N, sum(X), sum(X*X) in that order.
 *
 * Note that we represent N as a float to avoid having to build a special
 * datatype.  Given a reasonable floating-point implementation, there should
 * be no accuracy loss unless N exceeds 2 ^ 52 or so (by which time the
 * user will have doubtless lost interest anyway...)
 */
static float8 *
check_float8_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "%s: expected %d-element float8 array", caller, n);
	return (float8 *) ARR_DATA_PTR(transarray);
}

pg_noinline void
float_overflow_error(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value out of range: overflow")));
}

pg_noinline void
float_underflow_error(void)
{
	ereport(ERROR,
	        (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("value out of range: underflow")));
}

/*
 * float8_combine
 *
 * An aggregate combine function used to combine two 3 fields
 * aggregate transition data into a single transition data.
 * This function is used only in two stage aggregation and
 * shouldn't be called outside aggregate context.
 */
Datum
float8_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray2 = PG_GETARG_ARRAYTYPE_P(1);
	float8	   *transvalues1;
	float8	   *transvalues2;
	float8		N1,
				Sx1,
				Sxx1,
				N2,
				Sx2,
				Sxx2,
				tmp,
				N,
				Sx,
				Sxx;

	transvalues1 = check_float8_array(transarray1, "float8_combine", 3);
	transvalues2 = check_float8_array(transarray2, "float8_combine", 3);

	N1 = transvalues1[0];
	Sx1 = transvalues1[1];
	Sxx1 = transvalues1[2];

	N2 = transvalues2[0];
	Sx2 = transvalues2[1];
	Sxx2 = transvalues2[2];

	/*--------------------
	 * The transition values combine using a generalization of the
	 * Youngs-Cramer algorithm as follows:
	 *
	 *	N = N1 + N2
	 *	Sx = Sx1 + Sx2
	 *	Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N;
	 *
	 * It's worth handling the special cases N1 = 0 and N2 = 0 separately
	 * since those cases are trivial, and we then don't need to worry about
	 * division-by-zero errors in the general case.
	 *--------------------
	 */
	if (N1 == 0.0)
	{
		N = N2;
		Sx = Sx2;
		Sxx = Sxx2;
	}
	else if (N2 == 0.0)
	{
		N = N1;
		Sx = Sx1;
		Sxx = Sxx1;
	}
	else
	{
		N = N1 + N2;

		Sx = Sx1 + Sx2;
		if (unlikely(isinf(Sx)) && !isinf(Sx1) && !isinf(Sx2))
			float_overflow_error();

		tmp = Sx1 / N1 - Sx2 / N2;
		Sxx = Sxx1 + Sxx2 + N1 * N2 * tmp * tmp / N;
		if (unlikely(isinf(Sxx)) && !isinf(Sxx1) && !isinf(Sxx2))
			float_overflow_error();
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues1[0] = N;
		transvalues1[1] = Sx;
		transvalues1[2] = Sxx;

		PG_RETURN_ARRAYTYPE_P(transarray1);
	}
	else
	{
		Datum		transdatums[3];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);

		result = construct_array(transdatums, 3,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

Datum
float8_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8		newval = PG_GETARG_FLOAT8(1);
	float8	   *transvalues;
	float8		N,
				Sx,
				Sxx,
				tmp;

	transvalues = check_float8_array(transarray, "float8_accum", 3);
	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new value into the
	 * transition values.
	 */
	N += 1.0;
	Sx += newval;
	if (transvalues[0] > 0.0)
	{
		tmp = newval * N - Sx;
		Sxx += tmp * tmp / (N * transvalues[0]);

		/*
		 * Overflow check.  We only report an overflow error when finite
		 * inputs lead to infinite results.  Note also that Sxx should be NaN
		 * if any of the inputs are infinite, so we intentionally prevent Sxx
		 * from becoming infinite.
		 */
		if (isinf(Sx) || isinf(Sxx))
		{
			if (!isinf(transvalues[1]) && !isinf(newval))
				float_overflow_error();

			Sxx = get_float8_nan();
		}
	}
	else
	{
		/*
		 * At the first input, we normally can leave Sxx as 0.  However, if
		 * the first input is Inf or NaN, we'd better force Sxx to NaN;
		 * otherwise we will falsely report variance zero when there are no
		 * more inputs.
		 */
		if (isnan(newval) || isinf(newval))
			Sxx = get_float8_nan();
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues[0] = N;
		transvalues[1] = Sx;
		transvalues[2] = Sxx;

		PG_RETURN_ARRAYTYPE_P(transarray);
	}
	else
	{
		Datum		transdatums[3];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);

		result = construct_array(transdatums, 3,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

Datum
float4_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);

	/* do computations as float8 */
	float8		newval = PG_GETARG_FLOAT4(1);
	float8	   *transvalues;
	float8		N,
				Sx,
				Sxx,
				tmp;

	transvalues = check_float8_array(transarray, "float4_accum", 3);
	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new value into the
	 * transition values.
	 */
	N += 1.0;
	Sx += newval;
	if (transvalues[0] > 0.0)
	{
		tmp = newval * N - Sx;
		Sxx += tmp * tmp / (N * transvalues[0]);

		/*
		 * Overflow check.  We only report an overflow error when finite
		 * inputs lead to infinite results.  Note also that Sxx should be NaN
		 * if any of the inputs are infinite, so we intentionally prevent Sxx
		 * from becoming infinite.
		 */
		if (isinf(Sx) || isinf(Sxx))
		{
			if (!isinf(transvalues[1]) && !isinf(newval))
				float_overflow_error();

			Sxx = get_float8_nan();
		}
	}
	else
	{
		/*
		 * At the first input, we normally can leave Sxx as 0.  However, if
		 * the first input is Inf or NaN, we'd better force Sxx to NaN;
		 * otherwise we will falsely report variance zero when there are no
		 * more inputs.
		 */
		if (isnan(newval) || isinf(newval))
			Sxx = get_float8_nan();
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues[0] = N;
		transvalues[1] = Sx;
		transvalues[2] = Sxx;

		PG_RETURN_ARRAYTYPE_P(transarray);
	}
	else
	{
		Datum		transdatums[3];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);

		result = construct_array(transdatums, 3,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

Datum
float8_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sx;

	transvalues = check_float8_array(transarray, "float8_avg", 3);
	N = transvalues[0];
	Sx = transvalues[1];
	/* ignore Sxx */

	/* SQL defines AVG of no values to be NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sx / N);
}

Datum
float8_var_pop(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx;

	transvalues = check_float8_array(transarray, "float8_var_pop", 3);
	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Population variance is undefined when N is 0, so return NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Sxx / N);
}

Datum
float8_var_samp(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx;

	transvalues = check_float8_array(transarray, "float8_var_samp", 3);
	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Sample variance is undefined when N is 0 or 1, so return NULL */
	if (N <= 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Sxx / (N - 1.0));
}

Datum
float8_stddev_pop(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx;

	transvalues = check_float8_array(transarray, "float8_stddev_pop", 3);
	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Population stddev is undefined when N is 0, so return NULL */
	if (N == 0.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(sqrt(Sxx / N));
}

Datum
float8_stddev_samp(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx;

	transvalues = check_float8_array(transarray, "float8_stddev_samp", 3);
	N = transvalues[0];
	/* ignore Sx */
	Sxx = transvalues[2];

	/* Sample stddev is undefined when N is 0 or 1, so return NULL */
	if (N <= 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(sqrt(Sxx / (N - 1.0)));
}

/*
 *		=========================
 *		SQL2003 BINARY AGGREGATES
 *		=========================
 *
 * As with the preceding aggregates, we use the Youngs-Cramer algorithm to
 * reduce rounding errors in the aggregate final functions.
 *
 * The transition datatype for all these aggregates is a 6-element array of
 * float8, holding the values N, Sx=sum(X), Sxx=sum((X-Sx/N)^2), Sy=sum(Y),
 * Syy=sum((Y-Sy/N)^2), Sxy=sum((X-Sx/N)*(Y-Sy/N)) in that order.
 *
 * Note that Y is the first argument to all these aggregates!
 *
 * It might seem attractive to optimize this by having multiple accumulator
 * functions that only calculate the sums actually needed.  But on most
 * modern machines, a couple of extra floating-point multiplies will be
 * insignificant compared to the other per-tuple overhead, so I've chosen
 * to minimize code space instead.
 */

Datum
float8_regr_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8		newvalY = PG_GETARG_FLOAT8(1);
	float8		newvalX = PG_GETARG_FLOAT8(2);
	float8	   *transvalues;
	float8		N,
				Sx,
				Sxx,
				Sy,
				Syy,
				Sxy,
				tmpX,
				tmpY,
				scale;

	transvalues = check_float8_array(transarray, "float8_regr_accum", 6);
	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];
	Sy = transvalues[3];
	Syy = transvalues[4];
	Sxy = transvalues[5];

	/*
	 * Use the Youngs-Cramer algorithm to incorporate the new values into the
	 * transition values.
	 */
	N += 1.0;
	Sx += newvalX;
	Sy += newvalY;
	if (transvalues[0] > 0.0)
	{
		tmpX = newvalX * N - Sx;
		tmpY = newvalY * N - Sy;
		scale = 1.0 / (N * transvalues[0]);
		Sxx += tmpX * tmpX * scale;
		Syy += tmpY * tmpY * scale;
		Sxy += tmpX * tmpY * scale;

		/*
		 * Overflow check.  We only report an overflow error when finite
		 * inputs lead to infinite results.  Note also that Sxx, Syy and Sxy
		 * should be NaN if any of the relevant inputs are infinite, so we
		 * intentionally prevent them from becoming infinite.
		 */
		if (isinf(Sx) || isinf(Sxx) || isinf(Sy) || isinf(Syy) || isinf(Sxy))
		{
			if (((isinf(Sx) || isinf(Sxx)) &&
				 !isinf(transvalues[1]) && !isinf(newvalX)) ||
				((isinf(Sy) || isinf(Syy)) &&
				 !isinf(transvalues[3]) && !isinf(newvalY)) ||
				(isinf(Sxy) &&
				 !isinf(transvalues[1]) && !isinf(newvalX) &&
				 !isinf(transvalues[3]) && !isinf(newvalY)))
				float_overflow_error();

			if (isinf(Sxx))
				Sxx = get_float8_nan();
			if (isinf(Syy))
				Syy = get_float8_nan();
			if (isinf(Sxy))
				Sxy = get_float8_nan();
		}
	}
	else
	{
		/*
		 * At the first input, we normally can leave Sxx et al as 0.  However,
		 * if the first input is Inf or NaN, we'd better force the dependent
		 * sums to NaN; otherwise we will falsely report variance zero when
		 * there are no more inputs.
		 */
		if (isnan(newvalX) || isinf(newvalX))
			Sxx = Sxy = get_float8_nan();
		if (isnan(newvalY) || isinf(newvalY))
			Syy = Sxy = get_float8_nan();
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues[0] = N;
		transvalues[1] = Sx;
		transvalues[2] = Sxx;
		transvalues[3] = Sy;
		transvalues[4] = Syy;
		transvalues[5] = Sxy;

		PG_RETURN_ARRAYTYPE_P(transarray);
	}
	else
	{
		Datum		transdatums[6];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);
		transdatums[3] = Float8GetDatumFast(Sy);
		transdatums[4] = Float8GetDatumFast(Syy);
		transdatums[5] = Float8GetDatumFast(Sxy);

		result = construct_array(transdatums, 6,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

/*
 * float8_regr_combine
 *
 * An aggregate combine function used to combine two 6 fields
 * aggregate transition data into a single transition data.
 * This function is used only in two stage aggregation and
 * shouldn't be called outside aggregate context.
 */
Datum
float8_regr_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray1 = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *transarray2 = PG_GETARG_ARRAYTYPE_P(1);
	float8	   *transvalues1;
	float8	   *transvalues2;
	float8		N1,
				Sx1,
				Sxx1,
				Sy1,
				Syy1,
				Sxy1,
				N2,
				Sx2,
				Sxx2,
				Sy2,
				Syy2,
				Sxy2,
				tmp1,
				tmp2,
				N,
				Sx,
				Sxx,
				Sy,
				Syy,
				Sxy;

	transvalues1 = check_float8_array(transarray1, "float8_regr_combine", 6);
	transvalues2 = check_float8_array(transarray2, "float8_regr_combine", 6);

	N1 = transvalues1[0];
	Sx1 = transvalues1[1];
	Sxx1 = transvalues1[2];
	Sy1 = transvalues1[3];
	Syy1 = transvalues1[4];
	Sxy1 = transvalues1[5];

	N2 = transvalues2[0];
	Sx2 = transvalues2[1];
	Sxx2 = transvalues2[2];
	Sy2 = transvalues2[3];
	Syy2 = transvalues2[4];
	Sxy2 = transvalues2[5];

	/*--------------------
	 * The transition values combine using a generalization of the
	 * Youngs-Cramer algorithm as follows:
	 *
	 *	N = N1 + N2
	 *	Sx = Sx1 + Sx2
	 *	Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N
	 *	Sy = Sy1 + Sy2
	 *	Syy = Syy1 + Syy2 + N1 * N2 * (Sy1/N1 - Sy2/N2)^2 / N
	 *	Sxy = Sxy1 + Sxy2 + N1 * N2 * (Sx1/N1 - Sx2/N2) * (Sy1/N1 - Sy2/N2) / N
	 *
	 * It's worth handling the special cases N1 = 0 and N2 = 0 separately
	 * since those cases are trivial, and we then don't need to worry about
	 * division-by-zero errors in the general case.
	 *--------------------
	 */
	if (N1 == 0.0)
	{
		N = N2;
		Sx = Sx2;
		Sxx = Sxx2;
		Sy = Sy2;
		Syy = Syy2;
		Sxy = Sxy2;
	}
	else if (N2 == 0.0)
	{
		N = N1;
		Sx = Sx1;
		Sxx = Sxx1;
		Sy = Sy1;
		Syy = Syy1;
		Sxy = Sxy1;
	}
	else
	{
		N = N1 + N2;
		Sx = Sx1 + Sx2;
		if (unlikely(isinf(Sx)) && !isinf(Sx1) && !isinf(Sx2))
			float_overflow_error();

		tmp1 = Sx1 / N1 - Sx2 / N2;
		Sxx = Sxx1 + Sxx2 + N1 * N2 * tmp1 * tmp1 / N;
		if (unlikely(isinf(Sxx)) && !isinf(Sxx1) && !isinf(Sxx2))
			float_overflow_error();

		Sy = Sy1 + Sy2;
		if (unlikely(isinf(Sy)) && !isinf(Sy1) && !isinf(Sy2))
			float_overflow_error();

		tmp2 = Sy1 / N1 - Sy2 / N2;
		Syy = Syy1 + Syy2 + N1 * N2 * tmp2 * tmp2 / N;
		if (unlikely(isinf(Syy)) && !isinf(Syy1) && !isinf(Syy2))
			float_overflow_error();

		Sxy = Sxy1 + Sxy2 + N1 * N2 * tmp1 * tmp2 / N;
		if (unlikely(isinf(Sxy)) && !isinf(Sxy1) && !isinf(Sxy2))
			float_overflow_error();
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues1[0] = N;
		transvalues1[1] = Sx;
		transvalues1[2] = Sxx;
		transvalues1[3] = Sy;
		transvalues1[4] = Syy;
		transvalues1[5] = Sxy;

		PG_RETURN_ARRAYTYPE_P(transarray1);
	}
	else
	{
		Datum		transdatums[6];
		ArrayType  *result;

		transdatums[0] = Float8GetDatumFast(N);
		transdatums[1] = Float8GetDatumFast(Sx);
		transdatums[2] = Float8GetDatumFast(Sxx);
		transdatums[3] = Float8GetDatumFast(Sy);
		transdatums[4] = Float8GetDatumFast(Syy);
		transdatums[5] = Float8GetDatumFast(Sxy);

		result = construct_array(transdatums, 6,
								 FLOAT8OID,
								 sizeof(float8), FLOAT8PASSBYVAL, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}


Datum
float8_regr_sxx(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx;

	transvalues = check_float8_array(transarray, "float8_regr_sxx", 6);
	N = transvalues[0];
	Sxx = transvalues[2];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Sxx);
}

Datum
float8_regr_syy(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Syy;

	transvalues = check_float8_array(transarray, "float8_regr_syy", 6);
	N = transvalues[0];
	Syy = transvalues[4];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Syy is guaranteed to be non-negative */

	PG_RETURN_FLOAT8(Syy);
}

Datum
float8_regr_sxy(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_regr_sxy", 6);
	N = transvalues[0];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* A negative result is valid here */

	PG_RETURN_FLOAT8(Sxy);
}

Datum
float8_regr_avgx(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sx;

	transvalues = check_float8_array(transarray, "float8_regr_avgx", 6);
	N = transvalues[0];
	Sx = transvalues[1];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sx / N);
}

Datum
float8_regr_avgy(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sy;

	transvalues = check_float8_array(transarray, "float8_regr_avgy", 6);
	N = transvalues[0];
	Sy = transvalues[3];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sy / N);
}

Datum
float8_covar_pop(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_covar_pop", 6);
	N = transvalues[0];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / N);
}

Datum
float8_covar_samp(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_covar_samp", 6);
	N = transvalues[0];
	Sxy = transvalues[5];

	/* if N is <= 1 we should return NULL */
	if (N < 2.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / (N - 1.0));
}

Datum
float8_corr(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx,
				Syy,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_corr", 6);
	N = transvalues[0];
	Sxx = transvalues[2];
	Syy = transvalues[4];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx and Syy are guaranteed to be non-negative */

	/* per spec, return NULL for horizontal and vertical lines */
	if (Sxx == 0 || Syy == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / sqrt(Sxx * Syy));
}

Datum
float8_regr_r2(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx,
				Syy,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_regr_r2", 6);
	N = transvalues[0];
	Sxx = transvalues[2];
	Syy = transvalues[4];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx and Syy are guaranteed to be non-negative */

	/* per spec, return NULL for a vertical line */
	if (Sxx == 0)
		PG_RETURN_NULL();

	/* per spec, return 1.0 for a horizontal line */
	if (Syy == 0)
		PG_RETURN_FLOAT8(1.0);

	PG_RETURN_FLOAT8((Sxy * Sxy) / (Sxx * Syy));
}

Datum
float8_regr_slope(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sxx,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_regr_slope", 6);
	N = transvalues[0];
	Sxx = transvalues[2];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	/* per spec, return NULL for a vertical line */
	if (Sxx == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(Sxy / Sxx);
}

Datum
float8_regr_intercept(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8		N,
				Sx,
				Sxx,
				Sy,
				Sxy;

	transvalues = check_float8_array(transarray, "float8_regr_intercept", 6);
	N = transvalues[0];
	Sx = transvalues[1];
	Sxx = transvalues[2];
	Sy = transvalues[3];
	Sxy = transvalues[5];

	/* if N is 0 we should return NULL */
	if (N < 1.0)
		PG_RETURN_NULL();

	/* Note that Sxx is guaranteed to be non-negative */

	/* per spec, return NULL for a vertical line */
	if (Sxx == 0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8((Sy - Sx * Sxy / Sxx) / N);
}

/*
 *		====================================
 *		MIXED-PRECISION ARITHMETIC OPERATORS
 *		====================================
 */

/*
 *		float48pl		- returns arg1 + arg2
 *		float48mi		- returns arg1 - arg2
 *		float48mul		- returns arg1 * arg2
 *		float48div		- returns arg1 / arg2
 */
Datum
float48pl(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	result = arg1 + arg2;
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT8(result);
}

Datum
float48mi(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	result = arg1 - arg2;
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT8(result);
}

Datum
float48mul(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	result = arg1 * arg2;
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2),
				  arg1 == 0 || arg2 == 0);
	PG_RETURN_FLOAT8(result);
}

Datum
float48div(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);
	float8		result;

	if (arg2 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result = arg1 / arg2;
	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}

/*
 *		float84pl		- returns arg1 + arg2
 *		float84mi		- returns arg1 - arg2
 *		float84mul		- returns arg1 * arg2
 *		float84div		- returns arg1 / arg2
 */
Datum
float84pl(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float8		result;

	result = arg1 + arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT8(result);
}

Datum
float84mi(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float8		result;

	result = arg1 - arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), true);
	PG_RETURN_FLOAT8(result);
}

Datum
float84mul(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float8		result;

	result = arg1 * arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2),
				  arg1 == 0 || arg2 == 0);
	PG_RETURN_FLOAT8(result);
}

Datum
float84div(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);
	float8		result;

	if (arg2 == 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	result = arg1 / arg2;

	CHECKFLOATVAL(result, isinf(arg1) || isinf(arg2), arg1 == 0);
	PG_RETURN_FLOAT8(result);
}

/*
 *		====================
 *		COMPARISON OPERATORS
 *		====================
 */

/*
 *		float48{eq,ne,lt,le,gt,ge}		- float4/float8 comparison operations
 */
Datum
float48eq(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) == 0);
}

Datum
float48ne(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) != 0);
}

Datum
float48lt(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) < 0);
}

Datum
float48le(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) <= 0);
}

Datum
float48gt(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) > 0);
}

Datum
float48ge(PG_FUNCTION_ARGS)
{
	float4		arg1 = PG_GETARG_FLOAT4(0);
	float8		arg2 = PG_GETARG_FLOAT8(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) >= 0);
}

/*
 *		float84{eq,ne,lt,le,gt,ge}		- float8/float4 comparison operations
 */
Datum
float84eq(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) == 0);
}

Datum
float84ne(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) != 0);
}

Datum
float84lt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) < 0);
}

Datum
float84le(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) <= 0);
}

Datum
float84gt(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) > 0);
}

Datum
float84ge(PG_FUNCTION_ARGS)
{
	float8		arg1 = PG_GETARG_FLOAT8(0);
	float4		arg2 = PG_GETARG_FLOAT4(1);

	PG_RETURN_BOOL(float8_cmp_internal(arg1, arg2) >= 0);
}

/*
 * Implements the float8 version of the width_bucket() function
 * defined by SQL2003. See also width_bucket_numeric().
 *
 * 'bound1' and 'bound2' are the lower and upper bounds of the
 * histogram's range, respectively. 'count' is the number of buckets
 * in the histogram. width_bucket() returns an integer indicating the
 * bucket number that 'operand' belongs to in an equiwidth histogram
 * with the specified characteristics. An operand smaller than the
 * lower bound is assigned to bucket 0. An operand greater than the
 * upper bound is assigned to an additional bucket (with number
 * count+1). We don't allow "NaN" for any of the float8 inputs, and we
 * don't allow either of the histogram bounds to be +/- infinity.
 */
Datum
width_bucket_float8(PG_FUNCTION_ARGS)
{
	float8		operand = PG_GETARG_FLOAT8(0);
	float8		bound1 = PG_GETARG_FLOAT8(1);
	float8		bound2 = PG_GETARG_FLOAT8(2);
	int32		count = PG_GETARG_INT32(3);
	int32		result;

	if (count <= 0.0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("count must be greater than zero")));

	if (isnan(operand) || isnan(bound1) || isnan(bound2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("operand, lower bound, and upper bound cannot be NaN")));

	/* Note that we allow "operand" to be infinite */
	if (isinf(bound1) || isinf(bound2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("lower and upper bounds must be finite")));

	if (bound1 < bound2)
	{
		if (operand < bound1)
			result = 0;
		else if (operand >= bound2)
		{
			if (pg_add_s32_overflow(count, 1, &result))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("integer out of range")));
		}
		else
			result = ((float8) count * (operand - bound1) / (bound2 - bound1)) + 1;
	}
	else if (bound1 > bound2)
	{
		if (operand > bound1)
			result = 0;
		else if (operand <= bound2)
		{
			if (pg_add_s32_overflow(count, 1, &result))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("integer out of range")));
		}
		else
			result = ((float8) count * (bound1 - operand) / (bound1 - bound2)) + 1;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("lower bound cannot equal upper bound")));
		result = 0;				/* keep the compiler quiet */
	}

	PG_RETURN_INT32(result);
}

/*convert from float8 to interval*/
Datum
float4_interval(PG_FUNCTION_ARGS)
{
	Datum val = PG_GETARG_DATUM(0);
	return DirectFunctionCall1(numeric_interval, DirectFunctionCall1(float4_numeric, val));
}

/*convert from float8 to interval*/
Datum
float8_interval(PG_FUNCTION_ARGS)
{
	Datum val = PG_GETARG_DATUM(0);
	return DirectFunctionCall1(numeric_interval,
                               DirectFunctionCall1(float8_numeric, val));
}

/* ========== PRIVATE ROUTINES ========== */

#ifndef HAVE_CBRT

static double
cbrt(double x)
{
	int			isneg = (x < 0.0);
	double		absx = fabs(x);
	double		tmpres = pow(absx, (double) 1.0 / (double) 3.0);

	/*
	 * The result is somewhat inaccurate --- not really pow()'s fault, as the
	 * exponent it's handed contains roundoff error.  We can improve the
	 * accuracy by doing one iteration of Newton's formula.  Beware of zero
	 * input however.
	 */
	if (tmpres > 0.0)
		tmpres -= (tmpres - absx / (tmpres * tmpres)) / (double) 3.0;

	return isneg ? -tmpres : tmpres;
}

#endif							/* !HAVE_CBRT */
