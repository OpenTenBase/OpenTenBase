/*-------------------------------------------------------------------------
 *
 * numeric.c
 *	  An exact numeric data type for the Postgres database system
 *
 * Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
 *
 * Many of the algorithmic ideas are borrowed from David M. Smith's "FM"
 * multiple-precision math library, most recently published as Algorithm
 * 786: Multiple-Precision Complex Arithmetic and Functions, ACM
 * Transactions on Mathematical Software, Vol. 24, No. 4, December 1998,
 * pages 359-367.
 *
 * Copyright (c) 1998-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/numeric.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <limits.h>
#include <math.h>

#include "access/hash.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "funcapi.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/sortsupport.h"
#include "utils/biginteger.h"

/* ----------
 * Uncomment the following to enable compilation of dump_numeric()
 * and dump_var() and to get a dump of any result produced by make_result().
 * ----------
#define NUMERIC_DEBUG
 */

/* ----------
 * Local data types
 *
 * Numeric values are represented in a base-NBASE floating point format.
 * Each "digit" ranges from 0 to NBASE-1.  The type NumericDigit is signed
 * and wide enough to store a digit.  We assume that NBASE*NBASE can fit in
 * an int.  Although the purely calculational routines could handle any even
 * NBASE that's less than sqrt(INT_MAX), in practice we are only interested
 * in NBASE a power of ten, so that I/O conversions and decimal rounding
 * are easy.  Also, it's actually more efficient if NBASE is rather less than
 * sqrt(INT_MAX), so that there is "headroom" for mul_var and div_var_fast to
 * postpone processing carries.
 *
 * Values of NBASE other than 10000 are considered of historical interest only
 * and are no longer supported in any sense; no mechanism exists for the client
 * to discover the base, so every client supporting binary mode expects the
 * base-10000 format.  If you plan to change this, also note the numeric
 * abbreviation code, which assumes NBASE=10000.
 * ----------
 */

#if 0
#define NBASE		10
#define HALF_NBASE	5
#define DEC_DIGITS	1			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	4	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	8

typedef signed char NumericDigit;
#endif

#if 0
#define NBASE		100
#define HALF_NBASE	50
#define DEC_DIGITS	2			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	3	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	6

typedef signed char NumericDigit;
#endif

/* below is moved to numeric.h */
#if 0
#if 1
#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

typedef int16 NumericDigit;
#endif

/*
 * The Numeric type as stored on disk.
 *
 * If the high bits of the first word of a NumericChoice (n_header, or
 * n_short.n_header, or n_long.n_sign_dscale) are NUMERIC_SHORT, then the
 * numeric follows the NumericShort format; if they are NUMERIC_POS or
 * NUMERIC_NEG, it follows the NumericLong format.  If they are NUMERIC_NAN,
 * it is a NaN.  We currently always store a NaN using just two bytes (i.e.
 * only n_header), but previous releases used only the NumericLong format,
 * so we might find 4-byte NaNs on disk if a database has been migrated using
 * pg_upgrade.  In either case, when the high bits indicate a NaN, the
 * remaining bits are never examined.  Currently, we always initialize these
 * to zero, but it might be possible to use them for some other purpose in
 * the future.
 *
 * In the NumericShort format, the remaining 14 bits of the header word
 * (n_short.n_header) are allocated as follows: 1 for sign (positive or
 * negative), 6 for dynamic scale, and 7 for weight.  In practice, most
 * commonly-encountered values can be represented this way.
 *
 * In the NumericLong format, the remaining 14 bits of the header word
 * (n_long.n_sign_dscale) represent the display scale; and the weight is
 * stored separately in n_weight.
 *
 * NOTE: by convention, values in the packed form have been stripped of
 * all leading and trailing zero digits (where a "digit" is of base NBASE).
 * In particular, if the value is zero, there will be no digits at all!
 * The weight is arbitrary in that case, but we normally set it to zero.
 */

struct NumericShort
{
	uint16		n_header;		/* Sign + display scale + weight */
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
	uint16		n_sign_dscale;	/* Sign + display scale */
	int16		n_weight;		/* Weight of 1st digit	*/
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
	uint16		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
};

struct NumericData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};


/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_NAN			0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_NAN(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_NAN, we want the short header;
 * otherwise, we want the long one.  Instead of testing against each value, we
 * can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_DSCALE_MAX		\
	(NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_SHORT_WEIGHT_MAX		NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN		(-(NUMERIC_SHORT_WEIGHT_MASK+1))

/*
 * Extract sign, display scale, weight.
 */

#define NUMERIC_DSCALE_MASK			0x3FFF

#define NUMERIC_SIGN(n) \
	(NUMERIC_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
		NUMERIC_NEG : NUMERIC_POS) : NUMERIC_FLAGBITS(n))
#define NUMERIC_DSCALE(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))

/* ----------
 * NumericVar is the format we use for arithmetic.  The digit-array part
 * is the same as the NumericData storage format, but the header is more
 * complex.
 *
 * The value represented by a NumericVar is determined by the sign, weight,
 * ndigits, and digits[] array.
 *
 * Note: the first digit of a NumericVar's value is assumed to be multiplied
 * by NBASE ** weight.  Another way to say it is that there are weight+1
 * digits before the decimal point.  It is possible to have weight < 0.
 *
 * buf points at the physical start of the palloc'd digit buffer for the
 * NumericVar.  digits points at the first digit in actual use (the one
 * with the specified weight).  We normally leave an unused digit or two
 * (preset to zeroes) between buf and digits, so that there is room to store
 * a carry out of the top digit without reallocating space.  We just need to
 * decrement digits (and increment weight) to make room for the carry digit.
 * (There is no such extra space in a numeric value stored in the database,
 * only in a NumericVar in memory.)
 *
 * If buf is NULL then the digit buffer isn't actually palloc'd and should
 * not be freed --- see the constants below for an example.
 *
 * dscale, or display scale, is the nominal precision expressed as number
 * of digits after the decimal point (it must always be >= 0 at present).
 * dscale may be more than the number of physically stored fractional digits,
 * implying that we have suppressed storage of significant trailing zeroes.
 * It should never be less than the number of stored digits, since that would
 * imply hiding digits that are present.  NOTE that dscale is always expressed
 * in *decimal* digits, and so it may correspond to a fractional number of
 * base-NBASE digits --- divide by DEC_DIGITS to convert to NBASE digits.
 *
 * rscale, or result scale, is the target precision for a computation.
 * Like dscale it is expressed as number of *decimal* digits after the decimal
 * point, and is always >= 0 at present.
 * Note that rscale is not stored in variables --- it's figured on-the-fly
 * from the dscales of the inputs.
 *
 * While we consistently use "weight" to refer to the base-NBASE weight of
 * a numeric value, it is convenient in some scale-related calculations to
 * make use of the base-10 weight (ie, the approximate log10 of the value).
 * To avoid confusion, such a decimal-units weight is called a "dweight".
 *
 * NB: All the variable-level functions are written in a style that makes it
 * possible to give one and the same variable as argument and destination.
 * This is feasible because the digit buffer is separate from the variable.
 * ----------
 */
#define	NUMERIC_LOCAL_NDIG	36		/* number of 'digits' in local digits[] */
#define NUMERIC_LOCAL_NMAX	(NUMERIC_LOCAL_NDIG - 2)
#define	NUMERIC_LOCAL_DTXT	128		/* number of char in local text */
#define NUMERIC_LOCAL_DMAX	(NUMERIC_LOCAL_DTXT - 2)

typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
	NumericDigit ndb[NUMERIC_LOCAL_NDIG];	/* local space for digits[] */
} NumericVar;
#endif

/* ----------
 * Data for generate_series
 * ----------
 */
typedef struct
{
	NumericVar	current;
	NumericVar	stop;
	NumericVar	step;
} generate_series_numeric_fctx;


/* ----------
 * Sort support.
 * ----------
 */
typedef struct
{
	void	   *buf;			/* buffer for short varlenas */
	int64		input_count;	/* number of non-null values seen */
	bool		estimating;		/* true if estimating cardinality */

	hyperLogLogState abbr_card; /* cardinality estimator */
} NumericSortSupport;


/* ----------
 * Fast sum accumulator.
 *
 * NumericSumAccum is used to implement SUM(), and other standard aggregates
 * that track the sum of input values.  It uses 32-bit integers to store the
 * digits, instead of the normal 16-bit integers (with NBASE=10000).  This
 * way, we can safely accumulate up to NBASE - 1 values without propagating
 * carry, before risking overflow of any of the digits.  'num_uncarried'
 * tracks how many values have been accumulated without propagating carry.
 *
 * Positive and negative values are accumulated separately, in 'pos_digits'
 * and 'neg_digits'.  This is simpler and faster than deciding whether to add
 * or subtract from the current value, for each new value (see sub_var() for
 * the logic we avoid by doing this).  Both buffers are of same size, and
 * have the same weight and scale.  In accum_sum_final(), the positive and
 * negative sums are added together to produce the final result.
 *
 * When a new value has a larger ndigits or weight than the accumulator
 * currently does, the accumulator is enlarged to accommodate the new value.
 * We normally have one zero digit reserved for carry propagation, and that
 * is indicated by the 'have_carry_space' flag.  When accum_sum_carry() uses
 * up the reserved digit, it clears the 'have_carry_space' flag.  The next
 * call to accum_sum_add() will enlarge the buffer, to make room for the
 * extra digit, and set the flag again.
 *
 * To initialize a new accumulator, simply reset all fields to zeros.
 *
 * The accumulator does not handle NaNs.
 * ----------
 */
typedef struct NumericSumAccum
{
	int			ndigits;
	int			weight;
	int			dscale;
	int			num_uncarried;
	bool		have_carry_space;
	int32	   *pos_digits;
	int32	   *neg_digits;
} NumericSumAccum;

/*
 * We define our own macros for packing and unpacking abbreviated-key
 * representations for numeric values in order to avoid depending on
 * USE_FLOAT8_BYVAL.  The type of abbreviation we use is based only on
 * the size of a datum, not the argument-passing convention for float8.
 */
#define NUMERIC_ABBREV_BITS (SIZEOF_DATUM * BITS_PER_BYTE)
#if SIZEOF_DATUM == 8
#define NumericAbbrevGetDatum(X) ((Datum) SET_8_BYTES(X))
#define DatumGetNumericAbbrev(X) ((int64) GET_8_BYTES(X))
#define NUMERIC_ABBREV_NAN		 NumericAbbrevGetDatum(PG_INT64_MIN)
#define NUMERIC_ABBREV_POS_INF		 NumericAbbrevGetDatum(PG_INT64_MIN)
#define NUMERIC_ABBREV_NEG_INF		 NumericAbbrevGetDatum(PG_INT64_MAX)
#else
#define NumericAbbrevGetDatum(X) ((Datum) SET_4_BYTES(X))
#define DatumGetNumericAbbrev(X) ((int32) GET_4_BYTES(X))
#define NUMERIC_ABBREV_NAN		 NumericAbbrevGetDatum(PG_INT32_MIN)
#define NUMERIC_ABBREV_POS_INF		 NumericAbbrevGetDatum(PG_INT32_MIN)
#define NUMERIC_ABBREV_NEG_INF		 NumericAbbrevGetDatum(PG_INT32_MAX)
#endif


int enable_simple_numeric = 0;

/* ----------
 * Some preinitialized constants
 * ----------
 */
static NumericDigit const_zero_data[1] = {0};
static NumericVar const_zero =
{0, 0, NUMERIC_POS, 0, NULL, const_zero_data, NUMERIC_NUMBER};

static NumericDigit const_one_data[1] = {1};
static NumericVar const_one =
{1, 0, NUMERIC_POS, 0, NULL, const_one_data, NUMERIC_NUMBER};

static NumericVar const_neg_one =
{1, 0, NUMERIC_NEG, 0, NULL, const_one_data, NUMERIC_NUMBER};

static NumericDigit const_two_data[1] = {2};
static NumericVar const_two =
{1, 0, NUMERIC_POS, 0, NULL, const_two_data, NUMERIC_NUMBER};

#if DEC_DIGITS == 4 || DEC_DIGITS == 2
static NumericDigit const_ten_data[1] = {10};
static NumericVar const_ten =
{1, 0, NUMERIC_POS, 0, NULL, const_ten_data, NUMERIC_NUMBER};
#elif DEC_DIGITS == 1
static NumericDigit const_ten_data[1] = {1};
static NumericVar const_ten =
{1, 1, NUMERIC_POS, 0, NULL, const_ten_data, NUMERIC_NUMBER};
#endif

#if DEC_DIGITS == 4
static NumericDigit const_zero_point_nine_data[1] = {9000};
#elif DEC_DIGITS == 2
static NumericDigit const_zero_point_nine_data[1] = {90};
#elif DEC_DIGITS == 1
static NumericDigit const_zero_point_nine_data[1] = {9};
#endif
static NumericVar const_zero_point_nine =
{1, -1, NUMERIC_POS, 1, NULL, const_zero_point_nine_data, NUMERIC_NUMBER};

#if DEC_DIGITS == 4
static NumericDigit const_one_point_one_data[2] = {1, 1000};
#elif DEC_DIGITS == 2
static NumericDigit const_one_point_one_data[2] = {1, 10};
#elif DEC_DIGITS == 1
static NumericDigit const_one_point_one_data[2] = {1, 1};
#endif
static NumericVar const_one_point_one =
{2, 0, NUMERIC_POS, 1, NULL, const_one_point_one_data, NUMERIC_NUMBER};

static NumericVar const_nan =
{0, 0, NUMERIC_NAN, 0, NULL, NULL, NUMERIC_NUMBER};

#if DEC_DIGITS == 4
static const int round_powers[4] = {0, 1000, 100, 10};
#endif

static const NumericDigit const_ora_one[1] = {100};

/* Min abs-value of NUMBER type in opentenbase_ora, 10^-130, include it */
static const NumericVar const_ora_min =
{1, -33, NUMERIC_POS, 130, NULL, (NumericDigit *) const_ora_one, NUMERIC_NUMBER};

/* Max abs-value of NUMBER type in opentenbase_ora, 10^126, not include it */
static const NumericVar const_ora_max =
{1, 31, NUMERIC_POS, 0, NULL, (NumericDigit *) const_ora_one, NUMERIC_NUMBER};

/* TODO DEC_DIGITS == 2 */
static NumericDigit const_ora_binary_float_pos_max_data[2] = {340, 2820};
static NumericDigit const_ora_binary_float_pos_min_data[2] = {117, 5490};
static NumericDigit const_ora_binary_double_pos_max_data[5] = {1, 7976, 9313, 4862, 3100};
static NumericDigit const_ora_binary_double_pos_min_data[5] = {2, 2250, 7485, 8507, 2000};

/* 3.40282 * 10^38 */
static const NumericVar const_ora_binary_float_pos_max =
{2, 9, NUMERIC_POS, 0, NULL, (NumericDigit *) const_ora_binary_float_pos_max_data, NUMERIC_BINARY_FLOAT};

/* 1.17549 * 10^-38 */
static const NumericVar const_ora_binary_float_pos_min =
{2, -10, NUMERIC_POS, 43, NULL, (NumericDigit *) const_ora_binary_float_pos_min_data, NUMERIC_BINARY_FLOAT};

/* 1.79769313486231 * 10^308 */
static const NumericVar const_ora_binary_double_pos_max =
{5, 77, NUMERIC_POS, 0, NULL, (NumericDigit *) const_ora_binary_double_pos_max_data, NUMERIC_BINARY_DOUBLE};

/* 2.22507485850720 * 10^-308 */
static const NumericVar const_ora_binary_double_pos_min =
{5, -77, NUMERIC_POS, 322, NULL, (NumericDigit *) const_ora_binary_double_pos_min_data, NUMERIC_BINARY_DOUBLE};

static const NumericVar const_ora_binary_float_pos_inf =
{0, 0, NUMERIC_POS_INF, 0, NULL, NULL, NUMERIC_BINARY_FLOAT};

static const NumericVar const_ora_binary_float_neg_inf =
{0, 0, NUMERIC_NEG_INF, 0, NULL, NULL, NUMERIC_BINARY_FLOAT};

static const NumericVar const_ora_binary_double_pos_inf =
{0, 0, NUMERIC_POS_INF, 0, NULL, NULL, NUMERIC_BINARY_DOUBLE};

static const NumericVar const_ora_binary_double_neg_inf =
{0, 0, NUMERIC_NEG_INF, 0, NULL, NULL, NUMERIC_BINARY_DOUBLE};

/* ----------
 * Local functions
 * ----------
 */

#ifdef NUMERIC_DEBUG
static void dump_numeric(const char *str, Numeric num);
static void dump_var(const char *str, NumericVar *var);
#else
#define dump_numeric(s,n)
#define dump_var(s,v)
#endif

#define numeric_handle_inf_datum(num)                                         \
    do {                                                                      \
		if (NUMERIC_IS_POS_INF(num))                                          \
            PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf)); \
        if (NUMERIC_IS_NEG_INF(num))                                          \
            PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_neg_inf)); \
	} while (0)

#define numeric_handle_inf_cstr(num)  \
    do {                              \
		if (NUMERIC_IS_POS_INF(num))  \
            return pstrdup("Inf");    \
        if (NUMERIC_IS_NEG_INF(num))  \
            return pstrdup("-Inf");   \
	} while (0)

#define numeric_handle_inf_numeric(num1, num2)                     \
    do {                                                           \
		if (NUMERIC_IS_POS_INF(num1) || NUMERIC_IS_POS_INF(num2))  \
            return make_result(&const_ora_binary_double_pos_inf);  \
        if (NUMERIC_IS_NEG_INF(num1) || NUMERIC_IS_NEG_INF(num2))  \
            return make_result(&const_ora_binary_double_neg_inf);  \
	} while (0)

#define numeric_handle_inf_datum2(num1, num2)                      \
    do {                                                           \
		if (NUMERIC_IS_POS_INF(num1) || NUMERIC_IS_POS_INF(num2))  \
            PG_RETURN_NUMERIC(&const_ora_binary_double_pos_inf);   \
        if (NUMERIC_IS_NEG_INF(num1) || NUMERIC_IS_NEG_INF(num2))  \
            PG_RETURN_NUMERIC(&const_ora_binary_double_neg_inf);   \
	} while (0)

#define numeric_handle_inf_datum2_res(num1, num2, res)                        \
    else if (NUMERIC_IS_POS_INF(num1) || NUMERIC_IS_POS_INF(num2))            \
        res = NumericGetDatum(make_result(&const_ora_binary_double_pos_inf)); \
    else if (NUMERIC_IS_NEG_INF(num1) || NUMERIC_IS_NEG_INF(num2))            \
        res = NumericGetDatum(make_result(&const_ora_binary_double_neg_inf))

#define numeric_handle_inf_datum_res(num, res)                        \
    else if (NUMERIC_IS_POS_INF(num))            \
        res = NumericGetDatum(make_result(&const_ora_binary_double_pos_inf)); \
    else if (NUMERIC_IS_NEG_INF(num))            \
        res = NumericGetDatum(make_result(&const_ora_binary_double_neg_inf))

#define numeric_handle_inf_datum2_res_pointer(num1, num2, res)                        \
    else if (NUMERIC_IS_POS_INF(num1) || NUMERIC_IS_POS_INF(num2))            \
        res = (Pointer)(make_result(&const_ora_binary_double_pos_inf)); \
    else if (NUMERIC_IS_NEG_INF(num1) || NUMERIC_IS_NEG_INF(num2))            \
        res = (Pointer)(make_result(&const_ora_binary_double_neg_inf))

#define numeric_handle_inf_datum_res_pointer(num, res)                        \
    else if (NUMERIC_IS_POS_INF(num))            \
        res = (Pointer)(make_result(&const_ora_binary_double_pos_inf)); \
    else if (NUMERIC_IS_NEG_INF(num))            \
        res = (Pointer)(make_result(&const_ora_binary_double_neg_inf))

#define numeric_handle_inf_float8(num)                \
    do {                                              \
		if (NUMERIC_IS_POS_INF(num))                  \
            PG_RETURN_FLOAT8(get_float8_infinity());  \
        if (NUMERIC_IS_NEG_INF(num))                  \
            PG_RETURN_FLOAT8(-get_float8_infinity()); \
	} while (0)

#define numeric_handle_inf_float4(num)                \
    do {                                              \
		if (NUMERIC_IS_POS_INF(num))                  \
            PG_RETURN_FLOAT4(get_float4_infinity());  \
        if (NUMERIC_IS_NEG_INF(num))                  \
            PG_RETURN_FLOAT4(-get_float4_infinity()); \
	} while (0)

#define numeric_handle_inf_null(num)  \
    do {                              \
		if (NUMERIC_IS_INF(num))      \
            PG_RETURN_NULL();         \
	} while (0)

#define numeric_handle_inf_convert(num, msg)                                     \
    do {                                                                         \
		if (NUMERIC_IS_INF(num))                                                 \
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),errmsg(msg))); \
	} while (0)

#define numeric_handle_inf_elog(num, msg)                                        \
    do {                                                                         \
		if (NUMERIC_IS_INF(num))                                                 \
            elog(ERROR, msg);                                                    \
	} while (0)

#define numeric_handle_inf_cmp(num1, num2, res)               \
    else if (NUMERIC_IS_POS_INF(num1))                        \
    {                                                         \
        if (NUMERIC_IS_POS_INF(num2))                         \
            res = 0; /* Inf = Inf */                          \
        else                                                  \
            res = 1; /* Inf > non-Inf(other than NAN) */      \
    }                                                         \
    else if (NUMERIC_IS_POS_INF(num2))                        \
        res = -1; /* non-Inf(other than NAN) < Inf */         \
    else if (NUMERIC_IS_NEG_INF(num1))                        \
    {                                                         \
        if (NUMERIC_IS_NEG_INF(num2))                         \
            res = 0; /* -Inf = -Inf */                        \
        else                                                  \
            res = -1; /* -Inf < non-(-Inf)(other than NAN) */ \
    }                                                         \
    else if (NUMERIC_IS_NEG_INF(num2))                        \
        /* non-(-Inf)(other than NAN) > -Inf */               \
        res = 1

#define digitbuf_alloc(ndigits)  \
	((NumericDigit *) palloc((ndigits) * sizeof(NumericDigit)))

#define digitbuf_free(v)			\
	do { 					\
		if ((v)->buf != (v)->ndb)	\
		{				\
			pfree((v)->buf); 	\
			(v)->buf = (v)->ndb;	\
		}				\
	} while (0)

#define free_var(v)	\
				digitbuf_free((v));

/*
 * init_alloc_var() -
 *
 *	Init a var and allocate digit buffer of ndigits digits (plus a spare
 *  digit for rounding).
 *  Called when first using a var.
 */
#define	init_alloc_var(v, n) \
	do 	{	\
		(v)->buf = (v)->ndb;	\
		(v)->ndigits = (n);	\
		if ((n) > NUMERIC_LOCAL_NMAX)	\
			(v)->buf = digitbuf_alloc((n) + 1);	\
		(v)->buf[0] = 0;	\
		(v)->digits = (v)->buf + 1;\
    	(v)->subtype = ORA_MODE ? NUMERIC_NUMBER : NUMERIC_NUMERIC; \
	} while (0)

/* moved to numeric.h */
#if 0
#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_CAN_BE_SHORT(scale,weight) \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && \
	(weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
	(weight) >= NUMERIC_SHORT_WEIGHT_MIN)
#endif

static void alloc_var(NumericVar *var, int ndigits);
static void zero_var(NumericVar *var);

static const char *set_var_from_str(const char *str, const char *cp,
				 NumericVar *dest);
static void set_var_from_num(Numeric value, NumericVar *dest);
static void init_var_from_var(const NumericVar *value, NumericVar *dest);
static void set_var_from_var(const NumericVar *value, NumericVar *dest);
static char *get_str_from_var(const NumericVar *var);

static Numeric make_result_opt_error(NumericVar *var, bool *error);

static void apply_typmod(NumericVar *var, int32 typmod);

static bool numericvar_to_int32(const NumericVar *var, int32 *result);
static void int64_to_numericvar(int64 val, NumericVar *var);

static bool numericvar_to_int128(const NumericVar *var, int128 *result);
#ifdef HAVE_INT128
static void int128_to_numericvar(int128 val, NumericVar *var);
#endif

static double numeric_to_double_no_overflow(Numeric num);
static double numericvar_to_double_no_overflow(const NumericVar *var);

static Datum numeric_abbrev_convert(Datum original_datum, SortSupport ssup);
static bool numeric_abbrev_abort(int memtupcount, SortSupport ssup);
static int	numeric_fast_cmp(Datum x, Datum y, SortSupport ssup);
static int	numeric_cmp_abbrev(Datum x, Datum y, SortSupport ssup);

static Datum numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss);

//static int	cmp_numerics(Numeric num1, Numeric num2);
static int	cmp_var(const NumericVar *var1, const NumericVar *var2);
static int	cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
						   int var1weight, int var1sign,
						   const NumericDigit *var2digits, int var2ndigits,
						   int var2weight, int var2sign);
static void add_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void sub_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result);
static void mul_var(const NumericVar *var1, const NumericVar *var2,
					NumericVar *result,
					int rscale);
static void div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result, int rscale,
                    RoundType rt);
static void div_var_fast(const NumericVar *var1, const NumericVar *var2,
						 NumericVar *result, int rscale, RoundType rt);
static void div_var_int(const NumericVar *var, int ival, int ival_weight,
						NumericVar *result, int rscale, RoundType rt, bool ora_scale);
static int	select_div_scale(const NumericVar *var1, const NumericVar *var2);
static void mod_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result);
static void remainder_var(NumericVar *var1, NumericVar *var2, NumericVar *result);
static void div_mod_var(const NumericVar *var1, const NumericVar *var2,
						NumericVar *quot, NumericVar *rem);
static void ceil_var(const NumericVar *var, NumericVar *result);
static void floor_var(const NumericVar *var, NumericVar *result);

static void sqrt_var(NumericVar *arg, NumericVar *result, int rscale);
static void exp_var(NumericVar *arg, NumericVar *result, int rscale);
static int	estimate_ln_dweight(NumericVar *var);
static void ln_var(NumericVar *arg, NumericVar *result, int rscale);
static void log_var(NumericVar *base, NumericVar *num, NumericVar *result);
static void power_var(NumericVar *base, NumericVar *exp, NumericVar *result);
static void power_var_int(NumericVar *base, int exp, NumericVar *result,
			  int rscale);

static int	cmp_abs(const NumericVar *var1, const NumericVar *var2);
static int cmp_abs_common(const NumericDigit *var1digits, int var1ndigits,
			   int var1weight,
			   const NumericDigit *var2digits, int var2ndigits,
			   int var2weight);
static void add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result);
static void sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result);
static void round_var(NumericVar *var, int rscale, RoundType rt);
static void trunc_var(NumericVar *var, int rscale);
static void strip_var(NumericVar *var);
static void compute_bucket(Numeric operand, Numeric bound1, Numeric bound2,
			   const NumericVar *count_var, NumericVar *result_var);

static void accum_sum_add(NumericSumAccum *accum, const NumericVar *var1);
static void accum_sum_rescale(NumericSumAccum *accum, const NumericVar *val);
static void accum_sum_carry(NumericSumAccum *accum);
static void accum_sum_reset(NumericSumAccum *accum);
static void accum_sum_final(NumericSumAccum *accum, NumericVar *result);
static void accum_sum_copy(NumericSumAccum *dst, NumericSumAccum *src);
static void accum_sum_combine(NumericSumAccum *accum, NumericSumAccum *accum2);
static void check_ora_number_overflow(NumericVar *var, bool report_error);
static bool adjust_scale_ora(NumericVar *var);
static void adjust_numeric_var(NumericVar *result, int rscale, RoundType rt, bool ora_scale);
static NumericSubtype typmod_to_numeric_subtype(int32 typmod);
/* ----------------------------------------------------------------------
 *
 * Input-, output- and rounding-functions
 *
 * ----------------------------------------------------------------------
 */


/*
 * numeric_in() -
 *
 *	Input function for numeric data type
 */
Datum
numeric_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	Numeric		res = NULL;
	const char *cp;
	/* Skip leading spaces */
	cp = str;
	while (*cp)
	{
		if (!isspace((unsigned char) *cp))
			break;
		cp++;
	}

	if (isdigit(*cp))
	{
		/*
 		* Use set_var_from_str() to parse a normal numeric value
 		*/
		NumericVar	value;

		cp = set_var_from_str(str, cp, &value);

		/*
		 * We duplicate a few lines of code here because we would like to
		 * throw any trailing-junk syntax error before any semantic error
		 * resulting from apply_typmod.  We can't easily fold the two cases
		 * together because we mustn't apply apply_typmod to a NaN.
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								errmsg("invalid input syntax for type %s: \"%s\"",
									   "numeric", str)));
			cp++;
		}

		apply_typmod(&value, typmod);

		res = make_result(&value);
		free_var(&value);
		PG_RETURN_NUMERIC(res);
	}

	/*
	 * Check for NaN
	 */
	if ((*cp == 'N' || *cp == 'n') && pg_strncasecmp(cp, "NaN", 3) == 0)
	{
		res = make_result(&const_nan);

		/* Should be nothing left but spaces */
		cp += 3;
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type %s: \"%s\"",
								"numeric", str)));
			cp++;
		}
	}
	else if (unlikely((*cp == '+' || *cp == '-' || *cp == 'I' || *cp == 'i')
					&& (((pg_strncasecmp(cp, "+Inf", 4) == 0) ? ((res = make_result(&const_ora_binary_double_pos_inf)) && (cp += 4)) : FALSE)
					  || ((pg_strncasecmp(cp, "Inf", 3) == 0)  ? ((res = make_result(&const_ora_binary_double_pos_inf)) && (cp += 3)) : FALSE)
					  || ((pg_strncasecmp(cp, "-Inf", 4) == 0)  ? ((res = make_result(&const_ora_binary_double_neg_inf)) && (cp += 4)) : FALSE))))
	{
		/* Should be nothing left but spaces */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
								errmsg("invalid input syntax for type %s: \"%s\"",
									   "numeric", str)));
			cp++;
		}
	}
	else
	{
		/*
		 * Use set_var_from_str() to parse a normal numeric value
		 */
		NumericVar	value;

		cp = set_var_from_str(str, cp, &value);

		/*
		 * We duplicate a few lines of code here because we would like to
		 * throw any trailing-junk syntax error before any semantic error
		 * resulting from apply_typmod.  We can't easily fold the two cases
		 * together because we mustn't apply apply_typmod to a NaN.
		 */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type %s: \"%s\"",
								"numeric", str)));
			cp++;
		}

		apply_typmod(&value, typmod);

		res = make_result(&value);
		free_var(&value);
	}

	PG_RETURN_NUMERIC(res);
}

/*
 * @Description: print numeric data to string with trailing zeroes if exists
 *               Internal function
 *
 * @IN num: numeric data
 * @Return: Output string for numeric data type with trailing zeroes if exists
 */
char *
numeric_out_internal(Numeric num)
{
	NumericVar	x;
	char	   *str;

	/*
	 * Handle NaN, Inf and -Inf
	 */
	if (NUMERIC_IS_NAN(num))
		return pstrdup("NaN");
    if (NUMERIC_IS_POS_INF(num))
        return pstrdup("Inf");
    if (NUMERIC_IS_NEG_INF(num))
	{
        return pstrdup("-Inf");
	}


	/*
	 * Get the number in the variable format.
	 */
	init_var_from_num(num, &x);

	str = get_str_from_var(&x);

	return str;
}

/*
 * numeric_out() -
 *
 *	Output function for numeric data type
 *	Output suppres trailing zeroes in opentenbase_ora mode
 */
Datum
numeric_out(PG_FUNCTION_ARGS)
{
	char *str = numeric_out_internal(PG_GETARG_NUMERIC(0));

	if (ORA_MODE)
	{
		suppres_trailing_zeroes(str);

		suppres_leading_zeroes(str);
	}

	PG_RETURN_CSTRING(str);
}

/*
 * numeric_is_nan() -
 *
 *	Is Numeric value a NaN?
 */
bool
numeric_is_nan(Numeric num)
{
	return NUMERIC_IS_NAN(num);
}

/*
 * numeric_is_inf() -
 *
 *	Is Numeric value a Inf or -Inf?
 */
bool
numeric_is_inf(Numeric num)
{
    return NUMERIC_IS_INF(num);
}

bool
numeric_sign_equal(Numeric num1, Numeric num2)
{
    int sign1 = NUMERIC_SIGN(num1);
    int sign2 = NUMERIC_SIGN(num2);
    if (sign1 == NUMERIC_POS_INF)
        sign1 = NUMERIC_POS;
    else if (sign1 == NUMERIC_NEG_INF)
        sign1 = NUMERIC_NEG;

    if (sign2 == NUMERIC_POS_INF)
        sign2 = NUMERIC_POS;
    else if (sign2 == NUMERIC_NEG_INF)
        sign2 = NUMERIC_NEG;

    return sign1 == sign2;
}
/*
 * numeric_maximum_size() -
 *
 *	Maximum size of a numeric with given typmod, or -1 if unlimited/unknown.
 */
int32
numeric_maximum_size(int32 typmod)
{
	int			precision;
	int			numeric_digits;

	if (typmod < (int32) (VARHDRSZ))
		return -1;

	/* precision (ie, max # of digits) is in upper bits of typmod */
	precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;

#ifdef _PG_ORCL_
	if (precision > NUMERIC_MAX_PRECISION)
	{
		precision = FLOAT_OPENTENBASE_ORA_MAX_PRECISION;
	}
#endif

	/*
	 * This formula computes the maximum number of NumericDigits we could need
	 * in order to store the specified number of decimal digits. Because the
	 * weight is stored as a number of NumericDigits rather than a number of
	 * decimal digits, it's possible that the first NumericDigit will contain
	 * only a single decimal digit.  Thus, the first two decimal digits can
	 * require two NumericDigits to store, but it isn't until we reach
	 * DEC_DIGITS + 2 decimal digits that we potentially need a third
	 * NumericDigit.
	 */
	numeric_digits = (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS;

	/*
	 * In most cases, the size of a numeric will be smaller than the value
	 * computed below, because the varlena header will typically get toasted
	 * down to a single byte before being stored on disk, and it may also be
	 * possible to use a short numeric header.  But our job here is to compute
	 * the worst case.
	 */
	return NUMERIC_HDRSZ + (numeric_digits * sizeof(NumericDigit));
}

/*
 * numeric_out_sci() -
 *
 *	Output function for numeric data type in scientific notation.
 */
char *
numeric_out_sci(Numeric num, int scale)
{
	NumericVar	x;
	char	   *str;

    /*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		return pstrdup("NaN");

    /*
     * Handle Inf and -Inf
     */
    numeric_handle_inf_cstr(num);

	init_var_from_num(num, &x);
	str = get_str_from_var_sci(&x, scale);

	return str;
}

/*
 * suppres_trailing_zeroes() -
 *
 *	Suppressing insignificant trailing zeroes and then any trailing decimal point.
 */
void
suppres_trailing_zeroes(char *str)
{
	int last = 0;

	/* If there's no decimal point, there's certainly nothing to remove. */
	if (strchr(str, '.') == NULL)
	{
		return;
	}

	/* If the number is expressed using scientific notation, there is no need to
	* deal with subsequent 0's
	*/
	if (strchr(str, '.') != NULL && (strchr(str, 'e') != NULL || strchr(str, 'E') != NULL))
	{
		return;
	}

	/*
	 * Back up over trailing fractional zeroes.  Since there is a decimal
	 * point, this loop will terminate safely.
	 */
	last = strlen(str) - 1;
	while (str[last] == '0')
		last--;

	/* We want to get rid of the decimal point too, if it's now last. */
	if (str[last] == '.')
		last--;

	/* Delete whatever we backed up over. */
	str[last + 1] = '\0';
}

void
suppres_leading_zeroes(char *str)
{
	int begin = 0;
	int pos = 0;
	int zlen = 0;

	/* Has decimal point */
	if (strchr(str, '.') == NULL)
		return;

	/* Has sign bit */
	if ((str[0] == '+') || str[0] == '-')
		pos = 1;

	begin = pos;
	while (str[pos] == '0')
		pos++;

	zlen = pos - begin;
	if (zlen == 0)
		return;

	for (pos = begin; pos < strlen(str) - zlen; pos++)
		str[pos] = str[pos+zlen];

	str[pos] = '\0';
}

/*
 * numeric_normalize() -
 *
 *	Output function for numeric data type, suppressing insignificant trailing
 *	zeroes and then any trailing decimal point.  The intent of this is to
 *	produce strings that are equal if and only if the input numeric values
 *	compare equal.
 */
char *
numeric_normalize(Numeric num)
{
	NumericVar	x;
	char	   *str;

	/*
	 * Handle NaN, Inf and -Inf
	 */
	if (NUMERIC_IS_NAN(num))
		return pstrdup("NaN");
    numeric_handle_inf_cstr(num);

	init_var_from_num(num, &x);

	str = get_str_from_var(&x);

	suppres_trailing_zeroes(str);

	return str;
}

/*
 *		numeric_recv			- converts external binary format to numeric
 *
 * External format is a sequence of int16's:
 * ndigits, weight, sign, dscale, NumericDigits.
 */
Datum
numeric_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	NumericVar	value;
	Numeric		res;
	int			len,
				i;

	len = (uint16) pq_getmsgint(buf, sizeof(uint16));

	init_alloc_var(&value, len);

	value.weight = (int16) pq_getmsgint(buf, sizeof(int16));
	/* we allow any int16 for weight --- OK? */

	value.sign = (uint16) pq_getmsgint(buf, sizeof(uint16));
	if (!(value.sign == NUMERIC_POS ||
		  value.sign == NUMERIC_NEG ||
		  value.sign == NUMERIC_NAN ||
          value.sign == NUMERIC_POS_INF ||
          value.sign == NUMERIC_NEG_INF))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid sign in external \"numeric\" value")));

	value.dscale = (uint16) pq_getmsgint(buf, sizeof(uint16));
	if (ORA_MODE)
	{
		if ((int16)value.dscale < 0)
		{
			value.dscale = (int16)value.dscale;
		}

		if ((value.dscale < NUMERIC_OPENTENBASE_ORA_MIN_SCALE) ||
			(value.dscale >= 0 && (value.dscale & NUMERIC_DSCALE_MASK) != value.dscale))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid scale in external \"numeric\" value")));
		}
	}
	else if ((value.dscale & NUMERIC_DSCALE_MASK) != value.dscale)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid scale in external \"numeric\" value")));

	for (i = 0; i < len; i++)
	{
		NumericDigit d = pq_getmsgint(buf, sizeof(NumericDigit));

		if (d < 0 || d >= NBASE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("invalid digit in external \"numeric\" value")));
		value.digits[i] = d;
	}

	/*
	 * If the given dscale would hide any digits, truncate those digits away.
	 * We could alternatively throw an error, but that would take a bunch of
	 * extra code (about as much as trunc_var involves), and it might cause
	 * client compatibility issues.
	 */
	trunc_var(&value, value.dscale);

	apply_typmod(&value, typmod);

	res = make_result(&value);
	free_var(&value);

	PG_RETURN_NUMERIC(res);
}

/*
 *		numeric_send			- converts numeric to binary format
 */
Datum
numeric_send(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	StringInfoData buf;
	int			i;

	init_var_from_num(num, &x);

	pq_begintypsend(&buf);

	pq_sendint16(&buf, x.ndigits);
	pq_sendint16(&buf, x.weight);
	pq_sendint16(&buf, x.sign);
	pq_sendint16(&buf, x.dscale);
	for (i = 0; i < x.ndigits; i++)
		pq_sendint16(&buf, x.digits[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*
 * numeric_transform() -
 *
 * Flatten calls to numeric's length coercion function that solely represent
 * increases in allowable precision.  Scale changes mutate every datum, so
 * they are unoptimizable.  Some values, e.g. 1E-1001, can only fit into an
 * unconstrained numeric, so a change from an unconstrained numeric to any
 * constrained numeric is also unoptimizable.
 */
Datum
numeric_transform(PG_FUNCTION_ARGS)
{
	FuncExpr   *expr = castNode(FuncExpr, PG_GETARG_POINTER(0));
	Node	   *ret = NULL;
	Node	   *typmod;

	Assert(list_length(expr->args) >= 2);

	typmod = (Node *) lsecond(expr->args);

	if (IsA(typmod, Const) &&!((Const *) typmod)->constisnull)
	{
		Node	   *source = (Node *) linitial(expr->args);
		int32		old_typmod = exprTypmod(source);
		int32		new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
		int32		old_scale = (old_typmod - VARHDRSZ) & 0xffff;
		int32		new_scale = (new_typmod - VARHDRSZ) & 0xffff;
		int32		old_precision = (old_typmod - VARHDRSZ) >> 16 & 0xffff;
		int32		new_precision = (new_typmod - VARHDRSZ) >> 16 & 0xffff;

		/*
		 * If new_typmod < VARHDRSZ, the destination is unconstrained; that's
		 * always OK.  If old_typmod >= VARHDRSZ, the source is constrained,
		 * and we're OK if the scale is unchanged and the precision is not
		 * decreasing.  See further notes in function header comment.
		 */
		if (new_typmod < (int32) VARHDRSZ ||
			(old_typmod >= (int32) VARHDRSZ &&
			 new_scale == old_scale && new_precision >= old_precision))
			ret = relabel_to_typmod(source, new_typmod);
	}

	PG_RETURN_POINTER(ret);
}

/*
 * numeric() -
 *
 *	This is a special function called by the Postgres database system
 *	before a value is stored in a tuple's attribute. The precision and
 *	scale of the attribute have to be applied on the value.
 */
Datum
numeric		(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		typmod = PG_GETARG_INT32(1);
	Numeric		new;
	int32		tmp_typmod;
	int			precision;
	int			scale;
	int			ddigits;
	int			maxdigits;
	NumericVar	var;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));
    /*
     * Handle Inf and -Inf
     */
    numeric_handle_inf_datum(num);
	/*
	 * If the value isn't a valid type modifier, simply return a copy of the
	 * input value
	 */
	if (typmod < (int32) (VARHDRSZ))
	{
		new = (Numeric) palloc(VARSIZE(num));
		memcpy(new, num, VARSIZE(num));
		PG_RETURN_NUMERIC(new);
	}

	/*
	 * Get the precision and scale out of the typmod value
	 */
	tmp_typmod = typmod - VARHDRSZ;
	precision = (tmp_typmod >> 16) & 0xffff;
	scale = tmp_typmod & 0xffff;
	if (ORA_MODE)
	{
		if (precision > NUMERIC_MAX_PRECISION && scale == 0)
        {
            precision = precision - NUMERIC_MAX_PRECISION;
            if (precision == OPENTENBASE_ORA_BINARY_FLOAT_PRECISION || precision == OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION)
                precision = precision - NUMERIC_MAX_PRECISION;
        }
		else if (precision > NUMERIC_OPENTENBASE_ORA_MAX_PRECISION)
			precision = NUMERIC_OPENTENBASE_ORA_MAX_PRECISION;
	}
	
	maxdigits = precision - scale;

	/*
	 * If the number is certainly in bounds and due to the target scale no
	 * rounding could be necessary, just make a copy of the input and modify
	 * its scale fields, unless the larger scale forces us to abandon the
	 * short representation.  (Note we assume the existing dscale is
	 * honest...)
	 * Need round When the scale is greater than 1000 (indicating negative
	 * numbers in opentenbase_ora mode).
	 */
	ddigits = (NUMERIC_WEIGHT(num) + 1) * DEC_DIGITS;
	if ((ddigits <= maxdigits) && scale >= NUMERIC_DSCALE(num) &&
	    (NUMERIC_CAN_BE_SHORT(scale, NUMERIC_WEIGHT(num)) || !NUMERIC_IS_SHORT(num)))
	{
		new = (Numeric) palloc(VARSIZE(num));
		memcpy(new, num, VARSIZE(num));
		if (NUMERIC_IS_SHORT(num))
			new->choice.n_short.n_header =
				(num->choice.n_short.n_header & ~NUMERIC_SHORT_DSCALE_MASK)
				| (scale << NUMERIC_SHORT_DSCALE_SHIFT);
		else
			new->choice.n_long.n_sign_dscale = NUMERIC_SIGN(new) |
				((uint16) scale & NUMERIC_DSCALE_MASK);
		PG_RETURN_NUMERIC(new);
	}

	/*
	 * We really need to fiddle with things - unpack the number into a
	 * variable and let apply_typmod() do it.
	 */
	init_var(&var);

	set_var_from_num(num, &var);
	apply_typmod(&var, typmod);
	new = make_result(&var);

	free_var(&var);

	PG_RETURN_NUMERIC(new);
}

Datum
numerictypmodin(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int32	   *tl;
	int			n;
	int32		typmod;
	int32		tl0;
	int32		tl1;

	tl = ArrayGetIntegerTypmods(ta, &n);
	tl0 = tl[0];
	tl1 = tl[1];

	if (tl[0] < 0 && ORA_MODE)
	{
		if ((n == 3 && tl[1] == FLOAT_OPENTENBASE_ORA_MAX_RANGE * (-1) && tl[2] == FLOAT_OPENTENBASE_ORA_MAX_RANGE * (-1))
		|| (n == 2 && tl[0] == OPENTENBASE_ORA_BINARY_FLOAT_PRECISION * (-1) && tl[1] == 0)
		|| (n == 2 && tl[0] == OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION * (-1) && tl[1] == 0))
		{
			n = 1;
		}
	}

	if (n == 2)
	{
		if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
		{
			if (ORA_MODE)
			{
				if (tl[0] < FLOAT_OPENTENBASE_ORA_MAX_RANGE * (-1))
					ereport(ERROR,
					        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					         errmsg("NUMERIC precision %d for type float must be less than 126",
					                tl[0])));
				else if (tl[0] < 0)
					tl0 = tl[0] * (-1) + NUMERIC_MAX_PRECISION;
				else if (tl[0] > NUMERIC_MAX_PRECISION)
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					                errmsg("NUMERIC precision %d must be between 1 and %d", tl[0],
					                       NUMERIC_MAX_PRECISION)));
			}
			else
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				                errmsg("NUMERIC precision %d must be between 1 and %d", tl[0],
				                       NUMERIC_MAX_PRECISION)));
		}
		if (tl[1] < 0 || tl[1] > tl[0])
		{
#ifdef _PG_ORCL_
			if (ORA_MODE)
			{
				/*
				 * opentenbase_ora scale range is between -84 and 127
				 */
				if (tl[1] < NUMERIC_OPENTENBASE_ORA_MIN_SCALE)
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("NUMERIC scale %d must be between %d and precision %d",
									tl[1], NUMERIC_OPENTENBASE_ORA_MIN_SCALE, NUMERIC_OPENTENBASE_ORA_MAX_SCALE)));
				}
				else if (tl[1] < 0)
				{
					/*
					 * If the scale is below 0, use formula below to indicate scale
					 * 	scale = scale*(-1) + NUMERIC_MAX_PRECISION
					 */
					tl1 = tl[1] * (-1) + NUMERIC_MAX_PRECISION;
				}
				else if (tl[1] > NUMERIC_OPENTENBASE_ORA_MAX_SCALE)
				{
					/*
					* Allow scale is bigger than precision if scale is smaller
					* than NUMERIC_OPENTENBASE_ORA_MAX_SCALE.
					*/
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("NUMERIC precision %d must be between 0 and precision %d",
									tl[0], NUMERIC_OPENTENBASE_ORA_MAX_SCALE)));
				}
			}
			else
			{
#endif
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("NUMERIC scale %d must be between 0 and precision %d",
								tl[1], tl[0])));
#ifdef _PG_ORCL_
			}
#endif
		}
		typmod = ((tl[0] << 16) | tl1) + VARHDRSZ;
	}
	else if (n == 1)
	{
		tl1 = 0;

		if (tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION)
		{
#ifdef _PG_ORCL_
			if (ORA_MODE)
			{
				if (tl[0] < 0)
				{
					tl0 = tl[0]*(-1) + NUMERIC_MAX_PRECISION;
				}
			}
			else
#endif
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("NUMERIC precision %d must be between 1 and %d",
							tl[0], NUMERIC_MAX_PRECISION)));
		}
		/* scale defaults to zero */
		typmod = ((tl0 << 16) | tl1) + VARHDRSZ;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NUMERIC type modifier")));
		typmod = 0;				/* keep compiler quiet */
	}

	PG_RETURN_INT32(typmod);
}

Datum
numerictypmodout(PG_FUNCTION_ARGS)
{
	int32		typmod = PG_GETARG_INT32(0);
	char	   *res = (char *) palloc(64);
	int32		precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
	int32		scale = (typmod - VARHDRSZ) & 0xffff;

#ifdef _PG_ORCL_
	if (precision > NUMERIC_MAX_PRECISION)
	{
		precision = precision - NUMERIC_MAX_PRECISION;
		precision = (precision) * (-1);
	}

	if (scale > NUMERIC_MAX_PRECISION)
	{
		scale = (scale - NUMERIC_MAX_PRECISION) * (-1);
	}
#endif

	if (typmod >= 0)
		snprintf(res, 64, "(%d,%d)", precision, scale);
	else
		*res = '\0';

	PG_RETURN_CSTRING(res);
}


/* ----------------------------------------------------------------------
 *
 * Sign manipulation, rounding and the like
 *
 * ----------------------------------------------------------------------
 */

Datum
numeric_abs(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;

	/*
     * Handle NaN, Inf and -Inf
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));
    if (NUMERIC_IS_INF(num))
        PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf));

	/*
	 * Do it the easy way directly on the packed format
	 */
	res = (Numeric) palloc(VARSIZE(num));
	memcpy(res, num, VARSIZE(num));

	if (NUMERIC_IS_SHORT(res))
		res->choice.n_short.n_header =
			res->choice.n_short.n_header & ~NUMERIC_SHORT_SIGN_MASK;
	else
		res->choice.n_long.n_sign_dscale = NUMERIC_POS | NUMERIC_DSCALE(res);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_uminus(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;

	/*
	 * Handle NaN, Inf and -Inf
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));
    if (NUMERIC_IS_POS_INF(num))
        PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_neg_inf));
    if (NUMERIC_IS_NEG_INF(num))
        PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf));

	/*
	 * Do it the easy way directly on the packed format
	 */
	res = (Numeric) palloc(VARSIZE(num));
	memcpy(res, num, VARSIZE(num));

	/*
	 * The packed format is known to be totally zero digit trimmed always. So
	 * we can identify a ZERO by the fact that there are no digits at all.  Do
	 * nothing to a zero.
	 */
	if (NUMERIC_NDIGITS(res) != 0)
	{
		/* Else, flip the sign */
		if (NUMERIC_IS_SHORT(res))
			res->choice.n_short.n_header =
				res->choice.n_short.n_header ^ NUMERIC_SHORT_SIGN_MASK;
		else if (NUMERIC_SIGN(res) == NUMERIC_POS)
			res->choice.n_long.n_sign_dscale =
				NUMERIC_NEG | NUMERIC_DSCALE(res);
		else
			res->choice.n_long.n_sign_dscale =
				NUMERIC_POS | NUMERIC_DSCALE(res);
	}

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_uplus(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;

	res = (Numeric) palloc(VARSIZE(num));
	memcpy(res, num, VARSIZE(num));

	PG_RETURN_NUMERIC(res);
}

/*
 * numeric_sign() -
 *
 * returns -1 if the argument is less than 0, 0 if the argument is equal
 * to 0, and 1 if the argument is greater than zero.
 */
Datum
numeric_sign(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
	{
		/* opentenbase_ora function bug: return 1 when the value is nan  */
		if (ORA_MODE)
			PG_RETURN_NUMERIC(make_result(&const_one));
		else
			PG_RETURN_NUMERIC(make_result(&const_nan));
	}

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_POS_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_one));
	if (NUMERIC_IS_NEG_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_neg_one));
	init_var(&result);

	/*
	 * The packed format is known to be totally zero digit trimmed always. So
	 * we can identify a ZERO by the fact that there are no digits at all.
	 */
	if (NUMERIC_NDIGITS(num) == 0)
		set_var_from_var(&const_zero, &result);
	else
	{
		/*
		 * And if there are some, we return a copy of ONE with the sign of our
		 * argument
		 */
		set_var_from_var(&const_one, &result);
		result.sign = NUMERIC_SIGN(num);
	}

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_round() -
 *
 *	Round a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying rounding before the decimal
 *	point --- opentenbase_ora interprets rounding that way.
 */
Datum
numeric_round(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		scale = PG_GETARG_INT32(1);
	Numeric		res;
	NumericVar	arg;

    /*
    * Handle Inf and -Inf
    */
    numeric_handle_inf_datum(num);

	/*
	 * Limit the scale value to avoid possible overflow in calculations
	 */
	scale = Max(scale, -NUMERIC_MAX_RESULT_SCALE);
	scale = Min(scale, NUMERIC_MAX_RESULT_SCALE);

	/*
	 * Unpack the argument and round it at the proper digit position
	 */
	init_var(&arg);
	set_var_from_num(num, &arg);

	round_var(&arg, scale, HALF_ADJUST_ROUND);

	/* We don't allow negative output dscale */
	if (scale < 0)
		arg.dscale = 0;

	/*
	 * Return the rounded result
	 */
	res = make_result(&arg);

	free_var(&arg);
	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_trunc() -
 *
 *	Truncate a value to have 'scale' digits after the decimal point.
 *	We allow negative 'scale', implying a truncation before the decimal
 *	point --- opentenbase_ora interprets truncation that way.
 */
Datum
numeric_trunc(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	int32		scale = PG_GETARG_INT32(1);
	Numeric		res;
	NumericVar	arg;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));
    /*
    * Handle Inf and -Inf
    */
    numeric_handle_inf_datum(num);
	/*
	 * Limit the scale value to avoid possible overflow in calculations
	 */
	scale = Max(scale, -NUMERIC_MAX_RESULT_SCALE);
	scale = Min(scale, NUMERIC_MAX_RESULT_SCALE);

	/*
	 * Unpack the argument and truncate it at the proper digit position
	 */
	init_var(&arg);
	set_var_from_num(num, &arg);

	trunc_var(&arg, scale);

	/* We don't allow negative output dscale */
	if (scale < 0)
		arg.dscale = 0;

	/*
	 * Return the truncated result
	 */
	res = make_result(&arg);

	free_var(&arg);
	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_ceil() -
 *
 *	Return the smallest integer greater than or equal to the argument
 */
Datum
numeric_ceil(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

    /*
    * Handle Inf and -Inf
    */
    numeric_handle_inf_datum(num);
	init_var_from_num(num, &result);
	ceil_var(&result, &result);

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_floor() -
 *
 *	Return the largest integer equal to or less than the argument
 */
Datum
numeric_floor(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

    /*
    * Handle Inf and -Inf
    */
    numeric_handle_inf_datum(num);
	init_var_from_num(num, &result);
	floor_var(&result, &result);

	res = make_result(&result);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * generate_series_numeric() -
 *
 *	Generate series of numeric.
 */
Datum
generate_series_numeric(PG_FUNCTION_ARGS)
{
	return generate_series_step_numeric(fcinfo);
}

Datum
generate_series_step_numeric(PG_FUNCTION_ARGS)
{
	generate_series_numeric_fctx *fctx;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;

	if (SRF_IS_FIRSTCALL())
	{
		Numeric		start_num = PG_GETARG_NUMERIC(0);
		Numeric		stop_num = PG_GETARG_NUMERIC(1);
		NumericVar	steploc = const_one;

		/* handle NaN, Inf and -Inf in start and stop values */
		if (NUMERIC_IS_NAN(start_num))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("start value cannot be NaN")));
        if (NUMERIC_IS_POS_INF(start_num))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("start value cannot be Inf")));
        if (NUMERIC_IS_NEG_INF(start_num))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("start value cannot be -Inf")));
		if (NUMERIC_IS_NAN(stop_num))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("stop value cannot be NaN")));
        if (NUMERIC_IS_POS_INF(stop_num))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("stop value cannot be Inf")));
        if (NUMERIC_IS_NEG_INF(stop_num))
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("stop value cannot be -Inf")));

		/* see if we were given an explicit step size */
		if (PG_NARGS() == 3)
		{
			Numeric		step_num = PG_GETARG_NUMERIC(2);

			if (NUMERIC_IS_NAN(step_num))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("step size cannot be NaN")));
            if (NUMERIC_IS_POS_INF(step_num))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("step size cannot be Inf")));
            if (NUMERIC_IS_NEG_INF(step_num))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("step size cannot be -Inf")));

			init_var_from_num(step_num, &steploc);

			if (cmp_var(&steploc, &const_zero) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("step size cannot equal zero")));
		}

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * Switch to memory context appropriate for multiple function calls.
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* allocate memory for user context */
		fctx = (generate_series_numeric_fctx *)
			palloc(sizeof(generate_series_numeric_fctx));

		/*
		 * Use fctx to keep state from call to call. Seed current with the
		 * original start value. We must copy the start_num and stop_num
		 * values rather than pointing to them, since we may have detoasted
		 * them in the per-call context.
		 */
		init_var(&fctx->current);
		init_var(&fctx->stop);
		init_var(&fctx->step);

		set_var_from_num(start_num, &fctx->current);
		set_var_from_num(stop_num, &fctx->stop);
		set_var_from_var(&steploc, &fctx->step);

		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	/*
	 * Get the saved state and use current state as the result of this
	 * iteration.
	 */
	fctx = funcctx->user_fctx;

	if ((fctx->step.sign == NUMERIC_POS &&
		 cmp_var(&fctx->current, &fctx->stop) <= 0) ||
		(fctx->step.sign == NUMERIC_NEG &&
		 cmp_var(&fctx->current, &fctx->stop) >= 0))
	{
		Numeric		result = make_result(&fctx->current);

		/* switch to memory context appropriate for iteration calculation */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* increment current in preparation for next iteration */
		add_var(&fctx->current, &fctx->step, &fctx->current);
		MemoryContextSwitchTo(oldcontext);

		/* do when there is more left to send */
		SRF_RETURN_NEXT(funcctx, NumericGetDatum(result));
	}
	else
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
}


/*
 * Implements the numeric version of the width_bucket() function
 * defined by SQL2003. See also width_bucket_float8().
 *
 * 'bound1' and 'bound2' are the lower and upper bounds of the
 * histogram's range, respectively. 'count' is the number of buckets
 * in the histogram. width_bucket() returns an integer indicating the
 * bucket number that 'operand' belongs to in an equiwidth histogram
 * with the specified characteristics. An operand smaller than the
 * lower bound is assigned to bucket 0. An operand greater than the
 * upper bound is assigned to an additional bucket (with number
 * count+1). We don't allow "NaN" for any of the numeric arguments.
 */
Datum
width_bucket_numeric(PG_FUNCTION_ARGS)
{
	Numeric		operand = PG_GETARG_NUMERIC(0);
	Numeric		bound1 = PG_GETARG_NUMERIC(1);
	Numeric		bound2 = PG_GETARG_NUMERIC(2);
	int32		count = PG_GETARG_INT32(3);
	NumericVar	count_var;
	NumericVar	result_var;
	int32		result;

	if (count <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("count must be greater than zero")));

	if (NUMERIC_IS_NAN(operand) ||
		NUMERIC_IS_NAN(bound1) ||
		NUMERIC_IS_NAN(bound2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
				 errmsg("operand, lower bound, and upper bound cannot be NaN")));

    /*
     * operand can be Inf or -Inf, but lower bound, and upper bound cannot be Inf or -Inf.
     * If we lift the restrictions of lower bound and upper bound, the subsequent compute_bucket function
     * will perform division operation on Inf or -Inf, resulting in division by zero error.
     */
    if (NUMERIC_IS_INF(bound1) ||
        NUMERIC_IS_INF(bound2))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
                        errmsg("lower bound, and upper bound cannot be Inf or -Inf")));

	quick_init_var(&result_var);
	quick_init_var(&count_var);

	/* Convert 'count' to a numeric, for ease of use later */
	int64_to_numericvar((int64) count, &count_var);

	switch (cmp_numerics(bound1, bound2))
	{
		case 0:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
					 errmsg("lower bound cannot equal upper bound")));

			/* bound1 < bound2 */
		case -1:
			if (cmp_numerics(operand, bound1) < 0)
				set_var_from_var(&const_zero, &result_var);
			else if (cmp_numerics(operand, bound2) >= 0)
				add_var(&count_var, &const_one, &result_var);
			else
				compute_bucket(operand, bound1, bound2,
							   &count_var, &result_var);
			break;

			/* bound1 > bound2 */
		case 1:
			if (cmp_numerics(operand, bound1) > 0)
				set_var_from_var(&const_zero, &result_var);
			else if (cmp_numerics(operand, bound2) <= 0)
				add_var(&count_var, &const_one, &result_var);
			else
				compute_bucket(operand, bound1, bound2,
							   &count_var, &result_var);
			break;
	}

	/* if result exceeds the range of a legal int4, we ereport here */
	if (!numericvar_to_int32(&result_var, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("integer out of range")));

	free_var(&count_var);
	free_var(&result_var);

	PG_RETURN_INT32(result);
}

/*
 * If 'operand' is not outside the bucket range, determine the correct
 * bucket for it to go. The calculations performed by this function
 * are derived directly from the SQL2003 spec.
 */
static void
compute_bucket(Numeric operand, Numeric bound1, Numeric bound2,
			   const NumericVar *count_var, NumericVar *result_var)
{
	NumericVar	bound1_var;
	NumericVar	bound2_var;
	NumericVar	operand_var;

	init_var_from_num(bound1, &bound1_var);
	init_var_from_num(bound2, &bound2_var);
	init_var_from_num(operand, &operand_var);

	if (cmp_var(&bound1_var, &bound2_var) < 0)
	{
		sub_var(&operand_var, &bound1_var, &operand_var);
		sub_var(&bound2_var, &bound1_var, &bound2_var);
		div_var(&operand_var, &bound2_var, result_var,
				select_div_scale(&operand_var, &bound2_var), HALF_ADJUST_ROUND);
	}
	else
	{
		sub_var(&bound1_var, &operand_var, &operand_var);
		sub_var(&bound1_var, &bound2_var, &bound1_var);
		div_var(&operand_var, &bound1_var, result_var,
				select_div_scale(&operand_var, &bound1_var), HALF_ADJUST_ROUND);
	}

	mul_var(result_var, count_var, result_var,
			result_var->dscale + count_var->dscale);
	add_var(result_var, &const_one, result_var);
	floor_var(result_var, result_var);

	free_var(&bound1_var);
	free_var(&bound2_var);
	free_var(&operand_var);
}

/* ----------------------------------------------------------------------
 *
 * Comparison functions
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *
 * Sort support:
 *
 * We implement the sortsupport strategy routine in order to get the benefit of
 * abbreviation. The ordinary numeric comparison can be quite slow as a result
 * of palloc/pfree cycles (due to detoasting packed values for alignment);
 * while this could be worked on itself, the abbreviation strategy gives more
 * speedup in many common cases.
 *
 * Two different representations are used for the abbreviated form, one in
 * int32 and one in int64, whichever fits into a by-value Datum.  In both cases
 * the representation is negated relative to the original value, because we use
 * the largest negative value for NaN, which sorts higher than other values. We
 * convert the absolute value of the numeric to a 31-bit or 63-bit positive
 * value, and then negate it if the original number was positive.
 *
 * We abort the abbreviation process if the abbreviation cardinality is below
 * 0.01% of the row count (1 per 10k non-null rows).  The actual break-even
 * point is somewhat below that, perhaps 1 per 30k (at 1 per 100k there's a
 * very small penalty), but we don't want to build up too many abbreviated
 * values before first testing for abort, so we take the slightly pessimistic
 * number.  We make no attempt to estimate the cardinality of the real values,
 * since it plays no part in the cost model here (if the abbreviation is equal,
 * the cost of comparing equal and unequal underlying values is comparable).
 * We discontinue even checking for abort (saving us the hashing overhead) if
 * the estimated cardinality gets to 100k; that would be enough to support many
 * billions of rows while doing no worse than breaking even.
 *
 * ----------------------------------------------------------------------
 */

/*
 * Sort support strategy routine.
 */
Datum
numeric_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = numeric_fast_cmp;

	if (ssup->abbreviate)
	{
		NumericSortSupport *nss;
		MemoryContext oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

		nss = palloc(sizeof(NumericSortSupport));

		/*
		 * palloc a buffer for handling unaligned packed values in addition to
		 * the support struct
		 */
		nss->buf = palloc(VARATT_SHORT_MAX + VARHDRSZ + 1);

		nss->input_count = 0;
		nss->estimating = true;
		initHyperLogLog(&nss->abbr_card, 10);

		ssup->ssup_extra = nss;

		ssup->abbrev_full_comparator = ssup->comparator;
		ssup->comparator = numeric_cmp_abbrev;
		ssup->abbrev_converter = numeric_abbrev_convert;
		ssup->abbrev_abort = numeric_abbrev_abort;

		MemoryContextSwitchTo(oldcontext);
	}

	PG_RETURN_VOID();
}

/*
 * Abbreviate a numeric datum, handling NaNs and detoasting
 * (must not leak memory!)
 */
static Datum
numeric_abbrev_convert(Datum original_datum, SortSupport ssup)
{
	NumericSortSupport *nss = ssup->ssup_extra;
	void	   *original_varatt = PG_DETOAST_DATUM_PACKED(original_datum);
	Numeric		value;
	Datum		result;

	nss->input_count += 1;

	/*
	 * This is to handle packed datums without needing a palloc/pfree cycle;
	 * we keep and reuse a buffer large enough to handle any short datum.
	 */
	if (VARATT_IS_SHORT(original_varatt))
	{
		void	   *buf = nss->buf;
		Size		sz = VARSIZE_SHORT(original_varatt) - VARHDRSZ_SHORT;

		Assert(sz <= VARATT_SHORT_MAX - VARHDRSZ_SHORT);

		SET_VARSIZE(buf, VARHDRSZ + sz);
		memcpy(VARDATA(buf), VARDATA_SHORT(original_varatt), sz);

		value = (Numeric) buf;
	}
	else
		value = (Numeric) original_varatt;

	if (NUMERIC_IS_NAN(value))
	{
		result = NUMERIC_ABBREV_NAN;
	}
    else if (NUMERIC_IS_POS_INF(value))
    {
        result = NUMERIC_ABBREV_POS_INF;
    }
    else if (NUMERIC_IS_NEG_INF(value))
    {
        result = NUMERIC_ABBREV_NEG_INF;
    }
	else
	{
		NumericVar	var;

		init_var_from_num(value, &var);


		result = numeric_abbrev_convert_var(&var, nss);
	}

	/* should happen only for external/compressed toasts */
	if ((Pointer) original_varatt != DatumGetPointer(original_datum))
		pfree(original_varatt);

	return result;
}

/*
 * Consider whether to abort abbreviation.
 *
 * We pay no attention to the cardinality of the non-abbreviated data. There is
 * no reason to do so: unlike text, we have no fast check for equal values, so
 * we pay the full overhead whenever the abbreviations are equal regardless of
 * whether the underlying values are also equal.
 */
static bool
numeric_abbrev_abort(int memtupcount, SortSupport ssup)
{
	NumericSortSupport *nss = ssup->ssup_extra;
	double		abbr_card;

	if (memtupcount < 10000 || nss->input_count < 10000 || !nss->estimating)
		return false;

	abbr_card = estimateHyperLogLog(&nss->abbr_card);

	/*
	 * If we have >100k distinct values, then even if we were sorting many
	 * billion rows we'd likely still break even, and the penalty of undoing
	 * that many rows of abbrevs would probably not be worth it. Stop even
	 * counting at that point.
	 */
	if (abbr_card > 100000.0)
	{
#ifdef TRACE_SORT
		if (trace_sort)
			elog(LOG,
				 "numeric_abbrev: estimation ends at cardinality %f"
				 " after " INT64_FORMAT " values (%d rows)",
				 abbr_card, nss->input_count, memtupcount);
#endif
		nss->estimating = false;
		return false;
	}

	/*
	 * Target minimum cardinality is 1 per ~10k of non-null inputs.  (The
	 * break even point is somewhere between one per 100k rows, where
	 * abbreviation has a very slight penalty, and 1 per 10k where it wins by
	 * a measurable percentage.)  We use the relatively pessimistic 10k
	 * threshold, and add a 0.5 row fudge factor, because it allows us to
	 * abort earlier on genuinely pathological data where we've had exactly
	 * one abbreviated value in the first 10k (non-null) rows.
	 */
	if (abbr_card < nss->input_count / 10000.0 + 0.5)
	{
#ifdef TRACE_SORT
		if (trace_sort)
			elog(LOG,
				 "numeric_abbrev: aborting abbreviation at cardinality %f"
				 " below threshold %f after " INT64_FORMAT " values (%d rows)",
				 abbr_card, nss->input_count / 10000.0 + 0.5,
				 nss->input_count, memtupcount);
#endif
		return true;
	}

#ifdef TRACE_SORT
	if (trace_sort)
		elog(LOG,
			 "numeric_abbrev: cardinality %f"
			 " after " INT64_FORMAT " values (%d rows)",
			 abbr_card, nss->input_count, memtupcount);
#endif

	return false;
}

/*
 * Non-fmgr interface to the comparison routine to allow sortsupport to elide
 * the fmgr call.  The saving here is small given how slow numeric comparisons
 * are, but it is a required part of the sort support API when abbreviations
 * are performed.
 *
 * Two palloc/pfree cycles could be saved here by using persistent buffers for
 * aligning short-varlena inputs, but this has not so far been considered to
 * be worth the effort.
 */
static int
numeric_fast_cmp(Datum x, Datum y, SortSupport ssup)
{
	Numeric		nx = DatumGetNumeric(x);
	Numeric		ny = DatumGetNumeric(y);
	int			result;

	result = cmp_numerics(nx, ny);

	if ((Pointer) nx != DatumGetPointer(x))
		pfree(nx);
	if ((Pointer) ny != DatumGetPointer(y))
		pfree(ny);

	return result;
}

/*
 * Compare abbreviations of values. (Abbreviations may be equal where the true
 * values differ, but if the abbreviations differ, they must reflect the
 * ordering of the true values.)
 */
static int
numeric_cmp_abbrev(Datum x, Datum y, SortSupport ssup)
{
	/*
	 * NOTE WELL: this is intentionally backwards, because the abbreviation is
	 * negated relative to the original value, to handle NaN.
	 */
	if (DatumGetNumericAbbrev(x) < DatumGetNumericAbbrev(y))
		return 1;
	if (DatumGetNumericAbbrev(x) > DatumGetNumericAbbrev(y))
		return -1;
	return 0;
}

/*
 * Abbreviate a NumericVar according to the available bit size.
 *
 * The 31-bit value is constructed as:
 *
 *	0 + 7bits digit weight + 24 bits digit value
 *
 * where the digit weight is in single decimal digits, not digit words, and
 * stored in excess-44 representation[1]. The 24-bit digit value is the 7 most
 * significant decimal digits of the value converted to binary. Values whose
 * weights would fall outside the representable range are rounded off to zero
 * (which is also used to represent actual zeros) or to 0x7FFFFFFF (which
 * otherwise cannot occur). Abbreviation therefore fails to gain any advantage
 * where values are outside the range 10^-44 to 10^83, which is not considered
 * to be a serious limitation, or when values are of the same magnitude and
 * equal in the first 7 decimal digits, which is considered to be an
 * unavoidable limitation given the available bits. (Stealing three more bits
 * to compare another digit would narrow the range of representable weights by
 * a factor of 8, which starts to look like a real limiting factor.)
 *
 * (The value 44 for the excess is essentially arbitrary)
 *
 * The 63-bit value is constructed as:
 *
 *	0 + 7bits weight + 4 x 14-bit packed digit words
 *
 * The weight in this case is again stored in excess-44, but this time it is
 * the original weight in digit words (i.e. powers of 10000). The first four
 * digit words of the value (if present; trailing zeros are assumed as needed)
 * are packed into 14 bits each to form the rest of the value. Again,
 * out-of-range values are rounded off to 0 or 0x7FFFFFFFFFFFFFFF. The
 * representable range in this case is 10^-176 to 10^332, which is considered
 * to be good enough for all practical purposes, and comparison of 4 words
 * means that at least 13 decimal digits are compared, which is considered to
 * be a reasonable compromise between effectiveness and efficiency in computing
 * the abbreviation.
 *
 * (The value 44 for the excess is even more arbitrary here, it was chosen just
 * to match the value used in the 31-bit case)
 *
 * [1] - Excess-k representation means that the value is offset by adding 'k'
 * and then treated as unsigned, so the smallest representable value is stored
 * with all bits zero. This allows simple comparisons to work on the composite
 * value.
 */

#if NUMERIC_ABBREV_BITS == 64

static Datum
numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss)
{
	int			ndigits = var->ndigits;
	int			weight = var->weight;
	int64		result;

	if (ndigits == 0 || weight < -44)
	{
		result = 0;
	}
	else if (weight > 83)
	{
		result = PG_INT64_MAX;
	}
	else
	{
		result = ((int64) (weight + 44) << 56);

		switch (ndigits)
		{
			default:
				result |= ((int64) var->digits[3]);
				/* FALLTHROUGH */
			case 3:
				result |= ((int64) var->digits[2]) << 14;
				/* FALLTHROUGH */
			case 2:
				result |= ((int64) var->digits[1]) << 28;
				/* FALLTHROUGH */
			case 1:
				result |= ((int64) var->digits[0]) << 42;
				break;
		}
	}

	/* the abbrev is negated relative to the original */
	if (var->sign == NUMERIC_POS)
		result = -result;

	if (nss->estimating)
	{
		uint32		tmp = ((uint32) result
						   ^ (uint32) ((uint64) result >> 32));

		addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
	}

	return NumericAbbrevGetDatum(result);
}

#endif							/* NUMERIC_ABBREV_BITS == 64 */

#if NUMERIC_ABBREV_BITS == 32

static Datum
numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss)
{
	int			ndigits = var->ndigits;
	int			weight = var->weight;
	int32		result;

	if (ndigits == 0 || weight < -11)
	{
		result = 0;
	}
	else if (weight > 20)
	{
		result = PG_INT32_MAX;
	}
	else
	{
		NumericDigit nxt1 = (ndigits > 1) ? var->digits[1] : 0;

		weight = (weight + 11) * 4;

		result = var->digits[0];

		/*
		 * "result" now has 1 to 4 nonzero decimal digits. We pack in more
		 * digits to make 7 in total (largest we can fit in 24 bits)
		 */

		if (result > 999)
		{
			/* already have 4 digits, add 3 more */
			result = (result * 1000) + (nxt1 / 10);
			weight += 3;
		}
		else if (result > 99)
		{
			/* already have 3 digits, add 4 more */
			result = (result * 10000) + nxt1;
			weight += 2;
		}
		else if (result > 9)
		{
			NumericDigit nxt2 = (ndigits > 2) ? var->digits[2] : 0;

			/* already have 2 digits, add 5 more */
			result = (result * 100000) + (nxt1 * 10) + (nxt2 / 1000);
			weight += 1;
		}
		else
		{
			NumericDigit nxt2 = (ndigits > 2) ? var->digits[2] : 0;

			/* already have 1 digit, add 6 more */
			result = (result * 1000000) + (nxt1 * 100) + (nxt2 / 100);
		}

		result = result | (weight << 24);
	}

	/* the abbrev is negated relative to the original */
	if (var->sign == NUMERIC_POS)
		result = -result;

	if (nss->estimating)
	{
		uint32		tmp = (uint32) result;

		addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
	}

	return NumericAbbrevGetDatum(result);
}

#endif							/* NUMERIC_ABBREV_BITS == 32 */

/*
 * Ordinary (non-sortsupport) comparisons follow.
 */

Datum
numeric_cmp(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	int			result;

	result = cmp_numerics(num1, num2);

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_INT32(result);
}


Datum
numeric_eq(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) == 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

Datum
numeric_ne(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) != 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

Datum
numeric_gt(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) > 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

Datum
numeric_ge(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) >= 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

Datum
numeric_lt(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) < 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

Datum
numeric_lt_zero(PG_FUNCTION_ARGS)
{
	Numeric	num1 = PG_GETARG_NUMERIC(0);
	Numeric	zero = make_result(&const_zero);
	return DirectFunctionCall2(numeric_lt, NumericGetDatum(num1), NumericGetDatum(zero));
}

Datum
numeric_le(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	bool		result;

	result = cmp_numerics(num1, num2) <= 0;

	PG_FREE_IF_COPY(num1, 0);
	PG_FREE_IF_COPY(num2, 1);

	PG_RETURN_BOOL(result);
}

int
cmp_numerics(Numeric num1, Numeric num2)
{
	int			result;

#ifdef __OPENTENBASE_C__
	/* compare numeric data, convert big integer to numeric */
	Numeric leftarg = NUMERIC_IS_BI(num1) ? makeNumericNormal(num1) : num1;
	Numeric rightarg = NUMERIC_IS_BI(num2) ? makeNumericNormal(num2) : num2;
#endif
	uint16 num1_flag_bits = NUMERIC_FLAGBITS(num1);
	uint16 num2_flag_bits = NUMERIC_FLAGBITS(num2);
	if (num1_flag_bits == NUMERIC_SIGN_MASK || num2_flag_bits == NUMERIC_SIGN_MASK)
	{
		/*
	 	* We consider all NANs to be equal and larger than any non-NAN. This is
	 	* somewhat arbitrary; the important thing is to have a consistent sort
	 	* order.
	 	*/
		if (NUMERIC_IS_NAN(leftarg))
		{
			if (NUMERIC_IS_NAN(rightarg))
				result = 0;			/* NAN = NAN */
			else
				result = 1;			/* NAN > non-NAN */
		}
		else if (NUMERIC_IS_NAN(rightarg))
		{
			result = -1;			/* non-NAN < NAN */
		}
		numeric_handle_inf_cmp(num1, num2, result);
		else
		{
			result = cmp_var_common(NUMERIC_DIGITS(leftarg), NUMERIC_NDIGITS(leftarg),
									NUMERIC_WEIGHT(leftarg), NUMERIC_SIGN(leftarg),
									NUMERIC_DIGITS(rightarg), NUMERIC_NDIGITS(rightarg),
									NUMERIC_WEIGHT(rightarg), NUMERIC_SIGN(rightarg));
		}
	}
	else
	{
		result = cmp_var_common(NUMERIC_DIGITS(leftarg), NUMERIC_NDIGITS(leftarg),
								NUMERIC_WEIGHT(leftarg), NUMERIC_SIGN(leftarg),
								NUMERIC_DIGITS(rightarg), NUMERIC_NDIGITS(rightarg),
								NUMERIC_WEIGHT(rightarg), NUMERIC_SIGN(rightarg));
	}

	return result;
}

Datum
hash_numeric(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	NumericDigit *digits;

	/* If it's NaN, Inf or -Inf don't try to hash the rest of the fields */
	if (NUMERIC_IS_NAN(key) || NUMERIC_IS_INF(key))
		PG_RETURN_UINT32(0);

	weight = NUMERIC_WEIGHT(key);
	start_offset = 0;
	end_offset = 0;

	/*
	 * Omit any leading or trailing zeros from the input to the hash. The
	 * numeric implementation *should* guarantee that leading and trailing
	 * zeros are suppressed, but we're paranoid. Note that we measure the
	 * starting and ending offsets in units of NumericDigits, not bytes.
	 */
	digits = NUMERIC_DIGITS(key);
	for (i = 0; i < NUMERIC_NDIGITS(key); i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		/*
		 * The weight is effectively the # of digits before the decimal point,
		 * so decrement it for each leading zero we skip.
		 */
		weight--;
	}

	/*
	 * If there are no non-zero digits, then the value of the number is zero,
	 * regardless of any other fields.
	 */
	if (NUMERIC_NDIGITS(key) == start_offset)
		PG_RETURN_UINT32(-1);

	for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	/* If we get here, there should be at least one non-zero digit */
	Assert(start_offset + end_offset < NUMERIC_NDIGITS(key));

	/*
	 * Note that we don't hash on the Numeric's scale, since two numerics can
	 * compare equal but have different scales. We also don't hash on the
	 * sign, although we could: since a sign difference implies inequality,
	 * this shouldn't affect correctness.
	 */
	hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
	digit_hash = hash_any((unsigned char *) (NUMERIC_DIGITS(key) + start_offset),
						  hash_len * sizeof(NumericDigit));

	/* Mix in the weight, via XOR */
	result = digit_hash ^ weight;

	PG_RETURN_DATUM(result);
}

Datum
hash_numeric_new(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	NumericDigit *digits;

	/* If it's NaN, don't try to hash the rest of the fields */
	if (NUMERIC_IS_NAN(key) || NUMERIC_IS_INF(key))
		PG_RETURN_UINT32(0);

	weight = NUMERIC_WEIGHT(key);
	start_offset = 0;
	end_offset = 0;

	/*
	 * Omit any leading or trailing zeros from the input to the hash. The
	 * numeric implementation *should* guarantee that leading and trailing
	 * zeros are suppressed, but we're paranoid. Note that we measure the
	 * starting and ending offsets in units of NumericDigits, not bytes.
	 */
	digits = NUMERIC_DIGITS(key);
	for (i = 0; i < NUMERIC_NDIGITS(key); i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		/*
		 * The weight is effectively the # of digits before the decimal point,
		 * so decrement it for each leading zero we skip.
		 */
		weight--;
	}

	/*
	 * If there are no non-zero digits, then the value of the number is zero,
	 * regardless of any other fields.
	 */
	if (NUMERIC_NDIGITS(key) == start_offset)
		PG_RETURN_UINT32(-1);

	for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	/* If we get here, there should be at least one non-zero digit */
	Assert(start_offset + end_offset < NUMERIC_NDIGITS(key));

	/*
	 * Note that we don't hash on the Numeric's scale, since two numerics can
	 * compare equal but have different scales. We also don't hash on the
	 * sign, although we could: since a sign difference implies inequality,
	 * this shouldn't affect correctness.
	 */
	hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
	digit_hash = hash_any_new((unsigned char *) (NUMERIC_DIGITS(key) + start_offset),
						  hash_len * sizeof(NumericDigit));

	/* Mix in the weight, via XOR */
	result = digit_hash ^ weight;

	PG_RETURN_DATUM(result);
}

/*
 * Returns 64-bit value by hashing a value to a 64-bit value, with a seed.
 * Otherwise, similar to hash_numeric.
 */
Datum
hash_numeric_extended(PG_FUNCTION_ARGS)
{
	Numeric		key = PG_GETARG_NUMERIC(0);
	uint64		seed = PG_GETARG_INT64(1);
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	NumericDigit *digits;

	if (NUMERIC_IS_NAN(key) || NUMERIC_IS_INF(key))
		PG_RETURN_UINT64(seed);

	weight = NUMERIC_WEIGHT(key);
	start_offset = 0;
	end_offset = 0;

	digits = NUMERIC_DIGITS(key);
	for (i = 0; i < NUMERIC_NDIGITS(key); i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		weight--;
	}

	if (NUMERIC_NDIGITS(key) == start_offset)
		PG_RETURN_UINT64(seed - 1);

	for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	Assert(start_offset + end_offset < NUMERIC_NDIGITS(key));

	hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
	digit_hash = hash_any_extended((unsigned char *) (NUMERIC_DIGITS(key)
													  + start_offset),
								   hash_len * sizeof(NumericDigit),
								   seed);

	result = UInt64GetDatum(DatumGetUInt64(digit_hash) ^ weight);

	PG_RETURN_DATUM(result);
}


/* ----------------------------------------------------------------------
 *
 * Basic arithmetic functions
 *
 * ----------------------------------------------------------------------
 */


/*
 * numeric_add() -
 *
 *	Add two numerics
 */
Datum
numeric_add(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_add_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}

/*
 * numeric_add_opt_error() -
 *
 *	Internal version of numeric_add().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_add_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		return make_result(&const_nan);

    /*
	 * Handle Inf and -Inf
	 */
    numeric_handle_inf_numeric(num1, num2);
	/*
	 * Unpack the values, let add_var() compute the result and return it.
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	add_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_sub() -
 *
 *	Subtract one numeric from another
 */
Datum
numeric_sub(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_sub_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_sub_opt_error() -
 *
 *	Internal version of numeric_sub().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_sub_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		return make_result(&const_nan);

    /*
	 * Handle Inf and -Inf
	 */
    numeric_handle_inf_numeric(num1, num2);
	/*
	 * Unpack the values, let sub_var() compute the result and return it.
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	sub_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_mul() -
 *
 *	Calculate the product of two numerics
 */
Datum
numeric_mul(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_mul_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_mul_opt_error() -
 *
 *	Internal version of numeric_mul().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_mul_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		return make_result(&const_nan);

    /*
	 * Handle Inf and -Inf
     If either num1 or num2 is Inf or -Inf:
     1.The result of multiplication of Inf or -Inf with NaN and 0 is NaN.
     2.Otherwise, if num1 and num2 have the same sign, the result is Inf. If they have different signs, the result is -Inf.
	 */
    if (NUMERIC_IS_INF(num1) || NUMERIC_IS_INF(num2))
    {
        if (numeric_is_zero(num1) || numeric_is_zero(num2) || numeric_is_nan(num1) || numeric_is_nan(num2))
            return make_result(&const_nan);
        return numeric_sign_equal(num1, num2) ?
               make_result(&const_ora_binary_double_pos_inf) : make_result(&const_ora_binary_double_neg_inf);
    }
	/*
	 * Unpack the values, let mul_var() compute the result and return it.
	 * Unlike add_var() and sub_var(), mul_var() will round its result. In the
	 * case of numeric_mul(), which is invoked for the * operator on numerics,
	 * we request exact representation for the product (rscale = sum(dscale of
	 * arg1, dscale of arg2)).
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);
	mul_var(&arg1, &arg2, &result, arg1.dscale + arg2.dscale);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}


/*
 * numeric_div() -
 *
 *	Divide one numeric into another
 */
Datum
numeric_div(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	res = numeric_div_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_div_opt_error() -
 *
 *	Internal version of numeric_div().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;
	int			rscale;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		return make_result(&const_nan);

    /*
	 * Handle Inf and -Inf
	 */
    if (NUMERIC_IS_INF(num1))
    {
        if (NUMERIC_IS_NAN(num2) || NUMERIC_IS_INF(num2))
            return make_result(&const_nan);
        return numeric_sign_equal(num1, num2)
               ? make_result(&const_ora_binary_double_pos_inf) : make_result(&const_ora_binary_double_neg_inf);
    }
    if (NUMERIC_IS_INF(num2))
		return make_result(&const_zero);
	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Select scale for division result
	 */
	rscale = select_div_scale(&arg1, &arg2);

	/*
	 * If "have_error" is provided, check for division by zero here
	 */
	if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0))
	{
		*have_error = true;
		return NULL;
	}

	/*
	 * Do the divide and return the result
	 */
	div_var(&arg1, &arg2, &result, rscale, HALF_ADJUST_ROUND);

	res = make_result_opt_error(&result, have_error);

	free_var(&result);

	return res;
}

/*
 * numeric_div_trunc() -
 *
 *	Divide one numeric into another, truncating the result to an integer
 */
Datum
numeric_div_trunc(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;
	Numeric		res;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_INF(num1))
	{
		if (NUMERIC_IS_NAN(num2) || NUMERIC_IS_INF(num2))
			PG_RETURN_NUMERIC(make_result(&const_nan));
		PG_RETURN_NUMERIC(numeric_sign_equal(num1, num2) ?
				make_result(&const_ora_binary_double_pos_inf)
				: make_result(&const_ora_binary_double_neg_inf));
	}
	if (NUMERIC_IS_INF(num2))
		PG_RETURN_NUMERIC(make_result(&const_zero));
	/*
	 * Unpack the arguments
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * Do the divide and return the result
	 */
	div_var(&arg1, &arg2, &result, 0, TRUNCATE_NUM);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_mod() -
 *
 *	Calculate the modulo of two numerics
 */
Datum
numeric_mod(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;

	/* non-Inf mod 0 is original val */
	if (ORA_MODE && numeric_is_zero(num2) && !NUMERIC_IS_INF(num1))
		PG_RETURN_NUMERIC(num1);

	res = numeric_mod_opt_error(num1, num2, NULL);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_mod_opt_error() -
 *
 *	Internal version of numeric_mod().  If "*have_error" flag is provided,
 *	on error it's set to true, NULL returned.  This is helpful when caller
 *	need to handle errors by itself.
 */
Numeric
numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		return make_result(&const_nan);

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_INF(num2))
	{
		if (!NUMERIC_IS_INF(num1) && !NUMERIC_IS_NAN(num1))
		{
			init_var_from_num(num1, &result);
			res = make_result_opt_error(&result, NULL);
			free_var(&result);
			return res;
		}
		return make_result(&const_nan);
	}
	if (NUMERIC_IS_INF(num1))
		return make_result(&const_nan);

	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	/*
	 * If "have_error" is provided, check for division by zero here
	 */
	if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0))
	{
		*have_error = true;
		return NULL;
	}

	mod_var(&arg1, &arg2, &result);

	res = make_result_opt_error(&result, NULL);

	free_var(&result);

	return res;
}

/*
 * numeric_remainder() -
 *
 *	Calculate the remainder of two numerics
 */
Datum
numeric_remainder(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_INF(num2))
	{
		if (!NUMERIC_IS_INF(num1) && !NUMERIC_IS_NAN(num1))
		{
			init_var_from_num(num1, &result);
			res = make_result(&result);
			free_var(&result);
			PG_RETURN_NUMERIC(res);
		}
		PG_RETURN_NUMERIC(make_result(&const_nan));
	}
	if (NUMERIC_IS_INF(num1))
		PG_RETURN_NUMERIC(make_result(&const_nan));
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	init_var(&result);

	remainder_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}
/*
 * numeric_inc() -
 *
 *	Increment a number by one
 */
Datum
numeric_inc(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	arg;
	Numeric		res;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_datum(num);
	/*
	 * Compute the result and return it
	 */
	init_var_from_num(num, &arg);

	add_var(&arg, &const_one, &arg);

	res = make_result(&arg);

	free_var(&arg);

	PG_RETURN_NUMERIC(res);
}

/*
 * numeric_dec() -
 *
 *	Decrement a number by one
 */
Datum
numeric_dec(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	arg;
	Numeric		res;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_datum(num);
	/*
	 * Compute the result and return it
	 */
	init_var_from_num(num, &arg);

	sub_var(&arg, &const_one, &arg);

	res = make_result(&arg);

	free_var(&arg);

	PG_RETURN_NUMERIC(res);
}

Datum
numeric_inc_any(PG_FUNCTION_ARGS)
{
	return numeric_inc(fcinfo);
}

Datum
numeric_dec_any(PG_FUNCTION_ARGS)
{
	return numeric_dec(fcinfo);
}
/*
 * numeric_smaller() -
 *
 *	Return the smaller of two numbers
 */
Datum
numeric_smaller(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);

	/*
	 * Use cmp_numerics so that this will agree with the comparison operators,
	 * particularly as regards comparisons involving NaN.
	 */
	if (cmp_numerics(num1, num2) < 0)
		PG_RETURN_NUMERIC(num1);
	else
		PG_RETURN_NUMERIC(num2);
}


/*
 * numeric_larger() -
 *
 *	Return the larger of two numbers
 */
Datum
numeric_larger(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);

	/*
	 * Use cmp_numerics so that this will agree with the comparison operators,
	 * particularly as regards comparisons involving NaN.
	 */
	if (cmp_numerics(num1, num2) > 0)
		PG_RETURN_NUMERIC(num1);
	else
		PG_RETURN_NUMERIC(num2);
}


/* ----------------------------------------------------------------------
 *
 * Advanced math functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * numeric_fac()
 *
 * Compute factorial
 */
Datum
numeric_fac(PG_FUNCTION_ARGS)
{
	int64		num = PG_GETARG_INT64(0);
	Numeric		res;
	NumericVar	fact;
	NumericVar	result;

	if (num <= 1)
	{
		res = make_result(&const_one);
		PG_RETURN_NUMERIC(res);
	}
	/* Fail immediately if the result would overflow */
	if (num > 32177)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));

	quick_init_var(&fact);
	quick_init_var(&result);

	int64_to_numericvar(num, &result);

	for (num = num - 1; num > 1; num--)
	{
		/* this loop can take awhile, so allow it to be interrupted */
		CHECK_FOR_INTERRUPTS();

		int64_to_numericvar(num, &fact);

		mul_var(&result, &fact, &result, 0);
	}

	res = make_result(&result);

	free_var(&fact);
	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_sqrt() -
 *
 *	Compute the square root of a numeric.
 */
Datum
numeric_sqrt(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	arg;
	NumericVar	result;
	int			sweight;
	int			rscale;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_POS_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf));
	if (NUMERIC_IS_NEG_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));
	/*
	 * Unpack the argument and determine the result scale.  We choose a scale
	 * to give at least NUMERIC_MIN_SIG_DIGITS significant digits; but in any
	 * case not less than the input's dscale.
	 */
	init_var_from_num(num, &arg);

	quick_init_var(&result);

	/* Assume the input was normalized, so arg.weight is accurate */
	sweight = (arg.weight + 1) * DEC_DIGITS / 2 - 1;

	rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
	rscale = Max(rscale, arg.dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/*
	 * Let sqrt_var() do the calculation and return the result.
	 */
	sqrt_var(&arg, &result, rscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_exp() -
 *
 *	Raise e to the power of x
 */
Datum
numeric_exp(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	arg;
	NumericVar	result;
	int			rscale;
	double		val;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_POS_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf));
	if (NUMERIC_IS_NEG_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_zero));
	/*
	 * Unpack the argument and determine the result scale.  We choose a scale
	 * to give at least NUMERIC_MIN_SIG_DIGITS significant digits; but in any
	 * case not less than the input's dscale.
	 */
	init_var_from_num(num, &arg);

	quick_init_var(&result);

	/* convert input to float8, ignoring overflow */
	val = numericvar_to_double_no_overflow(&arg);

	/*
	 * log10(result) = num * log10(e), so this is approximately the decimal
	 * weight of the result:
	 */
	val *= 0.434294481903252;

	/* limit to something that won't cause integer overflow */
	val = Max(val, -NUMERIC_MAX_RESULT_SCALE);
	val = Min(val, NUMERIC_MAX_RESULT_SCALE);

	rscale = NUMERIC_MIN_SIG_DIGITS - (int) val;
	rscale = Max(rscale, arg.dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/*
	 * Let exp_var() do the calculation and return the result.
	 */
	exp_var(&arg, &result, rscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_ln() -
 *
 *	Compute the natural logarithm of x
 */
Datum
numeric_ln(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	Numeric		res;
	NumericVar	arg;
	NumericVar	result;
	int			ln_dweight;
	int			rscale;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_POS_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf));
	if (NUMERIC_IS_NEG_INF(num))
		PG_RETURN_NUMERIC(make_result(&const_nan));
	init_var_from_num(num, &arg);
	quick_init_var(&result);

	/* Estimated dweight of logarithm */
	ln_dweight = estimate_ln_dweight(&arg);

	rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
	rscale = Max(rscale, arg.dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	ln_var(&arg, &result, rscale);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_log() -
 *
 *	Compute the logarithm of x in a given base
 */
Datum
numeric_log(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Initialize things
	 */
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);
	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_INF(num1) || NUMERIC_IS_INF(num2))
	{
		/* +Inf ^ 0 = 1, 1 ^ +Inf = +Inf */
		if (NUMERIC_IS_POS_INF(num1) && cmp_var(&const_one, &arg2) == 0)
			PG_RETURN_NUMERIC(make_result(&const_zero));
		if (NUMERIC_IS_POS_INF(num2) && cmp_var(&const_one, &arg1) == 0)
			PG_RETURN_NUMERIC(make_result(&const_ora_binary_double_pos_inf));
		PG_RETURN_NUMERIC(make_result(&const_nan));
	}
	quick_init_var(&result);

	/*
	 * Call log_var() to compute and return the result; note it handles scale
	 * selection itself.
	 */
	log_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


/*
 * numeric_power() -
 *
 *	Raise b to the power of x
 */
Datum
numeric_power(PG_FUNCTION_ARGS)
{
	Numeric		num1 = PG_GETARG_NUMERIC(0);
	Numeric		num2 = PG_GETARG_NUMERIC(1);
	Numeric		res;
	NumericVar	arg1;
	NumericVar	arg2;
	NumericVar	arg2_trunc;
	NumericVar	result;

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	/*
	 * Initialize things
	 */
	init_var(&arg2_trunc);
	quick_init_var(&result);
	init_var_from_num(num1, &arg1);
	init_var_from_num(num2, &arg2);

	/*
	 * Handle Inf and -Inf
	 */
	if (NUMERIC_IS_INF(num1))
	{
		/* +-Inf ^ 0 = 1 */
		if (numeric_is_zero(num2))
			PG_RETURN_NUMERIC(make_result(&const_one));
		/* +-Inf ^ neg number = 0 */
		if (NUMERIC_SIGN(num2) == NUMERIC_NEG || NUMERIC_SIGN(num2) == NUMERIC_NEG_INF)
			PG_RETURN_NUMERIC(make_result(&const_zero));
		PG_RETURN_NUMERIC(make_result(&arg1));
	}
	if (NUMERIC_IS_INF(num2))
	{
		/* 0 ^ +Inf = 0, 0 ^ -Inf = Inf*/
		if (numeric_is_zero(num1))
			PG_RETURN_NUMERIC(NUMERIC_IS_POS_INF(num2) ? make_result(&const_zero) :
						make_result(&const_ora_binary_double_pos_inf));
		PG_RETURN_NUMERIC(make_result(&const_nan));
	}

	set_var_from_var(&arg2, &arg2_trunc);
	trunc_var(&arg2_trunc, 0);

	/*
	 * The SQL spec requires that we emit a particular SQLSTATE error code for
	 * certain error conditions.  Specifically, we don't return a
	 * divide-by-zero error code for 0 ^ -1.
	 */
	if (cmp_var(&arg1, &const_zero) == 0 &&
		cmp_var(&arg2, &const_zero) < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("zero raised to a negative power is undefined")));

	if (cmp_var(&arg1, &const_zero) < 0 &&
		cmp_var(&arg2, &arg2_trunc) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("a negative number raised to a non-integer power yields a complex result")));

	/*
	 * Call power_var() to compute and return the result; note it handles
	 * scale selection itself.
	 */
	power_var(&arg1, &arg2, &result);

	res = make_result(&result);

	free_var(&result);
	free_var(&arg2_trunc);

	PG_RETURN_NUMERIC(res);
}

/*
 * numeric_scale() -
 *
 *	Returns the scale, i.e. the count of decimal digits in the fractional part
 */
Datum
numeric_scale(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	/*
	 * Handle NaN
	 */
	if (NUMERIC_IS_NAN(num))
		PG_RETURN_NULL();

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_null(num);
	PG_RETURN_INT32(NUMERIC_DSCALE_NORMAL(num));
}



/* ----------------------------------------------------------------------
 *
 * Type conversion functions
 *
 * ----------------------------------------------------------------------
 */


Datum
int4_numeric(PG_FUNCTION_ARGS)
{
	int32		val = PG_GETARG_INT32(0);
	Numeric		res;
	NumericVar	result;

	quick_init_var(&result);

	int64_to_numericvar((int64) val, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}

int32
numeric_int4_opt_error(Numeric num, bool *have_error)
{
	NumericVar	x;
	int32		result;

	/* XXX would it be better to return NULL? */
	if (NUMERIC_IS_NAN(num))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert NaN to integer")));
		}
	}

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_convert(num, "cannot convert Inf or -Inf to integer");
	/* Convert to variable format, then convert to int4 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int32(&x, &result))
	{
		if (have_error)
		{
			*have_error = true;
			return 0;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("integer out of range")));
		}
	}

	return result;
}

Datum
numeric_int4(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);

	PG_RETURN_INT32(numeric_int4_opt_error(num, NULL));
}

/*
 * Given a NumericVar, convert it to an int32. If the NumericVar
 * exceeds the range of an int32, false is returned, otherwise true is returned.
 * The input NumericVar is *not* free'd.
 */
static bool
numericvar_to_int32(const NumericVar *var, int32 *result)
{
	int64		val;

	if (!numericvar_to_int64(var, &val))
		return false;

	/* Down-convert to int4 */
	*result = (int32) val;

	/* Test for overflow by reverse-conversion. */
	return ((int64) *result == val);
}

Datum
int8_numeric(PG_FUNCTION_ARGS)
{
	int64		val = PG_GETARG_INT64(0);
	Numeric		res;
	NumericVar	result;

	quick_init_var(&result);

	int64_to_numericvar(val, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_int8(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	int64		result;

	/* XXX would it be better to return NULL? */
	if (NUMERIC_IS_NAN(num))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot convert NaN to bigint")));

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_convert(num, "cannot convert Inf or -Inf to bigint");
	/* Convert to variable format and thence to int8 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int64(&x, &result))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("bigint out of range")));

	PG_RETURN_INT64(result);
}


Datum
int2_numeric(PG_FUNCTION_ARGS)
{
	int16		val = PG_GETARG_INT16(0);
	Numeric		res;
	NumericVar	result;

	quick_init_var(&result);

	int64_to_numericvar((int64) val, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_int2(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	NumericVar	x;
	int64		val;
	int16		result;

	/* XXX would it be better to return NULL? */
	if (NUMERIC_IS_NAN(num))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot convert NaN to smallint")));

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_convert(num, "cannot convert Inf or -Inf to smallint");
	/* Convert to variable format and thence to int8 */
	init_var_from_num(num, &x);

	if (!numericvar_to_int64(&x, &val))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	/* Down-convert to int2 */
	result = (int16) val;

	/* Test for overflow by reverse-conversion. */
	if ((int64) result != val)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("smallint out of range")));

	PG_RETURN_INT16(result);
}


Datum
float8_numeric(PG_FUNCTION_ARGS)
{
	float8		val = PG_GETARG_FLOAT8(0);
	int32       typmod;
	Numeric		res;
	NumericVar	result;
	char		buf[DBL_DIG + 100];

	if (PG_NARGS() >= 2)
		typmod = PG_GETARG_INT32(1);
	else
		typmod = -1;

	if (isnan(val))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	sprintf(buf, "%.*g", DBL_DIG, val);

	/* Assume we need not worry about leading/trailing spaces */
	(void) set_var_from_str(buf, buf, &result);

	/*
	 * We have to identify what's the NumericVar represents for
	 * (number, float, binary_float or binary_double ?) in opentenbase_ora mode.
	 * Different subtype have different maxmum and minimum value.
	 */
	if (ORA_MODE)
		result.subtype = typmod_to_numeric_subtype(typmod);

	apply_typmod(&result, typmod);
	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_float8(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	char	   *tmp;
	Datum		result;

	if (NUMERIC_IS_NAN(num))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_float8(num);
	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	result = DirectFunctionCall1(float8in, CStringGetDatum(tmp));

	pfree(tmp);

	PG_RETURN_DATUM(result);
}


/*
 * Convert numeric to float8; if out of range, return +/- HUGE_VAL
 *
 * (internal helper function, not directly callable from SQL)
 */
Datum
numeric_float8_no_overflow(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	double		val;

	if (NUMERIC_IS_NAN(num))
		PG_RETURN_FLOAT8(get_float8_nan());

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_float8(num);
	val = numeric_to_double_no_overflow(num);

	PG_RETURN_FLOAT8(val);
}

Datum
float4_numeric(PG_FUNCTION_ARGS)
{
	float4		val = PG_GETARG_FLOAT4(0);
	Numeric		res;
	NumericVar	result;
	char		buf[FLT_DIG + 100];

	if (isnan(val))
		PG_RETURN_NUMERIC(make_result(&const_nan));

	sprintf(buf, "%.*g", FLT_DIG, val);

	/* Assume we need not worry about leading/trailing spaces */
	(void) set_var_from_str(buf, buf, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
}


Datum
numeric_float4(PG_FUNCTION_ARGS)
{
	Numeric		num = PG_GETARG_NUMERIC(0);
	char	   *tmp;
	Datum		result;

	if (NUMERIC_IS_NAN(num))
		PG_RETURN_FLOAT4(get_float4_nan());

	/*
	 * Handle Inf and -Inf
	 */
	numeric_handle_inf_float4(num);
	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	result = DirectFunctionCall1(float4in, CStringGetDatum(tmp));

	pfree(tmp);

	PG_RETURN_DATUM(result);
}

Datum
varchar_to_numeric(PG_FUNCTION_ARGS)
{
	char *str = TextDatumGetCString(PG_GETARG_DATUM(0));
	return DirectFunctionCall3(numeric_in, 
							   CStringGetDatum(str), 
							   ObjectIdGetDatum(InvalidOid),
                               Int32GetDatum(-1));
}

/* ----------------------------------------------------------------------
 *
 * Aggregate functions
 *
 * The transition datatype for all these aggregates is declared as INTERNAL.
 * Actually, it's a pointer to a NumericAggState allocated in the aggregate
 * context.  The digit buffers for the NumericVars will be there too.
 *
 * On platforms which support 128-bit integers some aggregates instead use a
 * 128-bit integer based transition datatype to speed up calculations.
 *
 * ----------------------------------------------------------------------
 */

typedef struct NumericAggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	MemoryContext agg_context;	/* context we're calculating in */
	int64		N;				/* count of processed numbers */
	NumericSumAccum sumX;		/* sum of processed numbers */
	NumericSumAccum sumX2;		/* sum of squares of processed numbers */
	int			maxScale;		/* maximum scale seen so far */
	int64		maxScaleCount;	/* number of values seen with maximum scale */
	int64		NaNcount;		/* count of NaN values (not included in N!) */
	Numeric		numericBISum;	/* sum of big integer */
} NumericAggState;

/*
 * Prepare state data for a numeric aggregate function that needs to compute
 * sum, count and optionally sum of squares of the input.
 */
static NumericAggState *
makeNumericAggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
	NumericAggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (NumericAggState *) palloc0(sizeof(NumericAggState));
	state->calcSumX2 = calcSumX2;
	state->agg_context = agg_context;

	MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * Like makeNumericAggState(), but allocate the state in the current memory
 * context.
 */
static NumericAggState *
makeNumericAggStateCurrentContext(bool calcSumX2)
{
	NumericAggState *state;

	state = (NumericAggState *) palloc0(sizeof(NumericAggState));
	state->calcSumX2 = calcSumX2;
	state->agg_context = CurrentMemoryContext;

	return state;
}

/*
 * Accumulate a new input value for numeric aggregate functions.
 */
static void
do_numeric_accum(NumericAggState *state, Numeric newval)
{
	NumericVar	X;
	NumericVar	X2;
	MemoryContext old_context;

	/* Count NaN inputs separately from all else */
	if (NUMERIC_IS_NAN(newval) || NUMERIC_IS_INF(newval))
	{
		state->NaNcount++;
		return;
	}

	/* load processed number in short-lived context */
	init_var_from_num(newval, &X);
	init_var(&X2);

	/*
	 * Track the highest input dscale that we've seen, to support inverse
	 * transitions (see do_numeric_discard).
	 */
	if (X.dscale > state->maxScale)
	{
		state->maxScale = X.dscale;
		state->maxScaleCount = 1;
	}
	else if (X.dscale == state->maxScale)
		state->maxScaleCount++;

	/* if we need X^2, calculate that in short-lived context */
	if (state->calcSumX2)
	{
		mul_var(&X, &X, &X2, X.dscale * 2);
	}

	/* The rest of this needs to work in the aggregate context */
	old_context = MemoryContextSwitchTo(state->agg_context);

	state->N++;

	/* Accumulate sums */
	accum_sum_add(&(state->sumX), &X);

	if (state->calcSumX2)
		accum_sum_add(&(state->sumX2), &X2);

	MemoryContextSwitchTo(old_context);
}

/*
 * Attempt to remove an input value from the aggregated state.
 *
 * If the value cannot be removed then the function will return false; the
 * possible reasons for failing are described below.
 *
 * If we aggregate the values 1.01 and 2 then the result will be 3.01.
 * If we are then asked to un-aggregate the 1.01 then we must fail as we
 * won't be able to tell what the new aggregated value's dscale should be.
 * We don't want to return 2.00 (dscale = 2), since the sum's dscale would
 * have been zero if we'd really aggregated only 2.
 *
 * Note: alternatively, we could count the number of inputs with each possible
 * dscale (up to some sane limit).  Not yet clear if it's worth the trouble.
 */
static bool
do_numeric_discard(NumericAggState *state, Numeric newval)
{
	NumericVar	X;
	NumericVar	X2;
	MemoryContext old_context;

	/* Count NaN inputs separately from all else */
	if (NUMERIC_IS_NAN(newval) || NUMERIC_IS_INF(newval))
	{
		state->NaNcount--;
		return true;
	}

	/* load processed number in short-lived context */
	init_var_from_num(newval, &X);
	init_var(&X2);
	/*
	 * state->sumX's dscale is the maximum dscale of any of the inputs.
	 * Removing the last input with that dscale would require us to recompute
	 * the maximum dscale of the *remaining* inputs, which we cannot do unless
	 * no more non-NaN inputs remain at all.  So we report a failure instead,
	 * and force the aggregation to be redone from scratch.
	 */
	if (X.dscale == state->maxScale)
	{
		if (state->maxScaleCount > 1 || state->maxScale == 0)
		{
			/*
			 * Some remaining inputs have same dscale, or dscale hasn't gotten
			 * above zero anyway
			 */
			state->maxScaleCount--;
		}
		else if (state->N == 1)
		{
			/* No remaining non-NaN inputs at all, so reset maxScale */
			state->maxScale = 0;
			state->maxScaleCount = 0;
		}
		else
		{
			/* Correct new maxScale is uncertain, must fail */
			return false;
		}
	}

	/* if we need X^2, calculate that in short-lived context */
	if (state->calcSumX2)
	{
		mul_var(&X, &X, &X2, X.dscale * 2);
	}

	/* The rest of this needs to work in the aggregate context */
	old_context = MemoryContextSwitchTo(state->agg_context);

	if (state->N-- > 1)
	{
		/* Negate X, to subtract it from the sum */
		X.sign = (X.sign == NUMERIC_POS ? NUMERIC_NEG : NUMERIC_POS);
		accum_sum_add(&(state->sumX), &X);

		if (state->calcSumX2)
		{
			/* Negate X^2. X^2 is always positive */
			X2.sign = NUMERIC_NEG;
			accum_sum_add(&(state->sumX2), &X2);
		}
	}
	else
	{
		/* Zero the sums */
		Assert(state->N == 0);

		accum_sum_reset(&state->sumX);
		if (state->calcSumX2)
			accum_sum_reset(&state->sumX2);
	}

	MemoryContextSwitchTo(old_context);

	return true;
}

/*
 * Generic transition function for numeric aggregates that require sumX2.
 */
Datum
numeric_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
		do_numeric_accum(state, PG_GETARG_NUMERIC(1));

	PG_RETURN_POINTER(state);
}

/*
 * Generic combine function for numeric aggregates which require sumX2
 */
Datum
numeric_combine(PG_FUNCTION_ARGS)
{
	NumericAggState *state1;
	NumericAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (NumericAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makeNumericAggStateCurrentContext(true);
		state1->N = state2->N;
		state1->NaNcount = state2->NaNcount;
		state1->maxScale = state2->maxScale;
		state1->maxScaleCount = state2->maxScaleCount;

		accum_sum_copy(&state1->sumX, &state2->sumX);
		accum_sum_copy(&state1->sumX2, &state2->sumX2);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	if (state2->N > 0)
	{
		state1->N += state2->N;
		state1->NaNcount += state2->NaNcount;

		/*
		 * These are currently only needed for moving aggregates, but let's do
		 * the right thing anyway...
		 */
		if (state2->maxScale > state1->maxScale)
		{
			state1->maxScale = state2->maxScale;
			state1->maxScaleCount = state2->maxScaleCount;
		}
		else if (state2->maxScale == state1->maxScale)
			state1->maxScaleCount += state2->maxScaleCount;

		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);
		accum_sum_combine(&state1->sumX2, &state2->sumX2);

		MemoryContextSwitchTo(old_context);
	}
	PG_RETURN_POINTER(state1);
}

/*
 * Generic transition function for numeric aggregates that don't require sumX2.
 */
Datum
numeric_avg_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
		do_numeric_accum(state, PG_GETARG_NUMERIC(1));

	PG_RETURN_POINTER(state);
}

/*
 * Combine function for numeric aggregates which don't require sumX2
 */
Datum
numeric_avg_combine(PG_FUNCTION_ARGS)
{
	NumericAggState *state1;
	NumericAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (NumericAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makeNumericAggStateCurrentContext(false);
		state1->N = state2->N;
		state1->NaNcount = state2->NaNcount;
		state1->maxScale = state2->maxScale;
		state1->maxScaleCount = state2->maxScaleCount;

		accum_sum_copy(&state1->sumX, &state2->sumX);

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	if (state2->N > 0)
	{
		state1->N += state2->N;
		state1->NaNcount += state2->NaNcount;

		/*
		 * These are currently only needed for moving aggregates, but let's do
		 * the right thing anyway...
		 */
		if (state2->maxScale > state1->maxScale)
		{
			state1->maxScale = state2->maxScale;
			state1->maxScaleCount = state2->maxScaleCount;
		}
		else if (state2->maxScale == state1->maxScale)
			state1->maxScaleCount += state2->maxScaleCount;

		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);

		MemoryContextSwitchTo(old_context);
	}
	PG_RETURN_POINTER(state1);
}

/*
 * numeric_avg_serialize
 *		Serialize NumericAggState for numeric aggregates that don't require
 *		sumX2.
 */
Datum
numeric_avg_serialize(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	StringInfoData buf;
	Datum		temp;
	bytea	   *sumX;
	bytea	   *result;
	NumericVar	tmp_var;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (NumericAggState *) PG_GETARG_POINTER(0);

	/*
	 * This is a little wasteful since make_result converts the NumericVar
	 * into a Numeric and numeric_send converts it back again. Is it worth
	 * splitting the tasks in numeric_send into separate functions to stop
	 * this? Doing so would also remove the fmgr call overhead.
	 */
	init_var(&tmp_var);
	if (state->numericBISum == NULL)
		accum_sum_final(&state->sumX, &tmp_var);
	else
		init_var_from_num(makeNumericNormal(state->numericBISum), &tmp_var);

	temp = DirectFunctionCall1(numeric_send,
							   NumericGetDatum(make_result(&tmp_var)));
	sumX = DatumGetByteaPP(temp);
	free_var(&tmp_var);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	pq_sendbytes(&buf, VARDATA_ANY(sumX), VARSIZE_ANY_EXHDR(sumX));

	/* maxScale */
	pq_sendint32(&buf, state->maxScale);

	/* maxScaleCount */
	pq_sendint64(&buf, state->maxScaleCount);

	/* NaNcount */
	pq_sendint64(&buf, state->NaNcount);

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

/*
 * numeric_avg_deserialize
 *		Deserialize bytea into NumericAggState for numeric aggregates that
 *		don't require sumX2.
 */
Datum
numeric_avg_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	NumericAggState *result;
	Datum		temp;
	NumericVar	tmp_var;
	StringInfoData buf;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf,
						   VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

	result = makeNumericAggStateCurrentContext(false);

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	temp = DirectFunctionCall3(numeric_recv,
							   PointerGetDatum(&buf),
							   InvalidOid,
							   -1);
	init_var_from_num(DatumGetNumeric(temp), &tmp_var);
	accum_sum_add(&(result->sumX), &tmp_var);

	/* maxScale */
	result->maxScale = pq_getmsgint(&buf, 4);

	/* maxScaleCount */
	result->maxScaleCount = pq_getmsgint64(&buf);

	/* NaNcount */
	result->NaNcount = pq_getmsgint64(&buf);

	pq_getmsgend(&buf);
	pfree(buf.data);

	PG_RETURN_POINTER(result);
}

/*
 * numeric_serialize
 *		Serialization function for NumericAggState for numeric aggregates that
 *		require sumX2.
 */
Datum
numeric_serialize(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	StringInfoData buf;
	Datum		temp;
	bytea	   *sumX;
	NumericVar	tmp_var;
	bytea	   *sumX2;
	bytea	   *result;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (NumericAggState *) PG_GETARG_POINTER(0);

	/*
	 * This is a little wasteful since make_result converts the NumericVar
	 * into a Numeric and numeric_send converts it back again. Is it worth
	 * splitting the tasks in numeric_send into separate functions to stop
	 * this? Doing so would also remove the fmgr call overhead.
	 */
	init_var(&tmp_var);

	if (state->numericBISum == NULL)
		accum_sum_final(&state->sumX, &tmp_var);
	else
		init_var_from_num(makeNumericNormal(state->numericBISum), &tmp_var);

	temp = DirectFunctionCall1(numeric_send,
							   NumericGetDatum(make_result(&tmp_var)));
	sumX = DatumGetByteaPP(temp);

	accum_sum_final(&state->sumX2, &tmp_var);
	temp = DirectFunctionCall1(numeric_send,
							   NumericGetDatum(make_result(&tmp_var)));
	sumX2 = DatumGetByteaPP(temp);

	free_var(&tmp_var);

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	pq_sendbytes(&buf, VARDATA_ANY(sumX), VARSIZE_ANY_EXHDR(sumX));

	/* sumX2 */
	pq_sendbytes(&buf, VARDATA_ANY(sumX2), VARSIZE_ANY_EXHDR(sumX2));

	/* maxScale */
	pq_sendint32(&buf, state->maxScale);

	/* maxScaleCount */
	pq_sendint64(&buf, state->maxScaleCount);

	/* NaNcount */
	pq_sendint64(&buf, state->NaNcount);

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

/*
 * numeric_deserialize
 *		Deserialization function for NumericAggState for numeric aggregates that
 *		require sumX2.
 */
Datum
numeric_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	NumericAggState *result;
	Datum		temp;
	NumericVar	sumX_var;
	NumericVar	sumX2_var;
	StringInfoData buf;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf,
						   VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

	result = makeNumericAggStateCurrentContext(false);

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	temp = DirectFunctionCall3(numeric_recv,
							   PointerGetDatum(&buf),
							   InvalidOid,
							   -1);
	init_var_from_num(DatumGetNumeric(temp), &sumX_var);
	accum_sum_add(&(result->sumX), &sumX_var);

	/* sumX2 */
	temp = DirectFunctionCall3(numeric_recv,
							   PointerGetDatum(&buf),
							   InvalidOid,
							   -1);
	init_var_from_num(DatumGetNumeric(temp), &sumX2_var);
	accum_sum_add(&(result->sumX2), &sumX2_var);

	/* maxScale */
	result->maxScale = pq_getmsgint(&buf, 4);

	/* maxScaleCount */
	result->maxScaleCount = pq_getmsgint64(&buf);

	/* NaNcount */
	result->NaNcount = pq_getmsgint64(&buf);

	pq_getmsgend(&buf);
	pfree(buf.data);

	PG_RETURN_POINTER(result);
}

/*
 * Generic inverse transition function for numeric aggregates
 * (with or without requirement for X^2).
 */
Datum
numeric_accum_inv(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "numeric_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
		/* If we fail to perform the inverse transition, return NULL */
		if (!do_numeric_discard(state, PG_GETARG_NUMERIC(1)))
			PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(state);
}


/*
 * Integer data types in general use Numeric accumulators to share code
 * and avoid risk of overflow.
 *
 * However for performance reasons optimized special-purpose accumulator
 * routines are used when possible.
 *
 * On platforms with 128-bit integer support, the 128-bit routines will be
 * used when sum(X) or sum(X*X) fit into 128-bit.
 *
 * For 16 and 32 bit inputs, the N and sum(X) fit into 64-bit so the 64-bit
 * accumulators will be used for SUM and AVG of these data types.
 */

#ifdef HAVE_INT128
typedef struct Int128AggState
{
	bool		calcSumX2;		/* if true, calculate sumX2 */
	int64		N;				/* count of processed numbers */
	int128		sumX;			/* sum of processed numbers */
	int128		sumX2;			/* sum of squares of processed numbers */
} Int128AggState;

/*
 * Prepare state data for a 128-bit aggregate function that needs to compute
 * sum, count and optionally sum of squares of the input.
 */
static Int128AggState *
makeInt128AggState(FunctionCallInfo fcinfo, bool calcSumX2)
{
	Int128AggState *state;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old_context = MemoryContextSwitchTo(agg_context);

	state = (Int128AggState *) palloc0(sizeof(Int128AggState));
	state->calcSumX2 = calcSumX2;

	MemoryContextSwitchTo(old_context);

	return state;
}

/*
 * Like makeInt128AggState(), but allocate the state in the current memory
 * context.
 */
static Int128AggState *
makeInt128AggStateCurrentContext(bool calcSumX2)
{
	Int128AggState *state;

	state = (Int128AggState *) palloc0(sizeof(Int128AggState));
	state->calcSumX2 = calcSumX2;

	return state;
}

/*
 * Accumulate a new input value for 128-bit aggregate functions.
 */
static void
do_int128_accum(Int128AggState *state, int128 newval)
{
	if (state->calcSumX2)
		state->sumX2 += newval * newval;

	state->sumX += newval;
	state->N++;
}

/*
 * Remove an input value from the aggregated state.
 */
static void
do_int128_discard(Int128AggState *state, int128 newval)
{
	if (state->calcSumX2)
		state->sumX2 -= newval * newval;

	state->sumX -= newval;
	state->N--;
}

typedef Int128AggState PolyNumAggState;
#define makePolyNumAggState makeInt128AggState
#define makePolyNumAggStateCurrentContext makeInt128AggStateCurrentContext
#else
typedef NumericAggState PolyNumAggState;
#define makePolyNumAggState makeNumericAggState
#define makePolyNumAggStateCurrentContext makeNumericAggStateCurrentContext
#endif

Datum
int2_accum(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_accum(state, (int128) PG_GETARG_INT16(1));
#else
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int2_numeric,
													 PG_GETARG_DATUM(1)));
		do_numeric_accum(state, newval);
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int4_accum(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_accum(state, (int128) PG_GETARG_INT32(1));
#else
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
													 PG_GETARG_DATUM(1)));
		do_numeric_accum(state, newval);
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int8_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, true);

	if (!PG_ARGISNULL(1))
	{
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													 PG_GETARG_DATUM(1)));
		do_numeric_accum(state, newval);
	}

	PG_RETURN_POINTER(state);
}

/*
 * Combine function for numeric aggregates which require sumX2
 */
Datum
numeric_poly_combine(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state1;
	PolyNumAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makePolyNumAggState(fcinfo, true);
		state1->N = state2->N;

#ifdef HAVE_INT128
		state1->sumX = state2->sumX;
		state1->sumX2 = state2->sumX2;
#else
		accum_sum_copy(&state1->sumX, &state2->sumX);
		accum_sum_copy(&state1->sumX2, &state2->sumX2);
#endif

		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	if (state2->N > 0)
	{
		state1->N += state2->N;

#ifdef HAVE_INT128
		state1->sumX += state2->sumX;
		state1->sumX2 += state2->sumX2;
#else
		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);
		accum_sum_combine(&state1->sumX2, &state2->sumX2);

		MemoryContextSwitchTo(old_context);
#endif

	}
	PG_RETURN_POINTER(state1);
}

/*
 * numeric_poly_serialize
 *		Serialize PolyNumAggState into bytea for aggregate functions which
 *		require sumX2.
 */
Datum
numeric_poly_serialize(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;
	StringInfoData buf;
	bytea	   *sumX;
	bytea	   *sumX2;
	bytea	   *result;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (PolyNumAggState *) PG_GETARG_POINTER(0);

	/*
	 * If the platform supports int128 then sumX and sumX2 will be a 128 bit
	 * integer type. Here we'll convert that into a numeric type so that the
	 * combine state is in the same format for both int128 enabled machines
	 * and machines which don't support that type. The logic here is that one
	 * day we might like to send these over to another server for further
	 * processing and we want a standard format to work with.
	 */
	{
		Datum		temp;
		NumericVar	num;

		init_var(&num);

#ifdef HAVE_INT128
		int128_to_numericvar(state->sumX, &num);
#else
		if (state->numericBISum == NULL)
			accum_sum_final(&state->sumX, &num);
		else
			init_var_from_num(makeNumericNormal(state->numericBISum), &num);
#endif
		temp = DirectFunctionCall1(numeric_send,
								   NumericGetDatum(make_result(&num)));
		sumX = DatumGetByteaPP(temp);

#ifdef HAVE_INT128
		int128_to_numericvar(state->sumX2, &num);
#else
		accum_sum_final(&state->sumX2, &num);
#endif
		temp = DirectFunctionCall1(numeric_send,
								   NumericGetDatum(make_result(&num)));
		sumX2 = DatumGetByteaPP(temp);

		free_var(&num);
	}

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	pq_sendbytes(&buf, VARDATA_ANY(sumX), VARSIZE_ANY_EXHDR(sumX));

	/* sumX2 */
	pq_sendbytes(&buf, VARDATA_ANY(sumX2), VARSIZE_ANY_EXHDR(sumX2));

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

/*
 * numeric_poly_deserialize
 *		Deserialize PolyNumAggState from bytea for aggregate functions which
 *		require sumX2.
 */
Datum
numeric_poly_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	PolyNumAggState *result;
	Datum		sumX;
	NumericVar	sumX_var;
	Datum		sumX2;
	NumericVar	sumX2_var;
	StringInfoData buf;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf,
						   VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

	result = makePolyNumAggStateCurrentContext(false);

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	sumX = DirectFunctionCall3(numeric_recv,
							   PointerGetDatum(&buf),
							   InvalidOid,
							   -1);

	/* sumX2 */
	sumX2 = DirectFunctionCall3(numeric_recv,
								PointerGetDatum(&buf),
								InvalidOid,
								-1);

	init_var_from_num(DatumGetNumeric(sumX), &sumX_var);
#ifdef HAVE_INT128
	numericvar_to_int128(&sumX_var, &result->sumX);
#else
	accum_sum_add(&result->sumX, &sumX_var);
#endif

	init_var_from_num(DatumGetNumeric(sumX2), &sumX2_var);
#ifdef HAVE_INT128
	numericvar_to_int128(&sumX2_var, &result->sumX2);
#else
	accum_sum_add(&result->sumX2, &sumX2_var);
#endif

	pq_getmsgend(&buf);
	pfree(buf.data);

	PG_RETURN_POINTER(result);
}

/*
 * Transition function for int8 input when we don't need sumX2.
 */
Datum
int8_avg_accum(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makePolyNumAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_accum(state, (int128) PG_GETARG_INT64(1));
#else
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													 PG_GETARG_DATUM(1)));
		do_numeric_accum(state, newval);
#endif
	}

	PG_RETURN_POINTER(state);
}

/*
 * Combine function for PolyNumAggState for aggregates which don't require
 * sumX2
 */
Datum
int8_avg_combine(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state1;
	PolyNumAggState *state2;
	MemoryContext agg_context;
	MemoryContext old_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state1 = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);
	state2 = PG_ARGISNULL(1) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(1);

	if (state2 == NULL)
		PG_RETURN_POINTER(state1);

	/* manually copy all fields from state2 to state1 */
	if (state1 == NULL)
	{
		old_context = MemoryContextSwitchTo(agg_context);

		state1 = makePolyNumAggState(fcinfo, false);
		state1->N = state2->N;

#ifdef HAVE_INT128
		state1->sumX = state2->sumX;
#else
		accum_sum_copy(&state1->sumX, &state2->sumX);
#endif
		MemoryContextSwitchTo(old_context);

		PG_RETURN_POINTER(state1);
	}

	if (state2->N > 0)
	{
		state1->N += state2->N;

#ifdef HAVE_INT128
		state1->sumX += state2->sumX;
#else
		/* The rest of this needs to work in the aggregate context */
		old_context = MemoryContextSwitchTo(agg_context);

		/* Accumulate sums */
		accum_sum_combine(&state1->sumX, &state2->sumX);

		MemoryContextSwitchTo(old_context);
#endif

	}
	PG_RETURN_POINTER(state1);
}

/*
 * int8_avg_serialize
 *		Serialize PolyNumAggState into bytea using the standard
 *		recv-function infrastructure.
 */
Datum
int8_avg_serialize(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;
	StringInfoData buf;
	bytea	   *sumX;
	bytea	   *result;

	/* Ensure we disallow calling when not in aggregate context */
	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	state = (PolyNumAggState *) PG_GETARG_POINTER(0);

	/*
	 * If the platform supports int128 then sumX will be a 128 integer type.
	 * Here we'll convert that into a numeric type so that the combine state
	 * is in the same format for both int128 enabled machines and machines
	 * which don't support that type. The logic here is that one day we might
	 * like to send these over to another server for further processing and we
	 * want a standard format to work with.
	 */
	{
		Datum		temp;
		NumericVar	num;

		init_var(&num);

#ifdef HAVE_INT128
		int128_to_numericvar(state->sumX, &num);
#else
		if (state->numericBISum != NULL)
			init_var_from_num(makeNumericNormal(state->numericBISum), &num);
		else
			accum_sum_final(&state->sumX, &num);
#endif
		temp = DirectFunctionCall1(numeric_send,
								   NumericGetDatum(make_result(&num)));
		sumX = DatumGetByteaPP(temp);

		free_var(&num);
	}

	pq_begintypsend(&buf);

	/* N */
	pq_sendint64(&buf, state->N);

	/* sumX */
	pq_sendbytes(&buf, VARDATA_ANY(sumX), VARSIZE_ANY_EXHDR(sumX));

	result = pq_endtypsend(&buf);

	PG_RETURN_BYTEA_P(result);
}

/*
 * int8_avg_deserialize
 *		Deserialize bytea back into PolyNumAggState.
 */
Datum
int8_avg_deserialize(PG_FUNCTION_ARGS)
{
	bytea	   *sstate;
	PolyNumAggState *result;
	StringInfoData buf;
	Datum		temp;
	NumericVar	num;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	sstate = PG_GETARG_BYTEA_PP(0);

	/*
	 * Copy the bytea into a StringInfo so that we can "receive" it using the
	 * standard recv-function infrastructure.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf,
						   VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

	result = makePolyNumAggStateCurrentContext(false);

	/* N */
	result->N = pq_getmsgint64(&buf);

	/* sumX */
	temp = DirectFunctionCall3(numeric_recv,
							   PointerGetDatum(&buf),
							   InvalidOid,
							   -1);
	init_var_from_num(DatumGetNumeric(temp), &num);
#ifdef HAVE_INT128
	numericvar_to_int128(&num, &result->sumX);
#else
	accum_sum_add(&result->sumX, &num);
#endif

	pq_getmsgend(&buf);
	pfree(buf.data);

	PG_RETURN_POINTER(result);
}

/*
 * Inverse transition functions to go with the above.
 */

Datum
int2_accum_inv(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int2_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_discard(state, (int128) PG_GETARG_INT16(1));
#else
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int2_numeric,
													 PG_GETARG_DATUM(1)));

		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, newval))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int4_accum_inv(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int4_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_discard(state, (int128) PG_GETARG_INT32(1));
#else
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
													 PG_GETARG_DATUM(1)));

		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, newval))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
int8_accum_inv(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int8_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													 PG_GETARG_DATUM(1)));

		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, newval))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
	}

	PG_RETURN_POINTER(state);
}

Datum
int8_avg_accum_inv(PG_FUNCTION_ARGS)
{
	PolyNumAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "int8_avg_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
#ifdef HAVE_INT128
		do_int128_discard(state, (int128) PG_GETARG_INT64(1));
#else
		Numeric		newval;

		newval = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													 PG_GETARG_DATUM(1)));

		/* Should never fail, all inputs have dscale 0 */
		if (!do_numeric_discard(state, newval))
			elog(ERROR, "do_numeric_discard failed unexpectedly");
#endif
	}

	PG_RETURN_POINTER(state);
}

Datum
numeric_poly_sum(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	NumericVar	result;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	init_var(&result);

	int128_to_numericvar(state->sumX, &result);

	res = make_result(&result);

	free_var(&result);

	PG_RETURN_NUMERIC(res);
#else
	return numeric_sum(fcinfo);
#endif
}

Datum
numeric_poly_avg(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	NumericVar	result;
	Datum		countd,
				sumd;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || state->N == 0)
		PG_RETURN_NULL();

	init_var(&result);

	int128_to_numericvar(state->sumX, &result);

	countd = DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(state->N));
	sumd = NumericGetDatum(make_result(&result));

	free_var(&result);

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
#else
	return numeric_avg(fcinfo);
#endif
}

Datum
numeric_avg(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Datum		N_datum;
	Datum		sumX_datum;
	NumericVar	sumX_var;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || (state->N + state->NaNcount) == 0)
		PG_RETURN_NULL();

	if (state->NaNcount > 0)	/* there was at least one NaN input */
		PG_RETURN_NUMERIC(make_result(&const_nan));

	N_datum = DirectFunctionCall1(int8_numeric, Int64GetDatum(state->N));

	init_var(&sumX_var);
	accum_sum_final(&state->sumX, &sumX_var);
	sumX_datum = NumericGetDatum(make_result(&sumX_var));
	free_var(&sumX_var);

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumX_datum, N_datum));
}

Datum
numeric_sum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	NumericVar	sumX_var;
	Numeric		result;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* If there were no non-null inputs, return NULL */
	if (state == NULL || (state->N + state->NaNcount) == 0)
		PG_RETURN_NULL();

	if (state->NaNcount > 0)	/* there was at least one NaN input */
		PG_RETURN_NUMERIC(make_result(&const_nan));


	init_var(&sumX_var);
	accum_sum_final(&state->sumX, &sumX_var);
	result = make_result(&sumX_var);
	free_var(&sumX_var);

	PG_RETURN_NUMERIC(result);
}

/*
 * Workhorse routine for the standard deviance and variance
 * aggregates. 'state' is aggregate's transition state.
 * 'variance' specifies whether we should calculate the
 * variance or the standard deviation. 'sample' indicates whether the
 * caller is interested in the sample or the population
 * variance/stddev.
 *
 * If appropriate variance statistic is undefined for the input,
 * *is_null is set to true and NULL is returned.
 */
static Numeric
numeric_stddev_internal(NumericAggState *state,
						bool variance, bool sample,
						bool *is_null)
{
	Numeric		res;
	NumericVar	vN,
				vsumX,
				vsumX2,
				vNminus1;
	NumericVar *comp;
	int			rscale;

	/* Deal with empty input and NaN-input cases */
	if (state == NULL || (state->N + state->NaNcount) == 0)
	{
		*is_null = true;
		return NULL;
	}

	*is_null = false;

	if (state->NaNcount > 0)
		return make_result(&const_nan);

	init_var(&vN);
	init_var(&vsumX);
	init_var(&vsumX2);

	int64_to_numericvar(state->N, &vN);
	accum_sum_final(&(state->sumX), &vsumX);
	accum_sum_final(&(state->sumX2), &vsumX2);

	/*
	 * Sample stddev and variance are undefined when N <= 1; population stddev
	 * is undefined when N == 0. Return NULL in either case.
	 */
	if (sample)
		comp = &const_one;
	else
		comp = &const_zero;

	if (cmp_var(&vN, comp) <= 0)
	{
		*is_null = true;
		return NULL;
	}

	quick_init_var(&vNminus1);
	sub_var(&vN, &const_one, &vNminus1);

	/* compute rscale for mul_var calls */
	rscale = vsumX.dscale * 2;

	mul_var(&vsumX, &vsumX, &vsumX, rscale);	/* vsumX = sumX * sumX */
	mul_var(&vN, &vsumX2, &vsumX2, rscale); /* vsumX2 = N * sumX2 */
	sub_var(&vsumX2, &vsumX, &vsumX2);	/* N * sumX2 - sumX * sumX */

	if (cmp_var(&vsumX2, &const_zero) <= 0)
	{
		/* Watch out for roundoff error producing a negative numerator */
		res = make_result(&const_zero);
	}
	else
	{
		if (sample)
			mul_var(&vN, &vNminus1, &vNminus1, 0);	/* N * (N - 1) */
		else
			mul_var(&vN, &vN, &vNminus1, 0);	/* N * N */
		rscale = select_div_scale(&vsumX2, &vNminus1);
		div_var(&vsumX2, &vNminus1, &vsumX, rscale, HALF_ADJUST_ROUND);	/* variance */
		if (!variance)
			sqrt_var(&vsumX, &vsumX, rscale);	/* stddev */

		res = make_result(&vsumX);
	}

	free_var(&vNminus1);
	free_var(&vsumX);
	free_var(&vsumX2);

	return res;
}

Datum
numeric_var_samp(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, true, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

Datum
numeric_stddev_samp(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, false, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

Datum
numeric_var_pop(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, true, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

Datum
numeric_stddev_pop(PG_FUNCTION_ARGS)
{
	NumericAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	res = numeric_stddev_internal(state, false, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
}

#ifdef HAVE_INT128
static Numeric
numeric_poly_stddev_internal(Int128AggState *state,
							 bool variance, bool sample,
							 bool *is_null)
{
	NumericAggState numstate;
	Numeric		res;

	/* Initialize an empty agg state */
	memset(&numstate, 0, sizeof(NumericAggState));

	if (state)
	{
		NumericVar	tmp_var;

		numstate.N = state->N;

		init_var(&tmp_var);

		int128_to_numericvar(state->sumX, &tmp_var);
		accum_sum_add(&numstate.sumX, &tmp_var);

		int128_to_numericvar(state->sumX2, &tmp_var);
		accum_sum_add(&numstate.sumX2, &tmp_var);

		free_var(&tmp_var);
	}

	res = numeric_stddev_internal(&numstate, variance, sample, is_null);

	if (numstate.sumX.ndigits > 0)
	{
		pfree(numstate.sumX.pos_digits);
		pfree(numstate.sumX.neg_digits);
	}
	if (numstate.sumX2.ndigits > 0)
	{
		pfree(numstate.sumX2.pos_digits);
		pfree(numstate.sumX2.neg_digits);
	}

	return res;
}
#endif

Datum
numeric_poly_var_samp(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, true, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_var_samp(fcinfo);
#endif
}

Datum
numeric_poly_stddev_samp(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, false, true, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_stddev_samp(fcinfo);
#endif
}

Datum
numeric_poly_var_pop(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, true, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_var_pop(fcinfo);
#endif
}

Datum
numeric_poly_stddev_pop(PG_FUNCTION_ARGS)
{
#ifdef HAVE_INT128
	PolyNumAggState *state;
	Numeric		res;
	bool		is_null;

	state = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);

	res = numeric_poly_stddev_internal(state, false, false, &is_null);

	if (is_null)
		PG_RETURN_NULL();
	else
		PG_RETURN_NUMERIC(res);
#else
	return numeric_stddev_pop(fcinfo);
#endif
}

/*
 * SUM transition functions for integer datatypes.
 *
 * To avoid overflow, we use accumulators wider than the input datatype.
 * A Numeric accumulator is needed for int8 input; for int4 and int2
 * inputs, we use int8 accumulators which should be sufficient for practical
 * purposes.  (The latter two therefore don't really belong in this file,
 * but we keep them here anyway.)
 *
 * Because SQL defines the SUM() of no values to be NULL, not zero,
 * the initial condition of the transition data value needs to be NULL. This
 * means we can't rely on ExecAgg to automatically insert the first non-null
 * data value into the transition data: it doesn't know how to do the type
 * conversion.  The upshot is that these routines have to be marked non-strict
 * and handle substitution of the first non-null input themselves.
 *
 * Note: these functions are used only in plain aggregation mode.
 * In moving-aggregate mode, we use intX_avg_accum and intX_avg_accum_inv.
 */

Datum
int2_sum(PG_FUNCTION_ARGS)
{
	int64		newval;

	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		newval = (int64) PG_GETARG_INT16(1);
		PG_RETURN_INT64(newval);
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to avoid palloc overhead. If not, we need to return
	 * the new value of the transition variable. (If int8 is pass-by-value,
	 * then of course this is useless as well as incorrect, so just ifdef it
	 * out.)
	 */
#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *oldsum = (int64 *) PG_GETARG_POINTER(0);

		/* Leave the running sum unchanged in the new input is null */
		if (!PG_ARGISNULL(1))
			*oldsum = *oldsum + (int64) PG_GETARG_INT16(1);

		PG_RETURN_POINTER(oldsum);
	}
	else
#endif
	{
		int64		oldsum = PG_GETARG_INT64(0);

		/* Leave sum unchanged if new input is null. */
		if (PG_ARGISNULL(1))
			PG_RETURN_INT64(oldsum);

		/* OK to do the addition. */
		newval = oldsum + (int64) PG_GETARG_INT16(1);

		PG_RETURN_INT64(newval);
	}
}

Datum
int4_sum(PG_FUNCTION_ARGS)
{
	int64		newval;

	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		newval = (int64) PG_GETARG_INT32(1);
		PG_RETURN_INT64(newval);
	}

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to avoid palloc overhead. If not, we need to return
	 * the new value of the transition variable. (If int8 is pass-by-value,
	 * then of course this is useless as well as incorrect, so just ifdef it
	 * out.)
	 */
#ifndef USE_FLOAT8_BYVAL		/* controls int8 too */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		int64	   *oldsum = (int64 *) PG_GETARG_POINTER(0);

		/* Leave the running sum unchanged in the new input is null */
		if (!PG_ARGISNULL(1))
			*oldsum = *oldsum + (int64) PG_GETARG_INT32(1);

		PG_RETURN_POINTER(oldsum);
	}
	else
#endif
	{
		int64		oldsum = PG_GETARG_INT64(0);

		/* Leave sum unchanged if new input is null. */
		if (PG_ARGISNULL(1))
			PG_RETURN_INT64(oldsum);

		/* OK to do the addition. */
		newval = oldsum + (int64) PG_GETARG_INT32(1);

		PG_RETURN_INT64(newval);
	}
}

/*
 * Note: this function is obsolete, it's no longer used for SUM(int8).
 */
Datum
int8_sum(PG_FUNCTION_ARGS)
{
	Numeric		oldsum;
	Datum		newval;

	if (PG_ARGISNULL(0))
	{
		/* No non-null input seen so far... */
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();	/* still no non-null */
		/* This is the first non-null input. */
		newval = DirectFunctionCall1(int8_numeric, PG_GETARG_DATUM(1));
		PG_RETURN_DATUM(newval);
	}

	/*
	 * Note that we cannot special-case the aggregate case here, as we do for
	 * int2_sum and int4_sum: numeric is of variable size, so we cannot modify
	 * our first parameter in-place.
	 */

	oldsum = PG_GETARG_NUMERIC(0);

	/* Leave sum unchanged if new input is null. */
	if (PG_ARGISNULL(1))
		PG_RETURN_NUMERIC(oldsum);

	/* OK to do the addition. */
	newval = DirectFunctionCall1(int8_numeric, PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_add,
										NumericGetDatum(oldsum), newval));
}


/*
 * Routines for avg(int2) and avg(int4).  The transition datatype
 * is a two-element int8 array, holding count and sum.
 *
 * These functions are also used for sum(int2) and sum(int4) when
 * operating in moving-aggregate mode, since for correct inverse transitions
 * we need to count the inputs.
 */

typedef struct Int8TransTypeData
{
	int64		count;
	int64		sum;
} Int8TransTypeData;

Datum
int2_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int16		newval = PG_GETARG_INT16(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count++;
	transdata->sum += newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int4_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int32		newval = PG_GETARG_INT32(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count++;
	transdata->sum += newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int4_avg_combine(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray1;
	ArrayType  *transarray2;
	Int8TransTypeData *state1;
	Int8TransTypeData *state2;

	if (!AggCheckCallContext(fcinfo, NULL))
		elog(ERROR, "aggregate function called in non-aggregate context");

	transarray1 = PG_GETARG_ARRAYTYPE_P(0);
	transarray2 = PG_GETARG_ARRAYTYPE_P(1);

	if (ARR_HASNULL(transarray1) ||
		ARR_SIZE(transarray1) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	if (ARR_HASNULL(transarray2) ||
		ARR_SIZE(transarray2) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	state1 = (Int8TransTypeData *) ARR_DATA_PTR(transarray1);
	state2 = (Int8TransTypeData *) ARR_DATA_PTR(transarray2);

	state1->count += state2->count;
	state1->sum += state2->sum;

	PG_RETURN_ARRAYTYPE_P(transarray1);
}

Datum
int2_avg_accum_inv(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int16		newval = PG_GETARG_INT16(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count--;
	transdata->sum -= newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int4_avg_accum_inv(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	int32		newval = PG_GETARG_INT32(1);
	Int8TransTypeData *transdata;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we need to make
	 * a copy of it before scribbling on it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
		transarray = PG_GETARG_ARRAYTYPE_P(0);
	else
		transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");

	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
	transdata->count--;
	transdata->sum -= newval;

	PG_RETURN_ARRAYTYPE_P(transarray);
}

Datum
int8_avg(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Int8TransTypeData *transdata;
	Datum		countd,
				sumd;

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");
	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

	/* SQL defines AVG of no values to be NULL */
	if (transdata->count == 0)
		PG_RETURN_NULL();

	countd = DirectFunctionCall1(int8_numeric,
								 Int64GetDatumFast(transdata->count));
	sumd = DirectFunctionCall1(int8_numeric,
							   Int64GetDatumFast(transdata->sum));

	PG_RETURN_DATUM(DirectFunctionCall2(numeric_div, sumd, countd));
}

/*
 * SUM(int2) and SUM(int4) both return int8, so we can use this
 * final function for both.
 */
Datum
int2int4_sum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	Int8TransTypeData *transdata;

	if (ARR_HASNULL(transarray) ||
		ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
		elog(ERROR, "expected 2-element int8 array");
	transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

	/* SQL defines SUM of no values to be NULL */
	if (transdata->count == 0)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(Int64GetDatumFast(transdata->sum));
}


/* ----------------------------------------------------------------------
 *
 * Debug support
 *
 * ----------------------------------------------------------------------
 */

#ifdef NUMERIC_DEBUG

/*
 * dump_numeric() - Dump a value in the db storage format for debugging
 */
static void
dump_numeric(const char *str, Numeric num)
{
	NumericDigit *digits = NUMERIC_DIGITS(num);
	int			ndigits;
	int			i;

	ndigits = NUMERIC_NDIGITS(num);

	printf("%s: NUMERIC w=%d d=%d ", str,
		   NUMERIC_WEIGHT(num), NUMERIC_DSCALE(num));
	switch (NUMERIC_SIGN(num))
	{
		case NUMERIC_POS:
			printf("POS");
			break;
		case NUMERIC_NEG:
			printf("NEG");
			break;
		case NUMERIC_NAN:
			printf("NaN");
			break;
		default:
			printf("SIGN=0x%x", NUMERIC_SIGN(num));
			break;
	}

	for (i = 0; i < ndigits; i++)
		printf(" %0*d", DEC_DIGITS, digits[i]);
	printf("\n");
}


/*
 * dump_var() - Dump a value in the variable format for debugging
 */
static void
dump_var(const char *str, NumericVar *var)
{
	int			i;

	printf("%s: VAR w=%d d=%d ", str, var->weight, var->dscale);
	switch (var->sign)
	{
		case NUMERIC_POS:
			printf("POS");
			break;
		case NUMERIC_NEG:
			printf("NEG");
			break;
		case NUMERIC_NAN:
			printf("NaN");
			break;
		default:
			printf("SIGN=0x%x", var->sign);
			break;
	}

	for (i = 0; i < var->ndigits; i++)
		printf(" %0*d", DEC_DIGITS, var->digits[i]);

	printf("\n");
}
#endif							/* NUMERIC_DEBUG */


/* ----------------------------------------------------------------------
 *
 * Local functions follow
 *
 * In general, these do not support NaNs --- callers must eliminate
 * the possibility of NaN first.  (make_result() is an exception.)
 *
 * ----------------------------------------------------------------------
 */


/*
 * alloc_var() -
 *
 *	Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 */
static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var);
	init_alloc_var(var, ndigits);
}


/*
 * zero_var() -
 *
 *	Set a variable to ZERO.
 *	Note: its dscale is not touched.
 */
static void
zero_var(NumericVar *var)
{
	digitbuf_free(var);
	quick_init_var(var);
	var->ndigits = 0;
	var->subtype = ORA_MODE ? NUMERIC_NUMBER : NUMERIC_NUMERIC;
	var->weight = 0;			/* by convention; doesn't really matter */
	var->sign = NUMERIC_POS;	/* anything but NAN... */
}


/*
 * set_var_from_str()
 *
 *	Parse a string and put the number into a variable
 *
 * This function does not handle leading or trailing spaces, and it doesn't
 * accept "NaN" either.  It returns the end+1 position so that caller can
 * check for trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  (Typically cp would be the same except advanced over spaces.)
 */
static const char *
set_var_from_str(const char *str, const char *cp, NumericVar *dest)
{
	bool		have_dp = FALSE;
	int			i;
	unsigned char *decdigits;
	int			sign = NUMERIC_POS;
	int			dweight = -1;
	int			ddigits;
	int			dscale = 0;
	int			weight;
	int			ndigits;
	int			offset;
	NumericDigit *digits;
	unsigned char tdd[NUMERIC_LOCAL_DTXT];

	/*
	 * We first parse the string to extract decimal digits and determine the
	 * correct decimal weight.  Then convert to NBASE representation.
	 */
	switch (*cp)
	{
		case '+':
			sign = NUMERIC_POS;
			cp++;
			break;

		case '-':
			sign = NUMERIC_NEG;
			cp++;
			break;
	}

	if (*cp == '.')
	{
		have_dp = TRUE;
		cp++;
	}

	if (!isdigit((unsigned char) *cp))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"numeric", str)));

	decdigits = tdd;
	i = strlen(cp) + DEC_DIGITS * 2;
	if ( i > NUMERIC_LOCAL_DMAX)
		decdigits = (unsigned char *) palloc(i);

	/* leading padding for digit alignment later */
	memset(decdigits, 0, DEC_DIGITS);
	i = DEC_DIGITS;

	while (*cp)
	{
		if (isdigit((unsigned char) *cp))
		{
			decdigits[i++] = *cp++ - '0';
			if (!have_dp)
				dweight++;
			else
				dscale++;
		}
		else if (*cp == '.')
		{
			if (have_dp)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						 errmsg("invalid input syntax for type %s: \"%s\"",
								"numeric", str)));
			have_dp = TRUE;
			cp++;
		}
		else
			break;
	}

	ddigits = i - DEC_DIGITS;
	/* trailing padding for digit alignment later */
	memset(decdigits + i, 0, DEC_DIGITS - 1);

	/* Handle exponent, if any */
	if (*cp == 'e' || *cp == 'E')
	{
		long		exponent;
		char	   *endptr;

		cp++;
		exponent = strtol(cp, &endptr, 10);
		if (endptr == cp)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s: \"%s\"",
							"numeric", str)));
		cp = endptr;

		/*
		 * At this point, dweight and dscale can't be more than about
		 * INT_MAX/2 due to the MaxAllocSize limit on string length, so
		 * constraining the exponent similarly should be enough to prevent
		 * integer overflow in this function.  If the value is too large to
		 * fit in storage format, make_result() will complain about it later;
		 * for consistency use the same ereport errcode/text as make_result().
		 */
		if (exponent >= INT_MAX / 2 || exponent <= -(INT_MAX / 2))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		dweight += (int) exponent;
		dscale -= (int) exponent;
		if (dscale < 0)
			dscale = 0;
	}

	/*
	 * Okay, convert pure-decimal representation to base NBASE.  First we need
	 * to determine the converted weight and ndigits.  offset is the number of
	 * decimal zeroes to insert before the first given digit to have a
	 * correctly aligned first NBASE digit.
	 */
	if (dweight >= 0)
		weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
	else
		weight = -((-dweight - 1) / DEC_DIGITS + 1);
	offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
	ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

	init_alloc_var(dest, ndigits);

	dest->subtype = ORA_MODE ? NUMERIC_NUMBER : NUMERIC_NUMERIC;
	dest->sign = sign;
	dest->weight = weight;
	dest->dscale = dscale;

	i = DEC_DIGITS - offset;
	digits = dest->digits;

	while (ndigits-- > 0)
	{
#if DEC_DIGITS == 4
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2]) * 10 + decdigits[i + 3];
#elif DEC_DIGITS == 2
		*digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif DEC_DIGITS == 1
		*digits++ = decdigits[i];
#else
#error unsupported NBASE
#endif
		i += DEC_DIGITS;
	}

	if (decdigits != tdd)
		pfree(decdigits);

	if (ORA_MODE)
		/* Strip any leading/trailing zeroes, and normalize weight if zero */
		strip_var(dest);
	
	/* Return end+1 position for caller */
	return cp;
}


/*
 * set_var_from_num() -
 *
 *	Convert the packed db format into a variable
 */
static void
set_var_from_num(Numeric num, NumericVar *dest)
{
	int			ndigits;

	ndigits = NUMERIC_NDIGITS(num);

	alloc_var(dest, ndigits);

	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE_NORMAL(num);

	memcpy(dest->digits, NUMERIC_DIGITS(num), ndigits * sizeof(NumericDigit));
}


/*
 * init_var_from_num() -
 *
 *	Initialize a variable from packed db format. The digits array is not
 *	copied, which saves some cycles when the resulting var is not modified.
 *	Also, there's no need to call free_var(), as long as you don't assign any
 *	other value to it (with set_var_* functions, or by using the var as the
 *	destination of a function like add_var())
 *
 *	CAUTION: Do not modify the digits buffer of a var initialized with this
 *	function, e.g by calling round_var() or trunc_var(), as the changes will
 *	propagate to the original Numeric! It's OK to use it as the destination
 *	argument of one of the calculational functions, though.
 */
void
init_var_from_num(Numeric num, NumericVar *dest)
{
	dest->ndigits = NUMERIC_NDIGITS(num);
	dest->weight = NUMERIC_WEIGHT(num);
	dest->sign = NUMERIC_SIGN(num);
	dest->dscale = NUMERIC_DSCALE_NORMAL(num);
	dest->digits = NUMERIC_DIGITS(num);
	dest->subtype = ORA_MODE ? NUMERIC_NUMBER : NUMERIC_NUMERIC;
	dest->buf = dest->ndb;
}


/*
 * init_var_from_var() -
 *
 *	init one variable from another - they must NOT be the same variable
 */
static void
init_var_from_var(const NumericVar *value, NumericVar *dest)
{
	init_alloc_var(dest, value->ndigits);

	dest->weight = value->weight;
	dest->sign = value->sign;
	dest->dscale = value->dscale;
	dest->subtype = value->subtype;

	memcpy(dest->digits, value->digits, value->ndigits * sizeof(NumericDigit));
}

/*
 * set_var_from_var() -
 *
 *	Copy one variable into another
 */
static void
set_var_from_var(const NumericVar *value, NumericVar *dest)
{
	NumericDigit *newbuf;

	newbuf = digitbuf_alloc(value->ndigits + 1);
	newbuf[0] = 0;				/* spare digit for rounding */
	if (value->ndigits > 0)		/* else value->digits might be null */
		memcpy(newbuf + 1, value->digits,
			   value->ndigits * sizeof(NumericDigit));

	digitbuf_free(dest);

	memmove(dest, value, sizeof(NumericVar));
	dest->buf = newbuf;
	dest->digits = newbuf + 1;
}


/*
 * get_str_from_var() -
 *
 *	Convert a var to text representation (guts of numeric_out).
 *	The var is displayed to the number of digits indicated by its dscale.
 *	Returns a palloc'd string.
 */
static char *
get_str_from_var(const NumericVar *var)
{
	int			dscale;
	char	   *str;
	char	   *cp;
	char	   *endcp;
	int			i;
	int			d;
	NumericDigit dig;

#if DEC_DIGITS > 1
	NumericDigit d1;
#endif

	dscale = var->dscale;

	/*
	 * the scale is below than 0, do not output decimal values
	 */
	if (ORA_MODE && dscale > NUMERIC_MAX_PRECISION)
		dscale = 0;
	
	/*
	 * Allocate space for the result.
	 *
	 * i is set to the # of decimal digits before decimal point. dscale is the
	 * # of decimal digits we will print after decimal point. We may generate
	 * as many as DEC_DIGITS-1 excess digits at the end, and in addition we
	 * need room for sign, decimal point, null terminator.
	 */
	i = (var->weight + 1) * DEC_DIGITS;
	if (i <= 0)
		i = 1;

	if(dscale > 0)
		str = palloc(i + dscale + DEC_DIGITS + 2);
	else
		str = palloc(i + DEC_DIGITS + 2);
	cp = str;

	/*
	 * Output a dash for negative values
	 */
	if (var->sign == NUMERIC_NEG)
		*cp++ = '-';

	/*
	 * Output all digits before the decimal point
	 */
	if (var->weight < 0)
	{
		d = var->weight + 1;
		*cp++ = '0';
	}
	else
	{
		for (d = 0; d <= var->weight; d++)
		{
			dig = (d < var->ndigits) ? var->digits[d] : 0;
			/* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
			{
				bool		putit = (d > 0);

				d1 = dig / 1000;
				dig -= d1 * 1000;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 100;
				dig -= d1 * 100;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';
				d1 = dig / 10;
				dig -= d1 * 10;
				putit |= (d1 > 0);
				if (putit)
					*cp++ = d1 + '0';

				*cp++ = dig + '0';
			}
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			if (d1 > 0 || d > 0)
				*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
	}

	/*
	 * If requested, output a decimal point and all the digits that follow it.
	 * We initially put out a multiple of DEC_DIGITS digits, then truncate if
	 * needed.
	 */
	if (dscale > 0)
	{
		*cp++ = '.';
		endcp = cp + dscale;
		for (i = 0; i < dscale; d++, i += DEC_DIGITS)
		{
			dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
			d1 = dig / 1000;
			dig -= d1 * 1000;
			*cp++ = d1 + '0';
			d1 = dig / 100;
			dig -= d1 * 100;
			*cp++ = d1 + '0';
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 2
			d1 = dig / 10;
			dig -= d1 * 10;
			*cp++ = d1 + '0';
			*cp++ = dig + '0';
#elif DEC_DIGITS == 1
			*cp++ = dig + '0';
#else
#error unsupported NBASE
#endif
		}
		cp = endcp;
	}

	/*
	 * terminate the string and return it
	 */
	*cp = '\0';
	return str;
}

/*
 * get_str_from_var_sci() -
 *
 *	Convert a var to a normalised scientific notation text representation.
 *	This function does the heavy lifting for numeric_out_sci().
 *
 *	This notation has the general form a * 10^b, where a is known as the
 *	"significand" and b is known as the "exponent".
 *
 *	Because we can't do superscript in ASCII (and because we want to copy
 *	printf's behaviour) we display the exponent using E notation, with a
 *	minimum of two exponent digits.
 *
 *	For example, the value 1234 could be output as 1.2e+03.
 *
 *	We assume that the exponent can fit into an int32.
 *
 *	rscale is the number of decimal digits desired after the decimal point in
 *	the output, negative values will be treated as meaning zero.
 *
 *	Returns a palloc'd string.
 */
char *
get_str_from_var_sci(const NumericVar *var, int rscale)
{
	int32		exponent;
	NumericVar	denominator;
	NumericVar	significand;
	int			denom_scale;
	size_t		len;
	char	   *str;
	char	   *sig_out;

	if (rscale < 0)
		rscale = 0;

	/*
	 * Determine the exponent of this number in normalised form.
	 *
	 * This is the exponent required to represent the number with only one
	 * significant digit before the decimal place.
	 */
	if (var->ndigits > 0)
	{
		exponent = (var->weight + 1) * DEC_DIGITS;

		/*
		 * Compensate for leading decimal zeroes in the first numeric digit by
		 * decrementing the exponent.
		 */
		exponent -= DEC_DIGITS - (int) log10(var->digits[0]);
	}
	else
	{
		/*
		 * If var has no digits, then it must be zero.
		 *
		 * Zero doesn't technically have a meaningful exponent in normalised
		 * notation, but we just display the exponent as zero for consistency
		 * of output.
		 */
		exponent = 0;
	}

	/*
	 * The denominator is set to 10 raised to the power of the exponent.
	 *
	 * We then divide var by the denominator to get the significand, rounding
	 * to rscale decimal digits in the process.
	 */
	if (exponent < 0)
		denom_scale = -exponent;
	else
		denom_scale = 0;

	init_var(&denominator);
	init_var(&significand);

	power_var_int(&const_ten, exponent, &denominator, denom_scale);
	div_var(var, &denominator, &significand, rscale, HALF_ADJUST_ROUND);
	sig_out = get_str_from_var(&significand);

	if (ORA_MODE && significand.ndigits > 0 && significand.digits[0] > 9)
	{
		char *decimal_pt = strchr(sig_out, '.');

		if (decimal_pt != NULL)
		{
			decimal_pt[0] = sig_out[decimal_pt - sig_out - 1];
			sig_out[decimal_pt - sig_out - 1] = '.';
		}
		else
		{
			int sig_out_len = strlen(sig_out);
			sig_out[sig_out_len - 1] = '\0';
		}
		exponent = exponent + 1;
	}

	free_var(&denominator);
	free_var(&significand);

	/*
	 * Allocate space for the result.
	 *
	 * In addition to the significand, we need room for the exponent
	 * decoration ("e"), the sign of the exponent, up to 10 digits for the
	 * exponent itself, and of course the null terminator.
	 */
	len = strlen(sig_out) + 13;
	str = palloc(len);

	if (ORA_MODE)
		snprintf(str, len, "%sE%+03d", sig_out, exponent);
	else
		snprintf(str, len, "%se%+03d", sig_out, exponent);

	pfree(sig_out);

	return str;
}


/*
 * make_result_opt_error() -
 *
 *	Create the packed db numeric format in palloc()'d memory from
 *	a variable.  If "*have_error" flag is provided, on error it's set to
 *	true, NULL returned.  This is helpful when caller need to handle errors
 *	by itself.
 */
static Numeric
make_result_opt_error(NumericVar *var, bool *have_error)
{
	Numeric		result;
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	int			n;
	Size		len;

	if (sign == NUMERIC_NAN || sign == NUMERIC_POS_INF || sign == NUMERIC_NEG_INF)
	{
		result = (Numeric) palloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = sign;
		/* the header word is all we need */

		dump_numeric("make_result()", result);
		return result;
	}

	/*
	 * The precision of numeric compatible with the opentenbase_ora database, i.e. if the
	 * total number of significant digits exceeds 40, it will be rounded, and
	 * only 39/40 significant digits will be reserved.
	 */
	if (ORA_MODE)
	{
		if (adjust_scale_ora(var))
		{
			round_var(var, var->dscale, HALF_ADJUST_ROUND);
			digits = var->digits;
			weight = var->weight;
			sign = var->sign;
			if ((var->dscale < 0 && var->weight > 0) || var->ndigits == 0)
				var->dscale = 0;
		}
	}
	n = var->ndigits;

	/* Build Inf and -Inf result.
	 * var maybe set to Inf or -Inf in adjust_scale_ora if numeric is overflow.
	 */
	if (var->sign == NUMERIC_POS_INF || var->sign == NUMERIC_NEG_INF)
	{
		result = (Numeric) palloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = var->sign;
		/* the header word is all we need */

		dump_numeric("make_result()", result);
		return result;
	}
	/*
	 * var maybe reset to a Inf or -Inf value in adjust_scale_ora.
	 * we need to retrieve the variable value of var.
	 */
	digits = var->digits;
	weight = var->weight;
	sign = var->sign;
	/* truncate leading zeroes */
	while (n > 0 && *digits == 0)
	{
		digits++;
		weight--;
		n--;
	}
	/* truncate trailing zeroes */
	while (n > 0 && digits[n - 1] == 0)
		n--;

	/* If zero result, force to weight=0 and positive sign */
	if (n == 0)
	{
		weight = 0;
		sign = NUMERIC_POS;
	}

	/* Build the result */
	if (NUMERIC_CAN_BE_SHORT(var->dscale, weight))
	{
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
			 : NUMERIC_SHORT)
			| (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
			| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
			| (weight & NUMERIC_SHORT_WEIGHT_MASK);
	}
	else
	{
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		result = (Numeric) palloc(len);
		SET_VARSIZE(result, len);
		if (var->dscale < 0)
		{
			result->choice.n_long.n_sign_dscale =
				sign | ((var->dscale * (-1) + NUMERIC_MAX_PRECISION) & NUMERIC_DSCALE_MASK);
		}
		else
			result->choice.n_long.n_sign_dscale = sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	Assert(NUMERIC_NDIGITS(result) == n);
	if (n > 0)
		memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT(result) != weight ||
		NUMERIC_DSCALE_NORMAL(result) != var->dscale)
	{
		if (have_error)
		{
			*have_error = true;
			return NULL;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("value overflows numeric format")));
		}
	}

	dump_numeric("make_result()", result);
	return result;
}


/*
 * make_result() -
 *
 *	An interface to make_result_opt_error() without "have_error" argument.
 */
Numeric
make_result(const NumericVar *var)
{
	return make_result_opt_error((NumericVar *) var, NULL);
}

/*
 * typmod_to_numeric_subtype() -
 *
 *	Judge what's the Numeric represents for by the typmod value in opentenbase_ora mode.
 */
static NumericSubtype
typmod_to_numeric_subtype(int32 typmod)
{
    NumericSubtype res;
    int	precision;
    /* we assume this Numeric represents for a number type in OPENTENBASE_ORA at first */
    res = NUMERIC_NUMBER;

    /* 1. typmod < VARHDRSZ: i.e. typmod = -1, NUMBER
     * 2. typmod > VARHDRSZ:
     *      2.1. precision <= NUMERIC_MAX_PRECISION: NUMBER
     *      2.2. precision <= NUMERIC_MAX_PRECISION + FLOAT_OPENTENBASE_ORA_MAX_PRECISION: FLOAT
     *      2.3. precision <= NUMERIC_MAX_PRECISION + OPENTENBASE_ORA_BINARY_FLOAT_PRECISION: BINARY_FLOAT
     *      2.3. precision <= NUMERIC_MAX_PRECISION + OPENTENBASE_ORA_BINARY_FLOAT_PRECISION: BINARY_DOUBLE
     *      */
    if (typmod > (int32) (VARHDRSZ))
    {
        typmod -= VARHDRSZ;
        precision = (typmod >> 16) & 0xffff;
        if (precision > NUMERIC_MAX_PRECISION)
        {
            if (precision == (NUMERIC_MAX_PRECISION + OPENTENBASE_ORA_BINARY_FLOAT_PRECISION))
                res = NUMERIC_BINARY_FLOAT;
            else if (precision == (NUMERIC_MAX_PRECISION + OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION))
                res = NUMERIC_BINARY_DOUBLE;
            else
                res = NUMERIC_FLOAT;
        }
    }

    return  res;
}
/*
 * apply_typmod() -
 *
 *	Do bounds checking and rounding according to the attributes
 *	typmod field.
 */
static void
apply_typmod(NumericVar *var, int32 typmod)
{
	int			precision;
	int			scale;
	int			maxdigits = 0;
	int			ddigits;
	int			i;
	int			precision_real;
	int			scale_real;
	int			dec_digits = 4;

	var->subtype = NUMERIC_NUMBER;
	/* Do nothing if we have a default typmod (-1) */
	if (typmod < (int32) (VARHDRSZ))
		return;

	if (ORA_MODE)
		var->subtype = typmod_to_numeric_subtype(typmod);

	typmod -= VARHDRSZ;
	precision = (typmod >> 16) & 0xffff;
	scale = typmod & 0xffff;
	scale_real = scale;

/*
 * precompile could not be in nest
 */
#if DEC_DIGITS == 4
	dec_digits = 4;
#elif DEC_DIGITS == 2
	dec_digits = 2;
#elif DEC_DIGITS == 1
	dec_digits = 1;
#else
#error unsupported NBASE
#endif

#ifdef _PG_ORCL_
	if (ORA_MODE)
	{
		if (scale > NUMERIC_MAX_PRECISION)
		{
			/*
			 * If scale is smaller than 0, the scale is saved as 
			 *	scale_saved = scale_real * (-1) + NUMERIC_MAX_PRECISION
			 *
			 * When calculate the real scale as (VARHDRSZ already substract)
			 * 	scale_real = (scale_saved - NUMERIC_MAX_PRECISION) * (-1)
			 */
			scale_real = (scale_real - NUMERIC_MAX_PRECISION) * (-1);
			scale =  scale_real;
		}
		else if (precision > NUMERIC_MAX_PRECISION)
		{
			/*
			 * If precision > NUMERIC_MAX_PRECISION, the precision is absoluted.
			 */
			precision_real = precision - NUMERIC_MAX_PRECISION;
			if (precision_real == OPENTENBASE_ORA_BINARY_FLOAT_PRECISION || precision_real == OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION)
				precision_real = precision_real - NUMERIC_MAX_PRECISION;

			ddigits = 0;
			/*
			 * Check number of valid digits.
			 */
			if (var->weight >= 0 && var->digits[0] != 0)
			{			
				ddigits = (var->weight + 1) * DEC_DIGITS;

				if(dec_digits == 4)
				{
					if (var->digits[0] < 10)
						ddigits -= 3;
					else if (var->digits[0] < 100)
						ddigits -= 2;
					else if (var->digits[0] < 1000)
						ddigits -= 1;
				}
				else if(dec_digits == 2)
				{
					if (var->digits[0] < 10)
						ddigits -= 1;
				}
				
				scale_real = precision_real - ddigits;

				if (scale_real > var->dscale)
				{
					scale_real = var->dscale;
				}
			}
			else
			{
				ddigits = var->dscale + (var->weight + 1) * DEC_DIGITS;

				for (i =0; i < var->ndigits; i++)
				{
					if(dec_digits == 4)
					{
						if (var->digits[i] == 0)
							ddigits = ddigits - 4;
						else if (var->digits[i] < 10)
							ddigits = ddigits - 3;
						else if (var->digits[i] < 100)
							ddigits = ddigits - 2;
						else if (var->digits[i] < 1000)
							ddigits = ddigits - 1;
					}
					else if(dec_digits == 2)
					{
						if (var->digits[i] == 0)
							ddigits = ddigits - 2;
						else if (var->digits[i] < 10)
							ddigits = ddigits - 1;
					}
					else if(dec_digits == 1)
					{
						if (var->digits[i] == 0)
							ddigits = ddigits - 1;
					}
					
					if (var->digits[i] > 0)
						break;
				}

				if (ddigits > precision_real)
					scale_real = var->dscale - (ddigits - precision_real);
				else
					scale_real = var->dscale;
			}
		}
	}
#endif

	if (precision <= NUMERIC_MAX_PRECISION)
	{
		maxdigits = precision - scale_real;
	}

	/* Round to target scale (and set var->dscale) */
	round_var(var, scale_real, HALF_ADJUST_ROUND);

	/*
	 * Check for overflow - note we can't do this before rounding, because
	 * rounding could raise the weight.  Also note that the var's weight could
	 * be inflated by leading zeroes, which will be stripped before storage
	 * but perhaps might not have been yet. In any case, we must recognize a
	 * true zero, whose weight doesn't mean anything.
	 */
	ddigits = (var->weight + 1) * DEC_DIGITS;
	if (precision <= NUMERIC_MAX_PRECISION && ddigits > maxdigits)
	{
		/* Determine true weight; and check for all-zero result */
		for (i = 0; i < var->ndigits; i++)
		{
			NumericDigit dig = var->digits[i];

			if (dig)
			{
				/* Adjust for any high-order decimal zero digits */
#if DEC_DIGITS == 4
				if (dig < 10)
					ddigits -= 3;
				else if (dig < 100)
					ddigits -= 2;
				else if (dig < 1000)
					ddigits -= 1;
#elif DEC_DIGITS == 2
				if (dig < 10)
					ddigits -= 1;
#elif DEC_DIGITS == 1
				/* no adjustment */
#else
#error unsupported NBASE
#endif
				if (ddigits > maxdigits)
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("numeric field overflow"),
							 errdetail("A field with precision %d, scale %d must round to an absolute value less than %s%d.",
									   precision, scale,
					/* Display 10^0 as 1 */
									   maxdigits ? "10^" : "",
									   maxdigits ? maxdigits : 1
									   )));
				break;
			}
			ddigits -= DEC_DIGITS;
		}
	}
}

/*
 * Convert numeric to int8, rounding if needed.
 *
 * If overflow, return FALSE (no error is raised).  Return TRUE if okay.
 */
bool
numericvar_to_int64(const NumericVar *var, int64 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	int64		val;
	bool		neg;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0, HALF_ADJUST_ROUND);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/*
	 * Construct the result. To avoid issues with converting a value
	 * corresponding to INT64_MIN (which can't be represented as a positive 64
	 * bit two's complement integer), accumulate value as a negative number.
	 */
	digits = rounded.digits;
	neg = (rounded.sign == NUMERIC_NEG);
	val = -digits[0];
	for (i = 1; i <= weight; i++)
	{
		if (unlikely(pg_mul_s64_overflow(val, NBASE, &val)))
		{
			free_var(&rounded);
			return false;
		}

		if (i < ndigits)
		{
			if (unlikely(pg_sub_s64_overflow(val, digits[i], &val)))
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	if (!neg)
	{
		if (unlikely(val == INT64_MIN))
			return false;
		val = -val;
	}
	*result = val;

	return true;
}

/*
 * Convert int8 value to numeric.
 */
static void
int64_to_numericvar(int64 val, NumericVar *var)
{
	uint64		uval,
				newuval;
	NumericDigit *ptr;
	int			ndigits;

	/* int64 can require at most 19 decimal digits; add one for safety */
	alloc_var(var, 20 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = -val;
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}

/*
 * Convert numeric to int128, rounding if needed.
 *
 * If overflow, return FALSE (no error is raised).  Return TRUE if okay.
 */
static bool
numericvar_to_int128(const NumericVar *var, int128 *result)
{
	NumericDigit *digits;
	int			ndigits;
	int			weight;
	int			i;
	int128		val,
				oldval;
	bool		neg;
	NumericVar	rounded;

	/* Round to nearest integer */
	init_var(&rounded);
	set_var_from_var(var, &rounded);
	round_var(&rounded, 0, HALF_ADJUST_ROUND);

	/* Check for zero input */
	strip_var(&rounded);
	ndigits = rounded.ndigits;
	if (ndigits == 0)
	{
		*result = 0;
		free_var(&rounded);
		return true;
	}

	/*
	 * For input like 10000000000, we must treat stripped digits as real. So
	 * the loop assumes there are weight+1 digits before the decimal point.
	 */
	weight = rounded.weight;
	Assert(weight >= 0 && ndigits <= weight + 1);

	/* Construct the result */
	digits = rounded.digits;
	neg = (rounded.sign == NUMERIC_NEG);
	val = digits[0];
	for (i = 1; i <= weight; i++)
	{
		oldval = val;
		val *= NBASE;
		if (i < ndigits)
			val += digits[i];

		/*
		 * The overflow check is a bit tricky because we want to accept
		 * INT128_MIN, which will overflow the positive accumulator.  We can
		 * detect this case easily though because INT128_MIN is the only
		 * nonzero value for which -val == val (on a two's complement machine,
		 * anyway).
		 */
		if ((val / NBASE) != oldval)	/* possible overflow? */
		{
			if (!neg || (-val) != val || val == 0 || oldval < 0)
			{
				free_var(&rounded);
				return false;
			}
		}
	}

	free_var(&rounded);

	*result = neg ? -val : val;
	return true;
}

#ifdef HAVE_INT128
/*
 * Convert 128 bit integer to numeric.
 */
static void
int128_to_numericvar(int128 val, NumericVar *var)
{
	uint128		uval,
				newuval;
	NumericDigit *ptr;
	int			ndigits;

	/* int128 can require at most 39 decimal digits; add one for safety */
	alloc_var(var, 40 / DEC_DIGITS);
	if (val < 0)
	{
		var->sign = NUMERIC_NEG;
		uval = -val;
	}
	else
	{
		var->sign = NUMERIC_POS;
		uval = val;
	}
	var->dscale = 0;
	if (val == 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		return;
	}
	ptr = var->digits + var->ndigits;
	ndigits = 0;
	do
	{
		ptr--;
		ndigits++;
		newuval = uval / NBASE;
		*ptr = uval - newuval * NBASE;
		uval = newuval;
	} while (uval);
	var->digits = ptr;
	var->ndigits = ndigits;
	var->weight = ndigits - 1;
}
#endif

/*
 * Convert numeric to float8; if out of range, return +/- HUGE_VAL
 */
static double
numeric_to_double_no_overflow(Numeric num)
{
	char	   *tmp;
	double		val;
	char	   *endptr;

	tmp = DatumGetCString(DirectFunctionCall1(numeric_out,
											  NumericGetDatum(num)));

	/* unlike float8in, we ignore ERANGE from strtod */
	val = strtod(tmp, &endptr);
	if (*endptr != '\0')
	{
		/* shouldn't happen ... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"double precision", tmp)));
	}

	pfree(tmp);

	return val;
}

/* As above, but work from a NumericVar */
static double
numericvar_to_double_no_overflow(const NumericVar *var)
{
	char	   *tmp;
	double		val;
	char	   *endptr;

	tmp = get_str_from_var(var);

	/* unlike float8in, we ignore ERANGE from strtod */
	val = strtod(tmp, &endptr);
	if (*endptr != '\0')
	{
		/* shouldn't happen ... */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"double precision", tmp)));
	}

	pfree(tmp);

	return val;
}


/*
 * cmp_var() -
 *
 *	Compare two values on variable level.  We assume zeroes have been
 *	truncated to no digits.
 */
static int
cmp_var(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_var_common(var1->digits, var1->ndigits,
						  var1->weight, var1->sign,
						  var2->digits, var2->ndigits,
						  var2->weight, var2->sign);
}

/*
 * cmp_var_common() -
 *
 *	Main routine of cmp_var(). This function can be used by both
 *	NumericVar and Numeric.
 */
static int
cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
			   int var1weight, int var1sign,
			   const NumericDigit *var2digits, int var2ndigits,
			   int var2weight, int var2sign)
{
	if (var1ndigits == 0)
	{
		if (var2ndigits == 0)
			return 0;
		if (var2sign == NUMERIC_NEG)
			return 1;
		return -1;
	}
	if (var2ndigits == 0)
	{
		if (var1sign == NUMERIC_POS)
			return 1;
		return -1;
	}

	if (var1sign == NUMERIC_POS)
	{
		if (var2sign == NUMERIC_NEG)
			return 1;
		return cmp_abs_common(var1digits, var1ndigits, var1weight,
							  var2digits, var2ndigits, var2weight);
	}

	if (var2sign == NUMERIC_POS)
		return -1;

	return cmp_abs_common(var2digits, var2ndigits, var2weight,
						  var1digits, var1ndigits, var1weight);
}


/*
 * add_var() -
 *
 *	Full version of add functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
 */
static void
add_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_POS)
		{
			/*
			 * Both are positive result = +(ABS(var1) + ABS(var2))
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/*
			 * var1 is positive, var2 is negative Must compare absolute values
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_POS)
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * Both are negative
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}


/*
 * sub_var() -
 *
 *	Full version of sub functionality on variable level (handling signs).
 *	result might point to one of the operands too without danger.
 */
static void
sub_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	/*
	 * Decide on the signs of the two variables what to do
	 */
	if (var1->sign == NUMERIC_POS)
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * var1 is positive, var2 is negative
			 * result = +(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_POS;
		}
		else
		{
			/* ----------
			 * Both are positive
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = +(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_POS;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = -(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_NEG;
					break;
			}
		}
	}
	else
	{
		if (var2->sign == NUMERIC_NEG)
		{
			/* ----------
			 * Both are negative
			 * Must compare absolute values
			 * ----------
			 */
			switch (cmp_abs(var1, var2))
			{
				case 0:
					/* ----------
					 * ABS(var1) == ABS(var2)
					 * result = ZERO
					 * ----------
					 */
					zero_var(result);
					result->dscale = Max(var1->dscale, var2->dscale);
					break;

				case 1:
					/* ----------
					 * ABS(var1) > ABS(var2)
					 * result = -(ABS(var1) - ABS(var2))
					 * ----------
					 */
					sub_abs(var1, var2, result);
					result->sign = NUMERIC_NEG;
					break;

				case -1:
					/* ----------
					 * ABS(var1) < ABS(var2)
					 * result = +(ABS(var2) - ABS(var1))
					 * ----------
					 */
					sub_abs(var2, var1, result);
					result->sign = NUMERIC_POS;
					break;
			}
		}
		else
		{
			/* ----------
			 * var1 is negative, var2 is positive
			 * result = -(ABS(var1) + ABS(var2))
			 * ----------
			 */
			add_abs(var1, var2, result);
			result->sign = NUMERIC_NEG;
		}
	}
}


/*
 * mul_var() -
 *
 *	Multiplication on variable level. Product of var1 * var2 is stored
 *	in result.  Result is rounded to no more than rscale fractional digits.
 */
static void
mul_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
		int rscale)
{
	int			res_ndigits;
	int			res_sign;
	int			res_weight;
	int			maxdigits;
	int		   *dig;
	int			carry;
	int			maxdig;
	int			newdig;
	int			var1ndigits;
	int			var2ndigits;
	NumericDigit *var1digits;
	NumericDigit *var2digits;
	NumericDigit *res_digits;
	int			i,
				i1,
				i2;

	/*
	 * Arrange for var1 to be the shorter of the two numbers.  This improves
	 * performance because the inner multiplication loop is much simpler than
	 * the outer loop, so it's better to have a smaller number of iterations
	 * of the outer loop.  This also reduces the number of times that the
	 * accumulator array needs to be normalized.
	 */
	if (var1->ndigits > var2->ndigits)
	{
		const NumericVar *tmp = var1;

		var1 = var2;
		var2 = tmp;
	}

	/* copy these values into local vars for speed in inner loop */
	var1ndigits = var1->ndigits;
	var2ndigits = var2->ndigits;
	var1digits = var1->digits;
	var2digits = var2->digits;

	if (var1ndigits == 0 || var2ndigits == 0)
	{
		/* one or both inputs is zero; so is result */
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/* Determine result sign and (maximum possible) weight */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight + var2->weight + 2;

	/*
	 * Determine the number of result digits to compute.  If the exact result
	 * would have more than rscale fractional digits, truncate the computation
	 * with MUL_GUARD_DIGITS guard digits, i.e., ignore input digits that
	 * would only contribute to the right of that.  (This will give the exact
	 * rounded-to-rscale answer unless carries out of the ignored positions
	 * would have propagated through more than MUL_GUARD_DIGITS digits.)
	 *
	 * Note: an exact computation could not produce more than var1ndigits +
	 * var2ndigits digits, but we allocate one extra output digit in case
	 * rscale-driven rounding produces a carry out of the highest exact digit.
	 */
	res_ndigits = var1ndigits + var2ndigits + 1;
	maxdigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS +
		MUL_GUARD_DIGITS;
	res_ndigits = Min(res_ndigits, maxdigits);

	if (res_ndigits < 3)
	{
		/* All input digits will be ignored; so result is zero */
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * We do the arithmetic in an array "dig[]" of signed int's.  Since
	 * INT_MAX is noticeably larger than NBASE*NBASE, this gives us headroom
	 * to avoid normalizing carries immediately.
	 *
	 * maxdig tracks the maximum possible value of any dig[] entry; when this
	 * threatens to exceed INT_MAX, we take the time to propagate carries.
	 * Furthermore, we need to ensure that overflow doesn't occur during the
	 * carry propagation passes either.  The carry values could be as much as
	 * INT_MAX/NBASE, so really we must normalize when digits threaten to
	 * exceed INT_MAX - INT_MAX/NBASE.
	 *
	 * To avoid overflow in maxdig itself, it actually represents the max
	 * possible value divided by NBASE-1, ie, at the top of the loop it is
	 * known that no dig[] entry exceeds maxdig * (NBASE-1).
	 */
	dig = (int *) palloc0(res_ndigits * sizeof(int));
	maxdig = 0;

	/*
	 * The least significant digits of var1 should be ignored if they don't
	 * contribute directly to the first res_ndigits digits of the result that
	 * we are computing.
	 *
	 * Digit i1 of var1 and digit i2 of var2 are multiplied and added to digit
	 * i1+i2+2 of the accumulator array, so we need only consider digits of
	 * var1 for which i1 <= res_ndigits - 3.
	 */
	for (i1 = Min(var1ndigits - 1, res_ndigits - 3); i1 >= 0; i1--)
	{
		NumericDigit var1digit = var1digits[i1];

		if (var1digit == 0)
			continue;

		/* Time to normalize? */
		maxdig += var1digit;
		if (maxdig > (INT_MAX - INT_MAX / NBASE) / (NBASE - 1))
		{
			/* Yes, do it */
			carry = 0;
			for (i = res_ndigits - 1; i >= 0; i--)
			{
				newdig = dig[i] + carry;
				if (newdig >= NBASE)
				{
					carry = newdig / NBASE;
					newdig -= carry * NBASE;
				}
				else
					carry = 0;
				dig[i] = newdig;
			}
			Assert(carry == 0);
			/* Reset maxdig to indicate new worst-case */
			maxdig = 1 + var1digit;
		}

		/*
		 * Add the appropriate multiple of var2 into the accumulator.
		 *
		 * As above, digits of var2 can be ignored if they don't contribute,
		 * so we only include digits for which i1+i2+2 <= res_ndigits - 1.
		 */
		for (i2 = Min(var2ndigits - 1, res_ndigits - i1 - 3), i = i1 + i2 + 2;
			 i2 >= 0; i2--)
			dig[i--] += var1digit * var2digits[i2];
	}

	/*
	 * Now we do a final carry propagation pass to normalize the result, which
	 * we combine with storing the result digits into the output. Note that
	 * this is still done at full precision w/guard digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;
	carry = 0;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE)
		{
			carry = newdig / NBASE;
			newdig -= carry * NBASE;
		}
		else
			carry = 0;
		res_digits[i] = newdig;
	}
	Assert(carry == 0);

	pfree(dig);

	/*
	 * Finally, round the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale, HALF_ADJUST_ROUND);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}


/*
 * div_var() -
 *
 *	Division on variable level. Quotient of var1 / var2 is stored in result.
 *	The quotient is figured to exactly rscale fractional digits.
 *	If round is true, it is rounded at the rscale'th digit; if false, it
 *	is truncated (towards zero) at that digit.
 */
static void
div_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result, int rscale, RoundType rt)
{
	int			div_ndigits;
	int			res_ndigits;
	int			res_sign;
	int			res_weight;
	int			carry;
	int			borrow;
	int			divisor1;
	int			divisor2;
	NumericDigit *dividend;
	NumericDigit *divisor;
	NumericDigit *res_digits;
	int			i;
	int			j;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;

	/*
	 * First of all division by zero check; we must not be handed an
	 * unnormalized divisor.
	 */
	if (var2ndigits == 0 || var2->digits[0] == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * If the divisor has just one or two digits, delegate to div_var_int(),
	 * which uses fast short division.
	 */
	if (var2ndigits <= 2)
	{
		int			idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		if (var2ndigits == 2)
		{
			idivisor = idivisor * NBASE + var2->digits[1];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int(var1, idivisor, idivisor_weight, result, rscale, rt, true);
		return;
	}

	/*
	 * Otherwise, perform full long division.
	 */

	/* Result zero check */
	if (var1ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight - var2->weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (rt != TRUNCATE_ROUND && rt != TRUNCATE_NUM)
		res_ndigits++;

	/*
	 * The working dividend normally requires res_ndigits + var2ndigits
	 * digits, but make it at least var1ndigits so we can load all of var1
	 * into it.  (There will be an additional digit dividend[0] in the
	 * dividend space, but for consistency with Knuth's notation we don't
	 * count that in div_ndigits.)
	 */
	div_ndigits = res_ndigits + var2ndigits;
	div_ndigits = Max(div_ndigits, var1ndigits);

	/*
	 * We need a workspace with room for the working dividend (div_ndigits+1
	 * digits) plus room for the possibly-normalized divisor (var2ndigits
	 * digits).  It is convenient also to have a zero at divisor[0] with the
	 * actual divisor data in divisor[1 .. var2ndigits].  Transferring the
	 * digits into the workspace also allows us to realloc the result (which
	 * might be the same as either input var) before we begin the main loop.
	 * Note that we use palloc0 to ensure that divisor[0], dividend[0], and
	 * any additional dividend positions beyond var1ndigits, start out 0.
	 */
	dividend = (NumericDigit *)
		palloc0((div_ndigits + var2ndigits + 2) * sizeof(NumericDigit));
	divisor = dividend + (div_ndigits + 1);
	memcpy(dividend + 1, var1->digits, var1ndigits * sizeof(NumericDigit));
	memcpy(divisor + 1, var2->digits, var2ndigits * sizeof(NumericDigit));

	/*
	 * Now we can realloc the result to hold the generated quotient digits.
	 */
	alloc_var(result, res_ndigits);
	res_digits = result->digits;

		/*
		 * The full multiple-place algorithm is taken from Knuth volume 2,
		 * Algorithm 4.3.1D.
		 *
		 * We need the first divisor digit to be >= NBASE/2.  If it isn't,
		 * make it so by scaling up both the divisor and dividend by the
		 * factor "d".  (The reason for allocating dividend[0] above is to
		 * leave room for possible carry here.)
		 */
		if (divisor[1] < HALF_NBASE)
		{
			int			d = NBASE / (divisor[1] + 1);

			carry = 0;
			for (i = var2ndigits; i > 0; i--)
			{
				carry += divisor[i] * d;
				divisor[i] = carry % NBASE;
				carry = carry / NBASE;
			}
			Assert(carry == 0);
			carry = 0;
			/* at this point only var1ndigits of dividend can be nonzero */
			for (i = var1ndigits; i >= 0; i--)
			{
				carry += dividend[i] * d;
				dividend[i] = carry % NBASE;
				carry = carry / NBASE;
			}
			Assert(carry == 0);
			Assert(divisor[1] >= HALF_NBASE);
		}
		/* First 2 divisor digits are used repeatedly in main loop */
		divisor1 = divisor[1];
		divisor2 = divisor[2];

		/*
		 * Begin the main loop.  Each iteration of this loop produces the j'th
		 * quotient digit by dividing dividend[j .. j + var2ndigits] by the
		 * divisor; this is essentially the same as the common manual
		 * procedure for long division.
		 */
		for (j = 0; j < res_ndigits; j++)
		{
			/* Estimate quotient digit from the first two dividend digits */
			int			next2digits = dividend[j] * NBASE + dividend[j + 1];
			int			qhat;

			/*
			 * If next2digits are 0, then quotient digit must be 0 and there's
			 * no need to adjust the working dividend.  It's worth testing
			 * here to fall out ASAP when processing trailing zeroes in a
			 * dividend.
			 */
			if (next2digits == 0)
			{
				res_digits[j] = 0;
				continue;
			}

			if (dividend[j] == divisor1)
				qhat = NBASE - 1;
			else
				qhat = next2digits / divisor1;

			/*
			 * Adjust quotient digit if it's too large.  Knuth proves that
			 * after this step, the quotient digit will be either correct or
			 * just one too large.  (Note: it's OK to use dividend[j+2] here
			 * because we know the divisor length is at least 2.)
			 */
			while (divisor2 * qhat >
				   (next2digits - qhat * divisor1) * NBASE + dividend[j + 2])
				qhat--;

			/* As above, need do nothing more when quotient digit is 0 */
			if (qhat > 0)
			{
				NumericDigit *dividend_j = &dividend[j];

				/*
				 * Multiply the divisor by qhat, and subtract that from the
				 * working dividend.  The multiplication and subtraction are
				 * folded together here, noting that qhat <= NBASE (since it
				 * might be one too large), and so the intermediate result
				 * "tmp_result" is in the range [-NBASE^2, NBASE - 1], and
				 * "borrow" is in the range [0, NBASE].
				 */
				borrow = 0;
				for (i = var2ndigits; i >= 0; i--)
				{
					int			tmp_result;

					tmp_result = dividend_j[i] - borrow - divisor[i] * qhat;
					borrow = (NBASE - 1 - tmp_result) / NBASE;
					dividend_j[i] = tmp_result + borrow * NBASE;
				}

				/*
				 * If we got a borrow out of the top dividend digit, then
				 * indeed qhat was one too large.  Fix it, and add back the
				 * divisor to correct the working dividend.  (Knuth proves
				 * that this will occur only about 3/NBASE of the time; hence,
				 * it's a good idea to test this code with small NBASE to be
				 * sure this section gets exercised.)
				 */
				if (borrow)
				{
					qhat--;
					carry = 0;
					for (i = var2ndigits; i >= 0; i--)
					{
						carry += dividend_j[i] + divisor[i];
						if (carry >= NBASE)
						{
							dividend_j[i] = carry - NBASE;
							carry = 1;
						}
						else
						{
							dividend_j[i] = carry;
							carry = 0;
						}
					}
					/* A carry should occur here to cancel the borrow above */
					Assert(carry == 1);
				}
			}

			/* And we're done with this quotient digit */
			res_digits[j] = qhat;
		}

	pfree(dividend);

	/*
	 * Finally, round or truncate the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	adjust_numeric_var(result, rscale, rt, true);

	/* Strip leading and trailing zeroes */
	strip_var(result);
}


/*
 * div_var_fast() -
 *
 *	This has the same API as div_var, but is implemented using the division
 *	algorithm from the "FM" library, rather than Knuth's schoolbook-division
 *	approach.  This is significantly faster but can produce inaccurate
 *	results, because it sometimes has to propagate rounding to the left,
 *	and so we can never be entirely sure that we know the requested digits
 *	exactly.  We compute DIV_GUARD_DIGITS extra digits, but there is
 *	no certainty that that's enough.  We use this only in the transcendental
 *	function calculation routines, where everything is approximate anyway.
 *
 *	Although we provide a "round" argument for consistency with div_var,
 *	it is unwise to use this function with round=false.  In truncation mode
 *	it is possible to get a result with no significant digits, for example
 *	with rscale=0 we might compute 0.99999... and truncate that to 0 when
 *	the correct answer is 1.
 */
static void
div_var_fast(const NumericVar *var1, const NumericVar *var2, NumericVar *result,
			 int rscale, RoundType rt)
{
	int			div_ndigits;
	int			load_ndigits;
	int			res_sign;
	int			res_weight;
	int		   *div;
	int			qdigit;
	int			carry;
	int			maxdiv;
	int			newdig;
	NumericDigit *res_digits;
	double		fdividend,
				fdivisor,
				fdivisorinverse,
				fquotient;
	int			qi;
	int			i;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;

	/*
	 * First of all division by zero check; we must not be handed an
	 * unnormalized divisor.
	 */
	if (var2ndigits == 0 || var2digits[0] == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	/*
	 * If the divisor has just one or two digits, delegate to div_var_int(),
	 * which uses fast short division.
	 */
	if (var2ndigits <= 2)
	{
		int			idivisor;
		int			idivisor_weight;

		idivisor = var2->digits[0];
		idivisor_weight = var2->weight;
		if (var2ndigits == 2)
		{
			idivisor = idivisor * NBASE + var2->digits[1];
			idivisor_weight--;
		}
		if (var2->sign == NUMERIC_NEG)
			idivisor = -idivisor;

		div_var_int(var1, idivisor, idivisor_weight, result, rscale, rt, false);
		return;
	}

	/*
	 * Otherwise, perform full long division.
	 */

	/* Result zero check */
	if (var1ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate
	 */
	if (var1->sign == var2->sign)
		res_sign = NUMERIC_POS;
	else
		res_sign = NUMERIC_NEG;
	res_weight = var1->weight - var2->weight + 1;
	/* The number of accurate result digits we need to produce: */
	div_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* Add guard digits for roundoff error */
	div_ndigits += DIV_GUARD_DIGITS;
	if (div_ndigits < DIV_GUARD_DIGITS)
		div_ndigits = DIV_GUARD_DIGITS;

	/*
	 * We do the arithmetic in an array "div[]" of signed int's.  Since
	 * INT_MAX is noticeably larger than NBASE*NBASE, this gives us headroom
	 * to avoid normalizing carries immediately.
	 *
	 * We start with div[] containing one zero digit followed by the
	 * dividend's digits (plus appended zeroes to reach the desired precision
	 * including guard digits).  Each step of the main loop computes an
	 * (approximate) quotient digit and stores it into div[], removing one
	 * position of dividend space.  A final pass of carry propagation takes
	 * care of any mistaken quotient digits.
	 *
	 * Note that div[] doesn't necessarily contain all of the digits from the
	 * dividend --- the desired precision plus guard digits might be less than
	 * the dividend's precision.  This happens, for example, in the square
	 * root algorithm, where we typically divide a 2N-digit number by an
	 * N-digit number, and only require a result with N digits of precision.
	 */
	div = (int *) palloc0((div_ndigits + 1) * sizeof(int));
	load_ndigits = Min(div_ndigits, var1ndigits);
	for (i = 0; i < load_ndigits; i++)
		div[i + 1] = var1digits[i];

	/*
	 * We estimate each quotient digit using floating-point arithmetic, taking
	 * the first four digits of the (current) dividend and divisor.  This must
	 * be float to avoid overflow.  The quotient digits will generally be off
	 * by no more than one from the exact answer.
	 */
	fdivisor = (double) var2digits[0];
	for (i = 1; i < 4; i++)
	{
		fdivisor *= NBASE;
		if (i < var2ndigits)
			fdivisor += (double) var2digits[i];
	}
	fdivisorinverse = 1.0 / fdivisor;

	/*
	 * maxdiv tracks the maximum possible absolute value of any div[] entry;
	 * when this threatens to exceed INT_MAX, we take the time to propagate
	 * carries.  Furthermore, we need to ensure that overflow doesn't occur
	 * during the carry propagation passes either.  The carry values may have
	 * an absolute value as high as INT_MAX/NBASE + 1, so really we must
	 * normalize when digits threaten to exceed INT_MAX - INT_MAX/NBASE - 1.
	 *
	 * To avoid overflow in maxdiv itself, it represents the max absolute
	 * value divided by NBASE-1, ie, at the top of the loop it is known that
	 * no div[] entry has an absolute value exceeding maxdiv * (NBASE-1).
	 *
	 * Actually, though, that holds good only for div[] entries after div[qi];
	 * the adjustment done at the bottom of the loop may cause div[qi + 1] to
	 * exceed the maxdiv limit, so that div[qi] in the next iteration is
	 * beyond the limit.  This does not cause problems, as explained below.
	 */
	maxdiv = 1;

	/*
	 * Outer loop computes next quotient digit, which will go into div[qi]
	 */
	for (qi = 0; qi < div_ndigits; qi++)
	{
		/* Approximate the current dividend value */
		fdividend = (double) div[qi];
		for (i = 1; i < 4; i++)
		{
			fdividend *= NBASE;
			if (qi + i <= div_ndigits)
				fdividend += (double) div[qi + i];
		}
		/* Compute the (approximate) quotient digit */
		fquotient = fdividend * fdivisorinverse;
		qdigit = (fquotient >= 0.0) ? ((int) fquotient) :
			(((int) fquotient) - 1);	/* truncate towards -infinity */

		if (qdigit != 0)
		{
			/* Do we need to normalize now? */
			maxdiv += Abs(qdigit);
			if (maxdiv > (INT_MAX - INT_MAX / NBASE - 1) / (NBASE - 1))
			{
				/*
				 * Yes, do it.  Note that if var2ndigits is much smaller than
				 * div_ndigits, we can save a significant amount of effort
				 * here by noting that we only need to normalise those div[]
				 * entries touched where prior iterations subtracted multiples
				 * of the divisor.
				 */
				carry = 0;
				for (i = Min(qi + var2ndigits - 2, div_ndigits); i > qi; i--)
				{
					newdig = div[i] + carry;
					if (newdig < 0)
					{
						carry = -((-newdig - 1) / NBASE) - 1;
						newdig -= carry * NBASE;
					}
					else if (newdig >= NBASE)
					{
						carry = newdig / NBASE;
						newdig -= carry * NBASE;
					}
					else
						carry = 0;
					div[i] = newdig;
				}
				newdig = div[qi] + carry;
				div[qi] = newdig;

				/*
				 * All the div[] digits except possibly div[qi] are now in the
				 * range 0..NBASE-1.  We do not need to consider div[qi] in
				 * the maxdiv value anymore, so we can reset maxdiv to 1.
				 */
				maxdiv = 1;

				/*
				 * Recompute the quotient digit since new info may have
				 * propagated into the top four dividend digits
				 */
				fdividend = (double) div[qi];
				for (i = 1; i < 4; i++)
				{
					fdividend *= NBASE;
					if (qi + i <= div_ndigits)
						fdividend += (double) div[qi + i];
				}
				/* Compute the (approximate) quotient digit */
				fquotient = fdividend * fdivisorinverse;
				qdigit = (fquotient >= 0.0) ? ((int) fquotient) :
					(((int) fquotient) - 1);	/* truncate towards -infinity */
				maxdiv += Abs(qdigit);
			}

			/*
			 * Subtract off the appropriate multiple of the divisor.
			 *
			 * The digits beyond div[qi] cannot overflow, because we know they
			 * will fall within the maxdiv limit.  As for div[qi] itself, note
			 * that qdigit is approximately trunc(div[qi] / vardigits[0]),
			 * which would make the new value simply div[qi] mod vardigits[0].
			 * The lower-order terms in qdigit can change this result by not
			 * more than about twice INT_MAX/NBASE, so overflow is impossible.
			 *
			 * This inner loop is the performance bottleneck for division, so
			 * code it in the same way as the inner loop of mul_var() so that
			 * it can be auto-vectorized.  We cast qdigit to NumericDigit
			 * before multiplying to allow the compiler to generate more
			 * efficient code (using 16-bit multiplication), which is safe
			 * since we know that the quotient digit is off by at most one, so
			 * there is no overflow risk.
			 */
			if (qdigit != 0)
			{
				int			istop = Min(var2ndigits, div_ndigits - qi + 1);
				int		   *div_qi = &div[qi];

				for (i = 0; i < istop; i++)
					div_qi[i] -= ((NumericDigit) qdigit) * var2digits[i];
			}
		}

		/*
		 * The dividend digit we are about to replace might still be nonzero.
		 * Fold it into the next digit position.
		 *
		 * There is no risk of overflow here, although proving that requires
		 * some care.  Much as with the argument for div[qi] not overflowing,
		 * if we consider the first two terms in the numerator and denominator
		 * of qdigit, we can see that the final value of div[qi + 1] will be
		 * approximately a remainder mod (vardigits[0]*NBASE + vardigits[1]).
		 * Accounting for the lower-order terms is a bit complicated but ends
		 * up adding not much more than INT_MAX/NBASE to the possible range.
		 * Thus, div[qi + 1] cannot overflow here, and in its role as div[qi]
		 * in the next loop iteration, it can't be large enough to cause
		 * overflow in the carry propagation step (if any), either.
		 *
		 * But having said that: div[qi] can be more than INT_MAX/NBASE, as
		 * noted above, which means that the product div[qi] * NBASE *can*
		 * overflow.  When that happens, adding it to div[qi + 1] will always
		 * cause a canceling overflow so that the end result is correct.  We
		 * could avoid the intermediate overflow by doing the multiplication
		 * and addition in int64 arithmetic, but so far there appears no need.
		 */
		div[qi + 1] += div[qi] * NBASE;

		div[qi] = qdigit;
	}

	/*
	 * Approximate and store the last quotient digit (div[div_ndigits])
	 */
	fdividend = (double) div[qi];
	for (i = 1; i < 4; i++)
		fdividend *= NBASE;
	fquotient = fdividend * fdivisorinverse;
	qdigit = (fquotient >= 0.0) ? ((int) fquotient) :
		(((int) fquotient) - 1);	/* truncate towards -infinity */
	div[qi] = qdigit;

	/*
	 * Because the quotient digits might be off by one, some of them might be
	 * -1 or NBASE at this point.  The represented value is correct in a
	 * mathematical sense, but it doesn't look right.  We do a final carry
	 * propagation pass to normalize the digits, which we combine with storing
	 * the result digits into the output.  Note that this is still done at
	 * full precision w/guard digits.
	 */
	alloc_var(result, div_ndigits + 1);
	res_digits = result->digits;
	carry = 0;
	for (i = div_ndigits; i >= 0; i--)
	{
		newdig = div[i] + carry;
		if (newdig < 0)
		{
			carry = -((-newdig - 1) / NBASE) - 1;
			newdig -= carry * NBASE;
		}
		else if (newdig >= NBASE)
		{
			carry = newdig / NBASE;
			newdig -= carry * NBASE;
		}
		else
			carry = 0;
		res_digits[i] = newdig;
	}
	Assert(carry == 0);

	pfree(div);

	/*
	 * Finally, round the result to the requested precision.
	 */
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round to target rscale (and set result->dscale) */
	if (rt == HALF_ADJUST_ROUND || rt == BANKER_ROUND)
	{
		if (ORA_MODE)
			check_ora_number_overflow(result, true);
		round_var(result, rscale, rt);
	}
	else
		trunc_var(result, rscale);
	/* Strip leading and trailing zeroes */
	strip_var(result);
}


/*
 * div_var_int() -
 *
 *	Divide a numeric variable by a 32-bit integer with the specified weight.
 *	The quotient var / (ival * NBASE^ival_weight) is stored in result.
 */
static void
div_var_int(const NumericVar *var, int ival, int ival_weight,
			NumericVar *result, int rscale, RoundType rt, bool ora_scale)
{
	NumericDigit *var_digits = var->digits;
	int			var_ndigits = var->ndigits;
	int			res_sign;
	int			res_weight;
	int			res_ndigits;
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	uint32		divisor;
	int			i;

	/* Guard against division by zero */
	if (ival == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				errmsg("division by zero")));

	/* Result zero check */
	if (var_ndigits == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Determine the result sign, weight and number of digits to calculate.
	 * The weight figured here is correct if the emitted quotient has no
	 * leading zero digits; otherwise strip_var() will fix things up.
	 */
	if (var->sign == NUMERIC_POS)
		res_sign = ival > 0 ? NUMERIC_POS : NUMERIC_NEG;
	else
		res_sign = ival > 0 ? NUMERIC_NEG : NUMERIC_POS;
	res_weight = var->weight - ival_weight;
	/* The number of accurate result digits we need to produce: */
	res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
	/* ... but always at least 1 */
	res_ndigits = Max(res_ndigits, 1);
	/* If rounding needed, figure one more digit to ensure correct result */
	if (rt != TRUNCATE_ROUND && rt != TRUNCATE_NUM)
		res_ndigits++;

	res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	/*
	 * Now compute the quotient digits.  This is the short division algorithm
	 * described in Knuth volume 2, section 4.3.1 exercise 16, except that we
	 * allow the divisor to exceed the internal base.
	 *
	 * In this algorithm, the carry from one digit to the next is at most
	 * divisor - 1.  Therefore, while processing the next digit, carry may
	 * become as large as divisor * NBASE - 1, and so it requires a 64-bit
	 * integer if this exceeds UINT_MAX.
	 */
	divisor = Abs(ival);

	if (divisor <= UINT_MAX / NBASE)
	{
		/* carry cannot overflow 32 bits */
		uint32		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}
	else
	{
		/* carry may exceed 32 bits */
		uint64		carry = 0;

		for (i = 0; i < res_ndigits; i++)
		{
			carry = carry * NBASE + (i < var_ndigits ? var_digits[i] : 0);
			res_digits[i] = (NumericDigit) (carry / divisor);
			carry = carry % divisor;
		}
	}

	/* Store the quotient in result */
	digitbuf_free(result);
	result->ndigits = res_ndigits;
	result->buf = res_buf;
	result->digits = res_digits;
	result->weight = res_weight;
	result->sign = res_sign;

	/* Round or truncate to target rscale (and set result->dscale) */
	adjust_numeric_var(result, rscale, rt, ora_scale);
	
	/* Strip leading/trailing zeroes */
	strip_var(result);
}


/*
 * Default scale selection for division
 *
 * Returns the appropriate result scale for the division result.
 */
static int
select_div_scale(const NumericVar *var1, const NumericVar *var2)
{
	int			weight1,
				weight2,
				qweight,
				i;
	NumericDigit firstdigit1,
				firstdigit2;
	int			rscale;

	/*
	 * The result scale of a division isn't specified in any SQL standard. For
	 * PostgreSQL we select a result scale that will give at least
	 * NUMERIC_MIN_SIG_DIGITS significant digits, so that numeric gives a
	 * result no less accurate than float8; but use a scale not less than
	 * either input's display scale.
	 */

	/* Get the actual (normalized) weight and first digit of each input */

	weight1 = 0;				/* values to use if var1 is zero */
	firstdigit1 = 0;
	for (i = 0; i < var1->ndigits; i++)
	{
		firstdigit1 = var1->digits[i];
		if (firstdigit1 != 0)
		{
			weight1 = var1->weight - i;
			break;
		}
	}

	weight2 = 0;				/* values to use if var2 is zero */
	firstdigit2 = 0;
	for (i = 0; i < var2->ndigits; i++)
	{
		firstdigit2 = var2->digits[i];
		if (firstdigit2 != 0)
		{
			weight2 = var2->weight - i;
			break;
		}
	}

	/*
	 * Estimate weight of quotient.  If the two first digits are equal, we
	 * can't be sure, but assume that var1 is less than var2.
	 */
	qweight = weight1 - weight2;
	if (firstdigit1 <= firstdigit2)
		qweight--;

	/* Select result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, var1->dscale);
	rscale = Max(rscale, var2->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	return rscale;
}


/*
 * mod_var() -
 *
 *	Calculate the modulo of two numerics at variable level
 */
static void
mod_var(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericVar	tmp;

	quick_init_var(&tmp);

	/* ---------
	 * We do this using the equation
	 *		mod(x,y) = x - trunc(x/y)*y
	 * div_var can be persuaded to give us trunc(x/y) directly.
	 * ----------
	 */
	div_var(var1, var2, &tmp, 0, TRUNCATE_ROUND);

	mul_var(var2, &tmp, &tmp, var2->dscale);

	sub_var(var1, &tmp, result);

	free_var(&tmp);
}

/*
 * remainder_var() -Calculate the remainder of two numerics at variable level
 */
static void
remainder_var(NumericVar *var1, NumericVar *var2, NumericVar *result)
{
	NumericVar	tmp;

	init_var(&tmp);

	/* ---------
   	 * We do this using the equation
   	 *		mod(x,y) = x - round(x/y)*y
   	 * div_var can be persuaded to give us round(x/y) directly.
   	 * ----------
   	 */
	div_var(var1, var2, &tmp, 0, BANKER_ROUND);

	mul_var(var2, &tmp, &tmp, var2->dscale);

	sub_var(var1, &tmp, result);

	free_var(&tmp);
}

/*
 * div_mod_var() -
 *
 *	Calculate the truncated integer quotient and numeric remainder of two
 *	numeric variables.  The remainder is precise to var2's dscale.
 */
static void
div_mod_var(const NumericVar *var1, const NumericVar *var2,
			NumericVar *quot, NumericVar *rem)
{
	NumericVar	q;
	NumericVar	r;

	init_var(&q);
	init_var(&r);

	/*
	 * Use div_var_fast() to get an initial estimate for the integer quotient.
	 * This might be inaccurate (per the warning in div_var_fast's comments),
	 * but we can correct it below.
	 */
	div_var_fast(var1, var2, &q, 0, TRUNCATE_NUM);

	/* Compute initial estimate of remainder using the quotient estimate. */
	mul_var(var2, &q, &r, var2->dscale);
	sub_var(var1, &r, &r);

	/*
	 * Adjust the results if necessary --- the remainder should have the same
	 * sign as var1, and its absolute value should be less than the absolute
	 * value of var2.
	 */
	while (r.ndigits != 0 && r.sign != var1->sign)
	{
		/* The absolute value of the quotient is too large */
		if (var1->sign == var2->sign)
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
		else
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
	}

	while (cmp_abs(&r, var2) >= 0)
	{
		/* The absolute value of the quotient is too small */
		if (var1->sign == var2->sign)
		{
			add_var(&q, &const_one, &q);
			sub_var(&r, var2, &r);
		}
		else
		{
			sub_var(&q, &const_one, &q);
			add_var(&r, var2, &r);
		}
	}

	set_var_from_var(&q, quot);
	set_var_from_var(&r, rem);

	free_var(&q);
	free_var(&r);
}


/*
 * ceil_var() -
 *
 *	Return the smallest integer greater than or equal to the argument
 *	on variable level
 */
static void
ceil_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_POS && cmp_var(var, &tmp) != 0)
		add_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}


/*
 * floor_var() -
 *
 *	Return the largest integer equal to or less than the argument
 *	on variable level
 */
static void
floor_var(const NumericVar *var, NumericVar *result)
{
	NumericVar	tmp;

	init_var_from_var(var, &tmp);

	trunc_var(&tmp, 0);

	if (var->sign == NUMERIC_NEG && cmp_var(var, &tmp) != 0)
		sub_var(&tmp, &const_one, &tmp);

	set_var_from_var(&tmp, result);
	free_var(&tmp);
}


/*
 * sqrt_var() -
 *
 *	Compute the square root of x using the Karatsuba Square Root algorithm.
 *	NOTE: we allow rscale < 0 here, implying rounding before the decimal
 *	point.
 */
static void
sqrt_var(NumericVar *arg, NumericVar *result, int rscale)
{
	int			stat;
	int			res_weight;
	int			res_ndigits;
	int			src_ndigits;
	int			step;
	int			ndigits[32];
	int			blen;
	int64		arg_int64;
	int			src_idx;
	int64		s_int64;
	int64		r_int64;
	NumericVar	s_var;
	NumericVar	r_var;
	NumericVar	a0_var;
	NumericVar	a1_var;
	NumericVar	q_var;
	NumericVar	u_var;

	stat = cmp_var(arg, &const_zero);
	if (stat == 0)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * SQL2003 defines sqrt() in terms of power, so we need to emit the right
	 * SQLSTATE error code if the operand is negative.
	 */
	if (stat < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
				 errmsg("cannot take square root of a negative number")));

	init_var(&s_var);
	init_var(&r_var);
	init_var(&a0_var);
	init_var(&a1_var);
	init_var(&q_var);
	init_var(&u_var);

	/*
	 * The result weight is half the input weight, rounded towards minus
	 * infinity --- res_weight = floor(arg->weight / 2).
	 */
	if (arg->weight >= 0)
		res_weight = arg->weight / 2;
	else
		res_weight = -((-arg->weight - 1) / 2 + 1);

	/*
	 * Number of NBASE digits to compute.  To ensure correct rounding, compute
	 * at least 1 extra decimal digit.  We explicitly allow rscale to be
	 * negative here, but must always compute at least 1 NBASE digit.  Thus
	 * res_ndigits = res_weight + 1 + ceil((rscale + 1) / DEC_DIGITS) or 1.
	 */
	if (rscale + 1 >= 0)
		res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS;
	else
		res_ndigits = res_weight + 1 - (-rscale - 1) / DEC_DIGITS;
	res_ndigits = Max(res_ndigits, 1);

	/*
	 * Number of source NBASE digits logically required to produce a result
	 * with this precision --- every digit before the decimal point, plus 2
	 * for each result digit after the decimal point (or minus 2 for each
	 * result digit we round before the decimal point).
	 */
	src_ndigits = arg->weight + 1 + (res_ndigits - res_weight - 1) * 2;
	src_ndigits = Max(src_ndigits, 1);

	/* ----------
	 * From this point on, we treat the input and the result as integers and
	 * compute the integer square root and remainder using the Karatsuba
	 * Square Root algorithm, which may be written recursively as follows:
	 *
	 *	SqrtRem(n = a3*b^3 + a2*b^2 + a1*b + a0):
	 *		[ for some base b, and coefficients a0,a1,a2,a3 chosen so that
	 *		  0 <= a0,a1,a2 < b and a3 >= b/4 ]
	 *		Let (s,r) = SqrtRem(a3*b + a2)
	 *		Let (q,u) = DivRem(r*b + a1, 2*s)
	 *		Let s = s*b + q
	 *		Let r = u*b + a0 - q^2
	 *		If r < 0 Then
	 *			Let r = r + s
	 *			Let s = s - 1
	 *			Let r = r + s
	 *		Return (s,r)
	 *
	 * See "Karatsuba Square Root", Paul Zimmermann, INRIA Research Report
	 * RR-3805, November 1999.  At the time of writing this was available
	 * on the net at <https://hal.inria.fr/inria-00072854>.
	 *
	 * The way to read the assumption "n = a3*b^3 + a2*b^2 + a1*b + a0" is
	 * "choose a base b such that n requires at least four base-b digits to
	 * express; then those digits are a3,a2,a1,a0, with a3 possibly larger
	 * than b".  For optimal performance, b should have approximately a
	 * quarter the number of digits in the input, so that the outer square
	 * root computes roughly twice as many digits as the inner one.  For
	 * simplicity, we choose b = NBASE^blen, an integer power of NBASE.
	 *
	 * We implement the algorithm iteratively rather than recursively, to
	 * allow the working variables to be reused.  With this approach, each
	 * digit of the input is read precisely once --- src_idx tracks the number
	 * of input digits used so far.
	 *
	 * The array ndigits[] holds the number of NBASE digits of the input that
	 * will have been used at the end of each iteration, which roughly doubles
	 * each time.  Note that the array elements are stored in reverse order,
	 * so if the final iteration requires src_ndigits = 37 input digits, the
	 * array will contain [37,19,11,7,5,3], and we would start by computing
	 * the square root of the 3 most significant NBASE digits.
	 *
	 * In each iteration, we choose blen to be the largest integer for which
	 * the input number has a3 >= b/4, when written in the form above.  In
	 * general, this means blen = src_ndigits / 4 (truncated), but if
	 * src_ndigits is a multiple of 4, that might lead to the coefficient a3
	 * being less than b/4 (if the first input digit is less than NBASE/4), in
	 * which case we choose blen = src_ndigits / 4 - 1.  The number of digits
	 * in the inner square root is then src_ndigits - 2*blen.  So, for
	 * example, if we have src_ndigits = 26 initially, the array ndigits[]
	 * will be either [26,14,8,4] or [26,14,8,6,4], depending on the size of
	 * the first input digit.
	 *
	 * Additionally, we can put an upper bound on the number of steps required
	 * as follows --- suppose that the number of source digits is an n-bit
	 * number in the range [2^(n-1), 2^n-1], then blen will be in the range
	 * [2^(n-3)-1, 2^(n-2)-1] and the number of digits in the inner square
	 * root will be in the range [2^(n-2), 2^(n-1)+1].  In the next step, blen
	 * will be in the range [2^(n-4)-1, 2^(n-3)] and the number of digits in
	 * the next inner square root will be in the range [2^(n-3), 2^(n-2)+1].
	 * This pattern repeats, and in the worst case the array ndigits[] will
	 * contain [2^n-1, 2^(n-1)+1, 2^(n-2)+1, ... 9, 5, 3], and the computation
	 * will require n steps.  Therefore, since all digit array sizes are
	 * signed 32-bit integers, the number of steps required is guaranteed to
	 * be less than 32.
	 * ----------
	 */
	step = 0;
	while ((ndigits[step] = src_ndigits) > 4)
	{
		/* Choose b so that a3 >= b/4, as described above */
		blen = src_ndigits / 4;
		if (blen * 4 == src_ndigits && arg->digits[0] < NBASE / 4)
			blen--;

		/* Number of digits in the next step (inner square root) */
		src_ndigits -= 2 * blen;
		step++;
	}

	/*
	 * First iteration (innermost square root and remainder):
	 *
	 * Here src_ndigits <= 4, and the input fits in an int64.  Its square root
	 * has at most 9 decimal digits, so estimate it using double precision
	 * arithmetic, which will in fact almost certainly return the correct
	 * result with no further correction required.
	 */
	arg_int64 = arg->digits[0];
	for (src_idx = 1; src_idx < src_ndigits; src_idx++)
	{
		arg_int64 *= NBASE;
		if (src_idx < arg->ndigits)
			arg_int64 += arg->digits[src_idx];
	}

	s_int64 = (int64) sqrt((double) arg_int64);
	r_int64 = arg_int64 - s_int64 * s_int64;

	/*
	 * Use Newton's method to correct the result, if necessary.
	 *
	 * This uses integer division with truncation to compute the truncated
	 * integer square root by iterating using the formula x -> (x + n/x) / 2.
	 * This is known to converge to isqrt(n), unless n+1 is a perfect square.
	 * If n+1 is a perfect square, the sequence will oscillate between the two
	 * values isqrt(n) and isqrt(n)+1, so we can be assured of convergence by
	 * checking the remainder.
	 */
	while (r_int64 < 0 || r_int64 > 2 * s_int64)
	{
		s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
		r_int64 = arg_int64 - s_int64 * s_int64;
	}

	/*
	 * Iterations with src_ndigits <= 8:
	 *
	 * The next 1 or 2 iterations compute larger (outer) square roots with
	 * src_ndigits <= 8, so the result still fits in an int64 (even though the
	 * input no longer does) and we can continue to compute using int64
	 * variables to avoid more expensive numeric computations.
	 *
	 * It is fairly easy to see that there is no risk of the intermediate
	 * values below overflowing 64-bit integers.  In the worst case, the
	 * previous iteration will have computed a 3-digit square root (of a
	 * 6-digit input less than NBASE^6 / 4), so at the start of this
	 * iteration, s will be less than NBASE^3 / 2 = 10^12 / 2, and r will be
	 * less than 10^12.  In this case, blen will be 1, so numer will be less
	 * than 10^17, and denom will be less than 10^12 (and hence u will also be
	 * less than 10^12).  Finally, since q^2 = u*b + a0 - r, we can also be
	 * sure that q^2 < 10^17.  Therefore all these quantities fit comfortably
	 * in 64-bit integers.
	 */
	step--;
	while (step >= 0 && (src_ndigits = ndigits[step]) <= 8)
	{
		int			b;
		int			a0;
		int			a1;
		int			i;
		int64		numer;
		int64		denom;
		int64		q;
		int64		u;

		blen = (src_ndigits - src_idx) / 2;

		/* Extract a1 and a0, and compute b */
		a0 = 0;
		a1 = 0;
		b = 1;

		for (i = 0; i < blen; i++, src_idx++)
		{
			b *= NBASE;
			a1 *= NBASE;
			if (src_idx < arg->ndigits)
				a1 += arg->digits[src_idx];
		}

		for (i = 0; i < blen; i++, src_idx++)
		{
			a0 *= NBASE;
			if (src_idx < arg->ndigits)
				a0 += arg->digits[src_idx];
		}

		/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
		numer = r_int64 * b + a1;
		denom = 2 * s_int64;
		q = numer / denom;
		u = numer - q * denom;

		/* Compute s = s*b + q and r = u*b + a0 - q^2 */
		s_int64 = s_int64 * b + q;
		r_int64 = u * b + a0 - q * q;

		if (r_int64 < 0)
		{
			/* s is too large by 1; set r += s, s--, r += s */
			r_int64 += s_int64;
			s_int64--;
			r_int64 += s_int64;
		}

		Assert(src_idx == src_ndigits); /* All input digits consumed */
		step--;
	}

	/*
	 * On platforms with 128-bit integer support, we can further delay the
	 * need to use numeric variables.
	 */
#ifdef HAVE_INT128
	if (step >= 0)
	{
		int128		s_int128;
		int128		r_int128;

		s_int128 = s_int64;
		r_int128 = r_int64;

		/*
		 * Iterations with src_ndigits <= 16:
		 *
		 * The result fits in an int128 (even though the input doesn't) so we
		 * use int128 variables to avoid more expensive numeric computations.
		 */
		while (step >= 0 && (src_ndigits = ndigits[step]) <= 16)
		{
			int64		b;
			int64		a0;
			int64		a1;
			int64		i;
			int128		numer;
			int128		denom;
			int128		q;
			int128		u;

			blen = (src_ndigits - src_idx) / 2;

			/* Extract a1 and a0, and compute b */
			a0 = 0;
			a1 = 0;
			b = 1;

			for (i = 0; i < blen; i++, src_idx++)
			{
				b *= NBASE;
				a1 *= NBASE;
				if (src_idx < arg->ndigits)
					a1 += arg->digits[src_idx];
			}

			for (i = 0; i < blen; i++, src_idx++)
			{
				a0 *= NBASE;
				if (src_idx < arg->ndigits)
					a0 += arg->digits[src_idx];
			}

			/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
			numer = r_int128 * b + a1;
			denom = 2 * s_int128;
			q = numer / denom;
			u = numer - q * denom;

			/* Compute s = s*b + q and r = u*b + a0 - q^2 */
			s_int128 = s_int128 * b + q;
			r_int128 = u * b + a0 - q * q;

			if (r_int128 < 0)
			{
				/* s is too large by 1; set r += s, s--, r += s */
				r_int128 += s_int128;
				s_int128--;
				r_int128 += s_int128;
			}

			Assert(src_idx == src_ndigits); /* All input digits consumed */
			step--;
		}

		/*
		 * All remaining iterations require numeric variables.  Convert the
		 * integer values to NumericVar and continue.  Note that in the final
		 * iteration we don't need the remainder, so we can save a few cycles
		 * there by not fully computing it.
		 */
		int128_to_numericvar(s_int128, &s_var);
		if (step >= 0)
			int128_to_numericvar(r_int128, &r_var);
	}
	else
	{
		int64_to_numericvar(s_int64, &s_var);
		/* step < 0, so we certainly don't need r */
	}
#else							/* !HAVE_INT128 */
	int64_to_numericvar(s_int64, &s_var);
	if (step >= 0)
		int64_to_numericvar(r_int64, &r_var);
#endif							/* HAVE_INT128 */

	/*
	 * The remaining iterations with src_ndigits > 8 (or 16, if have int128)
	 * use numeric variables.
	 */
	while (step >= 0)
	{
		int			tmp_len;

		src_ndigits = ndigits[step];
		blen = (src_ndigits - src_idx) / 2;

		/* Extract a1 and a0 */
		if (src_idx < arg->ndigits)
		{
			tmp_len = Min(blen, arg->ndigits - src_idx);
			alloc_var(&a1_var, tmp_len);
			memcpy(a1_var.digits, arg->digits + src_idx,
				   tmp_len * sizeof(NumericDigit));
			a1_var.weight = blen - 1;
			a1_var.sign = NUMERIC_POS;
			a1_var.dscale = 0;
			strip_var(&a1_var);
		}
		else
		{
			zero_var(&a1_var);
			a1_var.dscale = 0;
		}
		src_idx += blen;

		if (src_idx < arg->ndigits)
		{
			tmp_len = Min(blen, arg->ndigits - src_idx);
			alloc_var(&a0_var, tmp_len);
			memcpy(a0_var.digits, arg->digits + src_idx,
				   tmp_len * sizeof(NumericDigit));
			a0_var.weight = blen - 1;
			a0_var.sign = NUMERIC_POS;
			a0_var.dscale = 0;
			strip_var(&a0_var);
		}
		else
		{
			zero_var(&a0_var);
			a0_var.dscale = 0;
		}
		src_idx += blen;

		/* Compute (q,u) = DivRem(r*b + a1, 2*s) */
		set_var_from_var(&r_var, &q_var);
		q_var.weight += blen;
		add_var(&q_var, &a1_var, &q_var);
		add_var(&s_var, &s_var, &u_var);
		div_mod_var(&q_var, &u_var, &q_var, &u_var);

		/* Compute s = s*b + q */
		s_var.weight += blen;
		add_var(&s_var, &q_var, &s_var);

		/*
		 * Compute r = u*b + a0 - q^2.
		 *
		 * In the final iteration, we don't actually need r; we just need to
		 * know whether it is negative, so that we know whether to adjust s.
		 * So instead of the final subtraction we can just compare.
		 */
		u_var.weight += blen;
		add_var(&u_var, &a0_var, &u_var);
		mul_var(&q_var, &q_var, &q_var, 0);

		if (step > 0)
		{
			/* Need r for later iterations */
			sub_var(&u_var, &q_var, &r_var);
			if (r_var.sign == NUMERIC_NEG)
			{
				/* s is too large by 1; set r += s, s--, r += s */
				add_var(&r_var, &s_var, &r_var);
				sub_var(&s_var, &const_one, &s_var);
				add_var(&r_var, &s_var, &r_var);
			}
		}
		else
		{
			/* Don't need r anymore, except to test if s is too large by 1 */
			if (cmp_var(&u_var, &q_var) < 0)
				sub_var(&s_var, &const_one, &s_var);
		}

		Assert(src_idx == src_ndigits); /* All input digits consumed */
		step--;
	}

	/*
	 * Construct the final result, rounding it to the requested precision.
	 */
	set_var_from_var(&s_var, result);
	result->weight = res_weight;
	result->sign = NUMERIC_POS;

	/* Round to target rscale (and set result->dscale) */
	round_var(result, rscale, HALF_ADJUST_ROUND);

	/* Strip leading and trailing zeroes */
	strip_var(result);

	free_var(&s_var);
	free_var(&r_var);
	free_var(&a0_var);
	free_var(&a1_var);
	free_var(&q_var);
	free_var(&u_var);
}


/*
 * exp_var() -
 *
 *	Raise e to the power of x, computed to rscale fractional digits
 */
static void
exp_var(NumericVar *arg, NumericVar *result, int rscale)
{
	NumericVar	x;
	NumericVar	elem;
	int			ni;
	double		val;
	int			dweight;
	int			ndiv2;
	int			sig_digits;
	int			local_rscale;

	init_var(&x);
	init_var(&elem);

	set_var_from_var(arg, &x);

	/*
	 * Estimate the dweight of the result using floating point arithmetic, so
	 * that we can choose an appropriate local rscale for the calculation.
	 */
	val = numericvar_to_double_no_overflow(&x);

	/* Guard against overflow */
	/* If you change this limit, see also power_var()'s limit */
	if (Abs(val) >= NUMERIC_MAX_RESULT_SCALE * 3)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));

	/* decimal weight = log10(e^x) = x * log10(e) */
	dweight = (int) (val * 0.434294481903252);

	/*
	 * Reduce x to the range -0.01 <= x <= 0.01 (approximately) by dividing by
	 * 2^ndiv2, to improve the convergence rate of the Taylor series.
	 *
	 * Note that the overflow check above ensures that Abs(x) < 6000, which
	 * means that ndiv2 <= 20 here.
	 */
	if (Abs(val) > 0.01)
	{
		ndiv2 = 1;
		val /= 2;

		while (Abs(val) > 0.01)
		{
			ndiv2++;
			val /= 2;
		}

		local_rscale = x.dscale + ndiv2;
		div_var_int(&x, 1 << ndiv2, 0, &x, local_rscale, HALF_ADJUST_ROUND, false);
	}
	else
		ndiv2 = 0;

	/*
	 * Set the scale for the Taylor series expansion.  The final result has
	 * (dweight + rscale + 1) significant digits.  In addition, we have to
	 * raise the Taylor series result to the power 2^ndiv2, which introduces
	 * an error of up to around log10(2^ndiv2) digits, so work with this many
	 * extra digits of precision (plus a few more for good measure).
	 */
	sig_digits = 1 + dweight + rscale + (int) (ndiv2 * 0.301029995663981);
	sig_digits = Max(sig_digits, 0) + 8;

	local_rscale = sig_digits - 1;

	/*
	 * Use the Taylor series
	 *
	 * exp(x) = 1 + x + x^2/2! + x^3/3! + ...
	 *
	 * Given the limited range of x, this should converge reasonably quickly.
	 * We run the series until the terms fall below the local_rscale limit.
	 */
	add_var(&const_one, &x, result);

	mul_var(&x, &x, &elem, local_rscale);
	ni = 2;
	div_var_int(&elem, ni, 0, &elem, local_rscale, HALF_ADJUST_ROUND, false);

	while (elem.ndigits != 0)
	{
		add_var(result, &elem, result);

		mul_var(&elem, &x, &elem, local_rscale);
		ni++;
		div_var_int(&elem, ni, 0, &elem, local_rscale, HALF_ADJUST_ROUND,false);
	}

	/*
	 * Compensate for the argument range reduction.  Since the weight of the
	 * result doubles with each multiplication, we can reduce the local rscale
	 * as we proceed.
	 */
	while (ndiv2-- > 0)
	{
		local_rscale = sig_digits - result->weight * 2 * DEC_DIGITS;
		local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
		mul_var(result, result, result, local_rscale);
	}

	/* Round to requested rscale */
	round_var(result, rscale, HALF_ADJUST_ROUND);

	free_var(&x);
	free_var(&elem);
}


/*
 * Estimate the dweight of the most significant decimal digit of the natural
 * logarithm of a number.
 *
 * Essentially, we're approximating log10(abs(ln(var))).  This is used to
 * determine the appropriate rscale when computing natural logarithms.
 */
static int
estimate_ln_dweight(NumericVar *var)
{
	int			ln_dweight;

	if (cmp_var(var, &const_zero_point_nine) >= 0 &&
		cmp_var(var, &const_one_point_one) <= 0)
	{
		/*
		 * 0.9 <= var <= 1.1
		 *
		 * ln(var) has a negative weight (possibly very large).  To get a
		 * reasonably accurate result, estimate it using ln(1+x) ~= x.
		 */
		NumericVar	x;

		init_var(&x);
		sub_var(var, &const_one, &x);

		if (x.ndigits > 0)
		{
			/* Use weight of most significant decimal digit of x */
			ln_dweight = x.weight * DEC_DIGITS + (int) log10(x.digits[0]);
		}
		else
		{
			/* x = 0.  Since ln(1) = 0 exactly, we don't need extra digits */
			ln_dweight = 0;
		}

		free_var(&x);
	}
	else
	{
		/*
		 * Estimate the logarithm using the first couple of digits from the
		 * input number.  This will give an accurate result whenever the input
		 * is not too close to 1.
		 */
		if (var->ndigits > 0)
		{
			int			digits;
			int			dweight;
			double		ln_var;

			digits = var->digits[0];
			dweight = var->weight * DEC_DIGITS;

			if (var->ndigits > 1)
			{
				digits = digits * NBASE + var->digits[1];
				dweight -= DEC_DIGITS;
			}

			/*----------
			 * We have var ~= digits * 10^dweight
			 * so ln(var) ~= ln(digits) + dweight * ln(10)
			 *----------
			 */
			ln_var = log((double) digits) + dweight * 2.302585092994046;
			ln_dweight = (int) log10(Abs(ln_var));
		}
		else
		{
			/* Caller should fail on ln(0), but for the moment return zero */
			ln_dweight = 0;
		}
	}

	return ln_dweight;
}


/*
 * ln_var() -
 *
 *	Compute the natural log of x
 */
static void
ln_var(NumericVar *arg, NumericVar *result, int rscale)
{
	NumericVar	x;
	NumericVar	xx;
	int			ni;
	NumericVar	elem;
	NumericVar	fact;
	int			local_rscale;
	int			cmp;

	cmp = cmp_var(arg, &const_zero);
	if (cmp == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of zero")));
	else if (cmp < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
				 errmsg("cannot take logarithm of a negative number")));

	init_var(&x);
	init_var(&xx);
	init_var(&elem);
	init_var(&fact);

	init_var_from_var(arg, &x);
	set_var_from_var(&const_two, &fact);

	/*
	 * Reduce input into range 0.9 < x < 1.1 with repeated sqrt() operations.
	 *
	 * The final logarithm will have up to around rscale+6 significant digits.
	 * Each sqrt() will roughly halve the weight of x, so adjust the local
	 * rscale as we work so that we keep this many significant digits at each
	 * step (plus a few more for good measure).
	 *
	 * Note that we allow local_rscale < 0 during this input reduction
	 * process, which implies rounding before the decimal point.  sqrt_var()
	 * explicitly supports this, and it significantly reduces the work
	 * required to reduce very large inputs to the required range.  Once the
	 * input reduction is complete, x.weight will be 0 and its display scale
	 * will be non-negative again.
	 */
	while (cmp_var(&x, &const_zero_point_nine) <= 0)
	{
		local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
		sqrt_var(&x, &x, local_rscale);
		mul_var(&fact, &const_two, &fact, 0);
	}
	while (cmp_var(&x, &const_one_point_one) >= 0)
	{
		local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
		sqrt_var(&x, &x, local_rscale);
		mul_var(&fact, &const_two, &fact, 0);
	}

	/*
	 * We use the Taylor series for 0.5 * ln((1+z)/(1-z)),
	 *
	 * z + z^3/3 + z^5/5 + ...
	 *
	 * where z = (x-1)/(x+1) is in the range (approximately) -0.053 .. 0.048
	 * due to the above range-reduction of x.
	 *
	 * The convergence of this is not as fast as one would like, but is
	 * tolerable given that z is small.
	 */
	local_rscale = rscale + 8;

	sub_var(&x, &const_one, result);
	add_var(&x, &const_one, &elem);
	div_var_fast(result, &elem, result, local_rscale, HALF_ADJUST_ROUND);
	set_var_from_var(result, &xx);
	mul_var(result, result, &x, local_rscale);

	ni = 1;

	for (;;)
	{
		ni += 2;
		mul_var(&xx, &x, &xx, local_rscale);
		div_var_int(&xx, ni, 0, &elem, local_rscale, HALF_ADJUST_ROUND, false);

		if (elem.ndigits == 0)
			break;

		add_var(result, &elem, result);

		if (elem.weight < (result->weight - local_rscale * 2 / DEC_DIGITS))
			break;
	}

	/* Compensate for argument range reduction, round to requested rscale */
	mul_var(result, &fact, result, rscale);

	free_var(&x);
	free_var(&xx);
	free_var(&elem);
	free_var(&fact);
}


/*
 * log_var() -
 *
 *	Compute the logarithm of num in a given base.
 *
 *	Note: this routine chooses dscale of the result.
 */
static void
log_var(NumericVar *base, NumericVar *num, NumericVar *result)
{
	NumericVar	ln_base;
	NumericVar	ln_num;
	int			ln_base_dweight;
	int			ln_num_dweight;
	int			result_dweight;
	int			rscale;
	int			ln_base_rscale;
	int			ln_num_rscale;

	quick_init_var(&ln_base);
	quick_init_var(&ln_num);

	/* Estimated dweights of ln(base), ln(num) and the final result */
	ln_base_dweight = estimate_ln_dweight(base);
	ln_num_dweight = estimate_ln_dweight(num);
	result_dweight = ln_num_dweight - ln_base_dweight;

	/*
	 * Select the scale of the result so that it will have at least
	 * NUMERIC_MIN_SIG_DIGITS significant digits and is not less than either
	 * input's display scale.
	 */
	rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, num->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/*
	 * Set the scales for ln(base) and ln(num) so that they each have more
	 * significant digits than the final result.
	 */
	ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
	ln_base_rscale = Max(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
	ln_num_rscale = Max(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	/* Form natural logarithms */
	ln_var(base, &ln_base, ln_base_rscale);
	ln_var(num, &ln_num, ln_num_rscale);

	/* Divide and round to the required scale */
	div_var_fast(&ln_num, &ln_base, result, rscale, HALF_ADJUST_ROUND);

	free_var(&ln_num);
	free_var(&ln_base);
}


/*
 * power_var() -
 *
 *	Raise base to the power of exp
 *
 *	Note: this routine chooses dscale of the result.
 */
static void
power_var(NumericVar *base, NumericVar *exp, NumericVar *result)
{
	NumericVar	ln_base;
	NumericVar	ln_num;
	int			ln_dweight;
	int			rscale;
	int			local_rscale;
	double		val;

	/* If exp can be represented as an integer, use power_var_int */
	if (exp->ndigits == 0 || exp->ndigits <= exp->weight + 1)
	{
		/* exact integer, but does it fit in int? */
		int64		expval64;

		if (numericvar_to_int64(exp, &expval64))
		{
			int			expval = (int) expval64;

			/* Test for overflow by reverse-conversion. */
			if ((int64) expval == expval64)
			{
				/* Okay, select rscale */
				rscale = NUMERIC_MIN_SIG_DIGITS;
				rscale = Max(rscale, base->dscale);
				rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
				rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

				power_var_int(base, expval, result, rscale);
				return;
			}
		}
	}

	/*
	 * This avoids log(0) for cases of 0 raised to a non-integer.  0 ^ 0 is
	 * handled by power_var_int().
	 */
	if (cmp_var(base, &const_zero) == 0)
	{
		set_var_from_var(&const_zero, result);
		result->dscale = NUMERIC_MIN_SIG_DIGITS;	/* no need to round */
		return;
	}

	quick_init_var(&ln_base);
	quick_init_var(&ln_num);

	/*----------
	 * Decide on the scale for the ln() calculation.  For this we need an
	 * estimate of the weight of the result, which we obtain by doing an
	 * initial low-precision calculation of exp * ln(base).
	 *
	 * We want result = e ^ (exp * ln(base))
	 * so result dweight = log10(result) = exp * ln(base) * log10(e)
	 *
	 * We also perform a crude overflow test here so that we can exit early if
	 * the full-precision result is sure to overflow, and to guard against
	 * integer overflow when determining the scale for the real calculation.
	 * exp_var() supports inputs up to NUMERIC_MAX_RESULT_SCALE * 3, so the
	 * result will overflow if exp * ln(base) >= NUMERIC_MAX_RESULT_SCALE * 3.
	 * Since the values here are only approximations, we apply a small fuzz
	 * factor to this overflow test and let exp_var() determine the exact
	 * overflow threshold so that it is consistent for all inputs.
	 *----------
	 */
	ln_dweight = estimate_ln_dweight(base);

	local_rscale = 8 - ln_dweight;
	local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);
	local_rscale = Min(local_rscale, NUMERIC_MAX_DISPLAY_SCALE);

	ln_var(base, &ln_base, local_rscale);

	mul_var(&ln_base, exp, &ln_num, local_rscale);

	val = numericvar_to_double_no_overflow(&ln_num);

	/* initial overflow test with fuzz factor */
	if (Abs(val) > NUMERIC_MAX_RESULT_SCALE * 3.01)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));

	val *= 0.434294481903252;	/* approximate decimal result weight */

	/* choose the result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - (int) val;
	rscale = Max(rscale, base->dscale);
	rscale = Max(rscale, exp->dscale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	/* set the scale for the real exp * ln(base) calculation */
	local_rscale = rscale + (int) val - ln_dweight + 8;
	local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

	/* and do the real calculation */

	ln_var(base, &ln_base, local_rscale);

	mul_var(&ln_base, exp, &ln_num, local_rscale);

	exp_var(&ln_num, result, rscale);

	free_var(&ln_num);
	free_var(&ln_base);
}

/*
 * power_var_int() -
 *
 *	Raise base to the power of exp, where exp is an integer.
 */
static void
power_var_int(NumericVar *base, int exp, NumericVar *result, int rscale)
{
	double		f;
	int			p;
	int			i;
	int			sig_digits;
	unsigned int mask;
	bool		neg;
	NumericVar	base_prod;
	int			local_rscale;

	/* Handle some common special cases, as well as corner cases */
	switch (exp)
	{
		case 0:

			/*
			 * While 0 ^ 0 can be either 1 or indeterminate (error), we treat
			 * it as 1 because most programming languages do this. SQL:2003
			 * also requires a return value of 1.
			 * http://en.wikipedia.org/wiki/Exponentiation#Zero_to_the_zero_power
			 */
			set_var_from_var(&const_one, result);
			result->dscale = rscale;	/* no need to round */
			return;
		case 1:
			set_var_from_var(base, result);
			round_var(result, rscale, HALF_ADJUST_ROUND);
			return;
		case -1:
			div_var(&const_one, base, result, rscale, HALF_ADJUST_ROUND);
			return;
		case 2:
			mul_var(base, base, result, rscale);
			return;
		default:
			break;
	}

	/* Handle the special case where the base is zero */
	if (base->ndigits == 0)
	{
		if (exp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_DIVISION_BY_ZERO),
					 errmsg("division by zero")));
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * The general case repeatedly multiplies base according to the bit
	 * pattern of exp.
	 *
	 * First we need to estimate the weight of the result so that we know how
	 * many significant digits are needed.
	 */
	f = base->digits[0];
	p = base->weight * DEC_DIGITS;

	for (i = 1; i < base->ndigits && i * DEC_DIGITS < 16; i++)
	{
		f = f * NBASE + base->digits[i];
		p -= DEC_DIGITS;
	}

	/*----------
	 * We have base ~= f * 10^p
	 * so log10(result) = log10(base^exp) ~= exp * (log10(f) + p)
	 *----------
	 */
	f = exp * (log10(f) + p);

	/*
	 * Apply crude overflow/underflow tests so we can exit early if the result
	 * certainly will overflow/underflow.
	 */
	if (f > 3 * SHRT_MAX * DEC_DIGITS)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numeric format")));
	if (f + 1 < -rscale || f + 1 < -NUMERIC_MAX_DISPLAY_SCALE)
	{
		zero_var(result);
		result->dscale = rscale;
		return;
	}

	/*
	 * Approximate number of significant digits in the result.  Note that the
	 * underflow test above means that this is necessarily >= 0.
	 */
	sig_digits = 1 + rscale + (int) f;

	/*
	 * The multiplications to produce the result may introduce an error of up
	 * to around log10(abs(exp)) digits, so work with this many extra digits
	 * of precision (plus a few more for good measure).
	 */
	sig_digits += (int) log(Abs(exp)) + 8;

	/*
	 * Now we can proceed with the multiplications.
	 */
	neg = (exp < 0);
	mask = Abs(exp);

	init_var(&base_prod);
	set_var_from_var(base, &base_prod);

	if (mask & 1)
		set_var_from_var(base, result);
	else
		set_var_from_var(&const_one, result);

	while ((mask >>= 1) > 0)
	{
		/*
		 * Do the multiplications using rscales large enough to hold the
		 * results to the required number of significant digits, but don't
		 * waste time by exceeding the scales of the numbers themselves.
		 */
		local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
		local_rscale = Min(local_rscale, 2 * base_prod.dscale);
		local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

		mul_var(&base_prod, &base_prod, &base_prod, local_rscale);

		if (mask & 1)
		{
			local_rscale = sig_digits -
				(base_prod.weight + result->weight) * DEC_DIGITS;
			local_rscale = Min(local_rscale,
							   base_prod.dscale + result->dscale);
			local_rscale = Max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

			mul_var(&base_prod, result, result, local_rscale);
		}

		/*
		 * When abs(base) > 1, the number of digits to the left of the decimal
		 * point in base_prod doubles at each iteration, so if exp is large we
		 * could easily spend large amounts of time and memory space doing the
		 * multiplications.  But once the weight exceeds what will fit in
		 * int16, the final result is guaranteed to overflow (or underflow, if
		 * exp < 0), so we can give up before wasting too many cycles.
		 */
		if (base_prod.weight > SHRT_MAX || result->weight > SHRT_MAX)
		{
			/* overflow, unless neg, in which case result should be 0 */
			if (!neg)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value overflows numeric format")));
			zero_var(result);
			neg = false;
			break;
		}
	}

	free_var(&base_prod);

	/* Compensate for input sign, and round to requested rscale */
	if (neg)
		div_var_fast(&const_one, result, result, rscale, HALF_ADJUST_ROUND);
	else
		round_var(result, rscale, HALF_ADJUST_ROUND);
}


/* ----------------------------------------------------------------------
 *
 * Following are the lowest level functions that operate unsigned
 * on the variable level
 *
 * ----------------------------------------------------------------------
 */


/* ----------
 * cmp_abs() -
 *
 *	Compare the absolute values of var1 and var2
 *	Returns:	-1 for ABS(var1) < ABS(var2)
 *				0  for ABS(var1) == ABS(var2)
 *				1  for ABS(var1) > ABS(var2)
 * ----------
 */
static int
cmp_abs(const NumericVar *var1, const NumericVar *var2)
{
	return cmp_abs_common(var1->digits, var1->ndigits, var1->weight,
						  var2->digits, var2->ndigits, var2->weight);
}

/* ----------
 * cmp_abs_common() -
 *
 *	Main routine of cmp_abs(). This function can be used by both
 *	NumericVar and Numeric.
 * ----------
 */
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
			   const NumericDigit *var2digits, int var2ndigits, int var2weight)
{
	int			i1 = 0;
	int			i2 = 0;

	/* Check any digits before the first common digit */

	while (var1weight > var2weight && i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
		var1weight--;
	}
	while (var2weight > var1weight && i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
		var2weight--;
	}

	/* At this point, either w1 == w2 or we've run out of digits */

	if (var1weight == var2weight)
	{
		while (i1 < var1ndigits && i2 < var2ndigits)
		{
			int			stat = var1digits[i1++] - var2digits[i2++];

			if (stat)
			{
				if (stat > 0)
					return 1;
				return -1;
			}
		}
	}

	/*
	 * At this point, we've run out of digits on one side or the other; so any
	 * remaining nonzero digits imply that side is larger
	 */
	while (i1 < var1ndigits)
	{
		if (var1digits[i1++] != 0)
			return 1;
	}
	while (i2 < var2ndigits)
	{
		if (var2digits[i2++] != 0)
			return -1;
	}

	return 0;
}


/*
 * add_abs() -
 *
 *	Add the absolute values of two variables into result.
 *	result might point to one of the operands without danger.
 */
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			carry = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;
	NumericDigit tdig[NUMERIC_LOCAL_NDIG];

	res_weight = Max(var1->weight, var2->weight) + 1;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = tdig;
	if (res_ndigits > NUMERIC_LOCAL_NMAX)
		res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			carry += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			carry += var2digits[i2];

		if (carry >= NBASE)
		{
			res_digits[i] = carry - NBASE;
			carry = 1;
		}
		else
		{
			res_digits[i] = carry;
			carry = 0;
		}
	}

	Assert(carry == 0);			/* else we failed to allow for carry out */

	digitbuf_free(result);
	if (res_buf != tdig)
	{
		result->buf = res_buf;
		result->digits = res_digits;
	}
	else
	{
		result->digits = result->buf = result->ndb;
		memcpy(result->buf, res_buf, (sizeof(NumericDigit) * (res_ndigits +1)));
		result->digits ++;
	}
	result->ndigits = res_ndigits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}


/*
 * sub_abs()
 *
 *	Subtract the absolute value of var2 from the absolute value of var1
 *	and store in result. result might point to one of the operands
 *	without danger.
 *
 *	ABS(var1) MUST BE GREATER OR EQUAL ABS(var2) !!!
 */
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
	NumericDigit *res_buf;
	NumericDigit *res_digits;
	int			res_ndigits;
	int			res_weight;
	int			res_rscale,
				rscale1,
				rscale2;
	int			res_dscale;
	int			i,
				i1,
				i2;
	int			borrow = 0;

	/* copy these values into local vars for speed in inner loop */
	int			var1ndigits = var1->ndigits;
	int			var2ndigits = var2->ndigits;
	NumericDigit *var1digits = var1->digits;
	NumericDigit *var2digits = var2->digits;
	NumericDigit tdig[NUMERIC_LOCAL_NDIG];

	res_weight = var1->weight;

	res_dscale = Max(var1->dscale, var2->dscale);

	/* Note: here we are figuring rscale in base-NBASE digits */
	rscale1 = var1->ndigits - var1->weight - 1;
	rscale2 = var2->ndigits - var2->weight - 1;
	res_rscale = Max(rscale1, rscale2);

	res_ndigits = res_rscale + res_weight + 1;
	if (res_ndigits <= 0)
		res_ndigits = 1;

	res_buf = tdig;
	if (res_ndigits > NUMERIC_LOCAL_NMAX)
		res_buf = digitbuf_alloc(res_ndigits + 1);
	res_buf[0] = 0;				/* spare digit for later rounding */
	res_digits = res_buf + 1;

	i1 = res_rscale + var1->weight + 1;
	i2 = res_rscale + var2->weight + 1;
	for (i = res_ndigits - 1; i >= 0; i--)
	{
		i1--;
		i2--;
		if (i1 >= 0 && i1 < var1ndigits)
			borrow += var1digits[i1];
		if (i2 >= 0 && i2 < var2ndigits)
			borrow -= var2digits[i2];

		if (borrow < 0)
		{
			res_digits[i] = borrow + NBASE;
			borrow = -1;
		}
		else
		{
			res_digits[i] = borrow;
			borrow = 0;
		}
	}

	Assert(borrow == 0);		/* else caller gave us var1 < var2 */

	digitbuf_free(result);
	if (res_buf != tdig)
	{
		result->buf = res_buf;
		result->digits = res_digits;
	}
	else
	{
		result->digits = result->buf = result->ndb;
		memcpy(result->buf, res_buf, (sizeof(NumericDigit) * (res_ndigits +1)));
		result->digits ++;
	}
	result->ndigits = res_ndigits;
	result->weight = res_weight;
	result->dscale = res_dscale;

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/*
 * round_var
 *
 * Round the value of a variable to no more than rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * rounding before the decimal point.
 */
static void
round_var(NumericVar *var, int rscale, RoundType rt)
{
	NumericDigit *digits = var->digits;
	int			di;
	int			ndigits;
	int			carry;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di = 0, the value loses all digits, but could round up to 1 if its
	 * first extra digit is >= 5.  If di < 0 the result must be 0.
	 */
	if (di < 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		/* 0, or number of decimal digits to keep in last NBASE digit */
		di %= DEC_DIGITS;

		if (ndigits < var->ndigits ||
			(ndigits == var->ndigits && di > 0))
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/*
			 * di must be zero
			 * 1. values above 0.5 go up
			 * 2. values of exactly 0.5 go up for normal rounding
			 * 3. values of exactly 0.5 go to the nearest even number for Banker's Rounding
			 */
			if ((digits[ndigits] > HALF_NBASE) ||
				(rt == HALF_ADJUST_ROUND && digits[ndigits] == HALF_NBASE) ||
				(rt == BANKER_ROUND && (digits[ndigits - 1] & 1) && digits[ndigits] == HALF_NBASE))
				carry = 1;
			else
				carry = 0;
#else
			if (di == 0)
			{
				/*
				 * 1. values above 0.5 go up
				 * 2. values of exactly 0.5 go up for normal rounding
				 * 3. values of exactly 0.5 go to the nearest even number for Banker's Rounding
				 */
				if ((digits[ndigits] > HALF_NBASE) ||
					(rt == HALF_ADJUST_ROUND && digits[ndigits] == HALF_NBASE) ||
					(rt == BANKER_ROUND && (digits[ndigits - 1] & 1) &&
					digits[ndigits] == HALF_NBASE))
					carry = 1;
				else
					carry = 0;
			}
			else
			{
				/* Must round within last NBASE digit */
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
				carry = 0;
				if (extra >= pow10 / 2)
				{
					pow10 += digits[ndigits];
					if (pow10 >= NBASE)
					{
						pow10 -= NBASE;
						carry = 1;
					}
					digits[ndigits] = pow10;
				}
			}
#endif

			/* Propagate carry if needed */
			while (carry)
			{
				carry += digits[--ndigits];
				if (carry >= NBASE)
				{
					digits[ndigits] = carry - NBASE;
					carry = 1;
				}
				else
				{
					digits[ndigits] = carry;
					carry = 0;
				}
			}

			if (ndigits < 0)
			{
				Assert(ndigits == -1);	/* better not have added > 1 digit */
				Assert(var->digits > var->buf);
				var->digits--;
				var->ndigits++;
				var->weight++;
			}
		}
	}
	if (ORA_MODE)
	{
		check_ora_number_overflow(var, false);
	}
}

/*
 * trunc_var
 *
 * Truncate (towards zero) the value of a variable at rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * truncation before the decimal point.
 */
static void
trunc_var(NumericVar *var, int rscale)
{
	int			di;
	int			ndigits;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di <= 0, the value loses all digits.
	 */
	if (di <= 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		if (ndigits <= var->ndigits)
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* no within-digit stuff to worry about */
#else
			/* 0, or number of decimal digits to keep in last NBASE digit */
			di %= DEC_DIGITS;

			if (di > 0)
			{
				/* Must truncate within last NBASE digit */
				NumericDigit *digits = var->digits;
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
			}
#endif
		}
	}
}

/*
 * strip_var
 *
 * Strip any leading and trailing zeroes from a numeric variable
 */
static void
strip_var(NumericVar *var)
{
	NumericDigit *digits = var->digits;
	int			ndigits = var->ndigits;

	/* Strip leading zeroes */
	while (ndigits > 0 && *digits == 0)
	{
		digits++;
		var->weight--;
		ndigits--;
	}

	/* Strip trailing zeroes */
	while (ndigits > 0 && digits[ndigits - 1] == 0)
		ndigits--;

	/* If it's zero, normalize the sign and weight */
	if (ndigits == 0)
	{
		var->sign = NUMERIC_POS;
		var->weight = 0;
	}

	var->digits = digits;
	var->ndigits = ndigits;
}


/* ----------------------------------------------------------------------
 *
 * Fast sum accumulator functions
 *
 * ----------------------------------------------------------------------
 */

/*
 * Reset the accumulator's value to zero.  The buffers to hold the digits
 * are not free'd.
 */
static void
accum_sum_reset(NumericSumAccum *accum)
{
	int			i;

	accum->dscale = 0;
	for (i = 0; i < accum->ndigits; i++)
	{
		accum->pos_digits[i] = 0;
		accum->neg_digits[i] = 0;
	}
}

/*
 * Accumulate a new value.
 */
static void
accum_sum_add(NumericSumAccum *accum, const NumericVar *val)
{
	int32	   *accum_digits;
	int			i,
				val_i;
	int			val_ndigits;
	NumericDigit *val_digits;

	/*
	 * If we have accumulated too many values since the last carry
	 * propagation, do it now, to avoid overflowing.  (We could allow more
	 * than NBASE - 1, if we reserved two extra digits, rather than one, for
	 * carry propagation.  But even with NBASE - 1, this needs to be done so
	 * seldom, that the performance difference is negligible.)
	 */
	if (accum->num_uncarried == NBASE - 1)
		accum_sum_carry(accum);

	/*
	 * Adjust the weight or scale of the old value, so that it can accommodate
	 * the new value.
	 */
	accum_sum_rescale(accum, val);

	/* */
	if (val->sign == NUMERIC_POS)
		accum_digits = accum->pos_digits;
	else
		accum_digits = accum->neg_digits;

	/* copy these values into local vars for speed in loop */
	val_ndigits = val->ndigits;
	val_digits = val->digits;

	i = accum->weight - val->weight;
	for (val_i = 0; val_i < val_ndigits; val_i++)
	{
		accum_digits[i] += (int32) val_digits[val_i];
		i++;
	}

	accum->num_uncarried++;
}

/*
 * Propagate carries.
 */
static void
accum_sum_carry(NumericSumAccum *accum)
{
	int			i;
	int			ndigits;
	int32	   *dig;
	int32		carry;
	int32		newdig = 0;

	/*
	 * If no new values have been added since last carry propagation, nothing
	 * to do.
	 */
	if (accum->num_uncarried == 0)
		return;

	/*
	 * We maintain that the weight of the accumulator is always one larger
	 * than needed to hold the current value, before carrying, to make sure
	 * there is enough space for the possible extra digit when carry is
	 * propagated.  We cannot expand the buffer here, unless we require
	 * callers of accum_sum_final() to switch to the right memory context.
	 */
	Assert(accum->pos_digits[0] == 0 && accum->neg_digits[0] == 0);

	ndigits = accum->ndigits;

	/* Propagate carry in the positive sum */
	dig = accum->pos_digits;
	carry = 0;
	for (i = ndigits - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE)
		{
			carry = newdig / NBASE;
			newdig -= carry * NBASE;
		}
		else
			carry = 0;
		dig[i] = newdig;
	}
	/* Did we use up the digit reserved for carry propagation? */
	if (newdig > 0)
		accum->have_carry_space = false;

	/* And the same for the negative sum */
	dig = accum->neg_digits;
	carry = 0;
	for (i = ndigits - 1; i >= 0; i--)
	{
		newdig = dig[i] + carry;
		if (newdig >= NBASE)
		{
			carry = newdig / NBASE;
			newdig -= carry * NBASE;
		}
		else
			carry = 0;
		dig[i] = newdig;
	}
	if (newdig > 0)
		accum->have_carry_space = false;

	accum->num_uncarried = 0;
}

/*
 * Re-scale accumulator to accommodate new value.
 *
 * If the new value has more digits than the current digit buffers in the
 * accumulator, enlarge the buffers.
 */
static void
accum_sum_rescale(NumericSumAccum *accum, const NumericVar *val)
{
	int			old_weight = accum->weight;
	int			old_ndigits = accum->ndigits;
	int			accum_ndigits;
	int			accum_weight;
	int			accum_rscale;
	int			val_rscale;

	accum_weight = old_weight;
	accum_ndigits = old_ndigits;

	/*
	 * Does the new value have a larger weight? If so, enlarge the buffers,
	 * and shift the existing value to the new weight, by adding leading
	 * zeros.
	 *
	 * We enforce that the accumulator always has a weight one larger than
	 * needed for the inputs, so that we have space for an extra digit at the
	 * final carry-propagation phase, if necessary.
	 */
	if (val->weight >= accum_weight)
	{
		accum_weight = val->weight + 1;
		accum_ndigits = accum_ndigits + (accum_weight - old_weight);
	}

	/*
	 * Even though the new value is small, we might've used up the space
	 * reserved for the carry digit in the last call to accum_sum_carry().  If
	 * so, enlarge to make room for another one.
	 */
	else if (!accum->have_carry_space)
	{
		accum_weight++;
		accum_ndigits++;
	}

	/* Is the new value wider on the right side? */
	accum_rscale = accum_ndigits - accum_weight - 1;
	val_rscale = val->ndigits - val->weight - 1;
	if (val_rscale > accum_rscale)
		accum_ndigits = accum_ndigits + (val_rscale - accum_rscale);

	if (accum_ndigits != old_ndigits ||
		accum_weight != old_weight)
	{
		int32	   *new_pos_digits;
		int32	   *new_neg_digits;
		int			weightdiff;

		weightdiff = accum_weight - old_weight;

		new_pos_digits = palloc0(accum_ndigits * sizeof(int32));
		new_neg_digits = palloc0(accum_ndigits * sizeof(int32));

		if (accum->pos_digits)
		{
			memcpy(&new_pos_digits[weightdiff], accum->pos_digits,
				   old_ndigits * sizeof(int32));
			pfree(accum->pos_digits);

			memcpy(&new_neg_digits[weightdiff], accum->neg_digits,
				   old_ndigits * sizeof(int32));
			pfree(accum->neg_digits);
		}

		accum->pos_digits = new_pos_digits;
		accum->neg_digits = new_neg_digits;

		accum->weight = accum_weight;
		accum->ndigits = accum_ndigits;

		Assert(accum->pos_digits[0] == 0 && accum->neg_digits[0] == 0);
		accum->have_carry_space = true;
	}

	if (val->dscale > accum->dscale)
		accum->dscale = val->dscale;
}

/*
 * Return the current value of the accumulator.  This perform final carry
 * propagation, and adds together the positive and negative sums.
 *
 * Unlike all the other routines, the caller is not required to switch to
 * the memory context that holds the accumulator.
 */
static void
accum_sum_final(NumericSumAccum *accum, NumericVar *result)
{
	int			i;
	NumericVar	pos_var;
	NumericVar	neg_var;

	if (accum->ndigits == 0)
	{
		set_var_from_var(&const_zero, result);
		return;
	}

	/* Perform final carry */
	accum_sum_carry(accum);

	/* Create NumericVars representing the positive and negative sums */
	init_var(&pos_var);
	init_var(&neg_var);

	pos_var.ndigits = neg_var.ndigits = accum->ndigits;
	pos_var.weight = neg_var.weight = accum->weight;
	pos_var.dscale = neg_var.dscale = accum->dscale;
	pos_var.sign = NUMERIC_POS;
	neg_var.sign = NUMERIC_NEG;

	pos_var.buf = pos_var.digits = digitbuf_alloc(accum->ndigits);
	neg_var.buf = neg_var.digits = digitbuf_alloc(accum->ndigits);

	for (i = 0; i < accum->ndigits; i++)
	{
		Assert(accum->pos_digits[i] < NBASE);
		pos_var.digits[i] = (int16) accum->pos_digits[i];

		Assert(accum->neg_digits[i] < NBASE);
		neg_var.digits[i] = (int16) accum->neg_digits[i];
	}

	/* And add them together */
	add_var(&pos_var, &neg_var, result);

	/* Remove leading/trailing zeroes */
	strip_var(result);
}

/*
 * Copy an accumulator's state.
 *
 * 'dst' is assumed to be uninitialized beforehand.  No attempt is made at
 * freeing old values.
 */
static void
accum_sum_copy(NumericSumAccum *dst, NumericSumAccum *src)
{
	dst->pos_digits = palloc(src->ndigits * sizeof(int32));
	dst->neg_digits = palloc(src->ndigits * sizeof(int32));

	memcpy(dst->pos_digits, src->pos_digits, src->ndigits * sizeof(int32));
	memcpy(dst->neg_digits, src->neg_digits, src->ndigits * sizeof(int32));
	dst->num_uncarried = src->num_uncarried;
	dst->ndigits = src->ndigits;
	dst->weight = src->weight;
	dst->dscale = src->dscale;
}

/*
 * Add the current value of 'accum2' into 'accum'.
 */
static void
accum_sum_combine(NumericSumAccum *accum, NumericSumAccum *accum2)
{
	NumericVar	tmp_var;

	init_var(&tmp_var);

	accum_sum_final(accum2, &tmp_var);
	accum_sum_add(accum, &tmp_var);

	free_var(&tmp_var);
}

/* Convert numeric to interval */
Datum
numeric_interval(PG_FUNCTION_ARGS)
{
	Datum           num = PG_GETARG_DATUM(0);
	const char     *fmt = "day";
	Datum           result;
	StringInfoData  str;
	int             str_len;
	char           *buf = NULL;
	char           *cp = NULL;

	CHECK_RETNULL_INIT();

	initStringInfo(&str);
	appendStringInfoString(&str, DatumGetCString(CHECK_RETNULL_DIRECTCALL1(numeric_out, num)));
	appendStringInfoString(&str, " ");
	appendStringInfoString(&str, fmt);
	cp = str.data;

	if (*cp == '.' || (*cp == '-' && *(cp + 1) == '.'))
	{
		str_len = str.len + 2;
		buf = (char *) palloc0(str_len);
		if (*cp == '.')
		{
			sprintf(buf, "0.%s", cp + 1);
		}
		else
		{
			sprintf(buf, "-0.%s", cp + 2);
		}
		resetStringInfo(&str);
		appendStringInfoString(&str, buf);
		pfree(buf);
	}

	result = CHECK_RETNULL_DIRECTCALL3(interval_in,
										CStringGetDatum(str.data),
										ObjectIdGetDatum(InvalidOid),
										Int32GetDatum(-1));
	pfree(str.data);
	CHECK_RETNULL_RETURN_DATUM(result);
}

/*
 * REF: numeric_avg_accum
 * Sum for text col or val, 
 * it will convert to 'text' type to numeric type and do it the same with numeric_avg_accum
 * param: 2281 25
 * result: 2281
 */
Datum
text_nu_avg_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
	{
		/*
		 * using to_number tansform the string to numeric
		 */
		 Datum dval = DirectFunctionCall1(orcl_text_tonumber, PG_GETARG_DATUM(1));
		do_numeric_accum(state, DatumGetNumeric(dval));
	}

	PG_RETURN_POINTER(state);
}

/*
 * REF: numeric_accum_inv
 * Generic inverse transition function for numeric aggregates
 * (with or without requirement for X^2).
 * it will convert to 'text' type to numeric type and do it the same with numeric_accum_inv
 * param: 2281 25
 * result: 2281
 *
 */
Datum
text_nu_accum_inv(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "numeric_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
		/* If we fail to perform the inverse transition, return NULL */
		 Datum dval = DirectFunctionCall1(orcl_text_tonumber, PG_GETARG_DATUM(1));
		if (!do_numeric_discard(state, DatumGetNumeric(dval)))
			PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(state);
}

Datum
text_nu_sum(PG_FUNCTION_ARGS)
{
	return numeric_sum(fcinfo);
}

Datum
text_nu_avg_combine(PG_FUNCTION_ARGS)
{
	return numeric_avg_combine(fcinfo);
}

Datum
text_nu_avg_serialize(PG_FUNCTION_ARGS)
{
	return numeric_avg_serialize(fcinfo);
}

Datum
text_nu_avg_deserialize(PG_FUNCTION_ARGS)
{
	return numeric_avg_deserialize(fcinfo);
}

/*
 * REF: numeric_avg_accum
 * Sum for varchar2 col or val, 
 * it will convert to 'varchar2' type to numeric type and do it the same with numeric_avg_accum
 * param: 2281 781
 * result: 2281
 */
Datum
varchar2_nu_avg_accum(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Create the state data on the first call */
	if (state == NULL)
		state = makeNumericAggState(fcinfo, false);

	if (!PG_ARGISNULL(1))
	{
		/*
		 * using to_number tansform the string to numeric
		 */	
		 Datum dval = DirectFunctionCall1(orcl_text_tonumber, PG_GETARG_DATUM(1));
		do_numeric_accum(state, DatumGetNumeric(dval));
	}

	PG_RETURN_POINTER(state);
}

bool
numeric_is_zero_fast(Numeric num)
{
	NumericVar var;

	init_var_from_num(num, &var);
	return var.ndigits == 0;
}

bool
numeric_is_zero(Numeric num)
{
	NumericVar var;

	init_var_from_num(num, &var);
	return (var.ndigits == 0) && (var.sign == NUMERIC_POS || var.sign == NUMERIC_NEG);
	}

/*
 * REF: numeric_accum_inv
 * Generic inverse transition function for numeric aggregates
 * (with or without requirement for X^2).
 * it will convert to 'varchar2' type to numeric type and do it the same with numeric_accum_inv
 * param: 2281 25
 * result: 2281
 *
 */
Datum
varchar2_nu_accum_inv(PG_FUNCTION_ARGS)
{
	NumericAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

	/* Should not get here with no state */
	if (state == NULL)
		elog(ERROR, "numeric_accum_inv called with NULL state");

	if (!PG_ARGISNULL(1))
	{
		/* If we fail to perform the inverse transition, return NULL */
		Datum dval = DirectFunctionCall1(orcl_text_tonumber, PG_GETARG_DATUM(1));
		if (!do_numeric_discard(state, DatumGetNumeric(dval)))
			PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(state);
}

/*
 * Bourdary check of NUMBER type.
 *
 * In opentenbase_ora, the min abs-value of number is 10^-130, any value less than it
 * will be stored as zero; the max abs-value of number is 10^126, any value
 * large or equal to it will throw a `numeric overflow` error.
 */
static void
check_ora_number_overflow(NumericVar *var, bool report_error)
{
    NumericVar *pos_min;
    NumericVar *pos_max;

    if (var->ndigits == 0) {
        return;
    }

    switch (var->subtype)
    {
        case NUMERIC_BINARY_FLOAT:
            if (cmp_var(var, &const_ora_binary_float_pos_max) > 0)
                /* Convert values greater than INF to INF */
                set_var_from_var(&const_ora_binary_float_pos_inf, var);
            else if (cmp_var(var, &const_zero) < 0 && cmp_abs(var, &const_ora_binary_float_pos_max) > 0)
                /* Convert values less than -INF to -INF */
                set_var_from_var(&const_ora_binary_float_neg_inf, var);
            else if ((cmp_var(var, &const_zero) < 0 && cmp_abs(var, &const_ora_binary_float_pos_min) < 0)
                     || (cmp_var(var, &const_zero) > 0 && cmp_abs(var, &const_ora_binary_float_pos_min) < 0))
                /* Convert values between -const_ora_binary_float_pos_min and  const_ora_binary_float_pos_min to zero */
                set_var_from_var(&const_zero, var);
            break;
        case NUMERIC_BINARY_DOUBLE:
            if (cmp_var(var, &const_ora_binary_double_pos_max) > 0)
                /* Convert values greater than INF to INF */
                set_var_from_var(&const_ora_binary_double_pos_inf, var);
            else if (cmp_var(var, &const_zero) < 0 && cmp_abs(var, &const_ora_binary_double_pos_max) > 0)
                /* Convert values less than -INF to -INF */
                set_var_from_var(&const_ora_binary_double_neg_inf, var);
            else if ((cmp_var(var, &const_zero) < 0 && cmp_abs(var, &const_ora_binary_double_pos_min) < 0)
                     || (cmp_var(var, &const_zero) > 0 && cmp_abs(var, &const_ora_binary_double_pos_min) < 0))
                /* Convert values between -const_ora_binary_double_pos_min and  const_ora_binary_double_pos_min to zero */
                set_var_from_var(&const_zero, var);
            break;
        default:
            /* for NUMERIC_NUMBER and NUMERIC_FLOAT */
            pos_min = (NumericVar *) &const_ora_min;
            pos_max = (NumericVar *) &const_ora_max;
            if (cmp_abs(var, pos_min) < 0)
            {
                if (!report_error)
                    var->ndigits = 0;
                else
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("numeric overflow")));
                }
            }

            if (cmp_abs(var, pos_max) >= 0)
            {
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("numeric overflow")));
            }
    }
}

static void
adjust_numeric_var(NumericVar *result, int rscale, RoundType rt, bool ora_scale)
{
	if (ORA_MODE && rt != TRUNCATE_NUM && ora_scale)
	{
		strip_var(result);
		if (rt != TRUNCATE_ROUND)
		{
			check_ora_number_overflow(result, true);
		}
		if (adjust_scale_ora(result))
		{
			rscale = result->dscale;
			/* In opentenbase_ora mode, when the number of digits exceeds 40, ROUND is preferred over
			 * TRUNCATE. */
			rt = (rt == TRUNCATE_ROUND) ? HALF_ADJUST_ROUND : rt;
		}
	}

	if (rt == TRUNCATE_ROUND || rt == TRUNCATE_NUM)
	{
		trunc_var(result, rscale);
	}
	else
	{
		round_var(result, rscale, rt);

		if (result->dscale < 0 && result->weight > 0)
			result->dscale = 0;
	}
}

/*
 * opentenbase_ora use 20 byte to save 20 base-100 digits, each byte save 0 - 99, so the
 * precision of number in opentenbase_ora is 39 or 40 decimal digits depending on the
 * position of the decimal point.
 * If the highest weight digit has only one digit (1-9), and the total number of
 * significant digits exceeds 40, then the number will be rounded to 39 digits,
 * else rounded to 40 digits.
 *
 * We assume m is the number of significant digits before the decimal point, n
 * is the number of significant digits after the decimal point, m >= 0, n >= 0.
 *
 * 1. m + n < 40, round is not required.
 * 2: m + n = 40, round is required if m and n is odd number.
 * 3: m + n > 40, round is required.
 *
 * In PgSQL, we use 10000-based digits to store numeric, so we can skip round if
 * the NumericVar->ndigits less than or equal to 10.
 *
 * If the numeric has integer part, then the rounded position is
 * Min(n, 40 - ((m + 1) / 2 * 2)).
 *
 * Following list some examples:
 * 12345000,  m = 8, n = 0
 * 12345,     m = 5, n = 0
 * 12.00345,  m = 2, n = 5
 * 0.00012345,m = 0, n = 5
 *
 * Following list some numeric example, and all these need round.
 *
 * Example-1: 123456789123456789123456789123456789123456789(m = 45, n = 0)
 *            123456789123456789123456789123456789123000000
 * Example-2: 12345678912345678912345678912345678912345678(m = 44, n = 0)
 *            12345678912345678912345678912345678912350000
 * Example-3: 1234567891234567891234567891234567891.345(m = 37, n = 3)
 *            1234567891234567891234567891234567891.35
 * Example-4: 12345678912345678912345678912345678912.34(m = 38, n = 2)
 *            12345678912345678912345678912345678912.34
 * Example-5: 0.01234567891234567891234567891234567891234(m = 0, n = 40)
 *            0.0123456789123456789123456789123456789123
 */
static bool
adjust_scale_ora(NumericVar *var)
{
	NumericDigit *digits;
	int           ndig;
	int           highest_valid_digits = DEC_DIGITS;
	int           lowest_valid_digits = DEC_DIGITS;
	int           valid_digits;
	int           highest_base_10;

	/*
	 * fast check whether need to rounded
	 * Number of ndigits less than or equal to 10, which means number of total
	 * significant digits less than or equal to 40, round is not required.
	 */
	if (var->ndigits <= 10)
	{
		check_ora_number_overflow(var, false);
		return false;
	}

	digits = var->digits;
	ndig = var->ndigits;

	/*
	 * highest valid digit number count from right in the highest
	 * base-NBASE digit when trim leading zeros
	 */
	highest_valid_digits = HIGHEST_DIGIT_IN_BASE_DIGIT(digits[0]);
	/*
	 * lowest valid digit number count from left in the lowest
	 * base-NBASE digit when trim tailing zeros
	 */
	lowest_valid_digits = DEC_DIGITS - LOWEST_DIGIT_IN_BASE_DIGIT(digits[ndig - 1]) + 1;

	/* number of valid digits in numeric*/
	valid_digits = highest_valid_digits + lowest_valid_digits + (ndig - 2) * DEC_DIGITS;

	/*
	 *When greater than 0, it count from 1, and when less than 0, it count from -1.
	 * Like: 1 -> 1, 10 -> 2, 0.1 -> -1, 0.01 -> -2
	 */
	if (var->weight >= 0)
		highest_base_10 = var->weight * DEC_DIGITS + highest_valid_digits;
	else
		highest_base_10 = (var->weight + 1) * DEC_DIGITS - (DEC_DIGITS - highest_valid_digits + 1);

/*
 * opentenbase_ora use most 20 bytes to save NUMBER type, and the total length of NUMBER
 * type is 40, we define two macros to represent it.
 * https://docs.opentenbase_ora.com/en/database/opentenbase_ora/opentenbase-ora-database/19/sqlrf/Data-Types.html#GUID-9401BC04-81C4-4CD5-99E7-C5E25C83F608
 */
#define ORA_NUMBER_MAX_SIZE 20
#define ORA_NUMBER_MAX_LEN  40
	if (highest_base_10 > 0)
	{
		int int_part_size = (highest_base_10 + 1) / 2;
		int dec_part_size = ORA_NUMBER_MAX_SIZE - int_part_size;
		if (valid_digits > ORA_NUMBER_MAX_LEN ||
			(valid_digits == ORA_NUMBER_MAX_LEN && highest_base_10 % 2 == 1))
		{
			int scale = 2 * dec_part_size;
			var->dscale = scale;
			return true;
		}
	}
	else
	{
		/* Avoid unnecessary round */
		if (valid_digits > ORA_NUMBER_MAX_LEN ||
			(valid_digits == ORA_NUMBER_MAX_LEN && (highest_base_10 + 1) % 2 == -1))
		{
			int scale = ((-highest_base_10 - 1) / 2) * 2 + ORA_NUMBER_MAX_LEN;
			var->dscale = scale;
			return true;
		}
	}

	return false;
}

#include "./numericd.c"
#include "./numericf.c"

Numeric
makeNumericData128(int128 v)
{
	Numeric	result;

	result = DatumGetNumeric(makeNumeric128(v, 0));
	return result;
}

static int128
getInt128FromNumeric(Numeric num)
{
	uint16		numFlags;
	int			scale = 0;
	int128		result;
	NumericVar	x;

	numFlags = NUMERIC_NB_FLAGBITS(num);
	if (NUMERIC_FLAG_IS_BI64(numFlags))
	{
		result = NUMERIC_64VALUE(num);
		scale = NUMERIC_BI_SCALE(num);
	}
	else if (NUMERIC_FLAG_IS_BI128(numFlags))
	{
		memcpy(&result, (num)->choice.n_bi.n_data, sizeof(int128));
		scale = NUMERIC_BI_SCALE(num);
	} else
	{
		/* Convert to variable format and thence to int8 */
		init_var_from_num(num, &x);

		if (!numericvar_to_int128(&x, &result))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					errmsg("bigint out of range")));
	}
	if (scale != 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("scale is not zero.")));
	return result;
}

int128
numericGetInt128(Numeric d)
{
	if (likely(!VARATT_IS_SHORT(d)))
	{
		return getInt128FromNumeric((Numeric) d);
	}
	else
	{
		elog(ERROR, "Should not be short numeric.");
		return -1;
	}
}

int128
datumGetInt128(Datum d)
{
	return numericGetInt128(DatumGetNumeric(d));
}

void
setInt128ByDatum(Datum d, int128 v)
{
	struct varlena		*var;
	union NumericChoice *choice;
	Numeric				n;

	if (likely(!VARATT_IS_SHORT(d)))
	{
		n = (Numeric) d;
		memcpy(n->choice.n_bi.n_data, &v, sizeof(int128));
	}
	else
	{
		var = (struct varlena *) d;
		choice = (union NumericChoice *) VARDATA_SHORT(var);
		memcpy(choice->n_bi.n_data, &v, sizeof(int128));
	}
}

/*
 * Whether the unknown type const is a numeric which can't be treated as int2/int4/int8?
 *
 * Note: We use this function to judge whether the unknown type
 * can't be treated as int2/int4/int8 in comparison operation.
 */
bool
is_unknown_a_numeric(Node *unknown, Oid other_type)
{
	if (IsA(unknown, Const))
	{
		Const *con = (Const *) unknown;
		int64 val64;

		/* The const is null, we think it is not a numeric */
		if (con->constisnull || con->constvalue == 0)
			return false;

		/* could be an oversize integer as well as a float ... */
		if (scanint8(DatumGetCString(con->constvalue), true, &val64))
		{
			/*
			 * It might actually fit in int32 or int16. Probably only INT_MIN can
			 * occur, but we'll code the test generally just to be sure.
			 */
			int32	val32 = (int32) val64;
			int16	val16 = (int16) val64;

			if (other_type == INT8OID)
				return false;
			else if (other_type == INT2OID)
				return val64 != (int64) val16 ;
			else if (other_type == INT4OID)
				return val64 != (int64) val32;
			else
				return true;
		}
		else
			return true;
	}
	else
		return false;
}

/*
 * in_range support function for numeric.
 */
Datum
in_range_numeric_numeric(PG_FUNCTION_ARGS)
{
	Numeric		val = PG_GETARG_NUMERIC(0);
	Numeric		base = PG_GETARG_NUMERIC(1);
	Numeric		offset = PG_GETARG_NUMERIC(2);
	bool		sub = PG_GETARG_BOOL(3);
	bool		less = PG_GETARG_BOOL(4);
	bool		result;

	/*
	 * Reject negative or NaN offset.  Negative is per spec, and NaN is
	 * because appropriate semantics for that seem non-obvious.
	 */
	if (NUMERIC_IS_NAN(offset) || NUMERIC_SIGN(offset) == NUMERIC_NEG)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PRECEDING_FOLLOWING_SIZE),
				 errmsg("invalid preceding or following size in window function")));

	/*
	 * Deal with cases where val and/or base is NaN, following the rule that
	 * NaN sorts after non-NaN (cf cmp_numerics).  The offset cannot affect
	 * the conclusion.
	 */
	if (NUMERIC_IS_NAN(val))
	{
		if (NUMERIC_IS_NAN(base))
			result = true;		/* NAN = NAN */
		else
			result = !less;		/* NAN > non-NAN */
	}
	else if (NUMERIC_IS_NAN(base))
	{
		result = less;			/* non-NAN < NAN */
	}
	else
	{
		/*
		 * Otherwise go ahead and compute base +/- offset.  While it's
		 * possible for this to overflow the numeric format, it's unlikely
		 * enough that we don't take measures to prevent it.
		 */
		NumericVar	valv;
		NumericVar	basev;
		NumericVar	offsetv;
		NumericVar	sum;

		init_var_from_num(val, &valv);
		init_var_from_num(base, &basev);
		init_var_from_num(offset, &offsetv);
		init_var(&sum);

		if (sub)
			sub_var(&basev, &offsetv, &sum);
		else
			add_var(&basev, &offsetv, &sum);

		if (less)
			result = (cmp_var(&valv, &sum) <= 0);
		else
			result = (cmp_var(&valv, &sum) >= 0);

		free_var(&sum);
	}

	PG_FREE_IF_COPY(val, 0);
	PG_FREE_IF_COPY(base, 1);
	PG_FREE_IF_COPY(offset, 2);

	PG_RETURN_BOOL(result);
}
