/*-------------------------------------------------------------------------
 *
 * numeric.h
 *	  Definitions for the exact numeric data type of Postgres
 *
 * Original coding 1998, Jan Wieck.  Heavily revised 2003, Tom Lane.
 *
 * Copyright (c) 1998-2017, PostgreSQL Global Development Group
 *
 * src/include/utils/numeric.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PG_NUMERIC_H_
#define _PG_NUMERIC_H_

#include "fmgr.h"

/*
 * Limit on the precision (and hence scale) specifiable in a NUMERIC typmod.
 * Note that the implementation limit on the length of a numeric value is
 * much larger --- beware of what you use this for!
 */
#define NUMERIC_MAX_PRECISION		1000

/*
 * Internal limits on the scales chosen for calculation results
 */
#define NUMERIC_MAX_DISPLAY_SCALE	NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE	0

#define NUMERIC_MAX_RESULT_SCALE	(NUMERIC_MAX_PRECISION * 2)

/*
 * For inherently inexact calculations such as division and square root,
 * we try to get at least this many significant digits; the idea is to
 * deliver a result no worse than float8 would.
 */
#define NUMERIC_MIN_SIG_DIGITS		(ORA_MODE ? 40 : 16)

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

#if 1
#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

typedef int16 NumericDigit;
#endif

/*
 * numeric subtype in opentenbase_ora mode (NUMBER, FLOAT, BINARY_FLOAT or BINARY_DOUBLE)
 */
typedef enum NumericSubtype
{
    NUMERIC_NUMERIC,
    NUMERIC_NUMBER,
    NUMERIC_FLOAT,
    NUMERIC_BINARY_FLOAT,
    NUMERIC_BINARY_DOUBLE,
} NumericSubtype;

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

/*
 * NumericBi is used for big integer(bi64 or bi128)
 * n_header stores mark bits and scale of big integer, first 4 bits to
 * distinguish bi64 and bi128, next 4 bits are not used, the last 8 bits
 * store the scale of bit integer, scale value is between 0 and 38.
 * n_data store big integer value(int64 or int128)
 *
 */
struct NumericBi
{
	uint16 n_header;
	uint8 n_data[1];
};

/*
 * Add NumericBi struct to NumericChoice
 */
union NumericChoice
{
	uint16		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
	struct NumericBi n_bi;		/* Short form (2-byte header) */
};

struct NumericData
{
	int32		vl_len_;		  /* varlena header (do not touch directly!) */
	union NumericChoice choice;   /* choice of format */
};

struct NumericData;
typedef struct NumericData *Numeric;
typedef struct NumericData NumericData;

/*
 * TRUNCATE_ROUND: Truncate (towards zero) the value of a variable
 *                 at rscale decimal digits after the decimal point.
 *                 For mod & remainder, we need round before trunc in opentenbase_ora mode
 * HALF_ADJUST_ROUND: Round the value of a variable to no more than
 *                 after the decimal point. Values below 0.5 go down
 *                 and others go up.
 * BANKER_ROUND: values below 0.5 go down and values above 0.5 go up.
 *               Values of exactly 0.5 go to the nearest even number.
 * TRUNCATE_NUM: Trunc number without round
 */
typedef enum
{
	TRUNCATE_ROUND,  
	HALF_ADJUST_ROUND,
	BANKER_ROUND,
	TRUNCATE_NUM  
} RoundType;

/*
 * Interpretation of high bits.
 */
#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_SHORT 0x8000
#define NUMERIC_NAN 0xC000

/*
 * big integer macro
 * n_header is the mark bits of numeric struct, when numeric is NAN, n_header
 * marked 0xC000. To distinguish bi64, bi128 and numeric, we use 0xD000 to mark
 * bi64, 0xE000 marks bi128, others are numeric type.
 */
#define NUMERIC_64 0xD000
#define NUMERIC_128 0xE000
#define NUMERIC_BI_MASK 0xF000
#define NUMERIC_BI_SCALEMASK 0x00FF

/*
 * +Inf and -Inf support for BINARY_FLOAT and BINARY_DOUBLE in opentenbase_ora mode.
 * We make the first four bits of the n header be set to 0xF to indicate that this is an Inf number.
 * The fifth bit marks the specific type of Inf.
 */
#define NUMERIC_POS_INF 0xF800
#define NUMERIC_NEG_INF 0xF000
#define NUMERIC_INF_MASK 0xF800
/*
 * Hardcoded precision limit - arbitrary, but must be small enough that
 * dscale values will fit in 14 bits.
 */
#define NUMERIC_MAX_PRECISION 1000

/*
 * Numeric scale range for opentenbase_ora compatibility
 */
#define NUMERIC_OPENTENBASE_ORA_MIN_SCALE -84
#define NUMERIC_OPENTENBASE_ORA_MAX_SCALE 127
#define NUMERIC_OPENTENBASE_ORA_MAX_PRECISION 38

/*
 * Float for opentenbase_ora number compatibility
 */
#define FLOAT_OPENTENBASE_ORA_MAX_PRECISION 38
#define FLOAT_OPENTENBASE_ORA_MAX_RANGE 126
#define OPENTENBASE_ORA_FLOAT_CALCULATION_FACTOR 0.30103

#define OPENTENBASE_ORA_DECIMAL_DEFAULT_PRECISION 38
#define OPENTENBASE_ORA_DECIMAL_DEFAULT_SCALE 0
/*
 * BINARY_FLOAT and BINARY_DOUBLE for opentenbase_ora compatibility
 * The real precision of BINARY_FLOAT and BINARY_DOUBLE is 9 and 17.
 *
 */
#define OPENTENBASE_ORA_BINARY_FLOAT_PRECISION (9 + NUMERIC_MAX_PRECISION)
#define OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION (17 + NUMERIC_MAX_PRECISION)

#define DEFAULT_ORACL_NUMERIC_TYPEMOD (-1)
#define ORACL_BINARY_FLOAT_TYPEMOD \
((((int32) (NUMERIC_MAX_PRECISION + OPENTENBASE_ORA_BINARY_FLOAT_PRECISION) & 0xffff) << 16) + VARHDRSZ)

#define ORACL_BINARY_DOUBLE_TYPEMOD \
((((int32) (NUMERIC_MAX_PRECISION + OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION) & 0xffff) << 16) + VARHDRSZ)

/*
 * Internal limits on the scales chosen for calculation results
 */
#define NUMERIC_MAX_DISPLAY_SCALE NUMERIC_MAX_PRECISION
#define NUMERIC_MIN_DISPLAY_SCALE 0

#define NUMERIC_MAX_RESULT_SCALE (NUMERIC_MAX_PRECISION * 2)

/*
 * numeric nan data length exclude header
 */
#define NUMERIC_NAN_DATALENGTH 0
#define NUMERIC_ZERO_DATALENGTH 2

#define NUMERIC_HDRSZ (VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_NB_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_BI_MASK)  // nan or biginteger
#define NUMERIC_INF_FLAG_BITS(n) ((n)->choice.n_header & NUMERIC_INF_MASK)

#define NUMERIC_IS_NAN(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n) (NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)
#define NUMERIC_IS_POS_INF(n) (NUMERIC_INF_FLAG_BITS(n) == NUMERIC_POS_INF)
#define NUMERIC_IS_NEG_INF(n) (NUMERIC_INF_FLAG_BITS(n) == NUMERIC_NEG_INF)
#define NUMERIC_IS_INF(n) (NUMERIC_IS_POS_INF(n) || NUMERIC_IS_NEG_INF(n))
/*
 * big integer macro
 * determine the type of numeric
 * verify whether a numeric data is NAN or BI by it's flag.
 */
#define NUMERIC_FLAG_IS_NAN(n) (n == NUMERIC_NAN)
#define NUMERIC_FLAG_IS_BI64(n) (n == NUMERIC_64)
#define NUMERIC_FLAG_IS_BI128(n) (n == NUMERIC_128)
#define NUMERIC_FLAG_IS_BI(n) (NUMERIC_FLAG_IS_BI64(n) || NUMERIC_FLAG_IS_BI128(n))
#define NUMERIC_FLAG_IS_NANORBI(n) (NUMERIC_FLAG_IS_NAN(n) || NUMERIC_FLAG_IS_BI(n))

/*
 * big integer macro
 * determine the type of numeric
 * verify whether a numeric data is NAN or BI by itself.
 */
#define NUMERIC_IS_BI64(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_64)
#define NUMERIC_IS_BI128(n) (NUMERIC_NB_FLAGBITS(n) == NUMERIC_128)
#define NUMERIC_IS_BI(n) (NUMERIC_IS_BI64(n) || NUMERIC_IS_BI128(n))
#define NUMERIC_IS_NANORBI(n) (NUMERIC_IS_NAN(n) || NUMERIC_IS_BI(n))

/*
 * big integer macro
 * the size of bi64 or bi128 is fixed.
 * the size of bi64 is:
 *     4 bytes(vl_len_) + 2 bytes(n_header) + 8 bytes(int64) = 14 bytes
 * the size of bi128 is:
 *     4 bytes(vl_len_) + 2 bytes(n_header) + 16 bytes(int128) = 22 bytes
 */
#define NUMERIC_64SZ (NUMERIC_HDRSZ_SHORT + sizeof(int64))
#define NUMERIC_128SZ (NUMERIC_HDRSZ_SHORT + sizeof(int128))

/*
 * big integer macro
 * get the scale of bi64 or bi128.
 *     scale = n_header & NUMERIC_BI_SCALEMASK
 * get the value of bi64 or bi128
 *     convert NumericBi.n_data to int64 or int128 pointer, then
 *     assign value.
 */
#define NUMERIC_BI_SCALE(n) ((n)->choice.n_header & NUMERIC_BI_SCALEMASK)
#define NUMERIC_64VALUE(n) (*((int64*)((n)->choice.n_bi.n_data)))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_NAN, we want the short header;
 * otherwise, we want the long one.  Instead of testing against each value, we
 * can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_SIZE(n) (VARHDRSZ + sizeof(uint16) + (((NUMERIC_FLAGBITS(n) & 0x8000) == 0) ? sizeof(int16) : 0))

/*
 * Short format definitions.
 */
#define NUMERIC_SHORT_SIGN_MASK 0x2000
#define NUMERIC_SHORT_DSCALE_MASK 0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT 7
#define NUMERIC_SHORT_DSCALE_MAX (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK 0x0040
#define NUMERIC_SHORT_WEIGHT_MASK 0x003F
#define NUMERIC_SHORT_WEIGHT_MAX NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN (-(NUMERIC_SHORT_WEIGHT_MASK + 1))

/*
 * Extract sign, display scale, weight.
 */
#define NUMERIC_DSCALE_MASK 0x3FFF

#define NUMERIC_SIGN(n)                                                                                  \
	(NUMERIC_IS_INF(n)                                                                                   \
        ? NUMERIC_INF_FLAG_BITS(n)                                                                       \
        : (NUMERIC_IS_SHORT(n)                                                                           \
                ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? NUMERIC_NEG : NUMERIC_POS) \
                : NUMERIC_FLAGBITS(n)))

#define NUMERIC_DSCALE_NORMAL_I(dsn)                                                               \
	((dsn > NUMERIC_MAX_PRECISION) ? (dsn - NUMERIC_MAX_PRECISION) * (-1) : dsn)

#define NUMERIC_DSCALE_NORMAL(n)                                                                   \
	(ORA_MODE ? NUMERIC_DSCALE_NORMAL_I(NUMERIC_DSCALE(n)) : NUMERIC_DSCALE(n))

#define NUMERIC_DSCALE(n)                                                                          \
	(NUMERIC_IS_SHORT((n)) ? ((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) >>         \
	                             NUMERIC_SHORT_DSCALE_SHIFT                                        \
	                       : ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))

#define NUMERIC_WEIGHT(n)                                                                                         \
    (NUMERIC_IS_SHORT((n))                                                                                        \
            ? (((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) | \
                  ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK))                                     \
            : ((n)->choice.n_long.n_weight))
#define NUMERIC_DIGITS(num) (NUMERIC_IS_SHORT(num) ? (num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) ((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_CAN_BE_SHORT(scale, weight)                                                        \
	((scale) >= 0 && (scale) <= NUMERIC_SHORT_DSCALE_MAX &&                                        \
	 (weight) <= NUMERIC_SHORT_WEIGHT_MAX && (weight) >= NUMERIC_SHORT_WEIGHT_MIN)

#define NUMERICD_SIZE sizeof(NumericdVar)
#define NUMERICF_SIZE sizeof(NumericfVar)

#define HIGHEST_DIGIT_IN_BASE_DIGIT(base)                                                          \
	({                                                                                             \
		int valid_digits = DEC_DIGITS;                                                             \
		if (base < 10)                                                                             \
			valid_digits -= 3;                                                                     \
		else if (base < 100)                                                                       \
		{                                                                                          \
			valid_digits -= 2;                                                                     \
		}                                                                                          \
		else if (base < 1000)                                                                      \
		{                                                                                          \
			valid_digits -= 1;                                                                     \
		}                                                                                          \
		valid_digits;                                                                              \
	})

#define LOWEST_DIGIT_IN_BASE_DIGIT(base)                                                           \
	({                                                                                             \
		int valid_digits = DEC_DIGITS;                                                             \
		if (base % 10)                                                                             \
			valid_digits = 1;                                                                      \
		else if (base % 100)                                                                       \
		{                                                                                          \
			valid_digits = 2;                                                                      \
		}                                                                                          \
		else if (base % 1000)                                                                      \
		{                                                                                          \
			valid_digits = 3;                                                                      \
		}                                                                                          \
		valid_digits;                                                                              \
	})

#define INT128_HIGH(x) ((long long unsigned int) ((x >> 64) & 0xFFFFFFFFFFFFFFFF))
#define INT128_LOW(x) ((long long unsigned int) (x & 0xFFFFFFFFFFFFFFFF))

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
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, NUMERIC_NAN, NUMERIC_POS_INF, or NUMERIC_NEG_INF */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
    NumericSubtype subtype;     /* for opentenbase_ora compatiable */
    NumericDigit ndb[NUMERIC_LOCAL_NDIG];	/* local space for digits[] */
} NumericVar;

typedef struct NumericdVar
{
	int128			value;
    short           scale;
}
#if defined(pg_attribute_packed) && defined(pg_attribute_aligned)
pg_attribute_packed()
pg_attribute_aligned(2)
#endif
NumericdVar;

typedef struct NumericfVar
{
	int64			value;
    short           scale;
}
#if defined(pg_attribute_packed) && defined(pg_attribute_aligned)
pg_attribute_packed()
pg_attribute_aligned(2)
#endif
NumericfVar;

#define ConvertNumericfdInt(val) \
	((val)->scale >= 0 ? (int128) (val)->value / ScaleMultiplerExtd[(val)->scale] : (int128) (val)->value * ScaleMultiplerExtd[0 - (val)->scale])

#ifdef __OPENTENBASE_C__
/*
 * @Description: copy bi64 to ptr,  this operation no need to allocate memory
 * @IN  ptr: the numeric pointer
 * @IN  value: the value of bi64
 * @IN  scale: the scale of bi64
 */
#define MAKE_NUMERIC64(ptr, value, scale)                  \
    do                                                     \
    {                                                      \
        Numeric result = (Numeric)(ptr);                   \
        SET_VARSIZE(result, NUMERIC_64SZ);                 \
        result->choice.n_header = NUMERIC_64 + (scale);    \
        *((int64*)(result->choice.n_bi.n_data)) = (value); \
    } while (0)

/*
 * @Description: copy bi128 to ptr, this operation no need to allocate memory
 * @IN  ptr: the numeric pointer
 * @IN  value: the value of bi128
 * @IN  scale: the scale of bi128
 */
#define MAKE_NUMERIC128(ptr, value, scale)                            \
    do                                                                \
    {                                                                 \
        Numeric result = (Numeric)(ptr);                              \
        SET_VARSIZE(result, NUMERIC_128SZ);                           \
        result->choice.n_header = NUMERIC_128 + (scale);              \
        memcpy(result->choice.n_bi.n_data, (&value), sizeof(int128)); \
    } while (0)

/*
 * the header size of short numeric is 6 bytes.
 * the same to NUMERIC_HEADER_SIZE but for short numeric.
 */
#define SHORT_NUMERIC_HEADER_SIZE (VARHDRSZ + sizeof(uint16))

/*
 * the same to NUMERIC_NDIGITS but for short numeric.
 */
#define SHORT_NUMERIC_NDIGITS(num) \
    (AssertMacro(NUMERIC_IS_SHORT(num)), ((VARSIZE(num) - SHORT_NUMERIC_HEADER_SIZE) / sizeof(NumericDigit)))

/*
 * the same to NUMERIC_DIGITS but for short numeric.
 */
#define SHORT_NUMERIC_DIGITS(num) (AssertMacro(NUMERIC_IS_SHORT(num)), (num)->choice.n_short.n_data)
#endif

/*
 * fmgr interface macros
 */

#define DatumGetNumeric(X)		  ((Numeric) PG_DETOAST_DATUM(X))
#define DatumGetNumericCopy(X)	  ((Numeric) PG_DETOAST_DATUM_COPY(X))
#define NumericGetDatum(X)		  PointerGetDatum(X)
#define PG_GETARG_NUMERIC(n)	  DatumGetNumeric(PG_GETARG_DATUM(n))
#define PG_GETARG_NUMERIC_COPY(n) DatumGetNumericCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERIC(x)	  return NumericGetDatum(x)


#define NumericdGetDatum(X)		  PointerGetDatum(X)
#define DatumGetNumericd(X)       ((NumericdVar *) DatumGetPointer(X))
#define PG_GETARG_NUMERICD(n)     DatumGetNumericd(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERICD(x)	  return NumericdGetDatum(x)

#define NumericfGetDatum(X)		  PointerGetDatum(X)
#define DatumGetNumericf(X)       ((NumericfVar *) DatumGetPointer(X))
#define PG_GETARG_NUMERICF(n)     DatumGetNumericf(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERICF(x)	  return NumericfGetDatum(x)

#define GetNumericdVal(result, colvec)  result = (NumericdVar*) (colvec->cv_buf->head->next->buf)
#define SetNextNumericdVal(result)      result = (NumericdVar*) ((char*)result + NUMERICD_SIZE);
#define GetNumericfVal(result, colvec)  result = (NumericfVar*) (colvec->cv_buf->head->next->buf)
#define SetNextNumericfVal(result)      result = (NumericfVar*) ((char*)result + NUMERICF_SIZE);
#define SetResultVal(res, i, result)    res[i] = (char*) result;

/* Move these two initialization macros from numeric.c to numeric.h */
#define quick_init_var(v) \
	do { \
		(v)->buf = (v)->ndb;	\
		(v)->digits = NULL;     \
    	(v)->subtype = ORA_MODE ? NUMERIC_NUMBER : NUMERIC_NUMERIC; \
	} while (0)

#define init_var(v) \
	do { \
		quick_init_var((v));	\
		(v)->ndigits = (v)->weight = (v)->sign = (v)->dscale = 0; \
		(v)->subtype = ORA_MODE ? NUMERIC_NUMBER : NUMERIC_NUMERIC; \
	} while (0)

/*
 * Utility functions in numeric.c
 */
extern bool numeric_is_nan(Numeric num);
extern bool numeric_is_inf(Numeric num);
extern bool numeric_sign_equal(Numeric num1, Numeric num2);
int32		numeric_maximum_size(int32 typmod);
extern char *numeric_out_sci(Numeric num, int scale);
extern char *numeric_normalize(Numeric num);

extern Numeric numeric_add_opt_error(Numeric num1, Numeric num2,
					  bool *have_error);
extern Numeric numeric_sub_opt_error(Numeric num1, Numeric num2,
					  bool *have_error);
extern Numeric numeric_mul_opt_error(Numeric num1, Numeric num2,
					  bool *have_error);
extern Numeric numeric_div_opt_error(Numeric num1, Numeric num2,
					  bool *have_error);
extern Numeric numeric_mod_opt_error(Numeric num1, Numeric num2,
					  bool *have_error);
extern int32 numeric_int4_opt_error(Numeric num, bool *error);

/* numeric output internal function, output with trailing zeroes if exists */
extern char *numeric_out_internal(Numeric num);
extern bool numeric_is_zero_fast(Numeric num);
extern bool numeric_is_zero(Numeric num);
extern void init_var_from_num(Numeric num, NumericVar *dest);
extern int	cmp_numerics(Numeric num1, Numeric num2);
extern Numeric make_result(const NumericVar *var);
extern int enable_simple_numeric;

extern void suppres_trailing_zeroes(char *str);
extern void suppres_leading_zeroes(char *str);

extern Numeric makeNumericData128(int128 v);
extern int128 datumGetInt128(Datum d);
extern void setInt128ByDatum(Datum d, int128 v);
extern int128 numericGetInt128(Numeric d);

extern bool is_unknown_a_numeric(Node *unknown, Oid other_type);
extern bool numericvar_to_int64(const NumericVar *var, int64 *result);
extern char *get_str_from_var_sci(const NumericVar *var, int rscale);
extern Datum in_range_numeric_numeric(PG_FUNCTION_ARGS);
#endif							/* _PG_NUMERIC_H_ */
