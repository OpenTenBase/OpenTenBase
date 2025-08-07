/*-------------------------------------------------------------------------
 *
 * biginteger.h
 *	  Definitions for the big integer data type
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/biginteger.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BIGINTERGER_H
#define BIGINTERGER_H

#include "postgres.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/numeric.h"

/*
 * int32 integer:
 *    int32 value must in [-2147483648, 2147483647]
 *    abs(int32_value) < 10^11, so the max dscale of int32 can represent is 10.
 *
 * int64 integer:
 *    int64 value must in [-9223372036854775808, 9223372036854775807]
 *    abs(int64_value) < 10^20, so the max dscale of int64 can represent is 19.
 *
 * int128 integer:
 *    int128 value must in [-170141183460469231731687303715884105728,
 *                           170141183460469231731687303715884105727]
 *    abs(int128_value) < 10^40, so the max dscale of int128 can represent is
 *    39, but some numbers with 39 digits can't be stored by int128, so just
 *    choose 38 instead because one number with 38 digits must can be stored
 *    by int128.
 */
#define MAXINT32DIGIT 10
#define MAXINT64DIGIT 19
#define MAXINT128DIGIT 38

/* the flag of failed dscale encoding */
#define FAILED_DSCALE_ENCODING -1

/* the value of 10^18, it is used for print int128 data */
#define P10_INT64 1000000000000000000LL /* 18 zeroes */

/* this macro represents the first dimension index of BiAggFunMatrix */
#define BI_AGG_ADD 0
#define BI_AGG_SMALLER 1
#define BI_AGG_LARGER 2

/*
 * determine whether one num can convert to int64 or int128 by the
 * num's scale, this macro is used for both executor and storage modules.
 */
#define CAN_CONVERT_BI64(whole_scale) ((whole_scale) <= (MAXINT64DIGIT - 1))
#define CAN_CONVERT_BI128(whole_scale) ((whole_scale) <= MAXINT128DIGIT)

/* jduge whether int64 data can be transformed to int8 */
#define INT64_INT8_EQ(data) ((data) == (int64)((int8)(data)))

/* jduge whether int64 data can be transformed to int16 */
#define INT64_INT16_EQ(data) ((data) == (int64)((int16)(data)))

/* jduge whether int64 data can be transformed to int32 */
#define INT64_INT32_EQ(data) ((data) == (int64)((int32)(data)))

/* jduge whether int128 data can be transformed to int64 */
#define INT128_INT64_EQ(data) ((data) == (int128)((int64)(data)))

/*
 * big integer common operators, we implement corresponding
 * functions of big integer for higher performance.
 */
enum biop
{
	BIADD = 0,  // +
	BISUB,      // -
	BIMUL,      // *
	BIDIV,      // /
	BIEQ,       // =
	BINEQ,      // !=
	BILE,       // <=
	BILT,       // <
	BIGE,       // >=
	BIGT,       // >
	BICMP       // > || == || <
};

/*
 * when adjust scale for two big integer(int64 or int128) data,
 * determine the result whether out of bound.
 */
typedef enum BiAdjustScale { BI_AJUST_TRUE = 0, BI_LEFT_OUT_OF_BOUND, BI_RIGHT_OUT_OF_BOUND } BiAdjustScale;

/*
 * this struct is used for numeric agg function.
 * use vechashtable::CopyVarP to decrease memory application.
 */
typedef struct bictl
{
	Datum store_pos;
	MemoryContext context;
} bictl;

/*
 * Function to compute two big integer data(int64 or int128);
 * include four operations and comparison operations.
 */
typedef Datum (*biopfun)(Numeric larg, Numeric rarg, bictl* ctl);

/*
 * make numeric
 */
extern Datum makeNumeric64(int64 value, uint8 scale);
extern Datum makeNumeric128(int128 value, uint8 scale);
extern Numeric makeNumericBICopy(Numeric val);
extern Numeric makeNumericNormal(Numeric val);

/*
 * try to get bigint from numeric if possible
 */
extern Numeric DatumGetBINumeric(Datum num);

/* convert big integer data to numeric */
extern Numeric bitonumeric(Datum arg);
/* convert int(int1/int2/int4/int8) to big integer type */
extern Datum int2_numeric_bi(PG_FUNCTION_ARGS);
extern Datum int4_numeric_bi(PG_FUNCTION_ARGS);
extern Datum int8_numeric_bi(PG_FUNCTION_ARGS);
/* hash function for big integer(int64/int128/numeric) */
extern Datum hash_bi(PG_FUNCTION_ARGS);
/* replace numeric hash function and equal function for VecHashAgg/VecHashJoin Node */
extern void replace_numeric_hash_to_bi(int numCols, FmgrInfo* hashFunctions);

/* bi64 and bi128 output internal function, output with trailing zeroes if exists */
extern char *bi64_out_internal(int64 data, int scale);
extern char *bi128_out_internal(int128 data, int scale);

/* bi64 and bi128 output function, output suppres trailing zeroes in opentenbase_ora mode */
Datum bi64_out(int64 data, int scale);
Datum bi128_out(int128 data, int scale);

Datum Simplebi64div64(Numeric larg, Numeric rarg);

void Int128GetVal(bool is_int128, int128* val, Numeric* arg);

/* Numeric Compression Codes Area */
#define INT32_MIN_ASCALE (-2)
#define INT32_MAX_ASCALE (2)
#define INT64_MIN_ASCALE (-4)
#define INT64_MAX_ASCALE (4)

extern const int16 INT16_MIN_VALUE;
extern const int16 INT16_MAX_VALUE;
extern const int32 INT32_MIN_VALUE;
extern const int32 INT32_MAX_VALUE;
extern const int64 INT64_MIN_VALUE;
extern const int64 INT64_MAX_VALUE;

/*
 * convert functions between int32|int64|int128 and numerc
 *     convert numeric to int32, int64 or int128
 *     convert int32, int64, int128 to numeric
 */
extern Datum try_convert_numeric_normal_to_fast(Datum value);
extern int64 convert_short_numeric_to_int64_byscale(_in_ Numeric n, _in_ int scale);
extern void convert_short_numeric_to_int128_byscale(_in_ Numeric n, _in_ int scale, _out_ int128 *result);
extern int get_whole_scale(const NumericVar *numVar);
extern int batch_convert_short_numeric_to_int64(Datum* batchValues, char* batchNulls, int batchRows,
    int64* outInt, char* outDscales, bool* outSuccess, int* outNullCount, bool hasNull);

extern Numeric convert_int128_to_numeric(int128 data, int scale);

#endif /* SRC_INCLUDE_UTILS_BIGINTERGER_H_ */
