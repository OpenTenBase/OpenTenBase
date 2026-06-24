/*-------------------------------------------------------------------------
 *
 * biginteger.h
 *	  an optimiztion of numeric by converting to big integer and performing computation on it
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/biginteger.c
 *
 *-------------------------------------------------------------------------
 */
#include "utils/biginteger.h"
#include "utils/bitmap.h"

#include "access/hash.h"
#include "common/int.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/guc.h"

/* guc variable */
bool numeric_optimizer = true;

/* INT64_MIN string */
const char *int64_min_str = "-9223372036854775808";
/* INT128_MIN string */
const int int128_min_str_length = 41;
const char *int128_min_str = "-170141183460469231731687303715884105728";

/*
 * max length of bi64
 * symbol(+/-) + digits + decimal point + '\0' + reserved for safe
 *      1      +   19   +      1        +  1   + 3 = 25
 */
#define MAXBI64LEN 25

/*
 * max length of bi128
 * symbol(+/-) + digits + decimal point + '\0' + reserved for safe
 *      1      +   39   +      1        +  1   + 3 = 45
 */
#define MAXBI128LEN 45

/* jduge whether a and b have the same sign */
#define SAMESIGN(a, b) (((a) < 0) == ((b) < 0))



/*
 * align two int64 vals by dscale, multipler is factorial of 10.
 * set ScaleMultipler[i] = 10^i, i is between 0 and 18.
 * 10^19 is out of int64 bound, so set ScaleMultipler[19] to 0.
 */
static int64 ScaleMultipler[20] =
{
	1LL,
	10LL,
	100LL,
	1000LL,
	10000LL,
	100000LL,
	1000000LL,
	10000000LL,
	100000000LL,
	1000000000LL,
	10000000000LL,
	100000000000LL,
	1000000000000LL,
	10000000000000LL,
	100000000000000LL,
	1000000000000000LL,
	10000000000000000LL,
	100000000000000000LL,
	1000000000000000000LL,
	0LL
};

/*
 * fast compare whether the result of arg * ScaleMultipler[i]
 * is out of int64 bound.
 * the value of "Int64MultiOutOfBound[i]" equals to "INT64_MIN / ScaleMultipler[i]"
 */
static int64 Int64Multipler[19] =
{
	INT64_MIN, /* keep compiler quiet */
	-922337203685477580LL,
	-92233720368547758LL,
	-9223372036854775LL,
	-922337203685477LL,
	-92233720368547LL,
	-9223372036854LL,
	-922337203685LL,
	-92233720368LL,
	-9223372036LL,
	-922337203LL,
	-92233720LL,
	-9223372LL,
	-922337LL,
	-92233LL,
	-9223LL,
	-922LL,
	-92LL,
	-9LL
};
#define Int64MultiOutOfBound(scale) Int64Multipler[scale]

/*
 * @Description: calculates the factorial of 10, align two int128
 *               vals by dscale, multipler is factorial of 10, set
 *               values[i] = 10^i, i is between 0 and 38.
 * @IN  scale: the scale'th power
 * @Return: the factorial of 10
 */
static int128 ScaleMultiplerExt[39] =
{
	(int128)(1LL),
	(int128)(10LL),
	(int128)(100LL),
	(int128)(1000LL),
	(int128)(10000LL),
	(int128)(100000LL),
	(int128)(1000000LL),
	(int128)(10000000LL),
	(int128)(100000000LL),
	(int128)(1000000000LL),
	(int128)(10000000000LL),
	(int128)(100000000000LL),
	(int128)(1000000000000LL),
	(int128)(10000000000000LL),
	(int128)(100000000000000LL),
	(int128)(1000000000000000LL),
	(int128)(10000000000000000LL),
	(int128)(100000000000000000LL),
	(int128)(1000000000000000000LL),
	(int128)(1000000000000000000LL) * 10LL,
	(int128)(1000000000000000000LL) * 100LL,
	(int128)(1000000000000000000LL) * 1000LL,
	(int128)(1000000000000000000LL) * 10000LL,
	(int128)(1000000000000000000LL) * 100000LL,
	(int128)(1000000000000000000LL) * 1000000LL,
	(int128)(1000000000000000000LL) * 10000000LL,
	(int128)(1000000000000000000LL) * 100000000LL,
	(int128)(1000000000000000000LL) * 1000000000LL,
	(int128)(1000000000000000000LL) * 10000000000LL,
	(int128)(1000000000000000000LL) * 100000000000LL,
	(int128)(1000000000000000000LL) * 1000000000000LL,
	(int128)(1000000000000000000LL) * 10000000000000LL,
	(int128)(1000000000000000000LL) * 100000000000000LL,
	(int128)(1000000000000000000LL) * 1000000000000000LL,
	(int128)(1000000000000000000LL) * 10000000000000000LL,
	(int128)(1000000000000000000LL) * 100000000000000000LL,
	(int128)(1000000000000000000LL) * 100000000000000000LL * 10LL,
	(int128)(1000000000000000000LL) * 100000000000000000LL * 100LL,
	(int128)(1000000000000000000LL) * 100000000000000000LL * 1000LL
};

/*
 * @Description: fast compare whether the result of arg * ScaleMultiplerExt[i]
 *               is out of int128 bound. getScaleQuotient(i) equals to
 *               INT128_MIN / ScaleMultiplerExt[i]
 * @IN  scale: num between 0 and MAXINT128DIGIT
 * @Return: values[scale]
 */
#define getScaleQuotient(scale) INT128_MIN/ScaleMultiplerExt[scale]

/*
 * @Description: allocate memory for bi64, and assign value to it
 * @IN  value: the value of bi64
 * @IN  scale: the scale of bi64
 */
inline Datum makeNumeric64(int64 value, uint8 scale)
{
	Numeric result;
	result = (Numeric)palloc(NUMERIC_64SZ);
	SET_VARSIZE(result, NUMERIC_64SZ);
	result->choice.n_header = NUMERIC_64 + scale;
	*((int64*)(result->choice.n_bi.n_data)) = value;
	return (Datum)result;
}

/*
 * @Description: allocate memory for bi128, and assign value to it
 * @IN  value: the value of bi128
 * @IN  scale: the scale of bi128
 */
inline Datum makeNumeric128(int128 value, uint8 scale)
{
	Numeric result;
	result = (Numeric)palloc(NUMERIC_128SZ);
	SET_VARSIZE(result, NUMERIC_128SZ);
	result->choice.n_header = NUMERIC_128 + scale;
	memcpy(result->choice.n_bi.n_data, &value, sizeof(int128));
	return (Datum)result;
}

inline Numeric makeNumericBICopy(Numeric val)
{
	Numeric result = NULL;

	if (NUMERIC_IS_BI128(val))
	{
		int128 val128;
		uint8 scale = NUMERIC_BI_SCALE(val);

		Int128GetVal(true, &val128, &val);
		result = DatumGetNumeric(makeNumeric128(val128, scale));
	}
	else if (NUMERIC_IS_BI64(val))
	{
		int64 val64 = NUMERIC_64VALUE(val);
		uint8 scale = NUMERIC_BI_SCALE(val);

		result = DatumGetNumeric(makeNumeric64(val64, scale));
	}

	return result;
}
/*
 * @Description: This function convert big integer64 to
 *               short numeric
 *
 * @IN data:  value of bi64
 * @IN scale: scale of bi64
 * @return: Numeric - the result of numeric type
 */
static Numeric convert_int64_to_numeric(int64 data, uint8 scale)
{
	uint64 uval = 0;
	uint64 newuval = 0;
	Size len = 0;
	int tmp_loc = 0;
	NumericVar var;
	uint64 pre_data;
	uint64 post_data;
	/* int64 can require at most 19 decimal digits;
	 * add one for safety, buf1 stores pre_data,
	 * buf2 stores post_data.
	 */
	NumericDigit buf1[20 / DEC_DIGITS];
	NumericDigit buf2[20 / DEC_DIGITS];
	NumericDigit* ptr1 = buf1 + 5;
	NumericDigit* ptr2 = buf2;
	int pre_digits = 0;
	int post_digits = 0;
	Numeric result = NULL;

	Assert(scale <= MAXINT64DIGIT);

	init_var(&var);
	/* step 1: get the absolute value of int64 data */
	if (data < 0)
	{
		var.sign = NUMERIC_NEG;
		/* (-1 * data) maybe out of int64 bound, turn to uint64 */
		uval = -data;
	}
	else
	{
		var.sign = NUMERIC_POS;
		uval = data;
	}
	var.dscale = scale;
	var.ndigits = 0;
	var.weight = 0;
	/* data equals to 0, return here */
	if (uval == 0)
	{
		Numeric result;
		len = NUMERIC_HDRSZ_SHORT;
		result = (Numeric)palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(uint16)((NUMERIC_SHORT) | (((uint32)var.dscale) << NUMERIC_SHORT_DSCALE_SHIFT));
		return result;
	}

	/* step 2: split source int64 data into pre_data and post_data by decimal point
	 * pre_data stores the data before decimal point.
	 */
	pre_data = uval / ScaleMultiplerExt[scale];
	/* pre_data stores the data after decimal point. */
	post_data = uval % ScaleMultiplerExt[scale];

	/* step 3: calculate pre_data and store result in buf1
	 * pre_data == 0, skip this
	 */
	if (pre_data != 0)
	{
		do
		{
			ptr1--;
			pre_digits++;
			newuval = pre_data / NBASE;
			*ptr1 = pre_data - newuval * NBASE;
			pre_data = newuval;
		}
		while (pre_data);
		var.weight = pre_digits - 1;
	}
	/* step 4: calculate pre_data and store result in buf2
	 * post_data == 0, skip this
	 */
	if (post_data != 0)
	{
		int result_scale = (int)scale;
		while (post_data && result_scale >= DEC_DIGITS)
		{
			post_digits++;
			result_scale = result_scale - DEC_DIGITS;
			*ptr2 = post_data / ScaleMultipler[result_scale];
			post_data = post_data % ScaleMultipler[result_scale];
			ptr2++;
		}
		if (post_data)
		{
			Assert(result_scale < DEC_DIGITS);
			post_digits++;
			*ptr2 = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
			ptr2++;
		}
	}

	/* step5: make numeric result */
	if (pre_digits)
	{
		/* pre_digits != 0 && post_digits != 0
		 * Example: 900000000.0001
		 */
		if (post_digits)
		{
			var.ndigits = pre_digits + post_digits;
			len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
			result = (Numeric)palloc(len);
			SET_VARSIZE(result, len);

			memcpy(result->choice.n_short.n_data,
				   ptr1,
				   pre_digits * sizeof(NumericDigit));
			memcpy(result->choice.n_short.n_data + pre_digits,
				   buf2,
				   post_digits * sizeof(NumericDigit));
		}
		else
		{
			/* pre_digits != 0 && post_digits == 0
			 * Example: 9000000.000
			 */
			for (tmp_loc = 0; tmp_loc < pre_digits;)
			{
				if (buf1[4 - tmp_loc] == 0)
				{
					tmp_loc++;
				}
				else
				{
					break;
				}
			}
			pre_digits = pre_digits - tmp_loc;
			var.ndigits = pre_digits;
			len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
			result = (Numeric)palloc(len);
			SET_VARSIZE(result, len);
			memcpy(result->choice.n_short.n_data,
				   ptr1,
				   pre_digits * sizeof(NumericDigit));
		}
	}
	else
	{
		/* pre_digits == 0 && post_digits != 0
		 * Example: 0.0000001
		 */
		Assert(post_digits <= 5);
		var.weight = 0;
		for (tmp_loc = 0; tmp_loc < post_digits; tmp_loc++)
		{
			var.weight--;
			if (buf2[tmp_loc] != 0)
				break;
		}
		post_digits = post_digits - tmp_loc;
		var.ndigits = post_digits;
		len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
		result = (Numeric)palloc(len);
		SET_VARSIZE(result, len);
		memcpy(result->choice.n_short.n_data,
			   buf2 + tmp_loc,
			   post_digits * sizeof(NumericDigit));
	}

	result->choice.n_short.n_header =
		(var.sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
		((uint32)(var.dscale) << NUMERIC_SHORT_DSCALE_SHIFT) | (var.weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
		((uint32)(var.weight) & NUMERIC_SHORT_WEIGHT_MASK);

	/* Check for overflow of int64 fields */
	Assert((unsigned int)(pre_digits + post_digits) == NUMERIC_NDIGITS(result));
	Assert(var.weight == NUMERIC_WEIGHT(result));
	Assert(var.dscale == NUMERIC_DSCALE(result));

	return result;
}

/*
 * @Description: This function convert big integer128 to
 *               short numeric
 *
 * @IN data:  value of bi128
 * @IN scale: scale of bi128
 * @return: Numeric - the result of numeric type
 */
Numeric convert_int128_to_numeric(int128 data, int scale)
{
	uint128 uval = 0;
	uint128 newuval = 0;
	NumericVar var;
	Size len = 0;
	int tmp_loc = 0;
	uint128 pre_data;
	uint128 post_data;
	/* int128 can require at most 38 decimal digits;
	 * add two for safety, buf1 stores pre_data,
	 * buf2 stores post_data
	 */
	NumericDigit buf1[40 / DEC_DIGITS];
	NumericDigit buf2[40 / DEC_DIGITS];
	NumericDigit* ptr1 = buf1 + 10;
	NumericDigit* ptr2 = buf2;
	int pre_digits = 0;
	int post_digits = 0;
	Numeric result = NULL;

	Assert(scale <= MAXINT128DIGIT);

	init_var(&var);
	/* step 1: get the absolute value of int128 data */
	if (data < 0)
	{
		var.sign = NUMERIC_NEG;
		/* (-1 * data) maybe out of int128 bound, turn to uint128 */
		uval = -data;
	}
	else
	{
		var.sign = NUMERIC_POS;
		uval = data;
	}
	var.dscale = scale;
	var.ndigits = 0;
	var.weight = 0;
	/* data equals to 0, return here */
	if (uval == 0)
	{
		Numeric result;
		len = NUMERIC_HDRSZ_SHORT;
		result = (Numeric)palloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(uint16)((NUMERIC_SHORT) | (((uint32)(var.dscale)) << NUMERIC_SHORT_DSCALE_SHIFT));
		return result;
	}

	/* step 2: split source int128 data into pre_data and post_data by decimal point
	 * pre_data stores the data before decimal point.
	 */
	pre_data = uval / ScaleMultiplerExt[scale];
	/* pre_data stores the data after decimal point. */
	post_data = uval % ScaleMultiplerExt[scale];

	/* step 3: calculate pre_data and store result in buf1
	 * pre_data == 0, skip this
	 */
	if (pre_data != 0)
	{
		do
		{
			ptr1--;
			pre_digits++;
			newuval = pre_data / NBASE;
			*ptr1 = pre_data - newuval * NBASE;
			pre_data = newuval;
		}
		while (pre_data);
		var.weight = pre_digits - 1;
	}
	/* step 4: calculate pre_data and store result in buf2
	 * post_data == 0, skip this
	 */
	if (post_data != 0)
	{
		int result_scale = (int)scale;
		while (post_data && result_scale >= DEC_DIGITS)
		{
			post_digits++;
			*ptr2 = post_data / ScaleMultiplerExt[result_scale - DEC_DIGITS];
			post_data = post_data % ScaleMultiplerExt[result_scale - DEC_DIGITS];
			result_scale = result_scale - DEC_DIGITS;
			ptr2++;
		}
		if (post_data)
		{
			Assert(result_scale < DEC_DIGITS);
			post_digits++;
			*ptr2 = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
			ptr2++;
		}
	}

	/* step5: make numeric result */
	if (pre_digits)
	{
		/* pre_digits != 0 && post_digits != 0
		 * Example: 9000000.00001
		 */
		if (post_digits)
		{
			var.ndigits = pre_digits + post_digits;
			len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
			result = (Numeric)palloc(len);
			SET_VARSIZE(result, len);

			memcpy(result->choice.n_short.n_data,
				   ptr1,
				   pre_digits * sizeof(NumericDigit));
			memcpy(result->choice.n_short.n_data + pre_digits,
				   buf2,
				   post_digits * sizeof(NumericDigit));
		}
		else
		{
			/* pre_digits != 0 && post_digits == 0
			 * Example: 9000000.000
			 */
			for (tmp_loc = 0; tmp_loc < pre_digits;)
			{
				if (buf1[9 - tmp_loc] == 0)
				{
					tmp_loc++;
				}
				else
				{
					break;
				}
			}
			pre_digits = pre_digits - tmp_loc;
			var.ndigits = pre_digits;
			len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
			result = (Numeric)palloc(len);
			SET_VARSIZE(result, len);
			memcpy(result->choice.n_short.n_data,
				   ptr1,
				   pre_digits * sizeof(NumericDigit));
		}
	}
	else
	{
		/* pre_digits == 0 && post_digits != 0
		 * Example: 0.000001
		 */
		Assert(post_digits <= 10);
		var.weight = 0;
		for (tmp_loc = 0; tmp_loc < post_digits; tmp_loc++)
		{
			var.weight--;
			if (buf2[tmp_loc] != 0)
				break;
		}
		post_digits = post_digits - tmp_loc;
		var.ndigits = post_digits;
		len = NUMERIC_HDRSZ_SHORT + var.ndigits * sizeof(NumericDigit);
		result = (Numeric)palloc(len);
		SET_VARSIZE(result, len);
		memcpy(result->choice.n_short.n_data,
			   buf2 + tmp_loc,
			   post_digits * sizeof(NumericDigit));
	}

	result->choice.n_short.n_header =
		(var.sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
		(var.dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (var.weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
		(var.weight & NUMERIC_SHORT_WEIGHT_MASK);

	/* Check for overflow of int64 fields */
	Assert((unsigned int)(pre_digits + post_digits) == NUMERIC_NDIGITS(result));
	Assert(var.weight == NUMERIC_WEIGHT(result));
	Assert(var.dscale == NUMERIC_DSCALE(result));

	return result;
}

/*
 * @Description: convert bi64 or bi128 to numeric type
 * @IN  val: the bi64 or bi128 data
 * @Return: the result of numeric type
 */
inline Numeric makeNumericNormal(Numeric val)
{
	Assert(NUMERIC_IS_BI(val));

	if (NUMERIC_IS_BI64(val))
	{
		return convert_int64_to_numeric(NUMERIC_64VALUE(val), NUMERIC_BI_SCALE(val));
	}
	else if (NUMERIC_IS_BI128(val))
	{
		int128 tmp_data = 0;
		memcpy(&tmp_data, val->choice.n_bi.n_data, sizeof(int128));
		return convert_int128_to_numeric(tmp_data, NUMERIC_BI_SCALE(val));
	}
	else
	{
		elog(ERROR, "unrecognized big integer numeric format");
	}
	return NULL;
}

/*
 * @Description: Detoast column numeric data. Column numeric is
 *               a short-header type.
 * @IN num: input numeric data
 * @return: Numeric - detoast numeric data
 */
static inline Numeric DatumGetBINumericShort(Datum num)
{
	/*
	 * unlikely this is a short-header varlena --- convert to 4-byte header format
	 */
	struct varlena* new_attr = NULL;
	struct varlena* old_attr = (struct varlena*)num;
	Size data_size = VARSIZE_SHORT(old_attr) - VARHDRSZ_SHORT;
	Size new_size = data_size + VARHDRSZ;

	new_attr = (struct varlena*)palloc(new_size);
	SET_VARSIZE(new_attr, new_size);
	memcpy(VARDATA(new_attr), VARDATA_SHORT(old_attr), data_size);
	old_attr = new_attr;
	return (Numeric)old_attr;
}

/*
 * @Description: Detoast column numeric data. Column numeric is unlikely
 *               a short-header type, simplify macro DatumGetNumeric
 *
 * @IN num: input numeric data
 * @return: Numeric - detoast numeric data
 */
inline Numeric DatumGetBINumeric(Datum num)
{
	if (likely(!VARATT_IS_SHORT(num)))
	{
		return (Numeric)num;
	}
	else
	{
		return DatumGetBINumericShort(num);
	}
}

/*
 * @Description: adjust two int64 number to same scale
 * @IN  x: the value of num1
 * @IN  x_scale: the scale of num1
 * @IN  y: the value of num2
 * @IN  y_scale: the scale of num2
 * @OUT result_scale: the adjusted scale of num1 and num2, the
 *                    maximum value of x_scale and y_scale
 * @OUT x_scaled: the value of num1 which scale is result_scale
 * @OUT y_scaled: the value of num2 which scale is result_scale
 * @Return: adjust succeed or not
 *
 * Example:
 * +-------------------+----------------------+-------------------------------+
 * | defination        | num1: numeric(10, 2) |  num2: numeric(12, 4)         |
 * +-------------------+----------------------+-------------------------------+
 * | user data         |      num1 = 1.0      |        num2 = 2.0             |
 * +-------------------+----------------------+-------------------------------+
 * | BI implementation | x = 100, x_scale = 2 | y = 20000, y_scale = 4        |
 * +-------------------+----------------------+-------------------------------+
 * | Adjusted result   | result_scale = 4, x_scaled = 10000, y_scaled = 20000 |
 * +-------------------+----------------------+-------------------------------+
 */
static inline BiAdjustScale adjustBi64ToSameScale(
	int64 x, int x_scale, int64 y, int y_scale, int* result_scale, int64* x_scaled, int64* y_scaled)
{
	int delta_scale = x_scale - y_scale;
	int64 tmpval = 0;

	Assert(x_scale >= 0 && x_scale <= MAXINT64DIGIT);
	Assert(y_scale >= 0 && y_scale <= MAXINT64DIGIT);

	if (delta_scale >= 0)
	{
		/* use negative number is to avoid INT64_MIN * -1
		 * out of int64 bound.
		 */
		tmpval = y < 0 ? y : -y;
		*result_scale = x_scale;
		*x_scaled = x;
		/* the result of tmpval * ScaleMultipler[delta_scale]
		 * doesn't out of int64 bound.
		 */
		if (likely(tmpval >= Int64MultiOutOfBound(delta_scale)))
		{
			*y_scaled = y * ScaleMultipler[delta_scale];
			return BI_AJUST_TRUE;
		}
		else     /* y_scaled must be out of int64 bound */
		{
			return BI_RIGHT_OUT_OF_BOUND;
		}
	}
	else
	{
		/* use negative number is to avoid INT64_MIN * -1
		 * out of int64 bound.
		 */
		tmpval = x < 0 ? x : -x;
		*result_scale = y_scale;
		*y_scaled = y;
		/* the result of tmpval * ScaleMultipler[delta_scale]
		 * doesn't out of int64 bound.
		 */
		if (likely(tmpval >= Int64MultiOutOfBound(-delta_scale)))
		{
			*x_scaled = x * ScaleMultipler[-delta_scale];
			return BI_AJUST_TRUE;
		}
		else
		{
			return BI_LEFT_OUT_OF_BOUND;
		}
	}
	/* not possible, just keep compiler quiet */
	return BI_AJUST_TRUE;
}

/*
 * @Description: adjust two int128 number to same scale
 * @IN  x: the value of num1
 * @IN  x_scale: the scale of num1
 * @IN  y: the value of num2
 * @IN  y_scale: the scale of num2
 * @OUT result_scale: the adjusted scale of num1 and num2, the
 *                    maximum value of x_scale and y_scale
 * @OUT x_scaled: the value of num1 which scale is result_scale
 * @OUT y_scaled: the value of num2 which scale is result_scale
 * @Return: adjust succeed or not
 */
static inline BiAdjustScale adjustBi128ToSameScale(
	int128 x, int x_scale, int128 y, int y_scale, int* result_scale, int128* x_scaled, int128* y_scaled)
{
	int delta_scale = x_scale - y_scale;
	int128 tmpval = 0;

	Assert(x_scale >= 0 && x_scale <= MAXINT128DIGIT);
	Assert(y_scale >= 0 && y_scale <= MAXINT128DIGIT);

	if (delta_scale >= 0)
	{
		/* use negative number is to avoid INT128_MIN * -1
		 * out of int128 bound.
		 */
		tmpval = y < 0 ? y : -y;
		*result_scale = x_scale;
		*x_scaled = x;
		/* the result of tmpval * ScaleMultipler[delta_scale]
		 * doesn't out of int128 bound.
		 */
		if (likely(tmpval >= getScaleQuotient(delta_scale)))
		{
			*y_scaled = y * ScaleMultiplerExt[delta_scale];
			return BI_AJUST_TRUE;
		}
		else     /* y_scaled must be out of int64 bound */
		{
			return BI_RIGHT_OUT_OF_BOUND;
		}
	}
	else
	{
		/* use negative number is to avoid INT128_MIN * -1
		 * out of int128 bound.
		 */
		tmpval = x < 0 ? x : -x;
		*result_scale = y_scale;
		*y_scaled = y;
		/* the result of tmpval * ScaleMultipler[delta_scale]
		 * doesn't out of int128 bound.
		 */
		if (likely(tmpval >= getScaleQuotient(-delta_scale)))
		{
			*x_scaled = x * ScaleMultiplerExt[-delta_scale];
			return BI_AJUST_TRUE;
		}
		else
		{
			return BI_LEFT_OUT_OF_BOUND;
		}
	}
}

/*
 * @Description: get the weight and firstDigit of bi64 or bi128, it is used to
 *               calculate bi div bi like numeric_div does.
 *
 * @IN val: the value of bi64/bi128 num.
 * @IN scale: the scale of bi64/bi128 num.
 * @OUT weight: the weight of num in Numeric mode.
 * @OUT firstDigit: the first digit of num in Numeric mode.
 * @return: NULL
 */
static inline void getBi64WeightDigit(uint64 val, int scale, int* weight, int* firstDigit)
{
	/* calculate weight of val */
	int digits = 0;
	int result_scale = scale;
	/* temp value of calculation */
	uint64 tmp_data = 0;
	uint64 pre_data;
	uint64 post_data;

	Assert(scale >= 0 && scale <= MAXINT128DIGIT);

	/* step 1: split source int64 data into pre_data and post_data by decimal point */
	/* pre_data stores the data before decimal point. */
	pre_data = val / ScaleMultiplerExt[result_scale];
	/* pre_data stores the data after decimal point. */
	post_data = val % ScaleMultiplerExt[result_scale];

	/* step 2: calculate pre_data */
	/* pre_data == 0, skip this */
	if (pre_data != 0)
	{
		do
		{
			digits++;
			*firstDigit = pre_data % NBASE;
			pre_data = pre_data / NBASE;
		}
		while (pre_data);
		*weight = digits - 1;
		return;
	}

	/* step 3: calculate post_data */
	/* post_data == 0, skip this */
	if (post_data != 0)
	{
		while (post_data && result_scale >= DEC_DIGITS)
		{
			digits++;
			result_scale = result_scale - DEC_DIGITS;
			tmp_data = post_data / ScaleMultiplerExt[result_scale];
			if (tmp_data)
			{
				*firstDigit = tmp_data;
				*weight = -digits;
				return;
			}
			post_data = post_data % ScaleMultiplerExt[result_scale];
		}
		Assert(post_data != 0 && result_scale < DEC_DIGITS);
		*weight = -(digits + 1);
		*firstDigit = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
		return;
	}
	*weight = 0;
	*firstDigit = 0;
	return;
}

static inline void getBi128WeightDigit(uint128 val, int scale, int* weight, int* firstDigit)
{
	/* calculate weight of val */
	int digits = 0;
	int result_scale = scale;
	/* temp value of calculation */
	uint128 tmp_data = 0;
	uint128 pre_data;
	uint128 post_data;

	Assert(scale >= 0 && scale <= MAXINT128DIGIT);

	/* step 1: split source int64 data into pre_data and post_data by decimal point */
	/* pre_data stores the data before decimal point. */
	pre_data = val / ScaleMultiplerExt[result_scale];
	/* pre_data stores the data after decimal point. */
	post_data = val % ScaleMultiplerExt[result_scale];

	/* step 2: calculate pre_data */
	/* pre_data == 0, skip this */
	if (pre_data != 0)
	{
		do
		{
			digits++;
			*firstDigit = pre_data % NBASE;
			pre_data = pre_data / NBASE;
		}
		while (pre_data);
		*weight = digits - 1;
		return;
	}

	/* step 3: calculate post_data */
	/* post_data == 0, skip this */
	if (post_data != 0)
	{
		while (post_data && result_scale >= DEC_DIGITS)
		{
			digits++;
			result_scale = result_scale - DEC_DIGITS;
			tmp_data = post_data / ScaleMultiplerExt[result_scale];
			if (tmp_data)
			{
				*firstDigit = tmp_data;
				*weight = -digits;
				return;
			}
			post_data = post_data % ScaleMultiplerExt[result_scale];
		}
		Assert(post_data != 0 && result_scale < DEC_DIGITS);
		*weight = -(digits + 1);
		*firstDigit = post_data * ScaleMultipler[DEC_DIGITS - result_scale];
		return;
	}
	*weight = 0;
	*firstDigit = 0;
	return;
}

/*
 * @Description: Calculate the result of bi64 divide bi64. The
 *               result is same with numeric_div.
 *
 * @IN larg: left-hand operand of division(/).
 * @IN rarg: right-hand operand of division(/).
 * @IN ctl: ctl is always NULL.
 * @return: Datum - the datum data points to bi128 or numeric.
 */
static Datum bi64div64(Numeric larg, Numeric rarg, bictl* ctl)
{
	uint8 lvalscale = NUMERIC_BI_SCALE(larg);
	uint8 rvalscale = NUMERIC_BI_SCALE(rarg);
	int64 leftval = NUMERIC_64VALUE(larg);
	int64 rightval = NUMERIC_64VALUE(rarg);
	int weight1 = 0;
	int weight2 = 0;
	int firstdigit1 = 0;
	int firstdigit2 = 0;
	int qweight = 0;
	int rscale = 0;
	int adjustScale = 0;
	int128 divnum = 0;
	int128 result = 0;
	bool round = false;
	FunctionCallInfoData finfo;

	Assert(NUMERIC_IS_BI(larg) && NUMERIC_IS_BI(rarg));
	Assert(NUMERIC_BI_SCALE(larg) >= 0 && NUMERIC_BI_SCALE(larg) <= MAXINT64DIGIT);
	Assert(NUMERIC_BI_SCALE(rarg) >= 0 && NUMERIC_BI_SCALE(rarg) <= MAXINT64DIGIT);

	if (unlikely(rightval == 0))
	{
		ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
	}

	/* Use the absolute value of leftval and rightval. */
	getBi64WeightDigit((leftval >= 0 ? leftval : -leftval), lvalscale, &weight1, &firstdigit1);
	getBi64WeightDigit((rightval >= 0 ? rightval : -rightval), rvalscale, &weight2, &firstdigit2);

	/*
	 * Estimate weight of quotient.  If the two first digits are equal, we
	 * can't be sure, but assume that var1 is less than var2.
	 */
	qweight = weight1 - weight2;
	if (firstdigit1 <= firstdigit2)
	{
		qweight--;
	}

	/* Select result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, lvalscale);
	rscale = Max(rscale, rvalscale);
	/* the reason of rscale++ is for calculate bound of last digit */
	rscale++;
	adjustScale = rscale + rvalscale - lvalscale;

	if (likely(rscale <= MAXINT128DIGIT && adjustScale <= MAXINT128DIGIT &&
			   !pg_mul_s128_overflow(leftval, ScaleMultiplerExt[adjustScale], &divnum)))
	{
		result = divnum / rightval;
		if (result > 0)
		{
			round = ((result % 10) >= 5);
			result = round ? (result / 10 + 1) : result / 10;
		}
		else if (result < 0)
		{
			round = ((result % 10) <= -5);
			result = round ? (result / 10 - 1) : result / 10;
		}
		/* round over, rscale-- */
		rscale--;
		/* return the BI64 type result */
		if (rscale <= MAXINT64DIGIT && INT128_INT64_EQ(result))
		{
			return makeNumeric64((int64)result, rscale);
		}
		/* return the BI128 type result */
		return makeNumeric128(result, rscale);
	}

	/* result is out of int128 bound, call numeric_div calculate */
	finfo.arg[0] = (Datum)makeNumericNormal(larg);
	finfo.arg[1] = (Datum)makeNumericNormal(rarg);
	return numeric_div(&finfo);
}

/*
 * @Description: Calculate the result of bi64 divide bi64. Since
 * 				 ctl is always NULL, reduce this parameter and
 *				 make it simple for the useness of codegeneration
 *
 * @IN larg: left-hand operand of division(/).
 * @IN rarg: right-hand operand of division(/).
 * @return: Datum - the datum data points to bi128 or numeric.
 */
Datum Simplebi64div64(Numeric larg, Numeric rarg)
{
	return bi64div64(larg, rarg, NULL);
}

/*
 * @Description: convert data(bi64, bi128 or numeric) to numeric type
 *
 * @IN  arg: numeric data
 * @Return:  the numeric result
 */
Numeric bitonumeric(Datum arg)
{
	Numeric val = DatumGetNumeric(arg);

	if (NUMERIC_IS_BI(val))
	{
		return makeNumericNormal(val);
	}
	return val;
}

/*
 * @Description: convert int2 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int2_numeric_bi(PG_FUNCTION_ARGS)
{
	/* convert int2 to int8 */
	int64 val = PG_GETARG_INT16(0);
	/* make bi64 data */
	return makeNumeric64(val, 0);
}

/*
 * @Description: convert int4 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int4_numeric_bi(PG_FUNCTION_ARGS)
{
	/* convert int4 to int8 */
	int64 val = PG_GETARG_INT32(0);
	/* make bi64 data */
	return makeNumeric64(val, 0);
}

/*
 * @Description: convert int8 to big integer type
 *
 * @IN  arg: int type data
 * @Return:  the Datum point to bi64 type
 */
Datum int8_numeric_bi(PG_FUNCTION_ARGS)
{
	/* convert int8 to int8 */
	int64 val = PG_GETARG_INT64(0);
	/* make bi64 data */
	return makeNumeric64(val, 0);
}

/*
 * @Description: calculate the hash value of specified
 *               int64 and scale.
 *
 * @IN num: int64 value of bi64(big integer).
 * @IN scale: scale of bi64(big integer).
 * @Return:  the hash value
 */
static inline Datum int64_hash_bi(int64 num, int scale)
{
	uint32 lohalf = 0;
	uint32 hihalf = 0;

	/* handle num = 0 case */
	if (num == 0)
	{
		return hash_uint32(0) ^ 0;
	}

	/* Normalize num: omit any trailing zeros from the input to the hash */
	while ((num % 10) == 0 && scale > 0)
	{
		num /= 10;
		scale--;
	}

	lohalf = (uint32)num;
	hihalf = (uint32)(num >> 32);

	lohalf ^= (num >= 0) ? hihalf : ~hihalf;
	return hash_uint32(lohalf) ^ scale;
}

/*
 * @Description: calculate the hash value of specified
 *               int128 and scale.
 *
 * @IN num: int128 value of bi128(big integer).
 * @IN scale: scale of bi128(big integer).
 * @Return:  the hash value
 */
static inline Datum int128_hash_bi(int128 num, int scale)
{
	uint64 lohalf64 = 0;
	uint64 hihalf64 = 0;
	uint32 lohalf32 = 0;
	uint32 hihalf32 = 0;

	/* handle num = 0 case */
	if (num == 0)
	{
		return hash_uint32(0) ^ 0;
	}

	/* Normalize num: omit any trailing zeros from the input to the hash */
	while ((num % 10) == 0 && scale > 0)
	{
		num /= 10;
		scale--;
	}
	/* call bi64_hash */
	if (INT128_INT64_EQ(num))
	{
		return int64_hash_bi(num, scale);
	}

	lohalf64 = (uint64)num;
	hihalf64 = (uint64)(num >> 64);

	lohalf64 ^= (num >= 0) ? hihalf64 : ~hihalf64;

	lohalf32 = (uint32)lohalf64;
	hihalf32 = (uint32)(lohalf64 >> 32);

	lohalf32 ^= (num >= 0) ? hihalf32 : ~hihalf32;
	return hash_uint32(lohalf32) ^ scale;
}

/*
 * @Description: inline function, call hash_numeric(PG_FUNCTION_ARGS)
 *
 * @IN num: numeric type data
 * @Return:  the hash value
 */
static inline Datum call_hash_numeric(Numeric num)
{
	/* call hash_numeric here */
	FunctionCallInfoData finfo;
	finfo.arg[0] = NumericGetDatum(num);
	return hash_numeric(&finfo);
}

/*
 * @Description: calculate the hash value of numeric data,
 *               make sure the same value with diferent types
 *               have the same hash value.
 *               e.g. int64   1.00->(100, 2)
 *                    int128  1.0000->(10000, 4)
 *                    numeric 1.0000000000000000
 *               they must have the same hash value
 *
 * @IN num: int128 value of bi128(big integer).
 * @Return:  the hash value
 */
static Datum numeric_hash_bi(Numeric num)
{
	int ndigits = 0;
	NumericDigit* digits;
	int weight = 0;
	int dscale = 0;
	int num_sign = 0;
	int last_digit = 0;
	int last_base = 0;
	int i = 0;
	int128 data = 0;
	int128 mul_tmp_data = 0;
	int128 add_tmp_data = 0;

	/* If it's NaN, Inf or -Inf don't try to hash the rest of the fields */
	if (NUMERIC_IS_NAN(num) || NUMERIC_IS_INF(num))
	{
		PG_RETURN_UINT32(0);
	}

	ndigits = NUMERIC_NDIGITS(num);

	/*
	 * If there are no non-zero digits, then the value of the number is zero,
	 * regardless of any other fields.
	 */
	if (ndigits == 0)
	{
		return hash_uint32(0) ^ 0;
	}

	digits = NUMERIC_DIGITS(num);

	Assert(ndigits > 0);
	Assert(digits[0] != 0);
	Assert(digits[ndigits - 1] != 0);

	weight = NUMERIC_WEIGHT(num);
	dscale = NUMERIC_DSCALE(num);
	num_sign = (NUMERIC_SIGN(num) == NUMERIC_POS) ? 1 : -1;

	/* numeric data can't convert to int128, call hash_numeric */
	if (weight >= 10)
	{
		return call_hash_numeric(num);
	}

	/* step1. numeric can convert to int128 */
	if ((weight >= 0 && CAN_CONVERT_BI128(weight * DEC_DIGITS + DEC_DIGITS + dscale)) ||
			(weight < 0 && CAN_CONVERT_BI128(dscale)))
	{
		convert_short_numeric_to_int128_byscale(num, dscale, &data);
		return int128_hash_bi(data, dscale);
	}

	/* step2. numeric data have only pre data
	 * e.g. numeric data->100.0000
	 */
	if (weight >= 0 && (weight + 1) >= ndigits)
	{
		for (i = 0; i < ndigits; i++)
		{
			if (!pg_mul_s128_overflow(
						num_sign * digits[i], ScaleMultiplerExt[(weight - i) * DEC_DIGITS], &mul_tmp_data))
			{
				if (!pg_add_s128_overflow(data, mul_tmp_data, &add_tmp_data))
				{
					data = add_tmp_data;
				}
				else
				{
					return call_hash_numeric(num);
				}
			}
			else
			{
				return call_hash_numeric(num);
			}
		}
		return int128_hash_bi(data, 0);
	}

	/* step3. delete the trailing zeros of last digit
	 * e.g. numeric(8,4) value is 2.1000, we should delete
	 * the last 3 zeros, the last value is (value:21, scale:1)
	 */
	/* update dscale(e.g. numeric(100,50) data=1.1000 dscale = 4) */
	dscale = (ndigits - weight - 1) * DEC_DIGITS;

	i = ndigits - 1;
	if (digits[i] % 1000 == 0)
	{
		last_digit = digits[i] / 1000;
		last_base = NBASE / 1000;
		dscale = dscale - 3;
	}
	else if (digits[i] % 100 == 0)
	{
		last_digit = digits[i] / 100;
		last_base = NBASE / 100;
		dscale = dscale - 2;
	}
	else if (digits[i] % 10 == 0)
	{
		last_digit = digits[i] / 10;
		last_base = NBASE / 10;
		dscale = dscale - 1;
	}
	else
	{
		last_digit = digits[i];
		last_base = NBASE;
	}

	/* step4. calculate the hash of type a.b(like: 123.1000 or 0.12301200).
	 * if data is out of int128 bound, call hash_numeric directly.
	 */
	for (i = 0; i < (ndigits - 1); i++)
	{
		if (!pg_mul_s128_overflow(data, NBASE, &mul_tmp_data))
		{
			if (!pg_add_s128_overflow(digits[i] * num_sign, mul_tmp_data, &add_tmp_data))
			{
				data = add_tmp_data;
			}
			else
			{
				return call_hash_numeric(num);
			}
		}
		else
		{
			return call_hash_numeric(num);
		}
	}
	/* calculate the last digit carefully to avoid out of int128 bound */
	if (!pg_mul_s128_overflow(data, last_base, &mul_tmp_data))
	{
		if (!pg_add_s128_overflow(last_digit * num_sign, mul_tmp_data, &add_tmp_data))
		{
			data = add_tmp_data;
		}
		else
		{
			return call_hash_numeric(num);
		}
	}
	else
	{
		return call_hash_numeric(num);
	}

	return int128_hash_bi(data, dscale);
}

/*
 * @Description: calculate the hash value of big integer
 *               (int64/int128/numeric type).
 *
 * @IN num: big integer.
 * @Return:  the hash value
 */
Datum hash_bi(PG_FUNCTION_ARGS)
{
	Numeric key = PG_GETARG_NUMERIC(0);
	uint16 keyFlag = NUMERIC_NB_FLAGBITS(key);
	uint8 keyScale = NUMERIC_BI_SCALE(key);

	/* Numeric is int64 */
	if (NUMERIC_FLAG_IS_BI64(keyFlag))
	{
		/*
		 * If the value of the number is zero, return -1 directly,
		 * the same with hash_numeric.
		 */
		int64 keyValue = NUMERIC_64VALUE(key);
		return int64_hash_bi(keyValue, keyScale);
	}
	else if (NUMERIC_FLAG_IS_BI128(keyFlag))
	{
		/*
		 * Numeric is int128
		 * If the value of the number is zero, return -1 directly,
		 * the same with hash_numeric.
		 */
		int128 keyValue = 0;
		memcpy(&keyValue, (key)->choice.n_bi.n_data, sizeof(int128));
		return int128_hash_bi(keyValue, keyScale);
	}
	/* Normal numeric */
	return numeric_hash_bi(key);
}

/*
 * @Description: replace numeric hash function to bihash function for
 *               vec hash agg and hash join.
 *
 * @IN numCols: num columns
 * @OUT hashFunctions: replace the corresponding hash function
 */
void replace_numeric_hash_to_bi(int numCols, FmgrInfo* hashFunctions)
{
	int i = 0;

	for (i = 0; i < numCols; i++)
	{
		/* replace hash_numeric to hash_bi */
		if (hashFunctions[i].fn_addr == hash_numeric)
		{
			hashFunctions[i].fn_addr = hash_bi;
		}
	}
}

/*
 * @Description: print bi64 data to string with trailing zeroes if exists
 *               Internal function
 *
 * @IN  data:   int64 data
 * @IN  scale:  the scale of bi64 data
 * @Return: Output string for numeric data type with trailing zeroes if exists
 */
char *bi64_out_internal(int64 data, int scale)
{
	char buf[MAXBI64LEN * 5];
	char* result = NULL;
	uint64 val_u64 = 0;
	int64 pre_val = 0;
	int64 post_val = 0;
	Assert(scale >= 0 && scale <= MAXINT64DIGIT);

	/* data equals to "INT64_MIN" */
	if (unlikely(data == (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1)))
	{
		/*
		 * Avoid problems with the most negative integer not being representable
		 * as a positive integer.
		 */
		if (scale > 0)
		{
			int len = strlen(int64_min_str) - scale;
			memcpy(buf, int64_min_str, len);
			buf[len] = '.';
			memcpy(buf + len + 1, int64_min_str + len, scale + 1);
		}
		else
		{
			memcpy(buf, int64_min_str, strlen(int64_min_str) + 1);
		}
		result = pstrdup(buf);
		return result;
	}

	/* data is positive */
	if (data >= 0)
	{
		val_u64 = data;
		pre_val = val_u64 / (uint64)ScaleMultiplerExt[scale];
		post_val = val_u64 % (uint64)ScaleMultiplerExt[scale];
		if (likely(scale > 0 && scale <= MAXINT64DIGIT))
		{
			sprintf(buf, "%ld.%0*ld", pre_val, scale, post_val);
		}
		else if (scale <= 0)
		{
			sprintf(buf, "%ld", pre_val);
		} else
        {
            // never happen, make gcc happy
            elog(ERROR, "illegal scale");
        }
	}
	else
	{
		/* data is negative */
		val_u64 = -data;
		pre_val = val_u64 / (uint64)ScaleMultiplerExt[scale];
		post_val = val_u64 % (uint64)ScaleMultiplerExt[scale];
		if (likely(scale > 0 && scale <= MAXINT64DIGIT))
		{
			sprintf(buf, "-%ld.%0*ld", pre_val, scale, post_val);
		}
		else if (scale <= 0)
		{
			sprintf(buf, "-%ld", pre_val);
		}
        else
        {
            // never happen, make gcc happy
            elog(ERROR, "illegal scale");
        }
	}

	result = pstrdup(buf);
	return result;
}

/*
 * @Description: print bi64 data to string like numeric_out.
 *               Output function.
 *
 * @IN  data:   int64 data
 * @IN  scale:  the scale of bi64 data
 * @Return: Output string for numeric data type
 *          Output suppres trailing zeroes in opentenbase_ora mode
 */
Datum bi64_out(int64 data, int scale)
{
	char* result = bi64_out_internal(data, scale);

	if (ORA_MODE)
	{
		suppres_trailing_zeroes(result);
	}

	PG_RETURN_CSTRING(result);
}

/*
 * @Description: print int128 data to string, because of current GCC doesn't provide
 *               solution to print int128 data, we provide this function.
 *
 * @IN  preceding_zero: mark whether print preceding zero or not
 * @IN  data: int128 data
 * @OUT str:  the output string buffer
 * @IN  len:  the length of string buffer
 * @IN  scale:when preceding_zero is true, scale is the standard output width size
 * @Return: print succeed or not
 */
#define INT128_TO_String_Func(int128_to_string_func, preceding_zero)                       \
static void int128_to_string_func(int128 data, char* str, int len, int scale)              \
{                                                                                          \
    int128 num = 0;                                                                        \
    int64 leading = 0;                                                                     \
    int64 trailing = 0;                                                                    \
    int64 middle = 0;                                                                      \
    Assert(data >= 0);                                                                     \
    Assert(scale >= 0 && scale <= MAXINT128DIGIT);                                         \
    /* turn to int64 */                                                                    \
    if (INT128_INT64_EQ(data)) {                                                           \
        if (preceding_zero) {                                                              \
            sprintf(str, "%0*ld", scale, (int64)data);                                     \
        } else {                                                                           \
            sprintf(str, "%ld", (int64)data);                                              \
        }                                                                                  \
        return;                                                                            \
    }                                                                                      \
    /* get the absolute value of data, it's useful for sprintf */                          \
    num = data;                                                                            \
    trailing = num % P10_INT64;                                                            \
    num = num / P10_INT64;                                                                 \
    /* two int64 num can represent the int128 data */                                      \
    if (INT128_INT64_EQ(num)) {                                                            \
        leading = (int64)num;                                                              \
        if (preceding_zero) {                                                              \
            Assert(scale > 18);                                                            \
            sprintf(str, "%0*ld%018ld", scale - 18, leading, trailing);                    \
        } else {                                                                           \
            sprintf(str, "%ld%018ld", leading, trailing);                                  \
        }                                                                                  \
        return;                                                                            \
    }                                                                                      \
    /* two int64 num can't represent int128data, use 3 int64 numbers */                    \
    middle = num % P10_INT64;                                                              \
    num = num / P10_INT64;                                                                 \
    leading = (int64)num;                                                                  \
    /* both the middle and trailing digits have 18 digits */                               \
    if (preceding_zero) {                                                                  \
        Assert(scale > 36);                                                                \
        sprintf(str, "%0*ld%018ld%018ld", scale - 36, leading, middle, trailing);          \
    } else {                                                                               \
        sprintf(str, "%ld%018ld%018ld", leading, middle, trailing);                        \
    }                                                                                      \
    return;                                                                                \
}

INT128_TO_String_Func(int128_to_string_preceding_zero, true);
INT128_TO_String_Func(int128_to_string, false);

/*
 * @Description: print bi128 data to string with trailing zeroes if exists
 *               Internal function
 *
 * @IN  data:   int128 data
 * @IN  scale:  the scale of bi128 data
 * @Return: Output string for numeric data type with trailing zeroes if exists
 */
char *bi128_out_internal(int128 data, int scale)
{
	char buf[MAXBI128LEN];
	char* result = NULL;
	int128 pre_val = 0;
	int128 post_val = 0;

	if (unlikely(scale < 0 || scale > MAXINT128DIGIT))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				errmsg("value overflows bi128 format")));
	}

	/* data equals to "INT128_MIN" */
	if (unlikely(data == INT128_MIN))
	{
		/*
		 * Avoid problems with the most negative integer not being representable
		 * as a positive integer.
		 */
		if (scale > 0)
		{
			int len = strlen(int128_min_str) - scale;
			memcpy(buf, int128_min_str, len);
			buf[len] = '.';
			memcpy(buf + len + 1, int128_min_str + len, scale + 1);
		}
		else
		{
			memcpy(buf, int128_min_str, strlen(int128_min_str) + 1);
		}
		result = pstrdup(buf);
		return result;
	}

	/* data is positive */
	if (data >= 0)
	{
		pre_val = data / ScaleMultiplerExt[scale];
		post_val = data % ScaleMultiplerExt[scale];
		if (likely(scale > 0))
		{
			int len;
			(void)int128_to_string(pre_val, buf, MAXBI128LEN, 0);
			len = strlen(buf);
			buf[len] = '.';
			(void)int128_to_string_preceding_zero(post_val, buf + len + 1, MAXBI128LEN - len - 1, scale);
		}
		else
		{
			(void)int128_to_string(pre_val, buf, MAXBI128LEN, 0);
		}
	}
	else     /* data is negative */
	{
		data = -data;
		pre_val = data / ScaleMultiplerExt[scale];
		post_val = data % ScaleMultiplerExt[scale];
		buf[0] = '-';
		if (likely(scale > 0))
		{
			int len;
			int128_to_string(pre_val, buf + 1, MAXBI128LEN - 1, 0);
			len = strlen(buf);
			buf[len] = '.';
			int128_to_string_preceding_zero(post_val, buf + len + 1, MAXBI128LEN - len - 1, scale);
		}
		else
		{
			int128_to_string(pre_val, buf + 1, MAXBI128LEN - 1, 0);
		}
	}
	result = pstrdup(buf);
	return result;
}

/*
 * @Description: print bi128 data to string like numeric_out.
 *               Output function.
 *
 * @IN  data:   int128 data
 * @IN  scale:  the scale of bi128 data
 * @Return: Output string for numeric data type
 *          Output suppres trailing zeroes in opentenbase_ora mode
 */
Datum bi128_out(int128 data, int scale)
{
	char* result = bi128_out_internal(data, scale);

	if (ORA_MODE)
	{
		suppres_trailing_zeroes(result);
	}

	PG_RETURN_CSTRING(result);
}

inline void Int128GetVal(bool is_int128, int128* val, Numeric* arg)
{
	if (is_int128)
	{
		memcpy(val, (*arg)->choice.n_bi.n_data, sizeof(int128));
	}
	else
	{
		*val = NUMERIC_64VALUE(*arg);
	}
}

int get_whole_scale(const NumericVar *numVar)
{
	int whole_scale = 0;
	int first_digit;
	if (numVar->weight >= 0 && numVar->ndigits > 0)
	{
		whole_scale = numVar->weight * DEC_DIGITS + numVar->dscale;
		first_digit = numVar->digits[0];
		if (first_digit >= 1000)
		{
			whole_scale = whole_scale + 4;
		}
		else if (first_digit >= 100)
		{
			whole_scale = whole_scale + 3;
		}
		else if (first_digit >= 10)
		{
			whole_scale = whole_scale + 2;
		}
		else
		{
			whole_scale = whole_scale + 1;
		}
	}
	else
	{
		whole_scale = numVar->dscale;
	}

	return whole_scale;
}

#define update_result(__result, __digits, __weight) \
    do {                                            \
        (__result) *= NBASE;                        \
        (__result) += (__digits)[(__weight)];       \
    } while (0)

#define encode_digits(__result, __digits, __ndigits)  \
    do {                                              \
        switch (__ndigits) {                          \
            case 6: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                update_result(__result, __digits, 3); \
                update_result(__result, __digits, 4); \
                update_result(__result, __digits, 5); \
                break;                                \
            }                                         \
            case 5: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                update_result(__result, __digits, 3); \
                update_result(__result, __digits, 4); \
                break;                                \
            }                                         \
            case 4: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                update_result(__result, __digits, 3); \
                break;                                \
            }                                         \
            case 3: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                update_result(__result, __digits, 2); \
                break;                                \
            }                                         \
            case 2: {                                 \
                (__result) = (__digits)[0];           \
                update_result(__result, __digits, 1); \
                break;                                \
            }                                         \
            case 1: {                                 \
                (__result) = (__digits)[0];           \
                break;                                \
            }                                         \
            default: {                                \
                Assert(0 == (__ndigits));             \
                (__result) = 0;                       \
                break;                                \
            }                                         \
        }                                             \
    } while (0)

int64 convert_short_numeric_to_int64_byscale(_in_ Numeric n, _in_ int scale)
{
	int128 result = 0;
	int weight = NUMERIC_WEIGHT(n);
	int ndigits = SHORT_NUMERIC_NDIGITS(n);
	int ascale = (ndigits > 0) ? (ndigits - weight - 1) : 0;
	int scaleDiff = scale - ascale * DEC_DIGITS;
	NumericDigit* digits = SHORT_NUMERIC_DIGITS(n);

	encode_digits(result, digits, ndigits);

	/* adjust scale */
	result = (scaleDiff > 0) ? (result * ScaleMultipler[scaleDiff]) : (result / ScaleMultipler[-scaleDiff]);
	/* get the result by sign */
	result = (NUMERIC_POS == NUMERIC_SIGN(n)) ? result : -result;
	Assert(INT128_INT64_EQ(result));

	return (int64)result;
}


/*
 * n: ndigits
 * w: weight
 * a: ascale  =  n - (w + 1)
 * d: dscale (display scale)
 * a <= 0 means no scale part in NumericShort;
 * w <  0 means just scale part in NumericShort;
 *
 * ... w=1   w=0    w=-1 ...
 * ... xxxx  xxxx . xxxx ...
 * ... a=-1  a=0    a=1  ...
 *
 * convert NumericShort to int128:
 * step 1: get all valid digitals to result; if d%4 != 0, just get valid
 *         digitals from the last item in digits[]. such as digits[-1] = 6800,
 *         d%4=2, so just 68 is needed.
 *
 * step 2: get diff_scale as four cases below:
 *   +--------------------------------------+---------------------------------+
 *   | a <= 0                               |                                 |
 *   +--------------------------------------+                                 |
 *   | a > 0 and (d%4) == 0                 |  diff_scale = d - a*4           |
 *   +--------------------------------------+                                 |
 *   | a > 0 and (d+4)/4 > a                |                                 |
 *   +--------------------------------------+---------------------------------+
 *   | a > 0 and (d%4) != 0 and (d+4)/4==a  |  diff_scale = d - (a-1)*4 - d%4 |
 *   +------------------------------------------------------------------------+
 * NOTE:
 *   case 4 : (a-1)*4+d%4 means the number of the valid digitals in scale part
 * example:
 *   case 1: 1.0
 *   case 2: dec(4,4) 1.1230, dec(4,8) 1.1230
 *   case 3: dec(6,9) 1.1230
 *   case 4: dec(6,3) 1.1230
 *
 * setp 3:
 *   result *= S_INT128_POWER[diff_scale]
 *
 *==============================================================================
 *
 * @Description: convert PG Numeric format to the data type of int128
 *
 * @IN  value: the source data with PG Numeric format
 * @IN  value: display scale
 * @OUT value: result, the result of the data type of int128.
 * @return: None
 */
void convert_short_numeric_to_int128_byscale(_in_ Numeric n, _in_ int dscale, _out_ int128 *result)
{
	bool special_do = false;
	int ndigits = SHORT_NUMERIC_NDIGITS(n);
	int remainder = dscale % DEC_DIGITS;
	int weight = NUMERIC_WEIGHT(n);
	int ascale = ndigits - (weight + 1);
	int end_index = ndigits;
	int diff_scale = 0;
	NumericDigit* digits = SHORT_NUMERIC_DIGITS(n);
	int i;
	Assert(ndigits >= 0);

	/* ndigits is 0, result is 0, return directly */
	if (0 == ndigits)
	{
		*result = 0;
		return;
	}

	if (ascale > 0 && remainder != 0 && (dscale / DEC_DIGITS + 1) == ascale)
	{
		special_do = true;
		--end_index;
	}

	/* step1. get all valid digitals to result */
	for (i = 0; i < end_index; i++)
		*result += (digits[i] * ScaleMultiplerExt[(end_index - 1 - i) * DEC_DIGITS]);

	if (special_do)
	{
		*result = (*result * ScaleMultiplerExt[remainder]) +
				  (digits[ndigits - 1] / ScaleMultiplerExt[DEC_DIGITS - remainder]);
		/* step2. get diff_scale by dscale and ascale */
		diff_scale = dscale - (ascale - 1) * 4 - dscale % 4;
	}
	else
	{
		/* step2. get diff_scale by dscale and ascale */
		diff_scale = dscale - ascale * 4;
	}

	/* step3. adjust result by diff_scale */
	*result *= ScaleMultiplerExt[diff_scale];

	*result = (NUMERIC_POS == NUMERIC_SIGN(n)) ? *result : -(*result);
}

Datum
try_convert_numeric_normal_to_fast(Datum value)
{
	Numeric val = DatumGetNumeric(value);
	NumericVar numVar;
	int whole_scale;

	if (NUMERIC_IS_NANORBI(val) || !numeric_optimizer)
		return NumericGetDatum(val);

	init_var_from_num(val, &numVar);

	whole_scale = get_whole_scale(&numVar);

	// should be ( whole_scale <= MAXINT64DIGIT)
	if (CAN_CONVERT_BI64(whole_scale))
	{
		int64 result = convert_short_numeric_to_int64_byscale(val, numVar.dscale);
		return makeNumeric64(result, numVar.dscale);
	}
	else if (CAN_CONVERT_BI128(whole_scale))
	{
		int128 result = 0;
		convert_short_numeric_to_int128_byscale(val, numVar.dscale, &result);
		return makeNumeric128(result, numVar.dscale);
	}
	else
	{
		return NumericGetDatum(val);
	}
}


/* Numeric Compression Codes Area */

/* ascale: adjusted scale */
#if DEC_DIGITS == 4

/*
 * we know that it's failed to convert a numeric value to int64,
 * whose digits number is equal to or greater than 6.
 */
#define NUMERIC_NDIGITS_UPLIMITED (6)
#define NUMERIC_NDIGITS_INT128_UPLIMITED (11)

/*
 * infor about holding int64 min value.
 * 1. how many digits at least;
 * 2. its tailing data;
 */
#define INT64_MIN_VAL_NDIGITS (5)
#define INT64_MIN_VALUE_LAST (5808)
#define INT64_MAX_VALUE_FIRST (922)

/* max buffer size for holding an valid int64 numeric. */
#define MAX_NUMERIC_BUFFER_SIZE (NUMERIC_HDRSZ_SHORT + INT64_MIN_VAL_NDIGITS * sizeof(NumericDigit))

/* the same to MAX_NUMERIC_BUFFER_SIZE but for 1 varlena head */
#define MAX_1HEAD_NUMERIC_BUFFER_SIZE (VARHDRSZ_SHORT + sizeof(uint16) + INT64_MIN_VAL_NDIGITS * sizeof(NumericDigit))

#endif


const int16 INT16_MIN_VALUE = INT16_MIN;  // equal to 0x8000
const int16 INT16_MAX_VALUE = INT16_MAX;  // equal to 0x7fff
const int32 INT32_MIN_VALUE = INT32_MIN;  // equal to 0x80000000
const int32 INT32_MAX_VALUE = INT32_MAX;  // equal to 0x7fffffff
const int64 INT64_MIN_VALUE = INT64_MIN;  // equal to 8000000000000000
const int64 INT64_MAX_VALUE = INT64_MAX;  // equal to 7FFFFFFFFFFFFFFF   

/* make a copy with 4B varlena header. */
static inline Numeric numeric_copy(Numeric shortNum, char* outBuf)
{
    const int diff = VARHDRSZ - VARHDRSZ_SHORT;
    int len = VARSIZE_SHORT(shortNum);

    /* Notice: include two parts:
     * 1. numeric header, uint16;
     * 2. numeric digits, int16[];
     * so that we also can handle 0 numeric.
	 */
    int nShortDigtis = (len - VARHDRSZ_SHORT) / sizeof(uint16);

    char* buf = outBuf;
    uint16* dest = (uint16*)(buf + VARHDRSZ);
    uint16* src = (uint16*)((char*)shortNum + VARHDRSZ_SHORT);

    Assert(nShortDigtis >= 1 && nShortDigtis <= NUMERIC_NDIGITS_UPLIMITED);
	Assert(VARATT_IS_SHORT(shortNum));

    /* set varlena header size. */
    SET_VARSIZE(buf, len + diff);
    
	/* copy all the data, including flags and digits. */
    switch (nShortDigtis) {
        case 6:
            *dest++ = *src++;
            /* fall through */
        case 5:
            *dest++ = *src++;
            /* fall through */
        case 4:
            *dest++ = *src++;
            /* fall through */
        case 3:
            *dest++ = *src++;
            /* fall through */
        case 2:
            *dest++ = *src++;
            /* fall through */
        case 1:
        default:
            *dest++ = *src++;
            break;
    }

    return (Numeric)buf;
}

/*
 * encoding numeric to dscale int64
 * input parameter:
 * 		batchValues: array of numeric values
 * 		batchNulls: array of nulls values
 * 		batchRows: number of array
 * output parameter:
 * 		outInt: array of converted int64
 * 		outDscales: array of dscale
 * 		outSuccess: array of success convert
 * Return:
 * 		number of success convert
 */
int batch_convert_short_numeric_to_int64(Datum* batchValues, char* batchNulls, int batchRows,
    int64* outInt, char* outDscales, bool* outSuccess, int* outNullCount, bool hasNull)
{
    NumericVar numVar;
    Numeric num = NULL;
    char* outBuffer = NULL;
    char* tmpOutBuf = NULL;
    char* notNullDscales = outDscales;
    int batchCnt = 0;
    int dscale = 0;
    int i = 0;

	Assert(4 == DEC_DIGITS);
	outBuffer = (char*)palloc(MAX_NUMERIC_BUFFER_SIZE * batchRows);
    tmpOutBuf = outBuffer;

    for (i = 0; i < batchRows; ++i) 
	{
        if (hasNull && bitmap_query(batchNulls, i)) 
		{
            /* treat NULL as failed case. */
            *outSuccess++ = false;
            (*outNullCount)++;
            continue;
        }

        num = (Numeric)DatumGetPointer(batchValues[i]);

        /*
		 * make sure that every Numeric in batch is with 4 byte var-header.
         * if it's with 1 byte var-header, we have to do a copy
         * which is with 4 bytes var-header.
		 */
        if (unlikely(VARATT_IS_SHORT(num))) 
		{
            /* this short numeric is out of range. */
            if (unlikely(VARSIZE_SHORT(num) > MAX_1HEAD_NUMERIC_BUFFER_SIZE)) 
			{
                *outSuccess++ = false;
                *notNullDscales++ = FAILED_DSCALE_ENCODING;
                continue;
            }

            num = numeric_copy(num, tmpOutBuf);
            tmpOutBuf += MAX_NUMERIC_BUFFER_SIZE;
        }

        if (NUMERIC_IS_BI64(num)) 
		{
            /* numeric with bigint64 */
            *outSuccess++ = true;
            *outInt++ = NUMERIC_64VALUE(num);
            dscale = NUMERIC_BI_SCALE(num);
            Assert(dscale < MAXINT64DIGIT && dscale >= 0);
            *notNullDscales++ = (char)dscale;
            ++batchCnt;
        }
		else if (NUMERIC_IS_BI128(num)) 
		{
            /* numeric with bigint128 */
            *outSuccess++ = false;
            *notNullDscales++ = FAILED_DSCALE_ENCODING;
        }
		else if (NUMERIC_IS_NAN(num))
		{
            *outSuccess++ = false;
            *notNullDscales++ = FAILED_DSCALE_ENCODING;
        }
        else if (NUMERIC_IS_INF(num))
        {
            *outSuccess++ = false;
            *notNullDscales++ = FAILED_DSCALE_ENCODING;
        }
		else
		{
            int whole_scale = 0 ;
            /* numeric */
            init_var_from_num(num, &numVar);

            /* whole scale */
            whole_scale = get_whole_scale(&numVar);

            if (likely(CAN_CONVERT_BI64(whole_scale))) 
			{
                /* convert to BI64 */
                *outSuccess++ = true;
                *outInt++ = convert_short_numeric_to_int64_byscale(num, numVar.dscale);
                Assert(numVar.dscale < MAXINT64DIGIT && numVar.dscale >= 0);
                *notNullDscales++ = (char)numVar.dscale;
                ++batchCnt;
            }
			else
			{
                /* can't not convert to BI64 */
                *outSuccess++ = false;
                *notNullDscales++ = FAILED_DSCALE_ENCODING;
            }
        }
    }

    return batchCnt;
}

/* one member of make_numeric() family. this is for numeric 0. */
static inline int make_short_numeric_of_zero(Numeric result, int typmod)
{
    /* set display scale if typmod is given, otherwise is 0 at default. */
    int dscale = (typmod >= (int32)(VARHDRSZ)) ? ((typmod - VARHDRSZ) & 0xffff) : 0;

    SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);                                   /* length info */
    result->choice.n_short.n_header = NUMERIC_SHORT                             /* sign is NUMERIC_POS */
                                      | (dscale << NUMERIC_SHORT_DSCALE_SHIFT)  /* dscale info */
        ;
    return (int)NUMERIC_HDRSZ_SHORT;
}

/* one member of make_numeric() family.
 * make numeric result for the min-int64 value. and return the bytes size used. */
static inline int make_short_numeric_of_int64_minval(Numeric result, int dscale, int weight)
{
    const int len = MAX_NUMERIC_BUFFER_SIZE;
    NumericDigit* digits = result->choice.n_short.n_data;

    SET_VARSIZE(result, len);

    result->choice.n_short.n_header = (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)            /* sign info */
                                      | (dscale << NUMERIC_SHORT_DSCALE_SHIFT)             /* display info */
                                      | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)  /* weight info */
                                      | (weight & NUMERIC_SHORT_WEIGHT_MASK);

    /* reference INT64_MIN_VALUE value. */
    digits[0] = 922;
    digits[1] = 3372;
    digits[2] = 368;
    digits[3] = 5477;
    digits[4] = INT64_MIN_VALUE_LAST;

    return len;
}

static inline int get_weight_from_ascale(int ndigits, int ascale)
{
    return (ndigits - ascale - 1);
}

/* the same to make_result() but only for short numeric. and the bytes size will be returned also. */
static inline int make_short_numeric(
    Numeric result, NumericDigit* digits, int ndigits, int sign, int weight, int dscale)
{
    const int totalSize = (NUMERIC_HDRSZ_SHORT + ndigits * sizeof(NumericDigit));
    NumericDigit* src = NULL;
    NumericDigit* dest = NULL;

    Assert(sign != NUMERIC_NAN);
    Assert(NUMERIC_CAN_BE_SHORT(dscale, weight));

    /* check the leading and tailing zeros have been stripped. */
    Assert(ndigits > 0 && ndigits <= NUMERIC_NDIGITS_INT128_UPLIMITED);
    Assert(digits[0] != 0);

    /* step 1: set length info */
    SET_VARSIZE(result, totalSize);

    /* step 2: set head info, including sign, weight and dscale. */
    result->choice.n_short.n_header =
        ((sign == NUMERIC_NEG) ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK) : NUMERIC_SHORT) |
        (dscale << NUMERIC_SHORT_DSCALE_SHIFT) | (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0) |
        (((uint32)weight) & NUMERIC_SHORT_WEIGHT_MASK);

    /* step 3: build the data info */
    src = digits;
    dest = result->choice.n_short.n_data;
    switch (ndigits)
    {
        case 11:
            *dest++ = *src++;
            /* fall through */
        case 10:
            *dest++ = *src++;
            /* fall through */
        case 9:
            *dest++ = *src++;
            /* fall through */
        case 8:
            *dest++ = *src++;
            /* fall through */
        case 7:
            *dest++ = *src++;
            /* fall through */
        case 6:
            *dest++ = *src++;
            /* fall through */
        case 5:
            *dest++ = *src++;
            /* fall through */
        case 4:
            *dest++ = *src++;
            /* fall through */
        case 3:
            *dest++ = *src++;
            /* fall through */
        case 2:
            *dest++ = *src++;
            /* fall through */
        case 1:
            *dest++ = *src++;
            /* fall through */
        default:
            break;
    }

    /* Check for overflow of int16 fields */
    Assert(NUMERIC_NDIGITS(result) == (unsigned int)(ndigits));
    Assert(weight == NUMERIC_WEIGHT(result));
    Assert(dscale == NUMERIC_DSCALE(result));
    return totalSize;
}
