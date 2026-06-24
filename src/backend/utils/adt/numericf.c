#include "utils/elog.h"

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

#define MAXINT64DIGIT 19

static inline bool check_numericf_overflow(int64 val, short scale);

/*
 * @Description: fast compare whether the result of arg * ScaleMultipler[i]
 *               is out of int64 bound. getScaleQuotient(i) equals to
 *               INT64_MIN / ScaleMultipler[i]
 * @IN  scale: num between 0 and MAXINT64DIGIT
 * @Return: values[scale]
 */
#define getScaleQuotient(scale) INT64_MAX / ScaleMultipler[scale]

#define CHECK_SCALE_OVERFLOW_INT64(val, scale ) 									\
				if (check_numericf_overflow((val), (scale)))				\
				{															\
					ereport(ERROR,											\
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),		\
						errmsg("value overflows numeric int64 format")));	\
				}

#define NUMERICF_INT64_ADD(val1, val2, result)								\
				if (unlikely(pg_add_s64_overflow(val1,						\
												 val2,						\
												 result)))					\
				{															\
					ereport(ERROR,											\
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),			\
				 	errmsg("value overflows numeric int64 format")));		\
				}

#define NUMERICF_INT64_SUB(val1, val2, result)								\
				if (unlikely(pg_sub_s64_overflow(val1,						\
												 val2,						\
												 result)))					\
				{															\
					ereport(ERROR,											\
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),			\
				 	errmsg("value overflows numeric int64 format")));		\
				}


static inline bool
check_numericf_overflow(int64 val, short scale)
{
	int64 tmpval;

	if (val == 0)
		return false;

	tmpval = val > 0 ? val : -val;

	if (unlikely(tmpval >= getScaleQuotient(scale)))
	{
		return true;
	}

	return false;
}

static inline int
get_int64_digit_num(int64 num)
{
	int			i = 1;

	num = num > 0 ? num : -num;

	for (;i <= 19; i++)
	{
		if (num < ScaleMultipler[i])
			return i;
	}

	return MAXINT64DIGIT;
}

static void
convert_numeric_numericf(const NumericVar *var, NumericfVar *result)
{
	int			dscale;
	int			i;
	int			d;
	int64		res = 0;
	NumericDigit dig;

	elog(ERROR, "Data type numericf hash been deprecated.");

	result->scale = var->dscale;
	dscale = result->scale;

	if (dscale > MAXINT64DIGIT ||
		(var->weight > 0 && 
		  ((DEC_DIGITS * (var->weight - 1) +
		    get_int64_digit_num(var->digits[0])) + dscale > MAXINT64DIGIT)))
	{
		ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
		 	errmsg("value overflows numeric int64 format")));
	}

	/*
	 * Output all digits before the decimal point
	 */
	if (var->weight < 0)
	{
		d = var->weight + 1;
	}
	else
	{
		for (d = 0; d <= var->weight; d++)
		{
			dig = (d < var->ndigits) ? var->digits[d] : 0;
			/* In the first digit, suppress extra leading decimal zeroes */
#if DEC_DIGITS == 4
			{
				res = res * 10000 + dig;
			}
#elif DEC_DIGITS == 2
			{
				res = res * 100 + dig;
			}
#elif DEC_DIGITS == 1
			{
				res = dig;
			}
#else
#error unsupported NBASE
#endif
		}
	}

	result->value = res * ScaleMultipler[dscale];
	res=0;

	/*
	 * If requested, output a decimal point and all the digits that follow it.
	 * We initially put out a multiple of DEC_DIGITS digits, then truncate if
	 * needed.
	 */
	if (dscale > 0)
	{
		for (i = 0; i < dscale; d++, i += DEC_DIGITS)
		{
			dig = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
#if DEC_DIGITS == 4
			if (dscale - i >= 4)
			{	
				res = res * 10000 + dig;
			}
			else if (dscale - i == 3)
			{
				res = res * 1000 + dig / 10;
			}
			else if (dscale - i == 2)
			{
				dig = 
				res = res * 100 + dig / 100;
			}
			else
			{
				res = res * 10 + dig / 1000;
			}
			
#elif DEC_DIGITS == 2
			if (dscale - i > 2)
			{	
				res = res * 100 + dig;
			}
			else
			{
				res = res * 10 + dig / 10;
			}
#elif DEC_DIGITS == 1
			res = res + dig;
#else
#error unsupported NBASE
#endif
		}
	}
	/*
	 * terminate the string and return it
	 */
	result->value = result->value + res;

	/*
	 * Output a dash for negative values
	 */
	if (var->sign == NUMERIC_NEG)
		result->value = - result->value;
	return;
}

Datum
numericf_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	int32		typmod = PG_GETARG_INT32(2);
	NumericfVar	*res;
	const char *cp;

	elog(ERROR, "Data type numericf hash been deprecated.");

	/* Skip leading spaces */
	cp = str;
	while (*cp)
	{
		if (!isspace((unsigned char) *cp))
			break;
		cp++;
	}
	/*
	 * Check for NaN
	 */
	if (pg_strncasecmp(cp, "NaN", 3) == 0)
	{
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

		res = (NumericfVar *) palloc(sizeof(NumericfVar));

		res->value = 0;
		res->scale = -1;
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
		res = (NumericfVar *) palloc(sizeof(NumericfVar));
		convert_numeric_numericf(&value, res);
		free_var(&value);
	}

	PG_RETURN_NUMERICF(res);
}

Datum
numericf_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

#ifdef NOT_USED
	Oid			typelem = PG_GETARG_OID(1);
#endif
	NumericfVar	*res;

	elog(ERROR, "Data type numericf hash been deprecated.");

	res = (NumericfVar *) palloc(sizeof(NumericfVar));

	pq_copymsgbytes(buf, (char *) &res->value, sizeof(int64));
	res->scale = pq_getmsgint(buf, sizeof(res->scale));

	PG_RETURN_NUMERICF(res);
}

/*
 *		numeric_send			- converts numeric to binary format
 */
Datum
numericf_send(PG_FUNCTION_ARGS)
{
	StringInfoData		buf;
	NumericfVar*		num = PG_GETARG_NUMERICF(0);

	elog(ERROR, "Data type numericf hash been deprecated.");

	pq_begintypsend(&buf);

	pq_sendint64(&buf, num->value);
	pq_sendint8(&buf, num->scale);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
numericf_out(PG_FUNCTION_ARGS)
{
	NumericfVar	*res = PG_GETARG_NUMERICF(0);

	elog(ERROR, "Data type numericf hash been deprecated.");

	PG_RETURN_CSTRING(bi64_out(res->value, res->scale));
}

Datum
numericf_eq(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		PG_RETURN_BOOL(arg1->value == arg2->value);
	}
	else if (arg1->scale > arg2->scale)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, arg1->scale - arg2->scale);
		PG_RETURN_BOOL(arg1->value == (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]));
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value, arg2->scale - arg1->scale);
		PG_RETURN_BOOL(((int64)arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) == arg2->value);
	}
}

Datum
numericf(PG_FUNCTION_ARGS)
{
	NumericfVar	*num;
	int32		 typmod;
	short		 scale;

	elog(ERROR, "Data type numericf hash been deprecated.");

	num = PG_GETARG_NUMERICF(0);
	typmod = PG_GETARG_INT32(1);
	scale = (typmod - VARHDRSZ) & 0xffff;

	if (scale == num->scale)
	{
		PG_RETURN_NUMERICF(num);
	}
	else if (scale > num->scale)
	{
		if (scale > MAXINT64DIGIT)
		{
			ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numericf format")));
		}

		CHECK_SCALE_OVERFLOW_INT64(num->value, scale - num->scale);
		num->value = num->value * ScaleMultipler[scale - num->scale];
		num->scale = scale;
		PG_RETURN_NUMERICF(num);
	}
	else
	{
		num->value =  num->value > 0 ?
				(num->value + ScaleMultipler[num->scale - scale - 1] * 5 ) / ScaleMultipler[num->scale - scale] :
				(num->value - ScaleMultipler[num->scale - scale - 1] * 5 ) / ScaleMultipler[num->scale - scale];
		num->scale = scale;
		PG_RETURN_NUMERICF(num);
	}
}

Datum
numericf_transform(PG_FUNCTION_ARGS)
{
	FuncExpr   *expr = castNode(FuncExpr, PG_GETARG_POINTER(0));
	Node	   *ret = NULL;
	Node	   *typmod;

	elog(ERROR, "Data type numericf hash been deprecated.");

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

Datum
numericf_ne(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		PG_RETURN_BOOL(arg1->value != arg2->value);
	}
	else if (arg1->scale > arg2->scale)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, arg1->scale - arg2->scale);
		PG_RETURN_BOOL(arg1->value != (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]));
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value,arg2->scale - arg1->scale);
		PG_RETURN_BOOL(((int64)arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) != arg2->value);
	}
}

Datum
numericf_lt(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		PG_RETURN_BOOL(arg1->value < arg2->value);
	}
	else if (arg1->scale > arg2->scale)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, arg1->scale - arg2->scale);
		PG_RETURN_BOOL(arg1->value < (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]));
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value,arg2->scale - arg1->scale);
		PG_RETURN_BOOL(((int64)arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) < arg2->value);
	}
}

Datum
numericf_le(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		PG_RETURN_BOOL(arg1->value <= arg2->value);
	}
	else if (arg1->scale > arg2->scale)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, arg1->scale - arg2->scale);
		PG_RETURN_BOOL(arg1->value <= (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]));
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value,arg2->scale - arg1->scale);
		PG_RETURN_BOOL(((int64)arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) <= arg2->value);
	}
}

Datum
numericf_gt(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		PG_RETURN_BOOL(arg1->value > arg2->value);
	}
	else if (arg1->scale > arg2->scale)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, arg1->scale - arg2->scale);
		PG_RETURN_BOOL(arg1->value > (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]));
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value,arg2->scale - arg1->scale);
		PG_RETURN_BOOL((int64)(arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) > arg2->value);
	}
}

Datum
numericf_ge(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		PG_RETURN_BOOL(arg1->value >= arg2->value);
	}
	else if (arg1->scale > arg2->scale)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, arg1->scale - arg2->scale);
		PG_RETURN_BOOL(arg1->value >= (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]));
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value,arg2->scale - arg1->scale);
		PG_RETURN_BOOL(((int64)arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) >= arg2->value);
	}
}

/*
 *		numericf_pl			- returns arg1 + arg2
 *		numericf_mi			- returns arg1 - arg2
 *		numericf_mul		- returns arg1 * arg2
 *		numericf_div		- returns arg1 / arg2
 */
Datum
numericf_pl(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;
	NumericfVar	*result;
	short		scale1 = 0;
	short		scale2 = 0;
	int64		res = 0;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));
	scale1 = arg1->scale;
	scale2 = arg2->scale;

	if (scale1 == scale2)
	{

		NUMERICF_INT64_ADD(arg1->value, arg2->value, &res);
		result->value = res;
		result->scale = scale1;
	}
	else if (scale1 > scale2)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, scale1 - scale2);
		NUMERICF_INT64_ADD(arg1->value, arg2->value * ScaleMultipler[scale1 - scale2], &res);
		result->value = res;
		result->scale = scale1;
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value, scale2 - scale1);
		NUMERICF_INT64_ADD(arg1->value * ScaleMultipler[scale2 - scale1], arg2->value, &res);
		result->value = res;
		result->scale = scale2;
	}
	PG_RETURN_NUMERICF(result);
}

Datum
numericf_mi(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;
	NumericfVar	*result;
	short		scale1 = 0;
	short		scale2 = 0;
	int64		res = 0;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));
	scale1 = arg1->scale;
	scale2 = arg2->scale;

	if (scale1 == scale2)
	{
		NUMERICF_INT64_SUB(arg1->value, arg2->value, &res);
		result->value = res;
		result->scale = scale1;
	}
	else if (scale1 > scale2)
	{
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, scale1 - scale2);
		NUMERICF_INT64_SUB(arg1->value, arg2->value * ScaleMultipler[scale1 - scale2], &res);
		result->value = res;
		result->scale = scale1;
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value, scale2 - scale1);
		NUMERICF_INT64_SUB(arg1->value * ScaleMultipler[scale2 - scale1], arg2->value, &res);
		result->value = res;
		result->scale = scale2;
	}

	PG_RETURN_NUMERICF(result);
}

Datum
numericf_mul(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;
	NumericdVar	*result;
	int128		 res = 0;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);
	result = (NumericdVar *) palloc0(sizeof(NumericdVar));

	if (likely(arg1->scale + arg2->scale <= MAXINT128DIGIT &&
			   !pg_mul_s128_overflow(arg1->value, arg2->value, &res)))
	{
		result->value = res;
		result->scale = arg1->scale + arg2->scale;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value overflows numericf format")));
	}

	PG_RETURN_NUMERICD(result);
}

/*
 * Default scale selection for division
 *
 * Returns the appropriate result scale for the division result.
 */
static int
select_div_numericf_scale(const NumericfVar *var1, const NumericfVar *var2)
{
	int			weight1,
				weight2,
				qweight;
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
	weight1 = (get_int64_digit_num(var1->value) - var1->scale -1) / DEC_DIGITS;
	firstdigit1 = var1->value / ScaleMultipler[weight1 * DEC_DIGITS + var1->scale];
	weight2 = (get_int64_digit_num(var2->value) - var2->scale - 1) / DEC_DIGITS;
	firstdigit2 = var2->value / ScaleMultipler[weight2 * DEC_DIGITS + var2->scale];

	/*
	 * Estimate weight of quotient.  If the two first digits are equal, we
	 * can't be sure, but assume that var1 is less than var2.
	 */
	qweight = weight1 - weight2;
	if (firstdigit1 <= firstdigit2)
		qweight--;

	/* Select result scale */
	rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
	rscale = Max(rscale, var1->scale);
	rscale = Max(rscale, var2->scale);
	rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
	rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

	return rscale;
}

static void
numericfdiv_internal(NumericfVar *arg1, NumericfVar	*arg2, NumericdVar *result)
{
	int128					res;
	int 					scale = 0;
	int 					diff_scale = 0;
	FunctionCallInfoData	finfo;
	Numeric					big_result;
	NumericVar				x;

	/* the reason of rscale++ is for calculate bound of last digit */
	scale = select_div_numericf_scale(arg1, arg2);
	scale++;
	diff_scale = arg2->scale - arg1->scale + scale;
	if (arg2->value == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DIVISION_BY_ZERO),
				 errmsg("division by zero")));

	if (scale <= MAXINT128DIGIT &&
		diff_scale <= MAXINT128DIGIT &&
		!check_numericd_overflow(arg1->value, diff_scale))
	{
		res = (arg1->value*ScaleMultiplerExtd[diff_scale]) / arg2->value;
		if (res > 0)
		{
			res = ((res % 10) >= 5) ? (res / 10 + 1) : res / 10;
		}
		else
		{
			res = ((res % 10) <= -5) ? (res / 10 - 1) : res / 10;
		}
		scale--;
		result->value = res;
		result->scale = scale;
		return;
	}

	/* result is out of int128 bound, call numeric_div calculate */
	finfo.arg[0] = (Datum)DirectFunctionCall1(numericf_numeric, PointerGetDatum(arg1));
	finfo.arg[1] = (Datum)DirectFunctionCall1(numericf_numeric, PointerGetDatum(arg2));

	big_result = (Numeric) numeric_div(&finfo);
	init_var_from_num(big_result, &x);
	convert_numeric_numericd(&x, result);

	return;
}

Datum
numericf_div(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;
	NumericdVar	*result;

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	elog(ERROR, "Data type numericf hash been deprecated.");

	result = (NumericdVar *) palloc(sizeof(NumericdVar));
	numericfdiv_internal(arg1, arg2, result);

	PG_RETURN_NUMERICD(result);
}

Datum
btnumericfcmp(PG_FUNCTION_ARGS)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;
	int			scale1;
	int			scale2;
	int64		arg1_int, arg2_int;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);
	scale1 = arg1->scale;
	scale2 = arg2->scale;

	if (scale1 == scale2)
	{
		arg1_int = arg1->value;
		arg2_int = arg2->value;
	}
	else if (scale1 > scale2)
	{
		arg1_int = arg1->value;
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, scale1 - scale2);
		arg2_int = arg2->value * ScaleMultipler[scale1 - scale2];
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value, scale2 - scale1);
		arg1_int = arg1->value * ScaleMultipler[scale2 - scale1];
		arg2_int = arg2->value;
	}

	if (arg1_int == arg2_int)
		PG_RETURN_INT32(0);

	if (arg1_int > arg2_int)
		PG_RETURN_INT32(1);

	PG_RETURN_INT32(-1);
}

static int
btnumericffastcmp(Datum x, Datum y, SortSupport ssup)
{
	NumericfVar	*arg1;
	NumericfVar	*arg2;
	int			scale1;
	int			scale2;
	int64		arg1_int, arg2_int;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = DatumGetNumericf(x);
	arg2 = DatumGetNumericf(y);
	scale1 = arg1->scale;
	scale2 = arg2->scale;

	if (scale1 == scale2)
	{
		arg1_int = arg1->value;
		arg2_int = arg2->value;
	}
	else if (scale1 > scale2)
	{
		arg1_int = arg1->value;
		CHECK_SCALE_OVERFLOW_INT64(arg2->value, scale1 - scale2);
		arg2_int = arg2->value * ScaleMultipler[scale1 - scale2];
	}
	else
	{
		CHECK_SCALE_OVERFLOW_INT64(arg1->value, scale2 - scale1);
		arg1_int = arg1->value * ScaleMultipler[scale2 - scale1];
		arg2_int = arg2->value;
	}

	if (arg1_int == arg2_int)
		return 0;

	if (arg1_int > arg2_int)
		return 1;

	return -1;
}

Datum
btnumericfsortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	elog(ERROR, "Data type numericf hash been deprecated.");

	ssup->comparator = btnumericffastcmp;
	PG_RETURN_VOID();
}

Datum
hashnumericf(PG_FUNCTION_ARGS)
{
	NumericfVar	*res;
	int64		key;

	elog(ERROR, "Data type numericf hash been deprecated.");

	res = PG_GETARG_NUMERICF(0);
	key = res->value;

	/*
	 * On IEEE-float machines, minus zero and zero have different bit patterns
	 * but should compare as equal.  We must ensure that they have the same
	 * hash value, which is most reliably done this way:
	 */
	if (key == (int64) 0)
		PG_RETURN_UINT32(0);
	return hash_any((unsigned char *) &key, sizeof(key));
}

Datum
hashnumericfextended(PG_FUNCTION_ARGS)
{
	NumericfVar	*res;
	int64		 key;
	uint64		 seed;

	elog(ERROR, "Data type numericf hash been deprecated.");

	res = PG_GETARG_NUMERICF(0);
	key = res->value;
	seed = PG_GETARG_INT64(1);

	/* Same approach as hashfloat8 */
	if (key == (int64) 0)
		PG_RETURN_UINT64(seed);
	return hash_any_extended((unsigned char *) &key, sizeof(key), seed);
}

Datum
numeric_numericf(PG_FUNCTION_ARGS)
{
	Numeric		  num;
	NumericVar	  x;
	NumericfVar  *result;
	uint16 		  numFlags;

	elog(ERROR, "Data type numericf hash been deprecated.");

	num = PG_GETARG_NUMERIC(0);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));
	numFlags = NUMERIC_NB_FLAGBITS(num);

	if (NUMERIC_FLAG_IS_NANORBI(numFlags))
	{
		/* Handle Big Integer */
		if (NUMERIC_FLAG_IS_BI(numFlags))
			num = makeNumericNormal(num);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert NaN to integer")));
	}

	/* Convert to variable format and thence to int8 */
	init_var_from_num(num, &x);
	convert_numeric_numericf(&x, result);
	PG_RETURN_NUMERICF(result);
}

Datum
int4_numericf(PG_FUNCTION_ARGS)
{
	int32		val;
	NumericfVar  *result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_INT32(0);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));

	result->value = (int64)(val);
	result->scale = 0;
	PG_RETURN_NUMERICF(result);
}

Datum
numericf_int4(PG_FUNCTION_ARGS)
{
	NumericfVar	*val = PG_GETARG_NUMERICF(0);

	elog(ERROR, "Data type numericf hash been deprecated.");

	PG_RETURN_INT32((int32) ConvertNumericfdInt(val));
}

Datum
int8_numericf(PG_FUNCTION_ARGS)
{
	int64			val;
	NumericfVar		*result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_INT64(0);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));

	result->value = (int64)(val);
	result->scale = 0;
	PG_RETURN_NUMERICF(result);
}

Datum
numericf_int8(PG_FUNCTION_ARGS)
{
	NumericfVar	*val = PG_GETARG_NUMERICF(0);

	elog(ERROR, "Data type numericf hash been deprecated.");

	PG_RETURN_INT64((int64) ConvertNumericfdInt(val));
}

Datum
int2_numericf(PG_FUNCTION_ARGS)
{
	int16			val;
	NumericfVar		*result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_INT16(0);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));

	result->value = (int64)(val);
	result->scale = 0;
	PG_RETURN_NUMERICF(result);
}

Datum
numericf_int2(PG_FUNCTION_ARGS)
{
	NumericfVar	*val = PG_GETARG_NUMERICF(0);

	elog(ERROR, "Data type numericf hash been deprecated.");

	PG_RETURN_INT16((int16) ConvertNumericfdInt(val));
}

Datum
float8_numericf(PG_FUNCTION_ARGS)
{
	float8			val;
	NumericfVar		*result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_FLOAT8(0);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));

	result->value = val > 0 ? ((int64)(val * ScaleMultipler[6] + 0.5))
							: ((int64)(val * ScaleMultipler[6] - 0.5));
	
	result->scale = 6;
	PG_RETURN_NUMERICF(result);
}

Datum
numericf_float8(PG_FUNCTION_ARGS)
{
	NumericfVar	*val;
	float8  	result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_NUMERICF(0);
	result = val->value * 1.0 / ScaleMultipler[val->scale];

	PG_RETURN_FLOAT8(result);
}

Datum
float4_numericf(PG_FUNCTION_ARGS)
{
	float4		val;
	NumericfVar		*result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_FLOAT4(0);
	result = (NumericfVar *) palloc(sizeof(NumericfVar));

	result->value = val > 0 ? ((int64)(val * ScaleMultipler[6] + 0.5))
							: ((int64)(val * ScaleMultipler[6] - 0.5));
	
	result->scale = 6;
	PG_RETURN_NUMERICF(result);
}

Datum
numericf_float4(PG_FUNCTION_ARGS)
{
	NumericfVar	*val;
	float4  result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_NUMERICF(0);
	result = val->value * 1.0 / ScaleMultipler[val->scale];
	PG_RETURN_FLOAT4(result);
}

Datum
numericf_numeric(PG_FUNCTION_ARGS)
{
	NumericfVar	*val;
	NumericVar	 value;
	Numeric		 res;
	const char	*cp;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_NUMERICF(0);
	cp = (const char  *) bi64_out(val->value, val->scale);
	cp = set_var_from_str(cp, cp, &value);
	res = make_result(&value);
	free_var(&value);

	PG_RETURN_NUMERIC(res);
}

static NumericdVar*
numericf_numericd_internal(NumericfVar *val)
{
	NumericdVar	*result;

	elog(ERROR, "Data type numericf hash been deprecated.");

	result = (NumericdVar *) palloc(sizeof(NumericdVar));
	result->value = val->value;
	result->scale = val->scale;

	return result;
}

Datum
numericf_numericd(PG_FUNCTION_ARGS)
{
	NumericfVar	*val;

	elog(ERROR, "Data type numericf hash been deprecated.");

	val = PG_GETARG_NUMERICF(0);

	PG_RETURN_NUMERICD(numericf_numericd_internal(val));
}

static NumericdVar *
check_numericf_array(ArrayType *transarray, const char *caller, int n)
{
	/*
	 * We expect the input to be an N-element float array; verify that. We
	 * don't need to use deconstruct_array() since the array data is just
	 * going to look like a C array of N float8 values.
	 */
	if (ARR_NDIM(transarray) != 1 ||
		ARR_DIMS(transarray)[0] != n ||
		ARR_HASNULL(transarray) ||
		ARR_ELEMTYPE(transarray) != NUMERICDOID)
		elog(ERROR, "%s: expected %d-element numericf array", caller, n);

	return (NumericdVar *) ARR_DATA_PTR(transarray);
}

/*
 * Generic transition function for numericd aggregates that don't require sumX2.
 */
Datum
numericf_avg_accum(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray;
	NumericfVar *newval;
	NumericdVar *transvalues;
	NumericdVar	N,
				sumX;
	int128		res = 0;

	elog(ERROR, "Data type numericf hash been deprecated.");

	transarray = PG_GETARG_ARRAYTYPE_P(0);
	newval = PG_GETARG_NUMERICF(1);
	transvalues = check_numericf_array(transarray, "numericf_avg_accum", 3);

	N = transvalues[0];
	sumX = transvalues[1];
	N.value = N.value + 1;

	pg_add_s128_overflow(sumX.value, newval->value, &res);

	NUMERICD_INT128_ADD(sumX.value, newval->value, &res);
	sumX.value = res;
	sumX.scale = newval->scale;

	/*
	 * If we're invoked as an aggregate, we can cheat and modify our first
	 * parameter in-place to reduce palloc overhead. Otherwise we construct a
	 * new array with the updated transition data and return it.
	 */
	if (AggCheckCallContext(fcinfo, NULL))
	{
		transvalues[0] = N;
		transvalues[1] = sumX;
		PG_RETURN_ARRAYTYPE_P(transarray);
	}
	else
	{
		Datum		transdatums[3];
		ArrayType  *result;

		transdatums[0] = PointerGetDatum(&N);
		transdatums[1] = PointerGetDatum(&sumX);
	
		result = construct_array(transdatums, 2,
								 NUMERICDOID,
								 sizeof(NumericdVar), false, 'd');

		PG_RETURN_ARRAYTYPE_P(result);
	}
}

/*
 * numericf_larger() -
 *
 *	Return the larger of two numbers
 */
Datum
numericf_larger(PG_FUNCTION_ARGS)
{
	NumericfVar *arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		if (arg1->value >= arg2->value)
		{
			PG_RETURN_NUMERICF(arg1);
		}
		else
		{
			PG_RETURN_NUMERICF(arg2);
		}
	}
	else if (arg1->scale > arg2->scale)
	{
		if (arg1->value >= (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]))
		{
			PG_RETURN_NUMERICF(arg1);
		}
		else
		{
			PG_RETURN_NUMERICF(arg2);
		}
	}
	else
	{
		if ((int64)(arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) >= arg2->value)
		{
			PG_RETURN_NUMERICF(arg1);
		}
		else
		{
			PG_RETURN_NUMERICF(arg2);
		}
	}
}

/*
 * numericfsmaller() -
 *
 *	Return the smaller of two numbers
 */
Datum
numericf_smaller(PG_FUNCTION_ARGS)
{
	NumericfVar *arg1;
	NumericfVar	*arg2;

	elog(ERROR, "Data type numericf hash been deprecated.");

	arg1 = PG_GETARG_NUMERICF(0);
	arg2 = PG_GETARG_NUMERICF(1);

	if (arg1->scale == arg2->scale)
	{
		if (arg1->value <= arg2->value)
		{
			PG_RETURN_NUMERICF(arg1);
		}
		else
		{
			PG_RETURN_NUMERICF(arg2);
		}
	}
	else if (arg1->scale > arg2->scale)
	{
		if (arg1->value <= (int64)(arg2->value * ScaleMultipler[arg1->scale - arg2->scale]))
		{
			PG_RETURN_NUMERICF(arg1);
		}
		else
		{
			PG_RETURN_NUMERICF(arg2);
		}
	}
	else
	{
		if ((int64)(arg1->value * ScaleMultipler[arg2->scale - arg1->scale]) <= arg2->value)
		{
			PG_RETURN_NUMERICF(arg1);
		}
		else
		{
			PG_RETURN_NUMERICF(arg2);
		}
	}
}
