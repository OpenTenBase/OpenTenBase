/*-------------------------------------------------------------------------
 *
 * numutils.c
 *	  utility functions for I/O of built-in numeric types.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/numutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <limits.h>
#include <ctype.h>

#include "common/int.h"
#include "utils/builtins.h"

/*
 * pg_atoi: convert string to integer
 *
 * allows any number of leading or trailing whitespace characters.
 *
 * 'size' is the sizeof() the desired integral result (1, 2, or 4 bytes).
 *
 * c, if not 0, is a terminator character that may appear after the
 * integer (plus whitespace).  If 0, the string must end after the integer.
 *
 * Unlike plain atoi(), this will throw ereport() upon bad input format or
 * overflow.
 */
int32
pg_atoi(const char *s, int size, int c)
{
	long		l;
	char	   *badp;

	/*
	 * Some versions of strtol treat the empty string as an error, but some
	 * seem not to.  Make an explicit test to be sure we catch it.
	 */
	if (s == NULL)
		elog(ERROR, "NULL pointer");
	if (*s == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for %s: \"%s\"",
						"integer", s)));

	errno = 0;
	l = strtol(s, &badp, 10);

	/* We made no progress parsing the string, so bail out */
	if (s == badp)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for %s: \"%s\"",
						"integer", s)));

	switch (size)
	{
		case sizeof(int32):
			if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
			/* won't get ERANGE on these with 64-bit longs... */
				|| l < INT_MIN || l > INT_MAX
#endif
				)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value \"%s\" is out of range for type %s", s,
								"integer")));
			break;
		case sizeof(int16):
			if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value \"%s\" is out of range for type %s", s,
								"smallint")));
			break;
		case sizeof(int8):
			if (errno == ERANGE || l < SCHAR_MIN || l > SCHAR_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value \"%s\" is out of range for 8-bit integer", s)));
			break;
		default:
			elog(ERROR, "unsupported result size: %d", size);
	}

	/*
	 * Skip any trailing whitespace; if anything but whitespace remains before
	 * the terminating character, bail out
	 */
	while (*badp && *badp != c && isspace((unsigned char) *badp))
		badp++;

	if (*badp && *badp != c)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for %s: \"%s\"",
						"integer", s)));

	return (int32) l;
}

/*
 * Convert input string to a signed 16 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters. Will throw
 * ereport() upon bad input format or overflow.
 *
 * NB: Accumulate input as a negative number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * positive number.
 */
int16
pg_strtoint16(const char *s)
{
	const char *ptr = s;
	int16		tmp = 0;
	bool		neg = false;

	/* skip leading spaces */
	while (likely(*ptr) && isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		goto invalid_syntax;

	/* process digits */
	while (*ptr && isdigit((unsigned char) *ptr))
	{
		int8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_s16_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_sub_s16_overflow(tmp, digit, &tmp)))
			goto out_of_range;
	}

	/* allow trailing whitespace, but not other trailing chars */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (!neg)
	{
		/* could fail if input is most negative number */
		if (unlikely(tmp == PG_INT16_MIN))
			goto out_of_range;
		tmp = -tmp;
	}

	return tmp;

out_of_range:
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "smallint")));

invalid_syntax:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"smallint", s)));

	return 0;					/* keep compiler quiet */
}

/*
 * Convert input string to a signed 32 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters. Will throw
 * ereport() upon bad input format or overflow.
 *
 * NB: Accumulate input as a negative number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * positive number.
 */
int32
pg_strtoint32(const char *s)
{
	const char *ptr = s;
	int32		tmp = 0;
	bool		neg = false;

	/* skip leading spaces */
	while (likely(*ptr) && isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		goto invalid_syntax;

	/* process digits */
	while (*ptr && isdigit((unsigned char) *ptr))
	{
		int8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_s32_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_sub_s32_overflow(tmp, digit, &tmp)))
			goto out_of_range;
	}

	/* allow trailing whitespace, but not other trailing chars */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (!neg)
	{
		/* could fail if input is most negative number */
		if (unlikely(tmp == PG_INT32_MIN))
			goto out_of_range;
		tmp = -tmp;
	}

	return tmp;

out_of_range:
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "integer")));

invalid_syntax:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"integer", s)));

	return 0;					/* keep compiler quiet */
}

/*
 * Convert input string to a signed 64 bit integer.
 *
 * Allows any number of leading or trailing whitespace characters. Will throw
 * ereport() upon bad input format or overflow.
 *
 * NB: Accumulate input as a negative number, to deal with two's complement
 * representation of the most negative number, which can't be represented as a
 * positive number.
 */
int64
pg_strtoint64(const char *s)
{
	const char *ptr = s;
	int64		tmp = 0;
	bool		neg = false;

	/*
	 * Do our own scan, rather than relying on sscanf which might be broken
	 * for long long.
	 *
	 * As INT64_MIN can't be stored as a positive 64 bit integer, accumulate
	 * value as a negative number.
	 */

	/* skip leading spaces */
	while (*ptr && isspace((unsigned char) *ptr))
		ptr++;

	/* handle sign */
	if (*ptr == '-')
	{
		ptr++;
		neg = true;
	}
	else if (*ptr == '+')
		ptr++;

	/* require at least one digit */
	if (unlikely(!isdigit((unsigned char) *ptr)))
		goto invalid_syntax;

	/* process digits */
	while (*ptr && isdigit((unsigned char) *ptr))
	{
		int8		digit = (*ptr++ - '0');

		if (unlikely(pg_mul_s64_overflow(tmp, 10, &tmp)) ||
			unlikely(pg_sub_s64_overflow(tmp, digit, &tmp)))
			goto out_of_range;
	}

	/* allow trailing whitespace, but not other trailing chars */
	while (*ptr != '\0' && isspace((unsigned char) *ptr))
		ptr++;

	if (unlikely(*ptr != '\0'))
		goto invalid_syntax;

	if (!neg)
	{
		/* could fail if input is most negative number */
		if (unlikely(tmp == PG_INT64_MIN))
			goto out_of_range;
		tmp = -tmp;
	}

	return tmp;

out_of_range:
	ereport(ERROR,
			(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
			 errmsg("value \"%s\" is out of range for type %s",
					s, "bigint")));

invalid_syntax:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("invalid input syntax for type %s: \"%s\"",
					"bigint", s)));

	return 0;					/* keep compiler quiet */
}

/*
 * pg_itoa: converts a signed 16-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 7 bytes, counting a leading sign and trailing NUL).
 *
 * It doesn't seem worth implementing this separately.
 */
void
pg_itoa(int16 i, char *a)
{
	pg_ltoa((int32) i, a);
}

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
void
pg_ltoa(int32 value, char *a)
{
	char	   *start = a;
	bool		neg = false;

	/*
	 * Avoid problems with the most negative integer not being representable
	 * as a positive integer.
	 */
	if (value == PG_INT32_MIN)
	{
		memcpy(a, "-2147483648", 12);
		return;
	}
	else if (value < 0)
	{
		value = -value;
		neg = true;
	}

	/* Compute the result string backwards. */
	do
	{
		int32		remainder;
		int32		oldval = value;

		value /= 10;
		remainder = oldval - value * 10;
		*a++ = '0' + remainder;
	} while (value != 0);

	if (neg)
		*a++ = '-';

	/* Add trailing NUL byte, and back up 'a' to the last character. */
	*a-- = '\0';

	/* Reverse string. */
	while (start < a)
	{
		char		swap = *start;

		*start++ = *a;
		*a-- = swap;
	}
}

/*
 * pg_lltoa: convert a signed 64-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT8LEN+1 bytes, counting a leading sign and trailing NUL).
 */
void
pg_lltoa(int64 value, char *a)
{
	char	   *start = a;
	bool		neg = false;

	/*
	 * Avoid problems with the most negative integer not being representable
	 * as a positive integer.
	 */
	if (value == PG_INT64_MIN)
	{
		memcpy(a, "-9223372036854775808", 21);
		return;
	}
	else if (value < 0)
	{
		value = -value;
		neg = true;
	}

	/* Compute the result string backwards. */
	do
	{
		int64		remainder;
		int64		oldval = value;

		value /= 10;
		remainder = oldval - value * 10;
		*a++ = '0' + remainder;
	} while (value != 0);

	if (neg)
		*a++ = '-';

	/* Add trailing NUL byte, and back up 'a' to the last character. */
	*a-- = '\0';

	/* Reverse string. */
	while (start < a)
	{
		char		swap = *start;

		*start++ = *a;
		*a-- = swap;
	}
}


/*
 * pg_ltostr_zeropad
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *		'minwidth' specifies the minimum width of the result; any extra space
 *		is filled up by prefixing the number with zeros.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_ltostr_zeropad(str, hours, 2);
 *	*str++ = ':';
 *	str = pg_ltostr_zeropad(str, mins, 2);
 *	*str++ = ':';
 *	str = pg_ltostr_zeropad(str, secs, 2);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char *
pg_ltostr_zeropad(char *str, int32 value, int32 minwidth)
{
	char	   *start = str;
	char	   *end = &str[minwidth];
	int32		num = value;

	Assert(minwidth > 0);

	/*
	 * Handle negative numbers in a special way.  We can't just write a '-'
	 * prefix and reverse the sign as that would overflow for INT32_MIN.
	 */
	if (num < 0)
	{
		*start++ = '-';
		minwidth--;

		/*
		 * Build the number starting at the last digit.  Here remainder will
		 * be a negative number, so we must reverse the sign before adding '0'
		 * in order to get the correct ASCII digit.
		 */
		while (minwidth--)
		{
			int32		oldval = num;
			int32		remainder;

			num /= 10;
			remainder = oldval - num * 10;
			start[minwidth] = '0' - remainder;
		}
	}
	else
	{
		/* Build the number starting at the last digit */
		while (minwidth--)
		{
			int32		oldval = num;
			int32		remainder;

			num /= 10;
			remainder = oldval - num * 10;
			start[minwidth] = '0' + remainder;
		}
	}

	/*
	 * If minwidth was not high enough to fit the number then num won't have
	 * been divided down to zero.  We punt the problem to pg_ltostr(), which
	 * will generate a correct answer in the minimum valid width.
	 */
	if (num != 0)
		return pg_ltostr(str, value);

	/* Otherwise, return last output character + 1 */
	return end;
}

/*
 * pg_ltostr
 *		Converts 'value' into a decimal string representation stored at 'str'.
 *
 * Returns the ending address of the string result (the last character written
 * plus 1).  Note that no NUL terminator is written.
 *
 * The intended use-case for this function is to build strings that contain
 * multiple individual numbers, for example:
 *
 *	str = pg_ltostr(str, a);
 *	*str++ = ' ';
 *	str = pg_ltostr(str, b);
 *	*str = '\0';
 *
 * Note: Caller must ensure that 'str' points to enough memory to hold the
 * result.
 */
char *
pg_ltostr(char *str, int32 value)
{
	char	   *start;
	char	   *end;

	/*
	 * Handle negative numbers in a special way.  We can't just write a '-'
	 * prefix and reverse the sign as that would overflow for INT32_MIN.
	 */
	if (value < 0)
	{
		*str++ = '-';

		/* Mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int32		oldval = value;
			int32		remainder;

			value /= 10;
			remainder = oldval - value * 10;
			/* As above, we expect remainder to be negative. */
			*str++ = '0' - remainder;
		} while (value != 0);
	}
	else
	{
		/* Mark the position we must reverse the string from. */
		start = str;

		/* Compute the result string backwards. */
		do
		{
			int32		oldval = value;
			int32		remainder;

			value /= 10;
			remainder = oldval - value * 10;
			*str++ = '0' + remainder;
		} while (value != 0);
	}

	/* Remember the end+1 and back up 'str' to the last character. */
	end = str--;

	/* Reverse string. */
	while (start < str)
	{
		char		swap = *start;

		*start++ = *str;
		*str-- = swap;
	}

	return end;
}

/*
 * pg_strtouint64
 *		Converts 'str' into an unsigned 64-bit integer.
 *
 * This has the identical API to strtoul(3), except that it will handle
 * 64-bit ints even where "long" is narrower than that.
 *
 * For the moment it seems sufficient to assume that the platform has
 * such a function somewhere; let's not roll our own.
 */
uint64
pg_strtouint64(const char *str, char **endptr, int base)
{
#ifdef _MSC_VER					/* MSVC only */
	return _strtoui64(str, endptr, base);
#elif defined(HAVE_STRTOULL) && SIZEOF_LONG < 8
	return strtoull(str, endptr, base);
#else
	return strtoul(str, endptr, base);
#endif
}

/*
 * Convert a signed 128-bit integer to its string representation.
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least MAXINT16LEN + 1 bytes, counting a leading sign and a trailing NUL).
 */
void 
pg_i128toa(int128 value, char *a, int length)
{
	char swap;
	char *start = a;
	bool neg = false;

	if (a == NULL)
		return;

	/*
	 * avoid problems with the most negative integer not being representable
	 * as a positive integer
	 */
	if (value == (PG_INT128_MIN))
	{
		strncpy(a, int128_min_str, int128_min_str_length);
		a[int128_min_str_length] = '\0';
		return;
	}
	else if (value < 0)
	{
		value = -value;
		neg = true;
	}

	/* compute the result string backwards */
	do {
		int128 remainder;
		int128 oldval = value;

		value /= 10;
		remainder = oldval - value * 10;
		*a++ = '0' + remainder;
	} while (value != 0);

	if (neg)
		*a++ = '-';

	/* add trailing NUL byte, and back up 'a' to the last character. */
	*a-- = '\0';

	/* reverse strings */
	while (start < a)
	{
		swap = *start;

		*start++ = *a;
		*a-- = swap;
	}
}

double
pg_banker_round(double number)
{
	double rounded = round(number);
	double fractional = number - rounded;

	if (fabs(fractional) == 0.5)
	{
		if (fmod(rounded, 2.0) != 0.0)
		{
			rounded = rounded > 0 ? rounded - 1 : rounded + 1;
		}
	}

	return rounded;
}