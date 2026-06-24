/*-------------------------------------------------------------------------
 *
 *	  GB18030 <--> UTF8
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "../../Unicode/gb18030_2022_to_utf8.map"
#include "../../Unicode/utf8_to_gb18030_2022.map"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gb18030_2022_to_utf8);
PG_FUNCTION_INFO_V1(utf8_to_gb18030_2022);

/*
 * Convert 4-byte GB18030 characters to and from a linear code space
 *
 * The first and third bytes can range from 0x81 to 0xfe (126 values),
 * while the second and fourth bytes can range from 0x30 to 0x39 (10 values).
 */
static inline uint32
gb_linear(uint32 gb)
{
	uint32		b0 = (gb & 0xff000000) >> 24;
	uint32		b1 = (gb & 0x00ff0000) >> 16;
	uint32		b2 = (gb & 0x0000ff00) >> 8;
	uint32		b3 = (gb & 0x000000ff);

	return b0 * 12600 + b1 * 1260 + b2 * 10 + b3 -
	       (0x81 * 12600 + 0x30 * 1260 + 0x81 * 10 + 0x30);
}

static inline uint32
gb_unlinear(uint32 lin)
{
	uint32		r0 = 0x81 + lin / 12600;
	uint32		r1 = 0x30 + (lin / 1260) % 10;
	uint32		r2 = 0x81 + (lin / 10) % 126;
	uint32		r3 = 0x30 + lin % 10;

	return (r0 << 24) | (r1 << 16) | (r2 << 8) | r3;
}

/*
 * Convert word-formatted UTF8 to and from Unicode code points
 *
 * Probably this should be somewhere else ...
 */
static inline uint32
unicode_to_utf8word(uint32 c)
{
	uint32		word;

	if (c <= 0x7F)
	{
		word = c;
	}
	else if (c <= 0x7FF)
	{
		word = (0xC0 | ((c >> 6) & 0x1F)) << 8;
		word |= 0x80 | (c & 0x3F);
	}
	else if (c <= 0xFFFF)
	{
		word = (0xE0 | ((c >> 12) & 0x0F)) << 16;
		word |= (0x80 | ((c >> 6) & 0x3F)) << 8;
		word |= 0x80 | (c & 0x3F);
	}
	else
	{
		word = (0xF0 | ((c >> 18) & 0x07)) << 24;
		word |= (0x80 | ((c >> 12) & 0x3F)) << 16;
		word |= (0x80 | ((c >> 6) & 0x3F)) << 8;
		word |= 0x80 | (c & 0x3F);
	}

	return word;
}

static inline uint32
utf8word_to_unicode(uint32 c)
{
	uint32		ucs;

	if (c <= 0x7F)
	{
		ucs = c;
	}
	else if (c <= 0xFFFF)
	{
		ucs = ((c >> 8) & 0x1F) << 6;
		ucs |= c & 0x3F;
	}
	else if (c <= 0xFFFFFF)
	{
		ucs = ((c >> 16) & 0x0F) << 12;
		ucs |= ((c >> 8) & 0x3F) << 6;
		ucs |= c & 0x3F;
	}
	else
	{
		ucs = ((c >> 24) & 0x07) << 18;
		ucs |= ((c >> 16) & 0x3F) << 12;
		ucs |= ((c >> 8) & 0x3F) << 6;
		ucs |= c & 0x3F;
	}

	return ucs;
}

/*
 * Perform mapping of GB18030 ranges to UTF8
 * If in the supplementary plane，calculate it by algorithm.
 * All are ranges of 4-byte GB18030 codes.
 */
static uint32
conv_18030_2022_to_utf8(uint32 code)
{
#define conv18030_2022(minunicode, mincode, maxcode) \
	if (code >= mincode && code <= maxcode) \
		return unicode_to_utf8word(gb_linear(code) - gb_linear(mincode) + minunicode)

	conv18030_2022(0x10000, 0x90308130, 0xE3329A35);
	/* No mapping exists */
	return 0;
}

/*
 * Perform mapping of UTF8 ranges to GB18030
 */
static uint32
conv_utf8_to_18030_2022(uint32 code)
{
	uint32		ucs = utf8word_to_unicode(code);

#define convutf8(minunicode, maxunicode, mincode) \
	if (ucs >= minunicode && ucs <= maxunicode) \
		return gb_unlinear(ucs - minunicode + gb_linear(mincode))

	convutf8(0x10000, 0x10FFFF, 0x90308130);
	/* No mapping exists */
	return 0;
}

/* ----------
 * conv_proc(
 *		INTEGER,	-- source encoding id
 *		INTEGER,	-- destination encoding id
 *		CSTRING,	-- source string (null terminated C string)
 *		CSTRING,	-- destination string (null terminated C string)
 *		INTEGER		-- source string length
 * ) returns VOID;
 * ----------
 */
Datum
gb18030_2022_to_utf8(PG_FUNCTION_ARGS)
{
	unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
	unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
	int			len = PG_GETARG_INT32(4);

	CHECK_ENCODING_CONVERSION_ARGS(PG_GB18030_2022, PG_UTF8);

	LocalToUtf(src, len, dest,
	           &gb18030_2022_to_unicode_tree,
	           NULL, 0,
	           conv_18030_2022_to_utf8,
	           PG_GB18030);

	PG_RETURN_VOID();
}

Datum
utf8_to_gb18030_2022(PG_FUNCTION_ARGS)
{
	unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
	unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
	int			len = PG_GETARG_INT32(4);

	CHECK_ENCODING_CONVERSION_ARGS(PG_UTF8, PG_GB18030_2022);

	UtfToLocal(src, len, dest,
	           &gb18030_2022_from_unicode_tree,
	           NULL, 0,
	           conv_utf8_to_18030_2022,
	           PG_GB18030);

	PG_RETURN_VOID();
}
