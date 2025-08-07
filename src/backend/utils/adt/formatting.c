/* -----------------------------------------------------------------------
 * formatting.c
 *
 * src/backend/utils/adt/formatting.c
 *
 *
 *	 Portions Copyright (c) 1999-2017, PostgreSQL Global Development Group
 *
 *
 *	 TO_CHAR(); TO_TIMESTAMP(); TO_DATE(); TO_NUMBER();
 *
 *	 The PostgreSQL routines for a timestamp/int/float/numeric formatting,
 *	 inspired by the opentenbase_ora TO_CHAR() / TO_DATE() / TO_NUMBER() routines.
 *
 *
 *	 Cache & Memory:
 *	Routines use (itself) internal cache for format pictures.
 *
 *	The cache uses a static buffer and is persistent across transactions.  If
 *	the format-picture is bigger than the cache buffer, the parser is called
 *	always.
 *
 *	 NOTE for Number version:
 *	All in this version is implemented as keywords ( => not used
 *	suffixes), because a format picture is for *one* item (number)
 *	only. It not is as a timestamp version, where each keyword (can)
 *	has suffix.
 *
 *	 NOTE for Timestamp routines:
 *	In this module the POSIX 'struct tm' type is *not* used, but rather
 *	PgSQL type, which has tm_mon based on one (*non* zero) and
 *	year *not* based on 1900, but is used full year number.
 *	Module supports AD / BC / AM / PM.
 *
 *	Supported types for to_char():
 *
 *		Timestamp, Numeric, int4, int8, float4, float8
 *
 *	Supported types for reverse conversion:
 *
 *		Timestamp	- to_timestamp()
 *		Date		- to_date()
 *		Numeric		- to_number()
 *
 *
 *	Karel Zak
 *
 * TODO
 *	- better number building (formatting) / parsing, now it isn't
 *		  ideal code
 *	- use Assert()
 *	- add support for abstime
 *	- add support for roman number to standard number conversion
 *	- add support for number spelling
 *	- add support for string to string formatting (we must be better
 *	  than opentenbase_ora :-),
 *		to_char('Hello', 'X X X X X') -> 'H e l l o'
 *
 * -----------------------------------------------------------------------
 */

#ifdef DEBUG_TO_FROM_CHAR
#define DEBUG_elog_output	DEBUG3
#endif

#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <math.h>
#include <float.h>
#include <limits.h>
#include <time.h>

#ifdef _PG_ORCL_
#include "opentenbase_ora/opentenbase_ora.h"
#endif

/*
 * towlower() and friends should be in <wctype.h>, but some pre-C99 systems
 * declare them in <wchar.h>.
 */
#ifdef HAVE_WCHAR_H
#include <wchar.h>
#endif
#ifdef HAVE_WCTYPE_H
#include <wctype.h>
#endif

#ifdef USE_ICU
#include <unicode/ustring.h>
#endif

#include "catalog/pg_collation.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/formatting.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/orcl_datetime_formatting.h"
#include "utils/pg_locale.h"
#include "utils/numeric.h"

/* ----------
 * Routines type
 * ----------
 */
#define DCH_TYPE		1		/* DATE-TIME version	*/
#define NUM_TYPE		2		/* NUMBER version	*/

/* ----------
 * KeyWord Index (ascii from position 32 (' ') to 126 (~))
 * ----------
 */
#define KeyWord_INDEX_SIZE		('~' - ' ')
#define KeyWord_INDEX_FILTER(_c)	((_c) <= ' ' || (_c) >= '~' ? 0 : 1)

/* ----------
 * Maximal length of one node
 * ----------
 */
#define DCH_MAX_ITEM_SIZ	   12	/* max localized day name		*/
#define NUM_MAX_ITEM_SIZ		8	/* roman number (RN has 15 chars)	*/
#define ORA_NUM_MAX_ITEM_SIZ	32	/* TM has 64 chars */
#define ORA_NUM_WIDTH 10

/* ----------
 * More is in float.c
 * ----------
 */
#define MAXFLOATWIDTH	60
#define MAXDOUBLEWIDTH	500


/* ----------
 * Format parser structs
 * ----------
 */
typedef struct
{
	char	   *name;			/* suffix string		*/
	int			len,			/* suffix length		*/
				id,				/* used in node->suffix */
				type;			/* prefix / postfix			*/
} KeySuffix;

/* ----------
 * FromCharDateMode
 * ----------
 *
 * This value is used to nominate one of several distinct (and mutually
 * exclusive) date conventions that a keyword can belong to.
 */
typedef enum
{
	FROM_CHAR_DATE_NONE = 0,	/* Value does not affect date mode. */
	FROM_CHAR_DATE_GREGORIAN,	/* Gregorian (day, month, year) style date */
	FROM_CHAR_DATE_ISOWEEK		/* ISO 8601 week date */
} FromCharDateMode;

typedef struct
{
	const char *name;
	int			len;
	int			id;
	bool		is_digit;
	FromCharDateMode date_mode;
} KeyWord;

typedef struct FormatNode
{
	int			type;			/* node type			*/
	const KeyWord *key;			/* if node type is KEYWORD	*/
	char		character[MAX_MULTIBYTE_CHAR_LEN + 1];	/* if node type is CHAR */
	int			suffix;			/* keyword suffix		*/
	bool 			valid;
} FormatNode;

#define NODE_TYPE_END		1
#define NODE_TYPE_ACTION	2
#define NODE_TYPE_CHAR		3
#ifdef _PG_ORCL_
#define NODE_TYPE_INVALID_CHAR     4
#endif

#define SUFFTYPE_PREFIX		1
#define SUFFTYPE_POSTFIX	2

#define CLOCK_24_HOUR		0
#define CLOCK_12_HOUR		1


/* ----------
 * Full months
 * ----------
 */
static const char *const months_full[] = {
	"January", "February", "March", "April", "May", "June", "July",
	"August", "September", "October", "November", "December", NULL
};

static const char *const days_short[] = {
	"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", NULL
};

static const char *const ora_char_fmt[] = {
	"FM", "MI", "PL", "PR", "RN", "SG", "SP", "TH", "TM" 
};

/* ----------
 * AD / BC
 * ----------
 *	There is no 0 AD.  Years go from 1 BC to 1 AD, so we make it
 *	positive and map year == -1 to year zero, and shift all negative
 *	years up one.  For interval years, we just return the year.
 */
#define ADJUST_YEAR(year, is_interval)	((is_interval) ? (year) : ((year) <= 0 ? -((year) - 1) : (year)))

#define A_D_STR		"A.D."
#define a_d_STR		"a.d."
#define AD_STR		"AD"
#define ad_STR		"ad"

#define B_C_STR		"B.C."
#define b_c_STR		"b.c."
#define BC_STR		"BC"
#define bc_STR		"bc"

/*
 * AD / BC strings for seq_search.
 *
 * These are given in two variants, a long form with periods and a standard
 * form without.
 *
 * The array is laid out such that matches for AD have an even index, and
 * matches for BC have an odd index.  So the boolean value for BC is given by
 * taking the array index of the match, modulo 2.
 */
static const char *const adbc_strings[] = {ad_STR, bc_STR, AD_STR, BC_STR, NULL};
static const char *const adbc_strings_long[] = {a_d_STR, b_c_STR, A_D_STR, B_C_STR, NULL};

/* ----------
 * AM / PM
 * ----------
 */
#define A_M_STR		"A.M."
#define a_m_STR		"a.m."
#define AM_STR		"AM"
#define am_STR		"am"

#define P_M_STR		"P.M."
#define p_m_STR		"p.m."
#define PM_STR		"PM"
#define pm_STR		"pm"

/*
 * AM / PM strings for seq_search.
 *
 * These are given in two variants, a long form with periods and a standard
 * form without.
 *
 * The array is laid out such that matches for AM have an even index, and
 * matches for PM have an odd index.  So the boolean value for PM is given by
 * taking the array index of the match, modulo 2.
 */
static const char *const ampm_strings[] = {am_STR, pm_STR, AM_STR, PM_STR, NULL};
static const char *const ampm_strings_long[] = {a_m_STR, p_m_STR, A_M_STR, P_M_STR, NULL};

/* ----------
 * Months in roman-numeral
 * (Must be in reverse order for seq_search (in FROM_CHAR), because
 *	'VIII' must have higher precedence than 'V')
 * ----------
 */
static const char *const rm_months_upper[] =
{"XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I", NULL};

static const char *const rm_months_lower[] =
{"xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i", NULL};

/* ----------
 * Roman numbers
 * ----------
 */
static const char *const rm1[] = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", NULL};
static const char *const rm10[] = {"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC", NULL};
static const char *const rm100[] = {"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM", NULL};

/* ----------
 * Ordinal postfixes
 * ----------
 */
static const char *const numTH[] = {"ST", "ND", "RD", "TH", NULL};
static const char *const numth[] = {"st", "nd", "rd", "th", NULL};

/* ----------
 * Flags & Options:
 * ----------
 */
#define ONE_UPPER		1		/* Name */
#define ALL_UPPER		2		/* NAME */
#define ALL_LOWER		3		/* name */

#define FULL_SIZ		0

#define MAX_MONTH_LEN	9
#define MAX_MON_LEN		3
#define MAX_DAY_LEN		9
#define MAX_DY_LEN		3
#define MAX_RM_LEN		4

#define TH_UPPER		1
#define TH_LOWER		2

/* ----------
 * Number description struct
 * ----------
 */
typedef struct
{
	int			pre,			/* (count) numbers before decimal */
				post,			/* (count) numbers after decimal  */
				lsign,			/* want locales sign		  */
				flag,			/* number parameters		  */
				pre_lsign_num,	/* tmp value for lsign		  */
				multi,			/* multiplier for 'V'		  */
				zero_start,		/* position of first zero	  */
				zero_end,		/* position of last zero	  */
				need_locale;	/* needs it locale		  */
} NUMDesc;

/* ----------
 * Flags for NUMBER version
 * ----------
 */
#define NUM_F_DECIMAL		(1 << 1)
#define NUM_F_LDECIMAL		(1 << 2)
#define NUM_F_ZERO			(1 << 3)
#define NUM_F_BLANK			(1 << 4)
#define NUM_F_FILLMODE		(1 << 5)
#define NUM_F_LSIGN			(1 << 6)
#define NUM_F_BRACKET		(1 << 7)
#define NUM_F_MINUS			(1 << 8)
#define NUM_F_PLUS			(1 << 9)
#define NUM_F_ROMAN			(1 << 10)
#define NUM_F_MULTI			(1 << 11)
#define NUM_F_PLUS_POST		(1 << 12)
#define NUM_F_MINUS_POST	(1 << 13)
#define NUM_F_EEEE			(1 << 14)
#define NUM_F_DOLLAR		(1 << 15)
#define NUM_F_HEX			(1 << 16)
#define NUM_F_TM			(1 << 17)

#define NUM_LSIGN_PRE	(-1)
#define NUM_LSIGN_POST	1
#define NUM_LSIGN_NONE	0

/* ----------
 * Tests
 * ----------
 */
#define IS_DECIMAL(_f)	((_f)->flag & NUM_F_DECIMAL)
#define IS_LDECIMAL(_f) ((_f)->flag & NUM_F_LDECIMAL)
#define IS_ZERO(_f) ((_f)->flag & NUM_F_ZERO)
#define IS_BLANK(_f)	((_f)->flag & NUM_F_BLANK)
#define IS_FILLMODE(_f) ((_f)->flag & NUM_F_FILLMODE)
#define IS_BRACKET(_f)	((_f)->flag & NUM_F_BRACKET)
#define IS_MINUS(_f)	((_f)->flag & NUM_F_MINUS)
#define IS_LSIGN(_f)	((_f)->flag & NUM_F_LSIGN)
#define IS_PLUS(_f) ((_f)->flag & NUM_F_PLUS)
#define IS_ROMAN(_f)	((_f)->flag & NUM_F_ROMAN)
#define IS_MULTI(_f)	((_f)->flag & NUM_F_MULTI)
#define IS_EEEE(_f)		((_f)->flag & NUM_F_EEEE)
#define IS_DOLLAR(_f)	((_f)->flag & NUM_F_DOLLAR)
#define IS_HEX(_f)		((_f)->flag & NUM_F_HEX)
#define IS_TM(_f)		((_f)->flag & NUM_F_TM)

/* ----------
 * Format picture cache
 *
 * We will cache datetime format pictures up to DCH_CACHE_SIZE bytes long;
 * likewise number format pictures up to NUM_CACHE_SIZE bytes long.
 *
 * For simplicity, the cache entries are fixed-size, so they allow for the
 * worst case of a FormatNode for each byte in the picture string.
 *
 * The max number of entries in the caches is DCH_CACHE_ENTRIES
 * resp. NUM_CACHE_ENTRIES.
 * ----------
 */
#define NUM_CACHE_SIZE		64
#define NUM_CACHE_ENTRIES	20
#define DCH_CACHE_SIZE		128
#define DCH_CACHE_ENTRIES	20

typedef struct
{
	FormatNode	format[DCH_CACHE_SIZE + 1];
	char		str[DCH_CACHE_SIZE + 1];
	bool		valid;
	int			age;
} DCHCacheEntry;

typedef struct
{
	FormatNode	format[NUM_CACHE_SIZE + 1];
	char		str[NUM_CACHE_SIZE + 1];
	bool		valid;
	int			age;
	NUMDesc		Num;
} NUMCacheEntry;

/* global cache for date/time format pictures */
static DCHCacheEntry DCHCache[DCH_CACHE_ENTRIES];
static int	n_DCHCache = 0;		/* current number of entries */
static int	DCHCounter = 0;		/* aging-event counter */

/* global cache for number format pictures */
static NUMCacheEntry NUMCache[NUM_CACHE_ENTRIES];
static int	n_NUMCache = 0;		/* current number of entries */
static int	NUMCounter = 0;		/* aging-event counter */

/* ----------
 * For char->date/time conversion
 * ----------
 */
typedef struct
{
	FromCharDateMode mode;
	int			hh,
				pm,
				mi,
				ss,
				ssss,
				d,				/* stored as 1-7, Sunday = 1, 0 means missing */
				dd,
				ddd,
				mm,
				ms,
				year,
				bc,
				ww,
				w,
				cc,
				j,
				us,
				yysz,			/* is it YY or YYYY ? */
				clock,			/* 12 or 24 hour clock? */
				tzsign,			/* +1, -1 or 0 if timezone info is absent */
				tzh,
				tzm;
} TmFromChar;

#define ZERO_tmfc(_X) memset(_X, 0, sizeof(TmFromChar))

/* ----------
 * Debug
 * ----------
 */
#ifdef DEBUG_TO_FROM_CHAR
#define DEBUG_TMFC(_X) \
		elog(DEBUG_elog_output, "TMFC:\nmode %d\nhh %d\npm %d\nmi %d\nss %d\nssss %d\nd %d\ndd %d\nddd %d\nmm %d\nms: %d\nyear %d\nbc %d\nww %d\nw %d\ncc %d\nj %d\nus: %d\nyysz: %d\nclock: %d", \
			(_X)->mode, (_X)->hh, (_X)->pm, (_X)->mi, (_X)->ss, (_X)->ssss, \
			(_X)->d, (_X)->dd, (_X)->ddd, (_X)->mm, (_X)->ms, (_X)->year, \
			(_X)->bc, (_X)->ww, (_X)->w, (_X)->cc, (_X)->j, (_X)->us, \
			(_X)->yysz, (_X)->clock);
#define DEBUG_TM(_X) \
		elog(DEBUG_elog_output, "TM:\nsec %d\nyear %d\nmin %d\nwday %d\nhour %d\nyday %d\nmday %d\nnisdst %d\nmon %d\n",\
			(_X)->tm_sec, (_X)->tm_year,\
			(_X)->tm_min, (_X)->tm_wday, (_X)->tm_hour, (_X)->tm_yday,\
			(_X)->tm_mday, (_X)->tm_isdst, (_X)->tm_mon)
#else
#define DEBUG_TMFC(_X)
#define DEBUG_TM(_X)
#endif

/* ----------
 * Datetime to char conversion
 * ----------
 */
typedef struct TmToChar
{
	struct pg_tm tm;			/* classic 'tm' struct */
	fsec_t		fsec;			/* fractional seconds */
	const char *tzn;			/* timezone */
} TmToChar;

#define tmtcTm(_X)	(&(_X)->tm)
#define tmtcTzn(_X) ((_X)->tzn)
#define tmtcFsec(_X)	((_X)->fsec)

#define ZERO_tm(_X) \
do {	\
	(_X)->tm_sec  = (_X)->tm_year = (_X)->tm_min = (_X)->tm_wday = \
	(_X)->tm_hour = (_X)->tm_yday = (_X)->tm_isdst = 0; \
	(_X)->tm_mday = (_X)->tm_mon  = 1; \
	(_X)->tm_zone = NULL; \
} while(0)

#define ZERO_tmtc(_X) \
do { \
	ZERO_tm( tmtcTm(_X) ); \
	tmtcFsec(_X) = 0; \
	tmtcTzn(_X) = NULL; \
} while(0)

/*
 *	to_char(time) appears to to_char() as an interval, so this check
 *	is really for interval and time data types.
 */
#define INVALID_FOR_INTERVAL  \
do { \
	if (is_interval) \
		ereport(ERROR, \
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT), \
				 errmsg("invalid format specification for an interval value"), \
				 errhint("Intervals are not tied to specific calendar dates."))); \
} while(0)

/*****************************************************************************
 *			KeyWord definitions
 *****************************************************************************/

/* ----------
 * Suffixes:
 * ----------
 */
#define DCH_S_FM	0x01
#define DCH_S_TH	0x02
#define DCH_S_th	0x04
#define DCH_S_SP	0x08
#define DCH_S_TM	0x10

/* ----------
 * Suffix tests
 * ----------
 */
#define S_THth(_s)	((((_s) & DCH_S_TH) || ((_s) & DCH_S_th)) ? 1 : 0)
#define S_TH(_s)	(((_s) & DCH_S_TH) ? 1 : 0)
#define S_th(_s)	(((_s) & DCH_S_th) ? 1 : 0)
#define S_TH_TYPE(_s)	(((_s) & DCH_S_TH) ? TH_UPPER : TH_LOWER)

/* opentenbase_ora toggles FM behavior, we don't; see docs. */
#define S_FM(_s)	(((_s) & DCH_S_FM) ? 1 : 0)
#define S_SP(_s)	(((_s) & DCH_S_SP) ? 1 : 0)
#define S_TM(_s)	(((_s) & DCH_S_TM) ? 1 : 0)

/* ----------
 * Suffixes definition for DATE-TIME TO/FROM CHAR
 * ----------
 */
#define TM_SUFFIX_LEN	2

static const KeySuffix DCH_suff[] = {
	{"FM", 2, DCH_S_FM, SUFFTYPE_PREFIX},
	{"fm", 2, DCH_S_FM, SUFFTYPE_PREFIX},
	{"TM", TM_SUFFIX_LEN, DCH_S_TM, SUFFTYPE_PREFIX},
	{"tm", 2, DCH_S_TM, SUFFTYPE_PREFIX},
	{"TH", 2, DCH_S_TH, SUFFTYPE_POSTFIX},
	{"th", 2, DCH_S_th, SUFFTYPE_POSTFIX},
	{"SP", 2, DCH_S_SP, SUFFTYPE_POSTFIX},
	/* last */
	{NULL, 0, 0, 0}
};


/* ----------
 * Format-pictures (KeyWord).
 *
 * The KeyWord field; alphabetic sorted, *BUT* strings alike is sorted
 *		  complicated -to-> easy:
 *
 *	(example: "DDD","DD","Day","D" )
 *
 * (this specific sort needs the algorithm for sequential search for strings,
 * which not has exact end; -> How keyword is in "HH12blabla" ? - "HH"
 * or "HH12"? You must first try "HH12", because "HH" is in string, but
 * it is not good.
 *
 * (!)
 *	 - Position for the keyword is similar as position in the enum DCH/NUM_poz.
 * (!)
 *
 * For fast search is used the 'int index[]', index is ascii table from position
 * 32 (' ') to 126 (~), in this index is DCH_ / NUM_ enums for each ASCII
 * position or -1 if char is not used in the KeyWord. Search example for
 * string "MM":
 *	1)	see in index to index['M' - 32],
 *	2)	take keywords position (enum DCH_MI) from index
 *	3)	run sequential search in keywords[] from this position
 *
 * ----------
 */

typedef enum
{
	DCH_A_D,
	DCH_A_M,
	DCH_AD,
	DCH_AM,
	DCH_B_C,
	DCH_BC,
	DCH_CC,
	DCH_DAY,
	DCH_DDD,
	DCH_DD,
	DCH_DY,
	DCH_Day,
	DCH_Dy,
	DCH_D,
	DCH_FF1,						/* opentenbase_ora FF(us) format support */
	DCH_FF2,
	DCH_FF3,
	DCH_FF4,
	DCH_FF5,
	DCH_FF6,
	DCH_FF7,
	DCH_FF8,
	DCH_FF9,
	DCH_FF,
	DCH_FX,						/* global suffix */
	DCH_HH24,
	DCH_HH12,
	DCH_HH,
	DCH_IDDD,
	DCH_ID,
	DCH_IW,
	DCH_IYYY,
	DCH_IYY,
	DCH_IY,
	DCH_I,
	DCH_J,
	DCH_MI,
	DCH_MM,
	DCH_MONTH,
	DCH_MON,
	DCH_MS,
	DCH_Month,
	DCH_Mon,
	DCH_OF,
	DCH_P_M,
	DCH_PM,
	DCH_Q,
	DCH_RM,
	DCH_RR,						/* opentenbase_ora/lightweight_ora rr(year) format support */
	DCH_SSSSS,
	DCH_SSSS,
	DCH_SS,
	DCH_TZH,
	DCH_TZM,
	DCH_TZ,
	DCH_US,
	DCH_WW,
	DCH_W,
	DCH_Y_YYY,
	DCH_YYYY,
	DCH_YYY,
	DCH_YY,
	DCH_Y,
	DCH_a_d,
	DCH_a_m,
	DCH_ad,
	DCH_am,
	DCH_b_c,
	DCH_bc,
	DCH_cc,
	DCH_day,
	DCH_ddd,
	DCH_dd,
	DCH_dy,
	DCH_d,
	DCH_ff1,						/* opentenbase_ora ff(us) format support */
	DCH_ff2,
	DCH_ff3,
	DCH_ff4,
	DCH_ff5,
	DCH_ff6,
	DCH_ff7,
	DCH_ff8,
	DCH_ff9,
	DCH_ff,
	DCH_fx,
	DCH_hh24,
	DCH_hh12,
	DCH_hh,
	DCH_iddd,
	DCH_id,
	DCH_iw,
	DCH_iyyy,
	DCH_iyy,
	DCH_iy,
	DCH_i,
	DCH_j,
	DCH_mi,
	DCH_mm,
	DCH_month,
	DCH_mon,
	DCH_ms,
	DCH_p_m,
	DCH_pm,
	DCH_q,
	DCH_rm,
	DCH_rr,						/* opentenbase_ora/lightweight_ora rr(year) format support */
	DCH_sssss,
	DCH_ssss,
	DCH_ss,
	DCH_tz,
	DCH_us,
	DCH_ww,
	DCH_w,
	DCH_y_yyy,
	DCH_yyyy,
	DCH_yyy,
	DCH_yy,
	DCH_y,

	/* last */
	_DCH_last_
}			DCH_poz;

typedef enum
{
	NUM_$,
	NUM_COMMA,
	NUM_DEC,
	NUM_0,
	NUM_9,
	NUM_B,
	NUM_C,
	NUM_D,
	NUM_E,
	NUM_FM,
	NUM_G,
	NUM_L,
	NUM_MI,
	NUM_PL,
	NUM_PR,
	NUM_RN,
	NUM_SG,
	NUM_SP,
	NUM_S,
	NUM_TH,
	NUM_TM,
	NUM_TME,
	NUM_U,
	NUM_V,
	NUM_X,
	NUM_b,
	NUM_c,
	NUM_d,
	NUM_e,
	NUM_fm,
	NUM_g,
	NUM_l,
	NUM_mi,
	NUM_pl,
	NUM_pr,
	NUM_rn,
	NUM_sg,
	NUM_sp,
	NUM_s,
	NUM_th,
	NUM_tm,
	NUM_tme,
	NUM_u,
	NUM_v,
	NUM_x,

	/* last */
	_NUM_last_
}			NUM_poz;

/* ----------
 * KeyWords for DATE-TIME version
 * ----------
 */
static const KeyWord DCH_keywords[] = {
/*	name, len, id, is_digit, date_mode */
	{"A.D.", 4, DCH_A_D, FALSE, FROM_CHAR_DATE_NONE},	/* A */
	{"A.M.", 4, DCH_A_M, FALSE, FROM_CHAR_DATE_NONE},
	{"AD", 2, DCH_AD, FALSE, FROM_CHAR_DATE_NONE},
	{"AM", 2, DCH_AM, FALSE, FROM_CHAR_DATE_NONE},
	{"B.C.", 4, DCH_B_C, FALSE, FROM_CHAR_DATE_NONE},	/* B */
	{"BC", 2, DCH_BC, FALSE, FROM_CHAR_DATE_NONE},
	{"CC", 2, DCH_CC, TRUE, FROM_CHAR_DATE_NONE},	/* C */
	{"DAY", 3, DCH_DAY, FALSE, FROM_CHAR_DATE_NONE},	/* D */
	{"DDD", 3, DCH_DDD, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"DD", 2, DCH_DD, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"DY", 2, DCH_DY, FALSE, FROM_CHAR_DATE_NONE},
	{"Day", 3, DCH_Day, FALSE, FROM_CHAR_DATE_NONE},
	{"Dy", 2, DCH_Dy, FALSE, FROM_CHAR_DATE_NONE},
	{"D", 1, DCH_D, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"FF1", 3, DCH_FF1, FALSE, FROM_CHAR_DATE_NONE},	/* opentenbase_ora FF(us) format support */
	{"FF2", 3, DCH_FF2, FALSE, FROM_CHAR_DATE_NONE},
	{"FF3", 3, DCH_FF3, FALSE, FROM_CHAR_DATE_NONE},
	{"FF4", 3, DCH_FF4, FALSE, FROM_CHAR_DATE_NONE},
	{"FF5", 3, DCH_FF5, FALSE, FROM_CHAR_DATE_NONE},
	{"FF6", 3, DCH_FF6, FALSE, FROM_CHAR_DATE_NONE},
	{"FF7", 3, DCH_FF7, FALSE, FROM_CHAR_DATE_NONE},
	{"FF8", 3, DCH_FF8, FALSE, FROM_CHAR_DATE_NONE},
	{"FF9", 3, DCH_FF9, FALSE, FROM_CHAR_DATE_NONE},
	{"FF", 2, DCH_FF, FALSE, FROM_CHAR_DATE_NONE},
	{"FX", 2, DCH_FX, FALSE, FROM_CHAR_DATE_NONE},	/* F */
	{"HH24", 4, DCH_HH24, TRUE, FROM_CHAR_DATE_NONE},	/* H */
	{"HH12", 4, DCH_HH12, TRUE, FROM_CHAR_DATE_NONE},
	{"HH", 2, DCH_HH, TRUE, FROM_CHAR_DATE_NONE},
	{"IDDD", 4, DCH_IDDD, TRUE, FROM_CHAR_DATE_ISOWEEK},	/* I */
	{"ID", 2, DCH_ID, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"IW", 2, DCH_IW, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"IYYY", 4, DCH_IYYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"IYY", 3, DCH_IYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"IY", 2, DCH_IY, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"I", 1, DCH_I, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"J", 1, DCH_J, TRUE, FROM_CHAR_DATE_NONE}, /* J */
	{"MI", 2, DCH_MI, TRUE, FROM_CHAR_DATE_NONE},	/* M */
	{"MM", 2, DCH_MM, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"MONTH", 5, DCH_MONTH, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"MON", 3, DCH_MON, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"MS", 2, DCH_MS, TRUE, FROM_CHAR_DATE_NONE},
	{"Month", 5, DCH_Month, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"Mon", 3, DCH_Mon, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"OF", 2, DCH_OF, FALSE, FROM_CHAR_DATE_NONE},	/* O */
	{"P.M.", 4, DCH_P_M, FALSE, FROM_CHAR_DATE_NONE},	/* P */
	{"PM", 2, DCH_PM, FALSE, FROM_CHAR_DATE_NONE},
	{"Q", 1, DCH_Q, TRUE, FROM_CHAR_DATE_NONE}, /* Q */
	{"RM", 2, DCH_RM, FALSE, FROM_CHAR_DATE_GREGORIAN}, /* R */
	{"RR", 2, DCH_RR, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"SSSSS", 5, DCH_SSSSS, TRUE, FROM_CHAR_DATE_NONE},	/* S */
	{"SSSS", 4, DCH_SSSS, TRUE, FROM_CHAR_DATE_NONE},	/* S */
	{"SS", 2, DCH_SS, TRUE, FROM_CHAR_DATE_NONE},
	{"TZH", 3, DCH_TZH, FALSE, FROM_CHAR_DATE_NONE},    /* T */
	{"TZM", 3, DCH_TZM, TRUE, FROM_CHAR_DATE_NONE},
	{"TZ", 2, DCH_TZ, FALSE, FROM_CHAR_DATE_NONE},
	{"US", 2, DCH_US, TRUE, FROM_CHAR_DATE_NONE},	/* U */
	{"WW", 2, DCH_WW, TRUE, FROM_CHAR_DATE_GREGORIAN},	/* W */
	{"W", 1, DCH_W, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"Y,YYY", 5, DCH_Y_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN},	/* Y */
	{"YYYY", 4, DCH_YYYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"YYY", 3, DCH_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"YY", 2, DCH_YY, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"Y", 1, DCH_Y, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"a.d.", 4, DCH_a_d, FALSE, FROM_CHAR_DATE_NONE},	/* a */
	{"a.m.", 4, DCH_a_m, FALSE, FROM_CHAR_DATE_NONE},
	{"ad", 2, DCH_ad, FALSE, FROM_CHAR_DATE_NONE},
	{"am", 2, DCH_am, FALSE, FROM_CHAR_DATE_NONE},
	{"b.c.", 4, DCH_b_c, FALSE, FROM_CHAR_DATE_NONE},	/* b */
	{"bc", 2, DCH_bc, FALSE, FROM_CHAR_DATE_NONE},
	{"cc", 2, DCH_CC, TRUE, FROM_CHAR_DATE_NONE},	/* c */
	{"day", 3, DCH_day, FALSE, FROM_CHAR_DATE_NONE},	/* d */
	{"ddd", 3, DCH_DDD, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"dd", 2, DCH_DD, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"dy", 2, DCH_dy, FALSE, FROM_CHAR_DATE_NONE},
	{"d", 1, DCH_D, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"ff1", 3, DCH_ff1, FALSE, FROM_CHAR_DATE_NONE},	/* opentenbase_ora ff(us) format support */
	{"ff2", 3, DCH_ff2, FALSE, FROM_CHAR_DATE_NONE},
	{"ff3", 3, DCH_ff3, FALSE, FROM_CHAR_DATE_NONE},
	{"ff4", 3, DCH_ff4, FALSE, FROM_CHAR_DATE_NONE},
	{"ff5", 3, DCH_ff5, FALSE, FROM_CHAR_DATE_NONE},
	{"ff6", 3, DCH_ff6, FALSE, FROM_CHAR_DATE_NONE},
	{"ff7", 3, DCH_ff7, FALSE, FROM_CHAR_DATE_NONE},
	{"ff8", 3, DCH_ff8, FALSE, FROM_CHAR_DATE_NONE},
	{"ff9", 3, DCH_ff9, FALSE, FROM_CHAR_DATE_NONE},
	{"ff", 2, DCH_ff, FALSE, FROM_CHAR_DATE_NONE},
	{"fx", 2, DCH_FX, FALSE, FROM_CHAR_DATE_NONE},	/* f */
	{"hh24", 4, DCH_HH24, TRUE, FROM_CHAR_DATE_NONE},	/* h */
	{"hh12", 4, DCH_HH12, TRUE, FROM_CHAR_DATE_NONE},
	{"hh", 2, DCH_HH, TRUE, FROM_CHAR_DATE_NONE},
	{"iddd", 4, DCH_IDDD, TRUE, FROM_CHAR_DATE_ISOWEEK},	/* i */
	{"id", 2, DCH_ID, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"iw", 2, DCH_IW, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"iyyy", 4, DCH_IYYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"iyy", 3, DCH_IYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"iy", 2, DCH_IY, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"i", 1, DCH_I, TRUE, FROM_CHAR_DATE_ISOWEEK},
	{"j", 1, DCH_J, TRUE, FROM_CHAR_DATE_NONE}, /* j */
	{"mi", 2, DCH_MI, TRUE, FROM_CHAR_DATE_NONE},	/* m */
	{"mm", 2, DCH_MM, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"month", 5, DCH_month, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"mon", 3, DCH_mon, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"ms", 2, DCH_MS, TRUE, FROM_CHAR_DATE_NONE},
	{"p.m.", 4, DCH_p_m, FALSE, FROM_CHAR_DATE_NONE},	/* p */
	{"pm", 2, DCH_pm, FALSE, FROM_CHAR_DATE_NONE},
	{"q", 1, DCH_Q, TRUE, FROM_CHAR_DATE_NONE}, /* q */
	{"rm", 2, DCH_rm, FALSE, FROM_CHAR_DATE_GREGORIAN}, /* r */
	{"rr", 2, DCH_rr, FALSE, FROM_CHAR_DATE_GREGORIAN},
	{"sssss", 5, DCH_SSSSS, TRUE, FROM_CHAR_DATE_NONE},	/* s */
	{"ssss", 4, DCH_SSSS, TRUE, FROM_CHAR_DATE_NONE},	/* s */
	{"ss", 2, DCH_SS, TRUE, FROM_CHAR_DATE_NONE},
	{"tz", 2, DCH_tz, FALSE, FROM_CHAR_DATE_NONE},	/* t */
	{"us", 2, DCH_US, TRUE, FROM_CHAR_DATE_NONE},	/* u */
	{"ww", 2, DCH_WW, TRUE, FROM_CHAR_DATE_GREGORIAN},	/* w */
	{"w", 1, DCH_W, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"y,yyy", 5, DCH_Y_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN},	/* y */
	{"yyyy", 4, DCH_YYYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"yyy", 3, DCH_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"yy", 2, DCH_YY, TRUE, FROM_CHAR_DATE_GREGORIAN},
	{"y", 1, DCH_Y, TRUE, FROM_CHAR_DATE_GREGORIAN},

	/* last */
	{NULL, 0, 0, 0, 0}
};

/* ----------
 * KeyWords for NUMBER version
 *
 * The is_digit and date_mode fields are not relevant here.
 * ----------
 */
static const KeyWord NUM_keywords[] = {
/*	name, len, id			is in Index */
	{"$", 1, NUM_$},			/* $ */
	{",", 1, NUM_COMMA},		/* , */
	{".", 1, NUM_DEC},			/* . */
	{"0", 1, NUM_0},			/* 0 */
	{"9", 1, NUM_9},			/* 9 */
	{"B", 1, NUM_B},			/* B */
	{"C", 1, NUM_C},			/* C */
	{"D", 1, NUM_D},			/* D */
	{"EEEE", 4, NUM_E},			/* E */
	{"E", 1, NUM_TME},
	{"FM", 2, NUM_FM},			/* F */
	{"G", 1, NUM_G},			/* G */
	{"L", 1, NUM_L},			/* L */
	{"MI", 2, NUM_MI},			/* M */
	{"PL", 2, NUM_PL},			/* P */
	{"PR", 2, NUM_PR},
	{"RN", 2, NUM_RN},			/* R */
	{"SG", 2, NUM_SG},			/* S */
	{"SP", 2, NUM_SP},
	{"S", 1, NUM_S},
	{"TH", 2, NUM_TH},			/* T */
	{"TM", 2, NUM_TM},
	{"U", 1, NUM_U},			/* u */
	{"V", 1, NUM_V},			/* V */
	{"X", 1, NUM_X},            /* X */
	{"b", 1, NUM_B},			/* b */
	{"c", 1, NUM_C},			/* c */
	{"d", 1, NUM_D},			/* d */
	{"eeee", 4, NUM_E},			/* e */
	{"e", 1, NUM_TME},
	{"fm", 2, NUM_FM},			/* f */
	{"g", 1, NUM_G},			/* g */
	{"l", 1, NUM_L},			/* l */
	{"mi", 2, NUM_MI},			/* m */
	{"pl", 2, NUM_PL},			/* p */
	{"pr", 2, NUM_PR},
	{"rn", 2, NUM_rn},			/* r */
	{"sg", 2, NUM_SG},			/* s */
	{"sp", 2, NUM_SP},
	{"s", 1, NUM_S},
	{"th", 2, NUM_th},			/* t */
	{"tm", 2, NUM_TM},
	{"u", 1, NUM_U},			/* u */
	{"v", 1, NUM_V},			/* v */
	{"x", 1, NUM_X},            /* x */

	/* last */
	{NULL, 0, 0}
};


/* ----------
 * KeyWords index for DATE-TIME version
 * ----------
 */
static const int DCH_index[KeyWord_INDEX_SIZE] = {
/*
0	1	2	3	4	5	6	7	8	9
*/
	/*---- first 0..31 chars are skipped ----*/

	-1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, DCH_A_D, DCH_B_C, DCH_CC, DCH_DAY, -1,
	DCH_FF1, -1, DCH_HH24, DCH_IDDD, DCH_J, -1, -1, DCH_MI, -1, DCH_OF,
	DCH_P_M, DCH_Q, DCH_RM, DCH_SSSSS, DCH_TZH, DCH_US, -1, DCH_WW, -1, DCH_Y_YYY,
	-1, -1, -1, -1, -1, -1, -1, DCH_a_d, DCH_b_c, DCH_cc,
	DCH_day, -1, DCH_ff1, -1, DCH_hh24, DCH_iddd, DCH_j, -1, -1, DCH_mi,
	-1, -1, DCH_p_m, DCH_q, DCH_rm, DCH_sssss, DCH_tz, DCH_us, -1, DCH_ww,
	-1, DCH_y_yyy, -1, -1, -1, -1

	/*---- chars over 126 are skipped ----*/
};

/* ----------
 * KeyWords index for NUMBER version
 * ----------
 */
static const int NUM_index[KeyWord_INDEX_SIZE] = {
/*
0	1	2	3	4	5	6	7	8	9
*/
	/*---- first 0..31 chars are skipped ----*/

	-1, -1, -1, -1, NUM_$, -1, -1, -1,
	-1, -1, -1, -1, NUM_COMMA, -1, NUM_DEC, -1, NUM_0, -1,
	-1, -1, -1, -1, -1, -1, -1, NUM_9, -1, -1,
	-1, -1, -1, -1, -1, -1, NUM_B, NUM_C, NUM_D, NUM_E,
	NUM_FM, NUM_G, -1, -1, -1, -1, NUM_L, NUM_MI, -1, -1,
	NUM_PL, -1, NUM_RN, NUM_SG, NUM_TH, NUM_U, NUM_V, -1, NUM_X, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, NUM_b, NUM_c,
	NUM_d, NUM_e, NUM_fm, NUM_g, -1, -1, -1, -1, NUM_l, NUM_mi,
	-1, -1, NUM_pl, -1, NUM_rn, NUM_sg, NUM_th, NUM_u, NUM_v, -1,
	NUM_x, -1, -1, -1, -1, -1

	/*---- chars over 126 are skipped ----*/
};

/* ----------
 * Number processor struct
 * ----------
 */
typedef struct NUMProc
{
	bool		is_to_char;
	NUMDesc    *Num;			/* number description		*/

	int			sign,			/* '-' or '+'			*/
				sign_wrote,		/* was sign write		*/
				num_count,		/* number of write digits	*/
				num_in,			/* is inside number		*/
				num_curr,		/* current position in number	*/
				out_pre_spaces, /* spaces before first digit	*/
				dollar_in,		/* to_char - is '$' inside of outstr. */

				read_dec,		/* to_number - was read dec. point	*/
				read_post,		/* to_number - number of dec. digit */
				read_pre;		/* to_number - number non-dec. digit */

	char	   *number,			/* string with number	*/
			   *number_p,		/* pointer to current number position */
			   *inout,			/* in / out buffer	*/
			   *inout_p,		/* pointer to current inout position */
			   *last_relevant,	/* last relevant number after decimal point */

			   *L_negative_sign,	/* Locale */
			   *L_positive_sign,
			   *decimal,
			   *L_thousands_sep,
			   *L_currency_symbol;
} NUMProc;


/* ----------
 * Functions
 * ----------
 */
static const KeyWord *index_seq_search(const char *str, const KeyWord *kw,
				 const int *index);

static const KeySuffix *suff_search(const char *str, const KeySuffix *suf, int type);
static void NUMDesc_prepare(NUMDesc *num, FormatNode *n);
static void parse_format(FormatNode *node, const char *str, const KeyWord *kw,
			 const KeySuffix *suf, const int *index, int ver, NUMDesc *Num);

static void DCH_to_char(FormatNode *node, bool is_interval,
			TmToChar *in, char *out, Oid collid);
static void DCH_from_char(FormatNode *node, char *in, TmFromChar *out);

#ifdef DEBUG_TO_FROM_CHAR
static void dump_index(const KeyWord *k, const int *index);
static void dump_node(FormatNode *node, int max);
#endif

static const char *get_th(char *num, int type);
static char *str_numth(char *dest, char *num, int type);
static int	adjust_partial_year_to_2020(int year);
static int	strspace_len(char *str);
static void from_char_set_mode(TmFromChar *tmfc, const FromCharDateMode mode);
static void from_char_set_int(int *dest, const int value, const FormatNode *node);
static int	from_char_parse_int_len(int *dest, char **src, const int len, FormatNode *node);
static int	from_char_parse_int(int *dest, char **src, FormatNode *node);
static int	seq_search(char *name, const char *const *array, int type, int max, int *len);
static int	from_char_seq_search(int *dest, char **src, const char *const *array, int type, int max, FormatNode *node);
static void do_to_timestamp(text *date_txt, text *fmt,
				struct pg_tm *tm, fsec_t *fsec);
static char *fill_str(char *str, int c, int max);
static FormatNode *NUM_cache(int len, NUMDesc *Num, text *pars_str, bool *shouldFree);
static char *int_to_roman(int number);
static void NUM_prepare_locale(NUMProc *Np);
static char *get_last_relevant_decnum(char *num);
static void NUM_numpart_from_char(NUMProc *Np, int id, int input_len);
static void NUM_numpart_to_char(NUMProc *Np, int id);
static char *NUM_processor(FormatNode *node, NUMDesc *Num, char *inout,
			  char *number, int from_char_input_len, int to_char_out_pre_spaces,
			  int sign, bool is_to_char, Oid collid);
static DCHCacheEntry *DCH_cache_getnew(const char *str);
static DCHCacheEntry *DCH_cache_search(const char *str);
static DCHCacheEntry *DCH_cache_fetch(const char *str);
static NUMCacheEntry *NUM_cache_getnew(const char *str);
static NUMCacheEntry *NUM_cache_search(const char *str);
static NUMCacheEntry *NUM_cache_fetch(const char *str);
static bool check_valid_fmt(char *number, FormatNode *node);
static bool check_cut_zero(char *number, FormatNode *node);

extern Datum numeric_lt_zero(PG_FUNCTION_ARGS);

static bool
is_ora_keyword(const char *str, const KeyWord *k)
{
	int begin, end, mid, cmp;
	int result = -1;

	if (!ORA_MODE || strlen(str) != 2 || k->len != 2)
		return false;

	begin = 0;
	end = sizeof(ora_char_fmt) / sizeof(ora_char_fmt[0]) - 1;
	while (begin <= end)
	{
		mid = begin + ceil((end - begin) / 2);
		cmp = strncasecmp(ora_char_fmt[mid], str, strlen(ora_char_fmt[mid]));
		if (cmp == 0)
		{
			result = mid;
			break;
		}
		else if (cmp < 0)
			begin = mid + 1;
		else
			end = mid - 1;
	}

	if (result == -1)
		return false;

	if (isupper(str[0]) && strncmp(ora_char_fmt[result], k->name, k->len) == 0)
		return true;

	if (islower(str[0]) && strncmp(asc_tolower(ora_char_fmt[result], strlen(ora_char_fmt[result])), k->name, k->len) == 0)
		return true;

	return false;
}

/* ----------
 * Fast sequential search, use index for data selection which
 * go to seq. cycle (it is very fast for unwanted strings)
 * (can't be used binary search in format parsing)
 * ----------
 */
static const KeyWord *
index_seq_search(const char *str, const KeyWord *kw, const int *index)
{
	int			poz;

	if (!KeyWord_INDEX_FILTER(*str))
		return NULL;

	// find keyword begin with *str.
	if ((poz = *(index + (*str - ' '))) > -1)
	{
		const KeyWord *k = kw + poz;

		do
		{
			if (strncmp(str, k->name, k->len) == 0)
				return k;

			if (is_ora_keyword(str, k))
				return k;

			k++;
			if (!k->name)
				return NULL;
		} while (*str == *k->name);
	}
	return NULL;
}

static const KeySuffix *
suff_search(const char *str, const KeySuffix *suf, int type)
{
	const KeySuffix *s;

	for (s = suf; s->name != NULL; s++)
	{
		if (s->type != type)
			continue;

		if (strncmp(str, s->name, s->len) == 0)
			return s;
	}
	return NULL;
}

/* ----------
 * Prepare NUMDesc (number description struct) via FormatNode struct
 * ----------
 */
static void
NUMDesc_prepare(NUMDesc *num, FormatNode *n)
{
	if (n->type != NODE_TYPE_ACTION)
		return;

	if (IS_EEEE(num) && n->key->id != NUM_E)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("\"EEEE\" must be the last pattern used")));

	switch (n->key->id)
	{
		case NUM_9:
			if (IS_BRACKET(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"9\" must be ahead of \"PR\"")));
			if (IS_MULTI(num))
			{
				++num->multi;
				break;
			}
			if (IS_DECIMAL(num))
				++num->post;
			else
				++num->pre;
			break;

		case NUM_0:
			if (IS_BRACKET(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"0\" must be ahead of \"PR\"")));
			/* opentenbase_ora function to_char bug: to_char('1.123', '9V0000') */
			if (ORA_MODE && IS_MULTI(num))
			{
				++num->multi;
				break;
			}
			if (!IS_ZERO(num) && !IS_DECIMAL(num))
			{
				num->flag |= NUM_F_ZERO;
				num->zero_start = num->pre + 1;
			}
			if (!IS_DECIMAL(num))
				++num->pre;
			else
				++num->post;

			num->zero_end = num->pre + num->post;
			break;

		case NUM_B:
			if (num->pre == 0 && num->post == 0 && (!IS_ZERO(num)))
				num->flag |= NUM_F_BLANK;
			break;

		case NUM_D:
			num->flag |= NUM_F_LDECIMAL;
			num->need_locale = TRUE;
			/* FALLTHROUGH */
		case NUM_DEC:
			if (IS_DECIMAL(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple decimal points")));
			if (IS_MULTI(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"V\" and decimal point together")));
			num->flag |= NUM_F_DECIMAL;
			break;

		case NUM_FM:
			num->flag |= NUM_F_FILLMODE;
			break;

		case NUM_S:
			if (IS_LSIGN(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"S\" twice")));
			if (IS_PLUS(num) || IS_MINUS(num) || IS_BRACKET(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together")));
			if (!IS_DECIMAL(num))
			{
				num->lsign = NUM_LSIGN_PRE;
				num->pre_lsign_num = num->pre;
				num->need_locale = TRUE;
				num->flag |= NUM_F_LSIGN;
			}
			else if (num->lsign == NUM_LSIGN_NONE)
			{
				num->lsign = NUM_LSIGN_POST;
				num->need_locale = TRUE;
				num->flag |= NUM_F_LSIGN;
			}
			break;

		case NUM_MI:
			if (IS_LSIGN(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"S\" and \"MI\" together")));
			num->flag |= NUM_F_MINUS;
			if (IS_DECIMAL(num))
				num->flag |= NUM_F_MINUS_POST;
			break;

		case NUM_PL:
			if (IS_LSIGN(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"S\" and \"PL\" together")));
			num->flag |= NUM_F_PLUS;
			if (IS_DECIMAL(num))
				num->flag |= NUM_F_PLUS_POST;
			break;

		case NUM_SG:
			if (IS_LSIGN(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"S\" and \"SG\" together")));
			num->flag |= NUM_F_MINUS;
			num->flag |= NUM_F_PLUS;
			break;

		case NUM_PR:
			if (IS_LSIGN(num) || IS_PLUS(num) || IS_MINUS(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together")));
			num->flag |= NUM_F_BRACKET;
			break;

		case NUM_rn:
		case NUM_RN:
			num->flag |= NUM_F_ROMAN;
			break;

		case NUM_L:
		case NUM_G:
			num->need_locale = TRUE;
			break;

		/* opentenbase_ora funtion bug: the fmt 'U' support */
		case NUM_U:
			if (ORA_MODE)
				num->need_locale = TRUE;
			else
				elog(ERROR, "Invalid number format model");
			break;

		/* opentenbase_ora funtion bug: the fmt 'TM' support */
		case NUM_TM:
			if (ORA_MODE)
				num->flag |= NUM_F_TM;
			else
				elog(ERROR, "Invalid number format model");
			break;

		case NUM_V:
			if (IS_DECIMAL(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"V\" and decimal point together")));
			num->flag |= NUM_F_MULTI;
			break;

		case NUM_E:
			if (IS_EEEE(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use \"EEEE\" twice")));
			if (IS_BLANK(num) || IS_FILLMODE(num) || IS_LSIGN(num) ||
				IS_BRACKET(num) || IS_MINUS(num) || IS_PLUS(num) ||
				IS_ROMAN(num) || IS_MULTI(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"EEEE\" is incompatible with other formats"),
						 errdetail("\"EEEE\" may only be used together with digit and decimal point patterns.")));
			num->flag |= NUM_F_EEEE;
			break;

		case NUM_$: /* only while ORA_MODE is TRUE. */
			if (IS_DOLLAR(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("cannot use \"$\" twice")));
			num->flag |= NUM_F_DOLLAR;
			++num->pre;
			break;
		case NUM_X:
			if (0 != num-> pre && !IS_HEX(num))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("can't use \"x\" with other character")));
			num->flag |= NUM_F_HEX;
			++num->pre; /* count 'xxx' numbers */
			break;
	}
}

/* ----------
 * Format parser, search small keywords and keyword's suffixes, and make
 * format-node tree.
 *
 * for DATE-TIME & NUMBER version
 * ----------
 */
static void
parse_format(FormatNode *node, const char *str, const KeyWord *kw,
			 const KeySuffix *suf, const int *index, int ver, NUMDesc *Num)
{
	const KeySuffix *s;
	FormatNode *n;
	int         suffix;

#ifdef DEBUG_TO_FROM_CHAR
	elog(DEBUG_elog_output, "to_char/number(): run parser");
#endif

	n = node;

	while (*str)
	{
		suffix = 0;

		/*
		 * Prefix
		 */
		if (ver == DCH_TYPE && (s = suff_search(str, suf, SUFFTYPE_PREFIX)) != NULL)
		{
			suffix |= s->id;
			if (s->len)
				str += s->len;
		}

		/*
		 * Keyword
		 * "$" is keyword only when ORA_MODE is TRUE.
		 * 'NUM_X' is only enable when ORA_MODE is TRUE.
		 */
		if (*str && ((n->key = index_seq_search(str, kw, index)) != NULL) &&
			!((strncmp(str, "$", 1) == 0) && (!ORA_MODE)) &&
			!((strncasecmp(str, "x", 1) == 0) && (!ORA_MODE)))
		{
			n->type = NODE_TYPE_ACTION;
			n->suffix = suffix;
			if (n->key->len)
				str += n->key->len;

			/*
			 * NUM version: Prepare global NUMDesc struct
			 */
			if (ver == NUM_TYPE)
				NUMDesc_prepare(Num, n);

			/*
			 * Postfix
			 */
			if (ver == DCH_TYPE && *str &&
				(s = suff_search(str, suf, SUFFTYPE_POSTFIX)) != NULL)
			{
				n->suffix |= s->id;
				if (s->len)
					str += s->len;
			}

			n++;
		}
		else if (*str)
		{
			int chlen;
			/*
			 * Process double-quoted literal string, if any
			 */
			if (*str == '"')
			{
				str++;
				while (*(str))
				{
					if (*str == '"')
					{
						str++;
						break;
					}
					/* backslash quotes the next character, if any */
					if (*str == '\\' && *(str + 1))
						str++;
					chlen = pg_mblen(str);
					n->type = NODE_TYPE_CHAR;
					memcpy(n->character, str, chlen);
					n->character[chlen] = '\0';
					n->key = NULL;
					n->suffix = 0;
					n++;
					str += chlen;
				}
			}
			else
			{
				/*
				 * Outside double-quoted strings, backslash is only special if
				 * it immediately precedes a double quote.
				 */
				if (*str && *str == '\\' && *(str + 1) == '"')
					str++;
				chlen = pg_mblen(str);

				if (ORA_MODE && isalnum(*str))
					n->type = NODE_TYPE_INVALID_CHAR;
				else
					n->type = NODE_TYPE_CHAR;

				memcpy(n->character, str, chlen);
				n->character[chlen] = '\0';
				n->key = NULL;
				n->suffix = 0;
				n++;
				str += chlen;
			}
		}
	}

	n->type = NODE_TYPE_END;
	n->suffix = 0;
}

/* ----------
 * DEBUG: Dump the FormatNode Tree (debug)
 * ----------
 */
#ifdef DEBUG_TO_FROM_CHAR

#define DUMP_THth(_suf) (S_TH(_suf) ? "TH" : (S_th(_suf) ? "th" : " "))
#define DUMP_FM(_suf)	(S_FM(_suf) ? "FM" : " ")

static void
dump_node(FormatNode *node, int max)
{
	FormatNode *n;
	int			a;

	elog(DEBUG_elog_output, "to_from-char(): DUMP FORMAT");

	for (a = 0, n = node; a <= max; n++, a++)
	{
		if (n->type == NODE_TYPE_ACTION)
			elog(DEBUG_elog_output, "%d:\t NODE_TYPE_ACTION '%s'\t(%s,%s)",
				 a, n->key->name, DUMP_THth(n->suffix), DUMP_FM(n->suffix));
		else if (n->type == NODE_TYPE_CHAR)
			elog(DEBUG_elog_output, "%d:\t NODE_TYPE_CHAR '%s'", a, n->character);
		else if (n->type == NODE_TYPE_END)
		{
			elog(DEBUG_elog_output, "%d:\t NODE_TYPE_END", a);
			return;
		}
		else
			elog(DEBUG_elog_output, "%d:\t unknown NODE!", a);
	}
}
#endif							/* DEBUG */

/*****************************************************************************
 *			Private utils
 *****************************************************************************/

/* ----------
 * Return ST/ND/RD/TH for simple (1..9) numbers
 * type --> 0 upper, 1 lower
 * ----------
 */
static const char *
get_th(char *num, int type)
{
	int			len = strlen(num),
				last,
				seclast;

	last = *(num + (len - 1));
	if (!isdigit((unsigned char) last))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("\"%s\" is not a number", num)));

	/*
	 * All "teens" (<x>1[0-9]) get 'TH/th', while <x>[02-9][123] still get
	 * 'ST/st', 'ND/nd', 'RD/rd', respectively
	 */
	if ((len > 1) && ((seclast = num[len - 2]) == '1'))
		last = 0;

	switch (last)
	{
		case '1':
			if (type == TH_UPPER)
				return numTH[0];
			return numth[0];
		case '2':
			if (type == TH_UPPER)
				return numTH[1];
			return numth[1];
		case '3':
			if (type == TH_UPPER)
				return numTH[2];
			return numth[2];
		default:
			if (type == TH_UPPER)
				return numTH[3];
			return numth[3];
	}
}

/* ----------
 * Convert string-number to ordinal string-number
 * type --> 0 upper, 1 lower
 * ----------
 */
static char *
str_numth(char *dest, char *num, int type)
{
	if (dest != num)
		strcpy(dest, num);
	strcat(dest, get_th(num, type));
	return dest;
}

/*****************************************************************************
 *			upper/lower/initcap functions
 *****************************************************************************/

#ifdef USE_ICU

typedef int32_t (*ICU_Convert_Func) (UChar *dest, int32_t destCapacity,
									 const UChar *src, int32_t srcLength,
									 const char *locale,
									 UErrorCode *pErrorCode);

static int32_t
icu_convert_case(ICU_Convert_Func func, pg_locale_t mylocale,
				 UChar **buff_dest, UChar *buff_source, int32_t len_source)
{
	UErrorCode	status;
	int32_t		len_dest;

	len_dest = len_source;		/* try first with same length */
	*buff_dest = palloc(len_dest * sizeof(**buff_dest));
	status = U_ZERO_ERROR;
	len_dest = func(*buff_dest, len_dest, buff_source, len_source,
					mylocale->info.icu.locale, &status);
	if (status == U_BUFFER_OVERFLOW_ERROR)
	{
		/* try again with adjusted length */
		pfree(*buff_dest);
		*buff_dest = palloc(len_dest * sizeof(**buff_dest));
		status = U_ZERO_ERROR;
		len_dest = func(*buff_dest, len_dest, buff_source, len_source,
						mylocale->info.icu.locale, &status);
	}
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("case conversion failed: %s", u_errorName(status))));
	return len_dest;
}

static int32_t
u_strToTitle_default_BI(UChar *dest, int32_t destCapacity,
						const UChar *src, int32_t srcLength,
						const char *locale,
						UErrorCode *pErrorCode)
{
	return u_strToTitle(dest, destCapacity, src, srcLength,
						NULL, locale, pErrorCode);
}

#endif							/* USE_ICU */

/*
 * If the system provides the needed functions for wide-character manipulation
 * (which are all standardized by C99), then we implement upper/lower/initcap
 * using wide-character functions, if necessary.  Otherwise we use the
 * traditional <ctype.h> functions, which of course will not work as desired
 * in multibyte character sets.  Note that in either case we are effectively
 * assuming that the database character encoding matches the encoding implied
 * by LC_CTYPE.
 *
 * If the system provides locale_t and associated functions (which are
 * standardized by Open Group's XBD), we can support collations that are
 * neither default nor C.  The code is written to handle both combinations
 * of have-wide-characters and have-locale_t, though it's rather unlikely
 * a platform would have the latter without the former.
 */

/*
 * collation-aware, wide-character-aware lower function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
str_tolower(const char *buff, size_t nbytes, Oid collid)
{
	char	   *result;

	if (!buff)
		return NULL;

	/* C/POSIX collations use this path regardless of database encoding */
	if (lc_ctype_is_c(collid))
	{
		result = asc_tolower(buff, nbytes);
	}
#ifdef USE_WIDE_UPPER_LOWER
	else
	{
		pg_locale_t mylocale = 0;

		if (collid != DEFAULT_COLLATION_OID)
		{
			if (!OidIsValid(collid))
			{
				/*
				 * This typically means that the parser could not resolve a
				 * conflict of implicit collations, so report it that way.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("could not determine which collation to use for lower() function"),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));
			}
			mylocale = pg_newlocale_from_collation(collid);
		}

#ifdef USE_ICU
		if (mylocale && mylocale->provider == COLLPROVIDER_ICU)
		{
			int32_t		len_uchar;
			int32_t		len_conv;
			UChar	   *buff_uchar;
			UChar	   *buff_conv;

			len_uchar = icu_to_uchar(&buff_uchar, buff, nbytes);
			len_conv = icu_convert_case(u_strToLower, mylocale,
										&buff_conv, buff_uchar, len_uchar);
			icu_from_uchar(&result, buff_conv, len_conv);
			pfree(buff_uchar);
		}
		else
#endif
		{
			if (pg_database_encoding_max_length() > 1)
			{
				wchar_t    *workspace;
				size_t		curr_char;
				size_t		result_size;

				/* Overflow paranoia */
				if ((nbytes + 1) > (INT_MAX / sizeof(wchar_t)))
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));

				/* Output workspace cannot have more codes than input bytes */
				workspace = (wchar_t *) palloc((nbytes + 1) * sizeof(wchar_t));

				char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

				for (curr_char = 0; workspace[curr_char] != 0; curr_char++)
				{
#ifdef HAVE_LOCALE_T
					if (mylocale)
						workspace[curr_char] = towlower_l(workspace[curr_char], mylocale->info.lt);
					else
#endif
						workspace[curr_char] = towlower(workspace[curr_char]);
				}

				/*
				 * Make result large enough; case change might change number
				 * of bytes
				 */
				result_size = curr_char * pg_database_encoding_max_length() + 1;
				result = palloc(result_size);

				wchar2char(result, workspace, result_size, mylocale);
				pfree(workspace);
			}
#endif							/* USE_WIDE_UPPER_LOWER */
			else
			{
				char	   *p;

				result = pnstrdup(buff, nbytes);

				/*
				 * Note: we assume that tolower_l() will not be so broken as
				 * to need an isupper_l() guard test.  When using the default
				 * collation, we apply the traditional Postgres behavior that
				 * forces ASCII-style treatment of I/i, but in non-default
				 * collations you get exactly what the collation says.
				 */
				for (p = result; *p; p++)
				{
#ifdef HAVE_LOCALE_T
					if (mylocale)
						*p = tolower_l((unsigned char) *p, mylocale->info.lt);
					else
#endif
						*p = pg_tolower((unsigned char) *p);
				}
			}
		}
	}

	return result;
}

#ifdef USE_ICU
char*
ToUpperDBEncodeUseICU(const char* buff, size_t nbytes, Oid collid)
{
	int32_t len_uchar;
	int32_t len_conv;
	UChar *buff_uchar = NULL;
	UChar *buff_conv = NULL;
	char* result = NULL;

	len_uchar = icu_to_uchar(&buff_uchar, buff, nbytes);
	len_conv = icu_convert_case(u_strToLower, mylocale, &buff_conv, buff_uchar, len_uchar);
	icu_from_uchar(&result, buff_conv, len_conv);
	pfree(buff_uchar);
	return result;
}
#endif

static char*
str_toupper_database_encode(const char* buff, size_t nbytes, pg_locale_t mylocale)
{
	wchar_t* workspace = NULL;
	size_t curr_char;
	size_t result_size;
	char* result = NULL;

	/* Overflow paranoia */
	if ((nbytes + 1) > (INT_MAX / sizeof(wchar_t)))
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

	/* Output workspace cannot have more codes than input bytes */
	workspace = (wchar_t*)palloc((nbytes + 1) * sizeof(wchar_t));

	char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

	for (curr_char = 0; workspace[curr_char] != 0; curr_char++)
	{
#ifdef HAVE_LOCALE_T
		if (mylocale)
			workspace[curr_char] = towupper_l(workspace[curr_char], mylocale->info.lt);
		else
#endif
		workspace[curr_char] = towupper(workspace[curr_char]);
	}

	/* Make result large enough; case change might change number of bytes */
	result_size = curr_char * pg_database_encoding_max_length() + 1;
	result = (char*)palloc(result_size);

	wchar2char(result, workspace, result_size, mylocale);
	pfree(workspace);
	return result;
}

static char*
str_toupper_locale_encode(const char* buff, size_t nbytes, pg_locale_t mylocale)
{
	char* p = NULL;
	char *result = pnstrdup(buff, nbytes);

	/*
	* Note: we assume that toupper_l() will not be so broken as to need
	* an islower_l() guard test.  When using the default collation, we
	* apply the traditional Postgres behavior that forces ASCII-style
	* treatment of I/i, but in non-default collations you get exactly
	* what the collation says.
	*/
	for (p = result; *p; p++)
	{
#ifdef HAVE_LOCALE_T
		if (mylocale)
			*p = toupper_l((unsigned char)*p, mylocale->info.lt);
		else
#endif
		*p = pg_toupper((unsigned char)*p);
	}
	return result;
}

/*
 * collation-aware, wide-character-aware upper function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
str_toupper(const char *buff, size_t nbytes, Oid collid)
{
	char *result = NULL;
	pg_locale_t mylocale = NULL;

	if (!buff)
		return NULL;

	/* C/POSIX collations use this path regardless of database encoding */
	if (lc_ctype_is_c(collid))
	{
		return asc_toupper(buff, nbytes);
	}

#ifdef USE_WIDE_UPPER_LOWER
	mylocale = get_newlocale_from_collation_secure(collid);
#ifdef USE_ICU
	if (mylocale && mylocale->provider == COLLPROVIDER_ICU)
	{
		result = ToUpperDBEncodeUseICU(buff, nbytes, collid);
	}
	else
#endif
	{
		if (pg_database_encoding_max_length() > 1)
		{
			result = str_toupper_database_encode(buff, nbytes, mylocale);
		}
		else
		{
			result = str_toupper_locale_encode(buff, nbytes, mylocale);
		}
	}
#endif
	return result;
}

/*
 * str_toupper for rawout, no need to trans to wchar_t for database encode
 */
char *
ToUpperForRaw(const char *buff, size_t nbytes, Oid collid)
{
	char *result = NULL;
	if (NULL == buff)
		return NULL;

	/* C/POSIX collations use this path regardless of database encoding */
	if (lc_ctype_is_c(collid))
	{
		result = asc_toupper(buff, nbytes);
	}
	else
	{
		pg_locale_t mylocale = get_newlocale_from_collation_secure(collid);

#ifdef USE_ICU
		if (mylocale && mylocale->provider == COLLPROVIDER_ICU)
		{
			result = ToUpperDBEncodeUseICU(buff, nbytes, collid);
		}
		else
#endif
		{
			result = str_toupper_locale_encode(buff, nbytes, mylocale);
		}
	}
	return result;
}

static Datum
pg_char2wchar(PG_FUNCTION_ARGS)
{
	wchar_t *to = (wchar_t *)PG_GETARG_POINTER(0);
	size_t tolen = (size_t)DatumGetUInt32(PG_GETARG_DATUM(1));
	char *from = (char *)PG_GETARG_POINTER(2);
	size_t fromlen = (size_t)DatumGetUInt32(PG_GETARG_DATUM(3));
	pg_locale_t locale = 0;
	
	locale = (pg_locale_t)PG_GETARG_POINTER(4);

	return DatumGetInt32(char2wchar(to, tolen, from, fromlen, locale));
}

static char *
orcl_initcap_skip_illegal_characters(const char *buff, size_t nbytes, wchar_t *workspace, pg_locale_t mylocale)
{
	size_t		curr_char;
	size_t		result_size;
	char	   *result;
	int			pos = 0;
	int			result_pos = 0;
	int			mblen = 0;
	int			wasalnum = false;
	int			well_formed_len = 0;

	result_size = nbytes * pg_database_encoding_max_length() + 1;
	result = palloc(result_size);
	while( (pos < nbytes) && (0 == pg_well_formed_len(buff + pos, nbytes - pos, &well_formed_len)))
	{
		/* invalid byte */
		if (well_formed_len == 0)
		{
			memcpy(result + result_pos, buff + pos, 1);
			++result_pos;
			++pos;
		}
		else
		{
			char2wchar(workspace, nbytes + 1, buff + pos, well_formed_len, mylocale);
			wasalnum = false;
			for (curr_char = 0; workspace[curr_char] != 0; curr_char++)
			{
#ifdef HAVE_LOCALE_T
				if (mylocale)
				{
					if (wasalnum)
						workspace[curr_char] = towlower_l(workspace[curr_char], mylocale->info.lt);
					else
						workspace[curr_char] = towupper_l(workspace[curr_char], mylocale->info.lt);
					wasalnum = iswalnum_l(workspace[curr_char], mylocale->info.lt);
				}
				else
#endif
				{
					if (wasalnum)
						workspace[curr_char] = towlower(workspace[curr_char]);
					else
						workspace[curr_char] = towupper(workspace[curr_char]);
					wasalnum = iswalnum(workspace[curr_char]);
				}
			}
			mblen = wchar2char(result + result_pos, workspace, result_size - result_pos, mylocale);
			if (mblen != well_formed_len)
				elog(ERROR, "diff len mblen well_formed_len");
			result_pos += well_formed_len;
			pos += well_formed_len;
		}
	}
	result[result_pos] = '\0';
	pfree(workspace);
	return result;
}

/*
 * collation-aware, wide-character-aware initcap function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
str_initcap(const char *buff, size_t nbytes, Oid collid)
{
	char	   *result;
	int			wasalnum = false;
	bool		hasError = false;

	if (!buff)
		return NULL;

	/* C/POSIX collations use this path regardless of database encoding */
	if (lc_ctype_is_c(collid))
	{
		result = asc_initcap(buff, nbytes);
	}
#ifdef USE_WIDE_UPPER_LOWER
	else
	{
		pg_locale_t mylocale = 0;

		if (collid != DEFAULT_COLLATION_OID)
		{
			if (!OidIsValid(collid))
			{
				/*
				 * This typically means that the parser could not resolve a
				 * conflict of implicit collations, so report it that way.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("could not determine which collation to use for initcap() function"),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));
			}
			mylocale = pg_newlocale_from_collation(collid);
		}

#ifdef USE_ICU
		if (mylocale && mylocale->provider == COLLPROVIDER_ICU)
		{
			int32_t		len_uchar,
						len_conv;
			UChar	   *buff_uchar;
			UChar	   *buff_conv;

			len_uchar = icu_to_uchar(&buff_uchar, buff, nbytes);
			len_conv = icu_convert_case(u_strToTitle_default_BI, mylocale,
										&buff_conv, buff_uchar, len_uchar);
			icu_from_uchar(&result, buff_conv, len_conv);
			pfree(buff_uchar);
		}
		else
#endif
		{
			if (pg_database_encoding_max_length() > 1)
			{
				wchar_t    *workspace;
				size_t		curr_char;
				size_t		result_size;

				/* Overflow paranoia */
				if ((nbytes + 1) > (INT_MAX / sizeof(wchar_t)))
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
							 errmsg("out of memory")));

				/* Output workspace cannot have more codes than input bytes */
				workspace = (wchar_t *) palloc((nbytes + 1) * sizeof(wchar_t));

				if (ORA_MODE)
				{
					DirectFunctionCall5WithSubTran(pg_char2wchar,
						PointerGetDatum(workspace), UInt32GetDatum(nbytes + 1),
						PointerGetDatum(buff), UInt32GetDatum(nbytes), PointerGetDatum(mylocale),
						&hasError);
					/* opentenbase_ora skip the illegal charset */
					if (hasError)
						return orcl_initcap_skip_illegal_characters(buff, nbytes, workspace, mylocale);
				}
				else
					char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

				for (curr_char = 0; workspace[curr_char] != 0; curr_char++)
				{
#ifdef HAVE_LOCALE_T
					if (mylocale)
					{
						if (wasalnum)
							workspace[curr_char] = towlower_l(workspace[curr_char], mylocale->info.lt);
						else
							workspace[curr_char] = towupper_l(workspace[curr_char], mylocale->info.lt);
						wasalnum = iswalnum_l(workspace[curr_char], mylocale->info.lt);
					}
					else
#endif
					{
						if (wasalnum)
							workspace[curr_char] = towlower(workspace[curr_char]);
						else
							workspace[curr_char] = towupper(workspace[curr_char]);
						wasalnum = iswalnum(workspace[curr_char]);
					}
				}

				/*
				 * Make result large enough; case change might change number
				 * of bytes
				 */
				result_size = curr_char * pg_database_encoding_max_length() + 1;
				result = palloc(result_size);

				wchar2char(result, workspace, result_size, mylocale);
				pfree(workspace);
			}
#endif							/* USE_WIDE_UPPER_LOWER */
			else
			{
				char	   *p;

				result = pnstrdup(buff, nbytes);

				/*
				 * Note: we assume that toupper_l()/tolower_l() will not be so
				 * broken as to need guard tests.  When using the default
				 * collation, we apply the traditional Postgres behavior that
				 * forces ASCII-style treatment of I/i, but in non-default
				 * collations you get exactly what the collation says.
				 */
				for (p = result; *p; p++)
				{
#ifdef HAVE_LOCALE_T
					if (mylocale)
					{
						if (wasalnum)
							*p = tolower_l((unsigned char) *p, mylocale->info.lt);
						else
							*p = toupper_l((unsigned char) *p, mylocale->info.lt);
						wasalnum = isalnum_l((unsigned char) *p, mylocale->info.lt);
					}
					else
#endif
					{
						if (wasalnum)
							*p = pg_tolower((unsigned char) *p);
						else
							*p = pg_toupper((unsigned char) *p);
						wasalnum = isalnum((unsigned char) *p);
					}
				}
			}
		}
	}

	return result;
}

/*
 * ASCII-only lower function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
asc_tolower(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
		*p = pg_ascii_tolower((unsigned char) *p);

	return result;
}

/*
 * ASCII-only upper function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
asc_toupper(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
		*p = pg_ascii_toupper((unsigned char) *p);

	return result;
}

/*
 * ASCII-only initcap function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char *
asc_initcap(const char *buff, size_t nbytes)
{
	char	   *result;
	char	   *p;
	int			wasalnum = false;

	if (!buff)
		return NULL;

	result = pnstrdup(buff, nbytes);

	for (p = result; *p; p++)
	{
		char		c;

		if (wasalnum)
			*p = c = pg_ascii_tolower((unsigned char) *p);
		else
			*p = c = pg_ascii_toupper((unsigned char) *p);
		/* we don't trust isalnum() here */
		wasalnum = ((c >= 'A' && c <= 'Z') ||
					(c >= 'a' && c <= 'z') ||
					(c >= '0' && c <= '9'));
	}

	return result;
}

/* convenience routines for when the input is null-terminated */

static char *
str_tolower_z(const char *buff, Oid collid)
{
	return str_tolower(buff, strlen(buff), collid);
}

static char *
str_toupper_z(const char *buff, Oid collid)
{
	return str_toupper(buff, strlen(buff), collid);
}

static char *
str_initcap_z(const char *buff, Oid collid)
{
	return str_initcap(buff, strlen(buff), collid);
}

static char *
asc_tolower_z(const char *buff)
{
	return asc_tolower(buff, strlen(buff));
}

static char *
asc_toupper_z(const char *buff)
{
	return asc_toupper(buff, strlen(buff));
}

/* asc_initcap_z is not currently needed */


/* ----------
 * Skip TM / th in FROM_CHAR
 *
 * If S_THth is on, skip two chars, assuming there are two available
 * ----------
 */
#define SKIP_THth(ptr, _suf) \
	do { \
		if (S_THth(_suf)) \
		{ \
			if (*(ptr)) (ptr) += pg_mblen(ptr); \
			if (*(ptr)) (ptr) += pg_mblen(ptr); \
		} \
	} while (0)


#ifdef DEBUG_TO_FROM_CHAR
/* -----------
 * DEBUG: Call for debug and for index checking; (Show ASCII char
 * and defined keyword for each used position
 * ----------
 */
static void
dump_index(const KeyWord *k, const int *index)
{
	int			i,
				count = 0,
				free_i = 0;

	elog(DEBUG_elog_output, "TO-FROM_CHAR: Dump KeyWord Index:");

	for (i = 0; i < KeyWord_INDEX_SIZE; i++)
	{
		if (index[i] != -1)
		{
			elog(DEBUG_elog_output, "\t%c: %s, ", i + 32, k[index[i]].name);
			count++;
		}
		else
		{
			free_i++;
			elog(DEBUG_elog_output, "\t(%d) %c %d", i, i + 32, index[i]);
		}
	}
	elog(DEBUG_elog_output, "\n\t\tUsed positions: %d,\n\t\tFree positions: %d",
		 count, free_i);
}
#endif							/* DEBUG */

/* ----------
 * Return TRUE if next format picture is not digit value
 * ----------
 */
static bool
is_next_separator(FormatNode *n)
{
	if (n->type == NODE_TYPE_END)
		return FALSE;

	if (n->type == NODE_TYPE_ACTION && S_THth(n->suffix))
		return TRUE;

	/*
	 * Next node
	 */
	n++;

	/* end of format string is treated like a non-digit separator */
	if (n->type == NODE_TYPE_END)
		return TRUE;

	if (n->type == NODE_TYPE_ACTION)
	{
		if (n->key->is_digit)
			return FALSE;

		return TRUE;
	}
	else if (n->character[1] == '0' && isdigit((unsigned char) n->character[0]))
		return FALSE;

	return TRUE;				/* some non-digit input (separator) */
}


static int
adjust_partial_year_to_2020(int year)
{
	/*
	 * Adjust all dates toward 2020; this is effectively what happens when we
	 * assume '70' is 1970 and '69' is 2069.
	 */
	/* Force 0-69 into the 2000's */
	if (year < 70)
		return year + 2000;
	/* Force 70-99 into the 1900's */
	else if (year < 100)
		return year + 1900;
	/* Force 100-519 into the 2000's */
	else if (year < 520)
		return year + 2000;
	/* Force 520-999 into the 1000's */
	else if (year < 1000)
		return year + 1000;
	else
		return year;
}


static int
strspace_len(char *str)
{
	int			len = 0;

	while (*str && isspace((unsigned char) *str))
	{
		str++;
		len++;
	}
	return len;
}

/*
 * Set the date mode of a from-char conversion.
 *
 * Puke if the date mode has already been set, and the caller attempts to set
 * it to a conflicting mode.
 */
static void
from_char_set_mode(TmFromChar *tmfc, const FromCharDateMode mode)
{
	if (mode != FROM_CHAR_DATE_NONE)
	{
		if (tmfc->mode == FROM_CHAR_DATE_NONE)
			tmfc->mode = mode;
		else if (tmfc->mode != mode)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
					 errmsg("invalid combination of date conventions"),
					 errhint("Do not mix Gregorian and ISO week date "
							 "conventions in a formatting template.")));
	}
}

/*
 * Set the integer pointed to by 'dest' to the given value.
 *
 * Puke if the destination integer has previously been set to some other
 * non-zero value.
 */
static void
from_char_set_int(int *dest, const int value, const FormatNode *node)
{
	if (*dest != 0 && *dest != value)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("conflicting values for \"%s\" field in formatting string",
						node->key->name),
				 errdetail("This value contradicts a previous setting for "
						   "the same field type.")));
	*dest = value;
}

/*
 * Read a single integer from the source string, into the int pointed to by
 * 'dest'. If 'dest' is NULL, the result is discarded.
 *
 * In fixed-width mode (the node does not have the FM suffix), consume at most
 * 'len' characters.  However, any leading whitespace isn't counted in 'len'.
 *
 * We use strtol() to recover the integer value from the source string, in
 * accordance with the given FormatNode.
 *
 * If the conversion completes successfully, src will have been advanced to
 * point at the character immediately following the last character used in the
 * conversion.
 *
 * Return the number of characters consumed.
 *
 * Note that from_char_parse_int() provides a more convenient wrapper where
 * the length of the field is the same as the length of the format keyword (as
 * with DD and MI).
 */
static int
from_char_parse_int_len(int *dest, char **src, const int len, FormatNode *node)
{
	long		result;
	char		copy[DCH_MAX_ITEM_SIZ + 1] = {0};
	char	   *init = *src;
	int			used;

	/*
	 * Skip any whitespace before parsing the integer.
	 */
	*src += strspace_len(*src);

	Assert(len <= DCH_MAX_ITEM_SIZ);
	used = (int) strlcpy(copy, *src, len + 1);

	if (S_FM(node->suffix) || is_next_separator(node))
	{
		/*
		 * This node is in Fill Mode, or the next node is known to be a
		 * non-digit value, so we just slurp as many characters as we can get.
		 */
		errno = 0;
		result = strtol(init, src, 10);
	}
	else
	{
		/*
		 * We need to pull exactly the number of characters given in 'len' out
		 * of the string, and convert those.
		 */
		char	   *last;

		if (!ORA_MODE && used < len)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
					 errmsg("source string too short for \"%s\" formatting field",
							node->key->name),
					 errdetail("Field requires %d characters, but only %d "
							   "remain.",
							   len, used),
					 errhint("If your source string is not fixed-width, try "
							 "using the \"FM\" modifier.")));

		errno = 0;
		result = strtol(copy, &last, 10);
		used = last - copy;

		if (used > 0 && used < len)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
					 errmsg("invalid value \"%s\" for \"%s\"",
							copy, node->key->name),
					 errdetail("Field requires %d characters, but only %d "
							   "could be parsed.", len, used),
					 errhint("If your source string is not fixed-width, try "
							 "using the \"FM\" modifier.")));

		*src += used;
	}

	if (*src == init)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("invalid value \"%s\" for \"%s\"",
						copy, node->key->name),
				 errdetail("Value must be an integer.")));

	if (errno == ERANGE || result < INT_MIN || result > INT_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("value for \"%s\" in source string is out of range",
						node->key->name),
				 errdetail("Value must be in the range %d to %d.",
						   INT_MIN, INT_MAX)));

	if (dest != NULL)
		from_char_set_int(dest, (int) result, node);
	return *src - init;
}

/*
 * Call from_char_parse_int_len(), using the length of the format keyword as
 * the expected length of the field.
 *
 * Don't call this function if the field differs in length from the format
 * keyword (as with HH24; the keyword length is 4, but the field length is 2).
 * In such cases, call from_char_parse_int_len() instead to specify the
 * required length explicitly.
 */
static int
from_char_parse_int(int *dest, char **src, FormatNode *node)
{
	return from_char_parse_int_len(dest, src, node->key->len, node);
}

/* ----------
 * Sequential search with to upper/lower conversion
 * ----------
 */
static int
seq_search(char *name, const char *const *array, int type, int max, int *len)
{
	const char *p;
	const char *const *a;
	char	   *n;
	int			last,
				i;

	*len = 0;

	if (!*name)
		return -1;

	/* set first char */
	if (type == ONE_UPPER || type == ALL_UPPER)
		*name = pg_toupper((unsigned char) *name);
	else if (type == ALL_LOWER)
		*name = pg_tolower((unsigned char) *name);

	for (last = 0, a = array; *a != NULL; a++)
	{
		/* compare first chars */
		if (*name != **a)
			continue;

		for (i = 1, p = *a + 1, n = name + 1;; n++, p++, i++)
		{
			/* search fragment (max) only */
			if (max && i == max)
			{
				*len = i;
				return a - array;
			}
			/* full size */
			if (*p == '\0')
			{
				*len = i;
				return a - array;
			}
			/* Not found in array 'a' */
			if (*n == '\0')
				break;

			/*
			 * Convert (but convert new chars only)
			 */
			if (i > last)
			{
				if (type == ONE_UPPER || type == ALL_LOWER)
					*n = pg_tolower((unsigned char) *n);
				else if (type == ALL_UPPER)
					*n = pg_toupper((unsigned char) *n);
				last = i;
			}

#ifdef DEBUG_TO_FROM_CHAR
			elog(DEBUG_elog_output, "N: %c, P: %c, A: %s (%s)",
				 *n, *p, *a, name);
#endif
			if (*n != *p)
				break;
		}
	}

	return -1;
}

/*
 * Perform a sequential search in 'array' for text matching the first 'max'
 * characters of the source string.
 *
 * If a match is found, copy the array index of the match into the integer
 * pointed to by 'dest', advance 'src' to the end of the part of the string
 * which matched, and return the number of characters consumed.
 *
 * If the string doesn't match, throw an error.
 */
static int
from_char_seq_search(int *dest, char **src, const char *const *array, int type, int max,
					 FormatNode *node)
{
	int			len;

	*dest = seq_search(*src, array, type, max, &len);
	if (len <= 0)
	{
		char		copy[DCH_MAX_ITEM_SIZ + 1];

		Assert(max <= DCH_MAX_ITEM_SIZ);
		strlcpy(copy, *src, max + 1);

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
				 errmsg("invalid value \"%s\" for \"%s\"",
						copy, node->key->name),
				 errdetail("The given value did not match any of the allowed "
						   "values for this field.")));
	}
	*src += len;
	return len;
}

/* ----------
 * Process a TmToChar struct as denoted by a list of FormatNodes.
 * The formatted data is written to the string pointed to by 'out'.
 * ----------
 */
static void
DCH_to_char(FormatNode *node, bool is_interval, TmToChar *in, char *out, Oid collid)
{
	FormatNode *n;
	char	   *s;
	struct pg_tm *tm = &in->tm;
	int			i;

	/* cache localized days and months */
	cache_locale_time();

	s = out;
	for (n = node; n->type != NODE_TYPE_END; n++)
	{
		if (n->type != NODE_TYPE_ACTION)
		{
			const char *ptr = n->character;

			while (*ptr != '\0')
			{
				if ((
					enable_lightweight_ora_syntax) &&
					(*ptr == 'X' || *ptr == 'x'))
				{
					*s = '.';			/* only in opentenbase_ora or LIGHTWEIGHT_ORA mode */
				}
				else
				{
					*s = *ptr;
				}

				s += 1;
				ptr += 1;
			}
			continue;
		}
		switch (n->key->id)
		{
			case DCH_A_M:
			case DCH_P_M:
				strcpy(s, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2)
					   ? P_M_STR : A_M_STR);
				s += strlen(s);
				break;
			case DCH_AM:
			case DCH_PM:
				strcpy(s, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2)
					   ? PM_STR : AM_STR);
				s += strlen(s);
				break;
			case DCH_a_m:
			case DCH_p_m:
				strcpy(s, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2)
					   ? p_m_STR : a_m_STR);
				s += strlen(s);
				break;
			case DCH_am:
			case DCH_pm:
				strcpy(s, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2)
					   ? pm_STR : am_STR);
				s += strlen(s);
				break;
			case DCH_HH:
			case DCH_HH12:
				/*
				 * display time as shown on a 12-hour clock, even for
				 * intervals
				 */
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : (tm->tm_hour >= 0) ? 2 : 3,
						tm->tm_hour % (HOURS_PER_DAY / 2) == 0 ? HOURS_PER_DAY / 2 :
						tm->tm_hour % (HOURS_PER_DAY / 2));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_HH24:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : (tm->tm_hour >= 0) ? 2 : 3,
						tm->tm_hour);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_MI:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : (tm->tm_min >= 0) ? 2 : 3,
						tm->tm_min);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_SS:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : (tm->tm_sec >= 0) ? 2 : 3,
						tm->tm_sec);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_MS:		/* millisecond */
				sprintf(s, "%03d", (int) (in->fsec / INT64CONST(1000)));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_US:		/* microsecond */
				sprintf(s, "%06d", (int) in->fsec);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_FF1:
			case DCH_FF2:
			case DCH_FF3:
			case DCH_FF4:
			case DCH_FF5:
			case DCH_FF6:
			case DCH_FF7:
			case DCH_FF8:
			case DCH_FF9:
			case DCH_FF:
			case DCH_ff1:
			case DCH_ff2:
			case DCH_ff3:
			case DCH_ff4:
			case DCH_ff5:
			case DCH_ff6:
			case DCH_ff7:
			case DCH_ff8:
			case DCH_ff9:
			case DCH_ff:
				/* PG16 supports FF1~FF6 but not FF7~FF */
					/* just copy the unrecognized token, this is PG behaviour */
					strncpy(s, n->key->name, strlen(n->key->name));
					s += strlen(n->key->name);
				break;
			case DCH_SSSS:
			case DCH_SSSSS:
				sprintf(s, "%d", tm->tm_hour * SECS_PER_HOUR +
						tm->tm_min * SECS_PER_MINUTE +
						tm->tm_sec);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_tz:
				INVALID_FOR_INTERVAL;
				if (tmtcTzn(in))
				{
					/* We assume here that timezone names aren't localized */
					char	   *p = asc_tolower_z(tmtcTzn(in));

					strcpy(s, p);
					pfree(p);
					s += strlen(s);
				}
				break;
			case DCH_TZ:
				INVALID_FOR_INTERVAL;
				if (tmtcTzn(in))
				{
					strcpy(s, tmtcTzn(in));
					s += strlen(s);
				}
				break;
			case DCH_TZH:
				INVALID_FOR_INTERVAL;
				sprintf(s, "%c%02d",
						(tm->tm_gmtoff >= 0) ? '+' : '-',
						abs((int) tm->tm_gmtoff) / SECS_PER_HOUR);
				s += strlen(s);
				break;
			case DCH_TZM:
				INVALID_FOR_INTERVAL;
				sprintf(s, "%02d",
						(abs((int) tm->tm_gmtoff) % SECS_PER_HOUR) / SECS_PER_MINUTE);
				s += strlen(s);
				break;
			case DCH_OF:
				INVALID_FOR_INTERVAL;
				sprintf(s, "%c%0*d",
						(tm->tm_gmtoff >= 0) ? '+' : '-',
						S_FM(n->suffix) ? 0 : 2,
						abs((int) tm->tm_gmtoff) / SECS_PER_HOUR);
				s += strlen(s);
				if (abs((int) tm->tm_gmtoff) % SECS_PER_HOUR != 0)
				{
					sprintf(s, ":%02d",
							(abs((int) tm->tm_gmtoff) % SECS_PER_HOUR) / SECS_PER_MINUTE);
					s += strlen(s);
				}
				break;
			case DCH_A_D:
			case DCH_B_C:
				INVALID_FOR_INTERVAL;
				strcpy(s, (tm->tm_year <= 0 ? B_C_STR : A_D_STR));
				s += strlen(s);
				break;
			case DCH_AD:
			case DCH_BC:
				INVALID_FOR_INTERVAL;
				strcpy(s, (tm->tm_year <= 0 ? BC_STR : AD_STR));
				s += strlen(s);
				break;
			case DCH_a_d:
			case DCH_b_c:
				INVALID_FOR_INTERVAL;
				strcpy(s, (tm->tm_year <= 0 ? b_c_STR : a_d_STR));
				s += strlen(s);
				break;
			case DCH_ad:
			case DCH_bc:
				INVALID_FOR_INTERVAL;
				strcpy(s, (tm->tm_year <= 0 ? bc_STR : ad_STR));
				s += strlen(s);
				break;
			case DCH_MONTH:
				INVALID_FOR_INTERVAL;
				if (!tm->tm_mon)
					break;
				if (S_TM(n->suffix))
				{
					char	   *str = str_toupper_z(localized_full_months[tm->tm_mon - 1], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -9,
							asc_toupper_z(months_full[tm->tm_mon - 1]));
				s += strlen(s);
				break;
			case DCH_Month:
				INVALID_FOR_INTERVAL;
				if (!tm->tm_mon)
					break;
				if (S_TM(n->suffix))
				{
					char	   *str = str_initcap_z(localized_full_months[tm->tm_mon - 1], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -9,
							months_full[tm->tm_mon - 1]);
				s += strlen(s);
				break;
			case DCH_month:
				INVALID_FOR_INTERVAL;
				if (!tm->tm_mon)
					break;
				if (S_TM(n->suffix))
				{
					char	   *str = str_tolower_z(localized_full_months[tm->tm_mon - 1], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -9,
							asc_tolower_z(months_full[tm->tm_mon - 1]));
				s += strlen(s);
				break;
			case DCH_MON:
				INVALID_FOR_INTERVAL;
				if (!tm->tm_mon)
					break;
				if (S_TM(n->suffix))
				{
					char	   *str = str_toupper_z(localized_abbrev_months[tm->tm_mon - 1], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					strcpy(s, asc_toupper_z(months[tm->tm_mon - 1]));
				s += strlen(s);
				break;
			case DCH_Mon:
				INVALID_FOR_INTERVAL;
				if (!tm->tm_mon)
					break;
				if (S_TM(n->suffix))
				{
					char	   *str = str_initcap_z(localized_abbrev_months[tm->tm_mon - 1], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					strcpy(s, months[tm->tm_mon - 1]);
				s += strlen(s);
				break;
			case DCH_mon:
				INVALID_FOR_INTERVAL;
				if (!tm->tm_mon)
					break;
				if (S_TM(n->suffix))
				{
					char	   *str = str_tolower_z(localized_abbrev_months[tm->tm_mon - 1], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					strcpy(s, asc_tolower_z(months[tm->tm_mon - 1]));
				s += strlen(s);
				break;
			case DCH_MM:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : (tm->tm_mon >= 0) ? 2 : 3,
						tm->tm_mon);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_DAY:
				INVALID_FOR_INTERVAL;
				if (S_TM(n->suffix))
				{
					char	   *str = str_toupper_z(localized_full_days[tm->tm_wday], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -9,
							asc_toupper_z(days[tm->tm_wday]));
				s += strlen(s);
				break;
			case DCH_Day:
				INVALID_FOR_INTERVAL;
				if (S_TM(n->suffix))
				{
					char	   *str = str_initcap_z(localized_full_days[tm->tm_wday], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -9,
							days[tm->tm_wday]);
				s += strlen(s);
				break;
			case DCH_day:
				INVALID_FOR_INTERVAL;
				if (S_TM(n->suffix))
				{
					char	   *str = str_tolower_z(localized_full_days[tm->tm_wday], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -9,
							asc_tolower_z(days[tm->tm_wday]));
				s += strlen(s);
				break;
			case DCH_DY:
				INVALID_FOR_INTERVAL;
				if (S_TM(n->suffix))
				{
					char	   *str = str_toupper_z(localized_abbrev_days[tm->tm_wday], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					strcpy(s, asc_toupper_z(days_short[tm->tm_wday]));
				s += strlen(s);
				break;
			case DCH_Dy:
				INVALID_FOR_INTERVAL;
				if (S_TM(n->suffix))
				{
					char	   *str = str_initcap_z(localized_abbrev_days[tm->tm_wday], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					strcpy(s, days_short[tm->tm_wday]);
				s += strlen(s);
				break;
			case DCH_dy:
				INVALID_FOR_INTERVAL;
				if (S_TM(n->suffix))
				{
					char	   *str = str_tolower_z(localized_abbrev_days[tm->tm_wday], collid);

					if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)
						strcpy(s, str);
					else
						ereport(ERROR,
								(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
								 errmsg("localized string format value too long")));
				}
				else
					strcpy(s, asc_tolower_z(days_short[tm->tm_wday]));
				s += strlen(s);
				break;
			case DCH_DDD:
			case DCH_IDDD:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 3,
						(n->key->id == DCH_DDD) ?
						tm->tm_yday :
						date2isoyearday(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_DD:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_mday);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_D:
				INVALID_FOR_INTERVAL;
				sprintf(s, "%d", tm->tm_wday + 1);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_ID:
				INVALID_FOR_INTERVAL;
				sprintf(s, "%d", (tm->tm_wday == 0) ? 7 : tm->tm_wday);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_WW:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2,
						(tm->tm_yday - 1) / 7 + 1);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_IW:
				sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : 2,
						date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_Q:
				if (!tm->tm_mon)
					break;
				sprintf(s, "%d", (tm->tm_mon - 1) / 3 + 1);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_CC:
				if (is_interval)	/* straight calculation */
					i = tm->tm_year / 100;
				else
				{
					if (tm->tm_year > 0)
						/* Century 20 == 1901 - 2000 */
						i = (tm->tm_year - 1) / 100 + 1;
					else
						/* Century 6BC == 600BC - 501BC */
						i = tm->tm_year / 100 - 1;
				}
				if (i <= 99 && i >= -99)
					sprintf(s, "%0*d", S_FM(n->suffix) ? 0 : (i >= 0) ? 2 : 3, i);
				else
					sprintf(s, "%d", i);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_Y_YYY:
				i = ADJUST_YEAR(tm->tm_year, is_interval) / 1000;
				sprintf(s, "%d,%03d", i,
						ADJUST_YEAR(tm->tm_year, is_interval) - (i * 1000));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_YYYY:
			case DCH_IYYY:
				sprintf(s, "%0*d",
						S_FM(n->suffix) ? 0 :
						(ADJUST_YEAR(tm->tm_year, is_interval) >= 0) ? 4 : 5,
						(n->key->id == DCH_YYYY ?
						 ADJUST_YEAR(tm->tm_year, is_interval) :
						 ADJUST_YEAR(date2isoyear(tm->tm_year,
												  tm->tm_mon,
												  tm->tm_mday),
									 is_interval)));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_YYY:
			case DCH_IYY:
				sprintf(s, "%0*d",
						S_FM(n->suffix) ? 0 :
						(ADJUST_YEAR(tm->tm_year, is_interval) >= 0) ? 3 : 4,
						(n->key->id == DCH_YYY ?
						 ADJUST_YEAR(tm->tm_year, is_interval) :
						 ADJUST_YEAR(date2isoyear(tm->tm_year,
												  tm->tm_mon,
												  tm->tm_mday),
									 is_interval)) % 1000);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_rr:
			case DCH_RR:
			case DCH_YY:
			case DCH_IY:
				sprintf(s, "%0*d",
						S_FM(n->suffix) ? 0 :
						(ADJUST_YEAR(tm->tm_year, is_interval) >= 0) ? 2 : 3,
						(n->key->id == DCH_YY ?
						 ADJUST_YEAR(tm->tm_year, is_interval) :
						 ADJUST_YEAR(date2isoyear(tm->tm_year,
												  tm->tm_mon,
												  tm->tm_mday),
									 is_interval)) % 100);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_Y:
			case DCH_I:
				sprintf(s, "%1d",
						(n->key->id == DCH_Y ?
						 ADJUST_YEAR(tm->tm_year, is_interval) :
						 ADJUST_YEAR(date2isoyear(tm->tm_year,
												  tm->tm_mon,
												  tm->tm_mday),
									 is_interval)) % 10);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_RM:
				/* FALLTHROUGH */
			case DCH_rm:

				/*
				 * For intervals, values like '12 month' will be reduced to 0
				 * month and some years.  These should be processed.
				 */
				if (!tm->tm_mon && !tm->tm_year)
					break;
				else
				{
					int			mon = 0;
					const char *const *months;

					if (n->key->id == DCH_RM)
						months = rm_months_upper;
					else
						months = rm_months_lower;

					/*
					 * Compute the position in the roman-numeral array.  Note
					 * that the contents of the array are reversed, December
					 * being first and January last.
					 */
					if (tm->tm_mon == 0)
					{
						/*
						 * This case is special, and tracks the case of full
						 * interval years.
						 */
						mon = tm->tm_year >= 0 ? 0 : MONTHS_PER_YEAR - 1;
					}
					else if (tm->tm_mon < 0)
					{
						/*
						 * Negative case.  In this case, the calculation is
						 * reversed, where -1 means December, -2 November,
						 * etc.
						 */
						mon = -1 * (tm->tm_mon + 1);
					}
					else
					{
						/*
						 * Common case, with a strictly positive value.  The
						 * position in the array matches with the value of
						 * tm_mon.
						 */
						mon = MONTHS_PER_YEAR - tm->tm_mon;
					}

					sprintf(s, "%*s", S_FM(n->suffix) ? 0 : -4,
							months[mon]);
					s += strlen(s);
				}
				break;
			case DCH_W:
				sprintf(s, "%d", (tm->tm_mday - 1) / 7 + 1);
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
			case DCH_J:
				sprintf(s, "%d", date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
				if (S_THth(n->suffix))
					str_numth(s, s, S_TH_TYPE(n->suffix));
				s += strlen(s);
				break;
		}
	}

	*s = '\0';
}

static void
OpenTenBaseOraUseLocalTime(TmFromChar *out)
{
	/*
	 * Just like opentenbase_ora, set default year and month,
	 * Then we can check day, wday and yday.
	 */
	if (out->year == 0 || (out->mm == 0 && out->ddd == 0))
	{
		struct tm tm_s;
		time_t    now;

		now = time(NULL);
		localtime_r(&now, &tm_s);

		if (out->year == 0)
			out->year = tm_s.tm_year + 1900;
		if (out->mm == 0 && out->ddd == 0)
			out->mm = tm_s.tm_mon + 1;
	}

	return;
}

static bool
DoFullDateParse(char *s, int yy_order, int mm_order, int dd_order, TmFromChar *out)
{
	int       current_date;
	const int minlen = 5;
	const int maxlen = 8;
	const int numlen = 10;
	char     *head = s;
	char     *end = NULL;
	int       slen = strlen(s);
	char      tmp[numlen];

	/* must contains yyyy-mm-dd */
	if (slen < minlen || slen > maxlen)
	{
		return false;
	}

	memset(tmp, 0, numlen);
	current_date = strtol(s, &end, numlen);
	strcpy(tmp, s);

	/* case1: yyyy-mm-dd */
	if (yy_order < mm_order && mm_order < dd_order)
	{
		out->dd = current_date % 100;
		current_date /= 100;
		out->mm = current_date % 100;
		current_date /= 100;
		out->year = current_date;
	}
	/* case2: yyyy-dd-mm */
	else if (yy_order < dd_order && dd_order < mm_order)
	{
		out->mm = current_date % 100;
		current_date /= 100;
		out->dd = current_date % 100;
		current_date /= 100;
		out->year = current_date;
	}
	/* case3: mm-dd-yyyy */
	else if (mm_order < dd_order && dd_order < yy_order)
	{
		out->mm = (*head - '0') * 10 + *(head + 1) - '0';
		head += 2;
		out->dd = (*head - '0') * 10 + *(head + 1) - '0';
		head += 2;
		out->year = strtol(head, &end, numlen);
	}
	/* case4: dd-mm-yyyy */
	else if (dd_order < mm_order && mm_order < yy_order)
	{
		head = tmp;
		out->dd = (*head - '0') * 10 + *(head + 1) - '0';
		head += 2;
		out->mm = (*head - '0') * 10 + *(head + 1) - '0';
		head += 2;
		out->year = strtol(head, &end, numlen);
	}
	/* case5: mm-yyyy-dd */
	else if (mm_order < yy_order && yy_order < dd_order)
	{
		head = tmp;
		out->mm = (*head - '0') * 10 + *(head + 1) - '0';
		out->dd = (*(head + slen - 2) - '0') * 10 + *(head + slen - 1) - '0';
		*(head + slen - 2) = '\0';
		out->year = strtol(head + 2, &end, numlen);
	}
	/* case6: dd-yyyy-mm */
	else if (dd_order < yy_order && yy_order < mm_order)
	{
		head = tmp;
		out->dd = (*head - '0') * 10 + *(head + 1) - '0';
		out->mm = (*(head + slen - 2) - '0') * 10 + *(head + slen - 1) - '0';
		*(head + slen - 2) = '\0';
		out->year = strtol(head + 2, &end, numlen);
	}

	return true;
}

static bool
DoPartDateParse(char *s, int yy_order, int mm_order, int dd_order, TmFromChar *out)
{
	int        current_date;
	const int  minlen = 3;
	const int  maxlen = 6;
	const int  numlen = 10;
	const int  invalid_order = -1;
	char      *head = s;
	char      *end = NULL;
	int        slen = strlen(s);
	char       tmp[numlen];

	/* must contains two of yyyy-mm-dd */
	if (slen < minlen || slen > maxlen)
	{
		return false;
	}

	memset(tmp, 0, numlen);
	current_date = strtol(s, &end, numlen);
	strcpy(tmp, s);

	if ((yy_order == invalid_order && mm_order == invalid_order) ||
	    (yy_order == invalid_order && dd_order == invalid_order) ||
	    (mm_order == invalid_order && dd_order == invalid_order) ||
	    (yy_order == invalid_order && mm_order == invalid_order && dd_order == invalid_order))
	{
		return false;
	}

	if (yy_order == invalid_order)
	{
		if (mm_order < dd_order)
		{
			/* case1 mm-dd */
			out->mm = current_date % 100;
			current_date /= 100;
			out->dd = current_date;
		}
		else
		{
			/* case2 dd-mm */
			out->dd = current_date % 100;
			current_date /= 100;
			out->mm = current_date;
		}
	}
	else if (mm_order == invalid_order)
	{
		if (yy_order < dd_order)
		{
			/* case3 yyyy-dd */
			out->dd = current_date % 100;
			current_date /= 100;
			out->year = current_date;
		}
		else
		{
			/* case4 dd-yyyy */
			head = tmp;
			out->dd = (*head - '0') * 10 + *(head + 1) - '0';
			out->year = strtol(head + 2, &end, numlen);
		}
	}
	else if (dd_order == invalid_order)
	{
		if (yy_order < mm_order)
		{
			/* case5 yyyy-mm */
			out->mm = current_date % 100;
			current_date /= 100;
			out->year = current_date;
		}
		else
		{
			/* case6 mm-yyyy */
			head = tmp;
			out->mm = (*head - '0') * 10 + *(head + 1) - '0';
			out->year = strtol(head + 2, &end, numlen);
		}
	}
	else
	{
		return false;
	}

	return true;
}

static void
CheckDataValid(TmFromChar *out)
{
	const int max_support_year = 9999;
	const int min_support_year = 0;
	const int max_support_mm = 12;

	/* Output the same error message as opentenbase_ora */
	if (out->year > max_support_year || out->year <= min_support_year)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
		         errmsg("(full) year must be between -4713 and +9999, and not be 0.")));
	}

	if (out->mm < 0 || out->mm > max_support_mm)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("literal does not match format string")));
	}
}

#define CHECK_FMT_VALID(current_order, last_order)                                                 \
	if (current_order - last_order > 1)                                                            \
	{                                                                                              \
		return false;                                                                              \
	}

/*
 * Only contains yyyy-mm-dd
 */
static bool
PreprocessDateStr(FormatNode *node, char *in, TmFromChar *out)
{
	FormatNode *n = NULL;
	char       *s = in;
	char       *end = NULL;
	const int   invalid_order = -1;
	const int   numlen = 10;
	const int   max_support_len = 8; /* yyyymmdd */
	bool        is_done = false;
	int         slen = 0;
	int         needlen = 0;
	int         noaction_cnt = 0;
	int         other_fmt_cnt = 0;
	int         current_order = 1;
	int         last_order = 0;
	int         current_date;
	int         yy_order = invalid_order;
	int         mm_order = invalid_order;
	int         dd_order = invalid_order;

	current_date = strtol(s, &end, numlen);
	slen = end - in;
	if ((end && *end != '\0') || current_date == 0 || (slen > max_support_len) || out == NULL ||
	    s == NULL)
	{
		return false;
	}

	/* parse the format order */
	for (n = node; n->type != NODE_TYPE_END; n++)
	{
		if (n->type != NODE_TYPE_ACTION)
		{
			noaction_cnt++;
			continue;
		}

		switch (n->key->id)
		{
			case DCH_Y_YYY:
			case DCH_YYYY:
			case DCH_IYYY:
			{
				out->yysz = (out->yysz) ? out->yysz : 4;
			}
			case DCH_YYY:
			case DCH_IYY:
			{
				out->yysz = (out->yysz) ? out->yysz : 3;
			}
			case DCH_rr:
			case DCH_RR:
			case DCH_YY:
			case DCH_IY:
			{
				out->yysz = (out->yysz) ? out->yysz : 2;
			}
			case DCH_Y:
			case DCH_I:
			{
				CHECK_FMT_VALID(current_order, last_order);
				last_order = current_order;
				yy_order = current_order;
				out->yysz = (out->yysz) ? out->yysz : 1;
				needlen += out->yysz;
				break;
			}
			case DCH_MONTH:
			case DCH_Month:
			case DCH_month:
			case DCH_MON:
			case DCH_Mon:
			case DCH_mon:
			case DCH_MM:
			{
				CHECK_FMT_VALID(current_order, last_order);
				last_order = current_order;
				mm_order = current_order;
				needlen += 2;
				break;
			}
			case DCH_DD:
			{
				CHECK_FMT_VALID(current_order, last_order);
				last_order = current_order;
				dd_order = current_order;
				needlen += 2;
				break;
			}
			default:
			{
				other_fmt_cnt++;
				current_order++;
				break;
			}
		}

		current_order++;
	}

	/*
	 * 1. Only year, month and day are processed, and no interval character is processed
	 * 2. In incompatible mode, the length must be the same
	 */
	if ((other_fmt_cnt > 0 && noaction_cnt == 0) || slen != needlen)
	{
		return false;
	}

	/* do parser */
	if (yy_order != invalid_order && mm_order != invalid_order && dd_order != invalid_order)
	{
		is_done = DoFullDateParse(in, yy_order, mm_order, dd_order, out);
	}
	else
	{
		is_done = DoPartDateParse(in, yy_order, mm_order, dd_order, out);
	}

	/* If the rules are not met, the match fails */
	if (is_done == false)
	{
		return false;
	}

	/* lightweight_ora: fill default year and month. */
	OpenTenBaseOraUseLocalTime(out);

	/* lightweight_ora: adjust year */
	if (out->year / 10000 == 0)
	{
		out->year = adjust_partial_year_to_2020(out->year);
	}

	CheckDataValid(out);

	return true;
}

/* ----------
 * Process a string as denoted by a list of FormatNodes.
 * The TmFromChar struct pointed to by 'out' is populated with the results.
 *
 * Note: we currently don't have any to_interval() function, so there
 * is no need here for INVALID_FOR_INTERVAL checks.
 * ----------
 */
static void
DCH_from_char(FormatNode *node, char *in, TmFromChar *out)
{
	FormatNode *n;
	char	   *s;
	int			len,
				value;
	bool		fx_mode = false;

	/*
	 * Support LIGHTWEIGHT_ORA TO_DATE(20170809) function to pre-parse the date string.
	 * If successful, return the parsed date directly.
	 */
	if (PreprocessDateStr(node, in, out) == true)
	{
		return;
	}

	for (n = node, s = in; n->type != NODE_TYPE_END && *s != '\0'; n++)
	{
		if (n->type != NODE_TYPE_ACTION)
		{
			/*
			 * Separator, so consume one character from input string.  Notice
			 * we don't insist that the consumed character match the format's
			 * character.
			 */
			s += pg_mblen(s);
			continue;
		}

		/* Ignore spaces before fields when not in FX (fixed width) mode */
		if (!fx_mode && n->key->id != DCH_FX)
		{
			while (*s != '\0' && isspace((unsigned char) *s))
				s++;
		}

		from_char_set_mode(out, n->key->date_mode);

		switch (n->key->id)
		{
			case DCH_FX:
				fx_mode = true;
				break;
			case DCH_A_M:
			case DCH_P_M:
			case DCH_a_m:
			case DCH_p_m:
				from_char_seq_search(&value, &s, ampm_strings_long,
									 ALL_UPPER, n->key->len, n);
				from_char_set_int(&out->pm, value % 2, n);
				out->clock = CLOCK_12_HOUR;
				break;
			case DCH_AM:
			case DCH_PM:
			case DCH_am:
			case DCH_pm:
				from_char_seq_search(&value, &s, ampm_strings,
									 ALL_UPPER, n->key->len, n);
				from_char_set_int(&out->pm, value % 2, n);
				out->clock = CLOCK_12_HOUR;
				break;
			case DCH_HH:
			case DCH_HH12:
				from_char_parse_int_len(&out->hh, &s, 2, n);
				out->clock = CLOCK_12_HOUR;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_HH24:
				from_char_parse_int_len(&out->hh, &s, 2, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_MI:
				from_char_parse_int(&out->mi, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_SS:
				from_char_parse_int(&out->ss, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_MS:		/* millisecond */
				len = from_char_parse_int_len(&out->ms, &s, 3, n);

				/*
				 * 25 is 0.25 and 250 is 0.25 too; 025 is 0.025 and not 0.25
				 */
				out->ms *= len == 1 ? 100 :
					len == 2 ? 10 : 1;

				SKIP_THth(s, n->suffix);
				break;
			case DCH_US:		/* microsecond */
				len = from_char_parse_int_len(&out->us, &s, 6, n);

				out->us *= len == 1 ? 100000 :
					len == 2 ? 10000 :
					len == 3 ? 1000 :
					len == 4 ? 100 :
					len == 5 ? 10 : 1;

				SKIP_THth(s, n->suffix);
				break;
			case DCH_SSSS:
			case DCH_SSSSS:
				from_char_parse_int(&out->ssss, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_tz:
			case DCH_TZ:
			case DCH_OF:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("formatting field \"%s\" is only supported in to_char",
								n->key->name)));
				break;
			case DCH_TZH:
				out->tzsign = *s == '-' ? -1 : +1;

				if (*s == '+' || *s == '-' || *s == ' ')
					s++;

				from_char_parse_int_len(&out->tzh, &s, 2, n);
				break;
			case DCH_TZM:
				/* assign positive timezone sign if TZH was not seen before */
				if (!out->tzsign)
					out->tzsign = +1;
				from_char_parse_int_len(&out->tzm, &s, 2, n);
				break;
			case DCH_A_D:
			case DCH_B_C:
			case DCH_a_d:
			case DCH_b_c:
				from_char_seq_search(&value, &s, adbc_strings_long,
									 ALL_UPPER, n->key->len, n);
				from_char_set_int(&out->bc, value % 2, n);
				break;
			case DCH_AD:
			case DCH_BC:
			case DCH_ad:
			case DCH_bc:
				from_char_seq_search(&value, &s, adbc_strings,
									 ALL_UPPER, n->key->len, n);
				from_char_set_int(&out->bc, value % 2, n);
				break;
			case DCH_MONTH:
			case DCH_Month:
			case DCH_month:
			case DCH_MON:
			case DCH_Mon:
			case DCH_mon:
			case DCH_MM:

				/* Determine which type month belong to */
				if (ORA_MODE && s)
				{
					/* must be MM mode */
					if (isdigit(*s))
					{
						from_char_parse_int(&out->mm, &s, n);
						SKIP_THth(s, n->suffix);
					}
					else
					{
						const int monend = 3;
						int slen = strlen(s);
						char *monend_ptr = NULL;

						if (slen >= monend)
						{
							monend_ptr = s + monend;

							/* must be Month mode */
							if (islower(*monend_ptr) || isupper(*monend_ptr))
							{
								from_char_seq_search(&value, &s, months_full, ONE_UPPER, MAX_MONTH_LEN, n);
								from_char_set_int(&out->mm, value + 1, n);
							}
							else
							{
								from_char_seq_search(&value, &s, months, ONE_UPPER, MAX_MON_LEN, n);
								from_char_set_int(&out->mm, value + 1, n);
							}
						}
						else
						{
							ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT),  errmsg("invalid month value")));
						}
					}
				}
				else
				{
					if (n->key->id == DCH_MM)
					{
						from_char_parse_int(&out->mm, &s, n);
						SKIP_THth(s, n->suffix);
					}
					else if (n->key->id == DCH_MON ||
						n->key->id == DCH_Mon ||
						n->key->id == DCH_mon)
					{
						from_char_seq_search(&value, &s, months, ONE_UPPER, MAX_MON_LEN, n);
						from_char_set_int(&out->mm, value + 1, n);
					}
					else
					{
						from_char_seq_search(&value, &s, months_full, ONE_UPPER, MAX_MONTH_LEN, n);
						from_char_set_int(&out->mm, value + 1, n);
					}
				}
				break;
			case DCH_DAY:
			case DCH_Day:
			case DCH_day:
				from_char_seq_search(&value, &s, days, ONE_UPPER,
									 MAX_DAY_LEN, n);
				from_char_set_int(&out->d, value, n);
				out->d++;
				break;
			case DCH_DY:
			case DCH_Dy:
			case DCH_dy:
				from_char_seq_search(&value, &s, days, ONE_UPPER,
									 MAX_DY_LEN, n);
				from_char_set_int(&out->d, value, n);
				out->d++;
				break;
			case DCH_DDD:
				from_char_parse_int(&out->ddd, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_IDDD:
				from_char_parse_int_len(&out->ddd, &s, 3, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_DD:
				from_char_parse_int(&out->dd, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_D:
				from_char_parse_int(&out->d, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_ID:
				from_char_parse_int_len(&out->d, &s, 1, n);
				/* Shift numbering to match Gregorian where Sunday = 1 */
				if (++out->d > 7)
					out->d = 1;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_WW:
			case DCH_IW:
				from_char_parse_int(&out->ww, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_Q:

				/*
				 * We ignore 'Q' when converting to date because it is unclear
				 * which date in the quarter to use, and some people specify
				 * both quarter and month, so if it was honored it might
				 * conflict with the supplied month. That is also why we don't
				 * throw an error.
				 *
				 * We still parse the source string for an integer, but it
				 * isn't stored anywhere in 'out'.
				 */
				from_char_parse_int((int *) NULL, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_CC:
				from_char_parse_int(&out->cc, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_Y_YYY:
				{
					int			matched,
								years,
								millennia,
								nch;

					matched = sscanf(s, "%d,%03d%n", &millennia, &years, &nch);
					if (matched < 2)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
								 errmsg("invalid input string for \"Y,YYY\"")));
					years += (millennia * 1000);
					from_char_set_int(&out->year, years, n);
					out->yysz = 4;
					s += nch;
					SKIP_THth(s, n->suffix);
				}
				break;
			case DCH_YYYY:
			case DCH_IYYY:
				from_char_parse_int(&out->year, &s, n);
				out->yysz = 4;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_YYY:
			case DCH_IYY:
				if (from_char_parse_int(&out->year, &s, n) < 4)
					out->year = adjust_partial_year_to_2020(out->year);
				out->yysz = 3;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_YY:
			case DCH_IY:
				if (from_char_parse_int(&out->year, &s, n) < 4)
					out->year = adjust_partial_year_to_2020(out->year);
				out->yysz = 2;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_Y:
			case DCH_I:
				if (from_char_parse_int(&out->year, &s, n) < 4)
					out->year = adjust_partial_year_to_2020(out->year);
				out->yysz = 1;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_rr:
			case DCH_RR:
				len = from_char_parse_int_len(&out->year, &s, 4, n);
				out->year = adjust_partial_year_to_2020(out->year);
				out->yysz = len;
				SKIP_THth(s, n->suffix);
				break;
			case DCH_RM:
				from_char_seq_search(&value, &s, rm_months_upper,
									 ALL_UPPER, MAX_RM_LEN, n);
				from_char_set_int(&out->mm, MONTHS_PER_YEAR - value, n);
				break;
			case DCH_rm:
				from_char_seq_search(&value, &s, rm_months_lower,
									 ALL_LOWER, MAX_RM_LEN, n);
				from_char_set_int(&out->mm, MONTHS_PER_YEAR - value, n);
				break;
			case DCH_W:
				from_char_parse_int(&out->w, &s, n);
				SKIP_THth(s, n->suffix);
				break;
			case DCH_J:
				from_char_parse_int(&out->j, &s, n);
				SKIP_THth(s, n->suffix);
				break;
		}
	}

	/*
	 * Just like opentenbase_ora, set default year and month,
	 * Then we can check day, wday and yday.
	 */
	if (ORA_MODE)
	{
		if (out->year == 0 || (out->mm == 0 && out->ddd == 0))
		{
			struct tm tm_s;
			time_t now;

			now = time(NULL);
			localtime_r(&now,&tm_s);

			if (out->year == 0)
				out->year = tm_s.tm_year + 1900;
			if (out->mm == 0 && out->ddd == 0)
				out->mm = tm_s.tm_mon + 1;
		}
	}
}

/* select a DCHCacheEntry to hold the given format picture */
static DCHCacheEntry *
DCH_cache_getnew(const char *str)
{
	DCHCacheEntry *ent;

	/* counter overflow check - paranoia? */
	if (DCHCounter >= (INT_MAX - DCH_CACHE_ENTRIES))
	{
		DCHCounter = 0;

		for (ent = DCHCache; ent < (DCHCache + DCH_CACHE_ENTRIES); ent++)
			ent->age = (++DCHCounter);
	}

	/*
	 * If cache is full, remove oldest entry (or recycle first not-valid one)
	 */
	if (n_DCHCache >= DCH_CACHE_ENTRIES)
	{
		DCHCacheEntry *old = DCHCache + 0;

#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "cache is full (%d)", n_DCHCache);
#endif
		if (old->valid)
		{
			for (ent = DCHCache + 1; ent < (DCHCache + DCH_CACHE_ENTRIES); ent++)
			{
				if (!ent->valid)
				{
					old = ent;
					break;
				}
				if (ent->age < old->age)
					old = ent;
			}
		}
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "OLD: '%s' AGE: %d", old->str, old->age);
#endif
		old->valid = false;
		StrNCpy(old->str, str, DCH_CACHE_SIZE + 1);
		old->age = (++DCHCounter);
		/* caller is expected to fill format, then set valid */
		return old;
	}
	else
	{
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "NEW (%d)", n_DCHCache);
#endif
		ent = DCHCache + n_DCHCache;
		ent->valid = false;
		StrNCpy(ent->str, str, DCH_CACHE_SIZE + 1);
		ent->age = (++DCHCounter);
		/* caller is expected to fill format, then set valid */
		++n_DCHCache;
		return ent;
	}
}

/* look for an existing DCHCacheEntry matching the given format picture */
static DCHCacheEntry *
DCH_cache_search(const char *str)
{
	int			i;
	DCHCacheEntry *ent;

	/* counter overflow check - paranoia? */
	if (DCHCounter >= (INT_MAX - DCH_CACHE_ENTRIES))
	{
		DCHCounter = 0;

		for (ent = DCHCache; ent < (DCHCache + DCH_CACHE_ENTRIES); ent++)
			ent->age = (++DCHCounter);
	}

	for (i = 0, ent = DCHCache; i < n_DCHCache; i++, ent++)
	{
		if (ent->valid && strcmp(ent->str, str) == 0)
		{
			ent->age = (++DCHCounter);
			return ent;
		}
	}

	return NULL;
}

/* Find or create a DCHCacheEntry for the given format picture */
static DCHCacheEntry *
DCH_cache_fetch(const char *str)
{
	DCHCacheEntry *ent;

	if ((ent = DCH_cache_search(str)) == NULL)
	{
		/*
		 * Not in the cache, must run parser and save a new format-picture to
		 * the cache.  Do not mark the cache entry valid until parsing
		 * succeeds.
		 */
		ent = DCH_cache_getnew(str);

		parse_format(ent->format, str, DCH_keywords,
					 DCH_suff, DCH_index, DCH_TYPE, NULL);

		ent->valid = true;
	}
	return ent;
}

/*
 * Format a date/time or interval into a string according to fmt.
 * We parse fmt into a list of FormatNodes.  This is then passed to DCH_to_char
 * for formatting.
 */
static text *
datetime_to_char_body(TmToChar *tmtc, text *fmt, bool is_interval, Oid collid)
{
	FormatNode *format;
	char	   *fmt_str,
			   *result;
	bool		incache;
	int			fmt_len;
	text	   *res;

	/*
	 * Convert fmt to C string
	 */
	fmt_str = text_to_cstring(fmt);
	fmt_len = strlen(fmt_str);

	/*
	 * Allocate workspace for result as C string
	 */
	result = palloc((fmt_len * DCH_MAX_ITEM_SIZ) + 1);
	*result = '\0';

	if (fmt_len > DCH_CACHE_SIZE)
	{
		/*
		 * Allocate new memory if format picture is bigger than static cache
		 * and do not use cache (call parser always)
		 */
		incache = FALSE;

		format = (FormatNode *) palloc((fmt_len + 1) * sizeof(FormatNode));

		parse_format(format, fmt_str, DCH_keywords,
					 DCH_suff, DCH_index, DCH_TYPE, NULL);
	}
	else
	{
		/*
		 * Use cache buffers
		 */
		DCHCacheEntry *ent = DCH_cache_fetch(fmt_str);

		incache = TRUE;
		format = ent->format;
	}

	/* The real work is here */
	DCH_to_char(format, is_interval, tmtc, result, collid);

	if (!incache)
		pfree(format);

	pfree(fmt_str);

	/* convert C-string result to TEXT format */
	res = cstring_to_text(result);

	pfree(result);
	return res;
}

/* replace date format to %.... */
static text *
format_replace(text *fmt)
{
	text *ret;
	StringInfo fmt_new = makeStringInfo();
	uint32_t i = 0;
	uint32_t len = (unsigned)(VARSIZE(fmt) - VARHDRSZ);

	while (i < len)
	{
		bool is_replace = true;
		char ch = ((char *)VARDATA(fmt))[i];

		if (ch == '%' && (i + 1) < len)
		{
			char ch1 = ((char *)VARDATA(fmt))[i+1];
			switch (ch1)
			{
				case 'Y':
					appendStringInfoString(fmt_new, "YYYY");
					break;
				case 'y':
					appendStringInfoString(fmt_new, "YY");
					break;
				case 'M':
					appendStringInfoString(fmt_new, "Month");
					break;
				case 'b':
					appendStringInfoString(fmt_new, "Mon");
					break;
				case 'm':
					appendStringInfoString(fmt_new, "MM");
					break;
				case 'D':
					appendStringInfoString(fmt_new, "DDth");
					break;
				case 'd':
					appendStringInfoString(fmt_new, "DD");
					break;
				case 'H':
				case 'k':
					appendStringInfoString(fmt_new, "HH24");
					break;
				case 'I':
				case 'l':
				case 'h':
					appendStringInfoString(fmt_new, "HH12");
					break;
				case 'i':
					appendStringInfoString(fmt_new, "MI");
					break;
				case 'S':
				case 's':
					appendStringInfoString(fmt_new, "SS");
					break;
				case 'f':
					appendStringInfoString(fmt_new, "US");
					break;
				case 'W':
					appendStringInfoString(fmt_new, "Day");
					break;
				case 'a':
					appendStringInfoString(fmt_new, "Dy");
					break;
				case 'j':
					appendStringInfoString(fmt_new, "DDD");
					break;
				case 'p':
					appendStringInfoString(fmt_new, "AM");
					break;
				case 'r':
					appendStringInfoString(fmt_new, "HH:MI:SS AM");
					break;
				case 'T':
					appendStringInfoString(fmt_new, "HH24:MI:SS");
					break;
				default:
					is_replace = false;
					break;
			}

			if (is_replace)
			{
				i += 2;
				continue;
			}
		}

		appendStringInfoChar(fmt_new, ch);
		i++;
	}

	ret = palloc0(VARHDRSZ + fmt_new->len);
	SET_VARSIZE(ret, VARHDRSZ + fmt_new->len);
	memcpy(VARDATA(ret), (void *)fmt_new->data, fmt_new->len);

	return ret;
}

/****************************************************************************
 *				Public routines
 ***************************************************************************/

/* -------------------
 * TIMESTAMP to_char()
 * -------------------
 */
Datum
timestamp_to_char(PG_FUNCTION_ARGS)
{
	Timestamp	dt = PG_GETARG_TIMESTAMP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1),
			   *res;
	TmToChar	tmtc;
	struct pg_tm *tm;
	int			thisdate;

	if (VARSIZE_ANY_EXHDR(fmt) <= 0 || TIMESTAMP_NOT_FINITE(dt))
		PG_RETURN_NULL();

	if (enable_lightweight_ora_syntax)
		fmt = format_replace(fmt);

	ZERO_tmtc(&tmtc);
	tm = tmtcTm(&tmtc);

	if (timestamp2tm(dt, NULL, tm, &tmtcFsec(&tmtc), NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	thisdate = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
	tm->tm_wday = (thisdate + 1) % 7;
	tm->tm_yday = thisdate - date2j(tm->tm_year, 1, 1) + 1;

	if (!(res = datetime_to_char_body(&tmtc, fmt, false, PG_GET_COLLATION())))
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(res);
}

Datum
timestamptz_to_char(PG_FUNCTION_ARGS)
{
	TimestampTz dt = PG_GETARG_TIMESTAMP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1),
			   *res;
	TmToChar	tmtc;
	int			tz;
	struct pg_tm *tm;
	int			thisdate;

	if (VARSIZE_ANY_EXHDR(fmt) <= 0 || TIMESTAMP_NOT_FINITE(dt))
		PG_RETURN_NULL();

	if (enable_lightweight_ora_syntax)
		fmt = format_replace(fmt);

	ZERO_tmtc(&tmtc);
	tm = tmtcTm(&tmtc);

	if (timestamp2tm(dt, &tz, tm, &tmtcFsec(&tmtc), &tmtcTzn(&tmtc), NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	thisdate = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
	tm->tm_wday = (thisdate + 1) % 7;
	tm->tm_yday = thisdate - date2j(tm->tm_year, 1, 1) + 1;

	if (!(res = datetime_to_char_body(&tmtc, fmt, false, PG_GET_COLLATION())))
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(res);
}

/* -------------------
 * INTERVAL to_char()
 * -------------------
 */
Datum
interval_to_char(PG_FUNCTION_ARGS)
{
	Interval   *it = PG_GETARG_INTERVAL_P(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1),
			   *res;
	TmToChar	tmtc;
	struct pg_tm *tm;

	if (VARSIZE_ANY_EXHDR(fmt) <= 0)
		PG_RETURN_NULL();

	ZERO_tmtc(&tmtc);
	tm = tmtcTm(&tmtc);

	if (interval2tm(*it, tm, &tmtcFsec(&tmtc)) != 0)
		PG_RETURN_NULL();

	/* wday is meaningless, yday approximates the total span in days */
	tm->tm_yday = (tm->tm_year * MONTHS_PER_YEAR + tm->tm_mon) * DAYS_PER_MONTH + tm->tm_mday;


	if (!(res = datetime_to_char_body(&tmtc, fmt, true, PG_GET_COLLATION())))
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(res);
}

/* ---------------------
 * TO_TIMESTAMP()
 *
 * Make Timestamp from date_str which is formatted at argument 'fmt'
 * ( to_timestamp is reverse to_char() )
 * ---------------------
 */
Datum
to_timestamp(PG_FUNCTION_ARGS)
{
	text	   *date_txt = PG_GETARG_TEXT_PP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	Timestamp	result;
	int			tz;
	struct pg_tm tm;
	fsec_t		fsec;

	do_to_timestamp(date_txt, fmt, &tm, &fsec);

	/* Use the specified time zone, if any. */
	if (tm.tm_zone)
	{
		int			dterr = DecodeTimezone((char *) tm.tm_zone, &tz);

		if (dterr)
			DateTimeParseError(dterr, text_to_cstring(date_txt), "timestamptz");
	}
	else
		tz = DetermineTimeZoneOffset(&tm, session_timezone);

	if (tm2timestamp(&tm, fsec, &tz, &result) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMP(result);
}

/* ----------
 * TO_DATE
 *	Make Date from date_str which is formated at argument 'fmt'
 * ----------
 */
Datum
to_date(PG_FUNCTION_ARGS)
{
	text	   *date_txt = PG_GETARG_TEXT_PP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	DateADT		result;
	struct pg_tm tm;
	fsec_t		fsec;

	do_to_timestamp(date_txt, fmt, &tm, &fsec);

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range: \"%s\"",
						text_to_cstring(date_txt))));

	result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

	/* Now check for just-out-of-range dates */
	if (!IS_VALID_DATE(result))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("date out of range: \"%s\"",
						text_to_cstring(date_txt))));

	PG_RETURN_DATEADT(result);
}



/*
 * lightweight_ora, to_date(numeric)
 */
Datum
lightweight_ora_num_to_date(PG_FUNCTION_ARGS)
{
	Numeric num = PG_GETARG_NUMERIC(0);
	text *  fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	Datum   date_text;

	/* get default date format style */
	if (fmt == NULL || PG_ARGISNULL(1))
	{
		if (nls_date_format && strlen(nls_date_format))
		{
			fmt = cstring_to_text(nls_date_format);
		}
		else
		{
			ereport(ERROR,
			        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			         errmsg("invalid fmt, please check guc: nls_date_format")));
		}
	}

	date_text = DirectFunctionCall1(orcl_numeric_tochar, NumericGetDatum(num));
	return DirectFunctionCall2(lightweight_ora_todate, date_text, PointerGetDatum(fmt));
}

Datum
lightweight_ora_todate(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	Datum result;

	/* return null if input txt is null */
	if (!txt)
		PG_RETURN_NULL();
	/* return null if input fmt is null */
	if (!fmt)
		PG_RETURN_NULL();

	/* return null if input txt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	/* return null if input fmt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

	/* it will return timestamp at GMT */
	result = DirectFunctionCall2(to_timestamp,
								 PointerGetDatum(txt),
								 PointerGetDatum(fmt));

	/* convert to local timestamp */
	result = DirectFunctionCall1(timestamptz_timestamp, result);

	PG_RETURN_TIMESTAMP(result);
}


Datum
lightweight_ora_todate_default(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *fmt = NULL;
	Datum result;

	/* return null if input txt is null */
	if (!txt)
		PG_RETURN_NULL();

	/* return null if input txt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	/* get default date format style */
	if (nls_date_format && strlen(nls_date_format))
		fmt = cstring_to_text(nls_date_format);

	if (fmt)
	{
		/* it will return timestamp at GMT */
		result = DirectFunctionCall2(lightweight_ora_todate,
									 PointerGetDatum(txt),
									 PointerGetDatum(fmt));
	}
	else
	{
		result = DirectFunctionCall3(timestamp_in,
									 CStringGetDatum(text_to_cstring(txt)),
									 ObjectIdGetDatum(InvalidOid),
									 Int32GetDatum(0));
	}

	PG_RETURN_TIMESTAMP(result);
}

static Datum
lightweight_ora_totimestamptz(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP(0);
	text *fmt = PG_GETARG_TEXT_PP(1);
	Datum result;

	/* return null if input txt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	/* return null if input fmt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(fmt);

	/* it will return timestamp at GMT */
	result = DirectFunctionCall2(to_timestamp,
								 PointerGetDatum(txt),
								 PointerGetDatum(fmt));

	return result;
}

Datum
lightweight_ora_totimestamptz_default(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP(0);
	text *fmt = NULL;
	Datum result;

	/* return null if input txt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	/* get default timestamptz format style */
	if (nls_timestamp_tz_format && strlen(nls_timestamp_tz_format))
		fmt = cstring_to_text(nls_timestamp_tz_format);

	if (fmt)
	{
		/* it will return timestamp at GMT */
		result = DirectFunctionCall2(lightweight_ora_totimestamptz,
									 PointerGetDatum(txt),
									 PointerGetDatum(fmt));
	}
	else
	{
		result = DirectFunctionCall3(timestamptz_in,
									 PointerGetDatum(txt),
									 ObjectIdGetDatum(InvalidOid),
									 Int32GetDatum(-1));
	}

	return result;
}


Datum
lightweight_ora_totimestamp_default(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP_IF_NULL(0);
	text *fmt = NULL;
	Datum result;
	MemoryContext ctx;

	/* return null if input txt is null */
	if (!txt)
		PG_RETURN_NULL();

	/* return null if input txt is empty */
	PG_RETURN_NULL_IF_EMPTY_TEXT(txt);

	/* get default date format style */
	if (nls_timestamp_format && strlen(nls_timestamp_format))
		fmt = cstring_to_text(nls_timestamp_format);

	ctx = CurrentMemoryContext;
	PG_TRY();
	{
		if (fmt)
		{
			/* it will return timestamp at GMT */
			result = DirectFunctionCall2(to_timestamp, PointerGetDatum(txt), PointerGetDatum(fmt));

			/* convert to local timestamp */
			result = DirectFunctionCall1(timestamptz_timestamp, result);
		}
		else
		{
			result = DirectFunctionCall3(timestamp_in,
										 CStringGetDatum(text_to_cstring(txt)),
										 ObjectIdGetDatum(InvalidOid),
										 Int32GetDatum(-1));
		}
	}
	PG_CATCH();
	{
		char *input_str;
		Datum datum;

		/*
		 * if failing in decoding txt as a timestamp, call float8_timestamptz()
		 * which is the impletation of to_timestamp(float8) deriving from
		 * Postgresql to decode it
		 * 
		 * note that a timestamp is expected here while to_timestamp(float8)
		 * returns a timestamptz, need to do a cast in the end
		 */
		MemoryContextSwitchTo(ctx);
		FlushErrorState();

		input_str = text_to_cstring(txt);
		datum = DirectFunctionCall1(float8in, PointerGetDatum(input_str));
		pfree(input_str);
		result = DirectFunctionCall1(float8_timestamptz, datum);
		/* convert to local timestamp */
		result = DirectFunctionCall1(timestamptz_timestamp, result);
	}
	PG_END_TRY();

	PG_RETURN_TIMESTAMP(result);
}


#define HEXBASE 16

/*
 * In opentenbase_ora, to_char(number, fmt). It converts the number to hex digit string.
 * The fmt must be a string with 'X' and 'x'. If the fmt only includes 'X', the hex digit string
 * shows the results using upper char 'A', 'B', 'C', 'D', 'E', 'F'.
 * If the number is negative, it returns '###...' that consistes the number of fmt_len '#'.
 */
static Datum
int64_to_hex_char(int64 value,  char* fmt, int fmt_len, bool *result_ok)
{
	text			*result = NULL;
	char			*warn_str = NULL;
	char			*ptr = NULL;
	const char		*digits = "0123456789abcdef";
	char			buf[65];		/* bigger than needed, but reasonable */
	char			ch;
	int				i = 0;
	int				zero_cnt = 0;
	bool			is_upper = true;
	bool			is_fm_fmt = false;
	bool			is_hex_fmt = false;
	int				num_len = fmt_len;
	bool			has_invalid_char = false;

	/*
	 * In opentenbase_ora, to_char(value, fmt), the max length of string 'fmt' is 63
	 */
	if (fmt_len > ORA_MAX_FMT_LEN)
		elog(ERROR, "invalid number format model");

	if (strchr(fmt, 'x') == NULL && strchr(fmt, 'X') == NULL)
	{
		*result_ok = false;
		PG_RETURN_VOID();
	}

	*result_ok = true;
	warn_str = palloc0(sizeof(char) * (fmt_len+ 2));
	*warn_str = '#';

	for (i = 0; i < fmt_len; i++)
	{
		/*
		 * The fmt must be a string with 'X' and 'x'.
		 * If the fmt only includes 'X', the hex digit string shows the results using upper
		 * char 'A', 'B', 'C', 'D', 'E', 'F'.
		 */
		switch (fmt[i])
		{
			case 'x':
			case 'X':
				{
					*(warn_str + 1 + i) = '#';
					is_upper = is_upper && ('X' == *(fmt + i));
					is_hex_fmt = true;

					/*
					 * You can precede this element only with 0 (which returns leading
					 * zeroes) or FM. Any other elements return an error. If you specify
					 * neither 0 nor FM with X, then the return always has one leading
					 * blank.
					 */
					if (ORA_MODE || enable_lightweight_ora_syntax)
					{
						if (i == 1 && fmt[0] != '0' && fmt[0] != 'x' && fmt[0] != 'X')
							elog(ERROR, "invalid number format model");

						if (i > 1 && fmt[i - 1] != '0' &&
								fmt[i - 1] != 'x' && fmt[i - 1] != 'X' &&
								fmt[i - 1] != 'm' && fmt[i - 1] != 'M' &&
								fmt[i - 2] != 'f' && fmt[i - 2] != 'F' )
							elog(ERROR, "invalid number format model");
					}
				}
				break;
			case 'f':
			case 'F':
				{
					/* check fmt is fm */
					if (i + 1 < fmt_len && fmt[i+1] != 'm' && fmt[i+1] != 'M')
						elog(ERROR, "invalid number format model");

					if (is_hex_fmt || zero_cnt > 0 || is_fm_fmt)
						elog(ERROR, "invalid number format model");

					i++;
					is_fm_fmt = true;
					num_len = num_len - 2;
				}
				break;
			case '0':
				{
					if (is_hex_fmt)
						elog(ERROR, "invalid number format model");

					zero_cnt ++;
				}
				break;
			default:
				{
					if ((ORA_MODE || enable_lightweight_ora_syntax) && i + 1 < fmt_len &&
							(fmt[i+1] == 'x' || fmt[i+1] == 'X'))
						elog(ERROR, "invalid number format model");

					if (is_hex_fmt)
						elog(ERROR, "invalid number format model");

					has_invalid_char = true;
				}
				break;
		}
	}

	if (has_invalid_char)
	{
		*result_ok = false;
		pfree(warn_str);
		PG_RETURN_VOID();
	}

	/*
	 * In opentenbase_ora, to_char(value, fmt),
	 * if the value is negative, it returns string only including '#'
	 */
	if (value < 0)
	{
		result = cstring_to_text(warn_str);
		PG_RETURN_TEXT_P(result);
	}

	ptr = buf + sizeof(buf) - 1;
	*ptr = '\0';

	/*
	 * In opentenbase_ora, to_char(value, fmt),
	 * Convert the value to hex string,
	 * if the string 'fmt' is all 'X', it converts to upper as 'A'-'F'
	 */
	do
	{
		ch = digits[value % HEXBASE];
		if (is_upper && (ch <= 'f' && ch >= 'a'))
		{
			ch =  toupper(ch);
		}

		*--ptr = ch;
		value /= HEXBASE;
		num_len--;
	} while (ptr > buf && value);

	/*
	 * if the length fmt is less than result that coverted by value,
	 * it returns string only including '#',
	 * otherwise it return the string that converted by value
	 */
	if (num_len < 0)
	{
		result = cstring_to_text(warn_str);
		PG_RETURN_TEXT_P(result);
	}
	else
	{
		/* opentenbase_ora function bugs: 'X', fill sign symbol and space symbol */
		if (ORA_MODE)
		{
			while (num_len > 0 && ptr > buf)
			{
				if (zero_cnt > 0)
					*(--ptr) = '0';
				else if (!is_fm_fmt)
					*(--ptr) = ' ';

				--num_len;
			}

			if (ptr > buf)
			{
				int blank_len = 0;

				if (!is_fm_fmt)
					blank_len = 1;

				while (blank_len > 0 && ptr > buf)
				{
					 *(--ptr) = ' ';
					 --blank_len;
				}
			}
		}
		result = cstring_to_text(ptr);
		PG_RETURN_TEXT_P(result);
	}
}

/*
 * general_do_to_timestamp: same to do_to_timestamp.
 */
void general_do_to_timestamp(char *date_str, char *fmt, struct pg_tm *tm, fsec_t *fsec)
{
	return do_to_timestamp(cstring_to_text(date_str), cstring_to_text(fmt), tm, fsec);
}

/*
 * do_to_timestamp: shared code for to_timestamp and to_date
 *
 * Parse the 'date_txt' according to 'fmt', return results as a struct pg_tm
 * and fractional seconds.
 *
 * We parse 'fmt' into a list of FormatNodes, which is then passed to
 * DCH_from_char to populate a TmFromChar with the parsed contents of
 * 'date_txt'.
 *
 * The TmFromChar is then analysed and converted into the final results in
 * struct 'tm' and 'fsec'.
 */
static void
do_to_timestamp(text *date_txt, text *fmt,
				struct pg_tm *tm, fsec_t *fsec)
{
	FormatNode *format;
	TmFromChar	tmfc;
	int			fmt_len;
	char	   *date_str;
	int			fmask;

	fmt_len = VARSIZE_ANY_EXHDR(fmt);

	/* opentenbase_ora: string to upper in parsing phase */
	if (ORA_MODE)
	{
		date_txt = DatumGetTextP(DirectFunctionCall1Coll(upper, DEFAULT_COLLATION_OID, PointerGetDatum(date_txt)));
		if (fmt_len > 0)
		{
			fmt = DatumGetTextP(DirectFunctionCall1Coll(upper, DEFAULT_COLLATION_OID, PointerGetDatum(fmt)));
		}
	}

	date_str = text_to_cstring(date_txt);

	ZERO_tmfc(&tmfc);
	ZERO_tm(tm);
	*fsec = 0;
	fmask = 0;					/* bit mask for ValidateDate() */

	if (fmt_len)
	{
		char	   *fmt_str;
		bool		incache;

		fmt_str = text_to_cstring(fmt);

		if (fmt_len > DCH_CACHE_SIZE)
		{
			/*
			 * Allocate new memory if format picture is bigger than static
			 * cache and do not use cache (call parser always)
			 */
			incache = FALSE;

			format = (FormatNode *) palloc((fmt_len + 1) * sizeof(FormatNode));

			parse_format(format, fmt_str, DCH_keywords,
						 DCH_suff, DCH_index, DCH_TYPE, NULL);
		}
		else
		{
			/*
			 * Use cache buffers
			 */
			DCHCacheEntry *ent = DCH_cache_fetch(fmt_str);

			incache = TRUE;
			format = ent->format;
		}

#ifdef DEBUG_TO_FROM_CHAR
		/* dump_node(format, fmt_len); */
		/* dump_index(DCH_keywords, DCH_index); */
#endif

		DCH_from_char(format, date_str, &tmfc);

		pfree(fmt_str);
		if (!incache)
			pfree(format);
	}

	DEBUG_TMFC(&tmfc);

	/*
	 * Convert to_date/to_timestamp input fields to standard 'tm'
	 */
	if (tmfc.ssss)
	{
		int			x = tmfc.ssss;

		tm->tm_hour = x / SECS_PER_HOUR;
		x %= SECS_PER_HOUR;
		tm->tm_min = x / SECS_PER_MINUTE;
		x %= SECS_PER_MINUTE;
		tm->tm_sec = x;
	}

#ifdef _PG_ORCL_
    if (ORA_MODE && (tmfc.ssss && (tmfc.ss || tmfc.mi || tmfc.hh)))
    {
        if (tmfc.hh != tm->tm_hour)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                            errmsg("hour conflicts with seconds in day")));
        else if (tmfc.mi != tm->tm_min)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                            errmsg("minutes of hour conflicts with seconds in day")));
        else if (tmfc.ss != tm->tm_sec)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                            errmsg("seconds of minute conflicts with seconds in day")));
    }
#endif

	if (tmfc.ss)
		tm->tm_sec = tmfc.ss;
	if (tmfc.mi)
		tm->tm_min = tmfc.mi;
	if (tmfc.hh)
		tm->tm_hour = tmfc.hh;

	if (tmfc.clock == CLOCK_12_HOUR)
	{
		if (tm->tm_hour < 1 || tm->tm_hour > HOURS_PER_DAY / 2)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
					 errmsg("hour \"%d\" is invalid for the 12-hour clock",
							tm->tm_hour),
					 errhint("Use the 24-hour clock, or give an hour between 1 and 12.")));

		if (tmfc.pm && tm->tm_hour < HOURS_PER_DAY / 2)
			tm->tm_hour += HOURS_PER_DAY / 2;
		else if (!tmfc.pm && tm->tm_hour == HOURS_PER_DAY / 2)
			tm->tm_hour = 0;
	}

	if (tmfc.year)
	{
		/*
		 * If CC and YY (or Y) are provided, use YY as 2 low-order digits for
		 * the year in the given century.  Keep in mind that the 21st century
		 * AD runs from 2001-2100, not 2000-2099; 6th century BC runs from
		 * 600BC to 501BC.
		 */
		if (tmfc.cc && tmfc.yysz <= 2)
		{
			if (tmfc.bc)
				tmfc.cc = -tmfc.cc;
			tm->tm_year = tmfc.year % 100;
			if (tm->tm_year)
			{
				if (tmfc.cc >= 0)
					tm->tm_year += (tmfc.cc - 1) * 100;
				else
					tm->tm_year = (tmfc.cc + 1) * 100 - tm->tm_year + 1;
			}
			else
			{
				/* find century year for dates ending in "00" */
				tm->tm_year = tmfc.cc * 100 + ((tmfc.cc >= 0) ? 0 : 1);
			}
		}
		else
		{
			/* If a 4-digit year is provided, we use that and ignore CC. */
			tm->tm_year = tmfc.year;
			if (tmfc.bc && tm->tm_year > 0)
				tm->tm_year = -(tm->tm_year - 1);
		}
		fmask |= DTK_M(YEAR);
	}
	else if (tmfc.cc)
	{
		/* use first year of century */
		if (tmfc.bc)
			tmfc.cc = -tmfc.cc;
		if (tmfc.cc >= 0)
			/* +1 because 21st century started in 2001 */
			tm->tm_year = (tmfc.cc - 1) * 100 + 1;
		else
			/* +1 because year == 599 is 600 BC */
			tm->tm_year = tmfc.cc * 100 + 1;
		fmask |= DTK_M(YEAR);
	}

	if (tmfc.j)
	{
		j2date(tmfc.j, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
		fmask |= DTK_DATE_M;
	}

	if (tmfc.ww)
	{
		if (tmfc.mode == FROM_CHAR_DATE_ISOWEEK)
		{
			/*
			 * If tmfc.d is not set, then the date is left at the beginning of
			 * the ISO week (Monday).
			 */
			if (tmfc.d)
				isoweekdate2date(tmfc.ww, tmfc.d, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
			else
				isoweek2date(tmfc.ww, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
			fmask |= DTK_DATE_M;
		}
		else
			tmfc.ddd = (tmfc.ww - 1) * 7 + 1;
	}

	if (tmfc.w)
		tmfc.dd = (tmfc.w - 1) * 7 + 1;
	if (tmfc.dd)
	{
		tm->tm_mday = tmfc.dd;
		fmask |= DTK_M(DAY);
	}
	if (tmfc.mm)
	{
		tm->tm_mon = tmfc.mm;
		fmask |= DTK_M(MONTH);
	}

	if (tmfc.ddd && (tm->tm_mon <= 1 || tm->tm_mday <= 1))
	{
		/*
		 * The month and day field have not been set, so we use the
		 * day-of-year field to populate them.  Depending on the date mode,
		 * this field may be interpreted as a Gregorian day-of-year, or an ISO
		 * week date day-of-year.
		 */

		if (!tm->tm_year && !tmfc.bc)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_DATETIME_FORMAT),
					 errmsg("cannot calculate day of year without year information")));

		if (tmfc.mode == FROM_CHAR_DATE_ISOWEEK)
		{
			int			j0;		/* zeroth day of the ISO year, in Julian */

			j0 = isoweek2j(tm->tm_year, 1) - 1;

			j2date(j0 + tmfc.ddd, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
			fmask |= DTK_DATE_M;
		}
		else
		{
			const int  *y;
			int			i;

			static const int ysum[2][13] = {
				{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
			{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366}};

			y = ysum[isleap(tm->tm_year)];

			for (i = 1; i <= MONTHS_PER_YEAR; i++)
			{
				if (tmfc.ddd <= y[i])
					break;
			}
			if (tm->tm_mon <= 1)
				tm->tm_mon = i;

			if (tm->tm_mday <= 1)
				tm->tm_mday = tmfc.ddd - y[i - 1];

			fmask |= DTK_M(MONTH) | DTK_M(DAY);
		}
	}

	if (tmfc.ms)
		*fsec += tmfc.ms * 1000;
	if (tmfc.us)
		*fsec += tmfc.us;

	/* Range-check date fields according to bit mask computed above */
	if (fmask != 0)
	{
		/* We already dealt with AD/BC, so pass isjulian = true */
		int			dterr = ValidateDate(fmask, true, false, false, tm);

		if (dterr != 0)
		{
			/*
			 * Force the error to be DTERR_FIELD_OVERFLOW even if ValidateDate
			 * said DTERR_MD_FIELD_OVERFLOW, because we don't want to print an
			 * irrelevant hint about datestyle.
			 */
			DateTimeParseError(DTERR_FIELD_OVERFLOW, date_str, "timestamp");
		}
	}

	/* Range-check time fields too */
	if (tm->tm_hour < 0 || tm->tm_hour >= HOURS_PER_DAY ||
		tm->tm_min < 0 || tm->tm_min >= MINS_PER_HOUR ||
		tm->tm_sec < 0 || tm->tm_sec >= SECS_PER_MINUTE ||
		*fsec < INT64CONST(0) || *fsec >= USECS_PER_SEC)
		DateTimeParseError(DTERR_FIELD_OVERFLOW, date_str, "timestamp");

	/* Save parsed time-zone into tm->tm_zone if it was specified */
	if (tmfc.tzsign)
	{
		char	   *tz;

		if (tmfc.tzh < 0 || tmfc.tzh > MAX_TZDISP_HOUR ||
			tmfc.tzm < 0 || tmfc.tzm >= MINS_PER_HOUR)
			DateTimeParseError(DTERR_TZDISP_OVERFLOW, date_str, "timestamp");

		tz = palloc(7);

		snprintf(tz, 7, "%c%02d:%02d",
				 tmfc.tzsign > 0 ? '+' : '-', tmfc.tzh, tmfc.tzm);

		tm->tm_zone = tz;
	}

	DEBUG_TM(tm);

	pfree(date_str);
}


/**********************************************************************
 *	the NUMBER version part
 *********************************************************************/


static char *
fill_str(char *str, int c, int max)
{
	memset(str, c, max);
	*(str + max) = '\0';
	return str;
}

#define zeroize_NUM(_n) \
do { \
	(_n)->flag		= 0;	\
	(_n)->lsign		= 0;	\
	(_n)->pre		= 0;	\
	(_n)->post		= 0;	\
	(_n)->pre_lsign_num = 0;	\
	(_n)->need_locale	= 0;	\
	(_n)->multi		= 0;	\
	(_n)->zero_start	= 0;	\
	(_n)->zero_end		= 0;	\
} while(0)

/* select a NUMCacheEntry to hold the given format picture */
static NUMCacheEntry *
NUM_cache_getnew(const char *str)
{
	NUMCacheEntry *ent;

	/* counter overflow check - paranoia? */
	if (NUMCounter >= (INT_MAX - NUM_CACHE_ENTRIES))
	{
		NUMCounter = 0;

		for (ent = NUMCache; ent < (NUMCache + NUM_CACHE_ENTRIES); ent++)
			ent->age = (++NUMCounter);
	}

	/*
	 * If cache is full, remove oldest entry (or recycle first not-valid one)
	 */
	if (n_NUMCache >= NUM_CACHE_ENTRIES)
	{
		NUMCacheEntry *old = NUMCache + 0;

#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "Cache is full (%d)", n_NUMCache);
#endif
		if (old->valid)
		{
			for (ent = NUMCache + 1; ent < (NUMCache + NUM_CACHE_ENTRIES); ent++)
			{
				if (!ent->valid)
				{
					old = ent;
					break;
				}
				if (ent->age < old->age)
					old = ent;
			}
		}
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "OLD: \"%s\" AGE: %d", old->str, old->age);
#endif
		old->valid = false;
		StrNCpy(old->str, str, NUM_CACHE_SIZE + 1);
		old->age = (++NUMCounter);
		/* caller is expected to fill format and Num, then set valid */
		return old;
	}
	else
	{
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "NEW (%d)", n_NUMCache);
#endif
		ent = NUMCache + n_NUMCache;
		ent->valid = false;
		StrNCpy(ent->str, str, NUM_CACHE_SIZE + 1);
		ent->age = (++NUMCounter);
		/* caller is expected to fill format and Num, then set valid */
		++n_NUMCache;
		return ent;
	}
}

/* look for an existing NUMCacheEntry matching the given format picture */
static NUMCacheEntry *
NUM_cache_search(const char *str)
{
	int			i;
	NUMCacheEntry *ent;

	/* counter overflow check - paranoia? */
	if (NUMCounter >= (INT_MAX - NUM_CACHE_ENTRIES))
	{
		NUMCounter = 0;

		for (ent = NUMCache; ent < (NUMCache + NUM_CACHE_ENTRIES); ent++)
			ent->age = (++NUMCounter);
	}

	for (i = 0, ent = NUMCache; i < n_NUMCache; i++, ent++)
	{
		if (ent->valid && strcmp(ent->str, str) == 0)
		{
			ent->age = (++NUMCounter);
			return ent;
		}
	}

	return NULL;
}

/* Find or create a NUMCacheEntry for the given format picture */
static NUMCacheEntry *
NUM_cache_fetch(const char *str)
{
	NUMCacheEntry *ent;

	if ((ent = NUM_cache_search(str)) == NULL)
	{
		/*
		 * Not in the cache, must run parser and save a new format-picture to
		 * the cache.  Do not mark the cache entry valid until parsing
		 * succeeds.
		 */
		ent = NUM_cache_getnew(str);

		zeroize_NUM(&ent->Num);

		parse_format(ent->format, str, NUM_keywords,
					 NULL, NUM_index, NUM_TYPE, &ent->Num);

		ent->valid = true;
	}
	return ent;
}

/* ----------
 * Cache routine for NUM to_char version
 * ----------
 */
static FormatNode *
NUM_cache(int len, NUMDesc *Num, text *pars_str, bool *shouldFree)
{
	FormatNode *format = NULL;
	char	   *str;

	str = text_to_cstring(pars_str);

	if (len > NUM_CACHE_SIZE)
	{
		/*
		 * Allocate new memory if format picture is bigger than static cache
		 * and do not use cache (call parser always)
		 */
		format = (FormatNode *) palloc((len + 1) * sizeof(FormatNode));

		*shouldFree = true;

		zeroize_NUM(Num);

		parse_format(format, str, NUM_keywords,
					 NULL, NUM_index, NUM_TYPE, Num);
	}
	else
	{
		/*
		 * Use cache buffers
		 */
		NUMCacheEntry *ent = NUM_cache_fetch(str);

		*shouldFree = false;

		format = ent->format;

		/*
		 * Copy cache to used struct
		 */
		Num->flag = ent->Num.flag;
		Num->lsign = ent->Num.lsign;
		Num->pre = ent->Num.pre;
		Num->post = ent->Num.post;
		Num->pre_lsign_num = ent->Num.pre_lsign_num;
		Num->need_locale = ent->Num.need_locale;
		Num->multi = ent->Num.multi;
		Num->zero_start = ent->Num.zero_start;
		Num->zero_end = ent->Num.zero_end;
	}

#ifdef DEBUG_TO_FROM_CHAR
	/* dump_node(format, len); */
	dump_index(NUM_keywords, NUM_index);
#endif

	pfree(str);
	return format;
}


static char *
int_to_roman(int number)
{
	int			len = 0,
				num = 0;
	char	   *p = NULL,
			   *result,
				numstr[5];

	result = (char *) palloc(16);
	*result = '\0';

	if (number > 3999 || number < 1)
	{
		fill_str(result, '#', 15);
		return result;
	}
	len = snprintf(numstr, sizeof(numstr), "%d", number);

	for (p = numstr; *p != '\0'; p++, --len)
	{
		num = *p - 49;			/* 48 ascii + 1 */
		if (num < 0)
			continue;

		if (len > 3)
		{
			while (num-- != -1)
				strcat(result, "M");
		}
		else
		{
			if (len == 3)
				strcat(result, rm100[num]);
			else if (len == 2)
				strcat(result, rm10[num]);
			else if (len == 1)
				strcat(result, rm1[num]);
		}
	}
	return result;
}



/* ----------
 * Locale
 * ----------
 */
static void
NUM_prepare_locale(NUMProc *Np)
{
	if (Np->Num->need_locale)
	{
		struct lconv *lconv;

		/*
		 * Get locales
		 */
		lconv = PGLC_localeconv();

		/*
		 * Positive / Negative number sign
		 */
		if (lconv->negative_sign && *lconv->negative_sign)
			Np->L_negative_sign = lconv->negative_sign;
		else
			Np->L_negative_sign = "-";

		if (lconv->positive_sign && *lconv->positive_sign)
			Np->L_positive_sign = lconv->positive_sign;
		else
			Np->L_positive_sign = "+";

		/*
		 * Number decimal point
		 */
		if (lconv->decimal_point && *lconv->decimal_point)
			Np->decimal = lconv->decimal_point;

		else
			Np->decimal = ".";

		if (!IS_LDECIMAL(Np->Num))
			Np->decimal = ".";

		/*
		 * Number thousands separator
		 *
		 * Some locales (e.g. broken glibc pt_BR), have a comma for decimal,
		 * but "" for thousands_sep, so we set the thousands_sep too.
		 * http://archives.postgresql.org/pgsql-hackers/2007-11/msg00772.php
		 */
		if (lconv->thousands_sep && *lconv->thousands_sep)
			Np->L_thousands_sep = lconv->thousands_sep;
		/* Make sure thousands separator doesn't match decimal point symbol. */
		else if (strcmp(Np->decimal, ",") !=0)
			Np->L_thousands_sep = ",";
		else
			Np->L_thousands_sep = ".";

		/*
		 * Currency symbol
		 */
		if (lconv->currency_symbol && *lconv->currency_symbol)
			Np->L_currency_symbol = lconv->currency_symbol;
		else
			Np->L_currency_symbol = (ORA_MODE)? "$" : " ";
	}
	else
	{
		/*
		 * Default values
		 */
		Np->L_negative_sign = "-";
		Np->L_positive_sign = "+";
		Np->decimal = ".";

		Np->L_thousands_sep = ",";
		Np->L_currency_symbol = (ORA_MODE)? "$" : " ";
	}
}

/* ----------
 * Return pointer of last relevant number after decimal point
 *	12.0500 --> last relevant is '5'
 *	12.0000 --> last relevant is '.'
 * If there is no decimal point, return NULL (which will result in same
 * behavior as if FM hadn't been specified).
 * ----------
 */
static char *
get_last_relevant_decnum(char *num)
{
	char	   *result,
			   *p = strchr(num, '.');

#ifdef DEBUG_TO_FROM_CHAR
	elog(DEBUG_elog_output, "get_last_relevant_decnum()");
#endif

	if (!p)
		return NULL;

	result = p;

	while (*(++p))
	{
		if (*p != '0')
			result = p;
	}

	return result;
}

/* ----------
 * Number extraction for TO_NUMBER()
 * ----------
 */
static void
NUM_numpart_from_char(NUMProc *Np, int id, int input_len)
{
	bool		isread = FALSE;

#ifdef DEBUG_TO_FROM_CHAR
	elog(DEBUG_elog_output, " --- scan start --- id=%s",
		 (id == NUM_0 || id == NUM_9) ? "NUM_0/9" : id == NUM_DEC ? "NUM_DEC" : "???");
#endif

/*
 * These macros are used in NUM_processor() and its subsidiary routines.
 * OVERLOAD_TEST: true if we've reached end of input string
 * AMOUNT_TEST(s): true if at least s bytes remain in string
 */
#define OVERLOAD_TEST	(Np->inout_p >= Np->inout + input_len)
#define AMOUNT_TEST(_s) (input_len-(Np->inout_p-Np->inout) >= _s)

	if (OVERLOAD_TEST)
		return;

	if (*Np->inout_p == ' ' && NUM_X != id)
		Np->inout_p++;

	if (OVERLOAD_TEST)
		return;

	/*
	 * read sign before number
	 */
	if (*Np->number == ' ' && (id == NUM_0 || id == NUM_9) &&
		(Np->read_pre + Np->read_post) == 0)
	{
#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "Try read sign (%c), locale positive: %s, negative: %s",
			 *Np->inout_p, Np->L_positive_sign, Np->L_negative_sign);
#endif

		/*
		 * locale sign
		 */
		if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_PRE)
		{
			int			x = 0;

#ifdef DEBUG_TO_FROM_CHAR
			elog(DEBUG_elog_output, "Try read locale pre-sign (%c)", *Np->inout_p);
#endif
			if ((x = strlen(Np->L_negative_sign)) &&
				AMOUNT_TEST(x) &&
				strncmp(Np->inout_p, Np->L_negative_sign, x) == 0)
			{
				Np->inout_p += x;
				*Np->number = '-';
			}
			else if ((x = strlen(Np->L_positive_sign)) &&
					 AMOUNT_TEST(x) &&
					 strncmp(Np->inout_p, Np->L_positive_sign, x) == 0)
			{
				Np->inout_p += x;
				*Np->number = '+';
			}
		}
		else
		{
#ifdef DEBUG_TO_FROM_CHAR
			elog(DEBUG_elog_output, "Try read simple sign (%c)", *Np->inout_p);
#endif

			/*
			 * simple + - < >
			 */
			if (*Np->inout_p == '-' || (IS_BRACKET(Np->Num) &&
										*Np->inout_p == '<'))
			{
				*Np->number = '-';	/* set - */
				Np->inout_p++;
			}
			else if (*Np->inout_p == '+')
			{
				*Np->number = '+';	/* set + */
				Np->inout_p++;
			}
		}
	}

	if (OVERLOAD_TEST)
		return;

#ifdef DEBUG_TO_FROM_CHAR
	elog(DEBUG_elog_output, "Scan for numbers (%c), current number: '%s'", *Np->inout_p, Np->number);
#endif

	if (ORA_MODE)
	{
		if (NUM_X == id &&
			!(isdigit((unsigned char) *Np->inout_p) ||
			 isxdigit((unsigned char) *Np->inout_p)))
		{
			elog(ERROR, "invalid number, contain illegal character:%c", *Np->inout_p);
		}
	}

	/*
	 * read digit or decimal point
	 */
	if (isdigit((unsigned char) *Np->inout_p) ||
	    (ORA_MODE &&
		 (isxdigit((unsigned char) *Np->inout_p) && NUM_X == id)))
	{
		if (Np->read_dec && Np->read_post == Np->Num->post)
			return;

		*Np->number_p = *Np->inout_p;
		Np->number_p++;

		if (Np->read_dec)
			Np->read_post++;
		else
			Np->read_pre++;

		isread = TRUE;

#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "Read digit (%c)", *Np->inout_p);
#endif
	}
	else if (IS_DECIMAL(Np->Num) && Np->read_dec == FALSE)
	{
		/*
		 * We need not test IS_LDECIMAL(Np->Num) explicitly here, because
		 * Np->decimal is always just "." if we don't have a D format token.
		 * So we just unconditionally match to Np->decimal.
		 */
		int			x = strlen(Np->decimal);

#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "Try read decimal point (%c)",
			 *Np->inout_p);
#endif
		if (x && AMOUNT_TEST(x) && strncmp(Np->inout_p, Np->decimal, x) == 0)
		{
			Np->inout_p += x - 1;
			*Np->number_p = '.';
			Np->number_p++;
			Np->read_dec = TRUE;
			isread = TRUE;
		}
	}

	if (OVERLOAD_TEST)
		return;

	/*
	 * Read sign behind "last" number
	 *
	 * We need sign detection because determine exact position of post-sign is
	 * difficult:
	 *
	 * FM9999.9999999S	   -> 123.001- 9.9S			   -> .5- FM9.999999MI ->
	 * 5.01-
	 */
	if (*Np->number == ' ' && Np->read_pre + Np->read_post > 0)
	{
		/*
		 * locale sign (NUM_S) is always anchored behind a last number, if: -
		 * locale sign expected - last read char was NUM_0/9 or NUM_DEC - and
		 * next char is not digit
		 */
		if (IS_LSIGN(Np->Num) && isread &&
			(Np->inout_p + 1) < Np->inout + input_len &&
			!isdigit((unsigned char) *(Np->inout_p + 1)))
		{
			int			x;
			char	   *tmp = Np->inout_p++;

#ifdef DEBUG_TO_FROM_CHAR
			elog(DEBUG_elog_output, "Try read locale post-sign (%c)", *Np->inout_p);
#endif
			if ((x = strlen(Np->L_negative_sign)) &&
				AMOUNT_TEST(x) &&
				strncmp(Np->inout_p, Np->L_negative_sign, x) == 0)
			{
				Np->inout_p += x - 1;	/* -1 .. NUM_processor() do inout_p++ */
				*Np->number = '-';
			}
			else if ((x = strlen(Np->L_positive_sign)) &&
					 AMOUNT_TEST(x) &&
					 strncmp(Np->inout_p, Np->L_positive_sign, x) == 0)
			{
				Np->inout_p += x - 1;	/* -1 .. NUM_processor() do inout_p++ */
				*Np->number = '+';
			}
			if (*Np->number == ' ')
				/* no sign read */
				Np->inout_p = tmp;
		}

		/*
		 * try read non-locale sign, it's happen only if format is not exact
		 * and we cannot determine sign position of MI/PL/SG, an example:
		 *
		 * FM9.999999MI			   -> 5.01-
		 *
		 * if (.... && IS_LSIGN(Np->Num)==FALSE) prevents read wrong formats
		 * like to_number('1 -', '9S') where sign is not anchored to last
		 * number.
		 */
		else if (isread == FALSE && IS_LSIGN(Np->Num) == FALSE &&
				 (IS_PLUS(Np->Num) || IS_MINUS(Np->Num)))
		{
#ifdef DEBUG_TO_FROM_CHAR
			elog(DEBUG_elog_output, "Try read simple post-sign (%c)", *Np->inout_p);
#endif

			/*
			 * simple + -
			 */
			if (*Np->inout_p == '-' || *Np->inout_p == '+')
				/* NUM_processor() do inout_p++ */
				*Np->number = *Np->inout_p;
		}
	}
}

#define IS_PREDEC_SPACE(_n) \
		(IS_ZERO((_n)->Num)==FALSE && \
		 (_n)->number == (_n)->number_p && \
		 *(_n)->number == '0' && \
				 (_n)->Num->post != 0)

/* ----------
 * Add digit or sign to number-string
 * ----------
 */
static void
NUM_numpart_to_char(NUMProc *Np, int id)
{
	int			end;
	char			*ptr = NULL;

	if (IS_ROMAN(Np->Num))
		return;

	/* Note: in this elog() output not set '\0' in 'inout' */

#ifdef DEBUG_TO_FROM_CHAR

	/*
	 * Np->num_curr is number of current item in format-picture, it is not
	 * current position in inout!
	 */
	elog(DEBUG_elog_output,
		 "SIGN_WROTE: %d, CURRENT: %d, NUMBER_P: \"%s\", INOUT: \"%s\"",
		 Np->sign_wrote,
		 Np->num_curr,
		 Np->number_p,
		 Np->inout);
#endif
	Np->num_in = FALSE;

	/*
	 * Write sign if real number will write to output Note: IS_PREDEC_SPACE()
	 * handle "9.9" --> " .1"
	 */
	if ((Np->num_curr >= Np->out_pre_spaces ||
		 (IS_ZERO(Np->Num) && Np->Num->zero_start == Np->num_curr)) &&
		(IS_PREDEC_SPACE(Np) == FALSE ||
		 (Np->last_relevant && *Np->last_relevant == '.')))
	{
		int sign_len = 0;
		if (Np->sign_wrote == FALSE)
		{
			if (IS_LSIGN(Np->Num))
			{
				if (Np->Num->lsign == NUM_LSIGN_PRE)
				{
					if (Np->sign == '-')
						strcpy(Np->inout_p, Np->L_negative_sign);
					/* opentenbase_ora function to_char bug: 'S' invalid fmt fill '#' */
					else if (Np->sign == '#')
						strcpy(Np->inout_p, "#\0");
					else
						strcpy(Np->inout_p, Np->L_positive_sign);
					sign_len = strlen(Np->inout_p);
					Np->inout_p += sign_len;
					Np->sign_wrote = TRUE;
				}
			}
			else if (IS_BRACKET(Np->Num))
			{
				if (ORA_MODE)
					/* opentenbase_ora function to_char bug: 'PR' invalid fmt fill '#' */
					*Np->inout_p = Np->sign == '#' ? '#' : (Np->sign == '+' ? ' ' : '<');
				else
					*Np->inout_p = Np->sign == '+' ? ' ' : '<';
				++Np->inout_p;
				sign_len = 1;
				Np->sign_wrote = TRUE;
			}
			else if (Np->sign == '+')
			{
				if (!IS_FILLMODE(Np->Num))
				{
					*Np->inout_p = ' '; /* Write + */
					++Np->inout_p;
					sign_len = 1;
				}
				Np->sign_wrote = TRUE;
			}
			else if (Np->sign == '-')
			{						/* Write - */
				*Np->inout_p = '-';
				++Np->inout_p;
				sign_len = 1;
				Np->sign_wrote = TRUE;
			}
			/* opentenbase_ora function to_char bug: invalid fmt, fill '#' */
			else if (Np->sign == '#')
			{
				*Np->inout_p = '#';
				++Np->inout_p;
				sign_len = 1;
				Np->sign_wrote = TRUE;
			}
		}

		if (IS_DOLLAR(Np->Num) && !Np->dollar_in)
		{
			/* Write '$' behind sign if needed, and move sign to the left. */
			if (Np->inout + 1 == Np->inout_p)
			{
				*(Np->inout_p) = (Np->sign == '#') ? ('#') : ('$');
				++Np->inout_p;
			}
			else
			{
				if (sign_len && *(Np->inout_p - 1) != ' ')
					memmove(Np->inout_p - sign_len - 1, Np->inout_p - sign_len, sign_len);
				/* opentenbase_ora function to_char bug: '$', invalid fill '#' */
				if (Np->sign == '#')
					*(Np->inout_p - 1) = '#';
				else
					*(Np->inout_p - 1) = '$';
			}
			Np->dollar_in = TRUE;
		}
		else if (ORA_MODE && sign_len == 1)
		{
			/* opentenbase_ora function to_char bug: 'C', fill 'USD' flag */
			if ((ptr = strstr(Np->inout, "USD")))
			{
				memmove(ptr, ptr + 3, Np->inout_p - (ptr + 3));
				if (Np->sign == '#')
					memcpy(Np->inout_p - 3, "###", 3);
				else
					memcpy(Np->inout_p - 3, "USD", 3);
			}
			/* opentenbase_ora function to_char bug: 'L', swap sign symbol and '$' */
			else if ((ptr = strchr(Np->inout, '$')))
			{
				memmove(ptr, ptr + 1, Np->inout_p - (ptr + 1));
				if (Np->sign == '#')
					*(Np->inout_p - 1) = '#';
				else
					*(Np->inout_p - 1) = '$';
			}
		}
	}

	/*
	 * digits / FM / Zero / Dec. point
	 */
	if (id == NUM_9 || id == NUM_0 || id == NUM_D || id == NUM_DEC || id == NUM_$ || id == NUM_C)
	{
		if (Np->num_curr < Np->out_pre_spaces &&
			(Np->Num->zero_start > Np->num_curr || !IS_ZERO(Np->Num)))
		{
			/*
			 * Write blank space
			 */
			if (!IS_FILLMODE(Np->Num))
			{
				*Np->inout_p = ' '; /* Write ' ' */
				++Np->inout_p;
			}
		}
		else if (IS_ZERO(Np->Num) &&
				 Np->num_curr < Np->out_pre_spaces &&
				 Np->Num->zero_start <= Np->num_curr)
		{
			/*
			 * Write ZERO
			 */
			*Np->inout_p = '0'; /* Write '0' */
			++Np->inout_p;
			Np->num_in = TRUE;
		}
		else
		{
			/*
			 * Write Decimal point
			 */
			if (*Np->number_p == '.')
			{
				if (!Np->last_relevant || *Np->last_relevant != '.')
				{
					strcpy(Np->inout_p, Np->decimal);	/* Write DEC/D */
					Np->inout_p += strlen(Np->inout_p);
				}

				/*
				 * Ora 'n' -- FM9.9 --> 'n.'
				 */
				else if (IS_FILLMODE(Np->Num) &&
						 Np->last_relevant && *Np->last_relevant == '.')
				{
					strcpy(Np->inout_p, Np->decimal);	/* Write DEC/D */
					Np->inout_p += strlen(Np->inout_p);
				}
			}
			else
			{
				/*
				 * Write Digits
				 */
				if (Np->last_relevant && Np->number_p > Np->last_relevant &&
					id != NUM_0)
					;

				/*
				 * '0.1' -- 9.9 --> '  .1'
				 */
				else if (IS_PREDEC_SPACE(Np))
				{
					if (!IS_FILLMODE(Np->Num))
					{
						*Np->inout_p = ' ';
						++Np->inout_p;
					}

					/*
					 * '0' -- FM9.9 --> '0.'
					 */
					else if (Np->last_relevant && *Np->last_relevant == '.')
					{
						*Np->inout_p = '0';
						++Np->inout_p;
					}
				}
				else
				{
					if (!(Np->sign == '#' && id == NUM_$))
					{
						*Np->inout_p = *Np->number_p;	/* Write DIGIT */
						++Np->inout_p;
						Np->num_in = TRUE;
					}
				}
			}
			/* do no exceed string length */
			if (*Np->number_p && !(Np->sign == '#' && id == NUM_$))
				++Np->number_p;
		}

		end = Np->num_count + (Np->out_pre_spaces ? 1 : 0) + (IS_DECIMAL(Np->Num) ? 1 : 0);

		if (Np->last_relevant && Np->last_relevant == Np->number_p)
			end = Np->num_curr;

		if (Np->num_curr + 1 == end)
		{
			if (Np->sign_wrote == TRUE && IS_BRACKET(Np->Num))
			{
				if (ORA_MODE)
					/* opentenbase_ora function to_char bug: 'PR' invalid fmt fill '#' */
					*Np->inout_p = Np->sign == '#' ? '#' : (Np->sign == '+' ? ' ' : '>');
				else
					*Np->inout_p = Np->sign == '+' ? ' ' : '>';
				++Np->inout_p;
			}
			else if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_POST)
			{
				if (Np->sign == '-')
					strcpy(Np->inout_p, Np->L_negative_sign);
				/* opentenbase_ora function to_char bug: 'S' invalid fmt fill '#' */
				else if (Np->sign == '#')
					strcpy(Np->inout_p, "#\0");
				else
					strcpy(Np->inout_p, Np->L_positive_sign);
				Np->inout_p += strlen(Np->inout_p);
			}
		}
	}

	++Np->num_curr;
}

/*
 * Skip over "n" input characters, but only if they aren't numeric data
 */
static void
NUM_eat_non_data_chars(NUMProc *Np, int n, int input_len)
{
	while (n-- > 0)
	{
		if (OVERLOAD_TEST)
			break;		/* end of input */
		if (strchr("0123456789.,+-", *Np->inout_p) != NULL)
			break;		/* it's a data character */
		Np->inout_p += pg_mblen(Np->inout_p);
	}
}

static char
extract_digit(char digit)
{
#define SETDIGIT(c)                            \
do                                                  \
{                                                   \
dg = digit - (c);                                   \
} while(0)

	char dg = 0;
	if(digit <= '9' && digit >= '0')
	{
		SETDIGIT('0');
	}
	else if(digit <= 'f' && digit >= 'a')
	{
		SETDIGIT('a');
		dg = dg + 10;
	}
	else if(digit <= 'F' && digit >= 'A')
	{
		SETDIGIT('A');
		dg = dg + 10;
	}
	else if (digit == ' ')
	{
		/* space */
		dg = 0;
	}
	else
	{
		elog(ERROR, "invalid hex character");
	}
	return dg;
#undef SETDIGIT
}

/*
 * a simple explaination:
 * suppose we have hex string: '1e9fba02', the idea is
 * 1 * 16^7 + 14 * 16^6 + ... + 2 * 16^0
 */
static void
hex2dec(char *hex, char *dec_out)
{
	int n;
	char *s;
	Datum mul_res;
	Datum dec_acc;
	Datum base;
	Datum exp;
	Datum digit;
	Datum dec16;
	char *dec_str;
	long   result = 0;
	char   digit_c = 0;
	const int MAX_HEX_LEN = 128;

	n = strlen(hex);
	s = hex;

	if (MAX_HEX_LEN <= n)
	{
		elog(ERROR, "hex string length : %d large than max value(128)", n);
	}
	else if (16 > n)
	{

		while(--n >= 0)
		{
			digit_c = extract_digit(*s);
			/* result = digit * 16 ^ (n-1) */
			result += digit_c * (n == 0 ? 1 :(16L << ((n - 1) * 4)));
			s++;
		}
		sprintf(dec_out, "%ld", result);
		return;
	}
	else
	{
		base = DirectFunctionCall1(int4_numeric,
									Int32GetDatum(16));
		dec_acc = DirectFunctionCall1(int4_numeric,
									Int32GetDatum(0));
		while(--n >= 0)
		{
			exp = DirectFunctionCall1(int4_numeric,
									Int32GetDatum(n));

			digit_c = extract_digit(*s);
			s++;
			if (digit_c != 0)
			{
				digit = DirectFunctionCall1(int4_numeric,
											Int32GetDatum((int)digit_c));
				dec16 = DirectFunctionCall2(numeric_power,
											base,
											exp);
				mul_res = DirectFunctionCall2(numeric_mul,
											digit,
											dec16);
				dec_acc = DirectFunctionCall2(numeric_add,
											mul_res,
											dec_acc);
			}
		}
		dec_str = DatumGetCString(DirectFunctionCall1(numeric_out, dec_acc));
		StrNCpy(dec_out, dec_str, strlen(dec_str) + 1);
	}
	return;
}

static char *
NUM_processor(FormatNode *node, NUMDesc *Num, char *inout,
			  char *number, int input_len, int to_char_out_pre_spaces,
			  int sign, bool is_to_char, Oid collid)
{
	FormatNode *n;
	NUMProc		_Np,
			   *Np = &_Np;
	const char *pattern;
	int         pattern_len;
	bool 	    ishex2dec = false;
	/* opentenbase_ora function to_number bug: calculate fmt */
	char      *input_end = inout + input_len;
	int                i = 0;
	int         c_dollar = 0;  /* -1: start, 0 not exist, 1: end */
	int           c_sign = 0;  /* -1: start, 0 not exist, 1: end */
	bool           c_iso = false;
	bool             c_g = false;
	char      *input_tmp = NULL;
	FormatNode       *n1;
	/* opentenbase_ora function to_char bug: calculate fmt node length */
	int         node_len = 0;
	int         number_i = 0;
	bool			end_c = false;
	bool           end_u = false;
	bool           end_l = false;
	bool           end_s = false;
	int f_count[_NUM_last_] = {0};
	FormatNode    *tnode = node;
	if (ORA_MODE)
	{
		if (node->type == NODE_TYPE_ACTION && node->key->id == NUM_FM)
		{
			tnode->valid = true;
			++tnode;
		}
		for (n = tnode; n->type != NODE_TYPE_END; n++)
		{
			if (n->type != NODE_TYPE_ACTION)
				elog(ERROR, "Invalid number format model");
			n->valid = true;
			++f_count[n->key->id];
			switch (n->key->id)
			{
				case NUM_B:
				{
					if (f_count[n->key->id] > 1)
						elog(ERROR, "Invalid number format model");
					break;
				}
				case NUM_MI:
				case NUM_PR:
				{
					if (f_count[n->key->id] > 1 || (n + 1)->type != NODE_TYPE_END)
						elog(ERROR, "Invalid number format model");

					if (ORA_MODE && n->key->id == NUM_PR && *inout == '-')
						elog(ERROR, "invalid number");
					break;
				}
				case NUM_C:
				{
					if (f_count[n->key->id] > 1 || (n != tnode && (n + 1)->type != NODE_TYPE_END && sign != '#'))
						elog(ERROR, "Invalid number format model");
					if ((n + 1)->type == NODE_TYPE_END)
						end_c = true;
					break;
				}
				case NUM_L:
				{
					if (f_count[n->key->id] > 1 || (n != tnode && (n + 1)->type != NODE_TYPE_END && sign != '#'))
						elog(ERROR, "Invalid number format model");
					if ((n + 1)->type == NODE_TYPE_END)
						end_l = true;
					break;
				}
				case NUM_S:
				{
					if (f_count[n->key->id] > 1 || (n != tnode && (n + 1)->type != NODE_TYPE_END))
						elog(ERROR, "Invalid number format model");
					if (n == tnode)
						Num->lsign = NUM_LSIGN_PRE;
					if ((n + 1)->type == NODE_TYPE_END)
						end_s = true;
					break;
				}
				case NUM_U:
				{
					if (f_count[n->key->id] > 1 || (n != tnode && (n + 1)->type != NODE_TYPE_END && sign != '#'))
						elog(ERROR, "Invalid number format model");
					if ((n + 1)->type == NODE_TYPE_END)
						end_u = true;
					break;
				}
				case NUM_RN:
				case NUM_rn:
				case NUM_V:
				{
					if (f_count[n->key->id] > 1)
						elog(ERROR, "Invalid number format model");
					break;
				}
				case NUM_G:
				{
					if (n == tnode || f_count[NUM_D] > 0)
						elog(ERROR, "Invalid number format model");
					break;
				}
				case NUM_COMMA:
				{
					if (n == tnode || f_count[NUM_DEC] > 0)
						elog(ERROR, "Invalid number format model");
					break;
				}
				case NUM_TME:
				{
					if (!IS_TM(Num))
						elog(ERROR, "Invalid number format model");
					break;
				}
			}
			++node_len;
		}
		/* opentenbase_ora function to_number bug: calculate fmt */
		if (!is_to_char)
		{
			for (i = 0; i < input_len; ++i)
			{
				if (inout[i] == '$')
				{
					if (i == 1 || i == 0)
					{
						c_dollar = -1;
						inout[i] = inout[i - 1];
					}
					else if (i == input_len - 1)
						c_dollar = 1;
				}
				else if (inout[i] == 'U')
				{
					if (i + 2 < input_len && memcmp(inout + i, "USD", 3) == 0)
						c_iso = true;
				}
				else if (inout[i] == ' ' || inout[i] == '-' || inout[i] == '+')
				{
					if (i == 0)
						c_sign = -1;
					else if (i == input_len - 1)
						c_sign = 1;
				}
				else if (inout[i] == ',')
				{
					c_g = true;
				}
			}
		}
	}

	MemSet(Np, 0, sizeof(NUMProc));

	Np->Num = Num;
	Np->is_to_char = is_to_char;
	Np->number = number;
	Np->inout = inout;
	Np->last_relevant = NULL;
	Np->read_post = 0;
	Np->read_pre = 0;
	Np->read_dec = FALSE;
	Np->dollar_in = FALSE;

	if (Np->Num->zero_start)
		--Np->Num->zero_start;

	if (IS_EEEE(Np->Num))
	{
		if (!ORA_MODE && !Np->is_to_char)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"EEEE\" not supported for input")));
		if (Np->is_to_char)
		{
			Np->inout_p = Np->inout;
			if (ORA_MODE)
			{
				/* opentenbase_ora function to_char bug: 'EEEE', only one fmt */
				if ((node + 1)->type == NODE_TYPE_END)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid number format model")));
				/* opentenbase_ora function to_char bug: 'EEEE', fill one space */
				else
					*(Np->inout_p++) = ' ';
			}
			strcpy(Np->inout_p, number);
			return Np->inout;
		}
	}

	/*
	 * Roman correction
	 */
	if (IS_ROMAN(Np->Num))
	{
		if (!Np->is_to_char)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"RN\" not supported for input")));

		Np->Num->lsign = Np->Num->pre_lsign_num = Np->Num->post =
			Np->Num->pre = Np->out_pre_spaces = Np->sign = 0;

		if (IS_FILLMODE(Np->Num))
		{
			Np->Num->flag = 0;
			Np->Num->flag |= NUM_F_FILLMODE;
		}
		else
			Np->Num->flag = 0;
		Np->Num->flag |= NUM_F_ROMAN;
	}

	/*
	 * Sign
	 */
	if (is_to_char)
	{
		Np->sign = sign;

		/* MI/PL/SG - write sign itself and not in number */
		if (IS_PLUS(Np->Num) || IS_MINUS(Np->Num))
		{
			if (IS_PLUS(Np->Num) && IS_MINUS(Np->Num) == FALSE)
				Np->sign_wrote = FALSE; /* need sign */
			else
				Np->sign_wrote = TRUE;	/* needn't sign */
		}
		else
		{
			if (Np->sign != '-')
			{
				if (IS_BRACKET(Np->Num) && IS_FILLMODE(Np->Num))
					Np->Num->flag &= ~NUM_F_BRACKET;
				if (IS_MINUS(Np->Num))
					Np->Num->flag &= ~NUM_F_MINUS;
			}
			else if (Np->sign != '+' && IS_PLUS(Np->Num))
				Np->Num->flag &= ~NUM_F_PLUS;

			if (Np->sign == '+' && IS_FILLMODE(Np->Num) && IS_LSIGN(Np->Num) == FALSE)
				Np->sign_wrote = TRUE;	/* needn't sign */
			else
				Np->sign_wrote = FALSE; /* need sign */

			if (Np->Num->lsign == NUM_LSIGN_PRE && Np->Num->pre == Np->Num->pre_lsign_num)
				Np->Num->lsign = NUM_LSIGN_POST;
		}
	}
	else
		Np->sign = FALSE;

	/*
	 * Count
	 */
	Np->num_count = Np->Num->post + Np->Num->pre - 1;

	if (is_to_char)
	{
		Np->out_pre_spaces = to_char_out_pre_spaces;

		if (IS_FILLMODE(Np->Num) && IS_DECIMAL(Np->Num))
		{
			Np->last_relevant = get_last_relevant_decnum(Np->number);

			/*
			 * If any '0' specifiers are present, make sure we don't strip
			 * those digits.
			 */
			if (Np->last_relevant && Np->Num->zero_end > Np->out_pre_spaces)
			{
				char	   *last_zero;

				last_zero = Np->number + (Np->Num->zero_end - Np->out_pre_spaces);
				if (Np->last_relevant < last_zero)
					Np->last_relevant = last_zero;
			}
		}

		if (Np->sign_wrote == FALSE && Np->out_pre_spaces == 0)
			++Np->num_count;
	}
	else
	{
		Np->out_pre_spaces = 0;
		*Np->number = ' ';		/* sign space */
		*(Np->number + 1) = '\0';
	}

	Np->num_in = 0;
	Np->num_curr = 0;

#ifdef DEBUG_TO_FROM_CHAR
	elog(DEBUG_elog_output,
		 "\n\tSIGN: '%c'\n\tNUM: '%s'\n\tPRE: %d\n\tPOST: %d\n\tNUM_COUNT: %d\n\tNUM_PRE: %d\n\tSIGN_WROTE: %s\n\tZERO: %s\n\tZERO_START: %d\n\tZERO_END: %d\n\tLAST_RELEVANT: %s\n\tBRACKET: %s\n\tPLUS: %s\n\tMINUS: %s\n\tFILLMODE: %s\n\tROMAN: %s\n\tEEEE: %s",
		 Np->sign,
		 Np->number,
		 Np->Num->pre,
		 Np->Num->post,
		 Np->num_count,
		 Np->out_pre_spaces,
		 Np->sign_wrote ? "Yes" : "No",
		 IS_ZERO(Np->Num) ? "Yes" : "No",
		 Np->Num->zero_start,
		 Np->Num->zero_end,
		 Np->last_relevant ? Np->last_relevant : "<not set>",
		 IS_BRACKET(Np->Num) ? "Yes" : "No",
		 IS_PLUS(Np->Num) ? "Yes" : "No",
		 IS_MINUS(Np->Num) ? "Yes" : "No",
		 IS_FILLMODE(Np->Num) ? "Yes" : "No",
		 IS_ROMAN(Np->Num) ? "Yes" : "No",
		 IS_EEEE(Np->Num) ? "Yes" : "No"
		);
#endif

	/*
	 * Locale
	 */
	NUM_prepare_locale(Np);

	/*
	 * Processor direct cycle
	 */
	if (Np->is_to_char)
		Np->number_p = Np->number;
	else
		Np->number_p = Np->number + 1;	/* first char is space for sign */

	Np->inout_p = Np->inout;

	/* opentenbase_ora function to_char bug: fill some spaces */
	if (ORA_MODE && Np->is_to_char)
	{
		if (f_count[NUM_C] > 0)
		{
			if (Np->sign == '#')
				memset(Np->inout_p, '#', 4);
			else
				memset(Np->inout_p, ' ', 4);
			Np->inout_p += 4;
		}
		else if (f_count[NUM_L] > 0 || f_count[NUM_U] > 0)
		{
			pattern = Np->L_currency_symbol;
			if (Np->sign == '#')
				memset(Np->inout_p, '#', ORA_NUM_WIDTH - strlen(pattern));
			else
				memset(Np->inout_p, ' ', ORA_NUM_WIDTH - strlen(pattern));

			Np->inout_p += ORA_NUM_WIDTH - strlen(pattern);
		}
		if ((f_count[NUM_G] > 0 || f_count[NUM_D]) &&
				(f_count[NUM_COMMA] > 0 || f_count[NUM_DEC] > 0))
			elog(ERROR, "Invalid number format model");
	}
	/* opentenbase_ora function to_number bug: remove '$' */
	else if (ORA_MODE && !Np->is_to_char)
	{
		if (f_count[NUM_TM] > 0 || f_count[NUM_V] > 0)
			elog(ERROR, "invalid number");
		if (c_dollar == -1)
			++Np->inout_p;
		else if (c_dollar == 1)
			--input_len;
		input_end = inout + input_len;

		/* opentenbase_ora function to_number bug: the fmt 'G' doesn't match ',' */
		/* eg: to_number('-$123.456', 'L9G9G999D999'), to_number('-$123.456', '9G$9G999D999') */
		if (!c_g && (f_count[NUM_G] > 0 || f_count[NUM_COMMA] > 0))
		{
			n = node;
			n1 = node;
			while (n1->type == NODE_TYPE_ACTION)
			{
				if (n1->key->id == NUM_G || n1->key->id == NUM_COMMA)
					n = n1;
				++n1;
			}
			++n;
			n1 = node;
			while (n1 < n)
			{
				if (n1->key->id == NUM_9 || n1->key->id == NUM_G || n1->key->id == NUM_COMMA)
					n1->valid = false;
				++n1;
			}
		}
	}

	for (n = node; n->type != NODE_TYPE_END; n++)
	{
		/* opentenbase_ora function to_number bug: the fmt need skip some special format node */
		if (ORA_MODE && !Np->is_to_char && !n->valid)
			continue;
		if (!Np->is_to_char)
		{
			/*
			 * Check at least one byte remains to be scanned.  (In actions
			 * below, must use AMOUNT_TEST if we want to read more bytes than
			 * that.)
			 */
			if (OVERLOAD_TEST)
			{
				/* opentenbase_ora function to_number bug: invalid end string */
				if (ORA_MODE && n->type == NODE_TYPE_ACTION)
				{
					if (f_count[NUM_TM] > 0)
						elog(ERROR, "invalid number");
					else if ((end_u || end_l) && (c_dollar == 0 || c_dollar == -1))
						elog(ERROR, "invalid number");
					else if (end_c && c_dollar == -1)
						elog(ERROR, "invalid number");
					else if ((f_count[NUM_MI] > 0 || end_s) && Np->inout_p - 1 >= Np->inout)
					{
						if (*(Np->inout_p - 1) != '-' && *(Np->inout_p - 1) != ' ' && *(Np->inout_p - 1) != '+')
							elog(ERROR, "invalid number");
					}
				}
				break;
			}
		}

		/*
		 * Format pictures actions
		 */
		if (n->type == NODE_TYPE_ACTION)
		{
			/*
			 * Create/read digit/zero/blank/sign/special-case
			 *
			 * 'NUM_S' note: The locale sign is anchored to number and we
			 * read/write it when we work with first or last number
			 * (NUM_0/NUM_9).  This is why NUM_S is missing in switch().
			 *
			 * Notice the "Np->inout_p++" at the bottom of the loop.  This is
			 * why most of the actions advance inout_p one less than you might
			 * expect.  In cases where we don't want that increment to happen,
			 * a switch case ends with "continue" not "break".
			 */
			switch (n->key->id)
			{
				case NUM_9:
				case NUM_0:
				case NUM_DEC:
				case NUM_D:
				case NUM_$: /* only while ORA_MODE is TRUE. */
				case NUM_X:
					ishex2dec = NUM_X == n->key->id ? true : false;
					if (Np->is_to_char)
					{
						if (n->key->id == NUM_$ && node_len == 1)
						{
							*(Np->inout_p++) = (Np->sign == '+') ? (' ') : (Np->sign);
							*(Np->inout_p++) = (Np->sign == '#') ? ('#') : ('$');
							continue;
						}
						NUM_numpart_to_char(Np, n->key->id);
						continue;	/* for() */
					}
					else
					{
						if (ORA_MODE)
						{
							/* oralce function to_number bug: fmt 'G' doesn't match ',' */
							/* eg: to_number('-1,23.456', '999D999') */
							if (*(Np->inout_p) == ',' && (f_count[NUM_G] <= 0 && f_count[NUM_COMMA] <= 0))
								++(Np->inout_p);
							/* opentenbase_ora function to_number bug: fmt '9' corresponds to number */
							if (n->key->id == NUM_9 &&
									*(Np->inout_p) != ' ' &&
									*(Np->inout_p) != '+' &&
									*(Np->inout_p) != '-' &&
									*(Np->inout_p) != '<' &&
									(*(Np->inout_p) < '0' || *(Np->inout_p) > '9'))
								continue;
							/* opentenbase_ora function to_number bug: fmt '$' corresponds to string '$' */
							else if (n->key->id == NUM_$)
							{
								if (c_dollar != -1)
									elog(ERROR, "invalid number");
								continue;
							}
							else if (n->key->id == NUM_D && *(Np->inout_p) != '.')
								elog(ERROR, "invalid number");
						}

						NUM_numpart_from_char(Np, n->key->id, input_len);
						break;	/* switch() case: */
					}

				case NUM_COMMA:
					if (Np->is_to_char)
					{
						if (!Np->num_in)
						{
							/* opentenbase_ora function to_char bug: ',' invalid fmt fill '#' */
							if (node_len == 1)
								elog(ERROR, "Invalid number format model");
							if (IS_FILLMODE(Np->Num))
								continue;
							else
								*Np->inout_p = ' ';
						}
						else
							/* opentenbase_ora function to_char bug: ',' invalid fmt fill '#' */
							if (Np->sign == '#')
								*Np->inout_p = '#';
							else
								*Np->inout_p = ',';
					}
					else
					{
						if (!Np->num_in)
						{
							if (IS_FILLMODE(Np->Num))
								continue;
						}
						if (*Np->inout_p != ',')
						{
							/* opentenbase_ora function to_number bug: ',' invalid fmt throw */
							if (ORA_MODE)
								elog(ERROR, "invalid number");
							else
								continue;
						}
					}
					break;

				case NUM_G:
					pattern = Np->L_thousands_sep;
					pattern_len = strlen(pattern);
					if (Np->is_to_char)
					{
						if (!Np->num_in)
						{
							/* opentenbase_ora function to_char bug: 'G', invalid fmt fiil '#' */
							if (node_len == 1)
								elog(ERROR, "Invalid number format model");
							if (IS_FILLMODE(Np->Num))
								continue;
							else
							{
								/* just in case there are MB chars */
								pattern_len = pg_mbstrlen(pattern);

								memset(Np->inout_p, ' ', pattern_len);
								Np->inout_p += pattern_len - 1;
							}
						}
						else
						{
							/* opentenbase_ora function to_char bug: 'G', invalid fmt fiil '#' */
							if (Np->sign == '#')
								memset(Np->inout_p, '#', pattern_len);
							else
								strcpy(Np->inout_p, pattern);
							Np->inout_p += pattern_len - 1;
						}
					}
					else
					{
						if (!Np->num_in)
						{
							if (IS_FILLMODE(Np->Num))
								continue;
						}
						/* opentenbase_ora function to_number bug: fmt 'G' matched ',' */
						if (ORA_MODE)
						{
							if (*Np->inout_p != ',')
								elog(ERROR, "invalid number");
							else
							{
								++(Np->inout_p);
								continue;
							}
						}
						/*
						 * Because L_thousands_sep typically contains data
						 * characters (either '.' or ','), we can't use
						 * NUM_eat_non_data_chars here.  Instead skip only if
						 * the input matches L_thousands_sep.
						 */
						if (AMOUNT_TEST(pattern_len) &&
							strncmp(Np->inout_p, pattern, pattern_len) == 0)
							Np->inout_p += pattern_len - 1;
						else
							continue;
					}
					break;

				case NUM_L:
					pattern = Np->L_currency_symbol;
					if (Np->is_to_char)
					{
						/* opentenbase_ora function to_char bug: 'L' invalid fmt fill '#' */
						if (node_len == 1)
						{
							*(Np->inout_p++) = (Np->sign == '+') ? (' ') : (Np->sign);
							*(Np->inout_p++) = (Np->sign == '#') ? ('#') : ('$');
							continue;
						}
						strcpy(Np->inout_p, pattern);
						Np->inout_p += strlen(pattern) - 1;
					}
					else
					{
						/* opentenbase_ora function to_number bug: 'L' invalid fmt throw */
						if (ORA_MODE)
						{
							if (c_dollar == 0 || (c_dollar == -1 && n != node) ||
									(c_dollar == 1 && n != node + node_len - 1))
								elog(ERROR, "invalid number");
						}
						NUM_eat_non_data_chars(Np, pg_mbstrlen(pattern), input_len);
						continue;
					}
					break;

				case NUM_RN:
					if (IS_FILLMODE(Np->Num))
					{
						strcpy(Np->inout_p, Np->number_p);
						Np->inout_p += strlen(Np->inout_p) - 1;
					}
					else
					{
						sprintf(Np->inout_p, "%15s", Np->number_p);
						Np->inout_p += strlen(Np->inout_p) - 1;
					}
					break;

				case NUM_rn:
					if (IS_FILLMODE(Np->Num))
					{
						strcpy(Np->inout_p, asc_tolower_z(Np->number_p));
						Np->inout_p += strlen(Np->inout_p) - 1;
					}
					else
					{
						sprintf(Np->inout_p, "%15s", asc_tolower_z(Np->number_p));
						Np->inout_p += strlen(Np->inout_p) - 1;
					}
					break;

				case NUM_th:
					if (IS_ROMAN(Np->Num) || *Np->number == '#' ||
						Np->sign == '-' || IS_DECIMAL(Np->Num))
						continue;

					if (Np->is_to_char)
					{
						strcpy(Np->inout_p, get_th(Np->number, TH_LOWER));
						Np->inout_p += 1;
					}
					else
					{
						/* All variants of 'th' occupy 2 characters */
						NUM_eat_non_data_chars(Np, 2, input_len);
						continue;
					}

					Np->inout_p += 1;
					break;

				case NUM_TH:
					if (IS_ROMAN(Np->Num) || *Np->number == '#' ||
						Np->sign == '-' || IS_DECIMAL(Np->Num))
						continue;

					if (Np->is_to_char)
					{
						strcpy(Np->inout_p, get_th(Np->number, TH_UPPER));
						Np->inout_p += 1;
					}
					else
					{
						/* All variants of 'TH' occupy 2 characters */
						NUM_eat_non_data_chars(Np, 2, input_len);
						continue;
					}

					Np->inout_p += 1;
					break;

				case NUM_MI:
					if (Np->is_to_char)
					{
						if (Np->sign == '-')
							*Np->inout_p = '-';
						/* opentenbase_ora function to_char bug: 'MI', invalid fmt fill '#' */
						else if (Np->sign == '#')
							*Np->inout_p = '#';
						else if (IS_FILLMODE(Np->Num))
							continue;
						else
							*Np->inout_p = ' ';
					}
					else
					{
						/* opentenbase_ora function to_number bug: 'MI' invalid fmt throw */
						if (ORA_MODE && Np->inout <= input_end - 1 &&
								((*(input_end - 1) != ' ' || (*(input_end - 1) == ' ' && *Np->inout == '-'))
								 && *(input_end - 1) != '+' && *(input_end - 1) != '-'))
							elog(ERROR, "invalid number");

						if (*Np->inout_p == '-')
							*Np->number = '-';
						else if (*Np->inout_p == '+')
							*Np->number = ' ';
						else
						{
							NUM_eat_non_data_chars(Np, 1, input_len);
							continue;
						}
					}
					break;

				case NUM_PL:
					if (Np->is_to_char)
					{
						if (Np->sign == '+')
							*Np->inout_p = '+';
						else if (IS_FILLMODE(Np->Num))
							continue;
						else
							*Np->inout_p = ' ';
					}
					else
					{
						if (*Np->inout_p == '+')
							*Np->number = '+';
						else
						{
							NUM_eat_non_data_chars(Np, 1, input_len);
							continue;
						}
					}
					break;

				case NUM_SG:
					if (Np->is_to_char)
						*Np->inout_p = Np->sign;

					else
					{
						if (*Np->inout_p == '-')
							*Np->number = '-';
						else if (*Np->inout_p == '+')
							*Np->number = '+';
						else
						{
							NUM_eat_non_data_chars(Np, 1, input_len);
							continue;
						}
					}
					break;

				/* opentenbase_ora function to_char bug: 'PR', invalid fmt fill '#' */
				case NUM_PR:
				{
					if (Np->is_to_char && node_len == 1)
					{
						if (Np->sign == '#')
							memcpy(Np->inout_p, "##", 2);
						else if (Np->sign == '-')
							memcpy(Np->inout_p, "<>", 2);
						else
							memcpy(Np->inout_p, "  ", 2);
						Np->inout_p += 2;
					}
					else if (ORA_MODE && Np->is_to_char && Np->num_count + 1 == node_len)
					{
						if (Np->sign == '+')
							*(Np->inout_p++) = ' ';
						else if (Np->sign == '-')
							*(Np->inout_p++) = '>';
					}
					else if (ORA_MODE && !Np->is_to_char)
					{
						if (*(Np->inout_p) == ' ')
							elog(ERROR, "invalid number");
						else if (*(Np->inout) == '<' && *(Np->inout_p) == '>')
							++Np->inout_p;
					}
					continue;
				}
				/* opentenbase_ora function to_char bug: 'S' and 'V', invalid fmt fill '#' */
				case NUM_S:
				{
					if (Np->is_to_char)
					{
						if (node_len == 1 || (end_s && *(Np->number) == '.'))
							*(Np->inout_p++) = Np->sign;
					}
					else
					{
						/* opentenbase_ora function to_number bug: 'L' invalid fmt throw */
						if (ORA_MODE && (c_sign == 0 ||
								(c_sign == -1 && n != node) ||
								(c_sign == 1 && n != node + node_len - 1)))
							elog(ERROR, "invalid number");
					}
					continue;
				}
				case NUM_V:
				{
					if (Np->is_to_char && node_len == 1)
						*(Np->inout_p++) = (Np->sign == '+') ? (' ') : (Np->sign);
					continue;
				}
				/* opentenbase_ora function to_char bug: 'B', the zero value should return space */
				case NUM_B:
				{
					if (!ORA_MODE)
						continue;
					if (Np->is_to_char)
					{
						if (node_len == 1)
						{
							*(Np->inout_p++) = (Np->sign == '#') ? ('#') : (' ');
							continue;
						}
						for (number_i = 0; Np->number[number_i] != '\0'; ++number_i)
						{
							if (Np->number[number_i] != '0' && Np->number[number_i] != '.') break;
						}
						if ((Np->number[number_i]) == '\0')
						{
							Np->inout_p = Np->inout;
							memset(Np->inout_p, ' ', node_len);
							Np->inout_p += node_len;
							*(Np->inout_p) = '\0';
							return Np->inout;
						}
					}
					continue;
				}
				/* opentenbase_ora function to_char bug: 'C', fill 'USD' symbol */
				case NUM_C:
				{
					if (!ORA_MODE)
						continue;
					if (Np->is_to_char)
					{
						if (node_len == 1)
							*(Np->inout_p++) = (Np->sign == '+') ? (' ') : (Np->sign);
						if (Np->sign == '#')
						{
							memcpy(Np->inout_p, "###", 3);
							Np->inout_p += 3;
						}
						else
						{
							memcpy(Np->inout_p, "USD", 3);
							Np->inout_p += 3;
						}
					}
					else
					{
						/* opentenbase_ora function to_number bug: fmt 'C' corresponds to string 'USD' */
						if (!c_iso)
							elog(ERROR, "invalid number");
						input_tmp = Np->inout_p;
						if (*(Np->inout_p) == ' ' || *(Np->inout_p) == '-' || *(Np->inout_p) == '+')
						{
							++input_tmp;
							if (input_tmp + 2 >= input_end || memcmp(input_tmp, "USD", 3) != 0)
								elog(ERROR, "invalid number");
							*(input_tmp + 2) = *(Np->inout_p);
							Np->inout_p = input_tmp + 2;
						}
						else
						{
							if (input_tmp + 2 >= input_end || memcmp(input_tmp, "USD", 3) != 0)
								elog(ERROR, "invalid number");
							Np->inout_p += 3;
						}
					}
					continue;
				}
				/* opentenbase_ora function to_char bug: 'U', the new fmt support */
				case NUM_U:
				{
					if (!ORA_MODE)
						elog(ERROR, "Invalid number format model");
					if (Np->is_to_char)
					{
						if (node_len == 1)
						{
							if (Np->sign == '#')
							{
								memcpy(Np->inout_p, "##", 2);
								Np->inout_p += 2;
								continue;
							}
							else
								*(Np->inout_p++) = (Np->sign == '-') ? ('-') : (' ');
						}
						pattern = Np->L_currency_symbol;
						strcpy(Np->inout_p, pattern);
						Np->inout_p += strlen(pattern) - 1;
						break;
					}
					else
					{
						/* opentenbase_ora function to_number bug: 'U' invalid fmt throw */
						if (c_dollar == 0 || (c_dollar == -1 && n != node) ||
								(c_dollar == 1 && n != node + node_len - 1))
							elog(ERROR, "invalid number");
						continue;
					}
				}
				/* opentenbase_ora function to_char bug: 'TM' 'TM9' 'TMe', the new fmt support */
				case NUM_TM:
				{
					if (!ORA_MODE || !Np->is_to_char)
						elog(ERROR, "Invalid number format model");
					if (node_len > 2 || (node_len == 2 && (n + 1)->key->id != NUM_9 && (n + 1)->key->id != NUM_TME))
						elog(ERROR, "Invalid number format model");
					if (*(Np->number_p) == '-')
						*(Np->inout_p++) = *(Np->number_p++);
					if (*(Np->number_p) == '0' && *(Np->number_p + 1) == '.')
						++(Np->number_p);
					strcpy(Np->inout_p, Np->number_p);
					Np->inout_p += strlen(Np->number_p);
					*(Np->inout_p) = '\0';
					return Np->inout;
				}
				/* opentenbase_ora function to_number bug: fmt 'EEEE' support */
				case NUM_E:
				{
					if (ORA_MODE && !Np->is_to_char)
					{
						if (*(Np->inout_p) != 'E')
							elog(ERROR, "invalid number");
						else
							Np->inout_p = input_end;
					}
					continue;
				}

				default:
					continue;
					break;
			}
		}
		else
		{
			if (ORA_MODE)
				elog(ERROR, "Invalid number format model");

			/*
			 * In TO_CHAR, non-pattern characters in the format are copied to
			 * the output.  In TO_NUMBER, we skip one input character for each
			 * non-pattern format character, whether or not it matches the
			 * format character.
			 */
			if (Np->is_to_char)
			{
				strcpy(Np->inout_p, n->character);
				Np->inout_p += strlen(Np->inout_p);
			}
			else
			{
				Np->inout_p += pg_mblen(Np->inout_p);
			}
			continue;
		}
		Np->inout_p++;
	}

	if (Np->is_to_char)
	{
		*Np->inout_p = '\0';
		return Np->inout;
	}
	else
	{
		/* opentenbase_ora function to_number bug: fmt length less than value length */
		if (ORA_MODE && Np->inout_p < input_end)
			elog(ERROR, "invalid number");

		if (*(Np->number_p - 1) == '.')
			*(Np->number_p - 1) = '\0';
		else
			*Np->number_p = '\0';

		/*
		 * Correction - precision of dec. number
		 */
		Np->Num->post = Np->read_post;

		if (ORA_MODE && ishex2dec)
		{
			/* opentenbase_ora report error when have scale number */
			if (Np->Num->post > 0)
			{
				elog(ERROR, "invalid number format, 'x' format cannt't have scale");
			}

			hex2dec(Np->number, Np->number);
			Np->Num->pre = 80;
		}

#ifdef DEBUG_TO_FROM_CHAR
		elog(DEBUG_elog_output, "TO_NUMBER (number): '%s'", Np->number);
#endif
		return Np->number;
	}
}

/* ----------
 * MACRO: Start part of NUM - for all NUM's to_char variants
 *	(sorry, but I hate copy same code - macro is better..)
 * ----------
 */
#define NUM_TOCHAR_prepare \
do { \
	int len = VARSIZE_ANY_EXHDR(fmt); \
	if (len <= 0 || len >= (INT_MAX-VARHDRSZ)/(ORA_MODE ? ORA_NUM_MAX_ITEM_SIZ : NUM_MAX_ITEM_SIZ))		\
		PG_RETURN_TEXT_P(cstring_to_text("")); \
	result	= (text *) palloc0((len * (ORA_MODE ? ORA_NUM_MAX_ITEM_SIZ : NUM_MAX_ITEM_SIZ)) + 1 + VARHDRSZ);	\
	format	= NUM_cache(len, &Num, fmt, &shouldFree);		\
} while (0)

/* ----------
 * MACRO: Finish part of NUM
 * ----------
 */
#define NUM_TOCHAR_finish \
do { \
	int		len; \
									\
	NUM_processor(format, &Num, VARDATA(result), numstr, 0, out_pre_spaces, sign, true, PG_GET_COLLATION()); \
									\
	if (shouldFree)					\
		pfree(format);				\
									\
	/*								\
	 * Convert null-terminated representation of result to standard text. \
	 * The result is usually much bigger than it needs to be, but there \
	 * seems little point in realloc'ing it smaller. \
	 */								\
	len = strlen(VARDATA(result));	\
	SET_VARSIZE(result, len + VARHDRSZ); \
} while (0)

/* -------------------
 * NUMERIC to_number() (convert string to numeric)
 * -------------------
 */
Datum
numeric_to_number(PG_FUNCTION_ARGS)
{
	text	   *value = PG_GETARG_TEXT_PP(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	Datum		result;
	FormatNode *format;
	char	   *numstr;
	bool		shouldFree;
	int         len = 0, val_len = 0;
	int			scale,
				precision;
	char      *findptr = NULL;
	int        findlen = 0;
	char     *digit_ptr = NULL;
	Datum     digit_rlt;
	Numeric      num_10;
	int8              i = 0;
	char *inout = NULL;
	char *inoutsrc = NULL;

	len = VARSIZE_ANY_EXHDR(fmt);

	if (len <= 0 || len >= INT_MAX / NUM_MAX_ITEM_SIZ)
		PG_RETURN_NULL();

	format = NUM_cache(len, &Num, fmt, &shouldFree);

	numstr = (char *) palloc0((len * NUM_MAX_ITEM_SIZ) + 1);

	if (ORA_MODE && format->key && format->key->id == NUM_X)
	{
		val_len = VARSIZE_ANY_EXHDR(value);
		/*
		 * opentenbase_ora report error while value's len large than format's len
		 * ex: select to_number('fff', 'xx') from dual;
		 */
		if (val_len > len)
		{
			elog(ERROR, "invalid number");
		}
		if (len > 63)
		{
			elog(ERROR, "invalid number format model");
		}
	}

	inoutsrc = text_to_cstring(value);
	inout = inoutsrc;

	if (ORA_MODE)
	{
		while (*inout == ' ')
			inout++;
	}
	NUM_processor(format, &Num, inout, numstr, strlen(inout), 0, 0, false, PG_GET_COLLATION());

	scale = Num.post;
	precision = Num.pre + Num.multi + scale;

	if (shouldFree)
		pfree(format);

	result = DirectFunctionCall3(numeric_in,
								 CStringGetDatum(numstr),
								 ObjectIdGetDatum(InvalidOid),
								 Int32GetDatum(((precision << 16) | scale) + VARHDRSZ));
	/* opentenbase_ora function to_number bug: the fmt 'EEEE' support */
	if (ORA_MODE && IS_EEEE(&Num))
	{
		bool    pnum = true;
		char 	*varstr = text_to_cstring(value);

		findptr = strstr(varstr, "E+");
		if (!findptr)
		{
			findptr = strstr(varstr, "E-");
			pnum = false;
		}
		if (!findptr)
			elog(ERROR, "invalid number");

		findptr += 2;
		findlen = strlen(varstr) - (findptr - varstr);
		digit_ptr = (char *) palloc0(findlen + 1);
		memcpy(digit_ptr, findptr, findlen);

		num_10 = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(10)));
		digit_rlt = DirectFunctionCall1(int8in, CStringGetDatum(digit_ptr));

		if (pnum)
		{
			for (i = 0; i < DatumGetInt8(digit_rlt); ++i)
				result = DirectFunctionCall2(numeric_mul, result, NumericGetDatum(num_10));
		}
		else
		{
			for (i = 0; i < DatumGetInt8(digit_rlt); ++i)
				result = DirectFunctionCall2(numeric_div, result, NumericGetDatum(num_10));
		}

		pfree(digit_ptr);
		pfree(varstr);
	}

	if (IS_MULTI(&Num))
	{
		Numeric		x;
		Numeric		a = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
															Int32GetDatum(10)));
		Numeric		b = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
															Int32GetDatum(-Num.multi)));

		x = DatumGetNumeric(DirectFunctionCall2(numeric_power,
												NumericGetDatum(a),
												NumericGetDatum(b)));
		result = DirectFunctionCall2(numeric_mul,
									 result,
									 NumericGetDatum(x));
	}

	pfree(inoutsrc);
	pfree(numstr);
	return result;
}

/*
 * for lightweight_ora/opentenbase_ora, if the format string is of the type "XXX", "xxx" or '',
 * it needs to be converted to hexadecimal format.
 */
static bool
NeedConvertToHex(char *fmt, int len)
{
	if (ORA_MODE)
		return true;

	if (!enable_lightweight_ora_syntax)
		return false;

	while (len > 0 && fmt && *fmt != '\0')
	{
		if (*fmt != 'X' && *fmt != 'x')
		{
			return false;
		}
		fmt++;
		len--;
	}

	return true;
}


bool check_valid_fmt(char *number, FormatNode *node)
{
	int f_count = 0;
	int c_count = 0;
	char   *ptr = number;
	for (; node->type != NODE_TYPE_END; ++node)
	{
		if (node->type == NODE_TYPE_ACTION)
		{
			if (node->key->id == NUM_DEC || node->key->id == NUM_D)
				break;
			if (node->key->id == NUM_9 || node->key->id == NUM_0)
				++f_count;
		}
	}
	if (*ptr == '-')
		++ptr;
	for (; *ptr != '.' && *ptr != '\0'; ++ptr)
		++c_count;
	if (*number == '0' && (*(number + 1) == '\0' || *(number + 1) == '.'))
		return true;
	return f_count >= c_count;
}

bool check_cut_zero(char *number, FormatNode *node)
{
	bool res = false;
	if (strlen(number) >= 2 && *number == '0' && *(number + 1) == '.' &&
			node->type == NODE_TYPE_ACTION &&
			(node->key->id == NUM_DEC ||
			node->key->id == NUM_B ||
			node->key->id == NUM_C ||
			node->key->id == NUM_D ||
			node->key->id == NUM_L ||
			node->key->id == NUM_S ||
			node->key->id == NUM_U))
		res = true;
	return res;
}


/* ------------------
 * NUMERIC to_char()
 * ------------------
 */
Datum
numeric_to_char(PG_FUNCTION_ARGS)
{
	Numeric		value = PG_GETARG_NUMERIC(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *p = NULL;
	Numeric		x;
	int		pre = 0;
	int		post = 0;
	bool		valid = true;
	Datum dr;
	int64 val64 = 0;
	int fmt_len = 0;
	char *fmt_str = NULL;
	bool ok = false;

	/*
	 * It will try to convert numeric to hex digit string according to opentenbase_ora/lightweight_ora  at first.
	 */
	if (!PG_ARGISNULL(1))
	{
		fmt_str = text_to_cstring(fmt);
		fmt_len = VARSIZE_ANY_EXHDR(fmt);

		if (NeedConvertToHex(fmt_str, fmt_len))
		{
			NumericVar	xvar;

			init_var_from_num(value, &xvar);
			if (numericvar_to_int64(&xvar, &val64))
			{
				dr = int64_to_hex_char(val64, fmt_str, fmt_len, &ok);
				if (ok)
				{
					pfree(fmt_str);
					PG_RETURN_DATUM(dr);
				}
			}
		}
		pfree(fmt_str);
	}

	NUM_TOCHAR_prepare;

	/*
	 * On DateType depend part (numeric)
	 */
	if (IS_ROMAN(&Num))
	{
		x = DatumGetNumeric(DirectFunctionCall2(numeric_round,
												NumericGetDatum(value),
												Int32GetDatum(0)));
		numstr = int_to_roman(DatumGetInt32(DirectFunctionCall1(numeric_int4,
																NumericGetDatum(x))));
	}
	else if (IS_EEEE(&Num))
	{
		char *orgnum = numeric_out_sci(value, Num.post);

		/*
		 * numeric_out_sci() does not emit a sign for positive numbers.  We
		 * need to add a space in this case so that positive and negative
		 * numbers are aligned.  We also have to do the right thing for NaN.
		 */
		if (strcmp(orgnum, "NaN") == 0)
		{
			/*
			 * Allow 6 characters for the leading sign, the decimal point,
			 * "e", the exponent's sign and two exponent digits.
			 */
			numstr = (char *) palloc0(Num.pre + Num.post + 7);
			fill_str(numstr, '#', Num.pre + Num.post + 6);
			*numstr = ' ';
			*(numstr + Num.pre + 1) = '.';
		}
		else if (*orgnum != '-')
		{
			numstr = (char *) palloc0(strlen(orgnum) + 2);
			*numstr = ' ';
			strcpy(numstr + 1, orgnum);
		}
		else
		{
			numstr = orgnum;
		}
	}
	/* opentenbase_ora function to_char bug: the fmt 'TM' 'TM9' 'TMe' support */
	else if (ORA_MODE && IS_TM(&Num))
	{
		char *orgnum = DatumGetCString(DirectFunctionCall1(numeric_out, NumericdGetDatum(value)));

		if ((format + 1)->type == NODE_TYPE_ACTION && (format + 1)->key->id == NUM_TME)
		{
			if ((p = strchr(orgnum, '.')))
				post += 1;
			if (*orgnum == '-')
				post += 1;
			post = strlen(orgnum) - post - 1;
			orgnum = numeric_out_sci(value, post);
			if (strcmp(orgnum, "NaN") == 0)
			{
				pre = 1;
				numstr = (char *) palloc(pre + post + 7);
				fill_str(numstr, '#', pre + post + 6);
				*numstr = ' ';
				*(numstr + pre + 1) = '.';
			}
			else
			{
				numstr = orgnum;
			}
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		int			numstr_pre_len;
		char		*orgnum;
		Numeric		val = value;

		if (IS_MULTI(&Num))
		{
			Numeric		a = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
																Int32GetDatum(10)));
			Numeric		b = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
																Int32GetDatum(Num.multi)));

			x = DatumGetNumeric(DirectFunctionCall2(numeric_power,
													NumericGetDatum(a),
													NumericGetDatum(b)));
			val = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
													  NumericGetDatum(value),
													  NumericGetDatum(x)));
			Num.pre += Num.multi;
		}

		x = DatumGetNumeric(DirectFunctionCall2(numeric_round,
												NumericGetDatum(val),
												Int32GetDatum(Num.post)));

		if (ORA_MODE)
		{
			NumericVar	xvar;

			init_var_from_num(x, &xvar);
			if (xvar.weight == 0 && xvar.dscale == 0)
			{
				xvar.dscale = 1;
				x = make_result(&xvar);

				/* If sign is negative, resign it to x in opentenbase_ora mode. */
				if (NUMERIC_SIGN(val) == NUMERIC_NEG)
				{
					if (NUMERIC_IS_SHORT(x))
						x->choice.n_short.n_header = x->choice.n_short.n_header | (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK);
					else
						x->choice.n_long.n_sign_dscale = x->choice.n_long.n_sign_dscale | NUMERIC_NEG;
				}
			}
		}

		orgnum = numeric_out_internal(x);

		if (*orgnum == '-')
		{
			sign = '-';
			numstr = orgnum + 1;
		}
		else
		{
			sign = '+';
			numstr = orgnum;
		}

		if ((p = strchr(numstr, '.')))
			numstr_pre_len = p - numstr;
		else
			numstr_pre_len = strlen(numstr);

		if (ORA_MODE)
		{
			/* check integer significant digits */
			valid = check_valid_fmt(numstr, format);
			/* zero convert to string, eg: -0.123 -> 0, need to set correct sign */
			if (strlen(numstr) == 1 && *(numstr) == '0')
			{
				if (DatumGetBool(DirectFunctionCall1(numeric_lt_zero, NumericGetDatum(value))))
					sign = '-';
			}
			/* convert '0.123' to '.123' */
			if (check_cut_zero(numstr, format))
			{
				++numstr;
				++Num.pre;
			}
		}

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			/* opentenbase_ora function to_char bug: sign symbol changed to '#' */
			if (ORA_MODE && !valid)
			{
				sign = '#';
				numstr = (char *) palloc0(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
			}
			else if (!ORA_MODE)
			{
				numstr = (char *) palloc0(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
				*(numstr + Num.pre) = '.';
			}
		}
		else if (ORA_MODE && !valid)
		{
			sign = '#';
			numstr = (char *) palloc0(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* ---------------
 * INT4 to_char()
 * ---------------
 */
Datum
int4_to_char(PG_FUNCTION_ARGS)
{
	int32		value = PG_GETARG_INT32(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char		*numstr;
	int			post = 0;
	bool		valid = true;
	Datum		dr;
	int64		val64 = 0;
	int			fmt_len = 0;
	char		*fmt_str = NULL;
	bool		ok = false;

	/*
	 * It will try to convert int4 to hex digit string according to opentenbase_ora/lightweight_ora at first.
	 */
	if (!PG_ARGISNULL(1))
	{
		fmt_str = text_to_cstring(fmt);
		fmt_len = VARSIZE_ANY_EXHDR(fmt);

		if (NeedConvertToHex(fmt_str, fmt_len))
		{
			val64 = value;
			dr = int64_to_hex_char(val64, fmt_str, fmt_len, &ok);
			if (ok)
			{
				pfree(fmt_str);
				PG_RETURN_DATUM(dr);
			}
		}
		pfree(fmt_str);
	}

	NUM_TOCHAR_prepare;

	/*
	 * On DateType depend part (int32)
	 */
	if (IS_ROMAN(&Num))
		numstr = int_to_roman(value);
	else if (IS_EEEE(&Num))
	{
		/* we can do it easily because float8 won't lose any precision */
		float8		val = (float8) value;

		numstr = (char *) palloc0(MAXDOUBLEWIDTH + 1);
		snprintf(numstr, MAXDOUBLEWIDTH + 1, "%+.*e", Num.post, val);

		/*
		 * Swap a leading positive sign for a space.
		 */
		if (*numstr == '+')
			*numstr = ' ';
	}
	/* opentenbase_ora function to_char bug: the fmt 'TM' 'TM9' 'TMe' support */
	else if (ORA_MODE && IS_TM(&Num))
	{
		char *orgnum = DatumGetCString(DirectFunctionCall1(int4out, Int32GetDatum(value)));
		if ((format + 1)->type == NODE_TYPE_ACTION && (format + 1)->key->id == NUM_TME)
		{
			post = strlen(orgnum) - ((*orgnum != '-') ? (1) : (2));
			orgnum = (char *) palloc0(MAXDOUBLEWIDTH + 1);
			snprintf(orgnum, MAXDOUBLEWIDTH + 1, "%+.*e", post, (float8) value);
			if (*orgnum == '+')
			{
				numstr = (char *) palloc0(strlen(orgnum));
				strncpy(numstr, orgnum + 1, strlen(orgnum) - 1);
				if (orgnum)
					pfree(orgnum);
			}
			else
			{
				numstr = orgnum;
			}
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		int			numstr_pre_len;
		char		*orgnum;

		if (IS_MULTI(&Num))
		{
			orgnum = DatumGetCString(DirectFunctionCall1(int4out,
														 Int32GetDatum(value * ((int32) pow((double) 10, (double) Num.multi)))));
			Num.pre += Num.multi;
		}
		else
		{
			orgnum = DatumGetCString(DirectFunctionCall1(int4out,
														 Int32GetDatum(value)));
		}

		if (*orgnum == '-')
		{
			sign = '-';
			orgnum++;
		}
		else
			sign = '+';

		numstr_pre_len = strlen(orgnum);

		/* post-decimal digits?  Pad out with zeros. */
		if (Num.post)
		{
			numstr = (char *) palloc(numstr_pre_len + Num.post + 2);
			strcpy(numstr, orgnum);
			*(numstr + numstr_pre_len) = '.';
			memset(numstr + numstr_pre_len + 1, '0', Num.post);
			*(numstr + numstr_pre_len + Num.post + 1) = '\0';
		}
		else
			numstr = orgnum;

		if (ORA_MODE)
			/* check integer significant digits */
			valid = check_valid_fmt(numstr, format);

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			/* opentenbase_ora function to_char bug: sign symbol changed to '#' */
			if (ORA_MODE && !valid)
			{
				sign = '#';
				numstr = (char *) palloc0(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
			}
			else if (!ORA_MODE)
			{
				numstr = (char *) palloc0(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
				*(numstr + Num.pre) = '.';
			}
		}
		else if (ORA_MODE && !valid)
		{
			sign = '#';
			numstr = (char *) palloc0(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* ---------------
 * INT8 to_char()
 * ---------------
 */
Datum
int8_to_char(PG_FUNCTION_ARGS)
{
	int64		value = PG_GETARG_INT64(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr;
	int		post = 0;
	bool		valid = true;
	Datum dr;
	int64 val64 = 0;
	int fmt_len = 0;
	char *fmt_str = NULL;
	bool ok = false;

	/*
	 * It will try to convert int8 to hex digit string according to opentenbase_ora/lightweight_ora at first.
	 */
	if (!PG_ARGISNULL(1))
	{
		fmt_str = text_to_cstring(fmt);
		fmt_len = VARSIZE_ANY_EXHDR(fmt);

		if (NeedConvertToHex(fmt_str, fmt_len))
		{
			val64 = value;
			dr = int64_to_hex_char(val64, fmt_str, fmt_len, &ok);
			if (ok)
			{
				pfree(fmt_str);
				PG_RETURN_DATUM(dr);
			}
		}
		pfree(fmt_str);
	}

	NUM_TOCHAR_prepare;

	/*
	 * On DateType depend part (int32)
	 */
	if (IS_ROMAN(&Num))
	{
		/* Currently don't support int8 conversion to roman... */
		numstr = int_to_roman(DatumGetInt32(DirectFunctionCall1(int84, Int64GetDatum(value))));
	}
	else if (IS_EEEE(&Num))
	{
		/* to avoid loss of precision, must go via numeric not float8 */
		Numeric		val;
		char		*orgnum;

		val = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
												  Int64GetDatum(value)));
		orgnum = numeric_out_sci(val, Num.post);

		/*
		 * numeric_out_sci() does not emit a sign for positive numbers.  We
		 * need to add a space in this case so that positive and negative
		 * numbers are aligned.  We don't have to worry about NaN here.
		 */
		if (*orgnum != '-')
		{
			numstr = (char *) palloc(strlen(orgnum) + 2);
			*numstr = ' ';
			strcpy(numstr + 1, orgnum);
		}
		else
		{
			numstr = orgnum;
		}
	}
	/* opentenbase_ora function to_char bug: the fmt 'TM' 'TM9' 'TMe' support */
	else if (ORA_MODE && IS_TM(&Num))
	{
		char *orgnum = DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(value)));
		if ((format + 1)->type == NODE_TYPE_ACTION && (format + 1)->key->id == NUM_TME)
		{
			post = strlen(orgnum) - ((*orgnum != '-') ? (1) : (2));
			orgnum = numeric_out_sci(DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(value))), post);
			numstr = orgnum;
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		int			numstr_pre_len;
		char		*orgnum;

		if (IS_MULTI(&Num))
		{
			double		multi = pow((double) 10, (double) Num.multi);

			value = DatumGetInt64(DirectFunctionCall2(int8mul,
													  Int64GetDatum(value),
													  DirectFunctionCall1(dtoi8,
																		  Float8GetDatum(multi))));
			Num.pre += Num.multi;
		}

		orgnum = DatumGetCString(DirectFunctionCall1(int8out,
													 Int64GetDatum(value)));

		if (*orgnum == '-')
		{
			sign = '-';
			orgnum++;
		}
		else
			sign = '+';

		numstr_pre_len = strlen(orgnum);

		/* post-decimal digits?  Pad out with zeros. */
		if (Num.post)
		{
			numstr = (char *) palloc(numstr_pre_len + Num.post + 2);
			strcpy(numstr, orgnum);
			*(numstr + numstr_pre_len) = '.';
			memset(numstr + numstr_pre_len + 1, '0', Num.post);
			*(numstr + numstr_pre_len + Num.post + 1) = '\0';
		}
		else
			numstr = orgnum;

		if (ORA_MODE)
			/* check integer significant digits */
			valid = check_valid_fmt(numstr, format);

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			/* opentenbase_ora function to_char bug: sign symbol changed to '#' */
			if (ORA_MODE && !valid)
			{
				sign = '#';
				numstr = (char *) palloc(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
			}
			else if (!ORA_MODE)
			{
				numstr = (char *) palloc(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
				*(numstr + Num.pre) = '.';
			}
		}
		else if (ORA_MODE && !valid)
		{
			sign = '#';
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* -----------------
 * FLOAT4 to_char()
 * -----------------
 */
Datum
float4_to_char(PG_FUNCTION_ARGS)
{
	float4		value = PG_GETARG_FLOAT4(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *p;
	int		pre = 0;
	int		post = 0;
	bool		valid = true;
	Datum dr;
	int64 val64 = 0;
	int fmt_len = 0;
	char *fmt_str = NULL;
	bool ok = false;

	/*
	 * It will try to convert float4 to hex digit string according to opentenbase_ora/lightweight_ora at first.
	 */
	if (!PG_ARGISNULL(1))
	{
		fmt_str = text_to_cstring(fmt);
		fmt_len = VARSIZE_ANY_EXHDR(fmt);

		if (NeedConvertToHex(fmt_str, fmt_len))
		{
			val64 = (int64)rint(value);
			dr = int64_to_hex_char(val64, fmt_str, fmt_len, &ok);
			if (ok)
			{
				pfree(fmt_str);
				PG_RETURN_DATUM(dr);
			}
		}
		pfree(fmt_str);
	}

	NUM_TOCHAR_prepare;

	if (IS_ROMAN(&Num))
		numstr = int_to_roman((int) rint(value));
	else if (IS_EEEE(&Num))
	{
		if (isnan(value) || is_infinite(value))
		{
			/*
			 * Allow 6 characters for the leading sign, the decimal point,
			 * "e", the exponent's sign and two exponent digits.
			 */
			numstr = (char *) palloc0(Num.pre + Num.post + 7);
			fill_str(numstr, '#', Num.pre + Num.post + 6);
			*numstr = ' ';
			*(numstr + Num.pre + 1) = '.';
		}
		else
		{
			numstr = (char *) palloc0(MAXDOUBLEWIDTH + 1);
			snprintf(numstr, MAXDOUBLEWIDTH + 1, "%+.*e", Num.post, value);

			/*
			 * Swap a leading positive sign for a space.
			 */
			if (*numstr == '+')
				*numstr = ' ';
		}
	}
	/* opentenbase_ora function to_char bug: the fmt 'TM' 'TM9' 'TMe' support */
	else if (ORA_MODE && IS_TM(&Num))
	{
		char *orgnum = DatumGetCString(DirectFunctionCall1(float4out, Float4GetDatum(value)));

		if ((format + 1)->type == NODE_TYPE_ACTION && (format + 1)->key->id == NUM_TME)
		{
			if ((p = strchr(orgnum, '.')))
				post += 1;
			if (*orgnum == '-')
				post += 1;
			post = strlen(orgnum) - post - 1;
			if (orgnum)
				pfree(orgnum);

			if (isnan(value) || is_infinite(value))
			{
				pre = 1;
				numstr = (char *) palloc0(pre + post + 7);
				fill_str(numstr, '#', pre + post + 6);
				*numstr = ' ';
				*(numstr + pre + 1) = '.';
			}
			else
			{
				orgnum = (char *) palloc0(MAXDOUBLEWIDTH + 1);
				snprintf(orgnum, MAXDOUBLEWIDTH + 1, "%+.*e", post, value);
				if (*orgnum == '+')
				{
					numstr = (char *) palloc0(strlen(orgnum));
					strncpy(numstr, orgnum + 1, strlen(orgnum) - 1);
					if (orgnum)
						pfree(orgnum);
				}
				else
				{
					numstr = orgnum;
				}
			}
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		float4		val = value;
		char		*orgnum;
		int			numstr_pre_len;

		if (IS_MULTI(&Num))
		{
			float		multi = pow((double) 10, (double) Num.multi);

			val = value * multi;
			Num.pre += Num.multi;
		}

		orgnum = (char *) palloc0(MAXFLOATWIDTH + 1);
		snprintf(orgnum, MAXFLOATWIDTH + 1, "%.0f", fabs(val));
		numstr_pre_len = strlen(orgnum);

		/* adjust post digits to fit max float digits */
		if (numstr_pre_len >= FLT_DIG)
			Num.post = 0;
		else if (numstr_pre_len + Num.post > FLT_DIG)
			Num.post = FLT_DIG - numstr_pre_len;
		snprintf(orgnum, MAXFLOATWIDTH + 1, "%.*f", Num.post, val);

		if (*orgnum == '-')
		{						/* < 0 */
			sign = '-';
			numstr = orgnum + 1;
		}
		else
		{
			sign = '+';
			numstr = orgnum;
		}

		if ((p = strchr(numstr, '.')))
			numstr_pre_len = p - numstr;
		else
			numstr_pre_len = strlen(numstr);

		if (ORA_MODE)
		{
			/* check integer significant digits */
			valid = check_valid_fmt(numstr, format);
			/* zero convert to string, eg: -0.123 -> 0, need to set correct sign */
			if (strlen(numstr) == 1 && *(numstr) == '0')
			{
				if (DatumGetBool(DirectFunctionCall2(float4lt, Float4GetDatum(value), Float4GetDatum(0.0))))
					sign = '-';
			}
			/* convert '0.123' to '.123' */
			if (check_cut_zero(numstr, format))
				++numstr;
		}

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			/* opentenbase_ora function to_char bug: sign symbol changed to '#' */
			if (ORA_MODE && !valid)
			{
				sign = '#';
				numstr = (char *) palloc(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
			}
			else if (ORA_MODE)
			{
				numstr = (char *) palloc(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
				*(numstr + Num.pre) = '.';
			}
		}
		else if (ORA_MODE && !valid)
		{
			sign = '#';
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* -----------------
 * FLOAT8 to_char()
 * -----------------
 */
Datum
float8_to_char(PG_FUNCTION_ARGS)
{
	float8		value = PG_GETARG_FLOAT8(0);
	text	   *fmt = PG_GETARG_TEXT_PP(1);
	NUMDesc		Num;
	FormatNode *format;
	text	   *result;
	bool		shouldFree;
	int			out_pre_spaces = 0,
				sign = 0;
	char	   *numstr,
			   *p;
	int		pre = 0;
	int		post = 0;
	bool		valid = true;
	Datum dr;
	int64 val64 = 0;
	int fmt_len = 0;
	char *fmt_str = NULL;
	bool ok = false;

	/*
	 * It will try to convert float8 to hex digit string according to opentenbase_ora/lightweight_ora at first.
	 */
	if (!PG_ARGISNULL(1))
	{
		fmt_str = text_to_cstring(fmt);
		fmt_len = VARSIZE_ANY_EXHDR(fmt);

		if (NeedConvertToHex(fmt_str, fmt_len))
		{
			val64 = (int64)rint(value);
			dr = int64_to_hex_char(val64, fmt_str, fmt_len, &ok);
			if (ok)
			{
				pfree(fmt_str);
				PG_RETURN_DATUM(dr);
			}
		}
		pfree(fmt_str);
	}

	NUM_TOCHAR_prepare;

	if (IS_ROMAN(&Num))
		numstr = int_to_roman((int) rint(value));
	else if (IS_EEEE(&Num))
	{
		if (isnan(value) || is_infinite(value))
		{
			/*
			 * Allow 6 characters for the leading sign, the decimal point,
			 * "e", the exponent's sign and two exponent digits.
			 */
			numstr = (char *) palloc0(Num.pre + Num.post + 7);
			fill_str(numstr, '#', Num.pre + Num.post + 6);
			*numstr = ' ';
			*(numstr + Num.pre + 1) = '.';
		}
		else
		{
			numstr = (char *) palloc0(MAXDOUBLEWIDTH + 1);
			snprintf(numstr, MAXDOUBLEWIDTH + 1, "%+.*e", Num.post, value);

			/*
			 * Swap a leading positive sign for a space.
			 */
			if (*numstr == '+')
				*numstr = ' ';
		}
	}
	/* opentenbase_ora function to_char bug: the fmt 'TM' 'TM9' 'TMe' support */
	else if (ORA_MODE && IS_TM(&Num))
	{
		char *orgnum = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(value)));

		if ((format + 1)->type == NODE_TYPE_ACTION && (format + 1)->key->id == NUM_TME)
		{
			if ((p = strchr(orgnum, '.')))
				post += 1;
			if (*orgnum == '-')
				post += 1;
			post = strlen(orgnum) - post - 1;
			if (isnan(value) || is_infinite(value))
			{
				pre = 1;
				numstr = (char *) palloc(pre + post + 7);
				fill_str(numstr, '#', pre + post + 6);
				*numstr = ' ';
				*(numstr + pre + 1) = '.';
			}
			else
			{
				orgnum = (char *) palloc0(MAXDOUBLEWIDTH + 1);
				snprintf(orgnum, MAXDOUBLEWIDTH + 1, "%+.*e", post, value);
				if (*orgnum == '+')
				{
					numstr = (char *) palloc0(strlen(orgnum));
					strncpy(numstr, orgnum + 1, strlen(orgnum) - 1);
					if (orgnum)
						pfree(orgnum);
				}
				else
				{
					numstr = orgnum;
				}
			}
		}
		else
		{
			numstr = orgnum;
		}
	}
	else
	{
		float8		val = value;
		char		*orgnum;
		int			numstr_pre_len;

		if (IS_MULTI(&Num))
		{
			double		multi = pow((double) 10, (double) Num.multi);

			val = value * multi;
			Num.pre += Num.multi;
		}
		orgnum = (char *) palloc0(MAXDOUBLEWIDTH + 1);
		numstr_pre_len = snprintf(orgnum, MAXDOUBLEWIDTH + 1, "%.0f", fabs(val));

		/* adjust post digits to fit max double digits */
		if (numstr_pre_len >= DBL_DIG)
			Num.post = 0;
		else if (numstr_pre_len + Num.post > DBL_DIG)
			Num.post = DBL_DIG - numstr_pre_len;
		snprintf(orgnum, MAXDOUBLEWIDTH + 1, "%.*f", Num.post, val);

		if (*orgnum == '-')
		{						/* < 0 */
			sign = '-';
			numstr = orgnum + 1;
		}
		else
		{
			sign = '+';
			numstr = orgnum;
		}

		if ((p = strchr(numstr, '.')))
			numstr_pre_len = p - numstr;
		else
			numstr_pre_len = strlen(numstr);

		if (ORA_MODE)
		{
			/* check integer significant digits */
			valid = check_valid_fmt(numstr, format);
			/* zero convert to string, eg: -0.123 -> 0, need to set correct sign */
			if (strlen(numstr) == 1 && *(numstr) == '0')
			{
				if (DatumGetBool(DirectFunctionCall2(float8lt, Float8GetDatum(value), Float8GetDatum(0.0))))
					sign = '-';
			}
			/* convert '0.123' to '.123' */
			if (check_cut_zero(numstr, format))
				++numstr;
		}

		/* needs padding? */
		if (numstr_pre_len < Num.pre)
			out_pre_spaces = Num.pre - numstr_pre_len;
		/* overflowed prefix digit format? */
		else if (numstr_pre_len > Num.pre)
		{
			/* opentenbase_ora function to_char bug: sign symbol changed to '#' */
			if (ORA_MODE && !valid)
			{
				sign = '#';
				numstr = (char *) palloc(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
			}
			else if (!ORA_MODE)
			{
				numstr = (char *) palloc(Num.pre + Num.post + 2);
				fill_str(numstr, '#', Num.pre + Num.post + 1);
				*(numstr + Num.pre) = '.';
			}
		}
		else if (ORA_MODE && !valid)
		{
			sign = '#';
			numstr = (char *) palloc(Num.pre + Num.post + 2);
			fill_str(numstr, '#', Num.pre + Num.post + 1);
		}
	}

	NUM_TOCHAR_finish;
	PG_RETURN_TEXT_P(result);
}

/* NLS parameters analyze support */
typedef enum
{
	NLS_INVALID,
	NLS_NUMERIC_CHARACTERS,
	NLS_CURRENCY,
	NLS_ISO_CURRENCY,
	NLS_DUAL_CURRENCY,
	NLS_DATE_LANGUAGE
} NLSType;

typedef struct NLSNode
{
	NLSType type;
	char *value;
	bool valid;
	int position;  /* -1: prev, 0: middle, 1: post */
	bool alloc;
} NLSNode;

typedef struct NLSDesc
{
	int flag;
} NLSDesc;

char NLS_DEFAULT_NUMERIC_CHARACTERS[] = ".,";
char NLS_DEFAULT_CURRENCY[] = "$";
char NLS_DEFAULT_ISO_CURRENCY[] = "USD";
char NLS_DEFAULT_DUAL_CURRENCY[] = "$";
char NLS_DEFAULT_DATE_LANGUAGE[] = "American";

const NLSNode NLSDefaultValue[] = {
	{ NLS_INVALID, NULL, false, 0, false },
	{ NLS_NUMERIC_CHARACTERS, NLS_DEFAULT_NUMERIC_CHARACTERS, false, 0, false },
	{ NLS_CURRENCY, NLS_DEFAULT_CURRENCY, false, -1, false },
	{ NLS_ISO_CURRENCY, NLS_DEFAULT_ISO_CURRENCY, false, -1, false },
	{ NLS_DUAL_CURRENCY, NLS_DEFAULT_DUAL_CURRENCY, false, -1, false },
	{ NLS_DATE_LANGUAGE, NLS_DEFAULT_DATE_LANGUAGE, false, -1, false }
};

#define NLS_F_NUMERIC_CHARACTERS		(1 << 1)
#define NLS_F_CURRENCY				(1 << 2)
#define NLS_F_ISO_CURRENCY			(1 << 3)
#define NLS_F_DUAL_CURRENCY			(1 << 4)
#define NLS_F_DATE_LANGUAGE			(1 << 5)

#define IS_NLS_NUMERIC_CHARACTERS(desc)		(((desc)->flag & NLS_F_NUMERIC_CHARACTERS) >> 1)
#define IS_NLS_CURRENCY(desc)			(((desc)->flag & NLS_F_CURRENCY) >> 2)
#define IS_NLS_ISO_CURRENCY(desc)		(((desc)->flag & NLS_F_ISO_CURRENCY) >> 3)
#define IS_NLS_DUAL_CURRENCY(desc)		(((desc)->flag & NLS_F_DUAL_CURRENCY) >> 4)
#define IS_NLS_DATE_LANGUAGE(desc)		(((desc)->flag & NLS_F_DATE_LANGUAGE) >> 5)

#define SET_NLS_NUMERIC_CHARACTERS(desc)	((desc)->flag |= NLS_F_NUMERIC_CHARACTERS)
#define SET_NLS_CURRENCY(desc)			((desc)->flag |= NLS_F_CURRENCY)
#define SET_NLS_ISO_CURRENCY(desc)		((desc)->flag |= NLS_F_ISO_CURRENCY)
#define SET_NLS_DUAL_CURRENCY(desc)		((desc)->flag |= NLS_F_DUAL_CURRENCY)
#define SET_NLS_DATE_LANGUAGE(desc)		((desc)->flag |= NLS_F_DATE_LANGUAGE)

static void
nls_parser_upper(char *ptr, int len)
{
	int i = 0;
	for (i = 0; i < len; ++i)
	{
		if (ptr[i] >= 0x61 && ptr[i] <= 0x7A)
			ptr[i] -= 32;
	}
}

static char *
nls_parser_key(char *ptr, char *end, int *len)
{
	char *s;
	while (ptr <= end && (*ptr) == ' ')
		++ptr;
	if (ptr > end)
		return NULL;
	s = ptr;
	while (ptr <= end)
	{
		if (*ptr == ' ' || *ptr == '=')
			break;
		++ptr;
	}
	if (ptr > end)
		elog(ERROR, "invalid NLS parameter string used in SQL function");
	*len = ptr - s;
	return s;
}

static char *
nls_parser_value(char *ptr, char *end, int *len)
{
	char *s;
	while (ptr <= end && (*ptr) == ' ')
		++ptr;
	if (ptr > end)
		elog(ERROR, "invalid NLS parameter string used in SQL function");
	if (*ptr == '\'')
	{
		s = (++ptr);
		while (ptr <= end && *ptr != '\'')
			++ptr;
	}
	else
	{
		s = ptr;
		while (ptr <= end && *ptr != ' ')
			++ptr;
	}
	*len = ptr - s;
	return s;
}

static void
nls_parameters_valid(NLSNode *nlsNodes, int nlsLen, NLSDesc *nlsDesc, text *fmt)
{
	FormatNode *format;
	FormatNode   *node;
	NUMDesc	       Num;
	bool    shouldFree;
	int len = VARSIZE_ANY_EXHDR(fmt);
	if (len <= 0 || len >= (INT_MAX - VARHDRSZ) / NUM_MAX_ITEM_SIZ)
		return;
	format	= NUM_cache(len, &Num, fmt, &shouldFree);
	for (node = format; node->type != NODE_TYPE_END; node++)
	{
		if (node->type != NODE_TYPE_ACTION)
			elog(ERROR, "Invalid number format model");
		switch (node->key->id)
		{
			case NUM_D:
			case NUM_G:
			{
				if (IS_NLS_NUMERIC_CHARACTERS(nlsDesc))
					nlsNodes[NLS_NUMERIC_CHARACTERS].valid = true;
				break;
			}
			case NUM_L:
			{
				if (IS_NLS_CURRENCY(nlsDesc))
				{
					nlsNodes[NLS_CURRENCY].valid = true;
					if (node != format)
						nlsNodes[NLS_CURRENCY].position = 1;
				}
				break;
			}
			case NUM_C:
			{
				if (IS_NLS_ISO_CURRENCY(nlsDesc))
				{
					nlsNodes[NLS_ISO_CURRENCY].valid = true;
					if (node != format)
						nlsNodes[NLS_CURRENCY].position = 1;
				}
				break;
			}
			case NUM_U:
			{
				if (IS_NLS_DUAL_CURRENCY(nlsDesc))
				{
					nlsNodes[NLS_DUAL_CURRENCY].valid = true;
					if (node != format)
						nlsNodes[NLS_CURRENCY].position = 1;
				}
				break;
			}
			default:
			{
				break;
			}
		}
	}
}

static void
nls_parameters_parser(NLSNode *nlsNodes, int nlsLen, NLSDesc *nlsDesc, text *nlsStr)
{
	char     *ptr = NULL;
	char     *end = NULL;
	char *key_ptr = NULL;
	char *val_ptr = NULL;
	int   key_len = 0;
	int   val_len = 0;
	int         i = 0;
	int      mlen = 0;
	text    *tnls = NULL;
	char    *cnls = NULL;
	text    *tsrc = cstring_to_text("\n");
	text    *tdst = cstring_to_text(" ");
	nlsDesc->flag = 0;
	/* replace '\n' in nls parameters */
	tnls = DatumGetTextP(DirectFunctionCall3(replace_text,
						PointerGetDatum(nlsStr),
						PointerGetDatum(tsrc),
						PointerGetDatum(tdst)));
	cnls = text_to_cstring(tnls);
	ptr = cnls;
	end = ptr + strlen(ptr) - 1;
	while (ptr <= end)
	{
		key_ptr = nls_parser_key(ptr, end, &key_len);
		if (key_ptr == NULL)
		{
			if (i == 0)
				elog(ERROR, "invalid NLS parameter string used in SQL function");
			else
				break;
		}
		ptr = key_ptr + key_len;
		while (ptr <= end && (*ptr) == ' ')
			++ptr;
		if (ptr > end || (*ptr) != '=')
			elog(ERROR, "invalid NLS parameter string used in SQL function");
		val_ptr = nls_parser_value(ptr + 1, end, &val_len);
		if (val_len <= 0)
			elog(ERROR, "invalid NLS parameter string used in SQL function");
		nls_parser_upper(key_ptr, key_len);
		if (key_len == 12)
		{
			if (memcmp(key_ptr, "NLS_CURRENCY", key_len) != 0 ||
					IS_NLS_CURRENCY(nlsDesc))
				elog(ERROR, "invalid NLS parameter string used in SQL function");
			i = (int) NLS_CURRENCY;
			Assert(i < nlsLen);
			mlen = (val_len < 10) ? (val_len) : (10);
			nlsNodes[i].type = NLS_CURRENCY;
			nlsNodes[i].value = (char *) palloc(mlen + 1);
			memcpy(nlsNodes[i].value, val_ptr, mlen);
			nlsNodes[i].value[mlen] = '\0';
			nlsNodes[i].alloc = true;
			SET_NLS_CURRENCY(nlsDesc);
		}
		else if (key_len == 16)
		{
			if (memcmp(key_ptr, "NLS_ISO_CURRENCY", key_len) != 0 ||
					IS_NLS_ISO_CURRENCY(nlsDesc))
				elog(ERROR, "invalid NLS parameter string used in SQL function");
			i = (int) NLS_ISO_CURRENCY;
			Assert(i < nlsLen);
			mlen = (val_len < 7) ? (val_len) : (7);
			nlsNodes[i].type = NLS_ISO_CURRENCY;
			nlsNodes[i].value = (char *) palloc(mlen + 1);
			memcpy(nlsNodes[i].value, val_ptr, mlen);
			nlsNodes[i].value[mlen] = '\0';
			nlsNodes[i].alloc = true;
			SET_NLS_ISO_CURRENCY(nlsDesc);
		}
		else if (key_len == 17)
		{
			if(memcmp(key_ptr, "NLS_DUAL_CURRENCY", key_len) == 0)
			{
                if (IS_NLS_DUAL_CURRENCY(nlsDesc))
                    elog(ERROR, "invalid NLS parameter string used in SQL function");
				i = (int) NLS_DUAL_CURRENCY;
				Assert(i < nlsLen);
				mlen = (val_len < 10) ? (val_len) : (10);
				nlsNodes[i].type = NLS_DUAL_CURRENCY;
				nlsNodes[i].value = (char *) palloc(mlen + 1);
				memcpy(nlsNodes[i].value, val_ptr, mlen);
				nlsNodes[i].value[mlen] = '\0';
				nlsNodes[i].alloc = true;
				SET_NLS_DUAL_CURRENCY(nlsDesc);
			}
			else if (memcmp(key_ptr, "NLS_DATE_LANGUAGE", key_len) == 0)
			{
				if (IS_NLS_DATE_LANGUAGE(nlsDesc))
					elog(ERROR, "invalid NLS parameter string used in SQL function");
				i = (int) NLS_DATE_LANGUAGE;
				Assert(i < nlsLen);
				nlsNodes[i].type = NLS_DATE_LANGUAGE;
				nlsNodes[i].value = (char *) palloc(val_len + 1);
				memcpy(nlsNodes[i].value, val_ptr, val_len);
				nlsNodes[i].value[val_len] = '\0';
				nlsNodes[i].alloc = true;
				SET_NLS_DATE_LANGUAGE(nlsDesc);
			}
			else
				elog(ERROR, "invalid NLS parameter string used in SQL function");
		}
		else if (key_len == 22)
		{
			if (memcmp(key_ptr, "NLS_NUMERIC_CHARACTERS", key_len) != 0 ||
					IS_NLS_NUMERIC_CHARACTERS(nlsDesc) ||
					val_len < 2)
				elog(ERROR, "invalid NLS parameter string used in SQL function");
			i = (int) NLS_NUMERIC_CHARACTERS;
			Assert(i < nlsLen);
			nlsNodes[i].type = NLS_NUMERIC_CHARACTERS;
			nlsNodes[i].value = (char *) palloc(val_len + 1);
			memcpy(nlsNodes[i].value, val_ptr, val_len);
			nlsNodes[i].value[val_len] = '\0';
			nlsNodes[i].alloc = true;
			SET_NLS_NUMERIC_CHARACTERS(nlsDesc);
		}
		else
			elog(ERROR, "invalid NLS parameter string used in SQL function");
		ptr = val_ptr + val_len;
		while (ptr <= end && *ptr == '\'')
			++ptr;
	}
	if (tsrc)
		pfree(tsrc);
	if (tdst)
		pfree(tdst);
	if (cnls)
		pfree(cnls);
}


int 
convert_check_nls_date_language(const char* arg, const char* fmt, text* nls, bool is_parse)
{
	int cmp_ret = 0;
	
	/* todo: Check whether the format of value and fmt is correct according to nls_date_language*/

	return cmp_ret;
}

Datum
numeric_to_char_nls(PG_FUNCTION_ARGS)
{
	text  *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text  *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	text  *nls = PG_GETARG_TEXT_PP_IF_NULL(2);
	char *carg = NULL;
	int      i = 0;
	int   nlen = sizeof(NLSDefaultValue) / sizeof(NLSDefaultValue[0]);
	char *data = NULL;
	int   dlen = 0;
	char *dptr = NULL;
	int   tlen = 0;
	text  *res = NULL;
	char fmtstr[2] = {0, 0};
	NLSNode nodes[nlen];
	NLSDesc desc;

	Assert(arg && fmt && nls);

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nls);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	carg = text_to_cstring(arg);

	for (i = 0; i < nlen; ++i)
		nodes[i] = NLSDefaultValue[i];

	nls_parameters_parser((NLSNode *)nodes, nlen, &desc, nls);
	nls_parameters_valid((NLSNode *)nodes, nlen, &desc, fmt);

	dlen = strlen(carg);
	data = (char *) palloc0(dlen + 1);
	dptr = data + dlen - 1;

	for (i = dlen - 1; i >= 0; --i)
	{
		if (dptr < data)
			break;
		if (carg[i] == '$')
		{
			if (nodes[NLS_CURRENCY].valid)
			{
				tlen = strlen(nodes[NLS_CURRENCY].value);
				dptr -= tlen - 1;
				if (dptr < data)
					elog(ERROR, "numeric_to_char_nls invalid dptr pointer.");
				memcpy(dptr--, nodes[NLS_CURRENCY].value, tlen);
			}
			else if (nodes[NLS_DUAL_CURRENCY].valid)
			{
				tlen = strlen(nodes[NLS_DUAL_CURRENCY].value);
				dptr -= tlen - 1;
				if (dptr < data)
					elog(ERROR, "numeric_to_char_nls invalid dptr pointer.");
				memcpy(dptr--, nodes[NLS_DUAL_CURRENCY].value, tlen);
			}
			else
				*(dptr--) = carg[i];
		}
		else if (carg[i] == ',')
		{
			if (nodes[NLS_NUMERIC_CHARACTERS].valid && strlen(nodes[NLS_NUMERIC_CHARACTERS].value) >= 2)
				*(dptr--) = nodes[NLS_NUMERIC_CHARACTERS].value[1];
			else
				*(dptr--) = carg[i];
		}
		else if (carg[i] == '.')
		{
			if (nodes[NLS_NUMERIC_CHARACTERS].valid && strlen(nodes[NLS_NUMERIC_CHARACTERS].value) >= 1)
				*(dptr--) = nodes[NLS_NUMERIC_CHARACTERS].value[0];
			else
				*(dptr--) = carg[i];
		}
		else if (i - 2 >= 0 && memcmp(&carg[i - 2], "USD", 3) == 0)
		{
			if (nodes[NLS_ISO_CURRENCY].valid)
				/* opentenbase_ora 19 throw error */
				elog(ERROR, "invalid NLS parameter string used in SQL function");
			else
				*(dptr--) = carg[i];
		}
		else
			*(dptr--) = carg[i];
	}
	res = cstring_to_text(data);
	for (i = 0; i < nlen; ++i)
	{
		if (nodes[i].alloc)
			pfree(nodes[i].value);
	}
	if (carg)
		pfree(carg);
	if (data)
		pfree(data);
	return PointerGetDatum(res);
}

Datum
numeric_to_number_nls(PG_FUNCTION_ARGS)
{
	text  *arg = PG_GETARG_TEXT_PP_IF_NULL(0);
	text  *fmt = PG_GETARG_TEXT_PP_IF_NULL(1);
	text  *nls = PG_GETARG_TEXT_PP_IF_NULL(2);
	char *carg = NULL;
	int      i = 0;
	int   dlen = 0;
	char *dptr = NULL;
	char *pptr = NULL;
	char *eptr = NULL;
	int   nlen = sizeof(NLSDefaultValue) / sizeof(NLSDefaultValue[0]);
	text  *res = NULL;
	char fmtstr[2] = {0, 0};
	NLSNode nodes[nlen];
	NLSDesc desc;
	NLSNode *node;

	Assert(arg && fmt && nls);

	PG_RETURN_NULL_IF_EMPTY_TEXT(arg);
	PG_RETURN_NULL_IF_EMPTY_TEXT(nls);

	text_to_cstring_buffer(fmt, fmtstr, 2);
	if (fmtstr[0] == '\0')
		elog(ERROR, "invalid number");

	carg = text_to_cstring(arg);
	eptr = carg + strlen(carg) - 1;

	for (i = 0; i < nlen; ++i)
		nodes[i] = NLSDefaultValue[i];

	nls_parameters_parser((NLSNode *)nodes, nlen, &desc, nls);
	nls_parameters_valid((NLSNode *)nodes, nlen, &desc, fmt);

	/* nls paramater: NLS_CURRENCY */
	node = &nodes[NLS_CURRENCY];
	if (node->valid)
	{
		if (node->position == -1)
			dptr = strstr(carg, node->value);
		else
		{
			pptr = strstr(carg, node->value);
			dptr = pptr;
			while (pptr && pptr <= eptr)
			{
				dptr = pptr;
				pptr = strstr(pptr + 1, node->value);
			}
		}
		if (!dptr)
			elog(ERROR, "invalid number");
		dlen = strlen(NLSDefaultValue[NLS_CURRENCY].value);
		memcpy(dptr, NLSDefaultValue[NLS_CURRENCY].value, dlen);
		if (dlen != strlen(node->value))
		{
			memmove(dptr + dlen, dptr + strlen(node->value), strlen(dptr + strlen(node->value)) + 1);
			eptr = carg + strlen(carg) - 1;
		}
	}
	/* nls paramater: NLS_ISO_CURRENCY */
	node = &nodes[NLS_ISO_CURRENCY];
	/* In opentenbase_ora mode, this parameter throw error */
	if (node->valid)
		elog(ERROR, "invalid NLS parameter string used in SQL function");
	/* nls paramater: NLS_DUAL_CURRENCY */
	node = &nodes[NLS_DUAL_CURRENCY];
	if (node->valid)
	{
		if (strlen(node->value) != strlen(NLSDefaultValue[NLS_DUAL_CURRENCY].value))
			elog(ERROR, "invalid number");
		else if (memcmp(node->value, NLSDefaultValue[NLS_DUAL_CURRENCY].value, strlen(node->value)))
			elog(ERROR, "invalid number");
	}
	/* nls paramater: NLS_NUMERIC_CHARACTERS */
	node = &nodes[NLS_NUMERIC_CHARACTERS];
	if (node->valid)
	{
		if (strlen(node->value) > 0)
		{
			while (eptr >= carg && *eptr != node->value[0])
				--eptr;
			if (eptr >= carg)
				*eptr = NLSDefaultValue[NLS_NUMERIC_CHARACTERS].value[0];
			--eptr;
		}
		if (strlen(node->value) > 1)
		{
			while (eptr >= carg)
			{
				if (*eptr == node->value[1])
					*eptr = NLSDefaultValue[NLS_NUMERIC_CHARACTERS].value[1];
				--eptr;
			}
		}
	}

	res = cstring_to_text(carg);
	for (i = 0; i < nlen; ++i)
	{
		if (nodes[i].alloc)
			pfree(nodes[i].value);
	}
	if (carg)
		pfree(carg);
	return PointerGetDatum(res);
}
