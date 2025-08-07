/* -----------------------------------------------------------------------
 * formatting.h
 *
 * src/include/utils/formatting.h
 *
 *
 *	 Portions Copyright (c) 1999-2017, PostgreSQL Global Development Group
 *
 *	 The PostgreSQL routines for a DateTime/int/float/numeric formatting,
 *	 inspire with opentenbase_ora TO_CHAR() / TO_DATE() / TO_NUMBER() routines.
 *
 *	 Karel Zak
 *
 * -----------------------------------------------------------------------
 */

#ifndef _FORMATTING_H_
#define _FORMATTING_H_

#include "fmgr.h"


extern char *str_tolower(const char *buff, size_t nbytes, Oid collid);
extern char *str_toupper(const char *buff, size_t nbytes, Oid collid);
extern char *str_initcap(const char *buff, size_t nbytes, Oid collid);

extern char *asc_tolower(const char *buff, size_t nbytes);
extern char *asc_toupper(const char *buff, size_t nbytes);
extern char *asc_initcap(const char *buff, size_t nbytes);

#ifdef USE_ICU
extern char* ToUpperDBEncodeUseICU(const char* buff, size_t nbytes, Oid collid);
#endif
extern char* ToUpperForRaw(const char* buff, size_t nbytes, Oid collid);
extern int convert_check_nls_date_language(const char* arg, const char* fmt, text* nls, bool is_parse);

#define DCH_FMT_NOSUPPORT(name)												\
	do {																	\
		ereport(ERROR,														\
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),					\
				 errmsg("formatting field \"%s\" is not supported.", name)));\
	} while(0);

#define ORA_MAX_FMT_LEN 63
#endif
