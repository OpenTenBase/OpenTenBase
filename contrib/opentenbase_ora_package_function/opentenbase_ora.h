#ifndef __ORAFCE__
#define __ORAFCE__

#include "postgres.h"
#include "catalog/catversion.h"
#include "nodes/pg_list.h"
#include <sys/time.h>
#include "utils/datetime.h"
#include "utils/datum.h"

#define TextPCopy(t) \
	DatumGetTextP(datumCopy(PointerGetDatum(t), false, -1))

#define PG_GETARG_IF_EXISTS(n, type, defval) \
	((PG_NARGS() > (n) && !PG_ARGISNULL(n)) ? PG_GETARG_##type(n) : (defval))

#define PG_RETURN_NULL_IF_EMPTY_TEXT(_in) \
	do { \
		if (_in != NULL) \
		{ \
			char _instr[2] = {0, 0}; \
			text_to_cstring_buffer(_in, _instr, 2); \
			if (_instr[0] == '\0') \
				PG_RETURN_NULL(); \
		} \
	} while (0)

/* alignment of this struct must fit for all types */
typedef union vardata
{
	char	c;
	short	s;
	int		i;
	long	l;
	float	f;
	double	d;
	void   *p;
} vardata;

extern int ora_instr(text *txt, text *pattern, int start, int nth, bool in_byte);
extern int ora_mb_strlen(text *str, char **sizes, int **positions);
extern int ora_mb_strlen1(text *str);

extern char *nls_date_format;
extern char *orafce_timezone;
extern char *nls_timestamp_format;
extern char *nls_timestamp_tz_format;
extern char *nls_date_language;

extern bool orafce_varchar2_null_safe_concat;

/*
 * Version compatibility
 */

extern Oid	equality_oper_funcid(Oid argtype);

/*
 * Date utils
 */
#define STRING_PTR_FIELD_TYPE const char *const

extern STRING_PTR_FIELD_TYPE ora_days[];

extern int ora_seq_search(const char *name, STRING_PTR_FIELD_TYPE array[], size_t max);

#ifdef _MSC_VER

#define int2size(v)			(size_t)(v)
#define size2int(v)			(int)(v)

#else

#define int2size(v)			v
#define size2int(v)			v

#endif

#endif
