/*-------------------------------------------------------------------------
 *
 * long.c
 *	  Functions for the variable-length built-in types.
 *
 * Portions Copyright (c) 2023, Tencent Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/long.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include "access/hash.h"
#include "access/tuptoaster.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "common/md5.h"
#include "lib/hyperloglog.h"
#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "port/pg_bswap.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/sortsupport.h"
#include "utils/formatting.h"
#include "utils/varlena.h"
#include "utils/array.h"

/*
 * input interface of long type
 */
Datum
longin(PG_FUNCTION_ARGS)
{
	return textin(fcinfo);
}

/*
 * output interface of long type
 */
Datum
longout(PG_FUNCTION_ARGS)
{
    return textout(fcinfo);
}

Datum
longrecv(PG_FUNCTION_ARGS)
{
	return textrecv(fcinfo);
}

Datum
longsend(PG_FUNCTION_ARGS)
{
    return textsend(fcinfo);
}

Datum
long2text(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Do not support long to text");
	PG_RETURN_TEXT_P(NULL);
}