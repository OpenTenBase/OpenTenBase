/*-------------------------------------------------------------------------
 *
 * version.c
 *	 Returns the PostgreSQL version string
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Copyright (c) 1998-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"

Datum
pgsql_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
}

#ifdef __OPENTENBASE__
Datum
opentenbase_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PACKAGE_STRING));
}
#endif
