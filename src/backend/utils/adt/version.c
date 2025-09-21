/*-------------------------------------------------------------------------
 *
 * version.c
 *     Returns the PostgreSQL version string
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Copyright (c) 1998-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"


#define OPENTENBASE_VERSION_STR "OpenTenBase_V2.6.0_release"

Datum
pgsql_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR));
}

#ifdef __OPENTENBASE__
Datum
opentenbase_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(OPENTENBASE_VERSION_STR));
}
#endif
