/*-------------------------------------------------------------------------
 *
 * opentenbase_mysql_compat.h
 *		Shared declarations for the opentenbase_mysql_compat extension.
 *
 * Portions Copyright (c) 2026 The OpenTenBase Global Community
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/opentenbase_mysql_compat/opentenbase_mysql_compat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OPENTENBASE_MYSQL_COMPAT_H
#define OPENTENBASE_MYSQL_COMPAT_H

#include "fmgr.h"

/*
 * Convert the (already checked non-NULL) argument argno of a function
 * whose parameters are declared "any" into a palloc'ed C string, using
 * the type's output function.  Declared in opentenbase_mysql_compat.c.
 */
extern char *mysql_any_to_cstring(FunctionCallInfo fcinfo, int argno);

#endif							/* OPENTENBASE_MYSQL_COMPAT_H */
