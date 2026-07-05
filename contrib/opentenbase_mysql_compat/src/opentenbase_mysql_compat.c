/*-------------------------------------------------------------------------
 *
 * opentenbase_mysql_compat.c
 *	  Extension entry point for MySQL compatibility functions.
 *
 * This extension provides 30+ MySQL-compatible functions to reduce
 * migration cost from MySQL to OpenTenBase.
 *
 * Copyright (c) 2026, OpenTenBase Contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

/* Declarations for functions defined in sub-modules */
extern Datum mysql_ifnull(PG_FUNCTION_ARGS);
extern Datum mysql_concat_ws(PG_FUNCTION_ARGS);
extern Datum mysql_group_concat_transfn(PG_FUNCTION_ARGS);
extern Datum mysql_group_concat_finalfn(PG_FUNCTION_ARGS);
extern Datum mysql_find_in_set(PG_FUNCTION_ARGS);
extern Datum mysql_elt(PG_FUNCTION_ARGS);
extern Datum mysql_field(PG_FUNCTION_ARGS);
extern Datum mysql_insert_func(PG_FUNCTION_ARGS);
extern Datum mysql_date_format(PG_FUNCTION_ARGS);
extern Datum mysql_str_to_date(PG_FUNCTION_ARGS);
extern Datum mysql_datediff(PG_FUNCTION_ARGS);
extern Datum mysql_timestampdiff(PG_FUNCTION_ARGS);
extern Datum mysql_timestampadd(PG_FUNCTION_ARGS);
extern Datum mysql_last_day(PG_FUNCTION_ARGS);
extern Datum mysql_truncate_num(PG_FUNCTION_ARGS);
extern Datum mysql_format_num(PG_FUNCTION_ARGS);
extern Datum mysql_if_func(PG_FUNCTION_ARGS);

void _PG_init(void);
void _PG_fini(void);

void
_PG_init(void)
{
	/* Extension load - no special initialization needed */
}

void
_PG_fini(void)
{
	/* Extension unload */
}
