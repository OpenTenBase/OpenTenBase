/*----------------------------------------------------------------------
 *
 * tableamapi.c
 *		Support routines for API for Postgres table access methods
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/table/tableamapi.c
 *----------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


/*
 * GetTableAmRoutine
 *		Call the specified access method handler routine to get its
 *		TableAmRoutine struct, which will be palloc'd in the caller's
 *		memory context.
 */
const TableAmRoutine *
GetTableAmRoutine(Oid amhandler)
{
	Datum		datum;
	const TableAmRoutine *routine;

	datum = OidFunctionCall0(amhandler);
	routine = (TableAmRoutine *) DatumGetPointer(datum);

	if (routine == NULL || !IsA(routine, TableAmRoutine))
		elog(ERROR, "table access method handler %u did not return a TableAmRoutine struct",
			 amhandler);

	/*
	 * Assert that all required callbacks are present. That makes it a bit
	 * easier to keep AMs up to date, e.g. when forward porting them to a new
	 * major version.
	 */
	Assert(routine->scan_begin != NULL);
	Assert(routine->scan_end != NULL);
	Assert(routine->scan_rescan != NULL);

	Assert(routine->parallelscan_estimate != NULL);
	Assert(routine->parallelscan_initialize != NULL);
	Assert(routine->parallelscan_reinitialize != NULL);

	Assert(routine->index_fetch_begin != NULL);
	Assert(routine->index_fetch_reset != NULL);
	Assert(routine->index_fetch_end != NULL);
	Assert(routine->index_fetch_tuple != NULL);

	Assert(routine->tuple_fetch_row_version != NULL);
	Assert(routine->tuple_satisfies_snapshot != NULL);

	Assert(routine->tuple_insert != NULL);

	/*
	 * Could be made optional, but would require throwing error during
	 * parse-analysis.
	 */
	Assert(routine->tuple_insert_speculative != NULL);
	Assert(routine->tuple_complete_speculative != NULL);

	Assert(routine->multi_insert != NULL);
	Assert(routine->tuple_delete != NULL);
	Assert(routine->tuple_update != NULL);
	Assert(routine->tuple_lock != NULL);

	Assert(routine->relation_set_new_filenode != NULL);
	Assert(routine->relation_nontransactional_truncate != NULL);
	Assert(routine->relation_copy_data != NULL);
	Assert(routine->relation_copy_for_cluster != NULL);
	Assert(routine->relation_vacuum != NULL);
	Assert(routine->scan_analyze_next_block != NULL);
	Assert(routine->scan_analyze_next_tuple != NULL);
	Assert(routine->index_build_range_scan != NULL);
	Assert(routine->index_validate_scan != NULL);

	Assert(routine->relation_size != NULL);

	Assert(routine->relation_estimate_size != NULL);

	/* optional, but one callback implies presence of the other */
	Assert((routine->scan_bitmap_next_block == NULL) ==
		   (routine->scan_bitmap_next_tuple == NULL));
	Assert(routine->scan_sample_next_block != NULL);
	Assert(routine->scan_sample_next_tuple != NULL);

	return routine;
}
