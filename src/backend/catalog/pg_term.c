/*-------------------------------------------------------------------------
 *
 * pg_term.c
 *	routines to support manipulation of the pg_term relation
 *
 *-------------------------------------------------------------------------
 */
 
#include "postgres.h"

#include <stdlib.h>
#include <math.h>
#include "access/heapam.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "utils/acl.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_term.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "access/sysattr.h"
#include "utils/memutils.h"
#include "optimizer/clauses.h"
#include "utils/numeric.h"
static uint32 QueryTerm();

static uint32
QueryTerm(void)
{
	HeapTuple	tup;
	Form_pg_term term;
	uint32 termvalue;
	tup = SearchSysCache(TERMID, ObjectIdGetDatum(3), 0, 0, 0);
	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "pg_term 1:  not defined");
	}
	term = (Form_pg_term)GETSTRUCT(tup);
	termvalue = term->term;
	ReleaseSysCache(tup);
	elog(DEBUG1, "query term result %u", termvalue);
	return termvalue;
}

void
UpdateTerm(UpdateTermStmt *stmt)
{
	Relation	rel;
	HeapTuple	tup;
	Form_pg_term oldterm;
	tup = SearchSysCache(TERMID, ObjectIdGetDatum(3), 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
	{
		elog(ERROR, "pg_term 1:  not defined");
	}
	oldterm = (Form_pg_term)GETSTRUCT(tup);
	if (oldterm->term >= stmt->term)
	{
		elog(PANIC, "pg_term rollback, old %u, new %u", oldterm->term, stmt->term);
	}
	oldterm->term = stmt->term;
	rel = heap_open(TermRelationId, RowExclusiveLock);
	heap_inplace_update(rel, tup);
	heap_close(rel, RowExclusiveLock);
	elog(LOG, "update term from %u to %u", oldterm->term, stmt->term);
	ReleaseSysCache(tup);
}


void
ShowCurrentTerm(QueryTermStmt *stmt, DestReceiver *dest)
{
	TupOutputState *tstate;
	TupleDesc	tupdesc;
	uint32 termvalue;
	Datum	values[1]; 
	bool	isnull[1]; 
	isnull[0] = false;
	termvalue = QueryTerm();
	values[0] = UInt32GetDatum(termvalue);
	/* need a tuple descriptor representing a single TEXT column */
	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "term",
							  OIDOID, -1, 0);

	/* prepare for projection of tuples */
	tstate = begin_tup_output_tupdesc(dest, tupdesc);
	/* Send it */
	do_tup_output(tstate, values, isnull);
	end_tup_output(tstate);
}

