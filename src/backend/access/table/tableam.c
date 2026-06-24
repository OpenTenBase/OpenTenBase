/*----------------------------------------------------------------------
 *
 * tableam.c
 *		Table access method routines too big to be inline functions.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/table/tableam.c
 *
 * NOTES
 *	  Note that most function in here are documented in tableam.h, rather than
 *	  here. That's because there's a lot of inline functions in tableam.h and
 *	  it'd be harder to understand if one constantly had to switch between files.
 *
 *----------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"		/* for ss_* */
#include "access/tableam.h"
#include "access/xact.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"


/* GUC variables */
char	   *default_table_access_method = DEFAULT_TABLE_ACCESS_METHOD;
bool		synchronize_seqscans = true;


/* ----------------------------------------------------------------------------
 * Slot functions.
 * ----------------------------------------------------------------------------
 */

const TupleTableSlotOps *
table_slot_callbacks(Relation relation)
{
	const TupleTableSlotOps *tts_cb;

	if (relation->rd_tableam)
		tts_cb = relation->rd_tableam->slot_callbacks(relation);
	else if (relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		/*
		 * Historically FDWs expect to store heap tuples in slots. Continue
		 * handing them one, to make it less painful to adapt FDWs to new
		 * versions. The cost of a heap slot over a virtual slot is pretty
		 * small.
		 */
		tts_cb = &TTSOpsHeapTuple;
	}
	else
	{
		/*
		 * These need to be supported, as some parts of the code (like COPY)
		 * need to create slots for such relations too. It seems better to
		 * centralize the knowledge that a heap slot is the right thing in
		 * that case here.
		 */
		Assert(relation->rd_rel->relkind == RELKIND_VIEW ||
			   relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
		tts_cb = &TTSOpsVirtual;
	}

	return tts_cb;
}

TupleTableSlot *
table_slot_create(Relation relation, List **reglist)
{
	const TupleTableSlotOps *tts_cb;
	TupleTableSlot *slot;

	tts_cb = table_slot_callbacks(relation);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(relation), tts_cb);

	if (reglist)
		*reglist = lappend(*reglist, slot);

	return slot;
}


/* ----------------------------------------------------------------------------
 * Table scan functions.
 * ----------------------------------------------------------------------------
 */

TableScanDesc
table_beginscan_catalog(Relation relation, int nkeys, struct ScanKeyData *key)
{
	uint32		flags = SO_TYPE_SEQSCAN |
	SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE | SO_TEMP_SNAPSHOT;
	Oid			relid = RelationGetRelid(relation);
	Snapshot	snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));

	return relation->rd_tableam->scan_begin(relation, snapshot, nkeys, key,
											NULL, flags);
}

void
table_scan_update_snapshot(TableScanDesc scan, Snapshot snapshot)
{
	Assert(IsMVCCSnapshot(snapshot));

	RegisterSnapshot(snapshot);
	scan->rs_snapshot = snapshot;
	scan->rs_flags |= SO_TEMP_SNAPSHOT;
}


/* ----------------------------------------------------------------------------
 * Parallel table scan related functions.
 * ----------------------------------------------------------------------------
 */

Size
table_parallelscan_estimate(Relation rel, Snapshot snapshot)
{
	Size		sz = 0;

	if (IsMVCCSnapshot(snapshot))
		sz = add_size(sz, EstimateSnapshotSpace(snapshot));
	else
		Assert(snapshot == SnapshotAny);

	sz = add_size(sz, rel->rd_tableam->parallelscan_estimate(rel));

	return sz;
}

void
table_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan,
							  Snapshot snapshot)
{
	Size		snapshot_off = rel->rd_tableam->parallelscan_initialize(rel, pscan);

	pscan->phs_snapshot_off = snapshot_off;

	if (IsMVCCSnapshot(snapshot))
	{
		SerializeSnapshot(snapshot, (char *) pscan + pscan->phs_snapshot_off);
		pscan->phs_snapshot_any = false;
	}
	else
	{
		Assert(snapshot == SnapshotAny);
		pscan->phs_snapshot_any = true;
	}
}

TableScanDesc
table_beginscan_parallel(Relation relation, ParallelTableScanDesc parallel_scan)
{
	Snapshot	snapshot;
	uint32		flags = SO_TYPE_SEQSCAN |
	SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

	Assert(RelationGetRelid(relation) == parallel_scan->phs_relid);

	if (!parallel_scan->phs_snapshot_any)
	{
		/* Snapshot was serialized -- restore it */
		snapshot = RestoreSnapshot((char *) parallel_scan +
								   parallel_scan->phs_snapshot_off);
		RegisterSnapshot(snapshot);
		flags |= SO_TEMP_SNAPSHOT;
	}
	else
	{
		/* SnapshotAny passed by caller (not serialized) */
		snapshot = SnapshotAny;
	}

	return relation->rd_tableam->scan_begin(relation, snapshot, 0, NULL,
											parallel_scan, flags);
}


/* ----------------------------------------------------------------------------
 * Index scan related functions.
 * ----------------------------------------------------------------------------
 */

/*
 * To perform that check simply start an index scan, create the necessary
 * slot, do the heap lookup, and shut everything down again. This could be
 * optimized, but is unlikely to matter from a performance POV. If there
 * frequently are live index pointers also matching a unique index key, the
 * CPU overhead of this routine is unlikely to matter.
 */
bool
table_index_fetch_tuple_check(Relation rel,
							  ItemPointer tid,
							  Snapshot snapshot,
							  bool *all_dead)
{
	IndexFetchTableData *scan;
	TupleTableSlot *slot;
	bool		call_again = false;
	bool		found;

	slot = table_slot_create(rel, NULL);
	scan = table_index_fetch_begin(rel);
	found = table_index_fetch_tuple(scan, tid, snapshot, slot, &call_again,
									all_dead);
	table_index_fetch_end(scan);
	ExecDropSingleTupleTableSlot(slot);

	return found;
}


/* ------------------------------------------------------------------------
 * Functions for non-modifying operations on individual tuples
 * ------------------------------------------------------------------------
 */

void
table_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	Relation	rel = scan->rs_rd;
	const TableAmRoutine *tableam = rel->rd_tableam;

	/*
	 * Since this can be called with user-supplied TID, don't trust the input
	 * too much.
	 */
	if (!tableam->tuple_tid_valid(scan, tid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("tid (%u, %u) is not valid for relation \"%s\"",
						ItemPointerGetBlockNumberNoCheck(tid),
						ItemPointerGetOffsetNumberNoCheck(tid),
						RelationGetRelationName(rel))));

	tableam->tuple_get_latest_tid(scan, tid);
}


/* ----------------------------------------------------------------------------
 * Functions to make modifications a bit simpler.
 * ----------------------------------------------------------------------------
 */

/*
 * simple_table_tuple_insert - insert a tuple
 *
 * Currently, this routine differs from table_tuple_insert only in supplying a
 * default command ID and not allowing access to the speedup options.
 */
void
simple_table_tuple_insert(Relation rel, TupleTableSlot *slot)
{
	table_tuple_insert(rel, slot, GetCurrentCommandId(true), 0, NULL);
}

/*
 * simple_table_tuple_delete - delete a tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
void
simple_table_tuple_delete(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	TM_Result	result;
	TM_FailureData tmfd;

	result = table_tuple_delete(rel, tid,
								GetCurrentCommandId(true),
								snapshot, InvalidSnapshot,
								true /* wait for commit */ ,
								&tmfd, false /* changingPart */ );

	switch (result)
	{
		case TM_SelfModified:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case TM_Ok:
			/* done successfully */
			break;

		case TM_Updated:
			elog(ERROR, "tuple concurrently updated");
			break;

		case TM_Deleted:
			elog(ERROR, "tuple concurrently deleted");
			break;

		default:
			elog(ERROR, "unrecognized table_tuple_delete status: %u", result);
			break;
	}
}

/*
 * simple_table_tuple_update - replace a tuple
 *
 * This routine may be used to update a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
void
simple_table_tuple_update(Relation rel, ItemPointer otid,
						  TupleTableSlot *slot,
						  Snapshot snapshot,
						  bool *update_indexes)
{
	TM_Result	result;
	TM_FailureData tmfd;
	LockTupleMode lockmode;

	result = table_tuple_update(rel, otid, slot,
								GetCurrentCommandId(true),
								snapshot, InvalidSnapshot,
								true /* wait for commit */ ,
								&tmfd, &lockmode, update_indexes);

	switch (result)
	{
		case TM_SelfModified:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case TM_Ok:
			/* done successfully */
			break;

		case TM_Updated:
			elog(ERROR, "tuple concurrently updated");
			break;

		case TM_Deleted:
			elog(ERROR, "tuple concurrently deleted");
			break;

		default:
			elog(ERROR, "unrecognized table_tuple_update status: %u", result);
			break;
	}

}


/* ----------------------------------------------------------------------------
 * Helper functions to implement parallel scans for block oriented AMs.
 * ----------------------------------------------------------------------------
 */

Size
table_block_parallelscan_estimate(Relation rel)
{
	return sizeof(ParallelBlockTableScanDescData);
}

Size
table_block_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelBlockTableScanDesc bpscan = (ParallelBlockTableScanDesc) pscan;

	bpscan->base.phs_relid = RelationGetRelid(rel);
	bpscan->phs_nblocks = RelationGetNumberOfBlocks(rel);
	/* compare phs_syncscan initialization to similar logic in initscan */
	bpscan->base.phs_syncscan = synchronize_seqscans && (bpscan->phs_nblocks > NBuffers / 4);
	SpinLockInit(&bpscan->phs_mutex);
	bpscan->phs_startblock = InvalidBlockNumber;
	pg_atomic_init_u64(&bpscan->phs_nallocated, 0);

	return sizeof(ParallelBlockTableScanDescData);
}

void
table_block_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelBlockTableScanDesc bpscan = (ParallelBlockTableScanDesc) pscan;

	pg_atomic_write_u64(&bpscan->phs_nallocated, 0);
}
