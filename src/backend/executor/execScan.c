/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#ifdef _MLS_
#include "utils/mls.h"
#endif

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif

#ifdef __OPENTENBASE__
#include "pgxc/shardmap.h"
#include "access/htup_details.h"
#include "storage/nodelock.h"
#endif

#ifdef __OPENTENBASE_C__
#include "optimizer/planmain.h"
#endif


/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	CHECK_FOR_INTERRUPTS();

	if (estate->es_epqTuple != NULL)
	{
		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;
		TupleTableSlot *slot = node->ss_ScanTupleSlot;
		if (((Scan *) node->ps.plan)->isPartTbl)
		{
			scanrelid =
				list_nth_int(((Scan *) node->ps.plan)->partition_leaf_rels_idx, node->curPartIdx);
		}

		if (IsA(node, TidScanState) && ((TidScan *) node->ps.plan)->remote)
			slot = ((TidScanState *)node)->tss_ResultTupleSlot;
		if (scanrelid == 0)
		{
			/*
			 * This is a ForeignScan or CustomScan which has pushed down a
			 * join to the remote side.  The recheck method is responsible not
			 * only for rechecking the scan/join quals but also for storing
			 * the correct tuple in the slot.
			 */
			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */
			return slot;
		}
		else if (estate->es_epqTupleSet[scanrelid - 1])
		{
			/* Return empty slot if we already returned a tuple */
			if (estate->es_epqScanDone[scanrelid - 1])
				return ExecClearTuple(slot);
			/* Else mark to remember that we shouldn't return more */
			estate->es_epqScanDone[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (estate->es_epqTuple[scanrelid - 1] == NULL)
				return ExecClearTuple(slot);

			/* Store test tuple in the plan node's scan slot */
			ExecStoreHeapTuple(estate->es_epqTuple[scanrelid - 1],
							   slot, false);

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */

			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}

/* ----------------------------------------------------------------
 *		ExecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple in the direction specified
 *		in the global variable ExecDirection.
 *		The access method returns the next tuple and ExecScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecScan(ScanState *node,
		 ExecScanAccessMtd accessMtd,	/* function returning a tuple */
		 ExecScanRecheckMtd recheckMtd)
{
	ExprContext *econtext;
	ExprState  *qual;
	ProjectionInfo *projInfo;
	ShardID shardid = InvalidShardID;
#ifdef __AUDIT_FGA__
	ListCell *item;
	char *cmd_type = "SELECT";
#endif	
#ifdef __OPENTENBASE__
	CmdType	commandType = CMD_SELECT;

	if (node->ps.state && node->ps.state->es_plannedstmt)
	{
		commandType = node->ps.state->es_plannedstmt->commandType;
	}
#endif

	/*
	 * Fetch data from node
	 */
	//if (IsA(node, TidScanState) && castNode(TidScan, node->ps.plan)->remote)
	if (IsA(node, TidScanState) && castNode(TidScanState, node)->remote2local)
	{
		qual = castNode(TidScanState, node)->tss_remote_qual;
		projInfo = castNode(TidScanState, node)->tss_ProjInfo;
	}
	else
	{
		qual = node->ps.qual;
		projInfo = node->ps.ps_ProjInfo;
	}

	econtext = node->ps.ps_ExprContext;

	/* interrupt checks are in ExecScanFetch */

	/*
	 * If we have neither a qual to check nor a projection to do, just skip
	 * all the overhead and return the raw scan tuple.
	 */
#ifdef __AUDIT_FGA__
    if (!qual && !projInfo && !node->ps.audit_fga_qual && enable_fga)
#else
	if (!qual && !projInfo)
#endif
	{
		ResetExprContext(econtext);
#ifdef __OPENTENBASE__
		{
			TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);

			/* tts_tuple is NULL if ExecStoreAllNullTuple called */
			if (!TupIsNull(slot) && slot->tts_tuple)
			{
#ifdef _MLS_
				MlsExecCheck(node, slot);
#endif
				/* update shard statistic info about select if needed */
				if (g_StatShardInfo && IS_PGXC_DATANODE &&
					!enable_benchmark_execution &&
					(IsA(node, SeqScanState) ||
					 IsA(node, SampleScanState) ||
					 IsA(node, IndexScanState) ||
					 IsA(node, BitmapHeapScanState) ||
					 IsA(node, TidScanState)))
				{
					HeapTuple tup = slot->tts_tuple;

					UpdateShardStatistic(CMD_SELECT, HeapTupleGetShardId(tup), 0, 0);

					LightLockCheck(commandType, InvalidOid, HeapTupleGetShardId(tup), InvalidShardClusterId);
				}
			}

			return slot;
		}
#else
		return ExecScanFetch(node, accessMtd, recheckMtd);
#endif
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * get a tuple from the access method.  Loop until we obtain a tuple that
	 * passes the qualification.
	 */
	for (;;)
	{
		TupleTableSlot *slot;
		slot = ExecScanFetch(node, accessMtd, recheckMtd);

		/*
		 * if the slot returned by the accessMtd contains NULL, then it means
		 * there is nothing more to scan so we just return an empty slot,
		 * being careful to use the projection result slot so it has correct
		 * tupleDesc.
		 */
		if (TupIsNull(slot))
		{
			if (projInfo)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			else
				return slot;
		}

#ifdef __OPENTENBASE__
		shardid = InvalidShardID;
		/* tts_tuple is NULL if ExecStoreAllNullTuple called */
		if (slot->tts_tuple)
		{
#ifdef _MLS_
			MlsExecCheck(node, slot);
#endif
			/* update shard statistic info about select if needed */
			if (g_StatShardInfo && IS_PGXC_DATANODE &&
				!enable_benchmark_execution &&
				(IsA(node, SeqScanState) ||
				 IsA(node, SampleScanState) ||
				 IsA(node, IndexScanState) ||
				 IsA(node, BitmapHeapScanState) ||
				 IsA(node, TidScanState)))
			{
				HeapTuple tup = slot->tts_tuple;

				UpdateShardStatistic(CMD_SELECT, HeapTupleGetShardId(tup), 0, 0);

				shardid = HeapTupleGetShardId(tup);
			}
		}
#endif

		/*
		 * place the current tuple into the expr context
		 */
		/*
		 * place the current tuple into the expr context
		 */
		if (IsA(node, TidScanState) && castNode(TidScan, node->ps.plan)->remote &&
				!castNode(TidScanState, node)->remote2local)
		{
			/*
			 * For remote tid scans, consider slot as an outer tuple,
			 * as it is from an "outer" RemoteQuery node not shown
			 * in the plan.
			 */
			econtext->ecxt_outertuple = slot;
		}
		else
		{
			/* normal scanned tuple */
			econtext->ecxt_scantuple = slot;
		}


		/*
		 * check that the current tuple satisfies the qual-clause
		 *
		 * check for non-null qual here to avoid a function call to ExecQual()
		 * when the qual is null ... saves only a few cycles, but they add up
		 * ...
		 */
		if (qual == NULL || ExecQual(qual, econtext))
		{
#ifdef __AUDIT_FGA__
            if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
            {
                foreach (item, node->ps.audit_fga_qual)
                {
                    audit_fga_policy_state *audit_fga_qual = (audit_fga_policy_state *) lfirst(item);
                    if (audit_fga_qual != NULL)
        		    {
                        if(ExecQual(audit_fga_qual->qual, econtext))
                        {
                            audit_fga_log_policy_info_2(audit_fga_qual, cmd_type);

                            node->ps.audit_fga_qual = list_delete(node->ps.audit_fga_qual, audit_fga_qual);
                        }
                    }   
                }
            }
#endif
#ifdef __OPENTENBASE__
			if (shardid != InvalidShardID)
			{
				LightLockCheck(commandType, InvalidOid, shardid, InvalidShardClusterId);
			}
#endif
			/*			 * Found a satisfactory scan tuple.
			 */
			if (projInfo)
			{
				/*
				 * Form a projection tuple, store it in the result tuple slot
				 * and return it.
				 */
				return ExecProject(projInfo);
			}
			else
			{
				/*
				 * Here, we aren't projecting, so just return scan tuple.
				 */
				return slot;
			}
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * The scan slot's descriptor must have been set already.
 */
void
ExecAssignScanProjectionInfo(ScanState *node)
{
	Scan	   *scan = (Scan *) node->ps.plan;
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 */
void
ExecAssignScanProjectionInfoWithVarno(ScanState *node, int varno)
{
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, varno);
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void
ExecScanReScan(ScanState *node)
{
	EState	   *estate = node->ps.state;

	/* Rescan EvalPlanQual tuple if we're inside an EvalPlanQual recheck */
	if (estate->es_epqScanDone != NULL)
	{
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid > 0)
			estate->es_epqScanDone[scanrelid - 1] = false;
		else
		{
			Bitmapset  *relids;
			int			rtindex = -1;

			/*
			 * If an FDW or custom scan provider has replaced the join with a
			 * scan, there are multiple RTIs; reset the epqScanDone flag for
			 * all of them.
			 */
			if (IsA(node->ps.plan, ForeignScan))
				relids = ((ForeignScan *) node->ps.plan)->fs_relids;
			else if (IsA(node->ps.plan, CustomScan))
				relids = ((CustomScan *) node->ps.plan)->custom_relids;
			else
				elog(ERROR, "unexpected scan node: %d",
					 (int) nodeTag(node->ps.plan));

			while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
			{
				Assert(rtindex > 0);
				estate->es_epqScanDone[rtindex - 1] = false;
			}
		}
	}
}
