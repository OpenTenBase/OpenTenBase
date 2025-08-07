/*-------------------------------------------------------------------------
 *
 * portalcmds.c
 *	  Utility commands affecting portals (that is, SQL cursor commands)
 *
 * Note: see also tcop/pquery.c, which implements portal operations for
 * the FE/BE protocol.  This module uses pquery.c for some operations.
 * And both modules depend on utils/mmgr/portalmem.c, which controls
 * storage management for portals (but doesn't run any queries in them).
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/portalcmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/xact.h"
#include "commands/portalcmds.h"
#include "executor/execDispatchFragment.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "optimizer/cost.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

/*
 * PerformCursorOpen
 *		Execute SQL DECLARE CURSOR command.
 */
void
PerformCursorOpen(DeclareCursorStmt *cstmt, ParamListInfo params,
				  const char *queryString, bool isTopLevel)
{
	Query	   *query = castNode(Query, cstmt->query);
	List	   *rewritten;
	PlannedStmt *plan;
	Portal		portal;
	MemoryContext oldContext;

	/*
	 * Disallow empty-string cursor name (conflicts with protocol-level
	 * unnamed portal).
	 */
	if (!cstmt->portalname || cstmt->portalname[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_NAME),
				 errmsg("invalid cursor name: must not be empty")));

	/*
	 * If this is a non-holdable cursor, we require that this statement has
	 * been executed inside a transaction block (or else, it would have no
	 * user-visible effect).
	 */
	if (!(cstmt->options & CURSOR_OPT_HOLD))
		RequireTransactionChain(isTopLevel, "DECLARE CURSOR");

	/*
	 * Parse analysis was done already, but we still have to run the rule
	 * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
	 * came straight from the parser, or suitable locks were acquired by
	 * plancache.c.
	 *
	 * Because the rewriter and planner tend to scribble on the input, we make
	 * a preliminary copy of the source querytree.  This prevents problems in
	 * the case that the DECLARE CURSOR is in a portal or plpgsql function and
	 * is executed repeatedly.  (See also the same hack in EXPLAIN and
	 * PREPARE.)  XXX FIXME someday.
	 */
	rewritten = QueryRewrite((Query *) copyObject(query));

	/* SELECT should never rewrite to more or less than one query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "non-SELECT statement in DECLARE CURSOR");

	query = linitial_node(Query, rewritten);

	if (query->commandType != CMD_SELECT)
		elog(ERROR, "non-SELECT statement in DECLARE CURSOR");

#ifdef __OPENTENBASE_C__
	query->cursor_opt_scroll = true;
#endif

	/* Plan the query, applying the specified options */
	plan = pg_plan_query(query, cstmt->options, params, 
							params? params->numParams: 0, false);

	/*
	 * Create a portal and copy the plan and queryString into its memory.
	 */
	portal = CreatePortal(cstmt->portalname, false, false, true, NULL);

#ifdef PGXC
	/*
	 * Consume the command id of the command creating the cursor
	 */
	if (IS_PGXC_LOCAL_COORDINATOR)
		GetCurrentCommandId(true);
#endif

	oldContext = MemoryContextSwitchTo(portal->portalContext);

	plan = copyObject(plan);

	queryString = pstrdup(queryString);

	PortalDefineQuery(portal,
					  NULL,
					  queryString,
					  "SELECT", /* cursor's query is always a SELECT */
					  list_make1(plan),
					  NULL);

	/*----------
	 * Also copy the outer portal's parameter list into the inner portal's
	 * memory context.  We want to pass down the parameter values in case we
	 * had a command like
	 *		DECLARE c CURSOR FOR SELECT ... WHERE foo = $1
	 * This will have been parsed using the outer parameter set and the
	 * parameter value needs to be preserved for use when the cursor is
	 * executed.
	 *----------
	 */
	params = copyParamList(params);

	MemoryContextSwitchTo(oldContext);

	/*
	 * Set up options for portal.
	 *
	 * If the user didn't specify a SCROLL type, allow or disallow scrolling
	 * based on whether it would require any additional runtime overhead to do
	 * so.  Also, we disallow scrolling for FOR UPDATE cursors.
	 */
	portal->cursorOptions = cstmt->options;
	if (!(portal->cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)))
	{
		if (plan->rowMarks == NIL &&
			ExecSupportsBackwardScan(plan->planTree))
			portal->cursorOptions |= CURSOR_OPT_SCROLL;
		else
			portal->cursorOptions |= CURSOR_OPT_NO_SCROLL;
	}

	/*
	 * Start execution, inserting parameters if any.
	 */
	PortalStart(portal, params, 0, GetActiveSnapshot());

	Assert(portal->strategy == PORTAL_ONE_SELECT);

	/*
	 * We're done; the query won't actually be run until PerformPortalFetch is
	 * called.
	 */
}

/*
 * PerformPortalFetch
 *		Execute SQL FETCH or MOVE command.
 *
 *	stmt: parsetree node for command
 *	dest: where to send results
 *	completionTag: points to a buffer of size COMPLETION_TAG_BUFSIZE
 *		in which to store a command completion status string.
 *
 * completionTag may be NULL if caller doesn't want a status string.
 */
void
PerformPortalFetch(FetchStmt *stmt,
				   DestReceiver *dest,
				   char *completionTag)
{
	Portal		portal;
	uint64		nprocessed;

	/*
	 * Disallow empty-string cursor name (conflicts with protocol-level
	 * unnamed portal).
	 */
	if (!stmt->portalname || stmt->portalname[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_NAME),
				 errmsg("invalid cursor name: must not be empty")));

	/* get the portal from the portal name */
	portal = GetPortalByName(stmt->portalname);
	if (!PortalIsValid(portal))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("cursor \"%s\" does not exist", stmt->portalname)));
		return;					/* keep compiler happy */
	}

	/* Adjust dest if needed.  MOVE wants destination DestNone */
	if (stmt->ismove)
		dest = None_Receiver;

	/* Do it */
	nprocessed = PortalRunFetch(portal,
								stmt->direction,
								stmt->howMany,
								dest);

	/* Return command status if wanted */
	if (completionTag)
		snprintf(completionTag, COMPLETION_TAG_BUFSIZE, "%s " UINT64_FORMAT,
				 stmt->ismove ? "MOVE" : "FETCH",
				 nprocessed);
}

/*
 * PerformPortalClose
 *		Close a cursor.
 */
void
PerformPortalClose(const char *name)
{
	Portal		portal;

	/* NULL means CLOSE ALL */
	if (name == NULL)
	{
		PortalHashTableDeleteAll();
		return;
	}

	/*
	 * Disallow empty-string cursor name (conflicts with protocol-level
	 * unnamed portal).
	 */
	if (name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_NAME),
				 errmsg("invalid cursor name: must not be empty")));

	/*
	 * get the portal from the portal name
	 */
	portal = GetPortalByName(name);
	if (!PortalIsValid(portal))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CURSOR),
				 errmsg("cursor \"%s\" does not exist", name)));
		return;					/* keep compiler happy */
	}

#ifdef XCP
	elog(DEBUG3, "PerformPortalClose for portal %s", name);
#endif
	/*
	 * Note: PortalCleanup is called as a side-effect, if not already done.
	 */
	PortalDrop(portal, false);
}

/*
 * PortalCleanup
 *
 * Clean up a portal when it's dropped.  This is the standard cleanup hook
 * for portals.
 *
 * Note: if portal->status is PORTAL_FAILED, we are probably being called
 * during error abort, and must be careful to avoid doing anything that
 * is likely to fail again.
 */
void
PortalCleanup(Portal portal)
{
	QueryDesc  *queryDesc;

	/*
	 * sanity checks
	 */
	AssertArg(PortalIsValid(portal));
	AssertArg(portal->cleanup == PortalCleanup);

	/*
	 * Shut down executor, if still running.  We skip this during error abort,
	 * since other mechanisms will take care of releasing executor resources,
	 * and we can't be sure that ExecutorEnd itself wouldn't fail.
	 */
	queryDesc = portal->queryDesc;
	if (queryDesc)
	{
		/*
		 * Reset the queryDesc before anything else.  This prevents us from
		 * trying to shut down the executor twice, in case of an error below.
		 * The transaction abort mechanisms will take care of resource cleanup
		 * in such a case.
		 */
		portal->queryDesc = NULL;

		if (portal->status != PORTAL_FAILED)
		{
			ResourceOwner saveResourceOwner;

			/* We must make the portal's resource owner current */
			saveResourceOwner = CurrentResourceOwner;
			PG_TRY();
			{
				if (portal->resowner)
					CurrentResourceOwner = portal->resowner;
				ExecutorFinish(queryDesc);
				ExecutorEnd(queryDesc);
				FreeQueryDesc(queryDesc);
			}
			PG_CATCH();
			{
				/* Ensure CurrentResourceOwner is restored on error */
				CurrentResourceOwner = saveResourceOwner;
				PG_RE_THROW();
			}
			PG_END_TRY();
			CurrentResourceOwner = saveResourceOwner;
		}
	}
}

/*
 * PersistHoldablePortal
 *
 * Prepare the specified Portal for access outside of the current
 * transaction. When this function returns, all future accesses to the
 * portal must be done via the Tuplestore (not by invoking the
 * executor).
 */
void
PersistHoldablePortal(Portal portal)
{
	QueryDesc  *queryDesc = portal->queryDesc;
	Portal		saveActivePortal;
	ResourceOwner saveResourceOwner;
	MemoryContext savePortalContext;
	MemoryContext oldcxt;

	if(enable_distri_print)
		elog(LOG, "PersistHoldablePortal %s portal %p, %s queryDesc %p", portal->name? portal->name: "NULL", portal, queryDesc->sourceText, queryDesc);
	/*
	 * If we're preserving a holdable portal, we had better be inside the
	 * transaction that originally created it.
	 */
	Assert(portal->createSubid != InvalidSubTransactionId);
	Assert(queryDesc != NULL);

	/*
	 * Caller must have created the tuplestore already ... but not a snapshot.
	 */
	Assert(portal->holdContext != NULL);
	Assert(portal->holdStore != NULL);
	Assert(portal->holdSnapshot == NULL);

	/*
	 * Before closing down the executor, we must copy the tupdesc into
	 * long-term memory, since it was created in executor memory.
	 */
	oldcxt = MemoryContextSwitchTo(portal->holdContext);

	portal->tupDesc = CreateTupleDescCopy(portal->tupDesc);

	MemoryContextSwitchTo(oldcxt);

	/*
	 * Check for improper portal use, and mark portal active.
	 */
	MarkPortalActive(portal);

	/*
	 * Set up global portal context pointers.
	 */
	saveActivePortal = ActivePortal;
	saveResourceOwner = CurrentResourceOwner;
	savePortalContext = PortalContext;
	PG_TRY();
	{
		ScanDirection direction = ForwardScanDirection;

		ActivePortal = portal;
		if (portal->resowner)
			CurrentResourceOwner = portal->resowner;
		PortalContext = portal->portalContext;

		MemoryContextSwitchTo(PortalContext);

		PushActiveSnapshot(queryDesc->snapshot);

		/* Snapshot is changed, to trigger to send commandId to DN. */
		if (IS_PGXC_LOCAL_COORDINATOR)
			SetSendCommandId(true);

		/*
		 * If the portal is marked scrollable, we need to store the entire
		 * result set in the tuplestore, so that subsequent backward FETCHs
		 * can be processed.  Otherwise, store only the not-yet-fetched rows.
		 * (The latter is not only more efficient, but avoids semantic
		 * problems if the query's output isn't stable.)
		 */
		if (portal->cursorOptions & CURSOR_OPT_SCROLL)
		{
			ExecutorRewind(queryDesc);
		}
		else
		{
			/* We must reset the cursor state as though at start of query */
			portal->atStart = true;
			portal->portalPos = 0;

			/*
			 * If we already reached end-of-query, set the direction to
			 * NoMovement to avoid trying to fetch any tuples.  (This check
			 * exists because not all plan node types are robust about being
			 * called again if they've already returned NULL once.)  We'll
			 * still set up an empty tuplestore, though, to keep this from
			 * being a special case later.
			 */
			if (portal->atEnd)
				direction = NoMovementScanDirection;
		}

		/*
		 * Change the destination to output to the tuplestore.  Note we tell
		 * the tuplestore receiver to detoast all data passed through it; this
		 * makes it safe to not keep a snapshot associated with the data.
		 */
		queryDesc->dest = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(queryDesc->dest,
										portal->holdStore,
										portal->holdContext,
										true,
										NULL,
										NULL);

		/* Fetch the result set into the tuplestore */
		ExecutorRun(queryDesc, direction, 0L, false);

		queryDesc->dest->rDestroy(queryDesc->dest);
		queryDesc->dest = NULL;

		/*
		 * Now shut down the inner executor.
		 */
		portal->queryDesc = NULL;	/* prevent double shutdown */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);
		FreeQueryDesc(queryDesc);

		/*
		 * Set the position in the result set.
		 */
		MemoryContextSwitchTo(portal->holdContext);

		if (portal->atEnd)
		{
			/*
			 * Just force the tuplestore forward to its end.  The size of the
			 * skip request here is arbitrary.
			 */
			while (tuplestore_skiptuples(portal->holdStore, 1000000, true))
				 /* continue */ ;
		}
		else
		{
			tuplestore_rescan(portal->holdStore);

			if (!tuplestore_skiptuples(portal->holdStore,
									   portal->portalPos,
									   true))
				elog(ERROR, "unexpected end of tuple stream");
		}
	}
	PG_CATCH();
	{
		/* Uncaught error while executing portal: mark it dead */
		MarkPortalFailed(portal);

		/* Restore global vars and propagate error */
		ActivePortal = saveActivePortal;
		CurrentResourceOwner = saveResourceOwner;
		PortalContext = savePortalContext;

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);

	/* Mark portal not active */
	portal->status = PORTAL_READY;

	ActivePortal = saveActivePortal;
	CurrentResourceOwner = saveResourceOwner;
	PortalContext = savePortalContext;

	PopActiveSnapshot();

	/*
	 * We can now release any subsidiary memory of the portal's context;
	 * we'll never use it again.  The executor already dropped its context,
	 * but this will clean up anything that glommed onto the portal's context via
	 * PortalContext.
	 */
	MemoryContextDeleteChildren(portal->portalContext);
}
