/*-------------------------------------------------------------------------
 *
 * pquery.h
 *      prototypes for pquery.c.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/tcop/pquery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PQUERY_H
#define PQUERY_H

#include "nodes/parsenodes.h"
#include "utils/portal.h"


extern PGDLLIMPORT Portal ActivePortal;


extern PortalStrategy ChoosePortalStrategy(List *stmts);

extern List *FetchPortalTargetList(Portal portal);

extern List *FetchStatementTargetList(Node *stmt);

extern void PortalStart(Portal portal, ParamListInfo params,
            int eflags, Snapshot snapshot);

extern void PortalSetResultFormat(Portal portal, int nFormats,
                      int16 *formats);

extern bool PortalRun(Portal portal, long count, bool isTopLevel,
          bool run_once, DestReceiver *dest, DestReceiver *altdest,
          char *completionTag);

extern uint64 PortalRunFetch(Portal portal,
               FetchDirection fdirection,
               long count,
               DestReceiver *dest);

#ifdef XCP
extern int    AdvanceProducingPortal(Portal portal, bool can_wait);
extern void cleanupClosedProducers(void);
#endif

#ifdef __OPENTENBASE__
extern bool paramPassDown;
#endif

#ifdef __AUDIT__
extern bool PortalGetQueryInfo(Portal portal, Query ** parseTree, const char ** queryString);
#endif

#endif                            /* PQUERY_H */
