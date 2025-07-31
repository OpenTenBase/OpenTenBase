/* -------------------------------------------------------------------------
 *
 * pg_subscription_rel.h
 *        Local info about tables that come from the publisher of a
 *        subscription (pg_subscription_rel).
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_REL_H
#define PG_SUBSCRIPTION_REL_H

#include "access/xlogdefs.h"
#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------
 *        pg_subscription_rel definition. cpp turns this into
 *        typedef struct FormData_pg_subscription_rel
 * ----------------
 */
#define SubscriptionRelRelationId            6102

/* Workaround for genbki not knowing about XLogRecPtr */
#define pg_lsn XLogRecPtr

CATALOG(pg_subscription_rel,6102) BKI_WITHOUT_OIDS
{
    Oid            srsubid;        /* Oid of subscription */
    Oid            srrelid;        /* Oid of relation */
    char        srsubstate;        /* state of the relation in subscription */
    pg_lsn        srsublsn;        /* remote lsn of the state change used for
                                 * synchronization coordination */
} FormData_pg_subscription_rel;

typedef FormData_pg_subscription_rel *Form_pg_subscription_rel;

/* ----------------
 *        compiler constants for pg_subscription_rel
 * ----------------
 */
#define Natts_pg_subscription_rel                4
#define Anum_pg_subscription_rel_srsubid        1
#define Anum_pg_subscription_rel_srrelid        2
#define Anum_pg_subscription_rel_srsubstate        3
#define Anum_pg_subscription_rel_srsublsn        4

/* ----------------
 *        substate constants
 * ----------------
 */
#define SUBREL_STATE_INIT        'i' /* initializing (sublsn NULL) */
#define SUBREL_STATE_DATASYNC    'd' /* data is being synchronized (sublsn
                                     * NULL) */
#define SUBREL_STATE_SYNCDONE    's' /* synchronization finished in front of
                                     * apply (sublsn set) */
#define SUBREL_STATE_READY        'r' /* ready (sublsn set) */

/* These are never stored in the catalog, we only use them for IPC. */
#define SUBREL_STATE_UNKNOWN    '\0'    /* unknown state */
#define SUBREL_STATE_SYNCWAIT    'w' /* waiting for sync */
#define SUBREL_STATE_CATCHUP    'c' /* catching up with apply */

typedef struct SubscriptionRelState
{
    Oid            relid;
    XLogRecPtr    lsn;
    char        state;
} SubscriptionRelState;

extern Oid SetSubscriptionRelState(Oid subid, Oid relid, char state,
                        XLogRecPtr sublsn, bool update_only, bool skip_is_exist);
extern char GetSubscriptionRelState(Oid subid, Oid relid,
                        XLogRecPtr *sublsn, bool missing_ok);
extern void RemoveSubscriptionRel(Oid subid, Oid relid);

#ifdef __STORAGE_SCALABLE__
extern void RemoveSubscriptionShard(Oid subid, int32 shardid);

extern void RemoveSubscriptionTable(Oid subid, Oid relid);
#endif

extern List *GetSubscriptionRelations(Oid subid);
extern List *GetSubscriptionNotReadyRelations(Oid subid);

#endif                            /* PG_SUBSCRIPTION_REL_H */
