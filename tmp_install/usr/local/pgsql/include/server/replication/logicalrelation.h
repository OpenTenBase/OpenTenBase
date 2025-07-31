/*-------------------------------------------------------------------------
 *
 * logicalrelation.h
 *      Relation definitions for logical replication relation mapping.
 *
 * Portions Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/replication/logicalrelation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALRELATION_H
#define LOGICALRELATION_H

#include "replication/logicalproto.h"
#ifdef __SUBSCRIPTION__
#include "pgxc/locator.h"
#endif

typedef struct LogicalRepRelMapEntry
{
    LogicalRepRelation remoterel;    /* key is remoterel.remoteid */

    /* Mapping to local relation, filled as needed. */
    Oid            localreloid;    /* local relation id */
    Relation    localrel;        /* relcache entry */
    AttrNumber *attrmap;        /* map of local attributes to remote ones */
    bool        updatable;        /* Can apply updates/deletes? */

    /* Sync state. */
    char        state;
    XLogRecPtr    statelsn;
#ifdef __STORAGE_SCALABLE__
    uint64      ntups_insert;
    uint64      ntups_delete;
    uint64      checksum_insert;
    uint64      checksum_delete;
    void        *ent;
#endif
#ifdef __SUBSCRIPTION__
    Locator        *locator;        /* the locator object */
#endif
} LogicalRepRelMapEntry;

extern void logicalrep_relmap_update(LogicalRepRelation *remoterel);

extern LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid,
                    LOCKMODE lockmode);
extern void logicalrep_rel_close(LogicalRepRelMapEntry *rel,
                     LOCKMODE lockmode);

extern void logicalrep_typmap_update(LogicalRepTyp *remotetyp);
extern Oid    logicalrep_typmap_getid(Oid remoteid);

#ifdef __STORAGE_SCALABLE__
extern void logicalrep_statis_update_for_sync(Oid relid, Oid subid, char *subname);

extern void logicalrep_statis_update_for_apply(Oid subid, char *subname);
#endif

#ifdef __SUBSCRIPTION__
extern Oid GetRelationIdentityOrPK(Relation rel);

extern void logicl_apply_set_ignor_pk_conflict(bool ignore);
extern void logicl_aply_rset_ignor_pk_conflict(void);
extern bool logical_apply_ignore_pk_conflict(void);

extern void OpenTenBaseSubscriptionApplyWorkerReset(void);
extern void OpenTenBaseSubscriptionApplyWorkerSet(void);
extern bool AmOpenTenBaseSubscriptionApplyWorker(void);
#endif

#endif                            /* LOGICALRELATION_H */
