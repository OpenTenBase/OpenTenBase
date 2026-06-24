/*-------------------------------------------------------------------------
 *
 * logicalrelation.h
 *	  Relation definitions for logical replication relation mapping.
 *
 * Portions Copyright (c) 2016-2017, PostgreSQL Global Development Group
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
#include "access/external.h"

typedef struct LogicalRepRelMapEntry
{
	LogicalRepRelation remoterel;	/* key is remoterel.remoteid */

	/* Mapping to local relation, filled as needed. */
	Oid			localreloid;	/* local relation id */
	Relation	localrel;		/* relcache entry */
	AttrNumber *attrmap;		/* map of local attributes to remote ones */
	bool		updatable;		/* Can apply updates/deletes? */

	/* Sync state. */
	char		state;
	XLogRecPtr	statelsn;
#ifdef __STORAGE_SCALABLE__
	uint64      ntups_insert;
	uint64      ntups_delete;
	uint64      checksum_insert;
	uint64      checksum_delete;
	void        *ent;
#endif
#ifdef __SUBSCRIPTION__
	Locator		*locator;		/* the locator object */
#endif
} LogicalRepRelMapEntry;

extern void logicalrep_relmap_update(LogicalRepRelation *remoterel);

extern LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid,
					LOCKMODE lockmode);
extern void logicalrep_rel_close(LogicalRepRelMapEntry *rel,
					 LOCKMODE lockmode);

extern void logicalrep_typmap_update(LogicalRepTyp *remotetyp);
extern Oid	logicalrep_typmap_getid(Oid remoteid);

#ifdef __STORAGE_SCALABLE__
extern void logicalrep_statistic_update_for_sync(Oid relid, Oid subid, char *subname);

extern void logicalrep_statistic_update_for_apply(Oid subid, char *subname);
#endif

#ifdef __SUBSCRIPTION__
extern Oid GetRelationIdentityOrPK(Relation rel);

extern void logical_apply_set_ignore_pk_conflict(bool ignore);
extern void logical_apply_reset_ignore_pk_conflict(void);
extern bool logical_apply_ignore_pk_conflict(void);

extern void logical_apply_dispatch(StringInfo s);
extern void logical_apply_dispatch_batch(StringInfo s);
extern void tdx_apply_dispatch(StringInfo s, TdxSyncDesc tdxDesc);

extern void OpenTenBaseSubscriptionApplyWorkerReset(void);
extern void OpenTenBaseSubscriptionApplyWorkerSet(void);
extern bool AmOpenTenBaseSubscriptionApplyWorker(void);
#endif
extern List *load_partition_offset_pairs_asc(Oid ftoid, List *partitions);
#endif							/* LOGICALRELATION_H */
