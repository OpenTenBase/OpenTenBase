/*-------------------------------------------------------------------------
 * relation.c
 *	   PostgreSQL logical replication
 *
 * Copyright (c) 2016-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/relation.c
 *
 * NOTES
 *	  This file contains helper functions for logical replication relation
 *	  mapping cache.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/namespace.h"
#include "catalog/pg_subscription_rel.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#ifdef __STORAGE_SCALABLE__
#include "replication/logical_statistic.h"
#endif
#ifdef __SUBSCRIPTION__
#include "commands/trigger.h"
#include "rewrite/rewriteHandler.h"
#include "optimizer/planner.h"
#include "utils/snapmgr.h"
#include "access/xact.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxcnode.h"
#endif
#ifdef __OPENTENBASE_C__
#endif

static MemoryContext LogicalRepRelMapContext = NULL;

static HTAB *LogicalRepRelMap = NULL;
static HTAB *LogicalRepTypMap = NULL;

#ifdef __SUBSCRIPTION__
static bool g_logical_apply_ignore_pk_conflict = false;
static bool g_am_opentenbase_logical_apply_worker = false;
#endif

static void logicalrep_typmap_invalidate_cb(Datum arg, int cacheid,
								uint32 hashvalue);

/*
 * Relcache invalidation callback for our relation map cache.
 */
static void
logicalrep_relmap_invalidate_cb(Datum arg, Oid reloid)
{
	LogicalRepRelMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepRelMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		/* TODO, use inverse lookup hashtable? */
		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->localreloid == reloid)
			{
				entry->localreloid = InvalidOid;
#ifdef __SUBSCRIPTION__
				if (entry->locator)
				{
					freeLocator(entry->locator);
					entry->locator = NULL;
				}
#endif
				hash_seq_term(&status);
				break;
			}
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, LogicalRepRelMap);

		while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
		{
			entry->localreloid = InvalidOid;
#ifdef __SUBSCRIPTION__
			if (entry->locator)
			{
				freeLocator(entry->locator);
				entry->locator = NULL;
			}
#endif
		}
	}
}

/*
 * Initialize the relation map cache.
 */
static void
logicalrep_relmap_init(void)
{
	HASHCTL		ctl;

	if (!LogicalRepRelMapContext)
		LogicalRepRelMapContext =
			AllocSetContextCreate(CacheMemoryContext,
								  "LogicalRepRelMapContext",
								  ALLOCSET_DEFAULT_SIZES);

	/* Initialize the relation hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(LogicalRepRelId);
	ctl.entrysize = sizeof(LogicalRepRelMapEntry);
	ctl.hcxt = LogicalRepRelMapContext;

	LogicalRepRelMap = hash_create("logicalrep relation map cache", 128, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Initialize the type hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(LogicalRepTyp);
	ctl.hcxt = LogicalRepRelMapContext;

	/* This will usually be small. */
	LogicalRepTypMap = hash_create("logicalrep type map cache", 2, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(logicalrep_relmap_invalidate_cb,
								  (Datum) 0);
	CacheRegisterSyscacheCallback(TYPEOID, logicalrep_typmap_invalidate_cb,
								  (Datum) 0);
}

/*
 * Free the entry of a relation map cache.
 */
static void
logicalrep_relmap_free_entry(LogicalRepRelMapEntry *entry)
{
	LogicalRepRelation *remoterel;

	remoterel = &entry->remoterel;

	pfree(remoterel->nspname);
	pfree(remoterel->relname);

	if (remoterel->natts > 0)
	{
		int			i;

		for (i = 0; i < remoterel->natts; i++)
			pfree(remoterel->attnames[i]);

		pfree(remoterel->attnames);
		pfree(remoterel->atttyps);
	}
	bms_free(remoterel->attkeys);

	if (entry->attrmap)
		pfree(entry->attrmap);

#ifdef __SUBSCRIPTION__
	if (entry->locator)
	{
		freeLocator(entry->locator);
		entry->locator = NULL;
	}
#endif
}

/*
 * Add new entry or update existing entry in the relation map cache.
 *
 * Called when new relation mapping is sent by the publisher to update
 * our expected view of incoming data from said publisher.
 */
void
logicalrep_relmap_update(LogicalRepRelation *remoterel)
{
	MemoryContext oldctx;
	LogicalRepRelMapEntry *entry;
	bool		found;
	int			i;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(LogicalRepRelMap, (void *) &remoterel->remoteid,
						HASH_ENTER, &found);

	if (found)
		logicalrep_relmap_free_entry(entry);

	memset(entry, 0, sizeof(LogicalRepRelMapEntry));

	/* Make cached copy of the data */
	oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
	entry->remoterel.remoteid = remoterel->remoteid;
	entry->remoterel.nspname = pstrdup(remoterel->nspname);
	entry->remoterel.relname = pstrdup(remoterel->relname);
	entry->remoterel.natts = remoterel->natts;
	entry->remoterel.attnames = palloc(remoterel->natts * sizeof(char *));
	entry->remoterel.atttyps = palloc(remoterel->natts * sizeof(Oid));
	for (i = 0; i < remoterel->natts; i++)
	{
		entry->remoterel.attnames[i] = pstrdup(remoterel->attnames[i]);
		entry->remoterel.atttyps[i] = remoterel->atttyps[i];
	}
	entry->remoterel.replident = remoterel->replident;
	entry->remoterel.attkeys = bms_copy(remoterel->attkeys);
#ifdef __STORAGE_SCALABLE__
	entry->ntups_insert = 0;
	entry->ntups_delete = 0;
	entry->checksum_insert = 0;
	entry->checksum_delete = 0;
	entry->ent = NULL;
#endif

#ifdef __SUBSCRIPTION__
	entry->locator = NULL;
#endif
	MemoryContextSwitchTo(oldctx);
}

/*
 * Find attribute index in TupleDesc struct by attribute name.
 *
 * Returns -1 if not found.
 */
static int
logicalrep_rel_att_by_name(LogicalRepRelation *remoterel, const char *attname)
{
	int			i;

	for (i = 0; i < remoterel->natts; i++)
	{
		if (strcmp(remoterel->attnames[i], attname) == 0)
			return i;
	}

	return -1;
}

/*
 * Open the local relation associated with the remote one.
 *
 * Optionally rebuilds the Relcache mapping if it was invalidated
 * by local DDL.
 */
LogicalRepRelMapEntry *
logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
{
	LogicalRepRelMapEntry *entry = NULL;
	bool		found = false;

	if (LogicalRepRelMap == NULL)
		logicalrep_relmap_init();

	/* Search for existing entry. */
	entry = hash_search(LogicalRepRelMap, (void *) &remoteid,
						HASH_FIND, &found);

#ifdef __SUBSCRIPTION__
	if (!found)
	{
		if (am_opentenbase_subscription_dispatch_worker())
		{
			// elog(LOG, "no relation map entry for remote relation ID %u, ignoring this subscription", remoteid);
			return NULL;
		}
		else
		{
			elog(ERROR, "no relation map entry for remote relation ID %u",
			 	remoteid);
		}
	}
#else
	if (!found)
		elog(ERROR, "no relation map entry for remote relation ID %u",
			 remoteid);
#endif

	/* Need to update the local cache? */
	if (!OidIsValid(entry->localreloid))
	{
		Oid			relid;
		int			i;
		int			found;
		Bitmapset  *idkey;
		TupleDesc	desc;
		LogicalRepRelation *remoterel;
		MemoryContext oldctx;

		remoterel = &entry->remoterel;

		/* Try to find and lock the relation by name. */
		relid = RangeVarGetRelid(makeRangeVar(remoterel->nspname,
											  remoterel->relname, -1),
								 lockmode, true);
		if (!OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("logical replication target relation \"%s.%s\" does not exist",
							remoterel->nspname, remoterel->relname)));
		entry->localrel = heap_open(relid, NoLock);

		/* Check for supported relkind. */
		CheckSubscriptionRelkind(entry->localrel->rd_rel->relkind,
								 remoterel->nspname, remoterel->relname);

		/*
		 * Build the mapping of local attribute numbers to remote attribute
		 * numbers and validate that we don't miss any replicated columns as
		 * that would result in potentially unwanted data loss.
		 */
		desc = RelationGetDescr(entry->localrel);
		oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
		entry->attrmap = palloc(desc->natts * sizeof(int));
		MemoryContextSwitchTo(oldctx);

		found = 0;
		for (i = 0; i < desc->natts; i++)
		{
			int			attnum;
			Form_pg_attribute attr = TupleDescAttr(desc, i);

			if (attr->attisdropped)
			{
				entry->attrmap[i] = -1;
				continue;
			}

			attnum = logicalrep_rel_att_by_name(remoterel,
												NameStr(attr->attname));

			entry->attrmap[i] = attnum;
			if (attnum >= 0)
				found++;
		}

		/* TODO, detail message with names of missing columns */
		if (found < remoterel->natts)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("logical replication target relation \"%s.%s\" is missing "
							"some replicated columns",
							remoterel->nspname, remoterel->relname)));

		/*
		 * Check that replica identity matches. We allow for stricter replica
		 * identity (fewer columns) on subscriber as that will not stop us
		 * from finding unique tuple. IE, if publisher has identity
		 * (id,timestamp) and subscriber just (id) this will not be a problem,
		 * but in the opposite scenario it will.
		 *
		 * Don't throw any error here just mark the relation entry as not
		 * updatable, as replica identity is only for updates and deletes but
		 * inserts can be replicated even without it.
		 */
		entry->updatable = true;
		idkey = RelationGetIndexAttrBitmap(entry->localrel,
										   INDEX_ATTR_BITMAP_IDENTITY_KEY);
		/* fallback to PK if no replica identity */
		if (idkey == NULL)
		{
			idkey = RelationGetIndexAttrBitmap(entry->localrel,
											   INDEX_ATTR_BITMAP_PRIMARY_KEY);

			/*
			 * If no replica identity index and no PK, the published table
			 * must have replica identity FULL.
			 */
			if (idkey == NULL && remoterel->replident != REPLICA_IDENTITY_FULL)
				entry->updatable = false;
		}

		i = -1;
		while ((i = bms_next_member(idkey, i)) >= 0)
		{
			int			attnum = i + FirstLowInvalidHeapAttributeNumber;

			if (!AttrNumberIsForUserDefinedAttr(attnum))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("logical replication target relation \"%s.%s\" uses "
								"system columns in REPLICA IDENTITY index",
								remoterel->nspname, remoterel->relname)));

			attnum = AttrNumberGetAttrOffset(attnum);

			if (!bms_is_member(entry->attrmap[attnum], remoterel->attkeys))
			{
				entry->updatable = false;
				break;
			}
		}

		entry->localreloid = relid;
	}
	else
		entry->localrel = heap_open(entry->localreloid, lockmode);

	if (entry->state != SUBREL_STATE_READY)
		entry->state = GetSubscriptionRelState(MySubscription->oid,
											   entry->localreloid,
											   &entry->statelsn,
											   true);

	return entry;
}

/*
 * Close the previously opened logical relation.
 */
void
logicalrep_rel_close(LogicalRepRelMapEntry *rel, LOCKMODE lockmode)
{
#ifdef __SUBSCRIPTION__
	if (NULL == rel)
		return;
#endif

	heap_close(rel->localrel, lockmode);
	rel->localrel = NULL;
}


/*
 * Type cache invalidation callback for our type map cache.
 */
static void
logicalrep_typmap_invalidate_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	LogicalRepTyp *entry;

	/* Just to be sure. */
	if (LogicalRepTypMap == NULL)
		return;

	/* invalidate all cache entries */
	hash_seq_init(&status, LogicalRepTypMap);

	while ((entry = (LogicalRepTyp *) hash_seq_search(&status)) != NULL)
		entry->typoid = InvalidOid;
}

/*
 * Free the type map cache entry data.
 */
static void
logicalrep_typmap_free_entry(LogicalRepTyp *entry)
{
	pfree(entry->nspname);
	pfree(entry->typname);

	entry->typoid = InvalidOid;
}

/*
 * Add new entry or update existing entry in the type map cache.
 */
void
logicalrep_typmap_update(LogicalRepTyp *remotetyp)
{
	MemoryContext oldctx;
	LogicalRepTyp *entry;
	bool		found;

	if (LogicalRepTypMap == NULL)
		logicalrep_relmap_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(LogicalRepTypMap, (void *) &remotetyp->remoteid,
						HASH_ENTER, &found);

	if (found)
		logicalrep_typmap_free_entry(entry);

	/* Make cached copy of the data */
	entry->remoteid = remotetyp->remoteid;
	oldctx = MemoryContextSwitchTo(LogicalRepRelMapContext);
	entry->nspname = pstrdup(remotetyp->nspname);
	entry->typname = pstrdup(remotetyp->typname);
	MemoryContextSwitchTo(oldctx);
	entry->typoid = InvalidOid;
}

/*
 * Fetch type info from the cache.
 */
Oid
logicalrep_typmap_getid(Oid remoteid)
{
	LogicalRepTyp *entry;
	bool		found;
	Oid			nspoid;

	/* Internal types are mapped directly. */
	if (remoteid < FirstNormalObjectId)
	{
		if (!get_typisdefined(remoteid))
			ereport(ERROR,
					(errmsg("builtin type %u not found", remoteid),
					 errhint("This can be caused by having publisher with "
							 "higher major version than subscriber")));
		return remoteid;
	}

	if (LogicalRepTypMap == NULL)
		logicalrep_relmap_init();

	/* Try finding the mapping. */
	entry = hash_search(LogicalRepTypMap, (void *) &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(ERROR, "no type map entry for remote type %u",
			 remoteid);

	/* Found and mapped, return the oid. */
	if (OidIsValid(entry->typoid))
		return entry->typoid;

	/* Otherwise, try to map to local type. */
	nspoid = LookupExplicitNamespace(entry->nspname, true);
	if (OidIsValid(nspoid))
		entry->typoid = GetSysCacheOid2(TYPENAMENSP,
										PointerGetDatum(entry->typname),
										ObjectIdGetDatum(nspoid));
	else
		entry->typoid = InvalidOid;

	if (!OidIsValid(entry->typoid))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("data type \"%s.%s\" required for logical replication does not exist",
						entry->nspname, entry->typname)));

	return entry->typoid;
}

#ifdef __STORAGE_SCALABLE__
void
logicalrep_statistic_update_for_sync(Oid relid, Oid subid, char *subname)
{
	HASH_SEQ_STATUS status;
	LogicalRepRelMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepRelMap == NULL)
		return;

	hash_seq_init(&status, LogicalRepRelMap);

	while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->localreloid == relid)
		{
			//UpdateSubTableStatistics(subid, relid, 0, entry->ntups_insert, 
			//	                     entry->ntups_delete, entry->checksum_insert, entry->checksum_delete, STATE_APPLY, false);
			UpdateSubStatistics(subname, 0, entry->ntups_insert, entry->ntups_delete, 
				                entry->checksum_insert, entry->checksum_delete, false);

			entry->ntups_insert = 0;
			entry->ntups_delete = 0;
			entry->checksum_insert = 0;
			entry->checksum_delete = 0;
			entry->ent = NULL;
			
			hash_seq_term(&status);
			break;
		}
	}
}
void
logicalrep_statistic_update_for_apply(Oid subid, char *subname)
{
	HASH_SEQ_STATUS status;
	LogicalRepRelMapEntry *entry;

	/* Just to be sure. */
	if (LogicalRepRelMap == NULL)
		return;

	hash_seq_init(&status, LogicalRepRelMap);

	while ((entry = (LogicalRepRelMapEntry *) hash_seq_search(&status)) != NULL)
	{
		if (OidIsValid(entry->localreloid))
		{
			//UpdateSubTableStatistics(subid, entry->localreloid, 0, entry->ntups_insert, 
			//	                     entry->ntups_delete, entry->checksum_insert, entry->checksum_delete, STATE_APPLY, false);
			UpdateSubStatistics(subname, 0, entry->ntups_insert, entry->ntups_delete, 
				                entry->checksum_insert, entry->checksum_delete, false);

			entry->ntups_insert = 0;
			entry->ntups_delete = 0;
			entry->checksum_insert = 0;
			entry->checksum_delete = 0;
			entry->ent = NULL;
		}
	}
}
#endif

#ifdef __SUBSCRIPTION__
bool AmOpenTenBaseSubscriptionApplyWorker(void)
{
	return IS_PGXC_DATANODE && g_am_opentenbase_logical_apply_worker;
}

void OpenTenBaseSubscriptionApplyWorkerSet(void)
{
    g_am_opentenbase_logical_apply_worker = true;
    return;
}

void OpenTenBaseSubscriptionApplyWorkerReset(void)
{
    g_am_opentenbase_logical_apply_worker = false;
    return;
}

void logical_apply_set_ignore_pk_conflict(bool ignore)
{
	g_logical_apply_ignore_pk_conflict = ignore;
}

void logical_apply_reset_ignore_pk_conflict(void)
{
	logical_apply_set_ignore_pk_conflict(false);
}

bool logical_apply_ignore_pk_conflict(void)
{
	return g_logical_apply_ignore_pk_conflict;
}

/*
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *
logical_apply_create_estate_for_rel(Relation rel)
{
	EState	   *estate = NULL;
	ResultRelInfo *resultRelInfo = NULL;
	RangeTblEntry *rte = NULL;

	estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	estate->es_range_table = list_make1(rte);

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	/* Triggers might need a slot */
	if (resultRelInfo->ri_TrigDesc)
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate, NULL);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return estate;
}

/*
 * Executes default values for columns for which we can't map to remote
 * relation columns.
 *
 * This allows us to support tables which have more columns on the downstream
 * than on the upstream.
 */
static void
logical_apply_slot_fill_defaults(Relation rel,
									EState *estate,
				   					TupleTableSlot *slot,
				   					char **values)
{
	TupleDesc	desc = RelationGetDescr(rel);
	int			num_phys_attrs = desc->natts;
	int			i = 0;
	int			attnum = 0,
				num_defaults = 0;
	int		   *defmap = NULL;
	ExprState **defexprs = NULL;
	ExprContext *econtext = NULL;

	econtext = GetPerTupleExprContext(estate);
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 0; attnum < num_phys_attrs; attnum++)
	{
		Expr	   *defexpr = NULL;

		if (TupleDescAttr(desc, attnum)->attisdropped || values[attnum] != NULL)
			continue;

		defexpr = (Expr *) build_column_default(rel, attnum + 1);

		if (defexpr != NULL)
		{
			/* Run the expression through planner */
			defexpr = expression_planner(defexpr);

			/* Initialize executable expression in copycontext */
			defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
			defmap[num_defaults] = attnum;
			num_defaults++;
		}
	}

	for (i = 0; i < num_defaults; i++)
		slot->tts_values[defmap[i]] =
			ExecEvalExpr(defexprs[i], econtext, &slot->tts_isnull[defmap[i]]);
}

/*
 * Store data in C string form into slot.
 * This is similar to BuildTupleFromCStrings but TupleTableSlot fits our
 * use better.
 */
static void
logical_apply_slot_store_cstrings(TupleTableSlot *slot,
										Relation rel,
										char **values)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i = 0;

	ExecClearTuple(slot);

	/* Call the "in" function for each non-dropped attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (!att->attisdropped && values[i] != NULL)
		{
			Oid			typinput = InvalidOid;
			Oid			typioparam = InvalidOid;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput,
													   values[i],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			/*
			 * We assign NULL to dropped attributes, NULL values, and missing
			 * values (missing values should be later filled using
			 * logical_apply_slot_fill_defaults).
			 */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	ExecStoreVirtualTuple(slot);
}

/*
 * Modify slot with user data provided as C strings.
 * This is somewhat similar to heap_modify_tuple but also calls the type
 * input function on the user data as the input is the text representation
 * of the types.
 */
static void
logical_apply_slot_modify_cstrings(TupleTableSlot *slot,
										Relation rel,
					 					char **values,
					 					bool *replaces)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i = 0;

	slot_getallattrs(slot);
	ExecClearTuple(slot);

	/* Call the "in" function for each replaced attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (!replaces[i])
			continue;

		if (values[i] != NULL)
		{
			Oid			typinput = InvalidOid;
			Oid			typioparam = InvalidOid;

			getTypeInputInfo(att->atttypid, &typinput, &typioparam);
			slot->tts_values[i] = OidInputFunctionCall(typinput,
													   values[i],
													   typioparam,
													   att->atttypmod);
			slot->tts_isnull[i] = false;
		}
		else
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	ExecStoreVirtualTuple(slot);
}

/*
 * logical apply insert message from CN
 */
static void
logical_apply_insert(StringInfo s)
{
	Relation rel = NULL;
	LogicalRepTupleData newtup;
	EState	   *estate = NULL;
	TupleTableSlot *remoteslot = NULL;
	MemoryContext oldctx = NULL;

	char	   *nspname = NULL;
	char	   *relname = NULL;
	char		replident = 0;

	MemSet(&newtup, 0, sizeof(LogicalRepTupleData));

	logicalrep_read_insert(s, &nspname, &relname, &replident, &newtup, NULL);

	rel = relation_openrv(makeRangeVar(nspname, relname, -1), RowExclusiveLock);

	/* Check for supported relkind. */
	CheckSubscriptionRelkind(rel->rd_rel->relkind, nspname, relname);

	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));

	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	logical_apply_slot_store_cstrings(remoteslot, rel, newtup.values);
	logical_apply_slot_fill_defaults(rel, estate, remoteslot, newtup.values);
	MemoryContextSwitchTo(oldctx);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Do the insert. */
	ExecSimpleRelationInsert(estate, remoteslot);

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);

	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * logical apply update message from CN
 */
static void 
logical_apply_update(StringInfo s)
{
	Relation 	rel = NULL;
	Oid			idxoid = InvalidOid;
	EState	   *estate = NULL;
	EPQState	epqstate;
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool		has_oldtup = false;
	TupleTableSlot *localslot = NULL;
	TupleTableSlot *remoteslot = NULL;
	bool		found = false;
	MemoryContext oldctx = NULL;

	char	   *nspname = NULL;
	char	   *relname = NULL;
	char		replident = 0;

	MemSet(&epqstate, 0, sizeof(EPQState));
	MemSet(&oldtup, 0, sizeof(LogicalRepTupleData));
	MemSet(&newtup, 0, sizeof(LogicalRepTupleData));

	logicalrep_read_update(s, &nspname, &relname, &replident,
								&has_oldtup, &oldtup, &newtup, NULL, NULL);

	rel = relation_openrv(makeRangeVar(nspname, relname, -1), RowExclusiveLock);

	/* Check for supported relkind. */
	CheckSubscriptionRelkind(rel->rd_rel->relkind, nspname, relname);

	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	localslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Build the search tuple. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	logical_apply_slot_store_cstrings(remoteslot, rel,
											has_oldtup ? oldtup.values : newtup.values);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel);
	Assert(OidIsValid(idxoid) ||
		   (replident == REPLICA_IDENTITY_FULL && has_oldtup));


	if (OidIsValid(idxoid))
	{
		found = RelationFindReplTupleByIndex(rel, idxoid,
													LockTupleExclusive,
													remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
											remoteslot, localslot);
	}

	
	ExecClearTuple(remoteslot);

	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		ExecStoreHeapTuple(localslot->tts_tuple, remoteslot, false);
		logical_apply_slot_modify_cstrings(remoteslot, rel, newtup.values, newtup.changed);
		MemoryContextSwitchTo(oldctx);

		EvalPlanQualSetSlot(&epqstate, remoteslot);

		/* Do the actual update. */
		ExecSimpleRelationUpdate(estate, &epqstate, localslot, remoteslot);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * TODO what to do here, change the log level to LOG perhaps?
		 */
		elog(DEBUG1,
			 "logical apply did not find row for update "
			 "in apply target relation \"%s\"",
			 RelationGetRelationName(rel));
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);

	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * logical apply delete message from CN
 */
static void
logical_apply_delete(StringInfo s)
{
	Relation 			rel = NULL;
	LogicalRepTupleData oldtup;
	Oid			idxoid = InvalidOid;
	EState	   *estate = NULL;
	EPQState	epqstate;
	TupleTableSlot *remoteslot = NULL;
	TupleTableSlot *localslot = NULL;
	bool		found = false;
	MemoryContext oldctx = NULL;

	char	   *nspname = NULL;
	char	   *relname = NULL;
	char		replident = 0;

	MemSet(&epqstate, 0, sizeof(EPQState));
	MemSet(&oldtup, 0, sizeof(LogicalRepTupleData));

	logicalrep_read_delete(s, &nspname, &relname, &replident, &oldtup, NULL);

	rel = relation_openrv(makeRangeVar(nspname, relname, -1), RowExclusiveLock);

	/* Check for supported relkind. */
	CheckSubscriptionRelkind(rel->rd_rel->relkind, nspname, relname);

	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	localslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);

	/* Find the tuple using the replica identity index. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	logical_apply_slot_store_cstrings(remoteslot, rel, oldtup.values);
	MemoryContextSwitchTo(oldctx);

	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel);
	Assert(OidIsValid(idxoid) ||
		   (replident == REPLICA_IDENTITY_FULL));

	if (OidIsValid(idxoid))
	{
		
		found = RelationFindReplTupleByIndex(rel, idxoid,
												LockTupleExclusive,
												remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
											remoteslot, localslot);
	}
	/* If found delete it. */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);

		/* Do the actual delete. */
		ExecSimpleRelationDelete(estate, &epqstate, localslot);
	}
	else
	{
		/* The tuple to be deleted could not be found. */
		ereport(DEBUG1,
				(errmsg("logical apply could not find row for delete "
						"in apply target %s",
						RelationGetRelationName(rel))));
	}

	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);

	heap_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}
/*
 * Logical apply protocol message dispatcher.
 */
void logical_apply_dispatch(StringInfo s)
{
	char action = pq_getmsgbyte(s);

	OpenTenBaseSubscriptionApplyWorkerSet();

	switch (action)
	{
			/* INSERT */
		case 'I':
			logical_apply_insert(s);
			break;
			/* UPDATE */
		case 'U':
			logical_apply_update(s);
			break;
			/* DELETE */
		case 'D':
			logical_apply_delete(s);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid logical apply message type %c", action)));
	}
}

/*
 * logical apply batch insert messages from CN
 */
static void
logical_apply_batch_insert(StringInfo s)
{
	ApplyChangeItem **insert_change_items = NULL;
	TupleTableSlot **remote_slots = NULL;
	Relation rel = NULL;
	MemoryContext oldctx = NULL;
	EState *estate = NULL;
	
	char *nspname = NULL;
	char *relname = NULL;
	char replident = 0;
	
	unsigned int n_diff_changes = 0;
	int i = 0;
	
	/* read the relation id */
	logicalrep_read_batch_changes(s, &nspname, &relname, &replident, &insert_change_items, &n_diff_changes);
	if (n_diff_changes == 0)
	{
		ereport(DEBUG1,
		        (errmsg("No Changes to APPLY in this batch.")));
		return;
	}
	
	rel = relation_openrv(makeRangeVar(nspname, relname, -1), RowExclusiveLock);
	
	/* Check for supported relkind. */
	CheckSubscriptionRelkind(rel->rd_rel->relkind, nspname, relname);
	
	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remote_slots = (TupleTableSlot **) palloc0(n_diff_changes * sizeof(TupleTableSlot *));
	for (i = 0; i < n_diff_changes; i++)
	{
		remote_slots[i] = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	}
	
	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	
	for (i = 0; i < n_diff_changes; i++)
	{
		logical_apply_slot_store_cstrings(remote_slots[i], rel, insert_change_items[i]->tuple_data->values);
		logical_apply_slot_fill_defaults(rel, estate, remote_slots[i], insert_change_items[i]->tuple_data->values);
	}
	MemoryContextSwitchTo(oldctx);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);
	
	/* Do the batch insert. */
	Assert(insert_change_items != NULL);
	ExecSimpleRelationBatchInsert(estate, insert_change_items, n_diff_changes, remote_slots);
	
	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	
	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);
	
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);
	
	heap_close(rel, RowExclusiveLock);
	
	CommandCounterIncrement();
	
	for (i = 0; i < n_diff_changes; ++i)
	{
		pfree_ext(insert_change_items[i]->tuple_data);
		ExecDropSingleTupleTableSlot(remote_slots[i]);
		pfree_ext(insert_change_items[i]);
	}
	pfree_ext(insert_change_items);
}

/*
 * logical apply batch delete messages from CN
 */
static void
logical_apply_batch_delete(StringInfo s)
{
	Relation rel = NULL;
	EState *estate = NULL;
	MemoryContext oldctx = NULL;
	ApplyChangeItem **delete_change_items = NULL;
	unsigned int n_diff_changes = 0;
	Oid idxoid = InvalidOid;
	
	char *nspname = NULL;
	char *relname = NULL;
	char replident = 0;
	
	int i = 0;
	
	logicalrep_read_batch_changes(s, &nspname, &relname, &replident, &delete_change_items, &n_diff_changes);
	
	if (n_diff_changes == 0)
	{
		ereport(DEBUG1,
				(errmsg("No Changes to APPLY in this batch.")));
		return;
	}
	
	rel = relation_openrv(makeRangeVar(nspname, relname, -1), RowExclusiveLock);
	
	/* Check for supported relkind. */
	CheckSubscriptionRelkind(rel->rd_rel->relkind, nspname, relname);
	
	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	
	for (i = 0; i < n_diff_changes; ++i)
	{
		delete_change_items[i]->table_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	}
	
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	for (i = 0; i < n_diff_changes; ++i)
	{
		logical_apply_slot_store_cstrings(delete_change_items[i]->table_slot, rel, delete_change_items[i]->tuple_data->values);
		logical_apply_slot_fill_defaults(rel, estate, delete_change_items[i]->table_slot, delete_change_items[i]->tuple_data->values);
	}
	MemoryContextSwitchTo(oldctx);
	Assert(delete_change_items != NULL);
	
	/* For now we support only tables. */
	Assert(rel->rd_rel->relkind == RELKIND_RELATION);
	CheckCmdReplicaIdentity(rel, CMD_DELETE);
	
	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel);
	
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
	
	/* use local snapshot instead of global snapshot */
	ExecOpenIndices(estate->es_result_relation_info, false);
	
	for (i = 0; i < n_diff_changes; ++i)
	{
		int repeat = delete_change_items[i]->repeat;
		TupleTableSlot *searchslot = delete_change_items[i]->table_slot;
		
		Assert(OidIsValid(idxoid) || (replident == REPLICA_IDENTITY_FULL));
		
		do
		{
			bool found = false;
			TupleTableSlot *localslot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
			
			if (OidIsValid(idxoid))
			{
				found = RelationFindReplTupleByIndex(rel, idxoid, LockTupleExclusive, searchslot, localslot);
			}
			else
			{
				found = RelationFindReplTupleSeq(rel, LockTupleExclusive, searchslot, localslot);
			}
			/* If found delete it. */
			if (found)
			{
				simple_heap_delete(rel, &localslot->tts_tuple->t_self);
			}
			else
			{
				/* The tuple to be deleted could not be found. */
				ereport(DEBUG1,
						(errmsg("logical apply could not find row for delete "
								"in apply target %s",
								RelationGetRelationName(rel))));
			}
			
			/* Always release resources and reset the slot to empty */
			ExecDropSingleTupleTableSlot(localslot);
			CommandCounterIncrement();
		} while (--repeat > 0);
	}
	
	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	
	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);
	
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);
	heap_close(rel, RowExclusiveLock);
	
	for (i = 0; i < n_diff_changes; ++i)
	{
		ExecDropSingleTupleTableSlot(delete_change_items[i]->table_slot);
		pfree_ext(delete_change_items[i]->tuple_data);
		pfree_ext(delete_change_items[i]);
	}
	pfree_ext(delete_change_items);
	CommandCounterIncrement();
}

/*
 * Batch Logical apply protocol message dispatcher.
 */
void logical_apply_dispatch_batch(StringInfo s)
{
	char action = pq_getmsgbyte(s);
	
	OpenTenBaseSubscriptionApplyWorkerSet();
	
	switch (action)
	{
		/* INSERT */
		case 'I':
			logical_apply_batch_insert(s);
			break;
			/* DELETE */
		case 'D':
			logical_apply_batch_delete(s);
			break;
		default:
			ereport(ERROR,
			        (errcode(ERRCODE_PROTOCOL_VIOLATION),
					        errmsg("invalid logical apply message type %c", action)));
	}
}
#endif

/*
 * logical apply insert message from TDX
 */
static void
tdx_logical_apply_insert(StringInfo s, TdxSyncDesc tdxDesc)
{
	Relation rel = tdxDesc->rel;
	LogicalRepTupleData newtup;
	EState *estate = NULL;
	TupleTableSlot *remoteslot = NULL;
	MemoryContext oldctx = NULL;
	
	MemSet(&newtup, 0, sizeof(LogicalRepTupleData));
	tdx_logicalrep_read_insert(s, &newtup);
	
	/* We currently only support writing to regular tables. */
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
		        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				        errmsg("logical replication target relation \"%s.%s\" is not a table",
				               tdxDesc->nspname, tdxDesc->relname)));
	
	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	
	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	logical_apply_slot_store_cstrings(remoteslot, rel, newtup.values);
	logical_apply_slot_fill_defaults(rel, estate, remoteslot, newtup.values);
	MemoryContextSwitchTo(oldctx);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);
	
	/* Do the insert. */
	ExecSimpleRelationInsert(estate, remoteslot);
	
	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	
	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);
	
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);
	
	CommandCounterIncrement();
}

/*
 * logical apply update message from TDX
 */
static void
tdx_logical_apply_update(StringInfo s, TdxSyncDesc tdxDesc)
{
	Relation rel = tdxDesc->rel;
	Oid idxoid = InvalidOid;
	EState *estate = NULL;
	EPQState epqstate;
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool has_oldtup = false;
	TupleTableSlot *localslot = NULL;
	TupleTableSlot *remoteslot = NULL;
	bool found = false;
	MemoryContext oldctx = NULL;
	
	char replident = 0;
	
	MemSet(&epqstate, 0, sizeof(EPQState));
	MemSet(&oldtup, 0, sizeof(LogicalRepTupleData));
	MemSet(&newtup, 0, sizeof(LogicalRepTupleData));
	
	tdx_logicalrep_read_update(s, &has_oldtup, &oldtup, &newtup, &replident);
	
	if (!tdxDesc->updatable)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				        errmsg("TDX target relation \"%s.%s\" has no replica identity",
				               tdxDesc->nspname, tdxDesc->relname)));
	}
	
	/* We currently only support writing to regular tables. */
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
		        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				        errmsg("logical replication target relation \"%s.%s\" is not a table",
				               tdxDesc->nspname, tdxDesc->relname)));
	
	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	localslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);
	
	/* Build the search tuple. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	logical_apply_slot_store_cstrings(remoteslot, rel,
	                                  has_oldtup ? oldtup.values : newtup.values);
	MemoryContextSwitchTo(oldctx);
	
	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel);

	if (OidIsValid(idxoid))
	{
		found = RelationFindReplTupleByIndex(rel, idxoid,
												LockTupleExclusive,
												remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
											remoteslot, localslot);
	}
	
	ExecClearTuple(remoteslot);
	
	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		ExecStoreHeapTuple(localslot->tts_tuple, remoteslot, false);
		logical_apply_slot_modify_cstrings(remoteslot, rel, newtup.values, newtup.changed);
		MemoryContextSwitchTo(oldctx);
		
		EvalPlanQualSetSlot(&epqstate, remoteslot);
		
		/* Do the actual update. */
		ExecSimpleRelationUpdate(estate, &epqstate, localslot, remoteslot);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * TODO what to do here, change the log level to LOG perhaps?
		 */
		elog(DEBUG1,
		     "logical apply did not find row for update "
		     "in apply target relation \"%s\"",
		     RelationGetRelationName(rel));
	}
	
	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	
	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);
	
	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);
	CommandCounterIncrement();
}

/*
 * logical apply delete message from TDX
 */
static void
tdx_logical_apply_delete(StringInfo s, TdxSyncDesc tdxDesc)
{
	Relation rel = tdxDesc->rel;
	LogicalRepTupleData oldtup;
	Oid idxoid = InvalidOid;
	EState *estate = NULL;
	EPQState epqstate;
	TupleTableSlot *remoteslot = NULL;
	TupleTableSlot *localslot = NULL;
	bool found = false;
	MemoryContext oldctx = NULL;
	
	char replident = 0;
	
	MemSet(&epqstate, 0, sizeof(EPQState));
	MemSet(&oldtup, 0, sizeof(LogicalRepTupleData));
	
	tdx_logicalrep_read_delete(s, &oldtup, &replident);
	
	if (!tdxDesc->updatable)
	{
		ereport(ERROR,
		        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				        errmsg("TDX target relation \"%s.%s\" has no replica identity",
				               tdxDesc->nspname, tdxDesc->relname)));
	}
	/* We currently only support writing to regular tables. */
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
		        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				        errmsg("logical replication target relation \"%s.%s\" is not a table",
				               tdxDesc->nspname, tdxDesc->relname)));
	
	/* Initialize the executor state. */
	estate = logical_apply_create_estate_for_rel(rel);
	remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	localslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(rel));
	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);

#ifdef __STORAGE_SCALABLE__
	/* use local snapshot instead of global snapshot */
	PushActiveSnapshot(GetLocalTransactionSnapshot());
#else
	PushActiveSnapshot(GetTransactionSnapshot());
#endif
	ExecOpenIndices(estate->es_result_relation_info, false);
	
	/* Find the tuple using the replica identity index. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	logical_apply_slot_store_cstrings(remoteslot, rel, oldtup.values);
	MemoryContextSwitchTo(oldctx);
	
	/*
	 * Try to find tuple using either replica identity index, primary key or
	 * if needed, sequential scan.
	 */
	idxoid = GetRelationIdentityOrPK(rel);
	Assert(OidIsValid(idxoid) ||
	       (replident == REPLICA_IDENTITY_FULL));
	
	if (OidIsValid(idxoid))
	{
		
		found = RelationFindReplTupleByIndex(rel, idxoid,
												LockTupleExclusive,
												remoteslot, localslot);
	}
	else
	{
		found = RelationFindReplTupleSeq(rel, LockTupleExclusive,
											remoteslot, localslot);
	}

	/* If found delete it. */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);
		
		/* Do the actual delete. */
		ExecSimpleRelationDelete(estate, &epqstate, localslot);
	}
	else
	{
		/* The tuple to be deleted could not be found. */
		ereport(DEBUG1,
		        (errmsg("logical apply could not find row for delete "
		                "in apply target %s",
		                RelationGetRelationName(rel))));
	}
	
	/* Cleanup. */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	
	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(estate);
	
	EvalPlanQualEnd(&epqstate);
	ExecResetTupleTable(estate->es_tupleTable, false);

	FreeExecutorState(estate);
	CommandCounterIncrement();
}

/*
 * TDX Logical replication protocol message dispatcher.
 */
void
tdx_apply_dispatch(StringInfo s, TdxSyncDesc tdxDesc)
{
	char action = pq_getmsgbyte(s);
	
	switch (action)
	{
		/* INSERT */
		case 'I':
			tdx_logical_apply_insert(s, tdxDesc);
			break;
			/* UPDATE */
		case 'U':
			tdx_logical_apply_update(s, tdxDesc);
			break;
			/* DELETE */
		case 'D':
			tdx_logical_apply_delete(s, tdxDesc);
			break;
		default:
		{
			ereport(ERROR,
			        (errcode(ERRCODE_PROTOCOL_VIOLATION),
					        errmsg("invalid logical replication message type %c", action)));
		}
	}
}
