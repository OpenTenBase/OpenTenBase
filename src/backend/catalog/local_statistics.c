/*-------------------------------------------------------------------------
 *
 * local_statistics.c
 *	  code to get and set local statistics in transaction for ANALYZE (LOCAL)
 *
 * IDENTIFICATION
 *	  src/backend/catalog/local_statistics.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/local_statistics.h"
#include "catalog/pg_statistic.h"
#include "funcapi.h"
#include "utils/catcache.h"

#define LOCAL_STATISTIC_HASH_SIZE		1024

HTAB *local_statistic_hash = NULL;
static MemoryContext local_statistic_context = NULL;

static void
LocalStatisticHashInit(void)
{
	if (!local_statistic_hash)
	{
		HASHCTL		ctl;

		if (!CacheMemoryContext)
			CreateCacheMemoryContext();

		local_statistic_context = AllocSetContextCreate(CacheMemoryContext,
												 "local statistic context",
												 ALLOCSET_DEFAULT_SIZES);

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(local_statistic_hash_entry);
		ctl.hcxt = local_statistic_context;
		local_statistic_hash = hash_create("local statistic info",
											 LOCAL_STATISTIC_HASH_SIZE,
											 &ctl, HASH_ELEM | HASH_BLOBS);
	}
}

static void
LocalStatisticFree(local_statistic_hash_entry *entry)
{
	int i;

	for (i = 0; i < entry->natts; i++)
	{
		if (entry->att_stat_tups[i])
		{
			heap_freetuple(entry->att_stat_tups[i]);
			entry->att_stat_tups[i] = NULL;
		}

		if (entry->att_stat_tups_inh[i])
		{
			heap_freetuple(entry->att_stat_tups_inh[i]);
			entry->att_stat_tups_inh[i] = NULL;
		}
	}

	if (entry->attnum)
		pfree(entry->attnum);

	if (entry->att_stat_tups)
		pfree(entry->att_stat_tups);

	if (entry->att_stat_tups_inh)
		pfree(entry->att_stat_tups_inh);
}

void
LocalStatisticRemoveAttrStats(Oid relid, int attnum)
{
	local_statistic_hash_entry	*entry = NULL;
	int		i;

	if (!local_statistic_hash)
		return;

	entry = hash_search(local_statistic_hash, (void *) &(relid), HASH_FIND, NULL);
	if (entry == NULL)
		return;

	for (i = 0; i < entry->natts; i++)
	{
		if (entry->attnum[i] != attnum)
			continue;

		Assert(entry->att_stat_tups[i]);

		entry->attnum[i] = 0;
		if (entry->att_stat_tups[i])
			heap_freetuple(entry->att_stat_tups[i]);
		entry->att_stat_tups[i] = NULL;

		if (entry->att_stat_tups_inh[i])
			heap_freetuple(entry->att_stat_tups_inh[i]);
		entry->att_stat_tups_inh[i] = NULL;

		break;
	}
}

void
LocalStatisticRemoveByRelid(Oid relid)
{
	local_statistic_hash_entry	*entry = NULL;

	if (!local_statistic_hash)
		return;

	entry = hash_search(local_statistic_hash, (void *) &(relid), HASH_FIND, NULL);

	if (entry != NULL)
	{
		LocalStatisticFree(entry);
		hash_search(local_statistic_hash, (void *) &(relid), HASH_REMOVE, NULL);
	}
}

void
LocalStatisticRemoveAll(void)
{
	HASH_SEQ_STATUS status;
	local_statistic_hash_entry *entry;

	if (!local_statistic_hash)
		return;

	hash_seq_init(&status, local_statistic_hash);
	while ((entry = (local_statistic_hash_entry *)hash_seq_search(&status)) != NULL)
	{
		LocalStatisticFree(entry);
		hash_search(local_statistic_hash, (void *) &(entry->relid), HASH_REMOVE, NULL);
	}
}

void
LocalStatisticSetRelStats(Relation relation, BlockNumber relpages, BlockNumber relallvisible,
						  double livetuples, double deadtuples, bool enter)
{
	Oid	relid = RelationGetRelid(relation);
	local_statistic_hash_entry	*entry = NULL;
	bool	found;
	HASHACTION action = enter ? HASH_ENTER : HASH_FIND;

	if (!OidIsValid(relid))
		return;

	if (!local_statistic_hash)
		LocalStatisticHashInit();

	entry = hash_search(local_statistic_hash, (void *) &(relid), action, &found);

	if (!enter && !found)
		return;

	if (!found)
	{
		int natts;
		MemoryContext oldcontext;

		natts = RelationGetNumberOfAttributes(relation);
		oldcontext = MemoryContextSwitchTo(local_statistic_context);

		entry->relpages = 0;
		entry->relallvisible = 0;
		entry->livetuples = 0;
		entry->deadtuples = 0;
		entry->attnum = (int *) palloc0(sizeof(int) * natts);
		entry->att_stat_tups = (HeapTuple *) palloc0(sizeof(HeapTuple) * natts);
		entry->att_stat_tups_inh = (HeapTuple *) palloc0(sizeof(HeapTuple) * natts);
		entry->natts = natts;

		MemoryContextSwitchTo(oldcontext);
	}
	
	if (relpages >= 0)
	{
		entry->relpages = (int32)relpages;
		relation->rd_rel->relpages = (int32) relpages;
	}

	if (relallvisible >= 0)
	{
		entry->relallvisible = (int32)relallvisible;
		relation->rd_rel->relallvisible = (int32) relallvisible;
	}

	if (livetuples >= 0)
	{
		entry->livetuples = (float4)livetuples;
		relation->rd_rel->reltuples = (float4)livetuples;
	}
	
	if (deadtuples >= 0)
	{
		entry->deadtuples = (float4)deadtuples;
	}
}

bool
LocalStatisticGetRelStats(Oid relid, BlockNumber *relpages, BlockNumber *relallvisible, double *livetuples, double *deadtuples)
{
	local_statistic_hash_entry	*entry = NULL;

	if (!local_statistic_hash)
		return false;

	entry = hash_search(local_statistic_hash, (void *) &(relid), HASH_FIND, NULL);
	if (entry == NULL)
		return false;

	Assert(entry->relid == relid);

	if (relpages)
		*relpages = entry->relpages;

	if (relallvisible)
		*relallvisible = entry->relallvisible;
	
	if (livetuples)
		*livetuples = entry->livetuples;

	if (deadtuples)
		*deadtuples = entry->deadtuples;

	return true;
}

static void
LocalStatisticExtend(local_statistic_hash_entry *entry, int natts)
{

	int 		*attNum;
	HeapTuple	*attStatTups;
	HeapTuple	*attStatTupsInh;
	MemoryContext oldContext;
	int			i;

	Assert(entry != NULL);
	Assert(entry->natts < natts);

	oldContext = MemoryContextSwitchTo(local_statistic_context);

	attNum = (int *) palloc0(sizeof(int) * natts);
	attStatTups = (HeapTuple *) palloc0(sizeof(HeapTuple) * natts);
	attStatTupsInh = (HeapTuple *) palloc0(sizeof(HeapTuple) * natts);

	MemoryContextSwitchTo(oldContext);

	for (i = 0; i < entry->natts; i++)
	{
		if (entry->attnum[i] == 0)
			continue;

		attNum[i] = entry->attnum[i];
		attStatTups[i] = entry->att_stat_tups[i];
		attStatTupsInh[i] = entry->att_stat_tups_inh[i];
	}

	pfree(entry->attnum);
	pfree(entry->att_stat_tups);
	pfree(entry->att_stat_tups_inh);

	entry->natts = natts;
	entry->attnum = attNum;
	entry->att_stat_tups = attStatTups;
	entry->att_stat_tups_inh = attStatTupsInh;
}

void
LocalStatisticUpdateAttrStats(Oid relid, int attnum, int natts, TupleDesc tupleDescriptor, bool inh, Datum *values, bool *isnull)
{
	local_statistic_hash_entry	*entry;
	MemoryContext			oldcontext;
	int						i = 0;
	int						first_unused_slot = -1;
	bool					found;

	if (!local_statistic_hash)
		LocalStatisticHashInit();

	entry = hash_search(local_statistic_hash, (void *) &(relid), HASH_ENTER, &found);

	if (!found)
	{
		oldcontext = MemoryContextSwitchTo(local_statistic_context);

		entry->relpages = 0;
		entry->relallvisible = 0;
		entry->livetuples = 0;
		entry->deadtuples = 0;
		entry->attnum = (int *) palloc0(sizeof(int) * natts);
		entry->att_stat_tups = (HeapTuple *) palloc0(sizeof(HeapTuple) * natts);
		entry->att_stat_tups_inh = (HeapTuple *) palloc0(sizeof(HeapTuple) * natts);
		entry->natts = natts;

		MemoryContextSwitchTo(oldcontext);
	}

	if (entry->natts < natts)
		LocalStatisticExtend(entry, natts);

	Assert(entry->relid == relid);
	for (i = 0; i < entry->natts; i++)
	{
		if (entry->attnum[i] == attnum)
		{
			break;
		}

		if (entry->attnum[i] == 0 && first_unused_slot == -1)
			first_unused_slot = i;

	}

	if (i == entry->natts)
	{
		i = first_unused_slot;
		entry->attnum[i] = attnum;
	}

	Assert(i >= 0);

	oldcontext = MemoryContextSwitchTo(local_statistic_context);
	if (!inh)
	{
		if (entry->att_stat_tups[i])
			heap_freetuple(entry->att_stat_tups[i]);
		entry->att_stat_tups[i] = heap_form_tuple(tupleDescriptor, values, isnull);
	}
	else
	{
		if (entry->att_stat_tups_inh[i])
			heap_freetuple(entry->att_stat_tups_inh[i]);
		entry->att_stat_tups_inh[i] = heap_form_tuple(tupleDescriptor, values, isnull);
	}
	MemoryContextSwitchTo(oldcontext);
}

HeapTuple
LocalStatisticGetAttrStats(Oid relid, int attnum, bool inh)
{
	local_statistic_hash_entry	*entry;
	int						i = 0;

	if (!local_statistic_hash)
		return NULL;

	entry = hash_search(local_statistic_hash, (void *) &(relid), HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	for (i = 0; i < entry->natts; i++)
	{
		if (entry->attnum[i] == attnum)
		{
			if (!inh)
			{
				Assert(entry->att_stat_tups[i]);
				return entry->att_stat_tups[i];
			}
			else
			{
				Assert(entry->att_stat_tups_inh[i]);
				return entry->att_stat_tups_inh[i];
			}
		}
	}

	return NULL;
}

void
LocalStatisticReleaseCache(HeapTuple tup)
{
	return;
}
