
#include <stdlib.h>
#include <errno.h>
#include <ctype.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "pgxc/shard_vacuum.h"
#include "pgxc/pgxc.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_shard_map.h"
#include "catalog/namespace.h"
#include "nodes/bitmapset.h"
#include "utils/builtins.h"
#include "utils/tqual.h"
#include "utils/snapshot.h"
#include "utils/ruleutils.h"
#include "access/genam.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"


static void
split_shard_from_tablename(char *str, char **shards_str, char **tablename_str, int *sleep_interval);

static char *trimwhitespace(char *str);



Datum
vacuum_hidden_shards(PG_FUNCTION_ARGS)
{
	//arg: text   "321,87654,89765"
	char 		*arg = NULL;
	char		*shards_str = NULL;
	char		*tablename_str = NULL;
	int			sleep_interval = 0;
	List		*table_oid_list = NULL;
	List 		*shards = NULL;
	ListCell 	*lc;
	int			shard;
	Bitmapset 	*shardmap = NULL;
	Bitmapset	*to_vacuum = NULL;
	int64		vacuumed_rows = 0;
	Snapshot	vacuum_snapshot = NULL;
	bool		need_new_snapshot = false;
	SnapshotSatisfiesFunc snapfunc_tmp = NULL;

	Oid			reloid;

	if(!IS_PGXC_DATANODE)
		elog(ERROR, "this function can only be called in datanode");

	/* get shards to be vacuumed */	
	arg = text_to_cstring(PG_GETARG_TEXT_P(0));
	split_shard_from_tablename(arg, &shards_str, &tablename_str, &sleep_interval);
	
	shards = string_to_shard_list(shards_str);
	table_oid_list = string_to_reloid_list(tablename_str);	

	vacuum_snapshot = GetActiveSnapshot();
	if(!vacuum_snapshot)
	{
		need_new_snapshot = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		vacuum_snapshot = GetActiveSnapshot();
	}
	shardmap = vacuum_snapshot->shardgroup;

	if(!need_new_snapshot)
		snapfunc_tmp = vacuum_snapshot->satisfies;
	vacuum_snapshot->satisfies = HeapTupleSatisfiesUnshard;

	check_shardlist_visiblility(shards, SHARD_VISIBLE_CHECK_HIDDEN);

	/* check if the shard is hidden */
	foreach(lc, shards)
	{
		shard = lfirst_int(lc);

		if(bms_is_member(shard, shardmap))
			elog(ERROR, "shard %d is exist in share memory shardmap right now, it can not to be vacuumed", shard);
		
		to_vacuum = bms_add_member(to_vacuum, shard);
	}

	list_free(shards);

	/* get shard relations */
	if(list_length(table_oid_list) == 0)
		table_oid_list = GetShardRelations(true);

	/* vacumm rows belong to specifiend shards */
	foreach(lc, table_oid_list)
	{
		Relation shardrel;
		reloid = lfirst_oid(lc);
		shardrel = heap_open(reloid,RowExclusiveLock);

		vacuumed_rows += vacuum_shard_internal(shardrel, to_vacuum, vacuum_snapshot, sleep_interval, true);
		
		heap_close(shardrel,RowExclusiveLock);
	}

	bms_free(to_vacuum);

	if(!need_new_snapshot)
		vacuum_snapshot->satisfies = snapfunc_tmp;
	
	if(need_new_snapshot)
		PopActiveSnapshot();
	
	PG_RETURN_INT64(vacuumed_rows);
}


List *
string_to_shard_list(char *str)
{
	List 	*shards = NIL;
	char 	*shard_str = NULL;
	int	shard;

	if(str == NULL)
		return NULL;

	shard_str = strtok(str, ", ");

	while(shard_str)
	{
		errno = 0;    /* To distinguish success/failure after call */
	    //shard = strtol(str, NULL, 10);
		shard = pg_atoi(shard_str, sizeof(int32), '\0');

		/* Check for various possible errors */

      	if ((errno == ERANGE && (shard == INT_MAX || shard == INT_MIN))
               || (errno != 0 && shard == 0)) {
       		elog(ERROR, "shard is invalid:%s", shard_str);
       	}

		if(shard >= INT_MAX || shard < 0)
			elog(ERROR, "shard is over the range:%d", shard);

		shards = lappend_int(shards, (int)shard);
		
		shard_str = strtok(NULL, ", ");
	}

	return shards;
}

List *
string_to_reloid_list(char *str)
{
	List	*tablename_list = NULL;
	List 	*tableoids = NULL;
	char	*table_str = NULL;
	char	*orig_table_str = NULL;
	ListCell	*tablename_cl = NULL;

	table_str = strtok(str, ",");

	while(table_str)
	{
		tablename_list = lappend(tablename_list, table_str);
		table_str = strtok(NULL, ", ");
	}

	if(list_length(tablename_list) == 0)
		return NULL;

	foreach(tablename_cl, tablename_list)
	{
		char 	*str1 = NULL;
		char 	*str2 = NULL;
		char 	*schema = NULL;
		char 	*relname = NULL;
		Oid		reloid = InvalidOid;
		
		table_str = lfirst(tablename_cl);
		if(table_str == NULL)
			continue;

		orig_table_str = pstrdup(table_str);

		str1 = strtok(table_str, ". ");
		if(str1)
			str2 = strtok(NULL, ". ");

		if(str1 && str2)
		{
			schema = str1;
			relname = str2;
		}
		else if(str1)
		{
			relname = str1;
		}
		else
		{
			elog(ERROR, "table name %s is invalid.", orig_table_str);
		}

		if(schema)
		{
			Oid namespace_oid;
			namespace_oid = get_namespace_oid(schema, false);
			reloid = get_relname_relid(relname, namespace_oid);
		}
		else
		{
			reloid = RelnameGetRelid(relname);
		}

		if(InvalidOid == reloid)			
			elog(ERROR, "table %s is not exist.", orig_table_str);

		tableoids = list_append_unique_oid(tableoids, reloid);
	}

	list_free(tablename_list);

	return tableoids;
}

void
split_shard_from_tablename(char *str, char **shards_str, char **tablename_str, int *sleep_interval)
{	
	char *clean_str = NULL;
	int pos = 0;

	*shards_str = NULL;
	*tablename_str = NULL;
	
	if(strlen(str) == 0)
	{
		return;
	}

	clean_str = trimwhitespace(str);
	
	if(strlen(clean_str) == 0)
	{
		return;
	}

	while(clean_str[pos] != '#' && clean_str[pos] != '\0') 
		pos++;

	if(pos == 0)
	{
		*shards_str = NULL;
	}
	else
	{
		*shards_str = (char *)palloc0(pos + 1);
		memcpy(*shards_str, clean_str, pos);
	}

	if(clean_str[pos] == '\0')
	{
		*tablename_str = NULL;
		*sleep_interval = VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT;
		return;
	}

	pos++;
	clean_str = clean_str + pos;
	pos = 0;
	
	while(clean_str[pos] != '#' && clean_str[pos] != '\0') 
		pos++;

	if(pos == 0)
	{
		*tablename_str = NULL;
	}
	else
	{
		*tablename_str = (char *)palloc0(pos + 1);
		memcpy(*tablename_str, clean_str, pos);
	}
	
	if(clean_str[pos] == '\0')
	{
		*sleep_interval = VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT;
		return;
	}

	pos++;
	clean_str = clean_str + pos;
	pos = 0;

	while(clean_str[pos] != '#' && clean_str[pos] != '\0') 
		pos++;

	if(pos == 0)
	{
		*sleep_interval = VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT;
	}
	else
	{
		char *str_sleep_interval = NULL;
		str_sleep_interval = (char *)palloc0(pos + 1);
		memcpy(str_sleep_interval, clean_str, pos);

		*sleep_interval = pg_atoi(str_sleep_interval, sizeof(int32), '\0');
		pfree(str_sleep_interval);
	}
	
/*
	if(clean_str[0] == '#')
	{
		*tablename_str = strtok(clean_str, "#");
	}
	else
	{
		*shards_str = strtok(clean_str, "#");

		if(*shards_str)
			*tablename_str = strtok(NULL, "#");
	}
*/	
	return;
}

int64 vacuum_shard(Relation rel, Bitmapset *to_vacuum, Snapshot vacuum_snapshot, bool to_delete)
{
	return vacuum_shard_internal(rel, to_vacuum, vacuum_snapshot, VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT, to_delete);
}

int64 vacuum_shard_internal(Relation rel, Bitmapset *to_vacuum, Snapshot vacuum_snapshot, int sleep_interval, bool to_delete)
{
	HeapScanDesc scan;
	HeapTuple	tup;
	int64 n = 0;
	int tuples = 0;

	if(!IS_PGXC_DATANODE)
		return 0;
	
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return 0;

	scan = heap_beginscan(rel, vacuum_snapshot, 0, NULL);
	tup = heap_getnext(scan,ForwardScanDirection);
	
	while(HeapTupleIsValid(tup))
	{
		if(!to_vacuum || bms_is_member(HeapTupleGetShardId(tup),to_vacuum))
		{
			n++;
			if(to_delete)
				simple_heap_delete(rel, &tup->t_self);
			
			if(to_delete)
			{
				tuples++;
				if(tuples > 2000)
				{
					tuples = 0;
					pg_usleep(sleep_interval * 1000);
				}
			}
		}
		tup = heap_getnext(scan,ForwardScanDirection);
	}

	heap_endscan(scan);

	return n;
}


char *trimwhitespace(char *str)
{
  char *end;

  // Trim leading space
  while(isspace(*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while(end > str && isspace(*end)) end--;

  // Write new null terminator
  *(end+1) = 0;

  return str;
}

void check_shardlist_visiblility(List *shard_list, ShardVisibleCheckMode visible_mode)
{
	Relation    shardmapRel;
	HeapTuple   oldtup;
	ScanKeyData skey[2];
	SysScanDesc scan;
	
	ListCell	*shard_lc = NULL;
	int 	shard = 0;

	if(!IS_PGXC_DATANODE)
		elog(ERROR, "this function can only be called in datanode");
	
	shardmapRel = heap_open(PgxcShardMapRelationId, AccessShareLock);
	foreach(shard_lc, shard_list)
	{
		shard = lfirst_int(shard_lc);

		/* in datanode, we use invalidoid */
		ScanKeyInit(&skey[0],
					Anum_pgxc_shard_map_nodegroup,
					BTEqualStrategyNumber, F_OIDEQ,
					Int32GetDatum(InvalidOid));

		ScanKeyInit(&skey[1],
					Anum_pgxc_shard_map_shardgroupid,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(shard));
			
		scan = systable_beginscan(shardmapRel,
									PgxcShardMapShardIndexId,true,
									NULL,
									2,
									skey);
	
		oldtup = systable_getnext(scan);

		if(visible_mode == SHARD_VISIBLE_CHECK_VISIBLE && !HeapTupleIsValid(oldtup))
		{
			elog(ERROR,"sharding group[%d] is not exist in node[%s]", shard, PGXCNodeName);
		}
		else if(visible_mode == SHARD_VISIBLE_CHECK_HIDDEN && HeapTupleIsValid(oldtup))
		{
			elog(ERROR,"sharding group[%d] is visible in node[%s]", shard, PGXCNodeName);
		}

		systable_endscan(scan);
	}
	
	heap_close(shardmapRel,AccessShareLock);
}

