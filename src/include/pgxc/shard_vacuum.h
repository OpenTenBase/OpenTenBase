#ifndef SHARD_VACUUM_H
#define SHARD_VACUUM_H


typedef enum
{
	SHARD_VISIBLE_CHECK_VISIBLE,
	SHARD_VISIBLE_CHECK_HIDDEN
}ShardVisibleCheckMode;

extern List *string_to_shard_list(char *str);
extern List *string_to_reloid_list(char *str);

extern int64 vacuum_shard_internal(Relation rel, Bitmapset *to_vacuum, Snapshot vacuum_snapshot, int sleep_interval, bool to_delete);

extern int64 vacuum_shard(Relation rel, Bitmapset *to_vacuum, Snapshot vacuum_snapshot, bool to_delete);

#define VACUUM_SHARD_SLEEP_INTERVAL_DEFALUT 20  /* sleep sleep_interval ms after every 2000 deleted tuples, default 20ms */

extern Datum vacuum_hidden_shards(PG_FUNCTION_ARGS);

extern void check_shardlist_visiblility(List *shard_list, ShardVisibleCheckMode visible_mode);
#endif
