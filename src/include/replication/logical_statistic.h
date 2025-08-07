/* -------------------------------------------------------------------------
 *
 * pg_subscription_statistic.h
 *		Statistic info about shards of a subscription (pg_subscription_statistic).
 *
 * Portions Copyright (c) 2018, Tencent OpenTenBase Group
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SUBSCRIPTION_STATISTIC_H
#define PG_SUBSCRIPTION_STATISTIC_H

#include "postgres.h"
#include "fmgr.h"



extern int32 g_PubStatHashSize;
extern int32 g_PubTableStatHashSize;
extern int32 g_SubStatHashSize;
extern int32 g_SubTableStatHashSize;


/* ----------------
 *		state constants
 * ----------------
 */
#define STATE_INIT		'i' /* initializing (sublsn NULL) */
#define STATE_DATACOPY	'c' /* copy data to relation */
#define STATE_COPYDONE	'd' /* finished to copy data */
#define STATE_APPLY		'a' /* apply logical log */
#define STATE_SYNCDONE  's' /* finish to sync relation */

extern Size PubStatDataShmemSize(int ssize, int tsize);

extern void InitPubStatData(int ssize, int tsize);

extern Size SubStatDataShmemSize(int ssize, int tsize);

extern void InitSubStatData(int ssize, int tsize);

extern void UpdatePubStatistics(char *subname, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init);
extern void UpdatePubTableStatistics(Oid subid, Oid relid, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init);

extern void RemovePubStatistics(char *subname);

extern void SetPubStatCheck(bool pubstatcount, bool pubstatchecksum);

extern void UpdateSubStatistics(char *subname, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, bool init);
extern void UpdateSubTableStatistics(Oid subid, Oid relid, uint64 ntups_copy, uint64 ntups_insert, uint64 ntups_delete,
                                        uint64 checksum_insert, uint64 checksum_delete, char state, bool init);
extern void RemoveSubStatistics(char *subname);

extern void SetSubStatCheck(bool substatcount, bool substatchecksum);

extern void GetSubTableEntry(Oid subid, Oid relid, void **entry, CmdType cmd);

extern Datum opentenbase_get_pub_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_all_pub_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_sub_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_all_sub_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_pubtable_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_all_pubtable_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_subtable_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_all_subtable_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_set_pub_stat_check(PG_FUNCTION_ARGS);
extern Datum opentenbase_set_sub_stat_check(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_pub_stat_check(PG_FUNCTION_ARGS);
extern Datum opentenbase_get_sub_stat_check(PG_FUNCTION_ARGS);
extern Datum opentenbase_remove_pub_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_remove_pubtable_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_remove_sub_stat(PG_FUNCTION_ARGS);
extern Datum opentenbase_remove_subtable_stat(PG_FUNCTION_ARGS);
#endif							/* PG_SUBSCRIPTION_STATISTIC_H */
