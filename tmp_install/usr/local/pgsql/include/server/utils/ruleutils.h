/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *        Declarations for ruleutils.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/utils/ruleutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RULEUTILS_H
#define RULEUTILS_H

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "storage/lockdefs.h"

extern char *pg_get_indexdef_string(Oid indexrelid);
extern char *pg_get_indexdef_columns(Oid indexrelid, bool pretty);

extern char *pg_get_partkeydef_columns(Oid relid, bool pretty);

extern char *pg_get_constraintdef_command(Oid constraintId);
extern char *deparse_expression(Node *expr, List *dpcontext,
                   bool forceprefix, bool showimplicit);
extern List *deparse_context_for(const char *aliasname, Oid relid);
extern List *deparse_context_for_plan_rtable(List *rtable, List *rtable_names);
extern List *set_deparse_context_planstate(List *dpcontext,
                              Node *planstate, List *ancestors);
extern List *select_rtable_names_for_explain(List *rtable,
                                Bitmapset *rels_used);
extern char *generate_collation_name(Oid collid);
extern char *get_range_partbound_string(List *bound_datums);
extern void add_day_calculation(int *year, int *mon, int *day, int step, int steptype, bool is_leap_year);

#ifdef __OPENTENBASE__
extern char * GetPartitionName(Oid parentrelid, int partidx, bool isindex);

extern int RelationGetPartitionIdxByValue(Relation rel, Datum value);

extern List *RelationGetAllPartitions(Relation rel);
extern List *RelationGetAllPartitionsWithLock(Relation rel, LOCKMODE lockmode);
extern int GetAllPartitionIntervalCount(Oid parent_oid);

extern int RelationGetChildIndex(Relation rel, Oid childoid);

extern Oid RelationGetPartitionIndex(Relation rel, Oid indexOid, int partidx);

extern Oid RelationGetPartition(Relation rel, int partidx, bool isindex);

extern Bitmapset *RelationGetPartitionByValue(Relation rel, Const *value);

extern void replace_target_relation(Node *node, Index targetrel, Relation partitionparent, int partidx);

extern void replace_partidx_bitmapheapscan(Relation relation, Node *plan, int partidx);

extern Bitmapset *RelationGetPartitionsByQuals(Relation rel, List *strictinfos);

extern int32 get_timestamptz_gap(TimestampTz value, int32 interval);

extern int32 get_timestamptz_diff(TimestampTz value, int32 interval);

extern int32 date_diff(struct pg_tm *user_time);

extern int32 date_diff_indays(struct pg_tm *user_time);

extern int get_months_away_from_base(struct pg_tm * user_tm);

extern int get_days_away_from_base(struct pg_tm * user_tm);

extern bool is_sec_meet_temp_cold_date(TimestampTz secvalue, int32 interval, int step, TimestampTz startValue);

extern int32 GetPartitionIndex(TimestampTz start, int step, int steptype, int partitions, TimestampTz value);

extern bool is_first_day_from_start(int step, int steptype, struct pg_tm *start_time, struct pg_tm *current_time);
#endif

#endif                            /* RULEUTILS_H */
