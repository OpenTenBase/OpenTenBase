/*------------------------------------------------------------pg_spm_plan_history-------------
 *
 * pg_spm_plan_history.h
 *	  definition of the system "package" relation (pg_spm_plan_history)
 *	  along with the relation's initial contents.
 *
 * Portions Copyright (c) 2020-2030, Tencent.com.
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_spm_plan_history.h
 *
 * NOTES
 *	  The script catalog/genbki.pl reads this file and generates .bki
 *	  information from the DATA() statements.  utils/Gen_fmgrtab.pl
 *	  generates fmgroids.h and fmgrtab.c the same way.
 *
 *	  XXX do NOT break up DATA() statements into multiple lines!
 *		  the scripts are not as smart as you might think...
 *	  XXX (eg. #if 0 #endif won't do what you think)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SPM_PLAN_HISTORY_H
#define PG_SPM_PLAN_HISTORY_H

#include "catalog/genbki.h"

#define SPMPlanHistoryRelationId  9345

CATALOG(pg_spm_plan_history,9345) BKI_ROWTYPE_OID(8724) BKI_SCHEMA_MACRO
{
	NameData  spmname;
	Oid       spmnsp;
	Oid       ownerid;
	int8      sql_id;
	int8      plan_id;
	int8      created;
	int8      last_modified;
	int8      last_verified;
	int8      version;
	int8      elapsed_time;
	int8      executions;
	int8      task_id;
	int32     sql_normalize_rule;
	int32     priority;
	float4    cost;
	bool      enabled;
	bool      autopurge;
#ifdef CATALOG_VARLEN
	oidvector reloids;
	oidvector indexoids;
	text      sql_normalize;
	text      sql;
	text      hint;
	text      hint_detail;
	text      plan;
	text      guc_list;
	text      param_list;
	text      relnames[1];
	text      indexnames[1];
	text      description;
	text      origin;
#endif
} FormData_pg_spm_plan_history;

/* ----------------
 *		Form_pg_spm_plan_history corresponds to a pointer to a tuple with
 *		the format of pg_spm_plan_history relation.
 * ----------------
 */
typedef FormData_pg_spm_plan_history *Form_pg_spm_plan_history;

/* ----------------
 *		compiler constants for pg_spm_plan_history
 * ----------------
 */
#define Natts_pg_spm_plan_history                   30
#define Anum_pg_spm_plan_history_spmname            1
#define Anum_pg_spm_plan_history_namespace          2
#define Anum_pg_spm_plan_history_ownerid            3
#define Anum_pg_spm_plan_history_sql_id             4
#define Anum_pg_spm_plan_history_plan_id            5
#define Anum_pg_spm_plan_history_created            6
#define Anum_pg_spm_plan_history_last_modified      7
#define Anum_pg_spm_plan_history_last_verified      8
#define Anum_pg_spm_plan_history_version            9
#define Anum_pg_spm_plan_history_elapsed_time       10
#define Anum_pg_spm_plan_history_executions         11
#define Anum_pg_spm_plan_history_task_id            12
#define Anum_pg_spm_plan_history_sql_normalize_rule 13
#define Anum_pg_spm_plan_history_priority           14
#define Anum_pg_spm_plan_history_cost               15
#define Anum_pg_spm_plan_history_enabled            16
#define Anum_pg_spm_plan_history_autopurge          17
#define Anum_pg_spm_plan_history_reloids            18
#define Anum_pg_spm_plan_history_indexoids          19
#define Anum_pg_spm_plan_history_sql_normalize      20
#define Anum_pg_spm_plan_history_sql                21
#define Anum_pg_spm_plan_history_hint               22
#define Anum_pg_spm_plan_history_hint_detail        23
#define Anum_pg_spm_plan_history_plan               24
#define Anum_pg_spm_plan_history_guc_list           25
#define Anum_pg_spm_plan_history_param_list         26
#define Anum_pg_spm_plan_history_relnames           27
#define Anum_pg_spm_plan_history_indexnames         28
#define Anum_pg_spm_plan_history_description        29
#define Anum_pg_spm_plan_history_origin             30

#endif							/* PG_SPM_PLAN_HISTORY_H */
