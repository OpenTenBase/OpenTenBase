/*-------------------------------------------------------------------------
 *
 * spm.h
 *	  prototypes for various files in optimizer/spm
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/spm.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SPM_H
#define SPM_H

#include "miscadmin.h"
#include "nodes/relation.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_type.h"
#include "catalog/pg_spm_plan.h"
#include "catalog/pg_spm_plan_history.h"
#include "commands/defrem.h"
#include "executor/functions.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_type.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/queryjumble.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/planner.h"
#endif
#include "optimizer/spm_gen_hints.h"

/*
 * The threshold for triggering SPM length reduction. Any excess beyond this threshold will revert
 * to the default value during the next reallocation.
 */
#define SPM_PLAN_HINT_MAX_LEN 8192
#define SPM_PLAN_HINT_DETAIL_MAX_LEN 1024
#define SPM_PLAN_SQL_MAX_LEN 8192
#define SPM_PLAN_PARAM_MAX_LEN 4096
#define SPM_PLAN_DESC_MAX_LEN 1024
#define SPM_PLAN_PLAN_MAX_LEN 81920
#define SPM_PLAN_GUC_LIST_MAX_LEN 8192

/* The default length after SPM initialization. */
#define SPM_PLAN_HINT_DEFAULT_LEN 1024
#define SPM_PLAN_HINT_DETAIL_DEFAULT_LEN 1024
#define SPM_PLAN_SQL_DEFAULT_LEN 2048
#define SPM_PLAN_PARAM_DEFAULT_LEN 1024
#define SPM_PLAN_DESC_DEFAULT_LEN 512
#define SPM_PLAN_PLAN_DEFAULT_LEN 8192
#define SPM_PLAN_GUC_LIST_DEFAULT_LEN    512
#define SPM_PLAN_MAX_OIDSVLENGTH  7500

typedef struct LocalSPMPlanInfo
{
	NameData   spmname;
	Oid        spmnsp; /* if null, will search object under search_path */
	Oid        ownerid;

	int64      sql_id;
	int64      plan_id;
	int64      hint_id;
	char      *hint;
	int32      hint_len;
	char      *hint_detail;
	int32      hint_detail_len;
	char      *guc_list;
	int32      guc_list_len;
	int32      priority;
	bool       enabled;
	bool       autopurge;
	int64      executions;
	int64      created;
	int64      last_modified;
	int64      last_verified;
	int64      execute_time;
	int64      version;
	int64      elapsed_time;
	int64      task_id;
	int32      sql_normalize_rule;
	float4     cost;
	oidvector *reloids;
	oidvector *indexoids;
	char      *sql_normalize;
	int32      sql_normalize_len;
	char      *sql;
	int32      sql_len;
	char      *plan;
	int32      plan_len;
	char      *param_list;
	int32      param_list_len;
	ArrayType *relnames;
	ArrayType *indexnames;
	char      *description;
	int32      description_len;
	NameData   origin;
} LocalSPMPlanInfo;

typedef struct
{
	int64 execute_time;     // Execution time in milliseconds
	int   execute_count;    // Number of executions
} SPMPlanCaptureRules;

typedef enum
{
	SPM_CAPTURE_OFF,
	SPM_CAPTURE_ON
} SPMPlanCaptureMode;

typedef enum
{
	SPM_APPLY_OFF,
	SPM_APPLY_ON
} SPMPlanApplyMode;

typedef enum
{
	SPM_PLAN_FIXED = 1,    /* Fixed, Highest priority. */
	SPM_APPLY_ACCEPT = 100 /* Accepted, Lowest priority. */
} SPMPlanPriority;

typedef enum
{
	SPM_PLAN_SEND_PLAN = 1,
	SPM_PLAN_SEND_PLAN_DELETE,
	SPM_PLAN_SEND_PLAN_UPDATE,
} SPMPlanSendType;

extern LocalSPMPlanInfo   *pre_spmplan;
extern __thread bool       spmplan_inited;
extern int                 optimizer_capture_sql_plan_baselines;
extern int                 optimizer_use_sql_plan_baselines;
extern bool                debug_print_spm;
extern bool                enable_spm;
extern bool                enable_spm_alert;
extern int                 space_budget_limit;
extern int                 space_budget_percent;
extern int                 plan_retention_weeks;
extern bool                enable_experiment_spm;
extern MemoryContext       spm_context;
extern char               *optimizer_capture_sql_plan_baselines_rules;
extern SPMPlanCaptureRules spm_capture_rules;
extern int32               save_spmplan_state;

#define SAVE_SPMPLAN_ABNORMAL            (1 << 0)
#define SAVE_SPMPLAN_CAPTURE             (1 << 1)
#define SAVE_SPMPLAN_APPLY               (1 << 2)
#define SAVE_SPMPLAN_FILL_SQL_DONE       (1 << 3)
#define SAVE_SPMPLAN_FILL_GUC_DONE       (1 << 4)
#define SAVE_SPMPLAN_FILL_RELS_DONE      (1 << 5)
#define SAVE_SPMPLAN_FILL_HINT_DONE      (1 << 6)
#define SAVE_SPMPLAN_FILL_PLAN_DONE      (1 << 7)
#define SAVE_SPMPLAN_FILL_INDEX_DONE     (1 << 8)
#define SAVE_SPMPLAN_FILL_EXTRA_DONE     (1 << 9)
#define SAVE_SPMPLAN_FILL_RUNTIME_DONE   (1 << 10)
#define SAVE_SPMPLAN_FILL_PARAMLIST_DONE (1 << 11)
#define SAVE_SPMPLAN_INVALID_SQLID       (1 << 16)
#define SAVE_SPMPLAN_TEMP_TABLE          (1 << 17)
#define SAVE_SPMPLAN_SQL_CONTAINS_HINT   (1 << 18)
#define SAVE_SPMPLAN_EXPLAIN             (1 << 19)
#define SAVE_SPMPLAN_EXPLAN_EXECUTE      (1 << 20)
#define SAVE_SPMPLAN_APPLYED             (1 << 21)

#define SPM_SUPPORTED_VERSION 506
#define SPM_ENABLED (spmplan_inited && enable_spm && IsBackendPostgres)
#define NOSPM_DURING_UPGRADE (unlikely(upgrade_mode != NON_UPGRADE && WorkingGrandVersionNum <= SPM_SUPPORTED_VERSION))
#define CAP_SPMPLAN_ABNORMAL ((save_spmplan_state & SAVE_SPMPLAN_ABNORMAL) || NOSPM_DURING_UPGRADE)
#define CAP_SPM_MANUAL ((save_spmplan_state & (SAVE_SPMPLAN_CAPTURE | SAVE_SPMPLAN_APPLY)))

/* Hook for plugins to get control in ExecutorStart() */
typedef void (*SPMTrackSqlId_hook_type) (Query *query);
extern PGDLLIMPORT SPMTrackSqlId_hook_type SPMTrackSqlId_hook;

extern void  SPMPlanInit(void);
extern bool  ParseSPMPlanCaptureRules(const char *rules);
extern void  ResetPreSPMPlan(bool force);
extern void  ResetPreSPMPlanExtended(bool force);
extern void  DisableSPMPlanById(Oid spmplan_oid, bool history);
extern void  RemoveSPMPlanById(Oid spmplan_oid, bool history);
extern void  RemoveSPMPlanByRelId(Oid relid);
extern void  DeformSPMPlanTupe(Relation rel, HeapTuple tup, LocalSPMPlanInfo *spmplan);
extern char *FormatSpmplan(Oid oid);
extern char *FormatSpmplanHistory(Oid oid);
extern int DropSPMPlan(Oid relid, Oid indexid, int64 sql_id, int64 plan_id);
extern int AlterSPMPlan(int64 sql_id, int64 plan_id, char *attribute_name, 
						char *attribute_value);
extern bool ValidPreSPMPlan(bool output_log);

extern void  SetEnterExplainExecute(void);
extern void  SetExitExplainExecute(void);
extern void  SetSPMUnsupported(void);
extern void  SetSkipSPMProcess(void);
extern void  SPMFillStatement(Query *query, JumbleState *jstate, const char *query_string);
extern void  SPMFillStatementExtended(const char *query_string, const char *normalized_query,
                                      int64 sql_id);
extern void  SPMFillParamList(JumbleState *jstate);
extern void  SPMFillApplyedInfoExtended(int64 sql_id, int64 plan_id, bool spm_applyed);
extern void  SPMFillGUC(void);
extern void  SPMFillHint(char *hint, int hint_len, char *detail, int detail_len);
extern void  SPMFillPlan(char *s, int len);
extern void  SPMFillExtraData(PlannedStmt *plan);
extern void  SPMFillRunTime(void);
extern void  SPMFillIndex(List *itable);
extern void SPMGenHintPlan(QueryDesc *query_desc);
extern bool  SearchSPMPlan(int64 sql_id, int64 *plan_id);
extern bool  ChooseSPMPlanInternal(int64 sql_id, bool output_detail, char **hint);
extern bool  ChooseSPMPlan(int64 sql_id, bool output_detail);
extern char *ApplySPMPlan(char *hint_str, MemoryContext cxt, int64 sql_id);
extern bool  ValidateSPMPlan(int64 sql_id, int64 plan_id);
extern bool MatchRuleSPMPlan(void);
extern bool  CheckNewSPMPlanGenerated(void);
extern void  CaptureSPMPlan(void);
extern void  SPMSetParamStr(char *outstr, int outstr_size);
extern char **ArrayToList(ArrayType *array, int *nvalues, int *totallen, bool needres);
extern void ExecSendSPMPlan(LocalSPMPlanInfo *spmplan, SPMPlanSendType type, 
							char *attribute_name, char *attribute_value);
extern void SyncSPMPlan(LocalSPMPlanInfo *spmplan);
extern Datum TextListToArray(List *relnms);
extern void ResetSPMPlan(LocalSPMPlanInfo *spmplan);
extern void DeepFreeSPMPlan(LocalSPMPlanInfo *spmplan, bool force);
extern void ResetSPMPlan(LocalSPMPlanInfo *spmplan);
extern void InitSPMPlanInfo(LocalSPMPlanInfo *spmplan, MemoryContext cxt);
extern void EnlargeSPMPlanInfoMember(char **member, int *member_len, int needed);
extern void ShrinkSPMPlanInfo(LocalSPMPlanInfo *spmplan);
#endif /* SPM_H */
