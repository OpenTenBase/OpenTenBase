/*-------------------------------------------------------------------------
 *
 * explain.h
 *	  prototypes for explain.c
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/explain.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAIN_H
#define EXPLAIN_H

#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "parser/parse_node.h"
#include "pgxc/planner.h"
#include "optimizer/spm_gen_hints.h"
#include "utils/queryjumble.h"

extern bool remote_session;

typedef enum ExplainFormat
{
	EXPLAIN_FORMAT_TEXT,
	EXPLAIN_FORMAT_XML,
	EXPLAIN_FORMAT_JSON,
	EXPLAIN_FORMAT_YAML
} ExplainFormat;

typedef struct ExplainWorkersState
{
	int			num_workers;	/* # of worker processes the plan used */
	bool	   *worker_inited;	/* per-worker state-initialized flags */
	StringInfoData *worker_str; /* per-worker transient output buffers */
	int		   *worker_state_save;	/* per-worker grouping state save areas */
	StringInfo	prev_str;		/* saved output buffer while redirecting */
} ExplainWorkersState;

/* Hash table entry */
typedef struct PrintedInitPlanEntry
{
	/* fid of current fragment */
	int			fid;
	Bitmapset	*printed_initplans;
} PrintedInitPlanEntry;

typedef struct ExplainState
{
	StringInfo	str;			/* output buffer */
	StringInfo	warning_str;
	/* options */
	bool		verbose;		/* be verbose */
	bool		analyze;		/* print actual times */
	bool		costs;			/* print estimated costs */
	bool		buffers;		/* print buffer usage */
#ifdef PGXC
	bool		nodes;			/* print nodes in RemoteQuery node */
	bool		num_nodes;		/* print number of nodes in RemoteQuery node */
#endif /* PGXC */
	bool		timing;			/* print detailed node timing */
	bool		summary;		/* print total planning and execution timing */
	bool        settings;       /* print modified settings */
	ExplainSPM  spm;
	ExplainFormat format;		/* output format */
	bool		runtime;        /* print intermediate state of query execution, not after completion */
	/* state for output formatting --- not reset for each new plan tree */
	int			indent;			/* current indentation level */
	List	   *grouping_stack; /* format-specific grouping state */
	/* state related to the current plan tree (filled by ExplainPrintPlan) */
	PlannedStmt *pstmt;			/* top of plan */
	List	   *rtable;			/* range table */
	List	   *rtable_names;	/* alias names for RTEs */
	List	   *deparse_cxt;	/* context list for deparsing expressions */
	Bitmapset  *printed_subplans;	/* ids of SubPlans we've printed */
	List  *itable;			/* index table */
	HTAB		*printed_initplan_htab;
	bool		hide_workers;	/* set if we find an invisible Gather */
	/* state related to the current plan node */
	ExplainWorkersState *workers_state; /* needed if parallel plan */
#ifdef __OPENTENBASE_C__
	List       *fids;           /* fid list for not completed yet */
	uint64		query_mem;		/* add used memory if analyze */
#endif
	SpmHintsState *spm_hints_state;
	JumbleState   *jstate;
	bool           jumble_const;
} ExplainState;

/* Hook for plugins to get control in ExplainOneQuery() */
typedef void (*ExplainOneQuery_hook_type) (Query *query,
										   int cursorOptions,
										   IntoClause *into,
										   ExplainState *es,
										   const char *queryString,
										   ParamListInfo params);
extern PGDLLIMPORT ExplainOneQuery_hook_type ExplainOneQuery_hook;

/* Hook for plugins to get control in explain_get_index_name() */
typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
extern PGDLLIMPORT explain_get_index_name_hook_type explain_get_index_name_hook;


extern void ExplainQuery(ParseState *pstate, ExplainStmt *stmt, const char *queryString,
			 ParamListInfo params, QueryEnvironment *queryEnv, DestReceiver *dest);
#ifdef __OPENTENBASE__
extern void ExplainQueryFastCachedError(QueryDesc *queryDesc, bool warning);
extern void ExplainQueryFast(QueryDesc *queryDesc, bool warning);
#endif
extern ExplainState *NewExplainState(void);

extern TupleDesc ExplainResultDesc(ExplainStmt *stmt);

extern void ExplainOneUtility(Node *utilityStmt, IntoClause *into,
				  ExplainState *es, const char *queryString,
				  ParamListInfo params, QueryEnvironment *queryEnv);

extern void ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into,
						   ExplainState *es, const char *queryString,
						   ParamListInfo params, QueryEnvironment *queryEnv,
						   const instr_time *planduration,
						   const BufferUsage *bufusage);

extern void ExplainPrintPlan(ExplainState *es, QueryDesc *queryDesc);
extern void ExplainPrintTriggers(ExplainState *es, QueryDesc *queryDesc);
extern void ExplainPrintJITSummary(ExplainState *es, QueryDesc *queryDesc);
extern void ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *es);
extern void ExplainMemberNodes(PlanState **planstates, int nsubnodes, int nplans,
								List *ancestors, ExplainState *es);
extern void ExplainRemoteSubplan(Plan *plan, PlanState *planstate,
						List *ancestors, ExplainState *es);
extern void ExplainRemoteQuery(RemoteQuery *plan, PlanState *planstate,
								List *ancestors, ExplainState *es);
extern void ExplainQueryText(ExplainState *es, QueryDesc *queryDesc);

extern void ExplainBeginOutput(ExplainState *es);
extern void ExplainEndOutput(ExplainState *es);
extern void ExplainSeparatePlans(ExplainState *es);

extern void ExplainPropertyList(const char *qlabel, List *data,
					ExplainState *es);
extern void ExplainPropertyListNested(const char *qlabel, List *data,
						  ExplainState *es);
extern void ExplainPropertyText(const char *qlabel, const char *value,
					ExplainState *es);
extern void ExplainPropertyInteger(const char *qlabel, const char *unit,
					   int64 value, ExplainState *es);
extern void ExplainPropertyFloat(const char *qlabel, const char *unit,
					 double value, int ndigits, ExplainState *es);
extern void ExplainPropertyBool(const char *qlabel, bool value,
					ExplainState *es);
extern void ExplainNode(PlanState *planstate, List *ancestors,
					const char *relationship, const char *plan_name,
					ExplainState *es);
extern void ExplainCustomChildren(CustomScanState *css, List *ancestors,
					ExplainState *es);
extern void ExplainOpenGroup(const char *objtype, const char *labelname,
				 bool labeled, ExplainState *es);
extern void ExplainCloseGroup(const char *objtype, const char *labelname,
				  bool labeled, ExplainState *es);
extern HTAB *InitPrintedInitplanHtab(void);
extern bool CheckInitplanPrinted(int fid, int plan_node_id,
									HTAB *printed_initplan_htab);
extern void RecordInitplanPrinted(int fid, int plan_node_id,
									HTAB *printed_initplan_htab);
extern void ResetPrintedInitplanHtab(HTAB *printed_initplan_htab);
extern void ExplainQueryParameters(ExplainState *es, ParamListInfo params, int maxlen);
extern const char *explain_get_index_name(Oid indexId);
extern void ExplainSpmHints(ExplainState *es, QueryDesc *queryDesc);

#endif							/* EXPLAIN_H */
