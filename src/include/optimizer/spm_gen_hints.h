/*-------------------------------------------------------------------------
 *
 * spm_gen_hints.h:
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPM_GEN_HINTS_H
#define SPM_GEN_HINTS_H

#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "nodes/execnodes.h"
#include "executor/execdesc.h"

typedef enum ExplainSPM
{
	SPM_OFF,
	SPM_ONLY,
	SPM_CAPTURE,
	SPM_APPLY
} ExplainSPM;

/* for leading hint */
typedef struct SpmLeadingContext
{
	StringInfo	lead_str;
	int plannerinfo_id;
	List *planstate_list;
	List *plannerinfo_id_list;
	bool working;
} SpmLeadingContext;

typedef struct SpmHintsState
{
	Bitmapset  *hinted_subplans;	/* ids of SubPlans we've printed */
	List	   *rtable;			/* range table */
	List	   *rtable_names;	/* alias names for RTEs */
	StringInfo scan_str;
	StringInfo join_str;
	StringInfo rows_str;
	StringInfo distribution_str;
	StringInfo parallel_str;
	StringInfo aggpath_str;
	StringInfo guc_str;
	StringInfo table_seq_no_str;
	StringInfo mergejoin_key_str;
	StringInfo table_hint_list;
	SpmLeadingContext *leadcxt;
	Bitmapset  *invalid_query_block;
	Bitmapset  *appear_join;
	Bitmapset  *gen_groupby;
	Bitmapset  *gen_distinct;
	List              *init_plan_list;
	uint32                hint_id;
	uint32              plan_id;
	StringInfo         plan_str;
	StringInfo         plan_str_tmp;
} SpmHintsState;

extern char *spm_set_hint;

extern SpmHintsState *NewSpmHintsState(void);
extern SpmHintsState *spm_gen_hints(QueryDesc *queryDesc);
extern bool planstate_tree_walker_hints(PlanState *planstate, bool (*walker)(), void *context);
extern ExplainStmt *TransformToExplanHintStmt(Query *query);
extern void SPMExecExplain(ExplainStmt *new_query);

#endif							/* SPM_GEN_HINTS_H */