/*-------------------------------------------------------------------------
 *
 * createplan.c
 *	  Routines to create the desired plan for processing a query.
 *	  Planning is complete, we just need to convert the selected
 *	  Path into a Plan.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/createplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/stratnum.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/index.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/distribution.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/subselect.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "parser/parse_param.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#ifdef PGXC
#include "access/htup_details.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "parser/parse_coerce.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/planner.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "tcop/tcopprot.h"
#endif
#ifdef __OPENTENBASE__
#include "catalog/index.h"
#include "catalog/pg_type.h" 
#include "catalog/pgxc_class.h"
#include "pgxc/nodemgr.h"
#include "pgxc/squeue.h"
#include "utils/ruleutils.h"
#endif
#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif
#include "partitioning/partprune.h"
#ifdef __OPENTENBASE_C__
#include "access/xlog.h"
#include "executor/execFragment.h"
#include "foreign/foreign.h"
#include "pgxc/groupmgr.h"
#endif
#ifdef _PG_ORCL_
#include "utils/guc.h"
#endif
#include "executor/execMerge.h"

/*
 * Flag bits that can appear in the flags argument of create_plan_recurse().
 * These can be OR-ed together.
 *
 * CP_EXACT_TLIST specifies that the generated plan node must return exactly
 * the tlist specified by the path's pathtarget (this overrides both
 * CP_SMALL_TLIST and CP_LABEL_TLIST, if those are set).  Otherwise, the
 * plan node is allowed to return just the Vars and PlaceHolderVars needed
 * to evaluate the pathtarget.
 *
 * CP_SMALL_TLIST specifies that a narrower tlist is preferred.  This is
 * passed down by parent nodes such as Sort and Hash, which will have to
 * store the returned tuples.
 *
 * CP_LABEL_TLIST specifies that the plan node must return columns matching
 * any sortgrouprefs specified in its pathtarget, with appropriate
 * ressortgroupref labels.  This is passed down by parent nodes such as Sort
 * and Group, which need these values to be available in their inputs.
 *
 * CP_IGNORE_TLIST specifies that the caller plans to replace the targetlist,
 * and therefore it doens't matter a bit what target list gets generated.
 */
#define CP_EXACT_TLIST		0x0001	/* Plan must return specified tlist */
#define CP_SMALL_TLIST		0x0002	/* Prefer narrower tlists */
#define CP_LABEL_TLIST		0x0004	/* tlist must contain sortgrouprefs */
#define CP_IGNORE_TLIST		0x0008	/* caller will replace tlist */

#ifdef __OPENTENBASE__
int remote_subplan_depth = 0;
#endif
#ifdef __OPENTENBASE_C__
bool enable_qualification_pushdown = false;
bool enable_partition_iterator = false;

extern char* g_index_table_part_name;
#endif

static Plan *create_plan_recurse(PlannerInfo *root, Path *best_path,
					int flags);
static Plan *create_scan_plan(PlannerInfo *root, Path *best_path,
				 int flags);
static List *build_path_tlist(PlannerInfo *root, Path *path);
static bool use_physical_tlist(PlannerInfo *root, Path *path, int flags);
static List *get_gating_quals(PlannerInfo *root, List *quals);
static Plan *create_gating_plan(PlannerInfo *root, Path *path, Plan *plan,
				   List *gating_quals);
static Plan *create_join_plan(PlannerInfo *root, JoinPath *best_path);
static Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path);
static Plan *create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path);
static Result *create_result_plan(PlannerInfo *root, ResultPath *best_path);
static Result *create_qual_plan(PlannerInfo *root, QualPath *best_path);
static ProjectSet *create_project_set_plan(PlannerInfo *root, ProjectSetPath *best_path);
static Material *create_material_plan(PlannerInfo *root, MaterialPath *best_path,
					 int flags);
static Plan *create_unique_plan(PlannerInfo *root, UniquePath *best_path,
				   int flags);
static Gather *create_gather_plan(PlannerInfo *root, GatherPath *best_path);
static Plan *create_projection_plan(PlannerInfo *root,
					   ProjectionPath *best_path,
					   int flags);
static Plan *inject_projection_plan(Plan *subplan, List *tlist, bool parallel_safe);
static Sort *create_sort_plan(PlannerInfo *root, SortPath *best_path, int flags);
static Group *create_group_plan(PlannerInfo *root, GroupPath *best_path);
static Unique *create_upper_unique_plan(PlannerInfo *root, UpperUniquePath *best_path,
						 int flags);
static Agg *create_agg_plan(PlannerInfo *root, AggPath *best_path);
static Plan *create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path);
static Result *create_minmaxagg_plan(PlannerInfo *root, MinMaxAggPath *best_path);
static WindowAgg *create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path);
static SetOp *create_setop_plan(PlannerInfo *root, SetOpPath *best_path,
				  int flags);
static RecursiveUnion *create_recursiveunion_plan(PlannerInfo *root, RecursiveUnionPath *best_path);
static LockRows *create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path,
					 int flags);
static ModifyTable *create_modifytable_plan(PlannerInfo *root, ModifyTablePath *best_path);
static Limit *create_limit_plan(PlannerInfo *root, LimitPath *best_path,
				  int flags, int64 offset_est, int64 count_est);
static SeqScan *create_seqscan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses);
static SampleScan *create_samplescan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses);
static Scan *create_indexscan_plan(PlannerInfo *root, IndexPath *best_path,
					  List *tlist, List *scan_clauses, bool indexonly);
static BitmapHeapScan *create_bitmap_scan_plan(PlannerInfo *root,
						BitmapHeapPath *best_path,
						List *tlist, List *scan_clauses);
static Plan *create_bitmap_subplan(PlannerInfo *root, Path *bitmapqual,
					  List **qual, List **indexqual, List **indexECs);
static void bitmap_subplan_mark_shared(Plan *plan);
static List *flatten_partitioned_rels(List *partitioned_rels);
static TidScan *create_tidscan_plan(PlannerInfo *root, TidPath *best_path,
					List *tlist, List *scan_clauses);
static SubqueryScan *create_subqueryscan_plan(PlannerInfo *root,
						 SubqueryScanPath *best_path,
						 List *tlist, List *scan_clauses);
static FunctionScan *create_functionscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses);
static ValuesScan *create_valuesscan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses);
static TableFuncScan *create_tablefuncscan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses);
static CteScan *create_ctescan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses);
static NamedTuplestoreScan *create_namedtuplestorescan_plan(PlannerInfo *root,
								Path *best_path, List *tlist, List *scan_clauses);
static WorkTableScan *create_worktablescan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses);
static ForeignScan *create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path,
						List *tlist, List *scan_clauses);
static CustomScan *create_customscan_plan(PlannerInfo *root,
					   CustomPath *best_path,
					   List *tlist, List *scan_clauses);
static NestLoop *create_nestloop_plan(PlannerInfo *root, NestPath *best_path);
static MergeJoin *create_mergejoin_plan(PlannerInfo *root, MergePath *best_path);
static HashJoin *create_hashjoin_plan(PlannerInfo *root, HashPath *best_path);
static Node *replace_nestloop_params(PlannerInfo *root, Node *expr);
static Node *replace_nestloop_params_mutator(Node *node, PlannerInfo *root);
static void process_subquery_nestloop_params(PlannerInfo *root,
								 List *subplan_params);
static List *fix_indexqual_references(PlannerInfo *root, IndexPath *index_path);
static List *fix_indexorderby_references(PlannerInfo *root, IndexPath *index_path);
static Node *fix_indexqual_operand(Node *node, IndexOptInfo *index, int indexcol);
static List *get_switched_clauses(List *clauses, Relids outerrelids);
static List *order_qual_clauses(PlannerInfo *root, List *clauses);
static void copy_plan_costsize(Plan *dest, Plan *src);
static void label_sort_with_costsize(PlannerInfo *root, Sort *plan,
						 double limit_tuples);
static SeqScan *make_seqscan(List *qptlist, List *qpqual, Index scanrelid);
static SampleScan *make_samplescan(List *qptlist, List *qpqual, Index scanrelid,
				TableSampleClause *tsc);
static IndexScan *make_indexscan(List *qptlist, List *qpqual, Index scanrelid,
			   Oid indexid, List *indexqual, List *indexqualorig,
			   List *indexorderby, List *indexorderbyorig,
			   List *indexorderbyops,
			   ScanDirection indexscandir);
static IndexOnlyScan *make_indexonlyscan(List *qptlist, List *qpqual,
				   Index scanrelid, Oid indexid,
				   List *indexqual, List *indexorderby,
				   List *indextlist,
				   ScanDirection indexscandir);
static BitmapIndexScan *make_bitmap_indexscan(Index scanrelid, Oid indexid,
					  List *indexqual,
					  List *indexqualorig);
static BitmapHeapScan *make_bitmap_heapscan(List *qptlist,
					 List *qpqual,
					 Plan *lefttree,
					 List *bitmapqualorig,
					 Index scanrelid);
static TidScan *make_tidscan(List *qptlist, List *qpqual, Index scanrelid,
			 List *tidquals);
static SubqueryScan *make_subqueryscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  Plan *subplan);
static FunctionScan *make_functionscan(List *qptlist, List *qpqual,
				  Index scanrelid, List *functions, bool funcordinality);
static ValuesScan *make_valuesscan(List *qptlist, List *qpqual,
				Index scanrelid, List *values_lists);
static TableFuncScan *make_tablefuncscan(List *qptlist, List *qpqual,
				   Index scanrelid, TableFunc *tablefunc);
static CteScan *make_ctescan(List *qptlist, List *qpqual,
			 Index scanrelid, int ctePlanId, int cteParam);
static NamedTuplestoreScan *make_namedtuplestorescan(List *qptlist, List *qpqual,
						 Index scanrelid, char *enrname);
static WorkTableScan *make_worktablescan(List *qptlist, List *qpqual,
				   Index scanrelid, int wtParam);
static Append *make_append(List *appendplans, int first_partial_plan,
			List *tlist, List *partitioned_rels, PartitionPruneInfo *partpruneinfo);
static RecursiveUnion *make_recursive_union(List *tlist,
					 Plan *lefttree,
					 Plan *righttree,
					 int wtParam,
					 List *distinctList,
					 long numGroups);
static BitmapAnd *make_bitmap_and(List *bitmapplans);
static BitmapOr *make_bitmap_or(List *bitmapplans);
static NestLoop *make_nestloop(List *tlist,
			  List *joinclauses, List *otherclauses, List *nestParams,
			  Plan *lefttree, Plan *righttree,
			  JoinType jointype, bool inner_unique);
static HashJoin *make_hashjoin(List *tlist,
			  List *joinclauses, List *otherclauses,
			  List *hashclauses, List *hashoperators,
			  List *hashcollations, List *hashkeys,
			  Plan *lefttree, Plan *righttree,
			  JoinType jointype, bool inner_unique);
static Hash *make_hash(Plan *lefttree,
		  List *hashkeys,
		  Oid skewTable,
		  AttrNumber skewColumn,
		  bool skewInherit);
static MergeJoin *make_mergejoin(List *tlist,
			   List *joinclauses, List *otherclauses,
			   List *mergeclauses,
			   Oid *mergefamilies,
			   Oid *mergecollations,
			   int *mergestrategies,
			   bool *mergenullsfirst,
			   Plan *lefttree, Plan *righttree,
			   JoinType jointype, bool inner_unique,
			   bool skip_mark_restore,
			   bool is_not_full);
static Sort *make_sort(Plan *lefttree, int numCols,
		  AttrNumber *sortColIdx, Oid *sortOperators,
		  Oid *collations, bool *nullsFirst);
static Plan *prepare_sort_from_pathkeys(Plan *lefttree, List *pathkeys,
										Relids relids,
										const AttrNumber *reqColIdx,
										bool adjust_tlist_in_place,
										int *p_numsortkeys,
										AttrNumber **p_sortColIdx,
										Oid **p_sortOperators,
										Oid **p_collations,
										bool **p_nullsFirst);
static EquivalenceMember *find_ec_member_for_tle(EquivalenceClass *ec,
					   TargetEntry *tle,
					   Relids relids);
static Sort *make_sort_from_pathkeys(Plan *lefttree, List *pathkeys,
						Relids relids);
static Sort *make_sort_from_groupcols(List *groupcls,
						 AttrNumber *grpColIdx,
						 Plan *lefttree);
static Material *make_material(Plan *lefttree);
static WindowAgg *make_windowagg(List *tlist, Index winref,
			   int partNumCols, AttrNumber *partColIdx, Oid *partOperators,
			   int ordNumCols, AttrNumber *ordColIdx, Oid *ordOperators,
			   int frameOptions, Node *startOffset, Node *endOffset,
			   Oid startInRangeFunc, Oid endInRangeFunc,
			   Oid inRangeColl, bool inRangeAsc, bool inRangeNullsFirst,
			   Plan *lefttree);
static Group *make_group(List *tlist, List *qual, int numGroupCols,
		   AttrNumber *grpColIdx, Oid *grpOperators,
		   Plan *lefttree);
static Unique *make_unique_from_sortclauses(Plan *lefttree, List *distinctList);
static Unique *make_unique_from_pathkeys(Plan *lefttree,
						  List *pathkeys, int numCols);
static Gather *make_gather(List *qptlist, List *qpqual,
			int nworkers, int rescan_param, bool single_copy, Plan *subplan);
static SetOp *make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan *lefttree,
		   List *distinctList, AttrNumber flagColIdx, int firstFlag,
		   long numGroups);
static LockRows *make_lockrows(Plan *lefttree, List *rowMarks, int epqParam);
static Result *make_result(List *tlist, Node *resconstantqual, Plan *subplan, List *qual);
static ProjectSet *make_project_set(List *tlist, Plan *subplan);
static ModifyTable *make_modifytable(PlannerInfo *root, Plan *subplan,
									 CmdType operation, bool canSetTag,
									 Index nominalRelation, List *partitioned_rels,
									 bool partColsUpdated,
									 List *resultRelations,
									 List *updateColnosLists,
									 List *withCheckOptionLists, List *returningLists,
									 List *rowMarks, OnConflictExpr *onconflict, List *mergeActionLists, int epqParam);
static GatherMerge *create_gather_merge_plan(PlannerInfo *root,
						 GatherMergePath *best_path);

#ifdef XCP
static void adjust_subplan_distribution(PlannerInfo *root, Distribution *pathd,
						  Distribution *subd);
static Plan *create_remotescan_plan(PlannerInfo *root, RemoteSubPath *best_path);
static RemoteSubplan *find_push_down_plan(Plan *plan, bool force);
#endif
#ifdef __OPENTENBASE_C__
static bool is_tle_unmaterialized(Node *expr, Path *path);
static Sort *make_sort_from_groupClause(List *groupcls, Plan *lefttree);
static AttrNumber get_distribution_key_result(Plan *plan, RemoteSubplan *pushdown,
											  AttrNumber attrNum, List *tlist);

static AttrNumber get_distribution_key(Plan **lefttree_ptr, Node *distributionExpr);

static bool remote_transfer_datarow(Node *node, void *context);
#endif
static bool pgxc_targetlist_has_globalindex_col(Query *query);
static void make_tidscan_remote(TidScan *plan, Plan *subplan, Index baserelid,
					RangeTblEntry *rte, List *indexqualorig, bool for_update);
static MergeQualProj *create_mergequalproj_plan(PlannerInfo *root, MergeQualProjPath *best_path,
                                                int flags);
static ConnectBy *create_connect_by_plan(PlannerInfo *root, ConnectByPath *best_path);

static Plan          *create_partIterator_plan(PlannerInfo *root, PartIteratorPath *pIterpath);
static Plan          *setPlanPartitionInfo(PlannerInfo *root, Plan *plan, RelOptInfo *rel);

/*
 * Because Remote Subplan will do merge sort by the pathkeys, we should check
 * the pathkey coming from subpath, and reduce if can not found in targetlist.
 */
static List *
get_reasonable_pathkeys_tmp(List *tlist, List *pathkeys, Relids relids)
{
	ListCell   *lc;
	List	   *real_tlexprs = NIL;
	List	   *new_pathkeys = NIL;

	if (tlist == NULL)
		return NIL;

	/* remove RelabelType */
	foreach(lc, tlist)
	{
		TargetEntry *entry = (TargetEntry *) lfirst(lc);
		Expr *tlexpr = (Expr *)entry->expr;

		while (tlexpr && IsA(tlexpr, RelabelType))
			tlexpr = ((RelabelType *) tlexpr)->arg;

		real_tlexprs = lappend(real_tlexprs, tlexpr);
	}

	foreach(lc, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *ec = pathkey->pk_eclass;
		ListCell   *lc1 = NULL;

		/* do not check this */
		if (ec->ec_has_volatile)
		{
			new_pathkeys = lappend(new_pathkeys, pathkey);
			continue;
		}

		foreach(lc1, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc1);
			Expr	   *emexpr = NULL;
			List	   *exprvars;
			ListCell   *lc2 = NULL;

			if (em->em_is_const)
				continue;

			if (em->em_is_child &&
				!bms_is_subset(em->em_relids, relids))
				continue;

			emexpr = em->em_expr;
			while (emexpr && IsA(emexpr, RelabelType))
				emexpr = ((RelabelType *) emexpr)->arg;

			if (list_member(real_tlexprs, emexpr))
				break;

			exprvars = pull_var_clause((Node *) emexpr,
									   PVC_INCLUDE_AGGREGATES |
									   PVC_INCLUDE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS);
			foreach(lc2, exprvars)
			{
				Expr *node = (Expr *) lfirst(lc2);

				while (node && IsA(node, RelabelType))
					node = ((RelabelType *) node)->arg;

				if (list_member(real_tlexprs, node))
					break;
			}
			if (lc2)
				break;
		}

		if (lc1)
			new_pathkeys = lappend(new_pathkeys, pathkey);
		else
			break;
	}

	list_free(real_tlexprs);

	if (list_length(pathkeys) != list_length(new_pathkeys))
		elog(WARNING, "drop pathkey when make remote subplan");

	return new_pathkeys;
}


/*
 * create_plan
 *	  Creates the access plan for a query by recursively processing the
 *	  desired tree of pathnodes, starting at the node 'best_path'.  For
 *	  every pathnode found, we create a corresponding plan node containing
 *	  appropriate id, target list, and qualification information.
 *
 *	  The tlists and quals in the plan tree are still in planner format,
 *	  ie, Vars still correspond to the parser's numbering.  This will be
 *	  fixed later by setrefs.c.
 *
 *	  best_path is the best access path
 *
 *	  Returns a Plan tree.
 */
Plan *
create_plan(PlannerInfo *root, Path *best_path)
{
	Plan	   *plan;

	/* plan_params should not be in use in current query level */
	Assert(root->plan_params == NIL);

	/* Initialize this module's private workspace in PlannerInfo */
	root->curOuterRels = NULL;
	root->curOuterParams = NIL;
#ifdef XCP
	root->curOuterRestrict = NULL;
	adjust_subplan_distribution(root, root->distribution,
								best_path->distribution);
#endif

	/* Recursively process the path tree, demanding the correct tlist result */
	plan = create_plan_recurse(root, best_path, CP_EXACT_TLIST);

	/*
	 * Make sure the topmost plan node's targetlist exposes the original
	 * column names and other decorative info.  Targetlists generated within
	 * the planner don't bother with that stuff, but we must have it on the
	 * top-level tlist seen at execution time.  However, ModifyTable plan
	 * nodes don't have a tlist matching the querytree targetlist.
	 * ora_compatible
	 */
	if (!IsA(plan, ModifyTable) && (PG_MODE || !IsA(plan, MultiModifyTable)) && 
		!(IsA(plan, Gather) && IsA(outerPlan(plan), ModifyTable)))
		apply_tlist_labeling(plan->targetlist, root->processed_tlist);

	/*
	 * Attach any initPlans created in this query level to the topmost plan
	 * node.  (In principle the initplans could go in any plan node at or
	 * above where they're referenced, but there seems no reason to put them
	 * any lower than the topmost node for the query level.  Also, see
	 * comments for SS_finalize_plan before you try to change this.)
	 */
	SS_attach_initplans(root, plan);

	/* Check we successfully assigned all NestLoopParams to plan nodes */
	if (root->curOuterParams != NIL)
		elog(ERROR, "failed to assign all NestLoopParams to plan nodes");

	/*
	 * Reset plan_params to ensure param IDs used for nestloop params are not
	 * re-used later
	 */
	root->plan_params = NIL;

	return plan;
}

/*
 * create_plan_recurse
 *	  Recursive guts of create_plan().
 */
static Plan *
create_plan_recurse(PlannerInfo *root, Path *best_path, int flags)
{
	Plan	   *plan;

	/* Guard against stack overflow due to overly complex plans */
	check_stack_depth();

	switch (best_path->pathtype)
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_NamedTuplestoreScan:
		case T_ForeignScan:
		case T_CustomScan:
			plan = create_scan_plan(root, best_path, flags);
			break;
#ifdef XCP
		case T_RemoteSubplan:
			remote_subplan_depth++;
			plan = (Plan *) create_remotescan_plan(root,
												   (RemoteSubPath *) best_path);
			remote_subplan_depth--;
			break;
#endif
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:
			plan = create_join_plan(root,
									(JoinPath *) best_path);
			break;
		case T_PartIterator:
			plan = create_partIterator_plan(root, (PartIteratorPath *) best_path);
			break;
		case T_Append:
			plan = create_append_plan(root,
									  (AppendPath *) best_path);
			break;
		case T_MergeAppend:
			plan = create_merge_append_plan(root,
											(MergeAppendPath *) best_path);
			break;
		case T_Result:
			if (IsA(best_path, ProjectionPath))
			{
				plan = create_projection_plan(root,
											  (ProjectionPath *) best_path,
											  flags);
			}
			else if (IsA(best_path, MinMaxAggPath))
			{
				plan = (Plan *) create_minmaxagg_plan(root,
													  (MinMaxAggPath *) best_path);
			}
			else if (IsA(best_path, QualPath))
			{
				plan = (Plan *) create_qual_plan(root, (QualPath *) best_path);
			}
			else
			{
				Assert(IsA(best_path, ResultPath));
				plan = (Plan *) create_result_plan(root,
												   (ResultPath *) best_path);
			}
			break;
		case T_ProjectSet:
			plan = (Plan *) create_project_set_plan(root,
													(ProjectSetPath *) best_path);
			break;
		case T_Material:
			plan = (Plan *) create_material_plan(root,
												 (MaterialPath *) best_path,
												 flags);
			break;
		case T_Unique:
			if (IsA(best_path, UpperUniquePath))
			{
				plan = (Plan *) create_upper_unique_plan(root,
														 (UpperUniquePath *) best_path,
														 flags);
			}
			else
			{
				Assert(IsA(best_path, UniquePath));
				plan = create_unique_plan(root,
										  (UniquePath *) best_path,
										  flags);
			}
			break;
		case T_Gather:
			plan = (Plan *) create_gather_plan(root,
											   (GatherPath *) best_path);
			break;
		case T_Sort:
			plan = (Plan *) create_sort_plan(root,
											 (SortPath *) best_path,
											 flags);
			break;
		case T_Group:
			plan = (Plan *) create_group_plan(root,
											  (GroupPath *) best_path);
			break;
		case T_Agg:
			if (IsA(best_path, GroupingSetsPath))
				plan = create_groupingsets_plan(root,
												(GroupingSetsPath *) best_path);
			else
			{
				Assert(IsA(best_path, AggPath));
				plan = (Plan *) create_agg_plan(root,
												(AggPath *) best_path);
			}
			break;
		case T_WindowAgg:
			plan = (Plan *) create_windowagg_plan(root,
												  (WindowAggPath *) best_path);
			break;
		case T_SetOp:
			plan = (Plan *) create_setop_plan(root,
											  (SetOpPath *) best_path,
											  flags);
			break;
		case T_RecursiveUnion:
			plan = (Plan *) create_recursiveunion_plan(root,
													   (RecursiveUnionPath *) best_path);
			break;
		case T_LockRows:
			plan = (Plan *) create_lockrows_plan(root,
												 (LockRowsPath *) best_path,
												 flags);
			break;
		case T_ModifyTable:
			plan = (Plan *) create_modifytable_plan(root,
													(ModifyTablePath *) best_path);
			break;
		case T_Limit:
			plan = (Plan *) create_limit_plan(root,
											  (LimitPath *) best_path,
											  flags, 0, 1);
			break;
		case T_GatherMerge:
			plan = (Plan *) create_gather_merge_plan(root,
													 (GatherMergePath *) best_path);
			break;
		case T_MergeQualProj:
			plan = (Plan *) create_mergequalproj_plan(
				root, (MergeQualProjPath *) best_path, flags);
			break;
		case T_ConnectBy:
			plan = (Plan *) create_connect_by_plan(root,
												   (ConnectByPath *) best_path);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	return plan;
}

/*
 * create_scan_plan
 *	 Create a scan plan for the parent relation of 'best_path'.
 */
static Plan *
create_scan_plan(PlannerInfo *root, Path *best_path, int flags)
{
	RelOptInfo *rel = best_path->parent;
	List	   *scan_clauses;
	List	   *gating_clauses;
	List	   *tlist;
	Plan	   *plan;

	/*
	 * Extract the relevant restriction clauses from the parent relation. The
	 * executor must apply all these restrictions during the scan, except for
	 * pseudoconstants which we'll take care of below.
	 *
	 * If this is a plain indexscan or index-only scan, we need not consider
	 * restriction clauses that are implied by the index's predicate, so use
	 * indrestrictinfo not baserestrictinfo.  Note that we can't do that for
	 * bitmap indexscans, since there's not necessarily a single index
	 * involved; but it doesn't matter since create_bitmap_scan_plan() will be
	 * able to get rid of such clauses anyway via predicate proof.
	 */
	switch (best_path->pathtype)
	{
		case T_IndexScan:
		case T_IndexOnlyScan:
			scan_clauses = castNode(IndexPath, best_path)->indexinfo->indrestrictinfo;
			break;
		default:
			scan_clauses = rel->baserestrictinfo;
			break;
	}

	/*
	 * If this is a parameterized scan, we also need to enforce all the join
	 * clauses available from the outer relation(s).
	 *
	 * For paranoia's sake, don't modify the stored baserestrictinfo list.
	 */
	if (best_path->param_info)
		scan_clauses = list_concat(list_copy(scan_clauses),
								   best_path->param_info->ppi_clauses);

	/*
	 * Detect whether we have any pseudoconstant quals to deal with.  Then, if
	 * we'll need a gating Result node, it will be able to project, so there
	 * are no requirements on the child's tlist.
	 */
	gating_clauses = get_gating_quals(root, scan_clauses);
	if (gating_clauses)
		flags = 0;

	/*
	 * For table scans, rather than using the relation targetlist (which is
	 * only those Vars actually needed by the query), we prefer to generate a
	 * tlist containing all Vars in order.  This will allow the executor to
	 * optimize away projection of the table tuples, if possible.
	 *
	 * But if the caller is going to ignore our tlist anyway, then don't
	 * bother generating one at all.  We use an exact equality test here, so
	 * that this only applies when CP_IGNORE_TLIST is the only flag set.
	 */
	if (flags == CP_IGNORE_TLIST)
	{
		tlist = NULL;
	}
	else if (use_physical_tlist(root, best_path, flags))
	{
		if (best_path->pathtype == T_IndexOnlyScan)
		{
			/* For index-only scan, the preferred tlist is the index's */
			tlist = copyObject(((IndexPath *) best_path)->indexinfo->indextlist);

			/*
			 * Transfer sortgroupref data to the replacement tlist, if
			 * requested (use_physical_tlist checked that this will work).
			 */
			if (flags & CP_LABEL_TLIST)
				apply_pathtarget_labeling_to_tlist(tlist, best_path->pathtarget);
		}
		else
		{
			tlist = build_physical_tlist(root, rel);
			if (tlist == NIL)
			{
				/* Failed because of dropped cols, so use regular method */
				tlist = build_path_tlist(root, best_path);
			}
			else
			{
				/* As above, transfer sortgroupref data to replacement tlist */
				if (flags & CP_LABEL_TLIST)
					apply_pathtarget_labeling_to_tlist(tlist, best_path->pathtarget);
			}
		}
	}
	else
	{
		tlist = build_path_tlist(root, best_path);
	}

	switch (best_path->pathtype)
	{
		case T_SeqScan:
			plan = (Plan *) create_seqscan_plan(root,
												best_path,
												tlist,
												scan_clauses);
			break;

		case T_SampleScan:
			plan = (Plan *) create_samplescan_plan(root,
												   best_path,
												   tlist,
												   scan_clauses);
			break;

		case T_IndexScan:
			plan = (Plan *) create_indexscan_plan(root,
												  (IndexPath *) best_path,
												  tlist,
												  scan_clauses,
												  false);
			break;

		case T_IndexOnlyScan:
			plan = (Plan *) create_indexscan_plan(root,
												  (IndexPath *) best_path,
												  tlist,
												  scan_clauses,
												  true);
			break;

		case T_BitmapHeapScan:
			plan = (Plan *) create_bitmap_scan_plan(root,
													(BitmapHeapPath *) best_path,
													tlist,
													scan_clauses);
			break;

		case T_TidScan:
			plan = (Plan *) create_tidscan_plan(root,
												(TidPath *) best_path,
												tlist,
												scan_clauses);
			break;

		case T_SubqueryScan:
			plan = (Plan *) create_subqueryscan_plan(root,
													 (SubqueryScanPath *) best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_FunctionScan:
			plan = (Plan *) create_functionscan_plan(root,
													 best_path,
													 tlist,
													 scan_clauses);
			break;

		case T_TableFuncScan:
			plan = (Plan *) create_tablefuncscan_plan(root,
													  best_path,
													  tlist,
													  scan_clauses);
			break;

		case T_ValuesScan:
			plan = (Plan *) create_valuesscan_plan(root,
												   best_path,
												   tlist,
												   scan_clauses);
			break;

		case T_CteScan:
			plan = (Plan *) create_ctescan_plan(root,
												best_path,
												tlist,
												scan_clauses);
			break;

		case T_NamedTuplestoreScan:
			plan = (Plan *) create_namedtuplestorescan_plan(root,
															best_path,
															tlist,
															scan_clauses);
			break;

		case T_WorkTableScan:
			plan = (Plan *) create_worktablescan_plan(root,
													  best_path,
													  tlist,
													  scan_clauses);
			break;

		case T_ForeignScan:
			plan = (Plan *) create_foreignscan_plan(root,
													(ForeignPath *) best_path,
													tlist,
													scan_clauses);
			break;

		case T_CustomScan:
			plan = (Plan *) create_customscan_plan(root,
												   (CustomPath *) best_path,
												   tlist,
												   scan_clauses);
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

	if(enable_partition_iterator)
		setPlanPartitionInfo(root, plan, rel);

	/*
	 * If there are any pseudoconstant clauses attached to this node, insert a
	 * gating Result node that evaluates the pseudoconstants as one-time
	 * quals.
	 */
	if (gating_clauses)
		plan = create_gating_plan(root, best_path, plan, gating_clauses);

	return plan;
}

/*
 * If this plan is for partitioned table, scan plan's iterator-referenced
 * atrributes must be specified.
 */
static Plan *
setPlanPartitionInfo(PlannerInfo *root, Plan *plan, RelOptInfo *rel)
{
	if (root->isPartIteratorPlanning)
	{
		switch (plan->type)
		{
			case T_SeqScan:
			case T_IndexScan:
			case T_IndexOnlyScan:
			case T_BitmapHeapScan:
			case T_TidScan:
			{
				Scan *scan = (Scan *) plan;
				scan->isPartTbl = true;
				scan->partScanDirection = ForwardScanDirection;
				scan->partIterParamno = root->curIteratorParamIndex;
				scan->leafNum = root->curleafNum;
				scan->partition_leaf_rels = list_copy(rel->partition_leaf_rels);
				scan->partition_leaf_rels_idx = list_copy(rel->partition_leaf_rels_idx);
			}
			break;
			default:
				ereport(ERROR,

				        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
				         errmsg("Only Scan operator have patition attribute")));
				break;
		}

		if (plan->type == T_BitmapHeapScan)
		{
			Plan *subplan = outerPlan(plan);
			switch (subplan->type)
			{
				case T_BitmapAnd:
				{
					BitmapAnd *ba = (BitmapAnd *) subplan;
					ba->isPartTbl = true;
				}
				break;
				case T_BitmapOr:
				{
					BitmapOr *bo = (BitmapOr *) subplan;
					bo->isPartTbl = true;
				}
				break;
				default:
					break;
			}
		}
	}
	return plan;
}

/*
 * Build a target list (ie, a list of TargetEntry) for the Path's output.
 *
 * This is almost just make_tlist_from_pathtarget(), but we also have to
 * deal with replacing nestloop params.
 */
static List *
build_path_tlist(PlannerInfo *root, Path *path)
{
	List	   *tlist = NIL;
	Index	   *sortgrouprefs = path->pathtarget->sortgrouprefs;
	int			resno = 1;
	int			sortgroupno = 1;	/* OpenTenBase-V3 added */
	ListCell   *v;

	foreach(v, path->pathtarget->exprs)
	{
		Node	   *node = (Node *) lfirst(v);
		TargetEntry *tle;

		/*
		 * If it's a parameterized path, there might be lateral references in
		 * the tlist, which need to be replaced with Params.  There's no need
		 * to remake the TargetEntry nodes, so apply this to each list item
		 * separately.
		 */
		if (path->param_info)
			node = replace_nestloop_params(root, node);

		/* OpenTenBase-V3: Skip the TLE if it's unmaterialized yet */
		if (is_tle_unmaterialized(node, path))
		{
			/* Keep adding sortgroupno to get correct sortgrouprefs */
			sortgroupno++;
			continue;
		}

		tle = makeTargetEntry((Expr *) node,
							  resno,
							  NULL,
							  false);
		if (sortgrouprefs)
			tle->ressortgroupref = sortgrouprefs[sortgroupno - 1];

		tlist = lappend(tlist, tle);
		resno++;
		sortgroupno++;
	}

	return tlist;
}

/*
 * use_physical_tlist
 *		Decide whether to use a tlist matching relation structure,
 *		rather than only those Vars actually referenced.
 */
static bool
use_physical_tlist(PlannerInfo *root, Path *path, int flags)
{
	RelOptInfo *rel = path->parent;
	int			i;
	ListCell   *lc;

	/*
	 * Forget it if either exact tlist or small tlist is demanded.
	 */
	if (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST))
		return false;

	/*
	 * if we got ConnectBy or Rownum expr, return false to use
	 * pathtarget to generate tlist.
	 */
	if (root->parse->hasRowNumExpr || root->parse->connectByExpr ||
		root->hasUserDefinedFun)
		return false;

	/*
	 * We can do this for real relation scans, subquery scans, function scans,
	 * tablefunc scans, values scans, and CTE scans (but not for, eg, joins).
	 */
	if (rel->rtekind != RTE_RELATION &&
		rel->rtekind != RTE_SUBQUERY &&
		rel->rtekind != RTE_FUNCTION &&
		rel->rtekind != RTE_TABLEFUNC &&
		rel->rtekind != RTE_VALUES &&
		rel->rtekind != RTE_CTE)
		return false;

	/*
	 * Can't do it with inheritance cases either (mainly because Append
	 * doesn't project; this test may be unnecessary now that
	 * create_append_plan instructs its children to return an exact tlist).
	 */
	if (rel->reloptkind != RELOPT_BASEREL)
		return false;

	/*
	 * Also, don't do it to a CustomPath; the premise that we're extracting
	 * columns from a simple physical tuple is unlikely to hold for those.
	 * (When it does make sense, the custom path creator can set up the path's
	 * pathtarget that way.)
	 */
	if (IsA(path, CustomPath))
		return false;

	/*
	 * If a bitmap scan's tlist is empty, keep it as-is.  This may allow the
	 * executor to skip heap page fetches, and in any case, the benefit of
	 * using a physical tlist instead would be minimal.
	 */
	if (IsA(path, BitmapHeapPath) &&
		path->pathtarget->exprs == NIL)
		return false;

	/*
	 * Can't do it if any system columns or whole-row Vars are requested.
	 * (This could possibly be fixed but would take some fragile assumptions
	 * in setrefs.c, I think.)
	 */
	for (i = rel->min_attr; i <= 0; i++)
	{
		if (!bms_is_empty(rel->attr_needed[i - rel->min_attr]))
			return false;
	}

	/*
	 * Can't do it if the rel is required to emit any placeholder expressions,
	 * either.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);

		if (bms_nonempty_difference(phinfo->ph_needed, rel->relids) &&
			bms_is_subset(phinfo->ph_eval_at, rel->relids))
			return false;
	}

	/*
	 * Also, can't do it if CP_LABEL_TLIST is specified and path is requested
	 * to emit any sort/group columns that are not simple Vars.  (If they are
	 * simple Vars, they should appear in the physical tlist, and
	 * apply_pathtarget_labeling_to_tlist will take care of getting them
	 * labeled again.)	We also have to check that no two sort/group columns
	 * are the same Var, else that element of the physical tlist would need
	 * conflicting ressortgroupref labels.
	 */
	if ((flags & CP_LABEL_TLIST) && path->pathtarget->sortgrouprefs)
	{
		Bitmapset  *sortgroupatts = NULL;

		i = 0;
		foreach(lc, path->pathtarget->exprs)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			if (path->pathtarget->sortgrouprefs[i])
			{
				if (expr && IsA(expr, Var))
				{
					int			attno = ((Var *) expr)->varattno;

					attno -= FirstLowInvalidHeapAttributeNumber;
					if (bms_is_member(attno, sortgroupatts))
						return false;
					sortgroupatts = bms_add_member(sortgroupatts, attno);
				}
				else
					return false;
			}
			i++;
		}
	}

	return true;
}

/*
 * get_gating_quals
 *	  See if there are pseudoconstant quals in a node's quals list
 *
 * If the node's quals list includes any pseudoconstant quals,
 * return just those quals.
 */
static List *
get_gating_quals(PlannerInfo *root, List *quals)
{
	/* No need to look if we know there are no pseudoconstants */
	if (!root->hasPseudoConstantQuals)
		return NIL;

	/* Sort into desirable execution order while still in RestrictInfo form */
	quals = order_qual_clauses(root, quals);

	/* Pull out any pseudoconstant quals from the RestrictInfo list */
	return extract_actual_clauses(quals, true);
}

/*
 * create_gating_plan
 *	  Deal with pseudoconstant qual clauses
 *
 * Add a gating Result node atop the already-built plan.
 */
static Plan *
create_gating_plan(PlannerInfo *root, Path *path, Plan *plan,
				   List *gating_quals)
{
	Plan	   *gplan;

	Assert(gating_quals);

	/*
	 * Since we need a Result node anyway, always return the path's requested
	 * tlist; that's never a wrong choice, even if the parent node didn't ask
	 * for CP_EXACT_TLIST.
	 */
	gplan = (Plan *) make_result(build_path_tlist(root, path),
								 (Node *) gating_quals,
								 plan, NIL);

	/*
	 * Notice that we don't change cost or size estimates when doing gating.
	 * The costs of qual eval were already included in the subplan's cost.
	 * Leaving the size alone amounts to assuming that the gating qual will
	 * succeed, which is the conservative estimate for planning upper queries.
	 * We certainly don't want to assume the output size is zero (unless the
	 * gating qual is actually constant FALSE, and that case is dealt with in
	 * clausesel.c).  Interpolating between the two cases is silly, because it
	 * doesn't reflect what will really happen at runtime, and besides which
	 * in most cases we have only a very bad idea of the probability of the
	 * gating qual being true.
	 */
	copy_plan_costsize(gplan, plan);

	/* Gating quals could be unsafe, so better use the Path's safety flag */
	gplan->parallel_safe = path->parallel_safe;

	return gplan;
}

/*
 * create_join_plan
 *	  Create a join plan for 'best_path' and (recursively) plans for its
 *	  inner and outer paths.
 */
static Plan *
create_join_plan(PlannerInfo *root, JoinPath *best_path)
{
	Plan	   *plan;
	List	   *gating_clauses;

	switch (best_path->path.pathtype)
	{
		case T_MergeJoin:
			plan = (Plan *) create_mergejoin_plan(root,
												  (MergePath *) best_path);
			break;
		case T_HashJoin:
			plan = (Plan *) create_hashjoin_plan(root,
												 (HashPath *) best_path);
			break;
		case T_NestLoop:
			plan = (Plan *) create_nestloop_plan(root,
												 (NestPath *) best_path);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) best_path->path.pathtype);
			plan = NULL;		/* keep compiler quiet */
			break;
	}

#ifdef __OPENTENBASE__
	if (!plan->lefttree->remote_flag)
	{
		plan->lefttree->remote_flag = contain_remote_subplan(plan->lefttree);
	}
	if (!plan->righttree->remote_flag)
	{
		plan->righttree->remote_flag = contain_remote_subplan(plan->righttree);
	}
	plan->remote_flag = plan->lefttree->remote_flag ||
						plan->righttree->remote_flag;
#endif

	/*
	 * If there are any pseudoconstant clauses attached to this node, insert a
	 * gating Result node that evaluates the pseudoconstants as one-time
	 * quals.
	 */
	gating_clauses = get_gating_quals(root, best_path->joinrestrictinfo);
	if (gating_clauses)
		plan = create_gating_plan(root, (Path *) best_path, plan,
								  gating_clauses);

#ifdef NOT_USED

	/*
	 * * Expensive function pullups may have pulled local predicates * into
	 * this path node.  Put them in the qpqual of the plan node. * JMH,
	 * 6/15/92
	 */
	if (get_loc_restrictinfo(best_path) != NIL)
		set_qpqual((Plan) plan,
				   list_concat(get_qpqual((Plan) plan),
							   get_actual_clauses(get_loc_restrictinfo(best_path))));
#endif

	return plan;
}

static Plan *
create_partIterator_plan(PlannerInfo *root, PartIteratorPath *pIterpath)
{
	/* Construct PartIterator plan node */
	PartIterator     *partItr = makeNode(PartIterator);
	PlannerParamItem *pitem = NULL;
	/* just for specifying value to PlannerParamItem.item, represent nothing. */
	Node             *itrParam = NULL;
	Bitmapset        *extparams = NULL;
	Bitmapset        *allparams = NULL;
	RelOptInfo       *rel = pIterpath->path.parent;

	partItr->direction = pIterpath->direction;
	partItr->leafNum = pIterpath->leafNum;
	partItr->part_prune_info = NULL;

	itrParam = (Node *) palloc(sizeof(Node));
	pitem = makeNode(PlannerParamItem);

	pitem->item = (Node *) itrParam;
	pitem->paramId = SS_assign_special_param(root);
	root->plan_params = lappend(root->plan_params, pitem);
	partItr->partParamno = pitem->paramId;

	/*
	 * Store partition parameter in PlannerInfo,
	 * it will be used by scan plan which is offspring of this iterator plan node.
	 */
	root->curIteratorParamIndex = pitem->paramId;
	root->isPartIteratorPlanning = true;
	root->curleafNum = pIterpath->leafNum;

	/* construct sub plan */
	partItr->plan.lefttree = create_plan_recurse(root, pIterpath->subPath, CP_EXACT_TLIST);

	/* constrcut PartIterator attributes */
	partItr->plan.targetlist = partItr->plan.lefttree->targetlist;

	extparams = (Bitmapset *) copyObject(partItr->plan.extParam);
	partItr->plan.extParam = bms_add_member(extparams, pitem->paramId);
	allparams = (Bitmapset *) copyObject(partItr->plan.allParam);
	partItr->plan.allParam = bms_add_member(allparams, pitem->paramId);

	root->isPartIteratorPlanning = false;
	root->curIteratorParamIndex = 0;

	if (enable_partition_pruning &&
		pIterpath->partitioned_rels != NIL)
	{
		List	   *prunequal;

		prunequal = extract_actual_clauses(rel->baserestrictinfo, false);

		if (pIterpath->path.param_info)
		{

			List	   *prmquals = pIterpath->path.param_info->ppi_clauses;

			prmquals = extract_actual_clauses(prmquals, false);
			prmquals = (List *) replace_nestloop_params(root,
														(Node *) prmquals);

			prunequal = list_concat(prunequal, prmquals);
		}

		/*
		 * If any quals exist, they may be useful to perform further partition
		 * pruning during execution.  Generate a PartitionPruneInfo for each
		 * partitioned rel to store these quals and allow translation of
		 * partition indexes into subpath indexes.
		 */
		if (prunequal != NIL)
			partItr->part_prune_info =
				make_partition_iter_pruneinfo(root,
										 rel,
										 pIterpath->partitioned_rels,
										 prunequal);
	}

	copy_generic_path_info((Plan *) partItr, (Path *) pIterpath);
    return (Plan *)partItr;
}

/*
 * create_append_plan
 *	  Create an Append plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_append_plan(PlannerInfo *root, AppendPath *best_path)
{
	Append	   *plan;
	List	   *tlist = build_path_tlist(root, &best_path->path);
	List	   *subplans = NIL;
	ListCell   *subpaths;
	RelOptInfo *rel = best_path->path.parent;
	PartitionPruneInfo *partpruneinfo = NULL;
#ifdef __OPENTENBASE__
	RemoteSubplan *rplan;
	int			remote_flag = 0;
	/* only grouping append consider local send */
	bool		use_localsend = best_path->grouping_append;
#endif

	/*
	 * The subpaths list could be empty, if every child was proven empty by
	 * constraint exclusion.  In that case generate a dummy plan that returns
	 * no rows.
	 *
	 * Note that an AppendPath with no members is also generated in certain
	 * cases where there was no appending construct at all, but we know the
	 * relation is empty (see set_dummy_rel_pathlist).
	 */
	if (best_path->subpaths == NIL)
	{
		/* Generate a Result plan with constant-FALSE gating qual */
		Plan	   *plan;

		plan = (Plan *) make_result(tlist,
									(Node *) list_make1(makeBoolConst(false,
																	  false)),
									NULL, NIL);

		copy_generic_path_info(plan, (Path *) best_path);

		return plan;
	}


	/* Build the plan for each child */
	foreach(subpaths, best_path->subpaths)
	{
		Path	   *subpath = (Path *) lfirst(subpaths);
		Plan	   *subplan;

		/* Must insist that all children return the same tlist */
		subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

#ifdef __OPENTENBASE_C__
		if (use_localsend && !IsA(subplan, RemoteSubplan) && best_path->path.distribution != NULL)
		{
			rplan = make_remotesubplan(root, subplan, NULL,
									   best_path->path.distribution, NIL, false,
									   NULL);
			rplan->localSend = true;
			subplan = (Plan *)rplan;
		}

		if (remote_flag == 0)
			remote_flag = contain_remote_subplan(subplan);
#endif

		subplans = lappend(subplans, subplan);
	}

	if (enable_partition_pruning &&
		rel->reloptkind == RELOPT_BASEREL &&
		best_path->partitioned_rels != NIL)
	{
		List	   *prunequal;

		prunequal = extract_actual_clauses(rel->baserestrictinfo, false);

		if (best_path->path.param_info)
		{

			List	   *prmquals = best_path->path.param_info->ppi_clauses;

			prmquals = extract_actual_clauses(prmquals, false);
			prmquals = (List *) replace_nestloop_params(root,
														(Node *) prmquals);

			prunequal = list_concat(prunequal, prmquals);
		}

		/*
		 * If any quals exist, they may be useful to perform further partition
		 * pruning during execution.  Generate a PartitionPruneInfo for each
		 * partitioned rel to store these quals and allow translation of
		 * partition indexes into subpath indexes.
		 */
		if (prunequal != NIL)
			partpruneinfo =
				make_partition_pruneinfo(root,
										 rel,
										 best_path->subpaths,
										 best_path->partitioned_rels,
										 prunequal);
	}

	/*
	 * XXX ideally, if there's just one child, we'd not bother to generate an
	 * Append node but just return the single child.  At the moment this does
	 * not work because the varno of the child scan plan won't match the
	 * parent-rel Vars it'll be asked to emit.
	 */

	plan = make_append(subplans, best_path->first_partial_path,
					   tlist, best_path->partitioned_rels, partpruneinfo);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

#ifdef __OPENTENBASE_C__
	plan->plan.remote_flag = remote_flag;
#endif

	return (Plan *) plan;
}

/*
 * create_merge_append_plan
 *	  Create a MergeAppend plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path)
{
	MergeAppend *node = makeNode(MergeAppend);
	Plan	   *plan = &node->plan;
	List	   *tlist = build_path_tlist(root, &best_path->path);
	List	   *pathkeys = best_path->path.pathkeys;
	List	   *subplans = NIL;
	ListCell   *subpaths;

	/*
	 * We don't have the actual creation of the MergeAppend node split out
	 * into a separate make_xxx function.  This is because we want to run
	 * prepare_sort_from_pathkeys on it before we do so on the individual
	 * child plans, to make cross-checking the sort info easier.
	 */
	copy_generic_path_info(plan, (Path *) best_path);
	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;

	/* Compute sort column info, and adjust MergeAppend's tlist as needed */
	(void) prepare_sort_from_pathkeys(plan, pathkeys,
									  best_path->path.parent->relids,
									  NULL,
									  true,
									  &node->numCols,
									  &node->sortColIdx,
									  &node->sortOperators,
									  &node->collations,
									  &node->nullsFirst);

	/*
	 * Now prepare the child plans.  We must apply prepare_sort_from_pathkeys
	 * even to subplans that don't need an explicit sort, to make sure they
	 * are returning the same sort key columns the MergeAppend expects.
	 */
	foreach(subpaths, best_path->subpaths)
	{
		Path	   *subpath = (Path *) lfirst(subpaths);
		Plan	   *subplan;
		int			numsortkeys;
		AttrNumber *sortColIdx;
		Oid		   *sortOperators;
		Oid		   *collations;
		bool	   *nullsFirst;

		/* Build the child plan */
		/* Must insist that all children return the same tlist */
		subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

		/* Compute sort column info, and adjust subplan's tlist as needed */
		subplan = prepare_sort_from_pathkeys(subplan, pathkeys,
											 subpath->parent->relids,
											 node->sortColIdx,
											 false,
											 &numsortkeys,
											 &sortColIdx,
											 &sortOperators,
											 &collations,
											 &nullsFirst);

		/*
		 * Check that we got the same sort key information.  We just Assert
		 * that the sortops match, since those depend only on the pathkeys;
		 * but it seems like a good idea to check the sort column numbers
		 * explicitly, to ensure the tlists really do match up.
		 */
		Assert(numsortkeys == node->numCols);
		if (memcmp(sortColIdx, node->sortColIdx,
				   numsortkeys * sizeof(AttrNumber)) != 0)
			elog(ERROR, "MergeAppend child's targetlist doesn't match MergeAppend");
		Assert(memcmp(sortOperators, node->sortOperators,
					  numsortkeys * sizeof(Oid)) == 0);
		Assert(memcmp(collations, node->collations,
					  numsortkeys * sizeof(Oid)) == 0);
		Assert(memcmp(nullsFirst, node->nullsFirst,
					  numsortkeys * sizeof(bool)) == 0);

		/* Now, insert a Sort node if subplan isn't sufficiently ordered */
		if (!pathkeys_contained_in(pathkeys, subpath->pathkeys))
		{
			Sort	   *sort = make_sort(subplan, numsortkeys,
										 sortColIdx, sortOperators,
										 collations, nullsFirst);

			label_sort_with_costsize(root, sort, best_path->limit_tuples);
			subplan = (Plan *) sort;
		}

		subplans = lappend(subplans, subplan);
	}

	node->partitioned_rels = flatten_partitioned_rels(best_path->partitioned_rels);
	node->mergeplans = subplans;

	return (Plan *) node;
}

/*
 * create_result_plan
 *	  Create a Result plan for 'best_path'.
 *	  This is only used for degenerate cases, such as a query with an empty
 *	  jointree.
 *
 *	  Returns a Plan node.
 */
static Result *
create_result_plan(PlannerInfo *root, ResultPath *best_path)
{
	Result	   *plan;
	List	   *tlist;
	List	   *quals;

	tlist = build_path_tlist(root, &best_path->path);

	/* best_path->quals is just bare clauses */
	quals = order_qual_clauses(root, best_path->quals);

	plan = make_result(tlist, (Node *) quals, NULL, NIL);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

static Result *
create_qual_plan(PlannerInfo *root, QualPath *best_path)
{
	Result	   *plan;
	Plan	   *subplan;
	List	   *tlist;
	List	   *quals;
	int			num_execnode = 0;
	List	   *nodeList = NIL;

	subplan = create_plan_recurse(root, best_path->subpath, 0);

	/*
	 * if the remotesubplan execute on only one node, then we don't have to add
	 * remotesubplan again.
	 */
	if (subplan && IsA(subplan, RemoteSubplan))
	{
		nodeList = ((RemoteSubplan *) subplan)->nodeList;
		num_execnode = list_length(nodeList);
	}

	tlist = build_path_tlist(root, &best_path->path);

	/* best_path->quals is just bare clauses */
	quals = order_qual_clauses(root, best_path->quals);

	plan = make_result(tlist, NULL, subplan, quals);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	/*
	 * For update using rownum in subquery, such as
	 * UPDATE t SET a=a+1 WHERE id IN (SELECT id FROM t WHERE rownum < 10)
	 * Previously, each DN fetch 10 rows from all other DNs randomly and join
	 * with local data, whick cause the final result maybe not expected.
	 * So we add a RemoteSubplan node outside Result, and the RemoteSubplan node
	 * execute on only one node. By this way, each DN recceive the same data to
	 * do update by join with their local data.
	 */
	if (ORA_MODE && !IS_CENTRALIZED_DATANODE && num_execnode > 1 && root->query_level > 1)
	{
		PlannerInfo *proot = root;

		while (proot->parent_root != NULL)
			proot = proot->parent_root;
		if ((proot->parse->commandType == CMD_UPDATE ||
			proot->parse->commandType == CMD_DELETE) &&
			contain_rownum((Node *)quals))
		{
			RemoteSubplan *p_subp;
			Distribution *execDistribution = makeNode(Distribution);
			Distribution *resultDistribution;
			int			  nodeid;

			nodeid = list_nth_int(nodeList, random() % num_execnode);
			execDistribution->distributionType = LOCATOR_TYPE_NONE;
			execDistribution->nodes = bms_add_member(execDistribution->nodes,
													 nodeid);
			resultDistribution = (Distribution *) copyObject(best_path->path.distribution);
			p_subp = make_remotesubplan(root,
										(Plan *) plan,
										resultDistribution,
										execDistribution,
										best_path->path.pathkeys, false,
										best_path->path.parent->relids);
			plan = (Result *) p_subp;
		}
	}

	return plan;
}

/*
 * create_project_set_plan
 *	  Create a ProjectSet plan for 'best_path'.
 *
 *	  Returns a Plan node.
 */
static ProjectSet *
create_project_set_plan(PlannerInfo *root, ProjectSetPath *best_path)
{
	ProjectSet *plan;
	Plan	   *subplan;
	List	   *tlist;

	/* Since we intend to project, we don't need to constrain child tlist */
	subplan = create_plan_recurse(root, best_path->subpath, 0);

	tlist = build_path_tlist(root, &best_path->path);

	plan = make_project_set(tlist, subplan);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_material_plan
 *	  Create a Material plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Material *
create_material_plan(PlannerInfo *root, MaterialPath *best_path, int flags)
{
	Material   *plan;
	Plan	   *subplan;

	/*
	 * We don't want any excess columns in the materialized tuples, so request
	 * a smaller tlist.  Otherwise, since Material doesn't project, tlist
	 * requirements pass through.
	 */
	subplan = create_plan_recurse(root, best_path->subpath,
								  flags | CP_SMALL_TLIST);

	plan = make_material(subplan);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_unique_plan
 *	  Create a Unique plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_unique_plan(PlannerInfo *root, UniquePath *best_path, int flags)
{
	Plan	   *plan;
	Plan	   *subplan;
	List	   *in_operators;
	List	   *uniq_exprs;
	List	   *newtlist;
	int			nextresno;
	bool		newitems;
	int			numGroupCols;
	AttrNumber *groupColIdx;
	int			groupColPos;
	ListCell   *l;

	/* Unique doesn't project, so tlist requirements pass through */
	subplan = create_plan_recurse(root, best_path->subpath, flags);

	/* Done if we don't need to do any actual unique-ifying */
	if (best_path->umethod == UNIQUE_PATH_NOOP)
		return subplan;

	/*
	 * As constructed, the subplan has a "flat" tlist containing just the Vars
	 * needed here and at upper levels.  The values we are supposed to
	 * unique-ify may be expressions in these variables.  We have to add any
	 * such expressions to the subplan's tlist.
	 *
	 * The subplan may have a "physical" tlist if it is a simple scan plan. If
	 * we're going to sort, this should be reduced to the regular tlist, so
	 * that we don't sort more data than we need to.  For hashing, the tlist
	 * should be left as-is if we don't need to add any expressions; but if we
	 * do have to add expressions, then a projection step will be needed at
	 * runtime anyway, so we may as well remove unneeded items. Therefore
	 * newtlist starts from build_path_tlist() not just a copy of the
	 * subplan's tlist; and we don't install it into the subplan unless we are
	 * sorting or stuff has to be added.
	 */
	in_operators = best_path->in_operators;
	uniq_exprs = best_path->uniq_exprs;

	/* initialize modified subplan tlist as just the "required" vars */
	newtlist = build_path_tlist(root, &best_path->path);
	nextresno = list_length(newtlist) + 1;
	newitems = false;

	foreach(l, uniq_exprs)
	{
		Expr	   *uniqexpr = lfirst(l);
		TargetEntry *tle;

		tle = tlist_member(uniqexpr, newtlist);
		if (!tle)
		{
			tle = makeTargetEntry((Expr *) uniqexpr,
								  nextresno,
								  NULL,
								  false);
			newtlist = lappend(newtlist, tle);
			nextresno++;
			newitems = true;
		}
	}

	if (newitems || best_path->umethod == UNIQUE_PATH_SORT)
	{
		/*
		 * If the top plan node can't do projections and its existing target
		 * list isn't already what we need, we need to add a Result node to
		 * help it along.
		 */
		if (!is_projection_capable_plan(subplan) &&
			!tlist_same_exprs(newtlist, subplan->targetlist))
			subplan = inject_projection_plan(subplan, newtlist,
											 best_path->path.parallel_safe);
		else
			subplan->targetlist = newtlist;
#ifdef XCP
		/*
		 * RemoteSubplan is conditionally projection capable - it is pushing
		 * projection to the data nodes
		 */
		if (IsA(subplan, RemoteSubplan))
			subplan->lefttree->targetlist = newtlist;
#endif
	}

	/*
	 * Build control information showing which subplan output columns are to
	 * be examined by the grouping step.  Unfortunately we can't merge this
	 * with the previous loop, since we didn't then know which version of the
	 * subplan tlist we'd end up using.
	 */
	newtlist = subplan->targetlist;
	numGroupCols = list_length(uniq_exprs);
	groupColIdx = (AttrNumber *) palloc(numGroupCols * sizeof(AttrNumber));

	groupColPos = 0;
	foreach(l, uniq_exprs)
	{
		Expr	   *uniqexpr = lfirst(l);
		TargetEntry *tle;

		tle = tlist_member(uniqexpr, newtlist);
		if (!tle)				/* shouldn't happen */
			elog(ERROR, "failed to find unique expression in subplan tlist");
		groupColIdx[groupColPos++] = tle->resno;
	}

	if (best_path->umethod == UNIQUE_PATH_HASH)
	{
		Oid		   *groupOperators;

		/*
		 * Get the hashable equality operators for the Agg node to use.
		 * Normally these are the same as the IN clause operators, but if
		 * those are cross-type operators then the equality operators are the
		 * ones for the IN clause operators' RHS datatype.
		 */
		groupOperators = (Oid *) palloc(numGroupCols * sizeof(Oid));
		groupColPos = 0;
		foreach(l, in_operators)
		{
			Oid			in_oper = lfirst_oid(l);
			Oid			eq_oper;

			if (!get_compatible_hash_operators(in_oper, NULL, &eq_oper))
				elog(ERROR, "could not find compatible hash operator for operator %u",
					 in_oper);
			groupOperators[groupColPos++] = eq_oper;
		}

		/*
		 * Since the Agg node is going to project anyway, we can give it the
		 * minimum output tlist, without any stuff we might have added to the
		 * subplan tlist.
		 */
		plan = (Plan *) make_agg(newitems ? newtlist : build_path_tlist(root, &best_path->path),
								 NIL,
								 AGG_HASHED,
								 AGGSPLIT_SIMPLE,
								 numGroupCols,
								 groupColIdx,
								 groupOperators,
								 NIL,
								 NIL,
								 best_path->path.rows,
								 0,
								 subplan);

		((Agg*) plan)->plannerinfo_seq_no = root->plannerinfo_seq_no;
		((Agg*) plan)->agg_type = FUN_DISTINCT;
	}
	else
	{
		List	   *sortList = NIL;
		Sort	   *sort;

		/* Create an ORDER BY list to sort the input compatibly */
		groupColPos = 0;
		foreach(l, in_operators)
		{
			Oid			in_oper = lfirst_oid(l);
			Oid			sortop;
			Oid			eqop;
			TargetEntry *tle;
			SortGroupClause *sortcl;

			sortop = get_ordering_op_for_equality_op(in_oper, false);
			if (!OidIsValid(sortop))	/* shouldn't happen */
				elog(ERROR, "could not find ordering operator for equality operator %u",
					 in_oper);

			/*
			 * The Unique node will need equality operators.  Normally these
			 * are the same as the IN clause operators, but if those are
			 * cross-type operators then the equality operators are the ones
			 * for the IN clause operators' RHS datatype.
			 */
			eqop = get_equality_op_for_ordering_op(sortop, NULL);
			if (!OidIsValid(eqop))	/* shouldn't happen */
				elog(ERROR, "could not find equality operator for ordering operator %u",
					 sortop);

			tle = get_tle_by_resno(subplan->targetlist,
								   groupColIdx[groupColPos]);
			Assert(tle != NULL);
			
			sortcl = makeNode(SortGroupClause);
			sortcl->tleSortGroupRef = assignSortGroupRef(tle,
														 subplan->targetlist);
			sortcl->eqop = eqop;
			sortcl->sortop = sortop;
			sortcl->nulls_first = false;
			sortcl->hashable = false;	/* no need to make this accurate */
			sortList = lappend(sortList, sortcl);
			groupColPos++;
		}
		sort = make_sort_from_sortclauses(sortList, subplan);
		label_sort_with_costsize(root, sort, -1.0);
		plan = (Plan *) make_unique_from_sortclauses((Plan *) sort, sortList);
		((Unique *)plan)->plannerinfo_seq_no = root->plannerinfo_seq_no;
		((Unique *)plan)->agg_type = FUN_DISTINCT;
	}

	/* Copy cost data from Path to Plan */
	copy_generic_path_info(plan, &best_path->path);

	return plan;
}

/*
 * create_gather_plan
 *
 *	  Create a Gather plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Gather *
create_gather_plan(PlannerInfo *root, GatherPath *best_path)
{
	Gather	   *gather_plan;
	Plan	   *subplan;
	List	   *tlist;

	/*
	 * Push projection down to the child node.  That way, the projection work
	 * is parallelized, and there can be no system columns in the result (they
	 * can't travel through a tuple queue because it uses MinimalTuple
	 * representation).
	 */
	subplan = create_plan_recurse(root, best_path->subpath, CP_EXACT_TLIST);

	tlist = build_path_tlist(root, &best_path->path);

	gather_plan = make_gather(tlist,
							  NIL,
							  best_path->num_workers,
							  SS_assign_special_param(root),
							  best_path->single_copy,
							  subplan);

	copy_generic_path_info(&gather_plan->plan, &best_path->path);

	/* use parallel mode for parallel plans. */
	root->glob->parallelModeNeeded = true;

	return gather_plan;
}

/*
 * create_gather_merge_plan
 *
 *	  Create a Gather Merge plan for 'best_path' and (recursively)
 *	  plans for its subpaths.
 */
static GatherMerge *
create_gather_merge_plan(PlannerInfo *root, GatherMergePath *best_path)
{
	GatherMerge *gm_plan;
	Plan	   *subplan;
	List	   *pathkeys = best_path->path.pathkeys;
	List	   *tlist = build_path_tlist(root, &best_path->path);

	/* As with Gather, project away columns in the workers. */
	subplan = create_plan_recurse(root, best_path->subpath, CP_EXACT_TLIST);

	/* Create a shell for a GatherMerge plan. */
	gm_plan = makeNode(GatherMerge);
	gm_plan->plan.targetlist = tlist;
	gm_plan->num_workers = best_path->num_workers;
	copy_generic_path_info(&gm_plan->plan, &best_path->path);

	/* Assign the rescan Param. */
	gm_plan->rescan_param = SS_assign_special_param(root);

	/* Gather Merge is pointless with no pathkeys; use Gather instead. */
	Assert(pathkeys != NIL);

	/* Compute sort column info, and adjust subplan's tlist as needed */
	subplan = prepare_sort_from_pathkeys(subplan, pathkeys,
										 best_path->subpath->parent->relids,
										 gm_plan->sortColIdx,
										 false,
										 &gm_plan->numCols,
										 &gm_plan->sortColIdx,
										 &gm_plan->sortOperators,
										 &gm_plan->collations,
										 &gm_plan->nullsFirst);


	/* Now, insert a Sort node if subplan isn't sufficiently ordered */
	if (!pathkeys_contained_in(pathkeys, best_path->subpath->pathkeys))
		subplan = (Plan *) make_sort(subplan, gm_plan->numCols,
									 gm_plan->sortColIdx,
									 gm_plan->sortOperators,
									 gm_plan->collations,
									 gm_plan->nullsFirst);

	/* Now insert the subplan under GatherMerge. */
	gm_plan->plan.lefttree = subplan;

	/* use parallel mode for parallel plans. */
	root->glob->parallelModeNeeded = true;


	return gm_plan;
}

/*
 * create_projection_plan
 *
 *	  Create a plan tree to do a projection step and (recursively) plans
 *	  for its subpaths.  We may need a Result node for the projection,
 *	  but sometimes we can just let the subplan do the work.
 */
static Plan *
create_projection_plan(PlannerInfo *root, ProjectionPath *best_path, int flags)
{
	Plan	   *plan;
	Plan	   *subplan;
	List	   *tlist;
	bool		needs_result_node = false;

	/*
	 * Convert our subpath to a Plan and determine whether we need a Result
	 * node.
	 *
	 * In most cases where we don't need to project, creation_projection_path
	 * will have set dummypp, but not always.  First, some createplan.c
	 * routines change the tlists of their nodes.  (An example is that
	 * create_merge_append_plan might add resjunk sort columns to a
	 * MergeAppend.)  Second, create_projection_path has no way of knowing
	 * what path node will be placed on top of the projection path and
	 * therefore can't predict whether it will require an exact tlist. For
	 * both of these reasons, we have to recheck here.
	 */
	if (use_physical_tlist(root, &best_path->path, flags))
	{
		/*
		 * Our caller doesn't really care what tlist we return, so we don't
		 * actually need to project.  However, we may still need to ensure
		 * proper sortgroupref labels, if the caller cares about those.
		 */
		subplan = create_plan_recurse(root, best_path->subpath, 0);
		tlist = subplan->targetlist;
		if (flags & CP_LABEL_TLIST)
			apply_pathtarget_labeling_to_tlist(tlist,
											   best_path->path.pathtarget);
	}
	else if (is_projection_capable_path(best_path->subpath))
	{
		/*
		 * Our caller requires that we return the exact tlist, but no separate
		 * result node is needed because the subpath is projection-capable.
		 * Tell create_plan_recurse that we're going to ignore the tlist it
		 * produces.
		 */
		subplan = create_plan_recurse(root, best_path->subpath,
									  CP_IGNORE_TLIST);
		tlist = build_path_tlist(root, &best_path->path);
	}
	else
	{
		/*
		 * It looks like we need a result node, unless by good fortune the
		 * requested tlist is exactly the one the child wants to produce.
		 */
		subplan = create_plan_recurse(root, best_path->subpath, 0);
		tlist = build_path_tlist(root, &best_path->path);
		needs_result_node = !tlist_same_exprs(tlist, subplan->targetlist);
	}

	/*
	 * If we make a different decision about whether to include a Result node
	 * than create_projection_path did, we'll have made slightly wrong cost
	 * estimates; but label the plan with the cost estimates we actually used,
	 * not "corrected" ones.  (XXX this could be cleaned up if we moved more
	 * of the sortcolumn setup logic into Path creation, but that would add
	 * expense to creating Paths we might end up not using.)
	 */
	if (!needs_result_node)
	{
		/* Don't need a separate Result, just assign tlist to subplan */
		plan = subplan;
		plan->targetlist = tlist;

		/* Label plan with the estimated costs we actually used */
		plan->startup_cost = best_path->path.startup_cost;
		plan->total_cost = best_path->path.total_cost;
		plan->plan_rows = best_path->path.rows;
		plan->plan_width = best_path->path.pathtarget->width;
		plan->parallel_safe = best_path->path.parallel_safe;
		/* ... but don't change subplan's parallel_aware flag */
	}
	else
	{
		/* We need a Result node */
		plan = (Plan *) make_result(tlist, NULL, subplan, NIL);

		copy_generic_path_info(plan, (Path *) best_path);
	}

	return plan;
}

/*
 * inject_projection_plan
 *	  Insert a Result node to do a projection step.
 *
 * This is used in a few places where we decide on-the-fly that we need a
 * projection step as part of the tree generated for some Path node.
 * We should try to get rid of this in favor of doing it more honestly.
 *
 * One reason it's ugly is we have to be told the right parallel_safe marking
 * to apply (since the tlist might be unsafe even if the child plan is safe).
 */
static Plan *
inject_projection_plan(Plan *subplan, List *tlist, bool parallel_safe)
{
	Plan	   *plan;

	plan = (Plan *) make_result(tlist, NULL, subplan, NIL);

	/*
	 * In principle, we should charge tlist eval cost plus cpu_per_tuple per
	 * row for the Result node.  But the former has probably been factored in
	 * already and the latter was not accounted for during Path construction,
	 * so being formally correct might just make the EXPLAIN output look less
	 * consistent not more so.  Hence, just copy the subplan's cost.
	 */
	copy_plan_costsize(plan, subplan);
	plan->parallel_safe = parallel_safe;

	return plan;
}

/*
 * create_sort_plan
 *
 *	  Create a Sort plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Sort *
create_sort_plan(PlannerInfo *root, SortPath *best_path, int flags)
{
	Sort	   *plan;
	Plan	   *subplan;

	/*
	 * We don't want any excess columns in the sorted tuples, so request a
	 * smaller tlist.  Otherwise, since Sort doesn't project, tlist
	 * requirements pass through.
	 */
	subplan = create_plan_recurse(root, best_path->subpath,
								  flags | CP_SMALL_TLIST);

#ifdef __OPENTENBASE_C__
	if (!best_path->path.pathkeys)
	{
		plan = make_sort_from_groupClause(best_path->groupClause, subplan);
	}
	else
	{
#endif
	plan = make_sort_from_pathkeys(subplan, best_path->path.pathkeys,
								   IS_OTHER_REL(best_path->subpath->parent) ?
								   best_path->path.parent->relids : NULL);
#ifdef __OPENTENBASE_C__
	}
#endif
	

	copy_generic_path_info(&plan->plan, (Path *) best_path);

#ifdef __OPENTENBASE__
	if (plan->plan.parallel_aware)
	{
		Plan *left = plan->plan.lefttree;

		if (left->type == T_RemoteSubplan)
		{
			left->parallel_aware = true;
		}
	}
#endif

	return plan;
}

/*
 * create_group_plan
 *
 *	  Create a Group plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Group *
create_group_plan(PlannerInfo *root, GroupPath *best_path)
{
	Group	   *plan;
	Plan	   *subplan;
	List	   *tlist;
	List	   *quals;

	/*
	 * Group can project, so no need to be terribly picky about child tlist,
	 * but we do need grouping columns to be available
	 */
	subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

	tlist = build_path_tlist(root, &best_path->path);

	quals = order_qual_clauses(root, best_path->qual);

	plan = make_group(tlist,
					  quals,
					  list_length(best_path->groupClause),
					  extract_grouping_cols(best_path->groupClause,
											subplan->targetlist),
					  extract_grouping_ops(best_path->groupClause),
					  subplan);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	plan->plannerinfo_seq_no = root->plannerinfo_seq_no;
	plan->agg_type = FUN_GROUP_BY;

	return plan;
}

/*
 * create_upper_unique_plan
 *
 *	  Create a Unique plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Unique *
create_upper_unique_plan(PlannerInfo *root, UpperUniquePath *best_path, int flags)
{
	Unique	   *plan;
	Plan	   *subplan;

	/*
	 * Unique doesn't project, so tlist requirements pass through; moreover we
	 * need grouping columns to be labeled.
	 */
	subplan = create_plan_recurse(root, best_path->subpath,
								  flags | CP_LABEL_TLIST);

	plan = make_unique_from_pathkeys(subplan,
									 best_path->path.pathkeys,
									 best_path->numkeys);
	
	plan->plannerinfo_seq_no = root->plannerinfo_seq_no;
	plan->agg_type = FUN_DISTINCT;

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_agg_plan
 *
 *	  Create an Agg plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Agg *
create_agg_plan(PlannerInfo *root, AggPath *best_path)
{
	Agg		   *plan;
	Plan	   *subplan;
	List	   *tlist;
	List	   *quals;
	int			flags;

	/*
	 * Agg can project, so no need to be terribly picky about child tlist, but
	 * we do need grouping columns to be available. We are a bit more careful
	 * with hash aggregate, where we explicitly request small tlist to minimize
	 * I/O needed for spilling (we can't be sure spilling won't be necessary,
	 * so we just do it every time).
	 */
	flags = CP_LABEL_TLIST;

	/* ensure small tlist for hash aggregate */
	if (best_path->aggstrategy == AGG_HASHED)
		flags |= CP_SMALL_TLIST;

	subplan = create_plan_recurse(root, best_path->subpath, flags);

#ifdef __OPENTENBASE_C__
	if (best_path->groupingFunc)
	{
		fix_grouping_func_refs(best_path->path.pathtarget, subplan->targetlist);
	}
#endif

	tlist = build_path_tlist(root, &best_path->path);

	quals = order_qual_clauses(root, best_path->qual);

	plan = make_agg(tlist, quals,
					best_path->aggstrategy,
					best_path->aggsplit,
					list_length(best_path->groupClause),
					extract_grouping_cols(best_path->groupClause,
										  subplan->targetlist),
					extract_grouping_ops(best_path->groupClause),
					NIL,
					NIL,
					best_path->numGroups,
					best_path->transitionSpace,
					subplan);

	plan->plannerinfo_seq_no = root->plannerinfo_seq_no;
	plan->agg_type = best_path->agg_type;
	copy_generic_path_info(&plan->plan, (Path *) best_path);

#ifdef __OPENTENBASE_C__
	if (best_path->groupingFunc)
	{
		if (best_path->aggstrategy == AGG_SORTED)
		{
			plan->groupingFunc = true;
		}
	}
#endif

	return plan;
}

/*
 * Given a groupclause for a collection of grouping sets, produce the
 * corresponding groupColIdx.
 *
 * root->grouping_map maps the tleSortGroupRef to the actual column position in
 * the input tuple. So we get the ref from the entries in the groupclause and
 * look them up there.
 */
static AttrNumber *
remap_groupColIdx(PlannerInfo *root, List *groupClause)
{
	AttrNumber *grouping_map = root->grouping_map;
	AttrNumber *new_grpColIdx;
	ListCell   *lc;
	int			i;

	Assert(grouping_map);

	new_grpColIdx = palloc0(sizeof(AttrNumber) * list_length(groupClause));

	i = 0;
	foreach(lc, groupClause)
	{
		SortGroupClause *clause = lfirst(lc);

		new_grpColIdx[i++] = grouping_map[clause->tleSortGroupRef];
	}

	return new_grpColIdx;
}

/*
 * create_groupingsets_plan
 *	  Create a plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  What we emit is an Agg plan with some vestigial Agg and Sort nodes
 *	  hanging off the side.  The top Agg implements the last grouping set
 *	  specified in the GroupingSetsPath, and any additional grouping sets
 *	  each give rise to a subsidiary Agg and Sort node in the top Agg's
 *	  "chain" list.  These nodes don't participate in the plan directly,
 *	  but they are a convenient way to represent the required data for
 *	  the extra steps.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)
{
	Agg		   *plan;
	Plan	   *subplan;
	List	   *rollups = best_path->rollups;
	AttrNumber *grouping_map;
	int			maxref;
	List	   *chain;
	ListCell   *lc;
	int			flags;

	/* Shouldn't get here without grouping sets */
	Assert(root->parse->groupingSets);
	Assert(rollups != NIL);

	/*
	 * Agg can project, so no need to be terribly picky about child tlist, but
	 * we do need grouping columns to be available. We are a bit more careful
	 * with hash aggregate, where we explicitly request small tlist to minimize
	 * I/O needed for spilling (we can't be sure spilling won't be necessary,
	 * so we just do it every time).
	 */
	flags = CP_LABEL_TLIST;

	/* ensure small tlist for hash aggregate */
	if (best_path->aggstrategy == AGG_HASHED)
		flags |= CP_SMALL_TLIST;

	subplan = create_plan_recurse(root, best_path->subpath, flags);

	/*
	 * Compute the mapping from tleSortGroupRef to column index in the child's
	 * tlist.  First, identify max SortGroupRef in groupClause, for array
	 * sizing.
	 */
	maxref = 0;
	foreach(lc, root->parse->groupClause)
	{
		SortGroupClause *gc = (SortGroupClause *) lfirst(lc);

		if (gc->tleSortGroupRef > maxref)
			maxref = gc->tleSortGroupRef;
	}

	grouping_map = (AttrNumber *) palloc0((maxref + 1) * sizeof(AttrNumber));

	/* Now look up the column numbers in the child's tlist */
	foreach(lc, root->parse->groupClause)
	{
		SortGroupClause *gc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(gc, subplan->targetlist);

		grouping_map[gc->tleSortGroupRef] = tle->resno;
	}

	/*
	 * During setrefs.c, we'll need the grouping_map to fix up the cols lists
	 * in GroupingFunc nodes.  Save it for setrefs.c to use.
	 */
	Assert(root->grouping_map == NULL);
	root->grouping_map = grouping_map;

	/*
	 * Generate the side nodes that describe the other sort and group
	 * operations besides the top one.  Note that we don't worry about putting
	 * accurate cost estimates in the side nodes; only the topmost Agg node's
	 * costs will be shown by EXPLAIN.
	 */
	chain = NIL;
	if (list_length(rollups) > 1)
	{
		ListCell   *lc2 = lnext(list_head(rollups));
		bool		is_first_sort = ((RollupData *) linitial(rollups))->is_hashed;

		for_each_cell(lc, lc2)
		{
			RollupData *rollup = lfirst(lc);
			AttrNumber *new_grpColIdx;
			Plan	   *sort_plan = NULL;
			Plan	   *agg_plan;
			AggStrategy strat;

			new_grpColIdx = remap_groupColIdx(root, rollup->groupClause);

			if (!rollup->is_hashed && !is_first_sort)
			{
				sort_plan = (Plan *)
					make_sort_from_groupcols(rollup->groupClause,
											 new_grpColIdx,
											 subplan);
			}

			if (!rollup->is_hashed)
				is_first_sort = false;

			if (rollup->is_hashed)
				strat = AGG_HASHED;
			else if (list_length(linitial(rollup->gsets)) == 0)
				strat = AGG_PLAIN;
			else
				strat = AGG_SORTED;

			agg_plan = (Plan *) make_agg(NIL,
										 NIL,
										 strat,
										 AGGSPLIT_SIMPLE,
										 list_length((List *) linitial(rollup->gsets)),
										 new_grpColIdx,
										 extract_grouping_ops(rollup->groupClause),
										 rollup->gsets,
										 NIL,
										 rollup->numGroups,
										 best_path->transitionSpace,
										 sort_plan);

			/*
			 * Remove stuff we don't need to avoid bloating debug output.
			 */
			if (sort_plan)
			{
				sort_plan->targetlist = NIL;
				sort_plan->lefttree = NULL;
			}

			chain = lappend(chain, agg_plan);
		}
	}

	/*
	 * Now make the real Agg node
	 */
	{
		RollupData *rollup = linitial(rollups);
		AttrNumber *top_grpColIdx;
		int			numGroupCols;

		top_grpColIdx = remap_groupColIdx(root, rollup->groupClause);

		numGroupCols = list_length((List *) linitial(rollup->gsets));

		plan = make_agg(build_path_tlist(root, &best_path->path),
						best_path->qual,
						best_path->aggstrategy,
						AGGSPLIT_SIMPLE,
						numGroupCols,
						top_grpColIdx,
						extract_grouping_ops(rollup->groupClause),
						rollup->gsets,
						chain,
						rollup->numGroups,
						best_path->transitionSpace,
						subplan);

		/* Copy cost data from Path to Plan */
		copy_generic_path_info(&plan->plan, &best_path->path);
	}

	return (Plan *) plan;
}

/*
 * create_minmaxagg_plan
 *
 *	  Create a Result plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Result *
create_minmaxagg_plan(PlannerInfo *root, MinMaxAggPath *best_path)
{
	Result	   *plan;
	List	   *tlist;
	ListCell   *lc;

	/* Prepare an InitPlan for each aggregate's subquery. */
	foreach(lc, best_path->mmaggregates)
	{
		MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);
		PlannerInfo *subroot = mminfo->subroot;
		Query	   *subparse = subroot->parse;
		Plan	   *plan;

		/*
		 * Generate the plan for the subquery. We already have a Path, but we
		 * have to convert it to a Plan and attach a LIMIT node above it.
		 * Since we are entering a different planner context (subroot),
		 * recurse to create_plan not create_plan_recurse.
		 */
		plan = create_plan(subroot, mminfo->path);

		plan = (Plan *) make_limit(plan,
								   subparse->limitOffset,
								   subparse->limitCount,
								   subparse->limitOption,
								   0, NULL, NULL, NULL);

		/* Must apply correct cost/width data to Limit node */
		plan->startup_cost = mminfo->path->startup_cost;
		plan->total_cost = mminfo->pathcost;
		plan->plan_rows = 1;
		plan->plan_width = mminfo->path->pathtarget->width;
		plan->parallel_aware = false;
		plan->parallel_safe = mminfo->path->parallel_safe;

		/*
		 * XL: Add a remote subplan, splitting the LIMIT into a remote and
		 * local part LIMIT parts.
		 *
		 * XXX This should probably happen when constructing the path in
		 * create_minmaxagg_path(), not this late.
		 *
		 * XXX The costing in here is mostly bogus. Not that it'd matter
		 * this late, though.
		 */
		if (mminfo->path->distribution)
		{
			remote_subplan_depth++;
			plan = (Plan *) make_remotesubplan(subroot, plan,
											   NULL,
											   mminfo->path->distribution,
											   mminfo->path->pathkeys, false,
											   mminfo->path->parent->relids);
			remote_subplan_depth--;

			plan = (Plan *) make_limit(plan,
									   subparse->limitOffset,
									   subparse->limitCount,
									   subparse->limitOption,
									   0, NULL, NULL, NULL);

			plan->startup_cost = mminfo->path->startup_cost;
			plan->total_cost = mminfo->pathcost;
			plan->plan_rows = 1;
			plan->plan_width = mminfo->path->pathtarget->width;
			plan->parallel_aware = false;
		}

		/* Convert the plan into an InitPlan in the outer query. */
		SS_make_initplan_from_plan(root, subroot, plan, mminfo->param);
	}

	/* Generate the output plan --- basically just a Result */
	tlist = build_path_tlist(root, &best_path->path);

	plan = make_result(tlist, (Node *) best_path->quals, NULL, NIL);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	/*
	 * During setrefs.c, we'll need to replace references to the Agg nodes
	 * with InitPlan output params.  (We can't just do that locally in the
	 * MinMaxAgg node, because path nodes above here may have Agg references
	 * as well.)  Save the mmaggregates list to tell setrefs.c to do that.
	 */
	Assert(root->minmax_aggs == NIL);
	root->minmax_aggs = best_path->mmaggregates;

	return plan;
}

/*
 * create_windowagg_plan
 *
 *	  Create a WindowAgg plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static WindowAgg *
create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)
{
	WindowAgg  *plan;
	WindowClause *wc = best_path->winclause;
	int         numPart = list_length(wc->partitionClause);
	int         numOrder = list_length(wc->orderClause);
	Plan	   *subplan;
	List	   *tlist;
	int			partNumCols;
	AttrNumber *partColIdx;
	Oid		   *partOperators;
	int			ordNumCols;
	AttrNumber *ordColIdx;
	Oid		   *ordOperators;
	ListCell   *lc;

	/*
	 * Choice of tlist here is motivated by the fact that WindowAgg will be
	 * storing the input rows of window frames in a tuplestore; it therefore
	 * behooves us to request a small tlist to avoid wasting space. We do of
	 * course need grouping columns to be available.
	 */
	subplan = create_plan_recurse(root, best_path->subpath,
								  CP_LABEL_TLIST | CP_SMALL_TLIST);

	tlist = build_path_tlist(root, &best_path->path);

	/*
	 * Convert SortGroupClause lists into arrays of attr indexes and equality
	 * operators, as wanted by executor.  (Note: in principle, it's possible
	 * to drop some of the sort columns, if they were proved redundant by
	 * pathkey logic.  However, it doesn't seem worth going out of our way to
	 * optimize such cases.  In any case, we must *not* remove the ordering
	 * column for RANGE OFFSET cases, as the executor needs that for in_range
	 * tests even if it's known to be equal to some partitioning column.)
	 */
	partColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numPart);
	partOperators = (Oid *) palloc(sizeof(Oid) * numPart);

	partNumCols = 0;
	foreach(lc, wc->partitionClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, subplan->targetlist);

		Assert(OidIsValid(sgc->eqop));
		partColIdx[partNumCols] = tle->resno;
		partOperators[partNumCols] = sgc->eqop;
		partNumCols++;
	}

	ordColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numOrder);
	ordOperators = (Oid *) palloc(sizeof(Oid) * numOrder);

	ordNumCols = 0;
	foreach(lc, wc->orderClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, subplan->targetlist);

		Assert(OidIsValid(sgc->eqop));
		ordColIdx[ordNumCols] = tle->resno;
		ordOperators[ordNumCols] = sgc->eqop;
		ordNumCols++;
	}

	/* And finally we can make the WindowAgg node */
	plan = make_windowagg(tlist,
						  wc->winref,
						  partNumCols,
						  partColIdx,
						  partOperators,
						  ordNumCols,
						  ordColIdx,
						  ordOperators,
						  wc->frameOptions,
						  wc->startOffset,
						  wc->endOffset,
						  wc->startInRangeFunc,
						  wc->endInRangeFunc,
						  wc->inRangeColl,
						  wc->inRangeAsc,
						  wc->inRangeNullsFirst,
						  subplan);
	if (ORA_MODE)
	{
		plan->start_contain_vars = wc->start_contain_vars;
		plan->end_contain_vars = wc->end_contain_vars;
	}
	else
	{
		plan->start_contain_vars = false;
		plan->end_contain_vars = false;
	}

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_setop_plan
 *
 *	  Create a SetOp plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static SetOp *
create_setop_plan(PlannerInfo *root, SetOpPath *best_path, int flags)
{
	SetOp	   *plan;
	Plan	   *subplan;
	long		numGroups;

	/*
	 * SetOp doesn't project, so tlist requirements pass through; moreover we
	 * need grouping columns to be labeled.
	 */
	subplan = create_plan_recurse(root, best_path->subpath,
								  flags | CP_LABEL_TLIST);

	/* Convert numGroups to long int --- but 'ware overflow! */
	numGroups = (long) Min(best_path->numGroups, (double) LONG_MAX);

	plan = make_setop(best_path->cmd,
					  best_path->strategy,
					  subplan,
					  best_path->distinctList,
					  best_path->flagColIdx,
					  best_path->firstFlag,
					  numGroups);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_recursiveunion_plan
 *
 *	  Create a RecursiveUnion plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static RecursiveUnion *
create_recursiveunion_plan(PlannerInfo *root, RecursiveUnionPath *best_path)
{
	RecursiveUnion *plan;
	Plan	   *leftplan;
	Plan	   *rightplan;
	List	   *tlist;
	long		numGroups;

	/* Need both children to produce same tlist, so force it */
	leftplan = create_plan_recurse(root, best_path->leftpath, CP_EXACT_TLIST);
	rightplan = create_plan_recurse(root, best_path->rightpath, CP_EXACT_TLIST);

	tlist = build_path_tlist(root, &best_path->path);

	/* Convert numGroups to long int --- but 'ware overflow! */
	numGroups = (long) Min(best_path->numGroups, (double) LONG_MAX);

	plan = make_recursive_union(tlist,
								leftplan,
								rightplan,
								best_path->wtParam,
								best_path->distinctList,
								numGroups);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}

/*
 * create_lockrows_plan
 *
 *	  Create a LockRows plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static LockRows *
create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path,
					 int flags)
{
	LockRows   *plan;
	Plan	   *subplan;

	/* LockRows doesn't project, so tlist requirements pass through */
	subplan = create_plan_recurse(root, best_path->subpath, flags);

	plan = make_lockrows(subplan, best_path->rowMarks, best_path->epqParam);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}


/*
 * create_modifytable_plan
 *	  Create a ModifyTable plan for 'best_path'.
 *
 *	  Returns a Plan node.
 */
static ModifyTable *
create_modifytable_plan(PlannerInfo *root, ModifyTablePath *best_path)
{
	ModifyTable *plan;
	Path	   *subpath = best_path->subpath;
	Plan        *subplan;
	bool        readonly_insert = false;
#ifdef __AUDIT_FGA__
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt = NULL;
    bool            need_audit_fga_quals = true;
    char            *operation;
    ListCell        *item;
#endif    

	/* Subplan must produce exactly the specified tlist */
	subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

	/* Transfer resname/resjunk labeling, too, to keep executor happy */
	apply_tlist_labeling(subplan->targetlist, root->processed_tlist);

	if (best_path->mergeActionLists)
	{
		ListCell *lc;
		MergeAction *action;
		List *mergeActionList = linitial(best_path->mergeActionLists);

		foreach (lc, mergeActionList)
		{
			action = lfirst_node(MergeAction, lc);
			
			if (action->commandType == CMD_INSERT && action->actionOnly == true)
			{
				
				action->qual = NULL;
				action->targetList = NIL;
				readonly_insert = true;
			}
		}
		if (readonly_insert)
		{
			ListCell *actions_lc;
			foreach (actions_lc, best_path->mergeActionLists)
			{
				List *mergeActionList = lfirst(actions_lc);
				foreach (lc, mergeActionList)
				{
					action = lfirst_node(MergeAction, lc);
					if (action->commandType == CMD_INSERT)
					{
						action->qual = NULL;
						action->targetList = NIL;
						action->actionOnly = true;
					}
				}
			}
		}
	}
	plan = make_modifytable(root,
							subplan,
							best_path->operation,
							best_path->canSetTag,
							best_path->nominalRelation,
							best_path->partitioned_rels,
							best_path->partColsUpdated,
							best_path->resultRelations,
							best_path->updateColnosLists,
							best_path->withCheckOptionLists,
							best_path->returningLists,
							best_path->rowMarks,
							best_path->onconflict,
							best_path->mergeActionLists,
							best_path->epqParam);

#ifdef _PG_ORCL_
	plan->rid_col_idx = root->tlist_rid_indx;
#endif
#ifdef __AUDIT_FGA__
    if (enable_fga)
    {
        plan->plan.audit_fga_quals = NULL;

        audit_rte = rt_fetch(root->parse->resultRelation, root->parse->rtable);

        switch (root->parse->commandType)
        {
    		case CMD_INSERT:
    			operation = "Insert";
    			break;
    		case CMD_UPDATE:
    			operation = "Update";
    			break;
    		case CMD_DELETE:
    			operation = "Delete";
    			break;
			case CMD_MERGE:
				operation = "Merge";
				break;
    		default:
    			operation = "???";
    			break;
        }
            
        need_audit_fga_quals = get_audit_fga_quals(audit_rte->relid, operation, NULL, &audit_fga_quals_stmt);
        if (need_audit_fga_quals)
        {
            foreach (item, audit_fga_quals_stmt)
            {
                AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
                if (!audit_fga_qual->qual)
                {
                    audit_fga_log_policy_info(audit_fga_qual, operation);

                    audit_fga_quals_stmt = list_delete_ptr(audit_fga_quals_stmt, audit_fga_qual);                   
                }
            }
            
            if (audit_fga_quals_stmt != NULL)
                plan->plan.audit_fga_quals = audit_fga_quals_stmt;
        }
    }
#endif    

	copy_generic_path_info(&plan->plan, &best_path->path);

#ifdef __OPENTENBASE__
	/*
	 * If we have unshippable triggers and functions that cannot be pushed down, we have to do DML on coordinators,
	 * generate remote_dml plan now.
     */
	if (root->parse->hasUnshippableDml)
	{
		create_remotedml_plan(root, (Plan *) plan, plan->operation,
		                      RCMD_NORETURN, &plan->remote_plans);
		plan->has_trigger = root->parse->has_trigger;
	}
#endif
	/* ora_compatible */
	if (ORA_MODE)
		plan->is_submod = root->glob->is_multi_insert;

	return plan;
}

/*
 * create_limit_plan
 *
 *	  Create a Limit plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static Limit *
create_limit_plan(PlannerInfo *root, LimitPath *best_path, int flags,
				  int64 offset_est, int64 count_est)
{
	Limit	   *plan;
	Plan	   *subplan;
	int			numUniqkeys = 0;
	AttrNumber *uniqColIdx = NULL;
	Oid		   *uniqOperators = NULL;
	Oid		   *uniqCollations = NULL;

	/* Limit doesn't project, so tlist requirements pass through */
	subplan = create_plan_recurse(root, best_path->subpath, flags);

	/* Extract information necessary for comparing rows for WITH TIES. */
	if (best_path->limitOption == LIMIT_OPTION_WITH_TIES ||
	    best_path->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES)
	{
		Query	   *parse = root->parse;
		ListCell   *l;

		numUniqkeys = list_length(parse->sortClause);
		uniqColIdx = (AttrNumber *) palloc(numUniqkeys * sizeof(AttrNumber));
		uniqOperators = (Oid *) palloc(numUniqkeys * sizeof(Oid));
		uniqCollations = (Oid *) palloc(numUniqkeys * sizeof(Oid));

		numUniqkeys = 0;
		foreach(l, parse->sortClause)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(l);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl, parse->targetList);

			uniqColIdx[numUniqkeys] = tle->resno;
			uniqOperators[numUniqkeys] = sortcl->eqop;
			uniqCollations[numUniqkeys] = exprCollation((Node *) tle->expr);
			numUniqkeys++;
		}
	}

	plan = make_limit(subplan,
					  best_path->limitOffset,
					  best_path->limitCount,
					  best_path->limitOption,
					  numUniqkeys, uniqColIdx, uniqOperators, uniqCollations);

	copy_generic_path_info(&plan->plan, (Path *) best_path);

	return plan;
}


/*****************************************************************************
 *
 *	BASE-RELATION SCAN METHODS
 *
 *****************************************************************************/


/*
 * create_seqscan_plan
 *	 Returns a seqscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SeqScan *
create_seqscan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses)
{
	SeqScan         *scan_plan;
	Index		    scan_relid = best_path->parent->relid;

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->parent->rtekind == RTE_RELATION);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}
	scan_plan = make_seqscan(tlist,
							 scan_clauses,
							 scan_relid);

#ifdef __AUDIT_FGA__
    if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
    {
		char            *operation = "select";
		RangeTblEntry   *audit_rte;
		List            *audit_fga_quals_stmt = NULL;
		ListCell        *item;
		bool            need_audit_fga_quals = true;

        scan_plan->plan.audit_fga_quals = NULL;

        audit_rte = planner_rt_fetch(scan_relid, root);

        need_audit_fga_quals = get_audit_fga_quals(audit_rte->relid, operation, tlist, &audit_fga_quals_stmt);
        if (need_audit_fga_quals)
        {
            foreach (item, audit_fga_quals_stmt)
            {
                AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
                if (!audit_fga_qual->qual)
                {
                    audit_fga_log_policy_info(audit_fga_qual, operation);

                    audit_fga_quals_stmt = list_delete_ptr(audit_fga_quals_stmt, audit_fga_qual);                   
                }
            }

            if (audit_fga_quals_stmt != NULL)
                scan_plan->plan.audit_fga_quals = audit_fga_quals_stmt;
        }
    }
#endif

	copy_generic_path_info(&scan_plan->plan, best_path);

	return scan_plan;
}

/*
 * create_samplescan_plan
 *	 Returns a samplescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SampleScan *
create_samplescan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses)
{
	SampleScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	TableSampleClause *tsc;
    
#ifdef __AUDIT_FGA__
    char            *operation = "select";
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt  = NULL;
    bool            need_audit_fga_quals = true;
    ListCell        *item;
#endif    

	/* it should be a base rel with a tablesample clause... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_RELATION);
	tsc = rte->tablesample;
	Assert(tsc != NULL);


	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		tsc = (TableSampleClause *)
			replace_nestloop_params(root, (Node *) tsc);
	}

	scan_plan = make_samplescan(tlist,
								scan_clauses,
								scan_relid,
								tsc);

#ifdef __AUDIT_FGA__
    if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
    {
        scan_plan->scan.plan.audit_fga_quals = NULL;

        audit_rte = planner_rt_fetch(scan_relid, root);

        need_audit_fga_quals = get_audit_fga_quals(audit_rte->relid, operation, tlist, &audit_fga_quals_stmt);
        if (need_audit_fga_quals)
        {
            foreach (item, audit_fga_quals_stmt)
            {
                AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
                if (!audit_fga_qual->qual)
                {
                    audit_fga_log_policy_info(audit_fga_qual, operation);

                    audit_fga_quals_stmt = list_delete_ptr(audit_fga_quals_stmt, audit_fga_qual);                   
                }
            }

            if (audit_fga_quals_stmt != NULL)
                scan_plan->scan.plan.audit_fga_quals = audit_fga_quals_stmt;
        }
    }
#endif    

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * get_indextable_indexid_by_indextableid
 *	 Returns the index oid on the index table.
 */
static Oid
get_indextable_indexid_by_indextableid(Oid indextableid)
{
	Relation	indrel;
	SysScanDesc indscan;
	ScanKeyData skey;
	HeapTuple	htup;
	Oid		 result = InvalidOid;

	/* Prepare to scan pg_index for entries having indrelid = this rel. */
	ScanKeyInit(&skey,
				Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(indextableid));

	indrel = heap_open(IndexRelationId, AccessShareLock);
	indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true,
								 NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		Form_pg_index index = (Form_pg_index) GETSTRUCT(htup);

		/*
		 * Ignore any indexes that are currently being dropped.  This will
		 * prevent them from being searched, inserted into, or considered in
		 * HOT-safety decisions.  It's unsafe to touch such an index at all
		 * since its catalog entries could disappear at any instant.
		 */
		if (!IndexIsLive(index))
			continue;

		/*
		 * Invalid, non-unique, non-immediate or predicate indexes aren't
		 * interesting for either oid indexes or replication identity indexes,
		 * so don't check them.
		 */
		if (!IndexIsValid(index) || !index->indimmediate ||
			!heap_attisnull(htup, Anum_pg_index_indpred, NULL))
			continue;

		result = index->indexrelid;
		/*
		 * Index table has only one index.
		 */
		break;
	}

	systable_endscan(indscan);
	heap_close(indrel, AccessShareLock);

	return result;
}

/*
 * build_index_table_targetlist
 *	 Build the targetlist for index table index scan.
 */
static List *
build_index_table_targetlist(Relation relation, Index varno)
{
	List	   *tlist = NIL;
	Var		   *var;
	int			attrno, numattrs;

	numattrs = RelationGetNumberOfAttributes(relation);
	for (attrno = 1; attrno <= numattrs; attrno++)
	{
		Form_pg_attribute att_tup = &relation->rd_att->attrs[attrno - 1];

		var = makeVar(varno,
					  attrno,
					  att_tup->atttypid,
					  att_tup->atttypmod,
					  att_tup->attcollation,
					  0);

		tlist = lappend(tlist,
						makeTargetEntry((Expr *) var,
										attrno,
										NULL,
										false));
	}

	return tlist;
}

/*
 * create_indexscan_plan
 *	  Returns an indexscan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 * We use this for both plain IndexScans and IndexOnlyScans, because the
 * qual preprocessing work is the same for both.  Note that the caller tells
 * us which to build --- we don't look at best_path->path.pathtype, because
 * create_bitmap_subplan needs to be able to override the prior decision.
 */
static Scan *
create_indexscan_plan(PlannerInfo *root,
					  IndexPath *best_path,
					  List *tlist,
					  List *scan_clauses,
					  bool indexonly)
{
	Scan	   *scan_plan;
	List	   *indexquals = best_path->indexquals;
	List	   *indexorderbys = best_path->indexorderbys;
	Index		baserelid = best_path->path.parent->relid;
	Oid			indexoid = best_path->indexinfo->indexoid;
	List	   *qpqual;
	List	   *stripped_indexquals;
	List	   *fixed_indexquals;
	List	   *fixed_indexorderbys;
	List	   *indexorderbyops = NIL;
	ListCell   *l;

#ifdef __AUDIT_FGA__
    char            *operation = "select";
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt = NULL;
    bool            need_audit_fga_quals = true;
    ListCell        *item;
#endif

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/*
	 * Build "stripped" indexquals structure (no RestrictInfos) to pass to
	 * executor as indexqualorig
	 */
	stripped_indexquals = get_actual_clauses(indexquals);

	/*
	 * The executor needs a copy with the indexkey on the left of each clause
	 * and with index Vars substituted for table ones.
	 */
	fixed_indexquals = fix_indexqual_references(root, best_path);

	/*
	 * Likewise fix up index attr references in the ORDER BY expressions.
	 */
	fixed_indexorderbys = fix_indexorderby_references(root, best_path);

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index, other than pseudoconstant clauses which will be handled
	 * by a separate gating plan node.  All the predicates in the indexquals
	 * will be checked (either by the index itself, or by nodeIndexscan.c),
	 * but if there are any "special" operators involved then they must be
	 * included in qpqual.  The upshot is that qpqual must contain
	 * scan_clauses minus whatever appears in indexquals.
	 *
	 * In normal cases simple pointer equality checks will be enough to spot
	 * duplicate RestrictInfos, so we try that first.
	 *
	 * Another common case is that a scan_clauses entry is generated from the
	 * same EquivalenceClass as some indexqual, and is therefore redundant
	 * with it, though not equal.  (This happens when indxpath.c prefers a
	 * different derived equality than what generate_join_implied_equalities
	 * picked for a parameterized scan's ppi_clauses.)
	 *
	 * In some situations (particularly with OR'd index conditions) we may
	 * have scan_clauses that are not equal to, but are logically implied by,
	 * the index quals; so we also try a predicate_implied_by() check to see
	 * if we can discard quals that way.  (predicate_implied_by assumes its
	 * first input contains only immutable functions, so we have to check
	 * that.)
	 *
	 * Note: if you change this bit of code you should also look at
	 * extract_nonindex_conditions() in costsize.c.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);

		if (rinfo->pseudoconstant)
			continue;			/* we may drop pseudoconstants here */
		if (list_member_ptr(indexquals, rinfo))
			continue;			/* simple duplicate */
		if (is_redundant_derived_clause(rinfo, indexquals))
			continue;			/* derived from same EquivalenceClass */
		if (!contain_mutable_functions((Node *) rinfo->clause) &&
			predicate_implied_by(list_make1(rinfo->clause), indexquals, false))
			continue;			/* provably implied by indexquals */
		qpqual = lappend(qpqual, rinfo);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(root, qpqual);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	qpqual = extract_actual_clauses(qpqual, false);

	/*
	 * We have to replace any outer-relation variables with nestloop params in
	 * the indexqualorig, qpqual, and indexorderbyorig expressions.  A bit
	 * annoying to have to do this separately from the processing in
	 * fix_indexqual_references --- rethink this when generalizing the inner
	 * indexscan support.  But note we can't really do this earlier because
	 * it'd break the comparisons to predicates above ... (or would it?  Those
	 * wouldn't have outer refs)
	 */
	if (best_path->path.param_info)
	{
		stripped_indexquals = (List *)
			replace_nestloop_params(root, (Node *) stripped_indexquals);
		qpqual = (List *)
			replace_nestloop_params(root, (Node *) qpqual);
		indexorderbys = (List *)
			replace_nestloop_params(root, (Node *) indexorderbys);
	}

	/*
	 * If there are ORDER BY expressions, look up the sort operators for their
	 * result datatypes.
	 */
	if (indexorderbys)
	{
		ListCell   *pathkeyCell,
				   *exprCell;

		/*
		 * PathKey contains OID of the btree opfamily we're sorting by, but
		 * that's not quite enough because we need the expression's datatype
		 * to look up the sort operator in the operator family.
		 */
		Assert(list_length(best_path->path.pathkeys) == list_length(indexorderbys));
		forboth(pathkeyCell, best_path->path.pathkeys, exprCell, indexorderbys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(pathkeyCell);
			Node	   *expr = (Node *) lfirst(exprCell);
			Oid			exprtype = exprType(expr);
			Oid			sortop;

			/* Get sort operator from opfamily */
			sortop = get_opfamily_member(pathkey->pk_opfamily,
										 exprtype,
										 exprtype,
										 pathkey->pk_strategy);
			if (!OidIsValid(sortop))
				elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
					 pathkey->pk_strategy, exprtype, exprtype, pathkey->pk_opfamily);
			indexorderbyops = lappend_oid(indexorderbyops, sortop);
		}
	}

	/* Finally ready to build the plan node */
	if (indexonly)
		scan_plan = (Scan *) make_indexonlyscan(tlist,
												qpqual,
												baserelid,
												indexoid,
												fixed_indexquals,
												fixed_indexorderbys,
												best_path->indexinfo->indextlist,
												best_path->indexscandir);
	/* Build plan for global index scan */
	else if (best_path->indexglobal && !(tlist == NULL && scan_clauses == NULL))
	{
		Oid index_table_indexoid	 = InvalidOid;
		IndexScan *indexscan = NULL;
		TidScan   *tidscan	 = NULL;
		List	  *indextable_tlist = NIL;
		Relation  indexTable;
		RangeTblEntry *rte;
		Index	  indextable_varno;

		if (IS_DISTRIBUTED_DATANODE && !g_index_table_part_name)
			elog(ERROR, "can not SELECT FROM a table with global index on DN without planning on CN");

		indexTable = heap_open(best_path->indexinfo->indexoid, NoLock);

		index_table_indexoid = get_indextable_indexid_by_indextableid(RelationGetRelid(indexTable));

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->alias = NULL;
		rte->relid = RelationGetRelid(indexTable);
		rte->relkind = indexTable->rd_rel->relkind;

		/*
		* Build the list of effective column names using user-supplied aliases
		* and/or actual column names.
		*/
		rte->eref = makeAlias(RelationGetRelationName(indexTable), NIL);

		/*
		 * Set flags and access permissions.
		 *
		 * The initial default on access checks is always check-for-READ-access,
		 * which is the right thing for all except target tables.
		 */
		rte->lateral = false;

		rte->requiredPerms = ACL_SELECT;
		rte->checkAsUser = InvalidOid;	  /* not set-uid by default, either */
		rte->selectedCols = NULL;
		rte->insertedCols = NULL;
		rte->updatedCols = NULL;

		/*
		 * Add completed RTE to pstate's range table list, but not to join list
		 * nor namespace --- caller must do that if appropriate.
		 */
		root->parse->rtable = lappend(root->parse->rtable, rte);

		indextable_varno = list_length(root->parse->rtable);

		indextable_tlist = build_index_table_targetlist(indexTable, indextable_varno);

		indexscan = make_indexscan(indextable_tlist,
								   NIL,
								   indextable_varno,
								   index_table_indexoid,
								   fixed_indexquals,
								   stripped_indexquals,
								   NIL,
								   NIL,
								   NIL,
								   best_path->indexscandir);

		tidscan = make_tidscan(tlist,
							   qpqual,
							   baserelid,
							   NIL);
		
		make_tidscan_remote(tidscan, (Plan *) indexscan, baserelid,
							rt_fetch(baserelid, root->parse->rtable),
							stripped_indexquals, root->parse->commandType != CMD_SELECT);
		
		scan_plan = (Scan *) tidscan;

		heap_close(indexTable, NoLock);

		/*
		 * Must disable the posteriori estimation for FQS if we have a global
		 * index scan plan.
		 */
		root->glob->has_gidx_plan = true;
	}
	else
		scan_plan = (Scan *) make_indexscan(tlist,
											qpqual,
											baserelid,
											indexoid,
											fixed_indexquals,
											stripped_indexquals,
											fixed_indexorderbys,
											indexorderbys,
											indexorderbyops,
											best_path->indexscandir);
#ifdef __AUDIT_FGA__
    if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
    {
        scan_plan->plan.audit_fga_quals = NULL;

        audit_rte = planner_rt_fetch(baserelid, root);

        need_audit_fga_quals = get_audit_fga_quals(audit_rte->relid, operation, tlist, &audit_fga_quals_stmt);
        if (need_audit_fga_quals)
        {
            foreach (item, audit_fga_quals_stmt)
            {
                AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
                if (!audit_fga_qual->qual)
                {
                    audit_fga_log_policy_info(audit_fga_qual, operation);

                    audit_fga_quals_stmt = list_delete_ptr(audit_fga_quals_stmt, audit_fga_qual);                   
                }
            }

            if (audit_fga_quals_stmt != NULL)
                scan_plan->plan.audit_fga_quals = audit_fga_quals_stmt;
        }
    }
#endif

	copy_generic_path_info(&scan_plan->plan, &best_path->path);

	return scan_plan;
}

/*
 * create_bitmap_scan_plan
 *	  Returns a bitmap scan plan for the base relation scanned by 'best_path'
 *	  with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static BitmapHeapScan *
create_bitmap_scan_plan(PlannerInfo *root,
						BitmapHeapPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		baserelid = best_path->path.parent->relid;
	Plan	   *bitmapqualplan;
	List	   *bitmapqualorig;
	List	   *indexquals;
	List	   *indexECs;
	List	   *qpqual;
	ListCell   *l;
	BitmapHeapScan *scan_plan;

#ifdef __AUDIT_FGA__
    char            *operation = "select";
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt = NULL;
    bool            need_audit_fga_quals = true;
    ListCell        *item;
#endif    

	/* it should be a base rel... */
	Assert(baserelid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	/* Process the bitmapqual tree into a Plan tree and qual lists */
	bitmapqualplan = create_bitmap_subplan(root, best_path->bitmapqual,
										   &bitmapqualorig, &indexquals,
										   &indexECs);

	if (best_path->path.parallel_aware)
		bitmap_subplan_mark_shared(bitmapqualplan);

	/*
	 * The qpqual list must contain all restrictions not automatically handled
	 * by the index, other than pseudoconstant clauses which will be handled
	 * by a separate gating plan node.  All the predicates in the indexquals
	 * will be checked (either by the index itself, or by
	 * nodeBitmapHeapscan.c), but if there are any "special" operators
	 * involved then they must be added to qpqual.  The upshot is that qpqual
	 * must contain scan_clauses minus whatever appears in indexquals.
	 *
	 * This loop is similar to the comparable code in create_indexscan_plan(),
	 * but with some differences because it has to compare the scan clauses to
	 * stripped (no RestrictInfos) indexquals.  See comments there for more
	 * info.
	 *
	 * In normal cases simple equal() checks will be enough to spot duplicate
	 * clauses, so we try that first.  We next see if the scan clause is
	 * redundant with any top-level indexqual by virtue of being generated
	 * from the same EC.  After that, try predicate_implied_by().
	 *
	 * Unlike create_indexscan_plan(), the predicate_implied_by() test here is
	 * useful for getting rid of qpquals that are implied by index predicates,
	 * because the predicate conditions are included in the "indexquals"
	 * returned by create_bitmap_subplan().  Bitmap scans have to do it that
	 * way because predicate conditions need to be rechecked if the scan
	 * becomes lossy, so they have to be included in bitmapqualorig.
	 */
	qpqual = NIL;
	foreach(l, scan_clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, l);
		Node	   *clause = (Node *) rinfo->clause;

		if (rinfo->pseudoconstant)
			continue;			/* we may drop pseudoconstants here */
		if (list_member(indexquals, clause))
			continue;			/* simple duplicate */
		if (rinfo->parent_ec && list_member_ptr(indexECs, rinfo->parent_ec))
			continue;			/* derived from same EquivalenceClass */
		if (!contain_mutable_functions(clause) &&
			predicate_implied_by(list_make1(clause), indexquals, false))
			continue;			/* provably implied by indexquals */
		qpqual = lappend(qpqual, rinfo);
	}

	/* Sort clauses into best execution order */
	qpqual = order_qual_clauses(root, qpqual);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	qpqual = extract_actual_clauses(qpqual, false);

	/*
	 * When dealing with special operators, we will at this point have
	 * duplicate clauses in qpqual and bitmapqualorig.  We may as well drop
	 * 'em from bitmapqualorig, since there's no point in making the tests
	 * twice.
	 */
	bitmapqualorig = list_difference_ptr(bitmapqualorig, qpqual);

	/*
	 * We have to replace any outer-relation variables with nestloop params in
	 * the qpqual and bitmapqualorig expressions.  (This was already done for
	 * expressions attached to plan nodes in the bitmapqualplan tree.)
	 */
	if (best_path->path.param_info)
	{
		qpqual = (List *)
			replace_nestloop_params(root, (Node *) qpqual);
		bitmapqualorig = (List *)
			replace_nestloop_params(root, (Node *) bitmapqualorig);
	}

	/* Finally ready to build the plan node */
	scan_plan = make_bitmap_heapscan(tlist,
									 qpqual,
									 bitmapqualplan,
									 bitmapqualorig,
									 baserelid);

#ifdef __AUDIT_FGA__
    if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
    {
        scan_plan->scan.plan.audit_fga_quals = NULL;

        audit_rte = planner_rt_fetch(baserelid, root);

        need_audit_fga_quals = get_audit_fga_quals(audit_rte->relid, operation, tlist, &audit_fga_quals_stmt);
        if (need_audit_fga_quals)
        {
            foreach (item, audit_fga_quals_stmt)
            {
                AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
                if (!audit_fga_qual->qual)
                {
                    audit_fga_log_policy_info(audit_fga_qual, operation);

                    audit_fga_quals_stmt = list_delete_ptr(audit_fga_quals_stmt, audit_fga_qual);                   
                }
            }

            if (audit_fga_quals_stmt != NULL)
                scan_plan->scan.plan.audit_fga_quals = audit_fga_quals_stmt;
        }
    }
#endif    

	copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * Given a bitmapqual tree, generate the Plan tree that implements it
 *
 * As byproducts, we also return in *qual and *indexqual the qual lists
 * (in implicit-AND form, without RestrictInfos) describing the original index
 * conditions and the generated indexqual conditions.  (These are the same in
 * simple cases, but when special index operators are involved, the former
 * list includes the special conditions while the latter includes the actual
 * indexable conditions derived from them.)  Both lists include partial-index
 * predicates, because we have to recheck predicates as well as index
 * conditions if the bitmap scan becomes lossy.
 *
 * In addition, we return a list of EquivalenceClass pointers for all the
 * top-level indexquals that were possibly-redundantly derived from ECs.
 * This allows removal of scan_clauses that are redundant with such quals.
 * (We do not attempt to detect such redundancies for quals that are within
 * OR subtrees.  This could be done in a less hacky way if we returned the
 * indexquals in RestrictInfo form, but that would be slower and still pretty
 * messy, since we'd have to build new RestrictInfos in many cases.)
 */
static Plan *
create_bitmap_subplan(PlannerInfo *root, Path *bitmapqual,
					  List **qual, List **indexqual, List **indexECs)
{
	Plan	   *plan;

	if (IsA(bitmapqual, BitmapAndPath))
	{
		BitmapAndPath *apath = (BitmapAndPath *) bitmapqual;
		List	   *subplans = NIL;
		List	   *subquals = NIL;
		List	   *subindexquals = NIL;
		List	   *subindexECs = NIL;
		ListCell   *l;
		double      nodes = 1;

#ifdef __OPENTENBASE__
		nodes = path_count_datanodes(&apath->path);
#endif

		/*
		 * There may well be redundant quals among the subplans, since a
		 * top-level WHERE qual might have gotten used to form several
		 * different index quals.  We don't try exceedingly hard to eliminate
		 * redundancies, but we do eliminate obvious duplicates by using
		 * list_concat_unique.
		 */
		foreach(l, apath->bitmapquals)
		{
			Plan	   *subplan;
			List	   *subqual;
			List	   *subindexqual;
			List	   *subindexEC;

			subplan = create_bitmap_subplan(root, (Path *) lfirst(l),
											&subqual, &subindexqual,
											&subindexEC);
			subplans = lappend(subplans, subplan);
			subquals = list_concat_unique(subquals, subqual);
			subindexquals = list_concat_unique(subindexquals, subindexqual);
			/* Duplicates in indexECs aren't worth getting rid of */
			subindexECs = list_concat(subindexECs, subindexEC);
		}
		plan = (Plan *) make_bitmap_and(subplans);
		plan->startup_cost = apath->path.startup_cost;
		plan->total_cost = apath->path.total_cost;
		plan->plan_rows =
			clamp_row_est(apath->bitmapselectivity * apath->path.parent->tuples / nodes);
		plan->plan_width = 0;	/* meaningless */
		plan->parallel_aware = false;
		plan->parallel_safe = apath->path.parallel_safe;
		*qual = subquals;
		*indexqual = subindexquals;
		*indexECs = subindexECs;
	}
	else if (IsA(bitmapqual, BitmapOrPath))
	{
		BitmapOrPath *opath = (BitmapOrPath *) bitmapqual;
		List	   *subplans = NIL;
		List	   *subquals = NIL;
		List	   *subindexquals = NIL;
		bool		const_true_subqual = false;
		bool		const_true_subindexqual = false;
		ListCell   *l;

		/*
		 * Here, we only detect qual-free subplans.  A qual-free subplan would
		 * cause us to generate "... OR true ..."  which we may as well reduce
		 * to just "true".  We do not try to eliminate redundant subclauses
		 * because (a) it's not as likely as in the AND case, and (b) we might
		 * well be working with hundreds or even thousands of OR conditions,
		 * perhaps from a long IN list.  The performance of list_append_unique
		 * would be unacceptable.
		 */
		foreach(l, opath->bitmapquals)
		{
			Plan	   *subplan;
			List	   *subqual;
			List	   *subindexqual;
			List	   *subindexEC;

			subplan = create_bitmap_subplan(root, (Path *) lfirst(l),
											&subqual, &subindexqual,
											&subindexEC);
			subplans = lappend(subplans, subplan);
			if (subqual == NIL)
				const_true_subqual = true;
			else if (!const_true_subqual)
				subquals = lappend(subquals,
								   make_ands_explicit(subqual));
			if (subindexqual == NIL)
				const_true_subindexqual = true;
			else if (!const_true_subindexqual)
				subindexquals = lappend(subindexquals,
										make_ands_explicit(subindexqual));
		}

		/*
		 * In the presence of ScalarArrayOpExpr quals, we might have built
		 * BitmapOrPaths with just one subpath; don't add an OR step.
		 */
		if (list_length(subplans) == 1)
		{
			plan = (Plan *) linitial(subplans);
		}
		else
		{
			double  nodes = 1;
#ifdef __OPENTENBASE__
			nodes = path_count_datanodes(&opath->path);
#endif
			plan = (Plan *) make_bitmap_or(subplans);
			plan->startup_cost = opath->path.startup_cost;
			plan->total_cost = opath->path.total_cost;
			plan->plan_rows =
				clamp_row_est(opath->bitmapselectivity * opath->path.parent->tuples / nodes);
			plan->plan_width = 0;	/* meaningless */
			plan->parallel_aware = false;
			plan->parallel_safe = opath->path.parallel_safe;
		}

		/*
		 * If there were constant-TRUE subquals, the OR reduces to constant
		 * TRUE.  Also, avoid generating one-element ORs, which could happen
		 * due to redundancy elimination or ScalarArrayOpExpr quals.
		 */
		if (const_true_subqual)
			*qual = NIL;
		else if (list_length(subquals) <= 1)
			*qual = subquals;
		else
			*qual = list_make1(make_orclause(subquals));
		if (const_true_subindexqual)
			*indexqual = NIL;
		else if (list_length(subindexquals) <= 1)
			*indexqual = subindexquals;
		else
			*indexqual = list_make1(make_orclause(subindexquals));
		*indexECs = NIL;
	}
	else if (IsA(bitmapqual, IndexPath))
	{
		IndexPath  *ipath = (IndexPath *) bitmapqual;
		IndexScan  *iscan;
		List	   *subindexECs;
		ListCell   *l;
		double      nodes = 1;
#ifdef __OPENTENBASE__
		nodes = path_count_datanodes(&ipath->path);
#endif
		/* Use the regular indexscan plan build machinery... */
		iscan = castNode(IndexScan,
						 create_indexscan_plan(root, ipath,
											   NIL, NIL, false));
		/* then convert to a bitmap indexscan */
		plan = (Plan *) make_bitmap_indexscan(iscan->scan.scanrelid, iscan->indexid,
		                                      iscan->indexqual, iscan->indexqualorig);
		if (root->isPartIteratorPlanning && enable_partition_iterator)
		{
			Scan *scan = (Scan *) plan;
			scan->isPartTbl = true;
			scan->partScanDirection = ForwardScanDirection;
			scan->partIterParamno = root->curIteratorParamIndex;
			scan->leafNum = root->curleafNum;
			scan->partition_leaf_rels = list_copy(ipath->path.parent->partition_leaf_rels);
		}
		/* and set its cost/width fields appropriately */
		plan->startup_cost = 0.0;
		plan->total_cost = ipath->indextotalcost;
		plan->plan_rows =
			clamp_row_est(ipath->indexselectivity * ipath->path.parent->tuples / nodes);
		plan->plan_width = 0;	/* meaningless */
		plan->parallel_aware = false;
		plan->parallel_safe = ipath->path.parallel_safe;
		*qual = get_actual_clauses(ipath->indexclauses);
		*indexqual = get_actual_clauses(ipath->indexquals);
		foreach(l, ipath->indexinfo->indpred)
		{
			Expr	   *pred = (Expr *) lfirst(l);

			/*
			 * We know that the index predicate must have been implied by the
			 * query condition as a whole, but it may or may not be implied by
			 * the conditions that got pushed into the bitmapqual.  Avoid
			 * generating redundant conditions.
			 */
			if (!predicate_implied_by(list_make1(pred), ipath->indexclauses,
									  false))
			{
				*qual = lappend(*qual, pred);
				*indexqual = lappend(*indexqual, pred);
			}
		}
		subindexECs = NIL;
		foreach(l, ipath->indexquals)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

			if (rinfo->parent_ec)
				subindexECs = lappend(subindexECs, rinfo->parent_ec);
		}
		*indexECs = subindexECs;
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d", nodeTag(bitmapqual));
		plan = NULL;			/* keep compiler quiet */
	}

	return plan;
}

/*
 * create_tidscan_plan
 *	 Returns a tidscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TidScan *
create_tidscan_plan(PlannerInfo *root, TidPath *best_path,
					List *tlist, List *scan_clauses)
{
	TidScan    *scan_plan;
	Index		scan_relid = best_path->path.parent->relid;
	List	   *tidquals = best_path->tidquals;
	List	   *ortidquals;

#ifdef __AUDIT_FGA__
    char            *operation = "select";
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt = NULL;
    bool            need_audit_fga_quals = true;
    ListCell        *item;
#endif    

	/* it should be a base rel... */
	Assert(scan_relid > 0);
	Assert(best_path->path.parent->rtekind == RTE_RELATION);


	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->path.param_info)
	{
		tidquals = (List *)
			replace_nestloop_params(root, (Node *) tidquals);
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	/*
	 * Remove any clauses that are TID quals.  This is a bit tricky since the
	 * tidquals list has implicit OR semantics.
	 */
	ortidquals = tidquals;
	if (list_length(ortidquals) > 1)
		ortidquals = list_make1(make_orclause(ortidquals));
	scan_clauses = list_difference(scan_clauses, ortidquals);

	scan_plan = make_tidscan(tlist,
							 scan_clauses,
							 scan_relid,
							 tidquals);

#ifdef __AUDIT_FGA__
    if (enable_fga && g_commandTag && (strcmp(g_commandTag, "SELECT") == 0))
    {
        scan_plan->scan.plan.audit_fga_quals = NULL;

        audit_rte = planner_rt_fetch(scan_relid, root);

        need_audit_fga_quals = get_audit_fga_quals(audit_rte->relid, operation, tlist, &audit_fga_quals_stmt);
        if (need_audit_fga_quals)
        {
            foreach (item, audit_fga_quals_stmt)
            {
                AuditFgaPolicy *audit_fga_qual = (AuditFgaPolicy *) lfirst(item);
                if (!audit_fga_qual->qual)
                {
                    audit_fga_log_policy_info(audit_fga_qual, operation);

                    audit_fga_quals_stmt = list_delete_ptr(audit_fga_quals_stmt, audit_fga_qual);                   
                }
            }
        
            if (audit_fga_quals_stmt != NULL)
                scan_plan->scan.plan.audit_fga_quals = audit_fga_quals_stmt;
        }
    }
#endif    

	copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);

	return scan_plan;
}

/*
 * create_subqueryscan_plan
 *	 Returns a subqueryscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan *
create_subqueryscan_plan(PlannerInfo *root, SubqueryScanPath *best_path,
						 List *tlist, List *scan_clauses)
{
	SubqueryScan *scan_plan;
	RelOptInfo *rel = best_path->path.parent;
	Index		scan_relid = rel->relid;
	Plan	   *subplan;

	/* it should be a subquery base rel... */
	Assert(scan_relid > 0);
	Assert(rel->rtekind == RTE_SUBQUERY);

	/*
	 * If a subquery is searched for many times, the paths in the candidate
	 * pathlist correspond to different subroot. When the create plan stage
	 * is reached, only one best path remains, and other paths have been
	 * eliminated. So use subroot in best path to correct subroot in RelOptInfo.
	 */
	if (best_path->subroot != NULL &&
		rel->subroot != best_path->subroot)
	{
		rel->subroot = best_path->subroot;
	}

	/*
	 * Recursively create Plan from Path for subquery.  Since we are entering
	 * a different planner context (subroot), recurse to create_plan not
	 * create_plan_recurse.
	 */
	subplan = create_plan(rel->subroot, best_path->subpath);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->path.param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		process_subquery_nestloop_params(root,
										 rel->subplan_params);
	}

	scan_plan = make_subqueryscan(tlist,
								  scan_clauses,
								  scan_relid,
								  subplan);

	copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);
	return scan_plan;
}

/*
 * create_functionscan_plan
 *	 Returns a functionscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static FunctionScan *
create_functionscan_plan(PlannerInfo *root, Path *best_path,
						 List *tlist, List *scan_clauses)
{
	FunctionScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	List	   *functions;

	/* it should be a function base rel... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_FUNCTION);
	functions = rte->functions;

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		/* The function expressions could contain nestloop params, too */
		functions = (List *) replace_nestloop_params(root, (Node *) functions);
	}

	scan_plan = make_functionscan(tlist, scan_clauses, scan_relid,
								  functions, rte->funcordinality);

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_tablefuncscan_plan
 *	 Returns a tablefuncscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TableFuncScan *
create_tablefuncscan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses)
{
	TableFuncScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	TableFunc  *tablefunc;

	/* it should be a function base rel... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_TABLEFUNC);
	tablefunc = rte->tablefunc;

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		/* The function expressions could contain nestloop params, too */
		tablefunc = (TableFunc *) replace_nestloop_params(root, (Node *) tablefunc);
	}

	scan_plan = make_tablefuncscan(tlist, scan_clauses, scan_relid,
								   tablefunc);

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_valuesscan_plan
 *	 Returns a valuesscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ValuesScan *
create_valuesscan_plan(PlannerInfo *root, Path *best_path,
					   List *tlist, List *scan_clauses)
{
	ValuesScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	List	   *values_lists;

	/* it should be a values base rel... */
	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_VALUES);
	values_lists = rte->values_lists;

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
		/* The values lists could contain nestloop params, too */
		values_lists = (List *)
			replace_nestloop_params(root, (Node *) values_lists);
	}

	scan_plan = make_valuesscan(tlist, scan_clauses, scan_relid,
								values_lists);

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_ctescan_plan
 *	 Returns a ctescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static CteScan *
create_ctescan_plan(PlannerInfo *root, Path *best_path,
					List *tlist, List *scan_clauses)
{
	CteScan    *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte = NULL;
	SubPlan    *ctesplan = NULL;
	int			plan_id;
	int			cte_param_id;
	PlannerInfo *cteroot;
	Index		levelsup;
	int			ndx;
	ListCell   *lc;

	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_CTE);
	Assert(!rte->self_reference);

	/*
	 * Find the referenced CTE, and locate the SubPlan previously made for it.
	 */
	levelsup = rte->ctelevelsup;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}

	/*
	 * Note: cte_plan_ids can be shorter than cteList, if we are still working
	 * on planning the CTEs (ie, this is a side-reference from another CTE).
	 * So we mustn't use forboth here.
	 */
	ndx = 0;
	foreach(lc, cteroot->parse->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

		if (strcmp(cte->ctename, rte->ctename) == 0)
			break;
		ndx++;
	}
	if (lc == NULL)				/* shouldn't happen */
		elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
	if (ndx >= list_length(cteroot->cte_plan_ids))
		elog(ERROR, "could not find plan for CTE \"%s\"", rte->ctename);
	plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);

	if (plan_id <= 0)
		elog(ERROR, "no plan was made for CTE \"%s\"", rte->ctename);
	foreach(lc, cteroot->init_plans)
	{
		ctesplan = (SubPlan *) lfirst(lc);
		if (ctesplan->plan_id == plan_id)
			break;
	}
	if (lc == NULL)				/* shouldn't happen */
		elog(ERROR, "could not find plan for CTE \"%s\"", rte->ctename);

	/*
	 * We need the CTE param ID, which is the sole member of the SubPlan's
	 * setParam list.
	 */
	cte_param_id = linitial_int(ctesplan->setParam);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	scan_plan = make_ctescan(tlist, scan_clauses, scan_relid,
							 plan_id, cte_param_id);

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_namedtuplestorescan_plan
 *	 Returns a tuplestorescan plan for the base relation scanned by
 *	'best_path' with restriction clauses 'scan_clauses' and targetlist
 *	'tlist'.
 */
static NamedTuplestoreScan *
create_namedtuplestorescan_plan(PlannerInfo *root, Path *best_path,
								List *tlist, List *scan_clauses)
{
	NamedTuplestoreScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;

	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_NAMEDTUPLESTORE);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	scan_plan = make_namedtuplestorescan(tlist, scan_clauses, scan_relid,
										 rte->enrname);

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_worktablescan_plan
 *	 Returns a worktablescan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static WorkTableScan *
create_worktablescan_plan(PlannerInfo *root, Path *best_path,
						  List *tlist, List *scan_clauses)
{
	WorkTableScan *scan_plan;
	Index		scan_relid = best_path->parent->relid;
	RangeTblEntry *rte;
	Index		levelsup;
	PlannerInfo *cteroot;

	Assert(scan_relid > 0);
	rte = planner_rt_fetch(scan_relid, root);
	Assert(rte->rtekind == RTE_CTE);
	Assert(rte->self_reference);

	/*
	 * We need to find the worktable param ID, which is in the plan level
	 * that's processing the recursive UNION, which is one level *below* where
	 * the CTE comes from.
	 */
	levelsup = rte->ctelevelsup;
	if (levelsup == 0)			/* shouldn't happen */
		elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	levelsup--;
	cteroot = root;
	while (levelsup-- > 0)
	{
		cteroot = cteroot->parent_root;
		if (!cteroot)			/* shouldn't happen */
			elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
	}
	if (cteroot->wt_param_id < 0)	/* shouldn't happen */
		elog(ERROR, "could not find param ID for CTE \"%s\"", rte->ctename);

	/* Sort clauses into best execution order */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->param_info)
	{
		scan_clauses = (List *)
			replace_nestloop_params(root, (Node *) scan_clauses);
	}

	scan_plan = make_worktablescan(tlist, scan_clauses, scan_relid,
								   cteroot->wt_param_id);

	copy_generic_path_info(&scan_plan->scan.plan, best_path);

	return scan_plan;
}

/*
 * create_foreignscan_plan
 *	 Returns a foreignscan plan for the relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ForeignScan *
create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path,
						List *tlist, List *scan_clauses)
{
	ForeignScan *scan_plan;
	RelOptInfo *rel = best_path->path.parent;
	Index		scan_relid = rel->relid;
	Oid			rel_oid = InvalidOid;
	Plan	   *outer_plan = NULL;

	Assert(rel->fdwroutine != NULL);

	/* transform the child path if any */
	if (best_path->fdw_outerpath)
		outer_plan = create_plan_recurse(root, best_path->fdw_outerpath,
										 CP_EXACT_TLIST);

	/*
	 * If we're scanning a base relation, fetch its OID.  (Irrelevant if
	 * scanning a join relation.)
	 */
	if (scan_relid > 0)
	{
		RangeTblEntry *rte;

		Assert(rel->rtekind == RTE_RELATION);
		rte = planner_rt_fetch(scan_relid, root);
		Assert(rte->rtekind == RTE_RELATION);
		rel_oid = rte->relid;
	}

	/*
	 * Sort clauses into best execution order.  We do this first since the FDW
	 * might have more info than we do and wish to adjust the ordering.
	 */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/*
	 * Let the FDW perform its processing on the restriction clauses and
	 * generate the plan node.  Note that the FDW might remove restriction
	 * clauses that it intends to execute remotely, or even add more (if it
	 * has selected some join clauses for remote use but also wants them
	 * rechecked locally).
	 */
	scan_plan = rel->fdwroutine->GetForeignPlan(root, rel, rel_oid,
												best_path,
												tlist, scan_clauses,
												outer_plan);

	/* Copy cost data from Path to Plan; no need to make FDW do this */
	copy_generic_path_info(&scan_plan->scan.plan, &best_path->path);

	/* Copy foreign server OID; likewise, no need to make FDW do this */
	scan_plan->fs_server = rel->serverid;

	/*
	 * Likewise, copy the relids that are represented by this foreign scan. An
	 * upper rel doesn't have relids set, but it covers all the base relations
	 * participating in the underlying scan, so use root's all_baserels.
	 */
	if (IS_UPPER_REL(rel))
		scan_plan->fs_relids = root->all_baserels;
	else
		scan_plan->fs_relids = best_path->path.parent->relids;

	/*
	 * If this is a foreign join, and to make it valid to push down we had to
	 * assume that the current user is the same as some user explicitly named
	 * in the query, mark the finished plan as depending on the current user.
	 */
	if (rel->useridiscurrent)
		root->glob->dependsOnRole = true;

	/*
	 * Replace any outer-relation variables with nestloop params in the qual,
	 * fdw_exprs and fdw_recheck_quals expressions.  We do this last so that
	 * the FDW doesn't have to be involved.  (Note that parts of fdw_exprs or
	 * fdw_recheck_quals could have come from join clauses, so doing this
	 * beforehand on the scan_clauses wouldn't work.)  We assume
	 * fdw_scan_tlist contains no such variables.
	 */
	if (best_path->path.param_info)
	{
		scan_plan->scan.plan.qual = (List *)
			replace_nestloop_params(root, (Node *) scan_plan->scan.plan.qual);
		scan_plan->fdw_exprs = (List *)
			replace_nestloop_params(root, (Node *) scan_plan->fdw_exprs);
		scan_plan->fdw_recheck_quals = (List *)
			replace_nestloop_params(root,
									(Node *) scan_plan->fdw_recheck_quals);
	}

	/*
	 * If rel is a base relation, detect whether any system columns are
	 * requested from the rel.  (If rel is a join relation, rel->relid will be
	 * 0, but there can be no Var with relid 0 in the rel's targetlist or the
	 * restriction clauses, so we skip this in that case.  Note that any such
	 * columns in base relations that were joined are assumed to be contained
	 * in fdw_scan_tlist.)	This is a bit of a kluge and might go away
	 * someday, so we intentionally leave it out of the API presented to FDWs.
	 */
	scan_plan->fsSystemCol = false;
	if (scan_relid > 0)
	{
		Bitmapset  *attrs_used = NULL;
		ListCell   *lc;
		int			i;

		/*
		 * First, examine all the attributes needed for joins or final output.
		 * Note: we must look at rel's targetlist, not the attr_needed data,
		 * because attr_needed isn't computed for inheritance child rels.
		 */
		pull_varattnos((Node *) rel->reltarget->exprs, scan_relid, &attrs_used);

		/* Add all the attributes used by restriction clauses. */
		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			pull_varattnos((Node *) rinfo->clause, scan_relid, &attrs_used);
		}

		/* Now, are any system columns requested from rel? */
		for (i = FirstLowInvalidHeapAttributeNumber + 1; i < 0; i++)
		{
			if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
			{
				scan_plan->fsSystemCol = true;
				break;
			}
		}

		bms_free(attrs_used);
	}

	return scan_plan;
}

/*
 * create_custom_plan
 *
 * Transform a CustomPath into a Plan.
 */
static CustomScan *
create_customscan_plan(PlannerInfo *root, CustomPath *best_path,
					   List *tlist, List *scan_clauses)
{
	CustomScan *cplan;
	RelOptInfo *rel = best_path->path.parent;
	List	   *custom_plans = NIL;
	ListCell   *lc;

	/* Recursively transform child paths. */
	foreach(lc, best_path->custom_paths)
	{
		Plan	   *plan = create_plan_recurse(root, (Path *) lfirst(lc),
											   CP_EXACT_TLIST);

		custom_plans = lappend(custom_plans, plan);
	}

	/*
	 * Sort clauses into the best execution order, although custom-scan
	 * provider can reorder them again.
	 */
	scan_clauses = order_qual_clauses(root, scan_clauses);

	/*
	 * Invoke custom plan provider to create the Plan node represented by the
	 * CustomPath.
	 */
	cplan = castNode(CustomScan,
					 best_path->methods->PlanCustomPath(root,
														rel,
														best_path,
														tlist,
														scan_clauses,
														custom_plans));

	/*
	 * Copy cost data from Path to Plan; no need to make custom-plan providers
	 * do this
	 */
	copy_generic_path_info(&cplan->scan.plan, &best_path->path);

	/* Likewise, copy the relids that are represented by this custom scan */
	cplan->custom_relids = best_path->path.parent->relids;

	/*
	 * Replace any outer-relation variables with nestloop params in the qual
	 * and custom_exprs expressions.  We do this last so that the custom-plan
	 * provider doesn't have to be involved.  (Note that parts of custom_exprs
	 * could have come from join clauses, so doing this beforehand on the
	 * scan_clauses wouldn't work.)  We assume custom_scan_tlist contains no
	 * such variables.
	 */
	if (best_path->path.param_info)
	{
		cplan->scan.plan.qual = (List *)
			replace_nestloop_params(root, (Node *) cplan->scan.plan.qual);
		cplan->custom_exprs = (List *)
			replace_nestloop_params(root, (Node *) cplan->custom_exprs);
	}

	return cplan;
}


/*****************************************************************************
 *
 *	JOIN METHODS
 *
 *****************************************************************************/

static NestLoop *
create_nestloop_plan(PlannerInfo *root,
					 NestPath *best_path)
{
	NestLoop   *join_plan;
	Plan	   *outer_plan;
	Plan	   *inner_plan;
	List	   *tlist = build_path_tlist(root, &best_path->path);
	List	   *joinrestrictclauses = best_path->joinrestrictinfo;
	List	   *joinclauses;
	List	   *otherclauses;
	Relids		outerrelids;
	List	   *nestParams;
	Relids		saveOuterRels = root->curOuterRels;
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	/* NestLoop can project, so no need to be picky about child tlists */
	outer_plan = create_plan_recurse(root, best_path->outerjoinpath, 0);

	/* For a nestloop, include outer relids in curOuterRels for inner side */
	root->curOuterRels = bms_union(root->curOuterRels,
								   best_path->outerjoinpath->parent->relids);

	inner_plan = create_plan_recurse(root, best_path->innerjoinpath, 0);

	/* Restore curOuterRels */
	bms_free(root->curOuterRels);
	root->curOuterRels = saveOuterRels;

	/* Sort join qual clauses into best execution order */
	joinrestrictclauses = order_qual_clauses(root, joinrestrictclauses);

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jointype))
	{
		extract_actual_join_clauses(joinrestrictclauses,
									best_path->path.parent->relids,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinrestrictclauses, false);
		otherclauses = NIL;
	}

	/* Replace any outer-relation variables with nestloop params */
	if (best_path->path.param_info)
	{
		joinclauses = (List *)
			replace_nestloop_params(root, (Node *) joinclauses);
		otherclauses = (List *)
			replace_nestloop_params(root, (Node *) otherclauses);
	}

	/*
	 * Identify any nestloop parameters that should be supplied by this join
	 * node, and move them from root->curOuterParams to the nestParams list.
	 */
	outerrelids = best_path->outerjoinpath->parent->relids;
	nestParams = NIL;
	prev = NULL;
	for (cell = list_head(root->curOuterParams); cell; cell = next)
	{
		NestLoopParam *nlp = (NestLoopParam *) lfirst(cell);

		next = lnext(cell);
		if (IsA(nlp->paramval, Var) &&
			bms_is_member(nlp->paramval->varno, outerrelids))
		{
			root->curOuterParams = list_delete_cell(root->curOuterParams,
													cell, prev);
			nestParams = lappend(nestParams, nlp);
		}
		else if (IsA(nlp->paramval, PlaceHolderVar) &&
				 bms_overlap(((PlaceHolderVar *) nlp->paramval)->phrels,
							 outerrelids) &&
				 bms_is_subset(find_placeholder_info(root,
													 (PlaceHolderVar *) nlp->paramval,
													 false)->ph_eval_at,
							   outerrelids))
		{
			root->curOuterParams = list_delete_cell(root->curOuterParams,
													cell, prev);
			nestParams = lappend(nestParams, nlp);
		}
		else
			prev = cell;
	}

	/*
	 * While NestLoop is executed it rescans inner plan. We do not want to
	 * rescan RemoteSubplan and do not support it.
	 * So if inner_plan is a RemoteSubplan, materialize it.
	 */
	if (nestParams == NULL &&
		contain_remote_subplan(inner_plan) &&
		!ExecMaterializesOutput(nodeTag(inner_plan)))
	{
		Plan	   *matplan = (Plan *) make_material(inner_plan);

		/*
		 * We assume the materialize will not spill to disk, and therefore
		 * charge just cpu_operator_cost per tuple.  (Keep this estimate in
		 * sync with cost_mergejoin.)
		 */
		copy_plan_costsize(matplan, inner_plan);
		matplan->total_cost += cpu_operator_cost * matplan->plan_rows;

		inner_plan = matplan;
	}

	join_plan = make_nestloop(tlist,
							  joinclauses,
							  otherclauses,
							  nestParams,
							  outer_plan,
							  inner_plan,
							  best_path->jointype,
							  best_path->inner_unique);

	join_plan->join.plannerinfo_id = root->plannerinfo_seq_no;

	copy_generic_path_info(&join_plan->join.plan, &best_path->path);

	return join_plan;
}

static MergeJoin *
create_mergejoin_plan(PlannerInfo *root,
					  MergePath *best_path)
{
	MergeJoin  *join_plan;
	Plan	   *outer_plan;
	Plan	   *inner_plan;
	List	   *tlist = build_path_tlist(root, &best_path->jpath.path);
	List	   *joinclauses;
	List	   *otherclauses;
	List	   *mergeclauses;
	List	   *outerpathkeys;
	List	   *innerpathkeys;
	int			nClauses;
	Oid		   *mergefamilies;
	Oid		   *mergecollations;
	int		   *mergestrategies;
	bool	   *mergenullsfirst;
	int			i;
	ListCell   *lc;
	ListCell   *lop;
	ListCell   *lip;
	Path	   *outer_path = best_path->jpath.outerjoinpath;
	Path	   *inner_path = best_path->jpath.innerjoinpath;

	/*
	 * MergeJoin can project, so we don't have to demand exact tlists from the
	 * inputs.  However, if we're intending to sort an input's result, it's
	 * best to request a small tlist so we aren't sorting more data than
	 * necessary.
	 */
	outer_plan = create_plan_recurse(root, best_path->jpath.outerjoinpath,
									 (best_path->outersortkeys != NIL) ? CP_SMALL_TLIST : 0);

	inner_plan = create_plan_recurse(root, best_path->jpath.innerjoinpath,
									 (best_path->innersortkeys != NIL) ? CP_SMALL_TLIST : 0);

	/* Sort join qual clauses into best execution order */
	/* NB: do NOT reorder the mergeclauses */
	joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jpath.jointype))
	{
		extract_actual_join_clauses(joinclauses,
									best_path->jpath.path.parent->relids,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinclauses, false);
		otherclauses = NIL;
	}

	/*
	 * Remove the mergeclauses from the list of join qual clauses, leaving the
	 * list of quals that must be checked as qpquals.
	 */
	mergeclauses = get_actual_clauses(best_path->path_mergeclauses);
	joinclauses = list_difference(joinclauses, mergeclauses);

	/*
	 * Replace any outer-relation variables with nestloop params.  There
	 * should not be any in the mergeclauses.
	 */
	if (best_path->jpath.path.param_info)
	{
		joinclauses = (List *)
			replace_nestloop_params(root, (Node *) joinclauses);
		otherclauses = (List *)
			replace_nestloop_params(root, (Node *) otherclauses);
	}

	/*
	 * Rearrange mergeclauses, if needed, so that the outer variable is always
	 * on the left; mark the mergeclause restrictinfos with correct
	 * outer_is_left status.
	 */
	mergeclauses = get_switched_clauses(best_path->path_mergeclauses,
										best_path->jpath.outerjoinpath->parent->relids);

	/*
	 * Create explicit sort nodes for the outer and inner paths if necessary.
	 */
	if (best_path->outersortkeys)
	{
		Relids		outer_relids = outer_path->parent->relids;
		Sort	   *sort = make_sort_from_pathkeys(outer_plan,
												   best_path->outersortkeys,
												   outer_relids);

		label_sort_with_costsize(root, sort, -1.0);
		outer_plan = (Plan *) sort;
		outerpathkeys = best_path->outersortkeys;
	}
	else
		outerpathkeys = best_path->jpath.outerjoinpath->pathkeys;

	if (best_path->innersortkeys)
	{
		Relids		inner_relids = inner_path->parent->relids;
		Sort	   *sort = make_sort_from_pathkeys(inner_plan,
												   best_path->innersortkeys,
												   inner_relids);

		label_sort_with_costsize(root, sort, -1.0);
		inner_plan = (Plan *) sort;
		innerpathkeys = best_path->innersortkeys;
	}
	else
		innerpathkeys = best_path->jpath.innerjoinpath->pathkeys;

	/*
	 * If specified, add a materialize node to shield the inner plan from the
	 * need to handle mark/restore.
	 */
	if (best_path->materialize_inner)
	{
		Plan	   *matplan = (Plan *) make_material(inner_plan);

		/*
		 * We assume the materialize will not spill to disk, and therefore
		 * charge just cpu_operator_cost per tuple.  (Keep this estimate in
		 * sync with final_cost_mergejoin.)
		 */
		copy_plan_costsize(matplan, inner_plan);
		matplan->total_cost += cpu_operator_cost * matplan->plan_rows;

		inner_plan = matplan;
	}

	/*
	 * Compute the opfamily/collation/strategy/nullsfirst arrays needed by the
	 * executor.  The information is in the pathkeys for the two inputs, but
	 * we need to be careful about the possibility of mergeclauses sharing a
	 * pathkey (compare find_mergeclauses_for_pathkeys()).
	 */
	nClauses = list_length(mergeclauses);
	Assert(nClauses == list_length(best_path->path_mergeclauses));
	mergefamilies = (Oid *) palloc(nClauses * sizeof(Oid));
	mergecollations = (Oid *) palloc(nClauses * sizeof(Oid));
	mergestrategies = (int *) palloc(nClauses * sizeof(int));
	mergenullsfirst = (bool *) palloc(nClauses * sizeof(bool));

	lop = list_head(outerpathkeys);
	lip = list_head(innerpathkeys);
	i = 0;
	foreach(lc, best_path->path_mergeclauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		EquivalenceClass *oeclass;
		EquivalenceClass *ieclass;
		PathKey    *opathkey;
		PathKey    *ipathkey;
		EquivalenceClass *opeclass;
		EquivalenceClass *ipeclass;
		ListCell   *l2;

		/* fetch outer/inner eclass from mergeclause */
		if (rinfo->outer_is_left)
		{
			oeclass = rinfo->left_ec;
			ieclass = rinfo->right_ec;
		}
		else
		{
			oeclass = rinfo->right_ec;
			ieclass = rinfo->left_ec;
		}
		Assert(oeclass != NULL);
		Assert(ieclass != NULL);

		/*
		 * For debugging purposes, we check that the eclasses match the paths'
		 * pathkeys.  In typical cases the merge clauses are one-to-one with
		 * the pathkeys, but when dealing with partially redundant query
		 * conditions, we might have clauses that re-reference earlier path
		 * keys.  The case that we need to reject is where a pathkey is
		 * entirely skipped over.
		 *
		 * lop and lip reference the first as-yet-unused pathkey elements;
		 * it's okay to match them, or any element before them.  If they're
		 * NULL then we have found all pathkey elements to be used.
		 */
		if (lop)
		{
			opathkey = (PathKey *) lfirst(lop);
			opeclass = opathkey->pk_eclass;
			if (oeclass == opeclass)
			{
				/* fast path for typical case */
				lop = lnext(lop);
			}
			else
			{
				/* redundant clauses ... must match something before lop */
				foreach(l2, outerpathkeys)
				{
					if (l2 == lop)
						break;
					opathkey = (PathKey *) lfirst(l2);
					opeclass = opathkey->pk_eclass;
					if (oeclass == opeclass)
						break;
				}
				if (oeclass != opeclass)
					elog(ERROR, "outer pathkeys do not match mergeclauses");
			}
		}
		else
		{
			/* redundant clauses ... must match some already-used pathkey */
			opathkey = NULL;
			opeclass = NULL;
			foreach(l2, outerpathkeys)
			{
				opathkey = (PathKey *) lfirst(l2);
				opeclass = opathkey->pk_eclass;
				if (oeclass == opeclass)
					break;
			}
			if (l2 == NULL)
				elog(ERROR, "outer pathkeys do not match mergeclauses");
		}

		if (lip)
		{
			ipathkey = (PathKey *) lfirst(lip);
			ipeclass = ipathkey->pk_eclass;
			if (ieclass == ipeclass)
			{
				/* fast path for typical case */
				lip = lnext(lip);
			}
			else
			{
				/* redundant clauses ... must match something before lip */
				foreach(l2, innerpathkeys)
				{
					if (l2 == lip)
						break;
					ipathkey = (PathKey *) lfirst(l2);
					ipeclass = ipathkey->pk_eclass;
					if (ieclass == ipeclass)
						break;
				}
				if (ieclass != ipeclass)
					elog(ERROR, "inner pathkeys do not match mergeclauses");
			}
		}
		else
		{
			/* redundant clauses ... must match some already-used pathkey */
			ipathkey = NULL;
			ipeclass = NULL;
			foreach(l2, innerpathkeys)
			{
				ipathkey = (PathKey *) lfirst(l2);
				ipeclass = ipathkey->pk_eclass;
				if (ieclass == ipeclass)
					break;
			}
			if (l2 == NULL)
				elog(ERROR, "inner pathkeys do not match mergeclauses");
		}

		/* pathkeys should match each other too (more debugging) */
		if (opathkey->pk_opfamily != ipathkey->pk_opfamily ||
			opathkey->pk_eclass->ec_collation != ipathkey->pk_eclass->ec_collation ||
			opathkey->pk_strategy != ipathkey->pk_strategy ||
			opathkey->pk_nulls_first != ipathkey->pk_nulls_first)
			elog(ERROR, "left and right pathkeys do not match in mergejoin");

		/* OK, save info for executor */
		mergefamilies[i] = opathkey->pk_opfamily;
		mergecollations[i] = opathkey->pk_eclass->ec_collation;
		mergestrategies[i] = opathkey->pk_strategy;
		mergenullsfirst[i] = opathkey->pk_nulls_first;
		i++;
	}

	/*
	 * Note: it is not an error if we have additional pathkey elements (i.e.,
	 * lop or lip isn't NULL here).  The input paths might be better-sorted
	 * than we need for the current mergejoin.
	 */

	/*
	 * Now we can build the mergejoin node.
	 */
	join_plan = make_mergejoin(tlist,
							   joinclauses,
							   otherclauses,
							   mergeclauses,
							   mergefamilies,
							   mergecollations,
							   mergestrategies,
							   mergenullsfirst,
							   outer_plan,
							   inner_plan,
							   best_path->jpath.jointype,
							   best_path->jpath.inner_unique,
							   best_path->skip_mark_restore,
							   best_path->is_not_full);

	join_plan->join.plannerinfo_id = root->plannerinfo_seq_no;

	/* Costs of sort and material steps are included in path cost already */
	copy_generic_path_info(&join_plan->join.plan, &best_path->jpath.path);

	return join_plan;
}

static HashJoin *
create_hashjoin_plan(PlannerInfo *root,
					 HashPath *best_path)
{
	HashJoin   *join_plan;
	Hash	   *hash_plan;
	Plan	   *outer_plan;
	Plan	   *inner_plan;
	List	   *tlist = build_path_tlist(root, &best_path->jpath.path);
	List	   *joinclauses;
	List	   *otherclauses;
	List	   *hashclauses;
	List	   *hashoperators = NIL;
	List	   *hashcollations = NIL;
	List	   *inner_hashkeys = NIL;
	List	   *outer_hashkeys = NIL;
	Oid			skewTable = InvalidOid;
	AttrNumber	skewColumn = InvalidAttrNumber;
	bool		skewInherit = false;
	ListCell	*lc;

	/*
	 * HashJoin can project, so we don't have to demand exact tlists from the
	 * inputs.  However, it's best to request a small tlist from the inner
	 * side, so that we aren't storing more data than necessary.  Likewise, if
	 * we anticipate batching, request a small tlist from the outer side so
	 * that we don't put extra data in the outer batch files.
	 */
	outer_plan = create_plan_recurse(root, best_path->jpath.outerjoinpath,
									 (best_path->num_batches > 1) ? CP_SMALL_TLIST : 0);

	inner_plan = create_plan_recurse(root, best_path->jpath.innerjoinpath,
									 CP_SMALL_TLIST);

	/* Sort join qual clauses into best execution order */
	joinclauses = order_qual_clauses(root, best_path->jpath.joinrestrictinfo);
	/* There's no point in sorting the hash clauses ... */

	/* Get the join qual clauses (in plain expression form) */
	/* Any pseudoconstant clauses are ignored here */
	if (IS_OUTER_JOIN(best_path->jpath.jointype))
	{
		extract_actual_join_clauses(joinclauses,
									best_path->jpath.path.parent->relids,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinclauses, false);
		otherclauses = NIL;
	}

	/*
	 * Remove the hashclauses from the list of join qual clauses, leaving the
	 * list of quals that must be checked as qpquals.
	 */
	hashclauses = get_actual_clauses(best_path->path_hashclauses);
	joinclauses = list_difference(joinclauses, hashclauses);

	/*
	 * Replace any outer-relation variables with nestloop params.  There
	 * should not be any in the hashclauses.
	 */
	if (best_path->jpath.path.param_info)
	{
		joinclauses = (List *)
			replace_nestloop_params(root, (Node *) joinclauses);
		otherclauses = (List *)
			replace_nestloop_params(root, (Node *) otherclauses);
	}

	/*
	 * Rearrange hashclauses, if needed, so that the outer variable is always
	 * on the left.
	 */
#ifdef __OPENTENBASE_C__
	if (!best_path->nonequijoin)
	{
#endif
	hashclauses = get_switched_clauses(best_path->path_hashclauses,
									   best_path->jpath.outerjoinpath->parent->relids);
#ifdef __OPENTENBASE_C__
	}
#endif
	/*
	 * If there is a single join clause and we can identify the outer variable
	 * as a simple column reference, supply its identity for possible use in
	 * skew optimization.  (Note: in principle we could do skew optimization
	 * with multiple join clauses, but we'd have to be able to determine the
	 * most common combinations of outer values, which we don't currently have
	 * enough stats for.)
	 */
#ifdef __OPENTENBASE_C__
	if (!best_path->nonequijoin && list_length(hashclauses) == 1)
#else
	if (list_length(hashclauses) == 1)
#endif
	{
		OpExpr	   *clause = (OpExpr *) linitial(hashclauses);
		Node	   *node;

		Assert(is_opclause(clause));
		node = (Node *) linitial(clause->args);
		if (IsA(node, RelabelType))
			node = (Node *) ((RelabelType *) node)->arg;
		if (IsA(node, Var))
		{
			Var		   *var = (Var *) node;
			RangeTblEntry *rte;

			rte = root->simple_rte_array[var->varno];
			if (rte->rtekind == RTE_RELATION)
			{
				skewTable = rte->relid;
				skewColumn = var->varattno;
				skewInherit = rte->inh;
			}
		}
	}

	/*
	 * Collect hash related information. The hashed expressions are
	 * deconstructed into outer/inner expressions, so they can be computed
	 * separately (inner expressions are used to build the hashtable via Hash,
	 * outer expressions to perform lookups of tuples from HashJoin's outer
	 * plan in the hashtable). Also collect operator information necessary to
	 * build the hashtable.
	 */
#ifdef __OPENTENBASE_C__
	if (best_path->nonequijoin)
	{
		foreach(lc, hashclauses)
		{
			DistinctExpr *disexpr = NULL;
			BoolExpr *hclause = lfirst_node(BoolExpr, lc);
			if (!IsA(hclause, BoolExpr))
			{
				elog(ERROR, "hashclause must be boolexpr in non-equal join");
			}

			disexpr = linitial_node(DistinctExpr, hclause->args);
			if (!IsA(disexpr, DistinctExpr))
			{
				elog(ERROR, "hashclause' args must be DistinctExpr in non-equal join");
			}

			hashoperators = lappend_oid(hashoperators, disexpr->opno);
			hashcollations = lappend_oid(hashcollations, disexpr->inputcollid);
			outer_hashkeys = lappend(outer_hashkeys, linitial(disexpr->args));
			inner_hashkeys = lappend(inner_hashkeys, lsecond(disexpr->args));
		}
	}
	else
	{
#endif
		foreach(lc, hashclauses)
		{
			OpExpr	   *hclause = lfirst_node(OpExpr, lc);

			hashoperators = lappend_oid(hashoperators, hclause->opno);
			hashcollations = lappend_oid(hashcollations, hclause->inputcollid);
			outer_hashkeys = lappend(outer_hashkeys, linitial(hclause->args));
			inner_hashkeys = lappend(inner_hashkeys, lsecond(hclause->args));
		}
#ifdef __OPENTENBASE_C__
	}
#endif

	/*
	 * Build the hash node and hash join node.
	 */
	hash_plan = make_hash(inner_plan,
						  inner_hashkeys,
						  skewTable,
						  skewColumn,
						  skewInherit);

	/*
	 * Set Hash node's startup & total costs equal to total cost of input
	 * plan; this only affects EXPLAIN display not decisions.
	 */
	copy_plan_costsize(&hash_plan->plan, inner_plan);
	hash_plan->plan.startup_cost = hash_plan->plan.total_cost;

	/*
	 * If parallel-aware, the executor will also need an estimate of the total
	 * number of rows expected from all participants so that it can size the
	 * shared hash table.
	 */
	if (best_path->jpath.path.parallel_aware)
	{
		hash_plan->plan.parallel_aware = true;
		hash_plan->rows_total = best_path->inner_rows_total;
	}

#ifdef __OPENTENBASE_C__
	/* non-equal join does need otherclauses */
	if (best_path->nonequijoin)
	{
		otherclauses = NULL;
	}
#endif

	join_plan = make_hashjoin(tlist,
							  joinclauses,
							  otherclauses,
							  hashclauses,
							  hashoperators,
							  hashcollations,
							  outer_hashkeys,
							  outer_plan,
							  (Plan *) hash_plan,
							  best_path->jpath.jointype,
							  best_path->jpath.inner_unique);

	copy_generic_path_info(&join_plan->join.plan, &best_path->jpath.path);

	join_plan->num_batches = best_path->num_batches;
	join_plan->num_buckets = best_path->num_buckets;
	join_plan->innerbucketsize = best_path->innerbucketsize;
	join_plan->hashjointuples = best_path->hashjointuples;
	join_plan->join.plannerinfo_id = root->plannerinfo_seq_no;

#ifdef __OPENTENBASE__
	join_plan->nonequijoin = best_path->nonequijoin;
#endif

	return join_plan;
}

/*****************************************************************************
 *
 *	SUPPORTING ROUTINES
 *
 *****************************************************************************/

/*
 * replace_nestloop_params
 *	  Replace outer-relation Vars and PlaceHolderVars in the given expression
 *	  with nestloop Params
 *
 * All Vars and PlaceHolderVars belonging to the relation(s) identified by
 * root->curOuterRels are replaced by Params, and entries are added to
 * root->curOuterParams if not already present.
 */
static Node *
replace_nestloop_params(PlannerInfo *root, Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return replace_nestloop_params_mutator(expr, root);
}

static Node *
replace_nestloop_params_mutator(Node *node, PlannerInfo *root)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		Param	   *param;
		NestLoopParam *nlp;
		ListCell   *lc;

		/* Upper-level Vars should be long gone at this point */
		Assert(var->varlevelsup == 0);
		/* If not to be replaced, we can just return the Var unmodified */
		if (IS_SPECIAL_VARNO(var->varno) ||
			!bms_is_member(var->varno, root->curOuterRels))
			return node;
		/* Create a Param representing the Var */
		param = assign_nestloop_param_var(root, var);
		/* Is this param already listed in root->curOuterParams? */
		foreach(lc, root->curOuterParams)
		{
			nlp = (NestLoopParam *) lfirst(lc);
			if (nlp->paramno == param->paramid)
			{
				Assert(equal(var, nlp->paramval));
				/* Present, so we can just return the Param */
				return (Node *) param;
			}
		}
		/* No, so add it */
		nlp = makeNode(NestLoopParam);
		nlp->paramno = param->paramid;
		nlp->paramval = var;
		root->curOuterParams = lappend(root->curOuterParams, nlp);
		/* And return the replacement Param */
		return (Node *) param;
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;
		Param	   *param;
		NestLoopParam *nlp;
		ListCell   *lc;

		/* Upper-level PlaceHolderVars should be long gone at this point */
		Assert(phv->phlevelsup == 0);

		/*
		 * Check whether we need to replace the PHV.  We use bms_overlap as a
		 * cheap/quick test to see if the PHV might be evaluated in the outer
		 * rels, and then grab its PlaceHolderInfo to tell for sure.
		 */
		if (!bms_overlap(phv->phrels, root->curOuterRels) ||
			!bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at,
						   root->curOuterRels))
		{
			/*
			 * We can't replace the whole PHV, but we might still need to
			 * replace Vars or PHVs within its expression, in case it ends up
			 * actually getting evaluated here.  (It might get evaluated in
			 * this plan node, or some child node; in the latter case we don't
			 * really need to process the expression here, but we haven't got
			 * enough info to tell if that's the case.)  Flat-copy the PHV
			 * node and then recurse on its expression.
			 *
			 * Note that after doing this, we might have different
			 * representations of the contents of the same PHV in different
			 * parts of the plan tree.  This is OK because equal() will just
			 * match on phid/phlevelsup, so setrefs.c will still recognize an
			 * upper-level reference to a lower-level copy of the same PHV.
			 */
			PlaceHolderVar *newphv = makeNode(PlaceHolderVar);

			memcpy(newphv, phv, sizeof(PlaceHolderVar));
			newphv->phexpr = (Expr *)
				replace_nestloop_params_mutator((Node *) phv->phexpr,
												root);
			return (Node *) newphv;
		}
		/* Create a Param representing the PlaceHolderVar */
		param = assign_nestloop_param_placeholdervar(root, phv);
		/* Is this param already listed in root->curOuterParams? */
		foreach(lc, root->curOuterParams)
		{
			nlp = (NestLoopParam *) lfirst(lc);
			if (nlp->paramno == param->paramid)
			{
				Assert(equal(phv, nlp->paramval));
				/* Present, so we can just return the Param */
				return (Node *) param;
			}
		}
		/* No, so add it */
		nlp = makeNode(NestLoopParam);
		nlp->paramno = param->paramid;
		nlp->paramval = (Var *) phv;
		root->curOuterParams = lappend(root->curOuterParams, nlp);
		/* And return the replacement Param */
		return (Node *) param;
	}
	return expression_tree_mutator(node,
								   replace_nestloop_params_mutator,
								   (void *) root);
}

/*
 * process_subquery_nestloop_params
 *	  Handle params of a parameterized subquery that need to be fed
 *	  from an outer nestloop.
 *
 * Currently, that would be *all* params that a subquery in FROM has demanded
 * from the current query level, since they must be LATERAL references.
 *
 * The subplan's references to the outer variables are already represented
 * as PARAM_EXEC Params, so we need not modify the subplan here.  What we
 * do need to do is add entries to root->curOuterParams to signal the parent
 * nestloop plan node that it must provide these values.
 */
static void
process_subquery_nestloop_params(PlannerInfo *root, List *subplan_params)
{
	ListCell   *ppl;

	foreach(ppl, subplan_params)
	{
		PlannerParamItem *pitem = (PlannerParamItem *) lfirst(ppl);

		if (IsA(pitem->item, Var))
		{
			Var		   *var = (Var *) pitem->item;
			NestLoopParam *nlp;
			ListCell   *lc;

			/* If not from a nestloop outer rel, complain */
			if (!bms_is_member(var->varno, root->curOuterRels))
				elog(ERROR, "non-LATERAL parameter required by subquery");
			/* Is this param already listed in root->curOuterParams? */
			foreach(lc, root->curOuterParams)
			{
				nlp = (NestLoopParam *) lfirst(lc);
				if (nlp->paramno == pitem->paramId)
				{
					Assert(equal(var, nlp->paramval));
					/* Present, so nothing to do */
					break;
				}
			}
			if (lc == NULL)
			{
				/* No, so add it */
				nlp = makeNode(NestLoopParam);
				nlp->paramno = pitem->paramId;
				nlp->paramval = copyObject(var);
				root->curOuterParams = lappend(root->curOuterParams, nlp);
			}
		}
		else if (IsA(pitem->item, PlaceHolderVar))
		{
			PlaceHolderVar *phv = (PlaceHolderVar *) pitem->item;
			NestLoopParam *nlp;
			ListCell   *lc;

			/* If not from a nestloop outer rel, complain */
			if (!bms_is_subset(find_placeholder_info(root, phv, false)->ph_eval_at,
							   root->curOuterRels))
				elog(ERROR, "non-LATERAL parameter required by subquery");
			/* Is this param already listed in root->curOuterParams? */
			foreach(lc, root->curOuterParams)
			{
				nlp = (NestLoopParam *) lfirst(lc);
				if (nlp->paramno == pitem->paramId)
				{
					Assert(equal(phv, nlp->paramval));
					/* Present, so nothing to do */
					break;
				}
			}
			if (lc == NULL)
			{
				/* No, so add it */
				nlp = makeNode(NestLoopParam);
				nlp->paramno = pitem->paramId;
				nlp->paramval = (Var *) copyObject(phv);
				root->curOuterParams = lappend(root->curOuterParams, nlp);
			}
		}
		else
			elog(ERROR, "unexpected type of subquery parameter");
	}
}

/*
 * fix_indexqual_references
 *	  Adjust indexqual clauses to the form the executor's indexqual
 *	  machinery needs.
 *
 * We have four tasks here:
 *	* Remove RestrictInfo nodes from the input clauses.
 *	* Replace any outer-relation Var or PHV nodes with nestloop Params.
 *	  (XXX eventually, that responsibility should go elsewhere?)
 *	* Index keys must be represented by Var nodes with varattno set to the
 *	  index's attribute number, not the attribute number in the original rel.
 *	* If the index key is on the right, commute the clause to put it on the
 *	  left.
 *
 * The result is a modified copy of the path's indexquals list --- the
 * original is not changed.  Note also that the copy shares no substructure
 * with the original; this is needed in case there is a subplan in it (we need
 * two separate copies of the subplan tree, or things will go awry).
 */
static List *
fix_indexqual_references(PlannerInfo *root, IndexPath *index_path)
{
	IndexOptInfo *index = index_path->indexinfo;
	List	   *fixed_indexquals;
	ListCell   *lcc,
			   *lci;

	fixed_indexquals = NIL;

	forboth(lcc, index_path->indexquals, lci, index_path->indexqualcols)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lcc);
		int			indexcol = lfirst_int(lci);
		Node	   *clause;

		/*
		 * Replace any outer-relation variables with nestloop params.
		 *
		 * This also makes a copy of the clause, so it's safe to modify it
		 * in-place below.
		 */
		clause = replace_nestloop_params(root, (Node *) rinfo->clause);

		if (IsA(clause, OpExpr))
		{
			OpExpr	   *op = (OpExpr *) clause;

			if (list_length(op->args) != 2)
				elog(ERROR, "indexqual clause is not binary opclause");

			/*
			 * Check to see if the indexkey is on the right; if so, commute
			 * the clause.  The indexkey should be the side that refers to
			 * (only) the base relation.
			 */
			if (!bms_equal(rinfo->left_relids, index->rel->relids))
				CommuteOpExpr(op);

			/*
			 * Now replace the indexkey expression with an index Var.
			 */
			linitial(op->args) = fix_indexqual_operand(linitial(op->args),
													   index,
													   indexcol);
		}
		else if (IsA(clause, RowCompareExpr))
		{
			RowCompareExpr *rc = (RowCompareExpr *) clause;
			Expr	   *newrc;
			List	   *indexcolnos;
			bool		var_on_left;
			ListCell   *lca,
					   *lcai;

			/*
			 * Re-discover which index columns are used in the rowcompare.
			 */
			newrc = adjust_rowcompare_for_index(rc,
												index,
												indexcol,
												&indexcolnos,
												&var_on_left);

			/*
			 * Trouble if adjust_rowcompare_for_index thought the
			 * RowCompareExpr didn't match the index as-is; the clause should
			 * have gone through that routine already.
			 */
			if (newrc != (Expr *) rc)
				elog(ERROR, "inconsistent results from adjust_rowcompare_for_index");

			/*
			 * Check to see if the indexkey is on the right; if so, commute
			 * the clause.
			 */
			if (!var_on_left)
				CommuteRowCompareExpr(rc);

			/*
			 * Now replace the indexkey expressions with index Vars.
			 */
			Assert(list_length(rc->largs) == list_length(indexcolnos));
			forboth(lca, rc->largs, lcai, indexcolnos)
			{
				lfirst(lca) = fix_indexqual_operand(lfirst(lca),
													index,
													lfirst_int(lcai));
			}
		}
		else if (IsA(clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;

			/* Never need to commute... */

			/* Replace the indexkey expression with an index Var. */
			linitial(saop->args) = fix_indexqual_operand(linitial(saop->args),
														 index,
														 indexcol);
		}
		else if (IsA(clause, NullTest))
		{
			NullTest   *nt = (NullTest *) clause;

			/* Replace the indexkey expression with an index Var. */
			nt->arg = (Expr *) fix_indexqual_operand((Node *) nt->arg,
													 index,
													 indexcol);
		}
		else
			elog(ERROR, "unsupported indexqual type: %d",
				 (int) nodeTag(clause));

		fixed_indexquals = lappend(fixed_indexquals, clause);
	}

	return fixed_indexquals;
}

/*
 * fix_indexorderby_references
 *	  Adjust indexorderby clauses to the form the executor's index
 *	  machinery needs.
 *
 * This is a simplified version of fix_indexqual_references.  The input does
 * not have RestrictInfo nodes, and we assume that indxpath.c already
 * commuted the clauses to put the index keys on the left.  Also, we don't
 * bother to support any cases except simple OpExprs, since nothing else
 * is allowed for ordering operators.
 */
static List *
fix_indexorderby_references(PlannerInfo *root, IndexPath *index_path)
{
	IndexOptInfo *index = index_path->indexinfo;
	List	   *fixed_indexorderbys;
	ListCell   *lcc,
			   *lci;

	fixed_indexorderbys = NIL;

	forboth(lcc, index_path->indexorderbys, lci, index_path->indexorderbycols)
	{
		Node	   *clause = (Node *) lfirst(lcc);
		int			indexcol = lfirst_int(lci);

		/*
		 * Replace any outer-relation variables with nestloop params.
		 *
		 * This also makes a copy of the clause, so it's safe to modify it
		 * in-place below.
		 */
		clause = replace_nestloop_params(root, clause);

		if (IsA(clause, OpExpr))
		{
			OpExpr	   *op = (OpExpr *) clause;

			if (list_length(op->args) != 2)
				elog(ERROR, "indexorderby clause is not binary opclause");

			/*
			 * Now replace the indexkey expression with an index Var.
			 */
			linitial(op->args) = fix_indexqual_operand(linitial(op->args),
													   index,
													   indexcol);
		}
		else
			elog(ERROR, "unsupported indexorderby type: %d",
				 (int) nodeTag(clause));

		fixed_indexorderbys = lappend(fixed_indexorderbys, clause);
	}

	return fixed_indexorderbys;
}

/*
 * fix_indexqual_operand
 *	  Convert an indexqual expression to a Var referencing the index column.
 *
 * We represent index keys by Var nodes having varno == INDEX_VAR and varattno
 * equal to the index's attribute number (index column position).
 *
 * Most of the code here is just for sanity cross-checking that the given
 * expression actually matches the index column it's claimed to.
 */
static Node *
fix_indexqual_operand(Node *node, IndexOptInfo *index, int indexcol)
{
	Var		   *result;
	int			pos;
	ListCell   *indexpr_item;

	/*
	 * Remove any binary-compatible relabeling of the indexkey
	 */
	if (IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;

	Assert(indexcol >= 0 && indexcol < index->ncolumns);

	if (index->indexkeys[indexcol] != 0)
	{
		/* It's a simple index column */
		if (IsA(node, Var) &&
			((Var *) node)->varno == index->rel->relid &&
			((Var *) node)->varattno == index->indexkeys[indexcol])
		{
			result = (Var *) copyObject(node);
			result->varno = INDEX_VAR;
			result->varattno = indexcol + 1;
			return (Node *) result;
		}
		else
			elog(ERROR, "index key does not match expected index column");
	}

	/* It's an index expression, so find and cross-check the expression */
	indexpr_item = list_head(index->indexprs);
	for (pos = 0; pos < index->ncolumns; pos++)
	{
		if (index->indexkeys[pos] == 0)
		{
			if (indexpr_item == NULL)
				elog(ERROR, "too few entries in indexprs list");
			if (pos == indexcol)
			{
				Node	   *indexkey;

				indexkey = (Node *) lfirst(indexpr_item);
				if (indexkey && IsA(indexkey, RelabelType))
					indexkey = (Node *) ((RelabelType *) indexkey)->arg;
				if (equal(node, indexkey))
				{
					result = makeVar(INDEX_VAR, indexcol + 1,
									 exprType(lfirst(indexpr_item)), -1,
									 exprCollation(lfirst(indexpr_item)),
									 0);
					return (Node *) result;
				}
				else
					elog(ERROR, "index key does not match expected index column");
			}
			indexpr_item = lnext(indexpr_item);
		}
	}

	/* Oops... */
	elog(ERROR, "index key does not match expected index column");
	return NULL;				/* keep compiler quiet */
}

/*
 * get_switched_clauses
 *	  Given a list of merge or hash joinclauses (as RestrictInfo nodes),
 *	  extract the bare clauses, and rearrange the elements within the
 *	  clauses, if needed, so the outer join variable is on the left and
 *	  the inner is on the right.  The original clause data structure is not
 *	  touched; a modified list is returned.  We do, however, set the transient
 *	  outer_is_left field in each RestrictInfo to show which side was which.
 */
static List *
get_switched_clauses(List *clauses, Relids outerrelids)
{
	List	   *t_list = NIL;
	ListCell   *l;

	foreach(l, clauses)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);
		OpExpr	   *clause = (OpExpr *) restrictinfo->clause;

		Assert(is_opclause(clause));
		if (bms_is_subset(restrictinfo->right_relids, outerrelids))
		{
			/*
			 * Duplicate just enough of the structure to allow commuting the
			 * clause without changing the original list.  Could use
			 * copyObject, but a complete deep copy is overkill.
			 */
			OpExpr	   *temp = makeNode(OpExpr);

			temp->opno = clause->opno;
			temp->opfuncid = InvalidOid;
			temp->opresulttype = clause->opresulttype;
			temp->opretset = clause->opretset;
			temp->opcollid = clause->opcollid;
			temp->inputcollid = clause->inputcollid;
			temp->args = list_copy(clause->args);
			temp->location = clause->location;
			/* Commute it --- note this modifies the temp node in-place. */
			CommuteOpExpr(temp);
			t_list = lappend(t_list, temp);
			restrictinfo->outer_is_left = false;
		}
		else
		{
			Assert(bms_is_subset(restrictinfo->left_relids, outerrelids));
			t_list = lappend(t_list, clause);
			restrictinfo->outer_is_left = true;
		}
	}
	return t_list;
}

/*
 * order_qual_clauses
 *		Given a list of qual clauses that will all be evaluated at the same
 *		plan node, sort the list into the order we want to check the quals
 *		in at runtime.
 *
 * When security barrier quals are used in the query, we may have quals with
 * different security levels in the list.  Quals of lower security_level
 * must go before quals of higher security_level, except that we can grant
 * exceptions to move up quals that are leakproof.  When security level
 * doesn't force the decision, we prefer to order clauses by estimated
 * execution cost, cheapest first.
 *
 * Ideally the order should be driven by a combination of execution cost and
 * selectivity, but it's not immediately clear how to account for both,
 * and given the uncertainty of the estimates the reliability of the decisions
 * would be doubtful anyway.  So we just order by security level then
 * estimated per-tuple cost, being careful not to change the order when
 * (as is often the case) the estimates are identical.
 *
 * Although this will work on either bare clauses or RestrictInfos, it's
 * much faster to apply it to RestrictInfos, since it can re-use cost
 * information that is cached in RestrictInfos.  XXX in the bare-clause
 * case, we are also not able to apply security considerations.  That is
 * all right for the moment, because the bare-clause case doesn't occur
 * anywhere that barrier quals could be present, but it would be better to
 * get rid of it.
 *
 * Note: some callers pass lists that contain entries that will later be
 * removed; this is the easiest way to let this routine see RestrictInfos
 * instead of bare clauses.  This is another reason why trying to consider
 * selectivity in the ordering would likely do the wrong thing.
 */
static List *
order_qual_clauses(PlannerInfo *root, List *clauses)
{
	typedef struct
	{
		Node	   *clause;
		Cost		cost;
		Index		security_level;
	} QualItem;
	int			nitems = list_length(clauses);
	QualItem   *items;
	ListCell   *lc;
	int			i;
	List	   *result;

	/* No need to work hard for 0 or 1 clause */
	if (nitems <= 1)
		return clauses;

	/*
	 * Collect the items and costs into an array.  This is to avoid repeated
	 * cost_qual_eval work if the inputs aren't RestrictInfos.
	 */
	items = (QualItem *) palloc(nitems * sizeof(QualItem));
	i = 0;
	foreach(lc, clauses)
	{
		Node	   *clause = (Node *) lfirst(lc);
		QualCost	qcost;

		cost_qual_eval_node(&qcost, clause, root);
		items[i].clause = clause;
		items[i].cost = qcost.per_tuple;
		if (IsA(clause, RestrictInfo))
		{
			RestrictInfo *rinfo = (RestrictInfo *) clause;

			/*
			 * If a clause is leakproof, it doesn't have to be constrained by
			 * its nominal security level.  If it's also reasonably cheap
			 * (here defined as 10X cpu_operator_cost), pretend it has
			 * security_level 0, which will allow it to go in front of
			 * more-expensive quals of lower security levels.  Of course, that
			 * will also force it to go in front of cheaper quals of its own
			 * security level, which is not so great, but we can alleviate
			 * that risk by applying the cost limit cutoff.
			 */
			if (rinfo->leakproof && items[i].cost < 10 * cpu_operator_cost)
				items[i].security_level = 0;
			else
				items[i].security_level = rinfo->security_level;
		}
		else
			items[i].security_level = 0;
		i++;
	}

	/*
	 * Sort.  We don't use qsort() because it's not guaranteed stable for
	 * equal keys.  The expected number of entries is small enough that a
	 * simple insertion sort should be good enough.
	 */
	for (i = 1; i < nitems; i++)
	{
		QualItem	newitem = items[i];
		int			j;

		/* insert newitem into the already-sorted subarray */
		for (j = i; j > 0; j--)
		{
			QualItem   *olditem = &items[j - 1];

			if (newitem.security_level > olditem->security_level ||
				(newitem.security_level == olditem->security_level &&
				 newitem.cost >= olditem->cost))
				break;
			items[j] = *olditem;
		}
		items[j] = newitem;
	}

	/* Convert back to a list */
	result = NIL;
	for (i = 0; i < nitems; i++)
		result = lappend(result, items[i].clause);

	return result;
}

/*
 * Copy cost and size info from a Path node to the Plan node created from it.
 * The executor usually won't use this info, but it's needed by EXPLAIN.
 * Also copy the parallel-related flags, which the executor *will* use.
 */
void
copy_generic_path_info(Plan *dest, Path *src)
{
	dest->startup_cost = src->startup_cost;
	dest->total_cost = src->total_cost;
	dest->plan_rows = src->rows;
	dest->plan_width = src->pathtarget->width;
	dest->parallel_aware = src->parallel_aware;
	dest->parallel_safe = src->parallel_safe;
#ifdef __OPENTENBASE_C__
	dest->parallel_num = src->parallel_workers;
#endif
}

/*
 * Copy cost and size info from a lower plan node to an inserted node.
 * (Most callers alter the info after copying it.)
 */
static void
copy_plan_costsize(Plan *dest, Plan *src)
{
	dest->startup_cost = src->startup_cost;
	dest->total_cost = src->total_cost;
	dest->plan_rows = src->plan_rows;
	dest->plan_width = src->plan_width;
	/* Assume the inserted node is not parallel-aware. */
	dest->parallel_aware = false;
	/* Assume the inserted node is parallel-safe, if child plan is. */
	dest->parallel_safe = src->parallel_safe;
}

/*
 * Some places in this file build Sort nodes that don't have a directly
 * corresponding Path node.  The cost of the sort is, or should have been,
 * included in the cost of the Path node we're working from, but since it's
 * not split out, we have to re-figure it using cost_sort().  This is just
 * to label the Sort node nicely for EXPLAIN.
 *
 * limit_tuples is as for cost_sort (in particular, pass -1 if no limit)
 */
static void
label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples)
{
	Plan	   *lefttree = plan->plan.lefttree;
	Path		sort_path;		/* dummy for result of cost_sort */

	cost_sort(&sort_path, root, NIL,
			  lefttree->total_cost,
			  lefttree->plan_rows,
			  lefttree->plan_width,
			  0.0,
			  work_mem,
			  limit_tuples);
	plan->plan.startup_cost = sort_path.startup_cost;
	plan->plan.total_cost = sort_path.total_cost;
	plan->plan.plan_rows = lefttree->plan_rows;
	plan->plan.plan_width = lefttree->plan_width;
	plan->plan.parallel_aware = false;
	plan->plan.parallel_safe = lefttree->parallel_safe;
}

/*
 * bitmap_subplan_mark_shared
 *	 Set isshared flag in bitmap subplan so that it will be created in
 *	 shared memory.
 */
static void
bitmap_subplan_mark_shared(Plan *plan)
{
	if (IsA(plan, BitmapAnd))
		bitmap_subplan_mark_shared(
								   linitial(((BitmapAnd *) plan)->bitmapplans));
	else if (IsA(plan, BitmapOr))
	{
		((BitmapOr *) plan)->isshared = true;
		bitmap_subplan_mark_shared(
								   linitial(((BitmapOr *) plan)->bitmapplans));
	}
	else if (IsA(plan, BitmapIndexScan))
		((BitmapIndexScan *) plan)->isshared = true;
	else
		elog(ERROR, "unrecognized node type: %d", nodeTag(plan));
}

/*
 * flatten_partitioned_rels
 *		Convert List of Lists into a single List with all elements from the
 *		sub-lists.
 */
static List *
flatten_partitioned_rels(List *partitioned_rels)
{
	List	   *newlist = NIL;
	ListCell   *lc;

	foreach(lc, partitioned_rels)
	{
		List	   *sublist = lfirst(lc);

		newlist = list_concat(newlist, list_copy(sublist));
	}

	return newlist;
}

/*****************************************************************************
 *
 *	PLAN NODE BUILDING ROUTINES
 *
 * In general, these functions are not passed the original Path and therefore
 * leave it to the caller to fill in the cost/width fields from the Path,
 * typically by calling copy_generic_path_info().  This convention is
 * somewhat historical, but it does support a few places above where we build
 * a plan node without having an exactly corresponding Path node.  Under no
 * circumstances should one of these functions do its own cost calculations,
 * as that would be redundant with calculations done while building Paths.
 *
 *****************************************************************************/

static SeqScan *
make_seqscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid)
{
	SeqScan    *node = makeNode(SeqScan);
	Plan	   *plan = &node->plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scanrelid = scanrelid;

	return node;
}

static SampleScan *
make_samplescan(List *qptlist,
				List *qpqual,
				Index scanrelid,
				TableSampleClause *tsc)
{
	SampleScan *node = makeNode(SampleScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->tablesample = tsc;

	return node;
}

static IndexScan *
make_indexscan(List *qptlist,
			   List *qpqual,
			   Index scanrelid,
			   Oid indexid,
			   List *indexqual,
			   List *indexqualorig,
			   List *indexorderby,
			   List *indexorderbyorig,
			   List *indexorderbyops,
			   ScanDirection indexscandir)
{
	IndexScan  *node = makeNode(IndexScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;
	node->indexorderby = indexorderby;
	node->indexorderbyorig = indexorderbyorig;
	node->indexorderbyops = indexorderbyops;
	node->indexorderdir = indexscandir;

	return node;
}

static IndexOnlyScan *
make_indexonlyscan(List *qptlist,
				   List *qpqual,
				   Index scanrelid,
				   Oid indexid,
				   List *indexqual,
				   List *indexorderby,
				   List *indextlist,
				   ScanDirection indexscandir)
{
	IndexOnlyScan *node = makeNode(IndexOnlyScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexorderby = indexorderby;
	node->indextlist = indextlist;
	node->indexorderdir = indexscandir;

	return node;
}

static BitmapIndexScan *
make_bitmap_indexscan(Index scanrelid,
					  Oid indexid,
					  List *indexqual,
					  List *indexqualorig)
{
	BitmapIndexScan *node = makeNode(BitmapIndexScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = NIL;		/* not used */
	plan->qual = NIL;			/* not used */
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->indexid = indexid;
	node->indexqual = indexqual;
	node->indexqualorig = indexqualorig;

	return node;
}

static BitmapHeapScan *
make_bitmap_heapscan(List *qptlist,
					 List *qpqual,
					 Plan *lefttree,
					 List *bitmapqualorig,
					 Index scanrelid)
{
	BitmapHeapScan *node = makeNode(BitmapHeapScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->bitmapqualorig = bitmapqualorig;

	return node;
}

/*
 * Make an already generated tidscan node exec remotely.
 * 
 * This is only used for global index now, the subplan is an index scan
 * on the index of global index table itself.
 */
static void
make_tidscan_remote(TidScan *plan, Plan *subplan, Index baserelid,
					RangeTblEntry *rte, List *indexqualorig, bool for_dml)
{
	TargetEntry *tle;
	ListCell	*lc;
	List	*varlist = NULL;
	Var	 *var;
	int		attrno = 0, numattrs;
	List	*quals = NIL;
	Relation	relation;
	List	*remote_quals = copyObject(indexqualorig);
	
	/* flag this is a remote process */
	plan->remote = true;
	if (for_dml)
	{
		relation = heap_open(rte->relid, NoLock);
		numattrs = RelationGetNumberOfAttributes(relation);
		for (attrno = 1; attrno <= numattrs; attrno++)
		{
			Form_pg_attribute att_tup = &relation->rd_att->attrs[attrno - 1];
			
			if (att_tup->attisdropped)
			{
				var = (Var *) makeNullConst(TEXTOID, -1, InvalidOid);
			}
			else
			{
				var = makeVar(baserelid,
							  attrno,
							  att_tup->atttypid,
							  att_tup->atttypmod,
							  att_tup->attcollation,
							  0);
			}
			tle = makeTargetEntry((Expr *) var,
										attrno,
										NULL,
										false);
			plan->remote_targets = lappend(plan->remote_targets, tle);
			if (for_dml && var->varattno != SelfItemPointerAttributeNumber)
			{
				plan->remote_targets_withoutctid = lappend(plan->remote_targets_withoutctid, tle);
			}
		}
			
		heap_close(relation, NoLock);
	}
	/*
	 * Fetch all vars and only vars into remote tidscan plan.
	 * We will qual it then project it locally after tuple fetched.
	 * Hence, we add indexcond var as target for local EPQ recheck.
	 */
	varlist = pull_var_clause((Node *) plan->scan.plan.qual, 0);
	varlist = list_concat(varlist, pull_var_clause((Node *) indexqualorig, 0));
	varlist = list_concat(varlist, pull_var_clause((Node *) plan->scan.plan.targetlist, 0));
	foreach(lc, varlist)
	{
		var = lfirst_node(Var, lc);			
		if (!tlist_member((Expr *) var, plan->remote_targets))
		{
			tle = makeTargetEntry((Expr *) var,
								  list_length(plan->remote_targets) + 1,
								  get_rte_attribute_name(rte, var->varattno),
								  false);
			
			if (for_dml && var->varattno != SelfItemPointerAttributeNumber)
			{
				plan->remote_targets_withoutctid = lappend(plan->remote_targets_withoutctid, tle);
			}
			plan->remote_targets = lappend(plan->remote_targets, tle);
		}
	}
	
	/*
	 * If quals of tidscan does not contain params, we can push them down to
	 * remote fetch plan. Also, we must keep the quals in local for EPQ.
	 */
	if (!contains_params((Node *) plan->scan.plan.qual))
	{
		/* concat with original index cond */
		remote_quals = list_concat(remote_quals, plan->scan.plan.qual);
		/* assign quals for pushing down */
		quals = plan->scan.plan.qual;
		/* reset quals of plan itself */
		plan->scan.plan.qual = NIL;
	}
	
	if (plan->remote_targets == NIL)
	{
		/* should not be here, but still, we assign whole row as an entry */
		var = makeWholeRowVar(rte, baserelid, 0, false);
		tle = makeTargetEntry((Expr *) var, 0, pstrdup("whole-row"), false);
		plan->remote_targets = list_make1(tle);
	}

	plan->remote_qual = quals;

	/* construct a remote sql, execute it in ExecRemoteTidFetch */
	plan->remote_sql = pgxc_build_remote_tidscan_statement(rte, 
					plan->remote_targets_withoutctid ? plan->remote_targets_withoutctid : plan->remote_targets, 
					quals, for_dml);
	
	/* set remote qual of indexscan */
	plan->remote_indexqual = remote_quals;
	
	/* set our left tree, for global index, it's an index scan */
	outerPlan(plan) = subplan;
}

static TidScan *
make_tidscan(List *qptlist,
			 List *qpqual,
			 Index scanrelid,
			 List *tidquals)
{
	TidScan    *node = makeNode(TidScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->tidquals = tidquals;

	return node;
}

static SubqueryScan *
make_subqueryscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  Plan *subplan)
{
	SubqueryScan *node = makeNode(SubqueryScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->subplan = subplan;

	return node;
}

static FunctionScan *
make_functionscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  List *functions,
				  bool funcordinality)
{
	FunctionScan *node = makeNode(FunctionScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->functions = functions;
	node->funcordinality = funcordinality;

	return node;
}

static TableFuncScan *
make_tablefuncscan(List *qptlist,
				   List *qpqual,
				   Index scanrelid,
				   TableFunc *tablefunc)
{
	TableFuncScan *node = makeNode(TableFuncScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->tablefunc = tablefunc;

	return node;
}

static ValuesScan *
make_valuesscan(List *qptlist,
				List *qpqual,
				Index scanrelid,
				List *values_lists)
{
	ValuesScan *node = makeNode(ValuesScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->values_lists = values_lists;

	return node;
}

static CteScan *
make_ctescan(List *qptlist,
			 List *qpqual,
			 Index scanrelid,
			 int ctePlanId,
			 int cteParam)
{
	CteScan    *node = makeNode(CteScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->ctePlanId = ctePlanId;
	node->cteParam = cteParam;

	return node;
}

static NamedTuplestoreScan *
make_namedtuplestorescan(List *qptlist,
						 List *qpqual,
						 Index scanrelid,
						 char *enrname)
{
	NamedTuplestoreScan *node = makeNode(NamedTuplestoreScan);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->enrname = enrname;

	return node;
}

static WorkTableScan *
make_worktablescan(List *qptlist,
				   List *qpqual,
				   Index scanrelid,
				   int wtParam)
{
	WorkTableScan *node = makeNode(WorkTableScan);
	Plan	   *plan = &node->scan.plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->wtParam = wtParam;

	return node;
}

ForeignScan *
make_foreignscan(List *qptlist,
				 List *qpqual,
				 Index scanrelid,
				 List *fdw_exprs,
				 List *fdw_private,
				 List *fdw_scan_tlist,
				 List *fdw_recheck_quals,
				 Plan *outer_plan)
{
	ForeignScan *node = makeNode(ForeignScan);

	Plan	   *plan = &node->scan.plan;

	/* cost will be filled in by create_foreignscan_plan */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = outer_plan;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->operation = CMD_SELECT;
	/* fs_server will be filled in by create_foreignscan_plan */
	node->fs_server = InvalidOid;
	node->fdw_exprs = fdw_exprs;
	node->fdw_private = fdw_private;
	node->fdw_scan_tlist = fdw_scan_tlist;
	node->fdw_recheck_quals = fdw_recheck_quals;
	/* fs_relids will be filled in by create_foreignscan_plan */
	node->fs_relids = NULL;
	/* fsSystemCol will be filled in by create_foreignscan_plan */
	node->fsSystemCol = false;

	return node;
}

static Append *
make_append(List *appendplans, int first_partial_plan,
			List *tlist, List *partitioned_rels, PartitionPruneInfo *partpruneinfo)
{
	Append	   *node = makeNode(Append);
	Plan	   *plan = &node->plan;

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->appendplans = appendplans;
	node->first_partial_plan = first_partial_plan;
	node->partitioned_rels = flatten_partitioned_rels(partitioned_rels);
	node->part_prune_info = partpruneinfo;

	return node;
}

static RecursiveUnion *
make_recursive_union(List *tlist,
					 Plan *lefttree,
					 Plan *righttree,
					 int wtParam,
					 List *distinctList,
					 long numGroups)
{
	RecursiveUnion *node = makeNode(RecursiveUnion);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->wtParam = wtParam;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	node->numCols = numCols;
	if (numCols > 0)
	{
		int			keyno = 0;
		AttrNumber *dupColIdx;
		Oid		   *dupOperators;
		ListCell   *slitem;

		dupColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
		dupOperators = (Oid *) palloc(sizeof(Oid) * numCols);

		foreach(slitem, distinctList)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl,
													   plan->targetlist);

			dupColIdx[keyno] = tle->resno;
			dupOperators[keyno] = sortcl->eqop;
			Assert(OidIsValid(dupOperators[keyno]));
			keyno++;
		}
		node->dupColIdx = dupColIdx;
		node->dupOperators = dupOperators;
	}
	node->numGroups = numGroups;

	return node;
}

static BitmapAnd *
make_bitmap_and(List *bitmapplans)
{
	BitmapAnd  *node = makeNode(BitmapAnd);
	Plan	   *plan = &node->plan;

	plan->targetlist = NIL;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->bitmapplans = bitmapplans;

	return node;
}

static BitmapOr *
make_bitmap_or(List *bitmapplans)
{
	BitmapOr   *node = makeNode(BitmapOr);
	Plan	   *plan = &node->plan;

	plan->targetlist = NIL;
	plan->qual = NIL;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->bitmapplans = bitmapplans;

	return node;
}

static NestLoop *
make_nestloop(List *tlist,
			  List *joinclauses,
			  List *otherclauses,
			  List *nestParams,
			  Plan *lefttree,
			  Plan *righttree,
			  JoinType jointype,
			  bool inner_unique)
{
	NestLoop   *node = makeNode(NestLoop);
	Plan	   *plan = &node->join.plan;

	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->join.jointype = jointype;
	node->join.inner_unique = inner_unique;
	node->join.joinqual = joinclauses;
	node->nestParams = nestParams;

	return node;
}

static HashJoin *
make_hashjoin(List *tlist,
			  List *joinclauses,
			  List *otherclauses,
			  List *hashclauses,
			  List *hashoperators,
			  List *hashcollations,
			  List *hashkeys,
			  Plan *lefttree,
			  Plan *righttree,
			  JoinType jointype,
			  bool inner_unique)
{
	HashJoin   *node = makeNode(HashJoin);
	Plan	   *plan = &node->join.plan;

	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->hashclauses = hashclauses;
	node->hashoperators = hashoperators;
	node->hashcollations = hashcollations;
	node->hashkeys = hashkeys;
	node->join.jointype = jointype;
	node->join.inner_unique = inner_unique;
	node->join.joinqual = joinclauses;

	return node;
}

static Hash *
make_hash(Plan *lefttree,
		  List *hashkeys,
		  Oid skewTable,
		  AttrNumber skewColumn,
		  bool skewInherit)
{
	Hash	   *node = makeNode(Hash);
	Plan	   *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->hashkeys = hashkeys;
	node->skewTable = skewTable;
	node->skewColumn = skewColumn;
	node->skewInherit = skewInherit;

	return node;
}

static MergeJoin *
make_mergejoin(List *tlist,
			   List *joinclauses,
			   List *otherclauses,
			   List *mergeclauses,
			   Oid *mergefamilies,
			   Oid *mergecollations,
			   int *mergestrategies,
			   bool *mergenullsfirst,
			   Plan *lefttree,
			   Plan *righttree,
			   JoinType jointype,
			   bool inner_unique,
			   bool skip_mark_restore,
			   bool is_not_full)
{
	MergeJoin  *node = makeNode(MergeJoin);
	Plan	   *plan = &node->join.plan;

	plan->targetlist = tlist;
	plan->qual = otherclauses;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	node->skip_mark_restore = skip_mark_restore;
	node->mergeclauses = mergeclauses;
	node->mergeFamilies = mergefamilies;
	node->mergeCollations = mergecollations;
	node->mergeStrategies = mergestrategies;
	node->mergeNullsFirst = mergenullsfirst;
	node->join.jointype = jointype;
	node->join.inner_unique = inner_unique;
	node->join.joinqual = joinclauses;
	node->is_not_full = is_not_full;

	return node;
}

/*
 * make_sort --- basic routine to build a Sort plan node
 *
 * Caller must have built the sortColIdx, sortOperators, collations, and
 * nullsFirst arrays already.
 */
static Sort *
make_sort(Plan *lefttree, int numCols,
		  AttrNumber *sortColIdx, Oid *sortOperators,
		  Oid *collations, bool *nullsFirst)
{
	Sort	   *node = makeNode(Sort);
	Plan	   *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->numCols = numCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->collations = collations;
	node->nullsFirst = nullsFirst;

	return node;
}

/*
 * prepare_sort_from_pathkeys
 *	  Prepare to sort according to given pathkeys
 *
 * This is used to set up for Sort, MergeAppend, and Gather Merge nodes.  It
 * calculates the executor's representation of the sort key information, and
 * adjusts the plan targetlist if needed to add resjunk sort columns.
 *
 * Input parameters:
 *	  'lefttree' is the plan node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'relids' identifies the child relation being sorted, if any
 *	  'reqColIdx' is NULL or an array of required sort key column numbers
 *	  'adjust_tlist_in_place' is TRUE if lefttree must be modified in-place
 *
 * We must convert the pathkey information into arrays of sort key column
 * numbers, sort operator OIDs, collation OIDs, and nulls-first flags,
 * which is the representation the executor wants.  These are returned into
 * the output parameters *p_numsortkeys etc.
 *
 * When looking for matches to an EquivalenceClass's members, we will only
 * consider child EC members if they belong to given 'relids'.  This protects
 * against possible incorrect matches to child expressions that contain no
 * Vars.
 *
 * If reqColIdx isn't NULL then it contains sort key column numbers that
 * we should match.  This is used when making child plans for a MergeAppend;
 * it's an error if we can't match the columns.
 *
 * If the pathkeys include expressions that aren't simple Vars, we will
 * usually need to add resjunk items to the input plan's targetlist to
 * compute these expressions, since a Sort or MergeAppend node itself won't
 * do any such calculations.  If the input plan type isn't one that can do
 * projections, this means adding a Result node just to do the projection.
 * However, the caller can pass adjust_tlist_in_place = TRUE to force the
 * lefttree tlist to be modified in-place regardless of whether the node type
 * can project --- we use this for fixing the tlist of MergeAppend itself.
 *
 * Returns the node which is to be the input to the Sort (either lefttree,
 * or a Result stacked atop lefttree).
 */
static Plan *
prepare_sort_from_pathkeys(Plan *lefttree, List *pathkeys,
						   Relids relids,
						   const AttrNumber *reqColIdx,
						   bool adjust_tlist_in_place,
						   int *p_numsortkeys,
						   AttrNumber **p_sortColIdx,
						   Oid **p_sortOperators,
						   Oid **p_collations,
						   bool **p_nullsFirst)
{
	List	   *tlist = lefttree->targetlist;
	ListCell   *i;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/*
	 * We will need at most list_length(pathkeys) sort columns; possibly less
	 */
	numsortkeys = list_length(pathkeys);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;

	foreach(i, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *em;
		TargetEntry *tle = NULL;
		Oid			pk_datatype = InvalidOid;
		Oid			sortop;
		ListCell   *j;

		if (ec->ec_has_volatile)
		{
			/*
			 * If the pathkey's EquivalenceClass is volatile, then it must
			 * have come from an ORDER BY clause, and we have to match it to
			 * that same targetlist entry.
			 */
			if (ec->ec_sortref == 0)	/* can't happen */
				elog(ERROR, "volatile EquivalenceClass has no sortref");
			tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
			Assert(tle);
			Assert(list_length(ec->ec_members) == 1);
			pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
		}
		else if (reqColIdx != NULL)
		{
			/*
			 * If we are given a sort column number to match, only consider
			 * the single TLE at that position.  It's possible that there is
			 * no such TLE, in which case fall through and generate a resjunk
			 * targetentry (we assume this must have happened in the parent
			 * plan as well).  If there is a TLE but it doesn't match the
			 * pathkey's EC, we do the same, which is probably the wrong thing
			 * but we'll leave it to caller to complain about the mismatch.
			 */
			tle = get_tle_by_resno(tlist, reqColIdx[numsortkeys]);
			if (tle)
			{
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr at right place in tlist */
					pk_datatype = em->em_datatype;
				}
				else
					tle = NULL;
			}
		}
		else
		{
			/*
			 * Otherwise, we can sort by any non-constant expression listed in
			 * the pathkey's EquivalenceClass.  For now, we take the first
			 * tlist item found in the EC. If there's no match, we'll generate
			 * a resjunk entry using the first EC member that is an expression
			 * in the input's vars.  (The non-const restriction only matters
			 * if the EC is below_outer_join; but if it isn't, it won't
			 * contain consts anyway, else we'd have discarded the pathkey as
			 * redundant.)
			 *
			 * XXX if we have a choice, is there any way of figuring out which
			 * might be cheapest to execute?  (For example, int4lt is likely
			 * much cheaper to execute than numericlt, but both might appear
			 * in the same equivalence class...)  Not clear that we ever will
			 * have an interesting choice in practice, so it may not matter.
			 */
			foreach(j, tlist)
			{
				tle = (TargetEntry *) lfirst(j);
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr already in tlist */
					pk_datatype = em->em_datatype;
					break;
				}
				tle = NULL;
			}
		}

		if (!tle)
		{
			/*
			 * No matching tlist item; look for a computable expression. Note
			 * that we treat Aggrefs as if they were variables; this is
			 * necessary when attempting to sort the output from an Agg node
			 * for use in a WindowFunc (since grouping_planner will have
			 * treated the Aggrefs as variables, too).  Likewise, if we find a
			 * WindowFunc in a sort expression, treat it as a variable.
			 */
			Expr	   *sortexpr = NULL;

			foreach(j, ec->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
				List	   *exprvars;
				ListCell   *k;

				/*
				 * We shouldn't be trying to sort by an equivalence class that
				 * contains a constant, so no need to consider such cases any
				 * further.
				 */
				if (em->em_is_const)
					continue;

				/*
				 * Ignore child members unless they belong to the rel being
				 * sorted.
				 */
				if (em->em_is_child &&
					!bms_is_subset(em->em_relids, relids))
					continue;

				sortexpr = em->em_expr;
				exprvars = pull_var_clause((Node *) sortexpr,
										   PVC_INCLUDE_AGGREGATES |
										   PVC_INCLUDE_WINDOWFUNCS |
										   PVC_INCLUDE_PLACEHOLDERS);
				foreach(k, exprvars)
				{
					if (!tlist_member_ignore_relabel(lfirst(k), tlist))
						break;
				}
				list_free(exprvars);
				if (!k)
				{
					pk_datatype = em->em_datatype;
					break;		/* found usable expression */
				}
			}
			if (!j)
				elog(ERROR, "could not find pathkey item to sort");

			/*
			 * Do we need to insert a Result node?
			 */
			if (!adjust_tlist_in_place &&
				!is_projection_capable_plan(lefttree))
			{
				/* copy needed so we don't modify input's tlist below */
				tlist = copyObject(tlist);
				lefttree = inject_projection_plan(lefttree, tlist,
												  lefttree->parallel_safe);
			}

			/* Don't bother testing is_projection_capable_plan again */
			adjust_tlist_in_place = true;

			/*
			 * Add resjunk entry to input's tlist
			 */
			tle = makeTargetEntry(sortexpr,
								  list_length(tlist) + 1,
								  NULL,
								  true);
			tlist = lappend(tlist, tle);
			lefttree->targetlist = tlist;	/* just in case NIL before */
#ifdef XCP
			/*
			 * RemoteSubplan is conditionally projection capable - it is
			 * pushing projection to the data nodes
			 */
			if (IsA(lefttree, RemoteSubplan))
				lefttree->lefttree->targetlist = tlist;
#endif
		}

		/*
		 * Look up the correct sort operator from the PathKey's slightly
		 * abstracted representation.
		 */
		sortop = get_opfamily_member(pathkey->pk_opfamily,
									 pk_datatype,
									 pk_datatype,
									 pathkey->pk_strategy);
		if (!OidIsValid(sortop))	/* should not happen */
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 pathkey->pk_strategy, pk_datatype, pk_datatype,
				 pathkey->pk_opfamily);

		/* Add the column to the sort arrays */
		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = sortop;
		collations[numsortkeys] = ec->ec_collation;
		nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
		numsortkeys++;
	}

	/* Return results */
	*p_numsortkeys = numsortkeys;
	*p_sortColIdx = sortColIdx;
	*p_sortOperators = sortOperators;
	*p_collations = collations;
	*p_nullsFirst = nullsFirst;

	return lefttree;
}

/*
 * find_ec_member_for_tle
 *		Locate an EquivalenceClass member matching the given TLE, if any
 *
 * Child EC members are ignored unless they belong to given 'relids'.
 */
static EquivalenceMember *
find_ec_member_for_tle(EquivalenceClass *ec,
					   TargetEntry *tle,
					   Relids relids)
{
	Expr	   *tlexpr;
	ListCell   *lc;

	/* We ignore binary-compatible relabeling on both ends */
	tlexpr = tle->expr;
	while (tlexpr && IsA(tlexpr, RelabelType))
		tlexpr = ((RelabelType *) tlexpr)->arg;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		Expr	   *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they belong to the rel being sorted.
		 */
		if (em->em_is_child &&
			!bms_is_subset(em->em_relids, relids))
			continue;

		/* Match if same expression (after stripping relabel) */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		if (equal(emexpr, tlexpr))
			return em;
	}

	return NULL;
}

/*
 * make_sort_from_pathkeys
 *	  Create sort plan to sort according to given pathkeys
 *
 *	  'lefttree' is the node which yields input tuples
 *	  'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'relids' is the set of relations required by prepare_sort_from_pathkeys()
 */
static Sort *
make_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids)
{
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/* Compute sort column info, and adjust lefttree as needed */
	lefttree = prepare_sort_from_pathkeys(lefttree, pathkeys,
										  relids,
										  NULL,
										  false,
										  &numsortkeys,
										  &sortColIdx,
										  &sortOperators,
										  &collations,
										  &nullsFirst);

	/* Now build the Sort node */
	return make_sort(lefttree, numsortkeys,
					 sortColIdx, sortOperators,
					 collations, nullsFirst);
}

/*
 * make_sort_from_sortclauses
 *	  Create sort plan to sort according to given sortclauses
 *
 *	  'sortcls' is a list of SortGroupClauses
 *	  'lefttree' is the node which yields input tuples
 */
Sort *
make_sort_from_sortclauses(List *sortcls, Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/* Convert list-ish representation to arrays wanted by executor */
	numsortkeys = list_length(sortcls);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;
	foreach(l, sortcls)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, sub_tlist);

		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = sortcl->sortop;
		collations[numsortkeys] = exprCollation((Node *) tle->expr);
		nullsFirst[numsortkeys] = sortcl->nulls_first;
		numsortkeys++;
	}

	return make_sort(lefttree, numsortkeys,
					 sortColIdx, sortOperators,
					 collations, nullsFirst);
}

/*
 * make_sort_from_groupcols
 *	  Create sort plan to sort based on grouping columns
 *
 * 'groupcls' is the list of SortGroupClauses
 * 'grpColIdx' gives the column numbers to use
 *
 * This might look like it could be merged with make_sort_from_sortclauses,
 * but presently we *must* use the grpColIdx[] array to locate sort columns,
 * because the child plan's tlist is not marked with ressortgroupref info
 * appropriate to the grouping node.  So, only the sort ordering info
 * is used from the SortGroupClause entries.
 */
static Sort *
make_sort_from_groupcols(List *groupcls,
						 AttrNumber *grpColIdx,
						 Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/* Convert list-ish representation to arrays wanted by executor */
	numsortkeys = list_length(groupcls);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;
	foreach(l, groupcls)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_tle_by_resno(sub_tlist, grpColIdx[numsortkeys]);

		if (!tle)
			elog(ERROR, "could not retrieve tle for sort-from-groupcols");

		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = grpcl->sortop;
		collations[numsortkeys] = exprCollation((Node *) tle->expr);
		nullsFirst[numsortkeys] = grpcl->nulls_first;
		numsortkeys++;
	}

	return make_sort(lefttree, numsortkeys,
					 sortColIdx, sortOperators,
					 collations, nullsFirst);
}

static Material *
make_material(Plan *lefttree)
{
	Material   *node = makeNode(Material);
	Plan	   *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	return node;
}

/*
 * materialize_finished_plan: stick a Material node atop a completed plan
 *
 * There are a couple of places where we want to attach a Material node
 * after completion of create_plan(), without any MaterialPath path.
 * Those places should probably be refactored someday to do this on the
 * Path representation, but it's not worth the trouble yet.
 */
Plan *
materialize_finished_plan(Plan *subplan)
{
	Plan	   *matplan;
	Path		matpath;		/* dummy for result of cost_material */

	matplan = (Plan *) make_material(subplan);

	/*
	 * XXX horrid kluge: if there are any initPlans attached to the subplan,
	 * move them up to the Material node, which is now effectively the top
	 * plan node in its query level.  This prevents failure in
	 * SS_finalize_plan(), which see for comments.  We don't bother adjusting
	 * the subplan's cost estimate for this.
	 */
	matplan->initPlan = subplan->initPlan;
	subplan->initPlan = NIL;

	/* Set cost data */
	cost_material(&matpath,
				  subplan->startup_cost,
				  subplan->total_cost,
				  subplan->plan_rows,
				  subplan->plan_width);
	matplan->startup_cost = matpath.startup_cost;
	matplan->total_cost = matpath.total_cost;
	matplan->plan_rows = subplan->plan_rows;
	matplan->plan_width = subplan->plan_width;
	matplan->parallel_aware = false;
	matplan->parallel_safe = subplan->parallel_safe;

	return matplan;
}

Agg *
make_agg(List *tlist, List *qual,
		 AggStrategy aggstrategy, AggSplit aggsplit,
		 int numGroupCols, AttrNumber *grpColIdx, Oid *grpOperators,
		 List *groupingSets, List *chain, double dNumGroups,
		 Size transitionSpace, Plan *lefttree)
{
	Agg		   *node = makeNode(Agg);
	Plan	   *plan = &node->plan;
	long		numGroups;

	/* Reduce to long, but 'ware overflow! */
	numGroups = (long) Min(dNumGroups, (double) LONG_MAX);

	node->aggstrategy = aggstrategy;
	node->aggsplit = aggsplit;
	node->numCols = numGroupCols;
	node->grpColIdx = grpColIdx;
	node->grpOperators = grpOperators;
	node->numGroups = numGroups;
	node->transitionSpace = transitionSpace;
	node->aggParams = NULL;		/* SS_finalize_plan() will fill this */
	node->groupingSets = groupingSets;
	node->chain = chain;

	plan->qual = qual;
	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	return node;
}

static WindowAgg *
make_windowagg(List *tlist, Index winref,
			   int partNumCols, AttrNumber *partColIdx, Oid *partOperators,
			   int ordNumCols, AttrNumber *ordColIdx, Oid *ordOperators,
			   int frameOptions, Node *startOffset, Node *endOffset,
			   Oid startInRangeFunc, Oid endInRangeFunc,
			   Oid inRangeColl, bool inRangeAsc, bool inRangeNullsFirst,
			   Plan *lefttree)
{
	WindowAgg  *node = makeNode(WindowAgg);
	Plan	   *plan = &node->plan;

	node->winref = winref;
	node->partNumCols = partNumCols;
	node->partColIdx = partColIdx;
	node->partOperators = partOperators;
	node->ordNumCols = ordNumCols;
	node->ordColIdx = ordColIdx;
	node->ordOperators = ordOperators;
	node->frameOptions = frameOptions;
	node->startOffset = startOffset;
	node->endOffset = endOffset;
	node->startInRangeFunc = startInRangeFunc;
	node->endInRangeFunc = endInRangeFunc;
	node->inRangeColl = inRangeColl;
	node->inRangeAsc = inRangeAsc;
	node->inRangeNullsFirst = inRangeNullsFirst;

	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	/* WindowAgg nodes never have a qual clause */
	plan->qual = NIL;

	return node;
}

static Group *
make_group(List *tlist,
		   List *qual,
		   int numGroupCols,
		   AttrNumber *grpColIdx,
		   Oid *grpOperators,
		   Plan *lefttree)
{
	Group	   *node = makeNode(Group);
	Plan	   *plan = &node->plan;

	node->numCols = numGroupCols;
	node->grpColIdx = grpColIdx;
	node->grpOperators = grpOperators;

	plan->qual = qual;
	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	return node;
}

/*
 * distinctList is a list of SortGroupClauses, identifying the targetlist items
 * that should be considered by the Unique filter.  The input path must
 * already be sorted accordingly.
 */
static Unique *
make_unique_from_sortclauses(Plan *lefttree, List *distinctList)
{
	Unique	   *node = makeNode(Unique);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);
	int			keyno = 0;
	AttrNumber *uniqColIdx;
	Oid		   *uniqOperators;
	ListCell   *slitem;
#ifdef XCP
	RemoteSubplan *pushdown;
#endif

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	Assert(numCols > 0);
	uniqColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
	uniqOperators = (Oid *) palloc(sizeof(Oid) * numCols);

	foreach(slitem, distinctList)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		uniqColIdx[keyno] = tle->resno;
		uniqOperators[keyno] = sortcl->eqop;
		Assert(OidIsValid(uniqOperators[keyno]));
		keyno++;
	}

	node->numCols = numCols;
	node->uniqColIdx = uniqColIdx;
	node->uniqOperators = uniqOperators;

#ifdef XCP
	/*
	 * We want to filter out duplicates on nodes to reduce amount of data sent
	 * over network and reduce coordinator load.
	 */
	pushdown = find_push_down_plan(lefttree, true);
	if (pushdown)
	{
		Unique	   *node1 = makeNode(Unique);
		Plan	   *plan1 = &node1->plan;

		copy_plan_costsize(plan1, pushdown->scan.plan.lefttree);
		plan1->targetlist = pushdown->scan.plan.lefttree->targetlist;
		plan1->qual = NIL;
		plan1->lefttree = pushdown->scan.plan.lefttree;
		pushdown->scan.plan.lefttree = plan1;
		plan1->righttree = NULL;

		node1->numCols = numCols;
		node1->uniqColIdx = uniqColIdx;
		node1->uniqOperators = uniqOperators;
	}
#endif

	return node;
}

/*
 * as above, but use pathkeys to identify the sort columns and semantics
 */
static Unique *
make_unique_from_pathkeys(Plan *lefttree, List *pathkeys, int numCols)
{
	Unique	   *node = makeNode(Unique);
	Plan	   *plan = &node->plan;
	int			keyno = 0;
	AttrNumber *uniqColIdx;
	Oid		   *uniqOperators;
	ListCell   *lc;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * Convert pathkeys list into arrays of attr indexes and equality
	 * operators, as wanted by executor.  This has a lot in common with
	 * prepare_sort_from_pathkeys ... maybe unify sometime?
	 */
	Assert(numCols >= 0 && numCols <= list_length(pathkeys));
	uniqColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
	uniqOperators = (Oid *) palloc(sizeof(Oid) * numCols);

	foreach(lc, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *em;
		TargetEntry *tle = NULL;
		Oid			pk_datatype = InvalidOid;
		Oid			eqop;
		ListCell   *j;

		/* Ignore pathkeys beyond the specified number of columns */
		if (keyno >= numCols)
			break;

		if (ec->ec_has_volatile)
		{
			/*
			 * If the pathkey's EquivalenceClass is volatile, then it must
			 * have come from an ORDER BY clause, and we have to match it to
			 * that same targetlist entry.
			 */
			if (ec->ec_sortref == 0)	/* can't happen */
				elog(ERROR, "volatile EquivalenceClass has no sortref");
			tle = get_sortgroupref_tle(ec->ec_sortref, plan->targetlist);
			Assert(tle);
			Assert(list_length(ec->ec_members) == 1);
			pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
		}
		else
		{
			/*
			 * Otherwise, we can use any non-constant expression listed in the
			 * pathkey's EquivalenceClass.  For now, we take the first tlist
			 * item found in the EC.
			 */
			foreach(j, plan->targetlist)
			{
				tle = (TargetEntry *) lfirst(j);
				em = find_ec_member_for_tle(ec, tle, NULL);
				if (em)
				{
					/* found expr already in tlist */
					pk_datatype = em->em_datatype;
					break;
				}
				tle = NULL;
			}
		}

		if (!tle)
			elog(ERROR, "could not find pathkey item to sort");

		/*
		 * Look up the correct equality operator from the PathKey's slightly
		 * abstracted representation.
		 */
		eqop = get_opfamily_member(pathkey->pk_opfamily,
								   pk_datatype,
								   pk_datatype,
								   BTEqualStrategyNumber);
		if (!OidIsValid(eqop))	/* should not happen */
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 BTEqualStrategyNumber, pk_datatype, pk_datatype,
				 pathkey->pk_opfamily);

		uniqColIdx[keyno] = tle->resno;
		uniqOperators[keyno] = eqop;

		keyno++;
	}

	node->numCols = numCols;
	node->uniqColIdx = uniqColIdx;
	node->uniqOperators = uniqOperators;

	return node;
}

static Gather *
make_gather(List *qptlist,
			List *qpqual,
			int nworkers,
			int rescan_param,
			bool single_copy,
			Plan *subplan)
{
	Gather	   *node = makeNode(Gather);
	Plan	   *plan = &node->plan;

	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	node->num_workers = nworkers;
	node->rescan_param = rescan_param;
	node->single_copy = single_copy;
	node->invisible = false;
	node->initParam = NULL;

	return node;
}

/*
 * distinctList is a list of SortGroupClauses, identifying the targetlist
 * items that should be considered by the SetOp filter.  The input path must
 * already be sorted accordingly.
 */
static SetOp *
make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan *lefttree,
		   List *distinctList, AttrNumber flagColIdx, int firstFlag,
		   long numGroups)
{
	SetOp	   *node = makeNode(SetOp);
	Plan	   *plan = &node->plan;
	int			numCols = list_length(distinctList);
	int			keyno = 0;
	AttrNumber *dupColIdx;
	Oid		   *dupOperators;
	ListCell   *slitem;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	/*
	 * convert SortGroupClause list into arrays of attr indexes and equality
	 * operators, as wanted by executor
	 */
	dupColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
	dupOperators = (Oid *) palloc(sizeof(Oid) * numCols);

	foreach(slitem, distinctList)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(slitem);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

		dupColIdx[keyno] = tle->resno;
		dupOperators[keyno] = sortcl->eqop;
		Assert(OidIsValid(dupOperators[keyno]));
		keyno++;
	}

	node->cmd = cmd;
	node->strategy = strategy;
	node->numCols = numCols;
	node->dupColIdx = dupColIdx;
	node->dupOperators = dupOperators;
	node->flagColIdx = flagColIdx;
	node->firstFlag = firstFlag;
	node->numGroups = numGroups;

	return node;
}

/*
 * make_lockrows
 *	  Build a LockRows plan node
 */
static LockRows *
make_lockrows(Plan *lefttree, List *rowMarks, int epqParam)
{
	LockRows   *node = makeNode(LockRows);
	Plan	   *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->rowMarks = rowMarks;
	node->epqParam = epqParam;

	return node;
}

/*
 * make_limit
 *	  Build a Limit plan node
 */
Limit *
make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount,
		   LimitOption limitOption, int uniqNumCols, AttrNumber *uniqColIdx,
		   Oid *uniqOperators, Oid *uniqCollations)
{
	Limit	   *node = makeNode(Limit);
	Plan	   *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;

	node->limitOffset = limitOffset;
	node->limitCount = limitCount;
	node->limitOption = limitOption;
	node->uniqNumCols = uniqNumCols;
	node->uniqColIdx = uniqColIdx;
	node->uniqOperators = uniqOperators;
	node->uniqCollations = uniqCollations;

	return node;
}

static bool
is_simple_rownum_expr(Node *node, void *context /* useless */)
{
	if (node == NULL)
		return false;

	if (IsA(node, RownumExpr))
		return true;
	else if (IsA(node, FuncExpr))
	{
		if (((FuncExpr *) node)->funcformat == COERCE_EXPLICIT_CAST ||
			((FuncExpr *) node)->funcformat == COERCE_IMPLICIT_CAST)
			return expression_tree_walker(node, is_simple_rownum_expr, NULL);
	}
	else if (IsA(node, CoerceViaIO))
	{
		if (((CoerceViaIO *) node)->coerceformat == COERCE_EXPLICIT_CAST ||
			((CoerceViaIO *) node)->coerceformat == COERCE_IMPLICIT_CAST)
			return expression_tree_walker(node, is_simple_rownum_expr, NULL);
	}

	return false;
}

static bool
is_simple_rownum_qual(List *qual)
{
	Node    *left = NULL;
	Node    *right = NULL;
	ListCell    *lc;
	Relids   varnos;

	if (!qual)
		return false;

	foreach(lc, qual)
	{
		varnos = pull_varnos((Node *) lfirst(lc));
		if (!bms_is_empty(varnos))
		{
			bms_free(varnos);
			return false;
		}
		bms_free(varnos);

		if (IsA(lfirst(lc), OpExpr))
		{
			OpExpr *expr = lfirst_node(OpExpr, lc);
			if (list_length(expr->args) != 2)
				return false;

			left = linitial(expr->args);
			right = lsecond(expr->args);

			if (is_simple_rownum_expr(left, NULL) ||
				is_simple_rownum_expr(right, NULL))
			{
				char *opname = get_opname(expr->opno);

				if (strcmp("<",opname) == 0 ||
					strcmp("<=",opname) == 0 ||
					strcmp("=",opname) == 0 ||
					strcmp(">=",opname) == 0 ||
					strcmp(">",opname) == 0 ||
					strcmp("<>",opname) == 0 ||
					strcmp("~~",opname) == 0 ||
					strcmp("!~~",opname) == 0)
					continue; /* next qual */
				else
					return false;
			}
			else
				return false;
		}
		else if (IsA(lfirst(lc), ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *expr = lfirst_node(ScalarArrayOpExpr, lc);
			if (list_length(expr->args) != 2)
				return false;

			left = linitial(expr->args);
			right = lsecond(expr->args);

			if (is_simple_rownum_expr(left, NULL) ||
				is_simple_rownum_expr(right, NULL))
				continue; /* next qual */
			else
				return false;
		}
		else
		{
			return false;
		}
	}

	/* all clear */
	return true;
}

/*
 * make_result
 *	  Build a Result plan node
 */
static Result *
make_result(List *tlist,
			Node *resconstantqual,
			Plan *subplan,
			List *qual)
{
	Result	   *node = makeNode(Result);
	Plan	   *plan = &node->plan;

	plan->targetlist = tlist;
	plan->qual = qual;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	node->resconstantqual = resconstantqual;
	node->fail_return = is_simple_rownum_qual(qual);

	/*
	 * Do not consider pushing down node if this node is made to process any
	 * projection or qualification that contain cn-expr.
	 */
	if (contain_cn_expr((Node *) tlist) || contain_cn_expr((Node *) qual))
		return node;

#ifdef XCP
	if (subplan)
	{
		/*
		 * We do not gain performance when pushing down Result, but Result on
		 * top of RemoteSubplan would not allow to push down other plan nodes
		 */
		RemoteSubplan *pushdown;
		pushdown = find_push_down_plan(subplan, true);
		if (pushdown)
		{
			/*
			 * Avoid pushing down results if the RemoteSubplan performs merge
			 * sort.
			 */
			if (pushdown->sort)
				return node;

			/*
			 * If remote subplan is generating distribution we should keep it
			 * correct. Set valid expression as a distribution key.
			 */
			if (pushdown->ndiskeys > 0)
			{
				int     i = 0;
				
				for (i = 0; i < pushdown->ndiskeys; i++)
				{
					pushdown->diskeys[i] =
						get_distribution_key_result(plan, pushdown,
													pushdown->diskeys[i], tlist);
				}
			}
			/* This will be set as lefttree of the Result plan */
			plan->lefttree = pushdown->scan.plan.lefttree;
			pushdown->scan.plan.lefttree = plan;
			/* Now RemoteSubplan returns different values */
			pushdown->scan.plan.targetlist = tlist;
			subplan->targetlist = tlist;
			return (Result *) subplan;
		}
	}
#endif /* XCP */
	return node;
}

/*
 * make_project_set
 *	  Build a ProjectSet plan node
 */
static ProjectSet *
make_project_set(List *tlist,
				 Plan *subplan)
{
	ProjectSet *node = makeNode(ProjectSet);
	Plan	   *plan = &node->plan;

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;

	return node;
}

/*
 * make_modifytable
 *	  Build a ModifyTable plan node
 */
static ModifyTable *
make_modifytable(PlannerInfo *root, Plan *subplan,
				 CmdType operation, bool canSetTag,
				 Index nominalRelation, List *partitioned_rels,
				 bool partColsUpdated,
				 List *resultRelations,
				 List *updateColnosLists,
				 List *withCheckOptionLists, List *returningLists,
				 List *rowMarks, OnConflictExpr *onconflict,
				 List *mergeActionLists, int epqParam)
{
	ModifyTable *node = makeNode(ModifyTable);
	List	   *fdw_private_list;
	Bitmapset  *direct_modify_plans;
	ListCell   *lc;
	int			i;

	Assert(operation == CMD_UPDATE ?
		   list_length(resultRelations) == list_length(updateColnosLists) :
		   updateColnosLists == NIL);
	Assert(withCheckOptionLists == NIL ||
		   list_length(resultRelations) == list_length(withCheckOptionLists));
	Assert(returningLists == NIL ||
		   list_length(resultRelations) == list_length(returningLists));

	node->plan.lefttree = subplan;
	node->plan.righttree = NULL;
	node->plan.qual = NIL;
	/* setrefs.c will fill in the targetlist, if needed */
	node->plan.targetlist = NIL;

    node->mergeActionLists = mergeActionLists;

	node->operation = operation;
	node->canSetTag = canSetTag;
	node->nominalRelation = nominalRelation;
	node->partitioned_rels = flatten_partitioned_rels(partitioned_rels);
	node->partColsUpdated = partColsUpdated;
	node->resultRelations = resultRelations;
	node->resultRelIndex = -1;	/* will be set correctly in setrefs.c */
	node->rootResultRelIndex = -1;	/* will be set correctly in setrefs.c */
	if (!onconflict)
	{
		node->onConflictAction = ONCONFLICT_NONE;
		node->onConflictSet = NIL;
		node->onConflictWhere = NULL;
		node->arbiterIndexes = NIL;
		node->exclRelRTI = 0;
		node->exclRelTlist = NIL;
	}
	else
	{
		node->onConflictAction = onconflict->action;
		node->onConflictSet = onconflict->onConflictSet;
		node->onConflictWhere = onconflict->onConflictWhere;

		/*
		 * If a set of unique index inference elements was provided (an
		 * INSERT...ON CONFLICT "inference specification"), then infer
		 * appropriate unique indexes (or throw an error if none are
		 * available).
		 */
		node->arbiterIndexes = infer_arbiter_indexes(root);

		node->exclRelRTI = onconflict->exclRelIndex;
		node->exclRelTlist = onconflict->exclRelTlist;
	}
	node->updateColnosLists = updateColnosLists;
	node->withCheckOptionLists = withCheckOptionLists;
	node->returningLists = returningLists;
	node->rowMarks = rowMarks;
	node->epqParam = epqParam;
#ifdef __OPENTENBASE__
	node->update_gindex_key = pgxc_targetlist_has_globalindex_col(root->parse);
	
	foreach(lc, resultRelations)
	{
		RangeTblEntry *rte = rt_fetch(lfirst_int(lc), root->parse->rtable);
		
		/* Bad relation ? */
		if (rte == NULL || rte->rtekind != RTE_RELATION)
			continue;
		node->global_index = relid_has_cross_node_index(rte->relid);
	}

	if (node->global_index && IS_DISTRIBUTED_DATANODE &&
		!g_index_table_part_name && !IsConnFromCoord())
		elog(ERROR, "can not modify a table with global index on DN without planning on CN");
#endif

	/*
	 * For each result relation that is a foreign table, allow the FDW to
	 * construct private plan data, and accumulate it all into a list.
	 */
	fdw_private_list = NIL;
	direct_modify_plans = NULL;
	i = 0;
	foreach(lc, resultRelations)
	{
		Index		rti = lfirst_int(lc);
		FdwRoutine *fdwroutine;
		List	   *fdw_private;
		bool		direct_modify;

		/*
		 * If possible, we want to get the FdwRoutine from our RelOptInfo for
		 * the table.  But sometimes we don't have a RelOptInfo and must get
		 * it the hard way.  (In INSERT, the target relation is not scanned,
		 * so it's not a baserel; and there are also corner cases for
		 * updatable views where the target rel isn't a baserel.)
		 */
		if (rti < root->simple_rel_array_size &&
			root->simple_rel_array[rti] != NULL)
		{
			RelOptInfo *resultRel = root->simple_rel_array[rti];

			fdwroutine = resultRel->fdwroutine;
		}
		else
		{
			RangeTblEntry *rte = planner_rt_fetch(rti, root);

			Assert(rte->rtekind == RTE_RELATION);
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				/* Check if the access to foreign tables is restricted */
				if (unlikely((restrict_nonsystem_relation_kind & RESTRICT_RELKIND_FOREIGN_TABLE) != 0))
				{
					/* there must not be built-in foreign tables */
					Assert(rte->relid >= FirstNormalObjectId);
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("access to non-system foreign table is restricted")));
				}

				fdwroutine = GetFdwRoutineByRelId(rte->relid);
			}
			else
				fdwroutine = NULL;
		}

		/*
		 * Try to modify the foreign table directly if (1) the FDW provides
		 * callback functions needed for that, (2) there are no row-level
		 * triggers on the foreign table, and (3) there are no WITH CHECK
		 * OPTIONs from parent views.
		 */
		direct_modify = false;
		if (fdwroutine != NULL &&
			fdwroutine->PlanDirectModify != NULL &&
			fdwroutine->BeginDirectModify != NULL &&
			fdwroutine->IterateDirectModify != NULL &&
			fdwroutine->EndDirectModify != NULL &&
			withCheckOptionLists == NIL &&
			!has_row_triggers(root, rti, operation))
			direct_modify = fdwroutine->PlanDirectModify(root, node, rti, i);
		if (direct_modify)
			direct_modify_plans = bms_add_member(direct_modify_plans, i);

		if (!direct_modify &&
			fdwroutine != NULL &&
			fdwroutine->PlanForeignModify != NULL)
			fdw_private = fdwroutine->PlanForeignModify(root, node, rti, i);
		else
			fdw_private = NIL;
		fdw_private_list = lappend(fdw_private_list, fdw_private);
		i++;
	}
	node->fdwPrivLists = fdw_private_list;
	node->fdwDirectModifyPlans = direct_modify_plans;

	return node;
}

/*
 * is_projection_capable_path
 *		Check whether a given Path node is able to do projection.
 */
bool
is_projection_capable_path(Path *path)
{
	/* Most plan types can project, so just list the ones that can't */
	switch (path->pathtype)
	{
		case T_Hash:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_LockRows:
		case T_Limit:
		case T_ModifyTable:
		case T_MergeAppend:
		case T_RecursiveUnion:
		case T_RemoteSubplan:
		case T_PartIterator:
			return false;
		case T_Append:

			/*
			 * Append can't project, but if it's being used to represent a
			 * dummy path, claim that it can project.  This prevents us from
			 * converting a rel from dummy to non-dummy status by applying a
			 * projection to its dummy path.
			 */
			return IS_DUMMY_PATH(path);
		case T_ProjectSet:

			/*
			 * Although ProjectSet certainly projects, say "no" because we
			 * don't want the planner to randomly replace its tlist with
			 * something else; the SRFs have to stay at top level.  This might
			 * get relaxed later.
			 */
			return false;
		default:
			break;
	}
	return true;
}

/*
 * is_projection_capable_plan
 *		Check whether a given Plan node is able to do projection.
 */
bool
is_projection_capable_plan(Plan *plan)
{
	/* Most plan types can project, so just list the ones that can't */
	switch (nodeTag(plan))
	{
		case T_Hash:
		case T_Material:
		case T_Sort:
		case T_Unique:
		case T_SetOp:
		case T_LockRows:
		case T_Limit:
		case T_ModifyTable:
		case T_Append:
		case T_MergeAppend:
		case T_RecursiveUnion:
#ifdef __OPENTENBASE_C__
		case T_PartIterator:
#endif
			return false;
#ifdef XCP
		/*
		 * Remote subplan may push down projection to the data nodes if do not
		 * performs merge sort
		 */
		case T_RemoteSubplan:
			return ((RemoteSubplan *) plan)->sort == NULL &&
					is_projection_capable_plan(plan->lefttree);
#endif
		case T_ProjectSet:

			/*
			 * Although ProjectSet certainly projects, say "no" because we
			 * don't want the planner to randomly replace its tlist with
			 * something else; the SRFs have to stay at top level.  This might
			 * get relaxed later.
			 */
			return false;
		default:
			break;
	}
	return true;
}


#ifdef XCP
/*
 * adjust_subplan_distribution
 * 	Make sure the distribution of the subplan is matching to the consumers.
 */
static void
adjust_subplan_distribution(PlannerInfo *root, Distribution *pathd,
							Distribution *subd)
{
	/* Replace path restriction with actual */
	if (pathd && !bms_is_empty(root->curOuterRestrict))
	{
		bms_free(pathd->restrictNodes);
		pathd->restrictNodes = bms_copy(root->curOuterRestrict);
	}

	root->curOuterRestrict = NULL;

	/*
	 * Set new restriction for the subpath.
	 */
	if (subd)
	{
		/*
		 * If subpath is replicated without restriction choose one execution
		 * datanode and set it as current restriction.
		 */
		if (IsLocatorReplicated(subd->distributionType) &&
			bms_num_members(subd->restrictNodes) != 1)
		{
			Bitmapset  *result = NULL;
			Bitmapset  *execute;
			Bitmapset  *common;
			int			node;

			/*
			 * We should choose one of the distribution nodes, but we can save
			 * some network traffic if chosen execution node will be one of
			 * the result nodes at the same time.
			 */
			if (pathd)
				result = bms_is_empty(pathd->restrictNodes) ?
										pathd->nodes : pathd->restrictNodes;
			execute = bms_is_empty(subd->restrictNodes) ?
										subd->nodes : subd->restrictNodes;
			common = bms_intersect(result, execute);
			if (bms_is_empty(common))
			{
				bms_free(common);
				common = bms_copy(subd->nodes);
			}
			if (subd->replication_for_update)
			{
				bms_free(common);
				root->curOuterRestrict = bms_copy(execute);
				bms_free(subd->restrictNodes);
				subd->restrictNodes = bms_copy(root->curOuterRestrict);
			}
			else
			{
				/*
				 * Check if any of the common nodes is preferred and choose one
				 * of the preferred
				 */
				node = GetAnyDataNode(common);
				bms_free(common);

				/* set restriction for the subplan */
				root->curOuterRestrict = bms_make_singleton(node);

				/* replace execution restriction for the generated  */
				bms_free(subd->restrictNodes);
				subd->restrictNodes = bms_make_singleton(node);
			}
		}
	}
}

/*
 * build_remote_value_list
 *	Build up remote value list as node CONST
 */
static List *
build_remote_value_list(List *source, Oid oid)
{
	ListCell *lc;
	Const    *const_value = NULL;
	List     *const_list = NULL;
	int16	  collen;
	bool	  constbyval;

	if (source == NULL)
		return NULL;

	get_typlenbyval(oid, &collen, &constbyval);

	foreach(lc, source)
	{
		Datum* value = (Datum *) lfirst(lc);

		const_value = makeNode(Const);

		const_value->consttype = oid;
		const_value->constcollid = InvalidOid;
		const_value->constlen = collen;
		const_value->constvalue = *value;
		const_value->constbyval = constbyval;
		if (constbyval)
		{
			const_value->consttypmod = collen;
		}
		else
		{
			const_value->consttypmod = VARSIZE(*value);
		}
		const_value->constisnull = false;
		const_value->location = -1;

		const_list = lappend(const_list, const_value);
	}

	return const_list;
}

/*
 * create_remotescan_plan
 *	  Create a RemoteSubquery plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 *
 *	  Returns a Plan node.
 */
static Plan *
create_remotescan_plan(PlannerInfo *root, RemoteSubPath *best_path)
{
	RemoteSubplan  *plan;
	Plan	   	   *subplan;
	Bitmapset  	   *saverestrict;
	Path		   *subpath = best_path->subpath;
	List		   *pathkeys = best_path->path.pathkeys;

	/*
	 * Subsequent code will modify current restriction, it needs to be restored
	 * so other path nodes in the outer tree could see correct value.
	 */
	saverestrict = root->curOuterRestrict;

	adjust_subplan_distribution(root,
								best_path->path.distribution,
								subpath->distribution);

	/* We don't want any excess columns in the remote tuples */
	subplan = create_plan_recurse(root, subpath, CP_SMALL_TLIST);

	/* Now, insert a Sort node if subplan isn't sufficiently ordered */
	if (!pathkeys_contained_in(pathkeys, subpath->pathkeys))
	{
		int 			numsortkeys;
		AttrNumber	   *sortColIdx;
		Oid			   *sortOperators;
		Oid			   *collations;
		bool		   *nullsFirst;

		subplan =  prepare_sort_from_pathkeys((Plan *)subplan,
											  pathkeys,
											  best_path->path.parent->relids,
											  NULL,
											  false,
											  &numsortkeys,
											  &sortColIdx,
											  &sortOperators,
											  &collations,
											  &nullsFirst);

		subplan = (Plan *) make_sort(subplan, numsortkeys,
									 sortColIdx, sortOperators,
									 collations, nullsFirst);
		label_sort_with_costsize(root, (Sort *)subplan, -1.0);
	}

#ifdef __OPENTENBASE_C__
	if (best_path->localSend)
	{
		plan = make_remotesubplan(root, subplan,
								NULL,
								subpath->distribution,
								NIL, true, NULL);
		plan->localSend = true;
	}
	else
#endif
	plan = make_remotesubplan(root, subplan,
							  best_path->path.distribution,
							  subpath->distribution,
							  best_path->path.pathkeys,
							  true, best_path->path.parent->relids);

	if (plan == NULL)
		return subplan;

#ifdef __OPENTENBASE__
	copy_generic_path_info((Plan *)plan, (Path *)best_path);
#endif

	/*
	 * Build up data skew path
	 *	There are two path method:
	 *	1. Roundrobin : One side distributed and the other side replicate
	 *	2. Keep MCV : Values in one side retain list are sended local;
	 *	              Values in the other side are replicated.
	 */
	if (best_path->localSend == false)
	{
		plan->roundrobin_distributed =
			best_path->data_skew_parameter.roundrobin_distributed;
		plan->roundrobin_replicate =
			best_path->data_skew_parameter.roundrobin_replicate;
		plan->retain_list =
			build_remote_value_list(best_path->data_skew_parameter.retain_list,
									best_path->data_skew_parameter.rel_oid);
		plan->replicated_list = 
			build_remote_value_list(best_path->data_skew_parameter.replicated_list,
									best_path->data_skew_parameter.rel_oid);
	}

#ifdef __OPENTENBASE_C__
	if (plan->num_workers == 0)
		plan->num_workers = ((Path *)best_path)->parallel_workers;
	/* use parallel mode for parallel plans. */
	if (plan->num_workers > 0 && !((Plan *)plan)->parallel_aware)
		root->glob->parallelModeNeeded = true;
#endif

	/* restore current restrict */
	bms_free(root->curOuterRestrict);
	root->curOuterRestrict = saverestrict;

	return (Plan *) plan;
}

static RemoteSubplan *
find_push_down_plan_int(PlannerInfo *root, Plan *plan, bool force, bool delete, Plan **parent)
{
	if (IsA(plan, RemoteSubplan) &&
			(force || (list_length(((RemoteSubplan *) plan)->nodeList) > 1 &&
					   ((RemoteSubplan *) plan)->execOnAll)))
	{
		if (delete && (parent))
			*parent = plan->lefttree;
		return (RemoteSubplan *) plan;
	}

	if (IsA(plan, Hash) ||
			IsA(plan, Material) ||
			IsA(plan, Unique) ||
			IsA(plan, Limit))
		return find_push_down_plan_int(root, plan->lefttree, force, delete, &plan->lefttree);

	/*
	 * If its a subquery scan and we are looking to replace RemoteSubplan then
	 * walk down the subplan to find a RemoteSubplan
	 */
	if (parent && IsA(plan, SubqueryScan))
	{
		Plan *subplan = ((SubqueryScan *)plan)->subplan;
		RemoteSubplan *remote_plan = find_push_down_plan_int(root,
				subplan, force, delete, &subplan);

		/*
		 * XXX This used to update rel->subplan, but thanks to upper-planner
		 * pathification the field was removed. But maybe this needs to tweak
		 * subroot instead?
		 */

		return remote_plan;
	}
	return NULL;
}

static RemoteSubplan *
find_push_down_plan(Plan *plan, bool force)
{
	return find_push_down_plan_int(NULL, plan, force, false, NULL);
}

/*
 * get_distribution_key
 *   Iterate through the target list to find the redistribution expression and
 *   return the corresponding resno. Please note that during the Path stage,
 *   the redistribution expression should have already been added to the target
 *   list using apply_distribution_exprs. If it cannot be found here, an error
 *   should be raised, but opentenbase_ora insert all grammar has some special case and
 *   we need it here to add distributionExpr.
 */
static AttrNumber
get_distribution_key(Plan **lefttree_ptr, Node *distributionExpr)
{
	ListCell   *lc;
	Expr	   *expr;
	AttrNumber result = InvalidAttrNumber;
	Plan        *lefttree = *lefttree_ptr;

	if (!distributionExpr)
		return result;

	/* XXX Is that correct to reference a column of different type? */
	expr = (Expr *) distributionExpr;
	while (IsA(expr, RelabelType))
		expr = ((RelabelType *) expr)->arg;

	/* Find distribution expression in the target list */
	foreach(lc, lefttree->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (equal(tle->expr, expr) ||
			equal(tle->expr, distributionExpr))
		{
			result = tle->resno;
			break;
		}
	}

	if (result == InvalidAttrNumber)
	{
		TargetEntry *newtle;

		/* The expression is not found, need to add junk */
		newtle = makeTargetEntry(expr,
								 list_length(lefttree->targetlist) + 1,
							     NULL,
								 true);

		if (is_projection_capable_plan(lefttree))
		{
			/* Ok to modify subplan's target list */
			lefttree->targetlist = lappend(lefttree->targetlist, newtle);
		}
		else
		{
			/* Use Result node to calculate expression */
			List *newtlist = list_copy(lefttree->targetlist);
			newtlist = lappend(newtlist, newtle);
			lefttree = (Plan *) make_result(newtlist, NULL, lefttree, NIL);
		}

		result = newtle->resno;
	}
	
	if (*lefttree_ptr != lefttree)
		*lefttree_ptr = lefttree;

	return result;
}

/*
 * begin ora_compatible
 * A int list to bitmap set.
 */
static Bitmapset *
list_to_bitmap(List *list)
{
	Bitmapset	*a = NULL;
	ListCell	*l;

	foreach(l, list)
	{
		int	i = lfirst_int(l);

		a = bms_add_member(a, i);
	}

	return a;
}
/* end ora_compatible */

RemoteSubplan *
make_remotesubplan(PlannerInfo *root,
				   Plan *lefttree,
				   Distribution *resultDistribution,
				   Distribution *execDistribution,
				   List *pathkeys, bool allow_dummy, Relids relids)
{
	RemoteSubplan *node = makeNode(RemoteSubplan);
	Plan	   *plan = &node->scan.plan;
	Bitmapset  *tmpset;
	int			nodenum;
	bool		is_multinsert = false; /* ora_compatible */

	/* Sanity checks */
	Assert(!IsA(lefttree, RemoteSubplan));
	/* ora_compatible */

	/* remove gather/gathermerge if no project */
	if ((IsA(lefttree, Gather) || IsA(lefttree, GatherMerge)) &&
		equal(lefttree->targetlist, lefttree->lefttree->targetlist))
	{
		if (IsA(lefttree, Gather))
		{
			node->num_workers = ((Gather *) lefttree)->num_workers;
			lefttree = lefttree->lefttree;
		}
		else
		{
			/* pathkeys is same already */
			node->num_workers = ((GatherMerge *) lefttree)->num_workers;
			lefttree = lefttree->lefttree;
		}
	}

	if (resultDistribution &&
		resultDistribution->distributionType != LOCATOR_TYPE_REPLICATED)
	{
		node->distributionType = resultDistribution->distributionType;
		node->ndiskeys = 0;
		node->diskeys = NULL;
		/* begin ora_compatible */
		if (root->query_level == 1 &&
			root->glob->subInsertPlans != NULL &&
			IsValidDistribution(resultDistribution) &&
			resultDistribution->nExprs == 1 &&
			IsA(resultDistribution->disExprs[0], List))
		{
			List        *subdists = NIL;
			List		*distList;
			ListCell	*lc;

			/* merge all dist/exec nodes together */
			is_multinsert = true;
			distList = (List *) resultDistribution->disExprs[0];

			foreach (lc, distList)
			{
				Distribution	*subdist = lfirst_node(Distribution, lc);
				RemoteSubplan   *rsubplan;

				rsubplan = make_remotesubplan(root,
											  lefttree,
											  subdist,
											  execDistribution,
											  pathkeys, false, relids);

				/*
				 * For replicated table in INSERT ALL/FIRST operation, we send
				 * tuple to them separately(do not use broadcast buffer),
				 * because maybe the tuple will be received by other type
				 * table(hash/shard) simultaneously.
				 */
				if (rsubplan->distributionType == LOCATOR_TYPE_REPLICATED)
				{
					Bitmapset  *tmpset;
					int			nodenum;

					tmpset = bms_copy(resultDistribution->nodes);
					rsubplan->distributionNodes = NIL;
					while ((nodenum = bms_first_member(tmpset)) >= 0)
					{
						rsubplan->distributionNodes =
							lappend_int(rsubplan->distributionNodes, nodenum);
					}
					/*
					 * Fragment will skip receive data if current node-id is not
					 * in subplan's distributionRestrict, so for replication
					 * table we also have to set distributionRestrict in
					 * INSERT ALL/FIRST scenario.
					 */
					rsubplan->distributionRestrict = list_copy(rsubplan->distributionNodes);
				}
				subdists = lappend(subdists, rsubplan);
				lefttree = rsubplan->scan.plan.lefttree;

				rsubplan->scan.plan.lefttree = NULL;
			}
			node->multi_dist = subdists;
			node->startno = root->mi_startno;
			node->endno = root->mi_endno;
			node->has_else = root->has_else;
		}
		/* end ora_compatible */
		if (IsValidDistribution(resultDistribution) &&
			resultDistribution->distributionType != LOCATOR_TYPE_MIXED)
		{
			int i = 0;
			Plan **lefttree_ptr = &lefttree;

			node->ndiskeys = resultDistribution->nExprs;
			node->diskeys = (AttrNumber *) palloc0(sizeof(AttrNumber) * node->ndiskeys);
			for (i = 0; i < resultDistribution->nExprs; i++)
			{
				node->diskeys[i] = get_distribution_key(lefttree_ptr, resultDistribution->disExprs[i]);
				if (*lefttree_ptr != lefttree)
					lefttree = *lefttree_ptr;
			}
		}
		/*
		 * The distributionNodes describes result distribution
		 */
		tmpset = bms_copy(resultDistribution->nodes);
		node->distributionNodes = NIL;
		while ((nodenum = bms_first_member(tmpset)) >= 0)
			node->distributionNodes = lappend_int(node->distributionNodes,
												  nodenum);
		bms_free(tmpset);
		/*
		 * The distributionRestrict defines the set of nodes where results are
		 * actually shipped. These are the nodes where upper level step
		 * is executed.
		 */
		if (resultDistribution->restrictNodes)
		{
			tmpset = bms_copy(resultDistribution->restrictNodes);
			node->distributionRestrict = NIL;
			while ((nodenum = bms_first_member(tmpset)) >= 0)
				node->distributionRestrict =
						lappend_int(node->distributionRestrict, nodenum);
			bms_free(tmpset);
		}
		else
			node->distributionRestrict = list_copy(node->distributionNodes);

		/* begin ora_compatible */
		if (is_multinsert)
		{
			/* Merge all nodes ID from sub plan */
			ListCell	*l;
			Bitmapset	*usetdist = NULL;
			Bitmapset	*usetrest = NULL;
			List	*subplan = castNode(List, node->multi_dist);

			foreach(l, subplan)
			{
				RemoteSubplan	*rsubplan;

				rsubplan = lfirst_node(RemoteSubplan, l);
				usetdist = bms_union(usetdist,
								list_to_bitmap(rsubplan->distributionNodes));
				usetrest = bms_union(usetrest,
								list_to_bitmap(rsubplan->distributionRestrict));
			}

			node->distributionNodes = NIL;
			while ((nodenum = bms_first_member(usetdist)) >= 0)
				node->distributionNodes = lappend_int(node->distributionNodes,
													  nodenum);
			bms_free(usetdist);

			if (!bms_is_empty(usetrest))
			{
				node->distributionRestrict = NIL;
				while ((nodenum = bms_first_member(usetrest)) >= 0)
					node->distributionRestrict =
						lappend_int(node->distributionRestrict, nodenum);
				bms_free(usetrest);
			}
			else
				node->distributionRestrict = list_copy(node->distributionNodes);
		}
		/* end ora_compatible */
	}
	else
	{
		node->distributionType = resultDistribution ?
			resultDistribution->distributionType : LOCATOR_TYPE_NONE;
		node->distributionNodes = NIL;
		node->ndiskeys = 0;
		node->diskeys = NULL;
	}

	/* determine where subplan will be executed */
	if (execDistribution)
	{
		if (execDistribution->restrictNodes)
			tmpset = bms_copy(execDistribution->restrictNodes);
		else
			tmpset = bms_copy(execDistribution->nodes);
		node->nodeList = NIL;
		while ((nodenum = bms_first_member(tmpset)) >= 0)
			node->nodeList = lappend_int(node->nodeList, nodenum);
		bms_free(tmpset);
		node->execOnAll = list_length(node->nodeList) == 1 ||
				!IsLocatorReplicated(execDistribution->distributionType);
		if (execDistribution->replication_for_update && IsLocatorReplicated(execDistribution->distributionType))
		{
			node->execOnAll = true;
			/*
			 * Replication table execute "SELECT FOR UPDATE", the command will be sent to
			 * all DNs. However, only one DN needs to return the data.
			 */
			node->only_one_send_tuple = execDistribution->replication_for_update;
		}
		else
			node->only_one_send_tuple = false;
	}
	else
	{
		/*
		 * Prepare single execution of replicated subplan. Choose one node from
		 * the execution node list, preferrably the node is also a member of
		 * the list of result nodes, so later all node executors contact the
		 * same node to get tuples
		 */
		tmpset = NULL;
		if (!bms_is_empty(resultDistribution->restrictNodes))
			tmpset = bms_copy(resultDistribution->restrictNodes);
		else
			tmpset = bms_copy(resultDistribution->nodes);
		/*
		 * If the result goes to a single node, ignore this remote operator and
		 * return null directly.
		 */
		if (bms_num_members(tmpset) > 1 || is_multinsert || !allow_dummy) /* ora_compatible */
		{
			bool load_balancing = true;

			/*
			 * file_fdw needs a stable file location
			 * TODO: Check more complicated plans
			 */
			if (IsA(lefttree, ForeignScan))
			{
				int serverid = ((ForeignScan *)lefttree)->fs_server;
				ForeignServer *server = GetForeignServer(serverid);

				if (pg_strcasecmp(server->servername, "pg_file_server") == 0)
					load_balancing = false;
			}

			/* get one execution node TODO: load balancing */
			if (load_balancing)
				nodenum = bms_any_member(tmpset);
			else
				nodenum = bms_first_member(tmpset);

			node->nodeList = list_make1_int(nodenum);
			node->execOnAll = true;
		}
		else
		{
			return NULL;
		}
		bms_free(tmpset);
	}

	if (IsA(lefttree, ForeignScan))
		((ForeignScan *)lefttree)->execNodeList = list_copy(node->nodeList);

	/*
	 * We do not need to merge sort if only one node is yielding tuples
	 * and there is no parallel.
	 */
	if (pathkeys && node->execOnAll &&
		(list_length(node->nodeList) > 1 || lefttree->parallel_num > 0))
	{
		List	   *tlist = lefttree->targetlist;
		ListCell   *i;
		int			numsortkeys;
		AttrNumber *sortColIdx;
		Oid		   *sortOperators;
		Oid		   *collations;
		bool	   *nullsFirst;

		pathkeys = get_reasonable_pathkeys_tmp(tlist, pathkeys, relids);

		/*
		 * mostly like prepare_sort_from_pathkeys
		 */
		numsortkeys = list_length(pathkeys);
		sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
		sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
		collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
		nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

		numsortkeys = 0;

		foreach(i, pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(i);
			EquivalenceClass *ec = pathkey->pk_eclass;
			TargetEntry *tle = NULL;
			Oid			pk_datatype = InvalidOid;
			Oid			sortop;
			ListCell   *j;

			if (ec->ec_has_volatile)
			{
				if (ec->ec_sortref == 0)	/* can't happen */
					elog(ERROR, "volatile EquivalenceClass has no sortref");
				tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
				Assert(tle);
				Assert(list_length(ec->ec_members) == 1);
				pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
			}
			else
			{
				foreach(j, ec->ec_members)
				{
					EquivalenceMember *em = (EquivalenceMember *) lfirst(j);

					if (em->em_is_const)
						continue;

					tle = tlist_member_ignore_relabel(em->em_expr, tlist);
					if (tle)
					{
						pk_datatype = em->em_datatype;
						break;		/* found expr already in tlist */
					}
				}

				if (!tle)
				{
					/* No matching tlist item; look for a computable expression */
					Expr	   *sortexpr = NULL;

					foreach(j, ec->ec_members)
					{
						EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
						List	   *exprvars;
						ListCell   *k;

						if (em->em_is_const)
							continue;

						sortexpr = em->em_expr;
						exprvars = pull_var_clause((Node *) sortexpr,
												   PVC_INCLUDE_AGGREGATES |
												   PVC_INCLUDE_WINDOWFUNCS |
												   PVC_INCLUDE_PLACEHOLDERS);
						foreach(k, exprvars)
						{
							if (!tlist_member_ignore_relabel(lfirst(k), tlist))
								break;
						}
						list_free(exprvars);
						if (!k)
						{
							pk_datatype = em->em_datatype;
							break;	/* found usable expression */
						}
					}
					if (!j)
						elog(ERROR, "could not find pathkey item to sort");

					/*
					 * Do we need to insert a Result node?
					 */
					if (!is_projection_capable_plan(lefttree))
					{
						/* copy needed so we don't modify input's tlist below */
						tlist = copyObject(tlist);
						lefttree = inject_projection_plan(lefttree, tlist,
														  lefttree->parallel_safe);
					}

					/*
					 * Add resjunk entry to input's tlist
					 */
					tle = makeTargetEntry(sortexpr,
										  list_length(tlist) + 1,
										  NULL,
										  true);
					tlist = lappend(tlist, tle);
					lefttree->targetlist = tlist;	/* just in case NIL before */
				}
			}

			/*
			 * Look up the correct sort operator from the PathKey's slightly
			 * abstracted representation.
			 */
			sortop = get_opfamily_member(pathkey->pk_opfamily,
										 pk_datatype,
										 pk_datatype,
										 pathkey->pk_strategy);
			if (!OidIsValid(sortop))	/* should not happen */
				elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
					 pathkey->pk_strategy, pk_datatype, pk_datatype,
					 pathkey->pk_opfamily);

			/* Add the column to the sort arrays */
			sortColIdx[numsortkeys] = tle->resno;
			sortOperators[numsortkeys] = sortop;
			collations[numsortkeys] = ec->ec_collation;
			nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
			numsortkeys++;
		}

		if (numsortkeys > 0)
		{
			node->sort = makeNode(SimpleSort);
			node->sort->numCols = numsortkeys;
			node->sort->sortColIdx = sortColIdx;
			node->sort->sortOperators = sortOperators;
			node->sort->sortCollations = collations;
			node->sort->nullsFirst = nullsFirst;
		}
	}

	plan->qual = NIL;
	plan->targetlist = lefttree->targetlist;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	copy_plan_costsize(plan, lefttree);

	node->cursor = get_internal_cursor();
	
#ifdef __OPENTENBASE_C__
	if (IsA(lefttree, ModifyTable))
	{
		List *returning = ((ModifyTable *) lefttree)->returningLists;
		Assert(plan->targetlist == NULL);
		node->transfer_datarow = remote_transfer_datarow((Node *) returning, NULL);
	}
	else if (IsA(lefttree, RemoteModifyTable))
	{
		List *returning = ((RemoteModifyTable *) lefttree)->returningLists;
		Assert(plan->targetlist == NULL);
		node->transfer_datarow = remote_transfer_datarow((Node *) returning, NULL);
	}
	else
	{
		/* check if we can send minimal tuple directly */
		node->transfer_datarow = remote_transfer_datarow((Node *) plan->targetlist, NULL);
	}
#endif

#ifdef __OPENTENBASE_C__
	/*
	 * initPlan will be check and reset in finalize_plan, and here
	 * we just keep the logic that the top node contains initPlan.
	 */
	plan->initPlan = lefttree->initPlan;
	lefttree->initPlan = NIL;

	if (execDistribution && (!IsLocatorReplicated(execDistribution->distributionType) || execDistribution->replication_for_update))
		root->glob->execNodeSet = list_union_int(root->glob->execNodeSet, node->nodeList);
#endif

	root->glob->remotesubplan_nums++;
	return node;
}


#define CNAME_MAXLEN 64
static int cursor_id = 0;
/*
 * Return a name unique for the cluster
 */
char *
get_internal_cursor(void)
{
	char *cursor;
	int  random_id;

	cursor = (char *) palloc(CNAME_MAXLEN);
	if (cursor_id++ == INT_MAX)
		cursor_id = 0;

	random_id = GetCurrentTimestamp() % INT_MAX;

	snprintf(cursor, CNAME_MAXLEN - 1, "p_%d_%x_%x_%x",
			 PGXCNodeId, getpid(), cursor_id, random_id);
	return cursor;
}
#endif

#ifdef __OPENTENBASE__
bool
contain_remote_subplan(Plan *node)
{
	if (!node)
	{
		return false;
	}
	if (node->remote_flag)
	{
		return true;
	}

	if (IsA(node, RemoteSubplan) || IsA(node, RemoteQuery))
	{
		return true;
	}

	if (IsA(node, CteScan))
	{
		return true;
	}

	if (IsA(node, SubqueryScan))
	{
		SubqueryScan *subquery = (SubqueryScan *)node;

		if (contain_remote_subplan(subquery->subplan))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	if (IsA(node, Append))
	{
		ListCell *lc;
		Append *append = (Append *)node;

		foreach(lc, append->appendplans)
		{
			Plan *appendplan = (Plan *)lfirst(lc);

			if (contain_remote_subplan(appendplan))
			{
				return true;
			}
		}

		return false;
	}

	if (IsA(node, MergeAppend))
	{
		ListCell *lc;
		MergeAppend *mergeappend = (MergeAppend *)node;

		foreach(lc, mergeappend->mergeplans)
		{
			Plan *mergeappendplan = (Plan *)lfirst(lc);

			if (contain_remote_subplan(mergeappendplan))
			{
				return true;
			}
		}

		return false;
	}

	if (contain_remote_subplan(outerPlan(node)))
	{
		return true;
	}
	if (contain_remote_subplan(innerPlan(node)))
	{
		return true;
	}

	return false;
}

static void
create_remotequery_for_rel(PlannerInfo *root, ModifyTable *mt,
						   RangeTblEntry *res_rel, Index resultRelationIndex,
						   int relcount, CmdType cmdtyp,
						   RelationAccessType accessType,
						   Relation relation, List **result,
						   RCmdType rcmdtype)
{
	char			*relname;
	RemoteQuery 	*fstep;
	RelationLocInfo	*rel_loc_info;
	Plan			*sourceDataPlan;
	RangeTblEntry	*dummy_rte;
	int 			ridx = resultRelationIndex;
	List			*sourceTargetList;

	relname = get_rel_name(res_rel->relid);

	/* Get location info of the target table */
	rel_loc_info = GetRelationLocInfo(res_rel->relid);
	if (rel_loc_info == NULL)
		return;

	fstep = makeNode(RemoteQuery);
	fstep->scan.scanrelid = resultRelationIndex;
	fstep->rcmdtype = rcmdtype;
	/*
	 * DML planning generates its own parameters that refer to the source
	 * data plan.
	 */
	fstep->is_temp = IsTempTable(res_rel->relid);
	fstep->read_only = false;
	fstep->dml_on_coordinator = true;
	sourceDataPlan = mt->plan.lefttree;
	sourceTargetList = sourceDataPlan->targetlist;
	
	if (rcmdtype == RCMD_DISTUPDATE)
	{
		int  attrno;
		Var *var;

		for (attrno = 1; attrno <= RelationGetNumberOfAttributes(relation); attrno++)
		{
			Form_pg_attribute att_tup = &(relation->rd_att->attrs[attrno - 1]);
			bool dropped = false;

			if (att_tup->attisdropped)
			{
				var = (Var *) makeNullConst(TEXTOID, -1, InvalidOid);
				dropped = true;
			}
			else
			{
				var = makeVar(ridx,
							attrno,
							att_tup->atttypid,
							att_tup->atttypmod,
							att_tup->attcollation,
							0);
			}

			if (!tlist_member((Expr *) var, fstep->base_tlist) || dropped)
			{
				TargetEntry *tle;
				tle = makeTargetEntry((Expr *) var, /* copy needed?? */
										list_length(fstep->base_tlist) + 1,
										NULL,
										false);
				fstep->base_tlist = lappend(fstep->base_tlist, tle);
			}
		}

		if (cmdtyp == CMD_INSERT)
		{
			/*
			 * If we are building INSERT stmt for dist-update,
			 * make 'ctid' as returning clause !ONLY!
			 */

			/* Anum_disupdate_insert_ctid */
			var = makeVar(ridx,
							SelfItemPointerAttributeNumber,
							TIDOID,
							-1,
							InvalidOid,
							0);
			fstep->base_tlist = add_to_flat_tlist(NULL, list_make1(var));
			/* Anum_disupdate_insert_shardid */
			var = makeVar(ridx,
							ShardIdAttributeNumber,
							INT4OID,
							-1,
							InvalidOid,
							0);
			fstep->base_tlist = add_to_flat_tlist(fstep->base_tlist, list_make1(var));

			var = makeVar(ridx,
							TableOidAttributeNumber,
							INT4OID,
							-1,
							InvalidOid,
							0);
			fstep->base_tlist = add_to_flat_tlist(fstep->base_tlist, list_make1(var));
		}

		if (mt->global_index && (cmdtyp == CMD_DELETE || cmdtyp == CMD_UPDATE))
		{
			Var *shardid;
			Var *ctid;
			Var *xc_node_id;

			ctid = makeVar(ridx, SelfItemPointerAttributeNumber,
									TIDOID, -1, InvalidOid, 0);
			shardid = makeVar(ridx, ShardIdAttributeNumber,
									INT4OID, -1, InvalidOid, 0);
			/*tableoid = makeVar(ridx, TableOidAttributeNumber,
									OIDOID, -1, InvalidOid, 0); */
			xc_node_id = makeVar(ridx, XC_NodeIdAttributeNumber,
									OIDOID, -1, InvalidOid, 0);
			fstep->base_tlist = add_to_flat_tlist(fstep->base_tlist,
									list_make3(ctid, shardid, /*tableoid,*/ xc_node_id));
		}
	}
	else if (rcmdtype == RCMD_NORETURN)
	{
		/* remote DML for trigger and functions that cannot be pushed down don't need RETURNING */
		fstep->base_tlist = NULL;
	}
	else if (mt->returningLists)
	{
		pgxc_add_returning_list(fstep,
						list_nth(mt->returningLists, relcount),
						resultRelationIndex);
	}

	pgxc_build_dml_statement(root, cmdtyp, ridx, fstep,
							sourceTargetList, false, rcmdtype);

	switch (cmdtyp)
	{
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
		case CMD_MERGE:
			fstep->combine_type  = (rel_loc_info->locatorType == LOCATOR_TYPE_REPLICATED) ?
					COMBINE_TYPE_SAME : COMBINE_TYPE_SUM;
			break;
		default:
			fstep->combine_type = COMBINE_TYPE_NONE;
			break;
	}

	if (cmdtyp == CMD_INSERT)
	{
		fstep->exec_nodes = makeNode(ExecNodes);
		fstep->exec_nodes->accesstype = accessType;
		fstep->exec_nodes->baselocatortype = rel_loc_info->locatorType;
		fstep->exec_nodes->primarynodelist = NULL;
		fstep->exec_nodes->nodeList = rel_loc_info->rl_nodeList;
	}
	else
	{
		fstep->exec_nodes = GetRelationNodes(rel_loc_info, NULL, NULL, 0,
											 accessType);
	}
	fstep->exec_nodes->en_relid = res_rel->relid;
	fstep->exec_type = EXEC_ON_DATANODES;
	fstep->exec_nodes->dis_exprs = pgxc_set_en_disExprs(res_rel->relid,
														resultRelationIndex,
														&fstep->exec_nodes->nExprs);

	dummy_rte = make_dummy_remote_rte(relname,
									makeAlias("REMOTE_DML_QUERY", NIL));
	root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
	fstep->scan.scanrelid = list_length(root->parse->rtable);

	/* get cursor for insert/update/delete/UPSERT */
	{
		bool upsert = false;
		char *cursor = get_internal_cursor();
		
		fstep->statement = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
		fstep->update_cursor = NULL;

		if (cmdtyp == CMD_INSERT && mt->onConflictAction == ONCONFLICT_UPDATE)
		{
			upsert = true;
			fstep->select_cursor = NULL;
			fstep->update_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);

			snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s%s", INSERT_TRIGGER, cursor);
			snprintf(fstep->update_cursor, CNAME_MAXLEN - 1, "%s%s", UPDATE_TRIGGER, cursor);
		}
		else
		{
			if (cmdtyp == CMD_INSERT)
			{
				snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s%s", INSERT_TRIGGER, cursor);
			}
			else if (cmdtyp == CMD_UPDATE)
			{
				snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s%s", UPDATE_TRIGGER, cursor);
			}
			else if (cmdtyp == CMD_DELETE)
			{
				snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s%s", DELETE_TRIGGER, cursor);
			}
			else
			{
				snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s", cursor);
			}
		}

		/* prepare statements */
		PrepareRemoteDMLStatement(upsert, fstep->statement, 
								  fstep->select_cursor, fstep->update_cursor);
	}

	fstep->action = UPSERT_NONE;
	*result = lappend(*result, fstep);
}

/*
 * create_remotedml_plan()
 *
 * For every target relation, add a remote query node to carry out remote
 * operations.
 */
void
create_remotedml_plan(PlannerInfo *root, Plan *topplan, CmdType cmdtyp, RCmdType rcmdtype, List **result)
{
	ModifyTable			*mt = (ModifyTable *)topplan;
	ListCell			*rel;
	int					relcount = -1;
	RelationAccessType	accessType;

	if (IS_CENTRALIZED_MODE)
		return;

	/* We expect to work only on ModifyTable node */
	if (!IsA(topplan, ModifyTable))
		elog(ERROR, "Unexpected node type: %d", topplan->type);

	switch(cmdtyp)
	{
		case CMD_SELECT:
			Assert(rcmdtype == RCMD_DISTUPDATE);
			/* command SELECT is used for remote lock tuple in dist-update case only */
			accessType = RELATION_ACCESS_READ_FOR_UPDATE;
		case CMD_UPDATE:
		case CMD_DELETE:
		case CMD_MERGE:
		case CMD_INSERT:
			accessType = RELATION_ACCESS_INSERT;
			break;

		default:
			elog(ERROR, "Unexpected command type: %d", cmdtyp);
			return ;
	}

	/*
	 * For every result relation, build a remote plan to execute remote DML.
	 */
	if (mt->partitioned_rels != NIL && IsA(topplan, ModifyTable) && ((ModifyTable*)topplan)->operation == CMD_MERGE)
	{
		Index resultRelationIndex = linitial_int(mt->partitioned_rels);
		RangeTblEntry	*res_rel;
		Relation        relation;

		relcount++;

		res_rel = rt_fetch(resultRelationIndex, root->parse->rtable);

		Assert(res_rel != NULL && res_rel->rtekind == RTE_RELATION);
		
		relation = heap_open(res_rel->relid, NoLock);

		create_remotequery_for_rel(root, mt, res_rel, resultRelationIndex, 
			                       relcount, cmdtyp, accessType, relation, result, rcmdtype);

		heap_close(relation, NoLock);
	}
	else
	{
		foreach (rel, mt->resultRelations)
		{
			Index          resultRelationIndex = lfirst_int(rel);
			RangeTblEntry *res_rel;
			Relation       relation;

			relcount++;

			res_rel = rt_fetch(resultRelationIndex, root->parse->rtable);

			/* Bad relation ? */
			if (res_rel == NULL || res_rel->rtekind != RTE_RELATION)
				continue;

			relation = heap_open(res_rel->relid, NoLock);

			create_remotequery_for_rel(root, mt, res_rel, resultRelationIndex, relcount, cmdtyp,
			                           accessType, relation, result, rcmdtype);

			heap_close(relation, NoLock);
		}
	}
}

bool
partkey_match_index(Oid indexoid, AttrNumber partkey)
{
	Relation indexrel;
	IndexInfo *indexinfo;
	bool	result;

	result = false;
	
	indexrel = index_open(indexoid, AccessShareLock);
	indexinfo = BuildIndexInfo(indexrel);

	if(indexinfo->ii_NumIndexAttrs >= 1 && indexinfo->ii_KeyAttrNumbers[0] == partkey)
	{
		result = true;
	}
	else
	{
		result = false;
	}

	pfree(indexinfo);

	index_close(indexrel, AccessShareLock);

	return result;
}

List *
build_physical_tlist_with_sysattr(List *relation_tlist, List *physical_tlist)
{
	ListCell *lc;
	TargetEntry *te;
	//TargetEntry *te_copy;
	Var *var;
	List *result = NULL;
	int offset = 1;

	foreach(lc, relation_tlist)
	{
		te = (TargetEntry*)lfirst(lc);

		/*
		  * find all vars from targetlist, and extract system attrs only
		  */
		if (IsA(te->expr, Var))
		{

			var = (Var *)te->expr;

			if(var->varattno >= 0)
				continue;

			result = lappend(result, makeTargetEntry((Expr *) var,
													 offset,
													 NULL,
													 false));
			offset++;
		}
		else
		{
			ListCell *cell;
			
			List *vars = pull_var_clause((Node *)te->expr, PVC_RECURSE_AGGREGATES | 
														   PVC_RECURSE_WINDOWFUNCS |
				                                           PVC_RECURSE_PLACEHOLDERS);

			foreach(cell, vars)
			{
				Node *node = lfirst(cell);

				if (!IsA(node, Var))
					elog(ERROR, "unexpected node %d in targetlist.", node->type);

				var = (Var *)node;

				if(var->varattno >= 0)
					continue;

				result = lappend(result, makeTargetEntry((Expr *) var,
										 offset,
										 NULL,
										 false));
				offset++;
			}
		}
	}

	offset = list_length(result);

	foreach(lc,physical_tlist)
	{
		te = (TargetEntry*)lfirst(lc);
		te->resno += offset;
	}

	return list_concat(result, physical_tlist);
}
#endif

#ifdef __OPENTENBASE_C__
/*
 * is_tle_unmaterialized
 *
 * 		Check if the TLE is not materialized yet
 */
static bool
is_tle_unmaterialized(Node *expr, Path *path)
{
	AttrMask 	*masks = path->attr_masks;

	if (path->attr_masks)
	{
		if (IsA(expr, Var))
		{
			int varno = ((Var *) expr)->varno;
			int attno = ((Var *) expr)->varattno;

			/* Return true if it's not in the path attr_mask */
			if (masks[varno] && !bms_is_member(attno, masks[varno]))
				return true;
		}
	}

	return false;
}

static Sort *
make_sort_from_groupClause(List *groupcls, Plan *lefttree)
{
	List	   *sub_tlist = lefttree->targetlist;
	ListCell   *l;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/* Convert list-ish representation to arrays wanted by executor */
	numsortkeys = list_length(groupcls);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;
	foreach(l, groupcls)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(l);
		TargetEntry *tle = get_sortgroupclause_tle(grpcl, sub_tlist);

		if (!tle)
			elog(ERROR, "could not retrieve tle for sort-from-groupClause");

		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = grpcl->sortop;
		collations[numsortkeys] = exprCollation((Node *) tle->expr);
		nullsFirst[numsortkeys] = grpcl->nulls_first;
		numsortkeys++;
	}

	return make_sort(lefttree, numsortkeys,
					 sortColIdx, sortOperators,
					 collations, nullsFirst);
}

static AttrNumber
get_distribution_key_result(Plan *plan, RemoteSubplan *pushdown,
							AttrNumber attrNum, List *tlist)
{
	ListCell	   *lc;
	TargetEntry    *key;
	AttrNumber result = InvalidAttrNumber;

	key = list_nth(pushdown->scan.plan.targetlist,
				   attrNum - 1);
	foreach(lc, tlist)
	{
		TargetEntry    *tle = (TargetEntry *) lfirst(lc);
		if (equal(tle->expr, key->expr))
		{
			result = tle->resno;
			break;
		}
	}

	if (result == InvalidAttrNumber)
	{
		/* Not found, adding */
		TargetEntry    *newtle;
		/*
		 * The target entry is *NOT* junk to ensure it is not
		 * filtered out before sending from the data node.
		 */
		newtle = makeTargetEntry(copyObject(key->expr),
								 list_length(tlist) + 1,
								 key->resname,
								 false);
		tlist = lappend(tlist, newtle);
		/* just in case if it was NIL */
		plan->targetlist = tlist;
		result = newtle->resno;
	}

	return result;
}

static int seq = 0;
Index
get_internal_index(void)
{
	Index index = 1 << 31;

	if (seq > 15)
	{
		elog(ERROR, "too many shared ctes %d", seq);
	}

	if (PGXCNodeId - 1 > 1023)
	{
		elog(ERROR, "exceed the allowed maximum node number %d", PGXCNodeId - 1);
	}

	if (MyProc->pgprocno > ((1 << 17) - 1))
	{
		elog(ERROR, "too may backends %d", MyProc->pgprocno);
	}
	
	index += ((PGXCNodeId - 1) << 21) + (MyProc->pgprocno << 4) + (seq & 0x0F);
	seq++;
	
	return index;
}

void reset_internal_index(void)
{
	seq = 0;
}

/*
 * make_remote_modifytable()
 *
 * Currently it's a little bit hack that we don't construct it from any PATH
 * we add it on top of any UPDATE command, the plan should be like:
 * 
 * Remote Update
 *	  -> Remote Subquery Scan
 *			  -> Update
 *					  -> Some scan or Join
 */
Plan *
make_remote_modifytable(PlannerInfo *root, Plan *subplan, ModifyTable *mt)
{
	RemoteModifyTable *rmt;
	ListCell	*lc;
	bool global_index = false;
	Assert(IsA(subplan, RemoteSubplan) || IsA(subplan, RemoteQuery));
	
	foreach(lc, mt->resultRelations)
	{
		RangeTblEntry *res_rel = rt_fetch(lfirst_int(lc), root->parse->rtable);
		
		/* Bad relation ? */
		if (res_rel == NULL || res_rel->rtekind != RTE_RELATION)
			continue;
		
		if (relid_has_cross_node_index(res_rel->relid))
		{
			global_index = true;
			break;
		}
	}

	if (global_index == false && mt->operation != CMD_MERGE)
		return subplan;

	rmt = makeNode(RemoteModifyTable);
	rmt->operation = mt->operation;
	rmt->returningLists = list_copy(mt->returningLists);
	rmt->global_index = global_index;

	if (rmt->global_index)
	{
		Var			*var;
		TargetEntry *tle;
		List		*tmp_list = NIL;
		RangeTblEntry *rte = rt_fetch(root->parse->resultRelation, root->parse->rtable);
		int			attrno;
		int			numattrs;
		Relation	relation;
		
		if (rte->rtekind != RTE_RELATION)
			elog(ERROR, "found an entry with global index that is not a relation");

		/* Assume we already have adequate lock */
		relation = heap_open(rte->relid, NoLock);
		
		numattrs = RelationGetNumberOfAttributes(relation);
		for (attrno = 1; attrno <= numattrs; attrno++)
		{
			Form_pg_attribute att_tup = &relation->rd_att->attrs[attrno - 1];
			
			if (att_tup->attisdropped)
			{
				var = (Var *) makeNullConst(TEXTOID, -1, InvalidOid);
			}
			else
			{
				var = makeVar(root->parse->resultRelation,
							  attrno,
							  att_tup->atttypid,
							  att_tup->atttypmod,
							  att_tup->attcollation,
							  0);
			}
			
			tmp_list = lappend(tmp_list,
							   makeTargetEntry((Expr *) var,
											   attrno,
											   NULL,
											   false));
		}
		
		heap_close(relation, NoLock);
		
		
		var = makeVar(root->parse->resultRelation,
					  ShardIdAttributeNumber,
					  INT4OID,
					  -1,
					  InvalidOid,
					  0);
		tle = makeTargetEntry((Expr *) var,
							  list_length(tmp_list) + 1,
							  pstrdup("shardid"),
							  true);
		tmp_list = lappend(tmp_list, tle);

		var = makeVar(root->parse->resultRelation,
					  TableOidAttributeNumber,
					  INT4OID,
					  -1,
					  InvalidOid,
					  0);
		/*tle = makeTargetEntry((Expr *) var,
							  list_length(tmp_list) + 1,
							  pstrdup("tableoid"),
							  true);
		tmp_list = lappend(tmp_list, tle);
		*/

		var = makeVar(root->parse->resultRelation,
					  XC_NodeIdAttributeNumber,
					  INT4OID,
					  -1,
					  InvalidOid,
					  0);
		tle = makeTargetEntry((Expr *) var,
							  list_length(tmp_list) + 1,
							  pstrdup("xc_node_id"),
							  true);
		tmp_list = lappend(tmp_list, tle);

		var = makeVar(root->parse->resultRelation,
					  SelfItemPointerAttributeNumber,
					  TIDOID,
					  -1,
					  InvalidOid,
					  0);
		tle = makeTargetEntry((Expr *) var,
							  list_length(tmp_list) + 1,
							  pstrdup("ctid"),
							  true);
		tmp_list = lappend(tmp_list, tle);

		list_free(mt->returningLists);
		mt->returningLists = list_make1(tmp_list);
	}

	/* for UPDATE command, build necessary plans for dist-update */
	if (mt->operation == CMD_UPDATE && rmt->global_index)
	{
		create_remotedml_plan(root, (Plan *)mt, CMD_UPDATE, RCMD_DISTUPDATE,
							  &rmt->dist_update_updates);
		create_remotedml_plan(root, (Plan *)mt, CMD_DELETE, RCMD_DISTUPDATE,
							  &rmt->dist_update_deletes);
		create_remotedml_plan(root, (Plan *)mt, CMD_INSERT, RCMD_DISTUPDATE,
							  &rmt->dist_update_inserts);
	}
	else if (mt->operation == CMD_DELETE && rmt->global_index)
	{
		foreach(lc, mt->resultRelations)
		{
			create_remotedml_plan(root, (Plan *)mt, CMD_DELETE, RCMD_DISTUPDATE,
								  &rmt->dist_update_deletes);
			break;
		}
	}
	else if (mt->operation == CMD_MERGE)
	{
		uint32    mt_merge_subcommands = 0;
		ListCell *l = NULL;
		foreach (l, root->parse->mergeActionList)
		{
			MergeAction *action = lfirst_node(MergeAction, l);
			switch (action->commandType)
			{
				case CMD_INSERT:
					mt_merge_subcommands |= MERGE_INSERT;
					break;
				case CMD_UPDATE:
					mt_merge_subcommands |= MERGE_UPDATE;
					break;
				case CMD_DELETE:
					mt_merge_subcommands |= MERGE_DELETE;
					break;
				case CMD_NOTHING:
					break;
				default:
					Assert(0);
					break;
			}
		}
		if ((mt_merge_subcommands & MERGE_INSERT) == MERGE_INSERT &&
		    (mt_merge_subcommands & MERGE_UPDATE) == MERGE_UPDATE)
		{
			ListCell          *l            = NULL;
			/* Currently, only the scenario of cross-node insertion is supported when epq fails
			 * after concurrent updates. */
			create_remotedml_plan(root,
			                      (Plan *) mt,
			                      CMD_INSERT,
			                      RCMD_DISTUPDATE,
			                      &rmt->dist_update_inserts);
			foreach (l, root->parse->mergeActionList)
			{
				MergeAction *action = lfirst_node(MergeAction, l);
				MergeAction *rmtAction;

				if (action->commandType == CMD_INSERT)
				{
					rmtAction = makeNode(MergeAction);
					rmtAction->commandType = action->commandType;
					rmtAction->matched = action->matched;
					rmtAction->qual = action->rmt_qual;
					action->rmt_qual = NULL;
					rmtAction->targetList = action->rmt_targetList;
					action->rmt_targetList = NIL;
					rmt->remoteMergeActionList = lappend(rmt->remoteMergeActionList, rmtAction);
				}
			}
		}
		else
			return subplan;
	}

	if (!rmt->global_index && (mt->operation != CMD_UPDATE) && (mt->operation != CMD_MERGE))
		return subplan;
	
	outerPlan(rmt) = subplan;
	
	return (Plan *) rmt;
}

/*
 * check if targetlist has global index col,
 * only call by update now
 */
bool
pgxc_targetlist_has_globalindex_col(Query *query)
{
	RangeTblEntry   *rte;
	RelationLocInfo	*rel_loc_info;
	ListCell	*lc;
	int i;
	List		*gindex_info_list;
	ListCell *l = NULL;
	IndexInfo *gindex_info;
	const char *attname;
	if (query == NULL || query->commandType != CMD_UPDATE)
	{
		return false;
	}

	rte = rt_fetch(query->resultRelation, query->rtable);
	if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION)
	{
		return false;
	}

	rel_loc_info = GetRelationLocInfo(rte->relid);
	if (!rel_loc_info)
	{
		return false;
	}

	/* get global index list info */
	gindex_info_list = RelationIdGetGlobalIndexInfoList(rte->relid);
	foreach (l, gindex_info_list)
	{
		gindex_info = lfirst(l);
		Assert(OidIsValid(gindex_info->gindex_oid));
		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			if (tle->resjunk)
				continue;
			for (i = 0; i < gindex_info->ii_NumIndexAttrs; i ++)
			{
				attname = get_attname(rte->relid, gindex_info->ii_KeyAttrNumbers[i]);
				if (strcmp(tle->resname, attname) == 0)
					return true;
			}
		}
	}
	return false;
}

/*
 * create_mergequalproj_plan
 *
 * for mot mathced tuple, calculate the real distribution key value
 */
static MergeQualProj *
create_mergequalproj_plan(PlannerInfo *root, MergeQualProjPath *best_path, int flags)
{
	MergeQualProj *mqpPlan = makeNode(MergeQualProj);
	Plan  *subplan;
	List *tlist;

	subplan = create_plan_recurse(root, best_path->subpath, flags);
	tlist	= build_path_tlist(root, &best_path->path);
	mqpPlan->plan.targetlist = tlist;
	mqpPlan->plan.lefttree = subplan;
	mqpPlan->mergeActionList = best_path->mergeActionLists;

	copy_generic_path_info(&mqpPlan->plan, (Path *) best_path);

	return mqpPlan;
}

static bool
remote_transfer_datarow(Node *node, void *context)
{
#define TypeTransferDatarow(id) (!IsSystemObjOid(id) || (id) == RECORDOID || (id) == RECORDARRAYOID)
	Expr	*expr;
	Oid 	type = InvalidOid;

	if (node == NULL)
		return false;

	expr = (Expr *) node;

	/* strip Relabels */
	while (IsA(expr, RelabelType))
	{
		if (((RelabelType *) expr)->resulttype == REGCLASSOID)
			return true;

		/* continue checking */
		expr = ((RelabelType *) expr)->arg;
	}

	switch (nodeTag(expr))
	{
		case T_Var:
			type = ((Var *) expr)->vartype;
			break;
		case T_Param:
			type = ((Param *) expr)->paramtype;
			break;
		case T_Aggref:
			if (TypeTransferDatarow(((Aggref *) expr)->aggfnoid))
				return true;
			break;
		case T_ArrayRef:
		case T_FuncExpr:
		case T_SubPlan:
		case T_FieldSelect:
		case T_ConvertRowtypeExpr:
		case T_RowExpr:
		case T_Const:
			if (TypeTransferDatarow(exprType((Node *) expr)))
				return true;
			break;
		default:
			/* continue */;
	}

	if (OidIsValid(type))
	{
		char	typ;
		Oid		typOutput;
		bool	typIsVarlena;

		if (type == RECORDOID || type == ACLITEMOID)
			return true;

		if (ORA_MODE)
		{
			/*
			 * Use the original pg array type corresponding to the nested
			 * table to perform transfer_datarow judgment, because the
			 * underlying layer of the nested table is implemented using
			 * the pg array.
			 */
			Oid		pg_array = InvalidOid;

			pg_array = nested_table_to_array_type(type);
			if (OidIsValid(pg_array))
				type = pg_array;
		}

		typ = get_typtype(type);
		if (typ == TYPTYPE_ENUM ||		/* enum */
			typ == TYPTYPE_DOMAIN ||	/* domain */
			typ == TYPTYPE_COMPOSITE ||	/* composite */
			typ == TYPTYPE_RANGE)		/* range */
			return true;

		if (TypeTransferDatarow(type))
		{
			getTypeOutputInfo(type, &typOutput, &typIsVarlena);
			if (typOutput == ARRAY_OUT_OID || typOutput == RECORD_OUT_OID)
				return true;
		}

		return false;
	}

	return expression_tree_walker(node, remote_transfer_datarow, context);
}

static ConnectBy *
make_connect_by(List *tlist, List *subtlist, List *qual,
				Plan *lefttree, Plan *righttree, bool nocycle,
				connect_by_param *cbparam, List *sortorder,
				int connect_by_no)
{
	int          n = -1;
	int          nparam;
	ConnectBy   *node = makeNode(ConnectBy);
	Plan        *plan = &node->plan;
	ListCell    *lc1, *lc2;
	TargetEntry *tle;

	Assert(list_length(cbparam->prior_params) ==
		   list_length(cbparam->prior_exprs));

	nparam = list_length(cbparam->prior_params);
	plan->targetlist = tlist;
	plan->lefttree = lefttree;
	plan->righttree = righttree;
	plan->qual = qual;

	node->nocycle = nocycle;
	node->reset_cache = cbparam->reset_cache;
	node->qual_again = cbparam->qual_again;
	node->cb_flags = cbparam->cb_flags;
	node->nparams = nparam;
	node->priorIdx = palloc(sizeof(AttrNumber) * nparam);
	node->paramIdx = palloc(sizeof(int) * nparam);

	node->nconnect = cbparam->connect_numParam;
	node->connectIdx = palloc(sizeof(AttrNumber) * node->nconnect);
	node->connectOperators = palloc(sizeof(Oid) * node->nconnect);
	node->sort = sortorder;
	node->connect_by_no = connect_by_no;

	forboth(lc1, cbparam->prior_exprs, lc2, cbparam->prior_params)
	{
		Expr   *expr = (Expr *) lfirst(lc1);
		Param  *param = (Param *) lfirst(lc2);
		Oid     eqop = InvalidOid;
		bool    hashable = false;

		n++;

		tle = tlist_member(expr, subtlist);
		if (tle == NULL)
			elog(ERROR, "can't find prior expr in sub-tlist");

		node->priorIdx[n] = tle->resno;
		node->paramIdx[n] = param->paramid;

		/*
		 * Planner ensures that connect params must generate before normal
		 * prior params, so just decrement counter to distinguish them.
		 */
		if (cbparam->connect_numParam <= 0)
			continue;
		cbparam->connect_numParam--;

		/* determine the eqop and optional sortop */
		get_sort_group_operators(param->paramtype,
								 false, false, false,
								 NULL, &eqop, NULL,
								 &hashable);

		if (!hashable || !OidIsValid(eqop))
		{
			elog(WARNING, "disable hash table cache for CONNECT BY "
						  "due to un-hashable prior expression");
			cbparam->connect_numParam = 0;
			pfree(node->connectIdx);
			pfree(node->connectOperators);
			node->connectIdx = NULL;
			node->connectOperators = NULL;
			node->nconnect = 0;
			continue;
		}

		node->connectIdx[n] = tle->resno;
		node->connectOperators[n] = eqop;
	}

	return node;
}

/*
 * create_connect_by_plan
 *
 *	  Create a Connect By plan for 'best_path' and (recursively) plans
 *	  for its subpaths.
 */
static ConnectBy *
create_connect_by_plan(PlannerInfo *root, ConnectByPath *best_path)
{
	ConnectBy   *plan;
	List    *tlist;
	List    *quals;
	Plan    *outer;
	Plan    *inner;
	PlannerInfo *connect_by_root;

	tlist = build_path_tlist(root, &best_path->path);
	quals = order_qual_clauses(root, best_path->quals);
	connect_by_root =
		list_nth(root->glob->connect_by_roots, best_path->connect_by_no - 1);

	/* Need both children to produce same tlist, so force it */
	outer = create_plan_recurse(root, best_path->outer, CP_EXACT_TLIST);
	inner = create_plan_recurse(connect_by_root, best_path->inner, CP_EXACT_TLIST);
	/*
	 * While NestLoop is executed it rescans inner plan. We do not want to
	 * rescan RemoteSubplan and do not support it.
	 * So if inner_plan is a RemoteSubplan, materialize it.
	 */
	if (list_length(best_path->cbparam->prior_params) <= 0 &&
		contain_remote_subplan(inner) &&
		!ExecMaterializesOutput(nodeTag(inner)))
	{
		Plan	   *matplan = (Plan *) make_material(inner);

		/*
		 * We assume the materialize will not spill to disk, and therefore
		 * charge just cpu_operator_cost per tuple.  (Keep this estimate in
		 * sync with cost_mergejoin.)
		 */
		copy_plan_costsize(matplan, inner);
		matplan->total_cost += cpu_operator_cost * matplan->plan_rows;

		inner = matplan;
	}

	plan = make_connect_by(tlist, inner->targetlist, quals, outer, inner,
						   best_path->nocycle, best_path->cbparam,
						   best_path->sort, best_path->connect_by_no);

	copy_generic_path_info(&plan->plan, (Path *)best_path);

	return plan;
}

#endif /* __OPENTENBASE_C__ */
