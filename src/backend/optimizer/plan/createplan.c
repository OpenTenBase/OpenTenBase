/*-------------------------------------------------------------------------
 *
 * createplan.c
 *      Routines to create the desired plan for processing a query.
 *      Planning is complete, we just need to convert the selected
 *      Path into a Plan.
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * IDENTIFICATION
 *      src/backend/optimizer/plan/createplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>
#include <math.h>

#include "access/stratnum.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
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
#include "parser/parsetree.h"
#ifdef PGXC
#include "access/htup_details.h"
#include "access/gtm.h"
#include "parser/parse_coerce.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/planner.h"
#include "access/sysattr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "access/gtm.h"
#include "catalog/pg_aggregate.h"
#include "parser/parse_coerce.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#endif /* PGXC */
#include "utils/lsyscache.h"
#ifdef __OPENTENBASE__
#include "pgxc/nodemgr.h"
#include "pgxc/squeue.h"
#include "catalog/pgxc_class.h"
#include "utils/ruleutils.h"
#include "catalog/index.h"
#endif

#ifdef __AUDIT_FGA__
#include "audit/audit_fga.h"
#endif

#include "executor/nodeAgg.h"
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
 */
#define CP_EXACT_TLIST        0x0001    /* Plan must return specified tlist */
#define CP_SMALL_TLIST        0x0002    /* Prefer narrower tlists */
#define CP_LABEL_TLIST        0x0004    /* tlist must contain sortgrouprefs */

#ifdef __OPENTENBASE__
int remote_subplan_depth = 0;
List *groupOids = NULL;
bool mergejoin = false;
bool child_of_gather = false;
bool enable_group_across_query = false;
bool enable_distributed_unique_plan = false;
int min_workers_of_hashjon_gather = PG_INT32_MAX;

static int replace_rda_depth = 0;
#endif
#ifdef __COLD_HOT__
bool has_cold_hot_table = false;
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
#ifdef XCP
static void adjust_subplan_distribution(PlannerInfo *root, Distribution *pathd,
                          Distribution *subd);
static RemoteSubplan *create_remotescan_plan(PlannerInfo *root,
                       RemoteSubPath *best_path);
//static char *get_internal_cursor(void);
static Plan *replace_rda_recurse(PlannerInfo *root, Plan *plan, Plan *parent);
#endif
static Result *create_qual_plan(PlannerInfo *root, QualPath *best_path);
static ProjectSet *create_project_set_plan(PlannerInfo *root, ProjectSetPath *best_path);
static Material *create_material_plan(PlannerInfo *root, MaterialPath *best_path,
                     int flags);
static Plan *create_unique_plan(PlannerInfo *root, UniquePath *best_path,
                   int flags);
static Gather *create_gather_plan(PlannerInfo *root, GatherPath *best_path);
static Plan *create_projection_plan(PlannerInfo *root, ProjectionPath *best_path);
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
static void get_column_info_for_window(PlannerInfo *root, WindowClause *wc,
                           List *tlist,
                           int numSortCols, AttrNumber *sortColIdx,
                           int *partNumCols,
                           AttrNumber **partColIdx,
                           Oid **partOperators,
                           int *ordNumCols,
                           AttrNumber **ordColIdx,
                           Oid **ordOperators);
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
static void copy_generic_path_info(Plan *dest, Path *src);
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
static Append *make_append(List *appendplans, List *tlist, List *partitioned_rels);
static RecursiveUnion *make_recursive_union(PlannerInfo *root,
                     List *tlist,
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
              List *hashclauses,
              Plan *lefttree, Plan *righttree,
              JoinType jointype, bool inner_unique);
static Hash *make_hash(Plan *lefttree,
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
               bool skip_mark_restore);
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
               Plan *lefttree);
static Group *make_group(List *tlist, List *qual, int numGroupCols,
           AttrNumber *grpColIdx, Oid *grpOperators,
           Plan *lefttree);
static Unique *make_unique_from_sortclauses(Plan *lefttree, List *distinctList);
static Unique *make_unique_from_pathkeys(Plan *lefttree,
                          List *pathkeys, int numCols);
static Gather *make_gather(List *qptlist, List *qpqual,
            int nworkers, bool single_copy, Plan *subplan);
static SetOp *make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan *lefttree,
           List *distinctList, AttrNumber flagColIdx, int firstFlag,
           long numGroups);
static LockRows *make_lockrows(Plan *lefttree, List *rowMarks, int epqParam);
static Result *make_result(List *tlist, Node *resconstantqual, Plan *subplan, List *qual);
static ProjectSet *make_project_set(List *tlist, Plan *subplan);
static ModifyTable *make_modifytable(PlannerInfo *root,
                 CmdType operation, bool canSetTag,
                 Index nominalRelation, List *partitioned_rels,
				 bool partColsUpdated,
                 List *resultRelations, List *subplans,
                 List *withCheckOptionLists, List *returningLists,
                 List *rowMarks, OnConflictExpr *onconflict, int epqParam);
static GatherMerge *create_gather_merge_plan(PlannerInfo *root,
                         GatherMergePath *best_path);

#ifdef XCP
static int add_sort_column(AttrNumber colIdx, Oid sortOp, Oid coll,
				bool nulls_first,int numCols, AttrNumber *sortColIdx,
				Oid *sortOperators, Oid *collations, bool *nullsFirst);
static char *get_internal_rda_seq(void);
#endif
#ifdef __OPENTENBASE__
static double GetPlanRows(Plan *plan);
static bool set_plan_parallel(Plan *plan);
static void set_plan_nonparallel(Plan *plan);
static Plan *materialize_top_remote_subplan(Plan *node);
static bool contain_node_walker(Plan *node, NodeTag type, bool search_nonparallel);
static bool check_exist_rda_replace(PlannerInfo *root, Plan *plan);
static void alter_all_gather_or_merge(PlannerInfo *root, Plan *plan);
#endif
static RemoteSubplan *find_push_down_plan(Plan *plan, bool force);

/*
 * create_plan
 *      Creates the access plan for a query by recursively processing the
 *      desired tree of pathnodes, starting at the node 'best_path'.  For
 *      every pathnode found, we create a corresponding plan node containing
 *      appropriate id, target list, and qualification information.
 *
 *      The tlists and quals in the plan tree are still in planner format,
 *      ie, Vars still correspond to the parser's numbering.  This will be
 *      fixed later by setrefs.c.
 *
 *      best_path is the best access path
 *
 *      Returns a Plan tree.
 */
Plan *
create_plan(PlannerInfo *root, Path *best_path)
{
    Plan       *plan;

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
     */
    if (!IsA(plan, ModifyTable))
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
 *      Recursive guts of create_plan().
 */
static Plan *
create_plan_recurse(PlannerInfo *root, Path *best_path, int flags)
{// #lizard forgives
    Plan       *plan;

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
                                              (ProjectionPath *) best_path);
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
        default:
            elog(ERROR, "unrecognized node type: %d",
                 (int) best_path->pathtype);
            plan = NULL;        /* keep compiler quiet */
            break;
    }

    return plan;
}

/*
 * create_scan_plan
 *     Create a scan plan for the parent relation of 'best_path'.
 */
static Plan *
create_scan_plan(PlannerInfo *root, Path *best_path, int flags)
{// #lizard forgives
    RelOptInfo *rel = best_path->parent;
    List       *scan_clauses;
    List       *gating_clauses;
    List       *tlist;
    Plan       *plan;

#ifdef __OPENTENBASE__
    bool    isindexscan = false;                /* result is sorted? */
    bool    need_merge_append = false;            /* need MergeAppend */
//    bool    need_pullup_filter = false;         /* need pull up filter */
    bool    isbackward = false;                 /* indexscan is backward ?*/
    AttrNumber partkey;
//    List        *outtlist = NULL;
//    List        *qual = NULL;

    Relation relation = NULL;
    RangeTblEntry *rte;

//    bool    does_use_physical_tlist = false;
//    bool    need_projection = false;
#endif

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
     */
    if (use_physical_tlist(root, best_path, flags))
    {
        if (best_path->pathtype == T_IndexOnlyScan)
        {
            /* For index-only scan, the preferred tlist is the index's */
            tlist = copyObject(((IndexPath *) best_path)->indexinfo->indextlist);

            /*
             * Transfer any sortgroupref data to the replacement tlist, unless
             * we don't care because the gating Result will handle it.
             */
            if (!gating_clauses)
                apply_pathtarget_labeling_to_tlist(tlist, best_path->pathtarget);
        }
        else
        {
            tlist = build_physical_tlist(root, rel);
#ifdef __OPENTENBASE__
//            does_use_physical_tlist = true;
#endif
            if (tlist == NIL)
            {
                /* Failed because of dropped cols, so use regular method */
                tlist = build_path_tlist(root, best_path);
            }
            else
            {
                /* As above, transfer sortgroupref data to replacement tlist */
                if (!gating_clauses)
                    apply_pathtarget_labeling_to_tlist(tlist, best_path->pathtarget);
            }
        }
    }
    else
    {
        tlist = build_path_tlist(root, best_path);
    }
        
#ifdef __COLD_HOT__
    /* find is there any tables located in more than one group */
    if ((rel->reloptkind == RELOPT_BASEREL || rel->reloptkind == RELOPT_OTHER_MEMBER_REL) && rel->rtekind == RTE_RELATION)
    {
        
        rte = root->simple_rte_array[rel->relid];
        relation = heap_open(rte->relid, NoLock);

        if (relation->rd_locator_info)
        {
            RelationLocInfo *loc = relation->rd_locator_info;

            if (AttributeNumberIsValid(loc->secAttrNum) || OidIsValid(loc->coldGroupId))
            {
                    has_cold_hot_table = true;
                }
            }

        heap_close(relation, NoLock);
    }
#endif

#ifdef __OPENTENBASE__
    if((rel->reloptkind == RELOPT_BASEREL || rel->reloptkind == RELOPT_OTHER_MEMBER_REL) && rel->rtekind == RTE_RELATION && rel->intervalparent && !rel->isdefault)
    {
        rte = root->simple_rte_array[rel->relid];
        relation = heap_open(rte->relid, NoLock);
        
        /* get partition key info, to decide to use append or mergeappend*/
        partkey = RelationGetPartitionColumnIndex(relation);

        if(IsA(best_path, IndexPath))
        {
            IndexPath *indexpath = (IndexPath *)best_path;

            isindexscan = true;
            if(indexpath->indexscandir == BackwardScanDirection)
            {
                isbackward = true;
            }
    
            if(partkey_match_index(indexpath->indexinfo->indexoid, partkey))
            {
                need_merge_append = false;
            }
            else
            {
                need_merge_append = true;
            }
//            need_pullup_filter = false;
/*
            if(need_pullup_filter)
            {
                if(does_use_physical_tlist)
                {
                    outtlist = tlist;
                }
                else
                {
                    List *physical_tlist = build_physical_tlist(root, rel);

                    outtlist = copyObject((void *)tlist);
                    tlist = build_physical_tlist_with_sysattr(tlist, physical_tlist);

                    if (!tlist_same_exprs(outtlist, tlist))
                        need_projection = true;
                }
            }
*/
        }

        if(bms_num_members(rel->childs) != 0)
        {
            best_path->total_cost = best_path->total_cost / bms_num_members(rel->childs);
            best_path->rows = best_path->rows / bms_num_members(rel->childs);
        }
    }
#endif

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
            plan = NULL;        /* keep compiler quiet */
            break;
    }

#ifdef __OPENTENBASE__
    if((rel->reloptkind == RELOPT_BASEREL || rel->reloptkind == RELOPT_OTHER_MEMBER_REL) && rel->rtekind == RTE_RELATION && rel->intervalparent && !rel->isdefault)
    {        
        /* create append plan with a list of scan.
          * If partition parent relation is target realtion of UPDATE or DELETE statement,
          * then compute pruning result and not need to expand.
          * in case of this, target relation will be expend in ModifyTable operator.
          */
        if(!bms_is_empty(rel->childs) 
            && (root->parse->resultRelation != rel->relid || root->parse->commandType == CMD_INSERT))
        {            
            List *scanlist;
            Scan *child;
            Scan **child_array = NULL;
            int arrlen;
            int arridx;
            int idx;
            Bitmapset *parts;
/*
            if(need_pullup_filter)
            {
                qual = plan->qual;
                plan->qual = NULL;
            }
*/
            arrlen = bms_num_members(rel->childs);
            parts = bms_copy(rel->childs);
            child_array = (Scan **)palloc0(arrlen*sizeof(Scan*));
            scanlist = NULL;
            child = NULL;    
            arridx = 0;
            while((idx = bms_first_member(parts)) != -1)
            {
                child = (Scan *)copyObject((void*)plan);
                child->ispartchild = true;
                child->childidx = idx;
                switch(nodeTag(child))
                {
                    case T_SeqScan:
					case T_SampleScan:
                        break;
                    case T_IndexScan:
                        {
                            IndexScan *idxscan_child;
                            idxscan_child = (IndexScan *)child;                 
                            idxscan_child->indexid = RelationGetPartitionIndex(relation,idxscan_child->indexid,idx);
                        }
                        break;
                    case T_IndexOnlyScan:
                        {
                            IndexOnlyScan *idxonlyscan_child;
                            idxonlyscan_child = (IndexOnlyScan *)child;                 
                            idxonlyscan_child->indexid = RelationGetPartitionIndex(relation,idxonlyscan_child->indexid,idx);
                        }
                        break;
                    case T_BitmapHeapScan:
                        {
                            //BitmapHeapScan *bscan;
                            //bscan = (BitmapHeapScan *)child;
                            replace_partidx_bitmapheapscan(relation,(Node*)child->plan.lefttree,idx);
                        }
                        break;
                    case T_TidScan:
                        elog(ERROR,"cannot use tidscan on partitioned table.");
                        break;
                    default:
                        elog(ERROR,"create scan plan: unsupported scan method[%d] on partitioned table.", nodeTag(child));
                        break;
                }            

                child_array[arridx++] = child;

                //scanlist = lappend(scanlist,child);
            }

            bms_free(parts);

            if(arrlen == 1)
            {
                plan = (Plan*)child_array[0];
/*
                if(need_pullup_filter)
                {
                    plan->qual = qual;
                }
*/
            }
            else
            {
                if(isindexscan && isbackward)
                {
                    arridx = 0;
                    for(arridx = arrlen-1; arridx >= 0; arridx--)
                        scanlist = lappend(scanlist, child_array[arridx]);
                }
                else
                {
                    arridx = 0;
                    for(arridx = 0; arridx < arrlen; arridx++)
                        scanlist = lappend(scanlist, child_array[arridx]);
                }

                if(need_merge_append)
                {
                    MergeAppend *mappend = makeNode(MergeAppend);
                    

                    prepare_sort_from_pathkeys(plan, best_path->pathkeys,
                                                rel->relids, 
                                                NULL, 
                                                true,
                                                &mappend->numCols,
                                                &mappend->sortColIdx,
                                                &mappend->sortOperators,
                                                &mappend->collations,
                                                &mappend->nullsFirst);

                    
                    mappend->mergeplans = scanlist;
                    mappend->plan.lefttree = NULL;
                    mappend->plan.righttree = NULL;

                    mappend->plan.plan_rows = plan->plan_rows * arrlen;
                    mappend->plan.plan_width = plan->plan_width;
                    mappend->plan.startup_cost = plan->startup_cost;
                    mappend->plan.total_cost = plan->total_cost * arrlen;    /* ms */
/*
                    if(need_pullup_filter)
                    {
                        mappend->plan.targetlist = outtlist;
                        mappend->plan.qual = qual;
                    }
                    else
*/
                    {
                        mappend->plan.targetlist = (List *)copyObject(plan->targetlist);
                        mappend->plan.qual = NULL;
                    }
                    mappend->interval = true;
                    mappend->plan.parallel_aware = best_path->parallel_aware;
                    plan = (Plan *)mappend;
                }
                else
                {
                    Append *append = NULL;
                    append = make_append(scanlist, tlist, NULL);
                    append->interval = true;
                    append->plan.parallel_aware = best_path->parallel_aware;
                    plan = (Plan *)append;
                }
            }
            pfree(child_array);
        }

        heap_close(relation, NoLock);

        if(bms_is_empty(rel->childs))
        {
            plan->isempty = true;
        }
/*
        if (need_projection)
        {
			plan = (Plan *)make_result(outtlist, NULL, plan, NULL);
            plan->parallel_aware = best_path->parallel_aware;
        }
*/
    }
#endif

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
 * Build a target list (ie, a list of TargetEntry) for the Path's output.
 *
 * This is almost just make_tlist_from_pathtarget(), but we also have to
 * deal with replacing nestloop params.
 */
static List *
build_path_tlist(PlannerInfo *root, Path *path)
{
    List       *tlist = NIL;
    Index       *sortgrouprefs = path->pathtarget->sortgrouprefs;
    int            resno = 1;
    ListCell   *v;

    foreach(v, path->pathtarget->exprs)
    {
        Node       *node = (Node *) lfirst(v);
        TargetEntry *tle;

        /*
         * If it's a parameterized path, there might be lateral references in
         * the tlist, which need to be replaced with Params.  There's no need
         * to remake the TargetEntry nodes, so apply this to each list item
         * separately.
         */
        if (path->param_info)
            node = replace_nestloop_params(root, node);

        tle = makeTargetEntry((Expr *) node,
                              resno,
                              NULL,
                              false);
        if (sortgrouprefs)
            tle->ressortgroupref = sortgrouprefs[resno - 1];

        tlist = lappend(tlist, tle);
        resno++;
    }
    return tlist;
}

/*
 * use_physical_tlist
 *        Decide whether to use a tlist matching relation structure,
 *        rather than only those Vars actually referenced.
 */
static bool
use_physical_tlist(PlannerInfo *root, Path *path, int flags)
{// #lizard forgives
    RelOptInfo *rel = path->parent;
    int            i;
    ListCell   *lc;

    /*
     * Forget it if either exact tlist or small tlist is demanded.
     */
    if (flags & (CP_EXACT_TLIST | CP_SMALL_TLIST))
        return false;

    /*
	 * if we got cn-udf or rownum expr, return false to use
	 * pathtarget to generate tlist.
	 */
	if (root->parse && root->parse->hasCoordFuncs)
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
     * labeled again.)    We also have to check that no two sort/group columns
     * are the same Var, else that element of the physical tlist would need
     * conflicting ressortgroupref labels.
     */
    if ((flags & CP_LABEL_TLIST) && path->pathtarget->sortgrouprefs)
    {
        Bitmapset  *sortgroupatts = NULL;

        i = 0;
        foreach(lc, path->pathtarget->exprs)
        {
            Expr       *expr = (Expr *) lfirst(lc);

            if (path->pathtarget->sortgrouprefs[i])
            {
                if (expr && IsA(expr, Var))
                {
                    int            attno = ((Var *) expr)->varattno;

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
 *      See if there are pseudoconstant quals in a node's quals list
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
 *      Deal with pseudoconstant qual clauses
 *
 * Add a gating Result node atop the already-built plan.
 */
static Plan *
create_gating_plan(PlannerInfo *root, Path *path, Plan *plan,
                   List *gating_quals)
{
    Plan       *gplan;

    Assert(gating_quals);

    /*
     * Since we need a Result node anyway, always return the path's requested
     * tlist; that's never a wrong choice, even if the parent node didn't ask
     * for CP_EXACT_TLIST.
     */
    gplan = (Plan *) make_result(build_path_tlist(root, path),
                                 (Node *) gating_quals,
								 plan, NULL);

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
 *      Create a join plan for 'best_path' and (recursively) plans for its
 *      inner and outer paths.
 */
static Plan *
create_join_plan(PlannerInfo *root, JoinPath *best_path)
{
    Plan       *plan;
    List       *gating_clauses;

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
            plan = NULL;        /* keep compiler quiet */
            break;
    }

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

/*
 * create_append_plan
 *      Create an Append plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 *
 *      Returns a Plan node.
 */
static Plan *
create_append_plan(PlannerInfo *root, AppendPath *best_path)
{
    Append       *plan;
    List       *tlist = build_path_tlist(root, &best_path->path);
    List       *subplans = NIL;
    ListCell   *subpaths;
#ifdef __OPENTENBASE__
    bool       parallel_aware = true;
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
        Plan       *plan;

        plan = (Plan *) make_result(tlist,
                                    (Node *) list_make1(makeBoolConst(false,
                                                                      false)),
									NULL, NULL);

        copy_generic_path_info(plan, (Path *) best_path);

        return plan;
    }

    /* Build the plan for each child */
    foreach(subpaths, best_path->subpaths)
    {
        Path       *subpath = (Path *) lfirst(subpaths);
        Plan       *subplan;

        /* Must insist that all children return the same tlist */
        subplan = create_plan_recurse(root, subpath, CP_EXACT_TLIST);

        subplans = lappend(subplans, subplan);

#ifdef __OPENTENBASE__
        parallel_aware = parallel_aware & subplan->parallel_aware;
#endif
    }

    /*
     * XXX ideally, if there's just one child, we'd not bother to generate an
     * Append node but just return the single child.  At the moment this does
     * not work because the varno of the child scan plan won't match the
     * parent-rel Vars it'll be asked to emit.
     */

    plan = make_append(subplans, tlist, best_path->partitioned_rels);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

#ifdef __OPENTENBASE__
    if (olap_optimizer)
    {
        plan->plan.parallel_aware = parallel_aware;
    }
#endif

    return (Plan *) plan;
}

/*
 * create_merge_append_plan
 *      Create a MergeAppend plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 *
 *      Returns a Plan node.
 */
static Plan *
create_merge_append_plan(PlannerInfo *root, MergeAppendPath *best_path)
{
    MergeAppend *node = makeNode(MergeAppend);
    Plan       *plan = &node->plan;
    List       *tlist = build_path_tlist(root, &best_path->path);
    List       *pathkeys = best_path->path.pathkeys;
    List       *subplans = NIL;
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
        Path       *subpath = (Path *) lfirst(subpaths);
        Plan       *subplan;
        int            numsortkeys;
        AttrNumber *sortColIdx;
        Oid           *sortOperators;
        Oid           *collations;
        bool       *nullsFirst;

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
            Sort       *sort = make_sort(subplan, numsortkeys,
                                         sortColIdx, sortOperators,
                                         collations, nullsFirst);

            label_sort_with_costsize(root, sort, best_path->limit_tuples);
            subplan = (Plan *) sort;
        }

        subplans = lappend(subplans, subplan);
    }

    node->partitioned_rels = best_path->partitioned_rels;
    node->mergeplans = subplans;

    return (Plan *) node;
}

/*
 * create_result_plan
 *      Create a Result plan for 'best_path'.
 *      This is only used for degenerate cases, such as a query with an empty
 *      jointree.
 *
 *      Returns a Plan node.
 */
static Result *
create_result_plan(PlannerInfo *root, ResultPath *best_path)
{
    Result       *plan;
    List       *tlist;
    List       *quals;

    tlist = build_path_tlist(root, &best_path->path);

    /* best_path->quals is just bare clauses */
    quals = order_qual_clauses(root, best_path->quals);

	plan = make_result(tlist, (Node *) quals, NULL, NULL);

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
	
	subplan = create_plan_recurse(root, best_path->subpath, 0);
	
	tlist = build_path_tlist(root, &best_path->path);
	
	/* best_path->quals is just bare clauses */
	quals = order_qual_clauses(root, best_path->quals);
	
	plan = make_result(tlist, NULL, subplan, quals);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}

/*
 * create_project_set_plan
 *      Create a ProjectSet plan for 'best_path'.
 *
 *      Returns a Plan node.
 */
static ProjectSet *
create_project_set_plan(PlannerInfo *root, ProjectSetPath *best_path)
{
    ProjectSet *plan;
    Plan       *subplan;
    List       *tlist;

    /* Since we intend to project, we don't need to constrain child tlist */
    subplan = create_plan_recurse(root, best_path->subpath, 0);

    tlist = build_path_tlist(root, &best_path->path);

    plan = make_project_set(tlist, subplan);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}

/*
 * create_material_plan
 *      Create a Material plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 *
 *      Returns a Plan node.
 */
static Material *
create_material_plan(PlannerInfo *root, MaterialPath *best_path, int flags)
{
    Material   *plan;
    Plan       *subplan;

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
 *      Create a Unique plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 *
 *      Returns a Plan node.
 */
static Plan *
create_unique_plan(PlannerInfo *root, UniquePath *best_path, int flags)
{// #lizard forgives
    Plan       *plan;
    Plan       *subplan;
    List       *in_operators;
    List       *uniq_exprs;
    List       *newtlist;
    int            nextresno;
    bool        newitems;
    int            numGroupCols;
    AttrNumber *groupColIdx;
    int            groupColPos;
    ListCell   *l;
#ifdef __OPENTENBASE__
	TargetEntry *distributed_expr = NULL;
	Path	    *subpath = best_path->subpath;
	Node	    *sub_disExpr = NULL;
	bool        match_distributed_column = false;

	if (enable_distributed_unique_plan)
	{
		if (subpath && subpath->distribution && subpath->distribution->distributionExpr)
		{
			sub_disExpr = subpath->distribution->distributionExpr;
		}
	}
#endif
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
        Expr       *uniqexpr = lfirst(l);
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
        Expr       *uniqexpr = lfirst(l);
        TargetEntry *tle;

        tle = tlist_member(uniqexpr, newtlist);
        if (!tle)                /* shouldn't happen */
            elog(ERROR, "failed to find unique expression in subplan tlist");
        groupColIdx[groupColPos++] = tle->resno;
#ifdef __OPENTENBASE__
		if (enable_distributed_unique_plan)
		{
			if (equal(tle->expr, sub_disExpr))
			{
				match_distributed_column = true;
			}
		}
#endif
    }

    if (best_path->umethod == UNIQUE_PATH_HASH)
    {
        Oid           *groupOperators;

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
            Oid            in_oper = lfirst_oid(l);
            Oid            eq_oper;

            if (!get_compatible_hash_operators(in_oper, NULL, &eq_oper))
                elog(ERROR, "could not find compatible hash operator for operator %u",
                     in_oper);
            groupOperators[groupColPos++] = eq_oper;
#ifdef __OPENTENBASE__
			if (enable_distributed_unique_plan)
			{
				if (!distributed_expr)
				{
					int groupColIndex = groupColPos - 1;
					distributed_expr = get_tle_by_resno(subplan->targetlist,
										   groupColIdx[groupColIndex]);
				}
			}
#endif
        }
#ifdef __OPENTENBASE__
		if (enable_distributed_unique_plan)
		{
			if (!match_distributed_column)
			{
				bool is_distributable = false;
				Oid  hashType = InvalidOid;
				if (distributed_expr)
				{
					hashType = exprType((Node *)distributed_expr->expr);
					is_distributable = IsTypeHashDistributable(hashType);
				}
				if (!is_distributable)
				{
					elog(LOG, "create_unique_plan using hash: redistribution is impossible because of"
						" undistributable hash type %u", hashType);
				}
				if (subpath && subpath->distribution && 
					subpath->distribution->distributionExpr && is_distributable)
				{
					Plan *remote_subplan = NULL;
					Distribution *distribution = NULL;
					/* remove duplicated tuples on each node */
					Plan *first_agg = (Plan *) make_agg(subplan->targetlist,
														 NIL,
														 AGG_HASHED,
														 AGGSPLIT_SIMPLE,
														 numGroupCols,
														 groupColIdx,
														 groupOperators,
														 NIL,
														 NIL,
														 best_path->path.rows,
														 subplan);
					/* redistributed group by results between nodes */
					distribution = copyObject(subpath->distribution);
					distribution->distributionExpr = (Node *)distributed_expr->expr;
					remote_subplan = (Plan *)make_remotesubplan(root, first_agg, distribution, subpath->distribution, NULL);

					/* remove duplicated tuples on each node again */
					plan = (Plan *) make_agg(build_path_tlist(root, &best_path->path),
														 NIL,
														 AGG_HASHED,
														 AGGSPLIT_SIMPLE,
														 numGroupCols,
														 groupColIdx,
														 groupOperators,
														 NIL,
														 NIL,
														 best_path->path.rows,
														 remote_subplan);
					/* Copy cost data from Path to Plan */
					copy_generic_path_info(plan, &best_path->path);

					return plan;
				}
			}
		}
#endif
        /*
         * Since the Agg node is going to project anyway, we can give it the
         * minimum output tlist, without any stuff we might have added to the
         * subplan tlist.
         */
        plan = (Plan *) make_agg(build_path_tlist(root, &best_path->path),
                                 NIL,
                                 AGG_HASHED,
                                 AGGSPLIT_SIMPLE,
                                 numGroupCols,
                                 groupColIdx,
                                 groupOperators,
                                 NIL,
                                 NIL,
                                 best_path->path.rows,
                                 subplan);    

    }
    else
    {
        List       *sortList = NIL;
        Sort       *sort;

        /* Create an ORDER BY list to sort the input compatibly */
        groupColPos = 0;
        foreach(l, in_operators)
        {
            Oid            in_oper = lfirst_oid(l);
            Oid            sortop;
            Oid            eqop;
            TargetEntry *tle;
            SortGroupClause *sortcl;

            sortop = get_ordering_op_for_equality_op(in_oper, false);
            if (!OidIsValid(sortop))    /* shouldn't happen */
                elog(ERROR, "could not find ordering operator for equality operator %u",
                     in_oper);

            /*
             * The Unique node will need equality operators.  Normally these
             * are the same as the IN clause operators, but if those are
             * cross-type operators then the equality operators are the ones
             * for the IN clause operators' RHS datatype.
             */
            eqop = get_equality_op_for_ordering_op(sortop, NULL);
            if (!OidIsValid(eqop))    /* shouldn't happen */
                elog(ERROR, "could not find equality operator for ordering operator %u",
                     sortop);

            tle = get_tle_by_resno(subplan->targetlist,
                                   groupColIdx[groupColPos]);
            Assert(tle != NULL);
#ifdef __OPENTENBASE__
            /* get distributed column for remote_subplan */
            if (enable_distributed_unique_plan)
            {
                if (!distributed_expr)
                {
                    distributed_expr = tle;
                }
            }
#endif
            sortcl = makeNode(SortGroupClause);
            if(tle)
            {
                sortcl->tleSortGroupRef = assignSortGroupRef(tle,
                                                         subplan->targetlist);
            }
            sortcl->eqop = eqop;
            sortcl->sortop = sortop;
            sortcl->nulls_first = false;
            sortcl->hashable = false;    /* no need to make this accurate */
            sortList = lappend(sortList, sortcl);
            groupColPos++;
        }
#ifdef __OPENTENBASE__
		if (enable_distributed_unique_plan)
		{
			if (!match_distributed_column)
			{
				bool is_distributable = false;
				Oid  hashType = InvalidOid;
				if (distributed_expr)
				{
					hashType = exprType((Node *)distributed_expr->expr);
					is_distributable = IsTypeHashDistributable(hashType);
				}
				if (!is_distributable)
				{
					elog(LOG, "create_unique_plan using sort: redistribution is impossible because of"
						" undistributable hash type %u", hashType);
				}
				if (subpath && subpath->distribution && 
					subpath->distribution->distributionExpr && is_distributable)
				{
					Distribution *distribution = copyObject(subpath->distribution);
					distribution->distributionExpr = (Node *)distributed_expr->expr;
					subplan = (Plan *)make_remotesubplan(root, subplan, distribution, subpath->distribution, NULL);
				}
			}
		}
#endif
        sort = make_sort_from_sortclauses(sortList, subplan);
        label_sort_with_costsize(root, sort, -1.0);
        plan = (Plan *) make_unique_from_sortclauses((Plan *) sort, sortList);
    }

    /* Copy cost data from Path to Plan */
    copy_generic_path_info(plan, &best_path->path);

    return plan;
}

/*
 * create_gather_plan
 *
 *      Create a Gather plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Gather *
create_gather_plan(PlannerInfo *root, GatherPath *best_path)
{
    Gather       *gather_plan;
    Plan       *subplan;
    List       *tlist;
    bool reset = false;
    bool contain_nonparallel_hashjoin = false;

    /* if child_of_gather is false, set child_of_gather true, and reset the value before return */
    if (!child_of_gather)
    {
        child_of_gather = true;
        reset = true;
    }

    /*
     * Although the Gather node can project, we prefer to push down such work
     * to its child node, so demand an exact tlist from the child.
     */
    subplan = create_plan_recurse(root, best_path->subpath, CP_EXACT_TLIST);

    tlist = build_path_tlist(root, &best_path->path);

	/* if contain nonparallel hashjoin, set num_workers to 1 */
    contain_nonparallel_hashjoin = contain_node_walker(subplan, T_HashJoin, true);

    gather_plan = make_gather(tlist,
                              NIL,
                              (contain_nonparallel_hashjoin) ? 1 : best_path->num_workers,
                              best_path->single_copy,
                              subplan);

    copy_generic_path_info(&gather_plan->plan, &best_path->path);

    /* use parallel mode for parallel plans. */
    root->glob->parallelModeNeeded = true;

	if (reset)
    {
        child_of_gather = false;
    }

    return gather_plan;
}

/*
 * create_gather_merge_plan
 *
 *      Create a Gather Merge plan for 'best_path' and (recursively)
 *      plans for its subpaths.
 */
static GatherMerge *
create_gather_merge_plan(PlannerInfo *root, GatherMergePath *best_path)
{
    GatherMerge *gm_plan;
    Plan       *subplan;
    List       *pathkeys = best_path->path.pathkeys;
    List       *tlist = build_path_tlist(root, &best_path->path);

    /* As with Gather, it's best to project away columns in the workers. */
    subplan = create_plan_recurse(root, best_path->subpath, CP_EXACT_TLIST);

    /* Create a shell for a GatherMerge plan. */
    gm_plan = makeNode(GatherMerge);
    gm_plan->plan.targetlist = tlist;
    gm_plan->num_workers = best_path->num_workers;
    copy_generic_path_info(&gm_plan->plan, &best_path->path);

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
 *      Create a plan tree to do a projection step and (recursively) plans
 *      for its subpaths.  We may need a Result node for the projection,
 *      but sometimes we can just let the subplan do the work.
 */
static Plan *
create_projection_plan(PlannerInfo *root, ProjectionPath *best_path)
{// #lizard forgives
    Plan       *plan;
    Plan       *subplan;
    List       *tlist;

    /* Since we intend to project, we don't need to constrain child tlist */
    subplan = create_plan_recurse(root, best_path->subpath, 0);

    tlist = build_path_tlist(root, &best_path->path);

    /*
     * We might not really need a Result node here, either because the subplan
     * can project or because it's returning the right list of expressions
     * anyway.  Usually create_projection_path will have detected that and set
     * dummypp if we don't need a Result; but its decision can't be final,
     * because some createplan.c routines change the tlists of their nodes.
     * (An example is that create_merge_append_plan might add resjunk sort
     * columns to a MergeAppend.)  So we have to recheck here.  If we do
     * arrive at a different answer than create_projection_path did, we'll
     * have made slightly wrong cost estimates; but label the plan with the
     * cost estimates we actually used, not "corrected" ones.  (XXX this could
     * be cleaned up if we moved more of the sortcolumn setup logic into Path
     * creation, but that would add expense to creating Paths we might end up
     * not using.)
     */
    if (is_projection_capable_path(best_path->subpath) ||
        tlist_same_exprs(tlist, subplan->targetlist))
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
#ifdef __OPENTENBASE__
        /* set interval child tlist */
        if (IsA(plan, Append))
        {
            Append *append = (Append *)plan;

            if (append->interval)
            {
                ListCell *lc;

                foreach(lc, append->appendplans)
                {
                    Plan *child_plan = (Plan *)lfirst(lc);

                    child_plan->targetlist = tlist;
                }
            }
        }
        else if (IsA(plan, MergeAppend))
        {
            MergeAppend *mergeappend = (MergeAppend *)plan;

            if (mergeappend->interval)
            {
                ListCell *lc;

                foreach(lc, mergeappend->mergeplans)
                {
                    Plan *child_plan = (Plan *)lfirst(lc);

                    child_plan->targetlist = tlist;
                }
            }
        }
#endif
    }
    else
    {
        /* We need a Result node */
		plan = (Plan *) make_result(tlist, NULL, subplan, NULL);

        copy_generic_path_info(plan, (Path *) best_path);
    }

    return plan;
}

/*
 * inject_projection_plan
 *      Insert a Result node to do a projection step.
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
    Plan       *plan;

	plan = (Plan *) make_result(tlist, NULL, subplan, NULL);

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
 *      Create a Sort plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Sort *
create_sort_plan(PlannerInfo *root, SortPath *best_path, int flags)
{
    Sort       *plan;
    Plan       *subplan;

    /*
     * We don't want any excess columns in the sorted tuples, so request a
     * smaller tlist.  Otherwise, since Sort doesn't project, tlist
     * requirements pass through.
     */
    subplan = create_plan_recurse(root, best_path->subpath,
                                  flags | CP_SMALL_TLIST);

	plan = make_sort_from_pathkeys(subplan, best_path->path.pathkeys, NULL);

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
 *      Create a Group plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Group *
create_group_plan(PlannerInfo *root, GroupPath *best_path)
{
    Group       *plan;
    Plan       *subplan;
    List       *tlist;
    List       *quals;

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

    return plan;
}

/*
 * create_upper_unique_plan
 *
 *      Create a Unique plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Unique *
create_upper_unique_plan(PlannerInfo *root, UpperUniquePath *best_path, int flags)
{
    Unique       *plan;
    Plan       *subplan;

    /*
     * Unique doesn't project, so tlist requirements pass through; moreover we
     * need grouping columns to be labeled.
     */
    subplan = create_plan_recurse(root, best_path->subpath,
                                  flags | CP_LABEL_TLIST);

    plan = make_unique_from_pathkeys(subplan,
                                     best_path->path.pathkeys,
                                     best_path->numkeys);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}

/*
 * create_agg_plan
 *
 *      Create an Agg plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Agg *
create_agg_plan(PlannerInfo *root, AggPath *best_path)
{
    Agg           *plan;
    Plan       *subplan;
    List       *tlist;
    List       *quals;

    /*
     * Agg can project, so no need to be terribly picky about child tlist, but
     * we do need grouping columns to be available
     */
    subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

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
                    subplan);

    copy_generic_path_info(&plan->plan, (Path *) best_path);
#ifdef __OPENTENBASE__
	/* set entry size of hashtable using by hashagg */
	if (g_hybrid_hash_agg)
	{
		if (best_path->aggstrategy == AGG_HASHED)
		{
			plan->entrySize = best_path->entrySize;
			plan->hybrid = best_path->hybrid;

			if (plan->entrySize == 0)
			{
				elog(ERROR, "Invalid hashtable entry size %u", plan->entrySize);
			}
		}
	}

	plan->noDistinct = best_path->noDistinct;
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
    int            i;

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
 *      Create a plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 *
 *      What we emit is an Agg plan with some vestigial Agg and Sort nodes
 *      hanging off the side.  The top Agg implements the last grouping set
 *      specified in the GroupingSetsPath, and any additional grouping sets
 *      each give rise to a subsidiary Agg and Sort node in the top Agg's
 *      "chain" list.  These nodes don't participate in the plan directly,
 *      but they are a convenient way to represent the required data for
 *      the extra steps.
 *
 *      Returns a Plan node.
 */
static Plan *
create_groupingsets_plan(PlannerInfo *root, GroupingSetsPath *best_path)
{// #lizard forgives
    Agg           *plan;
    Plan       *subplan;
    List       *rollups = best_path->rollups;
    AttrNumber *grouping_map;
    int            maxref;
    List       *chain;
    ListCell   *lc;

    /* Shouldn't get here without grouping sets */
    Assert(root->parse->groupingSets);
    Assert(rollups != NIL);

    /*
     * Agg can project, so no need to be terribly picky about child tlist, but
     * we do need grouping columns to be available
     */
    subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

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
     *
     * This doesn't work if we're in an inheritance subtree (see notes in
     * create_modifytable_plan).  Fortunately we can't be because there would
     * never be grouping in an UPDATE/DELETE; but let's Assert that.
     */
    Assert(!root->hasInheritedTarget);
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
        bool        is_first_sort = ((RollupData *) linitial(rollups))->is_hashed;

        for_each_cell(lc, lc2)
        {
            RollupData *rollup = lfirst(lc);
            AttrNumber *new_grpColIdx;
            Plan       *sort_plan = NULL;
            Plan       *agg_plan;
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
        int            numGroupCols;

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
                        subplan);

        /* Copy cost data from Path to Plan */
        copy_generic_path_info(&plan->plan, &best_path->path);
    }

    return (Plan *) plan;
}

/*
 * create_minmaxagg_plan
 *
 *      Create a Result plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Result *
create_minmaxagg_plan(PlannerInfo *root, MinMaxAggPath *best_path)
{
    Result       *plan;
    List       *tlist;
    ListCell   *lc;

    /* Prepare an InitPlan for each aggregate's subquery. */
    foreach(lc, best_path->mmaggregates)
    {
        MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);
        PlannerInfo *subroot = mminfo->subroot;
        Query       *subparse = subroot->parse;
        Plan       *plan;

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
								   0, 1,
								   false);

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
            plan = (Plan *) make_remotesubplan(root, plan,
                                               NULL,
                                               mminfo->path->distribution,
                                               mminfo->path->pathkeys);
            remote_subplan_depth--;

            plan = (Plan *) make_limit(plan,
                                       subparse->limitOffset,
                                       subparse->limitCount,
									   0, 1,
									   false);

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

	plan = make_result(tlist, (Node *) best_path->quals, NULL, NULL);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    /*
     * During setrefs.c, we'll need to replace references to the Agg nodes
     * with InitPlan output params.  (We can't just do that locally in the
     * MinMaxAgg node, because path nodes above here may have Agg references
     * as well.)  Save the mmaggregates list to tell setrefs.c to do that.
     *
     * This doesn't work if we're in an inheritance subtree (see notes in
     * create_modifytable_plan).  Fortunately we can't be because there would
     * never be aggregates in an UPDATE/DELETE; but let's Assert that.
     */
    Assert(!root->hasInheritedTarget);
    Assert(root->minmax_aggs == NIL);
    root->minmax_aggs = best_path->mmaggregates;

    return plan;
}

/*
 * create_windowagg_plan
 *
 *      Create a WindowAgg plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static WindowAgg *
create_windowagg_plan(PlannerInfo *root, WindowAggPath *best_path)
{
    WindowAgg  *plan;
    WindowClause *wc = best_path->winclause;
    Plan       *subplan;
    List       *tlist;
    int            numsortkeys;
    AttrNumber *sortColIdx;
    Oid           *sortOperators;
    Oid           *collations;
    bool       *nullsFirst;
    int            partNumCols;
    AttrNumber *partColIdx;
    Oid           *partOperators;
    int            ordNumCols;
    AttrNumber *ordColIdx;
    Oid           *ordOperators;

    /*
     * WindowAgg can project, so no need to be terribly picky about child
     * tlist, but we do need grouping columns to be available
     */
    subplan = create_plan_recurse(root, best_path->subpath, CP_LABEL_TLIST);

    tlist = build_path_tlist(root, &best_path->path);

    /*
     * We shouldn't need to actually sort, but it's convenient to use
     * prepare_sort_from_pathkeys to identify the input's sort columns.
     */
    subplan = prepare_sort_from_pathkeys(subplan,
                                         best_path->winpathkeys,
                                         NULL,
                                         NULL,
                                         false,
                                         &numsortkeys,
                                         &sortColIdx,
                                         &sortOperators,
                                         &collations,
                                         &nullsFirst);

    /* Now deconstruct that into partition and ordering portions */
    get_column_info_for_window(root,
                               wc,
                               subplan->targetlist,
                               numsortkeys,
                               sortColIdx,
                               &partNumCols,
                               &partColIdx,
                               &partOperators,
                               &ordNumCols,
                               &ordColIdx,
                               &ordOperators);

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
                          subplan);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}

/*
 * get_column_info_for_window
 *        Get the partitioning/ordering column numbers and equality operators
 *        for a WindowAgg node.
 *
 * This depends on the behavior of planner.c's make_pathkeys_for_window!
 *
 * We are given the target WindowClause and an array of the input column
 * numbers associated with the resulting pathkeys.  In the easy case, there
 * are the same number of pathkey columns as partitioning + ordering columns
 * and we just have to copy some data around.  However, it's possible that
 * some of the original partitioning + ordering columns were eliminated as
 * redundant during the transformation to pathkeys.  (This can happen even
 * though the parser gets rid of obvious duplicates.  A typical scenario is a
 * window specification "PARTITION BY x ORDER BY y" coupled with a clause
 * "WHERE x = y" that causes the two sort columns to be recognized as
 * redundant.)    In that unusual case, we have to work a lot harder to
 * determine which keys are significant.
 *
 * The method used here is a bit brute-force: add the sort columns to a list
 * one at a time and note when the resulting pathkey list gets longer.  But
 * it's a sufficiently uncommon case that a faster way doesn't seem worth
 * the amount of code refactoring that'd be needed.
 */
static void
get_column_info_for_window(PlannerInfo *root, WindowClause *wc, List *tlist,
                           int numSortCols, AttrNumber *sortColIdx,
                           int *partNumCols,
                           AttrNumber **partColIdx,
                           Oid **partOperators,
                           int *ordNumCols,
                           AttrNumber **ordColIdx,
                           Oid **ordOperators)
{
    int            numPart = list_length(wc->partitionClause);
    int            numOrder = list_length(wc->orderClause);

    if (numSortCols == numPart + numOrder)
    {
        /* easy case */
        *partNumCols = numPart;
        *partColIdx = sortColIdx;
        *partOperators = extract_grouping_ops(wc->partitionClause);
        *ordNumCols = numOrder;
        *ordColIdx = sortColIdx + numPart;
        *ordOperators = extract_grouping_ops(wc->orderClause);
    }
    else
    {
        List       *sortclauses;
        List       *pathkeys;
        int            scidx;
        ListCell   *lc;

        /* first, allocate what's certainly enough space for the arrays */
        *partNumCols = 0;
        *partColIdx = (AttrNumber *) palloc(numPart * sizeof(AttrNumber));
        *partOperators = (Oid *) palloc(numPart * sizeof(Oid));
        *ordNumCols = 0;
        *ordColIdx = (AttrNumber *) palloc(numOrder * sizeof(AttrNumber));
        *ordOperators = (Oid *) palloc(numOrder * sizeof(Oid));
        sortclauses = NIL;
        pathkeys = NIL;
        scidx = 0;
        foreach(lc, wc->partitionClause)
        {
            SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
            List       *new_pathkeys;

            sortclauses = lappend(sortclauses, sgc);
            new_pathkeys = make_pathkeys_for_sortclauses(root,
                                                         sortclauses,
                                                         tlist);
            if (list_length(new_pathkeys) > list_length(pathkeys))
            {
                /* this sort clause is actually significant */
                (*partColIdx)[*partNumCols] = sortColIdx[scidx++];
                (*partOperators)[*partNumCols] = sgc->eqop;
                (*partNumCols)++;
                pathkeys = new_pathkeys;
            }
        }
        foreach(lc, wc->orderClause)
        {
            SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
            List       *new_pathkeys;

            sortclauses = lappend(sortclauses, sgc);
            new_pathkeys = make_pathkeys_for_sortclauses(root,
                                                         sortclauses,
                                                         tlist);
            if (list_length(new_pathkeys) > list_length(pathkeys))
            {
                /* this sort clause is actually significant */
                (*ordColIdx)[*ordNumCols] = sortColIdx[scidx++];
                (*ordOperators)[*ordNumCols] = sgc->eqop;
                (*ordNumCols)++;
                pathkeys = new_pathkeys;
            }
        }
        /* complain if we didn't eat exactly the right number of sort cols */
        if (scidx != numSortCols)
            elog(ERROR, "failed to deconstruct sort operators into partitioning/ordering operators");
    }
}

/*
 * create_setop_plan
 *
 *      Create a SetOp plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static SetOp *
create_setop_plan(PlannerInfo *root, SetOpPath *best_path, int flags)
{
    SetOp       *plan;
    Plan       *subplan;
    long        numGroups;

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
 *      Create a RecursiveUnion plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static RecursiveUnion *
create_recursiveunion_plan(PlannerInfo *root, RecursiveUnionPath *best_path)
{
    RecursiveUnion *plan;
    Plan       *leftplan;
    Plan       *rightplan;
    List       *tlist;
    long        numGroups;

    /* Need both children to produce same tlist, so force it */
    leftplan = create_plan_recurse(root, best_path->leftpath, CP_EXACT_TLIST);
    rightplan = create_plan_recurse(root, best_path->rightpath, CP_EXACT_TLIST);

    tlist = build_path_tlist(root, &best_path->path);

    /* Convert numGroups to long int --- but 'ware overflow! */
    numGroups = (long) Min(best_path->numGroups, (double) LONG_MAX);

    plan = make_recursive_union(root,
                                tlist,
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
 *      Create a LockRows plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static LockRows *
create_lockrows_plan(PlannerInfo *root, LockRowsPath *best_path,
                     int flags)
{
    LockRows   *plan;
    Plan       *subplan;

    /* LockRows doesn't project, so tlist requirements pass through */
    subplan = create_plan_recurse(root, best_path->subpath, flags);

    plan = make_lockrows(subplan, best_path->rowMarks, best_path->epqParam);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}

/*
 * create_modifytable_plan
 *      Create a ModifyTable plan for 'best_path'.
 *
 *      Returns a Plan node.
 */
static ModifyTable *
create_modifytable_plan(PlannerInfo *root, ModifyTablePath *best_path)
{// #lizard forgives
    ModifyTable *plan;
    List       *subplans = NIL;
    ListCell   *subpaths,
               *subroots;

#ifdef __AUDIT_FGA__
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt = NULL;
    bool            need_audit_fga_quals = true;
    char            *operation;
    ListCell        *item;
#endif    

    /* Build the plan for each input path */
    forboth(subpaths, best_path->subpaths,
            subroots, best_path->subroots)
    {
        Path       *subpath = (Path *) lfirst(subpaths);
        PlannerInfo *subroot = (PlannerInfo *) lfirst(subroots);
        Plan       *subplan;

        /*
         * In an inherited UPDATE/DELETE, reference the per-child modified
         * subroot while creating Plans from Paths for the child rel.  This is
         * a kluge, but otherwise it's too hard to ensure that Plan creation
         * functions (particularly in FDWs) don't depend on the contents of
         * "root" matching what they saw at Path creation time.  The main
         * downside is that creation functions for Plans that might appear
         * below a ModifyTable cannot expect to modify the contents of "root"
         * and have it "stick" for subsequent processing such as setrefs.c.
         * That's not great, but it seems better than the alternative.
         */
        subplan = create_plan_recurse(subroot, subpath, CP_EXACT_TLIST);

        /* Transfer resname/resjunk labeling, too, to keep executor happy */
        apply_tlist_labeling(subplan->targetlist, subroot->processed_tlist);

        subplans = lappend(subplans, subplan);
    }

    plan = make_modifytable(root,
                            best_path->operation,
                            best_path->canSetTag,
                            best_path->nominalRelation,
                            best_path->partitioned_rels,
							best_path->partColsUpdated,
                            best_path->resultRelations,
                            subplans,
                            best_path->withCheckOptionLists,
                            best_path->returningLists,
                            best_path->rowMarks,
                            best_path->onconflict,
                            best_path->epqParam);
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
	 * If we have unshippable triggers, we have to do DML on coordinators,
	 * generate remote_dml plan now.
     */
	if (root->parse->hasUnshippableTriggers)
	{
		create_remotedml_plan(root, (Plan *)plan, plan->operation);
	}
#endif
    return plan;
}

/*
 * create_limit_plan
 *
 *      Create a Limit plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 */
static Limit *
create_limit_plan(PlannerInfo *root, LimitPath *best_path, int flags,
                  int64 offset_est, int64 count_est)
{
    Limit       *plan;
    Plan       *subplan;

    /* Limit doesn't project, so tlist requirements pass through */
    subplan = create_plan_recurse(root, best_path->subpath, flags);

    plan = make_limit(subplan,
                      best_path->limitOffset,
                      best_path->limitCount,
					  offset_est, count_est,
					  best_path->skipEarlyFinish);

    copy_generic_path_info(&plan->plan, (Path *) best_path);

    return plan;
}


#ifdef XCP
/*
 * adjust_subplan_distribution
 *     Make sure the distribution of the subplan is matching to the consumers.
 */
static void
adjust_subplan_distribution(PlannerInfo *root, Distribution *pathd,
                          Distribution *subd)
{// #lizard forgives
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
            int            node;

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

/*
 * create_remotescan_plan
 *      Create a RemoteSubquery plan for 'best_path' and (recursively) plans
 *      for its subpaths.
 *
 *      Returns a Plan node.
 */
static RemoteSubplan *
create_remotescan_plan(PlannerInfo *root,
                       RemoteSubPath *best_path)
{
    RemoteSubplan  *plan;
    Plan              *subplan;
    Bitmapset         *saverestrict;

    Path           *subpath = best_path->subpath;
    List           *pathkeys = best_path->path.pathkeys;

    int             numsortkeys;
    AttrNumber       *sortColIdx;
    Oid               *sortOperators;
    Oid               *collations;
    bool           *nullsFirst;

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

    /* */
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

    /* Now, insert a Sort node if subplan isn't sufficiently ordered */
    if (!pathkeys_contained_in(pathkeys, subpath->pathkeys))
    {
        subplan = (Plan *) make_sort(subplan, numsortkeys,
                                     sortColIdx, sortOperators,
                                     collations, nullsFirst);

        label_sort_with_costsize(root, (Sort *)subplan, -1.0);
    }

    plan = make_remotesubplan(root, subplan,
                              best_path->path.distribution,
                              best_path->subpath->distribution,
                              best_path->path.pathkeys);

#ifdef __OPENTENBASE__
    if (olap_optimizer)
    {
        plan->scan.plan.startup_cost = ((Path *)best_path)->startup_cost;
        plan->scan.plan.total_cost = ((Path *)best_path)->total_cost;
        plan->scan.plan.plan_rows = ((Path *)best_path)->rows;
        plan->scan.plan.plan_width = ((Path *)best_path)->pathtarget->width;
    }
    else
#endif
    copy_generic_path_info(&plan->scan.plan, (Path *) best_path);

    /* restore current restrict */
    bms_free(root->curOuterRestrict);
    root->curOuterRestrict = saverestrict;

    return plan;
}


static RemoteSubplan *
find_push_down_plan_int(PlannerInfo *root, Plan *plan, bool force, bool delete, Plan **parent)
{// #lizard forgives
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

#endif


/*****************************************************************************
 *
 *    BASE-RELATION SCAN METHODS
 *
 *****************************************************************************/


/*
 * create_seqscan_plan
 *     Returns a seqscan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SeqScan *
create_seqscan_plan(PlannerInfo *root, Path *best_path,
                    List *tlist, List *scan_clauses)
{// #lizard forgives
    SeqScan         *scan_plan;
    Index            scan_relid = best_path->parent->relid;

#ifdef __AUDIT_FGA__
    char            *operation = "select";
    RangeTblEntry   *audit_rte;
    List            *audit_fga_quals_stmt = NULL;
    ListCell        *item;
    bool            need_audit_fga_quals = true;
#endif

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
 *     Returns a samplescan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SampleScan *
create_samplescan_plan(PlannerInfo *root, Path *best_path,
                       List *tlist, List *scan_clauses)
{// #lizard forgives
    SampleScan *scan_plan;
    Index        scan_relid = best_path->parent->relid;
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
 * create_indexscan_plan
 *      Returns an indexscan plan for the base relation scanned by 'best_path'
 *      with restriction clauses 'scan_clauses' and targetlist 'tlist'.
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
{// #lizard forgives
    Scan       *scan_plan;
    List       *indexquals = best_path->indexquals;
    List       *indexorderbys = best_path->indexorderbys;
    Index        baserelid = best_path->path.parent->relid;
    Oid            indexoid = best_path->indexinfo->indexoid;
    List       *qpqual;
    List       *stripped_indexquals;
    List       *fixed_indexquals;
    List       *fixed_indexorderbys;
    List       *indexorderbyops = NIL;
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
            continue;            /* we may drop pseudoconstants here */
        if (list_member_ptr(indexquals, rinfo))
            continue;            /* simple duplicate */
        if (is_redundant_derived_clause(rinfo, indexquals))
            continue;            /* derived from same EquivalenceClass */
        if (!contain_mutable_functions((Node *) rinfo->clause) &&
            predicate_implied_by(list_make1(rinfo->clause), indexquals, false))
            continue;            /* provably implied by indexquals */
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
            Node       *expr = (Node *) lfirst(exprCell);
            Oid            exprtype = exprType(expr);
            Oid            sortop;

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
 *      Returns a bitmap scan plan for the base relation scanned by 'best_path'
 *      with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static BitmapHeapScan *
create_bitmap_scan_plan(PlannerInfo *root,
                        BitmapHeapPath *best_path,
                        List *tlist,
                        List *scan_clauses)
{// #lizard forgives
    Index        baserelid = best_path->path.parent->relid;
    Plan       *bitmapqualplan;
    List       *bitmapqualorig;
    List       *indexquals;
    List       *indexECs;
    List       *qpqual;
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
        Node       *clause = (Node *) rinfo->clause;

        if (rinfo->pseudoconstant)
            continue;            /* we may drop pseudoconstants here */
        if (list_member(indexquals, clause))
            continue;            /* simple duplicate */
        if (rinfo->parent_ec && list_member_ptr(indexECs, rinfo->parent_ec))
            continue;            /* derived from same EquivalenceClass */
        if (!contain_mutable_functions(clause) &&
            predicate_implied_by(list_make1(clause), indexquals, false))
            continue;            /* provably implied by indexquals */
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
{// #lizard forgives
    Plan       *plan;

    if (IsA(bitmapqual, BitmapAndPath))
    {
        BitmapAndPath *apath = (BitmapAndPath *) bitmapqual;
        List       *subplans = NIL;
        List       *subquals = NIL;
        List       *subindexquals = NIL;
        List       *subindexECs = NIL;
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
            Plan       *subplan;
            List       *subqual;
            List       *subindexqual;
            List       *subindexEC;

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
        plan->plan_width = 0;    /* meaningless */
        plan->parallel_aware = false;
        plan->parallel_safe = apath->path.parallel_safe;
        *qual = subquals;
        *indexqual = subindexquals;
        *indexECs = subindexECs;
    }
    else if (IsA(bitmapqual, BitmapOrPath))
    {
        BitmapOrPath *opath = (BitmapOrPath *) bitmapqual;
        List       *subplans = NIL;
        List       *subquals = NIL;
        List       *subindexquals = NIL;
        bool        const_true_subqual = false;
        bool        const_true_subindexqual = false;
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
            Plan       *subplan;
            List       *subqual;
            List       *subindexqual;
            List       *subindexEC;

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
            plan->plan_width = 0;    /* meaningless */
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
        List       *subindexECs;
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
        plan = (Plan *) make_bitmap_indexscan(iscan->scan.scanrelid,
                                              iscan->indexid,
                                              iscan->indexqual,
                                              iscan->indexqualorig);
        /* and set its cost/width fields appropriately */
        plan->startup_cost = 0.0;
        plan->total_cost = ipath->indextotalcost;
        plan->plan_rows =
			clamp_row_est(ipath->indexselectivity * ipath->path.parent->tuples / nodes);
        plan->plan_width = 0;    /* meaningless */
        plan->parallel_aware = false;
        plan->parallel_safe = ipath->path.parallel_safe;
        *qual = get_actual_clauses(ipath->indexclauses);
        *indexqual = get_actual_clauses(ipath->indexquals);
        foreach(l, ipath->indexinfo->indpred)
        {
            Expr       *pred = (Expr *) lfirst(l);

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
        plan = NULL;            /* keep compiler quiet */
    }

    return plan;
}

/*
 * create_tidscan_plan
 *     Returns a tidscan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TidScan *
create_tidscan_plan(PlannerInfo *root, TidPath *best_path,
                    List *tlist, List *scan_clauses)
{// #lizard forgives
    TidScan    *scan_plan;
    Index        scan_relid = best_path->path.parent->relid;
    List       *tidquals = best_path->tidquals;
    List       *ortidquals;

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
 *     Returns a subqueryscan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static SubqueryScan *
create_subqueryscan_plan(PlannerInfo *root, SubqueryScanPath *best_path,
                         List *tlist, List *scan_clauses)
{
    SubqueryScan *scan_plan;
    RelOptInfo *rel = best_path->path.parent;
    Index        scan_relid = rel->relid;
    Plan       *subplan;

    /* it should be a subquery base rel... */
    Assert(scan_relid > 0);
    Assert(rel->rtekind == RTE_SUBQUERY);

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
 *     Returns a functionscan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static FunctionScan *
create_functionscan_plan(PlannerInfo *root, Path *best_path,
                         List *tlist, List *scan_clauses)
{
    FunctionScan *scan_plan;
    Index        scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;
    List       *functions;

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
 *     Returns a tablefuncscan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static TableFuncScan *
create_tablefuncscan_plan(PlannerInfo *root, Path *best_path,
                          List *tlist, List *scan_clauses)
{
    TableFuncScan *scan_plan;
    Index        scan_relid = best_path->parent->relid;
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
 *     Returns a valuesscan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ValuesScan *
create_valuesscan_plan(PlannerInfo *root, Path *best_path,
                       List *tlist, List *scan_clauses)
{
    ValuesScan *scan_plan;
    Index        scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;
    List       *values_lists;

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
 *     Returns a ctescan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static CteScan *
create_ctescan_plan(PlannerInfo *root, Path *best_path,
                    List *tlist, List *scan_clauses)
{// #lizard forgives
    CteScan    *scan_plan;
    Index        scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;
    SubPlan    *ctesplan = NULL;
    int            plan_id;
    int            cte_param_id;
    PlannerInfo *cteroot;
    Index        levelsup;
    int            ndx;
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
        if (!cteroot)            /* shouldn't happen */
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
    if (lc == NULL)                /* shouldn't happen */
        elog(ERROR, "could not find CTE \"%s\"", rte->ctename);
    if (ndx >= list_length(cteroot->cte_plan_ids))
        elog(ERROR, "could not find plan for CTE \"%s\"", rte->ctename);
    plan_id = list_nth_int(cteroot->cte_plan_ids, ndx);
    Assert(plan_id > 0);
    foreach(lc, cteroot->init_plans)
    {
        ctesplan = (SubPlan *) lfirst(lc);
        if (ctesplan->plan_id == plan_id)
            break;
    }
    if (lc == NULL)                /* shouldn't happen */
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
 *     Returns a tuplestorescan plan for the base relation scanned by
 *    'best_path' with restriction clauses 'scan_clauses' and targetlist
 *    'tlist'.
 */
static NamedTuplestoreScan *
create_namedtuplestorescan_plan(PlannerInfo *root, Path *best_path,
                                List *tlist, List *scan_clauses)
{
    NamedTuplestoreScan *scan_plan;
    Index        scan_relid = best_path->parent->relid;
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
 *     Returns a worktablescan plan for the base relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static WorkTableScan *
create_worktablescan_plan(PlannerInfo *root, Path *best_path,
                          List *tlist, List *scan_clauses)
{
    WorkTableScan *scan_plan;
    Index        scan_relid = best_path->parent->relid;
    RangeTblEntry *rte;
    Index        levelsup;
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
    if (levelsup == 0)            /* shouldn't happen */
        elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
    levelsup--;
    cteroot = root;
    while (levelsup-- > 0)
    {
        cteroot = cteroot->parent_root;
        if (!cteroot)            /* shouldn't happen */
            elog(ERROR, "bad levelsup for CTE \"%s\"", rte->ctename);
    }
    if (cteroot->wt_param_id < 0)    /* shouldn't happen */
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
 *     Returns a foreignscan plan for the relation scanned by 'best_path'
 *     with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 */
static ForeignScan *
create_foreignscan_plan(PlannerInfo *root, ForeignPath *best_path,
                        List *tlist, List *scan_clauses)
{// #lizard forgives
    ForeignScan *scan_plan;
    RelOptInfo *rel = best_path->path.parent;
    Index        scan_relid = rel->relid;
    Oid            rel_oid = InvalidOid;
    Plan       *outer_plan = NULL;

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
     * in fdw_scan_tlist.)    This is a bit of a kluge and might go away
     * someday, so we intentionally leave it out of the API presented to FDWs.
     */
    scan_plan->fsSystemCol = false;
    if (scan_relid > 0)
    {
        Bitmapset  *attrs_used = NULL;
        ListCell   *lc;
        int            i;

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
    List       *custom_plans = NIL;
    ListCell   *lc;

    /* Recursively transform child paths. */
    foreach(lc, best_path->custom_paths)
    {
        Plan       *plan = create_plan_recurse(root, (Path *) lfirst(lc),
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
 *    JOIN METHODS
 *
 *****************************************************************************/

static NestLoop *
create_nestloop_plan(PlannerInfo *root,
                     NestPath *best_path)
{// #lizard forgives
    NestLoop   *join_plan;
    Plan       *outer_plan;
    Plan       *inner_plan;
    List       *tlist = build_path_tlist(root, &best_path->path);
    List       *joinrestrictclauses = best_path->joinrestrictinfo;
    List       *joinclauses;
    List       *otherclauses;
    Relids        outerrelids;
    List       *nestParams;
    Relids        saveOuterRels = root->curOuterRels;
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
#ifdef XCP
    /*
     * While NestLoop is executed it rescans inner plan. We do not want to
     * rescan RemoteSubplan and do not support it.
     * So if inner_plan is a RemoteSubplan, materialize it.
     */
#ifdef __OPENTENBASE__
	if (!IsA(inner_plan, Material) && contain_remote_subplan_walker((Node*)inner_plan, NULL, true))
    {
        inner_plan = materialize_top_remote_subplan(inner_plan);
    }
#else
    if (IsA(inner_plan, RemoteSubplan))
    {
        Plan       *matplan = (Plan *) make_material(inner_plan);

        /*
         * We assume the materialize will not spill to disk, and therefore
         * charge just cpu_operator_cost per tuple.  (Keep this estimate in
         * sync with cost_mergejoin.)
         */
        copy_plan_costsize(matplan, inner_plan);
        matplan->total_cost += cpu_operator_cost * matplan->plan_rows;

        inner_plan = matplan;
    }
#endif
#endif

    join_plan = make_nestloop(tlist,
                              joinclauses,
                              otherclauses,
                              nestParams,
                              outer_plan,
                              inner_plan,
                              best_path->jointype,
                              best_path->inner_unique);

    copy_generic_path_info(&join_plan->join.plan, &best_path->path);

    return join_plan;
}

static MergeJoin *
create_mergejoin_plan(PlannerInfo *root,
                      MergePath *best_path)
{// #lizard forgives
    MergeJoin  *join_plan;
    Plan       *outer_plan;
    Plan       *inner_plan;
    List       *tlist = build_path_tlist(root, &best_path->jpath.path);
    List       *joinclauses;
    List       *otherclauses;
    List       *mergeclauses;
    List       *outerpathkeys;
    List       *innerpathkeys;
    int            nClauses;
    Oid           *mergefamilies;
    Oid           *mergecollations;
    int           *mergestrategies;
    bool       *mergenullsfirst;
    int            i;
    ListCell   *lc;
    ListCell   *lop;
    ListCell   *lip;
        Path       *outer_path = best_path->jpath.outerjoinpath;
        Path       *inner_path = best_path->jpath.innerjoinpath;
#ifdef __OPENTENBASE__
    bool       reset = false;

    if (!mergejoin)
    {
        mergejoin = true;
        reset = true;
    }
#endif
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
        Sort       *sort = make_sort_from_pathkeys(outer_plan,
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
        Sort       *sort = make_sort_from_pathkeys(inner_plan,
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
        Plan       *matplan = (Plan *) make_material(inner_plan);

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
                               best_path->skip_mark_restore);

    /* Costs of sort and material steps are included in path cost already */
    copy_generic_path_info(&join_plan->join.plan, &best_path->jpath.path);

#ifdef __OPENTENBASE__
    if (reset)
    {
        mergejoin = false;
    }
#endif
    return join_plan;
}

static HashJoin *
create_hashjoin_plan(PlannerInfo *root,
                     HashPath *best_path)
{// #lizard forgives
    HashJoin   *join_plan;
    Hash       *hash_plan;
    Plan       *outer_plan;
    Plan       *inner_plan;
    List       *tlist = build_path_tlist(root, &best_path->jpath.path);
    List       *joinclauses;
    List       *otherclauses;
    List       *hashclauses;
    Oid            skewTable = InvalidOid;
    AttrNumber    skewColumn = InvalidAttrNumber;
    bool        skewInherit = false;
#ifdef __OPENTENBASE__
    bool       outer_parallel_aware = false;
    bool       hashjoin_parallel_aware = false;
    Plan       *subplan = NULL;
#endif
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
    hashclauses = get_switched_clauses(best_path->path_hashclauses,
                                       best_path->jpath.outerjoinpath->parent->relids);

    /*
     * If there is a single join clause and we can identify the outer variable
     * as a simple column reference, supply its identity for possible use in
     * skew optimization.  (Note: in principle we could do skew optimization
     * with multiple join clauses, but we'd have to be able to determine the
     * most common combinations of outer values, which we don't currently have
     * enough stats for.)
     */
    if (list_length(hashclauses) == 1)
    {
        OpExpr       *clause = (OpExpr *) linitial(hashclauses);
        Node       *node;

        Assert(is_opclause(clause));
        node = (Node *) linitial(clause->args);
        if (IsA(node, RelabelType))
            node = (Node *) ((RelabelType *) node)->arg;
        if (IsA(node, Var))
        {
            Var           *var = (Var *) node;
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
     * Build the hash node and hash join node.
     */
    hash_plan = make_hash(inner_plan,
                          skewTable,
                          skewColumn,
                          skewInherit);

    /*
     * Set Hash node's startup & total costs equal to total cost of input
     * plan; this only affects EXPLAIN display not decisions.
     */
    copy_plan_costsize(&hash_plan->plan, inner_plan);
    hash_plan->plan.startup_cost = hash_plan->plan.total_cost;

#ifdef __OPENTENBASE__
    /*
      * In parallel hashjoin, we need to set hash plan be parallel plan.
         */
    if (IsA(outer_plan, SubqueryScan))
    {
        SubqueryScan *sub = (SubqueryScan *)outer_plan;
        
        subplan = sub->subplan;
    }
    
    if ((outer_plan->parallel_aware ||
        IsA(outer_plan, Gather) || (subplan && subplan->parallel_aware)
        ) && olap_optimizer)
    {
        outer_parallel_aware = true;
        
        if (inner_plan->parallel_aware)
        {
            hash_plan->plan.parallel_aware = true;
            hashjoin_parallel_aware = true;
        }
    }

    
    if (outer_parallel_aware && !hash_plan->plan.parallel_aware)
    {
        if (set_plan_parallel(inner_plan))
        {
            hash_plan->plan.parallel_aware = true;
            hashjoin_parallel_aware = true;
        }
        else
        {
            set_plan_nonparallel(inner_plan);
            set_plan_nonparallel(outer_plan);
            hashjoin_parallel_aware = false;
        }
    }
#endif

    join_plan = make_hashjoin(tlist,
                              joinclauses,
                              otherclauses,
                              hashclauses,
                              outer_plan,
                              (Plan *) hash_plan,
                              best_path->jpath.jointype,
                              best_path->jpath.inner_unique);

    copy_generic_path_info(&join_plan->join.plan, &best_path->jpath.path);

#ifdef __OPENTENBASE__
    if (outer_parallel_aware)
    {
        join_plan->join.plan.parallel_aware = hashjoin_parallel_aware;
    }
#endif

    return join_plan;
}


/*****************************************************************************
 *
 *    SUPPORTING ROUTINES
 *
 *****************************************************************************/

/*
 * replace_nestloop_params
 *      Replace outer-relation Vars and PlaceHolderVars in the given expression
 *      with nestloop Params
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
{// #lizard forgives
    if (node == NULL)
        return NULL;
    if (IsA(node, Var))
    {
        Var           *var = (Var *) node;
        Param       *param;
        NestLoopParam *nlp;
        ListCell   *lc;

        /* Upper-level Vars should be long gone at this point */
        Assert(var->varlevelsup == 0);
        /* If not to be replaced, we can just return the Var unmodified */
        if (!bms_is_member(var->varno, root->curOuterRels))
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
        Param       *param;
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
 *      Handle params of a parameterized subquery that need to be fed
 *      from an outer nestloop.
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
{// #lizard forgives
    ListCell   *ppl;

    foreach(ppl, subplan_params)
    {
        PlannerParamItem *pitem = (PlannerParamItem *) lfirst(ppl);

        if (IsA(pitem->item, Var))
        {
            Var           *var = (Var *) pitem->item;
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
 *      Adjust indexqual clauses to the form the executor's indexqual
 *      machinery needs.
 *
 * We have four tasks here:
 *    * Remove RestrictInfo nodes from the input clauses.
 *    * Replace any outer-relation Var or PHV nodes with nestloop Params.
 *      (XXX eventually, that responsibility should go elsewhere?)
 *    * Index keys must be represented by Var nodes with varattno set to the
 *      index's attribute number, not the attribute number in the original rel.
 *    * If the index key is on the right, commute the clause to put it on the
 *      left.
 *
 * The result is a modified copy of the path's indexquals list --- the
 * original is not changed.  Note also that the copy shares no substructure
 * with the original; this is needed in case there is a subplan in it (we need
 * two separate copies of the subplan tree, or things will go awry).
 */
static List *
fix_indexqual_references(PlannerInfo *root, IndexPath *index_path)
{// #lizard forgives
    IndexOptInfo *index = index_path->indexinfo;
    List       *fixed_indexquals;
    ListCell   *lcc,
               *lci;

    fixed_indexquals = NIL;

    forboth(lcc, index_path->indexquals, lci, index_path->indexqualcols)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lcc);
        int            indexcol = lfirst_int(lci);
        Node       *clause;

        /*
         * Replace any outer-relation variables with nestloop params.
         *
         * This also makes a copy of the clause, so it's safe to modify it
         * in-place below.
         */
        clause = replace_nestloop_params(root, (Node *) rinfo->clause);

        if (IsA(clause, OpExpr))
        {
            OpExpr       *op = (OpExpr *) clause;

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
            Expr       *newrc;
            List       *indexcolnos;
            bool        var_on_left;
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
 *      Adjust indexorderby clauses to the form the executor's index
 *      machinery needs.
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
    List       *fixed_indexorderbys;
    ListCell   *lcc,
               *lci;

    fixed_indexorderbys = NIL;

    forboth(lcc, index_path->indexorderbys, lci, index_path->indexorderbycols)
    {
        Node       *clause = (Node *) lfirst(lcc);
        int            indexcol = lfirst_int(lci);

        /*
         * Replace any outer-relation variables with nestloop params.
         *
         * This also makes a copy of the clause, so it's safe to modify it
         * in-place below.
         */
        clause = replace_nestloop_params(root, clause);

        if (IsA(clause, OpExpr))
        {
            OpExpr       *op = (OpExpr *) clause;

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
 *      Convert an indexqual expression to a Var referencing the index column.
 *
 * We represent index keys by Var nodes having varno == INDEX_VAR and varattno
 * equal to the index's attribute number (index column position).
 *
 * Most of the code here is just for sanity cross-checking that the given
 * expression actually matches the index column it's claimed to.
 */
static Node *
fix_indexqual_operand(Node *node, IndexOptInfo *index, int indexcol)
{// #lizard forgives
    Var           *result;
    int            pos;
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
                Node       *indexkey;

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
    return NULL;                /* keep compiler quiet */
}

/*
 * get_switched_clauses
 *      Given a list of merge or hash joinclauses (as RestrictInfo nodes),
 *      extract the bare clauses, and rearrange the elements within the
 *      clauses, if needed, so the outer join variable is on the left and
 *      the inner is on the right.  The original clause data structure is not
 *      touched; a modified list is returned.  We do, however, set the transient
 *      outer_is_left field in each RestrictInfo to show which side was which.
 */
static List *
get_switched_clauses(List *clauses, Relids outerrelids)
{
    List       *t_list = NIL;
    ListCell   *l;

    foreach(l, clauses)
    {
        RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(l);
        OpExpr       *clause = (OpExpr *) restrictinfo->clause;

        Assert(is_opclause(clause));
        if (bms_is_subset(restrictinfo->right_relids, outerrelids))
        {
            /*
             * Duplicate just enough of the structure to allow commuting the
             * clause without changing the original list.  Could use
             * copyObject, but a complete deep copy is overkill.
             */
            OpExpr       *temp = makeNode(OpExpr);

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
 *        Given a list of qual clauses that will all be evaluated at the same
 *        plan node, sort the list into the order we want to check the quals
 *        in at runtime.
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
{// #lizard forgives
    typedef struct
    {
        Node       *clause;
        Cost        cost;
        Index        security_level;
    } QualItem;
    int            nitems = list_length(clauses);
    QualItem   *items;
    ListCell   *lc;
    int            i;
    List       *result;

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
        Node       *clause = (Node *) lfirst(lc);
        QualCost    qcost;

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
        QualItem    newitem = items[i];
        int            j;

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
static void
copy_generic_path_info(Plan *dest, Path *src)
{
    dest->startup_cost = src->startup_cost;
    dest->total_cost = src->total_cost;
    dest->plan_rows = src->rows;
    dest->plan_width = src->pathtarget->width;
    dest->parallel_aware = src->parallel_aware;
    dest->parallel_safe = src->parallel_safe;
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
    Plan       *lefttree = plan->plan.lefttree;
    Path        sort_path;        /* dummy for result of cost_sort */

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
 *     Set isshared flag in bitmap subplan so that it will be created in
 *     shared memory.
 */
static void
bitmap_subplan_mark_shared(Plan *plan)
{
    if (IsA(plan, BitmapAnd))
        bitmap_subplan_mark_shared(
                                   linitial(((BitmapAnd *) plan)->bitmapplans));
    else if (IsA(plan, BitmapOr))
        ((BitmapOr *) plan)->isshared = true;
    else if (IsA(plan, BitmapIndexScan))
        ((BitmapIndexScan *) plan)->isshared = true;
    else
        elog(ERROR, "unrecognized node type: %d", nodeTag(plan));
}

/*****************************************************************************
 *
 *    PLAN NODE BUILDING ROUTINES
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
    Plan       *plan = &node->plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

    plan->targetlist = NIL;        /* not used */
    plan->qual = NIL;            /* not used */
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
    Plan       *plan = &node->scan.plan;

    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->bitmapqualorig = bitmapqualorig;

    return node;
}

static TidScan *
make_tidscan(List *qptlist,
             List *qpqual,
             Index scanrelid,
             List *tidquals)
{
    TidScan    *node = makeNode(TidScan);
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

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
    Plan       *plan = &node->scan.plan;

    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->scan.scanrelid = scanrelid;
    node->wtParam = wtParam;

    return node;
}

#ifdef XCP
static void
replace_rda_in_expr_recurse(PlannerInfo *root, Expr *node, Plan *parent)
{
    switch (nodeTag(node))
    {
        case T_SubPlan:
        {
            SubPlan             *subplan = (SubPlan *) node;
            Plan                *expr_plan = (Plan *)list_nth(root->glob->subplans, subplan->plan_id - 1);
            PlannerInfo         *expr_root = (PlannerInfo *)list_nth(root->glob->subroots, subplan->plan_id - 1);

            replace_rda_recurse(expr_root, expr_plan, NULL);
            break;
        }
        case T_AlternativeSubPlan:
        {
            ListCell            *lc;
            AlternativeSubPlan  *asplan = (AlternativeSubPlan *) node;

            foreach(lc, asplan->subplans)
            {
                SubPlan         *subplan = lfirst_node(SubPlan, lc);
                Plan            *expr_plan = (Plan *)list_nth(root->glob->subplans, subplan->plan_id - 1);
                PlannerInfo     *expr_root = (PlannerInfo *)list_nth(root->glob->subroots, subplan->plan_id - 1);

                replace_rda_recurse(expr_root, expr_plan, NULL);
            }
            break;
        }
        case T_BoolExpr:
        {
            ListCell   *lc;
            BoolExpr   *boolexpr = (BoolExpr *) node;

            foreach(lc, boolexpr->args)
            {
                Expr	   *arg = (Expr *) lfirst(lc);
                replace_rda_in_expr_recurse(root, arg, parent);
            }
            break;
        }
        case T_FieldSelect:
        {
            FieldSelect *fselect = (FieldSelect *) node;

            replace_rda_in_expr_recurse(root, fselect->arg, parent);
            break;
        }
		case T_RelabelType:
        {
            /* relabel doesn't need to do anything at runtime */
            RelabelType *relabel = (RelabelType *) node;

            replace_rda_in_expr_recurse(root, relabel->arg, parent);
            break;
        }
		case T_ConvertRowtypeExpr:
        {
            ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) node;

			/* evaluate argument into step's result area */
            replace_rda_in_expr_recurse(root, convert->arg, parent);
            break;
        }
		case T_WindowFunc:
		{
			WindowFunc *wfunc = (WindowFunc *) node;
			ListCell   *lc;

			if (parent && IsA(parent, WindowAggState))
			{
				foreach(lc, wfunc->args)
				{
					Expr	   *e = lfirst(lc);
					replace_rda_in_expr_recurse(root, e, parent);
				}
				replace_rda_in_expr_recurse(root, wfunc->aggfilter, parent);
			}
			else
			{
				/* planner messed up */
				elog(ERROR, "WindowFunc found in non-WindowAgg plan node");
			}
			break;
		}
		case T_ArrayRef:
		{
			ListCell   *lc;
			ArrayRef   *aref = (ArrayRef *) node;
			bool	   isAssignment = (aref->refassgnexpr != NULL);
			replace_rda_in_expr_recurse(root, aref->refexpr, parent);
			/* Verify subscript list lengths are within limit */
	        if (list_length(aref->refupperindexpr) > MAXDIM)
	        	ereport(ERROR,
	        			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
	        			 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
	        					list_length(aref->refupperindexpr), MAXDIM)));
        
	        if (list_length(aref->reflowerindexpr) > MAXDIM)
	        	ereport(ERROR,
	        			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
	        			 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
	        					list_length(aref->reflowerindexpr), MAXDIM)));
			foreach(lc, aref->refupperindexpr)
			{
				Expr *e = (Expr *) lfirst(lc);
				if(!e)
				   continue;
				replace_rda_in_expr_recurse(root, e, parent);
			}
			foreach(lc, aref->reflowerindexpr)
			{
				Expr *e = (Expr *) lfirst(lc);
				if(!e)
				   continue;
				replace_rda_in_expr_recurse(root, e, parent);
			}
			if(isAssignment)
			{
				replace_rda_in_expr_recurse(root, aref->refassgnexpr, parent);
			}
			break;
		}
		case T_OpExpr:
		{
			ListCell   *lc;
			OpExpr	   *op = (OpExpr *) node;
			foreach(lc, op->args)
            {
				Expr	   *arg = (Expr *) lfirst(lc);
				if (!IsA(arg, Const))
				{
                    replace_rda_in_expr_recurse(root, arg, parent);
				}
            }
			break;
		}
		case T_FuncExpr:
		{
			ListCell   *lc;
			FuncExpr *func = (FuncExpr *) node;
			foreach(lc, func->args)
            {
				Expr	   *arg = (Expr *) lfirst(lc);
				if (!IsA(arg, Const))
				{
                    replace_rda_in_expr_recurse(root, arg, parent);
				}
            }
			break;
		}
		case T_DistinctExpr:
		{
			ListCell   *lc;
			DistinctExpr *op = (DistinctExpr *) node;
			foreach(lc, op->args)
            {
				Expr	   *arg = (Expr *) lfirst(lc);
				if (!IsA(arg, Const))
				{
                    replace_rda_in_expr_recurse(root, arg, parent);
				}
            }
			break;
		}
		case T_NullIfExpr:
		{
			ListCell   *lc;
			NullIfExpr *op = (NullIfExpr *) node;
			foreach(lc, op->args)
            {
				Expr	   *arg = (Expr *) lfirst(lc);
				if (!IsA(arg, Const))
				{
                    replace_rda_in_expr_recurse(root, arg, parent);
				}
            }
			break;			
		}
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) node;
			Expr	   *scalararg;
            Expr	   *arrayarg;
			Assert(list_length(opexpr->args) == 2);
			scalararg = (Expr *) linitial(opexpr->args);
			arrayarg = (Expr *) lsecond(opexpr->args);
			replace_rda_in_expr_recurse(root, scalararg, parent);
			replace_rda_in_expr_recurse(root, arrayarg, parent);
			break;
		}
		case T_FieldStore:
		{
			ListCell   *l1,*l2;
			FieldStore *fstore = (FieldStore *) node;
			replace_rda_in_expr_recurse(root, fstore->arg, parent);
			/* evaluate new field values, store in workspace columns */
			forboth(l1, fstore->newvals, l2, fstore->fieldnums)
			{
				Expr	   *e = (Expr *) lfirst(l1);
				AttrNumber	fieldnum = lfirst_int(l2);
				if (fieldnum <= 0)
						elog(ERROR, "field number %d is out of range in FieldStore",
							 fieldnum);

				replace_rda_in_expr_recurse(root, e, parent);
			}
			break;
		}
		case T_CoerceViaIO:
		{
			CoerceViaIO *iocoerce = (CoerceViaIO *) node;
			replace_rda_in_expr_recurse(root, iocoerce->arg, parent);
			break;
		}
		case T_ArrayCoerceExpr:
		{
			ArrayCoerceExpr *acoerce = (ArrayCoerceExpr *) node;
			replace_rda_in_expr_recurse(root, acoerce->arg, parent);
			break;
		}
		case T_CaseExpr:
		{
			ListCell   *lc;

			CaseExpr   *caseExpr = (CaseExpr *) node;
			if (caseExpr->arg != NULL)
			{
                replace_rda_in_expr_recurse(root, caseExpr->arg, parent);
			}
			foreach(lc, caseExpr->args)
			{
				CaseWhen   *when = (CaseWhen *) lfirst(lc);
				replace_rda_in_expr_recurse(root, when->expr, parent);
				replace_rda_in_expr_recurse(root, when->result, parent);
			}
			/* transformCaseExpr always adds a default */
			Assert(caseExpr->defresult);
			replace_rda_in_expr_recurse(root, caseExpr->defresult, parent);
		    break;
		}
		case T_ArrayExpr:
		{
			ListCell   *lc;
			ArrayExpr  *arrayexpr = (ArrayExpr *) node;

			foreach(lc, arrayexpr->elements)
			{
				Expr	   *e = (Expr *) lfirst(lc);
				replace_rda_in_expr_recurse(root, e, parent);
			}
			break;
		}
		case T_RowExpr:
		{
			RowExpr    *rowexpr = (RowExpr *) node;
			ListCell   *l;
			foreach(l, rowexpr->args)
			{
				Expr	   *e = (Expr *) lfirst(l);
				replace_rda_in_expr_recurse(root, e, parent);
			}
			break;
		}
		case T_RowCompareExpr:
		{
			RowCompareExpr *rcexpr = (RowCompareExpr *) node;
			int			nopers = list_length(rcexpr->opnos);
			ListCell   *l_left_expr,
					   *l_right_expr;
			int			off;
			
			Assert(list_length(rcexpr->largs) == nopers);
			Assert(list_length(rcexpr->rargs) == nopers);
			Assert(list_length(rcexpr->opfamilies) == nopers);
			Assert(list_length(rcexpr->inputcollids) == nopers);

			off = 0;
			for(off = 0,
				l_left_expr = list_head(rcexpr->largs),
				l_right_expr = list_head(rcexpr->rargs);
				off < nopers;
                off++,
                l_left_expr = lnext(l_left_expr),
                l_right_expr = lnext(l_right_expr))
			{
				Expr	   *left_expr = (Expr *) lfirst(l_left_expr);
				Expr	   *right_expr = (Expr *) lfirst(l_right_expr);
				replace_rda_in_expr_recurse(root, left_expr, parent);
				replace_rda_in_expr_recurse(root, right_expr, parent);
			}
			break;
		}
		case T_CoalesceExpr:
		{
			CoalesceExpr *coalesce = (CoalesceExpr *) node;
			ListCell   *lc;

			/* We assume there's at least one arg */
			Assert(coalesce->args != NIL);

			foreach(lc, coalesce->args)
			{
				Expr	   *e = (Expr *) lfirst(lc);
				replace_rda_in_expr_recurse(root, e, parent);
			}
			break;
		}
		case T_MinMaxExpr:
		{
			MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
			ListCell   *lc;

			foreach(lc, minmaxexpr->args)
			{
				Expr	   *e = (Expr *) lfirst(lc);
				replace_rda_in_expr_recurse(root, e, parent);
			}
			break;			
		}
		case T_XmlExpr:
		{
			XmlExpr    *xexpr = (XmlExpr *) node;
			ListCell   *arg;

			foreach(arg, xexpr->named_args)
			{
				Expr	   *e = (Expr *) lfirst(arg);
				replace_rda_in_expr_recurse(root, e, parent);
			}

			foreach(arg, xexpr->args)
			{
				Expr	   *e = (Expr *) lfirst(arg);
				replace_rda_in_expr_recurse(root, e, parent);
			}
			break;
		}
		case T_NullTest:
		{
			NullTest   *ntest = (NullTest *) node;
			replace_rda_in_expr_recurse(root, ntest->arg, parent);
			break;
		}
		case T_BooleanTest:
		{
			BooleanTest *btest = (BooleanTest *) node;
			replace_rda_in_expr_recurse(root, btest->arg, parent);
			break;
		}
		case T_CoerceToDomain:
		{
			CoerceToDomain *ctest = (CoerceToDomain *) node;
			replace_rda_in_expr_recurse(root, ctest->arg, parent);
			/* TODO: some RDA not do replace  in ExecInitCoerceToDomain*/
			break;
		}
		case T_TargetEntry:
		{
			TargetEntry* tle = (TargetEntry*)node;
			replace_rda_in_expr_recurse(root, tle->expr, parent);
		}
        default:
        {
            elog(DEBUG2, "unrecognized node type: %d", (int) nodeTag(node));
            break;
        }
    }
}

/* replace remotesubplan in expr */
static void
replace_rda_in_expr(PlannerInfo *root, List *qual, Plan *parent)
{
    ListCell   *lc;

    if (qual == NULL)
        return;

    Assert(IsA(qual, List));
    foreach(lc, qual)
    {
        Expr        *node = (Expr *) lfirst(lc);
        replace_rda_in_expr_recurse(root, node, parent);
    }
}

static void
replace_rda_node(PlannerInfo *root, RemoteSubplan *plan, Plan *parent)
{
    Plan       *sub_plan = plan->scan.plan.lefttree;
    Plan       *outerplan = ((Plan *)plan)->lefttree;

    /* Do not replace the top remoteSubplan in plan-tree */
    if (replace_rda_depth > 1)
    {
        /* hack here: the remoteSubplan node is also remoteDataAccess */
        RemoteDataAccess *remote_plan = (RemoteDataAccess *)plan;
        char *old_cursor = remote_plan->cursor;

        remote_plan->scan.plan.type = T_RemoteDataAccess;
        remote_plan->cursor = get_internal_rda_seq();
        if (old_cursor)
            pfree(old_cursor);

        if(outerplan)
        {
            if(IsA(outerplan, Gather))
            {
                Gather *gather = (Gather *) outerplan;
                gather->parallelWorker_sendTuple = false;
            }
        }

    }

    replace_rda_recurse(root, sub_plan, (Plan *)plan);
}

/*
 * replace all remoteSubplan node under the top one.
 * we just re-set the 'plan->type' to T_RemoteDataAccess now, as
 * the remoteDataAccess node has the same elements with remoteSubplan.
 * If we re-asssign a new remoteDataAccess struct, pls make a new plan
 * object node.
 */
static Plan *
replace_rda_recurse(PlannerInfo *root, Plan *plan, Plan *parent)
{
    if (IsA(plan, NestLoop))
        return plan;

    if (IsA(plan, RemoteSubplan) &&
            ((list_length(((RemoteSubplan *) plan)->nodeList) > 1 &&
                        ((RemoteSubplan *) plan)->execOnAll)))
    {
        replace_rda_depth++;
        replace_rda_node(root, (RemoteSubplan *)plan, parent);
        replace_rda_depth--;
    }
    else
    {
        /* recurse the child nodes directly if it's not RemoteSubplan */
        if (plan->lefttree)
            replace_rda_recurse(root, plan->lefttree, plan);
        if (plan->righttree)
            replace_rda_recurse(root, plan->righttree, plan);
			
        // if (plan->qual)
        //     replace_rda_in_expr(root, plan->qual, plan);

        /* subqueryScan case subplan */
        if (IsA(plan, SubqueryScan))
        {
            replace_rda_recurse(root, ((SubqueryScan *)plan)->subplan, NULL);
        }

		// if(IsA(plan, HashJoin))
		// {
		// 	HashJoin *node = (HashJoin *)plan;
		// 	if(node->join.joinqual)
		// 	    replace_rda_in_expr(root, node->join.joinqual, node);
		// 	if(node->hashclauses)
		// 	    replace_rda_in_expr(root, node->hashclauses, node);
		// }

        /* Append node case for subplan */
        if (IsA(plan, Append))
        {
            ListCell *lc;

            foreach(lc, ((Append *)plan)->appendplans)
            {
                Plan *aplan = (Plan *)lfirst(lc);

                replace_rda_recurse(root, aplan, plan);
            }
        }

		/* MergeAppend node case for subplan */
		if (IsA(plan, MergeAppend))
		{
            MergeAppend *mergeappend = (MergeAppend *)plan;            
            ListCell *lc;
            foreach(lc, mergeappend->mergeplans)
            {
                Plan *child_plan = (Plan *)lfirst(lc);
                replace_rda_recurse(root, child_plan, plan);
            }
		}

        /* replace RemoteSubplan in initPlan */
        if (plan->initPlan && root)
        {
            ListCell    *l;
            foreach(l, plan->initPlan)
            {
                SubPlan     *subplan = (SubPlan *) lfirst(l);
                Plan        *initplan = (Plan *)list_nth(root->glob->subplans, subplan->plan_id - 1);
                PlannerInfo *initroot = (PlannerInfo *)list_nth(root->glob->subroots, subplan->plan_id - 1);

                replace_rda_recurse(initroot, initplan, NULL);
            }
        }
    }

    return plan;
}

Plan *
replace_remotesubplan_with_rda(PlannerInfo *root, Plan *plan)
{
    /*
     * The first top 'RemoteSubplan' node will never be replaced,
     * it's safe to set parent as NULL.
     */
    return replace_rda_recurse(root, plan, NULL);
}

Plan *
check_parallel_rda_replace(PlannerInfo *root, Plan *plan)
{
    if((IsA(plan, Gather) || IsA(plan, GatherMerge)) && check_exist_rda_replace(root, plan))
    {
        alter_all_gather_or_merge(root,plan);
    }

    if (plan->lefttree)
        check_parallel_rda_replace(root, plan->lefttree);
    if (plan->righttree)
        check_parallel_rda_replace(root, plan->righttree);

    /* subqueryScan case subplan */
    if (IsA(plan, SubqueryScan))
    {
        check_parallel_rda_replace(root, ((SubqueryScan *)plan)->subplan);
    }

    /* Append node case for subplan */
    if (IsA(plan, Append))
    {
        ListCell *lc;
        foreach(lc, ((Append *)plan)->appendplans)
        {
            Plan *aplan = (Plan *)lfirst(lc);
            check_parallel_rda_replace(root, aplan);
        }
    }
	/* MergeAppend node case for subplan */
	if (IsA(plan, MergeAppend))
	{
        MergeAppend *mergeappend = (MergeAppend *)plan;            
        ListCell *lc;
        foreach(lc, mergeappend->mergeplans)
        {
            Plan *child_plan = (Plan *)lfirst(lc);
            check_parallel_rda_replace(root, child_plan);
        }
	}
    /* replace RemoteSubplan in initPlan */
    if (plan->initPlan && root)
    {
        ListCell    *l;
        foreach(l, plan->initPlan)
        {
            SubPlan     *subplan = (SubPlan *) lfirst(l);
            Plan        *initplan = (Plan *)list_nth(root->glob->subplans, subplan->plan_id - 1);
            PlannerInfo *initroot = (PlannerInfo *)list_nth(root->glob->subroots, subplan->plan_id - 1);
            check_parallel_rda_replace(initroot, initplan);
        }
    }

    return plan;
}

bool
check_exist_rda_replace(PlannerInfo *root, Plan *plan)
{
    if(IsA(plan, RemoteDataAccess))
        return true;

    if(plan->lefttree)
    {
        if(check_exist_rda_replace(root, plan->lefttree))
            return true;
    }

    if(plan->righttree)
    {
        if(check_exist_rda_replace(root, plan->righttree))
            return true;
    }

    /* subqueryScan case subplan */
    if (IsA(plan, SubqueryScan))
    {
        if(check_exist_rda_replace(root, ((SubqueryScan *)plan)->subplan))
            return true;
    }

    /* Append node case for subplan */
    if (IsA(plan, Append))
    {
        ListCell *lc;
        foreach(lc, ((Append *)plan)->appendplans)
        {
            Plan *aplan = (Plan *)lfirst(lc);
            if(check_exist_rda_replace(root, aplan))
                return true;
        }
    }
	/* MergeAppend node case for subplan */
	if (IsA(plan, MergeAppend))
	{
        MergeAppend *mergeappend = (MergeAppend *)plan;            
        ListCell *lc;
        foreach(lc, mergeappend->mergeplans)
        {
            Plan *child_plan = (Plan *)lfirst(lc);
            if(check_exist_rda_replace(root, child_plan))
                return true;
        }
	}
    /* replace RemoteSubplan in initPlan */
    if (plan->initPlan && root)
    {
        ListCell    *l;
        foreach(l, plan->initPlan)
        {
            SubPlan     *subplan = (SubPlan *) lfirst(l);
            Plan        *initplan = (Plan *)list_nth(root->glob->subplans, subplan->plan_id - 1);
            PlannerInfo *initroot = (PlannerInfo *)list_nth(root->glob->subroots, subplan->plan_id - 1);
            if(check_exist_rda_replace(initroot, initplan))
                return true;
        }
    }

    return false;
}

void
alter_all_gather_or_merge(PlannerInfo *root, Plan *plan)
{
    if(IsA(plan, Gather))
    {
        Gather *gather = (Gather *)plan;
        gather->num_workers = 0;
    }
    else if(IsA(plan, GatherMerge))
    {
        GatherMerge *gatherMerge = (GatherMerge *)plan;
        gatherMerge->num_workers = 0;
    }
}

/*
 * make_remotesubplan
 *     Create a RemoteSubplan node to execute subplan on remote nodes.
 *  leftree - the subplan which we want to push down to remote node.
 *  resultDistribution - the distribution of the remote result. May be NULL -
 * results are coming to the invoking node
 *  execDistribution - determines how source data of the subplan are
 * distributed, where we should send the subplan and how combine results.
 *    pathkeys - the remote subplan is sorted according to these keys, executor
 *         should perform merge sort of incoming tuples
 */
RemoteSubplan *
make_remotesubplan(PlannerInfo *root,
                   Plan *lefttree,
                   Distribution *resultDistribution,
                   Distribution *execDistribution,
                   List *pathkeys)
{// #lizard forgives
    RemoteSubplan *node = makeNode(RemoteSubplan);
    Plan       *plan = &node->scan.plan;
    Bitmapset  *tmpset;
    int            nodenum;
#ifdef __OPENTENBASE__
    char distributionType = resultDistribution ? resultDistribution->distributionType :
                                                 LOCATOR_TYPE_NONE;
    Plan *gather_left = lefttree;
    Plan *gather_parent = NULL;
    bool need_sort = true;
	double nodes = 1;
#endif

    /* Sanity checks */
    Assert(!equal(resultDistribution, execDistribution));
    Assert(!IsA(lefttree, RemoteSubplan));

#ifdef __OPENTENBASE__
	/* do things like path_count_datanodes, but we have only distribution here */
	if (execDistribution &&
	    (execDistribution->distributionType == LOCATOR_TYPE_HASH ||
	     execDistribution->distributionType == LOCATOR_TYPE_SHARD))
	{
		nodes = bms_num_members(execDistribution->nodes);
		if (nodes <= 0)
			/* should not happen, but for safety */
			nodes = 1;
	}
	
	if((IsA(lefttree, HashJoin) || IsA(lefttree, NestLoop) || IsA(lefttree, SeqScan) 
        || IsA(lefttree, Agg) || IsA(lefttree, Group) ||
        IsA(lefttree, Sort) || IsA(lefttree, Limit) || IsA(lefttree, Gather)) && 
        max_parallel_workers_per_gather && root->glob->parallelModeOK &&
        olap_optimizer && !mergejoin && is_parallel_safe(root, (Node *)root->parse) &&
        (distributionType == LOCATOR_TYPE_HASH ||
         distributionType == LOCATOR_TYPE_NONE ||
         distributionType == LOCATOR_TYPE_SHARD))
    {
        if (IsA(lefttree, Gather))
        {
            Gather *gather = (Gather *)lefttree;
            int nWorkers = gather->num_workers;
            Plan *leftplan = lefttree->lefttree;
            /* if contain nonparallel hashjoin, set num_workers to 1 */
            bool contain_nonparallel_hashjoin = contain_node_walker(leftplan, T_HashJoin, true);
            if (contain_nonparallel_hashjoin)
            {
                gather->num_workers = 1;
            }
            else
            {
			/* rows estimate is cut down to per data nodes, set it to all nodes for parallel estimate. */
			double rows = GetPlanRows(leftplan) * nodes;
            int    heap_parallel_threshold = 0;
            int    heap_parallel_workers = 1;
                bool contain_gather = contain_node_walker(leftplan, T_Gather, false);

			heap_parallel_threshold = Max(min_parallel_rows_size, 1);
            while (rows >= (heap_parallel_threshold * 3))
            {
                heap_parallel_workers++;
                heap_parallel_threshold *= 3;
                if (heap_parallel_threshold > INT_MAX / 3)
                    break;        /* avoid overflow */
            }

            heap_parallel_workers = Min(heap_parallel_workers, max_parallel_workers_per_gather);
                heap_parallel_workers = Max(heap_parallel_workers, nWorkers);
                /* if contain gather, need compare the workers with min_workers_of_hashjon_gather */
                gather->num_workers = (contain_gather) ? Min(heap_parallel_workers, min_workers_of_hashjon_gather) : heap_parallel_workers;
            }
        }
        else
        {
            /*
              * calculation of parallel workers is from compute_parallel_worker
              */
            double outer_rows              = lefttree->lefttree ? lefttree->lefttree->plan_rows : lefttree->plan_rows;
            double inner_rows              = lefttree->righttree ? lefttree->righttree->plan_rows : 0;
            double rows                    = outer_rows > inner_rows ? 
                                             outer_rows : inner_rows;
            bool contain_nonparallel_hashjoin = contain_node_walker(lefttree, T_HashJoin, true);
            bool   need_parallel           = true;
            int parallel_workers           = 0;

            /* if contain nonparallel hashjoin, don't add gather plan */
			if (contain_nonparallel_hashjoin)
            {
                need_parallel = false;
            }

            /* only add gather to remote_subplan at top */
            if (need_parallel && distributionType == LOCATOR_TYPE_NONE)
            {
                if (remote_subplan_depth > 1)
                    need_parallel = false;
            }

            if (need_parallel)
            {
                /* make decision whether we indeed need parallel execution or not ? */
                switch(nodeTag(lefttree))
                {
                    case T_SeqScan:
						if (rows >= min_parallel_rows_size * 3)
                        {
                            lefttree->parallel_aware = true;
                        }
                        break;
                    case T_HashJoin:
                        if (!lefttree->parallel_aware)
                        {
                            need_parallel = false;
                        }
                        break;
                    case T_Agg:
                        if (!lefttree->parallel_aware)
                        {
                            Agg *node = (Agg *)lefttree;

                            /* do not parallel if it's not safe */
							if (node->aggsplit == AGGSPLIT_INITIAL_SERIAL
							    && lefttree->parallel_safe)
                            {
                                switch(node->aggstrategy)
                                {
                                    case AGG_PLAIN:
                                    case AGG_HASHED:
                                        if (IsA(lefttree->lefttree, SubqueryScan))
                                        {
                                            SubqueryScan *subscan = (SubqueryScan *)lefttree->lefttree;

                                            if (!subscan->subplan->parallel_aware)
                                            {
                                                need_parallel = false;
                                            }
                                        }
                                        else
                                        {
                                            if (!lefttree->lefttree->parallel_aware)
                                            {
                                                need_parallel = false;
                                            }
                                        }
                                        break;
                                    case AGG_SORTED:
                                        {
                                            Plan *lplan = NULL;
                                            
                                            if (IsA(lefttree->lefttree, Sort))
                                            {
                                                lplan = lefttree->lefttree->lefttree;
                                            }
                                            else
                                            {
                                                lplan = lefttree->lefttree;
                                            }

                                            if (IsA(lplan, SubqueryScan))
                                            {
                                                SubqueryScan *subscan = (SubqueryScan *)lplan;

                                                if (!subscan->subplan->parallel_aware)
                                                {
                                                    need_parallel = false;
                                                }
                                            }
                                            else
                                            {
                                                if (!lplan->parallel_aware)
                                                {
                                                    need_parallel = false;
                                                }
                                            }
                                        }
                                        break;
                                    default:
                                        need_parallel = false;
                                        break;
                                }
                            }
                            else
                            {
                                need_parallel = false;
                            }
                        }
                        break;
                    case T_Group:
                        if (!lefttree->parallel_aware)
                        {
                            need_parallel = false;
                        }
                        break;
                    case T_Limit:
                        {
                            Plan *left = lefttree->lefttree;
                            gather_parent = lefttree;

                            if (nodeTag(left) == T_Sort)
                            {
                                gather_parent = left;
                                left = left->lefttree;
                                need_sort = false;
                            }

                            if (!left->parallel_aware)
                            {
                                if (nodeTag(left) == T_Agg)
                                {
                                    gather_left = left;
                                    
                                    left = left->lefttree;

                                    if (IsA(left, SubqueryScan))
                                    {
                                        SubqueryScan *subscan = (SubqueryScan *)left;

                                        left = subscan->subplan;
                                    }

                                    if (!left->parallel_aware)
                                        need_parallel = false;
                                    else
                                    {
                                        rows = left->lefttree->plan_rows;
                                    }
                                }
                                else
                                    need_parallel = false;
                            }
                            else
                            {
                                if (nodeTag(left) == T_Agg)
                                {
                                    rows = left->lefttree->plan_rows;
                                }

                                gather_left = left;
                            }
                        }
                        break;
                    case T_Sort:
                        {
                            Plan *left = lefttree->lefttree;
                            gather_parent = lefttree;
                            need_sort = false;

                            if (!left->parallel_aware)
                            {
                                if (nodeTag(left) == T_Agg)
                                {
                                    gather_left = left;
                                    
                                    left = left->lefttree;

                                    if (IsA(left, SubqueryScan))
                                    {
                                        SubqueryScan *subscan = (SubqueryScan *)left;

                                        left = subscan->subplan;
                                    }

                                    if (!left->parallel_aware)
                                        need_parallel = false;
                                    else
                                    {
                                        rows = left->lefttree->plan_rows;
                                    }
                                }
                                else
                                    need_parallel = false;
                            }
                            else
                            {
                                if (nodeTag(left) == T_Agg)
                                {
                                    rows = left->lefttree->plan_rows;
                                }

                                gather_left = left;
                            }
                        }
                        break;
                    default:
                        if (!lefttree->parallel_aware)
                        {
                            need_parallel = false;
                        }
                        break;
                }
            }

			/* rows estimate is cut down to per data nodes, set it to all nodes for parallel estimate. */
			rows *= nodes;
			if (rows < min_parallel_rows_size * 3)
                need_parallel = false;

            if (need_parallel)
            {
                int            heap_parallel_threshold;
                int            heap_parallel_workers = 1;
                Gather       *gather_plan           = NULL;
                Plan       *subplan               = NULL;

				heap_parallel_threshold = Max(min_parallel_rows_size, 1);
                while (rows >= (heap_parallel_threshold * 3))
                {
                    heap_parallel_workers++;
                    heap_parallel_threshold *= 3;
                    if (heap_parallel_threshold > INT_MAX / 3)
                        break;        /* avoid overflow */
                }

                parallel_workers = heap_parallel_workers;

                parallel_workers = Min(parallel_workers, max_parallel_workers_per_gather);
				/* launched parallel workers must less than hashjoin's parallel workers under it */
                parallel_workers = Min(parallel_workers, min_workers_of_hashjon_gather);
                
				gather_plan = make_gather(copyObject(gather_left->targetlist),
                                          NIL,
                                          parallel_workers,
                                          false,
                                          gather_left);

                gather_plan->plan.startup_cost = gather_left->startup_cost;
                gather_plan->plan.total_cost = gather_left->total_cost;
                gather_plan->plan.plan_rows = gather_left->plan_rows * parallel_workers;
                gather_plan->plan.plan_width = gather_left->plan_width;
                gather_plan->plan.parallel_aware = false;
                gather_plan->plan.parallel_safe = gather_left->parallel_safe;

                root->glob->parallelModeNeeded = true;

                subplan = (Plan *)gather_plan;

                /* need sort */
                if (distributionType == LOCATOR_TYPE_NONE && pathkeys && need_sort)
                {
					subplan = (Plan *)make_sort_from_pathkeys(subplan, pathkeys, NULL);

                    subplan->startup_cost = gather_plan->plan.startup_cost;
                    subplan->total_cost = gather_plan->plan.total_cost;
                    subplan->plan_rows = gather_plan->plan.plan_rows;
                    subplan->plan_width = gather_plan->plan.plan_width;
                }
                
                if (gather_parent)
                    gather_parent->lefttree = (Plan *)subplan;
                else
                    lefttree = (Plan *)subplan;
            }
        }
    }
    min_workers_of_hashjon_gather = PG_INT32_MAX;
#endif

    if (resultDistribution)
    {
        node->distributionType = resultDistribution->distributionType;
        node->distributionKey = InvalidAttrNumber;
        if (resultDistribution->distributionExpr)
        {
            ListCell   *lc;
            Expr       *expr;

            /* XXX Is that correct to reference a column of different type? */
            if (IsA(resultDistribution->distributionExpr, RelabelType))
                expr = ((RelabelType *) resultDistribution->distributionExpr)->arg;
            else
                expr = (Expr *) resultDistribution->distributionExpr;

            /* Find distribution expression in the target list */
            foreach(lc, lefttree->targetlist)
            {
                TargetEntry *tle = (TargetEntry *) lfirst(lc);

                if (equal(tle->expr, expr))
                {
                    node->distributionKey = tle->resno;
                    break;
                }
            }

            if (node->distributionKey == InvalidAttrNumber)
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
#ifdef __OPENTENBASE__
					if (IsA(lefttree, Gather)&& g_UseDataPump && olap_optimizer &&
						(distributionType == LOCATOR_TYPE_HASH || distributionType == LOCATOR_TYPE_SHARD))
					{
						Plan *leftchild = lefttree->lefttree;

						newtle = makeTargetEntry(expr,
										 list_length(leftchild->targetlist) + 1,
									     NULL,
										 true);
					
						if (is_projection_capable_plan(leftchild))
						{
							leftchild->targetlist = lappend(leftchild->targetlist, newtle);
						}
						else
						{
							List *newtlist = list_copy(leftchild->targetlist);
							newtlist = lappend(newtlist, newtle);
							leftchild = (Plan *) make_result(newtlist, NULL, leftchild, NULL);
							lefttree->lefttree = leftchild;
						}
					}
#endif
                }
                else
                {
                    /* Use Result node to calculate expression */
                    List *newtlist = list_copy(lefttree->targetlist);
                    newtlist = lappend(newtlist, newtle);
					lefttree = (Plan *) make_result(newtlist, NULL, lefttree, NULL);
                }

                node->distributionKey = newtle->resno;
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
    }
    else
    {
        node->distributionType = LOCATOR_TYPE_NONE;
        node->distributionKey = InvalidAttrNumber;
        node->distributionNodes = NIL;
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
        if(resultDistribution)
        {
            if (!bms_is_empty(resultDistribution->restrictNodes))
                tmpset = bms_copy(resultDistribution->restrictNodes);
            else
                tmpset = bms_copy(resultDistribution->nodes);
        }
        /*
         * If result goes on single node execute subplan locally
         */
        if (bms_num_members(tmpset) > 1)
        {
            /* get one execution node TODO: load balancing */
            nodenum = bms_any_member(tmpset);
            node->nodeList = list_make1_int(nodenum);
            node->execOnAll = true;
        }
        else
        {
            node->nodeList = NIL;
            node->execOnAll = false;
        }
        bms_free(tmpset);
    }

    /* We do not need to merge sort if only one node is yielding tuples */
    if (pathkeys && node->execOnAll && list_length(node->nodeList) > 1)
    {
        List       *tlist = lefttree->targetlist;
        ListCell   *i;
        int            numsortkeys;
        AttrNumber *sortColIdx;
        Oid           *sortOperators;
        Oid           *collations;
        bool       *nullsFirst;

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
            TargetEntry *tle = NULL;
            Oid            pk_datatype = InvalidOid;
            Oid            sortop;
            ListCell   *j;

            if (ec->ec_has_volatile)
            {
                /*
                 * If the pathkey's EquivalenceClass is volatile, then it must
                 * have come from an ORDER BY clause, and we have to match it to
                 * that same targetlist entry.
                 */
                if (ec->ec_sortref == 0)    /* can't happen */
                    elog(ERROR, "volatile EquivalenceClass has no sortref");
                tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
                Assert(tle);
                Assert(list_length(ec->ec_members) == 1);
                pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
            }
            else
            {
                /*
                 * Otherwise, we can sort by any non-constant expression listed in
                 * the pathkey's EquivalenceClass.  For now, we take the first one
                 * that corresponds to an available item in the tlist.    If there
                 * isn't any, use the first one that is an expression in the
                 * input's vars.  (The non-const restriction only matters if the
                 * EC is below_outer_join; but if it isn't, it won't contain
                 * consts anyway, else we'd have discarded the pathkey as
                 * redundant.)
                 *
                 * XXX if we have a choice, is there any way of figuring out which
                 * might be cheapest to execute?  (For example, int4lt is likely
                 * much cheaper to execute than numericlt, but both might appear
                 * in the same equivalence class...)  Not clear that we ever will
                 * have an interesting choice in practice, so it may not matter.
                 */
                foreach(j, ec->ec_members)
                {
                    EquivalenceMember *em = (EquivalenceMember *) lfirst(j);

                    if (em->em_is_const)
                        continue;

                    tle = tlist_member(em->em_expr, tlist);
                    if (tle)
                    {
                        pk_datatype = em->em_datatype;
                        break;        /* found expr already in tlist */
                    }

                    /*
                     * We can also use it if the pathkey expression is a relabel
                     * of the tlist entry, or vice versa.  This is needed for
                     * binary-compatible cases (cf. make_pathkey_from_sortinfo).
                     * We prefer an exact match, though, so we do the basic search
                     * first.
                     */
                    tle = tlist_member_ignore_relabel(em->em_expr, tlist);
                    if (tle)
                    {
                        pk_datatype = em->em_datatype;
                        break;        /* found expr already in tlist */
                    }
                }

                if (!tle)
                {
                    /* No matching tlist item; look for a computable expression */
                    Expr       *sortexpr = NULL;

                    foreach(j, ec->ec_members)
                    {
                        EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
                        List       *exprvars;
                        ListCell   *k;

                        if (em->em_is_const)
                            continue;
                        sortexpr = em->em_expr;
                        exprvars = pull_var_clause((Node *) sortexpr,
                                                   PVC_INCLUDE_AGGREGATES |
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
                            break;    /* found usable expression */
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
						lefttree = (Plan *) make_result(tlist, NULL, lefttree, NULL);
                    }

                    /*
                     * Add resjunk entry to input's tlist
                     */
                    tle = makeTargetEntry(sortexpr,
                                          list_length(tlist) + 1,
                                          NULL,
                                          true);
                    tlist = lappend(tlist, tle);
                    lefttree->targetlist = tlist;    /* just in case NIL before */
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
            if (!OidIsValid(sortop))    /* should not happen */
                elog(ERROR, "could not find member %d(%u,%u) of opfamily %u",
                     pathkey->pk_strategy, pk_datatype, pk_datatype,
                     pathkey->pk_opfamily);

            /*
             * The column might already be selected as a sort key, if the pathkeys
             * contain duplicate entries.  (This can happen in scenarios where
             * multiple mergejoinable clauses mention the same var, for example.)
             * So enter it only once in the sort arrays.
             */
            numsortkeys = add_sort_column(tle->resno,
                                          sortop,
                                          pathkey->pk_eclass->ec_collation,
                                          pathkey->pk_nulls_first,
                                          numsortkeys,
                                          sortColIdx, sortOperators,
                                          collations, nullsFirst);
        }
        Assert(numsortkeys > 0);

        node->sort = makeNode(SimpleSort);
        node->sort->numCols = numsortkeys;
        node->sort->sortColIdx = sortColIdx;
        node->sort->sortOperators = sortOperators;
        node->sort->sortCollations = collations;
        node->sort->nullsFirst = nullsFirst;
    }

    plan->qual = NIL;
    plan->targetlist = lefttree->targetlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;
    copy_plan_costsize(plan, lefttree);

    node->cursor = get_internal_cursor();
    node->unique = 0;

#ifdef __OPENTENBASE__
    /* 
      * if gather node is under remotesubplan, parallel workers can send tuples directly
      * without gather motion to speed up the data transfering.
      */
    if ((distributionType == LOCATOR_TYPE_HASH || distributionType == LOCATOR_TYPE_SHARD) 
		&& IsA(lefttree, Gather) && g_UseDataPump && olap_optimizer && list_length(node->distributionRestrict) > 1)
    {
        Gather *gather_plan = (Gather *)lefttree;
        
        node->parallelWorkerSendTuple         = true;
        gather_plan->parallelWorker_sendTuple = true;
    }

	if ((IsA(lefttree, Gather) || lefttree->parallel_aware || child_of_gather) &&
        olap_optimizer)
    {
        plan->parallel_aware = true;
        plan->parallel_safe  = true;
    }
#endif

    return node;
}
#endif /* XCP */


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

    Plan       *plan = &node->scan.plan;

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
make_append(List *appendplans, List *tlist, List *partitioned_rels)
{
    Append       *node = makeNode(Append);
    Plan       *plan = &node->plan;

    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    node->partitioned_rels = partitioned_rels;
    node->appendplans = appendplans;

    return node;
}

static RecursiveUnion *
make_recursive_union(PlannerInfo *root,
                     List *tlist,
                     Plan *lefttree,
                     Plan *righttree,
                     int wtParam,
                     List *distinctList,
                     long numGroups)
{
    RecursiveUnion *node = makeNode(RecursiveUnion);
    Plan       *plan = &node->plan;
    int            numCols = list_length(distinctList);

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
        int            keyno = 0;
        AttrNumber *dupColIdx;
        Oid           *dupOperators;
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
    Plan       *plan = &node->plan;

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
    Plan       *plan = &node->plan;

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
    Plan       *plan = &node->join.plan;

    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;
    node->join.jointype = jointype;
    node->join.inner_unique = inner_unique;
    node->join.joinqual = joinclauses;
    node->nestParams = nestParams;
    
#ifdef __OPENTENBASE__
    /* we prefetch the inner plan data to avoid deadlock */
    if (contain_remote_subplan_walker((Node*)lefttree, NULL, true))
    {        
        if (contain_remote_subplan_walker((Node*)righttree, NULL, true))
        {
            node->join.prefetch_inner = true;
        }
    }
#endif
    return node;
}

static HashJoin *
make_hashjoin(List *tlist,
              List *joinclauses,
              List *otherclauses,
              List *hashclauses,
              Plan *lefttree,
              Plan *righttree,
              JoinType jointype,
              bool inner_unique)
{
    HashJoin   *node = makeNode(HashJoin);
    Plan       *plan = &node->join.plan;

    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;
    node->hashclauses = hashclauses;
    node->join.jointype = jointype;
    node->join.inner_unique = inner_unique;
    node->join.joinqual = joinclauses;
#ifdef __OPENTENBASE__
    /* we prefetch the inner plan data to avoid deadlock */
    if (contain_remote_subplan_walker((Node*)lefttree, NULL, true))
    {
        node->join.prefetch_inner = true;
    }
#endif
    return node;
}

static Hash *
make_hash(Plan *lefttree,
          Oid skewTable,
          AttrNumber skewColumn,
          bool skewInherit)
{
    Hash       *node = makeNode(Hash);
    Plan       *plan = &node->plan;

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

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
               bool skip_mark_restore)
{
    MergeJoin  *node = makeNode(MergeJoin);
    Plan       *plan = &node->join.plan;

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

    
#ifdef __OPENTENBASE__
    /* we prefetch the inner plan data to avoid deadlock */
    if (contain_remote_subplan_walker((Node*)lefttree, NULL, true))
    {        
        if (contain_remote_subplan_walker((Node*)righttree, NULL, true))
        {
            node->join.prefetch_inner = true;
        }
    }
#endif
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
    Sort       *node = makeNode(Sort);
    Plan       *plan = &node->plan;

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
 * add_sort_column --- utility subroutine for building sort info arrays
 *
 * We need this routine because the same column might be selected more than
 * once as a sort key column; if so, the extra mentions are redundant.
 *
 * Caller is assumed to have allocated the arrays large enough for the
 * max possible number of columns.    Return value is the new column count.
 */
static int
add_sort_column(AttrNumber colIdx, Oid sortOp, Oid coll, bool nulls_first,
                int numCols, AttrNumber *sortColIdx,
                Oid *sortOperators, Oid *collations, bool *nullsFirst)
{
    int            i;

    Assert(OidIsValid(sortOp));

    for (i = 0; i < numCols; i++)
    {
        /*
         * Note: we check sortOp because it's conceivable that "ORDER BY foo
         * USING <, foo USING <<<" is not redundant, if <<< distinguishes
         * values that < considers equal.  We need not check nulls_first
         * however because a lower-order column with the same sortop but
         * opposite nulls direction is redundant.
         *
         * We could probably consider sort keys with the same sortop and
         * different collations to be redundant too, but for the moment treat
         * them as not redundant.  This will be needed if we ever support
         * collations with different notions of equality.
         */
        if (sortColIdx[i] == colIdx &&
            sortOperators[i] == sortOp &&
            collations[i] == coll)
        {
            /* Already sorting by this col, so extra sort key is useless */
            return numCols;
        }
    }

    /* Add the column */
    sortColIdx[numCols] = colIdx;
    sortOperators[numCols] = sortOp;
    collations[numCols] = coll;
    nullsFirst[numCols] = nulls_first;
    return numCols + 1;
}


/*
 * prepare_sort_from_pathkeys
 *      Prepare to sort according to given pathkeys
 *
 * This is used to set up for Sort, MergeAppend, and Gather Merge nodes.  It
 * calculates the executor's representation of the sort key information, and
 * adjusts the plan targetlist if needed to add resjunk sort columns.
 *
 * Input parameters:
 *      'lefttree' is the plan node which yields input tuples
 *      'pathkeys' is the list of pathkeys by which the result is to be sorted
 *      'relids' identifies the child relation being sorted, if any
 *      'reqColIdx' is NULL or an array of required sort key column numbers
 *      'adjust_tlist_in_place' is TRUE if lefttree must be modified in-place
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
{// #lizard forgives
    List       *tlist = lefttree->targetlist;
    ListCell   *i;
    int            numsortkeys;
    AttrNumber *sortColIdx;
    Oid           *sortOperators;
    Oid           *collations;
    bool       *nullsFirst;

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
        Oid            pk_datatype = InvalidOid;
        Oid            sortop;
        ListCell   *j;

        if (ec->ec_has_volatile)
        {
            /*
             * If the pathkey's EquivalenceClass is volatile, then it must
             * have come from an ORDER BY clause, and we have to match it to
             * that same targetlist entry.
             */
            if (ec->ec_sortref == 0)    /* can't happen */
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
            Expr       *sortexpr = NULL;

            foreach(j, ec->ec_members)
            {
                EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
                List       *exprvars;
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
                    break;        /* found usable expression */
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
            lefttree->targetlist = tlist;    /* just in case NIL before */
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
        if (!OidIsValid(sortop))    /* should not happen */
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
 *        Locate an EquivalenceClass member matching the given TLE, if any
 *
 * Child EC members are ignored unless they belong to given 'relids'.
 */
static EquivalenceMember *
find_ec_member_for_tle(EquivalenceClass *ec,
                       TargetEntry *tle,
                       Relids relids)
{// #lizard forgives
    Expr       *tlexpr;
    ListCell   *lc;

    /* We ignore binary-compatible relabeling on both ends */
    tlexpr = tle->expr;
    while (tlexpr && IsA(tlexpr, RelabelType))
        tlexpr = ((RelabelType *) tlexpr)->arg;

    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
        Expr       *emexpr;

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
 *      Create sort plan to sort according to given pathkeys
 *
 *      'lefttree' is the node which yields input tuples
 *      'pathkeys' is the list of pathkeys by which the result is to be sorted
 *	  'relids' is the set of relations required by prepare_sort_from_pathkeys()
 */
static Sort *
make_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids)
{
    int            numsortkeys;
    AttrNumber *sortColIdx;
    Oid           *sortOperators;
    Oid           *collations;
    bool       *nullsFirst;

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
 *      Create sort plan to sort according to given sortclauses
 *
 *      'sortcls' is a list of SortGroupClauses
 *      'lefttree' is the node which yields input tuples
 */
Sort *
make_sort_from_sortclauses(List *sortcls, Plan *lefttree)
{
    List       *sub_tlist = lefttree->targetlist;
    ListCell   *l;
    int            numsortkeys;
    AttrNumber *sortColIdx;
    Oid           *sortOperators;
    Oid           *collations;
    bool       *nullsFirst;

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
 *      Create sort plan to sort based on grouping columns
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
    List       *sub_tlist = lefttree->targetlist;
    ListCell   *l;
    int            numsortkeys;
    AttrNumber *sortColIdx;
    Oid           *sortOperators;
    Oid           *collations;
    bool       *nullsFirst;

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
    Plan       *plan = &node->plan;

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
    Plan       *matplan;
    Path        matpath;        /* dummy for result of cost_material */

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
         List *groupingSets, List *chain,
         double dNumGroups, Plan *lefttree)
{
    Agg           *node = makeNode(Agg);
    Plan       *plan = &node->plan;
    long        numGroups;

    /* Reduce to long, but 'ware overflow! */
    numGroups = (long) Min(dNumGroups, (double) LONG_MAX);

    node->aggstrategy = aggstrategy;
    node->aggsplit = aggsplit;
    node->numCols = numGroupCols;
    node->grpColIdx = grpColIdx;
    node->grpOperators = grpOperators;
    node->numGroups = numGroups;
    node->aggParams = NULL;        /* SS_finalize_plan() will fill this */
    node->groupingSets = groupingSets;
    node->chain = chain;
#ifdef __OPENTENBASE__
	node->hybrid = false;
	node->entrySize = 0;
	node->noDistinct = false;
#endif
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
               Plan *lefttree)
{
    WindowAgg  *node = makeNode(WindowAgg);
    Plan       *plan = &node->plan;

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
    Group       *node = makeNode(Group);
    Plan       *plan = &node->plan;

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
    Unique       *node = makeNode(Unique);
    Plan       *plan = &node->plan;
    int            numCols = list_length(distinctList);
    int            keyno = 0;
    AttrNumber *uniqColIdx;
    Oid           *uniqOperators;
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
        Unique       *node1 = makeNode(Unique);
        Plan       *plan1 = &node1->plan;

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
{// #lizard forgives
    Unique       *node = makeNode(Unique);
    Plan       *plan = &node->plan;
    int            keyno = 0;
    AttrNumber *uniqColIdx;
    Oid           *uniqOperators;
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
        Oid            pk_datatype = InvalidOid;
        Oid            eqop;
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
            if (ec->ec_sortref == 0)    /* can't happen */
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
        if (!OidIsValid(eqop))    /* should not happen */
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
            bool single_copy,
            Plan *subplan)
{
    Gather       *node = makeNode(Gather);
    Plan       *plan = &node->plan;

    plan->targetlist = qptlist;
    plan->qual = qpqual;
    plan->lefttree = subplan;
    plan->righttree = NULL;
    node->num_workers = nworkers;
    node->single_copy = single_copy;
    node->invisible = false;

#ifdef __OPENTENBASE__
	/*
	 * if there has hashjoin in the lower layer, write down the smallest workers
	 */
    if (min_workers_of_hashjon_gather > nworkers && contain_node_walker(subplan, T_HashJoin, false))
    {
        min_workers_of_hashjon_gather = nworkers;
    }
#endif
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
    SetOp       *node = makeNode(SetOp);
    Plan       *plan = &node->plan;
    int            numCols = list_length(distinctList);
    int            keyno = 0;
    AttrNumber *dupColIdx;
    Oid           *dupOperators;
    ListCell   *slitem;

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    /*
     * convert SortGroupClause list into arrays of attr indexes and equality
     * operators, as wanted by executor
     */
    Assert(numCols > 0);
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
 *      Build a LockRows plan node
 */
static LockRows *
make_lockrows(Plan *lefttree, List *rowMarks, int epqParam)
{
    LockRows   *node = makeNode(LockRows);
    Plan       *plan = &node->plan;

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
 *      Build a Limit plan node
 */
Limit *
make_limit(Plan *lefttree, Node *limitOffset, Node *limitCount,
		   int64 offset_est, int64 count_est, bool skipEarlyFinish)
{
    Limit       *node = makeNode(Limit);
    Plan       *plan = &node->plan;

    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    node->limitOffset = limitOffset;
    node->limitCount = limitCount;

#ifdef __OPENTENBASE__
	node->skipEarlyFinish = skipEarlyFinish;
#endif

    return node;
}

/*
 * make_result
 *      Build a Result plan node
 */
static Result *
make_result(List *tlist,
            Node *resconstantqual,
			Plan *subplan,
			List *qual)
{
    Result       *node = makeNode(Result);
    Plan       *plan = &node->plan;

    plan->targetlist = tlist;
	plan->qual = qual;
    plan->lefttree = subplan;
    plan->righttree = NULL;
    node->resconstantqual = resconstantqual;

#ifdef XCP
	/*
	 * Do not consider pushing down node if this node is make to process any
	 * project or qual that contain rownum or cn-udf.
	 */
	if (contain_user_defined_functions((Node *) tlist) ||
	    contain_user_defined_functions((Node *) qual))
		return node;
	
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
            if (pushdown->distributionKey != InvalidAttrNumber)
            {
                ListCell       *lc;
                TargetEntry    *key;

                key = list_nth(pushdown->scan.plan.targetlist,
                               pushdown->distributionKey);
                pushdown->distributionKey = InvalidAttrNumber;
                foreach(lc, tlist)
                {
                    TargetEntry    *tle = (TargetEntry *) lfirst(lc);
                    if (equal(tle->expr, key->expr))
                    {
                        pushdown->distributionKey = tle->resno;
                        break;
                    }
                }

                if (pushdown->distributionKey == InvalidAttrNumber)
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
                    pushdown->distributionKey = newtle->resno;
                }
            }
            /* This will be set as lefttree of the Result plan */
            plan->lefttree = pushdown->scan.plan.lefttree;
            pushdown->scan.plan.lefttree = plan;
            /* Now RemoteSubplan returns different values */
            pushdown->scan.plan.targetlist = tlist;
            return (Result *) subplan;
        }
    }
#endif /* XCP */
    return node;
}

/*
 * make_project_set
 *      Build a ProjectSet plan node
 */
static ProjectSet *
make_project_set(List *tlist,
                 Plan *subplan)
{
    ProjectSet *node = makeNode(ProjectSet);
    Plan       *plan = &node->plan;

    plan->targetlist = tlist;
    plan->qual = NIL;
    plan->lefttree = subplan;
    plan->righttree = NULL;

    return node;
}

/*
 * make_modifytable
 *      Build a ModifyTable plan node
 */
static ModifyTable *
make_modifytable(PlannerInfo *root,
                 CmdType operation, bool canSetTag,
                 Index nominalRelation, List *partitioned_rels,
				 bool partColsUpdated,
                 List *resultRelations, List *subplans,
                 List *withCheckOptionLists, List *returningLists,
                 List *rowMarks, OnConflictExpr *onconflict, int epqParam)
{// #lizard forgives
    ModifyTable *node = makeNode(ModifyTable);
    List       *fdw_private_list;
    Bitmapset  *direct_modify_plans;
    ListCell   *lc;
    int            i;
#ifdef __OPENTENBASE__
    int         partoffset;         
#endif

    Assert(list_length(resultRelations) == list_length(subplans));
    Assert(withCheckOptionLists == NIL ||
           list_length(resultRelations) == list_length(withCheckOptionLists));
    Assert(returningLists == NIL ||
           list_length(resultRelations) == list_length(returningLists));

    node->plan.lefttree = NULL;
    node->plan.righttree = NULL;
    node->plan.qual = NIL;
    /* setrefs.c will fill in the targetlist, if needed */
    node->plan.targetlist = NIL;

    node->operation = operation;
    node->canSetTag = canSetTag;
    node->nominalRelation = nominalRelation;
    node->partitioned_rels = partitioned_rels;
	node->partColsUpdated = partColsUpdated;
    node->resultRelations = resultRelations;
    node->resultRelIndex = -1;    /* will be set correctly in setrefs.c */
    node->rootResultRelIndex = -1;    /* will be set correctly in setrefs.c */
    node->plans = subplans;
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
    node->withCheckOptionLists = withCheckOptionLists;
    node->returningLists = returningLists;
    node->rowMarks = rowMarks;
    node->epqParam = epqParam;
#ifdef __OPENTENBASE__
    node->haspartparent = false;
    node->parentplanidx = -1;
    node->partplans = NULL;
    node->partpruning = NULL;
    node->partrelidx = -1;

    partoffset = -1;
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
        Index        rti = lfirst_int(lc);
        FdwRoutine *fdwroutine;
        List       *fdw_private;
        bool        direct_modify;

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
            
#ifdef __OPENTENBASE__
            /* for update or delete */
            if(resultRel->intervalparent)
            {
                if(root->parent_root)
                {
                    elog(ERROR,"ModifyTable: subquery cannot INSERT/UPDATE/DELETE on interval partition table.");
                }
                
                if(node->haspartparent)
                {
                    elog(ERROR,"ModifyTable: a sql statement must have one partitioned result relation at most.");
                }
                
                node->haspartparent = true;
                node->partrelidx = rti;
                node->partpruning = resultRel->childs;
                partoffset = i;
            }
#endif
        }
        else
        {
            RangeTblEntry *rte = planner_rt_fetch(rti, root);

            Assert(rte->rtekind == RTE_RELATION);
            if (rte->relkind == RELKIND_FOREIGN_TABLE)
                fdwroutine = GetFdwRoutineByRelId(rte->relid);
            else
                fdwroutine = NULL;
#ifdef __OPENTENBASE__
            /* for insert */
            if(rte->rtekind == RTE_RELATION)
            {
                Relation tempresultrel;
                AttrNumber partkey;
                tempresultrel = heap_open(rte->relid, NoLock);
                if(RELATION_IS_INTERVAL(tempresultrel))
                {
                    if(root->parent_root)
                    {
                        elog(ERROR,"ModifyTable: subquery cannot INSERT/UPDATE/DELETE on interval partition table.");
                    }
                    if(node->haspartparent)
                    {
                        elog(ERROR,"ModifyTable: a sql statement must have one partitioned result relation at most.");
                    }

                    partoffset = i;
                    node->haspartparent = true;
                    node->partrelidx = rti;
                    /* pruning */
                    if(operation == CMD_INSERT && root->parse->isSingleValues)
                    {
                        TargetEntry * targetentry;
                        
                        partkey = RelationGetPartitionColumnIndex(tempresultrel);
                        targetentry = get_tle_by_resno(root->parse->targetList,partkey);
                        if(!targetentry || !IsA(targetentry->expr,Const))
                        {
                            elog(ERROR, "the row inserted to partitioned table must have const value in partition key.");
                        }
                        
                        node->partpruning = RelationGetPartitionByValue(tempresultrel,(Const*)targetentry->expr);
                    }
                    else
                    {
                        node->partpruning = RelationGetPartitionsByQuals(tempresultrel, NULL);
                    }

                    if(!node->partpruning)
                    {
                        elog(ERROR, "value to inserted execeed range of partitioned table");
                    }
                }
                
                heap_close(tempresultrel, NoLock);
            }
#endif
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

#ifdef __OPENTENBASE__
    /* expand partitioned table */
    if(node->haspartparent)
    {
        node->parentplanidx = partoffset;
        if(operation == CMD_UPDATE || operation == CMD_DELETE)
        {
            Plan * parentplan;
            Plan * nextplan;
            Relation parentrel;
            RangeTblEntry *rte;
            Bitmapset *temp_bms;
            int nextpart;            
            
            rte = planner_rt_fetch(node->partrelidx, root);
            parentrel = heap_open(rte->relid,NoLock);
            parentplan = list_nth(node->plans,node->parentplanidx);

            temp_bms = bms_copy(node->partpruning);

            nextpart = -1;
            while((nextpart = bms_first_member(temp_bms)) >= 0)
            {
                nextplan = (Plan *)copyObject(parentplan);
                replace_target_relation((Node *)nextplan,node->partrelidx,parentrel,nextpart);
                node->partplans = lappend(node->partplans, nextplan);
            }

            heap_close(parentrel,NoLock);
            bms_free(temp_bms);
        }

        /* remembered in PlannerInfo */
        root->haspart_tobe_modify = true;
        root->partrelindex = node->partrelidx;
        root->partpruning = bms_copy(node->partpruning);
    }
#endif

    return node;
}

/*
 * is_projection_capable_path
 *        Check whether a given Path node is able to do projection.
 */
bool
is_projection_capable_path(Path *path)
{// #lizard forgives
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
 *        Check whether a given Plan node is able to do projection.
 */
bool
is_projection_capable_plan(Plan *plan)
{// #lizard forgives
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
#define CNAME_MAXLEN 64
static int cursor_id = 0;
static int32  rda_id_count = 0; /* rad id count use static, so we can identify the rdas of one query. */

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

static int64
get_internal_rda_id(void)
{
    int64 h_part, l_part;
    GlobalTimestamp  current_gts     = InvalidGlobalTimestamp;

    if (!IS_PGXC_LOCAL_COORDINATOR)
    {
        elog(ERROR, "RDAID can only be generated on coordinator!");
    }
    if (rda_id_count++ == INT32_MAX)
        rda_id_count = 0;
    current_gts = GetGlobalTimestampGTM() + rda_id_count;

    /* Get latest gts, in case gtm not avaliable, we can error asap. */
    h_part = (PGXCNodeId << 16 & 0XFFFF0000)| (MyProcPid & 0X0000FFFF);
    l_part = DatumGetUInt32((Datum)current_gts);

    /* rda identifer composed of current_gts as low 32 bits, PGXCNodeId as 48~64 bits, MyProcPid as 32~48 bits. */
    return (int64)(h_part<<32 | l_part);
}

/*
 * Get a unique sequence number for RemoteDataAccess plan node in the cluster
 */
static char *
get_internal_rda_seq(void)
{
    char *rda_seq;
    int64 rda_id = get_internal_rda_id();

    rda_seq = (char *)palloc(CNAME_MAXLEN);
    snprintf(rda_seq, CNAME_MAXLEN - 1, "%ld", rda_id);

    return rda_seq;
}
#endif


#ifdef __OPENTENBASE__
bool
contain_remote_subplan_walker(Node *node, void *context, bool include_cte)
{// #lizard forgives
    Node *plan = node;

    if (!plan)
    {
        return false;
    }
    
    if (IsA(node, RemoteSubplan) || IsA(node, RemoteQuery) || (include_cte && IsA(node, CteScan)))
    {
        return true;
    }

    if (IsA(node, SubqueryScan))
    {
        SubqueryScan *subquery = (SubqueryScan *)node;
        plan = (Node *)subquery->subplan;
    }

    if (IsA(plan, Append))
    {
        ListCell *lc;
        Append *append = (Append *)plan;

        foreach(lc, append->appendplans)
        {
            Plan *appendplan = (Plan *)lfirst(lc);

            if (appendplan && contain_remote_subplan_walker((Node*)appendplan, NULL, include_cte))
            {
                return true;
            }
        }

        return false;
    }
    else if (IsA(plan, MergeAppend))
    {
        ListCell *lc;
        MergeAppend *mergeappend = (MergeAppend *)plan;

        foreach(lc, mergeappend->mergeplans)
        {
            Plan *mergeappendplan = (Plan *)lfirst(lc);

            if (mergeappendplan && contain_remote_subplan_walker((Node*)mergeappendplan, NULL, include_cte))
            {
                return true;
            }
        }

        return false;
    }

    if (outerPlan(plan))
    {
        if (contain_remote_subplan_walker((Node*)outerPlan(plan), NULL, include_cte))
        {
            return true;
        }
    }

    if (innerPlan(plan))
    {
        if (contain_remote_subplan_walker((Node*)innerPlan(plan), NULL, include_cte))
        {
            return true;
        }
    }
    return false;
}

/*
 * check if contain the type node in the plan, only support
 * T_HashJoin and T_Gather now
 * search_nonparallel only work if type is T_HashJoin
 */
static bool
contain_node_walker(Plan *node, NodeTag type, bool search_nonparallel)
{
    Plan *plan = node;

    if (!plan)
    {
        return false;
    }

    if (IsA(node, RemoteSubplan) || IsA(node, RemoteQuery))
    {
        return false;
    }

    if (type == T_HashJoin)
    {
    if (IsA(node, HashJoin))
    {
            if (search_nonparallel)
            {
                /* return if contain non parallel hashjoin */
                HashJoin *join_plan = (HashJoin *) node;
                return !join_plan->join.plan.parallel_aware;
            }
            else
            {
        return true;
    }
        }
    }
    else if (type == T_Gather)
    {
        if (IsA(node, Gather))
        {
            return true;
        }
    }

    if (IsA(node, SubqueryScan))
    {
        SubqueryScan *subquery = (SubqueryScan *)node;
        plan = subquery->subplan;
    }

    if (IsA(plan, Append))
    {
        ListCell *lc;
        Append *append = (Append *)plan;

        foreach(lc, append->appendplans)
        {
            Plan *appendplan = (Plan *)lfirst(lc);

            if (appendplan && contain_node_walker(appendplan, type, search_nonparallel))
            {
                return true;
            }
        }

        return false;
    }
    else if (IsA(plan, MergeAppend))
    {
        ListCell *lc;
        MergeAppend *mergeappend = (MergeAppend *)plan;

        foreach(lc, mergeappend->mergeplans)
        {
            Plan *mergeappendplan = (Plan *)lfirst(lc);

            if (mergeappendplan && contain_node_walker(mergeappendplan, type, search_nonparallel))
            {
                return true;
            }
        }

        return false;
    }

    if (outerPlan(plan))
    {
        if (contain_node_walker(outerPlan(plan), type, search_nonparallel))
        {
            return true;
        }
    }

    if (innerPlan(plan))
    {
        if (contain_node_walker(innerPlan(plan), type, search_nonparallel))
        {
            return true;
        }
    }

    return false;
}


static Plan*
materialize_top_remote_subplan(Plan *node)
{
    Node *plan = (Node *)node;

    if (!plan)
    {
        return NULL;
    }

    if (IsA(node, Material))
    {
        return node;
    }

    if (IsA(node, RemoteSubplan))
    {
        Plan	   *matplan = (Plan *) make_material(node);

        /*
         * We assume the materialize will not spill to disk, and therefore
         * charge just cpu_operator_cost per tuple.  (Keep this estimate in
         * sync with cost_mergejoin.)
         */
        copy_plan_costsize(matplan, node);
        matplan->total_cost += cpu_operator_cost * matplan->plan_rows;

        return matplan;
    }

    if (IsA(node, SubqueryScan))
    {
        SubqueryScan *subquery = (SubqueryScan *)node;
        plan = (Node *)subquery->subplan;
    }

    if (IsA(plan, Append))
    {
        ListCell *lc;
        Append *append = (Append *)plan;

        foreach(lc, append->appendplans)
        {
            Plan *appendplan = (Plan *)lfirst(lc);

            if (appendplan)
            {
                Plan *tmpplan = materialize_top_remote_subplan(appendplan);
                if (tmpplan && tmpplan != lfirst(lc))
                {
                    lfirst(lc) = tmpplan;
                }
            }
        }

        return node;
    }
    else if (IsA(plan, MergeAppend))
    {
        ListCell *lc;
        MergeAppend *mergeappend = (MergeAppend *)plan;

        foreach(lc, mergeappend->mergeplans)
        {
            Plan *mergeappendplan = (Plan *)lfirst(lc);

            if (mergeappendplan)
            {
                Plan *tmpplan = materialize_top_remote_subplan(mergeappendplan);
                if (tmpplan && tmpplan != lfirst(lc))
                {
                    lfirst(lc) = tmpplan;
                }
            }
        }

        return node;
    }

    if (outerPlan(plan))
    {
        Plan *tmpplan = materialize_top_remote_subplan(outerPlan(plan));
        if (tmpplan && tmpplan != outerPlan(plan))
        {
            outerPlan(plan) = tmpplan;
        }
    }

    if (innerPlan(plan))
    {
        Plan *tmpplan = materialize_top_remote_subplan(innerPlan(plan));
        if (tmpplan && tmpplan != innerPlan(plan))
        {
            innerPlan(plan) = tmpplan;
        }
    }
    return node;
}

static void
create_remotequery_for_rel(PlannerInfo *root, ModifyTable *mt, RangeTblEntry *res_rel, Index resultRelationIndex,
                                   int relcount, CmdType cmdtyp, RelationAccessType    accessType, int partindex,
                                   Relation relation)
{// #lizard forgives
    char            *relname;
    RemoteQuery     *fstep;
    RelationLocInfo    *rel_loc_info;
    Plan            *sourceDataPlan;
    RangeTblEntry    *dummy_rte;
	char            *child_relname;
	Oid             child_reloid;

    relname = get_rel_name(res_rel->relid);

    /* Get location info of the target table */
    rel_loc_info = GetRelationLocInfo(res_rel->relid);
    if (rel_loc_info == NULL)
        return;

	if (partindex >= 0)
	{
		child_relname = GetPartitionName(res_rel->relid, partindex, false);
		child_reloid = get_relname_relid(child_relname, RelationGetNamespace(relation));
		if (InvalidOid == child_reloid)
		{
			if(child_relname)
			{
			    pfree(child_relname);
			}
			return;
		}
	}

    fstep = makeNode(RemoteQuery);
    fstep->scan.scanrelid = resultRelationIndex;

    /*
     * DML planning generates its own parameters that refer to the source
     * data plan.
     */

    fstep->is_temp = IsTempTable(res_rel->relid);
    fstep->read_only = false;
    fstep->dml_on_coordinator = true;

    if (mt->returningLists)
        pgxc_add_returning_list(fstep,
                                list_nth(mt->returningLists, relcount),
                                resultRelationIndex);

    if (partindex >= 0)
    {
        RangeTblEntry    *child_rte;
        /* Get the plan that is supposed to supply source data to this plan */
        sourceDataPlan = list_nth(mt->plans, relcount);

        child_rte = copyObject(res_rel);
        child_rte->intervalparent = false;
		child_rte->relname = child_relname;
		child_rte->relid = child_reloid;

        root->parse->rtable = lappend(root->parse->rtable, child_rte);

        pgxc_build_dml_statement(root, cmdtyp, list_length(root->parse->rtable), fstep,
                                    sourceDataPlan->targetlist, true);

    }
    else
    {
        /* Get the plan that is supposed to supply source data to this plan */
        sourceDataPlan = list_nth(mt->plans, relcount);

        pgxc_build_dml_statement(root, cmdtyp, resultRelationIndex, fstep,
                                    sourceDataPlan->targetlist, false);
    }

    switch (cmdtyp)
    {
        case CMD_INSERT:
        case CMD_UPDATE:
        case CMD_DELETE:
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
        fstep->exec_nodes = GetRelationNodes(rel_loc_info, 
                                             0, 
                                             true,
                                             0,
                                             true,
                                             accessType);
    }
    fstep->exec_nodes->en_relid = res_rel->relid;
    fstep->exec_type = EXEC_ON_DATANODES;
    //if (cmdtyp != CMD_DELETE)
    {
        
        fstep->exec_nodes->en_expr       = pgxc_set_en_expr(res_rel->relid, resultRelationIndex);
        //fstep->exec_nodes->sec_en_expr = pgxc_set_sec_en_expr(res_rel->relid, resultRelationIndex);

        /* delete is a little different, distributed column is the last output column */
        if (cmdtyp == CMD_DELETE)
        {
            Var *var = (Var *)fstep->exec_nodes->en_expr;

            var->varattno = var->varoattno = list_length(sourceDataPlan->targetlist);
        }
    }
    dummy_rte = make_dummy_remote_rte(relname,
                                    makeAlias("REMOTE_DML_QUERY", NIL));
    root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
    fstep->scan.scanrelid = list_length(root->parse->rtable);

    /* get cursor for insert/update/delete/UPSERT */
    {
        bool upsert = false;
        char *cursor = get_internal_cursor();
        
        //fstep->cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
        fstep->statement = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
        fstep->update_cursor = NULL;

        //snprintf(fstep->cursor, CNAME_MAXLEN - 1, "%s", cursor);

        if (cmdtyp == CMD_INSERT && mt->onConflictAction == ONCONFLICT_UPDATE)
        {
            upsert = true;
            //fstep->select_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
            fstep->select_cursor = NULL;
            fstep->update_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);

            snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s%s", INSERT_TRIGGER, cursor);
            //snprintf(fstep->select_cursor, CNAME_MAXLEN - 1, "select_%s", cursor);
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

    mt->remote_plans = lappend(mt->remote_plans, fstep);
}

/*
 * create_remotedml_plan()
 *
 * For every target relation, add a remote query node to carry out remote
 * operations.
 */
Plan *
create_remotedml_plan(PlannerInfo *root, Plan *topplan, CmdType cmdtyp)
{// #lizard forgives
    ModifyTable            *mt = (ModifyTable *)topplan;
    ListCell            *rel;
    int                    relcount = -1;
    RelationAccessType    accessType;

    /* We expect to work only on ModifyTable node */
    if (!IsA(topplan, ModifyTable))
        elog(ERROR, "Unexpected node type: %d", topplan->type);

    switch(cmdtyp)
    {
        case CMD_UPDATE:
        case CMD_DELETE:
            accessType = RELATION_ACCESS_UPDATE;
            break;

        case CMD_INSERT:
            accessType = RELATION_ACCESS_INSERT;
            break;

        default:
            elog(ERROR, "Unexpected command type: %d", cmdtyp);
            return NULL;
    }

    /*
     * For every result relation, build a remote plan to execute remote DML.
     */
    foreach(rel, mt->resultRelations)
    {
        Index            resultRelationIndex = lfirst_int(rel);
        RangeTblEntry    *res_rel;
        Relation        relation;

        relcount++;

        res_rel = rt_fetch(resultRelationIndex, root->parse->rtable);

        /* Bad relation ? */
        if (res_rel == NULL || res_rel->rtekind != RTE_RELATION)
            continue;

        relation = heap_open(res_rel->relid, NoLock);

        if (!RELATION_IS_INTERVAL(relation))
        {

            create_remotequery_for_rel(root, mt, res_rel, resultRelationIndex, 
                                       relcount, cmdtyp, accessType, -1, relation);
#if 0
            relname = get_rel_name(res_rel->relid);

            /* Get location info of the target table */
            rel_loc_info = GetRelationLocInfo(res_rel->relid);
            if (rel_loc_info == NULL)
                continue;

            fstep = makeNode(RemoteQuery);
            fstep->scan.scanrelid = resultRelationIndex;

            /*
             * DML planning generates its own parameters that refer to the source
             * data plan.
             */

            fstep->is_temp = IsTempTable(res_rel->relid);
            fstep->read_only = false;
            fstep->dml_on_coordinator = true;

            if (mt->returningLists)
                pgxc_add_returning_list(fstep,
                                        list_nth(mt->returningLists, relcount),
                                        resultRelationIndex);

            /* Get the plan that is supposed to supply source data to this plan */
            sourceDataPlan = list_nth(mt->plans, relcount);

            pgxc_build_dml_statement(root, cmdtyp, resultRelationIndex, fstep,
                                        sourceDataPlan->targetlist, false);

            switch (cmdtyp)
            {
                case CMD_INSERT:
                case CMD_UPDATE:
                case CMD_DELETE:
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
                fstep->exec_nodes = GetRelationNodes(rel_loc_info, 
                                                     0, 
                                                     true,
                                                     accessType);
            }
            fstep->exec_nodes->en_relid = res_rel->relid;
            fstep->exec_type = EXEC_ON_DATANODES;
            //if (cmdtyp != CMD_DELETE)
            {
                
                fstep->exec_nodes->en_expr     = pgxc_set_en_expr(res_rel->relid, resultRelationIndex);
                //fstep->exec_nodes->sec_en_expr = pgxc_set_sec_en_expr(res_rel->relid, resultRelationIndex);

                /* delete is a little different, distributed column is the last output column */
                if (cmdtyp == CMD_DELETE)
                {
                    Var *var = (Var *)fstep->exec_nodes->en_expr;

                    var->varattno = var->varoattno = list_length(sourceDataPlan->targetlist);
                }
            }
            dummy_rte = make_dummy_remote_rte(relname,
                                            makeAlias("REMOTE_DML_QUERY", NIL));
            root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
            fstep->scan.scanrelid = list_length(root->parse->rtable);

            /* get cursor for insert/update/delete/UPSERT */
            {
                bool upsert = false;
                char *cursor = get_internal_cursor();
                
                //fstep->cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
                fstep->statement = (char *)palloc(sizeof(char) * CNAME_MAXLEN);

                //snprintf(fstep->cursor, CNAME_MAXLEN - 1, "%s", cursor);

                if (cmdtyp == CMD_INSERT && mt->onConflictAction == ONCONFLICT_UPDATE)
                {
                    upsert = true;
                    //fstep->select_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
                    fstep->select_cursor = NULL;
                    fstep->update_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);

                    snprintf(fstep->statement, CNAME_MAXLEN - 1, "insert_%s", cursor);
                    //snprintf(fstep->select_cursor, CNAME_MAXLEN - 1, "select_%s", cursor);
                    snprintf(fstep->update_cursor, CNAME_MAXLEN - 1, "update_%s", cursor);
                }
                else
                {
                    snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s", cursor);
                }

                /* prepare statements */
                PrepareRemoteDMLStatement(upsert, fstep->statement, 
                                          fstep->select_cursor, fstep->update_cursor);
            }
            
            fstep->action = UPSERT_NONE;
            
            mt->remote_plans = lappend(mt->remote_plans, fstep);
#endif
        }
        else/* interval partition */
        {
            int nextpart = 0;
            while((nextpart = bms_first_member(mt->partpruning)) >= 0)
            {

                create_remotequery_for_rel(root, mt, res_rel, resultRelationIndex, relcount, cmdtyp,
                                           accessType, nextpart, relation);
#if 0
                RangeTblEntry    *child_rte;
                
                relname = get_rel_name(res_rel->relid);

                /* Get location info of the target table */
                rel_loc_info = GetRelationLocInfo(res_rel->relid);
                if (rel_loc_info == NULL)
                    continue;

                fstep = makeNode(RemoteQuery);
                fstep->scan.scanrelid = resultRelationIndex;

                /*
                 * DML planning generates its own parameters that refer to the source
                 * data plan.
                 */

                fstep->is_temp = IsTempTable(res_rel->relid);
                fstep->read_only = false;
                fstep->dml_on_coordinator = true;

                if (mt->returningLists)
                    pgxc_add_returning_list(fstep,
                                            list_nth(mt->returningLists, relcount),
                                            resultRelationIndex);

                /* Get the plan that is supposed to supply source data to this plan */
                sourceDataPlan = list_nth(mt->plans, relcount);

                child_rte = copyObject(res_rel);
                child_rte->intervalparent = false;
                child_rte->relname = GetPartitionName(res_rel->relid, nextpart, false);
                child_rte->relid = get_relname_relid(child_rte->relname, RelationGetNamespace(relation));

                root->parse->rtable = lappend(root->parse->rtable, child_rte);

                pgxc_build_dml_statement(root, cmdtyp, list_length(root->parse->rtable), fstep,
                                            sourceDataPlan->targetlist, true);

                switch (cmdtyp)
                {
                    case CMD_INSERT:
                    case CMD_UPDATE:
                    case CMD_DELETE:
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
                    fstep->exec_nodes = GetRelationNodes(rel_loc_info, 
                                                         0, 
                                                         true,
                                                         accessType);
                }
                fstep->exec_nodes->en_relid = res_rel->relid;
                fstep->exec_type = EXEC_ON_DATANODES;
                //if (cmdtyp != CMD_DELETE)
                {
                    
                    fstep->exec_nodes->en_expr     = pgxc_set_en_expr(res_rel->relid, resultRelationIndex);
                    //fstep->exec_nodes->sec_en_expr = pgxc_set_sec_en_expr(res_rel->relid, resultRelationIndex);

                    /* delete is a little different, distributed column is the last output column */
                    if (cmdtyp == CMD_DELETE)
                    {
                        Var *var = (Var *)fstep->exec_nodes->en_expr;

                        var->varattno = var->varoattno = list_length(sourceDataPlan->targetlist);
                    }
                }
                dummy_rte = make_dummy_remote_rte(relname,
                                                makeAlias("REMOTE_DML_QUERY", NIL));
                root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
                fstep->scan.scanrelid = list_length(root->parse->rtable);

                /* get cursor for insert/update/delete/UPSERT */
                {
                    bool upsert = false;
                    char *cursor = get_internal_cursor();
                    
                    //fstep->cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
                    fstep->statement = (char *)palloc(sizeof(char) * CNAME_MAXLEN);

                    //snprintf(fstep->cursor, CNAME_MAXLEN - 1, "%s", cursor);

                    if (cmdtyp == CMD_INSERT && mt->onConflictAction == ONCONFLICT_UPDATE)
                    {
                        upsert = true;
                        //fstep->select_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);
                        fstep->select_cursor = NULL;
                        fstep->update_cursor = (char *)palloc(sizeof(char) * CNAME_MAXLEN);

                        snprintf(fstep->statement, CNAME_MAXLEN - 1, "insert_%s", cursor);
                        //snprintf(fstep->select_cursor, CNAME_MAXLEN - 1, "select_%s", cursor);
                        snprintf(fstep->update_cursor, CNAME_MAXLEN - 1, "update_%s", cursor);
                    }
                    else
                    {
                        snprintf(fstep->statement, CNAME_MAXLEN - 1, "%s", cursor);
                    }

                    /* prepare statements */
                    PrepareRemoteDMLStatement(upsert, fstep->statement, 
                                              fstep->select_cursor, fstep->update_cursor);
                }
                
                fstep->action = UPSERT_NONE;
                
                mt->remote_plans = lappend(mt->remote_plans, fstep);
#endif
            }
        }

        heap_close(relation, NoLock);
    }

    return (Plan *)mt;
}


static double
GetPlanRows(Plan *plan)
{// #lizard forgives
    double rows = 0;
    
    switch(nodeTag(plan))
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
            rows =  plan->plan_rows;
            break;
        case T_Group:
        case T_Agg:
        case T_Sort:
        case T_Limit:
            rows = GetPlanRows(plan->lefttree);
            break;
        case T_HashJoin:
        case T_NestLoop:
        case T_MergeJoin:
            {
                double left = GetPlanRows(plan->lefttree);
                double right = GetPlanRows(plan->righttree);

                rows = Max(left, right);
            }
            break;
        default:
            rows =  plan->plan_rows;
            break;
    }

    return rows;
}

bool
partkey_match_index(Oid indexoid, AttrNumber partkey)
{
    Relation indexrel;
    IndexInfo *indexinfo;
    bool    result;

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
static void
bitmap_subplan_set_shared(Plan *plan, bool shared)
{
       if (!plan)
       {
               return;
       }
       if (IsA(plan, BitmapAnd))
               bitmap_subplan_set_shared(
                                 linitial(((BitmapAnd *) plan)->bitmapplans), shared);
       else if (IsA(plan, BitmapOr))
               ((BitmapOr *) plan)->isshared = shared;
       else if (IsA(plan, BitmapIndexScan))
               ((BitmapIndexScan *) plan)->isshared = shared;
       else
               elog(ERROR, "unrecognized node type: %d", nodeTag(plan));
}

static bool
set_plan_parallel(Plan *plan)
{// #lizard forgives
    bool result = false;
    
    if (!plan)
        return false;

    switch(plan->type)
    {
        case T_SeqScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_RemoteSubplan:
            {
                plan->parallel_aware = true;
                result = true;
            }
            break;
		case T_BitmapHeapScan:
			{
				bitmap_subplan_set_shared(plan->lefttree, true);
				plan->parallel_aware = true;
				result = true;
				break;
			}
        case T_NestLoop:
            {
                if (set_plan_parallel(plan->lefttree))
                {
                    plan->parallel_aware = true;
                    result = true;
                }
            }
            break;
        case T_HashJoin:
            {
                if (set_plan_parallel(plan->lefttree) && 
                    set_plan_parallel(plan->righttree))
                {
                    plan->parallel_aware = true;
                    result = true;
                }
            }
            break;
        case T_Gather:
            {
                result = true;
            }
            break;
        case T_Hash:
            {
                if (set_plan_parallel(plan->lefttree))
                {
                    plan->parallel_aware = true;
                    result = true;
                }
            }
            break;
        case T_SubqueryScan:
            {
                SubqueryScan *node = (SubqueryScan *)plan;
                Plan *subplan = node->subplan;

                if (set_plan_parallel(subplan))
                {
                    plan->parallel_aware = true;
                    result = true;
                }
            }
            break;
        case T_Append:
            {
                ListCell *lc;
                Append *append = (Append *)plan;

                foreach(lc, append->appendplans)
                {
                    Plan *appendplan = (Plan *)lfirst(lc);

                    if (!set_plan_parallel(appendplan))
                    {
                        return false;
                    }
                }

                plan->parallel_aware = true;
                result = true;
            }
            break;
        case T_MergeAppend:
            {
                ListCell *lc;
                MergeAppend *mergeappend = (MergeAppend *)plan;
        
                foreach(lc, mergeappend->mergeplans)
                {
                    Plan *mergeappendplan = (Plan *)lfirst(lc);
        
                    if (!set_plan_parallel(mergeappendplan))
                    {
                        return false;
                    }
                }
        
                plan->parallel_aware = true;
                result = true;
            }
            break;
        case T_Agg:
            {
                Agg *aggplan = (Agg *)plan;

                if (aggplan->aggsplit == AGGSPLIT_FINAL_DESERIAL)
                {
                    if (aggplan->aggstrategy == AGG_HASHED && plan->parallel_aware)
                    {
                        result = true;
                    }
                    else if (aggplan->aggstrategy == AGG_SORTED)
                    {
                        Plan *lefttree = plan->lefttree;

                        if (IsA(lefttree, Sort) && lefttree->parallel_aware)
                        {
                            result = true;
                        }
                    }
                }
            }
            break;
        default:
            break;
    }


    return result;
}

static void
set_plan_nonparallel(Plan *plan)
{
    Plan *subplan = plan;
    
    if (!plan)
        return;

    if (IsA(plan, SubqueryScan))
    {
        SubqueryScan *node = (SubqueryScan *)plan;

        subplan = node->subplan;
    }
    else if (IsA(plan, Gather))
    {
        return;
    }
    else if (IsA(plan, Append))
    {
        ListCell *lc;
        Append *append = (Append *)plan;

        foreach(lc, append->appendplans)
        {
            Plan *appendplan = (Plan *)lfirst(lc);

            set_plan_nonparallel(appendplan);
        }

        append->plan.parallel_aware = false;

        return;
    }
    else if (IsA(plan, MergeAppend))
    {
        ListCell *lc;
        MergeAppend *mergeappend = (MergeAppend *)plan;

        foreach(lc, mergeappend->mergeplans)
        {
            Plan *mergeappendplan = (Plan *)lfirst(lc);

            set_plan_nonparallel(mergeappendplan);
        }

        mergeappend->plan.parallel_aware = false;

        return;
    }
	else if (IsA(plan, BitmapHeapScan))
	{
		bitmap_subplan_set_shared(plan->lefttree, false);
		plan->parallel_aware = false;
		return;
	}

    set_plan_nonparallel(subplan->lefttree);
    set_plan_nonparallel(subplan->righttree);

    subplan->parallel_aware = false;
}
#endif

