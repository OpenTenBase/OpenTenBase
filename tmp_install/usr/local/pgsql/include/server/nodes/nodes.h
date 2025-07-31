/*-------------------------------------------------------------------------
 *
 * nodes.h
 *      Definitions for tagged nodes.
 *
 *
 * Portions Copyright (c) 2012-2014, TransLattice, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * This source code file contains modifications made by THL A29 Limited ("Tencent Modifications").
 * All Tencent Modifications are Copyright (C) 2023 THL A29 Limited.
 *
 * src/include/nodes/nodes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODES_H
#define NODES_H

/*
 * The first field of every node is NodeTag. Each node created (with makeNode)
 * will have one of the following tags as the value of its first field.
 *
 * Note that inserting or deleting node types changes the numbers of other
 * node types later in the list.  This is no problem during development, since
 * the node numbers are never stored on disk.  But don't do it in a released
 * branch, because that would represent an ABI break for extensions.
 */
typedef enum NodeTag
{
    T_Invalid = 0,

#ifdef __AUDIT__
    /*
     * TAGS FOR AUDIT
     */
    T_AuditStmt, 
    T_CleanAuditStmt,
#endif

    /*
     * TAGS FOR EXECUTOR NODES (execnodes.h)
     */
    T_IndexInfo,
    T_ExprContext,
    T_ProjectionInfo,
    T_JunkFilter,
    T_ResultRelInfo,
    T_EState,
    T_TupleTableSlot,

    /*
     * TAGS FOR PLAN NODES (plannodes.h)
     */
    T_Plan,
    T_Result,
    T_ProjectSet,
    T_ModifyTable,
    T_Append,
    T_MergeAppend,
    T_RecursiveUnion,
    T_BitmapAnd,
    T_BitmapOr,
    T_Scan,
    T_SeqScan,
    T_SampleScan,
    T_IndexScan,
    T_IndexOnlyScan,
    T_BitmapIndexScan,
    T_BitmapHeapScan,
    T_TidScan,
    T_SubqueryScan,
    T_FunctionScan,
    T_ValuesScan,
    T_TableFuncScan,
    T_CteScan,
    T_NamedTuplestoreScan,
    T_WorkTableScan,
    T_ForeignScan,
    T_CustomScan,
    T_Join,
    T_NestLoop,
    T_MergeJoin,
    T_HashJoin,
    T_Material,
    T_Sort,
    T_Group,
    T_Agg,
    T_WindowAgg,
    T_Unique,
    T_Gather,
    T_GatherMerge,
    T_Hash,
    T_SetOp,
    T_LockRows,
    T_Limit,
#ifdef PGXC
    /*
     * TAGS FOR PGXC NODES
     * (planner.h, locator.h, nodemgr.h, groupmgr.h)
     */
#ifdef XCP
    T_Distribution,
#endif
    T_ExecNodes,
    T_SimpleSort,
    T_RemoteQuery,
#ifdef XCP
    T_RemoteSubplan,
#endif
    T_PGXCNodeHandle,
    T_AlterNodeStmt,
    T_CreateNodeStmt,
    T_DropNodeStmt,
    T_CreateGroupStmt,
    T_DropGroupStmt,
    T_AlterGroupStmt,
    T_AlterGroupCmd,
#endif

#ifdef _MIGRATE_
    T_CreateShardStmt,
    T_CleanShardingStmt,
    T_DropShardStmt,
    T_MoveDataStmt,
#endif
#ifdef __COLD_HOT__
    T_CreateKeyValuesStmt,
    T_CheckOverLapStmt,
#endif
#ifdef __OPENTENBASE__
    T_LockNodeStmt,
	T_SampleStmt,
#endif

    /* these aren't subclasses of Plan: */
    T_NestLoopParam,
    T_PlanRowMark,
    T_PlanInvalItem,

    /*
     * TAGS FOR PLAN STATE NODES (execnodes.h)
     *
     * These should correspond one-to-one with Plan node types.
     */
    T_PlanState,
    T_ResultState,
    T_ProjectSetState,
    T_ModifyTableState,
    T_AppendState,
    T_MergeAppendState,
    T_RecursiveUnionState,
    T_BitmapAndState,
    T_BitmapOrState,
    T_ScanState,
    T_SeqScanState,
    T_SampleScanState,
    T_IndexScanState,
    T_IndexOnlyScanState,
    T_BitmapIndexScanState,
    T_BitmapHeapScanState,
    T_TidScanState,
    T_SubqueryScanState,
    T_FunctionScanState,
    T_TableFuncScanState,
    T_ValuesScanState,
    T_CteScanState,
    T_NamedTuplestoreScanState,
    T_WorkTableScanState,
    T_ForeignScanState,
    T_CustomScanState,
    T_JoinState,
    T_NestLoopState,
    T_MergeJoinState,
    T_HashJoinState,
    T_MaterialState,
    T_SortState,
    T_GroupState,
    T_AggState,
    T_WindowAggState,
    T_UniqueState,
    T_GatherState,
    T_GatherMergeState,
    T_HashState,
    T_SetOpState,
    T_LockRowsState,
    T_LimitState,
#ifdef PGXC
    T_RemoteQueryState,
#ifdef XCP
    T_RemoteSubplanState,
#endif
#endif

    /*
     * TAGS FOR PRIMITIVE NODES (primnodes.h)
     */
    T_Alias,
    T_RangeVar,
    T_TableFunc,
    T_Expr,
    T_Var,
    T_Const,
    T_Param,
    T_Aggref,
    T_GroupingFunc,
    T_WindowFunc,
    T_ArrayRef,
    T_FuncExpr,
    T_NamedArgExpr,
    T_OpExpr,
    T_DistinctExpr,
    T_NullIfExpr,
    T_ScalarArrayOpExpr,
    T_BoolExpr,
    T_SubLink,
    T_SubPlan,
    T_AlternativeSubPlan,
    T_FieldSelect,
    T_FieldStore,
    T_RelabelType,
    T_CoerceViaIO,
    T_ArrayCoerceExpr,
    T_ConvertRowtypeExpr,
    T_CollateExpr,
    T_CaseExpr,
    T_CaseWhen,
    T_CaseTestExpr,
    T_ArrayExpr,
    T_RowExpr,
    T_RowCompareExpr,
    T_CoalesceExpr,
    T_MinMaxExpr,
    T_SQLValueFunction,
    T_XmlExpr,
    T_NullTest,
    T_BooleanTest,
    T_CoerceToDomain,
    T_CoerceToDomainValue,
    T_SetToDefault,
    T_CurrentOfExpr,
    T_NextValueExpr,
    T_InferenceElem,
    T_TargetEntry,
    T_RangeTblRef,
    T_JoinExpr,
    T_FromExpr,
    T_OnConflictExpr,
    T_IntoClause,
        T_PartitionPruneStep,
        T_PartitionPruneStepOp,
        T_PartitionPruneStepCombine,
#ifdef PGXC
    T_DistributeBy,
    T_PGXCSubCluster,
#endif
#ifdef __OPENTENBASE__
    T_PartitionBy,
    T_AddDropPartitions,
    T_PartitionForExpr,
    T_ExchangeIndexName,
    T_ModifyPartStartValue,
#endif
#ifdef __AUDIT_FGA__
    T_AuditFgaPolicy,
#endif
    /*
     * TAGS FOR EXPRESSION STATE NODES (execnodes.h)
     *
     * ExprState represents the evaluation state for a whole expression tree.
     * Most Expr-based plan nodes do not have a corresponding expression state
     * node, they're fully handled within execExpr* - but sometimes the state
     * needs to be shared with other parts of the executor, as for example
     * with AggrefExprState, which nodeAgg.c has to modify.
     */
    T_ExprState,
    T_AggrefExprState,
    T_WindowFuncExprState,
    T_SetExprState,
    T_SubPlanState,
    T_AlternativeSubPlanState,
    T_DomainConstraintState,

    /*
     * TAGS FOR PLANNER NODES (relation.h)
     */
    T_PlannerInfo,
    T_PlannerGlobal,
    T_RelOptInfo,
    T_IndexOptInfo,
    T_ForeignKeyOptInfo,
    T_ParamPathInfo,
    T_Path,
    T_IndexPath,
    T_BitmapHeapPath,
    T_BitmapAndPath,
    T_BitmapOrPath,
    T_TidPath,
    T_SubqueryScanPath,
    T_ForeignPath,
    T_CustomPath,
    T_NestPath,
    T_MergePath,
    T_HashPath,
    T_AppendPath,
    T_MergeAppendPath,
    T_ResultPath,
	T_QualPath,
    T_MaterialPath,
    T_UniquePath,
    T_GatherPath,
    T_GatherMergePath,
    T_ProjectionPath,
    T_ProjectSetPath,
    T_SortPath,
    T_GroupPath,
    T_UpperUniquePath,
    T_AggPath,
    T_GroupingSetsPath,
    T_MinMaxAggPath,
    T_WindowAggPath,
    T_SetOpPath,
    T_RecursiveUnionPath,
    T_LockRowsPath,
    T_ModifyTablePath,
    T_LimitPath,
    /* these aren't subclasses of Path: */
    T_EquivalenceClass,
    T_EquivalenceMember,
    T_PathKey,
    T_PathTarget,
    T_RestrictInfo,
    T_PlaceHolderVar,
    T_SpecialJoinInfo,
    T_AppendRelInfo,
    T_PlaceHolderInfo,
    T_MinMaxAggInfo,
    T_PlannerParamItem,
#ifdef XCP
    T_RemoteSubPath,
#endif
    T_RollupData,
    T_GroupingSetData,
    T_StatisticExtInfo,

    /*
     * TAGS FOR MEMORY NODES (memnodes.h)
     */
    T_MemoryContext,
    T_AllocSetContext,
    T_SlabContext,

    /*
     * TAGS FOR VALUE NODES (value.h)
     */
    T_Value,
    T_Integer,
    T_Float,
    T_String,
    T_BitString,
    T_Null,

    /*
     * TAGS FOR LIST NODES (pg_list.h)
     */
    T_List,
    T_IntList,
    T_OidList,

    /*
     * TAGS FOR EXTENSIBLE NODES (extensible.h)
     */
    T_ExtensibleNode,

    /*
     * TAGS FOR STATEMENT NODES (mostly in parsenodes.h)
     */
    T_RawStmt,
    T_Query,
    T_PlannedStmt,
    T_InsertStmt,
    T_DeleteStmt,
    T_UpdateStmt,
    T_SelectStmt,
    T_AlterTableStmt,
    T_AlterTableCmd,
    T_AlterDomainStmt,
    T_SetOperationStmt,
    T_GrantStmt,
    T_GrantRoleStmt,
    T_AlterDefaultPrivilegesStmt,
    T_ClosePortalStmt,
    T_ClusterStmt,
    T_CopyStmt,
    T_CreateStmt,
    T_DefineStmt,
    T_DropStmt,
    T_TruncateStmt,
    T_CommentStmt,
    T_FetchStmt,
    T_IndexStmt,
    T_CreateFunctionStmt,
    T_AlterFunctionStmt,
    T_DoStmt,
    T_RenameStmt,
    T_RuleStmt,
    T_NotifyStmt,
    T_ListenStmt,
    T_UnlistenStmt,
    T_TransactionStmt,
    T_ViewStmt,
    T_LoadStmt,
    T_CreateDomainStmt,
    T_CreatedbStmt,
    T_DropdbStmt,
    T_VacuumStmt,
#ifdef _SHARDING_
    T_VacuumShardStmt,
#endif
    T_ExplainStmt,
    T_CreateTableAsStmt,
    T_CreateSeqStmt,
    T_AlterSeqStmt,
    T_VariableSetStmt,
    T_VariableShowStmt,
    T_DiscardStmt,
    T_CreateTrigStmt,
    T_CreatePLangStmt,
    T_CreateRoleStmt,
    T_AlterRoleStmt,
    T_DropRoleStmt,
    T_LockStmt,
    T_ConstraintsSetStmt,
    T_ReindexStmt,
    T_CheckPointStmt,
#ifdef PGXC
    T_BarrierStmt,
    T_PauseClusterStmt,
#endif
    T_CreateSchemaStmt,
    T_AlterDatabaseStmt,
    T_AlterDatabaseSetStmt,
    T_AlterRoleSetStmt,
    T_CreateConversionStmt,
    T_CreateCastStmt,
    T_CreateOpClassStmt,
    T_CreateOpFamilyStmt,
    T_AlterOpFamilyStmt,
    T_PrepareStmt,
    T_ExecuteStmt,
    T_DeallocateStmt,
    T_DeclareCursorStmt,
    T_CreateTableSpaceStmt,
    T_DropTableSpaceStmt,
    T_AlterObjectDependsStmt,
    T_AlterObjectSchemaStmt,
    T_AlterOwnerStmt,
    T_AlterOperatorStmt,
    T_DropOwnedStmt,
    T_ReassignOwnedStmt,
    T_CompositeTypeStmt,
    T_CreateEnumStmt,
    T_CreateRangeStmt,
    T_AlterEnumStmt,
    T_AlterTSDictionaryStmt,
    T_AlterTSConfigurationStmt,
    T_CreateFdwStmt,
    T_AlterFdwStmt,
    T_CreateForeignServerStmt,
    T_AlterForeignServerStmt,
    T_CreateUserMappingStmt,
    T_AlterUserMappingStmt,
    T_DropUserMappingStmt,
    T_ExecDirectStmt,
    T_CleanConnStmt,
#ifdef XCP
    T_RemoteStmt,
#endif
    T_AlterTableSpaceOptionsStmt,
    T_AlterTableMoveAllStmt,
    T_SecLabelStmt,
    T_CreateForeignTableStmt,
    T_ImportForeignSchemaStmt,
    T_CreateExtensionStmt,
    T_AlterExtensionStmt,
    T_AlterExtensionContentsStmt,
    T_CreateEventTrigStmt,
    T_AlterEventTrigStmt,
    T_RefreshMatViewStmt,
    T_ReplicaIdentityStmt,
    T_AlterSystemStmt,
    T_CreatePolicyStmt,
    T_AlterPolicyStmt,
    T_CreateTransformStmt,
    T_CreateAmStmt,
    T_CreatePublicationStmt,
    T_AlterPublicationStmt,
    T_CreateSubscriptionStmt,
    T_AlterSubscriptionStmt,
    T_DropSubscriptionStmt,
    T_CreateStatsStmt,
    T_AlterCollationStmt,

    /*
     * TAGS FOR PARSE TREE NODES (parsenodes.h)
     */
    T_A_Expr,
    T_ColumnRef,
    T_ParamRef,
    T_A_Const,
    T_FuncCall,
    T_A_Star,
    T_A_Indices,
    T_A_Indirection,
    T_A_ArrayExpr,
    T_ResTarget,
    T_MultiAssignRef,
    T_TypeCast,
    T_CollateClause,
    T_SortBy,
    T_WindowDef,
    T_RangeSubselect,
    T_RangeFunction,
    T_RangeTableSample,
    T_RangeTableFunc,
    T_RangeTableFuncCol,
    T_TypeName,
    T_ColumnDef,
    T_IndexElem,
    T_Constraint,
    T_DefElem,
    T_RangeTblEntry,
    T_RangeTblFunction,
    T_TableSampleClause,
    T_WithCheckOption,
    T_SortGroupClause,
    T_GroupingSet,
    T_WindowClause,
    T_ObjectWithArgs,
    T_AccessPriv,
    T_CreateOpClassItem,
    T_TableLikeClause,
    T_FunctionParameter,
    T_LockingClause,
    T_RowMarkClause,
    T_XmlSerialize,
    T_WithClause,
    T_InferClause,
    T_OnConflictClause,
    T_CommonTableExpr,
    T_RoleSpec,
    T_TriggerTransition,
    T_PartitionElem,
    T_PartitionSpec,
    T_PartitionBoundSpec,
    T_PartitionRangeDatum,
    T_PartitionCmd,

    /*
     * TAGS FOR REPLICATION GRAMMAR PARSE NODES (replnodes.h)
     */
    T_IdentifySystemCmd,
    T_BaseBackupCmd,
    T_CreateReplicationSlotCmd,
    T_DropReplicationSlotCmd,
    T_StartReplicationCmd,
    T_TimeLineHistoryCmd,
    T_SQLCmd,

    /*
     * TAGS FOR RANDOM OTHER STUFF
     *
     * These are objects that aren't part of parse/plan/execute node tree
     * structures, but we give them NodeTags anyway for identification
     * purposes (usually because they are involved in APIs where we want to
     * pass multiple object types through the same pointer).
     */
    T_TriggerData,                /* in commands/trigger.h */
    T_EventTriggerData,            /* in commands/event_trigger.h */
    T_ReturnSetInfo,            /* in nodes/execnodes.h */
    T_WindowObjectData,            /* private in nodeWindowAgg.c */
    T_TIDBitmap,                /* in nodes/tidbitmap.h */
    T_InlineCodeBlock,            /* in nodes/parsenodes.h */
    T_FdwRoutine,                /* in foreign/fdwapi.h */
    T_IndexAmRoutine,            /* in access/amapi.h */
    T_TsmRoutine,                /* in access/tsmapi.h */
    T_ForeignKeyCacheInfo        /* in utils/rel.h */
#ifdef _MLS_
    ,T_SyncBufIdInfo            /* in bufmgr.c*/
#endif
	 ,T_StatSyncOpt
} NodeTag;

/*
 * The first field of a node of any type is guaranteed to be the NodeTag.
 * Hence the type of any node can be gotten by casting it to Node. Declaring
 * a variable to be of Node * (instead of void *) can also facilitate
 * debugging.
 */
typedef struct Node
{
    NodeTag        type;
} Node;

#define nodeTag(nodeptr)        (((const Node*)(nodeptr))->type)

/*
 * newNode -
 *      create a new node of the specified size and tag the node with the
 *      specified tag.
 *
 * !WARNING!: Avoid using newNode directly. You should be using the
 *      macro makeNode.  eg. to create a Query node, use makeNode(Query)
 *
 * Note: the size argument should always be a compile-time constant, so the
 * apparent risk of multiple evaluation doesn't matter in practice.
 */
#ifdef __GNUC__

/* With GCC, we can use a compound statement within an expression */
#define newNode(size, tag) \
({    Node   *_result; \
    AssertMacro((size) >= sizeof(Node));        /* need the tag, at least */ \
    _result = (Node *) palloc0fast(size); \
    _result->type = (tag); \
    _result; \
})
#else

/*
 *    There is no way to dereference the palloc'ed pointer to assign the
 *    tag, and also return the pointer itself, so we need a holder variable.
 *    Fortunately, this macro isn't recursive so we just define
 *    a global variable for this purpose.
 */
extern PGDLLIMPORT Node *newNodeMacroHolder;

#define newNode(size, tag) \
( \
    AssertMacro((size) >= sizeof(Node)),        /* need the tag, at least */ \
    newNodeMacroHolder = (Node *) palloc0fast(size), \
    newNodeMacroHolder->type = (tag), \
    newNodeMacroHolder \
)
#endif                            /* __GNUC__ */


#define makeNode(_type_)        ((_type_ *) newNode(sizeof(_type_),T_##_type_))
#define NodeSetTag(nodeptr,t)    (((Node*)(nodeptr))->type = (t))

#define IsA(nodeptr,_type_)        (nodeTag(nodeptr) == T_##_type_)

/*
 * castNode(type, ptr) casts ptr to "type *", and if assertions are enabled,
 * verifies that the node has the appropriate type (using its nodeTag()).
 *
 * Use an inline function when assertions are enabled, to avoid multiple
 * evaluations of the ptr argument (which could e.g. be a function call).
 */
#ifdef USE_ASSERT_CHECKING
static inline Node *
castNodeImpl(NodeTag type, void *ptr)
{
    Assert(ptr == NULL || nodeTag(ptr) == type);
    return (Node *) ptr;
}
#define castNode(_type_, nodeptr) ((_type_ *) castNodeImpl(T_##_type_, nodeptr))
#else
#define castNode(_type_, nodeptr) ((_type_ *) (nodeptr))
#endif                            /* USE_ASSERT_CHECKING */


/* ----------------------------------------------------------------
 *                      extern declarations follow
 * ----------------------------------------------------------------
 */

/*
 * nodes/{outfuncs.c,print.c}
 */
#ifdef XCP
extern void set_portable_output(bool value);
#endif
struct Bitmapset;                /* not to include bitmapset.h here */
struct StringInfoData;            /* not to include stringinfo.h here */

extern void outNode(struct StringInfoData *str, const void *obj);
extern void outToken(struct StringInfoData *str, const char *s);
extern void outBitmapset(struct StringInfoData *str,
             const struct Bitmapset *bms);
extern void outDatum(struct StringInfoData *str, uintptr_t value,
         int typlen, bool typbyval);
extern char *nodeToString(const void *obj);
extern char *bmsToString(const struct Bitmapset *bms);

/*
 * nodes/{readfuncs.c,read.c}
 */
#ifdef XCP
extern bool set_portable_input(bool value);
#endif
extern void *stringToNode(char *str);
extern struct Bitmapset *readBitmapset(void);
extern uintptr_t readDatum(bool typbyval);
extern bool *readBoolCols(int numCols);
extern int *readIntCols(int numCols);
extern Oid *readOidCols(int numCols);
extern int16 *readAttrNumberCols(int numCols);

/*
 * nodes/copyfuncs.c
 */
extern void *copyObjectImpl(const void *obj);

/* cast result back to argument type, if supported by compiler */
#ifdef HAVE_TYPEOF
#define copyObject(obj) ((typeof(obj)) copyObjectImpl(obj))
#else
#define copyObject(obj) copyObjectImpl(obj)
#endif

/*
 * nodes/equalfuncs.c
 */
extern bool equalDistribution(const void *a, const void *b);
extern bool equal(const void *a, const void *b);


/*
 * Typedefs for identifying qualifier selectivities and plan costs as such.
 * These are just plain "double"s, but declaring a variable as Selectivity
 * or Cost makes the intent more obvious.
 *
 * These could have gone into plannodes.h or some such, but many files
 * depend on them...
 */
typedef double Selectivity;        /* fraction of tuples a qualifier will pass */
typedef double Cost;            /* execution cost (in page-access units) */


/*
 * CmdType -
 *      enums for type of operation represented by a Query or PlannedStmt
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum CmdType
{
    CMD_UNKNOWN,
    CMD_SELECT,                    /* select stmt */
    CMD_UPDATE,                    /* update stmt */
    CMD_INSERT,                    /* insert stmt */
    CMD_DELETE,
    CMD_UTILITY,                /* cmds like create, destroy, copy, vacuum,
                                 * etc. */
    CMD_NOTHING                    /* dummy command for instead nothing rules
                                 * with qual */
} CmdType;


/*
 * JoinType -
 *      enums for types of relation joins
 *
 * JoinType determines the exact semantics of joining two relations using
 * a matching qualification.  For example, it tells what to do with a tuple
 * that has no match in the other relation.
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum JoinType
{
	/*
	 * The canonical kinds of joins according to the SQL JOIN syntax. Only
	 * these codes can appear in parser output (e.g., JoinExpr nodes).
	 */
	JOIN_INNER,					/* matching tuple pairs only */
	JOIN_LEFT,					/* pairs + unmatched LHS tuples */
	JOIN_FULL,					/* pairs + unmatched LHS + unmatched RHS */
	JOIN_RIGHT,					/* pairs + unmatched RHS tuples */

	/*
	 * Semijoins and anti-semijoins (as defined in relational theory) do not
	 * appear in the SQL JOIN syntax, but there are standard idioms for
	 * representing them (e.g., using EXISTS).  The planner recognizes these
	 * cases and converts them to joins.  So the planner and executor must
	 * support these codes.  NOTE: in JOIN_SEMI output, it is unspecified
	 * which matching RHS row is joined to.  In JOIN_ANTI output, the row is
	 * guaranteed to be null-extended.
	 */
	JOIN_SEMI,					/* 1 copy of each LHS row that has match(es) */
	JOIN_ANTI,					/* 1 copy of each LHS row that has no match */

	/*
	 * These codes are used internally in the planner, but are not supported
	 * by the executor (nor, indeed, by most of the planner).
	 */
	JOIN_UNIQUE_OUTER,			/* LHS path must be made unique */
	JOIN_UNIQUE_INNER,			/* RHS path must be made unique */

#ifdef __OPENTENBASE__
	JOIN_LEFT_SCALAR,			/* pairs + unmatched LHS tuples, only 1 copy of
	 	 	 	 	 	 	 	 * each LHS row else report error. */
	JOIN_LEFT_SEMI				/* 1 copy of each LHS row that has match(es) +
								 * unmatched LHS tuples */
#endif

	/*
	 * We might need additional join types someday.
	 */
} JoinType;

/*
 * OUTER joins are those for which pushed-down quals must behave differently
 * from the join's own quals.  This is in fact everything except INNER and
 * SEMI joins.  However, this macro must also exclude the JOIN_UNIQUE symbols
 * since those are temporary proxies for what will eventually be an INNER
 * join.
 *
 * Note: semijoins are a hybrid case, but we choose to treat them as not
 * being outer joins.  This is okay principally because the SQL syntax makes
 * it impossible to have a pushed-down qual that refers to the inner relation
 * of a semijoin; so there is no strong need to distinguish join quals from
 * pushed-down quals.  This is convenient because for almost all purposes,
 * quals attached to a semijoin can be treated the same as innerjoin quals.
 */
#ifdef __OPENTENBASE__
#define IS_OUTER_JOIN(jointype) \
	(((1 << (jointype)) & \
	  ((1 << JOIN_LEFT) | \
	   (1 << JOIN_LEFT_SEMI) | \
	   (1 << JOIN_LEFT_SCALAR) | \
	   (1 << JOIN_FULL) | \
	   (1 << JOIN_RIGHT) | \
	   (1 << JOIN_ANTI))) != 0)
#else
#define IS_OUTER_JOIN(jointype) \
	(((1 << (jointype)) & \
	  ((1 << JOIN_LEFT) | \
	   (1 << JOIN_FULL) | \
	   (1 << JOIN_RIGHT) | \
	   (1 << JOIN_ANTI))) != 0)
#endif

/*
 * AggStrategy -
 *      overall execution strategies for Agg plan nodes
 *
 * This is needed in both plannodes.h and relation.h, so put it here...
 */
typedef enum AggStrategy
{
    AGG_PLAIN,                    /* simple agg across all input rows */
    AGG_SORTED,                    /* grouped agg, input must be sorted */
    AGG_HASHED,                    /* grouped agg, use internal hashtable */
    AGG_MIXED                    /* grouped agg, hash and sort both used */
} AggStrategy;

/*
 * AggSplit -
 *      splitting (partial aggregation) modes for Agg plan nodes
 *
 * This is needed in both plannodes.h and relation.h, so put it here...
 */

/* Primitive options supported by nodeAgg.c: */
#define AGGSPLITOP_COMBINE        0x01    /* substitute combinefn for transfn */
#define AGGSPLITOP_SKIPFINAL    0x02    /* skip finalfn, return state as-is */
#define AGGSPLITOP_SERIALIZE    0x04    /* apply serializefn to output */
#define AGGSPLITOP_DESERIALIZE    0x08    /* apply deserializefn to input */

/* Supported operating modes (i.e., useful combinations of these options): */
typedef enum AggSplit
{
    /* Basic, non-split aggregation: */
    AGGSPLIT_SIMPLE = 0,
    /* Initial phase of partial aggregation, with serialization: */
    AGGSPLIT_INITIAL_SERIAL = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE,
    /* Final phase of partial aggregation, with deserialization: */
    AGGSPLIT_FINAL_DESERIAL = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE,
    /* Combine phase of partial aggregation, with both steps: */
    AGGSPLIT_COMBINE = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE | \
                       AGGSPLITOP_SERIALIZE | AGGSPLITOP_SKIPFINAL
} AggSplit;

/* Test whether an AggSplit value selects each primitive option: */
#define DO_AGGSPLIT_COMBINE(as)        (((as) & AGGSPLITOP_COMBINE) != 0)
#define DO_AGGSPLIT_SKIPFINAL(as)    (((as) & AGGSPLITOP_SKIPFINAL) != 0)
#define DO_AGGSPLIT_SERIALIZE(as)    (((as) & AGGSPLITOP_SERIALIZE) != 0)
#define DO_AGGSPLIT_DESERIALIZE(as) (((as) & AGGSPLITOP_DESERIALIZE) != 0)

/*
 * SetOpCmd and SetOpStrategy -
 *      overall semantics and execution strategies for SetOp plan nodes
 *
 * This is needed in both plannodes.h and relation.h, so put it here...
 */
typedef enum SetOpCmd
{
    SETOPCMD_INTERSECT,
    SETOPCMD_INTERSECT_ALL,
    SETOPCMD_EXCEPT,
    SETOPCMD_EXCEPT_ALL
} SetOpCmd;

typedef enum SetOpStrategy
{
    SETOP_SORTED,                /* input must be sorted */
    SETOP_HASHED                /* use internal hashtable */
} SetOpStrategy;

/*
 * OnConflictAction -
 *      "ON CONFLICT" clause type of query
 *
 * This is needed in both parsenodes.h and plannodes.h, so put it here...
 */
typedef enum OnConflictAction
{
    ONCONFLICT_NONE,            /* No "ON CONFLICT" clause */
    ONCONFLICT_NOTHING,            /* ON CONFLICT ... DO NOTHING */
    ONCONFLICT_UPDATE            /* ON CONFLICT ... DO UPDATE */
} OnConflictAction;

#endif                            /* NODES_H */
