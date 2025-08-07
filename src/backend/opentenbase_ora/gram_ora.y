%{

/*#define YYDEBUG 1*/
/*-------------------------------------------------------------------------
 *
 * gram_ora.y
 *	  OPENTENBASE_ORA BISON rules/actions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/opentenbase_ora/gram_ora.y
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Sept, 1994		POSTQUEL to SQL conversion
 *	  Andrew Yu			Oct, 1994		lispy code conversion
 *
 * NOTES
 *	  CAPITALS are used to represent terminal symbols.
 *	  non-capitals are used to represent non-terminals.
 *
 *	  In general, nothing in this file should initiate database accesses
 *	  nor depend on changeable state (such as SET variables).  If you do
 *	  database accesses, your code will fail when we have aborted the
 *	  current transaction and are just parsing commands to find the next
 *	  ROLLBACK or COMMIT.  If you make use of SET variables, then you
 *	  will do the wrong thing in multi-query strings like this:
 *			SET constraint_exclusion TO off; SELECT * FROM foo;
 *	  because the entire string is parsed by gram.y before the SET gets
 *	  executed.  Anything that depends on the database or changeable state
 *	  should be handled during parse analysis so that it happens at the
 *	  right time not the wrong time.
 *
 * WARNINGS
 *	  If you use a list, make sure the datum is a node so that the printing
 *	  routines work.
 *
 *	  Sometimes we assign constants to makeStrings. Make sure we don't free
 *	  those.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "catalog/pg_directory.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "commands/trigger.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/gramparse_extra.h"
#include "opentenbase_ora/gram_ora.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "storage/lmgr.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/xml.h"
#include "storage/nodelock.h"
#include "parser/scansup.h"
#include "executor/spi.h"

#ifdef _PG_ORCL_
#include "utils/guc.h"
#include "port.h"
#include "catalog/pg_profile.h"
#include "catalog/pg_dblink.h"
#endif

#ifdef __AUDIT__
#include "audit/audit.h"
#endif
#ifdef _MLS_
#include "utils/mls.h"
#endif
/*
 * Location tracking support --- simpler than bison's default, since we only
 * want to track the start position not the end position of each nonterminal.
 */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if ((N) > 0) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (-1); \
	} while (0)

/*
 * The above macro assigns -1 (unknown) as the parse location of any
 * nonterminal that was reduced from an empty rule, or whose leftmost
 * component was reduced from an empty rule.  This is problematic
 * for nonterminals defined like
 *		OptFooList: / * EMPTY * / { ... } | OptFooList Foo { ... } ;
 * because we'll set -1 as the location during the first reduction and then
 * copy it during each subsequent reduction, leaving us with -1 for the
 * location even when the list is not empty.  To fix that, do this in the
 * action for the nonempty rule(s):
 *		if (@$ < 0) @$ = @2;
 * (Although we have many nonterminals that follow this pattern, we only
 * bother with fixing @$ like this when the nonterminal's parse location
 * is actually referenced in some rule.)
 *
 * A cleaner answer would be to make YYLLOC_DEFAULT scan all the Rhs
 * locations until it's found one that's not -1.  Then we'd get a correct
 * location for any nonterminal that isn't entirely empty.  But this way
 * would add overhead to every rule reduction, and so far there's not been
 * a compelling reason to pay that overhead.
 */

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

/* Private struct for the result of privilege_target production */
typedef struct PrivTarget
{
	GrantTargetType targtype;
	GrantObjectType objtype;
	List	   *objs;
} PrivTarget;

/* Private struct for the result of import_qualification production */
typedef struct ImportQual
{
	ImportForeignSchemaType type;
	List	   *table_names;
} ImportQual;

/* Private struct for the result of opt_select_limit production */
typedef struct SelectLimit
{
	Node *limitOffset;
	Node *limitCount;
	LimitOption limitOption;
} SelectLimit;

/*Support having clause before group by*/
typedef struct GroupHaving
{
	Node *having;
	List *groupby;
} GroupHaving;

/* ConstraintAttributeSpec yields an integer bitmask of these flags: */
#define CAS_NOT_DEFERRABLE			0x01
#define CAS_DEFERRABLE				0x02
#define CAS_INITIALLY_IMMEDIATE		0x04
#define CAS_INITIALLY_DEFERRED		0x08
#define CAS_NOT_VALID				0x10
#define CAS_NO_INHERIT				0x20


#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)

extern int base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);
extern int ora_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);
extern int pg_base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner);

static void ora_base_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner,
						 const char *msg);
static RawStmt *makeRawStmt(Node *stmt, int stmt_location);
static void updateRawStmtEnd(RawStmt *rs, int end_location);
static Node *makeColumnRef(char *colname, List *indirection,
						   int location, core_yyscan_t yyscanner);
static Node *makeEmptyStrAConst(int location);
static Node *makeFloatConst(char *str, int location);
static Node *makeBitStringConst(char *str, int location);
static Node *makeAConst(Value *v, int location);
static RoleSpec *makeRoleSpec(RoleSpecType type, int location);
static void check_qualified_name(List *names, core_yyscan_t yyscanner);
static List *check_func_name(List *names, core_yyscan_t yyscanner);
static List *check_indirection(List *indirection, core_yyscan_t yyscanner);
static List *extractArgTypes(ObjectType objtype, List *parameters);
static List *extractAggrArgTypes(List *aggrargs);
static List *makeOrderedSetArgs(List *directargs, List *orderedargs,
								core_yyscan_t yyscanner);
static void insertSelectOptions(SelectStmt *stmt,
								List *sortClause, List *lockingClause,
								SelectLimit *limitClause,
								WithClause *withClause,
								core_yyscan_t yyscanner);
static Node *makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg);
static Node *doNegate(Node *n, int location);
static void doNegateFloat(Value *v);
static Node *makeAndExpr(Node *lexpr, Node *rexpr, int location);
static Node *makeOrExpr(Node *lexpr, Node *rexpr, int location);
static Node *makeNotExpr(Node *expr, int location);
static Node *makeAArrayExpr(List *elements, int location);
static Node *makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod,
								  int location);
static Node *makeXmlExpr(XmlExprOp op, char *name, List *named_args,
						 List *args, int location);
static List *mergeTableFuncParameters(List *func_args, List *columns);
static TypeName *TableFuncTypeName(List *columns);
static RangeVar *makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner);
static void SplitColQualList(List *qualList,
							 List **constraintList, CollateClause **collClause,
							 core_yyscan_t yyscanner);
static void processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner);
static char* StrConcat(const char* str1, const char* str2);
static Node *makeRecursiveViewSelect(char *relname, List *aliases, Node *query);
static Node *reparse_decode_func(List *args, int location);

%}

%pure-parser
%expect 0
%name-prefix="ora_base_yy"
%locations

%parse-param {core_yyscan_t yyscanner}
%lex-param   {core_yyscan_t yyscanner}

%union
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	ValidateBehavior    vbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs		*objwithargs;
	DefElem				*defelt;
	AccessibleParameter   *acc_param;
	SortBy				*sortby;
	WindowDef			*windef;
	KeepDef				*keepdef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
/* PGXC_BEGIN */
	DistributeBy		*distby;
	PGXCSubCluster		*subclus;
/* PGXC_END */
	A_Const				*a_const;
	PartitionElem		*partelem;
	PartitionSpec		*partspec;
	PartitionBoundSpec	*partboundspec;
	RoleSpec			*rolespec;
	MergeWhenClause		*mergewhen;
	PartitionInto		*partinto;
	struct SelectLimit	*selectlimit;
	struct GroupHaving  *group_having;
}

%type <node>	stmt schema_stmt
		AlterEventTrigStmt AlterCollationStmt
		AlterDatabaseStmt AlterDatabaseSetStmt AlterDomainStmt AlterEnumStmt
		AlterFdwStmt AlterForeignServerStmt AlterGroupStmt
		AlterObjectDependsStmt AlterObjectSchemaStmt AlterOwnerStmt
		AlterOperatorStmt AlterSeqStmt AlterSystemStmt AlterTableStmt
		AlterTblSpcStmt AlterExtensionStmt AlterExtensionContentsStmt AlterForeignTableStmt
		AlterCompositeTypeStmt AlterUserMappingStmt
		AlterRoleStmt AlterRoleSetStmt AlterPolicyStmt
		AlterDefaultPrivilegesStmt AuditStmt DefACLAction
		AnalyzeStmt CleanAuditStmt CleanConnStmt CallStmt ClosePortalStmt ClusterStmt CommentStmt
		ConstraintsSetStmt CopyStmt CreateAsStmt CreateCastStmt
		CreateDomainStmt CreateExtensionStmt CreateGroupStmt CreateOpClassStmt
		CreateOpFamilyStmt AlterOpFamilyStmt CreatePLangStmt
		CreateSchemaStmt CreateSeqStmt CreateStmt CreateStatsStmt CreateTableSpaceStmt
		CreateFdwStmt CreateForeignServerStmt CreateForeignTableStmt
		CreateAssertStmt CreateTransformStmt CreateTrigStmt CreateEventTrigStmt
		CreateUserStmt CreateDirStmt CreateUserMappingStmt CreateRoleStmt CreatePolicyStmt
		CreatedbStmt DeclareCursorStmt DefineStmt DeleteStmt DiscardStmt DoStmt
		DropOpClassStmt DropOpFamilyStmt DropPLangStmt DropStmt
		DropAssertStmt DropCastStmt DropRoleStmt
		DropdbStmt DropDirStmt DropTableSpaceStmt
		DropTransformStmt
		DropUserMappingStmt ExplainStmt ExecDirectStmt FetchStmt
		GrantStmt GrantRoleStmt ImportForeignSchemaStmt IndexStmt InsertStmt
		ListenStmt LoadStmt LockStmt NotifyStmt ExplainableStmt PreparableStmt
		CreateFunctionStmt AlterFunctionStmt ReindexStmt RemoveAggrStmt
		RemoveFuncStmt RemoveOperStmt RenameStmt RevokeStmt RevokeRoleStmt
		RuleActionStmt RuleActionStmtOrEmpty RuleStmt
		SecLabelStmt SelectStmt TransactionStmt TruncateStmt
		UnlistenStmt UpdateStmt VacuumStmt
		VariableResetStmt VariableSetStmt VariableShowStmt
		ViewStmt CheckPointStmt CreateConversionStmt
		DeallocateStmt PrepareStmt ExecuteStmt
		DropOwnedStmt ReassignOwnedStmt
		AlterTSConfigurationStmt AlterTSDictionaryStmt
		BarrierStmt PauseStmt AlterNodeStmt CreateNodeStmt DropNodeStmt
		CreateNodeGroupStmt DropNodeGroupStmt
		CreateMatViewStmt RefreshMatViewStmt CreateAmStmt
		CreatePublicationStmt AlterPublicationStmt
		CreateSubscriptionStmt AlterSubscriptionStmt DropSubscriptionStmt
		CreateShardStmt MoveDataStmt DropShardStmt CleanShardingStmt
		AlterNodeGroupStmt LockNodeStmt CreateResourceQueueStmt DropResourceQueueStmt AlterResourceQueueStmt
		SampleStmt CreateProfileStmt AlterProfileStmt DropProfileStmt AlterResourceCostStmt
		CreatePackageStmt DeclareFuncInPkgStmt CreateFuncInPkgStmt DropPackageStmt LoadFromStmt MergeStmt CreateSynonymStmt DropSynonymStmt
		AlterPackageStmt CreateFuncInTypeObjStmt DeclareFuncInTypeObjStmts

%type <node>	DeclObjCons ObjFuncDeclElem

/* OpenTenBase-specific commands */
%type <node>	AlterResourceGroupStmt
		CreateExternalStmt CreateResourceGroupStmt
		DropResourceGroupStmt
		ExtTypedesc OptExtSingleRowErrorHandling

%type <node>	select_no_parens select_with_parens select_clause
				simple_select values_clause

%type <node>	alter_column_default opclass_item opclass_drop alter_using
%type <ival>	add_drop opt_asc_desc opt_nulls_order opt_materialized
%type <node>	alter_table_cmd alter_type_cmd opt_collate_clause
	   replica_identity partition_cmd alter_group_cmd index_partition_cmd
%type <list>	alter_table_cmds alter_type_cmds alter_group_cmds
%type <list>    alter_identity_column_option_list compiler_items
%type <defelt>  alter_identity_column_option

%type <dbehavior>	opt_drop_behavior
%type <vbehavior>	opt_validate

%type <list>	createdb_opt_list createdb_opt_items copy_opt_list
				transaction_mode_list
				create_extension_opt_list alter_extension_opt_list
				pgxcnode_list pgxcnodes
%type <defelt>	createdb_opt_item copy_opt_item
				transaction_mode_item
				create_extension_opt_item alter_extension_opt_item

%type <list>	ext_on_clause_list format_opt format_opt_list format_def_list
				ext_options ext_options_opt ext_options_list
				ext_opt_encoding_list
%type <defelt>	ext_on_clause_item format_opt_item format_def_item
				ext_options_item
				ext_opt_encoding_item

%type <ival>	opt_lock lock_type cast_context
%type <defelt>	drop_option
%type <ival>	vacuum_option_list vacuum_option_elem
%type <ival>	analyze_option_list analyze_option_elem
%type <boolean>	opt_force opt_or_replace opt_or_public
				opt_grant_grant_option opt_grant_admin_option
				opt_nowait opt_if_exists opt_with_data opt_with_cut
%type <ival>	opt_nowait_or_skip opt_wait_second opt_pkg_comp

%type <node>	opt_truncation_indicator opt_with_count_expr
%type <node> 	TypeRecordFieldElement
%type <list> 	TypeRecordFieldElementList TypeRecordFieldAttrs
%type <typnam>	TypeRFname TRFSimpleTypename

%type <list>	OptRoleList AlterOptRoleList
%type <defelt>	CreateOptRoleElem AlterOptRoleElem

%type <str>		opt_type
%type <str>		foreign_server_version opt_foreign_server_version
%type <str>		opt_in_database

%type <str>		OptSchemaName opt_dblink
%type <list>	OptSchemaEltList

%type <boolean> TriggerForSpec TriggerForType opt_debug opt_reuse ForeignTblWritable
%type <ival>	TriggerActionTime
%type <list>	TriggerEvents TriggerOneEvent
%type <value>	TriggerFuncArg
%type <node>	TriggerWhen copy_col_fixed_len
%type <str>		TransitionRelName
%type <boolean>	TransitionRowOrTable TransitionOldOrNew
%type <node>	TriggerTransition

%type <list>	event_trigger_when_list event_trigger_value_list
%type <defelt>	event_trigger_when_item
%type <chr>		enable_trigger

%type <str>		copy_file_name cmp_op cmp_eq
				database_name access_method_clause access_method attr_name
				name cursor_name file_name
				index_name opt_index_name cluster_index_specification
				pgxcnode_name pgxcgroup_name directory_name

%type <list>	func_name handler_name qual_Op qual_all_Op subquery_Op
				opt_class opt_inline_handler opt_validator validator_clause
				opt_collate func_name_no_parens

%type <range>	qualified_name pg_qualified_name insert_target OptConstrFromTable pkg_qualified_name opentenbase_ora_qualified_name
%type <range>	qualified_ref opentenbase_ora_qualified_ref
%type <str>		all_Op MathOp

%type <str>		row_security_cmd RowSecurityDefaultForCmd
%type <boolean> RowSecurityDefaultPermissive
%type <node>	RowSecurityOptionalWithCheck RowSecurityOptionalExpr
%type <list>	RowSecurityDefaultToRole RowSecurityOptionalToRole

%type <str>		iso_level opt_encoding Identified_plane
%type <rolespec> grantee
%type <list>	grantee_list
%type <accesspriv> privilege
%type <list>	privileges privilege_list
%type <privtarget> privilege_target
%type <objwithargs> function_with_argtypes aggregate_with_argtypes operator_with_argtypes procedure_with_argtypes function_with_argtypes_common
%type <list>	function_with_argtypes_list aggregate_with_argtypes_list operator_with_argtypes_list procedure_with_argtypes_list
%type <ival>	defacl_privilege_target
%type <defelt>	DefACLOption
%type <list>	DefACLOptionList
%type <ival>	import_qualification_type
%type <importqual> import_qualification
%type <selectlimit> opt_select_limit select_limit limit_clause

%type <list>	stmtblock stmtmulti
				OptTableElementList TableElementList OptInherit definition
				OptExtTableElementList ExtTableElementList ExtTableElementListWithPos
				OptTypedTableElementList TypedTableElementList
				reloptions opt_reloptions
				OptWith distinct_clause opt_all_clause opt_definition func_args func_args_list
				func_args_with_defaults func_args_with_defaults_list
				aggr_args aggr_args_list
				func_as createfunc_opt_list alterfunc_opt_list
				old_aggr_definition old_aggr_list
				oper_argtypes RuleActionList RuleActionMulti
				cdb_string_list
				opt_column_list column_list_copy_without_pos column_list_copy_with_pos columnList columnList_copy_with_pos columnList_copy_without_pos opt_name_list
				sort_clause opt_sort_clause sortby_list index_params
				name_list role_list from_clause from_list opt_array_bounds
				qualified_name_list any_name any_name_list type_name_list
				any_operator expr_list attrs opt_number number_expr_list
				target_list opt_target_list insert_column_list set_target_list
				set_clause_list set_clause
				def_list operator_def_list indirection opt_indirection opt_indirection_ora
				reloption_list group_clause TriggerFuncArgs opclass_item_list opclass_drop_list
				opclass_purpose opt_opfamily transaction_mode_list_or_empty
				OptTableFuncElementList TableFuncElementList opt_type_modifiers
				prep_type_clause
				execute_param_clause using_clause returning_clause
				opt_enum_val_list enum_val_list table_func_column_list
				create_generic_options alter_generic_options
				relation_expr_list dostmt_opt_list
				transform_element_list transform_type_list
				TriggerTransitions TriggerReferencing
				publication_name_list lock_param_list
				pkg_as createpkg_opt_list accessible_args accessible_args_list
				ctext_expr_list ctext_row merge_values_clause
				drop_option_list copy_foramtter
				opt_createtypobj_opt_list createtypobj_opt_list
				typobj_any_name

%type <list>	group_by_list
%type <node>	group_by_item empty_grouping_set rollup_clause cube_clause
%type <node>	grouping_sets_clause
%type <node>	opt_publication_for_tables publication_for_tables opt_publication_for_shards publication_for_shards
%type <value>	publication_name_item

%type <list>	opt_fdw_options fdw_options
%type <defelt>	fdw_option

%type <range>	OptTempTableName
%type <into>	into_clause create_as_target create_mv_target

%type <defelt>	createfunc_opt_item common_func_opt_item dostmt_opt_item createpkg_opt_item typpkg_opt_item
%type <acc_param> accessible_arg
%type <fun_param> func_arg func_arg_with_default table_func_column aggr_arg
%type <fun_param_mode> arg_class
%type <typnam>	func_return func_type

%type <boolean>  OptWeb OptWritable OptSrehLimitType
%type <chr>	 ExtLogErrorTable

%type <boolean>  opt_trusted opt_restart_seqs
%type <ival>	 opt_shardcluster
%type <ival>	 OptTemp
%type <ival>	 OptNoLog
%type <oncommit> OnCommitOption

%type <boolean> opt_editionable

%type <ival>	for_locking_strength
%type <node>	for_locking_item
%type <list>	for_locking_clause opt_for_locking_clause for_locking_items
%type <list>	locked_rels_list
%type <boolean>	all_or_distinct

%type <node>	join_outer join_qual
%type <jtype>	join_type

%type <list>	extract_list overlay_list position_list
%type <list>	substr_list trim_list
%type <list>	opt_interval interval_second
%type <node>	overlay_placing substr_from substr_for
%type <boolean> opt_instead
%type <boolean> opt_unique opt_concurrently opt_verbose opt_full opt_global_clause
%type <boolean> opt_freeze opt_default opt_recheck
%type <defelt>	opt_oids copy_delimiter

%type <str>		DirectStmt CleanConnDbName CleanConnUserName

/* PGXC_END */
%type <boolean> copy_from opt_program

%type <ival>	opt_column opt_timezone event cursor_options opt_hold
                opt_set_data
%type <objtype>	drop_type_any_name drop_type_name drop_type_name_on_any_name
				comment_type_any_name comment_type_name
				security_label_type_any_name security_label_type_name

%type <node>	fetch_args select_limit_value
				offset_clause select_offset_value
				select_fetch_first_value I_or_F_const
%type <ival>	row_or_rows first_or_next

%type <list>	OptSeqOptList SeqOptList OptParenthesizedSeqOptList
%type <defelt>	SeqOptElem

%type <istmt>	insert_rest
%type <infer>	opt_conf_expr
%type <onconflict> opt_on_conflict

%type <vsetstmt> generic_set set_rest set_rest_more generic_reset reset_rest
				 SetResetClause FunctionSetResetClause

%type <node>	TableElement TypedTableElement ConstraintElem TableFuncElement
%type <node>	columnDef columnOptions
%type <defelt>	def_elem reloption_elem old_aggr_elem operator_def_elem compiler_item
%type <node>	c_expr_spec_alais AexprConstAlias c_expr_single_in a_expr_spec_alais
%type <node>	ExtTableElement
%type <node>	ExtcolumnDef ExtcolumnDefWithPos
%type <node>	cdb_string
%type <node>	def_arg columnElem columnElem_copy_with_pos columnElem_copy_without_pos attr_pos ForeignPosition where_clause where_or_current_clause
				a_expr b_expr c_expr AexprConst indirection_el indirection_el_ora opt_slice_bound
				columnref columnref_col in_expr having_clause func_table xmltable array_expr
				ExclusionWhereClause operator_def_arg
%type <list>	rowsfrom_item rowsfrom_list opt_col_def_list
%type <boolean> opt_ordinality
%type <list>	ExclusionConstraintList ExclusionConstraintElem
%type <list>	func_arg_list
%type <node>	func_arg_expr
%type <list>	func_arg_def_on_conv_err_list func_arg_def_on_using_nls
%type <list>	row explicit_row implicit_row type_list array_expr_list
%type <node>	case_expr case_arg when_clause case_default
%type <list>	when_clause_list
%type <ival>	sub_type
%type <node>	ctext_expr
%type <value>	NumericOnly
%type <list>	NumericOnly_list
%type <alias>	alias_clause opt_alias_clause
%type <list>	func_alias_clause
%type <sortby>	sortby
%type <ielem>	index_elem
%type <node>	table_ref merge_relation_expr_opt_alias
%type <jexpr>	joined_table
%type <range>	relation_expr
%type <range>	relation_expr_opt_alias
%type <node>	tablesample_clause opt_repeatable_clause
%type <target>	target_el set_target insert_column_item

%type <str>		generic_option_name
%type <node>	generic_option_arg
%type <defelt>	generic_option_elem alter_generic_option_elem
%type <list>	generic_option_list alter_generic_option_list
%type <str>		explain_option_name
%type <node>	explain_option_arg
%type <defelt>	explain_option_elem
%type <list>	explain_option_list

%type <ival>	reindex_target_type reindex_target_multitable
%type <ival>	reindex_option_list reindex_option_elem

%type <node>	copy_generic_opt_arg copy_generic_opt_arg_list_item
%type <defelt>	copy_generic_opt_elem
%type <list>	copy_generic_opt_list copy_generic_opt_arg_list
%type <list>	copy_options

%type <typnam>	Typename SimpleTypename ConstTypename
				GenericType Numeric opt_float
				Character ConstCharacter
				CharacterWithLength CharacterWithoutLength
				ConstDatetime ConstInterval
				Bit ConstBit BitWithLength BitWithoutLength
%type <str>		character
%type <str>		extract_arg
%type <boolean> opt_varying opt_no_inherit opt_char

%type <ival>	Iconst SignedIconst
%type <str>		Sconst comment_text notify_payload
%type <str>		RoleId opt_boolean_or_string
%type <list>	var_list
%type <str>	AliasId	ColId ColLabel var_name type_function_name param_name PkgId TypObjId
%type <str>		NonReservedWord
%type <str>     opentenbase_ora_ident opentenbase_ora_refident opentenbase_ora_colref_ident opentenbase_ora_coldef_ident
%type <str>		createdb_opt_name opentenbase_ora_any_ident
%type <node>	var_value zone_value opt_illegal_conv_string
%type <rolespec> auth_ident RoleSpec opt_granted_by

%type <ival>		accessible_class

%type <keyword> unreserved_keyword type_func_name_keyword spec_aliasid_keyword a_expr_spec_alais_kw
%type <keyword> col_name_keyword reserved_keyword pkg_reserved_keyword
%type <keyword> opentenbase_ora_unreserved_keyword  opentenbase_ora_alias_unreserved
%type <keyword> opentenbase_ora_coldef_unreserved opentenbase_ora_colref_unreserved opentenbase_ora_any_unreserved
%type <keyword> opentenbase_ora_refunreserved_keyword
%type <keyword> opentenbase_ora_table_alias_unreserved opentenbase_ora_normal_alias_unreserved

%type <node>	TableConstraint TableLikeClause
%type <ival>	TableLikeOptionList TableLikeOption
%type <list>	ColQualList opt_storage_encoding
%type <node>	ColConstraint ColConstraintElem ConstraintAttr
%type <ival>	key_actions key_delete key_match key_update key_action
%type <ival>	ConstraintAttributeSpec ConstraintAttributeElem
%type <str>		ExistingIndex

%type <list>	constraints_set_list
%type <boolean> constraints_set_mode
%type <str>		OptTableSpace OptConsTableSpace
%type <str>		OptAlias
%type <rolespec> OptTableSpaceOwner
%type <node>    DistributedBy OptDistributedBy
%type <ival>	opt_check_option

%type <str>		opt_provider security_label

%type <target>	xml_attribute_el
%type <list>	xml_attribute_list xml_attributes
%type <node>	xml_root_version opt_xml_root_standalone
%type <node>	xmlexists_argument
%type <ival>	document_or_content
%type <ival>	xml_whitespace_or_wellformed_option
%type <list>	xmltable_column_list xmltable_column_option_list xmltable_column_list_opt
%type <node>	xmltable_column_el
%type <defelt>	xmltable_column_option_el
%type <list>	xml_namespace_list
%type <target>	xml_namespace_el

%type <node>	func_application opt_a_expr func_expr_common_subexpr
%type <node>	func_expr func_expr_windowless
%type <node>	common_table_expr
%type <with>	with_clause opt_with_clause
%type <list>	cte_list

%type <list>	within_group_clause
%type <node>	filter_clause
%type <list>	window_clause window_definition_list opt_partition_clause
%type <windef>	window_definition over_clause window_specification
				opt_frame_clause frame_extent frame_bound
%type <str>		opt_existing_window_name

%type <list>	distributed_by_list
%type <list>	distributed_by_elem

/* PGXC_BEGIN */
%type <str>		opt_barrier_id OptDistributeType DistributeStyle
%type <distby>	OptDistributeBy OptDistributeByInternal
%type <subclus> OptSubCluster OptSubClusterInternal
/* PGXC_END */

/* SHARD_BEGIN */
%type <a_const>   	data_node
%type <list>		shard_list opt_shard_list OptDistKey
/* SHARD_END */

%type <boolean> opt_if_not_exists
%type <ival>	generated_when override_kind opt_func_return
%type <partspec>	PartitionSpec OptPartitionSpec
%type <str>			part_strategy
%type <partelem>	part_elem
%type <list>		part_params
%type <partboundspec> PartitionBoundSpec
%type <node>		partbound_datum PartitionRangeDatum
%type <list>       hash_partbound partbound_datum_list range_datum_list
%type <defelt>     hash_partbound_elem

%type <node>	lock_param
%type <group_having> group_having

/* __AUDIT__ BEGIN */
%type <boolean> 	audit_or_noaudit
%type <list>		audit_user_list audit_stmt_list audit_stmts audit_obj
%type <ival>		audit_stmt audit_obj_type opt_when_success_or_not success_or_not
/* __AUDIT__ END */

/* OPENTENBASE_ORA PROFILE BEGIN */
%type <list>           profile_param_list
%type <str>         profile_name
%type <node>        profile_param
%type <value>          profile_value
/* OPENTENBASE_ORA PROFILE END */

/* OPENTENBASE_ORA OptPhyAttrClause BEGIN */
%type <defelt>        	StorageOneClause PhyAttrClauseCommon TableSpaceInPhyAttr
%type <list>		OptPhyAttrClauseList PhyAttrClauseList OnePhyAttrClauseList StorageAllClauseList StorageClauseList
%type <value>		BuffPool FlashCache SizeClause
/* OPENTENBASE_ORA OptPhyAttrClause END */


/* __RESOURCE_QUEUE__ BEGIN */
%type <str>			QueueId
/* __RESOURCE_QUEUE__ END */

%type <list>    OptResourceGroupList CreateOptResourceGroupList
%type <defelt>  OptResourceGroupElem

/* MERGE INTO */
%type <node> merge_when_clause opt_merge_where_condition
%type <list> merge_when_list
%type <mergewhen>	merge_insert merge_update merge_update_delete

/* OPENTENBASE_ORA IDENTIFIER UPPER CASE IN DEFAULT BEGIN */
%type <str>		Lower_IDENT Lower_ColLabel lower_name Lower_NonReservedWord_or_Sconst Lower_ColId Common_IDENT lower_attr_name Lower_NonReservedWord
%type <list>	lower_name_list lower_any_name lower_any_name_list lower_attrs
%type <objtype>	lower_type_any_name lower_comment_type_name lower_security_label_type_name lower_drop_type_name

/*
 * Non-keyword token types.  These are hard-wired into the "flex" lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  PL/pgSQL depends on this so that it can share the
 * same lexer.  If you add/change tokens here, fix PL/pgSQL to match!
 *
 * DOT_DOT is unused in the core SQL grammar, and so will always provoke
 * parse errors.  It is needed by PL/pgSQL.
 */
%token <str>	IDENT IDENT_IN_D_QUOTES FCONST SCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token		TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER OUTER_CONN_TBL PG_PARTITION
%token		LESS_EQUALS GREATER_EQUALS NOT_EQUALS CONCATENATION

/*
 * If you want to make any keyword changes, update the keyword table in
 * src/include/parser/kwlist.h and add new keywords to the appropriate one
 * of the reserved-or-not-so-reserved keyword lists, below; search
 * this file for "Keyword category lists".
 */

/* ordinary key words in alphabetical order */
/* PGXC - added DISTRIBUTE, DISTRIBUTED, DISTSYLE, DISTKEY, RANDOMLY, DIRECT, COORDINATOR, CLEAN,  NODE, BARRIER */
%token <keyword> ABORT_P ABSOLUTE_P ACCESS ACCOUNT ACTION ADD_P ADMIN AFTER
	AGGREGATE ALL ALSO ALTER ALWAYS ANALYSE ANALYZE AND ANY ARRAY AS ASC
	ASSERTION ASSIGNMENT ASYMMETRIC AT ATTACH ATTRIBUTE AUDIT AUTHENTICATED AUTHORIZATION

	BACKWARD BARRIER BEFORE BEGIN_P BEGIN_SUBTXN BETWEEN BIGINT BINARY BINARY_P BINARY_FLOAT BINARY_DOUBLE BINARY_DOUBLE_NAN BINARY_FLOAT_NAN BIT
	BOOLEAN_P BOTH BUFFER_POOL BY BYTE_P

	CACHE CALL CALLED CASCADE CASCADED CASE CAST CATALOG_P CELL_FLASH_CACHE CHAIN CHAR_P
	CHARACTER CHARACTERISTICS CHECK CHECKPOINT CHECKSUM CLASS CLEAN CLOSE
	CLUSTER COALESCE COLLATE COLLATION COLUMN COLUMNS COMMENT COMMENTS COMMIT COMMIT_SUBTXN
	COMMITTED COMPILE CONCURRENTLY CONFIGURATION CONFLICT CONNECT CONNECTION CONSTRAINT
	CONCURRENCY
	CONSTRAINTS CONTENT_P CONSTRUCTOR_P CONTINUE_P CONVERSION_P COORDINATOR COPY COST COUNT CPUSET CPU_RATE_LIMIT CREATE
	CROSS CSV CUBE CURRENT_P
	CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA
	CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR CYCLE AUTHID
	
	COMPATIBLE_ILLEGAL_CHARS FILL_MISSING_FIELDS IGNORE_EXTRA_DATA

	DATA_P DATABASE DAY_P DBTIMEZONE DEALLOCATE DEBUG DEC DECIMAL_P DECLARE DEFAULT DEFAULTS DETERMINISTIC
	DEFERRABLE DEFERRED DEFINER DELETE_P DELIMITER DELIMITERS DEPENDS DESC
	DETACH DICTIONARY DIRECT DIRECTORY DISABLE_P DISCARD DISTINCT DISTKEY DISTRIBUTE DISTRIBUTED DISTSTYLE DO DOCUMENT_P DOMAIN_P
	DOUBLE_P DROP

	EACH ELSE ENABLE_P ENCODING ENCRYPT ENCRYPTED END_P ENUM_P EOL ERROR_P ERRORS ESCAPE EVENT EXCEPT MINUS
	EXCHANGE
	EXCLUDE EXCLUDING EXCLUSIVE EXEC EXECUTE EXISTS EXPLAIN
	EXTENSION EXTENT EXTERNAL EXTRACT

	FALSE_P FAMILY FETCH FIELDS FILL FILTER FIRST_P FLASH_CACHE FLOAT_P FOLLOWING FOR
	FORCE FOREIGN FORMAT FORWARD FREELIST FREELISTS FREEZE FROM FULL FUNCTION FUNCTIONS FIXED_P FORMATTER

	PACKAGE EDITIONABLE NONEDITIONABLE SHARING BODY PACKAGES

	GENERATED GLOBAL GRANT GRANTED GREATEST GROUP_P GROUPING GROUPING_ID GROUPS GTM

	HANDLER HAVING HEADER_P HOLD HOST HOUR_P

	IDENTIFIED IDENTITY_P IF_P ILIKE IMMEDIATE IMMUTABLE IMPLICIT_P IMPORT_P IN_P INCLUDE
	INCLUDING INCREMENT INDEX INDEXES INHERIT INHERITS INITIAL INITIALLY INITRANS INLINE_P
	INNER_P INOUT INPUT_P INSENSITIVE INSERT INSTEAD INT_P INTEGER
	INTERSECT INTERVAL INTO INVALIDATE INVOKER IS ISNULL ISOLATION

	JOIN

	KEEP KEY USING_NLS_COMP ACCESSIBLE

	LABEL LANGUAGE LARGE_P LAST_P LATERAL_P
	LEADING LEAKPROOF LEAST LEFT LEFT_P LEVEL LIKE LIMIT LINK LINK_P LISTEN LOAD LOCAL
	LOCALTIME LOCALTIMESTAMP LOCATION LOCK_P LOCKED LOG_P LOGGED LONG

	MAPPING MASTER MATCH MATCHED MATERIALIZED MAXEXTENTS MAXVALUE MAXSIZE MAXTRANS MEMBER_P MEMORY_LIMIT MERGE METHOD MINEXTENTS MINUTE_P MINVALUE MISSING MOD MODE MONTH_P MOVE MULTISET

	NAME_P NAMES NATIONAL NATURAL NCHAR NCHAR_CS NEW NEWLINE NEXT NO NOAUDIT NOCACHE NOCYCLE NODE NOMAXVALUE NOMINVALUE NONE
	NOPARALLEL NOT NOTHING NOTIFY NOTNULL NOVALIDATE NOWAIT NULL_P NULLIF WAIT
	NULLS_P NUMBER NUMERIC

	OBJECT_P OF OFF OFFSET
	/* PGPAL added NODE token*/
	OID_P  
	OIDS OLD ON ONLY OPENTENBASE_P OPERATOR OPTIMAL OPTION OPTIONS OR
	ORDER ORDINALITY OUT_P OUTER_P OVER OVERFLOW OVERLAPS OVERLAY OVERRIDING OWNED OWNER

	PARALLEL PARALLEL_ENABLE PARSER PARTIAL PARTITION PARTITIONS PASSING PASSWORD PAUSE PCTFREE PCTINCREASE PCTUSED PERCENT PERSISTENTLY PG_SUBPARTITION PIPELINED PLACING PLANS POLICY
	POSITION PRECEDING PRECISION PREFERRED PRESERVE PREPARE PREPARED PRIMARY
	PRIOR PRIORITY PRIVILEGES PROCEDURAL PROCEDURE PROCEDURES PROFILE PROGRAM PROMPT PROTOCOL PUBLIC PUBLICATION PUSHDOWN

	QUEUE QUOTE

	RANDOMLY RANGE RAW READABLE READ REJECT_P REAL REASSIGN REBUILD RECHECK RECORD_P RECURSIVE RECYCLE REF REFERENCES REFERENCING
	REFRESH REINDEX RELATIVE_P RELEASE RENAME REPEATABLE REPLACE REPLICA
	RESET RESOURCE RESTART RESTRICT RESULT_CACHE RESULT_P RETURNING RETURNS REUSE REVOKE RIGHT ROLE ROLLBACK ROLLBACK_SUBTXN ROLLUP
	ROUTINE ROUTINES ROW ROWNUM ROWS ROWTYPE_P RULE RETURN

	SAMPLE SAVEPOINT SCHEMA SCHEMAS SCROLL SEARCH SECOND_P SECURITY SEGMENT SELECT SELF_P SEQUENCE SEQUENCES
	SERIALIZABLE SERVER SESSION SESSION_USER SESSIONTIMEZONE SET SETS SETOF SETTINGS SHARDCLUSTER SHARDING SHARE SHARED SHOW
	SIBLINGS SIMILAR SIMPLE SKIP SLICE SMALLINT SNAPSHOT SOME SPECIFICATION SQL_P STABLE STANDALONE_P
	START STATEMENT STATIC_P STATISTICS STDIN STDOUT STEP STMT_LEVEL_ROLLBACK STORAGE STRICT_P STRIP_P
	SUBSCRIPTION SUBPARTITION SUBPARTITIONS SPLIT SUBSTRING SUCCESSFUL SYMMETRIC SYNONYM SYSDATE SYSID SYSTEM_P SYSTIMESTAMP

	TABLE TABLES TABLESAMPLE TABLESPACE TEMP TEMPLATE TEMPORARY TEXT_P THAN THAN_P THEN
	TIES TIME TIMESTAMP TO TRAILING TRANSACTION TRANSFORM TREAT TRIGGER TRIM TRUE_P
	TRUNCATE TRUSTED TYPE_P TYPES_P

	UID_P UNBOUNDED UNCOMMITTED UNENCRYPTED UNION UNIQUE UNKNOWN UNLIMITED UNLISTEN UNLOCK_P UNLOGGED
	UNPAUSE UNTIL UPDATE USER USING

	VACUUM VALID VALIDATE VALIDATOR VALUE_P VALUES VARCHAR VARIADIC VARYING
	VERBOSE VERSION_P VIEW VIEWS VOLATILE

	WEB WELLFORMED WHEN WHENEVER WHERE WHITESPACE_P WINDOW WITH WITHIN WITHOUT WORK WRAPPER WRITABLE WRITE

	XML_P XMLATTRIBUTES XMLCONCAT XMLELEMENT XMLEXISTS XMLFOREST XMLNAMESPACES
	XMLPARSE XMLPI XMLROOT XMLSERIALIZE XMLTABLE

	YEAR_P YES_P

	ZONE

/*
 * The grammar thinks these are keywords, but they are not in the kwlist.h
 * list and so can never be entered directly.  The filter in parser.c
 * creates these tokens when required (based on looking one token ahead).
 *
 * NOT_LA exists so that productions such as NOT LIKE can be given the same
 * precedence as LIKE; otherwise they'd effectively have the same precedence
 * as NOT, at least with respect to their left-hand subexpression.
 * NULLS_LA and WITH_LA are needed to make the grammar LALR(1).
 *
 * ORDER_S is defined to give ORDER SIBLINGS BY the same precedence as ORDER BY.
 */
%token		NOT_LA NULLS_LA WITH_LA VALUES_LA ORDER_S LA_OVER DO_LA
%token		USING_LA CURRENT_TIME_LA LEVEL_LA

/* Precedence: for unreserved_keyword as alias without as */
%nonassoc   ABORT_P ABSOLUTE_P ACCESS ACCOUNT ACTION ADD_P ADMIN AFTER AGGREGATE ALSO ALTER ALWAYS ASSERTION ASSIGNMENT ATTACH ATTRIBUTE 
%nonassoc   BACKWARD BARRIER BEFORE BEGIN_P BEGIN_SUBTXN BIGINT BINARY BINARY_DOUBLE BINARY_FLOAT BIT BODY BOOLEAN_P BUFFER_POOL /*BOTH*/ /*BY*/
%nonassoc   CACHE CALL CALLED CASCADE CASCADED CATALOG_P CELL_FLASH_CACHE CHAIN CHARACTERISTICS CHECKPOINT CLASS CLEAN CLOSE CLUSTER /*COALESCE*/
%nonassoc   COLLATION COLUMNS COMMENT COMMENTS COMMIT COMMIT_SUBTXN COMMITTED  CONCURRENCY CONFIGURATION CONFLICT CONNECTION CONSTRAINTS CONSTRUCTOR_P CONTENT_P CONTINUE_P
%nonassoc   CONVERSION_P COORDINATOR COPY COST CPUSET CPU_RATE_LIMIT CSV CURRENT_P CURSOR CYCLE 
%nonassoc   DATA_P DATABASE DEALLOCATE DEC DECIMAL_P DECLARE DEFAULT DEFAULTS DEFERRED DEFINER DELIMITER DELIMITERS DEPENDS DETACH DICTIONARY DEBUG SPECIFICATION SETTINGS COMPILE DETERMINISTIC
%nonassoc   DIRECT DISABLE_P DISCARD DISTKEY DISTRIBUTE DISTRIBUTED DISTSTYLE DOCUMENT_P DOMAIN_P DOUBLE_P DROP 
%nonassoc   EACH EDITIONABLE ENABLE_P ENCODING ENCRYPT ENCRYPTED ENUM_P ERROR_P 
%nonassoc   EVENT EXCHANGE EXCLUDE EXCLUDING EXCLUSIVE EXEC EXECUTE EXISTS EXPLAIN EXTENSION EXTENT EXTERNAL /*EXTRACT*/ 
%nonassoc   FAMILY FILTER FIRST_P FLOAT_P FLASH_CACHE FOR FORCE FORWARD FREELIST FREELISTS FUNCTION FUNCTIONS
%nonassoc   GLOBAL GRANTED GROUPS GROUPING GROUPING_ID GTM HANDLER HEADER_P HOLD
%nonassoc	PACKAGES
%nonassoc   IDENTITY_P IF_P IMMEDIATE IMMUTABLE IMPLICIT_P IMPORT_P INCLUDING INCREMENT INDEX INDEXES INHERIT INHERITS INITIAL INITRANS INLINE_P
%nonassoc   INOUT INPUT_P INSENSITIVE INSERT INSTEAD INT_P INTEGER INTERVAL INVALIDATE INVOKER ISOLATION KEEP KEY LONG
%nonassoc   LANGUAGE LARGE_P LAST_P LEAKPROOF /*LEADING*/ /*LEVEL*/ LISTEN LOAD LOCAL LOCATION LOCK_P LOCKED LOGGED
%nonassoc   MAPPING MATCH MATCHED MATERIALIZED MAXEXTENTS MAXSIZE MAXTRANS MAXVALUE MEMBER_P MEMORY_LIMIT MERGE METHOD MINEXTENTS MINVALUE MODE MOVE
%nonassoc   NAME_P NATIONAL NCHAR NCHAR_CS /*NEW*/ NEXT NO /*NODE*/ NONE NONEDITIONABLE NOPARALLEL NOTHING NOTIFY NOVALIDATE NOWAIT NULLS_P NUMBER NUMERIC WAIT
%nonassoc   OBJECT_P OF OFF OID_P OIDS ONLY OPTION OPTIONS OPTIMAL ORDINALITY OUT_P OVERFLOW /*OVERRIDING*/ OWNED OWNER
%nonassoc   PACKAGE PARALLEL PARALLEL_ENABLE PARSER PARTIAL PASSING /*PASSWORD*/ PAUSE PERCENT PLANS PCTFREE PCTINCREASE PCTUSED PROMPT POLICY POSITION USING_NLS_COMP ACCESSIBLE
%nonassoc   PRECISION PREFERRED PREPARE PREPARED /*PRIOR*/ PRIORITY PRIVILEGES PROCEDURAL PROCEDURE PROCEDURES PROFILE PROGRAM PUBLICATION PUSHDOWN QUOTE
%nonassoc   RANDOMLY READ REAL REASSIGN REBUILD RECHECK RECORD_P RECURSIVE RECYCLE REF REFERENCING REFRESH REINDEX RELATIVE_P
%nonassoc   RELEASE RENAME REPEATABLE REPLICA RESET RESOURCE RESTART RESTRICT RESULT_CACHE RESULT_P RETURNS RETURN REUSE REVOKE ROLE ROLLBACK
%nonassoc   ROLLBACK_SUBTXN ROUTINE ROUTINES /*ROW*/ ROWTYPE_P RULE
%nonassoc   SAMPLE SAVEPOINT SCHEMA SCHEMAS SCROLL SEARCH SECURITY SEQUENCE SEQUENCES SERIALIZABLE SERVER SESSION
%nonassoc   SETOF SETS SHARDING SHARE SHARING SHOW SIBLINGS SIMPLE SKIP SLOT SMALLINT SNAPSHOT SQL_P STABLE STANDALONE_P /*START*/ STATEMENT
%nonassoc   STATIC_P STATISTICS STDIN STDOUT STEP STORAGE STRICT_P SUBSCRIPTION SUBPARTITION SUBPARTITIONS SPLIT SYSID SYSTEM_P SYS_CONNECT_BY_PATH SYSTIMESTAMP TABLES TABLESPACE
%nonassoc   OPENTENBASE_P TEMP TEMPLATE TEMPORARY /*TEXT_P*/ TIME TIMESTAMP TRANSACTION TRANSFORM TREAT TRIGGER /*TRAILING*/ /*TRIM*/ TRUNCATE TRUSTED TYPE_P TYPES_P
%nonassoc   UNCOMMITTED UNENCRYPTED UNKNOWN UNLIMITED UNLISTEN UNLOCK_P UNLOGGED UNPAUSE UNTIL UPDATE 
%nonassoc   VACUUM VALID VALIDATE VALIDATOR VALUE_P /*VALUES VARCHAR*/ VERSION_P VIEW VIEWS VOLATILE WHITESPACE_P WORK WRAPPER WRITE
%nonassoc   XML_P XMLATTRIBUTES XMLCONCAT XMLELEMENT XMLEXISTS XMLFOREST XMLNAMESPACES XMLPARSE XMLPI XMLROOT XMLSERIALIZE XMLTABLE YES_P ZONE LA_OVER DO_LA
%nonassoc   USING_LA CURRENT_TIME_LA LEVEL_LA

/* Precedence: lowest to highest */
%nonassoc	CONCURRENTLY SET SCONST				/* see relation_expr_opt_alias */
%left		UNION INTERSECT EXCEPT MINUS FROM MULTISET
/* OPENTENBASE_ORA_BEGIN */
%left		OUTER_CONN_TBL
/* OPENTENBASE_ORA_END */
%left		OR
%left		AND
%right		NOT VALUES OVERRIDING
%nonassoc	IS ISNULL NOTNULL	/* IS sets precedence for IS NULL, etc */
%nonassoc	'<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc	BETWEEN IN_P LIKE ILIKE SIMILAR NOT_LA
%nonassoc	ORDER ORDER_S
%nonassoc	ESCAPE			/* ESCAPE must be just above LIKE/ILIKE/SIMILAR */
%left		POSTFIXOP		/* dummy for postfix Op rules */
/*
 * To support target_el without AS, we must give IDENT an explicit priority
 * between POSTFIXOP and Op.  We can safely assign the same priority to
 * various unreserved keywords as needed to resolve ambiguities (this can't
 * have any bad effects since obviously the keywords will still behave the
 * same as if they weren't keywords).  We need to do this for PARTITION,
 * RANGE, ROWS to support opt_existing_window_name; and for RANGE, ROWS
 * so that they can follow a_expr without creating postfix-operator problems;
 * for GENERATED so that it can follow b_expr;
 * and for NULL so that it can follow b_expr in ColQualList without creating
 * postfix-operator problems.
 *
 * To support CUBE and ROLLUP in GROUP BY without reserving them, we give them
 * an explicit priority lower than '(', so that a rule with CUBE '(' will shift
 * rather than reducing a conflicting rule that takes CUBE as a function name.
 * Using the same precedence as IDENT seems right for the reasons given above.
 *
 * The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
 * are even messier: since UNBOUNDED is an unreserved keyword (per spec!),
 * there is no principled way to distinguish these from the productions
 * a_expr PRECEDING/FOLLOWING.  We hack this up by giving UNBOUNDED slightly
 * lower precedence than PRECEDING and FOLLOWING.  At present this doesn't
 * appear to cause UNBOUNDED to be treated differently from other unreserved
 * keywords anywhere else in the grammar, but it's definitely risky.  We can
 * blame any funny behavior of UNBOUNDED on the SQL standard, though.
 */
%nonassoc	UNBOUNDED		/* ideally should have same precedence as IDENT */
%nonassoc	IDENT IDENT_IN_D_QUOTES GENERATED NULL_P PARTITION RANGE ROWS PRECEDING FOLLOWING CUBE ROLLUP
%left		Op OPERATOR		/* multi-character ops and user-defined operators */
%left		'+' '-' CONCATENATION
%left		'*' '/' '%' ':' MOD
%left		'^'
/* Unary Operators */
%left		AT				/* sets precedence for AT TIME ZONE */
%left		COLLATE
%right		UMINUS
%left		'[' ']'
%left		'(' ')'
%left		TYPECAST
%left		'.'
%left		'@'
%left		NOCYCLE
/*
 * These might seem to be low-precedence, but actually they are not part
 * of the arithmetic hierarchy at all in their use as JOIN operators.
 * We make them high-precedence to support their use as function names.
 * They wouldn't be given a precedence at all, were it not that we need
 * left-associativity among the JOIN rules themselves.
 */
%left		JOIN CROSS LEFT_P FULL RIGHT INNER_P NATURAL
/* kluge to keep xml_whitespace_or_wellformed_option from causing shift/reduce conflicts */
%right		PRESERVE STRIP_P WELLFORMED

%%

/*
 *	The target production for the whole parse.
 */
stmtblock:	stmtmulti
			{
				pg_yyget_extra(yyscanner)->parsetree = $1;
			}
		;

/*
 * At top level, we wrap each stmt with a RawStmt node carrying start location
 * and length of the stmt's text.  Notice that the start loc/len are driven
 * entirely from semicolon locations (@2).  It would seem natural to use
 * @1 or @3 to get the true start location of a stmt, but that doesn't work
 * for statements that can start with empty nonterminals (opt_with_clause is
 * the main offender here); as noted in the comments for YYLLOC_DEFAULT,
 * we'd get -1 for the location in such cases.
 * We also take care to discard empty statements entirely.
 */
stmtmulti:	stmtmulti ';' stmt
				{
					if ($1 != NIL)
					{
						/* update length of previous stmt */
						updateRawStmtEnd(llast_node(RawStmt, $1), @2);
					}
					if ($3 != NULL)
						$$ = lappend($1, makeRawStmt($3, @2 + 1));
					else
						$$ = $1;

				}
			| stmt
				{
					if ($1 != NULL)
						$$ = list_make1(makeRawStmt($1, 0));
					else
						$$ = NIL;

				}
		;

stmt :
			AlterEventTrigStmt
			| AlterCollationStmt
			| AlterDatabaseStmt
			| AlterDatabaseSetStmt
			| AlterDefaultPrivilegesStmt
			| AlterDomainStmt
			| AlterEnumStmt
			| AlterExtensionStmt
			| AlterExtensionContentsStmt
			| AlterFdwStmt
			| AlterForeignServerStmt
			| AlterForeignTableStmt
			| AlterFunctionStmt
			| AlterGroupStmt
			| AlterNodeStmt
			| AlterNodeGroupStmt
			| AlterObjectDependsStmt
			| AlterObjectSchemaStmt
			| AlterOwnerStmt
			| AlterOperatorStmt
			| AlterPackageStmt
			| AlterPolicyStmt
			| AlterProfileStmt
            | AlterResourceCostStmt
/* __RESOURCE_QUEUE__ BEGIN */
			| AlterResourceQueueStmt
/* __RESOURCE_QUEUE__ END */
			| AlterResourceGroupStmt
			| AlterSeqStmt
			| AlterSystemStmt
			| AlterTableStmt
			| AlterTblSpcStmt
			| AlterCompositeTypeStmt
			| AlterPublicationStmt
			| AlterRoleSetStmt
			| AlterRoleStmt
			| AlterSubscriptionStmt
			| AlterTSConfigurationStmt
			| AlterTSDictionaryStmt
			| AlterUserMappingStmt
			| AnalyzeStmt
/* __AUDIT__ BEGIN */
			| AuditStmt
/* __AUDIT__ END */
			| BarrierStmt
			| CallStmt
			| CheckPointStmt
/* __AUDIT__ BEGIN */
			| CleanAuditStmt
/* __AUDIT__ END */
			| CleanConnStmt
			| CleanShardingStmt
			| ClosePortalStmt
			| ClusterStmt
			| CommentStmt
			| ConstraintsSetStmt
			| CopyStmt
			| CreateAmStmt
			| CreateAsStmt
			| CreateAssertStmt
			| CreateCastStmt
			| CreateConversionStmt
			| CreateDomainStmt
			| CreateExtensionStmt
			| CreateExternalStmt
			| CreateFdwStmt
			| CreateForeignServerStmt
			| CreateForeignTableStmt
			| CreateFunctionStmt
			| CreatePackageStmt
			| DropPackageStmt
			| DeclareFuncInPkgStmt
			| CreateFuncInPkgStmt
			| CreateGroupStmt
			| CreateMatViewStmt
			| CreateNodeGroupStmt
			| CreateNodeStmt
			| CreateOpClassStmt
			| CreateOpFamilyStmt
			| CreatePublicationStmt
			| AlterOpFamilyStmt
			| CreatePolicyStmt
			| CreateProfileStmt
			| CreatePLangStmt
/* __RESOURCE_QUEUE__ BEGIN */
			| CreateResourceQueueStmt
/* __RESOURCE_QUEUE__ END */
			| CreateResourceGroupStmt
			| CreateSchemaStmt
			| CreateSeqStmt
			| CreateShardStmt
			| CreateStmt
			| CreateSubscriptionStmt
			| CreateStatsStmt
			| CreateSynonymStmt
			| CreateTableSpaceStmt
			| CreateTransformStmt
			| CreateTrigStmt
			| CreateEventTrigStmt
			| CreateRoleStmt
			| CreateUserStmt
			| CreateUserMappingStmt
			| CreatedbStmt
			| CreateDirStmt
			| DeallocateStmt
			| DeclareCursorStmt
			| DefineStmt
			| DeleteStmt
			| DiscardStmt
			| DoStmt
			| DropAssertStmt
			| DropCastStmt
			| DropNodeGroupStmt
			| DropNodeStmt
			| DropOpClassStmt
			| DropOpFamilyStmt
			| DropOwnedStmt
			| DropPLangStmt
			| DropProfileStmt
/* __RESOURCE_QUEUE__ BEGIN */
			| DropResourceQueueStmt
/* __RESOURCE_QUEUE__ END */
			| DropResourceGroupStmt
			| DropShardStmt
			| DropStmt
			| DropSubscriptionStmt
			| DropTableSpaceStmt
			| DropTransformStmt
			| DropRoleStmt
			| DropSynonymStmt
			| DropUserMappingStmt
			| DropdbStmt
			| DropDirStmt
			| ExecuteStmt
			| ExecDirectStmt
			| ExplainStmt
			| FetchStmt
			| GrantStmt
			| GrantRoleStmt
			| ImportForeignSchemaStmt
			| IndexStmt
			| InsertStmt
			| ListenStmt
			| RefreshMatViewStmt
			| LoadStmt
			| LockStmt
			| LockNodeStmt
			| LoadFromStmt
			| MergeStmt
			| MoveDataStmt
			| NotifyStmt
			| PauseStmt
			| PrepareStmt
			| ReassignOwnedStmt
			| ReindexStmt
			| RemoveAggrStmt
			| RemoveFuncStmt
			| RemoveOperStmt
			| RenameStmt
			| RevokeStmt
			| RevokeRoleStmt
			| RuleStmt
			| SampleStmt
			| SecLabelStmt
			| SelectStmt
			| TransactionStmt
			| TruncateStmt
			| UnlistenStmt
			| UpdateStmt
			| VacuumStmt
			| VariableResetStmt
			| VariableSetStmt
			| VariableShowStmt
			| ViewStmt
			| CreateFuncInTypeObjStmt
			| DeclareFuncInTypeObjStmts
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * Create a new Resource Group
 *
 *****************************************************************************/

CreateResourceGroupStmt:
						CREATE RESOURCE GROUP_P lower_name WITH CreateOptResourceGroupList
                                {
                                        CreateResourceGroupStmt *n = makeNode(CreateResourceGroupStmt);
                                        n->name = $4;
                                        n->options = $6;
                                        $$ = (Node *)n;
                                }
                ;

/*****************************************************************************
 *
 * Drop a Resource Group
 *
 *****************************************************************************/

DropResourceGroupStmt:
                        DROP RESOURCE GROUP_P lower_name
                                 {
                                        DropResourceGroupStmt *n = makeNode(DropResourceGroupStmt);
                                        n->name = $4;
                                        $$ = (Node *)n;
                                 }
                ;

/*****************************************************************************
 *
 * Alter a Resource Group
 *
 *****************************************************************************/
AlterResourceGroupStmt:
                        ALTER RESOURCE GROUP_P lower_name SET OptResourceGroupList
                                 {
                                        AlterResourceGroupStmt *n = makeNode(AlterResourceGroupStmt);
                                        n->name = $4;
                                        n->options = $6;
                                        $$ = (Node *)n;
                                 }
                ;

/*
 * Option for ALTER RESOURCE GROUP
 */
OptResourceGroupList: OptResourceGroupElem                      { $$ = list_make1($1); }
                ;

OptResourceGroupElem:
                        CONCURRENCY SignedIconst
                                {
                                        /* was "concurrency" */
                                        $$ = makeDefElem("concurrency", (Node *) makeInteger($2), @1);
                                }
                        | CPU_RATE_LIMIT SignedIconst
                                {
                                        $$ = makeDefElem("cpu_rate_limit", (Node *) makeInteger($2), @1);
                                }
                        | CPUSET Sconst
                                {
                                        $$ = makeDefElem("cpuset", (Node *) makeString($2), @1);
                                }
                        | MEMORY_LIMIT SignedIconst
                                {
                                        $$ = makeDefElem("memory_limit", (Node *) makeInteger($2), @1);
                                }
                ;

CreateOptResourceGroupList:
			CreateOptResourceGroupList OptResourceGroupElem		{ $$ = lappend($1, $2); }
			| /* EMPTY */										{ $$ = NIL; }
			;

/*****************************************************************************
 *
 * CALL statement
 *
 *****************************************************************************/

CallStmt:	CALL func_application
				{
					CallStmt *n = makeNode(CallStmt);
					n->funccall = castNode(FuncCall, $2);
					$$ = (Node *)n;
				}
		    | EXEC func_application
				{
					CallStmt *n = makeNode(CallStmt);
					n->funccall = castNode(FuncCall, $2);
					$$ = (Node *)n;
				}
			| CALL func_name_no_parens
				{
					CallStmt *n = makeNode(CallStmt);

					if (!ORA_MODE)
						elog(ERROR,
							"set enable_opentenbase_ora_compatible to on to enable this grammer.");

					n->funccall = makeFuncCall($2, NIL, @2);
					$$ = (Node *)n;
				}
			| EXEC func_name_no_parens
				{
					CallStmt *n = makeNode(CallStmt);

					if (!ORA_MODE)
						elog(ERROR,
							"set enable_opentenbase_ora_compatible to on to enable this grammer.");

					n->funccall = makeFuncCall($2, NIL, @2);
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS role
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE ROLE RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_ROLE;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;


opt_with:	WITH									{}
			| WITH_LA								{}
			| /*EMPTY*/								{}
		;

/*
 * Options for CREATE ROLE and ALTER ROLE (also used by CREATE/ALTER USER
 * for backwards compatibility).  Note: the only option required by SQL99
 * is "WITH ADMIN name".
 */
OptRoleList:
			OptRoleList CreateOptRoleElem			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

AlterOptRoleList:
			AlterOptRoleList AlterOptRoleElem		{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

AlterOptRoleElem:
			PASSWORD Sconst
				{
					$$ = makeDefElem("password",
									 (Node *)makeString($2), @1);
				}
			| PASSWORD NULL_P
				{
					$$ = makeDefElem("password", NULL, @1);
				}
			| ENCRYPTED PASSWORD Sconst
				{
					/*
					 * These days, passwords are always stored in encrypted
					 * form, so there is no difference between PASSWORD and
					 * ENCRYPTED PASSWORD.
					 */
					$$ = makeDefElem("password",
									 (Node *)makeString($3), @1);
				}
			| UNENCRYPTED PASSWORD Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNENCRYPTED PASSWORD is no longer supported"),
							 errhint("Remove UNENCRYPTED to store the password in encrypted form instead."),
							 parser_errposition(@1)));
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(TRUE), @1);
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($3), @1);
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (Node *)makeString($3), @1);
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2, @1);
				}
		/* profile BEGIN*/
			| PROFILE ColId
				{
					$$ = makeDefElem("profile",
									 (Node *)makeString($2), @1);
				}
			| ACCOUNT UNLOCK_P
				{
					$$ = makeDefElem("unlock",
									 (Node *)makeInteger(TRUE), @1);
				}
		/* profile END */
			| PRIORITY Sconst
				{
					$$ = makeDefElem("priority", (Node *)makeString($2), @1);
				}
			| Common_IDENT
				{
					/*
					 * We handle identifiers that aren't parser keywords with
					 * the following special-case codes, to avoid bloating the
					 * size of the main parser.
					 */
					if (strcmp($1, "SUPERUSER") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(TRUE), @1);
					else if (strcmp($1, "NOSUPERUSER") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(FALSE), @1);
					else if (strcmp($1, "CREATEROLE") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(TRUE), @1);
					else if (strcmp($1, "NOCREATEROLE") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(FALSE), @1);
					else if (strcmp($1, "REPLICATION") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(TRUE), @1);
					else if (strcmp($1, "NOREPLICATION") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(FALSE), @1);
					else if (strcmp($1, "CREATEDB") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(TRUE), @1);
					else if (strcmp($1, "NOCREATEDB") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(FALSE), @1);
					else if (strcmp($1, "LOGIN") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(TRUE), @1);
					else if (strcmp($1, "NOLOGIN") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(FALSE), @1);
					else if (strcmp($1, "BYPASSRLS") == 0)
						$$ = makeDefElem("bypassrls", (Node *)makeInteger(TRUE), @1);
					else if (strcmp($1, "NOBYPASSRLS") == 0)
						$$ = makeDefElem("bypassrls", (Node *)makeInteger(FALSE), @1);
					else if (strcmp($1, "NOINHERIT") == 0)
					{
						/*
						 * Note that INHERIT is a keyword, so it's handled by main parser, but
						 * NOINHERIT is handled here.
						 */
						$$ = makeDefElem("inherit", (Node *)makeInteger(FALSE), @1);
					}
					else if (strcmp($1, "CREATEPROFILE") == 0)
					{
						$$ = makeDefElem("createprofile", (Node *)makeInteger(TRUE), @1);
					}
					else if (strcmp($1, "NOCREATEPROFILE") == 0)
					{
						$$ = makeDefElem("createprofile", (Node *)makeInteger(FALSE), @1);
					}
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized role option \"%s\"", $1),
									 parser_errposition(@1)));
				}
/* __RESOURCE_QUEUE__ BEGIN */
			| RESOURCE QUEUE QueueId
				{
					$$ = makeDefElem("resourceQueue",
									 (Node *)makeString($3), @1);
				}
/* __RESOURCE_QUEUE__ END */
			| RESOURCE GROUP_P lower_any_name
				{
					$$ = makeDefElem("resourceGroup", (Node *) $3, @1);
				}
		;

CreateOptRoleElem:
			AlterOptRoleElem			{ $$ = $1; }
			/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (Node *)makeInteger($2), @1);
				}
			| ADMIN role_list
				{
					$$ = makeDefElem("adminmembers", (Node *)$2, @1);
				}
			| ROLE role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2, @1);
				}
			| IN_P ROLE role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3, @1);
				}
			| IN_P GROUP_P role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3, @1);
				}
		;


/*****************************************************************************
 *
 * Create a new Postgres DBMS user (role with implied login ability)
 *
 *****************************************************************************/

CreateUserStmt:
			CREATE USER RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_USER;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql DBMS role
 *
 *****************************************************************************/

AlterRoleStmt:
			ALTER ROLE RoleSpec opt_with AlterOptRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_ROLE;
#endif
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					$$ = (Node *)n;
				 }
			| ALTER USER RoleSpec opt_with AlterOptRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_USER;
#endif
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					$$ = (Node *)n;
				 }
		;

opt_in_database:
			   /* EMPTY */					{ $$ = NULL; }
			| IN_P DATABASE database_name	{ $$ = $3; }
		;

AlterRoleSetStmt:
			ALTER ROLE RoleSpec opt_in_database SetResetClause
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_ROLE;
#endif
					n->role = $3;
					n->database = $4;
					n->setstmt = $5;
					$$ = (Node *)n;
				}
			| ALTER ROLE ALL opt_in_database SetResetClause
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_ROLE;
#endif
					n->role = NULL;
					n->database = $4;
					n->setstmt = $5;
					$$ = (Node *)n;
				}
			| ALTER USER RoleSpec opt_in_database SetResetClause
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_USER;
#endif
					n->role = $3;
					n->database = $4;
					n->setstmt = $5;
					$$ = (Node *)n;
				}
			| ALTER USER ALL opt_in_database SetResetClause
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_USER;
#endif
					n->role = NULL;
					n->database = $4;
					n->setstmt = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Drop a postgresql DBMS role
 *
 * XXX Ideally this would have CASCADE/RESTRICT options, but a role
 * might own objects in multiple databases, and there is presently no way to
 * implement cascading to other databases.  So we always behave as RESTRICT.
 *****************************************************************************/

DropRoleStmt:
			DROP ROLE role_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_ROLE;
#endif
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP ROLE IF_P EXISTS role_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_ROLE;
#endif
					n->missing_ok = TRUE;
					n->roles = $5;
					$$ = (Node *)n;
				}
			| DROP USER role_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_USER;
#endif
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP USER IF_P EXISTS role_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_USER;
#endif
					n->roles = $5;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			| DROP GROUP_P role_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_GROUP;
#endif
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP GROUP_P IF_P EXISTS role_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_GROUP;
#endif
					n->missing_ok = TRUE;
					n->roles = $5;
					$$ = (Node *)n;
				}
			;

/*****************************************************************************
 *
 * Create a new Postgres Resource Queue
 *
 *****************************************************************************/

CreateResourceQueueStmt:
			CREATE RESOURCE QUEUE QueueId WITH definition
				{
					CreateResourceQueueStmt *n = makeNode(CreateResourceQueueStmt);
					n->queue = $4;
					n->options = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * Alter a postgres Resource Queue
 *
 *****************************************************************************/

AlterResourceQueueStmt:
			ALTER RESOURCE QUEUE QueueId WITH definition
				 {
					AlterResourceQueueStmt *n = makeNode(AlterResourceQueueStmt);
					n->queue = $4;
					n->options = $6;
					$$ = (Node *)n;
				 }
		;

/*****************************************************************************
 *
 * Drop a postgres Resource Queue
 *
 *****************************************************************************/

DropResourceQueueStmt:
			DROP RESOURCE QUEUE QueueId
				 {
					DropResourceQueueStmt *n = makeNode(DropResourceQueueStmt);
					n->queue = $4;
					$$ = (Node *)n;
				 }
		;

/*****************************************************************************
 *
 * Create a postgresql group (role without login ability)
 *
 *****************************************************************************/

CreateGroupStmt:
			CREATE GROUP_P RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_GROUP;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql group
 *
 *****************************************************************************/

AlterGroupStmt:
			ALTER GROUP_P RoleSpec add_drop USER role_list
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_GROUP;
#endif
					n->role = $3;
					n->action = $4;
					n->options = list_make1(makeDefElem("rolemembers",
														(Node *)$6, @6));
					$$ = (Node *)n;
				}
		;

add_drop:	ADD_P									{ $$ = +1; }
			| DROP									{ $$ = -1; }
		;


/*****************************************************************************
 *
 * Manipulate a schema
 *
 *****************************************************************************/

CreateSchemaStmt:
			CREATE SCHEMA OptSchemaName AUTHORIZATION RoleSpec OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* One can omit the schema name or the authorization id. */
					n->schemaname = $3;
					n->authrole = $5;
					n->schemaElts = $6;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA ColId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* ...but not both */
					n->schemaname = $3;
					n->authrole = NULL;
					n->schemaElts = $4;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA IF_P NOT EXISTS OptSchemaName AUTHORIZATION RoleSpec OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* schema name can be omitted here, too */
					n->schemaname = $6;
					n->authrole = $8;
					if ($9 != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA IF NOT EXISTS cannot include schema elements"),
								 parser_errposition(@9)));
					n->schemaElts = $9;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA IF_P NOT EXISTS ColId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* ...but not here */
					n->schemaname = $6;
					n->authrole = NULL;
					if ($7 != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA IF NOT EXISTS cannot include schema elements"),
								 parser_errposition(@7)));
					n->schemaElts = $7;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
		;

OptSchemaName:
			ColId									{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSchemaEltList:
			OptSchemaEltList schema_stmt
				{
					if (@$ < 0)			/* see comments for YYLLOC_DEFAULT */
						@$ = @2;
					$$ = lappend($1, $2);
				}
			| /* EMPTY */
				{ $$ = NIL; }
		;

/*
 *	schema_stmt are the ones that can show up inside a CREATE SCHEMA
 *	statement (in addition to by themselves).
 */
schema_stmt:
			CreateStmt
			| IndexStmt
			| CreateSeqStmt
			| CreateTrigStmt
			| GrantStmt
			| ViewStmt
		;


/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			| SET LOCAL set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (Node *) n;
				}
			| SET SESSION set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

set_rest:
			TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION";
					n->args = $2;
					$$ = n;
				}
			| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "SESSION CHARACTERISTICS";
					n->args = $5;
					$$ = n;
				}
			| set_rest_more
			;

generic_set:
			var_name TO var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name '=' var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name TO DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name '=' DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}

set_rest_more:	/* Generic SET syntaxes: */
			generic_set 						{$$ = $1;}
			| var_name FROM CURRENT_P
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_CURRENT;
					n->name = $1;
					$$ = n;
				}
			/* Special syntaxes mandated by SQL standard: */
			| TIME ZONE zone_value
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| CATALOG_P Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("current database cannot be changed"),
							 parser_errposition(@2)));
					$$ = NULL; /*not reached*/
				}
			| SCHEMA Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "search_path";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| NAMES opt_encoding
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "client_encoding";
					if ($2 != NULL)
						n->args = list_make1(makeStringConst($2, @2));
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| ROLE Lower_NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "role";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| SESSION AUTHORIZATION Lower_NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "session_authorization";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
			| SESSION AUTHORIZATION DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "session_authorization";
					$$ = n;
				}
			| XML_P OPTION document_or_content
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "xmloption";
					n->args = list_make1(makeStringConst($3 == XMLOPTION_DOCUMENT ? "DOCUMENT" : "CONTENT", @3));
					$$ = n;
				}
			/* Special syntaxes invented by PostgreSQL: */
			| TRANSACTION SNAPSHOT Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION SNAPSHOT";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
		;

var_name:	Lower_ColId							{ $$ = $1; }
			| var_name '.' Lower_ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;

iso_level:	READ UNCOMMITTED						{ $$ = "read uncommitted"; }
			| READ COMMITTED						{ $$ = "read committed"; }
			| REPEATABLE READ						{ $$ = "repeatable read"; }
			| SERIALIZABLE							{ $$ = "serializable"; }
		;

opt_boolean_or_string:
			TRUE_P									{ $$ = "true"; lower_ident_as_const_str = false; }
			| FALSE_P								{ $$ = "false"; lower_ident_as_const_str = false; }
			| ON									{ $$ = "on"; lower_ident_as_const_str = false; }
			/*
			 * OFF is also accepted as a boolean value, but is handled by
			 * the NonReservedWord rule.  The action for booleans and strings
			 * is the same, so we don't need to distinguish them here.
			 */
			| Lower_NonReservedWord_or_Sconst			{ $$ = $1; }
			| COLUMN								{ $$ = "column"; lower_ident_as_const_str = false; }
		;

/* Timezone values can be:
 * - a string such as 'pst8pdt'
 * - an identifier such as "pst8pdt"
 * - an integer or floating point number
 * - a time interval per SQL99
 * ColId gives reduce/reduce errors against ConstInterval and LOCAL,
 * so use IDENT (meaning we reject anything that is a key word).
 */
zone_value:
			Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| Lower_IDENT
				{
					$$ = makeStringConst($1, @1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					if ($3 != NIL)
					{
						A_Const *n = (A_Const *) linitial($3);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@3)));
					}
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					TypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;

opt_encoding:
			Sconst									{ $$ = $1; }
			| DEFAULT								{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

VariableResetStmt:
			RESET reset_rest                                                { $$ = (Node *) $2; }
		;

reset_rest:
			generic_reset							{ $$ = $1; }
			| TIME ZONE
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "timezone";
					$$ = n;
				}
			| TRANSACTION ISOLATION LEVEL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "transaction_isolation";
					$$ = n;
				}
			| SESSION AUTHORIZATION
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "session_authorization";
					$$ = n;
				}
		;

generic_reset:
			var_name
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $1;
					$$ = n;
				}
			| ALL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = n;
				}
		;

/* SetResetClause allows SET or RESET without LOCAL */
SetResetClause:
			SET set_rest					{ $$ = $2; }
			| VariableResetStmt				{ $$ = (VariableSetStmt *) $1; }
		;

/* SetResetClause allows SET or RESET without LOCAL */
FunctionSetResetClause:
			SET set_rest_more				{ $$ = $2; }
			| VariableResetStmt				{ $$ = (VariableSetStmt *) $1; }
		;


VariableShowStmt:
			SHOW var_name
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = $2;
					$$ = (Node *) n;
				}
			| SHOW TIME ZONE
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| SHOW TRANSACTION ISOLATION LEVEL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| SHOW SESSION AUTHORIZATION
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| SHOW ALL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "all";
					$$ = (Node *) n;
				}
		;


ConstraintsSetStmt:
			SET CONSTRAINTS constraints_set_list constraints_set_mode
				{
					ConstraintsSetStmt *n = makeNode(ConstraintsSetStmt);
					n->constraints = $3;
					n->deferred = $4;
					$$ = (Node *) n;
				}
		;

constraints_set_list:
			ALL										{ $$ = NIL; }
			| qualified_name_list					{ $$ = $1; }
		;

constraints_set_mode:
			DEFERRED								{ $$ = TRUE; }
			| IMMEDIATE								{ $$ = FALSE; }
		;


/*
 * Checkpoint statement
 */
CheckPointStmt:
			CHECKPOINT
				{
					CheckPointStmt *n = makeNode(CheckPointStmt);
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * DISCARD { ALL | TEMP | PLANS | SEQUENCES }
 *
 *****************************************************************************/

DiscardStmt:
			DISCARD ALL
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_ALL;
					$$ = (Node *) n;
				}
			| DISCARD TEMP
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_TEMP;
					$$ = (Node *) n;
				}
			| DISCARD TEMPORARY
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_TEMP;
					$$ = (Node *) n;
				}
			| DISCARD PLANS
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_PLANS;
					$$ = (Node *) n;
				}
			| DISCARD SEQUENCES
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_SEQUENCES;
					$$ = (Node *) n;
				}

		;


/*****************************************************************************
 *
 *	ALTER [ TABLE | INDEX | SEQUENCE | VIEW | MATERIALIZED VIEW ] variations
 *
 * Note: we accept all subcommands for each of the five variants, and sort
 * out what's really legal at execution time.
 *****************************************************************************/
AlterTableStmt:
/*
 * To adapt for opentenbase_ora, this key word 'partition' is allowed to use as table column name.
 * Changing 'partition' as unreserved key words
 * In this gram sentence, it replaces 'PATITION' by 'PG_PARTITION' to avoid gram error.
 */
			ALTER TABLE relation_expr TRUNCATE PG_PARTITION '(' relation_expr_list ')' opt_truncate_opentenbase_ora_comp
				{
					TruncateStmt *n = makeNode(TruncateStmt);
					n->paretablename = $3;
					n->relations = $7;
					n->restart_seqs = false;
					n->behavior = DROP_RESTRICT;
					$$ = (Node *)n;
				}
		|	ALTER TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER TABLE relation_expr partition_cmd
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = list_make1($4);
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER TABLE IF_P EXISTS relation_expr partition_cmd
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = list_make1($6);
					n->relkind = OBJECT_TABLE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER TABLE ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $6;
					n->objtype = OBJECT_TABLE;
					n->roles = NIL;
					n->new_tablespacename = $9;
					n->nowait = $10;
					$$ = (Node *)n;
				}
		|	ALTER TABLE ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $6;
					n->objtype = OBJECT_TABLE;
					n->roles = $9;
					n->new_tablespacename = $12;
					n->nowait = $13;
					$$ = (Node *)n;
				}
		|	ALTER INDEX qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER INDEX IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER INDEX qualified_name index_partition_cmd
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = list_make1($4);
					n->relkind = OBJECT_INDEX;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER INDEX ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $6;
					n->objtype = OBJECT_INDEX;
					n->roles = NIL;
					n->new_tablespacename = $9;
					n->nowait = $10;
					$$ = (Node *)n;
				}
		|	ALTER INDEX ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $6;
					n->objtype = OBJECT_INDEX;
					n->roles = $9;
					n->new_tablespacename = $12;
					n->nowait = $13;
					$$ = (Node *)n;
				}
		|	ALTER SEQUENCE qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_SEQUENCE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_SEQUENCE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER MATERIALIZED VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_MATVIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $6;
					n->cmds = $7;
					n->relkind = OBJECT_MATVIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $7;
					n->objtype = OBJECT_MATVIEW;
					n->roles = NIL;
					n->new_tablespacename = $10;
					n->nowait = $11;
					$$ = (Node *)n;
				}
		|	ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $7;
					n->objtype = OBJECT_MATVIEW;
					n->roles = $10;
					n->new_tablespacename = $13;
					n->nowait = $14;
					$$ = (Node *)n;
				}
		;

update_index_clauses:
				UPDATE opt_global_clause INDEXES opt_parallel_clause            {}
				| INVALIDATE GLOBAL INDEXES opt_parallel_clause                         {}
				| /*EMPTY*/                                                                                                     {}
				;

opt_truncate_opentenbase_ora_comp:
		update_index_clauses					{}
        | DROP opt_all_clause STORAGE             {}
        | REUSE STORAGE                           {}
        ;

opt_parallel_clause:
          PARALLEL opt_int_clause               {}
        | NOPARALLEL                            {}
        | /*EMPTY*/								{}
        ;

opt_int_clause:
		  ICONST                                {}
		| /*EMPTY*/								{}

opt_global_clause:
		  GLOBAL								{ $$ = TRUE; }
		| /*EMPTY*/								{ $$ = FALSE;}
		;
opt_validate:
			VALIDATE                                { $$ = ALTER_VALIDATE; }
			| NOVALIDATE                            { $$ = ALTER_NOVALIDATE; }
			| /* EMPTY */                           { $$ = ALTER_VALIDATE_DEFAULT; }
		;

alter_table_cmds:
			alter_table_cmd
				{
					if ($1)
						$$ = list_make1($1);
					else
						$$ = NIL;
				}
			| alter_table_cmds ',' alter_table_cmd
				{
					if ($3)
						$$ = lappend($1, $3);
					else
						$$ = $1;
				}
		;

alter_group_cmds:
			alter_group_cmd							{ $$ = list_make1($1); }
			| alter_group_cmds ',' alter_group_cmd	{ $$ = lappend($1, $3); }
		;


partition_cmd:
			/* ALTER TABLE <name> ATTACH PARTITION <table_name> FOR VALUES */
			ATTACH PARTITION qualified_name PartitionBoundSpec
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					PartitionCmd *cmd = makeNode(PartitionCmd);

					n->subtype = AT_AttachPartition;
					cmd->name = $3;
					cmd->bound = $4;
					n->def = (Node *) cmd;

					$$ = (Node *) n;
				}
			/* ALTER TABLE <name> DETACH PARTITION <partition_name> */
			| DETACH PARTITION qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					PartitionCmd *cmd = makeNode(PartitionCmd);

					n->subtype = AT_DetachPartition;
					cmd->name = $3;
					cmd->bound = NULL;
					n->def = (Node *) cmd;

					$$ = (Node *) n;
				}
		;

alter_group_cmd:
			/* ALTER NODE GROUP <name> SET TO DEFAULT */
			SET TO DEFAULT
				{
					AlterGroupCmd *n = makeNode(AlterGroupCmd);
					n->subtype = AG_SetDefault;
					$$ = (Node *)n;
				}
		;

index_partition_cmd:
           /* ALTER INDEX <name> ATTACH PARTITION <index_name> */
           ATTACH PARTITION qualified_name
               {
                   AlterTableCmd *n = makeNode(AlterTableCmd);
                   PartitionCmd *cmd = makeNode(PartitionCmd);

                   n->subtype = AT_AttachPartition;
                   cmd->name = $3;
                   cmd->bound = NULL;
                   n->def = (Node *) cmd;

                   $$ = (Node *) n;
               }
       ;

alter_table_cmd:
			PhyAttrClauseList 		{ $$ = NULL; }
			/* ALTER TABLE <name> ADD <coldef> */
			| ADD_P columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $2;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD IF NOT EXISTS <coldef> */
			| ADD_P IF_P NOT EXISTS columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $5;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN <coldef> */
			| ADD_P COLUMN columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef> */
			| ADD_P COLUMN IF_P NOT EXISTS columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $6;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
			| ALTER opt_column ColId alter_column_default
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ColumnDefault;
					n->name = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL */
			| ALTER opt_column ColId DROP NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL */
			| ALTER opt_column ColId SET NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS <SignedIconst> */
			| ALTER opt_column ColId SET STATISTICS SignedIconst
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStatistics;
					n->name = $3;
					n->def = (Node *) makeInteger($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> RESET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
			| ALTER opt_column ColId SET STORAGE ColId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStorage;
					n->name = $3;
					n->def = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED ... AS IDENTITY ... */
			| ALTER opt_column ColId ADD_P GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					Constraint *c = makeNode(Constraint);

					c->contype = CONSTR_IDENTITY;
					c->generated_when = $6;
					c->options = $9;
					c->location = @5;

					n->subtype = AT_AddIdentity;
					n->name = $3;
					n->def = (Node *) c;

					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET <sequence options>/RESET */
			| ALTER opt_column ColId alter_identity_column_option_list
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetIdentity;
					n->name = $3;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY */
			| ALTER opt_column ColId DROP IDENTITY_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropIdentity;
					n->name = $3;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY IF EXISTS */
			| ALTER opt_column ColId DROP IDENTITY_P IF_P EXISTS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropIdentity;
					n->name = $3;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE] */
			| DROP opt_column IF_P EXISTS ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
			| DROP opt_column ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/*
			 * ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
			 *		[ USING <expression> ]
			 */
			| ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					/* We only use these fields of the ColumnDef node */
					def->typeName = $6;
					def->collClause = (CollateClause *) $7;
					def->raw_default = $8;
					def->location = @3;
					$$ = (Node *)n;
				}
			/* ALTER FOREIGN TABLE <name> ALTER [COLUMN] <colname> OPTIONS */
			| ALTER opt_column ColId alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AlterColumnGenericOptions;
					n->name = $3;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD CONSTRAINT ... */
			| ADD_P TableConstraint
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddConstraint;
					n->def = $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER CONSTRAINT ... */
			| ALTER CONSTRAINT name ConstraintAttributeSpec
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					Constraint *c = makeNode(Constraint);
					n->subtype = AT_AlterConstraint;
					n->def = (Node *) c;
					c->contype = CONSTR_FOREIGN; /* others not supported, yet */
					c->conname = $3;
					processCASbits($4, @4, "ALTER CONSTRAINT statement",
									&c->deferrable,
									&c->initdeferred,
									NULL, NULL, yyscanner);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> VALIDATE CONSTRAINT ... */
			| VALIDATE CONSTRAINT name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ValidateConstraint;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITH OIDS  */
			| SET WITH OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT OIDS  */
			| SET WITHOUT OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> CLUSTER ON <indexname> */
			| CLUSTER ON name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ClusterOn;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT CLUSTER */
			| SET WITHOUT CLUSTER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropCluster;
					n->name = NULL;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET LOGGED  */
			| SET LOGGED
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetLogged;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET UNLOGGED  */
			| SET UNLOGGED
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetUnLogged;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET (...) */
			| SET AS ColId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetAlias;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER <trig> */
			| ENABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS TRIGGER <trig> */
			| ENABLE_P ALWAYS TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA TRIGGER <trig> */
			| ENABLE_P REPLICA TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER ALL */
			| ENABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER USER */
			| ENABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE CONSTRAINT <name> */
			| ENABLE_P opt_validate CONSTRAINT name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableConstraint;
					n->validate = $2;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER <trig> */
			| DISABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER ALL */
			| DISABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER USER */
			| DISABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE CONSTRAINT <name> */
			| DISABLE_P opt_validate CONSTRAINT name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableConstraint;
					n->validate = $2;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE RULE <rule> */
			| ENABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS RULE <rule> */
			| ENABLE_P ALWAYS RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA RULE <rule> */
			| ENABLE_P REPLICA RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE RULE <rule> */
			| DISABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> INHERIT <parent> */
			| INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddInherit;
					n->def = (Node *) $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO INHERIT <parent> */
			| NO INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropInherit;
					n->def = (Node *) $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OF <type_name> */
			| OF any_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					TypeName *def = makeTypeNameFromNameList($2);
					def->location = @2;
					n->subtype = AT_AddOf;
					n->def = (Node *) def;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NOT OF */
			| NOT OF
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOf;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OWNER TO RoleSpec */
			| OWNER TO RoleSpec
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ChangeOwner;
					n->newowner = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET TABLESPACE <tablespacename> */
			| SET TABLESPACE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetTableSpace;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET (...) */
			| SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> RESET (...) */
			| RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> REPLICA IDENTITY  */
			| REPLICA IDENTITY_P replica_identity
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ReplicaIdentity;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ROW LEVEL SECURITY */
			| ENABLE_P ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableRowSecurity;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE ROW LEVEL SECURITY */
			| DISABLE_P ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableRowSecurity;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> FORCE ROW LEVEL SECURITY */
			| FORCE ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ForceRowSecurity;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO FORCE ROW LEVEL SECURITY */
			| NO FORCE ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_NoForceRowSecurity;
					$$ = (Node *)n;
				}
			| alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_GenericOptions;
					n->def = (Node *)$1;
					$$ = (Node *) n;
				}
/* PGXC_BEGIN */
			/* ALTER TABLE <name> DISTRIBUTE BY ... */
			| OptDistributeByInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DistributeBy;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> TO [ NODE (nodelist) | GROUP groupname ] */
			| OptSubClusterInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SubCluster;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD NODE (nodelist) */
			| ADD_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DELETE NODE (nodelist) */
			| DELETE_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DeleteNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
/* PGXC_END */
/* _SHARDING_ */
			| REBUILD EXTENT
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_RebuildExtent;
					$$ = (Node *)n;
				}
/* _SHARDING_ END*/
		;

alter_column_default:
			SET DEFAULT a_expr			{ $$ = $3; }
			| DROP DEFAULT				{ $$ = NULL; }
		;

opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| /* EMPTY */				{ $$ = DROP_RESTRICT; /* default */ }
		;

opt_collate_clause:
			COLLATE lower_any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| /* EMPTY */				{ $$ = NULL; }
		;

alter_using:
			USING a_expr				{ $$ = $2; }
			| /* EMPTY */				{ $$ = NULL; }
		;

replica_identity:
			NOTHING
				{
					ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
					n->identity_type = REPLICA_IDENTITY_NOTHING;
					n->name = NULL;
					$$ = (Node *) n;
				}
			| FULL
				{
					ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
					n->identity_type = REPLICA_IDENTITY_FULL;
					n->name = NULL;
					$$ = (Node *) n;
				}
			| DEFAULT
				{
					ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
					n->identity_type = REPLICA_IDENTITY_DEFAULT;
					n->name = NULL;
					$$ = (Node *) n;
				}
			| USING INDEX name
				{
					ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
					n->identity_type = REPLICA_IDENTITY_INDEX;
					n->name = $3;
					$$ = (Node *) n;
				}
;

Identified_plane:
            IN_P       Lower_ColId                  { $$ = $2; }
            |                                       { $$ = NULL; }
            ;
reloptions:
			'(' reloption_list ')'                                  { $$ = $2; }
		;

opt_reloptions:		WITH reloptions					{ $$ = $2; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

reloption_list:
			reloption_elem							{ $$ = list_make1($1); }
			| reloption_list ',' reloption_elem		{ $$ = lappend($1, $3); }
		;

/* This should match def_elem and also allow qualified names */
reloption_elem:
			Lower_ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| Lower_ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
			| Lower_ColLabel '.' Lower_ColLabel '=' def_arg
				{
					$$ = makeDefElemExtended($1, $3, (Node *) $5,
											 DEFELEM_UNSPEC, @1);
				}
			| Lower_ColLabel '.' Lower_ColLabel
				{
					$$ = makeDefElemExtended($1, $3, NULL, DEFELEM_UNSPEC, @1);
				}
		;

alter_identity_column_option_list:
			alter_identity_column_option
				{ $$ = list_make1($1); }
			| alter_identity_column_option_list alter_identity_column_option
				{ $$ = lappend($1, $2); }
		;

alter_identity_column_option:
			RESTART
				{
					$$ = makeDefElem("restart", NULL, @1);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3, @1);
				}
			| SET SeqOptElem
				{
					if (strcmp($2->defname, "as") == 0 ||
						strcmp($2->defname, "restart") == 0 ||
						strcmp($2->defname, "owned_by") == 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("sequence option \"%s\" not supported here", $2->defname),
								 parser_errposition(@2)));
					$$ = $2;
				}
			| SET GENERATED generated_when
				{
					$$ = makeDefElem("generated", (Node *) makeInteger($3), @1);
				}
		;

PartitionBoundSpec:
			/* a HASH partition*/
			FOR VALUES WITH '(' hash_partbound ')'
				{
					ListCell   *lc;
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_HASH;
					n->modulus = n->remainder = -1;

					foreach (lc, $5)
					{
						DefElem    *opt = lfirst_node(DefElem, lc);

						if (strcmp(opt->defname, "MODULUS") == 0)
						{
							if (n->modulus != -1)
								ereport(ERROR,
										(errcode(ERRCODE_DUPLICATE_OBJECT),
										 errmsg("modulus for hash partition provided more than once"),
										 parser_errposition(opt->location)));
							n->modulus = defGetInt32(opt);
						}
						else if (strcmp(opt->defname, "REMAINDER") == 0)
						{
							if (n->remainder != -1)
								ereport(ERROR,
										(errcode(ERRCODE_DUPLICATE_OBJECT),
										 errmsg("remainder for hash partition provided more than once"),
										 parser_errposition(opt->location)));
							n->remainder = defGetInt32(opt);
						}
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("unrecognized hash partition bound specification \"%s\"",
											opt->defname),
									 parser_errposition(opt->location)));
					}

					if (n->modulus == -1)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("modulus for hash partition must be specified")));
					if (n->remainder == -1)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("remainder for hash partition must be specified")));

					n->location = @3;

					$$ = n;
				}

			/* a LIST partition */
			| FOR VALUES IN_P '(' partbound_datum_list ')'
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_LIST;
					n->is_default = false;
					n->listdatums = $5;
					n->location = @3;

					$$ = n;
				}

			/* a RANGE partition */
			| FOR VALUES FROM '(' range_datum_list ')' TO '(' range_datum_list ')'
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_RANGE;
					n->is_default = false;
					n->lowerdatums = $5;
					n->upperdatums = $9;
					n->location = @3;

					$$ = n;
				}

			/* a DEFAULT partition */
			| DEFAULT
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->is_default = true;
					n->location = @1;

					$$ = n;
				}
		;

hash_partbound_elem:
		NonReservedWord Iconst
			{
				$$ = makeDefElem($1, (Node *)makeInteger($2), @1);
			}
		;

hash_partbound:
		hash_partbound_elem
			{
				$$ = list_make1($1);
			}
		| hash_partbound ',' hash_partbound_elem
			{
				$$ = lappend($1, $3);
			}
		;

partbound_datum:
			Sconst
			{
				#ifdef _PG_ORCL_
					if (strlen($1) == 0 )
						$$ = makeNullAConst(@1);
					else
				#endif
						$$ = makeStringConst($1, @1);
			}
			| NumericOnly	{ $$ = makeAConst($1, @1); }
			| TRUE_P		{ $$ = makeStringConst(pstrdup("true"), @1); }
			| FALSE_P		{ $$ = makeStringConst(pstrdup("false"), @1); }
			| NULL_P		{ $$ = makeNullAConst(@1); }
		;

partbound_datum_list:
			partbound_datum						{ $$ = list_make1($1); }
			| partbound_datum_list ',' partbound_datum
												{ $$ = lappend($1, $3); }
		;

range_datum_list:
			PartitionRangeDatum					{ $$ = list_make1($1); }
			| range_datum_list ',' PartitionRangeDatum
												{ $$ = lappend($1, $3); }
		;

PartitionRangeDatum:
			MINVALUE
				{
					PartitionRangeDatum *n = makeNode(PartitionRangeDatum);

					n->kind = PARTITION_RANGE_DATUM_MINVALUE;
					n->value = NULL;
					n->location = @1;

					$$ = (Node *) n;
				}
			| MAXVALUE
				{
					PartitionRangeDatum *n = makeNode(PartitionRangeDatum);

					n->kind = PARTITION_RANGE_DATUM_MAXVALUE;
					n->value = NULL;
					n->location = @1;

					$$ = (Node *) n;
				}
			| partbound_datum
				{
					PartitionRangeDatum *n = makeNode(PartitionRangeDatum);

					n->kind = PARTITION_RANGE_DATUM_VALUE;
					n->value = $1;
					n->location = @1;

					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *	ALTER TYPE
 *
 * really variants of the ALTER TABLE subcommands with different spellings
 *****************************************************************************/

AlterCompositeTypeStmt:
			ALTER TYPE_P any_name alter_type_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);

					/* can't use qualified_name, sigh */
					n->relation = makeRangeVarFromAnyName($3, @3, yyscanner);
					n->cmds = $4;
					n->relkind = OBJECT_TYPE;
					$$ = (Node *)n;
				}
			;

alter_type_cmds:
			alter_type_cmd							{ $$ = list_make1($1); }
			| alter_type_cmds ',' alter_type_cmd	{ $$ = lappend($1, $3); }
		;

alter_type_cmd:
			/* ALTER TYPE <name> ADD ATTRIBUTE <coldef> [RESTRICT|CASCADE] */
			ADD_P ATTRIBUTE TableFuncElement opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
			/* ALTER TYPE <name> DROP ATTRIBUTE IF EXISTS <attname> [RESTRICT|CASCADE] */
			| DROP ATTRIBUTE IF_P EXISTS ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TYPE <name> DROP ATTRIBUTE <attname> [RESTRICT|CASCADE] */
			| DROP ATTRIBUTE ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/* ALTER TYPE <name> ALTER ATTRIBUTE <attname> [SET DATA] TYPE <typename> [RESTRICT|CASCADE] */
			| ALTER ATTRIBUTE ColId opt_set_data TYPE_P Typename opt_collate_clause opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					n->behavior = $8;
					/* We only use these fields of the ColumnDef node */
					def->typeName = $6;
					def->collClause = (CollateClause *) $7;
					def->raw_default = NULL;
					def->location = @3;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				close <portalname>
 *
 *****************************************************************************/

ClosePortalStmt:
			CLOSE cursor_name
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = $2;
					$$ = (Node *)n;
				}
			| CLOSE ALL
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = NULL;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				COPY relname [(columnList)] FROM/TO file [WITH] [(options)]
 *				COPY ( query ) TO file	[WITH] [(options)]
 *
 *				where 'query' can be one of:
 *				{ SELECT | UPDATE | INSERT | DELETE }
 *
 *				and 'file' can be one of:
 *				{ PROGRAM 'command' | STDIN | STDOUT | 'filename' }
 *
 *				In the preferred syntax the options are comma-separated
 *				and use generic identifiers instead of keywords.  The pre-9.0
 *				syntax had a hard-wired, space-separated set of options.
 *
 *				Really old syntax, from versions 7.2 and prior:
 *				COPY [ BINARY ] table [ WITH OIDS ] FROM/TO file
 *					[ [ USING ] DELIMITERS 'delimiter' ] ]
 *					[ WITH NULL AS 'null string' ]
 *				This option placement is not supported with COPY (query...).
 *
 *****************************************************************************/

CopyStmt:	COPY BINARY_P qualified_name opt_column_list opt_oids opt_shard_list
			copy_from opt_program copy_file_name copy_delimiter opt_with copy_options OptExtSingleRowErrorHandling
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = $3;
					n->query = NULL;
					n->attlist = $4;
					n->attinfolist = NIL;
					n->fixed_position = false;
/* _SHARDING_ */
					n->shards = $6;
/* _SHARDING_ END */
					n->is_from = $7;
					n->is_program = $8;
					n->filename = $9;

					if (n->is_program && n->filename == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("STDIN/STDOUT not allowed with PROGRAM"),
								 parser_errposition(@8)));

					n->options = NIL;
					/* Concatenate user-supplied flags */
					/*if ($2)
						n->options = lappend(n->options, $2);*/
					n->options = lappend(n->options, makeDefElem("format", (Node *)makeString("binary"), @2));
					if ($5)
						n->options = lappend(n->options, $5);
					if ($10)
						n->options = lappend(n->options, $10);
					if ($12)
						n->options = list_concat(n->options, $12);

					n->sreh = $13;
					if(n->sreh && !n->is_from)
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("COPY single row error handling only available using COPY FROM")));

					$$ = (Node *)n;
				}
			| COPY qualified_name column_list_copy_without_pos opt_oids opt_shard_list
				copy_from opt_program copy_file_name copy_delimiter opt_with copy_options OptExtSingleRowErrorHandling
					{
						CopyStmt *n = makeNode(CopyStmt);
						ListCell *cell;
						n->relation = $2;
						n->query = NULL;
						n->attlist = NIL;
						n->attinfolist = $3;
						n->fixed_position = false;
						foreach(cell, $3)
						{
							AttInfo *attrinfo = (AttInfo *) lfirst(cell);
							n->attlist = lappend(n->attlist, attrinfo->attname);
						}
						/* _SHARDING_ */
						n->shards = $5;
						/* _SHARDING_ END */
						n->is_from = $6;
						n->is_program = $7;
						n->filename = $8;

						if (n->is_program && n->filename == NULL)
							ereport(ERROR,
							        (errcode(ERRCODE_SYNTAX_ERROR),
							         errmsg("STDIN/STDOUT not allowed with PROGRAM"),
							         parser_errposition(@7)));

						n->options = NIL;
						/* Concatenate user-supplied flags */
						if ($4)
							n->options = lappend(n->options, $4);
						if ($9)
							n->options = lappend(n->options, $9);
						if ($11)
							n->options = list_concat(n->options, $11);

						n->sreh = $12;
						if(n->sreh && !n->is_from)
							ereport(ERROR,
							        (errcode(ERRCODE_SYNTAX_ERROR),
							         errmsg("COPY single row error handling only available using COPY FROM")));
							$$ = (Node *)n;
						}
			| COPY qualified_name column_list_copy_with_pos opt_oids opt_shard_list
				copy_from opt_program copy_file_name copy_delimiter opt_with copy_options
				/* 1. num of fields  > 0; 2. fixed_position = true; 3. attinfolist != NIL*/
				{
					ListCell *cell;
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = $2;
					n->query = NULL;
					n->attlist = NIL;
					n->attinfolist = $3;
					n->fixed_position = true;
					if ($3 == NIL)
					ereport(ERROR,
					        (errcode(ERRCODE_SYNTAX_ERROR),
					         errmsg("COLUMN LIST could node be NIL in fixed_position."),
					         parser_errposition(@3)));
					foreach(cell, $3)
					{
						AttInfo *attrinfo = (AttInfo *) lfirst(cell);
						n->attlist = lappend(n->attlist, attrinfo->attname);
					}
					/* _SHARDING_ */
					n->shards = $5;
					/* _SHARDING_ END */
					n->is_from = $6;
					n->is_program = $7;
					n->filename = $8;

					if (n->is_program && n->filename == NULL)
					ereport(ERROR,
					        (errcode(ERRCODE_SYNTAX_ERROR),
					         errmsg("STDIN/STDOUT not allowed with PROGRAM"),
					         parser_errposition(@7)));

					if (!n->is_from)
						ereport(ERROR,
						        (errcode(ERRCODE_SYNTAX_ERROR),
					             errmsg("COLUMN POSITION not allowed in COPY TO"),
					             parser_errposition(@3)));

					n->options = NIL;
					/* Concatenate user-supplied flags */
					if ($4)
						n->options = lappend(n->options, $4);
					if ($9)
						n->options = lappend(n->options, $9);
					if ($11)
						n->options = list_concat(n->options, $11);
						$$ = (Node *)n;
				}
			| COPY '(' PreparableStmt ')' TO opt_program copy_file_name opt_with copy_options
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = NULL;
					n->query = $3;
					n->attlist = NIL;
					n->is_from = false;
					n->is_program = $6;
					n->filename = $7;
					n->options = $9;
/* _SHARDING_ */
					n->shards = NULL;
/* _SHARDING_ END */
					if (n->is_program && n->filename == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("STDIN/STDOUT not allowed with PROGRAM"),
								 parser_errposition(@5)));

					$$ = (Node *)n;
				}
		;

copy_from:
			FROM									{ $$ = TRUE; }
			| TO									{ $$ = FALSE; }
		;

opt_program:
			PROGRAM									{ $$ = TRUE; }
			| /* EMPTY */							{ $$ = FALSE; }
		;
/* _SHARDING_ */
opt_shard_list:
			SHARDING '(' shard_list ')'				{ $$ = $3; }
			| /* EMPTY */							{ $$ = NULL; }
		;
/* END _SHARDING_ */

/*
 * copy_file_name NULL indicates stdio is used. Whether stdin or stdout is
 * used depends on the direction. (It really doesn't make sense to copy from
 * stdout. We silently correct the "typo".)		 - AY 9/94
 */
copy_file_name:
			Sconst									{ $$ = $1; }
			| STDIN									{ $$ = NULL; }
			| STDOUT								{ $$ = NULL; }
		;

copy_options: copy_opt_list							{ $$ = $1; }
			| '(' copy_generic_opt_list ')'			{ $$ = $2; }
		;

/* old COPY option syntax */
copy_opt_list:
			copy_opt_list copy_opt_item				{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

copy_opt_item:
			BINARY
				{
					$$ = makeDefElem("format", (Node *)makeString("binary"), @1);
				}
			| OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE), @1);
				}
			| FREEZE
				{
					$$ = makeDefElem("freeze", (Node *)makeInteger(TRUE), @1);
				}
			| DELIMITER opt_as Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3), @1);
				}
			| NULL_P opt_as Sconst
				{
					$$ = makeDefElem("null", (Node *)makeString($3), @1);
				}
			| CSV
				{
					$$ = makeDefElem("format", (Node *)makeString("csv"), @1);
				}
			| ROWS
				{
					$$ = makeDefElem("format", (Node *)makeString("internal"), @1);
				}
			| FIXED_P
				{
					$$ = makeDefElem("format", (Node *)makeString("fixed"), @1);
				}
			| HEADER_P
				{
					$$ = makeDefElem("header", (Node *)makeInteger(TRUE), @1);
				}
			| QUOTE opt_as Sconst
				{
					$$ = makeDefElem("quote", (Node *)makeString($3), @1);
				}
			| ESCAPE opt_as Sconst
				{
					$$ = makeDefElem("escape", (Node *)makeString($3), @1);
				}
			| FORCE QUOTE columnList
				{
					$$ = makeDefElem("force_quote", (Node *)$3, @1);
				}
			| FORCE QUOTE '*'
				{
					$$ = makeDefElem("force_quote", (Node *)makeNode(A_Star), @1);
				}
			| FORCE NOT NULL_P columnList
				{
					$$ = makeDefElem("force_not_null", (Node *)$4, @1);
				}
			| FORCE NULL_P columnList
				{
					$$ = makeDefElem("force_null", (Node *)$3, @1);
				}
			| ENCODING Sconst
				{
					$$ = makeDefElem("encoding", (Node *)makeString($2), @1);
				}
			| EOL Sconst
				{
					$$ = makeDefElem("eol", (Node *)makeString($2), @1);
				}
			| COMPATIBLE_ILLEGAL_CHARS opt_illegal_conv_string
				{
					$$ = makeDefElem("compatible_illegal_chars", (Node*)$2, @1);
				}
			| FILL_MISSING_FIELDS
			    {
					$$ = makeDefElem("fill_missing_fields", (Node *)makeInteger(TRUE), @1);
			    }
			| IGNORE_EXTRA_DATA
				{
					$$ = makeDefElem("ignore_extra_data", (Node *)makeInteger(TRUE), @1);
				}
			| FORMATTER opt_with_cut '(' copy_foramtter ')'
				{
					CopyFormatter *cf = makeNode(CopyFormatter);
					cf->fixed_attrs = $4;
					cf->with_cut = $2;
					$$ = makeDefElem("formatter", (Node*)cf, @1);
				}
		;

opt_illegal_conv_string: 
			opt_as Sconst
				{
					$$ = (Node *) makeString($2);
				}
			| /* EMPTY */							{ $$ = (Node *)makeString("?"); }
			;
			
copy_foramtter:
			copy_col_fixed_len
				{
					$$ = list_make1($1);
				}
			| copy_foramtter ',' copy_col_fixed_len
				{
					$$ = lappend($1, $3);
				}
		;

copy_col_fixed_len:
			ColId opt_with_cut '(' Iconst ')'
				{
					CopyAttrFixed *arg = makeNode(CopyAttrFixed);
					arg->attname = $1;
					arg->fix_len = $4;
					arg->with_cut = $2;
					$$ = (Node*)arg;
				}

opt_with_cut:
			WITH TRUNCATE							{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* The following exist for backward compatibility with very old versions */
opt_oids:
			WITH OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE), @1);
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

copy_delimiter:
			opt_using DELIMITERS Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3), @2);
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_using:
			USING									{}
			| /*EMPTY*/								{}
		;

/* new COPY option syntax */
copy_generic_opt_list:
			copy_generic_opt_elem
				{
					$$ = list_make1($1);
				}
			| copy_generic_opt_list ',' copy_generic_opt_elem
				{
					$$ = lappend($1, $3);
				}
		;

copy_generic_opt_elem:
			Lower_ColLabel copy_generic_opt_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;

copy_generic_opt_arg:
			opt_boolean_or_string			{ $$ = (Node *) makeString($1); }
			| NumericOnly					{ $$ = (Node *) $1; }
			| '*'							{ $$ = (Node *) makeNode(A_Star); }
			| '(' copy_generic_opt_arg_list ')'		{ $$ = (Node *) $2; }
			| /* EMPTY */					{ $$ = NULL; }
		;

copy_generic_opt_arg_list:
			  copy_generic_opt_arg_list_item
				{
					$$ = list_make1($1);
				}
			| copy_generic_opt_arg_list ',' copy_generic_opt_arg_list_item
				{
					$$ = lappend($1, $3);
				}
		;

/* beware of emitting non-string list elements here; see commands/define.c */
copy_generic_opt_arg_list_item:
			opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
		;


/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *		PGXC-related extensions:
 *		1) Distribution type of a table:
 *			DISTRIBUTE BY ( HASH(column) | MODULO(column) |
 *							REPLICATION | ROUNDROBIN )
 *		2) Subcluster for table
 *			TO ( GROUP groupname | NODE nodename1,...,nodenameN )
 *
 *****************************************************************************/

CreateStmt:	CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptPartitionSpec OptWith OnCommitOption OptPhyAttrClauseList
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					ListCell *attr;

					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = $8;
					n->partspec = $9;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $10;
					n->oncommit = $11;

					foreach(attr, $12)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tablespacename = defGetString(d);
					}

					n->if_not_exists = false;
/* PGXC_BEGIN */
					if ($2 == RELPERSISTENCE_LOCAL_TEMP)
					{
						$4->relpersistence = RELPERSISTENCE_TEMP;
						n->islocal = true;
					}
					n->relkind = RELKIND_RELATION;
					n->distributeby = $13;
					n->subcluster = $14;
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('
			OptTableElementList ')' OptInherit OptPartitionSpec OptWith
			OnCommitOption OptPhyAttrClauseList
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					ListCell *attr;

					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->inhRelations = $11;
					n->partspec = $12;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $13;
					n->oncommit = $14;

					foreach(attr, $15)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tablespacename = defGetString(d);
					 }

					n->if_not_exists = true;
/* PGXC_BEGIN */
					if ($2 == RELPERSISTENCE_LOCAL_TEMP)
					{
						$7->relpersistence = RELPERSISTENCE_TEMP;
						n->islocal = true;
					}
					n->relkind = RELKIND_RELATION;
					n->distributeby = $16;
					n->subcluster = $17;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE qualified_name OF any_name
			OptTypedTableElementList OptPartitionSpec OptWith OnCommitOption
			OptPhyAttrClauseList
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					ListCell *attr;

					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $7;
					n->inhRelations = NIL;
					n->partspec = $8;
					n->ofTypename = makeTypeNameFromNameList($6);
					n->ofTypename->location = @6;
					n->constraints = NIL;
					n->options = $9;
					n->oncommit = $10;

					foreach(attr, $11)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tablespacename = defGetString(d);
					}

					n->if_not_exists = false;
/* PGXC_BEGIN */
					if ($2 == RELPERSISTENCE_LOCAL_TEMP)
					{
						$4->relpersistence = RELPERSISTENCE_TEMP;
						n->islocal = true;
					}
					n->relkind = RELKIND_RELATION;
					n->distributeby = $12;
					n->subcluster = $13;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name
			OptTypedTableElementList OptPartitionSpec OptWith OnCommitOption
			OptPhyAttrClauseList
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					ListCell *attr;

					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $10;
					n->inhRelations = NIL;
					n->partspec = $11;
					n->ofTypename = makeTypeNameFromNameList($9);
					n->ofTypename->location = @9;
					n->constraints = NIL;
					n->options = $12;
					n->oncommit = $13;

					foreach(attr, $14)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tablespacename = defGetString(d);
					}

					n->if_not_exists = true;
/* PGXC_BEGIN */
					if ($2 == RELPERSISTENCE_LOCAL_TEMP)
					{
						$7->relpersistence = RELPERSISTENCE_TEMP;
						n->islocal = true;
					}
					n->relkind = RELKIND_RELATION;
					n->distributeby = $15;
					n->subcluster = $16;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* PGXC_END */
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE qualified_name PARTITION OF qualified_name
			OptTypedTableElementList PartitionBoundSpec OptPartitionSpec OptWith
			OnCommitOption OptPhyAttrClauseList OptAlias
				{
					CreateStmt *n = makeNode(CreateStmt);
					ListCell *attr;

					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $8;
					n->inhRelations = list_make1($7);
					n->partbound = $9;
					n->partspec = $10;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $11;
					n->oncommit = $12;

					foreach(attr, $13)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tablespacename = defGetString(d);
					 }

					n->aliasname = $14;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name PARTITION OF
			qualified_name OptTypedTableElementList PartitionBoundSpec OptPartitionSpec
			OptWith OnCommitOption OptPhyAttrClauseList OptAlias
				{
					CreateStmt *n = makeNode(CreateStmt);
					ListCell *attr;

					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $11;
					n->inhRelations = list_make1($10);
					n->partbound = $12;
					n->partspec = $13;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = $14;
					n->oncommit = $15;

					foreach(attr, $16)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tablespacename = defGetString(d);
					}

					n->aliasname = $17;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior, so warn about that.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 */
OptTemp:	TEMPORARY					{ $$ = RELPERSISTENCE_TEMP; }
			| TEMP						{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMPORARY			{ $$ = RELPERSISTENCE_LOCAL_TEMP; }
			| LOCAL TEMP				{ $$ = RELPERSISTENCE_LOCAL_TEMP; }
			| GLOBAL TEMPORARY
				{
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMP
				{
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = RELPERSISTENCE_TEMP;
				}
			| UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;

OptTableElementList:
			TableElementList					{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

OptTypedTableElementList:
			'(' TypedTableElementList ')'		{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TableElementList:
			TableElement
				{
					$$ = list_make1($1);
				}
			| TableElementList ',' TableElement
				{
					$$ = lappend($1, $3);
				}
		;

TypedTableElementList:
			TypedTableElement
				{
					$$ = list_make1($1);
				}
			| TypedTableElementList ',' TypedTableElement
				{
					$$ = lappend($1, $3);
				}
		;

TableElement:
			columnDef							{ $$ = $1; }
			| TableLikeClause					{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;

TypedTableElement:
			columnOptions						{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;

columnDef:	ColId Typename opt_storage_encoding create_generic_options ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $4;
					n->encoding =  $3;
					SplitColQualList($5, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
			| opentenbase_ora_coldef_ident Typename opt_storage_encoding create_generic_options ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $4;
					n->encoding =  $3;
					SplitColQualList($5, &n->constraints, &n->collClause,
									yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
		;

columnOptions:	ColId opt_storage_encoding ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->encoding = $2;
					SplitColQualList($3, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
				| ColId WITH OPTIONS opt_storage_encoding ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->encoding = $4;
					SplitColQualList($5, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
		;

ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					Constraint *n = castNode(Constraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
			| COLLATE lower_any_name
				{
					/*
					 * Note: the CollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
		;

opt_storage_encoding: ENCODING definition { $$ = $2; }
		|/* nothing */ {$$ = NULL;}
		;

/* DEFAULT NULL is already the default for Postgres.
 * But define it here and carry it forward into the system
 * to make it explicit.
 * - thomas 1998-09-13
 *
 * WITH NULL and NULL are not SQL-standard syntax elements,
 * so leave them out. Use DEFAULT NULL to explicitly indicate
 * that a column may have that value. WITH NULL leads to
 * shift/reduce conflicts with WITH TIME ZONE anyway.
 * - thomas 1999-01-08
 *
 * DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
 * conflict on NOT (since NOT might start a subsequent NOT NULL constraint,
 * or be part of a_expr NOT LIKE or similar constructs).
 */
ColConstraintElem:
			NOT NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					n->indexspace = $3;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					n->indexspace = $4;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')' opt_no_inherit
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->skip_validation = false;
					n->initially_valid = true;
					$$ = (Node *)n;
				}
			| DEFAULT b_expr
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_DEFAULT;
					n->location = @1;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_IDENTITY;
					n->generated_when = $2;
					n->options = $5;
					n->location = @1;
					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid  = true;
					$$ = (Node *)n;
				}
		;

generated_when:
			ALWAYS			{ $$ = ATTRIBUTE_IDENTITY_ALWAYS; }
			| BY DEFAULT	{ $$ = ATTRIBUTE_IDENTITY_BY_DEFAULT; }
		;

/*
 * ConstraintAttr represents constraint attributes, which we parse as if
 * they were independent constraint clauses, in order to avoid shift/reduce
 * conflicts (since NOT might start either an independent NOT NULL clause
 * or an attribute).  parse_utilcmd.c is responsible for attaching the
 * attribute information to the preceding "real" constraint node, and for
 * complaining if attribute clauses appear in the wrong place or wrong
 * combinations.
 *
 * See also ConstraintAttributeSpec, which can be used in places where
 * there is no parsing conflict.  (Note: currently, NOT VALID and NO INHERIT
 * are allowed clauses in ConstraintAttributeSpec, but not here.  Someday we
 * might need to allow them here too, but for the moment it doesn't seem
 * useful in the statements that use ConstraintAttr.)
 */
ConstraintAttr:
			DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NOT DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_NOT_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY DEFERRED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRED;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY IMMEDIATE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_IMMEDIATE;
					n->location = @1;
					$$ = (Node *)n;
				}
		;


TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					n->options = $3;
					$$ = (Node *)n;
				}
		;

TableLikeOptionList:
				TableLikeOptionList INCLUDING TableLikeOption	{ $$ = $1 | $3; }
				| TableLikeOptionList EXCLUDING TableLikeOption	{ $$ = $1 & ~$3; }
				| /* EMPTY */						{ $$ = 0; }
		;

TableLikeOption:
				DEFAULTS			{ $$ = CREATE_TABLE_LIKE_DEFAULTS; }
				| CONSTRAINTS		{ $$ = CREATE_TABLE_LIKE_CONSTRAINTS; }
				| IDENTITY_P		{ $$ = CREATE_TABLE_LIKE_IDENTITY; }
				| INDEXES			{ $$ = CREATE_TABLE_LIKE_INDEXES; }
				| STORAGE			{ $$ = CREATE_TABLE_LIKE_STORAGE; }
				| COMMENTS			{ $$ = CREATE_TABLE_LIKE_COMMENTS; }
				| ALL				{ $$ = CREATE_TABLE_LIKE_ALL; }
		;


/* ConstraintElem specifies constraint syntax which is not embedded into
 *	a column definition. ColConstraintElem specifies the embedded form.
 * - thomas 1997-12-03
 */
TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					Constraint *n = castNode(Constraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ConstraintElem						{ $$ = $1; }
		;

ConstraintElem:
			CHECK '(' a_expr ')' ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					processCASbits($5, @5, "CHECK",
								   NULL, NULL, &n->skip_validation,
								   &n->is_no_inherit, yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
			| UNIQUE '(' columnList ')' opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->options = $5;
					n->indexname = NULL;
					n->indexspace = $6;
					processCASbits($7, @7, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| UNIQUE ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $2;
					n->indexspace = NULL;
					processCASbits($3, @3, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY '(' columnList ')' opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $4;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = $7;
					processCASbits($8, @8, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $3;
					n->indexspace = NULL;
					processCASbits($4, @4, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| EXCLUDE access_method_clause '(' ExclusionConstraintList ')'
				opt_definition OptConsTableSpace ExclusionWhereClause
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_EXCLUSION;
					n->location = @1;
					n->access_method	= $2;
					n->exclusions		= $4;
					n->options			= $6;
					n->indexname		= NULL;
					n->indexspace		= $7;
					n->where_clause		= $8;
					processCASbits($9, @9, "EXCLUDE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					processCASbits($11, @11, "FOREIGN KEY",
								   &n->deferrable, &n->initdeferred,
								   &n->skip_validation, NULL,
								   yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
		;

opt_no_inherit:	NO INHERIT							{  $$ = TRUE; }
			| /* EMPTY */							{  $$ = FALSE; }
		;

column_list_copy_without_pos: 	'(' columnList_copy_without_pos ')'		{ $$ = $2; }
				| /*EMPTY*/					{ $$ = NIL; }
		;

column_list_copy_with_pos:	'(' columnList_copy_with_pos ')'		{ $$ = $2; }
		;

columnList_copy_without_pos:
			columnElem_copy_without_pos								{ $$ = list_make1($1); }
			| columnList_copy_without_pos ',' columnElem_copy_without_pos				{ $$ = lappend($1, $3); }
		;

columnElem_copy_without_pos: ColId opt_a_expr
				{
					AttInfo *n = (AttInfo *)makeNode(AttInfo);
					n->attname = (Node *) makeString($1);
					n->attrpos = NULL;
					n->custom_expr = $2;
					$$ = (Node *)n;
				}
		;

columnList_copy_with_pos:
			columnElem_copy_with_pos								{ $$ = list_make1($1); }
			| columnList_copy_with_pos ',' columnElem_copy_with_pos				{ $$ = lappend($1, $3); }
		;

columnElem_copy_with_pos: ColId POSITION attr_pos opt_a_expr
				{
					AttInfo *n = (AttInfo *)makeNode(AttInfo);
					n->attname = (Node *) makeString($1);
					n->attrpos = (Position *)$3;
					n->custom_expr = $4;
					$$ = (Node *)n;
				}
		;

opt_a_expr:	a_expr 			{ $$ = $1; }
		|  /*EMPTY*/		{ $$ = NULL; }
		;

attr_pos:
		'(' Iconst ':' Iconst ')' /* (1 : 4) */
		{
			Position *n = (Position *)makeNode(Position);
			n->flag = ABSOLUTE_POS;
			n->start_offset = $2;
			n->end_offset = $4;
			if (n->start_offset <= 0 || n->start_offset > n->end_offset)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("invalid parameter"), parser_errposition(@2)));
			$$ = (Node *)n;
		}
		| '(' '*' ':' Iconst ')' /* (* : 6) */
		{
			Position *n = (Position *)makeNode(Position);
			n->flag = RELATIVE_POS;
			n->start_offset = 0;
			n->end_offset = $4;
			if (n->start_offset < 0 || n->start_offset > n->end_offset)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("invalid parameter"), parser_errposition(@4)));
			$$ = (Node *)n;
		}
		| '(' '*' '+' Iconst ':' Iconst')' /* ( * + 1 : 6) */
		{
			Position *n = (Position *)makeNode(Position);
			n->flag = RELATIVE_POS;
			n->start_offset = $4;
			n->end_offset = $6;
			if (n->start_offset <= 0 || n->start_offset > n->end_offset)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("invalid parameter"), parser_errposition(@4)));
			$$ = (Node *)n;
		}
		;

opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;

columnElem: ColId
				{
					$$ = (Node *) makeString($1);
				}
		;

distributed_by_list:
			distributed_by_elem						{ $$ = NIL; }
			| distributed_by_list ',' distributed_by_elem
				{ $$ = NIL; }
			;
distributed_by_elem: ColId opt_class
				{ $$ = NIL; }
			;

key_match:  MATCH FULL
			{
				$$ = FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented"),
						 parser_errposition(@1)));
				$$ = FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		| /*EMPTY*/
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		;

ExclusionConstraintList:
			ExclusionConstraintElem					{ $$ = list_make1($1); }
			| ExclusionConstraintList ',' ExclusionConstraintElem
													{ $$ = lappend($1, $3); }
		;

ExclusionConstraintElem: index_elem WITH any_operator
			{
				$$ = list_make2($1, $3);
			}
			/* allow OPERATOR() decoration for the benefit of ruleutils.c */
			| index_elem WITH OPERATOR '(' any_operator ')'
			{
				$$ = list_make2($1, $5);
			}
		;

ExclusionWhereClause:
			WHERE '(' a_expr ')'					{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*
 * We combine the update and delete actions into one value temporarily
 * for simplicity of parsing, and then break them down again in the
 * calling production.  update is in the left 8 bits, delete in the right.
 * Note that NOACTION is the default.
 */
key_actions:
			key_update
				{ $$ = ($1 << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
		;

key_update: ON UPDATE key_action		{ $$ = $3; }
		;

key_delete: ON DELETE_P key_action		{ $$ = $3; }
		;

key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }
		;

OptInherit: INHERITS '(' qualified_name_list ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* Optional partition key specification */
OptPartitionSpec: PartitionSpec	{ $$ = $1; }
			| /*EMPTY*/			{ $$ = NULL; }
		;

PartitionSpec: PARTITION BY part_strategy '(' part_params ')'
				{
					PartitionSpec *n = makeNode(PartitionSpec);

					n->strategy = $3;
					n->partParams = $5;
					n->location = @1;
					n->subPartSpec = NULL;

					$$ = n;
				}
		;

part_strategy:	Lower_IDENT				{ $$ = $1; }
				| unreserved_keyword	{ $$ = pstrdup($1); }
				| pkg_reserved_keyword	{ $$ = pstrdup($1); }
		;

part_params:	part_elem						{ $$ = list_make1($1); }
			| part_params ',' part_elem			{ $$ = lappend($1, $3); }
		;

part_elem: ColId opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = $1;
					n->expr = NULL;
					n->collation = $2;
					n->opclass = $3;
					n->location = @1;
					$$ = n;
				}
			| func_expr_windowless opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = NULL;
					n->expr = $1;
					n->collation = $2;
					n->opclass = $3;
					n->location = @1;
					$$ = n;
				}
			| '(' a_expr ')' opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = NULL;
					n->expr = $2;
					n->collation = $4;
					n->opclass = $5;
					n->location = @1;
					$$ = n;
				}
		;
/* WITH (options) is preferred, WITH OIDS and WITHOUT OIDS are legacy forms */
OptWith:
			WITH reloptions				{ $$ = $2; }
			| WITH OIDS					{ $$ = list_make1(makeDefElem("oids", (Node *) makeInteger(true), @1)); }
			| WITHOUT OIDS				{ $$ = list_make1(makeDefElem("oids", (Node *) makeInteger(false), @1)); }
			| /*EMPTY*/					{ $$ = NIL; }
		;

OnCommitOption:  ON COMMIT DROP				{ $$ = ONCOMMIT_DROP; }
			| ON COMMIT DELETE_P ROWS		{ $$ = ONCOMMIT_DELETE_ROWS; }
			| ON COMMIT PRESERVE ROWS		{ $$ = ONCOMMIT_PRESERVE_ROWS; }
			| /*EMPTY*/						{ $$ = ONCOMMIT_NOOP; }
		;

OptTableSpace:   TABLESPACE name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

TableSpaceInPhyAttr:  TABLESPACE name				{ $$ = makeDefElem(pstrdup($1), (Node *) makeString(pstrdup($2)), @1); }
		;

SizeClause:	NumericOnly IDENT		{ $$ = $1; }
			| NumericOnly		{ $$ = $1; }
			;

BuffPool:		KEEP				{ $$ = makeString(pstrdup($1)); }
			| RECYCLE			{ $$ = makeString(pstrdup($1)); }
			| DEFAULT			{ $$ = makeString(pstrdup($1)); }
			;

FlashCache:		KEEP				{ $$ = makeString(pstrdup($1)); }
			| NONE				{ $$ = makeString(pstrdup($1)); }
			| DEFAULT			{ $$ = makeString(pstrdup($1)); }
			;

StorageOneClause:	INITIAL SizeClause
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| NEXT SizeClause
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| MINEXTENTS NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| MAXEXTENTS NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| MAXEXTENTS UNLIMITED
			{
				$$ = makeDefElem(pstrdup($1), (Node *) makeString(pstrdup($2)), @1);
			}

			| MAXSIZE SizeClause
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| MAXSIZE UNLIMITED
			{
				$$ = makeDefElem(pstrdup($1), (Node *) makeString(pstrdup($2)), @1);
			}

			| PCTINCREASE NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| FREELISTS NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| FREELIST GROUPS NumericOnly
			{
				$$ = makeDefElem("freelist_groups", (Node *) $3, @1);
			}

			| OPTIMAL SizeClause
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| OPTIMAL NULL_P
			{
				$$ = makeDefElem(pstrdup($1), (Node *) makeString(pstrdup($2)), @1);
			}

			| BUFFER_POOL BuffPool
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| FLASH_CACHE FlashCache
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| CELL_FLASH_CACHE FlashCache
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| ENCRYPT
			{
				$$ = makeDefElem(pstrdup($1), (Node *)makeInteger(TRUE), @1);
			}
			;

StorageClauseList:	StorageOneClause			{ $$ = list_make1($1); }
			| StorageClauseList StorageOneClause	{ $$ = lappend($1, $2); }
			;

StorageAllClauseList:   	STORAGE '(' StorageClauseList  ')'		{ $$ = $3; }
			;

PhyAttrClauseCommon:  PCTFREE NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| PCTUSED NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| INITRANS NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}

			| MAXTRANS NumericOnly
			{
				$$ = makeDefElem(pstrdup($1), (Node *) $2, @1);
			}
			;

OnePhyAttrClauseList:  TableSpaceInPhyAttr	{ $$ = list_make1($1); }
			| PhyAttrClauseCommon			{ $$ = list_make1($1); }
			| StorageAllClauseList			{ $$ = $1; }
			;

PhyAttrClauseList:  OnePhyAttrClauseList				{ $$ = $1; }
			| PhyAttrClauseList OnePhyAttrClauseList	{ $$ = list_concat($1, $2); }
			;

OptPhyAttrClauseList:  PhyAttrClauseList	{ $$ = $1; }
			| /*EMPTY*/		{ $$ = NIL; }
			;

OptAlias:	 AS ColId					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		 ;

/* PGXC_BEGIN */
OptDistributeBy: OptDistributeByInternal			{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

/*
 * For the distribution type, we use IDENT to limit the impact of keywords
 * related to distribution on other commands and to allow extensibility for
 * new distributions.
 */
OptDistributeType: Lower_IDENT							{ $$ = $1; }
		;

DistributeStyle: ALL								{ $$ = strdup("all"); }
			| KEY									{ $$ = strdup("key"); }
			| Lower_IDENT
				{
					if (strcmp($1, "even") != 0)
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("unrecognized distribution style \"%s\"", $1)));
					$$ = $1;
				}
		;

OptDistKey: DISTKEY '(' name_list ')'					{ $$ = $3; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptDistributeByInternal:  DISTRIBUTE BY OptDistributeType '(' name_list ')'
				{
					DistributeBy *n = makeNode(DistributeBy);
					if (strcmp($3, "modulo") == 0)
						n->disttype = DISTTYPE_MODULO;
					else if (strcmp($3, "hash") == 0)
						n->disttype = DISTTYPE_HASH;
/*_PGPAL BEGIN*/
					else if (strcmp($3, "shard") == 0)
						n->disttype = DISTTYPE_SHARD;
/*_PGPAL END*/
					else
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("unrecognized distribution option \"%s\"", $3)));
					n->colname = $5;
					$$ = n;
				}
			| DISTRIBUTE BY OptDistributeType
				{
					DistributeBy *n = makeNode(DistributeBy);
					if (strcmp($3, "replication") == 0)
                        n->disttype = DISTTYPE_REPLICATION;
					else if (strcmp($3, "roundrobin") == 0)
						n->disttype = DISTTYPE_ROUNDROBIN;
                    else
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("unrecognized distribution option \"%s\"", $3)));
					n->colname = NULL;
					$$ = n;
				}
			| DISTRIBUTED BY '(' name_list ')'
				{
					DistributeBy *n = makeNode(DistributeBy);
					n->disttype = DISTTYPE_HASH;
					n->colname = $4;
					$$ = n;
				}
			| DISTRIBUTED RANDOMLY
				{
					DistributeBy *n = makeNode(DistributeBy);
					n->disttype = DISTTYPE_ROUNDROBIN;
					n->colname = NULL;
					$$ = n;
				}
			| DISTSTYLE DistributeStyle OptDistKey
				{
					DistributeBy *n = makeNode(DistributeBy);
					if (strcmp($2, "even") == 0)
						n->disttype = DISTTYPE_ROUNDROBIN;
					else if (strcmp($2, "key") == 0)
						n->disttype = DISTTYPE_HASH;
					else if (strcmp($2, "all") == 0)
						n->disttype = DISTTYPE_REPLICATION;
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized distribution style \"%s\"", $2)));
					if ((n->disttype == DISTTYPE_ROUNDROBIN ||
						 n->disttype == DISTTYPE_REPLICATION) &&
						($3 != NULL))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("distribution key cannot be specified for distribution style \"%s\"", $2)));

					if ((n->disttype == DISTTYPE_HASH) && ($3 == NULL))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("distribution key must be specified for distribution style \"%s\"", $2)));

					n->colname = $3;
					$$ = n;
				}
		;

OptSubCluster: OptSubClusterInternal				{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSubClusterInternal:
			TO NODE pgxcnodes
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_NODE;
					n->members = $3;
					$$ = n;
				}
			| TO GROUP_P pgxcgroup_name
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_GROUP;
					n->members = list_make1(makeString($3));
					$$ = n;
				}
		;
/* PGXC_END */

OptConsTableSpace:   USING INDEX TABLESPACE name	{ $$ = $4; }
/* OPENTENBASE_ORA_BEGIN */
			| USING INDEX							{ $$ = NULL; }
/* OPENTENBASE_ORA_END */
			| /*EMPTY*/								{ $$ = NULL; }
		;

ExistingIndex:   USING INDEX index_name				{ $$ = $3; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE STATISTICS [IF NOT EXISTS] stats_name [(stat types)]
 *					ON expression-list FROM from_list
 *
 * Note: the expectation here is that the clauses after ON are a subset of
 * SELECT syntax, allowing for expressions and joined tables, and probably
 * someday a WHERE clause.  Much less than that is currently implemented,
 * but the grammar accepts it and then we'll throw FEATURE_NOT_SUPPORTED
 * errors as necessary at execution.
 *
 *****************************************************************************/

CreateStatsStmt:
			CREATE STATISTICS any_name
			opt_name_list ON expr_list FROM from_list
				{
					CreateStatsStmt *n = makeNode(CreateStatsStmt);
					n->defnames = $3;
					n->stat_types = $4;
					n->exprs = $6;
					n->relations = $8;
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE STATISTICS IF_P NOT EXISTS any_name
			opt_name_list ON expr_list FROM from_list
				{
					CreateStatsStmt *n = makeNode(CreateStatsStmt);
					n->defnames = $6;
					n->stat_types = $7;
					n->exprs = $9;
					n->relations = $11;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
			;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname AS SelectStmt [ WITH [NO] DATA ]
 *
 *
 * Note: SELECT ... INTO is a now-deprecated alternative for this.
 *
 *****************************************************************************/

CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $6;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
					if ($2 == RELPERSISTENCE_LOCAL_TEMP)
					{
						$4->rel->relpersistence = RELPERSISTENCE_TEMP;
						ctas->islocal = true;
					}

					$4->skipData = !($7);
					$$ = (Node *) ctas;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $9;
					ctas->into = $7;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = true;
					/* cram additional flags into the IntoClause */
					$7->rel->relpersistence = $2;
					$7->skipData = !($10);
					$$ = (Node *) ctas;
				}
		;

create_as_target:
			qualified_name opt_column_list OptWith OnCommitOption OptPhyAttrClauseList
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					ListCell *attr;

					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;

					foreach(attr, $5)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							$$->tableSpaceName = defGetString(d);
					}

					$$->viewQuery = NULL;
					$$->skipData = false;		/* might get changed later */
/* PGXC_BEGIN */
					$$->distributeby = $6;
					$$->subcluster = $7;
/* PGXC_END */
				}
		;

opt_with_data:
			WITH DATA_P								{ $$ = TRUE; }
			| WITH NO DATA_P						{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = TRUE; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE EXTERNAL [WEB] TABLE relname
 *
 *****************************************************************************/

CreateExternalStmt:	CREATE OptWritable EXTERNAL OptWeb OptTemp TABLE qualified_name '(' OptExtTableElementList ')'
					ExtTypedesc FORMAT Sconst format_opt ext_options_opt ext_opt_encoding_list OptExtSingleRowErrorHandling OptDistributedBy
						{
							CreateExternalStmt *n = makeNode(CreateExternalStmt);
							n->iswritable = $2;
							n->isweb = $4;
							$7->relpersistence = $5;
							n->relation = $7;
							n->tableElts = $9;
							n->exttypedesc = (ExtTableTypeDesc *) $11;
							n->format = $13;
							n->formatOpts = $14;
							n->extOptions = $15;
							n->encoding = $16;
							n->sreh = $17;
							n->if_not_exists = false;
							n->distributeby = (DistributeBy *) $18;

							/* various syntax checks for EXECUTE external table */
							if(((ExtTableTypeDesc *) n->exttypedesc)->exttabletype == EXTTBL_TYPE_EXECUTE)
							{
								ExtTableTypeDesc *extdesc = (ExtTableTypeDesc *) n->exttypedesc;

								if(!n->isweb)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
										 	 errmsg("EXECUTE may not be used with a regular external table"),
											 errhint("Use CREATE EXTERNAL WEB TABLE instead.")));

								/* if no ON clause specified, default to "ON ALL" */
								if(extdesc->on_clause == NIL)
								{
									extdesc->on_clause = lappend(extdesc->on_clause,
										   				   		 makeDefElem("all", (Node *)makeInteger(true), @1));
								}
								else if(n->iswritable)
								{
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("ON clause may not be used with a writable external table")));
								}
							}
							if(n->sreh && n->iswritable)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("single row error handling may not be used with a writable external table")));

							$$ = (Node *)n;
						}
			| CREATE OptWritable EXTERNAL OptWeb OptTemp TABLE IF_P NOT EXISTS qualified_name '(' OptExtTableElementList ')'
					ExtTypedesc FORMAT Sconst format_opt ext_options_opt ext_opt_encoding_list OptExtSingleRowErrorHandling OptDistributedBy
						{
							CreateExternalStmt *n = makeNode(CreateExternalStmt);
							n->iswritable = $2;
							n->isweb = $4;
							$10->relpersistence = $5;
							n->relation = $10;
							n->tableElts = $12;
							n->exttypedesc = (ExtTableTypeDesc *) $14;
							n->format = $16;
							n->formatOpts = $17;
							n->extOptions = $18;
							n->encoding = $19;
							n->sreh = $20;
							n->if_not_exists = true;
							n->distributeby = (DistributeBy *) $21;

							/* various syntax checks for EXECUTE external table */
							if(((ExtTableTypeDesc *) n->exttypedesc)->exttabletype == EXTTBL_TYPE_EXECUTE)
							{
								ExtTableTypeDesc *extdesc = (ExtTableTypeDesc *) n->exttypedesc;

								if(!n->isweb)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
										 	 errmsg("EXECUTE may not be used with a regular external table"),
											 errhint("Use CREATE EXTERNAL WEB TABLE instead.")));

								/* if no ON clause specified, default to "ON ALL" */
								if(extdesc->on_clause == NIL)
								{
									extdesc->on_clause = lappend(extdesc->on_clause,
										   				   		 makeDefElem("all", (Node *)makeInteger(true), @1));
								}
								else if(n->iswritable)
								{
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("ON clause may not be used with a writable external table")));
								}
							}
							if(n->sreh && n->iswritable)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("single row error handling may not be used with a writable external table")));

							$$ = (Node *)n;
						}
						;
OptWritable:	WRITABLE				{ $$ = true; }
				| READABLE				{ $$ = false; }
				| /*EMPTY*/				{ $$ = false; }
				;
OptWeb:		WEB						{ $$ = true; }
			| /*EMPTY*/				{ $$ = false; }
			;
ExtTypedesc:
			LOCATION '(' cdb_string_list ')' ext_on_clause_list
			{
				ExtTableTypeDesc *n = makeNode(ExtTableTypeDesc);
				n->exttabletype = EXTTBL_TYPE_LOCATION;
				n->location_list = $3;
				n->on_clause = $5;
				n->command_string = NULL;
				$$ = (Node *)n;

			}
			| EXECUTE Sconst ext_on_clause_list
			{
				ExtTableTypeDesc *n = makeNode(ExtTableTypeDesc);
				n->exttabletype = EXTTBL_TYPE_EXECUTE;
				n->location_list = NIL;
				n->command_string = $2;
				n->on_clause = $3; /* default will get set later if needed */

				$$ = (Node *)n;
			}
			;
ext_on_clause_list:
			ext_on_clause_list ext_on_clause_item		{ $$ = lappend($1, $2); }
			| /*EMPTY*/									{ $$ = NIL; }
			;

ext_on_clause_item:
			ON ALL
			{
				$$ = makeDefElem("all", (Node *)makeInteger(true), @1);
			}
			| ON HOST Sconst
			{
				$$ = makeDefElem("hostname", (Node *)makeString($3), @1);
			}
			| ON HOST
			{
				$$ = makeDefElem("eachhost", (Node *)makeInteger(true), @1);
			}
			| ON MASTER
			{
				$$ = makeDefElem("coordinator", (Node *)makeInteger(true), @1);
			}
			| ON COORDINATOR
			{
				$$ = makeDefElem("coordinator", (Node *)makeInteger(true), @1);
			}
			| ON SEGMENT Iconst
			{
				$$ = makeDefElem("segment", (Node *)makeInteger($3), @1);
			}
			| ON Iconst
			{
				$$ = makeDefElem("random", (Node *)makeInteger($2), @1);
			}
			;
format_opt:
			  '(' format_opt_list ')'			{ $$ = $2; }
			| '(' format_def_list ')'			{ $$ = $2; }
			| '(' ')'							{ $$ = NIL; }
			| /*EMPTY*/							{ $$ = NIL; }
			;
format_opt_list:
			format_opt_item
			{
				$$ = list_make1($1);
			}
			| format_opt_list format_opt_item
			{
				$$ = lappend($1, $2);
			}
			;
format_def_list:
			format_def_item
			{
				$$ = list_make1($1);
			}
			| format_def_list ',' format_def_item
			{
				$$ = lappend($1, $3);
			}
			;
format_def_item:
    		Lower_ColLabel '=' def_arg
			{
				$$ = makeDefElem($1, $3, @1);
			}
			| Lower_ColLabel '=' '(' columnList ')'
			{
				$$ = makeDefElem($1, (Node *) $4, @1);
			}
			;

format_opt_item:
			DELIMITER opt_as Sconst
			{
				$$ = makeDefElem("delimiter", (Node *)makeString($3), @1);
			}
			| NULL_P opt_as Sconst
			{
				$$ = makeDefElem("null", (Node *)makeString($3), @1);
			}
			| CSV
			{
				$$ = makeDefElem("csv", (Node *)makeInteger(true), @1);
			}
			| HEADER_P
			{
				$$ = makeDefElem("header", (Node *)makeInteger(true), @1);
			}
			| QUOTE opt_as Sconst
			{
				$$ = makeDefElem("quote", (Node *)makeString($3), @1);
			}
			| ESCAPE opt_as Sconst
			{
				$$ = makeDefElem("escape", (Node *)makeString($3), @1);
			}
			| FORCE NOT NULL_P columnList
			{
				$$ = makeDefElem("force_not_null", (Node *)$4, @1);
			}
			| FORCE QUOTE columnList
			{
				$$ = makeDefElem("force_quote", (Node *)$3, @1);
			}
			| FORCE QUOTE '*'
			{
				$$ = makeDefElem("force_quote", (Node *)makeNode(A_Star), @1);
			}
			| FILL MISSING FIELDS
			{
				$$ = makeDefElem("fill_missing_fields", (Node *)makeInteger(true), @1);
			}
			| EOL Sconst
			{
				$$ = makeDefElem("eol", (Node *)makeString($2), @1);
			}
			| NEWLINE opt_as Sconst
			{
				$$ = makeDefElem("newline", (Node *)makeString($3), @1);
			}
			;
ext_options_opt:
			OPTIONS ext_options					{ $$ = $2; }
			| /*EMPTY*/                         { $$ = NIL; }
			;
ext_options:
			'(' ext_options_list ')'           { $$ = $2; }
			| '(' ')'                           { $$ = NIL; }
			;
ext_options_list:
			ext_options_item
			{
				$$ = list_make1($1);
			}
			| ext_options_list ',' ext_options_item
			{
				$$ = lappend($1, $3);
			}
			;
ext_options_item:
			ColLabel Sconst
			{
				$$ = makeDefElem($1, (Node *)makeString($2), @1);
			}
			;
OptExtTableElementList:
			ExtTableElementList				{ $$ = $1; }
			| ExtTableElementListWithPos			{ $$ = $1; }
			| /*EMPTY*/						{ $$ = NIL; }
			;
ExtTableElementList:
			ExtTableElement
			{
				$$ = list_make1($1);
			}
			| ExtTableElementList ',' ExtTableElement
			{
				$$ = lappend($1, $3);
			}
			;
ExtTableElementListWithPos:
			ExtcolumnDefWithPos
			{
				$$ = list_make1($1);
			}
			| ExtTableElementListWithPos ',' ExtcolumnDefWithPos
			{
				$$ = lappend($1, $3);
			}
			;
ExtTableElement:
			ExtcolumnDef					{ $$ = $1; }
			| TableLikeClause				{ $$ = $1; }
			;

ForeignPosition:
			POSITION attr_pos
			{
				$$ = $2;
			}
		;

/* column def for ext table - doesn't have room for constraints */
ExtcolumnDef:	ColId Typename
		{
			ColumnDef *n = makeNode(ColumnDef);
			n->colname = $1;
			n->typeName = $2;
			n->is_local = true;
			n->is_not_null = false;
			n->constraints = NIL;
			$$ = (Node *)n;
		}
		;

ExtcolumnDefWithPos:	ColId Typename ForeignPosition
		{
			ColumnDef *n = makeNode(ColumnDef);
			AttInfo *attinfo = (AttInfo *)makeNode(AttInfo);
			n->colname = $1;
			n->typeName = $2;
			n->is_local = true;
			n->is_not_null = false;
			n->constraints = NIL;

			attinfo->attname = (Node *)makeString(pstrdup($1));
			attinfo->attrpos = (Position *)$3;
			n->attinfo = attinfo;
			$$ = (Node *)n;
		}
		;

/*
 * External table Single row error handling SQL
 */
OptExtSingleRowErrorHandling:
		ExtLogErrorTable SEGMENT REJECT_P LIMIT Iconst OptSrehLimitType
		{
			SingleRowErrorDesc *n = makeNode(SingleRowErrorDesc);
			n->log_error_type = $1;
			n->rejectlimit = $5;
			n->is_limit_in_rows = $6; /* true for ROWS false for PERCENT */

			/* PERCENT value check */
			if(!n->is_limit_in_rows && (n->rejectlimit < 1 || n->rejectlimit > 100))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid PERCENT value. Should be (1 - 100)")));

			/* ROW values check */
			if(n->is_limit_in_rows && n->rejectlimit < 2)
			   ereport(ERROR,
					   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid (ROWS) reject limit. Should be 2 or larger")));
			$$ = (Node *)n;
		}
		| SEGMENT REJECT_P LIMIT Iconst OptSrehLimitType
		{
			SingleRowErrorDesc *n = makeNode(SingleRowErrorDesc);
			n->log_error_type = 'i'; /* does not log erros */
			n->rejectlimit = $4;
			n->is_limit_in_rows = $5; /* true for ROWS false for PERCENT */

			/* PERCENT value check */
			if(!n->is_limit_in_rows && (n->rejectlimit < 1 || n->rejectlimit > 100))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid PERCENT value. Should be (1 - 100)")));

			/* ROW values check */
			if(n->is_limit_in_rows && n->rejectlimit < 2)
			   ereport(ERROR,
					   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid (ROWS) reject limit. Should be 2 or larger")));
			$$ = (Node *)n;
		}
		| ExtLogErrorTable
		{
			SingleRowErrorDesc *n = makeNode(SingleRowErrorDesc);
			n->log_error_type = $1;
			n->rejectlimit = SREH_UNLIMITED; // is_limit_in_rows makes no sense in this case.
			ereport(NOTICE,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("LOG ERRORS WITHOUT REJECT LIMIT may produce large error log")));

			$$ = (Node *)n;
		}
		| /*EMPTY*/		{ $$ = NULL; }
		;

ExtLogErrorTable:
		LOG_P ERRORS					{ $$ = 't'; }
		| LOG_P ERRORS PERSISTENTLY			{ $$ = 'p'; }
		| LOG_P ERRORS INTO qualified_name
		{
			if (tdx_ignore_error_table) /* ignore the [INTO error-table] clause for backward compatibility */
			{
			ereport(WARNING,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("error table is not supported"),
					 errhint("Use tdx_read_error_log() and tdx_truncate_error_log() to view and manage the internal error log associated with your table.")));
			}
			else
			{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("error table is not supported"),
					 errhint("Set tdx_ignore_error_table to ignore the [INTO error-table] clause for backward compatibility."),
					 parser_errposition(@3)));
			}
			$$ = 't';
		}
		;

OptSrehLimitType:
		ROWS					{ $$ = true; }
		| PERCENT				{ $$ = false; }
		| /* default is ROWS */	{ $$ = true; }
		;
/*
 * ENCODING. (we cheat a little and use a list, even though it's 1 item max).
 */
ext_opt_encoding_list:
		ext_opt_encoding_list ext_opt_encoding_item		{ $$ = lappend($1, $2); }
		| /*EMPTY*/										{ $$ = NIL; }
		;

ext_opt_encoding_item:
		ENCODING opt_equal Sconst
		{
			$$ = makeDefElem("encoding", (Node *)makeString($3), @1);
		}
		| ENCODING opt_equal Iconst
		{
			$$ = makeDefElem("encoding", (Node *)makeInteger($3), @1);
		}
		;
DistributedBy:   DISTRIBUTED BY  '(' distributed_by_list ')'
			{
				$$ = (Node *) NIL;
			}
		;
OptDistributedBy:   DistributedBy			{ $$ = $1; }
			|							{ $$ = NULL; }
		;
cdb_string_list:
			cdb_string							{ $$ = list_make1($1); }
			| cdb_string_list ',' cdb_string
				{
					if (list_member($1, $3))
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
								 errmsg("duplicate location uri"),
								 parser_errposition(@3)));
					$$ = lappend($1, $3);
				}
		;
cdb_string:
			Sconst
				{
					$$ = (Node *) makeString($1);
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE MATERIALIZED VIEW relname AS SelectStmt
 *
 *****************************************************************************/

CreateMatViewStmt:
		CREATE OptNoLog MATERIALIZED VIEW create_mv_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $7;
					ctas->into = $5;
					ctas->relkind = OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->if_not_exists = false;
					/* cram additional flags into the IntoClause */
					$5->rel->relpersistence = $2;
					$5->skipData = !($8);
					$$ = (Node *) ctas;
				}
		| CREATE OptNoLog MATERIALIZED VIEW IF_P NOT EXISTS create_mv_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $10;
					ctas->into = $8;
					ctas->relkind = OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->if_not_exists = true;
					/* cram additional flags into the IntoClause */
					$8->rel->relpersistence = $2;
					$8->skipData = !($11);
					$$ = (Node *) ctas;
				}
		;

create_mv_target:
			qualified_name opt_column_list opt_reloptions OptPhyAttrClauseList
				{
					ListCell *attr;

					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = ONCOMMIT_NOOP;

					foreach(attr, $4)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							$$->tableSpaceName = defGetString(d);
					}

					$$->viewQuery = NULL;		/* filled at analysis time */
					$$->skipData = false;		/* might get changed later */
				}
		;

OptNoLog:	UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;


/*****************************************************************************
 *
 *		QUERY :
 *				REFRESH MATERIALIZED VIEW qualified_name
 *
 *****************************************************************************/

RefreshMatViewStmt:
			REFRESH MATERIALIZED VIEW opt_concurrently qualified_name opt_with_data
				{
					RefreshMatViewStmt *n = makeNode(RefreshMatViewStmt);
					n->concurrent = $4;
					n->relation = $5;
					n->skipData = !($6);
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/

CreateSeqStmt:
			CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$4->relpersistence = $2;

					/* Both opentenbase_ora and PostgreSQL do not support global sequences. */
					if ($2 == RELPERSISTENCE_GLOBAL_TEMP)
					{
						ereport(WARNING,(errmsg("GLOBAL is deprecated in temporary table creation"),
							parser_errposition(@1)));
						$4->relpersistence = RELPERSISTENCE_TEMP;
					}

					n->sequence = $4;
					n->options = $5;
					n->ownerId = InvalidOid;
/* PGXC_BEGIN */
					n->is_serial = false;
/* PGXC_END */
					n->if_not_exists = false;
					$$ = (Node *)n;
				}
			| CREATE OptTemp SEQUENCE IF_P NOT EXISTS qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$7->relpersistence = $2;

					/* Both opentenbase_ora and PostgreSQL do not support global sequences. */
					if ($2 == RELPERSISTENCE_GLOBAL_TEMP)
					{
						ereport(WARNING,(errmsg("GLOBAL is deprecated in temporary table creation"),
							parser_errposition(@1)));
						$7->relpersistence = RELPERSISTENCE_TEMP;
					}

					n->sequence = $7;
					n->options = $8;
					n->ownerId = InvalidOid;
/* PGXC_BEGIN */
					n->is_serial = false;
/* PGXC_END */
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
		;

AlterSeqStmt:
			ALTER SEQUENCE qualified_name SeqOptList
				{
					AlterSeqStmt *n = makeNode(AlterSeqStmt);
					n->sequence = $3;
					n->options = $4;
					n->missing_ok = false;
/* PGXC_BEGIN */
					n->is_serial = false;
/* PGXC_END */
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name SeqOptList
				{
					AlterSeqStmt *n = makeNode(AlterSeqStmt);
					n->sequence = $5;
					n->options = $6;
					n->missing_ok = true;
					$$ = (Node *)n;
				}

		;

OptSeqOptList: SeqOptList							{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

OptParenthesizedSeqOptList: '(' SeqOptList ')'		{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;

SeqOptElem: AS SimpleTypename
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2, @1);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(TRUE), @1);
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE), @1);
				}
			| NOCYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE), @1);
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (Node *)$3, @1);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (Node *)$2, @1);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (Node *)$2, @1);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL, @1);
				}
			| NOMAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL, @1);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| NOMINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3, @1);
				}
			| SEQUENCE NAME_P any_name
				{
					/* not documented, only used by pg_dump */
					$$ = makeDefElem("sequence_name", (Node *)$3, @1);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (Node *)$3, @1);
				}
			| RESTART
				{
					$$ = makeDefElem("restart", NULL, @1);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3, @1);
				}
		;

opt_by:		BY				{}
			| /* empty */	{}
	  ;

NumericOnly:
			FCONST								{ $$ = makeFloat($1); }
			| '+' FCONST						{ $$ = makeFloat($2); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;

NumericOnly_list:	NumericOnly						{ $$ = list_make1($1); }
				| NumericOnly_list ',' NumericOnly	{ $$ = lappend($1, $3); }
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE [OR REPLACE] [TRUSTED] [PROCEDURAL] LANGUAGE ...
 *				DROP [PROCEDURAL] LANGUAGE ...
 *
 *****************************************************************************/

CreatePLangStmt:
			CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE Lower_NonReservedWord_or_Sconst
			{
				CreatePLangStmt *n = makeNode(CreatePLangStmt);
				n->replace = $2;
				n->plname = $6;
				/* parameters are all to be supplied by system */
				n->plhandler = NIL;
				n->plinline = NIL;
				n->plvalidator = NIL;
				n->pltrusted = false;
				$$ = (Node *)n;
			}
			| CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE Lower_NonReservedWord_or_Sconst
			  HANDLER handler_name opt_inline_handler opt_validator
			{
				CreatePLangStmt *n = makeNode(CreatePLangStmt);
				n->replace = $2;
				n->plname = $6;
				n->plhandler = $8;
				n->plinline = $9;
				n->plvalidator = $10;
				n->pltrusted = $3;
				$$ = (Node *)n;
			}
		;

opt_trusted:
			TRUSTED									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* This ought to be just func_name, but that causes reduce/reduce conflicts
 * (CREATE LANGUAGE is the only place where func_name isn't followed by '(').
 * Work around by using simple names, instead.
 */
handler_name:
			name						{ $$ = list_make1(makeString($1)); }
			| name attrs				{ $$ = lcons(makeString($1), $2); }
		;

opt_inline_handler:
			INLINE_P handler_name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

validator_clause:
			VALIDATOR handler_name					{ $$ = $2; }
			| NO VALIDATOR							{ $$ = NIL; }
		;

opt_validator:
			validator_clause						{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

DropPLangStmt:
			DROP opt_procedural LANGUAGE Lower_NonReservedWord_or_Sconst opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_LANGUAGE;
					n->objects = list_make1(makeString($4));
					n->behavior = $5;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP opt_procedural LANGUAGE IF_P EXISTS Lower_NonReservedWord_or_Sconst opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_LANGUAGE;
					n->objects = list_make1(makeString($6));
					n->behavior = $7;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

opt_procedural:
			PROCEDURAL								{}
			| /*EMPTY*/								{}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE TABLESPACE tablespace LOCATION '/path/to/tablespace/'
 *
 *****************************************************************************/

CreateTableSpaceStmt: CREATE TABLESPACE name OptTableSpaceOwner LOCATION Sconst opt_reloptions
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = $4;
					n->location = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
		;

OptTableSpaceOwner: OWNER RoleSpec		{ $$ = $2; }
			| /*EMPTY */				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP TABLESPACE <tablespace>
 *
 *		No need for drop behaviour as we cannot implement dependencies for
 *		objects in other databases; we can only support RESTRICT.
 *
 ****************************************************************************/

DropTableSpaceStmt: DROP TABLESPACE name
				{
					DropTableSpaceStmt *n = makeNode(DropTableSpaceStmt);
					n->tablespacename = $3;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
				|  DROP TABLESPACE IF_P EXISTS name
				{
					DropTableSpaceStmt *n = makeNode(DropTableSpaceStmt);
					n->tablespacename = $5;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             [PREPARE|EXECUTE] CREATE EXTENSION extension
 *             [ WITH ] [ SCHEMA schema ] [ VERSION version ] [ FROM oldversion ]
 *
 *****************************************************************************/

CreateExtensionStmt: CREATE EXTENSION lower_name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $3;
					n->if_not_exists = false;
                    n->action = CREATEEXT_CREATE;
					n->options = $5;
					$$ = (Node *) n;
				}
				| CREATE EXTENSION IF_P NOT EXISTS lower_name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $6;
					n->if_not_exists = true;
                    n->action = CREATEEXT_CREATE;
					n->options = $8;
					$$ = (Node *) n;
				}
                | PREPARE CREATE EXTENSION lower_name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $4;
					n->if_not_exists = false;
					n->action = CREATEEXT_PREPARE;
					n->options = $6;
					$$ = (Node *) n;
				}
				| PREPARE CREATE EXTENSION IF_P NOT EXISTS lower_name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $7;
					n->if_not_exists = true;
					n->action = CREATEEXT_PREPARE;
					n->options = $9;
					$$ = (Node *) n;
				}
				| EXECUTE CREATE EXTENSION lower_name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $4;
					n->if_not_exists = false;
					n->action = CREATEEXT_EXECUTE;
					n->options = $6;
					$$ = (Node *) n;
				}
				| EXECUTE CREATE EXTENSION IF_P NOT EXISTS lower_name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $7;
					n->if_not_exists = true;
					n->action = CREATEEXT_EXECUTE;
					n->options = $9;
					$$ = (Node *) n;
				}
		;

create_extension_opt_list:
			create_extension_opt_list create_extension_opt_item
				{ $$ = lappend($1, $2); }
			| /* EMPTY */
				{ $$ = NIL; }
		;

create_extension_opt_item:
			SCHEMA name
				{
					$$ = makeDefElem("schema", (Node *)makeString($2), @1);
				}
			| VERSION_P Lower_NonReservedWord_or_Sconst
				{
					$$ = makeDefElem("new_version", (Node *)makeString($2), @1);
				}
			| FROM Lower_NonReservedWord_or_Sconst
				{
					$$ = makeDefElem("old_version", (Node *)makeString($2), @1);
				}
			| CASCADE
				{
					$$ = makeDefElem("cascade", (Node *)makeInteger(TRUE), @1);
				}
		;

/*****************************************************************************
 *
 * ALTER EXTENSION name UPDATE [ TO version ]
 *
 *****************************************************************************/

AlterExtensionStmt: ALTER EXTENSION lower_name UPDATE alter_extension_opt_list
				{
					AlterExtensionStmt *n = makeNode(AlterExtensionStmt);
					n->extname = $3;
					n->options = $5;
					$$ = (Node *) n;
				}
		;

alter_extension_opt_list:
			alter_extension_opt_list alter_extension_opt_item
				{ $$ = lappend($1, $2); }
			| /* EMPTY */
				{ $$ = NIL; }
		;

alter_extension_opt_item:
			TO Lower_NonReservedWord_or_Sconst
				{
					$$ = makeDefElem("new_version", (Node *)makeString($2), @1);
				}
		;

/*****************************************************************************
 *
 * ALTER EXTENSION name ADD/DROP object-identifier
 *
 *****************************************************************************/

AlterExtensionContentsStmt:
			ALTER EXTENSION lower_name add_drop ACCESS METHOD lower_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_ACCESS_METHOD;
					n->object = (Node *) makeString($7);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop AGGREGATE aggregate_with_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_AGGREGATE;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop CAST '(' Typename AS Typename ')'
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_CAST;
					n->object = (Node *) list_make2($7, $9);
					$$ = (Node *) n;
				}
			| ALTER EXTENSION lower_name add_drop COLLATION lower_any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_COLLATION;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop CONVERSION_P any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_CONVERSION;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop DOMAIN_P Typename
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_DOMAIN;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop FUNCTION function_with_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FUNCTION;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop opt_procedural LANGUAGE lower_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_LANGUAGE;
					n->object = (Node *) makeString($7);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop OPERATOR operator_with_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_OPERATOR;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop OPERATOR CLASS lower_any_name USING access_method
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_OPCLASS;
					n->object = (Node *) lcons(makeString($9), $7);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop OPERATOR FAMILY lower_any_name USING access_method
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_OPFAMILY;
					n->object = (Node *) lcons(makeString($9), $7);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop PROCEDURE procedure_with_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_PROCEDURE;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop ROUTINE procedure_with_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_ROUTINE;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop SCHEMA name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_SCHEMA;
					n->object = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop EVENT TRIGGER name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_EVENT_TRIGGER;
					n->object = (Node *) makeString($7);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TABLE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TABLE;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TEXT_P SEARCH PARSER lower_any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSPARSER;
					n->object = (Node *) $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TEXT_P SEARCH DICTIONARY lower_any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSDICTIONARY;
					n->object = (Node *) $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TEXT_P SEARCH TEMPLATE lower_any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSTEMPLATE;
					n->object = (Node *) $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TEXT_P SEARCH CONFIGURATION lower_any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSCONFIGURATION;
					n->object = (Node *) $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop SEQUENCE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_SEQUENCE;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop VIEW any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_VIEW;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop MATERIALIZED VIEW any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_MATVIEW;
					n->object = (Node *) $7;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop FOREIGN TABLE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FOREIGN_TABLE;
					n->object = (Node *) $7;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop FOREIGN DATA_P WRAPPER name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FDW;
					n->object = (Node *) makeString($8);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop SERVER name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FOREIGN_SERVER;
					n->object = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TRANSFORM FOR Typename LANGUAGE lower_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TRANSFORM;
					n->object = (Node *) list_make2($7, makeString($9));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name add_drop TYPE_P Typename
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TYPE;
					n->object = (Node *) $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE FOREIGN DATA WRAPPER name options
 *
 *****************************************************************************/

CreateFdwStmt: CREATE FOREIGN DATA_P WRAPPER name opt_fdw_options create_generic_options
				{
					CreateFdwStmt *n = makeNode(CreateFdwStmt);
					n->fdwname = $5;
					n->func_options = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
		;

fdw_option:
			HANDLER handler_name				{ $$ = makeDefElem("handler", (Node *)$2, @1); }
			| NO HANDLER						{ $$ = makeDefElem("handler", NULL, @1); }
			| VALIDATOR handler_name			{ $$ = makeDefElem("validator", (Node *)$2, @1); }
			| NO VALIDATOR						{ $$ = makeDefElem("validator", NULL, @1); }
		;

fdw_options:
			fdw_option							{ $$ = list_make1($1); }
			| fdw_options fdw_option			{ $$ = lappend($1, $2); }
		;

opt_fdw_options:
			fdw_options							{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER FOREIGN DATA WRAPPER name options
 *
 ****************************************************************************/

AlterFdwStmt: ALTER FOREIGN DATA_P WRAPPER name opt_fdw_options alter_generic_options
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);
					n->fdwname = $5;
					n->func_options = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name fdw_options
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);
					n->fdwname = $5;
					n->func_options = $6;
					n->options = NIL;
					$$ = (Node *) n;
				}
		;

/* Options definition for CREATE FDW, SERVER and USER MAPPING */
create_generic_options:
			OPTIONS '(' generic_option_list ')'			{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NIL; }
		;

generic_option_list:
			generic_option_elem
				{
					$$ = list_make1($1);
				}
			| generic_option_list ',' generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

/* Options definition for ALTER FDW, SERVER and USER MAPPING */
alter_generic_options:
			OPTIONS '(' alter_generic_option_list ')'		{ $$ = $3; }
		;

alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = list_make1($1);
				}
			| alter_generic_option_list ',' alter_generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1;
				}
			| SET generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_SET;
				}
			| ADD_P generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_ADD;
				}
			| DROP generic_option_name
				{
					$$ = makeDefElemExtended(NULL, $2, NULL, DEFELEM_DROP, @2);
				}
		;

generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;

generic_option_name:
				Lower_ColLabel			{ $$ = $1; }
		;

/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
				Sconst				{ $$ = (Node *) makeString($1); }
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE SERVER name [TYPE] [VERSION] [OPTIONS]
 *
 *****************************************************************************/

CreateForeignServerStmt: CREATE SERVER name opt_type opt_foreign_server_version
						 FOREIGN DATA_P WRAPPER name create_generic_options
				{
					CreateForeignServerStmt *n = makeNode(CreateForeignServerStmt);
					n->servername = $3;
					n->servertype = $4;
					n->version = $5;
					n->fdwname = $9;
					n->options = $10;
					n->if_not_exists = false;
					$$ = (Node *) n;
				}
				| CREATE SERVER IF_P NOT EXISTS name opt_type opt_foreign_server_version
						 FOREIGN DATA_P WRAPPER name create_generic_options
				{
					CreateForeignServerStmt *n = makeNode(CreateForeignServerStmt);
					n->servername = $6;
					n->servertype = $7;
					n->version = $8;
					n->fdwname = $12;
					n->options = $13;
					n->if_not_exists = true;
					$$ = (Node *) n;
				}
		;

opt_type:
			TYPE_P Sconst			{ $$ = $2; }
			| /*EMPTY*/				{ $$ = NULL; }
		;


foreign_server_version:
			VERSION_P Sconst		{ $$ = $2; }
		|	VERSION_P NULL_P		{ $$ = NULL; }
		;

opt_foreign_server_version:
			foreign_server_version	{ $$ = $1; }
			| /*EMPTY*/				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER SERVER name [VERSION] [OPTIONS]
 *
 ****************************************************************************/

AlterForeignServerStmt: ALTER SERVER name foreign_server_version alter_generic_options
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					n->servername = $3;
					n->version = $4;
					n->options = $5;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER SERVER name foreign_server_version
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					n->servername = $3;
					n->version = $4;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER SERVER name alter_generic_options
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					n->servername = $3;
					n->options = $4;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE FOREIGN TABLE relname (...) SERVER name (...)
 *
 *****************************************************************************/

CreateForeignTableStmt:
		CREATE FOREIGN TABLE qualified_name
			'(' OptTableElementList ')'
			OptInherit SERVER name create_generic_options ForeignTblWritable
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$4->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $4;
					n->base.tableElts = $6;
					n->base.inhRelations = $8;
					n->base.ofTypename = NULL;
					n->base.constraints = NIL;
					n->base.options = NIL;
					n->base.oncommit = ONCOMMIT_NOOP;
					n->base.tablespacename = NULL;
					n->base.if_not_exists = false;
					/* FDW-specific data */
					n->servername = $10;
					n->options = $11;
					n->write_only = $12;
					$$ = (Node *) n;
				}
		| CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name
			'(' OptTableElementList ')'
			OptInherit SERVER name create_generic_options ForeignTblWritable
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$7->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $7;
					n->base.tableElts = $9;
					n->base.inhRelations = $11;
					n->base.ofTypename = NULL;
					n->base.constraints = NIL;
					n->base.options = NIL;
					n->base.oncommit = ONCOMMIT_NOOP;
					n->base.tablespacename = NULL;
					n->base.if_not_exists = true;
					/* FDW-specific data */
					n->servername = $13;
					n->options = $14;
					n->write_only = $15;
					$$ = (Node *) n;
				}
		| CREATE FOREIGN TABLE qualified_name
			PARTITION OF qualified_name OptTypedTableElementList PartitionBoundSpec
			SERVER name create_generic_options ForeignTblWritable
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$4->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $4;
					n->base.inhRelations = list_make1($7);
					n->base.tableElts = $8;
					n->base.partbound = $9;
					n->base.ofTypename = NULL;
					n->base.constraints = NIL;
					n->base.options = NIL;
					n->base.oncommit = ONCOMMIT_NOOP;
					n->base.tablespacename = NULL;
					n->base.if_not_exists = false;
					/* FDW-specific data */
					n->servername = $11;
					n->options = $12;
					n->write_only = $13;
					$$ = (Node *) n;
				}
		| CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name
			PARTITION OF qualified_name OptTypedTableElementList PartitionBoundSpec
			SERVER name create_generic_options ForeignTblWritable
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$7->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $7;
					n->base.inhRelations = list_make1($10);
					n->base.tableElts = $11;
					n->base.partbound = $12;
					n->base.ofTypename = NULL;
					n->base.constraints = NIL;
					n->base.options = NIL;
					n->base.oncommit = ONCOMMIT_NOOP;
					n->base.tablespacename = NULL;
					n->base.if_not_exists = true;
					/* FDW-specific data */
					n->servername = $14;
					n->options = $15;
					n->write_only = $16;
					$$ = (Node *) n;
				}
		;

ForeignTblWritable : WRITE ONLY  { $$ = true; }
									| READ ONLY   { $$ = false; }
									| /* EMPTY */ { $$ = false; }
							;
/*****************************************************************************
 *
 *		QUERY:
 *             ALTER FOREIGN TABLE relname [...]
 *
 *****************************************************************************/

AlterForeignTableStmt:
			ALTER FOREIGN TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_FOREIGN_TABLE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $6;
					n->cmds = $7;
					n->relkind = OBJECT_FOREIGN_TABLE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				IMPORT FOREIGN SCHEMA remote_schema
 *				[ { LIMIT TO | EXCEPT } ( table_list ) ]
 *				FROM SERVER server_name INTO local_schema [ OPTIONS (...) ]
 *
 ****************************************************************************/

ImportForeignSchemaStmt:
		IMPORT_P FOREIGN SCHEMA name import_qualification
		  FROM SERVER name INTO name create_generic_options
			{
				ImportForeignSchemaStmt *n = makeNode(ImportForeignSchemaStmt);
				n->server_name = $8;
				n->remote_schema = $4;
				n->local_schema = $10;
				n->list_type = $5->type;
				n->table_list = $5->table_names;
				n->options = $11;
				$$ = (Node *) n;
			}
		;

import_qualification_type:
		LIMIT TO 				{ $$ = FDW_IMPORT_SCHEMA_LIMIT_TO; }
		| EXCEPT 				{ $$ = FDW_IMPORT_SCHEMA_EXCEPT; }
		;

import_qualification:
		import_qualification_type '(' relation_expr_list ')'
			{
				ImportQual *n = (ImportQual *) palloc(sizeof(ImportQual));
				n->type = $1;
				n->table_names = $3;
				$$ = n;
			}
		| /*EMPTY*/
			{
				ImportQual *n = (ImportQual *) palloc(sizeof(ImportQual));
				n->type = FDW_IMPORT_SCHEMA_ALL;
				n->table_names = NIL;
				$$ = n;
			}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE USER MAPPING FOR auth_ident SERVER name [OPTIONS]
 *
 *****************************************************************************/

CreateUserMappingStmt: CREATE USER MAPPING FOR auth_ident SERVER name create_generic_options
				{
					CreateUserMappingStmt *n = makeNode(CreateUserMappingStmt);
					n->user = $5;
					n->servername = $7;
					n->options = $8;
					n->if_not_exists = false;
					$$ = (Node *) n;
				}
				| CREATE USER MAPPING IF_P NOT EXISTS FOR auth_ident SERVER name create_generic_options
				{
					CreateUserMappingStmt *n = makeNode(CreateUserMappingStmt);
					n->user = $8;
					n->servername = $10;
					n->options = $11;
					n->if_not_exists = true;
					$$ = (Node *) n;
				}
		;

/* User mapping authorization identifier */
auth_ident: RoleSpec			{ $$ = $1; }
			| USER				{ $$ = makeRoleSpec(ROLESPEC_CURRENT_USER, @1); }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP USER MAPPING FOR auth_ident SERVER name
 *
 * XXX you'd think this should have a CASCADE/RESTRICT option, even if it's
 * only pro forma; but the SQL standard doesn't show one.
 ****************************************************************************/

DropUserMappingStmt: DROP USER MAPPING FOR auth_ident SERVER name
				{
					DropUserMappingStmt *n = makeNode(DropUserMappingStmt);
					n->user = $5;
					n->servername = $7;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
				|  DROP USER MAPPING IF_P EXISTS FOR auth_ident SERVER name
				{
					DropUserMappingStmt *n = makeNode(DropUserMappingStmt);
					n->user = $7;
					n->servername = $9;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER USER MAPPING FOR auth_ident SERVER name OPTIONS
 *
 ****************************************************************************/

AlterUserMappingStmt: ALTER USER MAPPING FOR auth_ident SERVER name alter_generic_options
				{
					AlterUserMappingStmt *n = makeNode(AlterUserMappingStmt);
					n->user = $5;
					n->servername = $7;
					n->options = $8;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERIES:
 *				CREATE POLICY name ON table
 *					[AS { PERMISSIVE | RESTRICTIVE } ]
 *					[FOR { SELECT | INSERT | UPDATE | DELETE } ]
 *					[TO role, ...]
 *					[USING (qual)] [WITH CHECK (with check qual)]
 *				ALTER POLICY name ON table [TO role, ...]
 *					[USING (qual)] [WITH CHECK (with check qual)]
 *
 *****************************************************************************/

CreatePolicyStmt:
			CREATE POLICY name ON qualified_name RowSecurityDefaultPermissive
				RowSecurityDefaultForCmd RowSecurityDefaultToRole
				RowSecurityOptionalExpr RowSecurityOptionalWithCheck
				{
					CreatePolicyStmt *n = makeNode(CreatePolicyStmt);
					n->policy_name = $3;
					n->table = $5;
					n->permissive = $6;
					n->cmd_name = $7;
					n->roles = $8;
					n->qual = $9;
					n->with_check = $10;
					$$ = (Node *) n;
				}
		;

AlterPolicyStmt:
			ALTER POLICY name ON qualified_name RowSecurityOptionalToRole
				RowSecurityOptionalExpr RowSecurityOptionalWithCheck
				{
					AlterPolicyStmt *n = makeNode(AlterPolicyStmt);
					n->policy_name = $3;
					n->table = $5;
					n->roles = $6;
					n->qual = $7;
					n->with_check = $8;
					$$ = (Node *) n;
				}
		;

RowSecurityOptionalExpr:
			USING '(' a_expr ')'	{ $$ = $3; }
			| /* EMPTY */			{ $$ = NULL; }
		;

RowSecurityOptionalWithCheck:
			WITH CHECK '(' a_expr ')'		{ $$ = $4; }
			| /* EMPTY */					{ $$ = NULL; }
		;

RowSecurityDefaultToRole:
			TO role_list			{ $$ = $2; }
			| /* EMPTY */			{ $$ = list_make1(makeRoleSpec(ROLESPEC_PUBLIC, -1)); }
		;

RowSecurityOptionalToRole:
			TO role_list			{ $$ = $2; }
			| /* EMPTY */			{ $$ = NULL; }
		;

RowSecurityDefaultPermissive:
			AS Lower_IDENT
				{
					if (strcmp($2, "permissive") == 0)
						$$ = true;
					else if (strcmp($2, "restrictive") == 0)
						$$ = false;
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unrecognized row security option \"%s\"", $2),
								 errhint("Only PERMISSIVE or RESTRICTIVE policies are supported currently."),
									 parser_errposition(@2)));

				}
			| /* EMPTY */			{ $$ = true; }
		;

RowSecurityDefaultForCmd:
			FOR row_security_cmd	{ $$ = $2; }
			| /* EMPTY */			{ $$ = "all"; }
		;

row_security_cmd:
			ALL				{ $$ = "all"; }
		|	SELECT			{ $$ = "select"; }
		|	INSERT			{ $$ = "insert"; }
		|	UPDATE			{ $$ = "update"; }
		|	DELETE_P		{ $$ = "delete"; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE ACCESS METHOD name HANDLER handler_name
 *
 *****************************************************************************/

CreateAmStmt: CREATE ACCESS METHOD lower_name TYPE_P INDEX HANDLER handler_name
				{
					CreateAmStmt *n = makeNode(CreateAmStmt);
					n->amname = $4;
					n->handler_name = $8;
					n->amtype = AMTYPE_INDEX;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE TRIGGER ...
 *
 *****************************************************************************/

CreateTrigStmt:
			CREATE TRIGGER name TriggerActionTime TriggerEvents ON
			qualified_name TriggerReferencing TriggerForSpec TriggerWhen
			EXECUTE FUNCTION_or_PROCEDURE func_name '(' TriggerFuncArgs ')'
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);

					n->trigname = $3;
					n->relation = $7;
					n->funcname = $13;
					n->args = $15;
					n->row = $9;
					n->timing = $4;
					n->events = intVal(linitial($5));
					n->columns = (List *) lsecond($5);
					n->whenClause = $10;
					n->transitionRels = $8;
					n->isconstraint  = FALSE;
					n->deferrable	 = FALSE;
					n->initdeferred  = FALSE;
					n->constrrel = NULL;
					$$ = (Node *)n;				
				}
			| CREATE CONSTRAINT TRIGGER name AFTER TriggerEvents ON
			qualified_name OptConstrFromTable ConstraintAttributeSpec
			FOR EACH ROW TriggerWhen
			EXECUTE FUNCTION_or_PROCEDURE func_name '(' TriggerFuncArgs ')'
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $4;
					n->relation = $8;
					n->funcname = $17;
					n->args = $19;
					n->row = TRUE;
					n->timing = TRIGGER_TYPE_AFTER;
					n->events = intVal(linitial($6));
					n->columns = (List *) lsecond($6);
					n->whenClause = $14;
					n->transitionRels = NIL;
					n->isconstraint  = TRUE;
					processCASbits($10, @10, "TRIGGER",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					n->constrrel = $9;
					$$ = (Node *)n;
				}
		;

TriggerActionTime:
			BEFORE								{ $$ = TRIGGER_TYPE_BEFORE; }
			| AFTER								{ $$ = TRIGGER_TYPE_AFTER; }
			| INSTEAD OF						{ $$ = TRIGGER_TYPE_INSTEAD; }
		;

TriggerEvents:
			TriggerOneEvent
				{ $$ = $1; }
			| TriggerEvents OR TriggerOneEvent
				{
					int		events1 = intVal(linitial($1));
					int		events2 = intVal(linitial($3));
					List   *columns1 = (List *) lsecond($1);
					List   *columns2 = (List *) lsecond($3);

					if (events1 & events2)
						parser_yyerror("duplicate trigger events specified");
					/*
					 * concat'ing the columns lists loses information about
					 * which columns went with which event, but so long as
					 * only UPDATE carries columns and we disallow multiple
					 * UPDATE items, it doesn't matter.  Command execution
					 * should just ignore the columns for non-UPDATE events.
					 */
					$$ = list_make2(makeInteger(events1 | events2),
									list_concat(columns1, columns2));
				}
		;

TriggerOneEvent:
			INSERT
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_INSERT), NIL); }
			| DELETE_P
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_DELETE), NIL); }
			| UPDATE
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_UPDATE), NIL); }
			| UPDATE OF columnList
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_UPDATE), $3); }
			| TRUNCATE
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_TRUNCATE), NIL); }
		;

TriggerReferencing:
			REFERENCING TriggerTransitions			{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

TriggerTransitions:
			TriggerTransition						{ $$ = list_make1($1); }
			| TriggerTransitions TriggerTransition	{ $$ = lappend($1, $2); }
		;

TriggerTransition:
			TransitionOldOrNew TransitionRowOrTable opt_as TransitionRelName
				{
					TriggerTransition *n = makeNode(TriggerTransition);
					n->name = $4;
					n->isNew = $1;
					n->isTable = $2;
					$$ = (Node *)n;
				}
		;

TransitionOldOrNew:
			NEW										{ $$ = TRUE; }
			| OLD									{ $$ = FALSE; }
		;

TransitionRowOrTable:
			TABLE									{ $$ = TRUE; }
			/*
			 * According to the standard, lack of a keyword here implies ROW.
			 * Support for that would require prohibiting ROW entirely here,
			 * reserving the keyword ROW, and/or requiring AS (instead of
			 * allowing it to be optional, as the standard specifies) as the
			 * next token.  Requiring ROW seems cleanest and easiest to
			 * explain.
			 */
			| ROW									{ $$ = FALSE; }
		;

TransitionRelName:
			ColId									{ $$ = $1; }
		;

TriggerForSpec:
			FOR TriggerForOptEach TriggerForType
				{
					$$ = $3;
				}
			| /* EMPTY */
				{
					/*
					 * If ROW/STATEMENT not specified, default to
					 * STATEMENT, per SQL
					 */
					$$ = FALSE;
				}
		;

TriggerForOptEach:
			EACH									{}
			| /*EMPTY*/								{}
		;

TriggerForType:
			ROW										{ $$ = TRUE; }
			| STATEMENT								{ $$ = FALSE; }
		;

TriggerWhen:
			WHEN '(' a_expr ')'						{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

FUNCTION_or_PROCEDURE:
			FUNCTION
		|	PROCEDURE
		;

TriggerFuncArgs:
			TriggerFuncArg							{ $$ = list_make1($1); }
			| TriggerFuncArgs ',' TriggerFuncArg	{ $$ = lappend($1, $3); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

TriggerFuncArg:
			Iconst
				{
					$$ = makeString(psprintf("%d", $1));
				}
			| FCONST								{ $$ = makeString($1); }
			| Sconst								{ $$ = makeString($1); }
			| ColLabel								{ $$ = makeString($1); }
		;

OptConstrFromTable:
			FROM qualified_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ConstraintAttributeSpec:
			/*EMPTY*/
				{ $$ = 0; }
			| ConstraintAttributeSpec ConstraintAttributeElem
				{
					/*
					 * We must complain about conflicting options.
					 * We could, but choose not to, complain about redundant
					 * options (ie, where $2's bit is already set in $1).
					 */
					int		newspec = $1 | $2;

					/* special message for this case */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)) == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 parser_errposition(@2)));
					/* generic message for other conflicts */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE) ||
						(newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)) == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting constraint properties"),
								 parser_errposition(@2)));
					$$ = newspec;
				}
		;

ConstraintAttributeElem:
			NOT DEFERRABLE					{ $$ = CAS_NOT_DEFERRABLE; }
			| DEFERRABLE					{ $$ = CAS_DEFERRABLE; }
			| INITIALLY IMMEDIATE			{ $$ = CAS_INITIALLY_IMMEDIATE; }
			| INITIALLY DEFERRED			{ $$ = CAS_INITIALLY_DEFERRED; }
			| NOT VALID						{ $$ = CAS_NOT_VALID; }
			| NO INHERIT					{ $$ = CAS_NO_INHERIT; }
		;


/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE EVENT TRIGGER ...
 *				ALTER EVENT TRIGGER ...
 *
 *****************************************************************************/

CreateEventTrigStmt:
			CREATE EVENT TRIGGER name ON Lower_ColLabel
			EXECUTE FUNCTION_or_PROCEDURE func_name '(' ')'
				{
					CreateEventTrigStmt *n = makeNode(CreateEventTrigStmt);
					n->trigname = $4;
					n->eventname = $6;
					n->whenclause = NULL;
					n->funcname = $9;
					$$ = (Node *)n;
				}
		  | CREATE EVENT TRIGGER name ON Lower_ColLabel
			WHEN event_trigger_when_list
			EXECUTE FUNCTION_or_PROCEDURE func_name '(' ')'
				{
					CreateEventTrigStmt *n = makeNode(CreateEventTrigStmt);
					n->trigname = $4;
					n->eventname = $6;
					n->whenclause = $8;
					n->funcname = $11;
					$$ = (Node *)n;
				}
		;

event_trigger_when_list:
		  event_trigger_when_item
			{ $$ = list_make1($1); }
		| event_trigger_when_list AND event_trigger_when_item
			{ $$ = lappend($1, $3); }
		;

event_trigger_when_item:
		Lower_ColId IN_P '(' event_trigger_value_list ')'
			{ $$ = makeDefElem($1, (Node *) $4, @1); }
		;

event_trigger_value_list:
		  SCONST
			{ $$ = list_make1(makeString($1)); }
		| event_trigger_value_list ',' SCONST
			{ $$ = lappend($1, makeString($3)); }
		;

AlterEventTrigStmt:
			ALTER EVENT TRIGGER name enable_trigger
				{
					AlterEventTrigStmt *n = makeNode(AlterEventTrigStmt);
					n->trigname = $4;
					n->tgenabled = $5;
					$$ = (Node *) n;
				}
		;

enable_trigger:
			ENABLE_P					{ $$ = TRIGGER_FIRES_ON_ORIGIN; }
			| ENABLE_P REPLICA			{ $$ = TRIGGER_FIRES_ON_REPLICA; }
			| ENABLE_P ALWAYS			{ $$ = TRIGGER_FIRES_ALWAYS; }
			| DISABLE_P					{ $$ = TRIGGER_DISABLED; }
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE ASSERTION ...
 *				DROP ASSERTION ...
 *
 *****************************************************************************/

CreateAssertStmt:
			CREATE ASSERTION name CHECK '(' a_expr ')'
			ConstraintAttributeSpec
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $3;
					n->args = list_make1($6);
					n->isconstraint  = TRUE;
					processCASbits($8, @8, "ASSERTION",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);

					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CREATE ASSERTION is not yet implemented")));

					$$ = (Node *)n;
				}
		;

DropAssertStmt:
			DROP ASSERTION name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = NIL;
					n->behavior = $4;
					n->removeType = OBJECT_TRIGGER; /* XXX */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DROP ASSERTION is not yet implemented")));
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				define (aggregate,operator,type)
 *
 *****************************************************************************/

DefineStmt:
			CREATE AGGREGATE func_name aggr_args definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = $4;
					n->definition = $5;
					$$ = (Node *)n;
				}
			| CREATE AGGREGATE func_name old_aggr_definition
				{
					/* old-style (pre-8.2) syntax for CREATE AGGREGATE */
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = true;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE OPERATOR any_operator definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_OPERATOR;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace opt_editionable TYPE_P typobj_any_name opt_force opt_createtypobj_opt_list definition
				{
					DefineStmt *n = makeNode(DefineStmt);

					if ($2 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'or replace' ident for postgres type"),
									 parser_errposition(@2)));
					}
					if ($6 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'FORCE' ident for postgres type"),
									 parser_errposition(@6)));
					}
					if ($7 != NULL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support attribute definition for postgres type"),
									 parser_errposition(@7)));
					}
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $5;
					n->args = NIL;
					n->definition = $8;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace opt_editionable TYPE_P typobj_any_name opt_force opt_createtypobj_opt_list
				{
					/* Shell type (identified by lack of definition) */
					DefineStmt *n = makeNode(DefineStmt);

					if ($2 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'or replace' ident for postgres type"),
									 parser_errposition(@2)));
					}
					if ($6 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'FORCE' ident for postgres type"),
									 parser_errposition(@6)));
					}
					if ($7 != NULL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support attribute definition for postgres type"),
									 parser_errposition(@7)));
					}
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $5;
					n->args = NIL;
					n->definition = NIL;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace opt_editionable TYPE_P typobj_any_name opt_force opt_createtypobj_opt_list AS '(' OptTableFuncElementList ')'
				{
					CompositeTypeStmt *n = makeNode(CompositeTypeStmt);

					if ($2 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'or replace' ident for postgres type"),
									 parser_errposition(@2)));
					}
					if ($6 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'FORCE' ident for postgres type"),
									 parser_errposition(@6)));
					}
					if ($7 != NULL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support attribute definition for postgres type"),
									 parser_errposition(@7)));
					}
					n->typevar = makeRangeVarFromAnyName($5, @5, yyscanner);
					n->coldeflist = $10;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace opt_editionable TYPE_P typobj_any_name opt_force opt_createtypobj_opt_list AS ENUM_P '(' opt_enum_val_list ')'
				{
					CreateEnumStmt *n = makeNode(CreateEnumStmt);

					if ($2 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'or replace' ident for enum type"),
									 parser_errposition(@2)));
					}
					if ($6 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'FORCE' ident for postgres enum type"),
									 parser_errposition(@6)));
					}
					if ($7 != NULL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support attribute definition for postgres enum type"),
									 parser_errposition(@7)));
					}
					n->typeName = $5;
					n->vals = $11;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace opt_editionable TYPE_P typobj_any_name opt_force opt_createtypobj_opt_list AS RANGE definition
				{
					CreateRangeStmt *n = makeNode(CreateRangeStmt);

					if ($2 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'or replace' ident for record type"),
									 parser_errposition(@2)));
					}
					if ($6 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'FORCE' ident for range type"),
									 parser_errposition(@6)));
					}
					if ($7 != NULL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support attribute definition for range type"),
									 parser_errposition(@7)));
					}
					n->typeName = $5;
					n->params	= $10;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH PARSER lower_any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSPARSER;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH DICTIONARY lower_any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSDICTIONARY;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH TEMPLATE lower_any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSTEMPLATE;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH CONFIGURATION lower_any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSCONFIGURATION;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE COLLATION lower_any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $3;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE COLLATION IF_P NOT EXISTS lower_any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $6;
					n->definition = $7;
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
			| CREATE COLLATION lower_any_name FROM lower_any_name
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $3;
					n->definition = list_make1(makeDefElem("from", (Node *) $5, @5));
					$$ = (Node *)n;
				}
			| CREATE COLLATION IF_P NOT EXISTS lower_any_name FROM lower_any_name
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $6;
					n->definition = list_make1(makeDefElem("from", (Node *) $8, @8));
					n->if_not_exists = true;
					$$ = (Node *)n;
				}
			| CREATE TEMPORARY TYPE_P any_name IS RECORD_P '(' TypeRecordFieldElementList ')'
				{
					CompositeTypeStmt *n = makeNode(CompositeTypeStmt);

					/* can't use qualified_name, sigh */
					n->typevar = makeRangeVarFromAnyName($4, @4, yyscanner);
					n->typevar->relpersistence = RELPERSISTENCE_TEMP;
					n->coldeflist = $8;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace opt_editionable TYPE_P typobj_any_name opt_force opt_createtypobj_opt_list IS RECORD_P '(' TypeRecordFieldElementList ')'
				{
					CompositeTypeStmt *n = makeNode(CompositeTypeStmt);

					if ($2 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'or replace' ident for postgres type"),
									 parser_errposition(@2)));
					}
					if ($6 == TRUE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support 'FORCE' ident for postgres record type"),
									 parser_errposition(@6)));
					}
					if ($7 != NULL)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("not support attribute definition for postgres record type"),
									 parser_errposition(@7)));
					}
					n->typevar = makeRangeVarFromAnyName($5, @5, yyscanner);
					n->coldeflist = $11;
					$$ = (Node *)n;
				}
		;

/***************************opentenbase_ora Type**************************************************/
TypeRecordFieldElementList:
					TypeRecordFieldElement
					{
						$$ = list_make1($1);
					}
					| TypeRecordFieldElementList ',' TypeRecordFieldElement
					{
						$$ = lappend($1, $3);
					}
				;
TypeRecordFieldElement:
				ColId TypeRFname
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collClause = NULL;
					n->collOid = InvalidOid;
					n->constraints = NIL;
					n->location = @1;
					$$ = (Node *)n;
				}
				;
TypeRFname:	TRFSimpleTypename opt_array_bounds
				{
					$$ = $1;
					$$->arrayBounds = $2;
				}
				;
TRFSimpleTypename:
			SimpleTypename	{$$=$1;}
			| TypeRecordFieldAttrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList($1);
					$$->pct_type = true;
					$$->pct_rowtype = false;
					$$->location = @1;
				}
			| TypeRecordFieldAttrs '%' ROWTYPE_P
				{
					$$ = makeTypeNameFromNameList($1);
					$$->pct_type = false;
					$$->pct_rowtype = true;
					$$->location = @1;
				}
			;
TypeRecordFieldAttrs:
			type_function_name attrs	{	$$=lcons(makeString($1), $2);}
			| type_function_name		{ 	$$=list_make1(makeString($1));}
		;

/***************************opentenbase_ora Type**************************************************/

definition: '(' def_list ')'						{ $$ = $2; }
				;

def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;

def_elem:	Lower_ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| Lower_ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;

/* Note: any simple identifier will be returned as a type name! */
def_arg:	func_type						{ $$ = (Node *)$1; }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| ROW							{ $$ = (Node *)makeString(pstrdup("row")); }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
			| NONE							{ $$ = (Node *)makeString(pstrdup($1)); }
		;

old_aggr_definition: '(' old_aggr_list ')'			{ $$ = $2; }
		;

old_aggr_list: old_aggr_elem						{ $$ = list_make1($1); }
			| old_aggr_list ',' old_aggr_elem		{ $$ = lappend($1, $3); }
		;

/*
 * Must use IDENT here to avoid reduce/reduce conflicts; fortunately none of
 * the item names needed in old aggregate definitions are likely to become
 * SQL keywords.
 */
old_aggr_elem:  Lower_IDENT '=' def_arg
				{
					$$ = makeDefElem($1, (Node *)$3, @1);
				}
		;

opt_enum_val_list:
		enum_val_list							{ $$ = $1; }
		| /*EMPTY*/								{ $$ = NIL; }
		;

enum_val_list:	Sconst
				{ $$ = list_make1(makeString($1)); }
			| enum_val_list ',' Sconst
				{ $$ = lappend($1, makeString($3)); }
		;

/*****************************************************************************
 *
 *	ALTER TYPE enumtype ADD ...
 *
 *****************************************************************************/

AlterEnumStmt:
		ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typeName = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = NULL;
				n->newValIsAfter = true;
				n->skipIfNewValExists = $6;
				$$ = (Node *) n;
			}
		 | ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst BEFORE Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typeName = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = $9;
				n->newValIsAfter = false;
				n->skipIfNewValExists = $6;
				$$ = (Node *) n;
			}
		 | ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst AFTER Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typeName = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = $9;
				n->newValIsAfter = true;
				n->skipIfNewValExists = $6;
				$$ = (Node *) n;
			}
		 | ALTER TYPE_P any_name RENAME VALUE_P Sconst TO Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typeName = $3;
				n->oldVal = $6;
				n->newVal = $8;
				n->newValNeighbor = NULL;
				n->newValIsAfter = false;
				n->skipIfNewValExists = false;
				$$ = (Node *) n;
			}
		 ;

opt_if_not_exists: IF_P NOT EXISTS              { $$ = true; }
		| /* empty */                          { $$ = false; }
		;


/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE OPERATOR CLASS ...
 *				CREATE OPERATOR FAMILY ...
 *				ALTER OPERATOR FAMILY ...
 *				DROP OPERATOR CLASS ...
 *				DROP OPERATOR FAMILY ...
 *
 *****************************************************************************/

CreateOpClassStmt:
			CREATE OPERATOR CLASS lower_any_name opt_default FOR TYPE_P Typename
			USING access_method opt_opfamily AS opclass_item_list
				{
					CreateOpClassStmt *n = makeNode(CreateOpClassStmt);
					n->opclassname = $4;
					n->isDefault = $5;
					n->datatype = $8;
					n->amname = $10;
					n->opfamilyname = $11;
					n->items = $13;
					$$ = (Node *) n;
				}
		;

opclass_item_list:
			opclass_item							{ $$ = list_make1($1); }
			| opclass_item_list ',' opclass_item	{ $$ = lappend($1, $3); }
		;

opclass_item:
			OPERATOR Iconst any_operator opclass_purpose opt_recheck
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					ObjectWithArgs *owa = makeNode(ObjectWithArgs);
					owa->objname = $3;
					owa->objargs = NIL;
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->name = owa;
					n->number = $2;
					n->order_family = $4;
					$$ = (Node *) n;
				}
			| OPERATOR Iconst operator_with_argtypes opclass_purpose
			  opt_recheck
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->name = $3;
					n->number = $2;
					n->order_family = $4;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst function_with_argtypes
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->name = $3;
					n->number = $2;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst '(' type_list ')' function_with_argtypes
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->name = $6;
					n->number = $2;
					n->class_args = $4;
					$$ = (Node *) n;
				}
			| STORAGE Typename
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_STORAGETYPE;
					n->storedtype = $2;
					$$ = (Node *) n;
				}
		;

opt_default:	DEFAULT						{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;

opt_opfamily:	FAMILY lower_any_name			{ $$ = $2; }
			| /*EMPTY*/						{ $$ = NIL; }
		;

opclass_purpose: FOR SEARCH					{ $$ = NIL; }
			| FOR ORDER BY lower_any_name		{ $$ = $4; }
			| /*EMPTY*/						{ $$ = NIL; }
		;

opt_recheck:	RECHECK
				{
					/*
					 * RECHECK no longer does anything in opclass definitions,
					 * but we still accept it to ease porting of old database
					 * dumps.
					 */
					ereport(NOTICE,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("RECHECK is no longer required"),
							 errhint("Update your data type."),
							 parser_errposition(@1)));
					$$ = TRUE;
				}
			| /*EMPTY*/						{ $$ = FALSE; }
		;


CreateOpFamilyStmt:
			CREATE OPERATOR FAMILY lower_any_name USING access_method
				{
					CreateOpFamilyStmt *n = makeNode(CreateOpFamilyStmt);
					n->opfamilyname = $4;
					n->amname = $6;
					$$ = (Node *) n;
				}
		;

AlterOpFamilyStmt:
			ALTER OPERATOR FAMILY lower_any_name USING access_method ADD_P opclass_item_list
				{
					AlterOpFamilyStmt *n = makeNode(AlterOpFamilyStmt);
					n->opfamilyname = $4;
					n->amname = $6;
					n->isDrop = false;
					n->items = $8;
					$$ = (Node *) n;
				}
			| ALTER OPERATOR FAMILY lower_any_name USING access_method DROP opclass_drop_list
				{
					AlterOpFamilyStmt *n = makeNode(AlterOpFamilyStmt);
					n->opfamilyname = $4;
					n->amname = $6;
					n->isDrop = true;
					n->items = $8;
					$$ = (Node *) n;
				}
		;

opclass_drop_list:
			opclass_drop							{ $$ = list_make1($1); }
			| opclass_drop_list ',' opclass_drop	{ $$ = lappend($1, $3); }
		;

opclass_drop:
			OPERATOR Iconst '(' type_list ')'
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->number = $2;
					n->class_args = $4;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst '(' type_list ')'
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->number = $2;
					n->class_args = $4;
					$$ = (Node *) n;
				}
		;


DropOpClassStmt:
			DROP OPERATOR CLASS lower_any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1(lcons(makeString($6), $4));
					n->removeType = OBJECT_OPCLASS;
					n->behavior = $7;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP OPERATOR CLASS IF_P EXISTS any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1(lcons(makeString($8), $6));
					n->removeType = OBJECT_OPCLASS;
					n->behavior = $9;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;

DropOpFamilyStmt:
			DROP OPERATOR FAMILY lower_any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1(lcons(makeString($6), $4));
					n->removeType = OBJECT_OPFAMILY;
					n->behavior = $7;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP OPERATOR FAMILY IF_P EXISTS lower_any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1(lcons(makeString($8), $6));
					n->removeType = OBJECT_OPFAMILY;
					n->behavior = $9;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP OWNED BY username [, username ...] [ RESTRICT | CASCADE ]
 *		REASSIGN OWNED BY username [, username ...] TO username
 *
 *****************************************************************************/
DropOwnedStmt:
			DROP OWNED BY role_list opt_drop_behavior
				{
					DropOwnedStmt *n = makeNode(DropOwnedStmt);
					n->roles = $4;
					n->behavior = $5;
					$$ = (Node *)n;
				}
		;

ReassignOwnedStmt:
			REASSIGN OWNED BY role_list TO RoleSpec
				{
					ReassignOwnedStmt *n = makeNode(ReassignOwnedStmt);
					n->roles = $4;
					n->newrole = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP itemtype [ IF EXISTS ] itemname [, itemname ...]
 *           [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

DropStmt:	DROP drop_type_any_name IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_any_name any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP lower_type_any_name IF_P EXISTS lower_any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP lower_type_any_name lower_any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name IF_P EXISTS name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP lower_drop_type_name IF_P EXISTS lower_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP lower_drop_type_name lower_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type_name_on_any_name name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($5, makeString($3)));
					n->behavior = $6;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP drop_type_name_on_any_name IF_P EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP TYPE_P type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TYPE;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP TYPE_P IF_P EXISTS type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TYPE;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP TYPE_P BODY type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_BODY_TYPE;
					n->missing_ok = FALSE;
					n->objects = $4;
					n->behavior = $5;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP DOMAIN_P type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_DOMAIN;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP DOMAIN_P IF_P EXISTS type_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_DOMAIN;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_INDEX;
					n->missing_ok = FALSE;
					n->objects = $4;
					n->behavior = $5;
					n->concurrent = true;
					$$ = (Node *)n;
				}
			| DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_INDEX;
					n->missing_ok = TRUE;
					n->objects = $6;
					n->behavior = $7;
					n->concurrent = true;
					$$ = (Node *)n;
				}
		;

/* object types taking any_name_list */
drop_type_any_name:
			TABLE									{ $$ = OBJECT_TABLE; }
			| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = OBJECT_MATVIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| GLOBAL INDEX                          { $$ = OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGN_TABLE; }
			| EXTERNAL TABLE						{ $$ = OBJECT_FOREIGN_TABLE; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| STATISTICS							{ $$ = OBJECT_STATISTIC_EXT; }
		;

/* object types taking name_list */
drop_type_name:
			EVENT TRIGGER							{ $$ = OBJECT_EVENT_TRIGGER; }
			| FOREIGN DATA_P WRAPPER				{ $$ = OBJECT_FDW; }
			| PUBLICATION							{ $$ = OBJECT_PUBLICATION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| SERVER								{ $$ = OBJECT_FOREIGN_SERVER; }
		;

/* object types attached to a table */
drop_type_name_on_any_name:
			POLICY									{ $$ = OBJECT_POLICY; }
			| RULE									{ $$ = OBJECT_RULE; }
			| TRIGGER								{ $$ = OBJECT_TRIGGER; }
		;

any_name_list:
			any_name								{ $$ = list_make1($1); }
			| any_name_list ',' any_name			{ $$ = lappend($1, $3); }
		;

any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
			| opentenbase_ora_any_ident   {$$ = list_make1(makeString($1));}
			| type_func_name_keyword
				{
					char *upper = upcase_identifier($1, strlen($1));
					$$ = list_make1(makeString(upper));
				}
		;

attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;

type_name_list:
			Typename								{ $$ = list_make1($1); }
			| type_name_list ',' Typename			{ $$ = lappend($1, $3); }

/*****************************************************************************
 *
 *		QUERY:
 *				truncate table relname1, relname2, ...
 *
 *****************************************************************************/

TruncateStmt:
			TRUNCATE opt_shardcluster opt_table relation_expr_list opt_restart_seqs opt_drop_behavior
				{
					TruncateStmt *n = makeNode(TruncateStmt);
					n->paretablename = NULL;
					n->sc_id = $2;
					n->relations = $4;
					n->restart_seqs = $5;
					n->behavior = $6;
					$$ = (Node *)n;
				}
			;

opt_shardcluster:
			SHARDCLUSTER Iconst			{ $$ = $2; }
			| /* EMPTY */				{ $$ = InvalidShardClusterId; }

opt_restart_seqs:
			CONTINUE_P IDENTITY_P		{ $$ = false; }
			| RESTART IDENTITY_P		{ $$ = true; }
			| /* EMPTY */				{ $$ = false; }
		;

/*****************************************************************************
 *
 *	The COMMENT ON statement can take different forms based upon the type of
 *	the object associated with the comment. The form of the statement is:
 *
 *	COMMENT ON [ [ ACCESS METHOD | CONVERSION | COLLATION |
 *                 DATABASE | DOMAIN |
 *                 EXTENSION | EVENT TRIGGER | FOREIGN DATA WRAPPER |
 *                 FOREIGN TABLE | INDEX | [PROCEDURAL] LANGUAGE |
 *                 MATERIALIZED VIEW | POLICY | ROLE | SCHEMA | SEQUENCE |
 *                 SERVER | STATISTICS | TABLE | TABLESPACE |
 *                 TEXT SEARCH CONFIGURATION | TEXT SEARCH DICTIONARY |
 *                 TEXT SEARCH PARSER | TEXT SEARCH TEMPLATE | TYPE |
 *                 VIEW] <objname> |
 *				 AGGREGATE <aggname> (arg1, ...) |
 *				 CAST (<src type> AS <dst type>) |
 *				 COLUMN <relname>.<colname> |
 *				 CONSTRAINT <constraintname> ON <relname> |
 *				 CONSTRAINT <constraintname> ON DOMAIN <domainname> |
 *				 FUNCTION <funcname> (arg1, arg2, ...) |
 *				 LARGE OBJECT <oid> |
 *				 OPERATOR <op> (leftoperand_typ, rightoperand_typ) |
 *				 OPERATOR CLASS <name> USING <access-method> |
 *				 OPERATOR FAMILY <name> USING <access-method> |
 *				 RULE <rulename> ON <relname> |
 *				 TRIGGER <triggername> ON <relname> ]
 *			   IS { 'text' | NULL }
 *
 *****************************************************************************/

CommentStmt:
			COMMENT ON comment_type_any_name any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON lower_type_any_name lower_any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON comment_type_name name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->object = (Node *) makeString($4);
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON lower_comment_type_name lower_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->object = (Node *) makeString($4);
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON TYPE_P Typename IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TYPE;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON DOMAIN_P Typename IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_DOMAIN;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON AGGREGATE aggregate_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_AGGREGATE;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON FUNCTION function_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_FUNCTION;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR operator_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPERATOR;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON CONSTRAINT name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TABCONSTRAINT;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON CONSTRAINT name ON DOMAIN_P any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_DOMCONSTRAINT;
					/*
					 * should use Typename not any_name in the production, but
					 * there's a shift/reduce conflict if we do that, so fix it
					 * up here.
					 */
					n->object = (Node *) list_make2(makeTypeNameFromNameList($7), makeString($4));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON POLICY name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_POLICY;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON PROCEDURE procedure_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_PROCEDURE;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON ROUTINE procedure_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_ROUTINE;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON RULE name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_RULE;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON TRANSFORM FOR Typename LANGUAGE lower_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TRANSFORM;
					n->object = (Node *) list_make2($5, makeString($7));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON TRIGGER name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TRIGGER;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR CLASS lower_any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPCLASS;
					n->object = (Node *) lcons(makeString($7), $5);
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR FAMILY lower_any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPFAMILY;
					n->object = (Node *) lcons(makeString($7), $5);
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON LARGE_P OBJECT_P NumericOnly IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_LARGEOBJECT;
					n->object = (Node *) $5;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON CAST '(' Typename AS Typename ')' IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_CAST;
					n->object = (Node *) list_make2($5, $7);
					n->comment = $10;
					$$ = (Node *) n;
				}
		;

/* object types taking any_name */
comment_type_any_name:
			COLUMN								{ $$ = OBJECT_COLUMN; }
			| INDEX								{ $$ = OBJECT_INDEX; }
			| SEQUENCE							{ $$ = OBJECT_SEQUENCE; }
			| STATISTICS						{ $$ = OBJECT_STATISTIC_EXT; }
			| TABLE								{ $$ = OBJECT_TABLE; }
			| VIEW								{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW					{ $$ = OBJECT_MATVIEW; }
			| CONVERSION_P						{ $$ = OBJECT_CONVERSION; }
			| FOREIGN TABLE						{ $$ = OBJECT_FOREIGN_TABLE; }
		;

/* object types taking name */
comment_type_name:
			DATABASE							{ $$ = OBJECT_DATABASE; }
			| EVENT TRIGGER						{ $$ = OBJECT_EVENT_TRIGGER; }
			| FOREIGN DATA_P WRAPPER			{ $$ = OBJECT_FDW; }
			| PUBLICATION						{ $$ = OBJECT_PUBLICATION; }
			| ROLE								{ $$ = OBJECT_ROLE; }
			| SCHEMA							{ $$ = OBJECT_SCHEMA; }
			| SERVER							{ $$ = OBJECT_FOREIGN_SERVER; }
			| SUBSCRIPTION						{ $$ = OBJECT_SUBSCRIPTION; }
			| TABLESPACE						{ $$ = OBJECT_TABLESPACE; }
		;

comment_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;


/*****************************************************************************
 *
 *  SECURITY LABEL [FOR <provider>] ON <object> IS <label>
 *
 *  As with COMMENT ON, <object> can refer to various types of database
 *  objects (e.g. TABLE, COLUMN, etc.).
 *
 *****************************************************************************/

SecLabelStmt:
			SECURITY LABEL opt_provider ON security_label_type_any_name any_name
			IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = $5;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON security_label_type_name name
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = $5;
					n->object = (Node *) makeString($6);
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON lower_security_label_type_name lower_name
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = $5;
					n->object = (Node *) makeString($6);
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON TYPE_P Typename
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_TYPE;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON DOMAIN_P Typename
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_DOMAIN;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON AGGREGATE aggregate_with_argtypes
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_AGGREGATE;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON FUNCTION function_with_argtypes
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_FUNCTION;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON LARGE_P OBJECT_P NumericOnly
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_LARGEOBJECT;
					n->object = (Node *) $7;
					n->label = $9;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON PROCEDURE procedure_with_argtypes
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_PROCEDURE;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON ROUTINE function_with_argtypes
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_ROUTINE;
					n->object = (Node *) $6;
					n->label = $8;
					$$ = (Node *) n;
				}
		;

opt_provider:	FOR Lower_NonReservedWord_or_Sconst	{ $$ = $2; }
				| /* empty */					{ $$ = NULL; }
		;

/* object types taking any_name */
security_label_type_any_name:
			COLUMN								{ $$ = OBJECT_COLUMN; }
			| FOREIGN TABLE						{ $$ = OBJECT_FOREIGN_TABLE; }
			| SEQUENCE							{ $$ = OBJECT_SEQUENCE; }
			| TABLE								{ $$ = OBJECT_TABLE; }
			| VIEW								{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW					{ $$ = OBJECT_MATVIEW; }
		;

/* object types taking name */
security_label_type_name:
			DATABASE							{ $$ = OBJECT_DATABASE; }
			| EVENT TRIGGER						{ $$ = OBJECT_EVENT_TRIGGER; }
			| PUBLICATION						{ $$ = OBJECT_PUBLICATION; }
			| ROLE								{ $$ = OBJECT_ROLE; }
			| SCHEMA							{ $$ = OBJECT_SCHEMA; }
			| SUBSCRIPTION						{ $$ = OBJECT_SUBSCRIPTION; }
			| TABLESPACE						{ $$ = OBJECT_TABLESPACE; }
		;

security_label:	Sconst				{ $$ = $1; }
				| NULL_P			{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *			fetch/move
 *
 *****************************************************************************/

FetchStmt:	FETCH fetch_args
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->ismove = FALSE;
					$$ = (Node *)n;
				}
			| MOVE fetch_args
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->ismove = TRUE;
					$$ = (Node *)n;
				}
		;

fetch_args:	cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $1;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $2;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| NEXT opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| PRIOR opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FIRST_P opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| LAST_P opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = -1;
					$$ = (Node *)n;
				}
			| ABSOLUTE_P SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| RELATIVE_P SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_RELATIVE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = $1;
					$$ = (Node *)n;
				}
			| ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| FORWARD opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FORWARD SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_FORWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| FORWARD ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| BACKWARD opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| BACKWARD SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_BACKWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| BACKWARD ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_BACKWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
		;

from_in:	FROM									{}
			| IN_P									{}
		;

opt_from_in:	from_in								{}
			| /* EMPTY */							{}
		;


/*****************************************************************************
 *
 * GRANT and REVOKE statements
 *
 *****************************************************************************/

GrantStmt:	GRANT privileges ON privilege_target TO grantee_list
			opt_grant_grant_option
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = true;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->grant_option = $7;
					$$ = (Node*)n;
				}
		;

RevokeStmt:
			REVOKE privileges ON privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = false;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->behavior = $7;
					$$ = (Node *)n;
				}
			| REVOKE GRANT OPTION FOR privileges ON privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = true;
					n->privileges = $5;
					n->targtype = ($7)->targtype;
					n->objtype = ($7)->objtype;
					n->objects = ($7)->objs;
					n->grantees = $9;
					n->behavior = $10;
					$$ = (Node *)n;
				}
		;


/*
 * Privilege names are represented as strings; the validity of the privilege
 * names gets checked at execution.  This is a bit annoying but we have little
 * choice because of the syntactic conflict with lists of role names in
 * GRANT/REVOKE.  What's more, we have to call out in the "privilege"
 * production any reserved keywords that need to be usable as privilege names.
 */

/* either ALL [PRIVILEGES] or a list of individual privileges */
privileges: privilege_list
				{ $$ = $1; }
			| ALL
				{ $$ = NIL; }
			| ALL PRIVILEGES
				{ $$ = NIL; }
			| ALL '(' columnList ')'
				{
					AccessPriv *n = makeNode(AccessPriv);
					n->priv_name = NULL;
					n->cols = $3;
					$$ = list_make1(n);
				}
			| ALL PRIVILEGES '(' columnList ')'
				{
					AccessPriv *n = makeNode(AccessPriv);
					n->priv_name = NULL;
					n->cols = $4;
					$$ = list_make1(n);
				}
		;

privilege_list:	privilege							{ $$ = list_make1($1); }
			| privilege_list ',' privilege			{ $$ = lappend($1, $3); }
		;

privilege:	SELECT opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| REFERENCES opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| CREATE opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| DELETE_P opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| Lower_ColId opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = $1;
				n->cols = $2;
				$$ = n;
			}
		| CONNECT opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		;


/* Don't bother trying to fold the first two rules into one using
 * opt_table.  You're going to get conflicts.
 */
privilege_target:
			qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $1;
					$$ = n;
				}
			| TABLE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $2;
					$$ = n;
				}
			| SEQUENCE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_SEQUENCE;
					n->objs = $2;
					$$ = n;
				}
			| FOREIGN DATA_P WRAPPER name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_FDW;
					n->objs = $4;
					$$ = n;
				}
			| FOREIGN SERVER name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_FOREIGN_SERVER;
					n->objs = $3;
					$$ = n;
				}
			| FUNCTION function_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_FUNCTION;
					n->objs = $2;
					$$ = n;
				}
			| PROCEDURE procedure_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_PROCEDURE;
					n->objs = $2;
					$$ = n;
				}
			| ROUTINE procedure_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_ROUTINE;
					n->objs = $2;
					$$ = n;
				}
			| DATABASE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_DATABASE;
					n->objs = $2;
					$$ = n;
				}
			| DOMAIN_P any_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_DOMAIN;
					n->objs = $2;
					$$ = n;
				}
			| LANGUAGE lower_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_LANGUAGE;
					n->objs = $2;
					$$ = n;
				}
			| LARGE_P OBJECT_P NumericOnly_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_LARGEOBJECT;
					n->objs = $3;
					$$ = n;
				}
			| SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_NAMESPACE;
					n->objs = $2;
					$$ = n;
				}
			| TABLESPACE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_TABLESPACE;
					n->objs = $2;
					$$ = n;
				}
			| TYPE_P any_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_TYPE;
					n->objs = $2;
					$$ = n;
				}
/* OPENTENBASE_ORA_BEGIN */
			| PACKAGE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_PACKAGE;
					n->objs = $2;
					$$ = n;
				}
			| ALL PACKAGES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_PACKAGE;
					n->objs = $5;
					$$ = n;
				}
/* OPENTENBASE_ORA_END */

			| ALL TABLES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $5;
					$$ = n;
				}
			| ALL SEQUENCES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_SEQUENCE;
					n->objs = $5;
					$$ = n;
				}
			| ALL FUNCTIONS IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_FUNCTION;
					n->objs = $5;
					$$ = n;
				}
			| ALL PROCEDURES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_PROCEDURE;
					n->objs = $5;
					$$ = n;
				}
			| ALL ROUTINES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_ROUTINE;
					n->objs = $5;
					$$ = n;
				}
		;


grantee_list:
			grantee									{ $$ = list_make1($1); }
			| grantee_list ',' grantee				{ $$ = lappend($1, $3); }
		;

grantee:
			RoleSpec								{ $$ = $1; }
			| GROUP_P RoleSpec						{ $$ = $2; }
		;


opt_grant_grant_option:
			WITH GRANT OPTION { $$ = TRUE; }
			| /*EMPTY*/ { $$ = FALSE; }
		;

/*****************************************************************************
 *
 * GRANT and REVOKE ROLE statements
 *
 *****************************************************************************/

GrantRoleStmt:
			GRANT privilege_list TO role_list opt_grant_admin_option opt_granted_by
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = true;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->admin_opt = $5;
					n->grantor = $6;
					$$ = (Node*)n;
				}
		;

RevokeRoleStmt:
			REVOKE privilege_list FROM role_list opt_granted_by opt_drop_behavior
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = false;
					n->admin_opt = false;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->behavior = $6;
					$$ = (Node*)n;
				}
			| REVOKE ADMIN OPTION FOR privilege_list FROM role_list opt_granted_by opt_drop_behavior
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = false;
					n->admin_opt = true;
					n->granted_roles = $5;
					n->grantee_roles = $7;
					n->behavior = $9;
					$$ = (Node*)n;
				}
		;

opt_grant_admin_option: WITH ADMIN OPTION				{ $$ = TRUE; }
			| /*EMPTY*/									{ $$ = FALSE; }
		;

opt_granted_by: GRANTED BY RoleSpec						{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * ALTER DEFAULT PRIVILEGES statement
 *
 *****************************************************************************/

AlterDefaultPrivilegesStmt:
			ALTER DEFAULT PRIVILEGES DefACLOptionList DefACLAction
				{
					AlterDefaultPrivilegesStmt *n = makeNode(AlterDefaultPrivilegesStmt);
					n->options = $4;
					n->action = (GrantStmt *) $5;
					$$ = (Node*)n;
				}
		;

DefACLOptionList:
			DefACLOptionList DefACLOption			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

DefACLOption:
			IN_P SCHEMA name_list
				{
					$$ = makeDefElem("schemas", (Node *)$3, @1);
				}
			| FOR ROLE role_list
				{
					$$ = makeDefElem("roles", (Node *)$3, @1);
				}
			| FOR USER role_list
				{
					$$ = makeDefElem("roles", (Node *)$3, @1);
				}
		;

/*
 * This should match GRANT/REVOKE, except that individual target objects
 * are not mentioned and we only allow a subset of object types.
 */
DefACLAction:
			GRANT privileges ON defacl_privilege_target TO grantee_list
			opt_grant_grant_option
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = true;
					n->privileges = $2;
					n->targtype = ACL_TARGET_DEFAULTS;
					n->objtype = $4;
					n->objects = NIL;
					n->grantees = $6;
					n->grant_option = $7;
					$$ = (Node*)n;
				}
			| REVOKE privileges ON defacl_privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = false;
					n->privileges = $2;
					n->targtype = ACL_TARGET_DEFAULTS;
					n->objtype = $4;
					n->objects = NIL;
					n->grantees = $6;
					n->behavior = $7;
					$$ = (Node *)n;
				}
			| REVOKE GRANT OPTION FOR privileges ON defacl_privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = true;
					n->privileges = $5;
					n->targtype = ACL_TARGET_DEFAULTS;
					n->objtype = $7;
					n->objects = NIL;
					n->grantees = $9;
					n->behavior = $10;
					$$ = (Node *)n;
				}
		;

defacl_privilege_target:
			TABLES			{ $$ = ACL_OBJECT_RELATION; }
			| FUNCTIONS		{ $$ = ACL_OBJECT_FUNCTION; }
			| ROUTINES		{ $$ = ACL_OBJECT_FUNCTION; }
			| SEQUENCES		{ $$ = ACL_OBJECT_SEQUENCE; }
			| TYPES_P		{ $$ = ACL_OBJECT_TYPE; }
			| SCHEMAS		{ $$ = ACL_OBJECT_NAMESPACE; }
		;


/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 * Note: we cannot put TABLESPACE clause after WHERE clause unless we are
 * willing to make TABLESPACE a fully reserved word.
 *****************************************************************************/

IndexStmt:	CREATE opt_global_clause opt_unique INDEX opt_concurrently opt_index_name
			ON relation_expr access_method_clause '(' index_params ')'
			opt_reloptions OptPhyAttrClauseList where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					ListCell *attr;

					n->cross_node = $2;
					n->unique = $3;
					n->concurrent = $5;
					n->idxname = $6;
					n->relation = $8;
					n->relationId = InvalidOid;
					n->accessMethod = $9;
					n->indexParams = $11;
					n->options = $13;

					foreach(attr, $14)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tableSpace = defGetString(d);
					}

					n->whereClause = $15;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					n->transformed = false;
					n->if_not_exists = false;
					n->reset_default_tblspc = false;
					$$ = (Node *)n;
				}
			| CREATE opt_global_clause opt_unique INDEX opt_concurrently IF_P NOT EXISTS index_name
			ON relation_expr access_method_clause '(' index_params ')'
			opt_reloptions OptPhyAttrClauseList where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					ListCell *attr;

					n->cross_node = $2;
					n->unique = $3;
					n->concurrent = $5;
					n->idxname = $9;
					n->relation = $11;
					n->relationId = InvalidOid;
					n->accessMethod = $12;
					n->indexParams = $14;
					n->options = $16;

					foreach(attr, $17)
					{
						DefElem *d = (DefElem *) lfirst(attr);

						if (pg_strcasecmp(d->defname, "tablespace") == 0)
							n->tableSpace = defGetString(d);
					}

					n->whereClause = $18;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					n->transformed = false;
					n->if_not_exists = true;
					n->reset_default_tblspc = false;
					$$ = (Node *)n;
				}
		;

opt_unique:
			UNIQUE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_concurrently:
			CONCURRENTLY							{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_index_name:
			index_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

access_method_clause:
			USING access_method						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = DEFAULT_INDEX_TYPE; }
		;

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }
		;

/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem:	ColId opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| opentenbase_ora_colref_ident opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| func_expr_windowless opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->indexcolname = NULL;
					$$->collation = $4;
					$$->opclass = $5;
					$$->ordering = $6;
					$$->nulls_ordering = $7;
				}
		;

opt_collate: COLLATE lower_any_name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_class:	lower_any_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_asc_desc: ASC							{ $$ = SORTBY_ASC; }
			| DESC							{ $$ = SORTBY_DESC; }
			| /*EMPTY*/						{ $$ = SORTBY_DEFAULT; }
		;

opt_nulls_order: NULLS_LA FIRST_P			{ $$ = SORTBY_NULLS_FIRST; }
			| NULLS_LA LAST_P				{ $$ = SORTBY_NULLS_LAST; }
			| /*EMPTY*/						{ $$ = SORTBY_NULLS_DEFAULT; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				create [or replace] function <fname>
 *						[(<type-1> { , <type-n>})]
 *						returns <type-r>
 *						as <filename or code in language as appropriate>
 *						language <lang> [with parameters]
 *
 *****************************************************************************/
CreateFunctionStmt:
			CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			opt_func_return func_return createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = $7;
					n->options = $8;
					n->withClause = $9;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			  opt_func_return TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = mergeTableFuncParameters($5, $9);
					n->returnType = TableFuncTypeName($9);
					n->returnType->location = @7;
					n->options = $11;
					n->withClause = $12;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			  createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->options = $6;
					n->withClause = $7;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace PROCEDURE func_name func_args_with_defaults
			  createfunc_opt_list
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->is_procedure = true;
					n->options = $6;
					$$ = (Node *)n;
				}
/* BEGIN_OPENTENBASE_ORA: func_name_no_parens + unreserved_keyword is same as func_name */
			| CREATE opt_or_replace PROCEDURE func_name_no_parens createfunc_opt_list
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = NIL;
					n->returnType = NULL;
					n->is_procedure = true;
					n->options = $5;
					$$ = (Node *)n;
				}
				/* For function */
				| CREATE opt_or_replace FUNCTION func_name_no_parens
					opt_func_return func_return createfunc_opt_list opt_definition
					{
						CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
						n->replace = $2;
						n->funcname = $4;
						n->parameters = NULL;
						n->returnType = $6;
						n->options = $7;
						n->withClause = $8;
						$$ = (Node *)n;
					}
				| CREATE opt_or_replace FUNCTION func_name_no_parens
					opt_func_return TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
					{
						CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
						n->replace = $2;
						n->funcname = $4;
						n->parameters = mergeTableFuncParameters($4, $8);
						n->returnType = TableFuncTypeName($8);
						n->returnType->location = @6;
						n->options = $10;
						n->withClause = $11;
						$$ = (Node *)n;
					}
				| CREATE opt_or_replace FUNCTION func_name_no_parens
					createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->replace = $2;
					n->funcname = $4;
					n->parameters = NULL;
					n->returnType = NULL;
					n->options = $5;
					n->withClause = $6;
					$$ = (Node *)n;
				}
		;

opt_func_return:
			RETURN          { $$ = 0; }
			| RETURNS       { $$ = 0; }
			;

opt_or_replace:
			OR REPLACE								{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

func_args:	'(' func_args_list ')'					{ $$ = $2; }
			| '(' ')'								{ $$ = NIL; }
		;

func_args_list:
			func_arg								{ $$ = list_make1($1); }
			| func_args_list ',' func_arg			{ $$ = lappend($1, $3); }
		;

function_with_argtypes_list:
			function_with_argtypes					{ $$ = list_make1($1); }
			| function_with_argtypes_list ',' function_with_argtypes
													{ $$ = lappend($1, $3); }
		;

procedure_with_argtypes_list:
			procedure_with_argtypes					{ $$ = list_make1($1); }
			| procedure_with_argtypes_list ',' procedure_with_argtypes
													{ $$ = lappend($1, $3); }
		;

function_with_argtypes:
			func_name func_args
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = extractArgTypes(OBJECT_FUNCTION, $2);
					$$ = n;
				}
			| function_with_argtypes_common
				{
					$$ = $1;
				}
		;

function_with_argtypes_common:
			/*
			 * Because of reduce/reduce conflicts, we can't use func_name
			 * below, but we can write it out the long way, which actually
			 * allows more cases.
			 */
			type_func_name_keyword
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = list_make1(makeString(upcase_identifier($1, strlen($1))));
					n->args_unspecified = true;
					$$ = n;
				}
			| ColId
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = list_make1(makeString($1));
					n->args_unspecified = true;
					$$ = n;
				}
			| ColId indirection
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = check_func_name(lcons(makeString($1), $2),
												  yyscanner);
					n->args_unspecified = true;
					$$ = n;
				}
		;

/*
 * This is different from function_with_argtypes in the call to
 * extractArgTypes().
 */
procedure_with_argtypes:
			func_name func_args
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = extractArgTypes(OBJECT_PROCEDURE, $2);
					$$ = n;
				}
			| function_with_argtypes_common
				{
					$$ = $1;
				}
		;

/*
 * func_args_with_defaults is separate because we only want to accept
 * defaults in CREATE FUNCTION, not in ALTER etc.
 */
func_args_with_defaults:
		'(' func_args_with_defaults_list ')'		{ $$ = $2; }
		| '(' ')'									{ $$ = NIL; }
		;

func_args_with_defaults_list:
		func_arg_with_default						{ $$ = list_make1($1); }
		| func_args_with_defaults_list ',' func_arg_with_default
													{ $$ = lappend($1, $3); }
		;

/*
 * The style with arg_class first is SQL99 standard, but opentenbase_ora puts
 * param_name first; accept both since it's likely people will try both
 * anyway.  Don't bother trying to save productions by letting arg_class
 * have an empty alternative ... you'll get shift/reduce conflicts.
 *
 * We can catch over-specified arguments here if we want to,
 * but for now better to silently swallow typmod, etc.
 * - thomas 2000-03-22
 */
func_arg:
			arg_class param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $2;
					n->argType = $3;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $3;
					n->mode = $2;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_IN;
					n->defexpr = NULL;
					$$ = n;
				}
			| arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $2;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $1;
					n->mode = FUNC_PARAM_IN;
					n->defexpr = NULL;
					$$ = n;
				}
		;

/* INOUT is SQL99 standard, IN OUT is for opentenbase_ora compatibility */
arg_class:	IN_P								{ $$ = FUNC_PARAM_IN; }
			| OUT_P								{ $$ = FUNC_PARAM_OUT; }
			| INOUT								{ $$ = FUNC_PARAM_INOUT; }
			| IN_P OUT_P						{ $$ = FUNC_PARAM_INOUT; }
			| VARIADIC							{ $$ = FUNC_PARAM_VARIADIC; }
		;

/*
 * Ideally param_name should be ColId, but that causes too many conflicts.
 */
param_name:	type_function_name
		  ;

func_return:
			func_type
				{
					/* We can catch over-specified results here if we want to,
					 * but for now better to silently swallow typmod, etc.
					 * - thomas 2000-03-22
					 */
					$$ = $1;
				}
		;

/*
 * We would like to make the %TYPE productions here be ColId attrs etc,
 * but that causes reduce/reduce conflicts.  type_function_name
 * is next best choice.
 */
func_type:	Typename								{ $$ = $1; }
			| type_function_name '%' ROWTYPE_P
				{ 
					$$ = makeTypeNameFromNameList(list_make1(makeString($1)));
					$$->pct_type = false;
					$$->pct_rowtype = true;
					$$->location = @1;
				}
			| type_function_name '%' TYPE_P
				{ 
					$$ = makeTypeNameFromNameList(list_make1(makeString($1)));
					$$->pct_type = true;
					$$->pct_rowtype = false;
					$$->location = @1;
				}
			| type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = true;
					$$->pct_rowtype = false;
					$$->location = @1;
				}
			| type_function_name attrs '%' ROWTYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = false;
					$$->pct_rowtype = true;
					$$->location = @1;
				}
			| SETOF type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($2), $3));
					$$->pct_type = true;
					$$->pct_rowtype = false;
					$$->setof = TRUE;
					$$->location = @2;
				}
		;

func_arg_with_default:
		func_arg
				{
					$$ = $1;
				}
		| func_arg DEFAULT a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg '=' a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg COLON_EQUALS a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		;

/* Aggregate args can be most things that function args can be */
aggr_arg:	func_arg
				{
					if (!($1->mode == FUNC_PARAM_IN ||
						  $1->mode == FUNC_PARAM_VARIADIC))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("aggregates cannot have output arguments"),
								 parser_errposition(@1)));
					$$ = $1;
				}
		;

/*
 * The SQL standard offers no guidance on how to declare aggregate argument
 * lists, since it doesn't have CREATE AGGREGATE etc.  We accept these cases:
 *
 * (*)									- normal agg with no args
 * (aggr_arg,...)						- normal agg with args
 * (ORDER BY aggr_arg,...)				- ordered-set agg with no direct args
 * (aggr_arg,... ORDER BY aggr_arg,...)	- ordered-set agg with direct args
 *
 * The zero-argument case is spelled with '*' for consistency with COUNT(*).
 *
 * An additional restriction is that if the direct-args list ends in a
 * VARIADIC item, the ordered-args list must contain exactly one item that
 * is also VARIADIC with the same type.  This allows us to collapse the two
 * VARIADIC items into one, which is necessary to represent the aggregate in
 * pg_proc.  We check this at the grammar stage so that we can return a list
 * in which the second VARIADIC item is already discarded, avoiding extra work
 * in cases such as DROP AGGREGATE.
 *
 * The return value of this production is a two-element list, in which the
 * first item is a sublist of FunctionParameter nodes (with any duplicate
 * VARIADIC item already dropped, as per above) and the second is an integer
 * Value node, containing -1 if there was no ORDER BY and otherwise the number
 * of argument declarations before the ORDER BY.  (If this number is equal
 * to the first sublist's length, then we dropped a duplicate VARIADIC item.)
 * This representation is passed as-is to CREATE AGGREGATE; for operations
 * on existing aggregates, we can just apply extractArgTypes to the first
 * sublist.
 */
aggr_args:	'(' '*' ')'
				{
					$$ = list_make2(NIL, makeInteger(-1));
				}
			| '(' aggr_args_list ')'
				{
					$$ = list_make2($2, makeInteger(-1));
				}
			| '(' ORDER BY aggr_args_list ')'
				{
					$$ = list_make2($4, makeInteger(0));
				}
			| '(' aggr_args_list ORDER BY aggr_args_list ')'
				{
					/* this is the only case requiring consistency checking */
					$$ = makeOrderedSetArgs($2, $5, yyscanner);
				}
		;

aggr_args_list:
			aggr_arg								{ $$ = list_make1($1); }
			| aggr_args_list ',' aggr_arg			{ $$ = lappend($1, $3); }
		;

aggregate_with_argtypes:
			func_name aggr_args
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = extractAggrArgTypes($2);
					$$ = n;
				}
		;

aggregate_with_argtypes_list:
			aggregate_with_argtypes					{ $$ = list_make1($1); }
			| aggregate_with_argtypes_list ',' aggregate_with_argtypes
													{ $$ = lappend($1, $3); }
		;

createfunc_opt_list:
			/* Must be at least one to prevent conflict */
			createfunc_opt_item						{ $$ = list_make1($1); }
			| createfunc_opt_list createfunc_opt_item { $$ = lappend($1, $2); }
	;

/*
 * Options common to both CREATE FUNCTION and ALTER FUNCTION
 */
common_func_opt_item:
			CALLED ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(FALSE), @1);
				}
			| RETURNS NULL_P ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(TRUE), @1);
				}
			| STRICT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(TRUE), @1);
				}
			| DETERMINISTIC
				{
					$$ = makeDefElem("volatility", (Node *)makeString("immutable"), @1);
				}
			| IMMUTABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("immutable"), @1);
				}
			| STABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("stable"), @1);
				}
			| RESULT_CACHE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("result_cache"), @1);
				}
			| VOLATILE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("volatile"), @1);
				}
			| EXTERNAL SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE), @1);
				}
			| EXTERNAL SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE), @1);
				}
			| SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE), @1);
				}
			| SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE), @1);
				}
			| LEAKPROOF
				{
					$$ = makeDefElem("leakproof", (Node *)makeInteger(TRUE), @1);
				}
			| NOT LEAKPROOF
				{
					$$ = makeDefElem("leakproof", (Node *)makeInteger(FALSE), @1);
				}
			| PUSHDOWN
				{
					if (!IS_CENTRALIZED_DATANODE)
						ereport(WARNING,
								(errcode(ERRCODE_WARNING),
									errmsg("You are pushing function down to datanode for execution"),
									errhint("Make sure it's not attempt to access any other datanode "
											"OR YOU MAY SUFFER DATA LOSE !!")));

					$$ = makeDefElem("pushdown", (Node *)makeInteger(TRUE), @1);
				}
			| NOT PUSHDOWN
				{
					$$ = makeDefElem("pushdown", (Node *)makeInteger(FALSE), @1);
				}
			| COST NumericOnly
				{
					$$ = makeDefElem("cost", (Node *)$2, @1);
				}
			| ROWS NumericOnly
				{
					$$ = makeDefElem("rows", (Node *)$2, @1);
				}
			| FunctionSetResetClause
				{
					/* we abuse the normal content of a DefElem here */
					$$ = makeDefElem("set", (Node *)$1, @1);
				}
			| PARALLEL Lower_ColId
				{
					$$ = makeDefElem("parallel", (Node *)makeString($2), @1);
				}
			/* OPENTENBASE_ORA_BEGIN */
			| AUTHID CURRENT_USER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE), @1);
				}
			| AUTHID DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE), @1);
				}
			| PIPELINED
				{
					$$ = makeDefElem("pipelined", (Node *)makeInteger(TRUE), @1);
				}
			/* OPENTENBASE_ORA_END */
		;

createfunc_opt_item:
			AS func_as
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| IS func_as
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| LANGUAGE Lower_NonReservedWord_or_Sconst
				{
					$$ = makeDefElem("language", (Node *)makeString($2), @1);
				}
			| TRANSFORM transform_type_list
				{
					$$ = makeDefElem("transform", (Node *)$2, @1);
				}
			| WINDOW
				{
					$$ = makeDefElem("window", (Node *)makeInteger(TRUE), @1);
				}
			| common_func_opt_item
				{
					$$ = $1;
				}
		;

func_as:	Sconst						{ $$ = list_make1(makeString($1)); }
			| Sconst ',' Sconst
				{
					$$ = list_make2(makeString($1), makeString($3));
				}
		;

transform_type_list:
			FOR TYPE_P Typename { $$ = list_make1($3); }
			| transform_type_list ',' FOR TYPE_P Typename { $$ = lappend($1, $5); }
		;

opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

table_func_column:	param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_TABLE;
					n->defexpr = NULL;
					$$ = n;
				}
		;

table_func_column_list:
			table_func_column
				{
					$$ = list_make1($1);
				}
			| table_func_column_list ',' table_func_column
				{
					$$ = lappend($1, $3);
				}
		;

/*****************************************************************************
 * ALTER FUNCTION / ALTER PROCEDURE / ALTER ROUTINE
 *
 * RENAME and OWNER subcommands are already provided by the generic
 * ALTER infrastructure, here we just specify alterations that can
 * only be applied to functions.
 *
 *****************************************************************************/
AlterFunctionStmt:
			ALTER FUNCTION function_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->objtype = OBJECT_FUNCTION;
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
			| ALTER PROCEDURE procedure_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->objtype = OBJECT_PROCEDURE;
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
			| ALTER ROUTINE procedure_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->objtype = OBJECT_ROUTINE;
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
		;

alterfunc_opt_list:
			/* At least one option must be specified */
			common_func_opt_item					{ $$ = list_make1($1); }
			| alterfunc_opt_list common_func_opt_item { $$ = lappend($1, $2); }
		;

/* Ignored, merely for SQL compliance */
opt_restrict:
			RESTRICT
			| /* EMPTY */
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP FUNCTION funcname (arg1, arg2, ...) [ RESTRICT | CASCADE ]
 *		DROP PROCEDURE procname (arg1, arg2, ...) [ RESTRICT | CASCADE ]
 *		DROP ROUTINE routname (arg1, arg2, ...) [ RESTRICT | CASCADE ]
 *		DROP AGGREGATE aggname (arg1, ...) [ RESTRICT | CASCADE ]
 *		DROP OPERATOR opname (leftoperand_typ, rightoperand_typ) [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

RemoveFuncStmt:
			DROP FUNCTION function_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP FUNCTION IF_P EXISTS function_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP PROCEDURE procedure_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_PROCEDURE;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP PROCEDURE IF_P EXISTS procedure_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_PROCEDURE;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP ROUTINE procedure_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_ROUTINE;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP ROUTINE IF_P EXISTS procedure_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_ROUTINE;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

RemoveAggrStmt:
			DROP AGGREGATE aggregate_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_AGGREGATE;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP AGGREGATE IF_P EXISTS aggregate_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_AGGREGATE;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

RemoveOperStmt:
			DROP OPERATOR operator_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_OPERATOR;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP OPERATOR IF_P EXISTS operator_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_OPERATOR;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

oper_argtypes:
			'(' Typename ')'
				{
				   ereport(ERROR,
						   (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("missing argument"),
							errhint("Use NONE to denote the missing argument of a unary operator."),
							parser_errposition(@3)));
				}
			| '(' Typename ',' Typename ')'
					{ $$ = list_make2($2, $4); }
			| '(' NONE ',' Typename ')'					/* left unary */
					{ $$ = list_make2(NULL, $4); }
			| '(' Typename ',' NONE ')'					/* right unary */
					{ $$ = list_make2($2, NULL); }
		;

any_operator:
			CONCATENATION
					{ $$ = list_make1(makeString("||")); }
			| all_Op
					{ $$ = list_make1(makeString($1)); }
			| ColId '.' any_operator
					{ $$ = lcons(makeString($1), $3); }
		;

operator_with_argtypes_list:
			operator_with_argtypes					{ $$ = list_make1($1); }
			| operator_with_argtypes_list ',' operator_with_argtypes
													{ $$ = lappend($1, $3); }
		;

operator_with_argtypes:
			any_operator oper_argtypes
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = $2;
					$$ = n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *				create [or replace] package <pname>
 *
 *****************************************************************************/

/*
 * END [ pkg_name ] is parsed in get_plpgsql_body(). So the gram rule need
 * not list END keyword.
 */
CreatePackageStmt:
			CREATE opt_or_replace opt_editionable PACKAGE pkg_qualified_name
				createpkg_opt_list
					{
						CreatePackageStmt *n = makeNode(CreatePackageStmt);

						n->replace = $2;
						n->edition = $3;
						n->pkgname = $5;
						n->options = $6;
						n->func_spec = pg_yyget_extra(yyscanner)->func_spec;
						n->reparse_spec = !pg_yyget_extra(yyscanner)->try_ora_plsql;

						$$ = (Node *)n;
					}
			| CREATE opt_or_replace opt_editionable PACKAGE BODY pkg_qualified_name
				createpkg_opt_list
				    {
						CreatePackageBodyStmt *n = makeNode(CreatePackageBodyStmt);
						ListCell	*lc;
						char	*body = NULL;
						base_yy_extra_type	*yyextra = pg_yyget_extra(yyscanner);

						n->replace = $2;
						n->edition = $3;
						n->pkgname = $6;
						n->options = $7;
						foreach(lc, n->options)
						{
							DefElem	*ele = (DefElem *) lfirst(lc);

							if (strcmp(ele->defname, "as") == 0)
							{
								body = strVal((Value *) linitial(castNode(List, ele->arg)));
								break;
							}
						}

						if (lc == NULL)
							elog(ERROR, "empty package body");

						foreach(lc, n->options)
						{
							DefElem	*ele = (DefElem *) lfirst(lc);

							if (strcmp(ele->defname, "authid") == 0)
								elog(ERROR, "unexpected AUTHID in PACKAGE body");
						}

						n->reparse_body = !yyextra->try_ora_plsql;
						if (yyextra->try_ora_plsql)
							extract_package_body_defs(n, yyextra, body);

						$$ = (Node *)n;
				    }
		;

opt_debug:
		 DEBUG			{ $$ = true; }
		 |				{ $$ = false; }
		;

opt_pkg_comp:
			PACKAGE		{ $$ = AlterPkgOptPackage; }
			| SPECIFICATION	{ $$ = AlterPkgOptSpec; }
			| BODY			{ $$ = AlterPkgOptBody; }
			|				{ $$ = AlterPkgOptPackage; /* As opentenbase_ora default */ }
		;
opt_reuse:
			REUSE SETTINGS	{ $$ = true; }
			|				{ $$ = false; }
		;

compiler_item:
		IDENT '=' AexprConst
			{
				$$ = makeDefElem((char *) $1, (Node *) $3, @1);
			}
		| IDENT '=' IDENT
			{
				$$ = makeDefElem((char *) $1, (Node *) $3, @1);
			}
		;

compiler_items:
				compiler_item
				{
					$$ = list_make1($1);
				}
			| compiler_items compiler_item
				{
					$$ = lappend($1, $2);
				}
		;

AlterPackageStmt:
			ALTER PACKAGE pkg_qualified_name COMPILE opt_debug opt_pkg_comp
				compiler_items opt_reuse
				{
					AlterPackageStmt	*alt_pkg = makeNode(AlterPackageStmt);

					alt_pkg->pkgname = $3;
					alt_pkg->debug = $5;
					alt_pkg->pkg_part = $6;
					alt_pkg->comp_items = $7;
					alt_pkg->reuse_settings = $8;

					$$ = (Node *) alt_pkg;
				}
			| ALTER PACKAGE pkg_qualified_name COMPILE opt_debug opt_pkg_comp
				opt_reuse
				{
					AlterPackageStmt	*alt_pkg = makeNode(AlterPackageStmt);

					alt_pkg->pkgname = $3;
					alt_pkg->debug = $5;
					alt_pkg->pkg_part = $6;
					alt_pkg->comp_items = NULL;
					alt_pkg->reuse_settings = $7;

					$$ = (Node *) alt_pkg;
				}
			| ALTER PACKAGE pkg_qualified_name opt_editionable
				{
					AlterPackageStmt	*alt_pkg = makeNode(AlterPackageStmt);

					alt_pkg->pkgname = $3;
					alt_pkg->debug = false;
					alt_pkg->pkg_part = AlterPkgOptPackage;
					alt_pkg->comp_items = NULL;
					alt_pkg->reuse_settings = false;
					alt_pkg->edition = $4;

					$$ = (Node *) alt_pkg;
				}
			;

opt_editionable:
			EDITIONABLE								{ $$ = TRUE; }
			| NONEDITIONABLE							{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

typobj_any_name:
			TypObjId					{ $$ = list_make1(makeString($1)); }
			| TypObjId attrs				{ $$ = lcons(makeString($1), $2); }

createtypobj_opt_list:
			typpkg_opt_item								{ $$ = list_make1($1); }
			| createtypobj_opt_list typpkg_opt_item			{ $$ = lappend($1, $2); }
			;

opt_createtypobj_opt_list:
			/* empty */ 					{ $$ = NIL; }
			| createtypobj_opt_list			{ $$ = $1; }

createpkg_opt_list:
			/* Must be at least one to prevent conflict */
			createpkg_opt_item						{ $$ = list_make1($1); }
			| createpkg_opt_list createpkg_opt_item { $$ = lappend($1, $2); }
	;

typpkg_opt_item:
			SHARING ColId
				{
					$$ = makeDefElem("sharing", (Node *)makeString($2), @1);
				}
			| DEFAULT COLLATION USING_NLS_COMP
				{
					$$ = makeDefElem("defcollation", (Node *)makeInteger(TRUE), @1);
				}
			| AUTHID CURRENT_USER
				{
					$$ = makeDefElem("authid", (Node *) makeString("current_user"), @1);
				}
			| AUTHID DEFINER
				{
					$$ = makeDefElem("authid", (Node *) makeString("definer"), @1);
				}
			| ACCESSIBLE BY accessible_args
				{
					$$ = makeDefElem("accessible", (Node *)$2, @1);
				}
				;

createpkg_opt_item:
			typpkg_opt_item { $$ = $1; }
			| IS pkg_as
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| AS pkg_as
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
		;

pkg_as:	Sconst						{ $$ = list_make1(makeString($1)); }
		;


accessible_args:	'(' accessible_args_list ')'					{ $$ = $2; }
			| '(' ')'							{ $$ = NIL; }
		;

accessible_args_list:
			accessible_arg							{ $$ = list_make1($1); }
			| accessible_args_list ',' accessible_arg			{ $$ = lappend($1, $3); }
		;

accessible_arg:
			accessible_class name
				{
					AccessibleParameter *n = makeNode(AccessibleParameter);
					n->kind = $1;
					n->name = $2;
					$$ = n;
				}
			| name
				{
					AccessibleParameter *n = makeNode(AccessibleParameter);
					n->kind = OBJECT_FUNCTION;
					n->name = $1;
					$$ = n;
				}
		;

accessible_class:
			FUNCTION						{ $$ = OBJECT_FUNCTION;  }
			| PROCEDURE						{ $$ = OBJECT_PROCEDURE; }
			| PACKAGE						{ $$ = OBJECT_PACKAGE; }
			| TRIGGER						{ $$ = TRIGGER; }
			| TYPE_P						{ $$ = OBJECT_TYPE; }
		;


DeclareFuncInPkgStmt:
			FUNCTION func_name func_args_with_defaults
			  opt_func_return func_return
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = $3;
					n->returnType = $5;
					$$ = (Node *)n;
				}
			| FUNCTION func_name func_args_with_defaults
			  opt_func_return TABLE '(' table_func_column_list ')'
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = mergeTableFuncParameters($3, $7);
					n->returnType = TableFuncTypeName($7);
					n->returnType->location = @5;
					$$ = (Node *)n;
				}
			| FUNCTION func_name func_args_with_defaults
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = $3;
					n->returnType = NULL;
					$$ = (Node *)n;
				}
			| PROCEDURE func_name func_args_with_defaults
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = $3;
					n->returnType = NULL;
					n->is_procedure = true;
					$$ = (Node *)n;
				}
			/* No parameter */
			| FUNCTION func_name_no_parens opt_func_return func_return
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = NULL;
				n->returnType = $4;
				$$ = (Node *)n;
			}
			| FUNCTION func_name_no_parens
			opt_func_return TABLE '(' table_func_column_list ')'
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = mergeTableFuncParameters(NULL, $6);
				n->returnType = TableFuncTypeName($6);
				n->returnType->location = @5;
				$$ = (Node *)n;
			}
			| FUNCTION func_name_no_parens
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = NULL;
				n->returnType = NULL;
				$$ = (Node *)n;
			}
			| PROCEDURE func_name_no_parens
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = NULL;
				n->returnType = NULL;
				n->is_procedure = true;
				$$ = (Node *)n;
			}
		;

opt_or_public:
			PUBLIC		{ $$ = true; }
			|				{ $$ = false; }

opt_dblink:
			'@' name		{ $$ = $2; }
			|				{ $$ = NULL; }

CreateSynonymStmt:
			CREATE opt_or_replace PUBLIC SYNONYM qualified_name
				FOR
                {
                }
                any_name opt_dblink
                {
                }
				{
					CreateSynonymStmt	*n = makeNode(CreateSynonymStmt);

					n->replace = $2;
					n->ispublic = true;
					n->synname = $5;
					n->objname = $8;
					n->dblink = $9;

					$$ = (Node *) n;
				}
			| CREATE opt_or_replace SYNONYM qualified_name
				FOR
                {
                }
                any_name opt_dblink
                {
                }
				{
					CreateSynonymStmt	*n = makeNode(CreateSynonymStmt);

					n->replace = $2;
					n->ispublic = false;
					n->synname = $4;
					n->objname = $7;
					n->dblink = $8;

					$$ = (Node *) n;
				}
		;

DropSynonymStmt:
			DROP opt_or_public SYNONYM qualified_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_SYNONYM;
					n->missing_ok = FALSE;
					n->objects = list_make1(list_make2($2 ? makeInteger(1) : makeInteger(0), $4));
					n->behavior = $5;
					$$ = (Node *)n;
				}
		;

DeclObjCons:
			CONSTRUCTOR_P FUNCTION func_name func_args_with_defaults RETURN SELF_P AS RESULT_P
				{
					TypeName *typename = makeTypeNameFromNameList(list_copy($3));
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->info = OBJ_FUNC_CONSTRUCTOR;
					n->pkginner = true;
					n->funcname = $3;
					n->parameters = $4;
					n->returnType = typename;
					n->is_procedure = false;
					$$ = (Node *) n;
				}
			| CONSTRUCTOR_P FUNCTION func_name_no_parens RETURN SELF_P AS RESULT_P
				{
					TypeName *typename = makeTypeNameFromNameList(list_copy($3));
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->info = OBJ_FUNC_CONSTRUCTOR;
					n->pkginner = true;
					n->funcname = $3;
					n->parameters = NIL;
					n->returnType = typename;
					n->is_procedure = false;
					$$ = (Node *) n;
				}
			;

ObjFuncDeclElem:
		DeclObjCons
			{
				$$ = $1;
			}
		| MEMBER_P DeclareFuncInPkgStmt
			{
				CreateFunctionStmt *n = (CreateFunctionStmt *) $2;
				n->info = OBJ_FUNC_MEMBER;
				$$ = (Node *) n;
			}
		| STATIC_P DeclareFuncInPkgStmt
			{
				CreateFunctionStmt *n = (CreateFunctionStmt *) $2;
				n->info = OBJ_FUNC_STATIC;
				$$ = (Node *) n;
			}
		;

DeclareFuncInTypeObjStmts:
			ObjFuncDeclElem
				{
					CreateTypeObjectStmt *n = makeNode(CreateTypeObjectStmt);
					n->objfunclist = list_make1($1);
					$$ = (Node *) n;
				}
			|
			DeclareFuncInTypeObjStmts ',' ObjFuncDeclElem
				{
					CreateTypeObjectStmt *n = makeNode(CreateTypeObjectStmt);
					n->objfunclist = list_concat(((CreateTypeObjectStmt *)$1)->objfunclist, list_make1($3));
					$$ = (Node *) n;
				}
			;

CreateFuncInTypeObjStmt:
			DeclObjCons createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = (CreateFunctionStmt *) $1;
					n->options = $2;
					n->withClause = $3;
					$$ = (Node *)n;
				}
			| MEMBER_P CreateFuncInPkgStmt
				{
					CreateFunctionStmt *n = (CreateFunctionStmt *) $2;
					n->info = OBJ_FUNC_MEMBER;
					$$ = (Node *) n;
				}
			| STATIC_P CreateFuncInPkgStmt
				{
					CreateFunctionStmt *n = (CreateFunctionStmt *) $2;
					n->info = OBJ_FUNC_STATIC;
					$$ = (Node *) n;
				}
		;

CreateFuncInPkgStmt:
			FUNCTION func_name func_args_with_defaults
			  opt_func_return func_return createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = $3;
					n->returnType = $5;
					n->options = $6;
					n->withClause = $7;
					$$ = (Node *)n;
				}
			| FUNCTION func_name func_args_with_defaults
			  opt_func_return TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = mergeTableFuncParameters($3, $7);
					n->returnType = TableFuncTypeName($7);
					n->returnType->location = @5;
					n->options = $9;
					n->withClause = $10;
					$$ = (Node *)n;
				}
			| FUNCTION func_name func_args_with_defaults
			  createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = $3;
					n->returnType = NULL;
					n->options = $4;
					n->withClause = $5;
					$$ = (Node *)n;
				}
			| PROCEDURE func_name func_args_with_defaults
			  createfunc_opt_list
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->pkginner = true;
					n->funcname = $2;
					n->parameters = $3;
					n->returnType = NULL;
					n->is_procedure = true;
					n->options = $4;
					$$ = (Node *)n;
				}
			/* no params */
			| FUNCTION func_name_no_parens opt_func_return func_return
			createfunc_opt_list opt_definition
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = NULL;
				n->returnType = $4;
				n->options = $5;
				n->withClause = $6;
				$$ = (Node *)n;
			}
			| FUNCTION func_name_no_parens opt_func_return
			TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = mergeTableFuncParameters(NULL, $6);
				n->returnType = TableFuncTypeName($6);
				n->returnType->location = @4;
				n->options = $8;
				n->withClause = $9;
				$$ = (Node *)n;
			}
			| FUNCTION func_name_no_parens createfunc_opt_list opt_definition
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = NULL;
				n->returnType = NULL;
				n->options = $3;
				n->withClause = $4;
				$$ = (Node *)n;
			}
			| PROCEDURE func_name_no_parens createfunc_opt_list
			{
				CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
				n->pkginner = true;
				n->funcname = $2;
				n->parameters = NULL;
				n->returnType = NULL;
				n->is_procedure = true;
				n->options = $3;
				$$ = (Node *)n;
			}
		;


DropPackageStmt:
			DROP PACKAGE pkg_qualified_name opt_drop_behavior
				{
					DropPackageStmt *n = makeNode(DropPackageStmt);
					n->removeType = OBJECT_PACKAGE;
					n->pkgname = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
 			| DROP PACKAGE BODY pkg_qualified_name opt_drop_behavior
				{
					DropPackageStmt *n = makeNode(DropPackageStmt);
					n->removeType = OBJECT_PACKAGE_BODY;
					n->pkgname = $4;
					n->behavior = $5;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		DO <anonymous code block> [ LANGUAGE language ]
 *
 * We use a DefElem list for future extensibility, and to allow flexibility
 * in the clause order.
 *
 *****************************************************************************/

DoStmt: DO dostmt_opt_list
				{
					DoStmt *n = makeNode(DoStmt);
					n->args = $2;
					$$ = (Node *)n;
				}
		;

dostmt_opt_list:
			dostmt_opt_item						{ $$ = list_make1($1); }
			| dostmt_opt_list dostmt_opt_item	{ $$ = lappend($1, $2); }
		;

dostmt_opt_item:
			Sconst
				{
					$$ = makeDefElem("as", (Node *)makeString($1), @1);
				}
			| LANGUAGE Lower_NonReservedWord_or_Sconst
				{
					$$ = makeDefElem("language", (Node *)makeString($2), @1);
				}
		;

/*****************************************************************************
 *
 *		CREATE CAST / DROP CAST
 *
 *****************************************************************************/

CreateCastStmt: CREATE CAST '(' Typename AS Typename ')'
					WITH FUNCTION function_with_argtypes cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = $10;
					n->context = (CoercionContext) $11;
					n->inout = false;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITHOUT FUNCTION cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					n->inout = false;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITH INOUT cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					n->inout = true;
					$$ = (Node *)n;
				}
		;

cast_context:  AS IMPLICIT_P					{ $$ = COERCION_IMPLICIT; }
		| AS ASSIGNMENT							{ $$ = COERCION_ASSIGNMENT; }
		| /*EMPTY*/								{ $$ = COERCION_EXPLICIT; }
		;


DropCastStmt: DROP CAST opt_if_exists '(' Typename AS Typename ')' opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_CAST;
					n->objects = list_make1(list_make2($5, $7));
					n->behavior = $9;
					n->missing_ok = $3;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

opt_if_exists: IF_P EXISTS						{ $$ = TRUE; }
		| /*EMPTY*/								{ $$ = FALSE; }
		;


/*****************************************************************************
 *
 *		CREATE TRANSFORM / DROP TRANSFORM
 *
 *****************************************************************************/

CreateTransformStmt: CREATE opt_or_replace TRANSFORM FOR Typename LANGUAGE lower_name '(' transform_element_list ')'
				{
					CreateTransformStmt *n = makeNode(CreateTransformStmt);
					n->replace = $2;
					n->type_name = $5;
					n->lang = $7;
					n->fromsql = linitial($9);
					n->tosql = lsecond($9);
					$$ = (Node *)n;
				}
		;

transform_element_list: FROM SQL_P WITH FUNCTION function_with_argtypes ',' TO SQL_P WITH FUNCTION function_with_argtypes
				{
					$$ = list_make2($5, $11);
				}
				| TO SQL_P WITH FUNCTION function_with_argtypes ',' FROM SQL_P WITH FUNCTION function_with_argtypes
				{
					$$ = list_make2($11, $5);
				}
				| FROM SQL_P WITH FUNCTION function_with_argtypes
				{
					$$ = list_make2($5, NULL);
				}
				| TO SQL_P WITH FUNCTION function_with_argtypes
				{
					$$ = list_make2(NULL, $5);
				}
		;


DropTransformStmt: DROP TRANSFORM opt_if_exists FOR Typename LANGUAGE lower_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TRANSFORM;
					n->objects = list_make1(list_make2($5, makeString($7)));
					n->behavior = $8;
					n->missing_ok = $3;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		REINDEX [ (options) ] type <name>
 *****************************************************************************/

ReindexStmt:
			REINDEX reindex_target_type qualified_name
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = $2;
					n->relation = $3;
					n->name = NULL;
					n->options = 0;
					$$ = (Node *)n;
				}
			| REINDEX reindex_target_multitable name
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = $2;
					n->name = $3;
					n->relation = NULL;
					n->options = 0;
					$$ = (Node *)n;
				}
			| REINDEX '(' reindex_option_list ')' reindex_target_type qualified_name
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = $5;
					n->relation = $6;
					n->name = NULL;
					n->options = $3;
					$$ = (Node *)n;
				}
			| REINDEX '(' reindex_option_list ')' reindex_target_multitable name
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = $5;
					n->name = $6;
					n->relation = NULL;
					n->options = $3;
					$$ = (Node *)n;
				}
		;
reindex_target_type:
			INDEX					{ $$ = REINDEX_OBJECT_INDEX; }
			| TABLE					{ $$ = REINDEX_OBJECT_TABLE; }
		;
reindex_target_multitable:
			SCHEMA					{ $$ = REINDEX_OBJECT_SCHEMA; }
			| SYSTEM_P				{ $$ = REINDEX_OBJECT_SYSTEM; }
			| DATABASE				{ $$ = REINDEX_OBJECT_DATABASE; }
		;
reindex_option_list:
			reindex_option_elem								{ $$ = $1; }
			| reindex_option_list ',' reindex_option_elem	{ $$ = $1 | $3; }
		;
reindex_option_elem:
			VERBOSE	{ $$ = REINDEXOPT_VERBOSE; }
		;

/*****************************************************************************
 *
 * ALTER TABLESPACE
 *
 *****************************************************************************/

AlterTblSpcStmt:
			ALTER TABLESPACE name SET reloptions
				{
					AlterTableSpaceOptionsStmt *n =
						makeNode(AlterTableSpaceOptionsStmt);
					n->tablespacename = $3;
					n->options = $5;
					n->isReset = FALSE;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name RESET reloptions
				{
					AlterTableSpaceOptionsStmt *n =
						makeNode(AlterTableSpaceOptionsStmt);
					n->tablespacename = $3;
					n->options = $5;
					n->isReset = TRUE;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name RENAME TO newname
 *
 *****************************************************************************/

RenameStmt: ALTER AGGREGATE aggregate_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_AGGREGATE;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER COLLATION lower_any_name RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLLATION;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_CONVERSION;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name RENAME TO database_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DATABASE;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DOMAIN;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name RENAME CONSTRAINT name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DOMCONSTRAINT;
					n->object = (Node *) $3;
					n->subname = $6;
					n->newname = $8;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FDW;
					n->object = (Node *) makeString($5);
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION function_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FUNCTION;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER GROUP_P RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_GROUP;
#endif
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER opt_procedural LANGUAGE lower_name RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_LANGUAGE;
					n->object = (Node *) makeString($4);
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS lower_any_name USING access_method RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_OPCLASS;
					n->object = (Node *) lcons(makeString($6), $4);
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR FAMILY lower_any_name USING access_method RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_OPFAMILY;
					n->object = (Node *) lcons(makeString($6), $4);
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER POLICY name ON qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_POLICY;
					n->relation = $5;
					n->subname = $3;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER POLICY IF_P EXISTS name ON qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_POLICY;
					n->relation = $7;
					n->subname = $5;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE procedure_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_PROCEDURE;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER PUBLICATION name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_PUBLICATION;
					n->object = (Node *) makeString($3);
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER ROUTINE procedure_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROUTINE;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SCHEMA name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SCHEMA;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SERVER name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FOREIGN_SERVER;
					n->object = (Node *) makeString($3);
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SUBSCRIPTION name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SUBSCRIPTION;
					n->object = (Node *) makeString($3);
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SEQUENCE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SEQUENCE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_MATVIEW;
					n->relation = $4;
					n->subname = NULL;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_MATVIEW;
					n->relation = $6;
					n->subname = NULL;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER INDEX qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FOREIGN_TABLE;
					n->relation = $4;
					n->subname = NULL;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FOREIGN_TABLE;
					n->relation = $6;
					n->subname = NULL;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_MATVIEW;
					n->relation = $4;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_MATVIEW;
					n->relation = $6;
					n->subname = $9;
					n->newname = $11;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABCONSTRAINT;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME CONSTRAINT name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABCONSTRAINT;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_FOREIGN_TABLE;
					n->relation = $4;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_FOREIGN_TABLE;
					n->relation = $6;
					n->subname = $9;
					n->newname = $11;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER RULE name ON qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_RULE;
					n->relation = $5;
					n->subname = $3;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TRIGGER name ON qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TRIGGER;
					n->relation = $5;
					n->subname = $3;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER EVENT TRIGGER name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_EVENT_TRIGGER;
					n->object = (Node *) makeString($4);
					n->newname = $7;
					$$ = (Node *)n;
				}
			| ALTER ROLE RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_ROLE;
#endif
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER USER RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
#ifdef __AUDIT__
					n->stmt_type = ROLESTMT_USER;
#endif
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLESPACE;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER STATISTICS any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_STATISTIC_EXT;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH PARSER lower_any_name RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSPARSER;
					n->object = (Node *) $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH DICTIONARY lower_any_name RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSDICTIONARY;
					n->object = (Node *) $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH TEMPLATE lower_any_name RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSTEMPLATE;
					n->object = (Node *) $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name RENAME TO lower_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSCONFIGURATION;
					n->object = (Node *) $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TYPE;
					n->object = (Node *) $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name RENAME ATTRIBUTE name TO name opt_drop_behavior
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ATTRIBUTE;
					n->relationType = OBJECT_TYPE;
					n->relation = makeRangeVarFromAnyName($3, @3, yyscanner);
					n->subname = $6;
					n->newname = $8;
					n->behavior = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;

opt_column: COLUMN									{ $$ = COLUMN; }
			| /*EMPTY*/								{ $$ = 0; }
		;

opt_set_data: SET DATA_P							{ $$ = 1; }
			| /*EMPTY*/								{ $$ = 0; }
		;

/*****************************************************************************
 *
 * ALTER THING name DEPENDS ON EXTENSION name
 *
 *****************************************************************************/

AlterObjectDependsStmt:
			ALTER FUNCTION function_with_argtypes DEPENDS ON EXTENSION lower_name
				{
					AlterObjectDependsStmt *n = makeNode(AlterObjectDependsStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = (Node *) $3;
					n->extname = makeString($7);
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE procedure_with_argtypes DEPENDS ON EXTENSION lower_name
				{
					AlterObjectDependsStmt *n = makeNode(AlterObjectDependsStmt);
					n->objectType = OBJECT_PROCEDURE;
					n->object = (Node *) $3;
					n->extname = makeString($7);
					$$ = (Node *)n;
				}
			| ALTER ROUTINE procedure_with_argtypes DEPENDS ON EXTENSION lower_name
				{
					AlterObjectDependsStmt *n = makeNode(AlterObjectDependsStmt);
					n->objectType = OBJECT_ROUTINE;
					n->object = (Node *) $3;
					n->extname = makeString($7);
					$$ = (Node *)n;
				}
			| ALTER TRIGGER name ON qualified_name DEPENDS ON EXTENSION lower_name
				{
					AlterObjectDependsStmt *n = makeNode(AlterObjectDependsStmt);
					n->objectType = OBJECT_TRIGGER;
					n->relation = $5;
					n->object = (Node *) list_make1(makeString($3));
					n->extname = makeString($9);
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name DEPENDS ON EXTENSION lower_name
				{
					AlterObjectDependsStmt *n = makeNode(AlterObjectDependsStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $4;
					n->extname = makeString($8);
					$$ = (Node *)n;
				}
			| ALTER INDEX qualified_name DEPENDS ON EXTENSION lower_name
				{
					AlterObjectDependsStmt *n = makeNode(AlterObjectDependsStmt);
					n->objectType = OBJECT_INDEX;
					n->relation = $3;
					n->extname = makeString($7);
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name SET SCHEMA name
 *
 *****************************************************************************/

AlterObjectSchemaStmt:
			ALTER AGGREGATE aggregate_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_AGGREGATE;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER COLLATION lower_any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_COLLATION;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_CONVERSION;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_DOMAIN;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION lower_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_EXTENSION;
					n->object = (Node *) makeString($3);
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION function_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR operator_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_OPERATOR;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS lower_any_name USING access_method SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_OPCLASS;
					n->object = (Node *) lcons(makeString($6), $4);
					n->newschema = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR FAMILY lower_any_name USING access_method SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_OPFAMILY;
					n->object = (Node *) lcons(makeString($6), $4);
					n->newschema = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE procedure_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_PROCEDURE;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER ROUTINE procedure_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_ROUTINE;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TABLE;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TABLE;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER STATISTICS any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_STATISTIC_EXT;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH PARSER lower_any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSPARSER;
					n->object = (Node *) $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH DICTIONARY lower_any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSDICTIONARY;
					n->object = (Node *) $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH TEMPLATE lower_any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSTEMPLATE;
					n->object = (Node *) $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSCONFIGURATION;
					n->object = (Node *) $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_SEQUENCE;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_SEQUENCE;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $6;
					n->newschema = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FOREIGN_TABLE;
					n->relation = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FOREIGN_TABLE;
					n->relation = $6;
					n->newschema = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TYPE;
					n->object = (Node *) $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER OPERATOR name SET define
 *
 *****************************************************************************/

AlterOperatorStmt:
			ALTER OPERATOR operator_with_argtypes SET '(' operator_def_list ')'
				{
					AlterOperatorStmt *n = makeNode(AlterOperatorStmt);
					n->opername = $3;
					n->options = $6;
					$$ = (Node *)n;
				}
		;

operator_def_list:	operator_def_elem								{ $$ = list_make1($1); }
			| operator_def_list ',' operator_def_elem				{ $$ = lappend($1, $3); }
		;

operator_def_elem: Lower_ColLabel '=' NONE
						{ $$ = makeDefElem($1, NULL, @1); }
					| Lower_ColLabel '=' operator_def_arg
						{ $$ = makeDefElem($1, (Node *) $3, @1); }
		;

/* must be similar enough to def_arg to avoid reduce/reduce conflicts */
operator_def_arg:
			func_type						{ $$ = (Node *)$1; }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
		;

/*****************************************************************************
 *
 * ALTER THING name OWNER TO newname
 *
 *****************************************************************************/

AlterOwnerStmt: ALTER AGGREGATE aggregate_with_argtypes OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_AGGREGATE;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER COLLATION lower_any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_COLLATION;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_CONVERSION;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DATABASE;
					n->object = (Node *) makeString($3);
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DOMAIN;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION function_with_argtypes OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER opt_procedural LANGUAGE lower_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_LANGUAGE;
					n->object = (Node *) makeString($4);
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER LARGE_P OBJECT_P NumericOnly OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_LARGEOBJECT;
					n->object = (Node *) $4;
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR operator_with_argtypes OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPERATOR;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS lower_any_name USING access_method OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPCLASS;
					n->object = (Node *) lcons(makeString($6), $4);
					n->newowner = $9;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR FAMILY lower_any_name USING access_method OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPFAMILY;
					n->object = (Node *) lcons(makeString($6), $4);
					n->newowner = $9;
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE procedure_with_argtypes OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_PROCEDURE;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER ROUTINE procedure_with_argtypes OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_ROUTINE;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER SCHEMA name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_SCHEMA;
					n->object = (Node *) makeString($3);
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TYPE;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TABLESPACE;
					n->object = (Node *) makeString($3);
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER STATISTICS any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_STATISTIC_EXT;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH DICTIONARY lower_any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TSDICTIONARY;
					n->object = (Node *) $5;
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TSCONFIGURATION;
					n->object = (Node *) $5;
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FDW;
					n->object = (Node *) makeString($5);
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER SERVER name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FOREIGN_SERVER;
					n->object = (Node *) makeString($3);
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER EVENT TRIGGER name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_EVENT_TRIGGER;
					n->object = (Node *) makeString($4);
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER PUBLICATION name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_PUBLICATION;
					n->object = (Node *) makeString($3);
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER SUBSCRIPTION name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_SUBSCRIPTION;
					n->object = (Node *) makeString($3);
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER opt_or_public SYNONYM qualified_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_SYNONYM;
					n->object = (Node *) list_make2($2 ? makeInteger(1) : makeInteger(0), $4);
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER PACKAGE pkg_qualified_name OWNER TO RoleSpec
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_PACKAGE;
					n->object = (Node *) $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * CREATE PUBLICATION name [ FOR TABLE ] [ WITH options ]
 *
 *****************************************************************************/

CreatePublicationStmt:
			CREATE PUBLICATION name opt_publication_for_tables opt_publication_for_shards opt_definition
				{
					CreatePublicationStmt *n = makeNode(CreatePublicationStmt);
					n->pubname = $3;
					n->options = $6;
					if ($4 != NULL)
					{
						/* FOR TABLE */
						if (IsA($4, List))
							n->tables = (List *)$4;
						/* FOR ALL TABLES */
						else
							n->for_all_tables = TRUE;
					}

					n->shards = (List *)$5;

					$$ = (Node *)n;
				}
		;

opt_publication_for_tables:
			publication_for_tables					{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

publication_for_tables:
			FOR TABLE relation_expr_list
				{
					$$ = (Node *) $3;
				}
			| FOR ALL TABLES
				{
					$$ = (Node *) makeInteger(TRUE);
				}
		;

opt_publication_for_shards:
			publication_for_shards					{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;
publication_for_shards:
			BY SHARDING '('shard_list')'
				{
					$$ = (Node *) $4;
				}
		;



/*****************************************************************************
 *
 * ALTER PUBLICATION name SET ( options )
 *
 * ALTER PUBLICATION name ADD TABLE table [, table2]
 *
 * ALTER PUBLICATION name DROP TABLE table [, table2]
 *
 * ALTER PUBLICATION name SET TABLE table [, table2]
 *
 *****************************************************************************/

AlterPublicationStmt:
			ALTER PUBLICATION name SET definition
				{
					AlterPublicationStmt *n = makeNode(AlterPublicationStmt);
					n->pubname = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
			| ALTER PUBLICATION name ADD_P TABLE relation_expr_list
				{
					AlterPublicationStmt *n = makeNode(AlterPublicationStmt);
					n->pubname = $3;
					n->tables = $6;
					n->tableAction = DEFELEM_ADD;
					$$ = (Node *)n;
				}
			| ALTER PUBLICATION name SET TABLE relation_expr_list
				{
					AlterPublicationStmt *n = makeNode(AlterPublicationStmt);
					n->pubname = $3;
					n->tables = $6;
					n->tableAction = DEFELEM_SET;
					$$ = (Node *)n;
				}
			| ALTER PUBLICATION name DROP TABLE relation_expr_list
				{
					AlterPublicationStmt *n = makeNode(AlterPublicationStmt);
					n->pubname = $3;
					n->tables = $6;
					n->tableAction = DEFELEM_DROP;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * CREATE SUBSCRIPTION name ...
 *
 *****************************************************************************/

CreateSubscriptionStmt:
			CREATE SUBSCRIPTION name CONNECTION Sconst PUBLICATION publication_name_list opt_definition
				{
					CreateSubscriptionStmt *n =
						makeNode(CreateSubscriptionStmt);
					n->subname = $3;
					n->conninfo = $5;
					n->publication = $7;
					n->options = $8;
					n->isopentenbase = false;
					n->sub_parallel_number = 0;
					n->sub_parallel_index = 0;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| CREATE OPENTENBASE_P SUBSCRIPTION name CONNECTION Sconst PUBLICATION publication_name_list opt_definition
				{
					CreateSubscriptionStmt *n =
						makeNode(CreateSubscriptionStmt);
					n->subname = $4;
					n->conninfo = $6;
					n->publication = $8;
					n->options = $9;
					n->isopentenbase = true;
					n->sub_parallel_number = 0;
					n->sub_parallel_index = 0;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
		;

publication_name_list:
			publication_name_item
				{
					$$ = list_make1($1);
				}
			| publication_name_list ',' publication_name_item
				{
					$$ = lappend($1, $3);
				}
		;

publication_name_item:
			ColLabel			{ $$ = makeString($1); };

/*****************************************************************************
 *
 * ALTER SUBSCRIPTION name ...
 *
 *****************************************************************************/

AlterSubscriptionStmt:
			ALTER SUBSCRIPTION name SET definition
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_OPTIONS;
					n->subname = $3;
					n->options = $5;
					n->isopentenbase = false;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| ALTER OPENTENBASE_P SUBSCRIPTION name SET definition
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_OPTIONS;
					n->subname = $4;
					n->options = $6;
					n->isopentenbase = true;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
			| ALTER SUBSCRIPTION name CONNECTION Sconst
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_CONNECTION;
					n->subname = $3;
					n->conninfo = $5;
					n->isopentenbase = false;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| ALTER OPENTENBASE_P SUBSCRIPTION name CONNECTION Sconst
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_CONNECTION;
					n->subname = $4;
					n->conninfo = $6;
					n->isopentenbase = true;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
			| ALTER SUBSCRIPTION name REFRESH PUBLICATION opt_definition
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_REFRESH;
					n->subname = $3;
					n->options = $6;
					n->isopentenbase = false;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| ALTER OPENTENBASE_P SUBSCRIPTION name REFRESH PUBLICATION opt_definition
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_REFRESH;
					n->subname = $4;
					n->options = $7;
					n->isopentenbase = true;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
			| ALTER SUBSCRIPTION name SET PUBLICATION publication_name_list opt_definition
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_PUBLICATION;
					n->subname = $3;
					n->publication = $6;
					n->options = $7;
					n->isopentenbase = false;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| ALTER OPENTENBASE_P SUBSCRIPTION name SET PUBLICATION publication_name_list opt_definition
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_PUBLICATION;
					n->subname = $4;
					n->publication = $7;
					n->options = $8;
					n->isopentenbase = true;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
			| ALTER SUBSCRIPTION name ENABLE_P
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_ENABLED;
					n->subname = $3;
					n->options = list_make1(makeDefElem("enabled",
											(Node *)makeInteger(TRUE), @1));
					n->isopentenbase = false;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| ALTER OPENTENBASE_P SUBSCRIPTION name ENABLE_P
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_ENABLED;
					n->subname = $4;
					n->options = list_make1(makeDefElem("enabled",
											(Node *)makeInteger(TRUE), @1));
					n->isopentenbase = true;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
			| ALTER SUBSCRIPTION name DISABLE_P
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_ENABLED;
					n->subname = $3;
					n->options = list_make1(makeDefElem("enabled",
											(Node *)makeInteger(FALSE), @1));
					n->isopentenbase = false;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ BEGIN */
			| ALTER OPENTENBASE_P SUBSCRIPTION name DISABLE_P
				{
					AlterSubscriptionStmt *n =
						makeNode(AlterSubscriptionStmt);
					n->kind = ALTER_SUBSCRIPTION_ENABLED;
					n->subname = $4;
					n->options = list_make1(makeDefElem("enabled",
											(Node *)makeInteger(FALSE), @1));
					n->isopentenbase = true;
					$$ = (Node *)n;
				}
/* __SUBSCRIPTION__ END */
		;

/*****************************************************************************
 *
 * DROP SUBSCRIPTION [ IF EXISTS ] name
 *
 *****************************************************************************/

DropSubscriptionStmt: DROP SUBSCRIPTION name opt_drop_behavior
				{
					DropSubscriptionStmt *n = makeNode(DropSubscriptionStmt);
					n->subname = $3;
					n->missing_ok = false;
					n->behavior = $4;
					n->isopentenbase = false;
					$$ = (Node *) n;
				}
/* __SUBSCRIPTION__ BEGIN */
				|	DROP OPENTENBASE_P SUBSCRIPTION name opt_drop_behavior
				{
					DropSubscriptionStmt *n = makeNode(DropSubscriptionStmt);
					n->subname = $4;
					n->missing_ok = false;
					n->behavior = $5;
					n->isopentenbase = true;
					$$ = (Node *) n;
				}
/* __SUBSCRIPTION__ END */
				|  DROP SUBSCRIPTION IF_P EXISTS name opt_drop_behavior
				{
					DropSubscriptionStmt *n = makeNode(DropSubscriptionStmt);
					n->subname = $5;
					n->missing_ok = true;
					n->behavior = $6;
					n->isopentenbase = false;
					$$ = (Node *) n;
				}
/* __SUBSCRIPTION__ BEGIN */
				|  DROP OPENTENBASE_P SUBSCRIPTION IF_P EXISTS name opt_drop_behavior
				{
					DropSubscriptionStmt *n = makeNode(DropSubscriptionStmt);
					n->subname = $6;
					n->missing_ok = true;
					n->behavior = $7;
					n->isopentenbase = true;
					$$ = (Node *) n;
				}
/* __SUBSCRIPTION__ END */
		;

/*****************************************************************************
 *
 *		QUERY:	Define Rewrite Rule
 *
 *****************************************************************************/

RuleStmt:	CREATE opt_or_replace RULE name AS
			ON event TO qualified_name where_clause
			DO opt_instead RuleActionList
				{
					RuleStmt *n = makeNode(RuleStmt);
					n->replace = $2;
					n->relation = $9;
					n->rulename = $4;
					n->whereClause = $10;
					n->event = $7;
					n->instead = $12;
					n->actions = $13;
					$$ = (Node *)n;
				}
		;

RuleActionList:
			NOTHING									{ $$ = NIL; }
			| RuleActionStmt						{ $$ = list_make1($1); }
			| '(' RuleActionMulti ')'				{ $$ = $2; }
		;

/* the thrashing around here is to discard "empty" statements... */
RuleActionMulti:
			RuleActionMulti ';' RuleActionStmtOrEmpty
				{ if ($3 != NULL)
					$$ = lappend($1, $3);
				  else
					$$ = $1;
				}
			| RuleActionStmtOrEmpty
				{ if ($1 != NULL)
					$$ = list_make1($1);
				  else
					$$ = NIL;
				}
		;

RuleActionStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| NotifyStmt
		;

RuleActionStmtOrEmpty:
			RuleActionStmt							{ $$ = $1; }
			|	/*EMPTY*/							{ $$ = NULL; }
		;

event:		SELECT									{ $$ = CMD_SELECT; }
			| UPDATE								{ $$ = CMD_UPDATE; }
			| DELETE_P								{ $$ = CMD_DELETE; }
			| INSERT								{ $$ = CMD_INSERT; }
		 ;

opt_instead:
			INSTEAD									{ $$ = TRUE; }
			| ALSO									{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				NOTIFY <identifier> can appear both in rule bodies and
 *				as a query-level command
 *
 *****************************************************************************/

NotifyStmt: NOTIFY Lower_ColId notify_payload
				{
					NotifyStmt *n = makeNode(NotifyStmt);
					n->conditionname = $2;
					n->payload = $3;
					$$ = (Node *)n;
				}
		;

notify_payload:
			',' Sconst							{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NULL; }
		;

ListenStmt: LISTEN Lower_ColId
				{
					ListenStmt *n = makeNode(ListenStmt);
					n->conditionname = $2;
					$$ = (Node *)n;
				}
		;

UnlistenStmt:
			UNLISTEN Lower_ColId
				{
					UnlistenStmt *n = makeNode(UnlistenStmt);
					n->conditionname = $2;
					$$ = (Node *)n;
				}
			| UNLISTEN '*'
				{
					UnlistenStmt *n = makeNode(UnlistenStmt);
					n->conditionname = NULL;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		Transactions:
 *
 *		BEGIN / COMMIT / ROLLBACK
 *		(also older versions END / ABORT)
 *
 *****************************************************************************/

TransactionStmt:
			ABORT_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| BEGIN_P opt_transaction transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = $3;
					$$ = (Node *)n;
				}
			| BEGIN_SUBTXN Iconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN_SUBTXN;
					n->options = list_make1(makeDefElem("nestingLevel",
									(Node *)makeInteger($2), @1));
					$$ = (Node *)n;
				}
			| START TRANSACTION transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = $3;
					$$ = (Node *)n;
				}
			| COMMIT_SUBTXN Iconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_SUBTXN;
					n->options = list_make1(makeDefElem("nestingLevel",
												(Node *)makeInteger($2), @1));
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| END_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK_SUBTXN Iconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_SUBTXN;
					n->options = list_make1(makeDefElem("nestingLevel",
														(Node *)makeInteger($2), @1));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2), @1));
					$$ = (Node *)n;
				}
			| RELEASE SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($3), @1));
					$$ = (Node *)n;
				}
			| RELEASE ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2), @1));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($5), @1));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($4), @1));
					$$ = (Node *)n;
				}
			| PREPARE TRANSACTION Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_PREPARE;
					n->gid = $3;
					check2pcGid(n->gid);
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst FOR CHECK ONLY
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED_CHECK;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst FOR CHECK ONLY
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED_CHECK;
					n->gid = $3;
					$$ = (Node *)n;
				}
		;

opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

transaction_mode_item:
			ISOLATION LEVEL iso_level
					{ $$ = makeDefElem("transaction_isolation",
									   makeStringConst($3, @3), @1); }
			| READ ONLY
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(TRUE, @1), @1); }
			| READ WRITE
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(FALSE, @1), @1); }
			| DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(TRUE, @1), @1); }
			| NOT DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(FALSE, @1), @1); }
		;

/* Syntax with commas is SQL-spec, without commas is Postgres historical */
transaction_mode_list:
			transaction_mode_item
					{ $$ = list_make1($1); }
			| transaction_mode_list ',' transaction_mode_item
					{ $$ = lappend($1, $3); }
			| transaction_mode_list transaction_mode_item
					{ $$ = lappend($1, $2); }
		;

transaction_mode_list_or_empty:
			transaction_mode_list
			| /* EMPTY */
					{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query> [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
 *
 *****************************************************************************/

ViewStmt: CREATE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
/* BEGIN_OPENTENBASE_ORA */
					n->view = $4;
					n->view->relpersistence = $2;
					n->aliases = $5;
					n->query = $8;
					n->replace = false;
					n->options = $6;
					n->withCheckOption = $9;
					n->isforce = false;
					n->as_loc = @7;
/* END_OPENTENBASE_ORA */
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
/* BEGIN_OPENTENBASE_ORA */
					n->view = $6;
					n->view->relpersistence = $4;
					n->aliases = $7;
					n->query = $10;
					n->replace = true;
					n->options = $8;
					n->withCheckOption = $11;
					n->isforce = false;
					n->as_loc = @9;
/* END_OPENTENBASE_ORA */
					$$ = (Node *) n;
				}
		| CREATE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $5;
					n->view->relpersistence = $2;
					n->aliases = $7;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $11);
					n->replace = false;
					n->options = $9;
					n->withCheckOption = $12;
					if (n->withCheckOption != NO_CHECK_OPTION)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("WITH CHECK OPTION not supported on recursive views"),
								 parser_errposition(@12)));
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')' opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $7;
					n->view->relpersistence = $4;
					n->aliases = $9;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $13);
					n->replace = true;
					n->options = $11;
					n->withCheckOption = $14;
					if (n->withCheckOption != NO_CHECK_OPTION)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("WITH CHECK OPTION not supported on recursive views"),
								 parser_errposition(@14)));
					$$ = (Node *) n;
				}
		;



opt_check_option:
		WITH CHECK OPTION				{ $$ = CASCADED_CHECK_OPTION; }
		| WITH CASCADED CHECK OPTION	{ $$ = CASCADED_CHECK_OPTION; }
		| WITH LOCAL CHECK OPTION		{ $$ = LOCAL_CHECK_OPTION; }
		| /* EMPTY */					{ $$ = NO_CHECK_OPTION; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/

LoadStmt:	LOAD file_name
				{
					LoadStmt *n = makeNode(LoadStmt);
					n->filename = $2;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		CREATE DATABASE
 *
 *****************************************************************************/

CreatedbStmt:
			CREATE DATABASE database_name opt_with createdb_opt_list
				{
					CreatedbStmt *n = makeNode(CreatedbStmt);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

createdb_opt_list:
			createdb_opt_items						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

createdb_opt_items:
			createdb_opt_item						{ $$ = list_make1($1); }
			| createdb_opt_items createdb_opt_item	{ $$ = lappend($1, $2); }
		;

createdb_opt_item:
			createdb_opt_name opt_equal SignedIconst
				{
					$$ = makeDefElem($1, (Node *)makeInteger($3), @1);
				}
			| createdb_opt_name opt_equal opt_boolean_or_string
				{
					$$ = makeDefElem($1, (Node *)makeString($3), @1);
				}
			| createdb_opt_name opt_equal DEFAULT
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;

/*
 * Ideally we'd use ColId here, but that causes shift/reduce conflicts against
 * the ALTER DATABASE SET/RESET syntaxes.  Instead call out specific keywords
 * we need, and allow IDENT so that database option names don't have to be
 * parser keywords unless they are already keywords for other reasons.
 *
 * XXX this coding technique is fragile since if someone makes a formerly
 * non-keyword option name into a keyword and forgets to add it here, the
 * option will silently break.  Best defense is to provide a regression test
 * exercising every such option, at least at the syntax level.
 */
createdb_opt_name:
			Lower_IDENT						{ $$ = $1; }
			| CONNECTION LIMIT				{ $$ = pstrdup("connection_limit"); }
			| ENCODING						{ $$ = pstrdup($1); }
			| LOCATION						{ $$ = pstrdup($1); }
			| OWNER							{ $$ = pstrdup($1); }
			| TABLESPACE					{ $$ = pstrdup($1); }
			| TEMPLATE						{ $$ = pstrdup($1); }
			| SQL_P MODE                    { $$ = pstrdup("sql_mode"); }
		;

/*
 *	Though the equals sign doesn't match other WITH options, pg_dump uses
 *	equals for backward compatibility, and it doesn't seem worth removing it.
 */
opt_equal:	'='										{}
			| /*EMPTY*/								{}
		;

/*****************************************************************************
 *
 *		CREATE DIRECTORY
 *
 *****************************************************************************/

CreateDirStmt:
			CREATE DIRECTORY directory_name AS Sconst
				{
					CreateDirStmt *n = makeNode(CreateDirStmt);
					n->dirname = $3;
					n->dirpath = $5;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		DROP DIRECTORY
 *
 *****************************************************************************/

DropDirStmt:
			DROP DIRECTORY directory_name
				{
					DropDirStmt *n = makeNode(DropDirStmt);
					n->dirname = $3;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		ALTER DATABASE
 *
 *****************************************************************************/

AlterDatabaseStmt:
			ALTER DATABASE database_name WITH createdb_opt_list
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				 }
			| ALTER DATABASE database_name createdb_opt_list
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = $4;
					$$ = (Node *)n;
				 }
			| ALTER DATABASE database_name SET TABLESPACE name
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = list_make1(makeDefElem("tablespace",
														(Node *)makeString($6), @6));
					$$ = (Node *)n;
				 }
		;

AlterDatabaseSetStmt:
			ALTER DATABASE database_name SetResetClause
				{
					AlterDatabaseSetStmt *n = makeNode(AlterDatabaseSetStmt);
					n->dbname = $3;
					n->setstmt = $4;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		DROP DATABASE [ IF EXISTS ] dbname [ [ WITH ] ( options ) ]
 *
 * This is implicitly CASCADE, no need for drop behavior
 *****************************************************************************/

DropdbStmt: DROP DATABASE database_name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $3;
					n->missing_ok = FALSE;
					n->prepare = FALSE;
					$$ = (Node *)n;
				}
			| DROP DATABASE IF_P EXISTS database_name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $5;
					n->missing_ok = TRUE;
					n->prepare = FALSE;
					$$ = (Node *)n;
				}
			| DROP DATABASE PREPARE database_name
        			{
        				DropdbStmt *n = makeNode(DropdbStmt);
        				n->dbname = $4;
        				n->missing_ok = FALSE;
        				n->prepare = TRUE;
        				$$ = (Node *)n;
        			}
			| DROP DATABASE PREPARE IF_P EXISTS database_name
			{
				DropdbStmt *n = makeNode(DropdbStmt);
				n->dbname = $6;
				n->missing_ok = TRUE;
				n->prepare = TRUE;
				$$ = (Node *)n;
			}
			| DROP DATABASE name opt_with '(' drop_option_list ')'
				{
					DropdbStmt *n = makeNode(DropdbStmt);

					n->dbname = $3;
					n->missing_ok = false;
					n->options = $6;
					$$ = (Node *) n;
				}
			| DROP DATABASE IF_P EXISTS name opt_with '(' drop_option_list ')'
				{
					DropdbStmt *n = makeNode(DropdbStmt);

					n->dbname = $5;
					n->missing_ok = true;
					n->options = $8;
					$$ = (Node *) n;
				}
			| DROP DATABASE PREPARE database_name opt_with '(' drop_option_list ')'
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $4;
					n->missing_ok = FALSE;
					n->prepare = TRUE;
					n->options = $7;
					$$ = (Node *)n;
				}
			| DROP DATABASE PREPARE IF_P EXISTS database_name opt_with '(' drop_option_list ')'
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $6;
					n->missing_ok = TRUE;
					n->prepare = TRUE;
					n->options = $9;
					$$ = (Node *)n;
				}
		;

drop_option_list:
			drop_option
				{
					$$ = list_make1((Node *) $1);
				}
			| drop_option_list ',' drop_option
				{
					$$ = lappend($1, (Node *) $3);
				}
		;

/*
 * Currently only the FORCE option is supported, but the syntax is designed
 * to be extensible so that we can add more options in the future if required.
 */
drop_option:
			FORCE
				{
					$$ = makeDefElem("force", NULL, @1);
				}
		;

/*****************************************************************************
 *
 *		ALTER COLLATION
 *
 *****************************************************************************/

AlterCollationStmt: ALTER COLLATION lower_any_name REFRESH VERSION_P
				{
					AlterCollationStmt *n = makeNode(AlterCollationStmt);
					n->collname = $3;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		ALTER SYSTEM
 *
 * This is used to change configuration parameters persistently.
 *****************************************************************************/

AlterSystemStmt:
			ALTER SYSTEM_P SET generic_set
				{
					AlterSystemStmt *n = makeNode(AlterSystemStmt);
					n->setstmt = $4;
					$$ = (Node *)n;
				}
			| ALTER SYSTEM_P RESET generic_reset
				{
					AlterSystemStmt *n = makeNode(AlterSystemStmt);
					n->setstmt = $4;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Manipulate a domain
 *
 *****************************************************************************/

CreateDomainStmt:
			CREATE DOMAIN_P any_name opt_as Typename ColQualList
				{
					CreateDomainStmt *n = makeNode(CreateDomainStmt);
					n->domainname = $3;
					n->typeName = $5;
					SplitColQualList($6, &n->constraints, &n->collClause,
									 yyscanner);
					$$ = (Node *)n;
				}
		;

AlterDomainStmt:
			/* ALTER DOMAIN <domain> {SET DEFAULT <expr>|DROP DEFAULT} */
			ALTER DOMAIN_P any_name alter_column_default
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'T';
					n->typeName = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP NOT NULL */
			| ALTER DOMAIN_P any_name DROP NOT NULL_P
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'N';
					n->typeName = $3;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> SET NOT NULL */
			| ALTER DOMAIN_P any_name SET NOT NULL_P
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'O';
					n->typeName = $3;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> ADD CONSTRAINT ... */
			| ALTER DOMAIN_P any_name ADD_P TableConstraint
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'C';
					n->typeName = $3;
					n->def = $5;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| ALTER DOMAIN_P any_name DROP CONSTRAINT name opt_drop_behavior
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'X';
					n->typeName = $3;
					n->name = $6;
					n->behavior = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| ALTER DOMAIN_P any_name DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'X';
					n->typeName = $3;
					n->name = $8;
					n->behavior = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> VALIDATE CONSTRAINT <name> */
			| ALTER DOMAIN_P any_name VALIDATE CONSTRAINT name
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'V';
					n->typeName = $3;
					n->name = $6;
					$$ = (Node *)n;
				}
			;

opt_as:		AS										{}
			| /* EMPTY */							{}
		;


/*****************************************************************************
 *
 * Manipulate a text search dictionary or configuration
 *
 *****************************************************************************/

AlterTSDictionaryStmt:
			ALTER TEXT_P SEARCH DICTIONARY lower_any_name definition
				{
					AlterTSDictionaryStmt *n = makeNode(AlterTSDictionaryStmt);
					n->dictname = $5;
					n->options = $6;
					$$ = (Node *)n;
				}
		;

AlterTSConfigurationStmt:
			ALTER TEXT_P SEARCH CONFIGURATION lower_any_name ADD_P MAPPING FOR lower_name_list any_with lower_any_name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->kind = ALTER_TSCONFIG_ADD_MAPPING;
					n->cfgname = $5;
					n->tokentype = $9;
					n->dicts = $11;
					n->override = false;
					n->replace = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name ALTER MAPPING FOR lower_name_list any_with lower_any_name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->kind = ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN;
					n->cfgname = $5;
					n->tokentype = $9;
					n->dicts = $11;
					n->override = true;
					n->replace = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name ALTER MAPPING REPLACE lower_any_name any_with lower_any_name
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->kind = ALTER_TSCONFIG_REPLACE_DICT;
					n->cfgname = $5;
					n->tokentype = NIL;
					n->dicts = list_make2($9,$11);
					n->override = false;
					n->replace = true;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name ALTER MAPPING FOR lower_name_list REPLACE lower_any_name any_with lower_any_name
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->kind = ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN;
					n->cfgname = $5;
					n->tokentype = $9;
					n->dicts = list_make2($11,$13);
					n->override = false;
					n->replace = true;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name DROP MAPPING FOR lower_name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->kind = ALTER_TSCONFIG_DROP_MAPPING;
					n->cfgname = $5;
					n->tokentype = $9;
					n->missing_ok = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION lower_any_name DROP MAPPING IF_P EXISTS FOR lower_name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->kind = ALTER_TSCONFIG_DROP_MAPPING;
					n->cfgname = $5;
					n->tokentype = $11;
					n->missing_ok = true;
					$$ = (Node*)n;
				}
		;

/* Use this if TIME or ORDINALITY after WITH should be taken as an identifier */
any_with:	WITH									{}
			| WITH_LA								{}
		;


/*****************************************************************************
 *
 * Manipulate a conversion
 *
 *		CREATE [DEFAULT] CONVERSION <conversion_name>
 *		FOR <encoding_name> TO <encoding_name> FROM <func_name>
 *
 *****************************************************************************/

CreateConversionStmt:
			CREATE opt_default CONVERSION_P any_name FOR Sconst
			TO Sconst FROM any_name
			{
				CreateConversionStmt *n = makeNode(CreateConversionStmt);
				n->conversion_name = $4;
				n->for_encoding_name = $6;
				n->to_encoding_name = $8;
				n->func_name = $10;
				n->def = $2;
				$$ = (Node *)n;
			}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CLUSTER [VERBOSE] <qualified_name> [ USING <index_name> ]
 *				CLUSTER [VERBOSE]
 *				CLUSTER [VERBOSE] <index_name> ON <qualified_name> (for pre-8.3)
 *
 *****************************************************************************/

ClusterStmt:
			CLUSTER opt_verbose qualified_name cluster_index_specification
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					n->relation = $3;
					n->indexname = $4;
					n->verbose = $2;
					$$ = (Node*)n;
				}
			| CLUSTER opt_verbose
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					n->relation = NULL;
					n->indexname = NULL;
					n->verbose = $2;
					$$ = (Node*)n;
				}
			/* kept for pre-8.3 compatibility */
			| CLUSTER opt_verbose index_name ON qualified_name
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					n->relation = $5;
					n->indexname = $3;
					n->verbose = $2;
					$$ = (Node*)n;
				}
		;

cluster_index_specification:
			USING index_name		{ $$ = $2; }
			| /*EMPTY*/				{ $$ = NULL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/

VacuumStmt:
			VACUUM opt_full opt_freeze opt_verbose
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($3)
						n->options |= VACOPT_FREEZE;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose pg_qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($3)
						n->options |= VACOPT_FREEZE;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					n->relation = $5;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose AnalyzeStmt
				{
					VacuumStmt *n = (VacuumStmt *) $5;
					n->options |= VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($3)
						n->options |= VACOPT_FREEZE;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					$$ = (Node *)n;
				}
			| VACUUM '(' vacuum_option_list ')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *) n;
				}
			| VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					n->relation = $5;
					n->va_cols = $6;
					if (n->va_cols != NIL)	/* implies analyze */
						n->options |= VACOPT_ANALYZE;
					$$ = (Node *) n;
				}
/* _SHARDING_ BEGIN */
			| VACUUM opt_full opt_freeze opt_verbose pg_qualified_name SHARDING '(' Iconst ')'
				{
					VacuumShardStmt *n = makeNode(VacuumShardStmt);
					n->relation = $5;
					n->options = TRUNSHARDOPT_FREESTORAGE;
					n->shards = list_make1(makeIntConst($8, -1));
					n->pause = 0;
					$$ = (Node *) n;
				}
			| REBUILD CHECKSUM qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					n->options |= VACOPT_FULL;
					n->relation = $3;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
/* _SHARDING_ END*/
		;

vacuum_option_list:
			vacuum_option_elem								{ $$ = $1; }
			| vacuum_option_list ',' vacuum_option_elem		{ $$ = $1 | $3; }
		;

vacuum_option_elem:
			analyze_keyword		{ $$ = VACOPT_ANALYZE; }
			| VERBOSE			{ $$ = VACOPT_VERBOSE; }
			| FREEZE			{ $$ = VACOPT_FREEZE; }
			| FULL				{ $$ = VACOPT_FULL; }
			| Lower_IDENT
				{
					if (strcmp($1, "disable_page_skipping") == 0)
						$$ = VACOPT_DISABLE_PAGE_SKIPPING;
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unrecognized VACUUM option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;
AnalyzeStmt:
			analyze_keyword opt_verbose
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| analyze_keyword opt_verbose qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->relation = $3;
					n->va_cols = $4;
					$$ = (Node *)n;
				}
			| analyze_keyword '(' analyze_option_list ')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE | $3;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| analyze_keyword '(' analyze_option_list ')' qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE | $3;
					n->relation = $5;
					n->va_cols = $6;
					$$ = (Node *)n;
				}
		;

analyze_keyword:
			ANALYZE									{}
			| ANALYSE /* British */					{}
		;

opt_verbose:
			VERBOSE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_full:	FULL									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_freeze: FREEZE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

analyze_option_list:
			analyze_option_elem								{ $$ = $1; }
			| analyze_option_list ',' analyze_option_elem		{ $$ = $1 | $3; }
		;

analyze_option_elem:
			VERBOSE			{ $$ = VACOPT_VERBOSE; }
			| COORDINATOR 	{ $$ = VACOPT_COORDINATOR; }
			| LOCAL			{ $$ = VACOPT_ANALYZE_LOCAL; }
		;

opt_name_list:
			'(' name_list ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
/* __AUDIT__ BEGIN */

AuditStmt:
			audit_or_noaudit audit_stmt_list opt_when_success_or_not
				{
					AuditStmt * n = makeNode(AuditStmt);

					n->audit_ison = $1;
					n->audit_type = AuditType_Statement;
					n->audit_mode = $3;
					n->action_list = $2;
					n->user_list = NIL;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| audit_or_noaudit audit_stmt_list BY audit_user_list opt_when_success_or_not
				{
					AuditStmt * n = makeNode(AuditStmt);

					n->audit_ison = $1;
					n->audit_type = AuditType_User;
					n->audit_mode = $5;
					n->action_list = $2;
					n->user_list = $4;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| audit_or_noaudit audit_stmt_list ON audit_obj_type audit_obj opt_when_success_or_not
				{
					AuditStmt * n = makeNode(AuditStmt);

					n->audit_ison = $1;
					n->audit_type = AuditType_Object;
					n->audit_mode = $6;
					n->action_list = $2;
					n->user_list = NIL;
					n->object_name = $5;
					n->object_type = $4;

					if (n->object_name == NIL)
					{
						/*
						 * Audit xxx ON TABLE DEFAULT
						 */
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid audit statement")));
					}

					$$ = (Node *)n;
				}
			| audit_or_noaudit audit_stmt_list ON audit_obj opt_when_success_or_not
				{
					AuditStmt * n = makeNode(AuditStmt);

					n->audit_ison = $1;
					n->audit_type = AuditType_Object;
					n->audit_mode = $5;
					n->action_list = $2;
					n->user_list = NIL;
					n->object_name = $4;
					n->object_type = OBJECT_TABLE;

					$$ = (Node *)n;
				}
			;

audit_or_noaudit:
			AUDIT		{ $$ = true; }
			| NOAUDIT 	{ $$ = false; }
			;

opt_when_success_or_not:
			WHENEVER success_or_not SUCCESSFUL	{ $$ = $2; }
			| /* empty */						{ $$ = AuditMode_All; }
			;

success_or_not:
			NOT				{ $$ = AuditMode_Fail; }
			| /* empty */	{ $$ = AuditMode_Success; }
			;

audit_stmt_list:
			audit_stmts	{ $$ = $1; }
			| ALL		{ $$ = list_make1_int(AuditSql_All); }	/* AUDIT ALL [ {BY xxx} | {ON xxx} ]*/
			;

audit_stmts:
			audit_stmt							{ $$ = list_make1_int($1); }
			| audit_stmts ',' audit_stmt		{ $$ = list_append_unique_int($1, $3); }
			;

audit_stmt:
										/* AuditSql_ShortcutBegin */
										/* SQL Statement Shortcuts for Auditing	*/
										/* Used for AUDIT xxx [BY xxx] */

			ALTER SYSTEM_P				{ $$ = AuditSql_AlterSystem; }

			| DATABASE					{ $$ = AuditSql_Database; }
			| ALTER DATABASE			{ $$ = AuditSql_Database_Alter; }
			| CREATE DATABASE			{ $$ = AuditSql_Database_Create; }
			| DROP DATABASE				{ $$ = AuditSql_Database_Drop; }

			| EXTENSION					{ $$ = AuditSql_Extension; }
			| ALTER EXTENSION			{ $$ = AuditSql_Extension_Alter; }
			| CREATE EXTENSION			{ $$ = AuditSql_Extension_Create; }
			| DROP EXTENSION			{ $$ = AuditSql_Extension_Drop; }

			| FUNCTION					{ $$ = AuditSql_Function; }
			| ALTER FUNCTION			{ $$ = AuditSql_Function_Alter; }
			| CREATE FUNCTION			{ $$ = AuditSql_Function_Create; }
			| DROP FUNCTION				{ $$ = AuditSql_Function_Drop; }

			| GROUP_P					{ $$ = AuditSql_Group; }
			| CREATE NODE GROUP_P		{ $$ = AuditSql_Group_CreateNodeGroup; }
			| CREATE SHARDING GROUP_P	{ $$ = AuditSql_Group_CreateShardingGroup; }
			| DROP NODE GROUP_P			{ $$ = AuditSql_Group_DropNodeGroup; }
			| DROP SHARDING IN_P GROUP_P	{ $$ = AuditSql_Group_DropShardingInGroup; }

			| INDEX						{ $$ = AuditSql_Index; }
			| ALTER INDEX				{ $$ = AuditSql_Index_Alter; }
			| CREATE INDEX				{ $$ = AuditSql_Index_Create; }
			| DROP INDEX				{ $$ = AuditSql_Index_Drop; }

			| MATERIALIZED VIEW			{ $$ = AuditSql_MaterializedView; }
			| ALTER MATERIALIZED VIEW	{ $$ = AuditSql_MaterializedView_Alter; }
			| CREATE MATERIALIZED VIEW	{ $$ = AuditSql_MaterializedView_Create; }
			| DROP MATERIALIZED VIEW	{ $$ = AuditSql_MaterializedView_Drop; }

			| NODE						{ $$ = AuditSql_Node; }
			| ALTER NODE				{ $$ = AuditSql_Node_Alter; }
			| CREATE NODE				{ $$ = AuditSql_Node_Create; }
			| DROP NODE					{ $$ = AuditSql_Node_Drop; }

			| ROLE						{ $$ = AuditSql_Role; }
			| ALTER ROLE				{ $$ = AuditSql_Role_Alter; }
			| CREATE ROLE				{ $$ = AuditSql_Role_Create; }
			| DROP ROLE					{ $$ = AuditSql_Role_Drop; }
			| SET ROLE					{ $$ = AuditSql_Role_Set; }

			| SCHEMA					{ $$ = AuditSql_Schema; }
			| ALTER SCHEMA				{ $$ = AuditSql_Schema_Alter; }
			| CREATE SCHEMA				{ $$ = AuditSql_Schema_Create; }
			| DROP SCHEMA				{ $$ = AuditSql_Schema_Drop; }

			| SEQUENCE					{ $$ = AuditSql_Sequence; }
			| CREATE SEQUENCE			{ $$ = AuditSql_Sequence_Create; }
			| DROP SEQUENCE				{ $$ = AuditSql_Sequence_Drop; }

			| TABLE						{ $$ = AuditSql_Table; }
			| CREATE TABLE				{ $$ = AuditSql_Table_Create; }
			| DROP TABLE				{ $$ = AuditSql_Table_Drop; }
			| TRUNCATE TABLE			{ $$ = AuditSql_Table_Truncate; }

			| TABLESPACE				{ $$ = AuditSql_Tablespace; }
			| ALTER TABLESPACE			{ $$ = AuditSql_Tablespace_Alter; }
			| CREATE TABLESPACE			{ $$ = AuditSql_Tablespace_Create; }
			| DROP TABLESPACE			{ $$ = AuditSql_Tablespace_Drop; }

			| TRIGGER					{ $$ = AuditSql_Trigger; }
			| ALTER TRIGGER				{ $$ = AuditSql_Trigger_Alter; }
			| CREATE TRIGGER			{ $$ = AuditSql_Trigger_Create; }
			| DROP TRIGGER				{ $$ = AuditSql_Trigger_Drop; }
			| DISABLE_P TRIGGER			{ $$ = AuditSql_Trigger_Disable; }
			| ENABLE_P TRIGGER			{ $$ = AuditSql_Trigger_Enable; }

			| TYPE_P					{ $$ = AuditSql_Type; }
			| ALTER TYPE_P				{ $$ = AuditSql_Type_Alter; }
			| CREATE TYPE_P				{ $$ = AuditSql_Type_Create; }
			| DROP TYPE_P				{ $$ = AuditSql_Type_Drop; }

			| USER						{ $$ = AuditSql_User; }
			| ALTER USER				{ $$ = AuditSql_User_Alter; }
			| CREATE USER				{ $$ = AuditSql_User_Create; }
			| DROP USER					{ $$ = AuditSql_User_Drop; }

			| VIEW						{ $$ = AuditSql_View; }
			| ALTER VIEW 				{ $$ = AuditSql_View_Alter; }
			| CREATE VIEW				{ $$ = AuditSql_View_Create; }
			| DROP VIEW					{ $$ = AuditSql_View_Drop; }

			| SYNONYM					{ $$ = AuditSql_Synonym; }
			| CREATE SYNONYM			{ $$ = AuditSql_Synonym_Create; }
			| ALTER SYNONYM				{ $$ = AuditSql_Synonym_Alter; }
			| DROP SYNONYM				{ $$ = AuditSql_Synonym_Drop; }

										/* AuditSql_ShortcutEnd */
										/* SQL Statement Shortcuts for Auditing	*/

										/* AuditSql_AdditionalBegin */
										/* Additional SQL Statement Shortcuts for Auditing */
										/* Used for AUDIT xxx [BY xxx] */

			| ALTER SEQUENCE			{ $$ = AuditSql_AlterSequence; }
			| ALTER TABLE				{ $$ = AuditSql_AlterTable; }
			| COMMENT TABLE				{ $$ = AuditSql_CommentTable; }
			| DELETE_P TABLE			{ $$ = AuditSql_DeleteTable; }
			| GRANT FUNCTION			{ $$ = AuditSql_GrantFunction; }
			| GRANT SEQUENCE			{ $$ = AuditSql_GrantSequence; }
			| GRANT TABLE				{ $$ = AuditSql_GrantTable; }
			| GRANT TYPE_P				{ $$ = AuditSql_GrantType; }
			| INSERT TABLE				{ $$ = AuditSql_InsertTable; }
			| LOCK_P TABLE				{ $$ = AuditSql_LockTable; }
			| SELECT SEQUENCE			{ $$ = AuditSql_SelectSequence; }
			| SELECT TABLE				{ $$ = AuditSql_SelectTable; }
			| SYSTEM_P AUDIT			{ $$ = AuditSql_SystemAudit; }
			| SYSTEM_P GRANT			{ $$ = AuditSql_SystemGrant; }
			| UPDATE TABLE				{ $$ = AuditSql_UpdateTable; }

										/* AuditSql_AdditionalEnd */
										/* Additional SQL Statement Shortcuts for Auditing */

										/* AuditSql_SchemaObjectBegin */
										/* Schema Object Auditing Options */
										/* Used for AUDIT xxx ON xxx */

			| ALTER						{ $$ = AuditSql_Alter; }
			| AUDIT						{ $$ = AuditSql_Audit; }
			| COMMENT					{ $$ = AuditSql_Comment; }
			| DELETE_P					{ $$ = AuditSql_Delete; }
			| GRANT						{ $$ = AuditSql_Grant; }
			/* Audit/NoAudit Index On Table/MView is not supported in current version */
			/* | INDEX						{ $$ = AuditSql_Index; } */
			| INSERT					{ $$ = AuditSql_Insert; }
			| LOCK_P					{ $$ = AuditSql_Lock; }
			| RENAME					{ $$ = AuditSql_Rename; }
			| SELECT					{ $$ = AuditSql_Select; }
			| UPDATE					{ $$ = AuditSql_Update; }

										/* AUditSql_SchemaObjectEnd */
										/* Schema Object Auditing Options */
			;

audit_user_list:
			ColId				{ $$ = list_make1(makeString($1)); }
			| audit_user_list ',' ColId	{ $$ = lappend($1, makeString($3)); }
			;

audit_obj_type:
			SEQUENCE				{ $$ = OBJECT_SEQUENCE; }
			| TABLE					{ $$ = OBJECT_TABLE; }
			| VIEW					{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW		{ $$ = OBJECT_MATVIEW; }
			/* | COLUMN				{ $$ = OBJECT_COLUMN; } */
			/* | INDEX				{ $$ = OBJECT_INDEX; } */
			| FUNCTION				{ $$ = OBJECT_FUNCTION; }
			| TYPE_P				{ $$ = OBJECT_TYPE; }
			;

audit_obj:
			ColId
				{
					$$ = list_make1(makeString($1));
				}
			| ColId '.' ColId
				{
					$$ = list_make2(makeString($1), makeString($3));
				}
			| ColId '.' ColId '.' ColId
				{
					$$ = list_make3(makeString($1), makeString($3), makeString($5));
				}
			| ColId '.' ColId '.' ColId '.' ColId
				{
					$$ = list_make4(makeString($1), makeString($3), makeString($5), makeString($7));
				}
			| DEFAULT
				{
					$$ = NIL;
				}
			;

CleanAuditStmt:
			CLEAN ALL AUDIT
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_All;
					n->user_list = NIL;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| CLEAN STATEMENT AUDIT
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_Statement;
					n->user_list = NIL;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| CLEAN USER AUDIT
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_User;
					n->user_list = NIL;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| CLEAN USER AUDIT BY audit_user_list
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_ByUser;
					n->user_list = $5;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| CLEAN OBJECT_P AUDIT
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_Object;
					n->user_list = NIL;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			| CLEAN OBJECT_P AUDIT ON audit_obj_type audit_obj
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_OnObject;
					n->user_list = NIL;
					n->object_name = $6;
					n->object_type = $5;

					if (n->object_name == NIL)
					{
						/*
						 * Clean Audit ON TABLE DEFAULT
						 */
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid clean audit statement")));
					}

					$$ = (Node *)n;
				}
			| CLEAN OBJECT_P AUDIT ON audit_obj
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_OnObject;
					n->user_list = NIL;
					n->object_name = $5;
					n->object_type = OBJECT_TABLE;

					if (n->object_name == NIL)
					{
						n->clean_type = CleanAuditType_OnDefault;
					}

					$$ = (Node *)n;
				}
			| CLEAN UNKNOWN AUDIT
				{
					CleanAuditStmt *n = makeNode(CleanAuditStmt);

					n->clean_type = CleanAuditType_Unknown;
					n->user_list = NIL;
					n->object_name = NIL;
					n->object_type = 0;

					$$ = (Node *)n;
				}
			;

/* __AUDIT__ END */


/* _SAMPLE_ BEGIN */
SampleStmt:
			SAMPLE qualified_name '(' Iconst ')'
				{
					SampleStmt *n = makeNode(SampleStmt);

					n->relation = $2;
					n->rownum = $4;

					$$ = (Node *)n;
				}
			;

/* _SAMPLE_ END */

/* PGXC_BEGIN */
PauseStmt: PAUSE CLUSTER
				{
					PauseClusterStmt *n = makeNode(PauseClusterStmt);
					n->pause = true;
					$$ = (Node *)n;
				}
			| UNPAUSE CLUSTER
				{
					PauseClusterStmt *n = makeNode(PauseClusterStmt);
					n->pause = false;
					$$ = (Node *)n;
				}
			;

BarrierStmt: CREATE BARRIER opt_barrier_id
				{
					BarrierStmt *n = makeNode(BarrierStmt);
					n->id = $3;
					$$ = (Node *)n;
				}
			;

opt_barrier_id:
				Sconst
				{
					$$ = pstrdup($1);
				}
			| /* EMPTY */
				{
					$$ = NULL;
				}
			;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		CREATE GTM NODE nodename WITH
 *				(
 *					[ HOST = 'hostname', ]
 *					[ PORT = portnum, ]
 *				)
 *
 *		CREATE NODE nodename WITH
 *				(
 *					[ TYPE = ('datanode' | 'coordinator' | 'gtm' | 'forward'), ]
 *					[ HOST = 'hostname', ]
 *					[ PORT = portnum, ]
 *					[ FORWARD = forwardport, ]
 *					[ PRIMARY [ = boolean ], ]
 *					[ PREFERRED [ = boolean ] ]
 *				)
 *
 *****************************************************************************/

CreateNodeStmt: CREATE NODE pgxcnode_name OptWith
				{
					CreateNodeStmt *n = makeNode(CreateNodeStmt);
					n->node_type = PGXC_NODE_NONE; /*default value*/
					n->node_name = $3;
					n->options = $4;
					$$ = (Node *)n;
				}
				| CREATE GTM NODE pgxcnode_name OptWith
				{
					CreateNodeStmt *n = makeNode(CreateNodeStmt);
					n->node_type = PGXC_NODE_GTM;
					n->node_name = $4;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

pgxcnode_name:
			Lower_ColId						{ $$ = $1; };

pgxcgroup_name:
			Lower_ColId						{ $$ = $1; };

pgxcnodes:
			'(' pgxcnode_list ')'			{ $$ = $2; }
		;

pgxcnode_list:
			pgxcnode_list ',' pgxcnode_name		{ $$ = lappend($1, makeString($3)); }
			| pgxcnode_name						{ $$ = list_make1(makeString($1)); }
		;

/*****************************************************************************
 *
 *		QUERY:
 *		ALTER [CLUSTER] NODE nodename WITH
 *				(
 *					[ TYPE = ('datanode' | 'coordinator' | 'forward'), ]
 *					[ HOST = 'hostname', ]
 *					[ PORT = portnum, ]
 *					[ PRIMARY [ = boolean ], ]
 *					[ PREFERRED [ = boolean ], ]
 *				)
 *
 *             If CLUSTER is mentioned, the command is executed on all nodes.
 *             PS: We need to add this option on all other pertinent NODE ddl
 *             operations too!)
 *****************************************************************************/

AlterNodeStmt: ALTER CLUSTER NODE pgxcnode_name Identified_plane OptWith
				{
					AlterNodeStmt *n = makeNode(AlterNodeStmt);
					n->node_type = PGXC_NODE_NONE; /*default value*/
					n->cluster = true;
					n->node_name = $4;
                    n->plane_name = $5;
					n->options = $6;
					$$ = (Node *)n;
				}
              | ALTER NODE pgxcnode_name Identified_plane OptWith
				{
					AlterNodeStmt *n = makeNode(AlterNodeStmt);
					n->node_type = PGXC_NODE_NONE; /*default value*/
					n->cluster = false;
					n->node_name = $3;
                    n->plane_name = $4;
					n->options = $5;
					$$ = (Node *)n;
				}
			  | ALTER GTM NODE pgxcnode_name OptWith
				{
					AlterNodeStmt *n = makeNode(AlterNodeStmt);
					n->node_type = PGXC_NODE_GTM;
					n->cluster = false;
					n->node_name = $4;
                    n->plane_name = NULL;
					n->options = $5;
					$$ = (Node *)n;
				}
			  | ALTER CLUSTER GTM NODE pgxcnode_name OptWith
				{
					AlterNodeStmt *n = makeNode(AlterNodeStmt);
					n->node_type = PGXC_NODE_GTM;
					n->cluster = true;
					n->node_name = $5;
                    n->plane_name = NULL;
					n->options = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP NODE nodename
 *
 *****************************************************************************/

DropNodeStmt: DROP NODE pgxcnode_name Identified_plane OptWith
				{
					DropNodeStmt *n = makeNode(DropNodeStmt);
					n->node_name = $3;
                    n->plane_name = $4;
					n->options = $5;
					$$ = (Node *)n;
				}
			| DROP GTM NODE pgxcnode_name
				{
					DropNodeStmt *n = makeNode(DropNodeStmt);
					n->node_name = $4;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CREATE NODE GROUP groupname WITH (node1,...,nodeN)
 *
 *****************************************************************************/

CreateNodeGroupStmt:
		CREATE NODE GROUP_P pgxcgroup_name WITH pgxcnodes
			{
				CreateGroupStmt *n = makeNode(CreateGroupStmt);
				n->group_name = $4;
				n->nodes = $6;
				n->default_group = 0;
				$$ = (Node *)n;
			}
		| CREATE DEFAULT NODE GROUP_P pgxcgroup_name WITH pgxcnodes
			{
				CreateGroupStmt *n = makeNode(CreateGroupStmt);
				n->group_name = $5;
				n->nodes = $7;
				n->default_group = 1;
				$$ = (Node *)n;
			}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				ALTER NODE GROUP groupname SET TO DEFAULT
 *
 *****************************************************************************/
AlterNodeGroupStmt: ALTER NODE GROUP_P pgxcgroup_name alter_group_cmds
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->cmds = $5;
					$$ = (Node *)n;
				}
		;



/*****************************************************************************
 *
 *		QUERY:
 *				DROP NODE GROUP groupname
 *
 *****************************************************************************/

DropNodeGroupStmt: DROP NODE GROUP_P pgxcgroup_name
				{
					DropGroupStmt *n = makeNode(DropGroupStmt);
					n->group_name = $4;
					$$ = (Node *)n;
				}
		;

/* PGXC_END */

/*****************************************************************************
 *
 *		QUERY:
 *				MOVE DATA FROM datanodename TO datanodename
 *
 *****************************************************************************/
MoveDataStmt:
		MOVE GROUP_P pgxcgroup_name DATA_P FROM data_node TO data_node SHARDCLUSTER Iconst
			{
				MoveDataStmt *stmt = makeNode(MoveDataStmt);
				stmt->strategy = MOVE_DATA_STRATEGY_NODE;
				stmt->isextension = false;
				stmt->from_node = $6;
				stmt->to_node = $8;
				stmt->shards = NIL;

				stmt->num_shard = 0;
				stmt->arr_shard = NULL;
				stmt->fromid = 0;
				stmt->toid = 0;
				stmt->split_point = -1;
				stmt->group		  = makeString($3);
				stmt->scid = $10;
				$$ = (Node *)stmt;
			}
		| MOVE GROUP_P pgxcgroup_name DATA_P FROM data_node TO data_node WITH '(' shard_list ')' SHARDCLUSTER Iconst
			{
				MoveDataStmt *stmt = makeNode(MoveDataStmt);
				stmt->strategy = MOVE_DATA_STRATEGY_SHARD;
				stmt->isextension = false;
				stmt->from_node = $6;
				stmt->to_node = $8;
				stmt->shards = $11;

				stmt->num_shard = 0;
				stmt->arr_shard = NULL;
				stmt->fromid = 0;
				stmt->toid = 0;
				stmt->split_point = -1;
				stmt->group		  = makeString($3);
				stmt->scid = $14;
				$$ = (Node *)stmt;
			}
		| MOVE GROUP_P pgxcgroup_name DATA_P FROM data_node TO data_node AT Iconst SHARDCLUSTER Iconst
			{
				MoveDataStmt *stmt = makeNode(MoveDataStmt);
				stmt->strategy = MOVE_DATA_STRATEGY_AT;
				stmt->isextension = false;
				stmt->from_node = $6;
				stmt->to_node = $8;
				stmt->split_point = $10;

				stmt->shards = NIL;
				stmt->num_shard = 0;
				stmt->arr_shard = NULL;
				stmt->fromid = 0;
				stmt->toid = 0;
				stmt->group		  = makeString($3);
				stmt->scid = $12;
				$$ = (Node *)stmt;
			}
		| MOVE EXTENSION GROUP_P pgxcgroup_name DATA_P FROM data_node TO data_node WITH '(' shard_list ')' SHARDCLUSTER Iconst
			{
				MoveDataStmt *stmt = makeNode(MoveDataStmt);
				stmt->strategy = MOVE_DATA_STRATEGY_SHARD;
				stmt->isextension = true;
				stmt->from_node = $7;
				stmt->to_node = $9;
				stmt->shards = $12;

				stmt->num_shard = 0;
				stmt->arr_shard = NULL;
				stmt->fromid = 0;
				stmt->toid = 0;
				stmt->split_point = -1;
				stmt->scid = $15;
				stmt->group		  = makeString($4);
				$$ = (Node *)stmt;
			}
		;

data_node:
			pgxcnode_name				{ $$ = (A_Const *)makeStringConst($1, @1); }
			| OID_P '(' Iconst ')'		{ $$ = (A_Const *)makeIntConst($3, @3); }
		;

shard_list:
			Iconst
				{
					$$ = list_make1(makeIntConst($1, @1));
				}
			| shard_list ',' Iconst
				{
					$$ = lappend($1,makeIntConst($3, @3));
				}
		;

/*****************************************************************************
 *
 *		SQL:
 *				CREATE [EXTENSION] SHARDING GROUP indexofnode IN numofnodes
 *				CREATE [EXTENSION] SHARDING GROUP TO GROUP group_name
 *
 *****************************************************************************/
CreateShardStmt:
		CREATE SHARDING GROUP_P Iconst IN_P Iconst
			{
				CreateShardStmt *stmt = makeNode(CreateShardStmt);
				stmt->isExtended  = false;
				stmt->isnull = false;
				stmt->idx_of_node = $4;
				stmt->num_of_node = $6;
				stmt->members     = NULL;
				$$ = (Node *)stmt;
			}
		| CREATE EXTENSION SHARDING GROUP_P Iconst IN_P Iconst
			{
				CreateShardStmt *stmt = makeNode(CreateShardStmt);
				stmt->isExtended  = true;
				stmt->isnull = false;
				stmt->idx_of_node = $5;
				stmt->num_of_node = $7;
				stmt->members     = NULL;
				$$ = (Node *)stmt;
			}
		| CREATE SHARDING  GROUP_P TO GROUP_P pgxcgroup_name
			{
				CreateShardStmt *stmt = makeNode(CreateShardStmt);
				stmt->isnull = true;
				stmt->isExtended  = false;
				stmt->num_of_node = -1;
				stmt->idx_of_node = -1;
				stmt->members     = list_make1(makeString($6));
				$$ = (Node *)stmt;
			}
		| CREATE EXTENSION SHARDING GROUP_P TO GROUP_P pgxcgroup_name
			{
				CreateShardStmt *stmt = makeNode(CreateShardStmt);
				stmt->isnull = true;
				stmt->isExtended  = true;
				stmt->num_of_node = -1;
				stmt->idx_of_node = -1;
				stmt->members     = list_make1(makeString($7));
				$$ = (Node *)stmt;
			}
		;

/*****************************************************************************
 *
 *		SQL:
 *				DROP SHARDING IN_P GROUP_P pgxcgroup_name
 *
 *****************************************************************************/
DropShardStmt:
		DROP SHARDING IN_P GROUP_P pgxcgroup_name
			{
				DropShardStmt *stmt = makeNode(DropShardStmt);
				stmt->group_name     = $5;
				$$ = (Node *)stmt;
			}
		;

/****************************************************************************
 *
 *      SQL:
 *				CLEAN SHARDING [GROUP_P pgxcgroup_name]
 ****************************************************************************/
CleanShardingStmt:
		CLEAN SHARDING
			{
				CleanShardingStmt *stmt = makeNode(CleanShardingStmt);
				stmt->group_name = NULL;
				$$ = (Node *)stmt;
			}
		| CLEAN SHARDING GROUP_P pgxcgroup_name FROM pgxcnode_name TO pgxcnode_name
			{
				CleanShardingStmt *stmt = makeNode(CleanShardingStmt);
				stmt->group_name = $4;
				stmt->dn_from = $6;
				stmt->dn_to = $8;
				$$ = (Node *)stmt;
			}
		;


/*****************************************************************************
 *
 *		QUERY:
 *				EXPLAIN [ANALYZE] [VERBOSE] query
 *				EXPLAIN ( options ) query
 *
 *****************************************************************************/

ExplainStmt:
		EXPLAIN ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $2;
					n->options = NIL;
					$$ = (Node *) n;
				}
		| EXPLAIN analyze_keyword opt_verbose ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $4;
					n->options = list_make1(makeDefElem("analyze", NULL, @2));
					if ($3)
						n->options = lappend(n->options,
											 makeDefElem("verbose", NULL, @3));
					$$ = (Node *) n;
				}
		| EXPLAIN VERBOSE ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $3;
					n->options = list_make1(makeDefElem("verbose", NULL, @2));
					$$ = (Node *) n;
				}
		| EXPLAIN '(' explain_option_list ')' ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $5;
					n->options = $3;
					$$ = (Node *) n;
				}
		;

ExplainableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| DeclareCursorStmt
			| CreateAsStmt
			| CreateMatViewStmt
			| RefreshMatViewStmt
			| MergeStmt
			| ExecuteStmt					/* by default all are $$=$1 */
			| CreateFuncInPkgStmt
		;

explain_option_list:
			explain_option_elem
				{
					$$ = list_make1($1);
				}
			| explain_option_list ',' explain_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

explain_option_elem:
			explain_option_name explain_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;

explain_option_name:
			Lower_NonReservedWord	{ $$ = $1; }
			| analyze_keyword		{ $$ = "analyze"; }
		;

explain_option_arg:
			opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
			| NumericOnly			{ $$ = (Node *) $1; }
			| /* EMPTY */			{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				EXECUTE DIRECT ON ( nodename [, ... ] ) query
 *
 *****************************************************************************/

ExecDirectStmt: EXECUTE DIRECT ON pgxcnodes DirectStmt
				{
					ExecDirectStmt *n = makeNode(ExecDirectStmt);
					n->node_names = $4;
					n->query = $5;
					$$ = (Node *)n;

					if (!superuser() && !mls_user())
						ereport(ERROR,
						       (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						        errmsg("must be superuser to use EXECUTE DIRECT")));
				}
		;

DirectStmt:
			Sconst					/* by default all are $$=$1 */
		;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		CLEAN CONNECTION TO { COORDINATOR ( nodename ) | NODE ( nodename ) | ALL {FORCE} }
 *				[ FOR DATABASE dbname ]
 *				[ TO USER username ]
 *
 *****************************************************************************/

CleanConnStmt: CLEAN CONNECTION TO COORDINATOR pgxcnodes CleanConnDbName CleanConnUserName
				{
					CleanConnStmt *n = makeNode(CleanConnStmt);
					n->is_coord = true;
					n->nodes = $5;
					n->is_force = false;
					n->dbname = $6;
					n->username = $7;
					$$ = (Node *)n;
				}
				| CLEAN CONNECTION TO NODE pgxcnodes CleanConnDbName CleanConnUserName
				{
					CleanConnStmt *n = makeNode(CleanConnStmt);
					n->is_coord = false;
					n->nodes = $5;
					n->is_force = false;
					n->dbname = $6;
					n->username = $7;
					$$ = (Node *)n;
				}
				| CLEAN CONNECTION TO ALL opt_force CleanConnDbName CleanConnUserName
				{
					CleanConnStmt *n = makeNode(CleanConnStmt);
					n->is_coord = true;
					n->nodes = NIL;
					n->is_force = $5;
					n->dbname = $6;
					n->username = $7;
					$$ = (Node *)n;
				}
		;

CleanConnDbName: FOR DATABASE database_name		{ $$ = $3; }
				| FOR database_name				{ $$ = $2; }
				| /* EMPTY */					{ $$ = NULL; }
		;

CleanConnUserName: TO USER RoleId				{ $$ = $3; }
				| TO RoleId						{ $$ = $2; }
				| /* EMPTY */					{ $$ = NULL; }
		;

opt_force:	FORCE									{  $$ = TRUE; }
			| /* EMPTY */							{  $$ = FALSE; }
		;

/* PGXC_END */

/*****************************************************************************
 *
 *		QUERY:
 *				PREPARE <plan_name> [(args, ...)] AS <query>
 *
 *****************************************************************************/

PrepareStmt: PREPARE name prep_type_clause AS PreparableStmt
				{
					PrepareStmt *n = makeNode(PrepareStmt);
					n->name = $2;
					n->argtypes = $3;
					n->query = $5;
					$$ = (Node *) n;
				}
		;

prep_type_clause: '(' type_list ')'			{ $$ = $2; }
				| /* EMPTY */				{ $$ = NIL; }
		;

PreparableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| MergeStmt					/* by default all are $$=$1 */
		;

/*****************************************************************************
 *
 * EXECUTE <plan_name> [(params, ...)]
 * CREATE TABLE <name> AS EXECUTE <plan_name> [(params, ...)]
 *
 *****************************************************************************/

ExecuteStmt: EXECUTE name execute_param_clause
				{
					ExecuteStmt *n = makeNode(ExecuteStmt);
					n->name = $2;
					n->params = $3;
					$$ = (Node *) n;
				}
			| CREATE OptTemp TABLE create_as_target AS
				EXECUTE name execute_param_clause opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ExecuteStmt *n = makeNode(ExecuteStmt);
					n->name = $7;
					n->params = $8;
					ctas->query = (Node *) n;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
#ifdef PGXC
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CREATE TABLE AS EXECUTE not yet supported")));
#endif
					$4->skipData = !($9);
					$$ = (Node *) ctas;
				}
		;

execute_param_clause: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
					;

/*****************************************************************************
 *
 *		QUERY:
 *				DEALLOCATE [PREPARE] <plan_name>
 *
 *****************************************************************************/

DeallocateStmt: DEALLOCATE name
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = $2;
						$$ = (Node *) n;
					}
				| DEALLOCATE PREPARE name
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = $3;
						$$ = (Node *) n;
					}
				| DEALLOCATE ALL
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = NULL;
						$$ = (Node *) n;
					}
				| DEALLOCATE PREPARE ALL
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = NULL;
						$$ = (Node *) n;
					}
		;
/*****************************************************************************
 *
 *		QUERY:
 *				CREATE PROFILE profname limit args..
 *				ALTER  PROFILE profname limit args..
 *              DROP   PROFILE profname
 *              ALTER  RESOURCE COST args..
 *
 *****************************************************************************/

CreateProfileStmt:
		CREATE PROFILE profile_name LIMIT profile_param_list
			{
				CreateProfileStmt *n = makeNode(CreateProfileStmt);
				n->name = $3;
				n->profile_args = $5;
				$$ = (Node *)n;
			}
		;
AlterProfileStmt:
		ALTER PROFILE profile_name LIMIT profile_param_list
			{
				AlterProfileStmt *n = makeNode(AlterProfileStmt);
				n->name = $3;
				n->profile_args = $5;
				$$ = (Node *)n;
			}
		;
DropProfileStmt:
		DROP PROFILE profile_name opt_drop_behavior
			{
				DropProfileStmt *n = makeNode(DropProfileStmt);
				n->name = $3;
				n->behavior = $4;
				$$ = (Node *)n;
			}
		;
AlterResourceCostStmt:
		ALTER RESOURCE COST profile_param_list
			{
				AlterResourceCostStmt *n = makeNode(AlterResourceCostStmt);
				n->resource_args = $4;
				$$ = (Node *)n;
			}
        ;
profile_name:
			ColId							{ $$ = $1; };
profile_value:
	         Iconst      { $$ = makeInteger($1); }
			 | FCONST    { $$ = makeFloat($1); }
			 | Lower_ColId { $$ = makeString($1); }
			 | NULL_P    { $$ = makeString("null"); }
			 | DEFAULT   { $$ = makeString("default"); }
			 | Iconst '/' Iconst
			 	{
			 	        $$ = (Value *)makeSimpleA_Expr(AEXPR_OP, "/", makeIntConst($1, @1), makeIntConst($3, @3), @2);
			 	}
			 ;

profile_param: Lower_ColId profile_value
			   {
			        ProfileParameter *n = (ProfileParameter *)palloc(sizeof(ProfileParameter));
					n->name = $1;
					n->value = $2;
					$$ = (Node *)n;
			   }
           ;
profile_param_list:	profile_param
					{
						$$ = list_make1($1);
					}
				| profile_param_list profile_param
					{
						$$ = lappend($1, $2);
					}
			;

/*****************************************************************************
 *
 *		QUERY:
 *				INSERT STATEMENTS
 *
 *****************************************************************************/

InsertStmt:
			with_clause in_insert_scope INSERT INTO
            {
            }
            insert_target
            {
            }
            insert_rest
			opt_on_conflict returning_clause out_insert_scope
				{
					$8->relation = $6;
					$8->onConflictClause = $9;
					$8->returningList = $10;
					$8->withClause = $1;
					$$ = (Node *) $8;
			}
			| in_insert_scope INSERT INTO
            {
            }
            insert_target
            {
			}
            insert_rest opt_on_conflict
			returning_clause out_insert_scope
			{
				$7->relation = $5;
				$7->onConflictClause = $8;
				$7->returningList = $9;
				$7->withClause = NULL;
				$$ = (Node *) $7;
			}
		;

/*
 * Can't easily make AS optional here, because VALUES in insert_rest would
 * have a shift/reduce conflict with VALUES as an optional alias.  We could
 * easily allow unreserved_keywords as optional aliases, but that'd be an odd
 * divergence from other places.  So just require AS for now.
 */
insert_target:
			qualified_name %prec ESCAPE
				{
					$$ = $1;
					$$->dblinkname = NULL;
				}
			/* begin opentenbase_ora */
			| qualified_name ColId
				{
					$1->alias = makeAlias($2, NIL);
					$$ = $1;
					$$->dblinkname = NULL;
				}
			| qualified_name opentenbase_ora_table_alias_unreserved
				{
					$1->alias = makeAlias(upcase_identifier($2, strlen($2)), NULL);
					$$ = $1;
					$$->dblinkname = NULL;
				}
			/* end opentenbase_ora */
			| qualified_name AS ColId
				{
					$1->alias = makeAlias($3, NIL);
					$$ = $1;
					$$->dblinkname = NULL;
				}
			| qualified_name AS opentenbase_ora_table_alias_unreserved
				{
					$1->alias = makeAlias(upcase_identifier($3, strlen($3)), NULL);
					$$ = $1;
					$$->dblinkname = NULL;
				}
			| qualified_name '@' name
		            	{
		            		$$ = $1;
		            		$$->dblinkname = $3;
		            	}
			| qualified_name PG_PARTITION '(' Common_IDENT ')'
				{
					$$ = $1;
					$$->dblinkname = NULL;
					$$->childtablename = $4;
				}
		;

insert_rest:
			SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = $1;
				}
			| OVERRIDING override_kind VALUE_P SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->override = $2;
					$$->selectStmt = $4;
				}
			| '(' insert_column_list ')' SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = $2;
					$$->selectStmt = $4;
				}
			| '(' insert_column_list ')' OVERRIDING override_kind VALUE_P SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = $2;
					$$->override = $5;
					$$->selectStmt = $7;
				}
			| DEFAULT VALUES
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = NULL;
				}
		;

override_kind:
			USER		{ $$ = OVERRIDING_USER_VALUE; }
			| SYSTEM_P	{ $$ = OVERRIDING_SYSTEM_VALUE; }
		;

insert_column_list:
			insert_column_item
					{ $$ = list_make1($1); }
			| insert_column_list ',' insert_column_item
					{ $$ = lappend($1, $3); }
		;

insert_column_item:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;
					$$->location = @1;
				}
			| opentenbase_ora_table_alias_unreserved opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = upcase_identifier($1, strlen($1));
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;
					$$->location = @1;
				}
		;

opt_on_conflict:
			ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list	where_clause
				{
					$$ = makeNode(OnConflictClause);
					$$->action = ONCONFLICT_UPDATE;
					$$->infer = $3;
					$$->targetList = $7;
					$$->whereClause = $8;
					$$->location = @1;
				}
			|
			ON CONFLICT opt_conf_expr DO NOTHING
				{
					$$ = makeNode(OnConflictClause);
					$$->action = ONCONFLICT_NOTHING;
					$$->infer = $3;
					$$->targetList = NIL;
					$$->whereClause = NULL;
					$$->location = @1;
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
		;

opt_conf_expr:
			'(' index_params ')' where_clause
				{
					$$ = makeNode(InferClause);
					$$->indexElems = $2;
					$$->whereClause = $4;
					$$->conname = NULL;
					$$->location = @1;
				}
			|
			ON CONSTRAINT name
				{
					$$ = makeNode(InferClause);
					$$->indexElems = NIL;
					$$->whereClause = NULL;
					$$->conname = $3;
					$$->location = @1;
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
		;

returning_clause:
			RETURNING target_list		{ $$ = $2; }
			| /* EMPTY */				{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				DELETE STATEMENTS
 *
 *****************************************************************************/

DeleteStmt: opt_with_clause DELETE_P FROM
            {
            }
            relation_expr_opt_alias
            {
            }
			using_clause where_or_current_clause returning_clause
				{
					DeleteStmt *n = makeNode(DeleteStmt);
					n->relation = $5;
					n->usingClause = $7;
					n->whereClause = $8;
					n->returningList = $9;
					n->withClause = $1;
					$$ = (Node *)n;
				}
/*
 * In opentenbase_ora, delete stmt allows to lose the keyword 'FROM'.
 */
			| opt_with_clause DELETE_P
            {
            }
            relation_expr_opt_alias
            {
            }
			using_clause where_or_current_clause returning_clause
				{
						DeleteStmt *n = makeNode(DeleteStmt);
						n->relation = $4;
						n->usingClause = $6;
						n->whereClause = $7;
						n->returningList = $8;
						n->withClause = $1;
						$$ = (Node *)n;
				}
		;

using_clause:
				USING from_list						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				LOCK TABLE
 *
 *****************************************************************************/

LockStmt:	LOCK_P opt_table relation_expr_list opt_lock opt_nowait
				{
					LockStmt *n = makeNode(LockStmt);

					n->relations = $3;
					n->mode = $4;
					n->nowait = $5;
					$$ = (Node *)n;
				}
		;

opt_lock:	IN_P lock_type MODE				{ $$ = $2; }
			| /*EMPTY*/						{ $$ = AccessExclusiveLock; }
		;

lock_type:	ACCESS SHARE					{ $$ = AccessShareLock; }
			| ROW SHARE						{ $$ = RowShareLock; }
			| ROW EXCLUSIVE					{ $$ = RowExclusiveLock; }
			| SHARE UPDATE EXCLUSIVE		{ $$ = ShareUpdateExclusiveLock; }
			| SHARE							{ $$ = ShareLock; }
			| SHARE ROW EXCLUSIVE			{ $$ = ShareRowExclusiveLock; }
			| EXCLUSIVE						{ $$ = ExclusiveLock; }
			| ACCESS EXCLUSIVE				{ $$ = AccessExclusiveLock; }
		;

opt_nowait:	NOWAIT							{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;

opt_nowait_or_skip:
			NOWAIT							{ $$ = LockWaitError; }
			| SKIP LOCKED					{ $$ = LockWaitSkip; }
			| /*EMPTY*/						{ $$ = LockWaitBlock; }
		;
/* OPENTENBASE_ORA_BEGIN */
opt_wait_second:
			WAIT Iconst
			{
				if($2 > 2147483)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("syntax error at or near \"%d\"",
								$2),
								parser_errposition(@2)));
				$$ = $2;
			}
		;
/* OPENTENBASE_ORA_END */
/*****************************************************************************
 *
 *		QUERY:
 *				LOCK/UNLOCK NODE(locktype,lockobject,lockparams......);
 *
 *****************************************************************************/
LockNodeStmt:   NODE LOCK_P WITH '(' lock_param_list ')'
				{
					LockNodeStmt *n = makeNode(LockNodeStmt);
					n->lock = true;
					n->params = $5;
					$$ = (Node *)n;
				}
                | NODE UNLOCK_P WITH '(' lock_param_list ')'
				{
					LockNodeStmt *n = makeNode(LockNodeStmt);
					n->lock = false;
					n->params = $5;
					$$ = (Node *)n;
				};

lock_param_list:  lock_param
					{ $$ = list_make1($1); }
				| lock_param_list ',' lock_param
					{ $$ = lappend($1, $3); }
			    ;
lock_param:       Iconst
					{$$ = (Node *)makeInteger($1); }
				| Sconst
					{$$ = (Node *)makeString($1); }
				;

/*****************************************************************************
 *
 *		QUERY:
 *				UpdateStmt (UPDATE)
 *
 *****************************************************************************/
in_update_scope:
	{
		ORA_ENTER_SCOPE(ORA_UPDATE_SCOPE, pg_yyget_extra(yyscanner));
	}
out_update_scope:
    {
        ORA_LEAVE_SCOPE(ORA_UPDATE_SCOPE, pg_yyget_extra(yyscanner));
    }
UpdateStmt: opt_with_clause in_update_scope UPDATE
            {
            }
            relation_expr_opt_alias
            {
            }
			SET set_clause_list
			from_clause
			where_or_current_clause
			returning_clause out_update_scope
				{
					UpdateStmt *n = makeNode(UpdateStmt);
					n->relation = $5;
					n->targetList = $8;
					n->fromClause = $9;
					n->whereClause = $10;
					n->returningList = $11;
					n->withClause = $1;
					$$ = (Node *)n;
				}
		;

set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause:
			set_target '=' a_expr
				{
					$1->val = (Node *) $3;
					$$ = list_make1($1);
				}
			| '(' set_target_list ')' '=' a_expr
				{
					int ncolumns = list_length($2);
					int i = 1;
					ListCell *col_cell;

					/* Create a MultiAssignRef source for each target */
					foreach(col_cell, $2)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						MultiAssignRef *r = makeNode(MultiAssignRef);

						r->source = (Node *) $5;
						r->colno = i;
						r->ncolumns = ncolumns;
						res_col->val = (Node *) r;
						i++;
					}

					$$ = $2;
				}
		;

set_target:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;	/* upper production sets this */
					$$->location = @1;
				}
			| opentenbase_ora_table_alias_unreserved opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = upcase_identifier($1, strlen($1));
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL; /* upper production sets this */
					$$->location = @1;
				}
		;

set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;

in_insert_scope:
	    /* enter insert scope  */
	{
		ORA_ENTER_SCOPE(ORA_INSERT_SCOPE, pg_yyget_extra(yyscanner));
	}
out_insert_scope:
    {
        ORA_LEAVE_SCOPE(ORA_INSERT_SCOPE, pg_yyget_extra(yyscanner));
    }

/*****************************************************************************
 *
 *		QUERY:
 *				LOAD INTO rel FROM extrel
 *
 *****************************************************************************/
LoadFromStmt:
			LOAD INTO qualified_name FROM qualified_name
				{
					LoadFromStmt *m = makeNode(LoadFromStmt);

					m->rel = $3;
					m->ext_rel = $5;
					$$ = (Node *)m;
				}
			;

/*****************************************************************************
 *
 *		QUERY:
 *				MERGE STATEMENTS
 *
 *****************************************************************************/
in_merge_scope:
	   /* enter merge stmt */
	{
		ORA_ENTER_SCOPE(ORA_MERGE_SCOPE, pg_yyget_extra(yyscanner));
	}
out_merge_scope:
    {
        ORA_LEAVE_SCOPE(ORA_MERGE_SCOPE, pg_yyget_extra(yyscanner));
    }
MergeStmt:
	opt_with_clause in_merge_scope MERGE INTO
            {
            }
            merge_relation_expr_opt_alias
            {
            }
			USING table_ref
			ON a_expr
			merge_when_list out_merge_scope
			{
				MergeStmt *m		= makeNode(MergeStmt);

				m->withClause		= $1;
				m->target			= $6;
				m->sourceRelation	= $9;
				m->joinCondition	= $11;
				m->mergeWhenClauses = $12;

				$$ = (Node *)m;
			}
			;

merge_when_list:
			merge_when_clause						{ $$ = list_make1($1); }
			| merge_when_list merge_when_clause 	{ $$ = lappend($1,$2); }
			;

merge_when_clause:
			WHEN MATCHED THEN merge_update opt_merge_where_condition merge_update_delete
				{
					$4->matched = true;
					$4->condition = $5;
					$4->deleteClause = $6;
					$$ = (Node *) $4;
				}
			| WHEN NOT MATCHED THEN merge_insert opt_merge_where_condition
				{
					$5->matched = false;
					$5->condition = $6;

					$$ = (Node *) $5;
				}
			;

opt_merge_where_condition:
			WHERE a_expr			{ $$ = $2; }
			|				{ $$ = NULL; }
			;

merge_update:
			UPDATE SET set_clause_list
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->targetList      = $3;
					n->commandType    = CMD_UPDATE;
					$$ = n;
				}
			;

merge_update_delete:
			DELETE_P opt_merge_where_condition
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->matched         = true;
					n->commandType     = CMD_DELETE;
					n->condition       = $2;
					$$ = n;
				}
			|				{ $$ = NULL; }
			;

merge_insert:
			INSERT merge_values_clause
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->targetList            = NIL;
					n->values          = $2;
					n->commandType	   = CMD_INSERT;
					n->override		   = OVERRIDING_NOT_SET;

					$$ = n;
				}
			| INSERT OVERRIDING override_kind VALUE_P merge_values_clause
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->commandType = CMD_INSERT;
					n->override = $3;
					n->targetList = NIL;
					n->values = $5;
					$$ = n;
				}
			| INSERT '(' insert_column_list ')' merge_values_clause
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->targetList            = $3;
					n->values          = $5;
					n->commandType	   = CMD_INSERT;
					n->override		   = OVERRIDING_NOT_SET;

					$$ = n;
				}
			| INSERT '(' insert_column_list ')' OVERRIDING override_kind VALUE_P merge_values_clause
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->commandType = CMD_INSERT;
					n->override = $6;
					n->targetList = $3;
					n->values = $8;
					$$ = n;
				}
			| INSERT DEFAULT VALUES
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->targetList            = NIL;
					n->values          = NIL;
					n->commandType	   = CMD_INSERT;
					$$ = n;
				}
			;

merge_values_clause:
			VALUES ctext_row
				{
					$$ = $2;
				}
			;

merge_relation_expr_opt_alias: relation_expr					%prec UMINUS
				{
					$$ = (Node *) $1;
				}
			| select_with_parens                        %prec UMINUS
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->lateral = false;
					n->subquery = $1;
					n->alias = NULL;
					if (n->alias == NULL)
					{
						n->alias = makeAnonymousAlias(@1);
					}
					$$ = (Node *) n;
				}
			| relation_expr ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $2;
					$1->alias = alias;
					$$ =  (Node *) $1;
				}
			| relation_expr opentenbase_ora_table_alias_unreserved
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = upcase_identifier($2, strlen($2));
					$1->alias = alias;
					$$ =  (Node *) $1;
				}
			| relation_expr AS ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $3;
					$1->alias = alias;
					$$ =  (Node *) $1;
				}
			| relation_expr AS opentenbase_ora_table_alias_unreserved
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = upcase_identifier($3, strlen($3));
					$1->alias = alias;
					$$ =  (Node *) $1;
				}
			| select_with_parens alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->lateral = false;
					n->subquery = $1;
					n->alias = $2;

					if (n->alias == NULL)
					{
						n->alias = makeAnonymousAlias(@1);
					}
					$$ = (Node *) n;
				}
		;

/*
 * The SQL spec defines "contextually typed value expressions" and
 * "contextually typed row value constructors", which for our purposes
 * are the same as "a_expr" and "row" except that DEFAULT can appear at
 * the top level.
 */

ctext_expr:
			a_expr					{ $$ = (Node *) $1; }
//			| DEFAULT
//				{
//					SetToDefault *n = makeNode(SetToDefault);
//					n->location = @1;
//					$$ = (Node *) n;
//				}
		;

ctext_expr_list:
			ctext_expr								{ $$ = list_make1($1); }
			| ctext_expr_list ',' ctext_expr		{ $$ = lappend($1, $3); }
		;

/*
 * We should allow ROW '(' ctext_expr_list ')' too, but that seems to require
 * making VALUES a fully reserved word, which will probably break more apps
 * than allowing the noise-word is worth.
 */
ctext_row: '(' ctext_expr_list ')'					{ $$ = $2; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CURSOR STATEMENTS
 *
 *****************************************************************************/
DeclareCursorStmt: DECLARE cursor_name cursor_options CURSOR opt_hold FOR SelectStmt
				{
					DeclareCursorStmt *n = makeNode(DeclareCursorStmt);
					n->portalname = $2;
					/* currently we always set FAST_PLAN option */
					n->options = $3 | $5 | CURSOR_OPT_FAST_PLAN;
					n->query = $7;
					$$ = (Node *)n;
				}
		;

cursor_name:	name						{ $$ = $1; }
		;

cursor_options: /*EMPTY*/					{ $$ = 0; }
			| cursor_options NO SCROLL		{ $$ = $1 | CURSOR_OPT_NO_SCROLL; }
			| cursor_options SCROLL			{ $$ = $1 | CURSOR_OPT_SCROLL; }
			| cursor_options BINARY			{ $$ = $1 | CURSOR_OPT_BINARY; }
			| cursor_options INSENSITIVE	{ $$ = $1 | CURSOR_OPT_INSENSITIVE; }
		;

opt_hold: /* EMPTY */						{ $$ = 0; }
			| WITH HOLD						{ $$ = CURSOR_OPT_HOLD; }
			| WITHOUT HOLD					{ $$ = 0; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				SELECT STATEMENTS
 *
 *****************************************************************************/

/* A complete SELECT statement looks like this.
 *
 * The rule returns either a single SelectStmt node or a tree of them,
 * representing a set-operation tree.
 *
 * There is an ambiguity when a sub-SELECT is within an a_expr and there
 * are excess parentheses: do the parentheses belong to the sub-SELECT or
 * to the surrounding a_expr?  We don't really care, but bison wants to know.
 * To resolve the ambiguity, we are careful to define the grammar so that
 * the decision is staved off as long as possible: as long as we can keep
 * absorbing parentheses into the sub-SELECT, we will do so, and only when
 * it's no longer possible to do that will we decide that parens belong to
 * the expression.	For example, in "SELECT (((SELECT 2)) + 3)" the extra
 * parentheses are treated as part of the sub-select.  The necessity of doing
 * it that way is shown by "SELECT (((SELECT 2)) UNION SELECT 2)".	Had we
 * parsed "((SELECT 2))" as an a_expr, it'd be too late to go back to the
 * SELECT viewpoint when we see the UNION.
 *
 * This approach is implemented by defining a nonterminal select_with_parens,
 * which represents a SELECT with at least one outer layer of parentheses,
 * and being careful to use select_with_parens, never '(' SelectStmt ')',
 * in the expression grammar.  We will then have shift-reduce conflicts
 * which we can resolve in favor of always treating '(' <select> ')' as
 * a select_with_parens.  To resolve the conflicts, the productions that
 * conflict with the select_with_parens productions are manually given
 * precedences lower than the precedence of ')', thereby ensuring that we
 * shift ')' (and then reduce to select_with_parens) rather than trying to
 * reduce the inner <select> nonterminal to something else.  We use UMINUS
 * precedence for this, which is a fairly arbitrary choice.
 *
 * To be able to define select_with_parens itself without ambiguity, we need
 * a nonterminal select_no_parens that represents a SELECT structure with no
 * outermost parentheses.  This is a little bit tedious, but it works.
 *
 * In non-expression contexts, we use SelectStmt which can represent a SELECT
 * with or without outer parentheses.
 */

SelectStmt: select_no_parens			%prec UMINUS
			| select_with_parens		%prec UMINUS
		;

select_with_parens:
			'(' select_no_parens ')'				{ $$ = $2; }
			| '(' select_with_parens ')'			{ $$ = $2; }
		;

/*
 * This rule parses the equivalent of the standard's <query expression>.
 * The duplicative productions are annoying, but hard to get rid of without
 * creating shift/reduce conflicts.
 *
 *	The locking clause (FOR UPDATE etc) may be before or after LIMIT/OFFSET.
 *	In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE
 *	We now support both orderings, but prefer LIMIT/OFFSET before the locking
 * clause.
 *	2002-08-28 bjm
 */
select_no_parens:
			simple_select						{ $$ = $1; }
			| select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, NIL,
										NULL, NULL,
										yyscanner);
					$$ = $1;
				}
			| select_clause opt_sort_clause for_locking_clause opt_sort_clause opt_select_limit
				{
					if ($2 != NULL && $4 != NULL)
						elog(ERROR, "SQL command not properly ended");
					insertSelectOptions((SelectStmt *) $1, $2 != NULL ? $2 : $4, $3,
										$5,
										NULL,
										yyscanner);
					$$ = $1;
				}
			| select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, $4,
										$3,
										NULL,
										yyscanner);
					$$ = $1;
				}
			| with_clause select_clause
				{
					insertSelectOptions((SelectStmt *) $2, NULL, NIL,
										NULL,
										$1,
										yyscanner);
					$$ = $2;
				}
			| with_clause select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, NIL,
										NULL,
										$1,
										yyscanner);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause for_locking_clause opt_sort_clause opt_select_limit
				{
					if ($3 != NULL && $5 != NULL)
						elog(ERROR, "SQL command not properly ended");
					insertSelectOptions((SelectStmt *) $2, $3 != NULL ? $3 : $5, $4,
										$6,
										$1,
										yyscanner);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, $5,
										$4,
										$1,
										yyscanner);
					$$ = $2;
				}
		;

select_clause:
			simple_select							{ $$ = $1; }
			| select_with_parens					{ $$ = $1; }
		;

/*
 * This rule parses SELECT statements that can appear within set operations,
 * including UNION, INTERSECT and EXCEPT.  '(' and ')' can be used to specify
 * the ordering of the set operations.	Without '(' and ')' we want the
 * operations to be ordered per the precedence specs at the head of this file.
 *
 * As with select_no_parens, simple_select cannot have outer parentheses,
 * but can have parenthesized subclauses.
 *
 * Note that sort clauses cannot be included at this level --- SQL requires
 *		SELECT foo UNION SELECT bar ORDER BY baz
 * to be parsed as
 *		(SELECT foo UNION SELECT bar) ORDER BY baz
 * not
 *		SELECT foo UNION (SELECT bar ORDER BY baz)
 * Likewise for WITH, FOR UPDATE and LIMIT.  Therefore, those clauses are
 * described as part of the select_no_parens production, not simple_select.
 * This does not limit functionality, because you can reintroduce these
 * clauses inside parentheses.
 *
 * NOTE: only the leftmost component SelectStmt should have INTO.
 * However, this is not checked by the grammar; parse analysis must check it.
 */
in_simple_select_scope:
		   /* enter simple select */
		{
			ORA_ENTER_SCOPE(ORA_SELECT_SCOPE, pg_yyget_extra(yyscanner));
		}
out_simple_select_scope:
        {
        	ORA_LEAVE_SCOPE(ORA_SELECT_SCOPE, pg_yyget_extra(yyscanner));
        }
simple_select:
			in_simple_select_scope SELECT opt_all_clause opt_target_list
			into_clause from_clause where_clause
			group_having window_clause out_simple_select_scope
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->targetList = $4;
					n->intoClause = $5;
					n->fromClause = $6;
					n->whereClause = $7;
					n->groupClause = $8->groupby;
					n->havingClause = $8->having;
					n->windowClause = $9;
					$$ = (Node *)n;
				}
			| in_simple_select_scope SELECT distinct_clause target_list
			into_clause from_clause where_clause
			group_having  window_clause out_simple_select_scope
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->distinctClause = $3;
					n->targetList = $4;
					n->intoClause = $5;
					n->fromClause = $6;
					n->whereClause = $7;
					n->groupClause = $8->groupby;
					n->havingClause = $8->having;
					n->windowClause = $9;
					$$ = (Node *)n;
				}
			| values_clause
			{ 
				$$ = $1;
			}
			| TABLE relation_expr
				{
					/* same as SELECT * FROM relation_expr */
					ColumnRef *cr = makeNode(ColumnRef);
					ResTarget *rt = makeNode(ResTarget);
					SelectStmt *n = makeNode(SelectStmt);

					cr->fields = list_make1(makeNode(A_Star));
					cr->location = -1;

					rt->name = NULL;
					rt->indirection = NIL;
					rt->val = (Node *)cr;
					rt->location = -1;

					n->targetList = list_make1(rt);
					n->fromClause = list_make1($2);
					$$ = (Node *)n;
				}
			| select_clause UNION all_or_distinct select_clause
				{
					$$ = makeSetOp(SETOP_UNION, $3, $1, $4);
				}
			| select_clause INTERSECT all_or_distinct select_clause
				{
					$$ = makeSetOp(SETOP_INTERSECT, $3, $1, $4);
				}
			| select_clause EXCEPT all_or_distinct select_clause
				{
					$$ = makeSetOp(SETOP_EXCEPT, $3, $1, $4);
				}
/* OPENTENBASE_ORA_BEGIN */
			| select_clause MINUS all_or_distinct select_clause
				{
					$$ = makeSetOp(SETOP_MINUS, $3, $1, $4);
				}
/* OPENTENBASE_ORA_END */
		;

group_having:
	  group_clause having_clause
		{
				$$ = palloc0(sizeof(GroupHaving));
				$$->groupby = $1;
				$$->having = $2;
		}
	| having_clause group_clause
		{
				$$ = palloc0(sizeof(GroupHaving));
				$$->groupby = $2;
				$$->having = $1;
		}
	| group_clause
		{
				$$ = palloc0(sizeof(GroupHaving));
				$$->groupby = $1;
				$$->having = NULL;
		}
	| having_clause
		{
				$$ = palloc0(sizeof(GroupHaving));
				$$->groupby = NIL;
				$$->having = $1;
		}
	| /*empty*/
		{
				$$ = palloc0(sizeof(GroupHaving));
				$$->groupby = NIL;
				$$->having = NULL;
		}
	;

opt_materialized:
		MATERIALIZED							{ $$ = CTEMaterializeAlways; }
		| NOT MATERIALIZED						{ $$ = CTEMaterializeNever; }
		| /*EMPTY*/								{ $$ = CTEMaterializeDefault; }
		;
/*
 * SQL standard WITH clause looks like:
 *
 * WITH [ RECURSIVE ] <query name> [ (<column>,...) ]
 *		AS (query) [ SEARCH or CYCLE clause ]
 *
 * We don't currently support the SEARCH or CYCLE clause.
 *
 * Recognizing WITH_LA here allows a CTE to be named TIME or ORDINALITY.
 */
with_clause:
		WITH cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $2;
				$$->recursive = false;
				$$->location = @1;
			}
		| WITH_LA cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $2;
				$$->recursive = false;
				$$->location = @1;
			}
		| WITH RECURSIVE cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $3;
				$$->recursive = true;
				$$->location = @1;
			}
		;

cte_list:
		common_table_expr						{ $$ = list_make1($1); }
		| cte_list ',' common_table_expr		{ $$ = lappend($1, $3); }
		;

common_table_expr:  name opt_name_list AS opt_materialized '(' PreparableStmt ')'
			{
				CommonTableExpr *n = makeNode(CommonTableExpr);
				n->ctename = $1;
				n->aliascolnames = $2;
				n->ctematerialized = $4;
				n->ctequery = $6;
				n->location = @1;
				$$ = (Node *) n;
			}
		| opentenbase_ora_ident opt_name_list AS opt_materialized '(' PreparableStmt ')'
			{
				CommonTableExpr *n = makeNode(CommonTableExpr);
				n->ctename = $1;
				n->aliascolnames = $2;
				n->ctematerialized = $4;
				n->ctequery = $6;
				n->location = @1;
				$$ = (Node *) n;
			}
		;

opt_with_clause:
		with_clause								{ $$ = $1; }
		| /*EMPTY*/								{ $$ = NULL; }
		;

into_clause:
			INTO OptTempTableName
				{
					$$ = makeNode(IntoClause);
					$$->rel = $2;
					$$->colNames = NIL;
					$$->options = NIL;
					$$->onCommit = ONCOMMIT_NOOP;
					$$->tableSpaceName = NULL;
					$$->viewQuery = NULL;
					$$->skipData = false;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTemp.
 */
OptTempTableName:
			TEMPORARY opt_table qualified_ref
				{
					$$ = $3;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| TEMP opt_table qualified_ref
				{
					$$ = $3;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| LOCAL TEMPORARY opt_table qualified_name
				{
					$$ = $4;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| LOCAL TEMP opt_table qualified_name
				{
					$$ = $4;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| UNLOGGED opt_table qualified_ref
				{
					$$ = $3;
					$$->relpersistence = RELPERSISTENCE_UNLOGGED;
				}
			| TABLE qualified_name
				{
					$$ = $2;
					$$->relpersistence = RELPERSISTENCE_PERMANENT;
				}
			| qualified_name
				{
					$$ = $1;
					$$->relpersistence = RELPERSISTENCE_PERMANENT;
				}
		;

opt_table:	TABLE									{}
			| /*EMPTY*/								{}
		;

all_or_distinct:
			ALL										{ $$ = TRUE; }
			| DISTINCT								{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* We use (NIL) as a placeholder to indicate that all target expressions
 * should be placed in the DISTINCT list during parsetree analysis.
 */
distinct_clause:
			DISTINCT								{ $$ = list_make1(NIL); }
			| DISTINCT ON '(' expr_list ')'			{ $$ = $4; }
			| UNIQUE								{ $$ = list_make1(NIL); }
		;

opt_truncation_indicator:
			Sconst									{ $$ = makeStringConst($1, @1);}
			| /*EMPTY*/								{ $$ = makeStringConst("...", -1);}
		;
opt_with_count_expr:
			WITH COUNT								{ $$ = makeIntConst(1, @1);}
			| WITHOUT COUNT							{ $$ = makeIntConst(0, @1);}
			| /*EMPTY*/								{ $$ = makeIntConst(1, -1);}
		;
opt_all_clause:
			ALL										{ $$ = NIL;}
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_sort_clause:
			sort_clause								{ $$ = $1;}
			| /*EMPTY*/								{ $$ = NIL; }
		;
in_sort_scope:
	  /* enter in sort scope */
	{
		ORA_ENTER_SCOPE(ORA_SORT_SCOPE, pg_yyget_extra(yyscanner));
	}
out_sort_scope:
    {
        ORA_LEAVE_SCOPE(ORA_SORT_SCOPE, pg_yyget_extra(yyscanner));
    }
sort_clause:
			in_sort_scope ORDER BY sortby_list out_sort_scope
			{
				$$ = $4;
			}
		;

sortby_list:
			sortby									{ $$ = list_make1($1); }
			| sortby_list ',' sortby				{ $$ = lappend($1, $3); }
		;

sortby:		a_expr USING qual_all_Op opt_nulls_order
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_dir = SORTBY_USING;
					$$->sortby_nulls = $4;
					$$->useOp = $3;
					$$->location = @3;
				}
			| a_expr opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_dir = $2;
					$$->sortby_nulls = $3;
					$$->useOp = NIL;
					$$->location = -1;		/* no operator */
				}
		;

select_limit:
			limit_clause offset_clause
				{
					$$ = $1;
					($$)->limitOffset = $2;
				}
			| offset_clause limit_clause
				{
					$$ = $2;
					($$)->limitOffset = $1;
				}
			| limit_clause
				{
					$$ = $1;
				}
			| offset_clause
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = $1;
					n->limitCount = NULL;
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
		;

opt_select_limit:
			select_limit						{ $$ = $1; }
			| /* EMPTY */						{ $$ = NULL; }
		;

limit_clause:
			LIMIT select_limit_value
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $2;
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
			| LIMIT select_limit_value ',' select_offset_value
				{
					/* Disabled because it was too confusing, bjm 2002-02-18 */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("LIMIT #,# syntax is not supported"),
							 errhint("Use separate LIMIT and OFFSET clauses."),
							 parser_errposition(@1)));
				}
			/* SQL:2008 syntax */
			/* to avoid shift/reduce conflicts, handle the optional value with
			 * a separate production rather than an opt_ expression.  The fact
			 * that ONLY is fully reserved means that this way, we defer any
			 * decision about what rule reduces ROW or ROWS to the point where
			 * we can see the ONLY token in the lookahead slot.
			 */
			| FETCH first_or_next select_fetch_first_value row_or_rows ONLY
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $3;
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
			| FETCH first_or_next select_fetch_first_value PERCENT row_or_rows WITH TIES
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $3;
					n->limitOption = LIMIT_OPTION_PERCENT_WITH_TIES;
					$$ = n;
				}
			| FETCH first_or_next select_fetch_first_value PERCENT row_or_rows ONLY
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $3;
					n->limitOption = LIMIT_OPTION_PERCENT_COUNT;
					$$ = n;
				}
			| FETCH first_or_next select_fetch_first_value row_or_rows WITH TIES
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = $3;
					n->limitOption = LIMIT_OPTION_WITH_TIES;
					$$ = n;
				}
			| FETCH first_or_next row_or_rows ONLY
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));
					n->limitOffset = NULL;
					n->limitCount = makeIntConst(1, -1);
					n->limitOption = LIMIT_OPTION_COUNT;
					$$ = n;
				}
			| FETCH first_or_next row_or_rows WITH TIES
				{
					SelectLimit *n = (SelectLimit *) palloc(sizeof(SelectLimit));

					n->limitOffset = NULL;
					n->limitCount = makeIntConst(1, -1);
					n->limitOption = LIMIT_OPTION_WITH_TIES;
					$$ = n;
				}
		;

offset_clause:
			OFFSET select_offset_value
				{ $$ = $2; }
			/* SQL:2008 syntax */
			| OFFSET select_fetch_first_value row_or_rows
				{ $$ = $2; }
		;

select_limit_value:
			a_expr									{ $$ = $1; }
			| ALL
				{
					/* LIMIT ALL is equivalent to limitCount = NULL */
					$$ = NULL;
				}
		;

select_offset_value:
			a_expr									{ $$ = $1; }
		;

/*
 * Allowing full expressions without parentheses causes various parsing
 * problems with the trailing ROW/ROWS key words.  SQL spec only calls for
 * <simple value specification>, which is either a literal or a parameter (but
 * an <SQL parameter reference> could be an identifier, bringing up conflicts
 * with ROW/ROWS). We solve this by leveraging the presence of ONLY (see above)
 * to determine whether the expression is missing rather than trying to make it
 * optional in this rule.
 *
 * c_expr covers almost all the spec-required cases (and more), but it doesn't
 * cover signed numeric literals, which are allowed by the spec. So we include
 * those here explicitly. We need FCONST as well as ICONST because values that
 * don't fit in the platform's "long", but do fit in bigint, should still be
 * accepted here. (This is possible in 64-bit Windows as well as all 32-bit
 * builds.)
 */
select_fetch_first_value:
			c_expr									{ $$ = $1; }
			| '+' I_or_F_const
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' I_or_F_const
				{ $$ = doNegate($2, @1); }
		;

I_or_F_const:
			Iconst									{ $$ = makeIntConst($1,@1); }
			| FCONST								{ $$ = makeFloatConst($1,@1); }
		;

/* noise words */
row_or_rows: ROW									{ $$ = 0; }
			| ROWS									{ $$ = 0; }
		;

first_or_next: FIRST_P								{ $$ = 0; }
			| NEXT									{ $$ = 0; }
		;


/*
 * This syntax for group_clause tries to follow the spec quite closely.
 * However, the spec allows only column references, not expressions,
 * which introduces an ambiguity between implicit row constructors
 * (a,b) and lists of column references.
 *
 * We handle this by using the a_expr production for what the spec calls
 * <ordinary grouping set>, which in the spec represents either one column
 * reference or a parenthesized list of column references. Then, we check the
 * top node of the a_expr to see if it's an implicit RowExpr, and if so, just
 * grab and use the list, discarding the node. (this is done in parse analysis,
 * not here)
 *
 * (we abuse the row_format field of RowExpr to distinguish implicit and
 * explicit row constructors; it's debatable if anyone sanely wants to use them
 * in a group clause, but if they have a reason to, we make it possible.)
 *
 * Each item in the group_clause list is either an expression tree or a
 * GroupingSet node of some type.
 */
group_clause:
			GROUP_P BY group_by_list				{ $$ = $3; }


group_by_list:
			group_by_item							{ $$ = list_make1($1); }
			| group_by_list ',' group_by_item		{ $$ = lappend($1,$3); }
		;

group_by_item:
			a_expr									{ $$ = $1; }
			| empty_grouping_set					{ $$ = $1; }
			| cube_clause							{ $$ = $1; }
			| rollup_clause							{ $$ = $1; }
			| grouping_sets_clause					{ $$ = $1; }
		;

empty_grouping_set:
			'(' ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_EMPTY, NIL, @1);
				}
		;

/*
 * These hacks rely on setting precedence of CUBE and ROLLUP below that of '(',
 * so that they shift in these rules rather than reducing the conflicting
 * unreserved_keyword rule.
 */

rollup_clause:
			ROLLUP '(' expr_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_ROLLUP, $3, @1);
				}
		;

cube_clause:
			CUBE '(' expr_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_CUBE, $3, @1);
				}
		;

grouping_sets_clause:
			GROUPING SETS '(' group_by_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_SETS, $4, @1);
				}
		;

having_clause:
			HAVING a_expr							{ $$ = $2; }
		;

for_locking_clause:
			for_locking_items						{ $$ = $1; }
			| FOR READ ONLY							{ $$ = NIL; }
		;

opt_for_locking_clause:
			for_locking_clause						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

for_locking_items:
			for_locking_item						{ $$ = list_make1($1); }
			| for_locking_items for_locking_item	{ $$ = lappend($1, $2); }
		;

for_locking_item:
			for_locking_strength locked_rels_list opt_nowait_or_skip
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $2;
					n->strength = $1;
					n->waitPolicy = $3;
					n->waitTimeout = 0;
					$$ = (Node *) n;
				}
/* OPENTENBASE_ORA_BEGIN */
			| for_locking_strength locked_rels_list opt_wait_second
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $2;
					n->strength = $1;
					n->waitPolicy = LockWaitBlock;
					n->waitTimeout = $3;
					if ($3 == 0)
						n->waitPolicy = LockWaitError;
					$$ = (Node *) n;
				}
/* OPENTENBASE_ORA_END */
		;

for_locking_strength:
			FOR UPDATE 							{ $$ = LCS_FORUPDATE; }
			| FOR NO KEY UPDATE 				{ $$ = LCS_FORNOKEYUPDATE; }
			| FOR SHARE 						{ $$ = LCS_FORSHARE; }
			| FOR KEY SHARE 					{ $$ = LCS_FORKEYSHARE; }
		;

locked_rels_list:
			OF qualified_name_list					{ $$ = $2; }
			| /* EMPTY */							{ $$ = NIL; }
		;


/*
 * We should allow ROW '(' expr_list ')' too, but that seems to require
 * making VALUES a fully reserved word, which will probably break more apps
 * than allowing the noise-word is worth.
 */
values_clause:
			VALUES '(' expr_list ')'
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->valuesLists = list_make1($3);
					$$ = (Node *) n;
				}
			| values_clause ',' '(' expr_list ')'
				{
					SelectStmt *n = (SelectStmt *) $1;
					n->valuesLists = lappend(n->valuesLists, $4);
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *	clauses common to all Optimizable Stmts:
 *		from_clause		- allow list of both JOIN expressions and table names
 *		where_clause	- qualifications for joins or restrictions
 *
 *****************************************************************************/

from_clause:
			FROM
            {
            }
            from_list
            {
            }						{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

from_list:
			table_ref								{ $$ = list_make1($1); }
			| from_list ',' 
			{
			}
			table_ref				{ $$ = lappend($1, $4); }
		;

/*
 * table_ref is where an alias clause can be attached.
 */
table_ref:	relation_expr opt_alias_clause
				{
					$1->alias = $2;
					$$ = (Node *) $1;
				}
			| relation_expr opt_alias_clause tablesample_clause
				{
					RangeTableSample *n = (RangeTableSample *) $3;
					$1->alias = $2;
					/* relation_expr goes inside the RangeTableSample node */
					n->relation = (Node *) $1;
					$$ = (Node *) n;
				}
			| func_table func_alias_clause
				{
					RangeFunction *n = (RangeFunction *) $1;
					n->alias = linitial($2);
					n->coldeflist = lsecond($2);
					$$ = (Node *) n;
				}
			| LATERAL_P func_table func_alias_clause
				{
					RangeFunction *n = (RangeFunction *) $2;
					n->lateral = true;
					n->alias = linitial($3);
					n->coldeflist = lsecond($3);
					$$ = (Node *) n;
				}
			| xmltable opt_alias_clause
				{
					RangeTableFunc *n = (RangeTableFunc *) $1;
					n->alias = $2;
					$$ = (Node *) n;
				}
			| LATERAL_P xmltable opt_alias_clause
				{
					RangeTableFunc *n = (RangeTableFunc *) $2;
					n->lateral = true;
					n->alias = $3;
					$$ = (Node *) n;
				}
			| select_with_parens opt_alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->lateral = false;
					n->subquery = $1;
					n->alias = $2;
					/*
					 * The SQL spec does not permit a subselect
					 * (<derived_table>) without an alias clause,
					 * so we don't either.  This avoids the problem
					 * of needing to invent a unique refname for it.
					 * That could be surmounted if there's sufficient
					 * popular demand, but for now let's just implement
					 * the spec and see if anyone complains.
					 * However, it does seem like a good idea to emit
					 * an error message that's better than "syntax error".
					 */
#ifdef _PG_ORCL_
					if (0 && $2 == NULL)
#else
					if ($2 == NULL)
#endif
					{
						if (IsA($1, SelectStmt) &&
							((SelectStmt *) $1)->valuesLists)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("VALUES in FROM must have an alias"),
									 errhint("For example, FROM (VALUES ...) [AS] foo."),
									 parser_errposition(@1)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("subquery in FROM must have an alias"),
									 errhint("For example, FROM (SELECT ...) [AS] foo."),
									 parser_errposition(@1)));
					}
#ifdef _PG_ORCL_
					if (n->alias == NULL)
					{
						n->alias = makeAnonymousAlias(@1);
					}
#endif
					$$ = (Node *) n;
				}
			| LATERAL_P select_with_parens opt_alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->lateral = true;
					n->subquery = $2;
					n->alias = $3;
					/* same comment as above */
#ifdef _PG_ORCL_
					if (0 && $3 == NULL)
#else
					if ($3 == NULL)
#endif
					{
						if (IsA($2, SelectStmt) &&
							((SelectStmt *) $2)->valuesLists)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("VALUES in FROM must have an alias"),
									 errhint("For example, FROM (VALUES ...) [AS] foo."),
									 parser_errposition(@2)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("subquery in FROM must have an alias"),
									 errhint("For example, FROM (SELECT ...) [AS] foo."),
									 parser_errposition(@2)));
					}
#ifdef _PG_ORCL_
					if (n->alias == NULL)
					{
						n->alias = makeAnonymousAlias(@2);
					}
#endif
					$$ = (Node *) n;
				}
			| joined_table
				{
					$$ = (Node *) $1;
				}
			| '(' joined_table ')' alias_clause
				{
					$2->alias = $4;
					$$ = (Node *) $2;
				}
		;

/*
 * It may seem silly to separate joined_table from table_ref, but there is
 * method in SQL's madness: if you don't do it this way you get reduce-
 * reduce conflicts, because it's not clear to the parser generator whether
 * to expect alias_clause after ')' or not.  For the same reason we must
 * treat 'JOIN' and 'join_type JOIN' separately, rather than allowing
 * join_type to expand to empty; if we try it, the parser generator can't
 * figure out when to reduce an empty join_type right after table_ref.
 *
 * Note that a CROSS JOIN is the same as an unqualified
 * INNER JOIN, and an INNER JOIN/ON has the same shape
 * but a qualification expression to limit membership.
 * A NATURAL JOIN implicitly matches column names between
 * tables and the shape is determined by which columns are
 * in common. We'll collect columns during the later transformations.
 */

joined_table:
			'(' joined_table ')'
				{
					$$ = $2;
				}
			| table_ref CROSS
			{
                	}	
			JOIN table_ref
				{
					/* CROSS JOIN is same as unqualified inner join */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $5;
					n->usingClause = NIL;
					n->quals = NULL;
					$$ = n;
				}
			| table_ref join_type
			{
                	}	
			JOIN table_ref join_qual
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $5;
					if ($6 != NULL && IsA($6, List))
						n->usingClause = (List *) $6; /* USING clause */
					else
						n->quals = $6; /* ON clause */
					$$ = n;
				}
			| table_ref JOIN
			{
                	}	
			table_ref join_qual
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					if ($5 != NULL && IsA($5, List))
						n->usingClause = (List *) $5; /* USING clause */
					else
						n->quals = $5; /* ON clause */
					$$ = n;
				}
			| table_ref NATURAL 
			{
                	}	
			join_type JOIN table_ref
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $4;
					n->isNatural = TRUE;
					n->larg = $1;
					n->rarg = $6;
					n->usingClause = NIL; /* figure out which columns later... */
					n->quals = NULL; /* fill later */
					$$ = n;
				}
			| table_ref NATURAL 
			{
                	}	
			JOIN table_ref
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = TRUE;
					n->larg = $1;
					n->rarg = $5;
					n->usingClause = NIL; /* figure out which columns later... */
					n->quals = NULL; /* fill later */
					$$ = n;
				}
		;

alias_clause:
			AS ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					$$->colnames = $4;
				}
			| AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
				}
			| ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
					$$->colnames = $3;
				}
			| ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
				}
			| opentenbase_ora_normal_alias_unreserved
				{
					$$ = makeNode(Alias);
					$$->aliasname = upcase_identifier($1, strlen($1));
				}
			| opentenbase_ora_normal_alias_unreserved '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = upcase_identifier($1, strlen($1));
					$$->colnames = $3;
				}
			| AS opentenbase_ora_normal_alias_unreserved
				{
					$$ = makeNode(Alias);
					$$->aliasname = upcase_identifier($2, strlen($2));
				}
			| AS opentenbase_ora_normal_alias_unreserved '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = upcase_identifier($2, strlen($2));
					$$->colnames = $4;
				}
		;

opt_alias_clause: alias_clause						{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*
 * func_alias_clause can include both an Alias and a coldeflist, so we make it
 * return a 2-element list that gets disassembled by calling production.
 */
func_alias_clause:
			alias_clause
				{
					$$ = list_make2($1, NIL);
				}
			| AS '(' TableFuncElementList ')'
				{
					$$ = list_make2(NULL, $3);
				}
			| AS ColId '(' TableFuncElementList ')'
				{
					Alias *a = makeNode(Alias);
					a->aliasname = $2;
					$$ = list_make2(a, $4);
				}
			| ColId '(' TableFuncElementList ')'
				{
					Alias *a = makeNode(Alias);
					a->aliasname = $1;
					$$ = list_make2(a, $3);
				}
			| /*EMPTY*/
				{
					$$ = list_make2(NULL, NIL);
				}
		;

join_type:	FULL join_outer							{ $$ = JOIN_FULL; }
			| LEFT_P join_outer						{ $$ = JOIN_LEFT; }
			| RIGHT join_outer						{ $$ = JOIN_RIGHT; }
			| INNER_P								{ $$ = JOIN_INNER; }
		;

/* OUTER is just noise... */
join_outer: OUTER_P									{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* JOIN qualification clauses
 * Possibilities are:
 *	USING ( column list ) allows only unqualified column names,
 *						  which must match between tables.
 *	ON expr allows more general qualifications.
 *
 * We return USING as a List node, while an ON-expr will not be a List.
 */

join_qual:	USING '(' name_list ')'					{ $$ = (Node *) $3; }
			| ON a_expr								{ $$ = $2; }
		;


relation_expr:
			qualified_ref
				{
					/* inheritance query, implicitly */
					$$ = $1;
					$$->inh = true;
					$$->alias = NULL;
#ifdef _PG_ORCL_
					$$->dblinkname = NULL;
					$$->childtablename = NULL;
#endif
				}
			| qualified_ref '*'
				{
					/* inheritance query, explicitly */
					$$ = $1;
					$$->inh = true;
					$$->alias = NULL;

#ifdef _PG_ORCL_
					$$->dblinkname = NULL;
					$$->childtablename = NULL;
#endif
				}
			| ONLY qualified_ref
				{
					/* no inheritance */
					$$ = $2;
					$$->inh = false;
					$$->alias = NULL;

#ifdef _PG_ORCL_
					$$->dblinkname = NULL;
					$$->childtablename = NULL;
#endif
				}
			| ONLY '(' qualified_ref ')'
				{
					/* no inheritance, SQL99-style syntax */
					$$ = $3;
					$$->inh = false;
					$$->alias = NULL;
#ifdef _PG_ORCL_
					$$->dblinkname = NULL;
					$$->childtablename = NULL;
				}
			| qualified_ref '@' name
				{
					/* opentenbase_ora compatible */
					$$ = $1;
					$$->inh = false;
					$$->alias = NULL;
					$$->dblinkname = $3;
					$$->childtablename = NULL;
				}
			| qualified_ref PG_PARTITION '(' Common_IDENT ')'
				{
					/* opentenbase_ora compatible */
					$$ = $1;
					$$->inh = true;
					$$->alias = NULL;
					$$->dblinkname = NULL;
					$$->childtablename = $4;
#endif
				}
		;

relation_expr_list:
			relation_expr							{ $$ = list_make1($1); }
			| relation_expr_list ',' relation_expr	{ $$ = lappend($1, $3); }
		;


/*
 * Given "UPDATE foo set set ...", we have to decide without looking any
 * further ahead whether the first "set" is an alias or the UPDATE's SET
 * keyword.  Since "set" is allowed as a column name both interpretations
 * are feasible.  We resolve the shift/reduce conflict by giving the first
 * relation_expr_opt_alias production a higher precedence than the SET token
 * has, causing the parser to prefer to reduce, in effect assuming that the
 * SET is not an alias.
 */
relation_expr_opt_alias: relation_expr					%prec UMINUS
				{
					$$ = $1;
				}
			| relation_expr ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $2;
					$1->alias = alias;
					$$ = $1;
				}
			| relation_expr AS ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $3;
					$1->alias = alias;
					$$ = $1;
				}
			| relation_expr opentenbase_ora_table_alias_unreserved
				{
					Alias *alias = makeNode(Alias);
                    alias->aliasname = upcase_identifier($2, strlen($2));
                    $1->alias = alias;
                    $$ = $1;
				}
			| relation_expr AS opentenbase_ora_table_alias_unreserved
				{
					Alias *alias = makeNode(Alias);
                    alias->aliasname = upcase_identifier($3, strlen($3));
                    $1->alias = alias;
                    $$ = $1;
				}
		;

/*
 * TABLESAMPLE decoration in a FROM item
 */
tablesample_clause:
			TABLESAMPLE func_name '(' expr_list ')' opt_repeatable_clause
				{
					RangeTableSample *n = makeNode(RangeTableSample);
					/* n->relation will be filled in later */
					n->method = $2;
					n->args = $4;
					n->repeatable = $6;
					n->location = @2;
					$$ = (Node *) n;
				}
		;

opt_repeatable_clause:
			REPEATABLE '(' a_expr ')'	{ $$ = (Node *) $3; }
			| /*EMPTY*/					{ $$ = NULL; }
		;

/*
 * func_table represents a function invocation in a FROM list. It can be
 * a plain function call, like "foo(...)", or a ROWS FROM expression with
 * one or more function calls, "ROWS FROM (foo(...), bar(...))",
 * optionally with WITH ORDINALITY attached.
 * In the ROWS FROM syntax, a column definition list can be given for each
 * function, for example:
 *     ROWS FROM (foo() AS (foo_res_a text, foo_res_b text),
 *                bar() AS (bar_res_a text, bar_res_b text))
 * It's also possible to attach a column definition list to the RangeFunction
 * as a whole, but that's handled by the table_ref production.
 */
func_table: func_expr_windowless opt_ordinality
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->lateral = false;
					n->ordinality = $2;
					n->is_rowsfrom = false;
					n->functions = list_make1(list_make2($1, NIL));
					/* alias and coldeflist are set by table_ref production */
					$$ = (Node *) n;
				}
			| ROWS FROM '(' rowsfrom_list ')' opt_ordinality
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->lateral = false;
					n->ordinality = $6;
					n->is_rowsfrom = true;
					n->functions = $4;
					/* alias and coldeflist are set by table_ref production */
					$$ = (Node *) n;
				}
		;

rowsfrom_item: func_expr_windowless opt_col_def_list
				{ $$ = list_make2($1, $2); }
		;

rowsfrom_list:
			rowsfrom_item						{ $$ = list_make1($1); }
			| rowsfrom_list ',' rowsfrom_item	{ $$ = lappend($1, $3); }
		;

opt_col_def_list: AS '(' TableFuncElementList ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_ordinality: WITH_LA ORDINALITY					{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;


where_clause:
			WHERE a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


/* variant for UPDATE and DELETE */
where_or_current_clause:
			WHERE a_expr							{ $$ = $2; }
			| WHERE CURRENT_P OF cursor_name
				{
					CurrentOfExpr *n = makeNode(CurrentOfExpr);
					/* cvarno is filled in by parse analysis */
					n->cursor_name = $4;
					n->cursor_param = 0;
					$$ = (Node *) n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


OptTableFuncElementList:
			TableFuncElementList				{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TableFuncElementList:
			TableFuncElement
				{
					$$ = list_make1($1);
				}
			| TableFuncElementList ',' TableFuncElement
				{
					$$ = lappend($1, $3);
				}
		;

TableFuncElement:	ColId Typename opt_collate_clause
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collClause = (CollateClause *) $3;
					n->collOid = InvalidOid;
					n->constraints = NIL;
					n->location = @1;
					$$ = (Node *)n;
				}
				| opentenbase_ora_coldef_ident Typename opt_collate_clause
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collClause = (CollateClause *) $3;
					n->collOid = InvalidOid;
					n->constraints = NIL;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

/*
 * XMLTABLE
 */
xmltable:
			XMLTABLE '(' c_expr xmlexists_argument xmltable_column_list_opt ')'
				{
					RangeTableFunc *n = makeNode(RangeTableFunc);
					n->rowexpr = $3;
					n->docexpr = $4;
					n->columns = $5;
					n->namespaces = NIL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| XMLTABLE '(' XMLNAMESPACES '(' xml_namespace_list ')' ','
				c_expr xmlexists_argument xmltable_column_list_opt ')'
				{
					RangeTableFunc *n = makeNode(RangeTableFunc);
					n->rowexpr = $8;
					n->docexpr = $9;
					n->columns = $10;
					n->namespaces = $5;
					n->location = @1;
					$$ = (Node *)n;
				}
		;
xmltable_column_list_opt:
				COLUMNS xmltable_column_list
					{
						$$ = $2;
					}
				| /*EMPTY*/
					{
						/*
						 * opentenbase_ora: Make a pseudocolumn column named column_value
						 * while the sql is without COLUMNS.
						 */
						RangeTableFuncCol *fc = makeNode(RangeTableFuncCol);
						TypeName *t;

						fc->colname = "COLUMN_VALUE";
						fc->for_ordinality = false;
						fc->is_not_null = false;
						fc->colexpr = makeStringConst(".", -1);
						fc->coldefexpr = NULL;
						fc->location = -1;
						t = makeTypeName("XML");
						t->typmods = NIL;
						t->location = -1;
						fc->typeName = t;
						$$ = list_make1(fc);
					}
		;

xmltable_column_list: xmltable_column_el					{ $$ = list_make1($1); }
			| xmltable_column_list ',' xmltable_column_el	{ $$ = lappend($1, $3); }
		;

xmltable_column_el:
			ColId Typename
				{
					RangeTableFuncCol	   *fc = makeNode(RangeTableFuncCol);

					fc->colname = $1;
					fc->for_ordinality = false;
					fc->typeName = $2;
					fc->is_not_null = false;
					fc->colexpr = NULL;
					fc->coldefexpr = NULL;
					fc->location = @1;

					$$ = (Node *) fc;
				}
			| ColId Typename xmltable_column_option_list
				{
					RangeTableFuncCol	   *fc = makeNode(RangeTableFuncCol);
					ListCell		   *option;
					bool				nullability_seen = false;

					fc->colname = $1;
					fc->typeName = $2;
					fc->for_ordinality = false;
					fc->is_not_null = false;
					fc->colexpr = NULL;
					fc->coldefexpr = NULL;
					fc->location = @1;

					foreach(option, $3)
					{
						DefElem   *defel = (DefElem *) lfirst(option);

						if (strcmp(defel->defname, "default") == 0)
						{
							if (fc->coldefexpr != NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("only one DEFAULT value is allowed"),
										 parser_errposition(defel->location)));
							fc->coldefexpr = defel->arg;
						}
						else if (strcmp(defel->defname, "path") == 0)
						{
							if (fc->colexpr != NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("only one PATH value per column is allowed"),
										 parser_errposition(defel->location)));
							fc->colexpr = defel->arg;
						}
						else if (strcmp(defel->defname, "is_not_null") == 0)
						{
							if (nullability_seen)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("conflicting or redundant NULL / NOT NULL declarations for column \"%s\"", fc->colname),
										 parser_errposition(defel->location)));
							fc->is_not_null = intVal(defel->arg);
							nullability_seen = true;
						}
						else
						{
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("unrecognized column option \"%s\"",
											defel->defname),
									 parser_errposition(defel->location)));
						}
					}
					$$ = (Node *) fc;
				}
			| ColId FOR ORDINALITY
				{
					RangeTableFuncCol	   *fc = makeNode(RangeTableFuncCol);

					fc->colname = $1;
					fc->for_ordinality = true;
					/* other fields are ignored, initialized by makeNode */
					fc->location = @1;

					$$ = (Node *) fc;
				}
		;

xmltable_column_option_list:
			xmltable_column_option_el
				{ $$ = list_make1($1); }
			| xmltable_column_option_list xmltable_column_option_el
				{ $$ = lappend($1, $2); }
		;

xmltable_column_option_el:
			Lower_IDENT b_expr
				{ $$ = makeDefElem($1, $2, @1); }
			| DEFAULT b_expr
				{ $$ = makeDefElem("default", $2, @1); }
			| NOT NULL_P
				{ $$ = makeDefElem("is_not_null", (Node *) makeInteger(true), @1); }
			| NULL_P
				{ $$ = makeDefElem("is_not_null", (Node *) makeInteger(false), @1); }
		;

xml_namespace_list:
			xml_namespace_el
				{ $$ = list_make1($1); }
			| xml_namespace_list ',' xml_namespace_el
				{ $$ = lappend($1, $3); }
		;

xml_namespace_el:
			b_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = $1;
					$$->location = @1;
				}
			| DEFAULT b_expr
				{
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = $2;
					$$->location = @1;
				}
		;

/*****************************************************************************
 *
 *	Type syntax
 *		SQL introduces a large amount of type-specific syntax.
 *		Define individual clauses to handle these cases, and use
 *		 the generic case to handle regular type-extensible Postgres syntax.
 *		- thomas 1997-10-10
 *
 *****************************************************************************/

Typename:	SimpleTypename opt_array_bounds
				{
					$$ = $1;
					$$->arrayBounds = $2;
				}
			| SETOF SimpleTypename opt_array_bounds
				{
					$$ = $2;
					$$->arrayBounds = $3;
					$$->setof = TRUE;
				}
			/* SQL standard syntax, currently only one-dimensional */
			| SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger($4));
				}
			| SETOF SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger($5));
					$$->setof = TRUE;
				}
			| SimpleTypename ARRAY
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger(-1));
				}
			| SETOF SimpleTypename ARRAY
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger(-1));
					$$->setof = TRUE;
				}
		;

opt_array_bounds:
			opt_array_bounds '[' ']'
					{  $$ = lappend($1, makeInteger(-1)); }
			| opt_array_bounds '[' Iconst ']'
					{  $$ = lappend($1, makeInteger($3)); }
			| /*EMPTY*/
					{  $$ = NIL; }
		;

SimpleTypename:
			GenericType								{ $$ = $1; }
			| Numeric								{ $$ = $1; }
			| Bit									{ $$ = $1; }
			| Character								{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
			| ConstInterval opt_interval
				{
					$$ = $1;
					$$->typmods = $2;
				}
			| ConstInterval '(' Iconst ')'
				{
					$$ = $1;
					$$->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											 makeIntConst($3, @3));
				}
		;

/* We have a separate ConstTypename to allow defaulting fixed-length
 * types such as CHAR() and BIT() to an unspecified length.
 * SQL9x requires that these default to a length of one, but this
 * makes no sense for constructs like CHAR 'hi' and BIT '0101',
 * where there is an obvious better choice to make.
 * Note that ConstInterval is not included here since it must
 * be pushed up higher in the rules to accommodate the postfix
 * options (e.g. INTERVAL '1' YEAR). Likewise, we have to handle
 * the generic-type-name case in AExprConst to avoid premature
 * reduce/reduce conflicts against function names.
 */
ConstTypename:
			Numeric									{ $$ = $1; }
			| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
		;

/*
 * GenericType covers all type names that don't have special syntax mandated
 * by the standard, including qualified names.  We also allow type modifiers.
 * To avoid parsing conflicts against function invocations, the modifiers
 * have to be shown as expr_list here, but parse analysis will only accept
 * constants for them.
 */
GenericType:
			type_function_name opt_type_modifiers
				{
					if (strcasecmp($1, "varchar2") == 0 && $2 &&
						IsA((Node *) linitial($2), A_Const))
					{
						A_Const *n = (A_Const *) linitial($2);

						if (n->location == -1)
						{
							$$ = makeTypeName("VARCHAR");
						}
						else
						{
							$$ = makeTypeName($1);
						}
					}
					else if (unlikely($1 != NULL && strcasecmp($1, "string") == 0))
					{
						$$ = makeTypeName("VARCHAR2");
					}
					else if (unlikely($1 != NULL && strcasecmp($1, "sys_refcursor") == 0))
					{
						$$ = makeTypeName("REFCURSOR");
					}
					else
					{
						$$ = makeTypeName($1);
					}

					$$->typmods = $2;
					$$->location = @1;
				}
			| type_function_name attrs opt_type_modifiers
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->typmods = $3;
					$$->location = @1;
				}
		;

opt_type_modifiers: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
		;

/*
 * SQL numeric data types
 */
Numeric:	INT_P
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| INTEGER
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| SMALLINT
				{
					$$ = SystemTypeName("int2");
					$$->location = @1;
				}
			| BIGINT
				{
					$$ = SystemTypeName("int8");
					$$->location = @1;
				}
			| REAL
				{
					$$ = SystemTypeName("float4");
					$$->location = @1;
				}
/* OPENTENBASE_ORA_BEGIN */
			| BINARY_FLOAT
				{
						$$ = SystemTypeName("numeric");
						$$->typmods = list_make1(makeIntConst(OPENTENBASE_ORA_BINARY_FLOAT_PRECISION*(-1), -1));	
						$$->typmods = list_concat($$->typmods, 
												list_make2(makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1),
															makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1)));
				}
/* OPENTENBASE_ORA_END */
			| FLOAT_P opt_float
				{
					$$ = $2;
					$$->location = @1;
				}
			| DOUBLE_P PRECISION
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
/* OPENTENBASE_ORA_BEGIN */
			| BINARY_DOUBLE
				{
						$$ = SystemTypeName("numeric");
						$$->typmods = list_make1(makeIntConst(OPENTENBASE_ORA_BINARY_DOUBLE_PRECISION*(-1), -1));	
						$$->typmods = list_concat($$->typmods, 
												list_make2(makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1),
															makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1)));
				}
/* OPENTENBASE_ORA_END */
			| DECIMAL_P opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					if ($2 == NIL)
						$$->typmods = list_make2(makeIntConst(NUMERIC_MAX_PRECISION, -1),
													makeIntConst(OPENTENBASE_ORA_DECIMAL_DEFAULT_SCALE, -1));
					else
						$$->typmods = $2;
					$$->location = @1;
				}
			| DEC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					if ($2 == NIL)
						$$->typmods = list_make2(makeIntConst(NUMERIC_MAX_PRECISION, -1),
													makeIntConst(OPENTENBASE_ORA_DECIMAL_DEFAULT_SCALE, -1));
					else
						$$->typmods = $2;
					$$->location = @1;
				}
			| NUMERIC opt_type_modifiers
				{
					if (enable_simple_numeric == 0)
						$$ = SystemTypeName("numeric");
					else if (enable_simple_numeric == 1)
						$$ = SystemTypeName("numericd");
					else if (enable_simple_numeric == 2)
						$$ = SystemTypeName("numericf");
					else
					{
						if (list_length($2) == 1)
						{
							$$ = SystemTypeName("numericd");
						}

						if (list_length($2) == 2 &&
							IsA(lsecond($2), A_Const))
						{
							A_Const *n = (A_Const *)lsecond($2);

							if (n->val.type == T_Integer &&
								n->val.val.ival == 0)
								$$ = SystemTypeName("numericd");
							else 
								$$ = SystemTypeName("numeric");
						}
					}

					$$->typmods = $2;
					$$->location = @1;
				}
/* OPENTENBASE_ORA_BEGIN */
			| NUMBER opt_number
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
/* OPENTENBASE_ORA_END */
			| BOOLEAN_P
				{
					$$ = SystemTypeName("bool");
					$$->location = @1;
				}
		;

opt_float:	'(' Iconst ')'
				{
					/*
					 * Check FLOAT() precision limits assuming IEEE floating
					 * types - thomas 1997-09-18
					 */
					if ($2 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be at least 1 bit"),
								 parser_errposition(@2)));
					else
					{
						if ($2 <= FLOAT_OPENTENBASE_ORA_MAX_RANGE)
						{
							$$ = SystemTypeName("numeric");
							$$->typmods = list_make1(makeIntConst(($2*OPENTENBASE_ORA_FLOAT_CALCULATION_FACTOR+1)*(-1), @1));
							$$->typmods = list_concat($$->typmods, 
												list_make2(makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1),
															makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1)));
						}
						else
							ereport(ERROR,
									(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("precision for type float must be less than 126"),
									parser_errposition(@2)));
					}
				}
			| /*EMPTY*/
				{
						$$ = SystemTypeName("numeric");
						$$->typmods = list_make3(makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE * OPENTENBASE_ORA_FLOAT_CALCULATION_FACTOR * (-1) - 1, -1), 
												makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1), 
												makeIntConst(FLOAT_OPENTENBASE_ORA_MAX_RANGE*(-1), -1));	
				}
		;

/* OPENTENBASE_ORA_BEGIN */
opt_number: '(' number_expr_list ')'				
			{
				/* check precision and scale range */
				if ($2 != NIL && list_length($2) >=1 && !IsA(linitial($2), A_Star))
				{
					int precision = 1;
					int scale = 1;
					List *typmods = $2;
					if (list_length(typmods) >= 1 && IsA(linitial(typmods), A_Const)
													&& IsA(&(((A_Const *) linitial(typmods))->val), Integer))
						precision = ((A_Const *) linitial(typmods))->val.val.ival;
					if (list_length(typmods) >= 2 && IsA(lsecond(typmods), A_Const)
													&& IsA(&(((A_Const *) lsecond(typmods))->val), Integer))
						scale = ((A_Const *) lsecond(typmods))->val.val.ival;
					if (precision < 1 || precision > 38)
						ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("NUMERIC precision %d must be between 1 and 38", precision),
								parser_errposition(@0)));
					if (scale < -84 || scale > 127)
						ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("NUMERIC scale %d must be between -84 and 127", scale),
								parser_errposition(@0)));
				}
				/* number(*) => number */
				if (list_length($2) == 1 && IsA(linitial($2), A_Star))
				{
					list_free_deep($2);
					$2 = NIL;
				}
				else if (IsA(linitial($2), A_Star))
				{
					int scale = 1;
					List *typmods = $2;
					if (list_length(typmods) >= 2 && IsA(lsecond(typmods), A_Const)
													&& IsA(&(((A_Const *) lsecond(typmods))->val), Integer))
						scale = ((A_Const *) lsecond(typmods))->val.val.ival;
					if (scale < -84 || scale > 127)
						ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("NUMERIC scale %d must be between -84 and 127", scale),
								parser_errposition(@0)));
					$2 = list_delete_first($2);
					$2 = list_concat(list_make1(makeIntConst(NUMERIC_MAX_PRECISION, @1)), $2);
				}
				else if(list_length($2) > 2)
				{
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("invalid NUMERIC type modifier")));
				}
				$$ = $2; 
			}
			| /* EMPTY */					{ $$ = NIL; }
		;

number_expr_list:	a_expr
				{
					$$ = list_make1($1);
				}
			| '*'
				{
					$$ = list_make1((Node *)makeNode(A_Star));
				}
			| number_expr_list ',' a_expr
				{
					$$ = lappend($1, $3);
				}
		;
/* OPENTENBASE_ORA_END */

/*
 * SQL bit-field data types
 * The following implements BIT() and BIT VARYING().
 */
Bit:		BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
				}
		;

/* ConstBit is like Bit except "BIT" defaults to unspecified length */
/* See notes for ConstCharacter, which addresses same issue for "CHAR" */
ConstBit:	BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
					$$->typmods = NIL;
				}
		;

BitWithLength:
			BIT opt_varying '(' expr_list ')'
				{
					char *typname;

					typname = $2 ? "varbit" : "bit";
					$$ = SystemTypeName(typname);
					$$->typmods = $4;
					$$->location = @1;
				}
		;

BitWithoutLength:
			BIT opt_varying
				{
					/* bit defaults to bit(1), varbit to no limit */
					if ($2)
					{
						$$ = SystemTypeName("varbit");
					}
					else
					{
						$$ = SystemTypeName("bit");
						$$->typmods = list_make1(makeIntConst(1, -1));
					}
					$$->location = @1;
				}
		;


/*
 * SQL character data types
 * The following implements CHAR() and VARCHAR().
 */
Character:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					$$ = $1;
				}
		;

ConstCharacter:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					/* Length was not specified so allow to be unrestricted.
					 * This handles problems with fixed-length (bpchar) strings
					 * which in column definitions must default to a length
					 * of one, but should not be constrained if the length
					 * was not specified.
					 */
					$$ = $1;
					$$->typmods = NIL;
				}
		;

CharacterWithLength:  character '(' Iconst opt_char ')'
				{
					$$ = makeTypeName($1);
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
		;

CharacterWithoutLength:	 character
				{
					$$ = makeTypeName($1);
					/* char defaults to char(1), varchar to no limit */
					if (strcmp($1, "BPCHAR") == 0)
						$$->typmods = list_make1(makeIntConst(1, -1));
					$$->location = @1;
				}
		;

character:	CHARACTER opt_varying
										{ $$ = $2 ? "VARCHAR": "BPCHAR"; }
			| CHAR_P opt_varying
										{ $$ = $2 ? "VARCHAR": "BPCHAR"; }
			| VARCHAR
										{ $$ = "VARCHAR"; }
			| RAW
										{ $$ = "RAW"; }
			| LONG RAW
										{ $$ = "LRAW"; }
			| LONG
										{ $$ = "LONG"; }
			| NATIONAL CHARACTER opt_varying
										{ $$ = $3 ? "VARCHAR": "BPCHAR"; }
			| NATIONAL CHAR_P opt_varying
										{ $$ = $3 ? "VARCHAR": "BPCHAR"; }
			| NCHAR opt_varying
										{ $$ = $2 ? "VARCHAR": "BPCHAR"; }
		;

opt_varying:
			VARYING									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/*
 * SQL date/time types
 */
ConstDatetime:
			TIMESTAMP '(' Iconst ')' opt_timezone
				{
					if ($5 == 1)
						$$ = SystemTypeName("timestamptz");
					else if ($5 == 2)
					    $$ = SystemTypeName("timestampltz");
					else
						$$ = SystemTypeName("timestamp");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIMESTAMP opt_timezone
				{
					if ($2 == 1)
						$$ = SystemTypeName("timestamptz");
					else if ($2 == 2)
					    $$ = SystemTypeName("timestampltz");
					else
						$$ = SystemTypeName("timestamp");
					$$->location = @1;
				}
			| TIME '(' Iconst ')' opt_timezone
				{
					if ($5 == 1)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIME opt_timezone
				{
					if ($2 == 1)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->location = @1;
				}
		;

ConstInterval:
			INTERVAL
				{
					$$ = SystemTypeName("interval");
					$$->location = @1;
				}
		;

opt_timezone:
			WITH_LA TIME ZONE						{ $$ = 1; }
			| WITHOUT TIME ZONE						{ $$ = 0; }
			| /*EMPTY*/								{ $$ = 0; }
			| WITH_LA LOCAL TIME ZONE				{ $$ = 2; }
		;

opt_interval:
			YEAR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR), @1)); }
			| YEAR_P '(' Iconst ')'
				{
					$$ = list_make3(makeIntConst(INTERVAL_MASK(YEAR), @1),
									makeIntConst($3, @3),
									makeIntConst(INTERVAL_MASK(YEAR), @1));
				}
			| MONTH_P '(' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(MONTH), @1),
									makeIntConst($3, @3));
				}
			| MONTH_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MONTH), @1)); }
			| DAY_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY), @1)); }
			| DAY_P '(' Iconst ')'
				{
					$$ = list_make3(makeIntConst(INTERVAL_MASK(DAY), @1),
									makeIntConst(INTERVAL_MIX_RREC($3, 0), @3),
									makeIntConst(INTERVAL_MASK(DAY), @1));
				}
			| HOUR_P '(' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(HOUR), @1),
									makeIntConst(INTERVAL_MIX_RREC($3, 0), @3));
				}
			| MINUTE_P '(' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(MINUTE), @1),
									makeIntConst(INTERVAL_MIX_RREC($3, 0), @3));
				}
			| HOUR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR), @1)); }
			| MINUTE_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MINUTE), @1)); }
			| interval_second
				{ $$ = $1; }
			| SECOND_P '(' Iconst ',' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(SECOND), @1),
									makeIntConst(INTERVAL_MIX_RREC($3, $5), @3));
				}
			| YEAR_P TO MONTH_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
												 INTERVAL_MASK(MONTH), @1));
				}
			| YEAR_P '(' Iconst ')' TO MONTH_P
				{
					$$ = list_make3(makeIntConst(INTERVAL_MASK(YEAR) |
												 INTERVAL_MASK(MONTH), @1),
									makeIntConst($3, @3),
									makeIntConst(INTERVAL_MASK(YEAR), @1));
				}
			| DAY_P TO HOUR_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR), @1));
				}
			| DAY_P '(' Iconst ')' TO HOUR_P
				{
					$$ = list_make3(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR), @1),
									makeIntConst(INTERVAL_MIX_RREC($3, 0), @3),
									makeIntConst(INTERVAL_MASK(DAY), @3));
				}
			| DAY_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| DAY_P '(' Iconst ')' TO MINUTE_P
				{
					$$ = list_make3(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1),
									makeIntConst(INTERVAL_MIX_RREC($3, 0), @3),
									makeIntConst(INTERVAL_MASK(DAY), @3));
				}
			| DAY_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| DAY_P '(' Iconst ')' TO interval_second
				{
					$$ = $6;
					linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);

					/* Replace 'day' scale */
					if (list_length($6) == 2)
					{
						A_Const	*con;
						int	sec_frac;

						con = (A_Const *) lsecond_node(A_Const, $6);
						sec_frac  = intVal(&con->val);

						lsecond($$) = makeIntConst(INTERVAL_MIX_RREC($3, sec_frac), @3);
						$$ = lappend($$, makeIntConst(INTERVAL_MASK(DAY) |
														INTERVAL_MASK(SECOND), @3));
					}
					else
					{
						Assert(list_length($6) == 1);
						$$ = lappend($$, makeIntConst(INTERVAL_MIX_RREC($3, 0), @3));
						$$ = lappend($$, makeIntConst(INTERVAL_MASK(DAY), @3));
					}
				}
			| HOUR_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| HOUR_P '(' Iconst ')' TO MINUTE_P
				{
					$$ = list_make3(makeIntConst(INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1),
												 makeIntConst(INTERVAL_MIX_RREC($3, 0), @3),
												 makeIntConst(INTERVAL_MASK(HOUR), @3));
				}
			| HOUR_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| HOUR_P '(' Iconst ')' TO interval_second
				{
					$$ = $6;
					linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);

					/* Replace 'day' scale */
					if (list_length($6) == 2)
					{
						A_Const	*con;
						int	sec_frac;

						con = (A_Const *) lsecond_node(A_Const, $6);
						sec_frac  = intVal(&con->val);

						lsecond($$) = makeIntConst(INTERVAL_MIX_RREC($3, sec_frac), @3);
						$$ = lappend($$, makeIntConst(INTERVAL_MASK(HOUR) |
													  INTERVAL_MASK(SECOND), @3));
					}
					else
					{
						Assert(list_length($6) == 1);
						$$ = lappend($$, makeIntConst(INTERVAL_MIX_RREC($3, 0), @3));
						$$ = lappend($$, makeIntConst(INTERVAL_MASK(HOUR), @3));
					}
				}
			| MINUTE_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| MINUTE_P '(' Iconst ')' TO interval_second
				{
					$$ = $6;
					linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);

					/* Replace 'minute' scale */
					if (list_length($6) == 2)
					{
						A_Const	*con;
						int	sec_frac;

						con = (A_Const *) lsecond_node(A_Const, $6);
						sec_frac  = intVal(&con->val);

						lsecond($$) = makeIntConst(INTERVAL_MIX_RREC($3, sec_frac), @3);
						$$ = lappend($$, makeIntConst(INTERVAL_MASK(MINUTE) |
													  INTERVAL_MASK(SECOND), @3));
					}
					else
					{
						Assert(list_length($6) == 1);
						$$ = lappend($$, makeIntConst(INTERVAL_MIX_RREC($3, 0), @3));
						$$ = lappend($$, makeIntConst(INTERVAL_MASK(MINUTE), @3));
					}
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;

interval_second:
			SECOND_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(SECOND), @1));
				}
			| SECOND_P '(' Iconst ')'
				{
					/*
					 * Length of interval typmods list maybe:
					 * - = 1: no precision
					 * - = 2: only second precision
					 * - = 3: day or/and second precsion. It is only the case
					 *   in opentenbase_ora compatibility. The 3rd cell of this list
					 *   tells what kind precision has.
					 */
					$$ = list_make2(makeIntConst(INTERVAL_MASK(SECOND), @1),
									makeIntConst($3, @3));
				}
		;
cmp_op:
	  '>'
		{$$ = ">";}
	  |'='
		{$$ = "=";}
	  |/*EMPTY*/{$$ = NULL;}

cmp_eq:
	  '='
		{$$ = "=";}
	  |/*EMPTY*/{$$ = NULL;}

/*****************************************************************************
 *
 *	expression grammar
 *
 *****************************************************************************/

/*
 * General expressions
 * This is the heart of the expression syntax.
 *
 * We have two expression types: a_expr is the unrestricted kind, and
 * b_expr is a subset that must be used in some places to avoid shift/reduce
 * conflicts.  For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr"
 * because that use of AND conflicts with AND as a boolean operator.  So,
 * b_expr is used in BETWEEN and we remove boolean keywords from b_expr.
 *
 * Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
 * always be used by surrounding it with parens.
 *
 * c_expr is all the productions that are common to a_expr and b_expr;
 * it's factored out just to eliminate redundant coding.
 *
 * Be careful of productions involving more than one terminal token.
 * By default, bison will assign such productions the precedence of their
 * last terminal, but in nearly all cases you want it to be the precedence
 * of the first terminal instead; otherwise you will not get the behavior
 * you expect!  So we use %prec annotations freely to set precedences.
 */
in_typecast_scope:
    {
        ORA_ENTER_SCOPE(ORA_TYPECAST_SCOPE, pg_yyget_extra(yyscanner));
    }
out_typecast_scope:
    {
        ORA_LEAVE_SCOPE(ORA_TYPECAST_SCOPE, pg_yyget_extra(yyscanner));
    }
a_expr:		c_expr									{ $$ = $1; }
			| a_expr TYPECAST Typename
					{
						base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
						$$ = makeTypeCast($1, $3, @2);
						if (IS_IN_SCOPE(ORA_TYPECAST_SCOPE, yyextra))
							ORA_LEAVE_SCOPE(ORA_TYPECAST_SCOPE, pg_yyget_extra(yyscanner));
						if ((IS_SCOPE_SET(ORA_SELECT_SCOPE, yyextra) || IS_SCOPE_SET(ORA_INSERT_SCOPE, yyextra) ||
							IS_SCOPE_SET(ORA_UPDATE_SCOPE, yyextra) || IS_SCOPE_SET(ORA_MERGE_SCOPE, yyextra) ||
							IS_SCOPE_SET(ORA_SORT_SCOPE, yyextra)) && (yychar == BYTE_P || yychar == CHARACTER))
						{
							yychar = IDENT;
							yylval.str = upcase_identifier(yylval.str, strlen(yylval.str));
						}
					}
			| a_expr COLLATE lower_any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = $1;
					n->collname = $3;
					n->location = @2;
					$$ = (Node *) n;
				}
			| a_expr AT TIME ZONE a_expr			%prec AT
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("timezone"),
											   list_make2($5, $1),
											   @2);
				}
		/*
		 * These operators must be called out explicitly in order to make use
		 * of bison's automatic operator-precedence handling.  All other
		 * operator names are handled by the generic productions using "Op",
		 * below; and all those operators will have the same precedence.
		 *
		 * If you add more explicitly-known operators, be sure to add them
		 * also to b_expr and to the MathOp list below.
		 */
			| '+' a_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' a_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| a_expr '+' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| a_expr '-' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| a_expr '*' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| a_expr '/' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| a_expr '%' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr MOD a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr '^' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| a_expr '<' cmp_op a_expr
				{
					if ($3 && $3[0] == '>')
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $4, @2);
					else if ($3 && $3[0] == '=')
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $4, @2);
					else
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $4, @2);
				}
			| a_expr '>' cmp_eq a_expr
				{
					if ($3 && $3[0] == '=')
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $4, @2);
					else
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $4, @2);
				}
			| a_expr '=' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| a_expr LESS_EQUALS a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
			| a_expr GREATER_EQUALS a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
			| a_expr NOT_EQUALS a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }
			| a_expr CONCATENATION a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "||", $1, $3, @2); }
			| a_expr qual_Op a_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op a_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| a_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }

			| a_expr AND a_expr
				{ $$ = makeAndExpr($1, $3, @2); }
			| a_expr OR a_expr
				{ $$ = makeOrExpr($1, $3, @2); }
			| NOT a_expr
				{ $$ = makeNotExpr($2, @1); }
			| NOT_LA a_expr						%prec NOT
				{ $$ = makeNotExpr($2, @1); }

			| a_expr LIKE a_expr
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "~~",
												   $1, $3, @2);
				}
			| a_expr LIKE a_expr ESCAPE a_expr					%prec LIKE
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($3, $5),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "~~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA LIKE a_expr							%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "!~~",
												   $1, $4, @2);
				}
			| a_expr NOT_LA LIKE a_expr ESCAPE a_expr			%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "!~~",
												   $1, (Node *) n, @2);
				}
			| a_expr ILIKE a_expr
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "~~*",
												   $1, $3, @2);
				}
			| a_expr ILIKE a_expr ESCAPE a_expr					%prec ILIKE
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($3, $5),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "~~*",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA ILIKE a_expr						%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "!~~*",
												   $1, $4, @2);
				}
			| a_expr NOT_LA ILIKE a_expr ESCAPE a_expr			%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "!~~*",
												   $1, (Node *) n, @2);
				}

			| a_expr SIMILAR TO a_expr							%prec SIMILAR
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($4, makeNullAConst(-1)),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "~",
												   $1, (Node *) n, @2);
				}
			| a_expr SIMILAR TO a_expr ESCAPE a_expr			%prec SIMILAR
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA SIMILAR TO a_expr					%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($5, makeNullAConst(-1)),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "!~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr		%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($5, $7),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "!~",
												   $1, (Node *) n, @2);
				}

			/* NullTest clause
			 * Define SQL-style Null test clause.
			 * Allow two forms described in the standard:
			 *	a IS NULL
			 *	a IS NOT NULL
			 * Allow two SQL extensions
			 *	a ISNULL
			 *	a NOTNULL
			 */
			| a_expr IS NULL_P							%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr ISNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr IS NOT NULL_P						%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr NOTNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| row OVERLAPS row
				{
					if (list_length($1) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on left side of OVERLAPS expression"),
								 parser_errposition(@1)));
					if (list_length($3) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on right side of OVERLAPS expression"),
								 parser_errposition(@3)));
					$$ = (Node *) makeFuncCall(SystemFuncName("overlaps"),
											   list_concat($1, $3),
											   @2);
				}
			| a_expr IS TRUE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_TRUE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT TRUE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_TRUE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS FALSE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_FALSE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT FALSE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_FALSE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS UNKNOWN							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_UNKNOWN;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT UNKNOWN						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_UNKNOWN;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS DISTINCT FROM a_expr			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| a_expr IS NOT DISTINCT FROM a_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| a_expr IS OF '(' type_list ')'			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| a_expr IS NOT OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			| a_expr BETWEEN opt_asymmetric b_expr AND a_expr		%prec BETWEEN
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN,
												   "BETWEEN",
												   $1,
												   (Node *) list_make2($4, $6),
												   @2);
				}
			| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN,
												   "NOT BETWEEN",
												   $1,
												   (Node *) list_make2($5, $7),
												   @2);
				}
			| a_expr BETWEEN SYMMETRIC b_expr AND a_expr			%prec BETWEEN
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN_SYM,
												   "BETWEEN SYMMETRIC",
												   $1,
												   (Node *) list_make2($4, $6),
												   @2);
				}
			| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr		%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN_SYM,
												   "NOT BETWEEN SYMMETRIC",
												   $1,
												   (Node *) list_make2($5, $7),
												   @2);
				}
			| a_expr IN_P in_expr
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($3, SubLink))
					{
						/* generate foo = ANY (subquery) */
						SubLink *n = (SubLink *) $3;
						n->subLinkType = ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		/* show it's IN not = ANY */
						n->location = @2;
						$$ = (Node *)n;
					}
					else
					{
						/* generate scalar IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, $3, @2);
					}
				}
			| a_expr NOT_LA IN_P in_expr						%prec NOT_LA
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($4, SubLink))
					{
						/* generate NOT (foo = ANY (subquery)) */
						/* Make an = ANY node */
						SubLink *n = (SubLink *) $4;
						n->subLinkType = ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		/* show it's IN not = ANY */
						n->location = @2;
						/* Stick a NOT on top; must have same parse location */
						$$ = makeNotExpr((Node *) n, @2);
					}
					else
					{
						/* generate scalar NOT IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, $4, @2);
					}
				}
			| a_expr IN_P c_expr_single_in
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, (Node *)list_make1($3), @2);
				}
			| a_expr NOT_LA IN_P c_expr_single_in
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, (Node *)list_make1($4), @2);
				}
			| a_expr subquery_Op sub_type select_with_parens	%prec Op
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = $3;
					n->subLinkId = 0;
					n->testexpr = $1;
					n->operName = $2;
					n->subselect = $4;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr subquery_Op sub_type '(' expr_list ')'     %prec Op
				{
					Node *n = NULL;
					Node* first = linitial($5);

					if (list_length($5) == 1 && IsA(first, A_ArrayExpr))
						n = linitial($5);
					else
					{
						Node *arr_expr = makeAArrayExpr($5, @1);
						A_ArrayExpr *expr = castNode(A_ArrayExpr, arr_expr);
						expr->location = @1;
						n = (Node*)expr;
					}
					if ($3 == ANY_SUBLINK)
						$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, n, @2);
					else
						$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, n, @2);
				}
			| a_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| a_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = makeNotExpr(makeXmlExpr(IS_DOCUMENT, NULL, NIL,
												 list_make1($1), @2),
									 @2);
				}
			| a_expr MULTISET UNION all_or_distinct a_expr
				{
					MultiSetExpr *muls = makeNode(MultiSetExpr);
					muls->location = @1;
					muls->setop = SETOP_UNION;
					muls->lexpr = $1;
					muls->rexpr = $5;
					muls->all = $4;
					$$ = (Node *) muls;
				}
			| a_expr MULTISET INTERSECT all_or_distinct a_expr
				{
					MultiSetExpr *muls = makeNode(MultiSetExpr);
					muls->location = @1;
					muls->setop = SETOP_INTERSECT;
					muls->lexpr = $1;
					muls->rexpr = $5;
					muls->all = $4;
					$$ = (Node *) muls;
				}
			| a_expr MULTISET EXCEPT all_or_distinct a_expr
				{
					MultiSetExpr *muls = makeNode(MultiSetExpr);
					muls->location = @1;
					muls->setop = SETOP_EXCEPT;
					muls->lexpr = $1;
					muls->rexpr = $5;
					muls->all = $4;
					$$ = (Node *) muls;
				}
		   
			| DEFAULT
				{
					/*
					 * The SQL spec only allows DEFAULT in "contextually typed
					 * expressions", but for us, it's easier to allow it in
					 * any a_expr and then throw error during parse analysis
					 * if it's in an inappropriate context.  This way also
					 * lets us say something smarter than "syntax error".
					 */
					SetToDefault *n = makeNode(SetToDefault);
					/* parse analysis will fill in the rest */
					n->location = @1;
					$$ = (Node *)n;
				}
		;

/*
 * Restricted expressions
 *
 * b_expr is a subset of the complete expression syntax defined by a_expr.
 *
 * Presently, AND, NOT, IS, and IN are the a_expr keywords that would
 * cause trouble in the places where b_expr is used.  For simplicity, we
 * just eliminate all the boolean-keyword-operator productions from b_expr.
 */
b_expr:		c_expr
				{ $$ = $1; }
			| b_expr TYPECAST Typename
				{
					base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
					$$ = makeTypeCast($1, $3, @2);
					if (IS_IN_SCOPE(ORA_TYPECAST_SCOPE, yyextra))
						ORA_LEAVE_SCOPE(ORA_TYPECAST_SCOPE, pg_yyget_extra(yyscanner));
				}
			| '+' b_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' b_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| b_expr '+' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| b_expr '-' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| b_expr '*' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| b_expr '/' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| b_expr '%' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| b_expr '^' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| b_expr '<' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| b_expr '>' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| b_expr '=' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| b_expr MOD b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| b_expr LESS_EQUALS b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
			| b_expr GREATER_EQUALS b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
			| b_expr NOT_EQUALS b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }
			| b_expr CONCATENATION b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "||", $1, $3, @2); }
			| b_expr qual_Op b_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op b_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| b_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }
			| b_expr IS DISTINCT FROM b_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| b_expr IS NOT DISTINCT FROM b_expr	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| b_expr IS OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| b_expr IS NOT OF '(' type_list ')'	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			| b_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| b_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = makeNotExpr(makeXmlExpr(IS_DOCUMENT, NULL, NIL,
												 list_make1($1), @2),
									 @2);
				}
		;

/*
 * Productions that can be used in both a_expr and b_expr.
 *
 * Note: productions that refer recursively to a_expr or b_expr mostly
 * cannot appear here.	However, it's OK to refer to a_exprs that occur
 * inside parentheses, such as function arguments; that cannot introduce
 * ambiguity to the b_expr syntax.
 */
c_expr:		columnref								{ $$ = $1; }
			| AexprConst							{ $$ = $1; }
			| PARAM opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = check_indirection($2, yyscanner);
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| '(' a_expr ')' opt_indirection
				{
					if ($4)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = $2;
						n->indirection = check_indirection($4, yyscanner);
						$$ = (Node *)n;
					}
					else if (operator_precedence_warning)
					{
						/*
						 * If precedence warnings are enabled, insert
						 * AEXPR_PAREN nodes wrapping all explicitly
						 * parenthesized subexpressions; this prevents bogus
						 * warnings from being issued when the ordering has
						 * been forced by parentheses.  Take care that an
						 * AEXPR_PAREN node has the same exprLocation as its
						 * child, so as not to cause surprising changes in
						 * error cursor positioning.
						 *
						 * In principle we should not be relying on a GUC to
						 * decide whether to insert AEXPR_PAREN nodes.
						 * However, since they have no effect except to
						 * suppress warnings, it's probably safe enough; and
						 * we'd just as soon not waste cycles on dummy parse
						 * nodes if we don't have to.
						 */
						$$ = (Node *) makeA_Expr(AEXPR_PAREN, NIL, $2, NULL,
												 exprLocation($2));
					}
					else
						$$ = $2;
				}
			| case_expr
				{ $$ = $1; }
			| func_expr
				{ $$ = $1; }
			| select_with_parens			%prec UMINUS
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					$$ = (Node *)n;
				}
			| select_with_parens indirection
				{
					/*
					 * Because the select_with_parens nonterminal is designed
					 * to "eat" as many levels of parens as possible, the
					 * '(' a_expr ')' opt_indirection production above will
					 * fail to match a sub-SELECT with indirection decoration;
					 * the sub-SELECT won't be regarded as an a_expr as long
					 * as there are parens around it.  To support applying
					 * subscripting or field selection to a sub-SELECT result,
					 * we need this redundant-looking production.
					 */
					SubLink *n = makeNode(SubLink);
					A_Indirection *a = makeNode(A_Indirection);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					a->arg = (Node *)n;
					a->indirection = check_indirection($2, yyscanner);
					$$ = (Node *)a;
				}
			| EXISTS select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXISTS_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = ARRAY_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY array_expr
				{
					A_ArrayExpr *n = castNode(A_ArrayExpr, $2);
					/* point outermost A_ArrayExpr to the ARRAY keyword */
					n->location = @1;
					$$ = (Node *)n;
				}
			| explicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_EXPLICIT_CALL; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| implicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_IMPLICIT_CAST; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| GROUPING '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  g->kind = GROUPING_FUNC;
				  $$ = (Node *)g;
			  }
			| GROUPING_ID '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  g->kind = GROUPING_FUNC_ID;
				  $$ = (Node *)g;
			  }			  
		;

c_expr_single_in:
			columnref			{ $$ = $1; }
			| AexprConst						{ $$ = $1; }
			| func_expr							{ $$ = $1; }
			| case_expr                                             { $$ = $1; }
			| '-' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", 0, $2, @2); }
			| '+' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", 0, $2, @2); }
			| c_expr_single_in CONCATENATION c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "||", $1, $3, @2); }
			| c_expr_single_in '+' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| c_expr_single_in '-' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| c_expr_single_in '*' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| c_expr_single_in '/' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| c_expr_single_in '%' c_expr_single_in
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
		;
a_expr_spec_alais:
            c_expr { $$ = $1; }
            |a_expr TYPECAST Typename
					{
						base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
						$$ = makeTypeCast($1, $3, @2);
						if (IS_IN_SCOPE(ORA_TYPECAST_SCOPE, yyextra))
							ORA_LEAVE_SCOPE(ORA_TYPECAST_SCOPE, pg_yyget_extra(yyscanner));
						if ((IS_SCOPE_SET(ORA_SELECT_SCOPE, yyextra) || IS_SCOPE_SET(ORA_INSERT_SCOPE, yyextra) ||
							IS_SCOPE_SET(ORA_UPDATE_SCOPE, yyextra) || IS_SCOPE_SET(ORA_MERGE_SCOPE, yyextra) ||
							IS_SCOPE_SET(ORA_SORT_SCOPE, yyextra)) && (yychar == BYTE_P || yychar == CHARACTER))
						{
							yychar = IDENT;
							yylval.str = upcase_identifier(yylval.str, strlen(yylval.str));
						}
					}
			| a_expr COLLATE lower_any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = $1;
					n->collname = $3;
					n->location = @2;
					$$ = (Node *) n;
				}
			| a_expr AT TIME ZONE a_expr_spec_alais			%prec AT
				{
					$$ = (Node *) makeFuncCall(OpenTenBaseOraFuncName("timezone"),
											   list_make2($5, $1),
											   @2);
				}
		/*
		 * These operators must be called out explicitly in order to make use
		 * of bison's automatic operator-precedence handling.  All other
		 * operator names are handled by the generic productions using "Op",
		 * below; and all those operators will have the same precedence.
		 *
		 * If you add more explicitly-known operators, be sure to add them
		 * also to b_expr and to the MathOp list below.
		 */
			| '+' a_expr_spec_alais					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' a_expr_spec_alais					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| a_expr '+' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| a_expr '-' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| a_expr '*' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| a_expr '/' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| a_expr '%' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr MOD a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr '^' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| a_expr '<' cmp_op a_expr_spec_alais
				{
					if ($3 && $3[0] == '>')
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $4, @2);
					else if ($3 && $3[0] == '=')
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $4, @2);
					else
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $4, @2);
				}
			| a_expr '>' cmp_eq a_expr_spec_alais
				{
					if ($3 && $3[0] == '=')
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $4, @2);
					else
						$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $4, @2);
				}
			| a_expr '=' a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| a_expr LESS_EQUALS a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
			| a_expr GREATER_EQUALS a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
			| a_expr NOT_EQUALS a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }
			| a_expr CONCATENATION a_expr_spec_alais
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "||", $1, $3, @2); }
			| a_expr qual_Op a_expr_spec_alais				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op a_expr_spec_alais					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| a_expr AND a_expr_spec_alais
				{ $$ = makeAndExpr($1, $3, @2); }
			| a_expr OR a_expr_spec_alais
				{ $$ = makeOrExpr($1, $3, @2); }
			| NOT a_expr_spec_alais
				{ $$ = makeNotExpr($2, @1); }
			| NOT_LA a_expr_spec_alais						%prec NOT
				{ $$ = makeNotExpr($2, @1); }

			| a_expr LIKE a_expr_spec_alais
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "~~",
												   $1, $3, @2);
				}
			| a_expr LIKE a_expr ESCAPE a_expr_spec_alais					%prec LIKE
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($3, $5),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "~~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA LIKE a_expr_spec_alais							%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "!~~",
												   $1, $4, @2);
				}
			| a_expr NOT_LA LIKE a_expr ESCAPE a_expr_spec_alais			%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_LIKE, "!~~",
												   $1, (Node *) n, @2);
				}
			| a_expr ILIKE a_expr_spec_alais
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "~~*",
												   $1, $3, @2);
				}
			| a_expr ILIKE a_expr ESCAPE a_expr_spec_alais					%prec ILIKE
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($3, $5),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "~~*",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA ILIKE a_expr_spec_alais						%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "!~~*",
												   $1, $4, @2);
				}
			| a_expr NOT_LA ILIKE a_expr ESCAPE a_expr_spec_alais			%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("like_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_ILIKE, "!~~*",
												   $1, (Node *) n, @2);
				}

			| a_expr SIMILAR TO a_expr_spec_alais							%prec SIMILAR
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($4, makeNullAConst(-1)),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "~",
												   $1, (Node *) n, @2);
				}
			| a_expr SIMILAR TO a_expr ESCAPE a_expr_spec_alais			%prec SIMILAR
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($4, $6),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA SIMILAR TO a_expr_spec_alais					%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($5, makeNullAConst(-1)),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "!~",
												   $1, (Node *) n, @2);
				}
			| a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr_spec_alais		%prec NOT_LA
				{
					FuncCall *n = makeFuncCall(SystemFuncName("similar_escape"),
											   list_make2($5, $7),
											   @2);
					$$ = (Node *) makeSimpleA_Expr(AEXPR_SIMILAR, "!~",
												   $1, (Node *) n, @2);
				}

			/* NullTest clause
			 * Define SQL-style Null test clause.
			 * Allow two forms described in the standard:
			 *	a IS NULL
			 *	a IS NOT NULL
			 * Allow two SQL extensions
			 *	a ISNULL
			 *	a NOTNULL
			 */
			| a_expr IS NULL_P							%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr ISNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr IS NOT NULL_P						%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr NOTNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					n->location = @2;
					$$ = (Node *)n;
				}
			| row OVERLAPS row
				{
					if (list_length($1) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on left side of OVERLAPS expression"),
								 parser_errposition(@1)));
					if (list_length($3) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("wrong number of parameters on right side of OVERLAPS expression"),
								 parser_errposition(@3)));
					$$ = (Node *) makeFuncCall(SystemFuncName("overlaps"),
											   list_concat($1, $3),
											   @2);
				}
			| a_expr IS TRUE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_TRUE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT TRUE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_TRUE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS FALSE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_FALSE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT FALSE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_FALSE;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS UNKNOWN							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_UNKNOWN;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS NOT UNKNOWN						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_UNKNOWN;
					b->location = @2;
					$$ = (Node *)b;
				}
			| a_expr IS DISTINCT FROM a_expr_spec_alais			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| a_expr IS NOT DISTINCT FROM a_expr_spec_alais		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| a_expr IS OF '(' type_list ')'			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| a_expr IS NOT OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			| a_expr BETWEEN opt_asymmetric b_expr AND a_expr_spec_alais		%prec BETWEEN
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN,
												   "BETWEEN",
												   $1,
												   (Node *) list_make2($4, $6),
												   @2);
				}
			| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr_spec_alais %prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN,
												   "NOT BETWEEN",
												   $1,
												   (Node *) list_make2($5, $7),
												   @2);
				}
			| a_expr BETWEEN SYMMETRIC b_expr AND a_expr_spec_alais			%prec BETWEEN
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN_SYM,
												   "BETWEEN SYMMETRIC",
												   $1,
												   (Node *) list_make2($4, $6),
												   @2);
				}
			| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr_spec_alais		%prec NOT_LA
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN_SYM,
												   "NOT BETWEEN SYMMETRIC",
												   $1,
												   (Node *) list_make2($5, $7),
												   @2);
				}
			| a_expr IN_P in_expr
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($3, SubLink))
					{
						/* generate foo = ANY (subquery) */
						SubLink *n = (SubLink *) $3;
						n->subLinkType = ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		/* show it's IN not = ANY */
						n->location = @2;
						$$ = (Node *)n;
					}
					else
					{
						/* generate scalar IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, $3, @2);
					}
				}
			| a_expr NOT_LA IN_P in_expr						%prec NOT_LA
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($4, SubLink))
					{
						/* generate NOT (foo = ANY (subquery)) */
						/* Make an = ANY node */
						SubLink *n = (SubLink *) $4;
						n->subLinkType = ANY_SUBLINK;
						n->subLinkId = 0;
						n->testexpr = $1;
						n->operName = NIL;		/* show it's IN not = ANY */
						n->location = @2;
						/* Stick a NOT on top; must have same parse location */
						$$ = makeNotExpr((Node *) n, @2);
					}
					else
					{
						/* generate scalar NOT IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, $4, @2);
					}
				}
			| a_expr IN_P c_expr_single_in
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, (Node *)list_make1($3), @2);
				}
			| a_expr NOT_LA IN_P c_expr_single_in
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, (Node *)list_make1($4), @2);
				}
			| a_expr subquery_Op sub_type select_with_parens	%prec Op
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = $3;
					n->subLinkId = 0;
					n->testexpr = $1;
					n->operName = $2;
					n->subselect = $4;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr subquery_Op sub_type '(' expr_list ')'     %prec Op
				{
					Node *n = NULL;
					Node* first = linitial($5);

					if (list_length($5) == 1 && IsA(first, A_ArrayExpr))
						n = linitial($5);
					else
					{
						Node *arr_expr = makeAArrayExpr($5, @1);
						A_ArrayExpr *expr = castNode(A_ArrayExpr, arr_expr);
						expr->location = @1;
						n = (Node*)expr;
					}
					if ($3 == ANY_SUBLINK)
						$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, n, @2);
					else
						$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, n, @2);
				}
			| a_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| a_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = makeNotExpr(makeXmlExpr(IS_DOCUMENT, NULL, NIL,
												 list_make1($1), @2),
									 @2);
				}
			| DEFAULT
				{
					/*
					 * The SQL spec only allows DEFAULT in "contextually typed
					 * expressions", but for us, it's easier to allow it in
					 * any a_expr and then throw error during parse analysis
					 * if it's in an inappropriate context.  This way also
					 * lets us say something smarter than "syntax error".
					 */
					SetToDefault *n = makeNode(SetToDefault);
					/* parse analysis will fill in the rest */
					n->location = @1;
					$$ = (Node *)n;
				}
		;
/*
* This c_expr as select target supporting special alias without as
* It change AexprConst to AexprConstAlias
*/
c_expr_spec_alais:
			columnref								{ $$ = $1; }
			| AexprConstAlias                                                    { $$ = $1; }
			| PARAM opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = check_indirection($2, yyscanner);
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| '(' a_expr ')' opt_indirection
				{
					if ($4)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = $2;
						n->indirection = check_indirection($4, yyscanner);
						$$ = (Node *)n;
					}
					else if (operator_precedence_warning)
					{
						/*
						 * If precedence warnings are enabled, insert
						 * AEXPR_PAREN nodes wrapping all explicitly
						 * parenthesized subexpressions; this prevents bogus
						 * warnings from being issued when the ordering has
						 * been forced by parentheses.  Take care that an
						 * AEXPR_PAREN node has the same exprLocation as its
						 * child, so as not to cause surprising changes in
						 * error cursor positioning.
						 *
						 * In principle we should not be relying on a GUC to
						 * decide whether to insert AEXPR_PAREN nodes.
						 * However, since they have no effect except to
						 * suppress warnings, it's probably safe enough; and
						 * we'd just as soon not waste cycles on dummy parse
						 * nodes if we don't have to.
						 */
						$$ = (Node *) makeA_Expr(AEXPR_PAREN, NIL, $2, NULL,
												 exprLocation($2));
					}
					else
						$$ = $2;
				}
			| case_expr
				{ $$ = $1; }
			| func_expr
				{ $$ = $1; }
			| select_with_parens			%prec UMINUS
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					$$ = (Node *)n;
				}
			| select_with_parens indirection
				{
					/*
					 * Because the select_with_parens nonterminal is designed
					 * to "eat" as many levels of parens as possible, the
					 * '(' a_expr ')' opt_indirection production above will
					 * fail to match a sub-SELECT with indirection decoration;
					 * the sub-SELECT won't be regarded as an a_expr as long
					 * as there are parens around it.  To support applying
					 * subscripting or field selection to a sub-SELECT result,
					 * we need this redundant-looking production.
					 */
					SubLink *n = makeNode(SubLink);
					A_Indirection *a = makeNode(A_Indirection);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					a->arg = (Node *)n;
					a->indirection = check_indirection($2, yyscanner);
					$$ = (Node *)a;
				}
			| EXISTS select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXISTS_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = ARRAY_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY array_expr
				{
					A_ArrayExpr *n = castNode(A_ArrayExpr, $2);
					/* point outermost A_ArrayExpr to the ARRAY keyword */
					n->location = @1;
					$$ = (Node *)n;
				}
			| explicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_EXPLICIT_CALL; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| implicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_IMPLICIT_CAST; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| GROUPING '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  g->kind = GROUPING_FUNC;
				  $$ = (Node *)g;
			  }
			| GROUPING_ID '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  g->kind = GROUPING_FUNC_ID;
				  $$ = (Node *)g;
			  } 
		;
func_application: func_name '(' ')'
				{
					$$ = (Node *) makeFuncCall($1, NIL, @1);
				}
			| func_name '(' func_arg_list opt_sort_clause ')' opt_indirection_ora
				{
					char   *nspname = NULL;
					char   *objname = NULL;
					bool    is_decode = false;

					objname = strVal(llast($1));
					if (strcasecmp(objname, "decode") == 0)
					{
						DeconstructQualifiedName($1, &nspname, &objname);
						if (nspname == NULL || strcasecmp(nspname, "opentenbase_ora") == 0)
						{
							if (list_length($3) < 3)
								ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR), errmsg("Not engouh parameters for \"decode\" function"), parser_errposition(@1)));

							is_decode = true;
							$$ = reparse_decode_func($3, @3);
						}
					}

					if (!is_decode)
					{
						FuncCall *n = makeFuncCall($1, $3, @1);
						n->agg_order = $4;
						n->indirection = $6;
						$$ = (Node *)n;
					}
				}
			| func_name '(' func_arg_list opt_sort_clause ON OVERFLOW ERROR_P ')'
				{
					FuncCall *n = makeFuncCall($1, lappend($3, makeIntConst(0, @7)), @1);
					n->agg_order = $4;
					$$ = (Node *)n;
				}
			| func_name '(' func_arg_list opt_sort_clause ON OVERFLOW TRUNCATE opt_truncation_indicator opt_with_count_expr ')'
				{
					FuncCall *n = makeFuncCall($1, lappend($3, makeIntConst(1, @7)) , @1);
					n->agg_order = $4;

					if ($8 != NULL)
					{
						n->args = lappend(n->args, $8);
					}

					if ($9 != NULL)
					{
						n->args = lappend(n->args, $9);
					}

					$$ = (Node *)n;
				}
			| func_name '(' VARIADIC func_arg_expr opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, list_make1($4), @1);
					n->func_variadic = TRUE;
					n->agg_order = $5;
					$$ = (Node *)n;
				}
			| func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, lappend($3, $6), @1);
					n->func_variadic = TRUE;
					n->agg_order = $7;
					$$ = (Node *)n;
				}
			| func_name '(' ALL func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, $4, @1);
					n->agg_order = $5;
					/* Ideally we'd mark the FuncCall node to indicate
					 * "must be an aggregate", but there's no provision
					 * for that in FuncCall at the moment.
					 */
					$$ = (Node *)n;
				}
			| func_name '(' ALL func_arg_list opt_sort_clause ON OVERFLOW ERROR_P ')'
				{
					FuncCall *n = makeFuncCall($1, lappend($4, makeIntConst(0,@8)), @1);
					n->agg_order = $5;
					$$ = (Node *)n;
				}
			| func_name '(' ALL func_arg_list opt_sort_clause ON OVERFLOW TRUNCATE opt_truncation_indicator opt_with_count_expr ')'
				{
					/*FuncCall *n = makeFuncCall($1, $4, @1);*/
					FuncCall *n = makeFuncCall($1, lappend($4, makeIntConst(1, @8)) , @1);
					n->agg_order = $5;

					if ($9 != NULL)
					{
						n->args = lappend(n->args, $9);
					}

					if ($10 != NULL)
					{
						n->args = lappend(n->args, $10);
					}

					$$ = (Node *)n;
				}
			| func_name '(' distinct_clause func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeFuncCall($1, $4, @1);
					n->agg_order = $5;
					n->agg_distinct = TRUE;
					$$ = (Node *)n;
				}
			| func_name '(' '*' ')'
				{
					/*
					 * We consider AGGREGATE(*) to invoke a parameterless
					 * aggregate.  This does the right thing for COUNT(*),
					 * and there are no other aggregates in SQL that accept
					 * '*' as parameter.
					 *
					 * The FuncCall node is also marked agg_star = true,
					 * so that later processing can detect what the argument
					 * really was.
					 */
					FuncCall *n = makeFuncCall($1, NIL, @1);
					n->agg_star = TRUE;
					$$ = (Node *)n;
				}
			| func_name '(' func_arg_def_on_conv_err_list ')'
				{
					/*
					 * Due to historical reasons, to_binary_float already has 2 text type parameters, currently,
					 * use a suffix "_ext" of func_name to support more to_binary_float functions.
					 */
					char *fnname = psprintf("%s_EXT", NameListToString($1));

					$$ = (Node *) makeFuncCall(OpenTenBaseOraFuncName(fnname), $3, @1);
				}
			| func_name '(' func_arg_def_on_using_nls ')'
				{
					char   *objname = NULL;

					objname = strVal(llast($1));
					if (strcasecmp(objname, "chr") != 0)
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
										errmsg(" (%s using nchar_cs) is not supportted", objname),
										parser_errposition(@1)));
					}
					$$ = (Node *) makeFuncCall($1, $3, @1);
				}
			| TABLE '(' func_arg_list ')'
				{
					FuncCall *n = makeFuncCall(list_make1(makeString("UNNEST")),
											   $3, @1);

					$$ = (Node *)n;
				}
			| TABLE '(' select_no_parens ')'
			{
				SubLink *s = NULL;

				s = makeNode(SubLink);
				s->subLinkType = EXPR_SUBLINK;
				s->subLinkId = 0;
				s->testexpr = NULL;
				s->operName = NIL;
				s->subselect = $3;
				s->location = @1;

				$$ = (Node *)makeFuncCall(list_make1(makeString("UNNEST")),
										  list_make1(s), @1);
			}
		;

/*
 * func_expr and its cousin func_expr_windowless are split out from c_expr just
 * so that we have classifications for "everything that is a function call or
 * looks like one".  This isn't very important, but it saves us having to
 * document which variants are legal in places like "FROM function()" or the
 * backwards-compatible functional-index syntax for CREATE INDEX.
 * (Note that many of the special SQL functions wouldn't actually make any
 * sense as functional index entries, but we ignore that consideration here.)
 */
func_expr: func_application within_group_clause filter_clause over_clause
				{
					if (nodeTag($1) == T_CaseExpr && ((CaseExpr *)$1)->isdecode)
						$$ = $1;
					else
					{
						FuncCall *n = (FuncCall *) $1;
						/*
						 * The order clause for WITHIN GROUP and the one for
						 * plain-aggregate ORDER BY share a field, so we have to
						 * check here that at most one is present.  We also check
						 * for DISTINCT and VARIADIC here to give a better error
						 * location.  Other consistency checks are deferred to
						 * parse analysis.
						 */
						if ($2 != NIL)
						{
							if (n->agg_order != NIL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
											errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP"),
											parser_errposition(@2)));
							if (n->agg_distinct)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
											errmsg("cannot use DISTINCT with WITHIN GROUP"),
											parser_errposition(@2)));
							if (n->func_variadic)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
											errmsg("cannot use VARIADIC with WITHIN GROUP"),
											parser_errposition(@2)));
							n->agg_order = $2;
							n->agg_within_group = TRUE;
						}
						n->agg_filter = $3;
						n->over = $4;
						$$ = (Node *) n;
					}
				}
			| func_application indirection within_group_clause filter_clause over_clause
				{
					if (ORA_MODE && nodeTag($1) == T_CaseExpr && ((CaseExpr *)$1)->isdecode)
							$$ = $1;
					else
					{
							FuncCall *n = (FuncCall *) $1;
        					/*
        					 * The order clause for WITHIN GROUP and the one for
        					 * plain-aggregate ORDER BY share a field, so we have to
        					 * check here that at most one is present.  We also check
        					 * for DISTINCT and VARIADIC here to give a better error
        					 * location.  Other consistency checks are deferred to
        					 * parse analysis.
        					 */
        					if ($3 != NIL)
        					{
        						if (n->agg_order != NIL)
        							ereport(ERROR,
        									(errcode(ERRCODE_SYNTAX_ERROR),
        									 errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP"),
        									 parser_errposition(@3)));
        						if (n->agg_distinct)
        							ereport(ERROR,
        									(errcode(ERRCODE_SYNTAX_ERROR),
        									 errmsg("cannot use DISTINCT with WITHIN GROUP"),
        									 parser_errposition(@3)));
        						if (n->func_variadic)
        							ereport(ERROR,
        									(errcode(ERRCODE_SYNTAX_ERROR),
        									 errmsg("cannot use VARIADIC with WITHIN GROUP"),
        									 parser_errposition(@3)));
        						n->agg_order = $3;
        						n->agg_within_group = TRUE;
        					}
        					n->agg_filter = $4;
        					n->over = $5;
							n->return_columnref = $2;
							
        					$$ = (Node *) n;
					}
				}
			| func_expr_common_subexpr
				{ $$ = $1; }
		;

/*
 * As func_expr but does not accept WINDOW functions directly
 * (but they can still be contained in arguments for functions etc).
 * Use this when window expressions are not allowed, where needed to
 * disambiguate the grammar (e.g. in CREATE INDEX).
 */
func_expr_windowless:
			func_application						{ $$ = $1; }
			| func_expr_common_subexpr				{ $$ = $1; }
		;

/*
 * Special expressions that are considered to be functions.
 */
func_expr_common_subexpr:
			COLLATION FOR '(' a_expr ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("pg_collation_for"),
											   list_make1($4),
											   @1);
				}
			| CURRENT_DATE
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_DATE, -1, @1);
				}
			| CURRENT_TIME
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIME, -1, @1);
				}
			| CURRENT_TIME '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIME_N, $3, @1);
				}
			| CURRENT_TIMESTAMP
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1, @1);
				}
			| CURRENT_TIMESTAMP '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP_N, $3, @1);
				}
			| LOCALTIME
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIME, -1, @1);
				}
			| LOCALTIME '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIME_N, $3, @1);
				}
			| LOCALTIMESTAMP
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIMESTAMP, -1, @1);
				}
			| LOCALTIMESTAMP '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_LOCALTIMESTAMP, -1, @1);
				}
/* OPENTENBASE_ORA_BEGIN */
			| SYSDATE
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_DATE, -1, @1);
				}
			| SYSDATE '(' ')'
				{
					if ( SPI_connect_level() >=0 )
						$$ = makeSQLValueFunction(SVFOP_CURRENT_DATE, -1, @1);
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("syntax error at or near \"(\""),
								parser_errposition(@2)));

				}
			| SYSTIMESTAMP
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1, @1);
				}
			| SYSTIMESTAMP '(' Iconst ')'
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_TIMESTAMP, -1, @1);
				}
/* OPENTENBASE_ORA_END */
			| CURRENT_ROLE
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_ROLE, -1, @1);
				}
			| CURRENT_USER
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_USER, -1, @1);
				}
			| SESSION_USER
				{
					$$ = makeSQLValueFunction(SVFOP_SESSION_USER, -1, @1);
				}
/* OPENTENBASE_ORA_BEGIN */
			| USER
				{
					$$ = makeSQLValueFunction(SVFOP_USER, -1, @1);
				}
			| UID_P
				{
					$$ = makeSQLValueFunction(SVFOP_USER_ID, -1, @1);
				}
			| CURRENT_CATALOG
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_CATALOG, -1, @1);
				}
			| CURRENT_SCHEMA
				{
					$$ = makeSQLValueFunction(SVFOP_CURRENT_SCHEMA, -1, @1);
				}
			| CAST in_typecast_scope '(' a_expr AS Typename out_typecast_scope ')'
				{
					$$ = makeTypeCast($4, $6, @1);
				}
			| EXTRACT '(' extract_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("date_part"), $3, @1);
				}
			| EXTRACT '(' expr_list ')'
				{
					$$ = (Node *) makeFuncCall(list_make1(makeString("ORCL_EXTRACT")), $3, @1);
				}
			| OVERLAY
				{
					ORA_ENTER_SCOPE(ORA_OVERLAY_SCOPE, pg_yyget_extra(yyscanner));
				} '(' overlay_list ')'
				{
					/* overlay(A PLACING B FROM C FOR D) is converted to
					 * overlay(A, B, C, D)
					 * overlay(A PLACING B FROM C) is converted to
					 * overlay(A, B, C)
					 */
					$$ = (Node *) makeFuncCall(SystemFuncName("overlay"), $4, @1);
					ORA_LEAVE_SCOPE(ORA_OVERLAY_SCOPE, pg_yyget_extra(yyscanner));
				}
			| POSITION '(' position_list ')'
				{
					/* position(A in B) is converted to position(B, A) */
					$$ = (Node *) makeFuncCall(SystemFuncName("position"), $3, @1);
				}
			| SUBSTRING '(' substr_list ')'
				{
					/* substring(A from B for C) is converted to
					 * substring(A, B, C) - thomas 2000-11-28
					 */
					$$ = (Node *) makeFuncCall(SystemFuncName("substring"), $3, @1);
				}
			| TREAT '(' a_expr AS Typename ')'
				{
					/* TREAT(expr AS target) converts expr of a particular type to target,
					 * which is defined to be a subtype of the original expression.
					 * In SQL99, this is intended for use with structured UDTs,
					 * but let's make this a generally useful form allowing stronger
					 * coercions than are handled by implicit casting.
					 *
					 * Convert SystemTypeName() to SystemFuncName() even though
					 * at the moment they result in the same thing.
					 */
					$$ = (Node *) makeFuncCall(SystemFuncName(((Value *)llast($5->names))->val.str),
												list_make1($3),
												@1);
				}
			| TRIM '(' BOTH trim_list ')'
				{
					/* various trim expressions are defined in SQL
					 * - thomas 1997-07-19
					 */
					if (list_length($4) == 2)
					{
						$$ = (Node *) makeFuncCall(SystemFuncName("btrim"),
							lappend($4, makeStringConst(pstrdup("t"), @1)), @1);

					}
					else
					{
						$$ = (Node *) makeFuncCall(SystemFuncName("btrim"), $4, @1);
					}
				}
			| TRIM '(' LEADING trim_list ')'
				{
					if (list_length($4) == 2)
					{
						$$ = (Node *) makeFuncCall(SystemFuncName("ltrim"),
							lappend($4, makeStringConst(pstrdup("t"), @1)), @1);

					}
					else
					{
						$$ = (Node *) makeFuncCall(SystemFuncName("ltrim"), $4, @1);	
					}
				}
			| TRIM '(' TRAILING trim_list ')'
				{
					if (list_length($4) == 2)
					{
						$$ = (Node *) makeFuncCall(SystemFuncName("rtrim"),
							lappend($4, makeStringConst(pstrdup("t"), @1)), @1);
					}
					else
					{
						$$ = (Node *) makeFuncCall(SystemFuncName("rtrim"), $4, @1);
					}
				}
			| TRIM '(' trim_list ')'
				{
					$$ = (Node *) makeFuncCall(SystemFuncName("btrim"), $3, @1);
				}
			| NULLIF '(' a_expr ',' a_expr ')'
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1);
				}
			| COALESCE '(' expr_list ')'
				{
					CoalesceExpr *c = makeNode(CoalesceExpr);
					c->args = $3;
					c->location = @1;
					$$ = (Node *)c;
				}
			| GREATEST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_GREATEST;
					v->location = @1;
					$$ = (Node *)v;
				}
			| LEAST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_LEAST;
					v->location = @1;
					$$ = (Node *)v;
				}
			| XMLCONCAT '(' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLCONCAT, NULL, NIL, $3, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, NIL, NIL, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, $6, NIL, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, NIL, $6, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ',' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, $6, $8, @1);
				}
			| XMLEXISTS '(' c_expr xmlexists_argument ')'
				{
					/* xmlexists(A PASSING [BY REF] B [BY REF]) is
					 * converted to xmlexists(A, B)*/
					$$ = (Node *) makeFuncCall(SystemFuncName("xmlexists"), list_make2($3, $4), @1);
				}
			| XMLFOREST '(' xml_attribute_list ')'
				{
					$$ = makeXmlExpr(IS_XMLFOREST, NULL, $3, NIL, @1);
				}
			| XMLPARSE '(' document_or_content a_expr xml_whitespace_or_wellformed_option ')'
				{
					XmlExpr *x = (XmlExpr *)
						makeXmlExpr(IS_XMLPARSE, NULL, NIL,
									list_make2($4, makeIntConst($5, -1)),
									@1);
					x->xmloption = $3;
					$$ = (Node *)x;
				}
			| XMLPI '(' NAME_P ColLabel ')'
				{
					$$ = makeXmlExpr(IS_XMLPI, $4, NULL, NIL, @1);
				}
			| XMLPI '(' NAME_P ColLabel ',' a_expr ')'
				{
					$$ = makeXmlExpr(IS_XMLPI, $4, NULL, list_make1($6), @1);
				}
			| XMLROOT '(' a_expr ',' xml_root_version opt_xml_root_standalone ')'
				{
					$$ = makeXmlExpr(IS_XMLROOT, NULL, NIL,
									 list_make3($3, $5, $6), @1);
				}
			| XMLSERIALIZE '(' document_or_content a_expr AS SimpleTypename ')'
				{
					XmlSerialize *n = makeNode(XmlSerialize);
					n->xmloption = $3;
					n->expr = $4;
					n->typeName = $6;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

/*
 * SQL/XML support
 */
xml_root_version: VERSION_P a_expr
				{ $$ = $2; }
			| VERSION_P NO VALUE_P
				{ $$ = makeNullAConst(-1); }
		;

opt_xml_root_standalone: ',' STANDALONE_P YES_P
				{ $$ = makeIntConst(XML_STANDALONE_YES, -1); }
			| ',' STANDALONE_P NO
				{ $$ = makeIntConst(XML_STANDALONE_NO, -1); }
			| ',' STANDALONE_P NO VALUE_P
				{ $$ = makeIntConst(XML_STANDALONE_NO_VALUE, -1); }
			| /*EMPTY*/
				{ $$ = makeIntConst(XML_STANDALONE_OMITTED, -1); }
		;

xml_attributes: XMLATTRIBUTES '(' xml_attribute_list ')'	{ $$ = $3; }
		;

xml_attribute_list:	xml_attribute_el					{ $$ = list_make1($1); }
			| xml_attribute_list ',' xml_attribute_el	{ $$ = lappend($1, $3); }
		;

xml_attribute_el: a_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = (Node *) $1;
					$$->location = @1;
				}
			| a_expr
				{
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *) $1;
					$$->location = @1;
				}
		;

document_or_content: DOCUMENT_P						{ $$ = XMLOPTION_DOCUMENT; }
			| CONTENT_P								{ $$ = XMLOPTION_CONTENT; }
		;

xml_whitespace_or_wellformed_option: PRESERVE WHITESPACE_P							{ $$ = XWOWO_PG_PRESERVE_WHITESPACE; }
			| STRIP_P WHITESPACE_P													{ $$ = XWOWO_PG_STRIP_WHITESPACE; }
			| WELLFORMED															{ $$ = XWOWO_OPENTENBASE_ORA_WELLFORMED; }
			| /*EMPTY In opentenbase_ora mode, is not WELLFORMED; in pg mode, is STRIP_P*/ 	{ $$ = XWOWO_NOT_OPTION; }
		;

/* We allow several variants for SQL and other compatibility. */
xmlexists_argument:
			PASSING c_expr
				{
					$$ = $2;
				}
			| PASSING c_expr BY REF
				{
					$$ = $2;
				}
			| PASSING BY REF c_expr
				{
					$$ = $4;
				}
			| PASSING BY REF c_expr BY REF
				{
					$$ = $4;
				}
		;


/*
 * Aggregate decoration clauses
 */
within_group_clause:
			WITHIN GROUP_P '(' sort_clause ')'		{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

filter_clause:
			FILTER '(' WHERE a_expr ')'				{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


/*
 * Window Definitions
 */
window_clause:
			WINDOW window_definition_list			{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

window_definition_list:
			window_definition						{ $$ = list_make1($1); }
			| window_definition_list ',' window_definition
													{ $$ = lappend($1, $3); }
		;

window_definition:
			ColId AS window_specification
				{
					WindowDef *n = $3;
					n->name = $1;
					$$ = n;
				}
		;

over_clause: OVER window_specification
				{ $$ = $2; }
			| OVER ColId
				{
					WindowDef *n = makeNode(WindowDef);
					n->name = $2;
					n->refname = NULL;
					n->partitionClause = NIL;
					n->orderClause = NIL;
					n->frameOptions = FRAMEOPTION_DEFAULTS;
					n->startOffset = NULL;
					n->endOffset = NULL;
					n->location = @2;
					$$ = n;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

window_specification: '(' opt_existing_window_name opt_partition_clause
						opt_sort_clause opt_frame_clause ')'
				{
					WindowDef *n = makeNode(WindowDef);
					n->name = NULL;
					n->refname = $2;
					n->partitionClause = $3;
					n->orderClause = $4;
					/* copy relevant fields of opt_frame_clause */
					n->frameOptions = $5->frameOptions;
					n->startOffset = $5->startOffset;
					n->endOffset = $5->endOffset;
					n->location = @1;
					$$ = n;
				}
		;

/*
 * If we see PARTITION, RANGE, or ROWS as the first token after the '('
 * of a window_specification, we want the assumption to be that there is
 * no existing_window_name; but those keywords are unreserved and so could
 * be ColIds.  We fix this by making them have the same precedence as IDENT
 * and giving the empty production here a slightly higher precedence, so
 * that the shift/reduce conflict is resolved in favor of reducing the rule.
 * These keywords are thus precluded from being an existing_window_name but
 * are not reserved for any other purpose.
 */
opt_existing_window_name: ColId						{ $$ = $1; }
			| /*EMPTY*/				%prec Op		{ $$ = NULL; }
		;

opt_partition_clause: PARTITION BY expr_list		{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*
 * For frame clauses, we return a WindowDef, but only some fields are used:
 * frameOptions, startOffset, and endOffset.
 *
 * This is only a subset of the full SQL:2008 frame_clause grammar.
 * We don't support <window frame exclusion> yet.
 */
opt_frame_clause:
			RANGE frame_extent
				{
					WindowDef *n = $2;
					n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_RANGE;
					$$ = n;
				}
			| ROWS frame_extent
				{
					WindowDef *n = $2;
					n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS;
					$$ = n;
				}
			| /*EMPTY*/
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_DEFAULTS;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
		;

frame_extent: frame_bound
				{
					WindowDef *n = $1;
					/* reject invalid cases */
					if (n->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
								 parser_errposition(@1)));
					if (n->frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from following row cannot end with current row"),
								 parser_errposition(@1)));
					n->frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
					$$ = n;
				}
			| BETWEEN frame_bound AND frame_bound
				{
					WindowDef *n1 = $2;
					WindowDef *n2 = $4;
					/* form merged options */
					int		frameOptions = n1->frameOptions;
					/* shift converts START_ options to END_ options */
					frameOptions |= n2->frameOptions << 1;
					frameOptions |= FRAMEOPTION_BETWEEN;
					/* reject invalid cases */
					if (frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
								 parser_errposition(@2)));
					if (frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame end cannot be UNBOUNDED PRECEDING"),
								 parser_errposition(@4)));
					if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) &&
						(frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING))
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from current row cannot have preceding rows"),
								 parser_errposition(@4)));
					if ((frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) &&
						(frameOptions & (FRAMEOPTION_END_OFFSET_PRECEDING |
										 FRAMEOPTION_END_CURRENT_ROW)))
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from following row cannot have preceding rows"),
								 parser_errposition(@4)));
					n1->frameOptions = frameOptions;
					n1->endOffset = n2->startOffset;
					$$ = n1;
				}
		;

/*
 * This is used for both frame start and frame end, with output set up on
 * the assumption it's frame start; the frame_extent productions must reject
 * invalid cases.
 */
frame_bound:
			UNBOUNDED PRECEDING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_UNBOUNDED_PRECEDING;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| UNBOUNDED FOLLOWING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_UNBOUNDED_FOLLOWING;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| CURRENT_P ROW
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_CURRENT_ROW;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| a_expr PRECEDING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_OFFSET_PRECEDING;
					n->startOffset = $1;
					n->endOffset = NULL;
					$$ = n;
				}
			| a_expr FOLLOWING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_OFFSET_FOLLOWING;
					n->startOffset = $1;
					n->endOffset = NULL;
					$$ = n;
				}
		;


/*
 * Supporting nonterminals for expressions.
 */

/* Explicit row production.
 *
 * SQL99 allows an optional ROW keyword, so we can now do single-element rows
 * without conflicting with the parenthesized a_expr production.  Without the
 * ROW keyword, there must be more than one a_expr inside the parens.
 */
row:		ROW '(' expr_list ')'					{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
			| '(' expr_list ',' a_expr ')'			{ $$ = lappend($2, $4); }
		;

explicit_row:	ROW '(' expr_list ')'				{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
		;

implicit_row:	'(' expr_list ',' a_expr ')'		{ $$ = lappend($2, $4); }
		;

sub_type:	ANY										{ $$ = ANY_SUBLINK; }
			| SOME									{ $$ = ANY_SUBLINK; }
			| ALL									{ $$ = ALL_SUBLINK; }
		;

all_Op:		Op										{ $$ = $1; }
			| MathOp								{ $$ = $1; }
		;

MathOp:		 '+'									{ $$ = "+"; }
			| '-'									{ $$ = "-"; }
			| '*'									{ $$ = "*"; }
			| '/'									{ $$ = "/"; }
			| '%'									{ $$ = "%"; }
			| '^'									{ $$ = "^"; }
			| '<'									{ $$ = "<"; }
			| '>'									{ $$ = ">"; }
			| '='									{ $$ = "="; }
			| LESS_EQUALS							{ $$ = "<="; }
			| GREATER_EQUALS						{ $$ = ">="; }
			| NOT_EQUALS							{ $$ = "<>"; }
		;

qual_Op:	Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

subquery_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
			| LIKE
					{ $$ = list_make1(makeString("~~")); }
			| NOT_LA LIKE
					{ $$ = list_make1(makeString("!~~")); }
			| ILIKE
					{ $$ = list_make1(makeString("~~*")); }
			| NOT_LA ILIKE
					{ $$ = list_make1(makeString("!~~*")); }
/* cannot put SIMILAR TO here, because SIMILAR TO is a hack.
 * the regular expression is preprocessed by a function (similar_escape),
 * and the ~ operator for posix regular expressions is used.
 *        x SIMILAR TO y     ->    x ~ similar_escape(y)
 * this transformation is made on the fly by the parser upwards.
 * however the SubLink structure which handles any/some/all stuff
 * is not ready for such a thing.
 */
			;

expr_list:	a_expr
				{
					$$ = list_make1($1);
				}
			| expr_list ',' a_expr
				{
					$$ = lappend($1, $3);
				}
		;

/* function arguments can have names */
func_arg_list:  func_arg_expr
				{
					$$ = list_make1($1);
				}
			| func_arg_list ',' func_arg_expr
				{
					$$ = lappend($1, $3);
				}
		;

func_arg_expr:  a_expr
				{
					$$ = $1;
				}
			| param_name COLON_EQUALS a_expr
				{
					NamedArgExpr *na = makeNode(NamedArgExpr);
					na->name = $1;
					na->arg = (Expr *) $3;
					na->argnumber = -1;		/* until determined */
					na->location = @1;
					$$ = (Node *) na;
				}
			| param_name EQUALS_GREATER a_expr
				{
					NamedArgExpr *na = makeNode(NamedArgExpr);
					na->name = $1;
					na->arg = (Expr *) $3;
					na->argnumber = -1;		/* until determined */
					na->location = @1;
					$$ = (Node *) na;
				}
		;

type_list:	Typename								{ $$ = list_make1($1); }
			| type_list ',' Typename				{ $$ = lappend($1, $3); }
		;

array_expr: '[' expr_list ']'
				{
					$$ = makeAArrayExpr($2, @1);
				}
			| '[' array_expr_list ']'
				{
					$$ = makeAArrayExpr($2, @1);
				}
			| '[' ']'
				{
					$$ = makeAArrayExpr(NIL, @1);
				}
		;

array_expr_list: array_expr							{ $$ = list_make1($1); }
			| array_expr_list ',' array_expr		{ $$ = lappend($1, $3); }
		;


extract_list:
			extract_arg FROM a_expr
				{
					$$ = list_make2(makeStringConst($1, @1), $3);
				}
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* Allow delimited string Sconst in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */
extract_arg:
			Common_IDENT							{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| Sconst								{ $$ = $1; }
		;

/* OVERLAY() arguments
 * SQL99 defines the OVERLAY() function:
 * o overlay(text placing text from int for int)
 * o overlay(text placing text from int)
 * and similarly for binary strings
 */
overlay_list:
			a_expr overlay_placing substr_from substr_for
				{
					$$ = list_make4($1, $2, $3, $4);
				}
			| a_expr overlay_placing substr_from
				{
					$$ = list_make3($1, $2, $3);
				}
		;

overlay_placing:
			PLACING a_expr
				{ $$ = $2; }
		;

/* position_list uses b_expr not a_expr to avoid conflict with general IN */

position_list:
			b_expr IN_P b_expr						{ $$ = list_make2($3, $1); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* SUBSTRING() arguments
 * SQL9x defines a specific syntax for arguments to SUBSTRING():
 * o substring(text from int for int)
 * o substring(text from int) get entire string from starting point "int"
 * o substring(text for int) get first "int" characters of string
 * o substring(text from pattern) get entire string matching pattern
 * o substring(text from pattern for escape) same with specified escape char
 * We also want to support generic substring functions which accept
 * the usual generic list of arguments. So we will accept both styles
 * here, and convert the SQL9x style to the generic list for further
 * processing. - thomas 2000-11-28
 */
substr_list:
			a_expr substr_from substr_for
				{
					$$ = list_make3($1, $2, $3);
				}
			| a_expr substr_for substr_from
				{
					/* not legal per SQL99, but might as well allow it */
					$$ = list_make3($1, $3, $2);
				}
			| a_expr substr_from
				{
					$$ = list_make2($1, $2);
				}
			| a_expr substr_for
				{
					/*
					 * Since there are no cases where this syntax allows
					 * a textual FOR value, we forcibly cast the argument
					 * to int4.  The possible matches in pg_proc are
					 * substring(text,int4) and substring(text,text),
					 * and we don't want the parser to choose the latter,
					 * which it is likely to do if the second argument
					 * is unknown or doesn't have an implicit cast to int4.
					 */
					$$ = list_make3($1, makeIntConst(1, -1),
									makeTypeCast($2,
												 SystemTypeName("int4"), -1));
				}
			| expr_list
				{
					$$ = $1;
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;

substr_from:
			FROM a_expr								{ $$ = $2; }
		;

substr_for: FOR a_expr								{ $$ = $2; }
		;

trim_list:	a_expr FROM expr_list					{ $$ = lappend($3, $1); }
			| FROM expr_list						{ $$ = $2; }
			| expr_list								{ $$ = $1; }
		;

in_expr:	select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subselect = $1;
					/* other fields will be filled later */
					$$ = (Node *)n;
				}
			| '(' expr_list ')'						{ $$ = (Node *)$2; }
		;


/*
 * SQL9x defines a specific syntax for arguments of func in opentenbase_ora:
 * - to_binary_float(arg1 default arg2 on conversion error)
 * - to_binary_float(arg1 default arg2 on conversion error, fmt)
 * - to_binary_double(arg1 default arg2 on conversion error)
 * - to_binary_double(arg1 default arg2 on conversion error, fmt)
 */
func_arg_def_on_conv_err_list:
	a_expr DEFAULT a_expr ON CONVERSION_P ERROR_P 	%prec DEFAULT
		{
			$$ = list_make2($1, $3);
		}
	| func_arg_def_on_conv_err_list ',' a_expr
		{
			$$ = lappend($1, $3);
		}
	;

/*
* - chr(n USING NCHAR_CS)
*/
func_arg_def_on_using_nls:
	a_expr USING NCHAR_CS
	{
		$$ = list_make2($1, makeStringConst("USING NCHAR_CS", @3));
	}
	;


/*
 * Define SQL-style CASE clause.
 * - Full specification
 *	CASE WHEN a = b THEN c ... ELSE d END
 * - Implicit argument
 *	CASE a WHEN b THEN c ... ELSE d END
 */
case_expr:
            {
                ORA_ENTER_SCOPE(ORA_CASE_WHEN_SCOPE, pg_yyget_extra(yyscanner));
            }
            CASE case_arg when_clause_list case_default END_P
			{
				CaseExpr *c = makeNode(CaseExpr);
				c->casetype = InvalidOid; /* not analyzed yet */
				c->arg = (Expr *) $3;
				c->args = $4;
				c->defresult = (Expr *) $5;
				c->location = @2;
				$$ = (Node *)c;

				ORA_LEAVE_SCOPE(ORA_CASE_WHEN_SCOPE, pg_yyget_extra(yyscanner));
			}
		;

when_clause_list:
			/* There must be at least one */
			when_clause								{ $$ = list_make1($1); }
			| when_clause_list when_clause			{ $$ = lappend($1, $2); }
		;

when_clause:
			WHEN a_expr THEN a_expr
				{
					CaseWhen *w = makeNode(CaseWhen);
					w->expr = (Expr *) $2;
					w->result = (Expr *) $4;
					w->location = @1;
					$$ = (Node *)w;
				}
		;

case_default:
			ELSE a_expr								{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

case_arg:	a_expr									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;


columnref:	columnref_col
				{
					$$ = $1;
				}
		;

columnref_col:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| opentenbase_ora_colref_ident
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| ColId indirection
				{
					$$ = makeColumnRef($1, $2, @1, yyscanner);
				}
			| opentenbase_ora_colref_ident indirection
				{
					$$ = makeColumnRef($1, $2, @1, yyscanner);
				}
			| ':' param_name
				{
					$$ = makeColumnRef(StrConcat(":", $2), NIL, @1, yyscanner);
				}
			| ':' param_name indirection
				{
					$$ = makeColumnRef(StrConcat(":", $2), $3, @1, yyscanner);
				}

		;

indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
			| '.' '*'
				{
					$$ = (Node *) makeNode(A_Star);
				}
			| '[' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->is_slice = false;
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' opt_slice_bound ':' opt_slice_bound ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->is_slice = true;
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;

indirection_el_ora:	'(' a_expr ')'
					{
						A_Indices *ai = makeNode(A_Indices);
						ai->is_slice = false;
						ai->lidx = NULL;
						ai->uidx = $2;
						$$ = (Node *) ai;
					}

opt_slice_bound:
			a_expr									{ $$ = $1; }
			| /*EMPTY*/ %prec UMINUS
													{ $$ = NULL; }
		;

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
		;

opt_indirection_ora:
			/*EMPTY*/									{ $$ = NIL; }
			| opt_indirection_ora indirection_el_ora	{ $$ = lappend($1, $2);}
		;

opt_asymmetric: ASYMMETRIC
			| /*EMPTY*/
		;


/*****************************************************************************
 *
 *	target list for SELECT
 *
 *****************************************************************************/

opt_target_list: target_list						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

target_list:
			target_el								{ $$ = list_make1($1); }
			| target_list ',' target_el				{ $$ = lappend($1, $3); }
		;

target_el:	a_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			/*
			 * We support omitting AS only for column labels that aren't
			 * any known keyword.  There is an ambiguity against postfix
			 * operators: is "a ! b" an infix expression, or a postfix
			 * expression and a column label?  We prefer to resolve this
			 * as an infix expression, which we accomplish by assigning
			 * IDENT a precedence higher than POSTFIXOP.
			 */
			| a_expr AliasId /*IDENT*/
			{
				$$ = makeNode(ResTarget);
				$$->name = $2;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}
			| c_expr_spec_alais spec_aliasid_keyword
				{
					$$ = makeNode(ResTarget);
					$$->name = upcase_identifier((char *) $2, strlen((char *) $2));
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| a_expr_spec_alais a_expr_spec_alais_kw
			    {
			        $$ = makeNode(ResTarget);
                    $$->name = upcase_identifier((char *) $2, strlen((char *) $2));
                    $$->indirection = NIL;
                    $$->val = (Node *)$1;
                    $$->location = @1;
			    }
			| a_expr
				{
					if (IsA($1, A_Const))
					{
						A_Const *n = (A_Const *)$1;
						if (n->location == -1)
						{
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("alias name is reserved keyword!"),
									 parser_errposition(@1)));
						}
					}
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| '*'
				{
					ColumnRef *n = makeNode(ColumnRef);
					n->fields = list_make1(makeNode(A_Star));
					n->location = @1;

					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)n;
					$$->location = @1;
				}
		;


/*****************************************************************************
 *
 *	Names and constants
 *
 *****************************************************************************/

qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

qualified_name:
	opentenbase_ora_qualified_name {$$ = $1;}
	| pg_qualified_name {$$ = $1;}
qualified_ref:
	opentenbase_ora_qualified_ref {$$ = $1;}
	| pg_qualified_name {$$ = $1;}

opentenbase_ora_qualified_ref:
	opentenbase_ora_refident
	{
		$$ = makeRangeVar(NULL, $1, @1);
	}
	| opentenbase_ora_refident indirection
	{
	  check_qualified_name($2, yyscanner);
		$$ = makeRangeVar(NULL, NULL, @1);
		switch (list_length($2))
		{
			case 1:
				$$->catalogname = NULL;
				$$->schemaname = $1;
				$$->relname = strVal(linitial($2));
				break;
			case 2:
				$$->catalogname = $1;
				$$->schemaname = strVal(linitial($2));
				$$->relname = strVal(lsecond($2));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("improper qualified name (too many dotted names): %s",
								NameListToString(lcons(makeString($1), $2))),
								parser_errposition(@1)));
				break;
		}
	}
	;

opentenbase_ora_qualified_name:
	opentenbase_ora_ident
	{
		$$ = makeRangeVar(NULL, $1, @1);
	}
	| opentenbase_ora_ident indirection
	{
		check_qualified_name($2, yyscanner);
		$$ = makeRangeVar(NULL, NULL, @1);
		switch (list_length($2))
		{
			case 1:
				$$->catalogname = NULL;
				$$->schemaname = $1;
				$$->relname = strVal(linitial($2));
				break;
			case 2:
				$$->catalogname = $1;
				$$->schemaname = strVal(linitial($2));
				$$->relname = strVal(lsecond($2));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("improper qualified name (too many dotted names): %s",
							NameListToString(lcons(makeString($1), $2))),
							parser_errposition(@1)));
				break;
		}
	}
	;

/*
 * The production for a qualified relation name has to exactly match the
 * production for a qualified func_name, because in a FROM clause we cannot
 * tell which we are parsing until we see what comes after it ('(' for a
 * func_name, something else for a relation). Therefore we allow 'indirection'
 * which may contain subscripts, and reject that case in the C code.
 */
pg_qualified_name:
			ColId
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| ColId indirection
				{
					check_qualified_name($2, yyscanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))),
									 parser_errposition(@1)));
							break;
					}
				}
		;

pkg_qualified_name:
			PkgId
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| PkgId indirection
				{
					check_qualified_name($2, yyscanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))),
									 parser_errposition(@1)));
							break;
					}
				}
		;

name_list:	name
					{ $$ = list_make1(makeString($1)); }
			| name_list ',' name
					{ $$ = lappend($1, makeString($3)); }
			| opentenbase_ora_coldef_ident
					{ $$ = list_make1(makeString($1)); }
			| name_list ',' opentenbase_ora_coldef_ident
					{
						$$ = lappend($1, makeString($3));
					}
		;


name:		ColId									{ $$ = $1; };

database_name:
			ColId									{ $$ = $1; };

directory_name:
			ColId									{ $$ = $1; };

access_method:
			Lower_ColId								{ $$ = $1; };

attr_name:	ColLabel								{ $$ = $1; };

index_name: ColId									{ $$ = $1; };

file_name:	Sconst									{ $$ = $1; };

/*
 * The production for a qualified func_name has to exactly match the
 * production for a qualified columnref, because we cannot tell which we
 * are parsing until we see what comes after it ('(' or Sconst for a func_name,
 * anything else for a columnref).  Therefore we allow 'indirection' which
 * may contain subscripts, and reject that case in the C code.  (If we
 * ever implement SQL99-like methods, such syntax may actually become legal!)
 */
func_name:	type_function_name
					{ $$ = list_make1(makeString($1)); }
			| ColId indirection
					{
						$$ = check_func_name(lcons(makeString($1), $2),
											 yyscanner);
					}
					;

func_name_no_parens:    Common_IDENT { $$ = list_make1(makeString($1)); }
					   | type_func_name_keyword { $$ = list_make1(makeString(upcase_identifier($1, strlen($1)))); }
					   | pkg_reserved_keyword  { $$ = list_make1(makeString(upcase_identifier($1, strlen($1)))); }
					   | col_name_keyword	{$$ = list_make1(makeString(upcase_identifier($1, strlen($1))));}
					   | unreserved_keyword { $$ = list_make1(makeString(upcase_identifier($1, strlen($1)))); }
					   | ColId indirection
						{
							$$ = check_func_name(lcons(makeString($1), $2),
												yyscanner);
						}
			   ;

/*
 * Constants
 */
AexprConst: Iconst opt_char
				{
					if ($2)
						$$ = makeIntConst($1, -1);
					else
						$$ = makeIntConst($1, @1);
				}
			| FCONST
				{
					$$ = makeFloatConst($1, @1);
				}
			| Sconst
				{
					if (strlen($1) == 0 && !disable_empty_to_null)
						$$ = makeEmptyStrAConst(@1);
					else
						$$ = makeStringConst($1, @1);
				}
			| BCONST
				{
					$$ = makeBitStringConst($1, @1);
				}
			| XCONST
				{
					/* This is a bit constant per SQL99:
					 * Without Feature F511, "BIT data type",
					 * a <general literal> shall not be a
					 * <bit string literal> or a <hex string literal>.
					 */
					$$ = makeBitStringConst($1, @1);
				}
			| func_name Sconst
				{
					/* generic type 'literal' syntax */
					TypeName *t = makeTypeNameFromNameList($1);
					t->location = @1;
					$$ = make_string_const_cast_orcl($2, @2, t);
				}
			| func_name '(' func_arg_list opt_sort_clause ')' Sconst
				{
					/* generic syntax with a type modifier */
					TypeName *t = makeTypeNameFromNameList($1);
					ListCell *lc;

					/*
					 * We must use func_arg_list and opt_sort_clause in the
					 * production to avoid reduce/reduce conflicts, but we
					 * don't actually wish to allow NamedArgExpr in this
					 * context, nor ORDER BY.
					 */
					foreach(lc, $3)
					{
						NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

						if (IsA(arg, NamedArgExpr))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have parameter name"),
									 parser_errposition(arg->location)));
					}
					if ($4 != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have ORDER BY"),
									 parser_errposition(@4)));

					t->typmods = $3;
					t->location = @1;
					$$ = makeStringConstCast($6, @6, t);
				}
			| ConstTypename Sconst
				{
					$$ = make_string_const_cast_orcl($2, @2, $1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					TypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| TRUE_P
				{
					$$ = makeBoolAConst(TRUE, @1);
				}
			| FALSE_P
				{
					$$ = makeBoolAConst(FALSE, @1);
				}
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
			| BINARY_DOUBLE_NAN
				{
					$$ = makeTypeCast(makeStringConst("NaN", @1), SystemTypeName("numeric"), @1);
				}
			| BINARY_FLOAT_NAN
				{
					$$ = makeTypeCast(makeStringConst("NaN", @1), SystemTypeName("numeric"), @1);
				}
		;

/*support special unreserved keyword as alias, but no as connection
* It delete "ConstInterval Sconst opt_interval" comparing to AexprConst
**/
AexprConstAlias: Iconst
				{
					$$ = makeIntConst($1, @1);
				}
			| FCONST
				{
					$$ = makeFloatConst($1, @1);
				}
			| Sconst
				{
					if (strlen($1) == 0 && !disable_empty_to_null)
						$$ = makeEmptyStrAConst(@1);
					else
						$$ = makeStringConst($1, @1);
				}
			| BCONST
				{
					$$ = makeBitStringConst($1, @1);
				}
			| XCONST
				{
					/* This is a bit constant per SQL99:
					 * Without Feature F511, "BIT data type",
					 * a <general literal> shall not be a
					 * <bit string literal> or a <hex string literal>.
					 */
					$$ = makeBitStringConst($1, @1);
				}
			| func_name Sconst
				{
					/* generic type 'literal' syntax */
					TypeName *t = makeTypeNameFromNameList($1);
					t->location = @1;
					$$ = makeStringConstCast($2, @2, t);
				}
			| func_name '(' func_arg_list opt_sort_clause ')' Sconst
				{
					/* generic syntax with a type modifier */
					TypeName *t = makeTypeNameFromNameList($1);
					ListCell *lc;

					/*
					 * We must use func_arg_list and opt_sort_clause in the
					 * production to avoid reduce/reduce conflicts, but we
					 * don't actually wish to allow NamedArgExpr in this
					 * context, nor ORDER BY.
					 */
					foreach(lc, $3)
					{
						NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

						if (IsA(arg, NamedArgExpr))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have parameter name"),
									 parser_errposition(arg->location)));
					}
					if ($4 != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have ORDER BY"),
									 parser_errposition(@4)));

					t->typmods = $3;
					t->location = @1;
					$$ = makeStringConstCast($6, @6, t);
				}
			| ConstTypename Sconst
				{
					$$ = makeStringConstCast($2, @2, $1);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					TypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| TRUE_P
				{
					$$ = makeBoolAConst(TRUE, @1);
				}
			| FALSE_P
				{
					$$ = makeBoolAConst(FALSE, @1);
				}
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
			| BINARY_DOUBLE_NAN
				{
					$$ = makeTypeCast(makeStringConst("NaN", @1), SystemTypeName("numeric"), @1);
				}
			| BINARY_FLOAT_NAN
				{
					$$ = makeTypeCast(makeStringConst("NaN", @1), SystemTypeName("numeric"), @1);
				}
		;
Iconst:		ICONST									{ $$ = $1; };
Sconst:		SCONST									{ $$ = $1; };

SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;

opt_char:
			CHAR_P									{ $$ = TRUE; }
			| CHARACTER								{ $$ = TRUE; }
			| BYTE_P								{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* Role specifications */
RoleId:		RoleSpec
				{
					RoleSpec *spc = (RoleSpec *) $1;
					switch (spc->roletype)
					{
						case ROLESPEC_CSTRING:
							$$ = spc->rolename;
							break;
						case ROLESPEC_PUBLIC:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("role name \"%s\" is reserved",
											"public"),
									 parser_errposition(@1)));
						case ROLESPEC_SESSION_USER:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"SESSION_USER"),
									 parser_errposition(@1)));
						case ROLESPEC_CURRENT_USER:
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("%s cannot be used as a role name here",
											"CURRENT_USER"),
									 parser_errposition(@1)));
					}
				}
			;

RoleSpec:	NonReservedWord
					{
						/*
						 * "public" and "none" are not keywords, but they must
						 * be treated specially here.
						 */
						RoleSpec *n;
						if (strcmp($1, "PUBLIC") == 0 || strcmp($1, "public") == 0)
						{
							n = (RoleSpec *) makeRoleSpec(ROLESPEC_PUBLIC, @1);
							n->roletype = ROLESPEC_PUBLIC;
						}
						else if (strcmp($1, "NONE") == 0)
						{
							ereport(ERROR,
									(errcode(ERRCODE_RESERVED_NAME),
									 errmsg("role name \"%s\" is reserved",
											"none"),
									 parser_errposition(@1)));
						}
						else
						{
							n = makeRoleSpec(ROLESPEC_CSTRING, @1);
							n->rolename = pstrdup($1);
						}
						$$ = n;
					}
			| CURRENT_USER
					{
						$$ = makeRoleSpec(ROLESPEC_CURRENT_USER, @1);
					}
			| SESSION_USER
					{
						$$ = makeRoleSpec(ROLESPEC_SESSION_USER, @1);
					}
		;

role_list:	RoleSpec
					{ $$ = list_make1($1); }
			| role_list ',' RoleSpec
					{ $$ = lappend($1, $3); }
		;

/* __RESOURCE_QUEUE__ BEGIN */
QueueId:	Lower_ColId								{ $$ = $1; };
/* __RESOURCE_QUEUE__ END */

/*
 * Name classification hierarchy.
 *
 * IDENT is the lexeme returned by the lexer for identifiers that match
 * no known keyword.  In most cases, we can accept certain keywords as
 * names, not only IDENTs.	We prefer to accept as many such keywords
 * as possible to minimize the impact of "reserved words" on programmers.
 * So, we divide names into several possible classes.  The classification
 * is chosen in part to make keywords acceptable as names wherever possible.
 */

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:		Common_IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
			| col_name_keyword						{ $$ = upcase_identifier($1, strlen($1)); }
			| pkg_reserved_keyword						{ $$ = upcase_identifier($1, strlen($1)); }
			| USING_LA                              { $$ = "USING"; }          
			| CURRENT_TIME_LA                       { $$ = "CURRENT_TIME"; }   
		;

TypObjId:	IDENT									{ $$ = $1; }
			| IDENT_IN_D_QUOTES						{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }

AliasId:    Common_IDENT                   { $$ = $1; }
            | ABORT_P                 { $$="ABORT";  }
            | ABSOLUTE_P                 { $$="ABSOLUTE";  }
            | ACCESS                 { $$="ACCESS";  }
            | ACCOUNT                 { $$="ACCOUNT";  }
            | ACTION                 { $$="ACTION";  }
            | ADD_P                 { $$="ADD";  }
            | ADMIN                 { $$="ADMIN";  }
            | AFTER                 { $$="AFTER";  }
            | AGGREGATE                 { $$="AGGREGATE";  }
            | ALSO                 { $$="ALSO";  }
            | ALTER                 { $$="ALTER";  }
            | ALWAYS                 { $$="ALWAYS";  }
            | ASSERTION                 { $$="ASSERTION";  }
            | ASSIGNMENT                 { $$="ASSIGNMENT";  }
            | AT                 { $$="AT";  }
            | ATTACH                 { $$="ATTACH";  }
            | ATTRIBUTE                 { $$="ATTRIBUTE";  }
            | BACKWARD                 { $$="BACKWARD";  }
            | BARRIER                 { $$="BARRIER";  }
            | BEFORE                 { $$="BEFORE";  }
            | BEGIN_P                 { $$="BEGIN";  }
            | BEGIN_SUBTXN                 { $$="BEGIN_SUBTXN";  }
            | BETWEEN                 { $$="BETWEEN";  }
            | BIGINT                 { $$="BIGINT";  }
            | BINARY_DOUBLE                 { $$="BINARY_DOUBLE";  }
            | BINARY_FLOAT                 { $$="BINARY_FLOAT";  }
            | BIT                 { $$="BIT";  }
            | BODY                 { $$="BODY";  }
            | BOOLEAN_P                 { $$="BOOLEAN";  }
            | BUFFER_POOL		{ $$="BUFFER_POOL"; }
            /*| BY                 { $$="BY";  }*/
            | CACHE                 { $$="CACHE";  }
            | CALL                 { $$="CALL";  }
            | CALLED                 { $$="CALLED";  }
            | CASCADE                 { $$="CASCADE";  }
            | CASCADED                 { $$="CASCADED";  }
            | CATALOG_P                 { $$="CATALOG";  }
            | CELL_FLASH_CACHE		{ $$="CELL_FLASH_CACHE"; }
            | CHAIN                 { $$="CHAIN";  }
            | CHARACTERISTICS                 { $$="CHARACTERISTICS";  }
            | CHECKPOINT                 { $$="CHECKPOINT";  }
            | CLASS                 { $$="CLASS";  }
            | CLEAN                 { $$="CLEAN";  }
            | CLOSE                 { $$="CLOSE";  }
            | CLUSTER                 { $$="CLUSTER";  }
            /*| COALESCE                 { $$="COALESCE";  }*/
            | COLUMNS                 { $$="COLUMNS";  }
            | COMMENT                 { $$="COMMENT";  }
            | COMMENTS                 { $$="COMMENTS";  }
            | COMMIT                 { $$="COMMIT";  }
            | COMMIT_SUBTXN                 { $$="COMMIT_SUBTXN";  }
            | COMMITTED                 { $$="COMMITTED";  }
            | CONFIGURATION                 { $$="CONFIGURATION";  }
            | CONFLICT                 { $$="CONFLICT";  }
            | CONNECTION                 { $$="CONNECTION";  }
            | CONSTRAINTS                 { $$="CONSTRAINTS";  }
            | CONTENT_P                 { $$="CONTENT";  }
            | CONTINUE_P                 { $$="CONTINUE";  }
            | CONVERSION_P                 { $$="CONVERSION";  }
            | COORDINATOR                 { $$="COORDINATOR";  }
            | COPY                 { $$="COPY";  }
            | COST                 { $$="COST";  }
            | CSV                 { $$="CSV";  }
            | CUBE                 { $$="CUBE";  }
            | CURRENT_P                 { $$="CURRENT";  }
            | CURSOR                 { $$="CURSOR";  }
            | CYCLE                 { $$="CYCLE";  }
            | DATA_P                 { $$="DATA";  }
            | DATABASE                 { $$="DATABASE";  }
            | DEALLOCATE                 { $$="DEALLOCATE";  }
            | DEC                 { $$="DEC";  }
            | DECIMAL_P                 { $$="DECIMAL";  }
            | DECLARE                 { $$="DECLARE";  }
            | DEFAULTS                 { $$="DEFAULTS";  }
            | DEFERRED                 { $$="DEFERRED";  }
            | DEFINER                 { $$="DEFINER";  }
            | DELIMITER                 { $$="DELIMITER";  }
            | DELIMITERS                 { $$="DELIMITERS";  }
            | DEPENDS                 { $$="DEPENDS";  }
            | DETACH                 { $$="DETACH";  }
            | DETERMINISTIC          { $$="deterministic"; }
            | DICTIONARY                 { $$="DICTIONARY";  }
            | DIRECT                 { $$="DIRECT";  }
            | DISABLE_P                 { $$="DISABLE";  }
            | DISCARD                 { $$="DISCARD";  }
            | DISTKEY                 { $$="DISTKEY";  }
            | DISTRIBUTE                 { $$="DISTRIBUTE";  }
            | DISTRIBUTED                 { $$="DISTRIBUTED";  }
            | DISTSTYLE                 { $$="DISTSTYLE";  }
			| DO_LA                 { $$ = "DO"; }
            | DOCUMENT_P                 { $$="DOCUMENT";  }
            | DOMAIN_P                 { $$="DOMAIN";  }
            | DOUBLE_P                 { $$="DOUBLE";  }
            | DROP                 { $$="DROP";  }
            | EACH                 { $$="EACH";  }
            | EDITIONABLE                 { $$="EDITIONABLE";  }
            | ENABLE_P                 { $$="ENABLE";  }
            | ENCODING                 { $$="ENCODING";  }
            | ENCRYPT                 { $$="ENCRYPT";  }
            | ENCRYPTED                 { $$="ENCRYPTED";  }
            | ENUM_P                 { $$="ENUM";  }
            | ERROR_P                 { $$="ERROR";  }
            | ESCAPE                 { $$="ESCAPE";  }
            | EVENT                 { $$="EVENT";  }
            | EXCHANGE                 { $$="EXCHANGE";  }
            | EXCLUDE                 { $$="EXCLUDE";  }
            | EXCLUDING                 { $$="EXCLUDING";  }
            | EXCLUSIVE                 { $$="EXCLUSIVE";  }
            | EXEC                 { $$="EXEC";  }
            | EXECUTE                 { $$="EXECUTE";  }
            | EXISTS                 { $$="EXISTS";  }
            | EXPLAIN                 { $$="EXPLAIN";  }
            | EXTENSION                 { $$="EXTENSION";  }
            | EXTENT                 { $$="EXTENT";  }
            | EXTERNAL                 { $$="EXTERNAL";  }
            /*| EXTRACT                 { $$="EXTRACT";  }*/
            | FAMILY                 { $$="FAMILY";  }
            | FIRST_P                 { $$="FIRST";  }
			| FLASH_CACHE                 { $$="FLASH_CACHE";  }
            | FLOAT_P                 { $$="FLOAT";  }
            | FOLLOWING                 { $$="FOLLOWING";  }
            | FORCE                 { $$="FORCE";  }
            | FORWARD                 { $$="FORWARD";  }
            | FREELIST                 { $$="FREELIST";  }
            | FREELISTS                 { $$="FREELISTS";  }
            | FUNCTION                 { $$="FUNCTION";  }
            | FUNCTIONS                 { $$="FUNCTIONS";  }
            | GENERATED                 { $$="GENERATED";  }
            | GLOBAL                 { $$="GLOBAL";  }
            | GRANTED                 { $$="GRANTED";  }
			| GROUPS                 { $$="GROUPS";  }
            | GROUPING                 { $$="GROUPING";  }
			| GROUPING_ID              { $$="GROUPING_ID";}
            | GTM                 { $$="GTM";  }
            | HANDLER                 { $$="HANDLER";  }
            | HEADER_P                 { $$="HEADER";  }
            | HOLD                 { $$="HOLD";  }
            | IDENTITY_P                 { $$="IDENTITY";  }
            | IF_P                 { $$="IF";  }
            | IMMEDIATE                 { $$="IMMEDIATE";  }
            | IMMUTABLE                 { $$="IMMUTABLE";  }
            | IMPLICIT_P                 { $$="IMPLICIT";  }
            | IMPORT_P                 { $$="IMPORT";  }
            | INCLUDING                 { $$="INCLUDING";  }
            | INCREMENT                 { $$="INCREMENT";  }
            | INDEX                 { $$="INDEX";  }
            | INDEXES                 { $$="INDEXES";  }
            | INHERIT                 { $$="INHERIT";  }
            | INHERITS                 { $$="INHERITS";  }
            | INITIAL                 { $$="INITIAL";  }
            | INITRANS                 { $$="INITRANS";  }
            | INLINE_P                 { $$="INLINE";  }
            | INOUT                 { $$="INOUT";  }
            | INPUT_P                 { $$="INPUT";  }
            | INSENSITIVE                 { $$="INSENSITIVE";  }
            | INSERT                 { $$="INSERT";  }
            | INSTEAD                 { $$="INSTEAD";  }
            | INT_P                 { $$="INT";  }
            | INTEGER                 { $$="INTEGER";  }
            | INTERVAL                 { $$="INTERVAL";  }
            | INVOKER                 { $$="INVOKER";  }
            | ISOLATION                 { $$="ISOLATION";  }
/*            | KEEP                 { $$="KEEP";  }*/
            | KEY                 { $$="KEY";  }
            | LANGUAGE                 { $$="LANGUAGE";  }
            | LARGE_P                 { $$="LARGE";  }
            | LAST_P                 { $$="LAST";  }
            | LEAKPROOF                 { $$="LEAKPROOF";  }
			| LEVEL_LA              { $$="LEVEL"; }
            | LISTEN                 { $$="LISTEN";  }
            | LOAD                 { $$="LOAD";  }
            | LOCAL                 { $$="LOCAL";  }
            | LOCATION                 { $$="LOCATION";  }
            | LOCK_P                 { $$="LOCK";  }
            | LOCKED                 { $$="LOCKED";  }
            | LOGGED                 { $$="LOGGED";  }
            | LONG                   { $$="LONG";  }
            | MAPPING                 { $$="MAPPING";  }
            | MATCH                 { $$="MATCH";  }
            | MATCHED                 { $$="MATCHED";  }
            | MATERIALIZED                 { $$="MATERIALIZED";  }
            | MAXEXTENTS                 { $$="MAXEXTENTS";  }
            | MAXSIZE                 { $$="MAXSIZE";  }
            | MAXTRANS                 { $$="MAXTRANS";  }
            | MAXVALUE                 { $$="MAXVALUE";  }
            | MERGE                 { $$="MERGE";  }
            | METHOD                 { $$="METHOD";  }
            | MINEXTENTS                 { $$="MINEXTENTS";  }
            | MINVALUE                 { $$="MINVALUE";  }
            | MODE                 { $$="MODE";  }
            | MOVE                 { $$="MOVE";  }
            /*| NAME_P                 { $$="NAME";  }*/
            | NATIONAL                 { $$="NATIONAL";  }
            | NCHAR                 { $$="NCHAR";  }
            | NCHAR_CS                 { $$ = "NCHAR_CS";	}
            /*| NEW                 { $$="NEW";  }*/
            | NEXT                 { $$="NEXT";  }
            | NO                 { $$="NO";  }
            /*| NODE                 { $$="NODE";  }*/
            | NONE                 { $$="NONE";  }
            | NONEDITIONABLE                 { $$="NONEDITIONABLE";  }
            | NOTHING                 { $$="NOTHING";  }
            | NOTIFY                 { $$="NOTIFY";  }
            | NOVALIDATE             { $$="NOVALIDATE";  }
            | NOWAIT                 { $$="NOWAIT";  }
            | NULLS_P                 { $$="NULLS";  }
            | NUMBER                 { $$="NUMBER";  }
            | NUMERIC                 { $$="NUMERIC";  }
            | OBJECT_P                 { $$="OBJECT";  }
            | OF                 { $$="OF";  }
            | OFF                 { $$="OFF";  }
            | OID_P                 { $$="OID";  }
            | OIDS                 { $$="OIDS";  }
            | OPERATOR                 { $$="OPERATOR";  }
            | OPTIMAL                 { $$="OPTIMAL";  }
            | OPTION                 	{ $$="OPTION";  }
            | OPTIONS                 	{ $$="OPTIONS";  }
            | ORDINALITY                 { $$="ORDINALITY";  }
            | ORDER_S                    { $$="ORDER_SIBLINGS_BY"; }
            | OUT_P                 		{ $$="OUT";  }
			| LA_OVER                   { $$ = "OVER"; }
            | OVERFLOW                 { $$="OVERFLOW";  }
            | OVERRIDING                 { $$="OVERRIDING";  }
            | OWNED                 { $$="OWNED";  }
            | OWNER                 { $$="OWNER";  }
            | PACKAGE                 { $$="PACKAGE";  }
			| PACKAGES                { $$="PACKAGES"; }
            | PARALLEL                 { $$="PARALLEL";  }
			| PARALLEL_ENABLE			{ $$="parallel_enable"; }
            | PARSER                 { $$="PARSER";  }
            | PARTIAL                 { $$="PARTIAL";  }
            | PASSING                 { $$="PASSING";  }
            /*| PASSWORD                 { $$="PASSWORD";  }*/
            | PAUSE                 { $$="PAUSE";  }
            | PCTFREE                 { $$="PCTFREE";  }
            | PCTINCREASE                 { $$="PCTINCREASE";  }
            | PCTUSED                 { $$="PCTUSED";  }
			| PERCENT				{ $$ = "PERCENT"; }
            | PLANS                 { $$="PLANS";  }
            | POLICY                 { $$="POLICY";  }
            | POSITION                 { $$="POSITION";  }
            | PRECEDING                 { $$="PRECEDING";  }
            /*| PRECISION                 { $$="PRECISION";  }*/
            | PREFERRED                 { $$="PREFERRED";  }
            | PREPARE                 { $$="PREPARE";  }
            | PREPARED                 { $$="PREPARED";  }
            | PRESERVE                 { $$="PRESERVE";  }
            | PRIVILEGES                 { $$="PRIVILEGES";  }
            | PROCEDURAL                 { $$="PROCEDURAL";  }
            | PROCEDURE                 { $$="PROCEDURE";  }
            | PROCEDURES                 { $$="PROCEDURES";  }
            | PROFILE                 { $$="PROFILE";  }
            | PROGRAM                 { $$="PROGRAM";  }
			| PROMPT					{ $$ = "PROMPT"; }
            | PUBLICATION                 { $$="PUBLICATION";  }
            | PUSHDOWN                 { $$="PUSHDOWN";  }
            | QUOTE                 { $$="QUOTE";  }
            | RANDOMLY                 { $$="RANDOMLY";  }
            | RANGE                 { $$="RANGE";  }
            | READ                 { $$="READ";  }
            | REAL                 { $$="REAL";  }
            | REASSIGN                 { $$="REASSIGN";  }
            | REBUILD                 { $$="REBUILD";  }
            | RECHECK                 { $$="RECHECK";  }
            | RECORD_P                 { $$="RECORD";  }
            | RECURSIVE                 { $$="RECURSIVE";  }
			| RECYCLE                 { $$="RECYCLE";  }
            | REF                 { $$="REF";  }
            | REFERENCING                 { $$="REFERENCING";  }
            | REFRESH                 { $$="REFRESH";  }
            | REINDEX                 { $$="REINDEX";  }
            | RELATIVE_P                 { $$="RELATIVE";  }
            | RELEASE                 { $$="RELEASE";  }
            | RENAME                 { $$="RENAME";  }
            | REPEATABLE                 { $$="REPEATABLE";  }
            | REPLICA                 { $$="REPLICA";  }
            | RESET                 { $$="RESET";  }
            | RESOURCE                 { $$="RESOURCE";  }
            | RESTART                 { $$="RESTART";  }
            | RESTRICT                 { $$="RESTRICT";  }
			| RESULT_P                 { $$="RESULT";}
			| RESULT_CACHE				{ $$="RESULT_CACHE"; }
            | RETURNS                 { $$="RETURNS";  }
            | REVOKE                 { $$="REVOKE";  }
            | ROLE                 { $$="ROLE";  }
            | ROLLBACK                 { $$="ROLLBACK";  }
            | ROLLBACK_SUBTXN                 { $$="ROLLBACK_SUBTXN";  }
            | ROLLUP                 { $$="ROLLUP";  }
            | ROUTINE                 { $$="ROUTINE";  }
            | ROUTINES                 { $$="ROUTINES";  }
            /*| ROW                 { $$="ROW";  }*/
            | ROWS                 { $$="ROWS";  }
            | ROWTYPE_P                 { $$="ROWTYPE";  }
            | RULE                 { $$="RULE";  }
            | SAMPLE                 { $$="SAMPLE";  }
            | SAVEPOINT                 { $$="SAVEPOINT";  }
            | SCHEMA                 { $$="SCHEMA";  }
            | SCHEMAS                 { $$="SCHEMAS";  }
            | SCROLL                 { $$="SCROLL";  }
            | SEARCH                 { $$="SEARCH";  }
            | SECURITY                 { $$="SECURITY";  }
            | SEQUENCE                 { $$="SEQUENCE";  }
            | SEQUENCES                 { $$="SEQUENCES";  }
            | SERIALIZABLE                 { $$="SERIALIZABLE";  }
            | SERVER                 { $$="SERVER";  }
            | SESSION                 { $$="SESSION";  }
            | SET                 { $$="SET";  }
            | SETOF                 { $$="SETOF";  }
            | SETS                 { $$="SETS";  }
            | SHARDING                 { $$="SHARDING";  }
            | SHARE                 { $$="SHARE";  }
            | SHARING                 { $$="SHARING";  }
            | SHOW                 { $$="SHOW";  }
            | SIBLINGS               { $$="SIBLINGS";  }
            | SIMPLE                 { $$="SIMPLE";  }
            | SKIP                 { $$="SKIP";  }
            | SLOT                 { $$="SLOT";  }
            | SMALLINT                 { $$="SMALLINT";  }
            | SNAPSHOT                 { $$="SNAPSHOT";  }
            | SQL_P                 { $$="SQL";  }
            | STABLE                 { $$="STABLE";  }
            | STANDALONE_P                 { $$="STANDALONE";  }
            | STATEMENT                 { $$="STATEMENT";  }
            | STATISTICS                 { $$="STATISTICS";  }
            | STDIN                 { $$="STDIN";  }
            | STDOUT                 { $$="STDOUT";  }
            | STEP                 { $$="STEP";  }
            | STORAGE                 { $$="STORAGE";  }
            | STRICT_P                 { $$="STRICT";  }
			| STRIP_P                 { $$="STRIP";  }
            | SUBSCRIPTION                 { $$="SUBSCRIPTION";  }
            | SYSID                 { $$="SYSID";  }
            | SYSTEM_P                 { $$="SYSTEM";  }
            | TABLES                 { $$="TABLES";  }
            | TABLESPACE                 { $$="TABLESPACE";  }
            | OPENTENBASE_P                 { $$="OPENTENBASE";  }
            | TEMP                 { $$="TEMP";  }
            | TEMPLATE                 { $$="TEMPLATE";  }
            | TEMPORARY                 { $$="TEMPORARY";  }
            /*| TEXT_P                 { $$="TEXT";  }*/
            | TIME                 { $$="TIME";  }
            | TIMESTAMP                 { $$="TIMESTAMP";  }
            | TRANSACTION                 { $$="TRANSACTION";  }
            | TRANSFORM                 { $$="TRANSFORM";  }
            | TREAT                 { $$="TREAT";  }
            | TRIGGER                 { $$="TRIGGER";  }
            /*| TRIM                 { $$="TRIM";  }*/
            | TRUNCATE                 { $$="TRUNCATE";  }
            | TRUSTED                 { $$="TRUSTED";  }
            | TYPE_P                 { $$="TYPE";  }
            | TYPES_P                 { $$="TYPES";  }
            | UNBOUNDED                 { $$="UNBOUNDED";  }
            | UNCOMMITTED                 { $$="UNCOMMITTED";  }
            | UNENCRYPTED                 { $$="UNENCRYPTED";  }
            | UNKNOWN                 { $$="UNKNOWN";  }
            | UNLIMITED                 { $$="UNLIMITED";  }
            | UNLISTEN                 { $$="UNLISTEN";  }
            | UNLOCK_P                 { $$="UNLOCK";  }
            | UNLOGGED                 { $$="UNLOGGED";  }
            | UNPAUSE                 { $$="UNPAUSE";  }
            | UNTIL                 { $$="UNTIL";  }
            | UPDATE                 { $$="UPDATE";  }
            | VACUUM                 { $$="VACUUM";  }
            | VALID                 { $$="VALID";  }
            | VALIDATE                 { $$="VALIDATE";  }
            | VALIDATOR                 { $$="VALIDATOR";  }
            | VALUE_P                 { $$="VALUE";  }
            | VALUES                 { $$="VALUES";  }
            /*| VARCHAR                 { $$="VARCHAR";  } */
	    | VERSION_P                 { $$="VERSION";  }
            | VIEW                 { $$="VIEW";  }
            | VIEWS                 { $$="VIEWS";  }
			| WAIT					{ $$="WAIT"; }
            | VOLATILE                 { $$="VOLATILE";  }
            | WHITESPACE_P                 { $$="WHITESPACE";  }
            | WORK                 { $$="WORK";  }
            | WRAPPER                 { $$="WRAPPER";  }
            | WRITE                 { $$="WRITE";  }
            | XML_P                 { $$="XML";  }
            | XMLATTRIBUTES                 { $$="XMLATTRIBUTES";  }
            | XMLCONCAT                 { $$="XMLCONCAT";  }
            | XMLELEMENT                 { $$="XMLELEMENT";  }
            | XMLEXISTS                 { $$="XMLEXISTS";  }
            | XMLFOREST                 { $$="XMLFOREST";  }
            | XMLNAMESPACES                 { $$="XMLNAMESPACES";  }
            | XMLPARSE                 { $$="XMLPARSE";  }
            | XMLPI                 { $$="XMLPI";  }
            | XMLROOT                 { $$="XMLROOT";  }
            | XMLSERIALIZE                 { $$="XMLSERIALIZE";  }
            | XMLTABLE                 { $$="XMLTABLE";  }
            | YES_P                 { $$="YES";  }
            | ZONE                 { $$="ZONE";  }
            ;

PkgId:		Common_IDENT							{ $$ = $1; }
			| opentenbase_ora_any_ident                      { $$ = $1; }
			| unreserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
			| col_name_keyword						{ $$ = upcase_identifier($1, strlen($1)); }
			| type_func_name_keyword				{ $$ = upcase_identifier($1, strlen($1)); }
		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	Common_IDENT					{ $$ = $1; }
			| unreserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
			| type_func_name_keyword				{ $$ = upcase_identifier($1, strlen($1)); }
			| pkg_reserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
		;

/* Any not-fully-reserved word --- these names can be, eg, role names.
 */
NonReservedWord:	Common_IDENT					{ $$ = $1; }
			| unreserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
			| col_name_keyword						{ $$ = upcase_identifier($1, strlen($1)); }
			| type_func_name_keyword				{ $$ = upcase_identifier($1, strlen($1)); }
			| pkg_reserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	Common_IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
			| col_name_keyword						{ $$ = upcase_identifier($1, strlen($1)); }
			| type_func_name_keyword				{ $$ = upcase_identifier($1, strlen($1)); }
			| reserved_keyword						{ $$ = upcase_identifier($1, strlen($1)); }
			| pkg_reserved_keyword					{ $$ = upcase_identifier($1, strlen($1)); }
		;

/* Need to downcase the identifier in these rules */
Common_IDENT:
				IDENT									{ $$ = $1; }
				| IDENT_IN_D_QUOTES						{ $$ = $1; }
		;
Lower_IDENT:
				IDENT									{ $$ = downcase_identifier($1, strlen($1), false, false); lower_ident_as_const_str = true; }
				| IDENT_IN_D_QUOTES						{ $$ = $1; lower_ident_as_const_str = false; }
		;
lower_name:	Lower_ColId									{ $$ = $1; };
Lower_NonReservedWord:
				Lower_IDENT								{ $$ = $1; }
				| unreserved_keyword					{ $$ = pstrdup($1); lower_ident_as_const_str = true; }
				| col_name_keyword						{ $$ = pstrdup($1); lower_ident_as_const_str = true; }
				| type_func_name_keyword				{ $$ = pstrdup($1); lower_ident_as_const_str = true; }
				| pkg_reserved_keyword					{ $$ = pstrdup($1); lower_ident_as_const_str = true; }
		;
Lower_NonReservedWord_or_Sconst:
				Lower_NonReservedWord					{ $$ = $1; }
				| Sconst								{ $$ = $1; lower_ident_as_const_str = false; }
		;
Lower_ColLabel:
				Lower_IDENT								{ $$ = $1; }
				| unreserved_keyword					{ $$ = pstrdup($1); }
				| col_name_keyword						{ $$ = pstrdup($1); }
				| type_func_name_keyword				{ $$ = pstrdup($1); }
				| reserved_keyword						{ $$ = pstrdup($1); }
				| pkg_reserved_keyword					{ $$ = pstrdup($1); }
		;
Lower_ColId:
				Lower_IDENT								{ $$ = $1; }
				| unreserved_keyword					{ $$ = pstrdup($1); }
				| col_name_keyword						{ $$ = pstrdup($1); }
				| pkg_reserved_keyword					{ $$ = pstrdup($1); }
		;
lower_name_list:
				lower_name								{ $$ = list_make1(makeString($1)); }
				| lower_name_list ',' lower_name		{ $$ = lappend($1, makeString($3)); }
		;
lower_attr_name:	Lower_ColLabel						{ $$ = $1; };
lower_attrs:
				'.' lower_attr_name						{ $$ = list_make1(makeString($2)); }
				| lower_attrs '.' lower_attr_name		{ $$ = lappend($1, makeString($3)); }
		;
lower_any_name_list:
				lower_any_name								{ $$ = list_make1($1); }
				| lower_any_name_list ',' lower_any_name	{ $$ = lappend($1, $3); }
		;
lower_any_name:
				Lower_ColId								{ $$ = list_make1(makeString($1)); }
				| Lower_ColId lower_attrs				{ $$ = lcons(makeString($1), $2); }
		;
lower_comment_type_name:
			ACCESS METHOD								{ $$ = OBJECT_ACCESS_METHOD; }
			| EXTENSION									{ $$ = OBJECT_EXTENSION; }
			| opt_procedural LANGUAGE					{ $$ = OBJECT_LANGUAGE; }
		;
lower_security_label_type_name:	opt_procedural LANGUAGE			{ $$ = OBJECT_LANGUAGE; };
lower_type_any_name:
			TEXT_P SEARCH PARSER						{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY					{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE					{ $$ = OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION				{ $$ = OBJECT_TSCONFIGURATION; }
			| COLLATION									{ $$ = OBJECT_COLLATION; }
		;
lower_drop_type_name:
			ACCESS METHOD								{ $$ = OBJECT_ACCESS_METHOD; }
			| EXTENSION									{ $$ = OBJECT_EXTENSION; }
		;
/*
 * Keyword category lists.  Generally, every keyword present in
 * the Postgres grammar should appear in exactly one of these lists.
 *
 * Put a new keyword into the first list that it can go into without causing
 * shift or reduce conflicts.  The earlier lists define "less reserved"
 * categories of keywords.
 *
 * Make sure that each keyword's category in kwlist.h matches where
 * it is listed here.  (Someday we may be able to generate these lists and
 * kwlist.h's table from a common master list.)
 */

/* "Unreserved" keywords --- available for use as any kind of name.
 */
/* PGXC - added DISTRIBUTE, DISTRIBUTED, RANDOMLY, DIRECT, COORDINATOR, CLEAN, NODE, BARRIER */
unreserved_keyword:
			  ABORT_P
			| ABSOLUTE_P
			| ACCESS
/* OPENTENBASE_ORA PROFILE BEGIN */
			| ACCESSIBLE
			| ACCOUNT
/* OPENTENBASE_ORA PROFILE END */
			| ACTION
			| ADD_P
			| ADMIN
			| AFTER
			| AGGREGATE
			| ALSO
			| ALTER
			| ALWAYS
			| ASSERTION
			| ASSIGNMENT
			| AT
			| ATTACH
			| ATTRIBUTE
/* BEGIN_OPENTENBASE_ORA */
			| AUTHID
/* END_OPENTENBASE_ORA */
			| BACKWARD
/* PGXC_BEGIN */
			| BARRIER
/* PGXC_END */
			| BEFORE
			| BEGIN_P
			| BEGIN_SUBTXN
			| BINARY
			| BUFFER_POOL
			| CACHE
			| CALL
			| CALLED
			| CASCADE
			| CASCADED
			| CATALOG_P
			| CELL_FLASH_CACHE
			| CHAIN
			| CHARACTERISTICS
			| CHECKPOINT
			| CHECKSUM
			| CLASS
			| CLEAN
			| CLOSE
			| CLUSTER
			| COLUMNS
			| COMMENT
			| COMMENTS
			| COMMIT
			| COMMITTED
			| COMMIT_SUBTXN
			| COMPATIBLE_ILLEGAL_CHARS
			| COMPILE
			| CONCURRENCY
			| CONFIGURATION
			| CONFLICT
			| CONNECTION
			| CONSTRAINTS
			| CONSTRUCTOR_P
			| CONTENT_P
			| CONTINUE_P
			| CONVERSION_P
			| COORDINATOR
			| COPY
			| COST
			| COUNT
			| CPUSET
			| CPU_RATE_LIMIT
			| CSV
			| CUBE
			| CURRENT_P
			| CURSOR
			| CYCLE
			| DATA_P
			| DATABASE
			| DAY_P
			| DEALLOCATE
			| DEBUG
			| DECLARE
			| DEFAULTS
			| DEFERRED
			| DEFINER
			| DELIMITER
			| DELIMITERS
			| DEPENDS
			| DETACH
			| DETERMINISTIC
			| DICTIONARY
			| DIRECT
			| DIRECTORY
			| DISABLE_P
			| DISCARD
/* PGXC_BEGIN */
			| DISTKEY
			| DISTRIBUTE
			| DISTRIBUTED
			| DISTSTYLE
/* PGXC_END */
			| DOCUMENT_P
			| DOMAIN_P
			| DOUBLE_P
			| DROP
			| EACH
			| EDITIONABLE
			| ENABLE_P
			| ENCODING
			| ENCRYPT
			| ENCRYPTED
			| ENUM_P
			| EOL
			| ERROR_P
			| ERRORS
			| ESCAPE
			| EVENT
			| EXCHANGE
			| EXCLUDE
			| EXCLUDING
			| EXCLUSIVE
			| EXEC
			| EXECUTE
			| EXPLAIN
			| EXTENSION
/* _SHARDING_ */
			| EXTENT
/* _SHARDING_ END */
			| EXTERNAL
			| FAMILY
			| FIELDS
			| FILL
			| FILL_MISSING_FIELDS
			| FILTER
			| FIRST_P
			| FIXED_P
			| FLASH_CACHE
			| FOLLOWING
			| FORCE
			| FORMAT
			| FORMATTER
			| FORWARD
			| FREELIST
			| FREELISTS
			| FUNCTION
			| FUNCTIONS
			| GENERATED
			| GLOBAL
			| GRANTED
			| GROUPS
			| GTM
			| HANDLER
			| HEADER_P
			| HOLD
			| HOST
			| HOUR_P
			| IDENTITY_P
			| IF_P
			| IGNORE_EXTRA_DATA
			| IMMEDIATE
			| IMMUTABLE
			| IMPLICIT_P
			| IMPORT_P
/* _PG_ORCL_ */
			| INCLUDE
/* _PG_ORCL_ END */
			| INCLUDING
			| INCREMENT
			| INDEX
			| INDEXES
			| INHERIT
			| INHERITS
			| INITIAL
			| INITRANS
			| INLINE_P
			| INPUT_P
			| INSENSITIVE
			| INSERT
			| INSTEAD
			| INVALIDATE
			| INVOKER
			| ISOLATION
			| KEEP
			| KEY
			| LABEL
			| LANGUAGE
			| LARGE_P
			| LAST_P
			| LEAKPROOF
			| LEFT
			| LINK
			| LISTEN
			| LOAD
			| LOCAL
			| LOCATION
			| LOCK_P
			| LOCKED
			| LOG_P
			| LOGGED
			| MAPPING
			| MASTER
			| MATCH
			| MATCHED
			| MATERIALIZED
			| MAXEXTENTS
			| MAXSIZE
			| MAXTRANS
			| MAXVALUE
			| MEMBER_P
			| MEMORY_LIMIT
			| MERGE
			| METHOD
			| MINEXTENTS
			| MINUTE_P
			| MINVALUE
			| MISSING
			| MOD
			| MODE
			| MONTH_P
			| MOVE
			| MULTISET
			| NAME_P
			| NAMES
			| NCHAR_CS
			| NEW
			| NEWLINE
			| NEXT
			| NO
/* _PG_ORCL_ */
			| NOCACHE
/* _PG_ORCL_ END */
			| NODE
			| NOMAXVALUE
			| NOMINVALUE
			| NONEDITIONABLE
			| NOPARALLEL
			| NOTHING
			| NOTIFY
/* _PG_ORCL_ */
			| NOVALIDATE
/* _PG_ORCL_ END */
			| NOWAIT
			| NULLS_P
			| OBJECT_P
			| OF
			| OFF
/* PGPAL_BEGIN */
			| OID_P
/* PGPAL_END */
			| OIDS
			| OLD
/* __SUBSCRIPTION__ BEGIN */
			| OPENTENBASE_P
/* __SUBSCRIPTION__ END */
			| OPERATOR
			| OPTIMAL
			| OPTION
			| OPTIONS
			| ORDINALITY
			| OVER
			| OVERFLOW
			| OVERRIDING
			| OWNED
			| OWNER
			| PACKAGE
			| PACKAGES
			| PARALLEL
			| PARALLEL_ENABLE
			| PARSER
			| PARTIAL
/*
 * To adapt for opentenbase_ora, this key word 'partition' is allowed to use as table column name.
 * Changing 'partition' as unreserved key words
 */
			| PARTITION
/* interval partition BEGIN */
			| PARTITIONS
/* interval partition END */
			| PASSING
			| PASSWORD
/* PGXC_BEGIN */
			| PAUSE
/* PGXC_END */
			| PCTFREE
			| PCTINCREASE
			| PCTUSED
			| PERCENT
			| PERSISTENTLY
			| PIPELINED
			| PLANS
			| POLICY
			| PRECEDING
/* PGXC_BEGIN */
			| PREFERRED
/* PGXC_END */
			| PREPARE
			| PREPARED
			| PRESERVE
			| PRIORITY
			| PRIVILEGES
			| PROCEDURAL
			| PROCEDURE
			| PROCEDURES
/* OPENTENBASE_ORA PROFILE BEGIN */
            | PROFILE
/* OPENTENBASE_ORA PROFILE END */
			| PROGRAM
			| PROMPT
			| PUBLIC
			| PUBLICATION
			| PUSHDOWN
/* __RESOURCE_QUEUE__ BEGIN */
			| QUEUE
/* __RESOURCE_QUEUE__ END */
			| QUOTE
/* PGXC_BEGIN */
			| RANDOMLY
/* PGXC_END */
			| RANGE
			| READ
			| READABLE
			| REASSIGN
/* _SHARDING_ */
			| REBUILD
/* _SHARDING_ END */
			| RECHECK
			| RECORD_P
			| RECURSIVE
			| RECYCLE
			| REF
			| REFERENCING
			| REFRESH
			| REINDEX
			| REJECT_P
			| RELATIVE_P
			| RELEASE
			| RENAME
			| REPEATABLE
			| REPLACE
			| REPLICA
			| RESET
/* __RESOURCE_QUEUE__ BEGIN */
			| RESOURCE
/* __RESOURCE_QUEUE__ END */
			| RESTART
			| RESTRICT
			| RESULT_P
			| RESULT_CACHE
			| RETURN
			| RETURNS
			| REUSE
			| REVOKE
			| ROLE
			| ROLLBACK
			| ROLLBACK_SUBTXN
			| ROLLUP
			| ROUTINE
			| ROUTINES
			| ROWS
			| ROWTYPE_P
			| RULE
			| SAMPLE
			| SAVEPOINT
			| SCHEMA
			| SCHEMAS
			| SCROLL
			| SEARCH
			| SECOND_P
			| SECURITY
			| SEGMENT
			| SELF_P
			| SEQUENCE
			| SEQUENCES
			| SERIALIZABLE
			| SERVER
			| SESSION
			| SET
			| SETS
			| SETTINGS
/* _SHARDING_ BEGIN*/
			| SHARDING
/* _SHARDING_ END*/
			| SHARE
			| SHARED
			| SHARING
			| SHOW
			| SIBLINGS
			| SIMPLE
			| SKIP
			| SNAPSHOT
			| SPECIFICATION
			| SPLIT
			| SQL_P
			| STABLE
			| STANDALONE_P
			| STATEMENT
			| STATIC_P
			| STATISTICS
			| STDIN
			| STDOUT
/* interval partition BEGIN */
			| STEP
/* interval partition END */
			| STORAGE
			| STRICT_P
			| STRIP_P
			| SUBPARTITION
			| SUBPARTITIONS
			| SUBSCRIPTION
/* _PG_ORCL_ */
			| SYNONYM
/* _PG_ORCL_ END */
			| SYSID
			| SYSTEM_P
			| TABLES
			| TABLESPACE
			| TEMP
			| TEMPLATE
			| TEMPORARY
			| TEXT_P
			| TIES
			| TRANSACTION
			| TRANSFORM
			| TRIGGER
			| TRUNCATE
			| TRUSTED
			| TYPE_P
			| TYPES_P
			| UNBOUNDED
			| UNCOMMITTED
			| UNENCRYPTED
			| UNKNOWN
			| UNLIMITED
			| UNLISTEN
			| UNLOCK_P
			| UNLOGGED
/* PGXC_BEGIN */
			| UNPAUSE
/* PGXC_END */
			| UNTIL
			| UPDATE
			| USING_NLS_COMP 
			| VACUUM
			| VALID
			| VALIDATE
			| VALIDATOR
			| VALUE_P
			| VARYING
			| VERSION_P
			| VIEW
			| VIEWS
			| VOLATILE
			| WAIT
			| WEB
			| WELLFORMED
			| WHITESPACE_P
			| WITHIN
			| WITHOUT
			| WORK
			| WRAPPER
			| WRITABLE
			| WRITE
			| XML_P
			| YEAR_P
			| YES_P
			| ZONE
		;

opentenbase_ora_ident:
	opentenbase_ora_unreserved_keyword { $$ = upcase_identifier($1, strlen($1)); }
opentenbase_ora_refident:
	opentenbase_ora_refunreserved_keyword { $$ = upcase_identifier($1, strlen($1)); }
opentenbase_ora_colref_ident:
	opentenbase_ora_colref_unreserved { $$ = upcase_identifier($1, strlen($1)); }
opentenbase_ora_coldef_ident:
	opentenbase_ora_coldef_unreserved { $$ = upcase_identifier($1, strlen($1)); }
opentenbase_ora_any_ident:
	opentenbase_ora_any_unreserved { $$ = upcase_identifier($1, strlen($1)); }

opentenbase_ora_normal_alias_unreserved:
	/* pg reserved diff opentenbase_ora*/
	ANALYZE
	| ARRAY
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	| COLLATE
	/*| COLUMN*/
	| CONSTRAINT
	| CURRENT_DATE
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
	| CURRENT_USER
	| DBTIMEZONE
	| DEFERRABLE
	| END_P
	/*| EXCEPT*/
	| FALSE_P
	/*| FETCH*/
	| FOREIGN
	| INITIALLY
	| LATERAL_P
	| LEADING
	/*| LEVEL*/
	/*| LINK*/ /* supported case by case */
	/*| LIMIT*/ /* supported by guc allow_limit_ident */
	| LOCALTIME
	| LOCALTIMESTAMP
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	/*| OFFSET*/
	| ONLY
	| PRIMARY
	| REFERENCES
	/*| RETURNING*/ /* supported case by case */
	/*| ROWNUM*/
	| SESSIONTIMEZONE
	| SOME
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	| SYSTIMESTAMP
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	| USER
	/*| USING*/
	/*| WHEN*/
	/*| WHENEVER*/
	/*pg type_func_ diff opentenbase_ora*/
	| AUTHORIZATION
	/*| BINARY*/ /* supported case by case */
	| COLLATION
	/*| CROSS*/ /* supported case by case */
	| CURRENT_SCHEMA
	/*| FULL*/ /* supported case by case */
	/*| INNER_P*/ /* supported case by case */
	/*| JOIN*/
	/*| LEFT*/ /* supported case by case */
	/*| NATURAL*/ /* supported case by case */
	| OUTER_P
	| OVERLAPS
	/*| RIGHT*/
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	| CURRENT_CATALOG
	| CURRENT_ROLE
	| DO
	| PLACING
	| SESSION_USER
	| SHARDCLUSTER
	| SYMMETRIC
	| VARIADIC
	/*| WINDOW*/ /* supported case by case */
	| CONCURRENTLY
	| FREEZE
	| ILIKE
	| ISNULL
	| NOTNULL
	| SIMILAR
	/*| TABLESAMPLE*/
	| VERBOSE
	;
opentenbase_ora_table_alias_unreserved:
	/* pg reserved diff opentenbase_ora*/
	ANALYZE
	| ARRAY
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	| COLLATE
	/*| COLUMN*/
	| CONSTRAINT
	| CURRENT_DATE
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
	| CURRENT_USER
	| DBTIMEZONE
	| DEFERRABLE
	| END_P
	| EXCEPT
	| FALSE_P
	| FETCH
	| FOREIGN
	| INITIALLY
	| LATERAL_P
	| LEADING
	/*| LEVEL*/
	/*| LINK*/
	| LIMIT
	| LOCALTIME
	| LOCALTIMESTAMP
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	| OFFSET
	| ONLY
	| PRIMARY
	| REFERENCES
	/*| RETURNING*/ /* supported case by case */
	/*| ROWNUM*/
	| SESSIONTIMEZONE
	| SOME
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	| SYSTIMESTAMP
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	| USER
	/*| USING*/
	/*| WHEN*/
	/*| WHENEVER*/
	/*pg type_func_ diff opentenbase_ora*/
	| AUTHORIZATION
	/*| BINARY*/
	| COLLATION
	| CROSS
	| CURRENT_SCHEMA
	| FULL
	| INNER_P
	| JOIN
	/*| LEFT*/
	| NATURAL
	| OUTER_P
	| OVERLAPS
	| RIGHT
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	| CURRENT_CATALOG
	| CURRENT_ROLE
	| DO
	| PLACING
	| SESSION_USER
	| SHARDCLUSTER
	| SYMMETRIC
	| VARIADIC
	| WINDOW
	| CONCURRENTLY
	| FREEZE
	| ILIKE
	| ISNULL
	| NOTNULL
	| SIMILAR
	| TABLESAMPLE
	| VERBOSE
	;


opentenbase_ora_any_unreserved:
	/* pg reserved diff opentenbase_ora*/
	ANALYZE
	| ARRAY
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	/*| COLLATE*/
	/*| COLUMN*/
	| CONSTRAINT
	| CURRENT_DATE
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
	| CURRENT_USER
	| DBTIMEZONE
	| DEFERRABLE
	| END_P
	| EXCEPT
	| FALSE_P
	| FETCH
	| FOREIGN
	| INITIALLY
	| LATERAL_P
	| LEADING
	/*| LEVEL*/
	/*| LINK*/
	| LIMIT
	| LOCALTIME
	| LOCALTIMESTAMP
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	| OFFSET
	| ONLY
	| PRIMARY
	| REFERENCES
	| RETURNING
	/*| ROWNUM*/
	| SESSIONTIMEZONE
	| SOME
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	| SYSTIMESTAMP
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	| USER
	| USING
	| WHEN
	/*| WHENEVER*/
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	| CURRENT_CATALOG
	| CURRENT_ROLE
	| DO
	| PLACING
	| SESSION_USER
	| SHARDCLUSTER
	| SYMMETRIC
	| VARIADIC
	| WINDOW
	/*| CONCURRENTLY*/
	/*| FREEZE*/
	/*| ILIKE*/
	/*| ISNULL*/
	/*| NOTNULL*/
	/*| SIMILAR*/
	/*| TABLESAMPLE*/
	/*| VERBOSE*/
	;

/* for qualified_ref, suport tableref name */
opentenbase_ora_refunreserved_keyword:
	/*pg reserved diff opentenbase_ora*/
	ANALYZE
	| ARRAY
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	| COLLATE
	/*| COLUMN*/
	/*| CONSTRAINT*/
	/*| CURRENT_DATE*/
	/*| CURRENT_TIME*/
	/*| CURRENT_TIMESTAMP*/
	/*| CURRENT_USER*/
	/*| DBTIMEZONE*/
	| DEFERRABLE
	| END_P
	/*| EXCEPT*/
	| FALSE_P
	/*| FETCH*/
	| FOREIGN
	| INITIALLY
	/*| LATERAL_P*/
	| LEADING
	/*| LEVEL*/
	/*| LINK*/
	/*| LIMIT*/
	/*| LOCALTIME*/
	/*| LOCALTIMESTAMP*/
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	/*| OFFSET*/
	/*| ONLY*/
	| PRIMARY
	| REFERENCES
	/*| RETURNING*/ /* supported case by case */
	/*| ROWNUM*/
	/*| SESSIONTIMEZONE*/
	| SOME
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	/*| SYSTIMESTAMP*/
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	/*| USER*/
	| USING
	| WHEN
	/*| WHENEVER*/
	/*pg type_func_ diff opentenbase_ora*/
	| AUTHORIZATION
	/*| BINARY*/
	| COLLATION
	| CROSS
	/*| CURRENT_SCHEMA*/
	| FULL
	| INNER_P
	| JOIN
	/*| LEFT*/
	| NATURAL
	| OUTER_P
	| OVERLAPS
	| RIGHT
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	/*| CURRENT_CATALOG*/
	/*| CURRENT_ROLE*/
	| DO
	| PLACING
	/*| SESSION_USER*/
	/*| SHARDCLUSTER*/
	| SYMMETRIC
	| VARIADIC
	/*| WINDOW*/
	| CONCURRENTLY
	| FREEZE
	| ILIKE
	| ISNULL
	| NOTNULL
	| SIMILAR
	| TABLESAMPLE
	| VERBOSE
	;

/* rule for qualified_name, for example Create table,view,mat view name etc.  */
opentenbase_ora_unreserved_keyword:
	/*pg reserved diff opentenbase_ora*/
	ANALYZE
	| ARRAY
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	| COLLATE
	/*| COLUMN*/
	| CONSTRAINT
	| CURRENT_DATE
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
	| CURRENT_USER
	| DBTIMEZONE
	| DEFERRABLE
	| END_P
	| EXCEPT
	| FALSE_P
	| FETCH
	| FOREIGN
	| INITIALLY
	| LATERAL_P
	| LEADING
	/*| LEVEL*/
	/*| LINK*/
	| LIMIT
	| LOCALTIME
	| LOCALTIMESTAMP
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	| OFFSET
	| ONLY
	| PRIMARY
	| REFERENCES
	| RETURNING
	/*| ROWNUM*/
	| SESSIONTIMEZONE
	| SOME
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	| SYSTIMESTAMP
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	| USER
	| USING
	| WHEN
	/*| WHENEVER*/
	/* pg type_func_ diff opentenbase_ora */
	| AUTHORIZATION
	/*| BINARY*/
	| COLLATION
	| CROSS
	| CURRENT_SCHEMA
	| FULL
	| INNER_P
	| JOIN
	/*| LEFT*/
	| NATURAL
	| OUTER_P
	| OVERLAPS
	| RIGHT
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN*/
	/*| INOUT*/
	/*| OUT*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	| CURRENT_CATALOG
	| CURRENT_ROLE
	| DO
	| PLACING
	| SESSION_USER
	| SHARDCLUSTER
	| SYMMETRIC
	| VARIADIC
	| WINDOW
	/*| CONCURRENTLY*/
	| FREEZE
	| ILIKE
	| ISNULL
	| NOTNULL
	| SIMILAR
	| TABLESAMPLE
	/*| VERBOSE*/
	;

opentenbase_ora_colref_unreserved:
	/*pg reserved diff opentenbase_ora*/
	ANALYZE
	/*| ARRAY*/
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH %prec CONCURRENTLY
	/*| CASE*/
	| CAST
	| COLLATE
	/*| COLUMN*/
	/*| CONSTRAINT*/
	/*| CURRENT_DATE*/
	/*| CURRENT_TIME*/
	/*| CURRENT_TIMESTAMP*/
	/*| CURRENT_USER*/
	/*| DBTIMEZONE*/
	/*| DEFERRABLE*/ /* supported case by case */
	/*| END_P*/
	/*| EXCEPT*/
	/*| FALSE_P*/
	/*| FETCH*/
	| FOREIGN
	/*| INITIALLY*/ /* supported case by case */
	| LATERAL_P
	| LEADING %prec CONCURRENTLY
	/*| LEVEL*/
	/*| LIMIT*/
	/*| LINK*/ /* supported case by case */
	/*| LOCALTIME*/
	/*| LOCALTIMESTAMP*/
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/ /* supported */
	/*| OFFSET*/
	| ONLY
	/*| PRIMARY*/ /* supported case by case */
	/*| REFERENCES*/ /* supported case by case */
	/*| RETURNING*/ /* supported case by case */
	/*| ROWNUM*/
	/*| SESSIONTIMEZONE*/
	/*| SOME*/ /* supported case by case */
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	/*| SYSTIMESTAMP*/
	| TRAILING %prec CONCURRENTLY
	/*| TRUE_P*/
	/*| UID_P*/
	/*| USER*/
	/*| USING*/
	/*| WHEN*/
	/*| WHENEVER*/
	/*pg type_func_ diff opentenbase_ora*/
	| AUTHORIZATION
	/*| BINARY*/ /* supported case by case */
	| COLLATION
	| CROSS
	/*| CURRENT_SCHEMA*/
	| FULL
	| INNER_P
	| JOIN
	/*| LEFT*/ /* supported case by case */
	| NATURAL
	| OUTER_P
	| OVERLAPS
	| RIGHT
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	/*| ASYMMETRIC*/ /* supported case by case*/
	/*| CURRENT_CATALOG*/
	/*| CURRENT_ROLE*/
	/*| DO*//*case by case*/
	/*| PLACING*/ /*case by case*/
	/*| SESSION_USER*/
	| SHARDCLUSTER
	/*| SYMMETRIC*//* supported case by case*/
	/*| VARIADIC*/ /*case by case*/
	/*| WINDOW*/ /*case by case*/
	| CONCURRENTLY
	| FREEZE
	| ILIKE
	| ISNULL
	| NOTNULL
	| SIMILAR
	| TABLESAMPLE
	| VERBOSE
	;
opentenbase_ora_coldef_unreserved:
	/*pg reserved diff opentenbase_ora*/
	ANALYZE
	| ARRAY
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	| COLLATE
	/*| COLUMN*/
	/*| CONSTRAINT*/
	| CURRENT_DATE
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
	| CURRENT_USER
	| DBTIMEZONE
	| DEFERRABLE
	| END_P
	| EXCEPT
	| FALSE_P
	| FETCH
	/*| FOREIGN*/
	| INITIALLY
	| LATERAL_P
	| LEADING
	/*| LEVEL*/
	/*| LINK*/
	| LIMIT
	| LOCALTIME
	| LOCALTIMESTAMP
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	| OFFSET
	| ONLY
	/*| PRIMARY*/ /* supported case by case */
	| REFERENCES
	| RETURNING
	/*| ROWNUM*/
	| SESSIONTIMEZONE
	| SOME
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	| SYSTIMESTAMP
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	| USER
	| USING
	| WHEN
	/*| WHENEVER*/
	/*type_func_ diff opentenbase_ora*/
	| AUTHORIZATION
	/*| BINARY*/
	| COLLATION
	| CROSS
	| CURRENT_SCHEMA
	| FULL
	| INNER_P
	| JOIN
	/*| LEFT*/
	| NATURAL
	| OUTER_P
	| OVERLAPS
	| RIGHT
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	| CURRENT_CATALOG
	| CURRENT_ROLE
	| DO
	| PLACING
	| SESSION_USER
	| SHARDCLUSTER
	| SYMMETRIC
	| VARIADIC
	| WINDOW
	| CONCURRENTLY
	| FREEZE
	| ILIKE
	| ISNULL
	| NOTNULL
	| SIMILAR
	| TABLESAMPLE
	| VERBOSE
	;

opentenbase_ora_alias_unreserved:
	/*pg reserved diff opentenbase_ora*/
	ANALYZE
	/*| ARRAY*/
	/*| AUDIT*/
	| AUTHENTICATED
	| BOTH
	| CASE
	| CAST
	/*| COLLATE*/
	/*| COLUMN*/
	| CONSTRAINT
	| CURRENT_DATE
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
	| CURRENT_USER
	| DBTIMEZONE
	| DEFERRABLE
	| END_P
	/*| EXCEPT*/
	| FALSE_P
	/*| FETCH*/
	| FOREIGN
	| INITIALLY
	| LATERAL_P
	| LEADING
	/*| LEVEL*/
	/*| LINK*/ /* supported case by case */
	/*| LIMIT*/
	| LOCALTIME
	| LOCALTIMESTAMP
	/*| NOAUDIT*/
	| NOCYCLE
	/*| NOORDER*/
	/*| OFFSET*/
	| ONLY
	| PRIMARY
	| REFERENCES
	/*| RETURNING*/ /* supported case by case */
	/*| ROWNUM*/
	| SESSIONTIMEZONE
	/*| SOME*/
	/*| SUCCESSFUL*/
	/*| SYSDATE*/
	| SYSTIMESTAMP
	| TRAILING
	| TRUE_P
	/*| UID_P*/
	| USER
	| USING
	| WHEN
	/*| WHENEVER*/
	/* type_func_ diff opentenbase_ora */
	| AUTHORIZATION
	/*| BINARY*/ /* supported case by case */
	| COLLATION
	| CURRENT_SCHEMA
	| FULL
	| INNER_P
	| JOIN
	/*| LEFT*/ /* supported case by case */
	| NATURAL
	| OUTER_P
	/*| OVERLAPS*/ /* supported case by case */
	| RIGHT
	/* col_name_ diff opentenbase_ora */
	| COALESCE
	| EXTRACT
	| PRECISION
	| ROW
	/*pg only*/
	/*| BIGINT*/
	/*| BIT*/
	/*| BOOLEAN_P*/
	/*| INOUT*/
	/*| OUT_P*/
	/*| OVERLAY*/
	/*| SETOF*/
	/*| SUBSTRING*/
	| ANALYSE
	| ASYMMETRIC
	| CURRENT_CATALOG
	| CURRENT_ROLE
	| DO
	| PLACING
	| SESSION_USER
	| SHARDCLUSTER
	| SYMMETRIC
	| VARIADIC
	/*| WINDOW*/
	| CONCURRENTLY
	| FREEZE
	/*| ILIKE*/ /* not supported conflict with a_expr ILIKE a_expr */
	/*| ISNULL*/ /* not supported conflict with a_expr ISNULL  */
	/*| NOTNULL*/ /* not supported conflict with a_expr NOTNULL  */
	/*| SIMILAR*/ /* supported case  by case*/
	| TABLESAMPLE
	| VERBOSE
	| PASSWORD
	;
/* Column identifier --- keywords that can be column, table, etc names.
 *
 * Many of these keywords will in fact be recognized as type or function
 * names too; but they have special productions for the purpose, and so
 * can't be treated as "generic" type or function names.
 *
 * The type names appearing here are not usable as function names
 * because they can be followed by '(' in typename productions, which
 * looks too much like a function call for an LR(1) parser.
 */
col_name_keyword:
			  BETWEEN
			| BIGINT
/* OPENTENBASE_ORA_BEGIN */
			| BINARY_DOUBLE
			| BINARY_FLOAT
/* OPENTENBASE_ORA_END */
			| BIT
			| BOOLEAN_P
			| BYTE_P
			| CHAR_P
			| CHARACTER
			| COALESCE
			| DEC
			| DECIMAL_P
			| EXISTS
			| EXTRACT
			| FLOAT_P
			| GREATEST
			| GROUPING
			| GROUPING_ID
			| INOUT
			| INT_P
			| INTEGER
			| INTERVAL
			| LEAST
			| LONG
			| NATIONAL
			| NCHAR
			| NONE
			| NULLIF
/* OPENTENBASE_ORA_BEGIN */
			| NUMBER
/* OPENTENBASE_ORA_END */
			| NUMERIC
			| OUT_P
			| OVERLAY
			| POSITION
			| PRECISION
			| RAW
			| REAL
			| ROW
			| SETOF
			| SMALLINT
			| SUBSTRING
			| TIME
			| TIMESTAMP
			| TREAT
			| TRIM
			| VALUES
			| VARCHAR
			| XMLATTRIBUTES
			| XMLCONCAT
			| XMLELEMENT
			| XMLEXISTS
			| XMLFOREST
			| XMLNAMESPACES
			| XMLPARSE
			| XMLPI
			| XMLROOT
			| XMLSERIALIZE
			| XMLTABLE
		;

/* Type/function identifier --- keywords that can be type or function names.
 *
 * Most of these are keywords that are used as operators in expressions;
 * in general such keywords can't be column names because they would be
 * ambiguous with variables, but they are unambiguous as function identifiers.
 *
 * Do not include POSITION, SUBSTRING, etc here since they have explicit
 * productions in a_expr to support the goofy SQL9x argument syntax.
 * - thomas 2000-11-28
 */
type_func_name_keyword:
			  AUTHORIZATION
			| COLLATION
			| CONCURRENTLY
			| CROSS
			| CURRENT_SCHEMA
			| FREEZE
			| FULL
			| ILIKE
			| INNER_P
			| IS
			| ISNULL
			| JOIN
			| LIKE
			| NATURAL
			| NOTNULL
			| OUTER_P
			| OVERLAPS
			| RIGHT
			| SIMILAR
			| TABLESAMPLE
			| VERBOSE
		;

/* Reserved keyword --- these keywords are usable only as a ColLabel.
 *
 * Keywords appear here if they could not be distinguished from variable,
 * type, or function names in some contexts.  Don't put things here unless
 * forced to.
 */
reserved_keyword:
			  ALL
			| ANALYSE
			| ANALYZE
			| AND
			| ANY
			| ARRAY
			| AS
			| ASC
			| ASYMMETRIC
/* __AUDIT__ BEGIN */
			| AUDIT
/* __AUDIT__ END */
			| AUTHENTICATED
/* OPENTENBASE_ORA_BEGIN */
			| BINARY_DOUBLE_NAN
			| BINARY_FLOAT_NAN
/* OPENTENBASE_ORA_END */
			| BOTH
			| BY
			| CASE
			| CAST
			| CHECK
			| COLLATE
			| COLUMN
			| CONNECT
			| CONSTRAINT
			| CREATE
			| CURRENT_CATALOG
			| CURRENT_DATE
			| CURRENT_ROLE
			| CURRENT_TIME
			| CURRENT_TIMESTAMP
			| CURRENT_USER
/* OPENTENBASE_ORA_BEGIN */
			| DBTIMEZONE
/* OPENTENBASE_ORA_END */
			| DEFAULT
			| DEFERRABLE
			| DELETE_P
			| DESC
			| DISTINCT
			| DO
			| ELSE
			| END_P
			| EXCEPT
			| FALSE_P
			| FETCH
			| FOR
			| FOREIGN
			| FROM
			| GRANT
			| GROUP_P
			| HAVING
			| IDENTIFIED
			| IN_P
			| INITIALLY
			| INTERSECT
			| INTO
			| LATERAL_P
			| LEADING
			| LEVEL
			| LIMIT
			| LOCALTIME
			| LOCALTIMESTAMP
/* OPENTENBASE_ORA_BEGIN */
			| MINUS
/* OPENTENBASE_ORA_END */
/* __AUDIT__ BEGIN */
			| NOAUDIT
/* __AUDIT__ END */
/* OPENTENBASE_ORA_BEGIN */
			| NOCYCLE
/* OPENTENBASE_ORA_END */
			| NOT
			| NULL_P
			| OFFSET
			| ON
			| ONLY
			| OR
			| ORDER
/* moved from unresorved key words */
/*
 * To adapt for opentenbase_ora, this key word 'partition' is allowed to use as table column name.
 * Changing 'partition' as unreserved key words
 */
//		    | PARTITION
/* end */
			| PLACING
			| PRIMARY
			| PRIOR
			| REFERENCES
			| RETURNING
/* OPENTENBASE_ORA_BEGIN */
			| ROWNUM
/* OPENTENBASE_ORA_END */
			| SELECT
/* OPENTENBASE_ORA_BEGIN */
			| SESSIONTIMEZONE
/* OPENTENBASE_ORA_END */
			| SESSION_USER
/* _OPENTENBASE_C__ BEGIN */
			| SHARDCLUSTER
/* _OPENTENBASE_C__ END */
			| SOME
			| START
/* __AUDIT__ BEGIN */
			| SUCCESSFUL
/* __AUDIT__ END */
			| SYMMETRIC
/* OPENTENBASE_ORA_BEGIN */
			| SYSDATE
			| SYSTIMESTAMP
/* OPENTENBASE_ORA_END */
			| TABLE
			| THEN
			| TO
			| TRAILING
			| TRUE_P
			| UID_P
			| UNION
			| UNIQUE
			| USER
			| USING
			| VARIADIC
			| WHEN
/* __AUDIT__ BEGIN */
			| WHENEVER
/* __AUDIT__ END */
			| WHERE
			| WINDOW
			| WITH
		;

pkg_reserved_keyword:
			BODY
		;
a_expr_spec_alais_kw:
            	BINARY
            	| NAME_P
                | SOME
                | COUNT
                | LABEL
                | LEFT
                | LINK
                | NULLIF
                | PARTITIONS
                | REPLACE
                | OVERLAY
                | GREATEST
                | LEAST
                | NAMES
                | SUBSTRING
                | TRIM
                | OLD
                |opentenbase_ora_alias_unreserved
/*this keyword using column alias without as*/
spec_aliasid_keyword:
              ARRAY
			| DAY_P
			| HOUR_P
			| MINUTE_P
			| MONTH_P
			| SECOND_P
			| YEAR_P

		;
%%

/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
ora_base_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static RawStmt *
makeRawStmt(Node *stmt, int stmt_location)
{
	RawStmt    *rs = makeNode(RawStmt);

	rs->stmt = stmt;
	rs->stmt_location = stmt_location;
	rs->stmt_len = 0;			/* might get changed later */
	return rs;
}

/* Adjust a RawStmt to reflect that it doesn't run to the end of the string */
static void
updateRawStmtEnd(RawStmt *rs, int end_location)
{
	/*
	 * If we already set the length, don't change it.  This is for situations
	 * like "select foo ;; select bar" where the same statement will be last
	 * in the string for more than one semicolon.
	 */
	if (rs->stmt_len > 0)
		return;

	/* OK, update length of RawStmt */
	rs->stmt_len = end_location - rs->stmt_location;
}

static Node *
makeColumnRef(char *colname, List *indirection,
			  int location, core_yyscan_t yyscanner)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection, nfields), yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		else if (IsA(lfirst(l), A_Star))
		{
			/* We only allow '*' at the end of a ColumnRef */
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}

/* Used by other files */
Node *
makeColumnRef_ext(char *colname)
{
	return makeColumnRef(colname, NIL, -1, 0);
}

Node *
makeEmptyStrAConst(int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Null;
	n->val.val.str = "";
	n->location = location;

	return (Node *)n;
}

static Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

static Node *
makeBitStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_BitString;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

static Node *
makeAConst(Value *v, int location)
{
	Node *n;

	switch (v->type)
	{
		case T_Float:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_Integer:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_String:
		default:
			n = makeStringConst(v->val.str, location);
			break;
	}

	return n;
}

/* makeRoleSpec
 * Create a RoleSpec with the given type
 */
static RoleSpec *
makeRoleSpec(RoleSpecType type, int location)
{
	RoleSpec *spec = makeNode(RoleSpec);

	spec->roletype = type;
	spec->location = location;

	return spec;
}

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
static void
check_qualified_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
}

/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
static List *
check_func_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
	return names;
}

/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
static List *
check_indirection(List *indirection, core_yyscan_t yyscanner)
{
	ListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Star))
		{
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/*
 * extractArgTypes()
 *
 * Given a list of FunctionParameter nodes, extract a list of just the
 * argument types (TypeNames) for signature parameters only (e.g., only input
 * parameters for functions).  This is what is needed to look up an existing
 * function, which is what is wanted by the productions that use this call.
 */
static List *
extractArgTypes(ObjectType objtype, List *parameters)
{
	List	   *result = NIL;
	ListCell   *i;

	foreach(i, parameters)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(i);

		if ((ORA_MODE || p->mode != FUNC_PARAM_OUT) && p->mode != FUNC_PARAM_TABLE)
			result = lappend(result, p->argType);
	}
	return result;
}

/* extractAggrArgTypes()
 * As above, but work from the output of the aggr_args production.
 */
static List *
extractAggrArgTypes(List *aggrargs)
{
	Assert(list_length(aggrargs) == 2);
	return extractArgTypes(OBJECT_AGGREGATE, (List *) linitial(aggrargs));
}

/* makeOrderedSetArgs()
 * Build the result of the aggr_args production (which see the comments for).
 * This handles only the case where both given lists are nonempty, so that
 * we have to deal with multiple VARIADIC arguments.
 */
static List *
makeOrderedSetArgs(List *directargs, List *orderedargs,
				   core_yyscan_t yyscanner)
{
	FunctionParameter *lastd = (FunctionParameter *) llast(directargs);
	int			ndirectargs;

	/* No restriction unless last direct arg is VARIADIC */
	if (lastd->mode == FUNC_PARAM_VARIADIC)
	{
		FunctionParameter *firsto = (FunctionParameter *) linitial(orderedargs);

		/*
		 * We ignore the names, though the aggr_arg production allows them;
		 * it doesn't allow default values, so those need not be checked.
		 */
		if (list_length(orderedargs) != 1 ||
			firsto->mode != FUNC_PARAM_VARIADIC ||
			!equal(lastd->argType, firsto->argType))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("an ordered-set aggregate with a VARIADIC direct argument must have one VARIADIC aggregated argument of the same data type"),
					 parser_errposition(exprLocation((Node *) firsto))));

		/* OK, drop the duplicate VARIADIC argument from the internal form */
		orderedargs = NIL;
	}

	/* don't merge into the next line, as list_concat changes directargs */
	ndirectargs = list_length(directargs);

	return list_make2(list_concat(directargs, orderedargs),
					  makeInteger(ndirectargs));
}

/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in SelectStmt productions.
 */
static void
insertSelectOptions(SelectStmt *stmt,
					List *sortClause, List *lockingClause,
					SelectLimit *limitClause,
					WithClause *withClause,
					core_yyscan_t yyscanner)
{
	Assert(IsA(stmt, SelectStmt));

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple ORDER BY clauses not allowed"),
					 parser_errposition(exprLocation((Node *) sortClause))));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitClause && limitClause->limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple OFFSET clauses not allowed"),
					 parser_errposition(exprLocation(limitClause->limitOffset))));
		stmt->limitOffset = limitClause->limitOffset;
	}
	if (limitClause && limitClause->limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple LIMIT clauses not allowed"),
					 parser_errposition(exprLocation(limitClause->limitCount))));
		stmt->limitCount = limitClause->limitCount;
	}
	if (limitClause && limitClause->limitOption != LIMIT_OPTION_DEFAULT)
	{
		if (stmt->limitOption)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple limit options not allowed")));
		if (!stmt->sortClause)
		{
			if (PG_MODE && limitClause->limitOption == LIMIT_OPTION_WITH_TIES)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("WITH TIES options can not be specified without ORDER BY clause")));

			/*
			 * If WITH TIES is specified, then the order_by_clause (sorting clause) must be specified.
			 * If the order_by_clause is not specified, no additional rows will be returned.
			 */
			if (ORA_MODE)
			{
				if (limitClause->limitOption == LIMIT_OPTION_WITH_TIES)
					limitClause->limitOption = LIMIT_OPTION_COUNT;
				else if (limitClause->limitOption == LIMIT_OPTION_PERCENT_WITH_TIES)
					limitClause->limitOption = LIMIT_OPTION_PERCENT_COUNT;
			}
		}
		stmt->limitOption = limitClause->limitOption;
	}

	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple WITH clauses not allowed"),
					 parser_errposition(exprLocation((Node *) withClause))));
		stmt->withClause = withClause;
	}
}

static Node *
makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg)
{
	SelectStmt *n = makeNode(SelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (SelectStmt *) larg;
	n->rarg = (SelectStmt *) rarg;
	return (Node *) n;
}


/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
static Node *
doNegate(Node *n, int location)
{
	if (IsA(n, A_Const))
	{
		A_Const *con = (A_Const *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_Integer)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_Float)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (Node *) makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

static void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;

	Assert(IsA(v, Float));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
		v->val.str = psprintf("-%s", oldval);
}

static Node *
makeAndExpr(Node *lexpr, Node *rexpr, int location)
{
	Node	   *lexp = lexpr;

	/* Look through AEXPR_PAREN nodes so they don't affect flattening */
	while (IsA(lexp, A_Expr) &&
		   ((A_Expr *) lexp)->kind == AEXPR_PAREN)
		lexp = ((A_Expr *) lexp)->lexpr;
	/* Flatten "a AND b AND c ..." to a single BoolExpr on sight */
	if (IsA(lexp, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexp;

		if (blexpr->boolop == AND_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *
makeOrExpr(Node *lexpr, Node *rexpr, int location)
{
	Node	   *lexp = lexpr;

	/* Look through AEXPR_PAREN nodes so they don't affect flattening */
	while (IsA(lexp, A_Expr) &&
		   ((A_Expr *) lexp)->kind == AEXPR_PAREN)
		lexp = ((A_Expr *) lexp)->lexpr;
	/* Flatten "a OR b OR c ..." to a single BoolExpr on sight */
	if (IsA(lexp, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexp;

		if (blexpr->boolop == OR_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), location);
}

static Node *
makeNotExpr(Node *expr, int location)
{
	return (Node *) makeBoolExpr(NOT_EXPR, list_make1(expr), location);
}

static Node *
makeAArrayExpr(List *elements, int location)
{
	A_ArrayExpr *n = makeNode(A_ArrayExpr);

	n->elements = elements;
	n->location = location;
	return (Node *) n;
}

static Node *
makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod, int location)
{
	SQLValueFunction *svf = makeNode(SQLValueFunction);

	svf->op = op;
	/* svf->type will be filled during parse analysis */
	svf->typmod = typmod;
	svf->location = location;
	return (Node *) svf;
}

static Node *
makeXmlExpr(XmlExprOp op, char *name, List *named_args, List *args,
			int location)
{
	XmlExpr		*x = makeNode(XmlExpr);

	x->op = op;
	x->name = name;
	/*
	 * named_args is a list of ResTarget; it'll be split apart into separate
	 * expression and name lists in transformXmlExpr().
	 */
	x->named_args = named_args;
	x->arg_names = NIL;
	x->args = args;
	/* xmloption, if relevant, must be filled in by caller */
	/* type and typmod will be filled in during parse analysis */
	x->type = InvalidOid;			/* marks the node as not analyzed */
	x->location = location;
	return (Node *) x;
}

/*
 * Merge the input and output parameters of a table function.
 */
static List *
mergeTableFuncParameters(List *func_args, List *columns)
{
	ListCell   *lc;

	/* Explicit OUT and INOUT parameters shouldn't be used in this syntax */
	foreach(lc, func_args)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(lc);

		if (p->mode != FUNC_PARAM_IN && p->mode != FUNC_PARAM_VARIADIC)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("OUT and INOUT arguments aren't allowed in TABLE functions")));
	}

	return list_concat(func_args, columns);
}

/*
 * Determine return type of a TABLE function.  A single result column
 * returns setof that column's type; otherwise return setof record.
 */
static TypeName *
TableFuncTypeName(List *columns)
{
	TypeName *result;

	if (list_length(columns) == 1)
	{
		FunctionParameter *p = (FunctionParameter *) linitial(columns);

		result = copyObject(p->argType);
	}
	else
		result = SystemTypeName("record");

	result->setof = true;

	return result;
}

/*
 * Convert a list of (dotted) names to a RangeVar (like
 * makeRangeVarFromNameList, but with position support).  The
 * "AnyName" refers to the any_name production in the grammar.
 */
static RangeVar *
makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner)
{
	RangeVar *r = makeNode(RangeVar);

	switch (list_length(names))
	{
		case 1:
			r->catalogname = NULL;
			r->schemaname = NULL;
			r->relname = strVal(linitial(names));
			break;
		case 2:
			r->catalogname = NULL;
			r->schemaname = strVal(linitial(names));
			r->relname = strVal(lsecond(names));
			break;
		case 3:
			r->catalogname = strVal(linitial(names));
			r->schemaname = strVal(lsecond(names));
			r->relname = strVal(lthird(names));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("improper qualified name (too many dotted names): %s",
							NameListToString(names)),
					 parser_errposition(position)));
			break;
	}

	r->relpersistence = RELPERSISTENCE_PERMANENT;
	r->location = position;

	return r;
}

/* Separate Constraint nodes from COLLATE clauses in a ColQualList */
static void
SplitColQualList(List *qualList,
				 List **constraintList, CollateClause **collClause,
				 core_yyscan_t yyscanner)
{
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	*collClause = NULL;
	prev = NULL;
	for (cell = list_head(qualList); cell; cell = next)
	{
		Node   *n = (Node *) lfirst(cell);

		next = lnext(cell);
		if (IsA(n, Constraint))
		{
			/* keep it in list */
			prev = cell;
			continue;
		}
		if (IsA(n, CollateClause))
		{
			CollateClause *c = (CollateClause *) n;

			if (*collClause)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple COLLATE clauses not allowed"),
						 parser_errposition(c->location)));
			*collClause = c;
		}
		else
			elog(ERROR, "unexpected node type %d", (int) n->type);
		/* remove non-Constraint nodes from qualList */
		qualList = list_delete_cell(qualList, cell, prev);
	}
	*constraintList = qualList;
}

/*
 * Process result of ConstraintAttributeSpec, and set appropriate bool flags
 * in the output command node.  Pass NULL for any flags the particular
 * command doesn't support.
 */
static void
processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner)
{
	/* defaults */
	if (deferrable)
		*deferrable = false;
	if (initdeferred)
		*initdeferred = false;
	if (not_valid)
		*not_valid = false;

	if (cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED))
	{
		if (deferrable)
			*deferrable = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_INITIALLY_DEFERRED)
	{
		if (initdeferred)
			*initdeferred = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NOT_VALID)
	{
		if (not_valid)
			*not_valid = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NOT VALID",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NO_INHERIT)
	{
		if (no_inherit)
			*no_inherit = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NO INHERIT",
							constrType),
					 parser_errposition(location)));
	}
}

static Node *
reparse_decode_func(List *args, int location)
{
	List		*cargs = NIL;
	ListCell 	*lc = NULL;
	Expr		*expr = NULL;
	Node		*search = NULL;

	CaseExpr *c = makeNode(CaseExpr);
	c->casetype = InvalidOid; /* not analyzed yet */
	c->isdecode = true;
	expr = (Expr *)linitial(args);

	for_each_cell(lc, lnext(list_head(args)))
	{
		CaseWhen *w;

		if (lnext(lc) == NULL)
			break;

		w = makeNode(CaseWhen);

		search = (Node *)lfirst(lc);
		if (IsA(expr, A_Const) &&
			((A_Const*)expr)->val.type == T_Null)
		{
			NullTest *n = (NullTest *)makeNode(NullTest);
			n->arg = (Expr *)search;
			n->nulltesttype = IS_NULL;
			w->expr = (Expr *)n;
		}
		else if (IsA(search, A_Const) &&
			((A_Const*)search)->val.type == T_Null)
		{
			NullTest *n = (NullTest *)makeNode(NullTest);
			n->arg = (Expr *)expr;
			n->nulltesttype = IS_NULL;
			w->expr = (Expr *)n;
		}
		else if (IsA(search, A_Const) || IsA(expr, A_Const))
		{
			/* If either of them is not null, caseexpr return true when expr = search. */
			w->expr = (Expr *) makeSimpleA_Expr(AEXPR_OP,
												"=",
												(Node *)expr,
												search,
												-1);
		}
		else
		{
			/*
			 * caseexpr return true in two cases:
			 * 1. expr = search return true.
			 * 2. both are null.
			 */
			NullTest *lNullTest = (NullTest *)makeNode(NullTest);
			NullTest *rNullTest = (NullTest *)makeNode(NullTest);
			Node *nullTest = NULL;
			Node *equalTest = NULL;

			lNullTest->arg = (Expr *)search;
			lNullTest->nulltesttype = IS_NULL;

			rNullTest->arg = expr;
			rNullTest->nulltesttype = IS_NULL;

			nullTest = makeAndExpr((Node *)lNullTest, (Node *)rNullTest, -1);

			equalTest = (Node *)makeSimpleA_Expr(AEXPR_OP, "=", (Node *)expr, copyObject(search), -1);

			w->expr = (Expr *)makeOrExpr(equalTest, nullTest, -1);
		}
		w->result = (Expr *) lfirst(lnext(lc));
		w->location = -1;
		cargs = lappend(cargs, w);
		lc = lnext(lc);
	}
	c->args = cargs;
	c->defresult = lc ? (Expr *)lfirst(lc) : NULL;
	c->location = location;

	return (Node *)c;
}

static char*
StrConcat(const char* str1, const char* str2)
{
	size_t len1 = strlen(str1);
	size_t len2 = strlen(str2);
	char* result = palloc0(len1 + len2 + 1);

	if (result == NULL)
	{
		return NULL;
	}

	strcpy(result, str1);
	strcat(result, str2);

	return result;
}

/*----------
 * Recursive view transformation
 *
 * Convert
 *
 *     CREATE RECURSIVE VIEW relname (aliases) AS query
 *
 * to
 *
 *     CREATE VIEW relname (aliases) AS
 *         WITH RECURSIVE relname (aliases) AS (query)
 *         SELECT aliases FROM relname
 *
 * Actually, just the WITH ... part, which is then inserted into the original
 * view definition as the query.
 * ----------
 */
static Node *
makeRecursiveViewSelect(char *relname, List *aliases, Node *query)
{
	SelectStmt *s = makeNode(SelectStmt);
	WithClause *w = makeNode(WithClause);
	CommonTableExpr *cte = makeNode(CommonTableExpr);
	List	   *tl = NIL;
	ListCell   *lc;

	/* create common table expression */
	cte->ctename = relname;
	cte->aliascolnames = aliases;
	cte->ctequery = query;
	cte->location = -1;

	/* create WITH clause and attach CTE */
	w->recursive = true;
	w->ctes = list_make1(cte);
	w->location = -1;

	/* create target list for the new SELECT from the alias list of the
	 * recursive view specification */
	foreach (lc, aliases)
	{
		ResTarget *rt = makeNode(ResTarget);

		rt->name = NULL;
		rt->indirection = NIL;
		rt->val = makeColumnRef(strVal(lfirst(lc)), NIL, -1, 0);
		rt->location = -1;

		tl = lappend(tl, rt);
	}

	/* create new SELECT combining WITH clause, target list, and fake FROM
	 * clause */
	s->withClause = w;
	s->targetList = tl;
	s->fromClause = list_make1(makeRangeVar(NULL, relname, -1));

	return (Node *) s;
}

/* ora_parser_init()
 * Initialize to parse one query string
 */
void
ora_parser_init(base_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;		/* in case grammar forgets to set it */
}

/*
 * Extract function definition, initialization part and private specification.
 */
void
extract_package_body_defs(CreatePackageBodyStmt *n, base_yy_extra_type *yyextra,
							char *body)
{
	if (yyextra->func_def_idx >= 0)
	{
		n->priv_decl = palloc0(yyextra->func_def_idx + 1);
		memcpy(n->priv_decl, body, yyextra->func_def_idx);

		if (yyextra->initialize_idx >= 0)
		{
			int	len = yyextra->initialize_idx - yyextra->func_def_idx;

			n->func_def = palloc0(len + 1);
			memcpy(n->func_def, &body[yyextra->func_def_idx], len);

			n->initial_part = pstrdup(&body[yyextra->initialize_idx]);
		}
		else
			n->func_def = pstrdup(&body[yyextra->func_def_idx]);
	}
	else
	{
		if (yyextra->initialize_idx >= 0)
		{
			n->priv_decl = palloc0(yyextra->initialize_idx + 1);
			memcpy(n->priv_decl, body, yyextra->initialize_idx);

			n->initial_part = pstrdup(&body[yyextra->initialize_idx]);
		}
		else
		{
			/*
			 * No procedure or function definition, no
			 * initialization part. Assume all is the
			 * private declare.
			 */
			n->priv_decl = pstrdup(body);
		}
	}
}
