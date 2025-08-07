/*-------------------------------------------------------------------------
 *
 * parse_node.h
 *		Internal definitions for parser
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_node.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_NODE_H
#define PARSE_NODE_H

#include "nodes/parsenodes.h"
#include "utils/queryenvironment.h"
#include "utils/relcache.h"
#include "utils/hsearch.h"

#define LIGHTWEIGHT_ORA_CONVERS_DATE_TIMESTAMP_TAG -100

/*
 * Expression kinds distinguished by transformExpr().  Many of these are not
 * semantically distinct so far as expression transformation goes; rather,
 * we distinguish them so that context-specific error messages can be printed.
 *
 * Note: EXPR_KIND_OTHER is not used in the core code, but is left for use
 * by extension code that might need to call transformExpr().  The core code
 * will not enforce any context-driven restrictions on EXPR_KIND_OTHER
 * expressions, so the caller would have to check for sub-selects, aggregates,
 * window functions, SRFs, etc if those need to be disallowed.
 */
typedef enum ParseExprKind
{
	EXPR_KIND_NONE = 0,			/* "not in an expression" */
	EXPR_KIND_OTHER,			/* reserved for extensions */
	EXPR_KIND_JOIN_ON,			/* JOIN ON */
	EXPR_KIND_JOIN_USING,		/* JOIN USING */
	EXPR_KIND_FROM_SUBSELECT,	/* sub-SELECT in FROM clause */
	EXPR_KIND_FROM_FUNCTION,	/* function in FROM clause */
	EXPR_KIND_WHERE,			/* WHERE */
	EXPR_KIND_HAVING,			/* HAVING */
	EXPR_KIND_FILTER,			/* FILTER */
	EXPR_KIND_WINDOW_PARTITION, /* window definition PARTITION BY */
	EXPR_KIND_WINDOW_ORDER,		/* window definition ORDER BY */
	EXPR_KIND_WINDOW_FRAME_RANGE,	/* window frame clause with RANGE */
	EXPR_KIND_WINDOW_FRAME_ROWS,	/* window frame clause with ROWS */
	EXPR_KIND_SELECT_TARGET,	/* SELECT target list item */
	EXPR_KIND_INSERT_TARGET,	/* INSERT target list item */
	EXPR_KIND_UPDATE_SOURCE,	/* UPDATE assignment source item */
	EXPR_KIND_UPDATE_TARGET,	/* UPDATE assignment target item */
	EXPR_KIND_MERGE_WHEN,		/* MERGE WHEN [NOT] MATCHED condition */
	EXPR_KIND_GROUP_BY,			/* GROUP BY */
	EXPR_KIND_ORDER_BY,			/* ORDER BY */
	EXPR_KIND_DISTINCT_ON,		/* DISTINCT ON */
	EXPR_KIND_LIMIT,			/* LIMIT */
	EXPR_KIND_OFFSET,			/* OFFSET */
	EXPR_KIND_RETURNING,		/* RETURNING */
	EXPR_KIND_VALUES,			/* VALUES */
	EXPR_KIND_VALUES_SINGLE,	/* single-row VALUES (in INSERT only) */
	EXPR_KIND_CHECK_CONSTRAINT, /* CHECK constraint for a table */
	EXPR_KIND_DOMAIN_CHECK,		/* CHECK constraint for a domain */
	EXPR_KIND_COLUMN_DEFAULT,	/* default value for a table column */
	EXPR_KIND_FUNCTION_DEFAULT, /* default parameter value for function */
	EXPR_KIND_INDEX_EXPRESSION, /* index expression */
	EXPR_KIND_INDEX_PREDICATE,	/* index predicate */
	EXPR_KIND_ALTER_COL_TRANSFORM,	/* transform expr in ALTER COLUMN TYPE */
	EXPR_KIND_EXECUTE_PARAMETER,	/* parameter value in EXECUTE */
	EXPR_KIND_TRIGGER_WHEN,		/* WHEN condition in CREATE TRIGGER */
	EXPR_KIND_POLICY,			/* USING or WITH CHECK expr in policy */
	EXPR_KIND_PARTITION_EXPRESSION,	/* PARTITION BY expression */
	EXPR_KIND_CALL_ARGUMENT     /* procedure argument in CALL */
} ParseExprKind;

typedef enum LongTypeCheck {
  LT_NONE,      /* No need to check for long type */
  LT_INSERT,    /* Need to check for long type in INSERT statement */
  LT_MAVIEW     /* Need to check for long type in CREATE MATERIALIZED VIEW AS SELECT statement */
} LongTypeCheck;

#define NULL_TYPE_STR_LEN -3
#define NULL_TYPE_NULL_LEN -2
typedef enum
{
	EXPR_ITEM_NONE = 0,
	EXPR_ITEM_FN_ARGS = 1,
	EXPR_ITEM_UDT_ASSIGN = 2
} ParseExprItem;

typedef enum
{
	COLLECT_NONE = 0, /* non-collection type */
	COLLECT_ASS_INDEX_LONG = 1, /* Associative array type and its index type is LONG. */
	COLLECT_ASS_INDEX_TEXT = 2, /* Associative array type and its index type is TEXT. */
	COLLECT_ASS_INDEX_INT = 3, /* Associative array type and its index type is INTEGER. */
	COLLECT_OTHERS = 4 /* Nested tables and VARRAY types */
} ParseCollect;

/*
 * Function signatures for parser hooks
 */
typedef struct ParseState ParseState;

typedef Node *(*PreParseColumnRefHook) (ParseState *pstate, ColumnRef *cref);
typedef Node *(*PostParseColumnRefHook) (ParseState *pstate, ColumnRef *cref, Node *var);
typedef Node *(*ParseParamRefHook) (ParseState *pstate, ParamRef *pref);
typedef ParseCollect (*ParseCollectRefHook) (ParseState *pstate, int paramno, Oid *indextypoid);
typedef int32 (*ParseVarTypMod) (ParseState *pstate, int paramno);
typedef List *(*GetCollectionValuesHook) (ParseState *pstate, ColumnRef *cref);
typedef Node *(*CoerceParamHook) (ParseState *pstate, Param *param,
								  Oid targetTypeId, int32 targetTypeMod,
								  int location);
typedef Node *(*CoerceRecordParamHook) (ParseState *pstate, Param *param);
typedef bool (*CoerceEmptyStringHook) (ParseState *pstate, Node *node);


/*
 * State information used during parse analysis
 *
 * parentParseState: NULL in a top-level ParseState.  When parsing a subquery,
 * links to current parse state of outer query.
 *
 * p_sourcetext: source string that generated the raw parsetree being
 * analyzed, or NULL if not available.  (The string is used only to
 * generate cursor positions in error messages: we need it to convert
 * byte-wise locations in parse structures to character-wise cursor
 * positions.)
 *
 * p_rtable: list of RTEs that will become the rangetable of the query.
 * Note that neither relname nor refname of these entries are necessarily
 * unique; searching the rtable by name is a bad idea.
 *
 * p_joinexprs: list of JoinExpr nodes associated with p_rtable entries.
 * This is one-for-one with p_rtable, but contains NULLs for non-join
 * RTEs, and may be shorter than p_rtable if the last RTE(s) aren't joins.
 *
 * p_joinlist: list of join items (RangeTblRef and JoinExpr nodes) that
 * will become the fromlist of the query's top-level FromExpr node.
 *
 * p_namespace: list of ParseNamespaceItems that represents the current
 * namespace for table and column lookup.  (The RTEs listed here may be just
 * a subset of the whole rtable.  See ParseNamespaceItem comments below.)
 *
 * p_lateral_active: TRUE if we are currently parsing a LATERAL subexpression
 * of this parse level.  This makes p_lateral_only namespace items visible,
 * whereas they are not visible when p_lateral_active is FALSE.
 *
 * p_ctenamespace: list of CommonTableExprs (WITH items) that are visible
 * at the moment.  This is entirely different from p_namespace because a CTE
 * is not an RTE, rather "visibility" means you could make an RTE from it.
 *
 * p_future_ctes: list of CommonTableExprs (WITH items) that are not yet
 * visible due to scope rules.  This is used to help improve error messages.
 *
 * p_parent_cte: CommonTableExpr that immediately contains the current query,
 * if any.
 *
 * p_target_relation: target relation, if query is INSERT, UPDATE, or DELETE.
 *
 * p_target_rangetblentry: target relation's entry in the rtable list.
 *
 * p_is_insert: true to process assignment expressions like INSERT, false
 * to process them like UPDATE.  (Note this can change intra-statement, for
 * cases like INSERT ON CONFLICT UPDATE.)
 *
 * p_windowdefs: list of WindowDefs representing WINDOW and OVER clauses.
 * We collect these while transforming expressions and then transform them
 * afterwards (so that any resjunk tlist items needed for the sort/group
 * clauses end up at the end of the query tlist).  A WindowDef's location in
 * this list, counting from 1, is the winref number to use to reference it.
 *
 * p_expr_kind: kind of expression we're currently parsing, as per enum above;
 * EXPR_KIND_NONE when not in an expression.
 *
 * p_next_resno: next TargetEntry.resno to assign, starting from 1.
 *
 * p_multiassign_exprs: partially-processed MultiAssignRef source expressions.
 *
 * p_locking_clause: query's FOR UPDATE/FOR SHARE clause, if any.
 *
 * p_locked_from_parent: true if parent query level applies FOR UPDATE/SHARE
 * to this subquery as a whole.
 *
 * p_resolve_unknowns: resolve unknown-type SELECT output columns as type TEXT
 * (this is true by default).
 *
 * p_hasAggs, p_hasWindowFuncs, etc: true if we've found any of the indicated
 * constructs in the query.
 *
 * p_last_srf: the set-returning FuncExpr or OpExpr most recently found in
 * the query, or NULL if none.
 *
 * p_pre_columnref_hook, etc: optional parser hook functions for modifying the
 * interpretation of ColumnRefs and ParamRefs.
 *
 * p_ref_hook_state: passthrough state for the parser hook functions.
 */
struct ParseState
{
	struct ParseState *parentParseState;	/* stack link */
	const char *p_sourcetext;	/* source text, or NULL if not available */
	List	   *p_rtable;		/* range table so far */
	List	   *p_joinexprs;	/* JoinExprs for RTE_JOIN p_rtable entries */
	List	   *p_joinlist;		/* join items so far (will become FromExpr
								 * node's fromlist) */
	List	   *p_namespace;	/* currently-referenceable RTEs (List of
								 * ParseNamespaceItem) */
	List	   **order_list;
	bool		p_lateral_active;	/* p_lateral_only items visible? */
	List	   *p_ctenamespace; /* current namespace for common table exprs */
	List	   *p_future_ctes;	/* common table exprs not yet in namespace */
	CommonTableExpr *p_parent_cte;	/* this query's containing CTE */
	Relation	p_target_relation;	/* INSERT/UPDATE/DELETE target rel */
	RangeTblEntry *p_target_rangetblentry;	/* target rel's RTE */
	bool		p_is_insert;	/* process assignment like INSERT not UPDATE */
	List	   *p_windowdefs;	/* raw representations of window clauses */
	ParseExprKind p_expr_kind;	/* what kind of expression we're parsing */
	int			p_next_resno;	/* next targetlist resno to assign */
	List	   *p_multiassign_exprs;	/* junk tlist entries for multiassign */
	List	   *p_locking_clause;	/* raw FOR UPDATE/FOR SHARE info */
	bool		p_locked_from_parent;	/* parent has marked this subquery
										 * with FOR UPDATE/FOR SHARE */
	bool		p_resolve_unknowns; /* resolve unknown-type SELECT outputs as
									 * type text */

	QueryEnvironment *p_queryEnv;	/* curr env, incl refs to enclosing env */

	/* Flags telling about things found in the query: */
	bool		p_hasAggs;
	bool		p_hasWindowFuncs;
	bool		p_hasTargetSRFs;
	bool		p_hasSubLinks;
	bool		p_hasModifyingCTE;
	bool		p_hasRownum;
	bool		p_hasDistinct;

	Node	   *p_last_srf;		/* most recent set-returning func/op found */

	List	   *p_nested_aggs;		/* Nested aggregate functions */
	bool		p_resolve_nestagg;

	/*
	 * Optional hook functions for parser callbacks.  These are null unless
	 * set up by the caller of make_parsestate.
	 */
	PreParseColumnRefHook p_pre_columnref_hook;
	PostParseColumnRefHook p_post_columnref_hook;
	ParseParamRefHook p_paramref_hook;
	ParseCollectRefHook p_collectref_hook;
	ParseVarTypMod p_vartypmod_hook;
	GetCollectionValuesHook p_get_collection_values_hook;
	CoerceParamHook p_coerce_param_hook;
	CoerceRecordParamHook	p_coerce_rec_hook;
	CoerceEmptyStringHook	p_coerce_empty_string_hook;
	void	   *p_ref_hook_state;	/* common passthrough link for above */
#ifdef __OPENTENBASE__
	const CopyStmt *stmt;
	bool fromCTAS; /* Param process control to prevent repeated param parsing in CTAS scenario. */

	/* opentenbase_ora long type syntax check */
	LongTypeCheck long_type_check;
#endif
	bool        p_hasConnectBy;		/* has connect by clause in the query */
	bool        p_hasWithFuncClause;
	bool        p_is_connect_by;    /* process special qual: CONNECT BY */
	bool        p_is_start_with;    /* process special qual: START WITH */
	bool        p_is_prior;     	/* process special expr: prior */
	bool		p_has_nocycle;		/* whether specify nocycle */

	bool	has_outer_join_operator;/* Fast path to whether is (+) */
	bool	need_ignore_outer_join;/* Fast path to whether is (+) in having */
	bool	has_outer_join_on;/* Fast path to whether is (+) in on */
	bool	has_ansi_join;/* Have ansi join in join */
	bool	is_right_or_left_join; /* Is left join or right join in join on */
	int		outer_join_varno;/* (+) Varno in join on */

	/* ora_compatible: force the agg calculated at its query level */
	bool		force_agg_local;

	/*
	 * The same SQL does not support multiple sequence nextVal calls. 
	 * The hash table records the number of calls to nextVal in different sequences of the same SQL.
	 * eg.
	 * case1: Statement "select seq_test.nextval, seq_test2.nextval from dual;" is true
	 * case2: Statement "select seq_test.nextval, seq_test2.nextval, seq_test2.nextval from dual;" is wrong
	 */
	HTAB *hash_seqnextval_calls;

	void	*p_pl_state;
	ParseExprItem	p_expr_item;
#ifdef _PG_ORCL_
	List	*with_funcs; /* List of Const of WITH FUNCTION - pg_proc tuples */
	List	*local_funcs; /* List of Const of Nested Function - pg_proc tuples */
	List	*cteList;

	bool	is_top_order;	/* If is parsing a ORDER BY of query */
	List	*ord_aggref;
	bool	parse_type_object;     
	bool	parse_typobj_fun_info; 
	bool	parse_pkg_func_header;
	int  param_cnt;
	List *param_list;
	SubLink	*p_sublink; /* from a sublink? Only check it from its direct child parse. */
	List *targetlist;
	Oid		p_pkgoid;
	Oid  langValidatorOid; /* plsql language validator function oid */
#endif
	bool		isHudiForeign;	/* true if it is hudi FOREIGN TABLE */
	bool		isHudiColumnTypeReset; /* hudi FOREIGN TABLE column timestamp type reset to date */
	bool        isMergeStmt;
	Oid			multisetRetOid;
	bool		isInCheck;		/* Only performs verification operations, no actual execution */
	bool		isDeparsedQuery;
};

/*
 * One nested-aggregate
 */
typedef struct
{
	Aggref	*aggref;
	List	*args;		/* Need replaced args */
	List	*vars;		/* Vars as the new args for 'aggref' */
} NestedAgg;

/*
 * Any aggregate functions with nested aggregate.
 */
typedef struct
{
	List	*aggs;		/* The nested aggref in the TargetEntry */
	int		cur_resno;	/* The nested aggref belongs to which TargetEntry */
} TargetNestedAggs;

/*
 * An element of a namespace list.
 *
 * Namespace items with p_rel_visible set define which RTEs are accessible by
 * qualified names, while those with p_cols_visible set define which RTEs are
 * accessible by unqualified names.  These sets are different because a JOIN
 * without an alias does not hide the contained tables (so they must be
 * visible for qualified references) but it does hide their columns
 * (unqualified references to the columns refer to the JOIN, not the member
 * tables, so we must not complain that such a reference is ambiguous).
 * Various special RTEs such as NEW/OLD for rules may also appear with only
 * one flag set.
 *
 * While processing the FROM clause, namespace items may appear with
 * p_lateral_only set, meaning they are visible only to LATERAL
 * subexpressions.  (The pstate's p_lateral_active flag tells whether we are
 * inside such a subexpression at the moment.)	If p_lateral_ok is not set,
 * it's an error to actually use such a namespace item.  One might think it
 * would be better to just exclude such items from visibility, but the wording
 * of SQL:2008 requires us to do it this way.  We also use p_lateral_ok to
 * forbid LATERAL references to an UPDATE/DELETE target table.
 *
 * At no time should a namespace list contain two entries that conflict
 * according to the rules in checkNameSpaceConflicts; but note that those
 * are more complicated than "must have different alias names", so in practice
 * code searching a namespace list has to check for ambiguous references.
 */
typedef struct ParseNamespaceItem
{
	RangeTblEntry *p_rte;		/* The relation's rangetable entry */
	bool		p_rel_visible;	/* Relation name is visible? */
	bool		p_cols_visible; /* Column names visible as unqualified refs? */
	bool		p_lateral_only; /* Is only visible to LATERAL expressions? */
	bool		p_lateral_ok;	/* If so, does join type allow use? */
} ParseNamespaceItem;

/* Support for parser_errposition_callback function */
typedef struct ParseCallbackState
{
	ParseState *pstate;
	int			location;
	ErrorContextCallback errcallback;
} ParseCallbackState;

/* Insert to copy feature internally depends on data. */
typedef struct
{
	ParamListInfo   params;
	char          **param_string_list;
	bool            partialinsert;

	/* The current memory context is used for memory allocation of data_list. */
	MemoryContext   cxt;
} InsertToCopyInfoData;
/* opentenbase_ora behavior checks that the same SQL statement does not contain more than one seq.nextval. */
typedef struct
{
	Oid seqId; /* Relation Oid related to this calls info */
	int seqnext_calls_num;
} SeqNextCallItem;

extern ParseState *make_parsestate(ParseState *parentParseState);
extern void free_parsestate(ParseState *pstate);
extern int	parser_errposition(ParseState *pstate, int location);

extern void setup_parser_errposition_callback(ParseCallbackState *pcbstate,
								  ParseState *pstate, int location);
extern void cancel_parser_errposition_callback(ParseCallbackState *pcbstate);

extern Var *make_var(ParseState *pstate, RangeTblEntry *rte, int attrno,
		 int location);
extern void set_with_local_funcs(ParseState *pstate);
extern void set_with_function(ParseState *pstate);
extern Oid	transformArrayType(Oid *arrayType, int32 *arrayTypmod);
extern ArrayRef *transformArraySubscripts(ParseState *pstate,
						 Node *arrayBase,
						 Oid arrayType,
						 Oid elementType,
						 int32 arrayTypMod,
						 List *indirection,
						 Node *assignFrom,
						 Oid indexTypOid);
extern Const *make_const(ParseState *pstate, Value *value, int location);

extern char *** transformInsertValuesToCopy(ParseState *pstate, SelectStmt *selectStmt, bool partialinsert, int *ncolumns, bool *support_tocopy);
extern char *** transformInsertValuesToCopyForBind(ParseState *pstate, List *query_list, SelectStmt *selectStmt, int ncolumns, InsertToCopyInfoData *infodata);
extern char *** InitCopyDatalist(int ndatarows, int ncolumns);
extern void DeepfreeCopyDatalist(char ***data_list, int nrows, int ncolumns);

#endif							/* PARSE_NODE_H */
